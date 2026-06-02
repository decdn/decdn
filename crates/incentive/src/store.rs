//! Persistent storage for per-channel voucher state.
//!
//! `ChannelState` (see [`crate::channel`]) tracks the latest accepted voucher
//! in memory. Without a backing store, a node restart loses `last_nonce` and
//! a client can resubmit a previously-accepted voucher for a second byte
//! delivery — see issue #527 and
//! [ADR 003 §Off-chain voucher state persistence](../../../adr/003-payments.md).
//!
//! This module defines the [`ChannelStateStore`] seam: a sync trait the
//! [`crate::channel::ChannelState::apply_voucher`] path commits to before
//! advancing in-memory state and acknowledging delivery. The wiring layer
//! (`crates/node`) provides a `redb`-backed persistent implementation;
//! tests use [`MemoryChannelStateStore`].

use std::collections::HashMap;
use std::sync::Mutex;

use crate::channel::{ChannelId, ChannelState};

/// Durable backing store for [`ChannelState`].
///
/// Implementations MUST persist `record` calls (including fsync, for any
/// disk-backed impl) before returning `Ok` — `apply_voucher` advances its
/// in-memory state only after `record` succeeds, so a successful return is
/// the protocol-level commit point.
///
/// The trait is intentionally synchronous: it's invoked from
/// [`ChannelState::apply_voucher`], which is itself sync. Callers running on
/// a Tokio runtime should invoke the voucher-acceptance path from
/// `tokio::task::spawn_blocking` — same shape as the `KeyStore` seam in
/// `adr/appendix-poc-production-seams.md` §1.
pub trait ChannelStateStore: Send + Sync {
    /// Load every persisted channel. Called once during node bring-up so the
    /// runtime can hydrate its in-memory map before the voucher-accepting
    /// handler starts.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store is unreadable or
    /// contains corrupt entries.
    fn load_all(&self) -> Result<Vec<ChannelState>, StoreError>;

    /// Persist the post-acceptance state for one channel. MUST be durable
    /// (fsynced for disk-backed impls) before returning `Ok`.
    ///
    /// Callers MUST pass a `ChannelState` whose `last_*` tuple is a
    /// non-strict monotonic successor of any previously-recorded state for
    /// `state.channel_id`. The trait does not re-validate this —
    /// [`ChannelState::apply_voucher`] is the canonical caller and enforces
    /// monotonicity upstream. Pushing the check into every implementation
    /// would force a read-modify-write transaction and would mask
    /// `apply_voucher` bugs by silently rejecting them.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the write or fsync fails. Callers MUST
    /// NOT advance their in-memory state on failure.
    fn record(&self, state: &ChannelState) -> Result<(), StoreError>;

    /// Drop the persisted entry for a settled channel. Called from the
    /// on-chain `ChannelSettled` event consumer (#327). A no-op if the
    /// channel has no record.
    ///
    /// Implementations MUST commit durably (fsync, on disk-backed impls)
    /// before returning `Ok` — same contract as `record`. A post-settlement
    /// crash that loses the delete would re-resurrect the row; while the
    /// on-chain monotonic-nonce guard prevents on-chain replay against a
    /// resurrected row, leaving stale entries in the store is a hazard for
    /// any future cross-channel logic that treats "present in store" as
    /// "this channel is live."
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the underlying delete or durable commit
    /// fails.
    fn forget(&self, channel_id: ChannelId) -> Result<(), StoreError>;

    /// Point-lookup the persisted state for one channel, or `None` if no
    /// record exists. Used by the on-chain seller settlement path (#327) to
    /// read the latest accepted voucher (`last_amount` / `last_nonce` /
    /// `last_bytes_delivered` / `last_signature`) when deciding whether an
    /// accrued claim crosses the redemption threshold — always the freshest
    /// committed voucher, so a redemption never submits a stale one.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store is unreadable or the
    /// record is corrupt.
    fn get(&self, channel_id: ChannelId) -> Result<Option<ChannelState>, StoreError>;
}

/// A channel this node closed on-chain (graceful shutdown or pre-expiry
/// sweep) that is awaiting a `settleChannel` call to finalize and route the
/// provider's un-withdrawn remainder (`claimedAmount - withdrawnAmount`)
/// through the `FeeRouter` (#327 / PR #743 review).
///
/// `closeChannel` only *opens* the dispute window; the provider is paid only
/// when `settleChannel` runs after `disputeDeadline`. The client is normally
/// incentivized to settle (to reclaim `deposit - claimedAmount`), but when the
/// client drew the full deposit (`clientRefund == 0`) nobody is — so the node
/// records the closed channel here and a background sweep settles it once the
/// dispute window has elapsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingSettle {
    /// The closed channel awaiting finalization.
    pub channel_id: ChannelId,
    /// On-chain `disputeDeadline` (Unix seconds). `settleChannel` reverts with
    /// `DisputeWindowActive` before this, so the sweep does not submit until
    /// `now >= settle_after` — avoiding gas burned on a guaranteed revert.
    pub settle_after: u64,
}

/// Durable set of channels closed by this node that await a `settleChannel`
/// finalization (#327 / PR #743 review). Separate from [`ChannelStateStore`]
/// because a settle needs only the channel id and a timestamp gate — not the
/// voucher state — and the lifecycles differ: voucher state is forgotten the
/// moment a channel closes, whereas the settle obligation outlives the close
/// by a full dispute window (12–72h) and must survive restarts.
///
/// Implementations MUST commit durably (fsync, on disk-backed impls) before
/// returning `Ok` from `record_pending` / `forget_pending`, mirroring the
/// [`ChannelStateStore`] durability contract.
pub trait PendingSettleStore: Send + Sync {
    /// Persist a closed channel awaiting settlement. Overwrites any existing
    /// entry for the same channel (a re-close re-stamps the deadline).
    ///
    /// Callers MUST set `entry.settle_after` to the channel's on-chain
    /// `disputeDeadline` (read post-close); the store does not validate it. A
    /// too-low value makes the settle sweep submit guaranteed-revert
    /// transactions; a too-high value defers settlement indefinitely.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the durable write fails.
    fn record_pending(&self, entry: &PendingSettle) -> Result<(), StoreError>;

    /// Load every channel awaiting settlement. Called on each sweep tick (and
    /// once at bring-up) to find channels whose dispute window has elapsed.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store is unreadable.
    fn load_pending(&self) -> Result<Vec<PendingSettle>, StoreError>;

    /// Drop the pending-settle entry for a finalized channel — after this
    /// node's own `settleChannel` lands, or when a `ChannelSettled` event
    /// shows another party finalized first. Idempotent; a no-op for an
    /// unknown channel.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the durable delete fails.
    fn forget_pending(&self, channel_id: ChannelId) -> Result<(), StoreError>;
}

/// Durable high-water mark of the last block the settlement watcher scanned for
/// `ChannelOpened` events (#751). Persisting it across restarts is what lets the
/// watcher bring-up backfill cover the **downtime gap**: a channel a client
/// opens against this node while the node is *down* lands in a block before the
/// next boot's head, so without a persisted floor the head-anchored backfill
/// (#762) never sees it and the channel's vouchers are rejected `WrongChannel`
/// forever. On boot the watcher backfills from the stored block (minus a small
/// reorg margin) up to the live-filter install block; `register_open_channel`
/// is idempotent, so re-scanning the overlap is harmless.
///
/// Implementations MUST commit durably (fsync, on disk-backed impls) before
/// returning `Ok` from `record_last_seen_block`, mirroring the
/// [`ChannelStateStore`] durability contract — a lost checkpoint silently
/// widens the rescan (safe, just more RPC), never narrows it.
pub trait WatcherCheckpointStore: Send + Sync {
    /// The last block scanned for `ChannelOpened`, or `None` on a never-written
    /// store (first-ever boot — there is no downtime gap to cover, so the
    /// caller backfills from the current head).
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store is unreadable.
    fn load_last_seen_block(&self) -> Result<Option<u64>, StoreError>;

    /// Persist the last block scanned for `ChannelOpened`. Overwrites the prior
    /// value. Called after the bring-up backfill completes and as the live
    /// stream advances, so the next boot resumes from here.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the durable write fails.
    fn record_last_seen_block(&self, block: u64) -> Result<(), StoreError>;
}

/// Failure modes shared by every [`ChannelStateStore`] implementation.
///
/// Disk-backed impls map their backend errors into [`StoreError::Backend`]
/// (preserving the underlying message); decoding failures use
/// [`StoreError::Codec`]; structurally-invalid on-disk records (wrong magic,
/// truncated value bytes) use [`StoreError::Corrupt`]. The
/// [`StoreError::UnsupportedSchema`] variant is reserved for the case where
/// an on-disk record carries a newer `schema_version` than this binary
/// understands.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// I/O failure (open, read, write, fsync, rename).
    #[error("channel store I/O: {0}")]
    Io(#[from] std::io::Error),
    /// Encoding or decoding failed for an in-store record.
    #[error("channel store encoding: {0}")]
    Codec(String),
    /// Backend-specific error from the underlying KV (e.g. redb transaction
    /// error). The string captures the backend's own description.
    #[error("channel store backend: {0}")]
    Backend(String),
    /// On-disk record carries a `schema_version` higher than `supported`.
    /// The node MUST refuse to start rather than silently ignoring fields
    /// it does not understand — see ADR 003 §Off-chain voucher state
    /// persistence.
    #[error("channel store on-disk schema version {found} not supported (max {supported})")]
    UnsupportedSchema {
        /// Version number read from disk.
        found: u32,
        /// Highest version this binary understands.
        supported: u32,
    },
    /// Record bytes are structurally invalid — missing magic, truncated, or
    /// otherwise unparseable. Includes the affected channel id (when it can
    /// be recovered) and a free-form detail.
    #[error("corrupt record for channel {channel_id:?}: {detail}")]
    Corrupt {
        /// Channel id of the corrupt record, or `None` if the corruption is
        /// at the table / file level and no key could be recovered.
        channel_id: Option<ChannelId>,
        /// Human-readable detail for the operator log.
        detail: String,
    },
    /// Post-create permission tightening (chmod) failed. Distinct from
    /// [`StoreError::Io`] because the operator remediation differs: a
    /// `PermissionTighten` typically means "the file mode is not `0o600`
    /// and the OS refused to fix it" (read-only mount, missing capability,
    /// EPERM on a foreign-owned inode) — recovery requires manual
    /// intervention on the filesystem, not just retrying the open. Log
    /// triage should escalate this above transient I/O.
    #[error("failed to tighten permissions on {path}: {source}")]
    PermissionTighten {
        /// Filesystem path of the affected file.
        path: std::path::PathBuf,
        /// Underlying I/O error returned by the chmod syscall.
        source: std::io::Error,
    },
}

/// In-memory [`ChannelStateStore`] for tests and the trait's reference
/// semantics. Not durable — drops with the process. The runtime uses the
/// redb-backed impl in `crates/node`.
#[derive(Debug, Default)]
pub struct MemoryChannelStateStore {
    inner: Mutex<HashMap<ChannelId, ChannelState>>,
}

impl MemoryChannelStateStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the current entry count. Useful in tests to assert that
    /// rejected vouchers did not write through. A poisoned mutex reports
    /// `0` rather than panicking — tests on this store should already have
    /// failed louder if a panic poisoned the lock.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().map_or(0, |m| m.len())
    }

    /// `true` when no channels are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ChannelStateStore for MemoryChannelStateStore {
    fn load_all(&self) -> Result<Vec<ChannelState>, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        Ok(guard.values().cloned().collect())
    }

    fn record(&self, state: &ChannelState) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        guard.insert(state.channel_id, state.clone());
        Ok(())
    }

    fn forget(&self, channel_id: ChannelId) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        guard.remove(&channel_id);
        Ok(())
    }

    fn get(&self, channel_id: ChannelId) -> Result<Option<ChannelState>, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        Ok(guard.get(&channel_id).cloned())
    }
}

/// In-memory [`PendingSettleStore`] for tests and the trait's reference
/// semantics. Not durable — drops with the process. The runtime uses the
/// redb-backed impl in `crates/node`.
#[derive(Debug, Default)]
pub struct MemoryPendingSettleStore {
    inner: Mutex<HashMap<ChannelId, u64>>,
}

impl MemoryPendingSettleStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl PendingSettleStore for MemoryPendingSettleStore {
    fn record_pending(&self, entry: &PendingSettle) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        guard.insert(entry.channel_id, entry.settle_after);
        Ok(())
    }

    fn load_pending(&self) -> Result<Vec<PendingSettle>, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        Ok(guard
            .iter()
            .map(|(&channel_id, &settle_after)| PendingSettle {
                channel_id,
                settle_after,
            })
            .collect())
    }

    fn forget_pending(&self, channel_id: ChannelId) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        guard.remove(&channel_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{U256, address, b256};

    fn sample(channel_id_byte: u8) -> ChannelState {
        let mut bytes = [0u8; 32];
        bytes[31] = channel_id_byte;
        ChannelState::hydrate(
            bytes.into(),
            address!("00000000000000000000000000000000000000aa"),
            address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            U256::from(10_000_000u64),
            U256::from(1_234u64),
            U256::from(7u64),
            U256::from(4_096u64),
            Some([0xABu8; 65]),
            1_900_000_000,
        )
    }

    #[test]
    fn memory_store_round_trip() -> anyhow::Result<()> {
        let store = MemoryChannelStateStore::new();
        let a = sample(1);
        let b = sample(2);
        store.record(&a)?;
        store.record(&b)?;
        let mut all = store.load_all()?;
        all.sort_by_key(|s| s.channel_id);
        anyhow::ensure!(all.len() == 2);
        let first = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        let second = all.get(1).ok_or_else(|| anyhow::anyhow!("missing [1]"))?;
        anyhow::ensure!(*first == a);
        anyhow::ensure!(*second == b);
        Ok(())
    }

    #[test]
    fn memory_store_record_overwrites() -> anyhow::Result<()> {
        let store = MemoryChannelStateStore::new();
        let s = sample(1);
        store.record(&s)?;
        // Re-record the same channel with an advanced nonce (overwrite). The
        // `last_*` fields are private, so rebuild via `hydrate` rather than
        // mutating in place.
        let advanced = ChannelState::hydrate(
            s.channel_id,
            s.client,
            s.token,
            s.deposit,
            s.last_amount(),
            U256::from(99u64),
            s.last_bytes_delivered(),
            s.last_signature().copied(),
            s.expires_at,
        );
        store.record(&advanced)?;
        let all = store.load_all()?;
        anyhow::ensure!(all.len() == 1);
        let only = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        anyhow::ensure!(only.last_nonce() == U256::from(99u64));
        Ok(())
    }

    #[test]
    fn memory_store_forget_removes_entry() -> anyhow::Result<()> {
        let store = MemoryChannelStateStore::new();
        let s = sample(1);
        let id = s.channel_id;
        store.record(&s)?;
        store.forget(id)?;
        anyhow::ensure!(store.is_empty());
        // Forgetting a never-recorded channel is a no-op.
        store.forget(b256!(
            "1111111111111111111111111111111111111111111111111111111111111111"
        ))?;
        Ok(())
    }

    fn pending(channel_id_byte: u8, settle_after: u64) -> PendingSettle {
        let mut bytes = [0u8; 32];
        bytes[31] = channel_id_byte;
        PendingSettle {
            channel_id: bytes.into(),
            settle_after,
        }
    }

    #[test]
    fn pending_store_round_trip_and_overwrite() -> anyhow::Result<()> {
        let store = MemoryPendingSettleStore::new();
        store.record_pending(&pending(1, 1_000))?;
        store.record_pending(&pending(2, 2_000))?;
        // A re-close re-stamps the deadline for the same channel.
        store.record_pending(&pending(1, 1_500))?;

        let mut all = store.load_pending()?;
        all.sort_by_key(|p| p.channel_id);
        anyhow::ensure!(all.len() == 2, "overwrite must not add a row");
        let first = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        anyhow::ensure!(first.settle_after == 1_500, "re-close re-stamps deadline");
        Ok(())
    }

    #[test]
    fn pending_store_forget_removes_entry() -> anyhow::Result<()> {
        let store = MemoryPendingSettleStore::new();
        let entry = pending(3, 5_000);
        store.record_pending(&entry)?;
        store.forget_pending(entry.channel_id)?;
        anyhow::ensure!(store.load_pending()?.is_empty());
        // Forgetting a never-recorded channel is a no-op.
        store.forget_pending(b256!(
            "2222222222222222222222222222222222222222222222222222222222222222"
        ))?;
        Ok(())
    }
}
