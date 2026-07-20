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

/// Identifies which on-chain event watcher a persisted scan checkpoint belongs
/// to (#1092). Every variant maps to a stable string key in the single
/// `watcher_checkpoint_v1` table, so one concrete store holds every watcher's
/// high-water mark (redb forbids two `Database` handles to one file).
///
/// The [`as_str`](CheckpointKey::as_str) literals are **on-disk identifiers**:
/// renaming one silently forfeits that watcher's resume (its next boot re-scans
/// from the configured floor instead of the stored block), so they are frozen
/// for backward compatibility. `ChannelOpened`'s literal predates the keyed
/// store (#751) and is preserved verbatim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CheckpointKey {
    /// Settlement watcher `ChannelOpened` high-water block (#751).
    ChannelOpened,
    /// Blacklist watcher deny-set scan cursor (#1108, #1181). Safe to persist
    /// only because the deny-set projection itself is durable
    /// ([`BlacklistEntryStore`]) — a resume no longer drops the
    /// still-out-of-scope entries the watcher retains for re-scoping. The
    /// deny-set write is committed before this cursor advances past the log that
    /// produced it.
    Blacklist,
    /// Origin-directory watcher `ContentClaimed` scan cursor (#1108).
    Origin,
}

impl CheckpointKey {
    /// The stable on-disk key for this checkpoint. Frozen for backward
    /// compatibility — see the type-level doc.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ChannelOpened => "channel_opened_last_block",
            Self::Blacklist => "content_blacklist_last_block",
            Self::Origin => "origin_directory_last_block",
        }
    }
}

/// Durable high-water marks of the last block each on-chain watcher scanned,
/// keyed by [`CheckpointKey`] (#751, generalized in #1092/#1108). Persisting
/// them across restarts is what lets a watcher's bring-up backfill cover the
/// **downtime gap**: an event landing while the node is *down* sits in a block
/// before the next boot's head, so without a persisted floor a head-anchored
/// backfill never sees it (e.g. a settlement `ChannelOpened` whose channel then
/// rejects vouchers `WrongChannel` forever, #762). On boot the watcher backfills
/// from the stored block (minus a small reorg margin) up to head; the per-log
/// sinks are idempotent, so re-scanning the overlap is harmless.
///
/// A directly disk-backed implementation MUST commit durably (fsync) before
/// returning `Ok` from [`record_checkpoint`](Self::record_checkpoint), mirroring the
/// [`ChannelStateStore`] durability contract. A debouncing *decorator* MAY
/// relax that per-call fsync — buffering in memory and coarsening the durable
/// write cadence — provided it preserves the two invariants this contract
/// rests on, **per key**: the persisted block is **monotonic** (never lowered)
/// and the latest buffered block is forced out on graceful shutdown via
/// [`flush_checkpoint`](KeyedCheckpointStore::flush_checkpoint). Both relaxations are safe because a
/// lost or lagging checkpoint only ever silently widens the rescan (more RPC),
/// never narrows it.
pub trait KeyedCheckpointStore: Send + Sync {
    /// The last block scanned for `key`, or `None` on a never-written key
    /// (first-ever boot for that watcher — the caller applies its configured
    /// none-fallback floor).
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store is unreadable.
    fn load_checkpoint(&self, key: CheckpointKey) -> Result<Option<u64>, StoreError>;

    /// Persist the last block scanned for `key`. Overwrites the prior value.
    /// Called as each backfill window and live tick advances, so the next boot
    /// resumes from here.
    ///
    /// A debouncing decorator MAY buffer the value in memory and defer the
    /// durable write to a coarser cadence (the floor only ever lags the true
    /// scan position, which is safe — see the type-level contract above); such
    /// a decorator overrides [`flush_checkpoint`](Self::flush_checkpoint) to force the buffered value
    /// out on graceful shutdown. A directly disk-backed implementation commits
    /// durably before returning `Ok`.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the durable write fails.
    fn record_checkpoint(&self, key: CheckpointKey, block: u64) -> Result<(), StoreError>;

    /// Force any buffered checkpoint for `key` to durable storage. The default
    /// is a no-op: implementations that already commit durably inside
    /// [`record_checkpoint`](Self::record_checkpoint) have nothing buffered. A debouncing decorator
    /// overrides this to fsync the latest deferred block, and the runtime calls
    /// it on graceful shutdown so the most recent scan progress is not lost to
    /// the next boot's rescan.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the durable write fails.
    fn flush_checkpoint(&self, key: CheckpointKey) -> Result<(), StoreError> {
        let _ = key;
        Ok(())
    }
}

/// One persisted deny-set row: `(region, hash)`, both raw 32-byte values, keyed
/// exactly as the contract's own `_hashEntries[region][hash]`.
pub type BlacklistEntryRow = ([u8; 32], [u8; 32]);

/// Durable projection of the blacklist deny-set: every `(region, hash)` entry the
/// blacklist watcher has seen and not yet locally evicted.
///
/// **Why this has to be durable.** `ContentBlacklist` exposes no enumeration
/// view, so the deny-set can only be built from events. The watcher must retain
/// entries that are currently *out of scope* — wrong region, or appeal-suspended
/// — because either can become live and in scope again with **no** new
/// `HashBlacklisted` log (an operator `CapacityBond.updateRegion` emits nothing on
/// this contract at all). While that projection was in-memory only, a persisted
/// scan cursor was unsafe: resuming past those logs would drop the still-retained
/// entries, silently removing them from re-scoping. That is why the watcher
/// full-replayed from the deploy block on every boot. Persisting the projection
/// here is what makes [`CheckpointKey::Blacklist`] safe to use.
///
/// **Durability contract.** Unlike [`KeyedCheckpointStore`], these writes may
/// **not** be debounced or buffered: an implementation MUST commit durably before
/// returning `Ok`. The watcher's scan cursor is only advanced after the log that
/// produced the write has been applied, so a write that is lost while the cursor
/// moves past it is an entry the node never re-learns — and serving a blacklisted
/// hash is slashable. A failed write must surface as `Err` so the caller can
/// refuse to advance the cursor.
///
/// Ordering is therefore: durable entry write first, cursor advance second. The
/// reverse (or a buffered write) reintroduces exactly the gap this replaces.
/// Re-applying a window is harmless — every operation here is idempotent.
pub trait BlacklistEntryStore: Send + Sync {
    /// Every persisted `(region, hash)` entry, for rebuilding the in-memory
    /// deny-set on boot. An empty result is a legitimate cold start.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store is unreadable.
    fn load_blacklist_entries(&self) -> Result<Vec<BlacklistEntryRow>, StoreError>;

    /// Record one `(region, hash)` entry. Idempotent: re-recording an existing
    /// entry is a no-op, so replaying a scan window is safe.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the durable write fails. The caller MUST NOT
    /// advance its scan cursor past the log that produced this entry.
    fn insert_blacklist_entry(&self, region: [u8; 32], hash: [u8; 32]) -> Result<(), StoreError>;

    /// Drop exactly one `(region, hash)` entry (a `HashRemoved` for one region
    /// must not disturb a surviving same-hash entry under another region).
    /// Idempotent.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the durable write fails.
    fn remove_blacklist_entry(&self, region: [u8; 32], hash: [u8; 32]) -> Result<(), StoreError>;

    /// Drop every entry for `hash` across all regions — used once the hash is
    /// locally evicted, since eviction is sticky and covers every region.
    /// Idempotent.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the durable write fails.
    fn remove_blacklist_hash(&self, hash: [u8; 32]) -> Result<(), StoreError>;
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
    /// The store's database file is already opened (write-locked) by another
    /// process — typically a second `decdn fetch`/`bundle pull` against the same
    /// `--data-dir`. redb holds a process-exclusive lock for the lifetime of the
    /// open `Database`, so the second opener fails fast rather than letting two
    /// processes corrupt the shared per-channel voucher nonce (#942). Truly
    /// concurrent shared-channel fetch is out of scope; run one process per
    /// data dir, give each a distinct `--data-dir`, or let one process
    /// multiplex internally.
    #[error(
        "another decdn process is using the channel store at {path} — run one fetch/pull \
         at a time per --data-dir, or give each invocation a separate --data-dir"
    )]
    AlreadyOpen {
        /// Filesystem path of the locked database file.
        path: std::path::PathBuf,
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
            false,
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
            s.cooperative_close_signed(),
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
