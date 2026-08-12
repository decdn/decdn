//! Persistent storage for per-lane voucher state.
//!
//! `LaneState` (see [`crate::lane`]) tracks the latest accepted voucher in
//! memory. Without a backing store, a node restart loses `last_amount` and a
//! client can resubmit a previously-accepted voucher for a second byte
//! delivery — see issue #527 and
//! [ADR 003 §Off-chain voucher state persistence](../../../adr/003-payments.md).
//!
//! This module defines the [`PoolStateStore`] seam: a sync trait the
//! [`crate::lane::LaneState::apply_voucher`] path commits to before advancing
//! in-memory state and delivering further bytes. The wiring layer
//! (`crates/node`) provides a `redb`-backed persistent implementation; tests use
//! [`MemoryPoolStateStore`].

use std::collections::HashMap;
use std::sync::Mutex;

use alloy::primitives::B256;

use crate::lane::{LaneKey, LaneState, PoolId};

/// Durable backing store for [`LaneState`], keyed by [`LaneKey`].
///
/// Implementations MUST persist `record` calls (including fsync, for any
/// disk-backed impl) before returning `Ok` — `apply_voucher` advances its
/// in-memory state only after `record` succeeds, so a successful return is the
/// protocol-level commit point.
///
/// The trait is intentionally synchronous: it's invoked from
/// [`LaneState::apply_voucher`], which is itself sync. Callers running on a
/// Tokio runtime should invoke the voucher-acceptance path from
/// `tokio::task::spawn_blocking` — same shape as the `KeyStore` seam in
/// `adr/appendix-poc-production-seams.md` §1.
pub trait PoolStateStore: Send + Sync {
    /// Load every persisted lane. Called once during node bring-up so the
    /// runtime can hydrate its in-memory map before the voucher-accepting
    /// handler starts.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store is unreadable or contains
    /// corrupt entries.
    fn load_all(&self) -> Result<Vec<LaneState>, StoreError>;

    /// Persist the post-acceptance state for one lane. MUST be durable (fsynced
    /// for disk-backed impls) before returning `Ok`.
    ///
    /// Callers MUST pass a `LaneState` whose `last_*` tuple is a non-strict
    /// monotonic successor of any previously-recorded state for the same
    /// [`LaneKey`]. The trait does not re-validate this —
    /// [`LaneState::apply_voucher`] is the canonical caller and enforces
    /// monotonicity upstream.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the write or fsync fails. Callers MUST NOT
    /// advance their in-memory state on failure.
    fn record(&self, state: &LaneState) -> Result<(), StoreError>;

    /// Drop the persisted entry for a settled lane. A no-op if the lane has no
    /// record.
    ///
    /// Implementations MUST commit durably (fsync, on disk-backed impls) before
    /// returning `Ok` — same contract as `record`.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the underlying delete or durable commit
    /// fails.
    fn forget(&self, key: LaneKey) -> Result<(), StoreError>;

    /// Point-lookup the persisted state for one lane, or `None` if no record
    /// exists. Used by the on-chain seller redemption path to read the latest
    /// accepted voucher (`last_amount` / `last_bytes_delivered` /
    /// `last_signature`) when deciding whether an accrued claim crosses the
    /// redemption threshold — always the freshest committed voucher.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store is unreadable or the record
    /// is corrupt.
    fn get(&self, key: LaneKey) -> Result<Option<LaneState>, StoreError>;
}

/// A pool this node closed on-chain (graceful shutdown or grace-window sweep)
/// that is awaiting the grace window to elapse before the provider's accrued
/// claim finalizes (#327).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingSettle {
    /// The pool awaiting finalization.
    pub pool_id: B256,
    /// Unix seconds after which the grace window has elapsed and the settle may
    /// proceed. The sweep does not submit until `now >= settle_after`.
    pub settle_after: u64,
}

/// Durable set of pools closed by this node that await finalization (#327).
/// Separate from [`PoolStateStore`] because a settle needs only the pool id and
/// a timestamp gate — not the voucher state — and the lifecycles differ.
///
/// Implementations MUST commit durably (fsync, on disk-backed impls) before
/// returning `Ok` from `record_pending` / `forget_pending`, mirroring the
/// [`PoolStateStore`] durability contract.
pub trait PendingSettleStore: Send + Sync {
    /// Persist a closed pool awaiting settlement. Overwrites any existing entry
    /// for the same pool (a re-close re-stamps the deadline).
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the durable write fails.
    fn record_pending(&self, entry: &PendingSettle) -> Result<(), StoreError>;

    /// Load every pool awaiting settlement. Called on each sweep tick (and once
    /// at bring-up) to find pools whose grace window has elapsed.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the backing store is unreadable.
    fn load_pending(&self) -> Result<Vec<PendingSettle>, StoreError>;

    /// Drop the pending-settle entry for a finalized pool. Idempotent; a no-op
    /// for an unknown pool.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the durable delete fails.
    fn forget_pending(&self, pool_id: B256) -> Result<(), StoreError>;
}

/// Identifies which on-chain event watcher a persisted scan checkpoint belongs
/// to (#1092). Every variant maps to a stable string key in the single
/// `watcher_checkpoint_v1` table, so one concrete store holds every watcher's
/// high-water mark (redb forbids two `Database` handles to one file).
///
/// The [`as_str`](CheckpointKey::as_str) literals are **on-disk identifiers**:
/// renaming one silently forfeits that watcher's resume (its next boot re-scans
/// from the configured floor instead of the stored block), so they are frozen
/// for backward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CheckpointKey {
    /// Redemption watcher `PoolOpened` high-water block.
    PoolOpened,
    /// Origin-directory watcher scan cursor (#1108). The origin watcher no
    /// longer persists one (it re-enumerates the namespace set every boot,
    /// #1504); the key remains the second checkpoint key the store's
    /// multi-watcher tests exercise.
    Origin,
}

impl CheckpointKey {
    /// The stable on-disk key for this checkpoint. Frozen for backward
    /// compatibility — see the type-level doc.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PoolOpened => "pool_opened_last_block",
            Self::Origin => "origin_directory_last_block",
        }
    }
}

/// Durable high-water marks of the last block each on-chain watcher scanned,
/// keyed by [`CheckpointKey`] (#751, generalized in #1092/#1108). Persisting
/// them across restarts is what lets a watcher's bring-up backfill cover the
/// **downtime gap**: an event landing while the node is *down* sits in a block
/// before the next boot's head, so without a persisted floor a head-anchored
/// backfill never sees it. On boot the watcher backfills from the stored block
/// (minus a small reorg margin) up to head; the per-log sinks are idempotent, so
/// re-scanning the overlap is harmless.
///
/// A directly disk-backed implementation MUST commit durably (fsync) before
/// returning `Ok` from [`record_checkpoint`](Self::record_checkpoint), mirroring
/// the [`PoolStateStore`] durability contract. A debouncing *decorator* MAY relax
/// that per-call fsync — buffering in memory and coarsening the durable write
/// cadence — provided it preserves the two invariants this contract rests on,
/// **per key**: the persisted block is **monotonic** (never lowered) and the
/// latest buffered block is forced out on graceful shutdown via
/// [`flush_checkpoint`](KeyedCheckpointStore::flush_checkpoint). Both relaxations
/// are safe because a lost or lagging checkpoint only ever silently widens the
/// rescan (more RPC), never narrows it.
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
    /// scan position, which is safe — see the type-level contract above); such a
    /// decorator overrides [`flush_checkpoint`](Self::flush_checkpoint) to force
    /// the buffered value out on graceful shutdown. A directly disk-backed
    /// implementation commits durably before returning `Ok`.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the durable write fails.
    fn record_checkpoint(&self, key: CheckpointKey, block: u64) -> Result<(), StoreError>;

    /// Force any buffered checkpoint for `key` to durable storage. The default
    /// is a no-op: implementations that already commit durably inside
    /// [`record_checkpoint`](Self::record_checkpoint) have nothing buffered. A
    /// debouncing decorator overrides this to fsync the latest deferred block,
    /// and the runtime calls it on graceful shutdown so the most recent scan
    /// progress is not lost to the next boot's rescan.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the durable write fails.
    fn flush_checkpoint(&self, key: CheckpointKey) -> Result<(), StoreError> {
        let _ = key;
        Ok(())
    }
}

/// Failure modes shared by every [`PoolStateStore`] implementation.
///
/// Disk-backed impls map their backend errors into [`StoreError::Backend`]
/// (preserving the underlying message); decoding failures use
/// [`StoreError::Codec`]; structurally-invalid on-disk records (wrong magic,
/// truncated value bytes) use [`StoreError::Corrupt`]. The
/// [`StoreError::UnsupportedSchema`] variant is reserved for the case where an
/// on-disk record carries a newer `schema_version` than this binary understands.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// I/O failure (open, read, write, fsync, rename).
    #[error("pool store I/O: {0}")]
    Io(#[from] std::io::Error),
    /// Encoding or decoding failed for an in-store record.
    #[error("pool store encoding: {0}")]
    Codec(String),
    /// Backend-specific error from the underlying KV (e.g. redb transaction
    /// error). The string captures the backend's own description.
    #[error("pool store backend: {0}")]
    Backend(String),
    /// On-disk record carries a `schema_version` higher than `supported`. The
    /// node MUST refuse to start rather than silently ignoring fields it does
    /// not understand — see ADR 003 §Off-chain voucher state persistence.
    #[error("pool store on-disk schema version {found} not supported (max {supported})")]
    UnsupportedSchema {
        /// Version number read from disk.
        found: u32,
        /// Highest version this binary understands.
        supported: u32,
    },
    /// Record bytes are structurally invalid — missing magic, truncated, or
    /// otherwise unparseable. Includes the affected id (when it can be
    /// recovered) and a free-form detail.
    #[error("corrupt record for {pool_id:?}: {detail}")]
    Corrupt {
        /// Id of the corrupt record, or `None` if the corruption is at the
        /// table / file level and no key could be recovered.
        pool_id: Option<PoolId>,
        /// Human-readable detail for the operator log.
        detail: String,
    },
    /// Post-create permission tightening (chmod) failed. Distinct from
    /// [`StoreError::Io`] because the operator remediation differs: a
    /// `PermissionTighten` typically means "the file mode is not `0o600` and the
    /// OS refused to fix it" (read-only mount, missing capability, EPERM on a
    /// foreign-owned inode) — recovery requires manual intervention on the
    /// filesystem, not just retrying the open.
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
    /// processes corrupt the shared voucher watermark (#942). Run one process
    /// per data dir, give each a distinct `--data-dir`, or let one process
    /// multiplex internally.
    #[error(
        "another decdn process is using the pool store at {path} — run one fetch/pull \
         at a time per --data-dir, or give each invocation a separate --data-dir"
    )]
    AlreadyOpen {
        /// Filesystem path of the locked database file.
        path: std::path::PathBuf,
    },
}

/// In-memory [`PoolStateStore`] for tests and the trait's reference semantics.
/// Not durable — drops with the process. The runtime uses the redb-backed impl
/// in `crates/node`.
#[derive(Debug, Default)]
pub struct MemoryPoolStateStore {
    inner: Mutex<HashMap<LaneKey, LaneState>>,
}

impl MemoryPoolStateStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot the current entry count. Useful in tests to assert that rejected
    /// vouchers did not write through. A poisoned mutex reports `0` rather than
    /// panicking.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().map_or(0, |m| m.len())
    }

    /// `true` when no lanes are tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl PoolStateStore for MemoryPoolStateStore {
    fn load_all(&self) -> Result<Vec<LaneState>, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        Ok(guard.values().cloned().collect())
    }

    fn record(&self, state: &LaneState) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        guard.insert(state.key(), state.clone());
        Ok(())
    }

    fn forget(&self, key: LaneKey) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        guard.remove(&key);
        Ok(())
    }

    fn get(&self, key: LaneKey) -> Result<Option<LaneState>, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        Ok(guard.get(&key).cloned())
    }
}

/// In-memory [`PendingSettleStore`] for tests and the trait's reference
/// semantics. Not durable — drops with the process. The runtime uses the
/// redb-backed impl in `crates/node`.
#[derive(Debug, Default)]
pub struct MemoryPendingSettleStore {
    inner: Mutex<HashMap<B256, u64>>,
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
        guard.insert(entry.pool_id, entry.settle_after);
        Ok(())
    }

    fn load_pending(&self) -> Result<Vec<PendingSettle>, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        Ok(guard
            .iter()
            .map(|(&pool_id, &settle_after)| PendingSettle {
                pool_id,
                settle_after,
            })
            .collect())
    }

    fn forget_pending(&self, pool_id: B256) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        guard.remove(&pool_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{U256, address, b256};

    fn sample(pool_id_byte: u8, signer_byte: u8) -> LaneState {
        let mut pool = [0u8; 32];
        pool[31] = pool_id_byte;
        let mut signer = [0u8; 20];
        signer[19] = signer_byte;
        LaneState::hydrate(
            pool.into(),
            signer.into(),
            address!("00000000000000000000000000000000000000b2"),
            U256::from(10_000_000u64),
            1_900_000_000,
            U256::from(1_234u64),
            U256::from(4_096u64),
            Some([0xABu8; 65]),
        )
    }

    #[test]
    fn memory_store_round_trip() -> anyhow::Result<()> {
        let store = MemoryPoolStateStore::new();
        let a = sample(1, 1);
        let b = sample(2, 2);
        store.record(&a)?;
        store.record(&b)?;
        let mut all = store.load_all()?;
        all.sort_by_key(|s| s.pool_id);
        anyhow::ensure!(all.len() == 2);
        let first = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        let second = all.get(1).ok_or_else(|| anyhow::anyhow!("missing [1]"))?;
        anyhow::ensure!(*first == a);
        anyhow::ensure!(*second == b);
        Ok(())
    }

    #[test]
    fn memory_store_record_overwrites() -> anyhow::Result<()> {
        let store = MemoryPoolStateStore::new();
        let s = sample(1, 1);
        store.record(&s)?;
        // Re-record the same lane with an advanced amount (overwrite). The
        // `last_*` fields are private, so rebuild via `hydrate` rather than
        // mutating in place.
        let advanced = LaneState::hydrate(
            s.pool_id,
            s.signer,
            s.provider,
            s.cap,
            s.expiry,
            U256::from(9_999u64),
            s.last_bytes_delivered(),
            s.last_signature().copied(),
        );
        store.record(&advanced)?;
        let all = store.load_all()?;
        anyhow::ensure!(all.len() == 1);
        let only = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        anyhow::ensure!(only.last_amount() == U256::from(9_999u64));
        Ok(())
    }

    #[test]
    fn memory_store_forget_removes_entry() -> anyhow::Result<()> {
        let store = MemoryPoolStateStore::new();
        let s = sample(1, 1);
        let key = s.key();
        store.record(&s)?;
        store.forget(key)?;
        anyhow::ensure!(store.is_empty());
        // Forgetting a never-recorded lane is a no-op.
        store.forget(LaneKey {
            pool_id: b256!("1111111111111111111111111111111111111111111111111111111111111111"),
            signer: address!("00000000000000000000000000000000000000a1"),
            provider: address!("00000000000000000000000000000000000000b2"),
        })?;
        Ok(())
    }

    fn pending(pool_id_byte: u8, settle_after: u64) -> PendingSettle {
        let mut bytes = [0u8; 32];
        bytes[31] = pool_id_byte;
        PendingSettle {
            pool_id: bytes.into(),
            settle_after,
        }
    }

    #[test]
    fn pending_store_round_trip_and_overwrite() -> anyhow::Result<()> {
        let store = MemoryPendingSettleStore::new();
        store.record_pending(&pending(1, 1_000))?;
        store.record_pending(&pending(2, 2_000))?;
        // A re-close re-stamps the deadline for the same pool.
        store.record_pending(&pending(1, 1_500))?;

        let mut all = store.load_pending()?;
        all.sort_by_key(|p| p.pool_id);
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
        store.forget_pending(entry.pool_id)?;
        anyhow::ensure!(store.load_pending()?.is_empty());
        // Forgetting a never-recorded pool is a no-op.
        store.forget_pending(b256!(
            "2222222222222222222222222222222222222222222222222222222222222222"
        ))?;
        Ok(())
    }
}
