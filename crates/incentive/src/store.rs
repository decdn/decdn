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

    /// Record the post-acceptance state for one lane in the store's working set.
    /// A disk-backed store MAY buffer this in memory and make it durable later
    /// via [`flush`](PoolStateStore::flush); it is not required to fsync before
    /// returning `Ok`. Frontier state is safe to lose on a crash (an honest
    /// client resumes forward; an un-redeemed replay is still on-chain-payable).
    ///
    /// Callers MUST pass a `LaneState` whose `last_*` tuple is a non-strict
    /// monotonic successor of any previously-recorded state for the same
    /// [`LaneKey`]. The trait does not re-validate this —
    /// [`LaneState::apply_voucher`] is the canonical caller and enforces
    /// monotonicity upstream.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the write fails. Callers MUST NOT
    /// advance their in-memory state on failure.
    fn record(&self, state: &LaneState) -> Result<(), StoreError>;

    /// Drop the persisted entry for a settled lane. A no-op if the lane has no
    /// record.
    ///
    /// A disk-backed store MAY buffer the removal and apply it on the next
    /// [`flush`](PoolStateStore::flush); it is not required to fsync before
    /// returning `Ok`.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the underlying delete fails.
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

    /// Force all buffered lane state durable (fsync, for disk-backed impls).
    ///
    /// The default is a no-op: a volatile store ([`MemoryPoolStateStore`]) holds
    /// nothing to flush, and a store that commits inside `record` has nothing
    /// buffered. A disk-backed implementation MAY override this to buffer
    /// `record` and `forget` in memory and make them durable here in one fsynced
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns a [`StoreError`] if the durable write fails.
    fn flush(&self) -> Result<(), StoreError> {
        Ok(())
    }

    /// Raise this lane's observed on-chain registration expiry
    /// (`LaneState::registered_until`) to `registered_until`, monotonically — a
    /// lower or equal value is a no-op, and `0` (unknown) never lowers a known
    /// value. Touches ONLY that field, never the replay-critical `last_*` tuple,
    /// so the seller redeemer can record a fresh registration without racing a
    /// concurrent voucher advance into a lost update. A no-op for a lane with no
    /// record. Buffered like `record`; durability is the next `flush`'s job.
    ///
    /// # Errors
    /// Returns a [`StoreError`] if the backing store is unwritable.
    fn set_registered_until(&self, key: LaneKey, registered_until: u64) -> Result<(), StoreError> {
        let _ = (key, registered_until);
        Ok(())
    }
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
/// returning `Ok` from `record_pending` / `forget_pending`.
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
/// returning `Ok` from [`record_checkpoint`](Self::record_checkpoint). A debouncing *decorator* MAY relax
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
        let mut next = state.clone();
        if let Some(existing) = guard.get(&next.key()) {
            next.registered_until = next.registered_until.max(existing.registered_until);
        }
        guard.insert(next.key(), next);
        Ok(())
    }

    fn set_registered_until(&self, key: LaneKey, registered_until: u64) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        if let Some(state) = guard.get_mut(&key) {
            state.registered_until = state.registered_until.max(registered_until);
        }
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

/// Durable per-pool accumulator of unrecoverable floor-credit loss (`µUSDC`), keyed
/// by pool id (ADR 003 §Pool solvency — the `dead_charge`). Persisted so a node
/// restart does not grant a pool a fresh free-floor budget: the un-vouchered floor
/// is un-redeemable, so it appears in no on-chain quantity and must be stored here.
///
/// Implementations MUST raise a pool's total monotonically: a total at or below
/// the stored one leaves the row unchanged, and [`PoolFloorLossStore::forget_loss`]
/// is the only downward transition. The store owns this rather than the caller
/// because a caller that persists its total from an independent task cannot order
/// its writes against another's.
///
/// Any write that raises the total MUST commit durably (fsync, on disk-backed
/// impls) before returning `Ok`, mirroring the [`PoolStateStore`] contract. A call
/// that raises nothing may skip the commit: the durable value already satisfies it.
pub trait PoolFloorLossStore: Send + Sync {
    /// Raise the pool's cumulative dead-charge total to `micro_usdc`. A total at or
    /// below the stored one is a no-op, so a late, smaller write cannot regress the
    /// row and re-grant already-consumed free-floor budget.
    ///
    /// # Errors
    /// Returns [`StoreError`] if the durable write fails.
    fn record_loss(&self, pool_id: B256, micro_usdc: u128) -> Result<(), StoreError>;

    /// Load every pool's persisted dead charge. Called once at bring-up to hydrate
    /// the in-memory accumulator.
    ///
    /// # Errors
    /// Returns [`StoreError`] if the backing store is unreadable.
    fn load_losses(&self) -> Result<Vec<(B256, u128)>, StoreError>;

    /// Drop a pool's entry (on pool close/reclaim). Idempotent.
    ///
    /// # Errors
    /// Returns [`StoreError`] if the durable delete fails.
    fn forget_loss(&self, pool_id: B256) -> Result<(), StoreError>;
}

/// In-memory [`PoolFloorLossStore`] for tests. Not durable — drops with the
/// process. The runtime uses the redb-backed impl in `crates/node`.
#[derive(Debug, Default)]
pub struct MemoryPoolFloorLossStore {
    inner: Mutex<HashMap<B256, u128>>,
}

impl MemoryPoolFloorLossStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl PoolFloorLossStore for MemoryPoolFloorLossStore {
    fn record_loss(&self, pool_id: B256, micro_usdc: u128) -> Result<(), StoreError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        // Monotonic raise, matching the redb store step for step: out-of-order drops
        // on one pool must not regress the total and re-grant consumed floor budget.
        // An absent row reads as zero rather than being created, so a total that
        // raises nothing — including a zero against an absent row — writes nothing.
        let stored = guard.get(&pool_id).copied().unwrap_or(0u128);
        if micro_usdc > stored {
            guard.insert(pool_id, micro_usdc);
        }
        Ok(())
    }

    fn load_losses(&self) -> Result<Vec<(B256, u128)>, StoreError> {
        let guard = self
            .inner
            .lock()
            .map_err(|err| StoreError::Backend(format!("memory store mutex poisoned: {err}")))?;
        Ok(guard.iter().map(|(&k, &v)| (k, v)).collect())
    }

    fn forget_loss(&self, pool_id: B256) -> Result<(), StoreError> {
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
            crate::lane::LaneChain::NONE,
        )
    }

    #[test]
    fn record_does_not_regress_registered_until() -> Result<(), StoreError> {
        let store = MemoryPoolStateStore::new();
        let mut st = sample(1, 2);
        st.registered_until = 0;
        store.record(&st)?; // voucher-path shape: unknown
        store.set_registered_until(st.key(), 1_800_000_000)?; // redeemer learns expiry
        // A later voucher record carries registered_until 0 again.
        store.record(&st)?;
        let got = store.get(st.key())?.ok_or(StoreError::Corrupt {
            pool_id: None,
            detail: "missing".into(),
        })?;
        assert_eq!(
            got.registered_until, 1_800_000_000,
            "record must not clobber to 0"
        );
        Ok(())
    }

    #[test]
    fn set_registered_until_is_monotone() -> Result<(), StoreError> {
        let store = MemoryPoolStateStore::new();
        let st = sample(3, 4);
        store.record(&st)?;
        store.set_registered_until(st.key(), 100)?;
        store.set_registered_until(st.key(), 50)?; // lower: no-op
        let got = store.get(st.key())?.ok_or(StoreError::Corrupt {
            pool_id: None,
            detail: "x".into(),
        })?;
        assert_eq!(got.registered_until, 100);
        Ok(())
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
            crate::lane::LaneChain::NONE,
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

    #[test]
    fn memory_store_flush_is_noop_ok() -> anyhow::Result<()> {
        let store = MemoryPoolStateStore::new();
        store.record(&sample(1, 1))?;
        // Volatile store: flush has nothing to do and must succeed.
        store.flush()?;
        anyhow::ensure!(store.len() == 1);
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

    #[test]
    fn floor_loss_store_round_trip_and_forget() -> anyhow::Result<()> {
        let store = MemoryPoolFloorLossStore::new();
        let a = b256!("0000000000000000000000000000000000000000000000000000000000000011");
        let b = b256!("0000000000000000000000000000000000000000000000000000000000000022");
        store.record_loss(a, 400)?;
        store.record_loss(b, 4_000_000)?;
        // A higher total raises the stored value.
        store.record_loss(a, 800)?;
        let mut all = store.load_losses()?;
        all.sort_by_key(|(k, _)| *k);
        anyhow::ensure!(all.len() == 2);
        anyhow::ensure!(all.first() == Some(&(a, 800u128)));
        store.forget_loss(a)?;
        anyhow::ensure!(store.load_losses()?.len() == 1);
        // Forgetting an unknown pool is a no-op.
        store.forget_loss(a)?;
        Ok(())
    }

    /// A late, smaller total — two floor-reservation drops on one pool landing out
    /// of order — leaves the larger stored total in place, and `forget_loss` is the
    /// only way back down.
    #[test]
    fn floor_loss_store_never_regresses() -> anyhow::Result<()> {
        let store = MemoryPoolFloorLossStore::new();
        let pool = b256!("0000000000000000000000000000000000000000000000000000000000000077");
        store.record_loss(pool, 5_000)?;
        store.record_loss(pool, 10)?;
        anyhow::ensure!(store.load_losses()? == vec![(pool, 5_000u128)]);
        store.record_loss(pool, 5_001)?;
        anyhow::ensure!(store.load_losses()? == vec![(pool, 5_001u128)]);
        store.forget_loss(pool)?;
        store.record_loss(pool, 10)?;
        anyhow::ensure!(
            store.load_losses()? == vec![(pool, 10u128)],
            "forget clears the row, so the next total starts fresh"
        );
        Ok(())
    }
}
