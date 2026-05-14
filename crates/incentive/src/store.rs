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
/// semantics. Not durable — drops with the process. Production wiring uses
/// the redb-backed impl in `crates/node`.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{U256, address, b256};

    fn sample(channel_id_byte: u8) -> ChannelState {
        let mut bytes = [0u8; 32];
        bytes[31] = channel_id_byte;
        ChannelState {
            channel_id: bytes.into(),
            client: address!("00000000000000000000000000000000000000aa"),
            token: address!("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            deposit: U256::from(10_000_000u64),
            last_amount: U256::from(1_234u64),
            last_nonce: U256::from(7u64),
            last_bytes_delivered: U256::from(4_096u64),
        }
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
        let mut s = sample(1);
        store.record(&s)?;
        s.last_nonce = U256::from(99u64);
        store.record(&s)?;
        let all = store.load_all()?;
        anyhow::ensure!(all.len() == 1);
        let only = all.first().ok_or_else(|| anyhow::anyhow!("missing [0]"))?;
        anyhow::ensure!(only.last_nonce == U256::from(99u64));
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
}
