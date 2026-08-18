//! Shared scaffolding for the chain-backed DHT projections (#1255).
//!
//! Each of [`ChainStakerSet`](super::chain_staker_set::ChainStakerSet),
//! [`ChainNodeAddressDirectory`](super::node_address::ChainNodeAddressDirectory),
//! and [`ChainOriginDirectory`](super::chain_origin_directory::ChainOriginDirectory)
//! caches chain state behind an `RwLock`, reads it on the hot path with
//! poison-tolerant recovery, republishes a size gauge only on a *real* mutation,
//! and owns the background watcher task that keeps the cache fresh. This module
//! factors those three pieces out:
//!
//! - [`with_read`] / [`with_write`] — the poison-recovery lock idiom, one
//!   `warn!` site, `label` naming the projection.
//! - [`mutate_gauged`] — mutate under the write lock, sample the size while
//!   still holding it, and republish the gauge only when the mutation actually
//!   changed the set (a re-scanned `eth_getLogs` window replays no-ops).
//! - [`ChainProjection`] — the façade-held bundle of the shared state and the
//!   owned [`WatcherHandle`] (#1236), exposing the read seam.
//!
//! The mutation helpers stay free functions over `&RwLock<T>` because the
//! *sink* — which lives inside the watcher task, so it cannot reach back into
//! the [`ChainProjection`] that owns the task — is what applies mutations,
//! holding its own `Arc<RwLock<T>>` clone made in `bootstrap`.

use std::sync::{Arc, RwLock};

use tracing::warn;

use crate::chain_events::resumable_watcher::WatcherHandle;

/// Poison-tolerant `RwLock` read. A poisoned lock means something panicked while
/// holding the write guard; the cached state is still structurally valid (no
/// panic is possible mid-mutation here), so recover the inner value rather than
/// propagate a panic into the read hot path. Log on the recovery arm so the
/// original panic surfaces somewhere, matching the precedent in
/// `cache/src/engine.rs`.
pub(crate) fn with_read<T, R>(state: &RwLock<T>, label: &str, f: impl FnOnce(&T) -> R) -> R {
    match state.read() {
        Ok(guard) => f(&guard),
        Err(poisoned) => {
            warn!("{label} RwLock poisoned; recovering inner state");
            f(&poisoned.into_inner())
        }
    }
}

/// Poison-tolerant `RwLock` write, mirroring [`with_read`].
pub(super) fn with_write<T, R>(state: &RwLock<T>, label: &str, f: impl FnOnce(&mut T) -> R) -> R {
    match state.write() {
        Ok(mut guard) => f(&mut guard),
        Err(poisoned) => {
            warn!("{label} RwLock poisoned; recovering inner state");
            f(&mut poisoned.into_inner())
        }
    }
}

/// Apply `mutate` under the write lock and republish a size gauge only on a real
/// change. `mutate` returns `Some(sample)` when it actually changed the state
/// (and the size to publish, sampled while the lock is held so the gauge never
/// observes a torn view) or `None` for an idempotent no-op — a re-insert or an
/// absent-remove, which a re-scanned `eth_getLogs` window produces routinely.
/// Returns whether it mutated, mirroring the underlying collection op's bool.
pub(super) fn mutate_gauged<T, S>(
    state: &RwLock<T>,
    label: &str,
    mutate: impl FnOnce(&mut T) -> Option<S>,
    gauge: impl FnOnce(S),
) -> bool {
    let sample = with_write(state, label, mutate);
    match sample {
        Some(sample) => {
            gauge(sample);
            true
        }
        None => false,
    }
}

/// The shared state plus the owned watcher handle behind a chain-backed
/// projection. Cheap to build via [`from_parts`](ChainProjection::from_parts);
/// the watcher is an `Arc<WatcherHandle>` so the capacity-bond registry can feed
/// one loop into both the staker-set and address-binding projections, the task
/// living while either façade does.
#[derive(Debug)]
pub(super) struct ChainProjection<T> {
    state: Arc<RwLock<T>>,
    label: &'static str,
    /// Held purely for its `Arc` refcount: as long as a projection built from
    /// this handle is alive, the shared watcher task it points at is too. No
    /// accessor reads it back — the runtime drives graceful shutdown through
    /// the `Arc<WatcherHandle>` it keeps directly from `bootstrap` instead.
    #[allow(dead_code)]
    watcher: Arc<WatcherHandle>,
}

impl<T> ChainProjection<T> {
    /// Assemble from the state the sink also holds (created in `bootstrap`) and
    /// the watcher that keeps it fresh.
    pub(super) const fn from_parts(
        state: Arc<RwLock<T>>,
        label: &'static str,
        watcher: Arc<WatcherHandle>,
    ) -> Self {
        Self {
            state,
            label,
            watcher,
        }
    }

    /// Poison-tolerant read of the cached state.
    pub(super) fn read<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        with_read(&self.state, self.label, f)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use std::collections::HashSet;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;

    /// The poison-recovery arms in [`with_read`] / [`with_write`] /
    /// [`mutate_gauged`] had no coverage before the consolidation (#1255). Poison
    /// the lock, then confirm every accessor still returns the inner state rather
    /// than propagating the panic into the read/apply hot path.
    #[test]
    fn accessors_recover_a_poisoned_lock() {
        let state = RwLock::new(HashSet::<u8>::from([1, 2]));

        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _guard = state.write().unwrap();
            panic!("poison the lock while holding the write guard");
        }));
        assert!(poisoned.is_err());
        assert!(state.is_poisoned());

        // Read recovers.
        assert_eq!(with_read(&state, "test", HashSet::len), 2);

        // Mutate + gauge recovers, and only fires on a real change.
        let mut gauged = None;
        let mutated = mutate_gauged(
            &state,
            "test",
            |s| s.insert(3).then_some(s.len()),
            |n| gauged = Some(n),
        );
        assert!(mutated);
        assert_eq!(gauged, Some(3));

        // A no-op re-insert leaves the gauge unpublished.
        let mut gauged_noop = None;
        let mutated_noop = mutate_gauged(
            &state,
            "test",
            |s| s.insert(3).then_some(s.len()),
            |n| gauged_noop = Some(n),
        );
        assert!(!mutated_noop);
        assert_eq!(gauged_noop, None);
    }
}
