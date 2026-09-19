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
//! - [`with_lock`] — the same idiom for a `Mutex`: the DHT republish scheduler's
//!   heap and its live-hash map, and the DHT routing table.
//! - [`mutate_gauged`] — mutate under the write lock, sample the size while
//!   still holding it, and republish the gauge only when the mutation actually
//!   changed the set (a re-scanned `eth_getLogs` window replays no-ops).
//! - [`ChainProjection`] — the façade-held bundle of the shared state, exposing
//!   the read seam.
//!
//! The mutation helpers stay free functions over `&RwLock<T>` because the
//! *sink* — which lives inside the shared poller task, so it cannot reach back
//! into the [`ChainProjection`] — is what applies mutations, holding its own
//! `Arc<RwLock<T>>` clone made in `bootstrap`.

use std::sync::{Arc, Mutex, RwLock};

use tracing::warn;

/// Poison-tolerant `RwLock` read. A poisoned lock means something panicked while
/// holding the write guard; the cached state is still structurally valid (no
/// panic is possible mid-mutation here), so recover the inner value rather than
/// propagate a panic into the read hot path. Log on the recovery arm so the
/// original panic surfaces somewhere, matching the precedent in
/// `cache/src/engine.rs`.
///
/// The recovery arm clears the poison, so the warning is one line per panic
/// rather than one per acquisition. Poison is otherwise sticky, and these locks
/// sit on paths that run per inbound request and on a 1 Hz timer — an ungated
/// log there is the megabytes-per-second flood `dispatch`'s own poison gate
/// exists to prevent.
pub(crate) fn with_read<T, R>(state: &RwLock<T>, label: &str, f: impl FnOnce(&T) -> R) -> R {
    match state.read() {
        Ok(guard) => f(&guard),
        Err(poisoned) => {
            warn!(lock = label, "RwLock poisoned; recovering inner state");
            state.clear_poison();
            f(&poisoned.into_inner())
        }
    }
}

/// Poison-tolerant `RwLock` write, mirroring [`with_read`].
pub(super) fn with_write<T, R>(state: &RwLock<T>, label: &str, f: impl FnOnce(&mut T) -> R) -> R {
    match state.write() {
        Ok(mut guard) => f(&mut guard),
        Err(poisoned) => {
            warn!(lock = label, "RwLock poisoned; recovering inner state");
            state.clear_poison();
            f(&mut poisoned.into_inner())
        }
    }
}

/// Poison-tolerant `Mutex` lock, mirroring [`with_write`] for state behind a
/// plain `Mutex`. Nest two calls to hold a pair of locks; the outer call is
/// taken first, so the nesting order is the lock order.
///
/// `pub(crate)` rather than `pub(super)` because the DHT routing table is
/// locked from the request handler too, and every acquisition of one `Mutex`
/// must share one recovery idiom: a site that recovers less than the others
/// turns a single panic into split-brain, where the republisher keeps working
/// on a table the query path answers empty for.
pub(crate) fn with_lock<T, R>(lock: &Mutex<T>, label: &str, f: impl FnOnce(&mut T) -> R) -> R {
    match lock.lock() {
        Ok(mut guard) => f(&mut guard),
        Err(poisoned) => {
            warn!(lock = label, "Mutex poisoned; recovering inner state");
            lock.clear_poison();
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

/// The shared state behind a chain-backed projection. Cheap to build via
/// [`from_parts`](ChainProjection::from_parts). The background loop that keeps
/// the state fresh is the single [`multiplexed_poller`] task the runtime owns
/// and shuts down; the projection no longer holds a per-watcher handle.
///
/// [`multiplexed_poller`]: crate::chain_events::multiplexed_poller
#[derive(Debug)]
pub(super) struct ChainProjection<T> {
    state: Arc<RwLock<T>>,
    label: &'static str,
}

impl<T> ChainProjection<T> {
    /// Assemble from the state the sink also holds (created in `bootstrap`).
    pub(super) const fn from_parts(state: Arc<RwLock<T>>, label: &'static str) -> Self {
        Self { state, label }
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

    /// [`with_lock`] recovers a poisoned `Mutex` the same way, including through
    /// a nested pair — the shape the republish scheduler holds its heap and its
    /// live-hash map in.
    #[test]
    fn with_lock_recovers_a_poisoned_mutex() {
        let outer = Mutex::new(vec![1u8, 2]);
        let inner = Mutex::new(HashSet::<u8>::from([1, 2]));

        let poisoned = catch_unwind(AssertUnwindSafe(|| {
            let _outer = outer.lock().unwrap();
            let _inner = inner.lock().unwrap();
            panic!("poison both locks while holding the guards");
        }));
        assert!(poisoned.is_err());
        assert!(outer.is_poisoned());
        assert!(inner.is_poisoned());

        let total = with_lock(&outer, "test outer", |o| {
            o.push(3);
            with_lock(&inner, "test inner", |i| {
                i.insert(3);
                o.len() + i.len()
            })
        });
        assert_eq!(total, 6);
    }
}
