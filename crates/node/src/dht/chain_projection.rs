//! Shared scaffolding for the chain-backed DHT projections (#1255).
//!
//! Each of [`ChainStakerSet`](super::chain_staker_set::ChainStakerSet),
//! [`ChainNodeAddressDirectory`](super::node_address::ChainNodeAddressDirectory),
//! and [`ChainOriginDirectory`](super::chain_origin_directory::ChainOriginDirectory)
//! caches chain state behind an `RwLock` and reads it on the hot path with
//! poison-tolerant recovery. A refresh loop replaces the cache from a fresh
//! chain snapshot. This module factors the shared pieces out:
//!
//! - [`with_read`] / [`with_write`] — the poison-recovery lock idiom, one
//!   `warn!` site, `label` naming the projection.
//! - [`with_lock`] — the same idiom for a `Mutex`: the DHT republish scheduler's
//!   heap and its live-hash map, and the DHT routing table.
//! - [`ChainProjection`] — the façade-held bundle of the shared state, exposing
//!   the read seam.
//!
//! The write helper stays a free function over `&RwLock<T>` because the refresh
//! loop, which cannot reach back into the [`ChainProjection`], applies each
//! snapshot through its own `Arc<RwLock<T>>` clone made in `bootstrap`.

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

/// The shared state behind a chain-backed projection. Cheap to build via
/// [`from_parts`](ChainProjection::from_parts). The registry refresh loop
/// writes the state; the projection only reads it.
#[derive(Debug)]
pub(super) struct ChainProjection<T> {
    state: Arc<RwLock<T>>,
    label: &'static str,
}

impl<T> ChainProjection<T> {
    /// Assemble from the state the registry refresh also holds.
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
mod tests;
