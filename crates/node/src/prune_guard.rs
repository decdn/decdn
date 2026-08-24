//! Shared single-flight guard for sweeps that must not run concurrently.
//!
//! Three callers bound work behind an [`AtomicBool`]: the winner of the
//! `false -> true` CAS owns the sweep and must reset the flag to `false` when it
//! finishes. [`PruneGuard`] is that RAII reset.
//!
//! - [`crate::dispatch::ConnectionLimiter`] (#440) and
//!   [`crate::rate_limit::ThreeLayerRateLimiter`] (#645) bound their keyed
//!   `governor` maps with a `retain_recent` sweep.
//! - [`crate::dht::publish::run_republish`] owns one lag sweep at a time, so a
//!   burst of dropped cache-commit events costs one store walk rather than one
//!   per lag.
//!
//! The reset has to survive a panic. Without it, a panic inside the sweep —
//! third-party `governor` or `iroh-blobs` code, or an allocation failure during
//! the `O(n)` walk — would leave the flag stuck `true` and permanently disable
//! that codepath for the lifetime of the process: the unbounded-keyspace
//! pathology on the limiter side, and on the republish side a node that stops
//! repairing its DHT records while the lag counter keeps climbing, which reads
//! as health. Because `Drop` runs during unwind, holding a `PruneGuard` across
//! the sweep releases the flag whether the sweep returns normally or panics.

use std::sync::atomic::{AtomicBool, Ordering};

/// RAII reset for a single-flight sweep flag. Holding one means
/// the holder owns the single-flight slot; on drop — including drop during panic
/// unwind — the flag is stored `false` with `Release` ordering. See the module
/// docs for the full rationale.
pub(crate) struct PruneGuard<'a>(pub(crate) &'a AtomicBool);

impl Drop for PruneGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
