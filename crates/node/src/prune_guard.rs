//! Shared single-flight guard for the keyed rate-limiter prune sweeps.
//!
//! Both [`crate::dispatch::ConnectionLimiter`] (#440) and
//! [`crate::rate_limit::ThreeLayerRateLimiter`] (#645) bound their keyed
//! `governor` maps with a `retain_recent` sweep that runs single-flighted
//! behind an [`AtomicBool`]: the thread that wins the `false -> true` CAS owns
//! the sweep and must reset the flag to `false` when it finishes.
//! [`PruneGuard`] is that RAII reset.
//!
//! The reset has to survive a panic. Without it, a panic inside `retain_recent`
//! — third-party `governor` code, or a future allocation failure during the
//! `O(n)` walk — would leave the flag stuck `true` and permanently disable the
//! prune codepath for the lifetime of the process, the exact unbounded-keyspace
//! pathology those two issues exist to prevent. Because `Drop` runs during
//! unwind, holding a `PruneGuard` across the sweep releases the flag whether the
//! sweep returns normally or panics.

use std::sync::atomic::{AtomicBool, Ordering};

/// RAII reset for a single-flight `retain_recent` prune flag. Holding one means
/// the holder owns the single-flight slot; on drop — including drop during panic
/// unwind — the flag is stored `false` with `Release` ordering. See the module
/// docs for the full rationale.
pub(crate) struct PruneGuard<'a>(pub(crate) &'a AtomicBool);

impl Drop for PruneGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
