//! Live speculative-prefetch acquisition (#820).
//!
//! On a [`super::decision::PrefetchDecision::Acquire`] the DHT handler calls
//! [`super::PrefetchEngine::try_acquire`], which delegates here. The acquirer
//! spawns a **bounded, detached** background task that drives the existing
//! cache pull-through machinery ([`decdn_cache::CacheEngine::populate`] → the
//! node-to-node `NodeOrigin`: DHT `find_providers` → `cdn/probe/v1` →
//! `cdn/client/v1` paid pull). The spend + bytes of that pull are fed back into
//! the [`super::decision::PrefetchPolicy`] ledgers by an
//! [`crate::node_origin::AcquisitionObserver`] (installed on `NodeOriginDeps`),
//! so the rolling-1h budget and demand-quality auto-throttle run on real data.
//!
//! Two bounds keep speculation from starving demand traffic:
//! - a [`Semaphore`] caps concurrent acquisitions (`max_concurrent_acquisitions`);
//! - an [`InflightSet`] dedupes per hash, and — because the acquirer task holds
//!   its in-flight guard across the pull — doubles as the "is this pull
//!   prefetch-initiated?" gate the observer consults before recording spend.
//!
//! Deps are injected once, after runtime bring-up (the cache + node-origin pull
//! path do not exist when the engine is first constructed), via a write-once
//! [`OnceLock`] mirroring [`crate::node_origin::NodeOrigin::provision`]. Until
//! provisioned, [`PrefetchAcquirer::spawn_acquire`] is a no-op.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use decdn_cache::{CacheEngine, Hash as CacheHash};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use super::acquired::PrefetchAcquiredSet;
use crate::metrics::Metrics;

/// In-flight prefetch hashes — per-hash dedupe AND the "prefetch-initiated"
/// marker the acquisition observer consults. An armed hash stays present for the
/// lifetime of its `InflightGuard`, which the acquisition task holds across
/// the whole pull, so the observer sees `true` exactly while a prefetch pull for
/// that hash is in flight.
#[derive(Debug, Default)]
pub struct InflightSet {
    inner: Mutex<HashSet<[u8; 32]>>,
}

impl InflightSet {
    /// New empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark `hash` in flight, returning a guard that clears it on drop, or
    /// `None` if it is already in flight (caller should skip — dedupe).
    fn arm(self: &Arc<Self>, hash: [u8; 32]) -> Option<InflightGuard> {
        let mut set = self.inner.lock().ok()?;
        if !set.insert(hash) {
            return None;
        }
        Some(InflightGuard {
            set: Arc::clone(self),
            hash,
        })
    }

    /// Whether `hash` currently has a prefetch pull in flight. A poisoned lock
    /// returns `false` so a fault never mis-attributes a demand pull's spend to
    /// the prefetch ledger (the safe direction: under-count prefetch spend).
    ///
    /// Note this gates on the *hash*, not a specific task: if a demand-miss pull
    /// and a prefetch acquisition for the same hash race, `CacheEngine::populate`
    /// coalesces them into one network pull whose observer fire is attributed to
    /// the prefetch ledger while this hash is armed. That over-attributes one
    /// pull's spend to prefetch — bounded by `max_concurrent_acquisitions` and
    /// rare — the one direction that can over-count rather than under-count.
    #[must_use]
    pub fn contains(&self, hash: &[u8; 32]) -> bool {
        self.inner.lock().is_ok_and(|s| s.contains(hash))
    }
}

/// RAII guard removing its hash from the [`InflightSet`] on drop.
struct InflightGuard {
    set: Arc<InflightSet>,
    hash: [u8; 32],
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.set.inner.lock() {
            set.remove(&self.hash);
        }
    }
}

/// Dependencies for live acquisition, injected post-bring-up.
pub struct PrefetchAcquirerDeps {
    /// Cache engine whose pull-through (`populate`) drives the network pull and
    /// ingests the blob.
    pub cache: CacheEngine,
    /// Shared set tagging prefetch-acquired hashes for the serve path.
    pub acquired: Arc<PrefetchAcquiredSet>,
    /// Shared in-flight set (dedupe + observer gate).
    pub pending: Arc<InflightSet>,
    /// Node metrics for acquisition-outcome counters.
    pub metrics: Arc<Metrics>,
    /// Concurrency cap on background acquisitions.
    pub semaphore: Arc<Semaphore>,
    /// Per-acquisition wall-clock deadline.
    pub timeout: Duration,
    /// Cancelled on shutdown to abandon in-flight acquisitions promptly.
    pub cancel: CancellationToken,
}

impl std::fmt::Debug for PrefetchAcquirerDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefetchAcquirerDeps")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// Spawns bounded background acquisitions. Cheap to construct unprovisioned.
#[derive(Debug)]
pub struct PrefetchAcquirer {
    deps: Arc<OnceLock<PrefetchAcquirerDeps>>,
}

impl PrefetchAcquirer {
    /// Build an unprovisioned acquirer. [`Self::spawn_acquire`] is a no-op until
    /// [`Self::provision`] supplies the dependencies.
    #[must_use]
    pub fn new() -> Self {
        Self {
            deps: Arc::new(OnceLock::new()),
        }
    }

    /// Supply the dependencies, enabling acquisition. Write-once: a second call
    /// is ignored with a warning (matches `NodeOrigin::provision`).
    pub fn provision(&self, deps: PrefetchAcquirerDeps) {
        if self.deps.set(deps).is_err() {
            tracing::warn!("PrefetchAcquirer provisioned more than once; keeping the first set");
        }
    }

    /// Spawn a bounded acquisition for `hash`. Non-blocking; returns immediately.
    /// No-op when unprovisioned, already in flight for this hash, or at the
    /// concurrency cap.
    pub fn spawn_acquire(&self, hash: [u8; 32]) {
        let Some(deps) = self.deps.get() else {
            return; // unprovisioned: feature off or pull-through unavailable
        };
        let Ok(permit) = Arc::clone(&deps.semaphore).try_acquire_owned() else {
            deps.metrics.prefetch_acquire_dropped_saturated();
            return;
        };
        let Some(guard) = deps.pending.arm(hash) else {
            return; // already in flight — dedupe (permit drops here, freeing a slot)
        };

        let cache = deps.cache.clone();
        let acquired = Arc::clone(&deps.acquired);
        let metrics = Arc::clone(&deps.metrics);
        let cancel = deps.cancel.clone();
        let timeout = deps.timeout;

        tokio::spawn(async move {
            // Hold the permit + in-flight guard for the whole pull: the guard
            // keeps `hash` in the pending set so the acquisition observer
            // attributes this pull's spend to the prefetch ledger.
            let _permit = permit;
            let _guard = guard;
            let cache_hash = CacheHash::from_bytes(hash);
            tokio::select! {
                biased;
                () = cancel.cancelled() => {}
                result = tokio::time::timeout(timeout, cache.populate(cache_hash)) => {
                    match result {
                        Ok(Ok(())) => {
                            // Tag for the serve path only if the blob is really
                            // resident now (populate ingested it). The ledger
                            // `record_acquisition` already fired via the observer
                            // on the successful paid pull.
                            if matches!(cache.has(cache_hash).await, Ok(true)) {
                                acquired.insert(hash, crate::payment_settlement::unix_now());
                                metrics.prefetch_acquire_succeeded();
                            } else {
                                metrics.prefetch_acquire_failed();
                            }
                        }
                        Ok(Err(err)) => {
                            tracing::debug!(%err, "prefetch acquisition pull-through failed");
                            metrics.prefetch_acquire_failed();
                        }
                        Err(_) => {
                            // Deadline elapsed: the populate future is dropped.
                            // A pull that already paid + acked recorded its spend
                            // via the observer (budget is charged), but the blob
                            // is NOT tagged in `acquired` here, so its later
                            // serves are not credited to the demand-quality
                            // numerator. That biases the ratio DOWN (toward more
                            // throttling) — the safe direction. Do NOT "fix" this
                            // by tagging on timeout: the blob may never have
                            // landed, which would over-credit serves.
                            tracing::debug!(?timeout, "prefetch acquisition hit its deadline");
                            metrics.prefetch_acquire_timeout();
                        }
                    }
                }
            }
        });
    }
}

impl Default for PrefetchAcquirer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::sync::Arc;

    use super::{InflightSet, PrefetchAcquirer};

    #[test]
    fn unprovisioned_spawn_is_noop() {
        // No tokio runtime needed: an unprovisioned acquirer returns before any
        // spawn, so this must not panic.
        let acq = PrefetchAcquirer::new();
        acq.spawn_acquire([7u8; 32]);
    }

    #[test]
    fn inflight_dedupes_and_clears_on_drop() {
        let set = Arc::new(InflightSet::new());
        let h = [3u8; 32];
        assert!(!set.contains(&h));
        let guard = set.arm(h).expect("first arm succeeds");
        assert!(set.contains(&h));
        // Second arm while in flight is refused.
        assert!(set.arm(h).is_none());
        drop(guard);
        assert!(!set.contains(&h));
    }
}
