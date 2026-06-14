//! Speculative-prefetch operator policy (ADR 022 §Popularity Signals and
//! Market Dynamics). Off-by-default; the `FIND_VALUE` handler feeds the
//! [`popularity::PopularityTracker`] and, on a threshold-cross, consults the
//! [`decision::PrefetchPolicy`]. On a [`decision::PrefetchDecision::Acquire`]
//! the engine fires a live, bounded background acquisition ([`acquirer`]) that
//! drives the cache pull-through machinery; the spend + served bytes flow back
//! into the policy ledgers (#820).

pub mod acquired;
pub mod acquirer;
pub mod decision;
pub mod popularity;

use std::sync::{Arc, Mutex};

use decdn_common::config::ResolvedPrefetch;

use self::acquired::PrefetchAcquiredSet;
use self::acquirer::{InflightSet, PrefetchAcquirer, PrefetchAcquirerDeps};
use self::decision::{PrefetchDecision, PrefetchPolicy};
use self::popularity::{MAX_TRACKED_HASHES, PopularityTracker};
use crate::dht::origin::{Hash, OriginDirectory};

/// Result of feeding one `FIND_VALUE` arrival to the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchOutcome {
    /// Prefetch is disabled; nothing happened.
    Inert,
    /// Observed, but the demand threshold was not reached.
    BelowThreshold,
    /// Threshold reached; the decision engine produced this outcome.
    Decided(PrefetchDecision),
}

/// Ties the popularity tracker + decision engine + origin directory together.
/// The DHT `FIND_VALUE` handler calls [`PrefetchEngine::on_find_value`] for
/// every inbound request; with `enabled == false` it is inert.
#[derive(Debug)]
pub struct PrefetchEngine {
    cfg: ResolvedPrefetch,
    tracker: Mutex<PopularityTracker>,
    policy: PrefetchPolicy,
    directory: Arc<dyn OriginDirectory>,
    /// Live background acquisition (#820). Unprovisioned (no-op) until the
    /// runtime injects the cache + pull deps after bring-up.
    acquirer: PrefetchAcquirer,
    /// Hashes whose local copy came from prefetch — consulted by the serve path
    /// to attribute served bytes to the demand-quality numerator.
    acquired: Arc<PrefetchAcquiredSet>,
    /// In-flight prefetch hashes: dedupe + the gate the acquisition observer
    /// consults before recording a pull's spend against the prefetch ledger.
    pending: Arc<InflightSet>,
}

impl PrefetchEngine {
    /// Construct from resolved config and the origin directory used for the
    /// authorized-origin gate. The acquirer starts unprovisioned; the runtime
    /// calls [`Self::provision_acquirer`] once the pull path exists.
    #[must_use]
    pub fn new(cfg: ResolvedPrefetch, directory: Arc<dyn OriginDirectory>) -> Self {
        let tracker = PopularityTracker::new(
            cfg.threshold_window_secs,
            cfg.find_value_threshold,
            MAX_TRACKED_HASHES,
        );
        Self {
            cfg,
            tracker: Mutex::new(tracker),
            policy: PrefetchPolicy::new(cfg),
            directory,
            acquirer: PrefetchAcquirer::new(),
            // Tag lifetime tracks the demand-quality window: once a blob ages
            // out of that window there is no value in attributing its serves.
            acquired: Arc::new(PrefetchAcquiredSet::new(cfg.demand_quality_window_secs)),
            pending: Arc::new(InflightSet::new()),
        }
    }

    /// Shared handle to the prefetch-acquired tag set.
    #[must_use]
    pub fn acquired(&self) -> Arc<PrefetchAcquiredSet> {
        Arc::clone(&self.acquired)
    }

    /// Build the acquirer's dependency set from the engine's shared state plus
    /// the runtime-supplied cache/concurrency/timeout, and provision the
    /// acquirer. Idempotent (write-once).
    pub fn provision_acquirer(
        &self,
        cache: decdn_cache::CacheEngine,
        metrics: Arc<crate::metrics::Metrics>,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.cfg.max_concurrent_acquisitions.max(1) as usize,
        ));
        self.acquirer.provision(PrefetchAcquirerDeps {
            cache,
            acquired: Arc::clone(&self.acquired),
            pending: Arc::clone(&self.pending),
            metrics,
            semaphore,
            timeout: std::time::Duration::from_secs(self.cfg.acquisition_timeout_secs.max(1)),
            cancel,
        });
    }

    /// Fire a live acquisition for `hash` (the bridge `[u8; 32]` form). No-op
    /// when prefetch is disabled or the acquirer is unprovisioned. Non-blocking.
    pub fn try_acquire(&self, hash: [u8; 32]) {
        if !self.cfg.enabled {
            return;
        }
        self.acquirer.spawn_acquire(hash);
    }

    /// Record a completed pull's spend/bytes against the prefetch ledger **iff**
    /// the pull was prefetch-initiated (its hash is in flight as a prefetch).
    /// Returns whether it was recorded — the caller meters spend only then.
    /// Called by the acquisition observer from the node-origin pull path.
    pub fn record_acquisition_if_pending(
        &self,
        hash: [u8; 32],
        micro_usdc: u64,
        bytes: u64,
    ) -> bool {
        if !self.pending.contains(&hash) {
            return false;
        }
        self.policy
            .record_acquisition(micro_usdc, bytes, crate::payment_settlement::unix_now());
        true
    }

    /// From the serve path: if `hash` is prefetch-acquired content, credit
    /// `bytes` to the demand-quality numerator (`record_served`).
    pub fn note_served_if_prefetched(&self, hash: [u8; 32], bytes: u64, now: u64) {
        if self.acquired.contains(&hash, now) {
            self.policy.record_served(bytes, now);
        }
    }

    /// Whether prefetch is enabled (drives the `decdn_prefetch_enabled` gauge).
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.cfg.enabled
    }

    /// Whether the authorized-origin gate is active (drives the
    /// authorized-vs-bypassed metric split).
    #[must_use]
    pub const fn policy_requires_origin(&self) -> bool {
        self.cfg.require_authorized_origin
    }

    /// Borrow the decision engine (metrics readers + the follow-up acquisition
    /// path feed it via `record_*`).
    #[must_use]
    pub const fn policy(&self) -> &PrefetchPolicy {
        &self.policy
    }

    /// Feed one `FIND_VALUE` arrival for `hash_bytes` at `now` (seconds).
    /// Records the demand signal and, on a threshold-cross, runs the decision
    /// engine, returning the outcome for metrics/logging. The caller fires the
    /// live acquisition via [`Self::try_acquire`] on an `Acquire` decision.
    pub fn on_find_value(&self, hash_bytes: &[u8; 32], now: u64) -> PrefetchOutcome {
        if !self.cfg.enabled {
            return PrefetchOutcome::Inert;
        }
        let Ok(mut tracker) = self.tracker.lock() else {
            tracing::error!("prefetch on_find_value: tracker mutex poisoned");
            return PrefetchOutcome::Inert;
        };
        let triggered = tracker.observe(hash_bytes, now);
        drop(tracker);
        if !triggered {
            return PrefetchOutcome::BelowThreshold;
        }
        let hash = Hash::from_bytes(*hash_bytes);
        let decision = self.policy.decide(&hash, &*self.directory, now);
        PrefetchOutcome::Decided(decision)
    }
}

/// Bridges the node-origin paid-pull path to the prefetch ledger (#820).
/// Installed on `NodeOriginDeps`; fires on every pull that spent (success or
/// paid-but-failed). It records spend only for prefetch-initiated pulls
/// (filtered via the engine's in-flight set), so demand-driven cache-miss pulls
/// never charge the prefetch budget.
#[derive(Debug)]
pub struct PrefetchAcquisitionObserver {
    engine: Arc<PrefetchEngine>,
    metrics: Arc<crate::metrics::Metrics>,
}

impl PrefetchAcquisitionObserver {
    /// Construct from the shared engine and node metrics.
    #[must_use]
    pub const fn new(engine: Arc<PrefetchEngine>, metrics: Arc<crate::metrics::Metrics>) -> Self {
        Self { engine, metrics }
    }
}

impl crate::node_origin::AcquisitionObserver for PrefetchAcquisitionObserver {
    fn on_pull(&self, hash: [u8; 32], micro_usdc: u64, bytes: u64) {
        if self
            .engine
            .record_acquisition_if_pending(hash, micro_usdc, bytes)
        {
            self.metrics.add_prefetch_spend(micro_usdc);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::sync::Arc;

    use decdn_common::config::ResolvedPrefetch;

    use super::{PrefetchEngine, PrefetchOutcome};
    use crate::dht::origin::{ConfigOriginDirectory, Hash, OriginDirectory};
    use crate::dht::routing::NodeId;

    fn dir_with(hash: Hash) -> Arc<dyn OriginDirectory> {
        let mut m = std::collections::HashMap::new();
        m.insert(hash, vec![NodeId::from_bytes([1u8; 32])]);
        Arc::new(ConfigOriginDirectory::new(m))
    }

    #[test]
    fn disabled_engine_never_triggers() {
        let cfg = ResolvedPrefetch::default(); // enabled = false
        let key = [9u8; 32];
        let dir: Arc<dyn OriginDirectory> =
            Arc::new(ConfigOriginDirectory::new(std::collections::HashMap::new()));
        let engine = PrefetchEngine::new(cfg, dir);
        for t in 0..10 {
            assert_eq!(engine.on_find_value(&key, t), PrefetchOutcome::Inert);
        }
    }

    #[test]
    fn enabled_engine_triggers_and_decides() {
        let cfg = ResolvedPrefetch {
            enabled: true,
            find_value_threshold: 3,
            budget_usdc_per_hour: 1_000_000,
            ..Default::default()
        };
        let key = [9u8; 32];
        let dir = dir_with(Hash::from_bytes(key));
        let engine = PrefetchEngine::new(cfg, dir);
        assert_eq!(
            engine.on_find_value(&key, 0),
            PrefetchOutcome::BelowThreshold
        );
        assert_eq!(
            engine.on_find_value(&key, 1),
            PrefetchOutcome::BelowThreshold
        );
        assert_eq!(
            engine.on_find_value(&key, 2),
            PrefetchOutcome::Decided(super::decision::PrefetchDecision::Acquire)
        );
    }

    fn enabled_engine() -> PrefetchEngine {
        let cfg = ResolvedPrefetch {
            enabled: true,
            budget_usdc_per_hour: 1_000_000,
            ..Default::default()
        };
        let dir: Arc<dyn OriginDirectory> =
            Arc::new(ConfigOriginDirectory::new(std::collections::HashMap::new()));
        PrefetchEngine::new(cfg, dir)
    }

    #[test]
    fn record_acquisition_skipped_when_not_inflight() {
        let engine = enabled_engine();
        // No prefetch pull is in flight for this hash, so a node-origin pull's
        // observer callback must NOT charge the prefetch ledger.
        assert!(!engine.record_acquisition_if_pending([1u8; 32], 100, 1_000));
        // acquired stays 0 => ratio reports the healthy 1.0 baseline.
        assert!((engine.policy().demand_quality_ratio(0) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn note_served_credits_only_prefetched_hashes() {
        let engine = enabled_engine();
        let h = [5u8; 32];
        // Baseline acquisition so the ratio has a non-zero denominator.
        engine.policy().record_acquisition(0, 1_000, 0);
        // Untagged hash: served bytes are ignored (ratio stays 0/1000 = 0).
        engine.note_served_if_prefetched(h, 500, 0);
        assert!(engine.policy().demand_quality_ratio(0).abs() < f64::EPSILON);
        // Tag the hash as prefetch-acquired, then the same call credits it.
        engine.acquired().insert(h, 0);
        engine.note_served_if_prefetched(h, 500, 0);
        assert!((engine.policy().demand_quality_ratio(0) - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn disabled_engine_try_acquire_is_noop() {
        // A disabled engine must never spawn an acquisition (and must not panic
        // outside a runtime). `try_acquire` returns before touching the acquirer.
        let engine = PrefetchEngine::new(
            ResolvedPrefetch::default(),
            Arc::new(ConfigOriginDirectory::new(std::collections::HashMap::new())),
        );
        engine.try_acquire([7u8; 32]);
    }

    #[test]
    fn observer_does_not_charge_demand_miss_pulls() {
        // Spend isolation (the whole reason the observer fires for ALL pulls):
        // a node-origin pull NOT initiated by prefetch — i.e. its hash is not in
        // the in-flight set — must not charge the prefetch budget or spend metric.
        // This drives the WIRED observer path, not the engine method directly.
        use crate::node_origin::AcquisitionObserver;
        let engine = Arc::new(enabled_engine());
        let metrics = Arc::new(crate::metrics::Metrics::new());
        let observer =
            super::PrefetchAcquisitionObserver::new(Arc::clone(&engine), Arc::clone(&metrics));
        // A demand-miss pull for a hash with no in-flight prefetch.
        observer.on_pull([8u8; 32], 5_000, 2_000);
        // Nothing recorded: ratio stays at the healthy 1.0 (acquired == 0) and the
        // spend counter never moved.
        assert!((engine.policy().demand_quality_ratio(0) - 1.0).abs() < f64::EPSILON);
        let scrape = metrics.encode().expect("encode metrics");
        let spend = scrape
            .lines()
            .find_map(|l| l.strip_prefix("decdn_prefetch_spend_usdc_total "))
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0);
        assert_eq!(spend, 0, "demand-miss pull must not charge prefetch spend");
    }
}
