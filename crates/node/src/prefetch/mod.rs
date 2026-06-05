//! Speculative-prefetch operator policy (ADR 022 §Popularity Signals and
//! Market Dynamics). Off-by-default; the `FIND_VALUE` handler feeds the
//! [`popularity::PopularityTracker`] and, on a threshold-cross, consults the
//! [`decision::PrefetchPolicy`]. This crate slice decides and meters but does
//! not yet fire the real acquisition (see #650 follow-up).

pub mod decision;
pub mod popularity;

use std::sync::{Arc, Mutex};

use decdn_common::config::ResolvedPrefetch;

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
}

impl PrefetchEngine {
    /// Construct from resolved config and the origin directory used for the
    /// authorized-origin gate.
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
    /// engine. Returns the outcome for metrics/logging. Does NOT perform any
    /// network acquisition (that is the #650 follow-up).
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

#[cfg(test)]
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
}
