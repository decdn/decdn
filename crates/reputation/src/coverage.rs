//! Per-operator regional-coverage signal (ADR 008 §Regional-Coverage
//! Reputation Signal).
//!
//! A derived metric — NOT a component of `final_score` and not an input to any
//! on-chain payout — summarizing where an operator's verified deliveries
//! originate geographically. Computed locally from the same gossip
//! [`crate::network::ReportInput`]s that drive the network score, keyed by the
//! reporter's self-declared region. Per-consumer and non-convergent, exactly
//! like the network score.

use crate::local::ConfigError;
use crate::network::{NetworkReputationConfig, decay_to};
use crate::settlement::SECONDS_PER_WEEK;
use iroh::PublicKey as NodeId;
use std::collections::HashMap;
use std::sync::RwLock;

const COVERAGE_NEUTRAL: f64 = 0.5;
const COVERAGE_NEUTRAL_F32: f32 = 0.5;
/// Buckets within this band of neutral are eviction candidates.
const EVICT_NEUTRAL_BAND: f64 = 0.05;
/// Buckets idle longer than this (and near neutral) may be evicted.
const EVICT_IDLE_WEEKS: f64 = 26.0;

/// A single region's coverage state for one operator (ADR 008 §Storage and
/// aggregation).
#[derive(Debug, Clone, Copy)]
pub struct CoverageBucket {
    /// Coverage score in `[0, 1]`, same scale as `final_score`.
    pub score: f32,
    /// Unix seconds of the most recent update.
    pub last_interaction_at: u64,
}

/// Per-operator sparse regional-coverage map. Cheap to share via
/// [`std::sync::Arc`].
#[derive(Debug)]
pub struct RegionalCoverage {
    config: NetworkReputationConfig,
    map: RwLock<HashMap<NodeId, HashMap<[u8; 2], CoverageBucket>>>,
}

impl RegionalCoverage {
    /// Build a new coverage map sharing the network aggregation config,
    /// validating it (same guard as [`crate::network::NetworkReputation::new`]
    /// — an out-of-range `alpha`/`max_delta`/`decay_rate` would otherwise let a
    /// `NaN` escape into a bucket score, violating the `[0, 1]` invariant).
    pub fn new(config: NetworkReputationConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self {
            config,
            map: RwLock::new(HashMap::new()),
        })
    }

    /// Fold one interaction into `operator`'s coverage for `region` (ADR 008
    /// §Update rule). `interaction_score` is the same value used for the
    /// network score; `weight` is the reporter weight. A non-positive weight is
    /// a no-op. The caller resolves `region` from the reporter's `NodeAnnounce`
    /// and MUST NOT call this when the reporter has no attested region (ADR 008
    /// step 1: such reports are dropped from regional aggregation only).
    pub fn record(
        &self,
        operator: NodeId,
        region: [u8; 2],
        interaction_score: f64,
        weight: f64,
        now_secs: u64,
    ) {
        if !weight.is_finite() || weight <= 0.0 {
            return;
        }
        let mut guard = self
            .map
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let bucket = guard
            .entry(operator)
            .or_default()
            .entry(region)
            .or_insert(CoverageBucket {
                score: COVERAGE_NEUTRAL_F32,
                last_interaction_at: now_secs,
            });
        // Clamp the effective "now" so an out-of-order or backward-clock report
        // never rewinds `last_interaction_at` into the past — which would make a
        // later read apply extra decay. `decay_to` already saturates elapsed at
        // 0; this keeps the *stored* timestamp monotonic too.
        let effective_now = now_secs.max(bucket.last_interaction_at);
        let decayed = decay_to(
            f64::from(bucket.score),
            bucket.last_interaction_at,
            effective_now,
            COVERAGE_NEUTRAL,
            self.config.decay_rate_per_week,
        );
        let alpha = (self.config.alpha * weight).clamp(0.0, 1.0);
        let next = (1.0 - alpha) * decayed + alpha * interaction_score;
        let delta = (next - decayed).clamp(
            -self.config.max_delta_per_update,
            self.config.max_delta_per_update,
        );
        let updated = (decayed + delta).clamp(0.0, 1.0);
        // updated ∈ [0,1]; narrowing to f32 is well within range.
        #[allow(clippy::cast_possible_truncation)]
        {
            bucket.score = updated as f32;
        }
        bucket.last_interaction_at = effective_now;
    }

    /// `operator`'s lazily-decayed coverage for `region`, or `None` if there is
    /// no signal (never seen or evicted) — distinct from "decayed to neutral".
    pub fn coverage(&self, operator: NodeId, region: [u8; 2], now_secs: u64) -> Option<f32> {
        let guard = self
            .map
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let bucket = guard.get(&operator)?.get(&region)?;
        Some(decayed_f32(
            bucket,
            now_secs,
            self.config.decay_rate_per_week,
        ))
    }

    /// All regions `operator` currently has a signal for, with lazily-decayed
    /// scores (ADR 008 §Consumer access).
    pub fn covered_regions(&self, operator: NodeId, now_secs: u64) -> Vec<([u8; 2], f32)> {
        let guard = self
            .map
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.get(&operator).map_or_else(Vec::new, |regions| {
            regions
                .iter()
                .map(|(region, bucket)| {
                    (
                        *region,
                        decayed_f32(bucket, now_secs, self.config.decay_rate_per_week),
                    )
                })
                .collect()
        })
    }

    /// Storage-cleanup pass (ADR 008 §Decay): evict buckets whose decayed score
    /// is within 0.05 of neutral AND whose last interaction is older than 26
    /// weeks, so the next lookup returns "no signal" rather than "neutral".
    pub fn evict(&self, now_secs: u64) {
        let mut guard = self
            .map
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for regions in guard.values_mut() {
            regions.retain(|_, bucket| {
                let decayed = decay_to(
                    f64::from(bucket.score),
                    bucket.last_interaction_at,
                    now_secs,
                    COVERAGE_NEUTRAL,
                    self.config.decay_rate_per_week,
                );
                #[allow(clippy::cast_precision_loss)]
                let idle_weeks =
                    now_secs.saturating_sub(bucket.last_interaction_at) as f64 / SECONDS_PER_WEEK;
                let near_neutral = (decayed - COVERAGE_NEUTRAL).abs() <= EVICT_NEUTRAL_BAND;
                !(near_neutral && idle_weeks > EVICT_IDLE_WEEKS)
            });
        }
        guard.retain(|_, regions| !regions.is_empty());
    }
}

fn decayed_f32(bucket: &CoverageBucket, now_secs: u64, decay_rate: f64) -> f32 {
    let decayed = decay_to(
        f64::from(bucket.score),
        bucket.last_interaction_at,
        now_secs,
        COVERAGE_NEUTRAL,
        decay_rate,
    );
    // decayed ∈ [0,1]; narrowing to f32 is well within range.
    #[allow(clippy::cast_possible_truncation)]
    {
        decayed as f32
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use anyhow::ensure;
    use iroh::SecretKey;

    fn op() -> NodeId {
        SecretKey::generate().public()
    }

    const DE: [u8; 2] = *b"DE";
    const US: [u8; 2] = *b"US";

    #[test]
    fn invalid_config_rejected() {
        let bad = NetworkReputationConfig {
            decay_rate_per_week: f64::NAN,
            ..Default::default()
        };
        assert!(
            RegionalCoverage::new(bad).is_err(),
            "RegionalCoverage must validate its config like NetworkReputation"
        );
    }

    #[test]
    fn unknown_operator_has_no_signal() {
        let rc = RegionalCoverage::new(NetworkReputationConfig::default()).expect("valid cfg");
        assert!(rc.coverage(op(), DE, 0).is_none());
        assert!(rc.covered_regions(op(), 0).is_empty());
    }

    #[test]
    fn record_initializes_and_moves_from_neutral() -> anyhow::Result<()> {
        let rc = RegionalCoverage::new(NetworkReputationConfig::default()).expect("valid cfg");
        let o = op();
        rc.record(o, DE, 1.0, 3.0, 0);
        let c = rc
            .coverage(o, DE, 0)
            .ok_or_else(|| anyhow::anyhow!("no signal"))?;
        // From 0.5 toward 1.0 with clamp +0.05 → 0.55.
        ensure!((c - 0.55).abs() < 1e-4, "got {c}");
        Ok(())
    }

    #[test]
    fn out_of_order_report_does_not_rewind_decay_clock() -> anyhow::Result<()> {
        let rc = RegionalCoverage::new(NetworkReputationConfig::default()).expect("valid cfg");
        let week = crate::settlement::SECONDS_PER_WEEK_U64;
        let o = op();
        // Establish a signal at t = 10 weeks.
        rc.record(o, DE, 1.0, 3.0, 10 * week);
        let baseline = rc
            .coverage(o, DE, 10 * week)
            .ok_or_else(|| anyhow::anyhow!("no signal"))?;
        // A delayed (out-of-order) positive report timestamped at t=0 must not
        // rewind `last_interaction_at` to 0; otherwise coverage(10wk) would
        // over-decay back toward neutral.
        rc.record(o, DE, 1.0, 3.0, 0);
        let after = rc
            .coverage(o, DE, 10 * week)
            .ok_or_else(|| anyhow::anyhow!("no signal"))?;
        ensure!(
            after >= baseline - 1e-6,
            "out-of-order report over-decayed coverage: {after} < {baseline}"
        );
        Ok(())
    }

    #[test]
    fn zero_weight_is_noop() {
        let rc = RegionalCoverage::new(NetworkReputationConfig::default()).expect("valid cfg");
        let o = op();
        rc.record(o, DE, 1.0, 0.0, 0);
        assert!(rc.coverage(o, DE, 0).is_none());
    }

    #[test]
    fn distinct_regions_tracked_separately() -> anyhow::Result<()> {
        let rc = RegionalCoverage::new(NetworkReputationConfig::default()).expect("valid cfg");
        let o = op();
        rc.record(o, DE, 1.0, 3.0, 0);
        rc.record(o, US, 0.0, 3.0, 0);
        let regions = rc.covered_regions(o, 0);
        ensure!(regions.len() == 2, "got {}", regions.len());
        Ok(())
    }

    #[test]
    fn coverage_decays_toward_neutral() -> anyhow::Result<()> {
        let rc = RegionalCoverage::new(NetworkReputationConfig::default()).expect("valid cfg");
        let o = op();
        // Push well above neutral with several reports.
        for _ in 0..20 {
            rc.record(o, DE, 1.0, 3.0, 0);
        }
        let fresh = rc
            .coverage(o, DE, 0)
            .ok_or_else(|| anyhow::anyhow!("no signal"))?;
        let week = crate::settlement::SECONDS_PER_WEEK_U64;
        let later = rc
            .coverage(o, DE, 10 * week)
            .ok_or_else(|| anyhow::anyhow!("no signal"))?;
        ensure!(later < fresh, "fresh {fresh} later {later}");
        ensure!(later > 0.5, "should still be above neutral: {later}");
        Ok(())
    }

    #[test]
    fn evict_removes_idle_near_neutral_buckets() -> anyhow::Result<()> {
        let rc = RegionalCoverage::new(NetworkReputationConfig::default()).expect("valid cfg");
        let o = op();
        // One small nudge then long idle → decays back within 0.05 of neutral.
        rc.record(o, DE, 1.0, 3.0, 0);
        let week = crate::settlement::SECONDS_PER_WEEK_U64;
        let now = 40 * week; // > 26 weeks idle, decayed to ~neutral
        rc.evict(now);
        ensure!(rc.coverage(o, DE, now).is_none(), "expected eviction");
        Ok(())
    }
}
