//! Network reputation aggregation (ADR 008 §Network Score Aggregation,
//! §Combined Score, §Score Decay, §Score Clamping).
//!
//! Folds gossip-propagated, reporter-weighted [`ReportInput`]s into a per-peer
//! `network_score` in `[0, 1]`. The reporter weight itself is computed by the
//! caller via [`crate::settlement`] (kept out of this module so aggregation
//! stays independent of the settlement source). In-memory only; per-node and
//! intentionally non-convergent (ADR 008 §Network Score Aggregation note).

use crate::interaction::{InteractionWeights, interaction_score_from_metrics};
use crate::local::ConfigError;
use crate::settlement::{DEFAULT_MIN_COUNTERPARTIES, MIN_COUNTERPARTIES_FLOOR, SECONDS_PER_WEEK};
use iroh::PublicKey as NodeId;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

const DEFAULT_ALPHA: f64 = 0.05;
const DEFAULT_INITIAL_SCORE: f64 = 0.5;
const DEFAULT_EXPECTED_BPS: u64 = 10 * 1024 * 1024;
const DEFAULT_SPEED_WEIGHT: f64 = 0.4;
const DEFAULT_CORRECTNESS_WEIGHT: f64 = 0.4;
const DEFAULT_REACHABILITY_WEIGHT: f64 = 0.2;
/// ADR 008 §Score Clamping: a single report moves `network_score` by at most
/// ±0.05. Unlike the local default, the network clamp DOES bind (high-weight
/// reporters can drive EWMA deltas up to 0.15).
const DEFAULT_MAX_DELTA_PER_UPDATE: f64 = 0.05;
const DEFAULT_MIN_DISTINCT_REPORTERS: u32 = 3;
const DEFAULT_DECAY_RATE_PER_WEEK: f64 = 0.10;
const DEFAULT_LOCAL_WEIGHT: f64 = 0.70;
const DEFAULT_NETWORK_WEIGHT: f64 = 0.30;

/// Tunable parameters for network aggregation, the regional-coverage signal,
/// and the combined score. Defaults follow ADR 008.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct NetworkReputationConfig {
    /// Base EWMA alpha; effective alpha is `alpha * reporter_weight`.
    pub alpha: f64,
    /// Neutral score for unseen / unscored peers.
    pub initial_score: f64,
    /// Delivery-speed baseline in bytes/sec for `speed_score`.
    pub expected_bps: u64,
    /// Interaction-score weight on delivery speed.
    pub speed_weight: f64,
    /// Interaction-score weight on data correctness.
    pub correctness_weight: f64,
    /// Interaction-score weight on reachability.
    pub reachability_weight: f64,
    /// Per-report clamp on the score delta (ADR 008 §Score Clamping).
    pub max_delta_per_update: f64,
    /// Distinct-counterparty target for full diversity credit.
    pub min_counterparties: u32,
    /// Minimum distinct weighted reporters before a network score departs from
    /// neutral (ADR 008 §Minimum reporter threshold).
    pub min_distinct_reporters: u32,
    /// Weekly decay toward neutral without new data (ADR 008 §Score Decay).
    pub decay_rate_per_week: f64,
    /// Weight on the local score in the combined score.
    pub local_weight: f64,
    /// Weight on the network score in the combined score.
    pub network_weight: f64,
}

impl Default for NetworkReputationConfig {
    fn default() -> Self {
        Self {
            alpha: DEFAULT_ALPHA,
            initial_score: DEFAULT_INITIAL_SCORE,
            expected_bps: DEFAULT_EXPECTED_BPS,
            speed_weight: DEFAULT_SPEED_WEIGHT,
            correctness_weight: DEFAULT_CORRECTNESS_WEIGHT,
            reachability_weight: DEFAULT_REACHABILITY_WEIGHT,
            max_delta_per_update: DEFAULT_MAX_DELTA_PER_UPDATE,
            min_counterparties: DEFAULT_MIN_COUNTERPARTIES,
            min_distinct_reporters: DEFAULT_MIN_DISTINCT_REPORTERS,
            decay_rate_per_week: DEFAULT_DECAY_RATE_PER_WEEK,
            local_weight: DEFAULT_LOCAL_WEIGHT,
            network_weight: DEFAULT_NETWORK_WEIGHT,
        }
    }
}

impl NetworkReputationConfig {
    /// Validate the config, rejecting non-finite / out-of-range parameters.
    pub fn validate(&self) -> Result<(), ConfigError> {
        unit("alpha", self.alpha)?;
        unit("initial_score", self.initial_score)?;
        unit("speed_weight", self.speed_weight)?;
        unit("correctness_weight", self.correctness_weight)?;
        unit("reachability_weight", self.reachability_weight)?;
        unit("max_delta_per_update", self.max_delta_per_update)?;
        unit("decay_rate_per_week", self.decay_rate_per_week)?;
        unit("local_weight", self.local_weight)?;
        unit("network_weight", self.network_weight)?;
        if self.expected_bps == 0 {
            return Err(ConfigError::ZeroExpectedBps);
        }
        if self.min_counterparties < MIN_COUNTERPARTIES_FLOOR {
            return Err(ConfigError::BelowMinimum {
                field: "min_counterparties",
                min: MIN_COUNTERPARTIES_FLOOR,
                value: self.min_counterparties,
            });
        }
        if self.min_distinct_reporters < 1 {
            return Err(ConfigError::BelowMinimum {
                field: "min_distinct_reporters",
                min: 1,
                value: self.min_distinct_reporters,
            });
        }
        let wsum = self.speed_weight + self.correctness_weight + self.reachability_weight;
        if (wsum - 1.0).abs() > 1e-9 {
            return Err(ConfigError::WeightsDoNotSumToOne {
                speed: self.speed_weight,
                correctness: self.correctness_weight,
                reachability: self.reachability_weight,
                sum: wsum,
            });
        }
        let split = self.local_weight + self.network_weight;
        if (split - 1.0).abs() > 1e-9 {
            return Err(ConfigError::SplitDoesNotSumToOne {
                local: self.local_weight,
                network: self.network_weight,
                sum: split,
            });
        }
        Ok(())
    }

    const fn weights(&self) -> InteractionWeights {
        InteractionWeights {
            speed: self.speed_weight,
            correctness: self.correctness_weight,
            reachability: self.reachability_weight,
        }
    }

    /// The interaction score (ADR 008 §Local Score formula) for a set of
    /// reported metrics, using this config's weights and speed baseline.
    /// Exposed so the regional-coverage path can score a report with the same
    /// formula the network score uses.
    pub fn interaction_score(
        &self,
        delivery_speed: Option<u32>,
        uptime_observed: Option<bool>,
        data_correct: Option<bool>,
    ) -> f64 {
        interaction_score_from_metrics(
            self.weights(),
            self.expected_bps,
            delivery_speed,
            uptime_observed,
            data_correct,
        )
    }
}

fn unit(field: &'static str, value: f64) -> Result<(), ConfigError> {
    if value.is_finite() && (0.0..=1.0).contains(&value) {
        Ok(())
    } else {
        Err(ConfigError::OutOfUnitInterval { field, value })
    }
}

/// One validated, weight-resolved report to fold (ADR 008 §Gossip Protocol).
#[derive(Debug, Clone)]
pub struct ReportInput {
    /// The peer being rated.
    pub provider: NodeId,
    /// The reporting peer (counts toward the distinct-reporter threshold).
    pub reporter: NodeId,
    /// Reported delivery rate in bytes/sec, if measured.
    pub delivery_speed: Option<u32>,
    /// Reported reachability, if observed.
    pub uptime_observed: Option<bool>,
    /// Reported data correctness, if observed.
    pub data_correct: Option<bool>,
    /// Receiver's wall-clock seconds when the report was accepted.
    pub now_secs: u64,
}

#[derive(Debug)]
struct NetworkEntry {
    score: f64,
    last_update_secs: u64,
    distinct_reporters: HashSet<NodeId>,
}

/// In-memory per-peer network reputation scores (ADR 008 §Network Score
/// Aggregation). Cheap to share via [`std::sync::Arc`].
#[derive(Debug)]
pub struct NetworkReputation {
    config: NetworkReputationConfig,
    scores: RwLock<HashMap<NodeId, NetworkEntry>>,
}

impl NetworkReputation {
    /// Build a new aggregator, validating the config.
    pub fn new(config: NetworkReputationConfig) -> Result<Self, ConfigError> {
        config.validate()?;
        Ok(Self {
            config,
            scores: RwLock::new(HashMap::new()),
        })
    }

    /// The configuration in use.
    pub const fn config(&self) -> &NetworkReputationConfig {
        &self.config
    }

    /// Fold one weighted report into `provider`'s network score and return the
    /// stored (raw, undecayed-at-future-reads) score.
    ///
    /// `weight` is the reporter's credibility weight (see
    /// [`crate::settlement::compute_reporter_weight`]). A non-positive weight is a
    /// no-op (ADR 008: zero-weight reporters have no effect) and does not count
    /// toward the distinct-reporter threshold. The stored score is first decayed
    /// to `now_secs`, then EWMA-folded with the report's interaction score and
    /// clamped to ±`max_delta_per_update`.
    pub fn record(&self, report: &ReportInput, weight: f64) -> f64 {
        let sample = interaction_score_from_metrics(
            self.config.weights(),
            self.config.expected_bps,
            report.delivery_speed,
            report.uptime_observed,
            report.data_correct,
        );
        let mut guard = self
            .scores
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = guard
            .entry(report.provider)
            .or_insert_with(|| NetworkEntry {
                score: self.config.initial_score,
                last_update_secs: report.now_secs,
                distinct_reporters: HashSet::new(),
            });
        if !weight.is_finite() || weight <= 0.0 {
            // No-op for zero-weight reporters; still report the current decayed
            // value so callers see a consistent number.
            return decay_to(
                entry.score,
                entry.last_update_secs,
                report.now_secs,
                self.config.initial_score,
                self.config.decay_rate_per_week,
            );
        }
        let decayed = decay_to(
            entry.score,
            entry.last_update_secs,
            report.now_secs,
            self.config.initial_score,
            self.config.decay_rate_per_week,
        );
        let alpha = (self.config.alpha * weight).clamp(0.0, 1.0);
        let next = (1.0 - alpha) * decayed + alpha * sample;
        let delta = (next - decayed).clamp(
            -self.config.max_delta_per_update,
            self.config.max_delta_per_update,
        );
        let updated = (decayed + delta).clamp(0.0, 1.0);
        entry.score = updated;
        entry.last_update_secs = report.now_secs;
        entry.distinct_reporters.insert(report.reporter);
        updated
    }

    /// `provider`'s network score at `now_secs`, lazily decayed toward neutral.
    ///
    /// Returns the neutral `initial_score` until reports from at least
    /// `min_distinct_reporters` distinct weighted reporters have been folded
    /// (ADR 008 §Minimum reporter threshold) — "unscored", not rated.
    pub fn score(&self, provider: NodeId, now_secs: u64) -> f64 {
        let guard = self
            .scores
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match guard.get(&provider) {
            None => self.config.initial_score,
            Some(entry) => {
                if u32::try_from(entry.distinct_reporters.len()).unwrap_or(u32::MAX)
                    < self.config.min_distinct_reporters
                {
                    return self.config.initial_score;
                }
                decay_to(
                    entry.score,
                    entry.last_update_secs,
                    now_secs,
                    self.config.initial_score,
                    self.config.decay_rate_per_week,
                )
            }
        }
    }

    /// Whether `provider` has met the distinct-reporter threshold (i.e.
    /// [`Self::score`] returns a real score rather than neutral).
    pub fn is_scored(&self, provider: NodeId) -> bool {
        let guard = self
            .scores
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.get(&provider).is_some_and(|e| {
            u32::try_from(e.distinct_reporters.len()).unwrap_or(u32::MAX)
                >= self.config.min_distinct_reporters
        })
    }

    /// Storage-cleanup pass (mirrors [`crate::coverage::RegionalCoverage::evict`]):
    /// drop entries whose lazily-decayed score is within 0.05 of neutral AND
    /// whose last update is older than 26 weeks, so the map stays bounded under
    /// provider churn. A re-seen provider simply re-inserts at neutral, so
    /// eviction is information-lossless for anything that has decayed to neutral.
    pub fn evict(&self, now_secs: u64) {
        let mut guard = self
            .scores
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.retain(|_, entry| {
            let decayed = decay_to(
                entry.score,
                entry.last_update_secs,
                now_secs,
                self.config.initial_score,
                self.config.decay_rate_per_week,
            );
            #[allow(clippy::cast_precision_loss)]
            let idle_weeks =
                now_secs.saturating_sub(entry.last_update_secs) as f64 / SECONDS_PER_WEEK;
            let near_neutral = (decayed - self.config.initial_score).abs() <= EVICT_NEUTRAL_BAND;
            !(near_neutral && idle_weeks > EVICT_IDLE_WEEKS)
        });
    }
}

/// Entries within this band of neutral are eviction candidates.
const EVICT_NEUTRAL_BAND: f64 = 0.05;
/// Entries idle longer than this (and near neutral) may be evicted.
const EVICT_IDLE_WEEKS: f64 = 26.0;

/// Closed-form decay toward `neutral` at `decay_rate` per week (ADR 008 §Score
/// Decay): `neutral + (score - neutral) * (1 - decay_rate)^weeks_elapsed`.
/// `weeks_elapsed` may be fractional. A clock that goes backwards yields no
/// decay (clamped at 0 weeks).
pub(crate) fn decay_to(
    score: f64,
    last_update_secs: u64,
    now_secs: u64,
    neutral: f64,
    decay_rate: f64,
) -> f64 {
    let elapsed_secs = now_secs.saturating_sub(last_update_secs);
    if elapsed_secs == 0 {
        return score;
    }
    #[allow(clippy::cast_precision_loss)]
    let weeks = elapsed_secs as f64 / SECONDS_PER_WEEK;
    neutral + (score - neutral) * (1.0 - decay_rate).powf(weeks)
}

/// Combine a local and network score (ADR 008 §Combined Score). With no local
/// observation the network score is used at 100%; otherwise the configured
/// `local_weight` / `network_weight` blend applies.
pub fn combined_score(local: Option<f64>, network: f64, cfg: &NetworkReputationConfig) -> f64 {
    match local {
        None => network,
        Some(l) => cfg.local_weight * l + cfg.network_weight * network,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::ensure;
    use iroh::SecretKey;

    fn approx(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    fn peer() -> NodeId {
        SecretKey::generate().public()
    }

    const BPS_U32: u32 = 10 * 1024 * 1024;

    fn full_report(provider: NodeId, reporter: NodeId, now: u64) -> ReportInput {
        ReportInput {
            provider,
            reporter,
            delivery_speed: Some(BPS_U32),
            uptime_observed: Some(true),
            data_correct: Some(true),
            now_secs: now,
        }
    }

    #[test]
    fn default_config_matches_adr() -> anyhow::Result<()> {
        let c = NetworkReputationConfig::default();
        ensure!(approx(c.alpha, 0.05));
        ensure!(approx(c.initial_score, 0.5));
        ensure!(approx(c.max_delta_per_update, 0.05));
        ensure!(c.min_counterparties == 5);
        ensure!(c.min_distinct_reporters == 3);
        ensure!(approx(c.decay_rate_per_week, 0.10));
        ensure!(approx(c.local_weight, 0.70));
        ensure!(approx(c.network_weight, 0.30));
        c.validate()?;
        Ok(())
    }

    #[test]
    fn invalid_config_rejected() {
        let bad = NetworkReputationConfig {
            min_counterparties: 1,
            ..Default::default()
        };
        assert!(matches!(
            bad.validate(),
            Err(ConfigError::BelowMinimum {
                field: "min_counterparties",
                ..
            })
        ));
        let bad = NetworkReputationConfig {
            local_weight: 0.5,
            network_weight: 0.4,
            ..Default::default()
        };
        assert!(matches!(
            bad.validate(),
            Err(ConfigError::SplitDoesNotSumToOne { .. })
        ));
    }

    #[test]
    fn score_neutral_until_three_distinct_reporters() -> anyhow::Result<()> {
        let nr = NetworkReputation::new(NetworkReputationConfig::default())?;
        let prov = peer();
        let (r1, r2, r3) = (peer(), peer(), peer());
        // Three positive reports but only two distinct reporters → still neutral.
        nr.record(&full_report(prov, r1, 0), 1.0);
        nr.record(&full_report(prov, r2, 0), 1.0);
        nr.record(&full_report(prov, r1, 0), 1.0);
        ensure!(approx(nr.score(prov, 0), 0.5), "got {}", nr.score(prov, 0));
        ensure!(!nr.is_scored(prov));
        // Third distinct reporter reveals the accumulated score.
        nr.record(&full_report(prov, r3, 0), 1.0);
        ensure!(nr.is_scored(prov));
        ensure!(nr.score(prov, 0) > 0.5, "got {}", nr.score(prov, 0));
        Ok(())
    }

    #[test]
    fn positive_reports_raise_score_with_pm_005_clamp() -> anyhow::Result<()> {
        let nr = NetworkReputation::new(NetworkReputationConfig::default())?;
        let prov = peer();
        // First full report from a 3x-weight reporter: sample 1.0 from 0.5,
        // EWMA delta = 0.05*3*(1.0-0.5) = 0.075, clamped to +0.05 → 0.55.
        let s = nr.record(&full_report(prov, peer(), 0), 3.0);
        ensure!(approx(s, 0.55), "got {s}");
        Ok(())
    }

    #[test]
    fn zero_weight_report_is_noop() -> anyhow::Result<()> {
        let nr = NetworkReputation::new(NetworkReputationConfig::default())?;
        let prov = peer();
        let r = peer();
        nr.record(&full_report(prov, r, 0), 0.0);
        // No distinct reporter recorded, no score change.
        ensure!(!nr.is_scored(prov));
        ensure!(approx(nr.score(prov, 0), 0.5));
        Ok(())
    }

    #[test]
    fn unreachable_reports_lower_score() -> anyhow::Result<()> {
        let nr = NetworkReputation::new(NetworkReputationConfig::default())?;
        let prov = peer();
        for r in [peer(), peer(), peer()] {
            nr.record(
                &ReportInput {
                    provider: prov,
                    reporter: r,
                    delivery_speed: None,
                    uptime_observed: Some(false),
                    data_correct: None,
                    now_secs: 0,
                },
                3.0,
            );
        }
        ensure!(nr.score(prov, 0) < 0.5, "got {}", nr.score(prov, 0));
        Ok(())
    }

    #[test]
    fn decay_closed_form_matches_adr_examples() {
        let week = crate::settlement::SECONDS_PER_WEEK_U64;
        // score 1.0 decaying 10%/week toward 0.5.
        let s1 = decay_to(1.0, 0, week, 0.5, 0.10);
        let s5 = decay_to(1.0, 0, 5 * week, 0.5, 0.10);
        let s10 = decay_to(1.0, 0, 10 * week, 0.5, 0.10);
        assert!((s1 - 0.95).abs() < 1e-3, "week1 {s1}");
        assert!((s5 - 0.795).abs() < 2e-3, "week5 {s5}");
        assert!((s10 - 0.6743).abs() < 2e-3, "week10 {s10}");
    }

    #[test]
    fn evict_drops_idle_neutral_keeps_recent() -> anyhow::Result<()> {
        let nr = NetworkReputation::new(NetworkReputationConfig::default())?;
        let week = crate::settlement::SECONDS_PER_WEEK_U64;
        // Low-weight reports at t=0 → tiny move; 40 weeks of decay returns it to
        // within 0.05 of neutral, and it's idle > 26 weeks → eligible for evict.
        let idle = peer();
        for r in [peer(), peer(), peer()] {
            nr.record(&full_report(idle, r, 0), 0.2);
        }
        let now = 40 * week;
        // A freshly-updated provider at `now` is retained regardless of score.
        let recent = peer();
        for r in [peer(), peer(), peer()] {
            nr.record(&full_report(recent, r, now), 3.0);
        }
        ensure!(nr.is_scored(idle), "idle peer was scored before eviction");
        nr.evict(now);
        ensure!(
            !nr.is_scored(idle),
            "idle near-neutral entry should be evicted"
        );
        ensure!(
            nr.is_scored(recent),
            "recently-updated entry must be retained"
        );
        Ok(())
    }

    #[test]
    fn combined_score_blends_or_falls_back() {
        let cfg = NetworkReputationConfig::default();
        assert!(approx(combined_score(None, 0.8, &cfg), 0.8));
        // 0.7*1.0 + 0.3*0.0 = 0.7
        assert!(approx(combined_score(Some(1.0), 0.0, &cfg), 0.7));
    }
}
