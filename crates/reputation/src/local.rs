//! Local reputation scoring (ADR 008 §3, §14a — `PoC` scope).
//!
//! Folds delivery outcomes into a per-peer EWMA score in `[0.0, 1.0]`.
//! In-memory only; persistence is deferred per ADR 008 §14a.3.

use iroh::PublicKey as NodeId;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;
use thiserror::Error;

const DEFAULT_ALPHA: f64 = 0.1;
const DEFAULT_INITIAL_SCORE: f64 = 0.5;
const DEFAULT_EXPECTED_BPS: u64 = 10 * 1024 * 1024;
const DEFAULT_SPEED_WEIGHT: f64 = 0.4;
const DEFAULT_CORRECTNESS_WEIGHT: f64 = 0.4;
const DEFAULT_REACHABILITY_WEIGHT: f64 = 0.2;
// ADR 008 §14a excludes per-report clamping for PoC. The clamp logic is kept
// so production can opt into §8's ±0.05 cap by overriding this field, but the
// default is `1.0` — a no-op cap given EWMA delta cannot exceed 1.0 when
// prev, sample ∈ [0,1] and alpha ∈ [0,1].
const DEFAULT_MAX_DELTA_PER_UPDATE: f64 = 1.0;

/// Validation errors when constructing a [`LocalReputation`].
#[derive(Debug, Error, PartialEq)]
pub enum ConfigError {
    #[error("{field} must be a finite number in [0.0, 1.0], got {value}")]
    OutOfUnitInterval { field: &'static str, value: f64 },
    #[error("expected_bps must be > 0")]
    ZeroExpectedBps,
    #[error(
        "weights must sum to 1.0; got speed={speed} + correctness={correctness} \
         + reachability={reachability} = {sum}"
    )]
    WeightsDoNotSumToOne {
        speed: f64,
        correctness: f64,
        reachability: f64,
        sum: f64,
    },
}

/// Outcome of a single delivery interaction with a peer.
///
/// Encodes the three outcome classes relevant to ADR 008 §3 scoring. Speed
/// contributes only when the peer actually delivered bytes; the
/// [`Outcome::Unreachable`] and [`Outcome::Corruption`] variants implicitly
/// score speed as 0.
///
/// `Delivered { bytes: 0, .. }` is treated as a successful but zero-rate
/// delivery — callers should prefer [`Outcome::Corruption`] when a peer
/// agreed to serve and then sent nothing useful, since `Delivered` still
/// rewards the correctness and reachability components at zero throughput.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Dial or handshake to the peer failed.
    Unreachable,
    /// Peer responded but BLAKE3 verification of the bytes failed.
    Corruption,
    /// Peer delivered correctly verified bytes over the wire.
    Delivered { bytes: u64, elapsed: Duration },
}

/// Tunable parameters for the local EWMA score.
///
/// Defaults follow ADR 008 §3 (α=0.1, initial=0.5, weights 40/40/20).
/// Validation runs at [`LocalReputation::new`]; constructing an invalid
/// config in isolation is allowed but installing it returns
/// [`ConfigError`].
///
/// `max_delta_per_update` defaults to `1.0` (no clamping) per §14a's `PoC`
/// scope; production operators can set it to `0.05` to enforce §8's
/// per-report cap without code changes.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct LocalReputationConfig {
    pub alpha: f64,
    pub initial_score: f64,
    pub expected_bps: u64,
    pub speed_weight: f64,
    pub correctness_weight: f64,
    pub reachability_weight: f64,
    pub max_delta_per_update: f64,
}

impl Default for LocalReputationConfig {
    fn default() -> Self {
        Self {
            alpha: DEFAULT_ALPHA,
            initial_score: DEFAULT_INITIAL_SCORE,
            expected_bps: DEFAULT_EXPECTED_BPS,
            speed_weight: DEFAULT_SPEED_WEIGHT,
            correctness_weight: DEFAULT_CORRECTNESS_WEIGHT,
            reachability_weight: DEFAULT_REACHABILITY_WEIGHT,
            max_delta_per_update: DEFAULT_MAX_DELTA_PER_UPDATE,
        }
    }
}

/// In-memory store of per-peer EWMA reputation scores keyed by [`NodeId`].
///
/// Cheap to share across tasks via [`std::sync::Arc`]; reads use a
/// read-lock, writes a write-lock. `PoC` scope — no persistence, no
/// network aggregation. See ADR 008 §14a.
#[derive(Debug)]
pub struct LocalReputation {
    config: LocalReputationConfig,
    scores: RwLock<HashMap<NodeId, f64>>,
}

impl LocalReputation {
    /// Build a new score store, validating the config.
    ///
    /// Validation rejects non-finite or out-of-range parameters so a single
    /// bad config write cannot poison every future score with NaN.
    pub fn new(config: LocalReputationConfig) -> Result<Self, ConfigError> {
        validate(&config)?;
        Ok(Self {
            config,
            scores: RwLock::new(HashMap::new()),
        })
    }

    /// Fold an outcome into the peer's score and return the new value.
    ///
    /// The returned value matches a subsequent [`Self::score`] call so
    /// callers logging both can avoid a re-lock.
    pub fn record(&self, peer: NodeId, outcome: Outcome) -> f64 {
        let sample = self.interaction_score(outcome);
        // Poison recovery: the only writer is this method, and the HashMap
        // is mutated only by the final `insert` after all arithmetic. A
        // panic earlier in the function leaves the map structurally intact.
        let mut guard = self
            .scores
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = guard
            .get(&peer)
            .copied()
            .unwrap_or(self.config.initial_score);
        let next = self.fold(prev, sample);
        guard.insert(peer, next);
        next
    }

    /// Current score for `peer`, or [`LocalReputationConfig::initial_score`] if unseen.
    pub fn score(&self, peer: NodeId) -> f64 {
        // Poison recovery: read-only path; cannot itself corrupt state.
        let guard = self
            .scores
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .get(&peer)
            .copied()
            .unwrap_or(self.config.initial_score)
    }

    /// Snapshot of all observed `(peer, score)` pairs. Allocates.
    pub fn snapshot(&self) -> Vec<(NodeId, f64)> {
        // Poison recovery: read-only path; same reasoning as `score`.
        let guard = self
            .scores
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.iter().map(|(k, v)| (*k, *v)).collect()
    }

    fn interaction_score(&self, outcome: Outcome) -> f64 {
        match outcome {
            Outcome::Unreachable => 0.0,
            Outcome::Corruption => self.config.reachability_weight,
            Outcome::Delivered { bytes, elapsed } => {
                let speed = speed_score(bytes, elapsed, self.config.expected_bps);
                self.config.speed_weight * speed
                    + self.config.correctness_weight
                    + self.config.reachability_weight
            }
        }
    }

    fn fold(&self, prev: f64, sample: f64) -> f64 {
        let next = (1.0 - self.config.alpha) * prev + self.config.alpha * sample;
        let delta = (next - prev).clamp(
            -self.config.max_delta_per_update,
            self.config.max_delta_per_update,
        );
        (prev + delta).clamp(0.0, 1.0)
    }
}

// u64→f64 loses precision above 2^53 (~9 PB) — far above any plausible
// single transfer; the result is then clamped to [0, 1] so any drift above
// 2^53 rounds within the saturated range and cannot escape the score envelope.
#[allow(clippy::cast_precision_loss)]
fn speed_score(bytes: u64, elapsed: Duration, expected_bps: u64) -> f64 {
    let secs = elapsed.as_secs_f64();
    if !secs.is_finite() || secs <= 0.0 || expected_bps == 0 {
        return 0.0;
    }
    let actual_bps = bytes as f64 / secs;
    let expected = expected_bps as f64;
    (actual_bps / expected).clamp(0.0, 1.0)
}

fn validate(c: &LocalReputationConfig) -> Result<(), ConfigError> {
    require_unit_interval(c.alpha, "alpha")?;
    require_unit_interval(c.initial_score, "initial_score")?;
    require_unit_interval(c.speed_weight, "speed_weight")?;
    require_unit_interval(c.correctness_weight, "correctness_weight")?;
    require_unit_interval(c.reachability_weight, "reachability_weight")?;
    require_unit_interval(c.max_delta_per_update, "max_delta_per_update")?;
    if c.expected_bps == 0 {
        return Err(ConfigError::ZeroExpectedBps);
    }
    let sum = c.speed_weight + c.correctness_weight + c.reachability_weight;
    if (sum - 1.0).abs() > 1e-9 {
        return Err(ConfigError::WeightsDoNotSumToOne {
            speed: c.speed_weight,
            correctness: c.correctness_weight,
            reachability: c.reachability_weight,
            sum,
        });
    }
    Ok(())
}

fn require_unit_interval(value: f64, field: &'static str) -> Result<(), ConfigError> {
    if value.is_finite() && (0.0..=1.0).contains(&value) {
        Ok(())
    } else {
        Err(ConfigError::OutOfUnitInterval { field, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{Context, ensure};
    use iroh::SecretKey;
    use std::sync::Arc;
    use std::thread;

    fn fresh_peer() -> NodeId {
        SecretKey::generate().public()
    }

    fn approx(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn default_config_matches_adr_008_section_3() -> anyhow::Result<()> {
        let c = LocalReputationConfig::default();
        ensure!(approx(c.alpha, 0.1), "alpha drift: {}", c.alpha);
        ensure!(
            approx(c.initial_score, 0.5),
            "initial drift: {}",
            c.initial_score
        );
        ensure!(approx(c.speed_weight, 0.4));
        ensure!(approx(c.correctness_weight, 0.4));
        ensure!(approx(c.reachability_weight, 0.2));
        ensure!(c.expected_bps == 10 * 1024 * 1024);
        // §14a defers clamping; default is no-op (1.0).
        ensure!(approx(c.max_delta_per_update, 1.0));
        let sum = c.speed_weight + c.correctness_weight + c.reachability_weight;
        ensure!((sum - 1.0).abs() < 1e-9, "weight sum drift: {sum}");
        Ok(())
    }

    #[test]
    fn invalid_config_rejected() {
        let bad = LocalReputationConfig {
            alpha: f64::NAN,
            ..LocalReputationConfig::default()
        };
        assert!(matches!(
            LocalReputation::new(bad),
            Err(ConfigError::OutOfUnitInterval { field: "alpha", .. })
        ));

        let bad = LocalReputationConfig {
            expected_bps: 0,
            ..LocalReputationConfig::default()
        };
        assert!(matches!(
            LocalReputation::new(bad),
            Err(ConfigError::ZeroExpectedBps)
        ));

        let bad = LocalReputationConfig {
            speed_weight: 0.5, // 0.5 + 0.4 + 0.2 = 1.1
            ..LocalReputationConfig::default()
        };
        assert!(matches!(
            LocalReputation::new(bad),
            Err(ConfigError::WeightsDoNotSumToOne { .. })
        ));

        let bad = LocalReputationConfig {
            initial_score: 1.5,
            ..LocalReputationConfig::default()
        };
        assert!(matches!(
            LocalReputation::new(bad),
            Err(ConfigError::OutOfUnitInterval {
                field: "initial_score",
                ..
            })
        ));
    }

    #[test]
    fn score_for_unseen_peer_returns_initial() -> anyhow::Result<()> {
        let r = LocalReputation::new(LocalReputationConfig::default())?;
        ensure!(approx(r.score(fresh_peer()), 0.5));
        Ok(())
    }

    #[test]
    fn delivered_at_baseline_speed_pulls_score_up() -> anyhow::Result<()> {
        let r = LocalReputation::new(LocalReputationConfig::default())?;
        let p = fresh_peer();
        // interaction = 0.4*1 + 0.4 + 0.2 = 1.0
        // EWMA from 0.5: 0.9*0.5 + 0.1*1.0 = 0.55 (no clamp; default max_delta=1.0)
        let next = r.record(
            p,
            Outcome::Delivered {
                bytes: 10 * 1024 * 1024,
                elapsed: Duration::from_secs(1),
            },
        );
        ensure!(approx(next, 0.55), "got {next}");
        Ok(())
    }

    #[test]
    fn delivered_at_zero_speed_isolates_correctness_weight() -> anyhow::Result<()> {
        // bytes=0 → speed_score=0; this lets us pin the *correctness* weight
        // (0.4) independently of speed: interaction = 0 + 0.4 + 0.2 = 0.6.
        // EWMA from 0.5: 0.51.
        let r = LocalReputation::new(LocalReputationConfig::default())?;
        let p = fresh_peer();
        let next = r.record(
            p,
            Outcome::Delivered {
                bytes: 0,
                elapsed: Duration::from_secs(1),
            },
        );
        ensure!(approx(next, 0.51), "got {next}");
        Ok(())
    }

    #[test]
    fn unreachable_pulls_score_down() -> anyhow::Result<()> {
        let r = LocalReputation::new(LocalReputationConfig::default())?;
        let p = fresh_peer();
        // interaction = 0; EWMA from 0.5: 0.45
        let next = r.record(p, Outcome::Unreachable);
        ensure!(approx(next, 0.45), "got {next}");
        Ok(())
    }

    #[test]
    fn corruption_holds_score_near_reachability_weight() -> anyhow::Result<()> {
        let r = LocalReputation::new(LocalReputationConfig::default())?;
        let p = fresh_peer();
        // Corruption interaction = 0.2; long-run fixed point of EWMA = 0.2.
        for _ in 0..200 {
            r.record(p, Outcome::Corruption);
        }
        let s = r.score(p);
        ensure!((s - 0.2).abs() < 1e-3, "score = {s}");
        Ok(())
    }

    #[test]
    fn clamp_caps_per_update_movement() -> anyhow::Result<()> {
        // Opt into §8 production clamp; alpha=1 makes the candidate next
        // value equal the sample so the clamp is the only invariant under
        // test. Without it, prev=0.5 + sample=0 would land at 0; with the
        // 0.05 cap it lands at 0.45.
        let cfg = LocalReputationConfig {
            alpha: 1.0,
            max_delta_per_update: 0.05,
            ..LocalReputationConfig::default()
        };
        let r = LocalReputation::new(cfg)?;
        let p = fresh_peer();
        let next = r.record(p, Outcome::Unreachable);
        ensure!(approx(next, 0.45), "got {next}");
        Ok(())
    }

    #[test]
    fn speed_score_saturates_above_baseline() -> anyhow::Result<()> {
        let r = LocalReputation::new(LocalReputationConfig::default())?;
        let p1 = fresh_peer();
        let p2 = fresh_peer();
        let baseline = r.record(
            p1,
            Outcome::Delivered {
                bytes: 10 * 1024 * 1024,
                elapsed: Duration::from_secs(1),
            },
        );
        let above = r.record(
            p2,
            Outcome::Delivered {
                bytes: 100 * 1024 * 1024, // 10× baseline
                elapsed: Duration::from_secs(1),
            },
        );
        ensure!(approx(baseline, above), "baseline={baseline} above={above}");
        // Anchor: rule out a "always returns initial" bug by pinning the
        // saturated value (0.55 from EWMA of 0.5 ← 1.0 with α=0.1).
        ensure!(
            approx(baseline, 0.55),
            "expected saturation to land at 0.55, got {baseline}"
        );
        Ok(())
    }

    #[test]
    fn speed_score_function_is_correct() {
        let bps = 10 * 1024 * 1024;
        let one_sec = Duration::from_secs(1);
        assert!(approx(speed_score(bps, one_sec, bps), 1.0));
        assert!(approx(speed_score(2 * bps, one_sec, bps), 1.0));
        assert!(approx(speed_score(bps / 10, one_sec, bps), 0.1));
        assert!(approx(speed_score(0, one_sec, bps), 0.0));
        assert!(approx(speed_score(1, Duration::ZERO, bps), 0.0));
        assert!(approx(speed_score(1, one_sec, 0), 0.0));
    }

    #[test]
    fn expected_bps_override_changes_speed_baseline() -> anyhow::Result<()> {
        let cfg = LocalReputationConfig {
            expected_bps: 1024 * 1024, // 1 MiB/s baseline
            ..LocalReputationConfig::default()
        };
        let r = LocalReputation::new(cfg)?;
        let p = fresh_peer();
        // 1 MiB in 1s now equals baseline → interaction = 1.0; EWMA → 0.55
        let next = r.record(
            p,
            Outcome::Delivered {
                bytes: 1024 * 1024,
                elapsed: Duration::from_secs(1),
            },
        );
        ensure!(approx(next, 0.55), "got {next}");
        Ok(())
    }

    #[test]
    fn slow_delivery_scores_between_corruption_and_full() -> anyhow::Result<()> {
        let cfg = LocalReputationConfig::default();
        let r_corrupt = LocalReputation::new(cfg.clone())?;
        let r_slow = LocalReputation::new(cfg.clone())?;
        let r_full = LocalReputation::new(cfg)?;
        let p = fresh_peer();
        let corrupt = r_corrupt.record(p, Outcome::Corruption);
        // ~10% of baseline → speed_score ≈ 0.1 → interaction ≈ 0.64
        let slow = r_slow.record(
            p,
            Outcome::Delivered {
                bytes: 1024 * 1024,
                elapsed: Duration::from_secs(1),
            },
        );
        let full = r_full.record(
            p,
            Outcome::Delivered {
                bytes: 10 * 1024 * 1024,
                elapsed: Duration::from_secs(1),
            },
        );
        ensure!(corrupt < slow, "corrupt={corrupt} slow={slow}");
        ensure!(slow < full, "slow={slow} full={full}");
        Ok(())
    }

    #[test]
    fn independent_peers_tracked_separately() -> anyhow::Result<()> {
        let r = LocalReputation::new(LocalReputationConfig::default())?;
        let p_good = fresh_peer();
        let p_bad = fresh_peer();
        for _ in 0..50 {
            r.record(
                p_good,
                Outcome::Delivered {
                    bytes: 10 * 1024 * 1024,
                    elapsed: Duration::from_secs(1),
                },
            );
            r.record(p_bad, Outcome::Unreachable);
        }
        let good = r.score(p_good);
        let bad = r.score(p_bad);
        ensure!(good > 0.9, "good={good}");
        ensure!(bad < 0.05, "bad={bad}");
        Ok(())
    }

    #[test]
    fn zero_elapsed_does_not_panic_and_yields_zero_speed() -> anyhow::Result<()> {
        let r = LocalReputation::new(LocalReputationConfig::default())?;
        let p = fresh_peer();
        // speed_score=0 → interaction = 0 + 0.4 + 0.2 = 0.6; EWMA → 0.51
        let next = r.record(
            p,
            Outcome::Delivered {
                bytes: 1,
                elapsed: Duration::ZERO,
            },
        );
        ensure!(approx(next, 0.51), "got {next}");
        Ok(())
    }

    #[test]
    fn record_returns_post_update_score() -> anyhow::Result<()> {
        let r = LocalReputation::new(LocalReputationConfig::default())?;
        let p = fresh_peer();
        let returned = r.record(p, Outcome::Unreachable);
        let queried = r.score(p);
        ensure!(approx(returned, queried));
        // Anchor the value so a buggy `record` returning `prev` (0.5) instead
        // of `next` (0.45) cannot pass via the queried==returned tautology.
        ensure!(approx(returned, 0.45), "expected 0.45, got {returned}");
        Ok(())
    }

    #[test]
    fn snapshot_includes_all_observed_peers() -> anyhow::Result<()> {
        let r = LocalReputation::new(LocalReputationConfig::default())?;
        let p1 = fresh_peer();
        let p2 = fresh_peer();
        r.record(p1, Outcome::Unreachable); // 0.45
        r.record(p2, Outcome::Corruption); // 0.9*0.5 + 0.1*0.2 = 0.47
        let snap: HashMap<NodeId, f64> = r.snapshot().into_iter().collect();
        ensure!(snap.len() == 2, "expected 2 entries, got {}", snap.len());
        let s1 = *snap.get(&p1).context("p1 missing from snapshot")?;
        let s2 = *snap.get(&p2).context("p2 missing from snapshot")?;
        ensure!(approx(s1, 0.45), "p1 score={s1}");
        ensure!(approx(s2, 0.47), "p2 score={s2}");
        Ok(())
    }

    #[test]
    fn concurrent_record_and_score_does_not_lose_updates() -> anyhow::Result<()> {
        let r = Arc::new(LocalReputation::new(LocalReputationConfig::default())?);
        let p = fresh_peer();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let r = Arc::clone(&r);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    r.record(p, Outcome::Unreachable);
                    let _ = r.score(p);
                }
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| anyhow::anyhow!("worker thread panicked"))?;
        }
        let snap = r.snapshot();
        ensure!(
            snap.len() == 1,
            "expected exactly one peer entry, got {}",
            snap.len()
        );
        let s = r.score(p);
        ensure!((0.0..=1.0).contains(&s), "score out of range: {s}");
        // After 8000 Unreachable events from 0.5, EWMA decays as 0.5 * 0.9^n
        // — well below 1e-100, so any non-trivial value would indicate a
        // lost update from a torn read-modify-write.
        ensure!(s < 1e-4, "expected near-zero score, got {s}");
        Ok(())
    }
}
