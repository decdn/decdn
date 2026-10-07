//! Local reputation scoring (ADR 008 §3 with the §14a scope reductions).
//!
//! Folds delivery outcomes into a per-peer EWMA score in `[0.0, 1.0]`, and
//! decays idle scores back toward neutral over time (ADR 008 §Score Decay) so
//! a peer that stops being used drifts back to unopinionated rather than
//! holding a stale high or low score. In-memory only; persistence is deferred
//! per ADR 008 §14a.3.

use iroh::PublicKey as NodeId;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use thiserror::Error;

const DEFAULT_ALPHA: f64 = 0.1;
const DEFAULT_INITIAL_SCORE: f64 = 0.5;
const DEFAULT_REFERENCE_BPS: u64 = 1024 * 1024 * 1024; // 1 GiB/s log reference (~1.0)
const DEFAULT_SPEED_WEIGHT: f64 = 0.4;
const DEFAULT_CORRECTNESS_WEIGHT: f64 = 0.4;
const DEFAULT_REACHABILITY_WEIGHT: f64 = 0.2;
/// Default decay half-life: 3 days. Sub-weekly on purpose — a transiently
/// dinged node re-enters selection in days, not weeks, and no node coasts on
/// stale reputation, both of which widen the serving set (ADR 008 §Score Decay).
const DEFAULT_DECAY_HALF_LIFE_SECS: u64 = 3 * 24 * 3600; // 259_200

/// Seconds in a week — used only to bound the eviction idle window (ADR 008
/// §Score Decay).
const SECONDS_PER_WEEK: f64 = 7.0 * 24.0 * 3600.0;

/// Number of independent score shards. Peers are spread across shards by the
/// first byte of their [`NodeId`] (uniformly distributed for an Ed25519 key), so
/// a `score()` read on the selection hot path locks one shard, and the periodic
/// `evict()` sweep walks shards one at a time instead of holding a single global
/// write lock across an O(peers) scan. A concurrent miss then contends only with
/// the ~1/N of peers sharing its shard, not the whole peer set.
const SHARD_COUNT: usize = 16;

/// Eviction candidates must be within this band of neutral (ADR 008
/// §Score Decay: converge within 0.05 of neutral).
const EVICT_NEUTRAL_BAND: f64 = 0.05;
/// …and idle for longer than this many weeks. 26 weeks (~½ year) is far
/// longer than the ~10 days a score now needs to reach the neutral band, so
/// eviction only drops entries already reading neutral.
const EVICT_IDLE_WEEKS: f64 = 26.0;
// ADR 008 §14a excludes per-report clamping from the local scoring rule. The
// clamp logic is kept so callers can opt into §8's ±0.05 cap by overriding this
// field, but the default is `1.0` — a no-op cap given EWMA delta cannot exceed
// 1.0 when prev, sample ∈ [0,1] and alpha ∈ [0,1]. The production node wiring
// opts in via [`LocalReputationConfig::with_max_delta_per_update`] +
// [`LOCAL_SCORE_MAX_DELTA_PER_REPORT`]; the library default stays a no-op.
const DEFAULT_MAX_DELTA_PER_UPDATE: f64 = 1.0;

/// ADR 008 §8 per-report score-movement cap (±0.05). The library default is a
/// no-op cap of `1.0`; the node opts into this value via
/// [`LocalReputationConfig::with_max_delta_per_update`] so a single bad
/// interaction cannot over-penalize an otherwise good peer.
pub const LOCAL_SCORE_MAX_DELTA_PER_REPORT: f64 = 0.05;

/// Validation errors when constructing a [`LocalReputation`].
#[derive(Debug, Error, PartialEq)]
pub enum ConfigError {
    /// A config field that must sit in `[0.0, 1.0]` is outside it, infinite,
    /// or NaN.
    #[error("{field} must be a finite number in [0.0, 1.0], got {value}")]
    OutOfUnitInterval {
        /// Name of the offending [`LocalReputationConfig`] field.
        field: &'static str,
        /// The value that was rejected.
        value: f64,
    },
    /// `reference_bps` is `0`, which would make the log speed curve undefined.
    #[error("reference_bps must be > 0")]
    ZeroReferenceBps,
    /// The three component weights do not add up to `1.0`, so the blended
    /// score would not stay in `[0.0, 1.0]`.
    #[error(
        "weights must sum to 1.0; got speed={speed} + correctness={correctness} \
         + reachability={reachability} = {sum}"
    )]
    WeightsDoNotSumToOne {
        /// The configured speed weight.
        speed: f64,
        /// The configured correctness weight.
        correctness: f64,
        /// The configured reachability weight.
        reachability: f64,
        /// What the three actually add up to.
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
    Delivered {
        /// Bytes that verified against the blob hash.
        bytes: u64,
        /// Wall-clock time the delivery took; with `bytes`, the throughput
        /// the speed component scores.
        elapsed: Duration,
    },
    /// Peer self-attested *this node's own* region yet the observed probe
    /// latency exceeded the ADR 030 ceiling — the canonical region-spoofing
    /// signal
    /// ([ADR 030 §Soft mitigation](../../../adr/030-node-region-self-attestation.md)).
    /// Scored as a fully negative sample (like [`Outcome::Unreachable`]); it is a
    /// local-only observation.
    RegionLatencyMismatch,
}

/// Tunable parameters for the local EWMA score.
///
/// Defaults follow ADR 008 §3 (α=0.1, initial=0.5, weights 40/40/20).
/// Validation runs at [`LocalReputation::new`]; constructing an invalid
/// config in isolation is allowed but installing it returns
/// [`ConfigError`].
///
/// `max_delta_per_update` defaults to `1.0` (no clamping) per ADR 008 §14a;
/// operators can set it to `0.05` to enforce §8's per-report cap without
/// code changes.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct LocalReputationConfig {
    /// EWMA smoothing factor: the weight given to the newest sample. Larger
    /// reacts faster and forgets faster.
    pub alpha: f64,
    /// Score a peer starts at, before any interaction is recorded.
    pub initial_score: f64,
    /// Reference throughput scoring ~1.0 under the log speed curve (was the
    /// flat saturation baseline).
    pub reference_bps: u64,
    /// Weight of the throughput component. The three weights must sum to
    /// `1.0` or [`LocalReputation::new`] rejects the config.
    pub speed_weight: f64,
    /// Weight of the hash-verification component.
    pub correctness_weight: f64,
    /// Weight of the dial-success component.
    pub reachability_weight: f64,
    /// Largest score movement one report may cause. `1.0` disables the clamp;
    /// see [`LOCAL_SCORE_MAX_DELTA_PER_REPORT`].
    pub max_delta_per_update: f64,
    /// Half-life (seconds) of idle-score decay toward neutral (ADR 008 §Score
    /// Decay). Each half-life halves the distance to neutral. `0` disables decay.
    pub decay_half_life_secs: u64,
}

impl Default for LocalReputationConfig {
    fn default() -> Self {
        Self {
            alpha: DEFAULT_ALPHA,
            initial_score: DEFAULT_INITIAL_SCORE,
            reference_bps: DEFAULT_REFERENCE_BPS,
            speed_weight: DEFAULT_SPEED_WEIGHT,
            correctness_weight: DEFAULT_CORRECTNESS_WEIGHT,
            reachability_weight: DEFAULT_REACHABILITY_WEIGHT,
            max_delta_per_update: DEFAULT_MAX_DELTA_PER_UPDATE,
            decay_half_life_secs: DEFAULT_DECAY_HALF_LIFE_SECS,
        }
    }
}

impl LocalReputationConfig {
    /// Set the per-report score-movement cap and return the config.
    ///
    /// The struct is `#[non_exhaustive]`, so downstream crates cannot use a
    /// struct-update literal to override a single field; this builder is how the
    /// node wiring opts into [`LOCAL_SCORE_MAX_DELTA_PER_REPORT`]. The value is
    /// validated (finite, `[0.0, 1.0]`) at [`LocalReputation::new`], not here.
    #[must_use]
    pub const fn with_max_delta_per_update(mut self, cap: f64) -> Self {
        self.max_delta_per_update = cap;
        self
    }
}

/// A source of wall-clock time in whole seconds since the Unix epoch, driving
/// idle-score decay (ADR 008 §Score Decay). Injected so tests can advance time
/// deterministically instead of sleeping.
pub trait Clock: std::fmt::Debug + Send + Sync {
    /// Current time as seconds since the Unix epoch.
    fn now_secs(&self) -> u64;
}

/// Production clock reading the system wall clock.
///
/// A clock before the Unix epoch (or a `duration_since` error) reads as `0`.
/// Since `decay` saturates elapsed time at 0, a `0` read is never *after* a
/// stored `last_update_secs`, so it yields no decay rather than corrupting a
/// score — the conservative outcome for a host whose clock is not a real
/// deployment.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_secs(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    }
}

/// A stored per-peer score plus the wall-clock second it was last written, so
/// reads can lazily decay it toward neutral (ADR 008 §Score Decay).
#[derive(Debug, Clone, Copy)]
struct Entry {
    score: f64,
    last_update_secs: u64,
}

/// In-memory store of per-peer EWMA reputation scores keyed by [`NodeId`].
///
/// Cheap to share across tasks via [`std::sync::Arc`]; reads use a
/// read-lock, writes a write-lock. Idle scores decay toward neutral lazily at
/// read time (ADR 008 §Score Decay). No persistence and no network
/// aggregation per ADR 008 §14a.
///
/// The peer map is split into `SHARD_COUNT` independently-locked shards so
/// per-candidate reads during ranking and the periodic maintenance sweep do not
/// serialize on one global lock.
#[derive(Debug)]
pub struct LocalReputation {
    config: LocalReputationConfig,
    clock: Arc<dyn Clock>,
    scores: Vec<RwLock<HashMap<NodeId, Entry>>>,
}

impl LocalReputation {
    /// Build a new score store backed by the system clock, validating the config.
    ///
    /// Validation rejects non-finite or out-of-range parameters so a single
    /// bad config write cannot poison every future score with NaN.
    pub fn new(config: LocalReputationConfig) -> Result<Self, ConfigError> {
        Self::with_clock(config, Arc::new(SystemClock))
    }

    /// Build a new score store driven by an explicit [`Clock`], validating the
    /// config. Tests inject a controllable clock to exercise decay without
    /// sleeping; production uses [`Self::new`] (the system clock).
    pub fn with_clock(
        config: LocalReputationConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, ConfigError> {
        validate(&config)?;
        Ok(Self {
            config,
            clock,
            scores: (0..SHARD_COUNT)
                .map(|_| RwLock::new(HashMap::new()))
                .collect(),
        })
    }

    /// The shard holding `peer`'s entry, chosen by the first key byte. Both
    /// lookups are `.first()`/`.get()` rather than indexing, so the anti-panic
    /// `indexing_slicing` lint has nothing to flag; `None` is unreachable (a key
    /// is 32 bytes and the index is taken modulo the shard count, which is the
    /// vector's length) but lets callers fall back to a neutral answer.
    fn shard_for(&self, peer: &NodeId) -> Option<&RwLock<HashMap<NodeId, Entry>>> {
        let first = *peer.as_bytes().first()?;
        self.scores.get(usize::from(first) % SHARD_COUNT)
    }

    /// Fold an outcome into the peer's score and return the new value.
    ///
    /// The stored score is first decayed to now (ADR 008 §Score Decay), then
    /// EWMA-folded with this interaction's sample. The returned value matches a
    /// subsequent [`Self::score`] call taken at the same instant so callers
    /// logging both can avoid a re-lock.
    pub fn record(&self, peer: NodeId, outcome: Outcome) -> f64 {
        let sample = self.interaction_score(outcome);
        let now = self.clock.now_secs();
        let Some(shard) = self.shard_for(&peer) else {
            return sample;
        };
        // Poison recovery: the only writer is this method, and the map entry
        // is mutated only after all arithmetic. A panic earlier in the
        // function leaves the map structurally intact.
        let mut guard = shard
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = guard.entry(peer).or_insert(Entry {
            score: self.config.initial_score,
            last_update_secs: now,
        });
        // Clamp the effective "now" so a backward clock read never rewinds
        // `last_update_secs`, which would make a later `score()` apply extra
        // decay. `decay` also saturates elapsed at 0; this keeps the stored
        // timestamp monotonic too.
        let effective_now = now.max(entry.last_update_secs);
        let decayed = self.decay(entry.score, entry.last_update_secs, effective_now);
        let next = self.fold(decayed, sample);
        entry.score = next;
        entry.last_update_secs = effective_now;
        next
    }

    /// Current score for `peer`, decayed toward neutral for idle time (ADR 008
    /// §Score Decay), or [`LocalReputationConfig::initial_score`] if unseen.
    pub fn score(&self, peer: NodeId) -> f64 {
        let now = self.clock.now_secs();
        let Some(shard) = self.shard_for(&peer) else {
            return self.config.initial_score;
        };
        // Poison recovery: read-only path; cannot itself corrupt state.
        let guard = shard
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.get(&peer).map_or(self.config.initial_score, |e| {
            self.decay(e.score, e.last_update_secs, now)
        })
    }

    /// Snapshot of all observed `(peer, score)` pairs, each decayed to now
    /// (ADR 008 §Score Decay). Allocates.
    pub fn snapshot(&self) -> Vec<(NodeId, f64)> {
        let now = self.clock.now_secs();
        let mut out = Vec::new();
        // Poison recovery: read-only path; same reasoning as `score`.
        for shard in &self.scores {
            let guard = shard
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            out.extend(
                guard
                    .iter()
                    .map(|(k, e)| (*k, self.decay(e.score, e.last_update_secs, now))),
            );
        }
        out
    }

    /// Drop entries whose decayed score has converged within `EVICT_NEUTRAL_BAND`
    /// (0.05) of neutral AND that have been idle for more than `EVICT_IDLE_WEEKS`
    /// (26), so the map stays bounded under peer churn without
    /// discarding any opinion that still reads as non-neutral. A re-seen peer
    /// simply re-inserts at neutral, so eviction is information-lossless for
    /// anything that has already decayed to neutral. Best-effort maintenance —
    /// call periodically; correctness never depends on it.
    pub fn evict(&self) {
        let now = self.clock.now_secs();
        let neutral = self.config.initial_score;
        // Sweep shard by shard: each write lock is held only across its own
        // shard's retain, so a maintenance pass never blocks a `score()` read on
        // a different shard.
        for shard in &self.scores {
            let mut guard = shard
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.retain(|_, e| {
                let decayed = self.decay(e.score, e.last_update_secs, now);
                #[allow(clippy::cast_precision_loss)]
                let idle_weeks = now.saturating_sub(e.last_update_secs) as f64 / SECONDS_PER_WEEK;
                let near_neutral = (decayed - neutral).abs() <= EVICT_NEUTRAL_BAND;
                !(near_neutral && idle_weeks > EVICT_IDLE_WEEKS)
            });
        }
    }

    /// Closed-form half-life decay toward neutral (ADR 008 §Score Decay):
    /// `neutral + (score - neutral) * 0.5^(elapsed_secs / half_life_secs)`.
    /// A `0` half-life disables decay; a backward clock yields no decay
    /// (elapsed saturates at 0).
    fn decay(&self, score: f64, last_update_secs: u64, now_secs: u64) -> f64 {
        let elapsed_secs = now_secs.saturating_sub(last_update_secs);
        let hl = self.config.decay_half_life_secs;
        if elapsed_secs == 0 || hl == 0 {
            return score;
        }
        #[allow(clippy::cast_precision_loss)]
        let half_lives = elapsed_secs as f64 / hl as f64;
        let neutral = self.config.initial_score;
        neutral + (score - neutral) * 0.5_f64.powf(half_lives)
    }

    fn interaction_score(&self, outcome: Outcome) -> f64 {
        let w = crate::interaction::InteractionWeights {
            speed: self.config.speed_weight,
            correctness: self.config.correctness_weight,
            reachability: self.config.reachability_weight,
        };
        match outcome {
            // Fully negative sample (speed 0, correctness 0, reachability 0):
            // `Unreachable` — the peer could not be reached; and
            // `RegionLatencyMismatch` — the ADR 030 latency-vs-claim penalty,
            // which taxes a same-region claim contradicted by measured RTT.
            Outcome::Unreachable | Outcome::RegionLatencyMismatch => 0.0,
            // reachable but the bytes failed verification: only reachability
            Outcome::Corruption => crate::interaction::interaction_score(w, 0.0, 0.0, 1.0),
            // correctly delivered: full correctness + reachability, scaled speed
            Outcome::Delivered { bytes, elapsed } => {
                let speed = speed_score(bytes, elapsed, self.config.reference_bps);
                crate::interaction::interaction_score(w, speed, 1.0, 1.0)
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

// Thin wrapper over the shared formula so local and network paths cannot
// drift (see [`crate::interaction`]).
fn speed_score(bytes: u64, elapsed: Duration, reference_bps: u64) -> f64 {
    crate::interaction::speed_score_from_transfer(bytes, elapsed, reference_bps)
}

fn validate(c: &LocalReputationConfig) -> Result<(), ConfigError> {
    require_unit_interval(c.alpha, "alpha")?;
    require_unit_interval(c.initial_score, "initial_score")?;
    require_unit_interval(c.speed_weight, "speed_weight")?;
    require_unit_interval(c.correctness_weight, "correctness_weight")?;
    require_unit_interval(c.reachability_weight, "reachability_weight")?;
    require_unit_interval(c.max_delta_per_update, "max_delta_per_update")?;
    if c.reference_bps == 0 {
        return Err(ConfigError::ZeroReferenceBps);
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
mod tests;
