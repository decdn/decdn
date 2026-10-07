//! W-TinyLFU estimator + policies (filled in Stage B, Tasks 5-7).
use super::EvictionPolicy;
use super::FrequencyEstimator;
use super::sketch::ShardedCountMinSketch;
use super::{AdmissionContext, AdmissionDecision, AdmissionPolicy, Segment};
use super::{EvictionContext, EvictionPlan};
use crate::Hash;
use std::collections::HashSet;
use std::sync::Arc;

/// Shared W-TinyLFU frequency evidence: one sighting per served request from
/// the serve path, one estimate read per fill-path admission decision and per
/// probation candidate at sweep time.
///
/// The counters live in a [`ShardedCountMinSketch`], so a sighting or an
/// estimate locks the one shard its hash routes to. Serve completions and
/// admission reads for different hashes therefore proceed in parallel, and the
/// periodic aging pass touches a single shard rather than the whole array.
///
/// Aging runs on one node-wide clock across all shards. Both readers below
/// compare estimates on a shared scale — [`ProbationAdmission`] against a
/// node-wide threshold, [`TinyLfuEviction`] by ranking candidates from
/// different shards against each other — so counters are only meaningful if
/// each is caught up to that one clock when it is read. A clock per shard would
/// age at a rate set by key skew and put the two readers on scales that cannot
/// be compared at all.
#[derive(Debug)]
pub struct TinyLfuEstimator {
    inner: ShardedCountMinSketch,
}

impl TinyLfuEstimator {
    /// Size the sketch to roughly `sketch_bytes` of counters: one `u8` per
    /// counter over `ROWS` rows, so `cols = bytes / 4`.
    ///
    /// The floor of 64 columns is a backstop for callers outside config
    /// resolution, which rejects a `cache.tinylfu.sketch_bytes` small enough to
    /// reach it (`decdn_common::config::MIN_TINYLFU_SKETCH_BYTES`). It keeps the
    /// width above [`super::sketch::SHARDS`], below which a sharded sketch
    /// degenerates to one column per shard (see [`ShardedCountMinSketch::new`]).
    /// It is not a width the estimates read usefully at: 64 columns over
    /// [`super::sketch::SHARDS`] shards is four columns per shard, and the
    /// collision rate that matters is the per-shard one.
    #[must_use]
    pub fn new(sketch_bytes: usize) -> Self {
        // one u8 per counter, ROWS(=4) rows: cols = bytes / 4.
        let cols = (sketch_bytes / 4).max(64);
        Self {
            inner: ShardedCountMinSketch::new(cols),
        }
    }
}

impl FrequencyEstimator for TinyLfuEstimator {
    fn observe(&self, hash: Hash) {
        self.inner.increment(&hash);
    }
    fn estimate(&self, hash: Hash) -> u32 {
        u32::from(self.inner.estimate(&hash))
    }
}

/// Admits a first-ever miss into `Probation`; once the shared estimator has
/// seen `promotion_threshold` prior sightings of a hash, admits straight to
/// `Main`. Reads `estimate` only — never calls `observe` (see the ordering
/// invariant in the module docs: a request must never count as evidence for
/// its own promotion).
///
/// The estimate read takes one sketch shard, so a fill decision and a
/// concurrent serve completion collide only when their hashes share a shard.
#[derive(Debug)]
pub struct ProbationAdmission {
    /// The shared frequency estimator; read, never written, by this policy.
    pub freq: Arc<dyn FrequencyEstimator>,
    /// Prior sightings a hash needs before a miss is admitted straight to
    /// [`Segment::Main`] instead of `Probation`.
    pub promotion_threshold: u32,
}

impl AdmissionPolicy for ProbationAdmission {
    fn admit(&self, ctx: &AdmissionContext) -> AdmissionDecision {
        let segment = if self.freq.estimate(ctx.hash) < self.promotion_threshold {
            Segment::Probation
        } else {
            Segment::Main
        };
        AdmissionDecision::Store { segment }
    }
}

/// Ranks eviction candidates least-frequent-first, reading frequency from a
/// shared estimator; ties break oldest-access-first (LRFU), then largest
/// first, so blobs sharing the open-time recency seed release in the order that
/// reaches the target in the fewest releases. Also owns the
/// probation lifecycle at sweep time: promotes probation members whose
/// buffered frequency has reached `promotion_threshold`, and caps probation's
/// footprint to `probation_target_pct` of `cache_bytes` by evicting the
/// least-frequent non-promoted probation members first.
#[derive(Debug)]
pub struct TinyLfuEviction {
    freq: Arc<dyn FrequencyEstimator>,
    promotion_threshold: u32,
    probation_target_pct: u64,
}

impl TinyLfuEviction {
    /// Rank against `freq`, promoting probation members at
    /// `promotion_threshold` sightings and holding probation to
    /// `probation_target_pct` of the cache.
    #[must_use]
    pub fn new(
        freq: Arc<dyn FrequencyEstimator>,
        promotion_threshold: u32,
        probation_target_pct: u64,
    ) -> Self {
        Self {
            freq,
            promotion_threshold,
            probation_target_pct,
        }
    }
}

impl EvictionPolicy for TinyLfuEviction {
    fn plan(&self, ctx: &EvictionContext<'_>) -> EvictionPlan {
        let size = |h: &Hash| ctx.sizes.get(h).copied().unwrap_or(0);
        // 1. Promote: probation members whose buffered frequency now clears
        // the threshold graduate to Main. They're excluded from the
        // probation-cap eviction below (and from the global loop, since a
        // hash can't need eviction and promotion in the same sweep).
        let mut promote = Vec::new();
        let mut promoted: HashSet<Hash> = HashSet::new();
        for (h, seg) in ctx.segments {
            if *seg == Segment::Probation && self.freq.estimate(*h) >= self.promotion_threshold {
                promote.push((*h, Segment::Main));
                promoted.insert(*h);
            }
        }

        let mut evict = Vec::new();
        let mut evicted: HashSet<Hash> = HashSet::new();
        let mut freed = 0u64;

        // 2. Cap: bound probation's footprint (excluding promoted members) to
        // probation_target_pct of the configured cache size.
        let probation_limit = ctx
            .cache_bytes
            .saturating_mul(self.probation_target_pct)
            .saturating_div(100);
        let mut probation_members: Vec<(Hash, u32, std::time::Instant)> = ctx
            .candidates
            .iter()
            .filter(|(h, _)| {
                ctx.segments.get(*h).copied() == Some(Segment::Probation) && !promoted.contains(*h)
            })
            .map(|(h, t)| (*h, self.freq.estimate(*h), *t))
            .collect();
        let probation_footprint: u64 = probation_members
            .iter()
            .map(|(h, _, _)| ctx.sizes.get(h).copied().unwrap_or(0))
            .sum();
        if probation_footprint > probation_limit {
            // Least frequent first; tie-break oldest access first, then largest.
            probation_members.sort_by(|a, b| {
                a.1.cmp(&b.1)
                    .then(a.2.cmp(&b.2))
                    .then_with(|| size(&b.0).cmp(&size(&a.0)))
            });
            let mut remaining = probation_footprint;
            for (h, _, _) in probation_members {
                if remaining <= probation_limit {
                    break;
                }
                if evict.len() as u64 >= ctx.budget {
                    break;
                }
                let size = ctx.sizes.get(&h).copied().unwrap_or(0);
                remaining = remaining.saturating_sub(size);
                freed = freed.saturating_add(size);
                evicted.insert(h);
                evict.push(h);
            }
        }

        // 3. Global target: continue least-frequent-first eviction toward
        // target_bytes, skipping anything already evicted or promoted this
        // sweep.
        let mut scored: Vec<(Hash, u32, std::time::Instant)> = ctx
            .candidates
            .iter()
            .filter(|(h, _)| !evicted.contains(*h) && !promoted.contains(*h))
            .map(|(h, t)| (*h, self.freq.estimate(*h), *t))
            .collect();
        scored.sort_by(|a, b| {
            a.1.cmp(&b.1)
                .then(a.2.cmp(&b.2))
                .then_with(|| size(&b.0).cmp(&size(&a.0)))
        });
        for (h, _, _) in scored {
            if ctx.total_bytes.saturating_sub(freed) <= ctx.target_bytes {
                break;
            }
            if evict.len() as u64 >= ctx.budget {
                break;
            }
            freed = freed.saturating_add(ctx.sizes.get(&h).copied().unwrap_or(0));
            evict.push(h);
        }

        EvictionPlan { evict, promote }
    }
}

#[cfg(test)]
mod tests;
