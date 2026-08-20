//! W-TinyLFU estimator + policies (filled in Stage B, Tasks 5-7).
use super::EvictionPolicy;
use super::FrequencyEstimator;
use super::sketch::CountMinSketch;
use super::{AdmissionContext, AdmissionDecision, AdmissionPolicy, Segment};
use super::{EvictionContext, EvictionPlan};
use crate::Hash;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

#[derive(Debug)]
pub struct TinyLfuEstimator {
    inner: Mutex<CountMinSketch>,
}

impl TinyLfuEstimator {
    #[must_use]
    pub fn new(sketch_bytes: usize) -> Self {
        // one u8 per counter, ROWS(=4) rows: cols = bytes / 4.
        let cols = (sketch_bytes / 4).max(64);
        Self {
            inner: Mutex::new(CountMinSketch::new(cols)),
        }
    }
}

impl FrequencyEstimator for TinyLfuEstimator {
    fn observe(&self, hash: Hash) {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .increment(&hash);
    }
    fn estimate(&self, hash: Hash) -> u32 {
        u32::from(
            self.inner
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .estimate(&hash),
        )
    }
}

/// Admits a first-ever miss into `Probation`; once the shared estimator has
/// seen `promotion_threshold` prior sightings of a hash, admits straight to
/// `Main`. Reads `estimate` only — never calls `observe` (see the ordering
/// invariant in the module docs: a request must never count as evidence for
/// its own promotion).
#[derive(Debug)]
pub struct ProbationAdmission {
    pub freq: Arc<dyn FrequencyEstimator>,
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
/// shared estimator; ties break oldest-access-first (LRFU). Also owns the
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
    fn plan(&self, ctx: &EvictionContext) -> EvictionPlan {
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
            // Least frequent first; tie-break oldest access first.
            probation_members.sort_by(|a, b| a.1.cmp(&b.1).then(a.2.cmp(&b.2)));
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
        scored.sort_by(|a, b| a.1.cmp(&b.1).then(a.2.cmp(&b.2)));
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
mod tests {
    use super::*;
    fn h(b: u8) -> Hash {
        Hash::from([b; 32])
    }

    #[test]
    fn observe_and_estimate_round_trip() {
        let est = TinyLfuEstimator::new(1024);
        for _ in 0..3 {
            est.observe(h(42));
        }
        assert!(est.estimate(h(42)) >= 3);
        assert_eq!(est.estimate(h(43)), 0);
    }

    #[test]
    fn tinylfu_evicts_least_frequent_first_not_least_recent() {
        let freq: std::sync::Arc<dyn FrequencyEstimator> =
            std::sync::Arc::new(TinyLfuEstimator::new(4096));
        let hot = h(1);
        let cold = h(2);
        for _ in 0..20 {
            freq.observe(hot);
        } // hot: high frequency, touched long ago
        freq.observe(cold); // cold: low frequency, touched just now
        let now = std::time::Instant::now();
        let mut map = std::collections::HashMap::new();
        map.insert(
            hot,
            now.checked_sub(std::time::Duration::from_secs(100))
                .unwrap_or(now),
        ); // LRU would evict hot
        map.insert(cold, now);
        let candidates = crate::EvictionCandidates::from_map_for_test(map);
        let sizes = std::collections::HashMap::new();
        let segments = std::collections::HashMap::new();
        // Empty sizes (all 0) so `freed` never grows: with total > target and a
        // wide budget the whole set is planned in least-frequent-first order.
        let plan = TinyLfuEviction::new(freq, 2, 10).plan(&EvictionContext {
            candidates: &candidates,
            sizes: &sizes,
            segments: &segments,
            total_bytes: 1,
            target_bytes: 0,
            budget: 100,
            cache_bytes: 0,
        });
        assert_eq!(
            plan.evict.first().copied(),
            Some(cold),
            "least-frequent must go first"
        );
        assert!(plan.promote.is_empty());
    }

    #[test]
    fn probation_admission_first_sight_is_probation() {
        let freq: Arc<dyn FrequencyEstimator> = Arc::new(TinyLfuEstimator::new(4096));
        let pol = crate::policy::ProbationAdmission {
            freq: freq.clone(),
            promotion_threshold: 2,
        };
        let ctx = crate::policy::AdmissionContext {
            hash: h(1),
            known_size: None,
        };
        assert!(matches!(
            pol.admit(&ctx),
            crate::policy::AdmissionDecision::Store {
                segment: crate::policy::Segment::Probation
            }
        ));
        freq.observe(h(1));
        freq.observe(h(1));
        assert!(matches!(
            pol.admit(&ctx),
            crate::policy::AdmissionDecision::Store {
                segment: crate::policy::Segment::Main
            }
        ));
    }

    #[test]
    fn plan_promotes_hot_probation_member() {
        let freq: Arc<dyn FrequencyEstimator> = Arc::new(TinyLfuEstimator::new(4096));
        let hot = h(1);
        let cold = h(2);
        freq.observe(hot);
        freq.observe(hot); // hot: estimate >= threshold(2)
        // cold: estimate 0 (< threshold)
        let now = std::time::Instant::now();
        let mut cmap = std::collections::HashMap::new();
        cmap.insert(hot, now);
        cmap.insert(cold, now);
        let candidates = crate::EvictionCandidates::from_map_for_test(cmap);
        let sizes = std::collections::HashMap::new();
        let mut segments = std::collections::HashMap::new();
        segments.insert(hot, super::super::Segment::Probation);
        segments.insert(cold, super::super::Segment::Probation);
        let plan = TinyLfuEviction::new(freq, 2, 100).plan(&EvictionContext {
            candidates: &candidates,
            sizes: &sizes,
            segments: &segments,
            total_bytes: 0,
            target_bytes: 0,
            budget: 100,
            cache_bytes: 1000,
        });
        assert!(plan.promote.contains(&(hot, super::super::Segment::Main)));
        assert!(!plan.promote.iter().any(|(h, _)| *h == cold));
    }

    #[test]
    fn plan_caps_probation() {
        let freq: Arc<dyn FrequencyEstimator> = Arc::new(TinyLfuEstimator::new(4096));
        let a = h(1);
        let b = h(2);
        // both cold, no promotion. sizes push probation footprint over cap.
        let now = std::time::Instant::now();
        let mut cmap = std::collections::HashMap::new();
        cmap.insert(a, now);
        cmap.insert(b, now);
        let candidates = crate::EvictionCandidates::from_map_for_test(cmap);
        let mut sizes = std::collections::HashMap::new();
        sizes.insert(a, 60u64);
        sizes.insert(b, 60u64);
        let mut segments = std::collections::HashMap::new();
        segments.insert(a, super::super::Segment::Probation);
        segments.insert(b, super::super::Segment::Probation);
        // cache_bytes=1000, probation_target_pct=10 -> limit=100. footprint=120 > 100.
        // total_bytes under target_bytes so global loop wouldn't evict anything.
        let plan = TinyLfuEviction::new(freq, 2, 10).plan(&EvictionContext {
            candidates: &candidates,
            sizes: &sizes,
            segments: &segments,
            total_bytes: 10,
            target_bytes: 1000,
            budget: 100,
            cache_bytes: 1000,
        });
        assert!(!plan.evict.is_empty(), "cap must evict from probation");
        assert!(plan.evict.contains(&a) || plan.evict.contains(&b));
    }
}
