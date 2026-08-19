//! W-TinyLFU estimator + policies (filled in Stage B, Tasks 5-7).
use super::EvictionPolicy;
use super::FrequencyEstimator;
use super::sketch::CountMinSketch;
use super::{EvictionContext, EvictionPlan};
use crate::Hash;
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

/// Ranks eviction candidates least-frequent-first, reading frequency from a
/// shared estimator; ties break oldest-access-first (LRFU).
#[derive(Debug)]
pub struct TinyLfuEviction {
    freq: Arc<dyn FrequencyEstimator>,
}

impl TinyLfuEviction {
    #[must_use]
    pub fn new(freq: Arc<dyn FrequencyEstimator>) -> Self {
        Self { freq }
    }
}

impl EvictionPolicy for TinyLfuEviction {
    fn plan(&self, ctx: &EvictionContext) -> EvictionPlan {
        let mut scored: Vec<(Hash, u32, std::time::Instant)> = ctx
            .candidates
            .iter()
            .map(|(h, t)| (*h, self.freq.estimate(*h), *t))
            .collect();
        // Least frequent first; tie-break oldest access first (LRFU).
        scored.sort_by(|a, b| a.1.cmp(&b.1).then(a.2.cmp(&b.2)));
        // Same target/budget loop as LRU. Promotion/probation cap is Task 8.
        let mut evict = Vec::new();
        let mut freed = 0u64;
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
        EvictionPlan {
            evict,
            promote: Vec::new(),
        }
    }

    fn on_access(&self, hash: Hash) {
        self.freq.observe(hash);
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
        let plan = TinyLfuEviction::new(freq).plan(&EvictionContext {
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
}
