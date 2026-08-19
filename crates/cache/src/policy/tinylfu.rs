//! W-TinyLFU estimator + policies (filled in Stage B, Tasks 5-7).
use super::EvictionPolicy;
use super::FrequencyEstimator;
use super::sketch::CountMinSketch;
use crate::{EvictionCandidates, Hash};
use std::collections::HashMap;
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
    fn select_victims(
        &self,
        candidates: &EvictionCandidates,
        _sizes: &HashMap<Hash, u64>,
    ) -> Vec<Hash> {
        let mut scored: Vec<(Hash, u32, std::time::Instant)> = candidates
            .iter()
            .map(|(h, t)| (*h, self.freq.estimate(*h), *t))
            .collect();
        // Least frequent first; tie-break oldest access first (LRFU).
        scored.sort_by(|a, b| a.1.cmp(&b.1).then(a.2.cmp(&b.2)));
        scored.into_iter().map(|(h, _, _)| h).collect()
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
        let victims = TinyLfuEviction::new(freq)
            .select_victims(&candidates, &std::collections::HashMap::new());
        assert_eq!(
            victims.first().copied(),
            Some(cold),
            "least-frequent must go first"
        );
    }
}
