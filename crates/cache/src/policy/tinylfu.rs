//! W-TinyLFU estimator + policies (filled in Stage B, Tasks 5-7).
use super::FrequencyEstimator;
use super::sketch::CountMinSketch;
use crate::Hash;
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
}
