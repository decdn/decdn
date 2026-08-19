//! Count-min frequency sketch keyed directly on BLAKE3 hashes (ADR 040).
//!
//! Keys are already uniform 32-byte hashes, so each row's column index is a
//! disjoint 4-byte slice of the key reduced mod `cols` — no hash functions.
//! Counters are 4-bit-style saturating `u8`; periodic halving ages them.
use crate::Hash;

const ROWS: usize = 4;

#[derive(Debug)]
pub struct CountMinSketch {
    cols: usize,
    counters: Vec<u8>, // ROWS * cols, row-major
    sample: u64,
    sample_max: u64,
}

impl CountMinSketch {
    #[must_use]
    pub fn new(cols: usize) -> Self {
        let cols = cols.max(1);
        Self {
            cols,
            counters: vec![0u8; ROWS * cols],
            sample: 0,
            sample_max: (cols as u64).saturating_mul(10), // reset window ~ 10x width
        }
    }

    pub fn increment(&mut self, key: &Hash) {
        let bytes = key.as_bytes(); // [u8; 32]
        for row in 0..ROWS {
            let start = row * 4;
            let slice = bytes.get(start..start + 4).unwrap_or(&[0, 0, 0, 0]);
            let word = u32::from_le_bytes([
                *slice.first().unwrap_or(&0),
                *slice.get(1).unwrap_or(&0),
                *slice.get(2).unwrap_or(&0),
                *slice.get(3).unwrap_or(&0),
            ]);
            let col = (word as usize) % self.cols;
            if let Some(c) = self.counters.get_mut(row * self.cols + col) {
                *c = c.saturating_add(1);
            }
        }
        self.sample = self.sample.saturating_add(1);
        if self.sample >= self.sample_max {
            for c in &mut self.counters {
                *c >>= 1;
            }
            self.sample = 0;
        }
    }

    #[must_use]
    pub fn estimate(&self, key: &Hash) -> u8 {
        let bytes = key.as_bytes();
        let mut min = u8::MAX;
        for row in 0..ROWS {
            let start = row * 4;
            let slice = bytes.get(start..start + 4).unwrap_or(&[0, 0, 0, 0]);
            let word = u32::from_le_bytes([
                *slice.first().unwrap_or(&0),
                *slice.get(1).unwrap_or(&0),
                *slice.get(2).unwrap_or(&0),
                *slice.get(3).unwrap_or(&0),
            ]);
            let col = (word as usize) % self.cols;
            min = min.min(*self.counters.get(row * self.cols + col).unwrap_or(&0));
        }
        min
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Hash;
    fn h(b: u8) -> Hash {
        Hash::from([b; 32])
    }

    #[test]
    fn estimate_rises_with_increments() {
        let mut s = CountMinSketch::new(256);
        for _ in 0..5 {
            s.increment(&h(7));
        }
        assert!(s.estimate(&h(7)) >= 5);
        assert_eq!(s.estimate(&h(9)), 0); // unseen key
    }

    #[test]
    fn aging_halves_counters() {
        let mut s = CountMinSketch::new(4); // tiny → sample_max small → forces halving
        for _ in 0..1000 {
            s.increment(&h(1));
        }
        let before = s.estimate(&h(1));
        // after enough increments the periodic halving must have kept counters bounded
        assert!(before < u8::MAX, "counters must saturate/age, not overflow");
    }
}
