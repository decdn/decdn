//! Count-min frequency sketch keyed directly on BLAKE3 hashes (ADR 040).
//!
//! Keys are already uniform 32-byte hashes, so each row's column index is a
//! disjoint 4-byte slice of the key reduced mod the sketch's width — no hash
//! functions. Counters are 4-bit-style saturating `u8`; periodic halving ages
//! them.
//!
//! [`ShardedCountMinSketch`] is the form the estimator uses. It splits the
//! counter array into [`SHARDS`] independently locked [`CountMinSketch`]es of
//! `cols / SHARDS` columns each, routed by a 4-byte slice of the key that no
//! row index reads. Two properties carry over exactly from one wide sketch of
//! `cols` columns:
//!
//! - **Collision rate.** Two distinct keys share a row counter only if they
//!   share a shard (probability `1 / SHARDS`) and then a column within it
//!   (`SHARDS / cols`) — the same `1 / cols` a single wide sketch gives, because
//!   the shard slice and the row slices are disjoint bytes of a uniform hash.
//! - **Aging cadence.** Each shard halves after `10 x` its own width in
//!   observations, so with keys spread evenly a shard ages once per `cols * 10`
//!   observations node-wide — the single sketch's window. The halving walks one
//!   shard, so it costs `1 / SHARDS` of a whole-array pass and lands on one
//!   observer instead of stalling every one of them.
use crate::Hash;
use std::sync::{Mutex, PoisonError};

const ROWS: usize = 4;

/// Number of independently locked shards in a [`ShardedCountMinSketch`].
///
/// A concurrency knob only: the total counter budget, the per-row collision
/// rate, and the aging window are all held constant across it (see the module
/// docs), so raising or lowering it moves contention and halving granularity
/// without moving any admission or eviction decision.
pub const SHARDS: usize = 16;

/// Byte offset of the 4-byte shard-routing slice. It sits past the
/// `ROWS * 4 = 16` bytes the row indices consume, so shard choice and column
/// choice are independent functions of the key.
const SHARD_SLICE_START: usize = 16;

/// Read the 4-byte little-endian word at `start` in `key`, or `0` if the key is
/// shorter than the slice (unreachable for a 32-byte BLAKE3 hash, but expressed
/// as a total function rather than an index that could panic).
fn word_at(key: &Hash, start: usize) -> u32 {
    let bytes = key.as_bytes();
    let slice = bytes.get(start..start.saturating_add(4)).unwrap_or(&[]);
    u32::from_le_bytes([
        *slice.first().unwrap_or(&0),
        *slice.get(1).unwrap_or(&0),
        *slice.get(2).unwrap_or(&0),
        *slice.get(3).unwrap_or(&0),
    ])
}

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
        for row in 0..ROWS {
            let word = word_at(key, row.saturating_mul(4));
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
        let mut min = u8::MAX;
        for row in 0..ROWS {
            let word = word_at(key, row.saturating_mul(4));
            let col = (word as usize) % self.cols;
            min = min.min(*self.counters.get(row * self.cols + col).unwrap_or(&0));
        }
        min
    }
}

/// A [`CountMinSketch`] split into [`SHARDS`] independently locked pieces.
///
/// Every counter a given key touches lives in one shard, so an `increment` or
/// an `estimate` takes exactly one shard lock and reads or writes `ROWS`
/// counters. The periodic halving is per shard: it ages `1 / SHARDS` of the
/// counters, under that one shard's lock, while the other shards keep serving.
///
/// The interior mutability is deliberate — the frequency estimator is shared by
/// every serve completion and every fill-path admission read, so both take
/// `&self` and neither can serialize on a single process-wide lock.
#[derive(Debug)]
pub struct ShardedCountMinSketch {
    shards: Vec<Mutex<CountMinSketch>>,
}

impl ShardedCountMinSketch {
    /// Build a sketch of `cols` total columns per row, spread over [`SHARDS`]
    /// shards. The per-shard width rounds up, so the realized total is at least
    /// `cols`. A `cols` smaller than [`SHARDS`] yields one shard per column
    /// rather than empty shards.
    #[must_use]
    pub fn new(cols: usize) -> Self {
        let cols = cols.max(1);
        let shard_count = SHARDS.min(cols);
        let per_shard = cols.div_ceil(shard_count);
        Self {
            shards: (0..shard_count)
                .map(|_| Mutex::new(CountMinSketch::new(per_shard)))
                .collect(),
        }
    }

    /// The shard owning `key`. Derived from a byte slice the row indices do not
    /// read, so a key's shard and its per-row columns are independent.
    fn shard_of(&self, key: &Hash) -> Option<&Mutex<CountMinSketch>> {
        let count = self.shards.len();
        if count == 0 {
            return None;
        }
        let idx = (word_at(key, SHARD_SLICE_START) as usize) % count;
        self.shards.get(idx)
    }

    /// Count one sighting of `key`, locking only its shard.
    ///
    /// A poisoned shard recovers its inner sketch: the counters are approximate
    /// popularity evidence, and refusing to count into a shard would silently
    /// freeze the frequency of every key routed to it.
    pub fn increment(&self, key: &Hash) {
        if let Some(shard) = self.shard_of(key) {
            shard
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .increment(key);
        }
    }

    /// The count-min estimate for `key`, locking only its shard. An absent
    /// shard (only reachable from an empty sketch) reads as never seen.
    #[must_use]
    pub fn estimate(&self, key: &Hash) -> u8 {
        self.shard_of(key).map_or(0, |shard| {
            shard
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .estimate(key)
        })
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

    #[test]
    fn sharded_estimate_rises_with_increments() {
        let s = ShardedCountMinSketch::new(256);
        for _ in 0..5 {
            s.increment(&h(7));
        }
        assert!(s.estimate(&h(7)) >= 5);
        assert_eq!(s.estimate(&h(9)), 0); // unseen key
    }

    #[test]
    fn sharded_aging_bounds_counters() {
        let s = ShardedCountMinSketch::new(4); // tiny → per-shard window small
        for _ in 0..1000 {
            s.increment(&h(1));
        }
        assert!(
            s.estimate(&h(1)) < u8::MAX,
            "per-shard halving must keep counters bounded, not overflow"
        );
    }

    /// Sharding is a concurrency change, not a policy change: for the same load
    /// of collision-free keys the sharded sketch reports exactly what one wide
    /// sketch of the same total width reports, so every admission and eviction
    /// decision reading these estimates is unchanged.
    #[test]
    fn sharded_estimates_match_the_single_sketch() {
        let mut single = CountMinSketch::new(4096);
        let sharded = ShardedCountMinSketch::new(4096);
        for i in 0..64u8 {
            for _ in 0..=(i % 7) {
                single.increment(&h(i));
                sharded.increment(&h(i));
            }
        }
        for i in 0..64u8 {
            assert_eq!(
                sharded.estimate(&h(i)),
                single.estimate(&h(i)),
                "sharded and single-sketch estimates must agree for key {i}"
            );
        }
        assert_eq!(sharded.estimate(&h(200)), single.estimate(&h(200)));
    }

    /// A key's counters live in exactly one shard, so a shard reachable from a
    /// hot key must not report a cold key that routes elsewhere.
    #[test]
    fn shards_hold_disjoint_keys() {
        let s = ShardedCountMinSketch::new(4096);
        for _ in 0..40 {
            s.increment(&h(1));
        }
        let cold_and_zero = (2u8..64).filter(|i| s.estimate(&h(*i)) == 0).count();
        assert!(
            cold_and_zero >= 60,
            "increments of one key must not raise unrelated keys, {cold_and_zero} of 62 stayed 0"
        );
    }
}
