//! Count-min frequency sketch keyed directly on BLAKE3 hashes (ADR 040).
//!
//! Keys are already uniform 32-byte hashes, so each row's column index is a
//! disjoint 4-byte slice of the key reduced mod the sketch's width — no hash
//! functions. Counters are saturating `u8` — classic `TinyLFU` uses 4-bit
//! counters, and the extra headroom is free at one byte each. Halving ages them.
//!
//! [`ShardedCountMinSketch`] is the form the estimator uses. It splits the
//! counter array into [`SHARDS`] independently locked [`CountMinSketch`]es of
//! `cols / SHARDS` columns each, routed by a 4-byte slice of the key that no
//! row index reads. An `increment` or an `estimate` locks exactly one shard.
//!
//! Two properties of one wide sketch of `cols` columns are load-bearing for the
//! policies that read the estimates, and sharding keeps both:
//!
//! - **Aging cadence is global, not per shard.** Both readers compare estimates
//!   against a shared scale: [`super::tinylfu::ProbationAdmission`] tests one
//!   estimate against a node-wide `promotion_threshold`, and
//!   [`super::tinylfu::TinyLfuEviction`] ranks candidates from *different*
//!   shards least-frequent-first. Counters are only comparable if every counter
//!   is caught up to the same clock when it is read. So the halving clock is one
//!   node-wide observation counter, and a shard halves lazily — on its next
//!   touch — by however many windows have elapsed since it last caught up. The
//!   *work* stays sharded (`1 / SHARDS` of the array under one shard's lock,
//!   landing on one observer), while the *cadence* stays global. A shard clocked
//!   on its own traffic would age at a rate set by key skew, which is exactly
//!   what a frequency sketch must not do.
//! - **Per-row collision rate.** Two distinct keys share a row counter only if
//!   they share a shard (probability `1 / SHARDS`) and then a column within it
//!   (`SHARDS / cols`) — the same `1 / cols` a single wide sketch gives, because
//!   the shard slice and the row slices are disjoint bytes of a uniform hash.
//!
//! The over-report rate carries over as exactly as the per-row rate does. The
//! sketch reads a key hotter than it is when every one of its `ROWS` counters
//! also holds some other key's count. The polluting keys need not be the same
//! one across rows, so "two keys collide in all `ROWS` rows" is a far rarer
//! event that does not bound the error — do not size the sketch from it. For
//! `N` live keys the rate is `(1 - e^(-N / cols))^ROWS`, and [`SHARDS`] cancels
//! out: a shard divides the columns and the keys in the same proportion. So
//! sharding costs nothing in accuracy and the width alone sets it. The rate
//! turns on `N / cols`, which holds the shipped `cols = 65536` under one
//! percent out to roughly 25000 live keys. See
//! [`super::tinylfu::TinyLfuEstimator::new`] for the sizing and its floor.
use crate::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

const ROWS: usize = 4;

/// Number of independently locked shards in a [`ShardedCountMinSketch`].
///
/// A concurrency knob: the total counter budget, the per-row collision rate, and
/// the aging cadence are all held constant across it (see the module docs), so
/// raising or lowering it moves contention and halving granularity without
/// moving any admission or eviction decision. The one quantity it does move is
/// the whole-sketch over-report rate, by `SHARDS^(ROWS-1)` — negligible at the
/// shipped sizing, and bounded in the module docs.
pub const SHARDS: usize = 16;

/// Bytes of key each index consumes: one `u32` per row, and one for the shard.
const SLICE_BYTES: usize = 4;

/// Byte offset of the shard-routing slice. It is derived from the bytes the row
/// indices consume, so shard choice and column choice stay independent functions
/// of the key however many rows the sketch has. The module docs' collision-rate
/// argument rests on that independence, and a stride or row count that put a row
/// index on the shard slice would correlate the two while leaving every test in
/// this file green — hence the derivation rather than a literal offset.
const SHARD_SLICE_START: usize = ROWS * SLICE_BYTES;

// The derivation keeps the slices disjoint; this bounds them to the key. Past
// `ROWS = 7` the shard slice runs off the end of a hash, `word_at` reads zero
// for every key, and the sharding collapses to one shard.
const _: () = assert!(
    SHARD_SLICE_START + SLICE_BYTES <= size_of::<Hash>(),
    "the shard slice must fit inside the key"
);

/// Observations per node-wide aging window, per column of total width. A sketch
/// `cols` wide halves every `cols * AGING_WINDOW_PER_COL` observations.
const AGING_WINDOW_PER_COL: u64 = 10;

/// Read the little-endian word of [`SLICE_BYTES`] at `start` in `key`, or `0` if
/// the key is shorter than the slice (unreachable for a 32-byte BLAKE3 hash, but expressed
/// as a total function rather than an index that could panic).
fn word_at(key: &Hash, start: usize) -> u32 {
    let bytes = key.as_bytes();
    let slice = bytes
        .get(start..start.saturating_add(SLICE_BYTES))
        .unwrap_or(&[]);
    u32::from_le_bytes([
        *slice.first().unwrap_or(&0),
        *slice.get(1).unwrap_or(&0),
        *slice.get(2).unwrap_or(&0),
        *slice.get(3).unwrap_or(&0),
    ])
}

/// A plain count-min counter array. It carries no aging clock of its own:
/// [`ShardedCountMinSketch`] owns the node-wide clock and calls [`Self::halve`],
/// which is what keeps every shard on one cadence.
#[derive(Debug)]
pub struct CountMinSketch {
    cols: usize,
    counters: Vec<u8>, // ROWS * cols, row-major
}

impl CountMinSketch {
    /// A zeroed sketch `cols` counters wide per row. A `cols` of `0` is
    /// raised to `1` so the modulo is always defined.
    #[must_use]
    pub fn new(cols: usize) -> Self {
        let cols = cols.max(1);
        Self {
            cols,
            counters: vec![0u8; ROWS * cols],
        }
    }

    /// Record one access to `key`, saturating each row's counter rather
    /// than wrapping.
    pub fn increment(&mut self, key: &Hash) {
        for row in 0..ROWS {
            let word = word_at(key, row.saturating_mul(SLICE_BYTES));
            let col = (word as usize) % self.cols;
            if let Some(c) = self.counters.get_mut(row * self.cols + col) {
                *c = c.saturating_add(1);
            }
        }
    }

    /// Age every counter by `times` halvings. A `times` at or past the counter
    /// width drains the counter to zero — a shard cold for that many windows has
    /// nothing left to age — rather than overflowing the shift.
    pub fn halve(&mut self, times: u32) {
        if times == 0 {
            return;
        }
        for c in &mut self.counters {
            *c = c.checked_shr(times).unwrap_or(0);
        }
    }

    /// The count-min estimate for `key`: the smallest of its per-row
    /// counters, which bounds the collision overcount.
    #[must_use]
    pub fn estimate(&self, key: &Hash) -> u8 {
        let mut min = u8::MAX;
        for row in 0..ROWS {
            let word = word_at(key, row.saturating_mul(SLICE_BYTES));
            let col = (word as usize) % self.cols;
            min = min.min(*self.counters.get(row * self.cols + col).unwrap_or(&0));
        }
        min
    }
}

/// One shard's counters plus the aging epoch they have already been caught up
/// to. `epoch` trails the sketch's node-wide epoch until the shard is next
/// touched; [`ShardedCountMinSketch::catch_up`] closes the gap.
#[derive(Debug)]
struct Shard {
    sketch: CountMinSketch,
    epoch: u64,
}

/// A [`CountMinSketch`] split into [`SHARDS`] independently locked pieces on one
/// node-wide aging clock.
///
/// Every counter a given key touches lives in one shard, so an `increment` or an
/// `estimate` takes exactly one shard lock and reads or writes `ROWS` counters.
/// Halving is per shard and lazy: the shard is aged on its next touch by the
/// number of node-wide windows that have elapsed since it last caught up. So the
/// work is `1 / SHARDS` of the array under one lock, while every counter is
/// caught up to the node-wide clock at the moment it is read. A ranking pass
/// reads each candidate separately, so it carries one halving of skew for every
/// window boundary it crosses. A halving applied evenly across a pass cannot
/// invert the ranking — it only creates ties — but candidates read on opposite
/// sides of a boundary can misorder, and two whose estimates are within a
/// factor of two can swap. The window is `cols * 10` observations, so a sweep
/// crossing even one boundary is already the uncommon case, and the next sweep
/// re-reads both on one side of it.
///
/// The interior mutability is deliberate — the frequency estimator is shared by
/// every serve completion and every fill-path admission read, so both take
/// `&self` and neither can serialize on a single process-wide lock.
#[derive(Debug)]
pub struct ShardedCountMinSketch {
    shards: Vec<Mutex<Shard>>,
    /// Node-wide observation count. Divided by `window` it gives the current
    /// aging epoch, so the epoch needs no separate atomic and no reset — there
    /// is no read-modify-write for two observers to race on.
    observations: AtomicU64,
    /// Observations per aging window, node-wide. Equals `total_cols * 10`, the
    /// window a single sketch of the same total width would halve on.
    window: u64,
}

impl ShardedCountMinSketch {
    /// Build a sketch of `cols` total columns per row, spread over [`SHARDS`]
    /// shards. The per-shard width rounds up, so the realized total is at least
    /// `cols`.
    ///
    /// A `cols` below [`SHARDS`] yields one shard per column, which is a
    /// degenerate sketch rather than a smaller one: at `cols <= SHARDS` every
    /// shard is one column wide, so all `ROWS` rows of a shard index the same
    /// counter and the min-over-rows stops discriminating between keys that
    /// share a shard. Callers wanting a small sketch should floor `cols` well
    /// above [`SHARDS`], as [`super::tinylfu::TinyLfuEstimator::new`] does.
    #[must_use]
    pub fn new(cols: usize) -> Self {
        let cols = cols.max(1);
        let shard_count = SHARDS.min(cols);
        let per_shard = cols.div_ceil(shard_count);
        let total_cols = per_shard.saturating_mul(shard_count) as u64;
        Self {
            shards: (0..shard_count)
                .map(|_| {
                    Mutex::new(Shard {
                        sketch: CountMinSketch::new(per_shard),
                        epoch: 0,
                    })
                })
                .collect(),
            observations: AtomicU64::new(0),
            window: total_cols.saturating_mul(AGING_WINDOW_PER_COL).max(1),
        }
    }

    /// The shard owning `key`. Derived from a byte slice the row indices do not
    /// read, so a key's shard and its per-row columns are independent.
    fn shard_of(&self, key: &Hash) -> Option<&Mutex<Shard>> {
        let count = self.shards.len();
        if count == 0 {
            return None;
        }
        let idx = (word_at(key, SHARD_SLICE_START) as usize) % count;
        self.shards.get(idx)
    }

    /// Age `shard` up to `epoch` before it is read or written, so a counter is
    /// caught up to the node-wide clock at the moment it is observed. `epoch`
    /// never regresses a shard: a caller that computed a stale one finds
    /// `elapsed` at zero and leaves the counters alone.
    fn catch_up(shard: &mut Shard, epoch: u64) {
        let elapsed = epoch.saturating_sub(shard.epoch);
        if elapsed == 0 {
            return;
        }
        shard
            .sketch
            .halve(u32::try_from(elapsed).unwrap_or(u32::MAX));
        shard.epoch = epoch;
    }

    /// Count one sighting of `key`, locking only its shard.
    ///
    /// A poisoned shard recovers its inner sketch: the counters are approximate
    /// popularity evidence, and refusing to count into a shard would silently
    /// freeze the frequency of every key routed to it.
    pub fn increment(&self, key: &Hash) {
        // Claim this observation's slot on the node-wide clock before writing,
        // so at any instant the clock already counts every sighting committed to
        // a shard. An estimator loads the clock outside the shard lock, so the
        // epoch it applies is stale by whatever lands between its load and its
        // acquisition: it under-ages, never over-ages. Nothing bounds that gap
        // in principle; a window is `cols * 10` observations, so in practice it
        // is zero.
        let observed = self
            .observations
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let epoch = observed / self.window;
        if let Some(shard) = self.shard_of(key) {
            let mut guard = shard.lock().unwrap_or_else(PoisonError::into_inner);
            Self::catch_up(&mut guard, epoch);
            guard.sketch.increment(key);
        }
    }

    /// The count-min estimate for `key`, locking only its shard. An absent shard
    /// (only reachable from an empty sketch) reads as never seen.
    ///
    /// Reading ages the shard too: a key in a shard that has seen no traffic for
    /// several windows must not read back at the value it held before those
    /// windows elapsed, or a cold key would outrank a warm one.
    #[must_use]
    pub fn estimate(&self, key: &Hash) -> u8 {
        let epoch = self.observations.load(Ordering::Relaxed) / self.window;
        self.shard_of(key).map_or(0, |shard| {
            let mut guard = shard.lock().unwrap_or_else(PoisonError::into_inner);
            Self::catch_up(&mut guard, epoch);
            guard.sketch.estimate(key)
        })
    }
}

#[cfg(test)]
mod tests;
