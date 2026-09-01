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
    #[must_use]
    /// A zeroed sketch `cols` counters wide per row. A `cols` of `0` is
    /// raised to `1` so the modulo is always defined.
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

    #[must_use]
    /// The count-min estimate for `key`: the smallest of its per-row
    /// counters, which bounds the collision overcount.
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
mod tests {
    use super::*;
    use crate::Hash;

    /// Degenerate keys: all 32 bytes equal, so every row word AND the shard word
    /// are the same `u32`. Only for the bare [`CountMinSketch`] tests, which do
    /// not depend on the row slices being independent. Sharded tests must use
    /// [`key`] — see its docs.
    fn h(b: u8) -> Hash {
        Hash::from([b; 32])
    }

    /// Full-entropy test keys.
    ///
    /// The sharded sketch's correctness argument rests on the shard slice
    /// (bytes 16..20) being independent of the four row slices (bytes 0..16).
    /// `[b; 32]` keys make them perfectly *correlated* and put every row on one
    /// column, so a test built on them cannot observe a routing bug at all.
    /// Hashing the index restores the uniformity the sketch assumes.
    fn key(i: u32) -> Hash {
        Hash::new(i.to_le_bytes())
    }

    /// Total width used by the equivalence tests. Large enough that a
    /// four-row collision among the key set is vanishingly unlikely on either
    /// side, and an exact multiple of `SHARDS` so the sharded per-shard width
    /// does not round up past the reference width.
    const EQUIV_COLS: usize = 4096;

    /// Reference implementation: one wide sketch on a single global clock, i.e.
    /// the unsharded behaviour the sharded form must reproduce. Halving is
    /// driven here rather than inside `CountMinSketch` because the clock belongs
    /// to the sharded wrapper.
    struct SingleSketch {
        inner: CountMinSketch,
        seen: u64,
        window: u64,
    }

    impl SingleSketch {
        fn new(cols: usize) -> Self {
            Self {
                inner: CountMinSketch::new(cols),
                seen: 0,
                window: (cols as u64).saturating_mul(AGING_WINDOW_PER_COL),
            }
        }
        fn increment(&mut self, k: &Hash) {
            self.inner.increment(k);
            self.seen = self.seen.saturating_add(1);
            if self.seen >= self.window {
                self.inner.halve(1);
                self.seen = 0;
            }
        }
        fn estimate(&self, k: &Hash) -> u8 {
            self.inner.estimate(k)
        }
    }

    /// Deterministic skewed load: four sightings of one hot key for every
    /// sighting of a rotating cold key. Skew is the point — a uniform load
    /// cannot tell a global aging clock from a per-shard one, because with
    /// traffic spread evenly every shard reaches its own window at the same
    /// time anyway.
    fn drive_skewed(keys: &[Hash], observations: usize, mut sight: impl FnMut(&Hash)) {
        let cold = keys.len().saturating_sub(1).max(1);
        for n in 0..observations {
            let k = if n % 5 == 0 {
                keys.get(1 + (n / 5) % cold)
            } else {
                keys.first()
            };
            if let Some(k) = k {
                sight(k);
            }
        }
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
    fn halve_ages_counters_and_saturates_to_zero() {
        let mut s = CountMinSketch::new(4);
        for _ in 0..100 {
            s.increment(&h(1));
        }
        let before = s.estimate(&h(1));
        s.halve(1);
        assert_eq!(
            s.estimate(&h(1)),
            before / 2,
            "one halving is one right shift"
        );
        s.halve(0);
        assert_eq!(
            s.estimate(&h(1)),
            before / 2,
            "a zero-window catch-up is inert"
        );
        s.halve(u32::MAX);
        assert_eq!(
            s.estimate(&h(1)),
            0,
            "a catch-up past the counter width drains it rather than overflowing the shift"
        );
    }

    #[test]
    fn sharded_estimate_rises_with_increments() {
        let s = ShardedCountMinSketch::new(256);
        for _ in 0..5 {
            s.increment(&key(7));
        }
        assert!(s.estimate(&key(7)) >= 5);
        assert_eq!(s.estimate(&key(9)), 0); // unseen key
    }

    /// The aging clock is node-wide, not per shard: traffic to *other* shards
    /// ages this one. That is the property that keeps estimates from different
    /// shards comparable, and it is what a per-shard clock (each shard halving
    /// on its own sightings) gets wrong — there, a shard seeing no traffic never
    /// ages at all, however busy the node is.
    #[test]
    fn aging_is_driven_by_node_wide_traffic_not_the_shard_s_own() -> anyhow::Result<()> {
        let s = ShardedCountMinSketch::new(64); // small → short node-wide window
        let hot = key(1);
        for _ in 0..40 {
            s.increment(&hot);
        }
        let before = s.estimate(&hot);
        assert!(
            before >= 40,
            "the hot key must be counted before it is aged"
        );

        // Drive a full node-wide window through keys that route elsewhere,
        // without touching the hot key again.
        let Some(hot_shard) = s.shard_of(&hot) else {
            anyhow::bail!("a non-empty sketch must route every key to a shard");
        };
        let others: Vec<Hash> = (2u32..4096)
            .map(key)
            .filter(|k| s.shard_of(k).is_some_and(|o| !std::ptr::eq(o, hot_shard)))
            .take(64)
            .collect();
        assert!(
            !others.is_empty(),
            "some key must route outside the hot shard"
        );
        // Read the realized window off the sketch, never a local copy: a change
        // to SHARDS moves the realized width, and a stale copy would drive less
        // than a full window and silently test nothing.
        let window = usize::try_from(s.window)?;
        for n in 0..window {
            if let Some(k) = others.get(n % others.len()) {
                s.increment(k);
            }
        }

        assert!(
            s.estimate(&hot) < before,
            "a node-wide aging window must halve an untouched shard: {before} -> {}",
            s.estimate(&hot)
        );
        Ok(())
    }

    /// A shard idle across many aging windows catches up by *every* window it
    /// missed, not by one.
    ///
    /// Lazy catch-up is the mechanism that lets the halving work stay sharded
    /// while the cadence stays node-wide, and it is the only place a shard's
    /// elapsed-window count is read. A node with low key cardinality leaves most
    /// shards untouched across many windows, so a blob routing into a
    /// long-cold shard is exactly the case that must read back drained.
    ///
    /// Every value here is deterministic, so the assertions are exact. That is
    /// what discriminates the two ways to get this wrong: halving once per touch
    /// regardless of `elapsed` reads 20 at eight windows instead of 0, and
    /// halving by `elapsed - 1` reads 40 at one window and 20 at two, where a
    /// correct catch-up reads 20 and 10. A "did it shrink" check passes against
    /// both.
    #[test]
    fn a_shard_cold_for_many_windows_catches_up_by_all_of_them() -> anyhow::Result<()> {
        let hot = key(1);

        // Sight the hot key, then drive `windows` full node-wide windows through
        // keys that route to other shards, so the hot shard is only ever aged by
        // the catch-up on the final read.
        let drive = |windows: usize| -> anyhow::Result<u8> {
            let s = ShardedCountMinSketch::new(64); // small → short node-wide window
            // Read the window off the sketch rather than recomputing it: a
            // change to SHARDS moves the realized width, and a stale local copy
            // would leave the first arm driving less than a full window and
            // silently testing nothing.
            let window = usize::try_from(s.window)?;
            for _ in 0..40 {
                s.increment(&hot);
            }
            let Some(hot_shard) = s.shard_of(&hot) else {
                anyhow::bail!("a non-empty sketch must route every key to a shard");
            };
            let others: Vec<Hash> = (2u32..4096)
                .map(key)
                .filter(|k| s.shard_of(k).is_some_and(|o| !std::ptr::eq(o, hot_shard)))
                .take(64)
                .collect();
            anyhow::ensure!(
                !others.is_empty(),
                "some key must route outside the hot shard"
            );
            for n in 0..window.saturating_mul(windows) {
                if let Some(k) = others.get(n % others.len()) {
                    s.increment(k);
                }
            }
            Ok(s.estimate(&hot))
        };

        let one = drive(1)?;
        anyhow::ensure!(
            one == 20,
            "one window halves 40 once: expected 20, got {one}"
        );

        let two = drive(2)?;
        anyhow::ensure!(
            two == 10,
            "two windows halve 40 twice: expected 10, got {two}"
        );

        // 40 needs six halvings to reach zero, so eight windows drains it with
        // room to spare whatever the counter started at.
        let many = drive(8)?;
        anyhow::ensure!(many == 0, "eight windows drain 40 to zero, got {many}");
        Ok(())
    }

    /// Sharding is a concurrency change, not a policy change: under a *skewed*
    /// load driven well past several aging windows, the sharded sketch reports
    /// what one wide sketch of the same total width reports, so every admission
    /// and eviction decision reading these estimates is unchanged.
    ///
    /// The skew and the length are both load-bearing. Aging is the only place
    /// sharding can diverge, so a load that never reaches a halving window
    /// compares two exact counters and passes against any implementation. A
    /// per-shard clock (each shard halving on its own traffic) mismatches on
    /// essentially every key here.
    #[test]
    fn sharded_matches_the_single_sketch_across_aging_windows() {
        let keys: Vec<Hash> = (0..200).map(key).collect();
        let mut single = SingleSketch::new(EQUIV_COLS);
        let sharded = ShardedCountMinSketch::new(EQUIV_COLS);
        drive_skewed(&keys, 200_000, |k| {
            single.increment(k);
            sharded.increment(k);
        });

        let mismatched = keys
            .iter()
            .filter(|k| sharded.estimate(k) != single.estimate(k))
            .count();
        // Not exact equality: the two forms reduce columns mod different widths
        // (per-shard vs total), so an occasional four-row collision on one side
        // and not the other is expected and harmless. A clock regression moves
        // this to ~every key, so the bound discriminates with room to spare.
        assert!(
            mismatched <= 5,
            "sharded and single-sketch estimates diverged on {mismatched} of {} keys",
            keys.len()
        );
    }

    /// The property `TinyLfuEviction::plan` actually depends on: it ranks
    /// candidates from different shards least-frequent-first, so estimates must
    /// stay comparable *across* shards. Ordering, not the absolute value, is
    /// what a divergent aging clock destroys.
    #[test]
    fn sharded_preserves_cross_shard_ordering_under_skew() {
        let keys: Vec<Hash> = (0..200).map(key).collect();
        let mut single = SingleSketch::new(EQUIV_COLS);
        let sharded = ShardedCountMinSketch::new(EQUIV_COLS);
        drive_skewed(&keys, 200_000, |k| {
            single.increment(k);
            sharded.increment(k);
        });

        let mut compared = 0usize;
        let mut inverted = 0usize;
        for (i, a) in keys.iter().enumerate() {
            for b in keys.iter().skip(i + 1) {
                let (sa, sb) = (single.estimate(a), single.estimate(b));
                if sa == sb {
                    continue; // no ordering to preserve
                }
                compared += 1;
                if (sa > sb) != (sharded.estimate(a) > sharded.estimate(b)) {
                    inverted += 1;
                }
            }
        }
        assert!(compared > 1000, "the load must produce a rankable spread");
        // A per-shard aging clock inverts ~44% of these pairs.
        assert!(
            inverted * 100 <= compared,
            "{inverted} of {compared} ranked pairs invert against the single sketch"
        );
    }

    /// A key's counters live in exactly one shard, so incrementing a hot key
    /// must not raise keys that route elsewhere.
    #[test]
    fn shards_hold_disjoint_keys() {
        let s = ShardedCountMinSketch::new(EQUIV_COLS);
        for _ in 0..40 {
            s.increment(&key(1));
        }
        let cold_and_zero = (2u32..64).filter(|i| s.estimate(&key(*i)) == 0).count();
        assert!(
            cold_and_zero >= 60,
            "increments of one key must not raise unrelated keys, {cold_and_zero} of 62 stayed 0"
        );
    }

    /// A sighting must not wait on a shard another observer is inside.
    ///
    /// This is the whole point of the split: `observe` runs at the end of every
    /// completed serve and `estimate` on every fill-path admission decision, and
    /// a single sketch-wide mutex would serialize those node-wide.
    /// Holding one shard's lock and touching a key that routes elsewhere must
    /// still land; a routing regression that collapsed every key onto one shard
    /// would silently restore the contention and pass every other test here.
    #[test]
    fn increment_does_not_wait_on_another_shard() -> anyhow::Result<()> {
        use std::sync::mpsc;
        use std::time::Duration;

        let sketch = std::sync::Arc::new(ShardedCountMinSketch::new(EQUIV_COLS));
        let pinned = key(1);
        let Some(shard) = sketch.shard_of(&pinned) else {
            anyhow::bail!("a non-empty sketch must route every key to a shard");
        };
        let held = shard.lock().unwrap_or_else(PoisonError::into_inner);

        // Find a key that routes elsewhere by comparing shard pointers — the
        // routing function is the thing under test, so ask it directly.
        let elsewhere = (2u32..1024)
            .map(key)
            .find(|k| sketch.shard_of(k).is_some_and(|s| !std::ptr::eq(s, shard)));
        let Some(elsewhere) = elsewhere else {
            anyhow::bail!("no candidate key routed outside the pinned shard");
        };

        // Touch it from another thread so a wait shows up as a timeout rather
        // than a hung test.
        let (tx, rx) = mpsc::channel::<()>();
        let toucher = {
            let sketch = std::sync::Arc::clone(&sketch);
            std::thread::spawn(move || {
                sketch.increment(&elsewhere);
                let _ = sketch.estimate(&elsewhere);
                let _ = tx.send(());
            })
        };
        let landed = rx.recv_timeout(Duration::from_secs(10)).is_ok();

        // Release and reap before asserting, so a failure reports rather than
        // leaking the thread.
        drop(held);
        let joined = toucher.join().is_ok();
        anyhow::ensure!(landed, "a sighting waited on a lock held for another shard");
        anyhow::ensure!(joined, "the touching thread panicked");
        Ok(())
    }

    /// The `cols <= SHARDS` shape is degenerate, not merely small: every shard
    /// is one column wide, so all `ROWS` rows index the same counter and two
    /// keys sharing a shard become indistinguishable. Pinned so the constructor
    /// doc's warning is not silently invalidated — `TinyLfuEstimator::new`
    /// floors `cols` at 64 precisely to stay out of this regime.
    #[test]
    fn a_sketch_narrower_than_its_shard_count_degenerates() -> anyhow::Result<()> {
        let s = ShardedCountMinSketch::new(SHARDS);
        let hot = key(1);
        let Some(shard) = s.shard_of(&hot) else {
            anyhow::bail!("a non-empty sketch must route every key to a shard");
        };
        let Some(colliding) = (2u32..4096)
            .map(key)
            .find(|k| s.shard_of(k).is_some_and(|o| std::ptr::eq(o, shard)))
        else {
            anyhow::bail!("some key must share a shard with the hot key");
        };
        for _ in 0..5 {
            s.increment(&hot);
        }
        assert_eq!(
            s.estimate(&colliding),
            s.estimate(&hot),
            "at one column per shard a shard-mate is indistinguishable from the hot key"
        );
        Ok(())
    }
}
