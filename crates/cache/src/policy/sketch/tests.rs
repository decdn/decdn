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
