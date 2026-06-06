//! Responder-side `FIND_VALUE` popularity oracle (ADR 022 §"Prefetch Demand
//! Signal: DHT `FIND_VALUE` Query Frequency"). A node positioned close to hash
//! H in the Kademlia keyspace receives `FIND_VALUE` queries for H from the
//! whole network regardless of whether it holds H; the count within a rolling
//! window is the non-suppressible demand signal that MAY trip a speculative
//! prefetch.
//!
//! Pure: reads no clock, performs no I/O. The caller injects `now` (seconds)
//! so tests are deterministic. In production that clock is the node's
//! wall-clock (`handlers::dht::now_us() / 1_000_000`), consistent with the
//! rest of the DHT subsystem, whose record TTLs are wall-clock-anchored (ADR
//! 022 §STORE Flow). It is therefore NOT guaranteed monotonic: an NTP step
//! backward (or a mis-set clock reading `0`) makes `now` regress, which stalls
//! window pruning (`now.saturating_sub(ts)` saturates to `0`). To keep a single
//! hot hash's window from growing without bound while the clock is bad, each
//! per-hash window is length-capped (see [`PopularityTracker::observe`]); the
//! `MAX_TRACKED_HASHES` cap bounds only the number of *distinct* hashes.

use std::collections::{HashMap, VecDeque};

/// 32-byte BLAKE3 content hash used as the tracker key.
pub type HashKey = [u8; 32];

/// Hard cap on the number of distinct hashes tracked at once (ADR 022 names
/// 10,000 for the per-hash demand map). Bounds memory; the least-recently-seen
/// hash is evicted when a brand-new hash arrives at the cap.
pub const MAX_TRACKED_HASHES: usize = 10_000;

/// Sliding-window per-hash `FIND_VALUE` query counter with a bounded number of
/// tracked hashes.
#[derive(Debug)]
pub struct PopularityTracker {
    /// `FIND_VALUE` arrival timestamps (seconds) per hash, oldest-first.
    windows: HashMap<HashKey, VecDeque<u64>>,
    /// Rolling-window length in seconds (`prefetch.threshold_window_secs`).
    window_secs: u64,
    /// Trigger threshold (`prefetch.find_value_threshold`).
    threshold: u32,
    /// Hard cap on the number of tracked hashes (`>= 1`).
    max_hashes: usize,
}

impl PopularityTracker {
    /// Construct a tracker. `max_hashes` is clamped to at least 1.
    #[must_use]
    pub fn new(window_secs: u64, threshold: u32, max_hashes: usize) -> Self {
        Self {
            windows: HashMap::new(),
            window_secs,
            threshold,
            max_hashes: max_hashes.max(1),
        }
    }

    /// Record a `FIND_VALUE` arrival for `hash` at `now` (seconds). Returns
    /// `true` only on the observation that makes the in-window count first
    /// *reach* the threshold — an **edge** trigger, not a level one. Once the
    /// window is at or above the threshold, subsequent in-window queries return
    /// `false`; the trigger re-arms only after entries age out and the count
    /// crosses the threshold again. This keeps "demand crossed the threshold"
    /// a single event per burst rather than firing on every query above it.
    ///
    /// The window retains at most `threshold` timestamps — all the edge trigger
    /// needs is whether the count has reached the threshold. Capping there also
    /// bounds memory if the wall-clock regresses and pruning stalls (see the
    /// module docstring), so a stuck/backward clock can't grow the window
    /// without limit.
    pub fn observe(&mut self, hash: &HashKey, now: u64) -> bool {
        if !self.windows.contains_key(hash) && self.windows.len() >= self.max_hashes {
            self.evict_least_recent();
        }
        let threshold = self.threshold;
        let window = self.windows.entry(*hash).or_default();
        Self::prune(window, self.window_secs, now);
        let before = u32::try_from(window.len()).unwrap_or(u32::MAX);
        window.push_back(now);
        // Retain only the most-recent `threshold` timestamps (drop oldest).
        // This is the bounded-length guard against clock regression and makes
        // the count saturate at `threshold` — beyond it the trigger boolean is
        // already determined.
        while u32::try_from(window.len()).unwrap_or(u32::MAX) > threshold {
            window.pop_front();
        }
        let after = u32::try_from(window.len()).unwrap_or(u32::MAX);
        // Upward crossing: fire only on the transition from below the threshold
        // to at/above it. Equivalent to `== threshold` while exactly one arrival
        // is pushed per call, but robust to a future change that batches several
        // arrivals into one call (which the old `== threshold` test would skip).
        before < threshold && after >= threshold
    }

    /// Current in-window query count for `hash` at `now` (seconds), pruning
    /// stale timestamps as a side effect. `0` for an untracked hash. Saturates
    /// at the configured threshold, since [`observe`](Self::observe) retains
    /// only the most-recent `threshold` timestamps.
    pub fn count(&mut self, hash: &HashKey, now: u64) -> u32 {
        let window_secs = self.window_secs;
        match self.windows.get_mut(hash) {
            Some(window) => {
                Self::prune(window, window_secs, now);
                u32::try_from(window.len()).unwrap_or(u32::MAX)
            }
            None => 0,
        }
    }

    /// Drop timestamps strictly older than the window (`age >= window_secs`).
    fn prune(window: &mut VecDeque<u64>, window_secs: u64, now: u64) {
        while let Some(front) = window.front() {
            if now.saturating_sub(*front) >= window_secs {
                window.pop_front();
            } else {
                break;
            }
        }
    }

    /// Evict the hash whose most-recent observation is the oldest. O(n) but
    /// only runs when a brand-new hash arrives at the cap.
    fn evict_least_recent(&mut self) {
        let victim = self
            .windows
            .iter()
            .map(|(k, v)| (*k, v.back().copied().unwrap_or(0)))
            .min_by_key(|(_, newest)| *newest)
            .map(|(k, _)| k);
        if let Some(k) = victim {
            self.windows.remove(&k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_TRACKED_HASHES, PopularityTracker};

    fn h(b: u8) -> [u8; 32] {
        [b; 32]
    }

    #[test]
    fn below_threshold_no_trigger() {
        // threshold 3, window 300s.
        let mut t = PopularityTracker::new(300, 3, MAX_TRACKED_HASHES);
        assert!(!t.observe(&h(1), 0));
        assert!(!t.observe(&h(1), 1));
        assert_eq!(t.count(&h(1), 1), 2);
    }

    #[test]
    fn crossing_threshold_triggers() {
        let mut t = PopularityTracker::new(300, 3, MAX_TRACKED_HASHES);
        assert!(!t.observe(&h(1), 0));
        assert!(!t.observe(&h(1), 10));
        assert!(t.observe(&h(1), 20)); // third within window
    }

    #[test]
    fn fires_once_per_threshold_cross() {
        // Edge trigger: only the observation that reaches the threshold fires;
        // further in-window queries above it do not re-fire.
        let mut t = PopularityTracker::new(300, 2, MAX_TRACKED_HASHES);
        assert!(!t.observe(&h(1), 0)); // len 1
        assert!(t.observe(&h(1), 1)); // len 2 == threshold -> fire
        assert!(!t.observe(&h(1), 2)); // len 3 -> no re-fire
        assert!(!t.observe(&h(1), 3)); // len 4 -> no re-fire
    }

    #[test]
    fn refires_after_window_drains_and_recrosses() {
        let mut t = PopularityTracker::new(100, 2, MAX_TRACKED_HASHES);
        assert!(!t.observe(&h(1), 0)); // len 1
        assert!(t.observe(&h(1), 1)); // len 2 -> fire
        // Both early entries age out (>= 100s old); this one rebuilds to len 1.
        assert!(!t.observe(&h(1), 200));
        // Re-crosses the threshold -> fires again.
        assert!(t.observe(&h(1), 201));
    }

    #[test]
    fn stale_timestamps_age_out() {
        let mut t = PopularityTracker::new(100, 3, MAX_TRACKED_HASHES);
        assert!(!t.observe(&h(1), 0));
        assert!(!t.observe(&h(1), 50));
        // First two are now > 100s old; the third does not reach threshold 3.
        assert!(!t.observe(&h(1), 201));
        assert_eq!(t.count(&h(1), 201), 1);
    }

    #[test]
    fn separate_hashes_are_independent() {
        let mut t = PopularityTracker::new(300, 2, MAX_TRACKED_HASHES);
        assert!(!t.observe(&h(1), 0));
        assert!(!t.observe(&h(2), 0));
        assert!(t.observe(&h(1), 1));
    }

    #[test]
    fn evicts_least_recent_at_cap() {
        let mut t = PopularityTracker::new(1000, 2, 2);
        t.observe(&h(1), 0); // h1 newest = 0
        t.observe(&h(2), 5); // h2 newest = 5
        t.observe(&h(3), 10); // at cap (2) + new hash => evict h1 (oldest newest=0)
        assert_eq!(t.count(&h(1), 10), 0); // evicted
        assert_eq!(t.count(&h(2), 10), 1);
        assert_eq!(t.count(&h(3), 10), 1);
    }

    #[test]
    fn evict_tie_break_removes_exactly_one_at_cap() {
        // h1 and h2 share the same newest timestamp (5). `min_by_key` breaks the
        // tie by HashMap iteration order, so which of the two is evicted is
        // unspecified — but the invariant holds: the brand-new hash is admitted
        // and exactly one tied hash is dropped (total stays at the cap of 2).
        let mut t = PopularityTracker::new(1000, 2, 2);
        t.observe(&h(1), 5);
        t.observe(&h(2), 5); // tie on newest = 5
        t.observe(&h(3), 10); // at cap + new hash => evict one of h1/h2
        assert_eq!(t.count(&h(3), 10), 1, "new hash admitted");
        let h1_gone = t.count(&h(1), 10) == 0;
        let h2_gone = t.count(&h(2), 10) == 0;
        assert!(h1_gone ^ h2_gone, "exactly one tied hash evicted");
    }

    #[test]
    fn window_length_is_capped_at_threshold() {
        // A backward clock stalls pruning (now.saturating_sub(ts) == 0), but the
        // per-hash window must not grow past `threshold`. Hammer one hash at a
        // frozen `now` well past the threshold and confirm the count saturates.
        let mut t = PopularityTracker::new(300, 3, MAX_TRACKED_HASHES);
        for _ in 0..1000 {
            t.observe(&h(1), 0);
        }
        assert_eq!(t.count(&h(1), 0), 3, "window capped at threshold");
    }
}
