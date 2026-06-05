//! Responder-side `FIND_VALUE` popularity oracle (ADR 022 §"Prefetch Demand
//! Signal: DHT `FIND_VALUE` Query Frequency"). A node positioned close to hash
//! H in the Kademlia keyspace receives `FIND_VALUE` queries for H from the
//! whole network regardless of whether it holds H; the count within a rolling
//! window is the non-suppressible demand signal that MAY trip a speculative
//! prefetch.
//!
//! Pure: reads no clock, performs no I/O. The caller supplies a monotonic
//! `now` (seconds) so tests are deterministic.

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
    pub fn observe(&mut self, hash: &HashKey, now: u64) -> bool {
        if !self.windows.contains_key(hash) && self.windows.len() >= self.max_hashes {
            self.evict_least_recent();
        }
        let window = self.windows.entry(*hash).or_default();
        Self::prune(window, self.window_secs, now);
        window.push_back(now);
        // Exactly-equal, not `>=`: one timestamp is pushed per call, so the
        // count rises by at most one — `== threshold` is the upward crossing.
        // A count already past the threshold returns `false`.
        u32::try_from(window.len()).unwrap_or(u32::MAX) == self.threshold
    }

    /// Current in-window query count for `hash` at `now` (seconds), pruning
    /// stale timestamps as a side effect. `0` for an untracked hash.
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
}
