//! Tracks hashes whose local copy was obtained by speculative prefetch (#820).
//!
//! The serve path consults this set to attribute served bytes to the
//! demand-quality numerator ([`super::decision::PrefetchPolicy::record_served`]):
//! a blob is only "prefetch content" if a prefetch acquisition put it in the
//! cache. The set is **bounded** (capacity-capped, oldest-evicted) and **TTL'd**
//! on the same horizon as the demand-quality window — once a blob ages out of
//! that window there is no value in still attributing its serves, so the entry
//! self-expires and cannot leak memory.

use std::collections::HashMap;
use std::sync::Mutex;

/// Default capacity. A 32-byte key + an 8-byte timestamp per entry keeps the
/// worst case to tens of KB; entries also expire by TTL well before this caps.
pub const DEFAULT_CAPACITY: usize = 4096;

/// Bounded, TTL'd set of prefetch-acquired content hashes.
#[derive(Debug)]
pub struct PrefetchAcquiredSet {
    /// `hash -> insert timestamp (unix seconds)`.
    inner: Mutex<HashMap<[u8; 32], u64>>,
    ttl_secs: u64,
    capacity: usize,
}

impl PrefetchAcquiredSet {
    /// Construct with `ttl_secs` (typically `demand_quality_window_secs`) and the
    /// default capacity.
    #[must_use]
    pub fn new(ttl_secs: u64) -> Self {
        Self::with_capacity(ttl_secs, DEFAULT_CAPACITY)
    }

    /// Construct with an explicit capacity (used by tests).
    #[must_use]
    pub fn with_capacity(ttl_secs: u64, capacity: usize) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            ttl_secs,
            // A zero capacity would make every insert a no-op and silently drop
            // all tagging; clamp to at least 1 so the set is always usable.
            capacity: capacity.max(1),
        }
    }

    /// Tag `hash` as prefetch-acquired at `now`. Opportunistically purges expired
    /// entries and, if still at capacity, evicts the oldest. A poisoned lock is a
    /// no-op (the worst case is an un-tagged blob, i.e. an under-counted served
    /// ratio — the safe direction).
    pub fn insert(&self, hash: [u8; 32], now: u64) {
        let Ok(mut map) = self.inner.lock() else {
            tracing::error!("prefetch acquired-set insert: mutex poisoned");
            return;
        };
        map.retain(|_, ts| now.saturating_sub(*ts) < self.ttl_secs);
        if map.len() >= self.capacity
            && !map.contains_key(&hash)
            && let Some(oldest) = map.iter().min_by_key(|(_, ts)| **ts).map(|(k, _)| *k)
        {
            map.remove(&oldest);
        }
        map.insert(hash, now);
    }

    /// Whether `hash` was prefetch-acquired and has not yet expired at `now`.
    /// Lazily purges an expired entry. A poisoned lock returns `false` so a
    /// fault never over-credits the demand-quality numerator.
    #[must_use]
    pub fn contains(&self, hash: &[u8; 32], now: u64) -> bool {
        let Ok(mut map) = self.inner.lock() else {
            return false;
        };
        match map.get(hash) {
            Some(ts) if now.saturating_sub(*ts) < self.ttl_secs => true,
            Some(_) => {
                map.remove(hash);
                false
            }
            None => false,
        }
    }

    /// Current entry count (test/observability helper).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().map_or(0, |m| m.len())
    }

    /// Whether the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::PrefetchAcquiredSet;

    #[test]
    fn contains_after_insert() {
        let set = PrefetchAcquiredSet::new(3600);
        let h = [1u8; 32];
        assert!(!set.contains(&h, 0));
        set.insert(h, 100);
        assert!(set.contains(&h, 100));
        assert!(set.contains(&h, 100 + 3599));
    }

    #[test]
    fn expires_after_ttl() {
        let set = PrefetchAcquiredSet::new(3600);
        let h = [2u8; 32];
        set.insert(h, 100);
        // age == ttl => expired (matches the `>=` prune convention).
        assert!(!set.contains(&h, 100 + 3600));
        // The expired entry was lazily purged.
        assert!(set.is_empty());
    }

    #[test]
    fn capacity_evicts_oldest() {
        let set = PrefetchAcquiredSet::with_capacity(3600, 2);
        set.insert([1u8; 32], 10);
        set.insert([2u8; 32], 20);
        // Third insert is over capacity => oldest ([1], ts=10) evicted.
        set.insert([3u8; 32], 30);
        assert!(!set.contains(&[1u8; 32], 30));
        assert!(set.contains(&[2u8; 32], 30));
        assert!(set.contains(&[3u8; 32], 30));
    }

    #[test]
    fn reinsert_does_not_evict_when_present() {
        let set = PrefetchAcquiredSet::with_capacity(3600, 1);
        set.insert([1u8; 32], 10);
        // Re-inserting the SAME hash must not evict it to make room for itself.
        set.insert([1u8; 32], 20);
        assert!(set.contains(&[1u8; 32], 20));
    }

    #[test]
    fn zero_capacity_clamped_to_usable() {
        let set = PrefetchAcquiredSet::with_capacity(3600, 0);
        set.insert([1u8; 32], 10);
        assert!(set.contains(&[1u8; 32], 10));
    }
}
