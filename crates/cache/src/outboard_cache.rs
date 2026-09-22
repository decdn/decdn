//! A byte-bounded, least-recently-used memory cache of origin `{H}.obao4`
//! outboards (ADR 037 § Range-scoped origin pull).
//!
//! An outboard is content-derived and `blob / 256` bytes. Every draw of a fill,
//! and every serviceability probe of the hash, reads it from this cache, so the
//! origin serves it once per hash while the entry stays cached.
//!
//! The cache holds only outboards that passed the exact-length gate in
//! [`crate::CacheEngine::origin_fetch_outboard_bytes`], each tagged with the
//! origin that served it. A verify fault evicts the copy the failed draw used
//! ([`OutboardCache::evict_if_same`]), so a bad copy is read again, not reused.
//!
//! [`OutboardCache`] is a cheap, cloneable handle. Every method takes the lock
//! for one synchronous map operation and returns, so no caller can hold it
//! across an await.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use iroh_blobs::Hash;

/// Total outboard bytes the cache holds. An outboard is about 0.4% of its blob,
/// so this holds the outboards of about five 13 GB blobs at once. An outboard
/// larger than this (a blob over 64 GiB) is never cached, and each draw of that
/// blob reads it from the origin.
pub(crate) const OUTBOARD_CACHE_BYTES: u64 = 256 * 1024 * 1024;

/// Entries the cache holds, whatever their bytes. Small blobs have tiny
/// outboards, so the byte budget alone would let the entry count, and the
/// `O(n)` eviction scan, grow very large.
pub(crate) const OUTBOARD_CACHE_ENTRIES: usize = 4096;

/// A cached outboard and the index, in the engine's origin chain, of the origin
/// that served it.
#[derive(Debug, Clone)]
pub(crate) struct CachedOutboard {
    /// The outboard bytes, of the exact length for the blob size they were
    /// gated against.
    pub(crate) bytes: Bytes,
    /// Position of the serving origin in the engine's origin chain.
    pub(crate) origin_ix: usize,
}

/// Cloneable handle to the shared cache state.
#[derive(Debug, Clone)]
pub(crate) struct OutboardCache {
    state: Arc<Mutex<State>>,
}

/// The cache state behind the handle's mutex.
///
/// Eviction scans every entry for the least recent one. That is `O(n)` under
/// the lock, and `n` is at most `max_entries`.
#[derive(Debug)]
struct State {
    /// Each entry is the outboard and the tick of its last use.
    entries: HashMap<Hash, (CachedOutboard, u64)>,
    /// Sum of the entries' lengths.
    bytes: u64,
    /// Largest `bytes` the cache holds.
    budget: u64,
    /// Largest entry count the cache holds.
    max_entries: usize,
    /// Use counter. A larger tick is a more recent use.
    tick: u64,
}

/// A `Bytes` length as `u64`. A `usize` always fits on supported targets.
fn len64(bytes: &Bytes) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}

impl State {
    fn remove(&mut self, hash: Hash) {
        if let Some((entry, _)) = self.entries.remove(&hash) {
            self.bytes = self.bytes.saturating_sub(len64(&entry.bytes));
        }
    }
}

impl OutboardCache {
    /// An empty cache that holds at most `budget` outboard bytes in at most
    /// `max_entries` entries.
    pub(crate) fn new(budget: u64, max_entries: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                entries: HashMap::new(),
                bytes: 0,
                budget,
                max_entries,
                tick: 0,
            })),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The cached outboard for `hash` if its length is `expected_len`, marked
    /// as the most recent use. An entry of another length was gated against a
    /// different probed blob size; it is dropped and reads as a miss.
    pub(crate) fn get(&self, hash: Hash, expected_len: u64) -> Option<CachedOutboard> {
        let mut state = self.lock();
        let len = len64(&state.entries.get(&hash)?.0.bytes);
        if len != expected_len {
            state.remove(hash);
            return None;
        }
        state.tick = state.tick.saturating_add(1);
        let tick = state.tick;
        state.entries.get_mut(&hash).map(|(entry, used)| {
            *used = tick;
            entry.clone()
        })
    }

    /// Cache `bytes` for `hash` as served by origin `origin_ix`, evicting the
    /// least recently used entries until it fits in both the byte budget and
    /// the entry count. Returns `false`, caching nothing, for an outboard
    /// larger than the whole budget. An empty outboard (a blob of one chunk
    /// group has no interior nodes) costs no origin read to repeat, so it is
    /// not cached and returns `true`.
    pub(crate) fn insert(&self, hash: Hash, bytes: Bytes, origin_ix: usize) -> bool {
        let len = len64(&bytes);
        if len == 0 {
            return true;
        }
        let mut state = self.lock();
        if len > state.budget || state.max_entries == 0 {
            return false;
        }
        state.remove(hash);
        while state.bytes.saturating_add(len) > state.budget
            || state.entries.len() >= state.max_entries
        {
            let Some(oldest) = state
                .entries
                .iter()
                .min_by_key(|(_, (_, used))| *used)
                .map(|(h, _)| *h)
            else {
                break;
            };
            state.remove(oldest);
        }
        state.tick = state.tick.saturating_add(1);
        let tick = state.tick;
        state.bytes = state.bytes.saturating_add(len);
        state
            .entries
            .insert(hash, (CachedOutboard { bytes, origin_ix }, tick));
        true
    }

    /// Drop the cached outboard for `hash` only if it is `used`, the copy a
    /// failed draw verified against. A newer copy that another draw read after
    /// the failure stays cached.
    pub(crate) fn evict_if_same(&self, hash: Hash, used: &Bytes) {
        let mut state = self.lock();
        let same = state.entries.get(&hash).is_some_and(|(entry, _)| {
            entry.bytes.as_ptr() == used.as_ptr() && entry.bytes.len() == used.len()
        });
        if same {
            state.remove(hash);
        }
    }

    /// Sum of the cached outboards' lengths.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> u64 {
        self.lock().bytes
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;

    fn ob(len: usize) -> Bytes {
        Bytes::from(vec![7u8; len])
    }

    #[test]
    fn a_cached_outboard_is_returned_with_its_origin() {
        let cache = OutboardCache::new(100, 16);
        let h = Hash::new(b"a");
        assert!(cache.get(h, 10).is_none());
        assert!(cache.insert(h, ob(10), 2));
        let hit = cache.get(h, 10).unwrap();
        assert_eq!((hit.bytes.len(), hit.origin_ix), (10, 2));
        assert_eq!(cache.bytes(), 10);
    }

    #[test]
    fn a_hit_of_another_length_is_a_miss_and_drops_the_entry() {
        let cache = OutboardCache::new(100, 16);
        let h = Hash::new(b"a");
        cache.insert(h, ob(10), 0);
        assert!(cache.get(h, 11).is_none());
        assert_eq!(cache.bytes(), 0, "the mismatched entry is dropped");
        assert!(cache.get(h, 10).is_none());
    }

    #[test]
    fn an_insert_past_the_budget_evicts_the_least_recently_used() {
        let cache = OutboardCache::new(100, 16);
        let (a, b, c) = (Hash::new(b"a"), Hash::new(b"b"), Hash::new(b"c"));
        cache.insert(a, ob(40), 0);
        cache.insert(b, ob(40), 0);
        // Touch `a`, so `b` is the least recently used.
        assert!(cache.get(a, 40).is_some());
        cache.insert(c, ob(40), 0);
        assert!(cache.get(a, 40).is_some());
        assert!(
            cache.get(b, 40).is_none(),
            "the least recently used entry goes"
        );
        assert!(cache.get(c, 40).is_some());
        assert_eq!(cache.bytes(), 80);
    }

    #[test]
    fn an_insert_evicts_as_many_entries_as_it_needs() {
        let cache = OutboardCache::new(100, 16);
        let hashes: Vec<Hash> = (0u8..4).map(|i| Hash::new([i])).collect();
        for h in &hashes {
            cache.insert(*h, ob(25), 0);
        }
        let big = Hash::new(b"big");
        assert!(
            cache.insert(big, ob(100), 0),
            "an entry equal to the budget fits"
        );
        assert!(hashes.iter().all(|h| cache.get(*h, 25).is_none()));
        assert_eq!(cache.bytes(), 100);
    }

    #[test]
    fn an_empty_outboard_is_not_cached() {
        let cache = OutboardCache::new(100, 16);
        let h = Hash::new(b"a");
        assert!(cache.insert(h, Bytes::new(), 0));
        assert!(cache.get(h, 0).is_none());
    }

    #[test]
    fn the_entry_count_is_bounded() {
        let cache = OutboardCache::new(1_000, 2);
        let (a, b, c) = (Hash::new(b"a"), Hash::new(b"b"), Hash::new(b"c"));
        cache.insert(a, ob(1), 0);
        cache.insert(b, ob(1), 0);
        cache.insert(c, ob(1), 0);
        assert!(cache.get(a, 1).is_none(), "the oldest entry goes");
        assert!(cache.get(b, 1).is_some() && cache.get(c, 1).is_some());
        assert_eq!(cache.bytes(), 2);
    }

    #[test]
    fn an_outboard_larger_than_the_budget_is_not_cached() {
        let cache = OutboardCache::new(100, 16);
        let (a, big) = (Hash::new(b"a"), Hash::new(b"big"));
        cache.insert(a, ob(40), 0);
        assert!(!cache.insert(big, ob(101), 0));
        assert!(cache.get(big, 101).is_none());
        assert!(
            cache.get(a, 40).is_some(),
            "a refused insert evicts nothing"
        );
    }

    #[test]
    fn a_reinsert_replaces_without_double_counting() {
        let cache = OutboardCache::new(100, 16);
        let h = Hash::new(b"a");
        cache.insert(h, ob(40), 0);
        cache.insert(h, ob(30), 1);
        assert_eq!(cache.bytes(), 30);
        assert_eq!(cache.get(h, 30).unwrap().origin_ix, 1);
    }

    #[test]
    fn evict_if_same_drops_only_the_copy_that_failed() {
        let cache = OutboardCache::new(100, 16);
        let h = Hash::new(b"a");
        let first = ob(40);
        cache.insert(h, first.clone(), 0);
        // Another draw re-read a fresh copy after the first one failed.
        let fresh = ob(40);
        cache.insert(h, fresh.clone(), 1);
        cache.evict_if_same(h, &first);
        assert!(cache.get(h, 40).is_some(), "the fresh copy stays");
        cache.evict_if_same(h, &fresh);
        assert!(cache.get(h, 40).is_none());
        assert_eq!(cache.bytes(), 0);
    }
}
