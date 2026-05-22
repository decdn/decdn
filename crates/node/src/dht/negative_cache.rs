//! Requester-side negative probe cache (ADR 001 §Probe cache).
//!
//! Suppresses repeated `cdn/probe/v1` requests to a `(NodeId, hash)`
//! pair that already returned `has_blob: false` within the TTL window.
//! Per ADR 001 §97–99 the cache:
//!
//! - is keyed `(NodeId, hash)`;
//! - holds at most 1024 entries with LRU eviction;
//! - retains entries for 5 minutes (TTL anchored at insertion);
//! - does NOT retain the probe response signature — purely a request-
//!   suppression structure, not slashing evidence.
//!
//! The DHT iterative lookup ([`super::lookup`]) consumes
//! [`NegativeProbeCache::contains_active`] as the third ADR 022
//! §Lookup-integrity filter on `FindValueResponse.providers`. The
//! producer side ([`NegativeProbeCache::record_failure`]) belongs to
//! a future outbound `cdn/probe/v1` client that does not exist on
//! `main` yet; the method ships now so the lookup-side tests can
//! populate the cache, and so the eventual probe client has a
//! stable surface to call.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tracing::warn;

use crate::dht::routing::NodeId;

/// 32-byte content hash. Aliased here (matching the alias in
/// [`crate::dht::records`]) so the lookup-side filter callsites read
/// in domain language rather than as a raw `[u8; 32]`.
pub type Hash = [u8; 32];

/// ADR 001 §99: negative cache TTL is 5 minutes — longer than the
/// positive probe cache (15s) because false-STORE results are less
/// time-sensitive, and shorter than the DHT record TTL (1h) so a
/// publisher that genuinely acquires the blob during the negative
/// window can re-establish reachability after one cache lifetime.
const DEFAULT_TTL: Duration = Duration::from_mins(5);

/// ADR 001 §97: max 1024 entries.
const DEFAULT_CAPACITY: usize = 1024;

type Key = (NodeId, Hash);

#[derive(Debug)]
struct Inner {
    /// Key → expiry deadline. The map is the authoritative
    /// membership check; the queue below only tracks order.
    map: HashMap<Key, Instant>,
    /// Front = most-recently-used, back = least-recently-used.
    /// Stays in lockstep with `map`: every key in `map` is present
    /// exactly once in `order`. Bookkeeping is O(n) at cap=1024
    /// which is fine for the consultation frequency (once per probe
    /// candidate, not once per wire packet).
    order: VecDeque<Key>,
    cap: usize,
    ttl: Duration,
}

/// Bounded LRU cache of `(NodeId, hash)` pairs returned
/// `has_blob: false` within the TTL window.
#[derive(Debug)]
pub struct NegativeProbeCache {
    inner: Mutex<Inner>,
}

impl Default for NegativeProbeCache {
    fn default() -> Self {
        Self::new()
    }
}

impl NegativeProbeCache {
    /// Build a cache with the ADR 001 §97 defaults (1024 entries, 5-
    /// minute TTL).
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity_and_ttl(DEFAULT_CAPACITY, DEFAULT_TTL)
    }

    /// Build with a custom capacity (TTL stays at the spec default).
    /// `cap == 0` is clamped to 1.
    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self::with_capacity_and_ttl(cap, DEFAULT_TTL)
    }

    /// Build with a custom capacity and TTL. Test-only convenience
    /// to allow sub-second TTL expiry checks; production callers use
    /// [`Self::new`].
    #[must_use]
    pub fn with_capacity_and_ttl(cap: usize, ttl: Duration) -> Self {
        let cap = cap.max(1);
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::with_capacity(cap),
                order: VecDeque::with_capacity(cap),
                cap,
                ttl,
            }),
        }
    }

    /// Filter 3 (ADR 022 §184): returns `true` iff
    /// `(node_id, hash)` is in the cache and its TTL hasn't elapsed.
    /// A live hit bumps the entry to the front of the LRU ordering;
    /// an expired entry is evicted before returning `false`.
    #[must_use]
    pub fn contains_active(&self, node_id: &NodeId, hash: &Hash) -> bool {
        let key = (*node_id, *hash);
        let now = Instant::now();
        let mut guard = self.lock();
        let Some(expiry) = guard.map.get(&key).copied() else {
            return false;
        };
        if expiry <= now {
            guard.map.remove(&key);
            guard.order.retain(|k| k != &key);
            return false;
        }
        Self::touch(&mut guard, &key);
        true
    }

    /// Insert / refresh `(node_id, hash)` with `now + TTL` expiry.
    /// Evicts the least-recently-used entry on cap overflow. Called
    /// by the future outbound probe client after a `has_blob: false`
    /// response; the lookup module never calls this directly.
    pub fn record_failure(&self, node_id: NodeId, hash: Hash) {
        let key = (node_id, hash);
        let mut guard = self.lock();
        let expiry = Instant::now() + guard.ttl;
        let was_present = guard.map.insert(key, expiry).is_some();
        if was_present {
            guard.order.retain(|k| k != &key);
        } else if guard.map.len() > guard.cap
            && let Some(victim) = guard.order.pop_back()
        {
            guard.map.remove(&victim);
        }
        guard.order.push_front(key);
    }

    /// Current entry count. Includes expired entries that haven't
    /// been swept yet — call [`Self::contains_active`] first if a
    /// precise live count is needed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().map.len()
    }

    /// Whether the cache holds zero entries (including stale).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().map.is_empty()
    }

    fn touch(guard: &mut Inner, key: &Key) {
        guard.order.retain(|k| k != key);
        guard.order.push_front(*key);
    }

    /// Poison-tolerant lock acquisition. Matches the in-repo pattern
    /// already established for the chain-backed staker set's
    /// `RwLock` and `decdn-cache`'s GC sweeps: a poisoned mutex
    /// means *something panicked while holding the lock*, but our
    /// writers never panic, so the inner state is structurally
    /// sound and we recover rather than propagating.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                warn!("NegativeProbeCache mutex poisoned; recovering inner state");
                poisoned.into_inner()
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use std::thread;

    fn nid(byte: u8) -> NodeId {
        [byte; 32]
    }
    fn h(byte: u8) -> Hash {
        [byte; 32]
    }

    #[test]
    fn absent_key_returns_false() {
        let c = NegativeProbeCache::new();
        assert!(!c.contains_active(&nid(1), &h(1)));
        assert!(c.is_empty());
    }

    #[test]
    fn record_then_contains_returns_true() {
        let c = NegativeProbeCache::new();
        c.record_failure(nid(1), h(1));
        assert!(c.contains_active(&nid(1), &h(1)));
        assert!(!c.contains_active(&nid(2), &h(1)));
        assert!(!c.contains_active(&nid(1), &h(2)));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn expired_entry_returns_false_and_is_evicted() {
        let c = NegativeProbeCache::with_capacity_and_ttl(8, Duration::from_millis(40));
        c.record_failure(nid(1), h(1));
        assert!(c.contains_active(&nid(1), &h(1)));
        thread::sleep(Duration::from_millis(60));
        assert!(!c.contains_active(&nid(1), &h(1)));
        assert!(
            c.is_empty(),
            "expired entry should have been evicted on read"
        );
    }

    #[test]
    fn lru_eviction_at_cap_drops_oldest() {
        let c = NegativeProbeCache::with_capacity(2);
        c.record_failure(nid(1), h(1));
        c.record_failure(nid(2), h(2));
        c.record_failure(nid(3), h(3));
        // nid(1) was least-recently-used → evicted.
        assert!(!c.contains_active(&nid(1), &h(1)));
        assert!(c.contains_active(&nid(2), &h(2)));
        assert!(c.contains_active(&nid(3), &h(3)));
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn read_hit_bumps_lru_so_oldest_eviction_changes() {
        let c = NegativeProbeCache::with_capacity(2);
        c.record_failure(nid(1), h(1));
        c.record_failure(nid(2), h(2));
        // Bump nid(1) by reading it → nid(2) becomes LRU.
        assert!(c.contains_active(&nid(1), &h(1)));
        c.record_failure(nid(3), h(3));
        assert!(c.contains_active(&nid(1), &h(1)));
        assert!(!c.contains_active(&nid(2), &h(2)));
        assert!(c.contains_active(&nid(3), &h(3)));
    }

    #[test]
    fn re_recording_same_key_refreshes_ttl_does_not_grow_len() {
        let c = NegativeProbeCache::with_capacity_and_ttl(8, Duration::from_millis(80));
        c.record_failure(nid(1), h(1));
        thread::sleep(Duration::from_millis(40));
        c.record_failure(nid(1), h(1));
        thread::sleep(Duration::from_millis(60));
        // First insert would have expired by now (40+60=100ms > 80ms);
        // the refresh at 40ms reset the window so we're at 60ms post-
        // refresh, still inside the 80ms TTL.
        assert!(c.contains_active(&nid(1), &h(1)));
        assert_eq!(c.len(), 1);
    }
}
