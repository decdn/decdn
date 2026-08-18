//! Lazy, bounded, TTL-split cache of `getOrigins(namespaceId)` results.
//!
//! `ChainOriginDirectory` resolves a namespace to its authorized origin
//! operators lazily, on first request, rather than by watching every
//! `getOrigins` event up front. Namespace creation is permissionless and
//! free, so an attacker can mint namespace IDs at will; if every request for
//! an unknown namespace forced a fresh `getOrigins` RPC, a burst of
//! attacker-chosen namespaces would turn into a proportional burst of chain
//! calls. This cache exists to break that link: it remembers a namespace's
//! result — including "this namespace has no origins" — for a bounded time,
//! so repeated lookups against the same (possibly bogus) namespace cost one
//! RPC, not one per request.
//!
//! A non-empty `getOrigins` result is a POSITIVE entry and lives for
//! `positive_ttl`. An empty result (no origins, including a namespace that
//! does not exist on chain) is a NEGATIVE entry and lives for the shorter
//! `negative_ttl` — the actual `DoS` bound: it caps how often an attacker can
//! force a real RPC per bogus namespace, while keeping genuinely-empty
//! namespaces from squatting on cache space or masking a legitimate
//! `getOrigins` call for very long once one is registered.
//!
//! The cache stores the raw operator ADDRESS set, never resolved `NodeId`s.
//! Resolving addresses to live `NodeId`s (via the capacity-bond registry's
//! reverse projection) and filtering by `StakerSet` liveness both happen at
//! READ time, in `resolve_active` — so a TTL-anchored cache entry never goes
//! stale on the parts of the answer that change without a new `getOrigins`
//! call: operator staking status and operator→node bindings.
//!
//! Modeled directly on `super::probe_cache`: an `IndexMap` doubles as the
//! store and the LRU order (index 0 = most-recently-used), the TTL is
//! anchored on `Instant` at insert and never refreshed on read, and the lock
//! is poison-tolerant.

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use indexmap::IndexMap;
use tracing::warn;

use crate::dht::routing::NodeId;
use crate::dht::staker_set::StakerSet;

/// One cached `getOrigins(namespaceId)` result.
struct Entry {
    /// Authorized operator addresses as returned by `getOrigins`. Empty for a
    /// negative entry (namespace has no origins / does not exist). Resolved
    /// to live `NodeId`s at read time, never stored resolved — liveness and
    /// the operator→`NodeId` binding both move without a re-fetch.
    operators: Vec<Address>,
    /// Absolute expiry, anchored at insert, never refreshed on read. Uses the
    /// positive TTL when `operators` is non-empty, the negative TTL when
    /// empty.
    expiry: Instant,
}

struct Inner {
    /// `namespaceId → entry`. `IndexMap` collapses the LRU ordering (index 0
    /// = most-recently-used) into the same store, mirroring
    /// [`super::probe_cache`].
    entries: IndexMap<U256, Entry>,
    /// Hard cap on live namespaces. Clamped to ≥ 1 in the constructor.
    cap: usize,
    /// TTL applied to a non-empty (positive) entry on insert.
    positive_ttl: Duration,
    /// TTL applied to an empty (negative) entry on insert. Shorter than
    /// `positive_ttl` — this is the `DoS` bound described in the module docs.
    negative_ttl: Duration,
}

/// Bounded LRU of `namespaceId → authorized operator set` with split TTLs.
/// Pure: no chain access. `ChainOriginDirectory` owns one and feeds it the
/// `getOrigins` result on a miss.
pub(crate) struct LazyOriginCache {
    inner: Mutex<Inner>,
}

impl LazyOriginCache {
    /// Build a cache with an explicit capacity and split TTLs. `cap` is
    /// clamped to ≥ 1.
    #[must_use]
    pub(crate) fn new(cap: usize, positive_ttl: Duration, negative_ttl: Duration) -> Self {
        let cap = cap.max(1);
        Self {
            inner: Mutex::new(Inner {
                entries: IndexMap::with_capacity(cap),
                cap,
                positive_ttl,
                negative_ttl,
            }),
        }
    }

    /// The cached operator set for `ns`, iff an entry exists and its TTL
    /// hasn't elapsed.
    ///
    /// `Some(vec![])` is a live NEGATIVE hit — a cached "no origins" — and is
    /// distinct from `None`, which means a miss or an expired entry (either
    /// way, the caller must issue a fresh `getOrigins` RPC). A live hit bumps
    /// the entry to the front of the LRU ordering without refreshing its
    /// expiry; an expired entry is evicted before returning `None`.
    #[must_use]
    pub(crate) fn get(&self, ns: &U256) -> Option<Vec<Address>> {
        let now = Instant::now();
        let mut guard = self.lock();
        let entry = guard.entries.shift_remove(ns)?;
        if entry.expiry <= now {
            return None;
        }
        let operators = entry.operators.clone();
        guard.entries.shift_insert(0, *ns, entry);
        Some(operators)
    }

    /// Insert / replace the entry for `ns`, moving it to the front of the LRU
    /// ordering. Picks the positive or negative TTL by `operators.is_empty()`
    /// and evicts the least-recently-used namespace on cap overflow.
    pub(crate) fn insert(&self, ns: U256, operators: Vec<Address>) {
        let mut guard = self.lock();
        let ttl = if operators.is_empty() {
            guard.negative_ttl
        } else {
            guard.positive_ttl
        };
        let expiry = Instant::now() + ttl;
        let entry = Entry { operators, expiry };
        guard.entries.shift_insert(0, ns, entry);
        if guard.entries.len() > guard.cap {
            guard.entries.pop();
        }
    }

    /// Current namespace count. Includes expired entries that haven't been
    /// swept yet — call [`Self::get`] first if a precise live count is
    /// needed.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.lock().entries.len()
    }

    /// Poison-tolerant lock acquisition, mirroring
    /// [`super::probe_cache::PositiveProbeCache::lock`].
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => {
                warn!("LazyOriginCache mutex poisoned; recovering inner state");
                poisoned.into_inner()
            }
        }
    }
}

/// Resolve an authorized operator address set to currently-active origin
/// `NodeId`s: map each operator through the capacity-bond reverse
/// projection, keep only operators the `StakerSet` reports active, sort +
/// dedup for a deterministic order. Mirrors the old `DirectoryCache::resolve`
/// split, but the operator→`NodeId` map is the shared registry projection,
/// not a per-directory `nodeIdOf` cache.
///
/// An operator with no reverse binding is dropped — there is no chain
/// fallback here; a missing binding means the registry hasn't observed that
/// operator's node registration (yet, or ever), and this function is pure /
/// chain-free by design.
pub(crate) fn resolve_active(
    operators: &[Address],
    operator_to_node: &RwLock<HashMap<Address, NodeId>>,
    staker_set: &dyn StakerSet,
) -> Vec<NodeId> {
    let mut nodes: Vec<NodeId> = crate::dht::chain_projection::with_read(
        operator_to_node,
        "chain operator reverse map",
        |rev| {
            operators
                .iter()
                .filter_map(|op| rev.get(op).copied())
                .filter(|nid| staker_set.is_active(nid))
                .collect()
        },
    );
    nodes.sort_unstable();
    nodes.dedup();
    nodes
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use std::sync::RwLock as StdRwLock;
    use std::thread;

    use super::*;

    fn nid(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 32])
    }
    fn addr(byte: u8) -> Address {
        Address::from([byte; 20])
    }
    fn ns(n: u64) -> U256 {
        U256::from(n)
    }

    #[derive(Debug)]
    struct StubStakers(StdRwLock<std::collections::HashSet<NodeId>>);

    impl StubStakers {
        fn new(active: &[NodeId]) -> Self {
            Self(StdRwLock::new(active.iter().copied().collect()))
        }
    }

    impl StakerSet for StubStakers {
        fn is_active(&self, node_id: &NodeId) -> bool {
            self.0.read().unwrap().contains(node_id)
        }
        fn active_nodes(&self) -> Vec<NodeId> {
            self.0.read().unwrap().iter().copied().collect()
        }
        fn len(&self) -> usize {
            self.0.read().unwrap().len()
        }
    }

    // -- cache mechanics --

    #[test]
    fn absent_namespace_returns_none() {
        let c = LazyOriginCache::new(8, Duration::from_secs(30), Duration::from_secs(5));
        assert!(c.get(&ns(1)).is_none());
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn positive_insert_then_get_returns_operators() {
        let c = LazyOriginCache::new(8, Duration::from_secs(30), Duration::from_secs(5));
        c.insert(ns(1), vec![addr(1), addr(2)]);
        assert_eq!(c.get(&ns(1)), Some(vec![addr(1), addr(2)]));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn negative_entry_is_a_live_hit_returning_empty() {
        let c = LazyOriginCache::new(8, Duration::from_secs(30), Duration::from_secs(5));
        c.insert(ns(1), vec![]);
        assert_eq!(
            c.get(&ns(1)),
            Some(vec![]),
            "a cached empty result is a hit, not a miss"
        );
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn positive_ttl_outlives_negative_ttl() {
        let c = LazyOriginCache::new(8, Duration::from_millis(500), Duration::from_millis(100));
        c.insert(ns(1), vec![addr(1)]);
        c.insert(ns(2), vec![]);
        thread::sleep(Duration::from_millis(200));
        assert!(
            c.get(&ns(1)).is_some(),
            "positive entry should still be live at 200ms with a 500ms TTL"
        );
        assert!(
            c.get(&ns(2)).is_none(),
            "negative entry should have expired at 200ms with a 100ms TTL"
        );
    }

    #[test]
    fn read_hit_does_not_refresh_ttl() {
        let c = LazyOriginCache::new(8, Duration::from_millis(500), Duration::from_millis(500));
        c.insert(ns(1), vec![addr(1)]);
        thread::sleep(Duration::from_millis(250));
        assert!(c.get(&ns(1)).is_some());
        thread::sleep(Duration::from_millis(500));
        assert!(
            c.get(&ns(1)).is_none(),
            "read-hit illegally extended the TTL"
        );
    }

    #[test]
    fn lru_eviction_at_cap_drops_oldest() {
        let c = LazyOriginCache::new(2, Duration::from_secs(30), Duration::from_secs(5));
        c.insert(ns(1), vec![addr(1)]);
        c.insert(ns(2), vec![addr(2)]);
        c.insert(ns(3), vec![addr(3)]);
        assert!(c.get(&ns(1)).is_none());
        assert!(c.get(&ns(2)).is_some());
        assert!(c.get(&ns(3)).is_some());
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn read_hit_bumps_lru() {
        let c = LazyOriginCache::new(2, Duration::from_secs(30), Duration::from_secs(5));
        c.insert(ns(1), vec![addr(1)]);
        c.insert(ns(2), vec![addr(2)]);
        assert!(c.get(&ns(1)).is_some()); // bump ns(1) -> ns(2) becomes LRU
        c.insert(ns(3), vec![addr(3)]);
        assert!(c.get(&ns(1)).is_some());
        assert!(c.get(&ns(2)).is_none());
        assert!(c.get(&ns(3)).is_some());
    }

    #[test]
    fn zero_positive_ttl_never_positively_hits() {
        let c = LazyOriginCache::new(8, Duration::ZERO, Duration::from_secs(5));
        c.insert(ns(1), vec![addr(1)]);
        assert!(c.get(&ns(1)).is_none());
    }

    // -- resolution --

    #[test]
    fn resolves_operators_to_active_nodes() {
        let rev = StdRwLock::new(HashMap::from([(addr(1), nid(0xA)), (addr(2), nid(0xB))]));
        let stakers = StubStakers::new(&[nid(0xA), nid(0xB)]);
        let got = resolve_active(&[addr(1), addr(2)], &rev, &stakers);
        assert_eq!(got, vec![nid(0xA), nid(0xB)]);
    }

    #[test]
    fn inactive_operators_are_filtered_out() {
        let rev = StdRwLock::new(HashMap::from([(addr(1), nid(0xA)), (addr(2), nid(0xB))]));
        let stakers = StubStakers::new(&[nid(0xA)]);
        let got = resolve_active(&[addr(1), addr(2)], &rev, &stakers);
        assert_eq!(got, vec![nid(0xA)]);
    }

    #[test]
    fn operator_without_reverse_binding_is_dropped() {
        let rev = StdRwLock::new(HashMap::from([(addr(1), nid(0xA))]));
        let stakers = StubStakers::new(&[nid(0xA), nid(0xB)]);
        // addr(2) has no reverse binding at all; must be dropped, not
        // fallen-back-to-chain.
        let got = resolve_active(&[addr(1), addr(2)], &rev, &stakers);
        assert_eq!(got, vec![nid(0xA)]);
    }

    #[test]
    fn result_is_sorted_and_deduped() {
        let rev = StdRwLock::new(HashMap::from([
            (addr(1), nid(0xB)),
            (addr(2), nid(0xA)),
            (addr(3), nid(0xB)),
        ]));
        let stakers = StubStakers::new(&[nid(0xA), nid(0xB)]);
        let got = resolve_active(&[addr(1), addr(2), addr(3)], &rev, &stakers);
        assert_eq!(got, vec![nid(0xA), nid(0xB)]);
    }
}
