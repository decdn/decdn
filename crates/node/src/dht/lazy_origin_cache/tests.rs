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
