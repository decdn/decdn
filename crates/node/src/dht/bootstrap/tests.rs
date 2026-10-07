use super::*;
use crate::dht::staker_set::ConfigStakerSet;
use std::collections::HashSet;

fn nid(b: u8) -> NodeId {
    NodeId::from_bytes([b; 32])
}

/// Without spinning up real iroh endpoints we can't exercise the
/// `FindNode` round-trip from a unit test — that lives in the
/// loopback integration test. But step 1 (seed the routing table
/// from `StakerSet::active_nodes`) is pure local mutation; verify
/// it works against a self-id and against an empty set.
#[test]
fn empty_staker_set_yields_empty_routing_table() {
    let self_id = nid(0);
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::empty());
    let routing = Arc::new(Mutex::new(RoutingTable::new(self_id)));
    // Step 1 is pure; we can run it without an endpoint.
    let seeds = staker_set.active_nodes();
    assert!(seeds.is_empty());
    for peer in seeds {
        routing.lock().unwrap().insert(peer);
    }
    assert!(routing.lock().unwrap().is_empty());
}

#[test]
fn seed_step_skips_self_id_and_inserts_others() {
    let self_id = nid(0);
    let mut set = HashSet::new();
    set.insert(self_id); // should be filtered
    set.insert(nid(1));
    set.insert(nid(2));
    set.insert(nid(3));
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(set));
    let routing = Arc::new(Mutex::new(RoutingTable::new(self_id)));
    // Simulate the seed step in isolation.
    let mut inserted = 0usize;
    for &peer in &staker_set.active_nodes() {
        if peer == self_id {
            continue;
        }
        if routing.lock().unwrap().insert(peer) {
            inserted += 1;
        }
    }
    assert_eq!(inserted, 3, "all non-self seeds must land in the table");
    assert!(!routing.lock().unwrap().contains(&self_id));
    assert!(routing.lock().unwrap().contains(&nid(1)));
}
