use super::*;

fn nid(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}

#[test]
fn config_staker_set_is_active_matches_membership() {
    let mut s = HashSet::new();
    s.insert(nid(1));
    s.insert(nid(2));
    let set = ConfigStakerSet::new(s);
    assert!(set.is_active(&nid(1)));
    assert!(set.is_active(&nid(2)));
    assert!(!set.is_active(&nid(3)));
}

#[test]
fn config_staker_set_empty_rejects_every_node() {
    let set = ConfigStakerSet::empty();
    assert!(set.is_empty());
    assert!(!set.is_active(&nid(0)));
    assert!(!set.is_active(&nid(0xFF)));
}

#[test]
fn config_staker_set_active_nodes_returns_full_membership() {
    let mut s = HashSet::new();
    s.insert(nid(1));
    s.insert(nid(2));
    s.insert(nid(3));
    let set = ConfigStakerSet::new(s.clone());
    let got: HashSet<NodeId> = set.active_nodes().into_iter().collect();
    assert_eq!(got, s);
    assert_eq!(set.len(), 3);
    assert!(!set.is_empty());
}
