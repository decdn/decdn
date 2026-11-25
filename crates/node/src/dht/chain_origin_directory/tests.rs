use super::*;
use crate::dht::seated_origins::SeatedOriginSet;
use std::sync::RwLock as StdRwLock;

fn nid(b: u8) -> NodeId {
    NodeId::from_bytes([b; 32])
}
fn addr(b: u8) -> Address {
    Address::from([b; 20])
}
fn ns(n: u64) -> U256 {
    U256::from(n)
}

/// Stub staker set with a mutable active set, for liveness tests.
#[derive(Debug)]
struct StubStakers(StdRwLock<std::collections::HashSet<NodeId>>);

impl StubStakers {
    fn new(active: &[NodeId]) -> Self {
        Self(StdRwLock::new(active.iter().copied().collect()))
    }
    fn set_active(&self, node_id: NodeId, active: bool) {
        let mut guard = self.0.write().unwrap();
        if active {
            guard.insert(node_id);
        } else {
            guard.remove(&node_id);
        }
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

fn directory(
    seated: Arc<SeatedOriginSet>,
    operator_to_node: HashMap<Address, NodeId>,
    staker_set: Arc<StubStakers>,
    content_deny: Arc<ContentDenylist>,
) -> ChainOriginDirectory {
    ChainOriginDirectory::new(
        seated,
        Arc::new(RwLock::new(operator_to_node)),
        staker_set,
        content_deny,
    )
}

fn no_deny() -> Arc<ContentDenylist> {
    Arc::new(ContentDenylist::empty())
}

#[tokio::test]
async fn resolves_the_seated_operators_of_a_namespace() {
    let seated = Arc::new(SeatedOriginSet::from_seats([
        (ns(7), addr(0xA)),
        (ns(7), addr(0xB)),
        (ns(8), addr(0xC)),
    ]));
    let stakers = Arc::new(StubStakers::new(&[nid(0xA), nid(0xB), nid(0xC)]));
    let dir = directory(
        seated,
        HashMap::from([
            (addr(0xA), nid(0xA)),
            (addr(0xB), nid(0xB)),
            (addr(0xC), nid(0xC)),
        ]),
        stakers,
        no_deny(),
    );
    assert_eq!(dir.lookup_origins(ns(7)).await, vec![nid(0xA), nid(0xB)]);
    assert_eq!(dir.lookup_origins(ns(8)).await, vec![nid(0xC)]);
}

#[tokio::test]
async fn unseated_namespace_resolves_no_origins() {
    let seated = Arc::new(SeatedOriginSet::from_seats([(ns(7), addr(0xA))]));
    let stakers = Arc::new(StubStakers::new(&[nid(0xA)]));
    let dir = directory(
        seated,
        HashMap::from([(addr(0xA), nid(0xA))]),
        stakers,
        no_deny(),
    );
    assert!(dir.lookup_origins(ns(9)).await.is_empty());
    assert!(dir.lookup_origins(ns(0)).await.is_empty());
}

#[tokio::test]
async fn seat_changes_take_effect_on_the_next_lookup() {
    let seated = Arc::new(SeatedOriginSet::new());
    let stakers = Arc::new(StubStakers::new(&[nid(0xA)]));
    let dir = directory(
        Arc::clone(&seated),
        HashMap::from([(addr(0xA), nid(0xA))]),
        stakers,
        no_deny(),
    );
    assert!(dir.lookup_origins(ns(7)).await.is_empty());
    seated.replace([(ns(7), addr(0xA))]);
    assert_eq!(dir.lookup_origins(ns(7)).await, vec![nid(0xA)]);
    seated.replace([]);
    assert!(dir.lookup_origins(ns(7)).await.is_empty());
}

#[tokio::test]
async fn liveness_is_applied_live_from_staker_set() {
    let seated = Arc::new(SeatedOriginSet::from_seats([
        (ns(7), addr(0xA)),
        (ns(7), addr(0xB)),
    ]));
    let stakers = Arc::new(StubStakers::new(&[nid(0xA), nid(0xB)]));
    let dir = directory(
        seated,
        HashMap::from([(addr(0xA), nid(0xA)), (addr(0xB), nid(0xB))]),
        Arc::clone(&stakers),
        no_deny(),
    );
    assert_eq!(dir.lookup_origins(ns(7)).await, vec![nid(0xA), nid(0xB)]);
    stakers.set_active(nid(0xB), false);
    assert_eq!(dir.lookup_origins(ns(7)).await, vec![nid(0xA)]);
}

#[tokio::test]
async fn inactive_or_unbound_operators_are_filtered() {
    // B is bonded-but-inactive; C has no reverse binding at all.
    let seated = Arc::new(SeatedOriginSet::from_seats([
        (ns(7), addr(0xA)),
        (ns(7), addr(0xB)),
        (ns(7), addr(0xC)),
    ]));
    let stakers = Arc::new(StubStakers::new(&[nid(0xA)]));
    let dir = directory(
        seated,
        HashMap::from([(addr(0xA), nid(0xA)), (addr(0xB), nid(0xB))]),
        stakers,
        no_deny(),
    );
    assert_eq!(dir.lookup_origins(ns(7)).await, vec![nid(0xA)]);
}

#[tokio::test]
async fn blacklisted_operator_is_dropped_from_the_resolved_set() {
    // Both A and B are bonded, active, and seated, but B is on the deny-set
    // (a governance origin/operator blacklist the watcher synced). The node
    // must not route an origin-pull to B even though the seat set still lists
    // it: the contract does not filter getOrigins.
    let seated = Arc::new(SeatedOriginSet::from_seats([
        (ns(7), addr(0xA)),
        (ns(7), addr(0xB)),
    ]));
    let stakers = Arc::new(StubStakers::new(&[nid(0xA), nid(0xB)]));
    let deny = Arc::new(ContentDenylist::empty());
    deny.set_chain_origins(std::collections::HashSet::from([addr(0xB)]));
    let dir = directory(
        seated,
        HashMap::from([(addr(0xA), nid(0xA)), (addr(0xB), nid(0xB))]),
        stakers,
        Arc::clone(&deny),
    );

    assert_eq!(dir.lookup_origins(ns(7)).await, vec![nid(0xA)]);
    assert_eq!(dir.origin_nodes(&[addr(0xB)]), Vec::<NodeId>::new());

    // The filter is applied live: lifting the blacklist restores B.
    deny.set_chain_origins(std::collections::HashSet::new());
    assert_eq!(dir.lookup_origins(ns(7)).await, vec![nid(0xA), nid(0xB)]);
}
