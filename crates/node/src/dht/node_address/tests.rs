use super::*;

fn nid(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}

fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

fn fresh() -> (Arc<RwLock<HashMap<NodeId, Address>>>, Arc<Metrics>) {
    (
        Arc::new(RwLock::new(HashMap::new())),
        Arc::new(Metrics::new()),
    )
}

#[test]
fn static_directory_resolves_known_and_misses_unknown() {
    let mut m = HashMap::new();
    m.insert(nid(1), addr(0xAA));
    let dir = StaticNodeAddressDirectory::new(m);
    assert_eq!(dir.address_of(&nid(1)), Some(addr(0xAA)));
    assert_eq!(dir.address_of(&nid(2)), None);
}

/// `set_binding` inserts and surfaces the address; the size gauge tracks the
/// growing set, and a same-key overwrite keeps cardinality (and the gauge)
/// stable while updating the bound address.
#[test]
fn set_binding_inserts_and_overwrites() {
    let (bindings, metrics) = fresh();
    set_binding(&bindings, &metrics, nid(1), addr(0xAA));
    assert_eq!(
        bindings.read().unwrap().get(&nid(1)).copied(),
        Some(addr(0xAA))
    );
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_node_address_directory_size 1"),
        "gauge should report 1 after one insert:\n{text}"
    );

    // Re-registration of the same node with a rotated address overwrites
    // without changing cardinality.
    set_binding(&bindings, &metrics, nid(1), addr(0xBB));
    assert_eq!(
        bindings.read().unwrap().get(&nid(1)).copied(),
        Some(addr(0xBB))
    );
    assert_eq!(bindings.read().unwrap().len(), 1);
}

/// `remove_binding` drops a present key (and shrinks the gauge) but is a
/// no-op for an absent one.
#[test]
fn remove_binding_present_and_absent() {
    let (bindings, metrics) = fresh();
    set_binding(&bindings, &metrics, nid(1), addr(0xAA));
    set_binding(&bindings, &metrics, nid(2), addr(0xBB));
    remove_binding(&bindings, &metrics, &nid(1));
    assert!(bindings.read().unwrap().get(&nid(1)).is_none());
    assert_eq!(bindings.read().unwrap().len(), 1);
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_node_address_directory_size 1"),
        "gauge should report 1 after removing one of two:\n{text}"
    );

    // Removing an absent key changes nothing.
    remove_binding(&bindings, &metrics, &nid(0xFF));
    assert_eq!(bindings.read().unwrap().len(), 1);
}
