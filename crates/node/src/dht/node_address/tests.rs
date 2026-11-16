use super::*;

fn nid(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}

fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

#[test]
fn static_directory_resolves_known_and_misses_unknown() {
    let mut m = HashMap::new();
    m.insert(nid(1), addr(0xAA));
    let dir = StaticNodeAddressDirectory::new(m);
    assert_eq!(dir.address_of(&nid(1)), Some(addr(0xAA)));
    assert_eq!(dir.address_of(&nid(2)), None);
}

#[test]
fn chain_directory_reads_the_shared_bindings() {
    let bindings = Arc::new(RwLock::new(HashMap::from([(nid(1), addr(0xAA))])));
    let dir = ChainNodeAddressDirectory::from_parts(Arc::clone(&bindings));
    assert_eq!(dir.address_of(&nid(1)), Some(addr(0xAA)));
    bindings.write().unwrap().insert(nid(1), addr(0xBB));
    assert_eq!(dir.address_of(&nid(1)), Some(addr(0xBB)));
    assert_eq!(dir.address_of(&nid(2)), None);
}
