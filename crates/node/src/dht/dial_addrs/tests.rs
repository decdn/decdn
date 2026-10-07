use super::*;

fn node(seed: u8) -> (NodeId, PublicKey) {
    let pk = iroh::SecretKey::from_bytes(&[seed; 32]).public();
    (NodeId::from_bytes(*pk.as_bytes()), pk)
}

fn sock(port: u16) -> SocketAddr {
    SocketAddr::from(([203, 0, 113, 10], port))
}

fn lookup_addrs(dir: &DialAddrDirectory, pk: PublicKey) -> Vec<SocketAddr> {
    dir.lookup()
        .get_endpoint_info(pk)
        .map(|info| info.to_endpoint_addr().ip_addrs().copied().collect())
        .unwrap_or_default()
}

#[test]
fn set_publishes_to_the_lookup_and_empty_removes() {
    let dir = DialAddrDirectory::default();
    let (id, pk) = node(1);

    dir.set(id, vec![sock(4433)]);
    assert_eq!(dir.get(&id), Some(vec![sock(4433)]));
    assert_eq!(lookup_addrs(&dir, pk), vec![sock(4433)]);

    dir.set(id, vec![sock(5000)]);
    assert_eq!(
        lookup_addrs(&dir, pk),
        vec![sock(5000)],
        "an update replaces the old address rather than adding to it"
    );

    dir.set(id, Vec::new());
    assert_eq!(dir.get(&id), None);
    assert!(dir.lookup().get_endpoint_info(pk).is_none());
}

#[test]
fn remove_clears_the_lookup() {
    let dir = DialAddrDirectory::default();
    let (id, pk) = node(2);
    dir.set(id, vec![sock(4433)]);

    dir.remove(&id);

    assert_eq!(dir.get(&id), None);
    assert!(dir.lookup().get_endpoint_info(pk).is_none());
}

#[test]
fn replace_all_drops_nodes_absent_from_the_new_set() {
    let dir = DialAddrDirectory::default();
    let (stale, stale_pk) = node(3);
    let (kept, kept_pk) = node(4);
    let (emptied, emptied_pk) = node(5);
    dir.set(stale, vec![sock(1)]);
    dir.set(emptied, vec![sock(2)]);

    dir.replace_all(HashMap::from([
        (kept, vec![sock(3)]),
        (emptied, Vec::new()),
    ]));

    assert!(dir.lookup().get_endpoint_info(stale_pk).is_none());
    assert!(dir.lookup().get_endpoint_info(emptied_pk).is_none());
    assert_eq!(lookup_addrs(&dir, kept_pk), vec![sock(3)]);
    assert_eq!(dir.get(&emptied), None, "an empty list is absence");
    assert_eq!(dir.get(&stale), None);
}
