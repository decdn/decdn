use super::*;

fn ns(n: u64) -> U256 {
    U256::from(n)
}
fn nid(b: u8) -> NodeId {
    NodeId::from_bytes([b; 32])
}

#[tokio::test]
async fn empty_directory_returns_empty_for_every_lookup() {
    // The runtime's non-chain fallback: nothing resolves for any namespace.
    let dir = EmptyOriginDirectory;
    assert!(dir.lookup_origins(ns(0)).await.is_empty());
    assert!(dir.lookup_origins(ns(7)).await.is_empty());
}

#[tokio::test]
async fn static_directory_with_no_entries_resolves_nothing() {
    let dir = StaticOriginDirectory::new(HashMap::new());
    assert!(dir.lookup_origins(ns(3)).await.is_empty());
}

#[tokio::test]
async fn lookup_returns_configured_origins() {
    let mut m = HashMap::new();
    m.insert(ns(1), vec![nid(0xA), nid(0xB)]);
    m.insert(ns(2), vec![nid(0xC)]);
    let dir = StaticOriginDirectory::new(m);
    assert_eq!(dir.lookup_origins(ns(1)).await, vec![nid(0xA), nid(0xB)]);
    assert_eq!(dir.lookup_origins(ns(2)).await, vec![nid(0xC)]);
    // Namespace 0 (no namespace) and any unknown id fall through to an empty
    // vec — the caller distinguishes "directory has nothing for this
    // namespace" from "directory not initialised" via the empty result, NOT
    // via an Option.
    assert!(dir.lookup_origins(ns(0)).await.is_empty());
    assert!(dir.lookup_origins(ns(3)).await.is_empty());
}
