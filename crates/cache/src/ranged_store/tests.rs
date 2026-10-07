use super::NodeRangedStore;
use crate::engine::CacheEngine;
use iroh_blobs::Hash;

/// The `hash`/`engine` accessors round-trip the values
/// `new` was constructed with — the node-crate `NodeAdmitStore` wrapper
/// reaches `admit_bao_stream` through these rather than duplicating the
/// `(engine, hash, total_bytes)` triple.
#[tokio::test]
async fn node_ranged_store_accessors_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let hash = Hash::from([7u8; 32]);
    let store = NodeRangedStore::new(engine, hash, 4096);
    assert_eq!(store.hash(), hash);
    // Round-trip a query through the accessor's engine handle to prove it
    // is a live, queryable handle rather than just a structural copy.
    assert!(
        store
            .engine()
            .present_ranges(hash)
            .await
            .unwrap()
            .is_empty(),
        "the accessor's engine handle is live and queryable"
    );
}
