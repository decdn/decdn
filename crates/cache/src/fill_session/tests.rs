use super::{FillError, FillSession, IROH_BLOCK_SIZE};
use bao_tree::io::fsm::Outboard;
use bao_tree::{BaoTree, blake3};

fn h(byte: u8) -> blake3::Hash {
    blake3::Hash::from([byte; 32])
}

/// A blob spanning several chunk groups, so the tree has interior nodes with
/// real pre-order offsets to save and read back.
const TOTAL: u64 = 5 * 16 * 1024 + 321;

#[tokio::test]
async fn capture_then_load_round_trips_each_internal_node() {
    let session = FillSession::new(h(0xAA), TOTAL);
    let mut reader = session.outboard_reader();
    let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);

    // Capture a distinct pair into every internal node, then read them all back.
    let mut saved = Vec::new();
    for (i, node) in tree.pre_order_nodes_iter().enumerate() {
        if tree.pre_order_offset(node).is_some() {
            let tag = u8::try_from(i % 251).unwrap();
            let pair = (h(tag), h(tag.wrapping_add(101)));
            session.capture(node, pair);
            saved.push((node, pair));
        }
    }
    for (node, pair) in saved {
        assert_eq!(reader.load(node).await.unwrap(), Some(pair));
    }
}

#[tokio::test]
async fn outboard_buffer_is_allocated_lazily_on_first_capture() {
    // A freshly built session (as `make_session` builds one under the global
    // registry lock) must allocate no outboard buffer — the MB-scale zeroing
    // is deferred to the first capture, off that lock.
    let session = FillSession::new(h(0xDD), TOTAL);
    assert!(
        session.outboard().state.lock().unwrap().bytes.is_empty(),
        "construction allocates no outboard buffer under the registry lock"
    );

    // The first capture sizes the buffer to the full pre-order outboard.
    let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);
    let node = tree
        .pre_order_nodes_iter()
        .find(|n| tree.pre_order_offset(*n).is_some())
        .expect("an interior node exists");
    session.capture(node, (h(1), h(2)));
    assert_eq!(
        session.outboard().state.lock().unwrap().bytes.len() as u64,
        tree.outboard_size(),
        "the first capture sizes the buffer to the whole outboard"
    );
}

#[tokio::test]
async fn capture_many_round_trips_each_internal_node() {
    // One batched `capture_many` must land every pair a per-node `capture` loop
    // would, so a serve leg reads back the whole tree the same way.
    let session = FillSession::new(h(0xBB), TOTAL);
    let mut reader = session.outboard_reader();
    let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);

    let mut batch = Vec::new();
    for (i, node) in tree.pre_order_nodes_iter().enumerate() {
        if tree.pre_order_offset(node).is_some() {
            let tag = u8::try_from(i % 251).unwrap();
            batch.push((node, (h(tag), h(tag.wrapping_add(101)))));
        }
    }
    session.capture_many(batch.clone());
    for (node, pair) in batch {
        assert_eq!(reader.load(node).await.unwrap(), Some(pair));
    }
}

#[tokio::test]
async fn load_awaits_then_resolves_on_capture_many() {
    // A parked reader must wake from the batch's single notify, not only from a
    // per-node one — the notify fires once for the whole admit.
    let session = FillSession::new(h(0xCC), TOTAL);
    let mut reader = session.outboard_reader();
    let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);
    let node = tree
        .pre_order_nodes_iter()
        .find(|n| tree.pre_order_offset(*n).is_some())
        .expect("an interior node exists");
    let pair = (h(7), h(9));

    let load = tokio::spawn(async move { reader.load(node).await });
    tokio::task::yield_now().await;
    assert!(
        !load.is_finished(),
        "load must park until the node is captured"
    );
    session.capture_many([(node, pair)]);
    assert_eq!(load.await.unwrap().unwrap(), Some(pair));
}

#[tokio::test]
async fn leaf_node_loads_none_without_awaiting() {
    // A single-chunk-group blob is one leaf node with no interior hash pairs, so
    // `load(root)` must return `None` immediately (bao_tree sources the leaf from
    // the data reader) and never park — even though nothing was ever captured.
    let small = 4 * 1024;
    let session = FillSession::new(h(1), small);
    let mut reader = session.outboard_reader();
    let tree = BaoTree::new(small, IROH_BLOCK_SIZE);
    let root = tree.root();
    assert!(
        tree.pre_order_offset(root).is_none(),
        "a one-group tree's root is a leaf"
    );
    assert_eq!(reader.load(root).await.unwrap(), None);
}

#[tokio::test]
async fn load_awaits_then_resolves_on_capture() {
    let session = FillSession::new(h(2), TOTAL);
    let mut reader = session.outboard_reader();
    let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);
    let node = tree
        .pre_order_nodes_iter()
        .find(|n| tree.pre_order_offset(*n).is_some())
        .expect("an interior node exists");
    let pair = (h(7), h(9));

    // Load races ahead of capture: it must block, then wake on capture.
    let load = tokio::spawn(async move { reader.load(node).await });
    tokio::task::yield_now().await;
    assert!(
        !load.is_finished(),
        "load must park until the node is captured"
    );
    session.capture(node, pair);
    assert_eq!(load.await.unwrap().unwrap(), Some(pair));
}

#[tokio::test]
async fn load_fails_when_pull_ends_err_before_capture() {
    let session = FillSession::new(h(3), TOTAL);
    let mut reader = session.outboard_reader();
    let tree = BaoTree::new(TOTAL, IROH_BLOCK_SIZE);
    let node = tree
        .pre_order_nodes_iter()
        .find(|n| tree.pre_order_offset(*n).is_some())
        .expect("an interior node exists");

    let load = tokio::spawn(async move { reader.load(node).await });
    tokio::task::yield_now().await;
    session.mark_ended(Err(FillError::new("upstream died")));
    let err = load
        .await
        .unwrap()
        .expect_err("a failed pull must fail the load");
    assert!(err.to_string().contains("upstream pull failed"));
}

#[test]
fn chunk_span_sums_every_finite_piece() {
    use bao_tree::{ChunkNum, ChunkRanges};
    let two =
        &ChunkRanges::from(ChunkNum(0)..ChunkNum(3)) | &ChunkRanges::from(ChunkNum(5)..ChunkNum(9));
    assert_eq!(super::chunk_span(&two), 7);
    assert_eq!(super::chunk_span(&ChunkRanges::empty()), 0);
}

#[test]
fn chunk_span_ignores_an_open_ended_tail() {
    use bao_tree::{ChunkNum, ChunkRanges};
    let open = &ChunkRanges::from(ChunkNum(0)..ChunkNum(2)) | &ChunkRanges::from(ChunkNum(4)..);
    assert_eq!(super::chunk_span(&open), 2);
}
