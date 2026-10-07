use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::Bytes;
use decdn_bao_range::{IROH_BLOCK_SIZE, RangedStore, align_range, encode_verified_range};
use decdn_cache::CacheEngine;
use decdn_client::IngestStore;
use decdn_client::sink::StashedFault;
use iroh_io::AsyncStreamReader;

use super::NodeAdmitStore;

/// Deterministic pseudo-random blob, matching the generator the cache
/// crate's own `admit_bao_stream` tests use (`crates/cache/src/engine.rs`).
fn synth_blob(len: usize) -> ([u8; 32], Vec<u8>, Bytes) {
    let mut plaintext = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut plaintext {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    let ob = PreOrderMemOutboard::create(&plaintext, IROH_BLOCK_SIZE);
    (*ob.root.as_bytes(), plaintext, Bytes::from(ob.data))
}

/// A minimal in-memory [`decdn_client::BaoRangeReader`]: an
/// [`AsyncStreamReader`] over a `Bytes` cursor plus a trivial
/// [`StashedFault`] that never parks anything (mirrors the shape of
/// `crates/client/src/source.rs`'s `ScriptedReader` test double).
struct MemReader {
    wire: Bytes,
}

impl AsyncStreamReader for MemReader {
    async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        let take = self.wire.len().min(len);
        Ok(self.wire.split_to(take))
    }

    async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
        if self.wire.len() < L {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "MemReader exhausted before a fixed-size bao read",
            ));
        }
        let got = self.wire.split_to(L);
        let mut out = [0u8; L];
        out.copy_from_slice(&got);
        Ok(out)
    }
}

impl StashedFault for MemReader {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        None
    }
}

/// Build the header-less bao wire for an interior aligned range of a
/// synthetic 4-group blob, exactly as `crates/cache/src/engine.rs`'s
/// `admit_bao_stream` tests do: `encode_verified_range` produces the
/// 8-byte-size-header-prefixed combined encoding, and the wire
/// `admit_bao_stream` consumes is header-less (ADR 038; the size comes
/// from the caller's `total_bytes` instead).
fn interior_range_wire() -> (
    [u8; 32],
    u64,
    decdn_bao_range::AlignedRange,
    bao_tree::ChunkRanges,
    Bytes,
) {
    let group = decdn_cache::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let aligned = align_range(group, group, total).expect("align interior group");
    let s = aligned.fetch_start() as usize;
    let e = aligned.fetch_end() as usize;
    let combined = encode_verified_range(root, &aligned, &plaintext[s..e], outboard)
        .expect("encode verified range");
    assert!(combined.len() > 8, "combined wire must carry the header");
    let header_less = combined.slice(8..);
    let ranges = aligned.chunk_ranges().clone();
    (root, total, aligned, ranges, header_less)
}

/// `ingest_stream` streams a gap's header-less bao wire into the cache and
/// the store reports the range present but the blob still partial.
#[tokio::test]
async fn node_admit_store_ingests_a_partial_range() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let (root, total, aligned, _ranges, wire) = interior_range_wire();
    let hash = decdn_cache::Hash::from(root);
    let store = NodeAdmitStore::new(engine, hash, total, None);

    let reader = MemReader { wire };
    let (mut drained, _) =
        IngestStore::ingest_stream(&store, &aligned, reader, None, aligned.blob_size(), None)
            .await
            .unwrap();
    assert_eq!(
        drained.read_bytes(1).await.unwrap().len(),
        0,
        "the reader is fully drained by admit_bao_stream"
    );

    let present = RangedStore::present_ranges(&store).await.unwrap();
    assert!(!present.is_empty(), "the ingested range must be present");
    assert!(
        !RangedStore::is_complete(&store).await.unwrap(),
        "a single interior group of a 4-group blob must not be complete"
    );
}

/// The cache admit cannot stop a range early, so an ingest asked to stop
/// is refused as this process's own fault before a byte is read.
#[tokio::test]
async fn node_admit_store_refuses_a_stop_point_as_a_local_fault() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let (root, total, aligned, _ranges, wire) = interior_range_wire();
    let store = NodeAdmitStore::new(engine, decdn_cache::Hash::from(root), total, None);
    let end = std::sync::atomic::AtomicU64::new(u64::MAX);
    let err = IngestStore::ingest_stream(
        &store,
        &aligned,
        MemReader { wire },
        None,
        aligned.blob_size(),
        Some(&end),
    )
    .await
    .err()
    .expect("a stop point is refused");
    assert!(err.is::<decdn_client::LocalPullFault>(), "{err:#}");
    assert!(
        RangedStore::present_ranges(&store)
            .await
            .unwrap()
            .is_empty()
    );
}

/// `RangedStore` queries delegate to the inner `NodeRangedStore`: the
/// full aligned range is missing before ingest, and the gap shrinks after.
#[tokio::test]
async fn node_admit_store_delegates_ranged_store_queries() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let (root, total, aligned, ranges, wire) = interior_range_wire();
    let hash = decdn_cache::Hash::from(root);
    let store = NodeAdmitStore::new(engine, hash, total, None);

    let before = RangedStore::missing_ranges(&store, aligned.fetch_start(), aligned.fetch_len())
        .await
        .unwrap();
    assert_eq!(
        &before, &ranges,
        "before ingest, the whole aligned range must be missing"
    );

    let reader = MemReader { wire };
    let _drained =
        IngestStore::ingest_stream(&store, &aligned, reader, None, aligned.blob_size(), None)
            .await
            .unwrap();

    let after = RangedStore::missing_ranges(&store, aligned.fetch_start(), aligned.fetch_len())
        .await
        .unwrap();
    assert!(
        after.is_empty(),
        "after ingest, the just-admitted range must no longer be missing"
    );
}

/// The capture invariant the coherent serve encoder relies on (#1621, ADR 038):
/// admitting a blob feeds the shared outboard exactly the blob's true
/// pre-order outboard. Every interior node the encoder will `load` must match
/// `bao_tree`'s own outboard for the same content, so the re-encoded downstream
/// wire is byte-identical to a single whole-blob encode.
#[tokio::test]
async fn capture_reconstructs_the_true_outboard() {
    use bao_tree::BaoTree;
    use bao_tree::io::fsm::Outboard as FsmOutboard;
    use bao_tree::io::sync::Outboard as SyncOutboard;

    let group = decdn_cache::CHUNK_GROUP_BYTES;
    let total = 5 * group + 321; // several groups plus a ragged tail
    let (root, plaintext, outboard) = synth_blob(total as usize);

    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let hash = decdn_cache::Hash::from(root);

    let session = decdn_cache::FillSession::new(bao_tree::blake3::Hash::from(root), total);
    let store = NodeAdmitStore::new(engine, hash, total, Some(std::sync::Arc::clone(&session)));

    // Admit the whole blob in one range → the capture unions to the whole tree.
    let aligned = align_range(0, total, total).expect("align whole blob");
    let combined =
        encode_verified_range(root, &aligned, &plaintext, outboard).expect("encode whole blob");
    let wire = combined.slice(8..);
    IngestStore::ingest_stream(
        &store,
        &aligned,
        MemReader { wire },
        None,
        aligned.blob_size(),
        None,
    )
    .await
    .unwrap();

    // Compare the captured outboard against the blob's true outboard, node by node.
    let mut reader = session.outboard_reader();
    let truth = PreOrderMemOutboard::create(&plaintext, IROH_BLOCK_SIZE);
    let tree = BaoTree::new(total, IROH_BLOCK_SIZE);
    let mut internal = 0u64;
    for node in tree.pre_order_nodes_iter() {
        if tree.pre_order_offset(node).is_some() {
            internal += 1;
            let got = FsmOutboard::load(&mut reader, node).await.unwrap();
            let want = SyncOutboard::load(&truth, node).unwrap();
            assert_eq!(
                got, want,
                "captured node {node:?} must match the true outboard"
            );
        }
    }
    assert!(
        internal > 0,
        "a multi-group blob has interior nodes to capture"
    );
}

/// Set every directory under `root` to `mode`, so a store opened there can
/// (or cannot) create files.
#[cfg(unix)]
fn chmod_dirs(root: &std::path::Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                dirs.push(entry.path());
            }
        }
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(mode)).unwrap();
    }
}

/// The store failing under an ingest is this node's own fault (#2286). The
/// failure comes from the real store, not a hand-built error: iroh-blobs
/// reports a failed data-file write without its `io::ErrorKind`, so the
/// verdict cannot rest on the kind.
#[cfg(unix)]
#[tokio::test]
async fn a_store_that_cannot_write_is_a_local_fault_not_the_peers() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let (root, total, aligned, _ranges, wire) = interior_range_wire();
    let store = NodeAdmitStore::new(engine, decdn_cache::Hash::from(root), total, None);

    chmod_dirs(tmp.path(), 0o555);
    if std::fs::File::create(tmp.path().join("probe")).is_ok() {
        // Running as root: directory modes do not stop it, so there is no
        // store fault to observe.
        chmod_dirs(tmp.path(), 0o755);
        return;
    }
    let err = IngestStore::ingest_stream(
        &store,
        &aligned,
        MemReader { wire },
        None,
        aligned.blob_size(),
        None,
    )
    .await
    .err();
    chmod_dirs(tmp.path(), 0o755);
    let err = err.expect("a store that cannot create its data file fails the ingest");
    assert_eq!(
        super::super::pull_verdict(&err),
        super::super::PullVerdict::OurLocalFault,
        "{err:#}"
    );
}

/// A wire the peer cut short fails the same ingest, and that is the peer's
/// short delivery: it still scores `Unreachable`.
#[tokio::test]
async fn a_truncated_wire_is_the_peers_fault() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let (root, total, aligned, _ranges, wire) = interior_range_wire();
    let store = NodeAdmitStore::new(engine, decdn_cache::Hash::from(root), total, None);

    let cut = wire.slice(..wire.len() / 2);
    let err = IngestStore::ingest_stream(
        &store,
        &aligned,
        MemReader { wire: cut },
        None,
        aligned.blob_size(),
        None,
    )
    .await
    .err()
    .expect("a truncated wire fails the ingest");
    assert_eq!(
        super::super::pull_verdict(&err),
        super::super::PullVerdict::Unreachable,
        "{err:#}"
    );
}
