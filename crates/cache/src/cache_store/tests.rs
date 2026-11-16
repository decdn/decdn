use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::Bytes;
use decdn_bao_range::{
    AlignedRange, CHUNK_GROUP_BYTES, IROH_BLOCK_SIZE, IngestEnd, IngestStore, RangedStore,
    StashedFault, StreamStore, align_range, encode_verified_range,
};
use iroh_io::AsyncStreamReader;

use super::CacheStore;
use crate::{CacheEngine, FillSession, Hash};

/// A leg's wire in memory, and the fault it parks, if any.
struct MemReader {
    wire: Bytes,
    fault: Option<anyhow::Error>,
}

impl MemReader {
    fn new(wire: Bytes) -> Self {
        Self { wire, fault: None }
    }
}

impl AsyncStreamReader for MemReader {
    async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        let take = self.wire.len().min(len);
        Ok(self.wire.split_to(take))
    }

    async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
        let mut out = [0u8; L];
        if self.wire.len() < L {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        out.copy_from_slice(&self.wire.split_to(L));
        Ok(out)
    }
}

impl StashedFault for MemReader {
    fn take_fault(&mut self) -> Option<anyhow::Error> {
        self.fault.take()
    }
}

/// A blob of `len` pseudo-random bytes: its root, its bytes and its outboard.
fn synth_blob(len: u64) -> ([u8; 32], Vec<u8>, Bytes) {
    let mut data = vec![0u8; usize::try_from(len).unwrap()];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut data {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes()[0];
    }
    let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
    (*ob.root.as_bytes(), data, Bytes::from(ob.data))
}

/// The header-less wire for `[offset, offset + len)` of a `total`-byte blob.
fn wire(total: u64, offset: u64, len: u64) -> ([u8; 32], Vec<u8>, AlignedRange, Bytes) {
    let (root, data, outboard) = synth_blob(total);
    let range = align_range(offset, len, total).unwrap();
    let start = usize::try_from(range.fetch_start()).unwrap();
    let end = usize::try_from(range.fetch_end()).unwrap();
    let encoded = encode_verified_range(root, &range, &data[start..end], outboard).unwrap();
    (root, data, range, encoded.slice(8..))
}

async fn engine(dir: &tempfile::TempDir) -> CacheEngine {
    CacheEngine::open(dir.path(), vec![], 16).await.unwrap()
}

/// A drained leg reports each verified group's bytes on its range, wakes a
/// waiting reader, and leaves the entry whole for the next store over it.
#[tokio::test]
async fn a_drained_leg_lands_in_the_entry_for_the_next_store() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(&dir).await;
    let total = 4 * CHUNK_GROUP_BYTES;
    let (root, data, range, wire) = wire(total, 0, total);
    let store = CacheStore::new(engine.clone(), Hash::from_bytes(root), total);

    let grew = store.present_grew().notified();
    tokio::pin!(grew);
    grew.as_mut().enable();
    let seen = Mutex::new(Vec::new());
    let on_progress = |received: u64| seen.lock().unwrap().push(received);
    let (_, end) = IngestStore::ingest_stream(
        &store,
        &range,
        MemReader::new(wire),
        Some(&on_progress),
        total,
        None,
    )
    .await
    .unwrap();

    assert_eq!(end, IngestEnd::Drained);
    assert_eq!(
        *seen.lock().unwrap(),
        [1, 2, 3, 4].map(|n| n * CHUNK_GROUP_BYTES),
        "progress counts the bytes verified on the range, one report per group"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), grew)
            .await
            .is_ok(),
        "a waiting reader wakes as the bytes land"
    );
    let again = CacheStore::new(engine, Hash::from_bytes(root), total);
    assert!(again.is_complete().await.unwrap());
    assert_eq!(again.read(0, total).await.unwrap(), data);
}

/// A lowered end stops the leg at the first group boundary past it, and
/// exactly that prefix is present.
#[tokio::test]
async fn a_lowered_end_stops_the_leg_at_a_group_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let total = 8 * CHUNK_GROUP_BYTES;
    let (root, data, range, wire) = wire(total, 0, total);
    let store = CacheStore::new(engine(&dir).await, Hash::from_bytes(root), total);

    let stop_at = AtomicU64::new(CHUNK_GROUP_BYTES + 1);
    let (_, end) = IngestStore::ingest_stream(
        &store,
        &range,
        MemReader::new(wire),
        None,
        total,
        Some(&stop_at),
    )
    .await
    .unwrap();

    assert_eq!(end, IngestEnd::Stopped);
    let held = 2 * CHUNK_GROUP_BYTES;
    assert_eq!(
        store.missing_ranges(0, total).await.unwrap(),
        bao_tree::ChunkRanges::from(
            bao_tree::ChunkNum(held / 1024)..bao_tree::ChunkNum(total / 1024)
        ),
    );
    assert_eq!(
        store.read(0, held).await.unwrap(),
        data[..usize::try_from(held).unwrap()]
    );
}

/// An interior range lands as a partial: missing before, present after, and
/// the blob not complete. The hook sees the bytes the leg admitted.
#[tokio::test]
async fn an_interior_range_lands_as_a_partial_and_counts_its_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let total = 4 * CHUNK_GROUP_BYTES;
    let (root, _, range, wire) = wire(total, CHUNK_GROUP_BYTES, CHUNK_GROUP_BYTES);
    let admitted = Arc::new(AtomicU64::new(0));
    let counted = Arc::clone(&admitted);
    let store = CacheStore::new(engine(&dir).await, Hash::from_bytes(root), total).on_admitted(
        Arc::new(move |bytes| {
            counted.fetch_add(bytes, Ordering::SeqCst);
        }),
    );

    let before = store
        .missing_ranges(range.fetch_start(), range.fetch_len())
        .await
        .unwrap();
    assert_eq!(&before, range.chunk_ranges());
    let (mut drained, _) =
        IngestStore::ingest_stream(&store, &range, MemReader::new(wire), None, total, None)
            .await
            .unwrap();
    assert!(drained.read_bytes(1).await.unwrap().is_empty(), "drained");

    let after = store
        .missing_ranges(range.fetch_start(), range.fetch_len())
        .await
        .unwrap();
    assert!(after.is_empty());
    assert!(!store.is_complete().await.unwrap());
    assert_eq!(admitted.load(Ordering::SeqCst), CHUNK_GROUP_BYTES);
}

/// A fault the reader parked after its last byte (a voucher rejected at the
/// closing interval) outranks the clean admit, and the hook counts nothing.
#[tokio::test]
async fn a_fault_parked_after_the_last_byte_outranks_a_clean_admit() {
    let dir = tempfile::tempdir().unwrap();
    let total = 2 * CHUNK_GROUP_BYTES;
    let (root, _, range, wire) = wire(total, 0, total);
    let admitted = Arc::new(AtomicU64::new(0));
    let counted = Arc::clone(&admitted);
    let store = CacheStore::new(engine(&dir).await, Hash::from_bytes(root), total).on_admitted(
        Arc::new(move |bytes| {
            counted.fetch_add(bytes, Ordering::SeqCst);
        }),
    );

    let reader = MemReader {
        wire,
        fault: Some(anyhow::anyhow!("voucher rejected at the closing interval")),
    };
    let err = IngestStore::ingest_stream(&store, &range, reader, None, total, None)
        .await
        .err()
        .expect("the parked fault fails the ingest");
    assert!(err.to_string().contains("voucher rejected"), "{err:#}");
    assert_eq!(admitted.load(Ordering::SeqCst), 0);
}

/// Bytes that do not verify, with no fault parked, are a `HashMismatch`.
#[tokio::test]
async fn corrupt_bytes_are_a_hash_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let total = 2 * CHUNK_GROUP_BYTES;
    let (root, _, range, wire) = wire(total, 0, total);
    let store = CacheStore::new(engine(&dir).await, Hash::from_bytes(root), total);

    let mut corrupt = wire.to_vec();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0xff;
    let err = IngestStore::ingest_stream(
        &store,
        &range,
        MemReader::new(Bytes::from(corrupt)),
        None,
        total,
        None,
    )
    .await
    .err()
    .expect("corrupt bytes fail the ingest");
    assert!(err.is::<decdn_bao_range::HashMismatch>(), "{err:#}");
}

/// The capture invariant the coherent serve encoder relies on (#1621, ADR
/// 038): admitting a blob feeds the shared outboard exactly the blob's true
/// pre-order outboard, so a serving leg's re-encoded wire is byte-identical
/// to a whole-blob encode.
#[tokio::test]
async fn capture_reconstructs_the_true_outboard() {
    use bao_tree::BaoTree;
    use bao_tree::io::fsm::Outboard as FsmOutboard;
    use bao_tree::io::sync::Outboard as SyncOutboard;

    let dir = tempfile::tempdir().unwrap();
    let total = 5 * CHUNK_GROUP_BYTES + 321;
    let (root, data, range, wire) = wire(total, 0, total);
    let session = FillSession::new(bao_tree::blake3::Hash::from(root), total);
    let store = CacheStore::new(engine(&dir).await, Hash::from_bytes(root), total)
        .with_session(Some(Arc::clone(&session)));

    IngestStore::ingest_stream(&store, &range, MemReader::new(wire), None, total, None)
        .await
        .unwrap();

    let mut reader = session.outboard_reader();
    let truth = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
    let tree = BaoTree::new(total, IROH_BLOCK_SIZE);
    let mut internal = 0u64;
    for node in tree.pre_order_nodes_iter() {
        if tree.pre_order_offset(node).is_some() {
            internal += 1;
            let got = FsmOutboard::load(&mut reader, node).await.unwrap();
            let want = SyncOutboard::load(&truth, node).unwrap();
            assert_eq!(
                got, want,
                "captured node {node:?} matches the true outboard"
            );
        }
    }
    assert!(
        internal > 0,
        "a multi-group blob has interior nodes to capture"
    );
}
