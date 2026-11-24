//! Client-specific tests for `ClientRangedStore` over a file store (#2881):
//! what a resumed download reads back from the store an earlier run left,
//! and the export a finished blob ends in. The shared conformance suite
//! (`ranged_store_conformance.rs`) covers the cross-backend contract.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests

use bao_tree::{ChunkNum, ChunkRanges};
use bytes::Bytes;
use decdn_bao_range::{AlignedRange, CHUNK_GROUP_BYTES, IROH_BLOCK_SIZE, RangedStore};
use decdn_client::ClientRangedStore;

const GROUP: u64 = CHUNK_GROUP_BYTES;

/// Deterministic blob of `len` bytes plus its bao root and full pre-order
/// outboard. Mirrors `crates/cache/tests/range_pull.rs::make_blob` — an
/// xorshift fill, not random, so runs are reproducible.
fn synth_blob(len: usize) -> ([u8; 32], Vec<u8>, Bytes) {
    let mut plaintext = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut plaintext {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(&plaintext, IROH_BLOCK_SIZE);
    let root: [u8; 32] = *ob.root.as_bytes();
    (root, plaintext, Bytes::from(ob.data))
}

/// Interleaved bao (combined format: 8-byte header + body) for `aligned`,
/// ready to hand to `RangedStore::admit`.
fn bao_for(root: [u8; 32], plaintext: &[u8], outboard: Bytes, aligned: &AlignedRange) -> Bytes {
    let s = usize::try_from(aligned.fetch_start()).expect("fits usize");
    let e = usize::try_from(aligned.fetch_end()).expect("fits usize");
    let data = plaintext.get(s..e).expect("aligned span in bounds");
    decdn_bao_range::encode_verified_range(root, aligned, data, outboard).expect("verifies")
}

/// The chunk range of `[start, end)` bytes.
fn chunks(start: u64, end: u64) -> ChunkRanges {
    ChunkRanges::from(ChunkNum::full_chunks(start)..ChunkNum::chunks(end))
}

/// A store reopened in the directory an earlier run left holds what that run
/// admitted, and only that: a resume lists only the missing middle, so it
/// fetches and pays for nothing twice. The final group proved the size.
#[tokio::test]
async fn a_reopened_store_holds_what_an_earlier_run_admitted() {
    let total = 4 * GROUP + 99;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
    let dir = tempfile::tempdir().expect("tmp dir");

    let store = ClientRangedStore::on_disk(dir.path(), root, total)
        .await
        .expect("open");
    for (start, len) in [(0, GROUP), (4 * GROUP, 99)] {
        let aligned = decdn_bao_range::align_range(start, len, total).expect("align");
        let bao = bao_for(root, &plaintext, outboard.clone(), &aligned);
        store.admit(aligned, bao).await.expect("admit");
    }
    store.close().await.expect("close");

    let reopened = ClientRangedStore::on_disk(dir.path(), root, total)
        .await
        .expect("reopen");
    let present = reopened.present_ranges().await.expect("present");
    assert_eq!(present, chunks(0, GROUP) | chunks(4 * GROUP, total));
    assert_eq!(
        reopened.proven(),
        Some(total),
        "the final group proved the size"
    );
    assert_eq!(
        reopened.missing_ranges(0, 0).await.expect("missing"),
        chunks(GROUP, 4 * GROUP),
        "a resume fetches only the middle"
    );
    let head = reopened.read(0, GROUP).await.expect("read the held head");
    assert_eq!(
        head.as_ref(),
        &plaintext[..usize::try_from(GROUP).expect("fits")]
    );
    reopened.close().await.expect("close");
}

/// A blob small enough that the file store keeps it in its database exports
/// to its destination like any other.
#[tokio::test]
async fn a_small_blob_exports_to_its_destination() {
    for total in [1u64, 200, 1024, 4000, 20_000] {
        let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
        let dir = tempfile::tempdir().expect("tmp dir");
        let dest = dir.path().join("out.bin");
        let store = ClientRangedStore::on_disk(&dir.path().join("store"), root, total)
            .await
            .expect("open")
            .exporting_to(dest.clone());
        let aligned = decdn_bao_range::align_range(0, total, total).expect("align");
        let bao = bao_for(root, &plaintext, outboard, &aligned);
        store.admit(aligned, bao).await.expect("admit");
        store
            .finalize()
            .await
            .unwrap_or_else(|e| panic!("finalize a {total}-byte blob: {e}"));
        assert_eq!(
            std::fs::read(&dest).expect("read export"),
            plaintext,
            "{total} bytes"
        );
        store.close().await.expect("close");
    }
}

/// A store that holds the whole blob exports it to its destination at
/// `finalize`, byte for byte, and a second `finalize` keeps it.
#[tokio::test]
async fn a_finished_blob_exports_to_its_destination() {
    let total = 3 * GROUP + 7;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
    let dir = tempfile::tempdir().expect("tmp dir");
    let dest = dir.path().join("out.bin");

    let store = ClientRangedStore::on_disk(&dir.path().join("store"), root, total)
        .await
        .expect("open")
        .exporting_to(dest.clone());
    assert!(
        matches!(
            store.finalize().await,
            Err(decdn_bao_range::RangedStoreError::Incomplete)
        ),
        "nothing to export yet"
    );
    let aligned = decdn_bao_range::align_range(0, total, total).expect("align");
    let bao = bao_for(root, &plaintext, outboard, &aligned);
    store.admit(aligned, bao).await.expect("admit");
    assert!(store.is_complete().await.expect("complete"));

    store.finalize().await.expect("finalize");
    assert_eq!(std::fs::read(&dest).expect("read export"), plaintext);
    store.finalize().await.expect("finalize again");
    assert_eq!(std::fs::read(&dest).expect("read export"), plaintext);
    store.close().await.expect("close");
}
