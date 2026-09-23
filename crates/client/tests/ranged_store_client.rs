//! Client-specific tests for `ClientRangedStore` (#1621 P2 Task 4): lock the
//! "trust records, verify once" resume invariant that distinguishes the
//! client backend and isn't covered by the shared conformance suite (see
//! `ranged_store_conformance.rs`) or the `src`-local `admit`/`finalize` unit
//! tests (#1621 P2 Task 2).
//!
//! `ClientRangedStore::open` reconstructs `present` from the persisted
//! `.partial.ranges` record in O(1) — it does NOT re-hash the `.partial` data
//! against the outboard. That's the load-bearing shortcut documented on
//! `ranged_store.rs::open`: the ONE verify pass is `finalize`'s `valid_ranges`
//! sweep, already locked by Task 2's
//! `finalize_shrinks_to_valid_set_on_post_admit_corruption`. These tests prove
//! the other half — that `open` trusts the record rather than re-deriving it,
//! and that resume continues from the persisted frontier without re-listing
//! (and so without re-fetching/re-paying for) the held prefix.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests

use bytes::Bytes;
use decdn_bao_range::{AlignedRange, CHUNK_GROUP_BYTES, IROH_BLOCK_SIZE, RangedStore};
use decdn_client::ClientRangedStore;

const GROUP: u64 = CHUNK_GROUP_BYTES;

/// Deterministic blob of `len` bytes plus its bao root and full pre-order
/// outboard. Mirrors `crates/cache/tests/range_pull.rs::make_blob` /
/// `crates/client/src/ranged_store.rs::tests::synth_blob` — an xorshift
/// fill, not random, so runs are reproducible.
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

#[tokio::test]
async fn resume_trusts_record_without_rehashing() {
    let total = 3 * GROUP;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
    let dir = tempfile::tempdir().expect("tmp dir");

    let k = GROUP; // one full group prefix
    {
        let store = ClientRangedStore::create(dir.path(), "blob", root, total).expect("create");
        let aligned = decdn_bao_range::align_range(0, k, total).expect("align prefix");
        let bao_bytes = bao_for(root, &plaintext, outboard.clone(), &aligned);
        store.admit(aligned, bao_bytes).await.expect("admit prefix");
        // Drop the store, releasing the tempdir's inner handles but not the
        // directory itself (`dir` outlives this block).
    }

    // Corrupt the already-recorded `[0, K)` prefix directly on disk, leaving
    // the `.partial.ranges` record untouched (it still claims `[0, K)`
    // present). If `open` re-hashed the data against the outboard it would
    // discover this and drop the prefix from `present`.
    let partial_path = dir.path().join("blob.partial");
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&partial_path)
            .expect("open partial data file");
        f.seek(SeekFrom::Start(0)).expect("seek");
        let zeros = vec![0u8; usize::try_from(k).expect("fits")];
        f.write_all(&zeros).expect("corrupt prefix");
    }

    let reopened =
        ClientRangedStore::open(dir.path(), "blob", root, total).expect("open after corruption");

    // Presence is unchanged by the on-disk corruption: `open` trusted the
    // record instead of re-deriving it from (now-corrupt) data.
    let present = reopened.present_ranges().await.expect("present_ranges");
    let expected_prefix = decdn_bao_range::align_range(0, k, total)
        .expect("align prefix")
        .chunk_ranges()
        .clone();
    assert_eq!(
        present, expected_prefix,
        "open must trust the persisted record, not re-hash the (now corrupt) data"
    );

    let missing = reopened.missing_ranges(0, 0).await.expect("missing_ranges");
    let expected_missing = decdn_bao_range::align_range(0, 0, total)
        .expect("align whole blob")
        .chunk_ranges()
        .clone()
        - &expected_prefix;
    assert_eq!(missing, expected_missing);

    // The safety net: `finalize`'s one verify pass DOES catch the corruption,
    // once the record claims completeness. Admit the remaining bytes (over
    // the still-corrupt-on-disk prefix) so `is_complete` is true, then let
    // `finalize`'s sweep discover the drift.
    let rest_aligned = decdn_bao_range::align_range(k, total - k, total).expect("align rest");
    let rest_bao = bao_for(root, &plaintext, outboard, &rest_aligned);
    reopened
        .admit(rest_aligned, rest_bao)
        .await
        .expect("admit rest");
    assert!(
        reopened.is_complete().await.expect("is_complete"),
        "record claims every group present"
    );

    let err = reopened
        .finalize()
        .await
        .expect_err("finalize's verify sweep must catch the corrupted prefix");
    assert!(matches!(err, decdn_bao_range::RangedStoreError::Incomplete));
}

#[tokio::test]
async fn resume_continues_from_record_without_refetching_prefix() {
    let total = 3 * GROUP;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
    let dir = tempfile::tempdir().expect("tmp dir");

    let k = GROUP;
    {
        let store = ClientRangedStore::create(dir.path(), "blob", root, total).expect("create");
        let aligned = decdn_bao_range::align_range(0, k, total).expect("align prefix");
        let bao_bytes = bao_for(root, &plaintext, outboard.clone(), &aligned);
        store.admit(aligned, bao_bytes).await.expect("admit prefix");
    }

    let reopened = ClientRangedStore::open(dir.path(), "blob", root, total).expect("open");

    // The held prefix must NOT be re-listed as missing: a resumed fetch
    // driven by `missing_ranges` would skip it entirely, so it's never
    // re-fetched or re-paid for.
    let missing = reopened.missing_ranges(0, 0).await.expect("missing_ranges");
    let expected_missing = decdn_bao_range::align_range(0, 0, total)
        .expect("align whole blob")
        .chunk_ranges()
        .clone()
        - decdn_bao_range::align_range(0, k, total)
            .expect("align prefix")
            .chunk_ranges();
    assert_eq!(
        missing, expected_missing,
        "held prefix must not be re-listed as missing"
    );

    // Resume by admitting only the gap the record reports.
    let rest_aligned = decdn_bao_range::align_range(k, total - k, total).expect("align rest");
    let rest_bao = bao_for(root, &plaintext, outboard, &rest_aligned);
    reopened
        .admit(rest_aligned, rest_bao)
        .await
        .expect("admit rest");

    reopened.finalize().await.expect("finalize promotes");

    let final_path = dir.path().join("blob");
    let on_disk = std::fs::read(&final_path).expect("read promoted final file");
    assert_eq!(
        on_disk, plaintext,
        "promoted file must hold the exact full plaintext"
    );
}
