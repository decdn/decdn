use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

fn tmp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tmp dir")
}

/// A `.ranges` record whose stat fails (here a symlink loop, `ELOOP`) is not
/// read as "no record": `open_or_create` errors instead of creating, which
/// would truncate the `.partial` and the bytes already paid for in it.
#[cfg(unix)]
#[test]
fn open_or_create_does_not_truncate_on_a_failed_record_stat() {
    let dir = tmp_dir();
    let data = dir.path().join("blob.partial");
    std::fs::write(&data, b"paid bytes").expect("write partial");
    let ranges = dir.path().join("blob.partial.ranges");
    std::os::unix::fs::symlink(&ranges, &ranges).expect("self-referential symlink");

    let opened = ClientRangedStore::open_or_create(dir.path(), "blob", [0; 32], 10);
    assert!(opened.is_err(), "a failed stat must not create a store");
    assert_eq!(std::fs::read(&data).expect("read partial"), b"paid bytes");
}

// --- record codec round-trip ---

/// Write `state` as a record and read it back.
fn round_trip(name: &str, state: &StoreState) -> StoreState {
    let dir = tmp_dir();
    let path = dir.path().join(name);
    write_record(&path, state).expect("write");
    read_record(&path).expect("read")
}

#[test]
fn record_round_trip_empty() {
    let state = StoreState::empty(10);
    assert_eq!(round_trip("empty.ranges", &state), state);
}

#[test]
fn record_round_trip_single_range() {
    let mut state = StoreState::empty(9 * 1024);
    state.present = ChunkRanges::from(ChunkNum(2)..ChunkNum(9));
    assert_eq!(round_trip("single.ranges", &state), state);
}

#[test]
fn record_round_trip_disjoint_ranges_and_a_proven_size() {
    let mut state = StoreState::empty(40 * 1024);
    state.present = ChunkRanges::from(ChunkNum(0)..ChunkNum(3));
    state.present |= ChunkRanges::from(ChunkNum(10)..ChunkNum(15));
    state.prove(15 * 1024);
    assert_eq!(round_trip("disjoint.ranges", &state), state);
}

#[test]
fn record_rejects_odd_boundary_count() {
    let dir = tmp_dir();
    let path = dir.path().join("odd.ranges");
    let json = serde_json::json!({ "bound": 10, "proven": null, "boundaries": [1, 2, 3] });
    std::fs::write(&path, serde_json::to_vec(&json).unwrap()).unwrap();
    let err = read_record(&path).expect_err("odd count must be rejected");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

// --- store query methods ---

const GROUP: u64 = decdn_bao_range::CHUNK_GROUP_BYTES;

fn store_with_present(total_bytes: u64, present: ChunkRanges) -> ClientRangedStore {
    let dir = tmp_dir();
    let store =
        ClientRangedStore::create(dir.path(), "blob", [7u8; 32], total_bytes).expect("create");
    store.state.lock().expect("lock").present = present;
    // Keep the tempdir alive for the store's lifetime by leaking it —
    // acceptable in a unit test; the OS reclaims it at process exit.
    std::mem::forget(dir);
    store
}

fn write_plaintext(store: &ClientRangedStore, data: &[u8]) {
    let path = store.data_path.lock().expect("lock").clone();
    std::fs::write(path, data).expect("write plaintext");
}

#[tokio::test]
async fn total_bytes_reports_the_bound() {
    let store = store_with_present(3 * GROUP, ChunkRanges::empty());
    assert_eq!(store.total_bytes(), 3 * GROUP);
    store.set_bound(5 * GROUP);
    assert_eq!(store.total_bytes(), 5 * GROUP);
    assert_eq!(store.proven(), None);
}

/// A proven size is final: moving the bound after a proof does nothing.
#[tokio::test]
async fn set_bound_is_a_no_op_once_a_size_is_proven() {
    let store = store_with_present(3 * GROUP, ChunkRanges::empty());
    store.state.lock().expect("lock").prove(2 * GROUP);
    store.set_bound(9 * GROUP);
    assert_eq!(store.bound(), 2 * GROUP);
    assert_eq!(store.proven(), Some(2 * GROUP));
}

#[tokio::test]
async fn present_ranges_reflects_hand_set_value() {
    let present = ChunkRanges::from(ChunkNum(0)..ChunkNum(16));
    let store = store_with_present(3 * GROUP, present.clone());
    let got = store.present_ranges().await.expect("present_ranges");
    assert_eq!(got, present);
}

#[tokio::test]
async fn missing_ranges_interior_and_prefix() {
    let total = 4 * GROUP;
    // First group present, rest missing.
    let present = ChunkRanges::from(ChunkNum(0)..ChunkNum(16));
    let store = store_with_present(total, present);

    // Prefix: entirely within the present group -> nothing missing.
    let missing_prefix = store.missing_ranges(0, 1024).await.expect("missing prefix");
    assert!(missing_prefix.is_empty());

    // Interior spanning into the missing region -> non-empty.
    let missing_interior = store
        .missing_ranges(GROUP, GROUP)
        .await
        .expect("missing interior");
    assert!(!missing_interior.is_empty());
}

/// Complete means a proven size with every byte of it present.
#[tokio::test]
async fn is_complete_false_then_true() {
    let total = 2 * GROUP;
    let store = store_with_present(total, ChunkRanges::empty());
    assert!(!store.is_complete().await.expect("is_complete false"));

    store.state.lock().expect("lock").present = whole(total);
    assert!(
        !store.is_complete().await.expect("is_complete unproven"),
        "every byte of an unproven bound is not complete"
    );

    store.state.lock().expect("lock").prove(total);
    assert!(store.is_complete().await.expect("is_complete true"));
}

#[tokio::test]
async fn read_byte_exact_of_present_span() {
    let total = 2 * GROUP;
    let present = ChunkRanges::from(ChunkNum(0)..ChunkNum::full_chunks(total));
    let store = store_with_present(total, present);

    let mut data = vec![0u8; usize::try_from(total).expect("fits usize")];
    for (i, b) in data.iter_mut().enumerate() {
        *b = u8::try_from(i % 256).expect("i % 256 fits u8");
    }
    write_plaintext(&store, &data);

    let got = store.read(10, 20).await.expect("read");
    assert_eq!(got.as_ref(), &data[10..30]);
}

#[tokio::test]
async fn missing_ranges_oob_is_alignment_error() {
    let total = GROUP;
    let store = store_with_present(total, ChunkRanges::empty());
    let err = store
        .missing_ranges(total, 1)
        .await
        .expect_err("oob must error");
    assert!(matches!(err, RangedStoreError::Alignment(_)));
}

/// An end past the bound clamps to it, as on the node's store: the bound
/// can shrink under a caller that read it first. Only a start at or past
/// the bound is an alignment error.
#[tokio::test]
async fn an_end_past_the_bound_clamps() {
    let total = 2 * GROUP;
    let store = store_with_present(total, whole(total));
    let data: Vec<u8> = (0..total)
        .map(|i| u8::try_from(i % 251).expect("fits"))
        .collect();
    write_plaintext(&store, &data);

    let missing = store
        .missing_ranges(GROUP, 4 * GROUP)
        .await
        .expect("an end past the bound clamps");
    assert!(missing.is_empty());
    let got = store.read(GROUP, 4 * GROUP).await.expect("read clamps");
    assert_eq!(got.as_ref(), &data[usize::try_from(GROUP).expect("fits")..]);
}

#[tokio::test]
async fn read_oob_is_alignment_error() {
    let total = GROUP;
    let store = store_with_present(total, ChunkRanges::empty());
    let err = store.read(total, 1).await.expect_err("oob must error");
    assert!(matches!(err, RangedStoreError::Alignment(_)));
}

#[tokio::test]
async fn read_absent_span_is_backend_error() {
    let total = 2 * GROUP;
    // Only the second group present; ask for the (absent) first group.
    let present = ChunkRanges::from(ChunkNum::full_chunks(GROUP)..ChunkNum::full_chunks(total));
    let store = store_with_present(total, present);

    let err = store.read(0, 1024).await.expect_err("absent must error");
    assert!(matches!(err, RangedStoreError::Backend(_)));
}

// --- admit / finalize ---

/// Deterministic blob of `len` bytes plus its bao root and full pre-order
/// outboard. Mirrors `crates/bao-range/src/conformance.rs::synth_blob` /
/// `crates/cache/tests/range_pull.rs::make_blob` — an xorshift fill, not
/// random, so runs are reproducible.
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
/// ready to hand to `RangedStore::admit` — exactly what the shared
/// conformance suite's `bao_for_range` produces.
fn bao_for(root: [u8; 32], plaintext: &[u8], outboard: Bytes, aligned: &AlignedRange) -> Bytes {
    let s = usize::try_from(aligned.fetch_start()).expect("fits usize");
    let e = usize::try_from(aligned.fetch_end()).expect("fits usize");
    let data = plaintext.get(s..e).expect("aligned span in bounds");
    decdn_bao_range::encode_verified_range(root, aligned, data, outboard).expect("verifies")
}

/// Fresh `.partial` store for `(root, total_bytes)`, keeping its tempdir
/// alive for the test's lifetime the same way `store_with_present` does.
fn fresh_store(root: [u8; 32], total_bytes: u64) -> ClientRangedStore {
    let dir = tmp_dir();
    let store = ClientRangedStore::create(dir.path(), "blob", root, total_bytes).expect("create");
    std::mem::forget(dir);
    store
}

#[tokio::test]
async fn admit_prefix_range_is_readable_and_present() {
    let total = 3 * GROUP;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
    let store = fresh_store(root, total);

    let aligned = decdn_bao_range::align_range(0, GROUP, total).expect("align");
    let bao_bytes = bao_for(root, &plaintext, outboard, &aligned);

    store
        .admit(aligned.clone(), bao_bytes)
        .await
        .expect("admit prefix");

    let present = store.present_ranges().await.expect("present_ranges");
    assert_eq!(&present, aligned.chunk_ranges());

    let got = store.read(0, GROUP).await.expect("read admitted prefix");
    assert_eq!(
        got.as_ref(),
        plaintext
            .get(..usize::try_from(GROUP).expect("fits"))
            .expect("slice")
    );

    // The `.partial` file holds the bytes at the right offset.
    let on_disk = std::fs::read(store.data_path.lock().expect("lock").clone()).expect("read");
    assert_eq!(
        on_disk.get(..usize::try_from(GROUP).expect("fits")),
        plaintext.get(..usize::try_from(GROUP).expect("fits"))
    );
}

#[tokio::test]
async fn admit_corrupt_payload_is_backend_error_and_presence_unchanged() {
    let total = 2 * GROUP;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
    let store = fresh_store(root, total);

    let aligned = decdn_bao_range::align_range(0, GROUP, total).expect("align");
    let mut bao_bytes = bao_for(root, &plaintext, outboard, &aligned).to_vec();
    // Flip a byte well past the 8-byte header, inside the interleaved
    // proof+data body, so the corruption lands in bao-verified content.
    let flip_at = bao_bytes.len() - 1;
    if let Some(b) = bao_bytes.get_mut(flip_at) {
        *b ^= 0xFF;
    }

    let err = store
        .admit(aligned, Bytes::from(bao_bytes))
        .await
        .expect_err("corrupt payload must fail");
    assert!(matches!(err, RangedStoreError::Backend(_)));

    let present = store.present_ranges().await.expect("present_ranges");
    assert!(present.is_empty());
}

#[tokio::test]
async fn finalize_promotes_when_every_group_admitted() {
    let total = 2 * GROUP + 123;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
    let store = fresh_store(root, total);

    let aligned = decdn_bao_range::align_range(0, 0, total).expect("align whole blob");
    let bao_bytes = bao_for(root, &plaintext, outboard, &aligned);
    store.admit(aligned, bao_bytes).await.expect("admit all");

    store.finalize().await.expect("finalize promotes");

    let final_path = store.data_path.lock().expect("lock").clone();
    assert!(!final_path.to_string_lossy().ends_with(".partial"));
    let on_disk = std::fs::read(&final_path).expect("read final blob");
    assert_eq!(on_disk, plaintext);

    assert!(!store.ranges_path.exists());

    // Post-finalize `read` still works through the moved `data_path`.
    let got = store.read(0, 0).await.expect("read post-finalize");
    assert_eq!(got.as_ref(), plaintext.as_slice());
}

#[tokio::test]
async fn finalize_on_incomplete_store_is_incomplete_error() {
    let total = 2 * GROUP;
    let (root, _plaintext, _outboard) = synth_blob(usize::try_from(total).expect("fits"));
    let store = fresh_store(root, total);

    let err = store.finalize().await.expect_err("incomplete must error");
    assert!(matches!(err, RangedStoreError::Incomplete));
}

#[tokio::test]
async fn open_after_finalize_reconstructs_complete_store() {
    let total = 2 * GROUP + 123;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
    let dir = tmp_dir();
    let store = ClientRangedStore::create(dir.path(), "blob", root, total).expect("create");

    let aligned = decdn_bao_range::align_range(0, 0, total).expect("align whole blob");
    let bao_bytes = bao_for(root, &plaintext, outboard, &aligned);
    store.admit(aligned, bao_bytes).await.expect("admit all");
    store.finalize().await.expect("finalize promotes");
    drop(store);

    let reopened = ClientRangedStore::open(dir.path(), "blob", root).expect("open finalized");

    assert!(
        reopened.is_complete().await.expect("is_complete"),
        "reopened store must be complete"
    );
    assert_eq!(
        reopened.proven(),
        Some(total),
        "the final file proves its length"
    );
    assert_eq!(reopened.bound(), total);
    let got = reopened.read(0, total).await.expect("read");
    assert_eq!(got.as_ref(), plaintext.as_slice());
    let missing = reopened.missing_ranges(0, 0).await.expect("missing_ranges");
    assert!(missing.is_empty());
}

#[tokio::test]
async fn open_recovers_from_crash_between_rename_and_sidecar_delete() {
    let total = 2 * GROUP + 123;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
    let dir = tmp_dir();
    let store = ClientRangedStore::create(dir.path(), "blob", root, total).expect("create");

    let aligned = decdn_bao_range::align_range(0, 0, total).expect("align whole blob");
    let bao_bytes = bao_for(root, &plaintext, outboard, &aligned);
    store.admit(aligned, bao_bytes).await.expect("admit all");

    // Simulate the crash window: rename `.partial` -> final WITHOUT
    // deleting the record, mirroring a crash between finalize's rename
    // and its best-effort record cleanup.
    let partial_path = store.data_path.lock().expect("lock").clone();
    let final_path = dir.path().join("blob");
    std::fs::rename(&partial_path, &final_path).expect("simulate promote rename");
    let ranges_path = store.ranges_path.clone();
    assert!(ranges_path.exists());
    drop(store);

    let reopened = ClientRangedStore::open(dir.path(), "blob", root).expect("open post-crash");

    assert!(
        reopened.is_complete().await.expect("is_complete"),
        "reopened store must be complete"
    );
    let got = reopened.read(0, total).await.expect("read");
    assert_eq!(got.as_ref(), plaintext.as_slice());

    assert!(
        !ranges_path.exists(),
        "leftover ranges sidecar must be cleaned up"
    );
}

#[tokio::test]
async fn finalize_is_idempotent_on_reopened_finalized_store() {
    let total = 2 * GROUP + 123;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total).expect("fits"));
    let dir = tmp_dir();
    let store = ClientRangedStore::create(dir.path(), "blob", root, total).expect("create");

    let aligned = decdn_bao_range::align_range(0, 0, total).expect("align whole blob");
    let bao_bytes = bao_for(root, &plaintext, outboard, &aligned);
    store.admit(aligned, bao_bytes).await.expect("admit all");
    store.finalize().await.expect("finalize promotes");
    drop(store);

    let reopened = ClientRangedStore::open(dir.path(), "blob", root).expect("open finalized");

    // A second `finalize` on the promoted file is a no-op.
    reopened
        .finalize()
        .await
        .expect("finalize on already-finalized store is a no-op Ok");

    let got = reopened.read(0, total).await.expect("read still works");
    assert_eq!(got.as_ref(), plaintext.as_slice());
}

// --- seed_checkpointed_prefix (test-util fixture seam) ---

#[tokio::test]
async fn seed_prefix_resumes_from_the_recorded_prefix() {
    let total = 3 * GROUP + 123;
    let (root, plaintext, _outboard) = synth_blob(usize::try_from(total).expect("fits"));
    let dir = tmp_dir();
    let seeded = 2 * GROUP;

    ClientRangedStore::seed_checkpointed_prefix(dir.path(), "blob", &plaintext, seeded)
        .expect("seed prefix");
    let store = ClientRangedStore::open(dir.path(), "blob", root).expect("open seeded");
    assert_eq!(store.bound(), total);
    assert_eq!(
        store.proven(),
        None,
        "a prefix without the final chunk proves nothing"
    );

    // Present is exactly the aligned recorded prefix; the suffix is missing.
    let aligned = decdn_bao_range::align_range(0, seeded, total).expect("align prefix");
    let present = store.present_ranges().await.expect("present_ranges");
    assert_eq!(&present, aligned.chunk_ranges());

    let full = decdn_bao_range::align_range(0, 0, total).expect("align whole");
    let expected_missing = full.chunk_ranges().clone() - aligned.chunk_ranges();
    let missing = store.missing_ranges(0, 0).await.expect("missing_ranges");
    assert_eq!(missing, expected_missing);
    assert!(!store.is_complete().await.expect("is_complete"));

    // The recorded prefix is byte-exact readable off the `.partial`.
    let got = store.read(0, seeded).await.expect("read prefix");
    assert_eq!(
        got.as_ref(),
        plaintext
            .get(..usize::try_from(seeded).expect("fits"))
            .expect("slice")
    );
}

#[tokio::test]
async fn seed_complete_prefix_is_complete_and_finalizes() {
    // An exact-multiple-of-group blob seeded COMPLETE (journey-5 shape): the
    // record claims the whole blob, so the store is complete on open and
    // `finalize` promotes with no further pulls.
    let total = 4 * GROUP;
    let (root, plaintext, _outboard) = synth_blob(usize::try_from(total).expect("fits"));
    let dir = tmp_dir();

    ClientRangedStore::seed_checkpointed_prefix(dir.path(), "blob", &plaintext, total)
        .expect("seed complete");
    let store = ClientRangedStore::open(dir.path(), "blob", root).expect("open seeded");

    assert_eq!(
        store.proven(),
        Some(total),
        "a complete seed proves its length"
    );
    assert!(
        store.is_complete().await.expect("is_complete"),
        "a complete seed must open complete"
    );
    assert!(
        store
            .missing_ranges(0, 0)
            .await
            .expect("missing_ranges")
            .is_empty()
    );

    // The seeded data hashes to the root, so `finalize` promotes.
    store
        .finalize()
        .await
        .expect("finalize promotes seeded blob");
    let final_path = store.data_path.lock().expect("lock").clone();
    assert!(!final_path.to_string_lossy().ends_with(".partial"));
    let on_disk = std::fs::read(&final_path).expect("read promoted blob");
    assert_eq!(on_disk, plaintext);
}

// --- ingest_stream ---

/// Total bytes covered by a [`ChunkRanges`], reconstructed from its
/// boundary pairs the same way [`write_ranges_record`] encodes them
/// (`ChunkNum` counts 1 KiB chunks).
fn ranges_byte_len(ranges: &ChunkRanges) -> u64 {
    let boundaries = ranges.boundaries();
    let mut sum = 0u64;
    let mut it = boundaries.iter();
    while let (Some(a), Some(b)) = (it.next(), it.next()) {
        sum += (b.0 - a.0) * 1024;
    }
    sum
}

/// An ingest whose end is lowered stops once its verified prefix reaches
/// it: the prefix is present, nothing past it is, and the wire it read is
/// exactly the encoding of the shorter range, since a group-aligned end
/// cuts a pre-order bao encoding between two items. An end at or past the
/// range's end drains the range as usual.
#[tokio::test]
async fn ingest_stream_until_stops_at_a_lowered_end() -> anyhow::Result<()> {
    let total = 11 * GROUP + 321;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);
    let range = decdn_bao_range::align_range(GROUP, 9 * GROUP, total)?;
    let body = bao_for(root, &plaintext, outboard, &range).slice(8..);
    for stop in 2..10 {
        let store = fresh_store(root, total);
        let end = AtomicU64::new(stop * GROUP);
        let (rest, ended) = store
            .ingest_stream_until(&range, body.clone(), None, total, Some(&end))
            .await?;
        let kept = decdn_bao_range::align_range(GROUP, (stop - 1) * GROUP, total)?;
        assert_eq!(ended, IngestEnd::Stopped, "stop at group {stop}");
        assert_eq!(
            &store.present_ranges().await?,
            kept.chunk_ranges(),
            "stop at group {stop}"
        );
        assert_eq!(
            (body.len() - rest.len()) as u64,
            kept.wire_len(),
            "the wire read up to group {stop} is the shorter range's encoding"
        );
    }

    let store = fresh_store(root, total);
    let end = AtomicU64::new(10 * GROUP);
    let (rest, ended) = store
        .ingest_stream_until(&range, body.clone(), None, total, Some(&end))
        .await?;
    assert_eq!(
        ended,
        IngestEnd::Drained,
        "an end at the range's end drains"
    );
    assert!(rest.is_empty());
    assert_eq!(&store.present_ranges().await?, range.chunk_ranges());
    Ok(())
}

#[tokio::test]
async fn ingest_stream_writes_positioned_and_reflects_presence() -> anyhow::Result<()> {
    let total = 3 * GROUP + 123;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);
    let store = fresh_store(root, total);

    // A prefix gap: the first two groups only.
    let aligned = decdn_bao_range::align_range(0, 2 * GROUP, total)?;
    let bao_bytes = bao_for(root, &plaintext, outboard.clone(), &aligned);
    let body = bao_bytes.slice(8..);
    let reader = store.ingest_stream(&aligned, body, None, total).await?;
    assert_eq!(store.proven(), None, "a prefix proves no size");
    drop(reader);

    let present = store.present_ranges().await?;
    assert_eq!(&present, aligned.chunk_ranges());

    let got = store.read(0, 2 * GROUP).await?;
    assert_eq!(
        got.as_ref(),
        plaintext.get(..usize::try_from(2 * GROUP)?).expect("slice")
    );

    // The `.partial` file holds the bytes at the right (positioned)
    // offset, not appended.
    let on_disk = std::fs::read(store.data_path.lock().expect("lock").clone())?;
    assert_eq!(
        on_disk.get(..usize::try_from(2 * GROUP)?),
        plaintext.get(..usize::try_from(2 * GROUP)?)
    );

    // Ingest the remaining tail, then finalize promotes.
    let rest_aligned = decdn_bao_range::align_range(2 * GROUP, 0, total)?;
    let rest_bao = bao_for(root, &plaintext, outboard, &rest_aligned);
    let rest_body = rest_bao.slice(8..);
    let reader = store
        .ingest_stream(&rest_aligned, rest_body, None, total)
        .await?;
    assert_eq!(store.proven(), Some(total), "the tail proves the size");
    drop(reader);
    store.finalize().await.expect("finalize promotes");
    let final_bytes = store.read(0, total).await?;
    assert_eq!(final_bytes.as_ref(), plaintext.as_slice());

    Ok(())
}

/// A fault after a range's first parents and before its first leaf marks
/// nothing present. No leaf verified, so the received prefix is empty, not
/// the rest of the blob.
#[tokio::test]
async fn ingest_stream_fault_before_the_first_leaf_marks_nothing_present() -> anyhow::Result<()> {
    let total: u64 = 32 * GROUP;
    let plaintext: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
    // The wire of a range deep in the tree opens with its parents: 64 bytes
    // is one parent pair and no leaf.
    let source = crate::source::ScriptedSource::new(plaintext)?
        .with_fault_after(64, || anyhow::anyhow!("scripted reset"));
    let root = source.root();
    let store_dir = tmp_dir();
    let store = ClientRangedStore::create(store_dir.path(), "blob", root, total)?;
    let aligned = decdn_bao_range::align_range(4 * GROUP, 4 * GROUP, total)?;
    let (_header, reader) = {
        use crate::source::BlobSource;
        source.open(root, aligned.clone()).await?
    };

    let err = store
        .ingest_stream(&aligned, reader, None, total)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("the fault must surface as an error"))?;
    assert!(format!("{err:#}").contains("scripted reset"), "{err:#}");
    assert!(
        store.present_ranges().await?.is_empty(),
        "no leaf landed, so nothing is present"
    );
    assert!(!store.is_complete().await?);
    Ok(())
}

#[tokio::test]
async fn ingest_stream_mid_gap_fault_checkpoints_received_prefix() -> anyhow::Result<()> {
    // Big enough to cross at least one 4 MiB checkpoint interval before
    // the scripted fault lands.
    let total: u64 = 8 * 1024 * 1024;
    let plaintext = {
        let mut v = vec![0u8; usize::try_from(total)?];
        let mut x: u32 = 0x1234_5678;
        for b in &mut v {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *b = x.to_le_bytes().first().copied().unwrap_or(0);
        }
        v
    };
    let source = crate::source::ScriptedSource::new(plaintext.clone())?
        .with_fault_after(5 * 1024 * 1024, || {
            anyhow::anyhow!("scripted mid-gap stall")
        });
    let root = source.root();
    let store_dir = tmp_dir();
    let mut store = ClientRangedStore::create(store_dir.path(), "blob", root, total)?;
    // Hold the 4 MiB checkpoint's fsync past the fault at 5 MiB, so the
    // checkpointed prefix below exists only if the fault path waits for it.
    store.fsync_delay = Duration::from_millis(300);

    let aligned = decdn_bao_range::align_range(0, 0, total)?;
    let (_header, reader) = {
        use crate::source::BlobSource;
        source.open(root, aligned.clone()).await?
    };

    let err = store
        .ingest_stream(&aligned, reader, None, total)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("mid-gap fault must surface as an error"))?;
    assert!(
        err.to_string().contains("scripted mid-gap stall")
            || format!("{err:?}").contains("scripted mid-gap stall"),
        "the parked typed fault must survive: {err}"
    );

    // Checkpoints do not persist the `.ranges` record themselves (the
    // single-writer flush point): a real caller reaches this
    // via `drive`'s post-gap-loop flush, but this test drives
    // `ingest_stream` directly, so it flushes explicitly here before
    // simulating the resumed process re-opening the store.
    store.flush_present_record()?;

    // Re-open the store fresh (simulating a resumed process) and inspect
    // the persisted record: it must reflect the checkpointed prefix —
    // more than one checkpoint interval's worth (proving the fault path
    // checkpointed its verified batch on top of the interval checkpoint),
    // but strictly less than the whole gap (proving the bytes past the
    // fault were NOT claimed).
    let reopened = ClientRangedStore::open(store_dir.path(), "blob", root)?;
    let present = reopened.present_ranges().await?;
    let present_bytes = ranges_byte_len(&present);

    assert!(
        present_bytes > 0,
        "a mid-gap fault must not lose the whole gap: present is empty"
    );
    assert!(
        present_bytes > ClientRangedStore::INGEST_CHECKPOINT_BYTES,
        "the fault must checkpoint the verified batch past the first interval too, \
         got {present_bytes} bytes"
    );
    assert!(
        present_bytes < total,
        "the un-checkpointed tail past the fault must not be claimed as present, \
         got {present_bytes} of {total} bytes"
    );

    // And the checkpointed prefix is genuinely readable/verified data,
    // not garbage — a byte-exact prefix of the plaintext.
    let checkpointed = reopened
        .read(0, present_bytes)
        .await
        .expect("checkpointed prefix must be readable");
    assert_eq!(
        checkpointed.as_ref(),
        plaintext
            .get(..usize::try_from(present_bytes)?)
            .expect("slice")
    );

    Ok(())
}

/// Deterministic `len`-byte blob (xorshift fill, mirrors `synth_blob`'s
/// plaintext generation) — used by the concurrent-checkpoint test to build
/// a fixed-content blob before splitting it into disjoint ranges.
fn blob(len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x: u32 = 0x2545_f491;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

/// Thin wrapper over `PreOrderMemOutboard::create`: the bao root and full
/// pre-order outboard for an already-built blob, for tests that construct
/// `data` themselves rather than through `synth_blob`.
fn bao_root_and_outboard(data: &[u8]) -> ([u8; 32], Bytes) {
    let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(data, IROH_BLOCK_SIZE);
    (*ob.root.as_bytes(), Bytes::from(ob.data))
}

/// The header-less bao wire (content + interleaved proof) for `range` of
/// `data`, ready to hand to [`ClientRangedStore::ingest_stream`] as a
/// `Bytes` reader — a thin wrapper over `encode_verified_range`, mirroring
/// `ScriptedSource::wire_for` (`crate::source`).
fn scripted_reader_for(data: &[u8], range: &AlignedRange) -> anyhow::Result<Bytes> {
    let (root, outboard) = bao_root_and_outboard(data);
    let s = usize::try_from(range.fetch_start())?;
    let e = usize::try_from(range.fetch_end())?;
    let slice = data
        .get(s..e)
        .ok_or_else(|| anyhow::anyhow!("scripted range out of bounds"))?;
    let combined = decdn_bao_range::encode_verified_range(root, range, slice, outboard)?;
    let wire = combined
        .get(8..)
        .ok_or_else(|| anyhow::anyhow!("combined wire shorter than its 8-byte header"))?;
    Ok(Bytes::copy_from_slice(wire))
}

#[tokio::test]
async fn concurrent_ingest_present_record_never_regresses() -> anyhow::Result<()> {
    // Two disjoint bao-aligned ranges of one blob, ingested concurrently, then
    // flushed. The persisted .ranges must equal the union of both ranges.
    let dir = tempfile::tempdir()?;
    let data = blob(8 * 1024 * 1024); // 8 MiB -> two 4 MiB halves, group-aligned
    let (root, _) = bao_root_and_outboard(&data);
    let store = ClientRangedStore::create(dir.path(), "b", root, data.len() as u64)?;

    let lo = decdn_bao_range::align_range(0, 4 * 1024 * 1024, data.len() as u64)?;
    let hi = decdn_bao_range::align_range(4 * 1024 * 1024, 4 * 1024 * 1024, data.len() as u64)?;

    let total = data.len() as u64;
    let a = store.ingest_stream(&lo, scripted_reader_for(&data, &lo)?, None, total);
    let b = store.ingest_stream(&hi, scripted_reader_for(&data, &hi)?, None, total);
    let (ra, rb) = tokio::join!(a, b);
    ra?;
    rb?;

    store.flush_present_record()?;

    let on_disk = read_record(store.ranges_path())?;
    let expected = lo.chunk_ranges().clone() | hi.chunk_ranges().clone();
    assert_eq!(on_disk.present, expected);
    assert_eq!(on_disk.proven, Some(total), "the high half holds the tail");
    Ok(())
}

#[tokio::test]
async fn ingest_stream_corrupt_bao_is_error_not_written() -> anyhow::Result<()> {
    let total = 2 * GROUP;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);
    let store = fresh_store(root, total);

    let aligned = decdn_bao_range::align_range(0, 0, total)?;
    let mut bao_bytes = bao_for(root, &plaintext, outboard, &aligned).to_vec();
    // Flip a byte well past the 8-byte header, inside the interleaved
    // proof+data body.
    let flip_at = bao_bytes.len() - 1;
    if let Some(b) = bao_bytes.get_mut(flip_at) {
        *b ^= 0xFF;
    }
    let body = Bytes::from(bao_bytes).slice(8..);

    let err = store
        .ingest_stream(&aligned, body, None, total)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("corrupt payload must fail"))?;
    assert!(
        err.downcast_ref::<crate::HashMismatch>().is_some(),
        "corruption must surface as the typed HashMismatch, got: {err}"
    );

    // The flipped byte sits in the second group's leaf, so the first
    // group verified before the fault. The fault path checkpoints that
    // verified prefix and nothing past it: the corrupt group is never
    // claimed, and the claimed group reads back byte-exact.
    let first_group = decdn_bao_range::align_range(0, GROUP, total)?;
    let present = store.present_ranges().await?;
    assert_eq!(
        &present,
        first_group.chunk_ranges(),
        "presence must be exactly the verified prefix before the corrupt group"
    );
    let got = store.read(0, GROUP).await?;
    assert_eq!(
        got.as_ref(),
        plaintext.get(..usize::try_from(GROUP)?).expect("slice")
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn ingest_stream_slow_flush_does_not_block_the_runtime() -> anyhow::Result<()> {
    // The paid pull pays from its read/decode loop, so a worker thread
    // parked in a checkpoint fsync stops voucher sends on every lane that
    // shares the runtime (#2117). With a single worker, a stalled flush
    // must still leave that worker free to run other tasks.
    const FLUSH_DELAY: Duration = Duration::from_secs(3);
    const MAX_GAP: Duration = Duration::from_millis(1500);

    let dir = tempfile::tempdir()?;
    let data = blob(8 * 1024 * 1024); // two checkpoint intervals
    let total = data.len() as u64;
    let (root, _) = bao_root_and_outboard(&data);
    let mut store = ClientRangedStore::create(dir.path(), "b", root, total)?;
    store.fsync_delay = FLUSH_DELAY;
    let store = Arc::new(store);
    let aligned = decdn_bao_range::align_range(0, 0, total)?;
    let wire = scripted_reader_for(&data, &aligned)?;

    // Largest gap, in ms, between consecutive wakeups of a 20 ms ticker
    // sharing the one worker with the ingest.
    let max_gap_ms = Arc::new(AtomicU64::new(0));
    let ticker = tokio::spawn({
        let max_gap_ms = Arc::clone(&max_gap_ms);
        async move {
            let mut last = std::time::Instant::now();
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let now = std::time::Instant::now();
                let gap = u64::try_from(now.duration_since(last).as_millis()).unwrap_or(u64::MAX);
                max_gap_ms.fetch_max(gap, Ordering::Relaxed);
                last = now;
            }
        }
    });
    let ingest = tokio::spawn({
        let store = Arc::clone(&store);
        async move {
            store
                .ingest_stream(&aligned, wire, None, total)
                .await
                .map(drop)
        }
    });

    ingest.await??;
    // Let the ticker wake once more, so a stall at the very end is recorded.
    tokio::time::sleep(Duration::from_millis(100)).await;
    ticker.abort();
    let max_gap = Duration::from_millis(max_gap_ms.load(Ordering::Relaxed));
    assert!(
        max_gap < MAX_GAP,
        "a slow flush blocked the runtime worker for {max_gap:?}"
    );

    let present = store.present_ranges().await?;
    assert_eq!(
        &present,
        decdn_bao_range::align_range(0, 0, total)?.chunk_ranges()
    );
    Ok(())
}

/// A store over `dir` for `data`, with every ingest fsync stalled by
/// `fsync_delay`, plus the header-less wire of the whole blob.
fn slow_fsync_store(
    dir: &Path,
    data: &[u8],
    fsync_delay: Duration,
) -> anyhow::Result<(ClientRangedStore, AlignedRange, Bytes)> {
    let total = u64::try_from(data.len())?;
    let (root, _) = bao_root_and_outboard(data);
    let mut store = ClientRangedStore::create(dir, "b", root, total)?;
    store.fsync_delay = fsync_delay;
    let aligned = decdn_bao_range::align_range(0, 0, total)?;
    let wire = scripted_reader_for(data, &aligned)?;
    Ok((store, aligned, wire))
}

/// `INGEST_CHECKPOINT_BYTES` times `n`, as a `usize` blob length.
fn checkpoints(n: u64) -> anyhow::Result<usize> {
    Ok(usize::try_from(
        n * ClientRangedStore::INGEST_CHECKPOINT_BYTES,
    )?)
}

/// The decode loop, and so the payment it drives, runs past a checkpoint
/// whose fsync stalls: the first four checkpoints of the ramp fit the
/// pipeline slots plus the batch in progress, so the whole blob is
/// received before the first fsync lands. `present` extends only after
/// the fsync.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_stream_decodes_past_a_slow_fsync() -> anyhow::Result<()> {
    const FSYNC_DELAY: Duration = Duration::from_secs(3);

    let dir = tempfile::tempdir()?;
    let slots = ClientRangedStore::INGEST_MAX_QUEUED_CHECKPOINTS;
    let data = blob(usize::try_from(ramp_sum(slots + 1))?);
    let total = u64::try_from(data.len())?;
    let (store, aligned, wire) = slow_fsync_store(dir.path(), &data, FSYNC_DELAY)?;

    // When progress first reached `total`: the elapsed time, and whether
    // `present` was still empty then.
    let at_total: Mutex<Option<(Duration, bool)>> = Mutex::new(None);
    let started = std::time::Instant::now();
    let on_progress = |received: u64| {
        if received == total {
            let empty = store.state.lock().is_ok_and(|s| s.present.is_empty());
            if let Ok(mut slot) = at_total.lock() {
                slot.get_or_insert((started.elapsed(), empty));
            }
        }
    };
    store
        .ingest_stream(&aligned, wire, Some(&on_progress), total)
        .await?;

    let (elapsed, empty) = at_total
        .lock()
        .map_err(|_| anyhow::anyhow!("progress lock poisoned"))?
        .ok_or_else(|| anyhow::anyhow!("progress never reached the total"))?;
    assert!(
        elapsed < FSYNC_DELAY,
        "the decode loop waited for a slow fsync: whole blob received after {elapsed:?}"
    );
    assert!(
        empty,
        "present must not extend before the first fsync lands"
    );
    assert_eq!(&store.present_ranges().await?, aligned.chunk_ranges());
    Ok(())
}

/// The bytes received but not yet durable never exceed the pipeline
/// slots plus the batch the loop is building:
/// `(INGEST_MAX_QUEUED_CHECKPOINTS + 1) * INGEST_CHECKPOINT_BYTES`. That
/// is the crash and dropped-future re-pay bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_stream_bounds_undurable_bytes() -> anyhow::Result<()> {
    let slots = u64::try_from(ClientRangedStore::INGEST_MAX_QUEUED_CHECKPOINTS)?;
    let bound = (slots + 1) * ClientRangedStore::INGEST_CHECKPOINT_BYTES;

    let dir = tempfile::tempdir()?;
    let data = blob(checkpoints(slots + 4)?);
    let (store, aligned, wire) = slow_fsync_store(dir.path(), &data, Duration::from_secs(1))?;

    let max_undurable = AtomicU64::new(0);
    let on_progress = |received: u64| {
        let durable = store
            .state
            .lock()
            .map_or(0, |s| ranges_byte_len(&s.present));
        max_undurable.fetch_max(received.saturating_sub(durable), Ordering::Relaxed);
    };
    store
        .ingest_stream(&aligned, wire, Some(&on_progress), aligned.blob_size())
        .await?;

    let max_undurable = max_undurable.load(Ordering::Relaxed);
    assert!(
        max_undurable <= bound,
        "{max_undurable} bytes were received but not durable, over the {bound}-byte bound"
    );
    // The loop did run ahead of the stalled fsyncs, so the bound was
    // exercised rather than trivially met.
    assert!(
        max_undurable > 2 * ClientRangedStore::INGEST_CHECKPOINT_BYTES,
        "the loop never ran ahead of the disk: max undurable {max_undurable} bytes"
    );
    assert_eq!(&store.present_ranges().await?, aligned.chunk_ranges());
    Ok(())
}

/// Checkpoints that queue behind a slow fsync share the next fsync. The
/// first three checkpoints of the ramp fit the pipeline slots, so the
/// loop queues all three without waiting. The first fsync holds at least
/// the first one; the rest queue during its stall and the worker folds
/// them into one second fsync. One fsync per checkpoint would be three.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_stream_coalesces_queued_checkpoints() -> anyhow::Result<()> {
    let slots = ClientRangedStore::INGEST_MAX_QUEUED_CHECKPOINTS;

    let dir = tempfile::tempdir()?;
    let data = blob(usize::try_from(ramp_sum(slots))?);
    let (store, aligned, wire) = slow_fsync_store(dir.path(), &data, Duration::from_secs(1))?;

    store
        .ingest_stream(&aligned, wire, None, aligned.blob_size())
        .await?;

    let fsyncs = store.fsyncs.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        (1..=2).contains(&fsyncs),
        "{slots} queued checkpoints took {fsyncs} fsyncs, want at most 2"
    );
    assert_eq!(&store.present_ranges().await?, aligned.chunk_ranges());
    Ok(())
}

/// The batch size of each checkpoint of one call, in order, up to `n` of
/// them: the ramp from `INGEST_FIRST_CHECKPOINT_BYTES` to
/// `INGEST_CHECKPOINT_BYTES`.
fn ramp(n: usize) -> Vec<u64> {
    std::iter::successors(
        Some(ClientRangedStore::INGEST_FIRST_CHECKPOINT_BYTES),
        |&b| Some(ClientRangedStore::next_checkpoint_bytes(b)),
    )
    .take(n)
    .collect()
}

/// The sum of the first `n` ramp steps.
fn ramp_sum(n: usize) -> u64 {
    ramp(n).iter().sum()
}

/// Each call's checkpoints start small and double up to
/// `INGEST_CHECKPOINT_BYTES`, so the first verified bytes are durable,
/// and readable, after one small batch rather than a full interval.
#[tokio::test]
async fn ingest_stream_ramps_its_checkpoints() -> anyhow::Result<()> {
    let sizes = ramp(8);
    assert_eq!(sizes.first(), Some(&(64 * KIB)));
    assert_eq!(
        sizes.last(),
        Some(&ClientRangedStore::INGEST_CHECKPOINT_BYTES)
    );

    // Five ramp steps, then a tail shorter than the sixth.
    let total = ramp_sum(5) + 1024 * KIB;
    let data = blob(usize::try_from(total)?);
    let (root, _) = bao_root_and_outboard(&data);
    let dir = tempfile::tempdir()?;
    let store = ClientRangedStore::create(dir.path(), "b", root, total)?;
    let aligned = decdn_bao_range::align_range(0, 0, total)?;
    let wire = scripted_reader_for(&data, &aligned)?;
    store.ingest_stream(&aligned, wire, None, total).await?;

    let mut want: Vec<u64> = (1..=5).map(ramp_sum).collect();
    want.push(total);
    let ends = store
        .checkpoint_ends
        .lock()
        .map_err(|_| anyhow::anyhow!("lock poisoned"))?
        .clone();
    assert_eq!(ends, want);
    Ok(())
}

/// A waiter on `present_grew` wakes when a checkpoint extends `present`,
/// with no other signal.
#[tokio::test]
async fn a_checkpoint_wakes_present_waiters() -> anyhow::Result<()> {
    let total = 256 * KIB;
    let data = blob(usize::try_from(total)?);
    let (root, _) = bao_root_and_outboard(&data);
    let dir = tempfile::tempdir()?;
    let store = ClientRangedStore::create(dir.path(), "b", root, total)?;
    let aligned = decdn_bao_range::align_range(0, 0, total)?;
    let wire = scripted_reader_for(&data, &aligned)?;

    let grew = store.present_grew().notified();
    tokio::pin!(grew);
    grew.as_mut().enable();
    store.ingest_stream(&aligned, wire, None, total).await?;
    tokio::time::timeout(Duration::from_secs(1), grew)
        .await
        .map_err(|_| anyhow::anyhow!("no wakeup when present grew"))?;
    Ok(())
}

// --- offset-keyed store: each leg verifies under its own claim ---

const KIB: u64 = 1024;

/// Open `source` for `range`: the size its header signs, and the reader of
/// its wire.
async fn open_leg(
    source: &crate::source::ScriptedSource,
    range: &AlignedRange,
) -> anyhow::Result<(u64, crate::source::ScriptedReader)> {
    use crate::source::BlobSource;
    let (header, reader) = source.open(source.root(), range.clone()).await?;
    Ok((header.total_bytes, reader))
}

/// Mark `present` as verified, bypassing ingest.
fn set_present(store: &ClientRangedStore, present: ChunkRanges) {
    store.state.lock().expect("lock").present = present;
}

/// A hint below the true size: the leg that covers the true tail verifies
/// under its own claim, proves that size, and moves the bound to it.
#[tokio::test]
async fn a_leg_proves_its_claimed_size_and_sets_the_bound() -> anyhow::Result<()> {
    let truth = 1200 * KIB;
    let source = crate::source::ScriptedSource::new(blob(usize::try_from(truth)?))?;
    let dir = tmp_dir();
    let store = ClientRangedStore::create(dir.path(), "blob", source.root(), 1000 * KIB)?;
    assert_eq!(store.bound(), 1000 * KIB);
    assert_eq!(store.proven(), None);

    let tail = decdn_bao_range::align_range(1000 * KIB, 200 * KIB, truth)?;
    let (claim, reader) = open_leg(&source, &tail).await?;
    assert_eq!(claim, truth);
    store.ingest_stream(&tail, reader, None, claim).await?;

    assert_eq!(store.proven(), Some(truth));
    assert_eq!(store.bound(), truth);
    assert_eq!(store.total_bytes(), truth);
    assert_eq!(&store.present_ranges().await?, tail.chunk_ranges());
    Ok(())
}

/// A hint above the true size: the planner aligns the leg under its own
/// bound, the node serves up to the blob's end, and the leg verifies under
/// the node's claim. It lands genuine bytes and proves the smaller size.
#[tokio::test]
async fn a_leg_under_a_different_claim_lands_genuine_bytes() -> anyhow::Result<()> {
    let truth = 1024 * KIB;
    let data = blob(usize::try_from(truth)?);
    let source = crate::source::ScriptedSource::new(data.clone())?;
    let dir = tmp_dir();
    let store = ClientRangedStore::create(dir.path(), "blob", source.root(), 2048 * KIB)?;

    let leg = decdn_bao_range::align_range(0, truth, 2048 * KIB)?;
    let (claim, reader) = open_leg(&source, &leg).await?;
    assert_eq!(claim, truth);
    store.ingest_stream(&leg, reader, None, claim).await?;

    assert_eq!(store.proven(), Some(truth));
    assert_eq!(store.bound(), truth);
    assert!(store.is_complete().await?);
    assert_eq!(store.read(0, 0).await?.as_ref(), data.as_slice());
    Ok(())
}

/// A node that signs one byte more than the blob holds: its tail leg cannot
/// verify under that claim, so nothing lands and nothing is proven.
#[tokio::test]
async fn a_lying_claim_on_the_final_chunk_is_rejected() -> anyhow::Result<()> {
    let truth = 3 * GROUP + 100;
    let source =
        crate::source::ScriptedSource::new(blob(usize::try_from(truth)?))?.signing_size(truth + 1);
    let store = fresh_store(source.root(), truth);

    let tail = decdn_bao_range::align_range(3 * GROUP, 0, truth)?;
    let (claim, reader) = open_leg(&source, &tail).await?;
    assert_eq!(claim, truth + 1);
    let result = store.ingest_stream(&tail, reader, None, claim).await;

    assert!(result.is_err(), "a lying final-chunk claim must not verify");
    assert!(store.present_ranges().await?.is_empty());
    assert_eq!(store.proven(), None);
    assert_eq!(store.bound(), truth);
    Ok(())
}

/// A resumed store keeps the bound its record holds, whatever the caller
/// now hints, and discards none of the partial.
#[tokio::test]
async fn the_record_wins_over_a_new_hint() -> anyhow::Result<()> {
    let data = blob(40_000);
    let (root, _) = bao_root_and_outboard(&data);
    let dir = tmp_dir();
    ClientRangedStore::seed_checkpointed_prefix(dir.path(), "blob", &data, GROUP)?;
    let partial = dir.path().join("blob.partial");
    let before = std::fs::read(&partial)?;

    let store = ClientRangedStore::open_or_create(dir.path(), "blob", root, 80_000)?;

    assert_eq!(store.bound(), 40_000);
    assert_eq!(store.proven(), None);
    assert_eq!(
        &store.present_ranges().await?,
        decdn_bao_range::align_range(0, GROUP, 40_000)?.chunk_ranges()
    );
    assert_eq!(std::fs::read(&partial)?, before, "nothing is discarded");
    Ok(())
}

/// Every byte of the bound present is not enough: `finalize` needs a
/// proven size, and keeps the partial without one.
#[tokio::test]
async fn finalize_requires_a_proven_size() -> anyhow::Result<()> {
    let total = 2 * GROUP;
    let data = blob(usize::try_from(total)?);
    let (root, _) = bao_root_and_outboard(&data);
    let store = fresh_store(root, total);
    write_plaintext(&store, &data);
    set_present(
        &store,
        ChunkRanges::from(ChunkNum(0)..ChunkNum::full_chunks(total)),
    );

    assert!(!store.is_complete().await?);
    let err = store
        .finalize()
        .await
        .expect_err("an unproven size must not finalize");
    assert!(matches!(err, RangedStoreError::Incomplete), "{err:?}");
    let data_path = store.data_path.lock().expect("lock").clone();
    assert!(data_path.to_string_lossy().ends_with(".partial"));
    assert!(data_path.exists());
    Ok(())
}

/// `finalize` hashes the whole file against the root: a byte that changed
/// on disk after ingest fails it. It keeps the partial file and the bound
/// but claims nothing present, so the next ingest fetches over the bad
/// bytes, and `finalize` then promotes.
#[tokio::test]
async fn finalize_hashes_the_whole_file() -> anyhow::Result<()> {
    let total = 2 * GROUP + 123;
    let data = blob(usize::try_from(total)?);
    let source = crate::source::ScriptedSource::new(data.clone())?;
    let dir = tmp_dir();
    let store = ClientRangedStore::create(dir.path(), "blob", source.root(), total)?;
    let whole = decdn_bao_range::align_range(0, 0, total)?;
    let (claim, reader) = open_leg(&source, &whole).await?;
    store.ingest_stream(&whole, reader, None, claim).await?;
    assert_eq!(store.proven(), Some(total));

    let partial = dir.path().join("blob.partial");
    let mut flipped = data.clone();
    flipped[usize::try_from(GROUP)?] ^= 0xFF;
    std::fs::write(&partial, &flipped)?;
    let err = store
        .finalize()
        .await
        .expect_err("a whole-file hash mismatch must fail");
    assert!(matches!(err, RangedStoreError::Backend(_)), "{err:?}");
    assert!(partial.exists(), "a failed finalize keeps the partial");
    assert_eq!(store.bound(), total, "a failed finalize keeps the bound");
    assert_eq!(store.proven(), None);
    assert!(store.present_ranges().await?.is_empty());
    let record = read_record(&dir.path().join("blob.partial.ranges"))?;
    assert_eq!(
        record,
        StoreState::empty(total),
        "the record drops the claim"
    );

    let (claim, reader) = open_leg(&source, &whole).await?;
    store.ingest_stream(&whole, reader, None, claim).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("blob"))?, data);
    assert!(!partial.exists());
    assert!(!dir.path().join("blob.partial.ranges").exists());
    Ok(())
}

/// The empty blob is proven only against the empty root.
#[tokio::test]
async fn an_empty_blob_is_proven_only_against_the_empty_root() -> anyhow::Result<()> {
    let empty = decdn_bao_range::align_range(0, 0, 0)?;

    let good = fresh_store(*blake3::hash(&[]).as_bytes(), 0);
    good.ingest_stream(&empty, Bytes::new(), None, 0).await?;
    assert_eq!(good.proven(), Some(0));
    assert!(good.is_complete().await?);
    good.finalize().await?;

    let bad = fresh_store([7u8; 32], 0);
    assert!(
        bad.ingest_stream(&empty, Bytes::new(), None, 0)
            .await
            .is_err(),
        "an empty claim must not verify against a non-empty root"
    );
    assert_eq!(bad.proven(), None);
    assert!(matches!(
        bad.finalize().await,
        Err(RangedStoreError::Incomplete)
    ));
    Ok(())
}

/// A blob of less than one chunk is proven by its only leaf and finalizes.
#[tokio::test]
async fn a_one_chunk_blob_is_proven_and_finalizes() -> anyhow::Result<()> {
    let data = blob(500);
    let source = crate::source::ScriptedSource::new(data.clone())?;
    let store = fresh_store(source.root(), 500);
    let whole = decdn_bao_range::align_range(0, 0, 500)?;
    let (claim, reader) = open_leg(&source, &whole).await?;
    store.ingest_stream(&whole, reader, None, claim).await?;

    assert_eq!(store.proven(), Some(500));
    store.finalize().await?;
    assert_eq!(store.read(0, 0).await?.as_ref(), data.as_slice());
    Ok(())
}

/// Fetch `data` whole into a fresh store at `dir`/`stem` and finalize it,
/// leaving the promoted final file. Returns the blob's root.
async fn finalize_blob(dir: &Path, stem: &str, data: &[u8]) -> anyhow::Result<[u8; 32]> {
    let source = crate::source::ScriptedSource::new(data.to_vec())?;
    let total = u64::try_from(data.len())?;
    let store = ClientRangedStore::create(dir, stem, source.root(), total)?;
    let leg = decdn_bao_range::align_range(0, 0, total)?;
    let (claim, reader) = open_leg(&source, &leg).await?;
    store.ingest_stream(&leg, reader, None, claim).await?;
    store.finalize().await?;
    Ok(source.root())
}

/// A final file of blob A at the path blob B is fetched to is not B: with
/// B's partial on disk, `open` resumes the partial; with none,
/// `open_or_create` starts B fresh. A's file is untouched either way.
#[tokio::test]
async fn open_resumes_the_partial_when_the_final_file_is_another_blob() -> anyhow::Result<()> {
    let a = blob(3 * usize::try_from(GROUP)?);
    let mut b = blob(5 * usize::try_from(GROUP)?);
    b.reverse();
    let b_root = bao_root_and_outboard(&b).0;
    let b_total = u64::try_from(b.len())?;

    // B's interrupted partial beside A's final file.
    let dir = tmp_dir();
    finalize_blob(dir.path(), "out.bin", &a).await?;
    ClientRangedStore::seed_checkpointed_prefix(dir.path(), "out.bin", &b, 2 * GROUP)?;
    let store = ClientRangedStore::open(dir.path(), "out.bin", b_root)?;
    assert!(!store.is_complete().await?, "A's file must not complete B");
    assert_eq!(store.bound(), b_total);
    assert_eq!(store.proven(), None);
    assert_eq!(
        &store.present_ranges().await?,
        decdn_bao_range::align_range(0, 2 * GROUP, b_total)?.chunk_ranges()
    );
    assert!(dir.path().join("out.bin.partial.ranges").exists());
    assert_eq!(std::fs::read(dir.path().join("out.bin"))?, a);

    // No partial for B: a fresh store, A's file still untouched.
    let dir = tmp_dir();
    finalize_blob(dir.path(), "out.bin", &a).await?;
    assert!(ClientRangedStore::open(dir.path(), "out.bin", b_root).is_err());
    let store = ClientRangedStore::open_or_create(dir.path(), "out.bin", b_root, b_total)?;
    assert!(store.present_ranges().await?.is_empty());
    assert_eq!(store.proven(), None);
    assert_eq!(store.bound(), b_total);
    assert_eq!(std::fs::read(dir.path().join("out.bin"))?, a);
    Ok(())
}

/// A reopened store reports the byte spans its record holds, clamped to
/// the bound: the checkpointed prefix, and the ragged final chunk once a
/// fetch has it.
#[tokio::test]
async fn present_byte_ranges_reports_the_recorded_prefix() -> anyhow::Result<()> {
    let data = blob(3 * usize::try_from(GROUP)? + 99);
    let total = u64::try_from(data.len())?;
    let root = bao_root_and_outboard(&data).0;
    let dir = tmp_dir();
    ClientRangedStore::seed_checkpointed_prefix(dir.path(), "blob", &data, GROUP)?;
    let store = ClientRangedStore::open(dir.path(), "blob", root)?;
    assert_eq!(store.present_byte_ranges(), vec![(0, GROUP)]);

    let dir = tmp_dir();
    ClientRangedStore::seed_checkpointed_prefix(dir.path(), "blob", &data, total)?;
    let store = ClientRangedStore::open(dir.path(), "blob", root)?;
    assert_eq!(store.present_byte_ranges(), vec![(0, total)]);

    let dir = tmp_dir();
    let store = ClientRangedStore::create(dir.path(), "blob", root, total)?;
    assert!(store.present_byte_ranges().is_empty());
    Ok(())
}

/// A final file that hashes to the root reopens complete, and `open`
/// removes a stale record a crash left beside it (the positive path; the
/// foreign-file path is `open_resumes_the_partial_when_the_final_file_is_another_blob`).
#[tokio::test]
async fn open_on_a_matching_final_file_is_complete_and_clears_the_record() -> anyhow::Result<()> {
    let a = blob(3 * usize::try_from(GROUP)? + 7);
    let dir = tmp_dir();
    let root = finalize_blob(dir.path(), "out.bin", &a).await?;
    let record = dir.path().join("out.bin.partial.ranges");
    write_record(&record, &StoreState::empty(1))?;

    let store = ClientRangedStore::open(dir.path(), "out.bin", root)?;
    assert!(!record.exists(), "the stale record is removed");
    assert!(store.is_complete().await?);
    assert_eq!(store.proven(), Some(u64::try_from(a.len())?));
    assert_eq!(std::fs::read(dir.path().join("out.bin"))?, a);
    Ok(())
}

/// A `.partial` cut short after ingest fails `finalize` as a mismatch, not
/// as an I/O error: the store drops its claim, and a re-fetch completes.
#[tokio::test]
async fn a_truncated_partial_heals_on_the_next_fetch() -> anyhow::Result<()> {
    let total = 3 * GROUP + 99;
    let data = blob(usize::try_from(total)?);
    let source = crate::source::ScriptedSource::new(data.clone())?;
    let dir = tmp_dir();
    let store = ClientRangedStore::create(dir.path(), "blob", source.root(), total)?;
    let whole = decdn_bao_range::align_range(0, 0, total)?;
    let (claim, reader) = open_leg(&source, &whole).await?;
    store.ingest_stream(&whole, reader, None, claim).await?;

    let partial = dir.path().join("blob.partial");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&partial)?
        .set_len(GROUP)?;
    let err = store
        .finalize()
        .await
        .expect_err("a truncated partial must not finalize");
    let RangedStoreError::Backend(source_err) = &err else {
        panic!("expected a backend error, got {err:?}");
    };
    assert!(
        source_err.downcast_ref::<crate::HashMismatch>().is_some(),
        "a short partial is a hash mismatch, not an I/O error: {err:?}"
    );
    assert!(store.present_ranges().await?.is_empty());
    assert_eq!(store.proven(), None);
    assert!(partial.exists());

    let (claim, reader) = open_leg(&source, &whole).await?;
    store.ingest_stream(&whole, reader, None, claim).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("blob"))?, data);
    Ok(())
}
