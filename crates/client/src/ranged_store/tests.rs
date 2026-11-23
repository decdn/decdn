use std::assert_matches;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use bao_tree::{ChunkNum, ChunkRanges};
use bytes::Bytes;
use decdn_bao_range::{
    AlignedRange, CHUNK_GROUP_BYTES, IROH_BLOCK_SIZE, RangedStore, RangedStoreError,
};

use super::{ClientRangedStore, last_group, store_dir_for, whole};
use crate::ProvenSize;
use crate::source::IngestEnd;

const GROUP: u64 = CHUNK_GROUP_BYTES;
const KIB: u64 = 1024;

/// Deterministic `len`-byte blob (xorshift fill), its bao root and full
/// pre-order outboard.
fn synth_blob(len: usize) -> ([u8; 32], Vec<u8>, Bytes) {
    let plaintext = blob(len);
    let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(&plaintext, IROH_BLOCK_SIZE);
    (*ob.root.as_bytes(), plaintext, Bytes::from(ob.data))
}

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

/// Interleaved bao (combined format: 8-byte header + body) for `aligned`.
fn bao_for(root: [u8; 32], plaintext: &[u8], outboard: Bytes, aligned: &AlignedRange) -> Bytes {
    let s = usize::try_from(aligned.fetch_start()).expect("fits usize");
    let e = usize::try_from(aligned.fetch_end()).expect("fits usize");
    decdn_bao_range::encode_verified_range(root, aligned, &plaintext[s..e], outboard)
        .expect("verifies")
}

fn fresh_store(root: [u8; 32], total: u64) -> ClientRangedStore {
    ClientRangedStore::in_memory(root, total)
}

/// Total bytes a [`ChunkRanges`] covers (`ChunkNum` counts 1 KiB chunks).
fn ranges_byte_len(ranges: &ChunkRanges) -> u64 {
    let boundaries = ranges.boundaries();
    let mut sum = 0u64;
    let mut it = boundaries.iter();
    while let (Some(a), Some(b)) = (it.next(), it.next()) {
        sum += (b.0 - a.0) * 1024;
    }
    sum
}

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

// --- helpers ---

#[test]
fn whole_and_last_group_cover_the_blob_end() {
    let group_chunks = GROUP / 1024;
    assert_eq!(whole(0), ChunkRanges::empty());
    assert_eq!(last_group(0), ChunkRanges::empty());
    assert_eq!(whole(500), ChunkRanges::from(ChunkNum(0)..ChunkNum(1)));
    assert_eq!(last_group(500), ChunkRanges::from(ChunkNum(0)..ChunkNum(1)));
    let size = 3 * GROUP + 7;
    assert_eq!(
        last_group(size),
        ChunkRanges::from(ChunkNum(3 * group_chunks)..ChunkNum(3 * group_chunks + 1))
    );
    assert_eq!(
        last_group(4 * GROUP),
        ChunkRanges::from(ChunkNum(3 * group_chunks)..ChunkNum(4 * group_chunks))
    );
}

#[test]
fn the_store_dir_sits_beside_the_destination() {
    assert_eq!(
        store_dir_for(std::path::Path::new("/out/model.bin")).expect("dir"),
        std::path::PathBuf::from("/out/model.bin.decdn")
    );
    assert!(store_dir_for(std::path::Path::new("/")).is_err());
}

// --- queries ---

#[tokio::test]
async fn total_bytes_reports_the_bound_until_a_size_is_proven() {
    let store = fresh_store([1; 32], 10_000);
    assert_eq!(store.total_bytes(), 10_000);
    store.set_bound(20_000);
    assert_eq!(store.total_bytes(), 20_000);
    store.prove_size(ProvenSize::assume_for_tests(15_000));
    assert_eq!(store.total_bytes(), 15_000);
    store.set_bound(30_000);
    assert_eq!(store.total_bytes(), 15_000, "a proven size is final");
}

#[tokio::test]
async fn admit_prefix_range_is_readable_and_present() -> anyhow::Result<()> {
    let total = 3 * GROUP;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);
    let store = fresh_store(root, total);

    let aligned = decdn_bao_range::align_range(0, GROUP, total)?;
    store
        .admit(
            aligned.clone(),
            bao_for(root, &plaintext, outboard, &aligned),
        )
        .await?;

    assert_eq!(&store.present_ranges().await?, aligned.chunk_ranges());
    assert_eq!(
        store.read(0, GROUP).await?.as_ref(),
        &plaintext[..usize::try_from(GROUP)?]
    );
    assert_eq!(
        store.missing_ranges(0, 0).await?,
        whole(total) - aligned.chunk_ranges(),
        "everything past the prefix is missing"
    );
    Ok(())
}

#[tokio::test]
async fn read_of_an_absent_span_is_an_error() -> anyhow::Result<()> {
    let total = 3 * GROUP;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);
    let store = fresh_store(root, total);
    let aligned = decdn_bao_range::align_range(0, GROUP, total)?;
    store
        .admit(
            aligned.clone(),
            bao_for(root, &plaintext, outboard, &aligned),
        )
        .await?;
    assert_matches!(
        store.read(GROUP, GROUP).await,
        Err(RangedStoreError::Backend(_))
    );
    assert!(
        store.read(total, GROUP).await.is_err(),
        "a start past the bound"
    );
    Ok(())
}

#[tokio::test]
async fn admit_corrupt_payload_is_an_error_and_marks_nothing() -> anyhow::Result<()> {
    let total = 2 * GROUP;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);
    let store = fresh_store(root, total);
    let aligned = decdn_bao_range::align_range(0, 0, total)?;
    let mut bad = bao_for(root, &plaintext, outboard, &aligned).to_vec();
    let last = bad.len() - 1;
    bad[last] ^= 0xFF;
    assert!(store.admit(aligned, Bytes::from(bad)).await.is_err());
    assert!(
        !store.is_complete().await?,
        "a corrupt payload never completes the store"
    );
    assert_eq!(store.proven(), None);
    Ok(())
}

// --- ingest_stream ---

/// An ingest whose end is lowered stops once its verified prefix reaches
/// it: the prefix is present, nothing past it is, and the wire it read is
/// exactly the encoding of the shorter range. An end at or past the range's
/// end drains the range as usual.
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
        assert_eq!(&store.present_ranges().await?, kept.chunk_ranges());
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
    assert_eq!(ended, IngestEnd::Drained);
    assert!(rest.is_empty());
    assert_eq!(&store.present_ranges().await?, range.chunk_ranges());
    Ok(())
}

/// A prefix proves no size; the tail does, and the blob then finalizes.
#[tokio::test]
async fn ingest_stream_lands_present_bytes_and_the_tail_proves_the_size() -> anyhow::Result<()> {
    let total = 3 * GROUP + 123;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("out");
    let store = fresh_store(root, total).exporting_to(dest.clone());

    let head = decdn_bao_range::align_range(0, 2 * GROUP, total)?;
    let body = bao_for(root, &plaintext, outboard.clone(), &head).slice(8..);
    store.ingest_stream(&head, body, None, total).await?;
    assert_eq!(store.proven(), None, "a prefix proves no size");
    assert_eq!(&store.present_ranges().await?, head.chunk_ranges());
    assert_matches!(store.finalize().await, Err(RangedStoreError::Incomplete));

    let tail = decdn_bao_range::align_range(2 * GROUP, 0, total)?;
    let body = bao_for(root, &plaintext, outboard, &tail).slice(8..);
    store.ingest_stream(&tail, body, None, total).await?;
    assert_eq!(store.proven(), Some(total), "the tail proves the size");
    store.finalize().await?;
    assert_eq!(std::fs::read(&dest)?, plaintext);
    Ok(())
}

/// A fault after a range's first parents and before its first leaf marks
/// nothing present.
#[tokio::test]
async fn ingest_stream_fault_before_the_first_leaf_marks_nothing_present() -> anyhow::Result<()> {
    let total: u64 = 32 * GROUP;
    let source = crate::source::ScriptedSource::new(blob(usize::try_from(total)?))?
        .with_fault_after(64, || anyhow::anyhow!("scripted reset"));
    let store = fresh_store(source.root(), total);
    let aligned = decdn_bao_range::align_range(4 * GROUP, 4 * GROUP, total)?;
    let (_claim, reader) = open_leg(&source, &aligned).await?;

    let err = store
        .ingest_stream(&aligned, reader, None, total)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("the fault must surface as an error"))?;
    assert!(format!("{err:#}").contains("scripted reset"), "{err:#}");
    assert!(store.present_ranges().await?.is_empty());
    Ok(())
}

/// A mid-range fault keeps every verified group before it: the store writes
/// the batch it holds before it returns, and claims nothing past the fault.
#[tokio::test]
async fn ingest_stream_mid_range_fault_keeps_the_verified_prefix() -> anyhow::Result<()> {
    let total: u64 = 8 * 1024 * KIB;
    let data = blob(usize::try_from(total)?);
    let source = crate::source::ScriptedSource::new(data.clone())?
        .with_fault_after(5 * 1024 * 1024, || {
            anyhow::anyhow!("scripted mid-range stall")
        });
    let store = fresh_store(source.root(), total);
    let aligned = decdn_bao_range::align_range(0, 0, total)?;
    let (_claim, reader) = open_leg(&source, &aligned).await?;

    let err = store
        .ingest_stream(&aligned, reader, None, total)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("the fault must surface as an error"))?;
    assert!(
        format!("{err:#}").contains("scripted mid-range stall"),
        "the parked typed fault must survive: {err:#}"
    );
    let present = ranges_byte_len(&store.present_ranges().await?);
    assert!(
        present > ClientRangedStore::INGEST_CHECKPOINT_BYTES,
        "the batch held at the fault is kept: {present}"
    );
    assert!(
        present < total,
        "nothing past the fault is claimed: {present}"
    );
    assert_eq!(
        store.read(0, present).await?.as_ref(),
        &data[..usize::try_from(present)?]
    );
    Ok(())
}

/// Two disjoint ranges ingested concurrently both land.
#[tokio::test]
async fn concurrent_ingest_lands_both_ranges() -> anyhow::Result<()> {
    let total = 8 * 1024 * KIB;
    let (root, data, outboard) = synth_blob(usize::try_from(total)?);
    let store = fresh_store(root, total);
    let lo = decdn_bao_range::align_range(0, 4 * 1024 * KIB, total)?;
    let hi = decdn_bao_range::align_range(4 * 1024 * KIB, 4 * 1024 * KIB, total)?;
    let lo_body = bao_for(root, &data, outboard.clone(), &lo).slice(8..);
    let hi_body = bao_for(root, &data, outboard, &hi).slice(8..);
    let (a, b) = tokio::join!(
        store.ingest_stream(&lo, lo_body, None, total),
        store.ingest_stream(&hi, hi_body, None, total)
    );
    a?;
    b?;
    assert_eq!(
        store.present_ranges().await?,
        lo.chunk_ranges().clone() | hi.chunk_ranges().clone()
    );
    assert_eq!(store.proven(), Some(total), "the high half holds the tail");
    assert!(store.is_complete().await?);
    Ok(())
}

/// The batch ramp: a stalled ingest has made present exactly the whole
/// batches it received.
#[tokio::test]
async fn ingest_stream_marks_present_in_whole_batches() -> anyhow::Result<()> {
    let total: u64 = 8 * 1024 * KIB;
    let stall = 3 * 1024 * KIB;
    let source = crate::source::ScriptedSource::new(blob(usize::try_from(total)?))?
        .stall_after(stall, Duration::from_hours(1));
    let store = fresh_store(source.root(), total);
    let aligned = decdn_bao_range::align_range(0, 0, total)?;
    let (_claim, reader) = open_leg(&source, &aligned).await?;
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        store.ingest_stream(&aligned, reader, None, total),
    )
    .await;
    assert_eq!(
        ranges_byte_len(&store.present_ranges().await?),
        ClientRangedStore::checkpointed_len(stall)
    );
    Ok(())
}

/// A waiter on `present_grew` wakes when a batch extends `present`.
#[tokio::test]
async fn a_batch_wakes_present_waiters() -> anyhow::Result<()> {
    let total = 256 * KIB;
    let (root, data, outboard) = synth_blob(usize::try_from(total)?);
    let store = fresh_store(root, total);
    let aligned = decdn_bao_range::align_range(0, 0, total)?;
    let wire = bao_for(root, &data, outboard, &aligned).slice(8..);

    let grew = store.present_grew().notified();
    tokio::pin!(grew);
    grew.as_mut().enable();
    store.ingest_stream(&aligned, wire, None, total).await?;
    tokio::time::timeout(Duration::from_secs(1), grew)
        .await
        .map_err(|_| anyhow::anyhow!("no wakeup when present grew"))?;
    Ok(())
}

// --- sizes ---

/// A hint below the true size: the leg that covers the true tail verifies
/// under its own claim, proves that size, and moves the bound to it.
#[tokio::test]
async fn a_leg_proves_its_claimed_size_and_sets_the_bound() -> anyhow::Result<()> {
    let truth = 1200 * KIB;
    let source = crate::source::ScriptedSource::new(blob(usize::try_from(truth)?))?;
    let store = fresh_store(source.root(), 1000 * KIB);
    let tail = decdn_bao_range::align_range(1000 * KIB, 200 * KIB, truth)?;
    let (claim, reader) = open_leg(&source, &tail).await?;
    assert_eq!(claim, truth);
    store.ingest_stream(&tail, reader, None, claim).await?;

    assert_eq!(store.proven(), Some(truth));
    assert_eq!(store.bound(), truth);
    assert_eq!(&store.present_ranges().await?, tail.chunk_ranges());
    Ok(())
}

/// A hint above the true size: the leg verifies under the node's claim,
/// lands genuine bytes and proves the smaller size.
#[tokio::test]
async fn a_leg_under_a_different_claim_lands_genuine_bytes() -> anyhow::Result<()> {
    let truth = 1024 * KIB;
    let data = blob(usize::try_from(truth)?);
    let source = crate::source::ScriptedSource::new(data.clone())?;
    let store = fresh_store(source.root(), 2048 * KIB);
    let leg = decdn_bao_range::align_range(0, truth, 2048 * KIB)?;
    let (claim, reader) = open_leg(&source, &leg).await?;
    store.ingest_stream(&leg, reader, None, claim).await?;

    assert_eq!(store.proven(), Some(truth));
    assert!(store.is_complete().await?);
    assert_eq!(store.read(0, 0).await?.as_ref(), data.as_slice());
    Ok(())
}

/// A fresh proof replaces another proven size.
#[tokio::test]
async fn a_fresh_proof_replaces_another_proven_size() {
    let store = fresh_store([3; 32], 1000);
    store.prove_size(ProvenSize::assume_for_tests(2000));
    store.prove_size(ProvenSize::assume_for_tests(1500));
    assert_eq!(store.proven(), Some(1500));
    assert_eq!(store.bound(), 1500);
}

/// The empty blob is proven only against the empty root.
#[tokio::test]
async fn an_empty_blob_is_proven_only_against_the_empty_root() -> anyhow::Result<()> {
    let empty = decdn_bao_range::align_range(0, 0, 0)?;
    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("empty");
    let good = fresh_store(decdn_protocol::EMPTY_ROOT, 0).exporting_to(dest.clone());
    good.ingest_stream(&empty, Bytes::new(), None, 0).await?;
    assert_eq!(good.proven(), Some(0));
    assert!(good.is_complete().await?);
    good.finalize().await?;
    assert_eq!(std::fs::read(&dest)?, b"");

    let bad = fresh_store([7u8; 32], 0);
    assert!(
        bad.ingest_stream(&empty, Bytes::new(), None, 0)
            .await
            .is_err()
    );
    assert_eq!(bad.proven(), None);
    assert_matches!(bad.finalize().await, Err(RangedStoreError::Incomplete));
    Ok(())
}

/// A blob of less than one chunk is proven by its only leaf and finalizes.
#[tokio::test]
async fn a_one_chunk_blob_is_proven_and_finalizes() -> anyhow::Result<()> {
    let data = blob(500);
    let source = crate::source::ScriptedSource::new(data.clone())?;
    let store = fresh_store(source.root(), 500);
    let leg = decdn_bao_range::align_range(0, 0, 500)?;
    let (claim, reader) = open_leg(&source, &leg).await?;
    store.ingest_stream(&leg, reader, None, claim).await?;
    assert_eq!(store.proven(), Some(500));
    store.finalize().await?;
    assert_eq!(store.read(0, 0).await?.as_ref(), data.as_slice());
    Ok(())
}

/// Every byte of the bound present is not enough: `finalize` needs a
/// proven size.
#[tokio::test]
async fn finalize_requires_a_proven_size() -> anyhow::Result<()> {
    let total = 2 * GROUP;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);
    let store = fresh_store(root, total);
    // The head, then a bound moved down to it: every byte of the bound is
    // present, but no final chunk proved a size.
    let head = decdn_bao_range::align_range(0, GROUP, total)?;
    store
        .admit(head.clone(), bao_for(root, &plaintext, outboard, &head))
        .await?;
    store.set_bound(GROUP);
    assert_eq!(store.proven(), None);
    assert!(!store.is_complete().await?);
    assert_matches!(store.finalize().await, Err(RangedStoreError::Incomplete));
    Ok(())
}
