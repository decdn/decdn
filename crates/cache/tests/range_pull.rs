//! Tests for the origin range-pull verify/encode helper (#823, ADR 037
//! §Origin-tier pull-through). The helper turns a raw origin byte range plus the
//! published `{H}.obao4` outboard into a bao-encoded stream verified against the
//! root `H`, ready for `iroh-blobs`' `import_bao_bytes`.

use bao_tree::io::outboard::PreOrderMemOutboard;
use decdn_cache::range_pull::{
    IROH_BLOCK_SIZE, RangeVerifyError, align_range, bao_encoded_size, encode_verified_range,
};

// Bytes per chunk group, derived from the upstream block size (not hard-coded)
// so an iroh-blobs block-size change can't make these assertions silently wrong.
const GROUP: u64 = 1u64 << (IROH_BLOCK_SIZE.chunk_log() + 10);

/// Deterministic pseudo-random blob spanning several chunk groups.
fn make_blob(len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        // Low byte of the state; `to_le_bytes().first()` avoids a truncating cast.
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

/// Copy the half-open byte span `[start, end)` out of `data` without panicking
/// indexing (the workspace denies `indexing_slicing`).
fn sub(data: &[u8], start: u64, end: u64) -> anyhow::Result<Vec<u8>> {
    let s = usize::try_from(start)?;
    let e = usize::try_from(end)?;
    data.get(s..e)
        .map(<[u8]>::to_vec)
        .ok_or_else(|| anyhow::anyhow!("range [{start}, {end}) out of bounds"))
}

/// Assert a `Result` is `Err` and return the error, anti-panic style.
fn err_of<T, E>(r: Result<T, E>) -> anyhow::Result<E> {
    match r {
        Ok(_) => anyhow::bail!("expected Err, got Ok"),
        Err(e) => Ok(e),
    }
}

/// The header-less wire length of a `combined` (8-byte LE size header +
/// interleaved proof/data) encoding — what travels on `cdn/client/v1`.
fn strip_size_header(combined: &[u8]) -> anyhow::Result<u64> {
    u64::try_from(combined.len())?
        .checked_sub(8)
        .ok_or_else(|| anyhow::anyhow!("combined encoding shorter than its 8-byte header"))
}

#[test]
fn align_range_snaps_to_chunk_group_boundaries() -> anyhow::Result<()> {
    // A request for [20 KiB, 40 KiB) of a 200 KiB blob must widen to the
    // enclosing 16 KiB chunk groups: [16 KiB, 48 KiB).
    let a = align_range(20 * 1024, 20 * 1024, 200 * 1024)?;
    anyhow::ensure!(
        a.fetch_start() == GROUP,
        "floor to 16 KiB, got {}",
        a.fetch_start()
    );
    anyhow::ensure!(
        a.fetch_end() == 3 * GROUP,
        "ceil to 48 KiB, got {}",
        a.fetch_end()
    );
    Ok(())
}

#[test]
fn align_range_already_aligned_does_not_over_widen() -> anyhow::Result<()> {
    // A request that is already on group boundaries must be a no-op widen, and
    // the whole-blob pull (offset 0, len 0) must span the entire blob.
    let a = align_range(GROUP, GROUP, 200 * 1024)?;
    anyhow::ensure!(
        a.fetch_start() == GROUP && a.fetch_end() == 2 * GROUP,
        "no over-widen"
    );
    let whole = align_range(0, 0, 200 * 1024)?;
    anyhow::ensure!(whole.fetch_start() == 0 && whole.fetch_end() == 200 * 1024);
    Ok(())
}

#[test]
fn align_range_zero_len_means_to_end() -> anyhow::Result<()> {
    let blob_size = 200 * 1024;
    let a = align_range(64 * 1024, 0, blob_size)?;
    anyhow::ensure!(a.fetch_start() == 64 * 1024);
    anyhow::ensure!(a.fetch_end() == blob_size, "clamp to blob end");
    Ok(())
}

#[test]
fn align_range_rejects_offset_past_end() -> anyhow::Result<()> {
    let err = err_of(align_range(300 * 1024, 1024, 200 * 1024))?;
    anyhow::ensure!(
        matches!(err, RangeVerifyError::RangeOutOfBounds { .. }),
        "{err:?}"
    );
    Ok(())
}

#[test]
fn align_range_rejects_overflowing_len() -> anyhow::Result<()> {
    // byte_offset within the blob but byte_offset + byte_len overflows u64 — an
    // untrusted-wire input that must reject cleanly (no wrap), exercising the
    // `checked_add` branch distinct from the offset-past-end guard.
    let err = err_of(align_range(200 * 1024 - 1, u64::MAX, 200 * 1024))?;
    anyhow::ensure!(
        matches!(err, RangeVerifyError::RangeOutOfBounds { .. }),
        "{err:?}"
    );
    Ok(())
}

#[test]
fn align_range_rejects_len_past_end_but_accepts_exact_fit() -> anyhow::Result<()> {
    let blob_size: u64 = 200 * 1024;
    // One byte past the end rejects (ADR 005: reject, don't clamp).
    let over = err_of(align_range(64 * 1024, blob_size - 64 * 1024 + 1, blob_size))?;
    anyhow::ensure!(
        matches!(over, RangeVerifyError::RangeOutOfBounds { .. }),
        "{over:?}"
    );
    // Exactly to the end is accepted.
    let exact = align_range(64 * 1024, blob_size - 64 * 1024, blob_size)?;
    anyhow::ensure!(exact.fetch_end() == blob_size);
    Ok(())
}

#[test]
fn encode_verified_range_roundtrips_for_honest_bytes() -> anyhow::Result<()> {
    let blob = make_blob(200 * 1024);
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let root = *ob.root.as_bytes();
    let blob_size = u64::try_from(blob.len())?;

    let aligned = align_range(64 * 1024, 32 * 1024, blob_size)?;
    let data = sub(&blob, aligned.fetch_start(), aligned.fetch_end())?;

    // Honest range + honest outboard verifies and produces a non-empty stream.
    let encoded = encode_verified_range(root, &aligned, &data, ob.data.clone().into())?;
    anyhow::ensure!(!encoded.is_empty());
    Ok(())
}

#[test]
fn encode_verified_range_rejects_tampered_data() -> anyhow::Result<()> {
    let blob = make_blob(200 * 1024);
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let root = *ob.root.as_bytes();
    let blob_size = u64::try_from(blob.len())?;

    let aligned = align_range(64 * 1024, 32 * 1024, blob_size)?;
    let mut data = sub(&blob, aligned.fetch_start(), aligned.fetch_end())?;
    if let Some(b) = data.get_mut(100) {
        *b ^= 0xff; // corrupt one byte in the served range (leaf-hash path)
    }

    let err = err_of(encode_verified_range(
        root,
        &aligned,
        &data,
        ob.data.clone().into(),
    ))?;
    anyhow::ensure!(
        matches!(err, RangeVerifyError::Verification { .. }),
        "{err:?}"
    );
    Ok(())
}

#[test]
fn encode_verified_range_rejects_tampered_outboard() -> anyhow::Result<()> {
    // A flipped byte in the outboard exercises the parent-hash-spine path, which
    // is distinct from a corrupted data leaf.
    let blob = make_blob(200 * 1024);
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let root = *ob.root.as_bytes();
    let blob_size = u64::try_from(blob.len())?;

    let aligned = align_range(64 * 1024, 32 * 1024, blob_size)?;
    let data = sub(&blob, aligned.fetch_start(), aligned.fetch_end())?;
    let mut bad_outboard = ob.data.clone();
    if let Some(b) = bad_outboard.get_mut(40) {
        *b ^= 0xff;
    }

    let err = err_of(encode_verified_range(
        root,
        &aligned,
        &data,
        bad_outboard.into(),
    ))?;
    anyhow::ensure!(
        matches!(err, RangeVerifyError::Verification { .. }),
        "{err:?}"
    );
    Ok(())
}

#[test]
fn encode_verified_range_rejects_wrong_root() -> anyhow::Result<()> {
    let blob = make_blob(200 * 1024);
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let mut root = *ob.root.as_bytes();
    if let Some(b) = root.get_mut(0) {
        *b ^= 0xff; // wanted-root mismatch
    }
    let blob_size = u64::try_from(blob.len())?;

    let aligned = align_range(64 * 1024, 32 * 1024, blob_size)?;
    let data = sub(&blob, aligned.fetch_start(), aligned.fetch_end())?;

    let err = err_of(encode_verified_range(
        root,
        &aligned,
        &data,
        ob.data.clone().into(),
    ))?;
    anyhow::ensure!(
        matches!(err, RangeVerifyError::Verification { .. }),
        "{err:?}"
    );
    Ok(())
}

#[test]
fn encode_verified_range_rejects_wrong_length_outboard() -> anyhow::Result<()> {
    // A malformed/foreign `{H}.obao4` from an untrusted origin must be rejected
    // by the cheap length check before any crypto — a declared error variant.
    let blob = make_blob(200 * 1024);
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let root = *ob.root.as_bytes();
    let blob_size = u64::try_from(blob.len())?;
    let aligned = align_range(64 * 1024, 32 * 1024, blob_size)?;
    let data = sub(&blob, aligned.fetch_start(), aligned.fetch_end())?;

    let mut short = ob.data.clone();
    short.truncate(short.len() - 8);
    let err = err_of(encode_verified_range(root, &aligned, &data, short.into()))?;
    anyhow::ensure!(
        matches!(err, RangeVerifyError::OutboardSize { .. }),
        "{err:?}"
    );
    Ok(())
}

/// End-to-end store path (#823 Phase 2c): the verified encoding the helper
/// produces imports into `iroh-blobs` as a partial blob via `import_bao_bytes`,
/// and `export_ranges` reads the **originally requested** (narrower) sub-range
/// back from the widened import — no whole-blob present.
#[tokio::test]
async fn widened_import_serves_back_the_requested_subrange() -> anyhow::Result<()> {
    use iroh_blobs::store::mem::MemStore;

    let blob = make_blob(200 * 1024);
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let root = *ob.root.as_bytes();
    let blob_size = u64::try_from(blob.len())?;
    let hash = iroh_blobs::Hash::from_bytes(root);

    // Client wants [20 KiB, 40 KiB); the fetch widens to [16 KiB, 48 KiB).
    let (req_start, req_end) = (20 * 1024, 40 * 1024);
    let aligned = align_range(req_start, req_end - req_start, blob_size)?;
    anyhow::ensure!(aligned.fetch_start() == GROUP && aligned.fetch_end() == 3 * GROUP);
    let data = sub(&blob, aligned.fetch_start(), aligned.fetch_end())?;
    let encoded = encode_verified_range(root, &aligned, &data, ob.data.clone().into())?;

    let store = MemStore::new();
    store
        .blobs()
        .import_bao_bytes(hash, aligned.chunk_ranges().clone(), encoded)
        .await?;

    // The narrower client range is recoverable from the widened partial blob.
    let exported = store
        .blobs()
        .export_ranges(hash, req_start..req_end)
        .concatenate()
        .await?;
    anyhow::ensure!(
        exported == sub(&blob, req_start, req_end)?,
        "requested sub-range mismatch"
    );
    Ok(())
}

/// A blob whose size is **not** a chunk-group multiple exercises the final
/// partial group (`fetch_end == blob_size`, `chunks()` ceiling) — the common
/// real-world case the round-aligned tests miss.
#[tokio::test]
async fn non_group_aligned_blob_tail_roundtrips() -> anyhow::Result<()> {
    use iroh_blobs::store::mem::MemStore;

    let blob = make_blob(200 * 1024 + 1234); // deliberately not a 16 KiB multiple
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let root = *ob.root.as_bytes();
    let blob_size = u64::try_from(blob.len())?;
    let hash = iroh_blobs::Hash::from_bytes(root);

    // Request the final ~4 KiB, which lands inside the last partial group.
    let req_start = blob_size - 4096;
    let aligned = align_range(req_start, 0, blob_size)?;
    anyhow::ensure!(
        aligned.fetch_end() == blob_size,
        "tail clamps to blob end, not past it"
    );
    let data = sub(&blob, aligned.fetch_start(), aligned.fetch_end())?;
    let encoded = encode_verified_range(root, &aligned, &data, ob.data.clone().into())?;

    let store = MemStore::new();
    store
        .blobs()
        .import_bao_bytes(hash, aligned.chunk_ranges().clone(), encoded)
        .await?;
    let exported = store
        .blobs()
        .export_ranges(hash, req_start..blob_size)
        .concatenate()
        .await?;
    anyhow::ensure!(
        exported == sub(&blob, req_start, blob_size)?,
        "tail sub-range mismatch"
    );
    Ok(())
}

// --- bao_encoded_size (#915, ADR 038 §Payment metering) ----------------------

/// `bao_encoded_size` must equal the byte length the serve side actually emits.
/// `encode_verified_range` produces the *combined* encoding (an 8-byte LE size
/// header followed by the interleaved proof+data stream); the `cdn/client/v1`
/// wire carries the *header-less* form, so the wire length is `encoded.len() - 8`.
/// Cross-check `bao_encoded_size` against that for ranges that exercise the
/// distinct code paths: whole blob, an interior multi-group span, a single chunk
/// group, and a tail that lands in the partial final group.
fn assert_encoded_size_matches(blob_len: usize, offset: u64, len: u64) -> anyhow::Result<()> {
    let blob = make_blob(blob_len);
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let root = *ob.root.as_bytes();
    let blob_size = u64::try_from(blob.len())?;

    let aligned = align_range(offset, len, blob_size)?;
    let data = sub(&blob, aligned.fetch_start(), aligned.fetch_end())?;
    let combined = encode_verified_range(root, &aligned, &data, ob.data.clone().into())?;

    // Header-less wire length = combined length minus the 8-byte size header.
    let wire_len = strip_size_header(&combined)?;
    let predicted = bao_encoded_size(blob_size, aligned.chunk_ranges());
    anyhow::ensure!(
        predicted == wire_len,
        "bao_encoded_size {predicted} != actual wire length {wire_len} \
         (blob {blob_len}, offset {offset}, len {len})"
    );
    // Sanity: the wire carries at least as much as the data (equal only for a
    // single-leaf tree, which has no proof parents; strictly more otherwise).
    anyhow::ensure!(predicted >= aligned.fetch_len(), "wire must cover the data");
    Ok(())
}

#[test]
fn bao_encoded_size_matches_actual_wire_length() -> anyhow::Result<()> {
    // Whole blob (offset 0, len 0 == to-end).
    assert_encoded_size_matches(200 * 1024, 0, 0)?;
    // Interior multi-group span.
    assert_encoded_size_matches(200 * 1024, 64 * 1024, 32 * 1024)?;
    // A single 16 KiB chunk group.
    assert_encoded_size_matches(200 * 1024, 32 * 1024, 16 * 1024)?;
    // Tail inside the partial final group of a non-multiple blob.
    assert_encoded_size_matches(200 * 1024 + 1234, 196 * 1024, 0)?;
    // A blob smaller than one chunk group (single-leaf tree, no interior nodes).
    assert_encoded_size_matches(500, 0, 0)?;
    Ok(())
}

/// Golden wire number (#1060, ADR 038 §Risks): a hard-coded byte count so a
/// `bao-tree` bump that changes the encoding SHAPE fails here instead of passing
/// silently. Every other wire expectation in the suite is computed with the same
/// helper production uses, so none would catch a cross-version encoding change
/// that both sides compute identically-but-differently.
///
/// A 1.5 MiB blob is exactly 96 × 16 KiB chunk groups. A full binary tree over 96
/// leaves has 95 interior parent nodes; the whole-blob response emits every leaf
/// (1,572,864 content bytes) plus 95 × 64-byte parents (6,080 bytes) =
/// 1,578,944 header-less wire bytes.
#[test]
fn bao_encoded_size_whole_blob_1_5_mib_is_golden() -> anyhow::Result<()> {
    const BLOB_SIZE: u64 = 1_572_864; // 1.5 MiB, exactly 96 chunk groups
    const GOLDEN_WIRE: u64 = 1_578_944; // 1,572,864 content + 95 parents × 64

    let aligned = align_range(0, 0, BLOB_SIZE)?;
    let predicted = bao_encoded_size(BLOB_SIZE, aligned.chunk_ranges());
    anyhow::ensure!(
        predicted == GOLDEN_WIRE,
        "1.5 MiB whole-blob wire size changed: got {predicted}, expected {GOLDEN_WIRE} \
         (a bao-tree encoding-shape change — see ADR 038 §Risks)"
    );
    // Keep the golden coupled to the real encoding: it must equal what
    // encode_verified_range actually emits (header-less), not just a constant.
    let blob = make_blob(usize::try_from(BLOB_SIZE)?);
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let root = *ob.root.as_bytes();
    let data = sub(&blob, aligned.fetch_start(), aligned.fetch_end())?;
    let combined = encode_verified_range(root, &aligned, &data, ob.data.clone().into())?;
    let actual_wire = strip_size_header(&combined)?;
    anyhow::ensure!(
        actual_wire == GOLDEN_WIRE,
        "actual emitted wire {actual_wire} != golden {GOLDEN_WIRE}"
    );
    Ok(())
}
