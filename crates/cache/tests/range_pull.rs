//! Tests for the origin range-pull verify/encode helper (#823, ADR 037
//! §Origin-tier pull-through). The helper turns a raw origin byte range plus the
//! published `{H}.obao4` outboard into a bao-encoded stream verified against the
//! root `H`, ready for `iroh-blobs`' `import_bao_bytes`.

use bao_tree::io::outboard::PreOrderMemOutboard;
use decdn_cache::range_pull::{
    IROH_BLOCK_SIZE, RangeVerifyError, align_range, encode_verified_range,
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
    let encoded = encode_verified_range(root, blob_size, &aligned, &data, ob.data.clone().into())?;
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
        blob_size,
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
        blob_size,
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
        blob_size,
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
    let err = err_of(encode_verified_range(
        root,
        blob_size,
        &aligned,
        &data,
        short.into(),
    ))?;
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
    let encoded = encode_verified_range(root, blob_size, &aligned, &data, ob.data.clone().into())?;

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
    let encoded = encode_verified_range(root, blob_size, &aligned, &data, ob.data.clone().into())?;

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
