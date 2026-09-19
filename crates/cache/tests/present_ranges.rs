//! Integration tests for [`decdn_cache::CacheEngine::present_ranges`] — the
//! present-range oracle: which chunk ranges of a hash are on disk right now.

use decdn_cache::PresentRanges;
use decdn_cache::range_pull::align_range;
use iroh_blobs::Hash;

mod util;

#[tokio::test]
async fn absent_hash_reports_empty_incomplete() -> anyhow::Result<()> {
    let (engine, _tmp) = util::empty_engine().await?;
    let hash = Hash::new(b"never-stored");
    let pr: PresentRanges = engine.present_ranges(hash).await?;
    assert!(pr.is_empty());
    assert!(!pr.is_complete());
    Ok(())
}

#[tokio::test]
async fn complete_blob_reports_complete() -> anyhow::Result<()> {
    let payload = util::make_blob(200 * 1024);
    let (engine, hash, _tmp, _srv) = util::engine_with_whole_blob(&payload).await?;
    engine.populate(hash).await?; // whole-blob fill
    let pr: PresentRanges = engine.present_ranges(hash).await?;
    assert!(pr.is_complete());
    assert!(!pr.is_empty());
    Ok(())
}

#[tokio::test]
async fn partial_import_reports_only_the_imported_span() -> anyhow::Result<()> {
    let payload = util::make_blob(200 * 1024);
    let blob_size = u64::try_from(payload.len())?;
    let (engine, _tmp) = util::empty_engine().await?;

    // Admit a middle span only (chunk-group aligned).
    let (hash, aligned) = admit_span(&engine, &payload, 64 * 1024, 32 * 1024).await?;

    let pr = engine.present_ranges(hash).await?;
    assert!(!pr.is_complete(), "a middle span is not the whole blob");
    assert!(!pr.is_empty(), "the admitted span is present");
    // The present chunk ranges must cover the aligned span and not the whole blob.
    let cr = pr.chunk_ranges();
    // Byte→chunk-count via bao-tree's canonical conversion, not a hard-coded 1024.
    let group_chunks = bao_tree::ChunkNum::chunks(aligned.fetch_end()).0
        - bao_tree::ChunkNum::chunks(aligned.fetch_start()).0;
    assert!(group_chunks > 0);
    // Whole-blob chunk count is strictly greater than what we admitted.
    let whole_chunks = bao_tree::ChunkNum::chunks(blob_size).0;
    let present_chunk_count: u64 = cr
        .boundaries()
        .chunks(2)
        .filter_map(|w| match w {
            [a, b] => Some(b.0 - a.0),
            _ => None,
        })
        .sum();
    assert!(present_chunk_count < whole_chunks);
    assert!(present_chunk_count >= group_chunks);
    Ok(())
}

#[tokio::test]
async fn missing_ranges_empty_when_span_present() -> anyhow::Result<()> {
    let payload = util::make_blob(200 * 1024);
    let blob_size = u64::try_from(payload.len())?;
    let (engine, _tmp) = util::empty_engine().await?;

    let (req_start, req_len) = (64 * 1024, 32 * 1024);
    let (hash, _aligned) = admit_span(&engine, &payload, req_start, req_len).await?;

    // The exact span we admitted is now fully present → nothing missing.
    let missing = engine
        .missing_ranges(hash, req_start, req_len, blob_size)
        .await?;
    assert!(missing.is_empty());
    Ok(())
}

#[tokio::test]
async fn missing_ranges_covers_absent_span() -> anyhow::Result<()> {
    let payload = util::make_blob(200 * 1024);
    let blob_size = u64::try_from(payload.len())?;
    let (engine, _tmp) = util::empty_engine().await?;

    // Admit an early span; ask about a disjoint later span.
    let (hash, _aligned) = admit_span(&engine, &payload, 0, 32 * 1024).await?;
    let missing = engine
        .missing_ranges(hash, 128 * 1024, 32 * 1024, blob_size)
        .await?;
    assert!(!missing.is_empty(), "the later span was never admitted");
    Ok(())
}

#[tokio::test]
async fn missing_ranges_empty_on_complete_blob() -> anyhow::Result<()> {
    let payload = util::make_blob(200 * 1024);
    let blob_size = u64::try_from(payload.len())?;
    let (engine, hash, _tmp, _srv) = util::engine_with_whole_blob(&payload).await?;
    engine.populate(hash).await?; // whole-blob fill → Complete

    // A complete blob has every range present, so nothing is ever missing.
    let missing = engine.missing_ranges(hash, 0, 0, blob_size).await?;
    assert!(missing.is_empty());
    Ok(())
}

#[tokio::test]
async fn missing_ranges_absent_blob_is_whole_requested_span() -> anyhow::Result<()> {
    let payload = util::make_blob(200 * 1024);
    let blob_size = u64::try_from(payload.len())?;
    let (engine, _tmp) = util::empty_engine().await?;
    let hash = Hash::new(&payload);
    // len 0 = to end.
    let missing = engine.missing_ranges(hash, 0, 0, blob_size).await?;
    assert!(!missing.is_empty());
    Ok(())
}

/// Admit the chunk-group-aligned span `[off, off + len)` of `payload` as a verified
/// partial blob, the way a pull leg's admission does, without any origin.
async fn admit_span(
    engine: &decdn_cache::CacheEngine,
    payload: &[u8],
    off: u64,
    len: u64,
) -> anyhow::Result<(Hash, decdn_cache::range_pull::AlignedRange)> {
    let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(
        payload,
        decdn_cache::range_pull::IROH_BLOCK_SIZE,
    );
    let hash = Hash::from_bytes(*ob.root.as_bytes());
    let total = u64::try_from(payload.len())?;
    let aligned = align_range(off, len, total)?;
    let slice = payload
        .get(usize::try_from(aligned.fetch_start())?..usize::try_from(aligned.fetch_end())?)
        .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?;
    let encoded = decdn_cache::range_pull::encode_verified_range(
        *hash.as_bytes(),
        &aligned,
        slice,
        bytes::Bytes::from(ob.data),
    )?;
    engine
        .admit_bao(hash, aligned.chunk_ranges().clone(), encoded)
        .await?;
    Ok((hash, aligned))
}
