//! Integration test for [`decdn_cache::CacheEngine::admit_bao`] — the thin
//! wrapper over `import_bao_bytes` that lets a caller admit an
//! already-verified interleaved bao encoding without reaching into the
//! private store handle (#1621).

use bao_tree::io::outboard::PreOrderMemOutboard;
use decdn_cache::range_pull::{IROH_BLOCK_SIZE, align_range, encode_verified_range};
use iroh_blobs::Hash;

mod util;

#[tokio::test]
async fn admit_bao_imports_verified_range() -> anyhow::Result<()> {
    let payload = util::make_blob(200 * 1024);
    let blob_size = u64::try_from(payload.len())?;
    let ob = PreOrderMemOutboard::create(&payload, IROH_BLOCK_SIZE);
    let root: [u8; 32] = *ob.root.as_bytes();
    let hash = Hash::from(root);
    let outboard = bytes::Bytes::from(ob.data);
    let aligned = align_range(0, 0, blob_size)?; // whole blob
    let bao = encode_verified_range(root, &aligned, &payload, outboard)?;

    let (engine, _tmp) = util::empty_engine().await?;
    engine
        .admit_bao(hash, aligned.chunk_ranges().clone(), bao)
        .await?;
    assert!(engine.present_ranges(hash).await?.is_complete());
    Ok(())
}

/// Two live fills whose covered ranges overlap (the #2062 non-coalesced shape:
/// a whole-blob fill plus a tail fill the registry refused to attach) admit the
/// SAME chunk groups concurrently. The store must accept both idempotently and
/// end byte-exact — the safety half of the duplicate-egress trade.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_same_group_admits_are_idempotent() -> anyhow::Result<()> {
    let payload = util::make_blob(200 * 1024);
    let blob_size = u64::try_from(payload.len())?;
    let ob = PreOrderMemOutboard::create(&payload, IROH_BLOCK_SIZE);
    let root: [u8; 32] = *ob.root.as_bytes();
    let hash = Hash::from(root);
    let outboard = bytes::Bytes::from(ob.data);

    // The whole blob, and a tail overlapping its second half — both encodings
    // carry the tail's chunk groups.
    let whole = align_range(0, 0, blob_size)?;
    let tail = align_range(96 * 1024, 0, blob_size)?;
    let whole_bao = encode_verified_range(root, &whole, &payload, outboard.clone())?;
    let tail_slice = payload
        .get(usize::try_from(tail.fetch_start())?..)
        .ok_or_else(|| anyhow::anyhow!("tail out of bounds"))?;
    let tail_bao = encode_verified_range(root, &tail, tail_slice, outboard)?;

    let (engine, _tmp) = util::empty_engine().await?;
    let (e1, e2) = (engine.clone(), engine.clone());
    let whole_ranges = whole.chunk_ranges().clone();
    let tail_ranges = tail.chunk_ranges().clone();
    let (a, b) = tokio::join!(
        tokio::spawn(async move { e1.admit_bao(hash, whole_ranges, whole_bao).await }),
        tokio::spawn(async move { e2.admit_bao(hash, tail_ranges, tail_bao).await }),
    );
    a??;
    b??;

    assert!(engine.present_ranges(hash).await?.is_complete());
    let got = engine.get(hash).await?;
    assert_eq!(
        &got[..],
        &payload[..],
        "concurrent overlapping admits stay byte-exact"
    );
    Ok(())
}
