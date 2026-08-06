//! Integration test for [`decdn_cache::CacheEngine::admit_bao`] — the thin
//! wrapper over `import_bao_bytes` that lets a caller admit an
//! already-verified interleaved bao encoding without reaching into the
//! private store handle (Task 3, #1621).

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
