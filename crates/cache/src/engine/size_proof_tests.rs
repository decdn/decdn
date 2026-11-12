//! [`CacheEngine::size_proof`]: every source the engine builds a proof from,
//! and the refusal of a size that the blob does not have.

use super::*;
use crate::origin::FilesystemOrigin;

/// A blob over several chunk groups with a short last group.
fn blob() -> Vec<u8> {
    (0..(5 * CHUNK_GROUP_BYTES + 777))
        .map(|i| (i % 251) as u8)
        .collect()
}

/// A filesystem origin seeded as `decdn origin import` seeds it, with or
/// without the `{H}.sizeproof` sibling.
async fn seeded_fs_origin(
    data: &[u8],
    with_proof: bool,
) -> anyhow::Result<(Hash, tempfile::TempDir, Arc<dyn Origin>)> {
    let dir = tempfile::tempdir()?;
    let size = u64::try_from(data.len())?;
    let eo = decdn_bao_range::encode_outboard(data, size)?;
    let shard = dir.path().join(eo.shard());
    std::fs::create_dir_all(&shard)?;
    std::fs::write(shard.join(&eo.hash_hex), data)?;
    std::fs::write(shard.join(eo.obao4_name()), &eo.outboard)?;
    if with_proof {
        std::fs::write(shard.join(eo.size_proof_name()), &eo.size_proof)?;
    }
    let hash = Hash::new(data);
    let origin = Arc::new(FilesystemOrigin::new(dir.path()).await?) as Arc<dyn Origin>;
    Ok((hash, dir, origin))
}

#[tokio::test]
async fn an_origin_stored_proof_proves_the_size() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let (hash, _dir, origin) = seeded_fs_origin(&data, true).await?;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 64).await?;
    let proof = engine.size_proof(hash, size).await;
    anyhow::ensure!(proof.is_some(), "the stored proof proves the size");
    anyhow::ensure!(engine.kept_size_proof(hash).map(|(s, _)| s) == Some(size));
    Ok(())
}

#[tokio::test]
async fn an_origin_without_a_stored_proof_builds_one() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let (hash, _dir, origin) = seeded_fs_origin(&data, false).await?;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 64).await?;
    let proof = engine
        .size_proof(hash, size)
        .await
        .ok_or_else(|| anyhow::anyhow!("the outboard and the tail build a proof"))?;
    anyhow::ensure!(decdn_bao_range::verify_size_proof(
        *hash.as_bytes(),
        size,
        &proof
    ));
    Ok(())
}

#[tokio::test]
async fn a_wrong_size_gets_no_proof() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let (hash, _dir, origin) = seeded_fs_origin(&data, true).await?;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 64).await?;
    for wrong in [size - 1, size + 1, 1 << 60] {
        anyhow::ensure!(
            engine.size_proof(hash, wrong).await.is_none(),
            "size {wrong} is not the size of the blob"
        );
    }
    // With the true proof kept, a wrong size still gets none.
    anyhow::ensure!(engine.size_proof(hash, size).await.is_some());
    anyhow::ensure!(engine.size_proof(hash, size + 1).await.is_none());
    Ok(())
}

#[tokio::test]
async fn a_complete_local_blob_proves_its_size_with_no_origin() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let (hash, _dir, origin) = seeded_fs_origin(&data, false).await?;
    let tmp = tempfile::tempdir()?;
    // Pull the blob into the store, then reopen the store with no origin.
    {
        let engine = CacheEngine::open(tmp.path(), vec![origin], 64).await?;
        let _ = engine.get(hash).await?;
        engine.shutdown().await?;
    }
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await?;
    anyhow::ensure!(engine.kept_size_proof(hash).is_none());
    anyhow::ensure!(engine.size_proof(hash, size).await.is_some());
    Ok(())
}

#[tokio::test]
async fn keep_size_proof_refuses_a_mismatched_proof() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let hash = Hash::new(&data);
    let proof = decdn_bao_range::encode_outboard(data.as_slice(), size)?.size_proof;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await?;
    anyhow::ensure!(!engine.keep_size_proof(hash, size + 1, &proof).await);
    anyhow::ensure!(
        !engine
            .keep_size_proof(Hash::new(b"other"), size, &proof)
            .await
    );
    anyhow::ensure!(!engine.keep_size_proof(hash, 1 << 60, &[]).await);
    anyhow::ensure!(engine.kept_size_proof(hash).is_none());
    anyhow::ensure!(engine.keep_size_proof(hash, size, &proof).await);
    anyhow::ensure!(engine.kept_size_proof(hash).map(|(s, _)| s) == Some(size));
    Ok(())
}

/// A proof kept from an upstream, for a partial blob that does not hold its
/// tail, survives a restart: the reopened engine, with no origin, still
/// proves the size, and proves no other.
#[tokio::test]
async fn a_kept_proof_survives_a_restart() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let hash = Hash::new(&data);
    let proof = decdn_bao_range::encode_outboard(data.as_slice(), size)?.size_proof;
    let tmp = tempfile::tempdir()?;
    {
        let engine = CacheEngine::open(tmp.path(), vec![], 64).await?;
        // Hold the first chunk group only, as a pull-through partial does.
        let first = crate::range_pull::align_range(0, CHUNK_GROUP_BYTES, size)?;
        let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(
            &data,
            crate::range_pull::IROH_BLOCK_SIZE,
        );
        let wire = crate::range_pull::encode_verified_range(
            *hash.as_bytes(),
            &first,
            data.get(..usize::try_from(CHUNK_GROUP_BYTES)?)
                .ok_or_else(|| anyhow::anyhow!("first group"))?,
            Bytes::from(ob.data),
        )?;
        engine
            .admit_bao(hash, first.chunk_ranges().clone(), wire)
            .await?;
        anyhow::ensure!(engine.keep_size_proof(hash, size, &proof).await);
        engine.shutdown().await?;
    }
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await?;
    anyhow::ensure!(
        engine.kept_size_proof(hash).is_none(),
        "memory starts empty"
    );
    anyhow::ensure!(
        engine.size_proof(hash, size).await.as_deref() == Some(proof.as_ref()),
        "the stored proof proves the size"
    );
    anyhow::ensure!(engine.size_proof(hash, size + 1).await.is_none());
    anyhow::ensure!(
        engine.sweep_orphan_size_proofs().await? == 0,
        "a held blob's proof is no orphan"
    );
    Ok(())
}

/// A proof kept ahead of a pull that never landed a byte is an orphan: the
/// sweep at open deletes it, and a later lookup has nothing to read.
#[tokio::test]
async fn the_sweep_deletes_a_proof_whose_blob_never_landed() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let hash = Hash::new(&data);
    let proof = decdn_bao_range::encode_outboard(data.as_slice(), size)?.size_proof;
    let tmp = tempfile::tempdir()?;
    {
        let engine = CacheEngine::open(tmp.path(), vec![], 64).await?;
        anyhow::ensure!(engine.keep_size_proof(hash, size, &proof).await);
        engine.shutdown().await?;
    }
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await?;
    anyhow::ensure!(engine.size_proof(hash, size).await.is_none());
    anyhow::ensure!(
        engine.sweep_orphan_size_proofs().await? == 0,
        "open already swept it"
    );
    Ok(())
}

/// Evicting a blob drops its stored proof tag with the blob's other tags.
#[tokio::test]
async fn evicting_a_blob_drops_its_stored_proof() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let hash = Hash::new(&data);
    let proof = decdn_bao_range::encode_outboard(data.as_slice(), size)?.size_proof;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await?;
    anyhow::ensure!(engine.keep_size_proof(hash, size, &proof).await);
    engine.release_for_eviction(hash).await?;
    anyhow::ensure!(engine.size_proof(hash, size).await.is_none());
    Ok(())
}

#[tokio::test]
async fn evicting_a_blob_drops_its_proof() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let (hash, _dir, origin) = seeded_fs_origin(&data, true).await?;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 64).await?;
    let _ = engine.get(hash).await?;
    anyhow::ensure!(engine.size_proof(hash, size).await.is_some());
    engine.release_for_eviction(hash).await?;
    anyhow::ensure!(
        engine.kept_size_proof(hash).is_none(),
        "LRU release drops it"
    );
    anyhow::ensure!(engine.size_proof(hash, size).await.is_some());
    engine.evict(hash).await?;
    anyhow::ensure!(engine.kept_size_proof(hash).is_none(), "evict drops it");
    Ok(())
}
