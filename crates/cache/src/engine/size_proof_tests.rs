//! [`CacheEngine::size_proof`]: every source the engine builds a proof from,
//! and the refusal of a size that the blob does not have.

use super::*;
use crate::origin::{FilesystemOrigin, OriginFetch};

/// An origin that holds no object and answers every `{H}.sizeproof` read
/// with no proof, or with a transport fault when `down`. It counts the reads.
#[derive(Debug)]
struct ProofReads {
    reads: AtomicUsize,
    down: bool,
}

impl ProofReads {
    fn new(down: bool) -> Arc<Self> {
        Arc::new(Self {
            reads: AtomicUsize::new(0),
            down,
        })
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl Origin for ProofReads {
    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async { Ok(OriginFetch::NotFound) })
    }

    fn fetch_size_proof(
        &self,
        _hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, OriginPullError>> + Send + '_>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let down = self.down;
        Box::pin(async move {
            if down {
                Err(OriginPullError::Transient(anyhow::anyhow!("origin down")))
            } else {
                Ok(None)
            }
        })
    }

    fn kind(&self) -> OriginKind {
        OriginKind::Filesystem
    }
}

/// A lookup every origin answers with no proof is remembered: a repeat
/// within the miss window reads no origin, and one after it reads again.
#[tokio::test]
async fn a_proof_miss_is_remembered_for_its_window() -> anyhow::Result<()> {
    let origin = ProofReads::new(false);
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin.clone()], 64).await?;
    let (hash, size) = (Hash::new(b"unproven"), 9000);
    anyhow::ensure!(engine.size_proof(hash, size).await.is_none());
    anyhow::ensure!(engine.size_proof(hash, size).await.is_none());
    anyhow::ensure!(origin.reads() == 1, "the miss memo answers the repeat");
    // Age the miss past its window.
    let aged = Instant::now()
        .checked_sub(SIZE_PROOF_MISS_TTL + Duration::from_secs(1))
        .ok_or_else(|| anyhow::anyhow!("clock too young"))?;
    engine
        .inner
        .size_proof_misses
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert((hash, size), aged);
    anyhow::ensure!(engine.size_proof(hash, size).await.is_none());
    anyhow::ensure!(
        origin.reads() == 2,
        "an expired miss reads the origin again"
    );
    Ok(())
}

/// An origin outage is no answer: the next lookup reads the origin again.
#[tokio::test]
async fn an_origin_outage_is_not_remembered_as_a_miss() -> anyhow::Result<()> {
    let origin = ProofReads::new(true);
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin.clone()], 64).await?;
    let (hash, size) = (Hash::new(b"outage"), 9000);
    anyhow::ensure!(engine.size_proof(hash, size).await.is_none());
    anyhow::ensure!(!engine.size_proof_missed(hash, size));
    anyhow::ensure!(engine.size_proof(hash, size).await.is_none());
    anyhow::ensure!(origin.reads() == 2);
    Ok(())
}

/// The sweep deletes the proof tag of a blob the store no longer tracks,
/// and deletes nothing when the open-time recency seed failed.
#[tokio::test]
async fn the_sweep_deletes_the_proof_of_an_untracked_blob() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let (hash, _dir, origin) = seeded_fs_origin(&data, true).await?;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 64).await?;
    let _ = engine.get(hash).await?;
    anyhow::ensure!(engine.size_proof(hash, size).await.is_some());
    anyhow::ensure!(proof_tags(&engine, hash).await? == [Hash::EMPTY]);
    // The store stops tracking the blob, as after a GC reclaim.
    engine.inner.access_times.remove(&hash);
    engine.inner.recency_seeded.store(false, Ordering::Release);
    anyhow::ensure!(
        engine.sweep_orphan_size_proofs().await? == 0,
        "no seed, no sweep"
    );
    anyhow::ensure!(proof_tags(&engine, hash).await? == [Hash::EMPTY]);
    engine.inner.recency_seeded.store(true, Ordering::Release);
    anyhow::ensure!(engine.sweep_orphan_size_proofs().await? == 1);
    anyhow::ensure!(proof_tags(&engine, hash).await?.is_empty());
    Ok(())
}

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

/// An origin object with no `{H}.sizeproof` proves no size, so the node
/// states none for a blob it does not hold; a repeat lookup within the miss
/// memo reads the origin no more.
#[tokio::test]
async fn an_origin_without_a_stored_proof_proves_nothing() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let (hash, _dir, origin) = seeded_fs_origin(&data, false).await?;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 64).await?;
    anyhow::ensure!(engine.size_proof(hash, size).await.is_none());
    anyhow::ensure!(engine.size_proof_missed(hash, size), "the miss is memoized");
    anyhow::ensure!(engine.size_proof(hash, size).await.is_none());
    Ok(())
}

/// The names of the size proof tags the store keeps for `hash`, each with
/// the hash its tag points at.
async fn proof_tags(engine: &CacheEngine, hash: Hash) -> anyhow::Result<Vec<Hash>> {
    let prefix = size_proof_tag_prefix(hash);
    let mut tags = engine.inner.store.tags().list_prefix(&prefix).await?;
    let mut found = Vec::new();
    while let Some(info) = tags.next().await {
        found.push(info?.hash);
    }
    Ok(found)
}

/// A wrong size gets no proof from a held complete blob, nor from a partial
/// that lacks its tail, and neither lookup quarantines the blob.
#[tokio::test]
async fn a_wrong_size_of_a_held_blob_gets_no_proof_and_no_quarantine() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let (hash, _dir, origin) = seeded_fs_origin(&data, false).await?;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 64).await?;
    let _ = engine.get(hash).await?;
    for wrong in [size - 1, size + 1, 1 << 60] {
        anyhow::ensure!(engine.size_proof(hash, wrong).await.is_none(), "{wrong}");
    }
    anyhow::ensure!(!engine.is_quarantined(hash));
    anyhow::ensure!(engine.size_proof(hash, size).await.is_some());

    let partial_tmp = tempfile::tempdir()?;
    let partial = CacheEngine::open(partial_tmp.path(), vec![], 64).await?;
    hold_first_group(&partial, &data, hash).await?;
    for wrong in [size - 1, size, size + 1] {
        anyhow::ensure!(partial.size_proof(hash, wrong).await.is_none(), "{wrong}");
    }
    anyhow::ensure!(!partial.is_quarantined(hash));
    Ok(())
}

/// Admit the first chunk group of `data` into `engine`, as a pull-through
/// partial does.
async fn hold_first_group(engine: &CacheEngine, data: &[u8], hash: Hash) -> anyhow::Result<()> {
    let size = u64::try_from(data.len())?;
    let first = crate::range_pull::align_range(0, CHUNK_GROUP_BYTES, size)?;
    let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(
        data,
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
        // A pull-through keeps the upstream's proof before its first bytes land.
        anyhow::ensure!(engine.keep_size_proof(hash, size, &proof).await);
        anyhow::ensure!(proof_tags(&engine, hash).await?.is_empty());
        hold_first_group(&engine, &data, hash).await?;
        anyhow::ensure!(
            proof_tags(&engine, hash).await? == [Hash::EMPTY],
            "one tag, which roots no content"
        );
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

/// A proof kept for a blob the store does not hold stays in memory: no tag
/// is written, so nothing outlives a restart.
#[tokio::test]
async fn a_proof_for_an_unheld_blob_stays_in_memory() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let hash = Hash::new(&data);
    let proof = decdn_bao_range::encode_outboard(data.as_slice(), size)?.size_proof;
    let tmp = tempfile::tempdir()?;
    {
        let engine = CacheEngine::open(tmp.path(), vec![], 64).await?;
        anyhow::ensure!(engine.keep_size_proof(hash, size, &proof).await);
        anyhow::ensure!(engine.size_proof(hash, size).await.is_some());
        anyhow::ensure!(proof_tags(&engine, hash).await?.is_empty());
        engine.shutdown().await?;
    }
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await?;
    anyhow::ensure!(engine.size_proof(hash, size).await.is_none());
    anyhow::ensure!(engine.sweep_orphan_size_proofs().await? == 0);
    Ok(())
}

/// Releasing a held blob for eviction drops its proof from memory and from
/// the store. A later lookup reads the origin's `{H}.sizeproof` again, and
/// writes no tag for the blob the store no longer tracks, so nothing keeps
/// the released blob's content alive.
#[tokio::test]
async fn evicting_a_blob_drops_its_proof() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let (hash, _dir, origin) = seeded_fs_origin(&data, true).await?;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 64).await?;
    let _ = engine.get(hash).await?;
    anyhow::ensure!(engine.size_proof(hash, size).await.is_some());
    anyhow::ensure!(proof_tags(&engine, hash).await? == [Hash::EMPTY]);
    engine.release_for_eviction(hash).await?;
    anyhow::ensure!(
        engine.kept_size_proof(hash).is_none(),
        "LRU release drops it"
    );
    anyhow::ensure!(proof_tags(&engine, hash).await?.is_empty());
    anyhow::ensure!(engine.size_proof(hash, size).await.is_some());
    anyhow::ensure!(
        proof_tags(&engine, hash).await?.is_empty(),
        "a released blob gets no tag back"
    );
    engine.evict(hash).await?;
    anyhow::ensure!(engine.kept_size_proof(hash).is_none(), "evict drops it");
    Ok(())
}

/// Every proof tag points at the empty blob, so evicting the empty blob must
/// leave the proofs of other blobs in the store.
#[tokio::test]
async fn evicting_the_empty_blob_keeps_other_proofs() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let (hash, _dir, origin) = seeded_fs_origin(&data, true).await?;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 64).await?;
    let _ = engine.get(hash).await?;
    anyhow::ensure!(engine.size_proof(hash, size).await.is_some());
    anyhow::ensure!(proof_tags(&engine, hash).await? == [Hash::EMPTY]);
    engine.evict(Hash::EMPTY).await?;
    anyhow::ensure!(
        proof_tags(&engine, hash).await? == [Hash::EMPTY],
        "the proof of another blob survives"
    );
    Ok(())
}
