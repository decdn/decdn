//! [`CacheEngine::size_proof`]: every source the engine builds a proof from,
//! and the refusal of a size that the blob does not have; and
//! [`CacheEngine::origin_presence`], the origin's `{H}.sizeproof` read that
//! answers presence and keeps the proof.

use super::*;
use crate::CHUNK_GROUP_BYTES;
use crate::origin::FilesystemOrigin;
use std::sync::atomic::AtomicUsize;

/// What a [`ProofReads`] origin answers to every `{H}.sizeproof` read.
#[derive(Debug, Clone)]
enum ProofAnswer {
    /// No object: the origin does not hold the blob.
    Missing,
    /// A transport fault.
    Down,
    /// This `{H}.sizeproof` object.
    Object(Bytes),
}

/// An origin that answers every `{H}.sizeproof` read with `answer`, after
/// `delay`. It counts the reads.
#[derive(Debug)]
struct ProofReads {
    reads: AtomicUsize,
    answer: ProofAnswer,
    delay: Duration,
}

impl ProofReads {
    fn new(answer: ProofAnswer) -> Arc<Self> {
        Self::slow(answer, Duration::ZERO)
    }

    fn slow(answer: ProofAnswer, delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            reads: AtomicUsize::new(0),
            answer,
            delay,
        })
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl Origin for ProofReads {
    fn fetch_size_proof(
        &self,
        _hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, OriginPullError>> + Send + '_>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let answer = self.answer.clone();
        let delay = self.delay;
        Box::pin(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            match answer {
                ProofAnswer::Missing => Ok(None),
                ProofAnswer::Down => {
                    Err(OriginPullError::Transient(anyhow::anyhow!("origin down")))
                }
                ProofAnswer::Object(object) => Ok(Some(object)),
            }
        })
    }

    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }
}

/// The `{H}.sizeproof` object `decdn origin import` writes for `data`.
fn size_proof_object(data: &[u8]) -> anyhow::Result<Bytes> {
    let size = u64::try_from(data.len())?;
    Ok(Bytes::from(
        decdn_bao_range::encode_outboard(data, size)?.size_proof_object(),
    ))
}

/// An engine over the one origin `origin`.
async fn engine_over(origin: Arc<ProofReads>) -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 64).await?;
    Ok((engine, tmp))
}

/// A verifying `{H}.sizeproof` makes the blob present at the size it states,
/// and keeps the proof, so `size_proof` of that size reads no origin.
#[tokio::test]
async fn a_verifying_size_proof_is_present_and_kept() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let hash = Hash::new(&data);
    let origin = ProofReads::new(ProofAnswer::Object(size_proof_object(&data)?));
    let (engine, _tmp) = engine_over(Arc::clone(&origin)).await?;
    anyhow::ensure!(engine.origin_presence(hash).await == OriginPresence::Present(size));
    anyhow::ensure!(origin.reads() == 1);
    anyhow::ensure!(engine.kept_size_proof(hash).map(|(s, _)| s) == Some(size));
    anyhow::ensure!(engine.size_proof(hash, size).await.is_some());
    anyhow::ensure!(
        origin.reads() == 1,
        "the kept proof answers size_proof with no origin read"
    );
    anyhow::ensure!(engine.size_proof(hash, size + 1).await.is_none());
    Ok(())
}

/// The fs origin answers presence from the `{H}.sizeproof` that
/// `decdn origin import` writes.
#[tokio::test]
async fn an_fs_origin_is_present_from_its_size_proof() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let (hash, _dir, origin) = seeded_fs_origin(&data, true).await?;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 64).await?;
    anyhow::ensure!(engine.origin_presence(hash).await == OriginPresence::Present(size));

    let (bare, _bare_dir, bare_origin) = seeded_fs_origin(&data, false).await?;
    let bare_tmp = tempfile::tempdir()?;
    let bare_engine = CacheEngine::open(bare_tmp.path(), vec![bare_origin], 64).await?;
    anyhow::ensure!(
        bare_engine.origin_presence(bare).await == OriginPresence::Absent,
        "data with no size proof is not present"
    );
    Ok(())
}

/// No `{H}.sizeproof` anywhere, or no origin at all, is `Absent`, and the
/// absence is memoised.
#[tokio::test]
async fn no_size_proof_is_absent_and_memoised() -> anyhow::Result<()> {
    let hash = Hash::new(b"absent");
    let origin = ProofReads::new(ProofAnswer::Missing);
    let (engine, _tmp) = engine_over(Arc::clone(&origin)).await?;
    anyhow::ensure!(engine.origin_presence(hash).await == OriginPresence::Absent);
    anyhow::ensure!(engine.origin_presence(hash).await == OriginPresence::Absent);
    anyhow::ensure!(origin.reads() == 1, "the memo answers the repeat");
    anyhow::ensure!(engine.size_proof(hash, 9000).await.is_none());
    anyhow::ensure!(origin.reads() == 1, "size_proof takes the memoised absence");

    let tmp = tempfile::tempdir()?;
    let bare = CacheEngine::open(tmp.path(), vec![], 64).await?;
    anyhow::ensure!(bare.origin_presence(hash).await == OriginPresence::Absent);
    Ok(())
}

/// An adapter error is a `Fault`, memoised under the fault TTL. The read
/// runs once: the memo is the retry.
#[tokio::test]
async fn an_adapter_error_is_a_fault() -> anyhow::Result<()> {
    let hash = Hash::new(b"outage");
    let origin = ProofReads::new(ProofAnswer::Down);
    let (engine, _tmp) = engine_over(Arc::clone(&origin)).await?;
    anyhow::ensure!(engine.origin_presence(hash).await == OriginPresence::Fault);
    anyhow::ensure!(origin.reads() == 1, "the size proof read is not retried");
    anyhow::ensure!(engine.size_proof(hash, 9000).await.is_none());
    anyhow::ensure!(origin.reads() == 1, "the fault is memoised");
    Ok(())
}

/// An origin that answers an outage fails fast, so the walk reaches the next
/// origin, which proves the blob present.
#[tokio::test]
async fn a_size_proof_outage_falls_through_to_the_next_origin() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let hash = Hash::new(&data);
    let down = ProofReads::new(ProofAnswer::Down);
    let good = ProofReads::new(ProofAnswer::Object(size_proof_object(&data)?));
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(
        tmp.path(),
        vec![
            Arc::clone(&down) as Arc<dyn Origin>,
            Arc::clone(&good) as Arc<dyn Origin>,
        ],
        64,
    )
    .await?;
    anyhow::ensure!(engine.origin_presence(hash).await == OriginPresence::Present(size));
    anyhow::ensure!(
        down.reads() == 1,
        "the outage is read once: {}",
        down.reads()
    );
    Ok(())
}

/// An outage on the size proof is a transient fault, so the rescan carries
/// the hash forward rather than drop it.
#[tokio::test]
async fn a_size_proof_outage_is_a_transient_fault() -> anyhow::Result<()> {
    let hash = Hash::new(b"long-outage");
    let origin = ProofReads::new(ProofAnswer::Down);
    let (engine, _tmp) = engine_over(Arc::clone(&origin)).await?;
    let probe = engine
        .probe_origin_chain(hash, Duration::from_secs(30))
        .await;
    anyhow::ensure!(probe.presence == OriginPresence::Fault, "{probe:?}");
    anyhow::ensure!(probe.transient, "an outage stays transient: {probe:?}");
    Ok(())
}

/// A breaker that trips after one unavailable read and stays open.
const fn one_strike_breaker() -> CircuitBreakerPolicy {
    CircuitBreakerPolicy {
        enabled: true,
        failure_threshold: 1,
        cooldown_ms: 600_000,
        half_open_max_calls: 1,
    }
}

/// An engine over `origins` whose breakers trip on one unavailable read.
async fn one_strike_engine(
    origins: Vec<Arc<dyn Origin>>,
) -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open_full(
        tmp.path(),
        origins,
        64,
        PinnedHashes::empty(),
        RetryPolicy::default(),
        one_strike_breaker(),
        None,
        Duration::ZERO,
    )
    .await?;
    Ok((engine, tmp))
}

/// The state of the breaker of the origin at `ix`.
fn breaker_state(engine: &CacheEngine, ix: usize) -> Option<crate::circuit_breaker::BreakerState> {
    engine.inner.breakers.get(ix).map(|b| b.state())
}

/// A later origin that the walk cuts off at its ceiling had only the time an
/// earlier origin left, so its breaker is not charged. The walk names the
/// earlier origin's cause.
#[tokio::test]
async fn a_later_origin_cut_off_by_the_walk_ceiling_is_not_charged() -> anyhow::Result<()> {
    let data = blob();
    let hash = Hash::new(&data);
    let slow_down = ProofReads::slow(ProofAnswer::Down, Duration::from_millis(60));
    let slow_good = ProofReads::slow(
        ProofAnswer::Object(size_proof_object(&data)?),
        Duration::from_millis(400),
    );
    let (engine, _tmp) = one_strike_engine(vec![
        slow_down as Arc<dyn Origin>,
        Arc::clone(&slow_good) as Arc<dyn Origin>,
    ])
    .await?;
    let probe = engine
        .probe_origin_chain(hash, Duration::from_millis(100))
        .await;
    anyhow::ensure!(probe.presence == OriginPresence::Fault, "{probe:?}");
    anyhow::ensure!(probe.transient, "{probe:?}");
    anyhow::ensure!(
        probe
            .cause
            .as_deref()
            .is_some_and(|c| c.contains("origin down")),
        "the earlier origin's cause stands: {probe:?}"
    );
    anyhow::ensure!(slow_good.reads() == 1, "the later origin was reached");
    anyhow::ensure!(
        breaker_state(&engine, 1) == Some(crate::circuit_breaker::BreakerState::Closed),
        "a later origin cut off by the ceiling is not charged"
    );
    Ok(())
}

/// A `{H}.sizeproof` that does not verify — a corrupt proof, a wrong stated
/// size, or an object too short to parse — is a non-transient `Fault`, never
/// a presence.
#[tokio::test]
async fn a_size_proof_that_does_not_verify_is_a_permanent_fault() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let hash = Hash::new(&data);
    let good = size_proof_object(&data)?;
    let mut corrupt = good.to_vec();
    if let Some(last) = corrupt.last_mut() {
        *last ^= 0xff;
    }
    let mut wrong_size = good.to_vec();
    wrong_size
        .get_mut(..8)
        .ok_or_else(|| anyhow::anyhow!("size field"))?
        .copy_from_slice(&(size + 1).to_le_bytes());
    for object in [corrupt, wrong_size, vec![1, 2, 3]] {
        let origin = ProofReads::new(ProofAnswer::Object(Bytes::from(object)));
        let (engine, _tmp) = engine_over(Arc::clone(&origin)).await?;
        let probe = engine
            .probe_origin_chain(hash, Duration::from_secs(5))
            .await;
        anyhow::ensure!(probe.presence == OriginPresence::Fault, "{probe:?}");
        anyhow::ensure!(!probe.transient, "a broken object is no transient fault");
        anyhow::ensure!(
            probe
                .cause
                .as_deref()
                .is_some_and(|c| c.contains("decdn origin import")),
            "{probe:?}"
        );
        anyhow::ensure!(engine.origin_presence(hash).await == OriginPresence::Fault);
        anyhow::ensure!(engine.kept_size_proof(hash).is_none());
    }
    Ok(())
}

/// A later origin's verifying proof wins over an earlier origin's fault.
#[tokio::test]
async fn a_verifying_proof_wins_over_a_fault() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let hash = Hash::new(&data);
    let down = ProofReads::new(ProofAnswer::Down);
    let good = ProofReads::new(ProofAnswer::Object(size_proof_object(&data)?));
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(
        tmp.path(),
        vec![down as Arc<dyn Origin>, good as Arc<dyn Origin>],
        64,
    )
    .await?;
    anyhow::ensure!(engine.origin_presence(hash).await == OriginPresence::Present(size));
    Ok(())
}

/// A present answer is memoised: a second call reads no origin.
#[tokio::test]
async fn a_present_answer_is_memoised() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let hash = Hash::new(&data);
    let origin = ProofReads::new(ProofAnswer::Object(size_proof_object(&data)?));
    let (engine, _tmp) = engine_over(Arc::clone(&origin)).await?;
    for _ in 0..3 {
        anyhow::ensure!(engine.origin_presence(hash).await == OriginPresence::Present(size));
    }
    anyhow::ensure!(origin.reads() == 1);
    Ok(())
}

/// Concurrent calls on a cold hash share one origin read.
#[tokio::test]
async fn concurrent_cold_calls_read_the_origin_once() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let hash = Hash::new(&data);
    let origin = ProofReads::slow(
        ProofAnswer::Object(size_proof_object(&data)?),
        Duration::from_millis(100),
    );
    let (engine, _tmp) = engine_over(Arc::clone(&origin)).await?;
    let (a, b, c) = tokio::join!(
        engine.origin_presence(hash),
        engine.origin_presence(hash),
        engine.size_proof(hash, size),
    );
    anyhow::ensure!(a == OriginPresence::Present(size));
    anyhow::ensure!(b == OriginPresence::Present(size));
    anyhow::ensure!(c.is_some());
    anyhow::ensure!(origin.reads() == 1, "reads: {}", origin.reads());
    Ok(())
}

/// A refused hash is absent with no origin read.
#[tokio::test]
async fn a_refused_hash_reads_no_origin() -> anyhow::Result<()> {
    let data = blob();
    let hash = Hash::new(&data);
    let origin = ProofReads::new(ProofAnswer::Object(size_proof_object(&data)?));
    let (engine, _tmp) = engine_over(Arc::clone(&origin)).await?;
    engine.set_chain_denied_one(hash, true);
    anyhow::ensure!(engine.origin_presence(hash).await == OriginPresence::Absent);
    anyhow::ensure!(origin.reads() == 0);
    Ok(())
}

/// A walk slower than the probe ceiling is a transient `Fault`.
#[tokio::test]
async fn a_walk_past_the_ceiling_is_a_fault() -> anyhow::Result<()> {
    let data = blob();
    let hash = Hash::new(&data);
    let origin = ProofReads::slow(
        ProofAnswer::Object(size_proof_object(&data)?),
        Duration::from_millis(400),
    );
    let (engine, _tmp) = one_strike_engine(vec![Arc::clone(&origin) as Arc<dyn Origin>]).await?;
    engine.set_origin_probe_config(OriginProbePolicy {
        timeout: Duration::from_millis(20),
        ..OriginProbePolicy::default()
    });
    anyhow::ensure!(engine.origin_presence(hash).await == OriginPresence::Fault);
    anyhow::ensure!(engine.origin_presence(hash).await == OriginPresence::Fault);
    anyhow::ensure!(origin.reads() == 1, "the fault is memoised");
    anyhow::ensure!(
        breaker_state(&engine, 0) == Some(crate::circuit_breaker::BreakerState::Open),
        "the first origin had the whole ceiling, so its stall is charged"
    );
    Ok(())
}

/// The kept proof answers `size_proof` while the presence memo stands; once
/// the size-proof memo drops it, `size_proof` reads the origin again.
#[tokio::test]
async fn a_dropped_proof_is_read_again() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let hash = Hash::new(&data);
    let origin = ProofReads::new(ProofAnswer::Object(size_proof_object(&data)?));
    let (engine, _tmp) = engine_over(Arc::clone(&origin)).await?;
    anyhow::ensure!(engine.origin_presence(hash).await == OriginPresence::Present(size));
    engine.inner.size_proofs.remove(&hash);
    anyhow::ensure!(engine.size_proof(hash, size).await.is_some());
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
    anyhow::ensure!(engine.insert_blob(data.clone()).await? == hash);
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
        std::fs::write(shard.join(eo.size_proof_name()), eo.size_proof_object())?;
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
/// states none for a blob it does not hold.
#[tokio::test]
async fn an_origin_without_a_stored_proof_proves_nothing() -> anyhow::Result<()> {
    let data = blob();
    let size = u64::try_from(data.len())?;
    let (hash, _dir, origin) = seeded_fs_origin(&data, false).await?;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 64).await?;
    anyhow::ensure!(engine.size_proof(hash, size).await.is_none());
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
    anyhow::ensure!(engine.insert_blob(data.clone()).await? == hash);
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
    let hash = Hash::new(&data);
    let tmp = tempfile::tempdir()?;
    // Put the blob into the store, then reopen the store with no origin.
    {
        let engine = CacheEngine::open(tmp.path(), vec![], 64).await?;
        anyhow::ensure!(engine.insert_blob(data.clone()).await? == hash);
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
    anyhow::ensure!(engine.insert_blob(data.clone()).await? == hash);
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
    anyhow::ensure!(engine.insert_blob(data.clone()).await? == hash);
    anyhow::ensure!(engine.size_proof(hash, size).await.is_some());
    anyhow::ensure!(proof_tags(&engine, hash).await? == [Hash::EMPTY]);
    engine.evict(Hash::EMPTY).await?;
    anyhow::ensure!(
        proof_tags(&engine, hash).await? == [Hash::EMPTY],
        "the proof of another blob survives"
    );
    Ok(())
}

/// A rescan that lists an object with no `{H}.sizeproof` does not advertise
/// it, and counts it: the listing proves the data exists, so the publish is
/// broken, not absent.
#[tokio::test]
async fn a_listed_object_without_a_size_proof_is_counted() -> anyhow::Result<()> {
    let data = blob();
    let (hash, _dir, origin) = seeded_fs_origin(&data, false).await?;
    let tmp = tempfile::tempdir()?;
    let metrics = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![origin],
        64,
        PinnedHashes::empty(),
        RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&metrics)),
        Duration::ZERO,
    )
    .await?;
    engine.rescan_origins().await;
    anyhow::ensure!(engine.origin_held_size(hash).is_none());
    anyhow::ensure!(metrics.size_proof_origin_missing.get() == 1);
    anyhow::ensure!(engine.origin_held_snapshot().probe_faults == 0);
    Ok(())
}

/// A pin can name a hash no origin holds, so a pinned-only hash with no size
/// proof is absent, not a broken publish: it is not counted.
#[tokio::test]
async fn a_pinned_only_hash_without_a_size_proof_is_not_counted() -> anyhow::Result<()> {
    let hash = Hash::new(b"pinned-elsewhere");
    let origin = ProofReads::new(ProofAnswer::Missing);
    let tmp = tempfile::tempdir()?;
    let metrics = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![origin as Arc<dyn Origin>],
        64,
        PinnedHashes::new([from_store_hash(hash)].into_iter().collect()),
        RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&metrics)),
        Duration::ZERO,
    )
    .await?;
    engine.rescan_origins().await;
    anyhow::ensure!(engine.origin_held_size(hash).is_none());
    anyhow::ensure!(metrics.size_proof_origin_missing.get() == 0);
    Ok(())
}
