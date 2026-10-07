use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::Bytes;
use decdn_bao_range::IROH_BLOCK_SIZE;
use decdn_cache::{
    CacheEngine, FillError, FillSession, Hash, Origin, OriginFetch, OriginKind, OriginPullError,
    OriginRangeFetch, OriginRangeRequest, OutboardFetch,
};
use decdn_client::{Cumulative, PoolLedger};
use tokio_util::sync::CancellationToken;

use super::BackendSource;
use super::run_local_pull_leg;

/// What a [`FakeOrigin`] does when its range is fetched.
#[derive(Clone, Copy)]
enum Mode {
    /// Serve the genuine bytes for `H` — a healthy own origin.
    Serve,
    /// Return a transport error from `fetch_range_data` — an origin the node cannot
    /// reach (the no-hang-on-fault case).
    Fault,
    /// Serve a multi-window blob's first window, then return a transport error
    /// — an origin that fails after the wire has started streaming.
    FaultMidStream,
    /// Serve length-matching bytes that do NOT hash to `H` — a
    /// corrupt/misconfigured own origin (the local-verify case).
    Corrupt,
}

/// A minimal own-origin double serving one blob's aligned ranges + its
/// `{H}.obao4` outboard, parameterized by [`Mode`]. Twin of the `FakeOrigin` in
/// `backend_source.rs`'s tests (kept local — test doubles don't cross module
/// test boundaries).
#[derive(Debug)]
struct FakeOrigin {
    hash: Hash,
    data: Bytes,
    outboard: Bytes,
    size: u64,
    mode: FakeMode,
}

// A non-Copy Debug shim so `FakeOrigin` can derive `Debug` (Mode is internal).
#[derive(Debug, Clone, Copy)]
enum FakeMode {
    Serve,
    Fault,
    /// Windows starting at or past this offset fail.
    FaultFrom(u64),
}

impl FakeOrigin {
    fn new(hash: Hash, data: &[u8], outboard: Bytes, mode: Mode) -> Self {
        // `Corrupt` is expressed by feeding mismatched `data` under `Serve`; only
        // `Fault` needs distinct fetch behaviour, so the stored mode is binary.
        let fake_mode = match mode {
            Mode::Serve | Mode::Corrupt => FakeMode::Serve,
            Mode::Fault => FakeMode::Fault,
            Mode::FaultMidStream => FakeMode::FaultFrom(decdn_cache::RANGE_PULL_WINDOW_BYTES),
        };
        Self {
            hash,
            data: Bytes::from(data.to_vec()),
            outboard,
            size: data.len() as u64,
            mode: fake_mode,
        }
    }
}

impl Origin for FakeOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch(
        &self,
        hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        let result = if hash == self.hash {
            Ok(OriginFetch::found_one_shot(self.data.clone()))
        } else {
            Ok(OriginFetch::NotFound)
        };
        Box::pin(async move { result })
    }

    fn size(
        &self,
        hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, OriginPullError>> + Send + '_>> {
        let out = (hash == self.hash).then_some(self.size);
        Box::pin(async move { Ok(out) })
    }

    fn fetch_outboard(
        &self,
        hash: Hash,
        _outboard_max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, OriginPullError>> + Send + '_>> {
        let result = if hash == self.hash {
            OutboardFetch::Found(self.outboard.clone())
        } else {
            OutboardFetch::NotFound
        };
        Box::pin(async move { Ok(result) })
    }

    fn fetch_range_data(
        &self,
        hash: Hash,
        req: OriginRangeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>> {
        let faults = match self.mode {
            FakeMode::Serve => false,
            FakeMode::Fault => true,
            FakeMode::FaultFrom(from) => req.fetch_start >= from,
        };
        if faults {
            return Box::pin(async {
                Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "simulated own-origin transport fault"
                )))
            });
        }
        let result = if hash == self.hash {
            let s = req.fetch_start as usize;
            let e = req.fetch_end as usize;
            match self.data.get(s..e) {
                Some(span) => OriginRangeFetch::Ranged {
                    data: Bytes::copy_from_slice(span),
                },
                None => OriginRangeFetch::NotFound,
            }
        } else {
            OriginRangeFetch::Unsupported
        };
        Box::pin(async move { Ok(result) })
    }
}

/// A blob spanning several chunk groups plus a partial final group, so the bao
/// tree has real interior nodes.
fn test_blob() -> Vec<u8> {
    let size = 5 * decdn_cache::CHUNK_GROUP_BYTES as usize + 123;
    (0..size).map(|i| (i % 251) as u8).collect()
}

fn fresh_ledger() -> Arc<PoolLedger> {
    Arc::new(PoolLedger::new(Cumulative::default()))
}

/// Build an engine over one `FakeOrigin` in `mode`, plus the root/outboard/total
/// for the genuine blob. In `Corrupt` mode the origin serves `corrupt` bytes
/// (length-matched, different content) under the genuine `H`.
async fn engine_with_origin(
    mode: Mode,
) -> anyhow::Result<(CacheEngine, [u8; 32], u64, tempfile::TempDir)> {
    let data = match mode {
        // Past one window, so the fault lands after the wire has started.
        Mode::FaultMidStream => {
            let size = decdn_cache::RANGE_PULL_WINDOW_BYTES as usize
                + 5 * decdn_cache::CHUNK_GROUP_BYTES as usize
                + 123;
            (0..size).map(|i| (i % 251) as u8).collect()
        }
        Mode::Serve | Mode::Fault | Mode::Corrupt => test_blob(),
    };
    let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
    let root: [u8; 32] = *ob.root.as_bytes();
    let outboard = Bytes::from(ob.data.clone());
    let hash = Hash::from(root);
    let total = data.len() as u64;

    let served: Vec<u8> = match mode {
        Mode::Serve | Mode::Fault | Mode::FaultMidStream => data,
        Mode::Corrupt => data.iter().map(|b| b ^ 0xFF).collect(),
    };
    let origin = FakeOrigin::new(hash, &served, outboard, mode);
    let tmp = tempfile::tempdir()?;
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;
    Ok((engine, root, total, tmp))
}

/// Drive `run_local_pull_leg` to termination under a hard timeout — a hang (the
/// failure mode THE CRUX must rule out) surfaces as the timeout error rather
/// than wedging the test runner. `served_paid` is pre-advanced to `total` so the
/// downstream `RampPacer` never gates the pull (this test exercises the
/// completion path, not the window).
///
/// Returns the recorded `pull_result`. The leg sets `pull_result`
/// UNCONDITIONALLY on the line immediately before it fires `pull_ended` and
/// returns, so — because we await the leg to full completion — a `Some`
/// `pull_result` is the non-racy proof that the leg both terminated and fired
/// `pull_ended` (a fresh `notified()` here would miss the already-sent
/// `notify_waiters`, which stores no permit, so we do not watch the notify).
async fn run_to_termination(
    engine: &CacheEngine,
    root: [u8; 32],
    total: u64,
) -> anyhow::Result<Option<Result<(), FillError>>> {
    let hash = Hash::from(root);
    let source = BackendSource::new(engine.clone(), root, total, fresh_ledger());
    // Start the served frontier at `total` so the downstream `RampPacer` never
    // gates the pull (this test exercises the completion path, not the window).
    let session = FillSession::starting_at(bao_tree::blake3::Hash::from(root), total, total);

    let cancel = CancellationToken::new();
    let metrics = Arc::new(crate::metrics::Metrics::new());
    // A floor comfortably larger than the blob: `ramped_credit_window` never
    // returns below `floor`, so with `served_paid == total` the pull never waits,
    // regardless of divisor/credit_max — this only has to admit the whole gap.
    let credit_floor = total.saturating_mul(4).max(decdn_cache::CHUNK_GROUP_BYTES);

    tokio::time::timeout(
        Duration::from_secs(45),
        run_local_pull_leg(
            metrics,
            engine.clone(),
            source,
            hash,
            0,
            0,
            2,
            credit_floor,
            credit_floor,
            0,
            total,
            Arc::clone(&session),
            cancel,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("run_local_pull_leg HUNG — the CRUX failed to terminate"))?;

    // The leg records its terminal outcome unconditionally on its single exit, and
    // we awaited it to completion, so `outcome()` is the non-racy proof it ran.
    Ok(session.outcome())
}

/// (a) THE CRUX: a full-miss whole-blob local pull TERMINATES, fills the cache
/// byte-exact, and records `pull_result == Some(Ok(()))`.
#[tokio::test]
async fn local_pull_leg_full_miss_terminates_and_fills() -> anyhow::Result<()> {
    let (engine, root, total, _tmp) = engine_with_origin(Mode::Serve).await?;
    let hash = Hash::from(root);

    let result = run_to_termination(&engine, root, total)
        .await?
        .ok_or_else(|| anyhow::anyhow!("pull_result must be recorded (leg fired pull_ended)"))?;
    result.map_err(|e| anyhow::anyhow!("expected Ok, got Err: {e}"))?;

    // The cache now holds the whole blob, byte-exact.
    let want = test_blob();
    assert_eq!(
        engine.get(hash).await?.as_ref(),
        want.as_slice(),
        "the local pull must fill the cache byte-exact"
    );
    Ok(())
}

/// (b) A transport fault reaching the own origin records `pull_result ==
/// Some(Err(_))` and does NOT hang.
#[tokio::test]
async fn local_pull_leg_origin_fault_fails_without_hang() -> anyhow::Result<()> {
    let (engine, root, total, _tmp) = engine_with_origin(Mode::Fault).await?;

    let result = run_to_termination(&engine, root, total)
        .await?
        .ok_or_else(|| anyhow::anyhow!("pull_result must be recorded even on fault"))?;
    assert!(
        result.is_err(),
        "an origin transport fault must terminate the leg with Err"
    );
    Ok(())
}

/// (b2) A transport fault AFTER the wire has started streaming also records
/// `pull_result == Some(Err(_))` and does NOT hang.
#[tokio::test]
async fn local_pull_leg_mid_stream_origin_fault_fails_without_hang() -> anyhow::Result<()> {
    let (engine, root, total, _tmp) = engine_with_origin(Mode::FaultMidStream).await?;

    let result = run_to_termination(&engine, root, total)
        .await?
        .ok_or_else(|| anyhow::anyhow!("pull_result must be recorded even on fault"))?;
    assert!(
        result.is_err(),
        "a mid-stream origin transport fault must terminate the leg with Err"
    );
    Ok(())
}

/// (c) A corrupt own origin (bytes don't hash to `H`) terminates with `Err` —
/// classified LOCAL (there is no upstream to score) — and does NOT hang.
#[tokio::test]
async fn local_pull_leg_corrupt_origin_fails_local_without_hang() -> anyhow::Result<()> {
    let (engine, root, total, _tmp) = engine_with_origin(Mode::Corrupt).await?;

    let result = run_to_termination(&engine, root, total)
        .await?
        .ok_or_else(|| anyhow::anyhow!("pull_result must be recorded even on corruption"))?;
    assert!(
        result.is_err(),
        "a corrupt own origin must terminate the leg with a local Err"
    );
    Ok(())
}

/// An `Internal` fault anywhere in the chain (the range wire's encode panic)
/// reads as internal; an origin fault and a store fault do not.
#[test]
fn an_internal_fault_in_the_chain_is_internal() {
    use decdn_cache::CacheError;

    let panic = anyhow::Error::from(CacheError::Internal(anyhow::anyhow!("encode panicked")))
        .context("drive failed");
    assert!(super::is_internal_fault(&panic));
    let disk = anyhow::Error::from(CacheError::Store(anyhow::anyhow!("no space left")))
        .context("drive failed");
    assert!(
        !super::is_internal_fault(&disk),
        "a store fault is not a code bug"
    );
    let origin = anyhow::Error::from(CacheError::OriginError {
        hash: Hash::from([1; 32]),
        source: anyhow::anyhow!("origin stopped serving"),
    })
    .context("drive failed");
    assert!(!super::is_internal_fault(&origin));
    assert!(!super::is_internal_fault(&anyhow::anyhow!("bare")));
}

/// The store's import failure as iroh-blobs reports a failed data-file write
/// (a full disk among them): a kind-less I/O error under a context, inside
/// [`CacheError::Store`].
fn full_disk_store_fault() -> decdn_cache::CacheError {
    let io = iroh_blobs::api::Error::from(std::io::Error::other("write batch failed"));
    decdn_cache::CacheError::Store(anyhow::Error::new(io).context("admit_bao_stream: store import"))
}

/// A fault of this node's own store ends the assembly instead of moving the
/// range to the next holder: every holder's bytes land in the same store. It
/// holds whether the `CacheError` is the root (an admit) or boxed under a
/// `RangedStoreError::Backend` (a store query).
#[test]
fn a_full_disk_under_a_store_fault_ends_the_assembly() {
    let admitted = anyhow::Error::from(full_disk_store_fault());
    assert!(super::ends_the_assembly(&admitted), "{admitted:#}");

    let ranged = anyhow::Error::new(decdn_bao_range::RangedStoreError::Backend(Box::new(
        full_disk_store_fault(),
    )));
    assert!(super::ends_the_assembly(&ranged), "{ranged:#}");

    let fed = anyhow::Error::from(decdn_cache::CacheError::Feed(anyhow::anyhow!(
        "stream ended early"
    )));
    assert!(
        !super::ends_the_assembly(&fed),
        "a short delivery is the holder's, so the range moves on: {fed:#}"
    );
}
