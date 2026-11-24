//! Integration tests for [`decdn_cache::CacheEngine`]: the `populate` chain
//! walk (retry, coalescing, fallback), GC reclaim of evicted blobs, and the
//! bao export of a held blob.
//!
//! Only the node-to-node origin pulls a whole blob: it admits the verified
//! stream into the store itself and answers `OriginFetch::AlreadyAdmitted`.
//! The test origins here do the same, through `CacheEngine::insert_blob`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use decdn_cache::{
    CacheEngine, CacheError, CacheMetrics, FilesystemOrigin, Hash, Origin, OriginFetch, OriginKind,
    OriginPullError, OriginRangeRequest, PinnedHashes, RetryPolicy,
};

fn err_of<T: std::fmt::Debug, E>(r: Result<T, E>) -> anyhow::Result<E> {
    r.err()
        .ok_or_else(|| anyhow::anyhow!("expected error, got Ok"))
}

/// The engine a test origin admits into, set right after the engine opens.
type EngineSlot = Arc<OnceLock<CacheEngine>>;

/// Admit `payload` into the engine in `slot`, as the node-to-node origin
/// admits a verified stream, and answer `AlreadyAdmitted`.
async fn admit(slot: &EngineSlot, payload: bytes::Bytes) -> Result<OriginFetch, OriginPullError> {
    let engine = slot
        .get()
        .ok_or_else(|| OriginPullError::Permanent(anyhow::anyhow!("engine not set")))?;
    engine
        .insert_blob(payload)
        .await
        .map_err(|e| OriginPullError::Permanent(e.into()))?;
    Ok(OriginFetch::AlreadyAdmitted)
}

/// Deterministic pseudo-random blob.
fn make_blob(len: usize, seed: u32) -> Vec<u8> {
    let mut payload = vec![0u8; len];
    let mut x = seed;
    for b in &mut payload {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    payload
}

/// Bound on a GC wait. It only catches a hung GC task: the waits count
/// sweeps, so a loaded host makes them slower but does not fail them.
const GC_HANG_BOUND: Duration = Duration::from_secs(30);

/// Waits until the GC loop has run `target` sweeps (`gc_runs >= target`).
async fn wait_for_gc_runs(metrics: &CacheMetrics, target: u64) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + GC_HANG_BOUND;
    while metrics.gc_runs.get() < target {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "GC hung: gc_runs = {} after {GC_HANG_BOUND:?}, want {target}",
            metrics.gc_runs.get()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

/// Waits until GC deletes bytes whose last tag is gone before this call, and
/// counts them in `gc_bytes_reclaimed`.
///
/// Attribution lags one sweep: each sweep's protect callback diffs its
/// pre-sweep snapshot against the previous one. Let `r` be `gc_runs` on entry.
/// Sweep `r + 1` marks after the tag is gone, so it deletes the bytes at the
/// latest. The snapshot of sweep `r + 2` does not hold them, so its callback
/// counts them. The callback bumps `gc_runs` before `gc_bytes_reclaimed`, so
/// the wait runs to sweep `r + 3`, which starts after both bumps of sweep
/// `r + 2`.
async fn wait_for_gc_reclaim(metrics: &CacheMetrics) -> anyhow::Result<()> {
    let start = metrics.gc_runs.get();
    wait_for_gc_runs(metrics, start + 3).await
}

/// An engine with no origins, a metrics handle and a GC sweep every
/// `gc_interval` (`ZERO` disables GC).
async fn gc_engine(
    gc_interval: Duration,
) -> anyhow::Result<(CacheEngine, Arc<CacheMetrics>, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let metrics = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        Vec::new(),
        16,
        PinnedHashes::empty(),
        RetryPolicy::default(),
        decdn_cache::CircuitBreakerPolicy::default(),
        Some(Arc::clone(&metrics)),
        gc_interval,
    )
    .await?;
    Ok((engine, metrics, tmp))
}

/// #860 regression: a logically-evicted blob keeps its protecting named
/// tag, so iroh-blobs GC can never reclaim its disk — contradicting the
/// "reclaimed on the next GC sweep" guarantee. `evict()` must delete the
/// tag so the sweep frees the bytes while serving stops immediately.
#[tokio::test]
async fn evict_makes_blob_gc_eligible() -> anyhow::Result<()> {
    let payload: &[u8] = b"content to evict and reclaim";
    let (engine, metrics, _tmp) = gc_engine(Duration::from_millis(200)).await?;

    // Cache the blob (creates the protecting named tag), then evict it.
    let hash = engine.insert_blob(payload.to_vec()).await?;
    anyhow::ensure!(
        engine.inspect(hash).await?.size_bytes.is_some(),
        "blob must be on disk after caching"
    );
    engine.evict(hash).await?;
    anyhow::ensure!(
        engine.inspect(hash).await?.already_evicted,
        "evict must logically stop serving"
    );

    wait_for_gc_reclaim(&metrics).await?;

    anyhow::ensure!(
        engine.inspect(hash).await?.size_bytes.is_none(),
        "evicted blob's bytes must be GC-reclaimed once its tag is deleted (#860)"
    );
    anyhow::ensure!(
        metrics.gc_bytes_reclaimed.get() > 0,
        "gc_bytes_reclaimed must be nonzero after reclaiming the evicted blob; got {}",
        metrics.gc_bytes_reclaimed.get()
    );
    Ok(())
}

/// Locks in the **lagged-by-one-cycle attribution** documented in the
/// `gc_bytes_reclaimed_total` metric: the first sweep records a baseline
/// and reports zero reclaim, and each later sweep counts the bytes the
/// previous sweep deleted. The check is that the total equals the payload
/// exactly. A first sweep that counted the whole blob set would add the
/// payload twice, which inflates `gc_bytes_reclaimed_total` by the entire
/// blob set on every restart — the exact alert signal
/// `gc_bytes_reclaimed_total` exists for. A post-sweep snapshot never sees
/// the deleted blob and counts zero.
#[tokio::test]
async fn gc_attribution_lags_one_cycle() -> anyhow::Result<()> {
    let payload: &[u8] = b"a body evicted to become GC-eligible";
    let (engine, metrics, _tmp) = gc_engine(Duration::from_millis(200)).await?;
    let hash = engine.insert_blob(payload.to_vec()).await?;
    engine.evict(hash).await?;

    // The store holds only the evicted blob. One sweep deletes it and the
    // next one counts it; every later snapshot lacks it, so later sweeps add
    // nothing.
    wait_for_gc_reclaim(&metrics).await?;
    anyhow::ensure!(
        metrics.gc_bytes_reclaimed.get() == payload.len() as u64,
        "gc_bytes_reclaimed_total must count the deleted blob exactly once \
         ({} bytes): the first sweep has no baseline and counts nothing; got {}",
        payload.len(),
        metrics.gc_bytes_reclaimed.get()
    );
    Ok(())
}

/// With `gc_interval = Duration::ZERO`, iroh-blobs must NOT spawn a GC loop:
/// an evicted blob's untagged bytes stay on disk, and the GC metrics stay at
/// zero. Locks in the `if gc_interval.is_zero() { None } else { Some(...) }`
/// polarity at `engine.rs::open_full` — a regression that swapped it would
/// silently re-enable GC for every operator who set `gc_interval_sec = 0` (or
/// silently disable it for everyone else).
#[tokio::test]
async fn gc_disabled_does_not_reclaim_or_emit_metrics() -> anyhow::Result<()> {
    let payload: &[u8] = b"a body that stays on disk with GC disabled";
    let (engine, metrics, _tmp) = gc_engine(Duration::ZERO).await?;
    let hash = engine.insert_blob(payload.to_vec()).await?;
    engine.evict(hash).await?;

    // Sleep long enough that a 200ms-interval GC loop would have fired
    // multiple times if it had been spawned. With GC disabled, nothing
    // should change.
    tokio::time::sleep(Duration::from_millis(800)).await;

    anyhow::ensure!(
        engine.inspect(hash).await?.size_bytes.is_some(),
        "evicted bytes must remain on disk when GC is disabled; got size_bytes = None"
    );
    anyhow::ensure!(
        metrics.gc_runs.get() == 0,
        "gc_runs_total must stay at 0 when GC is disabled; got {}",
        metrics.gc_runs.get()
    );
    anyhow::ensure!(
        metrics.gc_bytes_reclaimed.get() == 0,
        "gc_bytes_reclaimed_total must stay at 0 when GC is disabled; got {}",
        metrics.gc_bytes_reclaimed.get()
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_flushes_without_drop() -> anyhow::Result<()> {
    // Prove that `shutdown()` — not `Drop` — is what flushes pending state
    // to disk. `FsStore::Drop` also flushes, so a naive "shutdown, drop,
    // reopen" test passes even if `shutdown()` is a no-op. Using
    // `std::mem::forget` to skip `Drop` isolates the flush-on-shutdown
    // contract.
    let payload: &[u8] = b"persisted via shutdown, not drop";

    let tmp = tempfile::tempdir()?;
    let hash = {
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 16).await?;
        let hash = engine.insert_blob(payload.to_vec()).await?;
        engine.shutdown().await?;
        // Skip Drop so this test fails if shutdown() stopped flushing.
        std::mem::forget(engine);
        hash
    };

    let engine = CacheEngine::open(tmp.path(), Vec::new(), 16).await?;
    anyhow::ensure!(
        engine.has(hash).await?,
        "reopened engine should see the blob flushed by shutdown()"
    );
    let got = engine.read_blob(hash).await?;
    anyhow::ensure!(
        &got[..] == payload,
        "reopened engine should hold the cached payload"
    );
    Ok(())
}

#[tokio::test]
async fn open_on_file_path_yields_store_error() -> anyhow::Result<()> {
    // `CacheEngine::open` calls `create_dir_all`; pointing at an existing
    // regular file means that fails, which must surface as
    // `CacheError::Store` (the only variant reserved for store I/O issues).
    let tmp = tempfile::NamedTempFile::new()?;
    let err = CacheEngine::open(tmp.path(), Vec::new(), 16)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected open to fail on a file path"))?;
    anyhow::ensure!(
        matches!(err, CacheError::Store(_)),
        "expected Store variant, got: {err:?}"
    );
    Ok(())
}

/// A filesystem origin serves its blobs by range only: `populate` pulls no
/// whole blob from it, even when the origin holds the data object.
#[tokio::test]
async fn populate_pulls_no_whole_blob_from_an_fs_origin() -> anyhow::Result<()> {
    let payload: &[u8] = b"content-addressed on local disk";
    let hash = Hash::new(payload);
    let origin_dir = tempfile::tempdir()?;
    let hex = hash.to_hex();
    let shard = origin_dir.path().join(
        hex.get(..2)
            .ok_or_else(|| anyhow::anyhow!("hex too short"))?,
    );
    std::fs::create_dir_all(&shard)?;
    std::fs::write(shard.join(hex.as_str()), payload)?;

    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(FilesystemOrigin::new(origin_dir.path()).await?);
    let engine = CacheEngine::open(cache_dir.path(), vec![origin as Arc<dyn Origin>], 16).await?;

    let err = err_of(engine.populate(hash).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::NotFound { .. }),
        "expected NotFound, got: {err:?}"
    );
    anyhow::ensure!(!engine.has(hash).await?, "nothing lands in the store");
    Ok(())
}

// ----- origin retry policy (#285) -----------------------------------------

/// Open an engine over `origins` with `policy` and `breaker`, then set
/// `slot` so the test origins can admit into it.
async fn open_engine(
    origins: Vec<Arc<dyn Origin>>,
    policy: RetryPolicy,
    breaker: decdn_cache::CircuitBreakerPolicy,
    metrics: Option<Arc<CacheMetrics>>,
    slot: &EngineSlot,
) -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open_full(
        tmp.path(),
        origins,
        16,
        PinnedHashes::empty(),
        policy,
        breaker,
        metrics,
        Duration::ZERO,
    )
    .await?;
    slot.set(engine.clone())
        .map_err(|_| anyhow::anyhow!("engine slot set twice"))?;
    Ok((engine, tmp))
}

/// Build a `CacheEngine` over one origin with a custom retry policy and no
/// metrics wiring. Returns the engine and the temp dir whose drop cleans up.
async fn build_engine_with_retry(
    origin: Arc<dyn Origin>,
    policy: RetryPolicy,
    slot: &EngineSlot,
) -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    open_engine(
        vec![origin],
        policy,
        decdn_cache::CircuitBreakerPolicy::default(),
        None,
        slot,
    )
    .await
}

/// Tiny policy used by tests where the *count* of attempts matters but
/// we don't want to wait around for real backoffs. 1ms initial / 5ms
/// cap / no jitter keeps total wall time under 100ms even on a fully
/// failing origin.
const fn fast_retry_policy(max_retries: u32) -> RetryPolicy {
    RetryPolicy {
        max_retries,
        initial_backoff_ms: 1,
        max_backoff_ms: 5,
        jitter_ratio: 0.0,
    }
}

/// Origin that returns N successive `OriginPullError::Transient` failures
/// before admitting `payload`, or, if `max_failures` is `usize::MAX`,
/// failing forever. Counts every fetch via an `AtomicUsize` so tests can
/// assert exact attempt totals.
#[derive(Debug)]
struct FailingThenSucceedingOrigin {
    payload: bytes::Bytes,
    target: Hash,
    fail_count: std::sync::atomic::AtomicUsize,
    fetch_count: std::sync::atomic::AtomicUsize,
    max_failures: usize,
    engine: EngineSlot,
}

impl FailingThenSucceedingOrigin {
    fn new(payload: &[u8], max_failures: usize, engine: &EngineSlot) -> Self {
        Self {
            target: Hash::new(payload),
            payload: bytes::Bytes::from(payload.to_vec()),
            fail_count: std::sync::atomic::AtomicUsize::new(0),
            fetch_count: std::sync::atomic::AtomicUsize::new(0),
            max_failures,
            engine: Arc::clone(engine),
        }
    }
    fn fetches(&self) -> usize {
        self.fetch_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Origin for FailingThenSucceedingOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Peer
    }

    fn fetch(
        &self,
        hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        self.fetch_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let prior = self
            .fail_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let max_failures = self.max_failures;
        let target = self.target;
        let payload = self.payload.clone();
        Box::pin(async move {
            if prior < max_failures {
                Err(OriginPullError::Transient(anyhow::anyhow!(
                    "synthetic transient failure {prior}"
                )))
            } else if hash == target {
                admit(&self.engine, payload).await
            } else {
                Ok(OriginFetch::NotFound)
            }
        })
    }
}

/// Origin that sleeps for a fixed `delay` before admitting `payload`. The
/// delay forces every concurrent `CacheEngine::populate` caller to reach the
/// coalescing table before the first owner's fetch completes — without it,
/// an instantaneous fetch can finish before any waiter is even scheduled,
/// making a coalescing assertion trivially pass for the wrong reason. Counts
/// every fetch.
#[derive(Debug)]
struct SlowOnceOrigin {
    payload: bytes::Bytes,
    target: Hash,
    fetch_count: std::sync::atomic::AtomicUsize,
    delay: Duration,
    engine: EngineSlot,
}

impl SlowOnceOrigin {
    fn new(payload: &[u8], delay: Duration, engine: &EngineSlot) -> Self {
        Self {
            target: Hash::new(payload),
            payload: bytes::Bytes::from(payload.to_vec()),
            fetch_count: std::sync::atomic::AtomicUsize::new(0),
            delay,
            engine: Arc::clone(engine),
        }
    }
    fn fetches(&self) -> usize {
        self.fetch_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Origin for SlowOnceOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Peer
    }

    fn fetch(
        &self,
        hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        self.fetch_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let target = self.target;
        let payload = self.payload.clone();
        let delay = self.delay;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            if hash == target {
                admit(&self.engine, payload).await
            } else {
                Ok(OriginFetch::NotFound)
            }
        })
    }
}

/// Origin that always returns `OriginPullError::Permanent` and counts
/// attempts. Used to verify the retry loop short-circuits on permanent
/// errors and (with a non-empty `marker`) to verify which origin's
/// error survives in the all-exhausted `OriginError.source` chain.
#[derive(Debug)]
struct PermanentlyFailingOrigin {
    fetch_count: std::sync::atomic::AtomicUsize,
    marker: String,
}

impl PermanentlyFailingOrigin {
    fn new() -> Self {
        Self::with_marker(String::new())
    }
    fn with_marker(marker: impl Into<String>) -> Self {
        Self {
            fetch_count: std::sync::atomic::AtomicUsize::new(0),
            marker: marker.into(),
        }
    }
    fn fetches(&self) -> usize {
        self.fetch_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Origin for PermanentlyFailingOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Peer
    }

    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        self.fetch_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let marker = self.marker.clone();
        Box::pin(async move {
            if marker.is_empty() {
                Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "synthetic permanent failure (e.g. 403)"
                )))
            } else {
                Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "synthetic permanent failure with marker={marker}"
                )))
            }
        })
    }
}

/// Origin that always returns `NotFound`. Used to simulate a mirror
/// that doesn't have the blob — the fallback chain should advance past
/// it to the next entry.
#[derive(Debug)]
struct NotFoundOrigin {
    fetch_count: std::sync::atomic::AtomicUsize,
}

impl NotFoundOrigin {
    const fn new() -> Self {
        Self {
            fetch_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    fn fetches(&self) -> usize {
        self.fetch_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Origin for NotFoundOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Peer
    }

    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        self.fetch_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move { Ok(OriginFetch::NotFound) })
    }
}

/// Origin that admits a fixed `payload` on every fetch and counts fetches.
#[derive(Debug)]
struct PayloadOrigin {
    payload: bytes::Bytes,
    fetch_count: std::sync::atomic::AtomicUsize,
    engine: EngineSlot,
}

impl PayloadOrigin {
    fn new(payload: &[u8], engine: &EngineSlot) -> Self {
        Self {
            payload: bytes::Bytes::from(payload.to_vec()),
            fetch_count: std::sync::atomic::AtomicUsize::new(0),
            engine: Arc::clone(engine),
        }
    }
    fn fetches(&self) -> usize {
        self.fetch_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Origin for PayloadOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Peer
    }

    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        self.fetch_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let payload = self.payload.clone();
        Box::pin(async move { admit(&self.engine, payload).await })
    }
}

#[tokio::test]
async fn retry_succeeds_after_transient_failures() -> anyhow::Result<()> {
    // 2 transient failures, then success on the 3rd attempt. Policy
    // allows 3 retries, so the loop has headroom.
    let payload: &[u8] = b"recovered";
    let slot = EngineSlot::default();
    let origin = Arc::new(FailingThenSucceedingOrigin::new(payload, 2, &slot));
    let (engine, _tmp) = build_engine_with_retry(
        origin.clone() as Arc<dyn Origin>,
        fast_retry_policy(3),
        &slot,
    )
    .await?;

    let hash = Hash::new(payload);
    engine.populate(hash).await?;
    anyhow::ensure!(
        &engine.read_blob(hash).await?[..] == payload,
        "payload survived retry chain"
    );
    anyhow::ensure!(
        origin.fetches() == 3,
        "expected exactly 3 origin attempts (2 fail + 1 success), got {}",
        origin.fetches()
    );
    Ok(())
}

#[tokio::test]
async fn retry_exhausts_on_persistent_transient_failures() -> anyhow::Result<()> {
    // Origin fails forever. Policy allows 3 retries -> 4 total attempts.
    let payload: &[u8] = b"unreachable";
    let slot = EngineSlot::default();
    let origin = Arc::new(FailingThenSucceedingOrigin::new(payload, usize::MAX, &slot));
    let (engine, _tmp) = build_engine_with_retry(
        origin.clone() as Arc<dyn Origin>,
        fast_retry_policy(3),
        &slot,
    )
    .await?;

    let err = err_of(engine.populate(Hash::new(payload)).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "exhausted retries should surface OriginError, got: {err:?}"
    );
    anyhow::ensure!(
        origin.fetches() == 4,
        "expected exactly 4 origin attempts (1 + 3 retries), got {}",
        origin.fetches()
    );
    Ok(())
}

#[tokio::test]
async fn retry_skips_permanent_failures() -> anyhow::Result<()> {
    // Permanent failure on the very first attempt — the loop must not
    // retry, even with a generous policy.
    let slot = EngineSlot::default();
    let origin = Arc::new(PermanentlyFailingOrigin::new());
    let (engine, _tmp) = build_engine_with_retry(
        origin.clone() as Arc<dyn Origin>,
        fast_retry_policy(5),
        &slot,
    )
    .await?;

    let err = err_of(engine.populate(Hash::new(b"anything")).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "permanent failure should surface OriginError, got: {err:?}"
    );
    anyhow::ensure!(
        origin.fetches() == 1,
        "expected exactly 1 attempt on permanent failure, got {}",
        origin.fetches()
    );
    Ok(())
}

#[tokio::test]
async fn retry_skips_not_found_responses() -> anyhow::Result<()> {
    // `OriginFetch::NotFound` is *not* an error and must not be retried.
    let slot = EngineSlot::default();
    let origin = Arc::new(NotFoundOrigin::new());
    let (engine, _tmp) = build_engine_with_retry(
        origin.clone() as Arc<dyn Origin>,
        fast_retry_policy(3),
        &slot,
    )
    .await?;

    let err = err_of(engine.populate(Hash::new(b"missing")).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::NotFound { .. }),
        "NotFound should map to CacheError::NotFound, got: {err:?}"
    );
    anyhow::ensure!(origin.fetches() == 1, "a miss is asked once");
    Ok(())
}

#[tokio::test]
async fn retry_disabled_policy_makes_a_single_attempt() -> anyhow::Result<()> {
    // RetryPolicy::disabled() makes a single attempt: a transient
    // failure surfaces immediately without retrying.
    let payload: &[u8] = b"unreached";
    let slot = EngineSlot::default();
    let origin = Arc::new(FailingThenSucceedingOrigin::new(payload, usize::MAX, &slot));
    let (engine, _tmp) = build_engine_with_retry(
        origin.clone() as Arc<dyn Origin>,
        RetryPolicy::disabled(),
        &slot,
    )
    .await?;

    let err = err_of(engine.populate(Hash::new(payload)).await)?;
    anyhow::ensure!(matches!(err, CacheError::OriginError { .. }));
    anyhow::ensure!(
        origin.fetches() == 1,
        "disabled policy must perform exactly one attempt, got {}",
        origin.fetches()
    );
    Ok(())
}

#[tokio::test]
async fn coalesced_owner_retry_unblocks_waiters_on_success() -> anyhow::Result<()> {
    // Multiple concurrent populates for the same hash: the engine coalesces
    // them through a single owner that runs the retry loop. The owner
    // retries through 2 transient failures, then succeeds; every waiter
    // observes the filled store — proof that the retry loop sits inside the
    // coalescing critical section, not outside.
    let payload: &[u8] = b"shared via coalescing";
    let slot = EngineSlot::default();
    let origin = Arc::new(FailingThenSucceedingOrigin::new(payload, 2, &slot));
    let (engine, _tmp) = build_engine_with_retry(
        origin.clone() as Arc<dyn Origin>,
        fast_retry_policy(3),
        &slot,
    )
    .await?;
    let hash = Hash::new(payload);

    let mut handles = Vec::new();
    for _ in 0..6 {
        let e = engine.clone();
        handles.push(tokio::spawn(async move { e.populate(hash).await }));
    }
    for h in handles {
        h.await??;
    }
    anyhow::ensure!(&engine.read_blob(hash).await?[..] == payload);
    // Coalescing collapses the 6 concurrent requests into one origin
    // pull. That one pull retries through 2 failures + 1 success = 3
    // origin fetches total. The exact total is the load-bearing
    // assertion: a regression that ran retry *outside* coalescing
    // would multiply this by 6.
    anyhow::ensure!(
        origin.fetches() == 3,
        "coalesced retry should fire exactly 3 origin attempts, got {}",
        origin.fetches()
    );
    Ok(())
}

#[tokio::test]
async fn coalesced_owner_exhaustion_bounded_by_circuit_breaker() -> anyhow::Result<()> {
    // Retry-amplification bound (#1615), the invariant the retry.rs module
    // doc describes. Under sustained transient failure, coalescing (#305)
    // serializes owners — at any instant exactly one owner runs its retry
    // budget — but after each owner exhausts, a *waiter* re-elects and runs a
    // fresh budget. Taken alone that is `N` *sequential* budgets for `N`
    // concurrent waiters. What actually bounds it is the per-origin
    // circuit-breaker (#963): after `failure_threshold` budget-exhaustions the
    // breaker trips OPEN and every later waiter short-circuits *before* the
    // retry loop. This test pins the bound in BOTH directions, with
    // `N_WAITERS` deliberately above `failure_threshold` so the breaker has to
    // shed load for the assertion to hold:
    //
    //   1. Every concurrent populate observes a `CacheError::OriginError`.
    //   2. With the breaker DISABLED the amplification is exactly the O(N)
    //      worst case `N * (1 + max_retries)`: every waiter re-elects and burns
    //      a full budget.
    //   3. With the default breaker ENABLED the total origin fetches are capped
    //      at `failure_threshold * (1 + max_retries)` — a constant independent
    //      of N — and therefore strictly below the O(N) case.
    //   4. Fetches stay at or above the no-coalescing minimum `1 + max_retries`,
    //      confirming retry actually runs before the breaker sheds.
    const N_WAITERS: u32 = 12;
    const MAX_RETRIES: u32 = 2;

    // Spawn `N_WAITERS` concurrent populates at a forever-failing origin
    // fronted by `breaker`; assert every one surfaces `OriginError`, then
    // return the total number of origin fetches driven.
    async fn drive_down_origin(
        breaker: decdn_cache::CircuitBreakerPolicy,
        payload: &'static [u8],
        hash: Hash,
        n: u32,
    ) -> anyhow::Result<usize> {
        let slot = EngineSlot::default();
        let origin = Arc::new(FailingThenSucceedingOrigin::new(payload, usize::MAX, &slot));
        let (engine, _tmp) = open_engine(
            vec![origin.clone() as Arc<dyn Origin>],
            fast_retry_policy(MAX_RETRIES),
            breaker,
            None,
            &slot,
        )
        .await?;

        let mut handles = Vec::new();
        for _ in 0..n {
            let e = engine.clone();
            handles.push(tokio::spawn(async move { e.populate(hash).await }));
        }
        for h in handles {
            match h.await? {
                Err(CacheError::OriginError { .. }) => {}
                other => anyhow::bail!("expected OriginError, got: {other:?}"),
            }
        }
        Ok(origin.fetches())
    }

    let budget = 1 + MAX_RETRIES as usize; // attempts per owner turn
    let payload: &[u8] = b"never available";
    let hash = Hash::new(payload);

    // Breaker DISABLED: the amplification is exactly O(N). Each of the N
    // waiters serially re-elects and burns one full `budget`-attempt turn.
    let unbounded = drive_down_origin(
        decdn_cache::CircuitBreakerPolicy::disabled(),
        payload,
        hash,
        N_WAITERS,
    )
    .await?;
    let o_n = (N_WAITERS as usize) * budget;
    anyhow::ensure!(
        unbounded == o_n,
        "breaker disabled: expected the O(N) worst case {o_n}, got {unbounded}",
    );

    // Breaker ENABLED (default): the total is capped by the breaker's failure
    // threshold, independent of N, and strictly below the O(N) case above.
    let threshold = decdn_cache::CircuitBreakerPolicy::default().failure_threshold as usize;
    anyhow::ensure!(
        threshold < N_WAITERS as usize,
        "test is only meaningful when failure_threshold ({threshold}) < N_WAITERS ({N_WAITERS})",
    );
    let bounded = drive_down_origin(
        decdn_cache::CircuitBreakerPolicy::default(),
        payload,
        hash,
        N_WAITERS,
    )
    .await?;
    let cap = threshold * budget;
    anyhow::ensure!(
        bounded <= cap,
        "breaker enabled: fetches {bounded} exceed the cap {cap} (failure_threshold * budget)",
    );
    anyhow::ensure!(
        bounded < o_n,
        "breaker enabled: fetches {bounded} not below the O(N) case {o_n} — breaker never shed load",
    );
    anyhow::ensure!(
        bounded >= budget,
        "breaker enabled: fetches {bounded} below the {budget}-attempt minimum — retry never ran?",
    );
    Ok(())
}

#[tokio::test]
async fn coalesced_concurrent_misses_fire_origin_exactly_once() -> anyhow::Result<()> {
    // Minimal happy-path companion to
    // `coalesced_owner_retry_unblocks_waiters_on_success`: many simultaneous
    // populates for an uncached hash collapse into a single origin pull when
    // no retry budget is in play (#530). A regression that broke coalescing
    // (e.g. waiters racing past the registration slot) would show up here as
    // fetches > 1 without retry noise muddying the count.
    //
    // The 25ms `SlowOnceOrigin` delay forces all 10 spawned tasks to reach
    // the coalescing table before the first owner's fetch completes —
    // without it an instantaneous fetch could finish before any waiter
    // is even scheduled, making the assertion trivially pass.
    let payload: &[u8] = b"single shared origin fetch";
    let slot = EngineSlot::default();
    let origin = Arc::new(SlowOnceOrigin::new(
        payload,
        Duration::from_millis(25),
        &slot,
    ));
    let (engine, _tmp) = build_engine_with_retry(
        origin.clone() as Arc<dyn Origin>,
        RetryPolicy::disabled(),
        &slot,
    )
    .await?;
    let hash = Hash::new(payload);

    let mut handles = Vec::new();
    for _ in 0..10 {
        let e = engine.clone();
        handles.push(tokio::spawn(async move { e.populate(hash).await }));
    }
    for h in handles {
        h.await??;
    }
    anyhow::ensure!(&engine.read_blob(hash).await?[..] == payload);
    anyhow::ensure!(
        origin.fetches() == 1,
        "coalesced concurrent misses must fire origin exactly once, got {}",
        origin.fetches()
    );
    Ok(())
}

// =====================================================================
// Multi-origin fallback chain (#284)
//
// `pull_through` consults `cache.origins` in declared order. The next
// entry is tried on `NotFound`, on `Permanent` errors, and on
// `Transient` errors after the per-origin retry budget is exhausted.
// =====================================================================

/// Construct a `CacheEngine` over an explicit fallback chain and a
/// fast retry policy. Sharing this helper across the fallback tests
/// keeps assertions focused on chain-walk behaviour rather than
/// engine-construction boilerplate.
async fn build_engine_with_origins(
    origins: Vec<Arc<dyn Origin>>,
    metrics: Arc<CacheMetrics>,
    slot: &EngineSlot,
) -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    open_engine(
        origins,
        fast_retry_policy(3),
        decdn_cache::CircuitBreakerPolicy::default(),
        Some(metrics),
        slot,
    )
    .await
}

#[tokio::test]
async fn fallback_advances_on_notfound_to_next_origin() -> anyhow::Result<()> {
    let payload: &[u8] = b"served by mirror #2";
    let hash = Hash::new(payload);
    let slot = EngineSlot::default();
    let first = Arc::new(NotFoundOrigin::new());
    let second = Arc::new(PayloadOrigin::new(payload, &slot));
    let metrics = Arc::new(CacheMetrics::default());
    let (engine, _tmp) = build_engine_with_origins(
        vec![
            Arc::clone(&first) as Arc<dyn Origin>,
            Arc::clone(&second) as Arc<dyn Origin>,
        ],
        Arc::clone(&metrics),
        &slot,
    )
    .await?;

    engine.populate(hash).await?;
    anyhow::ensure!(
        &engine.read_blob(hash).await?[..] == payload,
        "second origin must have served"
    );
    anyhow::ensure!(first.fetches() == 1, "primary must have been tried once");
    anyhow::ensure!(second.fetches() == 1, "fallback must have been reached");
    anyhow::ensure!(
        metrics.origin_fallback.get() == 1,
        "exactly one fallback advance step (primary -> secondary)"
    );
    anyhow::ensure!(
        metrics.origin_fetches.get() == 1,
        "origin_fetches must bump exactly once per pull_through call regardless of \
         chain length (regression guard for #285 alerting ratios)",
    );
    Ok(())
}

#[tokio::test]
async fn fallback_advances_on_permanent_error_to_next_origin() -> anyhow::Result<()> {
    let payload: &[u8] = b"served after Permanent failure";
    let hash = Hash::new(payload);
    let slot = EngineSlot::default();
    let first = Arc::new(PermanentlyFailingOrigin::new());
    let second = Arc::new(PayloadOrigin::new(payload, &slot));
    let metrics = Arc::new(CacheMetrics::default());
    let (engine, _tmp) = build_engine_with_origins(
        vec![
            Arc::clone(&first) as Arc<dyn Origin>,
            Arc::clone(&second) as Arc<dyn Origin>,
        ],
        Arc::clone(&metrics),
        &slot,
    )
    .await?;

    engine.populate(hash).await?;
    anyhow::ensure!(engine.has(hash).await?, "second origin must have served");
    anyhow::ensure!(
        first.fetches() == 1,
        "Permanent error must not burn retry budget on origin 0"
    );
    anyhow::ensure!(second.fetches() == 1, "fallback must have been reached");
    anyhow::ensure!(
        metrics.origin_fallback.get() == 1,
        "exactly one fallback advance step",
    );
    anyhow::ensure!(
        metrics.origin_fetches.get() == 1,
        "origin_fetches must bump exactly once per pull_through call",
    );
    Ok(())
}

#[tokio::test]
async fn fallback_advances_on_transient_after_retry_exhaustion() -> anyhow::Result<()> {
    let payload: &[u8] = b"served after retry exhaustion";
    let hash = Hash::new(payload);
    let slot = EngineSlot::default();
    // `max_failures = usize::MAX` => origin 0 transient-fails forever
    // and the retry budget runs out. With `fast_retry_policy(3)` that
    // is 1 + 3 = 4 attempts on origin 0 before the chain advances.
    let first = Arc::new(FailingThenSucceedingOrigin::new(payload, usize::MAX, &slot));
    let second = Arc::new(PayloadOrigin::new(payload, &slot));
    let metrics = Arc::new(CacheMetrics::default());
    let (engine, _tmp) = build_engine_with_origins(
        vec![
            Arc::clone(&first) as Arc<dyn Origin>,
            Arc::clone(&second) as Arc<dyn Origin>,
        ],
        Arc::clone(&metrics),
        &slot,
    )
    .await?;

    engine.populate(hash).await?;
    anyhow::ensure!(engine.has(hash).await?, "second origin must have served");
    anyhow::ensure!(
        first.fetches() == 4,
        "retry budget should be fully spent on origin 0 (1 + 3 retries), got {}",
        first.fetches(),
    );
    anyhow::ensure!(second.fetches() == 1, "fallback must have been reached");
    anyhow::ensure!(
        metrics.origin_retry_exhausted.get() == 1,
        "retry exhaustion on origin 0 must increment counter",
    );
    anyhow::ensure!(
        metrics.origin_fallback.get() == 1,
        "exactly one chain-walk step",
    );
    anyhow::ensure!(
        metrics.origin_fetches.get() == 1,
        "origin_fetches counts pull_through calls, not per-origin attempts",
    );
    Ok(())
}

#[tokio::test]
async fn all_origins_notfound_surfaces_notfound() -> anyhow::Result<()> {
    let hash = Hash::new(b"never-served");
    let slot = EngineSlot::default();
    let first = Arc::new(NotFoundOrigin::new());
    let second = Arc::new(NotFoundOrigin::new());
    let metrics = Arc::new(CacheMetrics::default());
    let (engine, _tmp) = build_engine_with_origins(
        vec![
            Arc::clone(&first) as Arc<dyn Origin>,
            Arc::clone(&second) as Arc<dyn Origin>,
        ],
        Arc::clone(&metrics),
        &slot,
    )
    .await?;

    let err = err_of(engine.populate(hash).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::NotFound { .. }),
        "all-NotFound chain must surface NotFound (=> 404 at edge), got: {err:?}",
    );
    anyhow::ensure!(first.fetches() == 1 && second.fetches() == 1);
    anyhow::ensure!(
        metrics.origin_fallback.get() == 1,
        "exactly one chain advance from origin 0 -> origin 1",
    );
    Ok(())
}

#[tokio::test]
async fn all_origins_error_surfaces_origin_error_with_last_failure() -> anyhow::Result<()> {
    // The chain-walk decision is "last error wins" — operators
    // triaging the surfaced 5xx see the most recently consulted
    // backend's failure. Use distinguishable markers on origins 0 and
    // 1 to assert origin 1's error survives into `OriginError.source`,
    // not origin 0's.
    let hash = Hash::new(b"never-served");
    let slot = EngineSlot::default();
    let first = Arc::new(PermanentlyFailingOrigin::with_marker("primary-marker"));
    let second = Arc::new(PermanentlyFailingOrigin::with_marker("mirror-marker"));
    let metrics = Arc::new(CacheMetrics::default());
    let (engine, _tmp) = build_engine_with_origins(
        vec![
            Arc::clone(&first) as Arc<dyn Origin>,
            Arc::clone(&second) as Arc<dyn Origin>,
        ],
        Arc::clone(&metrics),
        &slot,
    )
    .await?;

    let err = err_of(engine.populate(hash).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "all-error chain must surface OriginError (=> 5xx at edge), got: {err:?}",
    );
    let chain = err.display_chain().to_string();
    anyhow::ensure!(
        chain.contains("mirror-marker"),
        "OriginError must carry the LAST origin's failure marker; got chain: {chain}",
    );
    anyhow::ensure!(
        !chain.contains("primary-marker"),
        "OriginError must NOT carry the discarded earlier failure; got chain: {chain}",
    );
    Ok(())
}

#[tokio::test]
async fn fallback_counter_increments_per_step_in_three_origin_chain() -> anyhow::Result<()> {
    // Pin the per-step semantics of `origin_fallback`: with 3 origins
    // and the payload only on origin 2, the chain advances twice
    // (origin 0 -> 1, origin 1 -> 2), and the counter must reflect
    // both steps. A regression that bumps the counter once per
    // pull_through (rather than per advance) would still pass any
    // 2-origin test.
    let payload: &[u8] = b"served from third mirror";
    let hash = Hash::new(payload);
    let slot = EngineSlot::default();
    let first = Arc::new(NotFoundOrigin::new());
    let second = Arc::new(NotFoundOrigin::new());
    let third = Arc::new(PayloadOrigin::new(payload, &slot));
    let metrics = Arc::new(CacheMetrics::default());
    let (engine, _tmp) = build_engine_with_origins(
        vec![
            Arc::clone(&first) as Arc<dyn Origin>,
            Arc::clone(&second) as Arc<dyn Origin>,
            Arc::clone(&third) as Arc<dyn Origin>,
        ],
        Arc::clone(&metrics),
        &slot,
    )
    .await?;

    engine.populate(hash).await?;
    anyhow::ensure!(engine.has(hash).await?, "third origin must have served");
    anyhow::ensure!(first.fetches() == 1 && second.fetches() == 1 && third.fetches() == 1);
    anyhow::ensure!(
        metrics.origin_fallback.get() == 2,
        "expected exactly 2 chain-walk steps (0 -> 1 and 1 -> 2), got {}",
        metrics.origin_fallback.get(),
    );
    anyhow::ensure!(
        metrics.origin_fetches.get() == 1,
        "origin_fetches must bump once per pull_through regardless of chain length, got {}",
        metrics.origin_fetches.get(),
    );
    Ok(())
}

#[tokio::test]
async fn mixed_notfound_then_error_surfaces_error() -> anyhow::Result<()> {
    // A known backend errored is more diagnostic than "no backend had it",
    // so any non-NotFound failure beats a prior NotFound in the chain.
    let hash = Hash::new(b"never-served");
    let slot = EngineSlot::default();
    let first = Arc::new(NotFoundOrigin::new());
    let second = Arc::new(PermanentlyFailingOrigin::new());
    let metrics = Arc::new(CacheMetrics::default());
    let (engine, _tmp) = build_engine_with_origins(
        vec![
            Arc::clone(&first) as Arc<dyn Origin>,
            Arc::clone(&second) as Arc<dyn Origin>,
        ],
        Arc::clone(&metrics),
        &slot,
    )
    .await?;

    let err = err_of(engine.populate(hash).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "mixed NotFound-then-Error chain must surface OriginError, got: {err:?}",
    );
    Ok(())
}

#[tokio::test]
async fn single_origin_chain_never_advances() -> anyhow::Result<()> {
    let payload: &[u8] = b"single origin success";
    let hash = Hash::new(payload);
    let slot = EngineSlot::default();
    let only = Arc::new(PayloadOrigin::new(payload, &slot));
    let metrics = Arc::new(CacheMetrics::default());
    let (engine, _tmp) = build_engine_with_origins(
        vec![Arc::clone(&only) as Arc<dyn Origin>],
        Arc::clone(&metrics),
        &slot,
    )
    .await?;

    engine.populate(hash).await?;
    anyhow::ensure!(&engine.read_blob(hash).await?[..] == payload);
    anyhow::ensure!(only.fetches() == 1);
    anyhow::ensure!(
        metrics.origin_fallback.get() == 0,
        "single-origin chain never advances; counter stays flat at zero",
    );
    // A second populate is a local hit: no origin call.
    engine.populate(hash).await?;
    anyhow::ensure!(only.fetches() == 1, "a hit calls no origin");
    Ok(())
}

#[tokio::test]
async fn origin_range_request_len_and_empty() {
    let r = OriginRangeRequest {
        fetch_start: 16,
        fetch_end: 48,
    };
    assert_eq!(r.len(), 32);
    assert!(!r.is_empty());
    let z = OriginRangeRequest {
        fetch_start: 10,
        fetch_end: 10,
    };
    assert!(z.is_empty());
    assert_eq!(z.len(), 0);
}

/// An origin that serves only a fixed `{H}.obao4` via `fetch_outboard`.
#[derive(Debug)]
struct OutboardOnlyOrigin(bytes::Bytes);

impl Origin for OutboardOnlyOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch_outboard(
        &self,
        _hash: Hash,
        _outboard_max_bytes: u64,
    ) -> Pin<
        Box<dyn Future<Output = Result<decdn_cache::OutboardFetch, OriginPullError>> + Send + '_>,
    > {
        let ob = self.0.clone();
        Box::pin(async move { Ok(decdn_cache::OutboardFetch::Found(ob)) })
    }
}

#[tokio::test]
async fn outboard_read_rejects_a_wrong_length_outboard_and_advances_the_chain() -> anyhow::Result<()>
{
    // A truncated `{H}.obao4` (an HTML error body under the cap, a half-written
    // upload) can never verify against `H`, and a broken origin 1 must not
    // shadow a healthy origin 2. The outboard read treats a wrong length as a
    // decline that advances the origin chain.
    use bao_tree::io::outboard::PreOrderMemOutboard;
    let payload: Vec<u8> = (0..200 * 1024u32)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect();
    let ob = PreOrderMemOutboard::create(&payload, decdn_cache::range_pull::IROH_BLOCK_SIZE);
    let hash = Hash::from_bytes(*ob.root.as_bytes());
    let total = u64::try_from(payload.len())?;
    let correct = bytes::Bytes::from(ob.data);
    anyhow::ensure!(correct.len() > 8, "test premise: non-trivial outboard");
    let short = correct.slice(..correct.len() - 7);

    // Chain: broken origin first, healthy origin second → the healthy one wins.
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(
        tmp.path(),
        vec![
            Arc::new(OutboardOnlyOrigin(short.clone())) as Arc<dyn Origin>,
            Arc::new(OutboardOnlyOrigin(correct.clone())) as Arc<dyn Origin>,
        ],
        16,
    )
    .await?;
    let got = engine
        .origin_fetch_outboard_bytes(hash, total)
        .await?
        .ok_or_else(|| anyhow::anyhow!("the healthy origin's outboard must win"))?;
    anyhow::ensure!(got == correct, "the correct-length outboard wins the chain");

    // Only the broken origin configured → a clean decline, never a poisoned Some.
    let tmp2 = tempfile::tempdir()?;
    let engine2 = CacheEngine::open(
        tmp2.path(),
        vec![Arc::new(OutboardOnlyOrigin(short)) as Arc<dyn Origin>],
        16,
    )
    .await?;
    anyhow::ensure!(
        engine2
            .origin_fetch_outboard_bytes(hash, total)
            .await?
            .is_none(),
        "a wrong-length outboard must decline, not serve"
    );
    Ok(())
}

/// An engine with no origins whose store holds `payload` whole.
async fn engine_holding(payload: &[u8]) -> anyhow::Result<(CacheEngine, Hash, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Vec::new(), 16).await?;
    let hash = engine.insert_blob(payload.to_vec()).await?;
    Ok((engine, hash, tmp))
}

/// `export_bao_range` (#915, ADR 038 §Serve side) must emit the header-less bao
/// interleaved encoding that verifies against the content root. Round-trip: hold
/// a multi-group blob, export an interior range as bao, then reconstruct the
/// combined encoding (prepend the 8-byte size header) and import it into a
/// fresh store — `import_bao_bytes` verifies every group against the root, so a
/// clean import proves the export is honest bao for `hash`, and the decoded
/// content equals the original aligned span byte-for-byte.
#[tokio::test]
async fn export_bao_range_round_trips_and_verifies_against_root() -> anyhow::Result<()> {
    use bytes::Bytes;
    use decdn_cache::range_pull::align_range;
    use iroh_blobs::store::mem::MemStore;

    // Several 16 KiB groups, with a partial final group (not a group
    // multiple) to exercise the right edge.
    let payload = make_blob(200 * 1024 + 1234, 0x1234_5678);
    let (engine, hash, _tmp) = engine_holding(&payload).await?;

    let blob_size = u64::try_from(payload.len())?;
    let offset = 64 * 1024u64;
    let len = 32 * 1024u64;
    let wire = engine
        .export_bao_range(hash, offset, len, blob_size)
        .await?;
    anyhow::ensure!(!wire.is_empty(), "bao export must be non-empty");

    let aligned = align_range(offset, len, blob_size)?;
    let mut combined = blob_size.to_le_bytes().to_vec();
    combined.extend_from_slice(&wire);
    let store = MemStore::new();
    store
        .blobs()
        .import_bao_bytes(hash, aligned.chunk_ranges().clone(), Bytes::from(combined))
        .await?;

    let got = store
        .blobs()
        .export_ranges(hash, aligned.fetch_start()..aligned.fetch_end())
        .concatenate()
        .await?;
    let start = usize::try_from(aligned.fetch_start())?;
    let end = usize::try_from(aligned.fetch_end())?;
    let want = payload
        .get(start..end)
        .ok_or_else(|| anyhow::anyhow!("aligned span out of bounds"))?;
    anyhow::ensure!(got == want, "exported bao decodes to the wrong bytes");
    Ok(())
}

/// A whole-blob `export_bao_range(hash, 0, 0)` must cover the entire blob and
/// verify. This is the buffered drain of `export_bao_range_stream`, not the serve
/// path itself (the node streams — #1132); it covers the shared wire format both
/// forms produce (ADR 038 AC#3), and the whole-blob offset-0 byte-identity of
/// AC#7.
#[tokio::test]
async fn export_bao_range_whole_blob_offset_zero() -> anyhow::Result<()> {
    use bytes::Bytes;
    use decdn_cache::range_pull::align_range;
    use iroh_blobs::store::mem::MemStore;

    let payload = b"the whole blob, served bao from offset zero with no raw path".to_vec();
    let (engine, hash, _tmp) = engine_holding(&payload).await?;

    let blob_size = u64::try_from(payload.len())?;
    let wire = engine.export_bao_range(hash, 0, 0, blob_size).await?;
    let aligned = align_range(0, 0, blob_size)?;
    let mut combined = blob_size.to_le_bytes().to_vec();
    combined.extend_from_slice(&wire);
    let store = MemStore::new();
    store
        .blobs()
        .import_bao_bytes(hash, aligned.chunk_ranges().clone(), Bytes::from(combined))
        .await?;
    let got = store.blobs().get_bytes(hash).await?;
    anyhow::ensure!(
        got.as_ref() == payload.as_slice(),
        "whole-blob bao decodes to original"
    );
    Ok(())
}

/// A 0-byte blob served whole (`export_bao_range(hash, 0, 0, 0)`) must emit an
/// empty header-less wire form, and re-importing it round-trips to empty bytes
/// under the same content hash (#1054). The empty root is `Hash::new(&[])`.
#[tokio::test]
async fn export_bao_range_empty_blob_round_trips() -> anyhow::Result<()> {
    use bytes::Bytes;
    use decdn_cache::range_pull::align_range;
    use iroh_blobs::store::mem::MemStore;

    let (engine, hash, _tmp) = engine_holding(&[]).await?;
    anyhow::ensure!(hash == Hash::new([]), "the empty blob's root");

    let wire = engine.export_bao_range(hash, 0, 0, 0).await?;
    anyhow::ensure!(wire.is_empty(), "0-byte blob has an empty bao wire form");

    let aligned = align_range(0, 0, 0)?;
    // The combined encoding is the 8-byte LE size header (0) plus the (empty) wire.
    let mut combined = 0u64.to_le_bytes().to_vec();
    combined.extend_from_slice(&wire);
    let store = MemStore::new();
    store
        .blobs()
        .import_bao_bytes(hash, aligned.chunk_ranges().clone(), Bytes::from(combined))
        .await?;
    let got = store.blobs().get_bytes(hash).await?;
    anyhow::ensure!(got.as_ref().is_empty(), "empty blob decodes to empty bytes");
    Ok(())
}

/// `export_bao_range` on a 0-byte blob must honor a logical eviction (#279):
/// after `evict`, the empty early-return must surface a `Store` error rather
/// than keep serving an empty body (#1054 review). The empty blob is trivially
/// "present" in the store, so — unlike a non-empty blob, which can be genuinely
/// absent — eviction (not absence) is the only state that stops the empty serve,
/// and only the `has()` check (which honors the logical-eviction set) enforces
/// it here.
#[tokio::test]
async fn export_bao_range_empty_blob_evicted_errors() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Vec::new(), 16).await?;
    let hash = Hash::new([]);
    // The empty blob is trivially present; a whole serve succeeds.
    anyhow::ensure!(engine.has(hash).await?, "empty blob is trivially present");
    anyhow::ensure!(
        engine.export_bao_range(hash, 0, 0, 0).await?.is_empty(),
        "present empty blob serves empty"
    );

    // After eviction the node must stop serving it — including the empty body.
    engine.evict(hash).await?;
    anyhow::ensure!(!engine.has(hash).await?, "evicted blob reads as absent");
    let err = engine
        .export_bao_range(hash, 0, 0, 0)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected an error for an evicted empty blob"))?;
    anyhow::ensure!(
        matches!(err, CacheError::Store(_)),
        "evicted empty blob must surface a Store error, got {err:?}"
    );
    Ok(())
}

/// The streaming export (#1132) and the buffered one must produce the SAME wire
/// bytes — the buffered form is a drain of the stream, and the ADR 038
/// round-trip tests above exercise only the buffered form. This is what keeps
/// those tests meaningful proof for the path the node actually serves from.
///
/// It also asserts the stream really is chunked: a multi-group blob must arrive
/// as MANY items, not one. A regression that collected internally and yielded a
/// single item would pass a bytes-equality check alone while reintroducing a
/// whole-blob allocation.
#[tokio::test]
async fn export_bao_range_stream_matches_the_buffered_form_and_is_chunked() -> anyhow::Result<()> {
    use futures_util::StreamExt;

    // Several 16 KiB groups with a partial final group, matching the round-trip
    // test above.
    let payload = make_blob(200 * 1024 + 1234, 0x1234_5678);
    let (engine, hash, _tmp) = engine_holding(&payload).await?;

    let blob_size = u64::try_from(payload.len())?;
    // Both a whole-blob serve and an interior range: the offset-0 case is what
    // every client fetch takes, the interior case is the resume path.
    for (offset, len) in [(0u64, 0u64), (64 * 1024, 32 * 1024)] {
        let buffered = engine
            .export_bao_range(hash, offset, len, blob_size)
            .await?;

        let mut stream = engine
            .export_bao_range_stream(hash, offset, len, blob_size)
            .await?;
        let mut streamed = Vec::new();
        let mut items = 0usize;
        let mut largest = 0usize;
        while let Some(item) = stream.next().await {
            let bytes = item?;
            anyhow::ensure!(
                !bytes.is_empty(),
                "export stream must never yield an empty item at ({offset}, {len})"
            );
            largest = largest.max(bytes.len());
            items += 1;
            streamed.extend_from_slice(&bytes);
        }

        anyhow::ensure!(
            streamed == buffered.as_ref(),
            "streamed export differs from the buffered export at ({offset}, {len})"
        );
        anyhow::ensure!(
            items > 1,
            "export stream collapsed to {items} item(s) at ({offset}, {len}) — it must yield \
             per-bao-item so the serve path holds O(chunk group), not O(blob)"
        );
        // A leaf is one 16 KiB chunk group; nothing may exceed that.
        anyhow::ensure!(
            largest <= 16 * 1024,
            "export stream yielded a {largest}-byte item at ({offset}, {len}); items must stay \
             bounded by one chunk group"
        );
    }
    Ok(())
}

/// The 0-byte blob (#1054) must yield NO items, not one empty item: `ChunkData`
/// cannot carry an empty payload (#1088), so a zero-length item would be
/// unsendable and the serve path must go straight to `StreamEnd`.
#[tokio::test]
async fn export_bao_range_stream_empty_blob_yields_no_items() -> anyhow::Result<()> {
    use futures_util::StreamExt;

    let (engine, hash, _tmp) = engine_holding(&[]).await?;

    let mut stream = engine.export_bao_range_stream(hash, 0, 0, 0).await?;
    let mut items = 0usize;
    while let Some(item) = stream.next().await {
        item?;
        items += 1;
    }
    anyhow::ensure!(items == 0, "empty blob must yield no items, got {items}");
    Ok(())
}
