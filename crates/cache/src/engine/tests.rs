use super::*;
use crate::range_pull::encode_verified_range;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::origin::{Origin, OriginFetch, OriginKind};

/// A trivial in-memory origin for tests. Stores exactly one blob.
#[derive(Debug)]
struct StubOrigin {
    data: Bytes,
    hash: Hash,
}

impl StubOrigin {
    fn new(payload: &[u8]) -> Self {
        Self {
            hash: Hash::new(payload),
            data: Bytes::from(payload.to_vec()),
        }
    }
}

impl Origin for StubOrigin {
    fn kind(&self) -> OriginKind {
        // Stand in for an HTTP origin in tests so callers reasoning
        // about preview-side `origin_kinds` behaviour see a
        // non-empty `Vec<OriginKind>` entry (the field is
        // a vec, not an `Option`). The choice is arbitrary —
        // `Origin::kind` is a tag, not a behavioural switch.
        OriginKind::Http
    }

    fn fetch(
        &self,
        hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
    {
        let result = if hash == self.hash {
            Ok(OriginFetch::found_one_shot(self.data.clone()))
        } else {
            Ok(OriginFetch::NotFound)
        };
        Box::pin(async move { result })
    }
}

/// A stub origin that also answers [`Origin::size`] and
/// [`Origin::fetch_outboard`], for outboard-fetch tests. The
/// outboard answer is configurable so a test can exercise the
/// `NotFound`/`Unsupported` degrade.
#[derive(Debug)]
struct OutboardStubOrigin {
    data: Bytes,
    hash: Hash,
    outboard: Option<Bytes>,
}

impl OutboardStubOrigin {
    fn new(payload: &[u8], outboard: Option<Bytes>) -> Self {
        Self {
            hash: Hash::new(payload),
            data: Bytes::from(payload.to_vec()),
            outboard,
        }
    }
}

impl Origin for OutboardStubOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch(
        &self,
        hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
    {
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
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, crate::OriginPullError>> + Send + '_>>
    {
        let matches = hash == self.hash;
        let len = u64::try_from(self.data.len()).unwrap_or(u64::MAX);
        Box::pin(async move { Ok(matches.then_some(len)) })
    }

    fn fetch_outboard(
        &self,
        hash: Hash,
        _outboard_max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, crate::OriginPullError>> + Send + '_>>
    {
        let result = if hash == self.hash {
            match &self.outboard {
                Some(ob) => OutboardFetch::Found(ob.clone()),
                None => OutboardFetch::NotFound,
            }
        } else {
            OutboardFetch::NotFound
        };
        Box::pin(async move { Ok(result) })
    }
}

/// Serves several blobs from one origin so a single engine can hold
/// multiple distinct hashes (a second `CacheEngine::open` on the same
/// dir would deadlock on iroh-blobs' single-writer file lock).
#[derive(Debug)]
struct MultiStubOrigin {
    blobs: std::collections::HashMap<Hash, Bytes>,
}

impl MultiStubOrigin {
    fn new(payloads: &[&[u8]]) -> Self {
        let blobs = payloads
            .iter()
            .map(|p| (Hash::new(p), Bytes::from(p.to_vec())))
            .collect();
        Self { blobs }
    }
}

impl Origin for MultiStubOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch(
        &self,
        hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
    {
        let result = self
            .blobs
            .get(&hash)
            .map_or(Ok(OriginFetch::NotFound), |b| {
                Ok(OriginFetch::found_one_shot(b.clone()))
            });
        Box::pin(async move { result })
    }
}

/// Origin that answers `size()` (a `HEAD`/`HeadObject` stand-in) for one
/// hash and COUNTS the calls, so a test can prove `origin_probe_size`
/// memoises rather than re-probing the backend on every probe. An optional
/// delay exercises the per-probe timeout.
#[derive(Debug)]
struct CountingSizeOrigin {
    hash: Hash,
    size: u64,
    calls: Arc<AtomicUsize>,
    delay: Option<Duration>,
}

impl CountingSizeOrigin {
    fn new(hash: Hash, size: u64) -> (Self, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let origin = Self {
            hash,
            size,
            calls: Arc::clone(&calls),
            delay: None,
        };
        (origin, calls)
    }

    fn slow(hash: Hash, size: u64, delay: Duration) -> (Self, Arc<AtomicUsize>) {
        let (mut origin, calls) = Self::new(hash, size);
        origin.delay = Some(delay);
        (origin, calls)
    }
}

impl Origin for CountingSizeOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
    {
        // Never exercised by the probe path (existence + size only).
        Box::pin(async { Ok(OriginFetch::NotFound) })
    }

    fn size(
        &self,
        hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, crate::OriginPullError>> + Send + '_>>
    {
        let matches = hash == self.hash;
        let size = self.size;
        let calls = Arc::clone(&self.calls);
        let delay = self.delay;
        Box::pin(async move {
            // Count the *attempt* before any await, so a probe that the
            // caller times out still registers as a backend hit.
            calls.fetch_add(1, Ordering::SeqCst);
            if let Some(d) = delay {
                tokio::time::sleep(d).await;
            }
            Ok(matches.then_some(size))
        })
    }
}

/// Origin whose `size()` (a `HEAD`/`HeadObject` stand-in) always fails
/// with a transport error, and COUNTS the calls — the `Fault`-arm
/// counterpart to [`CountingSizeOrigin`].
#[derive(Debug)]
struct FailingSizeOrigin {
    calls: Arc<AtomicUsize>,
}

impl FailingSizeOrigin {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Self {
                calls: Arc::clone(&calls),
            },
            calls,
        )
    }
}

impl Origin for FailingSizeOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
    {
        Box::pin(async { Ok(OriginFetch::NotFound) })
    }

    fn size(
        &self,
        _hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, crate::OriginPullError>> + Send + '_>>
    {
        let calls = Arc::clone(&self.calls);
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Err(crate::OriginPullError::Transient(anyhow::anyhow!(
                "synthetic HEAD outage (connection refused)"
            )))
        })
    }
}

/// A per-origin transport error on the live `HEAD` — not just a timeout —
/// is `Fault`, never `Absent` (#1766 follow-up): `origin_probe_presence`
/// walks the origin chain itself rather than going through
/// [`CacheEngine::origin_size`], whose swallow-and-advance per-origin
/// error handling would otherwise launder a connection-refused/5xx/DNS
/// outage into a false `Ok(None)`. Also confirms the fault is never
/// memoised: a second probe re-walks the chain (call count 1 -> 2).
#[tokio::test]
async fn origin_probe_presence_transport_error_is_fault_not_absent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let hash = Hash::new(b"faulting origin probe");
    let (origin, calls) = FailingSizeOrigin::new();
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    assert_eq!(
        engine.origin_probe_presence(hash).await,
        OriginPresence::Fault,
        "a per-origin transport error must be Fault, not Absent",
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "one backend HEAD attempt");
    assert_eq!(
        engine.origin_probe_presence(hash).await,
        OriginPresence::Fault,
        "a fault is memoised under the fault TTL (#1789 item 6)",
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the second probe served the cached fault rather than re-walking \
         the origin chain — a steady state re-probes once per fault TTL, \
         not once per request",
    );
    assert_eq!(
        engine.origin_probe_size(hash).await,
        None,
        "the thin size wrapper still folds Fault to None",
    );
    Ok(())
}

/// A present remote object is discovered by a live `HEAD` and the answer is
/// memoised: a second probe for the same hash issues NO further backend call.
#[tokio::test]
async fn origin_probe_size_hits_origin_then_memoises_positive() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let hash = Hash::new(b"remote object");
    let (origin, calls) = CountingSizeOrigin::new(hash, 4096);
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    assert_eq!(
        engine.origin_probe_size(hash).await,
        Some(4096),
        "live HEAD finds it"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "one backend HEAD");
    assert_eq!(
        engine.origin_probe_size(hash).await,
        Some(4096),
        "served from memo"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "memo hit issues no second HEAD"
    );
    Ok(())
}

/// A 404 is memoised too — the whole point of the negative cache is that a
/// random-hash probe flood does not re-`HeadObject` the origin every time.
#[tokio::test]
async fn origin_probe_size_memoises_absent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let present = Hash::new(b"present");
    let absent = Hash::new(b"absent");
    let (origin, calls) = CountingSizeOrigin::new(present, 100);
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    assert_eq!(
        engine.origin_probe_size(absent).await,
        None,
        "not in origin"
    );
    assert_eq!(engine.origin_probe_size(absent).await, None, "still absent");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "negative answer is cached");
    Ok(())
}

/// A refused (denied/blacklisted/evicted) hash is never advertised, and the
/// guard short-circuits BEFORE any backend probe.
#[tokio::test]
async fn origin_probe_size_refused_never_probes() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let hash = Hash::new(b"denied object");
    let (origin, calls) = CountingSizeOrigin::new(hash, 100);
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
    engine.set_chain_denied_one(hash, true);

    assert_eq!(
        engine.origin_probe_size(hash).await,
        None,
        "refused stays hidden"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "no HEAD for a refused hash"
    );
    Ok(())
}

/// A slow origin must not stall the probe: the live HEAD is bounded by
/// `origin_probe_timeout_ms` and a timeout folds to `None` (safe — never
/// slashable).
#[tokio::test]
async fn origin_probe_size_times_out_to_absent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let hash = Hash::new(b"slow object");
    let (origin, _calls) = CountingSizeOrigin::slow(hash, 100, Duration::from_millis(400));
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
    // Tight timeout so the 400 ms origin overruns it.
    engine.set_origin_probe_config(OriginProbePolicy {
        positive_ttl: Duration::from_secs(15),
        negative_ttl: Duration::from_secs(2),
        fault_ttl: Duration::from_secs(5),
        timeout: Duration::from_millis(20),
        capacity: 16,
    });

    assert_eq!(
        engine.origin_probe_size(hash).await,
        None,
        "a HEAD slower than the ceiling folds to absent",
    );
    Ok(())
}

/// `origin_probe_presence` distinguishes `Present`/`Absent`/`Fault`
/// (#1766): a hash the origin holds is `Present(size)`, a hash it does not
/// is `Absent`, and a `HEAD` slower than the probe ceiling is `Fault` —
/// NOT `Absent`, so a caller can never sign an authoritative `NotFound`
/// off a backend blip.
#[tokio::test]
async fn origin_probe_presence_distinguishes_present_absent_and_fault() -> anyhow::Result<()> {
    // `Present`/`Absent` against a fast origin.
    let tmp = tempfile::tempdir()?;
    let present = Hash::new(b"present for presence");
    let absent = Hash::new(b"absent for presence");
    let (present_origin, _present_calls) = CountingSizeOrigin::new(present, 777);
    let engine = CacheEngine::open(
        tmp.path(),
        vec![Arc::new(present_origin) as Arc<dyn Origin>],
        10,
    )
    .await?;
    assert_eq!(
        engine.origin_probe_presence(present).await,
        OriginPresence::Present(777),
        "a live HEAD hit is Present, not folded away",
    );
    assert_eq!(
        engine.origin_probe_presence(absent).await,
        OriginPresence::Absent,
        "a genuine miss is Absent",
    );

    // `Fault` against a separate engine whose only origin overruns the
    // probe ceiling (a shared origin would delay the Present/Absent
    // probes above too, since `CountingSizeOrigin`'s delay is unconditional).
    let tmp2 = tempfile::tempdir()?;
    let slow = Hash::new(b"slow for presence");
    let (slow_origin, slow_calls) = CountingSizeOrigin::slow(slow, 100, Duration::from_millis(400));
    let fault_engine = CacheEngine::open(
        tmp2.path(),
        vec![Arc::new(slow_origin) as Arc<dyn Origin>],
        10,
    )
    .await?;
    // Tight timeout so the 400 ms origin overruns it.
    fault_engine.set_origin_probe_config(OriginProbePolicy {
        positive_ttl: Duration::from_secs(15),
        negative_ttl: Duration::from_secs(2),
        fault_ttl: Duration::from_secs(5),
        timeout: Duration::from_millis(20),
        capacity: 16,
    });
    assert_eq!(
        fault_engine.origin_probe_presence(slow).await,
        OriginPresence::Fault,
        "a HEAD slower than the ceiling is Fault, not Absent",
    );
    assert_eq!(
        slow_calls.load(Ordering::SeqCst),
        1,
        "one backend attempt for the first faulting probe",
    );
    assert_eq!(
        fault_engine.origin_probe_presence(slow).await,
        OriginPresence::Fault,
        "a memoised fault still answers Fault",
    );
    assert_eq!(
        slow_calls.load(Ordering::SeqCst),
        1,
        "the second probe served the cached fault instead of re-hitting \
         the backend (#1789 item 6)",
    );
    Ok(())
}

/// `origin_probe_size` stays a thin `Present -> Some`, `Absent`/`Fault ->
/// None` wrapper (#1766): callers that only care about advertising size
/// (probe `has_blob`, DHT announce) must not change behavior when a fault
/// is now distinguishable one layer down.
#[tokio::test]
async fn origin_probe_size_folds_fault_to_none_like_absent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let hash = Hash::new(b"slow object for size wrapper");
    let (origin, _calls) = CountingSizeOrigin::slow(hash, 100, Duration::from_millis(400));
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
    engine.set_origin_probe_config(OriginProbePolicy {
        positive_ttl: Duration::from_secs(15),
        negative_ttl: Duration::from_secs(2),
        fault_ttl: Duration::from_secs(5),
        timeout: Duration::from_millis(20),
        capacity: 16,
    });

    assert_eq!(
        engine.origin_probe_size(hash).await,
        None,
        "a fault still folds to None through the size wrapper",
    );
    Ok(())
}

/// `total_bytes` must sum the on-disk footprint — it is the eviction
/// driver's entire input, so a wrong sum silently mis-sizes every decision.
#[tokio::test]
async fn total_bytes_sums_populated_blobs() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"total-bytes payload";
    let hash = Hash::new(payload);
    let engine = CacheEngine::open(
        tmp.path(),
        vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
        10,
    )
    .await?;

    anyhow::ensure!(engine.total_bytes().await? == 0, "empty cache is 0 bytes");
    let _ = engine.get(hash).await?;

    let total = engine.total_bytes().await?;
    anyhow::ensure!(
        total >= payload.len() as u64,
        "total_bytes {total} should cover the {}-byte blob",
        payload.len()
    );
    let sizes = engine.size_snapshot().await?;
    anyhow::ensure!(
        sizes.get(&hash).copied() == Some(payload.len() as u64),
        "size_snapshot should report the blob's exact size"
    );
    Ok(())
}

/// The load-bearing distinction from `evict`: capacity eviction must NOT
/// write the durable takedown log, or every LRU victim would be permanently
/// un-servable and the log would grow without bound (#1173).
#[tokio::test]
async fn release_for_eviction_does_not_write_the_evicted_log() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"soft evict payload";
    let hash = Hash::new(payload);
    let engine = CacheEngine::open(
        tmp.path(),
        vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
        10,
    )
    .await?;
    let _ = engine.get(hash).await?;

    engine.release_for_eviction(hash).await?;

    anyhow::ensure!(
        !engine.is_evicted(hash),
        "soft evict must not enter the logical-evicted set"
    );
    anyhow::ensure!(
        !tmp.path().join("evicted.log").exists(),
        "soft evict must not create the durable takedown log"
    );
    // The access-time entry is forgotten, so the LRU driver won't re-pick it.
    anyhow::ensure!(
        engine.last_accessed(hash).is_none(),
        "soft evict should forget the access-time entry"
    );
    anyhow::ensure!(
        !engine.eviction_candidates().contains_key(&hash),
        "released hash should leave the candidate set"
    );
    Ok(())
}

/// The origin-held index (#1130) advertises fs-origin content by directory
/// enumeration, includes only *present* pins, and excludes refused hashes.
#[tokio::test]
async fn rescan_origins_indexes_fs_and_present_pins() -> anyhow::Result<()> {
    use crate::origin::FilesystemOrigin;

    async fn seed(base: &Path, payload: Vec<u8>) -> anyhow::Result<(Hash, u64)> {
        let hash = Hash::new(&payload);
        let hex = hash.to_hex();
        let shard = hex.get(..2).unwrap_or("");
        let dir = base.join(shard);
        tokio::fs::create_dir_all(&dir).await?;
        tokio::fs::write(dir.join(hex.as_str()), &payload).await?;
        let len = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        Ok((hash, len))
    }

    let origin_dir = tempfile::tempdir()?;
    let base = tokio::fs::canonicalize(origin_dir.path()).await?;
    let (h1, len1) = seed(&base, vec![1u8; 5000]).await?;
    let (h2, len2) = seed(&base, vec![2u8; 9000]).await?;

    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(FilesystemOrigin::new(&base).await?) as Arc<dyn Origin>;
    let engine = CacheEngine::open(cache_dir.path(), vec![origin], 10).await?;

    // Pin one present hash and one absent hash; only the present one is held.
    let absent = Hash::new(b"never-on-disk");
    engine.set_pinned(&PinnedHashes::new(
        [from_store_hash(h1), from_store_hash(absent)]
            .into_iter()
            .collect(),
    ));

    engine.rescan_origins().await;

    let held: HashSet<Hash> = engine.origin_held_snapshot().hashes;
    anyhow::ensure!(
        held.contains(&h1) && held.contains(&h2),
        "fs blobs must be enumerated into the held index",
    );
    anyhow::ensure!(!held.contains(&absent), "absent pin must not be held");
    anyhow::ensure!(engine.origin_held_size(h1) == Some(len1), "h1 size wrong");
    anyhow::ensure!(engine.origin_held_size(h2) == Some(len2), "h2 size wrong");
    anyhow::ensure!(
        engine.origin_held_size(absent).is_none(),
        "absent hash must not be servable from origin",
    );

    // A hash denied AFTER the last rescan must drop out of the *live* reads
    // immediately — the snapshot still lists it, but `refuses` is the
    // authority. This guards the #1130 blacklist-compliance interaction:
    // without it the probe would sign has_blob:true for a just-blacklisted
    // blob (g_node_04). No rescan between the deny and the assertions.
    engine.set_denied(&crate::DeniedHashes::new(
        [from_store_hash(h1)].into_iter().collect(),
    ));
    anyhow::ensure!(
        engine.origin_held_size(h1).is_none(),
        "a hash denied since the last rescan must not be servable from origin",
    );
    anyhow::ensure!(
        !engine.origin_held_snapshot().hashes.contains(&h1),
        "a hash denied since the last rescan must not be announced",
    );

    // And it also drops from the index proper on the next rescan.
    engine.set_denied(&crate::DeniedHashes::new(
        [from_store_hash(h1), from_store_hash(h2)]
            .into_iter()
            .collect(),
    ));
    engine.rescan_origins().await;
    anyhow::ensure!(
        engine.origin_held_size(h2).is_none(),
        "denied hash must not be advertised",
    );
    Ok(())
}

/// Origin that enumerates a fixed set and whose `size()` can be switched
/// from answering to faulting, standing in for a `HeadObject` throttle
/// window that opens between two rescans.
#[derive(Debug)]
struct ThrottlableOrigin {
    held: Vec<(Hash, u64)>,
    faulting: Arc<AtomicBool>,
    /// The fault is `Permanent` rather than `Transient` — a revoked ACL or a
    /// symlink escape, which no later rescan resolves.
    permanent: Arc<AtomicBool>,
    enumerate_fails: Arc<AtomicBool>,
    /// The origin still *lists* `held` but no longer serves it, so `size`
    /// answers an authoritative `Ok(None)`.
    holds: Arc<AtomicBool>,
    /// Milliseconds each `size` takes, so a test can keep a pass in flight
    /// while other triggers arrive.
    slow_ms: Arc<AtomicU64>,
    size_calls: Arc<AtomicUsize>,
}

impl ThrottlableOrigin {
    fn new(held: Vec<(Hash, u64)>) -> (Self, ThrottleControls) {
        let controls = ThrottleControls {
            faulting: Arc::new(AtomicBool::new(false)),
            permanent: Arc::new(AtomicBool::new(false)),
            enumerate_fails: Arc::new(AtomicBool::new(false)),
            holds: Arc::new(AtomicBool::new(true)),
            slow_ms: Arc::new(AtomicU64::new(0)),
            size_calls: Arc::new(AtomicUsize::new(0)),
        };
        (
            Self {
                held,
                faulting: Arc::clone(&controls.faulting),
                permanent: Arc::clone(&controls.permanent),
                enumerate_fails: Arc::clone(&controls.enumerate_fails),
                holds: Arc::clone(&controls.holds),
                slow_ms: Arc::clone(&controls.slow_ms),
                size_calls: Arc::clone(&controls.size_calls),
            },
            controls,
        )
    }
}

/// The switches and the probe counter a test drives [`ThrottlableOrigin`] by.
struct ThrottleControls {
    faulting: Arc<AtomicBool>,
    permanent: Arc<AtomicBool>,
    enumerate_fails: Arc<AtomicBool>,
    holds: Arc<AtomicBool>,
    slow_ms: Arc<AtomicU64>,
    size_calls: Arc<AtomicUsize>,
}

impl Origin for ThrottlableOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::S3
    }

    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
    {
        Box::pin(async { Ok(OriginFetch::NotFound) })
    }

    fn size(
        &self,
        hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, crate::OriginPullError>> + Send + '_>>
    {
        Box::pin(async move {
            self.size_calls.fetch_add(1, Ordering::SeqCst);
            let slow = self.slow_ms.load(Ordering::SeqCst);
            if slow > 0 {
                tokio::time::sleep(Duration::from_millis(slow)).await;
            }
            if self.faulting.load(Ordering::SeqCst) {
                return Err(if self.permanent.load(Ordering::SeqCst) {
                    crate::OriginPullError::Permanent(anyhow::anyhow!(
                        "synthetic HeadObject access denied"
                    ))
                } else {
                    crate::OriginPullError::Transient(anyhow::anyhow!(
                        "synthetic HeadObject throttle (503 SlowDown)"
                    ))
                });
            }
            if !self.holds.load(Ordering::SeqCst) {
                return Ok(None);
            }
            Ok(self
                .held
                .iter()
                .find(|(h, _)| *h == hash)
                .map(|(_, len)| *len))
        })
    }

    fn enumerate(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Hash>, crate::OriginPullError>> + Send + '_>> {
        Box::pin(async {
            if self.enumerate_fails.load(Ordering::SeqCst) {
                return Err(crate::OriginPullError::Transient(anyhow::anyhow!(
                    "synthetic ListObjectsV2 outage"
                )));
            }
            Ok(self.held.iter().map(|(h, _)| *h).collect())
        })
    }
}

/// A faulted size probe must not evict a hash from the origin-held index.
///
/// The index is what probe and DHT-announce advertise. A transport fault is
/// not an authoritative absence, so dropping on one makes a throttle window
/// the origin recovers from in seconds cost a whole rescan interval of
/// announce coverage — with every downstream signal reporting success.
#[tokio::test]
async fn rescan_origins_carries_a_faulted_probe_forward() -> anyhow::Result<()> {
    let indexed = Hash::new(b"throttle-indexed");
    let fresh = Hash::new(b"throttle-fresh");
    let (origin, controls) = ThrottlableOrigin::new(vec![(indexed, 5000), (fresh, 9000)]);
    let origin = Arc::new(origin) as Arc<dyn Origin>;

    let tmp = tempfile::tempdir()?;
    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![origin],
        10,
        // `fresh` is also pinned, so it reaches the probe loop twice — once
        // from the listing and once from the pin set. It must be probed once.
        crate::PinnedHashes::new([from_store_hash(fresh)].into_iter().collect()),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    // First rescan resolves `indexed` only: `fresh` is enumerated but the
    // origin is asked about it after the throttle opens.
    engine.set_denied(&crate::DeniedHashes::new(
        [from_store_hash(fresh)].into_iter().collect(),
    ));
    engine.rescan_origins().await;
    anyhow::ensure!(
        engine.origin_held_size(indexed) == Some(5000),
        "the healthy rescan must index the enumerated hash",
    );
    anyhow::ensure!(
        engine.origin_held_snapshot().probe_faults == 0,
        "a healthy rescan reports no faults",
    );

    // The throttle opens, and `fresh` becomes a candidate for the first time.
    engine.set_denied(&crate::DeniedHashes::new(HashSet::new()));
    controls.faulting.store(true, Ordering::SeqCst);
    controls.size_calls.store(0, Ordering::SeqCst);
    engine.rescan_origins().await;

    anyhow::ensure!(
        controls.size_calls.load(Ordering::SeqCst) == 2,
        "each candidate is probed once however many times it is listed, or a \
         duplicate inflates the fault count: {} probes for 2 candidates",
        controls.size_calls.load(Ordering::SeqCst),
    );

    anyhow::ensure!(
        engine.origin_held_size(indexed) == Some(5000),
        "a faulted probe must keep the previous index entry, not drop it",
    );
    anyhow::ensure!(
        engine.origin_held_size(fresh).is_none(),
        "a candidate first seen inside the fault window has nothing to carry forward",
    );
    anyhow::ensure!(
        engine.origin_held_snapshot().probe_faults == 2,
        "both faulted probes must be reported to the DHT seed paths, got {}",
        engine.origin_held_snapshot().probe_faults,
    );

    // The rescan is the metric's only source, so the counter and the
    // per-rescan report must agree.
    anyhow::ensure!(
        cm.origin_probe_failures.get() == 2,
        "faulted probes must surface on their counter, got {}",
        cm.origin_probe_failures.get(),
    );

    // A hash denied since the last rescan must not reach the announce set,
    // even though the index still lists it. `origin_held_snapshot` is what
    // the DHT seed reads, so the live refusal filter has to be applied there
    // and not only on the per-hash lookup.
    engine.set_denied(&crate::DeniedHashes::new(
        [from_store_hash(indexed)].into_iter().collect(),
    ));
    anyhow::ensure!(
        !engine.origin_held_snapshot().hashes.contains(&indexed),
        "a hash denied since the last rescan must not be announced",
    );
    engine.set_denied(&crate::DeniedHashes::new(HashSet::new()));

    // The throttle closes: the next rescan resolves both.
    controls.faulting.store(false, Ordering::SeqCst);
    engine.rescan_origins().await;
    anyhow::ensure!(
        engine.origin_held_size(fresh) == Some(9000),
        "a recovered origin must index what the fault window missed",
    );
    anyhow::ensure!(
        engine.origin_held_snapshot().probe_faults == 0,
        "a recovered rescan clears the fault report",
    );
    Ok(())
}

/// Carry-forward must not make an index entry immortal.
///
/// The counterpart risk of keeping a faulted candidate: a hash the origin
/// genuinely stopped holding has to leave, or the node advertises content it
/// cannot serve and every request for it becomes a refusal. Only a fault
/// carries an entry forward — an authoritative `Ok(None)` drops it.
#[tokio::test]
async fn rescan_origins_evicts_a_hash_the_origin_no_longer_holds() -> anyhow::Result<()> {
    let goes_away = Hash::new(b"origin-drops-it");
    let (origin, controls) = ThrottlableOrigin::new(vec![(goes_away, 4242)]);
    let origin = Arc::new(origin) as Arc<dyn Origin>;

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 10).await?;

    engine.rescan_origins().await;
    anyhow::ensure!(
        engine.origin_held_size(goes_away) == Some(4242),
        "the healthy rescan must index it",
    );

    // The origin still lists it but no longer holds it: `size` answers
    // `Ok(None)`, which is authoritative, not a fault.
    controls.holds.store(false, Ordering::SeqCst);
    engine.rescan_origins().await;

    anyhow::ensure!(
        engine.origin_held_size(goes_away).is_none(),
        "an authoritative absence must drop the entry, not carry it",
    );
    anyhow::ensure!(
        engine.origin_held_snapshot().probe_faults == 0,
        "a clean `not held` answer is not a fault",
    );
    Ok(())
}

/// Triggers arriving during a pass collapse into one rerun rather than
/// queueing.
///
/// A rescan gets slower exactly when the origin is faulting, which is when
/// the periodic trigger is most likely to fire on top of one. Queueing every
/// trigger would stack a waiter per tick and then run that backlog of
/// obsolete passes back to back, against an origin already struggling.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_rescans_collapse_into_one_rerun() -> anyhow::Result<()> {
    let listed = Hash::new(b"rescan-single-flight");
    let (origin, controls) = ThrottlableOrigin::new(vec![(listed, 11)]);
    let origin = Arc::new(origin) as Arc<dyn Origin>;

    let tmp = tempfile::tempdir()?;
    let engine = Arc::new(CacheEngine::open(tmp.path(), vec![origin], 10).await?);

    // Slow enough that the later triggers land while the first pass is
    // still probing.
    controls.slow_ms.store(50, Ordering::SeqCst);

    // Four triggers at once: one claims the slot, the rest collapse into a
    // single queued rerun — two passes over the one candidate, not four.
    controls.size_calls.store(0, Ordering::SeqCst);
    let mut joins = Vec::new();
    for _ in 0..4 {
        let engine = Arc::clone(&engine);
        joins.push(tokio::spawn(async move { engine.rescan_origins().await }));
    }
    for j in joins {
        j.await?;
    }

    let probes = controls.size_calls.load(Ordering::SeqCst);
    anyhow::ensure!(
        (1..=2).contains(&probes),
        "four triggers must collapse into at most one rerun, so at most two \
         passes probe the single candidate; saw {probes}",
    );
    anyhow::ensure!(
        engine.origin_held_size(listed) == Some(11),
        "and the index is still published",
    );

    // The slot is released, so a later trigger still runs.
    controls.slow_ms.store(0, Ordering::SeqCst);
    controls.size_calls.store(0, Ordering::SeqCst);
    engine.rescan_origins().await;
    anyhow::ensure!(
        controls.size_calls.load(Ordering::SeqCst) == 1,
        "a rescan after the burst must still run, or the slot is stranded",
    );
    Ok(())
}

/// A permanent fault must not carry an entry forward.
///
/// A revoked ACL or a symlink escape reads the same on every rescan, so
/// carrying it would hold the hash in the announce set until an operator
/// intervened — advertising content the serve path refuses, indefinitely.
#[tokio::test]
async fn rescan_origins_does_not_carry_a_permanent_fault() -> anyhow::Result<()> {
    let revoked = Hash::new(b"origin-acl-revoked");
    let (origin, controls) = ThrottlableOrigin::new(vec![(revoked, 777)]);
    let origin = Arc::new(origin) as Arc<dyn Origin>;

    let tmp = tempfile::tempdir()?;
    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![origin],
        10,
        crate::PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    engine.rescan_origins().await;
    anyhow::ensure!(
        engine.origin_held_size(revoked) == Some(777),
        "the healthy rescan must index it",
    );

    controls.faulting.store(true, Ordering::SeqCst);
    controls.permanent.store(true, Ordering::SeqCst);
    engine.rescan_origins().await;

    let report = engine.origin_held_snapshot();
    anyhow::ensure!(
        !report.hashes.contains(&revoked),
        "a permanent fault must drop the entry rather than advertise content \
         the serve path will refuse for the process lifetime",
    );
    anyhow::ensure!(
        report.probe_faults == 1,
        "it is still a probe that could not answer, so it still counts",
    );
    Ok(())
}

/// An origin that cannot be listed drops every hash discoverable only
/// through it, and must say so.
///
/// This is the more severe half of the same silent shrink: no candidate is
/// produced, so there is no per-hash fault and nothing to carry forward. A
/// seed reading only the probe-fault count would call the truncated set
/// healthy.
#[tokio::test]
async fn rescan_origins_reports_an_origin_it_could_not_list() -> anyhow::Result<()> {
    let listed = Hash::new(b"listing-outage");
    let (origin, controls) = ThrottlableOrigin::new(vec![(listed, 1234)]);
    let origin = Arc::new(origin) as Arc<dyn Origin>;

    let tmp = tempfile::tempdir()?;
    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![origin],
        10,
        crate::PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    engine.rescan_origins().await;
    anyhow::ensure!(
        engine.origin_held_size(listed) == Some(1234),
        "the healthy rescan must index the listed hash",
    );

    controls.enumerate_fails.store(true, Ordering::SeqCst);
    engine.rescan_origins().await;

    let report = engine.origin_held_snapshot();
    anyhow::ensure!(
        report.enumerate_failures == 1,
        "the failed listing must be reported, got {}",
        report.enumerate_failures,
    );
    anyhow::ensure!(
        report.probe_faults == 0,
        "no candidate was produced, so there is no probe to fault",
    );
    anyhow::ensure!(
        !report.hashes.contains(&listed),
        "an unlisted, unpinned hash genuinely leaves the announce set — the \
         point is that the report says so",
    );
    anyhow::ensure!(
        cm.origin_enumerate_failures.get() == 1,
        "the failed listing must surface on its counter, got {}",
        cm.origin_enumerate_failures.get(),
    );
    Ok(())
}

/// A pinned hash must be refused with `Ok(0)` and left completely untouched.
/// The driver relies on the `0` to avoid crediting a no-op as freed bytes.
#[tokio::test]
async fn release_for_eviction_refuses_pinned_hash() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"pinned payload";
    let hash = Hash::new(payload);
    let engine = CacheEngine::open(
        tmp.path(),
        vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
        10,
    )
    .await?;
    let _ = engine.get(hash).await?;

    engine.set_pinned(&PinnedHashes::new(
        [from_store_hash(hash)].into_iter().collect(),
    ));

    let released = engine.release_for_eviction(hash).await?;
    anyhow::ensure!(released == 0, "pinned hash must report 0 released");
    anyhow::ensure!(
        engine.last_accessed(hash).is_some(),
        "pinned refusal must not forget the access-time entry"
    );
    anyhow::ensure!(engine.has(hash).await?, "pinned blob must still be present");
    Ok(())
}

/// The pin exemption is carved out for a deny-listed hash: "deny wins over
/// pin" on the space path too, so a pinned + governance-denied hash is
/// reclaimable here rather than held on disk (unservable) until the takedown
/// `evict()` runs.
#[tokio::test]
async fn release_for_eviction_reclaims_a_deny_listed_pinned_hash() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"pinned then denied";
    let hash = Hash::new(payload);
    let engine = CacheEngine::open(
        tmp.path(),
        vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
        10,
    )
    .await?;
    let _ = engine.get(hash).await?;
    engine.set_pinned(&PinnedHashes::new(
        [from_store_hash(hash)].into_iter().collect(),
    ));

    // Pinned + clean: still exempt from space reclaim.
    anyhow::ensure!(
        engine.release_for_eviction(hash).await? == 0,
        "a pinned clean hash stays exempt"
    );
    anyhow::ensure!(
        engine.last_accessed(hash).is_some(),
        "the clean exemption must keep the access-time entry"
    );

    // Governance denies it → the pin no longer exempts it.
    anyhow::ensure!(
        engine.set_chain_denied_one(hash, true),
        "deny must change the set"
    );
    engine.release_for_eviction(hash).await?;
    anyhow::ensure!(
        engine.last_accessed(hash).is_none(),
        "a pinned + governance-denied hash must go through the reclaim path (which \
         forgets the access-time entry), not the pin early-return"
    );
    Ok(())
}

#[tokio::test]
async fn get_cache_hit_records_access_time() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"hello cache hit";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    // Prime the cache via pull-through.
    let _ = engine.get(hash).await?;

    // Clear the access time so the next get proves a cache-hit path.
    engine.inner.access_times.clear();

    // Read again — this time it's a local hit.
    let _ = engine.get(hash).await?;

    anyhow::ensure!(
        engine.last_accessed(hash).is_some(),
        "expected Some(Instant) after cache-hit get"
    );
    Ok(())
}

#[tokio::test]
async fn pull_through_records_access_time() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"hello pull-through";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    // First get triggers pull-through.
    let _ = engine.get(hash).await?;

    anyhow::ensure!(
        engine.last_accessed(hash).is_some(),
        "expected Some(Instant) after pull-through get"
    );
    Ok(())
}

/// The `FillMode` ↔ return-shape correspondence, asserted where the private
/// types are visible — the integration tests in `tests/pull_through.rs`
/// cannot see `PullThroughOutcome`, so they can only observe that the fill
/// happened, not which arm produced it.
///
/// This is the property the enum exists to carry, and it must hold in BOTH
/// directions: `ReturnBytes` always yields `Some`, `CommitOnly` always yields
/// `None`. Before `FillMode` the second half was false on the buffered arm,
/// which handed its drain buffer back regardless — an exception that made the
/// wrappers partial and the mode untrustworthy to read.
///
/// Deliberately run through the BUFFERED arm: `StubOrigin` advertises a size
/// hint well under `buffered_max_bytes`, so `should_buffer` routes here. The
/// streaming arm never had the defect.
#[tokio::test]
async fn fill_mode_determines_the_return_shape_in_both_directions() -> anyhow::Result<()> {
    let payload = b"drained, not streamed";
    let hash = Hash::new(payload);

    let tmp_fill = tempfile::tempdir()?;
    let engine = CacheEngine::open(
        tmp_fill.path(),
        vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
        10,
    )
    .await?;
    let committed = engine
        .pull_through(hash, false, FillMode::CommitOnly)
        .await?;
    anyhow::ensure!(
        committed.is_none(),
        "CommitOnly must yield no payload, got {committed:?}"
    );

    let tmp_bytes = tempfile::tempdir()?;
    let engine = CacheEngine::open(
        tmp_bytes.path(),
        vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
        10,
    )
    .await?;
    let returned = engine
        .pull_through(hash, false, FillMode::ReturnBytes)
        .await?;
    anyhow::ensure!(
        returned.as_deref() == Some(payload.as_slice()),
        "ReturnBytes must yield the payload, got {returned:?}"
    );
    Ok(())
}

#[tokio::test]
async fn subscribe_inserts_emits_on_pull_through_success() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"hello dht hook";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
    let mut rx = engine.subscribe_inserts();

    // Successful pull-through must announce the hash to the
    // subscriber. The DHT republish scheduler (#320)
    // consumes this stream to drive `Store` fan-out to the K+3
    // closest peers.
    let _ = engine.get(hash).await?;
    let announced = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for subscribe_inserts emit"))?
        .map_err(|e| anyhow::anyhow!("recv: {e}"))?;
    anyhow::ensure!(
        announced == hash,
        "expected announced hash to equal committed hash"
    );
    Ok(())
}

#[tokio::test]
async fn subscribe_inserts_does_not_emit_on_cache_hit() -> anyhow::Result<()> {
    // A `get` that hits the local store (no pull-through, no new
    // commit) must NOT emit on the channel — only fresh commits
    // do, because the consumer's job is to schedule a NEW publish
    // cycle for newly-cached blobs.
    let tmp = tempfile::tempdir()?;
    let payload = b"hello cached-hit";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    // First get: pull-through, should emit. Drain that emission so
    // the channel is empty before the second get.
    let mut rx = engine.subscribe_inserts();
    let _ = engine.get(hash).await?;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .map_err(|_| anyhow::anyhow!("first pull-through emission missing"))?
        .map_err(|e| anyhow::anyhow!("recv first: {e}"))?;

    // Second get: cache hit. No emission expected.
    let _ = engine.get(hash).await?;
    let r = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await;
    anyhow::ensure!(
        r.is_err(),
        "cache hit must not emit on subscribe_inserts; got {r:?}"
    );
    Ok(())
}

#[tokio::test]
async fn second_get_updates_access_time() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"hello update";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    // First access (pull-through).
    let _ = engine.get(hash).await?;
    let first = engine
        .last_accessed(hash)
        .ok_or_else(|| anyhow::anyhow!("expected Some after first get"))?;

    // Burn a tiny bit of real wall-clock time so Instant::now() advances.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    // Second access (cache hit).
    let _ = engine.get(hash).await?;
    let second = engine
        .last_accessed(hash)
        .ok_or_else(|| anyhow::anyhow!("expected Some after second get"))?;

    anyhow::ensure!(
        second > first,
        "access time should advance: first={first:?}, second={second:?}"
    );
    Ok(())
}

#[tokio::test]
async fn observe_hit_forwards_to_frequency_estimator() -> anyhow::Result<()> {
    use crate::policy::FrequencyEstimator;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[derive(Debug, Default)]
    struct Counter(AtomicU32);
    impl FrequencyEstimator for Counter {
        fn observe(&self, _h: Hash) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn estimate(&self, _h: Hash) -> u32 {
            self.0.load(Ordering::Relaxed)
        }
    }

    let tmp = tempfile::tempdir()?;
    let payload = b"hello frequency";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    let counter = Arc::new(Counter::default());
    engine.set_frequency_estimator(counter.clone());

    let _ = engine.get(hash).await?;

    anyhow::ensure!(counter.estimate(hash) >= 1, "observe should fire on access");
    Ok(())
}

/// ADR 040 serve-hit signal: the fill path emits NO hit sighting (so a
/// fill-and-serve miss never double-counts), and each served request emits
/// exactly one sighting through the serve chokepoint's
/// [`CacheEngine::observe_hit`]. A hot RESIDENT blob served repeatedly must
/// therefore accumulate frequency and become promotable — the case that was
/// inverted before serve paths emitted the signal.
#[tokio::test]
async fn serve_hit_signal_fires_once_per_serve_and_promotes_a_hot_resident_blob()
-> anyhow::Result<()> {
    use crate::policy::{FrequencyEstimator, ProbationAdmission, Segment, TinyLfuEstimator};

    let tmp = tempfile::tempdir()?;
    let payload = b"hot resident blob";
    let hash = Hash::new(payload);
    let engine = CacheEngine::open(
        tmp.path(),
        vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
        10,
    )
    .await?;

    let promotion_threshold = 3u32;
    let freq: Arc<dyn FrequencyEstimator> = Arc::new(TinyLfuEstimator::new(4096));
    engine.set_frequency_estimator(freq.clone());
    engine.set_admission_policy(Arc::new(ProbationAdmission {
        freq: freq.clone(),
        promotion_threshold,
    }));

    // Fill as a miss. The fill path reads the estimate for admission (0 ->
    // Probation) but must NOT emit the hit signal, so estimate stays 0 — this
    // is what keeps a fill-and-serve miss at one sighting, not two.
    engine.populate_local(hash).await?;
    anyhow::ensure!(
        freq.estimate(hash) == 0,
        "the fill alone must not observe (no double-count with the serve)"
    );
    anyhow::ensure!(
        engine.segment_of(hash) == Segment::Probation,
        "first sight lands in probation"
    );

    // Serve the resident blob repeatedly. Each served request is exactly one
    // sighting via the serve chokepoint's `observe_hit`.
    for i in 1..=promotion_threshold {
        engine.observe_hit(hash);
        anyhow::ensure!(
            freq.estimate(hash) == i,
            "each serve must be exactly one sighting"
        );
    }
    anyhow::ensure!(
        freq.estimate(hash) >= promotion_threshold,
        "a hot resident blob served repeatedly must become promotable"
    );
    Ok(())
}

#[tokio::test]
async fn default_admission_is_main_segment() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"hello admission";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    let ctx = crate::policy::AdmissionContext {
        hash,
        known_size: None,
    };
    assert_eq!(
        engine.admission_segment_for_test(&ctx),
        crate::policy::Segment::Main
    );
    Ok(())
}

#[tokio::test]
async fn access_times_snapshot_contains_accessed_hash() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"hello snapshot";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    let _ = engine.get(hash).await?;

    let snap = engine.access_times_snapshot();
    anyhow::ensure!(
        snap.contains_key(&hash),
        "snapshot should contain the accessed hash"
    );
    Ok(())
}

/// A serve completion's access record must not wait on a scan of the rest of
/// the access map.
///
/// `observe_hit` -> `record_access` is the terminal bookkeeping of every
/// completed serve, and `eviction_candidates` walks every entry on the
/// eviction sweep. The map is sharded, so the two meet on one shard at a
/// time: with a scan pinned inside one shard, a record for a hash that lives
/// in another shard still lands. Under one map-wide lock that record waits
/// for the whole scan, and the bounded wait below expires.
#[tokio::test]
async fn record_access_does_not_wait_on_a_scan_of_another_shard() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;

    // Stand in for a scan sitting inside one shard by holding a guard on
    // that shard. `get_mut` takes the write guard rather than the read guard
    // `eviction_candidates`' walk takes, which is the stronger hold: if a
    // record can land against an exclusive guard on another shard, it can
    // land against a shared one. What is pinned is the shard, which is the
    // property under test.
    let scanned = Hash::new(b"the shard under scan");
    engine.inner.access_times.insert(scanned, Instant::now());
    let Some(scan_guard) = engine.inner.access_times.get_mut(&scanned) else {
        anyhow::bail!("the seeded access-time entry must be present");
    };

    // Find a hash that lives outside the pinned shard: `try_get` reports
    // `Locked` for that shard alone and `Absent` for every other one.
    let completing = (0..1024u32).map(|i| Hash::new(i.to_le_bytes())).find(|h| {
        !matches!(
            engine.inner.access_times.try_get(h),
            dashmap::try_result::TryResult::Locked
        )
    });
    let Some(completing) = completing else {
        anyhow::bail!("no candidate hash landed outside the pinned shard");
    };

    // Record the completion from another thread so a wait is observable as a
    // timeout rather than a hung test.
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let recorder = {
        let engine = engine.clone();
        std::thread::spawn(move || {
            engine.observe_hit(completing);
            let _ = tx.send(());
        })
    };
    let landed = rx.recv_timeout(Duration::from_secs(10)).is_ok();

    // Release the scan and reap the recorder before asserting, so a failure
    // reports rather than leaks the thread.
    drop(scan_guard);
    let joined = recorder.join().is_ok();

    anyhow::ensure!(
        landed,
        "a serve completion's access record waited on a scan of another shard"
    );
    anyhow::ensure!(joined, "the recording thread panicked");
    anyhow::ensure!(
        engine.last_accessed(completing).is_some(),
        "the recorded access must be readable once the scan releases"
    );
    Ok(())
}

/// A concurrent record must not make the eviction sweep *lose* a candidate.
///
/// The sharded walk is not point-in-time, by design: a record landing
/// mid-walk may or may not appear. What it must never do is drop a
/// hash that was already in the map when the walk started, because
/// `eviction_candidates` is the only source of eviction candidates — a hash
/// silently skipped by every sweep is a blob that is never reclaimed, which
/// is unbounded disk growth. `DashMap::iter` holds each shard's read guard
/// for that shard's traversal, so a concurrent insert can add to a shard the
/// walk has not reached but cannot remove from one it has. This pins that.
///
/// A round only counts once its result carries a hash the writer produced
/// *after the walk began* — the writer's counter is sampled either side of
/// the walk, and only that window's keys are accepted as witnesses. A key
/// written before the walk started proves nothing: the walk would find it in
/// a quiet map too. Scheduling decides whether a given round lands one, so
/// rounds repeat until one does; the no-candidate-lost invariant is checked
/// on every round either way.
///
/// Each round writes into its own [`ROUND_STRIDE`]-wide key range, and the
/// range is asserted wide enough to hold that round's writes. Sharing one
/// range would make the witness vacuous from round two on: nothing evicts
/// the previous round's keys from `access_times`, so every later round would
/// "find" keys that were already there before its walk started, and a run
/// where the walk never overlapped the writer would report success.
#[tokio::test]
async fn a_scan_never_loses_a_candidate_to_a_concurrent_record() -> anyhow::Result<()> {
    const WRITER_BASE: u32 = 1_000_000;
    /// Keys per round. Rounds must not share keys (see the docstring), and
    /// a round that outran this would start reusing the next round's range.
    const ROUND_STRIDE: u32 = 10_000_000;
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;

    // Seed enough hashes to spread across every shard and to give the walk
    // enough work that a concurrent writer can get inside it.
    let seeded: Vec<Hash> = (0..4096u32).map(|i| Hash::new(i.to_le_bytes())).collect();
    for h in &seeded {
        engine.inner.access_times.insert(*h, Instant::now());
    }

    let mut overlapped = false;
    for round in 0..16u32 {
        let base = WRITER_BASE.saturating_add(round.saturating_mul(ROUND_STRIDE));
        // Hammer the map with fresh hashes for the duration of the walk.
        let stop = Arc::new(AtomicBool::new(false));
        let written = Arc::new(AtomicU64::new(0));
        let writer = {
            let engine = engine.clone();
            let stop = Arc::clone(&stop);
            let written = Arc::clone(&written);
            std::thread::spawn(move || {
                let mut i = base;
                while !stop.load(Ordering::Relaxed) {
                    engine.observe_hit(Hash::new(i.to_le_bytes()));
                    written.fetch_add(1, Ordering::Relaxed);
                    i = i.saturating_add(1);
                }
            })
        };

        // Do not start the walk until the writer is provably running, or the
        // scan can finish before the thread is even scheduled. Bounded, so a
        // writer that dies before its first record fails the test instead of
        // hanging it with the panic trapped in an unjoined thread.
        let spin_deadline = Instant::now() + Duration::from_secs(10);
        while written.load(Ordering::Relaxed) == 0 {
            anyhow::ensure!(
                Instant::now() < spin_deadline,
                "the recording thread never recorded an access"
            );
            std::thread::yield_now();
        }

        // The writer bumps its counter *after* the insert lands, so a key
        // index below `before` is certainly already in the map and one at or
        // above `after` is certainly not yet. Index `before` itself is the
        // ambiguous one, which is why the witness range below skips it.
        let before = written.load(Ordering::Relaxed);
        let candidates = engine.eviction_candidates();
        let after = written.load(Ordering::Relaxed);
        stop.store(true, Ordering::Relaxed);
        anyhow::ensure!(writer.join().is_ok(), "the recording thread panicked");

        let missing = seeded
            .iter()
            .filter(|h| !candidates.contains_key(h))
            .count();
        anyhow::ensure!(
            missing == 0,
            "the sweep dropped {missing} of {} pre-existing candidates",
            seeded.len()
        );

        anyhow::ensure!(
            after < u64::from(ROUND_STRIDE),
            "round wrote {after} keys, overrunning its {ROUND_STRIDE}-key range"
        );

        // Did this round's walk actually see a record that landed inside it?
        // The writer bumps its counter *after* the insert lands, so at the
        // instant `before` was read the key at index `before` may already be
        // in the map — skip it and start at the first index that cannot be.
        let during: HashSet<Hash> = (before.saturating_add(1)..after)
            .filter_map(|n| u32::try_from(n).ok())
            .map(|n| Hash::new(base.saturating_add(n).to_le_bytes()))
            .collect();
        overlapped |= candidates.iter().any(|(h, _)| during.contains(h));
        if overlapped {
            break;
        }
    }
    anyhow::ensure!(
        overlapped,
        "no round overlapped the writer, so the walk was never concurrent"
    );
    Ok(())
}

#[tokio::test]
async fn last_accessed_returns_none_for_unknown_hash() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    let unknown = Hash::new(b"never accessed");

    anyhow::ensure!(
        engine.last_accessed(unknown).is_none(),
        "expected None for a hash that was never accessed"
    );
    Ok(())
}

#[tokio::test]
async fn iter_hashes_returns_empty_on_empty_store() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    let hashes = engine.iter_hashes().await?;
    anyhow::ensure!(
        hashes.is_empty(),
        "expected empty iter_hashes on a fresh store, got {hashes:?}"
    );
    Ok(())
}

#[tokio::test]
async fn iter_hashes_returns_all_committed_blobs() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payloads: &[&[u8]] = &[b"iter-a", b"iter-b", b"iter-c"];
    let origin = MultiStubOrigin::new(payloads);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    let mut expected: HashSet<Hash> = HashSet::new();
    for p in payloads {
        let h = Hash::new(*p);
        let _ = engine.get(h).await?;
        expected.insert(h);
    }

    // Use a HashSet for the comparison — iroh-blobs `list()` order is
    // not contractually stable.
    let actual: HashSet<Hash> = engine.iter_hashes().await?.into_iter().collect();
    anyhow::ensure!(actual == expected, "expected {expected:?}, got {actual:?}");
    Ok(())
}

#[tokio::test]
async fn iter_hashes_excludes_evicted_blobs() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payloads: &[&[u8]] = &[b"evict-a", b"evict-b", b"evict-c", b"evict-d"];
    let origin = MultiStubOrigin::new(payloads);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    let hashes: Vec<Hash> = payloads.iter().map(|p| Hash::new(*p)).collect();
    for h in &hashes {
        let _ = engine.get(*h).await?;
    }

    // Evict half — odd indices.
    let mut kept: Vec<Hash> = Vec::new();
    let mut evicted: Vec<Hash> = Vec::new();
    for (i, h) in hashes.iter().enumerate() {
        if i % 2 == 0 {
            kept.push(*h);
        } else {
            evicted.push(*h);
        }
    }
    for h in &evicted {
        engine.evict(*h).await?;
    }

    let actual: HashSet<Hash> = engine.iter_hashes().await?.into_iter().collect();
    let kept_set: HashSet<Hash> = kept.iter().copied().collect();

    anyhow::ensure!(
        actual == kept_set,
        "iter_hashes must return exactly the non-evicted set; expected {kept_set:?}, got {actual:?}"
    );
    Ok(())
}

/// Pinned blobs MUST appear in `iter_hashes`: pinning protects against
/// LRU eviction, not against DHT republish. They're the most valuable
/// content to surface, so they have to seed the cold-start scheduler
/// alongside everything else. Locks the contract against a future
/// "exclude pinned for symmetry with `access_times_snapshot`" refactor.
#[tokio::test]
async fn iter_hashes_includes_pinned_blobs() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload_pinned: &[u8] = b"pin-a";
    let payload_plain: &[u8] = b"pin-b";
    let origin = MultiStubOrigin::new(&[payload_pinned, payload_plain]);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    let h_pinned = Hash::new(payload_pinned);
    let h_plain = Hash::new(payload_plain);
    let _ = engine.get(h_pinned).await?;
    let _ = engine.get(h_plain).await?;

    let s = [from_store_hash(h_pinned)].into_iter().collect();
    let diff = engine.set_pinned(&PinnedHashes::new(s));
    // Without this guard, a future bug where set_pinned silently
    // no-ops would let the test pass on the strength of pre-pin
    // presence alone.
    anyhow::ensure!(
        diff.added == 1 && diff.removed == 0,
        "set_pinned must apply the pin to make this test meaningful, got {diff:?}"
    );

    let actual: HashSet<Hash> = engine.iter_hashes().await?.into_iter().collect();
    let expected: HashSet<Hash> = [h_pinned, h_plain].into_iter().collect();
    anyhow::ensure!(
        actual == expected,
        "iter_hashes must include pinned blobs (they're the highest-value DHT advertisements); expected {expected:?}, got {actual:?}"
    );
    Ok(())
}

/// An origin that sleeps before returning, counting how many times
/// `fetch` was invoked. Used to verify coalescing of concurrent pulls.
#[derive(Debug)]
struct SlowCountingOrigin {
    data: Bytes,
    hash: Hash,
    fetch_count: AtomicUsize,
    delay: std::time::Duration,
}

impl SlowCountingOrigin {
    fn new(payload: &[u8], delay: std::time::Duration) -> Self {
        Self {
            hash: Hash::new(payload),
            data: Bytes::from(payload.to_vec()),
            fetch_count: AtomicUsize::new(0),
            delay,
        }
    }
}

impl Origin for SlowCountingOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch(
        &self,
        hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
    {
        self.fetch_count.fetch_add(1, Ordering::SeqCst);
        let result = if hash == self.hash {
            Ok(OriginFetch::found_one_shot(self.data.clone()))
        } else {
            Ok(OriginFetch::NotFound)
        };
        let delay = self.delay;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            result
        })
    }
}

#[tokio::test]
async fn concurrent_gets_coalesce_into_single_origin_fetch() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"coalesce me";
    let hash = Hash::new(payload);
    let origin = Arc::new(SlowCountingOrigin::new(
        payload,
        std::time::Duration::from_millis(50),
    ));

    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![origin.clone() as Arc<dyn Origin>],
        10,
        crate::PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    // Spawn several concurrent gets for the same hash.
    let mut handles = Vec::new();
    for _ in 0..5 {
        let e = engine.clone();
        handles.push(tokio::spawn(async move { e.get(hash).await }));
    }

    // Await all — they should all succeed.
    for handle in handles {
        let result = handle
            .await
            .map_err(|e| anyhow::anyhow!("task join: {e}"))?;
        anyhow::ensure!(result.is_ok(), "expected Ok, got {result:?}");
    }

    // The origin should have been called at most once (coalesced).
    let count = origin.fetch_count.load(Ordering::SeqCst);
    anyhow::ensure!(count == 1, "expected exactly 1 origin fetch, got {count}");

    // Counter accounting under coalescing (#418): exactly one task
    // becomes the owner and counts as a miss; the other four are
    // waiter-retry hits. A regression that moved the waiter-hit
    // bump out of `engine.rs::get`'s waiter branch would silently
    // mis-classify every coalesced workload as a miss-storm.
    anyhow::ensure!(
        cm.misses.get() == 1,
        "owner pulls once → exactly 1 miss, got {}",
        cm.misses.get()
    );
    anyhow::ensure!(
        cm.hits.get() == 4,
        "4 waiters retry into a hit, got {} hits",
        cm.hits.get()
    );
    anyhow::ensure!(
        cm.pull_through_bytes.get() == payload.len() as u64,
        "single origin fetch → pull_through_bytes == payload.len()"
    );
    anyhow::ensure!(
        cm.bytes_returned.get() == (payload.len() as u64) * 5,
        "all 5 callers got the bytes back, so bytes_returned == 5 * payload.len()"
    );
    Ok(())
}

#[tokio::test]
async fn inflight_map_is_empty_after_pull_completes() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"cleanup check";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    let _ = engine.get(hash).await?;

    let inflight_len = inflight_len(&engine);
    anyhow::ensure!(
        inflight_len == 0,
        "inflight map should be empty after pull, had {inflight_len} entries"
    );
    Ok(())
}

/// Cancelling the owner mid-pull must not leave the inflight entry
/// orphaned — otherwise every subsequent `get()` for the same hash hangs
/// on a `Notify` that never fires. The `InflightGuard`'s `Drop` impl
/// wakes waiters and clears the entry even on cancellation.
#[tokio::test]
async fn cancelled_owner_does_not_orphan_inflight_entry() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"cancel test";
    let hash = Hash::new(payload);
    let origin = SlowCountingOrigin::new(payload, std::time::Duration::from_secs(10));

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    // Spawn the owner with a tiny timeout so it gets cancelled mid-pull.
    let owner_engine = engine.clone();
    let owner = tokio::spawn(async move {
        tokio::time::timeout(std::time::Duration::from_millis(50), owner_engine.get(hash)).await
    });
    // Wait for the owner task to finish (timeout fires → future dropped).
    let _ = owner.await?;

    // The inflight map must be empty — InflightGuard::drop ran on cancel.
    let inflight_len = inflight_len(&engine);
    anyhow::ensure!(
        inflight_len == 0,
        "inflight map should be empty after cancellation, had {inflight_len} entries"
    );
    Ok(())
}

// ----- In-flight mutex poisoning (#1517) -----

/// Poison `inner.inflight` the only way a `std::sync::Mutex` can be
/// poisoned: panic while holding the guard.
///
/// This is synthetic by construction — the workspace anti-panic policy
/// (`unwrap_used` / `expect_used` / `panic` denied) is precisely why no
/// production path can do this, and precisely why a real firing would be
/// a bug worth an `error!` rather than a condition worth tuning.
#[allow(clippy::panic)]
fn poison_inflight(engine: &CacheEngine) {
    let inner = Arc::clone(&engine.inner);
    let handle = std::thread::spawn(move || {
        let _guard = inner.inflight.lock();
        panic!("deliberately poisoning the inflight mutex");
    });
    // Panicked by construction, so `join` returns the panic payload.
    drop(handle.join());
}

/// Read the map length without laundering poison into a false `0` —
/// `lock().ok()` would report an empty map on a poisoned mutex and make
/// the orphan assertion below vacuously true.
fn inflight_len(engine: &CacheEngine) -> usize {
    engine
        .inner
        .inflight
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .len()
}

/// The load-bearing half of #1517: a poisoned coalescing mutex must not
/// cost origin egress. Before the fix both `get` call sites discarded the
/// `PoisonError` and fell through to a *direct* pull, and std poison is
/// sticky for the process lifetime — so one panic permanently turned every
/// concurrent request for a missing blob into its own origin fetch. On a
/// metered `http`/`s3` origin that is an unbounded egress multiplier; on
/// the `Peer` origin reached via `populate` it is a USDC double-spend.
///
/// `fetch_count == 1` under a poisoned mutex is the whole assertion.
#[tokio::test]
async fn poisoned_inflight_mutex_still_coalesces_and_is_counted() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"coalesce under poison";
    let hash = Hash::new(payload);
    let origin = Arc::new(SlowCountingOrigin::new(
        payload,
        std::time::Duration::from_millis(50),
    ));

    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![origin.clone() as Arc<dyn Origin>],
        10,
        crate::PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    poison_inflight(&engine);

    let mut handles = Vec::new();
    for _ in 0..5 {
        let e = engine.clone();
        handles.push(tokio::spawn(async move { e.get(hash).await }));
    }
    for handle in handles {
        let result = handle
            .await
            .map_err(|e| anyhow::anyhow!("task join: {e}"))?;
        anyhow::ensure!(result.is_ok(), "expected Ok, got {result:?}");
    }

    let count = origin.fetch_count.load(Ordering::SeqCst);
    anyhow::ensure!(
        count == 1,
        "coalescing must survive poison — expected exactly 1 origin fetch, got {count}"
    );
    // Exactly one: `lock_inflight` clears the poison, so a single
    // panic is a single bump no matter how many locks follow it.
    // Asserting the exact value is what pins that — `> 0` would pass
    // just as happily if the clear were dropped and the counter
    // climbed with request volume.
    let bumps = cm.inflight_mutex_poisoned.get();
    anyhow::ensure!(
        bumps == 1,
        "one poisoning must count exactly once, got {bumps}"
    );
    anyhow::ensure!(
        inflight_len(&engine) == 0,
        "the owner's guard must still clear its entry under poison"
    );
    Ok(())
}

/// Two separate poisonings count twice while the log stays latched at one.
///
/// Two poisonings, not one, is the whole point: because `lock_inflight`
/// clears the poison, a single panic yields a single bump, so only a
/// *second* panic can show the counter advancing past the latch. This is
/// also the test that pins "counts poisonings, not locks-since-a-poisoning"
/// — drop the `clear_poison` and the first `get` alone drives the counter
/// to 2, which the exact-value assertions below reject.
///
/// The latch itself is asserted through the private `AtomicBool` rather
/// than by capturing log output: no crate in the workspace carries a
/// `tracing` capture layer in dev-deps, and that is a weak proxy — it
/// cannot catch a mutation that logs unconditionally *and* sets the flag.
/// Recorded rather than papered over; the counter assertions are the ones
/// carrying real weight here.
#[tokio::test]
async fn separate_poisonings_each_count_while_the_log_latches() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"latch check";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);

    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![Arc::new(origin) as Arc<dyn Origin>],
        10,
        crate::PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    poison_inflight(&engine);

    anyhow::ensure!(
        !engine.inner.inflight_poison_logged.load(Ordering::Relaxed),
        "the latch must not be set before any lock is taken"
    );

    // One miss takes the lock twice (claim the entry, then release it in
    // `InflightGuard::drop`) but sees the poison only on the first, since
    // that lock clears it.
    let _ = engine.get(hash).await?;
    let after_first = cm.inflight_mutex_poisoned.get();
    anyhow::ensure!(
        after_first == 1,
        "one poisoning, one bump — the poison must have been cleared; got {after_first}"
    );
    anyhow::ensure!(
        engine.inner.inflight_poison_logged.load(Ordering::Relaxed),
        "the first poisoned lock must latch the log"
    );

    // A second, independent panic. A second `get` would add nothing on its
    // own — it is a cache hit and returns above the coalescing loop without
    // locking at all — so poison directly and take one more lock.
    poison_inflight(&engine);
    drop(engine.inner.lock_inflight());
    let after_second = cm.inflight_mutex_poisoned.get();

    anyhow::ensure!(
        after_second == 2,
        "the counter must count the second poisoning too, got {after_second}"
    );
    anyhow::ensure!(
        engine.inner.inflight_poison_logged.load(Ordering::Relaxed),
        "the latch must stay set — the second poisoning must not re-log"
    );
    Ok(())
}

/// `populate_inner`'s coalescing loop is a near-verbatim *copy* of `get`'s,
/// not a shared helper, and the #1517 poison fall-through is per-copy —
/// each copy can carry its own. So covering `get` does not cover this, and a
/// mutation that reverts only `populate_inner` survives a `get`-only suite.
///
/// This is also the copy that matters most: `populate` (unlike
/// `populate_local`) walks the `Peer` origin, so a lost claim here is the
/// duplicate *paid* upstream pull — the USDC double-spend #1517 names.
/// `get`, by contrast, has no production caller in the daemon; the serve
/// path fills via `populate`/`populate_local`.
#[tokio::test]
async fn poisoned_populate_still_coalesces_into_one_fill() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"populate under poison";
    let hash = Hash::new(payload);
    let origin = Arc::new(SlowCountingOrigin::new(
        payload,
        std::time::Duration::from_millis(50),
    ));

    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![origin.clone() as Arc<dyn Origin>],
        10,
        crate::PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    poison_inflight(&engine);

    let mut handles = Vec::new();
    for _ in 0..5 {
        let e = engine.clone();
        handles.push(tokio::spawn(async move { e.populate(hash).await }));
    }
    for handle in handles {
        let result = handle
            .await
            .map_err(|e| anyhow::anyhow!("task join: {e}"))?;
        anyhow::ensure!(result.is_ok(), "expected Ok, got {result:?}");
    }

    let count = origin.fetch_count.load(Ordering::SeqCst);
    anyhow::ensure!(
        count == 1,
        "populate must coalesce under poison — expected 1 origin fetch, got {count}"
    );
    let bumps = cm.inflight_mutex_poisoned.get();
    anyhow::ensure!(bumps == 1, "one poisoning, one bump; got {bumps}");
    anyhow::ensure!(inflight_len(&engine) == 0, "the claim must be released");
    Ok(())
}

/// The drop-path half of #1517. If `InflightGuard::drop` swallowed the
/// `PoisonError` and skipped the removal, then — since `notify_waiters`
/// only wakes *current* waiters — the leaked entry would make every later
/// request for that hash park on a `Notify` that never fires again — a
/// permanent hang, which is the exact failure the guard exists to prevent.
///
/// The unpoisoned cancellation case is covered by
/// [`cancelled_owner_does_not_orphan_inflight_entry`]; this is its poisoned
/// twin, and it reads the map with `into_inner` so poison cannot fake a
/// pass.
#[tokio::test]
async fn poisoned_mutex_does_not_orphan_inflight_entry() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"poisoned cancel";
    let hash = Hash::new(payload);
    let origin_handle = Arc::new(SlowCountingOrigin::new(
        payload,
        std::time::Duration::from_secs(10),
    ));

    let engine = CacheEngine::open(
        tmp.path(),
        vec![origin_handle.clone() as Arc<dyn Origin>],
        10,
    )
    .await?;

    poison_inflight(&engine);

    let owner_engine = engine.clone();
    let owner = tokio::spawn(async move {
        tokio::time::timeout(std::time::Duration::from_millis(50), owner_engine.get(hash)).await
    });
    let _ = owner.await?;

    // Prove the owner actually claimed the entry before asserting it was
    // released. Without this the test passes vacuously whenever the 50 ms
    // timeout lands before the pull starts — `get` does `refuses()` and
    // `has()` (fs store I/O) first — and an empty map then proves nothing,
    // even under a leaking drop impl (#1517). A loaded CI box makes that a
    // silent false pass rather than a visible flake.
    let claimed = origin_handle.fetch_count.load(Ordering::SeqCst);
    anyhow::ensure!(
        claimed == 1,
        "owner must have reached the pull (and so held the entry), got {claimed} fetches"
    );

    // An empty map is the assertion proper, and it reads through
    // `into_inner` so poison cannot fake it. Under a leaking drop impl (#1517)
    // the entry survives here and `len == 1`. There is deliberately no
    // "now issue another get and time it" probe: this origin sleeps for ten
    // seconds by design, so any such timeout would measure the stub rather
    // than the orphaned `Notify`.
    let len = inflight_len(&engine);
    anyhow::ensure!(
        len == 0,
        "InflightGuard::drop must clear the entry under poison, had {len}"
    );
    Ok(())
}

// ----- Pinning (#276) -----

#[tokio::test]
async fn pinned_hash_is_excluded_from_eviction_candidates() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let pinned_payload = b"pinned blob";
    let evictable_payload = b"evictable blob";
    let pinned_hash = Hash::new(pinned_payload);
    let evictable_hash = Hash::new(evictable_payload);

    // Build the engine with pinned_hash in the pinning set. The
    // leaf `PinnedHashes` is keyed on the config-vocabulary hash, so
    // convert the store hash at the boundary (#578) — this also
    // regression-covers the leaf↔store conversion in `open_full`.
    let pinned_set = [from_store_hash(pinned_hash)].into_iter().collect();
    let engine =
        CacheEngine::open_with_pinned(tmp.path(), Vec::new(), 10, PinnedHashes::new(pinned_set))
            .await?;

    // Touch both hashes via direct access-time insertion (we don't
    // need actual blob content for this test).
    engine
        .inner
        .access_times
        .insert(pinned_hash, Instant::now());
    engine
        .inner
        .access_times
        .insert(evictable_hash, Instant::now());

    let raw = engine.access_times_snapshot();
    anyhow::ensure!(raw.len() == 2, "raw snapshot must include pinned");

    let candidates = engine.eviction_candidates();
    anyhow::ensure!(
        candidates.len() == 1,
        "candidates should exclude pinned, got {} entries",
        candidates.len()
    );
    anyhow::ensure!(
        candidates.contains_key(&evictable_hash),
        "evictable hash should be a candidate"
    );
    anyhow::ensure!(
        !candidates.contains_key(&pinned_hash),
        "pinned hash must NOT be a candidate"
    );
    anyhow::ensure!(engine.is_pinned(pinned_hash));
    anyhow::ensure!(!engine.is_pinned(evictable_hash));
    Ok(())
}

/// A pinned hash that is ALSO governance-denied stays an eviction candidate —
/// "deny wins over pin" on the LRU path, not only the takedown `evict()`. A
/// pinned clean hash is still excluded. The pin flag itself is unchanged.
#[tokio::test]
async fn deny_listed_pinned_hash_is_an_eviction_candidate() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let clean_hash = Hash::new(b"pinned clean blob");
    let denied_hash = Hash::new(b"pinned denied blob");

    let pinned_set = [from_store_hash(clean_hash), from_store_hash(denied_hash)]
        .into_iter()
        .collect();
    let engine =
        CacheEngine::open_with_pinned(tmp.path(), Vec::new(), 10, PinnedHashes::new(pinned_set))
            .await?;
    engine.inner.access_times.insert(clean_hash, Instant::now());
    engine
        .inner
        .access_times
        .insert(denied_hash, Instant::now());

    anyhow::ensure!(
        engine.set_chain_denied_one(denied_hash, true),
        "deny must change the set"
    );

    let candidates = engine.eviction_candidates();
    anyhow::ensure!(
        candidates.contains_key(&denied_hash),
        "a pinned + governance-denied hash must remain an eviction candidate"
    );
    anyhow::ensure!(
        !candidates.contains_key(&clean_hash),
        "a pinned clean hash must still be excluded"
    );
    anyhow::ensure!(
        engine.is_pinned(denied_hash) && engine.is_pinned(clean_hash),
        "the pin flag itself is unchanged — only the eviction carve-out differs"
    );
    Ok(())
}

/// The carve-out fires for the LOCAL (`content.denied_hashes` → `set_denied`)
/// deny half too, not only the on-chain half — the `is_denied` side of the
/// `is_denied || is_chain_denied` disjunction in `eviction_candidates`.
#[tokio::test]
async fn local_denied_pinned_hash_is_also_an_eviction_candidate() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let hash = Hash::new(b"pinned + locally denied");
    let engine = CacheEngine::open_with_pinned(
        tmp.path(),
        Vec::new(),
        10,
        PinnedHashes::new([from_store_hash(hash)].into_iter().collect()),
    )
    .await?;
    engine.inner.access_times.insert(hash, Instant::now());
    engine.set_denied(&denied(&[hash]));
    anyhow::ensure!(
        engine.eviction_candidates().contains_key(&hash),
        "a pinned + locally-denied hash must also be an eviction candidate"
    );
    Ok(())
}

#[tokio::test]
async fn set_pinned_atomically_updates_filter() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let h1 = Hash::new(b"one");
    let h2 = Hash::new(b"two");

    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    engine.inner.access_times.insert(h1, Instant::now());
    engine.inner.access_times.insert(h2, Instant::now());

    // No pinning yet — both candidates.
    anyhow::ensure!(engine.eviction_candidates().len() == 2);

    // Pin h1 (convert the store hash to the leaf config-vocabulary
    // hash at the boundary, #578).
    let s = [from_store_hash(h1)].into_iter().collect();
    let diff = engine.set_pinned(&PinnedHashes::new(s));
    anyhow::ensure!(
        diff.added == 1 && diff.removed == 0,
        "expected diff (added=1, removed=0), got {diff:?}"
    );

    let candidates = engine.eviction_candidates();
    anyhow::ensure!(candidates.len() == 1, "h1 should now be excluded");
    anyhow::ensure!(candidates.contains_key(&h2));

    // Replace with empty set — h1 becomes a candidate again.
    let diff2 = engine.set_pinned(&PinnedHashes::empty());
    anyhow::ensure!(
        diff2.added == 0 && diff2.removed == 1,
        "expected diff (added=0, removed=1), got {diff2:?}"
    );
    anyhow::ensure!(engine.eviction_candidates().len() == 2);
    Ok(())
}

// ----- Operator-evict (#279) -----

/// `evict` must take a previously-cached hash off the served set. After
/// evict, `has` reports false and `get` returns `NotFound` rather than
/// silently re-pulling from the origin (which would defeat the point of
/// the takedown use case behind issue #279).
#[tokio::test]
async fn evict_blocks_subsequent_serve() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"evict me";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;

    // Prime the cache so we evict a real, served blob — covers the
    // hot path the operator would actually be evicting.
    let _ = engine.get(hash).await?;
    anyhow::ensure!(
        engine.has(hash).await?,
        "expected blob present before evict"
    );

    engine.evict(hash).await?;

    anyhow::ensure!(engine.is_evicted(hash), "evict flag not set");
    anyhow::ensure!(
        !engine.has(hash).await?,
        "has() should report absent after evict"
    );
    match engine.get(hash).await {
        Err(CacheError::NotFound { .. }) => Ok(()),
        other => Err(anyhow::anyhow!(
            "expected NotFound after evict, got {other:?}"
        )),
    }
}

/// Eviction must survive a process restart — operators running DMCA
/// takedowns rely on the evict being durable. The on-disk
/// `evicted.log` is replayed by a fresh `open()`. Without this test,
/// a regression that dropped the persistence path (e.g. moved to
/// in-memory-only) would let evicted content silently resume serving
/// after a restart.
#[tokio::test]
async fn evict_persists_across_open() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"persist me";
    let hash = Hash::new(payload);

    // First open: prime + evict.
    {
        let origin = Arc::new(StubOrigin::new(payload));
        let engine = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 10).await?;
        let _ = engine.get(hash).await?;
        engine.evict(hash).await?;
        engine.shutdown().await?;
    }

    // Second open: same cache_dir, no origin so a re-pull would fail
    // loudly. The evicted set must reload from disk.
    let engine2 = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    anyhow::ensure!(
        engine2.is_evicted(hash),
        "evict flag should reload from <cache_dir>/evicted.log"
    );
    anyhow::ensure!(
        !engine2.has(hash).await?,
        "has() should report absent after restart"
    );
    Ok(())
}

/// Commit every payload into a cache at `dir`, then shut the engine down so
/// the next `open` sees them as blobs already on disk.
async fn commit_then_close(dir: &Path, payloads: &[&[u8]]) -> anyhow::Result<()> {
    let origin = Arc::new(MultiStubOrigin::new(payloads)) as Arc<dyn Origin>;
    let engine = CacheEngine::open(dir, vec![origin], 10).await?;
    for payload in payloads {
        let _ = engine.get(Hash::new(payload)).await?;
    }
    engine.shutdown().await?;
    Ok(())
}

/// A blob on disk before a restart is an eviction candidate right after
/// `open`, with no traffic. Otherwise a node restarted above its size limit
/// can release only the working set it serves after the restart.
#[tokio::test]
async fn a_blob_on_disk_before_open_is_an_eviction_candidate() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload: &[u8] = b"cold before restart";
    let hash = Hash::new(payload);
    commit_then_close(tmp.path(), &[payload]).await?;

    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    anyhow::ensure!(
        engine.eviction_candidates().contains_key(&hash),
        "a pre-open blob must be an eviction candidate without a touch"
    );
    // The seeded hash's protecting tag survives the reopen, so the driver
    // can actually release it.
    anyhow::ensure!(
        engine.release_for_eviction(hash).await? > 0,
        "a seeded pre-open blob must be releasable"
    );
    anyhow::ensure!(
        !engine.eviction_candidates().contains_key(&hash),
        "a released blob must leave the candidate set"
    );
    Ok(())
}

/// An operator eviction survives a restart: the open-time seed walks only
/// non-refused hashes, so an evicted blob whose bytes are still on disk does
/// not come back as an eviction candidate.
#[tokio::test]
async fn an_evicted_pre_open_blob_is_not_seeded() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let evicted_payload: &[u8] = b"evicted before restart";
    let kept_payload: &[u8] = b"kept across restart";
    let evicted = Hash::new(evicted_payload);
    let kept = Hash::new(kept_payload);
    {
        let origin =
            Arc::new(MultiStubOrigin::new(&[evicted_payload, kept_payload])) as Arc<dyn Origin>;
        let engine = CacheEngine::open(tmp.path(), vec![origin], 10).await?;
        let _ = engine.get(evicted).await?;
        let _ = engine.get(kept).await?;
        engine.evict(evicted).await?;
        engine.shutdown().await?;
    }

    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    // `eviction_candidates` never consults `is_evicted`, so only the raw map
    // can show an evicted hash re-entering through the seed.
    anyhow::ensure!(
        !engine.access_times_snapshot().contains_key(&evicted),
        "an evicted hash must not enter access_times through the seed"
    );
    let candidates = engine.eviction_candidates();
    anyhow::ensure!(
        !candidates.contains_key(&evicted),
        "an evicted hash must not be a candidate"
    );
    anyhow::ensure!(
        candidates.contains_key(&kept),
        "a kept hash must still be seeded"
    );
    Ok(())
}

/// The open-time seed ranks every pre-open blob behind anything accessed
/// after open, so the LRU driver releases cold content first.
#[tokio::test]
async fn a_touched_blob_ranks_after_an_untouched_pre_open_blob() -> anyhow::Result<()> {
    use crate::policy::{EvictionContext, EvictionPolicy, LruEviction};

    let tmp = tempfile::tempdir()?;
    let cold_payload: &[u8] = b"untouched after restart";
    let hot_payload: &[u8] = b"touched after restart";
    let cold = Hash::new(cold_payload);
    let hot = Hash::new(hot_payload);
    commit_then_close(tmp.path(), &[cold_payload, hot_payload]).await?;

    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    engine.observe_hit(hot);

    let candidates = engine.eviction_candidates();
    let cold_at = candidates
        .iter()
        .find_map(|(h, t)| (*h == cold).then_some(*t))
        .ok_or_else(|| anyhow::anyhow!("cold blob missing from candidates"))?;
    let hot_at = candidates
        .iter()
        .find_map(|(h, t)| (*h == hot).then_some(*t))
        .ok_or_else(|| anyhow::anyhow!("hot blob missing from candidates"))?;
    anyhow::ensure!(
        cold_at < hot_at,
        "the untouched pre-open blob must rank older than the touched one"
    );

    let sizes = HashMap::new();
    let segments = HashMap::new();
    let plan = LruEviction.plan(&EvictionContext {
        candidates: &candidates,
        sizes: &sizes,
        segments: &segments,
        total_bytes: 1,
        target_bytes: 0,
        budget: 1,
        cache_bytes: 0,
    });
    anyhow::ensure!(
        plan.evict == vec![cold],
        "LRU must release the untouched pre-open blob first, got {:?}",
        plan.evict
    );
    Ok(())
}

/// The open-time seed is not an access: the operator preview reports no
/// last access for a pre-open blob until something touches it.
#[tokio::test]
async fn a_seeded_blob_reports_no_last_access() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload: &[u8] = b"seeded, not accessed";
    let hash = Hash::new(payload);
    commit_then_close(tmp.path(), &[payload]).await?;

    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    anyhow::ensure!(
        engine.last_accessed(hash).is_none(),
        "a seeded entry must not read as an access"
    );
    anyhow::ensure!(
        engine.inspect(hash).await?.last_accessed_us_ago.is_none(),
        "the preview must not report the seed as an access"
    );

    engine.observe_hit(hash);
    anyhow::ensure!(
        engine.last_accessed(hash).is_some(),
        "a touch after open must read as an access"
    );
    anyhow::ensure!(
        engine.inspect(hash).await?.last_accessed_us_ago.is_some(),
        "the preview must report a touch after open"
    );
    Ok(())
}

/// Seeding does not bypass pinning: a pinned blob from before the restart
/// stays out of the candidate set.
#[tokio::test]
async fn a_pinned_pre_open_blob_is_not_a_candidate() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let pinned_payload: &[u8] = b"pinned across restart";
    let free_payload: &[u8] = b"unpinned across restart";
    let pinned = Hash::new(pinned_payload);
    let free = Hash::new(free_payload);
    commit_then_close(tmp.path(), &[pinned_payload, free_payload]).await?;

    let engine = CacheEngine::open_with_pinned(
        tmp.path(),
        Vec::new(),
        10,
        crate::PinnedHashes::new([from_store_hash(pinned)].into_iter().collect()),
    )
    .await?;
    let candidates = engine.eviction_candidates();
    anyhow::ensure!(
        !candidates.contains_key(&pinned),
        "a pinned pre-open blob must not be an eviction candidate"
    );
    anyhow::ensure!(
        candidates.contains_key(&free),
        "an unpinned pre-open blob must still be an eviction candidate"
    );
    Ok(())
}

/// Under `TinyLfuEviction`, a blob touched after open ranks after an untouched
/// pre-open blob of equal frequency: the seed is older than any post-open
/// access.
#[tokio::test]
async fn tinylfu_ranks_a_touched_blob_after_an_untouched_pre_open_blob() -> anyhow::Result<()> {
    use crate::policy::{EvictionContext, EvictionPolicy, TinyLfuEstimator, TinyLfuEviction};

    let tmp = tempfile::tempdir()?;
    let cold_payload: &[u8] = b"tinylfu: untouched after restart";
    let hot_payload: &[u8] = b"tinylfu: touched after restart";
    let cold = Hash::new(cold_payload);
    let hot = Hash::new(hot_payload);
    commit_then_close(tmp.path(), &[cold_payload, hot_payload]).await?;

    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    let freq: Arc<dyn crate::policy::FrequencyEstimator> = Arc::new(TinyLfuEstimator::new(4096));
    engine.set_frequency_estimator(Arc::clone(&freq));
    // Recency only, so both blobs stay at frequency 0 and age alone decides.
    engine.touch_recency(hot);

    let candidates = engine.eviction_candidates();
    let sizes = HashMap::new();
    let segments = HashMap::new();
    let plan = TinyLfuEviction::new(freq, 2, 10).plan(&EvictionContext {
        candidates: &candidates,
        sizes: &sizes,
        segments: &segments,
        total_bytes: 1,
        target_bytes: 0,
        budget: 1,
        cache_bytes: 0,
    });
    anyhow::ensure!(
        plan.evict == vec![cold],
        "TinyLFU must release the untouched pre-open blob first, got {:?}",
        plan.evict
    );
    Ok(())
}

/// A partial blob on disk at open is seeded like a complete one: it is an
/// eviction candidate with no traffic, and its protecting tag survives the
/// reopen so the driver can release it.
#[tokio::test]
async fn a_partial_blob_on_disk_before_open_is_an_eviction_candidate() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);
    {
        let engine = CacheEngine::open(tmp.path(), Vec::new(), 16).await?;
        engine.admit_bao(hash, ranges, bao).await?;
        anyhow::ensure!(
            !engine.present_ranges(hash).await?.is_complete(),
            "the admitted blob must be partial"
        );
        engine.shutdown().await?;
    }

    let engine = CacheEngine::open(tmp.path(), Vec::new(), 16).await?;
    anyhow::ensure!(
        engine.eviction_candidates().contains_key(&hash),
        "a pre-open partial must be an eviction candidate without a touch"
    );
    anyhow::ensure!(
        engine.last_accessed(hash).is_none(),
        "a seeded partial must not read as accessed"
    );
    anyhow::ensure!(
        engine.release_for_eviction(hash).await? > 0,
        "a seeded pre-open partial must be releasable"
    );
    Ok(())
}

/// Malformed lines in `evicted.log` (manual edit gone wrong, partial
/// write from an old crash) must not stop the engine from opening;
/// they get logged + skipped, and the well-formed lines still load.
#[tokio::test]
async fn evicted_log_skips_malformed_lines() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let log_path = tmp.path().join("evicted.log");
    let good = Hash::new(b"good entry");
    std::fs::write(
        &log_path,
        format!("\n# operator note\n{good}\nnot-a-hash\n{good}\n"),
    )?;

    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    anyhow::ensure!(engine.is_evicted(good), "valid hash line not loaded");
    Ok(())
}

#[tokio::test]
async fn evict_unknown_hash_is_a_no_op() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    let unknown = Hash::new(b"never seen");
    // Evicting a hash we've never cached is fine — operators may run
    // `decdn node evict` ahead of time as a precaution.
    engine.evict(unknown).await?;
    anyhow::ensure!(engine.is_evicted(unknown), "evict flag not set");
    Ok(())
}

/// `serve_audit` folds presence + size + eviction into one store contact
/// (#1789 item 7 part B) and matches the `has`/`inspect` pairing the
/// delivery path would otherwise make: a complete blob is `Serveable` with
/// its size, an absent hash is `Unavailable`, and an evicted hash is
/// `Unavailable` with `withdrawn` set so the serve path tells an eviction
/// from a plain miss without a second call.
#[tokio::test]
async fn serve_audit_reports_presence_size_and_eviction() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"a served blob";
    let origin = StubOrigin::new(payload);
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
    let hash = Hash::new(payload);
    let unseen = Hash::new(b"never cached");

    anyhow::ensure!(
        engine.serve_audit(unseen).await? == ServeAudit::Unavailable { withdrawn: false },
        "an absent hash is unavailable and not evicted"
    );

    engine.populate(hash).await?;
    anyhow::ensure!(
        engine.serve_audit(hash).await?
            == ServeAudit::Serveable {
                size: payload.len() as u64
            },
        "a complete blob is serveable and carries its size"
    );

    engine.evict(hash).await?;
    anyhow::ensure!(
        engine.serve_audit(hash).await? == ServeAudit::Unavailable { withdrawn: true },
        "evicted content is unavailable with the eviction surfaced"
    );
    Ok(())
}

/// A complete blob that a gate refuses is `Unavailable` and carries NO
/// size, so a caller cannot advertise the wire size of content the serve
/// path would refuse. `evicted` stays false: a chain-denied hash is a
/// different refusal from an eviction and the miss path must not report it
/// as `EvictedSinceProbe`.
#[tokio::test]
async fn serve_audit_withholds_the_size_of_a_denied_blob() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"denied but on disk";
    let origin = StubOrigin::new(payload);
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
    let hash = Hash::new(payload);
    engine.populate(hash).await?;

    engine.set_chain_denied_one(hash, true);
    let audit = engine.serve_audit(hash).await?;
    anyhow::ensure!(
        audit == ServeAudit::Unavailable { withdrawn: false },
        "a chain-denied blob is unavailable, un-evicted, and sizeless"
    );
    anyhow::ensure!(
        audit.hit_size().is_none(),
        "a refused blob never yields a wire size"
    );
    Ok(())
}

/// A genuinely empty blob is `Serveable { size: 0 }`. The delivery path
/// keys its "fill then size it yourself" branch on the absence of a size,
/// so a zero-length blob must not read as a miss.
#[tokio::test]
async fn serve_audit_reports_a_zero_length_blob_as_serveable() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = StubOrigin::new(b"");
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
    let hash = Hash::new(b"");
    engine.populate(hash).await?;

    let audit = engine.serve_audit(hash).await?;
    anyhow::ensure!(
        audit == ServeAudit::Serveable { size: 0 },
        "an empty blob is serveable at size 0"
    );
    anyhow::ensure!(
        audit.hit_size() == Some(0),
        "the delivery path reads 0, not a missing size"
    );
    Ok(())
}

/// Evicting the same hash twice must not append a duplicate line to
/// `<cache_dir>/evicted.log`. Without this contract a stuck
/// automation that mass-replays the same DMCA-takedown hash would
/// grow the log unboundedly. The first evict appends one line, the
/// second short-circuits via the `contains(&hash)` check at the top
/// of `evict()`.
#[tokio::test]
async fn evict_is_idempotent_and_does_not_grow_log() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let log_path = tmp.path().join("evicted.log");
    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    let hash = Hash::new(b"dup-evict");

    engine.evict(hash).await?;
    let after_first = std::fs::read_to_string(&log_path)?;
    let lines_first = after_first.lines().count();

    engine.evict(hash).await?;
    engine.evict(hash).await?;
    let after_third = std::fs::read_to_string(&log_path)?;
    let lines_third = after_third.lines().count();

    anyhow::ensure!(
        lines_first == 1 && lines_third == 1,
        "expected 1 log line both times, got first={lines_first}, third={lines_third}"
    );
    Ok(())
}

/// The lock-free `evicted` set (#1789 item 5) must never lose a write and
/// never present a torn view.
///
/// CONCURRENT WRITERS are the point: several tasks evict disjoint hashes at
/// once, and every one of them must be evicted at the end. A publish that
/// read a snapshot, cloned it and stored it without serializing — the
/// obvious "one less allocation" rewrite of
/// [`MonotoneHashSet::insert_if_absent`] — drops whichever writer lost the
/// race, and `evict` would have returned `Ok(())` while the hash kept
/// serving. That is a takedown failure, so it is asserted directly.
///
/// Readers run alongside on real worker threads and assert monotonicity: a
/// hash observed evicted stays evicted. The reader half fails safe (it can
/// only under-observe under scheduling pressure), so it also asserts it saw
/// something, otherwise a starved reader would assert nothing at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evicted_set_stays_consistent_under_concurrent_evict() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = Arc::new(empty_engine(tmp.path()).await?);
    let hashes: Vec<Hash> = (0..64)
        .map(|i| Hash::new(format!("concurrent-evict-{i}").as_bytes()))
        .collect();
    let stop = Arc::new(AtomicBool::new(false));

    // Eight writers over disjoint slices of the hash set, all evicting at
    // once. A lost update leaves one of the slices un-evicted.
    let mut writers = Vec::new();
    for chunk in hashes.chunks(8) {
        let engine = Arc::clone(&engine);
        let chunk: Vec<Hash> = chunk.to_vec();
        writers.push(tokio::spawn(async move {
            for h in &chunk {
                engine.evict(*h).await?;
            }
            Ok::<_, anyhow::Error>(())
        }));
    }

    let mut readers = Vec::new();
    for _ in 0..8 {
        let engine = Arc::clone(&engine);
        let hashes = hashes.clone();
        let stop = Arc::clone(&stop);
        readers.push(tokio::spawn(async move {
            let mut saw_evicted = HashSet::new();
            for _ in 0..50_000 {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                for h in &hashes {
                    if engine.is_evicted(*h) {
                        saw_evicted.insert(*h);
                    }
                }
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(saw_evicted)
        }));
    }

    for w in writers {
        w.await
            .map_err(|e| anyhow::anyhow!("writer task panicked: {e}"))??;
    }
    stop.store(true, Ordering::Relaxed);

    for h in &hashes {
        anyhow::ensure!(
            engine.is_evicted(*h),
            "{h} was evicted by a concurrent writer but is not in the set — lost update"
        );
    }

    let mut any_observed = false;
    for r in readers {
        let saw = r
            .await
            .map_err(|e| anyhow::anyhow!("reader task panicked: {e}"))??;
        any_observed |= !saw.is_empty();
        for h in &saw {
            anyhow::ensure!(
                engine.is_evicted(*h),
                "reader once saw {h} evicted but it is not evicted at the end — torn view"
            );
        }
    }
    anyhow::ensure!(
        any_observed,
        "no reader observed any eviction — the monotonicity half asserted nothing"
    );
    Ok(())
}

/// Hand-edited uppercase hex in `evicted.log` must be tolerated by
/// `parse_hex_hash` — the persisted format is canonically lowercase
/// (`Hash::Display` calls `to_hex()`), but operators pasting from
/// access logs / takedown notices may use either case. Without this,
/// a mixed-case hand-edit would silently get dropped at next open
/// and the takedown would resume serving content.
#[tokio::test]
async fn evicted_log_accepts_uppercase_hex() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let log_path = tmp.path().join("evicted.log");
    let hash = Hash::new(b"upper-hex");
    let upper = hash.to_string().to_uppercase();
    std::fs::write(&log_path, format!("{upper}\n"))?;

    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    anyhow::ensure!(
        engine.is_evicted(hash),
        "uppercase hex line should load as the same hash",
    );
    Ok(())
}

/// `evict()` must surface a persistence failure as `Err` rather than
/// silently degrading to in-memory-only — for DMCA-driven evicts the
/// operator must be able to tell whether the takedown is durable.
/// Forcing the failure: place a *directory* at the `evicted.log`
/// path so `OpenOptions::open(...)` fails (`EISDIR`) when
/// `append_evicted_log` runs. Avoids relying on filesystem
/// permission games that may not work uniformly across CI hosts.
#[tokio::test]
async fn evict_returns_err_when_persistence_fails() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    // Engine opened cleanly with no log file yet. Now plant a
    // directory at the path the engine will try to append to.
    std::fs::create_dir(tmp.path().join("evicted.log"))?;

    let hash = Hash::new(b"persist-fail");
    match engine.evict(hash).await {
        Err(CacheError::Store(_)) => Ok(()),
        other => Err(anyhow::anyhow!(
            "expected Store error from persistence failure, got {other:?}"
        )),
    }?;

    // And the in-memory set must NOT have been updated — otherwise
    // an operator seeing the error would (correctly) assume the
    // takedown didn't land, but the running node would actually have
    // already stopped serving. Either contract is reasonable on its
    // own; mixing them is the worst case.
    anyhow::ensure!(
        !engine.is_evicted(hash),
        "in-memory set must not commit when persistence fails",
    );
    Ok(())
}

// ----- inspect / dry-run preview (#379) -----

/// `inspect` on a freshly-opened cache must report all the
/// "absent" sentinels: no size, no last access, not pinned, not
/// evicted, not served. Locks the wire-shape contract so a
/// regression that defaulted `served` to `true` (or that swallowed
/// the iroh-blobs `NotFound` arm) is caught on every CI run.
#[tokio::test]
async fn inspect_unknown_hash_reports_absent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    let unknown = Hash::new(b"never seen");

    let preview = engine.inspect(unknown).await?;
    anyhow::ensure!(preview.size_bytes.is_none(), "expected size_bytes=None");
    anyhow::ensure!(
        preview.last_accessed_us_ago.is_none(),
        "expected last_accessed_us_ago=None"
    );
    anyhow::ensure!(!preview.pinned, "expected pinned=false");
    anyhow::ensure!(!preview.already_evicted, "expected already_evicted=false");
    anyhow::ensure!(!preview.served, "expected served=false");
    Ok(())
}

/// After a successful pull-through `get`, `inspect` reports the
/// concrete blob size, a finite `last_accessed_us_ago`, and
/// `served=true`. Asserts the size matches the payload exactly so
/// a regression that returned the partial-blob size (or a wrong
/// match arm in the `BlobStatus` decode) fails loudly.
#[tokio::test]
async fn inspect_after_get_reports_size_and_served() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"inspect me";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
    let _ = engine.get(hash).await?;

    let preview = engine.inspect(hash).await?;
    anyhow::ensure!(
        preview.size_bytes == Some(payload.len() as u64),
        "expected size_bytes={:?}, got {:?}",
        Some(payload.len() as u64),
        preview.size_bytes,
    );
    anyhow::ensure!(
        preview.last_accessed_us_ago.is_some(),
        "expected Some(last_accessed_us_ago) after get()"
    );
    anyhow::ensure!(preview.served, "expected served=true");
    anyhow::ensure!(!preview.already_evicted, "expected already_evicted=false");
    anyhow::ensure!(!preview.pinned, "expected pinned=false");
    Ok(())
}

/// `inspect` after `evict` must still report the on-disk
/// `size_bytes` (until iroh-blobs' periodic GC sweep reclaims, #518)
/// but flip `already_evicted` to `true` and `served` to `false`.
/// The size-still-reported part
/// is the load-bearing assertion: dry-run callers want to see
/// disk-reclaim potential, not a clean `None` that hides the bytes.
#[tokio::test]
async fn inspect_after_evict_keeps_size_but_flips_served() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"evicted blob";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);

    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
    let _ = engine.get(hash).await?;
    engine.evict(hash).await?;

    let preview = engine.inspect(hash).await?;
    anyhow::ensure!(
        preview.size_bytes == Some(payload.len() as u64),
        "expected size_bytes still reported post-evict, got {:?}",
        preview.size_bytes,
    );
    anyhow::ensure!(preview.already_evicted, "expected already_evicted=true");
    anyhow::ensure!(!preview.served, "expected served=false post-evict");
    // Eviction clears the access-time entry, so this should now be None.
    anyhow::ensure!(
        preview.last_accessed_us_ago.is_none(),
        "expected last_accessed cleared by evict, got {:?}",
        preview.last_accessed_us_ago,
    );
    Ok(())
}

/// `inspect` reflects the operator-pinned set without needing a
/// blob to be cached. The pinned flag must be observable for
/// hashes the operator hasn't fetched yet — that's the whole
/// point of pre-flight dry-run: confirm policy state before
/// committing to evict.
#[tokio::test]
async fn inspect_reports_pinned_flag() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let pinned_hash = Hash::new(b"pinned");
    let other_hash = Hash::new(b"other");
    // Convert the store hash to the leaf config-vocabulary hash at
    // the `PinnedHashes` boundary (#578).
    let set = [from_store_hash(pinned_hash)].into_iter().collect();
    let engine =
        CacheEngine::open_with_pinned(tmp.path(), Vec::new(), 10, PinnedHashes::new(set)).await?;

    let pinned_preview = engine.inspect(pinned_hash).await?;
    anyhow::ensure!(pinned_preview.pinned, "expected pinned=true");
    anyhow::ensure!(
        !pinned_preview.served,
        "pinned-but-uncached blob should not be served"
    );

    let other_preview = engine.inspect(other_hash).await?;
    anyhow::ensure!(!other_preview.pinned, "unrelated hash must not be pinned");
    Ok(())
}

/// `inspect` (#439) reports the configured origin's backend kind so
/// admin dry-run callers can estimate origin egress cost before
/// committing to an eviction. Engine constructed with no origin
/// reports an empty vec.
#[tokio::test]
async fn inspect_reports_origin_kinds_when_origin_configured() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let origin = StubOrigin::new(b"egress-cost preview");
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 10).await?;
    let preview = engine.inspect(Hash::new(b"never-fetched")).await?;
    anyhow::ensure!(
        preview.origin_kinds == vec![OriginKind::Http],
        "expected [Http], got {:?}",
        preview.origin_kinds,
    );
    Ok(())
}

#[tokio::test]
async fn inspect_reports_no_origin_kinds_when_cache_only() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Vec::new(), 10).await?;
    let preview = engine.inspect(Hash::new(b"absent")).await?;
    anyhow::ensure!(
        preview.origin_kinds.is_empty(),
        "expected empty Vec for cache-only mode, got {:?}",
        preview.origin_kinds,
    );
    Ok(())
}

// -------------------------------------------------------------------
// Cache hit/miss + bytes counters (#418)
// -------------------------------------------------------------------

#[tokio::test]
async fn hits_plus_misses_equals_total_gets() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"hello invariant";
    let hash = Hash::new(payload);
    let unknown = Hash::new(b"never present");
    let origin = StubOrigin::new(payload);
    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![Arc::new(origin) as Arc<dyn Origin>],
        10,
        crate::PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    // 1 miss (pull-through), 2 hits, 1 miss (origin NotFound), 1 miss (evicted).
    let _ = engine.get(hash).await?; // miss
    let _ = engine.get(hash).await?; // hit
    let _ = engine.get(hash).await?; // hit
    let _ = engine.get(unknown).await; // miss (origin NotFound)
    engine.evict(hash).await?;
    let _ = engine.get(hash).await; // miss (evicted)

    anyhow::ensure!(cm.hits.get() == 2, "hits = {}", cm.hits.get());
    anyhow::ensure!(cm.misses.get() == 3, "misses = {}", cm.misses.get());
    anyhow::ensure!(
        cm.hits.get() + cm.misses.get() == 5,
        "across cache-domain outcomes (no store-I/O errors), every get bumps exactly one of hits/misses"
    );
    // Only the priming Found bumped pull_through_bytes; the unknown
    // get took the origin-NotFound branch which returns before the
    // bump. Pin both, so a stray bump in either error path fails
    // the test.
    anyhow::ensure!(
        cm.pull_through_bytes.get() == payload.len() as u64,
        "pull_through_bytes = {}, expected {}",
        cm.pull_through_bytes.get(),
        payload.len()
    );
    Ok(())
}

#[tokio::test]
async fn pull_through_success_bumps_bytes_returned() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"hello pull-through bytes returned";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);
    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![Arc::new(origin) as Arc<dyn Origin>],
        10,
        crate::PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    let bytes = engine.get(hash).await?;
    anyhow::ensure!(
        cm.bytes_returned.get() == bytes.len() as u64,
        "bytes_returned should match payload length after a successful pull-through"
    );
    Ok(())
}

#[tokio::test]
async fn pull_through_bumps_pull_through_bytes() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"hello pull-through bytes";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);
    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![Arc::new(origin) as Arc<dyn Origin>],
        10,
        crate::PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    let bytes = engine.get(hash).await?;
    anyhow::ensure!(cm.misses.get() == 1, "first get is a miss");
    anyhow::ensure!(cm.hits.get() == 0, "no hits on first get");
    anyhow::ensure!(
        cm.pull_through_bytes.get() == bytes.len() as u64,
        "pull_through_bytes should equal payload length on a Found origin"
    );
    Ok(())
}

#[tokio::test]
async fn populate_fills_without_bumping_bytes_returned() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"populate must not count as bytes returned";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);
    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![Arc::new(origin) as Arc<dyn Origin>],
        10,
        crate::PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    engine.populate(hash).await?;
    anyhow::ensure!(engine.has(hash).await?, "populate must fill the store");
    // Origin egress IS counted (the bytes really left an origin)...
    anyhow::ensure!(
        cm.pull_through_bytes.get() == payload.len() as u64,
        "populate must still bump origin-egress pull_through_bytes"
    );
    // ...but it is NOT a `get` caller, so no served-bytes / hit accounting.
    anyhow::ensure!(
        cm.bytes_returned.get() == 0,
        "populate must NOT bump bytes_returned (#831: internal fill, not client egress)"
    );
    anyhow::ensure!(cm.hits.get() == 0, "populate must not count a hit");

    // A populate on an already-present hash is a no-op (no second pull).
    engine.populate(hash).await?;
    anyhow::ensure!(
        cm.pull_through_bytes.get() == payload.len() as u64,
        "a populate for an already-present blob must not re-pull"
    );
    anyhow::ensure!(cm.bytes_returned.get() == 0, "still no bytes_returned");
    Ok(())
}

#[tokio::test]
async fn no_origin_increments_misses_only() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        Vec::new(),
        10,
        crate::PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    let hash = Hash::new(b"missing payload");
    let Err(err) = engine.get(hash).await else {
        anyhow::bail!("expected NoOrigin, got Ok");
    };
    anyhow::ensure!(
        matches!(err, CacheError::NoOrigin { .. }),
        "expected NoOrigin"
    );
    anyhow::ensure!(cm.misses.get() == 1, "exactly one miss for a NoOrigin get");
    anyhow::ensure!(cm.hits.get() == 0, "no hits");
    anyhow::ensure!(
        cm.pull_through_bytes.get() == 0,
        "no origin bytes since origin not configured"
    );
    anyhow::ensure!(
        cm.bytes_returned.get() == 0,
        "no bytes returned on error path"
    );
    Ok(())
}

#[tokio::test]
async fn evicted_hash_increments_misses() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"hello evicted miss";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);
    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![Arc::new(origin) as Arc<dyn Origin>],
        10,
        crate::PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    // Prime then evict so the next get hits the evicted branch in get().
    let _ = engine.get(hash).await?;
    engine.evict(hash).await?;
    let misses_before = cm.misses.get();

    let Err(err) = engine.get(hash).await else {
        anyhow::bail!("expected NotFound, got Ok");
    };
    anyhow::ensure!(
        matches!(err, CacheError::NotFound { .. }),
        "evicted get must surface NotFound"
    );
    anyhow::ensure!(
        cm.misses.get() == misses_before + 1,
        "misses should bump by exactly 1 on an evicted-hash get"
    );
    Ok(())
}

// ---- Governance deny-set (ADR 011 §StreamRequest Response) ----

async fn empty_engine(tmp: &std::path::Path) -> anyhow::Result<CacheEngine> {
    Ok(CacheEngine::open_with_pinned(tmp, Vec::new(), 10, crate::PinnedHashes::empty()).await?)
}

/// The governance set has to feed `refuses`, or the takedown suppresses
/// nothing before the (separate, slower) eviction lands.
#[tokio::test]
async fn chain_denied_hash_is_refused() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = empty_engine(tmp.path()).await?;
    let hash = Hash::new(b"governance takedown");

    anyhow::ensure!(!engine.refuses(hash), "nothing refused before the deny");
    anyhow::ensure!(engine.set_chain_denied_one(hash, true), "set changed");
    anyhow::ensure!(engine.is_chain_denied(hash));
    anyhow::ensure!(engine.refuses(hash), "a governance deny must refuse");
    anyhow::ensure!(
        !engine.is_denied(hash),
        "and must NOT masquerade as a local denylist entry — the two feed \
         different operator metrics"
    );
    anyhow::ensure!(!engine.has(hash).await?, "refused hashes report absent");
    Ok(())
}

/// A no-op replay must be reported as such: the watcher re-scans a block
/// range after a restart and re-delivers events it already applied, and
/// every one of those would otherwise log as a fresh takedown.
#[tokio::test]
async fn set_chain_denied_one_reports_whether_it_changed_anything() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = empty_engine(tmp.path()).await?;
    let hash = Hash::new(b"replayed");

    anyhow::ensure!(engine.set_chain_denied_one(hash, true));
    anyhow::ensure!(
        !engine.set_chain_denied_one(hash, true),
        "replay is a no-op"
    );
    anyhow::ensure!(engine.set_chain_denied_one(hash, false));
    anyhow::ensure!(!engine.set_chain_denied_one(hash, false));
    anyhow::ensure!(
        !engine.refuses(hash),
        "a de-listed hash stops being refused"
    );
    Ok(())
}

/// The reason the two deny-sets are separate slots: their lifecycles are
/// independent. A config reload must not drop a governance takedown, and the
/// watcher must not drop the operator's own list.
#[tokio::test]
async fn local_and_chain_deny_sets_do_not_clobber_each_other() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = empty_engine(tmp.path()).await?;
    let local = Hash::new(b"local entry");
    let governance = Hash::new(b"governance entry");

    engine.set_denied(&denied(&[local]));
    engine.set_chain_denied_one(governance, true);

    // A reload that drops the local entry leaves the governance one standing.
    engine.set_denied(&crate::DeniedHashes::empty());
    anyhow::ensure!(!engine.refuses(local), "local entry lifted by the reload");
    anyhow::ensure!(
        engine.refuses(governance),
        "a config reload must not lift a governance takedown"
    );

    // ...and a governance removal leaves a re-added local entry standing.
    engine.set_denied(&denied(&[local]));
    engine.set_chain_denied_one(governance, false);
    anyhow::ensure!(engine.refuses(local));
    anyhow::ensure!(!engine.refuses(governance));
    Ok(())
}

/// A hash on BOTH lists must survive removal from one. A single shared set
/// would drop it and silently resume serving content still under a takedown.
#[tokio::test]
async fn hash_on_both_deny_sets_survives_removal_from_one() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = empty_engine(tmp.path()).await?;
    let hash = Hash::new(b"both lists");

    engine.set_denied(&denied(&[hash]));
    engine.set_chain_denied_one(hash, true);
    engine.set_chain_denied_one(hash, false);
    anyhow::ensure!(engine.refuses(hash), "the local entry still stands");
    Ok(())
}

/// The watcher's boot restore replaces wholesale — it is reloading a
/// projection, not merging events into one.
#[tokio::test]
async fn set_chain_denied_replaces_wholesale() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = empty_engine(tmp.path()).await?;
    let stale = Hash::new(b"stale");
    let restored = Hash::new(b"restored");

    engine.set_chain_denied_one(stale, true);
    engine.set_chain_denied([restored].into_iter().collect());
    anyhow::ensure!(!engine.refuses(stale));
    anyhow::ensure!(engine.refuses(restored));
    Ok(())
}

fn denied(hashes: &[Hash]) -> crate::DeniedHashes {
    crate::DeniedHashes::new(hashes.iter().map(|h| from_store_hash(*h)).collect())
}

// ---- Probe-triggered eviction hold (#318, ADR 005) ----

#[tokio::test]
async fn try_probe_hold_unavailable_when_blob_absent() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![], 10).await?;
    let absent = Hash::new(b"never fetched");
    anyhow::ensure!(
        engine.try_probe_hold(absent).await? == ProbeHoldOutcome::Unavailable,
        "absent blob must not be holdable (an absent blob is never advertised)"
    );
    Ok(())
}

#[tokio::test]
async fn try_probe_hold_true_when_cached_and_excluded_from_eviction() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"holdable blob";
    let hash = Hash::new(payload);
    let engine = CacheEngine::open(
        tmp.path(),
        vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
        10,
    )
    .await?;
    let _ = engine.get(hash).await?;

    anyhow::ensure!(
        engine.try_probe_hold(hash).await? == ProbeHoldOutcome::Held,
        "cached blob should hold"
    );
    anyhow::ensure!(engine.probe_hold_slots_used() == 1, "one slot used");
    anyhow::ensure!(
        !engine
            .eviction_candidates()
            .into_inner()
            .contains_key(&hash),
        "held hash must be invisible to the LRU driver"
    );
    Ok(())
}

#[tokio::test]
async fn probe_hold_is_shared_per_blob_not_per_probe() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"popular blob";
    let hash = Hash::new(payload);
    let engine = CacheEngine::open(
        tmp.path(),
        vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
        10,
    )
    .await?;
    let _ = engine.get(hash).await?;
    engine.set_max_probe_holds(1);

    // Many "peers" probing the same hash share one slot.
    for _ in 0..3 {
        anyhow::ensure!(engine.try_probe_hold(hash).await? == ProbeHoldOutcome::Held);
    }
    anyhow::ensure!(
        engine.probe_hold_slots_used() == 1,
        "shared per-blob slot must not grow with probe volume"
    );
    Ok(())
}

#[tokio::test]
async fn probe_hold_budget_exhaustion_reports_exhausted() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let a: &[u8] = b"blob a";
    let b: &[u8] = b"blob bee";
    let (ha, hb) = (Hash::new(a), Hash::new(b));
    // One engine, one multi-blob origin: opening a second engine on the
    // same dir would deadlock on iroh-blobs' single-writer lock.
    let origin = Arc::new(MultiStubOrigin::new(&[a, b]));
    let engine = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 10).await?;
    let _ = engine.get(ha).await?;
    let _ = engine.get(hb).await?;
    engine.set_max_probe_holds(1);

    anyhow::ensure!(
        engine.try_probe_hold(ha).await? == ProbeHoldOutcome::Held,
        "first hold fits budget"
    );
    anyhow::ensure!(
        engine.try_probe_hold(hb).await? == ProbeHoldOutcome::BudgetExhausted,
        "second distinct hold must be refused when budget is exhausted"
    );
    Ok(())
}

#[tokio::test]
async fn probe_hold_disabled_when_budget_zero() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"unhold me";
    let hash = Hash::new(payload);
    let engine = CacheEngine::open(
        tmp.path(),
        vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
        10,
    )
    .await?;
    let _ = engine.get(hash).await?;
    engine.set_max_probe_holds(0);
    // `max == 0` is an operator config decision (holds turned off), not
    // budget pressure — it must report a cause distinct from
    // `BudgetExhausted` (#739) so the "increase max_probe_holds" alert
    // isn't tripped by an intentional disable.
    anyhow::ensure!(
        engine.try_probe_hold(hash).await? == ProbeHoldOutcome::HoldsDisabled,
        "max_probe_holds=0 must disable has_blob:true entirely"
    );
    Ok(())
}

#[tokio::test]
async fn runtime_disable_overrides_existing_probe_hold() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"held then disabled";
    let hash = Hash::new(payload);
    let engine = CacheEngine::open(
        tmp.path(),
        vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
        10,
    )
    .await?;
    let _ = engine.get(hash).await?;
    engine.set_max_probe_holds(1);
    anyhow::ensure!(
        engine.try_probe_hold(hash).await? == ProbeHoldOutcome::Held,
        "hold should be granted while the budget is positive"
    );
    // Lowering the budget to 0 while a hold is live must take effect
    // immediately: a re-probe for the already-held blob must NOT refresh
    // the hold and re-sign has_blob:true (#739). `max == 0` means "never
    // sign has_blob:true", unconditionally.
    engine.set_max_probe_holds(0);
    anyhow::ensure!(
        engine.try_probe_hold(hash).await? == ProbeHoldOutcome::HoldsDisabled,
        "a runtime disable must override an existing live hold"
    );
    Ok(())
}

#[tokio::test]
async fn absent_blob_is_unavailable_even_when_holds_disabled() -> anyhow::Result<()> {
    // Precedence pin (#739): the `max == 0` early return must sit *after*
    // the `has()` check, so an absent blob is a true negative
    // (`Unavailable`), never a `HoldsDisabled` config-disable event. Guards
    // against a future reorder that moves the cheap `max == 0` load above
    // the store lookup and silently inflates
    // `probe_hold_unavailable{reason="disabled"}` with
    // probes for content the node never had.
    let tmp = tempfile::tempdir()?;
    let absent = Hash::new(b"never fetched");
    let engine = CacheEngine::open(tmp.path(), vec![], 10).await?;
    engine.set_max_probe_holds(0);
    anyhow::ensure!(
        engine.try_probe_hold(absent).await? == ProbeHoldOutcome::Unavailable,
        "absent blob must win over holds-disabled"
    );
    Ok(())
}

#[tokio::test]
async fn operator_evict_overrides_probe_hold() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"dmca target";
    let hash = Hash::new(payload);
    let engine = CacheEngine::open(
        tmp.path(),
        vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
        10,
    )
    .await?;
    let _ = engine.get(hash).await?;
    anyhow::ensure!(
        engine.try_probe_hold(hash).await? == ProbeHoldOutcome::Held,
        "held before evict"
    );

    engine.evict(hash).await?;
    anyhow::ensure!(
        engine.try_probe_hold(hash).await? == ProbeHoldOutcome::Unavailable,
        "operator evict (DMCA) must win over a probe hold"
    );
    Ok(())
}

#[tokio::test]
async fn expired_probe_hold_is_swept_and_re_evictable() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"expiring blob";
    let hash = Hash::new(payload);
    let engine = CacheEngine::open(
        tmp.path(),
        vec![Arc::new(StubOrigin::new(payload)) as Arc<dyn Origin>],
        10,
    )
    .await?;
    let _ = engine.get(hash).await?;
    engine.observe_hit(hash); // make it an LRU candidate

    // Inject an already-expired hold directly (the real 35s duration is
    // impractical to sleep on a real clock).
    {
        let mut g = engine
            .inner
            .probe_holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let past = tokio::time::Instant::now()
            .checked_sub(Duration::from_secs(1))
            .unwrap_or_else(tokio::time::Instant::now);
        g.insert(hash, past);
    }

    anyhow::ensure!(
        engine.probe_hold_slots_used() == 0,
        "expired hold must be swept from the slot count"
    );
    anyhow::ensure!(
        engine
            .eviction_candidates()
            .into_inner()
            .contains_key(&hash),
        "an expired hold must no longer shield the hash from LRU"
    );
    Ok(())
}

#[tokio::test]
async fn hit_increments_hits_and_bytes_returned() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let payload = b"hello hit metrics";
    let hash = Hash::new(payload);
    let origin = StubOrigin::new(payload);
    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![Arc::new(origin) as Arc<dyn Origin>],
        10,
        crate::PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    // First get is a pull-through (miss); prime the cache.
    let _ = engine.get(hash).await?;
    let hits_before = cm.hits.get();
    let bytes_before = cm.bytes_returned.get();

    // Second get must be a local hit.
    let bytes = engine.get(hash).await?;
    anyhow::ensure!(
        cm.hits.get() == hits_before + 1,
        "hits should increment by 1 on a cache hit"
    );
    anyhow::ensure!(
        cm.bytes_returned.get() == bytes_before + bytes.len() as u64,
        "bytes_returned should increase by bytes.len() on a cache hit"
    );
    Ok(())
}

/// A blob spanning several chunk groups plus a partial final group, so
/// the bao tree has real interior nodes (matches
/// `decdn-bao-range::streaming`'s test rationale).
fn local_outboard_pull_test_blob() -> Vec<u8> {
    let size = 5 * crate::CHUNK_GROUP_BYTES + 123;
    (0..size).map(|i| (i % 251) as u8).collect()
}

// -- origin_range_wire / origin_fetch_outboard_bytes --

/// A test origin that serves chunk-group-aligned ranges plus a configurable
/// `{H}.obao4` outboard, so the raw origin fetch+encode surface can be
/// exercised without an HTTP/S3/fs backend. `data`/`outboard`/`size` are held
/// independently so a test can serve bytes that do NOT hash to `hash` (a
/// corrupt / misconfigured OWN origin, the local-origin-fault case).
/// `support_range == false` models an origin with no `206`/outboard support,
/// i.e. the [`crate::OriginRangeFetch::Unsupported`] degrade. `max_req`
/// records the largest data span any single range fetch asked for.
#[derive(Debug)]
struct RangeStubOrigin {
    hash: Hash,
    data: Bytes,
    outboard: Option<Bytes>,
    size: u64,
    support_range: bool,
    max_req: AtomicU64,
    /// Windows starting at or past this offset come back one byte short.
    short_from: u64,
    /// Windows starting at or past this offset are declined.
    decline_from: u64,
    /// Windows starting at or past this offset fail with a transport error.
    fail_from: u64,
    /// A window starting at or past this offset panics the fetch.
    panic_from: u64,
    /// Windows starting at or past this offset wait on `gate`.
    gate_from: u64,
    gate: Arc<tokio::sync::Semaphore>,
    /// When true, [`Origin::fetch_outboard`] returns a transport
    /// [`OriginPullError`] instead of a clean decline — the degraded-origin
    /// case the serviceability probe must surface as a fault (#1129), not as a
    /// clean `Ok(None)` absence.
    fault_outboard: bool,
    /// Calls to [`Origin::fetch_outboard`] so far.
    outboard_fetches: AtomicUsize,
    /// When true, [`Origin::fetch_outboard`] waits on `gate`.
    gate_outboard: bool,
    /// Calls to [`Origin::fetch_range_data`] so far, shared so a test can
    /// read it after the engine takes the origin.
    range_fetches: Arc<AtomicUsize>,
}

impl RangeStubOrigin {
    /// An origin that serves `payload` and its genuine outboard for `hash`.
    fn serving(hash: Hash, payload: &[u8], outboard: Bytes) -> Self {
        Self {
            hash,
            data: Bytes::from(payload.to_vec()),
            outboard: Some(outboard),
            size: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            support_range: true,
            max_req: AtomicU64::new(0),
            short_from: u64::MAX,
            decline_from: u64::MAX,
            fail_from: u64::MAX,
            panic_from: u64::MAX,
            gate_from: u64::MAX,
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
            fault_outboard: false,
            outboard_fetches: AtomicUsize::new(0),
            gate_outboard: false,
            range_fetches: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// An origin that knows `hash`'s size but FAULTS its outboard fetch with a
    /// transport error — a degraded own origin, distinct from a clean absence.
    fn outboard_faulting(hash: Hash, size: u64) -> Self {
        Self {
            hash,
            data: Bytes::new(),
            outboard: None,
            size,
            support_range: false,
            max_req: AtomicU64::new(0),
            short_from: u64::MAX,
            decline_from: u64::MAX,
            fail_from: u64::MAX,
            panic_from: u64::MAX,
            gate_from: u64::MAX,
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
            fault_outboard: true,
            outboard_fetches: AtomicUsize::new(0),
            gate_outboard: false,
            range_fetches: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Origin for RangeStubOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch(
        &self,
        hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
    {
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
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, crate::OriginPullError>> + Send + '_>>
    {
        let out = (hash == self.hash).then_some(self.size);
        Box::pin(async move { Ok(out) })
    }

    fn fetch_outboard(
        &self,
        hash: Hash,
        _outboard_max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, crate::OriginPullError>> + Send + '_>>
    {
        self.outboard_fetches.fetch_add(1, Ordering::SeqCst);
        if self.fault_outboard && hash == self.hash {
            return Box::pin(async move {
                Err(crate::OriginPullError::Transient(anyhow::anyhow!(
                    "stub outboard transport fault"
                )))
            });
        }
        let result = match (&self.outboard, hash == self.hash) {
            (Some(ob), true) => OutboardFetch::Found(ob.clone()),
            _ => OutboardFetch::NotFound,
        };
        let gated = self.gate_outboard;
        Box::pin(async move {
            if gated {
                let _held = self.gate.acquire().await;
            }
            Ok(result)
        })
    }

    fn fetch_range_data(
        &self,
        hash: Hash,
        req: crate::OriginRangeRequest,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<crate::OriginRangeFetch, crate::OriginPullError>>
                + Send
                + '_,
        >,
    > {
        self.max_req.fetch_max(req.len(), Ordering::SeqCst);
        self.range_fetches.fetch_add(1, Ordering::SeqCst);
        assert!(
            req.fetch_start < self.panic_from,
            "stub origin panics on request"
        );
        if req.fetch_start >= self.fail_from {
            return Box::pin(async {
                Err(crate::OriginPullError::Transient(anyhow::anyhow!(
                    "stub range transport fault"
                )))
            });
        }
        let gated = req.fetch_start >= self.gate_from;
        let result = if req.fetch_start >= self.decline_from {
            crate::OriginRangeFetch::Unsupported
        } else if hash == self.hash && self.support_range {
            let s = usize::try_from(req.fetch_start).unwrap_or(usize::MAX);
            let mut e = usize::try_from(req.fetch_end).unwrap_or(usize::MAX);
            if req.fetch_start >= self.short_from {
                e = e.saturating_sub(1);
            }
            match self.data.get(s..e) {
                Some(span) => crate::OriginRangeFetch::Ranged {
                    data: Bytes::copy_from_slice(span),
                },
                None => crate::OriginRangeFetch::NotFound,
            }
        } else {
            crate::OriginRangeFetch::Unsupported
        };
        Box::pin(async move {
            if gated {
                let _held = self.gate.acquire().await;
            }
            Ok(result)
        })
    }
}

/// A blob of a little over two range-pull windows, so a whole-blob range
/// crosses window boundaries.
fn multi_window_test_blob() -> Vec<u8> {
    let size = 2 * crate::RANGE_PULL_WINDOW_BYTES + 5 * crate::CHUNK_GROUP_BYTES + 123;
    (0..size).map(|i| (i % 251) as u8).collect()
}

/// Drain an origin-range wire to its end: the bytes, and the fault it ended on.
async fn drain_wire(mut wire: OriginRangeWire) -> (Vec<u8>, Option<CacheError>) {
    let mut out = Vec::new();
    while let Some(item) = wire.next_chunk().await {
        match item {
            Ok(chunk) => out.extend_from_slice(&chunk),
            Err(fault) => {
                assert!(wire.next_chunk().await.is_none(), "a fault is terminal");
                return (out, Some(fault));
            }
        }
    }
    (out, None)
}

/// A blob of a little over two windows, its hash, and a stub origin
/// serving it with its genuine outboard, plus the whole-blob aligned range.
fn multi_window_stub() -> (Vec<u8>, Hash, RangeStubOrigin, AlignedRange) {
    use bao_tree::io::outboard::PreOrderMemOutboard;

    let data = multi_window_test_blob();
    let ob = PreOrderMemOutboard::create(&data, crate::range_pull::IROH_BLOCK_SIZE);
    let hash = Hash::from(*ob.root.as_bytes());
    let total = u64::try_from(data.len()).unwrap_or(u64::MAX);
    let origin = RangeStubOrigin::serving(hash, &data, Bytes::from(ob.data));
    let aligned = crate::range_pull::align_range(0, 0, total).expect("aligns");
    (data, hash, origin, aligned)
}

async fn stub_engine(
    origins: Vec<RangeStubOrigin>,
) -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let origins = origins
        .into_iter()
        .map(|o| Arc::new(o) as Arc<dyn Origin>)
        .collect();
    let engine = CacheEngine::open(tmp.path(), origins, 64).await?;
    Ok((engine, tmp))
}

/// The serviceability probe confirms an origin that serves the outboard and
/// ranges with one chunk-group read, then answers the next probe from the
/// latch without a read.
#[tokio::test]
async fn origin_range_serviceable_probes_one_group_then_latches() -> anyhow::Result<()> {
    let (data, hash, origin, _) = multi_window_stub();
    let total = u64::try_from(data.len())?;
    let fetches = Arc::clone(&origin.range_fetches);
    let (engine, _tmp) = stub_engine(vec![origin]).await?;
    anyhow::ensure!(engine.origin_range_serviceable(hash, total).await?);
    anyhow::ensure!(
        fetches.load(Ordering::SeqCst) == 1,
        "the first probe reads one window"
    );
    anyhow::ensure!(engine.origin_range_serviceable(hash, total).await?);
    anyhow::ensure!(
        fetches.load(Ordering::SeqCst) == 1,
        "a confirmed origin is not probed again"
    );
    Ok(())
}

/// An origin that publishes the outboard but declines ranges is not
/// serviceable, so the caller degrades before it signs.
#[tokio::test]
async fn origin_range_serviceable_is_false_without_range_support() -> anyhow::Result<()> {
    let (data, hash, mut origin, _) = multi_window_stub();
    let total = u64::try_from(data.len())?;
    origin.support_range = false;
    let (engine, _tmp) = stub_engine(vec![origin]).await?;
    anyhow::ensure!(!engine.origin_range_serviceable(hash, total).await?);
    Ok(())
}

/// A wrong-length first window cannot verify: the probe reports it as a hard
/// fault, not as serviceable.
#[tokio::test]
async fn origin_range_serviceable_faults_on_a_short_first_window() -> anyhow::Result<()> {
    let (data, hash, mut origin, _) = multi_window_stub();
    let total = u64::try_from(data.len())?;
    origin.short_from = 0;
    let (engine, _tmp) = stub_engine(vec![origin]).await?;
    let probed = engine.origin_range_serviceable(hash, total).await;
    anyhow::ensure!(
        matches!(probed, Err(CacheError::VerifyFailed { .. })),
        "a short first window must fault, got {probed:?}"
    );
    Ok(())
}

/// A blob of one chunk group has an empty outboard, which the outboard cache
/// never holds. The latch still keys on the origin that served it, so the
/// second probe reads no range.
#[tokio::test]
async fn origin_range_serviceable_latches_a_small_blob() -> anyhow::Result<()> {
    use bao_tree::io::outboard::PreOrderMemOutboard;

    let data: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
    let ob = PreOrderMemOutboard::create(&data, crate::range_pull::IROH_BLOCK_SIZE);
    anyhow::ensure!(ob.data.is_empty(), "one chunk group has an empty outboard");
    let hash = Hash::from(*ob.root.as_bytes());
    let origin = RangeStubOrigin::serving(hash, &data, Bytes::from(ob.data));
    let fetches = Arc::clone(&origin.range_fetches);
    let (engine, _tmp) = stub_engine(vec![origin]).await?;
    let total = u64::try_from(data.len())?;
    anyhow::ensure!(engine.origin_range_serviceable(hash, total).await?);
    anyhow::ensure!(engine.origin_range_serviceable(hash, total).await?);
    anyhow::ensure!(
        fetches.load(Ordering::SeqCst) == 1,
        "a confirmed origin is probed once, got {}",
        fetches.load(Ordering::SeqCst)
    );
    Ok(())
}

/// A first origin that publishes the outboard but declines ranges does not
/// shadow a second origin that serves both; the probe confirms the second.
#[tokio::test]
async fn origin_range_serviceable_walks_past_a_range_declining_origin() -> anyhow::Result<()> {
    let (data, hash, mut declining, _) = multi_window_stub();
    let total = u64::try_from(data.len())?;
    declining.support_range = false;
    let (_, _, serving, _) = multi_window_stub();
    let declined = Arc::clone(&declining.range_fetches);
    let served = Arc::clone(&serving.range_fetches);
    let (engine, _tmp) = stub_engine(vec![declining, serving]).await?;
    anyhow::ensure!(engine.origin_range_serviceable(hash, total).await?);
    anyhow::ensure!(
        served.load(Ordering::SeqCst) == 1,
        "the second origin serves the probe"
    );
    let before = declined.load(Ordering::SeqCst);
    anyhow::ensure!(engine.origin_range_serviceable(hash, total).await?);
    anyhow::ensure!(
        declined.load(Ordering::SeqCst) == before && served.load(Ordering::SeqCst) == 1,
        "a second probe reads no range from either origin"
    );
    Ok(())
}

/// A first origin that serves a wrong-length first window does not shadow a
/// healthy second origin: the probe confirms the second, and a draw opens
/// on it and completes.
#[tokio::test]
async fn a_short_first_window_advances_to_a_healthy_origin() -> anyhow::Result<()> {
    let (data, hash, mut short, aligned) = multi_window_stub();
    let total = u64::try_from(data.len())?;
    short.short_from = 0;
    let (_, _, healthy, _) = multi_window_stub();
    let (engine, _tmp) = stub_engine(vec![short, healthy]).await?;
    anyhow::ensure!(engine.origin_range_serviceable(hash, total).await?);
    let wire = engine
        .origin_range_wire(hash, &aligned)
        .await?
        .expect("opens on the healthy origin");
    let (_, fault) = drain_wire(wire).await;
    anyhow::ensure!(
        fault.is_none(),
        "the healthy origin's wire is whole: {fault:?}"
    );
    Ok(())
}

/// A draw that waits on a full pool past the warning threshold keeps its
/// place and opens once a permit frees.
#[tokio::test]
async fn a_permit_wait_past_the_warning_keeps_waiting() -> anyhow::Result<()> {
    use std::time::Duration;

    let (_, hash, mut origin, aligned) = multi_window_stub();
    origin.gate_from = crate::RANGE_PULL_WINDOW_BYTES;
    let (engine, _tmp) = stub_engine(vec![origin]).await?;
    let mut wires = Vec::new();
    for _ in 0..crate::MAX_CONCURRENT_RANGE_PULLS {
        let mut wire = engine
            .origin_range_wire(hash, &aligned)
            .await?
            .expect("opens");
        anyhow::ensure!(matches!(wire.next_chunk().await, Some(Ok(_))));
        wires.push(wire);
    }
    tokio::time::pause();
    let waiting = engine.origin_range_wire(hash, &aligned);
    tokio::pin!(waiting);
    anyhow::ensure!(
        tokio::time::timeout(RANGE_PULL_PERMIT_WARN_AFTER * 2, &mut waiting)
            .await
            .is_err(),
        "the warning must not end the wait"
    );
    tokio::time::resume();
    drop(wires.pop());
    let opened = tokio::time::timeout(Duration::from_secs(10), waiting).await??;
    anyhow::ensure!(
        opened.is_some(),
        "the waiting draw opens once a permit frees"
    );
    Ok(())
}

/// An origin that stops serving (declines) a later window ends the wire on
/// an `OriginError`, not a `VerifyFailed` and not a clean end.
#[tokio::test]
async fn origin_range_wire_mid_stream_decline_is_origin_error() -> anyhow::Result<()> {
    let (_, hash, mut origin, aligned) = multi_window_stub();
    origin.decline_from = crate::RANGE_PULL_WINDOW_BYTES;
    let (engine, _tmp) = stub_engine(vec![origin]).await?;
    let wire = engine
        .origin_range_wire(hash, &aligned)
        .await?
        .expect("opens");
    let (_, fault) = drain_wire(wire).await;
    anyhow::ensure!(
        matches!(fault, Some(CacheError::OriginError { .. })),
        "a mid-stream decline must be an OriginError, got {fault:?}"
    );
    Ok(())
}

/// A transport fault on a later window ends the wire on an `OriginError`.
#[tokio::test]
async fn origin_range_wire_mid_stream_transport_fault_is_origin_error() -> anyhow::Result<()> {
    let (_, hash, mut origin, aligned) = multi_window_stub();
    origin.fail_from = crate::RANGE_PULL_WINDOW_BYTES;
    let (engine, _tmp) = stub_engine(vec![origin]).await?;
    let wire = engine
        .origin_range_wire(hash, &aligned)
        .await?
        .expect("opens");
    let (_, fault) = drain_wire(wire).await;
    anyhow::ensure!(
        matches!(fault, Some(CacheError::OriginError { .. })),
        "a mid-stream transport fault must be an OriginError, got {fault:?}"
    );
    Ok(())
}

/// A panic inside the encode task never reads as a clean end, and never as
/// an origin or store fault: the wire ends on an `Internal` fault that
/// carries the panic's message, and the task still releases its permit.
#[tokio::test]
async fn origin_range_wire_panicking_encode_ends_on_an_internal_fault() -> anyhow::Result<()> {
    use std::time::Duration;

    let (_, hash, mut origin, aligned) = multi_window_stub();
    origin.panic_from = crate::RANGE_PULL_WINDOW_BYTES;
    let (engine, _tmp) = stub_engine(vec![origin]).await?;
    for _ in 0..=crate::MAX_CONCURRENT_RANGE_PULLS {
        let wire = tokio::time::timeout(
            Duration::from_secs(10),
            engine.origin_range_wire(hash, &aligned),
        )
        .await
        .map_err(|_| anyhow::anyhow!("a panicked encode must release its permit"))??
        .expect("opens");
        let (_, fault) = drain_wire(wire).await;
        let Some(CacheError::Internal(source)) = &fault else {
            anyhow::bail!("a panicked encode must end on an Internal fault, got {fault:?}");
        };
        anyhow::ensure!(
            format!("{source:#}").contains("stub origin panics on request"),
            "the fault must carry the panic message, got {source:#}"
        );
    }
    Ok(())
}

/// Dropping a wire releases its permit, and the own-origin pool is separate
/// from the range-pull pool: a full own-origin pool does not stall a range
/// pull. A draw that finds the pool full is counted as a permit wait.
#[tokio::test]
async fn origin_range_wire_releases_its_permit_on_drop() -> anyhow::Result<()> {
    use std::time::Duration;

    let (_, hash, mut origin, aligned) = multi_window_stub();
    // Every wire reads one chunk, so its encode parks on the full wire
    // channel, holding its permit.
    origin.gate_from = crate::RANGE_PULL_WINDOW_BYTES;
    let tmp = tempfile::tempdir()?;
    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![Arc::new(origin) as Arc<dyn Origin>],
        64,
        PinnedHashes::empty(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;

    let mut wires = Vec::new();
    for _ in 0..crate::MAX_CONCURRENT_RANGE_PULLS {
        let mut wire = engine
            .origin_range_wire(hash, &aligned)
            .await?
            .expect("opens");
        anyhow::ensure!(matches!(wire.next_chunk().await, Some(Ok(_))));
        wires.push(wire);
    }
    anyhow::ensure!(
        tokio::time::timeout(
            Duration::from_millis(200),
            engine.origin_range_wire(hash, &aligned)
        )
        .await
        .is_err(),
        "a full own-origin pool must make the next open wait",
    );
    anyhow::ensure!(
        cm.range_pull_permit_waits.get() == 1,
        "the waiting draw is counted once, got {}",
        cm.range_pull_permit_waits.get()
    );
    drop(wires);
    let reopened = tokio::time::timeout(
        Duration::from_secs(10),
        engine.origin_range_wire(hash, &aligned),
    )
    .await??;
    anyhow::ensure!(
        reopened.is_some(),
        "dropped wires must release their permits"
    );
    Ok(())
}

/// Dropping a wire whose encode is parked inside an origin window fetch
/// aborts the encode and frees its permit. The fetch never returns, so only
/// the abort ends the task: a dropped receiver alone does not.
#[tokio::test]
async fn dropping_a_wire_parked_in_an_origin_fetch_frees_its_permit() -> anyhow::Result<()> {
    use std::time::Duration;

    let (_, hash, mut origin, aligned) = multi_window_stub();
    origin.gate_from = crate::RANGE_PULL_WINDOW_BYTES;
    let (engine, _tmp) = stub_engine(vec![origin]).await?;
    let pool = Arc::clone(&engine.inner.own_origin_range_pulls);
    let mut wire = engine
        .origin_range_wire(hash, &aligned)
        .await?
        .expect("opens");
    // Drain the first window until the encode parks on the gated second fetch.
    while tokio::time::timeout(Duration::from_millis(200), wire.next_chunk())
        .await
        .is_ok()
    {}
    anyhow::ensure!(
        pool.available_permits() == crate::MAX_CONCURRENT_RANGE_PULLS - 1,
        "the parked encode must hold its permit"
    );
    drop(wire);
    tokio::time::timeout(Duration::from_secs(10), async {
        while pool.available_permits() < crate::MAX_CONCURRENT_RANGE_PULLS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("a dropped wire must abort its parked encode"))?;
    Ok(())
}

/// The 0-byte blob streams its (empty) wire and ends cleanly.
#[tokio::test]
async fn origin_range_wire_empty_blob_ends_cleanly() -> anyhow::Result<()> {
    use bao_tree::io::outboard::PreOrderMemOutboard;

    let ob = PreOrderMemOutboard::create([], crate::range_pull::IROH_BLOCK_SIZE);
    let root: [u8; 32] = *ob.root.as_bytes();
    let hash = Hash::from(root);
    let outboard = Bytes::from(ob.data.clone());
    let origin = RangeStubOrigin::serving(hash, &[], outboard.clone());
    let (engine, _tmp) = stub_engine(vec![origin]).await?;
    let aligned = crate::range_pull::align_range(0, 0, 0)?;
    let wire = engine
        .origin_range_wire(hash, &aligned)
        .await?
        .expect("opens");
    let (wire, fault) = drain_wire(wire).await;
    anyhow::ensure!(fault.is_none(), "the empty blob must not fault: {fault:?}");
    let reference = encode_verified_range(root, &aligned, &[], outboard)?;
    anyhow::ensure!(wire.as_slice() == &reference[8..]);
    Ok(())
}

/// The origin chain advances past an origin that declines the first window
/// and past one whose outboard read faults, to one that serves.
#[tokio::test]
async fn origin_range_wire_falls_back_to_a_serving_origin() -> anyhow::Result<()> {
    let (data, hash, serving, aligned) = multi_window_stub();
    let (_, _, mut declining, _) = multi_window_stub();
    declining.support_range = false;
    let faulting = RangeStubOrigin::outboard_faulting(hash, aligned.blob_size());
    let (engine, _tmp) = stub_engine(vec![faulting, declining, serving]).await?;
    let wire = engine
        .origin_range_wire(hash, &aligned)
        .await?
        .expect("opens");
    let (wire, fault) = drain_wire(wire).await;
    anyhow::ensure!(
        fault.is_none(),
        "the serving origin must not fault: {fault:?}"
    );

    let root = *hash.as_bytes();
    let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(
        &data,
        crate::range_pull::IROH_BLOCK_SIZE,
    );
    let reference = encode_verified_range(root, &aligned, &data, Bytes::from(ob.data))?;
    anyhow::ensure!(wire.as_slice() == &reference[8..], "the wire is byte-exact");
    Ok(())
}

/// Only transport faults and no serving origin: the last fault is the error.
#[tokio::test]
async fn origin_range_wire_reports_a_transport_fault_when_no_origin_serves() -> anyhow::Result<()> {
    let (_, hash, _, aligned) = multi_window_stub();
    let faulting = RangeStubOrigin::outboard_faulting(hash, aligned.blob_size());
    let (engine, _tmp) = stub_engine(vec![faulting]).await?;
    let err = engine
        .origin_range_wire(hash, &aligned)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected an OriginError"))?;
    anyhow::ensure!(matches!(err, CacheError::OriginError { .. }), "got {err:?}");
    Ok(())
}

/// A correct origin: `origin_range_wire` streams a multi-window range as
/// the same header-less wire a whole-span encode produces, asks the origin
/// for at most one window per fetch (#2065), and the wire admits under `H`.
#[tokio::test]
async fn origin_range_wire_streams_admittable_wire_in_windows() -> anyhow::Result<()> {
    use bao_tree::io::outboard::PreOrderMemOutboard;

    let data = multi_window_test_blob();
    let ob = PreOrderMemOutboard::create(&data, crate::range_pull::IROH_BLOCK_SIZE);
    let root: [u8; 32] = *ob.root.as_bytes();
    let outboard = Bytes::from(ob.data.clone());
    let hash = Hash::from(root);
    let total = u64::try_from(data.len()).unwrap_or(u64::MAX);

    let origin = Arc::new(RangeStubOrigin::serving(hash, &data, outboard.clone()));
    let tmp = tempfile::tempdir()?;
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::clone(&origin) as Arc<dyn Origin>], 64).await?;

    // Start mid-blob so the range is not window-aligned to the blob start.
    let aligned = crate::range_pull::align_range(3 * crate::CHUNK_GROUP_BYTES, 0, total)
        .map_err(|e| anyhow::anyhow!("align: {e}"))?;
    let Some(wire) = engine.origin_range_wire(hash, &aligned).await? else {
        anyhow::bail!("expected Some(wire) — the origin serves the range + outboard");
    };
    let (wire, fault) = drain_wire(wire).await;
    anyhow::ensure!(
        fault.is_none(),
        "a correct origin must not fault: {fault:?}"
    );

    let start = usize::try_from(aligned.fetch_start())?;
    let reference = encode_verified_range(root, &aligned, &data[start..], outboard)?;
    anyhow::ensure!(
        wire.as_slice() == &reference[8..],
        "the streamed wire must equal the header-less whole-span encode"
    );
    anyhow::ensure!(
        origin.max_req.load(Ordering::SeqCst) <= crate::RANGE_PULL_WINDOW_BYTES,
        "each origin read must be at most one window"
    );

    // Round-trip: admit the header-less wire into a FRESH engine, which
    // verifies it against `H`.
    let tmp2 = tempfile::tempdir()?;
    let engine2 = CacheEngine::open(tmp2.path(), vec![], 64).await?;
    let drained = engine2
        .admit_bao_stream(
            hash,
            aligned.chunk_ranges().clone(),
            total,
            Bytes::from(wire),
            None,
        )
        .await
        .map_err(|(_reader, e)| e)?;
    anyhow::ensure!(drained.is_empty(), "the wire is fully drained by admit");
    Ok(())
}

/// A corrupt own origin (a window that does NOT hash to `H`, served with the
/// genuine outboard) ends the wire on a HARD [`CacheError::VerifyFailed`] —
/// never a degrade — even when the corruption is past the first window.
#[tokio::test]
async fn origin_range_wire_hard_faults_on_mismatch() -> anyhow::Result<()> {
    use bao_tree::io::outboard::PreOrderMemOutboard;

    let genuine = multi_window_test_blob();
    let ob = PreOrderMemOutboard::create(&genuine, crate::range_pull::IROH_BLOCK_SIZE);
    let root: [u8; 32] = *ob.root.as_bytes();
    let outboard = Bytes::from(ob.data.clone());
    let hash = Hash::from(root);
    let total = u64::try_from(genuine.len()).unwrap_or(u64::MAX);

    // Same length; the second window's bytes differ, so it will not verify.
    let window = usize::try_from(crate::RANGE_PULL_WINDOW_BYTES)?;
    let mut corrupt = genuine.clone();
    for b in &mut corrupt[window..window + 1024] {
        *b ^= 0xFF;
    }
    let mut origin = RangeStubOrigin::serving(hash, &genuine, outboard);
    origin.data = Bytes::from(corrupt);
    let tmp = tempfile::tempdir()?;
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;

    let aligned =
        crate::range_pull::align_range(0, 0, total).map_err(|e| anyhow::anyhow!("align: {e}"))?;
    let Some(wire) = engine.origin_range_wire(hash, &aligned).await? else {
        anyhow::bail!("expected Some(wire) — the first window serves");
    };
    let (_, fault) = drain_wire(wire).await;
    anyhow::ensure!(
        matches!(fault, Some(CacheError::VerifyFailed { expected }) if expected == hash),
        "a corrupt own origin must be a hard VerifyFailed, got {fault:?}"
    );
    Ok(())
}

/// A later window the origin returns short cannot verify against `H`, so the
/// wire ends on a HARD [`CacheError::VerifyFailed`].
#[tokio::test]
async fn origin_range_wire_hard_faults_on_short_later_window() -> anyhow::Result<()> {
    use bao_tree::io::outboard::PreOrderMemOutboard;

    let data = multi_window_test_blob();
    let ob = PreOrderMemOutboard::create(&data, crate::range_pull::IROH_BLOCK_SIZE);
    let hash = Hash::from(*ob.root.as_bytes());
    let total = u64::try_from(data.len()).unwrap_or(u64::MAX);
    let aligned =
        crate::range_pull::align_range(0, 0, total).map_err(|e| anyhow::anyhow!("align: {e}"))?;

    let mut origin = RangeStubOrigin::serving(hash, &data, Bytes::from(ob.data.clone()));
    origin.short_from = crate::RANGE_PULL_WINDOW_BYTES;
    let tmp = tempfile::tempdir()?;
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;
    let Some(wire) = engine.origin_range_wire(hash, &aligned).await? else {
        anyhow::bail!("expected Some(wire) — the first window serves");
    };
    let (_, fault) = drain_wire(wire).await;
    anyhow::ensure!(
        matches!(fault, Some(CacheError::VerifyFailed { expected }) if expected == hash),
        "a short later window must be a hard VerifyFailed, got {fault:?}"
    );
    Ok(())
}

/// A short FIRST window is a hard [`CacheError::VerifyFailed`] at open,
/// before any wire.
#[tokio::test]
async fn origin_range_wire_hard_faults_on_short_first_window() -> anyhow::Result<()> {
    use bao_tree::io::outboard::PreOrderMemOutboard;

    let data = multi_window_test_blob();
    let ob = PreOrderMemOutboard::create(&data, crate::range_pull::IROH_BLOCK_SIZE);
    let hash = Hash::from(*ob.root.as_bytes());
    let total = u64::try_from(data.len()).unwrap_or(u64::MAX);
    let aligned =
        crate::range_pull::align_range(0, 0, total).map_err(|e| anyhow::anyhow!("align: {e}"))?;

    let mut origin = RangeStubOrigin::serving(hash, &data, Bytes::from(ob.data.clone()));
    origin.short_from = 0;
    let tmp = tempfile::tempdir()?;
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;
    let err = engine
        .origin_range_wire(hash, &aligned)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected VerifyFailed, got Ok"))?;
    anyhow::ensure!(
        matches!(err, CacheError::VerifyFailed { expected } if expected == hash),
        "a short first window must be a hard VerifyFailed, got {err:?}"
    );
    Ok(())
}

/// A wrong-length outboard cannot verify, so it is a HARD
/// [`CacheError::VerifyFailed`] before any wire is produced, and it is never
/// cached.
#[tokio::test]
async fn origin_range_wire_hard_faults_on_wrong_length_outboard() -> anyhow::Result<()> {
    let data = local_outboard_pull_test_blob();
    let hash = Hash::new(&data);
    let total = u64::try_from(data.len()).unwrap_or(u64::MAX);
    let origin = RangeStubOrigin::serving(hash, &data, Bytes::from_static(&[0u8; 64]));
    let tmp = tempfile::tempdir()?;
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;

    let aligned =
        crate::range_pull::align_range(0, 0, total).map_err(|e| anyhow::anyhow!("align: {e}"))?;
    let err = engine
        .origin_range_wire(hash, &aligned)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected VerifyFailed, got Ok"))?;
    anyhow::ensure!(
        matches!(err, CacheError::VerifyFailed { expected } if expected == hash),
        "a wrong-length outboard must be a hard VerifyFailed, got {err:?}"
    );
    anyhow::ensure!(
        engine.cached_outboard(hash, total).is_none(),
        "a wrong-length outboard must not be cached"
    );
    anyhow::ensure!(
        engine
            .origin_fetch_outboard_bytes(hash, total)
            .await?
            .is_none(),
        "the serviceability probe declines a wrong-length outboard"
    );
    Ok(())
}

/// Every draw of one hash reuses the outboard the first draw read: the origin
/// serves `{H}.obao4` once, however many draws follow.
#[tokio::test]
async fn origin_range_wire_reads_the_outboard_once_per_hash() -> anyhow::Result<()> {
    let (data, hash, origin, aligned) = multi_window_stub();
    let origin = Arc::new(origin);
    let tmp = tempfile::tempdir()?;
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::clone(&origin) as Arc<dyn Origin>], 64).await?;
    let total = u64::try_from(data.len())?;
    anyhow::ensure!(
        engine
            .origin_fetch_outboard_bytes(hash, total)
            .await?
            .is_some()
    );
    for _ in 0..3 {
        let Some(wire) = engine.origin_range_wire(hash, &aligned).await? else {
            anyhow::bail!("expected Some(wire)");
        };
        let (_, fault) = drain_wire(wire).await;
        anyhow::ensure!(fault.is_none(), "a genuine origin ends cleanly: {fault:?}");
    }
    anyhow::ensure!(
        origin.outboard_fetches.load(Ordering::SeqCst) == 1,
        "the probe and three draws must read the outboard once, read {}",
        origin.outboard_fetches.load(Ordering::SeqCst)
    );
    Ok(())
}

/// Concurrent cold misses of one hash read the outboard from the origin
/// once: the later probes wait for the first read and take its cached copy.
#[tokio::test]
async fn concurrent_cold_probes_read_the_outboard_once() -> anyhow::Result<()> {
    let (data, hash, mut origin, _) = multi_window_stub();
    origin.gate_outboard = true;
    let origin = Arc::new(origin);
    let gate = Arc::clone(&origin.gate);
    let tmp = tempfile::tempdir()?;
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::clone(&origin) as Arc<dyn Origin>], 64).await?;
    let total = u64::try_from(data.len())?;
    let probes: Vec<_> = (0..8)
        .map(|_| {
            let engine = engine.clone();
            tokio::spawn(async move { engine.origin_fetch_outboard_bytes(hash, total).await })
        })
        .collect();
    // Let every probe reach the origin read or the flight lock, then open
    // the gate for the one read in flight.
    tokio::time::sleep(Duration::from_millis(50)).await;
    gate.add_permits(64);
    for probe in probes {
        anyhow::ensure!(probe.await??.is_some(), "every probe gets the outboard");
    }
    anyhow::ensure!(
        origin.outboard_fetches.load(Ordering::SeqCst) == 1,
        "eight concurrent probes must read the outboard once, read {}",
        origin.outboard_fetches.load(Ordering::SeqCst)
    );
    Ok(())
}

/// `pull_through_bytes` counts the outboard once, at the origin read, plus
/// each data window; the probe and later draws that reuse the cached copy do
/// not count it again.
#[tokio::test]
async fn pull_through_bytes_counts_the_outboard_once() -> anyhow::Result<()> {
    let (data, hash, origin, aligned) = multi_window_stub();
    let outboard_len = u64::try_from(origin.outboard.as_ref().map_or(0, Bytes::len))?;
    let tmp = tempfile::tempdir()?;
    let cm = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![Arc::new(origin) as Arc<dyn Origin>],
        64,
        crate::PinnedHashes::default(),
        crate::RetryPolicy::default(),
        CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cm)),
        Duration::ZERO,
    )
    .await?;
    let total = u64::try_from(data.len())?;
    anyhow::ensure!(
        engine
            .origin_fetch_outboard_bytes(hash, total)
            .await?
            .is_some()
    );
    let draws = 2u64;
    for _ in 0..draws {
        let Some(wire) = engine.origin_range_wire(hash, &aligned).await? else {
            anyhow::bail!("expected Some(wire)");
        };
        let (_, fault) = drain_wire(wire).await;
        anyhow::ensure!(fault.is_none(), "clean drain: {fault:?}");
    }
    let span = aligned.fetch_end() - aligned.fetch_start();
    let got = cm.pull_through_bytes.get();
    anyhow::ensure!(
        got == outboard_len + draws * span,
        "want one outboard ({outboard_len}) plus {draws} spans of {span}, got {got}"
    );
    Ok(())
}

/// A draw that fails bao verification evicts the cached outboard, so the next
/// draw reads it from the origin again instead of reusing a possibly bad copy.
#[tokio::test]
async fn a_verify_fault_evicts_the_cached_outboard() -> anyhow::Result<()> {
    let (genuine, hash, mut origin, aligned) = multi_window_stub();
    let window = usize::try_from(crate::RANGE_PULL_WINDOW_BYTES)?;
    let mut corrupt = genuine;
    for b in &mut corrupt[window..window + 1024] {
        *b ^= 0xFF;
    }
    origin.data = Bytes::from(corrupt);
    let origin = Arc::new(origin);
    let tmp = tempfile::tempdir()?;
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::clone(&origin) as Arc<dyn Origin>], 64).await?;
    for round in 1..=2 {
        let Some(wire) = engine.origin_range_wire(hash, &aligned).await? else {
            anyhow::bail!("expected Some(wire) — the first window serves");
        };
        let (_, fault) = drain_wire(wire).await;
        anyhow::ensure!(
            matches!(fault, Some(CacheError::VerifyFailed { .. })),
            "a corrupt window is a hard VerifyFailed, got {fault:?}"
        );
        anyhow::ensure!(
            origin.outboard_fetches.load(Ordering::SeqCst) == round,
            "each draw after a verify fault re-reads the outboard"
        );
    }
    Ok(())
}

/// A stuck outboard fetch runs past its budget and advances the chain to
/// the next origin. With no other origin, the probe reports the timeout as
/// a fault (the #1129 latch), not as a clean absence.
#[tokio::test]
async fn a_stuck_outboard_fetch_times_out_and_advances_the_chain() -> anyhow::Result<()> {
    let (_, hash, serving, aligned) = multi_window_stub();
    let total = aligned.blob_size();
    let mut stuck =
        RangeStubOrigin::serving(hash, &[], serving.outboard.clone().unwrap_or_default());
    stuck.gate_outboard = true;
    let (engine, _tmp) = stub_engine(vec![stuck, serving]).await?;
    engine.set_origin_read_budget(Duration::from_millis(50), u64::MAX);
    anyhow::ensure!(
        engine
            .origin_fetch_outboard_bytes(hash, total)
            .await?
            .is_some(),
        "the second origin serves the outboard after the first times out"
    );

    let (_, hash, serving, _) = multi_window_stub();
    let mut stuck = RangeStubOrigin::serving(hash, &[], serving.outboard.unwrap_or_default());
    stuck.gate_outboard = true;
    let (engine, _tmp) = stub_engine(vec![stuck]).await?;
    engine.set_origin_read_budget(Duration::from_millis(50), u64::MAX);
    let err = engine.origin_fetch_outboard_bytes(hash, total).await.err();
    anyhow::ensure!(
        matches!(&err, Some(CacheError::OriginError { source, .. })
            if source.to_string().contains("budget")),
        "a lone stuck origin is a timeout fault, got {err:?}"
    );
    Ok(())
}

/// A first window that runs past its budget advances the chain to the next
/// origin, which serves the range.
#[tokio::test]
async fn a_stuck_first_window_advances_the_chain() -> anyhow::Result<()> {
    let (data, hash, serving, aligned) = multi_window_stub();
    let mut stuck =
        RangeStubOrigin::serving(hash, &data, serving.outboard.clone().unwrap_or_default());
    stuck.gate_from = 0;
    let (engine, _tmp) = stub_engine(vec![stuck, serving]).await?;
    engine.set_origin_read_budget(Duration::from_millis(50), u64::MAX);
    let Some(wire) = engine.origin_range_wire(hash, &aligned).await? else {
        anyhow::bail!("expected Some(wire) from the second origin");
    };
    let (_, fault) = drain_wire(wire).await;
    anyhow::ensure!(
        fault.is_none(),
        "the second origin serves cleanly: {fault:?}"
    );
    Ok(())
}

/// An origin whose outboard is cached but that has no data does not lend
/// that outboard to another origin's data: the next origin serves its own
/// outboard and data, so an orphaned, corrupt outboard cannot fail every
/// draw.
#[tokio::test]
async fn an_origin_without_data_does_not_lend_its_outboard() -> anyhow::Result<()> {
    let (_, hash, serving, aligned) = multi_window_stub();
    let total = aligned.blob_size();
    let genuine = serving.outboard.clone().unwrap_or_default();
    // Same length, wrong bytes: passes the length gate, fails verification.
    let corrupt = Bytes::from(vec![0u8; genuine.len()]);
    let mut orphan = RangeStubOrigin::serving(hash, &[], corrupt);
    orphan.support_range = false;
    let (engine, _tmp) = stub_engine(vec![orphan, serving]).await?;
    // The probe caches the orphan's copy: it is first in the chain.
    anyhow::ensure!(
        engine
            .origin_fetch_outboard_bytes(hash, total)
            .await?
            .is_some()
    );
    for _ in 0..2 {
        let Some(wire) = engine.origin_range_wire(hash, &aligned).await? else {
            anyhow::bail!("expected Some(wire) from the second origin");
        };
        let (_, fault) = drain_wire(wire).await;
        anyhow::ensure!(
            fault.is_none(),
            "the second origin serves cleanly: {fault:?}"
        );
    }
    anyhow::ensure!(
        engine.cached_outboard(hash, total) == Some(genuine),
        "the serving origin's outboard replaces the orphan's in the cache"
    );
    Ok(())
}

/// A window fetch that outlasts its budget ends the wire on an origin
/// transport fault, not a hang.
#[tokio::test]
async fn a_stuck_window_fetch_times_out() -> anyhow::Result<()> {
    let (_, hash, mut origin, aligned) = multi_window_stub();
    // Every window past the first waits on a gate that never opens.
    origin.gate_from = crate::RANGE_PULL_WINDOW_BYTES;
    let (engine, _tmp) = stub_engine(vec![origin]).await?;
    engine.set_origin_read_budget(Duration::from_millis(50), u64::MAX);
    let Some(wire) = engine.origin_range_wire(hash, &aligned).await? else {
        anyhow::bail!("expected Some(wire) — the first window serves");
    };
    let (_, fault) = tokio::time::timeout(Duration::from_secs(10), drain_wire(wire)).await?;
    anyhow::ensure!(
        matches!(&fault, Some(CacheError::OriginError { source, .. })
            if source.to_string().contains("budget")),
        "a stuck window must end on a timeout OriginError, got {fault:?}"
    );
    Ok(())
}

/// An origin with no range support degrades to `Ok(None)` — the caller then
/// falls through to a whole-blob path.
#[tokio::test]
async fn origin_range_wire_none_when_unsupported() -> anyhow::Result<()> {
    let data = local_outboard_pull_test_blob();
    let hash = Hash::new(&data);
    let total = u64::try_from(data.len()).unwrap_or(u64::MAX);
    let mut origin = RangeStubOrigin::serving(hash, &data, Bytes::new());
    origin.outboard = None;
    origin.support_range = false;
    let tmp = tempfile::tempdir()?;
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(origin) as Arc<dyn Origin>], 64).await?;

    let aligned =
        crate::range_pull::align_range(0, 0, total).map_err(|e| anyhow::anyhow!("align: {e}"))?;
    anyhow::ensure!(
        engine.origin_range_wire(hash, &aligned).await?.is_none(),
        "an unsupported origin must degrade origin_range_wire to Ok(None)"
    );
    Ok(())
}

/// `origin_fetch_outboard_bytes` returns the outboard when an origin
/// publishes it, and `None` when none do.
#[tokio::test]
async fn origin_fetch_outboard_bytes_found_and_absent() -> anyhow::Result<()> {
    use bao_tree::io::outboard::PreOrderMemOutboard;

    let data = local_outboard_pull_test_blob();
    let ob = PreOrderMemOutboard::create(&data, crate::range_pull::IROH_BLOCK_SIZE);
    let outboard = Bytes::from(ob.data.clone());
    let hash = Hash::new(&data);
    let total = u64::try_from(data.len()).unwrap_or(u64::MAX);

    let serving = RangeStubOrigin::serving(hash, &data, outboard.clone());
    let tmp = tempfile::tempdir()?;
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(serving) as Arc<dyn Origin>], 64).await?;
    anyhow::ensure!(
        engine.origin_fetch_outboard_bytes(hash, total).await? == Some(outboard),
        "a publishing origin must return its outboard bytes"
    );

    let bare = OutboardStubOrigin::new(&data, None);
    let tmp2 = tempfile::tempdir()?;
    let engine2 =
        CacheEngine::open(tmp2.path(), vec![Arc::new(bare) as Arc<dyn Origin>], 64).await?;
    anyhow::ensure!(
        engine2
            .origin_fetch_outboard_bytes(hash, total)
            .await?
            .is_none(),
        "no origin publishes the outboard; must be Ok(None)"
    );
    Ok(())
}

/// A genuine transport fault fetching the outboard (a degraded own origin) is
/// surfaced as `Err`, NOT collapsed into a clean `Ok(None)` absence — so the
/// serviceability caller can latch it into `fault_seen` (#1129) and terminate a
/// resulting miss as `InternalError` rather than a bare `NotFound`.
#[tokio::test]
async fn origin_fetch_outboard_bytes_surfaces_a_transport_fault() -> anyhow::Result<()> {
    let data = local_outboard_pull_test_blob();
    let hash = Hash::new(&data);
    let total = u64::try_from(data.len()).unwrap_or(u64::MAX);

    let faulting = RangeStubOrigin::outboard_faulting(hash, total);
    let tmp = tempfile::tempdir()?;
    let engine =
        CacheEngine::open(tmp.path(), vec![Arc::new(faulting) as Arc<dyn Origin>], 64).await?;
    anyhow::ensure!(
        engine
            .origin_fetch_outboard_bytes(hash, total)
            .await
            .is_err(),
        "an outboard transport fault must surface as Err, not a clean Ok(None)"
    );
    Ok(())
}

// -- #1607: admit_bao tags its partial so it survives GC --

fn synth_blob(len: usize) -> ([u8; 32], Vec<u8>, bytes::Bytes) {
    let mut plaintext = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut plaintext {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes()[0];
    }
    let ob = bao_tree::io::outboard::PreOrderMemOutboard::create(
        &plaintext,
        crate::range_pull::IROH_BLOCK_SIZE,
    );
    (*ob.root.as_bytes(), plaintext, bytes::Bytes::from(ob.data))
}

fn bao_for(
    root: [u8; 32],
    plaintext: &[u8],
    outboard: bytes::Bytes,
    off: u64,
    len: u64,
    total: u64,
) -> (Hash, bao_tree::ChunkRanges, bytes::Bytes) {
    let aligned = crate::range_pull::align_range(off, len, total).unwrap();
    let s = aligned.fetch_start() as usize;
    let e = aligned.fetch_end() as usize;
    let encoded =
        crate::range_pull::encode_verified_range(root, &aligned, &plaintext[s..e], outboard)
            .unwrap();
    (Hash::from(root), aligned.chunk_ranges().clone(), encoded)
}

async fn count_tags_for(engine: &CacheEngine, hash: Hash) -> usize {
    let mut stream = engine.inner.store.tags().list().await.unwrap();
    let mut n = 0usize;
    while let Some(info) = stream.next().await {
        if info.unwrap().hash == hash {
            n += 1;
        }
    }
    n
}

#[tokio::test]
async fn admit_bao_tags_the_partial() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);

    // Admit one interior group -> a genuine partial.
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard.clone(), group, group, total);
    engine.admit_bao(hash, ranges, bao).await.unwrap();
    assert!(
        !engine.present_ranges(hash).await.unwrap().is_complete(),
        "still partial"
    );
    assert_eq!(
        count_tags_for(&engine, hash).await,
        1,
        "partial admit creates exactly one protecting tag"
    );

    // Idempotent: admit a second group -> still exactly one tag.
    let (h2, r2, b2) = bao_for(root, &plaintext, outboard, 2 * group, group, total);
    engine.admit_bao(h2, r2, b2).await.unwrap();
    assert_eq!(
        count_tags_for(&engine, hash).await,
        1,
        "re-admit does not proliferate tags"
    );
}

#[tokio::test]
async fn protect_partial_skips_store_write_when_memoized() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);

    // First admit writes the protecting tag and memoizes it.
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);
    engine.admit_bao(hash, ranges, bao).await.unwrap();
    assert_eq!(count_tags_for(&engine, hash).await, 1);
    assert!(engine.inner.partial_protected.contains_key(&hash));

    // Delete the tag directly at the store, leaving the memo intact. A
    // memoized `protect_partial` must short-circuit and NOT re-create it —
    // proving it skipped the redundant store write on re-admit.
    let name = format!("decdn-partial-{hash}");
    engine
        .inner
        .store
        .tags()
        .delete(name.as_bytes())
        .await
        .unwrap();
    assert_eq!(count_tags_for(&engine, hash).await, 0);
    engine.protect_partial(hash).await.unwrap();
    assert_eq!(
        count_tags_for(&engine, hash).await,
        0,
        "a memoized protect_partial must skip the store write"
    );
}

#[tokio::test]
async fn dropping_tags_reinvalidates_protect_memo() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);

    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);
    engine.admit_bao(hash, ranges, bao).await.unwrap();
    assert!(engine.inner.partial_protected.contains_key(&hash));

    // The tag-drop path clears the memo, so a re-admit re-protects rather
    // than trusting a stale entry for a tag that no longer exists.
    engine.drop_named_tags_for(hash).await.unwrap();
    assert_eq!(count_tags_for(&engine, hash).await, 0);
    assert!(
        !engine.inner.partial_protected.contains_key(&hash),
        "dropping the tag must invalidate the memo"
    );
    engine.protect_partial(hash).await.unwrap();
    assert_eq!(
        count_tags_for(&engine, hash).await,
        1,
        "protect_partial re-creates the tag after the memo is invalidated"
    );
}

// -- admit_bao_stream — O(chunk-group) streaming range admit --

#[tokio::test]
async fn admit_bao_stream_admits_a_partial_range() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);

    // `bao_for` (via `encode_verified_range`) prepends the 8-byte LE size
    // header the in-memory `import_bao_bytes` path expects. The wire
    // `admit_bao_stream` consumes is header-less (ADR 038) — the size
    // comes from `total_bytes` instead — so strip it here to synthesize
    // that header-less wire for the reader.
    assert!(bao.len() > 8, "bao_for output must carry the 8-byte header");
    let header_less = bao.slice(8..);

    let reader = engine
        .admit_bao_stream(hash, ranges.clone(), total, header_less, None)
        .await
        .map_err(|(_reader, e)| e)
        .unwrap();
    assert_eq!(reader.len(), 0, "the reader is fully drained");

    let present = engine.present_ranges(hash).await.unwrap();
    assert!(!present.is_complete(), "still partial");
    assert!(!present.is_empty(), "the admitted range is present");
    assert_eq!(
        count_tags_for(&engine, hash).await,
        1,
        "streaming admit creates exactly one protecting tag"
    );
    assert!(
        engine.eviction_candidates().contains_key(&hash),
        "a streamed partial is an eviction candidate with no serve after it (#2157)"
    );
}

#[tokio::test]
async fn admit_bao_stream_captures_proof_into_the_session_during_import() {
    // With a serve leg attached, the decode pass captures the range's proof
    // nodes straight into the shared session — no post-admit `export_bao`
    // read-back. Prove the captured set equals exactly what the read-back would
    // have recovered, so a serve leg reads back an identical outboard.
    use bao_tree::io::fsm::Outboard;
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);
    let header_less = bao.slice(8..);

    let session = crate::FillSession::new(bao_tree::blake3::Hash::from(root), total);
    engine
        .admit_bao_stream(hash, ranges.clone(), total, header_less, Some(&session))
        .await
        .map_err(|(_reader, e)| e)
        .unwrap();

    // The proof nodes an `export_bao` read-back would recover for this range.
    let expected = engine.outboard_pairs(hash, &ranges).await.unwrap();
    assert!(
        !expected.is_empty(),
        "the admitted range spans interior proof nodes"
    );
    // Every one was captured into the session during import: a reader minted
    // from the session loads each without awaiting a further fill.
    let mut reader = session.outboard_reader();
    for (node, pair) in expected {
        assert_eq!(
            reader.load(node).await.unwrap(),
            Some(pair),
            "node {node:?} was captured during import"
        );
    }
}

/// #2328: once the pull has faulted, a serve reader with the store fallback
/// reads every node the store holds content under from the store's outboard.
/// A node over absent content still fails, and so does a reader without the
/// fallback.
#[tokio::test]
async fn a_dead_reader_reads_a_node_over_held_content_from_the_store() {
    use bao_tree::io::fsm::Outboard;
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 8 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let (hash, held, bao) = bao_for(root, &plaintext, outboard, group, group, total);
    engine
        .admit_bao_stream(hash, held.clone(), total, bao.slice(8..), None)
        .await
        .map_err(|(_reader, e)| e)
        .unwrap();

    let session = crate::FillSession::new(bao_tree::blake3::Hash::from(root), total);
    session.mark_ended(Err(crate::FillError::new("upstream died")));

    let truth = bao_tree::io::outboard::PreOrderMemOutboard::create(
        &plaintext,
        crate::range_pull::IROH_BLOCK_SIZE,
    );
    let tree = truth.tree;
    let internal: Vec<_> = tree
        .pre_order_nodes_iter()
        .filter(|n| tree.pre_order_offset(*n).is_some())
        .collect();
    let over_held = |n: &bao_tree::TreeNode| {
        !(&bao_tree::ChunkRanges::from(n.chunk_range()) & &held).is_empty()
    };
    assert!(internal.iter().any(over_held) && !internal.iter().all(over_held));

    let mut bare = session.outboard_reader();
    assert!(
        bare.load(tree.root()).await.is_err(),
        "without the fallback a dead node fails"
    );

    let mut reader = session
        .outboard_reader()
        .with_store_fallback(engine.clone());
    for node in internal {
        let loaded = reader.load(node).await;
        if over_held(&node) {
            let want = bao_tree::io::sync::Outboard::load(&truth, node).unwrap();
            assert_eq!(loaded.unwrap(), want, "node {node:?} reads from the store");
        } else {
            let err = loaded.expect_err("the store holds nothing under this node");
            assert!(err.to_string().contains("upstream pull failed"), "{err}");
        }
    }
}

#[tokio::test]
async fn admit_bao_stream_captures_a_leafs_proof_before_the_admit_finishes() {
    // A serve leg sharing the fill reads the first leaf's proof nodes as soon
    // as that leaf arrives, while the rest of the draw is still in flight.
    // The feed stalls for good after the first leaf, so the admit never
    // finishes, yet the root proof node is already in the session.
    use bao_tree::io::fsm::Outboard;

    /// Header-less bao wire reader that yields its bytes, then parks forever.
    struct StallingReader(bytes::Bytes);
    impl iroh_io::AsyncStreamReader for StallingReader {
        async fn read_bytes(&mut self, len: usize) -> std::io::Result<bytes::Bytes> {
            if self.0.is_empty() {
                std::future::pending::<()>().await;
            }
            Ok(self.0.split_to(self.0.len().min(len)))
        }
        async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
            if self.0.len() < L {
                std::future::pending::<()>().await;
            }
            let g = self.0.split_to(L);
            let mut out = [0u8; L];
            out.copy_from_slice(&g);
            Ok(out)
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, 0, total, total);
    let wire = bao.slice(8..);

    // The pre-order wire for four leaves opens with the root pair and the left
    // child pair, then leaf 0. Feed exactly that much.
    let first_leaf_end = 2 * 64 + usize::try_from(group).unwrap();
    let reader = StallingReader(wire.slice(..first_leaf_end));
    let root_pair = (
        bao_tree::blake3::Hash::from_bytes(wire[..32].try_into().unwrap()),
        bao_tree::blake3::Hash::from_bytes(wire[32..64].try_into().unwrap()),
    );
    let root_node = bao_tree::BaoTree::new(total, crate::range_pull::IROH_BLOCK_SIZE).root();

    let session = crate::FillSession::new(bao_tree::blake3::Hash::from(root), total);
    let mut outboard_reader = session.outboard_reader();
    let admit = engine.admit_bao_stream(hash, ranges, total, reader, Some(&session));
    tokio::pin!(admit);
    let loaded = tokio::select! {
        _ = &mut admit => panic!("a stalled feed cannot finish the admit"),
        loaded = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            outboard_reader.load(root_node),
        ) => loaded,
    };
    assert_eq!(
        loaded
            .expect("the root proof node is captured while the admit is in flight")
            .unwrap(),
        Some(root_pair),
    );
}

#[tokio::test]
async fn admit_bao_stream_handles_zero_total_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();

    // The canonical empty blob is a no-op success: nothing to decode or admit.
    engine
        .admit_bao_stream(Hash::EMPTY, ChunkRanges::empty(), 0, Bytes::new(), None)
        .await
        .map_err(|(_reader, e)| e)
        .expect("admitting the empty blob is a no-op success");

    // A zero size under any OTHER hash is an upstream inconsistency (the signed
    // total_bytes disagrees with a non-empty content hash) — a Feed fault.
    let (_reader, err) = engine
        .admit_bao_stream(
            Hash::from([9u8; 32]),
            ChunkRanges::empty(),
            0,
            Bytes::new(),
            None,
        )
        .await
        .expect_err("zero size under a non-empty hash is rejected");
    assert!(
        matches!(err, CacheError::Feed(_)),
        "expected Feed, got {err:?}"
    );
}

#[tokio::test]
async fn admit_bao_stream_rejects_corrupt_bao() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);
    let mut corrupt = bao.slice(8..).to_vec();
    let flip_at = corrupt.len() / 2;
    let byte = corrupt.get_mut(flip_at).expect("non-empty header-less bao");
    *byte ^= 0xFF;

    let (_reader, err) = engine
        .admit_bao_stream(hash, ranges, total, Bytes::from(corrupt), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, CacheError::VerifyFailed { expected } if expected == hash),
        "expected VerifyFailed, got {err:?}"
    );
    assert!(
        engine.present_ranges(hash).await.unwrap().is_empty(),
        "nothing admitted from a corrupt bao"
    );
    assert_eq!(
        count_tags_for(&engine, hash).await,
        0,
        "a rejected import must not tag a partial"
    );
}

// -- coverage: cached-block derivation (#1506) --

#[tokio::test]
async fn coverage_of_absent_hash_is_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let cov = engine.coverage(Hash::from([7u8; 32])).await.unwrap();
    assert!(cov.is_empty(), "an absent hash has no covered blocks");
    let (_, size) = engine.coverage_sized(Hash::from([7u8; 32])).await.unwrap();
    assert_eq!(size, None, "an absent hash has no size to advertise");
}

#[tokio::test]
async fn coverage_of_complete_blob_is_full() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 3 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, 0, total, total);
    engine.admit_bao(hash, ranges, bao).await.unwrap();
    assert!(
        engine.present_ranges(hash).await.unwrap().is_complete(),
        "whole blob admitted in one range"
    );

    let cov = engine.coverage(hash).await.unwrap();
    assert_eq!(
        cov,
        decdn_protocol::Coverage::full(decdn_protocol::num_blocks(total)),
        "a complete blob covers every discovery block it spans"
    );
}

#[tokio::test]
async fn coverage_of_partial_blob_covers_only_present_blocks() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let group = crate::CHUNK_GROUP_BYTES;
    // Total spans two 64 MiB discovery blocks: block 0 is fully in range,
    // block 1 covers the trailing 3 `group`s. iroh-blobs only reports a
    // `Partial` blob's size once the FINAL chunk is present (that is what
    // fixes the tree's total chunk count), so this admits block 0 in
    // full, then separately admits the blob's last group (establishing
    // the validated size) while leaving block 1's middle group missing —
    // block 1 stays not-covered even though the size is now known.
    let total = decdn_protocol::DISCOVERY_BLOCK_BYTES + 3 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);

    let (hash, block0_ranges, block0_bao) = bao_for(
        root,
        &plaintext,
        outboard.clone(),
        0,
        decdn_protocol::DISCOVERY_BLOCK_BYTES,
        total,
    );
    engine
        .admit_bao(hash, block0_ranges, block0_bao)
        .await
        .unwrap();

    let (_, tail_ranges, tail_bao) =
        bao_for(root, &plaintext, outboard, total - group, group, total);
    engine.admit_bao(hash, tail_ranges, tail_bao).await.unwrap();

    assert!(
        !engine.present_ranges(hash).await.unwrap().is_complete(),
        "block 1's middle group was never admitted"
    );

    let cov = engine.coverage(hash).await.unwrap();
    assert!(cov.covers(0), "block 0 was admitted in full");
    assert!(!cov.covers(1), "block 1's middle group is missing");
}

// -- On a front-prefix partial, the observe() bitfield knows the full --
// -- blob size while status() still reports it unknown (#1506). --

#[tokio::test]
async fn spike_bitfield_size_known_before_status_size_on_front_partial() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    // Front-fill only: admit block 0 and NEVER admit the trailing group,
    // so iroh-blobs never sees the blob's final chunk and `status()`
    // never learns the size.
    let total = decdn_protocol::DISCOVERY_BLOCK_BYTES + 3 * crate::CHUNK_GROUP_BYTES;
    let (root, plaintext, outboard) = synth_blob(total as usize);

    let (hash, block0_ranges, block0_bao) = bao_for(
        root,
        &plaintext,
        outboard,
        0,
        decdn_protocol::DISCOVERY_BLOCK_BYTES,
        total,
    );
    engine
        .admit_bao(hash, block0_ranges, block0_bao)
        .await
        .unwrap();

    let status = engine.inner.store.blobs().status(hash).await.unwrap();
    assert!(
        matches!(
            status,
            iroh_blobs::api::blobs::BlobStatus::Partial { size: None }
        ),
        "front-only partial must NOT have a status()-known size yet, got {status:?}"
    );

    let bitfield = engine.inner.store.blobs().observe(hash).await.unwrap();
    assert_eq!(
        bitfield.size(),
        total,
        "observe()'s bitfield must already know the full synthetic blob size"
    );
}

/// A front-prefix partial (block 0 present, no tail, `status()` size
/// still unknown) must still advertise coverage for block 0: the size
/// used to derive discovery blocks comes from the `observe()` bitfield
/// (via [`CacheEngine::present_ranges`]), not from `status()`, which
/// iroh-blobs leaves `None` until the blob's last chunk validates.
#[tokio::test]
async fn coverage_of_front_partial_covers_block_zero_before_status_knows_size() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let total = decdn_protocol::DISCOVERY_BLOCK_BYTES + 3 * crate::CHUNK_GROUP_BYTES;
    let (root, plaintext, outboard) = synth_blob(total as usize);

    let (hash, block0_ranges, block0_bao) = bao_for(
        root,
        &plaintext,
        outboard,
        0,
        decdn_protocol::DISCOVERY_BLOCK_BYTES,
        total,
    );
    engine
        .admit_bao(hash, block0_ranges, block0_bao)
        .await
        .unwrap();

    let status = engine.inner.store.blobs().status(hash).await.unwrap();
    assert!(
        matches!(
            status,
            iroh_blobs::api::blobs::BlobStatus::Partial { size: None }
        ),
        "front-only partial must NOT have a status()-known size yet, got {status:?}"
    );

    let cov = engine.coverage(hash).await.unwrap();
    assert!(
        cov.covers(0),
        "block 0 is fully present; coverage must not depend on status()'s size"
    );
    // The partial advertises its size with its blocks (#2195), read from the
    // same bitfield, before `status()` knows it.
    let (sized_cov, size) = engine.coverage_sized(hash).await.unwrap();
    assert_eq!(sized_cov, cov, "the sized read reports the same blocks");
    assert_eq!(
        size,
        Some(total),
        "a front partial reports the full blob size"
    );
}

// -- #2186: a ranged admit that completes a discovery block announces it --

/// The next `subscribe_inserts` emission, if one is queued. The admit sends
/// before it returns, so an emission it makes is already queued here.
fn next_insert(rx: &mut broadcast::Receiver<Hash>) -> Option<Hash> {
    rx.try_recv().ok()
}

/// ADR 022 §STORE Flow: a node publishes H once it verifies its first
/// 64 MiB block, on any verified partial. The ranged `admit_bao` path must
/// therefore announce the hash when an admit completes a discovery block.
#[tokio::test]
async fn subscribe_inserts_emits_when_admit_bao_completes_a_discovery_block() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let mut rx = engine.subscribe_inserts();
    let total = decdn_protocol::DISCOVERY_BLOCK_BYTES + 3 * crate::CHUNK_GROUP_BYTES;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let (hash, ranges, bao) = bao_for(
        root,
        &plaintext,
        outboard,
        0,
        decdn_protocol::DISCOVERY_BLOCK_BYTES,
        total,
    );

    engine.admit_bao(hash, ranges, bao).await.unwrap();

    assert_eq!(
        next_insert(&mut rx),
        Some(hash),
        "an admit that completes block 0 must announce the hash"
    );
}

/// The serve leg and the node-origin pull leg fill through
/// `admit_bao_stream`, so it must announce a completed block too.
#[tokio::test]
async fn subscribe_inserts_emits_when_admit_bao_stream_completes_a_discovery_block() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let mut rx = engine.subscribe_inserts();
    let total = decdn_protocol::DISCOVERY_BLOCK_BYTES + 3 * crate::CHUNK_GROUP_BYTES;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let (hash, ranges, bao) = bao_for(
        root,
        &plaintext,
        outboard,
        0,
        decdn_protocol::DISCOVERY_BLOCK_BYTES,
        total,
    );

    engine
        .admit_bao_stream(hash, ranges, total, bao.slice(8..), None)
        .await
        .map_err(|(_reader, e)| e)
        .unwrap();

    assert_eq!(
        next_insert(&mut rx),
        Some(hash),
        "a streamed admit that completes block 0 must announce the hash"
    );
}

/// An admit that leaves every block it touches incomplete announces
/// nothing: the hash has no coverage to advertise yet.
#[tokio::test]
async fn subscribe_inserts_silent_for_an_admit_that_completes_no_block() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let mut rx = engine.subscribe_inserts();
    let group = crate::CHUNK_GROUP_BYTES;
    let total = decdn_protocol::DISCOVERY_BLOCK_BYTES + 3 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);

    engine.admit_bao(hash, ranges, bao).await.unwrap();

    assert_eq!(
        next_insert(&mut rx),
        None,
        "one interior group completes no discovery block"
    );
}

/// A blob under 64 MiB is one discovery block, so it announces exactly
/// when the admit that completes the blob lands.
#[tokio::test]
async fn subscribe_inserts_emits_for_a_small_blob_only_once_it_completes() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let mut rx = engine.subscribe_inserts();
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);

    let (hash, front, front_bao) = bao_for(root, &plaintext, outboard.clone(), 0, 2 * group, total);
    engine.admit_bao(hash, front, front_bao).await.unwrap();
    assert_eq!(
        next_insert(&mut rx),
        None,
        "half a single-block blob completes nothing"
    );

    let (_, back, back_bao) = bao_for(root, &plaintext, outboard, 2 * group, 2 * group, total);
    engine.admit_bao(hash, back, back_bao).await.unwrap();
    assert_eq!(
        next_insert(&mut rx),
        Some(hash),
        "the admit that completes the only block must announce the hash"
    );
}

/// The short last block announces like any other: its span ends at the
/// blob's last chunk, not a full 64 MiB past its start.
#[tokio::test]
async fn subscribe_inserts_emits_when_an_admit_completes_the_short_last_block() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let mut rx = engine.subscribe_inserts();
    let group = crate::CHUNK_GROUP_BYTES;
    let block = decdn_protocol::DISCOVERY_BLOCK_BYTES;
    let total = block + 3 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);

    let (hash, tail, tail_bao) = bao_for(root, &plaintext, outboard, block, 3 * group, total);
    engine.admit_bao(hash, tail, tail_bao).await.unwrap();

    assert_eq!(
        next_insert(&mut rx),
        Some(hash),
        "the admit that completes block 1 must announce the hash"
    );
}

/// A middle-of-file fill — no front chunk, no tail chunk — still knows the
/// blob's size from the admit, so completing an interior block announces.
#[tokio::test]
async fn subscribe_inserts_emits_when_an_admit_completes_an_interior_block() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let mut rx = engine.subscribe_inserts();
    let block = decdn_protocol::DISCOVERY_BLOCK_BYTES;
    let total = 2 * block + crate::CHUNK_GROUP_BYTES;
    let (root, plaintext, outboard) = synth_blob(total as usize);

    let (hash, middle, middle_bao) = bao_for(root, &plaintext, outboard, block, block, total);
    engine.admit_bao(hash, middle, middle_bao).await.unwrap();

    assert_eq!(
        next_insert(&mut rx),
        Some(hash),
        "the admit that completes interior block 1 must announce the hash"
    );
}

/// A streamed admit that fails after the store already took a whole block
/// still announces it. The store keeps the verified items, and a gap-fill
/// retry admits only the missing ranges, so no later admit touches that
/// block again.
#[tokio::test]
async fn admit_bao_stream_announces_a_block_it_completed_before_failing() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let mut rx = engine.subscribe_inserts();
    let group = crate::CHUNK_GROUP_BYTES;
    let block = decdn_protocol::DISCOVERY_BLOCK_BYTES;
    let total = block + 3 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, 0, total, total);

    // The pre-order wire carries block 0's subtree first, so cutting the
    // last group off truncates the feed inside block 1.
    let wire = bao.slice(8..);
    let truncated = wire.slice(..wire.len() - usize::try_from(group).unwrap());
    let failed = engine
        .admit_bao_stream(hash, ranges, total, truncated, None)
        .await;
    let Err((_reader, err)) = failed else {
        panic!("a truncated feed must fail the admit");
    };
    // The sender's short delivery, not this node's store.
    assert!(matches!(err, CacheError::Feed(_)), "{err:?}");
    assert!(
        engine.coverage(hash).await.unwrap().covers(0),
        "fixture precondition: block 0 landed before the feed ended"
    );

    assert_eq!(
        next_insert(&mut rx),
        Some(hash),
        "a block that landed before the failure must still be announced"
    );
}

/// The cold-start seed and the lag sweep walk `iter_hashes`, so a partial
/// that ranged fills left behind must appear there.
#[tokio::test]
async fn iter_hashes_includes_partial_blobs() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, group, total);
    engine.admit_bao(hash, ranges, bao).await.unwrap();
    assert!(!engine.present_ranges(hash).await.unwrap().is_complete());

    let hashes = engine.iter_hashes().await.unwrap();
    assert_eq!(hashes, vec![hash], "a held partial is a held blob");
}

/// A middle-range partial (no front, no tail) leaves `status()`'s size
/// unknown, yet its bytes sit on disk. `size_snapshot` — the eviction
/// footprint and the `decdn_cache_bytes` gauge — must count them, and the
/// eviction driver must be able to release them (#2157).
#[tokio::test]
async fn size_snapshot_counts_middle_range_partial_by_present_bytes() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await?;
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);

    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, group, 2 * group, total);
    engine.admit_bao(hash, ranges, bao).await?;

    let status = engine.inner.store.blobs().status(hash).await?;
    anyhow::ensure!(
        matches!(
            status,
            iroh_blobs::api::blobs::BlobStatus::Partial { size: None }
        ),
        "a middle-range partial must leave status()'s size unknown, got {status:?}"
    );

    let sizes = engine.size_snapshot().await?;
    anyhow::ensure!(
        sizes.get(&hash).copied() == Some(2 * group),
        "size_snapshot must count the middle partial's present bytes, got {:?}",
        sizes.get(&hash)
    );
    anyhow::ensure!(
        engine.total_bytes().await? == 2 * group,
        "total_bytes must agree with size_snapshot"
    );
    anyhow::ensure!(
        engine.eviction_candidates().contains_key(&hash),
        "a counted partial must be an eviction candidate with no serve after it"
    );
    Ok(())
}

/// A partial that holds its tail has a validated `status()` size equal to
/// the whole blob, but only some of those bytes are on disk. The footprint
/// counts the present bytes, including the short last chunk (#2157).
#[tokio::test]
async fn size_snapshot_counts_tail_bearing_partial_by_present_bytes_not_total() -> anyhow::Result<()>
{
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await?;
    let group = crate::CHUNK_GROUP_BYTES;
    let tail_start = 3 * group;
    let tail_len = 123;
    let total = tail_start + tail_len;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);

    let (hash, head_ranges, head_bao) =
        bao_for(root, &plaintext, outboard.clone(), 0, group, total);
    engine.admit_bao(hash, head_ranges, head_bao).await?;
    let (_, tail_ranges, tail_bao) =
        bao_for(root, &plaintext, outboard, tail_start, tail_len, total);
    engine.admit_bao(hash, tail_ranges, tail_bao).await?;

    let status = engine.inner.store.blobs().status(hash).await?;
    anyhow::ensure!(
        matches!(
            status,
            iroh_blobs::api::blobs::BlobStatus::Partial { size: Some(s) } if s == total
        ),
        "the tail validates the size, got {status:?}"
    );

    let sizes = engine.size_snapshot().await?;
    anyhow::ensure!(
        sizes.get(&hash).copied() == Some(group + tail_len),
        "size_snapshot must count present bytes, not the validated total {total}, got {:?}",
        sizes.get(&hash)
    );
    Ok(())
}

/// The walk re-observes a partial only once its memoized count is a TTL
/// old: within the TTL it reuses the count, after it it measures the new
/// bytes. A hash that stops being partial leaves the memo.
#[tokio::test]
async fn snapshot_reuses_a_partial_count_within_the_ttl() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await?;
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let (root, plaintext, outboard) = synth_blob(usize::try_from(total)?);
    let store = &engine.inner.store;
    let memo: PartialSizeMemo = Mutex::new(HashMap::new());
    let t0 = Instant::now();

    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard.clone(), group, group, total);
    engine.admit_bao(hash, ranges, bao).await?;
    let first = snapshot_blob_sizes(store, &memo, t0, None).await?;
    anyhow::ensure!(
        first.get(&hash).copied() == Some(group),
        "first walk observes"
    );

    let (_, ranges, bao) = bao_for(root, &plaintext, outboard, 2 * group, group, total);
    engine.admit_bao(hash, ranges, bao).await?;
    let within = snapshot_blob_sizes(store, &memo, t0 + PARTIAL_SIZE_TTL / 2, None).await?;
    anyhow::ensure!(
        within.get(&hash).copied() == Some(group),
        "within the TTL the walk reuses the memoized count, got {:?}",
        within.get(&hash)
    );
    let after = snapshot_blob_sizes(store, &memo, t0 + PARTIAL_SIZE_TTL, None).await?;
    anyhow::ensure!(
        after.get(&hash).copied() == Some(2 * group),
        "after the TTL the walk re-observes, got {:?}",
        after.get(&hash)
    );

    memo.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(Hash::new(b"no longer partial"), (1, t0));
    snapshot_blob_sizes(store, &memo, t0 + PARTIAL_SIZE_TTL, None).await?;
    let kept: Vec<Hash> = memo
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .keys()
        .copied()
        .collect();
    anyhow::ensure!(
        kept == vec![hash],
        "the walk drops non-partial memo entries, got {kept:?}"
    );
    Ok(())
}

/// A hash that GC removes between `status()` and `observe()` must read as
/// 0 bytes, not fail: the store answers `observe` on a missing entry with
/// an empty bitfield.
#[tokio::test]
async fn observed_present_bytes_of_an_absent_hash_is_zero() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await?;
    let bytes = observed_present_bytes(engine.inner.store.blobs(), Hash::new(b"absent")).await?;
    anyhow::ensure!(bytes == 0, "an absent hash holds no bytes, got {bytes}");
    Ok(())
}

#[test]
fn present_byte_count_clamps_to_size() {
    use bao_tree::ChunkNum;
    assert_eq!(present_byte_count(&ChunkRanges::empty(), 5000), 0);
    assert_eq!(
        present_byte_count(&ChunkRanges::all(), 5000),
        5000,
        "a complete bitfield counts the whole blob, short last chunk included"
    );
    assert_eq!(
        present_byte_count(&ChunkRanges::from(ChunkNum(1)..ChunkNum(3)), 5000),
        2048,
        "a closed span counts 1 KiB per chunk"
    );
    assert_eq!(
        present_byte_count(&ChunkRanges::from(ChunkNum(4)..), 5000),
        5000 - 4096,
        "an open tail ends at the blob size"
    );
    assert_eq!(
        present_byte_count(&ChunkRanges::from(ChunkNum(4)..ChunkNum(5)), 5000),
        904,
        "a closed span over the short last chunk counts its true length"
    );
    assert_eq!(
        present_byte_count(&ChunkRanges::from(ChunkNum(10)..ChunkNum(12)), 5000),
        0,
        "a span wholly past the size counts nothing"
    );
    assert_eq!(
        present_byte_count(
            &(ChunkRanges::from(ChunkNum(0)..ChunkNum(1)) | ChunkRanges::from(ChunkNum(4)..)),
            5000
        ),
        1024 + 904,
        "disjoint spans sum"
    );
    assert_eq!(
        present_byte_count(&ChunkRanges::from(ChunkNum(1)..), 0),
        0,
        "an unknown size contributes nothing past it"
    );
}

/// Advertise (`coverage`) and serve (`partial_hit_size`, mirrored here via
/// `present_ranges` + `missing_ranges` — the exact sequence it runs)
/// must AGREE on a front-prefix partial (#1506).
///
/// Both size the blob from the `observe()` bitfield. `status()`'s size is
/// not usable here: iroh-blobs leaves it `None` for a `Partial` blob until
/// its FINAL chunk validates, so on a front-prefix partial (block 0
/// present, no tail) a `status()`-sized serve gate declines a block that
/// `coverage` already advertises as covered.
#[tokio::test]
async fn partial_hit_size_source_agrees_with_coverage_on_front_partial() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 16).await.unwrap();
    let total = decdn_protocol::DISCOVERY_BLOCK_BYTES + 3 * crate::CHUNK_GROUP_BYTES;
    let (root, plaintext, outboard) = synth_blob(total as usize);

    let (hash, block0_ranges, block0_bao) = bao_for(
        root,
        &plaintext,
        outboard,
        0,
        decdn_protocol::DISCOVERY_BLOCK_BYTES,
        total,
    );
    engine
        .admit_bao(hash, block0_ranges, block0_bao)
        .await
        .unwrap();

    let status = engine.inner.store.blobs().status(hash).await.unwrap();
    assert!(
        matches!(
            status,
            iroh_blobs::api::blobs::BlobStatus::Partial { size: None }
        ),
        "front-only partial must NOT have a status()-known size yet, got {status:?}"
    );

    // Advertise side: coverage() says block 0 is covered.
    let cov = engine.coverage(hash).await.unwrap();
    assert!(cov.covers(0), "advertise: block 0 is fully present");

    // Serve side: partial_hit_size's exact logic — bitfield size, then a
    // fully-present check for a range inside block 0.
    let present = engine.present_ranges(hash).await.unwrap();
    let size = present.size();
    assert_ne!(size, 0, "serve: the bitfield must know the blob's size");
    let byte_offset = 0;
    let byte_len = decdn_protocol::DISCOVERY_BLOCK_BYTES;
    let missing = engine
        .missing_ranges(hash, byte_offset, byte_len, size)
        .await
        .unwrap();
    assert!(
        missing.is_empty(),
        "serve: block 0's byte range is fully present, so partial_hit_size must return \
         Some(size), agreeing with advertise's coverage(0)"
    );
}

/// An origin that admits the blob into the store itself (as the ported
/// `NodeOrigin` does) and returns `AlreadyAdmitted`; the engine must then
/// serve it from the store without re-ingesting.
///
/// The origin needs a handle to the same `CacheEngine` it is registered
/// on to call `admit_bao_stream`, but `CacheEngine::open` needs the
/// origin list up front — so the handle is late-bound through a
/// `OnceLock` set right after `open` returns, mirroring how the node
/// wires its own origin against the engine it is constructed for.
#[tokio::test]
async fn already_admitted_short_circuits_and_serves_from_store() -> anyhow::Result<()> {
    /// Header-less bao wire reader for [`CacheEngine::admit_bao_stream`]
    /// in [`already_admitted_short_circuits_and_serves_from_store`].
    struct AdmitReader(bytes::Bytes);
    impl iroh_io::AsyncStreamReader for AdmitReader {
        async fn read_bytes(&mut self, len: usize) -> std::io::Result<bytes::Bytes> {
            Ok(self.0.split_to(self.0.len().min(len)))
        }
        async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
            if self.0.len() < L {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "short",
                ));
            }
            let g = self.0.split_to(L);
            let mut out = [0u8; L];
            out.copy_from_slice(&g);
            Ok(out)
        }
    }

    /// An origin whose `fetch` admits the blob into its own engine (via
    /// a late-bound handle — see the test doc comment) and returns
    /// `AlreadyAdmitted`, exactly as the ported `NodeOrigin` will.
    #[derive(Debug)]
    struct AdmittingOrigin {
        engine: std::sync::Arc<std::sync::OnceLock<CacheEngine>>,
        hash: Hash,
        total: u64,
        wire: bytes::Bytes,
        ranges: bao_tree::ChunkRanges,
    }

    impl Origin for AdmittingOrigin {
        fn kind(&self) -> OriginKind {
            OriginKind::Peer
        }

        fn fetch(
            &self,
            hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, crate::OriginPullError>> + Send + '_>>
        {
            Box::pin(async move {
                if hash != self.hash {
                    return Ok(OriginFetch::NotFound);
                }
                let engine = self
                    .engine
                    .get()
                    .expect("engine set by the caller right after open");
                engine
                    .admit_bao_stream(
                        self.hash,
                        self.ranges.clone(),
                        self.total,
                        AdmitReader(self.wire.clone()),
                        None,
                    )
                    .await
                    .map_err(|(_reader, e)| {
                        crate::OriginPullError::Permanent(anyhow::Error::from(e))
                    })?;
                Ok(OriginFetch::AlreadyAdmitted)
            })
        }
    }

    let group = crate::CHUNK_GROUP_BYTES;
    let total = 5 * group + 321;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let (hash, ranges, wire) = bao_for(root, &plaintext, outboard, 0, total, total);
    assert!(
        wire.len() > 8,
        "bao_for output must carry the 8-byte header"
    );
    let wire = wire.slice(8..);

    let engine_cell = std::sync::Arc::new(std::sync::OnceLock::<CacheEngine>::new());
    let origin = std::sync::Arc::new(AdmittingOrigin {
        engine: engine_cell.clone(),
        hash,
        total,
        wire,
        ranges: ranges.clone(),
    }) as Arc<dyn Origin>;

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 16).await?;
    engine_cell
        .set(engine.clone())
        .map_err(|_| anyhow::anyhow!("engine cell already set"))?;

    // populate (CommitOnly) → blob present without re-ingest.
    engine.populate(hash).await?;
    anyhow::ensure!(
        engine.has(hash).await?,
        "blob must be present after AlreadyAdmitted populate"
    );
    // get (ReturnBytes) → bytes read back from the store equal the content.
    let got = engine.get(hash).await?;
    anyhow::ensure!(
        got.as_ref() == plaintext.as_slice(),
        "served bytes must equal the blob"
    );
    Ok(())
}

#[tokio::test]
async fn tagged_partial_survives_gc_untagged_is_reclaimed() {
    use iroh_blobs::api::blobs::BlobStatus;
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 4 * group;
    let tmp = tempfile::tempdir().unwrap();
    // Short GC interval so the store's internal run_gc loop sweeps quickly.
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![],
        16,
        PinnedHashes::empty(),
        RetryPolicy::disabled(),
        CircuitBreakerPolicy::default(),
        Some(std::sync::Arc::new(CacheMetrics::default())),
        std::time::Duration::from_millis(200),
    )
    .await
    .unwrap();

    // Tagged: normal admit_bao (protect_partial fires).
    let (root_a, pt_a, ob_a) = synth_blob(total as usize);
    let (ha, ra, ba) = bao_for(root_a, &pt_a, ob_a, group, group, total);
    engine.admit_bao(ha, ra, ba).await.unwrap();

    // Control: same shape, distinct hash, imported WITHOUT a tag.
    let (root_b, pt_b, ob_b) = synth_blob((total + group) as usize); // different len -> different root
    let (hb, rb, bb) = bao_for(root_b, &pt_b, ob_b, group, group, total + group);
    engine
        .inner
        .store
        .blobs()
        .import_bao_bytes(hb, rb, bb)
        .await
        .unwrap();

    assert!(matches!(
        engine.inner.store.blobs().status(ha).await.unwrap(),
        BlobStatus::Partial { .. }
    ));
    assert!(matches!(
        engine.inner.store.blobs().status(hb).await.unwrap(),
        BlobStatus::Partial { .. }
    ));

    // Poll for the control's reclaim rather than sleeping a fixed span:
    // fails fast once GC sweeps (typically the first 200ms interval), and
    // only fails if GC never reclaims the untagged control within a generous
    // budget — robust on slow/loaded CI and independent of the exact GC
    // interval. The control's `NotFound` gates the test, so a genuine GC
    // failure still fails loud; it can never silently pass.
    let deadline = std::time::Duration::from_secs(15);
    let poll = std::time::Duration::from_millis(50);
    let start = std::time::Instant::now();
    loop {
        let reclaimed = matches!(
            engine.inner.store.blobs().status(hb).await.unwrap(),
            BlobStatus::NotFound
        );
        if reclaimed {
            break;
        }
        assert!(
            start.elapsed() < deadline,
            "control never reclaimed within {deadline:?}: GC did not run"
        );
        tokio::time::sleep(poll).await;
    }

    // The tagged partial must STILL be present after the control was swept —
    // proving the tag (not timing) is what protected it.
    assert!(
        matches!(
            engine.inner.store.blobs().status(ha).await.unwrap(),
            BlobStatus::Partial { .. }
        ),
        "tagged partial survives GC"
    );
}

/// Drain a serve-leg export stream to completion, prefixed with its
/// already-pulled `first` item. Every remaining item MUST be `Ok`: an
/// in-flight reader started before an evict has to keep delivering correct
/// bytes even after the blob is evicted and GC-swept out of the store.
/// Consumes (and thus drops) the stream, releasing its handle so the disk
/// space can free.
async fn drain_serve_leg(
    first: Bytes,
    mut stream: Pin<Box<dyn futures_util::Stream<Item = CacheResult<Bytes>> + Send>>,
) -> Bytes {
    let mut out = bytes::BytesMut::from(&first[..]);
    while let Some(item) = stream.next().await {
        let bytes = item.expect("in-flight serve-leg reader must deliver bytes despite evict + GC");
        out.extend_from_slice(&bytes);
    }
    out.freeze()
}

/// Poll the store until `hash` reports `NotFound`, or fail after `deadline`.
/// Used to gate on a GC sweep having reclaimed a blob without pinning the
/// test to the exact 200ms interval — robust on slow/loaded CI.
async fn wait_reclaimed(engine: &CacheEngine, hash: Hash, deadline: Duration) {
    use iroh_blobs::api::blobs::BlobStatus;
    let start = std::time::Instant::now();
    loop {
        if matches!(
            engine.inner.store.blobs().status(hash).await.unwrap(),
            BlobStatus::NotFound
        ) {
            return;
        }
        assert!(
            start.elapsed() < deadline,
            "blob {hash} never reclaimed within {deadline:?}: GC did not run"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Multi-observer coalescing (#1656) composes with operator eviction
/// (#279). A coalesced serve-miss fans one upstream pull out to N serve legs;
/// each serve leg reads the filling partial through its own in-flight
/// `export_bao_range_stream` handle. This test reduces that to the cache-level
/// invariant the node layer relies on: **two in-flight readers over one
/// partial, evicted mid-serve, both still finish delivering byte-for-byte
/// correct bytes even after the GC sweep has logically removed the blob.**
///
/// The load-bearing assumption: reader-pinning is
/// UNCHANGED by coalescing — N serve-leg readers survive an evict + GC sweep
/// exactly as one reader would, because each holds its own live export handle.
/// Eviction is a *logical* takedown: it drops the partial's protecting tag and blocks
/// NEW serves (`has` reports absent), but it does not tear down readers already
/// in flight.
///
/// One subtlety this pins precisely: the store flips the blob to `NotFound`
/// the instant the GC sweep deletes it — the logical delete does NOT wait for
/// the last reader. The in-flight readers still complete correctly because the
/// bytes they need stay reachable through their open handles until they drop
/// (the disk space is what frees only after the last handle closes).
#[tokio::test]
async fn evict_mid_serve_lets_coalesced_readers_finish_then_reclaims() {
    let group = crate::CHUNK_GROUP_BYTES;
    let total = 8 * group;
    let tmp = tempfile::tempdir().unwrap();
    // Short GC interval so the store's internal run_gc loop sweeps within the
    // test window (same knob as `tagged_partial_survives_gc_...`).
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![],
        16,
        PinnedHashes::empty(),
        RetryPolicy::disabled(),
        CircuitBreakerPolicy::default(),
        Some(Arc::new(CacheMetrics::default())),
        Duration::from_millis(200),
    )
    .await
    .unwrap();

    // The coalesced-fill target: a genuine partial (groups [0, 6g) of an 8g
    // blob), tagged by `admit_bao` exactly as the pull leg tags it.
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let (hash, ranges, bao) = bao_for(root, &plaintext, outboard, 0, 6 * group, total);
    engine.admit_bao(hash, ranges, bao).await.unwrap();
    assert!(
        !engine.present_ranges(hash).await.unwrap().is_complete(),
        "the fill target is a genuine partial"
    );
    assert_eq!(
        count_tags_for(&engine, hash).await,
        1,
        "the partial carries its protecting tag"
    );

    // The exact wire each serve leg must deliver, captured before the evict.
    let expected = engine
        .export_bao_range(hash, 0, 6 * group, total)
        .await
        .unwrap();

    // Two coalesced serve legs: each opens an in-flight verified-range stream
    // and pulls its first frame, so both hold a live export handle when the
    // evict lands.
    let mut leg_a = engine
        .export_bao_range_stream(hash, 0, 6 * group, total)
        .await
        .unwrap();
    let mut leg_b = engine
        .export_bao_range_stream(hash, 0, 6 * group, total)
        .await
        .unwrap();
    let first_a = leg_a.next().await.expect("leg A first frame").unwrap();
    let first_b = leg_b.next().await.expect("leg B first frame").unwrap();

    // Evict mid-serve. Logical takedown: tag dropped, new serves blocked.
    engine.evict(hash).await.unwrap();
    assert!(engine.is_evicted(hash), "evict flag set");
    assert_eq!(
        count_tags_for(&engine, hash).await,
        0,
        "evict drops the protecting tag"
    );
    assert!(
        !engine.has(hash).await.unwrap(),
        "a NEW serve is blocked immediately after evict"
    );

    // Wait — while BOTH serve legs are still held — for the target itself to
    // report `NotFound`. This is the documented subtlety made an assertion:
    // the GC sweep deletes the untagged blob and flips its logical status the
    // instant it runs, without waiting for the in-flight readers. Gating on
    // the target (not a proxy) both proves a sweep ran after the tag drop and
    // pins that the delete does not defer to the last reader.
    wait_reclaimed(&engine, hash, Duration::from_secs(15)).await;

    // The heart of the composition: BOTH in-flight readers, started before the
    // evict, still drain to completion with byte-for-byte identical bytes even
    // though the blob was already logically removed by the sweep above. Each
    // reader's open export handle keeps its bytes reachable — reader-pinning
    // held for two readers exactly as it would for one.
    let served_a = drain_serve_leg(first_a, leg_a).await;
    let served_b = drain_serve_leg(first_b, leg_b).await;
    assert_eq!(
        served_a, expected,
        "serve leg A delivered the full range intact"
    );
    assert_eq!(
        served_b, expected,
        "serve leg B delivered the full range intact"
    );
}
