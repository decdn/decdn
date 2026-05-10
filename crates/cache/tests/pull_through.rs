//! Integration tests for [`decdn_cache::CacheEngine`] end-to-end with a
//! mocked HTTP origin.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use decdn_cache::{
    CacheEngine, CacheError, CacheMetrics, DecompressMode, FilesystemOrigin, Hash, HttpOrigin,
    Origin, OriginError, OriginFetch, OriginKind, OriginPullError, PinnedHashes, RetryPolicy,
    SupportedEncoding,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Spin up a wiremock server that serves a single blob at
/// `/{blake3_hex}`. Returns the `(server, hash)` pair.
async fn serve_blob(payload: &'static [u8]) -> (MockServer, Hash) {
    let server = MockServer::start().await;
    let hash = Hash::new(payload);
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
        .mount(&server)
        .await;
    (server, hash)
}

async fn build_engine(origin_url: &str) -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(origin_url)?);
    let engine = CacheEngine::open(tmp.path(), Some(origin), 16).await?;
    Ok((engine, tmp))
}

/// Same as [`build_engine`] but with `RetryPolicy::disabled()`. Used by
/// status-mapping tests where the unit under test is the *first*-attempt
/// classification — without disabling retry, those tests would burn ~6
/// seconds of wall time exercising the default retry chain on every
/// transient-classified status (5xx/408/429), and a flake on the third
/// retry would surface as a status-mapping failure rather than a
/// retry-loop failure.
async fn build_engine_no_retry(
    origin_url: &str,
) -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(origin_url)?);
    let engine = CacheEngine::open_full(
        tmp.path(),
        Some(origin),
        16,
        decdn_cache::PinnedHashes::empty(),
        RetryPolicy::disabled(),
        None,
        Duration::ZERO,
    )
    .await?;
    Ok((engine, tmp))
}

fn err_of<T: std::fmt::Debug, E>(r: Result<T, E>) -> anyhow::Result<E> {
    r.err()
        .ok_or_else(|| anyhow::anyhow!("expected error, got Ok"))
}

#[tokio::test]
async fn cache_miss_pulls_from_origin_and_caches() -> anyhow::Result<()> {
    let payload: &[u8] = b"hello, decdn";
    let (server, hash) = serve_blob(payload).await;
    let (engine, _tmp) = build_engine(&server.uri()).await?;

    anyhow::ensure!(!engine.has(hash).await?, "blob should be absent initially");

    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload, "first get should return origin bytes");
    anyhow::ensure!(engine.has(hash).await?, "blob should be cached after miss");

    // Second get is served locally — drop the mock server to prove it.
    drop(server);
    let got2 = engine.get(hash).await?;
    anyhow::ensure!(&got2[..] == payload, "second get should be served locally");

    Ok(())
}

#[tokio::test]
async fn hash_mismatch_is_rejected_and_not_cached() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let expected = Hash::new(b"expected");
    let mismatched_payload: &[u8] = b"something else entirely";
    let actual = Hash::new(mismatched_payload);
    Mock::given(method("GET"))
        .and(path(format!("/{}", expected.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(mismatched_payload))
        .mount(&server)
        .await;

    let (engine, _tmp) = build_engine(&server.uri()).await?;
    let err = err_of(engine.get(expected).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::HashMismatch { .. }),
        "expected HashMismatch, got: {err:?}"
    );
    anyhow::ensure!(
        !engine.has(expected).await?,
        "expected hash must not be cached"
    );
    // Cache-poisoning mitigation: the wrong-hash bytes briefly land
    // in iroh-blobs under `actual` (their own BLAKE3) when
    // `add_stream` commits before we hash-check. The engine logically
    // evicts `actual` on mismatch so neither `has(actual)` nor
    // `get(actual)` reaches the partial bytes — the fix for the
    // attack window the streaming refactor introduced.
    anyhow::ensure!(
        !engine.has(actual).await?,
        "actual-hash bytes must be logically evicted (cache-poisoning mitigation)"
    );
    Ok(())
}

/// Periodic iroh-blobs GC must reclaim the partial-import bytes left
/// behind by a hash-mismatch pull-through (#518). Threat model: a
/// hostile origin streams `max_blob_size_mb - 1` of garbage and errors
/// on the last byte; the engine logically evicts the wrong-hash blob
/// (`actual`) so `engine.has(actual)` returns false, but the bytes
/// stay on disk under iroh-blobs' tag-less commit until the GC sweep
/// fires. This test wires a 200ms GC interval and asserts that within
/// a small handful of cycles the bytes really do leave disk and the
/// `gc_*` metrics record the reclaim.
#[tokio::test]
async fn gc_reclaims_partial_import_bytes() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let expected = Hash::new(b"expected");
    let mismatched_payload: &[u8] = b"a body the origin pretends matches the requested hash";
    let actual = Hash::new(mismatched_payload);
    Mock::given(method("GET"))
        .and(path(format!("/{}", expected.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(mismatched_payload))
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let metrics = Arc::new(CacheMetrics::default());
    // Tight interval so the test runs in well under a second on a
    // loaded CI machine. Two cycles are needed to observe a nonzero
    // `gc_bytes_reclaimed_total`: cycle 1 snapshots the pre-sweep set
    // (no prior baseline → reclaim = 0), cycle 2 sees the disappeared
    // hash and attributes its bytes to the previous sweep.
    let gc_interval = Duration::from_millis(200);
    let engine = CacheEngine::open_full(
        tmp.path(),
        Some(origin),
        16,
        PinnedHashes::empty(),
        RetryPolicy::disabled(),
        Some(Arc::clone(&metrics)),
        gc_interval,
    )
    .await?;

    let err = err_of(engine.get(expected).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::HashMismatch { .. }),
        "expected HashMismatch, got: {err:?}"
    );

    // Pre-GC: iroh-blobs still holds the mismatched bytes under
    // `actual`. `inspect` reads `BlobStatus` directly and ignores the
    // engine's logical-evict log, so a `Some(_)` size here proves the
    // disk-leak existed before GC ran.
    let pre = engine.inspect(actual).await?;
    anyhow::ensure!(
        pre.size_bytes.is_some(),
        "actual-hash bytes must be on disk before GC runs (got size_bytes = None)"
    );
    anyhow::ensure!(
        pre.already_evicted,
        "engine should have logically-evicted actual on hash mismatch"
    );

    // Wait for enough sweep cycles to (a) run the sweep that deletes
    // the bytes and (b) run the next sweep whose pre-sweep snapshot
    // observes the disappearance and bumps `gc_bytes_reclaimed_total`.
    // Sixteen cycles' worth of slack (3.2s wallclock total at 200ms
    // interval) absorbs scheduling jitter on heavily-loaded CI hosts
    // without making a green path slow.
    let deadline = std::time::Instant::now() + gc_interval * 16;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(gc_interval).await;
        let post = engine.inspect(actual).await?;
        if post.size_bytes.is_none() && metrics.gc_bytes_reclaimed.get() > 0 {
            break;
        }
    }

    let post = engine.inspect(actual).await?;
    anyhow::ensure!(
        post.size_bytes.is_none(),
        "actual-hash bytes must be reclaimed by iroh-blobs GC; got size_bytes = {:?}",
        post.size_bytes
    );
    anyhow::ensure!(
        metrics.gc_runs.get() >= 1,
        "gc_runs_total should be >=1 after the periodic loop has fired; got {}",
        metrics.gc_runs.get()
    );
    anyhow::ensure!(
        metrics.gc_bytes_reclaimed.get() >= mismatched_payload.len() as u64,
        "gc_bytes_reclaimed_total should cover at least the mismatched payload \
         ({} bytes); got {}",
        mismatched_payload.len(),
        metrics.gc_bytes_reclaimed.get()
    );

    Ok(())
}

/// Locks in the **lagged-by-one-cycle attribution** documented in the
/// `gc_bytes_reclaimed_total` metric: the first sweep records a baseline
/// and reports zero reclaim, the second sweep attributes the previous
/// sweep's deletes. A regression that snapshotted post-sweep instead of
/// pre-sweep (or double-counted the first cycle) would inflate
/// `gc_bytes_reclaimed_total` by the entire blob set on every restart —
/// the exact alert signal `gc_bytes_reclaimed_total` exists for —
/// and the broader-shaped `gc_reclaims_partial_import_bytes` test would
/// still pass because it only asserts `>= mismatched_payload.len()`.
#[tokio::test]
async fn gc_attribution_lags_one_cycle() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let expected = Hash::new(b"expected-lag");
    let mismatched_payload: &[u8] = b"a body whose blake3 disagrees with the requested hash";
    Mock::given(method("GET"))
        .and(path(format!("/{}", expected.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(mismatched_payload))
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let metrics = Arc::new(CacheMetrics::default());
    // Generous interval so the polling loop has wide margins between
    // cycle 1 and cycle 2 — we need to read the metric state after
    // cycle 1 fires but before cycle 2 fires.
    let gc_interval = Duration::from_millis(500);
    let engine = CacheEngine::open_full(
        tmp.path(),
        Some(origin),
        16,
        PinnedHashes::empty(),
        RetryPolicy::disabled(),
        Some(Arc::clone(&metrics)),
        gc_interval,
    )
    .await?;

    let err = err_of(engine.get(expected).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::HashMismatch { .. }),
        "expected HashMismatch, got: {err:?}"
    );

    // Poll for cycle 1 to fire. The cb bumps gc_runs_total once per
    // sweep, so the transition 0 -> 1 marks cycle 1 completion.
    let cycle1_deadline = std::time::Instant::now() + gc_interval * 4;
    while metrics.gc_runs.get() < 1 {
        anyhow::ensure!(
            std::time::Instant::now() < cycle1_deadline,
            "cycle 1 did not fire within {}ms",
            (gc_interval * 4).as_millis()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Cycle 1 fired. Both counter bumps happen in the same cb body, so
    // observing `gc_runs_total >= 1` means `gc_bytes_reclaimed_total`
    // has already taken its cycle-1 contribution (which must be 0 —
    // no prior baseline to diff against).
    anyhow::ensure!(
        metrics.gc_bytes_reclaimed.get() == 0,
        "after cycle 1, gc_bytes_reclaimed_total must be 0 (no prior baseline to diff against); got {}",
        metrics.gc_bytes_reclaimed.get()
    );

    // Poll for cycle 2.
    let cycle2_deadline = std::time::Instant::now() + gc_interval * 4;
    while metrics.gc_runs.get() < 2 {
        anyhow::ensure!(
            std::time::Instant::now() < cycle2_deadline,
            "cycle 2 did not fire within {}ms after cycle 1",
            (gc_interval * 4).as_millis()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    anyhow::ensure!(
        metrics.gc_bytes_reclaimed.get() > 0,
        "cycle 2 must attribute cycle-1's reclaim; got 0 bytes after 2 cycles"
    );

    Ok(())
}

/// The other limb of #518's threat model: the engine must reclaim
/// partial-import bytes from a *cap-breach* (mid-stream `BlobTooLarge`),
/// not just hash mismatch. The cap-breach path drops the (possibly
/// successful) temp tag in `engine.rs::pull_through` after the
/// `count_and_cap_stream` adapter writes a `BlobTooLargeMarker` into
/// the captured-error side-channel. Without GC, those bytes leak.
/// This locks in the metric path that an operator alert ("hostile
/// origin amplifying disk via repeated mid-stream errors", per the
/// `gc_bytes_reclaimed_total` docstring) actually depends on.
#[tokio::test]
async fn gc_reclaims_cap_breach_partial_bytes() -> anyhow::Result<()> {
    // 2 MiB chunked body, 1 MiB cap, no Content-Length → only the
    // streaming running-total check catches it. Same setup as
    // `mid_stream_overrun_is_rejected_by_http_origin` plus GC.
    let payload_bytes = 2 * 1024 * 1024;
    let addr = spawn_chunked_oversize_server(payload_bytes).await?;
    let origin_url = format!("http://{addr}/");

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&origin_url)?);
    let metrics = Arc::new(CacheMetrics::default());
    let gc_interval = Duration::from_millis(200);
    let engine = CacheEngine::open_full(
        tmp.path(),
        Some(origin),
        1, // max_blob_size_mb = 1 MiB; payload is 2 MiB
        PinnedHashes::empty(),
        RetryPolicy::disabled(),
        Some(Arc::clone(&metrics)),
        gc_interval,
    )
    .await?;

    let err = err_of(engine.get(Hash::new(b"doesn't matter")).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::BlobTooLarge { .. }),
        "expected BlobTooLarge from mid-stream cap, got: {err:?}"
    );

    // Wait the same way `gc_reclaims_partial_import_bytes` does:
    // cycle 1 establishes baseline, cycle 2 observes the reclaim and
    // attributes the bytes. Sixteen cycles' headroom for slow CI.
    let deadline = std::time::Instant::now() + gc_interval * 16;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(gc_interval).await;
        if metrics.gc_bytes_reclaimed.get() > 0 {
            break;
        }
    }

    anyhow::ensure!(
        metrics.gc_runs.get() >= 1,
        "gc_runs_total should be >=1 after the periodic loop has fired; got {}",
        metrics.gc_runs.get()
    );
    // We don't assert an exact byte count — `count_and_cap_stream`
    // can terminate the upstream slightly past `max_blob_bytes`
    // depending on chunk boundaries, and add_stream may have
    // committed a different fraction. The load-bearing claim is
    // that the metric is *nonzero*: GC is reclaiming partial-import
    // bytes from this code path.
    anyhow::ensure!(
        metrics.gc_bytes_reclaimed.get() > 0,
        "gc_bytes_reclaimed_total should be nonzero after cap-breach + GC; got {}",
        metrics.gc_bytes_reclaimed.get()
    );

    Ok(())
}

/// Sister to `gc_reclaims_partial_import_bytes` for the disabled
/// branch: when `gc_interval = Duration::ZERO`, iroh-blobs must NOT
/// spawn a GC loop, the partial-import bytes must stay on disk, and
/// the GC metrics must stay at zero indefinitely. Locks in the
/// `if !gc_interval.is_zero()` guard at `engine.rs::open_full` —
/// a regression that swapped the polarity of that condition would
/// silently re-enable GC for every operator who set `gc_interval_sec
/// = 0` (or vice versa, silently disabling for everyone else).
#[tokio::test]
async fn gc_disabled_does_not_reclaim_or_emit_metrics() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let expected = Hash::new(b"expected-disabled");
    let mismatched_payload: &[u8] = b"a body with a different blake3 than the requested hash";
    let actual = Hash::new(mismatched_payload);
    Mock::given(method("GET"))
        .and(path(format!("/{}", expected.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(mismatched_payload))
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let metrics = Arc::new(CacheMetrics::default());
    let engine = CacheEngine::open_full(
        tmp.path(),
        Some(origin),
        16,
        PinnedHashes::empty(),
        RetryPolicy::disabled(),
        Some(Arc::clone(&metrics)),
        Duration::ZERO, // GC disabled
    )
    .await?;

    let err = err_of(engine.get(expected).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::HashMismatch { .. }),
        "expected HashMismatch, got: {err:?}"
    );

    // Sleep long enough that a 200ms-interval GC loop would have fired
    // multiple times if it had been spawned. With GC disabled, nothing
    // should change.
    tokio::time::sleep(Duration::from_millis(800)).await;

    let post = engine.inspect(actual).await?;
    anyhow::ensure!(
        post.size_bytes.is_some(),
        "actual-hash bytes must remain on disk when GC is disabled; got size_bytes = None"
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
async fn origin_not_found_surfaces_not_found() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"absent");
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let (engine, _tmp) = build_engine(&server.uri()).await?;
    let err = err_of(engine.get(hash).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::NotFound { .. }),
        "expected NotFound, got: {err:?}"
    );
    Ok(())
}

#[tokio::test]
async fn blob_too_large_is_rejected_via_http_origin() -> anyhow::Result<()> {
    // Payload of 2 MB, cap at 1 MB. Wiremock sets an honest `Content-Length`,
    // so this trips the HTTP origin's fast-path rejection before any bytes
    // are buffered — which surfaces as `OriginError`.
    let payload = vec![0xABu8; 2 * 1024 * 1024];
    let hash = Hash::new(&payload);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let engine = CacheEngine::open(tmp.path(), Some(origin), 1).await?;

    let err = err_of(engine.get(hash).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError (fast-path content-length rejection), got: {err:?}"
    );
    Ok(())
}

/// Origin that returns bytes exceeding `max_bytes` regardless of its
/// argument — a stand-in for a misbehaving backend that bypasses the HTTP
/// origin's streaming cap (e.g. a future S3 / filesystem impl that forgets
/// to honor the size argument).
#[derive(Debug)]
struct OversizedOrigin {
    payload: bytes::Bytes,
}

impl Origin for OversizedOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, decdn_cache::OriginPullError>> + Send + '_>>
    {
        let payload = self.payload.clone();
        Box::pin(async move { Ok(OriginFetch::found_one_shot(payload)) })
    }
}

#[tokio::test]
async fn engine_rejects_oversize_bytes_from_misbehaving_origin() -> anyhow::Result<()> {
    // Origin ignores the max_bytes advisory and returns 2 MiB. Cap at 1 MiB.
    // This proves the engine's post-receive size check is load-bearing
    // defense in depth — a regression dropping that check would be caught
    // by this test even if the HTTP origin's streaming cap is fine.
    let payload = bytes::Bytes::from(vec![0xCDu8; 2 * 1024 * 1024]);
    let hash = Hash::new(&payload);
    let origin = Arc::new(OversizedOrigin { payload });

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Some(origin), 1).await?;

    let err = err_of(engine.get(hash).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::BlobTooLarge { .. }),
        "expected BlobTooLarge from engine-level check, got: {err:?}"
    );
    anyhow::ensure!(
        !engine.has(hash).await?,
        "oversize bytes must not be cached"
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
    let (server, hash) = serve_blob(payload).await;

    let tmp = tempfile::tempdir()?;
    {
        let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
        let engine = CacheEngine::open(tmp.path(), Some(origin), 16).await?;
        let _ = engine.get(hash).await?;
        engine.shutdown().await?;
        // Skip Drop so this test fails if shutdown() stopped flushing.
        std::mem::forget(engine);
    }
    drop(server);

    let engine = CacheEngine::open(tmp.path(), None, 16).await?;
    anyhow::ensure!(
        engine.has(hash).await?,
        "reopened engine should see the blob flushed by shutdown()"
    );
    let got = engine.get(hash).await?;
    anyhow::ensure!(
        &got[..] == payload,
        "reopened engine should serve the cached payload without an origin"
    );
    Ok(())
}

/// Bind an ephemeral TCP port and serve exactly one request with a
/// chunked-encoded body whose total size is `payload_bytes`. No
/// `Content-Length` is sent, so reqwest can't trip the HTTP origin's
/// fast-path rejection — the mid-stream running-total check is the only
/// line of defense.
async fn spawn_chunked_oversize_server(
    payload_bytes: usize,
) -> anyhow::Result<std::net::SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        // Drain request headers — we only need to know when the client is
        // done speaking before we start responding.
        let mut buf = [0u8; 4096];
        loop {
            match sock.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    let seen = buf.get(..n).unwrap_or(&[]);
                    if seen.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
            }
        }
        let header = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/octet-stream\r\n\r\n";
        if sock.write_all(header).await.is_err() {
            return;
        }
        let chunk_size = 64 * 1024;
        let pad = vec![0xEEu8; chunk_size];
        let mut remaining = payload_bytes;
        while remaining > 0 {
            let send = remaining.min(chunk_size);
            let hdr = format!("{send:x}\r\n");
            if sock.write_all(hdr.as_bytes()).await.is_err() {
                return;
            }
            let chunk = pad.get(..send).unwrap_or(&[]);
            if sock.write_all(chunk).await.is_err() {
                return;
            }
            if sock.write_all(b"\r\n").await.is_err() {
                return;
            }
            remaining -= send;
        }
        let _ = sock.write_all(b"0\r\n\r\n").await;
    });
    Ok(addr)
}

/// Bind an ephemeral TCP port, accept one connection, then hold the socket
/// open without ever writing response bytes. Exercises the response-headers
/// timeout: the TCP handshake completes (so `connect_timeout` doesn't fire)
/// but the server never writes a status line.
async fn spawn_silent_server() -> anyhow::Result<std::net::SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Ok((sock, _)) = listener.accept().await {
            let _sock = sock;
            std::future::pending::<()>().await;
        }
    });
    Ok(addr)
}

/// Bind an ephemeral TCP port, serve headers + a single short chunk, then
/// hang. Exercises the per-chunk idle timeout: the first chunk arrives
/// quickly, subsequent `.chunk()` calls block forever.
async fn spawn_stall_after_partial_body_server() -> anyhow::Result<std::net::SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = [0u8; 4096];
        loop {
            match sock.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    let seen = buf.get(..n).unwrap_or(&[]);
                    if seen.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
            }
        }
        let resp = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nABCD\r\n";
        let _ = sock.write_all(resp).await;
        // Hang — subsequent chunk reads must hit the idle timeout.
        let _sock = sock;
        std::future::pending::<()>().await;
    });
    Ok(addr)
}

#[tokio::test]
async fn response_headers_timeout_fires_on_silent_server() -> anyhow::Result<()> {
    // Use the disabled retry policy: the silent server only accepts one
    // TCP connection, so retrying would queue connect attempts that
    // wait out the 200ms headers timeout each time — irrelevant noise
    // for a test that only cares about the *first* timeout firing.
    let addr = spawn_silent_server().await?;
    let origin = HttpOrigin::parse(&format!("http://{addr}/"))?
        .with_timeouts(Duration::from_millis(200), Duration::from_secs(30));

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open_full(
        tmp.path(),
        Some(Arc::new(origin)),
        16,
        PinnedHashes::empty(),
        RetryPolicy::disabled(),
        None,
        Duration::ZERO,
    )
    .await?;

    let err = err_of(engine.get(Hash::new(b"anything")).await)?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError from headers timeout, got: {err:?}"
    );
    anyhow::ensure!(
        msg.contains("headers timed out"),
        "error message missing headers-timeout marker: {msg}"
    );
    Ok(())
}

#[tokio::test]
async fn chunk_idle_timeout_fires_when_origin_stalls_mid_body() -> anyhow::Result<()> {
    // Disabled retry policy for the same reason as the headers-timeout
    // test: the stall server only accepts one connection. A retry
    // would block on a 30-second headers timeout per attempt, turning
    // a sub-second test into a multi-minute one.
    let addr = spawn_stall_after_partial_body_server().await?;
    let origin = HttpOrigin::parse(&format!("http://{addr}/"))?
        .with_timeouts(Duration::from_secs(30), Duration::from_millis(200));

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open_full(
        tmp.path(),
        Some(Arc::new(origin)),
        16,
        PinnedHashes::empty(),
        RetryPolicy::disabled(),
        None,
        Duration::ZERO,
    )
    .await?;

    let err = err_of(engine.get(Hash::new(b"anything")).await)?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError from idle timeout, got: {err:?}"
    );
    anyhow::ensure!(
        msg.contains("body read stalled"),
        "error message missing stalled marker: {msg}"
    );
    // Sanity: 4 bytes of "ABCD" should be reported as buffered.
    anyhow::ensure!(
        msg.contains("4 bytes buffered"),
        "error message missing progress info: {msg}"
    );
    Ok(())
}

#[tokio::test]
async fn pull_through_succeeds_above_one_mib_payload() -> anyhow::Result<()> {
    // 2 MiB payload exercises the streaming pull-through across multiple
    // origin chunks — `tokio_util::io::ReaderStream` emits 4 KiB-sized
    // chunks by default, so 2 MiB → ~512 chunks through `add_stream`'s
    // bidi protocol. Used to exercise the explicit `spawn_blocking`
    // BLAKE3 path before #271; that double-hash is now handled inside
    // iroh-blobs' `add_stream` so this test is now a regression check
    // that multi-chunk streaming completes through the engine.
    let payload = vec![0x7Fu8; 2 * 1024 * 1024];
    let hash = Hash::new(&payload);

    let origin_dir = tempfile::tempdir()?;
    seed_fs_blob(origin_dir.path(), hash, &payload)?;

    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(FilesystemOrigin::new(origin_dir.path()).await?);
    let engine = CacheEngine::open(cache_dir.path(), Some(origin), 16).await?;

    let got = engine.get(hash).await?;
    anyhow::ensure!(got.len() == payload.len(), "size mismatch: {}", got.len());
    anyhow::ensure!(got[..] == payload[..], "content mismatch");
    anyhow::ensure!(engine.has(hash).await?, "blob should be cached");
    Ok(())
}

#[tokio::test]
async fn mid_stream_overrun_is_rejected_by_http_origin() -> anyhow::Result<()> {
    // 2 MiB chunked body, 1 MiB cap, no Content-Length → only the
    // streaming running-total check can catch this.
    let payload_bytes = 2 * 1024 * 1024;
    let addr = spawn_chunked_oversize_server(payload_bytes).await?;
    let origin_url = format!("http://{addr}/");

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&origin_url)?);
    let engine = CacheEngine::open(tmp.path(), Some(origin), 1).await?;

    // Any hash works — the raw TCP server doesn't match paths.
    let err = err_of(engine.get(Hash::new(b"doesn't matter")).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::BlobTooLarge { .. }),
        "expected BlobTooLarge from mid-stream cap, got: {err:?}"
    );
    Ok(())
}

#[tokio::test]
async fn open_on_file_path_yields_store_error() -> anyhow::Result<()> {
    // `CacheEngine::open` calls `create_dir_all`; pointing at an existing
    // regular file means that fails, which must surface as
    // `CacheError::Store` (the only variant reserved for store I/O issues).
    let tmp = tempfile::NamedTempFile::new()?;
    let err = CacheEngine::open(tmp.path(), None, 16)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected open to fail on a file path"))?;
    anyhow::ensure!(
        matches!(err, CacheError::Store(_)),
        "expected Store variant, got: {err:?}"
    );
    Ok(())
}

/// Write `payload` into the sharded filesystem layout rooted at `base`
/// so `FilesystemOrigin` can serve it.
fn seed_fs_blob(base: &std::path::Path, hash: Hash, payload: &[u8]) -> anyhow::Result<()> {
    let hex = hash.to_hex();
    let shard = hex
        .get(..2)
        .ok_or_else(|| anyhow::anyhow!("hex too short"))?;
    let dir = base.join(shard);
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join(hex.as_str()), payload)?;
    Ok(())
}

#[tokio::test]
async fn fs_origin_pulls_and_caches() -> anyhow::Result<()> {
    let payload: &[u8] = b"content-addressed from local disk";
    let hash = Hash::new(payload);

    let origin_dir = tempfile::tempdir()?;
    seed_fs_blob(origin_dir.path(), hash, payload)?;

    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(FilesystemOrigin::new(origin_dir.path()).await?);
    let engine = CacheEngine::open(cache_dir.path(), Some(origin), 16).await?;

    anyhow::ensure!(!engine.has(hash).await?, "blob should be absent initially");
    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload, "first get should return fs bytes");
    anyhow::ensure!(engine.has(hash).await?, "blob should be cached after miss");

    // Wipe the origin dir to prove the second read is purely local.
    drop(origin_dir);
    let got2 = engine.get(hash).await?;
    anyhow::ensure!(&got2[..] == payload, "second get should be served locally");
    Ok(())
}

#[tokio::test]
async fn fs_origin_reports_not_found() -> anyhow::Result<()> {
    let origin_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(FilesystemOrigin::new(origin_dir.path()).await?);
    let engine = CacheEngine::open(cache_dir.path(), Some(origin), 16).await?;

    let err = err_of(engine.get(Hash::new(b"absent")).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::NotFound { .. }),
        "expected NotFound, got: {err:?}"
    );
    Ok(())
}

#[tokio::test]
async fn fs_origin_rejects_oversize() -> anyhow::Result<()> {
    // 2 MiB payload seeded on disk; 1 MiB cap.
    let payload = vec![0x42u8; 2 * 1024 * 1024];
    let hash = Hash::new(&payload);

    let origin_dir = tempfile::tempdir()?;
    seed_fs_blob(origin_dir.path(), hash, &payload)?;

    let cache_dir = tempfile::tempdir()?;
    let origin = Arc::new(FilesystemOrigin::new(origin_dir.path()).await?);
    let engine = CacheEngine::open(cache_dir.path(), Some(origin), 1).await?;

    let err = err_of(engine.get(hash).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError from fs metadata-length fast-path, got: {err:?}"
    );
    Ok(())
}

#[tokio::test]
async fn miss_without_origin_returns_no_origin() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), None, 16).await?;
    let err = err_of(engine.get(Hash::new(b"whatever")).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::NoOrigin { .. }),
        "expected NoOrigin, got: {err:?}"
    );
    Ok(())
}

// ----- Decompression (#312) -----
//
// `HttpOrigin` must transparently decompress `Content-Encoding: gzip` and
// `Content-Encoding: zstd` responses before the engine's BLAKE3 verify
// runs. The content-address is computed over the canonical (decompressed)
// form, so a raw-bytes pass-through would fail every verify.

use std::io::Write;

fn gzip(payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    e.write_all(payload)?;
    Ok(e.finish()?)
}

fn zstd_compress(payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    Ok(zstd::stream::encode_all(payload, 1)?)
}

#[tokio::test]
async fn http_origin_decompresses_gzip_response() -> anyhow::Result<()> {
    let payload: &[u8] = b"hello, gzipped world! repeat repeat repeat repeat";
    let hash = Hash::new(payload);
    let server = MockServer::start().await;
    let compressed = gzip(payload)?;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Encoding", "gzip")
                .set_body_bytes(compressed),
        )
        .mount(&server)
        .await;

    let (engine, _tmp) = build_engine(&server.uri()).await?;
    let got = engine.get(hash).await?;
    anyhow::ensure!(
        &got[..] == payload,
        "decompressed bytes should match canonical payload"
    );
    Ok(())
}

#[tokio::test]
async fn http_origin_decompresses_zstd_response() -> anyhow::Result<()> {
    let payload: &[u8] = b"hello, zstd! and a longer body to compress meaningfully xxxxx";
    let hash = Hash::new(payload);
    let server = MockServer::start().await;
    let compressed = zstd_compress(payload)?;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Encoding", "zstd")
                .set_body_bytes(compressed),
        )
        .mount(&server)
        .await;

    let (engine, _tmp) = build_engine(&server.uri()).await?;
    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload, "decompressed bytes should match");
    Ok(())
}

#[tokio::test]
async fn http_origin_passes_through_identity_encoding() -> anyhow::Result<()> {
    let payload: &[u8] = b"plain identity payload";
    let hash = Hash::new(payload);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Encoding", "identity")
                .set_body_bytes(payload),
        )
        .mount(&server)
        .await;

    let (engine, _tmp) = build_engine(&server.uri()).await?;
    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload);
    Ok(())
}

#[tokio::test]
async fn http_origin_rejects_unknown_encoding() -> anyhow::Result<()> {
    let payload: &[u8] = b"who knows what encoding";
    let hash = Hash::new(payload);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Encoding", "br") // Brotli — not supported.
                .set_body_bytes(payload),
        )
        .mount(&server)
        .await;

    let (engine, _tmp) = build_engine(&server.uri()).await?;
    let err = err_of(engine.get(hash).await)?;
    // Surfaced as an OriginError wrapping the UnsupportedEncoding source.
    let formatted = format!("{err:?}");
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError, got: {formatted}"
    );
    anyhow::ensure!(
        formatted.contains("Content-Encoding") || formatted.contains("br"),
        "error should mention the unsupported encoding: {formatted}"
    );
    Ok(())
}

#[tokio::test]
async fn http_origin_decompress_off_rejects_compressed_response() -> anyhow::Result<()> {
    // With `decompress=false`, an origin that still returns
    // `Content-Encoding: gzip` is a configuration mistake. We refuse
    // up-front rather than passing the raw bytes through and letting
    // the engine surface a confusing `HashMismatch` — operators get a
    // precise "your origin is using an encoding I'm not handling"
    // message they can act on.
    let payload: &[u8] = b"canonical payload";
    let hash = Hash::new(payload);
    let server = MockServer::start().await;
    let compressed = gzip(payload)?;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Encoding", "gzip")
                .set_body_bytes(compressed),
        )
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(
        HttpOrigin::parse(&server.uri())?.with_decompress_mode(decdn_cache::DecompressMode::Strict),
    );
    let engine = CacheEngine::open(tmp.path(), Some(origin), 16).await?;
    let err = err_of(engine.get(hash).await)?;
    let formatted = format!("{err:?}");
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError when decompression is off and encoding is set, got: {formatted}"
    );
    anyhow::ensure!(
        formatted.contains("unsupported Content-Encoding") || formatted.contains("gzip"),
        "error should name the unsupported encoding: {formatted}"
    );
    Ok(())
}

#[tokio::test]
async fn http_origin_decompress_off_passes_through_identity() -> anyhow::Result<()> {
    // The opt-out path is still useful for origins that legitimately
    // serve raw bytes (no Content-Encoding, or `identity`). The hash
    // must match because we never touched the bytes.
    let payload: &[u8] = b"canonical payload";
    let hash = Hash::new(payload);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir()?;
    let origin =
        Arc::new(HttpOrigin::parse(&server.uri())?.with_decompress_mode(DecompressMode::Strict));
    let engine = CacheEngine::open(tmp.path(), Some(origin), 16).await?;
    let bytes = engine.get(hash).await?;
    anyhow::ensure!(bytes.as_ref() == payload, "unexpected payload");
    Ok(())
}

// ----- Decompression: typed-error access, bombs, edge cases (#312) -----
//
// The variants on `OriginError` (UnsupportedEncoding, DecompressionFailed,
// MalformedEncoding) survive the `anyhow::Error → CacheError::OriginError`
// boundary via `CacheError::origin_error_kind`, which walks the source
// chain. Asserting on the typed variant locks in the contract — a future
// refactor that wraps a context layer above the typed error in a way
// that breaks downcast (e.g. flattening into a string) would fail these
// tests rather than silently degrading observability.

/// Build a wiremock origin that serves `body` with `Content-Encoding: encoding`
/// at `/<hash>`. Returns `(server, hash)`.
async fn serve_encoded(encoding: &'static str, body: Vec<u8>, canonical_hash: Hash) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", canonical_hash.to_hex())))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Encoding", encoding)
                .set_body_bytes(body),
        )
        .mount(&server)
        .await;
    server
}

/// Reject a decompression bomb mid-stream. Critical security path —
/// without this test, a regression that removed the running cap on
/// the decoded byte count would be a memory-exhaustion `DoS` via a
/// malicious origin.
///
/// Pre-#271 this was caught by `read_capped` inside `decompress_body`
/// (a typed `OriginError::DecompressionFailed`). Post-#271 the
/// engine's `count_and_cap_stream` is the single cap layer — it
/// enforces `max_blob_bytes` on the **decoded** stream regardless of
/// how the encoded bytes arrived (gzip, zstd, identity), so the bomb
/// surfaces as a generic `CacheError::OriginError` whose message
/// names the cap. Operator-debug fidelity: still actionable; the
/// previously-typed `DecompressionFailed` variant now reaches the
/// chain only when the decoder itself fails (truncated / empty body
/// — see the dedicated tests below).
#[tokio::test]
async fn http_origin_rejects_decompression_bomb() -> anyhow::Result<()> {
    // 4 MiB of zeros gzips to ~4 KiB. Engine cap = 1 MiB so neither
    // the Content-Length fast-path (4 KiB encoded) nor the per-chunk
    // cap on encoded bytes would catch this — only the
    // count_and_cap_stream running total over decoded bytes does.
    let payload = vec![0u8; 4 * 1024 * 1024];
    let canonical = Hash::new(&payload);
    let compressed = gzip(&payload)?;
    anyhow::ensure!(
        compressed.len() < 1024 * 1024,
        "test setup: compressed body must be smaller than the cap to \
         exercise the running-total check, was {} bytes",
        compressed.len()
    );
    let server = serve_encoded("gzip", compressed, canonical).await;

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let engine = CacheEngine::open(tmp.path(), Some(origin), 1).await?;

    let err = err_of(engine.get(canonical).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::BlobTooLarge { .. }),
        "expected BlobTooLarge from bomb cap, got: {err:?}"
    );
    Ok(())
}

/// Truncated gzip body: the decoder emits an `io::Error` mid-stream.
/// The whole point of `OriginError::DecompressionFailed` is to produce
/// a clear "decoder rejected the body" message rather than the
/// downstream `CacheError::HashMismatch` an operator would otherwise
/// see (the engine never gets to verify because there's nothing to
/// verify).
#[tokio::test]
async fn http_origin_truncated_gzip_surfaces_decompression_failed() -> anyhow::Result<()> {
    let payload: &[u8] = b"truncate me, please, but only the gzip wrapper";
    let canonical = Hash::new(payload);
    let mut compressed = gzip(payload)?;
    // Lop off the gzip trailer + the final byte of the deflate stream.
    // 12+ bytes is enough to break the CRC and length checks.
    let drop = compressed.len().saturating_sub(20);
    compressed.truncate(drop);
    let server = serve_encoded("gzip", compressed, canonical).await;

    let (engine, _tmp) = build_engine(&server.uri()).await?;
    let err = err_of(engine.get(canonical).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError, got: {err:?}"
    );
    let kind = err
        .origin_error_kind()
        .ok_or_else(|| anyhow::anyhow!("expected typed downcast"))?;
    anyhow::ensure!(
        matches!(
            kind,
            OriginError::DecompressionFailed {
                encoding: SupportedEncoding::Gzip,
                ..
            }
        ),
        "expected DecompressionFailed(Gzip), got: {kind:?}"
    );
    Ok(())
}

/// Unknown encoding surfaces typed `UnsupportedEncoding` — exercises the
/// downcast helper, complementing `http_origin_rejects_unknown_encoding`
/// which only asserts on the formatted message.
#[tokio::test]
async fn http_origin_unknown_encoding_is_typed() -> anyhow::Result<()> {
    let payload: &[u8] = b"who knows";
    let hash = Hash::new(payload);
    let server = serve_encoded("br", payload.to_vec(), hash).await;
    let (engine, _tmp) = build_engine(&server.uri()).await?;

    let err = err_of(engine.get(hash).await)?;
    let kind = err
        .origin_error_kind()
        .ok_or_else(|| anyhow::anyhow!("expected typed downcast"))?;
    let OriginError::UnsupportedEncoding { encoding } = kind else {
        anyhow::bail!("expected UnsupportedEncoding, got: {kind:?}");
    };
    anyhow::ensure!(
        encoding.as_ref() == "br",
        "encoding string should round-trip: {encoding:?}"
    );
    Ok(())
}

/// Case-insensitive matching: `Content-Encoding: GZIP` is RFC-compliant
/// and must be handled identically to lowercase `gzip`. Without an
/// explicit test, a refactor swapping `eq_ignore_ascii_case` for `==`
/// would silently break interop with origins that upper-case the value.
#[tokio::test]
async fn http_origin_accepts_uppercase_gzip_encoding() -> anyhow::Result<()> {
    let payload: &[u8] = b"upper case is fine, RFC says so";
    let hash = Hash::new(payload);
    let server = serve_encoded("GZIP", gzip(payload)?, hash).await;
    let (engine, _tmp) = build_engine(&server.uri()).await?;
    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload);
    Ok(())
}

/// Legacy `x-gzip` alias — ancient origins serve this. Same behaviour
/// as `gzip`.
#[tokio::test]
async fn http_origin_accepts_x_gzip_encoding() -> anyhow::Result<()> {
    let payload: &[u8] = b"the x prefix predates RFC 2616";
    let hash = Hash::new(payload);
    let server = serve_encoded("x-gzip", gzip(payload)?, hash).await;
    let (engine, _tmp) = build_engine(&server.uri()).await?;
    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload);
    Ok(())
}

/// Multi-encoding `gzip, zstd` must be rejected. The current
/// implementation doesn't split commas; future "smart" parsing would
/// silently change semantics, and this test locks the explicit
/// rejection in.
#[tokio::test]
async fn http_origin_rejects_multi_encoding_header() -> anyhow::Result<()> {
    let payload: &[u8] = b"don't try to be clever";
    let hash = Hash::new(payload);
    let server = serve_encoded("gzip, zstd", gzip(payload)?, hash).await;
    let (engine, _tmp) = build_engine(&server.uri()).await?;

    let err = err_of(engine.get(hash).await)?;
    let kind = err
        .origin_error_kind()
        .ok_or_else(|| anyhow::anyhow!("expected typed downcast"))?;
    anyhow::ensure!(
        matches!(kind, OriginError::UnsupportedEncoding { .. }),
        "expected UnsupportedEncoding for multi-encoding, got: {kind:?}"
    );
    Ok(())
}

/// Above the 1 MiB `DECOMPRESS_BLOCKING_THRESHOLD`, decompression runs
/// inside `spawn_blocking`. A regression dropping the `.await` or
/// flipping the comparator would fail this test (the current
/// `pull_through_succeeds_above_one_mib_payload` only exercises
/// the *hash* threshold via `FilesystemOrigin` and never decompresses).
#[tokio::test]
async fn http_origin_decompresses_above_blocking_threshold() -> anyhow::Result<()> {
    // 2 MiB decompressed → crosses the 1 MiB threshold for both the
    // raw read AND the spawn_blocking path inside the decoder.
    let payload = vec![0xC0u8; 2 * 1024 * 1024];
    let hash = Hash::new(&payload);
    let server = serve_encoded("gzip", gzip(&payload)?, hash).await;

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let engine = CacheEngine::open(tmp.path(), Some(origin), 16).await?;
    let got = engine.get(hash).await?;
    anyhow::ensure!(got.len() == payload.len(), "size mismatch: {}", got.len());
    anyhow::ensure!(got[..] == payload[..], "decompressed content mismatch");
    Ok(())
}

/// Pre-stream Content-Length rejection now applies to compressed bodies
/// too: an origin that advertises a compressed body larger than
/// `max_bytes` is malicious or misconfigured (compression ratios < 1
/// are universal in practice). Catching it before any byte streams
/// saves bandwidth and surfaces a clearer error.
#[tokio::test]
async fn http_origin_rejects_oversized_compressed_content_length() -> anyhow::Result<()> {
    // 2 MiB of repeating bytes gzips to about a few KiB, but we lie to
    // the client: serve a 5 MiB compressed body with `Content-Encoding:
    // gzip` and `Content-Length: 5 MiB`, then cap at 1 MiB.
    let canonical = Hash::new(b"doesn't matter");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", canonical.to_hex())))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Encoding", "gzip")
                // Honest C-L describing the body; mock-server's HTTP
                // layer fills the actual length, but it'll exceed the
                // cap anyway.
                .set_body_bytes(vec![0u8; 5 * 1024 * 1024]),
        )
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let engine = CacheEngine::open(tmp.path(), Some(origin), 1).await?;

    let err = err_of(engine.get(canonical).await)?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError from compressed C-L cap, got: {err:?}"
    );
    anyhow::ensure!(
        msg.contains("exceeds max"),
        "error should name the cap: {msg}"
    );
    Ok(())
}

/// Empty compressed body (the `Content-Encoding: gzip` is present but
/// the body has zero bytes) is malformed — gzip frames have minimum
/// header overhead. The decoder rejects mid-stream with
/// `DecompressionFailed`.
#[tokio::test]
async fn http_origin_empty_gzip_body_surfaces_decompression_failed() -> anyhow::Result<()> {
    let canonical = Hash::new(b"phantom");
    let server = serve_encoded("gzip", Vec::new(), canonical).await;
    let (engine, _tmp) = build_engine(&server.uri()).await?;

    let err = err_of(engine.get(canonical).await)?;
    let kind = err
        .origin_error_kind()
        .ok_or_else(|| anyhow::anyhow!("expected typed downcast"))?;
    anyhow::ensure!(
        matches!(
            kind,
            OriginError::DecompressionFailed {
                encoding: SupportedEncoding::Gzip,
                ..
            }
        ),
        "expected DecompressionFailed(Gzip), got: {kind:?}"
    );
    Ok(())
}

/// `BLAKE3` verify must run over the *decompressed* form (the whole
/// point of #312). Flipping a single byte of the canonical payload
/// before computing the expected hash would let a buggy implementation
/// (one that hashes the raw compressed bytes) pass — this test asserts
/// the verify happens after decompression by deliberately requesting
/// a hash that matches the compressed bytes, not the canonical ones,
/// and expecting `HashMismatch`.
#[tokio::test]
async fn http_origin_blake3_verify_runs_over_decompressed_bytes() -> anyhow::Result<()> {
    let payload: &[u8] = b"verify-after-decompress, not before";
    let canonical = Hash::new(payload);
    let compressed = gzip(payload)?;
    // Hash of the *compressed* bytes — what a regression would match.
    let raw_hash = Hash::new(&compressed);
    anyhow::ensure!(canonical != raw_hash, "test premise: hashes differ");

    let server = serve_encoded("gzip", compressed, raw_hash).await;
    let (engine, _tmp) = build_engine(&server.uri()).await?;

    // Asking for the raw-bytes hash: the decoded body has a different
    // BLAKE3 → engine surfaces HashMismatch (proving the verify saw
    // the canonical bytes, not the compressed ones).
    let err = err_of(engine.get(raw_hash).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::HashMismatch { .. }),
        "expected HashMismatch (verify ran over decompressed form), got: {err:?}"
    );
    Ok(())
}

/// Strict mode passes through identity, regression-locks the opt-out
/// path. (The companion test
/// `http_origin_decompress_off_passes_through_identity` already exists;
/// this is the "encoding header explicitly set to `identity`" variant.)
#[tokio::test]
async fn http_origin_strict_mode_accepts_explicit_identity() -> anyhow::Result<()> {
    let payload: &[u8] = b"explicit identity";
    let hash = Hash::new(payload);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Encoding", "identity")
                .set_body_bytes(payload),
        )
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir()?;
    let origin =
        Arc::new(HttpOrigin::parse(&server.uri())?.with_decompress_mode(DecompressMode::Strict));
    let engine = CacheEngine::open(tmp.path(), Some(origin), 16).await?;
    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload);
    Ok(())
}

// ----- HTTP status mapping (#375) -----
//
// `HttpOrigin::fetch` partitions response status into three buckets:
//   * 404 → `OriginFetch::NotFound` — surfaced as `CacheError::NotFound`,
//     the dedicated "object missing at origin" signal.
//   * 2xx → success, body is read.
//   * everything else → `anyhow::bail!`, surfaced as
//     `CacheError::OriginError` with the status in the message.
//
// The 404 case has dedicated coverage above. These tests pin the
// remaining buckets so a refactor that, say, broadens NotFound to all
// 4xx (which would corrupt operator triage by reporting a 410 Gone or
// 403 Forbidden as "absent") fails loudly.

/// Mount a wiremock at `/<hash>` for each status. Each status gets a
/// distinct hash so a single engine + server pair can serve the whole
/// table — avoids the per-iteration `MockServer::start` + `tempdir`
/// overhead that adds up across the suite.
async fn mount_status_mocks(server: &MockServer, statuses: &[u16]) -> Vec<Hash> {
    let mut hashes = Vec::with_capacity(statuses.len());
    for &status in statuses {
        let label = format!("status-mapping-{status}");
        let hash = Hash::new(label.as_bytes());
        Mock::given(method("GET"))
            .and(path(format!("/{}", hash.to_hex())))
            .respond_with(ResponseTemplate::new(status))
            .mount(server)
            .await;
        hashes.push(hash);
    }
    hashes
}

#[tokio::test]
async fn http_origin_4xx_other_than_404_surfaces_origin_error() -> anyhow::Result<()> {
    // Picks a representative spread:
    //   * 410 Gone — the case operators most often expect to be folded
    //     into NotFound; pinning OriginError keeps a future "broaden
    //     NotFound to all 4xx" refactor visible.
    //   * 401/403 — auth misconfiguration.
    //   * 400/422 — malformed request.
    //   * 429 — rate-limited, retryable; must not cache as absent.
    let statuses = [400u16, 401, 403, 410, 422, 429];
    let server = MockServer::start().await;
    let hashes = mount_status_mocks(&server, &statuses).await;
    // 429 is transient under the new classifier (#285) and would burn
    // the default retry budget without changing the assertion. Use a
    // disabled-retry engine so the test stays fast and asserts the
    // first-attempt mapping rather than the post-retry collapse.
    let (engine, _tmp) = build_engine_no_retry(&server.uri()).await?;

    for (status, hash) in statuses.iter().zip(hashes.iter()) {
        let err = err_of(engine.get(*hash).await)?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            matches!(err, CacheError::OriginError { .. }),
            "expected OriginError for status {status}, got: {err:?}"
        );
        anyhow::ensure!(
            msg.contains(&status.to_string()),
            "error should name status {status}: {msg}"
        );
        anyhow::ensure!(
            !engine.has(*hash).await?,
            "non-404 4xx must not populate the cache for {status}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn http_origin_5xx_surfaces_origin_error() -> anyhow::Result<()> {
    // 5xx is transient origin trouble, not "object missing". Pinning
    // `OriginError` (rather than `NotFound`) keeps the operator-facing
    // distinction intact: `NotFound` is the signal a client uses to
    // give up; misclassifying a 503 as `NotFound` would mask an outage
    // as missing data.
    let statuses = [500u16, 502, 503, 504];
    let server = MockServer::start().await;
    let hashes = mount_status_mocks(&server, &statuses).await;
    // Disabled retry: this test pins the first-attempt 5xx classification.
    // The retry-loop's behaviour on 5xx is covered by
    // `retry_classifies_http_5xx_as_transient_then_succeeds` below.
    let (engine, _tmp) = build_engine_no_retry(&server.uri()).await?;

    for (status, hash) in statuses.iter().zip(hashes.iter()) {
        let err = err_of(engine.get(*hash).await)?;
        let msg = format!("{err:#}");
        anyhow::ensure!(
            matches!(err, CacheError::OriginError { .. }),
            "expected OriginError for status {status}, got: {err:?}"
        );
        anyhow::ensure!(
            msg.contains(&status.to_string()),
            "error should name status {status}: {msg}"
        );
        anyhow::ensure!(
            !engine.has(*hash).await?,
            "5xx must not populate the cache for {status}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn http_origin_2xx_non_200_succeeds() -> anyhow::Result<()> {
    // `status.is_success()` is the gate, not `== 200`. RFC 9110 §15.3
    // permits a 2xx range; an origin returning 203 (Non-Authoritative
    // Information) or 206 should still be accepted. This pins the
    // wider acceptance so a future tightening to `== 200` is a
    // deliberate, test-visible decision.
    let payload: &[u8] = b"two-oh-three is fine";
    let hash = Hash::new(payload);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(203).set_body_bytes(payload))
        .mount(&server)
        .await;

    let (engine, _tmp) = build_engine(&server.uri()).await?;
    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload, "2xx body must be served");
    Ok(())
}

// ----- Content-Length advisory fast-path (#375) -----
//
// `HttpOrigin::fetch` short-circuits with an `OriginError` *before*
// reading any body when `Content-Length > max_bytes`. The existing
// `blob_too_large_is_rejected_via_http_origin` test trips both the
// fast-path and the streaming cap (2 MiB body, 1 MiB cap, wiremock
// sets honest C-L). These tests isolate the fast-path:
//
//   * spoof a huge `Content-Length` with no body — proves the check
//     fires from headers alone, before any chunk read,
//   * exercise the off-by-one boundary (cap == limit succeeds; cap + 1
//     fails). A regression flipping `>` to `>=` would slip past the
//     existing 2x-over test but fail here.

/// Bind an ephemeral TCP port and serve exactly one response with
/// `Content-Length: <advertised>` followed by `body_bytes`. Used to
/// spoof a Content-Length that lies about the body size — the
/// fast-path rejection inside `HttpOrigin::fetch` must fire on
/// the advertised length alone, never reading the body.
async fn spawn_spoofed_content_length_server(
    advertised: u64,
    body: Vec<u8>,
) -> anyhow::Result<std::net::SocketAddr> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        // Drain request headers — same pattern as the other raw-TCP
        // helpers in this file.
        let mut buf = [0u8; 4096];
        loop {
            match sock.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    let seen = buf.get(..n).unwrap_or(&[]);
                    if seen.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
            }
        }
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {advertised}\r\nContent-Type: application/octet-stream\r\n\r\n",
        );
        if sock.write_all(header.as_bytes()).await.is_err() {
            return;
        }
        let _ = sock.write_all(&body).await;
        // Hold the socket open so the client doesn't see EOF before it
        // has time to act on the headers.
        let _sock = sock;
        std::future::pending::<()>().await;
    });
    Ok(addr)
}

#[tokio::test]
async fn http_origin_rejects_advertised_oversize_before_reading_body() -> anyhow::Result<()> {
    // Spoof Content-Length: 5 GiB but write zero body bytes. If the
    // fast-path is wired correctly, `fetch` returns immediately with
    // an error naming the cap. If a regression removes the C-L check,
    // this would fall through to streaming and hit either the chunk
    // idle timeout (much slower) or — worse — block forever waiting
    // on a body that will never arrive.
    let advertised: u64 = 5 * 1024 * 1024 * 1024;
    let addr = spawn_spoofed_content_length_server(advertised, Vec::new()).await?;
    // Tight timeouts: if the fast-path fails, the test should fail
    // *fast* with a timeout error rather than tying up the suite.
    let origin = HttpOrigin::parse(&format!("http://{addr}/"))?
        .with_timeouts(Duration::from_secs(5), Duration::from_millis(500));

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Some(Arc::new(origin)), 1).await?;

    let err = err_of(engine.get(Hash::new(b"anything")).await)?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError from Content-Length fast-path, got: {err:?}"
    );
    anyhow::ensure!(
        msg.contains("exceeds max"),
        "error should name the fast-path message: {msg}"
    );
    anyhow::ensure!(
        msg.contains(&advertised.to_string()),
        "error should name the advertised length {advertised}: {msg}"
    );
    Ok(())
}

#[tokio::test]
async fn http_origin_accepts_content_length_exactly_at_cap() -> anyhow::Result<()> {
    // Exactly-at-cap is the right side of the `>` comparison —
    // `len > max_bytes` must be false when `len == max_bytes`. A
    // regression flipping to `>=` would reject this legitimate
    // payload, so the test pins the boundary.
    const CAP_MB: u64 = 1;
    const CAP_BYTES: usize = 1024 * 1024;
    let payload = vec![0xA5u8; CAP_BYTES];
    let hash = Hash::new(&payload);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.clone()))
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let engine = CacheEngine::open(tmp.path(), Some(origin), CAP_MB).await?;

    let got = engine.get(hash).await?;
    anyhow::ensure!(
        got.len() == payload.len(),
        "exact-cap size mismatch: {} vs {}",
        got.len(),
        payload.len()
    );
    Ok(())
}

#[tokio::test]
async fn http_origin_rejects_content_length_one_over_cap() -> anyhow::Result<()> {
    // Companion to the exact-cap test: `cap + 1` must trip the
    // fast-path. Together with the exact-cap test these pin the
    // off-by-one boundary that the existing 2x-over test cannot.
    const CAP_MB: u64 = 1;
    const CAP_BYTES: usize = 1024 * 1024;
    let payload = vec![0xA6u8; CAP_BYTES + 1];
    let hash = Hash::new(&payload);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let engine = CacheEngine::open(tmp.path(), Some(origin), CAP_MB).await?;

    let err = err_of(engine.get(hash).await)?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError at cap+1, got: {err:?}"
    );
    anyhow::ensure!(
        msg.contains("exceeds max"),
        "error should name the fast-path message at boundary: {msg}"
    );
    Ok(())
}

// ----- Redirect handling (#375) -----
//
// `HttpOrigin` builds its `reqwest::Client` without an explicit
// `redirect::Policy`, which means reqwest's default applies: follow up
// to 10 redirects, then error with `TooManyRedirects`. These tests
// pin that behaviour so a future change (e.g. switching to
// `Policy::none()` for SSRF protection, or relaxing the limit) is a
// deliberate, test-visible decision rather than a silent drift.
//
// The content-address invariant covers the worst case regardless: a
// redirect to attacker-controlled bytes still has to satisfy the
// BLAKE3 verify the engine runs after the body comes back, so the
// redirect itself can only get an attacker as far as a
// `HashMismatch`. But "redirects work" is a behaviour operators may
// rely on (e.g. an S3-fronted origin that 302s to a presigned URL),
// so we test both directions.

#[tokio::test]
async fn http_origin_follows_single_redirect_to_canonical_origin() -> anyhow::Result<()> {
    // 302 → final origin returns the canonical bytes. Models a
    // common pattern: a frontend that 302s to a CDN edge.
    let payload: &[u8] = b"behind a 302";
    let hash = Hash::new(payload);

    let final_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
        .mount(&final_server)
        .await;

    let redirect_server = MockServer::start().await;
    let location = format!("{}/{}", final_server.uri(), hash.to_hex());
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(302).insert_header("Location", location.as_str()))
        .mount(&redirect_server)
        .await;

    let (engine, _tmp) = build_engine(&redirect_server.uri()).await?;
    let got = engine.get(hash).await?;
    anyhow::ensure!(
        &got[..] == payload,
        "redirected fetch should land on canonical bytes"
    );
    anyhow::ensure!(
        engine.has(hash).await?,
        "redirected fetch should populate the cache"
    );
    Ok(())
}

#[tokio::test]
async fn http_origin_follows_301_redirect() -> anyhow::Result<()> {
    // 301 (permanent) is semantically distinct from 302 (temporary)
    // for caches and crawlers, but reqwest follows both transparently.
    // Pin that we don't accidentally treat 301 as terminal.
    let payload: &[u8] = b"behind a 301";
    let hash = Hash::new(payload);

    let final_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
        .mount(&final_server)
        .await;

    let redirect_server = MockServer::start().await;
    let location = format!("{}/{}", final_server.uri(), hash.to_hex());
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(301).insert_header("Location", location.as_str()))
        .mount(&redirect_server)
        .await;

    let (engine, _tmp) = build_engine(&redirect_server.uri()).await?;
    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload, "301 redirect should be followed");
    Ok(())
}

#[tokio::test]
async fn http_origin_redirected_404_surfaces_not_found() -> anyhow::Result<()> {
    // The status-bucket gate runs on the *final* response, not the
    // intermediate 302. If the redirect target returns 404, the
    // engine must see `NotFound` (not `OriginError`) — same as a
    // direct 404. Without this test, a refactor that started gating
    // on the original status would silently misclassify a redirected
    // 404 as a transport error.
    let hash = Hash::new(b"absent at the redirect target");

    let final_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(404))
        .mount(&final_server)
        .await;

    let redirect_server = MockServer::start().await;
    let location = format!("{}/{}", final_server.uri(), hash.to_hex());
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(302).insert_header("Location", location.as_str()))
        .mount(&redirect_server)
        .await;

    let (engine, _tmp) = build_engine(&redirect_server.uri()).await?;
    let err = err_of(engine.get(hash).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::NotFound { .. }),
        "expected NotFound from redirect target, got: {err:?}"
    );
    Ok(())
}

#[tokio::test]
async fn http_origin_rejects_redirect_loop() -> anyhow::Result<()> {
    // Self-referential redirect. reqwest's default policy caps at 10
    // hops, after which it errors out — surfacing as `OriginError`.
    // A regression switching to `Policy::limited(usize::MAX)` (or
    // disabling the cap somehow) would let this hang or loop, so the
    // test pins that some upper bound exists.
    let hash = Hash::new(b"loop me");

    let server = MockServer::start().await;
    // Self-redirect: every GET to /<hex> 302s back to itself.
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(302).insert_header(
            "Location",
            format!("{}/{}", server.uri(), hash.to_hex()).as_str(),
        ))
        .mount(&server)
        .await;

    // Tight headers timeout so an unbounded loop fails fast as a
    // timeout rather than tying up the suite indefinitely.
    let origin = HttpOrigin::parse(&server.uri())?
        .with_timeouts(Duration::from_secs(10), Duration::from_secs(5));

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Some(Arc::new(origin)), 16).await?;
    let err = err_of(engine.get(hash).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError from redirect loop, got: {err:?}"
    );
    Ok(())
}

// ----- origin retry policy (#285) -----------------------------------------

/// Build a `CacheEngine` with a custom retry policy and no metrics
/// wiring. Returns the engine and the temp dir whose drop cleans up.
async fn build_engine_with_retry(
    origin: Arc<dyn Origin>,
    policy: RetryPolicy,
) -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open_full(
        tmp.path(),
        Some(origin),
        16,
        PinnedHashes::empty(),
        policy,
        None,
        Duration::ZERO,
    )
    .await?;
    Ok((engine, tmp))
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
/// before either yielding `Found(payload)` or, if `max_failures` is
/// `usize::MAX`, failing forever. Counts every fetch via an `AtomicUsize`
/// so tests can assert exact attempt totals.
#[derive(Debug)]
struct FailingThenSucceedingOrigin {
    payload: bytes::Bytes,
    target: Hash,
    fail_count: std::sync::atomic::AtomicUsize,
    fetch_count: std::sync::atomic::AtomicUsize,
    max_failures: usize,
}

impl FailingThenSucceedingOrigin {
    fn new(payload: &[u8], max_failures: usize) -> Self {
        Self {
            target: Hash::new(payload),
            payload: bytes::Bytes::from(payload.to_vec()),
            fail_count: std::sync::atomic::AtomicUsize::new(0),
            fetch_count: std::sync::atomic::AtomicUsize::new(0),
            max_failures,
        }
    }
    fn fetches(&self) -> usize {
        self.fetch_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Origin for FailingThenSucceedingOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
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
                Ok(OriginFetch::found_one_shot(payload))
            } else {
                Ok(OriginFetch::NotFound)
            }
        })
    }
}

/// Origin that always returns `OriginPullError::Permanent` and counts
/// attempts. Used to verify the retry loop short-circuits on permanent
/// errors.
#[derive(Debug)]
struct PermanentlyFailingOrigin {
    fetch_count: std::sync::atomic::AtomicUsize,
}

impl PermanentlyFailingOrigin {
    const fn new() -> Self {
        Self {
            fetch_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    fn fetches(&self) -> usize {
        self.fetch_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Origin for PermanentlyFailingOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        self.fetch_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            Err(OriginPullError::Permanent(anyhow::anyhow!(
                "synthetic permanent failure (e.g. 403)"
            )))
        })
    }
}

#[tokio::test]
async fn retry_succeeds_after_transient_failures() -> anyhow::Result<()> {
    // 2 transient failures, then success on the 3rd attempt. Policy
    // allows 3 retries, so the loop has headroom.
    let payload: &[u8] = b"recovered";
    let origin = Arc::new(FailingThenSucceedingOrigin::new(payload, 2));
    let (engine, _tmp) =
        build_engine_with_retry(origin.clone() as Arc<dyn Origin>, fast_retry_policy(3)).await?;

    let hash = Hash::new(payload);
    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload, "payload survived retry chain");
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
    let origin = Arc::new(FailingThenSucceedingOrigin::new(payload, usize::MAX));
    let (engine, _tmp) =
        build_engine_with_retry(origin.clone() as Arc<dyn Origin>, fast_retry_policy(3)).await?;

    let hash = Hash::new(payload);
    let err = err_of(engine.get(hash).await)?;
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
    let origin = Arc::new(PermanentlyFailingOrigin::new());
    let (engine, _tmp) =
        build_engine_with_retry(origin.clone() as Arc<dyn Origin>, fast_retry_policy(5)).await?;

    let hash = Hash::new(b"anything");
    let err = err_of(engine.get(hash).await)?;
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
    // 404-equivalent: the adapter returns OriginFetch::NotFound, which
    // is *not* an error and must not be retried. `expect(1)` on the
    // mock panics on drop if more than one request lands.
    let server = MockServer::start().await;
    let hash = Hash::new(b"missing");
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(404))
        .expect(1)
        .mount(&server)
        .await;

    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let (engine, _tmp) =
        build_engine_with_retry(origin as Arc<dyn Origin>, fast_retry_policy(3)).await?;

    let err = err_of(engine.get(hash).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::NotFound { .. }),
        "404 should map to CacheError::NotFound, got: {err:?}"
    );
    Ok(())
}

#[tokio::test]
async fn retry_disabled_policy_makes_a_single_attempt() -> anyhow::Result<()> {
    // RetryPolicy::disabled() reproduces the pre-#285 behaviour: a
    // transient failure surfaces immediately without retrying.
    let payload: &[u8] = b"unreached";
    let origin = Arc::new(FailingThenSucceedingOrigin::new(payload, usize::MAX));
    let (engine, _tmp) =
        build_engine_with_retry(origin.clone() as Arc<dyn Origin>, RetryPolicy::disabled()).await?;

    let hash = Hash::new(payload);
    let err = err_of(engine.get(hash).await)?;
    anyhow::ensure!(matches!(err, CacheError::OriginError { .. }));
    anyhow::ensure!(
        origin.fetches() == 1,
        "disabled policy must perform exactly one attempt, got {}",
        origin.fetches()
    );
    Ok(())
}

#[tokio::test]
async fn retry_classifies_http_5xx_as_transient_then_succeeds() -> anyhow::Result<()> {
    // wiremock's `up_to_n_times(N)` makes a mock match at most N times,
    // after which subsequent requests fall through to the next mock
    // (matched in registration order). This lets us script "fail twice,
    // then succeed" without a custom Origin impl.
    let payload: &[u8] = b"after backoff";
    let hash = Hash::new(payload);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
        .mount(&server)
        .await;

    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let (engine, _tmp) =
        build_engine_with_retry(origin as Arc<dyn Origin>, fast_retry_policy(3)).await?;
    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload);
    Ok(())
}

#[tokio::test]
async fn retry_classifies_http_4xx_non_404_as_permanent() -> anyhow::Result<()> {
    // 403 is permanent — the retry loop must not paper over what is
    // probably an auth misconfiguration. `expect(1)` on the mock
    // double-covers the no-retry assertion at drop time.
    let payload: &[u8] = b"forbidden";
    let hash = Hash::new(payload);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(403))
        .expect(1)
        .mount(&server)
        .await;

    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let (engine, _tmp) =
        build_engine_with_retry(origin as Arc<dyn Origin>, fast_retry_policy(5)).await?;
    let err = err_of(engine.get(hash).await)?;
    anyhow::ensure!(matches!(err, CacheError::OriginError { .. }));
    Ok(())
}

#[tokio::test]
async fn coalesced_owner_retry_unblocks_waiters_on_success() -> anyhow::Result<()> {
    // Multiple concurrent gets for the same hash: the engine coalesces
    // them through a single owner that runs the retry loop. The owner
    // retries through 2 transient failures, then succeeds; every waiter
    // observes the final cached bytes — proof that the retry loop sits
    // inside the coalescing critical section, not outside.
    let payload: &[u8] = b"shared via coalescing";
    let origin = Arc::new(FailingThenSucceedingOrigin::new(payload, 2));
    let (engine, _tmp) =
        build_engine_with_retry(origin.clone() as Arc<dyn Origin>, fast_retry_policy(3)).await?;
    let hash = Hash::new(payload);

    let mut handles = Vec::new();
    for _ in 0..6 {
        let e = engine.clone();
        handles.push(tokio::spawn(async move { e.get(hash).await }));
    }
    for h in handles {
        let bytes = h.await??;
        anyhow::ensure!(&bytes[..] == payload);
    }
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
async fn coalesced_owner_exhaustion_bounded_by_serial_owner_count() -> anyhow::Result<()> {
    // Documents the known coalescing+retry limitation called out in the
    // retry.rs module doc: under sustained transient failure, after one
    // owner exhausts its retry budget, a *waiter* may become the next
    // owner and run a fresh budget. With N concurrent waiters this
    // produces up to `N` *sequential* retry budgets (no parallel
    // fan-out — at any instant exactly one owner is running). The
    // load-bearing invariants this test pins:
    //
    //   1. Every concurrent get observes a CacheError::OriginError
    //      (no spurious successes from a stale cache hit).
    //   2. Total origin fetches stay within the documented worst case
    //      `N * (1 + max_retries)` — a regression that re-spread retry
    //      across waiters in parallel would blow this bound.
    //   3. Total origin fetches stay above the no-coalescing best case
    //      `1 + max_retries` — confirming retry actually runs.
    //
    // The bound is asserted as <= worst-case, not == exact, because the
    // race between owner-exhaustion-notify and waiter-loop-reentry is
    // scheduler-dependent. A stricter bound would be flake-prone.
    const N_WAITERS: u32 = 5;
    const MAX_RETRIES: u32 = 2;
    let payload: &[u8] = b"never available";
    let origin = Arc::new(FailingThenSucceedingOrigin::new(payload, usize::MAX));
    let (engine, _tmp) = build_engine_with_retry(
        origin.clone() as Arc<dyn Origin>,
        fast_retry_policy(MAX_RETRIES),
    )
    .await?;
    let hash = Hash::new(payload);

    let mut handles = Vec::new();
    for _ in 0..N_WAITERS {
        let e = engine.clone();
        handles.push(tokio::spawn(async move { e.get(hash).await }));
    }
    let mut errors: u32 = 0;
    for h in handles {
        match h.await? {
            Err(CacheError::OriginError { .. }) => errors += 1,
            other => anyhow::bail!("expected OriginError, got: {other:?}"),
        }
    }
    anyhow::ensure!(errors == N_WAITERS);

    let attempts = origin.fetches();
    let worst_case = (N_WAITERS as usize) * (1 + MAX_RETRIES as usize);
    let no_coalesce_minimum = 1 + MAX_RETRIES as usize;
    anyhow::ensure!(
        attempts <= worst_case,
        "attempts={attempts} exceeds worst-case {worst_case} (N_WAITERS * (1+MAX_RETRIES))",
    );
    anyhow::ensure!(
        attempts >= no_coalesce_minimum,
        "attempts={attempts} below the minimum {no_coalesce_minimum} — retry never ran?",
    );
    Ok(())
}

#[tokio::test]
async fn fs_origin_retries_on_transient_io_kind_then_succeeds() -> anyhow::Result<()> {
    // Custom Origin that emits one Transient (synthetic Interrupted)
    // before delegating to a real FilesystemOrigin. Verifies the retry
    // loop fires for the FS adapter's Transient classification path.
    #[derive(Debug)]
    struct InterruptOnceOrigin {
        inner: FilesystemOrigin,
        first: std::sync::atomic::AtomicBool,
    }
    impl Origin for InterruptOnceOrigin {
        fn kind(&self) -> OriginKind {
            self.inner.kind()
        }

        fn fetch(
            &self,
            hash: Hash,
            max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>>
        {
            let already_failed = self.first.swap(true, std::sync::atomic::Ordering::SeqCst);
            if already_failed {
                self.inner.fetch(hash, max_bytes)
            } else {
                Box::pin(async move {
                    Err(OriginPullError::Transient(
                        std::io::Error::from(std::io::ErrorKind::Interrupted).into(),
                    ))
                })
            }
        }
    }

    let base = tempfile::tempdir()?;
    let payload: &[u8] = b"after interrupt";
    let hash = Hash::new(payload);
    // hash.to_hex() returns a stack-only ArrayString-style value; clone it
    // so the borrow ends before we call `.get(..2)` on it via deref.
    let hex = hash.to_hex().clone();
    let shard = hex.get(..2).ok_or_else(|| anyhow::anyhow!("hex prefix"))?;
    let shard_dir = base.path().join(shard);
    tokio::fs::create_dir_all(&shard_dir).await?;
    tokio::fs::write(shard_dir.join(&hex), payload).await?;

    let inner = FilesystemOrigin::new(base.path()).await?;
    let origin = Arc::new(InterruptOnceOrigin {
        inner,
        first: std::sync::atomic::AtomicBool::new(false),
    });
    let (engine, _tmp) =
        build_engine_with_retry(origin as Arc<dyn Origin>, fast_retry_policy(2)).await?;
    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload);
    Ok(())
}

#[tokio::test]
async fn fs_origin_does_not_retry_on_permission_denied() -> anyhow::Result<()> {
    // PermissionDenied falls in the Permanent arm of classify_io_error.
    // We synthesise it directly via a custom Origin since making a real
    // FS path PermissionDenied is platform-specific and would skip on
    // CI where the test runs as root in a container.
    #[derive(Debug)]
    struct DeniedOrigin {
        count: std::sync::atomic::AtomicUsize,
    }
    impl Origin for DeniedOrigin {
        fn kind(&self) -> OriginKind {
            OriginKind::Filesystem
        }

        fn fetch(
            &self,
            _hash: Hash,
            _max_bytes: u64,
        ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>>
        {
            self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                Err(OriginPullError::Permanent(
                    std::io::Error::from(std::io::ErrorKind::PermissionDenied).into(),
                ))
            })
        }
    }
    let origin = Arc::new(DeniedOrigin {
        count: std::sync::atomic::AtomicUsize::new(0),
    });
    let (engine, _tmp) =
        build_engine_with_retry(origin.clone() as Arc<dyn Origin>, fast_retry_policy(5)).await?;
    let hash = Hash::new(b"denied");
    let err = err_of(engine.get(hash).await)?;
    anyhow::ensure!(matches!(err, CacheError::OriginError { .. }));
    anyhow::ensure!(
        origin.count.load(std::sync::atomic::Ordering::SeqCst) == 1,
        "PermissionDenied must short-circuit after one attempt"
    );
    Ok(())
}

/// Wire-level proof that an operator-configured `cache.user_agent`
/// actually reaches the origin. The `new_with_user_agent_accepts_custom_value`
/// unit test in `crate::origin::http` only confirms the constructor doesn't
/// error, leaving the "is it set on the wire?" question untested. A future
/// reqwest builder reordering, or a typo passing the UA into the wrong
/// builder slot, would silently ship the default — exactly the regression
/// #435 is meant to prevent. Wiremock matches on the `User-Agent` header
/// here; if the header doesn't match, the mock returns 404 and the engine
/// errors out, which we assert against by checking the get succeeds.
#[tokio::test]
async fn http_origin_sends_configured_user_agent() -> anyhow::Result<()> {
    use decdn_cache::parse_origin_url;

    let payload: &[u8] = b"hello, decdn";
    let hash = Hash::new(payload);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .and(header("user-agent", "MyCdn/1.0 (+ops@example.com)"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
        .mount(&server)
        .await;

    let url = parse_origin_url(&server.uri())?;
    let origin = Arc::new(HttpOrigin::new_with_user_agent(
        url,
        "MyCdn/1.0 (+ops@example.com)",
    )?);
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Some(origin as Arc<dyn Origin>), 16).await?;

    let got = engine.get(hash).await?;
    anyhow::ensure!(
        &got[..] == payload,
        "fetch should have succeeded — if the configured UA never reached the wire, \
         the mock's header matcher would have served 404"
    );
    Ok(())
}

/// Default UA is sent on the wire when no override is configured. Pairs
/// with the test above: together they prove the seam between the config
/// resolver and the wire is intact in both branches.
#[tokio::test]
async fn http_origin_sends_default_user_agent_when_unset() -> anyhow::Result<()> {
    use decdn_cache::{DEFAULT_USER_AGENT, parse_origin_url};

    let payload: &[u8] = b"hello, decdn";
    let hash = Hash::new(payload);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .and(header("user-agent", DEFAULT_USER_AGENT))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
        .mount(&server)
        .await;

    let url = parse_origin_url(&server.uri())?;
    let origin = Arc::new(HttpOrigin::new(url)?);
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Some(origin as Arc<dyn Origin>), 16).await?;

    let got = engine.get(hash).await?;
    anyhow::ensure!(
        &got[..] == payload,
        "default UA fetch should have succeeded"
    );
    Ok(())
}
