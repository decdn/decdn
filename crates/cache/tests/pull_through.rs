//! Integration tests for [`decdn_cache::CacheEngine`] end-to-end with a
//! mocked HTTP origin.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use decdn_cache::{
    CacheEngine, CacheError, DecompressMode, FilesystemOrigin, Hash, HttpOrigin, Origin,
    OriginError, OriginFetch, SupportedEncoding,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use wiremock::matchers::{method, path};
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
    anyhow::ensure!(!engine.has(expected).await?, "bad bytes must not be cached");
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
    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<OriginFetch>> + Send + '_>> {
        let payload = self.payload.clone();
        Box::pin(async move { Ok(OriginFetch::Found(payload)) })
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
    let addr = spawn_silent_server().await?;
    let origin = HttpOrigin::parse(&format!("http://{addr}/"))?
        .with_timeouts(Duration::from_millis(200), Duration::from_secs(30));

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Some(Arc::new(origin)), 16).await?;

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
    let addr = spawn_stall_after_partial_body_server().await?;
    let origin = HttpOrigin::parse(&format!("http://{addr}/"))?
        .with_timeouts(Duration::from_secs(30), Duration::from_millis(200));

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), Some(Arc::new(origin)), 16).await?;

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
async fn pull_through_succeeds_above_blocking_hash_threshold() -> anyhow::Result<()> {
    // 2 MiB payload crosses the 1 MiB `BLOCKING_HASH_THRESHOLD`, exercising
    // the `spawn_blocking` branch of hash verification. A regression
    // (missing `.await`, wrong comparator, panic in the blocking task) is
    // caught here — prior tests above the threshold all abort earlier on
    // size / hash mismatch.
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
    let msg = format!("{err:#}");
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError from mid-stream cap, got: {err:?}"
    );
    anyhow::ensure!(
        msg.contains("mid-stream"),
        "error message missing mid-stream marker: {msg}"
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

/// Force `read_capped`'s mid-stream cap to fire: gzip a small payload
/// that decompresses well past `max_blob_mb`. Critical security path —
/// without this test, a regression flipping `>` to `>=` (or removing
/// the cap) is a memory-exhaustion `DoS` via a malicious origin.
#[tokio::test]
async fn http_origin_rejects_decompression_bomb() -> anyhow::Result<()> {
    // 4 MiB of zeros gzips to ~4 KiB. Engine cap = 1 MiB so the
    // mid-stream check inside `read_capped` is the only thing that
    // catches this — both the Content-Length fast-path (4 KiB encoded)
    // and the engine post-receive cap would let it through.
    let payload = vec![0u8; 4 * 1024 * 1024];
    let canonical = Hash::new(&payload);
    let compressed = gzip(&payload)?;
    anyhow::ensure!(
        compressed.len() < 1024 * 1024,
        "test setup: compressed body must be smaller than the cap to \
         exercise the mid-stream check, was {} bytes",
        compressed.len()
    );
    let server = serve_encoded("gzip", compressed, canonical).await;

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let engine = CacheEngine::open(tmp.path(), Some(origin), 1).await?;

    let err = err_of(engine.get(canonical).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError from bomb cap, got: {err:?}"
    );
    let kind = err
        .origin_error_kind()
        .ok_or_else(|| anyhow::anyhow!("expected typed OriginError downcast, got: {err:?}"))?;
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
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("mid-stream"),
        "error message should name the mid-stream cap: {msg}"
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
/// `pull_through_succeeds_above_blocking_hash_threshold` only exercises
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
