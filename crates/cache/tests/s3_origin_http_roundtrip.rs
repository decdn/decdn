//! End-to-end `Content-Encoding` decompression tests for [`decdn_cache::S3Origin`]
//! over a **real HTTP wire path** (#965, following on from #804 / #806).
//!
//! The sibling suite `tests/s3_origin.rs` already covers the decompression
//! contract, but it injects a pre-built [`aws_sdk_s3::operation::get_object::GetObjectOutput`]
//! via `aws-smithy-mocks` — that shortcuts the SDK's response-parsing layer:
//! the `Content-Encoding` header is set on the *modeled output struct*, not
//! parsed off an HTTP response, and the body is a `ByteStream::from(Vec)`
//! rather than a hyper body framed over the socket.
//!
//! This suite closes that gap by standing up a [`wiremock`] HTTP server that
//! speaks the S3 GET-object response subset (status + `Content-Encoding`
//! header + raw body bytes) and pointing a real [`S3Origin`] at it via the
//! `endpoint_url` + `path_style` knobs on [`S3OriginConfig`] — the same
//! configuration path R2 / B2 / `MinIO` operators use. The full SDK stack runs
//! end-to-end: request signing, the hyper-1 + rustls HTTP client, response
//! header parsing, and the `ByteStream::into_async_read -> ReaderStream ->
//! decode_stream` body seam in `S3Origin::fetch`. No Docker, no localstack —
//! wiremock is already a dev-dependency for the `HttpOrigin` suite in
//! `tests/pull_through.rs`, so this adds no new dependency.
//!
//! wiremock does not validate the `SigV4` signature, so static dummy
//! credentials are sufficient to drive a signed request through.

use std::io::Write;
use std::sync::Arc;

use decdn_cache::{
    CacheEngine, Hash, Origin, OriginFetch, OriginPullError, S3Credentials, S3Origin,
    S3OriginConfig, parse_origin_url,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Bucket used across the suite. Path-style addressing puts it in the URL
/// path (`GET /{bucket}/{key}`), which is what wiremock matches on.
const BUCKET: &str = "decdn-blobs";

/// Compress `payload` with gzip. Mirrors the `pull_through.rs` /
/// `s3_origin.rs` helpers so all three suites exercise the same canonical
/// vs. compressed pairs.
fn gzip(payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    e.write_all(payload)?;
    Ok(e.finish()?)
}

/// Compress `payload` with zstd at a low level (fast, deterministic).
fn zstd_compress(payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    Ok(zstd::stream::encode_all(payload, 1)?)
}

/// The sharded S3 key for `hash`: `{hex[0..2]}/{hex}` (no prefix). Mirrors
/// `S3Origin`'s internal `key_for` so the wiremock path matcher tracks the
/// adapter's real request shape.
fn key_for(hash: Hash) -> String {
    let hex = hash.to_hex();
    let shard = hex.get(..2).unwrap_or("");
    format!("{shard}/{}", hex.as_str())
}

/// Stand up a wiremock server that answers the single S3 GET for `hash`'s
/// sharded key with `body` under `Content-Encoding: encoding` (omitted when
/// `encoding` is `None`). The matched path is `/{bucket}/{shard}/{hex}` —
/// path-style addressing, so the bucket lives in the URL path.
async fn serve_s3_object(
    hash: Hash,
    encoding: Option<&str>,
    body: Vec<u8>,
) -> anyhow::Result<MockServer> {
    let server = MockServer::start().await;
    let mut tmpl = ResponseTemplate::new(200);
    if let Some(enc) = encoding {
        tmpl = tmpl.insert_header("Content-Encoding", enc);
    }
    // `Content-Length` is set automatically by wiremock from the body bytes,
    // matching what a real S3 endpoint returns for the *encoded* body.
    let tmpl = tmpl.set_body_bytes(body);
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/{}", key_for(hash))))
        .respond_with(tmpl)
        .mount(&server)
        .await;
    Ok(server)
}

/// Build a real `S3Origin` pointed at `endpoint` (path-style, dummy static
/// creds, `us-east-1`). Goes through `S3Origin::new`, so the live hyper +
/// rustls HTTP client and the SDK's full request/response stack are wired
/// up — the whole point of this suite versus the smithy-mocks one.
async fn s3_origin_at(endpoint: &str) -> anyhow::Result<S3Origin> {
    let cfg = S3OriginConfig {
        bucket: BUCKET.to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: Some(parse_origin_url(endpoint)?),
        path_style: true,
        prefix: String::new(),
        credentials: Some(S3Credentials::Static {
            access_key_id: "AKIA-test-fake".to_string(),
            secret_access_key: "secret-fake".to_string(),
            session_token: None,
        }),
    };
    S3Origin::new(&cfg).await
}

/// gzip object round-trips over the real SDK HTTP path: the adapter parses
/// `Content-Encoding: gzip` off the wire response and hands back canonical
/// (decompressed) bytes. The BLAKE3 verify in the engine runs over canonical
/// bytes, so a regression that handed the gzipped wire bytes through would
/// surface as a hash mismatch downstream — here we assert the decoded bytes
/// directly. Mirrors `pull_through::http_origin_decompresses_gzip_response`.
#[tokio::test]
async fn s3_origin_decompresses_gzip_over_http() -> anyhow::Result<()> {
    let payload: &[u8] = b"hello, gzipped s3 over the wire! repeat repeat repeat repeat";
    let hash = Hash::new(payload);
    let server = serve_s3_object(hash, Some("gzip"), gzip(payload)?).await?;
    let origin = s3_origin_at(&server.uri()).await?;

    let bytes = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected Found, got NotFound"))?;
    anyhow::ensure!(
        &bytes[..] == payload,
        "gzip body should decode to canonical payload over the HTTP wire path"
    );
    Ok(())
}

/// zstd object round-trips over the real SDK HTTP path. Same contract as the
/// gzip case for the other supported codec.
#[tokio::test]
async fn s3_origin_decompresses_zstd_over_http() -> anyhow::Result<()> {
    let payload: &[u8] = b"hello, zstd s3 over the wire! and a longer body to compress xxxxx";
    let hash = Hash::new(payload);
    let server = serve_s3_object(hash, Some("zstd"), zstd_compress(payload)?).await?;
    let origin = s3_origin_at(&server.uri()).await?;

    let bytes = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected Found, got NotFound"))?;
    anyhow::ensure!(
        &bytes[..] == payload,
        "zstd body should decode to canonical payload over the HTTP wire path"
    );
    Ok(())
}

/// `br` (Brotli) is not a supported encoding — the adapter has no decoder for
/// it, so a `Content-Encoding: br` response is a permanent error that fires
/// before any body bytes are consumed. This is the "br" leg the issue calls
/// for: over the real wire, the SDK parses the header and `S3Origin::fetch`
/// rejects it. Passing the bytes through would later trip a confusing
/// `HashMismatch`. Mirrors `pull_through::http_origin_rejects_unknown_encoding`.
#[tokio::test]
async fn s3_origin_rejects_brotli_encoding_over_http() -> anyhow::Result<()> {
    let hash = Hash::new(b"would-be-brotli-body");
    // The body content is irrelevant — rejection happens at the header phase
    // before any bytes are read.
    let server = serve_s3_object(hash, Some("br"), b"\x00\x01\x02brotli-ish".to_vec()).await?;
    let origin = s3_origin_at(&server.uri()).await?;

    let err = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("unknown `br` encoding must be rejected"))?;
    let msg = match &err {
        OriginPullError::Permanent(e) => format!("{e:#}"),
        OriginPullError::Transient(e) => {
            anyhow::bail!("unknown-encoding rejection must be Permanent, was Transient: {e:#}");
        }
    };
    anyhow::ensure!(
        msg.contains("Content-Encoding") && msg.contains("br"),
        "error should name the unsupported `br` encoding: {msg}"
    );
    Ok(())
}

/// Full round-trip through the cache engine over the real HTTP path: a miss
/// pulls the gzip object from the S3 origin, the engine decodes it, the
/// BLAKE3 verify passes over the canonical bytes, the blob is cached, and a
/// second `get` is served locally without a second HTTP dispatch. This is the
/// integration the issue asks for — gzip decompression *and* BLAKE3
/// verifiability, end-to-end through the S3 adapter over the wire. Mirrors
/// `pull_through::cache_miss_pulls_from_origin_and_caches` plus the gzip
/// decode path.
#[tokio::test]
async fn cache_engine_pulls_gzip_from_s3_over_http_and_caches() -> anyhow::Result<()> {
    let payload: &[u8] = b"engine pull of a gzip blob through the s3 wire path, then cached";
    let hash = Hash::new(payload);
    // `expect(1)` proves the second `get` is served from cache, not re-fetched.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/{}", key_for(hash))))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Encoding", "gzip")
                .set_body_bytes(gzip(payload)?),
        )
        .expect(1)
        .mount(&server)
        .await;

    let origin: Arc<dyn Origin> = Arc::new(s3_origin_at(&server.uri()).await?);
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 16).await?;

    let got = engine.get(hash).await?;
    anyhow::ensure!(
        &got[..] == payload,
        "first get must return canonical (decompressed) bytes"
    );
    anyhow::ensure!(
        engine.has(hash).await?,
        "blob must be cached after the gzip miss"
    );

    let got2 = engine.get(hash).await?;
    anyhow::ensure!(
        &got2[..] == payload,
        "second get must be served from the local cache"
    );
    // `expect(1)` on the mock asserts the single HTTP dispatch on drop, so a
    // re-fetch regression fails this test when `server` is dropped.
    Ok(())
}

/// Full round-trip through the cache engine for the zstd codec — the engine
/// surfaces the canonical payload, confirming decode + BLAKE3 verify on the
/// zstd wire path as well.
#[tokio::test]
async fn cache_engine_pulls_zstd_from_s3_over_http_and_caches() -> anyhow::Result<()> {
    let payload: &[u8] = b"engine pull of a zstd blob through the s3 wire path xxxxxxxxxxxx";
    let hash = Hash::new(payload);
    let server = serve_s3_object(hash, Some("zstd"), zstd_compress(payload)?).await?;
    let origin: Arc<dyn Origin> = Arc::new(s3_origin_at(&server.uri()).await?);

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 16).await?;

    let got = engine.get(hash).await?;
    anyhow::ensure!(
        &got[..] == payload,
        "engine must return canonical bytes for a zstd-encoded S3 object"
    );
    Ok(())
}

/// Sanity baseline on the same wire path: an *uncompressed* object (no
/// `Content-Encoding` header) round-trips unchanged. Guards against a
/// regression where the decode seam mangles identity bodies — and confirms
/// the wiremock + `endpoint_url` plumbing itself is sound, so a failure in
/// the gzip/zstd cases above points at decompression, not transport.
#[tokio::test]
async fn s3_origin_passes_through_uncompressed_over_http() -> anyhow::Result<()> {
    let payload: &[u8] = b"plain uncompressed s3 bytes over the wire";
    let hash = Hash::new(payload);
    let server = serve_s3_object(hash, None, payload.to_vec()).await?;
    let origin = s3_origin_at(&server.uri()).await?;

    match origin.fetch(hash, 16 * 1024 * 1024).await? {
        OriginFetch::Found { .. } => {}
        OriginFetch::NotFound => anyhow::bail!("expected Found for an uncompressed object"),
    }
    let bytes = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected Found"))?;
    anyhow::ensure!(&bytes[..] == payload, "uncompressed body must pass through");
    Ok(())
}
