//! End-to-end tests for [`decdn_cache::S3Origin`] over a **real HTTP wire
//! path** (#965).
//!
//! The sibling suite `tests/s3_origin.rs` injects pre-built
//! [`aws_sdk_s3::operation::get_object::GetObjectOutput`]s via
//! `aws-smithy-mocks`, which shortcuts the SDK's response-parsing layer:
//! headers are set on the *modeled output struct*, not parsed off an HTTP
//! response, and the body is a `ByteStream::from(Vec)` rather than a hyper
//! body framed over the socket.
//!
//! This suite stands up a [`wiremock`] HTTP server that speaks the S3
//! GET-object response subset (status, headers, raw body bytes) and points a
//! real [`S3Origin`] at it via the `endpoint_url` + `path_style` knobs on
//! [`S3OriginConfig`] — the same configuration path R2 / B2 / `MinIO`
//! operators use. The full SDK stack runs end-to-end: request signing, the
//! hyper-1 + rustls HTTP client and response header parsing.
//!
//! wiremock does not validate the `SigV4` signature, so static dummy
//! credentials are sufficient to drive a signed request through.
#![cfg(feature = "s3")]

use std::sync::Arc;

use decdn_cache::{
    CacheEngine, Hash, Origin, OriginPresence, OriginPullError, OriginRangeFetch,
    OriginRangeRequest, S3Credentials, S3Origin, S3OriginConfig, parse_origin_url,
};
use wiremock::matchers::{header_exists, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Bucket used across the suite. Path-style addressing puts it in the URL
/// path (`GET /{bucket}/{key}`), which is what wiremock matches on.
const BUCKET: &str = "decdn-blobs";

/// The sharded S3 key for `hash`: `{hex[0..2]}/{hex}` (no prefix). Mirrors
/// `S3Origin`'s internal `key_for` so the wiremock path matcher tracks the
/// adapter's real request shape.
fn key_for(hash: Hash) -> String {
    let hex = hash.to_hex();
    let shard = hex.get(..2).unwrap_or("");
    format!("{shard}/{}", hex.as_str())
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

/// A deterministic non-repeating blob of `len` bytes.
fn make_blob(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect()
}

/// The ranged read the SDK parses off the wire: a `206` answer to a request
/// carrying a `Range` header returns exactly the span.
#[tokio::test]
async fn s3_origin_reads_a_range_over_http() -> anyhow::Result<()> {
    let blob = make_blob(64 * 1024);
    let hash = Hash::new(&blob);
    let span = blob.get(..16 * 1024).unwrap_or_default().to_vec();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/{}", key_for(hash))))
        .and(header_exists("range"))
        .respond_with(ResponseTemplate::new(206).set_body_bytes(span.clone()))
        .expect(1)
        .mount(&server)
        .await;
    let origin = s3_origin_at(&server.uri()).await?;

    let req = OriginRangeRequest {
        fetch_start: 0,
        fetch_end: 16 * 1024,
    };
    match origin.fetch_range_data(hash, req).await? {
        OriginRangeFetch::Ranged { data } => {
            anyhow::ensure!(data.as_ref() == span.as_slice(), "span mismatch");
        }
        OriginRangeFetch::NotFound => anyhow::bail!("expected Ranged, got NotFound"),
    }
    Ok(())
}

/// A ranged answer that carries a `Content-Encoding` parsed off the wire is a
/// permanent fault: encoded bytes break the offset→byte mapping the bao proof
/// anchors on.
#[tokio::test]
async fn s3_origin_refuses_an_encoded_range_over_http() -> anyhow::Result<()> {
    let hash = Hash::new(b"encoded-range");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{BUCKET}/{}", key_for(hash))))
        .respond_with(
            ResponseTemplate::new(206)
                .insert_header("Content-Encoding", "gzip")
                .set_body_bytes(vec![0u8; 1024]),
        )
        .mount(&server)
        .await;
    let origin = s3_origin_at(&server.uri()).await?;

    let req = OriginRangeRequest {
        fetch_start: 0,
        fetch_end: 1024,
    };
    match origin.fetch_range_data(hash, req).await {
        Err(OriginPullError::Permanent(e)) => anyhow::ensure!(
            format!("{e:#}").contains("Content-Encoding"),
            "the fault must name the encoding: {e:#}"
        ),
        other => anyhow::bail!("expected a Permanent fault, got {other:?}"),
    }
    Ok(())
}

/// Full round-trip through the cache engine over the real HTTP path: the
/// engine's presence walk reads the S3 origin's `{key}.sizeproof` once, the
/// proof verifies, and a later presence and size proof lookup reads nothing
/// more.
#[tokio::test]
async fn cache_engine_reads_the_size_proof_over_http_once() -> anyhow::Result<()> {
    let blob = make_blob(40 * 1024);
    let hash = Hash::new(&blob);
    let size = u64::try_from(blob.len())?;
    let encoded = decdn_bao_range::encode_outboard(blob.as_slice(), size)?;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!(
            "/{BUCKET}/{}{}",
            key_for(hash),
            decdn_bao_range::SIZE_PROOF_SUFFIX
        )))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(encoded.size_proof_object()))
        .expect(1)
        .mount(&server)
        .await;

    let origin: Arc<dyn Origin> = Arc::new(s3_origin_at(&server.uri()).await?);
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 16).await?;

    anyhow::ensure!(
        engine.origin_presence(hash).await == OriginPresence::Present(size),
        "a verifying size proof must answer Present(size)"
    );
    anyhow::ensure!(
        engine.size_proof(hash, size).await.is_some(),
        "the presence walk keeps the proof"
    );
    anyhow::ensure!(
        engine.origin_presence(hash).await == OriginPresence::Present(size),
        "the memoised answer must stand"
    );
    // `expect(1)` on the mock asserts the single HTTP dispatch on drop.
    Ok(())
}
