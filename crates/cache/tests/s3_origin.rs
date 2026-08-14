//! Integration tests for [`decdn_cache::S3Origin`] using `aws-smithy-mocks`.
//!
//! The mocks crate hands us a fully-wired `aws_sdk_s3::Client` whose request
//! dispatcher returns canned responses while every other layer (signing,
//! body framing, modeled-error parsing) runs end-to-end. The SDK's
//! internal retry layer is **disabled** in every test via
//! [`mock_s3_client`] / [`mock_s3_client_match_any`], matching what
//! `S3Origin::new` configures (`RetryConfig::disabled()`). This keeps
//! the cache engine's `cache.origin_retry` policy as the single source
//! of retry budget and makes per-test dispatch counts deterministic.
//!
//! See `crates/cache/tests/pull_through.rs` for the equivalent `wiremock`
//! suite covering [`decdn_cache::HttpOrigin`].

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::types::error::NoSuchKey;
use aws_smithy_mocks::{Rule, RuleMode, mock, mock_client};
use aws_smithy_types::byte_stream::ByteStream;
use aws_smithy_types::retry::RetryConfig;
use decdn_cache::{
    CacheEngine, CacheError, DecompressMode, Hash, Origin, OriginError, OriginFetch,
    OriginPullError, RetryPolicy, S3Origin, SupportedEncoding,
};

/// Bucket and prefix used across tests. Matching constants on every rule
/// keep the test setup terse and the failure messages easy to read.
const BUCKET: &str = "decdn-blobs";

/// Build a mock-backed S3 client with the SDK's internal retry layer
/// **disabled**, matching what `S3Origin::new` configures (via
/// `aws_config::ConfigLoader::retry_config(RetryConfig::disabled())`).
/// Without this, `mock_client!`'s default would re-enable SDK retries and
/// every `Transient` test would silently observe extra dispatches.
///
/// The integration suite uses `S3Origin::from_parts` to inject the mock
/// client, which skips `S3Origin::new`'s SDK config wiring — so the
/// retry-disable has to be re-applied here on the mock side.
fn mock_s3_client(rules: &[&Rule]) -> Client {
    mock_client!(aws_sdk_s3, RuleMode::Sequential, rules, |conf| {
        conf.retry_config(RetryConfig::disabled())
    })
}

/// Same as [`mock_s3_client`] but with `RuleMode::MatchAny` for tests
/// that key dispatch on `match_requests` predicates rather than rule
/// ordering.
fn mock_s3_client_match_any(rules: &[&Rule]) -> Client {
    mock_client!(aws_sdk_s3, RuleMode::MatchAny, rules, |conf| {
        conf.retry_config(RetryConfig::disabled())
    })
}

/// Build an `S3Origin` over a mock-backed client. The bucket is fixed to
/// [`BUCKET`]; the prefix can vary so we can exercise prefix application.
fn s3_origin(client: Client, prefix: &str) -> S3Origin {
    S3Origin::from_parts(client, BUCKET, prefix)
}

/// Convert a hash to its expected sharded S3 key. Mirrors `S3Origin`'s
/// `key_for` so a regression in the layout would fail this helper rather
/// than silently mis-assert across multiple tests.
fn expected_key(prefix: &str, hash: Hash) -> String {
    let hex = hash.to_hex();
    let shard = hex.get(..2).unwrap_or("");
    format!("{prefix}{shard}/{}", hex.as_str())
}

/// Cache-miss happy path: SDK returns the requested bytes. We don't go
/// through `CacheEngine::get` here — the engine's miss-then-cache contract
/// is already covered by `pull_through.rs` against `HttpOrigin`. This test
/// scope is the S3 backend's own protocol surface.
#[tokio::test]
async fn fetch_returns_origin_bytes_on_success() -> anyhow::Result<()> {
    let payload: &[u8] = b"hello, decdn from s3";
    let hash = Hash::new(payload);
    let key = expected_key("", hash);
    let key_for_match = key.clone();

    let rule = mock!(Client::get_object)
        .match_requests(move |req| {
            req.bucket() == Some(BUCKET) && req.key() == Some(&key_for_match)
        })
        .then_output(|| {
            GetObjectOutput::builder()
                .body(ByteStream::from_static(b"hello, decdn from s3"))
                .build()
        });
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "");

    let bytes = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected Found, got NotFound"))?;
    anyhow::ensure!(&bytes[..] == payload, "got: {bytes:?}");
    anyhow::ensure!(rule.num_calls() == 1, "single fetch must hit the rule once");
    Ok(())
}

/// `NoSuchKey` (the modeled S3 not-found error) maps to `OriginFetch::NotFound`.
/// This is the canonical absent-object signal — the cache engine relies on
/// it returning `NotFound` rather than `Permanent` so a missing blob lookup
/// surfaces as `CacheError::NotFound`, not `CacheError::OriginError`.
#[tokio::test]
async fn fetch_no_such_key_maps_to_not_found() -> anyhow::Result<()> {
    let hash = Hash::new(b"absent");

    let rule = mock!(Client::get_object)
        .then_error(|| GetObjectError::NoSuchKey(NoSuchKey::builder().build()));
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "");

    match origin.fetch(hash, 16 * 1024 * 1024).await? {
        OriginFetch::NotFound => Ok(()),
        OriginFetch::Found { .. } => anyhow::bail!("expected NotFound on NoSuchKey"),
        OriginFetch::AlreadyAdmitted => {
            anyhow::bail!("S3Origin never admits directly; expected NotFound on NoSuchKey")
        }
    }
}

/// A bare HTTP 404 with no AWS error code also maps to `NotFound`.
/// Some non-AWS S3 endpoints have been observed to emit a 404 without
/// the modeled `NoSuchKey` XML body (no error code field set), so the
/// status-only fallback is load-bearing on those backends. Critically,
/// the fallback is **gated on an empty / `NoSuchKey` error code** — a
/// 404 with `NoSuchBucket` or `AccessDenied` (covered in their own
/// tests below) bypasses this branch and surfaces as `Permanent`.
#[tokio::test]
async fn fetch_bare_http_404_maps_to_not_found() -> anyhow::Result<()> {
    let hash = Hash::new(b"absent");

    let rule = mock!(Client::get_object)
        .sequence()
        .http_status(404, None)
        .build();
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "");

    match origin.fetch(hash, 16 * 1024 * 1024).await? {
        OriginFetch::NotFound => Ok(()),
        OriginFetch::Found { .. } => anyhow::bail!("expected NotFound on HTTP 404"),
        OriginFetch::AlreadyAdmitted => {
            anyhow::bail!("S3Origin never admits directly; expected NotFound on HTTP 404")
        }
    }
}

/// A 404 with `<Code>NoSuchBucket</Code>` in the AWS error body is a
/// **misconfigured-bucket** failure, not an absent-object failure. The
/// classifier must surface it as `Permanent` rather than fold it into
/// `NotFound` — otherwise an operator with a typo'd `bucket = "..."`
/// in their config would see every cache miss report "blob X is
/// missing" with no diagnostic, and chase BLAKE3 hashes for hours.
#[tokio::test]
async fn fetch_404_with_no_such_bucket_is_permanent_not_not_found() -> anyhow::Result<()> {
    let hash = Hash::new(b"any");
    // S3 XML error body. The SDK parses `<Code>NoSuchBucket</Code>`
    // into `ProvideErrorMetadata::code() == Some("NoSuchBucket")` even
    // though `NoSuchBucket` is not a modeled variant on `GetObjectError`
    // — that's exactly the masking case the classifier guards against.
    let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message>\
<BucketName>typo-bucket</BucketName></Error>"
        .to_string();
    let rule = mock!(Client::get_object)
        .sequence()
        .http_status(404, Some(body))
        .build();
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "");

    // Match on the result directly — `?` would propagate the Err that
    // we're trying to inspect.
    match origin.fetch(hash, 16 * 1024 * 1024).await {
        Ok(OriginFetch::NotFound) => anyhow::bail!(
            "NoSuchBucket masked as NotFound — operator would see 'missing blob' \
             instead of the real config error. fix in classify_get_object_error."
        ),
        Ok(OriginFetch::Found { .. }) => anyhow::bail!("expected error, got Found"),
        Ok(OriginFetch::AlreadyAdmitted) => {
            anyhow::bail!("S3Origin never admits directly; expected error, got AlreadyAdmitted")
        }
        Err(err) => {
            anyhow::ensure!(
                matches!(err, OriginPullError::Permanent(_)),
                "NoSuchBucket must be Permanent, got: {err:?}"
            );
            let msg = format!("{err:#}");
            anyhow::ensure!(
                msg.contains("NoSuchBucket"),
                "error context lost the AWS error code; would make ops debugging harder: {msg}"
            );
            Ok(())
        }
    }
}

/// A 404 with `<Code>AccessDenied</Code>` is what AWS S3 returns when
/// the principal lacks `s3:ListBucket` and the object is missing — the
/// service hides the existence/permission distinction by returning 404
/// instead of 403. The classifier must NOT fold this into `NotFound`,
/// or every cache miss against a misconfigured IAM role would silently
/// report "missing blob" with no log line surfacing the real cause.
#[tokio::test]
async fn fetch_404_with_access_denied_is_permanent_not_not_found() -> anyhow::Result<()> {
    let hash = Hash::new(b"forbidden");
    let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
        .to_string();
    let rule = mock!(Client::get_object)
        .sequence()
        .http_status(404, Some(body))
        .build();
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "");

    let err = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await
        .err()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "404 + AccessDenied masked as NotFound — IAM misconfig would silently \
                 report 'missing blob'. fix in classify_get_object_error."
            )
        })?;
    anyhow::ensure!(
        matches!(err, OriginPullError::Permanent(_)),
        "AccessDenied must be Permanent, got: {err:?}"
    );
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("AccessDenied"),
        "error context lost the AWS error code: {msg}"
    );
    Ok(())
}

/// 5xx errors are transient and surface to the cache engine after **a
/// single dispatch** — `S3Origin::new` configures the SDK with
/// `RetryConfig::disabled()` so the SDK does not retry internally. The
/// cache engine's outer `RetryPolicy` (config: `cache.origin_retry`) is
/// the single source of retry budget, matching the operator-facing
/// contract from #285. Pinning `num_calls() == 1` catches a regression
/// where a future SDK upgrade or feature-flag edit re-enables internal
/// retry — that would silently inflate per-fetch dispatches by up to 3x
/// (SDK default = 3 attempts) layered under the outer policy.
#[tokio::test]
async fn fetch_5xx_classifies_as_transient_with_no_internal_retry() -> anyhow::Result<()> {
    let hash = Hash::new(b"transient");

    let rule = mock!(Client::get_object)
        .sequence()
        .http_status(503, None)
        .build();
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "");

    let err = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("503 should produce a Transient error"))?;
    anyhow::ensure!(
        matches!(err, OriginPullError::Transient(_)),
        "expected Transient for 5xx, got: {err:?}"
    );
    anyhow::ensure!(
        rule.num_calls() == 1,
        "SDK internal retry must be disabled; expected 1 dispatch, got {}",
        rule.num_calls()
    );
    Ok(())
}

/// End-to-end through the cache engine: 5xx then success. This time the
/// retry happens via `cache.origin_retry` (the outer `RetryPolicy`), not
/// via the SDK. With the engine's default policy (3 retries), the cache
/// drives 3 total dispatches against the mock — proving (a) the
/// `Transient` classification from `S3Origin::fetch` correctly engages
/// `retry_fetch`, and (b) the SDK's own retry layer is disabled (a 12-
/// dispatch storm would fail the `num_calls() == 3` assertion).
#[tokio::test]
async fn cache_engine_retries_transient_via_origin_retry_policy() -> anyhow::Result<()> {
    let payload: &[u8] = b"recovered after engine retry";
    let hash = Hash::new(payload);

    let rule = mock!(Client::get_object)
        .sequence()
        .http_status(503, None)
        .times(2)
        .output(|| {
            GetObjectOutput::builder()
                .body(ByteStream::from_static(b"recovered after engine retry"))
                .build()
        })
        .build();
    let client = mock_s3_client(&[&rule]);
    let origin: Arc<dyn Origin> = Arc::new(s3_origin(client, ""));

    let tmp = tempfile::tempdir()?;
    // Use a fast policy so the test doesn't spend wall-time in jittered
    // backoff sleeps — three retries with 1ms initial / 4ms cap is
    // milliseconds total even with the jitter envelope.
    let policy = RetryPolicy {
        max_retries: 3,
        initial_backoff_ms: 1,
        max_backoff_ms: 4,
        jitter_ratio: 0.0,
        // Disable the buffered drain path: this test pins the SDK's
        // first-attempt classification of a 503 service error,
        // which is a headers-phase concern. Buffering would not
        // change behaviour but adds noise to the failure mode under
        // test if the mocked GetObject ever streams a body.
        buffered_max_bytes: 0,
    };
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![origin as Arc<dyn Origin>],
        16,
        decdn_cache::PinnedHashes::empty(),
        policy,
        decdn_cache::CircuitBreakerPolicy::default(),
        None,
        Duration::ZERO,
    )
    .await?;

    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload, "got: {got:?}");
    // 2 transients + 1 success = 3 dispatches. If the SDK's internal
    // retry slipped back on, we'd see 9 (3 × 3) dispatches instead.
    anyhow::ensure!(
        rule.num_calls() == 3,
        "expected 3 outer-policy retries (2 transient + success); SDK internal retry may have leaked back on. got {}",
        rule.num_calls()
    );
    Ok(())
}

/// 4xx errors (other than 404) are permanent. The SDK does *not* retry
/// 400/403/etc., so we observe exactly one call and the error surfaces
/// to the cache engine immediately. Pinning `num_calls() == 1` catches
/// a regression where a future SDK or feature flag change would start
/// retrying these — turning what should be fail-fast into "wait three
/// retries before failing".
#[tokio::test]
async fn fetch_4xx_classifies_as_permanent_and_does_not_retry() -> anyhow::Result<()> {
    let hash = Hash::new(b"forbidden");

    let rule = mock!(Client::get_object)
        .sequence()
        .http_status(403, None)
        .build();
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "");

    let err = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("4xx should produce an error"))?;
    anyhow::ensure!(
        matches!(err, OriginPullError::Permanent(_)),
        "expected Permanent for 4xx, got: {err:?}"
    );
    anyhow::ensure!(
        rule.num_calls() == 1,
        "permanent 4xx must not be retried by the SDK; got {} calls",
        rule.num_calls()
    );
    Ok(())
}

/// Compress `payload` with gzip for the decode tests. Mirrors the
/// `pull_through.rs` helper so the two backends are exercised against the
/// same canonical/compressed pairs.
fn gzip(payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    e.write_all(payload)?;
    Ok(e.finish()?)
}

fn zstd_compress(payload: &[u8]) -> anyhow::Result<Vec<u8>> {
    Ok(zstd::stream::encode_all(payload, 1)?)
}

/// `Content-Encoding: gzip` is transparently decompressed in the default
/// `Auto` mode (#804) — parity with the HTTP backend. The BLAKE3 verify in
/// the engine runs over canonical bytes, so the adapter must hand back the
/// decompressed payload, not the gzipped wire bytes.
#[tokio::test]
async fn fetch_with_content_encoding_gzip_is_decompressed() -> anyhow::Result<()> {
    let payload: &[u8] = b"hello, gzipped s3 world! repeat repeat repeat repeat";
    let hash = Hash::new(payload);
    let compressed = gzip(payload)?;

    let rule = mock!(Client::get_object).then_output(move || {
        GetObjectOutput::builder()
            .body(ByteStream::from(compressed.clone()))
            .content_encoding("gzip")
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "");

    let bytes = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected Found, got NotFound"))?;
    anyhow::ensure!(
        &bytes[..] == payload,
        "gzip body should decode to canonical payload"
    );
    Ok(())
}

/// `Content-Encoding: zstd` is transparently decompressed in `Auto` mode.
#[tokio::test]
async fn fetch_with_content_encoding_zstd_is_decompressed() -> anyhow::Result<()> {
    let payload: &[u8] = b"hello, zstd s3! and a longer body to compress meaningfully xxxxx";
    let hash = Hash::new(payload);
    let compressed = zstd_compress(payload)?;

    let rule = mock!(Client::get_object).then_output(move || {
        GetObjectOutput::builder()
            .body(ByteStream::from(compressed.clone()))
            .content_encoding("zstd")
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "");

    let bytes = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected Found, got NotFound"))?;
    anyhow::ensure!(&bytes[..] == payload, "zstd body should decode to payload");
    Ok(())
}

/// `DecompressMode::Strict` refuses a known encoding before any body
/// bytes are read — equivalent to the pre-#804 reject-everything posture,
/// for operators whose origin is guaranteed to serve canonical bytes.
#[tokio::test]
async fn fetch_with_strict_mode_rejects_gzip_encoding() -> anyhow::Result<()> {
    let hash = Hash::new(b"gzipped-strict");

    let rule = mock!(Client::get_object).then_output(|| {
        GetObjectOutput::builder()
            .body(ByteStream::from_static(
                b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03",
            ))
            .content_encoding("gzip")
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "").with_decompress_mode(DecompressMode::Strict);

    let err = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("strict mode must reject a compressed body"))?;
    let msg = match &err {
        OriginPullError::Permanent(e) => format!("{e:#}"),
        OriginPullError::Transient(e) => {
            anyhow::bail!("Content-Encoding rejection must be Permanent, was Transient: {e:#}");
        }
    };
    // Operators grep for these substrings in runbooks; pin the contract.
    anyhow::ensure!(
        msg.contains("Content-Encoding") && msg.contains("canonical bytes"),
        "strict rejection lost its actionable runbook hint: {msg}"
    );
    Ok(())
}

/// An unknown `Content-Encoding` (e.g. Brotli) is a permanent error in
/// either mode — the adapter has no decoder for it, and passing the bytes
/// through would later trip a confusing `HashMismatch`.
#[tokio::test]
async fn fetch_with_unknown_encoding_is_permanent() -> anyhow::Result<()> {
    let hash = Hash::new(b"brotli-body");

    let rule = mock!(Client::get_object).then_output(|| {
        GetObjectOutput::builder()
            .body(ByteStream::from_static(b"\x00\x01\x02brotli-ish"))
            .content_encoding("br")
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "");

    let err = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("unknown encoding must be rejected"))?;
    let msg = match &err {
        OriginPullError::Permanent(e) => format!("{e:#}"),
        OriginPullError::Transient(e) => {
            anyhow::bail!("unknown-encoding rejection must be Permanent, was Transient: {e:#}");
        }
    };
    anyhow::ensure!(
        msg.contains("Content-Encoding") && msg.contains("br"),
        "error should name the unsupported encoding: {msg}"
    );
    Ok(())
}

/// `identity` and empty Content-Encoding both pass through as no-ops.
/// AWS S3 itself does not normally set `Content-Encoding: identity`, but
/// some operator tooling adds the explicit header, and the engine must
/// not reject it.
#[tokio::test]
async fn fetch_with_content_encoding_identity_is_accepted() -> anyhow::Result<()> {
    let payload: &[u8] = b"plain bytes";
    let hash = Hash::new(payload);

    let rule = mock!(Client::get_object).then_output(|| {
        GetObjectOutput::builder()
            .body(ByteStream::from_static(b"plain bytes"))
            .content_encoding("identity")
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "");

    let bytes = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected Found"))?;
    anyhow::ensure!(&bytes[..] == payload);
    Ok(())
}

/// The S3 adapter's body stream produces every byte the wire
/// emits — no in-adapter cap, no silent truncation. Post-#271 the
/// `max_bytes` enforcement moved to the engine's
/// `count_and_cap_stream`, so this test confirms only the adapter
/// half of the contract: the bytes flow through verbatim. The
/// **engine-level** rejection of oversized bodies is covered
/// end-to-end by `cache_engine_miss_pulls_from_s3_and_caches`.
/// An origin lying in `Content-Length` (or omitting it for chunked
/// responses) is caught by the engine's running cap, which is the
/// load-bearing defense.
#[tokio::test]
async fn fetch_oversize_body_streams_through_for_engine_cap() -> anyhow::Result<()> {
    // 256 KiB payload, cap at 64 KiB. The SDK's mock layer doesn't
    // set Content-Length unless we provide one explicitly, so this
    // exercises the running cap on the streamed bytes rather than
    // the fast-path `content_length()` short-circuit.
    let payload = vec![0xABu8; 256 * 1024];
    let hash = Hash::new(&payload);
    let body = payload.clone();

    let rule = mock!(Client::get_object).then_output(move || {
        GetObjectOutput::builder()
            .body(ByteStream::from(body.clone()))
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "");

    // Adapter fetch returns the stream (no cap at the adapter).
    let bytes = origin
        .fetch(hash, 64 * 1024)
        .await?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected Found"))?;
    anyhow::ensure!(
        bytes.len() == payload.len(),
        "stream truncated: {} bytes vs {} in payload",
        bytes.len(),
        payload.len()
    );
    Ok(())
}

/// A non-empty prefix is applied to the request key. The rule's
/// `match_requests` predicate enforces the exact key shape, so a
/// regression in `key_for` (e.g. forgetting to apply the prefix, or
/// double-applying the trailing slash) would surface here as a
/// "rule did not match" failure.
#[tokio::test]
async fn fetch_applies_prefix_with_sharded_key_layout() -> anyhow::Result<()> {
    let payload: &[u8] = b"prefixed bytes";
    let hash = Hash::new(payload);
    let prefix = "blobs/";
    let key = expected_key(prefix, hash);
    let key_for_match = key.clone();

    let rule = mock!(Client::get_object)
        .match_requests(move |req| {
            req.bucket() == Some(BUCKET) && req.key() == Some(&key_for_match)
        })
        .then_output(|| {
            GetObjectOutput::builder()
                .body(ByteStream::from_static(b"prefixed bytes"))
                .build()
        });
    // Default `Sequential` mode would still match because there's only
    // one rule; using `MatchAny` here keeps the failure message clear
    // ("no rule matched the bucket/key" beats "sequence position out of
    // range") if a future regression breaks the key layout.
    let client = mock_s3_client_match_any(&[&rule]);
    let origin = s3_origin(client, prefix);

    let bytes = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await?
        .collect_to_bytes()
        .await?
        .ok_or_else(|| anyhow::anyhow!("expected Found"))?;
    anyhow::ensure!(&bytes[..] == payload);
    anyhow::ensure!(rule.num_calls() == 1, "rule must match the prefixed key");
    Ok(())
}

/// End-to-end through the cache engine: a miss pulls from the S3 origin,
/// the BLAKE3 verify passes, the bytes are cached, and a second `get`
/// is served locally. Mirrors `pull_through::cache_miss_pulls_from_origin_and_caches`
/// for the S3 backend.
#[tokio::test]
async fn cache_engine_miss_pulls_from_s3_and_caches() -> anyhow::Result<()> {
    let payload: &[u8] = b"engine-pull through s3";
    let hash = Hash::new(payload);

    // Single-output rule: if the engine accidentally re-fetches on the
    // second `get` (regressing the cache-then-serve contract), the SDK
    // would attempt a second dispatch and the `num_calls() == 1`
    // assertion below would fail.
    let rule = mock!(Client::get_object).then_output(|| {
        GetObjectOutput::builder()
            .body(ByteStream::from_static(b"engine-pull through s3"))
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin: Arc<dyn Origin> = Arc::new(s3_origin(client, ""));

    let tmp = tempfile::tempdir()?;
    // Disable our outer retry policy so the test surfaces a deterministic
    // call count. The SDK's internal retry layer is also disabled (see
    // `mock_s3_client`), so the call count reflects neither layer adding
    // attempts on a 200-OK happy path.
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![origin as Arc<dyn Origin>],
        16,
        decdn_cache::PinnedHashes::empty(),
        RetryPolicy::disabled(),
        decdn_cache::CircuitBreakerPolicy::default(),
        None,
        Duration::ZERO,
    )
    .await?;

    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload, "first get must return origin bytes");
    anyhow::ensure!(engine.has(hash).await?, "blob must be cached after miss");

    let got2 = engine.get(hash).await?;
    anyhow::ensure!(
        &got2[..] == payload,
        "second get must be served from the local cache"
    );
    anyhow::ensure!(
        rule.num_calls() == 1,
        "second get must not re-hit the origin; got {} dispatches",
        rule.num_calls()
    );
    Ok(())
}

/// End-to-end `BlobTooLarge` through the S3 backend: the engine's
/// `count_and_cap_stream` running cap on streamed bytes catches an
/// origin that delivers more than `max_blob_bytes`. Without this
/// test the S3 path could regress (e.g., re-buffer in the adapter,
/// or skip the engine wrapper) and the fix for issue #271 wouldn't
/// actually constrain S3-fed pulls. HTTP has the equivalent
/// (`mid_stream_overrun_is_rejected_by_http_origin`); this is the
/// S3 counterpart.
#[tokio::test]
async fn cache_engine_rejects_s3_body_larger_than_max_blob_bytes() -> anyhow::Result<()> {
    // 4 MiB body, 1 MiB cap. The mock layer doesn't set
    // Content-Length unless we provide one explicitly, so this
    // exercises the running cap on streamed bytes (engine's
    // `count_and_cap_stream`) rather than the adapter's
    // pre-stream Content-Length short-circuit.
    let payload = vec![0xCDu8; 4 * 1024 * 1024];
    let hash = Hash::new(&payload);
    let body = payload.clone();
    let rule = mock!(Client::get_object).then_output(move || {
        GetObjectOutput::builder()
            .body(ByteStream::from(body.clone()))
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin: Arc<dyn Origin> = Arc::new(s3_origin(client, ""));

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![origin as Arc<dyn Origin>],
        1, // max_blob_size_mb = 1 MiB
        decdn_cache::PinnedHashes::empty(),
        RetryPolicy::disabled(),
        decdn_cache::CircuitBreakerPolicy::default(),
        None,
        Duration::ZERO,
    )
    .await?;

    let err = engine
        .get(hash)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("oversize body must be rejected at engine cap"))?;
    anyhow::ensure!(
        matches!(err, CacheError::BlobTooLarge { .. }),
        "expected BlobTooLarge from engine cap, got: {err:?}"
    );
    Ok(())
}

/// Truncated gzip body fed through the S3 backend: the decoder emits an
/// `io::Error` wrapping a typed `OriginError::DecompressionFailed`, which
/// `classify_io_error` recognises as Permanent. The retry budget must not
/// be burned even with retries available, and the typed variant must
/// survive the S3-specific `ByteStream::into_async_read -> ReaderStream ->
/// decode_stream` seam — the highest-risk path, since no other test
/// exercises a decoder failure through the SDK body adapter. Mirrors
/// `pull_through::decompression_failure_is_permanent_under_threshold`.
#[tokio::test]
async fn cache_engine_s3_truncated_gzip_is_permanent_decompression_failed() -> anyhow::Result<()> {
    let payload: &[u8] = b"truncate me, please, but only the gzip wrapper xxxxxxxxxxxx";
    let hash = Hash::new(payload);
    let mut compressed = gzip(payload)?;
    // Lop off the gzip trailer (CRC + ISIZE) so the decoder errors after
    // the magic/header check passes rather than rejecting up front.
    let drop = compressed.len().saturating_sub(20);
    compressed.truncate(drop);

    let rule = mock!(Client::get_object).then_output(move || {
        GetObjectOutput::builder()
            .body(ByteStream::from(compressed.clone()))
            .content_encoding("gzip")
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin: Arc<dyn Origin> = Arc::new(s3_origin(client, ""));

    let tmp = tempfile::tempdir()?;
    // Fast policy with retries available: the assertion is that the
    // decoder error is Permanent, so the budget must go unspent.
    let policy = RetryPolicy {
        max_retries: 3,
        initial_backoff_ms: 1,
        max_backoff_ms: 4,
        jitter_ratio: 0.0,
        buffered_max_bytes: 4 << 20,
    };
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![origin as Arc<dyn Origin>],
        16,
        decdn_cache::PinnedHashes::empty(),
        policy,
        decdn_cache::CircuitBreakerPolicy::default(),
        None,
        Duration::ZERO,
    )
    .await?;

    let err = engine
        .get(hash)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("truncated gzip must be rejected"))?;
    anyhow::ensure!(
        matches!(err, CacheError::OriginError { .. }),
        "expected OriginError from truncated gzip, got: {err:?}"
    );
    anyhow::ensure!(
        matches!(
            err.origin_error_kind(),
            Some(OriginError::DecompressionFailed {
                encoding: SupportedEncoding::Gzip,
                ..
            })
        ),
        "expected typed DecompressionFailed(Gzip) on the error chain, got: {err:?}"
    );
    anyhow::ensure!(
        rule.num_calls() == 1,
        "decoder failure is Permanent; retry must not re-dispatch, got {}",
        rule.num_calls()
    );
    Ok(())
}

/// Decompression bomb through the S3 backend: a small gzip payload that
/// decodes to far more than `max_blob_bytes`. The decoded-side cap lives
/// in the engine's `count_and_cap_stream`, so the bomb must surface as
/// `BlobTooLarge` rather than pinning the decoded bytes in memory. The
/// only existing S3 `BlobTooLarge` test uses an *uncompressed* body, so
/// this is the first to prove the cap runs over the decoded stream and
/// that the small compressed `Content-Length` doesn't bypass it. Mirrors
/// `pull_through::http_origin_rejects_decompression_bomb`.
#[tokio::test]
async fn cache_engine_s3_rejects_decompression_bomb() -> anyhow::Result<()> {
    // 4 MiB of zeros gzips to a few KiB; cap at 1 MiB. Neither the
    // Content-Length fast-path (KiB encoded) nor any adapter cap catches
    // this — only the running total over decoded bytes does.
    let payload = vec![0u8; 4 * 1024 * 1024];
    let hash = Hash::new(&payload);
    let compressed = gzip(&payload)?;
    anyhow::ensure!(
        compressed.len() < 1024 * 1024,
        "test setup: compressed body must be smaller than the cap to \
         exercise the running-total check, was {} bytes",
        compressed.len()
    );
    let content_length = i64::try_from(compressed.len())?;
    let rule = mock!(Client::get_object).then_output(move || {
        GetObjectOutput::builder()
            .body(ByteStream::from(compressed.clone()))
            .content_encoding("gzip")
            .content_length(content_length)
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin: Arc<dyn Origin> = Arc::new(s3_origin(client, ""));

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(
        tmp.path(),
        vec![origin as Arc<dyn Origin>],
        1, // max_blob_size_mb = 1 MiB
    )
    .await?;

    let err =
        engine.get(hash).await.err().ok_or_else(|| {
            anyhow::anyhow!("decompression bomb must be rejected at the decoded cap")
        })?;
    anyhow::ensure!(
        matches!(err, CacheError::BlobTooLarge { .. }),
        "expected BlobTooLarge from bomb cap, got: {err:?}"
    );
    Ok(())
}

/// BLAKE3 verify must run over the *decompressed* form: the engine is
/// asked for the hash of the *compressed* wire bytes against a gzip body,
/// and the decoded body's different hash must surface as `HashMismatch`
/// (proving the verify saw canonical, not compressed, bytes on the S3
/// path). Mirrors `pull_through::http_origin_blake3_verify_runs_over_decompressed_bytes`.
#[tokio::test]
async fn cache_engine_s3_blake3_verify_runs_over_decompressed_bytes() -> anyhow::Result<()> {
    let payload: &[u8] = b"verify-after-decompress on the s3 path, not before";
    let canonical = Hash::new(payload);
    let compressed = gzip(payload)?;
    // Hash of the *compressed* bytes — what a regression that hashed the
    // raw wire bytes would match.
    let raw_hash = Hash::new(&compressed);
    anyhow::ensure!(canonical != raw_hash, "test premise: hashes differ");

    let rule = mock!(Client::get_object).then_output(move || {
        GetObjectOutput::builder()
            .body(ByteStream::from(compressed.clone()))
            .content_encoding("gzip")
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin: Arc<dyn Origin> = Arc::new(s3_origin(client, ""));

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 16).await?;

    // Asking for the raw-bytes hash: the decoded body has a different
    // BLAKE3 → the engine surfaces HashMismatch.
    let err =
        engine.get(raw_hash).await.err().ok_or_else(|| {
            anyhow::anyhow!("compressed-hash request must mismatch the decoded body")
        })?;
    anyhow::ensure!(
        matches!(err, CacheError::HashMismatch { .. }),
        "expected HashMismatch (verify ran over decompressed form), got: {err:?}"
    );
    Ok(())
}

/// Regression for #804: a compressed body whose *encoded* length fits
/// under `buffered_max_bytes` but whose *decoded* length exceeds it, while
/// still under `max_blob_bytes`, must succeed. Before the fix the adapter
/// reported the encoded `Content-Length` as `size_hint`, routing the body
/// into the buffer/drain path whose cap (`buffered_max_bytes`) is applied
/// to the *decoded* stream — falsely rejecting an in-bounds blob as
/// `BlobTooLarge`. The fix hands `None` for compressed bodies so they take
/// the streaming path capped at `max_blob_bytes`. The explicit
/// `content_length` is load-bearing: without it `size_hint` would already
/// be `None` and the bug would not reproduce.
#[tokio::test]
async fn cache_engine_s3_compressed_above_buffer_threshold_succeeds() -> anyhow::Result<()> {
    // Decodes to 8 MiB: above the 4 MiB default `buffered_max_bytes`,
    // below the 16 MiB `max_blob_size`.
    let payload = vec![0u8; 8 * 1024 * 1024];
    let hash = Hash::new(&payload);
    let compressed = gzip(&payload)?;
    anyhow::ensure!(
        compressed.len() < 4 * 1024 * 1024,
        "test setup: encoded length must be under buffered_max_bytes (4 MiB) \
         to route into the drain path pre-fix, was {} bytes",
        compressed.len()
    );
    let content_length = i64::try_from(compressed.len())?;
    let rule = mock!(Client::get_object).then_output(move || {
        GetObjectOutput::builder()
            .body(ByteStream::from(compressed.clone()))
            .content_encoding("gzip")
            .content_length(content_length)
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin: Arc<dyn Origin> = Arc::new(s3_origin(client, ""));

    let tmp = tempfile::tempdir()?;
    // Default policy → buffered_max_bytes = 4 MiB; max_blob_size = 16 MiB.
    let engine = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 16).await?;

    let got = engine.get(hash).await?;
    anyhow::ensure!(
        got.len() == payload.len() && got[..] == payload[..],
        "in-bounds compressed blob must decode and cache, got {} bytes",
        got.len()
    );
    Ok(())
}

/// End-to-end `NotFound`: the engine surfaces `CacheError::NotFound`,
/// not `CacheError::OriginError`. This is the contract the dispatch
/// layer (and clients via the cdn/client/v1 ALPN) depend on for
/// distinguishing "object doesn't exist" from "origin is broken".
#[tokio::test]
async fn cache_engine_surfaces_not_found_for_no_such_key() -> anyhow::Result<()> {
    let hash = Hash::new(b"missing");

    let rule = mock!(Client::get_object)
        .then_error(|| GetObjectError::NoSuchKey(NoSuchKey::builder().build()));
    let client = mock_s3_client(&[&rule]);
    let origin: Arc<dyn Origin> = Arc::new(s3_origin(client, ""));

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin as Arc<dyn Origin>], 16).await?;

    let err = engine
        .get(hash)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected NotFound"))?;
    anyhow::ensure!(
        matches!(err, CacheError::NotFound { .. }),
        "expected CacheError::NotFound, got: {err:?}"
    );
    Ok(())
}

// ----------------------------------------------------------------------------
// Origin range pull-through (#962, ADR 037 §Origin-tier pull-through).
//
// `S3Origin::fetch_range` issues a sibling `{key}.obao4` GET plus a ranged
// data GET. The mock dispatcher keys on the requested key (data vs. outboard)
// and on the presence of a `Range` so we can assert the range-scoped behavior
// and the always-correct degrade when the outboard is absent.
// ----------------------------------------------------------------------------

use bao_tree::io::outboard::PreOrderMemOutboard;
use decdn_cache::range_pull::IROH_BLOCK_SIZE;
use decdn_cache::{OriginRangeFetch, OriginRangeRequest};

fn make_blob(len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

#[tokio::test]
async fn fetch_range_returns_span_and_outboard() -> anyhow::Result<()> {
    let blob = make_blob(200 * 1024);
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let hash = Hash::from_bytes(*ob.root.as_bytes());
    let data_key = expected_key("", hash);
    let obao4_key = format!("{data_key}.obao4");

    // Aligned span [16 KiB, 48 KiB).
    let (start, end) = (16 * 1024usize, 48 * 1024usize);
    let span = blob.get(start..end).unwrap_or_default().to_vec();
    let outboard_bytes = ob.data.clone();

    // Outboard rule: plain GET on `{key}.obao4`.
    let obao4_match = obao4_key.clone();
    let obao4_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&obao4_match) && req.range().is_none())
        .then_output(move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(outboard_bytes.clone()))
                .build()
        });
    // Ranged data rule: GET on `{key}` with a Range header.
    let data_match = data_key.clone();
    let data_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&data_match) && req.range().is_some())
        .then_output(move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(span.clone()))
                .build()
        });
    let client = mock_s3_client_match_any(&[&obao4_rule, &data_rule]);
    let origin = s3_origin(client, "");

    let req = OriginRangeRequest {
        fetch_start: start as u64,
        fetch_end: end as u64,
    };
    match origin.fetch_range(hash, req, 1 << 20).await? {
        OriginRangeFetch::Ranged { data, outboard } => {
            anyhow::ensure!(
                data.as_ref() == blob.get(start..end).unwrap_or_default(),
                "span mismatch",
            );
            anyhow::ensure!(!outboard.is_empty(), "outboard must be served");
        }
        other => anyhow::bail!("expected Ranged, got {other:?}"),
    }
    anyhow::ensure!(obao4_rule.num_calls() == 1, "one outboard GET");
    anyhow::ensure!(data_rule.num_calls() == 1, "one ranged data GET");
    Ok(())
}

#[tokio::test]
async fn fetch_range_missing_outboard_is_unsupported() -> anyhow::Result<()> {
    // The sibling `{key}.obao4` is absent (`NoSuchKey`) → degrade, no data GET.
    let blob = make_blob(64 * 1024);
    let hash = Hash::new(&blob);
    let obao4_key = format!("{}.obao4", expected_key("", hash));

    let obao4_match = obao4_key.clone();
    let obao4_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&obao4_match))
        .then_error(|| GetObjectError::NoSuchKey(NoSuchKey::builder().build()));
    let client = mock_s3_client_match_any(&[&obao4_rule]);
    let origin = s3_origin(client, "");

    let req = OriginRangeRequest {
        fetch_start: 0,
        fetch_end: 16 * 1024,
    };
    anyhow::ensure!(
        matches!(
            origin.fetch_range(hash, req, 1 << 20).await?,
            OriginRangeFetch::Unsupported
        ),
        "missing outboard must degrade to Unsupported",
    );
    Ok(())
}

#[tokio::test]
async fn fetch_range_oversized_outboard_degrades_without_buffering() -> anyhow::Result<()> {
    // OOM guard: a hostile/oversized `{H}.obao4` (origin ignoring the bound, or
    // a foreign object served under the outboard key) must degrade to
    // `Unsupported` WITHOUT buffering the whole body. The mock omits
    // Content-Length (matching real chunked responses), so the
    // `content_length()` pre-check can't short-circuit — this exercises the
    // streaming abort in `collect_bounded`, which stops at the first over-cap
    // chunk instead of draining the body via `ByteStream::collect()`.
    let blob = make_blob(64 * 1024);
    let hash = Hash::new(&blob);
    let obao4_key = format!("{}.obao4", expected_key("", hash));

    // 8 MiB outboard against a 4 KiB cap. Pre-fix this 8 MiB would be fully
    // aggregated before the cap check; post-fix it aborts after ~4 KiB.
    let huge_outboard = vec![0x5Au8; 8 * 1024 * 1024];
    let obao4_match = obao4_key.clone();
    let obao4_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&obao4_match))
        .then_output(move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(huge_outboard.clone()))
                .build()
        });
    let client = mock_s3_client_match_any(&[&obao4_rule]);
    let origin = s3_origin(client, "");

    let req = OriginRangeRequest {
        fetch_start: 0,
        fetch_end: 16 * 1024,
    };
    // Tiny outboard cap forces the over-cap abort path.
    anyhow::ensure!(
        matches!(
            origin.fetch_range(hash, req, 4 * 1024).await?,
            OriginRangeFetch::Unsupported
        ),
        "oversized outboard must degrade to Unsupported",
    );
    anyhow::ensure!(
        obao4_rule.num_calls() == 1,
        "outboard GET issued exactly once",
    );
    Ok(())
}

#[tokio::test]
async fn fetch_range_wrong_length_span_degrades() -> anyhow::Result<()> {
    // Outboard present, but the ranged GET returns a *shorter* body than the
    // requested span (origin ignored Range / truncated). Must degrade, not
    // import a wrong-length span.
    let blob = make_blob(200 * 1024);
    let ob = PreOrderMemOutboard::create(&blob, IROH_BLOCK_SIZE);
    let hash = Hash::from_bytes(*ob.root.as_bytes());
    let data_key = expected_key("", hash);
    let obao4_key = format!("{data_key}.obao4");
    let outboard_bytes = ob.data.clone();

    let obao4_match = obao4_key.clone();
    let obao4_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&obao4_match) && req.range().is_none())
        .then_output(move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(outboard_bytes.clone()))
                .build()
        });
    // Returns only 8 bytes regardless of the 32 KiB requested span.
    let data_match = data_key.clone();
    let data_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&data_match) && req.range().is_some())
        .then_output(|| {
            GetObjectOutput::builder()
                .body(ByteStream::from_static(b"short!!!"))
                .build()
        });
    let client = mock_s3_client_match_any(&[&obao4_rule, &data_rule]);
    let origin = s3_origin(client, "");

    let req = OriginRangeRequest {
        fetch_start: 16 * 1024,
        fetch_end: 48 * 1024,
    };
    anyhow::ensure!(
        matches!(
            origin.fetch_range(hash, req, 1 << 20).await?,
            OriginRangeFetch::Unsupported
        ),
        "wrong-length span must degrade",
    );
    Ok(())
}

// ----------------------------------------------------------------------------
// `S3Origin::size` issues a `HeadObject` to learn the canonical blob length for
// scoping a range pull (#823). A `Content-Encoding` object or a missing key
// yields `None` (degrade to whole-blob); a genuine 5xx surfaces as an error.
// ----------------------------------------------------------------------------

use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::types::error::NotFound;

#[tokio::test]
async fn size_returns_head_object_content_length() -> anyhow::Result<()> {
    let hash = Hash::new(b"size-probe");
    let key = expected_key("", hash);
    let rule = mock!(Client::head_object)
        .match_requests(move |req| req.key() == Some(&key))
        .then_output(|| HeadObjectOutput::builder().content_length(4096).build());
    let client = mock_s3_client_match_any(&[&rule]);
    let origin = s3_origin(client, "");

    anyhow::ensure!(
        origin.size(hash).await? == Some(4096),
        "size must be the HeadObject Content-Length"
    );
    anyhow::ensure!(rule.num_calls() == 1, "exactly one HeadObject");
    Ok(())
}

#[tokio::test]
async fn size_compressed_object_is_unknown() -> anyhow::Result<()> {
    // A `Content-Encoding` HeadObject advertises the encoded length, not the
    // canonical blob size — `size` must degrade to `None`.
    let hash = Hash::new(b"compressed");
    let rule = mock!(Client::head_object).then_output(|| {
        HeadObjectOutput::builder()
            .content_length(1024)
            .content_encoding("gzip")
            .build()
    });
    let client = mock_s3_client_match_any(&[&rule]);
    let origin = s3_origin(client, "");

    anyhow::ensure!(
        origin.size(hash).await?.is_none(),
        "compressed object size must be unknown"
    );
    Ok(())
}

#[tokio::test]
async fn size_missing_object_is_none() -> anyhow::Result<()> {
    let hash = Hash::new(b"absent");
    let rule = mock!(Client::head_object)
        .then_error(|| HeadObjectError::NotFound(NotFound::builder().build()));
    let client = mock_s3_client_match_any(&[&rule]);
    let origin = s3_origin(client, "");

    anyhow::ensure!(
        origin.size(hash).await?.is_none(),
        "missing object size must be None"
    );
    Ok(())
}

// ----------------------------------------------------------------------------
// `S3Origin::fetch_outboard` (#1130) — a standalone `GetObject` on the
// sibling `{key}.obao4`, no accompanying data GET. Mirrors the outboard
// sub-fetch covered above for `fetch_range`, but as its own trait method.
// ----------------------------------------------------------------------------

use decdn_cache::OutboardFetch;

#[tokio::test]
async fn fetch_outboard_returns_sibling_obao4() -> anyhow::Result<()> {
    let hash = Hash::new(b"s3-outboard-marker");
    let obao4_key = format!("{}.obao4", expected_key("", hash));
    let outboard_bytes = vec![0xCDu8; 4096];

    let obao4_match = obao4_key.clone();
    let body = outboard_bytes.clone();
    let obao4_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&obao4_match))
        .then_output(move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(body.clone()))
                .build()
        });
    let client = mock_s3_client_match_any(&[&obao4_rule]);
    let origin = s3_origin(client, "");

    match origin.fetch_outboard(hash, 1 << 20).await? {
        OutboardFetch::Found(bytes) => {
            anyhow::ensure!(
                bytes.as_ref() == outboard_bytes.as_slice(),
                "outboard bytes mismatch"
            );
        }
        other => anyhow::bail!("expected Found, got {other:?}"),
    }
    anyhow::ensure!(obao4_rule.num_calls() == 1, "exactly one outboard GET");
    Ok(())
}

#[tokio::test]
async fn fetch_outboard_missing_sibling_is_not_found() -> anyhow::Result<()> {
    let hash = Hash::new(b"s3-outboard-missing-marker");
    let obao4_key = format!("{}.obao4", expected_key("", hash));

    let obao4_match = obao4_key.clone();
    let obao4_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&obao4_match))
        .then_error(|| GetObjectError::NoSuchKey(NoSuchKey::builder().build()));
    let client = mock_s3_client_match_any(&[&obao4_rule]);
    let origin = s3_origin(client, "");

    anyhow::ensure!(
        matches!(
            origin.fetch_outboard(hash, 1 << 20).await?,
            OutboardFetch::NotFound
        ),
        "missing sibling must be NotFound",
    );
    Ok(())
}

#[tokio::test]
async fn fetch_outboard_oversize_degrades_without_buffering() -> anyhow::Result<()> {
    // Same OOM guard as the `fetch_range` outboard sub-fetch: the mock omits
    // Content-Length (matching real chunked responses) so the pre-check can't
    // short-circuit, exercising the streaming abort in `collect_bounded`.
    let hash = Hash::new(b"s3-outboard-oversize-marker");
    let obao4_key = format!("{}.obao4", expected_key("", hash));
    let huge_outboard = vec![0x5Au8; 8 * 1024 * 1024];

    let obao4_match = obao4_key.clone();
    let obao4_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&obao4_match))
        .then_output(move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(huge_outboard.clone()))
                .build()
        });
    let client = mock_s3_client_match_any(&[&obao4_rule]);
    let origin = s3_origin(client, "");

    anyhow::ensure!(
        matches!(
            origin.fetch_outboard(hash, 4 * 1024).await?,
            OutboardFetch::Unsupported
        ),
        "oversize outboard must degrade to Unsupported",
    );
    anyhow::ensure!(
        obao4_rule.num_calls() == 1,
        "outboard GET issued exactly once"
    );
    Ok(())
}
