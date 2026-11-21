//! Integration tests for [`decdn_cache::S3Origin`] using `aws-smithy-mocks`.
//!
//! The mocks crate hands us a fully-wired `aws_sdk_s3::Client` whose request
//! dispatcher returns canned responses while every other layer (signing,
//! body framing, modeled-error parsing) runs end-to-end. The SDK's
//! internal retry layer is **disabled** in every test via
//! [`mock_s3_client`] / [`mock_s3_client_match_any`], matching what
//! `S3Origin::new` configures (`RetryConfig::disabled()`). This keeps
//! the cache engine as the single source of retry budget and makes
//! per-test dispatch counts deterministic.
//!
//! An S3 origin serves three objects per blob: the data `{key}`, the
//! outboard `{key}.obao4` and the size proof `{key}.sizeproof`. The status
//! classification is shared by every read; the tests below pin it on the
//! size proof read, which is the origin's presence answer.
#![cfg(feature = "s3")]

use std::sync::Arc;

use aws_sdk_s3::Client;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::types::error::NoSuchKey;
use aws_smithy_mocks::{Rule, RuleMode, mock, mock_client};
use aws_smithy_types::byte_stream::ByteStream;
use aws_smithy_types::retry::RetryConfig;
use decdn_cache::{
    CacheEngine, Hash, Origin, OriginPresence, OriginPullError, OriginRangeFetch,
    OriginRangeRequest, OutboardFetch, S3Origin,
};

/// Bucket used across tests. Matching constants on every rule keep the test
/// setup terse and the failure messages easy to read.
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
fn s3_origin(client: Client, prefix: &str) -> anyhow::Result<S3Origin> {
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

/// The size proof key of `hash` under `prefix`.
fn size_proof_key(prefix: &str, hash: Hash) -> String {
    format!(
        "{}{}",
        expected_key(prefix, hash),
        decdn_bao_range::SIZE_PROOF_SUFFIX
    )
}

/// The `{H}.sizeproof` object `decdn origin import` writes for `blob`, and
/// its hash.
fn size_proof_object(blob: &[u8]) -> anyhow::Result<(Hash, Vec<u8>)> {
    let encoded = decdn_bao_range::encode_outboard(blob, u64::try_from(blob.len())?)?;
    let hash = Hash::new(blob);
    Ok((hash, encoded.size_proof_object()))
}

/// A published size proof is read verbatim from `{key}.sizeproof`.
#[tokio::test]
async fn fetch_size_proof_returns_the_object() -> anyhow::Result<()> {
    let hash = Hash::new(b"s3-size-proof");
    let key = size_proof_key("", hash);
    let object = decdn_bao_range::size_proof_object(5, b"proof");
    let body = object.clone();
    let rule = mock!(Client::get_object)
        .match_requests(move |req| req.bucket() == Some(BUCKET) && req.key() == Some(&key))
        .then_output(move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(body.clone()))
                .build()
        });
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "")?;

    let got = origin.fetch_size_proof(hash).await?;
    anyhow::ensure!(got.as_deref() == Some(object.as_slice()), "got: {got:?}");
    anyhow::ensure!(
        rule.num_calls() == 1,
        "a single read must hit the rule once"
    );
    Ok(())
}

/// `NoSuchKey` (the modeled S3 not-found error) is a missing object: no
/// proof, so the origin does not hold the blob.
#[tokio::test]
async fn no_such_key_is_missing() -> anyhow::Result<()> {
    let rule = mock!(Client::get_object)
        .then_error(|| GetObjectError::NoSuchKey(NoSuchKey::builder().build()));
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "")?;

    anyhow::ensure!(
        origin
            .fetch_size_proof(Hash::new(b"absent"))
            .await?
            .is_none(),
        "NoSuchKey must be a missing object"
    );
    Ok(())
}

/// A bare HTTP 404 with no AWS error code is also a missing object. Some
/// non-AWS S3 endpoints emit a 404 without the modeled `NoSuchKey` XML body,
/// so the status-only fallback is load-bearing on those backends. The
/// fallback is **gated on an empty / `NoSuchKey` error code** — a 404 with
/// `NoSuchBucket` or `AccessDenied` (covered below) is `Permanent`.
#[tokio::test]
async fn bare_http_404_is_missing() -> anyhow::Result<()> {
    let rule = mock!(Client::get_object)
        .sequence()
        .http_status(404, None)
        .build();
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "")?;

    anyhow::ensure!(
        origin
            .fetch_size_proof(Hash::new(b"absent"))
            .await?
            .is_none(),
        "a bare 404 must be a missing object"
    );
    Ok(())
}

/// Require a `Permanent` error whose message carries `needle`.
fn ensure_permanent(err: Option<OriginPullError>, needle: &str) -> anyhow::Result<()> {
    let err = err.ok_or_else(|| anyhow::anyhow!("expected an error"))?;
    anyhow::ensure!(
        matches!(err, OriginPullError::Permanent(_)),
        "expected Permanent, got: {err:?}"
    );
    let msg = format!("{err:#}");
    anyhow::ensure!(msg.contains(needle), "error lacked {needle:?}: {msg}");
    Ok(())
}

/// A 404 with `<Code>NoSuchBucket</Code>` in the AWS error body is a
/// **misconfigured-bucket** failure, not an absent object. It must surface
/// as `Permanent` — otherwise an operator with a typo'd `bucket = "..."`
/// would see every blob reported missing with no diagnostic.
#[tokio::test]
async fn http_404_with_no_such_bucket_is_permanent_not_missing() -> anyhow::Result<()> {
    // The SDK parses `<Code>NoSuchBucket</Code>` into
    // `ProvideErrorMetadata::code() == Some("NoSuchBucket")` even though
    // `NoSuchBucket` is not a modeled variant on `GetObjectError` — the
    // masking case the classifier guards against.
    let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message>\
<BucketName>typo-bucket</BucketName></Error>"
        .to_string();
    let rule = mock!(Client::get_object)
        .sequence()
        .http_status(404, Some(body))
        .build();
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "")?;

    ensure_permanent(
        origin.fetch_size_proof(Hash::new(b"any")).await.err(),
        "NoSuchBucket",
    )
}

/// A 404 with `<Code>AccessDenied</Code>` is what AWS S3 returns when the
/// principal lacks `s3:ListBucket` and the object is missing. It must NOT
/// read as a missing object, or every lookup against a misconfigured IAM
/// role would silently report "missing blob".
#[tokio::test]
async fn http_404_with_access_denied_is_permanent_not_missing() -> anyhow::Result<()> {
    let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
        .to_string();
    let rule = mock!(Client::get_object)
        .sequence()
        .http_status(404, Some(body))
        .build();
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "")?;

    ensure_permanent(
        origin.fetch_size_proof(Hash::new(b"forbidden")).await.err(),
        "AccessDenied",
    )
}

/// 5xx errors are transient and surface after **a single dispatch** —
/// `S3Origin::new` configures the SDK with `RetryConfig::disabled()` so the
/// SDK does not retry internally. Pinning `num_calls() == 1` catches a
/// regression where a future SDK upgrade or feature-flag edit re-enables
/// internal retry.
#[tokio::test]
async fn http_5xx_is_transient_with_no_internal_retry() -> anyhow::Result<()> {
    let rule = mock!(Client::get_object)
        .sequence()
        .http_status(503, None)
        .build();
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "")?;

    let err = origin
        .fetch_size_proof(Hash::new(b"transient"))
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

/// 4xx errors (other than 404) are permanent and the SDK does not retry
/// them.
#[tokio::test]
async fn http_4xx_is_permanent_and_does_not_retry() -> anyhow::Result<()> {
    let rule = mock!(Client::get_object)
        .sequence()
        .http_status(403, None)
        .build();
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "")?;

    let err = origin
        .fetch_size_proof(Hash::new(b"forbidden"))
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

/// A size proof over its cap is a broken object: a permanent fault, never
/// an absence. The mock omits Content-Length, so the streaming abort in
/// `collect_bounded` is what catches it.
#[tokio::test]
async fn oversize_size_proof_is_permanent() -> anyhow::Result<()> {
    let big = vec![0u8; decdn_bao_range::MAX_SIZE_PROOF_OBJECT_BYTES + 1];
    let rule = mock!(Client::get_object).then_output(move || {
        GetObjectOutput::builder()
            .body(ByteStream::from(big.clone()))
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "")?;

    ensure_permanent(
        origin.fetch_size_proof(Hash::new(b"oversize")).await.err(),
        "over its",
    )
}

/// A non-empty prefix is applied to the request key. The rule's
/// `match_requests` predicate enforces the exact key shape, so a
/// regression in `key_for` (e.g. forgetting to apply the prefix, or
/// double-applying the trailing slash) would surface here as a
/// "rule did not match" failure.
#[tokio::test]
async fn reads_apply_prefix_with_sharded_key_layout() -> anyhow::Result<()> {
    let hash = Hash::new(b"prefixed");
    let prefix = "blobs/";
    let key = size_proof_key(prefix, hash);

    let rule = mock!(Client::get_object)
        .match_requests(move |req| req.bucket() == Some(BUCKET) && req.key() == Some(&key))
        .then_output(|| {
            GetObjectOutput::builder()
                .body(ByteStream::from_static(b"prefixed bytes"))
                .build()
        });
    // `MatchAny` keeps the failure message clear ("no rule matched the
    // bucket/key") if a future regression breaks the key layout.
    let client = mock_s3_client_match_any(&[&rule]);
    let origin = s3_origin(client, prefix)?;

    let got = origin.fetch_size_proof(hash).await?;
    anyhow::ensure!(got.as_deref() == Some(&b"prefixed bytes"[..]));
    anyhow::ensure!(rule.num_calls() == 1, "rule must match the prefixed key");
    Ok(())
}

/// End-to-end through the cache engine: the S3 origin's `{key}.sizeproof`
/// proves the blob's presence and size, and a second presence answer comes
/// from the memo with no further origin read.
#[tokio::test]
async fn cache_engine_presence_reads_the_s3_size_proof_once() -> anyhow::Result<()> {
    let blob = make_blob(40 * 1024);
    let (hash, object) = size_proof_object(&blob)?;
    let key = size_proof_key("", hash);
    let rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&key))
        .then_output(move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(object.clone()))
                .build()
        });
    let client = mock_s3_client_match_any(&[&rule]);
    let origin: Arc<dyn Origin> = Arc::new(s3_origin(client, "")?);

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 16).await?;
    let size = u64::try_from(blob.len())?;

    anyhow::ensure!(
        engine.origin_presence(hash).await == OriginPresence::Present(size),
        "a verifying size proof must answer Present(size)"
    );
    anyhow::ensure!(
        engine.origin_presence(hash).await == OriginPresence::Present(size),
        "the memoised answer must stand"
    );
    anyhow::ensure!(
        engine.size_proof(hash, size).await.is_some(),
        "the presence walk keeps the proof"
    );
    anyhow::ensure!(
        rule.num_calls() == 1,
        "presence and size proof must read the origin once; got {} dispatches",
        rule.num_calls()
    );
    Ok(())
}

/// End-to-end absence: a `NoSuchKey` size proof is `Absent`, not a fault.
/// This is the contract the serve gate depends on to distinguish "object
/// doesn't exist" from "origin is broken".
#[tokio::test]
async fn cache_engine_presence_is_absent_for_no_such_key() -> anyhow::Result<()> {
    let rule = mock!(Client::get_object)
        .then_error(|| GetObjectError::NoSuchKey(NoSuchKey::builder().build()));
    let client = mock_s3_client(&[&rule]);
    let origin: Arc<dyn Origin> = Arc::new(s3_origin(client, "")?);

    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), vec![origin], 16).await?;

    anyhow::ensure!(
        engine.origin_presence(Hash::new(b"missing")).await == OriginPresence::Absent,
        "NoSuchKey must answer Absent"
    );
    Ok(())
}

// ----------------------------------------------------------------------------
// Origin range pull-through (#962, ADR 037 §Origin-tier pull-through).
//
// `S3Origin::fetch_range_data` issues one ranged data GET; the outboard is a
// separate `fetch_outboard` GET (tested below). The mock dispatcher keys on the
// requested key and on the presence of a `Range`.
// ----------------------------------------------------------------------------

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
async fn fetch_range_data_returns_span() -> anyhow::Result<()> {
    let blob = make_blob(200 * 1024);
    let hash = Hash::new(&blob);
    let data_key = expected_key("", hash);

    // Aligned span [16 KiB, 48 KiB).
    let (start, end) = (16 * 1024usize, 48 * 1024usize);
    let span = blob.get(start..end).unwrap_or_default().to_vec();

    // Ranged data rule: GET on `{key}` with a Range header. No outboard GET —
    // the engine reads the outboard once, through `fetch_outboard`.
    let data_match = data_key.clone();
    let data_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&data_match) && req.range().is_some())
        .then_output(move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(span.clone()))
                .build()
        });
    let client = mock_s3_client_match_any(&[&data_rule]);
    let origin = s3_origin(client, "")?;

    let req = OriginRangeRequest {
        fetch_start: start as u64,
        fetch_end: end as u64,
    };
    match origin.fetch_range_data(hash, req).await? {
        OriginRangeFetch::Ranged { data } => {
            anyhow::ensure!(
                data.as_ref() == blob.get(start..end).unwrap_or_default(),
                "span mismatch",
            );
        }
        OriginRangeFetch::NotFound => anyhow::bail!("expected Ranged, got NotFound"),
    }
    anyhow::ensure!(data_rule.num_calls() == 1, "one ranged data GET");
    Ok(())
}

#[tokio::test]
async fn fetch_range_data_missing_object_is_not_found() -> anyhow::Result<()> {
    let blob = make_blob(64 * 1024);
    let hash = Hash::new(&blob);
    let data_match = expected_key("", hash);
    let data_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&data_match))
        .then_error(|| GetObjectError::NoSuchKey(NoSuchKey::builder().build()));
    let client = mock_s3_client_match_any(&[&data_rule]);
    let origin = s3_origin(client, "")?;

    let req = OriginRangeRequest {
        fetch_start: 0,
        fetch_end: 16 * 1024,
    };
    anyhow::ensure!(
        matches!(
            origin.fetch_range_data(hash, req).await?,
            OriginRangeFetch::NotFound
        ),
        "a missing object must be NotFound",
    );
    Ok(())
}

#[tokio::test]
async fn fetch_range_data_wrong_length_span_is_permanent() -> anyhow::Result<()> {
    // The ranged GET returns a *shorter* body than the requested span (origin
    // ignored Range / truncated object).
    let blob = make_blob(200 * 1024);
    let hash = Hash::new(&blob);
    let data_match = expected_key("", hash);
    let data_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&data_match) && req.range().is_some())
        .then_output(|| {
            GetObjectOutput::builder()
                .body(ByteStream::from_static(b"short!!!"))
                .build()
        });
    let client = mock_s3_client_match_any(&[&data_rule]);
    let origin = s3_origin(client, "")?;

    let req = OriginRangeRequest {
        fetch_start: 16 * 1024,
        fetch_end: 48 * 1024,
    };
    ensure_permanent(
        origin.fetch_range_data(hash, req).await.err(),
        "for a 32768-byte range",
    )
}

#[tokio::test]
async fn fetch_range_data_of_a_compressed_object_is_permanent() -> anyhow::Result<()> {
    // A ranged GET of a stored-compressed object returns encoded bytes, which
    // break the offset→byte mapping the bao proof anchors on.
    let blob = make_blob(64 * 1024);
    let hash = Hash::new(&blob);
    let data_rule = mock!(Client::get_object).then_output(|| {
        GetObjectOutput::builder()
            .body(ByteStream::from(vec![0u8; 16 * 1024]))
            .content_encoding("gzip")
            .build()
    });
    let client = mock_s3_client_match_any(&[&data_rule]);
    let origin = s3_origin(client, "")?;

    let req = OriginRangeRequest {
        fetch_start: 0,
        fetch_end: 16 * 1024,
    };
    ensure_permanent(
        origin.fetch_range_data(hash, req).await.err(),
        "Content-Encoding",
    )
}

// ----------------------------------------------------------------------------
// `S3Origin::fetch_outboard` (#1130) — a standalone `GetObject` on the
// sibling `{key}.obao4`, no accompanying data GET — the one outboard read a
// range pull makes before it fetches the span window by window.
// ----------------------------------------------------------------------------

#[tokio::test]
async fn fetch_outboard_returns_sibling_obao4() -> anyhow::Result<()> {
    let hash = Hash::new(b"s3-outboard-marker");
    let obao4_key = format!("{}.obao4", expected_key("", hash));
    let outboard_bytes = vec![0xCDu8; 4096];

    let body = outboard_bytes.clone();
    let obao4_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&obao4_key))
        .then_output(move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(body.clone()))
                .build()
        });
    let client = mock_s3_client_match_any(&[&obao4_rule]);
    let origin = s3_origin(client, "")?;

    match origin.fetch_outboard(hash, 1 << 20).await? {
        OutboardFetch::Found(bytes) => {
            anyhow::ensure!(
                bytes.as_ref() == outboard_bytes.as_slice(),
                "outboard bytes mismatch"
            );
        }
        OutboardFetch::NotFound => anyhow::bail!("expected Found, got NotFound"),
    }
    anyhow::ensure!(obao4_rule.num_calls() == 1, "exactly one outboard GET");
    Ok(())
}

#[tokio::test]
async fn fetch_outboard_missing_sibling_is_not_found() -> anyhow::Result<()> {
    let hash = Hash::new(b"s3-outboard-missing-marker");
    let obao4_key = format!("{}.obao4", expected_key("", hash));
    let obao4_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&obao4_key))
        .then_error(|| GetObjectError::NoSuchKey(NoSuchKey::builder().build()));
    let client = mock_s3_client_match_any(&[&obao4_rule]);
    let origin = s3_origin(client, "")?;

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
async fn fetch_outboard_oversize_is_permanent_without_buffering() -> anyhow::Result<()> {
    // OOM guard on the one outboard read a range pull makes: the mock omits
    // Content-Length (matching real chunked responses) so the pre-check can't
    // short-circuit, exercising the streaming abort in `collect_bounded`.
    let hash = Hash::new(b"s3-outboard-oversize-marker");
    let obao4_key = format!("{}.obao4", expected_key("", hash));
    let huge_outboard = vec![0x5Au8; 8 * 1024 * 1024];
    let obao4_rule = mock!(Client::get_object)
        .match_requests(move |req| req.key() == Some(&obao4_key))
        .then_output(move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(huge_outboard.clone()))
                .build()
        });
    let client = mock_s3_client_match_any(&[&obao4_rule]);
    let origin = s3_origin(client, "")?;

    ensure_permanent(
        origin.fetch_outboard(hash, 4 * 1024).await.err(),
        "over its 4096-byte cap",
    )?;
    anyhow::ensure!(
        obao4_rule.num_calls() == 1,
        "outboard GET issued exactly once"
    );
    Ok(())
}
