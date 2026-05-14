//! Integration tests for [`decdn_cache::S3Origin`] using `aws-smithy-mocks`.
//!
//! The mocks crate hands us a fully-wired `aws_sdk_s3::Client` whose request
//! dispatcher returns canned responses while every other layer (signing,
//! body framing, modeled-error parsing) runs end-to-end. The SDK's
//! internal retry layer is **disabled** in every test via
//! [`mock_s3_client`] / [`mock_s3_client_match_any`], matching the
//! production posture set by `S3Origin::new`'s
//! `RetryConfig::disabled()`. This keeps the cache engine's
//! `cache.origin_retry` policy as the single source of retry budget
//! and makes per-test dispatch counts deterministic.
//!
//! See `crates/cache/tests/pull_through.rs` for the equivalent `wiremock`
//! suite covering [`decdn_cache::HttpOrigin`].

use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::types::error::NoSuchKey;
use aws_smithy_mocks::{Rule, RuleMode, mock, mock_client};
use aws_smithy_types::byte_stream::ByteStream;
use aws_smithy_types::retry::RetryConfig;
use decdn_cache::{
    CacheEngine, CacheError, Hash, Origin, OriginFetch, OriginPullError, RetryPolicy, S3Origin,
};

/// Bucket and prefix used across tests. Matching constants on every rule
/// keep the test setup terse and the failure messages easy to read.
const BUCKET: &str = "decdn-blobs";

/// Build a mock-backed S3 client with the SDK's internal retry layer
/// **disabled**, matching the production posture set by `S3Origin::new`
/// (which configures `aws_config::ConfigLoader::retry_config(RetryConfig::disabled())`).
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

/// Convert a hash to its expected sharded S3 key. Mirrors the production
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

/// Non-identity `Content-Encoding` is rejected with the operator-friendly
/// pointer message. The BLAKE3 verify in the engine runs over canonical
/// bytes; passing through a gzipped body would later trip a `HashMismatch`
/// error with no explanation. This test pins the exact wording so a doc
/// drift between the error and the runbook doesn't slip past review.
#[tokio::test]
async fn fetch_with_content_encoding_gzip_is_permanent_with_pointer_message() -> anyhow::Result<()>
{
    let hash = Hash::new(b"gzipped");

    let rule = mock!(Client::get_object).then_output(|| {
        GetObjectOutput::builder()
            .body(ByteStream::from_static(
                b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03",
            ))
            .content_encoding("gzip")
            .build()
    });
    let client = mock_s3_client(&[&rule]);
    let origin = s3_origin(client, "");

    let err = origin
        .fetch(hash, 16 * 1024 * 1024)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("gzipped response must be rejected"))?;
    let msg = match &err {
        OriginPullError::Permanent(e) => format!("{e:#}"),
        OriginPullError::Transient(e) => {
            anyhow::bail!("Content-Encoding rejection must be Permanent, was Transient: {e:#}");
        }
    };
    // Operators grep for these substrings in runbooks; pin the contract.
    anyhow::ensure!(
        msg.contains("Content-Encoding") && msg.contains("canonical bytes"),
        "operator-facing rejection lost its actionable wording: {msg}"
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
