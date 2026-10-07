use super::*;

fn make_cfg() -> S3OriginConfig {
    S3OriginConfig {
        bucket: "decdn-blobs".to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: None,
        path_style: false,
        prefix: String::new(),
        credentials: Some(S3Credentials::DefaultChain { profile: None }),
    }
}

#[test]
fn body_stream_error_gains_s3_prefix() {
    // A plain network-fault io error (no typed marker) must come out
    // carrying the `s3://bucket/key` request context, mirroring the
    // header-phase `classify_get_object_error` prefix (issue #1617).
    let log_target = "s3://decdn-blobs/ab/abcdef";
    let raw = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset");
    let wrapped = prefix_body_stream_error(log_target, raw);
    let msg = wrapped.to_string();
    assert!(
        msg.starts_with(log_target),
        "body error lost the s3://bucket/key prefix: {msg}"
    );
    assert!(
        msg.contains("connection reset"),
        "prefix wrapper dropped the underlying error text: {msg}"
    );
    // Kind must survive so retry routing is unchanged.
    assert_eq!(wrapped.kind(), std::io::ErrorKind::ConnectionReset);
    // The original message sits in `Display`, so a chain formatter must
    // not repeat it through `source()`.
    let chained = format!("{:#}", anyhow::Error::new(wrapped));
    assert_eq!(
        chained.matches("connection reset").count(),
        1,
        "chain repeated the original error: {chained}"
    );
}

#[test]
fn body_stream_error_keeps_the_original_errors_causes() {
    // The original error carries a typed payload whose own cause is not in
    // its `Display`, so the cause can only reach the render through
    // `source()`.
    #[derive(Debug, thiserror::Error)]
    #[error("tls handshake failed")]
    struct Tls(#[source] std::io::Error);

    let raw = std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        Tls(std::io::Error::other("tls alert")),
    );
    let wrapped = prefix_body_stream_error("s3://decdn-blobs/ab/abcdef", raw);
    let chained = format!("{:#}", anyhow::Error::new(wrapped));
    assert_eq!(
        chained.matches("tls handshake failed").count(),
        1,
        "original error not named once: {chained}"
    );
    assert_eq!(
        chained.matches("tls alert").count(),
        1,
        "nested cause not named once: {chained}"
    );
}

#[test]
fn body_stream_error_preserves_retry_classification() {
    // The prefix wrapper must not change how `classify_io_error` routes a
    // transient network fault: an `UnexpectedEof` (mid-body truncation)
    // stays Transient after prefixing.
    let raw = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "truncated body");
    let wrapped = prefix_body_stream_error("s3://decdn-blobs/ab/abcdef", raw);
    assert!(
        matches!(
            crate::retry::classify_io_error(wrapped),
            OriginPullError::Transient(_)
        ),
        "prefixing broke transient classification of a mid-body EOF"
    );
}

#[test]
fn body_stream_error_passes_typed_marker_through() {
    // A typed `OriginError` marker (e.g. a decode failure) must pass
    // through untouched so `classify_io_error`'s downcast still fires and
    // routes it Permanent. Re-wrapping it in a prefixed `String` would
    // hide the marker and silently reclassify it as a transient `Other`.
    let typed = std::io::Error::other(OriginError::MalformedEncoding);
    let out = prefix_body_stream_error("s3://decdn-blobs/ab/abcdef", typed);
    // Message is unchanged (no prefix added) …
    assert!(
        !out.to_string().starts_with("s3://"),
        "typed marker was wrapped and lost its downcast identity"
    );
    // … and the downcast-driven classification still lands on Permanent.
    assert!(
        matches!(
            crate::retry::classify_io_error(out),
            OriginPullError::Permanent(_)
        ),
        "typed marker no longer classifies Permanent after prefixing"
    );
}

#[test]
fn key_for_no_prefix_uses_two_char_shard() {
    let hash = Hash::new(b"marker");
    let hex = hash.to_hex();
    let shard = hex.get(..2).unwrap_or("");
    let want = format!("{shard}/{}", hex.as_str());
    assert_eq!(key_for("", hash), want, "key shape regressed");
}

#[test]
fn key_for_applies_prefix_with_trailing_slash() {
    // The resolver guarantees a trailing slash on non-empty prefixes;
    // mirror that here. A prefix without trailing slash would produce
    // `blobsAB/hex` which is wrong, so the resolver invariant is
    // load-bearing — pin the expected shape.
    let hash = Hash::new(b"marker");
    let hex = hash.to_hex();
    let shard = hex.get(..2).unwrap_or("");
    let want = format!("blobs/{shard}/{}", hex.as_str());
    assert_eq!(key_for("blobs/", hash), want);
}

#[test]
fn is_transient_status_classification_matches_rfc_9110() {
    // 5xx
    assert!(is_transient_status(500));
    assert!(is_transient_status(503));
    assert!(is_transient_status(599));
    // Retry-flagged 4xx
    assert!(is_transient_status(408));
    assert!(is_transient_status(429));
    // Other 4xx — permanent.
    assert!(!is_transient_status(400));
    assert!(!is_transient_status(401));
    assert!(!is_transient_status(403));
    assert!(!is_transient_status(404));
    assert!(!is_transient_status(409));
    // 2xx / 3xx aren't error-class but the function shouldn't
    // misclassify them as transient if they ever hit it.
    assert!(!is_transient_status(200));
    assert!(!is_transient_status(304));
}

/// `S3Origin::new` with a minimal config (no static creds, no custom
/// endpoint) must succeed without performing any network I/O. We can't
/// drive a real `GetObject` from this unit-test layer (the integration
/// suite at `tests/s3_origin.rs` does that via `mock_client!`), but
/// we can at least confirm construction itself is non-blocking and
/// non-panicking — the SDK's `ClientBuilder::build` is the only
/// `.unwrap()` on the path and we want a regression test pinning that
/// it doesn't fire.
#[tokio::test]
async fn new_default_chain_construction_is_pure() {
    let cfg = make_cfg();
    let origin = S3Origin::new(&cfg)
        .await
        .expect("construction must succeed without I/O");
    assert_eq!(origin.bucket.as_ref(), "decdn-blobs");
    assert_eq!(origin.prefix.as_ref(), "");
}

/// Same as above but with the `Static` credential variant, so the
/// `SharedCredentialsProvider` arm gets exercised.
#[tokio::test]
async fn new_static_credentials_construction_is_pure() {
    let mut cfg = make_cfg();
    cfg.credentials = Some(S3Credentials::Static {
        access_key_id: "AKIA-test-fake".to_string(),
        secret_access_key: "secret-fake".to_string(),
        session_token: None,
    });
    let origin = S3Origin::new(&cfg)
        .await
        .expect("construction with static creds must succeed without I/O");
    assert_eq!(origin.bucket.as_ref(), "decdn-blobs");
}

/// `endpoint_url: Some(...)` round-trip — the entire R2/B2/MinIO
/// path. A regression in `OriginUrl::as_url().as_str()` (e.g. a
/// trailing-slash drift the SDK rejects, or a credential-bearing
/// URL slipping past the parser) would surface as an `Err` from
/// the SDK config builder during construction, failing this
/// test's `.expect(...)`.
#[tokio::test]
async fn new_with_endpoint_url_construction_is_pure() {
    let mut cfg = make_cfg();
    cfg.endpoint_url = Some(
        super::super::parse_origin_url("https://example-r2-endpoint.invalid/")
            .expect("test URL must parse"),
    );
    let origin = S3Origin::new(&cfg)
        .await
        .expect("construction with custom endpoint must succeed without I/O");
    assert_eq!(origin.bucket.as_ref(), "decdn-blobs");
}

/// `path_style: true` round-trip — the `MinIO` addressing path. The
/// SDK's `force_path_style(true)` call is what makes path-style
/// addressing actually take effect; if a future SDK rename or
/// removal of `force_path_style` slipped through, `MinIO` deployments
/// would break and this construction test would fail at compile
/// time (the type signature is the contract under test).
#[tokio::test]
async fn new_with_path_style_true_construction_is_pure() {
    let mut cfg = make_cfg();
    cfg.path_style = true;
    cfg.endpoint_url = Some(
        super::super::parse_origin_url("http://minio.invalid:9000/").expect("test URL must parse"),
    );
    cfg.credentials = Some(S3Credentials::Static {
        access_key_id: "minioadmin".to_string(),
        secret_access_key: "minioadmin".to_string(),
        session_token: None,
    });
    let origin = S3Origin::new(&cfg)
        .await
        .expect("construction with path_style + custom endpoint must succeed");
    assert_eq!(origin.bucket.as_ref(), "decdn-blobs");
}

/// `DefaultChain { profile: Some(name) }` round-trip — exercises
/// the `loader.profile_name(name.clone())` arm. Without this test,
/// dropping the `if let Some(name)` branch would silently fall
/// back every operator's non-default-profile config to `default`.
#[tokio::test]
async fn new_with_named_profile_construction_is_pure() {
    let mut cfg = make_cfg();
    cfg.credentials = Some(S3Credentials::DefaultChain {
        profile: Some("decdn-prod".to_string()),
    });
    let origin = S3Origin::new(&cfg)
        .await
        .expect("construction with named profile must succeed without I/O");
    assert_eq!(origin.bucket.as_ref(), "decdn-blobs");
}

/// Empty static credentials are rejected at construction time. A
/// regression that drops this defense would let a hand-built
/// `S3OriginConfig` (or a config-resolver gap) reach the SDK with
/// `Credentials::new("", "", None, ...)` — the operator would see
/// a confusing 403 from the service at first fetch instead of a
/// clear startup error pointing at `[cache.origin.credentials]`.
#[tokio::test]
async fn new_rejects_empty_static_credentials() {
    let mut cfg = make_cfg();
    cfg.credentials = Some(S3Credentials::Static {
        access_key_id: String::new(),
        secret_access_key: "secret-fake".to_string(),
        session_token: None,
    });
    let err = S3Origin::new(&cfg)
        .await
        .expect_err("empty access_key_id must reject");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("empty access_key_id") || msg.contains("empty"),
        "rejection message lost actionable wording: {msg}"
    );

    cfg.credentials = Some(S3Credentials::Static {
        access_key_id: "AKIA-test-fake".to_string(),
        secret_access_key: String::new(),
        session_token: None,
    });
    let err = S3Origin::new(&cfg)
        .await
        .expect_err("empty secret_access_key must reject");
    let msg = format!("{err:#}");
    assert!(msg.contains("empty"), "rejection message: {msg}");
}

/// `S3Credentials::Static` Debug must NEVER print the cleartext
/// access key, secret key, or session token. The `derive(Debug)`
/// would dump them; the manual impl is the only thing standing
/// between a stray `tracing::debug!(?creds)` or panic backtrace
/// and a credential leak in operator logs / Sentry. Pin the
/// contract.
#[test]
fn debug_redacts_static_credentials() {
    let creds = S3Credentials::Static {
        access_key_id: "AKIA-leaked-12345".to_string(),
        secret_access_key: "secret-leaked-67890".to_string(),
        session_token: Some("token-leaked-abcde".to_string()),
    };
    let dbg = format!("{creds:?}");
    assert!(
        !dbg.contains("AKIA-leaked-12345"),
        "access_key_id leaked through Debug: {dbg}"
    );
    assert!(
        !dbg.contains("secret-leaked-67890"),
        "secret_access_key leaked through Debug: {dbg}"
    );
    assert!(
        !dbg.contains("token-leaked-abcde"),
        "session_token leaked through Debug: {dbg}"
    );
    // Redaction marker present
    assert!(dbg.contains("***"), "redaction marker missing: {dbg}");
    // Variant tag preserved so operators can tell which branch is in use
    assert!(dbg.contains("Static"), "variant tag missing: {dbg}");
}

/// The same redaction must apply transitively when `S3Credentials`
/// is embedded inside an `S3OriginConfig`. A `?cfg` on the runtime
/// wiring path would otherwise leak credentials through the
/// containing struct.
#[test]
fn debug_redacts_credentials_when_nested_in_origin_config() {
    let cfg = S3OriginConfig {
        bucket: "b".to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: None,
        path_style: false,
        prefix: String::new(),
        credentials: Some(S3Credentials::Static {
            access_key_id: "AKIA-leaked-fff".to_string(),
            secret_access_key: "secret-leaked-ggg".to_string(),
            session_token: None,
        }),
    };
    let dbg = format!("{cfg:?}");
    assert!(!dbg.contains("AKIA-leaked-fff"), "access key leaked: {dbg}");
    assert!(!dbg.contains("secret-leaked-ggg"), "secret leaked: {dbg}");
    // The bucket name is operator config (not secret) and SHOULD
    // appear — verifies the manual Debug isn't accidentally
    // suppressing all fields.
    assert!(dbg.contains("\"b\""), "bucket missing from Debug: {dbg}");
}

/// `DefaultChain`'s `profile` is operator-set config (not secret)
/// and SHOULD appear in Debug — operators benefit from seeing
/// which profile is in use.
#[test]
fn debug_default_chain_shows_profile_name() {
    let creds = S3Credentials::DefaultChain {
        profile: Some("decdn-prod".to_string()),
    };
    let dbg = format!("{creds:?}");
    assert!(
        dbg.contains("decdn-prod"),
        "profile name should appear: {dbg}"
    );
    assert!(dbg.contains("DefaultChain"), "variant tag missing: {dbg}");
}
