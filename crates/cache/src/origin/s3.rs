//! S3-compatible origin backend (#437 PR2).
//!
//! Fetches blobs from `{prefix?}{hex[0..2]}/{hex}` keys in an S3 bucket via
//! the official AWS SDK. The sharded key layout mirrors
//! [`super::FilesystemOrigin`] so operators can `aws s3 sync` between a
//! filesystem origin and an S3 origin without rewriting object names.
//!
//! Supports plain AWS S3, Cloudflare R2, Backblaze B2, `MinIO`, and any other
//! S3-compatible service via the `endpoint_url` + `path_style` knobs on
//! [`S3OriginConfig`]. Credentials come from either explicit static keys or
//! the AWS default credential chain (env, `~/.aws/credentials` profile,
//! container/instance role).
//!
//! ## TLS / HTTP layer
//!
//! The SDK is configured to bring its own HTTP client built from
//! [`aws_smithy_http_client`] over **hyper-1 + rustls 0.23 + aws-lc-rs**,
//! matching the rest of the workspace. The SDK's stock HTTP stack
//! (hyper-0.14 + rustls 0.21 + ring) is suppressed via `default-features =
//! false` on `aws-sdk-s3` and `aws-config` — bringing it in would force a
//! third parallel TLS implementation into the dep tree.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_smithy_http_client::{Builder as HttpBuilder, tls};
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use aws_smithy_types::retry::RetryConfig;
use iroh_blobs::Hash;

use super::{Origin, OriginFetch, OriginKind, OriginUrl};
use crate::error::OriginPullError;

/// Validated, runtime-ready configuration for an [`S3Origin`].
///
/// This is the cache-crate's parallel of `decdn_common::config::ResolvedS3Config`
/// — the wiring layer in `decdn-node` does the conversion when constructing
/// the engine. Keeping the type here means `decdn-cache` doesn't depend on
/// `decdn-common` (which would be circular: `common -> cache` is the
/// established direction so the resolved-config types can carry parsed
/// `OriginUrl`s).
///
/// Construction is field-init at the call site; the wiring layer enforces
/// that the values come from `decdn_common::config::resolve_origin`, the
/// only producer that runs the bucket-name / region / endpoint-URL
/// validators.
///
/// The `Debug` impl is **manual** rather than derived: it forwards every
/// field through the [`S3Credentials`] redaction wrapper so an incidental
/// `tracing::debug!(?cfg)` or panic-backtrace formatter cannot leak the
/// access-key / secret-key bytes. The `#[derive(Debug)]` would print them
/// in cleartext (the wrapped `String`s lost their `SecretString` discipline
/// at the resolve→runtime boundary in `decdn-node`). Mirrors the pattern
/// established by `decdn_common::config::secret::SecretString` (same crate
/// can't be intra-doc-linked because `decdn-cache` deliberately does not
/// depend on `decdn-common`).
#[derive(Clone)]
pub struct S3OriginConfig {
    /// Bucket name. Validated DNS-safe at config-resolve time.
    pub bucket: String,
    /// AWS region.
    pub region: String,
    /// Custom endpoint URL for non-AWS S3-compatible services
    /// (R2, B2, `MinIO`). Parsed and trailing-slash-normalized.
    pub endpoint_url: Option<OriginUrl>,
    /// `true` selects path-style addressing (`https://endpoint/{bucket}/{key}`),
    /// required by `MinIO` and some on-prem providers. The SDK default
    /// (`false`) is virtual-hosted-style (`https://{bucket}.s3.amazonaws.com/{key}`).
    pub path_style: bool,
    /// Trailing-slash-normalized object key prefix prepended to every
    /// fetched object. Empty string means no prefix.
    pub prefix: String,
    /// Credential source. `None` is treated identically to
    /// `Some(S3Credentials::DefaultChain { profile: None })` — the SDK's
    /// stock chain (env, profile, container/instance role).
    pub credentials: Option<S3Credentials>,
}

/// How `S3Origin` obtains AWS credentials.
///
/// `Static` carries the cleartext bytes — the wiring layer in `decdn-node`
/// is responsible for unwrapping `decdn_common::config::secret::SecretString`
/// before passing them here. By the time credentials reach the SDK they
/// are plain bytes either way (the SDK's `Credentials::new` takes
/// `&str`/`String`).
///
/// The `Debug` impl is **manual** to preserve the redaction discipline
/// across the resolve→runtime boundary: `Static`'s key fields print as
/// `"***"` so a stray `tracing::debug!(?creds)` or panic-backtrace cannot
/// leak the credential bytes. `DefaultChain`'s `profile` is operator-set
/// configuration and prints in cleartext (no secret content).
#[derive(Clone)]
pub enum S3Credentials {
    /// Explicit IAM access key + secret + optional STS session token.
    Static {
        /// AWS access key ID.
        access_key_id: String,
        /// AWS secret access key.
        secret_access_key: String,
        /// Optional STS session token (for temporary credentials).
        session_token: Option<String>,
    },
    /// Use the AWS default credential chain. Identical to `aws-cli`'s
    /// discovery: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars,
    /// then `~/.aws/credentials` (selecting `profile` if set), then
    /// container / instance role via IMDS.
    DefaultChain {
        /// Profile name override for `~/.aws/credentials`.
        profile: Option<String>,
    },
}

impl std::fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Mirrors `decdn_common::config::secret::SecretString::Debug`: never
        // print the cleartext key, secret, or session token, regardless of
        // the surrounding formatter (`{:?}`, `{:#?}`, panic backtrace).
        // `session_token` discriminates `Some`/`None` so an operator can
        // tell whether STS temporary creds are in use without seeing the
        // token bytes.
        match self {
            Self::Static { session_token, .. } => f
                .debug_struct("Static")
                .field("access_key_id", &"***")
                .field("secret_access_key", &"***")
                .field("session_token", &session_token.as_ref().map(|_| "***"))
                .finish(),
            Self::DefaultChain { profile } => f
                .debug_struct("DefaultChain")
                .field("profile", profile)
                .finish(),
        }
    }
}

impl std::fmt::Debug for S3OriginConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Manual impl rather than derive so `credentials` flows through
        // [`S3Credentials`]'s redacting `Debug`. Bucket / region /
        // endpoint / prefix are operator-set configuration with no
        // secret content; print in the clear.
        f.debug_struct("S3OriginConfig")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint_url", &self.endpoint_url)
            .field("path_style", &self.path_style)
            .field("prefix", &self.prefix)
            .field("credentials", &self.credentials)
            .finish()
    }
}

/// Compute the S3 object key for `hash`: `{prefix}{hex[0..2]}/{hex}`.
///
/// The two-char shard mirrors `FilesystemOrigin::path_for` so operators
/// can copy blobs between filesystem and S3 origins without renaming.
/// Free function (not a method) so tests don't need a real `Client` to
/// exercise the layout — the integration suite at `tests/s3_origin.rs`
/// covers the SDK end-to-end separately.
fn key_for(prefix: &str, hash: Hash) -> String {
    let hex = hash.to_hex();
    // Defensive against an unexpected `iroh_blobs::Hash` format change;
    // the workspace `indexing_slicing = "deny"` lint forbids `&hex[..2]`.
    let shard = hex.get(..2).unwrap_or("");
    format!("{prefix}{shard}/{}", hex.as_str())
}

/// Origin backed by an S3-compatible object store.
///
/// `Clone` is cheap: the inner `Client` is `Clone` and uses an `Arc`
/// internally; the `bucket` and `prefix` fields use `Arc<str>` so adding
/// labels to diagnostic logs doesn't allocate.
#[derive(Debug, Clone)]
pub struct S3Origin {
    client: Client,
    /// Operator-configured bucket. `Arc<str>` so cheap clone preserves
    /// shared ownership for log fields without re-allocating.
    bucket: Arc<str>,
    /// Operator-configured key prefix (trailing slash already applied by
    /// the resolver). Same rationale as `bucket`.
    prefix: Arc<str>,
}

impl S3Origin {
    /// Build an `S3Origin` from validated config. Returns an error if a
    /// `Static` credential variant carries empty bytes (defense in depth
    /// against a config-resolver gap or a hand-built `S3OriginConfig`).
    ///
    /// **I/O posture:** construction itself is non-blocking — `aws_config`
    /// builds lazy provider chains and resolves credentials on the first
    /// signed request, not at `load()` time. The one operator-visible
    /// caveat is the AWS SSO provider (enabled via the `sso` feature on
    /// `aws-config`): if the `~/.aws/config` profile in use is configured
    /// for SSO and the cached SSO token has expired, the *first* `fetch`
    /// call may fire an HTTP request to the SSO endpoint to refresh the
    /// token. That's a property of the AWS credential chain, not this
    /// constructor; the construction itself stays I/O-free.
    ///
    /// `BehaviorVersion::latest()` is set explicitly even though
    /// `aws-config`'s `behavior-version-latest` feature would imply the
    /// same. Belt-and-braces against a future feature-flag edit dropping
    /// the implicit path — older SDK versions panicked when no
    /// `BehaviorVersion` was set; current versions surface a runtime error.
    /// Either way, the explicit call site cannot regress.
    pub async fn new(cfg: &S3OriginConfig) -> anyhow::Result<Self> {
        // Defense in depth against a config-resolver gap or a downstream
        // caller that bypasses `decdn_common::config::resolve_origin` and
        // hand-builds an `S3OriginConfig`. Empty static credentials would
        // otherwise produce a confusing 403 from the service at first
        // fetch — turn the failure into a clear startup-time error
        // pointing at the [cache.origin.credentials] block.
        if let Some(S3Credentials::Static {
            access_key_id,
            secret_access_key,
            ..
        }) = cfg.credentials.as_ref()
            && (access_key_id.is_empty() || secret_access_key.is_empty())
        {
            anyhow::bail!(
                "S3 static credentials have empty access_key_id or secret_access_key; \
                 check [cache.origin.credentials] in config or the secret-resolution layer"
            );
        }
        // Build the hyper-1 + rustls 0.23 + aws-lc-rs HTTP client.
        // Constructed once per S3Origin and shared across every fetch via
        // `Client::clone` (the SDK Client is cheaply cloneable). A single
        // shared connection pool keeps idle TLS connections warm across
        // back-to-back cache misses.
        let http_client = HttpBuilder::new()
            .tls_provider(tls::Provider::Rustls(
                tls::rustls_provider::CryptoMode::AwsLc,
            ))
            .build_https();

        // Disable the SDK's internal retry layer. Without this, transient
        // failures get retried *twice*: once by the SDK (default 3 attempts)
        // and again by the cache engine's outer `retry_fetch` (default 4
        // attempts), producing up to 12 dispatches per logical fetch on
        // sustained 5xx — not what an operator reading
        // `cache.origin_retry.max_retries = 3` expects. By disabling the
        // SDK budget, `cache.origin_retry` becomes the single source of
        // truth for retry policy across all three origin backends (HTTP,
        // FS, S3), matching the operator-facing contract from #285.
        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .http_client(http_client)
            .retry_config(RetryConfig::disabled())
            .region(Region::new(cfg.region.clone()));

        if let Some(endpoint) = cfg.endpoint_url.as_ref() {
            // `OriginUrl` already enforces http/https scheme + trailing-
            // slash normalization at config-resolve time; pass the
            // canonical string form to the SDK.
            loader = loader.endpoint_url(endpoint.as_url().as_str());
        }

        match cfg.credentials.as_ref() {
            Some(S3Credentials::Static {
                access_key_id,
                secret_access_key,
                session_token,
            }) => {
                let creds = Credentials::new(
                    access_key_id.clone(),
                    secret_access_key.clone(),
                    session_token.clone(),
                    None, // expires_after — None = treat as long-lived
                    "decdn-static-config",
                );
                loader = loader.credentials_provider(SharedCredentialsProvider::new(creds));
            }
            Some(S3Credentials::DefaultChain { profile }) => {
                if let Some(name) = profile.as_ref() {
                    loader = loader.profile_name(name.clone());
                }
            }
            // `None` falls through to whatever `aws_config::defaults`
            // resolves — same as `DefaultChain { profile: None }`.
            None => {}
        }

        let sdk_config = loader.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(cfg.path_style)
            .build();
        let client = Client::from_conf(s3_config);

        Ok(Self::from_parts(
            client,
            cfg.bucket.as_str(),
            cfg.prefix.as_str(),
        ))
    }

    /// Test-only constructor that skips `aws_config::defaults` and takes a
    /// pre-built `Client`. Used by `tests/s3_origin.rs` to drive the
    /// backend through `aws_smithy_mocks::mock_client!`-produced clients,
    /// which can't be obtained via `aws_config::defaults`.
    ///
    /// `pub` rather than `pub(crate)` because the integration tests live
    /// outside the crate. `#[doc(hidden)]` excludes the function from
    /// generated rustdoc only — it is technically callable by downstream
    /// crates and is part of the semver surface by convention. Production
    /// code uses [`Self::new`].
    #[doc(hidden)]
    pub fn from_parts(client: Client, bucket: &str, prefix: &str) -> Self {
        Self {
            client,
            bucket: Arc::from(bucket),
            prefix: Arc::from(prefix),
        }
    }
}

/// Classify an HTTP status code returned via an S3 `ServiceError`.
/// 5xx and the two retry-flagged 4xx codes (408 Request Timeout, 429
/// Too Many Requests) are transient; everything else is permanent.
const fn is_transient_status(status: u16) -> bool {
    status >= 500 || status == 408 || status == 429
}

/// Classify a `SdkError<GetObjectError>` into the cache engine's
/// `OriginFetch` / `OriginPullError` taxonomy.
///
/// Returns `Ok(OriginFetch::NotFound)` only when the response is
/// genuinely "object does not exist": either the modeled `NoSuchKey`
/// variant, or an HTTP 404 whose AWS error code is `NoSuchKey`/empty.
/// A 404 with any other code (`NoSuchBucket`, `AccessDenied`, ...) is
/// **not** treated as `NotFound` — those are config / permission
/// failures that AWS sometimes dresses up as 404 (e.g. when the
/// principal lacks `s3:ListBucket` it returns 404 for missing objects;
/// a typo'd bucket also surfaces as a non-modeled 404). Misclassifying
/// those as `NotFound` would silently mask the operator's real problem.
///
/// Errors that may be cured by retry (timeouts, dispatch failures,
/// 5xx/408/429) become `Transient`; everything else (other 4xx,
/// construction failures, response-parse failures, Glacier-cold objects)
/// becomes `Permanent`. The cache engine's `retry_fetch` drives the
/// retry budget off this distinction.
///
/// `SdkError` is `#[non_exhaustive]`. The catch-all arm classifies
/// future variants as `Permanent` (fail fast over retry-storm) and
/// emits a `tracing::warn!` so an SDK upgrade that adds a transient-
/// shaped variant we don't yet recognize is operator-visible rather
/// than silently misclassified.
fn classify_get_object_error(
    err: SdkError<GetObjectError>,
    log_target: &str,
) -> Result<OriginFetch, OriginPullError> {
    match err {
        SdkError::ServiceError(service_err) => {
            let status = service_err.raw().status().as_u16();
            let inner = service_err.into_err();
            // Modeled NoSuchKey is the canonical not-found signal.
            if matches!(inner, GetObjectError::NoSuchKey(_)) {
                return Ok(OriginFetch::NotFound);
            }
            // Bare HTTP 404 fallback: treat as NotFound when the AWS
            // error code is empty, explicitly `NoSuchKey`, or the SDK's
            // synthetic `NotFound` code (assigned when a 404 has no
            // parseable error body — e.g. some MinIO configurations).
            // Any *other* code on a 404 is a permission-disguise or
            // wrong-bucket failure (`NoSuchBucket`, `AccessDenied`) —
            // those bypass this branch and surface as `Permanent` so
            // the operator sees the real cause.
            if status == 404 {
                let code = inner.code().unwrap_or("");
                if code.is_empty()
                    || code.eq_ignore_ascii_case("NoSuchKey")
                    || code.eq_ignore_ascii_case("NotFound")
                {
                    return Ok(OriginFetch::NotFound);
                }
            }
            let context = format!(
                "{log_target}: S3 GetObject returned {status} ({})",
                inner.code().unwrap_or("<no error code>")
            );
            // `anyhow::Error::from(inner).context(...)` preserves the
            // SDK error in the source chain — `anyhow::Error::chain()`
            // walks through it for structured tracing, and downstream
            // downcasts (analogous to `CacheError::origin_error_kind`
            // for HttpOrigin) can recover the typed `GetObjectError`.
            // Using `anyhow::anyhow!("…{inner}")` would flatten the
            // chain to a single string and lose that.
            if is_transient_status(status) {
                Err(OriginPullError::Transient(
                    anyhow::Error::from(inner).context(context),
                ))
            } else {
                Err(OriginPullError::Permanent(
                    anyhow::Error::from(inner).context(context),
                ))
            }
        }
        // Network-layer transients: TCP/TLS connect failures, idle
        // timeouts, mid-stream disconnects. With the SDK's internal
        // retry layer disabled (see `S3Origin::new`), these surface
        // immediately on the first attempt; the cache engine's
        // outer `RetryPolicy` (config: `cache.origin_retry`) is the
        // single source of retry budget.
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) => {
            Err(OriginPullError::Transient(
                anyhow::Error::from(err)
                    .context(format!("{log_target}: S3 GetObject transport failure")),
            ))
        }
        // ConstructionFailure = the SDK could not even build the
        // request (e.g. impossible URL, bad credentials shape). Not
        // curable by retry. ResponseError = the SDK got a response
        // but failed to parse it; retrying against the same response
        // shape will keep failing.
        SdkError::ConstructionFailure(_) | SdkError::ResponseError(_) => {
            Err(OriginPullError::Permanent(
                anyhow::Error::from(err)
                    .context(format!("{log_target}: S3 GetObject permanent failure")),
            ))
        }
        // `SdkError` is `#[non_exhaustive]`. Conservatively classify
        // any future variant as Permanent so we don't retry forever
        // against a new failure mode the workspace doesn't yet
        // understand. Emit a `warn!` so an SDK upgrade that introduces
        // a *transient*-shaped variant we should be retrying is
        // operator-visible (file a ticket → adjust the classifier)
        // rather than silently treated as fail-fast.
        _ => {
            tracing::warn!(
                target: "decdn_cache::origin::s3",
                log_target = %log_target,
                sdk_error = %err,
                "S3 GetObject returned an SdkError variant this build does not classify; \
                 treating as Permanent. File a ticket if this fires after an aws-sdk-s3 bump."
            );
            Err(OriginPullError::Permanent(
                anyhow::Error::from(err)
                    .context(format!("{log_target}: S3 GetObject unrecognized failure")),
            ))
        }
    }
}

impl Origin for S3Origin {
    fn kind(&self) -> OriginKind {
        OriginKind::S3
    }

    fn fetch(
        &self,
        hash: Hash,
        max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            let key = key_for(&self.prefix, hash);
            // `s3://bucket/key` is the conventional log shape. Safe to
            // print: the bucket comes from operator-set config and the
            // key is `{prefix}{hex[..2]}/{hex}` over a content-addressed
            // BLAKE3 hash — no caller-controlled bytes, no credentials.
            // (Contrast with HttpOrigin's URL, which can carry userinfo
            // and is redacted via `redact_for_log` on every log line.)
            let log_target = format!("s3://{}/{}", self.bucket, key);

            let resp = match self
                .client
                .get_object()
                .bucket(self.bucket.as_ref())
                .key(&key)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => return classify_get_object_error(e, &log_target),
            };

            // Reject Content-Encoding upfront. The BLAKE3 verify in
            // `CacheEngine` runs over canonical bytes, so a gzipped
            // response would later trip a hash mismatch with a
            // confusing error. Decompression on this backend is a
            // follow-up issue (`HttpOrigin::decompress_body` is
            // reusable); until then, surface a clear pointer error
            // with operator-actionable workarounds.
            if let Some(enc) = resp.content_encoding() {
                let trimmed = enc.trim();
                if !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("identity") {
                    return Err(OriginPullError::Permanent(anyhow::anyhow!(
                        "{log_target}: the S3 backend does not yet support Content-Encoding \
                         decompression (got {trimmed:?}); either store canonical bytes or use \
                         the HTTP origin behind a CDN that strips encoding"
                    )));
                }
            }

            // Fast-path size check from the SDK-parsed Content-Length.
            // `content_length()` returns `Option<i64>` — negative is
            // a deterministic protocol violation; `> max_bytes` saves
            // us streaming bytes we'll throw away. The post-collect
            // length check below is the load-bearing defense against
            // an origin that lies in the header.
            if let Some(len) = resp.content_length() {
                if len < 0 {
                    return Err(OriginPullError::Permanent(anyhow::anyhow!(
                        "{log_target}: origin reported negative Content-Length={len}"
                    )));
                }
                #[allow(clippy::cast_sign_loss)] // checked >= 0 above.
                let len_u64 = len as u64;
                if len_u64 > max_bytes {
                    return Err(OriginPullError::Permanent(anyhow::anyhow!(
                        "{log_target}: Content-Length={len} exceeds max {max_bytes}"
                    )));
                }
            }

            // Collect the full body. PR2 buffers the entire payload before
            // returning — same approach as `HttpOrigin`. The cap is
            // `cache.max_blob_size_mb` (default 1 GB per
            // `decdn_common::config::DEFAULT_MAX_BLOB_SIZE_MB`); the only
            // upper bound is the runtime invariant
            // `max_blob_size_mb < cache_size_mb`. A streaming variant
            // is a follow-up if benchmarks show large blobs starve the
            // tokio executor.
            let body = resp
                .body
                .collect()
                .await
                .map_err(|e| {
                    // `aws_smithy_types::byte_stream::error::Error` doesn't
                    // expose a clean is-transient API; collect failures are
                    // typically mid-stream disconnects (genuinely transient)
                    // but can also be smithy-side deserialization faults
                    // (would be permanent). With the SDK's internal retry
                    // layer disabled, classifying conservatively as
                    // `Transient` lets `cache.origin_retry` take a crack —
                    // a deterministic deserialize fault will exhaust the
                    // outer budget and surface as `CacheError::OriginError`.
                    OriginPullError::Transient(
                        anyhow::Error::from(e)
                            .context(format!("{log_target}: body collect failed")),
                    )
                })?
                .into_bytes();

            // Defense in depth: an origin can lie in the Content-Length
            // header (or omit it for `Transfer-Encoding: chunked`
            // responses), so re-check the actual byte count. This
            // mirrors `HttpOrigin`'s post-stream cap and `CacheEngine`'s
            // own re-check.
            if body.len() as u64 > max_bytes {
                return Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "{log_target}: body is {} bytes, exceeds max {max_bytes}",
                    body.len()
                )));
            }

            // `into_bytes()` already returns `bytes::Bytes`; just hand it on
            // to `OriginFetch::Found` (no extra clone or wrap).
            Ok(OriginFetch::Found(body))
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
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
            super::super::parse_origin_url("http://minio.invalid:9000/")
                .expect("test URL must parse"),
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
}
