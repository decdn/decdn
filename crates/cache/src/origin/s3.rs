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
use iroh_blobs::Hash;

use super::{Origin, OriginFetch, OriginUrl};
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
#[derive(Debug, Clone)]
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
/// `&str`/`String`), so the redaction discipline is enforced at the
/// config / log boundary, not inside the SDK call site.
#[derive(Debug, Clone)]
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
    /// Build an `S3Origin` from validated config. Performs no network I/O —
    /// the SDK lazily connects on the first `fetch`. Returns an error only
    /// if the underlying SDK config builder rejects the inputs (e.g. a
    /// region string that fails to parse).
    ///
    /// `BehaviorVersion::latest()` is set explicitly here even though the
    /// `behavior-version-latest` cargo feature on `aws-config` would
    /// implicitly do the same. Belt-and-braces against a future feature-
    /// flag edit causing a silent panic in the SDK's `ClientBuilder::build`
    /// (the SDK panics if neither path is taken — its only `unwrap` on the
    /// construction hot path).
    pub async fn new(cfg: &S3OriginConfig) -> anyhow::Result<Self> {
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

        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .http_client(http_client)
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
    /// outside the crate; `#[doc(hidden)]` keeps it out of the public
    /// API surface. Production code uses [`Self::new`].
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

/// Classify a `SdkError<GetObjectError>`. Returns
/// `Ok(OriginFetch::NotFound)` for `NoSuchKey` (and the equivalent
/// 404 status — some non-AWS endpoints may surface the not-found case
/// without the modeled `NoSuchKey` variant).
///
/// Errors that may be cured by retry (timeouts, dispatch failures,
/// 5xx/408/429) become `Transient`; everything else (4xx, construction
/// failures, response-parse failures, Glacier-cold objects) becomes
/// `Permanent`.
///
/// `SdkError` is `#[non_exhaustive]`, so the catch-all arm conservatively
/// classifies any unknown future variant as `Permanent` (better to surface
/// a clear failure than retry indefinitely against a misclassification).
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
            // Some non-AWS S3 endpoints emit a bare 404 without the
            // modeled NoSuchKey error (notably MinIO under certain
            // configurations). Treat those as NotFound too — this
            // matches HttpOrigin's status-based 404 path.
            if status == 404 {
                return Ok(OriginFetch::NotFound);
            }
            let msg = format!("{log_target}: S3 GetObject returned {status}: {inner}");
            if is_transient_status(status) {
                Err(OriginPullError::Transient(anyhow::anyhow!(msg)))
            } else {
                Err(OriginPullError::Permanent(anyhow::anyhow!(msg)))
            }
        }
        // Network-layer transients: TCP/TLS connect failures, idle
        // timeouts, mid-stream disconnects. The SDK's own retry layer
        // has already exhausted whatever budget it was given before
        // surfacing these — wrapping them in `Transient` lets our
        // outer `RetryPolicy` (config: `cache.origin_retry`) take a
        // second crack from cold.
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) => {
            let msg = format!("{log_target}: S3 GetObject transport failure: {err}");
            Err(OriginPullError::Transient(anyhow::anyhow!(msg)))
        }
        // ConstructionFailure = the SDK could not even build the
        // request (e.g. impossible URL, bad credentials shape). Not
        // curable by retry. ResponseError = the SDK got a response
        // but failed to parse it; retrying against the same response
        // shape will keep failing.
        SdkError::ConstructionFailure(_) | SdkError::ResponseError(_) => {
            let msg = format!("{log_target}: S3 GetObject permanent failure: {err}");
            Err(OriginPullError::Permanent(anyhow::anyhow!(msg)))
        }
        // `SdkError` is `#[non_exhaustive]`. Conservatively classify
        // any future variant as Permanent so we don't retry forever
        // against a new failure mode the workspace doesn't yet
        // understand.
        _ => {
            let msg = format!("{log_target}: S3 GetObject unrecognized failure: {err}");
            Err(OriginPullError::Permanent(anyhow::anyhow!(msg)))
        }
    }
}

impl Origin for S3Origin {
    fn fetch(
        &self,
        hash: Hash,
        max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            let key = key_for(&self.prefix, hash);
            // `s3://bucket/key` is the conventional log shape and is
            // safe to print — bucket and key are operator-chosen, not
            // sourced from a request, so no userinfo / token leakage
            // path exists here (unlike HttpOrigin's URL-with-userinfo
            // case).
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
            // returning — same approach as `HttpOrigin` and acceptable
            // up to `max_blob_size_mb` (default 128 MB; runtime ceiling
            // 10 GB but operators rarely touch that). A streaming variant
            // is a follow-up if benchmarks show large blobs starve the
            // tokio executor.
            let body = resp
                .body
                .collect()
                .await
                .map_err(|e| {
                    OriginPullError::Transient(anyhow::anyhow!(
                        "{log_target}: body collect failed: {e}"
                    ))
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
}
