//! S3-compatible origin backend (#437).
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
//! the daemon's only use of aws-lc (every other TLS client in the workspace,
//! the client SDK and the CLI included, runs on ring). The SDK's stock HTTP stack
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
use bytes::{Bytes, BytesMut};
use iroh_blobs::Hash;

use super::fs::OBAO4_SUFFIX;
use super::io_runtime::IoRuntime;
use super::{Origin, OriginKind, OriginRangeFetch, OriginRangeRequest, OriginUrl, OutboardFetch};
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
    /// The runtime every SDK request send runs on, so the HTTP client's
    /// pooled keep-alive connections live there and not on a caller's
    /// pull-leg runtime. See [`super::io_runtime`].
    io: IoRuntime,
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
    /// for SSO and the cached SSO token has expired, the *first* read
    /// may fire an HTTP request to the SSO endpoint to refresh the
    /// token. That's a property of the AWS credential chain, not this
    /// constructor; the construction itself stays I/O-free.
    ///
    /// `BehaviorVersion::latest()` is set explicitly even though
    /// `aws-config`'s `behavior-version-latest` feature would imply the
    /// same. Belt-and-braces against a future feature-flag edit dropping
    /// the implicit path — older SDK versions panicked when no
    /// `BehaviorVersion` was set; current versions surface a runtime error.
    /// Either way, the explicit call site cannot regress.
    ///
    /// **Runtime:** the origin's pooled connections live on the tokio
    /// runtime this future runs on, and every later request send runs there
    /// (see `origin::io_runtime`). Build it on a runtime that outlives every
    /// fetch — the node's main runtime — never on a short-lived one such as a
    /// pull leg's.
    ///
    /// # Errors
    ///
    /// Fails when a `Static` credential carries an empty key or secret, or
    /// when this future does not run inside a tokio runtime.
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
        // back-to-back cache misses. Every send runs on `io`, so each pooled
        // connection lives on the runtime that built this origin, never on a
        // caller's pull-leg runtime (`super::io_runtime`).
        let http_client = HttpBuilder::new()
            .tls_provider(tls::Provider::Rustls(
                tls::rustls_provider::CryptoMode::AwsLc,
            ))
            .build_https();

        // Disable the SDK's internal retry layer. Without this, transient
        // failures get retried *twice*: once by the SDK (default 3 attempts)
        // and again by the cache engine's outer retry loop (default 4
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

        Self::from_parts(client, cfg.bucket.as_str(), cfg.prefix.as_str())
    }

    /// Assemble an origin from a pre-built `Client`, skipping
    /// `aws_config::defaults`. [`Self::new`] builds its client and then calls
    /// this. `tests/s3_origin.rs` calls it to inject
    /// `aws_smithy_mocks::mock_client!`-produced clients, which can't be
    /// obtained via `aws_config::defaults`.
    ///
    /// `pub` rather than `pub(crate)` because the integration tests live
    /// outside the crate. `#[doc(hidden)]` excludes the function from
    /// generated rustdoc only — it is technically callable by downstream
    /// crates and is part of the semver surface by convention. Other
    /// callers use [`Self::new`].
    ///
    /// The origin's connections live on the tokio runtime this is called
    /// on.
    ///
    /// # Errors
    ///
    /// Fails outside a tokio runtime.
    #[doc(hidden)]
    pub fn from_parts(client: Client, bucket: &str, prefix: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            io: IoRuntime::current()?,
            bucket: Arc::from(bucket),
            prefix: Arc::from(prefix),
        })
    }

    /// `GetObject` `key` (with `range`, when set) and answer the response, or
    /// `Ok(None)` when the object is missing. `log_target` names the object
    /// in errors.
    async fn get_object(
        &self,
        key: &str,
        range: Option<String>,
        log_target: &str,
    ) -> Result<Option<aws_sdk_s3::operation::get_object::GetObjectOutput>, OriginPullError> {
        let sent = self
            .io
            .run(
                self.client
                    .get_object()
                    .bucket(self.bucket.as_ref())
                    .key(key)
                    .set_range(range)
                    .send(),
            )
            .await
            .map_err(|e| e.map_inner(|e| e.context(format!("{log_target}: S3 GetObject"))))?;
        match sent {
            Ok(resp) => Ok(Some(resp)),
            Err(e) => classify_get_object_error(e, log_target).map(|Missing| None),
        }
    }
}

/// The object a `GetObject` asked for does not exist.
struct Missing;

/// Classify an HTTP status code returned via an S3 `ServiceError`.
/// 5xx and the two retry-flagged 4xx codes (408 Request Timeout, 429
/// Too Many Requests) are transient; everything else is permanent.
const fn is_transient_status(status: u16) -> bool {
    status >= 500 || status == 408 || status == 429
}

/// Classify a `SdkError<GetObjectError>` as a missing object or an
/// [`OriginPullError`].
///
/// Returns `Ok(Missing)` only when the response is
/// genuinely "object does not exist": either the modeled `NoSuchKey`
/// variant, or an HTTP 404 whose AWS error code is `NoSuchKey`/empty.
/// A 404 with any other code (`NoSuchBucket`, `AccessDenied`, ...) is
/// **not** treated as `NotFound` — those are config / permission
/// failures that AWS sometimes dresses up as 404 (e.g. when the
/// principal lacks `s3:ListBucket` it returns 404 for missing objects;
/// a typo'd bucket also surfaces as a non-modeled 404). Misclassifying
/// those as missing would silently mask the operator's real problem.
///
/// Errors that may be cured by retry (timeouts, dispatch failures,
/// 5xx/408/429) become `Transient`; everything else (other 4xx,
/// construction failures, response-parse failures, Glacier-cold objects)
/// becomes `Permanent`. The cache engine's retry loop
/// (`crate::retry::run_with_retry_classified`) drives the retry budget off
/// this distinction.
///
/// `SdkError` is `#[non_exhaustive]`. The catch-all arm classifies
/// future variants as `Permanent` (fail fast over retry-storm) and
/// emits a `tracing::warn!` so an SDK upgrade that adds a transient-
/// shaped variant we don't yet recognize is operator-visible rather
/// than silently misclassified.
fn classify_get_object_error(
    err: SdkError<GetObjectError>,
    log_target: &str,
) -> Result<Missing, OriginPullError> {
    match err {
        SdkError::ServiceError(service_err) => {
            let status = service_err.raw().status().as_u16();
            let inner = service_err.into_err();
            // Modeled NoSuchKey is the canonical not-found signal.
            if matches!(inner, GetObjectError::NoSuchKey(_)) {
                return Ok(Missing);
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
                    return Ok(Missing);
                }
            }
            let context = format!(
                "{log_target}: S3 GetObject returned {status} ({})",
                inner.code().unwrap_or("<no error code>")
            );
            // `anyhow::Error::from(inner).context(...)` preserves the
            // SDK error in the source chain — `anyhow::Error::chain()`
            // walks through it for structured tracing, and a downcast can
            // recover the typed `GetObjectError`. Using
            // `anyhow::anyhow!("…{inner}")` would flatten the chain to a
            // single string and lose that.
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

/// The permanent fault for an object at `log_target` that ran past its
/// `cap`.
fn oversize(log_target: &str, cap: u64) -> OriginPullError {
    OriginPullError::Permanent(anyhow::anyhow!(
        "{log_target}: object is over its {cap}-byte cap; re-run `decdn origin import` for \
         the object"
    ))
}

impl Origin for S3Origin {
    fn kind(&self) -> OriginKind {
        OriginKind::S3
    }

    fn fetch_range_data(
        &self,
        hash: Hash,
        req: OriginRangeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            let data_key = key_for(&self.prefix, hash);
            // Empty span only for a zero-length blob.
            if req.is_empty() {
                return Ok(OriginRangeFetch::Ranged { data: Bytes::new() });
            }

            // Ranged data read. S3 `Range` is inclusive-end (`bytes=a-b`),
            // matching HTTP. S3 answers `206` for an honored range; the SDK
            // surfaces that transparently, so we validate by exact returned
            // length instead of inspecting the status (a server that ignored
            // the range returns the whole object and trips the length check).
            let range_val = format!("bytes={}-{}", req.fetch_start, req.fetch_end - 1);
            let want = req.len();
            // `s3://bucket/key` is the conventional log shape. Safe to print:
            // the bucket comes from operator-set config and the key is
            // `{prefix}{hex[..2]}/{hex}` over a content-addressed BLAKE3 hash —
            // no caller-controlled bytes, no credentials.
            let log_target = format!("s3://{}/{} (range {range_val})", self.bucket, data_key);
            let Some(resp) = self
                .get_object(&data_key, Some(range_val), &log_target)
                .await?
            else {
                return Ok(OriginRangeFetch::NotFound);
            };
            // A ranged GET of a stored-compressed object returns encoded bytes,
            // which break the offset→byte mapping the bao proof anchors on.
            if resp
                .content_encoding()
                .is_some_and(|e| !e.trim().is_empty())
            {
                return Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "{log_target}: object carries a Content-Encoding; store canonical bytes \
                     with `decdn origin import`"
                )));
            }
            let wrong_length = |got: u64| {
                OriginPullError::Permanent(anyhow::anyhow!(
                    "{log_target}: answered {got} bytes for a {want}-byte range; the object \
                     is short or the origin ignores `Range`"
                ))
            };
            // Fail on a `Content-Length` that doesn't match the requested span
            // BEFORE collecting the body: an origin that ignored `Range` (and is
            // about to stream the whole object) advertises the full length
            // here. The exact-length gate on the collected bytes below is the
            // load-bearing check; this only avoids reading a body already known
            // to be wrong-sized.
            if let Some(len) = resp.content_length() {
                let len = u64::try_from(len).unwrap_or(u64::MAX);
                if len != want {
                    return Err(wrong_length(len));
                }
            }
            let want_usize = usize::try_from(want).unwrap_or(usize::MAX);
            let Some(data) = collect_bounded(resp.body, want_usize).await? else {
                return Err(wrong_length(want.saturating_add(1)));
            };
            // Exact-length gate: a server that ignored `Range` (returned the
            // whole object) or returned multipart bytes is rejected here.
            let got = u64::try_from(data.len()).unwrap_or(u64::MAX);
            if got != want {
                return Err(wrong_length(got));
            }
            Ok(OriginRangeFetch::Ranged { data })
        })
    }

    fn fetch_outboard(
        &self,
        hash: Hash,
        outboard_max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            let data_key = key_for(&self.prefix, hash);
            let obao4_key = format!("{data_key}{OBAO4_SUFFIX}");
            let log_target = format!("s3://{}/{}", self.bucket, obao4_key);
            let Some(resp) = self.get_object(&obao4_key, None, &log_target).await? else {
                return Ok(OutboardFetch::NotFound);
            };
            // An outboard over the cap is malformed or foreign, for example a
            // chunk-level outboard from another tool.
            if let Some(len) = resp.content_length()
                && (len < 0 || u64::try_from(len).unwrap_or(u64::MAX) > outboard_max_bytes)
            {
                return Err(oversize(&log_target, outboard_max_bytes));
            }
            let cap = usize::try_from(outboard_max_bytes).unwrap_or(usize::MAX);
            let Some(bytes) = collect_bounded(resp.body, cap).await? else {
                return Err(oversize(&log_target, outboard_max_bytes));
            };
            Ok(OutboardFetch::Found(bytes))
        })
    }

    fn fetch_size_proof(
        &self,
        hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            let key = format!(
                "{}{}",
                key_for(&self.prefix, hash),
                decdn_bao_range::SIZE_PROOF_SUFFIX
            );
            let log_target = format!("s3://{}/{}", self.bucket, key);
            let Some(resp) = self.get_object(&key, None, &log_target).await? else {
                return Ok(None);
            };
            let cap = decdn_bao_range::MAX_SIZE_PROOF_OBJECT_BYTES;
            match collect_bounded(resp.body, cap).await? {
                Some(object) => Ok(Some(object)),
                None => Err(oversize(&log_target, cap as u64)),
            }
        })
    }
}

/// Drain an S3 `ByteStream` into `Bytes`, returning `Ok(None)` the moment the
/// cumulative body exceeds `cap`. Streams chunk-by-chunk (via
/// `ByteStream::try_next`) and aborts on the first over-cap chunk WITHOUT
/// buffering the rest — a misbehaving origin that ignores `Range` and streams a
/// huge object (or serves an oversized `.obao4`) can't force a whole-body
/// allocation. This mirrors the bounded streaming reader in
/// [`crate::origin::http`] (`collect_capped`); `ByteStream::collect()` is
/// deliberately NOT used because it buffers the entire body before any cap
/// check. Transport errors mid-body surface as `Transient`.
async fn collect_bounded(
    mut body: aws_sdk_s3::primitives::ByteStream,
    cap: usize,
) -> Result<Option<Bytes>, OriginPullError> {
    let mut buf = BytesMut::new();
    loop {
        match body.try_next().await {
            Ok(None) => break,
            Ok(Some(chunk)) => {
                if buf.len().saturating_add(chunk.len()) > cap {
                    // Over the bound: stop here rather than keep draining the
                    // body. The caller reports the oversize object.
                    return Ok(None);
                }
                buf.extend_from_slice(&chunk);
            }
            Err(e) => {
                return Err(OriginPullError::Transient(
                    anyhow::Error::from(e).context("S3 range body read failed"),
                ));
            }
        }
    }
    Ok(Some(buf.freeze()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests;
