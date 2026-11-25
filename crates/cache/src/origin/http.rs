//! HTTP origin backend.
//!
//! Reads blobs from `{base_url}/{blake3_hex}` and their `{hex}.obao4` and
//! `{hex}.sizeproof` siblings. The base URL is opaque per-node config; it is
//! never put on the wire (see ADR 012 — "no external origin URLs are ever
//! exposed").

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Context;
use bytes::{Bytes, BytesMut};
use iroh_blobs::Hash;
use reqwest::StatusCode;
use reqwest::header::RANGE;

use super::fs::OBAO4_SUFFIX;
use super::io_runtime::IoRuntime;
use super::{
    DEFAULT_USER_AGENT, Origin, OriginKind, OriginRangeFetch, OriginRangeRequest, OriginUrl,
    OutboardFetch, parse_origin_url, redact_for_log,
};
use crate::error::OriginPullError;

/// How long to wait for the TCP/TLS handshake to complete. Per-request total
/// duration is intentionally *not* bounded because a range window or an
/// outboard can be large, and no single total-request timeout fits both small
/// and large reads. Instead, we pair this with phase-level timeouts below.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Ceiling on how long the origin may take to respond with headers after
/// the connection is established. Without this, a compromised or misbehaving
/// origin could accept the connection and then stall indefinitely, tying up
/// a fetch task with no bytes in flight to catch it any other way.
const RESPONSE_HEADERS_TIMEOUT: Duration = Duration::from_secs(30);

/// Ceiling on the idle gap between body chunks. Bounds the time a
/// slow-trickle origin can tie up a task — total bytes are capped per read,
/// but without this check a pathological origin could send a single byte per
/// second within the cap and keep the task alive for days.
const CHUNK_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Origin backed by a plain HTTP(S) endpoint serving content-addressed blobs
/// at `{base_url}/{blake3_hex}`.
#[derive(Debug, Clone)]
pub struct HttpOrigin {
    client: reqwest::Client,
    /// The runtime every request send runs on, so the client's pooled
    /// keep-alive connections live there and not on a caller's pull-leg
    /// runtime. See [`super::io_runtime`].
    io: IoRuntime,
    base_url: OriginUrl,
    response_headers_timeout: Duration,
    chunk_idle_timeout: Duration,
    /// Whether a `403` answer has already logged at warn. An origin in front
    /// of a bucket without `s3:ListBucket` answers 403 for every missing key,
    /// so the first one warns and the rest log at debug. Shared across
    /// clones, which are the same origin.
    forbidden_warned: Arc<AtomicBool>,
}

impl HttpOrigin {
    /// Build an [`HttpOrigin`] around a validated [`OriginUrl`]. The only way
    /// to obtain one is [`parse_origin_url`], so invariants are enforced at
    /// the type boundary rather than documented in prose. Timeouts default
    /// to the `RESPONSE_HEADERS_TIMEOUT` and `CHUNK_IDLE_TIMEOUT` constants.
    ///
    /// Sets [`DEFAULT_USER_AGENT`] on the inner reqwest client so origin
    /// access logs can attribute CDN pull-through traffic (#435). Override
    /// via [`Self::new_with_user_agent`].
    ///
    /// The origin's connections live on the tokio runtime this is called
    /// on, so call it on a runtime that outlives every fetch (the node's
    /// main runtime).
    ///
    /// # Errors
    ///
    /// Fails outside a tokio runtime, or when the reqwest client cannot be
    /// built.
    pub fn new(base_url: OriginUrl) -> anyhow::Result<Self> {
        Self::new_with_user_agent(base_url, DEFAULT_USER_AGENT)
    }

    /// Like [`Self::new`] but with a caller-supplied `User-Agent`. Used by
    /// the runtime to honour an operator-configured `cache.user_agent`
    /// without rebuilding the client through a separate code path.
    /// `user_agent` must be a valid header value (visible-ASCII; `reqwest`
    /// rejects others at build time and the error is surfaced as a
    /// startup failure).
    ///
    /// # Errors
    ///
    /// Fails outside a tokio runtime, or when the reqwest client cannot be
    /// built (an invalid `user_agent`).
    pub fn new_with_user_agent(base_url: OriginUrl, user_agent: &str) -> anyhow::Result<Self> {
        // Disable reqwest's transparent decompression: a range window is
        // verified byte for byte against the bao tree, so the bytes must be
        // the ones the origin stored, never a decoded body.
        //
        // `redirect::Policy::none()` defuses SSRF (#579): the configured
        // base URL is validated by `parse_origin_url`, but reqwest's
        // default policy (follow up to 10) would let a compromised or
        // misconfigured origin 3xx-redirect us to `http://169.254.169.254`
        // (cloud metadata), `http://127.0.0.1`, or any RFC-1918 host —
        // the request lands on the internal target before BLAKE3
        // verification gets a chance to fire on the body. Origins must
        // serve `{base}/{hex}` directly anyway (query/fragment are
        // already rejected by `parse_origin_url`), so a redirect was
        // never a legitimate response shape.
        //
        // reqwest's idle keep-alive pool is safe to share across callers:
        // `send` runs every request on `io`, so each pooled connection lives
        // on the runtime that built this origin, never on a caller's
        // pull-leg runtime (`super::io_runtime`).
        //
        // The workspace builds reqwest with `rustls-no-provider`, so
        // `build` panics without a process-wide rustls provider. Install
        // ring, as `decdn_common::tls::install_crypto_provider` does (this
        // crate does not depend on `decdn-common`); a no-op once installed.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .user_agent(user_agent)
            .redirect(reqwest::redirect::Policy::none())
            .no_gzip()
            .no_deflate()
            .no_brotli()
            .no_zstd()
            .build()
            .context("failed to build reqwest client")?;
        Ok(Self {
            client,
            io: IoRuntime::current()?,
            base_url,
            response_headers_timeout: RESPONSE_HEADERS_TIMEOUT,
            chunk_idle_timeout: CHUNK_IDLE_TIMEOUT,
            forbidden_warned: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Log a `403` the origin answered for `url_log`, read as a missing key:
    /// at warn the first time on this origin, at debug after that.
    fn log_forbidden(&self, url_log: &str) {
        if self.forbidden_warned.swap(true, Ordering::Relaxed) {
            tracing::debug!(url = %url_log, "origin answered 403; reading it as a missing object");
        } else {
            tracing::warn!(
                url = %url_log,
                "origin answered 403; reading it as a missing object, as an origin in front \
                 of a bucket without list permission answers for a missing key. If the \
                 object exists, check the origin's permissions (further 403s log at debug)",
            );
        }
    }

    /// Convenience constructor that parses `raw` via [`parse_origin_url`]
    /// before delegating to [`Self::new`]. Prefer [`Self::new`] in
    /// runtime paths so URL validation happens at config-load time.
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        Self::new(parse_origin_url(raw)?)
    }

    /// Override the phase timeouts.
    #[cfg(test)]
    #[must_use]
    pub const fn with_timeouts(
        mut self,
        response_headers_timeout: Duration,
        chunk_idle_timeout: Duration,
    ) -> Self {
        self.response_headers_timeout = response_headers_timeout;
        self.chunk_idle_timeout = chunk_idle_timeout;
        self
    }

    /// The URL of `name` under the base URL.
    fn url_for(&self, name: &str, hash: Hash) -> Result<reqwest::Url, OriginPullError> {
        // URL build can only fail on a malformed hash or a pathological base
        // URL — both are caller-side bugs, never transient.
        self.base_url
            .as_url()
            .join(name)
            .with_context(|| format!("failed to build URL for {hash}"))
            .map_err(OriginPullError::Permanent)
    }
}

/// Classify a `reqwest::Error` raised by `.send()` or `.chunk()`. The
/// transient bucket spans every realistic transport-layer failure on
/// this code path:
///
/// - `is_connect()` — TCP/TLS handshake failed.
/// - `is_timeout()` — reqwest's own timeout fired (separate from our
///   `tokio::time::timeout` wrappers, which produce `Elapsed` and never
///   reach this helper).
/// - `is_request()` — request-construction problems that *can* fire on
///   a parsed URL (e.g. invalid header value injected by reqwest mid-
///   redirect).
/// - `is_body()` — body stream ended unexpectedly. Could in principle
///   be a deterministic origin protocol violation (mid-stream framing
///   error), but in operational practice this fires on connection
///   resets and is worth retrying. Tradeoff is bounded by `max_retries`.
/// - `is_decode()` — character-set / chunked-transfer parse failure.
///   With reqwest auto-decompression disabled (see `HttpOrigin::new`)
///   this does not fire for gzip/zstd; the remaining triggers are
///   protocol-level and retrying *may* mask a deterministic bug, but
///   the `max_retries` ceiling bounds the cost.
///
/// The `else` arm covers `is_builder()` (unreachable with a parsed
/// `OriginUrl`), `is_redirect()` (unreachable with `Policy::none()` —
/// 3xx responses are returned as `resp.status()` instead of as a
/// reqwest error; see `HttpOrigin::new_with_user_agent`), `is_status()`
/// (we handle status codes ourselves before reaching here), and any
/// future reqwest variant. Permanent because none of those are
/// operationally retriable.
fn classify_reqwest_error(e: reqwest::Error) -> OriginPullError {
    if e.is_connect() || e.is_timeout() || e.is_request() || e.is_body() || e.is_decode() {
        OriginPullError::Transient(e.into())
    } else {
        OriginPullError::Permanent(e.into())
    }
}

/// Status-code classification per RFC 9110 + operational practice:
/// 5xx, 408 (Request Timeout), and 429 (Too Many Requests) are retriable;
/// every other 4xx is permanent. `404` and `403` are intercepted earlier
/// ([`HttpOrigin::missing_or_fault`]); this helper assumes the caller already
/// excluded them.
fn is_transient_status(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
}

/// How [`HttpOrigin::missing_or_fault`] reads a `403`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Forbidden {
    /// A missing object. An origin in front of a bucket without list
    /// permission answers 403 for a missing key. The outboard and data
    /// reads run only after a presence answer, so a missing object there is
    /// a fault either way.
    Missing,
    /// A fault. The `{H}.sizeproof` read is the presence answer, and an
    /// absence there is memoised and signed, so a refusal must not read as
    /// one.
    Fault,
}

impl Origin for HttpOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch_range_data(
        &self,
        hash: Hash,
        req: OriginRangeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            // Ranged data read. `Range: bytes=a-(b-1)` is inclusive-end. A
            // compliant origin answers `206 Partial Content` with exactly the
            // requested span (see `get_range_bytes`).
            let data_url = self.url_for(&hash.to_hex(), hash)?;
            // Empty span only for a zero-length blob — nothing to range.
            if req.is_empty() {
                return Ok(OriginRangeFetch::Ranged { data: Bytes::new() });
            }
            // Inclusive end: HTTP byte ranges are `[a, b]`, our span is
            // `[fetch_start, fetch_end)`. `fetch_end > fetch_start` here (the
            // empty-span case returned above), so the subtraction is sound.
            let range_val = format!("bytes={}-{}", req.fetch_start, req.fetch_end - 1);
            let want = req.len();
            let Some(data) = self.get_range_bytes(&data_url, &range_val, want).await? else {
                return Ok(OriginRangeFetch::NotFound);
            };
            Ok(OriginRangeFetch::Ranged { data })
        })
    }

    fn fetch_outboard(
        &self,
        hash: Hash,
        outboard_max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            let url = self.url_for(&format!("{}{OBAO4_SUFFIX}", hash.to_hex()), hash)?;
            Ok(
                match self
                    .get_bounded(&url, outboard_max_bytes, Forbidden::Missing)
                    .await?
                {
                    Some(bytes) => OutboardFetch::Found(bytes),
                    None => OutboardFetch::NotFound,
                },
            )
        })
    }

    fn fetch_size_proof(
        &self,
        hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            let url = self.url_for(
                &format!("{}{}", hash.to_hex(), decdn_bao_range::SIZE_PROOF_SUFFIX),
                hash,
            )?;
            let max = decdn_bao_range::MAX_SIZE_PROOF_OBJECT_BYTES as u64;
            // A 403 here is a fault, not an absence: the size proof is the
            // presence answer, and an absence is memoised and signed as one.
            self.get_bounded(&url, max, Forbidden::Fault).await
        })
    }
}

impl HttpOrigin {
    /// Send `req` on [`Self::io`] and wait for the response headers.
    /// `what` names the request in errors (`GET {url}`, already redacted).
    ///
    /// The client's `connect_timeout` covers the TCP/TLS handshake. The
    /// headers timeout covers the whole send — request write through
    /// response-header receipt, plus a connect when no idle connection is
    /// pooled — and bounds a server that accepts the connection but never
    /// writes back. The deadline runs on the caller's runtime, so it also
    /// fires when `io` stops polling the send; dropping the send then aborts
    /// the spawned task.
    ///
    /// # Errors
    ///
    /// A headers timeout is [`OriginPullError::Transient`]. A transport fault
    /// is classified by `classify_reqwest_error`. A fault of the spawned task
    /// itself comes from [`IoRuntime::run`].
    async fn send(
        &self,
        req: reqwest::RequestBuilder,
        what: &str,
    ) -> Result<reqwest::Response, OriginPullError> {
        let headers_timeout = self.response_headers_timeout;
        let sent = tokio::time::timeout(headers_timeout, self.io.run(req.send()))
            .await
            .map_err(|_elapsed| {
                OriginPullError::Transient(anyhow::anyhow!(
                    "origin {what} headers timed out after {headers_timeout:?}"
                ))
            })?
            .map_err(|e| e.map_inner(|e| e.context(format!("origin {what} failed"))))?;
        sent.map_err(|reqwest_err| {
            classify_reqwest_error(reqwest_err)
                .map_inner(|e| e.context(format!("origin {what} failed")))
        })
    }

    /// Classify a non-success `status` the origin answered to `what` on
    /// `url_log`. `Ok(())` means the object is missing: a 404, or a 403 under
    /// [`Forbidden::Missing`], which an origin in front of a bucket answers
    /// for a missing key. A retriable status (5xx/408/429 per
    /// [`is_transient_status`]) is an [`OriginPullError::Transient`]. A
    /// redirect (disabled per #579), a 403 under [`Forbidden::Fault`] and
    /// every other status are an [`OriginPullError::Permanent`].
    fn missing_or_fault(
        &self,
        status: StatusCode,
        url_log: &str,
        what: &str,
        forbidden: Forbidden,
    ) -> Result<(), OriginPullError> {
        if status == StatusCode::NOT_FOUND {
            return Ok(());
        }
        if status == StatusCode::FORBIDDEN {
            return match forbidden {
                Forbidden::Missing => {
                    self.log_forbidden(url_log);
                    Ok(())
                }
                Forbidden::Fault => Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "origin {what} {url_log} answered {status}; the node cannot tell a \
                     missing object from a refused one, so check the origin's permissions \
                     (an origin must answer 404 for a key it does not hold)"
                ))),
            };
        }
        if status.is_redirection() {
            return Err(OriginPullError::Permanent(anyhow::anyhow!(
                "origin {what} {url_log} returned {status}; redirects are disabled to prevent \
                 SSRF — origin must serve {{base}}/{{hex}} directly"
            )));
        }
        let err = anyhow::anyhow!("origin {what} {url_log} answered {status}");
        Err(if is_transient_status(status) {
            OriginPullError::Transient(err)
        } else {
            OriginPullError::Permanent(err)
        })
    }

    /// GET `url` and buffer the whole body, capped at `max_bytes`. `Ok(None)`
    /// when the object is missing ([`Self::missing_or_fault`] under
    /// `forbidden`). A body over the cap is malformed or foreign (for example
    /// a chunk-level outboard from another tool) and is an
    /// [`OriginPullError::Permanent`]. A transport fault on the send or the
    /// body read is an [`OriginPullError`] too.
    async fn get_bounded(
        &self,
        url: &reqwest::Url,
        max_bytes: u64,
        forbidden: Forbidden,
    ) -> Result<Option<Bytes>, OriginPullError> {
        let url_log = redact_for_log(url);
        let resp = self
            .send(self.client.get(url.clone()), &format!("GET {url_log}"))
            .await?;
        let status = resp.status();
        if !status.is_success() {
            self.missing_or_fault(status, &url_log, "GET", forbidden)?;
            return Ok(None);
        }
        let oversize = || {
            OriginPullError::Permanent(anyhow::anyhow!(
                "origin GET {url_log} body is over its {max_bytes}-byte cap; re-run `decdn \
                 origin import` for the object"
            ))
        };
        if resp.content_length().is_some_and(|len| len > max_bytes) {
            return Err(oversize());
        }
        match self
            .collect_capped(resp, max_bytes, url_log.clone())
            .await?
        {
            Some(bytes) => Ok(Some(bytes)),
            None => Err(oversize()),
        }
    }

    /// GET `url` with a `Range` header and buffer the partial body. `Ok(None)`
    /// when the object is missing ([`Self::missing_or_fault`]). Any other
    /// answer that is not a `206` of exactly `want` bytes — a `200` whole-blob
    /// response from an origin that ignores `Range`, a multipart or truncated
    /// body — is an [`OriginPullError::Permanent`]. `want` is both the buffer
    /// ceiling and the exact-length check, so an origin that streams the whole
    /// object on a `200` is rejected at the header (status) before any large
    /// buffer is committed.
    async fn get_range_bytes(
        &self,
        url: &reqwest::Url,
        range_val: &str,
        want: u64,
    ) -> Result<Option<Bytes>, OriginPullError> {
        let url_log = redact_for_log(url);
        let resp = self
            .send(
                self.client.get(url.clone()).header(RANGE, range_val),
                &format!("GET {url_log} (range)"),
            )
            .await?;
        let status = resp.status();
        if !status.is_success() {
            self.missing_or_fault(status, &url_log, "GET (range)", Forbidden::Missing)?;
            return Ok(None);
        }
        // Only `206 Partial Content` is an honored range. A `200` means the
        // origin ignored `Range` and is sending the whole blob — refuse
        // before reading the (potentially huge) body.
        if status != StatusCode::PARTIAL_CONTENT {
            return Err(OriginPullError::Permanent(anyhow::anyhow!(
                "origin GET {url_log} (range) answered {status}, not 206; the origin must \
                 serve byte ranges"
            )));
        }
        let wrong_length = |got: u64| {
            OriginPullError::Permanent(anyhow::anyhow!(
                "origin GET {url_log} (range {range_val}) answered {got} bytes for a \
                 {want}-byte range"
            ))
        };
        // A `206` whose advertised `Content-Length` already differs from the
        // requested span fails BEFORE the body is collected.
        if let Some(len) = resp.content_length()
            && len != want
        {
            return Err(wrong_length(len));
        }
        let Some(bytes) = self.collect_capped(resp, want, url_log.clone()).await? else {
            return Err(wrong_length(want.saturating_add(1)));
        };
        // A `206` whose body length differs from the requested span is a
        // misbehaving origin (multipart/byteranges, off-by-one, truncation).
        let got = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if got != want {
            return Err(wrong_length(got));
        }
        Ok(Some(bytes))
    }

    /// Drain `resp`'s body into `Bytes`, aborting (returning `Ok(None)`) the
    /// moment the cumulative size exceeds `max_bytes`. Per-chunk idle timeout
    /// bounds a slow-trickle origin. Transport errors mid-body surface as
    /// `Transient`. Every read buffers: the outboard is `O(blob/256)`, the
    /// size proof is a few KiB and a data window is at most one
    /// [`crate::RANGE_PULL_WINDOW_BYTES`].
    async fn collect_capped(
        &self,
        mut resp: reqwest::Response,
        max_bytes: u64,
        url_log: String,
    ) -> Result<Option<Bytes>, OriginPullError> {
        let cap = usize::try_from(max_bytes).unwrap_or(usize::MAX);
        let mut buf = BytesMut::new();
        loop {
            match tokio::time::timeout(self.chunk_idle_timeout, resp.chunk()).await {
                Err(_elapsed) => {
                    return Err(OriginPullError::Transient(anyhow::anyhow!(
                        "origin GET {url_log} body read stalled"
                    )));
                }
                Ok(Err(reqwest_err)) => {
                    return Err(classify_reqwest_error(reqwest_err)
                        .map_inner(|e| e.context(format!("origin GET {url_log} body failed"))));
                }
                Ok(Ok(None)) => break,
                Ok(Ok(Some(chunk))) => {
                    if buf.len().saturating_add(chunk.len()) > cap {
                        return Ok(None);
                    }
                    buf.extend_from_slice(&chunk);
                }
            }
        }
        Ok(Some(buf.freeze()))
    }
}

#[cfg(test)]
mod tests;
