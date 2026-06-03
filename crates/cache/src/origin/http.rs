//! HTTP origin backend.
//!
//! Fetches blobs from `{base_url}/{blake3_hex}`. The base URL is opaque
//! per-node config; it is never put on the wire (see ADR 012 — "no external
//! origin URLs are ever exposed").

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use futures_util::Stream;
use iroh_blobs::Hash;
use reqwest::StatusCode;
use reqwest::header::CONTENT_ENCODING;

use super::{
    DEFAULT_USER_AGENT, DecompressMode, Origin, OriginFetch, OriginKind, OriginUrl, decompress,
    parse_origin_url, redact_for_log,
};
use crate::error::{OriginError, OriginPullError};

/// How long to wait for the TCP/TLS handshake to complete. Per-request total
/// duration is intentionally *not* bounded because `max_blob_size_mb` can be
/// as large as 10 GB and no single total-request timeout fits both small and
/// large blobs. Instead, we pair this with phase-level timeouts below.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Ceiling on how long the origin may take to respond with headers after
/// the connection is established. Without this, a compromised or misbehaving
/// origin could accept the connection and then stall indefinitely, tying up
/// a fetch task with no bytes in flight to catch it any other way.
const RESPONSE_HEADERS_TIMEOUT: Duration = Duration::from_secs(30);

/// Ceiling on the idle gap between body chunks. Bounds the time a
/// slow-trickle origin can tie up a task — total bytes are capped by
/// `max_bytes`, but without this check a pathological origin could send
/// a single byte per second within the total cap and keep the task alive
/// for days.
const CHUNK_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Origin backed by a plain HTTP(S) endpoint serving content-addressed blobs
/// at `{base_url}/{blake3_hex}`.
#[derive(Debug, Clone)]
pub struct HttpOrigin {
    client: reqwest::Client,
    base_url: OriginUrl,
    response_headers_timeout: Duration,
    chunk_idle_timeout: Duration,
    /// How to handle `Content-Encoding` on the response. Defaults to
    /// [`DecompressMode::Auto`] — see that type for the full semantics.
    /// Override with [`Self::with_decompress_mode`].
    decompress: DecompressMode,
}

impl HttpOrigin {
    /// Build an [`HttpOrigin`] around a validated [`OriginUrl`]. The only way
    /// to obtain one is [`parse_origin_url`], so invariants are enforced at
    /// the type boundary rather than documented in prose. Timeouts default
    /// to the `RESPONSE_HEADERS_TIMEOUT` and `CHUNK_IDLE_TIMEOUT` constants; override
    /// with [`Self::with_timeouts`] if operator policy or tests require
    /// different values.
    ///
    /// Sets [`DEFAULT_USER_AGENT`] on the inner reqwest client so origin
    /// access logs can attribute CDN pull-through traffic (#435). Override
    /// via [`Self::new_with_user_agent`].
    pub fn new(base_url: OriginUrl) -> anyhow::Result<Self> {
        Self::new_with_user_agent(base_url, DEFAULT_USER_AGENT)
    }

    /// Like [`Self::new`] but with a caller-supplied `User-Agent`. Used by
    /// the runtime to honour an operator-configured `cache.user_agent`
    /// without rebuilding the client through a separate code path.
    /// `user_agent` must be a valid header value (visible-ASCII; `reqwest`
    /// rejects others at build time and the error is surfaced as a
    /// startup failure).
    pub fn new_with_user_agent(base_url: OriginUrl, user_agent: &str) -> anyhow::Result<Self> {
        // We want to inspect `Content-Encoding` and run the body through
        // our own decoders, so disable reqwest's built-in transparent
        // decompression — otherwise reqwest would strip the header and
        // hand back already-decompressed bytes, masking what the origin
        // actually sent and bypassing our `UnsupportedEncoding` error path.
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
            base_url,
            response_headers_timeout: RESPONSE_HEADERS_TIMEOUT,
            chunk_idle_timeout: CHUNK_IDLE_TIMEOUT,
            decompress: DecompressMode::Auto,
        })
    }

    /// Convenience constructor that parses `raw` via [`parse_origin_url`]
    /// before delegating to [`Self::new`]. Prefer [`Self::new`] in
    /// runtime paths so URL validation happens at config-load time.
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        Self::new(parse_origin_url(raw)?)
    }

    /// Override the phase timeouts. Primarily exists for tests — runtime
    /// callers should use the defaults unless operator policy dictates
    /// otherwise.
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

    /// Set the [`DecompressMode`]. See that type for the semantics of
    /// `Auto` vs `Strict`. Defaults to `Auto`.
    #[must_use]
    pub const fn with_decompress_mode(mut self, mode: DecompressMode) -> Self {
        self.decompress = mode;
        self
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
///   this no longer fires for gzip/zstd; the remaining triggers are
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
/// every other 4xx is permanent. `404` is intercepted earlier as
/// `OriginFetch::NotFound`; this helper assumes the caller already
/// excluded it.
fn is_transient_status(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
}

impl Origin for HttpOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch(
        &self,
        hash: Hash,
        max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            // URL build can only fail on a malformed hash or a
            // pathological base URL — both are caller-side bugs, never
            // transient.
            let url = self
                .base_url
                .as_url()
                .join(&hash.to_hex())
                .with_context(|| format!("failed to build URL for {hash}"))
                .map_err(OriginPullError::Permanent)?;
            // Always log the redacted form — `url` may inherit userinfo
            // from `base_url`, and errors go to logs.
            let url_log = redact_for_log(&url);

            // `connect_timeout` on the client covers the TCP/TLS handshake.
            // This wrapper covers `.send()` — which is request write through
            // response-header receipt (and a pool-miss connect if no idle
            // connection is available), bounding a server that accepts the
            // TCP/TLS but never writes back.
            let headers_timeout = self.response_headers_timeout;
            let send_fut = self.client.get(url.clone()).send();
            let resp = match tokio::time::timeout(headers_timeout, send_fut).await {
                Err(_elapsed) => {
                    // Headers-phase timeout: transient.
                    return Err(OriginPullError::Transient(anyhow::anyhow!(
                        "origin GET {url_log} headers timed out after {headers_timeout:?}"
                    )));
                }
                Ok(Err(reqwest_err)) => {
                    return Err(classify_reqwest_error(reqwest_err)
                        .map_inner(|e| e.context(format!("origin GET {url_log} failed"))));
                }
                Ok(Ok(resp)) => resp,
            };

            let status = resp.status();
            // SSRF defence (#579): with `redirect::Policy::none()` (set
            // in the client builder), reqwest returns 3xx responses
            // straight to us instead of following. Refuse them with a
            // clear operator-visible message rather than letting the
            // generic "returned 302" path fire — origins must serve
            // `{base}/{hex}` directly. Permanent because a compliant
            // origin will not redirect; retrying won't change that.
            if status.is_redirection() {
                return Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "origin GET {url_log} returned {status}; redirects are disabled \
                     to prevent SSRF — origin must serve {{base}}/{{hex}} directly"
                )));
            }
            if status == StatusCode::NOT_FOUND {
                return Ok(OriginFetch::NotFound);
            }
            if !status.is_success() {
                let err = anyhow::anyhow!("origin GET {url_log} returned {status}");
                return Err(if is_transient_status(status) {
                    OriginPullError::Transient(err)
                } else {
                    OriginPullError::Permanent(err)
                });
            }

            // Capture the encoding before consuming the body. RFC 9110
            // § 5.6.7 restricts the header to ASCII tokens; a value
            // that fails `to_str()` is a malformed origin response,
            // not "no encoding". Surfacing `MalformedEncoding` keeps
            // a buggy or hostile origin from smuggling raw compressed
            // bytes past the decompression layer.
            let encoding = match resp.headers().get(CONTENT_ENCODING) {
                None => String::new(),
                Some(v) => match v.to_str() {
                    Ok(s) => s.to_string(),
                    Err(_) => {
                        // Malformed header is a deterministic origin
                        // protocol violation — won't be cured by retry.
                        return Err(OriginPullError::Permanent(
                            OriginError::MalformedEncoding.into(),
                        ));
                    }
                },
            };
            let trimmed = encoding.trim();
            // Classify the encoding and apply the strict/auto policy in one
            // place shared with the S3 backend. `Err` covers both unknown
            // encodings (e.g. `br`) and known encodings rejected under
            // `Strict` — both are permanent and fire before any body bytes
            // are read.
            let supported_encoding = match decompress::resolve_encoding(trimmed, self.decompress) {
                Ok(encoding) => encoding,
                Err(err) => return Err(OriginPullError::Permanent(err.into())),
            };

            // Fast-path rejection using the advertised length before
            // we read anything. For identity bodies, encoded length is
            // payload length and must not exceed `max_bytes`. For
            // compressed bodies, encoded length is decoupled from
            // decompressed length, but a compressed body advertising
            // > max_bytes is malicious or misconfigured (compression
            // ratios < 1 are universal in practice).
            let advertised_len = resp.content_length();
            if let Some(len) = advertised_len
                && len > max_bytes
            {
                return Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "origin GET {url_log} advertises {len} bytes, exceeds max {max_bytes}"
                )));
            }

            // Build the body stream:
            //   1. response_chunk_stream pulls encoded chunks via
            //      `resp.chunk()` (a `stream::unfold` so we own the
            //      poll loop and the per-chunk idle timer)
            //   2. wrap with per-chunk idle timeout + cap on the encoded
            //      size (defense against unbounded slow-trickle / lying
            //      origins; same `max_bytes` cap as before, applied to
            //      encoded bytes for compressed responses — compression
            //      ratios < 1 mean this is also a sound bound on the
            //      decoded size).
            //   3. for compressed responses, layer
            //      `async_compression::tokio::bufread` decoder via a
            //      `StreamReader` -> decoder -> `ReaderStream`
            //      sandwich. The decompressed-side cap is enforced
            //      again by the engine's `count_and_cap_stream`
            //      (the running total there ensures we abort the
            //      import the moment decompressed bytes exceed
            //      `max_bytes`, so a 1 KB compressed payload that
            //      decompresses to 100 GB fails fast at the engine
            //      seam without ever pinning that memory).
            let idle_timeout = self.chunk_idle_timeout;
            let raw_stream = response_chunk_stream(resp, idle_timeout, max_bytes, url_log);

            // Layer the decoder (if any) onto the encoded chunk stream.
            // Decoder errors (truncated body, bad magic, mid-stream
            // checksum mismatch) are wrapped as a typed
            // `OriginError::DecompressionFailed` inside the shared helper so
            // `crate::retry::classify_io_error` routes them into
            // `OriginPullError::Permanent` (decoder failures aren't
            // retryable; the body is corrupt).
            let decoded_stream = decompress::decode_stream(raw_stream, supported_encoding);

            Ok(OriginFetch::Found {
                stream: decoded_stream,
                // For compressed responses the size_hint is the
                // *encoded* length (Content-Length over the wire) —
                // useful for the engine's pre-stream short-circuit
                // because compressed-len > max_bytes already implies
                // decoded-len > max_bytes (assuming sane compression
                // ratios). For identity responses it's the canonical
                // length.
                size_hint: advertised_len,
            })
        })
    }
}

/// Build the per-chunk stream over `resp.chunk()`. Wraps each chunk
/// fetch in a `tokio::time::timeout` to bound slow-trickle origins
/// (idle gap), and runs a per-chunk cap that aborts the moment
/// cumulative encoded bytes exceed `max_bytes`. reqwest transport
/// errors are converted to `io::Error::other(...)` since
/// `iroh_blobs::Blobs::add_stream` expects `io::Result<Bytes>`; the
/// transient/permanent classification we'd normally apply is not
/// recoverable mid-stream anyway (the engine surfaces stream errors
/// as a generic origin failure).
fn response_chunk_stream(
    resp: reqwest::Response,
    idle_timeout: Duration,
    max_bytes: u64,
    url_log: String,
) -> impl Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static {
    // **Termination after error:** wrap `resp` in `Option` so polling
    // stops once we yield `Err`. After an error or cap breach,
    // calling `resp.chunk()` again is undefined behaviour for reqwest
    // (the body stream may be in a torn-down state); the sentinel
    // makes the wrapper deterministic and prevents iroh-blobs'
    // `add_stream` from hanging on a re-poll.
    futures_util::stream::unfold(
        (Some(resp), 0u64, idle_timeout, url_log),
        move |(maybe_resp, total, idle, url_log)| async move {
            let mut resp = maybe_resp?;
            let chunk = match tokio::time::timeout(idle, resp.chunk()).await {
                Err(_elapsed) => {
                    let err = std::io::Error::other(format!(
                        "origin GET {url_log} body read stalled for {idle:?} after {total} bytes buffered"
                    ));
                    return Some((Err(err), (None, total, idle, url_log)));
                }
                Ok(Err(reqwest_err)) => {
                    let err = std::io::Error::other(format!(
                        "origin GET {url_log} body read failed: {reqwest_err}"
                    ));
                    return Some((Err(err), (None, total, idle, url_log)));
                }
                Ok(Ok(None)) => return None,
                Ok(Ok(Some(chunk))) => chunk,
            };
            let next_total = total.saturating_add(chunk.len() as u64);
            if next_total > max_bytes {
                // Pack a typed `BlobTooLargeMarker` so the engine
                // surfaces `CacheError::BlobTooLarge` rather than
                // generic `OriginError`. This is the *encoded*-side
                // cap; for compressed bodies an encoded overrun
                // implies a decoded overrun (compression ratios <1
                // in practice), so the same typed shape is the right
                // operator-visible error.
                let err = std::io::Error::other(super::BlobTooLargeMarker { max_bytes });
                return Some((Err(err), (None, total, idle, url_log)));
            }
            Some((Ok(chunk), (Some(resp), next_total, idle, url_log)))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `DEFAULT_USER_AGENT` (#435, now in the `decdn-config-types` leaf
    /// crate per #578) embeds that crate's `CARGO_PKG_VERSION` so origin
    /// operators can attribute pull-through traffic. The `decdn-node/`
    /// prefix is the stable contract — operators grep it in access logs.
    #[test]
    fn default_user_agent_has_expected_prefix_and_version() -> anyhow::Result<()> {
        anyhow::ensure!(
            DEFAULT_USER_AGENT.starts_with("decdn-node/"),
            "got: {DEFAULT_USER_AGENT}"
        );
        anyhow::ensure!(
            DEFAULT_USER_AGENT.len() > "decdn-node/".len(),
            "version segment missing: {DEFAULT_USER_AGENT}"
        );
        Ok(())
    }

    /// `HttpOrigin::new_with_user_agent` accepts a non-default UA without
    /// erroring on a typical operator-supplied value.
    #[test]
    fn new_with_user_agent_accepts_custom_value() -> anyhow::Result<()> {
        let url = parse_origin_url("https://origin.example/")?;
        let _origin = HttpOrigin::new_with_user_agent(url, "MyCdn/1.0 (+ops@example.com)")?;
        Ok(())
    }
}
