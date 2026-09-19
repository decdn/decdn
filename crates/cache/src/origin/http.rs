//! HTTP origin backend.
//!
//! Fetches blobs from `{base_url}/{blake3_hex}`. The base URL is opaque
//! per-node config; it is never put on the wire (see ADR 012 — "no external
//! origin URLs are ever exposed").

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::Context;
use bytes::{Bytes, BytesMut};
use futures_util::Stream;
use iroh_blobs::Hash;
use reqwest::StatusCode;
use reqwest::header::{CONTENT_ENCODING, CONTENT_LENGTH, RANGE};

use super::fs::OBAO4_SUFFIX;
use super::{
    DEFAULT_USER_AGENT, DecompressMode, Origin, OriginFetch, OriginKind, OriginRangeFetch,
    OriginRangeRequest, OriginUrl, OutboardFetch, decompress, parse_origin_url, redact_for_log,
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
        // `pool_max_idle_per_host(0)` disables reqwest's idle keep-alive pool: every
        // request opens a fresh connection and closes it when done, so no connection is
        // ever reused across two callers. This is REQUIRED for correctness, not tuning
        // (#1673). The own-origin serve-miss pull leg runs on an EPHEMERAL per-serve
        // current-thread runtime that the orchestration drops the instant that serve
        // finishes (`serve_via_backend_origin` in the node crate). Because this
        // `HttpOrigin`'s `Client` is shared (one per engine, cloned across serves), a
        // pooled keep-alive connection first driven on serve A's runtime could be
        // reused by a concurrent serve B — and when serve A finishes and its runtime is
        // dropped, that connection's hyper dispatch task dies under B, failing B's
        // in-flight GET with "dispatch task is gone: runtime dropped the dispatch task"
        // (a mid-stream close the paying client sees as `early eof`). Under CI
        // coverage-starvation the drop-while-in-flight window is wide, so two disjoint
        // concurrent own-origin pulls flake; on fast cores it almost never lands. With
        // no idle pool each serve opens its own connection on its own runtime, so a
        // finishing serve's runtime teardown can never strand another's request. The
        // own-origin path fetches few, large ranges, so the per-request handshake cost
        // amortizes over big transfers. The stable-pull-runtime fix that would let us
        // restore origin keep-alive is tracked in #1675.
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .user_agent(user_agent)
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(0)
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
            //   3. for compressed responses, layer the decoder via
            //      `decompress::decode_stream` — see that module for the
            //      decoder sandwich and how the decompressed-side cap is
            //      enforced by the engine's `count_and_cap_stream` (a 1 KB
            //      compressed payload that decompresses to 100 GB fails
            //      fast at the engine seam without ever pinning that
            //      memory).
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

            // For a decoded (compressed) response the advertised
            // `Content-Length` is the *encoded* length, which understates
            // the canonical length. Reporting it as `size_hint` would let
            // the engine route a body whose encoded length fits under
            // `buffered_max_bytes` into the buffer/drain path, where the
            // drain cap (`buffered_max_bytes`) is applied to the *decoded*
            // stream and falsely rejects an in-bounds blob as
            // `BlobTooLarge` (#804). Hand `None` for compressed bodies so
            // they always take the streaming path, where
            // `count_and_cap_stream` checks the running decoded total
            // against the correct `max_blob_bytes`. The pre-stream
            // `advertised_len > max_bytes` short-circuit above still runs
            // first (compressed-len > max_bytes already implies
            // decoded-len > max_bytes under sane ratios). For identity
            // responses the hint is the canonical length.
            let size_hint = if supported_encoding.is_some() {
                None
            } else {
                advertised_len
            };
            Ok(OriginFetch::Found {
                stream: decoded_stream,
                size_hint,
            })
        })
    }

    fn fetch_range_data(
        &self,
        hash: Hash,
        req: OriginRangeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            // Ranged data read. `Range: bytes=a-(b-1)` is inclusive-end. A
            // compliant origin answers `206 Partial Content` with exactly the
            // requested span. A `200` means the origin ignored `Range` and
            // would stream the whole blob — refuse and let the engine
            // whole-blob pull instead of buffering the entire object here.
            let data_url = self
                .base_url
                .as_url()
                .join(&hash.to_hex())
                .with_context(|| format!("failed to build URL for {hash}"))
                .map_err(OriginPullError::Permanent)?;
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
                return Ok(OriginRangeFetch::Unsupported);
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
            let hex = hash.to_hex();
            let obao4_url = self
                .base_url
                .as_url()
                .join(&format!("{hex}{OBAO4_SUFFIX}"))
                .with_context(|| format!("failed to build outboard URL for {hash}"))
                .map_err(OriginPullError::Permanent)?;
            self.get_outboard_bounded(&obao4_url, outboard_max_bytes)
                .await
        })
    }

    fn size(
        &self,
        hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, OriginPullError>> + Send + '_>> {
        Box::pin(async move {
            let hex = hash.to_hex();
            let url = self
                .base_url
                .as_url()
                .join(&hex)
                .with_context(|| format!("failed to build URL for {hash}"))
                .map_err(OriginPullError::Permanent)?;
            let url_log = redact_for_log(&url);
            // A `HEAD` is the cheapest way to learn the canonical length — no
            // body crosses the wire.
            let send_fut = self.client.head(url.clone()).send();
            let resp = match tokio::time::timeout(self.response_headers_timeout, send_fut).await {
                Err(_elapsed) => {
                    return Err(OriginPullError::Transient(anyhow::anyhow!(
                        "origin HEAD {url_log} headers timed out"
                    )));
                }
                Ok(Err(reqwest_err)) => {
                    return Err(classify_reqwest_error(reqwest_err)
                        .map_inner(|e| e.context(format!("origin HEAD {url_log} failed"))));
                }
                Ok(Ok(resp)) => resp,
            };
            let status = resp.status();
            // Status classification mirrors `fetch` and the S3 adapter's
            // `classify_head_object_error`: only a 404 asserts absence and
            // degrades to `Ok(None)`; every other non-success is a fault the
            // caller must not read as "the origin does not hold the object".
            // The distinction is load-bearing well beyond the range-pull
            // degrade: `probe_origin_chain` feeds the origin-only serve gate
            // and the DHT announce set, and an `Ok(None)` there is an
            // authoritative absence — memoised under the negative TTL, signed
            // as `NotFound` to a paying client, and grounds for the
            // republisher to unschedule the hash. A 503 must never do that.
            // Redirects stay disabled (SSRF, #579); a 3xx is a deterministic
            // origin misconfiguration, so it classifies as permanent like the
            // other non-transient statuses. Callers that only wanted the
            // best-effort range scope swallow the error and degrade to a
            // whole-blob `fetch`, which re-surfaces a persistent fault at
            // full severity.
            if status == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if !status.is_success() {
                let err = anyhow::anyhow!("origin HEAD {url_log} returned {status}");
                return Err(if is_transient_status(status) {
                    OriginPullError::Transient(err)
                } else {
                    OriginPullError::Permanent(err)
                });
            }
            // A `Content-Encoding` response advertises the *encoded* length in
            // `Content-Length`, not the canonical blob size the bao tree needs
            // — the same trap `fetch` sidesteps for `size_hint`. Treat as
            // unknown so a compressed origin degrades to a whole-blob pull
            // rather than feeding a wrong size into bao alignment. (A compressed
            // *ranged* fetch is separately caught downstream: the S3 adapter
            // refuses it explicitly, and the HTTP adapter's exact-length gate +
            // bao verification reject it — either way it degrades, never serves.)
            if resp
                .headers()
                .get(CONTENT_ENCODING)
                .is_some_and(|v| !v.is_empty())
            {
                return Ok(None);
            }
            // Read the `Content-Length` header directly rather than
            // `resp.content_length()`: the latter reflects the (empty) HEAD
            // response body, not the header, so it cannot surface the object's
            // true length; the header carries it. A malformed/absent header →
            // unknown size, degrade.
            Ok(resp
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.trim().parse::<u64>().ok()))
        })
    }
}

impl HttpOrigin {
    /// GET `url` and buffer the whole body, capped at `max_bytes`, as an
    /// [`OutboardFetch`]. A genuine 404 is [`OutboardFetch::NotFound`]; every
    /// other non-success status — redirect (disabled per #579), permission
    /// decline, 5xx — is [`OutboardFetch::Unsupported`]. Neither is an error; only a
    /// transport-level fault on the `.send()` surfaces as [`OriginPullError`].
    async fn get_outboard_bounded(
        &self,
        url: &reqwest::Url,
        max_bytes: u64,
    ) -> Result<OutboardFetch, OriginPullError> {
        let url_log = redact_for_log(url);
        let send_fut = self.client.get(url.clone()).send();
        let resp = match tokio::time::timeout(self.response_headers_timeout, send_fut).await {
            Err(_elapsed) => {
                return Err(OriginPullError::Transient(anyhow::anyhow!(
                    "origin GET {url_log} headers timed out"
                )));
            }
            Ok(Err(reqwest_err)) => {
                return Err(classify_reqwest_error(reqwest_err)
                    .map_inner(|e| e.context(format!("origin GET {url_log} failed"))));
            }
            Ok(Ok(resp)) => resp,
        };
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(OutboardFetch::NotFound);
        }
        // Any other non-success (3xx — redirects are disabled, #579 — 401/403,
        // 5xx) degrades rather than errors: the optimization is best-effort,
        // and a persistent fault re-surfaces at full severity on whatever
        // fallback path the caller takes next.
        if !resp.status().is_success() {
            return Ok(OutboardFetch::Unsupported);
        }
        if let Some(len) = resp.content_length()
            && len > max_bytes
        {
            return Ok(OutboardFetch::Unsupported);
        }
        match self.collect_capped(resp, max_bytes, url_log).await? {
            Some(bytes) => Ok(OutboardFetch::Found(bytes)),
            None => Ok(OutboardFetch::Unsupported),
        }
    }

    /// GET `url` with a `Range` header and buffer the partial body. Returns
    /// `Ok(None)` when the origin does not honor the range (any status other
    /// than `206`, e.g. a `200` whole-blob response) or the returned span is
    /// not exactly `want` bytes — both degrade the engine to a whole-blob
    /// pull. `want` is capped both as the buffer ceiling and as an exact-length
    /// check, so an origin that streams the whole object on a `200` is rejected
    /// at the header (status) before any large buffer is committed.
    async fn get_range_bytes(
        &self,
        url: &reqwest::Url,
        range_val: &str,
        want: u64,
    ) -> Result<Option<Bytes>, OriginPullError> {
        let url_log = redact_for_log(url);
        let send_fut = self.client.get(url.clone()).header(RANGE, range_val).send();
        let resp = match tokio::time::timeout(self.response_headers_timeout, send_fut).await {
            Err(_elapsed) => {
                return Err(OriginPullError::Transient(anyhow::anyhow!(
                    "origin GET {url_log} (range) headers timed out"
                )));
            }
            Ok(Err(reqwest_err)) => {
                return Err(classify_reqwest_error(reqwest_err)
                    .map_inner(|e| e.context(format!("origin GET {url_log} (range) failed"))));
            }
            Ok(Ok(resp)) => resp,
        };
        // Only `206 Partial Content` is an honored range. A `200` means the
        // origin ignored `Range` and is sending the whole blob — degrade
        // before reading the (potentially huge) body.
        if resp.status() != StatusCode::PARTIAL_CONTENT {
            return Ok(None);
        }
        // A `206` whose advertised `Content-Length` already differs from the
        // requested span is a misbehaving origin — degrade BEFORE collecting
        // the body rather than buffering up to `want` bytes only to reject
        // them. `collect_capped` caps at `want`, and the exact-length gate
        // below is the load-bearing check, but a wrong `Content-Length` lets
        // us skip the read entirely.
        if let Some(len) = resp.content_length()
            && len != want
        {
            return Ok(None);
        }
        let Some(bytes) = self.collect_capped(resp, want, url_log).await? else {
            return Ok(None);
        };
        // A `206` whose body length differs from the requested span is a
        // misbehaving origin (multipart/byteranges, off-by-one, truncation).
        // Degrade rather than feed a wrong-length span to verification.
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != want {
            return Ok(None);
        }
        Ok(Some(bytes))
    }

    /// Drain `resp`'s body into `Bytes`, aborting (returning `Ok(None)`) the
    /// moment the cumulative size exceeds `max_bytes`. Per-chunk idle timeout
    /// bounds a slow-trickle origin. Transport errors mid-body surface as
    /// `Transient`. The range path buffers (rather than streams) because the
    /// payloads are bounded: the outboard is `O(blob/256)` and the data span
    /// is at most one [`crate::RANGE_PULL_WINDOW_BYTES`] window, both far
    /// below the whole-blob streaming threshold that motivated #271.
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
                        // Over the bound → degrade (the optimization is
                        // best-effort; a too-big sibling/span is treated as
                        // "not range-pullable", never a hard error).
                        return Ok(None);
                    }
                    buf.extend_from_slice(&chunk);
                }
            }
        }
        Ok(Some(buf.freeze()))
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
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    /// A published sibling `{hex}.obao4` is fetched in full via
    /// `fetch_outboard` alone (no data GET at all, #1130 stream-while-store
    /// seam).
    #[tokio::test]
    async fn fetch_outboard_returns_sibling_obao4() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        let hash = Hash::new(b"http-outboard-marker");
        let hex = hash.to_hex();
        let outboard_bytes = vec![0xABu8; 4096];
        Mock::given(method("GET"))
            .and(path(format!("/{hex}.obao4")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard_bytes.clone()))
            .mount(&server)
            .await;
        let origin = HttpOrigin::parse(&server.uri())?;

        match origin.fetch_outboard(hash, 1 << 20).await? {
            OutboardFetch::Found(bytes) => {
                anyhow::ensure!(
                    bytes.as_ref() == outboard_bytes.as_slice(),
                    "outboard bytes mismatch"
                );
            }
            other => anyhow::bail!("expected Found, got {other:?}"),
        }
        Ok(())
    }

    /// A `404` on the sibling outboard is `NotFound`, not an error.
    #[tokio::test]
    async fn fetch_outboard_404_is_not_found() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        let hash = Hash::new(b"http-outboard-missing-marker");
        let hex = hash.to_hex();
        Mock::given(method("GET"))
            .and(path(format!("/{hex}.obao4")))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let origin = HttpOrigin::parse(&server.uri())?;

        anyhow::ensure!(
            matches!(
                origin.fetch_outboard(hash, 1 << 20).await?,
                OutboardFetch::NotFound
            ),
            "404 must be NotFound",
        );
        Ok(())
    }

    /// A non-404 decline (403 here) degrades to `Unsupported` rather than
    /// erroring — mirrors `fetch_range_data`'s "best-effort, never an error for a
    /// status-level decline" contract.
    #[tokio::test]
    async fn fetch_outboard_non_404_decline_is_unsupported() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        let hash = Hash::new(b"http-outboard-forbidden-marker");
        let hex = hash.to_hex();
        Mock::given(method("GET"))
            .and(path(format!("/{hex}.obao4")))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let origin = HttpOrigin::parse(&server.uri())?;

        anyhow::ensure!(
            matches!(
                origin.fetch_outboard(hash, 1 << 20).await?,
                OutboardFetch::Unsupported
            ),
            "non-404 decline must degrade to Unsupported",
        );
        Ok(())
    }

    /// An outboard whose advertised `Content-Length` exceeds
    /// `outboard_max_bytes` degrades to `Unsupported` rather than buffering —
    /// the OOM guard on the one outboard read a range pull makes.
    #[tokio::test]
    async fn fetch_outboard_oversize_is_unsupported() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        let hash = Hash::new(b"http-outboard-oversize-marker");
        let hex = hash.to_hex();
        let huge = vec![0x5Au8; 8192];
        Mock::given(method("GET"))
            .and(path(format!("/{hex}.obao4")))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(huge))
            .mount(&server)
            .await;
        let origin = HttpOrigin::parse(&server.uri())?;

        anyhow::ensure!(
            matches!(
                origin.fetch_outboard(hash, 1024).await?,
                OutboardFetch::Unsupported
            ),
            "oversize outboard must degrade to Unsupported",
        );
        Ok(())
    }

    /// A `HEAD` 404 is an authoritative absence: `size` answers `Ok(None)`
    /// so the probe chain reads it as `Absent`.
    #[tokio::test]
    async fn size_404_is_none() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        let hash = Hash::new(b"http-size-missing-marker");
        Mock::given(method("HEAD"))
            .and(path(format!("/{}", hash.to_hex())))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let origin = HttpOrigin::parse(&server.uri())?;

        anyhow::ensure!(
            origin.size(hash).await?.is_none(),
            "a 404 must degrade to an unknown size, not error",
        );
        Ok(())
    }

    /// A `HEAD` 503 is a transient fault, never an absence (#1815): folding it
    /// into `Ok(None)` made `probe_origin_chain` answer `Absent` for an origin
    /// mid-outage — the serve gate then signed an authoritative `NotFound`,
    /// memoised under the negative TTL, and the DHT republisher unscheduled
    /// the hash entirely.
    #[tokio::test]
    async fn size_5xx_is_a_transient_fault_not_an_absence() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        let hash = Hash::new(b"http-size-outage-marker");
        Mock::given(method("HEAD"))
            .and(path(format!("/{}", hash.to_hex())))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let origin = HttpOrigin::parse(&server.uri())?;

        match origin.size(hash).await {
            Err(err) => anyhow::ensure!(
                err.is_transient(),
                "a 503 is worth waiting out; got a permanent fault: {err}"
            ),
            Ok(size) => anyhow::bail!("a 503 must fault, got Ok({size:?})"),
        }
        Ok(())
    }

    /// A `HEAD` 403 is a permanent fault: it will read the same on every
    /// probe, so fault-aware callers must not hold state open for it — but it
    /// is still not an absence the serve gate may sign. Mirrors the S3
    /// adapter's `classify_head_object_error`.
    #[tokio::test]
    async fn size_non_404_decline_is_a_permanent_fault() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        let hash = Hash::new(b"http-size-forbidden-marker");
        Mock::given(method("HEAD"))
            .and(path(format!("/{}", hash.to_hex())))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let origin = HttpOrigin::parse(&server.uri())?;

        match origin.size(hash).await {
            Err(err) => anyhow::ensure!(
                !err.is_transient(),
                "a 403 reads the same on every probe; got a transient fault: {err}"
            ),
            Ok(size) => anyhow::bail!("a 403 must fault, got Ok({size:?})"),
        }
        Ok(())
    }

    /// A successful `HEAD` reports the object's `Content-Length`.
    #[tokio::test]
    async fn size_reads_content_length_on_success() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        let hash = Hash::new(b"http-size-present-marker");
        Mock::given(method("HEAD"))
            .and(path(format!("/{}", hash.to_hex())))
            .respond_with(ResponseTemplate::new(200).insert_header("content-length", "12345"))
            .mount(&server)
            .await;
        let origin = HttpOrigin::parse(&server.uri())?;

        anyhow::ensure!(
            origin.size(hash).await? == Some(12345),
            "a 200 must surface the Content-Length",
        );
        Ok(())
    }

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
