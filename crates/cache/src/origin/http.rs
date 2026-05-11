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
use futures_util::{Stream, StreamExt};
use iroh_blobs::Hash;
use reqwest::StatusCode;
use reqwest::header::CONTENT_ENCODING;
use serde::{Deserialize, Serialize};
use tokio_util::io::{ReaderStream, StreamReader};

use super::{Origin, OriginByteStream, OriginFetch, OriginKind};
use crate::error::{OriginError, OriginPullError, SupportedEncoding};

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

/// Return a loggable form of an origin URL with userinfo (`user:pass@`)
/// stripped. Operators may legitimately use basic-auth-in-URL for
/// internal origins, but those credentials must not reach logs or
/// error messages.
///
/// Fails closed: if the setters can't strip userinfo (cannot-be-a-base
/// URLs, or URL-parsing ambiguity that misattributes credentials into
/// host/path), we emit a fixed placeholder rather than echoing anything
/// that might contain secrets. Callers from `parse_origin_url` and
/// `HttpOrigin::fetch` always pass http/https URLs (parse-and-scheme
/// validated, with `.join()` preserving scheme), so the happy path is
/// expected here.
fn redact_for_log(url: &reqwest::Url) -> String {
    let mut u = url.clone();
    let _ = u.set_username("");
    let _ = u.set_password(None);
    if !u.username().is_empty() || u.password().is_some() {
        return "<redacted URL>".to_string();
    }
    u.to_string()
}

/// Loggable form of a raw (not yet parsed) origin URL string. Used by
/// `parse_origin_url` when the input might contain credentials. For
/// unparseable input we don't echo it at all — no way to safely split
/// userinfo from the rest.
fn redact_raw_for_log(raw: &str) -> String {
    match reqwest::Url::parse(raw) {
        Ok(url) => redact_for_log(&url),
        Err(_) => "<unparseable URL>".to_string(),
    }
}

/// Parsed, scheme-validated, and path-normalized origin base URL. The only
/// way to construct one is [`parse_origin_url`] — once you have an
/// [`OriginUrl`], the following invariants are guaranteed by the type:
///
/// 1. Scheme is `http` or `https`.
/// 2. Path ends with a trailing `/`, so `{base}.join(&hex)` produces
///    `{base}/{hex}` rather than overwriting the final path component.
/// 3. No query string or fragment — both silently get dropped by
///    [`reqwest::Url::join`] and would turn into invisible footguns.
#[derive(Debug, Clone)]
pub struct OriginUrl(reqwest::Url);

impl OriginUrl {
    /// Borrow the underlying [`reqwest::Url`]. Needed when calling
    /// `.join(hash_hex)` or handing the URL to `reqwest::Client::get`.
    pub const fn as_url(&self) -> &reqwest::Url {
        &self.0
    }

    /// Consume into the underlying [`reqwest::Url`].
    #[allow(clippy::missing_const_for_fn)] // Destructuring a non-Copy newtype is not const.
    pub fn into_url(self) -> reqwest::Url {
        self.0
    }
}

impl std::fmt::Display for OriginUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Parse and normalize an origin base URL. This is the single source of
/// truth for origin-URL validation — config resolution and test helpers
/// both go through it, and the returned [`OriginUrl`] carries the
/// scheme/path/query invariants in the type.
pub fn parse_origin_url(raw: &str) -> anyhow::Result<OriginUrl> {
    // Error messages use `redact_raw_for_log` / `redact_for_log` so
    // userinfo (user:pass@) never reaches error strings or logs.
    let mut url = reqwest::Url::parse(raw)
        .with_context(|| format!("invalid origin base URL: {}", redact_raw_for_log(raw)))?;
    match url.scheme() {
        "http" | "https" => {}
        other => anyhow::bail!(
            "unsupported origin URL scheme {other:?} (expected http or https): {}",
            redact_for_log(&url)
        ),
    }
    // A query or fragment on the base would be silently dropped by
    // `Url::join` when we append the blake3 hex, so reject up front rather
    // than accept a URL that can't possibly do what the operator expects.
    if url.query().is_some() {
        anyhow::bail!(
            "origin URL must not contain a query string: {}",
            redact_for_log(&url)
        );
    }
    if url.fragment().is_some() {
        anyhow::bail!(
            "origin URL must not contain a fragment: {}",
            redact_for_log(&url)
        );
    }
    // Normalize the path (not the raw string) so `http://host/v1` becomes
    // `http://host/v1/` without corrupting any other URL component.
    if !url.path().ends_with('/') {
        let normalized = format!("{}/", url.path());
        url.set_path(&normalized);
    }
    Ok(OriginUrl(url))
}

/// How [`HttpOrigin`] handles `Content-Encoding` on the response.
///
/// `bool` was the original config knob, but the two states have richer
/// semantics than "on / off" — `Strict` is not "no decompression", it
/// is "I will refuse anything other than identity". Using a typed enum
/// keeps that distinction visible at every call site (config, runtime,
/// fetch path) and on operator-facing config files.
///
/// The TOML representation uses lowercase tag names: `decompress = "auto"`
/// or `decompress = "strict"`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DecompressMode {
    /// Decode `gzip` / `zstd` bodies transparently; identity passes
    /// through; unknown encodings raise [`OriginError::UnsupportedEncoding`].
    /// This is the default — most object stores serve compressed bodies
    /// and the BLAKE3 verify in [`crate::CacheEngine`] runs over the
    /// canonical (decompressed) form, so pass-through would always fail
    /// verification.
    #[default]
    Auto,
    /// Reject any non-identity `Content-Encoding`. The origin must serve
    /// canonical bytes; gzip / zstd / unknown encodings all return
    /// [`OriginError::UnsupportedEncoding`] before any decode runs.
    /// Useful only for origins that pre-canonicalise (e.g. an internal
    /// pre-warmed mirror).
    Strict,
}

/// Default `User-Agent` header set on every origin request unless overridden
/// via [`HttpOrigin::new_with_user_agent`]. Embeds `CARGO_PKG_VERSION` of
/// the `decdn-cache` crate (the workspace does not version-link its members,
/// so this can drift from the running `decdn-node` binary's version if the
/// two crates are bumped independently). Lets origin operators attribute
/// CDN pull-through traffic in access logs and apply origin-side rate
/// limits or routing rules separately from anonymous client traffic (#435).
pub const DEFAULT_USER_AGENT: &str = concat!("decdn-node/", env!("CARGO_PKG_VERSION"));

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
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .user_agent(user_agent)
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
    /// production paths so URL validation happens at config-load time.
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        Self::new(parse_origin_url(raw)?)
    }

    /// Override the phase timeouts. Primarily exists for tests — prod
    /// paths should use the defaults unless operator policy dictates
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

/// Classify a (trimmed) `Content-Encoding` token: identity / supported /
/// unsupported. Returns `None` for the identity case (empty or
/// `identity`), `Some(Ok(_))` for known decoders, and `Some(Err(_))`
/// for unknown encodings. Centralises the case-folding so callers can't
/// disagree on whether `GZIP` is gzip.
fn classify_encoding(trimmed: &str) -> Option<Result<SupportedEncoding, OriginError>> {
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("identity") {
        return None;
    }
    if trimmed.eq_ignore_ascii_case("gzip") || trimmed.eq_ignore_ascii_case("x-gzip") {
        return Some(Ok(SupportedEncoding::Gzip));
    }
    if trimmed.eq_ignore_ascii_case("zstd") {
        return Some(Ok(SupportedEncoding::Zstd));
    }
    Some(Err(OriginError::UnsupportedEncoding {
        encoding: trimmed.into(),
    }))
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
/// `OriginUrl`), `is_redirect()` (we follow with reqwest's default
/// limit), `is_status()` (we handle status codes ourselves before
/// reaching here), and any future reqwest variant. Permanent because
/// none of those are operationally retriable.
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
            let supported_encoding = match classify_encoding(trimmed) {
                None => None,
                Some(Ok(supported)) => Some(supported),
                Some(Err(unsupported_err)) => {
                    // Unsupported encoding (e.g. `br`): typed permanent
                    // error, before any body bytes are read.
                    return Err(OriginPullError::Permanent(unsupported_err.into()));
                }
            };
            let is_compressed = supported_encoding.is_some();

            // Strict mode rejects any non-identity encoding even when
            // we know the decoder. Equivalent to today's behaviour.
            if matches!(self.decompress, DecompressMode::Strict) && is_compressed {
                return Err(OriginPullError::Permanent(
                    OriginError::UnsupportedEncoding {
                        encoding: trimmed.into(),
                    }
                    .into(),
                ));
            }

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

            // `StreamReader` implements `AsyncBufRead` directly, so no
            // `tokio::io::BufReader` wrapper is needed for the
            // `bufread::*Decoder` family.
            //
            // Decoder errors (truncated body, bad magic, mid-stream
            // checksum mismatch) emerge from `ReaderStream` as
            // `io::Error`. We wrap them with a typed
            // `OriginError::DecompressionFailed` so
            // `CacheError::origin_error_kind` can recover the typed
            // variant later — the engine's
            // `count_and_cap_stream` side-channel preserves the
            // `io::Error`, and the engine's `build_origin_anyhow_from_io`
            // uses `into_inner` + `downcast` to make the typed
            // variant the deepest error in the anyhow chain.
            let decoded_stream: OriginByteStream = match supported_encoding {
                None => Box::pin(raw_stream),
                Some(SupportedEncoding::Gzip) => {
                    let reader = StreamReader::new(raw_stream);
                    let decoder = async_compression::tokio::bufread::GzipDecoder::new(reader);
                    Box::pin(ReaderStream::new(decoder).map(|res| {
                        res.map_err(|e| typed_decoder_error(SupportedEncoding::Gzip, e))
                    }))
                }
                Some(SupportedEncoding::Zstd) => {
                    let reader = StreamReader::new(raw_stream);
                    let decoder = async_compression::tokio::bufread::ZstdDecoder::new(reader);
                    Box::pin(ReaderStream::new(decoder).map(|res| {
                        res.map_err(|e| typed_decoder_error(SupportedEncoding::Zstd, e))
                    }))
                }
            };

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

/// Wrap a decoder `io::Error` into an `io::Error` whose source is a
/// typed [`OriginError::DecompressionFailed`]. The engine's
/// `count_and_cap_stream` side-channel preserves the `io::Error`
/// verbatim; the engine's `build_origin_anyhow_from_io` then peels
/// the typed variant out via `into_inner` + `downcast` so
/// `CacheError::origin_error_kind` finds it on the chain walk.
fn typed_decoder_error(encoding: SupportedEncoding, source: std::io::Error) -> std::io::Error {
    let kind = source.kind();
    let typed = OriginError::DecompressionFailed { encoding, source };
    std::io::Error::new(kind, typed)
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

    #[test]
    fn parse_origin_url_appends_trailing_slash() -> anyhow::Result<()> {
        let url = parse_origin_url("https://origin.example/v1")?;
        anyhow::ensure!(
            url.as_url().as_str() == "https://origin.example/v1/",
            "got: {url}"
        );
        // The full path segment must survive `join` — not collapse to `/`.
        let joined = url.as_url().join("abcdef")?;
        anyhow::ensure!(
            joined.as_str() == "https://origin.example/v1/abcdef",
            "join produced: {joined}"
        );
        Ok(())
    }

    #[test]
    fn parse_origin_url_preserves_existing_trailing_slash() -> anyhow::Result<()> {
        let url = parse_origin_url("https://origin.example/")?;
        anyhow::ensure!(
            url.as_url().as_str() == "https://origin.example/",
            "got: {url}"
        );
        Ok(())
    }

    #[test]
    fn parse_origin_url_rejects_query_string() -> anyhow::Result<()> {
        // `.join(hex)` on a query-bearing base drops the query silently —
        // operators would never notice. Reject at parse time instead.
        let err = parse_origin_url("https://origin.example/v1?token=abc")
            .err()
            .ok_or_else(|| anyhow::anyhow!("query-bearing URL should have been rejected"))?
            .to_string();
        anyhow::ensure!(
            err.contains("must not contain a query string"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    #[test]
    fn parse_origin_url_rejects_fragment() -> anyhow::Result<()> {
        let err = parse_origin_url("https://origin.example/v1#frag")
            .err()
            .ok_or_else(|| anyhow::anyhow!("fragment URL should have been rejected"))?
            .to_string();
        anyhow::ensure!(
            err.contains("must not contain a fragment"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    #[test]
    fn parse_origin_url_accepts_http_and_https() -> anyhow::Result<()> {
        parse_origin_url("http://origin.example")?;
        parse_origin_url("https://origin.example")?;
        Ok(())
    }

    #[test]
    fn parse_origin_url_rejects_non_http_schemes() -> anyhow::Result<()> {
        for raw in [
            "file:///etc/passwd",
            "ftp://origin.example",
            "ws://origin.example",
        ] {
            let err = parse_origin_url(raw)
                .err()
                .ok_or_else(|| anyhow::anyhow!("{raw} should have been rejected"))?
                .to_string();
            anyhow::ensure!(
                err.contains("unsupported origin URL scheme"),
                "error for {raw} lacked scheme context: {err}"
            );
        }
        Ok(())
    }

    #[test]
    fn redact_for_log_strips_user_and_password() -> anyhow::Result<()> {
        let url = reqwest::Url::parse("https://user:secret@host.example/path")?;
        let redacted = redact_for_log(&url);
        anyhow::ensure!(
            !redacted.contains("secret") && !redacted.contains("user"),
            "redaction leaked credentials: {redacted}"
        );
        anyhow::ensure!(
            redacted.contains("host.example"),
            "redaction dropped host: {redacted}"
        );
        Ok(())
    }

    #[test]
    fn redact_for_log_strips_username_only() -> anyhow::Result<()> {
        let url = reqwest::Url::parse("https://someuser@host.example/")?;
        let redacted = redact_for_log(&url);
        anyhow::ensure!(
            !redacted.contains("someuser"),
            "redaction kept username: {redacted}"
        );
        Ok(())
    }

    #[test]
    fn redact_raw_for_log_emits_placeholder_for_unparseable() -> anyhow::Result<()> {
        anyhow::ensure!(redact_raw_for_log("not a url") == "<unparseable URL>");
        anyhow::ensure!(redact_raw_for_log("   ") == "<unparseable URL>");
        Ok(())
    }

    #[test]
    fn parse_origin_url_errors_do_not_leak_credentials() -> anyhow::Result<()> {
        // URL parses cleanly but has a rejected query string. Credentials
        // must not appear in the rejection message — operators routinely
        // share config errors from logs.
        let err = parse_origin_url("https://user:secret@host.example/path?token=abc")
            .err()
            .ok_or_else(|| anyhow::anyhow!("expected rejection"))?
            .to_string();
        anyhow::ensure!(!err.contains("secret"), "error leaked password: {err}");
        anyhow::ensure!(!err.contains("user"), "error leaked username: {err}");
        Ok(())
    }

    #[test]
    fn parse_origin_url_rejects_unparseable_input() -> anyhow::Result<()> {
        let err = parse_origin_url("not a url")
            .err()
            .ok_or_else(|| anyhow::anyhow!("bare text should have been rejected"))?
            .to_string();
        anyhow::ensure!(
            err.contains("invalid origin base URL"),
            "error lacked context: {err}"
        );
        Ok(())
    }

    /// `DEFAULT_USER_AGENT` (#435) embeds the `decdn-cache` crate's
    /// `CARGO_PKG_VERSION` so origin operators can attribute pull-through
    /// traffic. The `decdn-node/` prefix is intentionally stable —
    /// operators may grep on it in access logs.
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
