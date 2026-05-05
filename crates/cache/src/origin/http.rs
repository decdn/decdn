//! HTTP origin backend.
//!
//! Fetches blobs from `{base_url}/{blake3_hex}`. The base URL is opaque
//! per-node config; it is never put on the wire (see ADR 012 — "no external
//! origin URLs are ever exposed").

use std::cmp::min;
use std::future::Future;
use std::io::Read;
use std::pin::Pin;
use std::time::Duration;

use anyhow::Context;
use bytes::{Bytes, BytesMut};
use iroh_blobs::Hash;
use reqwest::StatusCode;
use reqwest::header::CONTENT_ENCODING;
use serde::{Deserialize, Serialize};

use super::{Origin, OriginFetch};
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

/// Ceiling on the `BytesMut::with_capacity` hint derived from an
/// origin-advertised `Content-Length`. A legitimate 10 GB blob should grow
/// incrementally rather than pre-allocating 10 GB up front for one request.
const INITIAL_CAPACITY_HINT_CAP: usize = 1 << 20; // 1 MiB

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
    pub fn new(base_url: OriginUrl) -> anyhow::Result<Self> {
        // We want to inspect `Content-Encoding` and run the body through
        // our own decoders, so disable reqwest's built-in transparent
        // decompression — otherwise reqwest would strip the header and
        // hand back already-decompressed bytes, masking what the origin
        // actually sent and bypassing our `UnsupportedEncoding` error path.
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
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

/// Maximum buffer growth per decoder iteration. `flate2` and `zstd`'s
/// `Read` impls can ask for arbitrary sizes; we cap to avoid an attacker
/// sending a `Content-Encoding: gzip` body that decompresses to many GB.
/// The engine still enforces `max_blob_bytes` on the final result, so this
/// is just an inner-loop guard against pathological allocators.
const DECOMPRESS_READ_BUFFER: usize = 64 * 1024;

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

/// Decompress `body` according to `encoding`. Identity (empty / `identity`)
/// passes through. Anything else returns
/// [`OriginError::UnsupportedEncoding`].
///
/// The size cap (`max_bytes`) is enforced inside the decode loop so a
/// 1 KB compressed payload that expands to 100 GB fails fast rather than
/// allocating its way to OOM. The engine re-checks the final length, but
/// without the inner cap a malicious origin could keep the decoder buffer
/// growing past memory before the engine ever sees it.
fn decompress_body(body: Bytes, encoding: &str, max_bytes: u64) -> Result<Bytes, OriginError> {
    let trimmed = encoding.trim();
    let supported = match classify_encoding(trimmed) {
        None => return Ok(body),
        Some(Ok(s)) => s,
        Some(Err(e)) => return Err(e),
    };
    match supported {
        SupportedEncoding::Gzip => {
            let mut decoder = flate2::read::GzDecoder::new(body.as_ref());
            read_capped(&mut decoder, max_bytes, supported)
        }
        SupportedEncoding::Zstd => {
            let mut decoder =
                zstd::stream::read::Decoder::new(body.as_ref()).map_err(|source| {
                    OriginError::DecompressionFailed {
                        encoding: supported,
                        source,
                    }
                })?;
            read_capped(&mut decoder, max_bytes, supported)
        }
    }
}

/// Read from `decoder` into a growing buffer, bailing as soon as the total
/// would exceed `max_bytes`. Buffer grows in `DECOMPRESS_READ_BUFFER`
/// increments — small enough to stop a runaway decompression bomb early,
/// large enough to keep the syscall count down on legitimate payloads.
fn read_capped<R: Read>(
    decoder: &mut R,
    max_bytes: u64,
    encoding: SupportedEncoding,
) -> Result<Bytes, OriginError> {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = vec![0u8; DECOMPRESS_READ_BUFFER];
    loop {
        let n = decoder
            .read(&mut chunk)
            .map_err(|source| OriginError::DecompressionFailed { encoding, source })?;
        if n == 0 {
            break;
        }
        // n is bounded by chunk.len() = DECOMPRESS_READ_BUFFER, well under
        // u64::MAX, so the cast is safe — but use saturating_add anyway so
        // an unexpected n > buf.len() can't wrap.
        let next_total = (buf.len() as u64).saturating_add(n as u64);
        if next_total > max_bytes {
            return Err(OriginError::DecompressionFailed {
                encoding,
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("decompressed body exceeds max_bytes={max_bytes} mid-stream"),
                ),
            });
        }
        let slice = chunk.get(..n).unwrap_or(&[]);
        buf.extend_from_slice(slice);
    }
    Ok(Bytes::from(buf))
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
    status.is_server_error() || status == StatusCode::REQUEST_TIMEOUT || status.as_u16() == 429
}

impl Origin for HttpOrigin {
    // The body folds together URL parsing, the headers-phase request,
    // status classification, encoding extraction, the chunked-body
    // streaming loop, and the decompression branch. Splitting it
    // further would force passing `resp` and `url_log` across a
    // boundary that would obscure the linear failure-classification
    // flow more than the length itself does.
    #[allow(clippy::too_many_lines)]
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
            let mut resp = match tokio::time::timeout(headers_timeout, send_fut).await {
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

            // Capture the encoding before consuming the body. The
            // header is RFC 9110 § 5.6.7 — restricted to ASCII tokens —
            // so a header value that fails `to_str()` (non-ASCII bytes,
            // illegal control chars, etc.) is a malformed origin
            // response, not "no encoding". Surfacing
            // `MalformedEncoding` keeps a buggy or hostile origin from
            // smuggling raw compressed bytes past the decompression
            // layer by sending an unparsable header.
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
            let is_compressed = !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("identity");

            // Fast-path rejection using the advertised length before we
            // read anything. For identity bodies the encoded length is
            // the payload length and must not exceed `max_bytes`. For
            // compressed bodies the encoded length is decoupled from
            // the post-decompression byte count, but it can never
            // *legitimately* exceed `max_bytes` — a compression ratio
            // < 1 (encoded smaller than decoded) is universal in
            // practice. An origin advertising a compressed length above
            // the decompressed cap is either malicious or misconfigured;
            // either way we burn no bandwidth.
            if let Some(len) = resp.content_length()
                && len > max_bytes
            {
                // Size-cap breach: the origin is sending us more bytes
                // than the operator allowed. Retrying won't help — this
                // is permanent.
                return Err(OriginPullError::Permanent(anyhow::anyhow!(
                    "origin GET {url_log} advertises {len} bytes, exceeds max {max_bytes}"
                )));
            }

            // Stream the body chunk-by-chunk. Three guards run per chunk:
            //   1. per-chunk idle timeout (bounds slow-trickle origins)
            //   2. running total cap (bounds memory; the only defense when
            //      Content-Length is absent or misreported)
            //   3. propagate reqwest transport errors
            //
            // For compressed bodies, the per-chunk cap is the *encoded*
            // size — we still need an upper bound because a malicious
            // origin could otherwise stream forever. Use `max_bytes` as
            // the encoded cap too: realistic compressed payloads are
            // smaller than their decompressed form (compression ratios
            // > 1.0 are pathological), so the same ceiling works in
            // practice. `decompress_body` re-enforces `max_bytes` on the
            // decompressed bytes.
            let hint = resp
                .content_length()
                .and_then(|l| usize::try_from(l).ok())
                .map_or(0, |l| min(l, INITIAL_CAPACITY_HINT_CAP));
            let idle_timeout = self.chunk_idle_timeout;
            let mut buf = BytesMut::with_capacity(hint);
            loop {
                let chunk_result = match tokio::time::timeout(idle_timeout, resp.chunk()).await {
                    Err(_elapsed) => {
                        return Err(OriginPullError::Transient(anyhow::anyhow!(
                            "origin GET {url_log} body read stalled for {idle_timeout:?} after {} bytes buffered",
                            buf.len()
                        )));
                    }
                    Ok(Err(reqwest_err)) => {
                        return Err(classify_reqwest_error(reqwest_err).map_inner(|e| {
                            e.context(format!("origin GET {url_log} body read failed"))
                        }));
                    }
                    Ok(Ok(chunk)) => chunk,
                };
                let Some(chunk) = chunk_result else { break };
                let next_total = (buf.len() as u64).saturating_add(chunk.len() as u64);
                if next_total > max_bytes {
                    return Err(OriginPullError::Permanent(anyhow::anyhow!(
                        "origin GET {url_log} body exceeds max_bytes={max_bytes} mid-stream"
                    )));
                }
                buf.extend_from_slice(&chunk);
            }
            let raw = buf.freeze();

            // Decompression layer (#312). In `DecompressMode::Strict`
            // we still validate the encoding: a gzip-encoded response
            // served as raw bytes would later trip a BLAKE3 mismatch
            // in the engine, which is a much harder failure mode to
            // debug than a clear `UnsupportedEncoding` error here.
            if matches!(self.decompress, DecompressMode::Strict) {
                if is_compressed {
                    return Err(OriginPullError::Permanent(
                        OriginError::UnsupportedEncoding {
                            encoding: trimmed.into(),
                        }
                        .into(),
                    ));
                }
                return Ok(OriginFetch::Found(raw));
            }
            // Sync decompression on big payloads would block a tokio
            // worker the same way BLAKE3 does (engine.rs uses
            // `spawn_blocking` above 1 MiB). Mirror that policy here so
            // a 10 GB gzipped origin can't starve the executor.
            //
            // The `OriginError` is propagated *typed* (no
            // `with_context` wrapping it directly) so
            // [`crate::CacheError::origin_error_kind`] can recover the
            // variant from the chain without a string match.
            let decoded = if raw.len() <= DECOMPRESS_BLOCKING_THRESHOLD {
                decompress_body(raw, &encoding, max_bytes)
                    .map_err(|e| OriginPullError::Permanent(e.into()))?
            } else {
                let raw_for_decode = raw;
                let encoding_for_decode = encoding.clone();
                let join_result = tokio::task::spawn_blocking(move || {
                    decompress_body(raw_for_decode, &encoding_for_decode, max_bytes)
                })
                .await;
                match join_result {
                    Ok(Ok(bytes)) => bytes,
                    Ok(Err(decode_err)) => {
                        return Err(OriginPullError::Permanent(decode_err.into()));
                    }
                    Err(join_err) => {
                        // JoinError is a runtime failure (panic /
                        // cancellation). Treat as transient — a panic
                        // *here* probably won't recur on the next try
                        // and operators see the full chain in logs.
                        return Err(OriginPullError::Transient(
                            anyhow::Error::from(join_err).context(format!(
                                "origin GET {url_log} body decompression task failed to join"
                            )),
                        ));
                    }
                }
            };
            Ok(OriginFetch::Found(decoded))
        })
    }
}

/// Above this size we pay the `spawn_blocking` round-trip rather than
/// hold a tokio worker through a synchronous `flate2`/`zstd` decode.
/// Mirrors `engine.rs::CacheEngine::BLOCKING_HASH_THRESHOLD` so the
/// crossover point is consistent across the cache hot path.
const DECOMPRESS_BLOCKING_THRESHOLD: usize = 1 << 20; // 1 MiB

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
}
