//! HTTP origin backend.
//!
//! Fetches blobs from `{base_url}/{blake3_hex}`. The base URL is opaque
//! per-node config; it is never put on the wire (see ADR 012 — "no external
//! origin URLs are ever exposed").

use std::cmp::min;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::Context;
use bytes::BytesMut;
use iroh_blobs::Hash;
use reqwest::StatusCode;

use super::{Origin, OriginFetch};

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

/// Origin backed by a plain HTTP(S) endpoint serving content-addressed blobs
/// at `{base_url}/{blake3_hex}`.
#[derive(Debug, Clone)]
pub struct HttpOrigin {
    client: reqwest::Client,
    base_url: OriginUrl,
    response_headers_timeout: Duration,
    chunk_idle_timeout: Duration,
}

impl HttpOrigin {
    /// Build an [`HttpOrigin`] around a validated [`OriginUrl`]. The only way
    /// to obtain one is [`parse_origin_url`], so invariants are enforced at
    /// the type boundary rather than documented in prose. Timeouts default
    /// to the `RESPONSE_HEADERS_TIMEOUT` and `CHUNK_IDLE_TIMEOUT` constants; override
    /// with [`Self::with_timeouts`] if operator policy or tests require
    /// different values.
    pub fn new(base_url: OriginUrl) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .context("failed to build reqwest client")?;
        Ok(Self {
            client,
            base_url,
            response_headers_timeout: RESPONSE_HEADERS_TIMEOUT,
            chunk_idle_timeout: CHUNK_IDLE_TIMEOUT,
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
}

impl Origin for HttpOrigin {
    fn fetch(
        &self,
        hash: Hash,
        max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<OriginFetch>> + Send + '_>> {
        Box::pin(async move {
            let url = self
                .base_url
                .as_url()
                .join(&hash.to_hex())
                .with_context(|| format!("failed to build URL for {hash}"))?;
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
            let mut resp = tokio::time::timeout(headers_timeout, send_fut)
                .await
                .with_context(|| {
                    format!("origin GET {url_log} headers timed out after {headers_timeout:?}")
                })?
                .with_context(|| format!("origin GET {url_log} failed"))?;

            let status = resp.status();
            if status == StatusCode::NOT_FOUND {
                return Ok(OriginFetch::NotFound);
            }
            if !status.is_success() {
                anyhow::bail!("origin GET {url_log} returned {status}");
            }

            // Fast-path rejection using the advertised length before we
            // read anything — avoids setting up a streaming buffer when the
            // origin already told us it would overrun the cap.
            if let Some(len) = resp.content_length()
                && len > max_bytes
            {
                anyhow::bail!(
                    "origin GET {url_log} advertises {len} bytes, exceeds max {max_bytes}"
                );
            }

            // Stream the body chunk-by-chunk. Three guards run per chunk:
            //   1. per-chunk idle timeout (bounds slow-trickle origins)
            //   2. running total cap (bounds memory; the only defense when
            //      Content-Length is absent or misreported)
            //   3. propagate reqwest transport errors
            let hint = resp
                .content_length()
                .and_then(|l| usize::try_from(l).ok())
                .map_or(0, |l| min(l, INITIAL_CAPACITY_HINT_CAP));
            let idle_timeout = self.chunk_idle_timeout;
            let mut buf = BytesMut::with_capacity(hint);
            loop {
                let chunk_result = tokio::time::timeout(idle_timeout, resp.chunk())
                    .await
                    .with_context(|| {
                        format!(
                            "origin GET {url_log} body read stalled for {idle_timeout:?} after {} bytes buffered",
                            buf.len()
                        )
                    })?
                    .with_context(|| format!("origin GET {url_log} body read failed"))?;
                let Some(chunk) = chunk_result else { break };
                let next_total = (buf.len() as u64).saturating_add(chunk.len() as u64);
                if next_total > max_bytes {
                    anyhow::bail!(
                        "origin GET {url_log} body exceeds max_bytes={max_bytes} mid-stream"
                    );
                }
                buf.extend_from_slice(&chunk);
            }
            Ok(OriginFetch::Found(buf.freeze()))
        })
    }
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
}
