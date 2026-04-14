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
/// as large as 10 GB and no single timeout fits both small and large blobs.
///
/// Known MVP limitation: a stalled mid-stream origin is **not** time-bounded;
/// only total bytes are capped via [`Origin::fetch`]'s `max_bytes` argument.
/// A per-chunk idle timeout is a follow-up.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Ceiling on the `BytesMut::with_capacity` hint derived from an
/// origin-advertised `Content-Length`. A legitimate 10 GB blob should grow
/// incrementally rather than pre-allocating 10 GB up front for one request.
const INITIAL_CAPACITY_HINT_CAP: usize = 1 << 20; // 1 MiB

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
    let mut url =
        reqwest::Url::parse(raw).with_context(|| format!("invalid origin base URL: {raw:?}"))?;
    match url.scheme() {
        "http" | "https" => {}
        other => anyhow::bail!(
            "unsupported origin URL scheme {other:?} (expected http or https): {raw:?}"
        ),
    }
    // A query or fragment on the base would be silently dropped by
    // `Url::join` when we append the blake3 hex, so reject up front rather
    // than accept a URL that can't possibly do what the operator expects.
    if url.query().is_some() {
        anyhow::bail!("origin URL must not contain a query string: {raw:?}");
    }
    if url.fragment().is_some() {
        anyhow::bail!("origin URL must not contain a fragment: {raw:?}");
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
}

impl HttpOrigin {
    /// Build an [`HttpOrigin`] around a validated [`OriginUrl`]. The only way
    /// to obtain one is [`parse_origin_url`], so invariants are enforced at
    /// the type boundary rather than documented in prose.
    pub fn new(base_url: OriginUrl) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .context("failed to build reqwest client")?;
        Ok(Self { client, base_url })
    }

    /// Convenience constructor that parses `raw` via [`parse_origin_url`]
    /// before delegating to [`Self::new`]. Prefer [`Self::new`] in
    /// production paths so URL validation happens at config-load time.
    pub fn parse(raw: &str) -> anyhow::Result<Self> {
        Self::new(parse_origin_url(raw)?)
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

            let mut resp = self
                .client
                .get(url.clone())
                .send()
                .await
                .with_context(|| format!("origin GET {url} failed"))?;

            let status = resp.status();
            if status == StatusCode::NOT_FOUND {
                return Ok(OriginFetch::NotFound);
            }
            if !status.is_success() {
                anyhow::bail!("origin GET {url} returned {status}");
            }

            // Fast-path rejection using the advertised length before we
            // read anything — avoids setting up a streaming buffer when the
            // origin already told us it would overrun the cap.
            if let Some(len) = resp.content_length()
                && len > max_bytes
            {
                anyhow::bail!("origin GET {url} advertises {len} bytes, exceeds max {max_bytes}");
            }

            // Stream the body chunk-by-chunk, rejecting as soon as the
            // running total would exceed `max_bytes`. This bounds the
            // memory an untrusted origin can force us to allocate even
            // when it omits or misreports `Content-Length`.
            let hint = resp
                .content_length()
                .and_then(|l| usize::try_from(l).ok())
                .map_or(0, |l| min(l, INITIAL_CAPACITY_HINT_CAP));
            let mut buf = BytesMut::with_capacity(hint);
            while let Some(chunk) = resp
                .chunk()
                .await
                .with_context(|| format!("origin GET {url} body read failed"))?
            {
                let next_total = (buf.len() as u64).saturating_add(chunk.len() as u64);
                if next_total > max_bytes {
                    anyhow::bail!("origin GET {url} body exceeds max_bytes={max_bytes} mid-stream");
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
