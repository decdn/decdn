//! HTTP origin backend.
//!
//! Fetches blobs from `{base_url}/{blake3_hex}`. The base URL is opaque
//! per-node config; it is never put on the wire (see ADR 012 — "no external
//! origin URLs are ever exposed").

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::Context;
use bytes::BytesMut;
use iroh_blobs::Hash;
use reqwest::StatusCode;

use super::{Origin, OriginFetch};

/// How long to wait for the TCP/TLS handshake to complete. Per-chunk read
/// progress is bounded by the `max_bytes` cap in [`Origin::fetch`]; a total
/// request timeout is deliberately **not** set because it cannot be sized
/// correctly for both small blobs and `max_blob_size_mb = 10_240` (10 GB).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Origin backed by a plain HTTP(S) endpoint serving content-addressed blobs
/// at `{base_url}/{blake3_hex}`.
#[derive(Debug, Clone)]
pub struct HttpOrigin {
    client: reqwest::Client,
    base_url: reqwest::Url,
}

impl HttpOrigin {
    /// Build an [`HttpOrigin`] pointing at `base_url`. A trailing slash is
    /// appended if absent so that URL joining produces `{base}/{hex}` rather
    /// than overwriting the final path component. Only `http` and `https`
    /// schemes are accepted — `Url::parse` alone would silently accept
    /// `file://`, `ftp://`, etc.
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        let normalized = if base_url.ends_with('/') {
            base_url.to_string()
        } else {
            format!("{base_url}/")
        };
        let url = reqwest::Url::parse(&normalized)
            .with_context(|| format!("invalid origin base URL: {base_url:?}"))?;
        match url.scheme() {
            "http" | "https" => {}
            other => anyhow::bail!(
                "unsupported origin URL scheme {other:?} (expected http or https): {base_url:?}"
            ),
        }
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .context("failed to build reqwest client")?;
        Ok(Self {
            client,
            base_url: url,
        })
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
            let mut buf = BytesMut::with_capacity(
                usize::try_from(resp.content_length().unwrap_or(0)).unwrap_or(0),
            );
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
