//! HTTP origin backend.
//!
//! Fetches blobs from `{base_url}/{blake3_hex}`. The base URL is opaque
//! per-node config; it is never put on the wire (see ADR 012 — "no external
//! origin URLs are ever exposed").

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::Context;
use iroh_blobs::Hash;
use reqwest::StatusCode;

use super::{Origin, OriginFetch};

/// Default per-request timeout for the HTTP origin. Large enough for slow
/// first-byte over a WAN, small enough that a hung origin doesn't pin a task
/// indefinitely. Operators needing a different value construct via
/// [`HttpOrigin::with_timeout`].
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// than overwriting the final path component.
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        Self::with_timeout(base_url, DEFAULT_REQUEST_TIMEOUT)
    }

    /// Like [`Self::new`] but with a caller-specified request timeout.
    pub fn with_timeout(base_url: &str, timeout: Duration) -> anyhow::Result<Self> {
        let normalized = if base_url.ends_with('/') {
            base_url.to_string()
        } else {
            format!("{base_url}/")
        };
        let url = reqwest::Url::parse(&normalized)
            .with_context(|| format!("invalid origin base URL: {base_url:?}"))?;
        let client = reqwest::Client::builder()
            .timeout(timeout)
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

            let resp = self
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

            if let Some(len) = resp.content_length()
                && len > max_bytes
            {
                anyhow::bail!("origin GET {url} advertises {len} bytes, exceeds max {max_bytes}");
            }

            // Read into memory. Streaming directly into the store is a
            // follow-up; the MVP holds the blob in memory and the engine
            // enforces `max_bytes` on the actual payload after receipt.
            let body = resp
                .bytes()
                .await
                .with_context(|| format!("failed to read origin body for {hash}"))?;
            Ok(OriginFetch::Found(body))
        })
    }
}
