//! Origin backends — the source of truth a node falls back to on a cache miss.
//!
//! [`Origin`] is the seam. Three concrete implementations ship today:
//! [`HttpOrigin`] (generic HTTP/S), [`FilesystemOrigin`] (local disk), and
//! [`S3Origin`] (S3-compatible object stores: AWS S3, R2, B2, `MinIO`).

pub mod fs;
pub mod http;
pub mod s3;

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use futures_util::Stream;
use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};

pub use fs::FilesystemOrigin;
pub use http::{DEFAULT_USER_AGENT, DecompressMode, HttpOrigin, OriginUrl, parse_origin_url};
pub use s3::{S3Credentials, S3Origin, S3OriginConfig};

use crate::error::OriginPullError;

/// Boxed byte stream returned by [`Origin::fetch`]. Each [`Bytes`] item is
/// one chunk of the blob's payload; the stream completes when the origin
/// signals end-of-data.
///
/// The bound matches [`iroh_blobs::api::blobs::Blobs::add_stream`]: `Send +
/// Sync + 'static` so the engine can hand the stream straight through to
/// the store layer without an intermediate buffer. `'static` means impls
/// move whatever per-fetch state they hold (file handle, HTTP response,
/// S3 `ByteStream`) into the stream by `move`-into-async-block; nothing
/// borrows from `&self` on the origin.
///
/// Errors flow as `io::Error` so the engine can collapse mid-stream
/// failures into [`OriginPullError::Permanent`] without re-classifying
/// every adapter's native error type. Headers-phase classification stays
/// in each adapter (transient vs permanent) and is emitted before the
/// stream is constructed.
pub type OriginByteStream =
    Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static>>;

/// Tag identifying which [`Origin`] backend a [`crate::CacheEngine`] is
/// configured against (#439). Surfaced through
/// [`crate::EvictionPreview::origin_kind`] so admin dry-run callers can
/// estimate origin egress cost — re-fetching from a `Filesystem` origin
/// is a local read; `Http` and `S3` may consume metered bandwidth.
///
/// The serde representation uses lowercase tag names (`http`,
/// `filesystem`, `s3`) so admin RPC JSON output is operator-friendly
/// and stable for log scrapers. (`OriginKind` itself is output-only —
/// it is never deserialised from operator TOML; the `[cache.origin]`
/// table uses `OriginConfig`.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum OriginKind {
    /// Pull-through over HTTP/HTTPS via [`HttpOrigin`].
    Http,
    /// Pull-through from a local filesystem directory via
    /// [`FilesystemOrigin`].
    Filesystem,
    /// Pull-through from an S3-compatible object store via [`S3Origin`].
    S3,
}

impl OriginKind {
    /// Stable lowercase string label suitable for log fields and
    /// metrics. Operators may grep on this — keep the variants stable.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Filesystem => "filesystem",
            Self::S3 => "s3",
        }
    }
}

impl std::fmt::Display for OriginKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Result of an [`Origin::fetch`] call.
///
/// `Found` carries a streaming primitive (issue #271) so the engine can
/// pipe origin bytes straight into [`iroh_blobs::api::blobs::Blobs::add_stream`]
/// without ever holding the full blob in memory. `size_hint` is the
/// adapter's best-effort length advertisement (HTTP `Content-Length`,
/// filesystem `metadata().len()`, S3 `content_length()`); `None` when
/// the adapter doesn't know.
pub enum OriginFetch {
    /// The origin returned the bytes for this hash as a stream of chunks.
    Found {
        /// Chunked byte stream. See [`OriginByteStream`] for the bounds.
        stream: OriginByteStream,
        /// Best-effort upfront size estimate from the adapter. Used by
        /// the engine for short-circuit cap checks before reading the
        /// first byte; the engine still re-checks the running total as
        /// chunks arrive.
        size_hint: Option<u64>,
    },
    /// The origin reported the object does not exist (e.g. HTTP 404).
    NotFound,
}

impl std::fmt::Debug for OriginFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Found { size_hint, .. } => f
                .debug_struct("Found")
                .field("size_hint", size_hint)
                .finish_non_exhaustive(),
            Self::NotFound => f.write_str("NotFound"),
        }
    }
}

impl OriginFetch {
    /// Wrap a fully-buffered `Bytes` payload as a single-chunk
    /// [`OriginFetch::Found`]. Used by adapters during the PR-1 trait
    /// migration (issue #271) and by test mocks that have the whole
    /// payload in hand. Real streaming adapters land in PR 2-4 and use
    /// adapter-specific stream constructors directly.
    #[must_use]
    pub fn found_one_shot(bytes: Bytes) -> Self {
        let size_hint = u64::try_from(bytes.len()).ok();
        let stream = futures_util::stream::once(async move { Ok(bytes) });
        Self::Found {
            stream: Box::pin(stream),
            size_hint,
        }
    }

    /// Drain the stream into a single [`Bytes`] for tests and the
    /// engine's transitional buffered path. Returns `Ok(None)` for
    /// `NotFound`, `Ok(Some(bytes))` for `Found`.
    ///
    /// `cfg(any(test, feature = "test-support"))` would normally gate
    /// this — it stays `pub` (un-gated) for now because the engine's
    /// PR-1 transitional `pull_through` calls it on the production
    /// path; PR 2 removes that call site and gates this helper behind
    /// `cfg(test)`.
    pub async fn collect_to_bytes(self) -> Result<Option<Bytes>, std::io::Error> {
        use bytes::BytesMut;
        use futures_util::StreamExt;
        match self {
            Self::NotFound => Ok(None),
            Self::Found {
                mut stream,
                size_hint,
            } => {
                let cap = size_hint.and_then(|n| usize::try_from(n).ok()).unwrap_or(0);
                let mut buf = BytesMut::with_capacity(cap);
                while let Some(chunk) = stream.next().await {
                    buf.extend_from_slice(&chunk?);
                }
                Ok(Some(buf.freeze()))
            }
        }
    }
}

/// An origin backend. Implementors fetch a blob identified by its BLAKE3 hash.
///
/// The origin is **not** responsible for verifying the hash — the cache engine
/// does that after the bytes come back. Returning the wrong bytes is a
/// detectable protocol violation, not a security breach: no data is trusted
/// until it matches the content address.
///
/// ## Error classification
///
/// Adapters return [`OriginPullError::Transient`] for failures that can be
/// retried (HTTP 5xx/408/429, transient I/O), and
/// [`OriginPullError::Permanent`] for failures that won't be cured by
/// retrying (HTTP 4xx other than 404, encoding/cap breaches, permission
/// denied). The cache engine drives the retry loop in
/// [`crate::retry::retry_fetch`] off this distinction. `NotFound` is *not*
/// an error — adapters return [`OriginFetch::NotFound`].
pub trait Origin: std::fmt::Debug + Send + Sync + 'static {
    /// Fetch the blob with the given hash. `max_bytes` is an advisory cap —
    /// implementations should short-circuit if they can cheaply detect the
    /// payload would exceed it (e.g. from a `Content-Length` header), and the
    /// cache engine enforces the cap on the returned bytes regardless.
    fn fetch(
        &self,
        hash: Hash,
        max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>>;

    /// Tag identifying the backend type. Surfaced through
    /// [`crate::EvictionPreview::origin_kind`] so admin dry-run callers
    /// can estimate origin egress cost (#439) — `Filesystem` is a local
    /// read; `Http` may bill metered bandwidth.
    fn kind(&self) -> OriginKind;
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// `OriginKind`'s JSON serialisation is part of the operator-visible
    /// contract — admin RPC responses, log scrapers, and runbook regex
    /// patterns all depend on the literal lowercase strings `"http"`,
    /// `"filesystem"`, and `"s3"`. A future refactor that swapped
    /// `#[serde(rename_all = "lowercase")]` for `snake_case`, dropped the
    /// attribute, or accidentally derived a different shape would break
    /// every consumer silently. Pin the wire form here so any such
    /// regression fails this test.
    #[test]
    fn origin_kind_serialises_to_stable_lowercase_strings() {
        assert_eq!(
            serde_json::to_string(&OriginKind::Http).expect("serialise"),
            "\"http\""
        );
        assert_eq!(
            serde_json::to_string(&OriginKind::Filesystem).expect("serialise"),
            "\"filesystem\""
        );
        assert_eq!(
            serde_json::to_string(&OriginKind::S3).expect("serialise"),
            "\"s3\""
        );
    }

    /// Round-trip via JSON to lock both the serialise *and* deserialise
    /// directions: a regression that only changed one direction (e.g.
    /// renamed a variant but added a `#[serde(alias = ...)]`) would
    /// still drift the operator-visible string.
    #[test]
    fn origin_kind_round_trips_through_json() {
        for variant in [OriginKind::Http, OriginKind::Filesystem, OriginKind::S3] {
            let s = serde_json::to_string(&variant).expect("serialise");
            let back: OriginKind = serde_json::from_str(&s).expect("deserialise");
            assert_eq!(variant, back, "round-trip for {variant:?}");
        }
    }

    /// `as_str` and `Display` must agree with the serde tag — operators
    /// who grep on `decdn_origin_kind="http"` in tracing log lines see
    /// the `Display` form, while admin RPC consumers see the JSON form.
    /// The two paths must not drift.
    #[test]
    fn origin_kind_as_str_and_display_match_serde_tag() {
        for variant in [OriginKind::Http, OriginKind::Filesystem, OriginKind::S3] {
            let json = serde_json::to_string(&variant).expect("serialise");
            // JSON wraps the tag in quotes; strip them.
            let unquoted = json.trim_matches('"');
            assert_eq!(
                variant.as_str(),
                unquoted,
                "as_str disagrees with serde tag for {variant:?}"
            );
            assert_eq!(
                variant.to_string(),
                unquoted,
                "Display disagrees with serde tag for {variant:?}"
            );
        }
    }
}
