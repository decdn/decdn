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

/// Marker error packed into the `io::Error` that the cache's
/// stream wrappers write to the engine's side channel when an
/// origin delivers more than `max_blob_bytes`. The engine
/// downcasts via `io::Error::get_ref` to surface
/// `CacheError::BlobTooLarge` (matching the pre-streaming typed
/// variant) instead of generic `OriginError`. Adapter-internal
/// caps (HTTP's encoded chunk cap) and the engine's running cap
/// on streamed bytes both produce this marker so the operator-
/// visible error is consistent regardless of which layer first
/// detected the overrun.
#[derive(Debug)]
pub(crate) struct BlobTooLargeMarker {
    pub max_bytes: u64,
}

impl std::fmt::Display for BlobTooLargeMarker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "origin stream exceeded max_blob_bytes={} mid-flight",
            self.max_bytes
        )
    }
}

impl std::error::Error for BlobTooLargeMarker {}

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
/// Errors flow as `io::Error`. They are routed through retry
/// classification by one of two paths (#519):
///
/// - **Buffer-then-commit (small blobs):** when the adapter's
///   `size_hint` is at or below
///   `cache.origin_retry.buffered_max_bytes` (default 4 MiB), the
///   engine drains the stream into a bounded `BytesMut` before
///   handing to iroh-blobs. Drain errors are classified via
///   `crate::retry::classify_io_error` and re-feed the retry loop.
///   No on-disk amplification — the partial bytes never reach the
///   store.
/// - **Abort + restart (large blobs / unknown `size_hint`):** the
///   engine streams directly into `iroh_blobs::Blobs::add_stream`
///   and captures any mid-stream `io::Error` via the side channel.
///   After `temp_tag().await` completes, the captured error is
///   classified; on `Transient` the partial `TempTag` is dropped
///   (iroh-blobs GC reclaims the bytes at `cache.gc_interval_sec`
///   cadence, #518) and the retry loop restarts from the headers
///   phase. Worst-case orphaned bytes per fetch are
///   `(1 + max_retries) * max_blob_bytes` until GC.
///
/// Typed `Permanent` markers short-circuit retry regardless of which
/// path applies: the internal `BlobTooLargeMarker` surfaces as
/// [`crate::CacheError::BlobTooLarge`] (size-cap breach is
/// deterministic — retry won't fit a bigger blob into a smaller cap),
/// and [`crate::OriginError::DecompressionFailed`] surfaces as
/// `Permanent` (corrupt body — retry sees the same bytes again).
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
/// `Found` carries a streaming primitive so the cache engine pipes
/// origin bytes straight into
/// [`iroh_blobs::api::blobs::Blobs::add_stream`] without ever holding
/// the full blob in memory (issue #271). `size_hint` carries the
/// adapter's best-effort length advertisement (HTTP `Content-Length`,
/// filesystem `metadata().len()`, S3 `content_length()`); the engine
/// uses it for a short-circuit `BlobTooLarge` check before reading
/// the first byte and falls back to a running-total check via
/// `count_and_cap_stream` for adapters that can't know the length
/// upfront or origins that lie. The hint is **advisory** — adapters
/// that don't know set `None`, and even when set the actual stream
/// length may differ (e.g., HTTP origins lying in `Content-Length`).
pub enum OriginFetch {
    /// The origin returned the bytes for this hash as a stream of chunks.
    Found {
        /// Chunked byte stream. See [`OriginByteStream`] for the bounds.
        stream: OriginByteStream,
        /// Best-effort upfront size estimate from the adapter (HTTP
        /// `Content-Length`, filesystem `metadata().len()`, S3
        /// `content_length()`). Treated as advisory by the engine —
        /// the running cap on streamed bytes is the load-bearing
        /// defense.
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
    /// [`OriginFetch::Found`]. Useful for tests and any caller that
    /// already has the whole payload in hand. Real streaming adapters
    /// (`FilesystemOrigin`, `HttpOrigin`, `S3Origin`) construct their
    /// streams directly and don't go through this shim.
    #[must_use]
    pub fn found_one_shot(bytes: Bytes) -> Self {
        let size_hint = u64::try_from(bytes.len()).ok();
        let stream = futures_util::stream::once(async move { Ok(bytes) });
        Self::Found {
            stream: Box::pin(stream),
            size_hint,
        }
    }

    /// Drain the stream into a single [`Bytes`]. Returns `Ok(None)`
    /// for `NotFound`, `Ok(Some(bytes))` for `Found`. Used by tests
    /// and other helpers that need the full payload as a contiguous
    /// buffer; production code on the cache pull-through path goes
    /// through `add_stream` directly and never collects.
    ///
    /// The initial allocation is capped at 1 MiB regardless of
    /// `size_hint` so a hostile origin advertising a multi-TiB
    /// `Content-Length` can't trigger a huge alloc up front. The
    /// buffer still grows incrementally past the cap; the engine's
    /// running cap on streamed bytes catches the actual overrun.
    pub async fn collect_to_bytes(self) -> Result<Option<Bytes>, std::io::Error> {
        // Bound the upfront allocation regardless of `size_hint`. A
        // hostile origin advertising `Content-Length: 10 TiB` would
        // otherwise cause `BytesMut::with_capacity` to commit a huge
        // virtual region before the first byte arrives — the engine's
        // running cap on the streamed bytes catches the actual
        // overrun, but the per-fetch initial allocation is the
        // operator-visible DoS axis we still have to bound here. 1
        // MiB matches the initial buffer hint the buffered code
        // path used for the same reason; the buffer grows
        // incrementally past it as chunks arrive.
        const INITIAL_CAP: usize = 1 << 20; // 1 MiB
        use bytes::BytesMut;
        use futures_util::StreamExt;
        match self {
            Self::NotFound => Ok(None),
            Self::Found {
                mut stream,
                size_hint,
            } => {
                let cap = size_hint
                    .and_then(|n| usize::try_from(n).ok())
                    .unwrap_or(0)
                    .min(INITIAL_CAP);
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
