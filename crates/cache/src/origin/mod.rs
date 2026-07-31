//! Origin backends — the source of truth a node falls back to on a cache miss.
//!
//! [`Origin`] is the seam. Three concrete implementations ship today:
//! [`HttpOrigin`] (generic HTTP/S), [`FilesystemOrigin`] (local disk), and
//! [`S3Origin`] (S3-compatible object stores: AWS S3, R2, B2, `MinIO`).

pub(crate) mod decompress;
pub mod fs;
pub mod http;
pub mod s3;

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use futures_util::Stream;
use iroh_blobs::Hash;

pub use fs::FilesystemOrigin;
pub use http::HttpOrigin;
pub use s3::{S3Credentials, S3Origin, S3OriginConfig};
// `OriginKind`, `OriginUrl`, `DecompressMode`, `parse_origin_url`, and
// `DEFAULT_USER_AGENT` moved to the `decdn-config-types` leaf crate
// (#578); re-export from there so `decdn_cache::origin::*` paths and the
// `Origin` trait's `kind()` return type stay identical to the
// top-level `decdn_cache::*` re-exports.
pub use decdn_config_types::{
    DEFAULT_USER_AGENT, DecompressMode, OriginKind, OriginUrl, parse_origin_url, redact_for_log,
};

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

// `OriginKind` (the backend tag surfaced through the admin
// eviction-preview, #439) moved to the `decdn-config-types` leaf crate
// (#578) and is re-exported above. Its serde/`as_str`/`Display`
// wire-form contract and the `origin_kind_*` tests live there.

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
    /// buffer; the cache pull-through path goes through `add_stream`
    /// directly and never collects.
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

/// Result of an [`Origin::fetch_range`] call ([ADR 037 §Origin-tier
/// pull-through](../../../adr/037-regional-proxy-warming.md), #823).
///
/// A range-scoped origin pull fetches only the requested byte span plus the
/// small sibling `{H}.obao4` outboard, then verifies the span against the
/// root `H` via [`crate::range_pull::encode_verified_range`] before importing
/// it as a partial blob — avoiding whole-blob origin egress to serve a byte
/// range on a cache miss.
///
/// The optimization is **best-effort**: when the origin does not publish the
/// sibling outboard, does not honor `Range`, or the outboard is absent/short,
/// the adapter returns [`Self::Unsupported`] and the engine degrades to the
/// existing whole-blob [`Origin::fetch`] pull. That fallback is never a
/// correctness or availability failure — it only forgoes the cost reduction
/// (ADR 037 §"Fallback is always correct").
pub enum OriginRangeFetch {
    /// The origin served both the requested byte span and the sibling
    /// `{H}.obao4` outboard. `data` covers exactly
    /// `[aligned.fetch_start(), aligned.fetch_end())` (the chunk-group-aligned
    /// span the engine asked for); `outboard` is the untrusted pre-order
    /// outboard. Neither is trusted until
    /// [`crate::range_pull::encode_verified_range`] verifies them against `H`.
    Ranged {
        /// The aligned data bytes, exactly `aligned.fetch_len()` long.
        data: Bytes,
        /// The raw, untrusted `{H}.obao4` outboard bytes.
        outboard: Bytes,
    },
    /// The origin reported the object (data key) does not exist.
    NotFound,
    /// The range optimization is not available for this fetch — no published
    /// outboard, no `Range`/`206` support, or a short/absent outboard. The
    /// engine degrades to a whole-blob [`Origin::fetch`] pull. This is the
    /// *expected* path for origins that don't publish `{H}.obao4`, not an
    /// error.
    Unsupported,
}

impl std::fmt::Debug for OriginRangeFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ranged { data, outboard } => f
                .debug_struct("Ranged")
                .field("data_len", &data.len())
                .field("outboard_len", &outboard.len())
                .finish(),
            Self::NotFound => f.write_str("NotFound"),
            Self::Unsupported => f.write_str("Unsupported"),
        }
    }
}

/// The chunk-group-aligned span a [`Origin::fetch_range`] call must fetch,
/// passed from the engine to the adapter. Carries both the byte span (for the
/// data read) and the BLAKE3 [`struct@Hash`] (so the adapter can locate the sibling
/// `{H}.obao4` outboard key). This is the produced-by-engine half of the range
/// pull; the verify-against-root half lives in [`crate::range_pull`].
#[derive(Debug, Clone, Copy)]
pub struct OriginRangeRequest {
    /// First byte of the chunk-group-aligned data span to fetch (inclusive).
    pub fetch_start: u64,
    /// One past the last byte of the data span to fetch (exclusive). The
    /// adapter issues an inclusive-end `Range`/`GetObject` read of
    /// `[fetch_start, fetch_end)`.
    pub fetch_end: u64,
}

impl OriginRangeRequest {
    /// Number of data bytes to fetch (`fetch_end - fetch_start`). Saturating so
    /// a mis-constructed request can never underflow; the engine always builds
    /// these from a validated [`crate::range_pull::AlignedRange`], where
    /// `fetch_end >= fetch_start` holds by construction.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.fetch_end.saturating_sub(self.fetch_start)
    }

    /// Is the requested span empty? Only true for a zero-length blob; the
    /// engine never issues an empty range against a non-empty blob.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.fetch_end <= self.fetch_start
    }
}

/// Result of an [`Origin::fetch_outboard`] call — a standalone fetch of just
/// the sibling `{H}.obao4` outboard, with no accompanying blob data. Feeds a
/// later "stream-while-store" serve path (#1130): a node that already holds
/// the outboard but not the full blob can start verifying/serving a
/// requester's byte range before the origin pull-through of the data
/// completes, rather than waiting on a whole-blob fetch first.
///
/// Like [`OriginRangeFetch`], the origin is a dumb byte store: the returned
/// outboard is **untrusted** until verified against the root `H`.
pub enum OutboardFetch {
    /// The origin served the sibling `{H}.obao4` outboard, bounded by the
    /// caller's `outboard_max_bytes`.
    Found(Bytes),
    /// The origin reported the outboard object does not exist (e.g. HTTP
    /// 404, S3 `NoSuchKey`, a missing filesystem sibling).
    NotFound,
    /// The outboard fetch is not available from this origin — e.g. a status
    /// other than success/404 (redirect, permission decline), or an
    /// oversize outboard rejected before buffering. Never an error: the
    /// caller degrades to whatever fallback applies (a later whole-blob
    /// pull will re-surface a genuine, persistent fault at its proper
    /// severity).
    Unsupported,
}

impl std::fmt::Debug for OutboardFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Found(bytes) => f.debug_tuple("Found").field(&bytes.len()).finish(),
            Self::NotFound => f.write_str("NotFound"),
            Self::Unsupported => f.write_str("Unsupported"),
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

    /// Fetch the chunk-group-aligned byte span `req` of the blob `hash`
    /// **plus** its sibling `{H}.obao4` outboard, for a range-scoped pull
    /// ([ADR 037 §Origin-tier pull-through](../../../adr/037-regional-proxy-warming.md),
    /// #823). `outboard_max_bytes` caps the outboard read (the engine derives
    /// it from the blob size — an outboard is `O(blob/256)` and an oversize
    /// one is malformed/foreign).
    ///
    /// The default implementation returns [`OriginRangeFetch::Unsupported`],
    /// so a custom [`Origin`] needs no change and the engine degrades to a
    /// whole-blob [`Self::fetch`] pull. The three shipped adapters override it.
    ///
    /// Like [`Self::fetch`], the origin is a dumb byte store: the returned
    /// `data` and `outboard` are **untrusted** and verified against the root
    /// `H` by the engine via [`crate::range_pull::encode_verified_range`]
    /// before any byte is imported.
    ///
    /// Returning [`OriginRangeFetch::Unsupported`] is the correct, expected
    /// answer whenever the optimization can't apply (no `{H}.obao4`, no
    /// `Range`/`206`, short outboard) — it is not an error. Only genuine
    /// transport / permission failures surface as [`OriginPullError`].
    fn fetch_range(
        &self,
        _hash: Hash,
        _req: OriginRangeRequest,
        _outboard_max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async { Ok(OriginRangeFetch::Unsupported) })
    }

    /// Fetch **just** the sibling `{H}.obao4` outboard for the blob `hash` —
    /// no accompanying blob data (#1130, feeds a later "stream-while-store"
    /// serve path: a node that holds the outboard can start verifying
    /// against `H` before the full blob has finished pulling through).
    /// `outboard_max_bytes` caps the read, same rationale as
    /// [`Self::fetch_range`]'s outboard sub-fetch — the engine derives it
    /// from the blob size, and an oversize outboard is malformed/foreign.
    ///
    /// The default implementation returns [`OutboardFetch::Unsupported`], so
    /// a custom [`Origin`] needs no change. The three shipped adapters
    /// override it.
    ///
    /// Like [`Self::fetch_range`], returning [`OutboardFetch::Unsupported`]
    /// or [`OutboardFetch::NotFound`] is never an error — only a genuine
    /// transport / permission failure surfaces as [`OriginPullError`].
    fn fetch_outboard(
        &self,
        _hash: Hash,
        _outboard_max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async { Ok(OutboardFetch::Unsupported) })
    }

    /// Best-effort total byte size of the blob `hash`, used to scope a
    /// range-pull ([`Self::fetch_range`], #823): the bao tree needs the blob's
    /// **exact** total length to verify a sub-range against the root `H`, and
    /// the `{H}.obao4` outboard alone only pins the size to within one chunk
    /// group (`IROH_BLOCK_SIZE`, 16 KiB — the final group's true length isn't in
    /// the tree). The
    /// `cdn/client/v1` serving handler calls this on a cold ranged cache miss
    /// — via [`crate::CacheEngine::origin_size`] — before
    /// [`crate::CacheEngine::pull_through_range`].
    ///
    /// The probe is cheap (HTTP `HEAD` / S3 `HeadObject` / `fs` metadata) and
    /// **best-effort**: `Ok(None)` means the size is unavailable — the object
    /// is absent, the backend reports a `Content-Encoding` whose advertised
    /// length is the *encoded* size (not the canonical blob length, same trap
    /// as [`Self::fetch`]'s `size_hint`), or any other non-success status (404,
    /// permission denied, 5xx, a disabled redirect). Like the outboard read on
    /// the range-pull path ([`Self::fetch_range`]'s bounded helper), a
    /// status-level decline degrades to `None` rather than erroring — the engine
    /// falls back to a whole-blob [`Self::fetch`], which re-surfaces a genuine,
    /// *persistent* fault (a real 403/5xx) at its proper severity. Only a
    /// transport-level fault (timeout, connection failure) surfaces here as
    /// [`OriginPullError`]. The default returns `Ok(None)`, so a custom
    /// [`Origin`] needs no change and simply never range-pulls.
    fn size(
        &self,
        _hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, OriginPullError>> + Send + '_>> {
        Box::pin(async { Ok(None) })
    }

    /// Enumerate the hashes this origin currently holds, so a node can
    /// advertise them (probe `has_blob` / DHT announce) *before* any
    /// pull-through — closing the "cold origin blob is undiscoverable until
    /// first pulled" gap (#1130).
    ///
    /// The default returns an empty list. Only an *enumerable* backend
    /// overrides it: the local filesystem, whose sharded directory listing
    /// is the hash set. HTTP has no listing endpoint, and S3's
    /// `ListObjectsV2` is deliberately not walked (egress + unbounded
    /// bucket), so remote origins are advertised via the operator's
    /// `pinned_hashes` instead of this method.
    ///
    /// Enumeration is *presence-only* (trust model): it does not read or
    /// hash file contents. Callers verify served bytes against the content
    /// address at serve time, and a wrong/corrupt file is the publisher's
    /// error (it only hurts the publisher — an announced-but-unservable hash
    /// costs bandwidth + local reputation, never a bond slash, provided the
    /// serve path never signs an `ok:false` refusal).
    fn enumerate(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Hash>, OriginPullError>> + Send + '_>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    /// Tag identifying the backend type. Surfaced through
    /// [`crate::EvictionPreview::origin_kinds`] so admin dry-run callers
    /// can estimate origin egress cost (#439) — `Filesystem` is a local
    /// read; `Http` may bill metered bandwidth.
    fn kind(&self) -> OriginKind;
}
