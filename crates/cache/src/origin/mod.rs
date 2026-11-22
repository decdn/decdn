//! Origin backends — the source of truth a node falls back to on a cache miss.
//!
//! [`Origin`] is the seam. Three concrete implementations ship:
//! `HttpOrigin` (generic HTTP/S), `FilesystemOrigin` (local disk), and
//! `S3Origin` (S3-compatible object stores: AWS S3, R2, B2, `MinIO`). The
//! `s3` feature, on by default, builds `S3Origin` and links the AWS SDK.
//!
//! An origin publishes three objects per blob, all of which `decdn origin
//! import` writes: the data `{H}`, its pre-order bao outboard `{H}.obao4`,
//! and its size proof `{H}.sizeproof`. A missing or broken object is an
//! ordinary origin fault. A node reads its own origin only through the
//! ranged path: the size proof answers presence and size, the outboard and
//! verified data windows answer ranges.

pub mod fs;
pub mod http;
pub(crate) mod io_runtime;
#[cfg(test)]
mod log_capture;
#[cfg(feature = "s3")]
pub mod s3;

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use iroh_blobs::Hash;

pub use fs::FilesystemOrigin;
pub use http::HttpOrigin;
#[cfg(feature = "s3")]
pub use s3::{S3Credentials, S3Origin, S3OriginConfig};
// Re-exported from the `decdn-config-types` leaf crate so
// `decdn_cache::origin::*` paths and the `Origin` trait's `kind()` return
// type stay identical to the top-level `decdn_cache::*` re-exports.
pub use decdn_config_types::{
    DEFAULT_USER_AGENT, OriginKind, OriginUrl, parse_origin_url, redact_for_log,
};

use crate::error::OriginPullError;

/// Result of an [`Origin::fetch`] call.
#[derive(Debug)]
pub enum OriginFetch {
    /// The origin reported the object does not exist.
    NotFound,
    /// The origin streamed this blob into the store itself, verifying each chunk
    /// group against the rooted hash via [`crate::CacheEngine::admit_bao_stream`], and it
    /// is present. The engine does not re-ingest: it reports the fill as
    /// committed. The node-to-node pull uses this so a whole blob never lands
    /// in RAM.
    AlreadyAdmitted,
}

/// Result of an [`Origin::fetch_range_data`] call ([ADR 037 §Origin-tier
/// pull-through](https://github.com/decdn/decdn/blob/main/adr/037-regional-proxy-warming.md), #823).
///
/// A range-scoped origin pull fetches the small sibling `{H}.obao4` outboard
/// once ([`Origin::fetch_outboard`]), then fetches the requested byte span in
/// windows of at most [`crate::RANGE_PULL_WINDOW_BYTES`], verifying them
/// against the root `H` before any byte is imported or served, with memory
/// bounded by the window rather than the span (#2065). The verifier is the
/// engine: one [`bao_tree::io::fsm::encode_ranges_validated`] pass on the
/// [`crate::CacheEngine::origin_range_wire`] path.
pub enum OriginRangeFetch {
    /// The origin served the requested byte span. `data` covers exactly
    /// `[req.fetch_start, req.fetch_end)` (the chunk-group-aligned span the
    /// engine asked for) and is untrusted until the engine verifies it against
    /// `H`.
    Ranged {
        /// The aligned data bytes, exactly `req.len()` long.
        data: Bytes,
    },
    /// The origin reported the object (data key) does not exist.
    NotFound,
}

impl std::fmt::Debug for OriginRangeFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ranged { data } => f
                .debug_struct("Ranged")
                .field("data_len", &data.len())
                .finish(),
            Self::NotFound => f.write_str("NotFound"),
        }
    }
}

/// The chunk-group-aligned span a [`Origin::fetch_range_data`] call must
/// fetch, passed from the engine to the adapter. This is the
/// produced-by-engine half of the range pull; the verify-against-root half
/// lives in [`crate::range_pull`] and [`crate::origin_range`].
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
/// the sibling `{H}.obao4` outboard, with no accompanying blob data.
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
}

impl std::fmt::Debug for OutboardFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Found(bytes) => f.debug_tuple("Found").field(&bytes.len()).finish(),
            Self::NotFound => f.write_str("NotFound"),
        }
    }
}

/// An origin backend. Implementors serve the objects of a blob identified by
/// its BLAKE3 hash.
///
/// The origin is **not** responsible for verifying the hash — the cache engine
/// does that before any byte is imported or served. Returning the wrong bytes
/// is a detectable protocol violation, not a security breach: no data is
/// trusted until it matches the content address.
///
/// ## Error classification
///
/// Adapters return [`OriginPullError::Transient`] for failures that can be
/// retried (HTTP 5xx/408/429, transient I/O), and
/// [`OriginPullError::Permanent`] for failures that won't be cured by
/// retrying (HTTP 4xx other than 404, an object over its cap or shorter
/// than the requested span, permission denied). A missing object is *not* an
/// error — each read answers its own `NotFound`. The HTTP adapter also reads
/// a 403 on the outboard and data reads as a missing object, because an
/// origin in front of a bucket without list permission answers 403 for a
/// missing key. On the `{H}.sizeproof` read, which is the presence answer, a
/// 403 is a permanent fault.
pub trait Origin: std::fmt::Debug + Send + Sync + 'static {
    /// Pull the whole blob `hash` into the store. Only the node-to-node origin
    /// pulls a whole blob: it admits the verified stream itself and answers
    /// [`OriginFetch::AlreadyAdmitted`]. `max_bytes` caps the blob it admits.
    ///
    /// The default answers [`OriginFetch::NotFound`]: an fs, HTTP or S3 origin
    /// serves its blobs through the ranged path ([`Self::fetch_size_proof`],
    /// [`Self::fetch_outboard`] and [`Self::fetch_range_data`]) only.
    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async { Ok(OriginFetch::NotFound) })
    }

    /// Fetch the chunk-group-aligned byte span `req` of the blob `hash`, for a
    /// range-scoped pull
    /// ([ADR 037 §Origin-tier pull-through](https://github.com/decdn/decdn/blob/main/adr/037-regional-proxy-warming.md),
    /// #823). Each call covers at most [`crate::RANGE_PULL_WINDOW_BYTES`]; a
    /// longer span takes several calls, in order. The engine fetches the
    /// sibling `{H}.obao4` outboard separately through [`Self::fetch_outboard`].
    ///
    /// `data` must be exactly `req.len()` bytes; an adapter that cannot serve
    /// exactly that span (no `Range`/`206` support, an object shorter than
    /// the span) answers [`OriginPullError::Permanent`]. A missing data
    /// object is [`OriginRangeFetch::NotFound`]. The engine turns both into a
    /// [`crate::CacheError::OriginError`] for the range: there is no other
    /// way to serve it.
    ///
    /// The returned `data` is **untrusted** and verified against the root `H`
    /// by the engine: one [`bao_tree::io::fsm::encode_ranges_validated`] pass
    /// on the [`crate::CacheEngine::origin_range_wire`] path — before any byte
    /// is imported or served.
    ///
    /// The default answers [`OriginRangeFetch::NotFound`].
    fn fetch_range_data(
        &self,
        _hash: Hash,
        _req: OriginRangeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<OriginRangeFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async { Ok(OriginRangeFetch::NotFound) })
    }

    /// Fetch **just** the sibling `{H}.obao4` outboard for the blob `hash`.
    /// `outboard_max_bytes` caps the read — the engine derives it from the
    /// blob size (an outboard is `O(blob/256)`), and an outboard over the cap
    /// is malformed or foreign: an [`OriginPullError::Permanent`]. The engine
    /// caches a non-empty outboard of at most 256 MiB (a blob of up to
    /// 64 GiB) per hash across range pulls, so it reads such an outboard once
    /// while the entry stays cached. It reads an empty outboard, or a larger
    /// one, again on each draw. Each pull fetches its span window by window
    /// through [`Self::fetch_range_data`].
    ///
    /// A missing outboard is [`OutboardFetch::NotFound`]; the engine treats it
    /// as an origin fault for any range of the blob. A transport fault, a
    /// retriable origin fault (an HTTP 5xx, 408 or 429), or an object the
    /// origin refuses to read (the filesystem origin's symlink escape or
    /// non-regular file) surfaces as [`OriginPullError`]. The HTTP adapter
    /// reads a 403 as [`OutboardFetch::NotFound`], because an origin in front
    /// of a bucket answers 403 for a missing key; the S3 adapter faults it as
    /// [`OriginPullError::Permanent`].
    ///
    /// The default answers [`OutboardFetch::NotFound`].
    fn fetch_outboard(
        &self,
        _hash: Hash,
        _outboard_max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OutboardFetch, OriginPullError>> + Send + '_>> {
        Box::pin(async { Ok(OutboardFetch::NotFound) })
    }

    /// Fetch the sibling `{H}.sizeproof` object of the blob `hash`: the size
    /// and its proof that `decdn origin import` writes beside the outboard
    /// (ADR 005 § Bounded byte ranges; [`decdn_bao_range::size_proof_object`]).
    /// The read is capped at [`decdn_bao_range::MAX_SIZE_PROOF_OBJECT_BYTES`];
    /// a larger object is an [`OriginPullError::Permanent`].
    ///
    /// This is the origin's presence and size answer
    /// ([`crate::CacheEngine::origin_presence`]): `Ok(None)` means the origin
    /// does not hold the blob. Like every origin byte, a returned object is
    /// **untrusted** until its proof verifies against `H`. A transport fault
    /// surfaces as [`OriginPullError`]. The default returns `Ok(None)`.
    fn fetch_size_proof(
        &self,
        _hash: Hash,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, OriginPullError>> + Send + '_>> {
        Box::pin(async { Ok(None) })
    }

    /// Enumerate the hashes this origin currently holds, so a node can
    /// advertise them (open answers / DHT announce) *before* any
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
    /// costs bandwidth + local reputation, nothing more).
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
