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
use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};

pub use fs::FilesystemOrigin;
pub use http::{DEFAULT_USER_AGENT, DecompressMode, HttpOrigin, OriginUrl, parse_origin_url};
pub use s3::{S3Credentials, S3Origin, S3OriginConfig};

use crate::error::OriginPullError;

/// Tag identifying which [`crate::CacheEngine`] origin backend is
/// configured (#439). Surfaced through
/// [`crate::EvictionPreview::origin_kind`] so admin dry-run callers can
/// estimate origin egress cost — re-fetching from a `Filesystem` origin
/// is a local read; `Http` and `S3` may consume metered bandwidth.
///
/// Wire-form: lowercase serde tags (`http`, `filesystem`, `s3`) ship in
/// admin RPC JSON output so it stays operator-friendly and stable for
/// log scrapers. `OriginKind` itself is output-only — the
/// `[cache.origin]` TOML table is parsed via the unrelated
/// `OriginConfig` type in `decdn-common`.
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
#[derive(Debug)]
pub enum OriginFetch {
    /// The origin returned the bytes for this hash.
    Found(Bytes),
    /// The origin reported the object does not exist (e.g. HTTP 404).
    NotFound,
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
