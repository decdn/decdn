//! Origin backends — the source of truth a node falls back to on a cache miss.
//!
//! [`Origin`] is the seam. One concrete implementation (`HttpOrigin`) lives
//! alongside it; S3/R2/B2 backends are follow-up work.

pub mod http;

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use iroh_blobs::Hash;

pub use http::{HttpOrigin, OriginUrl, parse_origin_url};

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
pub trait Origin: std::fmt::Debug + Send + Sync + 'static {
    /// Fetch the blob with the given hash. `max_bytes` is an advisory cap —
    /// implementations should short-circuit if they can cheaply detect the
    /// payload would exceed it (e.g. from a `Content-Length` header), and the
    /// cache engine enforces the cap on the returned bytes regardless.
    fn fetch(
        &self,
        hash: Hash,
        max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<OriginFetch>> + Send + '_>>;
}
