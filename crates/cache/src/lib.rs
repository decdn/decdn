//! Cache engine for deCDN.
//!
//! Wraps `iroh-blobs` with origin pull-through logic: on a cache miss, the
//! engine fetches from a configured [`Origin`] backend, verifies the BLAKE3
//! hash, and inserts the bytes before returning them to the caller. Node-to-
//! node pull-through via `cdn/client/v1` is a follow-up once that ALPN exists.
//!
//! Leaf crate per ADR 023 — no `#[cfg(feature = "poc")]` here, no mode
//! branching. The `node` crate's wiring layer selects which origin backend
//! to construct.

pub mod engine;
pub mod error;
pub mod origin;

pub use engine::{CacheEngine, CacheStats};
pub use error::{CacheError, CacheResult, OriginError};
pub use iroh_blobs::Hash;
pub use origin::{FilesystemOrigin, HttpOrigin, Origin, OriginFetch, OriginUrl, parse_origin_url};
