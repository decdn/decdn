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
pub mod metrics;
pub mod origin;
pub mod probe_hold;
pub mod retry;

pub use engine::{
    CacheEngine, CacheStats, EvictionCandidates, EvictionPreview, PinDiff, PinnedHashes,
};
pub use error::{CacheError, CacheResult, OriginError, OriginPullError, SupportedEncoding};
pub use iroh_blobs::Hash;
pub use metrics::CacheMetrics;
pub use origin::{
    DEFAULT_USER_AGENT, DecompressMode, FilesystemOrigin, HttpOrigin, Origin, OriginFetch,
    OriginKind, OriginUrl, S3Credentials, S3Origin, S3OriginConfig, parse_origin_url,
};
pub use probe_hold::{
    DEFAULT_MAX_PROBE_HOLDS, PROBE_HOLD_DURATION, PROBE_HOLD_MARGIN, PROBE_SLASH_WINDOW,
    ProbeHoldOutcome,
};
pub use retry::RetryPolicy;
