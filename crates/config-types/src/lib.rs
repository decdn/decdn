//! Config-vocabulary value types shared by `decdn-cache` and `decdn-common`.
//!
//! This is a leaf crate: its only *runtime* dependencies are `serde`,
//! `url`, and `anyhow` (the test suite also uses `serde_json`, a
//! dev-dependency that never reaches the release binary).
//! It deliberately does NOT pull `iroh-blobs`, the AWS SDK, or `reqwest`,
//! so the publisher CLI (`decdn`, via `decdn-common`) links none of the
//! heavy storage/network stack — see `adr/appendix-binaries.md` and
//! issue #578.
//!
//! `decdn-cache` re-exports every type here so existing `decdn_cache::*`
//! paths in `decdn-node` keep resolving; the conversion between the
//! [`struct@Hash`] newtype here and `iroh_blobs::Hash` (the blob-store hash)
//! lives behind a thin shim in `decdn-cache`.

mod bytes;
mod circuit_breaker;
mod decompress;
mod defaults;
mod hash;
mod origin_kind;
mod origin_url;
mod retry;

pub use bytes::Bytes;
pub use circuit_breaker::CircuitBreakerPolicy;
pub use decompress::DecompressMode;
pub use defaults::{
    DEFAULT_MAX_PROBE_HOLDS, DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY,
    DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL_SEC, DEFAULT_ORIGIN_PROBE_TIMEOUT_MS,
    DEFAULT_ORIGIN_PROBE_TTL_SEC, DEFAULT_USER_AGENT,
};
pub use hash::{DeniedHashes, Hash, HashParseError, PinDiff, PinnedHashes};
pub use origin_kind::OriginKind;
pub use origin_url::{OriginUrl, parse_origin_url, redact_for_log};
pub use retry::{RetryPolicy, default_buffered_max_bytes};
