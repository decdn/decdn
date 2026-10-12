//! The value types of the deCDN config vocabulary: hashes, byte sizes, origin
//! URLs, and the retry, breaker, and decompression policies that a node's
//! config is built from.
//!
//! | Area | Types |
//! |---|---|
//! | Content | [`struct@Hash`] (a BLAKE3 hash as config text), [`PinnedHashes`], [`DeniedHashes`], [`PinDiff`] |
//! | Origins | [`OriginUrl`], [`OriginKind`], [`parse_origin_url`], [`redact_for_log`] |
//! | Policies | [`RetryPolicy`], [`CircuitBreakerPolicy`], [`DecompressMode`] |
//! | Sizes and defaults | [`Bytes`], the `DEFAULT_*` constants |
//!
//! Both `decdn-cache` and `decdn-common` build on these types. This crate
//! depends only on `serde`, `url`, and `anyhow`. It links no blob store, no
//! cloud SDK, and no HTTP client, so the `decdn` CLI can parse a node config
//! without them ([Binaries § Decision]).
//!
//! [Binaries § Decision]: https://github.com/decdn/decdn/blob/main/adr/appendix-binaries.md#decision

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
    DEFAULT_MAX_PROBE_HOLDS, DEFAULT_ORIGIN_PROBE_FAULT_TTL_SEC,
    DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY, DEFAULT_ORIGIN_PROBE_NEGATIVE_TTL_SEC,
    DEFAULT_ORIGIN_PROBE_TIMEOUT_MS, DEFAULT_ORIGIN_PROBE_TTL_SEC, DEFAULT_USER_AGENT,
};
pub use hash::{DeniedHashes, Hash, HashParseError, PinDiff, PinnedHashes};
pub use origin_kind::OriginKind;
pub use origin_url::{OriginUrl, parse_origin_url, redact_for_log};
pub use retry::{RetryPolicy, default_buffered_max_bytes};
