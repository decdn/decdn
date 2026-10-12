//! The deCDN node's cache engine: an `iroh-blobs` store with origin
//! pull-through, range admission, and pluggable admission and eviction
//! policies.
//!
//! [`CacheEngine`] owns the store. On a cache miss it pulls from a configured
//! [`Origin`] and verifies the bytes against their BLAKE3 hash before it
//! stores or returns them. Pinned blobs, probe holds, and the deny-sets are
//! enforced by the engine itself, whatever policy is plugged in.
//!
//! # Entry points
//!
//! | You want to | Use |
//! |---|---|
//! | Open a store | [`CacheEngine::open`] and its variants |
//! | Read a whole blob, pulling it from the origin on a miss | [`CacheEngine::get`], [`CacheEngine::populate`] |
//! | Stream a verified sub-range from the origin, one window per draw | [`CacheEngine::origin_range_wire`] |
//! | Export a stored range as the bao wire encoding | [`CacheEngine::export_bao_range_stream`] |
//! | Admit a verified bao stream into the store | [`CacheEngine::admit_bao_stream`] |
//! | Share one in-progress fill between several readers | [`FillRegistry`], [`FillSession`] |
//! | Pick an origin backend | [`FilesystemOrigin`], [`HttpOrigin`], [`S3Origin`], or your own [`Origin`] |
//! | Choose what to admit and what to evict | [`AdmissionPolicy`], [`EvictionPolicy`] ([ADR 040]) |
//! | Bound a failing origin | [`OriginBreaker`], [`RetryPolicy`] |
//!
//! # Seams
//!
//! This crate has no payment logic, so it runs on its own for local
//! development and tests. A paid node-to-node pull is one more [`Origin`]: the
//! node supplies an origin that pays a peer over `cdn/client/v1`. The crate has
//! no mode branching and no feature flags. The node's wiring layer picks the
//! backends ([PoC/production seams]).
//!
//! [ADR 040]: https://github.com/decdn/decdn/blob/main/adr/040-cache-policy.md
//! [PoC/production seams]: https://github.com/decdn/decdn/blob/main/adr/appendix-poc-production-seams.md

pub mod circuit_breaker;
pub mod engine;
pub mod error;
pub mod fill_session;
pub mod metrics;
pub mod origin;
pub mod origin_probe;
pub mod origin_range;
mod outboard_cache;
pub mod policy;
pub mod probe_hold;
pub mod range_pull;
pub mod ranged_store;
pub mod retry;
pub mod serve_store;

pub use circuit_breaker::{
    Admission, BreakerState, Clock, ManualClock, OriginBreaker, OriginOutcome, SystemClock,
};
/// Bao chunk-group granularity (16 KiB, ADR 038), re-exported because it is a
/// *billing-visible* property of [`CacheEngine::export_bao_range_stream`]: the
/// export snaps a requested range out to the enclosing group boundaries and the
/// serve path does not trim back, so the payer is charged for the aligned
/// superset. Any caller pricing a range before serving it must align first or it
/// under-reserves by up to two groups.
pub use decdn_bao_range::CHUNK_GROUP_BYTES;
pub use engine::{
    CacheEngine, EvictionCandidates, EvictionPreview, OriginHeldReport, OriginPresence,
    PresentRanges, ServeAudit,
};
pub use error::{
    CacheError, CacheResult, ErrorChain, OriginError, OriginPullError, SupportedEncoding,
};
pub use fill_session::{
    Demand, DemandSlot, DownstreamWatch, FillClaim, FillError, FillRegistry, FillSession, Frontier,
    HashOutboard, ObserverLease, SessionOutboardReader,
};
/// The blob-store hash: `decdn_cache::Hash` is `iroh_blobs::Hash`, the BLAKE3
/// digest the iroh-blobs store keys on. The *config-vocabulary* hash that crosses
/// operator-facing config/admin surfaces is the distinct
/// [`decdn_config_types::Hash`]; [`to_store_hash`]/[`from_store_hash`]
/// convert between the two (both are the same 32 bytes).
pub use iroh_blobs::Hash;
pub use metrics::CacheMetrics;
pub use origin::{
    FilesystemOrigin, HttpOrigin, Origin, OriginFetch, OriginRangeFetch, OriginRangeRequest,
    OutboardFetch, S3Credentials, S3Origin, S3OriginConfig,
};
pub use origin_range::{MAX_CONCURRENT_RANGE_PULLS, OriginRangeWire, RANGE_PULL_WINDOW_BYTES};
pub use policy::{
    AdmissionContext, AdmissionDecision, AdmissionPolicy, AlwaysAdmit, EvictionContext,
    EvictionPlan, EvictionPolicy, FrequencyEstimator, LruEviction, Segment,
};
pub use probe_hold::{
    PROBE_HOLD_DURATION, PROBE_HOLD_MARGIN, PROBE_SLASH_WINDOW, ProbeHoldOutcome,
};
pub use ranged_store::NodeRangedStore;
pub use serve_store::{EncodeStream, PresentRangeWatch, ServeStore};

// Config-vocabulary types live in the `decdn-config-types` leaf crate
// (no iroh-blobs / no AWS SDK) so the publisher CLI doesn't link the
// storage stack (see `adr/appendix-binaries.md`). They are re-exported here so the
// node names its cache vocabulary from one crate. `decdn_config_types::Hash`
// is NOT re-exported as `decdn_cache::Hash` — that name stays the store
// hash above.
/// The config-vocabulary hash type (`decdn_config_types::Hash`), re-exported so
/// callers can build a [`DeniedHashes`] / [`PinnedHashes`] without taking a
/// direct dependency on the leaf crate. Distinct from [`struct@Hash`], which is
/// the iroh-blobs store hash — see the `hash_bridge` conversions.
pub use decdn_config_types::Hash as LeafHash;
pub use decdn_config_types::{
    Bytes, CircuitBreakerPolicy, DEFAULT_MAX_PROBE_HOLDS, DEFAULT_USER_AGENT, DecompressMode,
    DeniedHashes, HashParseError, OriginKind, OriginUrl, PinDiff, PinnedHashes, RetryPolicy,
    parse_origin_url, redact_for_log,
};

/// Convert a config-vocabulary [`decdn_config_types::Hash`] into the
/// blob-store [`struct@Hash`]. Both are 32-byte BLAKE3 digests; this is a pure
/// byte-for-byte reinterpretation. Used at the node's
/// admin boundary where `parse_hash_arg` yields the config-vocabulary
/// hash that must then index the store.
#[must_use]
pub const fn to_store_hash(h: decdn_config_types::Hash) -> Hash {
    Hash::from_bytes(h.to_bytes())
}

/// Convert a blob-store [`struct@Hash`] into a config-vocabulary
/// [`decdn_config_types::Hash`].
#[must_use]
#[allow(clippy::missing_const_for_fn)] // iroh_blobs::Hash::as_bytes is not const
pub fn from_store_hash(h: Hash) -> decdn_config_types::Hash {
    decdn_config_types::Hash::from_bytes(*h.as_bytes())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod hash_bridge_tests;
