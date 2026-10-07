//! Cache engine for deCDN.
//!
//! Wraps `iroh-blobs` with origin pull-through logic. The buffered entry
//! points ([`engine::CacheEngine::get`] / `populate`) fetch from a configured
//! [`Origin`] backend on a cache miss, verify the BLAKE3 hash, and insert the
//! bytes before returning them to the caller. The streaming entry points do
//! not buffer whole-blob first: [`engine::CacheEngine::origin_range_wire`]
//! streams one verified sub-range per draw, window by window, for the node's
//! two-leg serve-miss spine. Node-to-node pull-through is one
//! such backend — the
//! `node` crate supplies an [`Origin`] (`NodeOrigin`, #831) that pays a peer
//! over `cdn/client/v1` — so this crate needs no knowledge of the paid path.
//!
//! Leaf crate per `adr/appendix-poc-production-seams.md` — no `#[cfg(...)]`
//! mode branching or feature flags here. The `node` crate's wiring layer
//! selects which origin backend to construct.

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
/// The blob-store hash. `decdn_cache::Hash` continues to mean
/// `iroh_blobs::Hash` (the BLAKE3 digest the iroh-blobs store keys on)
/// so every existing `decdn_cache::Hash` path in `decdn-node` and the
/// cache crate is unchanged. The *config-vocabulary* hash that crosses
/// operator-facing config/admin surfaces is the distinct
/// [`decdn_config_types::Hash`]; [`to_store_hash`]/[`from_store_hash`]
/// convert between the two (both are the same 32 bytes — issue #578).
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
// storage stack — see issue #578 and `adr/appendix-binaries.md`.
// Re-exported here so existing `decdn_cache::*` paths in `decdn-node`
// keep resolving unchanged. `decdn_config_types::Hash` is intentionally
// NOT re-exported as `decdn_cache::Hash` — that name stays the store
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
/// byte-for-byte reinterpretation (issue #578). Used at the node's
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
