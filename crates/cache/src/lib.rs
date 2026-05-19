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

pub use engine::{CacheEngine, CacheStats, EvictionCandidates, EvictionPreview};
pub use error::{CacheError, CacheResult, OriginError, OriginPullError, SupportedEncoding};
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
    FilesystemOrigin, HttpOrigin, Origin, OriginFetch, S3Credentials, S3Origin, S3OriginConfig,
};
pub use probe_hold::{
    PROBE_HOLD_DURATION, PROBE_HOLD_MARGIN, PROBE_SLASH_WINDOW, ProbeHoldOutcome,
};

// Config-vocabulary types live in the `decdn-config-types` leaf crate
// (no iroh-blobs / no AWS SDK) so the publisher CLI doesn't link the
// storage stack — see issue #578 and `adr/appendix-binaries.md`.
// Re-exported here so existing `decdn_cache::*` paths in `decdn-node`
// keep resolving unchanged. `decdn_config_types::Hash` is intentionally
// NOT re-exported as `decdn_cache::Hash` — that name stays the store
// hash above.
pub use decdn_config_types::{
    DEFAULT_MAX_PROBE_HOLDS, DEFAULT_USER_AGENT, DecompressMode, HashParseError, OriginKind,
    OriginUrl, PinDiff, PinnedHashes, RetryPolicy, parse_origin_url, redact_for_log,
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
mod hash_bridge_tests {
    //! The load-bearing #578 invariant: the `decdn-config-types` leaf
    //! `Hash` (its hand-rolled hex codec + serde) is byte- and
    //! hex-identical to `iroh_blobs::Hash`. `decdn-cache` is the only
    //! crate that links *both* types, so this contract is pinned here.
    //! Without this, a future change to the leaf hex codec would
    //! silently make every operator's `cache.pinned_hashes` config
    //! entry decode to the wrong 32 bytes — the pinned blob would not
    //! be protected — with no other test failing.

    use std::str::FromStr;

    use super::{Hash as StoreHash, from_store_hash, to_store_hash};

    /// Known-answer vector: BLAKE3 of the empty input. If the leaf hex
    /// codec ever drifts (nibble order, casing, base32, a `0x` prefix),
    /// this fails — exactly the operator-facing wire regression #578's
    /// `Hash` extraction must never introduce.
    const BLAKE3_EMPTY: &str = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262";

    #[test]
    fn leaf_hash_hex_is_byte_identical_to_iroh_blobs_for_known_vector() {
        let store = StoreHash::new(b"");
        assert_eq!(
            store.to_string(),
            BLAKE3_EMPTY,
            "sanity: iroh_blobs hex of BLAKE3(\"\")"
        );
        let leaf = from_store_hash(store);
        // Leaf hex == iroh-blobs hex, and == the known vector.
        assert_eq!(leaf.to_hex(), BLAKE3_EMPTY);
        assert_eq!(leaf.to_hex(), store.to_string());
        // iroh-blobs can parse what the leaf produced, back to the
        // same store hash (operator config string → leaf → store).
        let reparsed = StoreHash::from_str(&leaf.to_hex()).expect("iroh parses leaf hex");
        assert_eq!(reparsed, store);
        // And the leaf parses iroh's hex form to the same bytes
        // (admin/JSON-RPC string → leaf → store match).
        let leaf_from_iroh_hex =
            decdn_config_types::Hash::from_str(&store.to_string()).expect("leaf parses iroh hex");
        assert_eq!(to_store_hash(leaf_from_iroh_hex), store);
    }

    #[test]
    fn store_leaf_round_trip_is_lossless_both_directions() {
        for payload in [b"".as_slice(), b"pinned blob", &[0xff; 64]] {
            let store = StoreHash::new(payload);
            assert_eq!(
                to_store_hash(from_store_hash(store)),
                store,
                "store → leaf → store must be identity"
            );
            let leaf = from_store_hash(store);
            assert_eq!(
                from_store_hash(to_store_hash(leaf)),
                leaf,
                "leaf → store → leaf must be identity"
            );
        }
    }
}
