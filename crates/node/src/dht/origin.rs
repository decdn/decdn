//! On-chain origin-directory fallback (ADR 022 §FIND\_VALUE Flow
//! "Origin discovery" and §Bootstrap "the on-chain origin directory
//! provides the deterministic fallback").
//!
//! When a DHT lookup for hash H returns no providers, the requester
//! falls back to the deterministic chain-derived directory:
//!
//!   `PublisherRegistry.namespaceOf(H)`
//!     → `OriginAssignment.getOrigins(namespace_id)`
//!     → `CapacityBond.nodeIdOf(operator) → (NodeId, active)`
//!     → filter `active == true`
//!
//! This trait abstracts the directory so the iterative-lookup module
//! consumes a single `lookup_origins(hash) -> Vec<NodeId>` call without
//! caring whether the implementation reads from chain (production), an
//! operator-supplied file (initial-network bootstrap), or a mock (tests).
//!
//! The chain-backed `ChainOriginDirectory` lands with the on-chain
//! origin-directory follow-up; this module ships only the trait + a
//! `ConfigOriginDirectory` that reads from operator-supplied TOML for
//! testnet bring-up.

use std::collections::HashMap;

use crate::dht::routing::NodeId;

/// 32-byte content hash. Re-exported from [`crate::dht::records::Hash`]
/// so origin-directory call sites don't need to import the record store
/// crate path.
pub use crate::dht::records::Hash;

/// Read-only view of the on-chain origin directory.
///
/// Implementations MUST be cheap to clone (typically `Arc<inner>`); the
/// iterative-lookup module holds a long-lived `Arc<dyn OriginDirectory>`
/// and only invokes `lookup_origins` on lookup miss (an uncommon path
/// per ADR 022 §FIND\_VALUE Flow "This fallback is uncommon — under
/// normal operation the DHT contains entries for every actively-serving
/// authorized origin").
pub trait OriginDirectory: Send + Sync + std::fmt::Debug {
    /// Return the operator `NodeId`s authorised as origins for `hash`,
    /// already filtered to only currently-active stakers (the
    /// `active == true` filter from `CapacityBond.nodeIdOf` per ADR
    /// 022 §FIND\_VALUE Flow "Origin discovery").
    ///
    /// Returns an empty vector when no namespace is registered for the
    /// hash AND no default-open allow-list entry applies — i.e. the
    /// hash is truly unclaimed and unfindable via the directory.
    fn lookup_origins(&self, hash: &Hash) -> Vec<NodeId>;

    /// Whether at least one authorised origin exists for `hash`, without
    /// materialising the candidate list. The prefetch authorized-origin
    /// gate (ADR 022 §Prefetch Decision) calls this on the threshold-cross
    /// path purely to test emptiness; the default delegates to
    /// `lookup_origins`, but `Vec`-backed implementations SHOULD override
    /// to avoid the clone.
    fn has_origin(&self, hash: &Hash) -> bool {
        !self.lookup_origins(hash).is_empty()
    }
}

/// File-config-driven [`OriginDirectory`] implementation.
///
/// Used for testnet bring-up where the operator hand-curates the
/// hash → origin-NodeId map in `decdn-node.toml` before the on-chain
/// `PublisherRegistry` / `OriginAssignment` contracts ship. The
/// chain-backed `ChainOriginDirectory` (issue tracked in the on-chain
/// origin-directory follow-up) reads the same shape from RPC and
/// substitutes via the same trait.
#[derive(Debug)]
pub struct ConfigOriginDirectory {
    /// Map from BLAKE3 hash to the list of authorised origin `NodeId`s.
    /// Pre-filtered at construction (the resolver drops malformed
    /// entries with a config-level error), so `lookup_origins` is a
    /// single `HashMap::get` clone-and-return.
    origins: HashMap<Hash, Vec<NodeId>>,
}

impl ConfigOriginDirectory {
    /// Build from a parsed `hash → origins` map. The runtime constructs
    /// this from the resolved `dht.static_origins` field.
    #[must_use]
    pub const fn new(origins: HashMap<Hash, Vec<NodeId>>) -> Self {
        Self { origins }
    }

    /// Empty directory — useful for tests and as the default when no
    /// `dht.static_origins` is configured. An empty directory means
    /// every DHT lookup that returns no providers also gets no
    /// origin-fallback candidates, and the blob is reported as
    /// "unavailable on the network" to the caller.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            origins: HashMap::new(),
        }
    }

    /// Number of distinct hashes in the directory.
    #[must_use]
    pub fn len(&self) -> usize {
        self.origins.len()
    }

    /// Whether the directory contains zero hash mappings.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.origins.is_empty()
    }
}

impl OriginDirectory for ConfigOriginDirectory {
    fn lookup_origins(&self, hash: &Hash) -> Vec<NodeId> {
        self.origins.get(hash).cloned().unwrap_or_default()
    }

    fn has_origin(&self, hash: &Hash) -> bool {
        self.origins.get(hash).is_some_and(|o| !o.is_empty())
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;

    fn h(b: u8) -> Hash {
        Hash::from_bytes([b; 32])
    }
    fn nid(b: u8) -> NodeId {
        NodeId::from_bytes([b; 32])
    }

    #[test]
    fn empty_directory_returns_empty_for_every_lookup() {
        let dir = ConfigOriginDirectory::empty();
        assert!(dir.is_empty());
        assert_eq!(dir.len(), 0);
        assert!(dir.lookup_origins(&h(0)).is_empty());
        assert!(dir.lookup_origins(&h(0xFF)).is_empty());
    }

    #[test]
    fn lookup_returns_configured_origins() {
        let mut m = HashMap::new();
        m.insert(h(1), vec![nid(0xA), nid(0xB)]);
        m.insert(h(2), vec![nid(0xC)]);
        let dir = ConfigOriginDirectory::new(m);
        assert_eq!(dir.len(), 2);
        assert_eq!(dir.lookup_origins(&h(1)), vec![nid(0xA), nid(0xB)]);
        assert_eq!(dir.lookup_origins(&h(2)), vec![nid(0xC)]);
        // Unknown hash falls through to an empty vec — the caller
        // distinguishes "directory has nothing for this hash" from
        // "directory not initialised" via the empty result, NOT via
        // an Option.
        assert!(dir.lookup_origins(&h(3)).is_empty());
    }

    #[test]
    fn has_origin_matches_lookup_emptiness_without_cloning() {
        // The clone-free `has_origin` override must agree with
        // `!lookup_origins(..).is_empty()` for the prefetch gate: present,
        // absent, and the edge case of a hash mapped to an empty vec.
        let mut m = HashMap::new();
        m.insert(h(1), vec![nid(0xA)]);
        m.insert(h(2), Vec::new()); // present key, no origins
        let dir = ConfigOriginDirectory::new(m);
        assert!(dir.has_origin(&h(1)));
        assert!(!dir.has_origin(&h(2)));
        assert!(!dir.has_origin(&h(3)));
        for b in [1u8, 2, 3] {
            assert_eq!(dir.has_origin(&h(b)), !dir.lookup_origins(&h(b)).is_empty());
        }
    }
}
