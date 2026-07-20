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
//! caring whether the implementation resolves from chain (production),
//! from a fixed in-memory map (tests), or resolves nothing at all (the
//! fallback installed when the chain contracts aren't configured).
//!
//! Three implementations exist:
//!
//! - [`crate::dht::ChainOriginDirectory`] — the production path, and the only
//!   one that can resolve anything. It yields the same candidate set the chain
//!   above specifies, but does not perform that literal call sequence: the
//!   `hash → namespace` view is replayed from `ContentClaimed` logs rather than
//!   read via `namespaceOf`, and the `active` filter is applied from the shared
//!   `StakerSet` at lookup time rather than taken from the `nodeIdOf` tuple.
//!   Lookups are served from an event-fed in-memory cache and never hit RPC.
//! - [`EmptyOriginDirectory`] (this module) — resolves nothing. What the
//!   runtime installs when the chain contracts aren't configured.
//! - [`StaticOriginDirectory`] (this module) — a fixed in-memory map, for tests
//!   and any caller wiring a known origin set without a live chain. No config
//!   key populates it; it has no production call site.
//!
//! The runtime picks between them at bring-up: it constructs a
//! `ChainOriginDirectory` when **both** `blockchain.origin_assignment_address`
//! and `blockchain.publisher_registry_address` are set, and otherwise installs
//! an [`EmptyOriginDirectory`] (see `crate::runtime`). With that fallback in
//! place every lookup miss yields no origin candidates — see
//! [`EmptyOriginDirectory`] for what that means for each consumer.

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

/// [`OriginDirectory`] that resolves nothing.
///
/// The shape the runtime installs when the on-chain `PublisherRegistry` /
/// `OriginAssignment` addresses are not both configured. The chain-backed
/// [`crate::dht::ChainOriginDirectory`] is the only directory with a
/// production population path (ADR 022 §FIND\_VALUE Flow describes the
/// fallback as the *on-chain* origin directory throughout), so "no chain
/// config" means "no origin directory" rather than "an operator-supplied one".
///
/// Every consumer degrades to a hard deny, and in two cases that deny is
/// indistinguishable from ordinary operation — worth knowing before enabling
/// an authorized-origin gate on a node without chain addresses:
///
/// - the FIND\_VALUE fallback resolves nothing, so a DHT miss reports the blob
///   unavailable on the network;
/// - the prefetch gate (`prefetch.require_authorized_origin`) skips **every**
///   hash as `Unauthorized`, silently disabling prefetch;
/// - the pull-through gate (`cache.pull_through_require_authorized_origin`)
///   refuses **every** cache miss with a wire `NotFound`, which is
///   indistinguishable from a plain miss.
#[derive(Debug, Default, Clone, Copy)]
pub struct EmptyOriginDirectory;

impl OriginDirectory for EmptyOriginDirectory {
    fn lookup_origins(&self, _hash: &Hash) -> Vec<NodeId> {
        Vec::new()
    }

    fn has_origin(&self, _hash: &Hash) -> bool {
        false
    }
}

/// Static, in-memory [`OriginDirectory`] over a fixed hash → origin-`NodeId`
/// map. Used by tests and any caller wiring a known origin set without a live
/// chain.
///
/// No config key populates this map and the runtime never constructs one — the
/// chain-backed [`crate::dht::ChainOriginDirectory`] is the production path,
/// and [`EmptyOriginDirectory`] is what stands in when it is unconfigured.
/// Mirrors [`crate::dht::StaticNodeAddressDirectory`], the same `Chain*` /
/// `Static*` pairing one module over.
#[derive(Debug)]
pub struct StaticOriginDirectory {
    /// Map from BLAKE3 hash to the list of authorised origin `NodeId`s.
    /// Taken as-is from the caller, so `lookup_origins` is a single
    /// `HashMap::get` clone-and-return.
    origins: HashMap<Hash, Vec<NodeId>>,
}

impl StaticOriginDirectory {
    /// Build from a `hash → origins` map.
    #[must_use]
    pub const fn new(origins: HashMap<Hash, Vec<NodeId>>) -> Self {
        Self { origins }
    }
}

impl OriginDirectory for StaticOriginDirectory {
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
        // The runtime's non-chain fallback: nothing resolves, and `has_origin`
        // is false for every hash, so each consuming gate hard-denies.
        let dir = EmptyOriginDirectory;
        assert!(dir.lookup_origins(&h(0)).is_empty());
        assert!(dir.lookup_origins(&h(0xFF)).is_empty());
        assert!(!dir.has_origin(&h(0)));
        assert!(!dir.has_origin(&h(0xFF)));
    }

    #[test]
    fn static_directory_with_no_entries_resolves_nothing() {
        let dir = StaticOriginDirectory::new(HashMap::new());
        assert!(dir.lookup_origins(&h(0)).is_empty());
        assert!(!dir.has_origin(&h(0)));
    }

    #[test]
    fn lookup_returns_configured_origins() {
        let mut m = HashMap::new();
        m.insert(h(1), vec![nid(0xA), nid(0xB)]);
        m.insert(h(2), vec![nid(0xC)]);
        let dir = StaticOriginDirectory::new(m);
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
        let dir = StaticOriginDirectory::new(m);
        assert!(dir.has_origin(&h(1)));
        assert!(!dir.has_origin(&h(2)));
        assert!(!dir.has_origin(&h(3)));
        for b in [1u8, 2, 3] {
            assert_eq!(dir.has_origin(&h(b)), !dir.lookup_origins(&h(b)).is_empty());
        }
    }
}
