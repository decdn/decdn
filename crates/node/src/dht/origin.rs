//! On-chain origin-directory fallback (ADR 022 §FIND\_VALUE Flow
//! "Origin discovery" and §Bootstrap "the on-chain origin directory
//! provides the deterministic fallback").
//!
//! When a DHT lookup for a `namespaceId != 0` request returns no providers, the
//! requester falls back to the deterministic chain-derived directory keyed on
//! the request's namespace:
//!
//!   `OriginAssignment.getOrigins(namespace_id)` → `[operator...]`
//!     → `CapacityBond.nodeIdOf(operator) → (NodeId, active)`
//!     → filter `active == true`
//!
//! The bare hash carries no origin information (ADR 002 §Retrieval by
//! namespace): a request supplies the namespace its content is published under,
//! and origin authorization is a namespace-level property. `namespaceId == 0`
//! has no authorized origins, so the directory resolves nothing for it.
//!
//! This trait abstracts the directory so callers consume a single
//! `lookup_origins(namespace_id) -> Vec<NodeId>` call without caring whether the
//! implementation resolves from chain (production), from a fixed in-memory map
//! (tests), or resolves nothing at all (the fallback installed when the chain
//! contracts aren't configured).
//!
//! Three implementations exist:
//!
//! - [`crate::dht::ChainOriginDirectory`] — the production path, and the only
//!   one that can resolve anything. It yields the same candidate set the chain
//!   above specifies, but does not perform that literal call sequence: the
//!   `namespace → operators` view is fed from `OriginAssignment` events
//!   (`OriginAdded` / `OriginRemoved` / `BlacklistedOriginPruned`)
//!   and the `active` filter is applied from the shared `StakerSet` at lookup
//!   time rather than taken from the `nodeIdOf` tuple. Lookups are served from an
//!   event-fed in-memory cache and never hit RPC.
//! - [`EmptyOriginDirectory`] (this module) — resolves nothing. What the
//!   runtime installs when the chain contracts aren't configured.
//! - [`StaticOriginDirectory`] (this module) — a fixed in-memory map, for tests
//!   and any caller wiring a known origin set without a live chain. No config
//!   key populates it; it has no production call site.
//!
//! The runtime picks between them at bring-up: it constructs a
//! `ChainOriginDirectory` when `blockchain.origin_assignment_address` is set,
//! and otherwise installs an [`EmptyOriginDirectory`] (see `crate::runtime`).
//! With that fallback in place every lookup miss yields no origin candidates —
//! see [`EmptyOriginDirectory`] for what that means for each consumer.

use std::collections::HashMap;

use alloy::primitives::U256;

use crate::dht::routing::NodeId;

/// Read-only view of the on-chain origin directory, keyed by the namespace a
/// request is published under (ADR 002 §Retrieval by namespace).
///
/// Implementations MUST be cheap to clone (typically `Arc<inner>`); consumers
/// hold a long-lived `Arc<dyn OriginDirectory>` and only invoke
/// `lookup_origins` on lookup miss (an uncommon path per ADR 022 §FIND\_VALUE
/// Flow "This fallback is uncommon — under normal operation the DHT contains
/// entries for every actively-serving authorized origin").
pub trait OriginDirectory: Send + Sync + std::fmt::Debug {
    /// Return the operator `NodeId`s authorised as origins for `namespace_id`,
    /// already filtered to only currently-active stakers (the `active == true`
    /// filter from `CapacityBond.nodeIdOf` per ADR 022 §FIND\_VALUE Flow "Origin
    /// discovery").
    ///
    /// Returns an empty vector when the namespace has no authorized origins —
    /// including `namespace_id == 0` (no namespace: cache/DHT-only, no origins
    /// per ADR 002 §Namespace 0).
    fn lookup_origins(&self, namespace_id: U256) -> Vec<NodeId>;
}

/// [`OriginDirectory`] that resolves nothing.
///
/// The shape the runtime installs when the on-chain `OriginAssignment` address
/// is not configured. The chain-backed [`crate::dht::ChainOriginDirectory`] is
/// the only directory with a production population path (ADR 022 §FIND\_VALUE
/// Flow describes the fallback as the *on-chain* origin directory throughout),
/// so "no chain config" means "no origin directory" rather than "an
/// operator-supplied one".
///
/// Every consumer degrades to a hard deny on a node without a chain address:
/// the FIND\_VALUE fallback resolves nothing, so a `namespaceId != 0` DHT miss
/// reports the blob unavailable on the network.
#[derive(Debug, Default, Clone, Copy)]
pub struct EmptyOriginDirectory;

impl OriginDirectory for EmptyOriginDirectory {
    fn lookup_origins(&self, _namespace_id: U256) -> Vec<NodeId> {
        Vec::new()
    }
}

/// Static, in-memory [`OriginDirectory`] over a fixed namespace → origin-`NodeId`
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
    /// Map from namespace id to the list of authorised origin `NodeId`s.
    /// Taken as-is from the caller, so `lookup_origins` is a single
    /// `HashMap::get` clone-and-return.
    origins: HashMap<U256, Vec<NodeId>>,
}

impl StaticOriginDirectory {
    /// Build from a `namespace → origins` map.
    #[must_use]
    pub const fn new(origins: HashMap<U256, Vec<NodeId>>) -> Self {
        Self { origins }
    }
}

impl OriginDirectory for StaticOriginDirectory {
    fn lookup_origins(&self, namespace_id: U256) -> Vec<NodeId> {
        self.origins.get(&namespace_id).cloned().unwrap_or_default()
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

    fn ns(n: u64) -> U256 {
        U256::from(n)
    }
    fn nid(b: u8) -> NodeId {
        NodeId::from_bytes([b; 32])
    }

    #[test]
    fn empty_directory_returns_empty_for_every_lookup() {
        // The runtime's non-chain fallback: nothing resolves for any namespace.
        let dir = EmptyOriginDirectory;
        assert!(dir.lookup_origins(ns(0)).is_empty());
        assert!(dir.lookup_origins(ns(7)).is_empty());
    }

    #[test]
    fn static_directory_with_no_entries_resolves_nothing() {
        let dir = StaticOriginDirectory::new(HashMap::new());
        assert!(dir.lookup_origins(ns(3)).is_empty());
    }

    #[test]
    fn lookup_returns_configured_origins() {
        let mut m = HashMap::new();
        m.insert(ns(1), vec![nid(0xA), nid(0xB)]);
        m.insert(ns(2), vec![nid(0xC)]);
        let dir = StaticOriginDirectory::new(m);
        assert_eq!(dir.lookup_origins(ns(1)), vec![nid(0xA), nid(0xB)]);
        assert_eq!(dir.lookup_origins(ns(2)), vec![nid(0xC)]);
        // Namespace 0 (no namespace) and any unknown id fall through to an empty
        // vec — the caller distinguishes "directory has nothing for this
        // namespace" from "directory not initialised" via the empty result, NOT
        // via an Option.
        assert!(dir.lookup_origins(ns(0)).is_empty());
        assert!(dir.lookup_origins(ns(3)).is_empty());
    }
}
