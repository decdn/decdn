//! On-chain origin-directory fallback (ADR 022 §FIND\_VALUE Flow
//! "Origin discovery" and §Bootstrap "the on-chain origin directory
//! provides the deterministic fallback").
//!
//! When a DHT lookup for a `namespaceId != 0` request returns no providers, or
//! the probed holders do not cover every block of the blob, the requester reads
//! the deterministic chain-derived directory keyed on the request's namespace:
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
//!   `namespace → operators` view is fetched lazily on a cold-namespace miss
//!   and cached with a split TTL, and the `active` filter is applied from the
//!   shared `StakerSet` at lookup time rather than taken from the `nodeIdOf`
//!   tuple. A live cache hit resolves with no RPC; a miss issues one
//!   on-demand `getOrigins` call.
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
/// hold a long-lived `Arc<dyn OriginDirectory>` and invoke `lookup_origins` on
/// a lookup miss, or on a ranged pull whose probed holders do not cover every
/// block (ADR 022 §FIND\_VALUE Flow). Both are uncommon: under normal operation
/// the DHT holds an entry for every actively-serving authorized origin.
///
/// `async` because the chain-backed implementation may issue one on-demand
/// `getOrigins` RPC on a cold-namespace miss; `Empty`/`Static` resolve
/// synchronously.
#[async_trait::async_trait]
pub trait OriginDirectory: Send + Sync + std::fmt::Debug {
    /// Return the operator `NodeId`s authorised as origins for `namespace_id`,
    /// already filtered to only currently-active stakers (the `active == true`
    /// filter from `CapacityBond.nodeIdOf` per ADR 022 §FIND\_VALUE Flow "Origin
    /// discovery").
    ///
    /// Returns an empty vector when the namespace has no authorized origins —
    /// including `namespace_id == 0` (no namespace: cache/DHT-only, no origins
    /// per ADR 002 §Namespace 0).
    async fn lookup_origins(&self, namespace_id: U256) -> Vec<NodeId>;
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

#[async_trait::async_trait]
impl OriginDirectory for EmptyOriginDirectory {
    async fn lookup_origins(&self, _namespace_id: U256) -> Vec<NodeId> {
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

#[async_trait::async_trait]
impl OriginDirectory for StaticOriginDirectory {
    async fn lookup_origins(&self, namespace_id: U256) -> Vec<NodeId> {
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
mod tests;
