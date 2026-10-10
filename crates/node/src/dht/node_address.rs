//! Node-id → operator Ethereum address resolver (ADR 003 §Node Registry, #831).
//!
//! The node-to-node cache-miss pull path discovers upstream providers by iroh
//! [`NodeId`] (DHT `FIND_VALUE` / origin directory), but paying one requires
//! its bonded operator address: [`crate::buyer_pool::BuyerPoolService`]
//! opens the USDC pool *to* that address, and the pull's open stage
//! (`open_progressive_pull`) verifies the delivery `slash_sig`
//! recovers *to* it. This module resolves that binding from the same
//! `CapacityBond` data [`crate::dht::chain_staker_set::ChainStakerSet`] already
//! reads — `getRegisteredNodes()` returns `NodeInfo { nodeId, ethAddress, .. }` and
//! `NodeRegistered(nodeId, ethAddress, ..)` carries both — so no new contract
//! surface is needed.
//!
//! This module owns the **projection** only. Because both views come from one
//! contract, [`crate::dht::capacity_bond_registry`] does a single
//! `getRegisteredNodes` enumeration and runs a single `eth_getLogs` loop feeding
//! both, building this via `ChainNodeAddressDirectory::from_parts` (#1110).
//!
//! The binding is set at `registerNode` and cleared at `deregisterNode`; it is
//! unaffected by bond / unbonding / ejection transitions (those flip
//! `isActive`, which the *staker set* tracks separately). So the registry's
//! bindings arms follow only `NodeRegistered` (insert) and `NodeDeregistered`
//! (remove) — notably `NodeAutoEjected` deactivates without clearing a binding —
//! and the enumeration applies no `isActive` filter: an operator mid-unbonding is
//! momentarily inactive but still payable.
//!
//! # Failure model
//!
//! Bootstrap: the bindings projection is derived from page data the
//! (unconditional; fatal on a deterministic fault or once its boot retries are
//! exhausted) staker-set
//! enumeration already read, so it has no RPC of its
//! own and cannot fail independently — see `capacity_bond_registry`'s
//! §Fatality. It is simply not built when
//! `cache.node_to_node_pull_through_enabled` is off.
//!
//! Watcher RPC failure mid-run → the shared loop logs at `warn!`, backs off
//! (1s → 60s cap), and re-polls. The cursor is retained across the backoff, so
//! the next `eth_getLogs` tick re-scans `[cursor, head]` and re-applies any
//! binding event that landed during the outage — no backoff-gap drift. A missing
//! binding fails the pull *closed* — the orchestrator skips a provider it cannot
//! resolve rather than guessing an address it would then pay.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use alloy::primitives::Address;

use crate::dht::chain_projection::{ChainProjection, mutate_gauged};
use crate::dht::routing::NodeId;
use crate::metrics::Metrics;

/// Names the projection in the poison-recovery `warn!` and gauge helpers.
const LABEL: &str = "ChainNodeAddressDirectory bindings";

/// Read-only resolver from a provider's iroh [`NodeId`] to its bonded operator
/// Ethereum [`Address`]. Implementations MUST be cheap to clone (typically
/// `Arc<inner>`); the pull orchestrator holds a long-lived
/// `Arc<dyn NodeAddressResolver>` and calls [`Self::address_of`] once per
/// candidate it intends to pay.
pub trait NodeAddressResolver: Send + Sync + std::fmt::Debug {
    /// The bonded operator address for `node_id`, or `None` if the node is not
    /// currently registered. `None` means the caller MUST NOT attempt a paid
    /// pull from it — there is no address to pay or to verify the
    /// `slash_sig` against.
    fn address_of(&self, node_id: &NodeId) -> Option<Address>;
}

/// Static, in-memory [`NodeAddressResolver`] from a known map. Used by tests and
/// any caller wiring a fixed binding set without a live chain.
#[derive(Debug, Clone)]
pub struct StaticNodeAddressDirectory {
    map: Arc<HashMap<NodeId, Address>>,
}

impl StaticNodeAddressDirectory {
    /// Build from a fixed `NodeId → Address` map.
    #[must_use]
    pub fn new(map: HashMap<NodeId, Address>) -> Self {
        Self { map: Arc::new(map) }
    }
}

impl NodeAddressResolver for StaticNodeAddressDirectory {
    fn address_of(&self, node_id: &NodeId) -> Option<Address> {
        self.map.get(node_id).copied()
    }
}

/// Chain-backed [`NodeAddressResolver`]. Cheap to clone via the shared inner
/// [`Arc`]; the background loop that keeps the cache fresh is the single shared
/// multiplexed-poller task the runtime owns, so a node-restart cycle never leaks
/// chain-poll tasks.
#[derive(Debug)]
pub struct ChainNodeAddressDirectory {
    proj: ChainProjection<HashMap<NodeId, Address>>,
}

impl ChainNodeAddressDirectory {
    /// Assemble from the bindings state owned by `capacity_bond_registry`, which
    /// does the enumeration and registers the shared poller route.
    pub(super) const fn from_parts(bindings: Arc<RwLock<HashMap<NodeId, Address>>>) -> Self {
        Self {
            proj: ChainProjection::from_parts(bindings, LABEL),
        }
    }
}

impl NodeAddressResolver for ChainNodeAddressDirectory {
    fn address_of(&self, node_id: &NodeId) -> Option<Address> {
        self.proj.read(|bindings| bindings.get(node_id).copied())
    }
}

/// Insert/update `node_id → address`, republishing the size gauge only when the
/// set actually grew (a re-registration that overwrites an existing binding
/// with the same key does not change cardinality).
pub(super) fn set_binding(
    bindings: &RwLock<HashMap<NodeId, Address>>,
    metrics: &Arc<Metrics>,
    node_id: NodeId,
    address: Address,
) {
    mutate_gauged(
        bindings,
        LABEL,
        // `insert` returns the prior value: `None` means a new key (cardinality
        // grew); `Some` means an overwrite (same key, possibly rotated address)
        // that leaves the size — and thus the gauge — unchanged.
        |map| map.insert(node_id, address).is_none().then_some(map.len()),
        |size| metrics.node_address_directory_size(size),
    );
}

/// Remove `node_id`'s binding, republishing the size gauge only on a real
/// removal (removing an absent key is a no-op).
pub(super) fn remove_binding(
    bindings: &RwLock<HashMap<NodeId, Address>>,
    metrics: &Arc<Metrics>,
    node_id: &NodeId,
) {
    mutate_gauged(
        bindings,
        LABEL,
        |map| map.remove(node_id).is_some().then_some(map.len()),
        |size| metrics.node_address_directory_size(size),
    );
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests;
