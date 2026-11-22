//! Node-id → operator Ethereum address resolver (ADR 003 §Node Registry, #831).
//!
//! The node-to-node cache-miss pull path discovers upstream providers by iroh
//! [`NodeId`] (DHT `FIND_VALUE` / origin directory), but paying one requires
//! its bonded operator address: [`crate::buyer_pool::BuyerPoolService`]
//! opens the USDC pool *to* that address, and the pull's open stage
//! (`open_progressive_pull`) verifies the delivery `operator_sig`
//! recovers *to* it. This module resolves that binding from the same
//! `CapacityBond` data [`crate::dht::chain_staker_set::ChainStakerSet`] already
//! reads — `getRegisteredNodes()` returns `NodeInfo { nodeId, ethAddress, .. }`
//! — so no new contract surface is needed.
//!
//! This module owns the **projection** only. Because both views come from one
//! contract, [`crate::dht::capacity_bond_registry`] does a single
//! `getRegisteredNodes` snapshot feeding both, building this via
//! `ChainNodeAddressDirectory::from_parts` (#1110).
//!
//! The binding is set at `registerNode` and cleared at `deregisterNode`; it is
//! unaffected by bond / unbond / ejection transitions (those flip
//! `isActive`, which the *staker set* tracks separately). The snapshot applies
//! no `isActive` filter: an operator below its bond floor is inactive but still
//! payable.
//!
//! # Failure model
//!
//! The bindings projection is derived from page data the registry snapshot
//! already read, so it has no RPC of its own and cannot fail independently. It
//! is simply not built when `cache.node_to_node_pull_through_enabled` is off. A
//! failed re-read keeps the previous bindings. A missing binding fails the pull
//! *closed* — the orchestrator skips a provider it cannot resolve rather than
//! guessing an address it would then pay.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use alloy::primitives::Address;

use crate::dht::chain_projection::ChainProjection;
use crate::dht::routing::NodeId;

/// Names the projection in the poison-recovery `warn!`.
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
    /// `operator_sig` against.
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

/// Chain-backed [`NodeAddressResolver`] over the registry snapshot's bindings.
#[derive(Debug)]
pub struct ChainNodeAddressDirectory {
    proj: ChainProjection<HashMap<NodeId, Address>>,
}

impl ChainNodeAddressDirectory {
    /// Assemble from the bindings state owned by `capacity_bond_registry`.
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests;
