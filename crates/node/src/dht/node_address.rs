//! Node-id → operator Ethereum address resolver (ADR 003 §Node Registry, #831).
//!
//! The node-to-node cache-miss pull path discovers upstream providers by iroh
//! [`NodeId`] (DHT `FIND_VALUE` / origin directory), but paying one requires
//! its bonded operator address: [`crate::buyer_channel::BuyerPoolService`]
//! opens the USDC pool *to* that address, and
//! `stream_fetch` verifies the delivery `slash_sig`
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
//! Bootstrap: the bindings projection is derived from page data the (fatal,
//! unconditional) staker-set enumeration already read, so it has no RPC of its
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
    /// pull from it — there is no address to open a channel to or to verify the
    /// `slash_sig` against.
    fn address_of(&self, node_id: &NodeId) -> Option<Address>;

    /// Reverse lookup: a registered [`NodeId`] currently bound to `address`, or
    /// `None` if no registered node binds it (deregistered / never seen).
    ///
    /// The buyer-channel reconcile (#972) keys channels by the provider's
    /// operator *address* but must dial the provider by `NodeId` to request a
    /// cooperative-close waiver. An operator may run several nodes under one
    /// address; any is dialable for this purpose — they share the operator key
    /// that signs the waiver — so the first match is returned. `None` is the
    /// "unreachable / gone" signal: the caller leaves the channel for the
    /// expiry-reclaim sweep rather than dialing.
    fn node_id_for(&self, address: &Address) -> Option<NodeId>;
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

    fn node_id_for(&self, address: &Address) -> Option<NodeId> {
        self.map
            .iter()
            .find_map(|(node_id, addr)| (addr == address).then_some(*node_id))
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

    fn node_id_for(&self, address: &Address) -> Option<NodeId> {
        // O(n) scan of the binding set — the reconcile sweep calls this hourly
        // for a handful of channels, so a reverse index isn't worth maintaining.
        // Add a reverse map only if a node ever tracks thousands of buyer
        // channels.
        self.proj.read(|bindings| {
            bindings
                .iter()
                .find_map(|(node_id, addr)| (addr == address).then_some(*node_id))
        })
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
mod tests {
    use super::*;

    fn nid(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 32])
    }

    fn addr(byte: u8) -> Address {
        Address::repeat_byte(byte)
    }

    fn fresh() -> (Arc<RwLock<HashMap<NodeId, Address>>>, Arc<Metrics>) {
        (
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(Metrics::new()),
        )
    }

    #[test]
    fn static_directory_resolves_known_and_misses_unknown() {
        let mut m = HashMap::new();
        m.insert(nid(1), addr(0xAA));
        let dir = StaticNodeAddressDirectory::new(m);
        assert_eq!(dir.address_of(&nid(1)), Some(addr(0xAA)));
        assert_eq!(dir.address_of(&nid(2)), None);
    }

    #[test]
    fn static_directory_reverse_resolves_address_and_misses_unknown() {
        let mut m = HashMap::new();
        m.insert(nid(1), addr(0xAA));
        let dir = StaticNodeAddressDirectory::new(m);
        assert_eq!(dir.node_id_for(&addr(0xAA)), Some(nid(1)));
        // No node binds this address → unreachable/gone.
        assert_eq!(dir.node_id_for(&addr(0xBB)), None);
    }

    /// `set_binding` inserts and surfaces the address; the size gauge tracks the
    /// growing set, and a same-key overwrite keeps cardinality (and the gauge)
    /// stable while updating the bound address.
    #[test]
    fn set_binding_inserts_and_overwrites() {
        let (bindings, metrics) = fresh();
        set_binding(&bindings, &metrics, nid(1), addr(0xAA));
        assert_eq!(
            bindings.read().unwrap().get(&nid(1)).copied(),
            Some(addr(0xAA))
        );
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_node_address_directory_size 1"),
            "gauge should report 1 after one insert:\n{text}"
        );

        // Re-registration of the same node with a rotated address overwrites
        // without changing cardinality.
        set_binding(&bindings, &metrics, nid(1), addr(0xBB));
        assert_eq!(
            bindings.read().unwrap().get(&nid(1)).copied(),
            Some(addr(0xBB))
        );
        assert_eq!(bindings.read().unwrap().len(), 1);
    }

    /// `remove_binding` drops a present key (and shrinks the gauge) but is a
    /// no-op for an absent one.
    #[test]
    fn remove_binding_present_and_absent() {
        let (bindings, metrics) = fresh();
        set_binding(&bindings, &metrics, nid(1), addr(0xAA));
        set_binding(&bindings, &metrics, nid(2), addr(0xBB));
        remove_binding(&bindings, &metrics, &nid(1));
        assert!(bindings.read().unwrap().get(&nid(1)).is_none());
        assert_eq!(bindings.read().unwrap().len(), 1);
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_node_address_directory_size 1"),
            "gauge should report 1 after removing one of two:\n{text}"
        );

        // Removing an absent key changes nothing.
        remove_binding(&bindings, &metrics, &nid(0xFF));
        assert_eq!(bindings.read().unwrap().len(), 1);
    }
}
