//! Node-id → operator Ethereum address resolver (ADR 003 §Node Registry, #831).
//!
//! The node-to-node cache-miss pull path discovers upstream providers by iroh
//! [`NodeId`] (DHT `FIND_VALUE` / origin directory), but paying one requires
//! its bonded operator address: [`crate::buyer_channel::BuyerChannelService`]
//! opens the USDC channel *to* that address, and
//! [`crate::client_requester::stream_fetch`] verifies the delivery `slash_sig`
//! recovers *to* it. This module resolves that binding from the same
//! `CapacityBond` data [`crate::dht::chain_staker_set::ChainStakerSet`] already
//! reads — `getActiveNodes()` returns `NodeInfo { nodeId, ethAddress, .. }` and
//! `NodeRegistered(nodeId, ethAddress, ..)` carries both — so no new contract
//! surface is needed.
//!
//! The binding is set at `registerNode` and cleared at `deregisterNode`; it is
//! unaffected by bond / unbonding / ejection transitions (those flip
//! `isActive`, which the *staker set* tracks separately). So unlike
//! `ChainStakerSet` this directory follows only `NodeRegistered` (insert) and
//! `NodeDeregistered` (remove), applies no `isActive` filter, and never issues a
//! follow-up `nodeIdOf` RPC — there is no operator-indexed event to resolve.
//!
//! # Failure model
//!
//! Bootstrap RPC failure → the caller decides (the runtime treats node-to-node
//! pull-through as opportunistic and non-fatal, so it logs and disables the
//! buyer path rather than aborting). Watcher RPC failure mid-run → log at
//! `warn!`, back off (1s → 60s cap), and re-establish filters. The same drift
//! window `ChainStakerSet` documents applies: a registration whose event
//! arrives while filters are down is missed until the next event for that node
//! (a `getActiveNodes` resync is a follow-up). A missing binding fails the pull
//! *closed* — the orchestrator skips a provider it cannot resolve rather than
//! guessing an address it would then pay.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain_events::resumable_watcher::{
    self, CursorPolicy, LogSink, WatcherConfig, WatcherHook,
};
use crate::dht::routing::NodeId;
use crate::metrics::Metrics;
use crate::payment_settlement::MAX_BACKFILL_BLOCK_SPAN;
use decdn_incentive::capacity_bond::CapacityBond;

/// Page size for the initial paginated `getActiveNodes` read (matches
/// [`crate::dht::chain_staker_set`]).
const PAGE_SIZE: u64 = 100;

/// Backoff between watcher restart attempts after the event stream errors.
const WATCHER_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Upper bound for the watcher restart backoff.
const WATCHER_MAX_BACKOFF: Duration = Duration::from_mins(1);

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
/// [`Arc`]; the background watcher is owned via a private `AbortOnDrop` so a
/// node-restart cycle never leaks chain-poll tasks.
#[derive(Debug)]
pub struct ChainNodeAddressDirectory {
    bindings: Arc<RwLock<HashMap<NodeId, Address>>>,
    _watcher: AbortOnDrop,
}

impl ChainNodeAddressDirectory {
    /// Initial bootstrap: paginate `getActiveNodes` capturing every
    /// `(nodeId, ethAddress)` binding, then spawn the background event watcher.
    /// Returns once the cache is populated and the watcher is running.
    pub async fn bootstrap<P>(
        provider: P,
        registry_addr: Address,
        event_poll_interval: Duration,
        metrics: Arc<Metrics>,
    ) -> Result<Self>
    where
        P: Provider + Clone + 'static,
    {
        let registry = CapacityBond::new(registry_addr, provider.clone());
        let initial = bootstrap_bindings(&registry).await.with_context(|| {
            format!("paginated getActiveNodes from CapacityBond at {registry_addr}")
        })?;
        info!(
            binding_count = initial.len(),
            %registry_addr,
            "ChainNodeAddressDirectory bootstrap from CapacityBond complete"
        );
        metrics.node_address_directory_size(initial.len());

        let bindings = Arc::new(RwLock::new(initial));
        // Authoritative bindings came from `getActiveNodes` enumeration above; the
        // watcher only live-tails binding events from head on the getLogs poller
        // (#1092/#1106), no historical backfill and no persisted cursor.
        let sink = NodeAddressSink {
            bindings: Arc::clone(&bindings),
            metrics: Arc::clone(&metrics),
        };
        let cfg = WatcherConfig {
            filter: Filter::new().address(registry_addr).event_signature(vec![
                CapacityBond::NodeRegistered::SIGNATURE_HASH,
                CapacityBond::NodeDeregistered::SIGNATURE_HASH,
            ]),
            from_block: 0,
            poll_interval: event_poll_interval,
            confirmations: 0,
            reorg_margin: 0,
            // Live-from-head, but still chunk `[cursor, head]` so a long lag
            // (RPC outage / rate-limit) recovers in bounded windows instead of one
            // range-limit-tripping `eth_getLogs`.
            max_backfill_span: MAX_BACKFILL_BLOCK_SPAN,
            cursor: CursorPolicy::HeadMinusWindow {
                window_blocks: 0,
                floor: 0,
            },
            initial_backoff: WATCHER_INITIAL_BACKOFF,
            max_backoff: WATCHER_MAX_BACKOFF,
            rpc_call_timeout: None,
            shutdown: CancellationToken::new(),
            seed_cursor: None,
            label: "node-address",
            on_established: Some(established_hook(&metrics)),
            on_backoff: Some(backoff_hook(&metrics)),
        };
        let watcher_handle = tokio::spawn(resumable_watcher::run(provider, cfg, sink));

        Ok(Self {
            bindings,
            _watcher: AbortOnDrop(watcher_handle),
        })
    }
}

/// Applies `CapacityBond` `NodeRegistered`/`NodeDeregistered` logs to the
/// node→address bindings (#1092). `apply` live-tails from head (the
/// authoritative set came from `getActiveNodes` at bootstrap) and never returns
/// `Err` — an undecodable log is logged and skipped.
struct NodeAddressSink {
    bindings: Arc<RwLock<HashMap<NodeId, Address>>>,
    metrics: Arc<Metrics>,
}

impl LogSink for NodeAddressSink {
    #[allow(clippy::cognitive_complexity)]
    async fn apply(&mut self, log: Log) -> Result<()> {
        match log.topic0().copied() {
            Some(sig) if sig == CapacityBond::NodeRegistered::SIGNATURE_HASH => {
                match CapacityBond::NodeRegistered::decode_log_data(&log.inner.data) {
                    Ok(event) => set_binding(
                        &self.bindings,
                        &self.metrics,
                        NodeId::from_bytes(event.nodeId.0),
                        event.ethAddress,
                    ),
                    Err(err) => warn!(%err, "skipping undecodable NodeRegistered log"),
                }
            }
            Some(sig) if sig == CapacityBond::NodeDeregistered::SIGNATURE_HASH => {
                match CapacityBond::NodeDeregistered::decode_log_data(&log.inner.data) {
                    Ok(event) => {
                        remove_binding(
                            &self.bindings,
                            &self.metrics,
                            &NodeId::from_bytes(event.nodeId.0),
                        );
                    }
                    Err(err) => warn!(%err, "skipping undecodable NodeDeregistered log"),
                }
            }
            _ => {
                debug!(topic0 = ?log.topic0(), "unmatched CapacityBond event in subscribed OR-set");
            }
        }
        Ok(())
    }
}

/// Wire the healthy-cycle transition to the down-seconds gauge (→ 0).
fn established_hook(metrics: &Arc<Metrics>) -> WatcherHook {
    let metrics = Arc::clone(metrics);
    Box::new(move || metrics.node_address_watcher_cycle_established())
}

/// Wire a tick failure to the backoff gauge.
fn backoff_hook(metrics: &Arc<Metrics>) -> WatcherHook {
    let metrics = Arc::clone(metrics);
    Box::new(move || metrics.node_address_watcher_backoff_started())
}

impl NodeAddressResolver for ChainNodeAddressDirectory {
    fn address_of(&self, node_id: &NodeId) -> Option<Address> {
        // Poison-tolerant read: a poisoned lock means something panicked while
        // holding the write guard; the map is still structurally valid, so
        // recover rather than propagate a panic into the pull path.
        match self.bindings.read() {
            Ok(guard) => guard.get(node_id).copied(),
            Err(poisoned) => {
                warn!("ChainNodeAddressDirectory bindings RwLock poisoned; recovering inner state");
                poisoned.into_inner().get(node_id).copied()
            }
        }
    }

    fn node_id_for(&self, address: &Address) -> Option<NodeId> {
        // O(n) scan of the binding set — the reconcile sweep calls this hourly
        // for a handful of channels, so a reverse index isn't worth maintaining.
        // Add a reverse map only if a node ever tracks thousands of buyer
        // channels.
        let scan = |guard: &HashMap<NodeId, Address>| {
            guard
                .iter()
                .find_map(|(node_id, addr)| (addr == address).then_some(*node_id))
        };
        match self.bindings.read() {
            Ok(guard) => scan(&guard),
            Err(poisoned) => {
                warn!("ChainNodeAddressDirectory bindings RwLock poisoned; recovering inner state");
                scan(&poisoned.into_inner())
            }
        }
    }
}

/// Watcher join handle that aborts the task on drop (see
/// [`crate::dht::chain_staker_set`] for the rationale).
#[derive(Debug)]
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Paginate `getActiveNodes`, collecting every registered node's
/// `nodeId → ethAddress` binding. Unlike the staker-set bootstrap this applies
/// NO `isActive` filter: the binding is valid for any registered operator, and
/// we may need to pay one whose `isActive` predicate is momentarily false
/// (mid-unbonding) but whose channel is still serviceable.
async fn bootstrap_bindings<P>(
    registry: &CapacityBond::CapacityBondInstance<P>,
) -> Result<HashMap<NodeId, Address>>
where
    P: Provider + Clone,
{
    let mut bindings = HashMap::new();
    let mut offset = 0u64;
    loop {
        let page = registry
            .getActiveNodes(U256::from(offset), U256::from(PAGE_SIZE))
            .call()
            .await
            .with_context(|| format!("getActiveNodes(offset={offset}, limit={PAGE_SIZE})"))?;
        if page.is_empty() {
            break;
        }
        let page_len = page.len() as u64;
        for node in &page {
            bindings.insert(NodeId::from_bytes(node.nodeId.0), node.ethAddress);
        }
        offset = offset.saturating_add(page_len);
    }
    Ok(bindings)
}

/// Insert/update `node_id → address`, republishing the size gauge only when the
/// set actually grew (a re-registration that overwrites an existing binding
/// with the same key does not change cardinality).
fn set_binding(
    bindings: &Arc<RwLock<HashMap<NodeId, Address>>>,
    metrics: &Arc<Metrics>,
    node_id: NodeId,
    address: Address,
) {
    let (is_new, size) = {
        let mut guard = match bindings.write() {
            Ok(g) => g,
            Err(poisoned) => {
                warn!("ChainNodeAddressDirectory bindings RwLock poisoned; recovering inner state");
                poisoned.into_inner()
            }
        };
        // `insert` returns the prior value: `None` means a new key (cardinality
        // grew); `Some` means an overwrite (same key, possibly rotated address)
        // that leaves the size — and thus the gauge — unchanged.
        let is_new = guard.insert(node_id, address).is_none();
        (is_new, guard.len())
    };
    if is_new {
        metrics.node_address_directory_size(size);
    }
}

/// Remove `node_id`'s binding, republishing the size gauge only on a real
/// removal (removing an absent key is a no-op).
fn remove_binding(
    bindings: &Arc<RwLock<HashMap<NodeId, Address>>>,
    metrics: &Arc<Metrics>,
    node_id: &NodeId,
) {
    let (removed, size) = {
        let mut guard = match bindings.write() {
            Ok(g) => g,
            Err(poisoned) => {
                warn!("ChainNodeAddressDirectory bindings RwLock poisoned; recovering inner state");
                poisoned.into_inner()
            }
        };
        let removed = guard.remove(node_id).is_some();
        (removed, guard.len())
    };
    if removed {
        metrics.node_address_directory_size(size);
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
