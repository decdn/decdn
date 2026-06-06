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
use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::dht::routing::NodeId;
use crate::metrics::Metrics;
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
        metrics: Arc<Metrics>,
    ) -> Result<Self>
    where
        P: Provider + Clone + 'static,
    {
        let registry = CapacityBond::new(registry_addr, provider);
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
        let watcher_handle = tokio::spawn(watcher_loop(registry, Arc::clone(&bindings), metrics));

        Ok(Self {
            bindings,
            _watcher: AbortOnDrop(watcher_handle),
        })
    }
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

/// Background event loop following `NodeRegistered` / `NodeDeregistered`. On a
/// stream error it logs, backs off exponentially, and re-establishes filters.
async fn watcher_loop<P>(
    registry: CapacityBond::CapacityBondInstance<P>,
    bindings: Arc<RwLock<HashMap<NodeId, Address>>>,
    metrics: Arc<Metrics>,
) where
    P: Provider + Clone,
{
    let mut backoff = WATCHER_INITIAL_BACKOFF;
    loop {
        match run_watcher_once(&registry, &bindings, &metrics).await {
            Ok(()) => {
                debug!("node-address watcher stream ended cleanly; restarting subscription");
                backoff = WATCHER_INITIAL_BACKOFF;
            }
            Err(err) => {
                // Open a drift window: bindings arriving on-chain while filters
                // are down are missed until the next re-establish, so those
                // providers become unpayable and are skipped. Edge-trip the
                // restart counter + start the down-seconds clock (mirrors the
                // staker-set watcher, #788).
                metrics.node_address_watcher_backoff_started();
                warn!(
                    %err,
                    backoff_secs = backoff.as_secs(),
                    "ChainNodeAddressDirectory watcher RPC error; restarting after backoff \
                     (binding map may be briefly stale)"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(WATCHER_MAX_BACKOFF);
            }
        }
    }
}

/// Open the two binding-mutating event filters and drain them until either
/// errors. `Ok(())` on a clean stream end (filter expiry / provider rotation);
/// `Err` on a transport-level failure.
async fn run_watcher_once<P>(
    registry: &CapacityBond::CapacityBondInstance<P>,
    bindings: &Arc<RwLock<HashMap<NodeId, Address>>>,
    metrics: &Arc<Metrics>,
) -> Result<()>
where
    P: Provider + Clone,
{
    let mut node_registered = registry
        .NodeRegistered_filter()
        .watch()
        .await
        .context("watch NodeRegistered")?
        .into_stream();
    let mut node_deregistered = registry
        .NodeDeregistered_filter()
        .watch()
        .await
        .context("watch NodeDeregistered")?
        .into_stream();

    // Both filters established: a healthy cycle. Clear the down-seconds clock so
    // it reads 0 for the life of this cycle (it climbs again only on the next
    // error).
    metrics.node_address_watcher_cycle_established();

    loop {
        tokio::select! {
            ev = node_registered.next() => match ev {
                Some(Ok((event, _log))) => {
                    set_binding(bindings, metrics, NodeId::from_bytes(event.nodeId.0), event.ethAddress);
                }
                Some(Err(e)) => return Err(e).context("NodeRegistered stream"),
                None => return Ok(()),
            },
            ev = node_deregistered.next() => match ev {
                Some(Ok((event, _log))) => {
                    remove_binding(bindings, metrics, &NodeId::from_bytes(event.nodeId.0));
                }
                Some(Err(e)) => return Err(e).context("NodeDeregistered stream"),
                None => return Ok(()),
            },
        }
    }
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
