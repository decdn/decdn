//! One `CapacityBond` snapshot feeding five registry projections.
//!
//! [`ChainStakerSet`] (membership: `NodeId → active?`), [`ChainNodeAddressDirectory`]
//! (bindings: `NodeId → operator address`), the region map (`NodeId → regionHint`),
//! the operator reverse map (`operator address → NodeId`), and the
//! [`DialAddrDirectory`] (`NodeId → registry multiaddrs`, as iroh direct-dial
//! addresses) all derive from one paginated `getRegisteredNodes` read at a
//! pinned block. The node re-reads that snapshot every
//! `REGISTRY_REFRESH_INTERVAL`, and early when the staker set meets a node it
//! does not hold, at most once per `UNKNOWN_NODE_REFRESH_GAP`. It reads no
//! event log.
//!
//! Bindings is gated on `cache.node_to_node_pull_through_enabled`: when
//! pull-through is off, the bindings projection is not built at all. Regions,
//! the operator reverse map, and the dial-address directory are always built,
//! regardless of pull-through — the ADR-030 selection penalty needs the region
//! map unconditionally, the chain-backed origin directory needs the reverse map
//! to resolve operators locally with no `nodeIdOf` RPC, and every node dial
//! (DHT included) resolves direct addresses through the dial-address directory.
//!
//! `active` takes the per-entry `active[i]` that `getRegisteredNodes` computes
//! on-chain (`isActive(operator)`: registered AND active bond ≥ the operator's
//! floor AND not ejected). `bindings`, `regions`, the reverse map, and the
//! dial-address directory are unfiltered: an operator below its bond floor is
//! inactive but still payable, and liveness is applied at read time via the
//! shared `StakerSet`.
//!
//! # Fatality
//!
//! The boot snapshot is fatal: staker-set requires it. A transient RPC failure
//! retries on the shared boot budget; a deterministic fault or an exhausted
//! budget propagates. A failed re-read later keeps the previous projections
//! (build-then-swap) and counts
//! `decdn_capacity_bond_registry_resync_failures_total`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use alloy::eips::BlockId;
use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use anyhow::{Context, Result};
use decdn_incentive::node_register::decode_dial_addrs;
use decdn_protocol::Region;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain_events::boot_retry::{BootFault, BootRetry};
use crate::chain_events::refresh::{RefreshTrigger, run_refresh_loop};
use crate::chain_events::snapshot::snapshot_block;
use crate::chain_events::timed;
use crate::dht::chain_projection::{with_read, with_write};
use crate::dht::chain_staker_set::ChainStakerSet;
use crate::dht::dial_addrs::DialAddrDirectory;
use crate::dht::node_address::{ChainNodeAddressDirectory, NodeAddressResolver};
use crate::dht::routing::NodeId;
use crate::dht::staker_set::StakerSet;
use crate::metrics::Metrics;
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::capacity_bond::CapacityBond;

/// Page size for the paginated `getRegisteredNodes` read. Matches ADR 019 § Step
/// 3.3's worked-example limit.
const PAGE_SIZE: u64 = 100;

/// How often the registry snapshot is re-read. Five minutes keeps a bond,
/// ejection or region change visible within one DHT republish cycle, at one
/// paginated read per interval.
pub(crate) const REGISTRY_REFRESH_INTERVAL: Duration = Duration::from_mins(5);

/// The least time between two registry reads that a lookup miss pulls forward.
/// One minute lets a newly registered node in quickly while a flood of unknown
/// `NodeId`s costs at most one snapshot a minute.
pub(crate) const UNKNOWN_NODE_REFRESH_GAP: Duration = Duration::from_mins(1);

/// A refresh loop ready to spawn: it re-reads its snapshot until the shutdown
/// token it was built with is cancelled.
pub(crate) type RefreshTask = Pin<Box<dyn Future<Output = ()> + Send>>;

/// The registry projections, plus the refresh loop that keeps them current.
///
/// `node_addresses` is `None` when `cache.node_to_node_pull_through_enabled` is
/// off: the bindings projection is not built at all, so nothing ever moves
/// `node_address_directory_size` off `0`. The gauge is still exported in that
/// state, so any alert on it must be scoped to nodes with pull-through on
/// (#1231).
pub struct RegistryHandles {
    /// The bonded-operator set the DHT admits records from.
    pub staker_set: Arc<dyn StakerSet>,
    /// `NodeId` → dial address, present only where pull-through is on.
    pub node_addresses: Option<Arc<dyn NodeAddressResolver>>,
    /// `NodeId → regionHint`, as the canonical [`decdn_protocol::Region`] code;
    /// an empty or invalid hint has no entry. Read by the ADR-030
    /// region-latency penalty on the selection/pull path. Always present,
    /// unlike `node_addresses`.
    pub regions: Arc<RwLock<HashMap<NodeId, String>>>,
    /// `operator address → bound NodeId`, always built (like `regions`, unlike
    /// the pull-through-gated `bindings`) so the chain-backed origin directory
    /// can resolve operators locally with no `nodeIdOf` RPC.
    pub operator_to_node: Arc<RwLock<HashMap<Address, NodeId>>>,
    /// `NodeId → registry-published direct dial addresses`, always built. The
    /// runtime adds its [`DialAddrDirectory::lookup`] to the node's endpoint.
    pub dial_addrs: DialAddrDirectory,
    /// The refresh loop. The runtime stops it after `router.shutdown`, because
    /// the staker set gates DHT admission during drain.
    pub(crate) refresh: RefreshTask,
}

impl std::fmt::Debug for RegistryHandles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistryHandles").finish_non_exhaustive()
    }
}

/// The chain reads the registry makes, behind a trait so the refresh is
/// unit-testable without a provider.
pub(crate) trait RegistryChainReads: Send + Sync {
    /// The block a snapshot pins its pages to (`snapshot::snapshot_block`).
    fn snapshot_block(&self) -> impl Future<Output = Result<u64>> + Send;

    /// Read the projections from chain state at block `at`.
    fn full_snapshot(&self, at: u64) -> impl Future<Output = Result<RegistrySnapshot>> + Send;
}

/// The five registry projections one `getRegisteredNodes` read derives.
#[derive(Debug, Clone, Default)]
pub(crate) struct RegistrySnapshot {
    /// `isActive`-filtered membership.
    pub(crate) active: HashSet<NodeId>,
    /// `NodeId → operator address`, unfiltered.
    pub(crate) bindings: HashMap<NodeId, Address>,
    /// `operator address → NodeId`, the inverse of `bindings`.
    pub(crate) operator_to_node: HashMap<Address, NodeId>,
    /// `NodeId → canonical regionHint`; an empty or invalid hint has no entry.
    pub(crate) regions: HashMap<NodeId, String>,
    /// `NodeId → decoded registry multiaddrs`; a node with none has no entry.
    pub(crate) dial_addrs: HashMap<NodeId, Vec<SocketAddr>>,
}

/// Production [`RegistryChainReads`] over the live contract.
struct ContractReads<P: Provider + Clone> {
    registry: CapacityBond::CapacityBondInstance<P>,
}

impl<P: Provider + Clone> RegistryChainReads for ContractReads<P> {
    async fn snapshot_block(&self) -> Result<u64> {
        snapshot_block(self.registry.provider(), *self.registry.address()).await
    }

    async fn full_snapshot(&self, at: u64) -> Result<RegistrySnapshot> {
        bootstrap_registry(&self.registry, BlockId::number(at)).await
    }
}

/// The projections the registry writes, and the reads that refresh them.
pub(crate) struct RegistrySync<R> {
    pub(crate) reads: R,
    pub(crate) active: Arc<RwLock<HashSet<NodeId>>>,
    /// `None` when pull-through is off — see [`RegistryHandles::node_addresses`].
    pub(crate) bindings: Option<Arc<RwLock<HashMap<NodeId, Address>>>>,
    pub(crate) regions: Arc<RwLock<HashMap<NodeId, String>>>,
    pub(crate) operator_to_node: Arc<RwLock<HashMap<Address, NodeId>>>,
    pub(crate) dial_addrs: DialAddrDirectory,
    pub(crate) metrics: Arc<Metrics>,
}

impl<R: RegistryChainReads> RegistrySync<R> {
    /// Swap a snapshot into every projection and publish the gauges.
    fn install(&self, snap: RegistrySnapshot) {
        let RegistrySnapshot {
            active,
            bindings,
            operator_to_node,
            regions,
            dial_addrs,
        } = snap;
        let active_len = active.len();
        let binding_len = bindings.len();
        with_write(&self.active, "chain staker set", |set| *set = active);
        self.metrics.staker_set_active_count(active_len);
        if let Some(slot) = &self.bindings {
            with_write(slot, "chain node-address directory", |map| *map = bindings);
            self.metrics.node_address_directory_size(binding_len);
        }
        with_write(&self.operator_to_node, "chain operator reverse map", |m| {
            *m = operator_to_node;
        });
        with_write(&self.regions, "chain region directory", |map| {
            *map = regions;
        });
        self.dial_addrs.replace_all(dial_addrs);
        publish_region_counts(&self.active, &self.regions, &self.metrics);
        self.metrics.capacity_bond_registry_resync();
    }

    /// Re-read the snapshot and swap it in. Build-then-swap: a failed read
    /// leaves the previous projections intact, because an empty staker set
    /// would make the node treat every peer as unstaked. A failure counts
    /// `decdn_capacity_bond_registry_resync_failures_total`; a success stamps
    /// `decdn_capacity_bond_registry_last_resync_timestamp_seconds`.
    pub(crate) async fn refresh(&self) {
        let read = async {
            let at = self.reads.snapshot_block().await?;
            self.reads.full_snapshot(at).await
        };
        match read.await {
            Ok(snap) => {
                let active = snap.active.len();
                self.install(snap);
                debug!(active, "capacity-bond registry re-read");
            }
            Err(err) => {
                self.metrics.capacity_bond_registry_resync_failure();
                warn!(
                    error = %sanitize_err_chain(&err),
                    "capacity-bond registry re-read failed; keeping current projections"
                );
            }
        }
    }
}

/// One paginated `getRegisteredNodes` read feeding all five projections.
///
/// `getRegisteredNodes` computes `isActive` per entry on-chain and returns it
/// as a parallel `active[]`, so a single call per page derives every
/// projection with no per-entry round-trip. Every page reads at `at`, so the
/// pages agree with each other: a load-balanced provider cannot answer one page
/// from a lagging upstream and the next from a current one.
async fn bootstrap_registry<P>(
    registry: &CapacityBond::CapacityBondInstance<P>,
    at: BlockId,
) -> Result<RegistrySnapshot>
where
    P: Provider + Clone,
{
    let mut snap = RegistrySnapshot::default();
    let mut offset = 0u64;
    loop {
        let resp = timed(
            None,
            "getRegisteredNodes",
            registry
                .getRegisteredNodes(U256::from(offset), U256::from(PAGE_SIZE))
                .block(at)
                .call(),
        )
        .await
        .with_context(|| format!("getRegisteredNodes(offset={offset}, limit={PAGE_SIZE})"))?;
        // `page` and `active` are equal-length by construction (the contract fills
        // both in one loop). A divergence is an ABI/decoder fault; the `zip` below
        // would silently truncate and drop stakers/bindings, so fail loudly. It is
        // a `BootFault`, so a boot read does not retry it.
        if resp.page.len() != resp.active.len() {
            return Err(BootFault(format!(
                "getRegisteredNodes(offset={offset}) returned mismatched page/active \
                 lengths ({} vs {}) — ABI or decoder fault",
                resp.page.len(),
                resp.active.len()
            ))
            .into());
        }
        if resp.page.is_empty() {
            break;
        }
        let page_len = resp.page.len() as u64;
        for (node, &is_active) in resp.page.iter().zip(resp.active.iter()) {
            let node_id = NodeId::from_bytes(node.nodeId.0);
            snap.bindings.insert(node_id, node.ethAddress);
            snap.operator_to_node.insert(node.ethAddress, node_id);
            if let Some(region) = canonical_region(&node.regionHint) {
                snap.regions.insert(node_id, region);
            }
            let dial = decode_dial_addrs(&node.multiaddrs);
            if !dial.is_empty() {
                snap.dial_addrs.insert(node_id, dial);
            }
            if is_active {
                snap.active.insert(node_id);
            }
        }
        offset = offset.saturating_add(page_len);
    }
    Ok(snap)
}

/// Read `CapacityBond` once at a pinned block and return the projections with
/// the refresh loop that keeps them current until `shutdown` is cancelled.
///
/// The snapshot-block read and the snapshot retry transient failures on
/// `boot`'s budget; a deterministic fault or an exhausted budget fails the
/// bootstrap. `track_node_addresses` mirrors
/// `cache.node_to_node_pull_through_enabled`.
///
/// # Errors
///
/// The boot snapshot's error once the boot budget is spent, or at once on a
/// deterministic fault.
pub async fn bootstrap<P>(
    provider: P,
    registry_addr: Address,
    track_node_addresses: bool,
    metrics: Arc<Metrics>,
    boot: &BootRetry,
    shutdown: CancellationToken,
) -> Result<RegistryHandles>
where
    P: Provider + Clone + 'static,
{
    let reads = ContractReads {
        registry: CapacityBond::new(registry_addr, provider),
    };
    let (snapshot_block, snapshot) = boot
        .run("CapacityBond registry snapshot", || async {
            let snapshot_block = reads
                .snapshot_block()
                .await
                .context("read the CapacityBond registry snapshot block")?;
            let snapshot = reads.full_snapshot(snapshot_block).await.with_context(|| {
                format!("paginated getRegisteredNodes from CapacityBond at {registry_addr}")
            })?;
            Ok((snapshot_block, snapshot))
        })
        .await?;
    info!(
        active_count = snapshot.active.len(),
        binding_count = snapshot.bindings.len(),
        dial_addr_count = snapshot.dial_addrs.len(),
        track_node_addresses,
        snapshot_block,
        %registry_addr,
        "CapacityBond registry bootstrap complete"
    );
    let (handles, sync, trigger) = assemble(reads, track_node_addresses, metrics, snapshot);
    let sync = Arc::new(sync);
    let refresh: RefreshTask = Box::pin(run_refresh_loop(
        REGISTRY_REFRESH_INTERVAL,
        UNKNOWN_NODE_REFRESH_GAP,
        Some(trigger),
        shutdown,
        move || {
            let sync = Arc::clone(&sync);
            async move { sync.refresh().await }
        },
    ));
    Ok(RegistryHandles { refresh, ..handles })
}

/// Build the projections from a boot snapshot. Returns the handles (with an
/// idle refresh task), the sync that refreshes them and the trigger the
/// staker set pulls on a miss.
fn assemble<R: RegistryChainReads>(
    reads: R,
    track_node_addresses: bool,
    metrics: Arc<Metrics>,
    snapshot: RegistrySnapshot,
) -> (RegistryHandles, RegistrySync<R>, RefreshTrigger) {
    let sync = RegistrySync {
        reads,
        active: Arc::new(RwLock::new(HashSet::new())),
        bindings: track_node_addresses.then(|| Arc::new(RwLock::new(HashMap::new()))),
        operator_to_node: Arc::new(RwLock::new(HashMap::new())),
        regions: Arc::new(RwLock::new(HashMap::new())),
        dial_addrs: DialAddrDirectory::default(),
        metrics,
    };
    // The boot snapshot is a successful re-read: installing it stamps the
    // liveness gauge, which the staleness alert's `> 0` guard needs.
    sync.install(snapshot);
    let trigger = RefreshTrigger::new();
    let staker_set: Arc<dyn StakerSet> = Arc::new(ChainStakerSet::from_parts(
        Arc::clone(&sync.active),
        trigger.clone(),
    ));
    let node_addresses = sync.bindings.clone().map(|b| {
        Arc::new(ChainNodeAddressDirectory::from_parts(b)) as Arc<dyn NodeAddressResolver>
    });
    let handles = RegistryHandles {
        staker_set,
        node_addresses,
        regions: Arc::clone(&sync.regions),
        operator_to_node: Arc::clone(&sync.operator_to_node),
        dial_addrs: sync.dial_addrs.clone(),
        refresh: Box::pin(std::future::ready(())),
    };
    (handles, sync, trigger)
}

/// The canonical form of an on-chain `regionHint`, or `None` when it is empty
/// or not an accepted code.
///
/// The contract only length-checks `regionHint`, so `" de "` can land on
/// chain. Every region-map consumer compares codes as strings — the ADR-030
/// penalty against the node's own normalized `identity.region` — so the map
/// stores the [`Region::parse`] form and nothing else.
fn canonical_region(raw: &str) -> Option<String> {
    Region::parse(raw).map(|r| r.as_str().to_owned())
}

/// Publish the active-staker set size per declared region (ADR 030) to
/// `decdn_staker_set_active_by_region`.
///
/// `regions` is unfiltered, so the count walks `active` and looks each node up.
/// An active node with no region-map entry counts as unknown. The map holds
/// canonical codes only ([`canonical_region`]); parsing again yields the typed
/// key, and keeps an unparsed string from ever becoming a metric label.
fn publish_region_counts(
    active: &RwLock<HashSet<NodeId>>,
    regions: &RwLock<HashMap<NodeId, String>>,
    metrics: &Metrics,
) {
    let (counts, unknown) = with_read(active, "chain staker set", |active| {
        with_read(regions, "chain region directory", |regions| {
            let mut counts: BTreeMap<Region, usize> = BTreeMap::new();
            let mut unknown = 0usize;
            for id in active {
                match regions.get(id).and_then(|raw| Region::parse(raw)) {
                    Some(region) => *counts.entry(region).or_default() += 1,
                    None => unknown += 1,
                }
            }
            (counts, unknown)
        })
    });
    metrics.staker_set_active_by_region(&counts, unknown);
}

/// Read a node's operator-attested region (ADR 030) straight from the registry's
/// `NodeId → regionHint` projection, or `None` when the registry holds no region
/// for it. Poison-tolerant via [`with_read`]: a writer that panicked left the map
/// structurally intact, so recover it and log once rather than let a prior panic
/// permanently suppress region reads (and with them the ADR-030 selection penalty).
pub(crate) fn region_of(
    regions: &Arc<RwLock<HashMap<NodeId, String>>>,
    id: NodeId,
) -> Option<String> {
    with_read(regions, "registry regions", |m| m.get(&id).cloned())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests;
