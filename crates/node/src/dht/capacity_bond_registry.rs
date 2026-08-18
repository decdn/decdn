//! One `CapacityBond` watcher feeding four registry projections.
//!
//! [`ChainStakerSet`] (membership: `NodeId → active?`), [`ChainNodeAddressDirectory`]
//! (bindings: `NodeId → operator address`), the region map (`NodeId → regionHint`),
//! and the operator reverse map (`operator address → NodeId`) are all derived
//! from the same `CapacityBond` contract. This module runs one enumeration and
//! one `eth_getLogs` loop over the shared `CapacityBond` address, demuxing to
//! all four projections; node-address's two topics are a strict *subset* of
//! staker-set's five.
//!
//! Bindings is gated on `cache.node_to_node_pull_through_enabled`: when
//! pull-through is off, the bindings projection is not built at all. Regions
//! and the operator reverse map are always built, regardless of pull-through —
//! the ADR-030 selection penalty needs the region map unconditionally, and the
//! chain-backed origin directory needs the reverse map to resolve operators
//! locally with no `nodeIdOf` RPC.
//!
//! # What actually differs between the projections
//!
//! Mostly the **enumeration**. The live event arms are close to a clean union:
//! `NodeRegistered` inserts into active, bindings (if built), regions (if
//! non-empty), and the reverse map, all unfiltered — staker-set's live arm
//! applies no `isActive` check either (its filter is bootstrap-only). It is
//! tempting to read "staker-set is the filtered view, the rest are the
//! unfiltered ones" as a live-path difference and encode it in the sink; that
//! would be wrong, and would drop registrations from the active set.
//!
//! At bootstrap `active` and `bindings` genuinely diverge:
//!
//! - `active` takes the per-entry `active[i]` that `getRegisteredNodes` computes
//!   on-chain (`isActive(operator)`: registered AND bond ≥ minBond AND no
//!   unbonding AND not ejected) alongside the `_registeredAddrs` page.
//! - `bindings` applies no filter: an operator mid-unbonding has `isActive =
//!   false` but is still payable, so its binding must survive.
//! - `regions` and the reverse map are likewise unfiltered: liveness is applied
//!   separately at read time via the shared `StakerSet`.
//!
//! # Fatality
//!
//! The single `getRegisteredNodes` bootstrap is fatal: staker-set requires it,
//! and its failure propagates. The bindings projection (pull-through is
//! opportunistic, so it would otherwise be non-fatal) has **no RPC of its own**:
//! it is derived from page data already in hand and cannot fail independently.
//! The same is true of regions and the reverse map. A node either boots with
//! all four projections or fails at the one shared read, so pull-through is
//! never lost on its own.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use crate::chain_events::multiplexed_poller::{Route, SinkSource};
use crate::chain_events::resumable_watcher::{CursorStart, LogSink};
use crate::chain_events::shared_head::HeadSource;
use crate::chain_events::timed;
use crate::dht::chain_projection::{with_read, with_write};
use crate::dht::chain_staker_set::{ChainStakerSet, StakerChange, apply_change};
use crate::dht::node_address::{
    ChainNodeAddressDirectory, NodeAddressResolver, remove_binding, set_binding,
};
use crate::dht::routing::NodeId;
use crate::dht::staker_set::StakerSet;
use crate::metrics::{Metrics, metric_hook};
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::capacity_bond::CapacityBond;

/// Page size for the paginated `getRegisteredNodes` read. Matches ADR 019 § Step
/// 3.3's worked-example limit.
const PAGE_SIZE: u64 = 100;

/// Both projections, plus the shared watcher that keeps them current.
///
/// `node_addresses` is `None` when `cache.node_to_node_pull_through_enabled` is
/// off: the bindings projection is not built at all, so nothing ever moves
/// `node_address_directory_size` off `0`.
///
/// The gauge is still *exported* in that state — every `DecdnMetrics` field
/// registers unconditionally as one group — so a pull-through-off node (the
/// default) publishes a permanent `0`, which is indistinguishable from a
/// pull-through-on node whose directory has collapsed. Any alert on this gauge
/// must therefore be scoped to nodes with pull-through on. Making it genuinely
/// absent needs a split group or a labelled family; see #1231.
#[derive(Debug)]
pub struct RegistryHandles {
    pub staker_set: Arc<dyn StakerSet>,
    pub node_addresses: Option<Arc<dyn NodeAddressResolver>>,
    /// `NodeId → regionHint` (non-empty only). Read by the ADR-030 region-latency
    /// penalty on the selection/pull path. Always present, unlike `node_addresses`.
    pub regions: Arc<RwLock<HashMap<NodeId, String>>>,
    /// `operator address → bound NodeId`, always built (like `regions`, unlike
    /// the pull-through-gated `bindings`) so the chain-backed origin directory
    /// can resolve operators locally with no `nodeIdOf` RPC.
    pub operator_to_node: Arc<RwLock<HashMap<Address, NodeId>>>,
    /// The membership/binding [`Route`] the runtime registers on the shared
    /// multiplexed poller. The poller stops *after* `router.shutdown` because
    /// this route's staker-set projection gates DHT admission during drain.
    /// `pub(crate)`, since [`Route`] is crate-private and only the runtime wires
    /// it.
    pub(crate) route: Route,
}

/// The chain reads the live watcher performs, behind a trait so the event
/// handlers are unit-testable without a provider.
///
/// Spelled as RPITIT with an explicit `+ Send` (rather than `async fn`, whose
/// desugaring carries no `Send` bound) because the sink is handed to
/// `tokio::spawn`, which requires the whole `apply` future to be `Send`. Same
/// shape as [`LogSink`] itself, for the same reason. `Sync` because `&self` is
/// held across the `node_id_of` await inside `apply`.
pub(crate) trait RegistryChainReads: Send + Sync {
    /// `nodeIdOf(operator)` → `(nodeId, active)`; `None` when the operator has no
    /// binding (`bytes32(0)`).
    fn node_id_of(
        &self,
        operator: Address,
    ) -> impl Future<Output = Result<Option<(NodeId, bool)>>> + Send;

    /// Re-enumerate both projections from chain state, as the bootstrap does.
    ///
    /// This is the self-healing leg. Every other input to these projections is
    /// an event, and an event can be missed — an orphaned log at the unstable
    /// tip stays applied because `eth_getLogs` never reports a removal, and a
    /// tick lost to RPC backoff advances nothing but is not replayed. Neither
    /// shows up as an error, so without a periodic authoritative re-read the
    /// only repair for a drifted set is a process restart.
    #[allow(clippy::type_complexity)] // (active set, bindings, reverse map, regions) — the four projections
    fn full_snapshot(
        &self,
    ) -> impl Future<
        Output = Result<(
            HashSet<NodeId>,
            HashMap<NodeId, Address>,
            HashMap<Address, NodeId>,
            HashMap<NodeId, String>,
        )>,
    > + Send;
}

/// Production [`RegistryChainReads`] over the live contract.
struct ContractReads<P: Provider + Clone> {
    registry: CapacityBond::CapacityBondInstance<P>,
}

impl<P: Provider + Clone> RegistryChainReads for ContractReads<P> {
    async fn full_snapshot(
        &self,
    ) -> Result<(
        HashSet<NodeId>,
        HashMap<NodeId, Address>,
        HashMap<Address, NodeId>,
        HashMap<NodeId, String>,
    )> {
        bootstrap_registry(&self.registry).await
    }

    async fn node_id_of(&self, operator: Address) -> Result<Option<(NodeId, bool)>> {
        // Bounded explicitly: the poller's per-call timeout covers only its own
        // merged `get_logs`, so a sink's follow-up read stays unbounded unless
        // it wraps itself (the `blacklist_watcher::scope_check` precedent). That
        // matters here because this single loop feeds both the staker-set and the
        // bindings projection: one stalled `nodeIdOf` would wedge both.
        // A timeout surfaces as `Err`, which `on_operator_change`
        // already counts and skips without tripping the stream backoff.
        let resolved = timed(None, "nodeIdOf", self.registry.nodeIdOf(operator).call()).await?;
        let node_id = resolved.nodeId.0;
        if node_id == [0u8; 32] {
            return Ok(None);
        }
        Ok(Some((node_id.into(), resolved.active)))
    }
}

/// Applies the five `CapacityBond` membership/binding events to both projections
/// in one pass — one `decode_log_data` per log feeds both.
///
/// Never returns `Err`: an undecodable log is logged and skipped (a deterministic
/// re-scan of a permanently-undecodable log would hot-loop the cursor), and an
/// operator-indexed event whose `nodeIdOf` fails is counted and skipped without
/// tripping the stream backoff.
pub(crate) struct RegistrySink<R> {
    pub(crate) reads: R,
    pub(crate) active: Arc<RwLock<HashSet<NodeId>>>,
    /// `None` when pull-through is off — see [`RegistryHandles::node_addresses`].
    pub(crate) bindings: Option<Arc<RwLock<HashMap<NodeId, Address>>>>,
    /// `NodeId → regionHint`. Non-empty only; empty `regionHint` is absence.
    /// Always built (not gated on pull-through): the ADR-030 selection penalty
    /// reads it whether or not node-to-node pull is on.
    pub(crate) regions: Arc<RwLock<HashMap<NodeId, String>>>,
    /// `operator address → bound NodeId`. Always built (not gated on
    /// pull-through, like `regions`) and unfiltered: an ejected or unbonding
    /// operator keeps its reverse entry, since liveness is applied separately
    /// at read time via the shared `StakerSet`.
    pub(crate) operator_to_node: Arc<RwLock<HashMap<Address, NodeId>>>,
    pub(crate) metrics: Arc<Metrics>,
    /// How often [`RegistryChainReads::full_snapshot`] re-derives both
    /// projections. Deliberately a constant rather than a config knob: it is a
    /// correctness backstop, not a tuning surface, and an operator who set it
    /// wrong would silently lose the only drift repair short of a restart.
    pub(crate) resync_interval: Duration,
    /// When the last re-enumeration ran. Seeded to "just ran" at bootstrap,
    /// since the bootstrap enumeration IS one.
    pub(crate) last_resync: Option<Instant>,
}

impl<R: RegistryChainReads> RegistrySink<R> {
    /// `NodeRegistered`: active insert (unfiltered — see the module doc) AND a
    /// binding insert.
    fn on_registered(&self, node_id: NodeId, eth_address: Address, region: String) {
        apply_change(&self.active, &self.metrics, StakerChange::Active(node_id));
        if let Some(bindings) = &self.bindings {
            set_binding(bindings, &self.metrics, node_id, eth_address);
        }
        with_write(&self.operator_to_node, "chain operator reverse map", |m| {
            m.insert(eth_address, node_id);
        });
        if !region.is_empty() {
            with_write(&self.regions, "chain region directory", |m| {
                m.insert(node_id, region);
            });
        }
    }

    /// `NodeDeregistered`: the binding and region are cleared only here —
    /// `registerNode` sets them and `deregisterNode` clears them.
    /// Bond/unbonding/ejection transitions flip `isActive` without touching
    /// either.
    fn on_deregistered(&self, node_id: NodeId) {
        apply_change(&self.active, &self.metrics, StakerChange::Inactive(node_id));
        if let Some(bindings) = &self.bindings {
            remove_binding(bindings, &self.metrics, &node_id);
        }
        // No address is carried on this event, so remove by value: the reverse
        // map is always-on (unlike `bindings`), so it cannot be sourced from
        // `bindings` to find the address first.
        with_write(&self.operator_to_node, "chain operator reverse map", |m| {
            m.retain(|_addr, nid| *nid != node_id);
        });
        with_write(&self.regions, "chain region directory", |m| {
            m.remove(&node_id);
        });
    }

    /// Resolve an operator-indexed event via `nodeIdOf` and apply the implied
    /// change. The canonical `nodeIdOf.active` wins over what the event implies
    /// (they can disagree if a later transition raced the event). Bindings are
    /// untouched: these events never change one.
    async fn on_operator_change(&self, operator: Address, event_implies_active: bool) {
        let resolved = match self.reads.node_id_of(operator).await {
            Ok(r) => r,
            Err(err) => {
                // Dropping the change leaves the cached set out of sync with chain
                // state until a follow-up event for the same operator arrives.
                // Unlike a stream-level error this does not trip the watcher
                // backoff, so without this counter it would move no metric at all.
                self.metrics.staker_set_watcher_resolve_failure();
                warn!(
                    err = %sanitize_err_chain(&err),
                    %operator,
                    "nodeIdOf RPC failed; cached active set may diverge from chain state for this operator"
                );
                return;
            }
        };
        let Some((node_id, now_active)) = resolved else {
            debug!(%operator, "operator-indexed event for unbound operator; ignoring");
            return;
        };
        if now_active != event_implies_active {
            debug!(
                %operator,
                event_implies_active,
                now_active,
                "operator-indexed event disagrees with canonical nodeIdOf.active; trusting nodeIdOf"
            );
        }
        let change = if now_active {
            StakerChange::Active(node_id)
        } else {
            StakerChange::Inactive(node_id)
        };
        apply_change(&self.active, &self.metrics, change);
    }
}

impl<R: RegistryChainReads> LogSink for RegistrySink<R> {
    #[allow(clippy::cognitive_complexity)]
    async fn apply(&mut self, log: Log) -> Result<()> {
        match log.topic0().copied() {
            Some(sig) if sig == CapacityBond::NodeRegistered::SIGNATURE_HASH => {
                match CapacityBond::NodeRegistered::decode_log_data(&log.inner.data) {
                    Ok(event) => {
                        self.on_registered(
                            NodeId::from_bytes(event.nodeId.0),
                            event.ethAddress,
                            event.regionHint,
                        );
                    }
                    Err(err) => warn!(%err, "skipping undecodable NodeRegistered log"),
                }
            }
            Some(sig) if sig == CapacityBond::NodeDeregistered::SIGNATURE_HASH => {
                match CapacityBond::NodeDeregistered::decode_log_data(&log.inner.data) {
                    Ok(event) => self.on_deregistered(NodeId::from_bytes(event.nodeId.0)),
                    Err(err) => warn!(%err, "skipping undecodable NodeDeregistered log"),
                }
            }
            Some(sig) if sig == CapacityBond::NodeAutoEjected::SIGNATURE_HASH => {
                match CapacityBond::NodeAutoEjected::decode_log_data(&log.inner.data) {
                    // Deactivates WITHOUT clearing the binding: ejection flips
                    // `isActive` only, and the operator may still be owed payment
                    // on an open channel.
                    Ok(event) => {
                        apply_change(
                            &self.active,
                            &self.metrics,
                            StakerChange::Inactive(event.nodeId.0.into()),
                        );
                    }
                    Err(err) => warn!(%err, "skipping undecodable NodeAutoEjected log"),
                }
            }
            Some(sig) if sig == CapacityBond::Reinstated::SIGNATURE_HASH => {
                match CapacityBond::Reinstated::decode_log_data(&log.inner.data) {
                    Ok(event) => self.on_operator_change(event.operator, true).await,
                    Err(err) => warn!(%err, "skipping undecodable Reinstated log"),
                }
            }
            Some(sig) if sig == CapacityBond::UnbondingRequested::SIGNATURE_HASH => {
                match CapacityBond::UnbondingRequested::decode_log_data(&log.inner.data) {
                    Ok(event) => self.on_operator_change(event.operator, false).await,
                    Err(err) => warn!(%err, "skipping undecodable UnbondingRequested log"),
                }
            }
            _ => {
                debug!(topic0 = ?log.topic0(), "unmatched CapacityBond event in subscribed OR-set");
            }
        }
        Ok(())
    }

    /// Cadence-gated re-enumeration: rebuild both projections from chain state
    /// and swap them in.
    ///
    /// Build-then-swap, never clear-then-fill: the new sets are fully
    /// materialized before either lock is taken for writing, so a failed read
    /// leaves the previous projections intact rather than emptying them. An
    /// empty staker set would make the node treat every peer as unstaked.
    ///
    /// Returns `Ok` on failure. The event tail is the primary path and is still
    /// working; backing the whole watcher off would also stall event pickup,
    /// trading a stale set for no updates at all.
    async fn on_tick_complete(&mut self) -> Result<()> {
        let now = Instant::now();
        if self
            .last_resync
            .is_some_and(|last| now.duration_since(last) < self.resync_interval)
        {
            return Ok(());
        }
        // Stamp before the call, not only on success, so a persistently failing
        // read retries on the resync cadence rather than on every watcher tick.
        self.last_resync = Some(now);

        let (active, bindings, operator_to_node, regions) = match self.reads.full_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                warn!(%err, "capacity-bond registry resync failed; keeping current projections");
                return Ok(());
            }
        };

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
        debug!(
            active_count = active_len,
            binding_count = binding_len,
            "capacity-bond registry resynced from chain"
        );
        Ok(())
    }
}

/// How often the watcher re-derives both projections from `getRegisteredNodes`.
///
/// Fifteen minutes is well under any plausible drift-detection window and costs
/// one paginated read plus an `isActive` per registered operator — on a network
/// of tens of nodes, a handful of calls per quarter hour. Deliberately a
/// constant, not a config knob: it is a correctness backstop rather than a
/// tuning surface, and an operator who set it wrong (or to zero) would silently
/// lose the only repair for a drifted set short of a process restart.
const REGISTRY_RESYNC_INTERVAL: Duration = Duration::from_mins(15);

/// One paginated `getRegisteredNodes` read feeding all four projections.
///
/// Always returns all four maps and lets the caller drop the unwanted one — a
/// `want_bindings: bool` parameter would be a boolean trap, and building the map
/// is free while already iterating the page. See the module doc for why `active`
/// is `isActive`-filtered and `bindings` is not: `getRegisteredNodes` computes
/// `isActive` per entry on-chain and returns it as a parallel `active[]`, so a
/// single call per page derives `active` and `bindings` with no per-entry
/// round-trip. `operator_to_node` is the inverse of `bindings`, built from the
/// same page with no chain call of its own; like `regions` it is unfiltered and
/// always built.
async fn bootstrap_registry<P>(
    registry: &CapacityBond::CapacityBondInstance<P>,
) -> Result<(
    HashSet<NodeId>,
    HashMap<NodeId, Address>,
    HashMap<Address, NodeId>,
    HashMap<NodeId, String>,
)>
where
    P: Provider + Clone,
{
    let mut active = HashSet::new();
    let mut bindings = HashMap::new();
    let mut operator_to_node = HashMap::new();
    let mut regions = HashMap::new();
    let mut offset = 0u64;
    loop {
        let resp = registry
            .getRegisteredNodes(U256::from(offset), U256::from(PAGE_SIZE))
            .call()
            .await
            .with_context(|| format!("getRegisteredNodes(offset={offset}, limit={PAGE_SIZE})"))?;
        // `page` and `active` are equal-length by construction (the contract fills
        // both in one loop). A divergence is an ABI/decoder fault; the `zip` below
        // would silently truncate and drop stakers/bindings, so fail loudly.
        anyhow::ensure!(
            resp.page.len() == resp.active.len(),
            "getRegisteredNodes(offset={offset}) returned mismatched page/active \
             lengths ({} vs {}) — ABI or decoder fault",
            resp.page.len(),
            resp.active.len()
        );
        if resp.page.is_empty() {
            break;
        }
        let page_len = resp.page.len() as u64;
        // `active[i]` is `isActive(page[i])`, computed on-chain and index-aligned
        // with `page` by the contract. `bindings` takes every entry (unfiltered):
        // an operator mid-unbonding is inactive but still payable.
        for (node, &is_active) in resp.page.iter().zip(resp.active.iter()) {
            let node_id = NodeId::from_bytes(node.nodeId.0);
            bindings.insert(node_id, node.ethAddress);
            operator_to_node.insert(node.ethAddress, node_id);
            if !node.regionHint.is_empty() {
                regions.insert(node_id, node.regionHint.clone());
            }
            if is_active {
                active.insert(node_id);
            }
        }
        offset = offset.saturating_add(page_len);
    }
    Ok((active, bindings, operator_to_node, regions))
}

/// Enumerate `CapacityBond` once, then spawn the single watcher that keeps both
/// projections current.
///
/// `track_node_addresses` mirrors `cache.node_to_node_pull_through_enabled`.
pub async fn bootstrap<P>(
    provider: P,
    registry_addr: Address,
    head: Arc<dyn HeadSource>,
    track_node_addresses: bool,
    metrics: Arc<Metrics>,
) -> Result<RegistryHandles>
where
    P: Provider + Clone + 'static,
{
    let registry = CapacityBond::new(registry_addr, provider.clone());

    // Head BEFORE the enumeration, then seed the cursor there. The reverse order
    // would lose any event landing between the enumeration and the head read: it
    // would be neither in the snapshot nor above the cursor. Re-applying an event
    // the snapshot already reflects is a no-op in every arm, so overlap is safe
    // but a gap is not. (`SharedHead`'s TTL only ever makes this cursor *older*,
    // which widens the overlap — it cannot open a gap.)
    let snapshot_block = head
        .head()
        .await
        .context("read head block for CapacityBond registry snapshot")?;
    let (initial_active, initial_bindings, initial_operator_to_node, initial_regions) =
        bootstrap_registry(&registry).await.with_context(|| {
            format!("paginated getRegisteredNodes from CapacityBond at {registry_addr}")
        })?;
    info!(
        active_count = initial_active.len(),
        binding_count = initial_bindings.len(),
        track_node_addresses,
        snapshot_block,
        %registry_addr,
        "CapacityBond registry bootstrap complete"
    );
    // Publish the bootstrap sizes so the gauges are non-`(no data)` before the
    // first membership change.
    metrics.staker_set_active_count(initial_active.len());
    if track_node_addresses {
        metrics.node_address_directory_size(initial_bindings.len());
    }

    let active = Arc::new(RwLock::new(initial_active));
    let bindings = track_node_addresses.then(|| Arc::new(RwLock::new(initial_bindings)));
    let operator_to_node = Arc::new(RwLock::new(initial_operator_to_node));
    let regions = Arc::new(RwLock::new(initial_regions));

    let sink = RegistrySink {
        reads: ContractReads {
            registry: registry.clone(),
        },
        active: Arc::clone(&active),
        bindings: bindings.clone(),
        operator_to_node: Arc::clone(&operator_to_node),
        regions: Arc::clone(&regions),
        metrics: Arc::clone(&metrics),
        resync_interval: REGISTRY_RESYNC_INTERVAL,
        // The bootstrap enumeration just ran, so the first backstop resync is
        // due one interval from now rather than on the first tick.
        last_resync: Some(Instant::now()),
    };
    let route = Route {
        addresses: vec![registry_addr],
        topic0s: vec![
            CapacityBond::NodeRegistered::SIGNATURE_HASH,
            CapacityBond::NodeDeregistered::SIGNATURE_HASH,
            CapacityBond::NodeAutoEjected::SIGNATURE_HASH,
            CapacityBond::Reinstated::SIGNATURE_HASH,
            CapacityBond::UnbondingRequested::SIGNATURE_HASH,
        ],
        // Seed the live tail from the enumeration snapshot head; the staker set
        // is rebuilt from that enumeration each boot, so there is no durable
        // cursor to persist.
        start: CursorStart::Seeded {
            at: snapshot_block,
            persist: None,
        },
        // This sink observes no shutdown token, so it registers a `Ready` sink.
        sink: SinkSource::Ready(Box::new(sink)),
        label: "capacity-bond",
        // `staker_set_watcher_*` is the shared `capacity-bond` route's health, and
        // it covers the bindings projection too — one route feeds both, so there
        // is one thing to report.
        on_established: Some(metric_hook(
            &metrics,
            Metrics::staker_set_watcher_cycle_established,
        )),
        on_backoff: Some(metric_hook(
            &metrics,
            Metrics::staker_set_watcher_backoff_started,
        )),
        on_tick_success: Some(metric_hook(&metrics, Metrics::staker_set_watcher_tick)),
        on_task_panic: Some(metric_hook(
            &metrics,
            Metrics::staker_set_watcher_task_panicked,
        )),
    };

    let staker_set: Arc<dyn StakerSet> = Arc::new(ChainStakerSet::from_parts(active));
    let node_addresses = bindings.map(|b| {
        Arc::new(ChainNodeAddressDirectory::from_parts(b)) as Arc<dyn NodeAddressResolver>
    });
    Ok(RegistryHandles {
        staker_set,
        node_addresses,
        regions,
        operator_to_node,
        route,
    })
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
mod tests {
    use super::*;
    use alloy::primitives::{B256, Bytes, LogData};

    fn nid(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 32])
    }

    fn addr(byte: u8) -> Address {
        Address::from([byte; 20])
    }

    /// The four registry projections, as `full_snapshot` returns them.
    type Snapshot = (
        HashSet<NodeId>,
        HashMap<NodeId, Address>,
        HashMap<Address, NodeId>,
        HashMap<NodeId, String>,
    );

    /// Scripted [`RegistryChainReads`]: no provider, no chain.
    struct StubReads {
        node_id: std::result::Result<Option<(NodeId, bool)>, &'static str>,
        /// What `full_snapshot` returns; `Err` models an unreadable chain.
        snapshot: std::result::Result<Snapshot, &'static str>,
    }

    impl StubReads {
        fn new(node_id: std::result::Result<Option<(NodeId, bool)>, &'static str>) -> Self {
            Self {
                node_id,
                snapshot: Ok((
                    HashSet::new(),
                    HashMap::new(),
                    HashMap::new(),
                    HashMap::new(),
                )),
            }
        }

        fn with_snapshot(mut self, snapshot: std::result::Result<Snapshot, &'static str>) -> Self {
            self.snapshot = snapshot;
            self
        }
    }

    impl RegistryChainReads for StubReads {
        async fn node_id_of(&self, _operator: Address) -> Result<Option<(NodeId, bool)>> {
            match &self.node_id {
                Ok(v) => Ok(*v),
                Err(msg) => Err(anyhow::anyhow!(*msg)),
            }
        }

        async fn full_snapshot(
            &self,
        ) -> Result<(
            HashSet<NodeId>,
            HashMap<NodeId, Address>,
            HashMap<Address, NodeId>,
            HashMap<NodeId, String>,
        )> {
            match &self.snapshot {
                Ok(v) => Ok(v.clone()),
                Err(msg) => Err(anyhow::anyhow!(*msg)),
            }
        }
    }

    /// A sink plus handles on the projections it writes to.
    type Fixture = (
        RegistrySink<StubReads>,
        Arc<RwLock<HashSet<NodeId>>>,
        Option<Arc<RwLock<HashMap<NodeId, Address>>>>,
        Arc<RwLock<HashMap<Address, NodeId>>>,
        Arc<RwLock<HashMap<NodeId, String>>>,
        Arc<Metrics>,
    );

    /// A sink over empty projections. `bindings_on` mirrors
    /// `cache.node_to_node_pull_through_enabled`.
    fn sink(reads: StubReads, bindings_on: bool) -> Fixture {
        let active = Arc::new(RwLock::new(HashSet::new()));
        let bindings = bindings_on.then(|| Arc::new(RwLock::new(HashMap::new())));
        let operator_to_node = Arc::new(RwLock::new(HashMap::new()));
        let regions = Arc::new(RwLock::new(HashMap::new()));
        let metrics = Arc::new(Metrics::new());
        let s = RegistrySink {
            reads,
            active: Arc::clone(&active),
            bindings: bindings.clone(),
            operator_to_node: Arc::clone(&operator_to_node),
            regions: Arc::clone(&regions),
            metrics: Arc::clone(&metrics),
            resync_interval: REGISTRY_RESYNC_INTERVAL,
            last_resync: Some(Instant::now()),
        };
        (s, active, bindings, operator_to_node, regions, metrics)
    }

    fn ok_reads() -> StubReads {
        StubReads::new(Ok(None))
    }

    fn is_active(active: &Arc<RwLock<HashSet<NodeId>>>, id: NodeId) -> bool {
        active.read().is_ok_and(|g| g.contains(&id))
    }

    fn binding_of(
        bindings: Option<&Arc<RwLock<HashMap<NodeId, Address>>>>,
        id: NodeId,
    ) -> Option<Address> {
        bindings.and_then(|b| b.read().ok().and_then(|g| g.get(&id).copied()))
    }

    fn reverse_of(
        operator_to_node: &Arc<RwLock<HashMap<Address, NodeId>>>,
        operator: Address,
    ) -> Option<NodeId> {
        operator_to_node
            .read()
            .ok()
            .and_then(|g| g.get(&operator).copied())
    }

    /// `NodeRegistered(nodeId indexed, ethAddress indexed, ...)`.
    fn registered_log(id: NodeId, operator: Address) -> Log {
        let event = CapacityBond::NodeRegistered {
            nodeId: B256::from(*id.as_bytes()),
            ethAddress: operator,
            multiaddrs: Bytes::new(),
            regionHint: String::new(),
            bindingNonce: 1,
            registrationNonce: 1,
        };
        log_from(event.encode_log_data())
    }

    fn registered_log_region(id: NodeId, operator: Address, region: &str) -> Log {
        let event = CapacityBond::NodeRegistered {
            nodeId: B256::from(*id.as_bytes()),
            ethAddress: operator,
            multiaddrs: Bytes::new(),
            regionHint: region.to_string(),
            bindingNonce: 1,
            registrationNonce: 1,
        };
        log_from(event.encode_log_data())
    }

    fn deregistered_log(id: NodeId) -> Log {
        let event = CapacityBond::NodeDeregistered {
            nodeId: B256::from(*id.as_bytes()),
        };
        log_from(event.encode_log_data())
    }

    fn auto_ejected_log(id: NodeId) -> Log {
        let event = CapacityBond::NodeAutoEjected {
            nodeId: B256::from(*id.as_bytes()),
            remainingBond: U256::ZERO,
        };
        log_from(event.encode_log_data())
    }

    fn reinstated_log(operator: Address) -> Log {
        let event = CapacityBond::Reinstated { operator };
        log_from(event.encode_log_data())
    }

    fn log_from(data: LogData) -> Log {
        Log {
            inner: alloy::primitives::Log {
                address: Address::ZERO,
                data,
            },
            ..Default::default()
        }
    }

    /// The core of the merge: one log, one decode, both projections updated.
    #[tokio::test]
    async fn node_registered_updates_both_projections_from_one_decode() {
        let (mut s, active, bindings, _op, _regions, _m) = sink(ok_reads(), true);
        let r = s.apply(registered_log(nid(1), addr(9))).await;

        assert!(r.is_ok());
        assert!(is_active(&active, nid(1)), "registered node is active");
        assert_eq!(binding_of(bindings.as_ref(), nid(1)), Some(addr(9)));
    }

    /// With pull-through off the bindings projection does not exist, so the
    /// staker set still updates and nothing touches (or publishes) bindings.
    #[tokio::test]
    async fn node_registered_with_bindings_off_updates_only_the_staker_set() {
        let (mut s, active, bindings, _op, _regions, _m) = sink(ok_reads(), false);
        let r = s.apply(registered_log(nid(1), addr(9))).await;

        assert!(r.is_ok());
        assert!(is_active(&active, nid(1)));
        assert!(bindings.is_none(), "no bindings projection when gated off");
    }

    /// `deregisterNode` is the ONLY event that clears a binding.
    #[tokio::test]
    async fn node_deregistered_removes_from_both() {
        let (mut s, active, bindings, _op, _regions, _m) = sink(ok_reads(), true);
        let _ = s.apply(registered_log(nid(1), addr(9))).await;

        let r = s.apply(deregistered_log(nid(1))).await;

        assert!(r.is_ok());
        assert!(!is_active(&active, nid(1)));
        assert_eq!(binding_of(bindings.as_ref(), nid(1)), None);
    }

    /// The highest-value test in this module. Ejection flips `isActive` ONLY —
    /// the binding must survive, because the operator may still be owed payment
    /// on an open channel. A reflexive "union the arms" merge would drop it here.
    #[tokio::test]
    async fn node_auto_ejected_deactivates_but_keeps_the_binding() {
        let (mut s, active, bindings, _op, _regions, _m) = sink(ok_reads(), true);
        let _ = s.apply(registered_log(nid(1), addr(9))).await;

        let r = s.apply(auto_ejected_log(nid(1))).await;

        assert!(r.is_ok());
        assert!(!is_active(&active, nid(1)), "ejection deactivates");
        assert_eq!(
            binding_of(bindings.as_ref(), nid(1)),
            Some(addr(9)),
            "ejection must NOT clear the payout binding"
        );
    }

    /// `NodeRegistered` populates the reverse map alongside the forward binding.
    #[tokio::test]
    async fn node_registered_populates_reverse_map() {
        let (mut s, _active, _bindings, operator_to_node, _regions, _m) = sink(ok_reads(), true);
        let r = s.apply(registered_log(nid(1), addr(9))).await;

        assert!(r.is_ok());
        assert_eq!(reverse_of(&operator_to_node, addr(9)), Some(nid(1)));
    }

    /// `NodeDeregistered` removes the reverse entry, the same lifecycle as the
    /// forward binding.
    #[tokio::test]
    async fn node_deregistered_removes_from_reverse_map() {
        let (mut s, _active, _bindings, operator_to_node, _regions, _m) = sink(ok_reads(), true);
        let _ = s.apply(registered_log(nid(1), addr(9))).await;

        let r = s.apply(deregistered_log(nid(1))).await;

        assert!(r.is_ok());
        assert_eq!(reverse_of(&operator_to_node, addr(9)), None);
    }

    /// Ejection must NOT drop the reverse entry: the map is unfiltered, and the
    /// origin directory resolves an authorized operator's `NodeId` regardless of
    /// current liveness — the `StakerSet` applies the liveness filter separately
    /// at read time.
    #[tokio::test]
    async fn node_auto_ejected_keeps_reverse_binding() {
        let (mut s, _active, _bindings, operator_to_node, _regions, _m) = sink(ok_reads(), true);
        let _ = s.apply(registered_log(nid(1), addr(9))).await;

        let r = s.apply(auto_ejected_log(nid(1))).await;

        assert!(r.is_ok());
        assert_eq!(
            reverse_of(&operator_to_node, addr(9)),
            Some(nid(1)),
            "ejection must NOT clear the reverse binding"
        );
    }

    /// The reverse map is always built, unlike the pull-through-gated
    /// `bindings` projection.
    #[tokio::test]
    async fn reverse_map_built_even_with_pull_through_off() {
        let (mut s, _active, bindings, operator_to_node, _regions, _m) = sink(ok_reads(), false);
        let r = s.apply(registered_log(nid(1), addr(9))).await;

        assert!(r.is_ok());
        assert!(bindings.is_none(), "bindings is gated off");
        assert_eq!(
            reverse_of(&operator_to_node, addr(9)),
            Some(nid(1)),
            "the reverse map is always-on"
        );
    }

    /// The region projection mirrors binding lifecycle: set on registration,
    /// cleared on deregistration.
    #[tokio::test]
    async fn node_registered_captures_region_and_deregister_clears_it() {
        let (mut s, _active, _bindings, _op, regions, _m) = sink(ok_reads(), true);

        let _ = s.apply(registered_log_region(nid(1), addr(9), "DE")).await;
        assert_eq!(region_of(&regions, nid(1)), Some("DE".to_string()));

        let _ = s.apply(deregistered_log(nid(1))).await;
        assert_eq!(
            region_of(&regions, nid(1)),
            None,
            "deregister clears region"
        );
    }

    /// An empty `regionHint` means absence — no map entry, not an empty string
    /// entry.
    #[tokio::test]
    async fn empty_region_hint_is_not_stored() {
        let (mut s, _active, _bindings, _op, regions, _m) = sink(ok_reads(), true);
        // The existing `registered_log` helper sets `regionHint: String::new()`.
        let _ = s.apply(registered_log(nid(1), addr(9))).await;
        assert_eq!(region_of(&regions, nid(1)), None, "empty region is absence");
    }

    /// Ejection deactivates but must not clear the region, mirroring the
    /// binding: the region reflects the payout jurisdiction, not membership.
    #[tokio::test]
    async fn auto_eject_keeps_region() {
        let (mut s, _active, _bindings, _op, regions, _m) = sink(ok_reads(), true);
        let _ = s.apply(registered_log_region(nid(1), addr(9), "FR")).await;

        let _ = s.apply(auto_ejected_log(nid(1))).await;

        assert_eq!(
            region_of(&regions, nid(1)),
            Some("FR".to_string()),
            "ejection deactivates but keeps region, like the binding"
        );
    }

    /// Operator-indexed events resolve through `nodeIdOf` and never touch a
    /// binding.
    #[tokio::test]
    async fn reinstated_resolves_via_node_id_of_and_leaves_bindings_untouched() {
        let (mut s, active, bindings, _op, _regions, _m) =
            sink(StubReads::new(Ok(Some((nid(1), true)))), true);
        let _ = s.apply(registered_log(nid(1), addr(9))).await;
        let _ = s.apply(auto_ejected_log(nid(1))).await;

        let r = s.apply(reinstated_log(addr(9))).await;

        assert!(r.is_ok());
        assert!(is_active(&active, nid(1)), "nodeIdOf.active wins");
        assert_eq!(binding_of(bindings.as_ref(), nid(1)), Some(addr(9)));
    }

    /// The canonical `nodeIdOf.active` beats what the event implies when a later
    /// transition raced it.
    #[tokio::test]
    async fn node_id_of_disagreeing_with_the_event_wins() {
        // `Reinstated` implies active, but nodeIdOf says otherwise.
        let (mut s, active, _b, _op, _regions, _m) =
            sink(StubReads::new(Ok(Some((nid(1), false)))), true);
        let _ = s.apply(registered_log(nid(1), addr(9))).await;

        let r = s.apply(reinstated_log(addr(9))).await;

        assert!(r.is_ok());
        assert!(!is_active(&active, nid(1)), "canonical nodeIdOf wins");
    }

    /// A `nodeIdOf` failure is counted and skipped — it must NOT return `Err`,
    /// which would back the whole shared loop off (and now that the loop is
    /// shared, would stall the bindings projection too).
    #[tokio::test]
    async fn node_id_of_failure_bumps_resolve_failure_and_returns_ok() {
        let (mut s, _a, _b, _op, _regions, metrics) = sink(StubReads::new(Err("rpc down")), true);
        let r = s.apply(reinstated_log(addr(9))).await;

        assert!(r.is_ok(), "a resolve failure must not fail the tick");
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_staker_set_watcher_resolve_failures_total 1"),
            "the drop must surface on its own counter:\n{text}"
        );
    }

    /// An unbound operator (`bytes32(0)`) is ignored: binding precedes activation.
    #[tokio::test]
    async fn unbound_operator_event_is_ignored() {
        let (mut s, active, _b, _op, _regions, _m) = sink(StubReads::new(Ok(None)), true);
        let r = s.apply(reinstated_log(addr(9))).await;

        assert!(r.is_ok());
        assert_eq!(active.read().map_or(1, |g| g.len()), 0);
    }

    /// An undecodable log is skipped, not surfaced as `Err` — a deterministic
    /// re-scan of one would otherwise hot-loop the cursor forever.
    #[tokio::test]
    async fn undecodable_log_is_skipped_and_returns_ok() {
        let (mut s, active, _b, _op, _regions, _m) = sink(ok_reads(), true);
        let mut log = registered_log(nid(1), addr(9));
        log.inner.data.data = Bytes::from_static(b"garbage");

        let r = s.apply(log).await;

        assert!(r.is_ok(), "undecodable log must not fail the tick");
        assert!(!is_active(&active, nid(1)));
    }

    /// The staker-set family tracks the one shared loop. It is unconditional:
    /// this loop feeds the bindings projection too, so its health is the same
    /// health whether or not pull-through is on.
    ///
    /// The third call is what gives the `established` leg coverage.
    /// `backoff_started` is edge-triggered on `down_since` being unset, so the
    /// re-arm only counts if `established` actually closed the window. Asserting
    /// `down_seconds 0` after an `established` instead would prove nothing: an
    /// immediate scrape reads `0` even with the window open, because the elapsed
    /// time floors to zero seconds.
    #[test]
    fn watcher_hooks_track_the_shared_loop() {
        let metrics = Arc::new(Metrics::new());
        let established = metric_hook(&metrics, Metrics::staker_set_watcher_cycle_established);
        let backoff = metric_hook(&metrics, Metrics::staker_set_watcher_backoff_started);
        backoff();
        established();
        backoff();

        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_staker_set_watcher_restarts_total 2"),
            "established() must close the drift window so a later backoff re-arms:\n{text}"
        );
    }

    // ── Periodic re-enumeration ─────────────────────────────────────────────
    //
    // The self-healing leg. Every other input to these projections is an event,
    // and a missed or orphaned event never surfaces as an error — so without
    // this, a drifted set is only repaired by restarting the process.

    #[allow(clippy::type_complexity)] // (active set, bindings, reverse map, regions) — the four projections
    fn snapshot_of(
        ids: &[u8],
        addrs: &[u8],
    ) -> (
        HashSet<NodeId>,
        HashMap<NodeId, Address>,
        HashMap<Address, NodeId>,
        HashMap<NodeId, String>,
    ) {
        let active: HashSet<NodeId> = ids.iter().map(|b| nid(*b)).collect();
        let bindings: HashMap<NodeId, Address> =
            addrs.iter().map(|b| (nid(*b), addr(*b))).collect();
        let operator_to_node: HashMap<Address, NodeId> =
            addrs.iter().map(|b| (addr(*b), nid(*b))).collect();
        (active, bindings, operator_to_node, HashMap::new())
    }

    /// A due resync replaces both projections wholesale, so an entry the event
    /// tail dropped comes back and one it wrongly added goes away.
    #[tokio::test]
    async fn resync_replaces_both_projections() {
        let reads = StubReads::new(Ok(None)).with_snapshot(Ok(snapshot_of(&[2, 3], &[2, 3])));
        let (mut sink, active, bindings, operator_to_node, _regions, _m) = sink(reads, true);
        // Seed a stale view: 1 is gone from chain, 2 is missing locally.
        with_write(&active, "t", |set| {
            set.insert(nid(1));
        });
        sink.last_resync = None; // force the resync on this tick

        sink.on_tick_complete().await.unwrap();

        let got = active.read().unwrap().clone();
        assert_eq!(
            got,
            snapshot_of(&[2, 3], &[]).0,
            "stale entry dropped, missing one restored"
        );
        let b = bindings.unwrap();
        assert_eq!(b.read().unwrap().len(), 2, "bindings are replaced too");
        assert_eq!(
            operator_to_node.read().unwrap().len(),
            2,
            "the reverse map is replaced too"
        );
    }

    /// `full_snapshot` derives the reverse map as the inverse of `bindings` for
    /// every registered operator.
    #[tokio::test]
    async fn resync_derives_reverse_map_as_inverse_of_bindings() {
        let reads = StubReads::new(Ok(None)).with_snapshot(Ok(snapshot_of(&[2, 3], &[2, 3])));
        let (mut sink, _active, _bindings, operator_to_node, _regions, _m) = sink(reads, true);
        sink.last_resync = None;

        sink.on_tick_complete().await.unwrap();

        let rev = operator_to_node.read().unwrap();
        assert_eq!(rev.get(&addr(2)).copied(), Some(nid(2)));
        assert_eq!(rev.get(&addr(3)).copied(), Some(nid(3)));
        assert_eq!(rev.len(), 2, "one entry per registered operator");
    }

    /// A failed read must leave the previous projections intact. Emptying the
    /// staker set would make the node treat every peer as unstaked.
    #[tokio::test]
    async fn resync_failure_keeps_the_previous_projections() {
        let reads = StubReads::new(Ok(None)).with_snapshot(Err("rpc down"));
        let (mut sink, active, _bindings, _op, _regions, _m) = sink(reads, true);
        with_write(&active, "t", |set| {
            set.insert(nid(1));
        });
        sink.last_resync = None;

        sink.on_tick_complete().await.unwrap();

        assert!(
            active.read().unwrap().contains(&nid(1)),
            "a failed resync must not clear the set"
        );
    }

    /// Not-yet-due ticks must not re-read: the watcher ticks every few seconds,
    /// so an ungated resync would hammer the RPC with a paginated enumeration.
    #[tokio::test]
    async fn resync_is_cadence_gated() {
        let reads = StubReads::new(Ok(None)).with_snapshot(Ok(snapshot_of(&[9], &[9])));
        let (mut sink, active, _bindings, _op, _regions, _m) = sink(reads, true);
        // `sink()` stamps `last_resync` to now, so nothing is due yet.
        sink.on_tick_complete().await.unwrap();
        assert!(
            active.read().unwrap().is_empty(),
            "a resync ran despite not being due"
        );
    }

    /// A failing read still stamps the clock, so retries follow the resync
    /// cadence rather than the (seconds-scale) watcher tick.
    #[tokio::test]
    async fn failed_resync_still_stamps_the_clock() {
        let reads = StubReads::new(Ok(None)).with_snapshot(Err("rpc down"));
        let (mut sink, _active, _bindings, _op, _regions, _m) = sink(reads, true);
        sink.last_resync = None;

        sink.on_tick_complete().await.unwrap();

        assert!(
            sink.last_resync.is_some(),
            "a failed resync must still stamp, or it retries every tick"
        );
    }
}
