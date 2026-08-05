//! One `CapacityBond` watcher feeding both registry projections (#1110).
//!
//! [`ChainStakerSet`] (membership: `NodeId → active?`) and
//! [`ChainNodeAddressDirectory`] (bindings: `NodeId → operator address`) are both
//! derived from the same `CapacityBond` contract. They used to bootstrap and
//! watch it independently: two paginated `getRegisteredNodes` reads at boot, and
//! two `eth_getLogs` poll loops thereafter, filtering the same address — with
//! node-address's two topics a strict *subset* of staker-set's five. This module
//! runs one enumeration and one loop, demuxing to both projections.
//!
//! # What actually differs between them
//!
//! Only the **enumeration**. The live event arms are a clean union:
//! `NodeRegistered` inserts into *both* projections unfiltered — staker-set's
//! live arm applies no `isActive` check either (its filter is bootstrap-only). It
//! is tempting to read "staker-set is the filtered view, node-address is the
//! unfiltered one" as a live-path difference and encode it in the sink; that
//! would be wrong, and would drop registrations from the active set.
//!
//! At bootstrap they genuinely diverge:
//!
//! - `active` takes the per-entry `active[i]` that `getRegisteredNodes` computes
//!   on-chain (`isActive(operator)`: registered AND bond ≥ minBond AND no
//!   unbonding AND not ejected) alongside the `_registeredAddrs` page.
//! - `bindings` applies no filter: an operator mid-unbonding has `isActive =
//!   false` but is still payable, so its binding must survive.
//!
//! # Fatality
//!
//! Staker-set bootstrap is fatal; node-address bootstrap was non-fatal
//! (pull-through is opportunistic). Sharing the read *eliminates* rather than
//! violates that asymmetry: `getRegisteredNodes` failure was already fatal,
//! because the unconditional staker-set bootstrap ran first and propagated it —
//! the node-address bootstrap was never reached. With the read shared, the
//! bindings projection has **no RPC of its own**: it is derived from page data
//! already in hand and cannot fail independently. So no node that boots today
//! loses pull-through, and none that boots today starts failing.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use crate::chain_events::resumable_watcher::{
    self, CursorStart, LogSink, WatcherConfig, WatcherHandle,
};
use crate::chain_events::shared_head::HeadSource;
use crate::chain_events::timed;
use crate::dht::chain_projection::with_write;
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
    /// The shared watcher, exposed so the runtime can drive graceful shutdown in
    /// its deliberate order (this loop stops *after* `router.shutdown` because
    /// its staker-set projection gates DHT admission during drain). Also held
    /// inside both façades' projections, so the task lives until the last of the
    /// three drops. `pub(crate)`, since `WatcherHandle` is crate-private and only
    /// the runtime drives shutdown.
    pub(crate) watcher: Arc<WatcherHandle>,
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
    fn full_snapshot(
        &self,
    ) -> impl Future<Output = Result<(HashSet<NodeId>, HashMap<NodeId, Address>)>> + Send;
}

/// Production [`RegistryChainReads`] over the live contract.
struct ContractReads<P: Provider + Clone> {
    registry: CapacityBond::CapacityBondInstance<P>,
}

impl<P: Provider + Clone> RegistryChainReads for ContractReads<P> {
    async fn full_snapshot(&self) -> Result<(HashSet<NodeId>, HashMap<NodeId, Address>)> {
        bootstrap_registry(&self.registry).await
    }

    async fn node_id_of(&self, operator: Address) -> Result<Option<(NodeId, bool)>> {
        // Bounded explicitly: `WatcherConfig::rpc_call_timeout` covers only the
        // loop's own `get_logs`, so a sink's follow-up read stays unbounded unless
        // it wraps itself (the `blacklist_watcher::scope_check` precedent). That
        // matters more now than it did per-watcher: one stalled `nodeIdOf` used to
        // wedge just the staker-set loop, but this loop also feeds the bindings
        // projection. A timeout surfaces as `Err`, which `on_operator_change`
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
    fn on_registered(&self, node_id: NodeId, eth_address: Address) {
        apply_change(&self.active, &self.metrics, StakerChange::Active(node_id));
        if let Some(bindings) = &self.bindings {
            set_binding(bindings, &self.metrics, node_id, eth_address);
        }
    }

    /// `NodeDeregistered`: the binding is cleared only here — `registerNode` sets
    /// it and `deregisterNode` clears it. Bond/unbonding/ejection transitions flip
    /// `isActive` without touching the binding.
    fn on_deregistered(&self, node_id: NodeId) {
        apply_change(&self.active, &self.metrics, StakerChange::Inactive(node_id));
        if let Some(bindings) = &self.bindings {
            remove_binding(bindings, &self.metrics, &node_id);
        }
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
                        self.on_registered(NodeId::from_bytes(event.nodeId.0), event.ethAddress);
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

        let (active, bindings) = match self.reads.full_snapshot().await {
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

/// One paginated `getRegisteredNodes` read feeding both projections.
///
/// Always returns both maps and lets the caller drop the unwanted one — a
/// `want_bindings: bool` parameter would be a boolean trap, and building the map
/// is free while already iterating the page. See the module doc for why `active`
/// is `isActive`-filtered and `bindings` is not: `getRegisteredNodes` computes
/// `isActive` per entry on-chain and returns it as a parallel `active[]`, so a
/// single call per page derives both projections with no per-entry round-trip.
async fn bootstrap_registry<P>(
    registry: &CapacityBond::CapacityBondInstance<P>,
) -> Result<(HashSet<NodeId>, HashMap<NodeId, Address>)>
where
    P: Provider + Clone,
{
    let mut active = HashSet::new();
    let mut bindings = HashMap::new();
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
            if is_active {
                active.insert(node_id);
            }
        }
        offset = offset.saturating_add(page_len);
    }
    Ok((active, bindings))
}

/// Enumerate `CapacityBond` once, then spawn the single watcher that keeps both
/// projections current.
///
/// `track_node_addresses` mirrors `cache.node_to_node_pull_through_enabled`.
pub async fn bootstrap<P>(
    provider: P,
    registry_addr: Address,
    event_poll_interval: Duration,
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
    let (initial_active, initial_bindings) =
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

    let sink = RegistrySink {
        reads: ContractReads {
            registry: registry.clone(),
        },
        active: Arc::clone(&active),
        bindings: bindings.clone(),
        metrics: Arc::clone(&metrics),
        resync_interval: REGISTRY_RESYNC_INTERVAL,
        // The bootstrap enumeration just ran, so the first backstop resync is
        // due one interval from now rather than on the first tick.
        last_resync: Some(Instant::now()),
    };
    let cfg = WatcherConfig::new(
        head,
        Filter::new().address(registry_addr).event_signature(vec![
            CapacityBond::NodeRegistered::SIGNATURE_HASH,
            CapacityBond::NodeDeregistered::SIGNATURE_HASH,
            CapacityBond::NodeAutoEjected::SIGNATURE_HASH,
            CapacityBond::Reinstated::SIGNATURE_HASH,
            CapacityBond::UnbondingRequested::SIGNATURE_HASH,
        ]),
        // Seed the live tail from the enumeration snapshot head; the staker set
        // is rebuilt from that enumeration each boot, so there is no durable
        // cursor to persist.
        CursorStart::Seeded {
            at: snapshot_block,
            persist: None,
        },
        event_poll_interval,
        "capacity-bond",
    )
    // `staker_set_watcher_*` is the shared `capacity-bond` loop's health, and it
    // covers the bindings projection too — one loop feeds both, so there is one
    // thing to report. A parallel `node_address_watcher_*` family used to be
    // fired alongside these and was retired in #1231: since #1226 collapsed the
    // two loops into one it was a perfectly-correlated shadow, reporting the same
    // outage twice. It was kept at the time on the grounds that retiring it would
    // break existing dashboards and alerts — no dashboard, alert, ADR, or runbook
    // in this repo ever referenced it — and that a gauge frozen at `0` forever is
    // an alert that can never fire. That second argument was the stronger one,
    // and it cut the other way: the family was gated on the bindings projection
    // existing, so a pull-through-off node reported exactly that frozen `0`.
    .on_established(metric_hook(
        &metrics,
        Metrics::staker_set_watcher_cycle_established,
    ))
    .on_backoff(metric_hook(
        &metrics,
        Metrics::staker_set_watcher_backoff_started,
    ))
    .on_tick_success(metric_hook(&metrics, Metrics::staker_set_watcher_tick))
    .on_task_panic(metric_hook(
        &metrics,
        Metrics::staker_set_watcher_task_panicked,
    ));
    // One task, one `WatcherHandle`, shared by both façades and the runtime: it
    // lives while any of the three holds it and aborts when the last drops.
    // Strictly safer than the old shape, where dropping the resolver killed only
    // its own loop. This sink observes no shutdown token, so it ignores the one
    // `spawn` mints (`|_| sink`).
    let watcher = Arc::new(resumable_watcher::spawn(provider, cfg, move |_| sink));

    let staker_set: Arc<dyn StakerSet> =
        Arc::new(ChainStakerSet::from_parts(active, Arc::clone(&watcher)));
    let node_addresses = bindings.map(|b| {
        Arc::new(ChainNodeAddressDirectory::from_parts(
            b,
            Arc::clone(&watcher),
        )) as Arc<dyn NodeAddressResolver>
    });
    Ok(RegistryHandles {
        staker_set,
        node_addresses,
        watcher,
    })
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

    /// Scripted [`RegistryChainReads`]: no provider, no chain.
    struct StubReads {
        node_id: std::result::Result<Option<(NodeId, bool)>, &'static str>,
        /// What `full_snapshot` returns; `Err` models an unreadable chain.
        snapshot: std::result::Result<(HashSet<NodeId>, HashMap<NodeId, Address>), &'static str>,
    }

    impl StubReads {
        fn new(node_id: std::result::Result<Option<(NodeId, bool)>, &'static str>) -> Self {
            Self {
                node_id,
                snapshot: Ok((HashSet::new(), HashMap::new())),
            }
        }

        fn with_snapshot(
            mut self,
            snapshot: std::result::Result<
                (HashSet<NodeId>, HashMap<NodeId, Address>),
                &'static str,
            >,
        ) -> Self {
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

        async fn full_snapshot(&self) -> Result<(HashSet<NodeId>, HashMap<NodeId, Address>)> {
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
        Arc<Metrics>,
    );

    /// A sink over empty projections. `bindings_on` mirrors
    /// `cache.node_to_node_pull_through_enabled`.
    fn sink(reads: StubReads, bindings_on: bool) -> Fixture {
        let active = Arc::new(RwLock::new(HashSet::new()));
        let bindings = bindings_on.then(|| Arc::new(RwLock::new(HashMap::new())));
        let metrics = Arc::new(Metrics::new());
        let s = RegistrySink {
            reads,
            active: Arc::clone(&active),
            bindings: bindings.clone(),
            metrics: Arc::clone(&metrics),
            resync_interval: REGISTRY_RESYNC_INTERVAL,
            last_resync: Some(Instant::now()),
        };
        (s, active, bindings, metrics)
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
        let (mut s, active, bindings, _m) = sink(ok_reads(), true);
        let r = s.apply(registered_log(nid(1), addr(9))).await;

        assert!(r.is_ok());
        assert!(is_active(&active, nid(1)), "registered node is active");
        assert_eq!(binding_of(bindings.as_ref(), nid(1)), Some(addr(9)));
    }

    /// With pull-through off the bindings projection does not exist, so the
    /// staker set still updates and nothing touches (or publishes) bindings.
    #[tokio::test]
    async fn node_registered_with_bindings_off_updates_only_the_staker_set() {
        let (mut s, active, bindings, _m) = sink(ok_reads(), false);
        let r = s.apply(registered_log(nid(1), addr(9))).await;

        assert!(r.is_ok());
        assert!(is_active(&active, nid(1)));
        assert!(bindings.is_none(), "no bindings projection when gated off");
    }

    /// `deregisterNode` is the ONLY event that clears a binding.
    #[tokio::test]
    async fn node_deregistered_removes_from_both() {
        let (mut s, active, bindings, _m) = sink(ok_reads(), true);
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
        let (mut s, active, bindings, _m) = sink(ok_reads(), true);
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

    /// Operator-indexed events resolve through `nodeIdOf` and never touch a
    /// binding.
    #[tokio::test]
    async fn reinstated_resolves_via_node_id_of_and_leaves_bindings_untouched() {
        let (mut s, active, bindings, _m) = sink(StubReads::new(Ok(Some((nid(1), true)))), true);
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
        let (mut s, active, _b, _m) = sink(StubReads::new(Ok(Some((nid(1), false)))), true);
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
        let (mut s, _a, _b, metrics) = sink(StubReads::new(Err("rpc down")), true);

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
        let (mut s, active, _b, _m) = sink(StubReads::new(Ok(None)), true);

        let r = s.apply(reinstated_log(addr(9))).await;

        assert!(r.is_ok());
        assert_eq!(active.read().map_or(1, |g| g.len()), 0);
    }

    /// An undecodable log is skipped, not surfaced as `Err` — a deterministic
    /// re-scan of one would otherwise hot-loop the cursor forever.
    #[tokio::test]
    async fn undecodable_log_is_skipped_and_returns_ok() {
        let (mut s, active, _b, _m) = sink(ok_reads(), true);
        let mut log = registered_log(nid(1), addr(9));
        log.inner.data.data = Bytes::from_static(b"garbage");

        let r = s.apply(log).await;

        assert!(r.is_ok(), "undecodable log must not fail the tick");
        assert!(!is_active(&active, nid(1)));
    }

    /// The staker-set family tracks the one shared loop. It is unconditional:
    /// this loop feeds the bindings projection too, so its health is the same
    /// health whether or not pull-through is on. (Before #1231 a second,
    /// perfectly-correlated `node_address_watcher_*` family was fired alongside
    /// it, gated on the projection existing — which is what made the gate, and
    /// a second test for the gated-off case, necessary.)
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

    fn snapshot_of(ids: &[u8], addrs: &[u8]) -> (HashSet<NodeId>, HashMap<NodeId, Address>) {
        let active: HashSet<NodeId> = ids.iter().map(|b| nid(*b)).collect();
        let bindings: HashMap<NodeId, Address> =
            addrs.iter().map(|b| (nid(*b), addr(*b))).collect();
        (active, bindings)
    }

    /// A due resync replaces both projections wholesale, so an entry the event
    /// tail dropped comes back and one it wrongly added goes away.
    #[tokio::test]
    async fn resync_replaces_both_projections() {
        let reads = StubReads::new(Ok(None)).with_snapshot(Ok(snapshot_of(&[2, 3], &[2, 3])));
        let (mut sink, active, bindings, _m) = sink(reads, true);

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
    }

    /// A failed read must leave the previous projections intact. Emptying the
    /// staker set would make the node treat every peer as unstaked.
    #[tokio::test]
    async fn resync_failure_keeps_the_previous_projections() {
        let reads = StubReads::new(Ok(None)).with_snapshot(Err("rpc down"));
        let (mut sink, active, _bindings, _m) = sink(reads, true);
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
        let (mut sink, active, _bindings, _m) = sink(reads, true);
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
        let (mut sink, _active, _bindings, _m) = sink(reads, true);
        sink.last_resync = None;

        sink.on_tick_complete().await.unwrap();

        assert!(
            sink.last_resync.is_some(),
            "a failed resync must still stamp, or it retries every tick"
        );
    }
}
