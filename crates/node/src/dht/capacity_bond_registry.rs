//! One `CapacityBond` watcher feeding both registry projections (#1110).
//!
//! [`ChainStakerSet`] (membership: `NodeId → active?`) and
//! [`ChainNodeAddressDirectory`] (bindings: `NodeId → operator address`) are both
//! derived from the same `CapacityBond` contract. They used to bootstrap and
//! watch it independently: two paginated `getActiveNodes` reads at boot, and two
//! `eth_getLogs` poll loops thereafter, filtering the same address — with
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
//! - `active` applies `isActive(operator)` per entry (the `getActiveNodes` page is
//!   the *unfiltered* `_registeredAddrs` array per the contract's own comment).
//! - `bindings` applies no filter: an operator mid-unbonding has `isActive =
//!   false` but is still payable, so its binding must survive.
//!
//! # Fatality
//!
//! Staker-set bootstrap is fatal; node-address bootstrap was non-fatal
//! (pull-through is opportunistic). Sharing the read *eliminates* rather than
//! violates that asymmetry: `getActiveNodes`/`isActive` failure was already fatal,
//! because the unconditional staker-set bootstrap ran first and propagated it —
//! the node-address bootstrap was never reached. With the read shared, the
//! bindings projection has **no RPC of its own**: it is derived from page data
//! already in hand and cannot fail independently. So no node that boots today
//! loses pull-through, and none that boots today starts failing.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain_events::resumable_watcher::{
    self, CursorPolicy, LogSink, WatcherConfig, WatcherHook,
};
use crate::chain_events::shared_head::HeadSource;
use crate::chain_events::{
    AbortOnDrop, MAX_BACKFILL_BLOCK_SPAN, WATCHER_INITIAL_BACKOFF, WATCHER_MAX_BACKOFF, timed,
};
use crate::dht::chain_staker_set::{ChainStakerSet, apply_change};
use crate::dht::node_address::{
    ChainNodeAddressDirectory, NodeAddressResolver, remove_binding, set_binding,
};
use crate::dht::routing::NodeId;
use crate::dht::staker_set::{StakerChange, StakerSet};
use crate::metrics::Metrics;
use decdn_common::redact::sanitize_rpc_display;
use decdn_incentive::capacity_bond::CapacityBond;

/// Page size for the paginated `getActiveNodes` read. Matches ADR 019 § Step
/// 3.3's worked-example limit.
const PAGE_SIZE: u64 = 100;

/// Capacity of the `StakerChange` broadcast channel. Sized so a slow subscriber
/// can fall behind by a single bootstrap cycle (~100 changes in the bursty
/// re-sync window) without forcing a lagging recv into the `Lagged` arm.
const CHANGES_CHANNEL_CAPACITY: usize = 128;

/// Both projections, plus the shared watcher that keeps them current.
///
/// `node_addresses` is `None` when `cache.node_to_node_pull_through_enabled` is
/// off: the bindings projection is not built at all, so its
/// `node_address_directory_size` gauge stays absent rather than appearing on
/// every default node meaning something subtly different ("registered nodes"
/// rather than "resolvable payout targets").
#[derive(Debug)]
pub struct RegistryHandles {
    pub staker_set: Arc<dyn StakerSet>,
    pub node_addresses: Option<Arc<dyn NodeAddressResolver>>,
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
}

/// Production [`RegistryChainReads`] over the live contract.
struct ContractReads<P: Provider + Clone> {
    registry: CapacityBond::CapacityBondInstance<P>,
}

impl<P: Provider + Clone> RegistryChainReads for ContractReads<P> {
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
    pub(crate) changes_tx: broadcast::Sender<StakerChange>,
    /// `None` when pull-through is off — see [`RegistryHandles::node_addresses`].
    pub(crate) bindings: Option<Arc<RwLock<HashMap<NodeId, Address>>>>,
    pub(crate) metrics: Arc<Metrics>,
}

impl<R: RegistryChainReads> RegistrySink<R> {
    /// `NodeRegistered`: active insert (unfiltered — see the module doc) AND a
    /// binding insert.
    fn on_registered(&self, node_id: NodeId, eth_address: Address) {
        apply_change(
            &self.active,
            &self.changes_tx,
            &self.metrics,
            StakerChange::Active(node_id),
        );
        if let Some(bindings) = &self.bindings {
            set_binding(bindings, &self.metrics, node_id, eth_address);
        }
    }

    /// `NodeDeregistered`: the binding is cleared only here — `registerNode` sets
    /// it and `deregisterNode` clears it. Bond/unbonding/ejection transitions flip
    /// `isActive` without touching the binding.
    fn on_deregistered(&self, node_id: NodeId) {
        apply_change(
            &self.active,
            &self.changes_tx,
            &self.metrics,
            StakerChange::Inactive(node_id),
        );
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
                    err = %sanitize_rpc_display(&*err),
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
        apply_change(&self.active, &self.changes_tx, &self.metrics, change);
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
                    Ok(event) => apply_change(
                        &self.active,
                        &self.changes_tx,
                        &self.metrics,
                        StakerChange::Inactive(event.nodeId.0.into()),
                    ),
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
}

/// One paginated `getActiveNodes` read feeding both projections.
///
/// Always returns both maps and lets the caller drop the unwanted one — a
/// `want_bindings: bool` parameter would be a boolean trap, and building the map
/// is free while already iterating the page. See the module doc for why `active`
/// is `isActive`-filtered (at the cost of an N+1 per entry; `Multicall3` batching
/// remains the follow-up) and `bindings` is not.
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
            let node_id = NodeId::from_bytes(node.nodeId.0);
            bindings.insert(node_id, node.ethAddress);
            let is_active = registry
                .isActive(node.ethAddress)
                .call()
                .await
                .with_context(|| format!("isActive({operator})", operator = node.ethAddress))?;
            if is_active {
                active.insert(node_id);
            }
        }
        offset = offset.saturating_add(page_len);
    }
    Ok((active, bindings))
}

/// Fire the staker-set gauges, plus the node-address ones only when that
/// projection exists.
///
/// Both families are kept rather than collapsed to one: they now describe the
/// same loop, so they are perfectly correlated — but retiring one would break
/// existing dashboards and alerts, and freezing `node_address_watcher_down_seconds`
/// at 0 forever would be an alert that can never fire, which is worse than
/// deleting it. The `bindings_live` gate stops a pull-through-off node reporting
/// health for a directory it does not have.
fn hooks(metrics: &Arc<Metrics>, bindings_live: bool) -> (WatcherHook, WatcherHook) {
    let established = {
        let metrics = Arc::clone(metrics);
        Box::new(move || {
            metrics.staker_set_watcher_cycle_established();
            if bindings_live {
                metrics.node_address_watcher_cycle_established();
            }
        }) as WatcherHook
    };
    let backoff = {
        let metrics = Arc::clone(metrics);
        Box::new(move || {
            metrics.staker_set_watcher_backoff_started();
            if bindings_live {
                metrics.node_address_watcher_backoff_started();
            }
        }) as WatcherHook
    };
    (established, backoff)
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
            format!("paginated getActiveNodes from CapacityBond at {registry_addr}")
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

    let (changes_tx, _) = broadcast::channel(CHANGES_CHANNEL_CAPACITY);
    let active = Arc::new(RwLock::new(initial_active));
    let bindings = track_node_addresses.then(|| Arc::new(RwLock::new(initial_bindings)));

    let (on_established, on_backoff) = hooks(&metrics, bindings.is_some());
    let sink = RegistrySink {
        reads: ContractReads {
            registry: registry.clone(),
        },
        active: Arc::clone(&active),
        changes_tx: changes_tx.clone(),
        bindings: bindings.clone(),
        metrics: Arc::clone(&metrics),
    };
    let cfg = WatcherConfig {
        head,
        filter: Filter::new().address(registry_addr).event_signature(vec![
            CapacityBond::NodeRegistered::SIGNATURE_HASH,
            CapacityBond::NodeDeregistered::SIGNATURE_HASH,
            CapacityBond::NodeAutoEjected::SIGNATURE_HASH,
            CapacityBond::Reinstated::SIGNATURE_HASH,
            CapacityBond::UnbondingRequested::SIGNATURE_HASH,
        ]),
        from_block: 0,
        poll_interval: event_poll_interval,
        confirmations: 0,
        reorg_margin: 0,
        // Live-from-head, but still chunk `[cursor, head]` so a long lag (RPC
        // outage / rate-limit) recovers in bounded windows instead of one
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
        seed_cursor: Some(snapshot_block),
        label: "capacity-bond",
        on_established: Some(on_established),
        on_backoff: Some(on_backoff),
    };
    // One task, one `AbortOnDrop`, shared by both façades: it lives while either
    // does and aborts when the last is dropped. Strictly safer than the old shape,
    // where dropping the resolver killed only its own loop.
    let watcher = Arc::new(AbortOnDrop(tokio::spawn(resumable_watcher::run(
        provider, cfg, sink,
    ))));

    let staker_set: Arc<dyn StakerSet> = Arc::new(ChainStakerSet::from_parts(
        active,
        changes_tx,
        Arc::clone(&watcher),
    ));
    let node_addresses = bindings.map(|b| {
        Arc::new(ChainNodeAddressDirectory::from_parts(b, watcher)) as Arc<dyn NodeAddressResolver>
    });
    Ok(RegistryHandles {
        staker_set,
        node_addresses,
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
    struct StubReads(std::result::Result<Option<(NodeId, bool)>, &'static str>);

    impl RegistryChainReads for StubReads {
        async fn node_id_of(&self, _operator: Address) -> Result<Option<(NodeId, bool)>> {
            match &self.0 {
                Ok(v) => Ok(*v),
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
        let (changes_tx, _rx) = broadcast::channel(16);
        let s = RegistrySink {
            reads,
            active: Arc::clone(&active),
            changes_tx,
            bindings: bindings.clone(),
            metrics: Arc::clone(&metrics),
        };
        (s, active, bindings, metrics)
    }

    fn ok_reads() -> StubReads {
        StubReads(Ok(None))
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
        let (mut s, active, bindings, _m) = sink(StubReads(Ok(Some((nid(1), true)))), true);
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
        let (mut s, active, _b, _m) = sink(StubReads(Ok(Some((nid(1), false)))), true);
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
        let (mut s, _a, _b, metrics) = sink(StubReads(Err("rpc down")), true);

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
        let (mut s, active, _b, _m) = sink(StubReads(Ok(None)), true);

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

    /// Both gauge families track the one shared loop when bindings are live.
    #[test]
    fn composed_hooks_bump_both_families_when_bindings_live() {
        let metrics = Arc::new(Metrics::new());
        let (established, backoff) = hooks(&metrics, true);
        backoff();
        established();

        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_staker_set_watcher_restarts_total 1"),
            "staker-set family must track the shared loop:\n{text}"
        );
        assert!(
            text.lines()
                .any(|l| l == "decdn_node_address_watcher_restarts_total 1"),
            "node-address family must track the same shared loop:\n{text}"
        );
    }

    /// ...and only staker-set's when the directory does not exist, so a
    /// pull-through-off node never reports health for a projection it lacks.
    #[test]
    fn composed_hooks_bump_only_staker_set_when_bindings_absent() {
        let metrics = Arc::new(Metrics::new());
        let (established, backoff) = hooks(&metrics, false);
        backoff();
        established();

        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_staker_set_watcher_restarts_total 1"),
            "staker-set family still tracks the loop:\n{text}"
        );
        assert!(
            text.lines()
                .any(|l| l == "decdn_node_address_watcher_restarts_total 0"),
            "node-address family must stay flat when no directory exists:\n{text}"
        );
    }
}
