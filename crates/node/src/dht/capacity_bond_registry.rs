//! One `CapacityBond` watcher feeding five registry projections.
//!
//! [`ChainStakerSet`] (membership: `NodeId → active?`), [`ChainNodeAddressDirectory`]
//! (bindings: `NodeId → operator address`), the region map (`NodeId → regionHint`),
//! the operator reverse map (`operator address → NodeId`), and the
//! [`DialAddrDirectory`] (`NodeId → registry multiaddrs`, as iroh direct-dial
//! addresses) are all derived from the same `CapacityBond` contract. This module
//! runs one enumeration and one `eth_getLogs` loop over the shared
//! `CapacityBond` address, demuxing to all five projections; node-address's two
//! topics are a strict *subset* of the route's eight. `RegionUpdated` feeds the
//! region map alone, and `NodeMultiaddrUpdated` the dial-address directory alone.
//!
//! Bindings is gated on `cache.node_to_node_pull_through_enabled`: when
//! pull-through is off, the bindings projection is not built at all. Regions,
//! the operator reverse map, and the dial-address directory are always built,
//! regardless of pull-through — the ADR-030 selection penalty needs the region
//! map unconditionally, the chain-backed origin directory needs the reverse map
//! to resolve operators locally with no `nodeIdOf` RPC, and every node dial
//! (DHT included) resolves direct addresses through the dial-address directory.
//!
//! # What actually differs between the projections
//!
//! Mostly the **enumeration**. The live event arms are close to a clean union:
//! `NodeRegistered` inserts into active, bindings (if built), regions (if
//! non-empty), the reverse map, and the dial-address directory (if any address
//! decodes), all unfiltered — staker-set's live arm
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
//! - `regions`, the reverse map, and the dial-address directory are likewise
//!   unfiltered: liveness is applied separately at read time via the shared
//!   `StakerSet`, and an inactive node stays dialable for lane settlement.
//!
//! # Fatality
//!
//! The single `getRegisteredNodes` bootstrap is fatal: staker-set requires it.
//! A transient RPC failure retries on the shared boot budget; a deterministic
//! fault or an exhausted budget propagates. The bindings projection (pull-through is
//! opportunistic, so it would otherwise be non-fatal) has **no RPC of its own**:
//! it is derived from page data already in hand and cannot fail independently.
//! The same is true of regions, the reverse map, and the dial-address directory.
//! A node either boots with all five projections or fails at the one shared read, so pull-through is
//! never lost on its own.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use alloy::eips::BlockId;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use decdn_incentive::node_register::decode_dial_addrs;
use decdn_protocol::Region;
use tracing::{debug, info, warn};

use crate::chain_events::boot_retry::{BootFault, BootRetry};
use crate::chain_events::multiplexed_poller::{Route, SinkSource};
use crate::chain_events::resumable_watcher::{CursorStart, LogSink, clear_cadence_on_recovery};
use crate::chain_events::shared_head::{HeadSource, snapshot_block};
use crate::chain_events::timed;
use crate::dht::chain_projection::{with_read, with_write};
use crate::dht::chain_staker_set::{ChainStakerSet, StakerChange, apply_change};
use crate::dht::dial_addrs::DialAddrDirectory;
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
    /// The bonded-operator set the DHT admits records from.
    pub staker_set: Arc<dyn StakerSet>,
    /// `NodeId` → dial address, present only where pull-through is on.
    pub node_addresses: Option<Arc<dyn NodeAddressResolver>>,
    /// `NodeId → regionHint`, as the canonical [`decdn_protocol::Region`] code;
    /// an empty or invalid hint has no entry. Read by the ADR-030 region-latency penalty on the selection/pull path.
    /// Always present, unlike `node_addresses`.
    pub regions: Arc<RwLock<HashMap<NodeId, String>>>,
    /// `operator address → bound NodeId`, always built (like `regions`, unlike
    /// the pull-through-gated `bindings`) so the chain-backed origin directory
    /// can resolve operators locally with no `nodeIdOf` RPC.
    pub operator_to_node: Arc<RwLock<HashMap<Address, NodeId>>>,
    /// `NodeId → registry-published direct dial addresses`, always built. The
    /// runtime adds its [`DialAddrDirectory::lookup`] to the node's endpoint.
    pub dial_addrs: DialAddrDirectory,
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

    /// The block an enumeration pins its pages to
    /// (`shared_head::snapshot_block`): the lag margin below the head.
    fn snapshot_block(&self) -> impl Future<Output = Result<u64>> + Send;

    /// Re-enumerate the projections from chain state at block `at`, as the
    /// bootstrap does.
    ///
    /// This is the self-healing leg. Every other input to these projections is
    /// an event, and an event can be missed — an orphaned log at the unstable
    /// tip stays applied because `eth_getLogs` never reports a removal, and a
    /// tick lost to RPC backoff advances nothing but is not replayed. Neither
    /// shows up as an error, so without a periodic authoritative re-read the
    /// only repair for a drifted set is a process restart.
    fn full_snapshot(&self, at: u64) -> impl Future<Output = Result<RegistrySnapshot>> + Send;
}

/// The five registry projections one `getRegisteredNodes` enumeration derives.
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
    /// The shared head source the snapshot block derives from.
    head: Arc<dyn HeadSource>,
}

impl<P: Provider + Clone> RegistryChainReads for ContractReads<P> {
    async fn snapshot_block(&self) -> Result<u64> {
        snapshot_block(
            self.registry.provider(),
            &*self.head,
            *self.registry.address(),
        )
        .await
    }

    async fn full_snapshot(&self, at: u64) -> Result<RegistrySnapshot> {
        bootstrap_registry(&self.registry, BlockId::number(at)).await
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
    /// `NodeId → regionHint`, canonical codes only; an empty or invalid
    /// `regionHint` is absence (see [`canonical_region`]).
    /// Always built (not gated on pull-through): the ADR-030 selection penalty
    /// reads it whether or not node-to-node pull is on.
    pub(crate) regions: Arc<RwLock<HashMap<NodeId, String>>>,
    /// `operator address → bound NodeId`. Always built (not gated on
    /// pull-through, like `regions`) and unfiltered: an ejected or unbonding
    /// operator keeps its reverse entry, since liveness is applied separately
    /// at read time via the shared `StakerSet`.
    pub(crate) operator_to_node: Arc<RwLock<HashMap<Address, NodeId>>>,
    /// `NodeId → registry multiaddrs`. Always built and unfiltered, like
    /// `regions`.
    pub(crate) dial_addrs: DialAddrDirectory,
    pub(crate) metrics: Arc<Metrics>,
    /// How often [`RegistryChainReads::full_snapshot`] re-derives both
    /// projections. Deliberately a constant rather than a config knob: it is a
    /// correctness backstop, not a tuning surface, and an operator who set it
    /// wrong would silently lose the only drift repair short of a restart.
    pub(crate) resync_interval: Duration,
    /// When the last re-enumeration ran. Seeded to "just ran" at bootstrap,
    /// since the bootstrap enumeration IS one.
    pub(crate) last_resync: Option<Instant>,
    /// The block of the latest tail change to each `NodeId`. A resync keeps the
    /// current projection entries of every node changed above its snapshot block
    /// (see [`Self::overlay_tail_changes`]), then forgets the rest.
    pub(crate) tail_changes: HashMap<NodeId, u64>,
}

impl<R: RegistryChainReads> RegistrySink<R> {
    /// `NodeRegistered`: active insert (unfiltered — see the module doc) AND a
    /// binding insert.
    fn on_registered(
        &self,
        node_id: NodeId,
        eth_address: Address,
        region: &str,
        multiaddrs: &[u8],
    ) {
        apply_change(&self.active, &self.metrics, StakerChange::Active(node_id));
        self.dial_addrs.set(node_id, decode_dial_addrs(multiaddrs));
        if let Some(bindings) = &self.bindings {
            set_binding(bindings, &self.metrics, node_id, eth_address);
        }
        with_write(&self.operator_to_node, "chain operator reverse map", |m| {
            m.insert(eth_address, node_id);
        });
        if let Some(region) = canonical_region(region) {
            with_write(&self.regions, "chain region directory", |m| {
                m.insert(node_id, region);
            });
        }
    }

    /// `NodeDeregistered`: the binding, region, and dial addresses are cleared
    /// only here — `registerNode` sets them, `deregisterNode` clears them,
    /// `updateRegion` replaces the region ([`Self::on_region_updated`]), and
    /// `updateMultiaddrs` replaces the dial addresses. Bond/unbonding/ejection
    /// transitions flip `isActive` without touching any of them.
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
        self.dial_addrs.remove(&node_id);
    }

    /// `RegionUpdated`: the region map follows the operator's new
    /// `regionHint` on the tick it lands. Membership and bindings are
    /// untouched. An empty or invalid region is absence, as in
    /// [`Self::on_registered`].
    fn on_region_updated(&self, node_id: NodeId, new_region: &str) {
        let region = canonical_region(new_region);
        with_write(&self.regions, "chain region directory", |m| match region {
            Some(region) => {
                m.insert(node_id, region);
            }
            None => {
                m.remove(&node_id);
            }
        });
    }

    /// Resolve an operator-indexed event via `nodeIdOf` and apply the implied
    /// change. The canonical `nodeIdOf.active` wins over what the event implies
    /// (they can disagree if a later transition raced the event). Bindings are
    /// untouched: these events never change one.
    ///
    /// Returns the node the change applied to, `None` when it applied nothing.
    async fn on_operator_change(
        &self,
        operator: Address,
        event_implies_active: bool,
    ) -> Option<NodeId> {
        let resolved = match self.reads.node_id_of(operator).await {
            Ok(r) => r,
            Err(err) => {
                // Dropping the change leaves the cached set out of sync with chain
                // state until a follow-up event for the same operator arrives, or
                // until `on_tick_complete`'s re-enumeration succeeds. An RPC
                // failure here is an `eth_call` while the poll is `eth_getLogs`,
                // so it can happen with the route perfectly healthy — in which
                // case the repair is the cadence, not the recovery edge. When it
                // does travel with a poll failure, `on_recovered` forces the
                // re-enumeration on the tick the route comes back. Unlike a
                // stream-level error this does not trip the watcher backoff, so
                // without this counter it would move no metric at all.
                self.metrics.staker_set_watcher_resolve_failure();
                warn!(
                    error = %sanitize_err_chain(&err),
                    %operator,
                    "nodeIdOf RPC failed; cached active set may diverge from chain state for this operator"
                );
                return None;
            }
        };
        let Some((node_id, now_active)) = resolved else {
            debug!(%operator, "operator-indexed event for unbound operator; ignoring");
            return None;
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
        Some(node_id)
    }

    /// Carry every node the tail changed above `at` from the current projections
    /// into a snapshot pinned at `at`, then forget the changes at or below it.
    ///
    /// The snapshot reads the lagged block, so it misses any change the tail
    /// already applied above it. Replacing the projections wholesale would roll
    /// those back until the next resync. A node changed above `at` keeps its
    /// current entry in every projection; every other node takes the snapshot's.
    fn overlay_tail_changes(&mut self, at: u64, snap: &mut RegistrySnapshot) {
        self.tail_changes.retain(|_, block| *block > at);
        for node_id in self.tail_changes.keys() {
            if with_read(&self.active, "chain staker set", |set| {
                set.contains(node_id)
            }) {
                snap.active.insert(*node_id);
            } else {
                snap.active.remove(node_id);
            }
            if let Some(current) = &self.bindings {
                match with_read(current, "chain node-address directory", |m| {
                    m.get(node_id).copied()
                }) {
                    Some(addr) => snap.bindings.insert(*node_id, addr),
                    None => snap.bindings.remove(node_id),
                };
            }
            match with_read(&self.regions, "chain region directory", |m| {
                m.get(node_id).cloned()
            }) {
                Some(region) => snap.regions.insert(*node_id, region),
                None => snap.regions.remove(node_id),
            };
            match self.dial_addrs.get(node_id) {
                Some(addrs) => snap.dial_addrs.insert(*node_id, addrs),
                None => snap.dial_addrs.remove(node_id),
            };
            snap.operator_to_node.retain(|_, nid| nid != node_id);
            with_read(&self.operator_to_node, "chain operator reverse map", |m| {
                for (addr, nid) in m {
                    if nid == node_id {
                        snap.operator_to_node.insert(*addr, *nid);
                    }
                }
            });
        }
    }
}

impl<R: RegistryChainReads> LogSink for RegistrySink<R> {
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)] // one flat arm per event
    async fn apply(&mut self, log: Log) -> Result<()> {
        // A log without a block number never comes back from `eth_getLogs`; one
        // that did would count as the newest change, which a resync keeps.
        let block = log.block_number.unwrap_or(u64::MAX);
        let changed = match log.topic0().copied() {
            Some(sig) if sig == CapacityBond::NodeRegistered::SIGNATURE_HASH => {
                match CapacityBond::NodeRegistered::decode_log_data(&log.inner.data) {
                    Ok(event) => {
                        let node_id = NodeId::from_bytes(event.nodeId.0);
                        self.on_registered(
                            node_id,
                            event.ethAddress,
                            &event.regionHint,
                            &event.multiaddrs,
                        );
                        Some(node_id)
                    }
                    Err(err) => {
                        warn!(error = %err, "skipping undecodable NodeRegistered log");
                        None
                    }
                }
            }
            Some(sig) if sig == CapacityBond::NodeDeregistered::SIGNATURE_HASH => {
                match CapacityBond::NodeDeregistered::decode_log_data(&log.inner.data) {
                    Ok(event) => {
                        let node_id = NodeId::from_bytes(event.nodeId.0);
                        self.on_deregistered(node_id);
                        Some(node_id)
                    }
                    Err(err) => {
                        warn!(error = %err, "skipping undecodable NodeDeregistered log");
                        None
                    }
                }
            }
            Some(sig) if sig == CapacityBond::NodeAutoEjected::SIGNATURE_HASH => {
                match CapacityBond::NodeAutoEjected::decode_log_data(&log.inner.data) {
                    // Deactivates WITHOUT clearing the binding: ejection flips
                    // `isActive` only, and the operator may still be owed payment
                    // on an open lane.
                    Ok(event) => {
                        let node_id = NodeId::from(event.nodeId.0);
                        apply_change(&self.active, &self.metrics, StakerChange::Inactive(node_id));
                        Some(node_id)
                    }
                    Err(err) => {
                        warn!(error = %err, "skipping undecodable NodeAutoEjected log");
                        None
                    }
                }
            }
            Some(sig) if sig == CapacityBond::Reinstated::SIGNATURE_HASH => {
                match CapacityBond::Reinstated::decode_log_data(&log.inner.data) {
                    Ok(event) => self.on_operator_change(event.operator, true).await,
                    Err(err) => {
                        warn!(error = %err, "skipping undecodable Reinstated log");
                        None
                    }
                }
            }
            Some(sig) if sig == CapacityBond::UnbondingRequested::SIGNATURE_HASH => {
                match CapacityBond::UnbondingRequested::decode_log_data(&log.inner.data) {
                    Ok(event) => self.on_operator_change(event.operator, false).await,
                    Err(err) => {
                        warn!(error = %err, "skipping undecodable UnbondingRequested log");
                        None
                    }
                }
            }
            // The blacklist-ejection twin of `Reinstated` (#1030). `ejected` is a
            // conjunct of `isActive`, so an ejection deactivates the operator
            // exactly as an unbonding request does — but the event was missing
            // from this OR-set, so an ejection only landed at the next
            // `REGISTRY_RESYNC_INTERVAL` re-enumeration. Every consumer of the
            // staker set was wrong for up to 15 minutes, DHT admission included.
            Some(sig) if sig == CapacityBond::EjectedByBlacklist::SIGNATURE_HASH => {
                match CapacityBond::EjectedByBlacklist::decode_log_data(&log.inner.data) {
                    Ok(event) => self.on_operator_change(event.operator, false).await,
                    Err(err) => {
                        warn!(error = %err, "skipping undecodable EjectedByBlacklist log");
                        None
                    }
                }
            }
            Some(sig) if sig == CapacityBond::RegionUpdated::SIGNATURE_HASH => {
                match CapacityBond::RegionUpdated::decode_log_data(&log.inner.data) {
                    Ok(event) => {
                        let node_id = NodeId::from_bytes(event.nodeId.0);
                        self.on_region_updated(node_id, &event.newRegion);
                        Some(node_id)
                    }
                    Err(err) => {
                        warn!(error = %err, "skipping undecodable RegionUpdated log");
                        None
                    }
                }
            }
            // `updateMultiaddrs`: the dial-address directory follows the new
            // record on the tick it lands. An empty or undecodable record is
            // absence.
            Some(sig) if sig == CapacityBond::NodeMultiaddrUpdated::SIGNATURE_HASH => {
                match CapacityBond::NodeMultiaddrUpdated::decode_log_data(&log.inner.data) {
                    Ok(event) => {
                        let node_id = NodeId::from_bytes(event.nodeId.0);
                        self.dial_addrs
                            .set(node_id, decode_dial_addrs(&event.multiaddrs));
                        Some(node_id)
                    }
                    Err(err) => {
                        warn!(error = %err, "skipping undecodable NodeMultiaddrUpdated log");
                        None
                    }
                }
            }
            _ => {
                debug!(topic0 = ?log.topic0(), "unmatched CapacityBond event in subscribed OR-set");
                None
            }
        };
        if let Some(node_id) = changed {
            self.tail_changes.insert(node_id, block);
        }
        Ok(())
    }

    /// Force the next reconcile to re-enumerate, rather than waiting out the
    /// cadence.
    ///
    /// The watcher just recovered from an errored tick, which is exactly when
    /// the projections are most likely to have drifted: an event dropped by a
    /// failed `nodeIdOf` during the outage leaves the cached set out of sync,
    /// and the cadence is at its least helpful — the reconcile is skipped while
    /// the route is errored, so the repair has not been running. Clearing the
    /// clock makes [`Self::on_tick_complete`], on this same tick, re-read.
    ///
    /// Floored by [`clear_cadence_on_recovery`]: an endpoint that flaps rather
    /// than staying down produces a recovery edge every few seconds, and
    /// clearing the clock unconditionally would aim a full paginated enumeration
    /// at it on each one.
    fn on_recovered(&mut self) {
        clear_cadence_on_recovery(&mut self.last_resync, self.resync_interval);
    }

    /// Cadence-gated re-enumeration: rebuild the projections from chain state at
    /// the lagged snapshot block, carry over every node the tail changed above it
    /// ([`Self::overlay_tail_changes`]), and swap them in.
    ///
    /// Build-then-swap, never clear-then-fill: the new sets are fully
    /// materialized before either lock is taken for writing, so a failed read
    /// leaves the previous projections intact rather than emptying them. An
    /// empty staker set would make the node treat every peer as unstaked.
    ///
    /// Returns `Ok` on failure. The event tail is the primary path and is still
    /// working; backing the whole watcher off would also stall event pickup,
    /// trading a stale set for no updates at all. A failure moves
    /// `decdn_capacity_bond_registry_resync_failures_total` instead, and a
    /// success stamps
    /// `decdn_capacity_bond_registry_last_resync_timestamp_seconds`.
    ///
    /// Two triggers reach here: the cadence, and
    /// [`Self::on_recovered`] clearing the clock when the route comes back from
    /// an errored tick.
    async fn on_tick_complete(&mut self) -> Result<()> {
        // Once per clean tick rather than per event: one pass covers every
        // event the tick applied.
        publish_region_counts(&self.active, &self.regions, &self.metrics);
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

        let read = async {
            let at = self.reads.snapshot_block().await?;
            let snapshot = self.reads.full_snapshot(at).await?;
            anyhow::Ok((at, snapshot))
        };
        let (at, mut snap) = match read.await {
            Ok(read) => read,
            Err(err) => {
                // `Ok` upward, so the counter is the only thing that moves: an
                // `Err` marks the route errored, which stalls event pickup and
                // also skips the next resync entirely.
                self.metrics.capacity_bond_registry_resync_failure();
                warn!(
                    error = %sanitize_err_chain(&err),
                    "capacity-bond registry resync failed; keeping current projections"
                );
                return Ok(());
            }
        };

        self.overlay_tail_changes(at, &mut snap);
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

/// One paginated `getRegisteredNodes` read feeding all five projections.
///
/// Always returns all five maps and lets the caller drop the unwanted one — a
/// `want_bindings: bool` parameter would be a boolean trap, and building the map
/// is free while already iterating the page. See the module doc for why `active`
/// is `isActive`-filtered and `bindings` is not: `getRegisteredNodes` computes
/// `isActive` per entry on-chain and returns it as a parallel `active[]`, so a
/// single call per page derives `active` and `bindings` with no per-entry
/// round-trip. `operator_to_node` is the inverse of `bindings`, built from the
/// same page with no chain call of its own; like `regions` and `dial_addrs` it
/// is unfiltered and always built.
///
/// Every page reads at `at`, the snapshot block, so the pages agree with each
/// other: a load-balanced provider cannot answer one page from a lagging
/// upstream and the next from a current one.
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
        // `active[i]` is `isActive(page[i])`, computed on-chain and index-aligned
        // with `page` by the contract. `bindings` takes every entry (unfiltered):
        // an operator mid-unbonding is inactive but still payable.
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

/// Enumerate `CapacityBond` once, then spawn the single watcher that keeps both
/// projections current.
///
/// The snapshot-block read and the enumeration retry transient failures on
/// `boot`'s budget; a deterministic fault or an exhausted budget fails the
/// bootstrap.
///
/// `track_node_addresses` mirrors `cache.node_to_node_pull_through_enabled`.
pub async fn bootstrap<P>(
    provider: P,
    registry_addr: Address,
    head: Arc<dyn HeadSource>,
    track_node_addresses: bool,
    metrics: Arc<Metrics>,
    boot: &BootRetry,
) -> Result<RegistryHandles>
where
    P: Provider + Clone + 'static,
{
    let registry = CapacityBond::new(registry_addr, provider.clone());
    let reads = ContractReads {
        registry: registry.clone(),
        head,
    };

    // Pin every page to one snapshot block and seed the tail cursor at that same
    // block. An event at or below it is in the enumeration; one above it is on
    // the tail, which scans from the block inclusive. Re-applying an event the
    // snapshot already reflects is a no-op in every arm, so overlap is safe but a
    // gap is not. The block sits `SNAPSHOT_LAG_MARGIN_BLOCKS` below the reported
    // head so every upstream behind a load-balanced RPC can serve the pages.
    let (
        snapshot_block,
        RegistrySnapshot {
            active: initial_active,
            bindings: initial_bindings,
            operator_to_node: initial_operator_to_node,
            regions: initial_regions,
            dial_addrs: initial_dial_addrs,
        },
    ) = boot
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
        active_count = initial_active.len(),
        binding_count = initial_bindings.len(),
        dial_addr_count = initial_dial_addrs.len(),
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
    let dial_addrs = DialAddrDirectory::default();
    dial_addrs.replace_all(initial_dial_addrs);
    publish_region_counts(&active, &regions, &metrics);

    // Bootstrap IS a successful re-enumeration — the same `getRegisteredNodes`
    // read, against the same contract. Stamping the liveness gauge here is what
    // makes the staleness alert usable: its `> 0` guard would otherwise suppress
    // it forever on the node where every later resync fails or is skipped, which
    // is the exact node it exists to find.
    metrics.capacity_bond_registry_resync();

    let sink = RegistrySink {
        reads,
        active: Arc::clone(&active),
        bindings: bindings.clone(),
        operator_to_node: Arc::clone(&operator_to_node),
        regions: Arc::clone(&regions),
        dial_addrs: dial_addrs.clone(),
        metrics: Arc::clone(&metrics),
        resync_interval: REGISTRY_RESYNC_INTERVAL,
        // The bootstrap enumeration just ran, so the first backstop resync is
        // due one interval from now rather than on the first tick.
        last_resync: Some(Instant::now()),
        tail_changes: HashMap::new(),
    };
    let route = Route {
        addresses: vec![registry_addr],
        topic0s: registry_route_topic0s(),
        start: registry_cursor_start(snapshot_block),
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
        dial_addrs,
        route,
    })
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

/// The registry route's demux key: every `CapacityBond` staker-membership
/// event, plus `RegionUpdated` for the region map and `NodeMultiaddrUpdated`
/// for the dial-address directory. Split out from [`bootstrap`] so the exact topic0 set is unit-testable
/// without a provider.
fn registry_route_topic0s() -> Vec<B256> {
    vec![
        CapacityBond::NodeRegistered::SIGNATURE_HASH,
        CapacityBond::NodeDeregistered::SIGNATURE_HASH,
        CapacityBond::NodeAutoEjected::SIGNATURE_HASH,
        CapacityBond::Reinstated::SIGNATURE_HASH,
        CapacityBond::UnbondingRequested::SIGNATURE_HASH,
        CapacityBond::EjectedByBlacklist::SIGNATURE_HASH,
        CapacityBond::RegionUpdated::SIGNATURE_HASH,
        CapacityBond::NodeMultiaddrUpdated::SIGNATURE_HASH,
    ]
}

/// The registry route's cursor start: seed the live tail from the enumeration
/// snapshot block. The staker set is rebuilt from that enumeration each boot, so
/// there is no durable cursor to persist. Split out from [`bootstrap`] so the
/// cursor shape is unit-testable without a provider.
const fn registry_cursor_start(snapshot_block: u64) -> CursorStart {
    CursorStart::Seeded { at: snapshot_block }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests;
