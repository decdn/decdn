//! Chain-backed [`StakerSet`] implementation.
//!
//! Reads the active-staker set from `CapacityBond.getActiveNodes()`
//! at startup, filters each entry through `isActive(operator)` to apply
//! the full predicate (registered + bond ≥ minBond + no unbonding +
//! not ejected; the `getActiveNodes` page is the un-filtered
//! `_registeredAddrs` array per the contract's own comment), then runs
//! a background task that follows the five membership-mutating events
//! and emits [`StakerChange`] for every observed transition.
//!
//! # Spec mapping
//!
//! - ADR 022 §STORE Flow: the receiver checks `holder` is in the
//!   cached active-staker set populated from
//!   `CapacityBond.getActiveNodes()`.
//! - ADR 019 § Step 3.3: bootstrap pattern (initial paginated
//!   `getActiveNodes` + event subscription).
//! - The event set this watcher follows is grounded in
//!   `CapacityBond.sol`'s own write-paths — every contract write
//!   that flips the canonical `isActive` predicate is mirrored by an
//!   event here.
//!
//! # Failure model
//!
//! Bootstrap RPC failure → caller propagates the error (the runtime
//! treats it as fatal; the DHT cannot function without a staker set).
//!
//! Watcher RPC failure (mid-run) → the task logs at `warn!`, sleeps
//! for an exponentially-growing backoff (1s → 60s cap), and
//! re-establishes its event filters. Today there is no `getActiveNodes`
//! resync after extended outage, so the cached set can drift from
//! chain state when an event arrives while filters are down.
//!
//! A narrower drift source: an operator-indexed event whose follow-up
//! `nodeIdOf(operator)` RPC fails is dropped (the membership change is
//! lost) without tripping the stream backoff. That case bumps no
//! restart/down-seconds metric, so it is surfaced separately by
//! `decdn_staker_set_watcher_resolve_failures_total` (#788).
//!
//! This drift window is dashboard-visible and alertable via four
//! metrics (#783, #788), distinct from `decdn_rpc_healthy` (which tracks
//! the reachability watchdog, not this task):
//! - `decdn_staker_set_watcher_restarts_total` — distinct drift windows;
//!   bumped once on the *edge* from a healthy cycle into the error state,
//!   not once per backoff iteration of one continuous outage.
//! - `decdn_staker_set_watcher_down_seconds` — true downtime: reads `0`
//!   for the whole life of any established cycle (however long/quiet) and
//!   climbs only while the loop is in error/backoff between a failed cycle
//!   and the next success, so an alert fires on a *sustained* outage.
//! - `decdn_staker_set_watcher_resolve_failures_total` — operator-indexed
//!   events (`Reinstated` / `UnbondingRequested`) dropped because the
//!   follow-up `nodeIdOf` RPC failed; a nonzero rate is silent-drift risk
//!   that no stream-level restart would otherwise surface.
//! - `decdn_staker_set_active_count` — current cached active-set size,
//!   to spot a frozen or collapsed cache.
//!
//! A `getActiveNodes` resync-on-extended-outage path is a follow-up;
//! these metrics surface the window that path would close.
//!
//! # N+1 round-trips at bootstrap
//!
//! Each page of `getActiveNodes` returns up to 100 entries; we then
//! issue one `isActive(operator)` per entry to filter. At `PoC` scale
//! (tens of nodes) this is tens of RPC round-trips — acceptable for a
//! one-shot startup path. `Multicall3` batching is a natural
//! follow-up.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain_events::resumable_watcher::{
    self, CursorPolicy, LogSink, WatcherConfig, WatcherHook,
};
use crate::dht::routing::NodeId;
use crate::dht::staker_set::{StakerChange, StakerSet};
use crate::metrics::Metrics;
use decdn_common::redact::sanitize_rpc_display;
use decdn_incentive::capacity_bond::CapacityBond;

/// Page size for the initial paginated `getActiveNodes` read.
/// Matches ADR 019 § Step 3.3's worked-example limit.
const PAGE_SIZE: u64 = 100;

/// Capacity of the `StakerChange` broadcast channel. Sized so that a
/// slow subscriber can fall behind by a single bootstrap cycle (~100
/// changes in the bursty re-sync window) without forcing a lagging
/// recv into the `Lagged` arm. Real steady-state traffic is sparse
/// (operator stakes / unstakes are infrequent), so 128 is a healthy
/// headroom rather than a hot-path constraint.
const CHANGES_CHANNEL_CAPACITY: usize = 128;

/// Backoff between watcher restart attempts after an event-stream
/// terminates with an error.
const WATCHER_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Upper bound for the watcher restart backoff.
const WATCHER_MAX_BACKOFF: Duration = Duration::from_mins(1);

/// Chain-backed staker set. Cheap to clone via the shared inner
/// [`Arc`]; the runtime holds one `Arc<dyn StakerSet>` and consumers
/// access the cached set through it. The background watcher task is
/// owned via a private `AbortOnDrop` wrapper so a node-restart cycle
/// never leaks chain-poll tasks.
#[derive(Debug)]
pub struct ChainStakerSet {
    active: Arc<RwLock<HashSet<NodeId>>>,
    changes_tx: broadcast::Sender<StakerChange>,
    _watcher: AbortOnDrop,
}

impl ChainStakerSet {
    /// Initial bootstrap: paginate `getActiveNodes`, filter each entry
    /// through `isActive(operator)`, spawn the background event
    /// watcher. Returns once the cache is populated and the watcher
    /// is running — the watcher's own subscription failures do not
    /// fail bootstrap.
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
        let initial = bootstrap_active_set(&registry).await.with_context(|| {
            format!("paginated getActiveNodes from CapacityBond at {registry_addr}")
        })?;
        info!(
            active_count = initial.len(),
            %registry_addr,
            "ChainStakerSet bootstrap from CapacityBond complete"
        );
        // Publish the bootstrap size so the gauge is non-`(no data)` before
        // the first membership change, and so a watcher that never observes
        // an event still reports a meaningful active count.
        metrics.staker_set_active_count(initial.len());

        let (changes_tx, _) = broadcast::channel(CHANGES_CHANNEL_CAPACITY);
        let active = Arc::new(RwLock::new(initial));
        // The authoritative set came from `getActiveNodes` enumeration above; the
        // watcher only needs to *follow* membership events from head forward, so
        // it live-tails on the shared getLogs poller (#1092/#1106) with no
        // historical backfill and no persisted cursor.
        let sink = StakerSink {
            registry,
            active: Arc::clone(&active),
            changes_tx: changes_tx.clone(),
            metrics: Arc::clone(&metrics),
        };
        let cfg = WatcherConfig {
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
            max_backfill_span: u64::MAX,
            cursor: CursorPolicy::HeadMinusWindow {
                window_blocks: 0,
                floor: 0,
            },
            initial_backoff: WATCHER_INITIAL_BACKOFF,
            max_backoff: WATCHER_MAX_BACKOFF,
            rpc_call_timeout: None,
            shutdown: CancellationToken::new(),
            seed_cursor: None,
            label: "staker-set",
            on_established: Some(established_hook(&metrics)),
            on_backoff: Some(backoff_hook(&metrics)),
        };
        let watcher_handle = tokio::spawn(resumable_watcher::run(provider, cfg, sink));

        Ok(Self {
            active,
            changes_tx,
            _watcher: AbortOnDrop(watcher_handle),
        })
    }
}

/// Applies `CapacityBond` membership logs to the active set (#1092). `apply`
/// live-tails events from head (the authoritative set came from `getActiveNodes`
/// at bootstrap); it never returns `Err` — an operator-indexed event's
/// `nodeIdOf` resolution failure is counted and skipped, and an undecodable log
/// is logged and skipped, so neither hot-loops the deterministic re-scan.
struct StakerSink<P: Provider + Clone> {
    registry: CapacityBond::CapacityBondInstance<P>,
    active: Arc<RwLock<HashSet<NodeId>>>,
    changes_tx: broadcast::Sender<StakerChange>,
    metrics: Arc<Metrics>,
}

impl<P: Provider + Clone> LogSink for StakerSink<P> {
    #[allow(clippy::cognitive_complexity)]
    async fn apply(&mut self, log: Log) -> Result<()> {
        match log.topic0().copied() {
            Some(sig) if sig == CapacityBond::NodeRegistered::SIGNATURE_HASH => {
                match CapacityBond::NodeRegistered::decode_log_data(&log.inner.data) {
                    Ok(event) => apply_change(
                        &self.active,
                        &self.changes_tx,
                        &self.metrics,
                        StakerChange::Active(event.nodeId.0.into()),
                    ),
                    Err(err) => warn!(%err, "skipping undecodable NodeRegistered log"),
                }
            }
            Some(sig) if sig == CapacityBond::NodeDeregistered::SIGNATURE_HASH => {
                match CapacityBond::NodeDeregistered::decode_log_data(&log.inner.data) {
                    Ok(event) => apply_change(
                        &self.active,
                        &self.changes_tx,
                        &self.metrics,
                        StakerChange::Inactive(event.nodeId.0.into()),
                    ),
                    Err(err) => warn!(%err, "skipping undecodable NodeDeregistered log"),
                }
            }
            Some(sig) if sig == CapacityBond::NodeAutoEjected::SIGNATURE_HASH => {
                match CapacityBond::NodeAutoEjected::decode_log_data(&log.inner.data) {
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
                    Ok(event) => {
                        apply_operator_change(
                            &self.registry,
                            &self.active,
                            &self.changes_tx,
                            &self.metrics,
                            event.operator,
                            true,
                        )
                        .await;
                    }
                    Err(err) => warn!(%err, "skipping undecodable Reinstated log"),
                }
            }
            Some(sig) if sig == CapacityBond::UnbondingRequested::SIGNATURE_HASH => {
                match CapacityBond::UnbondingRequested::decode_log_data(&log.inner.data) {
                    Ok(event) => {
                        apply_operator_change(
                            &self.registry,
                            &self.active,
                            &self.changes_tx,
                            &self.metrics,
                            event.operator,
                            false,
                        )
                        .await;
                    }
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

/// Wire the healthy-cycle transition to the down-seconds gauge (→ 0).
fn established_hook(metrics: &Arc<Metrics>) -> WatcherHook {
    let metrics = Arc::clone(metrics);
    Box::new(move || metrics.staker_set_watcher_cycle_established())
}

/// Wire a tick failure to the backoff gauge (opens the down-seconds window).
fn backoff_hook(metrics: &Arc<Metrics>) -> WatcherHook {
    let metrics = Arc::clone(metrics);
    Box::new(move || metrics.staker_set_watcher_backoff_started())
}

impl StakerSet for ChainStakerSet {
    fn is_active(&self, node_id: &NodeId) -> bool {
        // RwLock read; on poison the active set is still
        // structurally valid (we never panic while holding the write
        // lock), so recover the inner state rather than propagating
        // a panic into the hot path.
        read_active(&self.active, |s| s.contains(node_id))
    }

    fn active_nodes(&self) -> Vec<NodeId> {
        read_active(&self.active, |s| s.iter().copied().collect())
    }

    fn len(&self) -> usize {
        read_active(&self.active, HashSet::len)
    }

    fn subscribe_changes(&self) -> broadcast::Receiver<StakerChange> {
        self.changes_tx.subscribe()
    }
}

/// Helper for the poison-tolerant `RwLock` read used by every
/// `StakerSet` accessor: recover the inner set rather than propagate
/// a panic into the handler hot path. A poisoned read means *something
/// panicked while holding the write lock* — we log on the recovery
/// arm so the panic surfaces somewhere, matching the in-repo
/// precedent in `cache/src/engine.rs`.
fn read_active<R, F>(active: &Arc<RwLock<HashSet<NodeId>>>, f: F) -> R
where
    F: FnOnce(&HashSet<NodeId>) -> R,
{
    match active.read() {
        Ok(guard) => f(&guard),
        Err(poisoned) => {
            warn!("ChainStakerSet active set RwLock poisoned; recovering inner state");
            f(&poisoned.into_inner())
        }
    }
}

/// Watcher join handle that aborts the task on drop. The task itself
/// is `pin`-friendly and self-contained, so abort is sufficient
/// cleanup; we do not await its completion (the runtime's drain pass
/// only awaits handles registered in the main `JoinSet`).
#[derive(Debug)]
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Paginate `getActiveNodes`, filter through `isActive`, collect the
/// strict active set. Returns an error if any RPC call fails — the
/// runtime cannot bootstrap the DHT without a complete picture.
async fn bootstrap_active_set<P>(
    registry: &CapacityBond::CapacityBondInstance<P>,
) -> Result<HashSet<NodeId>>
where
    P: Provider + Clone,
{
    let mut active = HashSet::new();
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
            let is_active = registry
                .isActive(node.ethAddress)
                .call()
                .await
                .with_context(|| format!("isActive({operator})", operator = node.ethAddress))?;
            if is_active {
                active.insert(NodeId::from_bytes(node.nodeId.0));
            }
        }
        offset = offset.saturating_add(page_len);
    }
    Ok(active)
}

/// Resolve an operator-indexed event to its `(NodeId, current_active)`
/// via `nodeIdOf`, then apply the implied change. If the event's
/// implied active state disagrees with the canonical `nodeIdOf.active`
/// (race between the event and a subsequent state change), the
/// canonical value wins. If the operator has no `nodeId` binding
/// (`bytes32(0)`), the change is silently dropped — the registry
/// invariant says binding precedes activation.
#[allow(clippy::cognitive_complexity)] // Tracing macros inflate complexity; the function body is straight-line resolve + decide + apply
async fn apply_operator_change<P>(
    registry: &CapacityBond::CapacityBondInstance<P>,
    active: &Arc<RwLock<HashSet<NodeId>>>,
    changes_tx: &broadcast::Sender<StakerChange>,
    metrics: &Arc<Metrics>,
    operator: Address,
    event_implies_active: bool,
) where
    P: Provider + Clone,
{
    let resolved = match registry.nodeIdOf(operator).call().await {
        Ok(r) => r,
        Err(err) => {
            // Dropping the change leaves the cached set out of sync
            // with chain state until either a follow-up event for
            // the same operator arrives or the resync-on-extended-
            // outage path is implemented. Unlike a stream-level error
            // this does not trip the watcher backoff, so without an
            // explicit counter it would move no metric at all — bump
            // the resolve-failure counter (alertable as drift risk)
            // alongside the loud `warn!`.
            metrics.staker_set_watcher_resolve_failure();
            warn!(
                err = %sanitize_rpc_display(&err),
                %operator,
                "nodeIdOf RPC failed; cached active set may diverge from chain state for this operator"
            );
            return;
        }
    };
    let node_id = resolved.nodeId.0;
    if node_id == [0u8; 32] {
        debug!(%operator, "operator-indexed event for unbound operator; ignoring");
        return;
    }
    let now_active = resolved.active;
    if now_active != event_implies_active {
        debug!(
            %operator,
            event_implies_active,
            now_active,
            "operator-indexed event disagrees with canonical nodeIdOf.active; trusting nodeIdOf"
        );
    }
    let change = if now_active {
        StakerChange::Active(node_id.into())
    } else {
        StakerChange::Inactive(node_id.into())
    };
    apply_change(active, changes_tx, metrics, change);
}

/// Apply a change to the active set, then broadcast it. The broadcast
/// send error case (no live subscribers) is ignored: the set has
/// already been updated, and any later `subscribe_changes` call sees
/// the post-update state via `is_active` / `active_nodes`. A `Lagged`
/// subscriber is on the receiver to handle.
fn apply_change(
    active: &Arc<RwLock<HashSet<NodeId>>>,
    changes_tx: &broadcast::Sender<StakerChange>,
    metrics: &Arc<Metrics>,
    change: StakerChange,
) {
    let (mutated, size) = {
        let mut guard = match active.write() {
            Ok(g) => g,
            Err(poisoned) => {
                warn!("ChainStakerSet active set RwLock poisoned; recovering inner state");
                poisoned.into_inner()
            }
        };
        let mutated = match change {
            StakerChange::Active(id) => guard.insert(id),
            StakerChange::Inactive(id) => guard.remove(&id),
        };
        // Sample the size while holding the lock so the gauge can never
        // observe a torn view from a concurrent change.
        (mutated, guard.len())
    };
    if mutated {
        // Only republish the gauge on a real membership change; a no-op
        // (re-insert / absent-remove) leaves the set — and the gauge —
        // unchanged.
        metrics.staker_set_active_count(size);
        // No live subscriber is a normal case for an empty
        // ConfigStakerSet runtime, or a chain-backed set during
        // bootstrap before any consumer has subscribed.
        let _ = changes_tx.send(change);
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

    fn fresh_state() -> (
        Arc<RwLock<HashSet<NodeId>>>,
        broadcast::Sender<StakerChange>,
        Arc<Metrics>,
    ) {
        let (tx, _) = broadcast::channel(8);
        (
            Arc::new(RwLock::new(HashSet::new())),
            tx,
            Arc::new(Metrics::new()),
        )
    }

    /// `apply_change(Active)` on an absent `NodeId` inserts and emits.
    /// Re-applying the same Active is a no-op (idempotent — no extra
    /// emission).
    #[test]
    fn apply_change_active_idempotent() {
        let (active, tx, metrics) = fresh_state();
        let mut rx = tx.subscribe();
        apply_change(&active, &tx, &metrics, StakerChange::Active(nid(1)));
        assert!(active.read().unwrap().contains(&nid(1)));
        assert_eq!(rx.try_recv().unwrap(), StakerChange::Active(nid(1)));

        apply_change(&active, &tx, &metrics, StakerChange::Active(nid(1)));
        assert_eq!(active.read().unwrap().len(), 1);
        // Second application was a no-op; channel empty.
        let err = rx.try_recv().expect_err("no second emission");
        assert!(matches!(err, broadcast::error::TryRecvError::Empty));
    }

    /// `apply_change(Inactive)` removes and emits. Removing an absent
    /// `NodeId` is a no-op (no emission).
    #[test]
    fn apply_change_inactive_idempotent() {
        let (active, tx, metrics) = fresh_state();
        active.write().unwrap().insert(nid(1));
        let mut rx = tx.subscribe();
        apply_change(&active, &tx, &metrics, StakerChange::Inactive(nid(1)));
        assert!(!active.read().unwrap().contains(&nid(1)));
        assert_eq!(rx.try_recv().unwrap(), StakerChange::Inactive(nid(1)));

        apply_change(&active, &tx, &metrics, StakerChange::Inactive(nid(2)));
        // nid(2) was never in the set; no emission.
        let err = rx.try_recv().expect_err("no emission for absent removal");
        assert!(matches!(err, broadcast::error::TryRecvError::Empty));
    }

    /// A real membership change republishes `decdn_staker_set_active_count`;
    /// an idempotent no-op leaves it untouched. This is the gauge that lets
    /// an operator spot a frozen/collapsed cache during a watcher outage
    /// (#783).
    #[test]
    fn apply_change_updates_active_count_gauge_only_on_real_change() {
        let (active, tx, metrics) = fresh_state();
        // Two distinct inserts → gauge tracks the growing set.
        apply_change(&active, &tx, &metrics, StakerChange::Active(nid(1)));
        apply_change(&active, &tx, &metrics, StakerChange::Active(nid(2)));
        let text = metrics.encode().unwrap();
        assert!(
            text.lines().any(|l| l == "decdn_staker_set_active_count 2"),
            "active-count gauge should report 2 after two distinct inserts:\n{text}"
        );

        // A no-op re-insert must not move the gauge.
        apply_change(&active, &tx, &metrics, StakerChange::Active(nid(1)));
        let text = metrics.encode().unwrap();
        assert!(
            text.lines().any(|l| l == "decdn_staker_set_active_count 2"),
            "active-count gauge should stay at 2 after an idempotent re-insert:\n{text}"
        );

        // A real removal shrinks it.
        apply_change(&active, &tx, &metrics, StakerChange::Inactive(nid(1)));
        let text = metrics.encode().unwrap();
        assert!(
            text.lines().any(|l| l == "decdn_staker_set_active_count 1"),
            "active-count gauge should report 1 after a removal:\n{text}"
        );
    }

    /// Simulate the watcher's drift-window accounting (#788): the restart
    /// counter is edge-triggered, so repeated `backoff_started` calls during
    /// one continuous outage (no intervening `cycle_established`) count as ONE
    /// window. A fresh window requires a `cycle_established` in between.
    /// Mirrors the `Err` arm of `watcher_loop`. Exercises the metric wiring
    /// without a live RPC provider (the real `run_watcher_once` needs a chain
    /// endpoint).
    #[test]
    fn watcher_error_restart_counts_one_per_drift_window() {
        let metrics = Arc::new(Metrics::new());
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_staker_set_watcher_restarts_total 0"),
            "restart counter should start at zero:\n{text}"
        );

        // First outage: three failed re-open attempts (three backoff
        // iterations) but a single continuous drift window → counts once.
        metrics.staker_set_watcher_backoff_started();
        metrics.staker_set_watcher_backoff_started();
        metrics.staker_set_watcher_backoff_started();
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_staker_set_watcher_restarts_total 1"),
            "one continuous outage should count exactly one restart:\n{text}"
        );

        // Filters re-establish (window closes), then a second outage opens a
        // new window → counts again.
        metrics.staker_set_watcher_cycle_established();
        metrics.staker_set_watcher_backoff_started();
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_staker_set_watcher_restarts_total 2"),
            "a second distinct outage should count a second restart:\n{text}"
        );
    }

    /// A `nodeIdOf` resolution failure in `apply_operator_change` bumps
    /// `decdn_staker_set_watcher_resolve_failures_total` (#788). Exercises the
    /// metric wiring directly — the `Err` arm calls exactly this method.
    #[test]
    fn watcher_resolve_failure_bumps_counter() {
        let metrics = Arc::new(Metrics::new());
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_staker_set_watcher_resolve_failures_total 0"),
            "resolve-failure counter should start at zero:\n{text}"
        );

        metrics.staker_set_watcher_resolve_failure();
        metrics.staker_set_watcher_resolve_failure();

        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_staker_set_watcher_resolve_failures_total 2"),
            "expected 2 resolve failures:\n{text}"
        );
    }
}
