//! Chain-backed [`StakerSet`] implementation.
//!
//! This module owns the **projection** only: the cached `NodeId → active?` set,
//! its accessors, and the change broadcast. The `CapacityBond` enumeration and
//! the event watcher that keep it current live in
//! [`crate::dht::capacity_bond_registry`] (#1110), which builds this via
//! `ChainStakerSet::from_parts` — it shares one `getActiveNodes` read and one
//! `eth_getLogs` loop with the node-address directory, since both are derived
//! from the same contract.
//!
//! The cached set is seeded from `CapacityBond.getActiveNodes()`, each entry
//! filtered through `isActive(operator)` to apply the full predicate — registered,
//! bond ≥ minBond, no unbonding, not ejected — because the `getActiveNodes` page
//! is the un-filtered `_registeredAddrs` array per the contract's own comment. It
//! then follows the five membership-mutating events, emitting [`StakerChange`]
//! for every observed transition.
//!
//! # Spec mapping
//!
//! - ADR 022 §STORE Flow: the receiver checks `holder` is in the
//!   cached active-staker set populated from
//!   `CapacityBond.getActiveNodes()`.
//! - ADR 019 § Step 3.3: bootstrap pattern (initial paginated
//!   `getActiveNodes` + event follow — implemented as an `eth_getLogs`
//!   poll, #1106).
//! - The event set the watcher follows is grounded in
//!   `CapacityBond.sol`'s own write-paths — every contract write
//!   that flips the canonical `isActive` predicate is mirrored by an
//!   event there.
//!
//! # Failure model
//!
//! Bootstrap RPC failure → the registry bootstrap propagates the error (the
//! runtime treats it as fatal; the DHT cannot function without a staker set).
//!
//! Watcher RPC failure (mid-run) → the shared loop logs at `warn!`, sleeps for an
//! exponentially-growing backoff (1s → 60s cap), and re-polls. The cursor is
//! retained across the backoff, so the next `eth_getLogs` tick re-scans
//! `[cursor, head]` and re-applies any membership event that landed during the
//! outage — no stream-level drift window. (There is still no `getActiveNodes`
//! resync to reconcile against a checkpoint older than the live cursor, but that
//! is only reachable via the per-event `nodeIdOf` drop below, not a backoff gap.)
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
//! Since #1110 these describe the one shared `capacity-bond` loop, so they move
//! in lockstep with the `node_address_watcher_*` family whenever pull-through is
//! on.
//!
//! A `getActiveNodes` resync-on-extended-outage path is a follow-up;
//! these metrics surface the window that path would close.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use tokio::sync::broadcast;
use tracing::warn;

use crate::chain_events::AbortOnDrop;
use crate::dht::routing::NodeId;
use crate::dht::staker_set::{StakerChange, StakerSet};
use crate::metrics::Metrics;

/// Chain-backed staker set. Cheap to clone via the shared inner
/// [`Arc`]; the runtime holds one `Arc<dyn StakerSet>` and consumers
/// access the cached set through it. The background watcher task is
/// owned via a private `AbortOnDrop` wrapper so a node-restart cycle
/// never leaks chain-poll tasks.
#[derive(Debug)]
pub struct ChainStakerSet {
    active: Arc<RwLock<HashSet<NodeId>>>,
    changes_tx: broadcast::Sender<StakerChange>,
    _watcher: Arc<AbortOnDrop>,
}

impl ChainStakerSet {
    /// Assemble from parts owned by `capacity_bond_registry`, which does the
    /// enumeration and runs the shared watcher. `watcher` is shared with
    /// `ChainNodeAddressDirectory` when pull-through is on, so the task outlives
    /// whichever façade is dropped first.
    pub(super) const fn from_parts(
        active: Arc<RwLock<HashSet<NodeId>>>,
        changes_tx: broadcast::Sender<StakerChange>,
        watcher: Arc<AbortOnDrop>,
    ) -> Self {
        Self {
            active,
            changes_tx,
            _watcher: watcher,
        }
    }
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

/// Apply a change to the active set, then broadcast it. The broadcast
/// send error case (no live subscribers) is ignored: the set has
/// already been updated, and any later `subscribe_changes` call sees
/// the post-update state via `is_active` / `active_nodes`. A `Lagged`
/// subscriber is on the receiver to handle.
pub(super) fn apply_change(
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
    /// Mirrors the `on_backoff`/`on_established` hooks the resumable poller fires.
    /// Exercises the metric wiring without a live RPC provider (the real poll loop
    /// needs a chain endpoint).
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
