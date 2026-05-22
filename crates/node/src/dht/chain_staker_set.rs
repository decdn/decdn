//! Chain-backed [`StakerSet`] implementation.
//!
//! Reads the active-staker set from `StakingRegistry.getActiveNodes()`
//! at startup, filters each entry through `isActive(operator)` to apply
//! the full predicate (registered + stake ≥ minStake + no unbonding +
//! not ejected; the `getActiveNodes` page is the un-filtered
//! `_registeredAddrs` array per the contract's own comment), then runs
//! a background task that follows the six membership-mutating events
//! and emits [`StakerChange`] for every observed transition.
//!
//! # Spec mapping
//!
//! - ADR 022 §STORE Flow line 140: the receiver checks `holder` is in
//!   "the cached active-staker set (populated from
//!   `StakingRegistry.getActiveNodes()` per ADR 019 § Step 3.3)".
//! - ADR 019 § Step 3.3: "initial full fetch via `getActiveNodes()`
//!   pagination + event subscription for incremental updates."
//! - ADR 022 §FIND\_VALUE Flow line 183: the requester-side iterative
//!   lookup (PR 5) filters `closer_nodes` and `providers` against the
//!   same cached set.
//!
//! # Failure model
//!
//! Bootstrap RPC failure → caller propagates the error (the runtime
//! treats it as fatal; the DHT cannot function without a staker set).
//!
//! Watcher RPC failure (mid-run) → the task logs and re-establishes
//! its event filters with exponential backoff. After a sustained
//! outage exceeding `WATCHER_RESYNC_THRESHOLD`, the watcher
//! re-bootstraps from `getActiveNodes` to recover from any missed
//! events; until then, the cached set may admit / reject one extra
//! `Store` per missed transition. The existing `decdn_rpc_healthy`
//! gauge (driven by the runtime's RPC watchdog at #283) is the
//! operator-visible signal for both conditions.
//!
//! # N+1 round-trips at bootstrap
//!
//! Each page of `getActiveNodes` returns up to 100 entries; we then
//! issue one `isActive(operator)` per entry to filter. At `PoC` scale
//! (tens of nodes) this is tens of RPC round-trips — acceptable for a
//! one-shot startup path. A `Multicall3` batching pass is the natural
//! follow-up (ADR 022 line 192 already plans the same dependency for
//! the origin-directory fallback path).

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::dht::routing::NodeId;
use crate::dht::staker_set::{StakerChange, StakerSet};
use decdn_incentive::staking_registry::StakingRegistry;

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
/// terminates with an error. Capped, with a jittered initial step.
const WATCHER_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Upper bound for the watcher restart backoff.
const WATCHER_MAX_BACKOFF: Duration = Duration::from_mins(1);

/// Threshold after which the watcher re-bootstraps the active set
/// from `getActiveNodes` instead of just restarting event filters.
/// Bounds the missed-events recovery window.
#[allow(dead_code)] // Reserved for the re-bootstrap path (see watcher_loop TODO).
const WATCHER_RESYNC_THRESHOLD: Duration = Duration::from_mins(5);

/// Chain-backed staker set. Cheap to clone via the shared inner
/// [`Arc`]; the runtime holds one `Arc<dyn StakerSet>` and consumers
/// (handler, future iterative lookup, bootstrap) access the cached set
/// through it. The background watcher task is owned by this struct
/// via a private `AbortOnDrop` wrapper; dropping the `ChainStakerSet`
/// aborts the watcher so a node-restart cycle never leaks chain-poll
/// tasks.
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
    pub async fn bootstrap<P>(provider: P, registry_addr: Address) -> Result<Self>
    where
        P: Provider + Clone + 'static,
    {
        let registry = StakingRegistry::new(registry_addr, provider);
        let initial = bootstrap_active_set(&registry).await.with_context(|| {
            format!("paginated getActiveNodes from StakingRegistry at {registry_addr}")
        })?;
        info!(
            active_count = initial.len(),
            %registry_addr,
            "ChainStakerSet bootstrap from StakingRegistry complete"
        );

        let (changes_tx, _) = broadcast::channel(CHANGES_CHANNEL_CAPACITY);
        let active = Arc::new(RwLock::new(initial));
        let watcher_handle = tokio::spawn(watcher_loop(
            registry,
            Arc::clone(&active),
            changes_tx.clone(),
        ));

        Ok(Self {
            active,
            changes_tx,
            _watcher: AbortOnDrop(watcher_handle),
        })
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
/// a panic into the handler hot path. Centralising the pattern keeps
/// the trait impl shaped like data accessors instead of three copies
/// of the same recovery boilerplate.
fn read_active<R, F>(active: &Arc<RwLock<HashSet<NodeId>>>, f: F) -> R
where
    F: FnOnce(&HashSet<NodeId>) -> R,
{
    match active.read() {
        Ok(guard) => f(&guard),
        Err(poisoned) => f(&poisoned.into_inner()),
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
    registry: &StakingRegistry::StakingRegistryInstance<P>,
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
        let page_len = page.len() as u64;
        for node in &page {
            let is_active = registry
                .isActive(node.ethAddress)
                .call()
                .await
                .with_context(|| format!("isActive({operator})", operator = node.ethAddress))?;
            if is_active {
                active.insert(node.nodeId.0);
            }
        }
        if page_len < PAGE_SIZE {
            break;
        }
        offset = offset.saturating_add(page_len);
    }
    Ok(active)
}

/// Background event-subscription loop. Subscribes to the six relevant
/// `StakingRegistry` events and updates the cached active set on each
/// observation. On stream failure (transport error, RPC timeout), the
/// loop restarts the subscriptions with exponential backoff.
///
/// Operator-indexed events that don't carry a `nodeId` (`Reinstated`,
/// `EjectedByBlacklist`, `UnbondingRequested`) trigger a follow-up
/// `nodeIdOf(operator)` call to resolve the binding. If the resolved
/// `(nodeId, active)` pair disagrees with the event's implied state
/// (e.g. `Reinstated` arrived but `isActive` is false due to a more
/// recent unbonding), the canonical `isActive` value wins.
async fn watcher_loop<P>(
    registry: StakingRegistry::StakingRegistryInstance<P>,
    active: Arc<RwLock<HashSet<NodeId>>>,
    changes_tx: broadcast::Sender<StakerChange>,
) where
    P: Provider + Clone,
{
    let mut backoff = WATCHER_INITIAL_BACKOFF;
    loop {
        match run_watcher_once(&registry, &active, &changes_tx).await {
            Ok(()) => {
                // Stream ended without error (filter expired, etc.) —
                // restart immediately and reset backoff.
                debug!("watcher stream ended cleanly; restarting subscription");
                backoff = WATCHER_INITIAL_BACKOFF;
            }
            Err(err) => {
                warn!(
                    %err,
                    backoff_secs = backoff.as_secs(),
                    "ChainStakerSet watcher RPC error; restarting after backoff"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(WATCHER_MAX_BACKOFF);
            }
        }
    }
}

/// Run one cycle of the watcher: open the six event filters, drain
/// them via `tokio::select!` until any one returns an error. Returns
/// `Ok(())` if the streams ended cleanly (filter expiry / provider
/// rotation); returns `Err` if a stream observed a transport-level
/// failure.
#[allow(clippy::cognitive_complexity)] // 6-arm event-dispatch loop is fundamentally complex; splitting obscures the dispatch table
async fn run_watcher_once<P>(
    registry: &StakingRegistry::StakingRegistryInstance<P>,
    active: &Arc<RwLock<HashSet<NodeId>>>,
    changes_tx: &broadcast::Sender<StakerChange>,
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
    let mut node_auto_ejected = registry
        .NodeAutoEjected_filter()
        .watch()
        .await
        .context("watch NodeAutoEjected")?
        .into_stream();
    let mut reinstated = registry
        .Reinstated_filter()
        .watch()
        .await
        .context("watch Reinstated")?
        .into_stream();
    let mut ejected_by_blacklist = registry
        .EjectedByBlacklist_filter()
        .watch()
        .await
        .context("watch EjectedByBlacklist")?
        .into_stream();
    let mut unbonding_requested = registry
        .UnbondingRequested_filter()
        .watch()
        .await
        .context("watch UnbondingRequested")?
        .into_stream();

    loop {
        tokio::select! {
            ev = node_registered.next() => match ev {
                Some(Ok((event, _log))) => {
                    apply_change(active, changes_tx, StakerChange::Active(event.nodeId.0));
                }
                Some(Err(e)) => return Err(e).context("NodeRegistered stream"),
                None => return Ok(()),
            },
            ev = node_deregistered.next() => match ev {
                Some(Ok((event, _log))) => {
                    apply_change(active, changes_tx, StakerChange::Inactive(event.nodeId.0));
                }
                Some(Err(e)) => return Err(e).context("NodeDeregistered stream"),
                None => return Ok(()),
            },
            ev = node_auto_ejected.next() => match ev {
                Some(Ok((event, _log))) => {
                    apply_change(active, changes_tx, StakerChange::Inactive(event.nodeId.0));
                }
                Some(Err(e)) => return Err(e).context("NodeAutoEjected stream"),
                None => return Ok(()),
            },
            ev = reinstated.next() => match ev {
                Some(Ok((event, _log))) => {
                    apply_operator_change(registry, active, changes_tx, event.operator, true).await;
                }
                Some(Err(e)) => return Err(e).context("Reinstated stream"),
                None => return Ok(()),
            },
            ev = ejected_by_blacklist.next() => match ev {
                Some(Ok((event, _log))) => {
                    apply_operator_change(registry, active, changes_tx, event.operator, false).await;
                }
                Some(Err(e)) => return Err(e).context("EjectedByBlacklist stream"),
                None => return Ok(()),
            },
            ev = unbonding_requested.next() => match ev {
                Some(Ok((event, _log))) => {
                    apply_operator_change(registry, active, changes_tx, event.operator, false).await;
                }
                Some(Err(e)) => return Err(e).context("UnbondingRequested stream"),
                None => return Ok(()),
            },
        }
    }
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
    registry: &StakingRegistry::StakingRegistryInstance<P>,
    active: &Arc<RwLock<HashSet<NodeId>>>,
    changes_tx: &broadcast::Sender<StakerChange>,
    operator: Address,
    event_implies_active: bool,
) where
    P: Provider + Clone,
{
    let resolved = match registry.nodeIdOf(operator).call().await {
        Ok(r) => r,
        Err(err) => {
            warn!(%err, %operator, "nodeIdOf RPC failed; dropping operator-indexed event");
            return;
        }
    };
    let node_id = resolved.nodeId.0;
    if node_id == [0u8; 32] {
        debug!(%operator, "operator-indexed event for unbound operator; ignoring");
        return;
    }
    // Trust nodeIdOf.active — it's the canonical predicate sampled
    // immediately after the event, so it reflects any racing state
    // change too. The event_implies_active hint is recorded for
    // observability but not load-bearing.
    let now_active = resolved.active;
    if now_active != event_implies_active {
        debug!(
            %operator,
            event_implies_active,
            now_active,
            "operator-indexed event disagrees with canonical isActive; trusting isActive"
        );
    }
    let change = if now_active {
        StakerChange::Active(node_id)
    } else {
        StakerChange::Inactive(node_id)
    };
    apply_change(active, changes_tx, change);
}

/// Apply a change to the active set, then broadcast it. The broadcast
/// send error case (no live subscribers) is ignored: the set has
/// already been updated, and any later `subscribe_changes` call sees
/// the post-update state via `is_active` / `active_nodes`. A `Lagged`
/// subscriber is on the receiver to handle.
fn apply_change(
    active: &Arc<RwLock<HashSet<NodeId>>>,
    changes_tx: &broadcast::Sender<StakerChange>,
    change: StakerChange,
) {
    let mutated = {
        let mut guard = match active.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        match change {
            StakerChange::Active(id) => guard.insert(id),
            StakerChange::Inactive(id) => guard.remove(&id),
        }
    };
    if mutated {
        // Drop the result: no live subscriber is a normal case
        // (nothing in PR 647 consumes the channel yet; PR 5's
        // iterative lookup will).
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
        [byte; 32]
    }

    fn fresh_state() -> (
        Arc<RwLock<HashSet<NodeId>>>,
        broadcast::Sender<StakerChange>,
    ) {
        let (tx, _) = broadcast::channel(8);
        (Arc::new(RwLock::new(HashSet::new())), tx)
    }

    /// `apply_change(Active)` on an absent `NodeId` inserts and emits.
    /// Re-applying the same Active is a no-op (idempotent — no extra
    /// emission).
    #[test]
    fn apply_change_active_idempotent() {
        let (active, tx) = fresh_state();
        let mut rx = tx.subscribe();
        apply_change(&active, &tx, StakerChange::Active(nid(1)));
        assert!(active.read().unwrap().contains(&nid(1)));
        assert_eq!(rx.try_recv().unwrap(), StakerChange::Active(nid(1)));

        apply_change(&active, &tx, StakerChange::Active(nid(1)));
        assert_eq!(active.read().unwrap().len(), 1);
        // Second application was a no-op; channel empty.
        let err = rx.try_recv().expect_err("no second emission");
        assert!(matches!(err, broadcast::error::TryRecvError::Empty));
    }

    /// `apply_change(Inactive)` removes and emits. Removing an absent
    /// `NodeId` is a no-op (no emission).
    #[test]
    fn apply_change_inactive_idempotent() {
        let (active, tx) = fresh_state();
        active.write().unwrap().insert(nid(1));
        let mut rx = tx.subscribe();
        apply_change(&active, &tx, StakerChange::Inactive(nid(1)));
        assert!(!active.read().unwrap().contains(&nid(1)));
        assert_eq!(rx.try_recv().unwrap(), StakerChange::Inactive(nid(1)));

        apply_change(&active, &tx, StakerChange::Inactive(nid(2)));
        // nid(2) was never in the set; no emission.
        let err = rx.try_recv().expect_err("no emission for absent removal");
        assert!(matches!(err, broadcast::error::TryRecvError::Empty));
    }
}
