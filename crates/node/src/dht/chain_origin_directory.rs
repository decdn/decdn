//! Chain-backed [`OriginDirectory`] implementation.
//!
//! Resolves a request's **namespace** to the set of currently-active operator
//! `NodeId`s authorised as origins for it, reading from an in-memory cache kept
//! current by a background `eth_getLogs`-polling task. The resolution chain (ADR
//! 022 § FIND\_VALUE Flow "Origin discovery", ADR 011 § Origin Assignment
//! Authority) is:
//!
//!   `OriginAssignment.getOrigins(namespaceId)` → `[operator...]`
//!     → `CapacityBond.nodeIdOf(operator)` → `NodeId` (binding only)
//!     → keep operators the shared [`StakerSet`] reports `active`
//!
//! The bare hash carries no origin information (ADR 002 § Retrieval by
//! namespace): the request supplies the namespace its content is published
//! under. `namespaceId == 0` has no authorized origins, so it resolves to no
//! origins here.
//!
//! Note the split on the last two steps: `nodeIdOf` is read **only** to resolve
//! the `operator → NodeId` binding (its `active` flag is ignored here), and the
//! `active == true` filter is applied at lookup time via the shared
//! [`StakerSet`] — which tracks the same `CapacityBond` active predicate but
//! stays current without an `OriginAssignment` event (an operator can unbond
//! while still listed as an origin). See "Why an event-fed cache" below.
//!
//! # Why an event-fed cache
//!
//! [`OriginDirectory::lookup_origins`] is synchronous and sits on the DHT
//! lookup-miss path, so it cannot issue RPC inline. Like
//! [`crate::dht::chain_staker_set`], this type snapshots current chain state at
//! bootstrap and then follows the membership-mutating events, serving every
//! lookup from the cache. Operator liveness is **not** re-tracked here — it is
//! delegated to the shared [`StakerSet`] (`is_active(node_id)` at lookup time),
//! so the cache only stores the `operator → NodeId` binding, never the
//! `active` bit (which can change without an `OriginAssignment` event).
//!
//! # Snapshot strategy
//!
//! `AssignmentActivated` names every namespace that has ever been given an
//! origin set, so the set of namespaces is discovered by replaying
//! `AssignmentActivated` logs from a configured start block (the
//! `OriginAssignment` deployment block; windowed to respect provider
//! `eth_getLogs` range caps). Each discovered namespace's `namespace → operators`
//! set is then snapshotted with a direct `getOrigins` point read (authoritative
//! current membership; a namespace whose set is now empty simply caches empty).
//!
//! The live path keeps that authoritative-read discipline: each
//! `OriginAssignment` event is a **signal to re-read `getOrigins` for the
//! affected namespace**, not a payload delta to apply. Because `getOrigins`
//! returns the current authoritative member set, neither cross-filter observation
//! order nor a single lost event can corrupt the cache — readers converge on
//! chain truth.
//!
//! # Failure model
//!
//! Bootstrap RPC failure → propagated (the runtime treats it as fatal; the
//! pull-through authorized-origin gate cannot be trusted without a complete
//! snapshot). Mirrors `ChainStakerSet::bootstrap`.
//!
//! The live tail runs on the shared `resumable_watcher` `eth_getLogs` poller
//! (#1092/#1106 — no `eth_newFilter`): one filter over the `OriginAssignment`
//! address, demuxed by `topic0`. Backfill and the live tail are one cursor loop
//! whose scan cursor is **persisted** (`CheckpointKey::Origin`, #1108), so a
//! restart resumes the `AssignmentActivated` replay floor rather than rescanning
//! from the deploy block. A namespace first activated below the resumed cursor is
//! re-surfaced by the live tail's next event for it; membership is always
//! authoritative via `getOrigins`, so no revoke is ever missed. A poll-tick RPC
//! failure backs off (1s → 60s) and re-scans the window on the next tick,
//! re-applying any event lost in the gap — surfaced by
//! `..._watcher_restarts_total` / `..._down_seconds`.
//!
//! A narrower drift source: a per-event `getOrigins` or `nodeIdOf` RPC failure
//! is surfaced by `decdn_origin_directory_watcher_resolve_failures_total`
//! (and a `warn!`). On an **activation** event the cache fail-closes for the
//! affected namespace (an un-added operator authorizes nothing); the failed
//! namespace is recorded and its `getOrigins` re-read retried at the end of every
//! poll tick until it succeeds (the tick fails → backs off while any retry is
//! outstanding), so the hole heals without waiting for another same-namespace
//! event — necessary because the persisted cursor may already have advanced past
//! the triggering log. On a **removal** event (revoke / prune) a failed re-read
//! falls back to a precise delta removal from the event payload, so a revoke is
//! never weaker than a direct delete even when `getOrigins` is unavailable (and
//! the namespace is still queued for the retry re-read).
//! `decdn_origin_directory_operator_count` tracks the live authorised-origin
//! surface to spot a frozen or collapsed cache.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use crate::chain_events::resumable_watcher::{
    self, Checkpoint, CursorStart, LogSink, WatcherConfig, WatcherHandle,
};
use crate::chain_events::shared_head::HeadSource;
use crate::chain_events::{REORG_MARGIN_BLOCKS, backfill_windows, check_backfill_range, timed};
use crate::dht::chain_projection::{ChainProjection, with_read, with_write};
use crate::dht::origin::OriginDirectory;
use crate::dht::routing::NodeId;
use crate::dht::staker_set::StakerSet;
use crate::metrics::{Metrics, metric_hook};
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::origin_assignment::OriginAssignment;
use decdn_incentive::{CheckpointKey, KeyedCheckpointStore};
// Event structs imported directly so the topic0 dispatch stays under the 100-col
// width (the fully-qualified `OriginAssignment::<Event>` paths overflow it).
use decdn_incentive::origin_assignment::OriginAssignment::{
    AssignmentActivated, AssignmentRevoked, BlacklistedAssignmentPruned,
};

/// Block-window size for the genesis `AssignmentActivated` log replay, passed as
/// the `span` to [`backfill_windows`]. Kept well under the common provider
/// `eth_getLogs` 10k-block cap so bootstrap works on range-limited RPCs without
/// a per-provider knob. This is a *deliberate* override of the shared
/// [`crate::chain_events::MAX_BACKFILL_BLOCK_SPAN`] (10k), not drift — the extra
/// 1k of margin is the point; don't "fix" the divergence by unifying the constant.
const REPLAY_WINDOW_BLOCKS: u64 = 9_000;

/// In-memory projection of the on-chain origin directory. All resolution logic
/// lives here as pure methods over the maps so it is unit-testable without a
/// chain. Held behind an [`RwLock`] in [`ChainOriginDirectory`].
#[derive(Debug, Default)]
struct DirectoryCache {
    /// `namespaceId → authorized operator addresses`. Snapshotted via `getOrigins`
    /// and kept current by authoritative `getOrigins` re-reads on each assignment
    /// event — never incremental deltas (see the module header). The only
    /// per-operator delete is the fail-closed [`delta_remove_origin`] fallback
    /// when a removal-triggered re-read errors. Namespace 0 is never inserted (it
    /// has no publisher and no origins), so it resolves to empty — enforced
    /// defensively at both insert sites (`bootstrap_cache`, `resync_namespace`), not
    /// merely trusted from the ABI-decoded event field.
    origins_of_ns: HashMap<U256, HashSet<Address>>,
    /// `operator address → bound NodeId` (from `CapacityBond.nodeIdOf`).
    /// Liveness is intentionally NOT stored — it is read live from the
    /// [`StakerSet`] at resolution time.
    operator_node: HashMap<Address, NodeId>,
}

impl DirectoryCache {
    /// Map an authorised operator address to its bound `NodeId`, but only if the
    /// [`StakerSet`] currently reports that node active. `None` when the operator
    /// has no cached binding or is not active.
    fn active_node_for(&self, op: &Address, staker_set: &dyn StakerSet) -> Option<NodeId> {
        self.operator_node
            .get(op)
            .copied()
            .filter(|node_id| staker_set.is_active(node_id))
    }

    /// Resolve `namespace_id` to currently-active origin `NodeId`s: the operators
    /// authorised for the namespace (`OriginAssignment.getOrigins`), mapped to
    /// active bound `NodeId`s. `namespace_id == 0` has no cached set and resolves
    /// to empty. Returned sorted + deduplicated for a deterministic order.
    fn resolve(&self, namespace_id: U256, staker_set: &dyn StakerSet) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = self
            .origins_of_ns
            .get(&namespace_id)
            .into_iter()
            .flatten()
            .filter_map(|op| self.active_node_for(op, staker_set))
            .collect();
        nodes.sort_unstable();
        nodes.dedup();
        nodes
    }

    /// Whether at least one active authorised origin exists for `namespace_id`.
    /// Iterates the underlying set directly and short-circuits on the first hit —
    /// no allocation, since the pull-through authorized-origin gate only tests
    /// emptiness on the (uncommon) lookup-miss path.
    fn has_any(&self, namespace_id: U256, staker_set: &dyn StakerSet) -> bool {
        self.origins_of_ns
            .get(&namespace_id)
            .into_iter()
            .flatten()
            .any(|op| self.active_node_for(op, staker_set).is_some())
    }
}

/// Names the projection in the poison-recovery `warn!` emitted by
/// [`read_cache`] / [`write_cache`].
const LABEL: &str = "ChainOriginDirectory cache";

/// Chain-backed origin directory. Cheap to clone via the shared inner [`Arc`];
/// the runtime holds one `Arc<dyn OriginDirectory>` and consumers resolve through
/// it. The background watcher task is owned via the projection so a node-restart
/// cycle never leaks chain-poll tasks.
#[derive(Debug)]
pub struct ChainOriginDirectory {
    proj: ChainProjection<DirectoryCache>,
    staker_set: Arc<dyn StakerSet>,
}

/// Contract handles the watcher needs, bundled so the bootstrap and watcher
/// share one construction site. Each is cheap to clone (wraps the provider).
struct Contracts<P: Provider + Clone> {
    origin: OriginAssignment::OriginAssignmentInstance<P>,
    bond: CapacityBond::CapacityBondInstance<P>,
}

/// The chain reads the live watcher performs, behind a trait so the event
/// handlers are unit-testable without a provider. The production implementation
/// is [`Contracts`]; tests supply a scripted stub.
///
/// `async fn` in a private trait carries no `Send` bound on its futures, but
/// every production call site monomorphizes `R = Contracts<P>`, whose alloy
/// futures are `Send` — so the spawned watcher future stays `Send`. The
/// `#[allow]` silences the lint that would otherwise nudge toward the
/// `-> impl Future + Send` form, which we don't need here.
#[allow(async_fn_in_trait)]
trait OriginChainReads {
    /// Authoritative current operator set for `namespace` (`getOrigins`).
    async fn get_origins(&self, namespace: U256) -> Result<Vec<Address>>;
    /// Bound `NodeId` for `operator`, or `None` when unbound (`bytes32(0)`).
    async fn node_id_of(&self, operator: Address) -> Result<Option<NodeId>>;
}

impl<P> OriginChainReads for Contracts<P>
where
    P: Provider + Clone,
{
    async fn get_origins(&self, namespace: U256) -> Result<Vec<Address>> {
        // Bounded here, in the production impl, rather than at the sink: a
        // timeout must surface as the same `Err` the sink's existing defer/retry
        // path already handles (`on_tick_complete` bails while any namespace is
        // deferred, driving the backoff). `StubReads` needs no timeout.
        timed(None, "getOrigins", self.origin.getOrigins(namespace).call())
            .await
            .with_context(|| format!("getOrigins(namespace={namespace})"))
    }

    async fn node_id_of(&self, operator: Address) -> Result<Option<NodeId>> {
        resolve_node_id(&self.bond, operator).await
    }
}

impl ChainOriginDirectory {
    /// Bootstrap: snapshot current chain state into the cache, then spawn the
    /// background event watcher. Returns once the cache is populated and the
    /// watcher is running — the watcher's own poll-tick failures do not fail
    /// bootstrap.
    ///
    /// A bootstrap RPC failure is propagated; the runtime treats it the same as
    /// the `ChainStakerSet` bootstrap (fatal — the pull-through authorized-origin
    /// gate cannot be trusted without a complete snapshot).
    ///
    /// The watcher owns its own shutdown token (minted by `resumable_watcher::
    /// spawn`); the runtime drives graceful stop via the returned directory's
    /// `watcher()` handle. That path matters here: the watcher
    /// persists its scan cursor through the (debounced) `checkpoint_store`, and
    /// only `shutdown()` (not the `AbortOnDrop` backstop) flushes the buffered
    /// tail (`CheckpointKey::Origin`) to disk — an abort-only teardown would
    /// silently drop up to a debounce window of progress on every clean stop.
    #[allow(clippy::too_many_arguments)]
    pub async fn bootstrap<P>(
        provider: P,
        origin_assignment_addr: Address,
        capacity_bond_addr: Address,
        from_block: u64,
        checkpoint_store: Arc<dyn KeyedCheckpointStore>,
        event_poll_interval: Duration,
        head: Arc<dyn HeadSource>,
        staker_set: Arc<dyn StakerSet>,
        metrics: Arc<Metrics>,
    ) -> Result<Self>
    where
        P: Provider + Clone + 'static,
    {
        let contracts = Contracts {
            origin: OriginAssignment::new(origin_assignment_addr, provider.clone()),
            bond: CapacityBond::new(capacity_bond_addr, provider.clone()),
        };

        // Resume the `AssignmentActivated` replay floor from the persisted cursor
        // (#1108), rewound by `REORG_MARGIN_BLOCKS` and floored at the deploy
        // block for shallow-reorg safety across restarts (#1238) — a checkpoint
        // written before a reorg re-enumerates the rewound span rather than
        // trusting a block that may have been orphaned. A restart rebuilds the
        // namespace set from there rather than from the deploy block. `None`
        // (first-ever boot) falls back to `from_block`. A namespace first
        // activated below the cursor is re-surfaced by its next live event;
        // membership is always authoritative via `getOrigins`, so no revoke is
        // ever missed.
        let replay_from = resume_replay_floor(checkpoint_store.as_ref(), from_block)?;
        let (cache, snapshot_block) = bootstrap_cache(&contracts, replay_from, &metrics)
            .await
            .context("snapshot OriginAssignment at bootstrap")?;
        info!(
            namespaces = cache.origins_of_ns.len(),
            operators = cache.operator_node.len(),
            replay_from,
            snapshot_block,
            "ChainOriginDirectory bootstrap snapshot complete"
        );
        metrics.origin_directory_operator_count(authorized_operator_count(&cache));

        let cache = Arc::new(RwLock::new(cache));
        // The live tail flows forward from the bootstrap snapshot block (bootstrap
        // already covered `[replay_from, snapshot_block]`), persisting the cursor
        // forward from there.
        let sink = OriginSink {
            contracts,
            cache: Arc::clone(&cache),
            metrics: Arc::clone(&metrics),
            deferred: HashSet::new(),
        };
        let cfg = WatcherConfig::new(
            head,
            Filter::new()
                .address(origin_assignment_addr)
                .event_signature(vec![
                    AssignmentActivated::SIGNATURE_HASH,
                    AssignmentRevoked::SIGNATURE_HASH,
                    BlacklistedAssignmentPruned::SIGNATURE_HASH,
                ]),
            cursor_start(snapshot_block, checkpoint_store),
            event_poll_interval,
            "origin-directory",
        )
        .with_from_block(from_block)
        .on_established(metric_hook(
            &metrics,
            Metrics::origin_directory_watcher_cycle_established,
        ))
        .on_backoff(metric_hook(
            &metrics,
            Metrics::origin_directory_watcher_backoff_started,
        ))
        .on_tick_success(metric_hook(
            &metrics,
            Metrics::origin_directory_watcher_tick,
        ))
        .on_task_panic(metric_hook(
            &metrics,
            Metrics::origin_directory_watcher_task_panicked,
        ));
        // This sink observes no shutdown token, so it ignores the one `spawn`
        // mints (`|_| sink`); the runtime drives graceful stop, then flushes the
        // origin scan checkpoint, via `watcher`.
        let watcher = Arc::new(resumable_watcher::spawn(provider, cfg, move |_| sink));

        Ok(Self {
            proj: ChainProjection::from_parts(cache, LABEL, watcher),
            staker_set,
        })
    }
}

/// Applies `OriginAssignment` logs to the directory cache (#1092). `apply`
/// demuxes by `topic0`. Each event is a *signal to re-read* the authoritative
/// chain set (`getOrigins`), not a delta — so cross-event reorder or a single
/// lost event cannot corrupt the cache, and the poller's re-scan on a getLogs
/// error re-applies any event lost in a filter-down gap. `apply` never returns
/// `Err` (a deterministic per-namespace failure must not stall the cursor):
/// a `getOrigins` re-read failure fail-closes the namespace and records it in
/// `deferred`; [`Self::on_tick_complete`] retries every deferred namespace each
/// tick and fails the tick (→ backoff) while any remain, so the re-read heals
/// on its own and a throttled provider gets backoff pressure instead of
/// full-cadence polling. An undecodable log is skipped.
struct OriginSink<P: Provider + Clone> {
    contracts: Contracts<P>,
    cache: Arc<RwLock<DirectoryCache>>,
    metrics: Arc<Metrics>,
    /// Namespaces whose authoritative `getOrigins` re-read failed. Bounded by
    /// the number of distinct namespaces. Retried in [`Self::on_tick_complete`]
    /// until each re-read succeeds; until then the affected namespace stays
    /// fail-closed (unpopulated → authorizes nothing).
    deferred: HashSet<U256>,
}

impl<P: Provider + Clone> LogSink for OriginSink<P> {
    async fn apply(&mut self, log: Log) -> Result<()> {
        // The subscribed filter is the OriginAssignment address only, so every
        // log demuxes by `topic0` in `apply_origin_event`.
        self.apply_origin_event(&log).await;
        Ok(())
    }

    /// Retry every deferred `getOrigins` re-read. Runs at the end of every tick
    /// (idle ones included), so a namespace whose re-read failed at apply time —
    /// after which the persisted cursor may already have advanced past the
    /// triggering event — heals here rather than staying fail-closed until an
    /// unrelated same-namespace event. Returns `Err` while any namespace is
    /// still failing so the tick backs off instead of re-polling a throttled
    /// provider at full cadence.
    async fn on_tick_complete(&mut self) -> Result<()> {
        if self.deferred.is_empty() {
            return Ok(());
        }
        let pending: Vec<U256> = self.deferred.iter().copied().collect();
        for namespace in pending {
            let resynced =
                resync_namespace(&self.contracts, &self.cache, &self.metrics, namespace).await;
            match resynced {
                Ok(()) => {
                    self.deferred.remove(&namespace);
                    info!(%namespace, "deferred getOrigins re-read healed");
                }
                Err(err) => {
                    self.metrics.origin_directory_watcher_resolve_failure();
                    warn!(
                        err = %sanitize_err_chain(&err),
                        %namespace,
                        "deferred getOrigins re-read still failing"
                    );
                }
            }
        }
        if self.deferred.is_empty() {
            Ok(())
        } else {
            anyhow::bail!(
                "{} deferred getOrigins re-read(s) still failing",
                self.deferred.len()
            )
        }
    }
}

impl<P: Provider + Clone> OriginSink<P> {
    /// Dispatch one `OriginAssignment` log by `topic0` (all decoded from the
    /// origin-assignment contract). Each arm re-reads the authoritative set.
    #[allow(clippy::cognitive_complexity)]
    async fn apply_origin_event(&mut self, log: &Log) {
        match log.topic0().copied() {
            Some(sig) if sig == AssignmentActivated::SIGNATURE_HASH => {
                match AssignmentActivated::decode_log_data(&log.inner.data) {
                    Ok(event) => {
                        on_namespace_changed(
                            &self.contracts,
                            &self.cache,
                            &self.metrics,
                            &mut self.deferred,
                            event.namespaceId,
                        )
                        .await;
                    }
                    Err(err) => {
                        // A signature-matched log that fails to decode is the
                        // symptom of the hand-written `sol!` binding drifting from
                        // the deployed contract (memory: "contract ABI drift is
                        // e2e-only"). Meter it so a systematic decode storm —
                        // silently dropping every event of a type — is observable on
                        // dashboards, not warn-log-only.
                        self.metrics.origin_directory_watcher_resolve_failure();
                        warn!(%err, "skipping undecodable AssignmentActivated log");
                    }
                }
            }
            Some(sig) if sig == AssignmentRevoked::SIGNATURE_HASH => {
                match AssignmentRevoked::decode_log_data(&log.inner.data) {
                    Ok(event) => {
                        on_origin_removed(
                            &self.contracts,
                            &self.cache,
                            &self.metrics,
                            &mut self.deferred,
                            event.namespaceId,
                            event.operator,
                        )
                        .await;
                    }
                    Err(err) => {
                        // See the AssignmentActivated arm: meter decode failures so
                        // an ABI-drift storm that silently stops applying revokes is
                        // visible, not warn-log-only.
                        self.metrics.origin_directory_watcher_resolve_failure();
                        warn!(%err, "skipping undecodable AssignmentRevoked log");
                    }
                }
            }
            Some(sig) if sig == BlacklistedAssignmentPruned::SIGNATURE_HASH => {
                match BlacklistedAssignmentPruned::decode_log_data(&log.inner.data) {
                    Ok(event) => {
                        on_origin_removed(
                            &self.contracts,
                            &self.cache,
                            &self.metrics,
                            &mut self.deferred,
                            event.namespaceId,
                            event.operator,
                        )
                        .await;
                    }
                    Err(err) => {
                        // See the AssignmentActivated arm: meter decode failures so
                        // an ABI-drift storm that silently stops applying prunes is
                        // visible, not warn-log-only.
                        self.metrics.origin_directory_watcher_resolve_failure();
                        warn!(%err, "skipping undecodable BlacklistedAssignmentPruned log");
                    }
                }
            }
            _ => {
                debug!(topic0 = ?log.topic0(), "unmatched OriginAssignment event in subscribed OR-set");
            }
        }
    }
}

/// The origin watcher's cursor start: **seed** the live tail from the bootstrap
/// snapshot block and **persist** the [`CheckpointKey::Origin`] cursor forward
/// each window (#1108). The historical range `[replay_from, snapshot_block]` is
/// covered by the enumeration `bootstrap` runs before the watcher spawns, so the
/// watcher does not resolve a floor — it starts at `at` and writes forward.
///
/// The reorg rewind is applied to `replay_from` (the enumeration floor), not
/// here: this watcher replays from an enumeration rather than the generic
/// `initial_from` path, so the durable checkpoint is rewound where it is read,
/// at bootstrap. Because the start no longer carries a `reorg_margin` /
/// `none_fallback` it never reads, the #1238 dead-field shape is gone — that is
/// why the split moved persistence (`persist`) apart from floor derivation.
fn cursor_start(at: u64, store: Arc<dyn KeyedCheckpointStore>) -> CursorStart {
    CursorStart::Seeded {
        at,
        persist: Some(Checkpoint {
            store,
            key: CheckpointKey::Origin,
        }),
    }
}

/// The `AssignmentActivated` replay floor for a bootstrap: the persisted
/// [`CheckpointKey::Origin`] checkpoint rewound `REORG_MARGIN_BLOCKS` for
/// shallow-reorg safety across restarts (#1238), floored at the deploy block;
/// `None` (first-ever boot) → the deploy block. Origin resolves this itself
/// rather than through the watcher's `initial_from` because it replays from an
/// enumeration, not the generic getLogs floor. Pure so the rewind is
/// unit-testable without a live provider.
const fn replay_floor(checkpoint: Option<u64>, from_block: u64) -> u64 {
    match checkpoint {
        Some(last) => {
            let rewound = last.saturating_sub(REORG_MARGIN_BLOCKS);
            if rewound > from_block {
                rewound
            } else {
                from_block
            }
        }
        None => from_block,
    }
}

/// Read the persisted [`CheckpointKey::Origin`] cursor and resolve the bootstrap
/// replay floor through [`replay_floor`]. A read error propagates (a failed
/// checkpoint read fails start, unchanged by #1238); a cold store degrades to
/// the deploy block. Wraps the read + rewind as one seam so `bootstrap`'s wiring
/// — that the durable cursor actually feeds the rewind — is unit-testable
/// without a live provider.
fn resume_replay_floor(store: &dyn KeyedCheckpointStore, from_block: u64) -> Result<u64> {
    let checkpoint = store
        .load_checkpoint(CheckpointKey::Origin)
        .context("read origin-directory scan checkpoint at bootstrap")?;
    Ok(replay_floor(checkpoint, from_block))
}

impl ChainOriginDirectory {
    /// The owned watcher handle, cloned so the runtime can drive graceful
    /// shutdown in its deliberate order (cancel, then flush the `Origin`
    /// checkpoint) — see `runtime`.
    pub(crate) fn watcher(&self) -> Arc<WatcherHandle> {
        self.proj.watcher()
    }
}

impl OriginDirectory for ChainOriginDirectory {
    fn lookup_origins(&self, namespace_id: U256) -> Vec<NodeId> {
        self.proj
            .read(|c| c.resolve(namespace_id, self.staker_set.as_ref()))
    }

    fn has_origin(&self, namespace_id: U256) -> bool {
        self.proj
            .read(|c| c.has_any(namespace_id, self.staker_set.as_ref()))
    }
}

/// Poison-tolerant `RwLock` read for the cache the sink and free helpers share,
/// delegating to the shared [`with_read`] so the recovery arm lives in one place
/// (#1255). The `Origin` cache is `T` for its own [`ChainProjection`], but the
/// event handlers below hold a bare `Arc<RwLock<DirectoryCache>>` (they run
/// inside the watcher task, so they cannot reach the projection), hence these
/// free wrappers.
fn read_cache<R, F>(cache: &Arc<RwLock<DirectoryCache>>, f: F) -> R
where
    F: FnOnce(&DirectoryCache) -> R,
{
    with_read(cache, LABEL, f)
}

/// Snapshot current chain state: replay `AssignmentActivated` to learn the set
/// of namespaces that have ever been assigned an origin set, then `getOrigins`
/// each for its authoritative current membership and resolve every operator's
/// `NodeId`.
async fn bootstrap_cache<P>(
    contracts: &Contracts<P>,
    replay_from_block: u64,
    metrics: &Metrics,
) -> Result<(DirectoryCache, u64)>
where
    P: Provider + Clone,
{
    let mut cache = DirectoryCache::default();

    // 1. Discover the namespace set, via windowed AssignmentActivated log replay
    //    starting at `replay_from_block`. Operators SHOULD set this to the
    //    OriginAssignment deployment block (`blockchain.origin_directory_from_block`);
    //    the default `0` is correct but scans the whole chain history in
    //    `REPLAY_WINDOW_BLOCKS` windows, which is slow / RPC-heavy on an
    //    established L2.
    let latest = contracts
        .origin
        .provider()
        .get_block_number()
        .await
        .context("get_block_number for AssignmentActivated replay")?;
    // The replay floor resumes from a persisted checkpoint (#1108), so a stale /
    // lagging RPC head can legitimately sit *behind* it (`replay_from_block >
    // latest`) — replication lag or a reorg. `backfill_windows` would silently
    // yield no windows for that inverted range, skipping the replay with no
    // signal. Do not propagate the error: `bootstrap` is one-shot (no retry) and
    // a fatal startup crash is strictly worse than graceful degradation. Instead
    // warn + bump a counter and degrade to an empty snapshot — namespaces are
    // re-surfaced once the live tail re-emits their assignment events (#1152).
    let mut namespaces: HashSet<U256> = HashSet::new();
    if let Err(err) = check_backfill_range(replay_from_block, latest) {
        warn!(
            %err, replay_from_block, latest,
            "origin-directory genesis replay: invalid range; skipping replay this boot \
             (empty snapshot until assignments re-surface via the live tail)"
        );
        metrics.origin_directory_bootstrap_range_anomaly();
    } else {
        for (from, to) in backfill_windows(replay_from_block, latest, REPLAY_WINDOW_BLOCKS) {
            let logs = contracts
                .origin
                .AssignmentActivated_filter()
                .from_block(from)
                .to_block(to)
                .query()
                .await
                .with_context(|| format!("query AssignmentActivated logs [{from}, {to}]"))?;
            for (event, _log) in logs {
                namespaces.insert(event.namespaceId);
            }
        }
    }

    // 2. namespace → operators, via getOrigins point reads (authoritative
    //    current set; avoids replaying assignment-mutation ordering). A namespace
    //    whose set was revoked to empty simply caches empty.
    for ns in namespaces {
        // Namespace 0 has no authorized origins by construction (`ownerOf(0) == 0`,
        // so `AssignmentActivated(0, …)` is unreachable). Defend the invariant here
        // rather than trust the ABI-decoded event field to never be 0 — a decode
        // drift must not be able to seat operators under `NO_NAMESPACE` and open the
        // gate for every request (ADR 002 §Namespace 0).
        if ns == U256::ZERO {
            continue;
        }
        let operators = contracts
            .origin
            .getOrigins(ns)
            .call()
            .await
            .with_context(|| format!("getOrigins(namespace={ns})"))?;
        cache
            .origins_of_ns
            .insert(ns, operators.into_iter().collect());
    }

    // 3. operator → NodeId for every operator we learned about.
    resolve_bootstrap_bindings(&contracts.bond, &mut cache, metrics).await;

    Ok((cache, latest))
}

/// Resolve every operator `bootstrap_cache` learned about to its `NodeId`,
/// filling `cache.operator_node`.
///
/// A failed lookup degrades — counted, warned, operator left unmapped — rather
/// than propagating, for the same reason the replay floor in `bootstrap_cache`
/// does: `bootstrap` is one-shot (no retry) and its error is fatal at
/// `runtime`'s call site, so a crash here is strictly worse than graceful
/// degradation. That matters more now `nodeIdOf` is bounded by [`timed`] —
/// propagating would turn one slow (>10s) call on a congested or rate-limited
/// endpoint (#1108) into a node that will not start, and this issues one such
/// call per authorised operator. An unmapped operator is a routing-only
/// degradation that self-heals: [`resolve_and_store_operators`] re-resolves it
/// on that operator's next event, which is the posture that path already takes
/// for this identical read.
async fn resolve_bootstrap_bindings<P>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    cache: &mut DirectoryCache,
    metrics: &Metrics,
) where
    P: Provider + Clone,
{
    let operators: HashSet<Address> = cache.origins_of_ns.values().flatten().copied().collect();
    for op in operators {
        match resolve_node_id(bond, op).await {
            Ok(Some(node_id)) => {
                cache.operator_node.insert(op, node_id);
            }
            Ok(None) => debug!(%op, "authorized operator has no NodeId binding; not probeable"),
            Err(err) => {
                metrics.origin_directory_watcher_resolve_failure();
                warn!(
                    err = %sanitize_err_chain(&err),
                    %op,
                    "nodeIdOf failed during bootstrap; operator unmapped until a later event"
                );
            }
        }
    }
}

/// Resolve an operator address to its bound `NodeId` via `nodeIdOf`. Returns
/// `Ok(None)` when the operator has no binding (`bytes32(0)`) — it cannot be a
/// probeable origin. The read is bounded by [`timed`]; RPC errors (including a
/// timeout) propagate to the caller.
///
/// Both callers absorb that error rather than fail on it — `bootstrap_cache`
/// because a one-shot startup path must not crash on a slow read, and
/// `resolve_and_store_operators` because the binding self-heals on the
/// operator's next event. Each bumps `origin_directory_watcher_resolve_failure`
/// and leaves the operator unmapped. Keep it that way: this returns `Result` so
/// the *decision* stays at the call site, not because failing is expected.
async fn resolve_node_id<P>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    operator: Address,
) -> Result<Option<NodeId>>
where
    P: Provider + Clone,
{
    let resolved = timed(None, "nodeIdOf", bond.nodeIdOf(operator).call())
        .await
        .with_context(|| format!("nodeIdOf({operator})"))?;
    let node_id = resolved.nodeId.0;
    if node_id == [0u8; 32] {
        Ok(None)
    } else {
        Ok(Some(NodeId::from_bytes(node_id)))
    }
}

/// Re-read a namespace's authoritative operator set (`getOrigins`) and replace
/// the cached set wholesale. The triggering event is treated only as a signal
/// that `namespace` changed — the chain, not the event payload, is the source of
/// truth, so observation order and lost single events cannot corrupt the cache.
/// The `getOrigins` RPC error propagates to the caller (which decides
/// defer-vs-fallback); a per-operator `nodeIdOf` failure is absorbed by
/// [`resolve_and_store_operators`] (bumped + skipped), unchanged from before.
async fn resync_namespace<R: OriginChainReads>(
    reads: &R,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    namespace: U256,
) -> Result<()> {
    // Namespace 0 authorizes nothing by construction (`ownerOf(0) == 0`), so an
    // `AssignmentActivated/Revoked(0, …)` is unreachable on-chain. Defend it here
    // rather than trust the decoded event field to never be 0 — seating operators
    // under `NO_NAMESPACE` would open the pull-through gate for every request
    // (ADR 002 §Namespace 0).
    if namespace == U256::ZERO {
        return Ok(());
    }
    let operators = reads.get_origins(namespace).await?;
    resolve_and_store_operators(reads, cache, metrics, &operators).await;
    write_cache(cache, |c| {
        c.origins_of_ns
            .insert(namespace, operators.into_iter().collect());
    });
    publish_authorized_count(cache, metrics);
    Ok(())
}

/// An addition/replace event for `namespace` (activation). Re-read the
/// authoritative set; on RPC failure, defer — an un-added operator authorizes
/// nothing, so failing closed is safe — and record the namespace for the
/// `on_tick_complete` retry.
async fn on_namespace_changed<R: OriginChainReads>(
    reads: &R,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    deferred: &mut HashSet<U256>,
    namespace: U256,
) {
    if let Err(err) = resync_namespace(reads, cache, metrics, namespace).await {
        metrics.origin_directory_watcher_resolve_failure();
        deferred.insert(namespace);
        warn!(err = %sanitize_err_chain(&err), %namespace, "getOrigins re-read failed on activation; deferred for retry");
    }
}

/// A removal event — `AssignmentRevoked` / `BlacklistedAssignmentPruned`.
/// Re-read the authoritative set; on RPC failure, fall back to the precise delta
/// removal from the event payload so a revoke is **never weaker** than a direct
/// delete even when `getOrigins` is unavailable — and still record the namespace
/// for the `on_tick_complete` retry, since the delta fallback leaves the rest of
/// the cached set potentially stale.
async fn on_origin_removed<R: OriginChainReads>(
    reads: &R,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    deferred: &mut HashSet<U256>,
    namespace: U256,
    operator: Address,
) {
    if let Err(err) = resync_namespace(reads, cache, metrics, namespace).await {
        metrics.origin_directory_watcher_resolve_failure();
        deferred.insert(namespace);
        warn!(err = %sanitize_err_chain(&err), %namespace, %operator, "getOrigins re-read failed on removal; applying precise delta fallback");
        delta_remove_origin(cache, metrics, namespace, operator);
    }
}

/// Precise single-operator removal, used as the fail-closed fallback when the
/// authoritative `getOrigins` re-read fails on a removal event. A `None` for a
/// namespace means it was never cached and already resolves to empty, so the
/// no-op is correct.
fn delta_remove_origin(
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    namespace: U256,
    operator: Address,
) {
    write_cache(cache, |c| {
        if let Some(set) = c.origins_of_ns.get_mut(&namespace) {
            set.remove(&operator);
        }
    });
    publish_authorized_count(cache, metrics);
}

/// Resolve any not-yet-cached operators to their `NodeId` and store the
/// bindings. A failed `nodeIdOf` bumps the resolve-failure counter and is
/// skipped (the operator can't be mapped until a later event re-surfaces it) —
/// the same silent-drift posture as `ChainStakerSet`.
async fn resolve_and_store_operators<R: OriginChainReads>(
    reads: &R,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    operators: &[Address],
) {
    for &op in operators {
        if read_cache(cache, |c| c.operator_node.contains_key(&op)) {
            continue;
        }
        match reads.node_id_of(op).await {
            Ok(Some(node_id)) => {
                write_cache(cache, |c| {
                    c.operator_node.insert(op, node_id);
                });
            }
            Ok(None) => debug!(%op, "authorized operator has no NodeId binding; not probeable"),
            Err(err) => {
                metrics.origin_directory_watcher_resolve_failure();
                warn!(err = %sanitize_err_chain(&err), %op, "nodeIdOf failed; operator unmapped until a later event");
            }
        }
    }
    // NB: the authorised-operator gauge is published by the calling event
    // handler AFTER it applies its set mutation — not here, where only the
    // binding cache changed (bindings don't alter the authorised set).
}

/// Distinct operator addresses currently authorised as origins — the union of
/// every namespace's operator set. Unlike the monotonic `operator_node` binding
/// cache, this rises on activate and falls on revoke/prune/replace, so it tracks
/// the directory's live authorised-origin surface.
fn authorized_operator_count(c: &DirectoryCache) -> usize {
    c.origins_of_ns
        .values()
        .flatten()
        .collect::<HashSet<_>>()
        .len()
}

/// Recompute and publish the authorised-operator gauge. Call AFTER an event
/// handler has applied its set mutation, so the gauge reflects the new state.
fn publish_authorized_count(cache: &Arc<RwLock<DirectoryCache>>, metrics: &Arc<Metrics>) {
    metrics.origin_directory_operator_count(read_cache(cache, authorized_operator_count));
}

/// Poison-tolerant `RwLock` write, mirroring [`read_cache`].
fn write_cache<R, F>(cache: &Arc<RwLock<DirectoryCache>>, f: F) -> R
where
    F: FnOnce(&mut DirectoryCache) -> R,
{
    with_write(cache, LABEL, f)
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
    use std::sync::RwLock as StdRwLock;

    /// Origin's `getOrigins` is bounded, so a stalled provider fails the tick
    /// into the existing defer/retry path instead of wedging it.
    ///
    /// This drives `Contracts<P>` — the *production* [`OriginChainReads`] impl —
    /// deliberately. Every other test in this module uses `StubReads`, which
    /// carries no `timed` wrap and would therefore pass whether or not the
    /// production impl bounds anything: a stub-level test here would look like a
    /// wiring test while asserting nothing about the wiring.
    #[tokio::test(start_paused = true)]
    async fn hanging_get_origins_fails_the_tick_rather_than_wedging() {
        use crate::chain_events::test_support::{bounded, hanging_provider};
        let provider = hanging_provider();
        let contracts = Contracts {
            origin: OriginAssignment::OriginAssignmentInstance::new(
                Address::ZERO,
                provider.clone(),
            ),
            bond: CapacityBond::CapacityBondInstance::new(Address::ZERO, provider),
        };
        let err = bounded("get_origins", contracts.get_origins(U256::from(1)))
            .await
            .err()
            .map(|e| format!("{e:#}"));
        assert!(
            err.as_ref()
                .is_some_and(|e| e.contains("getOrigins timed out after")),
            "a stalled getOrigins must fail into the backoff, not hang: {err:?}"
        );
    }

    /// Origin's `nodeIdOf` is bounded — but unlike `getOrigins` it fails no tick,
    /// so this asserts boundedness and nothing more. Both callers absorb the
    /// error: `resolve_and_store_operators` counts-and-skips, `bootstrap_cache`
    /// degrades rather than crash a one-shot startup. Hence "is bounded" and not
    /// "fails the tick" — the wedge is the bug; the policy above it is deliberate.
    ///
    /// Drives the free `resolve_node_id` rather than `Contracts<P>` because that
    /// is where the wrap lives and both callers route through it.
    #[tokio::test(start_paused = true)]
    async fn hanging_node_id_of_is_bounded_rather_than_wedging() {
        use crate::chain_events::test_support::{bounded, hanging_provider};
        let bond = CapacityBond::CapacityBondInstance::new(Address::ZERO, hanging_provider());
        let err = bounded("resolve_node_id", resolve_node_id(&bond, Address::ZERO))
            .await
            .err()
            .map(|e| format!("{e:#}"));
        assert!(
            err.as_ref()
                .is_some_and(|e| e.contains("nodeIdOf timed out after")),
            "a stalled nodeIdOf must be bounded, not hang: {err:?}"
        );
    }

    /// The operator-facing render must name *why* the read failed, not just what
    /// was attempted.
    ///
    /// `get_origins` wraps `timed` in `.with_context(…)`, and an `anyhow`
    /// Display renders only the outermost context — so logging it with
    /// `sanitize_rpc_display` prints a bare `getOrigins(namespace=1)` and drops
    /// "timed out after 10s", which is the entire product of bounding the read.
    /// This pins the *render*, deliberately: every wiring test above asserts on
    /// `format!("{e:#}")`, which no production log site uses, so all of them
    /// would pass while the operator's log said nothing at all.
    #[tokio::test(start_paused = true)]
    async fn stalled_get_origins_renders_the_timeout_to_the_operator() {
        use crate::chain_events::test_support::{bounded, hanging_provider};
        let provider = hanging_provider();
        let contracts = Contracts {
            origin: OriginAssignment::OriginAssignmentInstance::new(
                Address::ZERO,
                provider.clone(),
            ),
            bond: CapacityBond::CapacityBondInstance::new(Address::ZERO, provider),
        };
        let err = bounded("get_origins", contracts.get_origins(U256::from(1)))
            .await
            .unwrap_err();

        let rendered = sanitize_err_chain(&err);
        assert!(
            rendered.contains("getOrigins timed out after"),
            "the operator must see why it failed, not only what was attempted: {rendered}"
        );
        assert!(
            rendered.contains("namespace=1"),
            "the context must survive alongside the cause: {rendered}"
        );
    }

    /// A stalled `nodeIdOf` during bootstrap must degrade, not propagate.
    ///
    /// This is the other half of the policy split at [`resolve_node_id`]'s two
    /// callers, and the reason the bound there could not simply be inherited:
    /// `bootstrap_cache`'s error is fatal at `runtime`'s call site, so before
    /// this degraded, bounding the read turned one slow (>10s) call on a
    /// congested endpoint into a node that would not start at all.
    #[tokio::test(start_paused = true)]
    async fn hanging_bootstrap_binding_degrades_rather_than_failing_start() {
        use crate::chain_events::test_support::{bounded, hanging_provider};
        let bond = CapacityBond::CapacityBondInstance::new(Address::ZERO, hanging_provider());
        let metrics = Metrics::new();
        let mut cache = DirectoryCache::default();
        cache
            .origins_of_ns
            .insert(ns(1), std::iter::once(addr(1)).collect());

        bounded(
            "resolve_bootstrap_bindings",
            resolve_bootstrap_bindings(&bond, &mut cache, &metrics),
        )
        .await;

        assert!(
            cache.operator_node.is_empty(),
            "a stalled nodeIdOf must leave the operator unmapped, not bind it"
        );
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_origin_directory_watcher_resolve_failures_total 1"),
            "the degraded binding must be counted, not silently dropped:\n{text}"
        );
    }

    fn nid(b: u8) -> NodeId {
        NodeId::from_bytes([b; 32])
    }
    fn addr(b: u8) -> Address {
        Address::from([b; 20])
    }
    fn ns(n: u64) -> U256 {
        U256::from(n)
    }

    /// Stub staker set with a fixed active set, for resolution tests.
    #[derive(Debug)]
    struct StubStakers(StdRwLock<HashSet<NodeId>>);

    impl StubStakers {
        fn new(active: &[NodeId]) -> Self {
            Self(StdRwLock::new(active.iter().copied().collect()))
        }
    }

    impl StakerSet for StubStakers {
        fn is_active(&self, node_id: &NodeId) -> bool {
            self.0.read().unwrap().contains(node_id)
        }
        fn active_nodes(&self) -> Vec<NodeId> {
            self.0.read().unwrap().iter().copied().collect()
        }
        fn len(&self) -> usize {
            self.0.read().unwrap().len()
        }
    }

    /// Seed a cache: namespace→authorized operators and operator→NodeId
    /// bindings.
    fn cache_with(
        ns_origins: &[(u64, &[Address])],
        bindings: &[(Address, NodeId)],
    ) -> DirectoryCache {
        let mut c = DirectoryCache::default();
        for (n, ops) in ns_origins {
            c.origins_of_ns
                .insert(ns(*n), ops.iter().copied().collect());
        }
        c.operator_node = bindings.iter().copied().collect();
        c
    }

    /// Scripted [`OriginChainReads`]: per-namespace authoritative operator sets,
    /// operator→NodeId bindings,
    /// a head block, and an injectable set of namespaces whose `get_origins`
    /// returns an error (to exercise the defer / delta-fallback paths). The
    /// origin sets and failure set are behind locks so a test can mutate the
    /// authoritative state between an event and a later re-read.
    struct StubReads {
        origins: StdRwLock<HashMap<U256, Vec<Address>>>,
        bindings: HashMap<Address, NodeId>,
        fail_origins: StdRwLock<HashSet<U256>>,
    }

    impl StubReads {
        fn new() -> Self {
            Self {
                origins: StdRwLock::new(HashMap::new()),
                bindings: HashMap::new(),
                fail_origins: StdRwLock::new(HashSet::new()),
            }
        }
        fn origins(mut self, ns_ops: &[(u64, &[Address])]) -> Self {
            self.origins = StdRwLock::new(
                ns_ops
                    .iter()
                    .map(|(n, ops)| (ns(*n), ops.to_vec()))
                    .collect(),
            );
            self
        }
        fn bindings(mut self, b: &[(Address, NodeId)]) -> Self {
            self.bindings = b.iter().copied().collect();
            self
        }
        fn fail_origins(self, nss: &[u64]) -> Self {
            *self.fail_origins.write().unwrap() = nss.iter().map(|n| ns(*n)).collect();
            self
        }
    }

    impl OriginChainReads for StubReads {
        async fn get_origins(&self, namespace: U256) -> Result<Vec<Address>> {
            if self.fail_origins.read().unwrap().contains(&namespace) {
                return Err(anyhow::anyhow!(
                    "injected getOrigins failure for {namespace}"
                ));
            }
            Ok(self
                .origins
                .read()
                .unwrap()
                .get(&namespace)
                .cloned()
                .unwrap_or_default())
        }
        async fn node_id_of(&self, operator: Address) -> Result<Option<NodeId>> {
            Ok(self.bindings.get(&operator).copied())
        }
    }

    fn shared(c: DirectoryCache) -> Arc<RwLock<DirectoryCache>> {
        Arc::new(RwLock::new(c))
    }

    #[test]
    fn namespace_resolves_to_its_operators() {
        let c = cache_with(
            &[(7, &[addr(0xA), addr(0xB)])],
            &[(addr(0xA), nid(0xA)), (addr(0xB), nid(0xB))],
        );
        let stakers = StubStakers::new(&[nid(0xA), nid(0xB)]);
        // `resolve` returns sorted + deduplicated, so assert order directly.
        assert_eq!(c.resolve(ns(7), &stakers), vec![nid(0xA), nid(0xB)]);
        assert!(c.has_any(ns(7), &stakers));
    }

    #[test]
    fn inactive_operators_are_filtered_out() {
        let c = cache_with(
            &[(7, &[addr(0xA), addr(0xB)])],
            &[(addr(0xA), nid(0xA)), (addr(0xB), nid(0xB))],
        );
        // Only A is active; B is bonded-but-inactive (e.g. unbonding).
        let stakers = StubStakers::new(&[nid(0xA)]);
        assert_eq!(c.resolve(ns(7), &stakers), vec![nid(0xA)]);
        assert!(c.has_any(ns(7), &stakers));
    }

    #[test]
    fn operator_without_binding_is_dropped() {
        // Operator B is authorised but has no NodeId binding cached → unmapped.
        let c = cache_with(&[(7, &[addr(0xA), addr(0xB)])], &[(addr(0xA), nid(0xA))]);
        let stakers = StubStakers::new(&[nid(0xA), nid(0xB)]);
        assert_eq!(c.resolve(ns(7), &stakers), vec![nid(0xA)]);
    }

    #[test]
    fn has_any_agrees_with_resolve_emptiness() {
        // ns 7 has an operator, ns 8 is empty, ns 9 was never cached.
        let c = cache_with(&[(7, &[addr(0xA)]), (8, &[])], &[(addr(0xA), nid(0xA))]);
        let stakers = StubStakers::new(&[nid(0xA)]);
        for namespace in [ns(7), ns(8), ns(9)] {
            assert_eq!(
                c.has_any(namespace, &stakers),
                !c.resolve(namespace, &stakers).is_empty(),
                "has_any must agree with resolve emptiness for {namespace}"
            );
        }
    }

    // ---- Cache-mutation (event-application) semantics, exercised directly on
    //      the pure cache the way the watcher's apply helpers mutate it. ----

    #[test]
    fn assignment_activated_replaces_namespace_set() {
        let mut c = cache_with(&[(7, &[addr(0xA)])], &[]);
        // Wholesale replace 7's set with {B, C}.
        c.origins_of_ns
            .insert(ns(7), [addr(0xB), addr(0xC)].into_iter().collect());
        let set = &c.origins_of_ns[&ns(7)];
        assert!(!set.contains(&addr(0xA)));
        assert!(set.contains(&addr(0xB)) && set.contains(&addr(0xC)));
    }

    #[test]
    fn revoke_removes_single_operator() {
        let mut c = cache_with(&[(7, &[addr(0xA), addr(0xB)])], &[]);
        c.origins_of_ns.get_mut(&ns(7)).unwrap().remove(&addr(0xA));
        let set = &c.origins_of_ns[&ns(7)];
        assert!(!set.contains(&addr(0xA)));
        assert!(set.contains(&addr(0xB)));
    }

    // ---- Metric-wiring tests: the drift-surfacing guarantee this type exists
    //      to provide. Exercise the metric methods directly (the real watcher
    //      needs a chain endpoint), mirroring `chain_staker_set.rs`. ----

    #[test]
    fn watcher_restart_counts_one_per_drift_window() {
        let metrics = Arc::new(Metrics::new());
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_origin_directory_watcher_restarts_total 0"),
            "restart counter should start at zero:\n{text}"
        );

        // One continuous outage = three failed re-opens (backoff iterations)
        // with no intervening cycle → counts exactly once (edge-triggered).
        metrics.origin_directory_watcher_backoff_started();
        metrics.origin_directory_watcher_backoff_started();
        metrics.origin_directory_watcher_backoff_started();
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_origin_directory_watcher_restarts_total 1"),
            "one continuous outage should count exactly one restart:\n{text}"
        );

        // Filters re-establish (window closes), then a second outage → counts again.
        metrics.origin_directory_watcher_cycle_established();
        metrics.origin_directory_watcher_backoff_started();
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_origin_directory_watcher_restarts_total 2"),
            "a second distinct outage should count a second restart:\n{text}"
        );
    }

    #[test]
    fn watcher_resolve_failure_bumps_counter() {
        let metrics = Arc::new(Metrics::new());
        metrics.origin_directory_watcher_resolve_failure();
        metrics.origin_directory_watcher_resolve_failure();
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_origin_directory_watcher_resolve_failures_total 2"),
            "expected 2 resolve failures:\n{text}"
        );
    }

    #[test]
    fn operator_count_gauge_reflects_authorized_count_and_drops() {
        let metrics = Arc::new(Metrics::new());
        let mut c = cache_with(&[(7, &[addr(0xA), addr(0xB)])], &[]);
        // {A, B} → 2.
        metrics.origin_directory_operator_count(authorized_operator_count(&c));
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_origin_directory_operator_count 2"),
            "gauge should reflect authorised count 2:\n{text}"
        );
        // Revoke B from ns 7 and re-publish: the gauge must DROP (the bug the
        // fix addressed — the old gauge used the monotonic binding cache).
        c.origins_of_ns.get_mut(&ns(7)).unwrap().remove(&addr(0xB));
        metrics.origin_directory_operator_count(authorized_operator_count(&c));
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_origin_directory_operator_count 1"),
            "gauge should drop to 1 after a revoke:\n{text}"
        );
    }

    #[test]
    fn authorized_operator_count_reflects_current_sets_and_decreases() {
        // Distinct addresses across namespaces: {A, B} in ns 7, {C} in ns 8 = 3.
        let mut c = cache_with(&[(7, &[addr(0xA), addr(0xB)]), (8, &[addr(0xC)])], &[]);
        assert_eq!(authorized_operator_count(&c), 3);
        // A revoke must DECREASE the count (the bug this fix addresses: the old
        // gauge counted the monotonic binding cache and never dropped).
        c.origins_of_ns.get_mut(&ns(7)).unwrap().remove(&addr(0xB));
        assert_eq!(authorized_operator_count(&c), 2, "B removed → {{A, C}}");
        c.origins_of_ns.get_mut(&ns(7)).unwrap().remove(&addr(0xA));
        assert_eq!(authorized_operator_count(&c), 1, "A removed → {{C}}");
    }

    // ---- Authoritative re-read semantics (#855): events trigger a getOrigins
    //      re-read, not a payload-delta apply. Driven by the StubReads seam. ----

    #[tokio::test]
    async fn reorder_revoke_then_activate_converges_to_chain_truth() {
        // Scenario 2: both events re-read authoritative getOrigins(7). Chain truth
        // is {A} (C already revoked on-chain), so regardless of which event the
        // select observes first, C must not be resurrected.
        let metrics = Arc::new(Metrics::new());
        let reads = StubReads::new()
            .origins(&[(7, &[addr(0xA)])])
            .bindings(&[(addr(0xA), nid(0xA))]);
        // revoke-first then activate (the dangerous order).
        let cache = shared(cache_with(
            &[(7, &[addr(0xA), addr(0xC)])],
            &[(addr(0xA), nid(0xA)), (addr(0xC), nid(0xC))],
        ));
        let mut deferred = HashSet::new();
        on_origin_removed(&reads, &cache, &metrics, &mut deferred, ns(7), addr(0xC)).await;
        on_namespace_changed(&reads, &cache, &metrics, &mut deferred, ns(7)).await;
        read_cache(&cache, |c| {
            assert!(
                !c.origins_of_ns[&ns(7)].contains(&addr(0xC)),
                "revoke-then-activate"
            );
        });
        // activate-first then revoke (the other order) — same result.
        let cache = shared(cache_with(
            &[(7, &[addr(0xA), addr(0xC)])],
            &[(addr(0xA), nid(0xA)), (addr(0xC), nid(0xC))],
        ));
        on_namespace_changed(&reads, &cache, &metrics, &mut deferred, ns(7)).await;
        on_origin_removed(&reads, &cache, &metrics, &mut deferred, ns(7), addr(0xC)).await;
        assert!(deferred.is_empty(), "no failures → nothing deferred");
        read_cache(&cache, |c| {
            assert!(
                !c.origins_of_ns[&ns(7)].contains(&addr(0xC)),
                "activate-then-revoke"
            );
        });
    }

    #[tokio::test]
    async fn removal_falls_back_to_delta_when_get_origins_fails() {
        // A revoke whose authoritative re-read errors must STILL drop the operator
        // via the precise delta fallback (a revoke is never weaker than today).
        let metrics = Arc::new(Metrics::new());
        let cache = shared(cache_with(
            &[(7, &[addr(0xA), addr(0xC)])],
            &[(addr(0xA), nid(0xA)), (addr(0xC), nid(0xC))],
        ));
        let reads = StubReads::new().fail_origins(&[7]);
        let mut deferred = HashSet::new();
        on_origin_removed(&reads, &cache, &metrics, &mut deferred, ns(7), addr(0xC)).await;
        read_cache(&cache, |c| {
            assert!(
                !c.origins_of_ns[&ns(7)].contains(&addr(0xC)),
                "delta fallback must remove the revoked operator despite the RPC failure"
            );
            assert!(
                c.origins_of_ns[&ns(7)].contains(&addr(0xA)),
                "others untouched"
            );
        });
        assert!(
            deferred.contains(&ns(7)),
            "failed removal re-read must queue the namespace for the tick retry"
        );
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_origin_directory_watcher_resolve_failures_total 1"),
            "the failed re-read must bump the resolve-failure counter:\n{text}"
        );
    }

    #[tokio::test]
    async fn addition_defers_safely_when_get_origins_fails() {
        // An activation whose re-read errors leaves the set unchanged (fail-closed,
        // safe — an un-added operator authorizes nothing) and self-heals later.
        let metrics = Arc::new(Metrics::new());
        let cache = shared(cache_with(&[(7, &[addr(0xA)])], &[]));
        let reads = StubReads::new()
            .origins(&[(7, &[addr(0xA), addr(0xB)])])
            .bindings(&[(addr(0xA), nid(0xA)), (addr(0xB), nid(0xB))])
            .fail_origins(&[7]);
        let mut deferred = HashSet::new();
        on_namespace_changed(&reads, &cache, &metrics, &mut deferred, ns(7)).await;
        read_cache(&cache, |c| {
            assert_eq!(
                c.origins_of_ns[&ns(7)].len(),
                1,
                "set unchanged on failed add"
            );
        });
        assert!(
            deferred.contains(&ns(7)),
            "failed activation re-read must queue the namespace for the tick retry"
        );
        // The injected failure clears → a later activation heals to {A, B}.
        reads.fail_origins.write().unwrap().clear();
        on_namespace_changed(&reads, &cache, &metrics, &mut deferred, ns(7)).await;
        read_cache(&cache, |c| {
            assert!(
                c.origins_of_ns[&ns(7)].contains(&addr(0xB)),
                "heals on next event"
            );
        });
    }

    #[tokio::test]
    async fn namespace_zero_event_never_seats_operators() {
        // Defense-in-depth guard (`resync_namespace`): even if a decode drift
        // produced an `AssignmentActivated(0, …)` and `getOrigins(0)` answered
        // with operators, namespace 0 (`NO_NAMESPACE`) must NEVER gain a cached
        // set — seating one would open the pull-through gate for every
        // unnamespaced request (ADR 002 §Namespace 0). This asserts the guard,
        // not the on-chain unreachability the contract test already pins.
        let metrics = Arc::new(Metrics::new());
        // Hostile stub: namespace 0 "resolves" to an operator with a live binding.
        let reads = StubReads::new()
            .origins(&[(0, &[addr(0xA)])])
            .bindings(&[(addr(0xA), nid(0xA))]);
        let cache = shared(DirectoryCache::default());
        let mut deferred = HashSet::new();

        on_namespace_changed(&reads, &cache, &metrics, &mut deferred, ns(0)).await;

        let stakers = StubStakers::new(&[nid(0xA)]);
        read_cache(&cache, |c| {
            assert!(
                !c.origins_of_ns.contains_key(&ns(0)),
                "namespace 0 must never be seated in the directory cache"
            );
            // The load-bearing consequence: the gate stays closed for NO_NAMESPACE.
            assert!(
                c.resolve(U256::ZERO, &stakers).is_empty(),
                "NO_NAMESPACE must resolve to no origins"
            );
            assert!(
                !c.has_any(U256::ZERO, &stakers),
                "NO_NAMESPACE must not authorize any origin"
            );
        });
        assert!(
            deferred.is_empty(),
            "the ns-0 guard short-circuits with Ok(()), so nothing is deferred/retried"
        );
    }

    /// POLICY PIN: the origin watcher seeds the live tail from its bootstrap
    /// snapshot block and persists `CheckpointKey::Origin` forward. The historical
    /// range is covered by the enumeration `bootstrap` runs first; the
    /// cross-restart reorg rewind lives on `replay_from` (see `replay_floor`),
    /// not on the seeded start.
    #[test]
    fn cursor_start_is_seeded_with_origin_persist() {
        struct NoopCheckpointStore;
        impl KeyedCheckpointStore for NoopCheckpointStore {
            fn load_checkpoint(
                &self,
                _key: CheckpointKey,
            ) -> Result<Option<u64>, decdn_incentive::StoreError> {
                Ok(None)
            }
            fn record_checkpoint(
                &self,
                _key: CheckpointKey,
                _block: u64,
            ) -> Result<(), decdn_incentive::StoreError> {
                Ok(())
            }
        }
        let store: Arc<dyn KeyedCheckpointStore> = Arc::new(NoopCheckpointStore);
        assert!(matches!(
            cursor_start(1234, store),
            CursorStart::Seeded {
                at: 1234,
                persist: Some(Checkpoint {
                    key: CheckpointKey::Origin,
                    ..
                }),
            }
        ));
    }

    /// GUARDRAIL (#1238): a resumed origin boot re-enumerates `REORG_MARGIN_BLOCKS`
    /// below the persisted cursor so a shallow reorg near the checkpoint cannot
    /// orphan claims across a restart. Pins that the rewind *engages* — before
    /// #1238 `replay_from` was the raw checkpoint with no rewind, so this would
    /// read `10_000`.
    #[test]
    fn replay_floor_rewinds_checkpoint_by_reorg_margin() {
        assert_eq!(
            replay_floor(Some(10_000), 500),
            10_000 - REORG_MARGIN_BLOCKS
        );
    }

    /// The rewound floor never drops below the deploy block.
    #[test]
    fn replay_floor_floored_at_deploy_block() {
        assert_eq!(replay_floor(Some(REORG_MARGIN_BLOCKS), 500), 500);
    }

    /// A first-ever boot (cold store) replays from the deploy block.
    #[test]
    fn replay_floor_cold_boot_is_deploy_block() {
        assert_eq!(replay_floor(None, 500), 500);
    }

    /// GUARDRAIL (#1238) — the *wiring*, not just the arithmetic: `bootstrap`
    /// resolves its replay floor by reading the persisted `CheckpointKey::Origin`
    /// cursor and applying the rewind. The `replay_floor_*` tests pin the pure
    /// math; this pins that the durable read feeds it, so a call site that
    /// dropped the rewind (the pre-#1238 raw `unwrap_or`) fails here — it would
    /// read `10_000`, not `10_000 - REORG_MARGIN_BLOCKS`.
    #[test]
    fn resume_replay_floor_rewinds_the_persisted_origin_checkpoint() {
        /// Returns a fixed checkpoint, asserting the `Origin` key is the one read.
        struct SeededOriginStore(u64);
        impl KeyedCheckpointStore for SeededOriginStore {
            fn load_checkpoint(
                &self,
                key: CheckpointKey,
            ) -> Result<Option<u64>, decdn_incentive::StoreError> {
                assert_eq!(
                    key,
                    CheckpointKey::Origin,
                    "resume must read the Origin cursor"
                );
                Ok(Some(self.0))
            }
            fn record_checkpoint(
                &self,
                _key: CheckpointKey,
                _block: u64,
            ) -> Result<(), decdn_incentive::StoreError> {
                Ok(())
            }
        }
        let floor = resume_replay_floor(&SeededOriginStore(10_000), 500).ok();
        assert_eq!(floor, Some(10_000 - REORG_MARGIN_BLOCKS));
    }

    #[test]
    fn delta_remove_origin_unknown_namespace_is_a_noop() {
        // The fail-closed fallback for a namespace never cached must not panic or
        // create an entry — the operator already resolves to empty there.
        let metrics = Arc::new(Metrics::new());
        let cache = shared(cache_with(&[(7, &[addr(0xA)])], &[]));
        delta_remove_origin(&cache, &metrics, ns(99), addr(0xB));
        read_cache(&cache, |c| {
            assert!(
                !c.origins_of_ns.contains_key(&ns(99)),
                "no phantom entry created"
            );
            assert!(
                c.origins_of_ns[&ns(7)].contains(&addr(0xA)),
                "other namespaces untouched"
            );
        });
    }
}
