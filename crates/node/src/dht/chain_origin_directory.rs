//! Chain-backed [`OriginDirectory`] implementation.
//!
//! Resolves a content hash to the set of currently-active operator `NodeId`s
//! authorised as origins for it, reading from an in-memory cache kept current
//! by a background `eth_getLogs`-polling task. The resolution chain (ADR 022
//! § FIND\_VALUE Flow "Origin discovery", ADR 011 § Origin Assignment
//! Authority) is:
//!
//!   `PublisherRegistry.namespaceOf(H)` → `[namespaceId...]`
//!     → `OriginAssignment.getOrigins(namespaceId)` → `[operator...]`
//!     → `CapacityBond.nodeIdOf(operator)` → `NodeId` (binding only)
//!     → keep operators the shared [`StakerSet`] reports `active`
//!
//! Note the split on the last two steps: `nodeIdOf` is read **only** to resolve
//! the `operator → NodeId` binding (its `active` flag is ignored here), and the
//! `active == true` filter is applied at lookup time via the shared
//! [`StakerSet`] — which tracks the same `CapacityBond` active predicate but
//! stays current without an `OriginAssignment` event (an operator can unbond
//! while still listed as an origin). See "Why an event-fed cache" below.
//!
//! Per ADR 022, a hash with **no** claiming namespace falls back to the
//! default-open allow-list (`OriginAssignment.getOrigins(0)`); a hash that is
//! neither claimed nor default-open resolves to no origins.
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
//! `ContentClaimed` is the only on-chain write mapping a hash to a namespace,
//! so the `hash → namespaces` view is built by replaying `ContentClaimed` logs
//! from a configured start block (the `PublisherRegistry` deployment block;
//! windowed to respect provider `eth_getLogs` range caps). The
//! `namespace → operators` and default-open sets are snapshotted with direct
//! `getOrigins` point reads for every namespace the replay surfaced (plus
//! namespace 0).
//!
//! The live path keeps that authoritative-read discipline: each
//! `OriginAssignment` / `PublisherRegistry` event is a **signal to re-read
//! `getOrigins` for the affected namespace (or default-open set)**, not a payload
//! delta to apply. Because `getOrigins` returns the current authoritative member
//! set, neither cross-filter observation order nor a single lost event can
//! corrupt the cache — readers converge on chain truth. This subsumes the
//! `DefaultOpenAllowlistUpdated` `updateIndex` version field (#855).
//!
//! # Failure model
//!
//! Bootstrap RPC failure → propagated (the runtime treats it as fatal; the
//! prefetch authorized-origin gate cannot be trusted without a complete
//! snapshot). Mirrors `ChainStakerSet::bootstrap`.
//!
//! The live tail runs on the shared `resumable_watcher` `eth_getLogs` poller
//! (#1092/#1106 — no `eth_newFilter`): one filter over both contract addresses,
//! demuxed by `(address, topic0)`. Backfill and the live tail are one cursor loop
//! whose scan cursor is **persisted** (`CheckpointKey::Origin`, #1108), so a
//! restart resumes the `ContentClaimed` replay floor rather than rescanning from
//! the deploy block. A claim below the resumed cursor resolves as unclaimed
//! (→ default-open) until re-surfaced — a routing-only degradation, since
//! `getOrigins` keeps membership authoritative, so no revoke is ever missed. A
//! poll-tick RPC failure backs off (1s → 60s) and re-scans the window on the next
//! tick, re-applying any event lost in the gap — surfaced by
//! `..._watcher_restarts_total` / `..._down_seconds`.
//!
//! A narrower drift source: a per-event `getOrigins` or `nodeIdOf` RPC failure
//! is surfaced by `decdn_origin_directory_watcher_resolve_failures_total`
//! (and a `warn!`). On an **addition/replace** event (activation, default-open
//! add/replace) the cache fail-closes for the affected set (an un-added operator
//! authorizes nothing); the failed namespace is recorded and its `getOrigins`
//! re-read retried at the end of every poll tick until it succeeds (the tick
//! fails → backs off while any retry is outstanding), so the hole heals without
//! waiting for another same-namespace event — necessary because the persisted
//! cursor may already have advanced past the triggering log. On a **removal**
//! event (revoke / prune / default-open remove) a failed re-read falls back to a
//! precise delta removal from the event payload, so a revoke is never weaker
//! than a direct delete even when `getOrigins` is unavailable (and the
//! namespace is still queued for the retry re-read).
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
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain_events::resumable_watcher::{
    self, CursorPolicy, LogSink, NoneFallback, WatcherConfig, WatcherHook,
};
use crate::chain_events::shared_head::HeadSource;
use crate::chain_events::{
    AbortOnDrop, MAX_BACKFILL_BLOCK_SPAN, REORG_MARGIN_BLOCKS, WATCHER_INITIAL_BACKOFF,
    WATCHER_MAX_BACKOFF, backfill_windows, check_backfill_range, timed,
};
use crate::dht::origin::{Hash, OriginDirectory};
use crate::dht::routing::NodeId;
use crate::dht::staker_set::StakerSet;
use crate::metrics::Metrics;
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::origin_assignment::OriginAssignment;
use decdn_incentive::{CheckpointKey, KeyedCheckpointStore};
// Event structs imported directly so the topic0 dispatch stays under the 100-col
// width (the fully-qualified `OriginAssignment::<Event>` paths overflow it).
use decdn_incentive::origin_assignment::OriginAssignment::{
    AssignmentActivated, AssignmentRevoked, BlacklistedAssignmentPruned,
    DefaultOpenAllowlistUpdated, DefaultOpenOperatorAdded, DefaultOpenOperatorRemoved,
};
use decdn_incentive::publisher_registry::PublisherRegistry;
use decdn_incentive::publisher_registry::PublisherRegistry::ContentClaimed;

/// Block-window size for the genesis `ContentClaimed` log replay, passed as the
/// `span` to [`backfill_windows`]. Kept well under the common provider
/// `eth_getLogs` 10k-block cap so bootstrap works on range-limited RPCs without
/// a per-provider knob. This is a *deliberate* override of the shared
/// [`MAX_BACKFILL_BLOCK_SPAN`] (10k), not drift — the extra 1k of margin is the
/// point; don't "fix" the divergence by unifying the constant.
const REPLAY_WINDOW_BLOCKS: u64 = 9_000;

/// The default-open allow-list lives at namespace 0 (ADR 022 § FIND\_VALUE
/// Flow). A claimed hash never resolves here; only a hash with no claiming
/// namespace falls back to it.
const DEFAULT_OPEN_NAMESPACE: U256 = U256::ZERO;

/// In-memory projection of the on-chain origin directory. All resolution logic
/// lives here as pure methods over the maps so it is unit-testable without a
/// chain. Held behind an [`RwLock`] in [`ChainOriginDirectory`].
#[derive(Debug, Default)]
struct DirectoryCache {
    /// `hash → namespaceIds that claimed it` (from `ContentClaimed`). A hash
    /// absent here has no claiming namespace and falls back to default-open.
    namespaces_of: HashMap<Hash, HashSet<U256>>,
    /// `namespaceId → authorized operator addresses` for non-zero namespaces.
    /// Snapshotted via `getOrigins` and kept current by authoritative `getOrigins`
    /// re-reads on each assignment event — never incremental deltas (see the
    /// module header). The only per-operator delete is the fail-closed
    /// [`delta_remove_origin`] fallback when a removal-triggered re-read errors.
    origins_of_ns: HashMap<U256, HashSet<Address>>,
    /// The default-open allow-list (namespace 0).
    default_open: HashSet<Address>,
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

    /// Resolve `hash` to currently-active origin `NodeId`s: the operators
    /// authorised across the hash's claiming namespaces (or, for an unclaimed
    /// hash, the default-open allow-list per ADR 022 § FIND\_VALUE Flow), mapped
    /// to active bound `NodeId`s. Returned sorted + deduplicated — two
    /// namespaces may list the same operator, and a stable order keeps the set
    /// deterministic for callers and tests.
    fn resolve(&self, hash: &Hash, staker_set: &dyn StakerSet) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = match self.namespaces_of.get(hash) {
            Some(namespaces) if !namespaces.is_empty() => namespaces
                .iter()
                .filter_map(|ns| self.origins_of_ns.get(ns))
                .flatten()
                .filter_map(|op| self.active_node_for(op, staker_set))
                .collect(),
            // No claiming namespace → default-open fallback. (An empty
            // default-open set yields no origins: a truly-unclaimed hash.)
            _ => self
                .default_open
                .iter()
                .filter_map(|op| self.active_node_for(op, staker_set))
                .collect(),
        };
        nodes.sort_unstable();
        nodes.dedup();
        nodes
    }

    /// Whether at least one active authorised origin exists for `hash`. Iterates
    /// the underlying sets directly and short-circuits on the first hit — no
    /// allocation, since the prefetch authorized-origin gate only tests
    /// emptiness on the (uncommon) lookup-miss path.
    fn has_any(&self, hash: &Hash, staker_set: &dyn StakerSet) -> bool {
        match self.namespaces_of.get(hash) {
            Some(namespaces) if !namespaces.is_empty() => namespaces
                .iter()
                .filter_map(|ns| self.origins_of_ns.get(ns))
                .flatten()
                .any(|op| self.active_node_for(op, staker_set).is_some()),
            _ => self
                .default_open
                .iter()
                .any(|op| self.active_node_for(op, staker_set).is_some()),
        }
    }
}

/// Chain-backed origin directory. Cheap to clone via the shared inner [`Arc`];
/// the runtime holds one `Arc<dyn OriginDirectory>` and the prefetch engine
/// resolves through it. The background watcher task is owned via a private
/// `AbortOnDrop` wrapper so a node-restart cycle never leaks chain-poll tasks.
#[derive(Debug)]
pub struct ChainOriginDirectory {
    cache: Arc<RwLock<DirectoryCache>>,
    staker_set: Arc<dyn StakerSet>,
    _watcher: AbortOnDrop,
}

/// Contract handles the watcher needs, bundled so the bootstrap and watcher
/// share one construction site. Each is cheap to clone (wraps the provider).
struct Contracts<P: Provider + Clone> {
    origin: OriginAssignment::OriginAssignmentInstance<P>,
    publisher: PublisherRegistry::PublisherRegistryInstance<P>,
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
    /// the `ChainStakerSet` bootstrap (fatal — the prefetch authorized-origin
    /// gate cannot be trusted without a complete snapshot).
    ///
    /// `shutdown` must be a token the runtime cancels on graceful shutdown: the
    /// watcher persists its scan cursor through the (debounced)
    /// `checkpoint_store`, and only the cancel path flushes the buffered tail
    /// (`CheckpointKey::Origin`) to disk — an abort-only teardown would silently
    /// drop up to a debounce window of progress on every clean stop.
    #[allow(clippy::too_many_arguments)]
    pub async fn bootstrap<P>(
        provider: P,
        origin_assignment_addr: Address,
        publisher_registry_addr: Address,
        capacity_bond_addr: Address,
        from_block: u64,
        checkpoint_store: Arc<dyn KeyedCheckpointStore>,
        event_poll_interval: Duration,
        head: Arc<dyn HeadSource>,
        staker_set: Arc<dyn StakerSet>,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
    ) -> Result<Self>
    where
        P: Provider + Clone + 'static,
    {
        let contracts = Contracts {
            origin: OriginAssignment::new(origin_assignment_addr, provider.clone()),
            publisher: PublisherRegistry::new(publisher_registry_addr, provider.clone()),
            bond: CapacityBond::new(capacity_bond_addr, provider.clone()),
        };

        // Resume the `ContentClaimed` replay floor from the persisted cursor
        // (#1108) — a restart rebuilds `namespaces_of` from there rather than from
        // the deploy block. `None` (first-ever boot) falls back to `from_block`.
        // Claims below the cursor resolve as unclaimed until re-surfaced (the same
        // routing-only posture the resync clamp already documented); membership is
        // always authoritative via `getOrigins`, so no revoke is ever missed.
        let replay_from = checkpoint_store
            .load_checkpoint(CheckpointKey::Origin)
            .context("read origin-directory scan checkpoint at bootstrap")?
            .unwrap_or(from_block);
        let (cache, snapshot_block) = bootstrap_cache(&contracts, replay_from, &metrics)
            .await
            .context("snapshot OriginAssignment / PublisherRegistry at bootstrap")?;
        info!(
            namespaces = cache.origins_of_ns.len(),
            default_open = cache.default_open.len(),
            operators = cache.operator_node.len(),
            claimed_hashes = cache.namespaces_of.len(),
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
        let cfg = WatcherConfig {
            head,
            filter: Filter::new()
                .address(vec![publisher_registry_addr, origin_assignment_addr])
                .event_signature(vec![
                    ContentClaimed::SIGNATURE_HASH,
                    AssignmentActivated::SIGNATURE_HASH,
                    AssignmentRevoked::SIGNATURE_HASH,
                    BlacklistedAssignmentPruned::SIGNATURE_HASH,
                    DefaultOpenAllowlistUpdated::SIGNATURE_HASH,
                    DefaultOpenOperatorAdded::SIGNATURE_HASH,
                    DefaultOpenOperatorRemoved::SIGNATURE_HASH,
                ]),
            from_block,
            poll_interval: event_poll_interval,
            confirmations: 0,
            reorg_margin: REORG_MARGIN_BLOCKS,
            max_backfill_span: MAX_BACKFILL_BLOCK_SPAN,
            cursor: cursor_policy(checkpoint_store),
            initial_backoff: WATCHER_INITIAL_BACKOFF,
            max_backoff: WATCHER_MAX_BACKOFF,
            rpc_call_timeout: None,
            shutdown,
            seed_cursor: Some(snapshot_block),
            label: "origin-directory",
            on_established: Some(established_hook(&metrics)),
            on_backoff: Some(backoff_hook(&metrics)),
        };
        let watcher_handle = tokio::spawn(resumable_watcher::run(provider, cfg, sink));

        Ok(Self {
            cache,
            staker_set,
            _watcher: AbortOnDrop(watcher_handle),
        })
    }
}

/// Applies `PublisherRegistry` + `OriginAssignment` logs to the directory cache
/// (#1092). Two contract addresses share one filter; `apply` demuxes by
/// `(log.address, topic0)`. Each event is a *signal to re-read* the authoritative
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
    /// Namespaces whose authoritative `getOrigins` re-read failed
    /// ([`DEFAULT_OPEN_NAMESPACE`] = the default-open allow-list). Bounded by
    /// the number of distinct namespaces. Retried in [`Self::on_tick_complete`]
    /// until each re-read succeeds; until then the affected namespace stays
    /// fail-closed (unpopulated → authorizes nothing).
    deferred: HashSet<U256>,
}

impl<P: Provider + Clone> LogSink for OriginSink<P> {
    async fn apply(&mut self, log: Log) -> Result<()> {
        let publisher_addr = *self.contracts.publisher.address();
        if log.address() == publisher_addr {
            // PublisherRegistry's sole subscribed event is ContentClaimed.
            match ContentClaimed::decode_log_data(&log.inner.data) {
                Ok(event) => {
                    on_content_claimed(
                        &self.contracts,
                        &self.cache,
                        &self.metrics,
                        &mut self.deferred,
                        Hash::from_bytes(event.blake3Hash.0),
                        event.namespaceId,
                    )
                    .await;
                }
                Err(err) => warn!(%err, "skipping undecodable ContentClaimed log"),
            }
            return Ok(());
        }
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
            let resynced = if namespace == DEFAULT_OPEN_NAMESPACE {
                resync_default_open(&self.contracts, &self.cache, &self.metrics).await
            } else {
                resync_namespace(&self.contracts, &self.cache, &self.metrics, namespace).await
            };
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
                    Err(err) => warn!(%err, "skipping undecodable AssignmentActivated log"),
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
                    Err(err) => warn!(%err, "skipping undecodable AssignmentRevoked log"),
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
                    Err(err) => warn!(%err, "skipping undecodable BlacklistedAssignmentPruned log"),
                }
            }
            Some(sig) if sig == DefaultOpenAllowlistUpdated::SIGNATURE_HASH => {
                on_default_open_changed(
                    &self.contracts,
                    &self.cache,
                    &self.metrics,
                    &mut self.deferred,
                )
                .await;
            }
            Some(sig) if sig == DefaultOpenOperatorAdded::SIGNATURE_HASH => {
                on_default_open_changed(
                    &self.contracts,
                    &self.cache,
                    &self.metrics,
                    &mut self.deferred,
                )
                .await;
            }
            Some(sig) if sig == DefaultOpenOperatorRemoved::SIGNATURE_HASH => {
                match DefaultOpenOperatorRemoved::decode_log_data(&log.inner.data) {
                    Ok(event) => {
                        on_origin_removed(
                            &self.contracts,
                            &self.cache,
                            &self.metrics,
                            &mut self.deferred,
                            DEFAULT_OPEN_NAMESPACE,
                            event.operator,
                        )
                        .await;
                    }
                    Err(err) => warn!(%err, "skipping undecodable DefaultOpenOperatorRemoved log"),
                }
            }
            _ => {
                debug!(topic0 = ?log.topic0(), "unmatched OriginAssignment event in subscribed OR-set");
            }
        }
    }
}

/// The origin watcher's cursor policy: resume the durable
/// [`CheckpointKey::Origin`] scan cursor (#1108); a first-ever boot replays
/// `ContentClaimed` from the **deploy floor** — the `hash → namespaces` view
/// has no on-chain enumeration, so the stream must be replayed for
/// correctness. Pinned by a test: a `Head` fallback silently loses every claim
/// that predates the node. (In steady state `bootstrap` seeds the live cursor
/// at its snapshot block, so the fallback governs only a checkpoint-less start
/// of the poller itself.)
fn cursor_policy(store: Arc<dyn KeyedCheckpointStore>) -> CursorPolicy {
    CursorPolicy::Persisted {
        store,
        key: CheckpointKey::Origin,
        none_fallback: NoneFallback::FromBlock,
    }
}

/// Wire the watcher's healthy-cycle transition to the established gauge.
fn established_hook(metrics: &Arc<Metrics>) -> WatcherHook {
    let metrics = Arc::clone(metrics);
    Box::new(move || metrics.origin_directory_watcher_cycle_established())
}

/// Wire a tick failure to the backoff gauge.
fn backoff_hook(metrics: &Arc<Metrics>) -> WatcherHook {
    let metrics = Arc::clone(metrics);
    Box::new(move || metrics.origin_directory_watcher_backoff_started())
}

impl OriginDirectory for ChainOriginDirectory {
    fn lookup_origins(&self, hash: &Hash) -> Vec<NodeId> {
        read_cache(&self.cache, |c| c.resolve(hash, self.staker_set.as_ref()))
    }

    fn has_origin(&self, hash: &Hash) -> bool {
        read_cache(&self.cache, |c| c.has_any(hash, self.staker_set.as_ref()))
    }
}

/// Poison-tolerant `RwLock` read, mirroring `ChainStakerSet`: recover the inner
/// cache rather than propagate a panic into the lookup hot path. We never panic
/// while holding the write lock, so the inner state is structurally valid.
fn read_cache<R, F>(cache: &Arc<RwLock<DirectoryCache>>, f: F) -> R
where
    F: FnOnce(&DirectoryCache) -> R,
{
    match cache.read() {
        Ok(guard) => f(&guard),
        Err(poisoned) => {
            warn!("ChainOriginDirectory cache RwLock poisoned; recovering inner state");
            f(&poisoned.into_inner())
        }
    }
}

/// Snapshot current chain state: replay `ContentClaimed` to learn the
/// `hash → namespaces` map and the set of namespaces, then `getOrigins` each
/// namespace (plus default-open) and resolve every operator's `NodeId`.
async fn bootstrap_cache<P>(
    contracts: &Contracts<P>,
    replay_from_block: u64,
    metrics: &Metrics,
) -> Result<(DirectoryCache, u64)>
where
    P: Provider + Clone,
{
    let mut cache = DirectoryCache::default();

    // 1. hash → namespaces, via windowed ContentClaimed log replay starting at
    //    `replay_from_block`. Operators SHOULD set this to the PublisherRegistry
    //    deployment block (`blockchain.origin_directory_from_block`); the
    //    default `0` is correct but scans the whole chain history in
    //    `REPLAY_WINDOW_BLOCKS` windows, which is slow / RPC-heavy on an
    //    established L2.
    let latest = contracts
        .publisher
        .provider()
        .get_block_number()
        .await
        .context("get_block_number for ContentClaimed replay")?;
    // The replay floor resumes from a persisted checkpoint (#1108), so a stale /
    // lagging RPC head can legitimately sit *behind* it (`replay_from_block >
    // latest`) — replication lag or a reorg. `backfill_windows` would silently
    // yield no windows for that inverted range, skipping the replay with no
    // signal. Do not propagate the error: `bootstrap` is one-shot (no retry) and
    // a fatal startup crash is strictly worse than graceful degradation. Instead
    // warn + bump a counter and degrade to default-open routing. `namespaces_of`
    // stays empty, so the `getOrigins` loop below is a no-op and per-namespace
    // membership is absent this boot — every claimed hash falls back to the
    // default-open set (namespace 0, still authoritative via `getOrigins(0)`).
    // Per-namespace membership is restored once the live tail re-surfaces the
    // claims (#1152). Mirrors the buyer-reconcile consumer of the same range
    // check in `buyer_channel`.
    if let Err(err) = check_backfill_range(replay_from_block, latest) {
        warn!(
            %err, replay_from_block, latest,
            "origin-directory genesis replay: invalid range; skipping replay this boot \
             (default-open routing until claims re-surface via the live tail)"
        );
        metrics.origin_directory_bootstrap_range_anomaly();
    } else {
        for (from, to) in backfill_windows(replay_from_block, latest, REPLAY_WINDOW_BLOCKS) {
            let logs = contracts
                .publisher
                .ContentClaimed_filter()
                .from_block(from)
                .to_block(to)
                .query()
                .await
                .with_context(|| format!("query ContentClaimed logs [{from}, {to}]"))?;
            for (event, _log) in logs {
                cache
                    .namespaces_of
                    .entry(Hash::from_bytes(event.blake3Hash.0))
                    .or_default()
                    .insert(event.namespaceId);
            }
        }
    }

    // 2. namespace → operators, via getOrigins point reads (authoritative
    //    current set; avoids replaying assignment-mutation ordering).
    let namespaces: HashSet<U256> = cache.namespaces_of.values().flatten().copied().collect();
    for ns in namespaces {
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

    // 3. default-open allow-list (namespace 0).
    cache.default_open = contracts
        .origin
        .getOrigins(DEFAULT_OPEN_NAMESPACE)
        .call()
        .await
        .context("getOrigins(default-open namespace 0)")?
        .into_iter()
        .collect();

    // 4. operator → NodeId for every operator we learned about.
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
    let operators: HashSet<Address> = cache
        .origins_of_ns
        .values()
        .flatten()
        .chain(cache.default_open.iter())
        .copied()
        .collect();
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
    let operators = reads.get_origins(namespace).await?;
    resolve_and_store_operators(reads, cache, metrics, &operators).await;
    write_cache(cache, |c| {
        c.origins_of_ns
            .insert(namespace, operators.into_iter().collect());
    });
    publish_authorized_count(cache, metrics);
    Ok(())
}

/// Re-read the authoritative default-open allow-list (`getOrigins(0)`) and
/// replace the cached set wholesale. Same authoritative-read rationale as
/// [`resync_namespace`]; this subsumes the `updateIndex` version field the
/// `DefaultOpenAllowlistUpdated` event carries — re-reading the current set is
/// strictly stronger than ordering replaces by version.
async fn resync_default_open<R: OriginChainReads>(
    reads: &R,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    let operators = reads.get_origins(DEFAULT_OPEN_NAMESPACE).await?;
    resolve_and_store_operators(reads, cache, metrics, &operators).await;
    write_cache(cache, |c| {
        c.default_open = operators.into_iter().collect();
    });
    publish_authorized_count(cache, metrics);
    Ok(())
}

/// A new `(hash, namespace)` claim. Record the mapping and, for a not-yet-known
/// namespace, authoritatively read its current operator set so the hash resolves
/// immediately. On `getOrigins` failure the namespace is left unpopulated (the
/// hash resolves to empty, fail-closed), the failure is counted + warned, and
/// the namespace is recorded in `deferred` so the sink's `on_tick_complete`
/// retries the re-read every tick until it heals — the persisted cursor may
/// already have advanced past this event, so waiting for a later same-namespace
/// event would leave a permanent hole.
async fn on_content_claimed<R: OriginChainReads>(
    reads: &R,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    deferred: &mut HashSet<U256>,
    hash: Hash,
    namespace: U256,
) {
    let namespace_known = write_cache(cache, |c| {
        c.namespaces_of.entry(hash).or_default().insert(namespace);
        c.origins_of_ns.contains_key(&namespace)
    });
    if !namespace_known && let Err(err) = resync_namespace(reads, cache, metrics, namespace).await {
        metrics.origin_directory_watcher_resolve_failure();
        deferred.insert(namespace);
        warn!(err = %sanitize_err_chain(&err), %namespace, "getOrigins for newly-claimed namespace failed; deferred for retry");
    }
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

/// An addition/replace event for the default-open allow-list (replace / add).
/// Same defer-on-failure rationale as [`on_namespace_changed`].
async fn on_default_open_changed<R: OriginChainReads>(
    reads: &R,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    deferred: &mut HashSet<U256>,
) {
    if let Err(err) = resync_default_open(reads, cache, metrics).await {
        metrics.origin_directory_watcher_resolve_failure();
        deferred.insert(DEFAULT_OPEN_NAMESPACE);
        warn!(err = %sanitize_err_chain(&err), "getOrigins(0) re-read failed on default-open change; deferred for retry");
    }
}

/// A removal event — `AssignmentRevoked` / `BlacklistedAssignmentPruned` (which
/// may carry `namespace == 0` for a default-open operator, #851) or
/// `DefaultOpenOperatorRemoved` (dispatched here with `namespace == 0`). Re-read
/// the authoritative set; on RPC failure, fall back to the precise delta removal
/// from the event payload so a revoke is **never weaker** than a direct delete
/// even when `getOrigins` is unavailable — and still record the namespace for
/// the `on_tick_complete` retry, since the delta fallback leaves the rest of the
/// cached set potentially stale.
async fn on_origin_removed<R: OriginChainReads>(
    reads: &R,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    deferred: &mut HashSet<U256>,
    namespace: U256,
    operator: Address,
) {
    let resynced = if namespace == DEFAULT_OPEN_NAMESPACE {
        resync_default_open(reads, cache, metrics).await
    } else {
        resync_namespace(reads, cache, metrics, namespace).await
    };
    if let Err(err) = resynced {
        metrics.origin_directory_watcher_resolve_failure();
        deferred.insert(namespace);
        warn!(err = %sanitize_err_chain(&err), %namespace, %operator, "getOrigins re-read failed on removal; applying precise delta fallback");
        delta_remove_origin(cache, metrics, namespace, operator);
    }
}

/// Precise single-operator removal, used as the fail-closed fallback when the
/// authoritative `getOrigins` re-read fails on a removal event. Routes
/// `namespace == 0` to the default-open set (#851); a `None` for a non-zero
/// namespace means it was never cached and already resolves to empty, so the
/// no-op is correct.
fn delta_remove_origin(
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    namespace: U256,
    operator: Address,
) {
    write_cache(cache, |c| {
        if namespace == DEFAULT_OPEN_NAMESPACE {
            c.default_open.remove(&operator);
        } else if let Some(set) = c.origins_of_ns.get_mut(&namespace) {
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
/// every namespace's operator set and the default-open allow-list. Unlike the
/// monotonic `operator_node` binding cache, this rises on activate/add and
/// falls on revoke/prune/remove/replace, so it tracks the directory's live
/// authorised-origin surface.
fn authorized_operator_count(c: &DirectoryCache) -> usize {
    c.origins_of_ns
        .values()
        .flatten()
        .chain(c.default_open.iter())
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
    match cache.write() {
        Ok(mut guard) => f(&mut guard),
        Err(poisoned) => {
            warn!("ChainOriginDirectory cache RwLock poisoned; recovering inner state");
            f(&mut poisoned.into_inner())
        }
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
            publisher: PublisherRegistry::PublisherRegistryInstance::new(
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
            publisher: PublisherRegistry::PublisherRegistryInstance::new(
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
        cache.default_open.insert(addr(1));

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

    fn h(b: u8) -> Hash {
        Hash::from_bytes([b; 32])
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

    /// Seed a cache: claimed-hash→namespaces, namespace→operators,
    /// default-open, and operator→NodeId bindings.
    fn cache_with(
        claims: &[(Hash, &[u64])],
        ns_origins: &[(u64, &[Address])],
        default_open: &[Address],
        bindings: &[(Address, NodeId)],
    ) -> DirectoryCache {
        let mut c = DirectoryCache::default();
        for (hash, namespaces) in claims {
            c.namespaces_of
                .insert(*hash, namespaces.iter().map(|n| ns(*n)).collect());
        }
        for (n, ops) in ns_origins {
            c.origins_of_ns
                .insert(ns(*n), ops.iter().copied().collect());
        }
        c.default_open = default_open.iter().copied().collect();
        c.operator_node = bindings.iter().copied().collect();
        c
    }

    /// Scripted [`OriginChainReads`]: per-namespace authoritative operator sets,
    /// operator→NodeId bindings, a `(block, hash, namespace)` `ContentClaimed` log,
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
    fn claimed_hash_resolves_to_its_namespace_operators() {
        let c = cache_with(
            &[(h(1), &[7])],
            &[(7, &[addr(0xA), addr(0xB)])],
            &[],
            &[(addr(0xA), nid(0xA)), (addr(0xB), nid(0xB))],
        );
        let stakers = StubStakers::new(&[nid(0xA), nid(0xB)]);
        // `resolve` returns sorted + deduplicated, so assert order directly.
        assert_eq!(c.resolve(&h(1), &stakers), vec![nid(0xA), nid(0xB)]);
        assert!(c.has_any(&h(1), &stakers));
    }

    #[test]
    fn multi_namespace_hash_unions_operators_deduped() {
        // Hash claimed by namespaces 7 and 8; operator A appears in both.
        let c = cache_with(
            &[(h(1), &[7, 8])],
            &[(7, &[addr(0xA)]), (8, &[addr(0xA), addr(0xC)])],
            &[],
            &[(addr(0xA), nid(0xA)), (addr(0xC), nid(0xC))],
        );
        let stakers = StubStakers::new(&[nid(0xA), nid(0xC)]);
        assert_eq!(
            c.resolve(&h(1), &stakers),
            vec![nid(0xA), nid(0xC)],
            "A appears once despite two namespaces; output is sorted + deduped"
        );
    }

    #[test]
    fn unclaimed_hash_falls_back_to_default_open() {
        let c = cache_with(&[], &[], &[addr(0xD)], &[(addr(0xD), nid(0xD))]);
        let stakers = StubStakers::new(&[nid(0xD)]);
        assert_eq!(c.resolve(&h(9), &stakers), vec![nid(0xD)]);
        assert!(c.has_any(&h(9), &stakers));
    }

    #[test]
    fn claimed_hash_does_not_use_default_open() {
        // A claimed hash whose namespace has no active assignment resolves to
        // EMPTY — it must NOT fall through to the default-open list (ADR 022:
        // default-open is only for unclaimed content).
        let c = cache_with(
            &[(h(1), &[7])],
            &[(7, &[])],
            &[addr(0xD)],
            &[(addr(0xD), nid(0xD))],
        );
        let stakers = StubStakers::new(&[nid(0xD)]);
        assert!(c.resolve(&h(1), &stakers).is_empty());
        assert!(!c.has_any(&h(1), &stakers));
    }

    #[test]
    fn empty_default_open_yields_no_origins_for_unclaimed() {
        let c = cache_with(&[], &[], &[], &[]);
        let stakers = StubStakers::new(&[nid(0xD)]);
        assert!(c.resolve(&h(9), &stakers).is_empty());
        assert!(!c.has_any(&h(9), &stakers));
    }

    #[test]
    fn inactive_operators_are_filtered_out() {
        let c = cache_with(
            &[(h(1), &[7])],
            &[(7, &[addr(0xA), addr(0xB)])],
            &[],
            &[(addr(0xA), nid(0xA)), (addr(0xB), nid(0xB))],
        );
        // Only A is active; B is bonded-but-inactive (e.g. unbonding).
        let stakers = StubStakers::new(&[nid(0xA)]);
        assert_eq!(c.resolve(&h(1), &stakers), vec![nid(0xA)]);
        assert!(c.has_any(&h(1), &stakers));
    }

    #[test]
    fn operator_without_binding_is_dropped() {
        // Operator B is authorised but has no NodeId binding cached → unmapped.
        let c = cache_with(
            &[(h(1), &[7])],
            &[(7, &[addr(0xA), addr(0xB)])],
            &[],
            &[(addr(0xA), nid(0xA))],
        );
        let stakers = StubStakers::new(&[nid(0xA), nid(0xB)]);
        assert_eq!(c.resolve(&h(1), &stakers), vec![nid(0xA)]);
    }

    #[test]
    fn has_any_agrees_with_resolve_emptiness() {
        let c = cache_with(
            &[(h(1), &[7]), (h(2), &[8])],
            &[(7, &[addr(0xA)]), (8, &[])],
            &[],
            &[(addr(0xA), nid(0xA))],
        );
        let stakers = StubStakers::new(&[nid(0xA)]);
        for hash in [h(1), h(2), h(3)] {
            assert_eq!(
                c.has_any(&hash, &stakers),
                !c.resolve(&hash, &stakers).is_empty(),
                "has_any must agree with resolve emptiness for {hash:?}"
            );
        }
    }

    // ---- Cache-mutation (event-application) semantics, exercised directly on
    //      the pure cache the way the watcher's apply helpers mutate it. ----

    #[test]
    fn assignment_activated_replaces_namespace_set() {
        let mut c = cache_with(&[], &[(7, &[addr(0xA)])], &[], &[]);
        // Wholesale replace 7's set with {B, C}.
        c.origins_of_ns
            .insert(ns(7), [addr(0xB), addr(0xC)].into_iter().collect());
        let set = &c.origins_of_ns[&ns(7)];
        assert!(!set.contains(&addr(0xA)));
        assert!(set.contains(&addr(0xB)) && set.contains(&addr(0xC)));
    }

    #[test]
    fn revoke_removes_single_operator() {
        let mut c = cache_with(&[], &[(7, &[addr(0xA), addr(0xB)])], &[], &[]);
        c.origins_of_ns.get_mut(&ns(7)).unwrap().remove(&addr(0xA));
        let set = &c.origins_of_ns[&ns(7)];
        assert!(!set.contains(&addr(0xA)));
        assert!(set.contains(&addr(0xB)));
    }

    #[test]
    fn default_open_add_and_remove() {
        let mut c = cache_with(&[], &[], &[addr(0xD)], &[]);
        c.default_open.insert(addr(0xE));
        assert!(c.default_open.contains(&addr(0xE)));
        c.default_open.remove(&addr(0xD));
        assert!(!c.default_open.contains(&addr(0xD)));
    }

    #[test]
    fn revoke_namespace_zero_removes_from_default_open() {
        // `AssignmentRevoked(0, op)` / `BlacklistedAssignmentPruned(0, op)` route
        // through the `delta_remove_origin` fallback with `namespace ==
        // DEFAULT_OPEN_NAMESPACE`. The default-open set lives in its own field,
        // never in `origins_of_ns`, so the removal must target it (#851).
        let metrics = Arc::new(Metrics::new());
        let cache = Arc::new(RwLock::new(cache_with(
            &[],
            &[],
            &[addr(0xA), addr(0xB)],
            &[],
        )));
        delta_remove_origin(&cache, &metrics, DEFAULT_OPEN_NAMESPACE, addr(0xA));
        read_cache(&cache, |c| {
            assert!(
                !c.default_open.contains(&addr(0xA)),
                "revoked operator must be dropped from the default-open set"
            );
            assert!(
                c.default_open.contains(&addr(0xB)),
                "other default-open operators are untouched"
            );
        });
        // The removal must re-publish the authorised-operator gauge (the
        // observability half of the fix: pre-#851 the no-op left it stale).
        let text = metrics.encode().unwrap();
        assert!(
            text.lines()
                .any(|l| l == "decdn_origin_directory_operator_count 1"),
            "gauge must drop to 1 (only B remains) after a namespace-0 revoke:\n{text}"
        );
    }

    #[test]
    fn blacklist_prune_namespace_zero_leaves_nonzero_namespaces_intact() {
        // A namespace-0 prune touches only the default-open set; an operator
        // authorised in a specific (non-zero) namespace stays there.
        let metrics = Arc::new(Metrics::new());
        let cache = Arc::new(RwLock::new(cache_with(
            &[],
            &[(7, &[addr(0xA)])],
            &[addr(0xA), addr(0xB)],
            &[],
        )));
        delta_remove_origin(&cache, &metrics, DEFAULT_OPEN_NAMESPACE, addr(0xA));
        read_cache(&cache, |c| {
            assert!(
                !c.default_open.contains(&addr(0xA)),
                "pruned operator must leave the default-open set"
            );
            assert!(
                c.origins_of_ns[&ns(7)].contains(&addr(0xA)),
                "namespace-0 prune must not touch a non-zero namespace's set"
            );
        });
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
        let mut c = cache_with(&[], &[(7, &[addr(0xA), addr(0xB)])], &[addr(0xA)], &[]);
        // {A, B} (A shared with default-open) → 2.
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
        // Distinct addresses across namespaces + default-open; A is shared
        // between ns 7 and default-open, so it counts once: {A, B, C} = 3.
        let mut c = cache_with(
            &[],
            &[(7, &[addr(0xA), addr(0xB)]), (8, &[addr(0xC)])],
            &[addr(0xA)],
            &[],
        );
        assert_eq!(authorized_operator_count(&c), 3);
        // A revoke must DECREASE the count (the bug this fix addresses: the old
        // gauge counted the monotonic binding cache and never dropped).
        c.origins_of_ns.get_mut(&ns(7)).unwrap().remove(&addr(0xB));
        assert_eq!(authorized_operator_count(&c), 2, "B removed → {{A, C}}");
        // Removing A from ns 7 still leaves A in default-open → count unchanged.
        c.origins_of_ns.get_mut(&ns(7)).unwrap().remove(&addr(0xA));
        assert_eq!(authorized_operator_count(&c), 2, "A still in default-open");
        c.default_open.remove(&addr(0xA));
        assert_eq!(authorized_operator_count(&c), 1, "A fully removed → {{C}}");
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
            &[(h(1), &[7])],
            &[(7, &[addr(0xA), addr(0xC)])],
            &[],
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
            &[(h(1), &[7])],
            &[(7, &[addr(0xA), addr(0xC)])],
            &[],
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
            &[(h(1), &[7])],
            &[(7, &[addr(0xA), addr(0xC)])],
            &[],
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
        let cache = shared(cache_with(&[(h(1), &[7])], &[(7, &[addr(0xA)])], &[], &[]));
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
    async fn namespace_zero_removal_routes_to_default_open_via_resync() {
        // `Revoked(0, C)` / `DefaultOpenOperatorRemoved(C)` re-read getOrigins(0)
        // and replace the default-open set (preserves #851 routing).
        let metrics = Arc::new(Metrics::new());
        let cache = shared(cache_with(&[], &[], &[addr(0xA), addr(0xC)], &[]));
        let reads = StubReads::new()
            .origins(&[(0, &[addr(0xA)])])
            .bindings(&[(addr(0xA), nid(0xA))]);
        let mut deferred = HashSet::new();
        on_origin_removed(
            &reads,
            &cache,
            &metrics,
            &mut deferred,
            DEFAULT_OPEN_NAMESPACE,
            addr(0xC),
        )
        .await;
        read_cache(&cache, |c| {
            assert!(c.default_open.contains(&addr(0xA)));
            assert!(
                !c.default_open.contains(&addr(0xC)),
                "ns-0 removal must drop C from default-open"
            );
        });
    }

    #[tokio::test]
    async fn content_claimed_resolves_new_namespace_authoritatively() {
        let metrics = Arc::new(Metrics::new());
        let cache = shared(cache_with(&[], &[], &[], &[]));
        let reads = StubReads::new()
            .origins(&[(7, &[addr(0xA)])])
            .bindings(&[(addr(0xA), nid(0xA))]);
        let mut deferred = HashSet::new();
        on_content_claimed(&reads, &cache, &metrics, &mut deferred, h(1), ns(7)).await;
        let stakers = StubStakers::new(&[nid(0xA)]);
        read_cache(&cache, |c| {
            assert_eq!(c.resolve(&h(1), &stakers), vec![nid(0xA)]);
        });
    }

    /// POLICY PIN: the origin watcher persists `CheckpointKey::Origin` and a
    /// first-ever boot replays from the deploy floor — `hash → namespaces` has
    /// no enumeration view, so a `Head` fallback silently loses every claim
    /// that predates the node.
    #[test]
    fn cursor_policy_is_persisted_origin_from_block_fallback() {
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
            cursor_policy(store),
            CursorPolicy::Persisted {
                key: CheckpointKey::Origin,
                none_fallback: NoneFallback::FromBlock,
                ..
            }
        ));
    }

    #[test]
    fn delta_remove_origin_unknown_namespace_is_a_noop() {
        // The fail-closed fallback for a namespace never cached must not panic or
        // create an entry — the operator already resolves to empty there.
        let metrics = Arc::new(Metrics::new());
        let cache = shared(cache_with(&[], &[(7, &[addr(0xA)])], &[], &[]));
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
