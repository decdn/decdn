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
//! A narrower silent-drift source: a per-event `getOrigins` or `nodeIdOf` RPC
//! failure is surfaced by `decdn_origin_directory_watcher_resolve_failures_total`
//! (and a `warn!`). On an **addition/replace** event (activation, default-open
//! add/replace) the cache fail-closes for the affected set (an un-added operator
//! authorizes nothing) and self-heals on the next event (or the poller's window
//! re-scan). On a **removal** event (revoke / prune / default-open remove) a
//! failed re-read falls back to a precise delta removal from the event payload,
//! so a revoke is never weaker than a direct delete even when `getOrigins` is
//! unavailable. `decdn_origin_directory_operator_count` tracks the live
//! authorised-origin surface to spot a frozen or collapsed cache.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain_events::resumable_watcher::{
    self, CursorPolicy, LogSink, NoneFallback, WatcherConfig, WatcherHook,
};
use crate::dht::origin::{Hash, OriginDirectory};
use crate::dht::routing::NodeId;
use crate::dht::staker_set::StakerSet;
use crate::metrics::Metrics;
use crate::payment_settlement::{MAX_BACKFILL_BLOCK_SPAN, REORG_MARGIN_BLOCKS};
use decdn_common::redact::sanitize_rpc_display;
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

/// Block-window size for the genesis `ContentClaimed` log replay. Kept well
/// under the common provider `eth_getLogs` 10k-block cap so bootstrap works on
/// range-limited RPCs without a per-provider knob.
const REPLAY_WINDOW_BLOCKS: u64 = 9_000;

/// The default-open allow-list lives at namespace 0 (ADR 022 § FIND\_VALUE
/// Flow). A claimed hash never resolves here; only a hash with no claiming
/// namespace falls back to it.
const DEFAULT_OPEN_NAMESPACE: U256 = U256::ZERO;

/// Backoff between watcher restart attempts after an event-stream terminates
/// with an error. Mirrors [`crate::dht::chain_staker_set`].
const WATCHER_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Upper bound for the watcher restart backoff.
const WATCHER_MAX_BACKOFF: Duration = Duration::from_mins(1);

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
        self.origin
            .getOrigins(namespace)
            .call()
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
    /// watcher is running — the watcher's own subscription failures do not fail
    /// bootstrap.
    ///
    /// A bootstrap RPC failure is propagated; the runtime treats it the same as
    /// the `ChainStakerSet` bootstrap (fatal — the prefetch authorized-origin
    /// gate cannot be trusted without a complete snapshot).
    #[allow(clippy::too_many_arguments)]
    pub async fn bootstrap<P>(
        provider: P,
        origin_assignment_addr: Address,
        publisher_registry_addr: Address,
        capacity_bond_addr: Address,
        from_block: u64,
        checkpoint_store: Arc<dyn KeyedCheckpointStore>,
        event_poll_interval: Duration,
        staker_set: Arc<dyn StakerSet>,
        metrics: Arc<Metrics>,
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
        let (cache, snapshot_block) = bootstrap_cache(&contracts, replay_from)
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
        };
        let cfg = WatcherConfig {
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
            cursor: CursorPolicy::Persisted {
                store: checkpoint_store,
                key: CheckpointKey::Origin,
                none_fallback: NoneFallback::FromBlock,
            },
            initial_backoff: WATCHER_INITIAL_BACKOFF,
            max_backoff: WATCHER_MAX_BACKOFF,
            rpc_call_timeout: None,
            shutdown: CancellationToken::new(),
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
/// `Err`: a `getOrigins` re-read failure is deferred (fail-closed, healed by a
/// later event on the same namespace), and an undecodable log is skipped.
struct OriginSink<P: Provider + Clone> {
    contracts: Contracts<P>,
    cache: Arc<RwLock<DirectoryCache>>,
    metrics: Arc<Metrics>,
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
}

impl<P: Provider + Clone> OriginSink<P> {
    /// Dispatch one `OriginAssignment` log by `topic0` (all decoded from the
    /// origin-assignment contract). Each arm re-reads the authoritative set.
    #[allow(clippy::cognitive_complexity)]
    async fn apply_origin_event(&self, log: &Log) {
        match log.topic0().copied() {
            Some(sig) if sig == AssignmentActivated::SIGNATURE_HASH => {
                match AssignmentActivated::decode_log_data(&log.inner.data) {
                    Ok(event) => {
                        on_namespace_changed(
                            &self.contracts,
                            &self.cache,
                            &self.metrics,
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
                            event.namespaceId,
                            event.operator,
                        )
                        .await;
                    }
                    Err(err) => warn!(%err, "skipping undecodable BlacklistedAssignmentPruned log"),
                }
            }
            Some(sig) if sig == DefaultOpenAllowlistUpdated::SIGNATURE_HASH => {
                on_default_open_changed(&self.contracts, &self.cache, &self.metrics).await;
            }
            Some(sig) if sig == DefaultOpenOperatorAdded::SIGNATURE_HASH => {
                on_default_open_changed(&self.contracts, &self.cache, &self.metrics).await;
            }
            Some(sig) if sig == DefaultOpenOperatorRemoved::SIGNATURE_HASH => {
                match DefaultOpenOperatorRemoved::decode_log_data(&log.inner.data) {
                    Ok(event) => {
                        on_origin_removed(
                            &self.contracts,
                            &self.cache,
                            &self.metrics,
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

/// Watcher join handle that aborts the task on drop. Mirrors
/// `ChainStakerSet::AbortOnDrop` — abort is sufficient cleanup; we do not await
/// completion (the watcher is self-contained chain polling).
#[derive(Debug)]
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Snapshot current chain state: replay `ContentClaimed` to learn the
/// `hash → namespaces` map and the set of namespaces, then `getOrigins` each
/// namespace (plus default-open) and resolve every operator's `NodeId`.
async fn bootstrap_cache<P>(
    contracts: &Contracts<P>,
    replay_from_block: u64,
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
    let mut from = replay_from_block;
    while from <= latest {
        let to = from.saturating_add(REPLAY_WINDOW_BLOCKS - 1).min(latest);
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
        from = to.saturating_add(1);
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
    let operators: HashSet<Address> = cache
        .origins_of_ns
        .values()
        .flatten()
        .chain(cache.default_open.iter())
        .copied()
        .collect();
    for op in operators {
        if let Some(node_id) = resolve_node_id(&contracts.bond, op).await? {
            cache.operator_node.insert(op, node_id);
        }
    }

    Ok((cache, latest))
}

/// Resolve an operator address to its bound `NodeId` via `nodeIdOf`. Returns
/// `Ok(None)` when the operator has no binding (`bytes32(0)`) — it cannot be a
/// probeable origin. RPC errors propagate (caller decides fatal vs. drop).
async fn resolve_node_id<P>(
    bond: &CapacityBond::CapacityBondInstance<P>,
    operator: Address,
) -> Result<Option<NodeId>>
where
    P: Provider + Clone,
{
    let resolved = bond
        .nodeIdOf(operator)
        .call()
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
/// hash resolves to empty, fail-closed) and the failure is counted + warned. This
/// heals **event-drivenly**: a subsequent claim or assignment-mutation on the
/// same namespace re-reads it. (The old periodic re-arm resync pass that re-read
/// every namespace was removed with the poller migration — the poller's window
/// re-scan recovers lost *events*, but not a deferred `getOrigins` on an event
/// that was already scanned, so an unrelated event for a *different* namespace
/// does not heal this one.)
async fn on_content_claimed<R: OriginChainReads>(
    reads: &R,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    hash: Hash,
    namespace: U256,
) {
    let namespace_known = write_cache(cache, |c| {
        c.namespaces_of.entry(hash).or_default().insert(namespace);
        c.origins_of_ns.contains_key(&namespace)
    });
    if !namespace_known && let Err(err) = resync_namespace(reads, cache, metrics, namespace).await {
        metrics.origin_directory_watcher_resolve_failure();
        warn!(err = %sanitize_rpc_display(&err), %namespace, "getOrigins for newly-claimed namespace failed; origins deferred");
    }
}

/// An addition/replace event for `namespace` (activation). Re-read the
/// authoritative set; on RPC failure, defer — an un-added operator authorizes
/// nothing, so failing closed is safe and self-heals on the next event/resync.
async fn on_namespace_changed<R: OriginChainReads>(
    reads: &R,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    namespace: U256,
) {
    if let Err(err) = resync_namespace(reads, cache, metrics, namespace).await {
        metrics.origin_directory_watcher_resolve_failure();
        warn!(err = %sanitize_rpc_display(&err), %namespace, "getOrigins re-read failed on activation; namespace origins deferred");
    }
}

/// An addition/replace event for the default-open allow-list (replace / add).
/// Same defer-on-failure rationale as [`on_namespace_changed`].
async fn on_default_open_changed<R: OriginChainReads>(
    reads: &R,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
) {
    if let Err(err) = resync_default_open(reads, cache, metrics).await {
        metrics.origin_directory_watcher_resolve_failure();
        warn!(err = %sanitize_rpc_display(&err), "getOrigins(0) re-read failed on default-open change; deferred");
    }
}

/// A removal event — `AssignmentRevoked` / `BlacklistedAssignmentPruned` (which
/// may carry `namespace == 0` for a default-open operator, #851) or
/// `DefaultOpenOperatorRemoved` (dispatched here with `namespace == 0`). Re-read
/// the authoritative set; on RPC failure, fall back to the precise delta removal
/// from the event payload so a revoke is **never weaker** than a direct delete
/// even when `getOrigins` is unavailable.
async fn on_origin_removed<R: OriginChainReads>(
    reads: &R,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
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
        warn!(err = %sanitize_rpc_display(&err), %namespace, %operator, "getOrigins re-read failed on removal; applying precise delta fallback");
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
                warn!(err = %sanitize_rpc_display(&err), %op, "nodeIdOf failed; operator unmapped until a later event");
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
    use tokio::sync::broadcast;

    use crate::dht::staker_set::StakerChange;

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
        fn subscribe_changes(&self) -> broadcast::Receiver<StakerChange> {
            let (tx, rx) = broadcast::channel(1);
            drop(tx);
            rx
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
        on_origin_removed(&reads, &cache, &metrics, ns(7), addr(0xC)).await;
        on_namespace_changed(&reads, &cache, &metrics, ns(7)).await;
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
        on_namespace_changed(&reads, &cache, &metrics, ns(7)).await;
        on_origin_removed(&reads, &cache, &metrics, ns(7), addr(0xC)).await;
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
        on_origin_removed(&reads, &cache, &metrics, ns(7), addr(0xC)).await;
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
        on_namespace_changed(&reads, &cache, &metrics, ns(7)).await;
        read_cache(&cache, |c| {
            assert_eq!(
                c.origins_of_ns[&ns(7)].len(),
                1,
                "set unchanged on failed add"
            );
        });
        // The injected failure clears → a later activation heals to {A, B}.
        reads.fail_origins.write().unwrap().clear();
        on_namespace_changed(&reads, &cache, &metrics, ns(7)).await;
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
        on_origin_removed(&reads, &cache, &metrics, DEFAULT_OPEN_NAMESPACE, addr(0xC)).await;
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
        on_content_claimed(&reads, &cache, &metrics, h(1), ns(7)).await;
        let stakers = StubStakers::new(&[nid(0xA)]);
        read_cache(&cache, |c| {
            assert_eq!(c.resolve(&h(1), &stakers), vec![nid(0xA)]);
        });
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
