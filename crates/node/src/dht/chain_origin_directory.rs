//! Chain-backed [`OriginDirectory`] implementation.
//!
//! Resolves a content hash to the set of currently-active operator `NodeId`s
//! authorised as origins for it, reading from an in-memory cache kept current
//! by a background event-subscription task. The resolution chain (ADR 022
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
//! Watcher stream failure (mid-run) → `warn!` + exponential backoff (1s → 60s),
//! re-establishing filters. This opens a drift window surfaced by
//! `decdn_origin_directory_watcher_restarts_total` (edge-triggered, one per
//! window) and `..._down_seconds` (true downtime). On every re-subscription the
//! watcher runs a **re-arm resync pass** before going live (#855): it replays
//! `ContentClaimed` from the last observed block (catching claims emitted during
//! the outage) and re-reads `getOrigins` for every known namespace and the
//! default-open set, so a `*Revoked` / `*Pruned` / `*Removed` lost during the
//! backoff window is recovered instead of lingering for the process lifetime. The
//! pass is cleared only on full success, so a transient RPC error repeats it.
//!
//! A narrower silent-drift source: a per-event `getOrigins` or `nodeIdOf` RPC
//! failure is surfaced by `decdn_origin_directory_watcher_resolve_failures_total`
//! (and a `warn!`). On an **addition/replace** event (activation, default-open
//! add/replace) the cache fail-closes for the affected set (an un-added operator
//! authorizes nothing) and self-heals on the next event or re-arm resync. On a
//! **removal** event (revoke / prune / default-open remove) a failed re-read
//! falls back to a precise delta removal from the event payload, so a revoke is
//! never weaker than a direct delete even when `getOrigins` is unavailable.
//! `decdn_origin_directory_operator_count` tracks the live authorised-origin
//! surface to spot a frozen or collapsed cache.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::dht::origin::{Hash, OriginDirectory};
use crate::dht::routing::NodeId;
use crate::dht::staker_set::StakerSet;
use crate::metrics::Metrics;
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::origin_assignment::OriginAssignment;
use decdn_incentive::publisher_registry::PublisherRegistry;

/// Block-window size for the genesis `ContentClaimed` log replay. Kept well
/// under the common provider `eth_getLogs` 10k-block cap so bootstrap works on
/// range-limited RPCs without a per-provider knob.
const REPLAY_WINDOW_BLOCKS: u64 = 9_000;

/// Lookback applied to the re-arm resync's `ContentClaimed` replay floor, so a
/// shallow reorg around the gap boundary cannot drop a claim emitted right at
/// `last_block`. Re-replaying a handful of already-seen logs is idempotent (the
/// `namespaces_of` insert is a set op). Arbitrum Sepolia reorgs are shallow, so
/// a small fixed overlap suffices.
const REORG_OVERLAP: u64 = 12;

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
    /// `namespaceId → authorized operator addresses` for non-zero namespaces
    /// (from `getOrigins` snapshot + `AssignmentActivated` / `*Revoked` /
    /// `*Pruned` deltas).
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
/// handlers and the re-arm resync pass are unit-testable without a provider.
/// The production implementation is [`Contracts`]; tests supply a scripted stub.
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
    /// `(hash, namespace)` claims from `ContentClaimed` over `[from, to]`.
    async fn content_claimed(&self, from: u64, to: u64) -> Result<Vec<(Hash, U256)>>;
    /// Current chain head block number.
    async fn head_block(&self) -> Result<u64>;
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

    async fn content_claimed(&self, from: u64, to: u64) -> Result<Vec<(Hash, U256)>> {
        let logs = self
            .publisher
            .ContentClaimed_filter()
            .from_block(from)
            .to_block(to)
            .query()
            .await
            .with_context(|| format!("query ContentClaimed logs [{from}, {to}]"))?;
        Ok(logs
            .into_iter()
            .map(|(event, _log)| (Hash::from_bytes(event.blake3Hash.0), event.namespaceId))
            .collect())
    }

    async fn head_block(&self) -> Result<u64> {
        self.publisher
            .provider()
            .get_block_number()
            .await
            .context("get_block_number")
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
    pub async fn bootstrap<P>(
        provider: P,
        origin_assignment_addr: Address,
        publisher_registry_addr: Address,
        capacity_bond_addr: Address,
        replay_from_block: u64,
        staker_set: Arc<dyn StakerSet>,
        metrics: Arc<Metrics>,
    ) -> Result<Self>
    where
        P: Provider + Clone + 'static,
    {
        let contracts = Contracts {
            origin: OriginAssignment::new(origin_assignment_addr, provider.clone()),
            publisher: PublisherRegistry::new(publisher_registry_addr, provider.clone()),
            bond: CapacityBond::new(capacity_bond_addr, provider),
        };

        let (cache, snapshot_block) = bootstrap_cache(&contracts, replay_from_block)
            .await
            .context("snapshot OriginAssignment / PublisherRegistry at bootstrap")?;
        info!(
            namespaces = cache.origins_of_ns.len(),
            default_open = cache.default_open.len(),
            operators = cache.operator_node.len(),
            claimed_hashes = cache.namespaces_of.len(),
            snapshot_block,
            "ChainOriginDirectory bootstrap snapshot complete"
        );
        metrics.origin_directory_operator_count(authorized_operator_count(&cache));

        let cache = Arc::new(RwLock::new(cache));
        let watcher_handle = tokio::spawn(watcher_loop(
            contracts,
            Arc::clone(&cache),
            Arc::clone(&metrics),
            snapshot_block,
        ));

        Ok(Self {
            cache,
            staker_set,
            _watcher: AbortOnDrop(watcher_handle),
        })
    }
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

/// Cursor + resync flag threaded across [`run_watcher_once`] cycles, mirroring
/// the `reputation_indexer` backfill discipline.
struct WatcherState {
    /// Highest block observed from any event log (seeded with the bootstrap
    /// snapshot block). The re-arm resync replays `ContentClaimed` from here, so
    /// a claim emitted during an outage window is recovered.
    last_block: u64,
    /// `Some(start)` when the next cycle must run the re-arm resync pass before
    /// going live. Set on every re-subscription (clean or error); cleared only
    /// after a fully-successful pass so a transient RPC error repeats it.
    resync_from: Option<u64>,
}

/// Advance the observed-block cursor from an event log's block number (best
/// effort — a pending log with no block number leaves the cursor unchanged).
fn advance_block(state: &mut WatcherState, block_number: Option<u64>) {
    if let Some(bn) = block_number {
        state.last_block = state.last_block.max(bn);
    }
}

/// Background event-subscription loop. Follows the `OriginAssignment` and
/// `PublisherRegistry` events that mutate the directory. Each event is a *signal
/// to re-read* the authoritative chain set, not a delta to apply — so neither
/// cross-filter reorder nor a single lost event can corrupt the cache. On stream
/// failure, restarts with exponential backoff and re-arms the resync pass so the
/// outage gap is recovered. Health-metric semantics match
/// [`crate::dht::chain_staker_set`].
async fn watcher_loop<P>(
    contracts: Contracts<P>,
    cache: Arc<RwLock<DirectoryCache>>,
    metrics: Arc<Metrics>,
    snapshot_block: u64,
) where
    P: Provider + Clone,
{
    let mut state = WatcherState {
        last_block: snapshot_block,
        resync_from: None,
    };
    let mut backoff = WATCHER_INITIAL_BACKOFF;
    loop {
        match run_watcher_once(&contracts, &cache, &metrics, &mut state).await {
            Ok(()) => {
                debug!("origin-directory watcher stream ended cleanly; restarting subscription");
                backoff = WATCHER_INITIAL_BACKOFF;
                // Any re-subscription (even a clean end) re-arms the resync:
                // events between the old stream's end and the new head-watch
                // would otherwise be lost. Cheap when nothing changed.
                state.resync_from = Some(state.last_block);
            }
            Err(err) => {
                state.resync_from = Some(state.last_block);
                metrics.origin_directory_watcher_backoff_started();
                warn!(
                    %err,
                    backoff_secs = backoff.as_secs(),
                    "ChainOriginDirectory watcher RPC error; restarting after backoff"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(WATCHER_MAX_BACKOFF);
            }
        }
    }
}

/// Re-establish authoritative directory state over a watcher gap. First replays
/// `ContentClaimed` from `start` (minus a reorg overlap) to head — catching any
/// `hash → namespace` claim emitted during the gap — then re-reads `getOrigins`
/// for every known/claimed namespace and the default-open set. The `getOrigins`
/// re-read is what closes the security hole: it reflects current authoritative
/// membership, so any revoke/prune/remove lost in the gap is dropped. Returns the
/// head block scanned. Any RPC error propagates so the caller leaves the resync
/// armed and the next cycle repeats the whole pass.
async fn resync_pass<R: OriginChainReads>(
    reads: &R,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    start: u64,
) -> Result<u64> {
    let head = reads.head_block().await?;
    let mut from = start.saturating_sub(REORG_OVERLAP);
    while from <= head {
        let to = from.saturating_add(REPLAY_WINDOW_BLOCKS - 1).min(head);
        for (hash, namespace) in reads.content_claimed(from, to).await? {
            write_cache(cache, |c| {
                c.namespaces_of.entry(hash).or_default().insert(namespace);
            });
        }
        from = to.saturating_add(1);
    }
    // Authoritatively re-read every namespace we know a claim for (reflecting any
    // revoke/prune lost in the gap) plus the default-open set.
    let namespaces: HashSet<U256> = read_cache(cache, |c| {
        c.origins_of_ns
            .keys()
            .copied()
            .chain(c.namespaces_of.values().flatten().copied())
            .collect()
    });
    for namespace in namespaces {
        resync_namespace(reads, cache, metrics, namespace).await?;
    }
    resync_default_open(reads, cache, metrics).await?;
    Ok(head)
}

/// Run one cycle of the watcher: if armed, run the re-arm resync pass; then open
/// the seven event filters and drain them via `tokio::select!`, re-reading the
/// authoritative set for whatever each event signals changed, until any one
/// returns an error.
#[allow(
    // 7-arm event-dispatch loop across two contracts is fundamentally
    // complex/long; splitting the filter setup from the select obscures the
    // dispatch table without reducing real complexity.
    clippy::cognitive_complexity,
    clippy::too_many_lines
)]
async fn run_watcher_once<P>(
    contracts: &Contracts<P>,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    state: &mut WatcherState,
) -> Result<()>
where
    P: Provider + Clone,
{
    // Re-arm resync: re-establish authoritative state over the gap before going
    // live. Cleared only on full success, so a transient RPC error repeats it.
    if let Some(start) = state.resync_from {
        let head = resync_pass(contracts, cache, metrics, start).await?;
        state.last_block = state.last_block.max(head);
        state.resync_from = None;
    }

    let mut content_claimed = contracts
        .publisher
        .ContentClaimed_filter()
        .watch()
        .await
        .context("watch ContentClaimed")?
        .into_stream();
    let mut activated = contracts
        .origin
        .AssignmentActivated_filter()
        .watch()
        .await
        .context("watch AssignmentActivated")?
        .into_stream();
    let mut revoked = contracts
        .origin
        .AssignmentRevoked_filter()
        .watch()
        .await
        .context("watch AssignmentRevoked")?
        .into_stream();
    let mut pruned = contracts
        .origin
        .BlacklistedAssignmentPruned_filter()
        .watch()
        .await
        .context("watch BlacklistedAssignmentPruned")?
        .into_stream();
    let mut default_updated = contracts
        .origin
        .DefaultOpenAllowlistUpdated_filter()
        .watch()
        .await
        .context("watch DefaultOpenAllowlistUpdated")?
        .into_stream();
    let mut default_added = contracts
        .origin
        .DefaultOpenOperatorAdded_filter()
        .watch()
        .await
        .context("watch DefaultOpenOperatorAdded")?
        .into_stream();
    let mut default_removed = contracts
        .origin
        .DefaultOpenOperatorRemoved_filter()
        .watch()
        .await
        .context("watch DefaultOpenOperatorRemoved")?
        .into_stream();

    metrics.origin_directory_watcher_cycle_established();

    loop {
        tokio::select! {
            ev = content_claimed.next() => match ev {
                Some(Ok((event, log))) => {
                    advance_block(state, log.block_number);
                    on_content_claimed(
                        contracts, cache, metrics,
                        Hash::from_bytes(event.blake3Hash.0), event.namespaceId,
                    ).await;
                }
                Some(Err(e)) => return Err(e).context("ContentClaimed stream"),
                None => return Ok(()),
            },
            ev = activated.next() => match ev {
                Some(Ok((event, log))) => {
                    advance_block(state, log.block_number);
                    on_namespace_changed(contracts, cache, metrics, event.namespaceId).await;
                }
                Some(Err(e)) => return Err(e).context("AssignmentActivated stream"),
                None => return Ok(()),
            },
            ev = revoked.next() => match ev {
                Some(Ok((event, log))) => {
                    advance_block(state, log.block_number);
                    on_origin_removed(
                        contracts, cache, metrics, event.namespaceId, event.operator,
                    ).await;
                }
                Some(Err(e)) => return Err(e).context("AssignmentRevoked stream"),
                None => return Ok(()),
            },
            ev = pruned.next() => match ev {
                Some(Ok((event, log))) => {
                    advance_block(state, log.block_number);
                    on_origin_removed(
                        contracts, cache, metrics, event.namespaceId, event.operator,
                    ).await;
                }
                Some(Err(e)) => return Err(e).context("BlacklistedAssignmentPruned stream"),
                None => return Ok(()),
            },
            ev = default_updated.next() => match ev {
                Some(Ok((_event, log))) => {
                    advance_block(state, log.block_number);
                    on_default_open_changed(contracts, cache, metrics).await;
                }
                Some(Err(e)) => return Err(e).context("DefaultOpenAllowlistUpdated stream"),
                None => return Ok(()),
            },
            ev = default_added.next() => match ev {
                Some(Ok((_event, log))) => {
                    advance_block(state, log.block_number);
                    on_default_open_changed(contracts, cache, metrics).await;
                }
                Some(Err(e)) => return Err(e).context("DefaultOpenOperatorAdded stream"),
                None => return Ok(()),
            },
            ev = default_removed.next() => match ev {
                Some(Ok((event, log))) => {
                    advance_block(state, log.block_number);
                    on_origin_removed(
                        contracts, cache, metrics, DEFAULT_OPEN_NAMESPACE, event.operator,
                    ).await;
                }
                Some(Err(e)) => return Err(e).context("DefaultOpenOperatorRemoved stream"),
                None => return Ok(()),
            },
        }
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
/// hash resolves to empty, fail-closed) and self-heals on the next event for that
/// namespace or the next re-arm resync.
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
        warn!(%err, %namespace, "getOrigins for newly-claimed namespace failed; origins deferred");
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
        warn!(%err, %namespace, "getOrigins re-read failed on activation; namespace origins deferred");
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
        warn!(%err, "getOrigins(0) re-read failed on default-open change; deferred");
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
        warn!(%err, %namespace, %operator, "getOrigins re-read failed on removal; applying precise delta fallback");
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
                warn!(%err, %op, "nodeIdOf failed; operator unmapped until a later event");
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
        claims: Vec<(u64, Hash, U256)>,
        head: u64,
        fail_origins: StdRwLock<HashSet<U256>>,
    }

    impl StubReads {
        fn new() -> Self {
            Self {
                origins: StdRwLock::new(HashMap::new()),
                bindings: HashMap::new(),
                claims: Vec::new(),
                head: 0,
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
        fn claims(mut self, c: &[(u64, Hash, u64)]) -> Self {
            self.claims = c.iter().map(|(b, h, n)| (*b, *h, ns(*n))).collect();
            self
        }
        fn head(mut self, h: u64) -> Self {
            self.head = h;
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
        async fn content_claimed(&self, from: u64, to: u64) -> Result<Vec<(Hash, U256)>> {
            Ok(self
                .claims
                .iter()
                .filter(|(b, _, _)| *b >= from && *b <= to)
                .map(|(_, h, n)| (*h, *n))
                .collect())
        }
        async fn head_block(&self) -> Result<u64> {
            Ok(self.head)
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
    async fn resync_pass_drops_operator_revoked_during_outage() {
        // Scenario 1: a `Revoked(ns1, C)` is lost during a watcher backoff window
        // (never delivered). The cache still lists C in ns 7. The re-arm resync
        // re-reads getOrigins(7) — now {A} — and must drop C.
        let metrics = Arc::new(Metrics::new());
        let cache = shared(cache_with(
            &[(h(1), &[7])],
            &[(7, &[addr(0xA), addr(0xC)])],
            &[],
            &[(addr(0xA), nid(0xA)), (addr(0xC), nid(0xC))],
        ));
        // Chain truth after the lost revoke: ns 7 → {A} only.
        let reads = StubReads::new()
            .origins(&[(7, &[addr(0xA)])])
            .bindings(&[(addr(0xA), nid(0xA))])
            .head(100);
        resync_pass(&reads, &cache, &metrics, 50).await.unwrap();
        read_cache(&cache, |c| {
            assert!(c.origins_of_ns[&ns(7)].contains(&addr(0xA)));
            assert!(
                !c.origins_of_ns[&ns(7)].contains(&addr(0xC)),
                "revoke lost during the outage must be recovered by the resync re-read"
            );
        });
    }

    #[tokio::test]
    async fn resync_pass_recovers_claim_emitted_during_outage() {
        // Scenario 1 (completeness): a `ContentClaimed(h9, ns8)` emitted during
        // the gap is replayed from the cursor, and ns 8's origins are read.
        let metrics = Arc::new(Metrics::new());
        let cache = shared(cache_with(&[], &[], &[], &[]));
        let reads = StubReads::new()
            .origins(&[(8, &[addr(0xB)])])
            .bindings(&[(addr(0xB), nid(0xB))])
            .claims(&[(60, h(9), 8)])
            .head(100);
        resync_pass(&reads, &cache, &metrics, 55).await.unwrap();
        let stakers = StubStakers::new(&[nid(0xB)]);
        read_cache(&cache, |c| {
            assert_eq!(
                c.resolve(&h(9), &stakers),
                vec![nid(0xB)],
                "claim + origins emitted during the gap must be recovered"
            );
        });
    }

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

    #[tokio::test]
    async fn resync_pass_propagates_error_and_leaves_state_armable() {
        // If a getOrigins re-read fails mid-pass, the pass returns Err so the
        // caller keeps `resync_from` set and retries the whole pass.
        let metrics = Arc::new(Metrics::new());
        let cache = shared(cache_with(&[(h(1), &[7])], &[(7, &[addr(0xC)])], &[], &[]));
        let reads = StubReads::new().head(100).fail_origins(&[7]);
        let result = resync_pass(&reads, &cache, &metrics, 50).await;
        assert!(
            result.is_err(),
            "a mid-pass getOrigins failure must propagate"
        );
    }

    #[test]
    fn advance_block_tracks_max_observed() {
        let mut state = WatcherState {
            last_block: 10,
            resync_from: None,
        };
        advance_block(&mut state, Some(25));
        assert_eq!(state.last_block, 25);
        advance_block(&mut state, Some(20)); // lower block does not regress.
        assert_eq!(state.last_block, 25);
        advance_block(&mut state, None); // pending log leaves it unchanged.
        assert_eq!(state.last_block, 25);
    }
}
