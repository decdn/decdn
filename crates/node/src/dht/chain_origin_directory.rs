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
//!     → `CapacityBond.nodeIdOf(operator)` → `(NodeId, active)`
//!     → keep `active == true`
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
//! from genesis (windowed to respect provider `eth_getLogs` range caps). The
//! `namespace → operators` and default-open sets are snapshotted with direct
//! `getOrigins` point reads for every namespace the replay surfaced (plus
//! namespace 0), then kept current from `OriginAssignment` events. A
//! `getLogs`-resync-after-extended-outage path is a follow-up, mirroring the
//! same posture as `ChainStakerSet`; the watcher health metrics surface the
//! drift window.

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
    /// Operator addresses authorised as origins for `hash`: the union over the
    /// hash's claiming namespaces, or — if the hash is unclaimed — the
    /// default-open allow-list (ADR 022 § FIND\_VALUE Flow).
    fn authorised_operators(&self, hash: &Hash) -> HashSet<Address> {
        match self.namespaces_of.get(hash) {
            Some(namespaces) if !namespaces.is_empty() => namespaces
                .iter()
                .filter_map(|ns| self.origins_of_ns.get(ns))
                .flatten()
                .copied()
                .collect(),
            // No claiming namespace → default-open fallback. (An empty
            // default-open set yields no origins: a truly-unclaimed hash.)
            _ => self.default_open.clone(),
        }
    }

    /// Resolve `hash` to currently-active origin `NodeId`s. Maps each
    /// authorised operator to its bound `NodeId` and keeps only those the
    /// [`StakerSet`] reports active. Deduplicated (two namespaces may list the
    /// same operator).
    fn resolve(&self, hash: &Hash, staker_set: &dyn StakerSet) -> Vec<NodeId> {
        self.authorised_operators(hash)
            .iter()
            .filter_map(|op| self.operator_node.get(op))
            .filter(|node_id| staker_set.is_active(node_id))
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Whether at least one active authorised origin exists for `hash`, without
    /// materialising the candidate list (the prefetch authorized-origin gate
    /// only tests emptiness).
    fn has_any(&self, hash: &Hash, staker_set: &dyn StakerSet) -> bool {
        self.authorised_operators(hash)
            .iter()
            .filter_map(|op| self.operator_node.get(op))
            .any(|node_id| staker_set.is_active(node_id))
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

        let cache = bootstrap_cache(&contracts)
            .await
            .context("snapshot OriginAssignment / PublisherRegistry at bootstrap")?;
        info!(
            namespaces = cache.origins_of_ns.len(),
            default_open = cache.default_open.len(),
            operators = cache.operator_node.len(),
            claimed_hashes = cache.namespaces_of.len(),
            "ChainOriginDirectory bootstrap snapshot complete"
        );
        metrics.origin_directory_operator_count(cache.operator_node.len());

        let cache = Arc::new(RwLock::new(cache));
        let watcher_handle = tokio::spawn(watcher_loop(
            contracts,
            Arc::clone(&cache),
            Arc::clone(&metrics),
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
async fn bootstrap_cache<P>(contracts: &Contracts<P>) -> Result<DirectoryCache>
where
    P: Provider + Clone,
{
    let mut cache = DirectoryCache::default();

    // 1. hash → namespaces, via windowed ContentClaimed log replay.
    let latest = contracts
        .publisher
        .provider()
        .get_block_number()
        .await
        .context("get_block_number for ContentClaimed replay")?;
    let mut from = 0u64;
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

    Ok(cache)
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

/// Background event-subscription loop. Follows the `OriginAssignment` and
/// `PublisherRegistry` events that mutate the directory, updating the cache on
/// each observation. On stream failure, restarts with exponential backoff —
/// identical control flow and health-metric semantics to
/// [`crate::dht::chain_staker_set`].
async fn watcher_loop<P>(
    contracts: Contracts<P>,
    cache: Arc<RwLock<DirectoryCache>>,
    metrics: Arc<Metrics>,
) where
    P: Provider + Clone,
{
    let mut backoff = WATCHER_INITIAL_BACKOFF;
    loop {
        match run_watcher_once(&contracts, &cache, &metrics).await {
            Ok(()) => {
                debug!("origin-directory watcher stream ended cleanly; restarting subscription");
                backoff = WATCHER_INITIAL_BACKOFF;
            }
            Err(err) => {
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

/// Run one cycle of the watcher: open the seven event filters, drain them via
/// `tokio::select!` until any one returns an error.
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
) -> Result<()>
where
    P: Provider + Clone,
{
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
                Some(Ok((event, _log))) => {
                    on_content_claimed(
                        contracts, cache, metrics,
                        Hash::from_bytes(event.blake3Hash.0), event.namespaceId,
                    ).await;
                }
                Some(Err(e)) => return Err(e).context("ContentClaimed stream"),
                None => return Ok(()),
            },
            ev = activated.next() => match ev {
                Some(Ok((event, _log))) => {
                    on_assignment_activated(
                        contracts, cache, metrics, event.namespaceId, event.operators,
                    ).await;
                }
                Some(Err(e)) => return Err(e).context("AssignmentActivated stream"),
                None => return Ok(()),
            },
            ev = revoked.next() => match ev {
                Some(Ok((event, _log))) => {
                    remove_origin(cache, metrics, event.namespaceId, event.operator);
                }
                Some(Err(e)) => return Err(e).context("AssignmentRevoked stream"),
                None => return Ok(()),
            },
            ev = pruned.next() => match ev {
                Some(Ok((event, _log))) => {
                    remove_origin(cache, metrics, event.namespaceId, event.operator);
                }
                Some(Err(e)) => return Err(e).context("BlacklistedAssignmentPruned stream"),
                None => return Ok(()),
            },
            ev = default_updated.next() => match ev {
                Some(Ok((event, _log))) => {
                    on_default_open_replaced(contracts, cache, metrics, event.operators).await;
                }
                Some(Err(e)) => return Err(e).context("DefaultOpenAllowlistUpdated stream"),
                None => return Ok(()),
            },
            ev = default_added.next() => match ev {
                Some(Ok((event, _log))) => {
                    on_default_open_added(contracts, cache, metrics, event.operator).await;
                }
                Some(Err(e)) => return Err(e).context("DefaultOpenOperatorAdded stream"),
                None => return Ok(()),
            },
            ev = default_removed.next() => match ev {
                Some(Ok((event, _log))) => {
                    remove_default_open(cache, event.operator);
                }
                Some(Err(e)) => return Err(e).context("DefaultOpenOperatorRemoved stream"),
                None => return Ok(()),
            },
        }
    }
}

/// A new `(hash, namespace)` claim. Record the mapping and, for a
/// not-yet-known namespace, snapshot its current operator set so the hash
/// resolves immediately.
async fn on_content_claimed<P>(
    contracts: &Contracts<P>,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    hash: Hash,
    namespace: U256,
) where
    P: Provider + Clone,
{
    let namespace_known = write_cache(cache, |c| {
        c.namespaces_of.entry(hash).or_default().insert(namespace);
        c.origins_of_ns.contains_key(&namespace)
    });
    if !namespace_known {
        match contracts.origin.getOrigins(namespace).call().await {
            Ok(operators) => {
                let ops: Vec<Address> = operators;
                resolve_and_store_operators(contracts, cache, metrics, &ops).await;
                write_cache(cache, |c| {
                    c.origins_of_ns.insert(namespace, ops.into_iter().collect());
                });
            }
            Err(err) => {
                metrics.origin_directory_watcher_resolve_failure();
                warn!(%err, %namespace, "getOrigins for newly-claimed namespace failed; origins deferred");
            }
        }
    }
}

/// A namespace's authorized set was replaced wholesale.
async fn on_assignment_activated<P>(
    contracts: &Contracts<P>,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    namespace: U256,
    operators: Vec<Address>,
) where
    P: Provider + Clone,
{
    resolve_and_store_operators(contracts, cache, metrics, &operators).await;
    write_cache(cache, |c| {
        c.origins_of_ns
            .insert(namespace, operators.into_iter().collect());
    });
}

/// The default-open allow-list was replaced wholesale.
async fn on_default_open_replaced<P>(
    contracts: &Contracts<P>,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    operators: Vec<Address>,
) where
    P: Provider + Clone,
{
    resolve_and_store_operators(contracts, cache, metrics, &operators).await;
    write_cache(cache, |c| {
        c.default_open = operators.into_iter().collect();
    });
}

/// A single operator was added to the default-open allow-list.
async fn on_default_open_added<P>(
    contracts: &Contracts<P>,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    operator: Address,
) where
    P: Provider + Clone,
{
    resolve_and_store_operators(contracts, cache, metrics, std::slice::from_ref(&operator)).await;
    write_cache(cache, |c| {
        c.default_open.insert(operator);
    });
}

/// Remove an operator from a namespace's authorized set (revoke / prune).
fn remove_origin(
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
    metrics.origin_directory_operator_count(cache_operator_count(cache));
}

/// Remove an operator from the default-open allow-list.
fn remove_default_open(cache: &Arc<RwLock<DirectoryCache>>, operator: Address) {
    write_cache(cache, |c| {
        c.default_open.remove(&operator);
    });
}

/// Resolve any not-yet-cached operators to their `NodeId` and store the
/// bindings. A failed `nodeIdOf` bumps the resolve-failure counter and is
/// skipped (the operator can't be mapped until a later event re-surfaces it) —
/// the same silent-drift posture as `ChainStakerSet`.
async fn resolve_and_store_operators<P>(
    contracts: &Contracts<P>,
    cache: &Arc<RwLock<DirectoryCache>>,
    metrics: &Arc<Metrics>,
    operators: &[Address],
) where
    P: Provider + Clone,
{
    for &op in operators {
        if read_cache(cache, |c| c.operator_node.contains_key(&op)) {
            continue;
        }
        match resolve_node_id(&contracts.bond, op).await {
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
    metrics.origin_directory_operator_count(cache_operator_count(cache));
}

/// Number of cached `operator → NodeId` bindings, for the size gauge.
fn cache_operator_count(cache: &Arc<RwLock<DirectoryCache>>) -> usize {
    read_cache(cache, |c| c.operator_node.len())
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

    #[test]
    fn claimed_hash_resolves_to_its_namespace_operators() {
        let c = cache_with(
            &[(h(1), &[7])],
            &[(7, &[addr(0xA), addr(0xB)])],
            &[],
            &[(addr(0xA), nid(0xA)), (addr(0xB), nid(0xB))],
        );
        let stakers = StubStakers::new(&[nid(0xA), nid(0xB)]);
        let mut got = c.resolve(&h(1), &stakers);
        got.sort();
        assert_eq!(got, vec![nid(0xA), nid(0xB)]);
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
        let mut got = c.resolve(&h(1), &stakers);
        got.sort();
        assert_eq!(
            got,
            vec![nid(0xA), nid(0xC)],
            "A appears once despite two namespaces"
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
}
