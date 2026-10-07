//! Blacklist compliance watcher (ADR 011 § Content Takedown).
//!
//! Every paid-delivery node runs this task after resolving the mandatory
//! `blockchain.content_blacklist_address`. It keeps the local blob store
//! compliant with `ContentBlacklist`: it evicts any blob whose hash is
//! blacklisted *in scope* for this operator (global ∪ current-region ∪
//! ripening-prev-region). The scope decision is the contract's
//! `isHashBlacklistedForOperator` view, so region packing and the ADR 030
//! ripening math never leave the chain.
//!
//! **Enumerated deny-set, re-scoped.** The set of blacklisted entries is read
//! straight from `ContentBlacklist`'s enumeration views at one pinned block on
//! boot, rather than replayed from `HashBlacklisted` logs (#1497). Two views feed
//! two sets:
//!
//! - **Hash entries.** `getScopeRegions(operator)` gives the one-to-three region
//!   keys in scope right now; each region's set is paged from
//!   `blacklistedHashes` / `blacklistedHashCount`. Entries are keyed by
//!   `(region, hash)` — the contract's own key (`_hashEntries[region][hash]`) —
//!   so a same-hash entry under another region survives a `HashRemoved` for the
//!   first. Every seen-but-not-yet-evicted entry is retained in `known` and
//!   re-scoped on a periodic (`on_tick_complete`) pass, because a hash can become
//!   live + in scope with **no** `HashBlacklisted` event, via an operator
//!   region/ripening change (`CapacityBond.updateRegion`), which emits nothing on
//!   this contract at all.
//! - **Address union.** `blacklistedAddresses` / `blacklistedAddressCount`
//!   enumerate the origin ∪ operator deny set. Each address is liveness-filtered
//!   by the contract's own `isOriginBlacklisted(a) || isOperatorBlacklisted(a)`
//!   disjunction (the same one `OriginAssignment._syncAddr` evaluates), which
//!   keeps voted-out OPERATORS (`addOperator` sets only the operator mapping) and
//!   live origins while dropping emergency origins past their auto-expiry. The
//!   survivors seed [`crate::content_deny::ContentDenylist`], which the delivery
//!   path consults to refuse any `StreamRequest` funded by a blacklisted address.
//!
//! **No durable projection, no scan cursor.** Both sets are enumerable on chain,
//! so a restart rebuilds them by reading the chain rather than by reloading a
//! persisted mirror or replaying from a deploy-block floor. The live tail is
//! seeded at the enumeration snapshot block (`CursorStart::Seeded`, no persist) —
//! there is no historical scan on any boot, warm or cold. This is the same
//! shape the origin directory and slash watchers already use.
//!
//! The pinned-block reads are load-bearing, not tidiness: the enumeration views
//! remove by swap-and-pop, so a page and the count it is checked against MUST be
//! read at one block height, or a concurrent removal can move an unread element
//! into an already-read slot and skip it. A `seen != count` mismatch aborts the
//! snapshot rather than seating a partial set.
//!
//! **The live tail** follows both event families on the shared `resumable_watcher`
//! `eth_getLogs` poller: `HashBlacklisted`/`HashRemoved` for the hash set and
//! `OriginBlacklistUpdated` + `OperatorBlacklisted`/`OperatorBlacklistCleared`
//! for the address union (both routed to the one deny-set, mirroring
//! `OriginAssignment`'s union). It is a low-latency signal to re-read; the
//! periodic re-enumeration is the backstop that catches anything the tail missed
//! (a reorg at the tip, or a region/ripening transition into a never-enumerated
//! region).
//!
//! Enforcement is two writes per hash, in this order: the *deny* records that the
//! refusal is governance-sourced, then the *eviction* reclaims the bytes.
//!
//! [`decdn_cache::CacheEngine::evict`] durably records the takedown (survives
//! restart via `evicted.log`) and cascades to every serving surface through
//! [`decdn_cache::CacheEngine::refuses`]: the DHT republisher drops the hash on
//! its next tick, the probe handler stops signing `has_blob: true`, and the
//! client handler never re-pull-fills it. `evict` is sticky and works on absent
//! hashes, so a hash blacklisted while the node was offline (and not yet held) is
//! still pre-blocked.
//!
//! The deny is what `evicted.log` cannot express: *why*. Without it the client
//! handler answers a governance takedown with `EvictedSinceProbe` while a local
//! `[content] denied_hashes` entry answers `HashBlacklisted`, so one request tells
//! a client which list a hash is on — the fingerprint ADR 011 §`StreamRequest`
//! Response forecloses. The governance deny-set the delivery path reads
//! (`CacheEngine::set_chain_denied_one`) is rebuilt each boot by re-checking every
//! enumerated hash through the same `isHashBlacklistedForOperator` liveness the
//! tail uses, so a lapsed entry is not enforced.
//!
//! A fail-closed readiness gate (`InitialSyncGate`) keeps every ALPN listener shut
//! until the first enumeration + enforcement pass completes: a node refuses to
//! serve until blacklist enforcement is live.
//!
//! **Resilience.** The re-scope + re-enumeration pass runs on the operator's
//! rescan cadence (checked at the end of every poll tick), pulled forward to the
//! poll cadence whenever a live re-check fails — an enforcement failure must not
//! wait out the full cadence while the blob stays slashably servable; every RPC
//! read is bounded by a per-call timeout; the re-scope is interruptible by
//! shutdown (checked between hashes) so a large backlog cannot overrun the runtime
//! shutdown deadline. Serving a blacklisted hash is slashable
//! (`SlashJudge.submitBlacklistChallenge`), so prompt eviction is the node's only
//! local protection.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::eips::BlockId;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use anyhow::{Context as _, Result};
use decdn_cache::{CacheEngine, Hash};
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::content_blacklist::ContentBlacklist;
use decdn_incentive::content_blacklist::ContentBlacklist::{
    HashBlacklisted, HashRemoved, OperatorBlacklistCleared, OperatorBlacklisted,
    OriginBlacklistUpdated,
};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain_events::boot_retry::{BootFault, BootRetry};
use crate::chain_events::multiplexed_poller::{Route, SinkSource};
use crate::chain_events::resumable_watcher::{CursorStart, LogSink, clear_cadence_on_recovery};
use crate::chain_events::shared_head::{HeadSource, snapshot_block};
use crate::chain_events::timed;
use crate::chain_freshness::ChainFreshness;
use crate::content_deny::ContentDenylist;
use crate::metrics::{Metrics, metric_hook};
use crate::warming_allowance::WarmingAllowance;

/// Page size for the swap-and-pop enumeration views. Bounded so one huge deny-set
/// cannot ask for an unbounded array in a single `eth_call`; the paging loop keeps
/// reading until it has `count` entries at the pinned block.
const BLACKLIST_ENUM_PAGE_SIZE: u64 = 100;

/// Result reported exactly once when the first enumeration + enforcement pass
/// either establishes compliance or proves startup cannot safely continue.
pub(crate) type InitialSyncResult = std::result::Result<(), String>;

#[derive(Clone)]
struct InitialSyncGate(Arc<Mutex<Option<oneshot::Sender<InitialSyncResult>>>>);

impl InitialSyncGate {
    fn new(sender: oneshot::Sender<InitialSyncResult>) -> Self {
        Self(Arc::new(Mutex::new(Some(sender))))
    }

    fn signal(&self, result: InitialSyncResult) {
        let sender = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(sender) = sender {
            let _ = sender.send(result);
        }
    }
}

/// The chain reads the boot enumeration and the periodic re-enumeration perform,
/// behind a trait so the pure paging + liveness helpers are unit-testable with a
/// scripted stub and no live chain.
///
/// Spelled RPITIT with an explicit `+ Send` (rather than `async fn`, whose futures
/// carry no `Send` bound) because the re-enumeration calls these from inside the
/// spawned watcher's `on_tick_complete`, whose future `tokio::spawn` requires to be
/// `Send`. Production monomorphizes to the alloy contract impl
/// ([`ContractReads`]).
trait BlacklistChainReads: Send + Sync {
    /// The block every enumeration read is pinned to
    /// (`shared_head::snapshot_block`): the lag margin below the head.
    fn snapshot_block(&self) -> impl Future<Output = Result<u64>> + Send;
    /// The one-to-three region keys in scope for `operator` right now
    /// (`getScopeRegions`, GLOBAL first).
    fn scope_regions(
        &self,
        operator: Address,
        at: u64,
    ) -> impl Future<Output = Result<Vec<B256>>> + Send;
    /// Number of entries indexed under `region` (`blacklistedHashCount`).
    fn blacklisted_hash_count(
        &self,
        region: B256,
        at: u64,
    ) -> impl Future<Output = Result<U256>> + Send;
    /// A page of `region`'s RAW entry set (`blacklistedHashes`).
    fn blacklisted_hashes(
        &self,
        region: B256,
        offset: U256,
        limit: U256,
        at: u64,
    ) -> impl Future<Output = Result<Vec<B256>>> + Send;
    /// Number of addresses in the origin ∪ operator deny set
    /// (`blacklistedAddressCount`).
    fn blacklisted_address_count(&self, at: u64) -> impl Future<Output = Result<U256>> + Send;
    /// A page of the RAW origin ∪ operator address set (`blacklistedAddresses`).
    fn blacklisted_addresses(
        &self,
        offset: U256,
        limit: U256,
        at: u64,
    ) -> impl Future<Output = Result<Vec<Address>>> + Send;
    /// Origin-level liveness (`isOriginBlacklisted`, honouring emergency
    /// auto-expiry). `false` for an operator-only entry — must be OR-ed with
    /// [`Self::is_operator_blacklisted`].
    fn is_origin_blacklisted(
        &self,
        addr: Address,
        at: u64,
    ) -> impl Future<Output = Result<bool>> + Send;
    /// Operator-level liveness (`isOperatorBlacklisted`, the `addOperator` leg).
    fn is_operator_blacklisted(
        &self,
        addr: Address,
        at: u64,
    ) -> impl Future<Output = Result<bool>> + Send;
}

/// Production [`BlacklistChainReads`] over the live `ContentBlacklist` contract.
/// Every read is bounded by the shared [`timed`] per-call timeout.
#[derive(Clone)]
struct ContractReads<P: Provider + Clone> {
    contract: ContentBlacklist::ContentBlacklistInstance<P>,
    /// The shared, TTL-cached single-flight head source every watcher reads
    /// through (`chain_events::shared_head`), so the enumeration snapshot's block
    /// pin does not cost its own per-boot `eth_blockNumber` call.
    head: Arc<dyn HeadSource>,
}

impl<P: Provider + Clone> BlacklistChainReads for ContractReads<P> {
    async fn snapshot_block(&self) -> Result<u64> {
        snapshot_block(
            self.contract.provider(),
            &*self.head,
            *self.contract.address(),
        )
        .await
    }

    async fn scope_regions(&self, operator: Address, at: u64) -> Result<Vec<B256>> {
        timed(
            None,
            "getScopeRegions",
            self.contract
                .getScopeRegions(operator)
                .block(BlockId::Number(at.into()))
                .call(),
        )
        .await
    }

    async fn blacklisted_hash_count(&self, region: B256, at: u64) -> Result<U256> {
        timed(
            None,
            "blacklistedHashCount",
            self.contract
                .blacklistedHashCount(region)
                .block(BlockId::Number(at.into()))
                .call(),
        )
        .await
    }

    async fn blacklisted_hashes(
        &self,
        region: B256,
        offset: U256,
        limit: U256,
        at: u64,
    ) -> Result<Vec<B256>> {
        timed(
            None,
            "blacklistedHashes",
            self.contract
                .blacklistedHashes(region, offset, limit)
                .block(BlockId::Number(at.into()))
                .call(),
        )
        .await
    }

    async fn blacklisted_address_count(&self, at: u64) -> Result<U256> {
        timed(
            None,
            "blacklistedAddressCount",
            self.contract
                .blacklistedAddressCount()
                .block(BlockId::Number(at.into()))
                .call(),
        )
        .await
    }

    async fn blacklisted_addresses(
        &self,
        offset: U256,
        limit: U256,
        at: u64,
    ) -> Result<Vec<Address>> {
        timed(
            None,
            "blacklistedAddresses",
            self.contract
                .blacklistedAddresses(offset, limit)
                .block(BlockId::Number(at.into()))
                .call(),
        )
        .await
    }

    async fn is_origin_blacklisted(&self, addr: Address, at: u64) -> Result<bool> {
        timed(
            None,
            "isOriginBlacklisted",
            self.contract
                .isOriginBlacklisted(addr)
                .block(BlockId::Number(at.into()))
                .call(),
        )
        .await
    }

    async fn is_operator_blacklisted(&self, addr: Address, at: u64) -> Result<bool> {
        timed(
            None,
            "isOperatorBlacklisted",
            self.contract
                .isOperatorBlacklisted(addr)
                .block(BlockId::Number(at.into()))
                .call(),
        )
        .await
    }
}

/// The current on-chain deny-set as enumerated at one pinned block.
struct BootstrapSnapshot {
    /// The block every read below was pinned to; the tail is seeded here.
    block: u64,
    /// The origin ∪ operator address deny set, liveness-filtered by the union
    /// predicate `isOriginBlacklisted || isOperatorBlacklisted`.
    origins: HashSet<Address>,
    /// Every `(region, hash)` entry across the in-scope regions.
    known: HashSet<(B256, Hash)>,
}

/// Enumerate the on-chain deny-set at one pinned block, the lag margin below
/// head: page the address union and the per-region hash sets against that
/// height.
async fn bootstrap_snapshot<R: BlacklistChainReads>(
    reads: &R,
    operator: Address,
) -> Result<BootstrapSnapshot> {
    let block = reads
        .snapshot_block()
        .await
        .context("read the ContentBlacklist enumeration snapshot block")?;
    let origins = enumerate_address_union(reads, operator, block).await?;
    let known = enumerate_known(reads, operator, block).await?;
    Ok(BootstrapSnapshot {
        block,
        origins,
        known,
    })
}

/// Page the origin ∪ operator address set at `at`, count-checked, then keep only
/// the addresses that are LIVE by the UNION predicate.
///
/// The liveness filter is `isOriginBlacklisted(a) || isOperatorBlacklisted(a)` —
/// the same disjunction `ContentBlacklist._syncAddr` / `OriginAssignment` use.
/// Filtering by `isOriginBlacklisted` ALONE would drop every voted-out operator
/// (`addOperator` sets only the operator mapping and never touches the origin
/// mapping) = the #1499 hole, a restarted node serving a governance-ejected
/// operator. The RAW enumeration also lists emergency origins past their
/// auto-expiry, which the union drops (fail-open) — operators never auto-expire.
async fn enumerate_address_union<R: BlacklistChainReads>(
    reads: &R,
    _operator: Address,
    at: u64,
) -> Result<HashSet<Address>> {
    let count = reads
        .blacklisted_address_count(at)
        .await
        .context("blacklistedAddressCount")?;
    let mut raw: HashSet<Address> = HashSet::new();
    let mut offset = U256::ZERO;
    let page = U256::from(BLACKLIST_ENUM_PAGE_SIZE);
    while offset < count {
        let batch = reads
            .blacklisted_addresses(offset, page, at)
            .await
            .with_context(|| format!("blacklistedAddresses(offset={offset}, limit={page})"))?;
        if batch.is_empty() {
            break;
        }
        offset = offset.saturating_add(U256::from(batch.len()));
        raw.extend(batch);
    }
    let seen = U256::from(raw.len());
    anyhow::ensure!(
        seen == count,
        "address deny-set enumeration read {seen} of {count} entries at block {at}; the pinned \
         set was read inconsistently (a mid-page swap-and-pop removal, or an inconsistent/reorged \
         RPC view of this block), so the snapshot would be missing an address — aborting rather \
         than seating a partial set"
    );

    let mut live: HashSet<Address> = HashSet::with_capacity(raw.len());
    for addr in raw {
        let denied = reads
            .is_origin_blacklisted(addr, at)
            .await
            .with_context(|| format!("isOriginBlacklisted({addr})"))?
            || reads
                .is_operator_blacklisted(addr, at)
                .await
                .with_context(|| format!("isOperatorBlacklisted({addr})"))?;
        if denied {
            live.insert(addr);
        }
    }
    Ok(live)
}

/// Page `region`'s RAW hash set at `at`, count-checked. Liveness (emergency
/// auto-expiry, region scope) is applied later by `isHashBlacklistedForOperator`
/// during enforcement, matching the tail's `scope_check`.
async fn enumerate_region_hashes<R: BlacklistChainReads>(
    reads: &R,
    region: B256,
    at: u64,
) -> Result<HashSet<Hash>> {
    let count = reads
        .blacklisted_hash_count(region, at)
        .await
        .with_context(|| format!("blacklistedHashCount({region})"))?;
    let mut hashes: HashSet<Hash> = HashSet::new();
    let mut offset = U256::ZERO;
    let page = U256::from(BLACKLIST_ENUM_PAGE_SIZE);
    while offset < count {
        let batch = reads
            .blacklisted_hashes(region, offset, page, at)
            .await
            .with_context(|| {
                format!("blacklistedHashes(region={region}, offset={offset}, limit={page})")
            })?;
        if batch.is_empty() {
            break;
        }
        offset = offset.saturating_add(U256::from(batch.len()));
        hashes.extend(batch.into_iter().map(|h| Hash::from_bytes(h.0)));
    }
    let seen = U256::from(hashes.len());
    anyhow::ensure!(
        seen == count,
        "region {region} hash enumeration read {seen} of {count} entries at block {at}; the \
         pinned set was read inconsistently (a mid-page swap-and-pop removal, or an \
         inconsistent/reorged RPC view of this block), so the snapshot would be missing a hash — \
         aborting rather than seating a partial set"
    );
    Ok(hashes)
}

/// Build the `(region, hash)` deny-set across the operator's in-scope regions.
async fn enumerate_known<R: BlacklistChainReads>(
    reads: &R,
    operator: Address,
    at: u64,
) -> Result<HashSet<(B256, Hash)>> {
    let regions = reads
        .scope_regions(operator, at)
        .await
        .with_context(|| format!("getScopeRegions({operator})"))?;
    let mut known = HashSet::new();
    for region in regions {
        for hash in enumerate_region_hashes(reads, region, at).await? {
            known.insert((region, hash));
        }
    }
    Ok(known)
}

/// Mutable deny-set carried across poll ticks. The scan cursor lives on the
/// resumable watcher (seeded at the boot snapshot); this holds only the
/// re-scopable entry set.
struct WatcherState {
    /// Every blacklisted `(region, hash)` entry seen and not yet locally evicted —
    /// including out-of-scope entries — re-scoped on each `on_tick_complete` pass
    /// so a later region/ripening transition (which emits no `HashBlacklisted`)
    /// still leads to eviction. Keyed like the contract's `_hashEntries`.
    known: HashSet<(B256, Hash)>,
    /// The live origin deny-set the delivery path reads (ADR 011 §On Blacklist
    /// Event). Seeded from the enumerated address union on boot and fed by
    /// `OriginBlacklistUpdated` / `OperatorBlacklisted` on the tail.
    denylist: Arc<ContentDenylist>,
    /// The latest tail change to each origin: its block and the blacklisted
    /// state it set. A re-enumeration applies every change above its snapshot
    /// block on top of the snapshot's origins, then forgets the rest.
    origin_changes: HashMap<Address, (u64, bool)>,
    /// The block of each `(region, hash)` entry the tail removed. A
    /// re-enumeration skips a snapshot entry the tail removed above its snapshot
    /// block, then forgets the rest.
    removed_entries: HashMap<(B256, Hash), u64>,
}

impl WatcherState {
    /// Record a `(region, hash)` entry in the re-scoping worklist.
    fn add_entry(&mut self, region: B256, hash: Hash) {
        self.known.insert((region, hash));
    }

    /// Fold a fresh re-enumeration snapshot into the projections. The asymmetry is
    /// deliberate and correctness-critical: the `known` worklist is UNIONED so an
    /// out-of-scope entry the tail learned (and retained for a future ripening
    /// transition, which emits no event) is not dropped by an in-scope-only
    /// enumeration, while the chain origins are authoritative and REPLACED
    /// wholesale.
    ///
    /// The snapshot reads the lagged block, so it misses any change the tail
    /// already applied above that block. Both halves keep those changes: every
    /// origin change above the block is applied on top of the snapshot's origins,
    /// and a snapshot entry the tail removed above the block is not re-added.
    /// Without that, the fold would drop an origin the tail blacklisted inside the
    /// margin.
    fn fold_reenumeration(&mut self, snapshot: BootstrapSnapshot) {
        let at = snapshot.block;
        self.origin_changes.retain(|_, (block, _)| *block > at);
        self.removed_entries.retain(|_, block| *block > at);
        let mut origins = snapshot.origins;
        for (origin, (_, blacklisted)) in &self.origin_changes {
            if *blacklisted {
                origins.insert(*origin);
            } else {
                origins.remove(origin);
            }
        }
        let removed = &self.removed_entries;
        self.known.extend(
            snapshot
                .known
                .into_iter()
                .filter(|entry| !removed.contains_key(entry)),
        );
        self.denylist.set_chain_origins(origins);
    }

    /// Drop exactly the `(region, hash)` entry — same-hash entries under other
    /// regions stay retained for re-scoping — and note the block of the removal
    /// for [`Self::fold_reenumeration`].
    fn remove_entry(&mut self, region: B256, hash: Hash, block: u64) {
        self.known.remove(&(region, hash));
        self.removed_entries.insert((region, hash), block);
    }

    /// Drop every entry for `hash` (once locally evicted, the sticky eviction
    /// covers all regions).
    fn drop_hash(&mut self, hash: Hash) {
        self.known.retain(|(_, known_hash)| *known_hash != hash);
    }

    /// Distinct hashes across all regions — the scope view
    /// (`isHashBlacklistedForOperator`) is per `(operator, hash)`, so each hash
    /// needs exactly one `eth_call` per pass regardless of how many regional
    /// entries reference it.
    fn distinct_hashes(&self) -> Vec<Hash> {
        let unique: HashSet<Hash> = self.known.iter().map(|(_, hash)| *hash).collect();
        unique.into_iter().collect()
    }

    /// Apply an origin/operator blacklist state change to the live deny-set the
    /// delivery path and the origin-routing filter both read. Both on-chain lists
    /// (`OriginBlacklistUpdated` and `OperatorBlacklisted`) feed this one set as a
    /// union `isOriginBlacklisted(op) || isOperatorBlacklisted(op)` — the node does
    /// not need to know which list an address came from, only that governance put
    /// it on one. Notes the change and its block for
    /// [`Self::fold_reenumeration`].
    fn set_origin(&mut self, origin: Address, blacklisted: bool, block: u64) {
        self.denylist.apply_chain_origin(origin, blacklisted);
        self.origin_changes.insert(origin, (block, blacklisted));
    }
}

/// Applies the five live event families to the deny-set, enforces compliance, and
/// re-enumerates + re-scopes on the operator's cadence. `apply` records each entry
/// and, for a `HashBlacklisted`, immediately re-checks scope + evicts (prompt live
/// enforcement); an undecodable ORIGIN log aborts the tick (there is no per-tick
/// backstop to reveal a skipped one until the next re-enumeration), while an
/// undecodable HASH log is logged and skipped (re-scoped
/// every pass, sticky eviction). [`Self::on_tick_complete`] runs the batched
/// re-enumeration + re-scope.
struct BlacklistSink<P: Provider + Clone> {
    contract: ContentBlacklist::ContentBlacklistInstance<P>,
    reads: ContractReads<P>,
    operator: Address,
    cache: CacheEngine,
    warming: Arc<WarmingAllowance>,
    state: WatcherState,
    shutdown: CancellationToken,
    /// How often the batched re-enumeration + re-scope runs (the operator's
    /// `content_blacklist_poll_interval_sec`); the getLogs poll cadence itself is
    /// faster so live entries enforce promptly.
    rescan_interval: Duration,
    /// When the last batched pass ran. Seeded to `Some(now)` at spawn (boot just
    /// enumerated + enforced), so the first backstop is one interval out; cleared
    /// by a failed live re-check to pull the next pass forward.
    last_rescan: Option<Instant>,
    /// Node metrics — feeds `blacklist_enforcement_failures` from a re-scope that
    /// could not enforce every entry (#1319).
    metrics: Arc<Metrics>,
}

impl<P: Provider + Clone> LogSink for BlacklistSink<P> {
    async fn apply(&mut self, log: Log) -> Result<()> {
        let failed = handle_log(
            &self.contract,
            self.operator,
            &self.cache,
            &self.warming,
            &mut self.state,
            log,
        )
        .await?;
        // A live `HashBlacklisted` whose scope-check or eviction failed must retry
        // at the poll cadence (seconds), not the operator's re-scope cadence
        // (default 10 min) — serving the blob meanwhile is slashable. Clearing
        // `last_rescan` forces the batched pass on this tick's `on_tick_complete`.
        if failed {
            self.last_rescan = None;
        }
        Ok(())
    }

    async fn on_tick_complete(&mut self) -> Result<()> {
        // Re-enumerate + re-scope the whole deny-set on the operator's cadence (not
        // every poll tick): catches a region/ripening transition that emits no
        // event, and a tail event lost to a reorg or RPC backoff.
        let due = self
            .last_rescan
            .is_none_or(|at| at.elapsed() >= self.rescan_interval);

        // The re-enumeration backstop. Rebuild the address union + in-scope hash set
        // at the lagged snapshot block and fold it into the projections: origins are
        // authoritative (replace wholesale, with the tail's newer changes on top),
        // the `known` worklist is UNIONed so an out-of-scope entry the tail learned
        // (retained for future ripening) is not dropped by an in-scope-only
        // enumeration. Build-then-fold; Ok-on-failure keeps the current set and
        // counts `decdn_blacklist_reenumeration_failures_total`. The `rescan` below
        // then re-scopes + evicts.
        if due {
            match bootstrap_snapshot(&self.reads, self.operator).await {
                Ok(snapshot) => self.state.fold_reenumeration(snapshot),
                Err(err) => {
                    self.metrics.blacklist_reenumeration_failure();
                    warn!(
                        error = %sanitize_err_chain(&err),
                        "blacklist watcher: periodic re-enumeration failed; keeping the current \
                         deny-set (the live tail is still the primary path)"
                    );
                }
            }
        }

        if due {
            let RescanOutcome { clean, failed, .. } = rescan(
                &self.contract,
                self.operator,
                &self.cache,
                &self.warming,
                &mut self.state,
                &self.shutdown,
                OnScopeFailure::Continue,
            )
            .await;
            // Surface the slashable "deny-set not fully enforced" condition (#1319):
            // a failed re-scope otherwise returns `Ok(())` while every downtime
            // metric reads healthy. Separate from the down-family (chain-read
            // outages only).
            if failed > 0 {
                self.metrics.blacklist_enforcement_failure(failed);
            }
            // A pass with any failed re-check retries at the poll cadence until it
            // comes back clean; only a clean pass waits out the full cadence again.
            self.last_rescan = clean.then(Instant::now);
        }
        Ok(())
    }

    /// Force the batched re-scope on the tick the watcher recovers.
    ///
    /// The same clock `apply` clears after a failed per-event re-scope, cleared
    /// for the same reason: a stale deny set means serving content a takedown
    /// covers, which is slashable. An outage is when the set is most likely to
    /// have drifted and when the cadence helps least, since the reconcile does
    /// not run at all while the route is errored.
    fn on_recovered(&mut self) {
        clear_cadence_on_recovery(&mut self.last_rescan, self.rescan_interval);
    }
}

/// Outcome of one batched re-scope pass.
struct RescanOutcome {
    /// `true` iff every entry was re-verified with no failed re-check. A
    /// shutdown-cancelled pass is unclean (`false`); it is decidedly **not** a
    /// drift window, but that is enforced in `multiplexed_poller::run` (which
    /// suppresses the backoff edge under a cancelled token), not here (#1321).
    clean: bool,
    /// Distinct hashes this pass could not enforce (`Recheck::Failed` — a cache
    /// error or a scope `eth_call` failure). Feeds `blacklist_enforcement_failures`
    /// so the slashable "deny-set not fully enforced" condition has a metric of its
    /// own. A shutdown-cancelled pass reports the failures observed **before** the
    /// cancel (not `0`).
    failed: u64,
    /// The failure that decides a boot attempt's fate: an eviction failure if
    /// any (a local disk fault a retry does not repair), else the first scope
    /// read failure. A boot attempt reports it as the cause, so it can tell a
    /// deterministic fault from a transient one.
    decisive_failure: Option<RecheckFailure>,
}

/// What a re-scope pass does after a scope read fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OnScopeFailure {
    /// Re-check every remaining hash: the periodic re-scope enforces every hash
    /// it can, and retries the rest on the next tick.
    Continue,
    /// Stop the pass: a boot attempt with a failed scope read is lost and
    /// retries whole, so its remaining reads only add load to a failing
    /// provider.
    Stop,
}

/// Re-scope every distinct hash in `known` (one scope `eth_call` per hash, not per
/// regional entry) and evict those now in scope. Interruptible by shutdown between
/// hashes; `on_scope_failure` decides whether a failed scope read ends the pass.
async fn rescan<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    warming: &Arc<WarmingAllowance>,
    state: &mut WatcherState,
    shutdown: &CancellationToken,
    on_scope_failure: OnScopeFailure,
) -> RescanOutcome
where
    P: Provider + Clone,
{
    let snapshot = state.distinct_hashes();
    let mut evicted = 0usize;
    let mut failed = 0u64;
    let mut decisive_failure: Option<RecheckFailure> = None;
    let mut clean = true;
    for hash in snapshot {
        if shutdown.is_cancelled() {
            return RescanOutcome {
                clean: false,
                failed,
                decisive_failure,
            };
        }
        match recheck(contract, operator, cache, warming, state, hash).await {
            Recheck::Evicted => evicted = evicted.saturating_add(1),
            Recheck::NoAction => {}
            Recheck::Failed(cause) => {
                clean = false;
                failed = failed.saturating_add(1);
                let is_scope = matches!(cause, RecheckFailure::Scope(_));
                let replaces = match &decisive_failure {
                    None => true,
                    Some(RecheckFailure::Scope(_)) => !is_scope,
                    Some(RecheckFailure::Evict(_)) => false,
                };
                if replaces {
                    decisive_failure = Some(cause);
                }
                if is_scope && on_scope_failure == OnScopeFailure::Stop {
                    break;
                }
            }
        }
    }
    if evicted > 0 {
        info!(evicted, "blacklist watcher evicted blacklisted blobs");
    }
    if failed > 0 {
        // The negative counterpart to the `evicted` line: the aggregate count of
        // entries left unenforced this pass (#1319).
        warn!(
            unenforced = failed,
            "blacklist re-scope could not enforce every entry"
        );
    }
    RescanOutcome {
        clean,
        failed,
        decisive_failure,
    }
}

/// Handle one live log: `HashBlacklisted` records the `(region, hash)` entry and
/// re-checks the hash; `HashRemoved` drops exactly that entry (hygiene — eviction
/// stays sticky, and same-hash entries in other regions survive). `Ok(true)` iff a
/// re-check failed and needs a prompt retry.
///
/// `Err` is reserved for an undecodable ORIGIN-class log: until the next
/// re-enumeration there is nothing to sweep against, so
/// skipping one is a silent deny-set gap — aborting the tick holds the scan cursor
/// so the readiness gate keeps the router closed rather than opening on a set we
/// know is incomplete. An undecodable HASH log is skipped (re-scoped every pass,
/// sticky eviction).
async fn handle_log<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    warming: &Arc<WarmingAllowance>,
    state: &mut WatcherState,
    log: Log,
) -> Result<bool>
where
    P: Provider + Clone,
{
    match log.topic0() {
        Some(topic) if *topic == HashBlacklisted::SIGNATURE_HASH => Ok(on_blacklisted_log(
            contract, operator, cache, warming, state, &log,
        )
        .await
        .is_failed()),
        Some(topic) if *topic == HashRemoved::SIGNATURE_HASH => {
            on_removed_log(contract, operator, cache, state, &log).await;
            Ok(false)
        }
        Some(topic) if *topic == OriginBlacklistUpdated::SIGNATURE_HASH => {
            on_origin_log(state, &log)?;
            Ok(false)
        }
        Some(topic) if *topic == OperatorBlacklisted::SIGNATURE_HASH => {
            on_operator_log(state, &log, true)?;
            Ok(false)
        }
        Some(topic) if *topic == OperatorBlacklistCleared::SIGNATURE_HASH => {
            on_operator_log(state, &log, false)?;
            Ok(false)
        }
        _ => Ok(false),
    }
}

/// Decode an `OperatorBlacklisted` / `OperatorBlacklistCleared` log and apply it to
/// the same origin deny-set `OriginBlacklistUpdated` feeds.
fn on_operator_log(state: &mut WatcherState, log: &Log, blacklisted: bool) -> Result<()> {
    let operator = if blacklisted {
        match OperatorBlacklisted::decode_log_data(&log.inner.data) {
            Ok(event) => event.operator,
            Err(err) => return Err(undecodable_origin_log(&err, log, "OperatorBlacklisted")),
        }
    } else {
        match OperatorBlacklistCleared::decode_log_data(&log.inner.data) {
            Ok(event) => event.operator,
            Err(err) => {
                return Err(undecodable_origin_log(
                    &err,
                    log,
                    "OperatorBlacklistCleared",
                ));
            }
        }
    };
    state.set_origin(operator, blacklisted, log_block(log));
    debug!(%operator, blacklisted, "blacklist watcher: operator blacklist updated");
    Ok(())
}

/// The block a tail log was mined in. A log without one never comes back from
/// `eth_getLogs`; one that did would count as the newest change, which a
/// re-enumeration keeps.
fn log_block(log: &Log) -> u64 {
    log.block_number.unwrap_or(u64::MAX)
}

/// An origin-class log we cannot decode is an ENFORCEMENT failure, not a parse
/// curiosity, so it aborts the tick and holds the scan cursor.
///
/// The hash events can afford to skip-and-continue: they are re-scoped every pass
/// and eviction is sticky. The origin events have no such per-tick backstop, so a
/// skipped log is gone until the next
/// re-enumeration and the deny-set is silently short an entry meanwhile. Holding
/// the cursor lets the readiness gate keep the router closed rather than opening on
/// a set we know is incomplete.
fn undecodable_origin_log(err: &alloy::sol_types::Error, log: &Log, event: &str) -> anyhow::Error {
    anyhow::anyhow!("{err}").context(format!(
        "undecodable {event} log at block {:?} tx {:?}; refusing to advance the scan cursor \
         past an unreadable takedown event",
        log.block_number, log.transaction_hash
    ))
}

fn on_origin_log(state: &mut WatcherState, log: &Log) -> Result<()> {
    let event = match OriginBlacklistUpdated::decode_log_data(&log.inner.data) {
        Ok(event) => event,
        Err(err) => return Err(undecodable_origin_log(&err, log, "OriginBlacklistUpdated")),
    };
    state.set_origin(event.origin, event.blacklisted, log_block(log));
    debug!(
        origin = %event.origin,
        blacklisted = event.blacklisted,
        "blacklist watcher: origin blacklist updated"
    );
    Ok(())
}

/// Decode a `HashBlacklisted` log, record its `(region, hash)` entry, and re-check
/// the hash. An undecodable log is [`Recheck::NoAction`] (skipped, per the
/// `LogSink` contract).
async fn on_blacklisted_log<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    warming: &Arc<WarmingAllowance>,
    state: &mut WatcherState,
    log: &Log,
) -> Recheck
where
    P: Provider + Clone,
{
    match HashBlacklisted::decode_log_data(&log.inner.data) {
        Ok(event) => {
            let hash = Hash::from_bytes(event.hash.0);
            state.add_entry(event.region, hash);
            recheck(contract, operator, cache, warming, state, hash).await
        }
        Err(err) => {
            warn!(error = %err, "blacklist watcher: undecodable HashBlacklisted log");
            Recheck::NoAction
        }
    }
}

/// Decode a `HashRemoved` log and drop exactly that `(region, hash)` entry.
///
/// Also retires the hash from the governance deny-set, but only on a definitive
/// out-of-scope read: `HashRemoved` is per-region, and a same-hash entry under
/// another region can still cover this operator. `isHashBlacklistedForOperator` is
/// the authoritative union, so it — not the event — decides. An RPC failure keeps
/// the hash denied, the conservative direction (the hash stays evicted regardless,
/// so the cost is a stale refusal *code*, not a stale refusal).
async fn on_removed_log<P: Provider + Clone>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    log: &Log,
) {
    match HashRemoved::decode_log_data(&log.inner.data) {
        Ok(event) => {
            let hash = Hash::from_bytes(event.hash.0);
            state.remove_entry(event.region, hash, log_block(log));
            if cache.is_chain_denied(hash) {
                lift_deny_if_out_of_scope(contract, operator, cache, hash).await;
            }
            debug!(
                region = %event.region,
                %hash,
                "blacklist entry removed on-chain (local eviction stays sticky)"
            );
        }
        Err(err) => warn!(error = %err, "blacklist watcher: undecodable HashRemoved log"),
    }
}

/// Lift the governance deny on `hash` only on a definitive out-of-scope read. A
/// failed read keeps the deny: over-denying is the safe direction.
async fn lift_deny_if_out_of_scope<P: Provider + Clone>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    hash: Hash,
) {
    match scope_check(contract, operator, hash).await {
        Ok(false) => undeny_hash(cache, hash),
        Ok(true) => {}
        Err(err) => warn!(
            %hash,
            error = %sanitize_err_chain(&err),
            "blacklist watcher: isHashBlacklistedForOperator failed after HashRemoved; \
             keeping the hash governance-denied"
        ),
    }
}

/// Outcome of one scope re-check, so callers can distinguish an enforcement
/// *failure* (retry promptly — the entry may be live and slashable) from a
/// legitimately out-of-scope entry (the periodic re-scope keeps watching it).
#[derive(Debug)]
enum Recheck {
    /// The hash was in scope and its eviction succeeded.
    Evicted,
    /// Nothing to do: already evicted, or currently out of scope.
    NoAction,
    /// The scope read or the eviction failed — the entry is retained and must be
    /// re-checked promptly.
    Failed(RecheckFailure),
}

impl Recheck {
    const fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }
}

/// Why a re-check failed.
#[derive(Debug)]
enum RecheckFailure {
    /// The `isHashBlacklistedForOperator` read failed (RPC error or timeout).
    Scope(anyhow::Error),
    /// The local cache could not evict the hash (a disk error).
    Evict(anyhow::Error),
}

/// Record that `hash` is refused because *governance* blacklisted it, and publish
/// that to the live deny-set the delivery path reads. This is what selects the
/// `HashBlacklisted` wire refusal code over the local-eviction `EvictedSinceProbe`.
fn deny_hash(cache: &CacheEngine, hash: Hash) {
    cache.set_chain_denied_one(hash, true);
}

/// Stop treating `hash` as governance-denied. The hash stays *refused* — eviction
/// is sticky and one-way — so this only moves its wire code from `HashBlacklisted`
/// back to `EvictedSinceProbe`, which is what any other evicted hash answers,
/// leaking nothing.
fn undeny_hash(cache: &CacheEngine, hash: Hash) {
    cache.set_chain_denied_one(hash, false);
}

/// Evict `hash` if in scope. Out-of-scope (`Ok(false)`) and RPC-error (`Err`)
/// hashes keep their `known` entries for the next re-scope (callers insert before
/// calling); evicted hashes drop *all* their regional entries — eviction is sticky
/// and region-independent.
async fn recheck<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    warming: &Arc<WarmingAllowance>,
    state: &mut WatcherState,
    hash: Hash,
) -> Recheck
where
    P: Provider + Clone,
{
    if cache.is_evicted(hash) {
        // Back-fill the governance deny-set for a hash that is already evicted but
        // not yet recorded as governance-denied. `evicted.log` persists the eviction
        // but records no cause, and the deny-set lives only in memory, so after a
        // restart every takedown evicted on a prior boot would otherwise keep
        // answering `EvictedSinceProbe` forever.
        if !cache.is_chain_denied(hash) {
            match scope_check(contract, operator, hash).await {
                Ok(true) => deny_hash(cache, hash),
                Ok(false) => {}
                Err(err) => warn!(
                    %hash,
                    error = %sanitize_err_chain(&err),
                    "blacklist watcher: isHashBlacklistedForOperator failed for an evicted \
                     hash; its governance deny waits for the next re-enumeration"
                ),
            }
        }
        state.drop_hash(hash);
        return Recheck::NoAction;
    }
    match scope_check(contract, operator, hash).await {
        Ok(true) => {
            // Deny before evicting: eviction is what retires the hash from `known`,
            // and `known` is the retry backstop. Denying first also means an
            // eviction that fails on a disk error still stops the serving, since
            // `CacheEngine::refuses` honors this set too.
            deny_hash(cache, hash);
            match evict(cache, warming, hash).await {
                Ok(()) => {
                    state.drop_hash(hash);
                    Recheck::Evicted
                }
                Err(err) => Recheck::Failed(RecheckFailure::Evict(err)),
            }
        }
        Ok(false) => Recheck::NoAction,
        Err(err) => {
            warn!(
                %hash,
                error = %sanitize_err_chain(&err),
                "blacklist watcher: isHashBlacklistedForOperator failed; keeping for re-scope"
            );
            Recheck::Failed(RecheckFailure::Scope(err))
        }
    }
}

/// `isHashBlacklistedForOperator` bounded by the shared
/// [`chain_events::DEFAULT_RPC_CALL_TIMEOUT`]. `Err` on timeout or RPC error;
/// each caller logs it with what the failure means on its path.
///
/// [`chain_events::DEFAULT_RPC_CALL_TIMEOUT`]: crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT
async fn scope_check<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    hash: Hash,
) -> Result<bool>
where
    P: Provider + Clone,
{
    let hash_key = B256::from(*hash.as_bytes());
    timed(
        None,
        "isHashBlacklistedForOperator",
        contract
            .isHashBlacklistedForOperator(hash_key, operator)
            .call(),
    )
    .await
    .with_context(|| format!("isHashBlacklistedForOperator({hash})"))
}

/// Evict `hash` from the cache (durable + sticky). A failure is logged here and
/// returned.
async fn evict(cache: &CacheEngine, warming: &Arc<WarmingAllowance>, hash: Hash) -> Result<()> {
    match cache.evict(hash).await {
        Ok(()) => {
            info!(%hash, "evicted blacklisted blob (ADR 011 compliance)");
            // ADR 041: drop the warming tag for the evicted hash, so a later
            // reuse of this slot can never credit a stale source's allowance.
            warming.forget(hash);
            Ok(())
        }
        Err(err) => {
            // Through `anyhow` so the log carries the whole chain down to the I/O
            // cause, not only the cache error's top line.
            let err = anyhow::Error::new(err).context(format!("evict {hash}"));
            warn!(%hash, error = %sanitize_err_chain(&err), "blacklist watcher: evict failed; will retry");
            Err(err)
        }
    }
}

/// One boot attempt: enumerate the current on-chain deny-set at one pinned
/// block, seed the live origin deny-set the delivery path reads, then enforce —
/// deny + evict every enumerated hash that is in scope right now, decided by the
/// same `isHashBlacklistedForOperator` liveness the tail uses (so a lapsed
/// emergency entry is not enforced).
///
/// The pass stops at the first failed scope read: the attempt is lost and
/// retries whole, so the remaining reads only add load to a failing provider.
/// An unclean pass fails the attempt with its decisive failure as the cause: a
/// local eviction error is a [`BootFault`], since a retry does not repair a
/// disk, and otherwise the scope read keeps its typed RPC error for the retry
/// classifier. The inline pass is not shutdown-interruptible; the periodic
/// re-scope on the sink is.
#[allow(clippy::too_many_arguments)]
async fn enumerate_and_enforce<P>(
    reads: &ContractReads<P>,
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    warming: &Arc<WarmingAllowance>,
    denylist: &Arc<ContentDenylist>,
    metrics: &Arc<Metrics>,
    shutdown: &CancellationToken,
) -> Result<BootEnforced>
where
    P: Provider + Clone,
{
    let snapshot = bootstrap_snapshot(reads, operator).await?;
    denylist.set_chain_origins(snapshot.origins.clone());
    let mut state = WatcherState {
        known: snapshot.known,
        denylist: Arc::clone(denylist),
        origin_changes: HashMap::new(),
        removed_entries: HashMap::new(),
    };
    let RescanOutcome {
        clean,
        failed,
        decisive_failure,
    } = rescan(
        contract,
        operator,
        cache,
        warming,
        &mut state,
        shutdown,
        OnScopeFailure::Stop,
    )
    .await;
    if failed > 0 {
        metrics.blacklist_enforcement_failure(failed);
    }
    if !clean {
        let cause = match decisive_failure {
            Some(RecheckFailure::Scope(err)) => err,
            Some(RecheckFailure::Evict(err)) => BootFault(format!("{err:#}")).into(),
            // Only a cancelled pass is unclean with no failure, and nothing
            // cancels the boot pass's token.
            None => anyhow::anyhow!("enforcement pass was cancelled"),
        };
        return Err(cause.context(format!(
            "initial ContentBlacklist enforcement could not enforce every entry ({failed} failed)"
        )));
    }
    Ok(BootEnforced {
        block: snapshot.block,
        origin_count: snapshot.origins.len(),
        state,
    })
}

/// A clean boot attempt: the deny-set enumerated and enforced.
struct BootEnforced {
    /// The pinned block the enumeration read, where the live tail starts.
    block: u64,
    /// Live origin and operator addresses in the seeded origin deny-set.
    origin_count: usize,
    /// The enforced deny-set the sink carries forward.
    state: WatcherState,
}

/// Enumerate the on-chain deny-set at one pinned block, enforce it, then return
/// the [`Route`] that follows the live tail seeded at that block on the shared
/// multiplexed poller.
///
/// The pinned block sits `SNAPSHOT_LAG_MARGIN_BLOCKS` below head (or at head for
/// a contract deployed inside the margin), so the readiness gate opens on the
/// deny-set as of that block. A takedown inside the margin is enforced by the
/// first tail tick, which replays the margin.
///
/// One boot attempt is the enumeration plus the enforcement pass. An attempt that
/// cannot read the chain (block or enumeration RPC error) or cannot enforce
/// every entry retries on `boot`'s budget. `initial_sync_tx` fires once, when
/// the attempts resolve: `Ok` after a clean pass, so the runtime can gate the
/// ALPN router on blacklist enforcement being live, or `Err` on a deterministic
/// fault or an exhausted budget. That failure also returns `Err`, so the router
/// never opens on an un-vetted deny-set. `rescan_interval` is the batched
/// re-enumeration + re-scope cadence.
///
/// The returned route carries a [`SinkSource::Factory`]: the blacklist sink must
/// observe the poller's own shutdown token (its re-scope polls it between per-hash
/// `eth_call`s), which the poller mints only at spawn — so the sink is built
/// inside that spawn from the freshly-minted token.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn bootstrap<P>(
    provider: P,
    contract_addr: Address,
    operator: Address,
    cache: CacheEngine,
    warming: Arc<WarmingAllowance>,
    head: Arc<dyn HeadSource>,
    rescan_interval: Duration,
    initial_sync_tx: oneshot::Sender<InitialSyncResult>,
    metrics: &Arc<Metrics>,
    denylist: Arc<ContentDenylist>,
    chain_freshness: ChainFreshness,
    boot: &BootRetry,
) -> Result<Route>
where
    P: Provider + Clone + 'static,
{
    let contract = ContentBlacklist::new(contract_addr, provider.clone());
    let reads = ContractReads {
        contract: contract.clone(),
        head: Arc::clone(&head),
    };
    let initial_sync = InitialSyncGate::new(initial_sync_tx);

    // An attempt that cannot enforce every entry is retried whole, unless its
    // first failure is deterministic; re-seeding and re-enforcing are
    // idempotent.
    let boot_shutdown = CancellationToken::new();
    let attempt = boot
        .run("ContentBlacklist boot snapshot", || {
            enumerate_and_enforce(
                &reads,
                &contract,
                operator,
                &cache,
                &warming,
                &denylist,
                metrics,
                &boot_shutdown,
            )
        })
        .await;
    let BootEnforced {
        block: snapshot_block,
        origin_count,
        state,
    } = match attempt {
        Ok(done) => done,
        Err(err) => {
            // Fail CLOSED: signal the readiness gate so the runtime keeps every ALPN
            // listener shut, then abort startup — a node that cannot vet the
            // takedown set must not serve.
            initial_sync.signal(Err(format!(
                "initial ContentBlacklist sync failed: {}",
                sanitize_err_chain(&err)
            )));
            return Err(err).context("enumerate and enforce the ContentBlacklist boot snapshot");
        }
    };
    // The boot enumeration + enforcement read the chain successfully, so seed the
    // freshness clock: a node that has just vetted its deny-set must not read as
    // stale in the window before the first poll tick stamps it.
    chain_freshness.stamp();
    initial_sync.signal(Ok(()));

    info!(
        %contract_addr,
        %operator,
        snapshot_block,
        known_hashes = state.known.len(),
        origins = origin_count,
        "blacklist compliance watcher enumerated its boot snapshot"
    );

    let sink_metrics = Arc::clone(metrics);
    // Unlike the flush-only sinks, `BlacklistSink` must observe the *same* token
    // the poller cancels: `rescan` polls it between per-hash `eth_call`s so a
    // large deny-set re-scope yields promptly to shutdown. The poller mints one
    // token at spawn and hands it to this factory, so sink and loop share it
    // (#1236).
    let sink_factory: SinkSource = SinkSource::Factory(Box::new(move |shutdown| {
        Box::new(BlacklistSink {
            contract,
            reads,
            operator,
            cache,
            warming,
            state,
            shutdown: shutdown.clone(),
            rescan_interval: rescan_interval.max(Duration::from_secs(1)),
            // Boot enumeration + enforcement just ran; first backstop is one
            // interval out.
            last_rescan: Some(Instant::now()),
            metrics: sink_metrics,
        }) as Box<dyn crate::chain_events::multiplexed_poller::ErasedSink>
    }));

    Ok(Route {
        addresses: vec![contract_addr],
        topic0s: blacklist_route_topic0s(),
        start: blacklist_cursor_start(snapshot_block),
        sink: sink_factory,
        label: "blacklist",
        on_established: Some(metric_hook(
            metrics,
            Metrics::blacklist_watcher_cycle_established,
        )),
        on_backoff: Some(metric_hook(
            metrics,
            Metrics::blacklist_watcher_backoff_started,
        )),
        // Every successful poll tick (including idle ones) both stamps the
        // liveness gauge and refreshes the serve-path staleness clock — the two
        // are the same signal, so they fire from one hook.
        on_tick_success: Some({
            let metrics = Arc::clone(metrics);
            let freshness = chain_freshness.clone();
            Box::new(move || {
                metrics.blacklist_watcher_tick();
                freshness.stamp();
            })
        }),
        on_task_panic: Some(metric_hook(
            metrics,
            Metrics::blacklist_watcher_task_panicked,
        )),
    })
}

/// The blacklist route's demux key: hash takedowns plus both origin-blacklist
/// paths. Split out from [`bootstrap`] so the exact topic0 set is
/// unit-testable without a provider.
///
/// Origin blacklisting rides the same scan (ADR 011 § Hash Evasion). Like the
/// hash events, it has no version counter to detect a missed one — the
/// re-enumeration is the backstop. `addOperator` is the PRIMARY governance
/// origin-blacklist path — it writes a SEPARATE mapping and emits
/// `OperatorBlacklisted`/`OperatorBlacklistCleared`, never
/// `OriginBlacklistUpdated`. `OriginAssignment` unions the two mappings
/// on-chain; watching only the first would leave the delivery gate enforcing
/// the softer list and missing the voted one.
fn blacklist_route_topic0s() -> Vec<B256> {
    vec![
        HashBlacklisted::SIGNATURE_HASH,
        HashRemoved::SIGNATURE_HASH,
        OriginBlacklistUpdated::SIGNATURE_HASH,
        OperatorBlacklisted::SIGNATURE_HASH,
        OperatorBlacklistCleared::SIGNATURE_HASH,
    ]
}

/// The blacklist route's cursor start: seed the tail at the enumeration
/// snapshot block. No durable cursor and no historical replay — the boot
/// enumeration rebuilt the whole deny-set, so the tail only follows forward
/// from the snapshot. Split out from [`bootstrap`] so the cursor shape is
/// unit-testable without a provider.
const fn blacklist_cursor_start(snapshot_block: u64) -> CursorStart {
    CursorStart::Seeded { at: snapshot_block }
}

#[cfg(test)]
// Test-only: the assertion style below intentionally panics on the negative
// branch, and unwraps/expects on results the fixtures guarantee. Matches the
// convention in `content_deny.rs` / `config/mod.rs`.
#[allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing
)]
mod tests;
