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

use std::collections::HashSet;
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

use crate::chain_events::multiplexed_poller::{Route, SinkSource};
use crate::chain_events::resumable_watcher::{CursorStart, LogSink};
use crate::chain_events::shared_head::HeadSource;
use crate::chain_events::timed;
use crate::content_deny::ContentDenylist;
use crate::metrics::{Metrics, metric_hook};

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
    /// Current head, the block every enumeration read is pinned to
    /// (`get_block_number`).
    fn block_number(&self) -> impl Future<Output = Result<u64>> + Send;
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
    async fn block_number(&self) -> Result<u64> {
        self.head.head().await
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

/// Enumerate the current on-chain deny-set at one pinned block: read head, then
/// page the address union and the per-region hash sets against that height.
async fn bootstrap_snapshot<R: BlacklistChainReads>(
    reads: &R,
    operator: Address,
) -> Result<BootstrapSnapshot> {
    let block = reads
        .block_number()
        .await
        .context("get_block_number for the ContentBlacklist enumeration snapshot")?;
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
    fn fold_reenumeration(&mut self, snapshot: BootstrapSnapshot) {
        self.known.extend(snapshot.known);
        self.denylist.set_chain_origins(snapshot.origins);
    }

    /// Drop exactly the `(region, hash)` entry — same-hash entries under other
    /// regions stay retained for re-scoping.
    fn remove_entry(&mut self, region: B256, hash: Hash) {
        self.known.remove(&(region, hash));
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
    /// delivery path reads. Both on-chain lists (`OriginBlacklistUpdated` and
    /// `OperatorBlacklisted`) feed this one set, mirroring `OriginAssignment`'s
    /// `isOriginBlacklisted(op) || isOperatorBlacklisted(op)` — the node does not
    /// need to know which list an address came from, only that governance put it on
    /// one.
    fn set_origin(&self, origin: Address, blacklisted: bool) {
        self.denylist.apply_chain_origin(origin, blacklisted);
    }
}

/// Applies the five live event families to the deny-set, enforces compliance, and
/// re-enumerates + re-scopes on the operator's cadence. `apply` records each entry
/// and, for a `HashBlacklisted`, immediately re-checks scope + evicts (prompt live
/// enforcement); an undecodable ORIGIN log aborts the tick (there is no
/// version/enumeration backstop to reveal a skipped one until the next
/// re-enumeration), while an undecodable HASH log is logged and skipped (re-scoped
/// every pass, sticky eviction). [`Self::on_tick_complete`] runs the batched
/// re-enumeration + re-scope.
struct BlacklistSink<P: Provider + Clone> {
    contract: ContentBlacklist::ContentBlacklistInstance<P>,
    reads: ContractReads<P>,
    operator: Address,
    cache: CacheEngine,
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
        // at a fresh pinned block and fold it into the projections: origins are
        // authoritative (replace wholesale), the `known` worklist is UNIONed so an
        // out-of-scope entry the tail learned (retained for future ripening) is not
        // dropped by an in-scope-only enumeration. Build-then-fold; Ok-on-failure
        // keeps the current set. The `rescan` below then re-scopes + evicts.
        if due {
            match bootstrap_snapshot(&self.reads, self.operator).await {
                Ok(snapshot) => self.state.fold_reenumeration(snapshot),
                Err(err) => warn!(
                    err = %sanitize_err_chain(&err),
                    "blacklist watcher: periodic re-enumeration failed; keeping the current \
                     deny-set (the live tail is still the primary path)"
                ),
            }
        }

        if due {
            let RescanOutcome { clean, failed } = rescan(
                &self.contract,
                self.operator,
                &self.cache,
                &mut self.state,
                &self.shutdown,
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
}

/// Re-scope every distinct hash in `known` (one scope `eth_call` per hash, not per
/// regional entry) and evict those now in scope. Interruptible by shutdown between
/// hashes.
async fn rescan<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    shutdown: &CancellationToken,
) -> RescanOutcome
where
    P: Provider + Clone,
{
    let snapshot = state.distinct_hashes();
    let mut evicted = 0usize;
    let mut failed = 0u64;
    let mut clean = true;
    for hash in snapshot {
        if shutdown.is_cancelled() {
            return RescanOutcome {
                clean: false,
                failed,
            };
        }
        match recheck(contract, operator, cache, state, hash).await {
            Recheck::Evicted => evicted = evicted.saturating_add(1),
            Recheck::NoAction => {}
            Recheck::Failed => {
                clean = false;
                failed = failed.saturating_add(1);
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
    RescanOutcome { clean, failed }
}

/// Handle one live log: `HashBlacklisted` records the `(region, hash)` entry and
/// re-checks the hash; `HashRemoved` drops exactly that entry (hygiene — eviction
/// stays sticky, and same-hash entries in other regions survive). `Ok(true)` iff a
/// re-check failed and needs a prompt retry.
///
/// `Err` is reserved for an undecodable ORIGIN-class log: there is no version
/// counter and (until the next re-enumeration) nothing to sweep against, so
/// skipping one is a silent deny-set gap — aborting the tick holds the scan cursor
/// so the readiness gate keeps the router closed rather than opening on a set we
/// know is incomplete. An undecodable HASH log is skipped (re-scoped every pass,
/// sticky eviction).
async fn handle_log<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    log: Log,
) -> Result<bool>
where
    P: Provider + Clone,
{
    match log.topic0() {
        Some(topic) if *topic == HashBlacklisted::SIGNATURE_HASH => {
            Ok(on_blacklisted_log(contract, operator, cache, state, &log).await == Recheck::Failed)
        }
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
    state.set_origin(operator, blacklisted);
    debug!(%operator, blacklisted, "blacklist watcher: operator blacklist updated");
    Ok(())
}

/// An origin-class log we cannot decode is an ENFORCEMENT failure, not a parse
/// curiosity, so it aborts the tick and holds the scan cursor.
///
/// The hash events can afford to skip-and-continue: they are re-scoped every pass
/// and eviction is sticky. The origin events have no such per-tick backstop — no
/// version counter to reveal a gap — so a skipped log is gone until the next
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
    state.set_origin(event.origin, event.blacklisted);
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
            recheck(contract, operator, cache, state, hash).await
        }
        Err(err) => {
            warn!(err = %err, "blacklist watcher: undecodable HashBlacklisted log");
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
            state.remove_entry(event.region, hash);
            if cache.is_chain_denied(hash)
                && scope_check(contract, operator, hash).await == Some(false)
            {
                undeny_hash(cache, hash);
            }
            debug!(
                region = %event.region,
                %hash,
                "blacklist entry removed on-chain (local eviction stays sticky)"
            );
        }
        Err(err) => warn!(err = %err, "blacklist watcher: undecodable HashRemoved log"),
    }
}

/// Outcome of one scope re-check, so callers can distinguish an enforcement
/// *failure* (retry promptly — the entry may be live and slashable) from a
/// legitimately out-of-scope entry (the periodic re-scope keeps watching it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Recheck {
    /// The hash was in scope and its eviction succeeded.
    Evicted,
    /// Nothing to do: already evicted, or currently out of scope.
    NoAction,
    /// The scope read or the eviction failed (RPC error/timeout, cache error) — the
    /// entry is retained and must be re-checked promptly.
    Failed,
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

/// Evict `hash` if in scope. Out-of-scope (`Some(false)`) and RPC-error (`None`)
/// hashes keep their `known` entries for the next re-scope (callers insert before
/// calling); evicted hashes drop *all* their regional entries — eviction is sticky
/// and region-independent.
async fn recheck<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    hash: Hash,
) -> Recheck
where
    P: Provider + Clone,
{
    if cache.is_evicted(hash) {
        // Back-fill the governance deny-set for a hash that is already evicted but
        // not yet recorded as governance-denied: `evicted.log` records no cause, so
        // takedowns discharged by an older build (or a prior boot's eviction) would
        // otherwise keep answering `EvictedSinceProbe` forever.
        if !cache.is_chain_denied(hash) && scope_check(contract, operator, hash).await == Some(true)
        {
            deny_hash(cache, hash);
        }
        state.drop_hash(hash);
        return Recheck::NoAction;
    }
    match scope_check(contract, operator, hash).await {
        Some(true) => {
            // Deny before evicting: eviction is what retires the hash from `known`,
            // and `known` is the retry backstop. Denying first also means an
            // eviction that fails on a disk error still stops the serving, since
            // `CacheEngine::refuses` honors this set too.
            deny_hash(cache, hash);
            if evict(cache, hash).await {
                state.drop_hash(hash);
                Recheck::Evicted
            } else {
                Recheck::Failed
            }
        }
        Some(false) => Recheck::NoAction,
        None => Recheck::Failed,
    }
}

/// `isHashBlacklistedForOperator` bounded by the shared
/// [`chain_events::DEFAULT_RPC_CALL_TIMEOUT`]. `None` on timeout or RPC error
/// (caller keeps the hash in `known` for the next re-scope), `Some(bool)`
/// otherwise.
///
/// [`chain_events::DEFAULT_RPC_CALL_TIMEOUT`]: crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT
async fn scope_check<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    hash: Hash,
) -> Option<bool>
where
    P: Provider + Clone,
{
    let hash_key = B256::from(*hash.as_bytes());
    match timed(
        None,
        "isHashBlacklistedForOperator",
        contract
            .isHashBlacklistedForOperator(hash_key, operator)
            .call(),
    )
    .await
    {
        Ok(flag) => Some(flag),
        Err(err) => {
            warn!(
                %hash,
                err = %sanitize_err_chain(&err),
                "blacklist watcher: isHashBlacklistedForOperator failed; keeping for re-scope"
            );
            None
        }
    }
}

/// Evict `hash` from the cache (durable + sticky). Returns `true` on success.
async fn evict(cache: &CacheEngine, hash: Hash) -> bool {
    match cache.evict(hash).await {
        Ok(()) => {
            info!(%hash, "evicted blacklisted blob (ADR 011 compliance)");
            true
        }
        Err(err) => {
            warn!(%hash, err = %err, "blacklist watcher: evict failed; will retry");
            false
        }
    }
}

/// Enumerate the current on-chain deny-set at one pinned block, enforce it, then
/// return the [`Route`] that follows the live tail seeded at that block on the
/// shared multiplexed poller.
///
/// `initial_sync_tx` fires once the boot enumeration + enforcement pass either
/// completes cleanly (`Ok`) or cannot enforce every entry (`Err`), so the runtime
/// can gate the ALPN router on blacklist enforcement being live. A failure to
/// READ the chain at boot (block or enumeration RPC error) is fatal — it signals
/// `Err` and returns `Err`, so the router never opens on an un-vetted deny-set.
/// `rescan_interval` is the batched re-enumeration + re-scope cadence.
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
    head: Arc<dyn HeadSource>,
    rescan_interval: Duration,
    initial_sync_tx: oneshot::Sender<InitialSyncResult>,
    metrics: &Arc<Metrics>,
    denylist: Arc<ContentDenylist>,
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

    // Enumerate the current on-chain deny-set at one pinned block.
    let snapshot = match bootstrap_snapshot(&reads, operator).await {
        Ok(snapshot) => snapshot,
        Err(err) => {
            // Fail CLOSED: signal the readiness gate so the runtime keeps every ALPN
            // listener shut, then abort startup — a node that cannot read the
            // takedown set must not serve.
            initial_sync.signal(Err(format!(
                "initial ContentBlacklist sync failed: {}",
                sanitize_err_chain(&err)
            )));
            return Err(err).context("enumerate ContentBlacklist state for the boot snapshot");
        }
    };
    let snapshot_block = snapshot.block;
    let origin_count = snapshot.origins.len();

    // Seed the live origin deny-set the delivery path reads.
    denylist.set_chain_origins(snapshot.origins.clone());

    let mut state = WatcherState {
        known: snapshot.known,
        denylist,
    };

    // Initial enforcement pass: deny + evict every enumerated hash that is in scope
    // right now, decided by the same `isHashBlacklistedForOperator` liveness the
    // tail uses (so a lapsed emergency entry is not enforced). A pass that cannot
    // enforce every entry is fail-CLOSED — the router never opens on an un-vetted
    // deny-set. This inline pass is not shutdown-interruptible, matching the boot
    // replay it replaces; the periodic re-scope on the sink IS.
    let boot_shutdown = CancellationToken::new();
    let RescanOutcome { clean, failed } =
        rescan(&contract, operator, &cache, &mut state, &boot_shutdown).await;
    if failed > 0 {
        metrics.blacklist_enforcement_failure(failed);
    }
    if clean {
        initial_sync.signal(Ok(()));
    } else {
        initial_sync.signal(Err(
            "initial ContentBlacklist enumeration could not enforce every entry".to_string(),
        ));
    }

    info!(
        %contract_addr,
        %operator,
        snapshot_block,
        known_hashes = state.known.len(),
        origins = origin_count,
        enforcement_clean = clean,
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
        on_tick_success: Some(metric_hook(metrics, Metrics::blacklist_watcher_tick)),
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
/// Origin blacklisting rides the same scan (ADR 011 § Hash Evasion). It is
/// deliberately outside the `getBlacklistVersion()` mechanism, so unlike the
/// hash events there is no counter to detect a missed one — the
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
/// snapshot head. No durable cursor and no historical replay — the boot
/// enumeration rebuilt the whole deny-set, so the tail only follows forward
/// from the snapshot. Split out from [`bootstrap`] so the cursor shape is
/// unit-testable without a provider.
const fn blacklist_cursor_start(snapshot_block: u64) -> CursorStart {
    CursorStart::Seeded {
        at: snapshot_block,
        persist: None,
    }
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
mod tests {
    use super::*;
    use std::collections::HashMap;

    const US: B256 = B256::repeat_byte(0x01);
    const FR: B256 = B256::repeat_byte(0x02);

    /// The blacklist route watches exactly the five hash + origin blacklist
    /// events — no more, no fewer.
    #[test]
    fn route_topic0s_covers_hash_and_origin_events() {
        assert_eq!(
            blacklist_route_topic0s(),
            vec![
                HashBlacklisted::SIGNATURE_HASH,
                HashRemoved::SIGNATURE_HASH,
                OriginBlacklistUpdated::SIGNATURE_HASH,
                OperatorBlacklisted::SIGNATURE_HASH,
                OperatorBlacklistCleared::SIGNATURE_HASH,
            ]
        );
    }

    /// The blacklist route seeds its cursor at the enumeration snapshot head,
    /// with no durable persistence (the deny-set is rebuilt from enumeration
    /// each boot).
    #[test]
    fn cursor_start_seeds_at_snapshot_with_no_persistence() {
        let start = blacklist_cursor_start(99_999);
        assert_eq!(start.seed(), Some(99_999));
        assert!(
            matches!(start, CursorStart::Seeded { persist: None, .. }),
            "must not carry a durable checkpoint"
        );
    }

    /// A provider that answers nothing. The pure-helper tests never reach an RPC
    /// through `WatcherState`; erasing to `DynProvider` keeps the fixture's type
    /// nameable so it unifies with the sink's own contract instance.
    fn mock_provider() -> alloy::providers::DynProvider {
        alloy::providers::ProviderBuilder::new()
            .connect_mocked_client(alloy::providers::mock::Asserter::new())
            .erased()
    }

    fn state() -> WatcherState {
        state_with_denylist(Arc::new(ContentDenylist::empty()))
    }

    fn state_with_denylist(denylist: Arc<ContentDenylist>) -> WatcherState {
        WatcherState {
            known: HashSet::new(),
            denylist,
        }
    }

    // ----- enumeration helpers (the #1497 core) -----

    fn hash(byte: u8) -> Hash {
        Hash::from_bytes([byte; 32])
    }

    fn b256(byte: u8) -> B256 {
        B256::repeat_byte(byte)
    }

    fn addr(byte: u8) -> Address {
        Address::repeat_byte(byte)
    }

    /// Scripted [`BlacklistChainReads`]: no provider, no chain. The address union
    /// and per-region hash sets are supplied directly, with optional count
    /// overrides so a test can simulate a swap-and-pop `seen != count` skew.
    struct StubReads {
        operator: Address,
        block: u64,
        /// RAW `blacklistedAddresses` membership.
        addresses: Vec<Address>,
        /// Override for `blacklistedAddressCount` (defaults to `addresses.len()`).
        address_count: Option<U256>,
        /// `isOriginBlacklisted == true` for these.
        origin_live: HashSet<Address>,
        /// `isOperatorBlacklisted == true` for these.
        operator_live: HashSet<Address>,
        /// `getScopeRegions(operator)`.
        scope_regions: Vec<B256>,
        /// RAW `blacklistedHashes` membership per region.
        hashes_by_region: HashMap<B256, Vec<B256>>,
        /// Override for a region's `blacklistedHashCount`.
        hash_count: HashMap<B256, U256>,
    }

    impl StubReads {
        fn new(operator: Address) -> Self {
            Self {
                operator,
                block: 42,
                addresses: Vec::new(),
                address_count: None,
                origin_live: HashSet::new(),
                operator_live: HashSet::new(),
                scope_regions: Vec::new(),
                hashes_by_region: HashMap::new(),
                hash_count: HashMap::new(),
            }
        }
    }

    impl BlacklistChainReads for StubReads {
        async fn block_number(&self) -> Result<u64> {
            Ok(self.block)
        }

        async fn scope_regions(&self, operator: Address, at: u64) -> Result<Vec<B256>> {
            assert_eq!(operator, self.operator, "unexpected operator");
            assert_eq!(at, self.block, "reads must be pinned to the snapshot block");
            Ok(self.scope_regions.clone())
        }

        async fn blacklisted_hash_count(&self, region: B256, at: u64) -> Result<U256> {
            assert_eq!(at, self.block);
            Ok(self.hash_count.get(&region).copied().unwrap_or_else(|| {
                U256::from(self.hashes_by_region.get(&region).map_or(0, Vec::len))
            }))
        }

        async fn blacklisted_hashes(
            &self,
            region: B256,
            offset: U256,
            limit: U256,
            at: u64,
        ) -> Result<Vec<B256>> {
            assert_eq!(at, self.block);
            Ok(page(
                self.hashes_by_region
                    .get(&region)
                    .map_or(&[], Vec::as_slice),
                offset,
                limit,
            ))
        }

        async fn blacklisted_address_count(&self, at: u64) -> Result<U256> {
            assert_eq!(at, self.block);
            Ok(self
                .address_count
                .unwrap_or_else(|| U256::from(self.addresses.len())))
        }

        async fn blacklisted_addresses(
            &self,
            offset: U256,
            limit: U256,
            at: u64,
        ) -> Result<Vec<Address>> {
            assert_eq!(at, self.block);
            Ok(page(&self.addresses, offset, limit))
        }

        async fn is_origin_blacklisted(&self, addr: Address, at: u64) -> Result<bool> {
            assert_eq!(at, self.block);
            Ok(self.origin_live.contains(&addr))
        }

        async fn is_operator_blacklisted(&self, addr: Address, at: u64) -> Result<bool> {
            assert_eq!(at, self.block);
            Ok(self.operator_live.contains(&addr))
        }
    }

    /// Slice out one `[offset, offset+limit)` page, saturating at the end.
    fn page<T: Clone>(all: &[T], offset: U256, limit: U256) -> Vec<T> {
        let start: usize = offset.saturating_to();
        let len: usize = limit.saturating_to();
        all.iter().skip(start).take(len).cloned().collect()
    }

    /// (a) The address union is built from `blacklistedAddresses` and
    /// liveness-filtered by the UNION predicate: an origin-only live address and an
    /// operator-only live address both survive; a lapsed emergency origin (in
    /// neither mapping) is dropped.
    #[tokio::test]
    async fn address_union_is_liveness_filtered_by_the_union_predicate() -> Result<()> {
        let op = addr(0xA0);
        let origin_only = addr(0xA1);
        let operator_only = addr(0xA2);
        let lapsed = addr(0xA3);
        let mut stub = StubReads::new(op);
        stub.addresses = vec![origin_only, operator_only, lapsed];
        stub.origin_live = [origin_only].into_iter().collect();
        stub.operator_live = [operator_only].into_iter().collect();

        let union = enumerate_address_union(&stub, op, stub.block).await?;

        assert!(union.contains(&origin_only), "a live origin survives");
        assert!(union.contains(&operator_only), "a live operator survives");
        assert!(
            !union.contains(&lapsed),
            "an address in neither mapping (lapsed emergency origin) is dropped"
        );
        assert_eq!(union.len(), 2);
        Ok(())
    }

    /// (b) The load-bearing #1499 regression guard, ported from
    /// `operator_blacklist_log_reaches_the_same_deny_set` to the enumeration path:
    /// an OPERATOR-only entry (`isOperatorBlacklisted == true`,
    /// `isOriginBlacklisted == false`) STILL lands in the deny-set. Filtering by
    /// `isOriginBlacklisted` alone would drop it — a restarted node serving a
    /// governance-ejected operator.
    #[tokio::test]
    async fn operator_only_entry_survives_the_enumeration_liveness_filter() -> Result<()> {
        let op = addr(0xB0);
        let operator_only = addr(0xB1);
        let mut stub = StubReads::new(op);
        stub.addresses = vec![operator_only];
        // The whole point: NOT in the origin mapping.
        stub.origin_live = HashSet::new();
        stub.operator_live = [operator_only].into_iter().collect();

        // Precondition making the guard's teeth explicit: the origin predicate alone
        // returns false, so an `isOriginBlacklisted`-only filter would drop this.
        assert!(
            !stub
                .is_origin_blacklisted(operator_only, stub.block)
                .await?
        );
        assert!(
            stub.is_operator_blacklisted(operator_only, stub.block)
                .await?
        );

        let union = enumerate_address_union(&stub, op, stub.block).await?;

        assert!(
            union.contains(&operator_only),
            "the operator-only address MUST survive the union filter (#1499)"
        );
        Ok(())
    }

    /// (c) A `seen != count` mismatch ABORTS (`ensure!`) rather than seating a
    /// partial set — the swap-and-pop / pinned-block guard.
    #[tokio::test]
    async fn address_count_mismatch_aborts_rather_than_seating_a_partial_set() {
        let op = addr(0xC0);
        let mut stub = StubReads::new(op);
        stub.addresses = vec![addr(0xC1)];
        stub.origin_live = [addr(0xC1)].into_iter().collect();
        // Count claims two, only one is enumerable → a concurrent swap-and-pop skew.
        stub.address_count = Some(U256::from(2u8));

        let err = enumerate_address_union(&stub, op, stub.block)
            .await
            .expect_err("a seen != count skew must abort");
        assert!(
            format!("{err:#}").contains("read 1 of 2"),
            "abort must name the shortfall: {err:#}"
        );
    }

    /// The hash half has the same pinned-block count guard.
    #[tokio::test]
    async fn hash_count_mismatch_aborts() {
        let op = addr(0xD0);
        let mut stub = StubReads::new(op);
        stub.hashes_by_region = [(US, vec![b256(0xEE)])].into_iter().collect();
        stub.hash_count = [(US, U256::from(3u8))].into_iter().collect();

        let err = enumerate_region_hashes(&stub, US, stub.block)
            .await
            .expect_err("a seen != count skew must abort");
        assert!(format!("{err:#}").contains("read 1 of 3"), "{err:#}");
    }

    /// `ContractReads::block_number` must route through the shared, TTL-cached
    /// [`SharedHead`] single-flight rather than issue its own `eth_blockNumber` —
    /// two calls inside the TTL cost exactly one RPC. The unconsumed asserter
    /// queue is the proof: a second direct read would have popped a response
    /// that was never pushed.
    #[tokio::test]
    async fn contract_reads_block_number_routes_through_shared_head() -> Result<()> {
        use crate::chain_events::shared_head::SharedHead;
        use alloy::primitives::U64;
        use alloy::providers::ProviderBuilder;
        use alloy::providers::mock::Asserter;

        const TTL: Duration = Duration::from_secs(4);

        let asserter = Asserter::new();
        asserter.push_success(&U64::from(100));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let head: Arc<dyn HeadSource> = Arc::new(SharedHead::with_ttl(provider.clone(), TTL, None));

        let contract = ContentBlacklist::new(Address::ZERO, provider);
        let reads = ContractReads { contract, head };

        assert_eq!(reads.block_number().await?, 100);
        assert_eq!(
            reads.block_number().await?,
            100,
            "second call is TTL-cached via SharedHead"
        );
        assert_eq!(
            asserter.read_q().len(),
            0,
            "exactly one eth_blockNumber RPC was issued"
        );
        Ok(())
    }

    /// Boot enumeration builds the full `(region, hash)` deny-set from every
    /// in-scope region — the enumeration analogue of "a fresh process rebuilds the
    /// full deny-set".
    #[tokio::test]
    async fn boot_enumeration_builds_the_known_set_across_in_scope_regions() -> Result<()> {
        let op = addr(0xF0);
        let global = b256(0x00);
        let mut stub = StubReads::new(op);
        stub.scope_regions = vec![global, US];
        stub.hashes_by_region = [
            (global, vec![b256(0x11)]),
            (US, vec![b256(0x22), b256(0x33)]),
        ]
        .into_iter()
        .collect();

        let snapshot = bootstrap_snapshot(&stub, op).await?;

        assert_eq!(snapshot.block, stub.block);
        assert_eq!(snapshot.known.len(), 3);
        assert!(snapshot.known.contains(&(global, hash(0x11))));
        assert!(snapshot.known.contains(&(US, hash(0x22))));
        assert!(snapshot.known.contains(&(US, hash(0x33))));
        Ok(())
    }

    /// Paging reads more than one page and stops when it has `count` entries.
    #[tokio::test]
    async fn enumeration_pages_past_the_page_size() -> Result<()> {
        let op = addr(0x30);
        let mut stub = StubReads::new(op);
        // One-and-a-bit pages of DISTINCT addresses (a two-byte counter, so no
        // truncation and no repeats to dedup).
        let total = BLACKLIST_ENUM_PAGE_SIZE + 5;
        let addrs: Vec<Address> = (0..total)
            .map(|i| {
                let mut bytes = [0u8; 20];
                bytes[0..8].copy_from_slice(&i.to_be_bytes());
                Address::from(bytes)
            })
            .collect();
        stub.addresses = addrs.clone();
        stub.origin_live = addrs.iter().copied().collect();

        let union = enumerate_address_union(&stub, op, stub.block).await?;
        assert_eq!(union.len(), addrs.len(), "every distinct address survives");
        Ok(())
    }

    // ----- periodic re-enumeration fold (Ok arm of `on_tick_complete`) -----

    /// The correctness-critical asymmetry of the re-enumeration fold: `known` is
    /// UNIONED (an out-of-scope entry the tail learned and retained for a future
    /// ripening transition must survive an in-scope-only re-enumeration), while the
    /// chain origins are REPLACED WHOLESALE (the authoritative current set). A live
    /// pre-existing origin X must NOT survive a snapshot that no longer lists it.
    #[test]
    fn reenumeration_unions_known_and_replaces_origins() {
        let x = addr(0x11);
        let y = addr(0x22);
        let denylist = Arc::new(ContentDenylist::empty());
        // Seed the prior chain-origin set with X, as a previous enumeration would.
        denylist.set_chain_origins([x].into_iter().collect());
        let mut state = state_with_denylist(Arc::clone(&denylist));
        // An out-of-scope `(region, hash)` the tail retained for a future ripening.
        let retained = (b256(0xEE), hash(0xEE));
        state.known.insert(retained);

        let snapshot = BootstrapSnapshot {
            block: 100,
            origins: [y].into_iter().collect(),
            known: [(US, hash(0x33))].into_iter().collect(),
        };
        state.fold_reenumeration(snapshot);

        // `known` is UNIONed: the retained out-of-scope entry survives alongside the
        // freshly enumerated in-scope one.
        assert!(
            state.known.contains(&retained),
            "the retained out-of-scope entry MUST survive the union"
        );
        assert!(
            state.known.contains(&(US, hash(0x33))),
            "the newly enumerated in-scope entry is added"
        );
        assert_eq!(state.known.len(), 2);

        // Chain origins are REPLACED wholesale: Y is now denied, X is gone.
        assert!(denylist.is_origin_denied(&y), "the new origin Y is denied");
        assert!(
            !denylist.is_origin_denied(&x),
            "the prior origin X was replaced wholesale, not unioned"
        );
    }

    // ----- in-memory worklist behaviour -----

    /// Removing one region's entry must not drop a surviving same-hash entry in
    /// another region — otherwise a later `updateRegion` into the surviving region
    /// (which emits no blacklist event) would never lead to eviction.
    #[test]
    fn remove_entry_is_region_scoped() {
        let h = hash(0xAB);
        let mut state = state();
        state.add_entry(US, h);
        state.add_entry(FR, h);

        state.remove_entry(FR, h);

        assert!(!state.known.contains(&(FR, h)));
        assert!(state.known.contains(&(US, h)), "US entry must survive");
        assert_eq!(state.distinct_hashes(), vec![h]);
    }

    #[test]
    fn remove_entry_drops_last_entry_for_hash() {
        let h = hash(0xCD);
        let mut state = state();
        state.add_entry(US, h);

        state.remove_entry(US, h);

        assert!(state.known.is_empty());
        assert!(state.distinct_hashes().is_empty());
    }

    /// Local eviction is sticky and region-independent, so `drop_hash` clears every
    /// regional entry for the hash while leaving other hashes untouched.
    #[test]
    fn drop_hash_clears_all_regions_for_that_hash_only() {
        let evicted = hash(0xEE);
        let retained = hash(0x11);
        let mut state = state();
        state.add_entry(US, evicted);
        state.add_entry(FR, evicted);
        state.add_entry(FR, retained);

        state.drop_hash(evicted);

        assert!(!state.known.contains(&(US, evicted)));
        assert!(!state.known.contains(&(FR, evicted)));
        assert_eq!(state.distinct_hashes(), vec![retained]);
    }

    /// The scope view is per `(operator, hash)`, so re-scoping must issue one check
    /// per distinct hash even when several regional entries share it.
    #[test]
    fn distinct_hashes_dedupes_across_regions() {
        let h = hash(0x42);
        let mut state = state();
        state.add_entry(US, h);
        state.add_entry(FR, h);

        assert_eq!(state.distinct_hashes(), vec![h]);
    }

    // ----- enforcement over a mocked provider -----

    /// One ABI-encoded `bool` return word, as an `eth_call` result.
    fn abi_bool(value: bool) -> alloy::primitives::Bytes {
        let mut word = [0u8; 32];
        if value && let Some(last) = word.last_mut() {
            *last = 1;
        }
        alloy::primitives::Bytes::from(word.to_vec())
    }

    /// A sink whose `isHashBlacklistedForOperator` calls answer `scope_results` in
    /// order, so a test can drive the enforcing path. Its `reads` provider is a
    /// separate empty mock (the enforcement tests call `recheck`/`on_removed_log`
    /// directly and never touch the enumeration path).
    async fn enforcing_sink(
        scope_results: &[bool],
        metrics: &Arc<Metrics>,
    ) -> Result<BlacklistSink<alloy::providers::DynProvider>> {
        let asserter = alloy::providers::mock::Asserter::new();
        for in_scope in scope_results {
            asserter.push_success(&abi_bool(*in_scope));
        }
        let provider = alloy::providers::ProviderBuilder::new()
            .connect_mocked_client(asserter)
            .erased();
        let tmp = tempfile::tempdir()?;
        let cache = CacheEngine::open(tmp.path(), Vec::new(), 1).await?;
        Ok(BlacklistSink {
            contract: ContentBlacklist::new(Address::repeat_byte(0x11), provider),
            reads: ContractReads {
                contract: ContentBlacklist::new(Address::repeat_byte(0x11), mock_provider()),
                head: Arc::new(crate::chain_events::shared_head::SharedHead::with_ttl(
                    mock_provider(),
                    Duration::from_secs(1),
                    None,
                )),
            },
            operator: Address::repeat_byte(0x22),
            cache,
            state: state(),
            shutdown: CancellationToken::new(),
            rescan_interval: Duration::from_secs(1),
            last_rescan: None,
            metrics: Arc::clone(metrics),
        })
    }

    /// A sink with `entries` distinct known hashes over an empty asserter, so every
    /// enumeration read AND every re-scope re-check fails (`Recheck::Failed`).
    async fn failing_sink(
        entries: u8,
        metrics: &Arc<Metrics>,
    ) -> Result<BlacklistSink<alloy::providers::DynProvider>> {
        let provider = alloy::providers::ProviderBuilder::new()
            .connect_mocked_client(alloy::providers::mock::Asserter::new())
            .erased();
        let tmp = tempfile::tempdir()?;
        let cache = CacheEngine::open(tmp.path(), Vec::new(), 1).await?;
        let mut state = state();
        for n in 0..entries {
            state.add_entry(US, hash(n));
        }
        Ok(BlacklistSink {
            contract: ContentBlacklist::new(Address::repeat_byte(0x11), provider),
            reads: ContractReads {
                contract: ContentBlacklist::new(Address::repeat_byte(0x11), mock_provider()),
                head: Arc::new(crate::chain_events::shared_head::SharedHead::with_ttl(
                    mock_provider(),
                    Duration::from_secs(1),
                    None,
                )),
            },
            operator: Address::repeat_byte(0x22),
            cache,
            state,
            shutdown: CancellationToken::new(),
            rescan_interval: Duration::from_secs(1),
            last_rescan: None,
            metrics: Arc::clone(metrics),
        })
    }

    /// Enforcement is TWO writes: the deny (which selects the wire refusal code) and
    /// the eviction (which reclaims the bytes). Both must land.
    #[tokio::test]
    async fn enforcement_denies_and_evicts() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let mut sink = enforcing_sink(&[true], &metrics).await?;
        let h = hash(0x51);
        sink.state.add_entry(US, h);

        let outcome = recheck(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            h,
        )
        .await;

        assert!(outcome == Recheck::Evicted);
        assert!(
            sink.cache.is_chain_denied(h),
            "the live deny-set the delivery path reads must carry the reason"
        );
        assert!(
            sink.cache.is_evicted(h),
            "and the bytes still get reclaimed"
        );
        Ok(())
    }

    /// The upgrade / restart path. `evicted.log` records that a hash was evicted,
    /// never *why*, so a hash evicted by an older build (or a prior boot) would
    /// answer `EvictedSinceProbe` forever. The enumeration re-check back-fills the
    /// governance reason.
    #[tokio::test]
    async fn an_already_evicted_hash_is_back_filled_into_the_deny_set() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let mut sink = enforcing_sink(&[true], &metrics).await?;
        let h = hash(0x53);
        sink.cache.evict(h).await?;
        sink.state.add_entry(US, h);

        let outcome = recheck(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            h,
        )
        .await;

        assert!(
            outcome == Recheck::NoAction,
            "already evicted, nothing to evict"
        );
        assert!(
            sink.cache.is_chain_denied(h),
            "but the reason must still be recorded, or the wire code stays wrong"
        );
        Ok(())
    }

    /// ...and it costs nothing once recorded: a second pass must not re-spend a
    /// scope read on a hash already known to be governance-denied. (The sink is
    /// built with ONE queued response, so a second `eth_call` would error.)
    #[tokio::test]
    async fn back_fill_does_not_repeat_once_recorded() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let mut sink = enforcing_sink(&[true], &metrics).await?;
        let h = hash(0x54);
        sink.cache.evict(h).await?;

        for _ in 0..2 {
            sink.state.add_entry(US, h);
            let outcome = recheck(
                &sink.contract,
                sink.operator,
                &sink.cache,
                &mut sink.state,
                h,
            )
            .await;
            assert!(outcome == Recheck::NoAction);
        }
        Ok(())
    }

    /// A `HashRemoved` lifts the governance deny — but only on a definitive
    /// out-of-scope read, since the event is per-region and a same-hash entry under
    /// another region can still cover this operator. The hash stays evicted either
    /// way; only the refusal *code* moves.
    #[tokio::test]
    async fn hash_removal_lifts_the_deny_when_out_of_scope() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        // Two scope reads: one to enforce, one on the removal.
        let mut sink = enforcing_sink(&[true, false], &metrics).await?;
        let h = hash(0x55);
        sink.state.add_entry(US, h);
        let _ = recheck(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            h,
        )
        .await;
        assert!(sink.cache.is_chain_denied(h), "denied before the removal");

        on_removed_log(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            &removed_log(US, *h.as_bytes()),
        )
        .await;

        assert!(
            !sink.cache.is_chain_denied(h),
            "de-listed hashes stop being blacklist-coded"
        );
        assert!(
            sink.cache.is_evicted(h),
            "...but the eviction is sticky and one-way"
        );
        Ok(())
    }

    /// ...whereas a hash still in scope under another region keeps its deny.
    #[tokio::test]
    async fn hash_removal_keeps_the_deny_when_still_in_scope() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let mut sink = enforcing_sink(&[true, true], &metrics).await?;
        let h = hash(0x56);
        sink.state.add_entry(US, h);
        let _ = recheck(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            h,
        )
        .await;

        on_removed_log(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            &removed_log(FR, *h.as_bytes()),
        )
        .await;

        assert!(sink.cache.is_chain_denied(h));
        Ok(())
    }

    /// #1319: a re-scope that cannot enforce every entry must NOT bail (so the loop
    /// reads healthy), but MUST bump `blacklist_enforcement_failures_total`.
    #[tokio::test]
    async fn enforcement_failure_counts_without_bailing() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let mut sink = failing_sink(1, &metrics).await?;

        let result = sink.on_tick_complete().await;
        assert!(
            result.is_ok(),
            "an enforcement failure must not bail the tick: {result:?}"
        );

        let text = metrics.encode()?;
        assert!(
            text.lines()
                .any(|l| l == "decdn_blacklist_enforcement_failures_total 1"),
            "one unenforced entry must bump the enforcement counter:\n{text}"
        );
        assert!(
            text.lines()
                .any(|l| l == "decdn_blacklist_watcher_down_seconds 0"),
            "an enforcement failure is not a chain-read outage — down_seconds stays 0:\n{text}"
        );
        Ok(())
    }

    /// #1319, aggregate semantics: the counter bumps by the *count* of unenforced
    /// hashes in one pass (`inc_by`), not once.
    #[tokio::test]
    async fn enforcement_failure_counter_aggregates_per_pass() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let mut sink = failing_sink(2, &metrics).await?;

        sink.on_tick_complete().await?;

        let text = metrics.encode()?;
        assert!(
            text.lines()
                .any(|l| l == "decdn_blacklist_enforcement_failures_total 2"),
            "two unenforced entries in one pass must bump the counter by 2:\n{text}"
        );
        Ok(())
    }

    fn removed_log(region: B256, hash_bytes: [u8; 32]) -> Log {
        let event = HashRemoved {
            region,
            hash: B256::from(hash_bytes),
            version: alloy::primitives::U256::from(1u64),
        };
        Log {
            inner: alloy::primitives::Log {
                address: Address::repeat_byte(0x11),
                data: event.encode_log_data(),
            },
            ..Default::default()
        }
    }

    fn origin_log(origin: Address, blacklisted: bool) -> Log {
        let event = OriginBlacklistUpdated {
            origin,
            blacklisted,
        };
        Log {
            inner: alloy::primitives::Log {
                address: Address::repeat_byte(0x11),
                data: event.encode_log_data(),
            },
            ..Default::default()
        }
    }

    fn operator_log(operator: Address) -> Log {
        let event = OperatorBlacklisted { operator };
        Log {
            inner: alloy::primitives::Log {
                address: Address::repeat_byte(0x11),
                data: event.encode_log_data(),
            },
            ..Default::default()
        }
    }

    // ----- origin deny-set (ADR 011 § Hash Evasion) -----

    /// An `OriginBlacklistUpdated` log reaches the deny-set the delivery path reads.
    #[test]
    fn origin_log_reaches_the_deny_set() -> Result<()> {
        let deny = Arc::new(ContentDenylist::empty());
        let mut state = state_with_denylist(Arc::clone(&deny));
        let origin = Address::repeat_byte(0x44);

        on_origin_log(&mut state, &origin_log(origin, true))?;

        assert!(deny.is_origin_denied(&origin), "reaches the live deny-set");
        Ok(())
    }

    /// De-listing clears the deny-set entry.
    #[test]
    fn origin_delisting_clears_the_deny_set() -> Result<()> {
        let deny = Arc::new(ContentDenylist::empty());
        let mut state = state_with_denylist(Arc::clone(&deny));
        let origin = Address::repeat_byte(0x45);

        on_origin_log(&mut state, &origin_log(origin, true))?;
        on_origin_log(&mut state, &origin_log(origin, false))?;

        assert!(!deny.is_origin_denied(&origin));
        Ok(())
    }

    /// `addOperator` is the primary governance path — it emits `OperatorBlacklisted`,
    /// never `OriginBlacklistUpdated`, and writes a different on-chain mapping.
    /// Watching only the latter left the voted, ejecting path unenforced at the
    /// delivery gate — the tail twin of the #1499 enumeration guard.
    #[test]
    fn operator_blacklist_log_reaches_the_same_deny_set() -> Result<()> {
        let deny = Arc::new(ContentDenylist::empty());
        let mut state = state_with_denylist(Arc::clone(&deny));
        let operator = Address::repeat_byte(0x46);

        on_operator_log(&mut state, &operator_log(operator), true)?;

        assert!(deny.is_origin_denied(&operator));
        Ok(())
    }

    /// An undecodable origin log must NOT be skipped: the tick aborts rather than
    /// advancing the cursor past a takedown the node could not read.
    #[test]
    fn undecodable_origin_log_aborts_the_tick() {
        let mut state = state();
        let mut log = origin_log(Address::repeat_byte(0x48), true);
        log.inner.data.data = vec![0x01].into();

        let Err(err) = on_origin_log(&mut state, &log) else {
            panic!("an unreadable takedown event must not be skipped");
        };
        assert!(
            format!("{err:#}").contains("refusing to advance"),
            "{err:#}"
        );
    }
}
