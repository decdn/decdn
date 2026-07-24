//! Blacklist compliance watcher (ADR 011 § Content Takedown, ADR 031).
//!
//! Every paid-delivery node runs this task after resolving the mandatory
//! `blockchain.content_blacklist_address`. It keeps the local blob store
//! compliant with `ContentBlacklist`: it evicts
//! any blob whose hash is blacklisted *in scope* for this operator (global ∪
//! current-region ∪ ripening-prev-region). The scope decision is the contract's
//! `isHashBlacklistedForOperator` view, so region packing and the ADR 030
//! ripening math never leave the chain.
//!
//! **Event-sourced, re-scoped deny-set.** The set of blacklisted entries is
//! learned from `HashBlacklisted` logs scanned by the shared
//! `resumable_watcher` `eth_getLogs` poller (#1092/#1106 — no `eth_newFilter`):
//! the first tick's window *is* the historical replay, later ticks are the live
//! tail. `ContentBlacklist` exposes no enumeration view, so events are the only
//! source. Entries are keyed by `(region, hash)` — the contract's own key
//! (`_hashEntries[region][hash]`) — so a `HashRemoved` for one region's entry
//! never drops a surviving same-hash entry in another region. Every
//! seen-but-not-yet-evicted entry is retained in `known` — *including* ones
//! currently out of scope (wrong region) or fast-track suspended — and
//! re-scoped on a periodic (`on_tick_complete`) pass. This is essential: a hash
//! can become live + in scope with **no** `HashBlacklisted` event — an operator
//! region/ripening change (`CapacityBond.updateRegion`) or an appeal
//! reversal/lapse that clears `suspended`. Re-scoping `known` is what catches
//! those.
//!
//! The appeal half of that also has a prompt path: `HashSuspensionUpdated`
//! (#1300) is emitted on every suspend/resume, so a resume re-checks scope
//! immediately instead of waiting out the rescan cadence with the hash servable
//! and slashable. The periodic re-scope remains the backstop — it is still the
//! only thing that catches a region/ripening transition, which emits no event at
//! all — so the two are complementary, not redundant.
//!
//! **Durable deny-set, resumable cursor (#1181).** `known` is mirrored to a
//! durable [`BlacklistEntryStore`] and reloaded on boot, so the scan cursor is
//! persisted (`CheckpointKey::Blacklist`) rather than replayed from the deploy
//! block every start.
//!
//! That ordering is the whole safety argument, because the contract exposes no
//! enumeration view. This watcher deliberately retains entries that are out of
//! scope (wrong region) or appeal-suspended, since either can become enforceable
//! again with no new `HashBlacklisted` log. While that set lived only in memory a
//! resume would skip past those logs and silently drop the entries from
//! re-scoping — a slashable compliance gap, and the reason the watcher used to
//! full-replay. Persisting the set removes the premise.
//!
//! Two invariants keep it sound, both pinned by tests:
//! 1. **The cursor never leads the deny-set.** Entry writes commit durably
//!    *before* the in-memory set is touched, and a failed write aborts the tick
//!    (`handle_log` returns `Err`) so the shared loop cannot persist a cursor
//!    past a log whose entry was lost. A *lagging* cursor is harmless: every
//!    operation here is idempotent, so a wider rescan only costs RPC.
//! 2. **A cold store still replays everything.** First boot has no cursor and no
//!    persisted entries, so it scans from `from_block` (`ColdStart::FromBlock`)
//!    — anchoring at head would miss every pre-existing blacklist entry.
//!
//! This also retires the #1108 crash-loop exposure at its source: a rate-limited
//! boot no longer re-scans the whole chain. The preflight's bounded 429/5xx retry
//! (`check_rpc_reachability`) remains the backstop for a persistently throttled
//! endpoint.
//!
//! Enforcement is two writes per hash, in this order: the *deny* records that
//! the refusal is governance-sourced, then the *eviction* reclaims the bytes.
//!
//! [`decdn_cache::CacheEngine::evict`] durably records the takedown (survives
//! restart via `evicted.log`) and cascades to every serving surface through
//! [`decdn_cache::CacheEngine::refuses`]: the DHT republisher drops the hash on
//! its next tick, the probe handler stops signing `has_blob: true`, and the
//! client handler never re-pull-fills it. `evict` is sticky and works on absent
//! hashes, so a hash blacklisted while the node was offline (and not yet held)
//! is still pre-blocked.
//!
//! The deny is what `evicted.log` cannot express: *why*. Without it the client
//! handler answers a governance takedown with `EvictedSinceProbe` and a local
//! `[content] denied_hashes` entry with `HashBlacklisted`, so one request tells a
//! client which list a hash is on — the probe ADR 011 §`StreamRequest` Response
//! forecloses, since only the local list is private. Both now answer
//! `HashBlacklisted`. It has its own durable projection because neither of the
//! other two records survives a restart usefully: `evicted.log` carries no cause,
//! and `known` drops a hash the moment it is evicted.
//!
//! **Origin blacklisting (ADR 011 § Hash Evasion and Origin Blacklisting).**
//! `OriginBlacklistUpdated` rides the same scan and feeds
//! [`crate::content_deny::ContentDenylist`], which the delivery path consults to
//! refuse any `StreamRequest` funded by a blacklisted operator address. This
//! half has *weaker* primitives than the hash half and the difference matters:
//! the event carries no `version`, so there is no counter that would reveal a
//! missed one, and `ContentBlacklist` exposes no enumeration of the blacklisted
//! set, so there is no sweep to reconcile against either. The durable origin
//! projection is therefore the entire guarantee — with the cursor persisted, a
//! resumed boot that could not reload it would come up with an EMPTY deny-set
//! and serve a blacklisted origin. Hence the same durable-write-before-cursor
//! ordering, and the same replay-from-floor fallback on an unreadable store.
//!
//! On a blacklisting the operator's registered `NodeId` is also dropped from the
//! local peer table AND barred from re-announcing via the shared announce-gate
//! deny-set (`AnnounceOriginDenySet`); on a clear it is un-barred. Both need a
//! `nodeIdOf` read, so they are best-effort and retried through
//! `WatcherState::pending_origin_peer_ops` on failure. The peer-table removal is
//! *advisory* on its own (`insert_or_refresh` re-admits on the next announce);
//! the deny-set is what reliably keeps a blacklisted origin out of the announce
//! gate for the lifetime of the blacklist. The deny-set is in-memory, not
//! persisted — it is reconstructed on boot by seeding
//! `pending_origin_peer_ops` from the durable origin projection. See
//! `WatcherState::apply_origin_peer`.
//!
//! **Resilience.** The re-scope pass runs on the operator's rescan cadence
//! (checked at the end of every poll tick), pulled forward to the poll cadence
//! whenever a live re-check or a re-scope pass fails — an enforcement failure
//! must not wait out the full cadence while the blob stays slashably servable;
//! every RPC read is bounded by a per-call timeout; the re-scope is interruptible
//! by shutdown (checked between hashes) so a large backlog cannot overrun the
//! runtime shutdown deadline. Serving a blacklisted hash is slashable
//! (`SlashJudge.submitBlacklistChallenge`), so prompt eviction is the node's only
//! local protection.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::{Context as _, Result};
use decdn_cache::{CacheEngine, Hash};
use decdn_common::redact::sanitize_err_chain;
use decdn_gossip::PeerTable;

use crate::announce_gate::AnnounceOriginDenySet;
use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::content_blacklist::ContentBlacklist;
use decdn_incentive::content_blacklist::ContentBlacklist::{
    HashBlacklisted, HashRemoved, HashSuspensionUpdated, OperatorBlacklistCleared,
    OperatorBlacklisted, OriginBlacklistUpdated,
};
use decdn_incentive::store::{BlacklistEntryStore, CheckpointKey, KeyedCheckpointStore};
use tokio::sync::{RwLock, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain_events::resumable_watcher::{
    self, Checkpoint, ColdStart, CursorStart, LogSink, WatcherConfig, WatcherHandle,
};
use crate::chain_events::shared_head::HeadSource;
use crate::chain_events::{REORG_MARGIN_BLOCKS, timed};
use crate::content_deny::ContentDenylist;
use crate::metrics::{Metrics, metric_hook};

/// Result reported exactly once when the first full replay + re-scope pass
/// either establishes compliance or proves startup cannot safely continue.
pub(crate) type InitialSyncResult = std::result::Result<(), String>;

#[derive(Clone)]
struct InitialSyncGate(Arc<Mutex<Option<oneshot::Sender<InitialSyncResult>>>>);

impl InitialSyncGate {
    fn new(sender: oneshot::Sender<InitialSyncResult>) -> Self {
        Self(Arc::new(Mutex::new(Some(sender))))
    }

    fn pending(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
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

/// Mutable deny-set carried across poll ticks. The scan cursor lives on the
/// resumable watcher; this holds only the re-scopable entry set.
struct WatcherState<P: Provider + Clone> {
    /// Every blacklisted `(region, hash)` entry seen and not yet locally
    /// evicted — including out-of-scope and suspended entries — re-scoped on
    /// each `on_tick_complete` pass so a later region/ripening or appeal
    /// transition (which emits no `HashBlacklisted`) still leads to eviction.
    /// Keyed like the contract's `_hashEntries[region][hash]` so a `HashRemoved`
    /// drops exactly the removed entry.
    ///
    /// Mirrors [`Self::store`]; loaded from it on boot.
    known: HashSet<(B256, Hash)>,
    /// Durable mirror of `known`, which is what makes the persisted scan cursor
    /// safe: a resume no longer forgets the still-out-of-scope entries this set
    /// deliberately retains. Writes go here **before** the in-memory set, and a
    /// failure propagates so the caller refuses to advance the cursor past the
    /// log that produced it.
    store: Arc<dyn BlacklistEntryStore>,
    /// The live origin deny-set the delivery path reads (ADR 011 §On Blacklist
    /// Event). Fed by `OriginBlacklistUpdated`, and restored from `store` on
    /// boot — that restore is load-bearing, see
    /// [`decdn_incentive::store::BlacklistEntryStore::load_blacklist_origins`]:
    /// origin events carry no version, nothing enumerates the set on-chain, and
    /// the scan cursor is persisted, so the durable projection is the only thing
    /// standing between a restart and a silently empty gate.
    denylist: Arc<ContentDenylist>,
    /// `CapacityBond`, for resolving a blacklisted origin address to its
    /// registered `NodeId` (ADR 011 § Hash Evasion and Origin Blacklisting: "the
    /// operator's registered `NodeId` is excluded from peer tables"). The event
    /// carries an address; the peer table is keyed by `NodeId`, and this contract
    /// read is the only binding between them.
    capacity_bond: CapacityBond::CapacityBondInstance<P>,
    /// Local peer table, for that removal.
    peer_table: Arc<RwLock<PeerTable>>,
    /// `NodeAnnounce` origin deny-set, shared with the admission gate (#1398).
    /// Fed the resolved `NodeId` of every origin-blacklisted operator so a
    /// blacklisted peer barred from the table above cannot walk straight back in
    /// on its next announce (the origin-only path does not eject, so the staker
    /// set still admits it). Keyed by `NodeId`; the `Address → NodeId` step is
    /// [`Self::apply_origin_peer`]'s `nodeIdOf` read.
    announce_deny: Arc<AnnounceOriginDenySet>,
    /// Origin peer-table drops / announce-gate bars whose `nodeIdOf` lookup
    /// failed, keyed by operator address → its blacklisted state, retried every
    /// [`BlacklistSink::on_tick_complete`]. Without this the `Recheck::Failed`
    /// signal was inert: clearing `last_rescan` only re-runs the hash re-scope,
    /// which has no origin leg, so a transient blip left the peer un-barred (add)
    /// or permanently barred (clear). The boot rebuild seeds this from the
    /// restored durable origin projection so the derived `NodeId` deny-set is
    /// reconstructed on the first tick.
    pending_origin_peer_ops: HashMap<Address, bool>,
}

impl<P: Provider + Clone> WatcherState<P> {
    /// Record a `HashBlacklisted(region, hash)` entry.
    ///
    /// Durable first: if the write fails the entry is *not* added to `known`,
    /// the tick aborts, and the log is re-read next tick. Losing an add is the
    /// compliance-critical direction — an entry the node never re-learns is a
    /// hash it may serve and be slashed for.
    fn add_entry(&mut self, region: B256, hash: Hash) -> Result<()> {
        self.store
            .insert_blacklist_entry(region.0, *hash.as_bytes())
            .context("persist blacklist deny-set entry")?;
        self.known.insert((region, hash));
        Ok(())
    }

    /// Drop exactly the `HashRemoved(region, hash)` entry — same-hash entries
    /// under other regions stay retained for re-scoping.
    fn remove_entry(&mut self, region: B256, hash: Hash) -> Result<()> {
        self.store
            .remove_blacklist_entry(region.0, *hash.as_bytes())
            .context("delete blacklist deny-set entry")?;
        self.known.remove(&(region, hash));
        Ok(())
    }

    /// Record that `hash` is refused because *governance* blacklisted it, and
    /// publish that to the live deny-set the delivery path reads.
    ///
    /// Durable first, exactly as [`Self::add_entry`] — and for a sharper reason
    /// than the worklist has. This projection is what a restart reloads to know
    /// a refusal is governance-sourced; `evicted.log` records the eviction but
    /// not its cause, and [`Self::known`] is emptied for the hash the moment it
    /// is evicted. Lose this write and the takedown silently starts answering
    /// `EvictedSinceProbe` after the next restart while local denylist entries
    /// keep answering `HashBlacklisted`, which is the fingerprint ADR 011
    /// §`StreamRequest` Response forecloses.
    ///
    /// An `Err` does not abort the tick: [`Self::add_entry`] has already durably
    /// recorded the entry in `known`, so the next re-scope retries this. That
    /// only holds because the caller refuses to evict past a failed deny — see
    /// `recheck`.
    fn deny_hash(&self, cache: &CacheEngine, hash: Hash) -> Result<()> {
        self.store
            .insert_blacklist_denied_hash(*hash.as_bytes())
            .context("persist governance-denied hash")?;
        cache.set_chain_denied_one(hash, true);
        Ok(())
    }

    /// Stop treating `hash` as governance-denied: governance removed the last
    /// entry that covered this operator.
    ///
    /// The hash stays *refused* — eviction is sticky and one-way — so this only
    /// moves its wire code from `HashBlacklisted` back to `EvictedSinceProbe`,
    /// which is what any other evicted hash answers. It leaks nothing: a hash no
    /// longer on the public blacklist is indistinguishable from one this
    /// operator evicted for corruption, which is the point.
    ///
    /// Durable first, like every write here. Unlike [`Self::deny_hash`] a
    /// failure is not tick-aborting: over-denying costs nothing but a wire code
    /// on a hash that is refused either way, so the row is left standing and
    /// retried by the next removal that arrives.
    fn undeny_hash(&self, cache: &CacheEngine, hash: Hash) {
        if let Err(err) = self.store.remove_blacklist_denied_hash(*hash.as_bytes()) {
            warn!(
                err = %sanitize_err_chain(&err.into()),
                %hash,
                "blacklist watcher: could not drop a de-listed hash from the durable \
                 governance deny-set; it stays refused, so this is a stale refusal code, \
                 not an enforcement gap"
            );
            return;
        }
        cache.set_chain_denied_one(hash, false);
    }

    /// Drop every entry for `hash` (once locally evicted, the sticky eviction
    /// covers all regions).
    ///
    /// Unlike the two above this never fails the tick: the hash has already been
    /// evicted locally (durably, via `evicted.log`), so a failed delete only
    /// leaves a row that costs one redundant scope re-check next pass. It is
    /// cleanup, not enforcement.
    fn drop_hash(&mut self, hash: Hash) {
        if let Err(err) = self.store.remove_blacklist_hash(*hash.as_bytes()) {
            warn!(
                err = %sanitize_err_chain(&err.into()),
                %hash,
                "blacklist watcher: could not drop evicted hash from the durable deny-set; \
                 it will be re-checked next pass (already evicted, so not an enforcement gap)"
            );
        }
        self.known.retain(|(_, known_hash)| *known_hash != hash);
    }

    /// Apply an origin blacklist state change to the local peer table and the
    /// shared `NodeAnnounce` deny-set (ADR 011 § Hash Evasion and Origin
    /// Blacklisting): on a blacklisting drop the operator's registered `NodeId`
    /// from the peer table AND bar it from re-announcing; on a clear, un-bar it.
    ///
    /// **The deny-set is what actually closes the re-entry gap.** Dropping the
    /// peer-table entry alone only buys one gossip interval —
    /// `PeerTable::insert_or_refresh` re-admits on the next announce. For the
    /// `addOperator` path that is already handled: operator blacklisting ejects
    /// from `CapacityBond`, so the announce gate's staked-node set stops
    /// recognising it. The origin-only path (`setOriginBlacklist` /
    /// `emergencyAddOrigin`) does NOT eject, so without the deny-set the peer
    /// re-enters; feeding [`Self::announce_deny`] here is what keeps it out.
    ///
    /// Best-effort on the `nodeIdOf` read: on an RPC error the address is queued
    /// in [`Self::pending_origin_peer_ops`] and [`Recheck::Failed`] is returned,
    /// so the next `on_tick_complete` retries it — a transient blip must not
    /// leave a blacklisted operator un-barred (add) or a de-listed one barred
    /// forever (clear). An operator with no registered node (`nodeId == 0`) is a
    /// settled no-op, not a failure: an address can be blacklisted before it ever
    /// registers, and the serving gate refuses it by address regardless.
    async fn apply_origin_peer(&mut self, origin: Address, blacklisted: bool) -> Recheck {
        let node_id =
            match timed(None, "nodeIdOf", self.capacity_bond.nodeIdOf(origin).call()).await {
                Ok(resolved) => resolved.nodeId,
                Err(err) => {
                    warn!(
                        %origin,
                        blacklisted,
                        err = %sanitize_err_chain(&err),
                        "blacklist watcher: nodeIdOf failed for an origin blacklist change; \
                         peer-table/announce-gate update deferred for retry"
                    );
                    self.pending_origin_peer_ops.insert(origin, blacklisted);
                    return Recheck::Failed;
                }
            };
        // Resolved (including to ZERO): the op is settled, so it no longer needs
        // a retry pass.
        self.pending_origin_peer_ops.remove(&origin);
        if node_id == B256::ZERO {
            // No registered node to bar. Known bounded limitation (pre-existing,
            // shared with the old `drop_origin_peer`): an address blacklisted
            // BEFORE it registers resolves to ZERO here and is dropped from the
            // retry queue, and no later `OriginBlacklistUpdated` fires to re-trigger
            // — so if it registers and announces afterward it is announce-selectable
            // until the next restart re-seeds the rebuild. It cannot transact,
            // though: the serving/pull gate refuses it by ADDRESS
            // (`ContentDenylist::is_origin_denied`) independent of NodeId. Closing
            // it would need an `addOperator`-time cross-check against the origin
            // deny-set (a gossip-crate feature), out of scope here.
            return Recheck::NoAction;
        }
        self.apply_resolved_origin(origin, node_id, blacklisted)
            .await;
        Recheck::NoAction
    }

    /// Apply a RESOLVED (non-`ZERO`) origin blacklist change to the peer table and
    /// the shared announce-gate deny-set: bar on a blacklisting, un-bar on a clear.
    /// `&self` — it touches only the shared `Arc` handles, not the pending map.
    async fn apply_resolved_origin(&self, origin: Address, node_id: B256, blacklisted: bool) {
        // The `insert`/`remove` "changed" return gates the log so a no-op replay
        // (the watcher re-reads block ranges after a restart) stays quiet.
        if blacklisted {
            let newly_barred = self.announce_deny.insert(node_id.0);
            let dropped = self.peer_table.write().await.remove(&node_id.0);
            if newly_barred || dropped {
                info!(%origin, %node_id, newly_barred, dropped, "blacklist watcher: barred a blacklisted origin from the announce gate / peer table");
            }
        } else if self.announce_deny.remove(&node_id.0) {
            info!(%origin, %node_id, "blacklist watcher: un-barred a de-listed origin from the announce gate");
        }
    }

    /// Distinct hashes across all regions — the scope view
    /// (`isHashBlacklistedForOperator`) is per `(operator, hash)`, so each
    /// hash needs exactly one `eth_call` per pass regardless of how many
    /// regional entries reference it.
    fn distinct_hashes(&self) -> Vec<Hash> {
        let unique: HashSet<Hash> = self.known.iter().map(|(_, hash)| *hash).collect();
        unique.into_iter().collect()
    }

    /// Apply an `OriginBlacklistUpdated(origin, blacklisted)` event.
    ///
    /// In-memory first, then durable. Applying the live deny-set update before
    /// the store write is what keeps the compliance gate fail-*closed*: an `Err`
    /// from the store still aborts the tick, so the cursor stays put and the log
    /// is re-read (the write retried next pass), while an unpersisted change is
    /// simply re-derived from the event tail on restart — the cursor never
    /// advanced past it. The previous order (persist, then apply) let a store
    /// error early-return through `?` and skip the in-memory apply entirely,
    /// leaving the node serving a just-blacklisted origin until a later tick
    /// happened to succeed — a fail-open on a compliance-critical path.
    ///
    /// The removal direction applies-first too: a failed delete un-denies the
    /// origin in memory now (service restored, matching the on-chain de-listing)
    /// and re-denies it on restart if the delete never persisted — the
    /// conservative direction. Over-denying a de-listed origin is a service
    /// complaint; under-denying a listed one is a compliance failure.
    fn set_origin(&mut self, origin: Address, blacklisted: bool) -> Result<()> {
        self.denylist.apply_chain_origin(origin, blacklisted);
        if blacklisted {
            self.store
                .insert_blacklist_origin(origin.into())
                .context("persist blacklisted origin")?;
        } else {
            self.store
                .remove_blacklist_origin(origin.into())
                .context("delete blacklisted origin")?;
        }
        Ok(())
    }
}

/// Applies `HashBlacklisted`/`HashRemoved` logs to the deny-set and enforces
/// compliance (#1092). `apply` records each entry and, for a `HashBlacklisted`,
/// immediately re-checks scope + evicts (prompt live enforcement); it never
/// returns `Err` — a scope-check RPC failure keeps the entry in `known` and
/// pulls the batched re-scope forward to the poll cadence until the re-check
/// succeeds, and an undecodable log is logged and skipped.
/// [`Self::on_tick_complete`] runs the batched re-scope (normally bounded to
/// the operator's rescan cadence) that catches no-event scope transitions.
struct BlacklistSink<P: Provider + Clone> {
    contract: ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: CacheEngine,
    state: WatcherState<P>,
    shutdown: CancellationToken,
    /// How often the batched full re-scope runs (the operator's
    /// `content_blacklist_poll_interval_sec`); the getLogs poll cadence itself is
    /// faster so live entries enforce promptly.
    rescan_interval: Duration,
    /// When the last batched re-scope ran; `None` forces one on the first tick.
    last_rescan: Option<Instant>,
    /// Still pending only during the mandatory first full replay + re-scope.
    initial_sync: InitialSyncGate,
    /// Node metrics — feeds `blacklist_enforcement_failures` from a re-scope that
    /// could not enforce every entry (#1319).
    metrics: Arc<Metrics>,
}

impl<P: Provider + Clone> BlacklistSink<P> {
    /// Retry deferred origin peer-table drops / announce-gate bars (and run the
    /// boot rebuild seed). [`WatcherState::apply_origin_peer`] clears an entry
    /// from the pending map on a successful `nodeIdOf`, or re-inserts it on
    /// another failure, so the set drains as the RPC recovers.
    async fn drain_pending_origin_peer_ops(&mut self) {
        if self.state.pending_origin_peer_ops.is_empty() {
            return;
        }
        let pending: Vec<(Address, bool)> = self
            .state
            .pending_origin_peer_ops
            .iter()
            .map(|(addr, blacklisted)| (*addr, *blacklisted))
            .collect();
        for (origin, blacklisted) in pending {
            let _ = self.state.apply_origin_peer(origin, blacklisted).await;
        }
    }
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
        // A live `HashBlacklisted` whose scope-check or eviction failed must
        // retry at the poll cadence (seconds), not the operator's re-scope
        // cadence (default 10 min) — serving the blob meanwhile is slashable.
        // Clearing `last_rescan` forces the batched re-scope on this tick's
        // `on_tick_complete`, which re-checks the retained entry.
        if failed {
            self.last_rescan = None;
        }
        Ok(())
    }

    async fn on_tick_complete(&mut self) -> Result<()> {
        // Retry origin peer-table drops / announce-gate bars whose `nodeIdOf`
        // lookup failed on the live event (or were seeded by the boot rebuild).
        // Runs every tick — not gated on the rescan cadence — because a failed
        // origin op has no hash leg for the batched re-scope to catch. On a fresh
        // node this also does the first-pass reconstruction of the derived
        // `NodeId` announce deny-set from the restored durable origin projection.
        self.drain_pending_origin_peer_ops().await;

        // Re-scope the whole deny-set on the operator's cadence (not every poll
        // tick): catches a region/ripening/appeal transition that emits no event.
        // An apply-time enforcement failure clears `last_rescan` (see `apply`),
        // pulling the next pass forward to the poll cadence.
        let due = self
            .last_rescan
            .is_none_or(|at| at.elapsed() >= self.rescan_interval);
        if due {
            let RescanOutcome { clean, failed } = rescan(
                &self.contract,
                self.operator,
                &self.cache,
                &mut self.state,
                &self.shutdown,
            )
            .await;
            // Surface the slashable "deny-set not fully enforced" condition even
            // after the initial-sync gate has fired, when a failed re-scope
            // otherwise returns `Ok(())` and every downtime metric reads healthy
            // (#1319). Separate from the down-family, which tracks chain-read
            // outages only.
            if failed > 0 {
                self.metrics.blacklist_enforcement_failure(failed);
            }
            // A pass with any failed re-check retries at the poll cadence until
            // it comes back clean; only a clean pass waits out the full
            // operator cadence again.
            self.last_rescan = clean.then(Instant::now);
            // An unenforceable initial re-scope bails so the shared loop's
            // backoff hook fires `Err` into the readiness gate — fail-CLOSED, so
            // the router never opens on an un-vetted deny-set. The shutdown case
            // is handled in `resumable_watcher::run`, which suppresses the
            // backoff EDGE (and the established edge) once the token is cancelled,
            // so an orderly stop mid-sync stamps no false drift window (#1321) and
            // signals no false readiness — no shutdown check is needed here.
            if !clean && self.initial_sync.pending() {
                anyhow::bail!(
                    "initial ContentBlacklist replay/re-scope could not enforce every entry"
                );
            }
        }
        Ok(())
    }
}

/// The durable deny-set projection reloaded at bring-up, plus whether it can be
/// trusted enough to resume from the persisted scan cursor.
struct RestoredProjection {
    known: HashSet<(B256, Hash)>,
    origins: Vec<[u8; 20]>,
    /// Hashes refused under a governance entry, restored so their wire refusal
    /// code survives the restart (ADR 011 §`StreamRequest` Response).
    denied_hashes: Vec<[u8; 32]>,
    /// `true` ⇒ ignore the cursor and rescan from the deploy block. Set whenever
    /// a projection is unreadable OR was never built, because resuming on a
    /// projection that does not reflect everything below the cursor is a silent
    /// fail-open on a takedown gate.
    replay_floor: bool,
}

/// Unwrap one membership projection, translating its two bad cases into the
/// replay obligation they carry.
///
/// Both cases set `replay_floor`, for the same reason and with the same
/// consequence if they did not. These projections are NEWER than the persisted
/// scan cursor, so a never-built (`None`) or unreadable one cannot be resumed
/// past: the cursor would start beyond every event that would have populated it,
/// the events carry no version to reveal the gap, and nothing enumerates the set
/// on-chain to sweep against. The gate would come up empty, permanently, with no
/// way to notice. `Some(vec![])` is the opposite and must NOT replay — it means
/// the projection is built and genuinely empty, and treating that as a gap would
/// rescan the whole chain on every boot.
fn restore_membership<T>(
    loaded: Result<Option<Vec<T>>, decdn_incentive::store::StoreError>,
    label: &str,
    replay_floor: &mut bool,
) -> Vec<T> {
    match loaded {
        Ok(Some(rows)) => rows,
        Ok(None) => {
            warn!(
                projection = label,
                "blacklist watcher: projection has never been built (new projection on an \
                 existing scan cursor); replaying from the deploy block to populate it"
            );
            *replay_floor = true;
            Vec::new()
        }
        Err(err) => {
            warn!(
                projection = label,
                err = %sanitize_err_chain(&err.into()),
                "blacklist watcher: durable projection unreadable; replaying from the deploy \
                 block rather than resuming on a partial set"
            );
            *replay_floor = true;
            Vec::new()
        }
    }
}

/// Reload every durable projection, deciding whether the persisted cursor is
/// still safe to resume from.
fn restore_projection(entry_store: &dyn BlacklistEntryStore) -> RestoredProjection {
    // A read failure is not fatal: an empty set plus the resumed cursor would
    // under-enforce, so fall back to replaying from `from_block`, which
    // reconstructs the set from events exactly as the pre-#1181 watcher did.
    let (entries, mut replay_floor) = match entry_store.load_blacklist_entries() {
        Ok(rows) => (rows, false),
        Err(err) => {
            warn!(
                err = %sanitize_err_chain(&err.into()),
                "blacklist watcher: durable deny-set unreadable; replaying from the deploy \
                 block to rebuild it rather than resuming on a partial set"
            );
            (Vec::new(), true)
        }
    };

    let origins = restore_membership(
        entry_store.load_blacklist_origins(),
        "origin deny-set",
        &mut replay_floor,
    );
    let denied_hashes = restore_membership(
        entry_store.load_blacklist_denied_hashes(),
        "governance hash deny-set",
        &mut replay_floor,
    );

    RestoredProjection {
        known: entries
            .into_iter()
            .map(|(region, hash)| (B256::from(region), Hash::from_bytes(hash)))
            .collect(),
        origins,
        denied_hashes,
        replay_floor,
    }
}

/// The blacklist watcher's cursor policy: **resume from the durable
/// [`CheckpointKey::Blacklist`] cursor, replaying from the deploy floor on a
/// cold store** ([`ColdStart::FromBlock`]).
///
/// This is only safe because the deny-set projection is itself durable
/// ([`BlacklistEntryStore`]). Before it was, a resume skipped logs whose (still
/// out-of-scope, so un-evicted) entries lived only in the in-memory `known`,
/// silently dropping them from re-scoping — a slashable compliance gap, which is
/// why this watcher previously full-replayed every boot. The durable set removes
/// the premise: a resumed boot reloads those entries from disk.
///
/// Two properties keep it safe, both pinned by tests:
/// - **Cold start replays everything.** A first-ever boot has no cursor *and* no
///   persisted entries, so it must scan from `from_block`; anchoring at head
///   (the settlement watcher's [`ColdStart::Head`]) would miss every pre-existing
///   blacklist entry.
/// - **The cursor never leads the deny-set.** `WatcherState`'s writes are durable
///   before the in-memory set is touched, and a failed write aborts the tick, so
///   the shared loop cannot persist a cursor past a log whose entry was lost. A
///   *lagging* cursor is harmless — the sinks are idempotent, so a wider rescan
///   only costs RPC.
fn cursor_start(store: Arc<dyn KeyedCheckpointStore>) -> CursorStart {
    CursorStart::FromCheckpoint {
        checkpoint: Checkpoint {
            store,
            key: CheckpointKey::Blacklist,
        },
        reorg_margin: REORG_MARGIN_BLOCKS,
        cold_start: ColdStart::FromBlock,
    }
}

/// Spawn the blacklist compliance watcher, returning the handle that owns its
/// task and shutdown token. `from_block` is the `ContentBlacklist` deploy block:
/// the floor a cold store replays from, and the lower clamp on a resumed cursor
/// (see the module header). `entry_store` is the durable deny-set — it MUST be
/// the unbuffered store, not a debouncing decorator, since the cursor's safety
/// rests on those writes being committed before it advances. `checkpoint_store`
/// carries the scan cursor and MAY be debounced (a lagging cursor only widens the
/// next rescan). `event_poll_interval` is the getLogs poll cadence;
/// `rescan_interval` is the batched re-scope cadence.
/// `initial_sync_tx` fires once the mandatory first full replay + re-scope
/// either completes cleanly (`Ok`) or the loop falls into backoff (`Err`), so
/// the runtime can gate the ALPN router on blacklist enforcement being live.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn<P>(
    provider: P,
    contract_addr: Address,
    operator: Address,
    cache: CacheEngine,
    from_block: u64,
    event_poll_interval: Duration,
    head: Arc<dyn HeadSource>,
    rescan_interval: Duration,
    initial_sync_tx: oneshot::Sender<InitialSyncResult>,
    metrics: &Arc<Metrics>,
    entry_store: Arc<dyn BlacklistEntryStore>,
    checkpoint_store: Arc<dyn KeyedCheckpointStore>,
    denylist: Arc<ContentDenylist>,
    capacity_bond_addr: Address,
    peer_table: Arc<RwLock<PeerTable>>,
    announce_deny: Arc<AnnounceOriginDenySet>,
) -> WatcherHandle
where
    P: Provider + Clone + 'static,
{
    let contract = ContentBlacklist::new(contract_addr, provider.clone());
    // Only used to resolve a blacklisted origin address to its registered
    // NodeId for peer-table removal — this watcher reads no other bond state.
    let capacity_bond = CapacityBond::new(capacity_bond_addr, provider.clone());
    let RestoredProjection {
        known,
        origins: restored_origins,
        denied_hashes: restored_denied_hashes,
        replay_floor,
    } = restore_projection(entry_store.as_ref());
    let restored_origin_count = restored_origins.len();
    let restored_denied_count = restored_denied_hashes.len();
    let restored_origin_addrs: Vec<Address> =
        restored_origins.into_iter().map(Address::from).collect();
    // The `NodeId` announce deny-set is a *derived* view of the durable origin
    // (Address) projection — nothing enumerates it on-chain, and `NodeId`s are
    // not persisted. Seed each restored origin as a pending `(addr, true)` op so
    // the first `on_tick_complete` resolves it through `nodeIdOf` and rebuilds the
    // deny-set (retrying any address whose lookup fails), exactly as a live
    // blacklisting would. Until that first pass the serving gate already refuses
    // these addresses via the restored `ContentDenylist`, so the window is
    // announce-selection only, not delivery.
    let pending_origin_peer_ops: HashMap<Address, bool> =
        restored_origin_addrs.iter().map(|a| (*a, true)).collect();
    denylist.set_chain_origins(restored_origin_addrs.into_iter().collect());
    cache.set_chain_denied(
        restored_denied_hashes
            .into_iter()
            .map(Hash::from_bytes)
            .collect(),
    );
    info!(
        %contract_addr,
        %operator,
        from_block,
        restored_entries = known.len(),
        restored_origins = restored_origin_count,
        restored_denied_hashes = restored_denied_count,
        replay_floor,
        "blacklist compliance watcher starting"
    );
    let initial_sync = InitialSyncGate::new(initial_sync_tx);
    let established_gate = initial_sync.clone();
    let backoff_gate = initial_sync.clone();
    let established_metrics = Arc::clone(metrics);
    let backoff_metrics = Arc::clone(metrics);
    let sink_metrics = Arc::clone(metrics);

    let cfg = WatcherConfig::new(
        head,
        Filter::new().address(contract_addr).event_signature(vec![
            HashBlacklisted::SIGNATURE_HASH,
            HashRemoved::SIGNATURE_HASH,
            HashSuspensionUpdated::SIGNATURE_HASH,
            // Origin blacklisting rides the same scan (ADR 011 § Hash Evasion
            // and Origin Blacklisting). It is deliberately outside the
            // `getBlacklistVersion()` mechanism, so unlike the three hash
            // events there is no counter to detect a missed one — the cursor
            // plus the durable origin projection are the whole guarantee.
            OriginBlacklistUpdated::SIGNATURE_HASH,
            // `addOperator` is the PRIMARY governance origin-blacklist path —
            // it is what ADR 011 § Hash Evasion names, and it ejects from
            // `CapacityBond`. It writes a SEPARATE mapping and emits these two
            // events, never `OriginBlacklistUpdated`. `OriginAssignment` unions
            // the two mappings on-chain; watching only the first would leave the
            // delivery gate enforcing the softer list and missing the voted one.
            OperatorBlacklisted::SIGNATURE_HASH,
            OperatorBlacklistCleared::SIGNATURE_HASH,
        ]),
        if replay_floor {
            CursorStart::FullReplay
        } else {
            cursor_start(checkpoint_store)
        },
        event_poll_interval.max(Duration::from_secs(1)),
        "blacklist",
    )
    .with_from_block(from_block)
    // The initial-sync gate rides the generic loop's healthy/backoff hooks: the
    // first established cycle signals `Ok`, the first backoff signals `Err`, and
    // an initial re-scope that can't enforce every entry bails the sink into that
    // backoff (see `BlacklistSink::on_tick_complete`). Later cycles are no-ops
    // once the gate has fired (`signal` takes the sender exactly once). The
    // watcher-health recorder fires *before* the gate signal so a test awaiting
    // the readiness oneshot never races the metric (#1283). This brings blacklist
    // to parity with its five peers, which all wire the same down-family.
    .on_established(Box::new(move || {
        established_metrics.blacklist_watcher_cycle_established();
        established_gate.signal(Ok(()));
    }))
    .on_backoff(Box::new(move || {
        backoff_metrics.blacklist_watcher_backoff_started();
        backoff_gate.signal(Err(
            "initial ContentBlacklist sync failed: chain RPC or cache eviction unavailable"
                .to_string(),
        ));
    }))
    .on_tick_success(metric_hook(metrics, Metrics::blacklist_watcher_tick))
    .on_task_panic(metric_hook(
        metrics,
        Metrics::blacklist_watcher_task_panicked,
    ));
    // Unlike the five flush-only sinks, `BlacklistSink` must observe the *same*
    // token the loop cancels: `rescan` polls it between per-hash `eth_call`s so a
    // large deny-set re-scope yields promptly to shutdown. `spawn` mints one
    // token and hands it to the factory, so sink and loop share it (#1236).
    resumable_watcher::spawn(provider, cfg, move |shutdown| BlacklistSink {
        contract,
        operator,
        cache,
        state: WatcherState {
            known: known.clone(),
            store: Arc::clone(&entry_store),
            denylist: Arc::clone(&denylist),
            capacity_bond: capacity_bond.clone(),
            peer_table: Arc::clone(&peer_table),
            announce_deny: Arc::clone(&announce_deny),
            // Moved, not cloned: the factory is `FnOnce` (constructed once), and
            // this map is not used after (unlike the shared `Arc` handles).
            pending_origin_peer_ops,
        },
        shutdown: shutdown.clone(),
        rescan_interval: rescan_interval.max(Duration::from_secs(1)),
        last_rescan: None,
        initial_sync,
        metrics: sink_metrics,
    })
}

/// Outcome of one batched re-scope pass.
struct RescanOutcome {
    /// `true` iff every entry was re-verified with no failed re-check. A
    /// shutdown-cancelled pass is unclean (`false`); it is decidedly **not** a
    /// drift window, but that is enforced in `resumable_watcher::run` (which
    /// suppresses the backoff edge under a cancelled token), not here (#1321).
    clean: bool,
    /// Distinct hashes this pass could not enforce (`Recheck::Failed` — a disk
    /// error or a scope `eth_call` failure). Feeds `blacklist_enforcement_failures`
    /// so the slashable "deny-set not fully enforced" condition has a metric of
    /// its own, even after the initial-sync gate has fired (#1319). A
    /// shutdown-cancelled pass reports the failures observed **before** the
    /// cancel (not `0`): a blob whose eviction already failed is a real, still-live
    /// exposure that must not be zeroed just because the process is stopping.
    failed: u64,
}

/// Re-scope every distinct hash in `known` (one scope `eth_call` per hash, not
/// per regional entry) and evict those now in scope. Interruptible by shutdown
/// between hashes. A shutdown-interrupted pass returns `clean = false` (so the
/// next tick would finish it — moot in practice, since the loop exits on cancel)
/// and the `failed` count accumulated up to the cancel point (#1319 exposures
/// already observed are not zeroed by an orderly stop).
async fn rescan<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState<P>,
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
        // entries left unenforced this pass (#1319). Per-hash failures already
        // log a `warn!` in `scope_check`/`evict`; this names the total.
        warn!(
            unenforced = failed,
            "blacklist re-scope could not enforce every entry"
        );
    }
    RescanOutcome { clean, failed }
}

/// Handle one live log: `HashBlacklisted` records the `(region, hash)` entry
/// and re-checks the hash; `HashRemoved` drops exactly that entry (hygiene —
/// eviction stays sticky, and same-hash entries in other regions survive).
/// `Ok(true)` iff a re-check failed and needs a prompt retry.
///
/// `Err` is reserved for a **durable deny-set write failure**, which is not a
/// retryable-in-place condition: it aborts the tick so the shared loop leaves the
/// scan cursor behind this log and re-reads it next tick. A scope-check RPC
/// failure is *not* an `Err` — the entry is already durably recorded, so it stays
/// in `known` and the pulled-forward re-scope retries it.
async fn handle_log<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState<P>,
    log: Log,
) -> Result<bool>
where
    P: Provider + Clone,
{
    match log.topic0() {
        Some(topic) if *topic == HashBlacklisted::SIGNATURE_HASH => Ok(on_blacklisted_log(
            contract, operator, cache, state, &log,
        )
        .await?
            == Recheck::Failed),
        Some(topic) if *topic == HashRemoved::SIGNATURE_HASH => {
            on_removed_log(contract, operator, cache, state, &log).await?;
            Ok(false)
        }
        Some(topic) if *topic == HashSuspensionUpdated::SIGNATURE_HASH => {
            Ok(on_suspension_log(contract, operator, cache, state, &log).await? == Recheck::Failed)
        }
        Some(topic) if *topic == OriginBlacklistUpdated::SIGNATURE_HASH => {
            Ok(on_origin_log(state, &log).await? == Recheck::Failed)
        }
        Some(topic) if *topic == OperatorBlacklisted::SIGNATURE_HASH => {
            Ok(on_operator_log(state, &log, true).await? == Recheck::Failed)
        }
        Some(topic) if *topic == OperatorBlacklistCleared::SIGNATURE_HASH => {
            Ok(on_operator_log(state, &log, false).await? == Recheck::Failed)
        }
        _ => Ok(false),
    }
}

/// Decode an `OriginBlacklistUpdated` log, apply it to the origin deny-set, and
/// then reconcile the peer table + announce-gate deny-set via
/// [`WatcherState::apply_origin_peer`] (ADR 011 § Hash Evasion and Origin
/// Blacklisting) — barring the `NodeId` on a blacklisting, un-barring on a clear.
///
/// Returns `Err` (aborting the tick, so the cursor does not advance) only if the
/// durable deny-set write fails. An undecodable log is skipped per the `LogSink`
/// contract, same as the hash events.
///
/// The peer-table/announce-gate reconcile is BEST-EFFORT on its `nodeIdOf` read
/// and reported as [`Recheck::Failed`] on an RPC error, which now queues the
/// address for a real retry in [`WatcherState::pending_origin_peer_ops`] (drained
/// every `on_tick_complete`) rather than aborting the tick. The ordering is
/// deliberate: the durable deny-set write (`set_origin`) is the enforcement — it
/// is what makes `serve_stream` refuse — while the peer/announce reconcile stops
/// us *selecting* or *re-admitting* that peer. Letting a `nodeIdOf` blip roll
/// back a durable deny-set write would trade the enforcing half for the advisory
/// one.
/// Decode an `OperatorBlacklisted` / `OperatorBlacklistCleared` log and apply it
/// to the same origin deny-set `OriginBlacklistUpdated` feeds.
///
/// One deny-set for both on-chain lists, mirroring `OriginAssignment`'s
/// `isOriginBlacklisted(op) || isOperatorBlacklisted(op)`. The node does not need
/// to know which list an address came from — only that governance put it on one.
async fn on_operator_log<P>(
    state: &mut WatcherState<P>,
    log: &Log,
    blacklisted: bool,
) -> Result<Recheck>
where
    P: Provider + Clone,
{
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
    state.set_origin(operator, blacklisted)?;
    debug!(%operator, blacklisted, "blacklist watcher: operator blacklist updated");
    // Both directions touch the announce deny-set (bar on add, un-bar on clear),
    // so unlike the old peer-table-only drop the clear path runs too.
    Ok(state.apply_origin_peer(operator, blacklisted).await)
}

/// An origin-class log we cannot decode is an ENFORCEMENT failure, not a parse
/// curiosity, so it aborts the tick and holds the scan cursor.
///
/// The hash events can afford to skip-and-continue: they are re-scoped every
/// pass and eviction is sticky. The origin events have no such backstop — no
/// version counter to reveal a gap, no enumeration to sweep against — so a
/// skipped log is gone permanently and the deny-set is silently short an entry.
/// Holding the cursor lets the readiness gate keep the router closed rather than
/// opening on a set we know is incomplete.
fn undecodable_origin_log(err: &alloy::sol_types::Error, log: &Log, event: &str) -> anyhow::Error {
    anyhow::anyhow!("{err}").context(format!(
        "undecodable {event} log at block {:?} tx {:?}; refusing to advance the scan cursor \
         past an unreadable takedown event",
        log.block_number, log.transaction_hash
    ))
}

async fn on_origin_log<P>(state: &mut WatcherState<P>, log: &Log) -> Result<Recheck>
where
    P: Provider + Clone,
{
    let event = match OriginBlacklistUpdated::decode_log_data(&log.inner.data) {
        Ok(event) => event,
        Err(err) => return Err(undecodable_origin_log(&err, log, "OriginBlacklistUpdated")),
    };
    state.set_origin(event.origin, event.blacklisted)?;
    debug!(
        origin = %event.origin,
        blacklisted = event.blacklisted,
        "blacklist watcher: origin blacklist updated"
    );
    Ok(state
        .apply_origin_peer(event.origin, event.blacklisted)
        .await)
}

/// Decode a `HashBlacklisted` log, record its `(region, hash)` entry, and
/// re-check the hash. An undecodable log is [`Recheck::NoAction`] (skipped, per
/// the `LogSink` contract).
async fn on_blacklisted_log<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState<P>,
    log: &Log,
) -> Result<Recheck>
where
    P: Provider + Clone,
{
    match HashBlacklisted::decode_log_data(&log.inner.data) {
        Ok(event) => {
            let hash = Hash::from_bytes(event.hash.0);
            state.add_entry(event.region, hash)?;
            Ok(recheck(contract, operator, cache, state, hash).await)
        }
        Err(err) => {
            warn!(err = %err, "blacklist watcher: undecodable HashBlacklisted log");
            Ok(Recheck::NoAction)
        }
    }
}

/// Decode a `HashRemoved` log and drop exactly that `(region, hash)` entry.
///
/// Also retires the hash from the governance deny-set, but only on a definitive
/// out-of-scope read: `HashRemoved` is per-region, and a same-hash entry under
/// another region can still cover this operator. `isHashBlacklistedForOperator`
/// is the authoritative union, so it — not the event — decides. An RPC failure
/// keeps the hash denied, which is the conservative direction: the hash stays
/// evicted regardless, so the cost is a stale refusal *code*, not a stale
/// refusal.
async fn on_removed_log<P: Provider + Clone>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState<P>,
    log: &Log,
) -> Result<()> {
    match HashRemoved::decode_log_data(&log.inner.data) {
        Ok(event) => {
            let hash = Hash::from_bytes(event.hash.0);
            state.remove_entry(event.region, hash)?;
            if cache.is_chain_denied(hash)
                && scope_check(contract, operator, hash).await == Some(false)
            {
                state.undeny_hash(cache, hash);
            }
            debug!(
                region = %event.region,
                %hash,
                "blacklist entry removed on-chain (local eviction stays sticky)"
            );
        }
        Err(err) => warn!(err = %err, "blacklist watcher: undecodable HashRemoved log"),
    }
    Ok(())
}

/// Decode a `HashSuspensionUpdated` log and react to the appeal-driven
/// suspend/resume toggle.
///
/// The entry is retained in `known` either way — as it already is for
/// out-of-scope entries — because suspension does not delete it and the periodic
/// re-scope must keep watching it. What this event buys is *latency* on the
/// resume edge: a reversal/lapse that clears `suspended` re-arms enforcement
/// while emitting no `HashBlacklisted`, so without this handler the hash stays
/// servable until the next batched re-scope (up to a full rescan cadence) — and
/// serving a re-enforced hash is slashable. On resume we therefore re-check
/// immediately, exactly as [`on_blacklisted_log`] does for a fresh entry.
///
/// The suspend edge needs no eviction: suspension only ever *narrows* what is
/// enforced (`isHashBlacklistedForOperator` starts returning false), and local
/// eviction is deliberately sticky and one-way — the node never un-evicts, which
/// stays conservative if it had already acted.
async fn on_suspension_log<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState<P>,
    log: &Log,
) -> Result<Recheck>
where
    P: Provider + Clone,
{
    match HashSuspensionUpdated::decode_log_data(&log.inner.data) {
        Ok(event) => {
            let hash = Hash::from_bytes(event.hash.0);
            state.add_entry(event.region, hash)?;
            if event.suspended {
                debug!(
                    region = %event.region,
                    %hash,
                    "blacklist entry suspended on-chain (retained for re-scoping; \
                     local eviction stays sticky)"
                );
                Ok(Recheck::NoAction)
            } else {
                debug!(
                    region = %event.region,
                    %hash,
                    "blacklist entry resumed on-chain; re-checking scope immediately"
                );
                Ok(recheck(contract, operator, cache, state, hash).await)
            }
        }
        Err(err) => {
            warn!(err = %err, "blacklist watcher: undecodable HashSuspensionUpdated log");
            Ok(Recheck::NoAction)
        }
    }
}

/// Outcome of one scope re-check, so callers can distinguish an enforcement
/// *failure* (retry promptly — the entry may be live and slashable) from a
/// legitimately out-of-scope entry (the periodic re-scope keeps watching it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Recheck {
    /// The hash was in scope and its eviction succeeded.
    Evicted,
    /// Nothing to do: already evicted, or currently out of scope / suspended.
    NoAction,
    /// The scope read or the eviction failed (RPC error/timeout, cache error) —
    /// the entry is retained and must be re-checked promptly.
    Failed,
}

/// Evict `hash` if in scope. Out-of-scope/suspended (`Some(false)`) and
/// RPC-error (`None`) hashes keep their `known` entries for the next re-scope
/// (callers insert before calling); evicted hashes drop *all* their regional
/// entries — eviction is sticky and region-independent.
async fn recheck<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState<P>,
    hash: Hash,
) -> Recheck
where
    P: Provider + Clone,
{
    if cache.is_evicted(hash) {
        // Back-fill the governance deny-set for a hash that is already evicted
        // but not yet recorded as governance-denied. This is the upgrade path:
        // `evicted.log` predates the deny projection and records no cause, so
        // takedowns discharged by an older build would otherwise keep answering
        // `EvictedSinceProbe` forever. Costs one scope read per already-evicted
        // entry on the replay that rebuilds the projection, and nothing after —
        // steady state drops evicted hashes from `known`, and a re-denied hash
        // short-circuits on `is_chain_denied`.
        if !cache.is_chain_denied(hash)
            && scope_check(contract, operator, hash).await == Some(true)
            && let Err(err) = state.deny_hash(cache, hash)
        {
            warn!(
                %hash,
                err = %sanitize_err_chain(&err),
                "blacklist watcher: could not back-fill a governance-denied hash; \
                 retrying on the next pass"
            );
            return Recheck::Failed;
        }
        state.drop_hash(hash);
        return Recheck::NoAction;
    }
    match scope_check(contract, operator, hash).await {
        Some(true) => {
            // Deny before evicting, and do NOT evict if the deny failed.
            // Eviction is what retires the hash from `known` (via `drop_hash`),
            // and `known` is the retry backstop: evicting past a failed deny
            // would drop the only worklist entry that would have retried it,
            // leaving the hash permanently refused under the wrong wire code.
            // Failing here is therefore safe but must be terminal for this pass.
            // Denying first also means an eviction that fails on a disk error
            // still stops the serving, since `CacheEngine::refuses` honors this
            // set too.
            if let Err(err) = state.deny_hash(cache, hash) {
                warn!(
                    %hash,
                    err = %sanitize_err_chain(&err),
                    "blacklist watcher: could not persist a governance-denied hash; \
                     retrying on the next pass"
                );
                return Recheck::Failed;
            }
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
        // One arm for both legs: `timed` folds the elapsed case into the same
        // `Err`, and its message names the call and the deadline ("… timed out
        // after 10s"), so the timeout stays distinguishable in the log text
        // without a separate arm carrying a `timeout_secs` field. That holds
        // only because the render is `sanitize_err_chain` — plain Display would
        // drop the deadline the moment anything above added context.
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

#[cfg(test)]
// Test-only: the assertion style below intentionally panics on the negative
// branch. Matches the convention in `content_deny.rs` / `config/mod.rs`.
#[allow(clippy::panic)]
mod tests {
    use super::*;

    const US: B256 = B256::repeat_byte(0x01);
    const FR: B256 = B256::repeat_byte(0x02);

    /// In-memory [`BlacklistEntryStore`], optionally wired to fail every write so
    /// a test can prove a durable-write failure aborts the tick.
    #[derive(Default)]
    struct MemEntryStore {
        rows: Mutex<HashSet<([u8; 32], [u8; 32])>>,
        origins: Mutex<HashSet<[u8; 20]>>,
        denied_hashes: Mutex<HashSet<[u8; 32]>>,
        fail_writes: bool,
        /// Models the redb table not existing yet — the first boot after
        /// upgrading to a build that tracks origins.
        origins_uninitialised: bool,
        /// Same, for the governance hash deny-set projection.
        denied_hashes_uninitialised: bool,
    }

    impl MemEntryStore {
        fn failing() -> Self {
            Self {
                fail_writes: true,
                ..Self::default()
            }
        }

        /// A store whose origin projection has never been built.
        fn uninitialised_origins() -> Self {
            Self {
                origins_uninitialised: true,
                ..Self::default()
            }
        }

        /// A store whose governance hash deny-set has never been built.
        fn uninitialised_denied_hashes() -> Self {
            Self {
                denied_hashes_uninitialised: true,
                ..Self::default()
            }
        }

        fn denied_hash_guard(&self) -> std::sync::MutexGuard<'_, HashSet<[u8; 32]>> {
            self.denied_hashes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        fn origin_guard(&self) -> std::sync::MutexGuard<'_, HashSet<[u8; 20]>> {
            self.origins
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        fn guard(&self) -> std::sync::MutexGuard<'_, HashSet<([u8; 32], [u8; 32])>> {
            self.rows
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        fn deny(&self) -> Result<(), decdn_incentive::store::StoreError> {
            if self.fail_writes {
                return Err(decdn_incentive::store::StoreError::Backend(
                    "injected deny-set write failure".into(),
                ));
            }
            Ok(())
        }
    }

    impl BlacklistEntryStore for MemEntryStore {
        fn load_blacklist_entries(
            &self,
        ) -> Result<Vec<([u8; 32], [u8; 32])>, decdn_incentive::store::StoreError> {
            Ok(self.guard().iter().copied().collect())
        }

        fn insert_blacklist_entry(
            &self,
            region: [u8; 32],
            hash: [u8; 32],
        ) -> Result<(), decdn_incentive::store::StoreError> {
            self.deny()?;
            self.guard().insert((region, hash));
            Ok(())
        }

        fn remove_blacklist_entry(
            &self,
            region: [u8; 32],
            hash: [u8; 32],
        ) -> Result<(), decdn_incentive::store::StoreError> {
            self.deny()?;
            self.guard().remove(&(region, hash));
            Ok(())
        }

        fn remove_blacklist_hash(
            &self,
            hash: [u8; 32],
        ) -> Result<(), decdn_incentive::store::StoreError> {
            self.deny()?;
            self.guard().retain(|(_, row_hash)| *row_hash != hash);
            Ok(())
        }

        fn load_blacklist_origins(
            &self,
        ) -> Result<Option<Vec<[u8; 20]>>, decdn_incentive::store::StoreError> {
            if self.origins_uninitialised {
                return Ok(None);
            }
            Ok(Some(self.origin_guard().iter().copied().collect()))
        }

        fn insert_blacklist_origin(
            &self,
            origin: [u8; 20],
        ) -> Result<(), decdn_incentive::store::StoreError> {
            self.deny()?;
            self.origin_guard().insert(origin);
            Ok(())
        }

        fn remove_blacklist_origin(
            &self,
            origin: [u8; 20],
        ) -> Result<(), decdn_incentive::store::StoreError> {
            self.deny()?;
            self.origin_guard().remove(&origin);
            Ok(())
        }

        fn load_blacklist_denied_hashes(
            &self,
        ) -> Result<Option<Vec<[u8; 32]>>, decdn_incentive::store::StoreError> {
            if self.denied_hashes_uninitialised {
                return Ok(None);
            }
            Ok(Some(self.denied_hash_guard().iter().copied().collect()))
        }

        fn insert_blacklist_denied_hash(
            &self,
            hash: [u8; 32],
        ) -> Result<(), decdn_incentive::store::StoreError> {
            self.deny()?;
            self.denied_hash_guard().insert(hash);
            Ok(())
        }

        fn remove_blacklist_denied_hash(
            &self,
            hash: [u8; 32],
        ) -> Result<(), decdn_incentive::store::StoreError> {
            self.deny()?;
            self.denied_hash_guard().remove(&hash);
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemCheckpointStore(Mutex<Option<u64>>);

    impl KeyedCheckpointStore for MemCheckpointStore {
        fn load_checkpoint(
            &self,
            _key: CheckpointKey,
        ) -> Result<Option<u64>, decdn_incentive::store::StoreError> {
            Ok(*self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner))
        }

        fn record_checkpoint(
            &self,
            _key: CheckpointKey,
            block: u64,
        ) -> Result<(), decdn_incentive::store::StoreError> {
            *self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(block);
            Ok(())
        }
    }

    /// A provider that answers nothing. The hash-event tests never reach an
    /// RPC through `WatcherState`; erasing to `DynProvider` keeps the fixture's
    /// type nameable so it unifies with the sink's own contract instance.
    fn mock_provider() -> alloy::providers::DynProvider {
        alloy::providers::ProviderBuilder::new()
            .connect_mocked_client(alloy::providers::mock::Asserter::new())
            .erased()
    }

    fn state() -> WatcherState<alloy::providers::DynProvider> {
        state_with(Arc::new(MemEntryStore::default()))
    }

    fn state_with(
        store: Arc<dyn BlacklistEntryStore>,
    ) -> WatcherState<alloy::providers::DynProvider> {
        state_with_denylist(store, Arc::new(ContentDenylist::empty()))
    }

    fn state_with_denylist(
        store: Arc<dyn BlacklistEntryStore>,
        denylist: Arc<ContentDenylist>,
    ) -> WatcherState<alloy::providers::DynProvider> {
        WatcherState {
            known: HashSet::new(),
            store,
            denylist,
            // Never called in these tests: they drive the hash-event paths,
            // which touch neither. The origin path is covered separately.
            capacity_bond: CapacityBond::new(Address::ZERO, mock_provider()),
            peer_table: Arc::new(RwLock::new(PeerTable::new(0, 0))),
            announce_deny: Arc::new(AnnounceOriginDenySet::new()),
            pending_origin_peer_ops: HashMap::new(),
        }
    }

    struct FailingHead;

    #[async_trait::async_trait]
    impl HeadSource for FailingHead {
        async fn head(&self) -> Result<u64> {
            anyhow::bail!("head unavailable")
        }
    }

    #[tokio::test]
    async fn initial_head_failure_signals_startup_error() -> Result<()> {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);
        let tmp = tempfile::tempdir()?;
        let cache = CacheEngine::open(tmp.path(), Vec::new(), 1).await?;
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        let handle = spawn(
            provider,
            Address::repeat_byte(0x11),
            Address::repeat_byte(0x22),
            cache,
            0,
            Duration::from_secs(1),
            Arc::new(FailingHead),
            Duration::from_mins(10),
            ready_tx,
            &Arc::new(Metrics::new()),
            Arc::new(MemEntryStore::default()),
            Arc::new(MemCheckpointStore::default()),
            Arc::new(ContentDenylist::empty()),
            Address::repeat_byte(0x33),
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            Arc::new(AnnounceOriginDenySet::new()),
        );
        let readiness = tokio::time::timeout(Duration::from_secs(1), ready_rx)
            .await
            .context("watcher did not report initial-sync failure")?
            .context("watcher dropped the readiness channel")?;

        assert!(
            readiness
                .as_ref()
                .is_err_and(|message| message.contains("initial ContentBlacklist sync failed")),
            "startup should receive a useful initial-sync error: {readiness:?}"
        );
        // Dropping the handle aborts the loop; cancel first for a graceful stop.
        handle.shutdown();
        Ok(())
    }

    struct StaticHead(u64);

    #[async_trait::async_trait]
    impl HeadSource for StaticHead {
        async fn head(&self) -> Result<u64> {
            Ok(self.0)
        }
    }

    /// The positive counterpart to the failure test: a clean first tick (head
    /// resolves, the replay window returns no logs, and the empty deny-set
    /// re-scope is trivially clean) must drive `on_established` and signal `Ok`
    /// through the real `spawn` wiring. Guards against a hook swap or mis-cloned
    /// `InitialSyncGate` that would keep every node's listeners closed forever
    /// while leaving the failure-path test green.
    #[tokio::test]
    async fn initial_clean_replay_signals_ready() -> Result<()> {
        let asserter = alloy::providers::mock::Asserter::new();
        // One empty `eth_getLogs` for the [from_block, head] replay window; the
        // head itself comes from the injected `StaticHead`, not the provider.
        asserter.push_success(&Vec::<Log>::new());
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);
        let tmp = tempfile::tempdir()?;
        let cache = CacheEngine::open(tmp.path(), Vec::new(), 1).await?;
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

        let handle = spawn(
            provider,
            Address::repeat_byte(0x11),
            Address::repeat_byte(0x22),
            cache,
            0,
            Duration::from_secs(1),
            Arc::new(StaticHead(25)),
            Duration::from_mins(10),
            ready_tx,
            &Arc::new(Metrics::new()),
            Arc::new(MemEntryStore::default()),
            Arc::new(MemCheckpointStore::default()),
            Arc::new(ContentDenylist::empty()),
            Address::repeat_byte(0x33),
            Arc::new(RwLock::new(PeerTable::new(0, 0))),
            Arc::new(AnnounceOriginDenySet::new()),
        );
        let readiness = tokio::time::timeout(Duration::from_secs(1), ready_rx)
            .await
            .context("watcher did not report initial-sync result")?
            .context("watcher dropped the readiness channel")?;

        assert!(
            readiness.is_ok(),
            "a clean first replay must signal readiness: {readiness:?}"
        );
        handle.shutdown();
        Ok(())
    }

    /// COMPLIANCE PIN, rewritten for #1181. The watcher now resumes from its
    /// durable cursor, which is only safe because the deny-set itself is durable.
    /// The cold-start leg is the part that must not regress: `ColdStart::Head`
    /// here would silently skip every blacklist entry that predates a node's
    /// first boot, and serving such a hash is slashable.
    #[test]
    fn cursor_start_resumes_but_cold_starts_from_the_deploy_block() {
        let store: Arc<dyn KeyedCheckpointStore> = Arc::new(MemCheckpointStore::default());
        assert!(
            matches!(
                cursor_start(store),
                CursorStart::FromCheckpoint {
                    checkpoint: Checkpoint {
                        key: CheckpointKey::Blacklist,
                        ..
                    },
                    cold_start: ColdStart::FromBlock,
                    ..
                }
            ),
            "blacklist must resume from the Blacklist checkpoint and cold-start at from_block"
        );
    }

    /// Removing one region's entry must not drop a surviving same-hash entry
    /// in another region — otherwise a later `updateRegion` into the surviving
    /// region (which emits no blacklist event) would never lead to eviction.
    #[test]
    fn remove_entry_is_region_scoped() -> Result<()> {
        let hash = Hash::from_bytes([0xAB; 32]);
        let mut state = state();
        state.add_entry(US, hash)?;
        state.add_entry(FR, hash)?;

        state.remove_entry(FR, hash)?;

        assert!(!state.known.contains(&(FR, hash)));
        assert!(state.known.contains(&(US, hash)), "US entry must survive");
        assert_eq!(state.distinct_hashes(), vec![hash]);
        Ok(())
    }

    #[test]
    fn remove_entry_drops_last_entry_for_hash() -> Result<()> {
        let hash = Hash::from_bytes([0xCD; 32]);
        let mut state = state();
        state.add_entry(US, hash)?;

        state.remove_entry(US, hash)?;

        assert!(state.known.is_empty());
        assert!(state.distinct_hashes().is_empty());
        Ok(())
    }

    /// Local eviction is sticky and region-independent, so it clears every
    /// regional entry for the hash while leaving other hashes untouched.
    #[test]
    fn drop_hash_clears_all_regions_for_that_hash_only() -> Result<()> {
        let store = Arc::new(MemEntryStore::default());
        let evicted = Hash::from_bytes([0xEE; 32]);
        let retained = Hash::from_bytes([0x11; 32]);
        let mut state = state_with(store.clone());
        state.add_entry(US, evicted)?;
        state.add_entry(FR, evicted)?;
        state.add_entry(FR, retained)?;

        state.drop_hash(evicted);

        assert!(!state.known.contains(&(US, evicted)));
        assert!(!state.known.contains(&(FR, evicted)));
        assert_eq!(state.distinct_hashes(), vec![retained]);
        // The durable half. Without this the in-memory assertions above pass
        // even if `drop_hash` stops writing through, and the evicted hash walks
        // back in on the next boot's reload.
        assert_eq!(
            store.load_blacklist_entries()?,
            vec![(FR.0, *retained.as_bytes())],
            "eviction must clear the hash from the durable set in every region"
        );
        Ok(())
    }

    /// The scope view is per `(operator, hash)`, so re-scoping must issue one
    /// check per distinct hash even when several regional entries share it.
    #[test]
    fn distinct_hashes_dedupes_across_regions() -> Result<()> {
        let hash = Hash::from_bytes([0x42; 32]);
        let mut state = state();
        state.add_entry(US, hash)?;
        state.add_entry(FR, hash)?;

        assert_eq!(state.distinct_hashes(), vec![hash]);
        Ok(())
    }

    /// Build a `BlacklistSink` with `entries` distinct known hashes over a mocked
    /// provider whose `eth_call` errors (empty asserter), so every re-scope
    /// re-check forces `Recheck::Failed`. `initial_sync` is left pending or
    /// pre-fired per the caller.
    async fn failing_sink(
        pending_gate: bool,
        entries: u8,
        metrics: &Arc<Metrics>,
    ) -> Result<BlacklistSink<alloy::providers::DynProvider>> {
        let asserter = alloy::providers::mock::Asserter::new();
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);
        let tmp = tempfile::tempdir()?;
        let cache = CacheEngine::open(tmp.path(), Vec::new(), 1).await?;
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let initial_sync = InitialSyncGate::new(tx);
        if !pending_gate {
            initial_sync.signal(Ok(()));
        }
        let mut state = state();
        for n in 0..entries {
            state.add_entry(US, Hash::from_bytes([n; 32]))?;
        }
        Ok(BlacklistSink {
            contract: ContentBlacklist::new(Address::repeat_byte(0x11), provider.erased()),
            operator: Address::repeat_byte(0x22),
            cache,
            state,
            shutdown: CancellationToken::new(),
            rescan_interval: Duration::from_secs(1),
            last_rescan: None,
            initial_sync,
            metrics: Arc::clone(metrics),
        })
    }

    /// [`failing_sink`]'s counterpart: a sink whose `isHashBlacklistedForOperator`
    /// calls answer `scope_results` in order, so a test can drive the *enforcing*
    /// path rather than the failure path. `store` is the caller's so it can
    /// assert on the durable projection.
    async fn enforcing_sink(
        scope_results: &[bool],
        store: Arc<dyn BlacklistEntryStore>,
        metrics: &Arc<Metrics>,
    ) -> Result<BlacklistSink<alloy::providers::DynProvider>> {
        let asserter = alloy::providers::mock::Asserter::new();
        for in_scope in scope_results {
            asserter.push_success(&abi_bool(*in_scope));
        }
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);
        let tmp = tempfile::tempdir()?;
        let cache = CacheEngine::open(tmp.path(), Vec::new(), 1).await?;
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let initial_sync = InitialSyncGate::new(tx);
        initial_sync.signal(Ok(()));
        Ok(BlacklistSink {
            contract: ContentBlacklist::new(Address::repeat_byte(0x11), provider.erased()),
            operator: Address::repeat_byte(0x22),
            cache,
            state: state_with(store),
            shutdown: CancellationToken::new(),
            rescan_interval: Duration::from_secs(1),
            last_rescan: None,
            initial_sync,
            metrics: Arc::clone(metrics),
        })
    }

    /// One ABI-encoded `bool` return word, as an `eth_call` result.
    fn abi_bool(value: bool) -> alloy::primitives::Bytes {
        let mut word = [0u8; 32];
        if value && let Some(last) = word.last_mut() {
            *last = 1;
        }
        alloy::primitives::Bytes::from(word.to_vec())
    }

    /// ABI-encoded `nodeIdOf` return `(bytes32 nodeId, bool active)`: the id word
    /// followed by the bool word. `apply_origin_peer` reads only `nodeId`, but the
    /// decoder needs both fields present.
    fn abi_node_id(node_id: [u8; 32], active: bool) -> alloy::primitives::Bytes {
        let mut out = node_id.to_vec();
        let mut active_word = [0u8; 32];
        if active {
            active_word[31] = 1;
        }
        out.extend_from_slice(&active_word);
        alloy::primitives::Bytes::from(out)
    }

    // ----- origin blacklist → announce-gate deny-set (#1398) -----

    /// A `WatcherState` over a mocked `capacity_bond` whose `nodeIdOf` calls
    /// answer from `asserter`, sharing `deny` so the announce-gate feed can be
    /// asserted. Origin-path only — `store`/`denylist`/`known` are the empty
    /// defaults these tests do not drive.
    fn origin_watcher_state(
        asserter: alloy::providers::mock::Asserter,
        deny: Arc<AnnounceOriginDenySet>,
    ) -> WatcherState<alloy::providers::DynProvider> {
        let provider = alloy::providers::ProviderBuilder::new()
            .connect_mocked_client(asserter)
            .erased();
        WatcherState {
            known: HashSet::new(),
            store: Arc::new(MemEntryStore::default()),
            denylist: Arc::new(ContentDenylist::empty()),
            capacity_bond: CapacityBond::new(Address::repeat_byte(0x33), provider),
            peer_table: Arc::new(RwLock::new(PeerTable::new(0, 0))),
            announce_deny: deny,
            pending_origin_peer_ops: HashMap::new(),
        }
    }

    /// A blacklisting bars the operator's `NodeId` from the announce gate, and a
    /// clear un-bars it — closing the re-entry gap the advisory peer-table drop
    /// leaves, since `setOriginBlacklist` does not eject.
    #[tokio::test]
    async fn origin_blacklist_feeds_and_clears_the_announce_deny_set() -> Result<()> {
        let node_id = [7u8; 32];
        let deny = Arc::new(AnnounceOriginDenySet::new());
        let asserter = alloy::providers::mock::Asserter::new();
        // Two `nodeIdOf` calls: the blacklisting, then the clear.
        asserter.push_success(&abi_node_id(node_id, true));
        asserter.push_success(&abi_node_id(node_id, true));
        let mut state = origin_watcher_state(asserter, Arc::clone(&deny));
        let op = Address::repeat_byte(0xAB);

        assert_eq!(state.apply_origin_peer(op, true).await, Recheck::NoAction);
        assert!(
            deny.contains(&node_id),
            "blacklisted operator is barred from announcing"
        );
        assert!(
            state.pending_origin_peer_ops.is_empty(),
            "a resolved op leaves no retry"
        );

        assert_eq!(state.apply_origin_peer(op, false).await, Recheck::NoAction);
        assert!(!deny.contains(&node_id), "cleared operator is un-barred");
        Ok(())
    }

    /// A `nodeIdOf` blip is no longer inert: the address is queued and the retry
    /// lands the bar once the RPC recovers (item 1 of #1398).
    #[tokio::test]
    async fn failed_nodeidof_queues_a_retry_that_later_lands() -> Result<()> {
        let node_id = [9u8; 32];
        let deny = Arc::new(AnnounceOriginDenySet::new());
        let asserter = alloy::providers::mock::Asserter::new();
        // First `nodeIdOf` errors; the retry succeeds.
        asserter.push_failure_msg("rpc down");
        asserter.push_success(&abi_node_id(node_id, true));
        let mut state = origin_watcher_state(asserter, Arc::clone(&deny));
        let op = Address::repeat_byte(0xCD);

        assert_eq!(state.apply_origin_peer(op, true).await, Recheck::Failed);
        assert_eq!(
            state.pending_origin_peer_ops.get(&op),
            Some(&true),
            "a failed lookup is queued for retry"
        );
        assert!(
            !deny.contains(&node_id),
            "nothing barred yet — the lookup failed"
        );

        // RPC recovers: the retry resolves, the bar lands, and the queue drains.
        assert_eq!(state.apply_origin_peer(op, true).await, Recheck::NoAction);
        assert!(deny.contains(&node_id));
        assert!(state.pending_origin_peer_ops.is_empty());
        Ok(())
    }

    /// The clear direction is symmetric: a `nodeIdOf` blip on an un-blacklist is
    /// also queued and retried, so a de-listed operator is not left barred from
    /// announcing forever (the hazard the retry queue's clear leg exists for).
    #[tokio::test]
    async fn failed_nodeidof_on_clear_queues_a_retry_that_later_unbars() -> Result<()> {
        let node_id = [0x3Cu8; 32];
        let deny = Arc::new(AnnounceOriginDenySet::new());
        let asserter = alloy::providers::mock::Asserter::new();
        asserter.push_success(&abi_node_id(node_id, true)); // bar
        asserter.push_failure_msg("rpc down"); // clear attempt fails
        asserter.push_success(&abi_node_id(node_id, true)); // clear retry succeeds
        let mut state = origin_watcher_state(asserter, Arc::clone(&deny));
        let op = Address::repeat_byte(0x4D);

        assert_eq!(state.apply_origin_peer(op, true).await, Recheck::NoAction);
        assert!(deny.contains(&node_id), "barred first");

        assert_eq!(state.apply_origin_peer(op, false).await, Recheck::Failed);
        assert_eq!(
            state.pending_origin_peer_ops.get(&op),
            Some(&false),
            "a failed clear is queued as the clear direction"
        );
        assert!(
            deny.contains(&node_id),
            "still barred until the clear lands"
        );

        assert_eq!(state.apply_origin_peer(op, false).await, Recheck::NoAction);
        assert!(!deny.contains(&node_id), "the clear retry un-bars");
        assert!(state.pending_origin_peer_ops.is_empty());
        Ok(())
    }

    /// An address with no registered node (`nodeIdOf → ZERO`) is a SETTLED no-op:
    /// it drains the pending entry (not a retry-forever) and bars nothing. This is
    /// the ZERO branch the origin-blacklist e2e silently exercises (its funder has
    /// no node).
    #[tokio::test]
    async fn zero_node_id_is_a_settled_no_op_that_clears_pending() -> Result<()> {
        let deny = Arc::new(AnnounceOriginDenySet::new());
        let asserter = alloy::providers::mock::Asserter::new();
        asserter.push_failure_msg("rpc down"); // first attempt fails → queued
        asserter.push_success(&abi_node_id([0u8; 32], false)); // retry resolves to ZERO
        let mut state = origin_watcher_state(asserter, Arc::clone(&deny));
        let op = Address::repeat_byte(0x5E);

        assert_eq!(state.apply_origin_peer(op, true).await, Recheck::Failed);
        assert_eq!(state.pending_origin_peer_ops.get(&op), Some(&true));

        assert_eq!(state.apply_origin_peer(op, true).await, Recheck::NoAction);
        assert!(
            state.pending_origin_peer_ops.is_empty(),
            "a ZERO resolution drains the pending entry (settled, not retried forever)"
        );
        assert!(deny.is_empty(), "a ZERO nodeId bars nothing");
        Ok(())
    }

    /// The actual retry DRIVER — `on_tick_complete` → `drain_pending_origin_peer_ops`
    /// — reconstructs the announce deny-set from the pending map, which is exactly
    /// the boot-rebuild path (`spawn` seeds `pending_origin_peer_ops` from the
    /// restored durable origin projection so the first tick re-derives the `NodeId`
    /// deny-set). Seeding the pending map directly here stands in for that seed and
    /// covers the drain end to end (the other origin tests call `apply_origin_peer`
    /// directly, bypassing the tick).
    #[tokio::test]
    async fn on_tick_complete_drains_pending_ops_into_the_deny_set() -> Result<()> {
        let node_id = [0x5Au8; 32];
        let deny = Arc::new(AnnounceOriginDenySet::new());
        let asserter = alloy::providers::mock::Asserter::new();
        asserter.push_success(&abi_node_id(node_id, true)); // answered on the drain
        let mut state = origin_watcher_state(asserter, Arc::clone(&deny));
        // Seed a pending op exactly as `spawn`'s boot rebuild does for a restored
        // origin, then let the tick drain resolve it.
        state
            .pending_origin_peer_ops
            .insert(Address::repeat_byte(0x7C), true);

        let tmp = tempfile::tempdir()?;
        let cache = CacheEngine::open(tmp.path(), Vec::new(), 1).await?;
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let initial_sync = InitialSyncGate::new(tx);
        initial_sync.signal(Ok(()));
        let mut sink = BlacklistSink {
            // No hashes in `known`, so the re-scope leg makes no contract call —
            // the only RPC this tick issues is the drain's `nodeIdOf`.
            contract: ContentBlacklist::new(Address::repeat_byte(0x11), mock_provider()),
            operator: Address::repeat_byte(0x22),
            cache,
            state,
            shutdown: CancellationToken::new(),
            rescan_interval: Duration::from_secs(1),
            last_rescan: None,
            initial_sync,
            metrics: Arc::new(Metrics::new()),
        };

        sink.on_tick_complete().await?;
        assert!(
            deny.contains(&node_id),
            "the tick drain must reconstruct the announce deny-set from pending ops"
        );
        assert!(
            sink.state.pending_origin_peer_ops.is_empty(),
            "a resolved op must drain from the pending map"
        );
        Ok(())
    }

    // ----- governance hash deny-set (ADR 011 §StreamRequest Response) -----

    /// Enforcement is TWO writes, and the deny is the one that is easy to forget
    /// because eviction alone already stops the serving. Without it the client
    /// handler answers a governance takedown with `EvictedSinceProbe` while a
    /// local `[content] denied_hashes` entry answers `HashBlacklisted`, so one
    /// request tells a client which list a hash is on — and since the governance
    /// list is public on-chain, that identifies the operator's PRIVATE entries by
    /// elimination.
    #[tokio::test]
    async fn enforcement_denies_and_evicts_and_persists() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store = Arc::new(MemEntryStore::default());
        let mut sink = enforcing_sink(&[true], Arc::clone(&store) as _, &metrics).await?;
        let hash = Hash::from_bytes([0x51; 32]);
        sink.state.add_entry(US, hash)?;

        let outcome = recheck(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            hash,
        )
        .await;

        assert!(outcome == Recheck::Evicted);
        assert!(
            sink.cache.is_chain_denied(hash),
            "the live deny-set the delivery path reads must carry the reason"
        );
        assert!(
            sink.cache.is_evicted(hash),
            "and the bytes still get reclaimed"
        );
        assert_eq!(
            store.load_blacklist_denied_hashes()?,
            Some(vec![*hash.as_bytes()]),
            "and the durable projection a restart rebuilds the reason from"
        );
        Ok(())
    }

    /// A failed deny must NOT be followed by the eviction. Eviction is what
    /// retires the hash from `known` (`drop_hash`), and `known` is the retry
    /// backstop — evicting past a failed deny would drop the only worklist entry
    /// that would have retried it, stranding the hash under the wrong wire code
    /// permanently.
    #[tokio::test]
    async fn a_failed_deny_leaves_the_hash_retryable() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store = Arc::new(MemEntryStore::failing());
        let mut sink = enforcing_sink(&[true], Arc::clone(&store) as _, &metrics).await?;
        let hash = Hash::from_bytes([0x52; 32]);
        // Inserted directly: `add_entry` would hit the same injected failure.
        sink.state.known.insert((US, hash));

        let outcome = recheck(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            hash,
        )
        .await;

        assert!(outcome == Recheck::Failed);
        assert!(
            !sink.cache.is_evicted(hash),
            "evicting past a failed deny would drop the retry"
        );
        assert!(
            sink.state.known.contains(&(US, hash)),
            "the entry stays on the worklist for the next re-scope"
        );
        Ok(())
    }

    /// The upgrade path. `evicted.log` predates this projection and records that
    /// a hash was evicted, never *why*, so takedowns discharged by an older build
    /// would answer `EvictedSinceProbe` forever. The replay that rebuilds the
    /// projection has to back-fill them.
    #[tokio::test]
    async fn an_already_evicted_hash_is_back_filled_into_the_deny_set() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store = Arc::new(MemEntryStore::default());
        let mut sink = enforcing_sink(&[true], Arc::clone(&store) as _, &metrics).await?;
        let hash = Hash::from_bytes([0x53; 32]);
        sink.cache.evict(hash).await?;
        sink.state.add_entry(US, hash)?;

        let outcome = recheck(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            hash,
        )
        .await;

        assert!(
            outcome == Recheck::NoAction,
            "already evicted, nothing to evict"
        );
        assert!(
            sink.cache.is_chain_denied(hash),
            "but the reason must still be recorded, or the wire code stays wrong"
        );
        Ok(())
    }

    /// ...and it costs nothing once recorded: a second pass must not re-spend a
    /// scope read on a hash already known to be governance-denied. (The sink is
    /// built with ONE queued response, so a second `eth_call` would error and the
    /// outcome would be `Failed`.)
    #[tokio::test]
    async fn back_fill_does_not_repeat_once_recorded() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store = Arc::new(MemEntryStore::default());
        let mut sink = enforcing_sink(&[true], Arc::clone(&store) as _, &metrics).await?;
        let hash = Hash::from_bytes([0x54; 32]);
        sink.cache.evict(hash).await?;

        for _ in 0..2 {
            sink.state.add_entry(US, hash)?;
            let outcome = recheck(
                &sink.contract,
                sink.operator,
                &sink.cache,
                &mut sink.state,
                hash,
            )
            .await;
            assert!(outcome == Recheck::NoAction);
        }
        Ok(())
    }

    /// A `HashRemoved` lifts the governance deny — but only on a definitive
    /// out-of-scope read, since the event is per-region and a same-hash entry
    /// under another region can still cover this operator. The hash stays
    /// evicted either way; only the refusal *code* moves.
    #[tokio::test]
    async fn hash_removal_lifts_the_deny_when_out_of_scope() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store = Arc::new(MemEntryStore::default());
        // Two scope reads: one to enforce, one on the removal.
        let mut sink = enforcing_sink(&[true, false], Arc::clone(&store) as _, &metrics).await?;
        let hash = Hash::from_bytes([0x55; 32]);
        sink.state.add_entry(US, hash)?;
        let _ = recheck(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            hash,
        )
        .await;
        assert!(
            sink.cache.is_chain_denied(hash),
            "denied before the removal"
        );

        on_removed_log(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            &removed_log(US, *hash.as_bytes()),
        )
        .await?;

        assert!(
            !sink.cache.is_chain_denied(hash),
            "de-listed hashes stop being blacklist-coded"
        );
        assert_eq!(store.load_blacklist_denied_hashes()?, Some(Vec::new()));
        assert!(
            sink.cache.is_evicted(hash),
            "...but the eviction is sticky and one-way"
        );
        Ok(())
    }

    /// ...whereas a hash still in scope under another region keeps its deny. The
    /// conservative direction: over-denying costs a wire code on a hash that is
    /// refused regardless, under-denying re-opens the fingerprint.
    #[tokio::test]
    async fn hash_removal_keeps_the_deny_when_still_in_scope() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store = Arc::new(MemEntryStore::default());
        let mut sink = enforcing_sink(&[true, true], Arc::clone(&store) as _, &metrics).await?;
        let hash = Hash::from_bytes([0x56; 32]);
        sink.state.add_entry(US, hash)?;
        let _ = recheck(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            hash,
        )
        .await;

        on_removed_log(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            &removed_log(FR, *hash.as_bytes()),
        )
        .await?;

        assert!(sink.cache.is_chain_denied(hash));
        Ok(())
    }

    /// The restart property. The projection is the only record of *why* a hash
    /// is refused that survives a process restart, so a reboot must come up with
    /// the same wire code it went down with.
    #[test]
    fn restored_denied_hashes_survive_a_restart() -> Result<()> {
        let store = MemEntryStore::default();
        let hash = [0x57u8; 32];
        store.insert_blacklist_denied_hash(hash)?;

        let restored = restore_projection(&store as &dyn BlacklistEntryStore);
        assert_eq!(restored.denied_hashes, vec![hash]);
        assert!(!restored.replay_floor);
        Ok(())
    }

    /// Same absent-vs-empty trap as the origin projection: this table is newer
    /// than the scan cursor, so resuming on an absent one would leave every
    /// pre-existing takedown answering the distinguishing code forever.
    #[test]
    fn absent_denied_hash_projection_forces_a_replay() {
        let store = MemEntryStore::uninitialised_denied_hashes();
        let restored = restore_projection(&store as &dyn BlacklistEntryStore);
        assert!(
            restored.replay_floor,
            "a never-built governance deny-set must replay from the deploy block"
        );
    }

    /// #1319: after the initial-sync gate has fired, a re-scope that cannot
    /// enforce every entry must NOT bail (so the loop reads healthy), but MUST
    /// bump `blacklist_enforcement_failures_total` — the slashable exposure gets
    /// its own metric even while `blacklist_watcher_down_seconds` reads 0.
    #[tokio::test]
    async fn post_gate_enforcement_failure_counts_without_bailing() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let mut sink = failing_sink(false, 1, &metrics).await?;

        let result = sink.on_tick_complete().await;
        assert!(
            result.is_ok(),
            "a post-gate enforcement failure must not bail the tick: {result:?}"
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
    /// hashes in one pass (`inc_by`), not once — undercounting would understate a
    /// slashable exposure that feeds a `rate()`-based alert.
    #[tokio::test]
    async fn enforcement_failure_counter_aggregates_per_pass() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let mut sink = failing_sink(false, 2, &metrics).await?;

        sink.on_tick_complete().await?;

        let text = metrics.encode()?;
        assert!(
            text.lines()
                .any(|l| l == "decdn_blacklist_enforcement_failures_total 2"),
            "two unenforced entries in one pass must bump the counter by 2:\n{text}"
        );
        Ok(())
    }

    /// The #1321 guard's counterpart at the sink layer: with the gate still
    /// pending and no shutdown, an unenforceable re-scope still bails into backoff
    /// (the shared loop only suppresses that bail's *effect* under a cancelled
    /// token — see `resumable_watcher::tests`; the sink must still signal it).
    #[tokio::test]
    async fn pending_unclean_still_bails() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let mut sink = failing_sink(true, 1, &metrics).await?;

        let result = sink.on_tick_complete().await;
        assert!(
            result.is_err(),
            "a genuine unenforceable initial re-scope must still bail into backoff"
        );
        Ok(())
    }

    fn blacklisted_log(region: B256, hash_bytes: [u8; 32], version: u64) -> Log {
        let event = HashBlacklisted {
            region,
            hash: B256::from(hash_bytes),
            version: alloy::primitives::U256::from(version),
            reason: String::new(),
        };
        Log {
            inner: alloy::primitives::Log {
                address: Address::repeat_byte(0x11),
                data: event.encode_log_data(),
            },
            ..Default::default()
        }
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

    // ----- origin deny-set (ADR 011 § Hash Evasion, #1179) -----

    /// The end-to-end unit property: an `OriginBlacklistUpdated` log reaches the
    /// deny-set the delivery path reads AND the durable projection a restart
    /// rebuilds from. Either half alone is a fail-open.
    #[tokio::test]
    async fn origin_log_denies_and_persists() -> Result<()> {
        let store = Arc::new(MemEntryStore::default());
        let deny = Arc::new(ContentDenylist::empty());
        let mut state = state_with_denylist(Arc::clone(&store) as _, Arc::clone(&deny));
        let origin = Address::repeat_byte(0x44);

        let _ = on_origin_log(&mut state, &origin_log(origin, true)).await?;

        assert!(deny.is_origin_denied(&origin), "reaches the live deny-set");
        assert_eq!(
            store.load_blacklist_origins()?,
            Some(vec![origin.into()]),
            "and the durable projection a restart rebuilds from"
        );
        Ok(())
    }

    /// De-listing must clear both halves, or a restart resurrects the entry.
    #[tokio::test]
    async fn origin_delisting_clears_both_halves() -> Result<()> {
        let store = Arc::new(MemEntryStore::default());
        let deny = Arc::new(ContentDenylist::empty());
        let mut state = state_with_denylist(Arc::clone(&store) as _, Arc::clone(&deny));
        let origin = Address::repeat_byte(0x45);

        let _ = on_origin_log(&mut state, &origin_log(origin, true)).await?;
        let _ = on_origin_log(&mut state, &origin_log(origin, false)).await?;

        assert!(!deny.is_origin_denied(&origin));
        assert_eq!(store.load_blacklist_origins()?, Some(Vec::new()));
        Ok(())
    }

    /// `addOperator` is the primary governance path — it emits
    /// `OperatorBlacklisted`, never `OriginBlacklistUpdated`, and writes a
    /// different on-chain mapping. Watching only the latter left the voted,
    /// ejecting path unenforced at the delivery gate.
    #[tokio::test]
    async fn operator_blacklist_log_reaches_the_same_deny_set() -> Result<()> {
        let store = Arc::new(MemEntryStore::default());
        let deny = Arc::new(ContentDenylist::empty());
        let mut state = state_with_denylist(Arc::clone(&store) as _, Arc::clone(&deny));
        let operator = Address::repeat_byte(0x46);

        let _ = on_operator_log(&mut state, &operator_log(operator), true).await?;

        assert!(deny.is_origin_denied(&operator));
        assert_eq!(store.load_blacklist_origins()?, Some(vec![operator.into()]));
        Ok(())
    }

    /// The origin twin of `durable_write_failure_aborts_the_tick`. A failed
    /// durable write must still hold the scan cursor (return `Err`); letting it
    /// advance loses the event permanently, because origin events carry no
    /// version and nothing enumerates the set on-chain.
    ///
    /// But the in-memory deny applies FIRST (#1398 item 2), so a store error
    /// leaves the origin DENIED in memory rather than serving. The old order
    /// (persist, then apply) skipped the in-memory apply via `?` on a store
    /// error, leaving the node serving a just-blacklisted origin until a later
    /// tick — a fail-open. The tick still aborts and the cursor still holds, so
    /// the write retries; an unpersisted entry is simply re-derived from the
    /// event tail on restart.
    #[tokio::test]
    async fn origin_write_failure_aborts_the_tick_but_denies_in_memory() {
        let store = Arc::new(MemEntryStore::failing());
        let deny = Arc::new(ContentDenylist::empty());
        let mut state = state_with_denylist(Arc::clone(&store) as _, Arc::clone(&deny));
        let origin = Address::repeat_byte(0x47);

        let Err(err) = on_origin_log(&mut state, &origin_log(origin, true)).await else {
            panic!("a failed durable write must abort the tick");
        };
        assert!(
            format!("{err:#}").contains("persist blacklisted origin"),
            "{err:#}"
        );
        assert!(
            deny.is_origin_denied(&origin),
            "the in-memory deny applies before the durable write, so a store error \
             still fails CLOSED — the origin is refused, not served"
        );
    }

    /// An undecodable origin log must NOT be skipped. The hash events can
    /// afford skip-and-continue (re-scoped every pass, sticky eviction); these
    /// cannot, so the tick aborts rather than advancing the cursor past a
    /// takedown the node could not read.
    #[tokio::test]
    async fn undecodable_origin_log_aborts_the_tick() {
        let mut state = state();
        let mut log = origin_log(Address::repeat_byte(0x48), true);
        log.inner.data.data = vec![0x01].into();

        let Err(err) = on_origin_log(&mut state, &log).await else {
            panic!("an unreadable takedown event must not be skipped");
        };
        assert!(
            format!("{err:#}").contains("refusing to advance"),
            "{err:#}"
        );
    }

    /// The upgrade path. The origin projection is newer than the scan cursor, so
    /// "table absent" and "table empty" have opposite consequences: absent must
    /// force a replay, or the resumed scan starts past every origin event ever
    /// emitted and the gate stays permanently empty.
    #[test]
    fn absent_origin_projection_forces_a_replay() {
        let store = MemEntryStore::uninitialised_origins();
        let restored = restore_projection(&store as &dyn BlacklistEntryStore);
        assert!(
            restored.replay_floor,
            "a never-built origin projection must replay from the deploy block"
        );
    }

    /// ...whereas a genuinely empty one must NOT, or every node would rescan the
    /// whole chain on every boot.
    #[test]
    fn empty_origin_projection_resumes_from_the_cursor() {
        let store = MemEntryStore::default();
        let restored = restore_projection(&store as &dyn BlacklistEntryStore);
        assert!(!restored.replay_floor);
    }

    /// A restart must come up enforcing what it learned before, without waiting
    /// for a fresh on-chain event — the durable projection is the whole
    /// guarantee here.
    #[test]
    fn restored_origins_survive_a_restart() -> Result<()> {
        let store = MemEntryStore::default();
        let origin = Address::repeat_byte(0x49);
        store.insert_blacklist_origin(origin.into())?;

        let restored = restore_projection(&store as &dyn BlacklistEntryStore);
        assert_eq!(restored.origins, vec![<[u8; 20]>::from(origin)]);
        assert!(!restored.replay_floor);
        Ok(())
    }

    /// #1181's load-bearing invariant. A durable deny-set write failure must
    /// abort the tick with `Err`, so the shared loop leaves the scan cursor
    /// *behind* this log and re-reads it next tick. Swallowing it would let the
    /// cursor advance past an entry the node never re-learns — a hash it would
    /// then serve, and be slashable for serving.
    #[tokio::test]
    async fn durable_write_failure_aborts_the_tick() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let mut sink = failing_sink(false, 0, &metrics).await?;
        sink.state = state_with(Arc::new(MemEntryStore::failing()));

        let outcome = handle_log(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            blacklisted_log(US, [0x05u8; 32], 3),
        )
        .await;

        assert!(
            outcome.is_err(),
            "a failed deny-set write must abort the tick so the cursor cannot advance past it"
        );
        assert!(
            sink.state.known.is_empty(),
            "a failed write must not leave the entry claimed in the in-memory set"
        );
        Ok(())
    }

    /// The durable set is what a resumed boot rebuilds from, so an add must be
    /// visible to a fresh reader and a remove must clear it.
    #[tokio::test]
    async fn deny_set_round_trips_through_the_store() -> Result<()> {
        let store = Arc::new(MemEntryStore::default());
        let mut state = state_with(store.clone());
        let hash = Hash::from_bytes([0x42u8; 32]);

        state.add_entry(US, hash)?;
        assert_eq!(
            store.load_blacklist_entries()?,
            vec![(US.0, *hash.as_bytes())],
            "an added entry must be durably visible for the next boot to reload"
        );

        state.remove_entry(US, hash)?;
        assert!(
            store.load_blacklist_entries()?.is_empty(),
            "a removed entry must not survive into the next boot"
        );
        Ok(())
    }

    fn suspension_log(region: B256, hash_bytes: [u8; 32], version: u64, suspended: bool) -> Log {
        let event = HashSuspensionUpdated {
            region,
            hash: B256::from(hash_bytes),
            version: alloy::primitives::U256::from(version),
            suspended,
        };
        Log {
            inner: alloy::primitives::Log {
                address: Address::repeat_byte(0x11),
                data: event.encode_log_data(),
            },
            ..Default::default()
        }
    }

    /// #1300, resume edge. A reversal/lapse clears `suspended` and re-arms
    /// enforcement while emitting no `HashBlacklisted`, so the handler must
    /// re-check scope *immediately* instead of leaving the hash servable (and
    /// slashable) until the next batched re-scope. The mocked provider has no
    /// queued response, so a re-check that is genuinely attempted surfaces as
    /// `Recheck::Failed` — which is exactly the proof that it ran.
    #[tokio::test]
    async fn resume_rechecks_immediately() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let mut sink = failing_sink(false, 0, &metrics).await?;
        let bytes = [0x07u8; 32];

        let outcome = on_suspension_log(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            &suspension_log(US, bytes, 9, false),
        )
        .await?;

        assert!(
            outcome == Recheck::Failed,
            "a resume must attempt an immediate scope re-check"
        );
        assert!(
            sink.state.known.contains(&(US, Hash::from_bytes(bytes))),
            "the resumed entry must stay known for re-scoping"
        );
        Ok(())
    }

    /// #1300, suspend edge. Suspension only ever *narrows* what is enforced, so
    /// it must not spend an RPC on a re-check — but the entry must stay in
    /// `known` so the periodic re-scope keeps watching it for the eventual
    /// resume. (Local eviction is sticky and one-way, so nothing is un-evicted.)
    #[tokio::test]
    async fn suspend_retains_entry_without_recheck() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let mut sink = failing_sink(false, 0, &metrics).await?;
        let bytes = [0x08u8; 32];

        let outcome = on_suspension_log(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &mut sink.state,
            &suspension_log(US, bytes, 10, true),
        )
        .await?;

        assert!(
            outcome == Recheck::NoAction,
            "a suspend must not attempt a scope re-check"
        );
        assert!(
            sink.state.known.contains(&(US, Hash::from_bytes(bytes))),
            "the suspended entry must stay known for re-scoping"
        );
        Ok(())
    }
}
