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
//! Eviction is the single lever, and it cascades to every serving surface:
//! [`decdn_cache::CacheEngine::evict`] durably records the takedown (survives
//! restart via `evicted.log`), the DHT republisher drops the hash on its next
//! tick (its `is_evicted` gate), the probe handler stops signing
//! `has_blob: true` once the blob leaves the store, and the client handler
//! refuses delivery with `EvictedSinceProbe` while never re-pull-filling it.
//! `evict` is sticky and works on absent hashes, so a hash blacklisted while the
//! node was offline (and not yet held) is still pre-blocked.
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

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::{Context as _, Result};
use decdn_cache::{CacheEngine, Hash};
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::content_blacklist::ContentBlacklist;
use decdn_incentive::content_blacklist::ContentBlacklist::{
    HashBlacklisted, HashRemoved, HashSuspensionUpdated,
};
use decdn_incentive::store::{BlacklistEntryStore, CheckpointKey, KeyedCheckpointStore};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain_events::resumable_watcher::{
    self, Checkpoint, ColdStart, CursorStart, LogSink, WatcherConfig, WatcherHandle,
};
use crate::chain_events::shared_head::HeadSource;
use crate::chain_events::{REORG_MARGIN_BLOCKS, timed};
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
struct WatcherState {
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
}

impl WatcherState {
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

    /// Distinct hashes across all regions — the scope view
    /// (`isHashBlacklistedForOperator`) is per `(operator, hash)`, so each
    /// hash needs exactly one `eth_call` per pass regardless of how many
    /// regional entries reference it.
    fn distinct_hashes(&self) -> Vec<Hash> {
        let unique: HashSet<Hash> = self.known.iter().map(|(_, hash)| *hash).collect();
        unique.into_iter().collect()
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
    state: WatcherState,
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
) -> WatcherHandle
where
    P: Provider + Clone + 'static,
{
    let contract = ContentBlacklist::new(contract_addr, provider.clone());
    // Rebuild the retained deny-set from disk before the first tick. A read
    // failure is not fatal: an empty set plus the resumed cursor would under-
    // enforce, so fall back to replaying from `from_block`, which reconstructs
    // the set from events exactly as the pre-#1181 watcher did.
    let (restored, replay_floor) = match entry_store.load_blacklist_entries() {
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
    let known: HashSet<(B256, Hash)> = restored
        .into_iter()
        .map(|(region, hash)| (B256::from(region), Hash::from_bytes(hash)))
        .collect();
    info!(
        %contract_addr,
        %operator,
        from_block,
        restored_entries = known.len(),
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
    state: &mut WatcherState,
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
            on_removed_log(state, &log)?;
            Ok(false)
        }
        Some(topic) if *topic == HashSuspensionUpdated::SIGNATURE_HASH => {
            Ok(on_suspension_log(contract, operator, cache, state, &log).await? == Recheck::Failed)
        }
        _ => Ok(false),
    }
}

/// Decode a `HashBlacklisted` log, record its `(region, hash)` entry, and
/// re-check the hash. An undecodable log is [`Recheck::NoAction`] (skipped, per
/// the `LogSink` contract).
async fn on_blacklisted_log<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
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
fn on_removed_log(state: &mut WatcherState, log: &Log) -> Result<()> {
    match HashRemoved::decode_log_data(&log.inner.data) {
        Ok(event) => {
            let hash = Hash::from_bytes(event.hash.0);
            state.remove_entry(event.region, hash)?;
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
    state: &mut WatcherState,
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
#[derive(Clone, Copy, PartialEq, Eq)]
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
    state: &mut WatcherState,
    hash: Hash,
) -> Recheck
where
    P: Provider + Clone,
{
    if cache.is_evicted(hash) {
        state.drop_hash(hash);
        return Recheck::NoAction;
    }
    match scope_check(contract, operator, hash).await {
        Some(true) => {
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
mod tests {
    use super::*;

    const US: B256 = B256::repeat_byte(0x01);
    const FR: B256 = B256::repeat_byte(0x02);

    /// In-memory [`BlacklistEntryStore`], optionally wired to fail every write so
    /// a test can prove a durable-write failure aborts the tick.
    #[derive(Default)]
    struct MemEntryStore {
        rows: Mutex<HashSet<([u8; 32], [u8; 32])>>,
        fail_writes: bool,
    }

    impl MemEntryStore {
        fn failing() -> Self {
            Self {
                rows: Mutex::new(HashSet::new()),
                fail_writes: true,
            }
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

    fn state() -> WatcherState {
        state_with(Arc::new(MemEntryStore::default()))
    }

    fn state_with(store: Arc<dyn BlacklistEntryStore>) -> WatcherState {
        WatcherState {
            known: HashSet::new(),
            store,
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
    ) -> Result<BlacklistSink<impl Provider + Clone>> {
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
            contract: ContentBlacklist::new(Address::repeat_byte(0x11), provider),
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
