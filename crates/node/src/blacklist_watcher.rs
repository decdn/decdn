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
//! **Full replay each boot (no persisted cursor).** `known` is in-memory and the
//! contract has no enumeration, so the deny-set can only be rebuilt by replaying
//! `HashBlacklisted` from the deploy block on **every** start (`from_block`
//! floor, no persist). A persisted scan cursor would resume past logs whose
//! (still out-of-scope, so un-evicted) entries are gone from the in-memory
//! `known`, silently dropping them from re-scoping — a compliance gap, since
//! serving a blacklisted hash is slashable. The #1108 crash-loop (a rate-limited
//! boot tripping the startup preflight) is instead mitigated by the preflight's
//! bounded 429/5xx retry (`check_rpc_reachability` — a *persistently* throttled
//! endpoint can still exhaust it): the poller then makes windowed forward
//! progress under backoff without exiting the process. A durable deny-set (to
//! make the resume safe) is a tracked follow-up.
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
use anyhow::Result;
use decdn_cache::{CacheEngine, Hash};
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::content_blacklist::ContentBlacklist;
use decdn_incentive::content_blacklist::ContentBlacklist::{HashBlacklisted, HashRemoved};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain_events::resumable_watcher::{
    self, CursorStart, LogSink, WatcherConfig, WatcherHandle,
};
use crate::chain_events::shared_head::HeadSource;
use crate::chain_events::timed;

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
    known: HashSet<(B256, Hash)>,
}

impl WatcherState {
    /// Record a `HashBlacklisted(region, hash)` entry.
    fn add_entry(&mut self, region: B256, hash: Hash) {
        self.known.insert((region, hash));
    }

    /// Drop exactly the `HashRemoved(region, hash)` entry — same-hash entries
    /// under other regions stay retained for re-scoping.
    fn remove_entry(&mut self, region: B256, hash: Hash) {
        self.known.remove(&(region, hash));
    }

    /// Drop every entry for `hash` (once locally evicted, the sticky eviction
    /// covers all regions).
    fn drop_hash(&mut self, hash: Hash) {
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
        .await;
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
            let clean = rescan(
                &self.contract,
                self.operator,
                &self.cache,
                &mut self.state,
                &self.shutdown,
            )
            .await;
            // A pass with any failed re-check retries at the poll cadence until
            // it comes back clean; only a clean pass waits out the full
            // operator cadence again.
            self.last_rescan = clean.then(Instant::now);
            if !clean && self.initial_sync.pending() {
                anyhow::bail!(
                    "initial ContentBlacklist replay/re-scope could not enforce every entry"
                );
            }
        }
        Ok(())
    }
}

/// The blacklist watcher's cursor policy: **full replay from the deploy floor
/// on every boot, never persisted**. The in-memory deny-set has no on-chain
/// enumeration source, so a persisted resume would skip logs whose (still
/// out-of-scope, so un-evicted) entries are gone from `known` — silently
/// dropping them from re-scoping, a slashable compliance gap (see the module
/// header). Pinned by a test so a wiring change to a persisted cursor cannot
/// land silently. The replay floor is the watcher's `from_block` (the deploy
/// block), resolved by [`CursorStart::FullReplay`] on every boot.
const fn cursor_start() -> CursorStart {
    CursorStart::FullReplay
}

/// Spawn the blacklist compliance watcher, returning the handle that owns its
/// task and shutdown token. `from_block` is where the `HashBlacklisted` replay
/// starts (the `ContentBlacklist` deploy block) — re-scanned every boot, no
/// persisted cursor (see the module header). `event_poll_interval` is the
/// getLogs poll cadence; `rescan_interval` is the batched re-scope cadence.
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
) -> WatcherHandle
where
    P: Provider + Clone + 'static,
{
    let contract = ContentBlacklist::new(contract_addr, provider.clone());
    info!(%contract_addr, %operator, from_block, "blacklist compliance watcher starting");
    let initial_sync = InitialSyncGate::new(initial_sync_tx);
    let established_gate = initial_sync.clone();
    let backoff_gate = initial_sync.clone();

    let cfg = WatcherConfig::new(
        head,
        Filter::new().address(contract_addr).event_signature(vec![
            HashBlacklisted::SIGNATURE_HASH,
            HashRemoved::SIGNATURE_HASH,
        ]),
        cursor_start(),
        event_poll_interval.max(Duration::from_secs(1)),
        "blacklist",
    )
    .with_from_block(from_block)
    // The initial-sync gate rides the generic loop's healthy/backoff hooks: the
    // first established cycle signals `Ok`, the first backoff signals `Err`, and
    // an initial re-scope that can't enforce every entry bails the sink into that
    // backoff (see `BlacklistSink::on_tick_complete`). Later cycles are no-ops
    // once the gate has fired (`signal` takes the sender exactly once).
    .on_established(Box::new(move || established_gate.signal(Ok(()))))
    .on_backoff(Box::new(move || {
        backoff_gate.signal(Err(
            "initial ContentBlacklist sync failed: chain RPC or cache eviction unavailable"
                .to_string(),
        ));
    }));
    // Unlike the five flush-only sinks, `BlacklistSink` must observe the *same*
    // token the loop cancels: `rescan` polls it between per-hash `eth_call`s so a
    // large deny-set re-scope yields promptly to shutdown. `spawn` mints one
    // token and hands it to the factory, so sink and loop share it (#1236).
    resumable_watcher::spawn(provider, cfg, move |shutdown| BlacklistSink {
        contract,
        operator,
        cache,
        state: WatcherState {
            known: HashSet::new(),
        },
        shutdown: shutdown.clone(),
        rescan_interval: rescan_interval.max(Duration::from_secs(1)),
        last_rescan: None,
        initial_sync,
    })
}

/// Re-scope every distinct hash in `known` (one scope `eth_call` per hash, not
/// per regional entry) and evict those now in scope. Interruptible by shutdown
/// between hashes. Returns `true` iff the pass completed with no failed
/// re-check (a shutdown-interrupted pass counts as unclean so the next tick
/// finishes it — moot in practice, since the loop exits on cancel).
async fn rescan<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    shutdown: &CancellationToken,
) -> bool
where
    P: Provider + Clone,
{
    let snapshot = state.distinct_hashes();
    let mut evicted = 0usize;
    let mut clean = true;
    for hash in snapshot {
        if shutdown.is_cancelled() {
            return false;
        }
        match recheck(contract, operator, cache, state, hash).await {
            Recheck::Evicted => evicted = evicted.saturating_add(1),
            Recheck::NoAction => {}
            Recheck::Failed => clean = false,
        }
    }
    if evicted > 0 {
        info!(evicted, "blacklist watcher evicted blacklisted blobs");
    }
    clean
}

/// Handle one live log: `HashBlacklisted` records the `(region, hash)` entry
/// and re-checks the hash; `HashRemoved` drops exactly that entry (hygiene —
/// eviction stays sticky, and same-hash entries in other regions survive).
/// Returns `true` iff a `HashBlacklisted` re-check failed and needs a prompt
/// retry.
async fn handle_log<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    log: Log,
) -> bool
where
    P: Provider + Clone,
{
    match log.topic0() {
        Some(topic) if *topic == HashBlacklisted::SIGNATURE_HASH => {
            on_blacklisted_log(contract, operator, cache, state, &log).await == Recheck::Failed
        }
        Some(topic) if *topic == HashRemoved::SIGNATURE_HASH => {
            on_removed_log(state, &log);
            false
        }
        _ => false,
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
fn on_removed_log(state: &mut WatcherState, log: &Log) {
    match HashRemoved::decode_log_data(&log.inner.data) {
        Ok(event) => {
            let hash = Hash::from_bytes(event.hash.0);
            state.remove_entry(event.region, hash);
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
    use anyhow::Context as _;

    const US: B256 = B256::repeat_byte(0x01);
    const FR: B256 = B256::repeat_byte(0x02);

    fn state() -> WatcherState {
        WatcherState {
            known: HashSet::new(),
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

    /// COMPLIANCE PIN: the blacklist watcher must full-replay from the deploy
    /// floor on every boot. A swap to a persisted resume skips past logs whose
    /// entries no longer exist in the in-memory deny-set, silently dropping them
    /// from re-scoping — serving such a hash is slashable.
    #[test]
    fn cursor_start_is_full_replay_never_persisted() {
        assert!(
            matches!(cursor_start(), CursorStart::FullReplay),
            "blacklist deny-set rebuild requires FullReplay from the deploy block"
        );
    }

    /// Removing one region's entry must not drop a surviving same-hash entry
    /// in another region — otherwise a later `updateRegion` into the surviving
    /// region (which emits no blacklist event) would never lead to eviction.
    #[test]
    fn remove_entry_is_region_scoped() {
        let hash = Hash::from_bytes([0xAB; 32]);
        let mut state = state();
        state.add_entry(US, hash);
        state.add_entry(FR, hash);

        state.remove_entry(FR, hash);

        assert!(!state.known.contains(&(FR, hash)));
        assert!(state.known.contains(&(US, hash)), "US entry must survive");
        assert_eq!(state.distinct_hashes(), vec![hash]);
    }

    #[test]
    fn remove_entry_drops_last_entry_for_hash() {
        let hash = Hash::from_bytes([0xCD; 32]);
        let mut state = state();
        state.add_entry(US, hash);

        state.remove_entry(US, hash);

        assert!(state.known.is_empty());
        assert!(state.distinct_hashes().is_empty());
    }

    /// Local eviction is sticky and region-independent, so it clears every
    /// regional entry for the hash while leaving other hashes untouched.
    #[test]
    fn drop_hash_clears_all_regions_for_that_hash_only() {
        let evicted = Hash::from_bytes([0xEE; 32]);
        let retained = Hash::from_bytes([0x11; 32]);
        let mut state = state();
        state.add_entry(US, evicted);
        state.add_entry(FR, evicted);
        state.add_entry(FR, retained);

        state.drop_hash(evicted);

        assert!(!state.known.contains(&(US, evicted)));
        assert!(!state.known.contains(&(FR, evicted)));
        assert_eq!(state.distinct_hashes(), vec![retained]);
    }

    /// The scope view is per `(operator, hash)`, so re-scoping must issue one
    /// check per distinct hash even when several regional entries share it.
    #[test]
    fn distinct_hashes_dedupes_across_regions() {
        let hash = Hash::from_bytes([0x42; 32]);
        let mut state = state();
        state.add_entry(US, hash);
        state.add_entry(FR, hash);

        assert_eq!(state.distinct_hashes(), vec![hash]);
    }
}
