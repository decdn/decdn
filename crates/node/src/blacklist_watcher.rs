//! Blacklist compliance watcher (ADR 011 § Content Takedown, ADR 031).
//!
//! When the operator configures `blockchain.content_blacklist_address`, this
//! task keeps the local blob store compliant with `ContentBlacklist`: it evicts
//! any blob whose hash is blacklisted *in scope* for this operator (global ∪
//! current-region ∪ ripening-prev-region). The scope decision is the contract's
//! `isHashBlacklistedForOperator` view, so region packing and the ADR 030
//! ripening math never leave the chain.
//!
//! **Event-sourced, re-scoped deny-set.** The set of blacklisted entries is
//! learned from `HashBlacklisted` logs replayed from a checkpoint block (the
//! configured deployment block on first pass) plus a live subscription.
//! `ContentBlacklist` exposes no enumeration view, so events are the only
//! source. Entries are keyed by `(region, hash)` — the contract's own key
//! (`_hashEntries[region][hash]`) — so a `HashRemoved` for one region's entry
//! never drops a surviving same-hash entry in another region. Every
//! seen-but-not-yet-evicted entry is retained in `known` — *including* ones
//! currently out of scope (wrong region) or fast-track suspended — and
//! re-scoped on every periodic pass. This is essential: a hash can become
//! live + in scope with **no** `HashBlacklisted` event — an operator
//! region/ripening change (`CapacityBond.updateRegion`) or an appeal
//! reversal/lapse that clears `suspended` — and the advanced checkpoint means
//! the original log is never replayed. Re-scoping `known` is what catches those.
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
//! **Resilience.** Replay/re-scope runs every cycle regardless of the
//! subscription (`eth_getLogs`/`eth_call` work even when the filter API is
//! broken); every RPC read is bounded by a per-call timeout; the whole
//! reconcile is interruptible by shutdown (checked between windows/hashes) so a
//! large backlog cannot overrun the runtime shutdown deadline; the subscribe is
//! shutdown-raced; and clean stream-ends are throttled. Serving a blacklisted
//! hash is slashable (`SlashJudge.submitBlacklistChallenge`), so prompt eviction
//! is the node's only local protection.

use std::collections::HashSet;
use std::time::Duration;

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use decdn_cache::{CacheEngine, Hash};
use decdn_common::redact::sanitize_rpc_display;
use decdn_incentive::content_blacklist::ContentBlacklist;
use decdn_incentive::content_blacklist::ContentBlacklist::{HashBlacklisted, HashRemoved};
use futures_util::StreamExt;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain_events::watch_contract_events;

/// Backoff floor after a failed subscription or an immediately-ending stream.
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Backoff ceiling — matches the other on-chain watchers.
const MAX_BACKOFF: Duration = Duration::from_mins(1);
/// Per-call ceiling on RPC reads (scope view, subscribe, log query, head) so a
/// stalled provider — which has no request timeout configured — cannot wedge the
/// watcher.
const RPC_CALL_TIMEOUT: Duration = Duration::from_secs(10);
/// `eth_getLogs` block-range window for `HashBlacklisted` replay. Matches
/// `chain_origin_directory`'s window so range-limited RPCs work uniformly.
const REPLAY_WINDOW_BLOCKS: u64 = 9_000;
/// A live subscription must survive at least this long before its clean end
/// resets the backoff — otherwise a filter that expires on the first poll (e.g.
/// a load balancer without sticky filter routing) would spin resubscribe.
const MIN_STREAM_SURVIVAL: Duration = Duration::from_secs(30);

/// Outcome of one subscribe → follow cycle.
enum Cycle {
    /// Shutdown fired — exit the watcher.
    Shutdown,
    /// Subscription failed or the stream ended — re-subscribe (the next cycle
    /// reconciles again first, so no enforcement gap depends on the subscription).
    Resubscribe,
}

/// Outcome of following one live subscription.
enum Follow {
    Shutdown,
    StreamEnded,
}

/// Mutable watcher state carried across cycles.
struct WatcherState {
    /// Next block the `HashBlacklisted` replay resumes from (advances per
    /// successfully-queried window).
    checkpoint: u64,
    /// Every blacklisted `(region, hash)` entry seen and not yet locally
    /// evicted — including out-of-scope and suspended entries — re-scoped on
    /// each reconcile so a later region/ripening or appeal transition (which
    /// emits no `HashBlacklisted`) still leads to eviction. Keyed like the
    /// contract's `_hashEntries[region][hash]` so a `HashRemoved` drops
    /// exactly the removed entry.
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

/// Run the blacklist compliance watcher until `shutdown` is cancelled.
/// `from_block` is where the first `HashBlacklisted` replay starts (the
/// `ContentBlacklist` deployment block; `0` scans all history);
/// `poll_interval` is the periodic replay + re-scope cadence.
pub(crate) async fn run<P>(
    provider: P,
    contract_addr: Address,
    operator: Address,
    cache: CacheEngine,
    from_block: u64,
    poll_interval: Duration,
    shutdown: CancellationToken,
) where
    P: Provider + Clone,
{
    // Config rejects a zero interval, but clamp defensively: `interval_at`
    // panics on a zero period, and a panic here silently stops enforcement.
    let poll_interval = poll_interval.max(Duration::from_secs(1));
    let contract = ContentBlacklist::new(contract_addr, provider.clone());
    info!(%contract_addr, %operator, from_block, "blacklist compliance watcher starting");

    let mut state = WatcherState {
        checkpoint: from_block,
        known: HashSet::new(),
    };
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match run_cycle(
            &contract,
            operator,
            &cache,
            &mut state,
            poll_interval,
            &shutdown,
            &mut backoff,
        )
        .await
        {
            Cycle::Shutdown => return,
            Cycle::Resubscribe => {}
        }
    }
}

/// One cycle: reconcile (regardless of subscription health), then subscribe
/// (bounded + shutdown-raced), then follow the live stream with a periodic
/// reconcile backstop.
#[allow(clippy::too_many_arguments)]
async fn run_cycle<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    poll_interval: Duration,
    shutdown: &CancellationToken,
    backoff: &mut Duration,
) -> Cycle
where
    P: Provider + Clone,
{
    reconcile(contract, operator, cache, state, shutdown).await;
    if shutdown.is_cancelled() {
        return Cycle::Shutdown;
    }

    // Subscribe, bounded by RPC_CALL_TIMEOUT and raced against shutdown so a hung
    // `eth_newFilter` cannot wedge the watcher or block graceful shutdown.
    let attempt = watch_contract_events(
        contract.provider(),
        *contract.address(),
        [HashBlacklisted::SIGNATURE_HASH, HashRemoved::SIGNATURE_HASH],
    );
    let subscribed = tokio::select! {
        () = shutdown.cancelled() => return Cycle::Shutdown,
        r = tokio::time::timeout(RPC_CALL_TIMEOUT, attempt) => r,
    };
    let mut stream = match subscribed {
        Ok(Ok(stream)) => stream,
        Ok(Err(err)) => {
            warn!(
                err = %sanitize_rpc_display(&err),
                backoff_secs = backoff.as_secs(),
                "blacklist watcher subscription failed; retrying after backoff"
            );
            return backoff_then_resubscribe(backoff, shutdown).await;
        }
        Err(_elapsed) => {
            warn!(
                timeout_secs = RPC_CALL_TIMEOUT.as_secs(),
                "blacklist watcher subscribe timed out"
            );
            return backoff_then_resubscribe(backoff, shutdown).await;
        }
    };
    *backoff = INITIAL_BACKOFF;

    let started = Instant::now();
    let outcome = follow_stream(
        &mut stream,
        contract,
        operator,
        cache,
        state,
        poll_interval,
        shutdown,
    )
    .await;
    after_follow(outcome, started, backoff, shutdown).await
}

/// Sleep `*backoff` (shutdown-raced), grow it, and ask for a resubscribe.
async fn backoff_then_resubscribe(backoff: &mut Duration, shutdown: &CancellationToken) -> Cycle {
    if sleep_or_cancel(*backoff, shutdown).await {
        return Cycle::Shutdown;
    }
    *backoff = (*backoff * 2).min(MAX_BACKOFF);
    Cycle::Resubscribe
}

/// Map a [`Follow`] outcome to the next [`Cycle`], throttling a stream that ended
/// before [`MIN_STREAM_SURVIVAL`] so a first-poll filter expiry cannot spin.
async fn after_follow(
    outcome: Follow,
    started: Instant,
    backoff: &mut Duration,
    shutdown: &CancellationToken,
) -> Cycle {
    match outcome {
        Follow::Shutdown => Cycle::Shutdown,
        Follow::StreamEnded if started.elapsed() < MIN_STREAM_SURVIVAL => {
            backoff_then_resubscribe(backoff, shutdown).await
        }
        Follow::StreamEnded => Cycle::Resubscribe,
    }
}

/// Consume the live stream, evicting on each `HashBlacklisted`, and run a
/// periodic reconcile (replay + re-scope) on the side.
#[allow(clippy::too_many_arguments)]
async fn follow_stream<P, S>(
    stream: &mut S,
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    poll_interval: Duration,
    shutdown: &CancellationToken,
) -> Follow
where
    P: Provider + Clone,
    S: futures_util::Stream<Item = Log> + Unpin,
{
    // First tick one interval out — the cycle already reconciled before us.
    let mut tick = interval_at(Instant::now() + poll_interval, poll_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                debug!("blacklist watcher shutting down");
                return Follow::Shutdown;
            }
            _ = tick.tick() => reconcile(contract, operator, cache, state, shutdown).await,
            maybe_log = stream.next() => {
                match maybe_log {
                    Some(log) => handle_log(contract, operator, cache, state, log).await,
                    None => return Follow::StreamEnded,
                }
            }
        }
    }
}

/// Replay new `HashBlacklisted` logs into `known`, then re-scope the whole
/// `known` set, evicting any now in scope. Interruptible by shutdown.
async fn reconcile<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    shutdown: &CancellationToken,
) where
    P: Provider + Clone,
{
    replay(contract, cache, state, shutdown).await;
    rescan(contract, operator, cache, state, shutdown).await;
}

/// Windowed `eth_getLogs` replay of `HashBlacklisted` from `state.checkpoint` to
/// head, inserting each not-yet-evicted hash into `known`. Advances the
/// checkpoint per queried window; stops on any query error (retried next tick)
/// or shutdown.
async fn replay<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    cache: &CacheEngine,
    state: &mut WatcherState,
    shutdown: &CancellationToken,
) where
    P: Provider + Clone,
{
    let Some(head) = head_block(contract).await else {
        return;
    };
    while state.checkpoint <= head {
        if shutdown.is_cancelled() {
            return;
        }
        let from = state.checkpoint;
        let to = from.saturating_add(REPLAY_WINDOW_BLOCKS - 1).min(head);
        let Some(logs) = query_window(contract, from, to).await else {
            return; // checkpoint stays at `from`; retried next tick
        };
        for (event, _log) in logs {
            let hash = Hash::from_bytes(event.hash.0);
            if !cache.is_evicted(hash) {
                state.add_entry(event.region, hash);
            }
        }
        state.checkpoint = to.saturating_add(1);
    }
}

/// Re-scope every distinct hash in `known` (one scope `eth_call` per hash, not
/// per regional entry) and evict those now in scope. Interruptible by shutdown
/// between hashes.
async fn rescan<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    shutdown: &CancellationToken,
) where
    P: Provider + Clone,
{
    let snapshot = state.distinct_hashes();
    let mut evicted = 0usize;
    for hash in snapshot {
        if shutdown.is_cancelled() {
            return;
        }
        if recheck(contract, operator, cache, state, hash).await {
            evicted = evicted.saturating_add(1);
        }
    }
    if evicted > 0 {
        info!(evicted, "blacklist watcher evicted blacklisted blobs");
    }
}

/// Handle one live log: `HashBlacklisted` records the `(region, hash)` entry
/// and re-checks the hash; `HashRemoved` drops exactly that entry (hygiene —
/// eviction stays sticky, and same-hash entries in other regions survive).
async fn handle_log<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    log: Log,
) where
    P: Provider + Clone,
{
    match log.topic0() {
        Some(topic) if *topic == HashBlacklisted::SIGNATURE_HASH => {
            on_blacklisted_log(contract, operator, cache, state, &log).await;
        }
        Some(topic) if *topic == HashRemoved::SIGNATURE_HASH => on_removed_log(state, &log),
        _ => {}
    }
}

/// Decode a `HashBlacklisted` log, record its `(region, hash)` entry, and
/// re-check the hash.
async fn on_blacklisted_log<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    log: &Log,
) where
    P: Provider + Clone,
{
    match HashBlacklisted::decode_log_data(&log.inner.data) {
        Ok(event) => {
            let hash = Hash::from_bytes(event.hash.0);
            state.add_entry(event.region, hash);
            recheck(contract, operator, cache, state, hash).await;
        }
        Err(err) => warn!(err = %err, "blacklist watcher: undecodable HashBlacklisted log"),
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

/// Evict `hash` if in scope. Returns `true` iff an eviction was performed.
/// Out-of-scope/suspended (`Some(false)`) and RPC-error (`None`) hashes keep
/// their `known` entries for the next re-scope (callers insert before calling);
/// evicted hashes drop *all* their regional entries — eviction is sticky and
/// region-independent.
async fn recheck<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    hash: Hash,
) -> bool
where
    P: Provider + Clone,
{
    if cache.is_evicted(hash) {
        state.drop_hash(hash);
        return false;
    }
    match scope_check(contract, operator, hash).await {
        Some(true) => {
            if evict(cache, hash).await {
                state.drop_hash(hash);
                true
            } else {
                false
            }
        }
        Some(false) | None => false,
    }
}

/// Current head block, bounded by [`RPC_CALL_TIMEOUT`]. `None` on error/timeout.
async fn head_block<P>(contract: &ContentBlacklist::ContentBlacklistInstance<P>) -> Option<u64>
where
    P: Provider + Clone,
{
    match tokio::time::timeout(RPC_CALL_TIMEOUT, contract.provider().get_block_number()).await {
        Ok(Ok(head)) => Some(head),
        Ok(Err(err)) => {
            warn!(err = %sanitize_rpc_display(&err), "blacklist watcher: get_block_number failed");
            None
        }
        Err(_elapsed) => {
            warn!("blacklist watcher: get_block_number timed out");
            None
        }
    }
}

/// Query `HashBlacklisted` logs for `[from, to]`, bounded by [`RPC_CALL_TIMEOUT`].
/// `None` on error/timeout (caller leaves the checkpoint and retries next tick).
async fn query_window<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    from: u64,
    to: u64,
) -> Option<Vec<(HashBlacklisted, Log)>>
where
    P: Provider + Clone,
{
    match tokio::time::timeout(
        RPC_CALL_TIMEOUT,
        contract
            .HashBlacklisted_filter()
            .from_block(from)
            .to_block(to)
            .query(),
    )
    .await
    {
        Ok(Ok(logs)) => Some(logs),
        Ok(Err(err)) => {
            warn!(
                from, to,
                err = %sanitize_rpc_display(&err),
                "blacklist watcher: HashBlacklisted replay query failed; will retry"
            );
            None
        }
        Err(_elapsed) => {
            warn!(
                from,
                to, "blacklist watcher: HashBlacklisted replay query timed out"
            );
            None
        }
    }
}

/// `isHashBlacklistedForOperator` bounded by [`RPC_CALL_TIMEOUT`]. `None` on
/// timeout or RPC error (caller keeps the hash in `known` for the next re-scope),
/// `Some(bool)` otherwise.
async fn scope_check<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    hash: Hash,
) -> Option<bool>
where
    P: Provider + Clone,
{
    let hash_key = B256::from(*hash.as_bytes());
    match tokio::time::timeout(
        RPC_CALL_TIMEOUT,
        contract
            .isHashBlacklistedForOperator(hash_key, operator)
            .call(),
    )
    .await
    {
        Ok(Ok(flag)) => Some(flag),
        Ok(Err(err)) => {
            warn!(
                %hash,
                err = %sanitize_rpc_display(&err),
                "blacklist watcher: isHashBlacklistedForOperator failed; keeping for re-scope"
            );
            None
        }
        Err(_elapsed) => {
            warn!(
                %hash,
                timeout_secs = RPC_CALL_TIMEOUT.as_secs(),
                "blacklist watcher: isHashBlacklistedForOperator timed out; keeping for re-scope"
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

/// Wait `dur`, returning `true` if `shutdown` was cancelled first.
async fn sleep_or_cancel(dur: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        () = shutdown.cancelled() => true,
        () = tokio::time::sleep(dur) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const US: B256 = B256::repeat_byte(0x01);
    const FR: B256 = B256::repeat_byte(0x02);

    fn state() -> WatcherState {
        WatcherState {
            checkpoint: 0,
            known: HashSet::new(),
        }
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
