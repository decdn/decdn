//! Blacklist compliance watcher (ADR 011 § Content Takedown, ADR 031).
//!
//! When the operator configures `blockchain.content_blacklist_address`, this
//! task keeps the local blob store compliant with `ContentBlacklist`: it evicts
//! any blob whose hash is blacklisted *in scope* for this operator (global ∪
//! current-region ∪ ripening-prev-region). The scope decision is the contract's
//! `isHashBlacklistedForOperator` view, so region packing and the ADR 030
//! ripening math never leave the chain.
//!
//! **Event-sourced deny-set.** The set of blacklisted hashes is learned from
//! `HashBlacklisted` logs replayed from a checkpoint block (the configured
//! deployment block on first pass), plus a live event subscription for the fast
//! path. `ContentBlacklist` exposes no enumeration view, so events are the only
//! source of truth. Replaying from a checkpoint — rather than scanning the
//! currently-held blobs — is what lets the watcher pre-block a hash that was
//! blacklisted while the node was offline and is *not yet held*: `cache.evict`
//! is sticky and works on absent hashes, so the pull-through admission gate
//! (`handlers/client.rs`, `is_evicted`) refuses a later origin fill. It also
//! makes each pass O(blacklist) rather than O(held).
//!
//! Eviction is the single lever, and it cascades to every serving surface:
//! [`decdn_cache::CacheEngine::evict`] durably records the takedown (survives
//! restart via `evicted.log`), the DHT republisher drops the hash on its next
//! tick (its `is_evicted` gate), the probe handler stops signing
//! `has_blob: true` once the blob leaves the store, and the client handler
//! refuses delivery with `EvictedSinceProbe` while never re-pull-filling it.
//!
//! **Resilience.** The replay/reconcile runs every cycle *regardless of the
//! subscription* (it uses `eth_getLogs`, so an endpoint whose filter API is
//! broken still enforces), the subscribe await is bounded and raced against
//! shutdown, a per-hash scope check that errors is queued for retry on the next
//! periodic tick, and clean stream-ends are throttled so a filter that expires
//! on the first poll can't spin. Serving a blacklisted hash is slashable
//! (`SlashJudge.submitBlacklistChallenge`), so prompt eviction is the node's
//! only local protection.
//!
//! `HashRemoved` / appeal-driven un-eviction is intentionally NOT handled here:
//! eviction is sticky by design, and the resume-after-appeal path is a separate
//! follow-up (ADR 011 § resumption).

use std::collections::HashSet;
use std::time::Duration;

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::sol_types::SolEvent;
use decdn_cache::{CacheEngine, Hash};
use decdn_common::redact::sanitize_rpc_display;
use decdn_incentive::content_blacklist::ContentBlacklist;
use decdn_incentive::content_blacklist::ContentBlacklist::{HashBlacklisted, HashRemoved};
use futures_util::StreamExt;
use tokio::sync::oneshot;
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tracing::{debug, info, warn};

use crate::chain_events::watch_contract_events;

/// Backoff floor after a failed subscription or an immediately-ending stream.
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Backoff ceiling — matches the other on-chain watchers.
const MAX_BACKOFF: Duration = Duration::from_mins(1);
/// Cadence of the periodic replay + scope-retry that backstops the live event
/// stream. Matches ADR 011 §Polling's 10-minute `getBlacklistVersion` cadence.
const RECONCILE_INTERVAL: Duration = Duration::from_mins(10);
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
    /// `shutdown` fired — exit the watcher.
    Shutdown,
    /// Subscription failed or the stream ended — re-subscribe (the next cycle
    /// replays again first, so no enforcement gap depends on the subscription).
    Resubscribe,
}

/// Outcome of following one live subscription.
enum Follow {
    Shutdown,
    StreamEnded,
}

/// Mutable watcher state carried across cycles: the next block to replay from,
/// and hashes whose scope check errored and must be retried.
struct WatcherState {
    /// Next block the `HashBlacklisted` replay resumes from (advances per
    /// successfully-queried window).
    checkpoint: u64,
    /// Hashes seen in an event/replay whose scope check hit a transient RPC
    /// error — retried on each periodic tick until resolved (bounded by the
    /// blacklist size, since already-evicted hashes are short-circuited).
    pending: HashSet<Hash>,
}

/// Run the blacklist compliance watcher until `shutdown` fires. `from_block` is
/// where the first `HashBlacklisted` replay starts (the `ContentBlacklist`
/// deployment block; `0` scans all history).
pub(crate) async fn run<P>(
    provider: P,
    contract_addr: Address,
    operator: Address,
    cache: CacheEngine,
    from_block: u64,
    mut shutdown: oneshot::Receiver<()>,
) where
    P: Provider + Clone,
{
    let contract = ContentBlacklist::new(contract_addr, provider.clone());
    info!(%contract_addr, %operator, from_block, "blacklist compliance watcher starting");

    let mut state = WatcherState {
        checkpoint: from_block,
        pending: HashSet::new(),
    };
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match run_cycle(
            &contract,
            operator,
            &cache,
            &mut state,
            &mut shutdown,
            &mut backoff,
        )
        .await
        {
            Cycle::Shutdown => return,
            Cycle::Resubscribe => {}
        }
    }
}

/// One cycle: replay+retry regardless of subscription health (#compliance under
/// a broken filter API), then subscribe (bounded + shutdown-raced), then follow
/// the live stream with a periodic replay/retry backstop.
async fn run_cycle<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    shutdown: &mut oneshot::Receiver<()>,
    backoff: &mut Duration,
) -> Cycle
where
    P: Provider + Clone,
{
    // Enforce first, independent of the subscription: replay new blacklist logs
    // and retry any queued scope checks. `eth_getLogs` works even when the
    // filter API used by the subscription does not.
    reconcile(contract, operator, cache, state).await;

    // Subscribe, bounded by RPC_CALL_TIMEOUT and raced against shutdown so a hung
    // `eth_newFilter` cannot wedge the watcher or block graceful shutdown.
    let attempt = watch_contract_events(
        contract.provider(),
        *contract.address(),
        [HashBlacklisted::SIGNATURE_HASH, HashRemoved::SIGNATURE_HASH],
    );
    let subscribed = tokio::select! {
        _ = &mut *shutdown => return Cycle::Shutdown,
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
    let outcome = follow_stream(&mut stream, contract, operator, cache, state, shutdown).await;
    after_follow(outcome, started, backoff, shutdown).await
}

/// Map a [`Follow`] outcome to the next [`Cycle`]. A stream that ended before
/// [`MIN_STREAM_SURVIVAL`] is throttled (a filter that expires on the first poll
/// must not spin); a longer-lived one resubscribes immediately.
async fn after_follow(
    outcome: Follow,
    started: Instant,
    backoff: &mut Duration,
    shutdown: &mut oneshot::Receiver<()>,
) -> Cycle {
    match outcome {
        Follow::Shutdown => Cycle::Shutdown,
        Follow::StreamEnded if started.elapsed() < MIN_STREAM_SURVIVAL => {
            backoff_then_resubscribe(backoff, shutdown).await
        }
        Follow::StreamEnded => Cycle::Resubscribe,
    }
}

/// Sleep `*backoff` (shutdown-raced), grow it, and ask for a resubscribe. Returns
/// `Cycle::Shutdown` if shutdown fired during the sleep.
async fn backoff_then_resubscribe(
    backoff: &mut Duration,
    shutdown: &mut oneshot::Receiver<()>,
) -> Cycle {
    if sleep_or_shutdown(*backoff, shutdown).await {
        return Cycle::Shutdown;
    }
    *backoff = (*backoff * 2).min(MAX_BACKOFF);
    Cycle::Resubscribe
}

/// Consume the live stream, evicting on each `HashBlacklisted`, and run a
/// periodic replay + scope-retry on the side. Returns when shutdown fires or the
/// stream ends.
async fn follow_stream<P, S>(
    stream: &mut S,
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    shutdown: &mut oneshot::Receiver<()>,
) -> Follow
where
    P: Provider + Clone,
    S: futures_util::Stream<Item = alloy::rpc::types::Log> + Unpin,
{
    // First tick one interval out — the cycle already reconciled before us.
    let mut tick = interval_at(Instant::now() + RECONCILE_INTERVAL, RECONCILE_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = &mut *shutdown => {
                debug!("blacklist watcher shutting down");
                return Follow::Shutdown;
            }
            _ = tick.tick() => reconcile(contract, operator, cache, state).await,
            maybe_log = stream.next() => {
                match maybe_log {
                    Some(log) => handle_log(contract, operator, cache, state, log).await,
                    None => return Follow::StreamEnded,
                }
            }
        }
    }
}

/// Replay `HashBlacklisted` logs from the checkpoint to head (advancing the
/// checkpoint per queried window) and retry any pending scope checks.
async fn reconcile<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
) where
    P: Provider + Clone,
{
    replay(contract, operator, cache, state).await;
    drain_pending(contract, operator, cache, state).await;
}

/// Windowed `eth_getLogs` replay of `HashBlacklisted` from `state.checkpoint` to
/// head. On any query error the pass stops with the checkpoint at the last
/// successful window (retried next tick); per-hash scope errors are queued.
async fn replay<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
) where
    P: Provider + Clone,
{
    let Some(head) = head_block(contract).await else {
        return;
    };
    let mut evicted = 0usize;
    while state.checkpoint <= head {
        let from = state.checkpoint;
        let to = from.saturating_add(REPLAY_WINDOW_BLOCKS - 1).min(head);
        let Some(logs) = query_window(contract, from, to).await else {
            return; // checkpoint stays at `from`; retried next tick
        };
        for (event, _log) in logs {
            if evict_scoped(
                contract,
                operator,
                cache,
                Hash::from_bytes(event.hash.0),
                state,
            )
            .await
            {
                evicted = evicted.saturating_add(1);
            }
        }
        state.checkpoint = to.saturating_add(1);
    }
    if evicted > 0 {
        info!(
            evicted,
            "blacklist watcher replay evicted blacklisted blobs"
        );
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
) -> Option<Vec<(HashBlacklisted, alloy::rpc::types::Log)>>
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

/// Retry the queued scope checks via [`evict_scoped`], which already removes a
/// hash once resolved (evicted or out-of-scope) and keeps it on a repeated error.
async fn drain_pending<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
) where
    P: Provider + Clone,
{
    if state.pending.is_empty() {
        return;
    }
    let retry: Vec<Hash> = state.pending.iter().copied().collect();
    for hash in retry {
        evict_scoped(contract, operator, cache, hash, state).await;
    }
}

/// Handle one live log: `HashBlacklisted` evicts in-scope; `HashRemoved` logs.
async fn handle_log<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    state: &mut WatcherState,
    log: alloy::rpc::types::Log,
) where
    P: Provider + Clone,
{
    match log.topic0() {
        Some(topic) if *topic == HashBlacklisted::SIGNATURE_HASH => {
            match HashBlacklisted::decode_log_data(&log.inner.data) {
                Ok(event) => {
                    evict_scoped(
                        contract,
                        operator,
                        cache,
                        Hash::from_bytes(event.hash.0),
                        state,
                    )
                    .await;
                }
                Err(err) => {
                    warn!(err = %err, "blacklist watcher: undecodable HashBlacklisted log");
                }
            }
        }
        Some(topic) if *topic == HashRemoved::SIGNATURE_HASH => {
            if let Ok(event) = HashRemoved::decode_log_data(&log.inner.data) {
                debug!(
                    hash = %Hash::from_bytes(event.hash.0),
                    "blacklist entry removed on-chain (local eviction stays sticky)"
                );
            }
        }
        _ => {}
    }
}

/// Evict `hash` if it is blacklisted in scope for `operator`. Short-circuits
/// already-evicted hashes (free), queues the hash for retry on a scope-check
/// error, and returns `true` iff an eviction was performed.
async fn evict_scoped<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    hash: Hash,
    state: &mut WatcherState,
) -> bool
where
    P: Provider + Clone,
{
    if cache.is_evicted(hash) {
        state.pending.remove(&hash);
        return false;
    }
    match scope_check(contract, operator, hash).await {
        Some(true) => {
            let done = evict(cache, hash).await;
            if done {
                state.pending.remove(&hash);
            } else {
                state.pending.insert(hash);
            }
            done
        }
        Some(false) => {
            state.pending.remove(&hash);
            false
        }
        None => {
            state.pending.insert(hash);
            false
        }
    }
}

/// `isHashBlacklistedForOperator` bounded by [`RPC_CALL_TIMEOUT`]. `None` on
/// timeout or RPC error (caller queues for retry), `Some(bool)` otherwise.
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
                "blacklist watcher: isHashBlacklistedForOperator failed; queued for retry"
            );
            None
        }
        Err(_elapsed) => {
            warn!(
                %hash,
                timeout_secs = RPC_CALL_TIMEOUT.as_secs(),
                "blacklist watcher: isHashBlacklistedForOperator timed out; queued for retry"
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

/// Sleep for `dur`, returning `true` if `shutdown` fired first.
async fn sleep_or_shutdown(dur: Duration, shutdown: &mut oneshot::Receiver<()>) -> bool {
    tokio::select! {
        _ = &mut *shutdown => true,
        () = tokio::time::sleep(dur) => false,
    }
}
