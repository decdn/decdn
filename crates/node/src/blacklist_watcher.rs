//! Blacklist compliance watcher (ADR 011 § Content Takedown, ADR 031).
//!
//! When the operator configures `blockchain.content_blacklist_address`, this
//! task keeps the local blob store compliant with `ContentBlacklist`: it evicts
//! any blob whose hash is blacklisted *in scope* for this operator (global ∪
//! current-region ∪ ripening-prev-region). The scope decision is the contract's
//! `isHashBlacklistedForOperator` view, so region packing and the ADR 030
//! ripening math never leave the chain.
//!
//! Eviction is the single lever, and it cascades to every serving surface:
//! [`decdn_cache::CacheEngine::evict`] durably records the takedown (survives
//! restart via `evicted.log`), the DHT republisher drops the hash on its next
//! tick (its `is_evicted` gate), the probe handler stops signing
//! `has_blob: true` once the blob leaves the store, and the client handler
//! refuses delivery with `EvictedSinceProbe` while never re-pull-filling it.
//! Because eviction is sticky, evicting a hash the node does not currently hold
//! is still useful — it blocks a later pull of blacklisted content.
//!
//! Serving a blacklisted hash past its compliance window is slashable
//! (`SlashJudge.submitBlacklistChallenge`), so prompt eviction is the node's
//! only local protection.
//!
//! `HashRemoved` / appeal-driven un-eviction is intentionally NOT handled here:
//! eviction is sticky by design, and the resume-after-appeal path is a separate
//! follow-up (ADR 011 § resumption).

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
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

use crate::chain_events::watch_contract_events;

/// Backoff floor after a failed event subscription.
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Backoff ceiling — matches the other on-chain watchers.
const MAX_BACKOFF: Duration = Duration::from_mins(1);

/// Outcome of following one event subscription to its end.
enum Follow {
    /// `shutdown` fired — the watcher must exit.
    Shutdown,
    /// The stream ended (provider drop / server filter expiry) — re-reconcile
    /// the gap and re-subscribe.
    StreamEnded,
}

/// Run the blacklist compliance watcher until `shutdown` fires.
///
/// Reconciles the full held set once at startup (catching entries added while
/// the node was offline), then follows `HashBlacklisted` events. After any
/// stream gap (provider drop / server filter expiry / transport error) it
/// re-reconciles the full held set before re-subscribing, so a lost event can
/// never leave blacklisted content served.
pub(crate) async fn run<P>(
    provider: P,
    contract_addr: Address,
    operator: Address,
    cache: CacheEngine,
    mut shutdown: oneshot::Receiver<()>,
) where
    P: Provider + Clone,
{
    let contract = ContentBlacklist::new(contract_addr, provider.clone());
    info!(%contract_addr, %operator, "blacklist compliance watcher starting");

    // Startup reconcile: evict anything already blacklisted in scope (covers
    // entries added while offline, and re-asserts durability independent of
    // `evicted.log`).
    reconcile_all(&contract, operator, &cache).await;

    let mut backoff = INITIAL_BACKOFF;
    loop {
        match run_cycle(
            &provider,
            contract_addr,
            &contract,
            operator,
            &cache,
            &mut shutdown,
            &mut backoff,
        )
        .await
        {
            Cycle::Shutdown => return,
            Cycle::Resubscribe => {}
            Cycle::Reconcile => {
                debug!("blacklist watcher stream ended; reconciling gap before re-subscribe");
                reconcile_all(&contract, operator, &cache).await;
            }
        }
    }
}

/// Outcome of one subscribe → follow cycle.
enum Cycle {
    /// `shutdown` fired — exit the watcher.
    Shutdown,
    /// Subscription failed and the backoff already elapsed — retry without a
    /// reconcile (no events were consumed).
    Resubscribe,
    /// The stream ended after running — reconcile the gap, then retry.
    Reconcile,
}

/// Subscribe to the membership events and follow them until the stream ends,
/// shutdown fires, or the subscription itself fails (backing off in place).
#[allow(clippy::too_many_arguments)]
async fn run_cycle<P>(
    provider: &P,
    contract_addr: Address,
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    shutdown: &mut oneshot::Receiver<()>,
    backoff: &mut Duration,
) -> Cycle
where
    P: Provider + Clone,
{
    let subscription = watch_contract_events(
        provider,
        contract_addr,
        [HashBlacklisted::SIGNATURE_HASH, HashRemoved::SIGNATURE_HASH],
    )
    .await;
    let mut stream = match subscription {
        Ok(stream) => stream,
        Err(err) => {
            warn!(
                err = %sanitize_rpc_display(&err),
                backoff_secs = backoff.as_secs(),
                "blacklist watcher subscription failed; retrying after backoff"
            );
            if sleep_or_shutdown(*backoff, shutdown).await {
                return Cycle::Shutdown;
            }
            *backoff = (*backoff * 2).min(MAX_BACKOFF);
            return Cycle::Resubscribe;
        }
    };
    *backoff = INITIAL_BACKOFF;
    match follow_stream(&mut stream, contract, operator, cache, shutdown).await {
        Follow::Shutdown => Cycle::Shutdown,
        Follow::StreamEnded => Cycle::Reconcile,
    }
}

/// Consume `stream` until it ends or `shutdown` fires, dispatching each log to
/// [`handle_log`].
async fn follow_stream<P, S>(
    stream: &mut S,
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    shutdown: &mut oneshot::Receiver<()>,
) -> Follow
where
    P: Provider + Clone,
    S: futures_util::Stream<Item = Log> + Unpin,
{
    loop {
        tokio::select! {
            _ = &mut *shutdown => {
                debug!("blacklist watcher shutting down");
                return Follow::Shutdown;
            }
            maybe_log = stream.next() => {
                match maybe_log {
                    Some(log) => handle_log(contract, operator, cache, log).await,
                    None => return Follow::StreamEnded,
                }
            }
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

/// Evict every held blob that is blacklisted in scope for `operator`. Best
/// effort: a per-hash RPC or eviction error is logged and skipped rather than
/// aborting the pass (the next event or reconnect retries).
async fn reconcile_all<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
) where
    P: Provider + Clone,
{
    let held = match cache.iter_hashes().await {
        Ok(hashes) => hashes,
        Err(err) => {
            warn!(err = %err, "blacklist watcher: iter_hashes failed; skipping reconcile pass");
            return;
        }
    };
    let mut evicted = 0usize;
    for hash in held {
        if evict_if_blacklisted(contract, operator, cache, hash).await {
            evicted = evicted.saturating_add(1);
        }
    }
    if evicted > 0 {
        info!(
            evicted,
            "blacklist watcher reconcile evicted blacklisted blobs"
        );
    }
}

/// Decode one membership log and act on it: `HashBlacklisted` triggers an
/// in-scope eviction, `HashRemoved` is logged only (eviction is sticky).
async fn handle_log<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    log: Log,
) where
    P: Provider + Clone,
{
    match log.topic0() {
        Some(topic) if *topic == HashBlacklisted::SIGNATURE_HASH => {
            match HashBlacklisted::decode_log_data(&log.inner.data) {
                Ok(event) => {
                    evict_if_blacklisted(contract, operator, cache, Hash::from_bytes(event.hash.0))
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

/// If `hash` is blacklisted in scope for `operator`, evict it. Returns `true`
/// iff an eviction was performed (or the blob was already evicted).
async fn evict_if_blacklisted<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    hash: Hash,
) -> bool
where
    P: Provider + Clone,
{
    if !is_blacklisted_for_operator(contract, operator, hash).await {
        return false;
    }
    if let Err(err) = cache.evict(hash).await {
        warn!(%hash, err = %err, "blacklist watcher: evict failed; blob still served (slash risk)");
        return false;
    }
    info!(%hash, "evicted blacklisted blob (ADR 011 compliance)");
    true
}

/// Query the operator-scope predicate for `hash`. On RPC error, returns `false`
/// (leave the blob in place) after logging — the next event or reconnect retries.
async fn is_blacklisted_for_operator<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    hash: Hash,
) -> bool
where
    P: Provider + Clone,
{
    let hash_key = B256::from(*hash.as_bytes());
    match contract
        .isHashBlacklistedForOperator(hash_key, operator)
        .call()
        .await
    {
        Ok(flag) => flag,
        Err(err) => {
            warn!(
                %hash,
                err = %sanitize_rpc_display(&err),
                "blacklist watcher: isHashBlacklistedForOperator failed; leaving blob in place"
            );
            false
        }
    }
}
