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
/// Cadence of the periodic re-reconcile that backstops the event stream. Matches
/// ADR 011 §Polling's 10-minute `getBlacklistVersion` cadence: it retries hashes
/// whose scope check hit a transient RPC error and is defense-in-depth for any
/// event the stream could still miss.
const RECONCILE_INTERVAL: Duration = Duration::from_mins(10);
/// Per-call ceiling on the `isHashBlacklistedForOperator` view so a stalled RPC
/// provider cannot hang a reconcile scan (the provider has no request timeout).
const RPC_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Outcome of one subscribe → follow cycle.
enum Cycle {
    /// `shutdown` fired — exit the watcher.
    Shutdown,
    /// Subscription failed, or the stream ended after running — re-subscribe
    /// (the next cycle reconciles again right after the filter is installed).
    Resubscribe,
}

/// Run the blacklist compliance watcher until `shutdown` fires.
///
/// Each cycle establishes the event subscription first, then reconciles the full
/// held set, then follows the stream. Reconciling *after* the server-side filter
/// is installed (which buffers from that moment) closes the race where an event
/// emitted between a reconcile and a later subscribe would be lost — covering
/// startup, stream gaps, and repeated subscription failures uniformly. While
/// following, a periodic re-reconcile retries any hash whose scope check hit a
/// transient RPC error.
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
        }
    }
}

/// Subscribe to the membership events, reconcile the held set once the filter is
/// installed, then follow the stream — until it ends, shutdown fires, or the
/// subscription itself fails (backing off in place).
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

    // Reconcile only after the filter is installed (and thus buffering): any
    // event emitted during the reconcile lands in `stream` rather than a gap.
    reconcile_all(contract, operator, cache).await;

    follow_stream(&mut stream, contract, operator, cache, shutdown).await
}

/// Consume `stream` until it ends or `shutdown` fires, dispatching each log to
/// [`handle_log`] and running a periodic re-reconcile on the side. Returns the
/// next [`Cycle`] to take.
async fn follow_stream<P, S>(
    stream: &mut S,
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    cache: &CacheEngine,
    shutdown: &mut oneshot::Receiver<()>,
) -> Cycle
where
    P: Provider + Clone,
    S: futures_util::Stream<Item = Log> + Unpin,
{
    // First tick fires one interval out, not immediately — the cycle already
    // reconciled before calling us.
    let mut reconcile = tokio::time::interval_at(
        tokio::time::Instant::now() + RECONCILE_INTERVAL,
        RECONCILE_INTERVAL,
    );
    loop {
        tokio::select! {
            _ = &mut *shutdown => {
                debug!("blacklist watcher shutting down");
                return Cycle::Shutdown;
            }
            _ = reconcile.tick() => {
                reconcile_all(contract, operator, cache).await;
            }
            maybe_log = stream.next() => {
                match maybe_log {
                    Some(log) => handle_log(contract, operator, cache, log).await,
                    None => return Cycle::Resubscribe,
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

/// Query the operator-scope predicate for `hash`, bounded by [`RPC_CALL_TIMEOUT`]
/// so a stalled provider cannot hang the reconcile scan. On timeout or RPC error
/// returns `false` (leave the blob in place) after logging — the next event or
/// the periodic re-reconcile retries.
async fn is_blacklisted_for_operator<P>(
    contract: &ContentBlacklist::ContentBlacklistInstance<P>,
    operator: Address,
    hash: Hash,
) -> bool
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
        Ok(Ok(flag)) => flag,
        Ok(Err(err)) => {
            warn!(
                %hash,
                err = %sanitize_rpc_display(&err),
                "blacklist watcher: isHashBlacklistedForOperator failed; leaving blob in place"
            );
            false
        }
        Err(_elapsed) => {
            warn!(
                %hash,
                timeout_secs = RPC_CALL_TIMEOUT.as_secs(),
                "blacklist watcher: isHashBlacklistedForOperator timed out; leaving blob in place"
            );
            false
        }
    }
}
