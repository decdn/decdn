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
//! boot tripping the startup preflight) is instead fixed by the preflight's
//! 429/5xx tolerance: the poller then makes windowed forward progress under
//! backoff without exiting the process. A durable deny-set (to make the resume
//! safe) is a tracked follow-up.
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
//! **Resilience.** The re-scope pass (`on_tick_complete`) runs every poll tick;
//! every RPC read is bounded by a per-call timeout; the re-scope is interruptible
//! by shutdown (checked between hashes) so a large backlog cannot overrun the
//! runtime shutdown deadline. Serving a blacklisted hash is slashable
//! (`SlashJudge.submitBlacklistChallenge`), so prompt eviction is the node's only
//! local protection.

use std::collections::HashSet;
use std::time::Duration;

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::Result;
use decdn_cache::{CacheEngine, Hash};
use decdn_common::redact::sanitize_rpc_display;
use decdn_incentive::content_blacklist::ContentBlacklist;
use decdn_incentive::content_blacklist::ContentBlacklist::{HashBlacklisted, HashRemoved};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain_events::resumable_watcher::{self, CursorPolicy, LogSink, WatcherConfig};
use crate::payment_settlement::{MAX_BACKFILL_BLOCK_SPAN, REORG_MARGIN_BLOCKS};

/// Backoff floor after a failed poll tick.
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Backoff ceiling — matches the other on-chain watchers.
const MAX_BACKOFF: Duration = Duration::from_mins(1);
/// Per-call ceiling on RPC reads (scope view, log query, head) so a stalled
/// provider — which has no request timeout configured — cannot wedge the watcher.
const RPC_CALL_TIMEOUT: Duration = Duration::from_secs(10);

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
/// returns `Err` — a scope-check RPC failure keeps the entry in `known` for the
/// next re-scope, and an undecodable log is logged and skipped.
/// [`Self::on_tick_complete`] runs the batched re-scope (bounded to the
/// operator's rescan cadence) that catches no-event scope transitions.
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
}

impl<P: Provider + Clone> LogSink for BlacklistSink<P> {
    async fn apply(&mut self, log: Log) -> Result<()> {
        handle_log(
            &self.contract,
            self.operator,
            &self.cache,
            &mut self.state,
            log,
        )
        .await;
        Ok(())
    }

    async fn on_tick_complete(&mut self) -> Result<()> {
        // Re-scope the whole deny-set on the operator's cadence (not every poll
        // tick): catches a region/ripening/appeal transition that emits no event.
        let due = self
            .last_rescan
            .is_none_or(|at| at.elapsed() >= self.rescan_interval);
        if due {
            rescan(
                &self.contract,
                self.operator,
                &self.cache,
                &mut self.state,
                &self.shutdown,
            )
            .await;
            self.last_rescan = Some(Instant::now());
        }
        Ok(())
    }
}

/// Run the blacklist compliance watcher until `shutdown` is cancelled.
/// `from_block` is where the `HashBlacklisted` replay starts (the
/// `ContentBlacklist` deploy block) — re-scanned every boot, no persisted cursor
/// (see the module header). `event_poll_interval` is the getLogs poll cadence;
/// `rescan_interval` is the batched re-scope cadence.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run<P>(
    provider: P,
    contract_addr: Address,
    operator: Address,
    cache: CacheEngine,
    from_block: u64,
    event_poll_interval: Duration,
    rescan_interval: Duration,
    shutdown: CancellationToken,
) where
    P: Provider + Clone,
{
    let contract = ContentBlacklist::new(contract_addr, provider.clone());
    info!(%contract_addr, %operator, from_block, "blacklist compliance watcher starting");

    let sink = BlacklistSink {
        contract,
        operator,
        cache,
        state: WatcherState {
            known: HashSet::new(),
        },
        shutdown: shutdown.clone(),
        rescan_interval: rescan_interval.max(Duration::from_secs(1)),
        last_rescan: None,
    };
    let cfg = WatcherConfig {
        filter: Filter::new().address(contract_addr).event_signature(vec![
            HashBlacklisted::SIGNATURE_HASH,
            HashRemoved::SIGNATURE_HASH,
        ]),
        from_block,
        poll_interval: event_poll_interval.max(Duration::from_secs(1)),
        confirmations: 0,
        reorg_margin: REORG_MARGIN_BLOCKS,
        max_backfill_span: MAX_BACKFILL_BLOCK_SPAN,
        // Full replay from the deploy floor every boot, no persisted cursor: the
        // in-memory deny-set has no enumeration source, so a resume would drop
        // still-out-of-scope entries (a compliance gap).
        cursor: CursorPolicy::FullReplay { floor: from_block },
        initial_backoff: INITIAL_BACKOFF,
        max_backoff: MAX_BACKOFF,
        rpc_call_timeout: Some(RPC_CALL_TIMEOUT),
        shutdown,
        seed_cursor: None,
        label: "blacklist",
        on_established: None,
        on_backoff: None,
    };
    resumable_watcher::run(provider, cfg, sink).await;
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

#[cfg(test)]
mod tests {
    use super::*;

    const US: B256 = B256::repeat_byte(0x01);
    const FR: B256 = B256::repeat_byte(0x02);

    fn state() -> WatcherState {
        WatcherState {
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
