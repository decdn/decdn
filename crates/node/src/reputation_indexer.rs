//! Network-wide on-chain settlement indexer (#326 follow-up, ADR 008 §Network
//! Score Aggregation).
//!
//! Reporter credibility in ADR 008 is weighted by a reporter's cumulative
//! settled USDC value across all payment channels it has settled on-chain. A
//! node observes that by indexing **every** `ChannelSettled` event on the
//! network (not just its own), correlating each to its `ChannelOpened` (which
//! carries `client` + `provider`), resolving both parties' Ethereum addresses
//! to `NodeId`s via `CapacityBond.nodeIdOf`, and feeding both into the shared
//! [`NodeSettlementSource`]. Once fed, `compute_reporter_weight` returns a
//! non-zero weight and received gossip reports actually move network scores.
//!
//! The watcher mirrors [`crate::dht::chain_staker_set`] (bootstrap → background
//! task → exponential-backoff resubscribe → `AbortOnDrop`) and the bring-up
//! backfill in [`crate::payment_settlement`].
//!
//! # Known limitations (flagged in #326 / tracked by the follow-up issue)
//!
//! - **Bounded backfill, in-memory rebuild.** [`NodeSettlementSource`] is
//!   in-memory, so on each boot the indexer rebuilds it by backfilling a bounded
//!   recent block window (`MAX_BACKFILL_BLOCK_SPAN`); settlements older than
//!   the window — and any arriving during a resubscribe gap — are not counted.
//!   Durable, full-52-week indexing is a refinement.
//! - **`settled_at` ≈ index time.** The settlement age used for exponential
//!   decay is stamped at index time rather than read from the settling block's
//!   timestamp. Within the bounded recent window this error is negligible
//!   against the 6.9-week decay half-life.
//! - **Current-membership staked proxy.** `staked_counterparty` reflects the
//!   counterparty's *current* `nodeIdOf(...).active`, not its status at
//!   settlement time (carried over from #326).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy::primitives::Address;
use alloy::providers::Provider;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use iroh::PublicKey;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::payment_channel::PaymentChannel;

use crate::metrics::Metrics;
use crate::reputation_wiring::NodeSettlementSource;

/// Maximum block span scanned for the bring-up backfill, and the lookback from
/// head used as the backfill floor. Mirrors `payment_settlement`'s bound so a
/// single `eth_getLogs` stays within typical RPC range caps.
const MAX_BACKFILL_BLOCK_SPAN: u64 = 10_000;
/// Initial resubscribe backoff after a watcher stream error.
const WATCHER_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Upper bound for the resubscribe backoff.
const WATCHER_MAX_BACKOFF: Duration = Duration::from_mins(1);

/// Aborts the watcher task on drop so a node-restart cycle never leaks a
/// chain-poll task. Same pattern as `chain_staker_set::AbortOnDrop`.
#[derive(Debug)]
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Background settlement indexer. Holds only the task handle; all observed
/// state flows into the shared [`NodeSettlementSource`] passed at bootstrap.
#[derive(Debug)]
pub struct SettlementIndexer {
    _watcher: AbortOnDrop,
}

impl SettlementIndexer {
    /// Capture the head block and spawn the indexer task. Returns once the task
    /// is running; the initial backfill happens inside the task so a transient
    /// RPC error backs off and retries rather than failing node startup.
    pub async fn bootstrap<P>(
        provider: P,
        payment_channel_addr: Address,
        capacity_bond_addr: Address,
        settlement: Arc<NodeSettlementSource>,
        metrics: Arc<Metrics>,
    ) -> Result<Self>
    where
        P: Provider + Clone + 'static,
    {
        let payment = PaymentChannel::new(payment_channel_addr, provider.clone());
        let capacity_bond = CapacityBond::new(capacity_bond_addr, provider);
        let head = payment
            .provider()
            .get_block_number()
            .await
            .context("read head block for settlement-indexer backfill")?;
        let backfill_from = head.saturating_sub(MAX_BACKFILL_BLOCK_SPAN);
        info!(
            %payment_channel_addr,
            backfill_from,
            head,
            "SettlementIndexer bootstrap (network-wide ChannelSettled)"
        );
        let handle = tokio::spawn(watcher_loop(
            payment,
            capacity_bond,
            settlement,
            backfill_from,
            metrics,
        ));
        Ok(Self {
            _watcher: AbortOnDrop(handle),
        })
    }
}

/// Persistent across resubscribe cycles: channel→parties map, the settled-once
/// dedup set, and whether the bring-up backfill still needs to run.
struct IndexerState {
    /// `channelId → (client, provider)` learned from `ChannelOpened`.
    channels: HashMap<[u8; 32], (Address, Address)>,
    /// `channelId`s already credited, so a backfill/live overlap or a
    /// resubscribe never double-counts (a channel settles exactly once).
    settled_seen: HashSet<[u8; 32]>,
    /// `Some(start)` until the one-shot bring-up backfill has run.
    backfill_from: Option<u64>,
}

async fn watcher_loop<P>(
    payment: PaymentChannel::PaymentChannelInstance<P>,
    capacity_bond: CapacityBond::CapacityBondInstance<P>,
    settlement: Arc<NodeSettlementSource>,
    backfill_from: u64,
    metrics: Arc<Metrics>,
) where
    P: Provider + Clone,
{
    let mut state = IndexerState {
        channels: HashMap::new(),
        settled_seen: HashSet::new(),
        backfill_from: Some(backfill_from),
    };
    let mut backoff = WATCHER_INITIAL_BACKOFF;
    loop {
        match run_watcher_once(&payment, &capacity_bond, &settlement, &mut state, &metrics).await {
            Ok(()) => {
                debug!("settlement-indexer stream ended cleanly; resubscribing");
                backoff = WATCHER_INITIAL_BACKOFF;
            }
            Err(err) => {
                metrics.reputation_indexer_rpc_failure();
                warn!(
                    %err,
                    backoff_secs = backoff.as_secs(),
                    "settlement-indexer RPC error; resubscribing after backoff"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(WATCHER_MAX_BACKOFF);
            }
        }
    }
}

// Backfill + two-arm event loop; the dispatch is fundamentally a few branches
// over two streams (same posture as `chain_staker_set::run_watcher_once`).
#[allow(clippy::cognitive_complexity)]
async fn run_watcher_once<P>(
    payment: &PaymentChannel::PaymentChannelInstance<P>,
    capacity_bond: &CapacityBond::CapacityBondInstance<P>,
    settlement: &Arc<NodeSettlementSource>,
    state: &mut IndexerState,
    metrics: &Arc<Metrics>,
) -> Result<()>
where
    P: Provider + Clone,
{
    let mut opened = payment
        .ChannelOpened_filter()
        .watch()
        .await
        .context("watch ChannelOpened")?
        .into_stream();
    let mut settled = payment
        .ChannelSettled_filter()
        .watch()
        .await
        .context("watch ChannelSettled")?
        .into_stream();

    // One-shot bring-up backfill: learn opens first (so settled events can find
    // their counterparty), then process settlements over the same window.
    // `backfill_from` is only cleared *after* the window completes — a transient
    // RPC error inside it returns `Err`, the watcher backs off, and the retry
    // re-runs the full backfill rather than silently skipping it.
    if let Some(start) = state.backfill_from {
        let to = payment
            .provider()
            .get_block_number()
            .await
            .context("read head for settlement backfill")?;
        if start <= to {
            let opened_logs = payment
                .ChannelOpened_filter()
                .from_block(start)
                .to_block(to)
                .query()
                .await
                .with_context(|| format!("backfill ChannelOpened over [{start}, {to}]"))?;
            for (event, _log) in opened_logs {
                state
                    .channels
                    .insert(event.channelId.0, (event.client, event.provider));
            }
            let settled_logs = payment
                .ChannelSettled_filter()
                .from_block(start)
                .to_block(to)
                .query()
                .await
                .with_context(|| format!("backfill ChannelSettled over [{start}, {to}]"))?;
            let (opened_n, settled_n) = (state.channels.len(), settled_logs.len());
            for (event, _log) in settled_logs {
                process_settled(
                    capacity_bond,
                    settlement,
                    state,
                    metrics,
                    event.channelId.0,
                    event.provider,
                    event.routedAmount,
                )
                .await?;
            }
            info!(
                start,
                to,
                opened = opened_n,
                settled = settled_n,
                "settlement-indexer backfill complete"
            );
        }
        // Reached only if every backfill RPC above succeeded; otherwise we
        // returned `Err` with `backfill_from` still set, so the retry repeats it.
        state.backfill_from = None;
    }

    // Filters established and (first cycle) backfill done — the indexer is live.
    info!("settlement-indexer event cycle established");
    loop {
        tokio::select! {
            ev = opened.next() => match ev {
                Some(Ok((event, _log))) => {
                    state
                        .channels
                        .insert(event.channelId.0, (event.client, event.provider));
                }
                Some(Err(e)) => return Err(e).context("ChannelOpened stream"),
                None => return Ok(()),
            },
            ev = settled.next() => match ev {
                Some(Ok((event, log))) => {
                    if let Err(err) = process_settled(
                        capacity_bond,
                        settlement,
                        state,
                        metrics,
                        event.channelId.0,
                        event.provider,
                        event.routedAmount,
                    )
                    .await
                    {
                        // A transient resolution error must not lose this live
                        // event: `.watch()` resubscribes at head and never
                        // replays it. Arm the backfill from this event's block
                        // (the earliest pending one) so the next cycle
                        // re-queries the window; `settled_seen` dedups anything
                        // already credited. The channel was *not* marked seen
                        // (process_settled returned before that), so it credits.
                        arm_backfill_from_log(state, &log);
                        return Err(err).context("ChannelSettled live event");
                    }
                }
                Some(Err(e)) => return Err(e).context("ChannelSettled stream"),
                None => return Ok(()),
            },
        }
    }
}

/// Resolve both parties (via chain RPC) then credit the settlement. Idempotent
/// per `channelId` (a channel settles once). Splits the chain-touching
/// resolution from the pure crediting in [`apply_settlement`] so the crediting
/// logic is unit-testable without a live chain.
///
/// A transient `nodeIdOf` RPC error is propagated as `Err` and the channel is
/// **not** marked seen, so the settlement is retried rather than silently
/// dropped: a backfill-path error leaves `backfill_from` set, and a live-path
/// error re-arms `backfill_from` from the event's block (see the live arm in
/// [`run_watcher_once`]), so either way the next cycle re-queries the window.
/// Only a fully resolved settlement — or a genuinely unresolvable one (unbound
/// key / amount overflow) — is marked seen.
async fn process_settled<P>(
    capacity_bond: &CapacityBond::CapacityBondInstance<P>,
    settlement: &Arc<NodeSettlementSource>,
    state: &mut IndexerState,
    metrics: &Arc<Metrics>,
    channel_id: [u8; 32],
    provider_addr: Address,
    routed_amount: alloy::primitives::U256,
) -> Result<()>
where
    P: Provider + Clone,
{
    if state.settled_seen.contains(&channel_id) {
        return Ok(()); // already credited (backfill/live overlap or resubscribe)
    }
    // Skip (don't saturate) an implausibly large amount: a `u128::MAX` would
    // poison `max_effective_settled_value` and drive every reporter's weight to
    // 0 (#326 review I2). Mark it seen so a re-delivery doesn't re-warn.
    let Ok(amount_usdc) = u128::try_from(routed_amount) else {
        metrics.reputation_indexer_amount_overflow();
        warn!(
            channel_id = %alloy::hex::encode(channel_id),
            %routed_amount,
            "settlement amount exceeds u128; skipping"
        );
        state.settled_seen.insert(channel_id);
        // The open→parties mapping is dead once a channel is marked seen (#864).
        state.channels.remove(&channel_id);
        return Ok(());
    };
    let client_addr = state.channels.get(&channel_id).map(|(client, _)| *client);
    let provider_node = resolve_binding(capacity_bond, provider_addr).await?;
    let client_node = match client_addr {
        Some(addr) => resolve_binding(capacity_bond, addr).await?,
        None => None,
    };
    let credited = apply_settlement(
        settlement,
        state,
        channel_id,
        amount_usdc,
        now_secs(),
        provider_addr,
        provider_node,
        client_addr,
        client_node,
    );
    if credited > 0 {
        metrics.reputation_indexer_settlements_credited(u64::from(credited));
    }
    Ok(())
}

/// Credit both parties of one already-resolved settlement (pure; no chain).
/// Idempotent per `channelId`. A party with no binding is skipped (fail-closed
/// → weight 0); a counterparty counts toward diversity only when it is itself a
/// staked node (ADR 008 §Counterparty validation). Returns the number of
/// parties credited (0, 1, or 2).
#[allow(clippy::too_many_arguments)]
fn apply_settlement(
    settlement: &Arc<NodeSettlementSource>,
    state: &mut IndexerState,
    channel_id: [u8; 32],
    amount_usdc: u128,
    now_secs: u64,
    provider_addr: Address,
    provider_node: Option<(PublicKey, bool)>,
    client_addr: Option<Address>,
    client_node: Option<(PublicKey, bool)>,
) -> u8 {
    if !state.settled_seen.insert(channel_id) {
        return 0;
    }
    // A channel settles exactly once, so its `ChannelOpened` parties mapping is
    // consumed here and never read again. Drop it so `channels` doesn't grow for
    // the whole process lifetime (#864); `settled_seen` continues to dedup. (A
    // `ChannelSettled` seen before its `ChannelOpened` finds no entry to remove;
    // re-crediting that late open is the separately-tracked #864 follow-up.)
    state.channels.remove(&channel_id);
    let mut credited = 0u8;
    if let Some((provider_pk, _)) = provider_node {
        let counterparty = client_addr.and_then(|addr| {
            client_node
                .as_ref()
                .filter(|(_, active)| *active)
                .map(|_| addr.into_array())
        });
        settlement.record_settlement(provider_pk, amount_usdc, now_secs, counterparty);
        credited += 1;
    }
    if let Some((client_pk, _)) = client_node {
        let counterparty = provider_node
            .as_ref()
            .filter(|(_, active)| *active)
            .map(|_| provider_addr.into_array());
        settlement.record_settlement(client_pk, amount_usdc, now_secs, counterparty);
        credited += 1;
    }
    credited
}

/// Resolve an Ethereum address to its bound `(NodeId, active)` via
/// `CapacityBond.nodeIdOf`.
///
/// - `Ok(Some(..))` — a live binding to a valid curve point.
/// - `Ok(None)` — *permanently* unresolvable: no binding, or the bound bytes
///   aren't a valid key. The caller may safely mark the settlement seen.
/// - `Err(..)` — a *transient* RPC failure. The caller propagates this to the
///   watcher loop's backoff-and-retry rather than dropping the settlement (the
///   watcher meters it as an RPC failure on the retry boundary).
async fn resolve_binding<P>(
    capacity_bond: &CapacityBond::CapacityBondInstance<P>,
    addr: Address,
) -> Result<Option<(PublicKey, bool)>>
where
    P: Provider + Clone,
{
    let resolved = capacity_bond
        .nodeIdOf(addr)
        .call()
        .await
        .with_context(|| format!("nodeIdOf RPC for {addr}"))?;
    let bytes = resolved.nodeId.0;
    if bytes == [0u8; 32] {
        return Ok(None);
    }
    match PublicKey::from_bytes(&bytes) {
        Ok(pk) => Ok(Some((pk, resolved.active))),
        Err(err) => {
            debug!(%addr, %err, "settlement indexer: bound nodeId is not a valid key");
            Ok(None)
        }
    }
}

/// Lower `state.backfill_from` to `log`'s block so the next watcher cycle
/// re-queries from there. Keeps the earliest pending block if one is already
/// armed. A log with no block number (should not happen for a confirmed event)
/// leaves the backfill window unchanged — the worst case is the pre-existing,
/// documented resubscribe-gap loss for that one event.
fn arm_backfill_from_log(state: &mut IndexerState, log: &alloy::rpc::types::Log) {
    if let Some(block) = log.block_number {
        state.backfill_from = Some(match state.backfill_from {
            Some(existing) => existing.min(block),
            None => block,
        });
    }
}

/// Wall-clock seconds since the Unix epoch (settlement age reference).
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use decdn_reputation::{SettlementSource, compute_reporter_weight};
    use iroh::SecretKey;

    fn pk() -> PublicKey {
        SecretKey::generate().public()
    }

    fn addr(n: u8) -> Address {
        Address::from([n; 20])
    }

    fn empty_state() -> IndexerState {
        IndexerState {
            channels: HashMap::new(),
            settled_seen: HashSet::new(),
            backfill_from: None,
        }
    }

    #[test]
    fn credits_both_parties_with_each_other_as_counterparty() {
        let src = Arc::new(NodeSettlementSource::new(5));
        let mut state = empty_state();
        let provider = pk();
        let client = pk();
        let now = now_secs();
        apply_settlement(
            &src,
            &mut state,
            [1u8; 32],
            10_000_000,
            now,
            addr(1),
            Some((provider, true)),
            Some(addr(2)),
            Some((client, true)),
        );
        // Both parties recorded one settlement.
        assert_eq!(src.settlements(provider).len(), 1);
        assert_eq!(src.settlements(client).len(), 1);
        // Each one's record names the other as a staked counterparty.
        assert_eq!(
            src.settlements(provider)
                .first()
                .expect("rec")
                .staked_counterparty,
            Some([2u8; 20])
        );
        assert_eq!(
            src.settlements(client)
                .first()
                .expect("rec")
                .staked_counterparty,
            Some([1u8; 20])
        );
    }

    #[test]
    fn settlement_drops_channels_entry_to_bound_growth() {
        // #864: the open→parties mapping must not outlive the settlement that
        // consumes it, or `channels` grows for the whole process lifetime.
        let src = Arc::new(NodeSettlementSource::new(5));
        let mut state = empty_state();
        let channel_id = [3u8; 32];
        state.channels.insert(channel_id, (addr(2), addr(1)));

        apply_settlement(
            &src,
            &mut state,
            channel_id,
            10_000_000,
            now_secs(),
            addr(1),
            Some((pk(), true)),
            Some(addr(2)),
            Some((pk(), true)),
        );

        assert!(
            !state.channels.contains_key(&channel_id),
            "channels entry must be removed once its settlement is credited"
        );
        // The settled-once dedup record persists so a re-delivery can't recredit.
        assert!(state.settled_seen.contains(&channel_id));
    }

    #[test]
    fn dedup_by_channel_id_prevents_double_count() {
        let src = Arc::new(NodeSettlementSource::new(5));
        let mut state = empty_state();
        let provider = pk();
        for _ in 0..3 {
            apply_settlement(
                &src,
                &mut state,
                [7u8; 32], // same channel id
                10_000_000,
                now_secs(),
                addr(1),
                Some((provider, true)),
                None,
                None,
            );
        }
        assert_eq!(src.settlements(provider).len(), 1, "settled-once dedup");
    }

    #[test]
    fn unresolved_party_skipped_and_unstaked_counterparty_not_credited() {
        let src = Arc::new(NodeSettlementSource::new(5));
        let mut state = empty_state();
        let provider = pk();
        // Client address known but unresolved (no binding) → client side
        // skipped, and provider's counterparty is not staked → None.
        apply_settlement(
            &src,
            &mut state,
            [9u8; 32],
            5_000_000,
            now_secs(),
            addr(1),
            Some((provider, true)),
            Some(addr(2)),
            None,
        );
        assert_eq!(src.settlements(provider).len(), 1);
        assert_eq!(
            src.settlements(provider)
                .first()
                .expect("rec")
                .staked_counterparty,
            None,
            "unresolved/unstaked counterparty earns no diversity credit"
        );
    }

    #[test]
    fn five_distinct_staked_counterparties_yield_positive_weight() {
        let src = Arc::new(NodeSettlementSource::new(5));
        let mut state = empty_state();
        let provider = pk();
        let now = now_secs();
        // Five settlements with five distinct *staked* clients → full diversity.
        for i in 0..5u8 {
            let mut channel = [0u8; 32];
            channel[0] = i;
            apply_settlement(
                &src,
                &mut state,
                channel,
                10_000_000,
                now,
                addr(100),
                Some((provider, true)),
                Some(addr(i)),
                Some((pk(), true)),
            );
        }
        let weight = compute_reporter_weight(src.as_ref(), provider, 5);
        assert!(
            weight > 0.0,
            "expected positive reporter weight, got {weight}"
        );
    }
}
