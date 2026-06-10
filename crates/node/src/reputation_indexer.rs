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

use std::collections::{HashMap, HashSet, VecDeque};
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

/// Upper bound on parked `ChannelSettled`-before-`ChannelOpened` events (#864).
/// Bounds the out-of-order buffer's memory; on overflow the oldest parked event
/// is dropped and the backfill is re-armed from its block so the next cycle
/// re-credits it (provider-only if its open still hasn't arrived). 4096 covers a
/// generous burst of opens/settles racing in the same poll window.
const MAX_PENDING_SETTLED: usize = 4096;

/// Upper bound on the `settled_seen` dedup set (#864). Unlike
/// `pending_settled`, eviction here carries no re-credit/backfill rearm —
/// it just forgets that a channel was already credited. The oldest entries
/// are the longest-settled, so FIFO eviction sheds exactly those. The
/// residual risk: if an evicted channel's `ChannelSettled` is then
/// re-delivered (a resubscribe/backfill overlap), `process_settled` no
/// longer short-circuits and `apply_settlement` re-credits the **provider**
/// (resolved from the never-removed on-chain `CapacityBond` binding) for
/// that amount once more — the counterparty leg is lost since the `channels`
/// entry was dropped on first credit (#864). That is a bounded
/// reputation double-count, not a no-op, but it requires both a full
/// `MAX_SETTLED_SEEN`-deep settlement history *and* a re-delivery of the
/// specific aged-out event, so 65536 keeps it astronomically unlikely while
/// capping memory. Eviction itself becomes routine once the set saturates,
/// so it stays at `debug!` rather than `warn!` to avoid per-settlement noise.
const MAX_SETTLED_SEEN: usize = 65_536;

/// A live `ChannelSettled` observed before its `ChannelOpened`, parked until the
/// open arrives so the client party's reporter credit isn't dropped (#864).
struct PendingSettled {
    provider_addr: Address,
    routed_amount: alloy::primitives::U256,
    /// Settled-event block, used to re-arm backfill if the parked event is
    /// evicted or its re-credit fails transiently.
    block: Option<u64>,
}

/// Persistent across resubscribe cycles: channel→parties map, the settled-once
/// dedup set, the out-of-order settled buffer, and whether the bring-up backfill
/// still needs to run.
struct IndexerState {
    /// `channelId → (client, provider)` learned from `ChannelOpened`.
    channels: HashMap<[u8; 32], (Address, Address)>,
    /// `channelId`s already credited, so a backfill/live overlap or a
    /// resubscribe never double-counts (a channel settles exactly once).
    /// Bounded by `MAX_SETTLED_SEEN` (#864); kept in sync with
    /// `settled_seen_order` (the FIFO eviction order) by `mark_settled_seen`.
    settled_seen: HashSet<[u8; 32]>,
    /// FIFO insertion order for `settled_seen`, so the bound evicts the
    /// longest-settled (oldest) channel ids first (#864).
    settled_seen_order: VecDeque<[u8; 32]>,
    /// Live `ChannelSettled` events seen before their `ChannelOpened` (#864),
    /// keyed by `channelId`; re-credited when the matching open arrives so the
    /// client party isn't lost. Bounded by `MAX_PENDING_SETTLED`; kept in sync
    /// with `pending_order` (the FIFO eviction order) by `take_pending` /
    /// `park_settled`.
    pending_settled: HashMap<[u8; 32], PendingSettled>,
    pending_order: VecDeque<[u8; 32]>,
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
        settled_seen_order: VecDeque::new(),
        pending_settled: HashMap::new(),
        pending_order: VecDeque::new(),
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
// One-shot backfill followed by the live select reads as a single sequence;
// splitting the backfill into its own function would obscure the bring-up flow.
#[allow(clippy::cognitive_complexity)]
#[allow(clippy::too_many_lines)]
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
                    handle_opened(
                        capacity_bond,
                        settlement,
                        state,
                        metrics,
                        event.channelId.0,
                        event.client,
                        event.provider,
                    )
                    .await?;
                }
                Some(Err(e)) => return Err(e).context("ChannelOpened stream"),
                None => return Ok(()),
            },
            ev = settled.next() => match ev {
                Some(Ok((event, log))) => {
                    handle_settled(
                        capacity_bond,
                        settlement,
                        state,
                        metrics,
                        event.channelId.0,
                        event.provider,
                        event.routedAmount,
                        log.block_number,
                    )
                    .await?;
                }
                Some(Err(e)) => return Err(e).context("ChannelSettled stream"),
                None => return Ok(()),
            },
        }
    }
}

/// Live `ChannelOpened` handler: record the channel's parties, then credit any
/// `ChannelSettled` that was parked before this open arrived (#864). A transient
/// re-credit failure re-arms the backfill from the parked event's block so it is
/// retried next cycle rather than lost.
async fn handle_opened<P>(
    capacity_bond: &CapacityBond::CapacityBondInstance<P>,
    settlement: &Arc<NodeSettlementSource>,
    state: &mut IndexerState,
    metrics: &Arc<Metrics>,
    channel_id: [u8; 32],
    client: Address,
    provider: Address,
) -> Result<()>
where
    P: Provider + Clone,
{
    state.channels.insert(channel_id, (client, provider));
    let Some(parked) = take_pending(state, &channel_id) else {
        return Ok(());
    };
    if let Err(err) = process_settled(
        capacity_bond,
        settlement,
        state,
        metrics,
        channel_id,
        parked.provider_addr,
        parked.routed_amount,
    )
    .await
    {
        if let Some(block) = parked.block {
            arm_backfill_from_block(state, block);
        }
        return Err(err).context("parked ChannelSettled re-credit");
    }
    Ok(())
}

/// Live `ChannelSettled` handler. Parks the event when its `ChannelOpened`
/// hasn't been observed (so the client party isn't dropped, #864); otherwise
/// credits it. A transient resolution error re-arms the backfill from `block`,
/// since `.watch()` resubscribes at head and never replays the event.
#[allow(clippy::too_many_arguments)]
async fn handle_settled<P>(
    capacity_bond: &CapacityBond::CapacityBondInstance<P>,
    settlement: &Arc<NodeSettlementSource>,
    state: &mut IndexerState,
    metrics: &Arc<Metrics>,
    channel_id: [u8; 32],
    provider: Address,
    routed_amount: alloy::primitives::U256,
    block: Option<u64>,
) -> Result<()>
where
    P: Provider + Clone,
{
    if !state.settled_seen.contains(&channel_id) && !state.channels.contains_key(&channel_id) {
        park_settled(state, channel_id, provider, routed_amount, block);
        return Ok(());
    }
    if let Err(err) = process_settled(
        capacity_bond,
        settlement,
        state,
        metrics,
        channel_id,
        provider,
        routed_amount,
    )
    .await
    {
        if let Some(block) = block {
            arm_backfill_from_block(state, block);
        }
        return Err(err).context("ChannelSettled live event");
    }
    Ok(())
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
        mark_settled_seen(state, channel_id);
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
    if !mark_settled_seen(state, channel_id) {
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

/// Mark a `channelId` seen for dedup, bounding the set at `MAX_SETTLED_SEEN`
/// (#864). Returns `true` when the id was newly inserted (caller should treat
/// the settlement as fresh), `false` when it was already present (a duplicate
/// — caller should no-op). On a genuinely new insert the id joins the FIFO
/// order and the oldest (longest-settled) entries are evicted until the set is
/// back under the bound; eviction is a pure dedup drop (no re-credit/backfill
/// rearm, unlike `park_settled`) — see `MAX_SETTLED_SEEN` for the bounded
/// re-credit risk a later re-delivery of an evicted id carries.
fn mark_settled_seen(state: &mut IndexerState, channel_id: [u8; 32]) -> bool {
    if !state.settled_seen.insert(channel_id) {
        return false;
    }
    state.settled_seen_order.push_back(channel_id);
    while state.settled_seen.len() > MAX_SETTLED_SEEN {
        let Some(evicted) = state.settled_seen_order.pop_front() else {
            break;
        };
        state.settled_seen.remove(&evicted);
        debug!(
            channel_id = %alloy::hex::encode(evicted),
            "settled-seen dedup set full; evicting oldest (longest-settled) entry (#864)"
        );
    }
    true
}

/// Re-arm the one-shot backfill to re-query from `block` (taking the earliest of
/// any already-armed start), so a settlement that couldn't be credited now is
/// re-attempted next cycle. `settled_seen` dedups anything already credited.
fn arm_backfill_from_block(state: &mut IndexerState, block: u64) {
    state.backfill_from = Some(match state.backfill_from {
        Some(existing) => existing.min(block),
        None => block,
    });
}

/// Park a live `ChannelSettled` whose `ChannelOpened` hasn't been observed yet
/// (#864), keeping `pending_settled` and `pending_order` in sync and bounding
/// the buffer at `MAX_PENDING_SETTLED`. On overflow the oldest parked event is
/// dropped and the backfill re-armed from its block so it isn't lost — the next
/// cycle re-credits it (provider-only if its open still hasn't arrived).
fn park_settled(
    state: &mut IndexerState,
    channel_id: [u8; 32],
    provider_addr: Address,
    routed_amount: alloy::primitives::U256,
    block: Option<u64>,
) {
    let entry = PendingSettled {
        provider_addr,
        routed_amount,
        block,
    };
    if state.pending_settled.insert(channel_id, entry).is_none() {
        // New key — append to the FIFO order. A duplicate (re-delivered settled
        // before its open) just refreshes the payload in place.
        state.pending_order.push_back(channel_id);
    }
    while state.pending_settled.len() > MAX_PENDING_SETTLED {
        let Some(evicted) = state.pending_order.pop_front() else {
            break;
        };
        if let Some(dropped) = state.pending_settled.remove(&evicted) {
            // Re-arm the backfill from the evicted event's block so it is
            // re-credited next cycle. A `None` block (should not occur for a
            // confirmed `.watch()` event) can't be re-queried, so log that the
            // credit is dropped unrecoverably rather than implying recovery.
            if let Some(block) = dropped.block {
                warn!(
                    channel_id = %alloy::hex::encode(evicted),
                    block,
                    "pending-settled buffer full; evicting oldest and re-arming backfill (#864)"
                );
                arm_backfill_from_block(state, block);
            } else {
                warn!(
                    channel_id = %alloy::hex::encode(evicted),
                    "pending-settled buffer full; evicting oldest with no block — credit dropped unrecoverably (#864)"
                );
            }
        }
    }
}

/// Remove a parked settled event from both `pending_settled` and the FIFO order,
/// returning it if present. Keeps the two structures in sync so `pending_order`
/// never accumulates stale ids.
fn take_pending(state: &mut IndexerState, channel_id: &[u8; 32]) -> Option<PendingSettled> {
    let removed = state.pending_settled.remove(channel_id);
    if removed.is_some()
        && let Some(pos) = state.pending_order.iter().position(|c| c == channel_id)
    {
        state.pending_order.remove(pos);
    }
    removed
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
    use alloy::primitives::U256;
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
            settled_seen_order: VecDeque::new(),
            pending_settled: HashMap::new(),
            pending_order: VecDeque::new(),
            backfill_from: None,
        }
    }

    fn cid(n: u8) -> [u8; 32] {
        [n; 32]
    }

    #[test]
    fn park_then_take_round_trips_and_keeps_order_in_sync() {
        let mut state = empty_state();
        park_settled(&mut state, cid(1), addr(1), U256::from(10), Some(100));
        park_settled(&mut state, cid(2), addr(2), U256::from(20), Some(101));
        assert_eq!(state.pending_settled.len(), 2);
        assert_eq!(state.pending_order.len(), 2);

        // A re-delivered settled for an already-parked channel refreshes in
        // place — it must not double-count in the FIFO order.
        park_settled(&mut state, cid(1), addr(1), U256::from(11), Some(100));
        assert_eq!(state.pending_settled.len(), 2);
        assert_eq!(state.pending_order.len(), 2);

        let taken = take_pending(&mut state, &cid(1)).expect("cid(1) was parked");
        assert_eq!(taken.provider_addr, addr(1));
        assert_eq!(taken.routed_amount, U256::from(11));
        // Both structures stay in sync — no stale id left behind in the order.
        assert_eq!(state.pending_settled.len(), 1);
        assert_eq!(state.pending_order.len(), 1);
        assert_eq!(state.pending_order.front(), Some(&cid(2)));
        assert!(take_pending(&mut state, &cid(1)).is_none());
    }

    #[test]
    fn pending_buffer_is_bounded_and_evicts_oldest_arming_backfill() {
        let mut state = empty_state();
        // Fill exactly to capacity; oldest is block 1_000.
        for i in 0..MAX_PENDING_SETTLED {
            let id = u32::try_from(i).expect("fits u32");
            let mut key = [0u8; 32];
            key[..4].copy_from_slice(&id.to_be_bytes());
            let block = 1_000 + u64::try_from(i).expect("fits u64");
            park_settled(&mut state, key, addr(1), U256::from(1), Some(block));
        }
        assert_eq!(state.pending_settled.len(), MAX_PENDING_SETTLED);
        assert!(state.backfill_from.is_none(), "no eviction yet");

        // One more overflows: the oldest (block 1_000) is evicted and the
        // backfill is re-armed from its block so its credit isn't lost.
        park_settled(&mut state, cid(255), addr(2), U256::from(2), Some(9_999));
        assert_eq!(state.pending_settled.len(), MAX_PENDING_SETTLED);
        assert_eq!(state.pending_order.len(), MAX_PENDING_SETTLED);
        assert_eq!(state.backfill_from, Some(1_000));
        // The newest entry survived; the evicted oldest is gone.
        assert!(state.pending_settled.contains_key(&cid(255)));
        let mut oldest = [0u8; 32];
        oldest[..4].copy_from_slice(&0u32.to_be_bytes());
        assert!(!state.pending_settled.contains_key(&oldest));
    }

    #[test]
    fn settled_seen_is_bounded_and_evicts_oldest() {
        // #864: the dedup set must not grow unbounded for the process
        // lifetime. Insert MAX + N distinct ids and assert the set is
        // pinned at the cap with the FIFO order kept consistent.
        let mut state = empty_state();
        let overflow = 100usize;
        for i in 0..(MAX_SETTLED_SEEN + overflow) {
            let id = u64::try_from(i).expect("fits u64");
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&id.to_be_bytes());
            assert!(mark_settled_seen(&mut state, key), "each id is new");
        }
        assert_eq!(
            state.settled_seen.len(),
            MAX_SETTLED_SEEN,
            "the dedup set is pinned at its bound"
        );
        // The set and its FIFO order stay in lockstep (no stale ids).
        assert_eq!(
            state.settled_seen_order.len(),
            MAX_SETTLED_SEEN,
            "the FIFO order tracks the set exactly"
        );
        // The first `overflow` ids (the oldest) were evicted; the newest survive.
        let mut oldest = [0u8; 32];
        oldest[..8].copy_from_slice(&0u64.to_be_bytes());
        assert!(
            !state.settled_seen.contains(&oldest),
            "the longest-settled id is evicted first"
        );
        let mut newest = [0u8; 32];
        newest[..8].copy_from_slice(
            &u64::try_from(MAX_SETTLED_SEEN + overflow - 1)
                .expect("fits u64")
                .to_be_bytes(),
        );
        assert!(
            state.settled_seen.contains(&newest),
            "the most-recently-settled id survives"
        );
    }

    #[test]
    fn mark_settled_seen_is_idempotent_for_duplicates() {
        // A re-delivered (duplicate) settlement must not double-count in
        // the FIFO order, mirroring `park_settled`'s in-place refresh.
        let mut state = empty_state();
        assert!(mark_settled_seen(&mut state, cid(1)), "first insert is new");
        assert!(
            !mark_settled_seen(&mut state, cid(1)),
            "the duplicate is reported as not-new"
        );
        assert_eq!(state.settled_seen.len(), 1);
        assert_eq!(
            state.settled_seen_order.len(),
            1,
            "a duplicate must not push a second FIFO entry"
        );
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
