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
//! The watcher runs on the shared `resumable_watcher` `eth_getLogs` poller
//! (#1092/#1106): its first tick backfills a bounded recent window and later
//! ticks are the live tail, with a background task aborted on drop.
//!
//! # Known limitations (flagged in #326 / tracked by the follow-up issue)
//!
//! - **Bounded backfill, in-memory rebuild.** [`NodeSettlementSource`] is
//!   in-memory, so on each boot the indexer rebuilds it by backfilling a bounded
//!   recent block window (`MAX_BACKFILL_BLOCK_SPAN`); settlements older than
//!   the window — and any arriving during a backoff gap — are not counted.
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
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use iroh::PublicKey;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use decdn_incentive::capacity_bond::CapacityBond;
use decdn_incentive::payment_channel::PaymentChannel;

use crate::chain_events::resumable_watcher::{
    self, CursorPolicy, LogSink, WatcherConfig, WatcherHook,
};
use crate::metrics::Metrics;
// `MAX_BACKFILL_BLOCK_SPAN` doubles as this watcher's head-anchored boot
// lookback; imported (not duplicated) so the shared per-call range cap can't
// silently diverge.
use crate::chain_events::shared_head::HeadSource;
use crate::chain_events::{
    AbortOnDrop, MAX_BACKFILL_BLOCK_SPAN, REORG_MARGIN_BLOCKS, WATCHER_INITIAL_BACKOFF,
    WATCHER_MAX_BACKOFF, timed,
};
use crate::reputation_wiring::NodeSettlementSource;

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
        event_poll_interval: Duration,
        head: Arc<dyn HeadSource>,
        metrics: Arc<Metrics>,
    ) -> Result<Self>
    where
        P: Provider + Clone + 'static,
    {
        // Fail-fast bring-up smoke check: confirm the RPC is reachable before
        // spawning the poller (the backfill floor itself is the poller's job now).
        // Deliberately a direct read, NOT the shared `head` source: a TTL-cached
        // hit would satisfy this without touching the RPC, defeating the only
        // thing this check exists to prove.
        let head_block = provider
            .get_block_number()
            .await
            .context("read head block for settlement-indexer bring-up")?;
        let capacity_bond = CapacityBond::new(capacity_bond_addr, provider.clone());
        info!(
            %payment_channel_addr,
            head_block,
            "SettlementIndexer bootstrap (network-wide ChannelSettled, getLogs poller)"
        );
        let sink = ReputationSink {
            capacity_bond,
            settlement,
            metrics: Arc::clone(&metrics),
            state: IndexerState::new(),
        };
        let cfg = WatcherConfig {
            head,
            filter: Filter::new()
                .address(payment_channel_addr)
                .event_signature(vec![
                    PaymentChannel::ChannelOpened::SIGNATURE_HASH,
                    PaymentChannel::ChannelSettled::SIGNATURE_HASH,
                ]),
            from_block: 0,
            poll_interval: event_poll_interval,
            confirmations: 0,
            reorg_margin: REORG_MARGIN_BLOCKS,
            max_backfill_span: MAX_BACKFILL_BLOCK_SPAN,
            // Bounded recent lookback each boot (in-memory rebuild; no durable
            // cursor); the live tail then flows forward from there.
            cursor: CursorPolicy::HeadMinusWindow {
                window_blocks: MAX_BACKFILL_BLOCK_SPAN,
                floor: 0,
            },
            initial_backoff: WATCHER_INITIAL_BACKOFF,
            max_backoff: WATCHER_MAX_BACKOFF,
            rpc_call_timeout: None,
            shutdown: CancellationToken::new(),
            seed_cursor: None,
            label: "reputation-indexer",
            on_established: None,
            on_backoff: Some(rpc_failure_hook(&metrics)),
        };
        let handle = tokio::spawn(resumable_watcher::run(provider, cfg, sink));
        Ok(Self {
            _watcher: AbortOnDrop(handle),
        })
    }
}

/// Wire a tick failure to the indexer's RPC-failure counter.
fn rpc_failure_hook(metrics: &Arc<Metrics>) -> WatcherHook {
    let metrics = Arc::clone(metrics);
    Box::new(move || metrics.reputation_indexer_rpc_failure())
}

/// Upper bound on parked `ChannelSettled`-before-`ChannelOpened` events (#864).
/// Bounds the out-of-order buffer's memory; on overflow the oldest parked event
/// is dropped (its client leg is recovered only if it is still within the next
/// boot's recent-window backfill — see [`park_settled`]). 4096 covers a generous
/// burst of opens/settles racing in the same poll window.
const MAX_PENDING_SETTLED: usize = 4096;

/// Upper bound on the `settled_seen` dedup set (#864). Unlike
/// `pending_settled`, eviction here carries no re-credit/backfill rearm —
/// it just forgets that a channel was already credited. The oldest entries
/// are the longest-settled, so FIFO eviction sheds exactly those. The
/// residual risk: if an evicted channel's `ChannelSettled` is then
/// re-delivered (a re-scan/backfill overlap), `process_settled` no
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
}

/// Persistent for the watcher's life (carried across poll ticks and backoff):
/// the channel→parties map, the settled-once dedup set, and the out-of-order
/// settled buffer.
struct IndexerState {
    /// `channelId → (client, provider)` learned from `ChannelOpened`.
    channels: HashMap<[u8; 32], (Address, Address)>,
    /// `channelId`s already credited, so a backfill/live overlap or a
    /// re-scan never double-counts (a channel settles exactly once).
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
}

impl IndexerState {
    fn new() -> Self {
        Self {
            channels: HashMap::new(),
            settled_seen: HashSet::new(),
            settled_seen_order: VecDeque::new(),
            pending_settled: HashMap::new(),
            pending_order: VecDeque::new(),
        }
    }
}

/// Applies network-wide `ChannelOpened`/`ChannelSettled` logs to the in-memory
/// settlement index (#1092). Backfill and the live tail are one
/// `resumable_watcher` `eth_getLogs` cursor loop (#1106): opens and settles
/// arrive block-ordered and interleaved, so a settle whose open is later in the
/// scan is parked (`pending_settled`, #864) until the open arrives. A transient
/// resolution failure returns `Err` so the tick backs off and re-scans the window
/// (`settled_seen` dedups already-credited channels); an undecodable log is
/// skipped (`Ok`) rather than hot-looping the deterministic re-scan.
struct ReputationSink<P: Provider + Clone> {
    capacity_bond: CapacityBond::CapacityBondInstance<P>,
    settlement: Arc<NodeSettlementSource>,
    metrics: Arc<Metrics>,
    state: IndexerState,
}

impl<P: Provider + Clone> LogSink for ReputationSink<P> {
    #[allow(clippy::cognitive_complexity)]
    async fn apply(&mut self, log: Log) -> Result<()> {
        match log.topic0().copied() {
            Some(sig) if sig == PaymentChannel::ChannelOpened::SIGNATURE_HASH => {
                let event = match PaymentChannel::ChannelOpened::decode_log_data(&log.inner.data) {
                    Ok(event) => event,
                    Err(err) => {
                        warn!(%err, "skipping undecodable ChannelOpened log");
                        return Ok(());
                    }
                };
                handle_opened(
                    &self.capacity_bond,
                    &self.settlement,
                    &mut self.state,
                    &self.metrics,
                    event.channelId.0,
                    event.client,
                    event.provider,
                )
                .await?;
            }
            Some(sig) if sig == PaymentChannel::ChannelSettled::SIGNATURE_HASH => {
                let event = match PaymentChannel::ChannelSettled::decode_log_data(&log.inner.data) {
                    Ok(event) => event,
                    Err(err) => {
                        warn!(%err, "skipping undecodable ChannelSettled log");
                        return Ok(());
                    }
                };
                handle_settled(
                    &self.capacity_bond,
                    &self.settlement,
                    &mut self.state,
                    &self.metrics,
                    event.channelId.0,
                    event.provider,
                    event.routedAmount,
                )
                .await?;
            }
            _ => {
                debug!(topic0 = ?log.topic0(), "unmatched PaymentChannel event in subscribed OR-set");
            }
        }
        Ok(())
    }
}

/// Live `ChannelOpened` handler: record the channel's parties, then credit any
/// `ChannelSettled` that was parked before this open arrived (#864). A transient
/// re-credit failure returns `Err` so the poll tick re-scans the window
/// (`settled_seen` dedups already-credited channels); the poller subsumes the
/// old explicit backfill re-arm.
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
        return Err(err).context("parked ChannelSettled re-credit");
    }
    Ok(())
}

/// Live `ChannelSettled` handler. Parks the event when its `ChannelOpened`
/// hasn't been observed (so the client party isn't dropped, #864); otherwise
/// credits it. A transient resolution error returns `Err` so the poll tick
/// re-scans the window (the poller subsumes the old backfill re-arm).
async fn handle_settled<P>(
    capacity_bond: &CapacityBond::CapacityBondInstance<P>,
    settlement: &Arc<NodeSettlementSource>,
    state: &mut IndexerState,
    metrics: &Arc<Metrics>,
    channel_id: [u8; 32],
    provider: Address,
    routed_amount: alloy::primitives::U256,
) -> Result<()>
where
    P: Provider + Clone,
{
    if !state.settled_seen.contains(&channel_id) && !state.channels.contains_key(&channel_id) {
        park_settled(state, channel_id, provider, routed_amount);
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
/// dropped: returning `Err` aborts the poll tick, which re-scans the window on
/// the next tick (`settled_seen` dedups anything already credited). Only a fully
/// resolved settlement — or a genuinely unresolvable one (unbound key / amount
/// overflow) — is marked seen.
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
        return Ok(()); // already credited (backfill/live overlap or re-scan)
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
/// - `Err(..)` — a *transient* RPC failure (including a [`timed`] timeout). The
///   caller propagates this to the watcher loop's backoff-and-retry rather than
///   dropping the settlement (the watcher meters it as an RPC failure on the
///   retry boundary).
///
/// Note the deliberate asymmetry with `capacity_bond_registry::ContractReads`,
/// which issues the *same* `nodeIdOf` read but counts-and-skips a failure
/// instead of failing the tick. Both are right for their watcher: a skipped
/// settlement here is a silent accounting gap in reporter weight that nothing
/// re-derives, whereas the registry's projection self-heals on the operator's
/// next event. Do not "unify" them.
async fn resolve_binding<P>(
    capacity_bond: &CapacityBond::CapacityBondInstance<P>,
    addr: Address,
) -> Result<Option<(PublicKey, bool)>>
where
    P: Provider + Clone,
{
    let resolved = timed(None, "nodeIdOf", capacity_bond.nodeIdOf(addr).call())
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

/// Park a live `ChannelSettled` whose `ChannelOpened` hasn't been observed yet
/// (#864), keeping `pending_settled` and `pending_order` in sync and bounding
/// the buffer at `MAX_PENDING_SETTLED`. On overflow the oldest parked event is
/// dropped — a bounded, rare degradation (it requires `MAX_PENDING_SETTLED`
/// opens racing their settles). The next boot's recent-window backfill
/// re-credits it if it is still within the lookback; otherwise the client leg is
/// lost. The old explicit backfill re-arm is subsumed by the poller's re-scan.
fn park_settled(
    state: &mut IndexerState,
    channel_id: [u8; 32],
    provider_addr: Address,
    routed_amount: alloy::primitives::U256,
) {
    let entry = PendingSettled {
        provider_addr,
        routed_amount,
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
        if state.pending_settled.remove(&evicted).is_some() {
            // Dropped: the poller re-scans a bounded recent window each boot, so a
            // still-in-window evicted settle is re-credited then; otherwise its
            // client leg is lost (bounded, rare — see the fn doc, #864).
            warn!(
                channel_id = %alloy::hex::encode(evicted),
                "pending-settled buffer full; evicting oldest parked settle (#864)"
            );
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

    /// A stalled `nodeIdOf` must fail the tick, not hang it — so the settlement
    /// is retried by the watcher's backoff rather than silently un-credited.
    ///
    /// This is the counterpart to `capacity_bond_registry`'s deliberately
    /// *opposite* policy for the same RPC (see `resolve_binding`'s doc). Driving
    /// the real read path is the point: a stub that returns `Err` would pass
    /// whether or not the `timed` wrap exists.
    #[tokio::test(start_paused = true)]
    async fn hanging_node_id_of_fails_the_tick_rather_than_wedging() {
        use crate::chain_events::test_support::{bounded, hanging_provider};
        let bond = CapacityBond::CapacityBondInstance::new(addr(1), hanging_provider());
        let err = bounded("resolve_binding", resolve_binding(&bond, addr(2)))
            .await
            .err()
            .map(|e| format!("{e:#}"));
        assert!(
            err.as_ref().is_some_and(|e| e.contains("timed out after")),
            "a stalled nodeIdOf must surface as a retryable Err: {err:?}"
        );
    }

    fn pk() -> PublicKey {
        SecretKey::generate().public()
    }

    fn addr(n: u8) -> Address {
        Address::from([n; 20])
    }

    fn empty_state() -> IndexerState {
        IndexerState::new()
    }

    fn cid(n: u8) -> [u8; 32] {
        [n; 32]
    }

    #[test]
    fn park_then_take_round_trips_and_keeps_order_in_sync() {
        let mut state = empty_state();
        park_settled(&mut state, cid(1), addr(1), U256::from(10));
        park_settled(&mut state, cid(2), addr(2), U256::from(20));
        assert_eq!(state.pending_settled.len(), 2);
        assert_eq!(state.pending_order.len(), 2);

        // A re-delivered settled for an already-parked channel refreshes in
        // place — it must not double-count in the FIFO order.
        park_settled(&mut state, cid(1), addr(1), U256::from(11));
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
    fn pending_buffer_is_bounded_and_evicts_oldest() {
        let mut state = empty_state();
        // Fill exactly to capacity.
        for i in 0..MAX_PENDING_SETTLED {
            let id = u32::try_from(i).expect("fits u32");
            let mut key = [0u8; 32];
            key[..4].copy_from_slice(&id.to_be_bytes());
            park_settled(&mut state, key, addr(1), U256::from(1));
        }
        assert_eq!(state.pending_settled.len(), MAX_PENDING_SETTLED);

        // One more overflows: the oldest is evicted (its credit is dropped —
        // bounded, rare; the poller's recent-window re-scan recovers an
        // in-window settle, #864). The buffer stays pinned at the cap.
        park_settled(&mut state, cid(255), addr(2), U256::from(2));
        assert_eq!(state.pending_settled.len(), MAX_PENDING_SETTLED);
        assert_eq!(state.pending_order.len(), MAX_PENDING_SETTLED);
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
