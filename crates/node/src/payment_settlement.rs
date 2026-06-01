//! On-chain `PaymentChannel` seller-settlement service (#327).
//!
//! The node is the *provider* (seller) on its channels. This service closes
//! the on-chain half of the paid-delivery loop that the off-chain voucher
//! layer ([`decdn_incentive`]) and the `cdn/client/v1` handler leave open:
//!
//! - **Channel lifecycle watcher.** Follows `ChannelOpened` (filtered to
//!   channels where this node is the provider) and persists a fresh
//!   [`ChannelState`] via [`ClientHandler::register_open_channel`] so the
//!   voucher handler will accept vouchers for it — without this, a
//!   freshly-opened channel is unknown to the handler and every voucher is
//!   rejected (the #327 gap noted in [`crate::handlers`]'s client module).
//!   `ChannelToppedUp` raises the tracked deposit via
//!   [`ClientHandler::update_channel_deposit`] so post-top-up vouchers are not
//!   wrongly rejected. On `ChannelSettled` it forgets the channel via
//!   [`ClientHandler::forget_channel`]; `ChannelCloseInitiated` is
//!   observed-only (the in-process dispute monitor is deferred — issue #324).
//!   The watcher uses `.watch()` filters, which only deliver logs from the
//!   block the filter is installed at forward. To close the sub-millisecond
//!   bring-up race between `bootstrap` returning and those filters installing
//!   (#762), the first watcher cycle records the head block `S` captured at
//!   bootstrap, installs the live filters, reads the block `F` they took over
//!   at, and `get_logs`-backfills `ChannelOpened` over `[S, F]` — so the
//!   backfilled `[S, F]` and the live stream (which starts at-or-before `F`)
//!   together leave no gap. The overlap around `F` is harmless because
//!   [`ClientHandler::register_open_channel`] is idempotent.
//!
//!   Scope of the backfill: **only `ChannelOpened`**. A `ChannelToppedUp` or
//!   `ChannelSettled` emitted inside `[S, F]` is not back-filled, so a channel
//!   both opened and topped-up/settled within that window could carry a stale
//!   tracked deposit or be re-registered after settlement. In practice the
//!   window is sub-second (and a settle is additionally gated by the on-chain
//!   dispute window, far longer than any bring-up), so this is negligible;
//!   ordered multi-event backfill belongs with the across-restart work (#751).
//!   A channel opened while this node was fully **down** (before `S`) is
//!   likewise not back-filled — that across-restart downtime gap needs the
//!   last-scanned block persisted across restarts (#751).
//! - **Redemption (threshold + on-shutdown).** On a redeem hint emitted by
//!   the voucher-accept path, it reads the latest persisted voucher and the
//!   on-chain `withdrawnAmount`, and submits `withdraw` once the accrued
//!   un-redeemed amount crosses a configurable threshold (a monotonic,
//!   client-signed claim needs no dispute window — ADR 003 § Operator early
//!   withdrawal). On graceful shutdown it best-effort `closeChannel`s every
//!   tracked channel that still carries an un-redeemed claim, starting the
//!   dispute window.
//! - **Settlement sweep.** `closeChannel` only *opens* the dispute window — it
//!   does not pay the provider; the un-withdrawn remainder
//!   (`claimedAmount - withdrawnAmount`) is routed only when `settleChannel`
//!   runs after `disputeDeadline`. The client is normally incentivized to
//!   settle (to reclaim `deposit - claimedAmount`), but when it drew the full
//!   deposit (`clientRefund == 0`) nobody is. So every close (shutdown or
//!   pre-expiry sweep) best-effort records a durable [`PendingSettle`] entry,
//!   and a periodic sweep calls `settleChannel` for entries whose window has
//!   elapsed — finalizing the provider's remainder without relying on the
//!   client (PR #743 review). Entries survive restarts and are dropped once a
//!   `ChannelSettled` event (from any party) confirms finalization.
//!
//! Buyer-side `openChannel` (node→node cache-miss pulls) and the in-process
//! dispute monitor (challenging a client's stale close, #324) are out of scope
//! here. Structurally this mirrors
//! [`crate::dht::chain_staker_set`]: a generic-over-`Provider` struct owning
//! `AbortOnDrop` background tasks with exponential-backoff resubscription.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::Provider;
use anyhow::{Context, Result};
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{
    ChannelId, ChannelState, ChannelStateStore, PendingSettle, PendingSettleStore,
};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::handlers::client::ClientHandler;

/// Capacity of the redeem-hint channel. Hints are advisory (a missed hint
/// only delays a redemption until the next voucher or shutdown), so a bounded
/// channel that drops on overflow is acceptable — sized for a burst of
/// concurrent channels without backpressuring the voucher-accept path.
pub const REDEEM_HINT_CAPACITY: usize = 256;

/// Backoff between watcher restart attempts after an event stream errors.
/// Mirrors [`crate::dht::chain_staker_set`]'s watcher policy.
const WATCHER_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const WATCHER_MAX_BACKOFF: Duration = Duration::from_mins(1);

/// How often the expiry sweep scans tracked channels (#327). Channel
/// lifetimes are long (default 90 days), so an hourly scan is ample.
const EXPIRY_SWEEP_INTERVAL: Duration = Duration::from_hours(1);

/// How far ahead of a channel's on-chain expiry the sweep closes it. Must be
/// comfortably larger than [`EXPIRY_SWEEP_INTERVAL`] so the close window is
/// never missed between scans. After `expiresAt` the contract reverts
/// `withdraw`/`closeChannel` and the client can `reclaimExpired` (full
/// refund), so closing early is what protects the provider's earned-but-
/// un-redeemed balance.
const EXPIRY_CLOSE_AHEAD_SECS: u64 = 6 * 3_600;

/// Ethereum recovery-id offset the on-chain `ECDSA.recover` requires (`v` ∈
/// {`27`, `28`}). The stored signature comes from
/// [`alloy::primitives::Signature::as_bytes`], which already encodes `v` as
/// `27 + y_parity`, so [`normalize_voucher_signature`] is a no-op on
/// well-formed signatures today — it exists as defense-in-depth in case the
/// stored/wire encoding ever switches to a raw `0`/`1` y-parity (e.g. an
/// `as_rsy`-style source).
const ETH_V_OFFSET: u8 = 27;

/// Current Unix time in seconds for on-chain expiry comparisons. A broken
/// system clock (time before the epoch) yields `0`, which makes every channel
/// look not-yet-expired — the safe direction (no spurious early closes / serve
/// refusals).
pub(crate) fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Whether a channel has reached its on-chain expiry. `expires_at == 0` means
/// "untracked / never expires" (v1-hydrated records, channels constructed
/// without a chain source) and is treated as not expired — the safe direction
/// (keep serving) when expiry is unknown.
pub(crate) const fn is_expired(now: u64, expires_at: u64) -> bool {
    expires_at != 0 && now >= expires_at
}

/// Whether a channel is within the pre-expiry close-ahead window and should be
/// proactively closed. `expires_at == 0` (untracked) is never in-window.
const fn within_close_window(now: u64, expires_at: u64) -> bool {
    expires_at != 0 && now.saturating_add(EXPIRY_CLOSE_AHEAD_SECS) >= expires_at
}

/// Whether a closed channel's dispute window has elapsed, so `settleChannel`
/// will not revert with `DisputeWindowActive`. `settle_after` is the on-chain
/// `disputeDeadline`; the contract requires `block.timestamp >= disputeDeadline`
/// to finalize (`contracts/src/PaymentChannel.sol` `settleChannel`).
const fn ready_to_settle(now: u64, settle_after: u64) -> bool {
    now >= settle_after
}

/// Validate the bring-up backfill range `[from, to]` (#762). On a consistent
/// chain the head is monotonic, so `to` (read on the first watcher cycle) is
/// always `>=` `from` (the head captured at bootstrap); `from == to` is a valid
/// single-block range that must still be scanned (a `ChannelOpened` can sit in
/// that exact block). `from > to` is an anomaly — RPC replication lag (a
/// load-balanced endpoint answering from a stale node) or a reorg — returned as
/// an `Err` so the caller retries via the watcher backoff rather than skipping
/// the backfill (which would permanently reopen the race once the lagging node
/// catches up).
fn check_backfill_range(from: u64, to: u64) -> Result<()> {
    if from > to {
        anyhow::bail!(
            "backfill range invalid: from_block ({from}) > to_block ({to}); \
             likely RPC replication lag or a reorg — retrying via watcher backoff"
        );
    }
    Ok(())
}

/// Aborts the wrapped task on drop so a node-restart cycle never leaks a
/// chain-poll task. Same pattern as `chain_staker_set::AbortOnDrop`.
#[derive(Debug)]
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Seller-side `PaymentChannel` settlement service. Generic over the alloy
/// [`Provider`] (a wallet-filled provider is required for the `withdraw` /
/// `closeChannel` write path). Cheap to construct; owns its background tasks.
pub struct PaymentChannelService<P: Provider + Clone + 'static> {
    contract: PaymentChannel::PaymentChannelInstance<P>,
    store: Arc<dyn ChannelStateStore>,
    pending_store: Arc<dyn PendingSettleStore>,
    redeem_tx: mpsc::Sender<ChannelId>,
    _watcher: AbortOnDrop,
    _redeemer: AbortOnDrop,
    _sweeper: AbortOnDrop,
}

impl<P: Provider + Clone + 'static> PaymentChannelService<P> {
    /// Bootstrap the service: self-check the contract, read the immutable USDC
    /// token (used to stamp `ChannelState.token`, since `ChannelOpened` omits
    /// it), and spawn the lifecycle watcher + redemption tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if the `usdc()` self-check call fails — a bad
    /// `payment_channel_address` or an unreachable RPC is fatal at bring-up,
    /// matching the staker-set bootstrap's fail-fast posture.
    pub async fn bootstrap(
        provider: P,
        payment_channel_addr: Address,
        self_address: Address,
        store: Arc<dyn ChannelStateStore>,
        pending_store: Arc<dyn PendingSettleStore>,
        handler: Arc<ClientHandler>,
        redeem_threshold: U256,
    ) -> Result<Self> {
        let contract = PaymentChannel::new(payment_channel_addr, provider);

        // Startup self-check: a cheap immutable view confirms the configured
        // address actually hosts the contract (and yields the settlement
        // token). Same fail-fast spirit as `check_rpc_reachability`.
        let usdc_token = contract.usdc().call().await.with_context(|| {
            format!("PaymentChannel.usdc() self-check at {payment_channel_addr}")
        })?;
        // Head block at bootstrap. The watcher backfills `ChannelOpened` from
        // here up to the block its live filters install at, closing the
        // bring-up race (#762). A read failure is fatal at bring-up, matching
        // the `usdc()` self-check's fail-fast posture.
        let start_block = contract
            .provider()
            .get_block_number()
            .await
            .context("read head block for watcher backfill at bootstrap")?;
        info!(
            %payment_channel_addr,
            %usdc_token,
            %self_address,
            start_block,
            "PaymentChannel settlement service bootstrap complete"
        );

        let (redeem_tx, redeem_rx) = mpsc::channel(REDEEM_HINT_CAPACITY);

        let watcher = tokio::spawn(watcher_loop(
            contract.clone(),
            self_address,
            usdc_token,
            Arc::clone(&handler),
            Arc::clone(&pending_store),
            start_block,
        ));
        let redeemer = tokio::spawn(redeemer_loop(
            contract.clone(),
            Arc::clone(&store),
            self_address,
            redeem_threshold,
            redeem_rx,
        ));
        let sweeper = tokio::spawn(sweeper_loop(
            contract.clone(),
            Arc::clone(&store),
            Arc::clone(&pending_store),
            handler,
            self_address,
        ));

        Ok(Self {
            contract,
            store,
            pending_store,
            redeem_tx,
            _watcher: AbortOnDrop(watcher),
            _redeemer: AbortOnDrop(redeemer),
            _sweeper: AbortOnDrop(sweeper),
        })
    }

    /// Sender the voucher-accept path uses to hint that a channel's accrued
    /// claim may have crossed the redemption threshold. Cloneable; dropping
    /// all senders simply ends the redemption task cleanly.
    #[must_use]
    pub fn redeem_hint_sender(&self) -> mpsc::Sender<ChannelId> {
        self.redeem_tx.clone()
    }

    /// Best-effort, bounded-by-`deadline` `closeChannel` for every tracked
    /// channel that still carries an un-redeemed claim. Called from the
    /// runtime shutdown sequence after the router has drained, so the
    /// persisted state is final. A close starts the dispute window; a later
    /// `settleChannel` (callable by anyone) finalizes it.
    pub async fn close_open_channels_on_shutdown(&self, deadline: Duration) {
        let Ok(closed) = tokio::time::timeout(deadline, self.close_all_unredeemed()).await else {
            warn!(
                deadline_secs = deadline.as_secs(),
                "shutdown channel-close deadline elapsed; some channels left open (settle later)"
            );
            return;
        };
        if closed > 0 {
            info!(closed, "closed unredeemed channels on shutdown");
        }
    }

    /// Iterate persisted channels and close any with an un-redeemed,
    /// still-open on-chain claim. Returns the number of `closeChannel`
    /// transactions that landed. Errors per channel are logged, not
    /// propagated — shutdown is best-effort.
    ///
    /// `closeChannel` transactions are *submitted* sequentially so the wallet
    /// provider's nonce filler assigns them in order, but their receipts are
    /// awaited *concurrently* via `join_all` — sequential receipt waits would
    /// blow the shutdown deadline once more than a couple of channels need
    /// closing, closing fewer of them within the bounded budget.
    async fn close_all_unredeemed(&self) -> usize {
        let states = match self.store.load_all() {
            Ok(s) => s,
            Err(err) => {
                warn!(%err, "shutdown close: failed to load channel state");
                return 0;
            }
        };
        let mut receipts = Vec::new();
        for st in states {
            // Nothing to claim without a signed voucher.
            if st.last_nonce.is_zero() || st.last_signature.is_empty() {
                continue;
            }
            let ch = match self.contract.getChannel(st.channel_id).call().await {
                Ok(ch) => ch,
                Err(err) => {
                    warn!(%err, channel_id = %st.channel_id, "shutdown close: getChannel failed");
                    continue;
                }
            };
            let unredeemed = st.last_amount.saturating_sub(ch.withdrawnAmount);
            if !matches!(ch.status, PaymentChannel::Status::Open) || unredeemed.is_zero() {
                continue;
            }
            if let Some(fut) = send_close(&self.contract, &self.pending_store, &st).await {
                receipts.push(fut);
            }
        }
        futures_util::future::join_all(receipts)
            .await
            .into_iter()
            .filter(|&landed| landed)
            .count()
    }
}

/// Submit `closeChannel` for `st`'s latest voucher. On a successful send,
/// returns a future that awaits the receipt and reports whether it landed (so
/// callers can await many concurrently); returns `None` if the send itself
/// failed. When the close lands the future also records a durable
/// [`PendingSettle`] entry (re-reading `disputeDeadline`) so the settle sweep
/// finalizes the provider's remainder after the dispute window. The returned
/// future captures only owned data (`use<P>`), so it borrows neither
/// `contract`, `pending_store`, nor `st`.
async fn send_close<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    pending_store: &Arc<dyn PendingSettleStore>,
    st: &ChannelState,
) -> Option<impl Future<Output = bool> + use<P>> {
    let sig = Bytes::from(normalize_voucher_signature(&st.last_signature));
    let channel_id = st.channel_id;
    match contract
        .closeChannel(
            channel_id,
            st.last_amount,
            st.last_nonce,
            st.last_bytes_delivered,
            sig,
        )
        .send()
        .await
    {
        Ok(pending) => {
            let contract = contract.clone();
            let pending_store = Arc::clone(pending_store);
            Some(async move {
                match pending.get_receipt().await {
                    // `get_receipt` returns `Ok` even for a reverted tx — a
                    // reverted close did NOT secure the claim, so report it as
                    // not landed so the caller does not retire/forget the
                    // channel.
                    Ok(receipt) if receipt.status() => {
                        info!(
                            %channel_id,
                            tx = %receipt.transaction_hash,
                            "closeChannel landed (dispute window open)"
                        );
                        record_pending_after_close(&contract, &pending_store, channel_id).await;
                        true
                    }
                    Ok(receipt) => {
                        warn!(
                            %channel_id,
                            tx = %receipt.transaction_hash,
                            "closeChannel transaction reverted on-chain"
                        );
                        false
                    }
                    Err(err) => {
                        warn!(%err, %channel_id, "closeChannel receipt failed");
                        false
                    }
                }
            })
        }
        Err(err) => {
            warn!(%err, %channel_id, "closeChannel send failed");
            None
        }
    }
}

/// After a `closeChannel` lands, re-read the channel to learn the
/// `disputeDeadline` the close just set and persist a [`PendingSettle`] entry
/// so the settle sweep can finalize the provider's un-withdrawn remainder once
/// the window elapses (PR #743 review). Best-effort: a failed read or write is
/// logged, not fatal — the close already secured the claim, and a missed entry
/// only means the node won't auto-settle (the client still can, or the next
/// run re-derives nothing — the remainder simply waits).
async fn record_pending_after_close<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    pending_store: &Arc<dyn PendingSettleStore>,
    channel_id: ChannelId,
) {
    let settle_after = match contract.getChannel(channel_id).call().await {
        Ok(ch) => ch.disputeDeadline,
        Err(err) => {
            // The close landed but we couldn't record the settle obligation.
            // For a fully-drawn channel (clientRefund == 0 — the case this
            // whole feature targets) the client has no incentive to settle, so
            // the remainder will NOT auto-settle: an operator must intervene.
            error!(
                %err, %channel_id,
                "post-close getChannel failed; settle obligation NOT recorded — \
                 if clientRefund==0 the remainder will not auto-settle, \
                 call settleChannel(<channel_id>) manually after the dispute window"
            );
            return;
        }
    };
    let entry = PendingSettle {
        channel_id,
        settle_after,
    };
    if let Err(err) = pending_store.record_pending(&entry) {
        // Unlike the getChannel arm this is not re-derivable: the channel is
        // forgotten right after close, so a lost write means no record this
        // channel ever needed settling. Same operator remedy.
        error!(
            %err, %channel_id, settle_after,
            "failed to persist pending-settle entry — \
             if clientRefund==0 the remainder will not auto-settle, \
             call settleChannel(<channel_id>) manually after the dispute window"
        );
    } else {
        info!(
            %channel_id, settle_after,
            "recorded channel for post-dispute settlement"
        );
    }
}

impl<P: Provider + Clone + 'static> std::fmt::Debug for PaymentChannelService<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaymentChannelService")
            .field("address", self.contract.address())
            .finish_non_exhaustive()
    }
}

/// Normalize a stored voucher signature (`r‖s‖v`) so `v` is in the `27`/`28`
/// convention the on-chain `ECDSA.recover` requires. The current source
/// (`Signature::as_bytes`) already emits `27`/`28`, so this is a no-op in
/// practice; it bridges a raw `0`/`1` y-parity should the encoding ever
/// change. `r`/`s` (the first 64 bytes) are untouched; a malformed-length
/// signature is passed through unchanged so the
/// contract's own validation produces the authoritative error.
fn normalize_voucher_signature(sig: &[u8]) -> Vec<u8> {
    let mut out = sig.to_vec();
    if let Some(v) = out.last_mut()
        && *v < ETH_V_OFFSET
    {
        *v = v.saturating_add(ETH_V_OFFSET);
    }
    out
}

/// Background lifecycle watcher: follow `ChannelOpened` / `ChannelSettled` /
/// `ChannelCloseInitiated`, restarting subscriptions with exponential backoff
/// on stream error. Mirrors `chain_staker_set::watcher_loop`.
async fn watcher_loop<P: Provider + Clone>(
    contract: PaymentChannel::PaymentChannelInstance<P>,
    self_address: Address,
    usdc_token: Address,
    handler: Arc<ClientHandler>,
    pending_store: Arc<dyn PendingSettleStore>,
    start_block: u64,
) {
    // One-time bring-up backfill range start (#762). Stays `Some` until a
    // backfill succeeds, so a `get_logs` error on the first cycle retries on
    // the next resubscription; once `None`, later resubscriptions skip it.
    let mut backfill_from = Some(start_block);
    let mut backoff = WATCHER_INITIAL_BACKOFF;
    loop {
        match run_watcher_once(
            &contract,
            self_address,
            usdc_token,
            &handler,
            &pending_store,
            &mut backfill_from,
        )
        .await
        {
            Ok(()) => {
                debug!("PaymentChannel watcher stream ended cleanly; resubscribing");
                backoff = WATCHER_INITIAL_BACKOFF;
            }
            Err(err) => {
                warn!(
                    %err,
                    backoff_secs = backoff.as_secs(),
                    "PaymentChannel watcher RPC error; restarting after backoff"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(WATCHER_MAX_BACKOFF);
            }
        }
    }
}

/// One watcher cycle: open the four event filters and drain them until one
/// errors (transport failure) or ends (filter expiry / provider rotation).
// 4-arm event-dispatch loop is fundamentally complex; splitting obscures the
// dispatch table (same posture as `chain_staker_set::run_watcher_once`). The
// line budget is likewise over by a hair for the same reason.
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
async fn run_watcher_once<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    self_address: Address,
    usdc_token: Address,
    handler: &Arc<ClientHandler>,
    pending_store: &Arc<dyn PendingSettleStore>,
    backfill_from: &mut Option<u64>,
) -> Result<()> {
    let mut opened = contract
        .ChannelOpened_filter()
        .watch()
        .await
        .context("watch ChannelOpened")?
        .into_stream();
    let mut topped_up = contract
        .ChannelToppedUp_filter()
        .watch()
        .await
        .context("watch ChannelToppedUp")?
        .into_stream();
    let mut settled = contract
        .ChannelSettled_filter()
        .watch()
        .await
        .context("watch ChannelSettled")?
        .into_stream();
    let mut close_initiated = contract
        .ChannelCloseInitiated_filter()
        .watch()
        .await
        .context("watch ChannelCloseInitiated")?
        .into_stream();

    // Bring-up backfill (#762): with the live filters now installed (they cover
    // blocks after their install point forward), read the block they took over
    // at and `get_logs` `ChannelOpened` over `[start, F]` so a channel opened in
    // the window between `bootstrap` returning and these filters installing is
    // still registered. The overlap at `F` is idempotent in the handler.
    if let Some(start) = *backfill_from {
        let to = contract
            .provider()
            .get_block_number()
            .await
            .context("read head block for ChannelOpened backfill")?;
        backfill_opened_channels(contract, self_address, usdc_token, handler, start, to).await?;
        *backfill_from = None;
    }

    loop {
        tokio::select! {
            ev = opened.next() => match ev {
                Some(Ok((event, _log))) => {
                    // Live arm: a persist failure is logged and skipped — the
                    // channel stays observable on-chain and a top-up/settle (or
                    // operator action) re-drives it; unlike the backfill, this
                    // is not the sole delivery, so we don't tear down the stream.
                    if let Err(err) =
                        apply_channel_opened(handler, self_address, usdc_token, &event, false).await
                    {
                        warn!(%err, channel_id = %event.channelId, "failed to persist opened channel");
                    }
                }
                Some(Err(e)) => return Err(e).context("ChannelOpened stream"),
                None => return Ok(()),
            },
            ev = topped_up.next() => match ev {
                Some(Ok((event, _log))) => {
                    // `ChannelToppedUp` is not provider-indexed; `update_channel_deposit`
                    // is a no-op for channels this node does not track.
                    if let Err(err) = handler
                        .update_channel_deposit(event.channelId, event.newDeposit)
                        .await
                    {
                        warn!(%err, channel_id = %event.channelId, "failed to apply channel top-up");
                    } else {
                        debug!(
                            channel_id = %event.channelId,
                            new_deposit = %event.newDeposit,
                            "channel top-up applied to tracked deposit"
                        );
                    }
                }
                Some(Err(e)) => return Err(e).context("ChannelToppedUp stream"),
                None => return Ok(()),
            },
            ev = settled.next() => match ev {
                Some(Ok((event, _log))) => {
                    if event.provider != self_address {
                        continue;
                    }
                    if let Err(err) = handler.forget_channel(event.channelId).await {
                        warn!(%err, channel_id = %event.channelId, "failed to forget settled channel");
                    } else {
                        info!(channel_id = %event.channelId, "channel settled; dropped tracked state");
                    }
                    // Settlement is final (by us or by the client) — drop any
                    // pending-settle obligation so the sweep stops retrying.
                    if let Err(err) = pending_store.forget_pending(event.channelId) {
                        warn!(%err, channel_id = %event.channelId, "failed to drop pending-settle entry on settle");
                    }
                }
                Some(Err(e)) => return Err(e).context("ChannelSettled stream"),
                None => return Ok(()),
            },
            ev = close_initiated.next() => match ev {
                Some(Ok((event, _log))) => {
                    // Dispute monitor is deferred (#324); observe-only.
                    debug!(
                        channel_id = %event.channelId,
                        initiator = %event.initiator,
                        "ChannelCloseInitiated observed (dispute monitor deferred, #324)"
                    );
                }
                Some(Err(e)) => return Err(e).context("ChannelCloseInitiated stream"),
                None => return Ok(()),
            },
        }
    }
}

/// Register a `ChannelOpened` event whose provider is this node so the voucher
/// path accepts vouchers for it. Shared by the live watcher arm and the
/// bring-up backfill (#762); `register_open_channel` is idempotent, so applying
/// the same event from both paths (the backfill/live overlap block) is safe.
/// `from_backfill` only varies the log line so the gap path is distinguishable.
///
/// Returns the persist result so the caller picks the failure policy: the live
/// arm logs and continues, but the backfill — the *sole* delivery of a channel
/// opened in `[S, F]` — propagates the error so the whole backfill retries with
/// `backfill_from` still set, rather than silently dropping the very channel the
/// backfill exists to recover.
async fn apply_channel_opened(
    handler: &Arc<ClientHandler>,
    self_address: Address,
    usdc_token: Address,
    event: &PaymentChannel::ChannelOpened,
    from_backfill: bool,
) -> Result<()> {
    // Only channels where this node is the provider concern us.
    if event.provider != self_address {
        return Ok(());
    }
    let mut state = ChannelState::new(event.channelId, event.client, usdc_token, event.deposit);
    // Track on-chain expiry so the sweep can close (and the handler can stop
    // serving) before `reclaimExpired` opens. A value past u64 is clamped to
    // "never" — safe, since the only effect of a too-far expiry is we never
    // force-close.
    state.expires_at = u64::try_from(event.expiresAt).unwrap_or(u64::MAX);
    handler
        .register_open_channel(state)
        .await
        .context("persist opened channel")?;
    let via = if from_backfill {
        "backfilled channel opened during watcher bring-up"
    } else {
        "registered channel opened against this node"
    };
    info!(
        channel_id = %event.channelId,
        client = %event.client,
        deposit = %event.deposit,
        "{via}"
    );
    Ok(())
}

/// One-time bring-up backfill (#762): `get_logs` (alloy's `.query()` issues an
/// `eth_getLogs`) every `ChannelOpened` in `[from_block, to_block]` and register
/// the ones this node provides, closing the gap between `bootstrap` returning
/// and the live `.watch()` filters installing.
///
/// Every failure mode returns an `Err` so the caller retries the whole backfill
/// via the watcher backoff (with `backfill_from` still set): an invalid
/// `from > to` range ([`check_backfill_range`]), the `get_logs` RPC, and — see
/// [`apply_channel_opened`] — a per-channel persist failure (the backfill is the
/// sole delivery of a `[S, F]` channel, so swallowing it would permanently drop
/// the channel). Only `ChannelOpened` is backfilled; see the module header for
/// why an in-window top-up/settle is out of scope (#751).
async fn backfill_opened_channels<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    self_address: Address,
    usdc_token: Address,
    handler: &Arc<ClientHandler>,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
    check_backfill_range(from_block, to_block)?;
    let logs = contract
        .ChannelOpened_filter()
        .from_block(from_block)
        .to_block(to_block)
        .query()
        .await
        .with_context(|| format!("backfill ChannelOpened over [{from_block}, {to_block}]"))?;
    debug!(
        from_block,
        to_block,
        count = logs.len(),
        "scanned ChannelOpened logs for watcher bring-up backfill"
    );
    for (event, _log) in logs {
        let channel_id = event.channelId;
        apply_channel_opened(handler, self_address, usdc_token, &event, true)
            .await
            .with_context(|| format!("backfill register channel {channel_id}"))?;
    }
    Ok(())
}

/// Redemption task: drain redeem hints and `withdraw` a channel's accrued
/// claim once it crosses the threshold. Ends cleanly when every hint sender
/// is dropped.
async fn redeemer_loop<P: Provider + Clone>(
    contract: PaymentChannel::PaymentChannelInstance<P>,
    store: Arc<dyn ChannelStateStore>,
    self_address: Address,
    redeem_threshold: U256,
    mut redeem_rx: mpsc::Receiver<ChannelId>,
) {
    // Per-channel cache of the last-known on-chain `withdrawnAmount`.
    // `withdrawnAmount` is only ever advanced by this node's own `withdraw`
    // transactions, so the cache is exact once seeded and is always `<=` the
    // true on-chain value — letting us skip the `getChannel` RPC for hints
    // whose accrued claim is provably still below the threshold (avoids RPC
    // spam under active per-MB voucher streaming). A cache miss estimates
    // from zero, so the first hint per channel still does one RPC.
    let mut withdrawn_cache: HashMap<ChannelId, U256> = HashMap::new();
    while let Some(channel_id) = redeem_rx.recv().await {
        if let Err(err) = try_redeem(
            &contract,
            &store,
            self_address,
            redeem_threshold,
            channel_id,
            &mut withdrawn_cache,
        )
        .await
        {
            warn!(%err, %channel_id, "redemption attempt failed");
        }
    }
    debug!("PaymentChannel redeemer loop ended (all hint senders dropped)");
}

/// Read the latest persisted voucher and the on-chain `withdrawnAmount`; if
/// the un-redeemed delta meets the threshold and the channel is still open,
/// submit `withdraw`.
async fn try_redeem<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn ChannelStateStore>,
    self_address: Address,
    redeem_threshold: U256,
    channel_id: ChannelId,
    withdrawn_cache: &mut HashMap<ChannelId, U256>,
) -> Result<()> {
    let Some(st) = store
        .get(channel_id)
        .context("load channel state for redemption")?
    else {
        // Channel not (yet) persisted — e.g. a hint raced the ChannelOpened
        // consumer. The next voucher re-hints.
        return Ok(());
    };
    if st.last_nonce.is_zero() || st.last_signature.is_empty() {
        return Ok(());
    }

    // Cheap pre-check against the cached withdrawn amount before any RPC. The
    // cache is `<=` the true on-chain `withdrawnAmount`, so this estimate is
    // an upper bound on the unredeemed claim — if it's already below the
    // threshold we can safely skip the `getChannel` call entirely.
    let cached_withdrawn = withdrawn_cache
        .get(&channel_id)
        .copied()
        .unwrap_or(U256::ZERO);
    if st.last_amount.saturating_sub(cached_withdrawn) < redeem_threshold {
        return Ok(());
    }

    let ch = contract
        .getChannel(channel_id)
        .call()
        .await
        .context("getChannel for redemption")?;
    withdrawn_cache.insert(channel_id, ch.withdrawnAmount);
    // Defensive: only redeem channels this node provides and that are open.
    if ch.provider != self_address || !matches!(ch.status, PaymentChannel::Status::Open) {
        return Ok(());
    }
    let unredeemed = st.last_amount.saturating_sub(ch.withdrawnAmount);
    if unredeemed < redeem_threshold {
        return Ok(());
    }

    let sig = Bytes::from(normalize_voucher_signature(&st.last_signature));
    let receipt = contract
        .withdraw(
            channel_id,
            st.last_amount,
            st.last_nonce,
            st.last_bytes_delivered,
            sig,
        )
        .send()
        .await
        .context("submit withdraw")?
        .get_receipt()
        .await
        .context("await withdraw receipt")?;
    // `get_receipt` resolves once the tx is mined, even if it reverted — a
    // reverted withdraw left `withdrawnAmount` unchanged on-chain, so we MUST
    // NOT seed the cache to `st.last_amount` (that would make every later hint
    // short-circuit and silently never redeem this channel again). The cache
    // already holds the correct pre-withdraw value from the getChannel read
    // above; leave it and let the next hint retry.
    if !receipt.status() {
        warn!(
            %channel_id,
            tx = %receipt.transaction_hash,
            "withdraw transaction reverted on-chain; leaving claim for retry"
        );
        return Ok(());
    }
    // A successful withdraw advances on-chain `withdrawnAmount` to the voucher
    // amount; reflect that in the cache so the next hints short-circuit.
    withdrawn_cache.insert(channel_id, st.last_amount);
    info!(
        %channel_id,
        tx = %receipt.transaction_hash,
        unredeemed = %unredeemed,
        amount = %st.last_amount,
        "withdrew accrued channel earnings"
    );
    Ok(())
}

/// Periodic expiry sweep: closes tracked channels approaching their on-chain
/// expiry so the provider redeems its earned-but-un-redeemed balance before
/// `withdraw`/`closeChannel` would start reverting and the client could
/// `reclaimExpired` for a full refund (#327). Threshold-driven `withdraw`
/// alone is insufficient: a long-lived channel that never crosses the
/// threshold (and never restarts the node) would otherwise expire un-redeemed.
async fn sweeper_loop<P: Provider + Clone>(
    contract: PaymentChannel::PaymentChannelInstance<P>,
    store: Arc<dyn ChannelStateStore>,
    pending_store: Arc<dyn PendingSettleStore>,
    handler: Arc<ClientHandler>,
    self_address: Address,
) {
    let mut ticker = tokio::time::interval(EXPIRY_SWEEP_INTERVAL);
    // Skip the immediate first tick — the expiry pass has nothing to do right
    // after bootstrap. The settle pass DOES run on the first real tick (a
    // channel closed before the last shutdown may already be past its dispute
    // window), but waiting one interval avoids racing the watcher's startup
    // and keeps startup I/O light.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        sweep_once(&contract, &store, &pending_store, &handler, self_address).await;
        // Read `now` here rather than before `sweep_once` (which does network
        // I/O) so the settle gate uses a fresh timestamp.
        settle_pass(&contract, &pending_store, unix_now()).await;
    }
}

/// One expiry-sweep pass. Errors are logged per channel and never abort the
/// sweep — it is best-effort background maintenance.
async fn sweep_once<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn ChannelStateStore>,
    pending_store: &Arc<dyn PendingSettleStore>,
    handler: &Arc<ClientHandler>,
    self_address: Address,
) {
    let states = match store.load_all() {
        Ok(s) => s,
        Err(err) => {
            warn!(%err, "expiry sweep: failed to load channel state");
            return;
        }
    };
    let now = unix_now();
    for st in states {
        try_close_for_expiry(contract, pending_store, handler, self_address, now, &st).await;
    }
}

/// Close one channel if it is within the close-ahead window of its on-chain
/// expiry and still carries an un-redeemed claim, then retire it from the
/// handler + store. All failure modes are logged and swallowed — the sweep is
/// best-effort.
// Linear guard-and-act sequence (filter → getChannel → close → retire); the
// early-return guards read more clearly inline than split across helpers.
#[allow(clippy::cognitive_complexity)]
async fn try_close_for_expiry<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    pending_store: &Arc<dyn PendingSettleStore>,
    handler: &Arc<ClientHandler>,
    self_address: Address,
    now: u64,
    st: &ChannelState,
) {
    // Only channels with a signed claim that are within the close-ahead window
    // of their tracked expiry (`within_close_window` handles the untracked
    // `expires_at == 0` case).
    if st.last_nonce.is_zero() || st.last_signature.is_empty() {
        return;
    }
    if !within_close_window(now, st.expires_at) {
        return;
    }
    let ch = match contract.getChannel(st.channel_id).call().await {
        Ok(ch) => ch,
        Err(err) => {
            warn!(%err, channel_id = %st.channel_id, "expiry sweep: getChannel failed");
            return;
        }
    };
    if ch.provider != self_address
        || !matches!(ch.status, PaymentChannel::Status::Open)
        || st.last_amount.saturating_sub(ch.withdrawnAmount).is_zero()
    {
        return;
    }
    let Some(receipt) = send_close(contract, pending_store, st).await else {
        return;
    };
    if !receipt.await {
        return;
    }
    // The claim is now secured by the close (settles after the dispute
    // window). Retire the channel: drop it from the handler + store so we stop
    // serving a channel we can no longer redeem against.
    if let Err(err) = handler.forget_channel(st.channel_id).await {
        warn!(%err, channel_id = %st.channel_id, "expiry sweep: forget after close failed");
    } else {
        info!(
            channel_id = %st.channel_id,
            expires_at = st.expires_at,
            "closed channel ahead of expiry and retired it"
        );
    }
}

/// One settlement-sweep pass: finalize every closed channel whose dispute
/// window has elapsed (PR #743 review). `closeChannel` only opens the window;
/// `settleChannel` is what routes the provider's un-withdrawn remainder, so
/// without this pass a fully-drawn channel (`clientRefund == 0`, no client
/// incentive to settle) would strand the provider's sub-threshold tail. Errors
/// are logged per channel and never abort the sweep.
async fn settle_pass<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    pending_store: &Arc<dyn PendingSettleStore>,
    now: u64,
) {
    let entries = match pending_store.load_pending() {
        Ok(e) => e,
        Err(err) => {
            warn!(%err, "settle sweep: failed to load pending-settle set");
            return;
        }
    };
    for entry in entries {
        // Gate on the stored `disputeDeadline` so we never submit a
        // guaranteed-revert `settleChannel` (and burn gas) before the window.
        if !ready_to_settle(now, entry.settle_after) {
            continue;
        }
        try_settle(contract, pending_store, entry.channel_id).await;
    }
}

/// Submit `settleChannel` for one closed channel past its dispute window and,
/// on success, drop its pending-settle entry. A revert almost always means
/// another party already settled (status left `Closing`): confirm via
/// `getChannel` and drop the entry if the channel is now `Closed`, otherwise
/// leave it for the next sweep (e.g. local-clock skew ahead of the chain).
// Linear guard-and-act sequence (send → receipt → status → confirm); the
// early-return guards read more clearly inline than split across helpers,
// same posture as `try_redeem` / `try_close_for_expiry`.
#[allow(clippy::cognitive_complexity)]
async fn try_settle<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    pending_store: &Arc<dyn PendingSettleStore>,
    channel_id: ChannelId,
) {
    let send = match contract.settleChannel(channel_id).send().await {
        Ok(p) => p,
        Err(err) => {
            warn!(%err, %channel_id, "settleChannel send failed; will retry next sweep");
            return;
        }
    };
    let receipt = match send.get_receipt().await {
        Ok(r) => r,
        Err(err) => {
            warn!(%err, %channel_id, "settleChannel receipt failed; will retry next sweep");
            return;
        }
    };
    if receipt.status() {
        info!(
            %channel_id,
            tx = %receipt.transaction_hash,
            "settled channel; routed provider remainder through FeeRouter"
        );
        forget_pending_logged(pending_store, channel_id);
        return;
    }
    // Reverted: most likely `ChannelNotClosing` because another party already
    // finalized. Confirm before dropping the obligation.
    warn!(
        %channel_id,
        tx = %receipt.transaction_hash,
        "settleChannel reverted; checking whether it was already finalized"
    );
    drop_pending_if_finalized(contract, pending_store, channel_id).await;
}

/// After a reverted `settleChannel`, read the channel to decide what to do
/// with the pending entry:
/// - `Closed`: another party finalized it — drop the entry.
/// - `Closing`: the window is still open. This happens when our local clock
///   ran ahead, or — the case worth handling — a `disputeChannel` *extended*
///   `disputeDeadline` past the value we stored at close
///   (`PaymentChannel.sol` forced-inclusion guarantee). Re-stamp `settle_after`
///   from the live deadline so the gate stops submitting a guaranteed-revert
///   `settleChannel` (and burning gas) every sweep until the new window passes.
/// - read error: keep the entry and retry next sweep.
async fn drop_pending_if_finalized<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    pending_store: &Arc<dyn PendingSettleStore>,
    channel_id: ChannelId,
) {
    match contract.getChannel(channel_id).call().await {
        Ok(ch) if matches!(ch.status, PaymentChannel::Status::Closed) => {
            info!(%channel_id, "channel already settled elsewhere; dropping pending entry");
            forget_pending_logged(pending_store, channel_id);
        }
        Ok(ch) if matches!(ch.status, PaymentChannel::Status::Closing) => {
            // Re-stamp the gate to the current on-chain deadline (overwrites by
            // contract). A no-op when unchanged; the fix when a dispute pushed
            // the deadline out from under our stored value.
            restamp_pending_logged(pending_store, channel_id, ch.disputeDeadline);
        }
        Ok(_) => {
            // `Open` is unreachable for a channel we closed (close → Closing →
            // Closed); leave the entry and retry next sweep if it ever occurs.
        }
        Err(err) => {
            warn!(%err, %channel_id, "post-revert getChannel failed; will retry next sweep");
        }
    }
}

/// Drop a pending-settle entry, logging (not propagating) a store failure —
/// the settlement already landed on-chain, so a failed delete only risks a
/// redundant `settleChannel` next sweep (which reverts harmlessly and re-drops
/// via the `Closed`-status path).
fn forget_pending_logged(pending_store: &Arc<dyn PendingSettleStore>, channel_id: ChannelId) {
    if let Err(err) = pending_store.forget_pending(channel_id) {
        warn!(%err, %channel_id, "failed to drop pending-settle entry after settlement");
    }
}

/// Re-stamp a pending entry's settle deadline (logging, not propagating, a
/// store failure). Used when a `settleChannel` reverts because a dispute
/// extended `disputeDeadline` past the value stored at close — keeping the
/// gate accurate so the sweep stops submitting guaranteed-revert transactions.
fn restamp_pending_logged(
    pending_store: &Arc<dyn PendingSettleStore>,
    channel_id: ChannelId,
    settle_after: u64,
) {
    let entry = PendingSettle {
        channel_id,
        settle_after,
    };
    if let Err(err) = pending_store.record_pending(&entry) {
        warn!(%err, %channel_id, "failed to re-stamp pending-settle deadline; will retry next sweep");
    } else {
        debug!(
            %channel_id,
            settle_after,
            "settleChannel reverted with the dispute window still open; re-stamped deadline"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 65-byte signature (`[u8; 65]` so a const index stays provably
    /// in-bounds for the anti-panic lints) with the recovery byte set to `v`.
    fn sig_with_v(v: u8) -> [u8; 65] {
        let mut s = [0u8; 65];
        s[64] = v;
        s
    }

    #[test]
    fn normalize_raw_parity_to_eth_v() {
        // y-parity 0 -> 27, 1 -> 28; already-27/28 untouched.
        assert_eq!(
            normalize_voucher_signature(&sig_with_v(0)).last(),
            Some(&27u8)
        );
        assert_eq!(
            normalize_voucher_signature(&sig_with_v(1)).last(),
            Some(&28u8)
        );
        assert_eq!(
            normalize_voucher_signature(&sig_with_v(27)).last(),
            Some(&27u8)
        );
        assert_eq!(
            normalize_voucher_signature(&sig_with_v(28)).last(),
            Some(&28u8)
        );
    }

    #[test]
    fn normalize_preserves_r_s_bytes() {
        let mut sig = [0u8; 65];
        for (i, b) in sig.iter_mut().enumerate() {
            *b = u8::try_from(i).unwrap_or(0);
        }
        sig[64] = 1; // parity
        let out = normalize_voucher_signature(&sig);
        // r/s (first 64 bytes) untouched; only the recovery byte changes.
        assert!(
            out.iter().zip(sig.iter()).take(64).all(|(a, b)| a == b),
            "r/s bytes must be untouched"
        );
        assert_eq!(out.last(), Some(&28u8));
    }

    #[test]
    fn normalize_passes_through_malformed_length() {
        // Not 65 bytes: pass through (last byte still normalized, but the
        // contract is the authority on length validity).
        let out = normalize_voucher_signature(&[1u8, 2, 3]);
        assert_eq!(out.len(), 3);
        assert_eq!(out.last(), Some(&30u8)); // 3 -> 3+27
    }

    #[test]
    fn normalize_empty_signature_is_empty() {
        // The v1-hydrated / no-voucher-yet state. Callers short-circuit on
        // `is_empty()`, but the normalizer must not panic or fabricate bytes.
        assert!(normalize_voucher_signature(&[]).is_empty());
    }

    #[test]
    fn is_expired_boundary() {
        // expires_at == 0 => never expires, regardless of now.
        assert!(!is_expired(0, 0));
        assert!(!is_expired(u64::MAX, 0));
        // Strictly before expiry: not expired. At/after: expired.
        assert!(!is_expired(999, 1_000));
        assert!(is_expired(1_000, 1_000), "now == expires_at is expired");
        assert!(is_expired(1_001, 1_000));
    }

    #[test]
    fn within_close_window_boundary() {
        let expires_at = 1_000_000u64;
        // Untracked expiry is never in-window.
        assert!(!within_close_window(u64::MAX, 0));
        // Just outside the window (more than CLOSE_AHEAD before expiry).
        assert!(!within_close_window(
            expires_at - EXPIRY_CLOSE_AHEAD_SECS - 1,
            expires_at
        ));
        // Exactly at the window edge, and inside it.
        assert!(within_close_window(
            expires_at - EXPIRY_CLOSE_AHEAD_SECS,
            expires_at
        ));
        assert!(within_close_window(expires_at - 1, expires_at));
        // Past expiry is still "in window" (we still want to attempt a close,
        // though the on-chain call will revert if truly expired).
        assert!(within_close_window(expires_at + 10, expires_at));
        // `now` near u64::MAX must not overflow the `+ CLOSE_AHEAD` add.
        assert!(within_close_window(u64::MAX, expires_at));
    }

    #[test]
    fn check_backfill_range_boundary() {
        // Empty range (`from > to`): an RPC-lag / reorg anomaly, not "no blocks
        // elapsed" — returned as a retryable `Err` so the caller retries rather
        // than silently skipping (which would reopen the race).
        assert!(check_backfill_range(1_001, 1_000).is_err());
        // Single block (`from == to`): valid and must be scanned — a
        // ChannelOpened can sit in the exact block bootstrap read the head at.
        assert!(check_backfill_range(1_000, 1_000).is_ok());
        // Normal forward range.
        assert!(check_backfill_range(1_000, 1_005).is_ok());
        // Genesis / zero head is a valid single-block range.
        assert!(check_backfill_range(0, 0).is_ok());
    }

    #[test]
    fn ready_to_settle_boundary() {
        let deadline = 1_000u64;
        // Strictly before the dispute deadline: not yet settleable (the
        // contract reverts DisputeWindowActive).
        assert!(!ready_to_settle(deadline - 1, deadline));
        // At and after the deadline: settleable (contract requires now >=).
        assert!(
            ready_to_settle(deadline, deadline),
            "now == disputeDeadline is settleable"
        );
        assert!(ready_to_settle(deadline + 1, deadline));
    }
}
