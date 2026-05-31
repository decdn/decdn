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
//!   rejected (the #327 gap noted in [`crate::handlers`]'s client module). On
//!   `ChannelSettled` it forgets the channel via
//!   [`ClientHandler::forget_channel`]; `ChannelCloseInitiated` is
//!   observed-only (the in-process dispute monitor is deferred — issue #324).
//! - **Redemption (threshold + on-shutdown).** On a redeem hint emitted by
//!   the voucher-accept path, it reads the latest persisted voucher and the
//!   on-chain `withdrawnAmount`, and submits `withdraw` once the accrued
//!   un-redeemed amount crosses a configurable threshold (a monotonic,
//!   client-signed claim needs no dispute window — ADR 003 § Operator early
//!   withdrawal). On graceful shutdown it best-effort `closeChannel`s every
//!   tracked channel that still carries an un-redeemed claim, starting the
//!   dispute window so a later `settleChannel` (by anyone) finalizes it.
//!
//! Buyer-side `openChannel` (node→node cache-miss pulls) and the dispute
//! monitor are out of scope here. Structurally this mirrors
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
use decdn_incentive::{ChannelId, ChannelState, ChannelStateStore};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

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

/// Minimum recovery-id byte the on-chain `ECDSA.recover` accepts. alloy's
/// [`alloy::primitives::Signature::as_bytes`] may encode the recovery id as a
/// raw y-parity (`0`/`1`); the contract requires the Ethereum convention
/// (`27`/`28`). [`normalize_voucher_signature`] bridges the two without
/// touching `r`/`s`.
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
    self_address: Address,
    redeem_threshold: U256,
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
        info!(
            %payment_channel_addr,
            %usdc_token,
            %self_address,
            "PaymentChannel settlement service bootstrap complete"
        );

        let (redeem_tx, redeem_rx) = mpsc::channel(REDEEM_HINT_CAPACITY);

        let watcher = tokio::spawn(watcher_loop(
            contract.clone(),
            self_address,
            usdc_token,
            Arc::clone(&handler),
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
            handler,
            self_address,
        ));

        Ok(Self {
            contract,
            store,
            self_address,
            redeem_threshold,
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
            if let Some(fut) = send_close(&self.contract, &st).await {
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
/// failed. The returned future captures only owned/`Copy` data (`use<P>`), so
/// it borrows neither `contract` nor `st`.
async fn send_close<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
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
        Ok(pending) => Some(async move {
            match pending.get_receipt().await {
                Ok(receipt) => {
                    info!(
                        %channel_id,
                        tx = %receipt.transaction_hash,
                        "closeChannel landed (dispute window open)"
                    );
                    true
                }
                Err(err) => {
                    warn!(%err, %channel_id, "closeChannel receipt failed");
                    false
                }
            }
        }),
        Err(err) => {
            warn!(%err, %channel_id, "closeChannel send failed");
            None
        }
    }
}

impl<P: Provider + Clone + 'static> std::fmt::Debug for PaymentChannelService<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaymentChannelService")
            .field("address", self.contract.address())
            .field("self_address", &self.self_address)
            .field("redeem_threshold", &self.redeem_threshold)
            .finish_non_exhaustive()
    }
}

/// Translate a stored voucher signature (`r‖s‖v`, with `v` possibly a raw
/// `0`/`1` y-parity) into the `27`/`28` convention the on-chain
/// `ECDSA.recover` requires. `r`/`s` (the first 64 bytes) are
/// untouched; a malformed-length signature is passed through unchanged so the
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
) {
    let mut backoff = WATCHER_INITIAL_BACKOFF;
    loop {
        match run_watcher_once(&contract, self_address, usdc_token, &handler).await {
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

/// One watcher cycle: open the three event filters and drain them until one
/// errors (transport failure) or ends (filter expiry / provider rotation).
// 3-arm event-dispatch loop is fundamentally complex; splitting obscures the
// dispatch table (same posture as `chain_staker_set::run_watcher_once`).
#[allow(clippy::cognitive_complexity)]
async fn run_watcher_once<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    self_address: Address,
    usdc_token: Address,
    handler: &Arc<ClientHandler>,
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

    loop {
        tokio::select! {
            ev = opened.next() => match ev {
                Some(Ok((event, _log))) => {
                    // Only channels where this node is the provider concern us.
                    if event.provider != self_address {
                        continue;
                    }
                    let mut state = ChannelState::new(
                        event.channelId,
                        event.client,
                        usdc_token,
                        event.deposit,
                    );
                    // Track on-chain expiry so the sweep can close (and the
                    // handler can stop serving) before `reclaimExpired` opens.
                    // A value past u64 is clamped to "never" — safe, since the
                    // only effect of a too-far expiry is we never force-close.
                    state.expires_at = u64::try_from(event.expiresAt).unwrap_or(u64::MAX);
                    if let Err(err) = handler.register_open_channel(state).await {
                        warn!(%err, channel_id = %event.channelId, "failed to persist opened channel");
                    } else {
                        info!(
                            channel_id = %event.channelId,
                            client = %event.client,
                            deposit = %event.deposit,
                            "registered channel opened against this node"
                        );
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
    handler: Arc<ClientHandler>,
    self_address: Address,
) {
    let mut ticker = tokio::time::interval(EXPIRY_SWEEP_INTERVAL);
    // Skip the immediate first tick — bootstrap just ran; nothing is near
    // expiry yet, and it avoids a redundant load_all at startup.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        sweep_once(&contract, &store, &handler, self_address).await;
    }
}

/// One expiry-sweep pass. Errors are logged per channel and never abort the
/// sweep — it is best-effort background maintenance.
async fn sweep_once<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn ChannelStateStore>,
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
        try_close_for_expiry(contract, handler, self_address, now, &st).await;
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
    handler: &Arc<ClientHandler>,
    self_address: Address,
    now: u64,
    st: &ChannelState,
) {
    // Only channels with a tracked expiry, a signed claim, and that are within
    // the close-ahead window of expiry.
    if st.expires_at == 0 || st.last_nonce.is_zero() || st.last_signature.is_empty() {
        return;
    }
    if now.saturating_add(EXPIRY_CLOSE_AHEAD_SECS) < st.expires_at {
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
    let Some(receipt) = send_close(contract, st).await else {
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
}
