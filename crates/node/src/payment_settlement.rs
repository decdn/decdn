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
//!   (#762) — and the across-restart **downtime gap** (#751) — the first watcher
//!   cycle backfills `ChannelOpened` from a floor block `S` up to the block `F`
//!   the live filters took over at, then streams forward. `S` is the *lower* of
//!   the bootstrap head and the persisted scan checkpoint minus a reorg margin
//!   (clamped to `<= head`): on first boot there is no checkpoint so `S` is the
//!   head (closing the #762 race only); on a restart the checkpoint sits below
//!   head, so `S` drops to it and the scanned range `[S, F]` covers both the
//!   downtime gap (#751, blocks below head) and the bring-up window (#762,
//!   `[head, F]`). As it runs, the watcher records the last block it scanned for
//!   `ChannelOpened` via [`WatcherCheckpointStore`] (advanced only on a
//!   successfully-registered open), so the next boot resumes from there and
//!   registers channels a client opened while this node was **down**. The
//!   backfilled `[S, F]` and the live stream (which starts at-or-before `F`)
//!   leave no gap; the overlap around `F` and any re-scanned blocks below the
//!   checkpoint are harmless because [`ClientHandler::register_open_channel`] is
//!   idempotent. A long gap is walked in bounded windows so a single `get_logs`
//!   never exceeds RPC range caps.
//!
//!   Scope of the backfill: **only `ChannelOpened`**. A `ChannelToppedUp` or
//!   `ChannelSettled` emitted inside the backfilled range is not back-filled, so
//!   a channel both opened and topped-up/settled within it could carry a stale
//!   tracked deposit or be re-registered after settlement. For the sub-second
//!   #762 window this is negligible; across a long downtime gap a re-registered
//!   already-settled channel is self-correcting (the redeemer/sweeper gate on
//!   the live on-chain status, and a stale deposit only over-restricts vouchers
//!   conservatively). Ordered multi-event backfill remains out of scope.
//!
//!   If a *live* `ChannelOpened` for this node fails to persist (a transient
//!   store error), the checkpoint is held below its block and the backfill is
//!   re-armed, so the channel is recovered on the next resubscribe or restart —
//!   not mid-cycle. On a healthy, never-resubscribing stream that window can be
//!   long; the client's vouchers are rejected `WrongChannel` until then, but
//!   nothing is stranded (the held checkpoint guarantees a restart re-covers it).
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
use std::time::{Duration, Instant};

use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::Provider;
use anyhow::{Context, Result};
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::{
    ChannelId, ChannelState, ChannelStateStore, PendingSettle, PendingSettleStore, StoreError,
    WatcherCheckpointStore,
};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::handlers::client::ClientHandler;
use crate::metrics::Metrics;

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

/// How often the redeemer self-tick scans all channels for an above-threshold
/// claim, independent of hints (#751). Hints from the bounded advisory channel
/// are best-effort and can be dropped under high channel fan-out; this low-
/// frequency backstop guarantees a channel that crossed the threshold is still
/// redeemed (it also recovers a `withdraw` whose receipt errored but later
/// mined, and a channel that went quiet just below threshold). Kept well below
/// the expiry sweep's hourly cadence so accrued earnings are withdrawn promptly
/// without leaning on per-voucher hints.
const REDEEM_TICK_INTERVAL: Duration = Duration::from_mins(5);

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

/// How many blocks the persisted scan checkpoint is rewound before the
/// resume backfill (#751), absorbing a shallow reorg between the last scanned
/// block and the next boot: a `ChannelOpened` re-mined at a slightly different
/// height after a reorg is still inside the rescanned window.
/// `register_open_channel` is idempotent, so the only cost of the margin is a
/// few extra blocks of `eth_getLogs`. Sized for the shallow reorgs of an
/// Arbitrum-Sepolia-class L2.
const REORG_MARGIN_BLOCKS: u64 = 128;

/// Maximum block span scanned per `eth_getLogs` during the resume backfill
/// (#751). A node down for a long time resumes from a checkpoint many thousands
/// of blocks behind head; a single unbounded `eth_getLogs` over that gap would
/// exceed the range/result caps most RPC providers enforce. The backfill walks
/// the gap in windows of this size instead.
///
/// This bounds the *block span* per request, not the *result count* — some
/// providers cap `eth_getLogs` by number of matched logs (or a lower block span)
/// rather than range, so a dense 10k-block window could still trip a result-count
/// limit. 10k is a conservative default that clears the common range caps; tie it
/// to the deployment provider's documented `eth_getLogs` limit (and make it
/// configurable) if a target RPC enforces a tighter or result-count-based cap.
const MAX_BACKFILL_BLOCK_SPAN: u64 = 10_000;

/// Debounce thresholds for the watcher scan checkpoint (#784). The persisted
/// checkpoint is only a *floor* for the resume backfill: `resolve_backfill_start`
/// rounds it down by [`REORG_MARGIN_BLOCKS`] and `register_open_channel` is
/// idempotent, so a checkpoint that lags the true scan position by a bounded
/// amount only ever *widens* the next rescan, never narrows it. That makes the
/// per-block fsync the live `opened` arm would otherwise pay (one fsync per
/// distinct block carrying a provider-owned `ChannelOpened`, both while draining
/// a resubscribe backlog and in steady state on a high-fan-out provider) safe to
/// coarsen: [`DebouncedCheckpointStore`] forwards a durable write only once the
/// buffered block is at least [`CHECKPOINT_FLUSH_BLOCKS`] ahead of the last
/// persisted value *or* at least [`CHECKPOINT_FLUSH_INTERVAL`] has elapsed since
/// the last durable write, whichever comes first, and the runtime forces a final
/// flush on graceful shutdown.
///
/// The floor is allowed to lag by far more than [`REORG_MARGIN_BLOCKS`] — a
/// crash just re-scans the lagging span via the head-anchored backfill, so the
/// reorg margin is not an upper bound on the debounce window. `512` is chosen so
/// a backlog drain fsyncs ~once per 512 blocks instead of per block (a ~512x
/// reduction) while keeping the worst-case re-scanned span small relative to a
/// long downtime gap.
const CHECKPOINT_FLUSH_BLOCKS: u64 = 512;

/// Time-based companion to [`CHECKPOINT_FLUSH_BLOCKS`]: even a slow trickle of
/// opens (well under [`CHECKPOINT_FLUSH_BLOCKS`] apart) persists at least this
/// often, bounding how many blocks a crash re-scans when block-cadence alone
/// would defer the write indefinitely. 30s keeps the steady-state fsync rate
/// negligible while making the worst-case lost progress a handful of L2 blocks.
const CHECKPOINT_FLUSH_INTERVAL: Duration = Duration::from_secs(30);

/// Resolve the watcher backfill floor from the persisted scan checkpoint and
/// the bootstrap head (#751). `None` (first-ever boot) → `head`: nothing was
/// opened against this node before it existed. `Some(b)` → `b - margin`, clamped
/// to `<= head` so a checkpoint momentarily ahead of a lagging RPC head never
/// inverts the `[start, head]` range. Pure so the policy is unit-testable
/// without a live provider.
const fn resolve_backfill_start(last_seen: Option<u64>, head: u64, margin: u64) -> u64 {
    match last_seen {
        Some(b) => {
            let floored = b.saturating_sub(margin);
            if floored < head { floored } else { head }
        }
        None => head,
    }
}

/// Split an inclusive `[from, to]` block range into successive inclusive windows
/// of at most `span` blocks (#751), so a long resume backfill issues bounded
/// `eth_getLogs` calls. Pure and allocation-light (one entry per window); a
/// `from > to` range yields no windows (the caller validates that separately via
/// [`check_backfill_range`]). Unit-tested for the window math.
fn backfill_windows(from: u64, to: u64, span: u64) -> Vec<(u64, u64)> {
    let mut windows = Vec::new();
    if from > to || span == 0 {
        return windows;
    }
    let mut start = from;
    loop {
        let end = start.saturating_add(span - 1).min(to);
        windows.push((start, end));
        if end >= to {
            return windows;
        }
        start = end.saturating_add(1);
    }
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
    /// The redemption task handle. Unlike `_watcher`/`_sweeper` (aborted only on
    /// drop) this is held so the shutdown path can abort+await it *before*
    /// closing channels (#751) — greatly narrowing (not eliminating; a tx already
    /// broadcast before the abort can still mine) the window where a live
    /// `withdraw` races the shutdown `closeChannel`, benign-reverts, and logs a
    /// misleading warn. `take()`n by
    /// [`Self::quiesce_redeemer`]; the [`Drop`] impl aborts whatever remains as
    /// the safety net the `AbortOnDrop` wrapper gave the other tasks. A
    /// `std::sync::Mutex` (not `tokio`): the guard is only ever held to `take()`
    /// the handle, never across an `.await`.
    redeemer: std::sync::Mutex<Option<JoinHandle<()>>>,
    _sweeper: AbortOnDrop,
    /// Held so graceful shutdown can force a final checkpoint flush (#784): the
    /// watcher debounces durable checkpoint writes, so the latest scan progress
    /// lives only in memory until either threshold trips. A clean stop flushes it
    /// here so the next boot resumes from the true scan position rather than
    /// re-scanning the debounce window. A directly-durable store's `flush` is a
    /// no-op, so this is harmless when no debounce decorator is installed.
    checkpoint_store: Arc<dyn WatcherCheckpointStore>,
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
    #[allow(clippy::too_many_arguments)]
    pub async fn bootstrap(
        provider: P,
        payment_channel_addr: Address,
        self_address: Address,
        store: Arc<dyn ChannelStateStore>,
        pending_store: Arc<dyn PendingSettleStore>,
        checkpoint_store: Arc<dyn WatcherCheckpointStore>,
        handler: Arc<ClientHandler>,
        redeem_threshold: U256,
        auto_settle: AutoSettleConfig,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        let contract = PaymentChannel::new(payment_channel_addr, provider);

        // Startup self-check: a cheap immutable view confirms the configured
        // address actually hosts the contract (and yields the settlement
        // token). Same fail-fast spirit as `check_rpc_reachability`.
        let usdc_token = contract.usdc().call().await.with_context(|| {
            format!("PaymentChannel.usdc() self-check at {payment_channel_addr}")
        })?;
        // Head block at bootstrap and the persisted scan checkpoint together
        // pick the backfill floor. The watcher backfills `ChannelOpened` from
        // that floor up to the block its live filters install at — closing both
        // the sub-millisecond bring-up race (#762, head floor) and the
        // across-restart downtime gap (#751, checkpoint floor). Read failures
        // are fatal at bring-up, matching the `usdc()` self-check.
        let head_block = contract
            .provider()
            .get_block_number()
            .await
            .context("read head block for watcher backfill at bootstrap")?;
        let last_seen = checkpoint_store
            .load_last_seen_block()
            .context("read watcher scan checkpoint at bootstrap")?;
        let start_block = resolve_backfill_start(last_seen, head_block, REORG_MARGIN_BLOCKS);
        info!(
            %payment_channel_addr,
            %usdc_token,
            %self_address,
            head_block,
            last_seen_block = last_seen,
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
            Arc::clone(&checkpoint_store),
            start_block,
            Arc::clone(&metrics),
        ));
        let redeemer = tokio::spawn(redeemer_loop(
            contract.clone(),
            Arc::clone(&store),
            Arc::clone(&pending_store),
            Arc::clone(&handler),
            self_address,
            redeem_threshold,
            auto_settle,
            redeem_rx,
            Arc::clone(&metrics),
        ));
        let sweeper = tokio::spawn(sweeper_loop(
            contract.clone(),
            Arc::clone(&store),
            Arc::clone(&pending_store),
            handler,
            self_address,
            Arc::clone(&metrics),
        ));

        Ok(Self {
            contract,
            store,
            pending_store,
            redeem_tx,
            _watcher: AbortOnDrop(watcher),
            redeemer: std::sync::Mutex::new(Some(redeemer)),
            _sweeper: AbortOnDrop(sweeper),
            checkpoint_store,
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
        // Force the debounced watcher checkpoint to disk first (#784) so the
        // most-recent scan progress survives the stop and the next boot does not
        // needlessly re-scan the debounce window. Done before the channel-close
        // deadline so a slow `closeChannel` cannot starve it.
        self.flush_checkpoint_on_shutdown();
        // Quiesce the redeemer first (#751): with the router already drained, no
        // new hints arrive, and aborting the redeemer here stops it from *issuing*
        // any further `withdraw`, so the `closeChannel`s below are very unlikely to
        // race one (which would benign-revert and log a misleading warn). This
        // narrows the window rather than closing it: a `withdraw` already broadcast
        // before the abort can still mine concurrently with a `closeChannel`. That
        // residual race is benign (the revert is expected; settlement is unchanged)
        // — the abort just keeps the common case quiet. The deadline below covers
        // only the closes; the abort+await is bounded (an aborted task stops at its
        // next await).
        self.quiesce_redeemer().await;
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

    /// Force the debounced watcher scan checkpoint to durable storage (#784).
    /// Best-effort: a failed flush only widens the next boot's rescan (the
    /// [`WatcherCheckpointStore`] floor contract), so it is logged, never
    /// propagated — it must not abort the shutdown close path.
    fn flush_checkpoint_on_shutdown(&self) {
        if let Err(err) = self.checkpoint_store.flush() {
            // A failed flush leaves the buffered block intact, so
            // `load_last_seen_block` surfaces the still-buffered high-water block:
            // how far ahead of the durable floor this stop lost progress, i.e. the
            // span the next boot's head-anchored backfill will re-scan. Logging it
            // gives a post-mortem the rescan depth without a dedicated accessor.
            //
            // The in-loop persist sites bump `metrics.watcher_persist_failure()`,
            // but no `metrics` handle is held on this shutdown path (the watcher
            // task owns it), so we deliberately do not plumb one through solely for
            // counter parity here — the warn is the signal for a failed flush.
            let pending_block = self.checkpoint_store.load_last_seen_block().ok().flatten();
            warn!(
                %err,
                ?pending_block,
                "failed to flush watcher scan checkpoint on shutdown"
            );
        }
    }

    /// Abort and await the redemption task so it issues no *further* `withdraw`
    /// into the shutdown close path (#751) — a `withdraw` already broadcast before
    /// the abort can still mine, so this narrows rather than eliminates the
    /// withdraw-vs-`closeChannel` race. Idempotent: once the handle is taken,
    /// later calls — and the [`Drop`] safety net — find `None` and no-op.
    /// Awaiting the aborted handle resolves promptly (cancellation lands at the
    /// task's next await point), so this adds no meaningful latency to shutdown.
    async fn quiesce_redeemer(&self) {
        // Sync lock: the guard is dropped at the end of this statement (before
        // the `handle.await` below), so it is never held across an await.
        // `into_inner` recovers a poisoned lock rather than panicking
        // (`unwrap`/`expect` are denied workspace-wide).
        let handle = self
            .redeemer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
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
            if st.last_nonce().is_zero() || st.last_signature().is_none() {
                continue;
            }
            let ch = match self.contract.getChannel(st.channel_id).call().await {
                Ok(ch) => ch,
                Err(err) => {
                    warn!(%err, channel_id = %st.channel_id, "shutdown close: getChannel failed");
                    continue;
                }
            };
            let unredeemed = st.last_amount().saturating_sub(ch.withdrawnAmount);
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
    // Nothing to submit without a signed voucher — callers already guard this,
    // but the `Option` makes it unrepresentable rather than a convention (the
    // `?` returns `None`, i.e. "no close future", on an absent signature).
    let sig_bytes = st.last_signature()?;
    let sig = Bytes::from(normalize_voucher_signature(sig_bytes));
    let channel_id = st.channel_id;
    match contract
        .closeChannel(
            channel_id,
            st.last_amount(),
            st.last_nonce(),
            st.last_bytes_delivered(),
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

impl<P: Provider + Clone + 'static> Drop for PaymentChannelService<P> {
    fn drop(&mut self) {
        // Safety net mirroring the `AbortOnDrop` the watcher/sweeper get: a
        // service dropped without a graceful `close_open_channels_on_shutdown`
        // (which `take()`s the handle via `quiesce_redeemer`) must not leak the
        // redemption task. The lock is uncontended (`&mut self` means we hold
        // the only reference); `into_inner` still aborts the task even if a
        // prior panic poisoned the lock. The handle is aborted, not awaited
        // (drop is sync).
        if let Some(handle) = self
            .redeemer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            handle.abort();
        }
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

/// Operator-configured auto-settlement triggers (#742). When either threshold
/// is crossed the redeemer proactively `closeChannel`s the channel — starting
/// the dispute window so a large unsubmitted voucher balance is secured on-chain
/// before the client can go dark, then the settle sweep finalizes the remainder.
///
/// Distinct from the `redeem_threshold`: `withdraw` reclaims earnings on a
/// still-open channel (cheap, repeatable), whereas a close caps total at-risk
/// exposure but ends the channel. Both fields default to `None` (disabled), so a
/// node that doesn't opt in behaves exactly as before #742. A `Some` value is
/// guaranteed `> 0` by config resolution.
#[derive(Debug, Clone, Copy, Default)]
pub struct AutoSettleConfig {
    /// Outstanding (un-redeemed) value (`µUSDC`) at which to close. `None`
    /// disables the value trigger.
    pub value_threshold: Option<U256>,
    /// Un-redeemed nonce SPAN at which to close, derived as the off-chain latest
    /// voucher nonce minus the on-chain `claimedNonce`. This is the nonce *span*,
    /// an UPPER BOUND on the voucher count — voucher nonces may skip values (ADR
    /// 003 §Voucher Nonce Convention; `ChannelState::apply_voucher` accepts a
    /// `nonce > last_nonce` with a gap), so the span can exceed the number of
    /// vouchers actually accepted. `None` disables the span trigger.
    pub voucher_nonce_span_threshold: Option<u64>,
}

/// Pure auto-settlement decision (#742): given a channel's un-redeemed value and
/// un-redeemed nonce span, decide whether either configured trigger has fired.
/// Returns `true` when the value crosses [`AutoSettleConfig::value_threshold`]
/// **or** the nonce span crosses
/// [`AutoSettleConfig::voucher_nonce_span_threshold`] (logical OR — a burst of
/// small vouchers can trip the span trigger without the value one, and a single
/// large voucher the reverse). The nonce span is `last_nonce − claimedNonce`, an
/// UPPER BOUND on the un-redeemed voucher count (nonces may skip values), not the
/// exact count. Both comparisons are `>=` so a channel sitting exactly at a
/// threshold settles. A disabled (`None`) trigger never fires. Pure so the policy
/// is unit-testable without a live provider.
fn should_auto_settle(
    cfg: &AutoSettleConfig,
    unredeemed_value: U256,
    unredeemed_nonce_span: u64,
) -> bool {
    if let Some(threshold) = cfg.value_threshold
        && unredeemed_value >= threshold
    {
        return true;
    }
    if let Some(span) = cfg.voucher_nonce_span_threshold
        && unredeemed_nonce_span >= span
    {
        return true;
    }
    false
}

/// Pure cheap-pre-check decision: given the off-chain voucher watermark
/// (`last_amount`, `last_nonce`) and the cached on-chain `(withdrawn, claimed
/// nonce)` lower bounds, decide whether the `getChannel` RPC can be skipped this
/// tick. The cached values are `<=` the true on-chain ones (only this node's
/// `withdraw`/`closeChannel` advance them), so the derived
/// `est_unredeemed = last_amount − cached_withdrawn` and
/// `est_nonce_span = last_nonce − cached_nonce` are UPPER BOUNDS — when every
/// upper bound sits strictly below its threshold the true values do too, so no
/// trigger (redeem, auto-settle value, or auto-settle nonce span) can fire and
/// the RPC is safe to skip. Each enabled threshold tightens the skip predicate;
/// a cache miss (estimating from zero) yields the widest bounds, so the first
/// hint per channel never skips. Pure so the predicate is unit-testable without
/// a live provider.
fn can_skip_redeem_rpc(
    last_amount: U256,
    last_nonce: U256,
    cached_withdrawn: U256,
    cached_nonce: U256,
    redeem_threshold: U256,
    auto_settle: &AutoSettleConfig,
) -> bool {
    let est_unredeemed = last_amount.saturating_sub(cached_withdrawn);
    // The redeem `withdraw` path always applies; its value bound is mandatory.
    let mut can_skip = est_unredeemed < redeem_threshold;
    // The auto-settle value trigger fires on the same `withdrawnAmount`-derived
    // delta, so the cached withdrawn lower bound proves it can't have fired.
    if let Some(val_threshold) = auto_settle.value_threshold {
        can_skip &= est_unredeemed < val_threshold;
    }
    // The auto-settle nonce-span trigger fires on `last_nonce − claimedNonce`;
    // the cached claimed-nonce lower bound proves it can't have fired.
    if let Some(span_threshold) = auto_settle.voucher_nonce_span_threshold {
        let est_nonce_span = unredeemed_nonce_span(last_nonce, cached_nonce);
        can_skip &= est_nonce_span < span_threshold;
    }
    can_skip
}

/// Why a [`run_watcher_once`] cycle returned, so [`watcher_loop`] can pick the
/// right resubscribe cadence.
enum CycleOutcome {
    /// An event filter ended cleanly (filter expiry / provider rotation).
    /// Resubscribe immediately with backoff reset.
    StreamEnded,
    /// A live `ChannelOpened` for this node failed to persist; the checkpoint is
    /// held below its block and `backfill_from` re-armed. We return early (rather
    /// than draining on) so the outer loop resubscribes and the next cycle's
    /// backfill re-covers the channel *without* waiting for an unrelated
    /// stream-end or restart — otherwise a healthy never-resubscribing stream
    /// would strand that client (vouchers rejected `WrongChannel`) indefinitely
    /// (#751 MEDIUM-1). Resubscribe is paced by backoff so a *durable* store
    /// failure backs off exponentially instead of hot-looping the backfill.
    RecoveryPending,
}

/// Background lifecycle watcher: follow `ChannelOpened` / `ChannelSettled` /
/// `ChannelCloseInitiated`, restarting subscriptions with exponential backoff
/// on stream error. Mirrors `chain_staker_set::watcher_loop`.
// The three-way resubscribe dispatch (clean end / recovery-pending / RPC error,
// each with its own backoff cadence) puts this a hair over the cognitive-
// complexity bar; splitting it would scatter the backoff state machine.
#[allow(clippy::too_many_arguments, clippy::cognitive_complexity)]
async fn watcher_loop<P: Provider + Clone>(
    contract: PaymentChannel::PaymentChannelInstance<P>,
    self_address: Address,
    usdc_token: Address,
    handler: Arc<ClientHandler>,
    pending_store: Arc<dyn PendingSettleStore>,
    checkpoint_store: Arc<dyn WatcherCheckpointStore>,
    start_block: u64,
    metrics: Arc<Metrics>,
) {
    // Bring-up backfill range start (#762/#751). Stays `Some` until a backfill
    // succeeds, so a `get_logs` error on the first cycle retries on the next
    // resubscription; once `None`, later resubscriptions skip it.
    let mut backfill_from = Some(start_block);
    let mut backoff = WATCHER_INITIAL_BACKOFF;
    loop {
        match run_watcher_once(
            &contract,
            self_address,
            usdc_token,
            &handler,
            &pending_store,
            &checkpoint_store,
            &mut backfill_from,
            &metrics,
        )
        .await
        {
            Ok(CycleOutcome::StreamEnded) => {
                debug!("PaymentChannel watcher stream ended cleanly; resubscribing");
                backoff = WATCHER_INITIAL_BACKOFF;
            }
            Ok(CycleOutcome::RecoveryPending) => {
                // A live open failed to persist; `backfill_from` is re-armed.
                // Resubscribe so the next cycle's backfill re-covers it, but pace
                // it with backoff so a persistent store failure doesn't hot-loop.
                warn!(
                    backoff_secs = backoff.as_secs(),
                    "watcher holding checkpoint after a failed open-persist; resubscribing to re-run the backfill"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(WATCHER_MAX_BACKOFF);
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
#[allow(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::too_many_arguments
)]
async fn run_watcher_once<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    self_address: Address,
    usdc_token: Address,
    handler: &Arc<ClientHandler>,
    pending_store: &Arc<dyn PendingSettleStore>,
    checkpoint_store: &Arc<dyn WatcherCheckpointStore>,
    backfill_from: &mut Option<u64>,
    metrics: &Arc<Metrics>,
) -> Result<CycleOutcome> {
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

    // Monotonic high-water mark of the block persisted to the scan checkpoint
    // (#751). Seeded from the stored value so it never regresses across watcher
    // cycles (a resubscribe resumes from where the last cycle left off rather
    // than re-writing block 0 on its first live event); a read failure defaults
    // to 0, which only risks one redundant write. `advance_checkpoint` keeps it
    // monotonic from here.
    let mut checkpoint_hw = match checkpoint_store.load_last_seen_block() {
        Ok(v) => v.unwrap_or(0),
        Err(err) => {
            warn!(%err, "failed to read watcher scan checkpoint; assuming none for this cycle");
            0
        }
    };

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
        // The gap is now scanned through `to`; persist it so the next boot
        // resumes from here (the #751 downtime floor). `to` is the live-filter
        // install block, i.e. at/after the seeded floor, so this never regresses
        // the stored value. Best-effort: a failed write only widens the next
        // rescan.
        checkpoint_hw = to;
        if let Err(err) = checkpoint_store.record_last_seen_block(to) {
            warn!(%err, block = to, "failed to persist watcher scan checkpoint after backfill");
        }
    }

    loop {
        tokio::select! {
            ev = opened.next() => match ev {
                Some(Ok((event, log))) => {
                    // The scan checkpoint is advanced ONLY here, on a successful
                    // persist (#751). A `ChannelOpened` is the sole registration
                    // delivery, and it is the only event that proves we scanned +
                    // registered opens through its block — the other arms (and
                    // foreign-channel events) say nothing about whether an earlier
                    // open of *ours* was persisted, so advancing the checkpoint
                    // from them could leap past an open that later fails and strand
                    // the channel (vouchers rejected `WrongChannel` forever). The
                    // `opened` stream is block-ordered, so the per-cycle hold below
                    // suffices against a later same-stream open.
                    match apply_channel_opened(handler, self_address, usdc_token, &event, false)
                        .await
                    {
                        Ok(()) => advance_checkpoint(
                            checkpoint_store,
                            log.block_number,
                            &mut checkpoint_hw,
                        ),
                        Err(err) => {
                            metrics.watcher_persist_failure();
                            warn!(
                                %err,
                                channel_id = %event.channelId,
                                "failed to persist opened channel; holding scan checkpoint below its block and resubscribing so the backfill re-covers it",
                            );
                            // `None` block (unconfirmed) → fall back to the current
                            // floor, conservatively re-scanning from there.
                            let failed = log.block_number.unwrap_or(checkpoint_hw);
                            // Re-arm the bring-up backfill from the failed block so the
                            // resubscribe forced just below re-scans and re-registers
                            // the channel. The persisted checkpoint already stays at
                            // or below `failed` — only the `opened` arm advances it,
                            // the stream is block-ordered, and we return before any
                            // higher-block open processes — so a restart's
                            // checkpoint-floored backfill also re-covers it.
                            *backfill_from =
                                Some(backfill_from.map_or(failed, |b| b.min(failed)));
                            // Don't drain on: return so `watcher_loop` resubscribes
                            // and the next cycle's backfill recovers the channel now,
                            // not on some unrelated future stream-end/restart (#751
                            // MEDIUM-1). Returning here (rather than holding the
                            // checkpoint and draining on) is also what keeps the
                            // checkpoint safe without an in-cycle hold. Backoff-paced,
                            // so a durable store failure backs off instead of
                            // hot-looping.
                            return Ok(CycleOutcome::RecoveryPending);
                        }
                    }
                }
                Some(Err(e)) => return Err(e).context("ChannelOpened stream"),
                None => return Ok(CycleOutcome::StreamEnded),
            },
            ev = topped_up.next() => match ev {
                Some(Ok((event, _log))) => {
                    // `ChannelToppedUp` is not provider-indexed; `update_channel_deposit`
                    // is a no-op for channels this node does not track. Does NOT
                    // advance the scan checkpoint — only the `opened` arm does.
                    if let Err(err) = handler
                        .update_channel_deposit(event.channelId, event.newDeposit)
                        .await
                    {
                        metrics.watcher_persist_failure();
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
                None => return Ok(CycleOutcome::StreamEnded),
            },
            ev = settled.next() => match ev {
                Some(Ok((event, _log))) => {
                    if event.provider != self_address {
                        continue;
                    }
                    if let Err(err) = handler.forget_channel(event.channelId).await {
                        metrics.watcher_persist_failure();
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
                None => return Ok(CycleOutcome::StreamEnded),
            },
            ev = close_initiated.next() => match ev {
                Some(Ok((event, _log))) => {
                    // Dispute monitor is deferred (#324); observe-only. Does NOT
                    // advance the scan checkpoint — only the `opened` arm does.
                    debug!(
                        channel_id = %event.channelId,
                        initiator = %event.initiator,
                        "ChannelCloseInitiated observed (dispute monitor deferred, #324)"
                    );
                }
                Some(Err(e)) => return Err(e).context("ChannelCloseInitiated stream"),
                None => return Ok(CycleOutcome::StreamEnded),
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

/// Bring-up backfill (#762/#751): register every `ChannelOpened` this node
/// provides in `[from_block, to_block]`, closing both the sub-millisecond
/// bring-up race (#762) and — when `from_block` comes from the persisted scan
/// checkpoint — the across-restart downtime gap (#751).
///
/// The range is walked in [`MAX_BACKFILL_BLOCK_SPAN`]-block windows so a long
/// downtime gap doesn't exceed the `eth_getLogs` range/result caps RPC providers
/// enforce; each window is one `get_logs` (alloy's `.query()`).
///
/// Every failure mode returns an `Err` so the caller retries the whole backfill
/// via the watcher backoff (with `backfill_from` still set): an invalid
/// `from > to` range ([`check_backfill_range`]), the `get_logs` RPC, and — see
/// [`apply_channel_opened`] — a per-channel persist failure (the backfill is the
/// sole delivery of a gap channel, so swallowing it would permanently drop the
/// channel). Only `ChannelOpened` is backfilled; see the module header for why
/// an in-window top-up/settle is out of scope (#751).
async fn backfill_opened_channels<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    self_address: Address,
    usdc_token: Address,
    handler: &Arc<ClientHandler>,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
    check_backfill_range(from_block, to_block)?;
    for (window_start, window_end) in
        backfill_windows(from_block, to_block, MAX_BACKFILL_BLOCK_SPAN)
    {
        backfill_window(
            contract,
            self_address,
            usdc_token,
            handler,
            window_start,
            window_end,
        )
        .await?;
    }
    Ok(())
}

/// Scan and register one bounded `[from_block, to_block]` window of
/// `ChannelOpened` logs. Factored out of [`backfill_opened_channels`] so the
/// windowing loop stays readable; the per-window failure policy is identical
/// (propagate so the whole backfill retries).
async fn backfill_window<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    self_address: Address,
    usdc_token: Address,
    handler: &Arc<ClientHandler>,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
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
        "scanned ChannelOpened logs for watcher backfill window"
    );
    for (event, _log) in logs {
        let channel_id = event.channelId;
        apply_channel_opened(handler, self_address, usdc_token, &event, true)
            .await
            .with_context(|| format!("backfill register channel {channel_id}"))?;
    }
    Ok(())
}

/// Best-effort advance of the persisted scan checkpoint to a processed log's
/// block (#751). Monotonic via the `high_water` guard (seeded from the stored
/// value, so it never regresses across cycles) — a burst of events in nearby
/// blocks costs at most one fsync per new block. A failed write is logged, not
/// fatal — per the [`WatcherCheckpointStore`] contract a lost checkpoint only
/// widens the next rescan, never narrows it.
///
/// # Caller contract (load-bearing — #751 MEDIUM-2)
///
/// The anti-strand guarantee rests on two properties of *how* this is called,
/// which the function cannot enforce from its arguments alone. A refactor that
/// merges a second event source into the advance, or feeds blocks out of order,
/// silently breaks them — so they are spelled out here rather than left implicit
/// at the call site:
///
/// 1. **Sole caller is the `opened` arm, on a successful register.** Only a
///    successful `ChannelOpened` proves opens were scanned + persisted through
///    its block. Advancing from any other arm (or on a foreign-channel event)
///    could persist the checkpoint past a block whose open *of ours* later fails
///    to persist, stranding that channel (vouchers rejected `WrongChannel` until
///    an unrelated rescan).
/// 2. **The `opened` stream is strictly block-ordered.** On a persist failure
///    the cycle returns immediately (see the `opened` arm), so every advance in a
///    cycle precedes its first failure — and block-ordering then guarantees those
///    advanced blocks are all `<=` the failed block. The persisted checkpoint
///    therefore never exceeds the failed block, so a restart's checkpoint-floored
///    backfill re-covers it. Feeding blocks out of order would let an advance
///    overshoot a later-failing open by more than `REORG_MARGIN_BLOCKS` and
///    strand it across a crash-before-resubscribe.
fn advance_checkpoint(
    checkpoint_store: &Arc<dyn WatcherCheckpointStore>,
    block: Option<u64>,
    high_water: &mut u64,
) {
    let Some(block) = block else {
        return;
    };
    if block <= *high_water {
        return;
    }
    *high_water = block;
    if let Err(err) = checkpoint_store.record_last_seen_block(block) {
        warn!(%err, block, "failed to persist watcher scan checkpoint");
    }
}

/// Mutable debounce bookkeeping for [`DebouncedCheckpointStore`], guarded by a
/// single `std::sync::Mutex`. The lock is only ever held for the brief duration
/// of a `record`/`flush` decision (no `.await` inside), so a sync mutex is the
/// right primitive.
struct DebounceState {
    /// The block last forwarded to a *durable* write on the inner store, or
    /// `None` if nothing has been persisted in this process yet. Also the floor
    /// against which the block-cadence threshold is measured.
    last_persisted: Option<u64>,
    /// Monotonic [`Instant`] of the last durable write, for the time-based
    /// threshold. Seeded at construction so the first interval is measured from
    /// service start.
    last_persist_at: Instant,
    /// The highest block buffered but not yet durably written. Monotonic
    /// (`record` only raises it). `None` once flushed/forwarded — i.e. equal to
    /// `last_persisted`.
    pending: Option<u64>,
    /// Whether the first in-process `record` has run. Until it does we lazily
    /// fold the inner store's existing checkpoint into `last_persisted` (the
    /// constructor stays I/O-free), and we force that first record durable so a
    /// restart re-anchors the floor to the live scan position promptly rather
    /// than lingering at a stale on-disk value.
    seeded: bool,
}

/// Debouncing decorator over a [`WatcherCheckpointStore`] (#784). Buffers
/// `record_last_seen_block` in memory and forwards a durable write only when the
/// buffered block is at least `flush_blocks` ahead of the last persisted value
/// *or* at least `flush_interval` has elapsed since the last durable write —
/// cutting the per-block fsync amplification the live `opened` arm would
/// otherwise pay on a backlog drain or a high-fan-out provider.
///
/// **Durability contract preserved.** The persisted value is only ever a *floor*
/// for the resume backfill (`resolve_backfill_start` rewinds it by
/// `REORG_MARGIN_BLOCKS` and `register_open_channel` is idempotent), so a
/// buffered-but-not-yet-fsynced advance that a crash loses merely widens the next
/// rescan — never narrows it. The forwarded value stays monotonic because
/// `record` only raises `pending`, and the watcher's own `advance_checkpoint`
/// high-water guard never feeds a lower block. [`Self::flush`] forces the
/// buffered value out and is wired into graceful shutdown so steady-state
/// progress survives a clean stop.
///
/// `load_last_seen_block` returns the max of the inner store's value and any
/// in-process buffered block, so a watcher resubscribe within the same process
/// resumes from the tightest known floor rather than re-reading a stale persisted
/// value (still safe either way; this just avoids a redundant in-process
/// re-scan). On a fresh boot the buffer is empty, so it reads through to the
/// inner store unchanged.
pub struct DebouncedCheckpointStore {
    inner: Arc<dyn WatcherCheckpointStore>,
    flush_blocks: u64,
    flush_interval: Duration,
    /// Clock source, injectable so the time-based threshold is unit-testable
    /// without sleeping. Production uses [`Instant::now`].
    clock: Box<dyn Fn() -> Instant + Send + Sync>,
    state: std::sync::Mutex<DebounceState>,
}

impl std::fmt::Debug for DebouncedCheckpointStore {
    // Manual impl: the boxed clock closure and the `dyn` inner store are not
    // `Debug`. Surface the static thresholds and the current buffered/persisted
    // floors (lock recovered from poison rather than panicking).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (pending, last_persisted) = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (state.pending, state.last_persisted)
        };
        f.debug_struct("DebouncedCheckpointStore")
            .field("flush_blocks", &self.flush_blocks)
            .field("flush_interval", &self.flush_interval)
            .field("pending", &pending)
            .field("last_persisted", &last_persisted)
            .finish_non_exhaustive()
    }
}

impl DebouncedCheckpointStore {
    /// Wrap `inner` with the production debounce thresholds
    /// (`CHECKPOINT_FLUSH_BLOCKS` / `CHECKPOINT_FLUSH_INTERVAL`) and the
    /// real wall clock.
    #[must_use]
    pub fn new(inner: Arc<dyn WatcherCheckpointStore>) -> Self {
        Self::with_params(
            inner,
            CHECKPOINT_FLUSH_BLOCKS,
            CHECKPOINT_FLUSH_INTERVAL,
            Box::new(Instant::now),
        )
    }

    /// Construct with explicit thresholds and clock — the seam the unit tests
    /// drive to exercise the block-cadence and time-cadence paths deterministically.
    fn with_params(
        inner: Arc<dyn WatcherCheckpointStore>,
        flush_blocks: u64,
        flush_interval: Duration,
        clock: Box<dyn Fn() -> Instant + Send + Sync>,
    ) -> Self {
        let now = clock();
        Self {
            inner,
            flush_blocks,
            flush_interval,
            clock,
            state: std::sync::Mutex::new(DebounceState {
                last_persisted: None,
                last_persist_at: now,
                pending: None,
                seeded: false,
            }),
        }
    }

    /// Forward `block` to the inner store and reset the debounce window. Caller
    /// holds `state`. On a durable-write error the buffered `pending` is left
    /// intact (and `last_persisted`/`last_persist_at` unchanged) so the next
    /// `record` or `flush` retries — a failed write must not advance the
    /// in-memory floor past what actually reached disk.
    fn persist_locked(&self, state: &mut DebounceState, block: u64) -> Result<(), StoreError> {
        self.inner.record_last_seen_block(block)?;
        state.last_persisted = Some(block);
        state.last_persist_at = (self.clock)();
        state.pending = None;
        Ok(())
    }
}

impl WatcherCheckpointStore for DebouncedCheckpointStore {
    fn load_last_seen_block(&self) -> Result<Option<u64>, StoreError> {
        let persisted = self.inner.load_last_seen_block()?;
        // Sync lock, dropped before return; never held across an await. Recover a
        // poisoned lock rather than panicking (anti-panic policy).
        let pending = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending;
        Ok(match (persisted, pending) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        })
    }

    fn record_last_seen_block(&self, block: u64) -> Result<(), StoreError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // On the first in-process record, fold the inner store's existing
        // checkpoint into `last_persisted`. The constructor leaves it `None`
        // (staying I/O-free and infallible), so without this a fresh wrapper
        // around an *already-populated* inner store could forward a block BELOW
        // its existing checkpoint and regress the on-disk floor. Seeding the
        // high-water guard with that floor keeps the monotonic-floor contract
        // for any caller, not just the watcher loop (which only feeds increasing
        // blocks). `first_record` is captured before flipping the flag so we can
        // still force that first advance durable (re-anchor after a restart).
        let first_record = !state.seeded;
        if first_record {
            state.seeded = true;
            state.last_persisted = self.inner.load_last_seen_block()?;
        }
        // Buffer the latest block (monotonic: never lower an already-buffered or
        // already-persisted floor).
        let highest = state
            .pending
            .max(state.last_persisted)
            .unwrap_or(0)
            .max(block);
        state.pending = Some(highest);

        // Nothing to persist if the guarded high-water is already at/below the
        // durable floor — an out-of-order or lower-than-floor record. Skip the
        // write entirely (no regression, no redundant fsync) regardless of the
        // first-record rule below.
        if state.last_persisted == Some(highest) {
            return Ok(());
        }

        let blocks_ahead = highest.saturating_sub(state.last_persisted.unwrap_or(0));
        let elapsed = (self.clock)().saturating_duration_since(state.last_persist_at);
        // The first record that actually advances the floor always goes through,
        // re-anchoring the durable checkpoint to the live scan position promptly
        // after a (re)start; thereafter debounce on either threshold.
        let due =
            first_record || blocks_ahead >= self.flush_blocks || elapsed >= self.flush_interval;
        if due {
            self.persist_locked(&mut state, highest)
        } else {
            Ok(())
        }
    }

    fn flush(&self) -> Result<(), StoreError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Only a buffered block strictly above the persisted floor needs a write.
        match state.pending {
            Some(block) if state.last_persisted != Some(block) => {
                self.persist_locked(&mut state, block)
            }
            _ => Ok(()),
        }
    }
}

/// Redemption task: `withdraw` a channel's accrued claim once it crosses the
/// threshold, driven by two sources — advisory hints from the voucher-accept
/// path and a low-frequency self-tick (#751) that scans every channel so a
/// dropped hint can never strand an above-threshold claim. Ends cleanly when
/// every hint sender is dropped (the shutdown path aborts it first via
/// [`PaymentChannelService::quiesce_redeemer`]).
#[allow(clippy::too_many_arguments)]
async fn redeemer_loop<P: Provider + Clone>(
    contract: PaymentChannel::PaymentChannelInstance<P>,
    store: Arc<dyn ChannelStateStore>,
    pending_store: Arc<dyn PendingSettleStore>,
    handler: Arc<ClientHandler>,
    self_address: Address,
    redeem_threshold: U256,
    auto_settle: AutoSettleConfig,
    mut redeem_rx: mpsc::Receiver<ChannelId>,
    metrics: Arc<Metrics>,
) {
    // Per-channel cache of the last-known on-chain `(withdrawnAmount,
    // claimedNonce)`. Both are only ever advanced by this node's own
    // `withdraw`/`closeChannel` transactions, so the cache is exact once seeded
    // and is always `<=` the true on-chain values — letting us skip the
    // `getChannel` RPC for hints whose accrued claim AND nonce span are
    // provably still below every enabled threshold (avoids RPC spam under
    // active per-MB voucher streaming, including for nodes that opt into
    // auto-settlement). A cache miss estimates both from zero, so the first
    // hint per channel still does one RPC.
    let mut withdrawn_cache: HashMap<ChannelId, (U256, U256)> = HashMap::new();
    let mut ticker = tokio::time::interval(REDEEM_TICK_INTERVAL);
    // Skip the immediate first tick: nothing has accrued right after bootstrap,
    // and the bring-up backfill + first vouchers hint anyway.
    ticker.tick().await;
    loop {
        tokio::select! {
            hint = redeem_rx.recv() => match hint {
                Some(channel_id) => {
                    redeem_one(
                        &contract, &store, &pending_store, &handler, self_address,
                        redeem_threshold, auto_settle, channel_id, &mut withdrawn_cache,
                        &metrics,
                    )
                    .await;
                }
                // All hint senders dropped — the service is going away.
                None => break,
            },
            _ = ticker.tick() => {
                redeem_sweep(
                    &contract, &store, &pending_store, &handler, self_address,
                    redeem_threshold, auto_settle, &mut withdrawn_cache, &metrics,
                )
                .await;
            }
        }
    }
    debug!("PaymentChannel redeemer loop ended (all hint senders dropped)");
}

/// Attempt one channel's redemption, recording the failure metric + `warn!` on
/// error. Shared by the hint arm and the self-tick sweep.
#[allow(clippy::too_many_arguments)]
async fn redeem_one<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn ChannelStateStore>,
    pending_store: &Arc<dyn PendingSettleStore>,
    handler: &Arc<ClientHandler>,
    self_address: Address,
    redeem_threshold: U256,
    auto_settle: AutoSettleConfig,
    channel_id: ChannelId,
    withdrawn_cache: &mut HashMap<ChannelId, (U256, U256)>,
    metrics: &Arc<Metrics>,
) {
    if let Err(err) = try_redeem(
        contract,
        store,
        pending_store,
        handler,
        self_address,
        redeem_threshold,
        auto_settle,
        channel_id,
        withdrawn_cache,
        metrics,
    )
    .await
    {
        metrics.redemption_failure();
        warn!(%err, %channel_id, "redemption attempt failed");
    }
}

/// Self-tick sweep (#751): scan every persisted channel and redeem any that
/// crossed the threshold, independent of hints. The cached-`withdrawn`
/// pre-check in [`try_redeem`] short-circuits sub-threshold channels for free,
/// but the cache starts empty and a miss estimates withdrawn-from-zero, so it
/// can't short-circuit an above-threshold channel: the **first sweep after boot
/// issues one `getChannel` per above-threshold channel** (O(channels) RPC
/// fan-out), and likewise whenever fresh bytes push a previously-redeemed
/// channel back over the threshold. Subsequent ticks are cheap — a `getChannel`
/// warms the entry, and a successful `withdraw` seeds it to the voucher amount
/// so the channel short-circuits until it next crosses the threshold. At the
/// testnet node count this stays well within RPC budget at the 5-min
/// [`REDEEM_TICK_INTERVAL`]; revisit the cadence (or seed the cache at bootstrap)
/// if a node tracks enough channels to make the post-boot fan-out costly. Errors
/// are per-channel (logged in [`redeem_one`]); a store-load failure is logged and
/// skips this tick.
#[allow(clippy::too_many_arguments)]
async fn redeem_sweep<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn ChannelStateStore>,
    pending_store: &Arc<dyn PendingSettleStore>,
    handler: &Arc<ClientHandler>,
    self_address: Address,
    redeem_threshold: U256,
    auto_settle: AutoSettleConfig,
    withdrawn_cache: &mut HashMap<ChannelId, (U256, U256)>,
    metrics: &Arc<Metrics>,
) {
    let states = match store.load_all() {
        Ok(s) => s,
        Err(err) => {
            warn!(%err, "redeemer self-tick: failed to load channel state");
            return;
        }
    };
    for st in states {
        redeem_one(
            contract,
            store,
            pending_store,
            handler,
            self_address,
            redeem_threshold,
            auto_settle,
            st.channel_id,
            withdrawn_cache,
            metrics,
        )
        .await;
    }
}

/// The un-redeemed nonce SPAN driving the count trigger: the off-chain latest
/// voucher nonce minus the on-chain `claimedNonce` (advanced by
/// `withdraw`/`closeChannel`), saturating so a stale on-chain nonce never
/// underflows and clamped to `u64::MAX` if it somehow exceeds `u64`. This is the
/// nonce *span*, an UPPER BOUND on the un-redeemed voucher count — nonces may skip
/// values (ADR 003 §Voucher Nonce Convention), so it is not the exact count. Pure
/// so the derivation (incl. the saturating-to-zero stale-nonce case) is
/// unit-testable.
fn unredeemed_nonce_span(last_nonce: U256, claimed_nonce: U256) -> u64 {
    last_nonce
        .saturating_sub(claimed_nonce)
        .try_into()
        .unwrap_or(u64::MAX)
}

/// Evaluate the auto-settlement triggers (#742) for one channel and, if either
/// fired, `closeChannel` to secure the balance on-chain. Returns `true` iff a
/// close transaction landed — the caller then skips the `withdraw` path (the
/// channel is now `Closing`). A disabled config, a sub-threshold balance, or an
/// absent signature return `false` without firing, leaving the redeem path to run
/// as before; a FAILED close (send returned `None`, or the receipt
/// reverted/errored) also returns `false` but bumps `settlement_auto_failures`
/// first so the silent-failure of a revenue-protection feature is observable.
///
/// On a landed close the channel is **retired** from the handler (mirroring the
/// expiry-close path in [`try_close_for_expiry`]): the channel is now `Closing`,
/// so `withdraw`/`closeChannel` revert against it and any further bytes served
/// would be unredeemable. The `ChannelCloseInitiated` watcher arm is observe-only
/// (#324 deferred), so without this forget the node would keep accepting vouchers
/// against a channel it can no longer redeem until the eventual `ChannelSettled`.
/// `send_close` records the durable pending-settle entry so the sweep finalizes
/// the provider's remainder after the dispute window.
#[allow(clippy::too_many_arguments)]
async fn try_auto_settle_close<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    pending_store: &Arc<dyn PendingSettleStore>,
    handler: &Arc<ClientHandler>,
    auto_settle: &AutoSettleConfig,
    st: &ChannelState,
    claimed_nonce: U256,
    unredeemed: U256,
    metrics: &Arc<Metrics>,
) -> bool {
    let nonce_span = unredeemed_nonce_span(st.last_nonce(), claimed_nonce);
    if !should_auto_settle(auto_settle, unredeemed, nonce_span) {
        return false;
    }
    let Some(fut) = send_close(contract, pending_store, st).await else {
        // Trigger fired but the `closeChannel` submit failed — the at-risk
        // balance is NOT secured. Surface it (the success counter alone can't
        // distinguish "never crossed" from "crossed and every close failing").
        metrics.settlement_auto_failure();
        return false;
    };
    if !fut.await {
        // Trigger fired and the close was submitted, but the receipt reverted
        // or errored — same unsecured outcome, same signal.
        metrics.settlement_auto_failure();
        return false;
    }
    // The claim is now secured by the close (settles after the dispute window).
    // Retire the channel — drop it from the handler so we stop serving a channel
    // we can no longer redeem against (mirrors `try_close_for_expiry`).
    let forget_result = handler.forget_channel(st.channel_id).await;
    record_auto_settle_close_outcome(
        forget_result,
        metrics,
        st.channel_id,
        unredeemed,
        nonce_span,
    )
}

/// Record the metric + log for a landed auto-settle close, given the result of
/// the post-close `forget_channel`, and return the value `try_auto_settle_close`
/// must propagate (always `true` — the close landed, so the caller MUST skip the
/// `withdraw`, which would revert against a now-`Closing` channel).
///
/// The success counter / "retired" log are gated on a *successful* forget so the
/// metric reflects a fully-retired close, not just a landed tx. A forget failure
/// leaves an unredeemable `Closing` channel (#742's leak), so it routes to the
/// failure counter instead — a stranded-but-closed channel stays observable
/// rather than masquerading as a secured success.
///
/// Split out so the forget-success vs forget-failure branch is unit-testable
/// without a provider (landing a real close requires the anvil e2e harness).
fn record_auto_settle_close_outcome(
    forget_result: Result<(), StoreError>,
    metrics: &Arc<Metrics>,
    channel_id: ChannelId,
    unredeemed: U256,
    nonce_span: u64,
) -> bool {
    if let Err(err) = forget_result {
        // Close landed but the channel was NOT retired — we keep serving an
        // unredeemable `Closing` channel. Surface it rather than claim success.
        warn!(%err, %channel_id, "auto-settle: forget after close failed; channel closed but not retired");
        metrics.settlement_auto_failure();
        return true; // close landed → still skip withdraw (channel is Closing)
    }
    metrics.settlement_auto_triggered();
    info!(
        %channel_id,
        unredeemed = %unredeemed,
        nonce_span,
        "auto-settlement trigger fired; closed + retired channel to secure balance (#742)"
    );
    true
}

/// Read the latest persisted voucher and the on-chain channel state; if the
/// un-redeemed balance crosses an auto-settlement trigger (#742) `closeChannel`
/// to secure it on-chain, otherwise if it meets the redeem threshold and the
/// channel is still open submit `withdraw`. Auto-settlement is checked first:
/// once a channel is large enough to settle, closing it (which secures the full
/// claim and starts the dispute window) supersedes a `withdraw` that would only
/// reclaim the same delta while leaving the channel — and its future exposure —
/// open.
#[allow(clippy::too_many_arguments)]
async fn try_redeem<P: Provider + Clone>(
    contract: &PaymentChannel::PaymentChannelInstance<P>,
    store: &Arc<dyn ChannelStateStore>,
    pending_store: &Arc<dyn PendingSettleStore>,
    handler: &Arc<ClientHandler>,
    self_address: Address,
    redeem_threshold: U256,
    auto_settle: AutoSettleConfig,
    channel_id: ChannelId,
    withdrawn_cache: &mut HashMap<ChannelId, (U256, U256)>,
    metrics: &Arc<Metrics>,
) -> Result<()> {
    let Some(st) = store
        .get(channel_id)
        .context("load channel state for redemption")?
    else {
        // Channel not (yet) persisted — e.g. a hint raced the ChannelOpened
        // consumer. The next voucher re-hints.
        return Ok(());
    };
    if st.last_nonce().is_zero() || st.last_signature().is_none() {
        return Ok(());
    }

    // Cheap pre-check against the cached on-chain `(withdrawn, claimedNonce)`
    // before any RPC. Both are `<=` the true on-chain values, so the derived
    // unredeemed value and nonce span are upper bounds — if every enabled
    // trigger's bound is below its threshold we can safely skip the
    // `getChannel` call entirely. This now holds even with auto-settlement
    // opted in: the value trigger keys off the same cached `withdrawnAmount`,
    // and the nonce-span trigger off the cached `claimedNonce`, so an idle
    // channel short-circuits regardless of which triggers are configured.
    let (cached_withdrawn, cached_nonce) = withdrawn_cache
        .get(&channel_id)
        .copied()
        .unwrap_or_default();
    if can_skip_redeem_rpc(
        st.last_amount(),
        st.last_nonce(),
        cached_withdrawn,
        cached_nonce,
        redeem_threshold,
        &auto_settle,
    ) {
        return Ok(());
    }

    let ch = contract
        .getChannel(channel_id)
        .call()
        .await
        .context("getChannel for redemption")?;
    withdrawn_cache.insert(channel_id, (ch.withdrawnAmount, ch.claimedNonce));
    // Defensive: only redeem channels this node provides and that are open.
    if ch.provider != self_address || !matches!(ch.status, PaymentChannel::Status::Open) {
        return Ok(());
    }
    let unredeemed = st.last_amount().saturating_sub(ch.withdrawnAmount);

    // Auto-settlement (#742) is checked before the redeem path: if a trigger
    // fired the channel is closed (securing the full claim + starting the
    // dispute window) and we return — the channel is now `Closing`, so the
    // `withdraw` below would revert.
    if try_auto_settle_close(
        contract,
        pending_store,
        handler,
        &auto_settle,
        &st,
        ch.claimedNonce,
        unredeemed,
        metrics,
    )
    .await
    {
        return Ok(());
    }

    // try_auto_settle_close returned false: either below threshold, or the close
    // failed (failure already surfaced via settlement_auto_failure). The channel is
    // still Open, so fall through to a best-effort withdraw — reclaim the delta this
    // tick; the auto-settle close retries on the next sweep.
    if unredeemed < redeem_threshold {
        return Ok(());
    }

    // The guard above returned on `last_signature().is_none()`, so this is Some.
    let Some(sig_bytes) = st.last_signature() else {
        return Ok(());
    };
    let sig = Bytes::from(normalize_voucher_signature(sig_bytes));
    let receipt = contract
        .withdraw(
            channel_id,
            st.last_amount(),
            st.last_nonce(),
            st.last_bytes_delivered(),
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
    // NOT seed the cache to `st.last_amount()` (that would make every later hint
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
    // amount and `claimedNonce` to the voucher nonce (strict watermark advance,
    // PaymentChannel.withdraw); reflect both in the cache so the next hints
    // short-circuit on the value AND nonce-span bounds.
    withdrawn_cache.insert(channel_id, (st.last_amount(), st.last_nonce()));
    info!(
        %channel_id,
        tx = %receipt.transaction_hash,
        unredeemed = %unredeemed,
        amount = %st.last_amount(),
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
    metrics: Arc<Metrics>,
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
        settle_pass(&contract, &pending_store, unix_now(), &metrics).await;
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
    if st.last_nonce().is_zero() || st.last_signature().is_none() {
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
        || st
            .last_amount()
            .saturating_sub(ch.withdrawnAmount)
            .is_zero()
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
    metrics: &Arc<Metrics>,
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
        try_settle(contract, pending_store, entry.channel_id, metrics).await;
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
    metrics: &Arc<Metrics>,
) {
    let send = match contract.settleChannel(channel_id).send().await {
        Ok(p) => p,
        Err(err) => {
            metrics.settlement_finalize_transient_send();
            warn!(
                %err, %channel_id, outcome = "transient_send",
                "settleChannel send failed; will retry next sweep"
            );
            return;
        }
    };
    let receipt = match send.get_receipt().await {
        Ok(r) => r,
        Err(err) => {
            metrics.settlement_finalize_transient_receipt();
            warn!(
                %err, %channel_id, outcome = "transient_receipt",
                "settleChannel receipt failed; will retry next sweep"
            );
            return;
        }
    };
    let landed = receipt.status();
    record_settle_receipt_outcome(metrics, landed);
    if landed {
        info!(
            %channel_id,
            tx = %receipt.transaction_hash,
            outcome = "ok",
            "settled channel; routed provider remainder through FeeRouter"
        );
        forget_pending_logged(pending_store, channel_id, metrics);
        return;
    }
    // Reverted: most likely `ChannelNotClosing` because another party already
    // finalized. Confirm before dropping the obligation.
    warn!(
        %channel_id,
        tx = %receipt.transaction_hash,
        outcome = "reverted",
        "settleChannel reverted; checking whether it was already finalized"
    );
    drop_pending_if_finalized(contract, pending_store, channel_id, metrics).await;
}

/// Count the terminal outcome of a `settleChannel` receipt: a landed receipt
/// (`status() == true`) ticks `settlement_finalize_ok`, a reverted one (`false`)
/// ticks the raw `settlement_finalize_reverted`. Split out from `try_settle` so
/// this high-consequence ok-vs-reverted mapping is unit-testable without a live
/// provider (mirrors `record_auto_settle_close_outcome`).
fn record_settle_receipt_outcome(metrics: &Arc<Metrics>, landed: bool) {
    if landed {
        metrics.settlement_finalize_ok();
    } else {
        metrics.settlement_finalize_reverted();
    }
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
    metrics: &Arc<Metrics>,
) {
    match contract.getChannel(channel_id).call().await {
        Ok(ch) => {
            record_revert_resolution(metrics, Some(&ch.status));
            match ch.status {
                PaymentChannel::Status::Closed => {
                    info!(%channel_id, "channel already settled elsewhere; dropping pending entry");
                    forget_pending_logged(pending_store, channel_id, metrics);
                }
                PaymentChannel::Status::Closing => {
                    // Re-stamp the gate to the current on-chain deadline
                    // (overwrites by contract). A no-op when unchanged; the fix
                    // when a dispute pushed the deadline out from under our
                    // stored value.
                    restamp_pending_logged(pending_store, channel_id, ch.disputeDeadline, metrics);
                }
                _ => {
                    // `Open` is unreachable for a channel we closed (close →
                    // Closing → Closed); seeing it means a contract /
                    // `channel_id` / reorg anomaly. `record_revert_resolution`
                    // already counted it as `confirm_failed` (an unresolved
                    // revert; `Status` has no `Debug` impl to log here). Emit a
                    // tripwire and keep the entry for the next sweep rather than
                    // dropping an obligation we can't explain.
                    warn!(
                        %channel_id,
                        "post-revert getChannel returned an unexpected non-terminal status \
                         (expected Closed or Closing); keeping entry"
                    );
                }
            }
        }
        Err(err) => {
            record_revert_resolution(metrics, None);
            warn!(%err, %channel_id, "post-revert getChannel failed; will retry next sweep");
        }
    }
}

/// Count how a reverted `settleChannel` resolved once the channel was re-read.
/// `Some(Closed)` → a co-settler finalized first (`..._confirmed_closed`,
/// benign; the pending entry is then dropped by the caller); `Some(Closing)` →
/// a dispute extended the window, so the caller re-stamps and retries
/// (`..._restamped`, benign); everything else — `None` (the confirming
/// `getChannel` read itself failed) and `Some(Open)` (unreachable for a closed
/// channel, so an anomaly) — is an unresolved revert counted as
/// `..._confirm_failed`, the genuinely-degraded signal. Folding the two
/// unresolved cases together keeps the exact invariant
/// `reverted == confirmed_closed + restamped + confirm_failed`. Split out so
/// the status→counter mapping is unit-testable without a provider.
fn record_revert_resolution(metrics: &Arc<Metrics>, status: Option<&PaymentChannel::Status>) {
    match status {
        Some(PaymentChannel::Status::Closed) => metrics.settlement_finalize_confirmed_closed(),
        Some(PaymentChannel::Status::Closing) => metrics.settlement_finalize_restamped(),
        _ => metrics.settlement_finalize_confirm_failed(),
    }
}

/// Drop a pending-settle entry, logging (not propagating) a store failure —
/// the settlement already landed on-chain, so a failed delete only risks a
/// redundant `settleChannel` next sweep (which reverts harmlessly and re-drops
/// via the `Closed`-status path).
fn forget_pending_logged(
    pending_store: &Arc<dyn PendingSettleStore>,
    channel_id: ChannelId,
    metrics: &Arc<Metrics>,
) {
    if let Err(err) = pending_store.forget_pending(channel_id) {
        metrics.settlement_pending_persist_failure();
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
    metrics: &Arc<Metrics>,
) {
    let entry = PendingSettle {
        channel_id,
        settle_after,
    };
    if let Err(err) = pending_store.record_pending(&entry) {
        metrics.settlement_pending_persist_failure();
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

    /// In-memory [`WatcherCheckpointStore`] for `advance_checkpoint` tests:
    /// records the last block written (sentinel `u64::MAX` = unset) and counts
    /// writes, all without `unwrap` (workspace anti-panic policy).
    struct RecordingCheckpointStore {
        /// `u64::MAX` sentinel = unset (never written). `#[derive(Default)]` would
        /// seed this to `0`, which `load_last_seen_block` would misreport as a
        /// real block-0 checkpoint — so `Default` is hand-written to the sentinel.
        stored: std::sync::atomic::AtomicU64,
        writes: std::sync::atomic::AtomicUsize,
        /// When set, `record_last_seen_block` returns an error without storing,
        /// to exercise the debouncer's retry-after-failure path.
        fail_writes: std::sync::atomic::AtomicBool,
    }

    impl Default for RecordingCheckpointStore {
        fn default() -> Self {
            Self {
                stored: std::sync::atomic::AtomicU64::new(u64::MAX),
                writes: std::sync::atomic::AtomicUsize::new(0),
                fail_writes: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    impl WatcherCheckpointStore for RecordingCheckpointStore {
        fn load_last_seen_block(&self) -> Result<Option<u64>, decdn_incentive::StoreError> {
            let v = self.stored.load(std::sync::atomic::Ordering::SeqCst);
            Ok((v != u64::MAX).then_some(v))
        }

        fn record_last_seen_block(&self, block: u64) -> Result<(), decdn_incentive::StoreError> {
            if self.fail_writes.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(decdn_incentive::StoreError::Backend("injected".into()));
            }
            self.stored
                .store(block, std::sync::atomic::Ordering::SeqCst);
            self.writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    impl RecordingCheckpointStore {
        fn writes(&self) -> usize {
            self.writes.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn set_fail(&self, fail: bool) {
            self.fail_writes
                .store(fail, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// A test clock whose `now` advances only when the test pushes it, so the
    /// time-based debounce threshold is exercised without sleeping. Cloned into
    /// the `Box<dyn Fn>` the store holds; both views share one atomic.
    #[derive(Clone, Default)]
    struct ManualClock {
        // Nanoseconds elapsed past a fixed `base` instant.
        elapsed_nanos: Arc<std::sync::atomic::AtomicU64>,
        base: Option<Instant>,
    }

    impl ManualClock {
        fn new() -> Self {
            Self {
                elapsed_nanos: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                base: Some(Instant::now()),
            }
        }

        fn advance(&self, by: Duration) {
            let add = u64::try_from(by.as_nanos()).unwrap_or(u64::MAX);
            self.elapsed_nanos
                .fetch_add(add, std::sync::atomic::Ordering::SeqCst);
        }

        fn now_fn(&self) -> Box<dyn Fn() -> Instant + Send + Sync> {
            let elapsed = Arc::clone(&self.elapsed_nanos);
            let base = self.base.unwrap_or_else(Instant::now);
            Box::new(move || {
                let n = elapsed.load(std::sync::atomic::Ordering::SeqCst);
                base + Duration::from_nanos(n)
            })
        }
    }

    /// Rapid `record` calls below both thresholds fsync only the first (which
    /// establishes the floor) and the stored value never regresses — it tracks
    /// the buffered high-water through `load`, while disk stays at the floor.
    #[test]
    fn debounce_buffers_rapid_records_below_thresholds() -> Result<(), StoreError> {
        let inner = Arc::new(RecordingCheckpointStore::default());
        let store = DebouncedCheckpointStore::with_params(
            Arc::clone(&inner) as Arc<dyn WatcherCheckpointStore>,
            512,
            Duration::from_secs(30),
            Box::new(Instant::now),
        );
        // First write establishes the floor immediately.
        store.record_last_seen_block(100)?;
        assert_eq!(inner.writes(), 1);
        assert_eq!(inner.load_last_seen_block()?, Some(100));

        // 50 more advances, each < 512 blocks ahead and within the interval: all
        // buffered, no further fsync.
        for b in 101..=150 {
            store.record_last_seen_block(b)?;
        }
        assert_eq!(
            inner.writes(),
            1,
            "rapid sub-threshold records must not fsync"
        );
        // Disk floor is still the first value...
        assert_eq!(inner.load_last_seen_block()?, Some(100));
        // ...but the decorator reports the tighter buffered floor.
        assert_eq!(store.load_last_seen_block()?, Some(150));
        Ok(())
    }

    /// A buffered advance never lowers the persisted floor even if a later
    /// `record` carries a lower block (defensive monotonicity).
    #[test]
    fn debounce_is_monotonic_against_out_of_order_records() -> Result<(), StoreError> {
        let inner = Arc::new(RecordingCheckpointStore::default());
        let store = DebouncedCheckpointStore::with_params(
            Arc::clone(&inner) as Arc<dyn WatcherCheckpointStore>,
            10,
            Duration::from_secs(30),
            Box::new(Instant::now),
        );
        store.record_last_seen_block(100)?;
        // Lower blocks are ignored by the high-water guard.
        store.record_last_seen_block(90)?;
        store.record_last_seen_block(50)?;
        store.flush()?;
        assert_eq!(inner.load_last_seen_block()?, Some(100));
        Ok(())
    }

    /// A fresh wrapper around an *already-populated* inner store must not regress
    /// its on-disk floor even if its very first `record` carries a lower block —
    /// the decorator seeds `last_persisted` from the inner store before applying
    /// the high-water guard, so the existing checkpoint is honored.
    #[test]
    fn debounce_seeds_floor_from_populated_inner_store() -> Result<(), StoreError> {
        let inner = Arc::new(RecordingCheckpointStore::default());
        // Inner store already has a checkpoint at 1000 (e.g. a prior boot).
        inner.record_last_seen_block(1000)?;
        let writes_before = inner.writes();

        let store = DebouncedCheckpointStore::with_params(
            Arc::clone(&inner) as Arc<dyn WatcherCheckpointStore>,
            10,
            Duration::from_secs(30),
            Box::new(Instant::now),
        );
        // First-ever record on the fresh wrapper carries a LOWER block.
        store.record_last_seen_block(500)?;
        store.flush()?;
        assert_eq!(
            inner.load_last_seen_block()?,
            Some(1000),
            "a lower first record must not regress the inner store's floor"
        );
        assert_eq!(
            inner.writes(),
            writes_before,
            "no redundant write below the existing floor"
        );
        // The decorator also reports the higher persisted floor, not the input.
        assert_eq!(store.load_last_seen_block()?, Some(1000));
        Ok(())
    }

    /// Crossing the block threshold forces a durable write of the buffered
    /// high-water.
    #[test]
    fn debounce_flushes_on_block_threshold() -> Result<(), StoreError> {
        let inner = Arc::new(RecordingCheckpointStore::default());
        let store = DebouncedCheckpointStore::with_params(
            Arc::clone(&inner) as Arc<dyn WatcherCheckpointStore>,
            512,
            Duration::from_secs(30),
            Box::new(Instant::now),
        );
        store.record_last_seen_block(100)?; // floor, write #1
        store.record_last_seen_block(500)?; // 400 < 512 ahead: buffered
        assert_eq!(inner.writes(), 1);
        assert_eq!(inner.load_last_seen_block()?, Some(100));
        store.record_last_seen_block(700)?; // 600 >= 512 ahead: flush
        assert_eq!(inner.writes(), 2);
        assert_eq!(inner.load_last_seen_block()?, Some(700));
        Ok(())
    }

    /// Crossing the time threshold forces a durable write even when the block
    /// delta is tiny — driven by the manual clock, no sleeping.
    #[test]
    fn debounce_flushes_on_time_threshold() -> Result<(), StoreError> {
        let inner = Arc::new(RecordingCheckpointStore::default());
        let clock = ManualClock::new();
        let store = DebouncedCheckpointStore::with_params(
            Arc::clone(&inner) as Arc<dyn WatcherCheckpointStore>,
            512,
            Duration::from_secs(30),
            clock.now_fn(),
        );
        store.record_last_seen_block(100)?; // floor, write #1 at t=0
        store.record_last_seen_block(101)?; // sub-threshold, buffered
        assert_eq!(inner.writes(), 1);
        clock.advance(Duration::from_secs(31)); // past the 30s interval
        store.record_last_seen_block(102)?; // time threshold trips: flush
        assert_eq!(inner.writes(), 2);
        assert_eq!(inner.load_last_seen_block()?, Some(102));
        Ok(())
    }

    /// `flush` (the graceful-shutdown path) persists the latest buffered value;
    /// a redundant flush with nothing newly buffered is a no-op.
    #[test]
    fn debounce_flush_persists_latest_then_is_idempotent() -> Result<(), StoreError> {
        let inner = Arc::new(RecordingCheckpointStore::default());
        let store = DebouncedCheckpointStore::with_params(
            Arc::clone(&inner) as Arc<dyn WatcherCheckpointStore>,
            512,
            Duration::from_secs(30),
            Box::new(Instant::now),
        );
        store.record_last_seen_block(100)?; // floor, write #1
        store.record_last_seen_block(300)?; // buffered
        store.record_last_seen_block(400)?; // buffered
        assert_eq!(inner.writes(), 1);
        store.flush()?; // shutdown flush persists 400, write #2
        assert_eq!(inner.writes(), 2);
        assert_eq!(inner.load_last_seen_block()?, Some(400));
        // Nothing new buffered → no extra write.
        store.flush()?;
        assert_eq!(inner.writes(), 2);
        Ok(())
    }

    /// `load_last_seen_block` returns the tighter of the persisted floor and any
    /// in-process buffered block; a fresh wrapper reads straight through.
    #[test]
    fn debounce_load_returns_max_of_persisted_and_pending() -> Result<(), StoreError> {
        let inner = Arc::new(RecordingCheckpointStore::default());
        inner.record_last_seen_block(50)?; // pre-existing persisted floor
        let store = DebouncedCheckpointStore::with_params(
            Arc::clone(&inner) as Arc<dyn WatcherCheckpointStore>,
            512,
            Duration::from_secs(30),
            Box::new(Instant::now),
        );
        // Fresh wrapper, nothing buffered: reads through to the inner floor.
        assert_eq!(store.load_last_seen_block()?, Some(50));
        store.record_last_seen_block(200)?; // sub-threshold buffer
        assert_eq!(store.load_last_seen_block()?, Some(200));
        Ok(())
    }

    /// A fresh wrapper over an inner store that *already* holds a persisted floor
    /// still forces a durable write on the FIRST in-process `record`, even when
    /// that record is well below both debounce thresholds. The decorator keys the
    /// block-cadence threshold off its own in-process `last_persisted` (seeded
    /// `None`), not the inner store's value it loads through — so the first write
    /// is always durable and the floor is re-anchored to the live scan position
    /// promptly after a restart, rather than lingering at the pre-existing
    /// (potentially much older) on-disk floor until a threshold trips. This is
    /// distinct from `debounce_load_returns_max_of_persisted_and_pending`, which
    /// only checks the read-through; here we assert the *write* behavior.
    #[test]
    fn debounce_first_record_writes_through_over_existing_floor() -> Result<(), StoreError> {
        let inner = Arc::new(RecordingCheckpointStore::default());
        inner.record_last_seen_block(50)?; // pre-existing persisted floor (write #1)
        assert_eq!(inner.writes(), 1);
        let store = DebouncedCheckpointStore::with_params(
            Arc::clone(&inner) as Arc<dyn WatcherCheckpointStore>,
            512,
            Duration::from_secs(30),
            Box::new(Instant::now),
        );
        // First in-process record is only 10 blocks ahead (< 512) and at t=0
        // (< 30s), so block- and time-cadence alone would buffer it — but the
        // `last_persisted.is_none()` first-write rule forces it durable anyway.
        store.record_last_seen_block(60)?;
        assert_eq!(inner.writes(), 2, "first in-process record must fsync");
        assert_eq!(inner.load_last_seen_block()?, Some(60));
        // The very next sub-threshold record is now genuinely buffered (the
        // in-process floor is established), proving the first-write was the
        // special case and not the steady-state behavior.
        store.record_last_seen_block(70)?;
        assert_eq!(inner.writes(), 2, "second sub-threshold record must buffer");
        assert_eq!(inner.load_last_seen_block()?, Some(60)); // disk floor unchanged
        assert_eq!(store.load_last_seen_block()?, Some(70)); // buffered high-water
        Ok(())
    }

    /// A failed durable write must not advance the in-memory floor: `pending`
    /// stays set so the next `record`/`flush` retries, and `load` keeps reporting
    /// the buffered value (the scan genuinely reached it). Once the inner store
    /// recovers, the buffered block is persisted.
    #[test]
    fn debounce_retries_after_a_failed_write() -> Result<(), StoreError> {
        let inner = Arc::new(RecordingCheckpointStore::default());
        let store = DebouncedCheckpointStore::with_params(
            Arc::clone(&inner) as Arc<dyn WatcherCheckpointStore>,
            10,
            Duration::from_secs(30),
            Box::new(Instant::now),
        );
        // First write would establish the floor, but the inner store is failing.
        inner.set_fail(true);
        assert!(store.record_last_seen_block(100).is_err());
        // Nothing reached disk, but the buffered floor is the attempted block.
        assert_eq!(inner.load_last_seen_block()?, None);
        assert_eq!(inner.writes(), 0);
        assert_eq!(store.load_last_seen_block()?, Some(100));
        // Recover and flush: the buffered block is now persisted.
        inner.set_fail(false);
        store.flush()?;
        assert_eq!(inner.writes(), 1);
        assert_eq!(inner.load_last_seen_block()?, Some(100));
        Ok(())
    }

    #[test]
    fn advance_checkpoint_is_monotonic() {
        let store: Arc<dyn WatcherCheckpointStore> = Arc::new(RecordingCheckpointStore::default());
        let mut hw = 0u64;
        // Advances and persists.
        advance_checkpoint(&store, Some(100), &mut hw);
        assert_eq!(hw, 100);
        // Monotonic: a lower or equal block does not move the high-water mark
        // (the seed-from-stored-value guard that keeps the checkpoint from
        // regressing across cycles, #751).
        advance_checkpoint(&store, Some(50), &mut hw);
        assert_eq!(hw, 100);
        advance_checkpoint(&store, Some(100), &mut hw);
        assert_eq!(hw, 100);
        // A higher block advances again.
        advance_checkpoint(&store, Some(140), &mut hw);
        assert_eq!(hw, 140);
        // A `None` block (unconfirmed log) is a no-op.
        advance_checkpoint(&store, None, &mut hw);
        assert_eq!(hw, 140);
    }

    #[test]
    fn resolve_backfill_start_policy() {
        // First-ever boot (no checkpoint): start at head.
        assert_eq!(resolve_backfill_start(None, 1_000, 128), 1_000);
        // Resume: checkpoint minus the reorg margin.
        assert_eq!(resolve_backfill_start(Some(1_000), 2_000, 128), 872);
        // Margin saturates at 0 for an early checkpoint (no underflow).
        assert_eq!(resolve_backfill_start(Some(10), 2_000, 128), 0);
        // Checkpoint ahead of a lagging RPC head clamps to head (range never
        // inverts).
        assert_eq!(resolve_backfill_start(Some(5_000), 1_000, 128), 1_000);
        // Checkpoint just below head: margin still applies (resume a little
        // before the checkpoint), since `1_050 - 128 = 922 < 1_000`.
        assert_eq!(resolve_backfill_start(Some(1_050), 1_000, 128), 922);
    }

    #[test]
    fn backfill_windows_split_the_range() {
        // Exact multiple: two full windows.
        assert_eq!(
            backfill_windows(0, 19, 10),
            vec![(0, 9), (10, 19)],
            "two contiguous inclusive windows"
        );
        // Remainder: a short final window.
        assert_eq!(
            backfill_windows(0, 25, 10),
            vec![(0, 9), (10, 19), (20, 25)]
        );
        // Single block and single-window-fits cases.
        assert_eq!(backfill_windows(1_000, 1_000, 10), vec![(1_000, 1_000)]);
        assert_eq!(backfill_windows(1_000, 1_005, 10), vec![(1_000, 1_005)]);
        // Non-zero start offset: windows stay aligned to `from`, not to 0.
        assert_eq!(backfill_windows(100, 119, 10), vec![(100, 109), (110, 119)]);
        // Degenerate inputs yield no windows (caller validates separately).
        assert!(backfill_windows(10, 5, 10).is_empty());
        assert!(backfill_windows(0, 10, 0).is_empty());
    }

    #[test]
    fn backfill_windows_cover_a_large_gap_without_overlap_or_gap() {
        // A long downtime gap: every block in [from, to] is covered exactly once
        // and no window exceeds the span.
        let (from, to, span) = (1_000u64, 55_321u64, MAX_BACKFILL_BLOCK_SPAN);
        let windows = backfill_windows(from, to, span);
        let mut expected_next = from;
        for (start, end) in &windows {
            assert_eq!(*start, expected_next, "windows must be contiguous");
            assert!(end >= start, "window end >= start");
            // Window length (end - start + 1) must not exceed the span cap;
            // written as `< span` to avoid clippy's int_plus_one.
            assert!(
                end - start < span,
                "window span {} exceeds cap {span}",
                end - start + 1
            );
            expected_next = end.saturating_add(1);
        }
        assert_eq!(
            windows.last().map(|w| w.1),
            Some(to),
            "the final window must reach `to`"
        );
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

    #[test]
    fn auto_settle_disabled_never_fires() {
        // Both triggers `None` (the default): no value or count ever settles,
        // so an opted-out node behaves exactly as before #742.
        let cfg = AutoSettleConfig::default();
        assert!(!should_auto_settle(&cfg, U256::from(u64::MAX), u64::MAX));
        assert!(!should_auto_settle(&cfg, U256::ZERO, 0));
    }

    #[test]
    fn auto_settle_value_threshold_boundary() {
        let cfg = AutoSettleConfig {
            value_threshold: Some(U256::from(1_000_000u64)),
            voucher_nonce_span_threshold: None,
        };
        // Strictly below the threshold: does not fire.
        assert!(!should_auto_settle(&cfg, U256::from(999_999u64), 9_999));
        // Exactly at the threshold settles (>= comparison).
        assert!(should_auto_settle(&cfg, U256::from(1_000_000u64), 0));
        // Above the threshold settles; nonce span is ignored when its
        // trigger is disabled.
        assert!(should_auto_settle(&cfg, U256::from(5_000_000u64), 0));
    }

    #[test]
    fn auto_settle_voucher_nonce_span_threshold_boundary() {
        let cfg = AutoSettleConfig {
            value_threshold: None,
            voucher_nonce_span_threshold: Some(100),
        };
        // A large value never fires when only the nonce-span trigger is set.
        assert!(!should_auto_settle(&cfg, U256::from(u64::MAX), 99));
        // Exactly at the span threshold settles (>= comparison).
        assert!(should_auto_settle(&cfg, U256::ZERO, 100));
        assert!(should_auto_settle(&cfg, U256::ZERO, 101));
    }

    #[test]
    fn auto_settle_either_trigger_fires_independently() {
        // Both set: a logical OR — either crossing fires.
        let cfg = AutoSettleConfig {
            value_threshold: Some(U256::from(1_000_000u64)),
            voucher_nonce_span_threshold: Some(100),
        };
        // Neither crossed.
        assert!(!should_auto_settle(&cfg, U256::from(500_000u64), 50));
        // Only the value crossed.
        assert!(should_auto_settle(&cfg, U256::from(1_000_000u64), 50));
        // Only the nonce span crossed.
        assert!(should_auto_settle(&cfg, U256::from(500_000u64), 100));
        // Both crossed.
        assert!(should_auto_settle(&cfg, U256::from(2_000_000u64), 200));
    }

    #[test]
    fn unredeemed_nonce_span_derivation() {
        // The span feeding the count trigger is `last_nonce − claimedNonce`
        // (an UPPER BOUND on the voucher count, since nonces may skip values),
        // saturating to zero on a stale on-chain nonce and clamping to
        // `u64::MAX` past the `u64` range.
        // Normal case: off-chain latest ahead of on-chain claimed.
        assert_eq!(
            unredeemed_nonce_span(U256::from(42u64), U256::from(7u64)),
            35
        );
        // Exactly caught up: span is zero (nothing un-redeemed).
        assert_eq!(unredeemed_nonce_span(U256::from(9u64), U256::from(9u64)), 0);
        // Stale on-chain nonce ABOVE the off-chain latest (e.g. a concurrent
        // close advanced claimedNonce past our cached voucher): saturating
        // subtraction yields zero, never an underflow.
        assert_eq!(
            unredeemed_nonce_span(U256::from(3u64), U256::from(10u64)),
            0
        );
        // A span exceeding u64 clamps rather than truncating.
        assert_eq!(
            unredeemed_nonce_span(U256::from(u64::MAX) + U256::from(5u64), U256::ZERO),
            u64::MAX
        );
    }

    /// Read one auto-settle counter from the `OpenMetrics` text (`<name> <n>`).
    /// Returns 0 if the line is absent (a fresh counter exports at zero, but be
    /// defensive). Avoids `unwrap`/indexing per the workspace anti-panic policy.
    fn auto_settle_counter(metrics: &Arc<Metrics>, name: &str) -> u64 {
        let Ok(text) = metrics.encode() else {
            return 0;
        };
        text.lines()
            .filter_map(|l| l.strip_prefix(name))
            .filter_map(|rest| rest.strip_prefix(' '))
            .find_map(|n| n.trim().parse::<u64>().ok())
            .unwrap_or(0)
    }

    fn triggered(metrics: &Arc<Metrics>) -> u64 {
        auto_settle_counter(metrics, "decdn_settlement_auto_triggered_total")
    }

    fn failures(metrics: &Arc<Metrics>) -> u64 {
        auto_settle_counter(metrics, "decdn_settlement_auto_failures_total")
    }

    #[test]
    fn auto_settle_close_outcome_forget_success_counts_as_secured() {
        // Baseline: a landed close whose post-close `forget_channel` succeeds is
        // the fully-retired path — success counter ticks, no failure, skip
        // withdraw.
        let metrics = Arc::new(Metrics::new());
        let channel_id = ChannelId::from([7u8; 32]);
        let returned =
            record_auto_settle_close_outcome(Ok(()), &metrics, channel_id, U256::from(15u64), 3);
        assert!(
            returned,
            "a landed close must skip the withdraw fallthrough"
        );
        assert_eq!(
            triggered(&metrics),
            1,
            "secured close ticks the success counter"
        );
        assert_eq!(failures(&metrics), 0, "a clean retire records no failure");
    }

    #[test]
    fn auto_settle_close_outcome_forget_failure_is_not_a_secured_success() {
        // Fix #1 regression (Alper review, #789): the close landed but
        // `forget_channel` errored, so the node keeps serving an unredeemable
        // `Closing` channel. This must NOT count as a secured success — it routes
        // to the FAILURE counter, leaves the success counter untouched, and still
        // returns `true` so the caller skips the withdraw (the channel is
        // `Closing`; a withdraw would revert).
        let metrics = Arc::new(Metrics::new());
        let channel_id = ChannelId::from([9u8; 32]);
        let returned = record_auto_settle_close_outcome(
            Err(StoreError::Backend("forget failed".into())),
            &metrics,
            channel_id,
            U256::from(15u64),
            3,
        );
        assert!(
            returned,
            "the close landed → must return true so the caller skips withdraw on a Closing channel"
        );
        assert_eq!(
            triggered(&metrics),
            0,
            "a forget failure must NOT increment the secured-success counter"
        );
        assert_eq!(
            failures(&metrics),
            1,
            "a stranded-but-closed channel must increment the failure counter so the leak is observable"
        );
    }

    // ---- settleChannel finalization-sweep counters (#810) ---------------------

    /// Read one finalization-sweep counter from the `OpenMetrics` text. Thin
    /// alias over [`auto_settle_counter`] (same `<name> <n>` parse) for tests
    /// that read the `settlement_finalize_*` family.
    fn finalize_counter(metrics: &Arc<Metrics>, name: &str) -> u64 {
        auto_settle_counter(metrics, name)
    }

    /// A `PendingSettleStore` whose writes always fail — drives the persist-
    /// failure counter in `forget_pending_logged` / `restamp_pending_logged`.
    struct FailingPendingStore;

    impl PendingSettleStore for FailingPendingStore {
        fn record_pending(&self, _entry: &PendingSettle) -> Result<(), StoreError> {
            Err(StoreError::Backend("forced write failure".into()))
        }
        fn load_pending(&self) -> Result<Vec<PendingSettle>, StoreError> {
            Ok(Vec::new())
        }
        fn forget_pending(&self, _channel_id: ChannelId) -> Result<(), StoreError> {
            Err(StoreError::Backend("forced delete failure".into()))
        }
    }

    #[test]
    fn settle_receipt_outcome_maps_landed_to_ok_and_revert_to_reverted() {
        // The highest-consequence mapping in the feature: a landed receipt is
        // the "settlement is landing" signal; a revert is the (raw) "did not
        // land" signal. A swap here would invert an operator's dashboard, so
        // assert each direction independently against a fresh registry.
        let landed = Arc::new(Metrics::new());
        record_settle_receipt_outcome(&landed, true);
        assert_eq!(
            finalize_counter(&landed, "decdn_settlement_finalize_ok_total"),
            1
        );
        assert_eq!(
            finalize_counter(&landed, "decdn_settlement_finalize_reverted_total"),
            0
        );

        let reverted = Arc::new(Metrics::new());
        record_settle_receipt_outcome(&reverted, false);
        assert_eq!(
            finalize_counter(&reverted, "decdn_settlement_finalize_ok_total"),
            0
        );
        assert_eq!(
            finalize_counter(&reverted, "decdn_settlement_finalize_reverted_total"),
            1
        );
    }

    #[test]
    fn revert_resolution_maps_each_status_to_its_counter() {
        // `Closed` = benign co-settler race; `Closing` = benign dispute
        // re-stamp; the two UNRESOLVED cases — `None` (read error) and
        // `Some(Open)` (unreachable-for-a-closed-channel anomaly) — both fold
        // into `confirm_failed` so the exact invariant
        // `reverted == confirmed_closed + restamped + confirm_failed` holds and
        // an anomaly is degraded-but-observable, never silently dropped.
        let closed = Arc::new(Metrics::new());
        record_revert_resolution(&closed, Some(&PaymentChannel::Status::Closed));
        assert_eq!(
            finalize_counter(&closed, "decdn_settlement_finalize_confirmed_closed_total"),
            1
        );
        assert_eq!(
            finalize_counter(&closed, "decdn_settlement_finalize_restamped_total"),
            0
        );
        assert_eq!(
            finalize_counter(&closed, "decdn_settlement_finalize_confirm_failed_total"),
            0
        );

        let closing = Arc::new(Metrics::new());
        record_revert_resolution(&closing, Some(&PaymentChannel::Status::Closing));
        assert_eq!(
            finalize_counter(&closing, "decdn_settlement_finalize_restamped_total"),
            1
        );
        assert_eq!(
            finalize_counter(&closing, "decdn_settlement_finalize_confirmed_closed_total"),
            0
        );

        // Read error → confirm_failed.
        let failed = Arc::new(Metrics::new());
        record_revert_resolution(&failed, None);
        assert_eq!(
            finalize_counter(&failed, "decdn_settlement_finalize_confirm_failed_total"),
            1
        );

        // Anomalous `Open` → also confirm_failed (degraded, not silent), and
        // never the benign buckets.
        let open = Arc::new(Metrics::new());
        record_revert_resolution(&open, Some(&PaymentChannel::Status::Open));
        assert_eq!(
            finalize_counter(&open, "decdn_settlement_finalize_confirm_failed_total"),
            1,
            "an unexpected Open must be counted as a degraded/unresolved revert"
        );
        for name in [
            "decdn_settlement_finalize_confirmed_closed_total",
            "decdn_settlement_finalize_restamped_total",
        ] {
            assert_eq!(
                finalize_counter(&open, name),
                0,
                "{name} must stay zero for the anomalous Open case"
            );
        }
    }

    #[test]
    fn reverted_partitions_exactly_into_resolution_counters() {
        // The additive invariant that justifies folding the two unresolved
        // cases (read error + anomalous Open) into `confirm_failed`:
        //   reverted == confirmed_closed + restamped + confirm_failed
        // The per-status tests above prove the mapping in isolation; this drives
        // both helpers in the SAME order `try_settle` → `drop_pending_if_finalized`
        // uses (bump `reverted`, then resolve once) across a mixed batch on ONE
        // registry and asserts the sum closes, so a future edit that skips or
        // double-bumps a resolution would break this even though the isolated
        // mapping tests still pass.
        let metrics = Arc::new(Metrics::new());
        let resolutions = [
            Some(PaymentChannel::Status::Closed),
            Some(PaymentChannel::Status::Closed),
            Some(PaymentChannel::Status::Closing),
            None,                               // read error → confirm_failed
            Some(PaymentChannel::Status::Open), // anomaly → confirm_failed
        ];
        for status in &resolutions {
            record_settle_receipt_outcome(&metrics, false);
            record_revert_resolution(&metrics, status.as_ref());
        }

        let reverted = finalize_counter(&metrics, "decdn_settlement_finalize_reverted_total");
        let confirmed_closed =
            finalize_counter(&metrics, "decdn_settlement_finalize_confirmed_closed_total");
        let restamped = finalize_counter(&metrics, "decdn_settlement_finalize_restamped_total");
        let confirm_failed =
            finalize_counter(&metrics, "decdn_settlement_finalize_confirm_failed_total");

        assert_eq!(reverted, 5, "five reverts were recorded");
        assert_eq!(
            reverted,
            confirmed_closed + restamped + confirm_failed,
            "every revert must resolve into exactly one resolution counter \
             (reverted={reverted}, confirmed_closed={confirmed_closed}, \
             restamped={restamped}, confirm_failed={confirm_failed})"
        );
        assert_eq!(confirmed_closed, 2, "two Closed re-reads");
        assert_eq!(restamped, 1, "one Closing re-read");
        assert_eq!(
            confirm_failed, 2,
            "read error + anomalous Open both fold here"
        );
    }

    #[test]
    fn pending_persist_failure_is_counted_for_forget_and_restamp() {
        // Both the forget (post-settle / already-Closed) and the re-stamp
        // (dispute-extended) store writes feed the SAME persist counter, the
        // way `watcher_persist_failure` lumps the watcher's persist sites.
        let store: Arc<dyn PendingSettleStore> = Arc::new(FailingPendingStore);
        let channel_id = ChannelId::from([3u8; 32]);
        let metrics = Arc::new(Metrics::new());

        forget_pending_logged(&store, channel_id, &metrics);
        assert_eq!(
            finalize_counter(&metrics, "decdn_settlement_pending_persist_failures_total"),
            1,
            "a failed forget must be counted, not just logged"
        );

        restamp_pending_logged(&store, channel_id, 123, &metrics);
        assert_eq!(
            finalize_counter(&metrics, "decdn_settlement_pending_persist_failures_total"),
            2,
            "a failed re-stamp feeds the same persist counter"
        );
    }

    #[test]
    fn pending_persist_success_does_not_count() {
        use decdn_incentive::MemoryPendingSettleStore;

        let store: Arc<dyn PendingSettleStore> = Arc::new(MemoryPendingSettleStore::new());
        let channel_id = ChannelId::from([4u8; 32]);
        let metrics = Arc::new(Metrics::new());

        // Both writes succeed against the in-memory store → no persist failure.
        restamp_pending_logged(&store, channel_id, 99, &metrics);
        forget_pending_logged(&store, channel_id, &metrics);
        assert_eq!(
            finalize_counter(&metrics, "decdn_settlement_pending_persist_failures_total"),
            0
        );
    }

    // ---- can_skip_redeem_rpc (cheap pre-check) --------------------------------

    /// Helper: a redeem threshold of one thousand.
    fn rt() -> U256 {
        U256::from(1_000u64)
    }

    #[test]
    fn skip_precheck_below_redeem_threshold_no_autosettle() {
        // est_unredeemed = 900 - 0 = 900 < 1_000: skip.
        let cfg = AutoSettleConfig::default();
        assert!(can_skip_redeem_rpc(
            U256::from(900u64),
            U256::from(50u64),
            U256::ZERO,
            U256::ZERO,
            rt(),
            &cfg,
        ));
    }

    #[test]
    fn no_skip_at_or_above_redeem_threshold_no_autosettle() {
        // est_unredeemed = 1_000 - 0 = 1_000, NOT < 1_000: must RPC.
        let cfg = AutoSettleConfig::default();
        assert!(!can_skip_redeem_rpc(
            U256::from(1_000u64),
            U256::from(50u64),
            U256::ZERO,
            U256::ZERO,
            rt(),
            &cfg,
        ));
    }

    #[test]
    fn skip_precheck_uses_cached_withdrawn_lower_bound() {
        // last_amount 5_000, cached withdrawn 4_500 => est_unredeemed 500 < 1_000.
        let cfg = AutoSettleConfig::default();
        assert!(can_skip_redeem_rpc(
            U256::from(5_000u64),
            U256::from(50u64),
            U256::from(4_500u64),
            U256::from(40u64),
            rt(),
            &cfg,
        ));
    }

    #[test]
    fn value_only_autosettle_can_still_skip() {
        // The regression the bots flagged: value-only auto-settle previously
        // forced an RPC every tick. With a value threshold of 600 and
        // est_unredeemed = 500, BOTH the redeem (1_000) and value (600) bounds
        // are below threshold, so the channel still short-circuits.
        let cfg = AutoSettleConfig {
            value_threshold: Some(U256::from(600u64)),
            voucher_nonce_span_threshold: None,
        };
        assert!(can_skip_redeem_rpc(
            U256::from(5_000u64),
            U256::from(50u64),
            U256::from(4_500u64), // est_unredeemed = 500
            U256::ZERO,
            rt(),
            &cfg,
        ));
    }

    #[test]
    fn value_only_autosettle_no_skip_when_value_threshold_crossable() {
        // est_unredeemed = 700 is below the redeem threshold (1_000) but at/above
        // the auto-settle value threshold (700): the value trigger could fire, so
        // we must NOT skip even though the redeem path alone would.
        let cfg = AutoSettleConfig {
            value_threshold: Some(U256::from(700u64)),
            voucher_nonce_span_threshold: None,
        };
        assert!(!can_skip_redeem_rpc(
            U256::from(700u64),
            U256::from(50u64),
            U256::ZERO,
            U256::ZERO,
            rt(),
            &cfg,
        ));
    }

    #[test]
    fn nonce_span_trigger_blocks_skip_until_cached_nonce_seeded() {
        // span threshold 10. Cache miss => cached_nonce 0 => est span = last_nonce
        // = 20 >= 10: cannot skip (must RPC to read claimedNonce), even though the
        // value bound (est_unredeemed 100 < 1_000) alone would allow it.
        let cfg = AutoSettleConfig {
            value_threshold: None,
            voucher_nonce_span_threshold: Some(10),
        };
        assert!(!can_skip_redeem_rpc(
            U256::from(100u64),
            U256::from(20u64),
            U256::ZERO,
            U256::ZERO,
            rt(),
            &cfg,
        ));
    }

    #[test]
    fn nonce_span_trigger_skips_once_cached_nonce_catches_up() {
        // Same config, but the cache now holds claimedNonce 15 (seeded by a prior
        // getChannel/withdraw). est span = 20 - 15 = 5 < 10 and est_unredeemed
        // 100 < 1_000: skip.
        let cfg = AutoSettleConfig {
            value_threshold: None,
            voucher_nonce_span_threshold: Some(10),
        };
        assert!(can_skip_redeem_rpc(
            U256::from(100u64),
            U256::from(20u64),
            U256::ZERO,
            U256::from(15u64),
            rt(),
            &cfg,
        ));
    }

    #[test]
    fn both_triggers_require_both_bounds_below() {
        let cfg = AutoSettleConfig {
            value_threshold: Some(U256::from(600u64)),
            voucher_nonce_span_threshold: Some(10),
        };
        // Value bound below (500) but nonce span at threshold (10): no skip.
        assert!(!can_skip_redeem_rpc(
            U256::from(5_000u64),
            U256::from(20u64),
            U256::from(4_500u64), // est_unredeemed 500 < 600
            U256::from(10u64),    // est span 10 >= 10
            rt(),
            &cfg,
        ));
        // Both below: skip.
        assert!(can_skip_redeem_rpc(
            U256::from(5_000u64),
            U256::from(20u64),
            U256::from(4_500u64), // est_unredeemed 500 < 600
            U256::from(15u64),    // est span 5 < 10
            rt(),
            &cfg,
        ));
    }
}
