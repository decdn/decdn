//! On-chain `PaymentPool` seller-settlement service (ADR 003).
//!
//! The node is the *provider* (payee) on its voucher lanes. This service closes
//! the on-chain half of the paid-delivery loop that the off-chain voucher layer
//! ([`decdn_incentive`]) and the `cdn/client/v1` handler leave open. A pool is
//! owner-owned and owner-closed: the node NEVER closes, disputes, or settles a
//! pool. Its only settlement primitive is redemption — `redeem` for a single
//! lane, `redeemMany` for a batch — plus register-once of each signer's
//! capability on that signer's first redemption.
//!
//! - **Paid-watermark watcher.** The paid side of every lane is driven by
//!   consuming `PoolRedeemed` events filtered on this node's own `provider`
//!   address (ADR 003 § Tracking owed vs. paid): each event sets the lane's paid
//!   cumulative to `newPaidCumulative`. `PoolToppedUp` re-drives a pool's lanes
//!   (a dry pool may have left `owed > paid`), and `PoolReclaimed` forgets the
//!   pool's lanes. The watcher reconciles like every other chain watcher —
//!   enumerate `PoolRedeemed` from a pinned block, then tail live, resyncing on a
//!   missed range — so paid is rebuilt from the event log, never guessed.
//! - **Redemption (threshold + on-shutdown).** On a redeem hint (a [`LaneKey`])
//!   emitted by the voucher-accept path, the node reads the lane's owed voucher
//!   and its cached paid watermark and submits `redeem` once `owed − paid`
//!   crosses a configurable threshold. A low-frequency self-tick sweeps every
//!   persisted lane into one `redeemMany` so a dropped hint never strands an
//!   above-threshold claim.
//! - **Close monitor.** A pool is owner-closed only. On a `PoolCloseInitiated`
//!   for a pool this node holds lanes against, the monitor redeems its highest
//!   voucher per lane before `disputeDeadline` (ADR 003 § Owner reclaims before a
//!   node redeems) — a node that has not redeemed by the deadline forfeits its
//!   outstanding vouchers.
//!
//! Buyer-side `openPool`/`topUp`/`reclaim` (node→node cache-miss pulls) is out of
//! scope here. Structurally this mirrors [`crate::dht::chain_staker_set`]: a
//! generic-over-`Provider` struct owning background tasks with exponential-backoff
//! poll retry — the paid-watermark watcher via its `WatcherHandle`, the redeemer
//! via a [`JoinHandle`] aborted on shutdown or drop.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::{SolEvent, SolValue};
use anyhow::{Context, Result};
use decdn_common::redact::sanitize_rpc_display;
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::{
    CheckpointKey, KeyedCheckpointStore, LaneKey, PoolId, PoolStateStore, StoreError,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::chain_events::REORG_MARGIN_BLOCKS;
use crate::chain_events::resumable_watcher::{
    self, Checkpoint, ColdStart, CursorStart, LogSink, WatcherConfig, WatcherHandle,
};
use crate::chain_events::shared_head::HeadSource;
use crate::handlers::client::ClientHandler;
use crate::metrics::{Metrics, metric_hook};
use crate::onchain_tx::{TxOutcome, send_and_await_receipt};

/// Capacity of the redeem-hint channel. Hints are advisory (a missed hint only
/// delays a redemption until the next voucher or self-tick sweep), so a bounded
/// channel that drops on overflow is acceptable — sized for a burst of concurrent
/// lanes without backpressuring the voucher-accept path.
pub const REDEEM_HINT_CAPACITY: usize = 256;

/// Bounded receipt wait for a redemption transaction. A stuck/dropped/replaced tx
/// must not wedge a background tick; a lapse yields [`TxOutcome::Timeout`], a
/// non-fatal failure (the tx may still mine later; the claim is at worst deferred,
/// never double-spent, because the on-chain lane watermark is monotone).
const REDEEM_RECEIPT_TIMEOUT: Duration = Duration::from_mins(3);

/// Ethereum recovery-id offset the on-chain `ECDSA.recover` requires (`v` ∈
/// {`27`, `28`}). The stored signature comes from
/// [`alloy::primitives::Signature::as_bytes`], which already encodes `v` as
/// `27 + y_parity`, so [`normalize_voucher_signature`] is a no-op on well-formed
/// signatures today — it exists as defense-in-depth in case the stored/wire
/// encoding ever switches to a raw `0`/`1` y-parity.
const ETH_V_OFFSET: u8 = 27;

/// Current Unix time in seconds for on-chain deadline comparisons. A broken
/// system clock (time before the epoch) yields `0`, which makes every pool look
/// still inside its grace window — the safe direction (attempt the redeem; the
/// contract's own `block.timestamp` check is the authoritative backstop).
pub(crate) fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Whether a capability/grace deadline has passed. `deadline == 0` means
/// "untracked / never" and is treated as not passed — the safe direction (keep
/// serving/redeeming) when the deadline is unknown.
pub(crate) const fn is_expired(now: u64, deadline: u64) -> bool {
    deadline != 0 && now >= deadline
}

/// Debounce thresholds for the watcher scan checkpoint (#784). The persisted
/// checkpoint is only a *floor* for the resume backfill: the resumable watcher's
/// `resolve_persisted_start` rounds it down by `REORG_MARGIN_BLOCKS` and every
/// sink is idempotent, so a checkpoint that lags the true scan position by a
/// bounded amount only ever *widens* the next rescan, never narrows it. `512` is
/// chosen so a backlog drain fsyncs ~once per 512 blocks instead of per block.
const CHECKPOINT_FLUSH_BLOCKS: u64 = 512;

/// Time-based companion to [`CHECKPOINT_FLUSH_BLOCKS`]: even a slow trickle of
/// events persists at least this often, bounding how many blocks a crash
/// re-scans when block-cadence alone would defer the write indefinitely.
const CHECKPOINT_FLUSH_INTERVAL: Duration = Duration::from_secs(30);

/// The owner-signed capability material a node attaches on a signer's FIRST
/// redemption to register `authorized[poolId][signer]` on-chain. `spending_cap`
/// and `expiry` also live on the lane's `decdn_incentive::LaneState`; `owner_sig` is the pool
/// owner's EIP-712 signature over the capability (`r‖s‖v`, or a longer
/// ERC-1271 payload), which the seller intake path persists when it accepts a
/// lane's first voucher. Every later redemption for that signer omits the
/// capability and rides the stored registration.
#[derive(Clone, Debug)]
pub struct CapabilityMaterial {
    /// The signer's cumulative spending cap (token base units).
    pub spending_cap: U256,
    /// Capability expiry (Unix seconds).
    pub expiry: u64,
    /// The pool owner's signature over the EIP-712 `Capability`.
    pub owner_sig: Bytes,
}

/// Source of the first-redemption registration material for a lane's signer.
///
/// `redeem`/`redeemMany` register a signer once, on its first redemption, from an
/// owner-signed capability. The lane's `decdn_incentive::LaneState` carries the signer's `cap`
/// and `expiry`, but not the owner's signature over the capability — that is
/// persisted by the seller voucher-intake path and surfaced here so the redeemer
/// can build the on-chain registration payload only when a signer is not yet
/// registered (`getAuthorization(poolId, signer).cap == 0`).
pub trait CapabilitySource: Send + Sync {
    /// The registration material for `key`'s signer, or `None` if this node holds
    /// no capability for it (in which case an unregistered signer cannot be
    /// redeemed and the lane is skipped until the material is available).
    fn registration_material(&self, key: &LaneKey) -> Option<CapabilityMaterial>;
}

/// The paid-cumulative watermark of every lane, keyed by [`LaneKey`]. Written
/// solely by the `PoolRedeemed` watcher (the single write path for the paid
/// side, ADR 003 § Tracking owed vs. paid) and read by the redeemer to compute
/// `unredeemed = owed − paid`. A `std::sync::Mutex`: the guard is only ever held
/// to read/insert a single entry, never across an `.await`.
#[derive(Clone, Default)]
pub struct PaidWatermarks {
    inner: Arc<Mutex<HashMap<LaneKey, U256>>>,
}

impl std::fmt::Debug for PaidWatermarks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self
            .inner
            .lock()
            .map_or_else(|p| p.into_inner().len(), |m| m.len());
        f.debug_struct("PaidWatermarks")
            .field("lanes", &len)
            .finish()
    }
}

impl PaidWatermarks {
    /// Set a lane's paid cumulative to the `PoolRedeemed` event's
    /// `newPaidCumulative`. Monotone on-chain, so this only ever advances.
    fn set(&self, key: LaneKey, paid: U256) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key, paid);
    }

    /// A lane's paid cumulative, or `U256::ZERO` if no `PoolRedeemed` has landed
    /// for it yet (the safe over-estimate of `unredeemed` — the on-chain redeem
    /// caps the increment and the event then corrects the cache).
    fn get(&self, key: &LaneKey) -> U256 {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .copied()
            .unwrap_or(U256::ZERO)
    }

    /// Drop a reclaimed pool's lane from the cache.
    fn forget(&self, key: &LaneKey) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
    }
}

/// Seller-side `PaymentPool` settlement service. Generic over the alloy
/// [`Provider`] (a wallet-filled provider is required for the `redeem` /
/// `redeemMany` write path). Cheap to construct; owns its background tasks.
pub struct PoolSettlementService<P: Provider + Clone + 'static> {
    contract: PaymentPool::PaymentPoolInstance<P>,
    redeem_tx: mpsc::Sender<LaneKey>,
    store: Arc<dyn PoolStateStore>,
    capabilities: Arc<dyn CapabilitySource>,
    paid: PaidWatermarks,
    self_address: Address,
    redeem_threshold: U256,
    metrics: Arc<Metrics>,
    /// The paid-watermark watcher, owning both its task and the shutdown token
    /// that stops it. Held (not `_`-dropped) so graceful `shutdown()` runs before
    /// the wrapped `AbortOnDrop` hard-stops the task.
    watcher: WatcherHandle,
    /// The redemption task handle. Held so shutdown can abort+await it before a
    /// final redeem sweep. `take()`n by [`Self::quiesce_redeemer`]; the [`Drop`]
    /// impl aborts whatever remains. A `std::sync::Mutex` (not `tokio`): the guard
    /// is only ever held to `take()` the handle, never across an `.await`.
    redeemer: std::sync::Mutex<Option<JoinHandle<()>>>,
    /// Held so graceful shutdown can force a final checkpoint flush (#784).
    checkpoint_store: Arc<dyn KeyedCheckpointStore>,
}

impl<P: Provider + Clone + 'static> PoolSettlementService<P> {
    /// Bootstrap the service: self-check the contract, then spawn the
    /// paid-watermark watcher + redemption task.
    ///
    /// # Errors
    ///
    /// Returns an error if the `usdc()` self-check call fails — a bad
    /// `payment_pool_address` or an unreachable RPC is fatal at bring-up.
    #[allow(clippy::too_many_arguments)]
    pub async fn bootstrap(
        provider: P,
        payment_pool_addr: Address,
        self_address: Address,
        store: Arc<dyn PoolStateStore>,
        checkpoint_store: Arc<dyn KeyedCheckpointStore>,
        handler: Arc<ClientHandler>,
        capabilities: Arc<dyn CapabilitySource>,
        redeem_threshold: U256,
        redeem_interval: Duration,
        event_poll_interval: Duration,
        head: Arc<dyn HeadSource>,
        metrics: Arc<Metrics>,
        redeem_tx: mpsc::Sender<LaneKey>,
        redeem_rx: mpsc::Receiver<LaneKey>,
    ) -> Result<Self> {
        let contract = PaymentPool::new(payment_pool_addr, provider);

        // Startup self-check: a cheap immutable view confirms the configured
        // address actually hosts the contract.
        let usdc_token = contract
            .usdc()
            .call()
            .await
            .with_context(|| format!("PaymentPool.usdc() self-check at {payment_pool_addr}"))?;
        info!(
            %payment_pool_addr,
            %usdc_token,
            %self_address,
            "PaymentPool settlement service bootstrap complete"
        );

        let paid = PaidWatermarks::default();

        // Paid-watermark watcher on the resumable `eth_getLogs` poller. The
        // backfill floor and downtime-gap resume are the cursor start's job: a
        // persisted `PoolRedeemed` checkpoint resumes across restarts; a
        // first-ever boot (cold store) anchors at head. `PoolRedeemed` is the
        // single write path for the paid side, so a drained-pool partial pay is
        // recorded exactly — the node never assumes its voucher cleared.
        let sink = PoolSettlementSink {
            contract: contract.clone(),
            self_address,
            store: Arc::clone(&store),
            handler,
            paid: paid.clone(),
            capabilities: Arc::clone(&capabilities),
            redeem_tx: redeem_tx.clone(),
            metrics: Arc::clone(&metrics),
        };
        let cfg = WatcherConfig::new(
            head,
            Filter::new()
                .address(payment_pool_addr)
                .event_signature(vec![
                    PaymentPool::PoolRedeemed::SIGNATURE_HASH,
                    PaymentPool::PoolToppedUp::SIGNATURE_HASH,
                    PaymentPool::PoolCloseInitiated::SIGNATURE_HASH,
                    PaymentPool::PoolReclaimed::SIGNATURE_HASH,
                ]),
            cursor_start(Arc::clone(&checkpoint_store)),
            event_poll_interval,
            "settlement",
        )
        .on_established(metric_hook(
            &metrics,
            Metrics::settlement_watcher_cycle_established,
        ))
        .on_backoff(metric_hook(
            &metrics,
            Metrics::settlement_watcher_backoff_started,
        ))
        .on_tick_success(metric_hook(&metrics, Metrics::settlement_watcher_tick))
        .on_task_panic(metric_hook(
            &metrics,
            Metrics::settlement_watcher_task_panicked,
        ));
        let watcher = resumable_watcher::spawn(contract.provider().clone(), cfg, move |_| sink);

        let redeemer = tokio::spawn(redeemer_loop(
            contract.clone(),
            Arc::clone(&store),
            Arc::clone(&capabilities),
            paid.clone(),
            self_address,
            redeem_threshold,
            redeem_interval,
            redeem_rx,
            Arc::clone(&metrics),
        ));

        Ok(Self {
            contract,
            redeem_tx,
            store,
            capabilities,
            paid,
            self_address,
            redeem_threshold,
            metrics,
            watcher,
            redeemer: std::sync::Mutex::new(Some(redeemer)),
            checkpoint_store,
        })
    }

    /// Sender the voucher-accept path uses to hint that a lane's accrued claim
    /// may have crossed the redemption threshold. Cloneable; dropping all senders
    /// simply ends the redemption task cleanly.
    #[must_use]
    pub fn redeem_hint_sender(&self) -> mpsc::Sender<LaneKey> {
        self.redeem_tx.clone()
    }

    /// Graceful shutdown: stop the watcher, flush the scan checkpoint, quiesce the
    /// redeemer, then run one final best-effort redeem sweep bounded by `deadline`
    /// so an above-threshold lane is not left un-redeemed across the stop. A pool
    /// is owner-closed only, so there is nothing to close here — only redeem.
    pub async fn shutdown(&self, deadline: Duration) {
        self.watcher.shutdown();
        self.flush_checkpoint_on_shutdown();
        self.quiesce_redeemer().await;
        if tokio::time::timeout(deadline, self.final_redeem_sweep())
            .await
            .is_err()
        {
            warn!(
                deadline_secs = deadline.as_secs(),
                "shutdown redeem deadline elapsed; some lanes left un-redeemed (redeem later)"
            );
        }
    }

    /// One last `redeemMany` over every above-threshold lane, so shutdown secures
    /// earnings the next boot would otherwise wait a hint/sweep to collect.
    async fn final_redeem_sweep(&self) {
        redeem_sweep(
            &self.contract,
            &self.store,
            &self.capabilities,
            &self.paid,
            self.self_address,
            self.redeem_threshold,
            &self.metrics,
        )
        .await;
    }

    /// Force the debounced `PoolRedeemed` scan checkpoint to durable storage
    /// (#784). Best-effort: a failed flush only widens the next boot's rescan.
    fn flush_checkpoint_on_shutdown(&self) {
        if let Err(err) = self
            .checkpoint_store
            .flush_checkpoint(CheckpointKey::PoolOpened)
        {
            let pending_block = self
                .checkpoint_store
                .load_checkpoint(CheckpointKey::PoolOpened)
                .ok()
                .flatten();
            warn!(
                %err,
                ?pending_block,
                "failed to flush settlement watcher scan checkpoint on shutdown"
            );
        }
    }

    /// Abort and await the redemption task so it issues no *further* `redeem` into
    /// the shutdown sweep. Idempotent: once the handle is taken, later calls — and
    /// the [`Drop`] safety net — find `None` and no-op.
    async fn quiesce_redeemer(&self) {
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
}

impl<P: Provider + Clone + 'static> std::fmt::Debug for PoolSettlementService<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolSettlementService")
            .field("address", self.contract.address())
            .finish_non_exhaustive()
    }
}

impl<P: Provider + Clone + 'static> Drop for PoolSettlementService<P> {
    fn drop(&mut self) {
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
/// practice; it bridges a raw `0`/`1` y-parity should the encoding ever change.
/// A non-65-byte signature (e.g. an ERC-1271 payload) is passed through
/// unchanged so the contract's own validation produces the authoritative error.
fn normalize_voucher_signature(sig: &[u8]) -> Vec<u8> {
    let mut out = sig.to_vec();
    if out.len() == 65
        && let Some(v) = out.last_mut()
        && *v < ETH_V_OFFSET
    {
        *v = v.saturating_add(ETH_V_OFFSET);
    }
    out
}

/// The single-`redeem` `capability` argument for a not-yet-registered signer:
/// `abi.encode(uint256 spendingCap, uint64 expiry, bytes ownerSig)`. Non-packed
/// `abi.encode` left-pads every integer to a 32-byte word, so encoding `expiry`
/// as a `U256` produces bytes identical to the contract's `uint64` decode. Empty
/// bytes are returned for an already-registered signer by the caller (this is
/// only reached with `Some` material).
fn encode_capability(material: &CapabilityMaterial) -> Bytes {
    Bytes::from(
        (
            material.spending_cap,
            U256::from(material.expiry),
            material.owner_sig.clone(),
        )
            .abi_encode_params(),
    )
}

/// The settlement watcher's cursor start: resume the durable
/// [`CheckpointKey::PoolOpened`] floor; a first-ever boot (cold store) anchors at
/// **head** ([`ColdStart::Head`]) — no `PoolRedeemed` toward this node can
/// predate the node itself, so there is no history to replay.
fn cursor_start(store: Arc<dyn KeyedCheckpointStore>) -> CursorStart {
    CursorStart::FromCheckpoint {
        checkpoint: Checkpoint {
            store,
            key: CheckpointKey::PoolOpened,
        },
        reorg_margin: REORG_MARGIN_BLOCKS,
        cold_start: ColdStart::Head,
    }
}

/// Applies `PaymentPool` settlement logs to the paid-watermark cache and drives
/// the close monitor. One per settlement watcher; the resumable `eth_getLogs`
/// poller feeds it block-ordered logs and advances + persists the scan checkpoint
/// per window on a clean tick.
///
/// Failure policy: paid-watermark updates are in-memory and always `Ok`. A
/// `PoolReclaimed` forget failure is logged and skipped (`Ok`) — the settled lane
/// owes nothing and the forget is idempotent. The close monitor is best-effort
/// and never fails the tick. An undecodable log is log-and-skip so a permanently
/// undecodable log never hot-loops the deterministic re-scan.
struct PoolSettlementSink<P: Provider + Clone> {
    contract: PaymentPool::PaymentPoolInstance<P>,
    self_address: Address,
    store: Arc<dyn PoolStateStore>,
    handler: Arc<ClientHandler>,
    paid: PaidWatermarks,
    capabilities: Arc<dyn CapabilitySource>,
    redeem_tx: mpsc::Sender<LaneKey>,
    metrics: Arc<Metrics>,
}

impl<P: Provider + Clone> LogSink for PoolSettlementSink<P> {
    #[allow(clippy::cognitive_complexity)]
    async fn apply(&mut self, log: Log) -> Result<()> {
        match log.topic0().copied() {
            Some(sig) if sig == PaymentPool::PoolRedeemed::SIGNATURE_HASH => {
                let event = match PaymentPool::PoolRedeemed::decode_log_data(&log.inner.data) {
                    Ok(event) => event,
                    Err(err) => {
                        warn!(%err, "skipping undecodable PoolRedeemed log");
                        return Ok(());
                    }
                };
                // Not enough to filter on the event signature — `provider` is the
                // third indexed topic but the OR-set filter cannot pin it, so
                // confirm it names this node before recording the watermark.
                if event.provider != self.self_address {
                    return Ok(());
                }
                let key = LaneKey {
                    pool_id: event.poolId,
                    signer: event.signer,
                    provider: event.provider,
                };
                self.paid.set(key, event.newPaidCumulative);
                debug!(
                    pool_id = %event.poolId,
                    signer = %event.signer,
                    paid_cumulative = %event.newPaidCumulative,
                    "recorded lane paid watermark from PoolRedeemed"
                );
            }
            Some(sig) if sig == PaymentPool::PoolToppedUp::SIGNATURE_HASH => {
                let event = match PaymentPool::PoolToppedUp::decode_log_data(&log.inner.data) {
                    Ok(event) => event,
                    Err(err) => {
                        warn!(%err, "skipping undecodable PoolToppedUp log");
                        return Ok(());
                    }
                };
                // A top-up may re-open lanes a dry pool left `owed > paid`.
                // Re-drive by hinting the redeemer for each of the pool's lanes
                // this node provides.
                self.redrive_pool_lanes(event.poolId);
            }
            Some(sig) if sig == PaymentPool::PoolCloseInitiated::SIGNATURE_HASH => {
                let event = match PaymentPool::PoolCloseInitiated::decode_log_data(&log.inner.data)
                {
                    Ok(event) => event,
                    Err(err) => {
                        warn!(%err, "skipping undecodable PoolCloseInitiated log");
                        return Ok(());
                    }
                };
                debug!(
                    pool_id = %event.poolId,
                    dispute_deadline = %event.disputeDeadline,
                    "PoolCloseInitiated observed; redeeming this node's lanes before the deadline"
                );
                // Best-effort and INLINE: redeem the node's highest voucher per
                // lane before the owner can reclaim. Never returns `Err` — a
                // benign revert (already fully redeemed, window closed) must not
                // fail the tick and re-scan the window.
                redeem_pool_on_close(
                    &self.contract,
                    &self.store,
                    &self.capabilities,
                    &self.paid,
                    self.self_address,
                    event.poolId,
                    saturating_u64(event.disputeDeadline),
                    &self.metrics,
                )
                .await;
            }
            Some(sig) if sig == PaymentPool::PoolReclaimed::SIGNATURE_HASH => {
                let event = match PaymentPool::PoolReclaimed::decode_log_data(&log.inner.data) {
                    Ok(event) => event,
                    Err(err) => {
                        warn!(%err, "skipping undecodable PoolReclaimed log");
                        return Ok(());
                    }
                };
                self.forget_pool_lanes(event.poolId).await;
            }
            // Unreachable today (the filter's topic0 OR-set bounds the inputs);
            // don't panic (anti-panic policy), log so a future OR-set drift leaves
            // a greppable trail instead of a silently dropped event.
            _ => {
                debug!(topic0 = ?log.topic0(), "unmatched PaymentPool event in subscribed OR-set");
            }
        }
        Ok(())
    }
}

impl<P: Provider + Clone> PoolSettlementSink<P> {
    /// Hint the redeemer for every lane of `pool_id` this node provides, so a
    /// top-up re-drives lanes a dry pool left `owed > paid`. Best-effort: a full
    /// hint channel drops the nudge (the self-tick sweep is the backstop).
    fn redrive_pool_lanes(&self, pool_id: PoolId) {
        let states = match self.store.load_all() {
            Ok(s) => s,
            Err(err) => {
                warn!(%err, %pool_id, "top-up re-drive: failed to load lane state");
                return;
            }
        };
        for st in states {
            if st.pool_id == pool_id && st.provider == self.self_address {
                let _ = self.redeem_tx.try_send(st.key());
            }
        }
    }

    /// Forget every lane of a reclaimed `pool_id` this node provides — the pool is
    /// `Closed`, so no further voucher can be redeemed against it.
    #[allow(clippy::cognitive_complexity)]
    async fn forget_pool_lanes(&self, pool_id: PoolId) {
        let states = match self.store.load_all() {
            Ok(s) => s,
            Err(err) => {
                warn!(%err, %pool_id, "reclaim cleanup: failed to load lane state");
                return;
            }
        };
        for st in states {
            if st.pool_id != pool_id || st.provider != self.self_address {
                continue;
            }
            let key = st.key();
            if let Err(err) = self.handler.forget_lane(key).await {
                self.metrics.watcher_persist_failure();
                warn!(%err, pool_id = %pool_id, signer = %key.signer, "failed to forget reclaimed lane");
            } else {
                self.paid.forget(&key);
                info!(pool_id = %pool_id, signer = %key.signer, "pool reclaimed; dropped tracked lane");
            }
        }
    }
}

/// Narrow an on-chain `uint256` deadline to `u64`, clamping an out-of-range value
/// to `u64::MAX` ("effectively never") — the safe direction for a redeem deadline.
fn saturating_u64(v: U256) -> u64 {
    u64::try_from(v).unwrap_or(u64::MAX)
}

/// Redeem this node's highest voucher per lane of a closing pool before its grace
/// deadline (ADR 003 § Owner reclaims before a node redeems). Best-effort: a
/// per-lane failure is logged and never propagated. Forces redemption regardless
/// of the threshold — a node that has not redeemed by the deadline forfeits its
/// outstanding vouchers.
#[allow(clippy::too_many_arguments)]
async fn redeem_pool_on_close<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    capabilities: &Arc<dyn CapabilitySource>,
    paid: &PaidWatermarks,
    self_address: Address,
    pool_id: PoolId,
    dispute_deadline: u64,
    metrics: &Arc<Metrics>,
) {
    if is_expired(unix_now(), dispute_deadline) {
        debug!(%pool_id, "grace window already closed; skipping close-redeem");
        return;
    }
    let states = match store.load_all() {
        Ok(s) => s,
        Err(err) => {
            warn!(%err, %pool_id, "close-redeem: failed to load lane state");
            return;
        }
    };
    for st in states {
        if st.pool_id != pool_id || st.provider != self_address {
            continue;
        }
        redeem_one(
            contract,
            store,
            capabilities,
            paid,
            self_address,
            U256::ZERO, // force: any unredeemed balance is worth redeeming before reclaim
            st.key(),
            metrics,
        )
        .await;
    }
}

/// Redemption task: `redeem` a lane's accrued claim once it crosses the
/// threshold, driven by two sources — advisory hints ([`LaneKey`]) from the
/// voucher-accept path and a low-frequency self-tick that sweeps every lane into
/// one `redeemMany` so a dropped hint can never strand an above-threshold claim.
/// Ends cleanly when every hint sender is dropped.
#[allow(clippy::too_many_arguments)]
async fn redeemer_loop<P: Provider + Clone>(
    contract: PaymentPool::PaymentPoolInstance<P>,
    store: Arc<dyn PoolStateStore>,
    capabilities: Arc<dyn CapabilitySource>,
    paid: PaidWatermarks,
    self_address: Address,
    redeem_threshold: U256,
    redeem_interval: Duration,
    mut redeem_rx: mpsc::Receiver<LaneKey>,
    metrics: Arc<Metrics>,
) {
    let mut ticker = tokio::time::interval(redeem_interval);
    // Skip the immediate first tick: nothing has accrued right after bootstrap,
    // and the first vouchers hint anyway.
    ticker.tick().await;
    loop {
        tokio::select! {
            hint = redeem_rx.recv() => match hint {
                Some(key) => {
                    redeem_one(
                        &contract, &store, &capabilities, &paid, self_address,
                        redeem_threshold, key, &metrics,
                    )
                    .await;
                }
                // All hint senders dropped — the service is going away.
                None => break,
            },
            _ = ticker.tick() => {
                redeem_sweep(
                    &contract, &store, &capabilities, &paid, self_address,
                    redeem_threshold, &metrics,
                )
                .await;
            }
        }
    }
    debug!("PaymentPool redeemer loop ended (all hint senders dropped)");
}

/// The outcome of evaluating one lane for redemption, without yet submitting.
enum RedeemPlan {
    /// Nothing to redeem: below threshold, not this node's, no signature, an
    /// unredeemed balance of zero, or an unregisterable signer.
    Skip,
    /// This lane's highest voucher should be redeemed. `register` is `Some` on the
    /// signer's first redemption (attach the capability) and `None` afterward.
    Redeem {
        voucher: Box<PaymentPool::RedeemVoucher>,
        register: Option<PaymentPool::CapabilityReg>,
    },
}

/// Read the lane's persisted highest voucher and its cached paid watermark; if
/// `owed − paid` meets `threshold` and the lane is still redeemable, return the
/// voucher (plus a capability registration on the signer's first redemption). A
/// `threshold` of `U256::ZERO` forces any non-zero unredeemed balance (the
/// close-monitor path). This does the per-lane chain read (`getAuthorization`) but
/// submits nothing itself, so it is the shared error-isolation boundary: the sweep
/// runs it per lane and drops only a failing leg from the batch.
async fn plan_redeem<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    capabilities: &Arc<dyn CapabilitySource>,
    paid: &PaidWatermarks,
    self_address: Address,
    threshold: U256,
    key: LaneKey,
) -> Result<RedeemPlan> {
    let Some(st) = store.get(key).context("load lane state for redemption")? else {
        // Lane not (yet) persisted — e.g. a hint raced the voucher-accept commit.
        return Ok(RedeemPlan::Skip);
    };
    // Defensive: only redeem lanes this node provides, with a signed voucher.
    if st.provider != self_address {
        return Ok(RedeemPlan::Skip);
    }
    let Some(sig_bytes) = st.last_signature() else {
        return Ok(RedeemPlan::Skip);
    };
    let owed = st.last_amount();
    if owed.is_zero() {
        return Ok(RedeemPlan::Skip);
    }
    let unredeemed = owed.saturating_sub(paid.get(&key));
    if unredeemed.is_zero() || unredeemed < threshold {
        return Ok(RedeemPlan::Skip);
    }

    // Register-once: a signer with `cap == 0` on-chain is not yet registered, so
    // the redemption must carry the owner-signed capability. Without the material
    // the signer cannot be registered and the lane is skipped until it arrives.
    let auth = contract
        .getAuthorization(key.pool_id, key.signer)
        .call()
        .await
        .context("getAuthorization for redemption")?;
    let register = if auth.cap.is_zero() {
        let Some(material) = capabilities.registration_material(&key) else {
            warn!(
                pool_id = %key.pool_id,
                signer = %key.signer,
                "signer not registered on-chain and no capability held; skipping redemption"
            );
            return Ok(RedeemPlan::Skip);
        };
        Some(PaymentPool::CapabilityReg {
            poolId: key.pool_id,
            signer: key.signer,
            spendingCap: material.spending_cap,
            expiry: material.expiry,
            ownerSig: material.owner_sig,
        })
    } else {
        None
    };

    let voucher = Box::new(PaymentPool::RedeemVoucher {
        poolId: key.pool_id,
        signer: key.signer,
        provider: key.provider,
        cumulative: owed,
        bytesDelivered: st.last_bytes_delivered(),
        voucherSig: Bytes::from(normalize_voucher_signature(sig_bytes)),
    });
    Ok(RedeemPlan::Redeem { voucher, register })
}

/// Hint-path (and close-path) redemption: plan one lane and, if it wants a
/// redeem, submit it as a single `redeem` tx. The per-lane path is one lane, so
/// batching buys nothing — the sweep is where `redeemMany` collapses N legs. Does
/// NOT seed the paid cache: the `PoolRedeemed` event this tx emits is the single
/// write path for the paid side.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::cognitive_complexity)]
async fn redeem_one<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    capabilities: &Arc<dyn CapabilitySource>,
    paid: &PaidWatermarks,
    self_address: Address,
    threshold: U256,
    key: LaneKey,
    metrics: &Arc<Metrics>,
) {
    let plan = match plan_redeem(
        contract,
        store,
        capabilities,
        paid,
        self_address,
        threshold,
        key,
    )
    .await
    {
        Ok(plan) => plan,
        Err(err) => {
            metrics.redemption_failure();
            warn!(err = %sanitize_rpc_display(&err), pool_id = %key.pool_id, "redemption planning failed");
            return;
        }
    };
    let RedeemPlan::Redeem { voucher, register } = plan else {
        return;
    };
    let capability = match &register {
        Some(reg) => encode_capability(&CapabilityMaterial {
            spending_cap: reg.spendingCap,
            expiry: reg.expiry,
            owner_sig: reg.ownerSig.clone(),
        }),
        None => Bytes::new(),
    };
    let sent = contract
        .redeem(
            voucher.poolId,
            voucher.signer,
            voucher.provider,
            voucher.cumulative,
            voucher.bytesDelivered,
            voucher.voucherSig.clone(),
            capability,
        )
        .send()
        .await;
    match send_and_await_receipt(sent, Some(REDEEM_RECEIPT_TIMEOUT)).await {
        TxOutcome::Landed(receipt) => {
            info!(
                pool_id = %key.pool_id,
                signer = %key.signer,
                tx = %receipt.transaction_hash,
                cumulative = %voucher.cumulative,
                registered = register.is_some(),
                "redeemed lane voucher"
            );
        }
        TxOutcome::Reverted(receipt) => {
            metrics.redemption_failure();
            warn!(
                pool_id = %key.pool_id,
                tx = %receipt.transaction_hash,
                "redeem reverted on-chain; leaving claim for retry"
            );
        }
        TxOutcome::SendErr(err) => {
            metrics.redemption_failure();
            warn!(err = %sanitize_rpc_display(&err), pool_id = %key.pool_id, "redeem send failed");
        }
        TxOutcome::ReceiptErr(err) => {
            metrics.redemption_failure();
            warn!(err = %sanitize_rpc_display(&err), pool_id = %key.pool_id, "redeem receipt failed");
        }
        TxOutcome::Timeout => {
            metrics.redemption_failure();
            warn!(
                pool_id = %key.pool_id,
                timeout = ?REDEEM_RECEIPT_TIMEOUT,
                "redeem receipt wait elapsed; leaving claim for retry"
            );
        }
    }
}

/// Self-tick sweep: scan every persisted lane, partition into the not-yet-
/// registered signers (one [`PaymentPool::CapabilityReg`] each) and the vouchers
/// above the threshold (one [`PaymentPool::RedeemVoucher`] each), and submit ONE
/// `redeemMany` for the whole tick (ADR 003 § Batch redemption). Registration is
/// decoupled from the voucher: a skipped voucher never loses a registration, and
/// the contract skips a transient-empty voucher rather than reverting the batch.
///
/// Per-lane error isolation runs through the **planning** phase: each lane's
/// load / `getAuthorization` / threshold check runs independently and a failure
/// drops only that leg. A store-load failure logs and skips this tick.
async fn redeem_sweep<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    capabilities: &Arc<dyn CapabilitySource>,
    paid: &PaidWatermarks,
    self_address: Address,
    redeem_threshold: U256,
    metrics: &Arc<Metrics>,
) {
    let states = match store.load_all() {
        Ok(s) => s,
        Err(err) => {
            warn!(%err, "redeemer self-tick: failed to load lane state");
            return;
        }
    };
    let mut caps: Vec<PaymentPool::CapabilityReg> = Vec::new();
    let mut seen_signers: HashSet<(PoolId, Address)> = HashSet::new();
    let mut vouchers: Vec<PaymentPool::RedeemVoucher> = Vec::new();
    for st in states {
        let key = st.key();
        match plan_redeem(
            contract,
            store,
            capabilities,
            paid,
            self_address,
            redeem_threshold,
            key,
        )
        .await
        {
            Ok(RedeemPlan::Skip) => {}
            Ok(RedeemPlan::Redeem { voucher, register }) => {
                if let Some(reg) = register
                    && seen_signers.insert((reg.poolId, reg.signer))
                {
                    caps.push(reg);
                }
                vouchers.push(*voucher);
            }
            Err(err) => {
                metrics.redemption_failure();
                warn!(err = %sanitize_rpc_display(&err), pool_id = %key.pool_id, "redemption planning failed");
            }
        }
    }
    if vouchers.is_empty() {
        return;
    }
    submit_redeem_many(contract, caps, vouchers, metrics).await;
}

/// Submit one `redeemMany` for the tick's collected registrations + vouchers.
/// Does NOT seed the paid cache: each paid voucher emits its own `PoolRedeemed`,
/// the single write path for the paid side. A batch that reverts (a structurally
/// invalid entry — bad signature, wrong provider, bad owner-signature) or fails to
/// send records one `redemption_failure`; the next tick re-prepares and retries.
#[allow(clippy::cognitive_complexity)]
async fn submit_redeem_many<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    caps: Vec<PaymentPool::CapabilityReg>,
    vouchers: Vec<PaymentPool::RedeemVoucher>,
    metrics: &Arc<Metrics>,
) {
    let cap_count = caps.len();
    let voucher_count = vouchers.len();
    let sent = contract.redeemMany(caps, vouchers).send().await;
    match send_and_await_receipt(sent, Some(REDEEM_RECEIPT_TIMEOUT)).await {
        TxOutcome::Landed(receipt) => {
            info!(
                cap_count,
                voucher_count,
                tx = %receipt.transaction_hash,
                "batched lane redemption landed (redeemMany)"
            );
        }
        TxOutcome::Reverted(receipt) => {
            metrics.redemption_failure();
            warn!(
                voucher_count,
                tx = %receipt.transaction_hash,
                "redeemMany reverted on-chain; leaving claims for retry"
            );
        }
        TxOutcome::SendErr(err) => {
            metrics.redemption_failure();
            warn!(err = %sanitize_rpc_display(&err), voucher_count, "redeemMany send failed; leaving claims for retry");
        }
        TxOutcome::ReceiptErr(err) => {
            metrics.redemption_failure();
            warn!(err = %sanitize_rpc_display(&err), voucher_count, "redeemMany receipt failed; leaving claims for retry");
        }
        TxOutcome::Timeout => {
            metrics.redemption_failure();
            warn!(
                voucher_count,
                timeout = ?REDEEM_RECEIPT_TIMEOUT,
                "redeemMany receipt wait elapsed; leaving claims for retry"
            );
        }
    }
}

/// Mutable debounce bookkeeping for [`DebouncedCheckpointStore`], guarded by a
/// single `std::sync::Mutex`. The lock is only ever held for the brief duration
/// of a `record`/`flush` decision (no `.await` inside), so a sync mutex is the
/// right primitive.
struct DebounceState {
    /// The block last forwarded to a *durable* write on the inner store, or
    /// `None` if nothing has been persisted in this process yet.
    last_persisted: Option<u64>,
    /// Monotonic [`Instant`] of the last durable write, for the time-based
    /// threshold. Seeded at construction so the first interval is measured from
    /// service start.
    last_persist_at: Instant,
    /// The highest block buffered but not yet durably written. Monotonic. `None`
    /// once flushed/forwarded — i.e. equal to `last_persisted`.
    pending: Option<u64>,
    /// Whether the first in-process `record` has run. Until it does we lazily fold
    /// the inner store's existing checkpoint into `last_persisted`, and force that
    /// first record durable so a restart re-anchors the floor promptly.
    seeded: bool,
}

/// Debouncing decorator over a [`KeyedCheckpointStore`] (#784, keyed in #1092).
/// Buffers `record_checkpoint` in memory **per [`CheckpointKey`]** and forwards a
/// durable write for a key only when its buffered block is at least `flush_blocks`
/// ahead of that key's last persisted value *or* at least `flush_interval` has
/// elapsed since that key's last durable write — cutting the per-block fsync
/// amplification a watcher's live tail would otherwise pay on a backlog drain.
///
/// **Durability contract preserved (per key).** The persisted value is only ever
/// a *floor* for the resume backfill (the resumable watcher rewinds it by
/// `REORG_MARGIN_BLOCKS` and every sink is idempotent), so a
/// buffered-but-not-yet-fsynced advance that a crash loses merely widens the next
/// rescan — never narrows it. `record_checkpoint` only raises a key's `pending`,
/// so the forwarded value stays monotonic. [`Self::flush_checkpoint`] forces the
/// buffered value out and is wired into graceful shutdown.
pub struct DebouncedCheckpointStore {
    inner: Arc<dyn KeyedCheckpointStore>,
    flush_blocks: u64,
    flush_interval: Duration,
    /// Clock source, injectable so the time-based threshold is unit-testable
    /// without sleeping. Production uses [`Instant::now`].
    clock: Box<dyn Fn() -> Instant + Send + Sync>,
    /// Per-key debounce bookkeeping, created lazily on a key's first record/flush.
    state: std::sync::Mutex<HashMap<CheckpointKey, DebounceState>>,
}

impl std::fmt::Debug for DebouncedCheckpointStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tracked: Vec<(CheckpointKey, Option<u64>, Option<u64>)> = {
            let map = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            map.iter()
                .map(|(k, s)| (*k, s.pending, s.last_persisted))
                .collect()
        };
        f.debug_struct("DebouncedCheckpointStore")
            .field("flush_blocks", &self.flush_blocks)
            .field("flush_interval", &self.flush_interval)
            .field("tracked", &tracked)
            .finish_non_exhaustive()
    }
}

impl DebouncedCheckpointStore {
    /// Wrap `inner` with the production debounce thresholds
    /// (`CHECKPOINT_FLUSH_BLOCKS` / `CHECKPOINT_FLUSH_INTERVAL`) and the real wall
    /// clock.
    #[must_use]
    pub fn new(inner: Arc<dyn KeyedCheckpointStore>) -> Self {
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
        inner: Arc<dyn KeyedCheckpointStore>,
        flush_blocks: u64,
        flush_interval: Duration,
        clock: Box<dyn Fn() -> Instant + Send + Sync>,
    ) -> Self {
        Self {
            inner,
            flush_blocks,
            flush_interval,
            clock,
            state: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Forward `block` to the inner store for `key` and reset that key's debounce
    /// window. Caller holds `state`. On a durable-write error the buffered
    /// `pending` is left intact so the next `record`/`flush` retries.
    fn persist_locked(
        &self,
        key: CheckpointKey,
        state: &mut DebounceState,
        block: u64,
    ) -> Result<(), StoreError> {
        self.inner.record_checkpoint(key, block)?;
        state.last_persisted = Some(block);
        state.last_persist_at = (self.clock)();
        state.pending = None;
        Ok(())
    }
}

impl KeyedCheckpointStore for DebouncedCheckpointStore {
    fn load_checkpoint(&self, key: CheckpointKey) -> Result<Option<u64>, StoreError> {
        let persisted = self.inner.load_checkpoint(key)?;
        let pending = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .and_then(|s| s.pending);
        Ok(match (persisted, pending) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        })
    }

    fn record_checkpoint(&self, key: CheckpointKey, block: u64) -> Result<(), StoreError> {
        let mut map = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let clock = &self.clock;
        let state = map.entry(key).or_insert_with(|| DebounceState {
            last_persisted: None,
            last_persist_at: clock(),
            pending: None,
            seeded: false,
        });
        // On the first in-process record for this key, fold the inner store's
        // existing checkpoint into `last_persisted` so a fresh key-state around an
        // already-populated inner store cannot regress the on-disk floor. `seeded`
        // flips only after the load *succeeds*.
        let first_record = !state.seeded;
        if first_record {
            state.last_persisted = self.inner.load_checkpoint(key)?;
            state.seeded = true;
        }
        let highest = state
            .pending
            .max(state.last_persisted)
            .unwrap_or(0)
            .max(block);
        state.pending = Some(highest);

        if state.last_persisted == Some(highest) {
            return Ok(());
        }

        let blocks_ahead = highest.saturating_sub(state.last_persisted.unwrap_or(0));
        let elapsed = (self.clock)().saturating_duration_since(state.last_persist_at);
        let due =
            first_record || blocks_ahead >= self.flush_blocks || elapsed >= self.flush_interval;
        if due {
            self.persist_locked(key, state, highest)
        } else {
            Ok(())
        }
    }

    fn flush_checkpoint(&self, key: CheckpointKey) -> Result<(), StoreError> {
        let mut map = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(state) = map.get_mut(&key) else {
            return Ok(());
        };
        match state.pending {
            Some(block) if state.last_persisted != Some(block) => {
                self.persist_locked(key, state, block)
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 65-byte signature with the recovery byte set to `v` (a `[u8; 65]` so a
    /// const index stays provably in-bounds for the anti-panic lints).
    fn sig_with_v(v: u8) -> [u8; 65] {
        let mut s = [7u8; 65];
        if let Some(last) = s.last_mut() {
            *last = v;
        }
        s
    }

    #[test]
    fn normalize_lifts_raw_y_parity_to_the_eth_convention() {
        assert_eq!(
            normalize_voucher_signature(&sig_with_v(0)).last(),
            Some(&27)
        );
        assert_eq!(
            normalize_voucher_signature(&sig_with_v(1)).last(),
            Some(&28)
        );
    }

    #[test]
    fn normalize_leaves_a_well_formed_v_untouched() {
        assert_eq!(
            normalize_voucher_signature(&sig_with_v(27)).last(),
            Some(&27)
        );
        assert_eq!(
            normalize_voucher_signature(&sig_with_v(28)).last(),
            Some(&28)
        );
    }

    #[test]
    fn normalize_passes_through_a_non_65_byte_payload() {
        // Not 65 bytes (e.g. an ERC-1271 payload): pass through unchanged so the
        // contract is the authority on validity — do NOT touch the last byte.
        let out = normalize_voucher_signature(&[1u8, 2, 3]);
        assert_eq!(out, vec![1u8, 2, 3]);
    }

    #[test]
    fn encode_capability_matches_abi_encode_uint256_uint64_bytes() {
        let material = CapabilityMaterial {
            spending_cap: U256::from(1_000u64),
            expiry: 42,
            owner_sig: Bytes::from(vec![0xAA, 0xBB]),
        };
        let bytes = encode_capability(&material);
        // Three head words (cap, expiry, offset-to-bytes) + length word + one
        // right-padded data word for the 2-byte signature = 5 * 32 bytes.
        assert_eq!(
            bytes.len(),
            5 * 32,
            "abi.encode(uint256,uint64,bytes) layout"
        );
        // The `uint64` expiry occupies a full left-padded word: byte 63 is 42.
        assert_eq!(bytes.get(63), Some(&42u8));
        // The dynamic `bytes` length word (word 3) ends in 2.
        assert_eq!(bytes.get(4 * 32 - 1), Some(&2u8));
    }

    /// The debounce decorator forwards through to the inner store on the first
    /// record, coalesces sub-threshold advances, and forces a buffered block out
    /// on flush. Driven by an in-test [`KeyedCheckpointStore`] double, since the
    /// incentive crate ships no memory checkpoint store.
    #[test]
    fn debounced_checkpoint_coalesces_then_flushes() {
        #[derive(Default)]
        struct MemCk(std::sync::Mutex<HashMap<CheckpointKey, u64>>);
        impl KeyedCheckpointStore for MemCk {
            fn load_checkpoint(&self, key: CheckpointKey) -> Result<Option<u64>, StoreError> {
                Ok(self
                    .0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&key)
                    .copied())
            }
            fn record_checkpoint(&self, key: CheckpointKey, block: u64) -> Result<(), StoreError> {
                self.0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(key, block);
                Ok(())
            }
        }
        let inner: Arc<dyn KeyedCheckpointStore> = Arc::new(MemCk::default());
        let deb = DebouncedCheckpointStore::with_params(
            Arc::clone(&inner),
            10,
            Duration::from_hours(1),
            Box::new(Instant::now),
        );
        let key = CheckpointKey::PoolOpened;
        let load = |s: &Arc<dyn KeyedCheckpointStore>| s.load_checkpoint(key).ok().flatten();
        // First advancing record forwards durably (re-anchor the floor promptly).
        let _ = deb.record_checkpoint(key, 5);
        assert_eq!(load(&inner), Some(5));
        // A sub-`flush_blocks` advance buffers only; the durable floor holds.
        let _ = deb.record_checkpoint(key, 8);
        assert_eq!(load(&inner), Some(5));
        assert_eq!(deb.load_checkpoint(key).ok().flatten(), Some(8));
        // A flush forces the buffered block out durably (the shutdown path).
        let _ = deb.flush_checkpoint(key);
        assert_eq!(load(&inner), Some(8));
    }
}
