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
//!   missed range — so paid is rebuilt from the event log, never guessed. The same
//!   scan also folds the serve path's pool solvency/funder projection
//!   ([`crate::pool_view::PoolProjection`]): `PoolOpened`/`PoolToppedUp` set a
//!   pool's `{owner, deposit}`, `PoolRedeemed` for every provider draws down its
//!   pool-wide `totalRedeemed`, and `PoolReclaimed` drops it — so a serve request
//!   reads `{owner, remaining}` in-memory instead of through a `getPool` `eth_call`.
//! - **Redemption (per-chunk floor + on-shutdown).** On a redeem hint (a
//!   [`LaneKey`]) emitted by the voucher-accept path, the node reads the lane's
//!   owed voucher and its cached paid watermark and plans the lane for
//!   redemption. A lane whose persisted `registered_until` is still live skips
//!   the chain read entirely; every other candidate lane in the batch is
//!   resolved with ONE `getAuthorizations` call rather than one
//!   `getAuthorization` per lane, and an observed on-chain registration is
//!   persisted back so later sweeps skip its read too. A low-frequency
//!   self-tick sweeps every persisted lane, packs the planned lanes into
//!   chunks, and submits a chunk — one `redeemMany` — only once the aggregate
//!   unredeemed value across that chunk's lanes clears a configurable floor,
//!   so a dropped hint never strands a lane whose chunk has cleared the floor.
//!   The node flushes the lane store durable after planning a chunk and before
//!   submitting it, so post-crash on-disk `owed ≥ submitted`.
//! - **Close monitor.** A pool is owner-closed only. On a `PoolCloseInitiated`
//!   for a pool this node holds lanes against, the monitor redeems its highest
//!   voucher per lane before `disputeDeadline` (ADR 003 § Owner reclaims before a
//!   node redeems) — a node that has not redeemed by the deadline forfeits its
//!   outstanding vouchers.
//!
//! Buyer-side `openPool`/`topUp`/`reclaim` (node→node cache-miss pulls) is out of
//! scope here. The paid-watermark watcher is a [`Route`] the runtime registers on
//! the shared multiplexed poller (which owns the loop, cursor, and shutdown); the
//! service itself owns only the redeemer [`JoinHandle`], aborted on shutdown or
//! drop.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use alloy::primitives::{Address, B256, Bytes, Signature, U256};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use decdn_common::redact::sanitize_rpc_display;
use decdn_incentive::payment_pool::{PaymentPool, to_pool_u64};
use decdn_incentive::sig_canon::is_high_s;
use decdn_incentive::{
    CheckpointKey, KeyedCheckpointStore, LaneKey, LaneState, PoolId, PoolStateStore, StoreError,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::chain_events::REORG_MARGIN_BLOCKS;
use crate::chain_events::multiplexed_poller::{Route, SinkSource};
use crate::chain_events::resumable_watcher::{Checkpoint, ColdStart, CursorStart, LogSink};
use crate::handlers::client::ClientHandler;
use crate::metrics::{Metrics, metric_hook};
use crate::onchain_tx::{TxOutcome, send_and_await_receipt};
use crate::pool_view::PoolProjection;

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

/// Bound on the reactive halve-and-retry when a chunk fails to send oversized.
/// A default-sized chunk (300) sits far under the block-gas ceiling, so a real
/// oversize needs at most one or two halvings; this caps the worst-case fan-out
/// (a transient error mis-flagged as oversize) at 2^6 doomed sub-sends.
const MAX_SPLIT_DEPTH: u32 = 6;

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
/// can build the on-chain registration payload only when a signer's batched
/// `getAuthorizations` read comes back with `cap == 0` (a lane whose persisted
/// `registered_until` is still live skips this read, and hence never needs
/// this material).
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
    redeem_max_vouchers_per_tx: usize,
    metrics: Arc<Metrics>,
    /// The redemption task handle. Held so shutdown can abort+await it before a
    /// final redeem sweep. `take()`n by [`Self::quiesce_redeemer`]; the [`Drop`]
    /// impl aborts whatever remains. A `std::sync::Mutex` (not `tokio`): the guard
    /// is only ever held to `take()` the handle, never across an `.await`.
    redeemer: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl<P: Provider + Clone + 'static> PoolSettlementService<P> {
    /// Bootstrap the service: self-check the contract, spawn the redemption
    /// task, and return the service alongside the paid-watermark [`Route`] the
    /// runtime registers on the shared multiplexed poller.
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
        redeem_max_vouchers_per_tx: usize,
        redeem_interval: Duration,
        metrics: Arc<Metrics>,
        pool_view: PoolProjection,
        redeem_tx: mpsc::Sender<LaneKey>,
        redeem_rx: mpsc::Receiver<LaneKey>,
    ) -> Result<(Self, Route)> {
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
            redeem_max_vouchers_per_tx,
            metrics: Arc::clone(&metrics),
            pool_view,
        };
        let route = Route {
            addresses: vec![payment_pool_addr],
            topic0s: settlement_route_topic0s(),
            // The durable `PoolOpened` checkpoint resumes across restarts; a
            // first-ever boot (cold store) anchors at head. The poller flushes
            // this checkpoint on shutdown (via `CursorStart::flush`), the same
            // flush the service used to perform itself.
            start: cursor_start(Arc::clone(&checkpoint_store)),
            // This sink observes no shutdown token, so it registers a `Ready`
            // sink; its own follow-up reads/writes ride the wallet contract it
            // holds, independent of the poller's read-only get_logs provider.
            sink: SinkSource::Ready(Box::new(sink)),
            label: "settlement",
            on_established: Some(metric_hook(
                &metrics,
                Metrics::settlement_watcher_cycle_established,
            )),
            on_backoff: Some(metric_hook(
                &metrics,
                Metrics::settlement_watcher_backoff_started,
            )),
            on_tick_success: Some(metric_hook(&metrics, Metrics::settlement_watcher_tick)),
            on_task_panic: Some(metric_hook(
                &metrics,
                Metrics::settlement_watcher_task_panicked,
            )),
        };

        let redeemer = tokio::spawn(redeemer_loop(
            contract.clone(),
            Arc::clone(&store),
            Arc::clone(&capabilities),
            paid.clone(),
            self_address,
            redeem_threshold,
            redeem_max_vouchers_per_tx,
            redeem_interval,
            redeem_rx,
            Arc::clone(&metrics),
        ));

        Ok((
            Self {
                contract,
                redeem_tx,
                store,
                capabilities,
                paid,
                self_address,
                redeem_threshold,
                redeem_max_vouchers_per_tx,
                metrics,
                redeemer: std::sync::Mutex::new(Some(redeemer)),
            },
            route,
        ))
    }

    /// Sender the voucher-accept path uses to hint that a lane's accrued claim
    /// may be ready to plan into a chunk whose aggregate clears the redemption
    /// floor. Cloneable; dropping all senders simply ends the redemption task
    /// cleanly.
    #[must_use]
    pub fn redeem_hint_sender(&self) -> mpsc::Sender<LaneKey> {
        self.redeem_tx.clone()
    }

    /// Graceful shutdown: quiesce the redeemer, then run one final best-effort
    /// redeem sweep bounded by `deadline` so a lane whose chunk has cleared the
    /// floor is not left un-redeemed across the stop. A pool is owner-closed only,
    /// so there is nothing to close here — only redeem.
    ///
    /// The paid-watermark watcher is now the shared multiplexed poller's route,
    /// not a service-owned task: the runtime cancels the poller (which triggers an
    /// async, best-effort flush of this route's `PoolOpened` scan checkpoint via
    /// `CursorStart::flush`) before calling this, but only cancels the token — it
    /// does not await the poller's exit — so that flush is not guaranteed to
    /// complete before the final sweep below runs. This is fine: the scan
    /// checkpoint is independent of the lane-state redeem sweep and idempotent to
    /// re-scan, so the two can race without a correctness impact.
    pub async fn shutdown(&self, deadline: Duration) {
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

    /// One last pass of `redeemMany` chunks over every lane whose chunk clears the
    /// floor, so shutdown secures earnings the next boot would otherwise wait a
    /// hint/sweep to collect.
    async fn final_redeem_sweep(&self) {
        // Shutdown redeems regardless of a flush failure (`strict_flush` false):
        // forfeiting the claim across the stop is worse than a bounded re-serve
        // risk, matching the close path.
        redeem_sweep(
            &self.contract,
            &self.store,
            &self.capabilities,
            &self.paid,
            self.self_address,
            self.redeem_threshold,
            self.redeem_max_vouchers_per_tx,
            false,
            &self.metrics,
        )
        .await;
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

/// Fold a stored voucher signature (`r‖s‖v`, 65 bytes) into the EIP-2098
/// compact pair `(r, vs)` the contract's `LaneVoucher` carries: `vs` is `s`
/// with the recovery bit in its top bit. Halving the signature is what makes
/// the voucher a static ABI struct, and so what makes a lane 160 calldata
/// bytes instead of 288.
///
/// # Errors
///
/// Errors on a signature that is not 65 bytes, on a malformed `r`/`s`/`v`, or
/// on a non-canonical high-`s` signature.
///
/// The canonicality check is [`is_high_s`], not "is the top bit of `s` free".
/// Those are not the same test: `secp256k1n / 2` sits below `2^255`, so about
/// `2^128` values have a clear top bit and are still high-`s`. Compaction would
/// happily fold the recovery bit into such a signature and the contract's
/// `ECDSA.tryRecover` would then reject it — reverting the whole `redeemMany`,
/// so one hostile lane would sink every honest lane batched with it. The node's
/// voucher-accept path already refuses high-`s` (#836), so this is the second
/// gate on a value that should never have been stored; a lane it rejects is
/// dropped from the batch rather than allowed to sink it.
fn compact_voucher_signature(sig: &[u8]) -> Result<(B256, B256)> {
    let raw: [u8; 65] = sig
        .try_into()
        .map_err(|_| anyhow::anyhow!("voucher signature is not 65 bytes (r‖s‖v)"))?;
    let parsed = Signature::from_raw(&raw).context("voucher signature is malformed")?;
    if is_high_s(&parsed) {
        anyhow::bail!("voucher signature is non-canonical (high `s`) and would revert on-chain");
    }

    let r = B256::from(parsed.r().to_be_bytes::<32>());
    let mut vs = parsed.s().to_be_bytes::<32>();
    // Canonical `s` is at most `n / 2`, which is below `2^255`, so the top bit
    // is free for the recovery bit. The `is_high_s` gate above is what
    // guarantees that.
    let Some(top) = vs.first_mut() else {
        anyhow::bail!("voucher signature `s` is empty");
    };
    *top |= u8::from(parsed.v()) << 7;

    Ok((r, B256::from(vs)))
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

/// The settlement route's demux key: every `PaymentPool` lifecycle event the
/// paid-watermark cache and close monitor need. Split out from
/// [`PoolSettlementService::bootstrap`] so the exact topic0 set is
/// unit-testable without a provider — dropping `PoolOpened` here, for example,
/// would silently blind the close monitor to newly opened pools.
fn settlement_route_topic0s() -> Vec<B256> {
    vec![
        PaymentPool::PoolOpened::SIGNATURE_HASH,
        PaymentPool::PoolRedeemed::SIGNATURE_HASH,
        PaymentPool::PoolToppedUp::SIGNATURE_HASH,
        PaymentPool::PoolCloseInitiated::SIGNATURE_HASH,
        PaymentPool::PoolReclaimed::SIGNATURE_HASH,
    ]
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
    redeem_max_vouchers_per_tx: usize,
    metrics: Arc<Metrics>,
    /// The serve path's pool solvency/funder view. This sink is its single writer:
    /// it folds `PoolOpened` (owner + deposit), `PoolToppedUp` (new deposit),
    /// `PoolRedeemed` for EVERY provider (pool-wide `totalRedeemed`), and
    /// `PoolReclaimed` (drop) into the projection so a serve request reads
    /// `{owner, remaining}` in-memory rather than through a `getPool` `eth_call`.
    pool_view: PoolProjection,
}

impl<P: Provider + Clone> LogSink for PoolSettlementSink<P> {
    #[allow(clippy::cognitive_complexity)]
    async fn apply(&mut self, log: Log) -> Result<()> {
        match log.topic0().copied() {
            Some(sig) if sig == PaymentPool::PoolOpened::SIGNATURE_HASH => {
                // Projection-only: `PoolOpened` seeds the serve path's owner +
                // deposit. It carries no settlement effect — this node holds no
                // lane against a freshly-opened pool until a voucher arrives.
                let event = match PaymentPool::PoolOpened::decode_log_data(&log.inner.data) {
                    Ok(event) => event,
                    Err(err) => {
                        warn!(%err, "skipping undecodable PoolOpened log");
                        return Ok(());
                    }
                };
                self.pool_view
                    .record_opened(event.poolId, event.owner, event.deposit);
                debug!(pool_id = %event.poolId, owner = %event.owner, "projected PoolOpened");
            }
            Some(sig) if sig == PaymentPool::PoolRedeemed::SIGNATURE_HASH => {
                let event = match PaymentPool::PoolRedeemed::decode_log_data(&log.inner.data) {
                    Ok(event) => event,
                    Err(err) => {
                        warn!(%err, "skipping undecodable PoolRedeemed log");
                        return Ok(());
                    }
                };
                // Pool-wide `totalRedeemed` folds EVERY provider's lanes, so the
                // solvency projection records this event before the own-provider
                // filter below — the serve path's `remaining` must reflect other
                // nodes' redemptions against the same pool, not just this node's.
                self.pool_view
                    .record_redeemed(event.poolId, event.provider, &event.lanes);
                // Not enough to filter on the event signature — `provider` is
                // an indexed topic but the OR-set filter cannot pin it, so
                // confirm it names this node before recording the paid watermark.
                if event.provider != self.self_address {
                    return Ok(());
                }
                // One event carries every lane the batch paid on this pool, so
                // the watermark write is per entry, not per log.
                for lane in &event.lanes {
                    let key = LaneKey {
                        pool_id: event.poolId,
                        signer: lane.signer,
                        provider: event.provider,
                    };
                    self.paid.set(key, U256::from(lane.newPaidCumulative));
                    debug!(
                        pool_id = %event.poolId,
                        signer = %lane.signer,
                        paid_cumulative = lane.newPaidCumulative,
                        "recorded lane paid watermark from PoolRedeemed"
                    );
                }
            }
            Some(sig) if sig == PaymentPool::PoolToppedUp::SIGNATURE_HASH => {
                let event = match PaymentPool::PoolToppedUp::decode_log_data(&log.inner.data) {
                    Ok(event) => event,
                    Err(err) => {
                        warn!(%err, "skipping undecodable PoolToppedUp log");
                        return Ok(());
                    }
                };
                // Raise the serve path's projected deposit so the solvency gate
                // reserves against the topped-up balance, not the stale one.
                self.pool_view.record_topup(event.poolId, event.newDeposit);
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
                    self.redeem_max_vouchers_per_tx,
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
                // The pool is `Closed` and refunded: drop it from the serve
                // projection so a later read fails open rather than reserving
                // against a stale remaining.
                self.pool_view.forget(event.poolId);
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
        self.handler.forget_pool_floor(pool_id).await;
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
/// of the floor — a node that has not redeemed by the deadline forfeits its
/// outstanding vouchers.
#[allow(clippy::too_many_arguments, clippy::cognitive_complexity)]
async fn redeem_pool_on_close<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    capabilities: &Arc<dyn CapabilitySource>,
    paid: &PaidWatermarks,
    self_address: Address,
    pool_id: PoolId,
    dispute_deadline: u64,
    max_vouchers: usize,
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
    let pool_states: Vec<LaneState> = states
        .into_iter()
        .filter(|st| st.pool_id == pool_id && st.provider == self_address)
        .collect();
    let plans = plan_lanes(
        contract,
        store,
        capabilities,
        paid,
        self_address,
        pool_states,
        metrics,
    )
    .await;
    // Force: any non-zero unredeemed balance is worth redeeming before reclaim.
    // The close path redeems regardless of a flush failure — forfeiting the claim
    // at the deadline is worse than a bounded re-serve risk (`strict_flush` false).
    redeem_planned_lanes(
        contract,
        store,
        plans,
        U256::ZERO,
        max_vouchers,
        false,
        metrics,
    )
    .await;
}

/// Redemption task: chunk lanes' accrued claims and `redeemMany` a chunk once
/// its aggregate unredeemed value clears the floor, driven by two sources —
/// advisory hints ([`LaneKey`]) from the voucher-accept path and a
/// low-frequency self-tick that sweeps every lane into chunked `redeemMany`
/// transactions so a dropped hint can never strand a lane whose chunk has
/// cleared the floor. Ends cleanly when every hint sender is dropped.
#[allow(clippy::too_many_arguments)]
async fn redeemer_loop<P: Provider + Clone>(
    contract: PaymentPool::PaymentPoolInstance<P>,
    store: Arc<dyn PoolStateStore>,
    capabilities: Arc<dyn CapabilitySource>,
    paid: PaidWatermarks,
    self_address: Address,
    redeem_threshold: U256,
    max_vouchers: usize,
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
                        redeem_threshold, max_vouchers, key, &metrics,
                    )
                    .await;
                }
                // All hint senders dropped — the service is going away.
                None => break,
            },
            _ = ticker.tick() => {
                redeem_sweep(
                    &contract, &store, &capabilities, &paid, self_address,
                    redeem_threshold, max_vouchers, true, &metrics,
                )
                .await;
            }
        }
    }
    debug!("PaymentPool redeemer loop ended (all hint senders dropped)");
}

/// One lane planned for redemption: its pool, its persistence key (so a landed
/// registration can be written back to `registered_until`), its unredeemed
/// value (for the per-chunk floor), the highest voucher to submit, and — on
/// the signer's first redemption — the owner-signed capability to register.
struct PlannedLane {
    pool_id: PoolId,
    key: LaneKey,
    unredeemed: U256,
    voucher: PaymentPool::LaneVoucher,
    register: Option<PaymentPool::CapabilityReg>,
}

/// Group planned lanes into one `PoolBatch` per distinct pool, in first-seen
/// order, so the contract amortizes each pool's status read and `totalRedeemed`
/// write across its lanes. A lane's capability registration (present only on a
/// signer's first redemption) rides in its pool's batch; each `(pool, signer)`
/// lane is distinct, so no capability is ever duplicated within a batch.
fn group_by_pool(lanes: &[PlannedLane]) -> Vec<PaymentPool::PoolBatch> {
    let mut order: Vec<PoolId> = Vec::new();
    let mut by_pool: HashMap<PoolId, PaymentPool::PoolBatch> = HashMap::new();
    for lane in lanes {
        let batch = by_pool.entry(lane.pool_id).or_insert_with(|| {
            order.push(lane.pool_id);
            PaymentPool::PoolBatch {
                poolId: lane.pool_id,
                capabilities: Vec::new(),
                vouchers: Vec::new(),
            }
        });
        if let Some(reg) = &lane.register {
            batch.capabilities.push(reg.clone());
        }
        batch.vouchers.push(lane.voucher.clone());
    }
    order
        .into_iter()
        .filter_map(|pool_id| by_pool.remove(&pool_id))
        .collect()
}

/// Partition planned lanes into gas-bounded redemption chunks (each an
/// independent `redeemMany`). Every returned chunk has at most `max_vouchers`
/// lanes and an aggregate unredeemed value `>= floor`; a chunk that cannot clear
/// the floor is dropped and its lanes defer to a later sweep (the force path
/// passes `floor == 0` to keep every chunk). The chunk count is the minimum that
/// respects `max_vouchers`, and high-value lanes are dealt round-robin across the
/// chunks so dust rides alongside real value instead of segregating into a
/// below-floor chunk.
fn chunk_redemptions(
    mut plans: Vec<PlannedLane>,
    floor: U256,
    max_vouchers: usize,
) -> Vec<Vec<PlannedLane>> {
    if plans.is_empty() {
        return Vec::new();
    }
    let cap = max_vouchers.max(1);
    let k = plans.len().div_ceil(cap);
    // Sort by unredeemed descending so round-robin dealing balances value across
    // chunks (largest lanes land in distinct buckets first).
    plans.sort_by_key(|plan| Reverse(plan.unredeemed));
    let mut buckets: Vec<Vec<PlannedLane>> = (0..k).map(|_| Vec::new()).collect();
    for (i, plan) in plans.into_iter().enumerate() {
        if let Some(bucket) = buckets.get_mut(i % k) {
            bucket.push(plan);
        }
    }
    buckets
        .into_iter()
        .filter(|bucket| {
            let sum = bucket
                .iter()
                .map(|l| l.unredeemed)
                .fold(U256::ZERO, |a, b| a + b);
            sum >= floor
        })
        .collect()
}

/// Whether a lane's observed registration is still live at `now`. `0`
/// (unknown/unregistered) and any past expiry are "not registered" — the safe
/// direction is to re-read rather than skip a possibly-absent registration.
const fn is_registered(registered_until: u64, now: u64) -> bool {
    registered_until > now
}

/// The on-chain registration status resolved for a lane before planning it.
enum RegistrationStatus {
    /// `registered_until > now`: skip the chain read, attach no `CapabilityReg`.
    Registered,
    /// A fresh `getAuthorization` for a lane whose persisted state was
    /// unknown/expired. `cap == 0` means "attach a `CapabilityReg`".
    Fetched(PaymentPool::Authorization),
}

/// Plan one lane for redemption from its already-loaded state and a resolved
/// registration status. Pure and I/O-free — the caller ([`plan_lanes`])
/// resolves the registration status first (batched chain read or the
/// persisted `registered_until` watermark). Returns `Ok(None)` when the lane
/// is not this node's, has no signed voucher, has nothing unredeemed, or is
/// an unregistered signer for which this node holds no capability material.
fn plan_lane(
    st: &LaneState,
    capabilities: &Arc<dyn CapabilitySource>,
    paid: &PaidWatermarks,
    self_address: Address,
    status: &RegistrationStatus,
) -> Result<Option<PlannedLane>> {
    if st.provider != self_address {
        return Ok(None);
    }
    let key = st.key();
    let Some(sig_bytes) = st.last_signature() else {
        return Ok(None);
    };
    let owed = st.last_amount();
    if owed.is_zero() {
        return Ok(None);
    }
    let unredeemed = owed.saturating_sub(paid.get(&key));
    if unredeemed.is_zero() {
        return Ok(None);
    }

    let register = match status {
        RegistrationStatus::Fetched(auth) if auth.cap == 0 => {
            let Some(material) = capabilities.registration_material(&key) else {
                warn!(
                    pool_id = %key.pool_id,
                    signer = %key.signer,
                    "signer not registered on-chain and no capability held; skipping redemption"
                );
                return Ok(None);
            };
            Some(PaymentPool::CapabilityReg {
                signer: key.signer,
                spendingCap: to_pool_u64(material.spending_cap, "spending cap")?,
                expiry: material.expiry,
                ownerSig: material.owner_sig,
            })
        }
        RegistrationStatus::Registered | RegistrationStatus::Fetched(_) => None,
    };

    let (r, vs) =
        compact_voucher_signature(sig_bytes).context("compact the lane's voucher signature")?;
    let voucher = PaymentPool::LaneVoucher {
        signer: key.signer,
        cumulative: to_pool_u64(owed, "voucher cumulative")?,
        bytesDelivered: to_pool_u64(st.last_bytes_delivered(), "voucher bytes delivered")?,
        r,
        vs,
    };
    Ok(Some(PlannedLane {
        pool_id: key.pool_id,
        key,
        unredeemed,
        voucher,
        register,
    }))
}

/// One `getAuthorizations` for every lane whose registration this node does not
/// already know, returned as a `(pool_id, signer) -> Authorization` map. An
/// empty input issues no call. A length skew in the reply (never expected from
/// the contract) drops the unpaired tail — those lanes simply defer to the next
/// sweep — rather than indexing out of bounds.
async fn batched_authorizations<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    keys: &[LaneKey],
) -> Result<HashMap<(B256, Address), PaymentPool::Authorization>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let pool_ids: Vec<B256> = keys.iter().map(|k| k.pool_id).collect();
    let signers: Vec<Address> = keys.iter().map(|k| k.signer).collect();
    let auths = contract
        .getAuthorizations(pool_ids, signers)
        .call()
        .await
        .context("getAuthorizations for redemption")?;
    if auths.len() != keys.len() {
        warn!(
            requested = keys.len(),
            returned = auths.len(),
            "getAuthorizations returned a mismatched count; deferring the unpaired lanes"
        );
    }
    let mut map = HashMap::with_capacity(keys.len());
    for (key, auth) in keys.iter().zip(auths) {
        map.insert((key.pool_id, key.signer), auth);
    }
    Ok(map)
}

/// Plan a set of candidate lanes for redemption with ONE batched
/// `getAuthorizations` for the lanes whose registration is unknown or expired.
/// A lane whose persisted `registered_until` is still live skips the read
/// entirely. For a freshly-read lane already registered on-chain (`cap != 0`),
/// the observed `expiry` is persisted so later sweeps skip its read too. A batch
/// read failure defers the read-needing lanes to the next sweep (their planning
/// is dropped this pass) while still planning the known-registered lanes.
#[allow(clippy::cognitive_complexity)]
async fn plan_lanes<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    capabilities: &Arc<dyn CapabilitySource>,
    paid: &PaidWatermarks,
    self_address: Address,
    states: Vec<LaneState>,
    metrics: &Arc<Metrics>,
) -> Vec<PlannedLane> {
    let now = unix_now();
    let read_keys: Vec<LaneKey> = states
        .iter()
        .filter(|st| st.provider == self_address && !is_registered(st.registered_until, now))
        .map(LaneState::key)
        .collect();
    let auth_map = match batched_authorizations(contract, &read_keys).await {
        Ok(m) => m,
        Err(err) => {
            metrics.redemption_failure();
            warn!(err = %sanitize_rpc_display(&err), "batched getAuthorizations failed; deferring read-needing lanes");
            HashMap::new()
        }
    };
    // Persist observed expiry for signers already registered on-chain, so a
    // restart or later sweep skips their read. `read_keys` already holds exactly
    // the lanes queried (each a unique `(pool_id, signer)` at `provider ==
    // self`), so walk it directly rather than rescanning `states` per auth.
    for key in &read_keys {
        if let Some(auth) = auth_map.get(&(key.pool_id, key.signer))
            && auth.cap != 0
            && let Err(err) = store.set_registered_until(*key, auth.expiry)
        {
            warn!(%err, pool_id = %key.pool_id, signer = %key.signer,
                "failed to persist observed registration expiry");
        }
    }
    let mut plans: Vec<PlannedLane> = Vec::new();
    for st in &states {
        if st.provider != self_address {
            continue;
        }
        let reg_status = if is_registered(st.registered_until, now) {
            RegistrationStatus::Registered
        } else {
            match auth_map.get(&(st.pool_id, st.signer)) {
                Some(auth) => RegistrationStatus::Fetched(auth.clone()),
                None => continue, // read failed/omitted; defer to the next sweep
            }
        };
        match plan_lane(st, capabilities, paid, self_address, &reg_status) {
            Ok(Some(lane)) => plans.push(lane),
            Ok(None) => {}
            Err(err) => {
                metrics.redemption_failure();
                warn!(err = %sanitize_rpc_display(&err), pool_id = %st.pool_id, "redemption planning failed");
            }
        }
    }
    plans
}

/// Hint-path redemption: plan one lane and, if it clears the per-chunk `floor`,
/// submit it as a one-lane `redeemMany`. A sub-floor hint defers to the next
/// sweep, which packs it with other lanes.
#[allow(clippy::too_many_arguments)]
async fn redeem_one<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    capabilities: &Arc<dyn CapabilitySource>,
    paid: &PaidWatermarks,
    self_address: Address,
    floor: U256,
    max_vouchers: usize,
    key: LaneKey,
    metrics: &Arc<Metrics>,
) {
    let st = match store.get(key) {
        Ok(Some(st)) => st,
        Ok(None) => return,
        Err(err) => {
            metrics.redemption_failure();
            warn!(err = %err, pool_id = %key.pool_id, "redemption planning failed to load lane");
            return;
        }
    };
    let plans = plan_lanes(
        contract,
        store,
        capabilities,
        paid,
        self_address,
        vec![st],
        metrics,
    )
    .await;
    // Hint path: require the durability floor and skip the submit on a failed
    // flush (`strict_flush`); the lane defers to the next sweep.
    redeem_planned_lanes(contract, store, plans, floor, max_vouchers, true, metrics).await;
}

/// Self-tick sweep: scan every persisted lane, plan each (per-lane error
/// isolation through the planning phase), then chunk the survivors under the
/// per-chunk floor + voucher-count cap and submit each chunk as its own
/// `redeemMany`. Value is spread across chunks so dust settles alongside real
/// value; a chunk that cannot clear the floor defers to a later tick.
#[allow(clippy::too_many_arguments)]
async fn redeem_sweep<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    capabilities: &Arc<dyn CapabilitySource>,
    paid: &PaidWatermarks,
    self_address: Address,
    floor: U256,
    max_vouchers: usize,
    strict_flush: bool,
    metrics: &Arc<Metrics>,
) {
    let states = match store.load_all() {
        Ok(s) => s,
        Err(err) => {
            warn!(%err, "redeemer self-tick: failed to load lane state");
            return;
        }
    };
    let plans = plan_lanes(
        contract,
        store,
        capabilities,
        paid,
        self_address,
        states,
        metrics,
    )
    .await;
    redeem_planned_lanes(
        contract,
        store,
        plans,
        floor,
        max_vouchers,
        strict_flush,
        metrics,
    )
    .await;
}

/// Submit one chunk of planned lanes as a single `redeemMany`. On an oversize
/// send failure (`is_oversize_send_err`) with more than one lane, halve the chunk
/// and retry each half, bounded by `MAX_SPLIT_DEPTH`. A revert / receipt failure /
/// timeout / non-oversize send error records one `redemption_failure` and leaves
/// the claims for the next sweep (cumulative, monotone, retry-safe). Does NOT seed
/// the paid cache — each paid voucher emits its own `PoolRedeemed`.
#[allow(clippy::cognitive_complexity)]
async fn submit_chunk<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    mut lanes: Vec<PlannedLane>,
    metrics: &Arc<Metrics>,
    depth: u32,
) {
    if lanes.is_empty() {
        return;
    }
    let batches = group_by_pool(&lanes);
    let cap_count: usize = batches.iter().map(|b| b.capabilities.len()).sum();
    let voucher_count = lanes.len();
    let pool_count = batches.len();
    let sent = contract.redeemMany(batches).send().await;
    match send_and_await_receipt(sent, Some(REDEEM_RECEIPT_TIMEOUT)).await {
        TxOutcome::Landed(receipt) => {
            info!(
                pool_count,
                cap_count,
                voucher_count,
                tx = %receipt.transaction_hash,
                "batched lane redemption landed (redeemMany)"
            );
            for lane in &lanes {
                if let Some(reg) = &lane.register
                    && let Err(err) = store.set_registered_until(lane.key, reg.expiry)
                {
                    warn!(%err, pool_id = %lane.pool_id, signer = %reg.signer,
                        "failed to persist registered_until after a landed registration");
                }
            }
        }
        TxOutcome::SendErr(err)
            if lanes.len() >= 2
                && depth < MAX_SPLIT_DEPTH
                && is_oversize_send_err(&err.to_string()) =>
        {
            let mid = lanes.len() / 2;
            let right = lanes.split_off(mid);
            warn!(
                voucher_count,
                depth, "redeemMany send rejected oversized; halving chunk and retrying"
            );
            Box::pin(submit_chunk(contract, store, lanes, metrics, depth + 1)).await;
            Box::pin(submit_chunk(contract, store, right, metrics, depth + 1)).await;
        }
        TxOutcome::Reverted(receipt) => {
            metrics.redemption_failure();
            warn!(voucher_count, tx = %receipt.transaction_hash, "redeemMany reverted on-chain; leaving claims for retry");
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
            warn!(voucher_count, timeout = ?REDEEM_RECEIPT_TIMEOUT, "redeemMany receipt timed out; leaving claims for retry");
        }
    }
}

/// Flush the lane store durable off the async worker (the fsync must not block a
/// runtime worker). Returns `true` on success; a failure is metered and logged.
/// A redeem that requires the redeemed-watermark floor (the periodic sweep) skips
/// its submit when this returns `false`; the forced close/shutdown paths proceed.
async fn flush_store_durable(store: &Arc<dyn PoolStateStore>, metrics: &Arc<Metrics>) -> bool {
    let store = Arc::clone(store);
    match tokio::task::spawn_blocking(move || store.flush()).await {
        Ok(Ok(())) => true,
        Ok(Err(err)) => {
            metrics.lane_flush_failure();
            warn!(%err, "pre-redeem lane store flush failed; deferring redeem");
            false
        }
        Err(join_err) => {
            metrics.lane_flush_failure();
            warn!(%join_err, "pre-redeem lane store flush task join failed");
            false
        }
    }
}

/// Chunk planned lanes under the per-chunk floor + voucher-count cap and submit
/// each chunk. `floor == U256::ZERO` forces every lane (the close/shutdown path).
///
/// Floors the redeemed watermark first: flushes the lane store durable AFTER the
/// lanes were planned (their cumulative amounts already read) and BEFORE any
/// chunk goes on-chain, so a crash right after a submit still finds on-disk
/// `owed ≥ submitted` (`record` is monotone, so the flush persists at least every
/// value in the batch). The periodic sweep and hint path require this floor and
/// skip their submit on a failed flush (`strict_flush`); the forced
/// close/shutdown paths redeem regardless, since forfeiting the whole claim at the
/// deadline is worse than a bounded re-serve risk.
async fn redeem_planned_lanes<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    plans: Vec<PlannedLane>,
    floor: U256,
    max_vouchers: usize,
    strict_flush: bool,
    metrics: &Arc<Metrics>,
) {
    let chunks = chunk_redemptions(plans, floor, max_vouchers);
    if chunks.is_empty() {
        return;
    }
    if !flush_store_durable(store, metrics).await && strict_flush {
        return;
    }
    for chunk in chunks {
        submit_chunk(contract, store, chunk, metrics, 0).await;
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

/// Whether a `redeemMany` send error looks like "the transaction is too big to
/// include" — exceeding the block gas limit or the node/mempool transaction-size
/// cap — rather than a revert or a transient RPC fault. Matched case-insensitively
/// against a small set of client markers; the caller also bounds retry depth, so a
/// false negative simply leaves the claim for the next sweep and a false positive
/// costs at most a bounded number of doomed smaller sends.
fn is_oversize_send_err(msg: &str) -> bool {
    const MARKERS: [&str; 5] = [
        "gas required exceeds",
        "exceeds block gas limit",
        "oversized data",
        "transaction too large",
        "request entity too large",
    ];
    let m = msg.to_ascii_lowercase();
    MARKERS.iter().any(|marker| m.contains(marker))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a [`PlannedLane`] for a given pool/signer, with an optional
    /// capability registration, for `group_by_pool` tests.
    fn planned(pool: u8, signer: u8, unredeemed: u64, register: bool) -> PlannedLane {
        let signer_addr = Address::from([signer; 20]);
        let reg = register.then(|| PaymentPool::CapabilityReg {
            signer: signer_addr,
            spendingCap: 1_000_000,
            expiry: 0,
            ownerSig: Bytes::from(vec![9u8; 65]),
        });
        PlannedLane {
            pool_id: PoolId::from([pool; 32]),
            key: LaneKey {
                pool_id: PoolId::from([pool; 32]),
                signer: signer_addr,
                provider: Address::from([0xEE; 20]),
            },
            unredeemed: U256::from(unredeemed),
            voucher: PaymentPool::LaneVoucher {
                signer: signer_addr,
                cumulative: unredeemed,
                bytesDelivered: 0,
                r: B256::ZERO,
                vs: B256::ZERO,
            },
            register: reg,
        }
    }

    #[test]
    fn group_by_pool_buckets_lanes_and_keeps_insertion_order() {
        let lanes = vec![
            planned(1, 10, 100, true),
            planned(2, 11, 200, false),
            planned(1, 12, 300, true),
        ];
        let batches = group_by_pool(&lanes);
        // Two pools, first-seen order: pool 1 then pool 2. Indexed via `.first()`
        // / `.get()` rather than `[]` per the workspace's anti-panic policy.
        assert_eq!(batches.len(), 2);
        let batch0 = batches.first();
        assert_eq!(batch0.map(|b| b.poolId), Some(PoolId::from([1u8; 32])));
        assert_eq!(batch0.map(|b| b.vouchers.len()), Some(2)); // both pool-1 lanes
        assert_eq!(batch0.map(|b| b.capabilities.len()), Some(2)); // both registered
        let batch1 = batches.get(1);
        assert_eq!(batch1.map(|b| b.poolId), Some(PoolId::from([2u8; 32])));
        assert_eq!(batch1.map(|b| b.vouchers.len()), Some(1));
        assert_eq!(batch1.map(|b| b.capabilities.len()), Some(0)); // register == false
    }

    /// Sum a chunk's unredeemed values, for `chunk_redemptions` tests.
    fn total_unredeemed(chunk: &[PlannedLane]) -> U256 {
        chunk
            .iter()
            .map(|l| l.unredeemed)
            .fold(U256::ZERO, |a, b| a + b)
    }

    #[test]
    fn chunk_redemptions_caps_vouchers_per_chunk() {
        // 5 lanes, cap 2 => 3 chunks (2 + 2 + 1). All above floor.
        let plans = (0..5).map(|i| planned(1, i, 1_000_000, false)).collect();
        let chunks = chunk_redemptions(plans, U256::from(1u64), 2);
        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|c| c.len() <= 2));
        assert_eq!(chunks.iter().map(Vec::len).sum::<usize>(), 5);
    }

    #[test]
    fn chunk_redemptions_drops_below_floor_chunk() {
        // One dust lane, floor 1 USDC => nothing submitted (defers).
        let plans = vec![planned(1, 0, 10, false)];
        let chunks = chunk_redemptions(plans, U256::from(1_000_000u64), 300);
        assert!(chunks.is_empty());
    }

    #[test]
    fn chunk_redemptions_zero_floor_keeps_everything() {
        // Force path: floor 0 keeps even a pure-dust chunk.
        let plans = vec![planned(1, 0, 1, false), planned(1, 1, 1, false)];
        let chunks = chunk_redemptions(plans, U256::ZERO, 300);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks.first().map(Vec::len), Some(2));
    }

    #[test]
    fn chunk_redemptions_spreads_value_so_dust_rides_along() {
        // 2 whales + 2 dust, cap 2 => 2 chunks. Value-spreading puts one whale in
        // each chunk, so each chunk clears a floor no single dust lane could.
        let plans = vec![
            planned(1, 0, 1_000_000, false), // whale
            planned(1, 1, 1_000_000, false), // whale
            planned(1, 2, 5, false),         // dust
            planned(1, 3, 5, false),         // dust
        ];
        let chunks = chunk_redemptions(plans, U256::from(500_000u64), 2);
        assert_eq!(chunks.len(), 2);
        // Every submitted chunk clears the floor (dust rode along with a whale).
        assert!(
            chunks
                .iter()
                .all(|c| total_unredeemed(c) >= U256::from(500_000u64))
        );
        // All four lanes survived (none stranded).
        assert_eq!(chunks.iter().map(Vec::len).sum::<usize>(), 4);
    }

    #[test]
    fn chunk_redemptions_empty_input_is_empty() {
        assert!(chunk_redemptions(Vec::new(), U256::ZERO, 300).is_empty());
    }

    /// A 65-byte `r‖s‖v` signature with a low `s` and the recovery byte set to
    /// `v` (a `[u8; 65]` so a const index stays provably in-bounds for the
    /// anti-panic lints).
    fn sig_with_v(v: u8) -> [u8; 65] {
        let mut s = [7u8; 65];
        if let Some(last) = s.last_mut() {
            *last = v;
        }
        s
    }

    #[test]
    fn compaction_folds_raw_y_parity_into_the_top_bit_of_s() -> Result<()> {
        // `s` here is 0x0707…07, so its top bit is free: parity 0 leaves it
        // clear, parity 1 sets it, and the rest of `s` is untouched.
        let (r, vs) = compact_voucher_signature(&sig_with_v(0))?;
        assert_eq!(r, B256::repeat_byte(7), "r passes through unchanged");
        assert_eq!(
            vs,
            B256::repeat_byte(7),
            "parity 0 leaves the top bit clear"
        );

        let (_, vs_odd) = compact_voucher_signature(&sig_with_v(1))?;
        assert_eq!(
            vs_odd.0.first(),
            Some(&0x87),
            "parity 1 sets the top bit of s"
        );
        assert_eq!(vs_odd.0.get(1), Some(&7), "and disturbs nothing else");
        Ok(())
    }

    #[test]
    fn compaction_accepts_the_eth_v_convention_identically() -> Result<()> {
        assert_eq!(
            compact_voucher_signature(&sig_with_v(27))?,
            compact_voucher_signature(&sig_with_v(0))?,
            "27 and 0 are the same parity"
        );
        assert_eq!(
            compact_voucher_signature(&sig_with_v(28))?,
            compact_voucher_signature(&sig_with_v(1))?,
            "28 and 1 are the same parity"
        );
        Ok(())
    }

    #[test]
    fn compaction_rejects_a_signature_it_cannot_represent() {
        // Not 65 bytes: there is no `r‖s‖v` to split.
        assert!(compact_voucher_signature(&[1u8, 2, 3]).is_err());

        // An unusable recovery id.
        assert!(compact_voucher_signature(&sig_with_v(4)).is_err());

        // High `s` with the top bit obviously set.
        let mut high_s = sig_with_v(27);
        if let Some(top) = high_s.get_mut(32) {
            *top = 0xFF;
        }
        assert!(compact_voucher_signature(&high_s).is_err());
    }

    /// The band a "is the top bit free?" test would wave through: `n / 2` sits
    /// below `2^255`, so roughly `2^128` values of `s` have a clear top bit and
    /// are still high-`s`. Compaction would fold the recovery bit into one
    /// happily, and the contract's `ECDSA.tryRecover` would then reject it —
    /// reverting the whole `redeemMany` and taking every honest lane in the
    /// batch with it.
    #[test]
    fn compaction_rejects_high_s_whose_top_bit_is_clear() {
        // n/2 + 1: the smallest high-`s` value, and its top bit is 0.
        let half_plus_one = alloy::primitives::U256::from_be_bytes([
            0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0x5d, 0x57, 0x6e, 0x73, 0x57, 0xa4, 0x50, 0x1d, 0xdf, 0xe9, 0x2f, 0x46,
            0x68, 0x1b, 0x20, 0xa1,
        ]);
        let s_bytes = half_plus_one.to_be_bytes::<32>();
        assert_eq!(
            s_bytes.first().map(|b| b & 0x80),
            Some(0),
            "top bit is clear"
        );

        let mut raw = [7u8; 65];
        if let Some(slot) = raw.get_mut(32..64) {
            slot.copy_from_slice(&s_bytes);
        }
        if let Some(last) = raw.last_mut() {
            *last = 27;
        }

        assert!(
            compact_voucher_signature(&raw).is_err(),
            "a clear top bit does not make `s` canonical"
        );
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

    #[test]
    fn is_oversize_send_err_matches_known_markers() {
        for m in [
            "err: gas required exceeds allowance (30000000)",
            "transaction exceeds block gas limit",
            "oversized data",
            "TRANSACTION TOO LARGE",
        ] {
            assert!(is_oversize_send_err(m), "should flag: {m}");
        }
    }

    #[test]
    fn is_oversize_send_err_ignores_unrelated_errors() {
        for m in ["nonce too low", "connection refused", "execution reverted"] {
            assert!(!is_oversize_send_err(m), "should not flag: {m}");
        }
    }

    /// The settlement route watches exactly the five `PaymentPool` lifecycle
    /// events — no more, no fewer. `PoolOpened` in particular must never be
    /// dropped: it is the close monitor's only signal that a pool exists.
    #[test]
    fn route_topic0s_covers_every_pool_lifecycle_event() {
        assert_eq!(
            settlement_route_topic0s(),
            vec![
                PaymentPool::PoolOpened::SIGNATURE_HASH,
                PaymentPool::PoolRedeemed::SIGNATURE_HASH,
                PaymentPool::PoolToppedUp::SIGNATURE_HASH,
                PaymentPool::PoolCloseInitiated::SIGNATURE_HASH,
                PaymentPool::PoolReclaimed::SIGNATURE_HASH,
            ]
        );
    }

    /// The settlement route resumes from the durable `PoolOpened` checkpoint,
    /// rewound by the reorg margin, and anchors a cold (first-ever) boot at
    /// head rather than replaying all of history.
    #[test]
    fn cursor_start_resumes_from_pool_opened_checkpoint() {
        #[derive(Default)]
        struct NoopCk;
        impl KeyedCheckpointStore for NoopCk {
            fn load_checkpoint(&self, _key: CheckpointKey) -> Result<Option<u64>, StoreError> {
                Ok(None)
            }
            fn record_checkpoint(
                &self,
                _key: CheckpointKey,
                _block: u64,
            ) -> Result<(), StoreError> {
                Ok(())
            }
        }
        let store: Arc<dyn KeyedCheckpointStore> = Arc::new(NoopCk);
        let start = cursor_start(store);
        match start {
            CursorStart::FromCheckpoint {
                checkpoint,
                reorg_margin,
                cold_start,
            } => {
                assert_eq!(checkpoint.key, CheckpointKey::PoolOpened);
                assert_eq!(reorg_margin, REORG_MARGIN_BLOCKS);
                assert_eq!(cold_start, ColdStart::Head);
            }
            CursorStart::Seeded { .. } => unreachable!("expected FromCheckpoint, got Seeded"),
            CursorStart::HeadMinusWindow { .. } => {
                unreachable!("expected FromCheckpoint, got HeadMinusWindow")
            }
        }
    }

    #[test]
    fn is_registered_treats_zero_and_past_as_unregistered() {
        assert!(!super::is_registered(0, 1000), "0 = unknown");
        assert!(!super::is_registered(999, 1000), "expired");
        assert!(super::is_registered(1001, 1000), "live");
    }

    /// A [`CapabilitySource`] test double: returns fixed material for a set of
    /// keys, `None` for everything else.
    struct FixedCapabilitySource {
        material: Option<CapabilityMaterial>,
    }

    impl CapabilitySource for FixedCapabilitySource {
        fn registration_material(&self, _key: &LaneKey) -> Option<CapabilityMaterial> {
            self.material.clone()
        }
    }

    /// A lane with a signed voucher and non-zero owed amount, ready for
    /// `plan_lane` tests.
    fn signed_lane_state(pool: u8, signer: u8, provider: u8) -> LaneState {
        LaneState::hydrate(
            PoolId::from([pool; 32]),
            Address::from([signer; 20]),
            Address::from([provider; 20]),
            U256::from(10_000_000u64),
            0,
            U256::from(1_000u64),
            U256::from(1_048_576u64),
            Some(sig_with_v(0)),
        )
    }

    fn material() -> CapabilityMaterial {
        CapabilityMaterial {
            spending_cap: U256::from(1_000_000u64),
            expiry: 1_800_000_000,
            owner_sig: Bytes::from(vec![9u8; 65]),
        }
    }

    fn auth(cap: u64, expiry: u64) -> PaymentPool::Authorization {
        PaymentPool::Authorization {
            cap,
            expiry,
            spent: 0,
        }
    }

    #[test]
    fn plan_lane_registered_status_skips_read_and_omits_capability_reg() -> Result<()> {
        let st = signed_lane_state(1, 10, 20);
        let capabilities: Arc<dyn CapabilitySource> =
            Arc::new(FixedCapabilitySource { material: None });
        let paid = PaidWatermarks::default();
        let plan = plan_lane(
            &st,
            &capabilities,
            &paid,
            Address::from([20u8; 20]),
            &RegistrationStatus::Registered,
        )?
        .ok_or_else(|| anyhow::anyhow!("registered lane with owed balance should plan"))?;
        assert!(
            plan.register.is_none(),
            "registered lane attaches no CapabilityReg"
        );
        assert_eq!(plan.key, st.key());
        Ok(())
    }

    #[test]
    fn plan_lane_fetched_unregistered_with_material_attaches_registration() -> Result<()> {
        let st = signed_lane_state(1, 11, 21);
        let capabilities: Arc<dyn CapabilitySource> = Arc::new(FixedCapabilitySource {
            material: Some(material()),
        });
        let paid = PaidWatermarks::default();
        let status = RegistrationStatus::Fetched(auth(0, 0));
        let plan = plan_lane(
            &st,
            &capabilities,
            &paid,
            Address::from([21u8; 20]),
            &status,
        )?
        .ok_or_else(|| anyhow::anyhow!("unregistered lane with held material should plan"))?;
        assert!(
            plan.register.is_some(),
            "cap==0 + material attaches a CapabilityReg"
        );
        assert_eq!(plan.key, st.key());
        Ok(())
    }

    #[test]
    fn plan_lane_fetched_unregistered_without_material_is_skipped() -> Result<()> {
        let st = signed_lane_state(1, 12, 22);
        let capabilities: Arc<dyn CapabilitySource> =
            Arc::new(FixedCapabilitySource { material: None });
        let paid = PaidWatermarks::default();
        let status = RegistrationStatus::Fetched(auth(0, 0));
        let plan = plan_lane(
            &st,
            &capabilities,
            &paid,
            Address::from([22u8; 20]),
            &status,
        )?;
        assert!(plan.is_none(), "no material and cap==0 skips the lane");
        Ok(())
    }

    #[test]
    fn plan_lane_fetched_already_registered_omits_registration() -> Result<()> {
        let st = signed_lane_state(1, 13, 23);
        let capabilities: Arc<dyn CapabilitySource> =
            Arc::new(FixedCapabilitySource { material: None });
        let paid = PaidWatermarks::default();
        let status = RegistrationStatus::Fetched(auth(5_000_000, 1_800_000_000));
        let plan = plan_lane(
            &st,
            &capabilities,
            &paid,
            Address::from([23u8; 20]),
            &status,
        )?
        .ok_or_else(|| anyhow::anyhow!("already-registered lane with owed balance should plan"))?;
        assert!(
            plan.register.is_none(),
            "cap!=0 rides the existing registration; no CapabilityReg attached"
        );
        assert_eq!(plan.key, st.key());
        Ok(())
    }
}
