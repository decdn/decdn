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
//!   redemption — with no chain read. A signer this node has not yet registered
//!   attaches a `CapabilityReg` from its held `owner_sig`; on-chain registration
//!   is idempotent, and the first landed redemption persists `registered_until`
//!   so later passes attach nothing. A low-frequency
//!   self-tick sweeps every persisted lane, packs the planned lanes into
//!   chunks, and submits a chunk — one `redeemMany` — only once the aggregate
//!   unredeemed value across that chunk's lanes clears a configurable floor,
//!   so a dropped hint never strands a lane whose chunk has cleared the floor.
//!   The node flushes the lane store durable after planning a chunk and before
//!   submitting it, so post-crash on-disk `owed ≥ submitted`.
//! - **Redeem before reclaim.** A pool is owner-owned and owner-closed; the node
//!   never closes, disputes, or settles. An owner's `closePool` only starts the
//!   grace window (ADR 003 § Owner reclaims before a node redeems). The node runs
//!   no force-redeem on close: the self-tick sweep runs far inside the 48h grace
//!   floor (`redeem_interval_secs` is capped well below it at config load), so it
//!   redeems every lane worth the gas before the owner can `reclaim`. A lane below
//!   the per-chunk floor is left for the owner to reclaim — spending more gas than
//!   it recovers is not worth it — and a node offline for the whole grace window
//!   forfeits its unredeemed vouchers, a node-ops failure, not a protocol gap.
//!   The sink does fold `PoolCloseInitiated` into the projection (marking the pool
//!   `Closing` with its deadline) so the redeemer's solvency gate drops a drained
//!   or past-deadline lane instead of submitting a `redeemMany` that reverts
//!   `PoolClosed`.
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
use tracing::{debug, error, info, warn};

use crate::chain_events::REORG_MARGIN_BLOCKS;
use crate::chain_events::multiplexed_poller::{Route, SinkSource};
use crate::chain_events::resumable_watcher::{Checkpoint, ColdStart, CursorStart, LogSink};
use crate::handlers::client::ClientHandler;
use crate::metrics::{Metrics, metric_hook};
use crate::onchain_tx::{TxOutcome, send_and_await_receipt};
use crate::pool_view::{Lifecycle, PoolProjection, PoolStatus};

/// Capacity of the redeem-hint channel. Hints are advisory (a missed hint only
/// delays a redemption until the next voucher or self-tick sweep), so a bounded
/// channel that drops on overflow is acceptable — sized for a burst of concurrent
/// lanes without backpressuring the voucher-accept path.
pub const REDEEM_HINT_CAPACITY: usize = 256;

/// How long the admit-path `getPool` suppresses a repeat call for a pool it
/// just found not-servable (nonexistent / `Closed`) or that errored. A
/// not-servable pool never folds into the projection, so its `snapshot` stays
/// `None`; without this a client re-sending its capability on every request would
/// drive one `getPool` per request against the same dead pool. On expiry the pool
/// is re-checked once — the window is short enough that a pool opened after a
/// negative result still becomes servable within it, long enough to collapse a
/// request flood to ~one call per pool per window.
const RESOLVE_NEGATIVE_TTL: Duration = Duration::from_mins(1);

/// Cap on the admit-path negative cache, bounding its memory against a flood of
/// distinct nonexistent pool ids. At the cap an insert first prunes expired
/// entries; a flood of live distinct negatives past that simply pays one
/// `getPool` per admit rather than growing the cache without limit.
const RESOLVE_NEGATIVE_CACHE_MAX: usize = 4096;

/// How long an admit-path `getAuthorization` headroom read stays fresh in the
/// signer-auth cache. A cached headroom may go stale as the signer spends its
/// shared `cap` at other nodes, so within the window a stale-OK entry admits
/// however many streams that signer opens against a cap it has since drained — a
/// TTL-bounded over-admission, not a per-stream one. The exposure is bounded anyway
/// by the on-chain `redeemMany`, which pays `min(desired, cap − spent)` and never
/// over-cashes; a short TTL keeps the window small while a repeat fetch within it
/// does no on-chain read.
const SIGNER_AUTH_TTL: Duration = Duration::from_mins(1);

/// Cap on the admit-path signer-auth cache, bounding its memory against a flood of
/// distinct `(pool, signer)` pairs. At the cap an insert first prunes expired
/// entries; if the cache is still full it skips caching that entry and pays one
/// extra `getAuthorization` next time, so the map never grows without bound.
/// Mirrors [`RESOLVE_NEGATIVE_CACHE_MAX`].
const AUTH_CACHE_MAX: usize = 4096;

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

/// Current Unix time in seconds, compared against a lane's cached
/// `registered_until` to skip the on-chain authorization read while the
/// registration is still live (`registered_until > now`). A broken system clock
/// (time before the epoch) yields `0`, so any non-zero `registered_until` looks
/// live and the read is skipped. This is benign: if the capability has actually
/// expired on-chain, the redemption simply no-ops as transient-empty at the
/// contract, which stays the authoritative backstop.
pub(crate) fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
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
    paid: PaidWatermarks,
    self_address: Address,
    redeem_threshold: U256,
    redeem_max_vouchers_per_tx: usize,
    metrics: Arc<Metrics>,
    pool_view: PoolProjection,
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
            self_address,
            store: Arc::clone(&store),
            handler,
            paid: paid.clone(),
            redeem_tx: redeem_tx.clone(),
            metrics: Arc::clone(&metrics),
            pool_view: pool_view.clone(),
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
            paid.clone(),
            self_address,
            redeem_threshold,
            redeem_max_vouchers_per_tx,
            redeem_interval,
            redeem_rx,
            Arc::clone(&metrics),
            pool_view.clone(),
        ));

        Ok((
            Self {
                contract,
                redeem_tx,
                store,
                paid,
                self_address,
                redeem_threshold,
                redeem_max_vouchers_per_tx,
                metrics,
                pool_view,
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
            &self.paid,
            self.self_address,
            self.redeem_threshold,
            self.redeem_max_vouchers_per_tx,
            false,
            &self.metrics,
            &self.pool_view,
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
/// paid-watermark cache and pool projection need. Split out from
/// [`PoolSettlementService::bootstrap`] so the exact topic0 set is
/// unit-testable without a provider — dropping `PoolOpened` here, for example,
/// would silently blind the projection to newly opened pools.
fn settlement_route_topic0s() -> Vec<B256> {
    vec![
        PaymentPool::PoolOpened::SIGNATURE_HASH,
        PaymentPool::PoolRedeemed::SIGNATURE_HASH,
        PaymentPool::PoolToppedUp::SIGNATURE_HASH,
        PaymentPool::PoolCloseInitiated::SIGNATURE_HASH,
        PaymentPool::PoolReclaimed::SIGNATURE_HASH,
    ]
}

/// Applies `PaymentPool` settlement logs to the paid-watermark cache and pool
/// projection. One per settlement watcher; the resumable `eth_getLogs`
/// poller feeds it block-ordered logs and advances + persists the scan checkpoint
/// per window on a clean tick.
///
/// Failure policy: paid-watermark updates are in-memory and always `Ok`. A
/// `PoolReclaimed` forget failure is logged and skipped (`Ok`) — the settled lane
/// owes nothing and the forget is idempotent. An undecodable log is log-and-skip
/// so a permanently undecodable log never hot-loops the deterministic re-scan.
struct PoolSettlementSink {
    self_address: Address,
    store: Arc<dyn PoolStateStore>,
    handler: Arc<ClientHandler>,
    paid: PaidWatermarks,
    redeem_tx: mpsc::Sender<LaneKey>,
    metrics: Arc<Metrics>,
    /// The serve path's pool solvency/funder view. This sink is its single writer:
    /// it folds `PoolOpened` (owner + deposit), `PoolToppedUp` (new deposit),
    /// `PoolRedeemed` for EVERY provider (pool-wide `totalRedeemed`),
    /// `PoolCloseInitiated` (mark `Closing` with its deadline), and `PoolReclaimed`
    /// (drop) into the projection so a serve request reads `{owner, remaining}`
    /// in-memory rather than through a `getPool` `eth_call`.
    pool_view: PoolProjection,
}

impl LogSink for PoolSettlementSink {
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
                // Mark the pool `Closing` in the projection so the redeemer's
                // solvency gate reads its dispute deadline: a lane whose pool is
                // drained or already past the deadline is dropped from planning
                // instead of submitting a `redeemMany` that reverts `PoolClosed`.
                // The node runs no force-redeem here — the periodic sweep redeems
                // every lane worth the gas well inside the grace window.
                self.pool_view.record_closing(
                    event.poolId,
                    u64::try_from(event.disputeDeadline).unwrap_or(u64::MAX),
                );
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

impl PoolSettlementSink {
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
        self.handler.forget_pool_floor(pool_id);
    }
}

/// A [`PoolView`](crate::pool_view::PoolView) that confirms a pool on-chain
/// before a serve is admitted.
///
/// The serve gates read `{owner, remaining}` from the event-fed [`PoolProjection`]
/// the settlement watcher folds, so the common case costs no `eth_call`. A pool
/// the projection has not observed is the hard case: a pool opened before this
/// node's `ColdStart::Head` anchor never appears in the forward-only `PoolOpened`
/// scan, so its owner is absent from the projection even though the pool is live.
/// Rather than fail open on that gap, [`status`](crate::pool_view::PoolView::status) does ONE `getPool` at
/// admission — a read the admission path tolerates (it may block) — folds a
/// servable pool into the projection, and refuses an absent, closed, or errored
/// pool. The client re-sends its capability on its next request (the documented
/// lane recovery path), and by then the folded owner registers the lane.
///
/// The mid-stream re-check calls [`cached_status`](crate::pool_view::PoolView::cached_status), which reads the
/// projection ONLY and never blocks on a `getPool` — a chain read at a voucher
/// boundary would stall delivery.
///
/// A short-TTL negative cache suppresses a repeat
/// `getPool` for a pool just found not-servable or that errored, so a client
/// re-requesting the same dead pool every request cannot drive one `getPool` per
/// request.
pub struct ResolvingPoolView<P: Provider + Clone> {
    /// The wallet/RPC-backed `PaymentPool` binding the admit-path `getPool` reads.
    contract: PaymentPool::PaymentPoolInstance<P>,
    /// The event-fed projection this view reads first and folds a resolved pool
    /// into. Shared with the settlement watcher's sink (the authoritative writer).
    projection: PoolProjection,
    /// Pool id → last-negative instant for pools recently found not-servable or
    /// that errored. The guard is held only to read/insert one entry, never across
    /// the `getPool` await.
    negative: Mutex<HashMap<B256, Instant>>,
    /// `(pool_id, signer)` → (last-observed `cap − spent` headroom in micro-USDC,
    /// observed-at instant), for the admit-path signer confirm. An entry younger
    /// than [`SIGNER_AUTH_TTL`] is served without a `getAuthorization`. The guard is
    /// held only to read/insert one entry, never across the `getAuthorization`
    /// await.
    auth_cache: Mutex<HashMap<(B256, Address), (u64, Instant)>>,
}

impl<P: Provider + Clone> std::fmt::Debug for ResolvingPoolView<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvingPoolView")
            .field("address", self.contract.address())
            .field("projection", &self.projection)
            .finish_non_exhaustive()
    }
}

impl<P: Provider + Clone> ResolvingPoolView<P> {
    /// Wrap the event-fed `projection` with an admit-path `getPool` fallback
    /// against `contract`.
    #[must_use]
    pub fn new(contract: PaymentPool::PaymentPoolInstance<P>, projection: PoolProjection) -> Self {
        Self {
            contract,
            projection,
            negative: Mutex::new(HashMap::new()),
            auth_cache: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl<P: Provider + Clone + 'static> crate::pool_view::PoolView for ResolvingPoolView<P> {
    async fn status(&self, pool_id: B256) -> Option<PoolStatus> {
        // Fast path: the event fold already knows this pool — no chain call.
        if let Some(status) = self.projection.snapshot(pool_id) {
            return Some(status);
        }
        // A pool recently found not-servable (or that errored) is suppressed for
        // the negative-cache window, so a re-request flood cannot storm `getPool`.
        {
            let guard = self
                .negative
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if negative_cache_hit(&guard, pool_id) {
                return None;
            }
        }
        let pool = match self.contract.getPool(pool_id).call().await {
            Ok(pool) => pool,
            Err(err) => {
                warn!(
                    err = %sanitize_rpc_display(&err),
                    %pool_id,
                    "admit getPool failed; refusing this pool"
                );
                let mut guard = self
                    .negative
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                remember_negative(&mut guard, pool_id);
                return None;
            }
        };
        let Some(lifecycle) = resolved_lifecycle(&pool) else {
            debug!(%pool_id, "admit getPool: pool absent or closed; refusing");
            let mut guard = self
                .negative
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            remember_negative(&mut guard, pool_id);
            return None;
        };
        self.projection
            .record_resolved(pool_id, pool.owner, U256::from(pool.deposit), lifecycle);
        // A later reopen at the same id (or a transient error that has since
        // cleared) must not stay suppressed once the pool actually resolves.
        {
            let mut guard = self
                .negative
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.remove(&pool_id);
        }
        self.projection.snapshot(pool_id)
    }

    async fn cached_status(&self, pool_id: B256) -> Option<PoolStatus> {
        // MUST NOT block: read the projection only, never a `getPool`.
        self.projection.snapshot(pool_id)
    }

    async fn signer_spent_cached(&self, pool_id: B256, signer: Address) -> Option<u64> {
        // The mid-stream signer cap-headroom re-check: projection ONLY, never a
        // `getAuthorization`. A signer that drains its shared `cap` at another node
        // shows up here as the settlement watcher folds that node's `PoolRedeemed`.
        Some(self.projection.signer_spent(pool_id, signer))
    }

    async fn signer_cap_headroom_micro(&self, pool_id: B256, signer: Address) -> Option<u64> {
        // Fast path: a fresh cached headroom needs no `getAuthorization`.
        {
            let guard = self
                .auth_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some((headroom, at)) = guard.get(&(pool_id, signer))
                && at.elapsed() < SIGNER_AUTH_TTL
            {
                return Some(*headroom);
            }
        }
        let auth = match self.contract.getAuthorization(pool_id, signer).call().await {
            Ok(auth) => auth,
            Err(err) => {
                warn!(
                    err = %sanitize_rpc_display(&err),
                    %pool_id,
                    %signer,
                    "admit getAuthorization failed; refusing this signer"
                );
                return None;
            }
        };
        // An UNREGISTERED signer reads as the all-zero authorization
        // (`cap == 0 && expiry == 0`): it has spent nothing on-chain and holds its
        // full off-chain capability budget, so it is unconstrained here. A
        // REGISTERED signer has real headroom `cap − spent` — including one whose
        // owner registered it with a zero `spendingCap` (`cap == 0` but
        // `expiry != 0`), whose headroom is `0`, so it is refused rather than
        // misread as unconstrained.
        let headroom = if auth.cap == 0 && auth.expiry == 0 {
            u64::MAX
        } else {
            auth.cap.saturating_sub(auth.spent)
        };
        {
            let mut guard = self
                .auth_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Bound the cache: at the cap, prune expired entries; if it is still
            // full, skip caching this one and pay one extra `getAuthorization` next
            // time rather than let a flood of distinct signers grow the map without
            // bound.
            if guard.len() >= AUTH_CACHE_MAX {
                guard.retain(|_, (_, at)| at.elapsed() < SIGNER_AUTH_TTL);
            }
            if guard.len() < AUTH_CACHE_MAX {
                guard.insert((pool_id, signer), (headroom, Instant::now()));
            }
        }
        Some(headroom)
    }
}

/// Whether `pool_id` is in the negative cache and still fresh — the admit path
/// then skips the `getPool`. Pure, so the TTL gate is unit-testable without a
/// provider.
fn negative_cache_hit(cache: &HashMap<B256, Instant>, pool_id: B256) -> bool {
    cache
        .get(&pool_id)
        .is_some_and(|at| at.elapsed() < RESOLVE_NEGATIVE_TTL)
}

/// Remember `pool_id` as recently not-servable / errored. At the cache cap, prune
/// expired entries first so a flood of distinct nonexistent ids cannot grow the
/// map without bound. Pure, so the cap-prune is unit-testable.
fn remember_negative(cache: &mut HashMap<B256, Instant>, pool_id: B256) {
    if cache.len() >= RESOLVE_NEGATIVE_CACHE_MAX {
        cache.retain(|_, at| at.elapsed() < RESOLVE_NEGATIVE_TTL);
    }
    cache.insert(pool_id, Instant::now());
}

/// The serve-path serve status a resolved `getPool` snapshot maps to, or `None`
/// when the pool is not worth seeding: a zero owner (the pool does not exist, or
/// a reorg unwound it) or a `Closed` (reclaimed / terminal) pool. Both leave the
/// serve gate fail-open `None` rather than register a lane against a pool that
/// can no longer be redeemed. Pure, so the mapping is unit-testable without a
/// provider.
fn resolved_lifecycle(pool: &PaymentPool::Pool) -> Option<Lifecycle> {
    if pool.owner == Address::ZERO {
        return None;
    }
    match pool.status {
        PaymentPool::Status::Open => Some(Lifecycle::Open),
        PaymentPool::Status::Closing => Some(Lifecycle::Closing {
            deadline: pool.disputeDeadline,
        }),
        _ => None,
    }
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
    paid: PaidWatermarks,
    self_address: Address,
    redeem_threshold: U256,
    max_vouchers: usize,
    redeem_interval: Duration,
    mut redeem_rx: mpsc::Receiver<LaneKey>,
    metrics: Arc<Metrics>,
    pool_view: PoolProjection,
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
                        &contract, &store, &paid, self_address,
                        redeem_threshold, max_vouchers, key, &metrics, &pool_view,
                    )
                    .await;
                }
                // All hint senders dropped — the service is going away.
                None => break,
            },
            _ = ticker.tick() => {
                redeem_sweep(
                    &contract, &store, &paid, self_address,
                    redeem_threshold, max_vouchers, true, &metrics, &pool_view,
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

/// Redemption policy: whether a lane whose pool has this `status` is worth
/// submitting now. Fails OPEN on an unknown pool (`None`) — a projection gap
/// (cold start, reorg, an unfolded event) must never hold a lane and strand real
/// money; only a positive zero-`remaining` holds. A drained `Open` pool is held
/// (a top-up re-drives it); a drained `Closing` pool is dropped (it cannot be
/// topped up, so `remaining == 0` is irreversible); a funded `Closing` pool is
/// redeemable only before its deadline, past which `redeemMany` reverts
/// `PoolClosed`.
fn pool_is_redeemable(status: Option<PoolStatus>, now: u64) -> bool {
    match status {
        None => true,
        Some(s) => {
            if s.remaining.is_zero() {
                return false;
            }
            match s.lifecycle {
                Lifecycle::Open => true,
                Lifecycle::Closing { deadline } => now < deadline,
            }
        }
    }
}

/// Split candidate lanes into the ones whose pool can pay now and a count of the
/// ones held/dropped by [`pool_is_redeemable`]. A pool absent from `snapshot` is
/// `None` (fail open).
fn partition_redeemable(
    states: Vec<LaneState>,
    snapshot: &HashMap<PoolId, Option<PoolStatus>>,
    now: u64,
) -> (Vec<LaneState>, usize) {
    let mut kept = Vec::with_capacity(states.len());
    let mut skipped = 0usize;
    for st in states {
        let pool_status = snapshot.get(&st.pool_id).copied().flatten();
        if pool_is_redeemable(pool_status, now) {
            kept.push(st);
        } else {
            skipped += 1;
        }
    }
    (kept, skipped)
}

/// Whether a lane's observed registration is still live at `now`. `0`
/// (unknown/unregistered) and any past expiry are "not registered" — the safe
/// direction is to re-read rather than skip a possibly-absent registration.
const fn is_registered(registered_until: u64, now: u64) -> bool {
    registered_until > now
}

/// Whether this node has confirmed a lane's signer is registered on-chain,
/// resolved from the persisted `registered_until` watermark alone — no chain read.
enum RegistrationStatus {
    /// `registered_until > now`: this node has landed the signer's registration;
    /// attach no `CapabilityReg`.
    Registered,
    /// Not yet confirmed registered by this node: attach a `CapabilityReg` built
    /// from the lane's held `owner_sig`. On-chain registration is idempotent (a
    /// duplicate or already-registered signer is a no-op), so this is safe even
    /// when another provider already registered the signer; the first landed
    /// redemption persists `registered_until` and every later one skips the reg.
    Unregistered,
}

/// Plan one lane for redemption from its already-loaded state and a resolved
/// registration status. Pure and I/O-free — the caller ([`plan_lanes`]) resolves
/// the status from the persisted `registered_until` watermark alone, with no chain
/// read. Returns `Ok(None)` when the lane is not this node's, has no signed
/// voucher, has nothing unredeemed, or — a node-durability fault that should not
/// occur — is an unregistered signer whose lane has lost its `owner_sig`.
fn plan_lane(
    st: &LaneState,
    paid: &PaidWatermarks,
    self_address: Address,
    status: &RegistrationStatus,
) -> Result<Option<PlannedLane>> {
    if st.provider != self_address {
        return Ok(None);
    }
    let key = st.key();
    // The lane's claim: its latest signature PLUS the frontier the chain has
    // proved on top of it.
    //
    // That sum is a real choice rather than a formality. A lane's worth is
    // `amount + verified_index × chunk_price`, so counting only the signed
    // cumulative would understate it by up to one whole chain — a lane sitting
    // on 200 unredeemed reveals and no fresh signature would look worthless to
    // the redemption floor and never be swept.
    //
    // There is only ever one claim to weigh. A rollover that folded less than
    // the frontier it retires is refused outright (ADR 003 §Rollover), so the
    // lane never holds a retired voucher worth more than its live one.
    //
    // In the cooperative case the strongest claim is always a signed voucher at
    // index 0 — every rollover and every close emits one whose `amount` already
    // folds the chain it retires — so a finalized delivery submits a zero index,
    // a zero preimage, and walks nothing on-chain.
    let Some(claim) = st.live_claim() else {
        return Ok(None);
    };
    let owed = claim.value();
    if owed.is_zero() {
        return Ok(None);
    }
    let unredeemed = owed.saturating_sub(paid.get(&key));
    if unredeemed.is_zero() {
        return Ok(None);
    }

    let register = match status {
        RegistrationStatus::Registered => None,
        RegistrationStatus::Unregistered => {
            let Some(owner_sig) = st.owner_sig else {
                // `owner_sig` is written in the SAME fsynced row as the frontier
                // (#1906), so a crash loses the value and its `owner_sig` together —
                // a lane with redeemable value always carries the material to
                // register its signer. Reaching here means the durable row kept the
                // value but lost the `owner_sig`, a node-side durability fault. The
                // client already sent its capability once; it is not asked to
                // re-send. Skip the lane and surface the invariant break.
                error!(
                    pool_id = %key.pool_id,
                    signer = %key.signer,
                    "BUG: redeemable lane has no owner_sig, so its signer cannot be \
                     registered; skipping (node durability fault, not a client re-send)"
                );
                return Ok(None);
            };
            // Every field of the registration payload comes off the lane record
            // itself — the same durable row as the frontier being redeemed (#1906).
            // On-chain registration is idempotent, so attaching this when the signer
            // is already registered elsewhere is a harmless no-op; the first landed
            // redemption persists `registered_until` and every later one omits it.
            // `cap` recovers the `u64` spending cap the intake path zero-extended
            // into the lane's `U256`.
            Some(PaymentPool::CapabilityReg {
                signer: key.signer,
                spendingCap: to_pool_u64(st.cap, "capability spending cap")?,
                expiry: st.expiry,
                ownerSig: Bytes::from(owner_sig.to_vec()),
            })
        }
    };

    let (r, vs) = compact_voucher_signature(&claim.signature)
        .context("compact the lane's voucher signature")?;
    let voucher = PaymentPool::LaneVoucher {
        signer: key.signer,
        // The claim's SIGNED anchor, not its extended value: the contract
        // rebuilds the EIP-712 digest from these, then re-derives the extension
        // itself from the chain fields below. Submitting the extended value here
        // would recover the wrong signer.
        cumulative: to_pool_u64(claim.amount, "voucher cumulative")?,
        bytesDelivered: to_pool_u64(claim.bytes_delivered, "voucher bytes delivered")?,
        r,
        vs,
        chainRoot: claim.chain.chain_root,
        // The deepest preimage this node has verified. At index 0 the root is
        // its own preimage, so a settlement voucher needs no chain state at all.
        preimage: claim.chain.tip,
        chainMeter: claim
            .chain_meter()
            .context("pack the lane's chainMeter word")?,
    };
    Ok(Some(PlannedLane {
        pool_id: key.pool_id,
        key,
        unredeemed,
        voucher,
        register,
    }))
}

/// Plan a set of candidate lanes for redemption. The only on-chain-derived gate
/// is pool solvency (the event-fed [`PoolProjection`], read below); a lane's
/// signer-registration status is resolved locally from its persisted
/// `registered_until` watermark — no `getAuthorization` read. A lane not yet
/// confirmed registered attaches a `CapabilityReg` from its held `owner_sig`
/// ([`plan_lane`]); on-chain registration is idempotent, so this is safe even for
/// a signer another provider already registered, and the first landed redemption
/// persists `registered_until` so later passes skip the reg.
#[allow(clippy::cognitive_complexity, clippy::too_many_arguments)]
fn plan_lanes(
    paid: &PaidWatermarks,
    self_address: Address,
    states: Vec<LaneState>,
    metrics: &Arc<Metrics>,
    pool_view: &PoolProjection,
) -> Vec<PlannedLane> {
    let now = unix_now();
    // The redeemer's only on-chain-derived gate: hold/drop lanes whose pool cannot
    // pay, read from the event-fed projection. Fail open on an unknown pool. A
    // lane's signer-registration status is resolved locally below — no chain read.
    let mut snapshot: HashMap<PoolId, Option<PoolStatus>> = HashMap::new();
    for st in &states {
        snapshot
            .entry(st.pool_id)
            .or_insert_with(|| pool_view.snapshot(st.pool_id));
    }
    let (states, skipped) = partition_redeemable(states, &snapshot, now);
    if skipped > 0 {
        metrics.redemption_skipped_insolvent_by(skipped as u64);
    }
    let mut plans: Vec<PlannedLane> = Vec::new();
    for st in &states {
        if st.provider != self_address {
            continue;
        }
        // Registered iff this node has already landed the signer's registration
        // (persisted `registered_until` still live); otherwise attach a
        // `CapabilityReg` from the held `owner_sig` ([`plan_lane`]).
        let reg_status = if is_registered(st.registered_until, now) {
            RegistrationStatus::Registered
        } else {
            RegistrationStatus::Unregistered
        };
        match plan_lane(st, paid, self_address, &reg_status) {
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
    paid: &PaidWatermarks,
    self_address: Address,
    floor: U256,
    max_vouchers: usize,
    key: LaneKey,
    metrics: &Arc<Metrics>,
    pool_view: &PoolProjection,
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
    let plans = plan_lanes(paid, self_address, vec![st], metrics, pool_view);
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
    paid: &PaidWatermarks,
    self_address: Address,
    floor: U256,
    max_vouchers: usize,
    strict_flush: bool,
    metrics: &Arc<Metrics>,
    pool_view: &PoolProjection,
) {
    let states = match store.load_all() {
        Ok(s) => s,
        Err(err) => {
            warn!(%err, "redeemer self-tick: failed to load lane state");
            return;
        }
    };
    let plans = plan_lanes(paid, self_address, states, metrics, pool_view);
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
/// runtime worker). One flush lands the lane frontier AND the buffered
/// capability rows, so it is the durability floor for both. Returns `true` on
/// success; a failure is metered and logged.
///
/// `strict` says what the caller does with a `false`, and is carried here only
/// so the log reports the outcome the caller actually takes: the periodic sweep
/// and hint paths defer their submit, while the forced close/shutdown paths
/// redeem anyway — with the residual that a `CapabilityReg` can go on-chain
/// against material that never reached disk.
async fn flush_store_durable(
    store: &Arc<dyn PoolStateStore>,
    metrics: &Arc<Metrics>,
    strict: bool,
) -> bool {
    let store = Arc::clone(store);
    match tokio::task::spawn_blocking(move || store.flush()).await {
        Ok(Ok(())) => true,
        Ok(Err(err)) => {
            metrics.lane_flush_failure();
            if strict {
                warn!(%err, "pre-redeem lane store flush failed; deferring redeem");
            } else {
                warn!(
                    %err,
                    "pre-redeem lane store flush failed; proceeding on the forced \
                     close/shutdown path with un-flushed lane and capability state"
                );
            }
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
    if !flush_store_durable(store, metrics, strict_flush).await && strict_flush {
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

    fn status(remaining: u64, lifecycle: Lifecycle) -> PoolStatus {
        PoolStatus {
            owner: Address::from([1u8; 20]),
            remaining: U256::from(remaining),
            lifecycle,
        }
    }

    #[test]
    fn unknown_pool_fails_open() {
        assert!(pool_is_redeemable(None, 1_000));
    }

    fn pool(owner: Address, status: PaymentPool::Status, deadline: u64) -> PaymentPool::Pool {
        PaymentPool::Pool {
            owner,
            status,
            disputeDeadline: deadline,
            deposit: 1_000,
            totalRedeemed: 0,
        }
    }

    #[test]
    fn resolved_lifecycle_open_pool_is_servable() {
        let p = pool(Address::from([7u8; 20]), PaymentPool::Status::Open, 0);
        assert_eq!(resolved_lifecycle(&p), Some(Lifecycle::Open));
    }

    #[test]
    fn resolved_lifecycle_closing_pool_carries_its_deadline() {
        let p = pool(
            Address::from([7u8; 20]),
            PaymentPool::Status::Closing,
            1_900_000_000,
        );
        assert_eq!(
            resolved_lifecycle(&p),
            Some(Lifecycle::Closing {
                deadline: 1_900_000_000
            })
        );
    }

    fn stale_instant() -> Instant {
        // An instant older than the negative-cache TTL, for the freshness gate.
        Instant::now()
            .checked_sub(RESOLVE_NEGATIVE_TTL + Duration::from_secs(1))
            .unwrap_or_else(Instant::now)
    }

    #[test]
    fn negative_cache_hit_only_for_a_fresh_entry() {
        let mut cache: HashMap<B256, Instant> = HashMap::new();
        let id = B256::repeat_byte(0x33);
        assert!(!negative_cache_hit(&cache, id), "an absent id is not a hit");
        remember_negative(&mut cache, id);
        assert!(
            negative_cache_hit(&cache, id),
            "a just-recorded id is a hit"
        );
        cache.insert(id, stale_instant());
        assert!(
            !negative_cache_hit(&cache, id),
            "an entry past the TTL is re-checked, not suppressed"
        );
    }

    #[test]
    fn remember_negative_prunes_expired_entries_at_the_cap() {
        let mut cache: HashMap<B256, Instant> = HashMap::new();
        // Fill to the cap with stale entries, then record one more: the insert
        // prunes the expired ones instead of growing past the cap.
        for i in 0..RESOLVE_NEGATIVE_CACHE_MAX {
            let id = B256::from(U256::from(i).to_be_bytes::<32>());
            cache.insert(id, stale_instant());
        }
        assert_eq!(cache.len(), RESOLVE_NEGATIVE_CACHE_MAX);
        remember_negative(&mut cache, B256::repeat_byte(0xff));
        assert_eq!(
            cache.len(),
            1,
            "the cap-prune drops every expired entry, leaving only the fresh insert"
        );
    }

    #[test]
    fn resolved_lifecycle_skips_zero_owner_and_closed() {
        // A nonexistent pool (zero owner) and a reclaimed (Closed) pool both seed
        // nothing — the serve gate stays fail-open None rather than register a lane
        // against funds that cannot be redeemed.
        let no_owner = pool(Address::ZERO, PaymentPool::Status::Open, 0);
        assert_eq!(resolved_lifecycle(&no_owner), None);
        let closed = pool(Address::from([7u8; 20]), PaymentPool::Status::Closed, 0);
        assert_eq!(resolved_lifecycle(&closed), None);
    }

    /// A mocked provider whose `eth_call` queue returns one ABI-encoded `getPool`
    /// result. `Pool` is a static tuple, so its `SolValue` encoding equals the
    /// single-struct return `getPool` decodes.
    fn mocked_getpool_view(
        response: Option<PaymentPool::Pool>,
    ) -> (
        ResolvingPoolView<impl Provider + Clone + 'static>,
        PoolProjection,
        alloy::providers::mock::Asserter,
    ) {
        use alloy::providers::ProviderBuilder;
        use alloy::providers::mock::Asserter;
        use alloy::sol_types::SolValue;

        let asserter = Asserter::new();
        if let Some(pool) = response {
            asserter.push_success(&Bytes::from(pool.abi_encode()));
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let contract = PaymentPool::new(Address::ZERO, provider);
        let projection = PoolProjection::new();
        let view = ResolvingPoolView::new(contract, projection.clone());
        (view, projection, asserter)
    }

    /// Admit path (i): a pool the projection has not observed triggers ONE
    /// `getPool`; a solvent pool is folded into the projection and admitted. A
    /// second request is a projection hit and issues no further `getPool`.
    #[tokio::test]
    async fn resolving_status_does_getpool_on_miss_and_admits_solvent_pool() -> Result<()> {
        use crate::pool_view::PoolView;

        let owner = Address::from([7u8; 20]);
        let (view, projection, asserter) =
            mocked_getpool_view(Some(pool(owner, PaymentPool::Status::Open, 0)));
        let pool_id = B256::repeat_byte(0x44);

        let status = view
            .status(pool_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("a solvent unknown pool must admit via getPool"))?;
        assert_eq!(status.owner, owner);
        // `pool()` seeds deposit 1000, totalRedeemed 0.
        assert_eq!(status.remaining, U256::from(1_000u64));
        assert!(
            projection.snapshot(pool_id).is_some(),
            "the resolved pool is folded into the projection"
        );
        assert_eq!(
            asserter.read_q().len(),
            0,
            "exactly one getPool was consumed"
        );

        // Admit path (iii): the second request hits the projection — no getPool.
        // The queue is empty, so any second eth_call would error and yield None;
        // a Some result therefore proves the projection served it.
        let again = view.status(pool_id).await.ok_or_else(|| {
            anyhow::anyhow!("a confirmed pool stays admitted from the projection")
        })?;
        assert_eq!(again.owner, owner);
        Ok(())
    }

    /// Admit path (ii): an absent (`owner == 0`) or `Closed` pool the `getPool`
    /// returns is refused (`None`) and never folded, and the negative cache then
    /// suppresses a repeat `getPool`.
    #[tokio::test]
    async fn resolving_status_refuses_absent_pool_and_caches_negative() -> Result<()> {
        use crate::pool_view::PoolView;

        let (view, projection, asserter) =
            mocked_getpool_view(Some(pool(Address::ZERO, PaymentPool::Status::Open, 0)));
        let pool_id = B256::repeat_byte(0x55);

        assert!(
            view.status(pool_id).await.is_none(),
            "an absent (zero-owner) pool is refused"
        );
        assert!(
            projection.snapshot(pool_id).is_none(),
            "an absent pool is never folded into the projection"
        );
        assert!(
            view.negative
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&pool_id),
            "the refusal is remembered in the negative cache"
        );
        // The negative cache short-circuits before any getPool — the queue stays
        // empty (only the first response was consumed), so no second call is made.
        assert!(view.status(pool_id).await.is_none());
        assert_eq!(asserter.read_q().len(), 0, "no second getPool was issued");
        Ok(())
    }

    /// Admit path (ii, errored): a `getPool` RPC fault refuses the pool (`None`)
    /// and remembers it in the negative cache, so a re-request flood cannot storm
    /// `getPool`.
    #[tokio::test]
    async fn resolving_status_refuses_on_getpool_error_and_caches_negative() -> Result<()> {
        use crate::pool_view::PoolView;

        // No response queued: the mocked eth_call errors.
        let (view, projection, _asserter) = mocked_getpool_view(None);
        let pool_id = B256::repeat_byte(0x66);

        assert!(
            view.status(pool_id).await.is_none(),
            "a getPool fault refuses the pool"
        );
        assert!(projection.snapshot(pool_id).is_none());
        assert!(
            view.negative
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&pool_id),
            "an errored getPool is remembered in the negative cache"
        );
        Ok(())
    }

    /// `cached_status` never issues a `getPool`: it reads the projection only, so
    /// an unknown pool returns `None` even though `status` would resolve it, and
    /// the mock's response queue is left untouched.
    #[tokio::test]
    async fn resolving_cached_status_never_calls_getpool() -> Result<()> {
        use crate::pool_view::PoolView;

        let owner = Address::from([7u8; 20]);
        let (view, _projection, asserter) =
            mocked_getpool_view(Some(pool(owner, PaymentPool::Status::Open, 0)));
        let pool_id = B256::repeat_byte(0x77);

        assert!(
            view.cached_status(pool_id).await.is_none(),
            "cached_status does not resolve an unknown pool on-chain"
        );
        assert_eq!(
            asserter.read_q().len(),
            1,
            "the queued getPool response is untouched by cached_status"
        );
        Ok(())
    }

    fn authz(cap: u64, spent: u64) -> PaymentPool::Authorization {
        PaymentPool::Authorization {
            cap,
            expiry: 0,
            spent,
        }
    }

    /// A mocked provider whose `eth_call` queue returns one ABI-encoded
    /// `getAuthorization` result per entry. `Authorization` is an all-static
    /// uint64 tuple, so its `SolValue` encoding equals the single-struct return
    /// `getAuthorization` decodes.
    fn mocked_getauth_view(
        responses: &[PaymentPool::Authorization],
    ) -> (
        ResolvingPoolView<impl Provider + Clone + 'static>,
        alloy::providers::mock::Asserter,
    ) {
        use alloy::providers::ProviderBuilder;
        use alloy::providers::mock::Asserter;
        use alloy::sol_types::SolValue;

        let asserter = Asserter::new();
        for auth in responses {
            asserter.push_success(&Bytes::from(auth.abi_encode()));
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let contract = PaymentPool::new(Address::ZERO, provider);
        let view = ResolvingPoolView::new(contract, PoolProjection::new());
        (view, asserter)
    }

    /// A registered signer with headroom reports `cap − spent` in micro-USDC.
    #[tokio::test]
    async fn signer_headroom_reports_cap_minus_spent() -> Result<()> {
        use crate::pool_view::PoolView;

        let (view, _asserter) = mocked_getauth_view(&[authz(1_000, 300)]);
        let headroom = view
            .signer_cap_headroom_micro(B256::repeat_byte(0x11), Address::from([2u8; 20]))
            .await
            .ok_or_else(|| anyhow::anyhow!("a registered signer reports headroom"))?;
        assert_eq!(headroom, 700, "cap 1000 − spent 300");
        Ok(())
    }

    /// A signer that has spent its full `cap` reports zero headroom — the floor
    /// gate in the dispatch path then refuses it, but the view reports the truth.
    #[tokio::test]
    async fn exhausted_signer_reports_zero_headroom() -> Result<()> {
        use crate::pool_view::PoolView;

        let (view, _asserter) = mocked_getauth_view(&[authz(1_000, 1_000)]);
        let headroom = view
            .signer_cap_headroom_micro(B256::repeat_byte(0x22), Address::from([3u8; 20]))
            .await
            .ok_or_else(|| anyhow::anyhow!("an exhausted signer still reports a value"))?;
        assert_eq!(headroom, 0, "spent == cap");
        Ok(())
    }

    /// An unregistered signer (`cap == 0`) reports `u64::MAX` — it has spent
    /// nothing on-chain and admits on its off-chain capability — and a second call
    /// within the TTL is served from cache with no further `getAuthorization`.
    #[tokio::test]
    async fn unregistered_signer_is_unconstrained_and_cached() -> Result<()> {
        use crate::pool_view::PoolView;

        // Only ONE response queued: a second on-chain read would error → None.
        let (view, asserter) = mocked_getauth_view(&[authz(0, 0)]);
        let pool_id = B256::repeat_byte(0x33);
        let signer = Address::from([4u8; 20]);

        let first = view
            .signer_cap_headroom_micro(pool_id, signer)
            .await
            .ok_or_else(|| anyhow::anyhow!("an unregistered signer is unconstrained"))?;
        assert_eq!(first, u64::MAX, "cap == 0 → no on-chain constraint");
        assert_eq!(
            asserter.read_q().len(),
            0,
            "exactly one getAuthorization was consumed"
        );

        let second = view
            .signer_cap_headroom_micro(pool_id, signer)
            .await
            .ok_or_else(|| anyhow::anyhow!("a fresh cache entry serves the second call"))?;
        assert_eq!(
            second,
            u64::MAX,
            "the cached headroom is returned unchanged"
        );
        assert_eq!(
            asserter.read_q().len(),
            0,
            "no second getAuthorization was issued within the TTL"
        );
        Ok(())
    }

    /// A REGISTERED signer with a zero `spendingCap` (`cap == 0` but `expiry != 0`)
    /// is NOT the all-zero unregistered struct: its headroom is `0`, so it reports
    /// `Some(0)` and the dispatch gate refuses it — it is not misread as
    /// unconstrained (`u64::MAX`), which would fail open and admit an uncashable
    /// signer.
    #[tokio::test]
    async fn registered_zero_cap_signer_is_refused_not_unconstrained() -> Result<()> {
        use crate::pool_view::PoolView;

        let auth = PaymentPool::Authorization {
            cap: 0,
            expiry: 1_900_000_000,
            spent: 0,
        };
        let (view, _asserter) = mocked_getauth_view(&[auth]);
        let headroom = view
            .signer_cap_headroom_micro(B256::repeat_byte(0x44), Address::from([5u8; 20]))
            .await
            .ok_or_else(|| anyhow::anyhow!("a registered zero-cap signer reports a value"))?;
        assert_eq!(
            headroom, 0,
            "cap == 0 with expiry != 0 is a registered zero-cap signer, not unregistered"
        );
        Ok(())
    }

    /// A `getAuthorization` RPC fault refuses the signer (`None`) so the caller
    /// does not fail open.
    #[tokio::test]
    async fn getauthorization_fault_refuses_signer() -> Result<()> {
        use crate::pool_view::PoolView;

        // No response queued: the mocked eth_call errors.
        let (view, _asserter) = mocked_getauth_view(&[]);
        assert!(
            view.signer_cap_headroom_micro(B256::repeat_byte(0x44), Address::from([5u8; 20]))
                .await
                .is_none(),
            "a getAuthorization fault refuses the signer"
        );
        Ok(())
    }

    /// A second call within `SIGNER_AUTH_TTL` is served from cache, consuming no
    /// further `getAuthorization` (proven by the single queued response and a
    /// `Some` result on the second call).
    #[tokio::test]
    async fn signer_headroom_second_call_hits_cache() -> Result<()> {
        use crate::pool_view::PoolView;

        let (view, asserter) = mocked_getauth_view(&[authz(1_000, 200)]);
        let pool_id = B256::repeat_byte(0x55);
        let signer = Address::from([6u8; 20]);

        let first = view
            .signer_cap_headroom_micro(pool_id, signer)
            .await
            .ok_or_else(|| anyhow::anyhow!("the first call resolves on-chain"))?;
        assert_eq!(first, 800);
        assert_eq!(asserter.read_q().len(), 0, "one getAuthorization consumed");

        let second = view
            .signer_cap_headroom_micro(pool_id, signer)
            .await
            .ok_or_else(|| anyhow::anyhow!("the second call is served from cache"))?;
        assert_eq!(second, 800, "the cached headroom is returned");
        assert_eq!(
            asserter.read_q().len(),
            0,
            "no second getAuthorization within the TTL"
        );
        Ok(())
    }

    #[test]
    fn open_funded_is_redeemable() {
        assert!(pool_is_redeemable(
            Some(status(500, Lifecycle::Open)),
            1_000
        ));
    }

    #[test]
    fn open_drained_is_held() {
        assert!(!pool_is_redeemable(Some(status(0, Lifecycle::Open)), 1_000));
    }

    #[test]
    fn closing_drained_is_dropped() {
        assert!(!pool_is_redeemable(
            Some(status(0, Lifecycle::Closing { deadline: 2_000 })),
            1_000
        ));
    }

    #[test]
    fn closing_funded_before_deadline_is_redeemable() {
        assert!(pool_is_redeemable(
            Some(status(500, Lifecycle::Closing { deadline: 2_000 })),
            1_999
        ));
    }

    #[test]
    fn closing_funded_at_or_after_deadline_is_dropped() {
        assert!(!pool_is_redeemable(
            Some(status(500, Lifecycle::Closing { deadline: 2_000 })),
            2_000
        ));
        assert!(!pool_is_redeemable(
            Some(status(500, Lifecycle::Closing { deadline: 2_000 })),
            2_500
        ));
    }

    #[test]
    fn partition_keeps_redeemable_and_counts_skips() {
        // pool 1: open+funded (keep), pool 2: open+drained (skip), pool 3: unknown (keep, fail open)
        let s1 = signed_lane_state(1, 10, 20, None);
        let s2 = signed_lane_state(2, 11, 20, None);
        let s3 = signed_lane_state(3, 12, 20, None);
        let mut snap: HashMap<PoolId, Option<PoolStatus>> = HashMap::new();
        snap.insert(s1.pool_id, Some(status(500, Lifecycle::Open)));
        snap.insert(s2.pool_id, Some(status(0, Lifecycle::Open)));
        // s3's pool intentionally absent from snap -> None -> fail open
        let (kept, skipped) = partition_redeemable(vec![s1.clone(), s2, s3.clone()], &snap, 1_000);
        let kept_pools: Vec<_> = kept.iter().map(|st| st.pool_id).collect();
        assert_eq!(skipped, 1);
        assert!(kept_pools.contains(&s1.pool_id));
        assert!(kept_pools.contains(&s3.pool_id));
        assert_eq!(kept.len(), 2);
    }

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
                // A sealed settlement voucher: the cooperative shape, which
                // walks nothing on-chain and compresses to almost no calldata.
                chainRoot: B256::ZERO,
                preimage: B256::ZERO,
                chainMeter: U256::ZERO,
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
    /// events the paid-watermark cache and pool projection need — no more, no
    /// fewer. `PoolOpened` in particular must never be dropped: it is the
    /// projection's only signal that a pool exists. `PoolCloseInitiated` folds a
    /// pool's `Closing` deadline into the projection so the redeemer's solvency
    /// gate drops a drained or past-deadline lane instead of submitting a
    /// `redeemMany` that reverts `PoolClosed`; the node still runs no force-redeem
    /// on close.
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

    /// A lane with a signed voucher and non-zero owed amount, ready for
    /// `plan_lane` tests. `owner_sig` is the lane's own registration material —
    /// `Some` for a lane whose intake verified an owner grant, `None` for one
    /// that never captured one.
    fn signed_lane_state(
        pool: u8,
        signer: u8,
        provider: u8,
        owner_sig: Option<[u8; 65]>,
    ) -> LaneState {
        let mut st = LaneState::hydrate(
            PoolId::from([pool; 32]),
            Address::from([signer; 20]),
            Address::from([provider; 20]),
            U256::from(10_000_000u64),
            1_800_000_000,
            U256::from(1_000u64),
            U256::from(1_048_576u64),
            Some(sig_with_v(0)),
            decdn_incentive::LaneChain::NONE,
        );
        st.owner_sig = owner_sig;
        st
    }

    #[test]
    fn plan_lane_registered_omits_capability_reg() -> Result<()> {
        let st = signed_lane_state(1, 10, 20, None);
        let paid = PaidWatermarks::default();
        let plan = plan_lane(
            &st,
            &paid,
            Address::from([20u8; 20]),
            &RegistrationStatus::Registered,
        )?
        .ok_or_else(|| anyhow::anyhow!("registered lane with owed balance should plan"))?;
        assert!(
            plan.register.is_none(),
            "a lane this node has registered attaches no CapabilityReg"
        );
        assert_eq!(plan.key, st.key());
        Ok(())
    }

    #[test]
    fn plan_lane_unregistered_with_material_attaches_registration() -> Result<()> {
        let st = signed_lane_state(1, 11, 21, Some(sig_with_v(1)));
        let paid = PaidWatermarks::default();
        let plan = plan_lane(
            &st,
            &paid,
            Address::from([21u8; 20]),
            &RegistrationStatus::Unregistered,
        )?
        .ok_or_else(|| anyhow::anyhow!("unregistered lane with held material should plan"))?;
        let reg = plan.register.ok_or_else(|| {
            anyhow::anyhow!("an unregistered signer with owner_sig attaches a CapabilityReg")
        })?;
        assert_eq!(reg.signer, st.signer, "reg names the lane's signer");
        assert_eq!(reg.expiry, st.expiry, "reg carries the lane's expiry");
        assert_eq!(
            reg.ownerSig.as_ref(),
            sig_with_v(1).as_slice(),
            "reg carries the lane's own owner signature"
        );
        assert_eq!(plan.key, st.key());
        Ok(())
    }

    #[test]
    fn plan_lane_unregistered_without_material_is_skipped() -> Result<()> {
        let st = signed_lane_state(1, 12, 22, None);
        let paid = PaidWatermarks::default();
        let plan = plan_lane(
            &st,
            &paid,
            Address::from([22u8; 20]),
            &RegistrationStatus::Unregistered,
        )?;
        assert!(
            plan.is_none(),
            "an unregistered lane with no owner_sig is a durability fault and is skipped"
        );
        Ok(())
    }
}
