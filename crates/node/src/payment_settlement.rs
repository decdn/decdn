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
//!   cumulative to `newPaidCumulative` and mirrors it onto the durable
//!   `LaneState::paid_cumulative` row. `PoolToppedUp` re-drives a pool's lanes
//!   (a dry pool may have left `owed > paid`), and `PoolReclaimed` forgets the
//!   pool's lanes. The watcher reconciles like every other chain watcher —
//!   enumerate `PoolRedeemed` from a pinned block, then tail live, resyncing on a
//!   missed range — so paid is rebuilt from the event log, never guessed. That
//!   forward rebuild starts at the poller's persisted cursor, so bootstrap first
//!   rehydrates the paid cache from the durable lane rows: a restart carries
//!   forward every redemption behind the cursor instead of re-submitting those
//!   lanes for silent on-chain no-ops (#2052). The same
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
//!   The same sweep also runs at each serve cutoff: one second after the voucher
//!   and preimage paths start to refuse a lane whose capability expiry is near.
//!   The lane's claim is final there, and the sweep has about one redeem interval
//!   to start the redemption and the landing slack to land it before the
//!   contract stops paying the capability. On a restart, a lane whose cutoff
//!   passed while the node was down is swept at once. A value that stays below
//!   the floor is not redeemed by the cutoff sweep; a later sweep before the
//!   landing slack can still pack it with other lanes.
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
//!   lane, or one whose deadline falls within the landing slack, instead of
//!   submitting a `redeemMany` that reverts `PoolClosed`.
//!
//! Buyer-side `openPool`/`topUp`/`reclaim` (node→node cache-miss pulls) is out of
//! scope here. The paid-watermark watcher is a [`Route`] the runtime registers on
//! the shared multiplexed poller (which owns the loop, cursor, and shutdown); the
//! service itself owns only the redeemer [`JoinHandle`], aborted on shutdown or
//! drop.

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use alloy::primitives::{Address, B256, Bytes, Signature, TxHash, U256};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use decdn_common::config::{REDEEM_LANDING_SLACK_SECS, capability_expiry_margin_secs};
use decdn_common::redact::{sanitize_err_chain, sanitize_error_sources};
use decdn_incentive::payment_pool::{PaymentPool, to_pool_u64};
use decdn_incentive::sig_canon::is_high_s;
use decdn_incentive::{
    CheckpointKey, KeyedCheckpointStore, LaneKey, LaneState, PoolId, PoolStateStore, StoreError,
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{Instrument as _, debug, error, info, warn};

use crate::chain_events::boot_retry::BootRetry;
use crate::chain_events::multiplexed_poller::{Route, SinkSource};
use crate::chain_events::resumable_watcher::{Checkpoint, ColdStart, CursorStart, LogSink};
use crate::chain_events::{REORG_MARGIN_BLOCKS, timed};
use crate::handlers::client::ClientHandler;
use crate::metrics::{Metrics, metric_hook};
use crate::onchain_tx::{TxOutcome, send_and_await_receipt};
use crate::pool_view::{Lifecycle, PoolProjection, PoolStatus};
use decdn_incentive::payment_pool::SignerAuthorization;

/// Capacity of the redeem-hint channel. Hints are advisory (a missed hint only
/// delays a redemption until the next voucher or self-tick sweep), so a bounded
/// channel that drops on overflow is acceptable — sized for a burst of concurrent
/// lanes without backpressuring the voucher-accept path.
pub const REDEEM_HINT_CAPACITY: usize = 256;

/// How long the admit path suppresses a repeat `getPool` for a pool it just
/// found not-servable (nonexistent / `Closed`). A not-servable pool never folds
/// into the projection, so its `snapshot` stays `None`; without this a client
/// re-sending its capability on every request would drive one `getPool` per
/// request against the same dead pool. On expiry the pool is re-checked once
/// — the window is short enough that a pool opened after a negative result still
/// becomes servable within it, long enough to collapse a request flood to ~one
/// call per pool per window.
const RESOLVE_VERDICT_TTL: Duration = Duration::from_mins(1);

/// How long the admit path suppresses a repeat `getPool` for a pool whose read
/// faulted (an RPC error or a `timed` timeout). A fault says nothing about the
/// pool, so the window is far shorter than [`RESOLVE_VERDICT_TTL`]: a live pool
/// is admitted again a few seconds after the RPC recovers. It is not zero: in an
/// outage a request inside the window refuses at once instead of waiting out a
/// read of its own. The window starts when the read ends; while a read is in
/// flight, other requests for that pool wait on it (see [`ReadSlot`]), so an
/// outage costs one read per pool at a time.
const RESOLVE_FAULT_TTL: Duration = Duration::from_secs(5);

/// Why a pool sits in the admit-path negative cache. The reason picks the
/// suppression window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NegativeReason {
    /// `getPool` answered: the pool is absent or `Closed`.
    Verdict,
    /// `getPool` failed or timed out; nothing is known about the pool.
    Fault,
}

impl NegativeReason {
    /// How long an entry with this reason suppresses a repeat `getPool`.
    const fn ttl(self) -> Duration {
        match self {
            Self::Verdict => RESOLVE_VERDICT_TTL,
            Self::Fault => RESOLVE_FAULT_TTL,
        }
    }
}

/// Cap on the admit-path negative cache, bounding its memory against a flood of
/// distinct nonexistent pool ids. At the cap an insert first prunes lapsed
/// entries; if the cache is still full it skips the insert, so a flood of
/// distinct in-window negatives past the cap pays one `getPool` per admit rather
/// than growing the cache without limit.
const RESOLVE_NEGATIVE_CACHE_MAX: usize = 4096;

/// How long an admit-path `Unregistered` read stays fresh in the signer-auth
/// cache. Any provider's next redemption registers the signer, and a
/// registration that pays no lane emits no event the projection can fold, so
/// only a re-read sees it. Inside the window the node admits the signer on its
/// presented capability; the on-chain `redeemMany` pays `min(desired, cap −
/// spent)` and never over-cashes. A `Registered` read has no window: see
/// [`AuthRead`].
const UNREGISTERED_AUTH_TTL: Duration = Duration::from_mins(1);

/// Cap on the admit-path signer-auth cache, bounding its memory against a flood of
/// distinct `(pool, signer)` pairs. At the cap an insert first prunes lapsed
/// `Unregistered` reads, then evicts the oldest read, so the map never grows
/// without bound and an evicted signer pays one `getAuthorization` on its next
/// admit.
const AUTH_CACHE_MAX: usize = 4096;

/// One admit-path `getAuthorization` read held in the signer-auth cache.
///
/// A `Registered` read never goes stale. Its `cap` and `expiry` are write-once,
/// and its `spent` moves only by redemptions, which the projection folds from
/// every provider's `PoolRedeemed`. The live `spent` is the read's `spent` plus
/// what the projection folded for the signer since the read. The baseline is
/// taken before the read is sent, so a redemption folded while the read is in
/// flight may count twice: the error makes `spent` high, which refuses near the
/// cap rather than over-admits.
#[derive(Clone, Copy, Debug)]
struct AuthRead {
    /// What the chain answered.
    auth: SignerAuthorization,
    /// The projection's folded `spent` for the signer when the read was sent.
    folded_at_read: u64,
    /// When the read landed.
    at: Instant,
}

/// What the signer-auth cache holds for one `(pool, signer)` at an admit.
#[derive(Debug, PartialEq, Eq)]
enum CachedAuth {
    /// A usable answer, with a registered `spent` brought up to the fold.
    Fresh(SignerAuthorization),
    /// An `Unregistered` read past [`UNREGISTERED_AUTH_TTL`], or one the
    /// projection has since seen the signer redeem: the signer may be
    /// registered now.
    Lapsed,
    /// No read is held for the pair.
    Absent,
}

/// Classify a held read against the projection's current fold for the signer.
/// Pure, so the freshness rules are unit-testable without a provider.
fn cached_auth(read: Option<&AuthRead>, folded_now: u64) -> CachedAuth {
    let Some(read) = read else {
        return CachedAuth::Absent;
    };
    let folded_since = folded_now.saturating_sub(read.folded_at_read);
    match read.auth {
        SignerAuthorization::Registered { cap, expiry, spent } => {
            CachedAuth::Fresh(SignerAuthorization::Registered {
                cap,
                expiry,
                spent: spent.saturating_add(folded_since),
            })
        }
        SignerAuthorization::Unregistered
            if folded_since == 0 && read.at.elapsed() < UNREGISTERED_AUTH_TTL =>
        {
            CachedAuth::Fresh(SignerAuthorization::Unregistered)
        }
        SignerAuthorization::Unregistered => CachedAuth::Lapsed,
    }
}

/// Store `read` for `key`. At [`AUTH_CACHE_MAX`], prune lapsed `Unregistered`
/// reads first; if the cache is still full, evict the oldest read. Pure, so the
/// bound is unit-testable.
fn remember_auth(
    cache: &mut HashMap<(B256, Address), AuthRead>,
    key: (B256, Address),
    read: AuthRead,
) {
    if cache.len() >= AUTH_CACHE_MAX && !cache.contains_key(&key) {
        cache.retain(|_, r| {
            matches!(r.auth, SignerAuthorization::Registered { .. })
                || r.at.elapsed() < UNREGISTERED_AUTH_TTL
        });
        if cache.len() >= AUTH_CACHE_MAX
            && let Some(oldest) = cache.iter().min_by_key(|(_, r)| r.at).map(|(k, _)| *k)
        {
            cache.remove(&oldest);
        }
    }
    cache.insert(key, read);
}

/// Bounded receipt wait for a redemption transaction. A stuck/dropped/replaced tx
/// must not wedge a background tick. A lapse first fetches the receipt by hash,
/// which adds at most 37 s (`RESOLVE_*` in `onchain_tx`); when that finds nothing
/// it yields [`TxOutcome::Timeout`], a non-fatal outcome (the tx may still mine
/// later; the claim is at worst deferred, never double-spent, because the
/// on-chain lane watermark is monotone).
const REDEEM_RECEIPT_TIMEOUT: Duration = Duration::from_mins(3);

/// Bound on the reactive halve-and-retry when a chunk fails to send oversized.
/// A default-sized chunk (300) sits far under the block-gas ceiling, so a real
/// oversize needs at most one or two halvings; this caps the worst-case fan-out
/// (a transient error mis-flagged as oversize) at 2^6 doomed sub-sends.
const MAX_SPLIT_DEPTH: u32 = 6;

/// Lanes per pre-redeem `getWatermarks` read ([`reconcile_onchain_watermarks`]).
/// [`plan_lanes`] plans every persisted lane owed more than it is paid, so the
/// count is bounded by nothing the redeemer controls — the per-transaction
/// voucher cap chunks the *submit*, after this read. 512 triples is ~49 KB of
/// calldata and ~32 KB of return, and under 3M gas — `test_getWatermarks_fullSizeBatch`
/// measures a batch this size and the gas snapshot tracks it, so a change that
/// makes the read too expensive shows up as a snapshot diff. That is well inside
/// the `eth_call` ceilings providers commonly set. An endpoint that rejects a
/// batch anyway surfaces as `redemption_reconcile_failures`, not as a wrong
/// answer. A plain const, not a config knob: no operator knowledge makes a
/// better choice here, and reusing the per-tx voucher cap would refragment the
/// read whenever an operator lowered it.
const WATERMARK_READ_BATCH_MAX: usize = 512;

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

/// In-memory index of every lane's paid-cumulative watermark, keyed by
/// [`LaneKey`], read by the redeemer to compute `unredeemed = owed − paid`
/// (ADR 003 § Tracking owed vs. paid). Written by the `PoolRedeemed` watcher,
/// which mirrors every update onto the durable `LaneState::paid_cumulative` row
/// in the same breath. The durable field is the source of truth; this cache is
/// rehydrated from it at bootstrap ([`PoolSettlementService::bootstrap`]) so a
/// restart carries forward redemptions that landed before the log-poller's
/// persisted cursor, instead of re-submitting those lanes for silent on-chain
/// no-ops (#2052).
///
/// A `std::sync::Mutex`: the guard is only ever held to read/insert a single
/// entry, never across an `.await`.
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

    /// A lane's paid cumulative, or `U256::ZERO` if none is cached — no
    /// `PoolRedeemed` has landed for it this run and the bootstrap rehydration
    /// found no durable watermark (a never-redeemed lane). `ZERO` is the safe
    /// over-estimate of `unredeemed`: the on-chain redeem caps the increment and
    /// the event then corrects both the cache and the durable row.
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

/// Startup self-check: a cheap immutable view confirms the configured address
/// actually hosts the `PaymentPool` contract. Transient errors retry on
/// `boot`'s budget; no contract at the address decodes as `ZeroData`, which is
/// permanent and fails at once.
async fn usdc_self_check<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    boot: &BootRetry,
) -> Result<Address> {
    let payment_pool_addr = *contract.address();
    boot.run("PaymentPool.usdc() self-check", || async {
        timed(None, "PaymentPool.usdc()", contract.usdc().call())
            .await
            .with_context(|| format!("PaymentPool.usdc() self-check at {payment_pool_addr}"))
    })
    .await
}

impl<P: Provider + Clone + 'static> PoolSettlementService<P> {
    /// Bootstrap the service: self-check the contract, spawn the redemption
    /// task, and return the service alongside the paid-watermark [`Route`] the
    /// runtime registers on the shared multiplexed poller.
    ///
    /// # Errors
    ///
    /// Returns an error if the `usdc()` self-check fails deterministically — a
    /// bad `payment_pool_address` is fatal at bring-up — or if `boot`'s budget
    /// runs out on transient RPC failures.
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
        boot: &BootRetry,
        pool_view: PoolProjection,
        redeem_tx: mpsc::Sender<LaneKey>,
        redeem_rx: mpsc::Receiver<LaneKey>,
    ) -> Result<(Self, Route)> {
        let contract = PaymentPool::new(payment_pool_addr, provider);

        let usdc_token = usdc_self_check(&contract, boot).await?;
        info!(
            %payment_pool_addr,
            %usdc_token,
            %self_address,
            "PaymentPool settlement service bootstrap complete"
        );

        // Rehydrate the cache from the durable lane records BEFORE the redeemer
        // starts, so a restart does not forget redemptions that landed before the
        // log-poller's persisted cursor (#2052).
        let paid = rehydrate_paid_watermarks(store.as_ref(), self_address);

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
            // this checkpoint on shutdown (via `CursorStart::flush`).
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
            capability_expiry_margin_secs(redeem_interval.as_secs()),
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
        // risk. It still applies the configured per-chunk floor, so sub-floor
        // dust stays unredeemed across the stop.
        let Some(states) = load_lanes(self.store.as_ref()) else {
            return;
        };
        // No later sweep follows and no lane is parked, so the chain-fault flag
        // has no use here.
        let _chain_fault = redeem_sweep(
            &self.contract,
            &self.store,
            states,
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
                        warn!(error = %err, "skipping undecodable PoolOpened log");
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
                        warn!(error = %err, "skipping undecodable PoolRedeemed log");
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
                    let paid_cumulative = U256::from(lane.newPaidCumulative);
                    self.paid.set(key, paid_cumulative);
                    // Mirror the watermark onto the durable lane record so it
                    // survives a restart (#2052). Buffered like `record`; the
                    // periodic lane flush fsyncs it. A no-op for a lane already
                    // forgotten, and the in-memory cache above stays authoritative
                    // for this run even if the durable write is refused, so a
                    // failure only lags durability — log it rather than fail the
                    // poller apply (whose failure policy is in-memory + always Ok).
                    if let Err(err) = self.store.set_paid_cumulative(key, paid_cumulative) {
                        self.metrics.watcher_persist_failure();
                        warn!(
                            error = %err,
                            pool_id = %event.poolId,
                            signer = %lane.signer,
                            "failed to persist lane paid watermark; in-memory cache updated, \
                             durable value lags until the next redemption"
                        );
                    }
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
                        warn!(error = %err, "skipping undecodable PoolToppedUp log");
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
                        warn!(error = %err, "skipping undecodable PoolCloseInitiated log");
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
                if self.holds_unredeemed_on(event.poolId) {
                    self.metrics.pool_grace_close();
                }
            }
            Some(sig) if sig == PaymentPool::PoolReclaimed::SIGNATURE_HASH => {
                let event = match PaymentPool::PoolReclaimed::decode_log_data(&log.inner.data) {
                    Ok(event) => event,
                    Err(err) => {
                        warn!(error = %err, "skipping undecodable PoolReclaimed log");
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
    /// Whether this node provides a lane of `pool_id` that is owed more than the
    /// chain has paid it — revenue that must redeem inside the grace window. A
    /// store read failure answers `false`: the counter this feeds is advisory.
    fn holds_unredeemed_on(&self, pool_id: PoolId) -> bool {
        match self.store.load_all() {
            Ok(states) => holds_unredeemed(&states, pool_id, self.self_address),
            Err(err) => {
                warn!(error = %err, %pool_id, "grace-close check: failed to load lane state");
                false
            }
        }
    }

    /// Hint the redeemer for every lane of `pool_id` this node provides, so a
    /// top-up re-drives lanes a dry pool left `owed > paid`. Best-effort: a full
    /// hint channel drops the nudge (the self-tick sweep is the backstop).
    fn redrive_pool_lanes(&self, pool_id: PoolId) {
        let states = match self.store.load_all() {
            Ok(s) => s,
            Err(err) => {
                warn!(error = %err, %pool_id, "top-up re-drive: failed to load lane state");
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
                warn!(error = %err, %pool_id, "reclaim cleanup: failed to load lane state");
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
                warn!(error = %err, pool_id = %pool_id, signer = %key.signer, "failed to forget reclaimed lane");
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
/// servable pool into the projection, and refuses an absent, closed, or faulted
/// pool. Every admit-path chain read (`getPool` and the signer's
/// `getAuthorization`) runs under `chain_events::timed`'s default bound, so a
/// hung read times out and takes the fault path instead of stalling admission.
/// The client re-sends its capability on its next request (the documented
/// lane recovery path), and by then the folded owner registers the lane.
///
/// The mid-stream re-check calls [`cached_status`](crate::pool_view::PoolView::cached_status), which reads the
/// projection ONLY and never blocks on a `getPool` — a chain read at a voucher
/// boundary would stall delivery.
///
/// A short-TTL negative cache suppresses a repeat `getPool` for a pool just
/// found not-servable (`RESOLVE_VERDICT_TTL`) or whose read faulted (the far
/// shorter `RESOLVE_FAULT_TTL`), so a client re-requesting the same dead pool
/// every request cannot drive one `getPool` per request, and one failed read does
/// not refuse a live pool for the full verdict window. Reads are coalesced per
/// pool: a request that finds a `getPool` in flight for its pool waits for it
/// and takes its answer.
///
/// The signer confirm reads `getAuthorization` once per `(pool, signer)` and
/// keeps a `Registered` answer for good (see `AuthRead`); only an
/// `Unregistered` answer is re-read. Those reads are coalesced per pair the same
/// way.
pub struct ResolvingPoolView<P: Provider + Clone> {
    /// The wallet/RPC-backed `PaymentPool` binding for the admit-path reads
    /// (`getPool`, `getAuthorization`).
    contract: PaymentPool::PaymentPoolInstance<P>,
    /// The event-fed projection this view reads first and folds a resolved pool
    /// into. Shared with the settlement watcher's sink (the authoritative writer).
    projection: PoolProjection,
    /// Pool id → (why, when) for pools recently found not-servable or whose read
    /// faulted. The reason picks the entry's window ([`NegativeReason::ttl`]).
    /// The guard is held only to read/insert one entry, never across the
    /// `getPool` await.
    negative: Mutex<HashMap<B256, (NegativeReason, Instant)>>,
    /// Pool id → a receiver for the in-flight admit `getPool` of that pool,
    /// which carries the read's answer. See [`ReadSlot`]. The guard is held only to
    /// read/insert one entry, never across the `getPool` await.
    inflight: InflightReads<B256, Option<PoolStatus>>,
    /// `(pool_id, signer)` → the last `getAuthorization` read of that pair, for
    /// the admit-path signer confirm. [`cached_auth`] decides whether a read
    /// still answers. The guard is held only to read/insert one entry, never
    /// across the `getAuthorization` await.
    auth_cache: Mutex<HashMap<(B256, Address), AuthRead>>,
    /// `(pool_id, signer)` → a receiver for the in-flight admit
    /// `getAuthorization` of that pair. See [`ReadSlot`].
    auth_inflight: InflightReads<(B256, Address), Option<SignerAuthorization>>,
    /// Counts each signer confirm by what the cache held for it.
    metrics: Arc<Metrics>,
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
    /// Wrap the event-fed `projection` with admit-path `getPool` and
    /// `getAuthorization` fallbacks against `contract`.
    #[must_use]
    pub fn new(
        contract: PaymentPool::PaymentPoolInstance<P>,
        projection: PoolProjection,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            contract,
            projection,
            negative: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
            auth_cache: Mutex::new(HashMap::new()),
            auth_inflight: Mutex::new(HashMap::new()),
            metrics,
        }
    }

    /// What the signer-auth cache holds for `(pool_id, signer)` now.
    fn lookup_auth(&self, pool_id: B256, signer: Address) -> CachedAuth {
        let folded_now = self.projection.signer_spent(pool_id, signer);
        let guard = self
            .auth_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cached_auth(guard.get(&(pool_id, signer)), folded_now)
    }
}

impl<P: Provider + Clone + 'static> ResolvingPoolView<P> {
    /// Read `pool_id` on-chain once and record the result: fold a servable pool
    /// into the projection, or negative-cache an absent / `Closed` pool as a
    /// [`NegativeReason::Verdict`] and a failed read as a
    /// [`NegativeReason::Fault`]. The caller holds the pool's [`ReadSlot::Lead`].
    async fn resolve(&self, pool_id: B256) -> Option<PoolStatus> {
        // Bounded: the alloy HTTP provider sets no timeout of its own, so a hung
        // read times out here and takes the fault path below.
        let pool = match timed(None, "admit getPool", self.contract.getPool(pool_id).call()).await {
            Ok(pool) => pool,
            Err(err) => {
                warn!(
                    error = %sanitize_err_chain(&err),
                    %pool_id,
                    suppressed_for = ?RESOLVE_FAULT_TTL,
                    "admit getPool failed; refusing this pool until the negative cache lapses"
                );
                let mut guard = self
                    .negative
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                remember_negative(&mut guard, pool_id, NegativeReason::Fault);
                return None;
            }
        };
        let Some(lifecycle) = resolved_lifecycle(&pool) else {
            debug!(%pool_id, "admit getPool: pool absent or closed; refusing");
            let mut guard = self
                .negative
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            remember_negative(&mut guard, pool_id, NegativeReason::Verdict);
            return None;
        };
        self.projection
            .record_resolved(pool_id, pool.owner, U256::from(pool.deposit), lifecycle);
        // The pool resolved: drop its lapsed negative entry. The projection
        // answers for it from here on.
        {
            let mut guard = self
                .negative
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.remove(&pool_id);
        }
        self.projection.snapshot(pool_id)
    }
}

#[async_trait::async_trait]
impl<P: Provider + Clone + 'static> crate::pool_view::PoolView for ResolvingPoolView<P> {
    async fn status(&self, pool_id: B256) -> Option<PoolStatus> {
        loop {
            // Fast path: the event fold already knows this pool — no chain call.
            if let Some(status) = self.projection.snapshot(pool_id) {
                return Some(status);
            }
            // A pool recently found not-servable, or whose read faulted, is
            // suppressed for its reason's window, so a re-request flood cannot
            // storm `getPool`.
            {
                let guard = self
                    .negative
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(reason) = negative_cache_hit(&guard, pool_id) {
                    debug!(
                        %pool_id,
                        ?reason,
                        "admit getPool suppressed by the negative cache; refusing"
                    );
                    return None;
                }
            }
            match claim_read(&self.inflight, pool_id) {
                ReadSlot::Lead(read) => {
                    let status = self.resolve(pool_id).await;
                    read.finish(status);
                    return status;
                }
                // The reader sends its answer before it ends the read. A read
                // that ends with no answer was cancelled: re-check, and take
                // the read over if no other waiter has.
                ReadSlot::Wait(mut done) => {
                    if done.changed().await.is_ok() {
                        return *done.borrow();
                    }
                }
            }
        }
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

    async fn signer_authorization(
        &self,
        pool_id: B256,
        signer: Address,
    ) -> Option<SignerAuthorization> {
        match self.lookup_auth(pool_id, signer) {
            CachedAuth::Fresh(auth) => {
                self.metrics.serve_signer_auth_cached();
                return Some(auth);
            }
            CachedAuth::Absent => self.metrics.serve_signer_auth_first_read(),
            CachedAuth::Lapsed => self.metrics.serve_signer_auth_reread(),
        }
        loop {
            match claim_read(&self.auth_inflight, (pool_id, signer)) {
                ReadSlot::Lead(read) => {
                    let auth = self.read_authorization(pool_id, signer).await;
                    read.finish(auth);
                    return auth;
                }
                // A read that ends with no answer was cancelled: take the
                // cache's answer if it landed one, else take the read over.
                ReadSlot::Wait(mut done) => {
                    if done.changed().await.is_ok() {
                        return *done.borrow();
                    }
                    if let CachedAuth::Fresh(auth) = self.lookup_auth(pool_id, signer) {
                        return Some(auth);
                    }
                }
            }
        }
    }
}

impl<P: Provider + Clone + 'static> ResolvingPoolView<P> {
    /// Read `getAuthorization` for `(pool_id, signer)` and cache the answer. A
    /// fault (an error or a timeout) caches nothing and answers `None`, which
    /// refuses the signer as unconfirmed. A `Registered` read is cached for
    /// good, so a fault only ever meets a signer with no usable read. The caller
    /// holds the pair's [`ReadSlot::Lead`].
    async fn read_authorization(
        &self,
        pool_id: B256,
        signer: Address,
    ) -> Option<SignerAuthorization> {
        // The baseline is taken before the read is sent; see `AuthRead`.
        let folded_at_read = self.projection.signer_spent(pool_id, signer);
        // Bounded: a hung read times out here and takes the fault path below.
        let auth = match timed(
            None,
            "admit getAuthorization",
            self.contract.getAuthorization(pool_id, signer).call(),
        )
        .await
        {
            Ok(auth) => SignerAuthorization::from_onchain(&auth),
            Err(err) => {
                warn!(
                    error = %sanitize_err_chain(&err),
                    %pool_id,
                    %signer,
                    "admit getAuthorization failed; refusing this signer"
                );
                return None;
            }
        };
        let read = AuthRead {
            auth,
            folded_at_read,
            at: Instant::now(),
        };
        {
            let mut guard = self
                .auth_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            remember_auth(&mut guard, (pool_id, signer), read);
        }
        // Answer as the next cache hit would, so a fold that landed during the
        // read already counts. An `Unregistered` read answers as read.
        match cached_auth(Some(&read), self.projection.signer_spent(pool_id, signer)) {
            CachedAuth::Fresh(fresh) => Some(fresh),
            CachedAuth::Lapsed | CachedAuth::Absent => Some(auth),
        }
    }
}

/// Why `pool_id` is suppressed, when its negative-cache entry is still inside its
/// reason's window — the admit path then skips the `getPool`. `None` for an
/// absent or lapsed entry. Provider-free, so the TTL gate is unit-testable.
fn negative_cache_hit(
    cache: &HashMap<B256, (NegativeReason, Instant)>,
    pool_id: B256,
) -> Option<NegativeReason> {
    cache
        .get(&pool_id)
        .filter(|(reason, at)| at.elapsed() < reason.ttl())
        .map(|(reason, _)| *reason)
}

/// Remember `pool_id` as recently not-servable or faulted, per `reason`. At the
/// cache cap, prune lapsed entries first; if the cache is still full, skip the
/// insert, so a flood of distinct ids cannot grow the map past
/// [`RESOLVE_NEGATIVE_CACHE_MAX`]. Provider-free, so the cap-prune is
/// unit-testable.
fn remember_negative(
    cache: &mut HashMap<B256, (NegativeReason, Instant)>,
    pool_id: B256,
    reason: NegativeReason,
) {
    if cache.len() >= RESOLVE_NEGATIVE_CACHE_MAX {
        cache.retain(|_, (reason, at)| at.elapsed() < reason.ttl());
    }
    if cache.len() < RESOLVE_NEGATIVE_CACHE_MAX || cache.contains_key(&pool_id) {
        cache.insert(pool_id, (reason, Instant::now()));
    }
}

/// Read key → a receiver for that key's in-flight admit chain read. The channel
/// starts at a seen `V::default()` and carries the read's answer once it ends.
type InflightReads<K, V> = Mutex<HashMap<K, watch::Receiver<V>>>;

/// A request's place in the admit chain read for one key. At most one request
/// per key reads at a time; the rest wait for it.
enum ReadSlot<'a, K: Eq + Hash, V> {
    /// No read is in flight: this request reads, and the guard ends the read.
    Lead(InflightRead<'a, K, V>),
    /// A read is in flight: the receiver's `changed` returns `Ok` with the
    /// read's answer, or an error when the read ends unanswered.
    Wait(watch::Receiver<V>),
}

/// A key's in-flight read. [`finish`](Self::finish) hands the answer to every
/// waiter. Dropping it ends the read: it removes the key's entry, then drops
/// the sender, which wakes every waiter. A cancelled reader ends the read with
/// no answer, so a waiter then re-checks and takes over the read.
struct InflightRead<'a, K: Eq + Hash, V> {
    /// The map this read is registered in.
    inflight: &'a InflightReads<K, V>,
    /// The key being read.
    key: K,
    /// Carries the answer to the waiters. Dropped after [`Drop::drop`] removes
    /// the entry, which closes the channel.
    done: watch::Sender<V>,
}

impl<K: Eq + Hash, V> InflightRead<'_, K, V> {
    /// Hand `answer` to every waiter, then end the read. The waiters take it
    /// as their own answer, so the read's outcome reaches them whether or not
    /// a cache had room to record it.
    fn finish(self, answer: V) {
        self.done.send_replace(answer);
    }
}

impl<K: Eq + Hash, V> Drop for InflightRead<'_, K, V> {
    fn drop(&mut self) {
        self.inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.key);
    }
}

/// Join the in-flight read for `key`, or start one.
fn claim_read<K: Eq + Hash + Copy, V: Default>(
    inflight: &InflightReads<K, V>,
    key: K,
) -> ReadSlot<'_, K, V> {
    let mut guard = inflight
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // A closed channel means its read already ended: start a new one rather
    // than wait on it, which would return at once and spin the caller's loop.
    if let Some(done) = guard.get(&key)
        && done.has_changed().is_ok()
    {
        return ReadSlot::Wait(done.clone());
    }
    let (done, waiters) = watch::channel(V::default());
    guard.insert(key, waiters);
    ReadSlot::Lead(InflightRead {
        inflight,
        key,
        done,
    })
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
/// its aggregate unredeemed value clears the floor, driven by three sources —
/// advisory hints ([`LaneKey`]) from the voucher-accept path, a low-frequency
/// self-tick that sweeps every lane into chunked `redeemMany` transactions so a
/// dropped hint can never strand a lane whose chunk has cleared the floor, and a
/// sweep at the earliest serve cutoff of a lane with a finite capability expiry
/// and value owed ([`serve_cutoff_wake`]). Ends cleanly when every hint sender
/// is dropped.
///
/// The cutoff sweep is what makes a delegated lane's final claim redeemable. The
/// claim stops growing at the serve cutoff, and the sweep then has about one
/// redeem interval of room before [`capability_expired`] skips the lane. A
/// self-tick alone leaves no room for delay: the last tick before the expiry can
/// fall just before the landing slack, and the loop is serial, so a receipt wait
/// that delays that tick forfeits the lane. The cutoff sweep waits behind an
/// in-flight redemption too, but the interval absorbs that wait.
/// `margin_secs` is the voucher path's capability-expiry margin.
///
/// A lane whose hint-path redemption hits a chain fault (an RPC fault or a
/// revert) is parked: each later hint for it is dropped and counted into
/// `redeem_hints_parked`. Each tick or cutoff sweep retries the parked lanes
/// with every other lane. Only a sweep that runs and reports no chain fault
/// clears the parked set. A failed lane load runs no sweep and keeps the set,
/// and a sweep with any chain fault keeps the whole set. So during an RPC
/// outage each lane costs one failed attempt per interval: the sweep's batched
/// retry. Hints for a parked lane stay dropped until a sweep finishes without a
/// chain fault. Parking delays only the hint path: the sweep retries every lane
/// each interval, and a serve cutoff still wakes a sweep, so no claim strands.
#[allow(clippy::too_many_arguments)]
async fn redeemer_loop<P: Provider + Clone>(
    contract: PaymentPool::PaymentPoolInstance<P>,
    store: Arc<dyn PoolStateStore>,
    paid: PaidWatermarks,
    self_address: Address,
    redeem_threshold: U256,
    max_vouchers: usize,
    redeem_interval: Duration,
    margin_secs: u64,
    mut redeem_rx: mpsc::Receiver<LaneKey>,
    metrics: Arc<Metrics>,
    pool_view: PoolProjection,
) {
    let mut ticker = tokio::time::interval(redeem_interval);
    // Skip the immediate first tick: new vouchers hint anyway, and the first
    // sweep waits one interval. A lane persisted before a restart whose claim
    // cannot wait that long gets its cutoff sweep instead: no scan has run yet,
    // so a cutoff that passed while the node was down is due at once.
    ticker.tick().await;
    let mut swept_at = 0;
    let mut parked: HashSet<LaneKey> = HashSet::new();
    let mut next_cutoff = load_lanes(store.as_ref()).and_then(|states| {
        next_serve_cutoff(
            &states,
            &paid,
            self_address,
            margin_secs,
            swept_at,
            unix_now(),
        )
    });
    loop {
        tokio::select! {
            hint = redeem_rx.recv() => match hint {
                Some(key) => {
                    if parked.contains(&key) {
                        metrics.redeem_hint_parked();
                        continue;
                    }
                    let outcome = redeem_one(
                        &contract, &store, &paid, self_address, margin_secs, swept_at,
                        redeem_threshold, max_vouchers, key, &metrics, &pool_view,
                    )
                    .await;
                    if outcome.chain_fault {
                        park_lane(&mut parked, key);
                    }
                    next_cutoff = next_cutoff.into_iter().chain(outcome.wake).min();
                    continue;
                }
                // All hint senders dropped — the service is going away.
                None => break,
            },
            _ = ticker.tick() => {}
            () = sleep_until_unix(next_cutoff) => {}
        }
        // A tick or a cutoff: sweep every lane, and schedule the next cutoff
        // from the same scan. A failed load keeps every cutoff still ahead and
        // drops one that has passed, so the loop does not re-fire it at once;
        // the next tick retries the scan. The sweep retries the parked lanes
        // with every other lane. A failed load runs no sweep, so the parked set
        // stays.
        let Some(states) = load_lanes(store.as_ref()) else {
            next_cutoff = next_cutoff.filter(|wake| *wake > unix_now());
            continue;
        };
        swept_at = unix_now();
        next_cutoff = next_serve_cutoff(
            &states,
            &paid,
            self_address,
            margin_secs,
            swept_at,
            swept_at,
        );
        let chain_fault = redeem_sweep(
            &contract,
            &store,
            states,
            &paid,
            self_address,
            redeem_threshold,
            max_vouchers,
            true,
            &metrics,
            &pool_view,
        )
        .await;
        // Release the parked lanes only after a sweep without a chain fault. A
        // sweep with a fault keeps the whole set: one failed attempt per lane
        // per interval, the sweep's own batched retry.
        if !chain_fault {
            parked.clear();
        }
    }
    debug!("PaymentPool redeemer loop ended (all hint senders dropped)");
}

/// One lane planned for redemption: its pool, its persistence key (so a landed
/// registration can be written back to `registered_until`), its full claim value
/// and its unredeemed value (for the per-chunk floor), the highest voucher to
/// submit, and — on the signer's first redemption — the owner-signed capability
/// to register.
struct PlannedLane {
    pool_id: PoolId,
    key: LaneKey,
    /// The claim's full on-chain-comparable value (`amount + verified frontier`).
    /// The contract pays `owed − w.amount`, so the pre-submit reconciliation
    /// ([`reconcile_onchain_watermarks`]) drops a lane whose on-chain watermark
    /// already reaches this, and recomputes `unredeemed` from the fresh read.
    owed: U256,
    unredeemed: U256,
    voucher: PaymentPool::LaneVoucher,
    register: Option<PaymentPool::CapabilityReg>,
}

/// Sum the unredeemed value across planned lanes — the raw USDC the node holds
/// in accepted vouchers it has not yet cashed on-chain. Saturating so a
/// pathological total can never wrap; in practice pool deposits bound it far
/// below `U256::MAX`. Feeds the `decdn_unredeemed_usdc` gauge once per sweep.
fn sum_unredeemed(plans: &[PlannedLane]) -> U256 {
    plans
        .iter()
        .fold(U256::ZERO, |acc, plan| acc.saturating_add(plan.unredeemed))
}

/// Sum the value still recoverable from the distinct pools behind `plans` —
/// `Σ (deposit − totalRedeemed)`, the ceiling those pools can still pay this
/// node. Feeds the `decdn_pool_deposit_usdc` gauge once per sweep.
///
/// `remaining`, not the raw deposit: already-redeemed funds have left the pool
/// and are not recoverable from it, so counting them would overstate what the
/// node can still collect. A pool the projection has not folded contributes
/// nothing rather than a guess — the same fail-quiet direction the planner
/// takes on an unknown pool.
///
/// Scoped to planned lanes, which is what bounds it to this node's own
/// counterparties: the projection also folds `PoolOpened` for pools this node
/// has no lane on, and summing those would report the network's escrow as its
/// own. The precise reading is therefore "pools that currently owe this node",
/// not every pool it has ever been paid by — a lane settles out of the planned
/// set once it is fully redeemed. The caller skips the publish on an empty plan
/// set so that settling out does not read as the escrow disappearing.
///
/// A pool the projection has not folded contributes nothing rather than a
/// guess, so the gauge under-reports in the window after a restart where the
/// planner has deliberately failed open on an unknown pool.
fn sum_recoverable_deposit(plans: &[PlannedLane], pool_view: &PoolProjection) -> U256 {
    let mut seen: HashSet<PoolId> = HashSet::new();
    plans
        .iter()
        .filter(|plan| seen.insert(plan.pool_id))
        .filter_map(|plan| pool_view.snapshot(plan.pool_id))
        .fold(U256::ZERO, |acc, status| {
            acc.saturating_add(status.remaining)
        })
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
/// the floor is dropped and its lanes defer to a later sweep (a zero floor
/// keeps every chunk). The chunk count is the minimum that
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
/// redeemable only while its deadline lies more than [`REDEEM_LANDING_SLACK_SECS`]
/// ahead. Past the deadline `redeemMany` reverts `PoolClosed`, and that revert
/// takes the whole batch down with it.
fn pool_is_redeemable(status: Option<PoolStatus>, now: u64) -> bool {
    match status {
        None => true,
        Some(s) => {
            if s.remaining.is_zero() {
                return false;
            }
            match s.lifecycle {
                Lifecycle::Open => true,
                Lifecycle::Closing { deadline } => {
                    now.saturating_add(REDEEM_LANDING_SLACK_SECS) < deadline
                }
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

/// Whether a capability `expiry` has passed, or falls within
/// [`REDEEM_LANDING_SLACK_SECS`] of `now`, so a redemption planned now would land
/// on an expired capability and pay 0. `0` means no tracked expiry and never
/// expires.
const fn capability_expired(expiry: u64, now: u64) -> bool {
    expiry != 0 && now.saturating_add(REDEEM_LANDING_SLACK_SECS) >= expiry
}

/// When the redeemer wakes for one lane's serve cutoff, in Unix seconds, or
/// `None` when the lane needs no cutoff sweep.
///
/// The serve cutoff is `expiry − margin_secs`. From that second on, the voucher
/// and preimage paths refuse the lane's proofs with `CapabilityExpired`
/// ([`inside_capability_expiry_margin`](decdn_common::config::inside_capability_expiry_margin)),
/// so the lane's claim is final. The wake
/// is one second after the cutoff, because those paths read a coarse clock that
/// can lag the wall clock by up to one refresh. A sweep at the wake has one
/// redeem interval, less that second, to start the redemption before
/// [`capability_expired`] skips the lane, and [`REDEEM_LANDING_SLACK_SECS`] to
/// land it.
///
/// Only a lane this node provides, with a finite expiry and value owed beyond its
/// cached paid watermark, needs a wake.
fn lane_cutoff_wake(
    st: &LaneState,
    paid: &PaidWatermarks,
    self_address: Address,
    margin_secs: u64,
) -> Option<u64> {
    if st.provider != self_address || st.expiry == 0 || st.owed() <= paid.get(&st.key()) {
        return None;
    }
    Some(st.expiry.saturating_sub(margin_secs).saturating_add(1))
}

/// When the redeemer must sweep for one lane's cutoff, given that its last
/// sweep scanned the lane store at `swept_at` (`0` before the first scan).
///
/// A scan at or after the lane's [`lane_cutoff_wake`] already read the final
/// claim, so the lane needs no further cutoff sweep. Otherwise the sweep is due
/// at the wake, or at `now` when the wake has passed unscanned: the node was
/// down at the cutoff, or the lane's first hint waited behind redemption work
/// until after it. A lane that [`capability_expired`] skips at `now` cannot be
/// redeemed and needs no sweep.
fn serve_cutoff_wake(
    st: &LaneState,
    paid: &PaidWatermarks,
    self_address: Address,
    margin_secs: u64,
    swept_at: u64,
    now: u64,
) -> Option<u64> {
    let wake = lane_cutoff_wake(st, paid, self_address, margin_secs)?;
    (wake > swept_at && !capability_expired(st.expiry, now)).then_some(wake.max(now))
}

/// The earliest [`serve_cutoff_wake`] across `states`.
fn next_serve_cutoff(
    states: &[LaneState],
    paid: &PaidWatermarks,
    self_address: Address,
    margin_secs: u64,
    swept_at: u64,
    now: u64,
) -> Option<u64> {
    states
        .iter()
        .filter_map(|st| serve_cutoff_wake(st, paid, self_address, margin_secs, swept_at, now))
        .min()
}

/// Sleep until the Unix second `wake`, or forever when there is none.
async fn sleep_until_unix(wake: Option<u64>) {
    match wake {
        Some(wake) => {
            let wait = Duration::from_secs(wake.saturating_sub(unix_now()));
            tokio::time::sleep(wait).await;
        }
        None => std::future::pending().await,
    }
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

/// Seed a fresh paid-watermark cache from the durable lane rows this node
/// provides. Bootstrap calls this before the redeemer starts so a restart carries
/// forward redemptions that landed before the log-poller's persisted cursor,
/// instead of re-planning those already-redeemed lanes into a `redeemMany` the
/// contract silently no-ops (#2052).
///
/// A `load_all` failure is not fatal: the `PoolRedeemed` watcher still rebuilds
/// `paid` forward from the cursor. It only re-opens the restart-amnesia window for
/// pre-cursor lanes, so the failure is surfaced loudly rather than aborting
/// bring-up.
fn rehydrate_paid_watermarks(store: &dyn PoolStateStore, self_address: Address) -> PaidWatermarks {
    let paid = PaidWatermarks::default();
    match store.load_all() {
        Ok(states) => {
            let mut restored = 0usize;
            for st in &states {
                if st.provider == self_address && !st.paid_cumulative.is_zero() {
                    paid.set(st.key(), st.paid_cumulative);
                    restored += 1;
                }
            }
            if restored > 0 {
                info!(
                    restored,
                    "rehydrated paid-watermark cache from durable lane store"
                );
            }
        }
        Err(err) => warn!(
            error = %err,
            "failed to rehydrate paid-watermark cache from lane store; pre-cursor \
             redemptions may be re-submitted until re-observed on-chain"
        ),
    }
    paid
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
        owed,
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
///
/// A lane with value owed whose capability expires within
/// [`REDEEM_LANDING_SLACK_SECS`] of `now` is skipped and metered: the contract
/// pays 0 for an expired capability, so its redemption would only spend gas. A
/// lane with nothing owed is dropped silently, expired or not.
#[allow(clippy::cognitive_complexity, clippy::too_many_arguments)]
fn plan_lanes(
    paid: &PaidWatermarks,
    self_address: Address,
    states: Vec<LaneState>,
    metrics: &Arc<Metrics>,
    pool_view: &PoolProjection,
    now: u64,
) -> Vec<PlannedLane> {
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
            // The expiry check follows `plan_lane`, which drops a lane with
            // nothing unredeemed. A fully redeemed lane stays in the store until
            // its pool is reclaimed, so only a skip that strands value is metered.
            Ok(Some(_)) if capability_expired(st.expiry, now) => {
                metrics.redemption_skipped_expired();
                debug!(
                    pool_id = %st.pool_id,
                    signer = %st.signer,
                    expiry = st.expiry,
                    "redeemer: skipping a lane with value owed whose capability has expired"
                );
            }
            Ok(Some(lane)) => plans.push(lane),
            Ok(None) => {}
            Err(err) => {
                metrics.redemption_failure();
                warn!(error = %sanitize_err_chain(&err), pool_id = %st.pool_id, "redemption planning failed");
            }
        }
    }
    plans
}

/// Park `key` after a chain fault on its hint path, until a sweep finishes
/// without a chain fault ([`redeemer_loop`]).
fn park_lane(parked: &mut HashSet<LaneKey>, key: LaneKey) {
    debug!(
        pool_id = %key.pool_id,
        signer = %key.signer,
        provider = %key.provider,
        "lane parked after a chain fault until a sweep finishes without one"
    );
    parked.insert(key);
}

/// What one hint-path redemption tells [`redeemer_loop`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct HintOutcome {
    /// The lane's [`serve_cutoff_wake`], if it has one.
    wake: Option<u64>,
    /// The redemption hit a chain fault: a failed watermark read or a
    /// `redeemMany` that did not land. The loop parks the lane until the next
    /// sweep.
    chain_fault: bool,
}

/// Hint-path redemption: plan one lane and, if it clears the per-chunk `floor`,
/// submit it as a one-lane `redeemMany`. A sub-floor hint defers to the next
/// sweep, which packs it with other lanes, and reads nothing from the chain.
/// Returns the lane's [`serve_cutoff_wake`] against the last sweep scan at
/// `swept_at`, so a lane first seen between sweeps gets its cutoff sweep, even
/// when the hint waited behind redemption work until after the cutoff. Also
/// returns whether the redemption hit a chain fault. A lane the store cannot
/// load is a local fault, not a chain fault.
#[allow(clippy::too_many_arguments)]
async fn redeem_one<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    paid: &PaidWatermarks,
    self_address: Address,
    margin_secs: u64,
    swept_at: u64,
    floor: U256,
    max_vouchers: usize,
    key: LaneKey,
    metrics: &Arc<Metrics>,
    pool_view: &PoolProjection,
) -> HintOutcome {
    let st = match store.get(key) {
        Ok(Some(st)) => st,
        Ok(None) => return HintOutcome::default(),
        Err(err) => {
            metrics.redemption_failure();
            warn!(error = %err, pool_id = %key.pool_id, "redemption planning failed to load lane");
            return HintOutcome::default();
        }
    };
    let now = unix_now();
    let wake = serve_cutoff_wake(&st, paid, self_address, margin_secs, swept_at, now);
    let plans = plan_lanes(paid, self_address, vec![st], metrics, pool_view, now);
    // Hint path: require the durability floor and skip the submit on a failed
    // flush (`strict_flush`); the lane defers to the next sweep.
    let chain_fault =
        redeem_planned_lanes(contract, store, plans, floor, max_vouchers, true, metrics).await;
    HintOutcome { wake, chain_fault }
}

/// Load every persisted lane for a sweep or for cutoff scheduling. A load
/// failure is logged and returns `None`.
fn load_lanes(store: &dyn PoolStateStore) -> Option<Vec<LaneState>> {
    store
        .load_all()
        .inspect_err(|err| warn!(error = %err, "redeemer sweep: failed to load lane state"))
        .ok()
}

/// Sweep: plan every lane in `states` (per-lane error isolation through the
/// planning phase), then chunk the survivors under the per-chunk floor +
/// voucher-count cap and submit each chunk as its own `redeemMany`. Value is
/// spread across chunks so dust settles alongside real value; a chunk that
/// cannot clear the floor defers to a later sweep.
///
/// Returns whether the sweep hit a chain fault, as [`redeem_planned_lanes`]
/// reports it. The periodic sweep in [`redeemer_loop`] releases the parked
/// lanes only when it returns `false`.
#[allow(clippy::too_many_arguments)]
async fn redeem_sweep<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    states: Vec<LaneState>,
    paid: &PaidWatermarks,
    self_address: Address,
    floor: U256,
    max_vouchers: usize,
    strict_flush: bool,
    metrics: &Arc<Metrics>,
    pool_view: &PoolProjection,
) -> bool {
    let plans = plan_lanes(paid, self_address, states, metrics, pool_view, unix_now());
    // Publish the pending-redemption total once per sweep. This is the whole
    // planned set, before the per-chunk floor defers any dust — a lane below the
    // floor is still owed and rides a later sweep, so counting it here is what
    // makes the gauge "USDC waiting to be redeemed" rather than "redeeming now".
    metrics.set_unredeemed_usdc(sum_unredeemed(&plans));
    // Only when there is something to measure. An empty plan set is the HEALTHY
    // steady state — everything owed has been redeemed — and publishing zero for
    // it would sawtooth the gauge to 0 after every successful sweep, which reads
    // exactly like insolvent counterparties. Leaving the last value standing is
    // the same discipline the buyer-wallet read takes.
    if !plans.is_empty() {
        metrics.set_pool_deposit_usdc(sum_recoverable_deposit(&plans, pool_view));
    }
    // A sweep is the retry for every lane, so it parks nothing itself. The
    // caller decides from the chain-fault flag whether parked lanes go free.
    redeem_planned_lanes(
        contract,
        store,
        plans,
        floor,
        max_vouchers,
        strict_flush,
        metrics,
    )
    .await
}

/// Submit one chunk of planned lanes as a single `redeemMany`. On an oversize
/// send failure (`is_oversize_send_err`) with more than one lane, halve the chunk
/// and retry each half, bounded by `MAX_SPLIT_DEPTH`. A revert or a non-oversize
/// send error records one `redemption_failure`. An unconfirmed receipt (a failed
/// or lapsed wait that the by-hash fetch in `send_and_await_receipt` could not
/// resolve) is not a failure: the tx may have mined, and it counts only into its
/// `onchain_tx_*` bucket. Every non-landed outcome leaves the claims for the next
/// sweep (cumulative, monotone, retry-safe). Does NOT seed the paid cache — each
/// paid voucher emits its own `PoolRedeemed`.
///
/// Returns whether the chunk hit a chain fault: any outcome other than
/// `Landed`, the unconfirmed ones included, in the chunk itself or in either
/// half of a split. The hint path parks the lane on a fault ([`redeemer_loop`]).
///
/// Runs inside an `onchain_tx` span that records the transaction hash (`tx`)
/// and the `outcome`. A halved retry nests its two halves as child spans.
#[allow(clippy::cognitive_complexity)]
#[tracing::instrument(
    name = "onchain_tx",
    skip_all,
    fields(
        op = "redeemMany",
        depth = depth,
        voucher_count = lanes.len(),
        tx = tracing::field::Empty,
        outcome = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    )
)]
async fn submit_chunk<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    mut lanes: Vec<PlannedLane>,
    metrics: &Arc<Metrics>,
    depth: u32,
) -> bool {
    if lanes.is_empty() {
        return false;
    }
    let span = tracing::Span::current();
    let batches = group_by_pool(&lanes);
    let cap_count: usize = batches.iter().map(|b| b.capabilities.len()).sum();
    let voucher_count = lanes.len();
    let pool_count = batches.len();
    let sent = contract.redeemMany(batches).send().await;
    let outcome = send_and_await_receipt(sent, Some(REDEEM_RECEIPT_TIMEOUT), metrics).await;
    let failed = is_redemption_failure(&outcome);
    let chain_fault = is_chain_fault(&outcome);
    match outcome {
        TxOutcome::Landed(receipt) => {
            span.record("tx", tracing::field::display(receipt.transaction_hash));
            span.record("outcome", "landed");
            info!(
                pool_count,
                cap_count,
                voucher_count,
                tx = %receipt.transaction_hash,
                "batched lane redemption landed (redeemMany)"
            );
            record_landed_chunk(contract, store, &lanes, metrics).await;
            return false;
        }
        TxOutcome::SendErr(err)
            if lanes.len() >= 2
                && depth < MAX_SPLIT_DEPTH
                && is_oversize_send_err(&err.to_string()) =>
        {
            span.record("outcome", "split");
            let mid = lanes.len() / 2;
            let right = lanes.split_off(mid);
            warn!(
                voucher_count,
                depth, "redeemMany send rejected oversized; halving chunk and retrying"
            );
            let left_faulted =
                Box::pin(submit_chunk(contract, store, lanes, metrics, depth + 1)).await;
            let right_faulted =
                Box::pin(submit_chunk(contract, store, right, metrics, depth + 1)).await;
            // Each half counts its own outcome; the split itself is not a failure.
            return left_faulted || right_faulted;
        }
        TxOutcome::Reverted(receipt) => {
            record_tx_failure(&span, "reverted", Some(receipt.transaction_hash));
            warn!(voucher_count, tx = %receipt.transaction_hash, "redeemMany reverted on-chain; leaving claims for retry");
        }
        TxOutcome::SendErr(err) => {
            record_tx_failure(&span, "send_failed", None);
            warn!(error = %sanitize_error_sources(&err), voucher_count, "redeemMany send failed; leaving claims for retry");
        }
        TxOutcome::ReceiptErr {
            error,
            tx_hash,
            last_lookup,
        } => {
            record_tx_failure(&span, "receipt_failed", Some(tx_hash));
            warn!(error = %sanitize_error_sources(&error), %last_lookup, voucher_count, tx = %tx_hash, "redeemMany receipt wait failed and the by-hash lookup missed; unconfirmed, leaving claims for retry");
        }
        TxOutcome::Timeout {
            tx_hash,
            last_lookup,
        } => {
            record_tx_failure(&span, "timeout", Some(tx_hash));
            warn!(%last_lookup, voucher_count, tx = %tx_hash, timeout = ?REDEEM_RECEIPT_TIMEOUT, "redeemMany receipt timed out and the by-hash lookup missed; unconfirmed, leaving claims for retry");
        }
    }
    if failed {
        metrics.redemption_failure();
    }
    chain_fault
}

/// Whether a `redeemMany` outcome is a chain fault that parks a hinted lane
/// ([`redeemer_loop`]). Every outcome but `Landed` is one, the unconfirmed ones
/// included: the lane's claim stays unsettled either way, so a hint that retries
/// it at once meets the same fault. An oversize send that `submit_chunk` halves
/// takes the outcome of its halves instead.
const fn is_chain_fault(outcome: &TxOutcome) -> bool {
    match outcome {
        TxOutcome::Landed(_) => false,
        TxOutcome::Reverted(_)
        | TxOutcome::SendErr(_)
        | TxOutcome::ReceiptErr { .. }
        | TxOutcome::Timeout { .. } => true,
    }
}

/// Whether a `redeemMany` outcome counts into `decdn_redemption_failures_total`.
/// A revert and a refused send are failures. A landed chunk is not, and neither
/// is an unconfirmed one (`ReceiptErr` / `Timeout`): the by-hash lookup already
/// resolves every transaction the RPC can see mined, so what stays unconfirmed
/// counts into `decdn_onchain_tx_receipt_failed_total` /
/// `decdn_onchain_tx_timeout_total`, which `DecdnOnchainTxUnconfirmed` alerts
/// on. An oversize send that `submit_chunk` halves never reaches this count.
const fn is_redemption_failure(outcome: &TxOutcome) -> bool {
    match outcome {
        TxOutcome::Reverted(_) | TxOutcome::SendErr(_) => true,
        TxOutcome::Landed(_) | TxOutcome::ReceiptErr { .. } | TxOutcome::Timeout { .. } => false,
    }
}

/// Record a landed `redeemMany` chunk: count its vouchers into
/// `pool_redemptions`, then persist `registered_until` for every lane whose
/// `CapabilityReg` rode in the chunk, so the next sweep does not register it
/// again.
///
/// The persisted value is the expiry the chain registered, read back with one
/// batched `getAuthorizations`, not the attached `CapabilityReg`'s expiry.
/// On-chain registration is write-once: when another provider landed a
/// different capability for the signer first, the attached one was a no-op and
/// its expiry is not the signer's. A failed or short read persists nothing for
/// the unread lanes; the next sweep attaches their `CapabilityReg` again (a
/// no-op on-chain) and re-reads.
async fn record_landed_chunk<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    lanes: &[PlannedLane],
    metrics: &Metrics,
) {
    metrics.pool_redemptions(u64::try_from(lanes.len()).unwrap_or(u64::MAX));
    let registering: Vec<LaneKey> = lanes
        .iter()
        .filter(|lane| lane.register.is_some())
        .map(|lane| lane.key)
        .collect();
    for chunk in registering.chunks(WATERMARK_READ_BATCH_MAX) {
        let pool_ids = chunk.iter().map(|key| key.pool_id).collect();
        let signers = chunk.iter().map(|key| key.signer).collect();
        let builder = contract.getAuthorizations(pool_ids, signers);
        match timed(None, "post-redeem getAuthorizations", builder.call()).await {
            Ok(auths) => persist_registered_expiries(store, chunk, &auths),
            Err(err) => {
                warn!(
                    error = %sanitize_err_chain(&err),
                    lanes = chunk.len(),
                    "post-redeem registration read failed; the next sweep re-registers and re-reads"
                );
                break;
            }
        }
    }
}

/// Persist each lane's registered expiry as its `registered_until`. `auths` is
/// the `getAuthorizations` result for `keys`, in the same order; a short result
/// is a positionally sound prefix and the unread tail is skipped. A signer the
/// chain still reports unregistered persists nothing.
fn persist_registered_expiries(
    store: &Arc<dyn PoolStateStore>,
    keys: &[LaneKey],
    auths: &[PaymentPool::Authorization],
) {
    if auths.len() != keys.len() {
        warn!(
            expected = keys.len(),
            got = auths.len(),
            "post-redeem registration read returned a mismatched count"
        );
    }
    for (key, auth) in keys.iter().zip(auths) {
        let SignerAuthorization::Registered { expiry, .. } =
            SignerAuthorization::from_onchain(auth)
        else {
            continue;
        };
        if let Err(err) = store.set_registered_until(*key, expiry) {
            warn!(error = %err, pool_id = %key.pool_id, signer = %key.signer,
                "failed to persist registered_until after a landed registration");
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
/// and hint paths defer their submit, while the shutdown sweep redeems anyway —
/// with the residual that a `CapabilityReg` can go on-chain against material
/// that never reached disk.
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
                warn!(error = %err, "pre-redeem lane store flush failed; deferring redeem");
            } else {
                warn!(
                    error = %err,
                    "pre-redeem lane store flush failed; the shutdown sweep proceeds \
                     with un-flushed lane and capability state"
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

/// Read the on-chain paid watermark for every planned lane and drop any lane the
/// chain already shows settled to its claim value — the last check before
/// spending gas on a `redeemMany` the contract would silently no-op (its own
/// `claimed <= w.amount` guard, `PaymentPool.redeem`). A surviving lane's
/// `unredeemed` is recomputed from the fresh on-chain paid, so the per-chunk
/// floor gates on the true delta.
///
/// This is the freshest possible signal and complements the durable paid cache:
/// the event-fed cache can lag the chain between poll ticks, and another actor
/// could have redeemed the same lane. The read is the contract's own
/// `getWatermarks` batch view: one `eth_call` per batch of at most
/// [`WATERMARK_READ_BATCH_MAX`] lanes, no per-lane fan-out and no dependency on
/// a contract outside the protocol's own deployment.
///
/// **Fail-open.** An RPC error, a timeout or a mismatched return length submits
/// the lanes it could not read unchanged: the contract's own no-op guard is the
/// backstop, and the only cost of a stale read is the gas this check saves. It
/// never holds up a redemption on a read it could not make. A failed batch stops
/// the read there, so later batches are not issued; the batches already read
/// still reconcile and keep their savings. Each batch meters itself through
/// `redemption_reconcile_ok` / `redemption_reconcile_failure`, because a zero
/// `redemption_reconciled_skip` alone cannot separate a healthy sweep with
/// nothing to drop from a read that never landed.
///
/// Returns the kept lanes and whether a batch failed: an RPC error, a timeout or
/// a mismatched return length, each also metered as a reconcile failure. The
/// hint path parks the lane on a failed read ([`redeemer_loop`]).
async fn reconcile_onchain_watermarks<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    plans: Vec<PlannedLane>,
    metrics: &Arc<Metrics>,
) -> (Vec<PlannedLane>, bool) {
    // An empty plan set is the healthy steady state, so short-circuit rather than
    // issue an `eth_call` with three empty arrays on every idle sweep.
    if plans.is_empty() {
        return (plans, false);
    }
    let mut read_failed = false;
    let mut onchain_paid: Vec<U256> = Vec::with_capacity(plans.len());
    for chunk in plans.chunks(WATERMARK_READ_BATCH_MAX) {
        let mut pool_ids = Vec::with_capacity(chunk.len());
        let mut signers = Vec::with_capacity(chunk.len());
        let mut providers = Vec::with_capacity(chunk.len());
        for plan in chunk {
            pool_ids.push(plan.pool_id);
            signers.push(plan.key.signer);
            providers.push(plan.key.provider);
        }
        // Bound the read: the alloy HTTP provider has no request timeout, so a hung
        // RPC would wedge the redeemer loop forever — the opposite of fail-open. A
        // timeout folds into the error arm and submits the unread tail unchanged
        // (degrade-and-continue, per `timed`); the default bound
        // (`DEFAULT_RPC_CALL_TIMEOUT`) applies.
        let builder = contract.getWatermarks(pool_ids, signers, providers);
        let lanes = match timed(None, "pre-redeem getWatermarks", builder.call()).await {
            Ok(lanes) => lanes,
            Err(err) => {
                warn!(
                    stage = "reconcile",
                    error = %sanitize_err_chain(&err),
                    lanes = chunk.len(),
                    "pre-redeem watermark reconciliation failed; submitting on the contract's own no-op guard"
                );
                metrics.redemption_reconcile_failure();
                read_failed = true;
                break;
            }
        };
        // A well-behaved contract returns one `Lane` per input triple, in input
        // order. Any other length means the decoder and the chain disagree about
        // the return shape, so the batch's values cannot be trusted to pair with
        // this chunk's plans — a long return may be offset, and dropping a lane
        // on a mis-paired watermark could forfeit real claim value at the next
        // shutdown sweep.
        // Keep only a short return's prefix, which is still positionally sound,
        // and stop either way; `reconcile_plans` keeps the untouched tail.
        if lanes.len() != chunk.len() {
            warn!(
                stage = "reconcile",
                expected = chunk.len(),
                got = lanes.len(),
                "watermark reconciliation returned a mismatched count; submitting the \
                 unreconciled lanes unchanged"
            );
            metrics.redemption_reconcile_failure();
            read_failed = true;
            if lanes.len() < chunk.len() {
                onchain_paid.extend(lanes.iter().map(|lane| U256::from(lane.amount)));
            }
            break;
        }
        metrics.redemption_reconcile_ok();
        onchain_paid.extend(lanes.iter().map(|lane| U256::from(lane.amount)));
    }
    let (kept, skipped) = reconcile_plans(plans, &onchain_paid);
    if skipped > 0 {
        metrics.redemption_reconciled_skip_by(skipped);
    }
    (kept, read_failed)
}

/// Pure core of [`reconcile_onchain_watermarks`]: given each plan's freshly-read
/// on-chain paid watermark (parallel to `plans`, same order), drop every lane the
/// chain already shows settled to its claim value and recompute `unredeemed` for
/// the survivors from that fresh paid. Returns the kept lanes and the count
/// dropped. `onchain_paid` is a prefix of `plans`, in the same order: the caller
/// supplies a shorter slice when a batch errors or under-returns, and the
/// untouched tail is conservatively kept.
fn reconcile_plans(plans: Vec<PlannedLane>, onchain_paid: &[U256]) -> (Vec<PlannedLane>, u64) {
    let mut kept = Vec::with_capacity(plans.len());
    let mut skipped = 0u64;
    for (idx, mut plan) in plans.into_iter().enumerate() {
        let Some(&onchain) = onchain_paid.get(idx) else {
            // No reading for this lane (short slice): keep it unchanged — the
            // contract's own no-op guard remains the backstop.
            kept.push(plan);
            continue;
        };
        if onchain >= plan.owed {
            skipped += 1;
            debug!(
                pool_id = %plan.pool_id,
                signer = %plan.key.signer,
                onchain_paid = %onchain,
                owed = %plan.owed,
                "reconciliation: lane already settled on-chain, dropping from redeem batch"
            );
            continue;
        }
        // Recompute against the fresh on-chain paid so a lagging cache cannot push
        // this lane over the per-chunk floor on value the chain has already paid.
        plan.unredeemed = plan.owed.saturating_sub(onchain);
        kept.push(plan);
    }
    (kept, skipped)
}

/// Record a failed transaction's `outcome`, its hash when one was issued, and
/// an error status on its `onchain_tx` span.
fn record_tx_failure(span: &tracing::Span, outcome: &'static str, tx: Option<TxHash>) {
    if let Some(tx) = tx {
        span.record("tx", tracing::field::display(tx));
    }
    span.record("outcome", outcome);
    span.record("otel.status_code", "ERROR");
}

/// Whether `provider` has a lane of `pool_id` in `states` that is owed more
/// than it has been paid.
fn holds_unredeemed(states: &[LaneState], pool_id: PoolId, provider: Address) -> bool {
    states.iter().any(|st| {
        st.pool_id == pool_id && st.provider == provider && st.owed() > st.paid_cumulative
    })
}

/// Chunk planned lanes under the per-chunk floor + voucher-count cap and submit
/// each chunk. Every caller passes the configured
/// `blockchain.redeem_threshold_micro_usdc`, which config keeps above zero, so a
/// sub-floor chunk defers on every path — the hint, the self-tick sweep, and the
/// graceful-shutdown sweep alike. The floor is per CHUNK, and value is spread
/// so dust rides alongside larger lanes: a sub-floor lane stays unredeemed
/// only until its chunk's aggregate clears the floor, or until the pool owner
/// reclaims it.
///
/// The floor gates the on-chain read too: when the whole set's cached
/// `unredeemed` is below the floor, it returns before
/// [`reconcile_onchain_watermarks`]. The cached paid watermark can only lag the
/// chain, so a fresh read can only lower `unredeemed`, and no chunk after the
/// read can hold more than the whole set. The gate is on the total, not per
/// chunk, because a lane the read drops can shrink the chunk count and re-pack
/// lanes from a failing chunk into one that clears. A sub-floor hint therefore
/// costs no `eth_call`. At `floor == 0` the gate always passes.
///
/// Floors the redeemed watermark first: flushes the lane store durable AFTER the
/// lanes were planned (their cumulative amounts already read) and BEFORE any
/// chunk goes on-chain, so a crash right after a submit still finds on-disk
/// `owed ≥ submitted` (`record` is monotone, so the flush persists at least every
/// value in the batch). The periodic sweep and hint path require this floor and
/// skip their submit on a failed flush (`strict_flush`); the shutdown sweep
/// redeems regardless, since forfeiting the claim across the stop is worse than
/// a bounded re-serve risk.
///
/// Returns whether the cycle hit a chain fault: a failed watermark read or a
/// chunk that did not land. A floor deferral, an empty chunk set and a failed
/// strict flush add no fault of their own; the flush is local to this node.
async fn redeem_planned_lanes<P: Provider + Clone>(
    contract: &PaymentPool::PaymentPoolInstance<P>,
    store: &Arc<dyn PoolStateStore>,
    plans: Vec<PlannedLane>,
    floor: U256,
    max_vouchers: usize,
    strict_flush: bool,
    metrics: &Arc<Metrics>,
) -> bool {
    // Apply the floor on the cached total before any chain read.
    if plans.is_empty() || sum_unredeemed(&plans) < floor {
        return false;
    }
    // Last check before spending gas: reconcile against the on-chain watermark
    // and drop lanes the chain already shows settled, so a lagging cache or a
    // concurrent redeemer can never cost a no-op `redeemMany`.
    let (plans, read_failed) = reconcile_onchain_watermarks(contract, plans, metrics).await;
    let chunks = chunk_redemptions(plans, floor, max_vouchers);
    if chunks.is_empty() {
        return read_failed;
    }
    let span = tracing::info_span!("redeem_cycle", chunks = chunks.len(), strict_flush);
    async move {
        if !flush_store_durable(store, metrics, strict_flush).await && strict_flush {
            return read_failed;
        }
        let mut faulted = read_failed;
        for chunk in chunks {
            faulted |= submit_chunk(contract, store, chunk, metrics, 0).await;
        }
        faulted
    }
    .instrument(span)
    .await
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
mod tests;
