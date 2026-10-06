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
use crate::pool_view::{Lifecycle, PoolProjection, PoolStatus, SignerAuthorization};

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
        redeem_sweep(
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
/// A lane whose hint-path redemption hits a chain fault is parked until the
/// next sweep: each later hint for it is dropped and counted into
/// `redeem_hints_parked`, so an RPC outage costs one failed attempt per lane
/// per interval, not one per voucher. Every sweep clears the parked set before
/// it runs, and the sweep itself retries the parked lanes. The set holds at
/// most one entry per lane.
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
                        parked.insert(key);
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
        // the next tick retries the scan. Every sweep releases the parked
        // lanes, so their hints redeem again after it.
        parked.clear();
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
        redeem_sweep(
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
) {
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
    // A sweep is the retry for every lane, so it parks nothing and drops the
    // chain-fault flag.
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
    true
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
/// chunk that did not land. A set the floor defers, a set with nothing left to
/// submit, and a failed strict flush are not chain faults. A failed flush is
/// local to this node, so it does not count.
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
mod tests {
    use super::*;

    fn boot_retries(metrics: &Metrics) -> u64 {
        let text = metrics.encode().unwrap_or_default();
        text.lines()
            .find_map(|l| l.strip_prefix("decdn_chain_boot_read_retries_total "))
            .and_then(|v| v.parse().ok())
            .unwrap_or(u64::MAX)
    }

    /// A transient error on the `usdc()` self-check is retried, not fatal.
    #[tokio::test(start_paused = true)]
    async fn a_transient_usdc_self_check_error_is_retried() {
        use crate::chain_events::boot_retry::BOOT_CHAIN_RETRY_BUDGET;
        use alloy::providers::mock::Asserter;
        use alloy::sol_types::SolValue;

        let usdc = Address::repeat_byte(0x33);
        let asserter = Asserter::new();
        asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
        asserter.push_success(&alloy::primitives::Bytes::from(usdc.abi_encode()));
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);
        let metrics = Arc::new(Metrics::new());

        let got = usdc_self_check(
            &PaymentPool::new(Address::repeat_byte(0x11), provider),
            &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
        )
        .await;

        assert_eq!(got.ok(), Some(usdc));
        assert_eq!(boot_retries(&metrics), 1);
    }

    /// No contract at the configured address fails the self-check at once.
    #[tokio::test(start_paused = true)]
    async fn a_missing_payment_pool_fails_the_self_check_at_once() {
        use crate::chain_events::boot_retry::BOOT_CHAIN_RETRY_BUDGET;
        use alloy::providers::mock::Asserter;

        let asserter = Asserter::new();
        asserter.push_success(&alloy::primitives::Bytes::new());
        let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);
        let metrics = Arc::new(Metrics::new());
        let start = tokio::time::Instant::now();

        let err = usdc_self_check(
            &PaymentPool::new(Address::repeat_byte(0x11), provider),
            &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
        )
        .await
        .err()
        .map(|e| format!("{e:#}"))
        .unwrap_or_default();

        assert!(err.contains("not retried"), "{err}");
        assert_eq!(boot_retries(&metrics), 0);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

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

    /// An instant `age` in the past, for the freshness gate.
    fn aged(age: Duration) -> Instant {
        Instant::now().checked_sub(age).unwrap_or_else(Instant::now)
    }

    /// An instant past the fault window but inside the verdict window.
    fn between_windows() -> Instant {
        aged(RESOLVE_FAULT_TTL + Duration::from_secs(1))
    }

    /// An instant past both windows.
    fn stale_instant() -> Instant {
        aged(RESOLVE_VERDICT_TTL + Duration::from_secs(1))
    }

    #[test]
    fn negative_cache_hit_only_for_a_fresh_entry() {
        let mut cache = HashMap::new();
        let id = B256::repeat_byte(0x33);
        assert_eq!(
            negative_cache_hit(&cache, id),
            None,
            "an absent id is not a hit"
        );
        for reason in [NegativeReason::Verdict, NegativeReason::Fault] {
            remember_negative(&mut cache, id, reason);
            assert_eq!(
                negative_cache_hit(&cache, id),
                Some(reason),
                "a just-recorded id is a hit that names its reason"
            );
            cache.insert(id, (reason, stale_instant()));
            assert_eq!(
                negative_cache_hit(&cache, id),
                None,
                "an entry past its window is re-checked, not suppressed"
            );
        }
    }

    /// A fault lapses long before a verdict: an entry of the same age suppresses
    /// a repeat `getPool` as a verdict but not as a fault.
    #[test]
    fn negative_cache_fault_lapses_before_a_verdict() {
        let mut cache = HashMap::new();
        let id = B256::repeat_byte(0x34);
        cache.insert(id, (NegativeReason::Fault, between_windows()));
        assert_eq!(
            negative_cache_hit(&cache, id),
            None,
            "a fault past its short window is re-checked"
        );
        cache.insert(id, (NegativeReason::Verdict, between_windows()));
        assert_eq!(
            negative_cache_hit(&cache, id),
            Some(NegativeReason::Verdict),
            "a verdict of the same age still suppresses"
        );
    }

    #[test]
    fn remember_negative_prunes_expired_entries_at_the_cap() {
        let mut cache = HashMap::new();
        // Fill to the cap with stale entries, then record one more: the insert
        // prunes the expired ones instead of growing past the cap.
        for i in 0..RESOLVE_NEGATIVE_CACHE_MAX {
            let id = B256::from(U256::from(i).to_be_bytes::<32>());
            cache.insert(id, (NegativeReason::Verdict, stale_instant()));
        }
        assert_eq!(cache.len(), RESOLVE_NEGATIVE_CACHE_MAX);
        remember_negative(&mut cache, B256::repeat_byte(0xff), NegativeReason::Verdict);
        assert_eq!(
            cache.len(),
            1,
            "the cap-prune drops every expired entry, leaving only the fresh insert"
        );
    }

    /// The cap-prune applies each entry's own window: a fault past its short
    /// window goes, a verdict of the same age stays.
    #[test]
    fn remember_negative_prunes_each_entry_by_its_reason() {
        let mut cache = HashMap::new();
        let mut verdicts = 0;
        for i in 0..RESOLVE_NEGATIVE_CACHE_MAX {
            let id = B256::from(U256::from(i).to_be_bytes::<32>());
            let reason = if i % 2 == 0 {
                verdicts += 1;
                NegativeReason::Verdict
            } else {
                NegativeReason::Fault
            };
            cache.insert(id, (reason, between_windows()));
        }
        remember_negative(&mut cache, B256::repeat_byte(0xff), NegativeReason::Fault);
        assert_eq!(
            cache.len(),
            verdicts + 1,
            "the prune keeps every in-window verdict and drops every lapsed fault"
        );
        assert!(
            cache
                .values()
                .filter(|(_, at)| at.elapsed() >= RESOLVE_FAULT_TTL)
                .all(|(reason, _)| *reason == NegativeReason::Verdict),
            "no lapsed fault survives the prune"
        );
    }

    /// A cache full of in-window entries skips a new insert rather than grow
    /// past the cap; an existing entry is still refreshed.
    #[test]
    fn remember_negative_skips_the_insert_when_full_of_fresh_entries() {
        let mut cache = HashMap::new();
        for i in 0..RESOLVE_NEGATIVE_CACHE_MAX {
            let id = B256::from(U256::from(i).to_be_bytes::<32>());
            remember_negative(&mut cache, id, NegativeReason::Verdict);
        }
        let newcomer = B256::repeat_byte(0xff);
        remember_negative(&mut cache, newcomer, NegativeReason::Fault);
        assert_eq!(
            cache.len(),
            RESOLVE_NEGATIVE_CACHE_MAX,
            "the map stays at the cap"
        );
        assert_eq!(
            negative_cache_hit(&cache, newcomer),
            None,
            "the newcomer is not cached"
        );

        let resident = B256::from(U256::from(0u8).to_be_bytes::<32>());
        remember_negative(&mut cache, resident, NegativeReason::Fault);
        assert_eq!(
            negative_cache_hit(&cache, resident),
            Some(NegativeReason::Fault),
            "a resident entry is still overwritten"
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
        let view = ResolvingPoolView::new(contract, projection.clone(), Arc::new(Metrics::new()));
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

    /// Admit path (ii, verdict): an absent (`owner == 0`) or `Closed` pool the
    /// `getPool` returns is refused (`None`), never folded, and negative-cached as
    /// a `Verdict`. The entry still suppresses a repeat `getPool` past the fault
    /// window: a queued live answer is left unread and the pool stays refused.
    #[tokio::test]
    async fn resolving_status_refuses_absent_pool_and_caches_negative() -> Result<()> {
        use crate::pool_view::PoolView;
        use alloy::sol_types::SolValue;

        let owner = Address::from([7u8; 20]);
        for (case, dead) in [
            ("absent", pool(Address::ZERO, PaymentPool::Status::Open, 0)),
            ("closed", pool(owner, PaymentPool::Status::Closed, 0)),
        ] {
            let (view, projection, asserter) = mocked_getpool_view(Some(dead));
            let pool_id = B256::repeat_byte(0x55);

            assert!(view.status(pool_id).await.is_none(), "{case}: refused");
            assert!(
                projection.snapshot(pool_id).is_none(),
                "{case}: never folded into the projection"
            );
            let reason = view
                .negative
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&pool_id)
                .map(|(reason, _)| *reason);
            assert_eq!(reason, Some(NegativeReason::Verdict), "{case}: a verdict");

            // Age the entry past the fault window and queue a live answer: a
            // verdict still suppresses, so the answer is never read.
            view.negative
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(pool_id, (NegativeReason::Verdict, between_windows()));
            asserter.push_success(&Bytes::from(
                pool(owner, PaymentPool::Status::Open, 0).abi_encode(),
            ));
            assert!(
                view.status(pool_id).await.is_none(),
                "{case}: still refused"
            );
            assert_eq!(asserter.read_q().len(), 1, "{case}: no second getPool");
        }
        Ok(())
    }

    /// Admit path (ii, fault): a `getPool` RPC error refuses the pool (`None`) and
    /// negative-caches it as a `Fault`, so a re-request flood inside
    /// `RESOLVE_FAULT_TTL` cannot storm `getPool`.
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
        let reason = view
            .negative
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&pool_id)
            .map(|(reason, _)| *reason);
        assert_eq!(
            reason,
            Some(NegativeReason::Fault),
            "an RPC error is negative-cached as a fault"
        );
        Ok(())
    }

    /// A live pool refused on a fault is admitted once the fault window lapses
    /// and the RPC answers: the re-read folds it and drops the negative entry.
    #[tokio::test]
    async fn resolving_status_admits_a_faulted_pool_after_the_rpc_recovers() -> Result<()> {
        use crate::pool_view::PoolView;
        use alloy::sol_types::SolValue;

        let (view, projection, asserter) = mocked_getpool_view(None);
        let pool_id = B256::repeat_byte(0x67);
        let owner = Address::from([9u8; 20]);
        assert!(view.status(pool_id).await.is_none(), "the fault refuses");

        view.negative
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(pool_id, (NegativeReason::Fault, between_windows()));
        asserter.push_success(&Bytes::from(
            pool(owner, PaymentPool::Status::Open, 0).abi_encode(),
        ));
        assert!(view.status(pool_id).await.is_some(), "the re-read admits");
        assert_eq!(
            projection.snapshot(pool_id).map(|s| s.owner),
            Some(owner),
            "the pool is folded"
        );
        assert!(
            !view
                .negative
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&pool_id),
            "the resolve drops the negative entry"
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

    const AUTH_EXPIRY: u64 = 1_900_000_000;

    /// A registered authorization (`expiry` non-zero), or the unregistered
    /// all-zero struct when `cap == 0`.
    fn authz(cap: u64, spent: u64) -> PaymentPool::Authorization {
        PaymentPool::Authorization {
            cap,
            expiry: if cap == 0 { 0 } else { AUTH_EXPIRY },
            spent,
        }
    }

    const fn registered(cap: u64, spent: u64) -> SignerAuthorization {
        SignerAuthorization::Registered {
            cap,
            expiry: AUTH_EXPIRY,
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
        let view =
            ResolvingPoolView::new(contract, PoolProjection::new(), Arc::new(Metrics::new()));
        (view, asserter)
    }

    /// A registered signer reports its registered terms and `spent`; the admit
    /// gate covers a floor up to `cap − spent`.
    #[tokio::test]
    async fn registered_signer_reports_terms_and_spent() -> Result<()> {
        use crate::pool_view::PoolView;

        let (view, _asserter) = mocked_getauth_view(&[authz(1_000, 300)]);
        let auth = view
            .signer_authorization(B256::repeat_byte(0x11), Address::from([2u8; 20]))
            .await
            .ok_or_else(|| anyhow::anyhow!("a registered signer reports its authorization"))?;
        assert_eq!(auth, registered(1_000, 300));
        assert!(
            auth.covers(U256::from(700u64), AUTH_EXPIRY - 1),
            "cap 1000 − spent 300"
        );
        assert!(!auth.covers(U256::from(701u64), AUTH_EXPIRY - 1));
        Ok(())
    }

    /// A signer that has spent its full `cap` covers no floor — the dispatch gate
    /// then refuses it, but the view reports the truth.
    #[tokio::test]
    async fn exhausted_signer_covers_no_floor() -> Result<()> {
        use crate::pool_view::PoolView;

        let (view, _asserter) = mocked_getauth_view(&[authz(1_000, 1_000)]);
        let auth = view
            .signer_authorization(B256::repeat_byte(0x22), Address::from([3u8; 20]))
            .await
            .ok_or_else(|| anyhow::anyhow!("an exhausted signer still reports a value"))?;
        assert_eq!(auth, registered(1_000, 1_000));
        assert!(
            !auth.covers(U256::from(1u64), AUTH_EXPIRY - 1),
            "spent == cap"
        );
        Ok(())
    }

    /// A registered signer whose registration has expired covers no floor, even
    /// with headroom left: the chain redeems nothing at or past the expiry.
    #[test]
    fn expired_registration_covers_no_floor() {
        let auth = registered(1_000, 0);
        assert!(auth.covers(U256::from(1u64), AUTH_EXPIRY - 1));
        assert!(!auth.covers(U256::from(1u64), AUTH_EXPIRY));
    }

    /// An unregistered signer (`cap == 0 && expiry == 0`) is unconstrained — it
    /// has spent nothing on-chain and admits on its presented capability — and a
    /// second call within [`UNREGISTERED_AUTH_TTL`] is served from cache with no
    /// further `getAuthorization`.
    #[tokio::test]
    async fn unregistered_signer_is_unconstrained_and_cached() -> Result<()> {
        use crate::pool_view::PoolView;

        // Only ONE response queued: a second on-chain read would error → None.
        let (view, asserter) = mocked_getauth_view(&[authz(0, 0)]);
        let pool_id = B256::repeat_byte(0x33);
        let signer = Address::from([4u8; 20]);

        let first = view
            .signer_authorization(pool_id, signer)
            .await
            .ok_or_else(|| anyhow::anyhow!("an unregistered signer is unconstrained"))?;
        assert_eq!(first, SignerAuthorization::Unregistered);
        assert!(first.covers(U256::MAX, u64::MAX), "no on-chain constraint");
        assert_eq!(
            asserter.read_q().len(),
            0,
            "exactly one getAuthorization was consumed"
        );

        let second = view
            .signer_authorization(pool_id, signer)
            .await
            .ok_or_else(|| anyhow::anyhow!("a fresh cache entry serves the second call"))?;
        assert_eq!(
            second,
            SignerAuthorization::Unregistered,
            "the cached read is returned unchanged"
        );
        assert_eq!(
            asserter.read_q().len(),
            0,
            "no second getAuthorization was issued within the TTL"
        );
        Ok(())
    }

    /// A REGISTERED signer with a zero `spendingCap` (`cap == 0` but `expiry != 0`)
    /// is NOT the all-zero unregistered struct: it is registered with no headroom,
    /// so the dispatch gate refuses it — it is not misread as unconstrained, which
    /// would fail open and admit an uncashable signer.
    #[tokio::test]
    async fn registered_zero_cap_signer_is_refused_not_unconstrained() -> Result<()> {
        use crate::pool_view::PoolView;

        let auth = PaymentPool::Authorization {
            cap: 0,
            expiry: AUTH_EXPIRY,
            spent: 0,
        };
        let (view, _asserter) = mocked_getauth_view(&[auth]);
        let auth = view
            .signer_authorization(B256::repeat_byte(0x44), Address::from([5u8; 20]))
            .await
            .ok_or_else(|| anyhow::anyhow!("a registered zero-cap signer reports a value"))?;
        assert_eq!(
            auth,
            registered(0, 0),
            "cap == 0 with expiry != 0 is a registered zero-cap signer, not unregistered"
        );
        assert!(!auth.covers(U256::from(1u64), AUTH_EXPIRY - 1));
        Ok(())
    }

    /// A `getAuthorization` RPC fault for a signer this node has never read
    /// returns `None`, so the caller refuses rather than fail open.
    #[tokio::test]
    async fn getauthorization_fault_refuses_signer() -> Result<()> {
        use crate::pool_view::PoolView;

        // No response queued: the mocked eth_call errors.
        let (view, _asserter) = mocked_getauth_view(&[]);
        assert!(
            view.signer_authorization(B256::repeat_byte(0x44), Address::from([5u8; 20]))
                .await
                .is_none(),
            "a fault with no cached read refuses the signer"
        );
        Ok(())
    }

    /// A `Registered` read answers however old it is: the second call finds an
    /// empty response queue, so a chain read would fault and refuse.
    #[tokio::test]
    async fn a_registered_read_never_ages_out() -> Result<()> {
        use crate::pool_view::PoolView;

        let pool_id = B256::repeat_byte(0x45);
        let signer = Address::from([6u8; 20]);
        let (view, _asserter) = mocked_getauth_view(&[authz(1_000_000, 200_000)]);
        assert_eq!(
            view.signer_authorization(pool_id, signer).await,
            Some(registered(1_000_000, 200_000))
        );
        age_auth_read(&view, pool_id, signer)?;
        assert_eq!(
            view.signer_authorization(pool_id, signer).await,
            Some(registered(1_000_000, 200_000)),
            "the old registered read answers with no chain read"
        );
        assert_eq!(signer_auth_counts(&view.metrics), (1, 1, 0));
        Ok(())
    }

    /// The admit signer-confirm counts as `(cached, first_read, reread)`, read
    /// from the scrape text.
    fn signer_auth_counts(metrics: &Metrics) -> (u64, u64, u64) {
        let text = metrics.encode().unwrap_or_default();
        let value = |name: &str| {
            text.lines()
                .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.parse::<u64>().ok())
                .unwrap_or(u64::MAX)
        };
        (
            value("decdn_serve_signer_auth_cached_total"),
            value("decdn_serve_signer_auth_first_read_total"),
            value("decdn_serve_signer_auth_reread_total"),
        )
    }

    /// Backdate the held read of `(pool_id, signer)` past
    /// [`UNREGISTERED_AUTH_TTL`].
    fn age_auth_read(
        view: &ResolvingPoolView<impl Provider + Clone>,
        pool_id: B256,
        signer: Address,
    ) -> Result<()> {
        let mut guard = view
            .auth_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = guard
            .get_mut(&(pool_id, signer))
            .ok_or_else(|| anyhow::anyhow!("the first read is cached"))?;
        entry.at = Instant::now()
            .checked_sub(UNREGISTERED_AUTH_TTL * 2)
            .ok_or_else(|| anyhow::anyhow!("clock too close to its epoch"))?;
        Ok(())
    }

    /// In-memory sink for the tracing output a test captures.
    #[derive(Clone, Default)]
    struct CapturedLog(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl CapturedLog {
        /// Capture this thread's tracing output until the guard drops. A
        /// paused-clock test runs on one thread, so the thread default sees
        /// every event the view logs.
        fn install(&self) -> tracing::subscriber::DefaultGuard {
            let writer = self.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .finish();
            tracing::subscriber::set_default(subscriber)
        }

        /// Every captured line that contains `needle`, or an error that shows
        /// the whole log when none does.
        fn lines(&self, needle: &str) -> Result<Vec<String>> {
            let bytes = self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            let text = String::from_utf8(bytes)?;
            let found: Vec<String> = text
                .lines()
                .filter(|l| l.contains(needle))
                .map(str::to_owned)
                .collect();
            if found.is_empty() {
                anyhow::bail!("no captured line contains {needle:?}:\n{text}");
            }
            Ok(found)
        }
    }

    /// A view whose every chain read hangs forever. Pair with
    /// `#[tokio::test(start_paused = true)]` so the `timed` bound fires on
    /// virtual time.
    fn hanging_view() -> ResolvingPoolView<impl Provider + Clone + 'static> {
        counting_hanging_view().0
    }

    /// A [`hanging_view`] plus a count of the chain reads dispatched to it.
    fn counting_hanging_view() -> (
        ResolvingPoolView<impl Provider + Clone + 'static>,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let (provider, calls) = crate::chain_events::test_support::counting_hanging_provider();
        let view = ResolvingPoolView::new(
            PaymentPool::new(Address::ZERO, provider),
            PoolProjection::new(),
            Arc::new(Metrics::new()),
        );
        (view, calls)
    }

    /// A hung admit `getPool` times out and takes the fault path: the pool is
    /// refused and negative-cached as a fault, so a request inside
    /// `RESOLVE_FAULT_TTL` refuses at once instead of waiting again. The WARN
    /// carries the timeout and the fault window.
    #[tokio::test(start_paused = true)]
    async fn admit_getpool_hang_refuses_within_the_bound() -> Result<()> {
        use crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT;
        use crate::chain_events::test_support::bounded;
        use crate::pool_view::PoolView;

        let log = CapturedLog::default();
        let _subscriber = log.install();
        let view = hanging_view();
        let pool_id = B256::repeat_byte(0x51);
        let started = tokio::time::Instant::now();
        assert!(
            bounded("admit getPool", view.status(pool_id))
                .await
                .is_none()
        );
        assert_eq!(
            started.elapsed(),
            DEFAULT_RPC_CALL_TIMEOUT,
            "the admit read uses the default bound"
        );
        let reason = view
            .negative
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&pool_id)
            .map(|(reason, _)| *reason);
        assert_eq!(
            reason,
            Some(NegativeReason::Fault),
            "the timed-out pool is negative-cached as a fault"
        );
        assert!(
            bounded("admit getPool", view.status(pool_id))
                .await
                .is_none(),
            "the negative cache refuses the next request"
        );
        assert_eq!(
            started.elapsed(),
            DEFAULT_RPC_CALL_TIMEOUT,
            "the next request does not wait on the chain again"
        );
        let warns = log.lines("admit getPool failed")?;
        assert_eq!(warns.len(), 1, "only the first request reads: {warns:?}");
        let line = warns.first().map_or("", String::as_str);
        assert!(line.contains("WARN"), "{line}");
        assert!(
            line.contains("admit getPool timed out after 10s"),
            "the WARN keeps the failure class: {line}"
        );
        assert!(
            line.contains("suppressed_for=5s"),
            "the WARN names the fault window: {line}"
        );
        Ok(())
    }

    /// Concurrent requests for one pool share one in-flight `getPool`: they all
    /// end when the single hung read times out, and only that read reaches the
    /// RPC.
    #[tokio::test(start_paused = true)]
    async fn admit_getpool_coalesces_concurrent_reads() {
        use crate::chain_events::test_support::bounded;
        use crate::pool_view::PoolView;
        use std::sync::atomic::Ordering;

        let (view, calls) = counting_hanging_view();
        let pool_id = B256::repeat_byte(0x55);
        let started = tokio::time::Instant::now();
        let answers = bounded(
            "admit getPool",
            futures_util::future::join_all((0..8).map(|_| view.status(pool_id))),
        )
        .await;
        assert!(
            answers.iter().all(Option::is_none),
            "every request is refused"
        );
        assert_eq!(
            started.elapsed(),
            crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT,
            "the waiters end with the one read, not after reads of their own"
        );
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "one getPool for eight requests"
        );
        assert!(
            view.inflight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty(),
            "the ended read leaves no in-flight entry"
        );
    }

    /// With the negative cache full of in-window entries, the read's fault is
    /// not cached; its waiters still take the read's answer instead of each
    /// starting a read of its own in turn.
    #[tokio::test(start_paused = true)]
    async fn admit_getpool_waiters_take_the_answer_when_the_cache_is_full() {
        use crate::chain_events::test_support::bounded;
        use crate::pool_view::PoolView;
        use std::sync::atomic::Ordering;

        let (view, calls) = counting_hanging_view();
        {
            let mut negative = view
                .negative
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for i in 0..RESOLVE_NEGATIVE_CACHE_MAX {
                let id = B256::from(U256::from(i).to_be_bytes::<32>());
                remember_negative(&mut negative, id, NegativeReason::Verdict);
            }
        }
        let pool_id = B256::repeat_byte(0xfe);
        let started = tokio::time::Instant::now();
        let answers = bounded(
            "admit getPool",
            futures_util::future::join_all((0..8).map(|_| view.status(pool_id))),
        )
        .await;
        assert!(
            answers.iter().all(Option::is_none),
            "every request is refused"
        );
        assert_eq!(
            started.elapsed(),
            crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT,
            "the waiters end with the one read"
        );
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "one getPool for eight requests"
        );
        assert!(
            !view
                .negative
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&pool_id),
            "the full cache did not record the fault"
        );
    }

    /// A reader that is cancelled mid-read ends the read for its waiters: one of
    /// them takes over and issues its own `getPool`.
    #[tokio::test(start_paused = true)]
    async fn admit_getpool_waiter_takes_over_a_cancelled_read() {
        use crate::chain_events::test_support::bounded;
        use crate::pool_view::PoolView;
        use std::sync::atomic::Ordering;

        let (view, calls) = counting_hanging_view();
        let pool_id = B256::repeat_byte(0x56);
        let cancel_after = Duration::from_secs(1);
        let started = tokio::time::Instant::now();
        let (reader, waiter) = bounded("admit getPool", async {
            tokio::join!(
                tokio::time::timeout(cancel_after, view.status(pool_id)),
                view.status(pool_id)
            )
        })
        .await;
        assert!(reader.is_err(), "the first reader is cancelled");
        assert!(waiter.is_none(), "the waiter's own read times out");
        assert_eq!(
            started.elapsed(),
            cancel_after + crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT,
            "the waiter reads from the cancellation on"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 2, "the waiter issued a read");
    }

    /// A pool negative-cached for a fault is read again once the short fault
    /// window lapses, while a verdict of the same age still suppresses the read.
    /// The cache stamps each entry with a `std::time::Instant`, which the paused
    /// tokio clock does not advance, so each entry is backdated rather than
    /// waited out.
    #[tokio::test(start_paused = true)]
    async fn admit_getpool_rereads_a_faulted_pool_after_the_fault_window() {
        use crate::chain_events::test_support::bounded;
        use crate::pool_view::PoolView;

        let view = hanging_view();
        let pool_id = B256::repeat_byte(0x54);
        let backdate = |reason| {
            view.negative
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(pool_id, (reason, between_windows()));
        };

        backdate(NegativeReason::Fault);
        let started = tokio::time::Instant::now();
        assert!(
            bounded("admit getPool", view.status(pool_id))
                .await
                .is_none()
        );
        assert_eq!(
            started.elapsed(),
            crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT,
            "a lapsed fault re-reads the pool (and waits out the hung read)"
        );

        backdate(NegativeReason::Verdict);
        let started = tokio::time::Instant::now();
        assert!(
            bounded("admit getPool", view.status(pool_id))
                .await
                .is_none()
        );
        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "a verdict of the same age suppresses the read"
        );
    }

    /// A held `Registered` read answers at once against a hung RPC: the admit
    /// path sends no read for it.
    #[tokio::test(start_paused = true)]
    async fn a_registered_read_answers_without_reaching_a_hung_rpc() -> Result<()> {
        use crate::chain_events::test_support::bounded;
        use crate::pool_view::PoolView;
        use std::sync::atomic::Ordering;

        let (view, calls) = counting_hanging_view();
        let pool_id = B256::repeat_byte(0x52);
        let signer = Address::from([7u8; 20]);
        let aged = Instant::now()
            .checked_sub(UNREGISTERED_AUTH_TTL * 2)
            .ok_or_else(|| anyhow::anyhow!("clock too close to its epoch"))?;
        view.auth_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                (pool_id, signer),
                AuthRead {
                    auth: registered(1_000_000, 200_000),
                    folded_at_read: 0,
                    at: aged,
                },
            );
        let started = tokio::time::Instant::now();
        assert_eq!(
            bounded(
                "admit getAuthorization",
                view.signer_authorization(pool_id, signer)
            )
            .await,
            Some(registered(1_000_000, 200_000)),
        );
        assert_eq!(started.elapsed(), Duration::ZERO, "no wait on the chain");
        assert_eq!(calls.load(Ordering::Relaxed), 0, "no getAuthorization sent");
        Ok(())
    }

    /// Concurrent confirms of one `(pool, signer)` share one in-flight
    /// `getAuthorization`: they all end when the single hung read times out,
    /// and only that read reaches the RPC.
    #[tokio::test(start_paused = true)]
    async fn admit_getauthorization_coalesces_concurrent_reads() {
        use crate::chain_events::test_support::bounded;
        use crate::pool_view::PoolView;
        use std::sync::atomic::Ordering;

        let (view, calls) = counting_hanging_view();
        let pool_id = B256::repeat_byte(0x56);
        let signer = Address::from([9u8; 20]);
        let started = tokio::time::Instant::now();
        let answers = bounded(
            "admit getAuthorization",
            futures_util::future::join_all(
                (0..8).map(|_| view.signer_authorization(pool_id, signer)),
            ),
        )
        .await;
        assert!(
            answers.iter().all(Option::is_none),
            "every confirm is refused"
        );
        assert_eq!(
            started.elapsed(),
            crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT,
            "the waiters end with the one read, not after reads of their own"
        );
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "one getAuthorization for eight confirms"
        );
        assert_eq!(signer_auth_counts(&view.metrics), (0, 8, 0));
        assert!(
            view.auth_inflight
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty(),
            "the ended read leaves no in-flight entry"
        );
    }

    /// A hung admit `getAuthorization` with no cached read times out and refuses
    /// the signer.
    #[tokio::test(start_paused = true)]
    async fn admit_getauthorization_hang_with_no_cached_read_refuses() -> Result<()> {
        use crate::chain_events::test_support::bounded;
        use crate::pool_view::PoolView;

        let log = CapturedLog::default();
        let _subscriber = log.install();
        let view = hanging_view();
        let pool_id = B256::repeat_byte(0x53);
        let signer = Address::from([8u8; 20]);
        assert_eq!(
            bounded(
                "admit getAuthorization",
                view.signer_authorization(pool_id, signer)
            )
            .await,
            None
        );
        // A fault caches nothing: a cached `Unregistered` here would answer the
        // fast path for a full TTL.
        assert!(
            view.auth_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty(),
            "a timed-out read leaves the signer cache empty"
        );
        let warns = log.lines("refusing this signer")?;
        let line = warns.first().map_or("", String::as_str);
        assert!(line.contains("WARN"), "{line}");
        assert!(
            line.contains("admit getAuthorization timed out after 10s"),
            "the WARN keeps the failure class: {line}"
        );
        Ok(())
    }

    /// A held `Registered` read adds what the projection folds for the signer
    /// after the read, from any provider, so a signer that drains its shared cap
    /// at other nodes is not admitted on the old headroom.
    #[tokio::test]
    async fn a_registered_read_adds_the_spent_folded_since() {
        use crate::pool_view::PoolView;

        let pool_id = B256::repeat_byte(0x47);
        let signer = Address::from([8u8; 20]);
        let (view, _asserter) = mocked_getauth_view(&[authz(1_000_000, 200_000)]);
        let lane = |paid: u64| PaymentPool::LaneSettled {
            signer,
            newPaidCumulative: paid,
            bytesPaid: 0,
        };
        view.projection
            .record_opened(pool_id, Address::from([1u8; 20]), U256::from(5_000_000u64));
        // Folded before the read: the read's `spent` already holds it.
        view.projection
            .record_redeemed(pool_id, Address::from([9u8; 20]), &[lane(200_000)]);
        assert_eq!(
            view.signer_authorization(pool_id, signer).await,
            Some(registered(1_000_000, 200_000))
        );
        view.projection
            .record_redeemed(pool_id, Address::from([10u8; 20]), &[lane(650_000)]);
        assert_eq!(
            view.signer_authorization(pool_id, signer).await,
            Some(registered(1_000_000, 850_000)),
            "a drain folded since the read adds to spent"
        );
    }

    /// A fault after a cached `Unregistered` read has aged past the TTL refuses
    /// the signer: a registration may have landed since, with its cap spent.
    #[tokio::test]
    async fn an_expired_unregistered_read_does_not_answer_a_fault() -> Result<()> {
        use crate::pool_view::PoolView;

        let pool_id = B256::repeat_byte(0x46);
        let signer = Address::from([7u8; 20]);
        let (view, _asserter) = mocked_getauth_view(&[authz(0, 0)]);
        assert_eq!(
            view.signer_authorization(pool_id, signer).await,
            Some(SignerAuthorization::Unregistered)
        );
        age_auth_read(&view, pool_id, signer)?;
        assert_eq!(
            view.signer_authorization(pool_id, signer).await,
            None,
            "an old Unregistered read is not trusted after a fault"
        );
        assert_eq!(signer_auth_counts(&view.metrics), (0, 1, 1));
        Ok(())
    }

    /// An `Unregistered` read lapses inside its window once the projection folds
    /// a redemption by the signer: that redemption registered it.
    #[tokio::test]
    async fn a_folded_redemption_lapses_an_unregistered_read() {
        use crate::pool_view::PoolView;

        let pool_id = B256::repeat_byte(0x48);
        let signer = Address::from([11u8; 20]);
        let (view, asserter) = mocked_getauth_view(&[authz(0, 0), authz(1_000_000, 50_000)]);
        assert_eq!(
            view.signer_authorization(pool_id, signer).await,
            Some(SignerAuthorization::Unregistered)
        );
        view.projection
            .record_opened(pool_id, Address::from([1u8; 20]), U256::from(5_000_000u64));
        view.projection.record_redeemed(
            pool_id,
            Address::from([9u8; 20]),
            &[PaymentPool::LaneSettled {
                signer,
                newPaidCumulative: 50_000,
                bytesPaid: 0,
            }],
        );
        assert_eq!(
            view.signer_authorization(pool_id, signer).await,
            Some(registered(1_000_000, 50_000)),
            "the fold sends a re-read, which finds the registration"
        );
        assert_eq!(asserter.read_q().len(), 0, "both reads were sent");
        assert_eq!(
            view.signer_authorization(pool_id, signer).await,
            Some(registered(1_000_000, 50_000)),
        );
        assert_eq!(signer_auth_counts(&view.metrics), (1, 1, 1));
    }

    fn auth_read(auth: SignerAuthorization, folded_at_read: u64, age: Duration) -> AuthRead {
        AuthRead {
            auth,
            folded_at_read,
            at: Instant::now().checked_sub(age).unwrap_or_else(Instant::now),
        }
    }

    #[test]
    fn cached_auth_classifies_each_read() {
        let fresh_unreg = auth_read(SignerAuthorization::Unregistered, 10, Duration::ZERO);
        assert_eq!(cached_auth(None, 0), CachedAuth::Absent);
        assert_eq!(
            cached_auth(Some(&fresh_unreg), 10),
            CachedAuth::Fresh(SignerAuthorization::Unregistered)
        );
        assert_eq!(
            cached_auth(Some(&fresh_unreg), 11),
            CachedAuth::Lapsed,
            "a fold since the read lapses an Unregistered read"
        );
        let old_unreg = auth_read(
            SignerAuthorization::Unregistered,
            10,
            UNREGISTERED_AUTH_TTL * 2,
        );
        assert_eq!(cached_auth(Some(&old_unreg), 10), CachedAuth::Lapsed);
        let old_reg = auth_read(registered(1_000, 100), 40, UNREGISTERED_AUTH_TTL * 2);
        assert_eq!(
            cached_auth(Some(&old_reg), 65),
            CachedAuth::Fresh(registered(1_000, 125)),
            "a registered read adds the fold since the read"
        );
        assert_eq!(
            cached_auth(Some(&old_reg), 0),
            CachedAuth::Fresh(registered(1_000, 100)),
            "a fold below the baseline adds nothing"
        );
    }

    #[test]
    fn remember_auth_prunes_lapsed_then_evicts_the_oldest() {
        let key = |i: usize| {
            let mut bytes = [0u8; 20];
            bytes[..8].copy_from_slice(&(i as u64).to_be_bytes());
            (B256::ZERO, Address::from(bytes))
        };
        let mut cache = HashMap::new();
        let oldest = auth_read(registered(1, 0), 0, Duration::from_secs(10));
        cache.insert(key(0), oldest);
        for i in 1..AUTH_CACHE_MAX {
            cache.insert(key(i), auth_read(registered(1, 0), 0, Duration::ZERO));
        }
        let lapsed = auth_read(
            SignerAuthorization::Unregistered,
            0,
            UNREGISTERED_AUTH_TTL * 2,
        );
        cache.insert(key(1), lapsed);
        let fresh = auth_read(registered(1, 0), 0, Duration::ZERO);

        remember_auth(&mut cache, key(AUTH_CACHE_MAX), fresh);
        assert_eq!(cache.len(), AUTH_CACHE_MAX, "the lapsed read made room");
        assert!(!cache.contains_key(&key(1)));
        assert!(cache.contains_key(&key(0)), "nothing else was evicted");

        remember_auth(&mut cache, key(AUTH_CACHE_MAX + 1), fresh);
        assert_eq!(cache.len(), AUTH_CACHE_MAX);
        assert!(!cache.contains_key(&key(0)), "the oldest read was evicted");

        remember_auth(&mut cache, key(2), fresh);
        assert_eq!(cache.len(), AUTH_CACHE_MAX, "a held key replaces in place");
    }

    /// A second call for a `Registered` signer is served from cache, consuming no
    /// further `getAuthorization` (proven by the single queued response and a
    /// `Some` result on the second call).
    #[tokio::test]
    async fn signer_authorization_second_call_hits_cache() -> Result<()> {
        use crate::pool_view::PoolView;

        let (view, asserter) = mocked_getauth_view(&[authz(1_000, 200)]);
        let pool_id = B256::repeat_byte(0x55);
        let signer = Address::from([6u8; 20]);

        let first = view
            .signer_authorization(pool_id, signer)
            .await
            .ok_or_else(|| anyhow::anyhow!("the first call resolves on-chain"))?;
        assert_eq!(first, registered(1_000, 200));
        assert_eq!(asserter.read_q().len(), 0, "one getAuthorization consumed");

        let second = view
            .signer_authorization(pool_id, signer)
            .await
            .ok_or_else(|| anyhow::anyhow!("the second call is served from cache"))?;
        assert_eq!(
            second,
            registered(1_000, 200),
            "the cached read is returned"
        );
        assert_eq!(
            asserter.read_q().len(),
            0,
            "a registered read answers with no second getAuthorization"
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

    /// A funded `Closing` pool stays redeemable while its deadline lies more than
    /// the landing slack ahead.
    #[test]
    fn closing_funded_before_the_landing_slack_is_redeemable() {
        assert!(pool_is_redeemable(
            Some(status(500, Lifecycle::Closing { deadline: 2_000 })),
            2_000 - REDEEM_LANDING_SLACK_SECS - 1
        ));
    }

    /// A funded `Closing` pool whose deadline falls within the landing slack is
    /// dropped: the batch could land past the deadline and revert `PoolClosed`.
    #[test]
    fn closing_funded_within_the_landing_slack_is_dropped() {
        assert!(!pool_is_redeemable(
            Some(status(500, Lifecycle::Closing { deadline: 2_000 })),
            2_000 - REDEEM_LANDING_SLACK_SECS
        ));
        assert!(!pool_is_redeemable(
            Some(status(500, Lifecycle::Closing { deadline: 2_000 })),
            1_999
        ));
    }

    /// A `Closing` deadline near `u64::MAX` saturates rather than wraps.
    #[test]
    fn closing_slack_saturates_at_the_top_of_the_clock() {
        assert!(!pool_is_redeemable(
            Some(status(500, Lifecycle::Closing { deadline: u64::MAX })),
            u64::MAX - 1
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
            owed: U256::from(unredeemed),
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

    /// Sum a chunk's unredeemed values, for `chunk_redemptions` tests. Delegates
    /// to the production [`sum_unredeemed`] so the tests exercise the same
    /// summation that feeds the `decdn_unredeemed_usdc` gauge.
    fn total_unredeemed(chunk: &[PlannedLane]) -> U256 {
        sum_unredeemed(chunk)
    }

    #[test]
    fn sum_unredeemed_totals_planned_lanes() {
        // Empty set is zero — the gauge reads 0 when nothing is owed.
        assert_eq!(sum_unredeemed(&[]), U256::ZERO);
        let plans = vec![
            planned(1, 10, 100, false),
            planned(1, 11, 250, false),
            planned(2, 12, 1_000_000, true),
        ];
        assert_eq!(sum_unredeemed(&plans), U256::from(1_000_350u64));
    }

    #[test]
    fn reconcile_plans_drops_fully_settled_and_keeps_remainder() {
        // Three lanes, all owed 1000. On-chain: lane 0 already settled to 1000
        // (drop), lane 1 partially at 600 (keep, remainder 400), lane 2 never
        // redeemed at 0 (keep, remainder 1000).
        let mut plans = vec![
            planned(1, 0, 1_000, false),
            planned(1, 1, 1_000, false),
            planned(1, 2, 1_000, false),
        ];
        for p in &mut plans {
            p.owed = U256::from(1_000u64);
        }
        let onchain = [U256::from(1_000u64), U256::from(600u64), U256::from(0u64)];
        let (kept, skipped) = reconcile_plans(plans, &onchain);
        assert_eq!(skipped, 1, "the fully-settled lane is dropped");
        assert_eq!(kept.len(), 2);
        // Survivors carry the recomputed remainder, not the stale full owed.
        assert_eq!(kept.first().map(|p| p.unredeemed), Some(U256::from(400u64)));
        assert_eq!(
            kept.get(1).map(|p| p.unredeemed),
            Some(U256::from(1_000u64))
        );
    }

    #[test]
    fn reconcile_plans_drops_when_onchain_exceeds_owed() {
        // A watermark strictly above owed (a fresher voucher already redeemed
        // elsewhere) still settles this claim to zero — drop it.
        let mut plans = vec![planned(1, 0, 1_000, false)];
        if let Some(p) = plans.first_mut() {
            p.owed = U256::from(1_000u64);
        }
        let (kept, skipped) = reconcile_plans(plans, &[U256::from(5_000u64)]);
        assert_eq!(skipped, 1);
        assert!(kept.is_empty());
    }

    #[test]
    fn reconcile_plans_short_slice_keeps_untouched_tail() {
        // Fail-open parity guard: a slice shorter than the plans keeps the
        // unread tail unchanged rather than mis-pairing.
        let plans = vec![planned(1, 0, 1_000, false), planned(1, 1, 1_000, false)];
        let (kept, skipped) = reconcile_plans(plans, &[U256::from(1_000u64)]);
        assert_eq!(skipped, 1, "the read lane (settled) is dropped");
        assert_eq!(kept.len(), 1, "the unread lane is kept unchanged");
        assert_eq!(
            kept.first().map(|p| p.unredeemed),
            Some(U256::from(1_000u64))
        );
    }

    /// A mocked `PaymentPool` whose `eth_call` queue returns one ABI-encoded
    /// `getWatermarks` result per entry — one entry per expected batch, each
    /// holding that batch's lanes.
    ///
    /// Encoded with `getWatermarksCall::abi_encode_returns`, not `SolValue`: the
    /// return is a *dynamic* array, so unlike the static-tuple `Pool` /
    /// `Authorization` fixtures elsewhere in this file, the standalone
    /// value encoding is not the function-return encoding (the head carries an
    /// offset word).
    fn mocked_getwatermarks_pool(
        responses: &[Vec<PaymentPool::Lane>],
    ) -> (
        PaymentPool::PaymentPoolInstance<impl Provider + Clone + 'static>,
        alloy::providers::mock::Asserter,
    ) {
        use alloy::providers::ProviderBuilder;
        use alloy::providers::mock::Asserter;
        use alloy::sol_types::SolCall;

        let asserter = Asserter::new();
        for lanes in responses {
            asserter.push_success(&Bytes::from(
                PaymentPool::getWatermarksCall::abi_encode_returns(lanes),
            ));
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        (PaymentPool::new(Address::ZERO, provider), asserter)
    }

    fn lane(amount: u64) -> PaymentPool::Lane {
        PaymentPool::Lane {
            amount,
            bytesDelivered: 0,
        }
    }

    /// A mocked `PaymentPool` whose `eth_call` queue answers one
    /// `getAuthorizations` per entry of `responses`.
    fn mocked_getauthorizations_pool(
        responses: &[Vec<PaymentPool::Authorization>],
    ) -> PaymentPool::PaymentPoolInstance<impl Provider + Clone + 'static> {
        use alloy::providers::ProviderBuilder;
        use alloy::providers::mock::Asserter;
        use alloy::sol_types::SolCall;

        let asserter = Asserter::new();
        for auths in responses {
            asserter.push_success(&Bytes::from(
                PaymentPool::getAuthorizationsCall::abi_encode_returns(auths),
            ));
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        PaymentPool::new(Address::ZERO, provider)
    }

    /// A landed chunk counts every lane into `pool_redemptions` and persists
    /// `registered_until` for the lane whose `CapabilityReg` rode in it (#2154).
    /// The persisted value is the chain's registered expiry, not the attached
    /// `CapabilityReg`'s: another provider may have registered a different
    /// capability for the signer first (#2265). A failed receipt wait whose
    /// receipt a by-hash fetch found takes this path too.
    #[tokio::test]
    async fn landed_chunk_counts_redemptions_and_persists_registration() -> Result<()> {
        use decdn_incentive::MemoryPoolStateStore;

        let store: Arc<dyn PoolStateStore> = Arc::new(MemoryPoolStateStore::new());
        let registering = signed_lane_state(7, 50, 0xEE, None);
        let registered = signed_lane_state(7, 51, 0xEE, None);
        store.record(&registering)?;
        store.record(&registered)?;
        let mut with_reg = planned(7, 50, 100, true);
        if let Some(reg) = with_reg.register.as_mut() {
            reg.expiry = 1_900_000_000;
        }
        let chain_expiry = 1_800_000_000;
        let contract = mocked_getauthorizations_pool(&[vec![PaymentPool::Authorization {
            cap: 40,
            expiry: chain_expiry,
            spent: 40,
        }]]);
        let lanes = vec![with_reg, planned(7, 51, 100, false)];
        let metrics = Metrics::new();

        record_landed_chunk(&contract, &store, &lanes, &metrics).await;

        let persisted = store
            .get(registering.key())?
            .ok_or_else(|| anyhow::anyhow!("the registering lane's row exists"))?;
        assert_eq!(
            persisted.registered_until, chain_expiry,
            "the chain's registered expiry, not the attached CapabilityReg's"
        );
        let untouched = store
            .get(registered.key())?
            .ok_or_else(|| anyhow::anyhow!("the second lane's row exists"))?;
        assert_eq!(untouched.registered_until, registered.registered_until);
        let text = metrics.encode()?;
        anyhow::ensure!(
            text.lines().any(|l| l == "decdn_pool_redemptions_total 2"),
            "{text}"
        );
        Ok(())
    }

    /// A failed post-redeem registration read persists nothing, so the next
    /// sweep attaches the `CapabilityReg` again and re-reads.
    #[tokio::test]
    async fn landed_chunk_read_failure_persists_no_registration() -> Result<()> {
        use decdn_incentive::MemoryPoolStateStore;

        let store: Arc<dyn PoolStateStore> = Arc::new(MemoryPoolStateStore::new());
        let registering = signed_lane_state(7, 50, 0xEE, None);
        store.record(&registering)?;
        let contract = mocked_getauthorizations_pool(&[]);

        record_landed_chunk(
            &contract,
            &store,
            &[planned(7, 50, 100, true)],
            &Metrics::new(),
        )
        .await;

        let persisted = store
            .get(registering.key())?
            .ok_or_else(|| anyhow::anyhow!("the registering lane's row exists"))?;
        assert_eq!(persisted.registered_until, 0);
        Ok(())
    }

    /// Only a revert and a refused send count as redemption failures (#2154).
    /// An unconfirmed chunk may have mined, so it counts only into its
    /// `onchain_tx_*` bucket.
    #[test]
    fn only_reverts_and_refused_sends_are_redemption_failures() {
        use alloy::providers::PendingTransactionError;
        use alloy::transports::TransportErrorKind;

        let receipt = || alloy::rpc::types::TransactionReceipt {
            inner: alloy::consensus::ReceiptEnvelope::Eip1559(alloy::consensus::ReceiptWithBloom {
                receipt: alloy::consensus::Receipt {
                    status: alloy::consensus::Eip658Value::Eip658(true),
                    cumulative_gas_used: 0,
                    logs: Vec::new(),
                },
                logs_bloom: alloy::primitives::Bloom::ZERO,
            }),
            transaction_hash: B256::ZERO,
            transaction_index: None,
            block_hash: None,
            block_number: None,
            gas_used: 0,
            effective_gas_price: 0,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::ZERO,
            to: None,
            contract_address: None,
        };
        let tx_hash = B256::repeat_byte(0x17);
        let last_lookup = || "no receipt yet".to_owned();

        assert!(is_redemption_failure(&TxOutcome::Reverted(receipt())));
        assert!(is_redemption_failure(&TxOutcome::SendErr(
            TransportErrorKind::custom_str("rejected").into()
        )));
        assert!(!is_redemption_failure(&TxOutcome::Landed(receipt())));
        assert!(!is_redemption_failure(&TxOutcome::ReceiptErr {
            error: PendingTransactionError::TransportError(TransportErrorKind::custom_str(
                "error code 26: Unknown block"
            )),
            tx_hash,
            last_lookup: last_lookup(),
        }));
        assert!(!is_redemption_failure(&TxOutcome::Timeout {
            tx_hash,
            last_lookup: last_lookup(),
        }));
    }

    /// The whole point of the pre-redeem read (#2076): a lane the chain already
    /// shows settled to its claim value leaves the redeem batch, and a survivor's
    /// `unredeemed` is recomputed from the fresh read rather than the stale plan.
    #[tokio::test]
    async fn reconcile_drops_a_lane_the_chain_already_settled() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (contract, _asserter) = mocked_getwatermarks_pool(&[vec![lane(1_000), lane(400)]]);
        let plans = vec![planned(1, 0, 1_000, false), planned(1, 1, 1_000, false)];

        let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
        assert!(!read_failed, "every batch landed");

        assert_eq!(kept.len(), 1, "the settled lane leaves the batch");
        let survivor = kept
            .first()
            .ok_or_else(|| anyhow::anyhow!("one lane survives"))?;
        assert_eq!(survivor.key.signer, Address::from([1u8; 20]));
        assert_eq!(
            survivor.unredeemed,
            U256::from(600u64),
            "recomputed from the fresh on-chain paid (1000 owed − 400 paid)"
        );
        let text = metrics.encode()?;
        anyhow::ensure!(
            text.lines()
                .any(|l| l == "decdn_redemption_reconciled_skip_total 1"),
            "{text}"
        );
        Ok(())
    }

    /// Fail-open: a read the node could not make never holds up a redemption.
    #[tokio::test]
    async fn reconcile_fails_open_on_a_read_error() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        // An empty queue makes the mocked transport error on the first call.
        let (contract, _asserter) = mocked_getwatermarks_pool(&[]);
        let plans = vec![planned(1, 0, 1_000, false), planned(1, 1, 2_000, false)];

        let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
        assert!(read_failed, "a batch failed");

        assert_eq!(kept.len(), 2, "every lane survives an unreadable batch");
        assert_eq!(
            kept.iter().map(|p| p.unredeemed).collect::<Vec<_>>(),
            vec![U256::from(1_000u64), U256::from(2_000u64)]
        );
        let text = metrics.encode()?;
        anyhow::ensure!(
            text.lines()
                .any(|l| l == "decdn_redemption_reconciled_skip_total 0"),
            "{text}"
        );
        Ok(())
    }

    /// An under-returning batch reconciles the prefix and keeps the unread tail.
    #[tokio::test]
    async fn reconcile_pairs_the_prefix_on_a_short_return() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (contract, _asserter) = mocked_getwatermarks_pool(&[vec![lane(1_000)]]);
        let plans = vec![planned(1, 0, 1_000, false), planned(1, 1, 2_000, false)];

        let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
        assert!(read_failed, "a short return is a failed read");

        assert_eq!(kept.len(), 1, "the read lane is settled and drops");
        let survivor = kept
            .first()
            .ok_or_else(|| anyhow::anyhow!("the unread lane survives"))?;
        assert_eq!(survivor.key.signer, Address::from([1u8; 20]));
        assert_eq!(
            survivor.unredeemed,
            U256::from(2_000u64),
            "the unread lane is kept unchanged"
        );
        Ok(())
    }

    /// Spanning `WATERMARK_READ_BATCH_MAX` splits the read, and a later batch
    /// that fails keeps the savings from the batches that landed: the settled
    /// lane in batch 1 still leaves the redeem set even though batch 2 errored.
    #[tokio::test]
    async fn reconcile_keeps_the_first_batch_when_a_later_one_fails() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        // Batch 1 reads in full (its first lane settled); batch 2 has no queued
        // response, so the mocked transport errors on it.
        let mut first = vec![lane(0); WATERMARK_READ_BATCH_MAX];
        if let Some(head) = first.first_mut() {
            *head = lane(1_000);
        }
        let (contract, _asserter) = mocked_getwatermarks_pool(&[first]);
        let plans: Vec<PlannedLane> = (0..=WATERMARK_READ_BATCH_MAX)
            .map(|i| planned(1, u8::try_from(i % 251).unwrap_or(0), 1_000, false))
            .collect();

        let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
        assert!(read_failed, "a batch failed");

        assert_eq!(
            kept.len(),
            WATERMARK_READ_BATCH_MAX,
            "only the settled lane from the batch that landed is dropped; the \
             unread tail survives the failed batch"
        );
        let text = metrics.encode()?;
        anyhow::ensure!(
            text.lines()
                .any(|l| l == "decdn_redemption_reconciled_skip_total 1"),
            "{text}"
        );
        Ok(())
    }

    /// A short return in an early batch stops the read there: the prefix it did
    /// deliver reconciles and every later lane is kept unchanged.
    #[tokio::test]
    async fn reconcile_stops_at_a_short_early_batch() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        // Batch 1 under-returns (2 lanes for 512), so batch 2 is never issued.
        let (contract, asserter) =
            mocked_getwatermarks_pool(&[vec![lane(1_000), lane(1_000)], vec![lane(1_000)]]);
        let plans: Vec<PlannedLane> = (0..=WATERMARK_READ_BATCH_MAX)
            .map(|i| planned(1, u8::try_from(i % 251).unwrap_or(0), 1_000, false))
            .collect();

        let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
        assert!(read_failed, "a short return is a failed read");

        assert_eq!(
            kept.len(),
            WATERMARK_READ_BATCH_MAX - 1,
            "the two lanes the short batch covered are settled and drop"
        );
        assert_eq!(
            asserter.read_q().len(),
            1,
            "the second batch is never issued after a short return"
        );
        Ok(())
    }

    /// Two batches that both land. This is what pins the chunking itself: the
    /// mock returns whatever is queued regardless of how many triples were
    /// asked for, so a test whose batches never both succeed passes just as
    /// well against an implementation that does not chunk at all. Putting the
    /// settled lane in the *second* batch, and requiring the queue to be
    /// drained, fixes the call count, the batch size, the accumulation across
    /// batches and the cross-batch pairing offset at once.
    #[tokio::test]
    async fn reconcile_reads_every_batch_and_pairs_across_the_boundary() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        // Batch 1: 512 untouched lanes. Batch 2: the single settled lane.
        let (contract, asserter) = mocked_getwatermarks_pool(&[
            vec![lane(0); WATERMARK_READ_BATCH_MAX],
            vec![lane(1_000)],
        ]);
        let plans: Vec<PlannedLane> = (0..=WATERMARK_READ_BATCH_MAX)
            .map(|i| planned(1, u8::try_from(i % 251).unwrap_or(0), 1_000, false))
            .collect();

        let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
        assert!(!read_failed, "every batch landed");

        assert_eq!(
            asserter.read_q().len(),
            0,
            "both batches are issued; an unchunked read would leave one queued"
        );
        assert_eq!(
            kept.len(),
            WATERMARK_READ_BATCH_MAX,
            "the lane settled in the SECOND batch is the one dropped"
        );
        let text = metrics.encode()?;
        for expected in [
            "decdn_redemption_reconciled_skip_total 1",
            "decdn_redemption_reconcile_ok_total 2",
            "decdn_redemption_reconcile_failures_total 0",
        ] {
            anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
        }
        Ok(())
    }

    /// A batch that fails stops the read: no later batch is issued, so a lane
    /// settled beyond the failure is never paired against the wrong plan.
    #[tokio::test]
    async fn reconcile_stops_at_a_failed_middle_batch() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        // Batch 1 lands. Batch 2 has no queued response and errors. Batch 3's
        // response stays queued, proving the read stopped rather than skipped.
        let (contract, asserter) =
            mocked_getwatermarks_pool(&[vec![lane(0); WATERMARK_READ_BATCH_MAX]]);
        let plans: Vec<PlannedLane> = (0..=2 * WATERMARK_READ_BATCH_MAX)
            .map(|i| planned(1, u8::try_from(i % 251).unwrap_or(0), 1_000, false))
            .collect();

        let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
        assert!(read_failed, "a batch failed");

        assert_eq!(
            asserter.read_q().len(),
            0,
            "batch 1 consumed the only queued response"
        );
        assert_eq!(
            kept.len(),
            2 * WATERMARK_READ_BATCH_MAX + 1,
            "batch 1 found nothing settled and batches 2-3 were never read, so \
             every lane survives"
        );
        let text = metrics.encode()?;
        for expected in [
            "decdn_redemption_reconcile_ok_total 1",
            "decdn_redemption_reconcile_failures_total 1",
        ] {
            anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
        }
        Ok(())
    }

    /// A return LONGER than the batch proves the decoder and the chain disagree
    /// about the return shape, so its values pair with nothing reliably. The
    /// batch fails rather than dropping a lane on data it cannot trust.
    #[tokio::test]
    async fn reconcile_fails_the_batch_on_an_over_return() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        // Three lanes for a two-plan batch, the first of them "settled".
        let (contract, _asserter) =
            mocked_getwatermarks_pool(&[vec![lane(1_000), lane(1_000), lane(1_000)]]);
        let plans = vec![planned(1, 0, 1_000, false), planned(1, 1, 1_000, false)];

        let (kept, read_failed) = reconcile_onchain_watermarks(&contract, plans, &metrics).await;
        assert!(read_failed, "an over-return is a failed read");

        assert_eq!(
            kept.len(),
            2,
            "no lane is dropped on a return the decoder cannot trust"
        );
        let text = metrics.encode()?;
        for expected in [
            "decdn_redemption_reconciled_skip_total 0",
            "decdn_redemption_reconcile_failures_total 1",
        ] {
            anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
        }
        Ok(())
    }

    /// The idle steady state issues no `eth_call` at all.
    #[tokio::test]
    async fn reconcile_empty_plans_issues_no_call() {
        let metrics = Arc::new(Metrics::new());
        let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(1)]]);

        let (kept, read_failed) = reconcile_onchain_watermarks(&contract, vec![], &metrics).await;
        assert!(!read_failed, "no batch was issued");

        assert!(kept.is_empty());
        assert_eq!(
            asserter.read_q().len(),
            1,
            "the queued getWatermarks response is untouched by an empty plan set"
        );
    }

    /// A sub-floor hint issues no `getWatermarks` call (#2217): the floor runs
    /// on the cached values before the read, and a set that fails it there
    /// cannot pass it after the read.
    #[tokio::test]
    async fn a_sub_floor_plan_set_reads_nothing_from_the_chain() -> Result<()> {
        use decdn_incentive::MemoryPoolStateStore;

        let metrics = Arc::new(Metrics::new());
        let store: Arc<dyn PoolStateStore> = Arc::new(MemoryPoolStateStore::new());
        let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);

        let faulted = redeem_planned_lanes(
            &contract,
            &store,
            vec![planned(1, 0, 10, false)],
            U256::from(1_000_000u64),
            300,
            true,
            &metrics,
        )
        .await;

        assert!(!faulted, "a deferred set is not a chain fault");

        assert_eq!(
            asserter.read_q().len(),
            1,
            "the queued getWatermarks response is untouched by a sub-floor set"
        );
        let text = metrics.encode()?;
        for expected in [
            "decdn_redemption_reconcile_ok_total 0",
            "decdn_redemption_reconcile_failures_total 0",
        ] {
            anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
        }
        Ok(())
    }

    /// A set whose cached total clears the floor reads every lane, even a lane
    /// whose cached chunk fails the floor: the read can drop a lane, shrink the
    /// chunk count and re-pack the rest into a chunk that clears. Here the cap
    /// of 2 deals `[whale, b]` and `[a]`; `[a]` alone is below the floor, but
    /// `[a, b]` clears it once the read drops the whale. A read of only the
    /// first chunk would see a short return and count a reconcile failure.
    #[tokio::test]
    async fn a_set_that_clears_the_floor_in_total_reads_every_lane() -> Result<()> {
        use decdn_incentive::MemoryPoolStateStore;

        let metrics = Arc::new(Metrics::new());
        let store: Arc<dyn PoolStateStore> = Arc::new(MemoryPoolStateStore::new());
        // The chain shows every lane settled, so nothing reaches `redeemMany`.
        let (contract, asserter) =
            mocked_getwatermarks_pool(&[vec![lane(1_000_000), lane(300_000), lane(300_000)]]);

        let faulted = redeem_planned_lanes(
            &contract,
            &store,
            vec![
                planned(1, 0, 1_000_000, false),
                planned(1, 1, 300_000, false),
                planned(1, 2, 300_000, false),
            ],
            U256::from(500_000u64),
            2,
            true,
            &metrics,
        )
        .await;

        assert!(
            !faulted,
            "a set the chain shows settled is not a chain fault"
        );
        assert_eq!(asserter.read_q().len(), 0, "the one read was issued");
        let text = metrics.encode()?;
        for expected in [
            "decdn_redemption_reconcile_ok_total 1",
            "decdn_redemption_reconcile_failures_total 0",
            "decdn_redemption_reconciled_skip_total 3",
        ] {
            anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
        }
        Ok(())
    }

    /// A zero floor always passes the floor gate, so every lane is still
    /// reconciled against the chain.
    #[tokio::test]
    async fn a_zero_floor_still_reads_every_lane() -> Result<()> {
        use decdn_incentive::MemoryPoolStateStore;

        let metrics = Arc::new(Metrics::new());
        let store: Arc<dyn PoolStateStore> = Arc::new(MemoryPoolStateStore::new());
        // Both dust lanes read as settled, so nothing reaches `redeemMany`.
        let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(1), lane(1)]]);

        let faulted = redeem_planned_lanes(
            &contract,
            &store,
            vec![planned(1, 0, 1, false), planned(1, 1, 1, false)],
            U256::ZERO,
            300,
            false,
            &metrics,
        )
        .await;

        assert!(
            !faulted,
            "a set the chain shows settled is not a chain fault"
        );
        assert_eq!(asserter.read_q().len(), 0, "the dust lanes were read");
        let text = metrics.encode()?;
        for expected in [
            "decdn_redemption_reconcile_ok_total 1",
            "decdn_redemption_reconciled_skip_total 2",
        ] {
            anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
        }
        Ok(())
    }

    /// A settlement service over `contract` and `store`, built without
    /// `bootstrap`: no redemption task runs, so `shutdown` goes straight to
    /// `final_redeem_sweep`. The paid cache and pool projection start empty, so
    /// planning treats the whole claim as unredeemed and fails open on the pool.
    fn shutdown_service<P: Provider + Clone + 'static>(
        contract: PaymentPool::PaymentPoolInstance<P>,
        store: Arc<dyn PoolStateStore>,
        self_address: Address,
        redeem_threshold: U256,
        metrics: Arc<Metrics>,
    ) -> PoolSettlementService<P> {
        let (redeem_tx, _redeem_rx) = mpsc::channel(1);
        PoolSettlementService {
            contract,
            redeem_tx,
            store,
            paid: PaidWatermarks::default(),
            self_address,
            redeem_threshold,
            redeem_max_vouchers_per_tx: 300,
            metrics,
            pool_view: PoolProjection::new(),
            redeemer: std::sync::Mutex::new(None),
        }
    }

    /// Persist one lane that provider `[20; 20]` holds. The lane is registered
    /// (`registered_until = u64::MAX`), so its plan carries no `CapabilityReg`
    /// and needs no `owner_sig`. Returns the provider address and what the lane
    /// is owed.
    fn seed_one_owed_lane(store: &dyn PoolStateStore) -> Result<(Address, U256)> {
        let mut st = signed_lane_state(1, 10, 20, None);
        st.registered_until = u64::MAX;
        store.record(&st)?;
        Ok((Address::from([20u8; 20]), st.owed()))
    }

    /// A fresh in-memory store holding the lane from [`seed_one_owed_lane`].
    fn store_with_one_owed_lane() -> Result<(Arc<dyn PoolStateStore>, Address, U256)> {
        let store: Arc<dyn PoolStateStore> = Arc::new(decdn_incentive::MemoryPoolStateStore::new());
        let (me, owed) = seed_one_owed_lane(store.as_ref())?;
        Ok((store, me, owed))
    }

    /// An in-memory lane store whose `flush` always fails.
    struct FailingFlushStore(decdn_incentive::MemoryPoolStateStore);

    impl PoolStateStore for FailingFlushStore {
        fn load_all(&self) -> Result<Vec<LaneState>, StoreError> {
            self.0.load_all()
        }

        fn record(&self, state: &LaneState) -> Result<(), StoreError> {
            self.0.record(state)
        }

        fn forget(&self, key: LaneKey) -> Result<(), StoreError> {
            self.0.forget(key)
        }

        fn get(&self, key: LaneKey) -> Result<Option<LaneState>, StoreError> {
            self.0.get(key)
        }

        fn flush(&self) -> Result<(), StoreError> {
            Err(StoreError::Backend("flush refused".into()))
        }
    }

    /// `final_redeem_sweep` applies the configured floor: a lane whose
    /// unredeemed value is below `redeem_threshold` is left alone at shutdown.
    /// The floor gate returns before the pre-submit `getWatermarks` read, so the
    /// queued response stays unconsumed and no `redeemMany` is built.
    #[tokio::test]
    async fn shutdown_sweep_leaves_a_sub_floor_lane_unread() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (store, me, owed) = store_with_one_owed_lane()?;
        let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);
        let svc = shutdown_service(
            contract,
            store,
            me,
            owed + U256::from(1u64),
            Arc::clone(&metrics),
        );

        svc.shutdown(Duration::from_secs(5)).await;

        assert_eq!(
            asserter.read_q().len(),
            1,
            "the queued getWatermarks response is untouched by a sub-floor lane"
        );
        let text = metrics.encode()?;
        for expected in [
            format!("decdn_unredeemed_usdc {owed}"),
            "decdn_redemption_reconcile_ok_total 0".to_owned(),
            "decdn_redemption_failures_total 0".to_owned(),
        ] {
            anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
        }
        Ok(())
    }

    /// `final_redeem_sweep` passes a lane owed exactly `redeem_threshold` on to
    /// the pre-submit reconcile. The chain reports the lane settled, so nothing
    /// reaches `redeemMany`.
    #[tokio::test]
    async fn shutdown_sweep_reconciles_a_lane_at_the_floor() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (store, me, owed) = store_with_one_owed_lane()?;
        let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(u64::try_from(owed)?)]]);
        let svc = shutdown_service(contract, store, me, owed, Arc::clone(&metrics));

        svc.shutdown(Duration::from_secs(5)).await;

        assert_eq!(asserter.read_q().len(), 0, "the lane was read");
        let text = metrics.encode()?;
        for expected in [
            "decdn_redemption_reconcile_ok_total 1",
            "decdn_redemption_reconcile_failures_total 0",
            "decdn_redemption_reconciled_skip_total 1",
            "decdn_onchain_tx_send_failed_total 0",
            "decdn_redemption_failures_total 0",
        ] {
            anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
        }
        Ok(())
    }

    /// `final_redeem_sweep` submits a `redeemMany` for an unsettled lane at the
    /// floor. The first RPC call the send makes fails, so the send counts one
    /// refused send and one redemption failure.
    #[tokio::test]
    async fn shutdown_sweep_submits_redeem_many_for_an_unsettled_lane() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let (store, me, owed) = store_with_one_owed_lane()?;
        let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);
        asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
        let svc = shutdown_service(contract, store, me, owed, Arc::clone(&metrics));

        svc.shutdown(Duration::from_secs(5)).await;

        assert_eq!(
            asserter.read_q().len(),
            0,
            "the lane was read and the send tried"
        );
        let text = metrics.encode()?;
        for expected in [
            "decdn_redemption_reconcile_ok_total 1",
            "decdn_lane_flush_failures_total 0",
            "decdn_onchain_tx_send_failed_total 1",
            "decdn_redemption_failures_total 1",
        ] {
            anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
        }
        Ok(())
    }

    /// `final_redeem_sweep` does not let a failed lane-store flush stop the
    /// submit: the shutdown sweep still sends `redeemMany` for an unsettled lane
    /// at the floor.
    #[tokio::test]
    async fn shutdown_sweep_redeems_despite_a_failed_flush() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store: Arc<dyn PoolStateStore> = Arc::new(FailingFlushStore(
            decdn_incentive::MemoryPoolStateStore::new(),
        ));
        let (me, owed) = seed_one_owed_lane(store.as_ref())?;
        let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);
        asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
        let svc = shutdown_service(contract, store, me, owed, Arc::clone(&metrics));

        svc.shutdown(Duration::from_secs(5)).await;

        assert_eq!(
            asserter.read_q().len(),
            0,
            "the lane was read and the send tried"
        );
        let text = metrics.encode()?;
        for expected in [
            "decdn_lane_flush_failures_total 1",
            "decdn_onchain_tx_send_failed_total 1",
            "decdn_redemption_failures_total 1",
        ] {
            anyhow::ensure!(text.lines().any(|l| l == expected), "{expected}\n{text}");
        }
        Ok(())
    }

    /// A lane this node provides, owed value, with capability `expiry`.
    fn expiring_lane(signer: u8, expiry: u64) -> LaneState {
        let mut st = signed_lane_state(1, signer, 20, None);
        st.expiry = expiry;
        st
    }

    #[test]
    fn serve_cutoff_wake_is_one_second_past_the_cutoff() {
        let me = Address::from([20u8; 20]);
        let paid = PaidWatermarks::default();
        let st = expiring_lane(1, 10_000);
        assert_eq!(serve_cutoff_wake(&st, &paid, me, 420, 0, 0), Some(9_581));
        assert_eq!(
            serve_cutoff_wake(&st, &paid, me, 420, 9_580, 9_580),
            Some(9_581),
            "a wake still ahead of the last scan is kept"
        );
        assert_eq!(
            serve_cutoff_wake(&st, &paid, me, 420, 9_581, 9_581),
            None,
            "the sweep at a cutoff does not schedule that cutoff again"
        );
        assert_eq!(
            serve_cutoff_wake(&expiring_lane(1, 200), &paid, me, 100, 0, 0),
            Some(101),
            "an expiry inside the margin saturates instead of underflowing"
        );
    }

    /// A cutoff that passed with no scan after it is due at once: at start
    /// (`swept_at == 0`), or for a hint handled after its lane's cutoff.
    #[test]
    fn a_cutoff_passed_unscanned_is_due_now() {
        let me = Address::from([20u8; 20]);
        let paid = PaidWatermarks::default();
        let st = expiring_lane(1, 10_000);
        assert_eq!(
            serve_cutoff_wake(&st, &paid, me, 420, 0, 9_700),
            Some(9_700),
            "the node was down at the cutoff"
        );
        assert_eq!(
            serve_cutoff_wake(&st, &paid, me, 420, 9_500, 9_700),
            Some(9_700),
            "the last scan ran before the cutoff"
        );
        assert_eq!(
            serve_cutoff_wake(&st, &paid, me, 420, 9_600, 9_700),
            None,
            "a scan after the cutoff already read the final claim"
        );
        assert_eq!(
            serve_cutoff_wake(&st, &paid, me, 420, 0, 10_000 - REDEEM_LANDING_SLACK_SECS),
            None,
            "a lane inside the landing slack cannot be redeemed, so it is not due"
        );
    }

    #[test]
    fn serve_cutoff_wake_skips_lanes_that_need_no_redeem() {
        let me = Address::from([20u8; 20]);
        let paid = PaidWatermarks::default();
        assert_eq!(
            serve_cutoff_wake(&expiring_lane(1, 0), &paid, me, 420, 0, 0),
            None,
            "a lane with no tracked expiry never expires"
        );
        let other = Address::from([9u8; 20]);
        assert_eq!(
            serve_cutoff_wake(&expiring_lane(1, 10_000), &paid, other, 420, 0, 0),
            None,
            "another provider's lane"
        );
        let settled = expiring_lane(1, 10_000);
        paid.set(settled.key(), settled.owed());
        assert_eq!(
            serve_cutoff_wake(&settled, &paid, me, 420, 0, 0),
            None,
            "a fully paid lane"
        );
    }

    #[test]
    fn next_serve_cutoff_picks_the_earliest_wake() {
        let me = Address::from([20u8; 20]);
        let paid = PaidWatermarks::default();
        let states = [
            expiring_lane(1, 30_000),
            expiring_lane(2, 0),
            expiring_lane(3, 10_000),
            expiring_lane(4, 20_000),
        ];
        assert_eq!(
            next_serve_cutoff(&states, &paid, me, 420, 0, 0),
            Some(9_581)
        );
        assert_eq!(
            next_serve_cutoff(&states, &paid, me, 420, 9_581, 9_581),
            Some(19_581),
            "a scanned cutoff yields to the next one"
        );
        assert_eq!(
            next_serve_cutoff(&states, &paid, me, 420, 0, 9_700),
            Some(9_700),
            "an unscanned passed cutoff comes first"
        );
        assert_eq!(next_serve_cutoff(&[], &paid, me, 420, 0, 0), None);
    }

    /// The voucher path's margin predicate refuses from the cutoff on, and the
    /// wake falls one second after it: no proof can land after the cutoff sweep
    /// reads the lane.
    #[test]
    fn the_cutoff_wake_follows_the_voucher_margin_gate() {
        use decdn_common::config::inside_capability_expiry_margin;
        let me = Address::from([20u8; 20]);
        let paid = PaidWatermarks::default();
        let (expiry, margin) = (10_000, 420);
        let wake = serve_cutoff_wake(&expiring_lane(1, expiry), &paid, me, margin, 0, 0);
        assert_eq!(wake, Some(9_581));
        assert!(!inside_capability_expiry_margin(expiry, margin, 9_579));
        assert!(
            inside_capability_expiry_margin(expiry, margin, 9_580),
            "the gate is closed one second before the wake"
        );
    }

    /// An in-memory lane store that counts `load_all` calls and fails every
    /// call after the first `ok_loads`.
    struct CountingLoadStore {
        inner: decdn_incentive::MemoryPoolStateStore,
        loads: std::sync::atomic::AtomicUsize,
        ok_loads: usize,
    }

    impl CountingLoadStore {
        fn new(ok_loads: usize) -> Arc<Self> {
            Arc::new(Self {
                inner: decdn_incentive::MemoryPoolStateStore::new(),
                loads: std::sync::atomic::AtomicUsize::new(0),
                ok_loads,
            })
        }

        fn loads(&self) -> usize {
            self.loads.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl PoolStateStore for CountingLoadStore {
        fn load_all(&self) -> Result<Vec<LaneState>, StoreError> {
            let n = self.loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < self.ok_loads {
                self.inner.load_all()
            } else {
                Err(StoreError::Backend("load refused".into()))
            }
        }

        fn record(&self, state: &LaneState) -> Result<(), StoreError> {
            self.inner.record(state)
        }

        fn forget(&self, key: LaneKey) -> Result<(), StoreError> {
            self.inner.forget(key)
        }

        fn get(&self, key: LaneKey) -> Result<Option<LaneState>, StoreError> {
            self.inner.get(key)
        }

        fn flush(&self) -> Result<(), StoreError> {
            self.inner.flush()
        }
    }

    /// Spawn `redeemer_loop` with self-tick `interval`. With a one-hour tick,
    /// only a hint or a serve cutoff makes it act within a test. Returns the
    /// hint sender (drop it to end the loop), the loop's handle, and its margin.
    fn spawn_redeemer<P: Provider + Clone + 'static>(
        contract: PaymentPool::PaymentPoolInstance<P>,
        store: Arc<dyn PoolStateStore>,
        floor: U256,
        interval: Duration,
        metrics: Arc<Metrics>,
    ) -> (mpsc::Sender<LaneKey>, JoinHandle<()>, u64) {
        let margin = capability_expiry_margin_secs(interval.as_secs());
        let (tx, rx) = mpsc::channel(8);
        let handle = tokio::spawn(redeemer_loop(
            contract,
            store,
            PaidWatermarks::default(),
            Address::from([20u8; 20]),
            floor,
            300,
            interval,
            margin,
            rx,
            metrics,
            PoolProjection::new(),
        ));
        (tx, handle, margin)
    }

    /// The margin of a redeemer with a one-hour self-tick.
    fn hour_tick_margin() -> u64 {
        capability_expiry_margin_secs(Duration::from_hours(1).as_secs())
    }

    /// Record a registered lane for each signer in `signers`, all expiring at
    /// `expiry`. Returns the first lane's key and the lanes' total owed value.
    fn record_lanes(
        store: &dyn PoolStateStore,
        signers: std::ops::RangeInclusive<u8>,
        expiry: u64,
    ) -> Result<(LaneKey, U256)> {
        let mut first = None;
        let mut total = U256::ZERO;
        for signer in signers {
            let mut st = expiring_lane(signer, expiry);
            st.registered_until = u64::MAX;
            total += st.owed();
            store.record(&st)?;
            first.get_or_insert(st.key());
        }
        Ok((first.context("at least one lane")?, total))
    }

    /// Wait up to 15 s for `done` to hold.
    async fn await_until(what: &str, mut done: impl FnMut() -> Result<bool>) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !done()? {
            anyhow::ensure!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }

    /// Wait up to 15 s for `line` to appear in the encoded metrics.
    async fn await_metric_line(metrics: &Metrics, line: &str) -> Result<()> {
        await_until(line, || Ok(metrics.encode()?.lines().any(|l| l == line))).await
    }

    /// Assert every line in `lines` is in the encoded metrics.
    fn ensure_metric_lines(metrics: &Metrics, lines: &[&str]) -> Result<()> {
        let text = metrics.encode()?;
        for line in lines {
            anyhow::ensure!(text.lines().any(|l| l == *line), "{line}\n{text}");
        }
        Ok(())
    }

    /// #2233: several sub-floor lanes hold capabilities with the same expiry.
    /// Their serve cutoff is about 3 s away, long before the next hourly
    /// self-tick, and together they clear the floor. The lanes appear after the
    /// loop starts, so a hint registers the cutoff. The cutoff sweep sends one
    /// `redeemMany` for all of them, no earlier than the wake, and runs once.
    #[tokio::test]
    async fn sub_floor_lanes_are_redeemed_together_at_their_serve_cutoff() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store = CountingLoadStore::new(usize::MAX);
        let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0), lane(0), lane(0)]]);
        // The redeemMany send fails at its first RPC call, so the send is metered.
        asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());

        let lane_value = expiring_lane(1, 0).owed();
        let floor = lane_value * U256::from(3u64);
        let (tx, handle, margin) = spawn_redeemer(
            contract,
            Arc::clone(&store) as Arc<dyn PoolStateStore>,
            floor,
            Duration::from_hours(1),
            Arc::clone(&metrics),
        );
        // Let the loop run its startup scan on the empty store.
        await_until("the startup scan", || Ok(store.loads() == 1)).await?;

        let expiry = unix_now() + margin + 2;
        let wake = expiry - margin + 1;
        let (hinted, _) = record_lanes(store.as_ref(), 1..=3, expiry)?;
        assert!(lane_value < floor, "each lane alone is below the floor");
        tx.send(hinted).await?;

        await_metric_line(&metrics, "decdn_onchain_tx_send_failed_total 1").await?;
        assert!(
            unix_now() >= wake,
            "the cutoff sweep runs no earlier than the wake"
        );
        assert_eq!(asserter.read_q().len(), 0, "the chunk was read and sent");

        // The sweep does not re-fire its passed cutoff: any second sweep would
        // reach the empty mock queue and count a reconcile failure.
        tokio::time::sleep(Duration::from_secs(2)).await;
        ensure_metric_lines(
            &metrics,
            &[
                "decdn_redemption_reconcile_ok_total 1",
                "decdn_redemption_reconcile_failures_total 0",
                "decdn_onchain_tx_send_failed_total 1",
            ],
        )?;
        assert_eq!(store.loads(), 2, "the startup scan and one cutoff sweep");

        drop(tx);
        handle.await?;
        Ok(())
    }

    /// Lanes persisted before the loop starts keep their cutoff sweep. A set
    /// that stays below the floor at the cutoff is swept once and left
    /// unredeemed: the sweep reads nothing from the chain.
    #[tokio::test]
    async fn a_sub_floor_set_at_its_serve_cutoff_reads_nothing() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store = CountingLoadStore::new(usize::MAX);
        let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0), lane(0)]]);
        let expiry = unix_now() + hour_tick_margin() + 2;
        let (_, total) = record_lanes(store.as_ref(), 1..=2, expiry)?;
        let (tx, handle, _) = spawn_redeemer(
            contract,
            Arc::clone(&store) as Arc<dyn PoolStateStore>,
            total + U256::from(1u64),
            Duration::from_hours(1),
            Arc::clone(&metrics),
        );

        // Every sweep publishes the planned total, so this line marks the
        // cutoff sweep.
        await_metric_line(&metrics, &format!("decdn_unredeemed_usdc {total}")).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(store.loads(), 2, "the startup scan and one cutoff sweep");
        drop(tx);
        handle.await?;
        assert_eq!(
            asserter.read_q().len(),
            1,
            "the queued getWatermarks response is untouched by a sub-floor set"
        );
        Ok(())
    }

    /// A lane whose cutoff passed while the node was down, with time left
    /// before the landing slack, is swept at once on start rather than after
    /// the first self-tick.
    #[tokio::test]
    async fn a_cutoff_passed_before_start_is_swept_at_once() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store: Arc<dyn PoolStateStore> = Arc::new(decdn_incentive::MemoryPoolStateStore::new());
        let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);
        asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
        // The cutoff passed 10 s ago; the landing slack is an hour away.
        let expiry = unix_now() + hour_tick_margin() - 10;
        let (_, total) = record_lanes(store.as_ref(), 1..=1, expiry)?;
        let (tx, handle, _) = spawn_redeemer(
            contract,
            store,
            total,
            Duration::from_hours(1),
            Arc::clone(&metrics),
        );

        await_metric_line(&metrics, "decdn_onchain_tx_send_failed_total 1").await?;
        ensure_metric_lines(&metrics, &["decdn_redemption_reconcile_ok_total 1"])?;

        drop(tx);
        handle.await?;
        Ok(())
    }

    /// A hint handled after its lane's cutoff, with no sweep since, makes the
    /// cutoff sweep due at once rather than leaving the lane to the next tick.
    #[tokio::test]
    async fn a_hint_after_an_unscanned_cutoff_sweeps_at_once() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store = CountingLoadStore::new(usize::MAX);
        let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0), lane(0)]]);
        asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
        let lane_value = expiring_lane(1, 0).owed();
        let (tx, handle, margin) = spawn_redeemer(
            contract,
            Arc::clone(&store) as Arc<dyn PoolStateStore>,
            lane_value * U256::from(2u64),
            Duration::from_hours(1),
            Arc::clone(&metrics),
        );
        await_until("the startup scan", || Ok(store.loads() == 1)).await?;

        // The cutoff passed 10 s ago; the landing slack is an hour away.
        let expiry = unix_now() + margin - 10;
        let (hinted, _) = record_lanes(store.as_ref(), 1..=2, expiry)?;
        tx.send(hinted).await?;

        await_metric_line(&metrics, "decdn_onchain_tx_send_failed_total 1").await?;
        ensure_metric_lines(&metrics, &["decdn_redemption_reconcile_ok_total 1"])?;

        drop(tx);
        handle.await?;
        Ok(())
    }

    /// A failed load at the cutoff sweep drops the passed cutoff instead of
    /// re-firing it: the loop parks until the next tick or hint.
    #[tokio::test]
    async fn a_failed_load_at_the_cutoff_does_not_spin() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        // The startup scan succeeds; every later load fails.
        let store = CountingLoadStore::new(1);
        let (contract, _asserter) = mocked_getwatermarks_pool(&[]);
        let expiry = unix_now() + hour_tick_margin() + 2;
        let (_, total) = record_lanes(store.as_ref(), 1..=1, expiry)?;
        let (tx, handle, _) = spawn_redeemer(
            contract,
            Arc::clone(&store) as Arc<dyn PoolStateStore>,
            total,
            Duration::from_hours(1),
            Arc::clone(&metrics),
        );

        await_until("the failed cutoff load", || Ok(store.loads() >= 2)).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(store.loads(), 2, "the passed cutoff is not re-fired");

        drop(tx);
        handle.await?;
        Ok(())
    }

    /// The self-tick sweeps an owed lane that no hint and no cutoff names.
    #[tokio::test]
    async fn the_self_tick_sweeps_a_lane_without_a_hint() -> Result<()> {
        let metrics = Arc::new(Metrics::new());
        let store: Arc<dyn PoolStateStore> = Arc::new(decdn_incentive::MemoryPoolStateStore::new());
        let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);
        asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
        // No tracked expiry, so no cutoff wake.
        let (_, total) = record_lanes(store.as_ref(), 1..=1, 0)?;
        let (tx, handle, _) = spawn_redeemer(
            contract,
            store,
            total,
            Duration::from_secs(1),
            Arc::clone(&metrics),
        );

        await_metric_line(&metrics, "decdn_onchain_tx_send_failed_total 1").await?;
        ensure_metric_lines(&metrics, &["decdn_redemption_reconcile_ok_total 1"])?;

        drop(tx);
        handle.await?;
        Ok(())
    }

    /// #2340: a hint whose redemption hits a chain fault parks its lane. The
    /// first hint reads the watermark and its `redeemMany` send fails. Every
    /// later hint for the lane is dropped and counted, and reads nothing: a
    /// further read would reach the empty mock queue and count a reconcile
    /// failure.
    #[tokio::test]
    async fn a_chain_fault_parks_the_lane_until_the_next_sweep() -> Result<()> {
        const HINTS: u64 = 4;
        let metrics = Arc::new(Metrics::new());
        let store: Arc<dyn PoolStateStore> = Arc::new(decdn_incentive::MemoryPoolStateStore::new());
        let (contract, asserter) = mocked_getwatermarks_pool(&[vec![lane(0)]]);
        asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
        // No tracked expiry, so no cutoff wake, and a one-hour tick: only the
        // hints act within the test.
        let (hinted, total) = record_lanes(store.as_ref(), 1..=1, 0)?;
        let (tx, handle, _) = spawn_redeemer(
            contract,
            store,
            total,
            Duration::from_hours(1),
            Arc::clone(&metrics),
        );

        for _ in 0..HINTS {
            tx.send(hinted).await?;
        }

        await_metric_line(
            &metrics,
            &format!("decdn_redeem_hints_parked_total {}", HINTS - 1),
        )
        .await?;
        ensure_metric_lines(
            &metrics,
            &[
                "decdn_redemption_reconcile_ok_total 1",
                "decdn_redemption_reconcile_failures_total 0",
                "decdn_onchain_tx_send_failed_total 1",
            ],
        )?;
        assert_eq!(asserter.read_q().len(), 0, "the one hint read and sent");

        drop(tx);
        handle.await?;
        Ok(())
    }

    /// #2340: a sweep releases a parked lane. A hint parks the lane, a second
    /// hint is dropped, the self-tick sweep retries the lane, and a hint after
    /// that sweep redeems again.
    #[tokio::test]
    async fn a_sweep_releases_a_parked_lane() -> Result<()> {
        use alloy::providers::ProviderBuilder;
        use alloy::providers::mock::Asserter;
        use alloy::sol_types::SolCall;

        let metrics = Arc::new(Metrics::new());
        let store: Arc<dyn PoolStateStore> = Arc::new(decdn_incentive::MemoryPoolStateStore::new());
        // One read and one failed send each for the first hint, the sweep and
        // the hint after the sweep, in the order the mock answers them.
        let asserter = Asserter::new();
        for _ in 0..3 {
            asserter.push_success(&Bytes::from(
                PaymentPool::getWatermarksCall::abi_encode_returns(&vec![lane(0)]),
            ));
            asserter.push_failure(alloy_json_rpc::ErrorPayload::internal_error());
        }
        let contract = PaymentPool::new(
            Address::ZERO,
            ProviderBuilder::new().connect_mocked_client(asserter.clone()),
        );
        let (hinted, total) = record_lanes(store.as_ref(), 1..=1, 0)?;
        let (tx, handle, _) = spawn_redeemer(
            contract,
            store,
            total,
            Duration::from_secs(3),
            Arc::clone(&metrics),
        );

        tx.send(hinted).await?;
        tx.send(hinted).await?;
        await_metric_line(&metrics, "decdn_redeem_hints_parked_total 1").await?;
        ensure_metric_lines(&metrics, &["decdn_onchain_tx_send_failed_total 1"])?;

        // The self-tick sweep retries the parked lane.
        await_metric_line(&metrics, "decdn_onchain_tx_send_failed_total 2").await?;
        ensure_metric_lines(&metrics, &["decdn_redemption_reconcile_ok_total 2"])?;

        // The sweep cleared the parked set, so this hint redeems again.
        tx.send(hinted).await?;
        await_metric_line(&metrics, "decdn_onchain_tx_send_failed_total 3").await?;
        ensure_metric_lines(
            &metrics,
            &[
                "decdn_redemption_reconcile_ok_total 3",
                "decdn_redemption_reconcile_failures_total 0",
                "decdn_redeem_hints_parked_total 1",
            ],
        )?;

        drop(tx);
        handle.await?;
        Ok(())
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
        // A zero floor keeps even a pure-dust chunk.
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

    /// The redeemer's expiry check: `0` never expires, and a capability counts
    /// as expired from the landing slack before its expiry onward.
    #[test]
    fn capability_expired_holds_the_landing_slack() {
        let expiry = 10_000;
        assert!(!super::capability_expired(0, u64::MAX), "0 = not tracked");
        assert!(!super::capability_expired(
            expiry,
            expiry - REDEEM_LANDING_SLACK_SECS - 1
        ));
        assert!(super::capability_expired(
            expiry,
            expiry - REDEEM_LANDING_SLACK_SECS
        ));
        assert!(super::capability_expired(expiry, expiry + 1));
        assert!(
            super::capability_expired(u64::MAX, u64::MAX - 1),
            "saturates"
        );
    }

    /// `decdn_redemption_skipped_expired_total` as the scrape reads it.
    fn skipped_expired(metrics: &Metrics) -> u64 {
        let text = metrics.encode().unwrap_or_default();
        text.lines()
            .find_map(|l| l.strip_prefix("decdn_redemption_skipped_expired_total "))
            .and_then(|v| v.parse().ok())
            .unwrap_or(u64::MAX)
    }

    /// The planner skips a lane with value owed whose capability expires within
    /// the landing slack, and meters it on its own counter. The same lane plans
    /// while its expiry lies further ahead.
    #[test]
    fn plan_lanes_skips_an_expired_capability() {
        let me = Address::from([20u8; 20]);
        // Held registration material, so the unregistered lane plans when live.
        let st = signed_lane_state(1, 10, 20, Some(sig_with_v(1)));
        let expiry = st.expiry;
        let paid = PaidWatermarks::default();
        let projection = PoolProjection::new();

        let metrics = Arc::new(Metrics::new());
        let live_now = expiry - REDEEM_LANDING_SLACK_SECS - 1;
        let plans = plan_lanes(&paid, me, vec![st.clone()], &metrics, &projection, live_now);
        assert_eq!(plans.len(), 1, "a live capability plans");
        assert_eq!(skipped_expired(&metrics), 0);

        let late_now = expiry - REDEEM_LANDING_SLACK_SECS;
        let plans = plan_lanes(&paid, me, vec![st], &metrics, &projection, late_now);
        assert!(
            plans.is_empty(),
            "an expiring capability pays 0 and is skipped"
        );
        assert_eq!(skipped_expired(&metrics), 1);
    }

    /// A fully redeemed lane stays in the store until its pool is reclaimed, so
    /// every sweep sees it. Once its capability expires, the planner drops it
    /// without metering: no value is stranded.
    #[test]
    fn plan_lanes_drops_an_expired_lane_with_nothing_owed_silently() {
        let me = Address::from([20u8; 20]);
        let st = signed_lane_state(1, 10, 20, Some(sig_with_v(1)));
        let late_now = st.expiry - REDEEM_LANDING_SLACK_SECS;
        let paid = PaidWatermarks::default();
        paid.set(st.key(), st.owed());
        let projection = PoolProjection::new();
        let metrics = Arc::new(Metrics::new());

        for _ in 0..3 {
            let plans = plan_lanes(&paid, me, vec![st.clone()], &metrics, &projection, late_now);
            assert!(
                plans.is_empty(),
                "a fully redeemed lane has nothing to plan"
            );
        }
        assert_eq!(
            skipped_expired(&metrics),
            0,
            "a lane with nothing owed strands no value"
        );
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

    /// A grace close counts only for this provider's lane on the closing pool
    /// that the chain has not fully paid.
    #[test]
    fn holds_unredeemed_matches_only_an_unpaid_lane_of_this_pool_and_provider() {
        let pool = PoolId::from([1; 32]);
        let me = Address::from([20; 20]);
        let owed = signed_lane_state(1, 10, 20, None);
        assert!(holds_unredeemed(std::slice::from_ref(&owed), pool, me));

        let mut paid = owed.clone();
        paid.paid_cumulative = paid.owed();
        assert!(!holds_unredeemed(&[paid], pool, me));

        let other_provider = signed_lane_state(1, 10, 21, None);
        assert!(!holds_unredeemed(&[other_provider], pool, me));

        let other_pool = signed_lane_state(2, 10, 20, None);
        assert!(!holds_unredeemed(&[other_pool], pool, me));
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

    /// #2052 acceptance: a lane redeemed to its owed value before a restart — the
    /// durable row carries `paid_cumulative == owed` — must NOT be re-planned once
    /// the in-memory cache is rebuilt from the store. Exercises the real bootstrap
    /// rehydration path ([`rehydrate_paid_watermarks`]) over a store, then plans.
    #[test]
    fn redeemed_lane_not_replanned_after_restart() -> Result<()> {
        use decdn_incentive::MemoryPoolStateStore;

        let provider = Address::from([40u8; 20]);
        let mut st = signed_lane_state(1, 30, 40, None);
        // On-chain the lane was fully redeemed before the restart; the durable
        // lane row records that.
        st.paid_cumulative = st.owed();
        anyhow::ensure!(!st.paid_cumulative.is_zero(), "fixture must be redeemable");

        let store = MemoryPoolStateStore::new();
        store.record(&st)?;

        // Restart: the volatile cache is gone. Rebuild it from the store exactly
        // as `bootstrap` does — this is the fix under test.
        let paid = rehydrate_paid_watermarks(&store, provider);

        let plan = plan_lane(&st, &paid, provider, &RegistrationStatus::Registered)?;
        assert!(
            plan.is_none(),
            "a lane already redeemed to its owed value must not be re-planned after restart (#2052)"
        );
        Ok(())
    }

    /// #2052: a partially-redeemed lane still owes the remainder after a restart,
    /// so rehydration must leave exactly that remainder to plan — never the full
    /// face value (the amnesia bug) and never nothing.
    #[test]
    fn partially_redeemed_lane_plans_only_the_remainder_after_restart() -> Result<()> {
        use decdn_incentive::MemoryPoolStateStore;

        let provider = Address::from([41u8; 20]);
        let mut st = signed_lane_state(1, 31, 41, None);
        let remainder = U256::from(250u64);
        st.paid_cumulative = st.owed() - remainder;

        let store = MemoryPoolStateStore::new();
        store.record(&st)?;
        let paid = rehydrate_paid_watermarks(&store, provider);

        let plan =
            plan_lane(&st, &paid, provider, &RegistrationStatus::Registered)?.ok_or_else(|| {
                anyhow::anyhow!("a partially-redeemed lane still owes and should plan")
            })?;
        assert_eq!(
            plan.unredeemed, remainder,
            "only the un-redeemed remainder is planned, not the full owed value"
        );
        Ok(())
    }

    /// The voucher-record path carries `paid_cumulative` from the live in-memory
    /// lane, which never learns the redeemed watermark. `record` must not let that
    /// zero clobber a persisted non-zero value, or a later frontier advance would
    /// silently reopen the #2052 amnesia within a single run.
    #[test]
    fn record_does_not_regress_paid_cumulative() -> Result<()> {
        use decdn_incentive::MemoryPoolStateStore;

        let store = MemoryPoolStateStore::new();
        let mut st = signed_lane_state(1, 32, 42, None);
        st.paid_cumulative = U256::from(600u64);
        store.record(&st)?;

        // A later voucher record for the same lane carries paid_cumulative back at
        // zero (the shape the serve path produces).
        let mut advanced = signed_lane_state(1, 32, 42, None);
        advanced.paid_cumulative = U256::ZERO;
        store.record(&advanced)?;

        let got = store
            .get(st.key())?
            .ok_or_else(|| anyhow::anyhow!("lane must persist"))?;
        assert_eq!(
            got.paid_cumulative,
            U256::from(600u64),
            "record must not regress the persisted paid watermark to zero"
        );
        Ok(())
    }
}
