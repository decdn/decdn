//! The #1608 gap-driven, range-minimized fetch driver.
//!
//! [`drive`] satisfies a request `R = [offset, offset+len)` of one blob by
//! filling ONLY the gaps the store is missing, paying the minimum: held ranges
//! are read locally, never pulled, never re-paid. It is the integration keystone
//! of the #1621 ranged-store effort — the piece that turns the sourcing axis
//! ([`BlobSource`]), the pacing axis ([`Pacer`]), and the funding axis
//! ([`Funder`]) into a single fetch that pulls exactly the bytes a request needs.
//!
//! # The money-relevant branches
//!
//! A whole-tail pull streams one advancing `byte_offset` to the end of the blob;
//! `drive` instead drives the same money-relevant branches per gap — it draws,
//! funds, and resumes one missing range at a time. The acquire loop
//! ([`crate::acquire`]) runs the same per-gap loop on each of its lanes. The
//! branches are:
//!
//! - **Draw** (the happy path): open the gap's [`AlignedRange`](decdn_bao_range::AlignedRange), stream it through
//!   [`crate::ClientRangedStore::ingest_stream`] (which durably checkpoints as it goes),
//!   then [`BlobSource::finish`] to drain the pull and recover the acked voucher
//!   watermark. The store's checkpoints are what make a mid-gap fault re-enter
//!   with a SMALLER gap: the store owns durability, so a resume never re-pulls a
//!   checkpointed prefix.
//! - **Reactive top-up**: a genuine mid-fetch exhaustion (confirmed against our
//!   OWN ledger via [`genuine_exhaustion`]) is funded through [`Funder::top_up`],
//!   then the gap is retried at its PAID frontier — NOT its checkpointed
//!   (delivered) frontier. [`crate::ClientRangedStore::ingest_stream`] checkpoints
//!   delivered+verified bytes payment-agnostically (the ADR 003 credit window
//!   lets the node stream a full interval before the voucher that pays for it is
//!   due), so a mid-leg exhaustion can leave the store's present-range frontier
//!   AHEAD of the last PAID byte. Resuming from `missing_ranges` alone would then
//!   skip billing the delivered-but-unpaid tail (an under-pay). So the per-leg
//!   resume offset is [`sink::content_paid_frontier`] of the leg's paid wire
//!   watermark — the tail is
//!   re-delivered (`ingest_stream` re-writes it idempotently) and re-billed.
//! - **Settle-wait**: after a top-up the driver retries the open immediately —
//!   the pacer sees the healed deposit and draws right away. If the node's chain
//!   watcher has not yet observed the new deposit, that retry is refused with the
//!   ambiguous [`crate::resume_may_be_stale`] shape; ONLY THEN does the driver
//!   sleep and retry, bounded by the settle-wait budget: back-off happens only on an
//!   actual stale-resume refusal.
//! - **Reseed** (wallet-less resync, #1481): an authenticated
//!   [`WatermarkBundle`](decdn_protocol::client::WatermarkBundle) that ADVANCES
//!   our committed watermark is a healable desync — the driver reseeds the ledger
//!   ([`PoolLedger::reseed`]) and retries. This is driver-owned, NOT a
//!   [`PaceDecision`] — the reseed check runs ahead of pacing, the same ordering
//!   the node's own miss-pull policy uses.
//!
//! # What `drive` does not do
//!
//! - No **stale-foreign-partial restart-from-zero**. A
//!   [`crate::ClientRangedStore`] is keyed to `(root, total_bytes)` and only ever
//!   holds bao-verified ranges, so an ambiguous `NotFound` can never mean "this
//!   file belongs to another blob": there is no foreign-partial ambiguity to
//!   resolve, and resume is always driven by the verified present set.
//! - No **progress reporting, deadlines, or durable watermark persistence** to a
//!   `BuyerChannelStore` — these are the caller's job. The acked watermark lives
//!   in the [`PoolLedger`] the caller owns; persisting it across process restarts,
//!   and drawing a progress bar, are CLI concerns.
//!
//! # Store abstraction
//!
//! The store is generic over [`crate::source::IngestStore`] (`RangedStore` +
//! `ingest_stream`), not the concrete `&ClientRangedStore` — [`crate::ClientRangedStore`]
//! is one implementer (the CLI/client backend, writing `.partial`/`.ranges`); a
//! node backend admits to the cache and tees to its downstream client
//! through the same seam.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::U256;
use bao_tree::ChunkRanges;
use decdn_bao_range::{
    AlignedRange, CHUNK_GROUP_BYTES, RangedStore, RangedStoreError, align_range,
};
use decdn_incentive::DepositOutcome;
use decdn_protocol::VoucherRejectReason;

use crate::buyer_pool::EscrowUntracked;
use crate::fault::HealExhausted;
use crate::pacer::{DownstreamFrontier, PaceDecision, PaceState};
use crate::source::{BlobSource, Funder, IngestEnd, IngestStore, SourceFuture};
use crate::{
    MAX_RESUME_ATTEMPTS, Pacer, PoolContext, PoolLedger, UpstreamPullHeader,
    UpstreamVoucherRejected, genuine_exhaustion, heal_watermark_desync, is_insufficient_deposit,
    reject_empty_claim_for_nonempty_root, rejection_watermark, resume_may_be_stale,
};

/// The shared pool cannot fund the next voucher: its remaining deposit is below
/// the next voucher's cost and reactive top-up is disabled or exhausted (the
/// pacer returned [`PaceDecision::Refuse`]).
///
/// Typed rather than a bare string so the fault classifier
/// ([`crate::classify`]) can `downcast_ref` it and rule it
/// [`crate::Fault::Unaffordable`]: the pool is the same deposit against every
/// provider (ADR 003), so moving the range to another lane cannot fund it. The
/// source waits for the deposit to rise instead of cooling.
#[derive(Debug)]
#[cfg_attr(not(feature = "test-util"), non_exhaustive)]
pub struct PoolExhausted {
    /// Start of the gap that could not be funded.
    pub gap_start: u64,
    /// Length of the gap that could not be funded.
    pub gap_len: u64,
}

impl std::fmt::Display for PoolExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "gap [{}, +{}) of blob cannot be funded: the remaining deposit cannot cover the \
             next voucher and reactive top-up is disabled or exhausted",
            self.gap_start, self.gap_len
        )
    }
}

impl std::error::Error for PoolExhausted {}

/// A source kept refusing a new stream as `InsufficientDeposit` while the pool's
/// remaining deposit already sat within the low water of the working deposit,
/// past the settle budget ([`DriveConfig::max_settle_waits`]).
///
/// No top-up the pacer would send moves such a deposit, so the source is not
/// priced out by it: either its chain view has not seen a refill yet, or its
/// refundable floor `M` exceeds what this working deposit can cover. The fault
/// classifier ([`crate::classify`]) rules it [`crate::Fault::Source`], so the
/// source cools and is asked again, instead of waiting for a deposit rise that
/// never comes.
#[derive(Debug)]
pub(crate) struct StaleDepositView {
    /// The remaining deposit by our own ledger.
    pub(crate) remaining: U256,
    /// The working deposit the pacer tops up toward.
    pub(crate) working: U256,
}

impl std::fmt::Display for StaleDepositView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the source refuses the pool as short of deposit while {} of the {} µUSDC working \
             deposit remains: its chain view is stale, or its refundable floor exceeds the \
             working deposit",
            self.remaining, self.working
        )
    }
}

impl std::error::Error for StaleDepositView {}

/// Marker on a reactive top-up that failed: [`Funder::top_up`] returned an
/// error, so the deposit did not rise. The failure is the buyer's funding,
/// never the serving source's delivery, so the fault classifier
/// ([`crate::classify`]) rules it [`crate::Fault::Transient`]: the source keeps
/// its health and the loop retries. A funder that found the wallet short of
/// USDC ([`crate::buyer_pool::WalletShortfall`]) makes it
/// [`crate::Fault::Unaffordable`], like [`PoolExhausted`], and an error that may
/// have escrowed USDC
/// ([`crate::buyer_pool::TopUpUnconfirmed`],
/// [`crate::buyer_pool::EscrowUntracked`]) stays fatal, whatever marker it
/// carries.
#[derive(Debug)]
#[doc(hidden)]
pub struct TopUpFailed;

impl std::fmt::Display for TopUpFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the reactive pool top-up failed")
    }
}

impl std::error::Error for TopUpFailed {}

/// A leg of a gap opened, streamed and finished cleanly, yet left both the gap's
/// paid frontier and the store's delivered frontier where they were.
///
/// A clean finish leaves the ledger crediting every wire byte the leg received:
/// one reveal per whole chunk, plus a closing signature for any residual. The
/// paid frontier therefore reaches the end of what the leg delivered, capped at
/// the delivered frontier, and the leg's bytes land in the store. A clean leg
/// that moves neither frontier means the store did not keep the bytes, the
/// ledger did not record the payment, or the upstream ended the stream without
/// taking the leg's final proof. It never means a slow peer: a stalled leg ends
/// with a stall error, not a clean finish. Opening the same range again would
/// pay for the same bytes to the same effect, so the gap-fill loop behind
/// [`drive`] and [`crate::acquire`] ends the gap with this error instead
/// (#2194).
///
/// [`crate::classify`] rules it [`crate::Fault::Source`]. The cause may be the
/// source, and each further source costs at most one more leg before it ends the
/// same way.
#[derive(Debug)]
#[doc(hidden)]
pub struct LegNoProgress {
    /// Start of the range the leg opened.
    pub offset: u64,
    /// Length of the range the leg opened.
    pub len: u64,
    /// The gap's paid frontier when the leg opened. The pass after the leg found
    /// it no further on.
    pub paid_frontier: u64,
    /// The store's delivered frontier in the gap when the leg opened. The pass
    /// after the leg found it no further on.
    pub delivered_frontier: u64,
    /// The channel ledger's committed WIRE bytes since the leg opened. The ledger
    /// is shared, so this counts concurrent pulls on the same channel too. With no
    /// concurrent pull, enough wire to cover the leg's first chunk group points at
    /// the store (it did not keep what was paid for), and zero points at the
    /// ledger or the upstream (the leg's payment was never recorded).
    pub paid_wire: u64,
}

impl std::fmt::Display for LegNoProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "leg [{}, +{}) finished cleanly but advanced neither the paid frontier ({}) nor \
             the delivered frontier ({}), with {} wire bytes credited; refusing to re-open \
             and re-pay the same range",
            self.offset, self.len, self.paid_frontier, self.delivered_frontier, self.paid_wire
        )
    }
}

impl std::error::Error for LegNoProgress {}

/// The last leg of a gap that finished cleanly, as the pass that opened it saw the
/// gap. [`fill_gap`]'s next pass must see one of the two frontiers move.
#[derive(Debug, Clone, Copy)]
struct CleanLeg {
    /// Start of the range the leg opened.
    offset: u64,
    /// Length of the range the leg opened.
    len: u64,
    /// The gap's paid frontier when the leg opened.
    paid_frontier: u64,
    /// The store's delivered frontier when the leg opened.
    delivered_frontier: u64,
    /// The ledger generation when the leg opened. A rebase since then moved the
    /// shared watermark under the leg, so its paid-frontier reading is void.
    generation: u64,
}

impl CleanLeg {
    /// The [`LegNoProgress`] this leg amounts to, given what the next pass reads:
    /// both frontiers, the wire the ledger credited since the leg opened, and the
    /// ledger generation. `None` when either frontier moved, or when a rebase makes
    /// the paid frontier incomparable.
    const fn stalled(
        self,
        paid_frontier: u64,
        delivered_frontier: u64,
        paid_wire: u64,
        generation: u64,
    ) -> Option<LegNoProgress> {
        if generation != self.generation
            || paid_frontier > self.paid_frontier
            || delivered_frontier > self.delivered_frontier
        {
            return None;
        }
        Some(LegNoProgress {
            offset: self.offset,
            len: self.len,
            paid_frontier: self.paid_frontier,
            delivered_frontier: self.delivered_frontier,
            paid_wire,
        })
    }
}

/// The state ONE pool deposit's concurrent lanes share, injected by the
/// multi-source scheduler. Absent (`None`) on the single-source path, where the
/// one lane IS the pool and its own `DriveCounters` and [`PoolContext`] already
/// hold every fact below.
///
/// Three facts are properties of the POOL, not of a lane, so a per-lane copy of
/// any of them lets N lanes each spend what only one pool holds:
///
/// - **Spend.** The deposit gate must subtract what EVERY lane committed, not
///   what this one did.
/// - **Top-up budget.** [`Funder::max_topups`] bounds the reactive top-ups ONE
///   fetch may escrow. Counting them per-lane multiplies the bound by the lane
///   count.
/// - **Deposit.** A landed top-up raises the deposit every lane draws on. Written
///   only through this lane's `ctx`, it is invisible to the others, whose gate
///   still subtracts the aggregate spend from a stale deposit and walks to a
///   false exhaustion.
#[doc(hidden)]
pub struct SharedPool<'a> {
    /// Sum, across every lane, of the committed voucher amount — the pool's
    /// total spend so far.
    pub spent: &'a (dyn Fn() -> U256 + Send + Sync),
    /// Reactive top-ups this FETCH has spent, across every lane.
    pub topups_used: &'a AtomicU32,
    /// Credit a landed top-up's new deposit to EVERY lane's `PoolContext`, so no
    /// lane gates on a stale deposit.
    pub credit: &'a (dyn Fn(U256) -> anyhow::Result<()> + Send + Sync),
    /// Held across each lane's top-up. Two lanes that reach the pool's floor
    /// together would each escrow the whole shortfall, and without
    /// `--max-approve` the second `topUp` reverts on the allowance the first one
    /// used. The lane that waits re-reads the deposit once it holds the lock,
    /// and decides again if a sibling's top-up already raised it.
    pub topup_lock: &'a tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for SharedPool<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedPool")
            .field("spent", &(self.spent)())
            .field("topups_used", &self.topups_used.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Why a window-bounded pacer paused the pull, handed to [`PacingWait::wait`]
/// so the caller can meter the two pauses apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum WaitReason {
    /// [`PaceDecision::Wait`]: the pull has run its full window ahead of the
    /// downstream paid frontier.
    WindowFull,
    /// [`PaceDecision::WaitForMinDraw`]: the window has room, but less than the
    /// minimum draw.
    MinDraw,
}

/// The injected wait signal for [`PaceDecision::Wait`] and
/// [`PaceDecision::WaitForMinDraw`] (ADR 037): the
/// node hands in an implementor that resolves once its serve leg's paid frontier
/// or demand frontier has advanced (so a re-decide has a chance of finding room);
/// the client path never needs one, since `BudgetPacer` never returns `Wait`.
#[doc(hidden)]
pub trait PacingWait: Send + Sync {
    /// Resolve once the caller judges it worth re-deciding (e.g. a downstream
    /// frontier advanced, or a bounded poll interval elapsed).
    ///
    /// `observed` holds the downstream frontiers the caller's `Wait` decision was
    /// computed from. An implementor backed by an edge-triggered wakeup (a
    /// [`tokio::sync::Notify`], which stores no permit across `notify_waiters`) MUST
    /// register its wakeup BEFORE re-reading the live frontiers and return
    /// immediately if either already moved past `observed` — otherwise an advance
    /// that races between the decision and the park is lost and the caller wedges
    /// forever (#1673).
    ///
    /// `reason` says which pause the decision was, for metering only; the wait
    /// itself is the same for both.
    fn wait(
        &self,
        observed: DownstreamFrontier,
        reason: WaitReason,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// Read the channel context's current deposit through the shared handle. A tiny
/// helper so the driver never holds the lock across an `.await` — it locks,
/// copies the `U256`, and drops the guard.
fn locked_deposit(ctx: &Mutex<PoolContext>) -> anyhow::Result<U256> {
    Ok(ctx
        .lock()
        .map_err(|_| anyhow::anyhow!("channel context lock poisoned"))?
        .deposit)
}

/// Whether `remaining` deposit pays for the next voucher and sits within the
/// low water of `working_deposit`, so the reactive top-up the pacer could send
/// is below [`crate::pacer::min_reactive_top_up`]. A peer that still refuses
/// such a deposit as insufficient reads a balance older than ours.
fn deposit_near_working(remaining: U256, next_voucher_cost: U256, working_deposit: U256) -> bool {
    !working_deposit.is_zero()
        && remaining >= next_voucher_cost
        && working_deposit.saturating_sub(remaining)
            < crate::pacer::min_reactive_top_up(working_deposit)
}

/// Decide the next step for a source that refuses a deposit near the working
/// target ([`deposit_near_working`]) after `waits` settle waits: `None` to
/// wait once more, or the [`StaleDepositView`] fault once the budget is spent.
/// Logs the first wait at info, each later one at debug, and the fault at warn.
fn stale_view_wait(remaining: U256, config: &DriveConfig, waits: u32) -> Option<StaleDepositView> {
    let working = config.working_deposit;
    if waits >= config.max_settle_waits {
        tracing::warn!(
            %remaining,
            %working,
            waits,
            "a source keeps refusing a deposit near the working target; its chain view is \
             stale or its refundable floor exceeds the working deposit"
        );
        return Some(StaleDepositView { remaining, working });
    }
    if waits == 0 {
        tracing::info!(
            %remaining,
            %working,
            "a source refuses a deposit near the working target; waiting for its chain view \
             to catch up"
        );
    } else {
        tracing::debug!(
            %remaining,
            waits,
            "still waiting for a source's chain view of the deposit"
        );
    }
    None
}

/// Bytes per [`bao_tree::ChunkNum`] — a 1 KiB bao chunk. A gap's byte span is its
/// chunk-range boundaries scaled by this.
const CHUNK_BYTES: u64 = 1024;

/// Deployment knobs the pure gap/pay core needs from its caller (CLI: #1497's
/// `MAX_TOPUP_SETTLE_WAITS` / `TOPUP_SETTLE_BACKOFF`; node: its own smaller
/// budgets). Kept minimal — progress and deadlines stay with the caller.
#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub struct DriveConfig {
    /// The reactive top-up target passed to the pacer as
    /// [`PaceState::working_deposit`]. `U256::ZERO` disables reactive top-up.
    pub working_deposit: U256,
    /// The buyer's estimate of a serving peer's refundable floor `M`, passed to the
    /// pacer as [`PaceState::seller_reserve`]. `U256::ZERO` tops up only once the
    /// next voucher is unaffordable.
    pub seller_reserve: U256,
    /// How many settle-backoff steps the driver may spend, after a top-up,
    /// retrying an open that keeps failing with [`crate::resume_may_be_stale`]
    /// before giving up on the node's chain watcher.
    pub max_settle_waits: u32,
    /// How long each settle-backoff step sleeps.
    pub settle_backoff: Duration,
}

impl DriveConfig {
    /// The CLI's reactive-graduation defaults (#1497): 30 settle waits of 500 ms.
    #[must_use]
    pub const fn cli(working_deposit: U256) -> Self {
        Self {
            working_deposit,
            seller_reserve: U256::ZERO,
            max_settle_waits: MAX_TOPUP_SETTLE_WAITS,
            settle_backoff: TOPUP_SETTLE_BACKOFF,
        }
    }
}

/// Reactive graduation (#1497): after an on-chain `topUp`, the node's settlement
/// watcher can briefly lag the `ChannelToppedUp` event, so its pre-serve deposit
/// gate (#1518) still sees the pre-top-up deposit and refuses the resumed open
/// (collapsed to `NotFound`). The client that performed the top-up waits out that
/// lag by retrying the open — money-safe, since an open sends no vouchers and does
/// not move `byte_offset`. `MAX_TOPUP_SETTLE_WAITS * TOPUP_SETTLE_BACKOFF` bounds
/// the total wait (15s), comfortably above the daemon's chain-event poll cadence
/// yet well under a fetch's overall deadline.
///
/// The single source of truth for [`drive`]'s settle-wait budget (via
/// [`DriveConfig::cli`]); the CLI `fetch` command and `bundle_pull` both run
/// `drive`, so they share one settle-wait policy.
pub(crate) const MAX_TOPUP_SETTLE_WAITS: u32 = 30;
/// Backoff between resume-open retries while waiting for the node's chain watcher
/// to observe a just-landed top-up (see [`MAX_TOPUP_SETTLE_WAITS`]).
pub(crate) const TOPUP_SETTLE_BACKOFF: Duration = Duration::from_millis(500);

/// How often a fetch's single flush owner persists the `.ranges` present record
/// while sources are still delivering. The ranged store's ingest `checkpoint`
/// fsyncs data and outboard every ~4 MiB but never writes the record, so this
/// interval bounds crash-loss of resume progress to at most one interval (spec
/// §5.5): a killed fetch resumes from the last flushed frontier instead of
/// refetching — and re-paying for — the whole in-flight download. 5 seconds
/// mirrors the voucher-flush cadence.
pub(crate) const PRESENT_RECORD_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// Drive `fut` (a fetch's whole gap/worker set) to completion while a single
/// periodic tick flushes the store's `.ranges` present record every `interval`,
/// so a crash mid-fetch costs at most one `interval` of resume progress (spec
/// §5.5). `fut` is the SOLE work driver and this loop is the SOLE periodic flush
/// owner — nothing inside `fut` flushes — which preserves the single-writer
/// property the ranged store's ingest `checkpoint` relies on. Returns once `fut`
/// resolves; the caller does the final flush.
///
/// `tokio::time::interval`'s first tick fires immediately, so it is consumed
/// before the loop to avoid a redundant flush at start.
pub(crate) async fn drive_with_interval_flush<St, Fut>(
    store: &St,
    interval: Duration,
    fut: Fut,
) -> anyhow::Result<()>
where
    St: IngestStore,
    Fut: Future<Output = anyhow::Result<()>>,
{
    tokio::pin!(fut);
    let mut tick = tokio::time::interval(interval);
    tick.tick().await; // consume the immediate first tick
    // The record write in flight, if any. It runs beside `fut`, never instead
    // of it: `fut` carries every lane's pay loop, so pausing it for a record
    // fsync would stall voucher sends. A tick that finds a write still in
    // flight skips, which keeps this the single record writer.
    let mut flush: Option<SourceFuture<'_, ()>> = None;
    loop {
        tokio::select! {
            // Prefer completion: if the work is done, finish rather than flush —
            // the caller's final flush persists the terminal snapshot. Land the
            // write in flight first, so it cannot rename an older snapshot over
            // the caller's final one.
            biased;
            res = &mut fut => {
                if let Some(pending) = flush.take() {
                    let flushed = pending.await;
                    res?;
                    return flushed;
                }
                return res;
            }
            flushed = await_flush(&mut flush), if flush.is_some() => {
                flush = None;
                flushed?;
            }
            _ = tick.tick() => {
                if flush.is_none() {
                    flush = Some(store.flush_present_record());
                }
            }
        }
    }
}

/// Await the record write in `flush`. Only polled while one is in flight.
async fn await_flush(flush: &mut Option<SourceFuture<'_, ()>>) -> anyhow::Result<()> {
    match flush {
        Some(pending) => pending.await,
        None => std::future::pending().await,
    }
}

/// A multi-source unit's live progress across its gaps, which [`fill_gap`]
/// writes as each verified leaf lands, before the store's checkpoint makes it
/// durable.
#[derive(Debug, Default)]
pub(crate) struct UnitProgress {
    /// The unit's verified bytes. The lane watchdog judges it, and a steal
    /// reads the unit's rate off it.
    pub(crate) verified: std::sync::atomic::AtomicU64,
    /// The end of the verified prefix the unit's legs have reached. A steal
    /// splits past it, so it never hands a stealer bytes this unit already
    /// received.
    pub(crate) frontier: std::sync::atomic::AtomicU64,
    /// When the unit verified its first byte: the start of its rate clock,
    /// so a leg's open and a cold first byte stay out of its rate.
    pub(crate) first_byte: std::sync::OnceLock<tokio::time::Instant>,
}

/// Fetch-wide counters that persist ACROSS the request's gaps (a top-up budget is
/// per-fetch, not per-gap), plus the last upstream quote used to price the next
/// voucher.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DriveCounters {
    /// Reactive top-ups spent so far — bounded by [`Funder::max_topups`].
    topups_used: u32,
    /// Desync-reseed retries spent so far — bounded by [`MAX_RESUME_ATTEMPTS`].
    resume_attempts: u32,
    /// `ceil(interval_bytes * rate_per_mb / MiB)` from the most recent successful
    /// open. `ZERO` before the first open, which keeps the pacer's affordability
    /// gate open so the first draw always proceeds (an exhaustion can only follow
    /// an open).
    next_voucher_cost: U256,
}

impl DriveCounters {
    /// Fresh per-fetch (single-source) or per-worker (multi-source) counters:
    /// no top-ups or reseeds spent, and no priced voucher yet.
    pub(crate) const fn new() -> Self {
        Self {
            topups_used: 0,
            resume_attempts: 0,
            next_voucher_cost: U256::ZERO,
        }
    }
}

/// Say at `info!` that a voucher rejection on the leg at `offset` healed the
/// lane ledger and the leg reopens, so `-v` shows why a retry happened.
fn log_healed_retry(
    hash: [u8; 32],
    offset: u64,
    attempt: u32,
    err: &anyhow::Error,
    healed: Option<crate::Healed>,
) {
    tracing::info!(
        hash = %blake3::Hash::from_bytes(hash).to_hex(),
        offset,
        attempt,
        max_attempts = MAX_RESUME_ATTEMPTS,
        reason = ?err
            .downcast_ref::<UpstreamVoucherRejected>()
            .map(|rejected| rejected.reason),
        ?healed,
        "voucher rejection healed the lane ledger; reopening the leg"
    );
}

/// True when `err` is a lane-watermark voucher rejection that a heal past the
/// resume budget scopes to its source: `UnderFold`, `AmountRegression`,
/// `Underpaid` or `BytesRegression`. A heal reseeds on any reason whose bundle
/// is ahead of the ledger, so the reason decides the scope, not the heal. A
/// `BytesRegression` with no bundle never heals, so it stays bare and ends the
/// command (ADR 005). A `SpendingCapExhausted` is the pool's, one deposit
/// behind every lane, so the exhaustion check judges it instead.
fn scopes_to_source(err: &anyhow::Error) -> bool {
    err.downcast_ref::<UpstreamVoucherRejected>()
        .is_some_and(|r| {
            matches!(
                r.reason,
                VoucherRejectReason::UnderFold
                    | VoucherRejectReason::AmountRegression
                    | VoucherRejectReason::Underpaid
                    | VoucherRejectReason::BytesRegression
            )
        })
}

/// Price the next voucher from an upstream header, the exact formula
/// [`PoolLedger`]'s own `next_voucher` and the CLI's reactive branch use.
fn voucher_cost(header: &UpstreamPullHeader) -> U256 {
    U256::from(header.interval_bytes)
        .saturating_mul(U256::from(header.rate_per_mb))
        .div_ceil(U256::from(decdn_protocol::MB_BYTES))
}

/// Fold a [`ChunkRanges`] (1 KiB `ChunkNum` units) into the contiguous byte
/// ranges it covers, in ascending order, clamping the final boundary to
/// `total_bytes` (the ragged last group). Each entry is `(start, len)` with
/// `len > 0`.
pub(crate) fn contiguous_byte_ranges(ranges: &ChunkRanges, total_bytes: u64) -> Vec<(u64, u64)> {
    let boundaries = ranges.boundaries();
    let mut out = Vec::new();
    let mut it = boundaries.iter();
    while let (Some(a), Some(b)) = (it.next(), it.next()) {
        let start = a.0.saturating_mul(CHUNK_BYTES).min(total_bytes);
        let end = b.0.saturating_mul(CHUNK_BYTES).min(total_bytes);
        if end > start {
            out.push((start, end - start));
        }
    }
    out
}

/// Total content bytes a [`ChunkRanges`] covers (clamped to `total_bytes`).
pub(crate) fn ranges_content_len(ranges: &ChunkRanges, total_bytes: u64) -> u64 {
    contiguous_byte_ranges(ranges, total_bytes)
        .iter()
        .map(|(_, len)| *len)
        .fold(0u64, u64::saturating_add)
}

/// The end of the present bytes contiguous from `gap_start` in a gap that ends
/// at `gap_end`, given the gap's `missing` chunks: the start of the first
/// missing range that overlaps the gap, clamped to `[gap_start, gap_end]`, or
/// `gap_end` when nothing in the gap is missing. Present bytes past a hole do
/// not move it.
pub(crate) fn contiguous_frontier(
    missing: &ChunkRanges,
    total_bytes: u64,
    gap_start: u64,
    gap_end: u64,
) -> u64 {
    contiguous_byte_ranges(missing, total_bytes)
        .into_iter()
        .find(|&(start, len)| start.saturating_add(len) > gap_start && start < gap_end)
        .map_or(gap_end, |(start, _)| start.max(gap_start).min(gap_end))
}

/// The chunks of `[start, start + len)` the store misses, clipped to its
/// bound, and that bound. A range at or past the bound misses nothing. A leg
/// can prove a smaller size between the bound read and the query, so a query
/// the store refuses as out of bounds reads the smaller bound again and clips
/// to it. A proven size is final, so the retry ends.
///
/// # Errors
///
/// Any other store error, marked [`crate::LocalPullFault`]: a store that cannot
/// answer is this process's fault, never a source's (#2213).
pub(crate) async fn missing_below_bound<St>(
    store: &St,
    start: u64,
    len: u64,
) -> anyhow::Result<(ChunkRanges, u64)>
where
    St: RangedStore + ?Sized,
{
    loop {
        let bound = store.total_bytes();
        let end = start.saturating_add(len).min(bound);
        if start >= end {
            return Ok((ChunkRanges::empty(), bound));
        }
        match store.missing_ranges(start, end - start).await {
            Ok(missing) => return Ok((missing, bound)),
            Err(decdn_bao_range::RangedStoreError::Alignment(_)) if store.total_bytes() < bound => {
                // A leg proved a smaller size after the bound read: read it
                // again on the next pass.
            }
            Err(e) => return Err(anyhow::Error::new(e).context(crate::LocalPullFault)),
        }
    }
}

/// Satisfy request `R = [offset, offset + len)` of blob `hash` by filling only its
/// gaps, paying the minimum. `len == 0` means "to the end of the blob". Held
/// ranges are read locally — never pulled, never paid.
///
/// The store is driven one write operation at a time (its documented concurrency
/// contract): `drive` never issues overlapping `ingest_stream`/`finalize` calls.
///
/// `pool` is `None` for a single-source fetch — the one lane is the whole pool.
/// A caller that drives several per-provider lanes over one shared deposit (the
/// node's ranged assembly) passes a [`SharedPool`] so every lane's solvency gate
/// subtracts the aggregate spend and the reactive-top-up budget spans the whole
/// set (#1506).
///
/// # What it does
///
/// 1. Compute `store.missing_ranges(offset, len)` and split it into the ordered
///    contiguous gaps. If none are missing, skip straight to the completion check.
/// 2. For each gap, run a per-gap resume/pay loop: assemble a [`PaceState`] from
///    the ledger/ctx/store/header and let the [`Pacer`] choose `Draw` / `TopUp` /
///    `Wait` / `Done` / `Refuse`. A `Draw` opens the gap's [`AlignedRange`] through the
///    [`BlobSource`], streams it into the store (which checkpoints durably), and
///    finishes the pull. A mid-gap fault re-enters with the checkpointed prefix
///    already held, so the re-open covers only the un-checkpointed tail.
/// 3. When the request's gaps are all filled AND the whole blob is present,
///    [`finalize`](decdn_bao_range::RangedStore::finalize) it (promoting `.partial` to its
///    final path). For a partial `R` that does not complete the blob, `drive`
///    returns `Ok(())` and leaves finalization to a later whole-blob fetch — the
///    store cannot promote a blob that is still missing bytes outside `R`.
///
/// # Errors
///
/// A [`PaceDecision::Refuse`] (out of budget/attempts), a terminal source/store
/// fault that is neither a healable desync nor a fundable exhaustion, a clean leg
/// that moved neither frontier ([`LegNoProgress`]), an escrowed-but-untracked
/// top-up outcome, or a `finalize` failure.
#[allow(clippy::too_many_arguments)]
#[doc(hidden)]
pub async fn drive<St, S, P, F>(
    store: &St,
    source: &S,
    pacer: &P,
    funder: &F,
    ctx: &Arc<Mutex<PoolContext>>,
    ledger: &Arc<PoolLedger>,
    hash: [u8; 32],
    offset: u64,
    len: u64,
    config: &DriveConfig,
    on_progress: Option<&(dyn Fn(u64, u64) + Send + Sync + '_)>,
    pacing_wait: Option<&dyn PacingWait>,
    downstream: Option<&(dyn Fn() -> DownstreamFrontier + Send + Sync)>,
    pool: Option<&SharedPool<'_>>,
) -> anyhow::Result<()>
where
    St: IngestStore,
    S: BlobSource,
    P: Pacer,
    F: Funder,
{
    let total_bytes = store.total_bytes();
    // A store sized from a `total_bytes == 0` claim has no gap to fill and is
    // complete as created, so nothing downstream ever verifies the empty stream
    // against the root. Prove it here, before the empty store can be finalized
    // (see [`reject_empty_claim_for_nonempty_root`]).
    reject_empty_claim_for_nonempty_root(total_bytes, hash)?;

    // Surface the already-present resume base on the progress bar immediately —
    // before the pre-fetch window (channel open, first chunk). `fill_gap`'s
    // per-gap reporter fires only from inside `ingest_stream` once streaming
    // begins (as `base_present + received`), so without this a resumed blob's bar
    // sits at `0` until the first byte arrives, then jumps to the resume point.
    // This is exactly the value that reporter emits at `received == 0`: content
    // bytes, matching the reporter's own unit and whole-blob `total_bytes`
    // denominator. A no-op on the node's serve legs, which pass `on_progress =
    // None`.
    if let Some(cb) = on_progress {
        let base_present = ranges_content_len(
            &store.present_ranges().await.map_err(store_query_fault)?,
            total_bytes,
        );
        cb(base_present, total_bytes);
    }

    let missing = store
        .missing_ranges(offset, len)
        .await
        .map_err(store_query_fault)?;
    let gaps = contiguous_byte_ranges(&missing, total_bytes);

    // Fill every gap while a single periodic tick flushes the `.ranges` present
    // record (the single-writer flush point). The ranged store's ingest
    // `checkpoint` never persists the record, so without this interval flush
    // a crash mid-fetch would leave `.ranges` at pre-session state and re-download
    // (and re-pay for) the whole in-flight range on resume; the interval bounds
    // that loss to one `PRESENT_RECORD_FLUSH_INTERVAL`. This loop is the
    // single-source path's sole periodic flush owner — `fill_gap` never flushes.
    let outcome = drive_with_interval_flush(store, PRESENT_RECORD_FLUSH_INTERVAL, async move {
        let mut counters = DriveCounters::new();
        for (gap_start, gap_len) in gaps {
            fill_gap(
                store,
                source,
                pacer,
                funder,
                ctx,
                ledger,
                hash,
                gap_start,
                gap_len,
                config,
                &mut counters,
                on_progress,
                // `None`: `drive` is the single-source path, whose one lane reports
                // its own present base directly — there is no cross-lane total to
                // aggregate. Only the multi-source scheduler passes an aggregator.
                None,
                // No unit watchdog and no steal on the single-source path.
                None,
                pacing_wait,
                downstream,
                // `None` on the single-source path: this one lane IS the pool,
                // so its own `counters` and `ctx` already hold the spend, the
                // top-up budget, and the deposit. The node's ranged-drive loop
                // injects a shared view of all three across its per-provider
                // lanes here instead (#1506); the client's multi-source
                // scheduler calls `fill_gap` directly with the same view.
                pool,
                // No steal on the single-source path.
                None,
            )
            .await?;
        }
        Ok(())
    })
    .await;

    // Final flush of whatever landed — on the FAILURE path too. The bytes a
    // failed drive did deliver are paid for, and the interval owner's last tick
    // can be up to one interval stale, so skipping this on `Err` discards
    // already-bought resume progress the next invocation would have to re-pay
    // for. The drive's own error is the more informative one, so it wins when
    // both fail; a flush failure alone still surfaces.
    let flushed = store.flush_present_record().await;
    outcome?;
    flushed?;

    // Promote only when the WHOLE blob is present — `finalize` verifies and
    // renames the whole `.partial`, which it cannot do while bytes outside `R`
    // are still missing. A whole-blob `R` reaches this complete; a partial `R`
    // leaves the `.partial` in place for a later fetch to finish.
    if store.is_complete().await.map_err(store_query_fault)? {
        store.finalize().await?;
    }
    Ok(())
}

/// The aligned range one paid leg opens: the unpaid tail `[resume_start,
/// gap_end)` of a gap, clamped to the `up_to_bytes` the pacer authorized.
///
/// The received-byte ceiling (#1895) caps the leg too, so the delivered frontier
/// can pass `max_blob_size_bytes` by at most one chunk group, at which point
/// [`fill_gap`]'s loop-top check aborts. Without this a [`crate::BudgetPacer`],
/// which draws the whole gap remainder, would pull an entire oversized blob before
/// that check ever runs. `resume_start` is always at or below the ceiling here (a
/// `resume_start` past it means the loop-top check already aborted), so
/// `cap_end - resume_start` is at least one chunk group and never collapses the
/// length to the `0` ("to end") sentinel. `max_blob_size_bytes == 0` is unlimited.
///
/// The one computation both [`fill_gap`] and [`first_leg`] use, so a caller that
/// opens the first leg ahead of the drive opens exactly the range the drive asks
/// for.
pub(crate) fn leg_range(
    resume_start: u64,
    gap_end: u64,
    up_to_bytes: u64,
    max_blob_size_bytes: u64,
    total_bytes: u64,
) -> anyhow::Result<AlignedRange> {
    let draw_len = gap_end.saturating_sub(resume_start).min(up_to_bytes);
    let draw_len = if max_blob_size_bytes > 0 {
        let cap_end = max_blob_size_bytes.saturating_add(CHUNK_GROUP_BYTES);
        draw_len.min(cap_end.saturating_sub(resume_start))
    } else {
        draw_len
    };
    Ok(align_range(resume_start, draw_len, total_bytes)?)
}

/// A store query's error, marked [`crate::LocalPullFault`] when the store itself
/// failed ([`RangedStoreError::Backend`]): no source caused it, and every source
/// would meet it. An alignment error is about the requested range, not the store,
/// so it stays unmarked.
fn store_query_fault(err: RangedStoreError) -> anyhow::Error {
    let store_failed = matches!(err, RangedStoreError::Backend(_));
    let err = anyhow::Error::new(err);
    if store_failed {
        err.context(crate::LocalPullFault)
    } else {
        err
    }
}

/// The missing gaps of every range in `ranges`, merged and in ascending order.
/// Each range is `(offset, len)`, and `len == 0` means "to the end of the blob".
/// Overlapping or adjacent ranges merge, so the gaps are disjoint.
async fn range_set_gaps<St: RangedStore + ?Sized>(
    store: &St,
    ranges: &[(u64, u64)],
) -> anyhow::Result<Vec<(u64, u64)>> {
    let mut missing = ChunkRanges::empty();
    for &(offset, len) in ranges {
        missing |= store
            .missing_ranges(offset, len)
            .await
            .map_err(store_query_fault)?;
    }
    Ok(contiguous_byte_ranges(&missing, store.total_bytes()))
}

/// The aligned range a drive of `ranges` over `store` opens first, or `None`
/// when every byte of them is already present.
///
/// A caller that must open a pull before the drive starts (to read the signed
/// `total_bytes`, or to learn whether the peer will serve) opens exactly this
/// range and hands the live pull to the drive through a [`crate::PrimedSource`],
/// so the first open is also the drive's first leg (#2063). It is the range
/// [`drive`] opens for its first gap when the pacer's
/// first decision is `Draw { up_to_bytes }`: a fresh drive's paid frontier
/// starts at the gap start, so the leg is the gap cut to `up_to_bytes`. A
/// [`crate::BudgetPacer`] draws the whole unpaid gap, so pass `u64::MAX` for it.
/// `max_blob_size_bytes` is the source's received-byte ceiling
/// ([`BlobSource::max_blob_size_bytes`]).
///
/// # Errors
///
/// A store query failure, or a range that does not align against the blob.
#[doc(hidden)]
pub async fn first_leg<St: RangedStore + ?Sized>(
    store: &St,
    ranges: &[(u64, u64)],
    up_to_bytes: u64,
    max_blob_size_bytes: u64,
) -> anyhow::Result<Option<AlignedRange>> {
    let gaps = range_set_gaps(store, ranges).await?;
    let Some(&(start, len)) = gaps.first() else {
        return Ok(None);
    };
    leg_range(
        start,
        start.saturating_add(len),
        up_to_bytes,
        max_blob_size_bytes,
        store.total_bytes(),
    )
    .map(Some)
}

/// Warn when the store holds bytes after a hole in the gap
/// `[gap_start, gap_end)`, once per distinct hole start. `hole` is the gap's
/// contiguous delivered frontier and `missing_bytes` the gap's missing bytes, so
/// the span past the hole less those is what the store holds after it. The next
/// leg opens at the hole and pays for those bytes again. `warned_hole` holds the
/// start of the last hole warned about.
fn warn_on_new_hole(
    warned_hole: &mut Option<u64>,
    hash: [u8; 32],
    gap_start: u64,
    hole: u64,
    gap_end: u64,
    missing_bytes: u64,
) {
    let present_after_hole = gap_end.saturating_sub(missing_bytes).saturating_sub(hole);
    if present_after_hole == 0 || *warned_hole == Some(hole) {
        return;
    }
    *warned_hole = Some(hole);
    tracing::warn!(
        hash = %blake3::Hash::from_bytes(hash).to_hex(),
        gap_start,
        hole,
        gap_end,
        present_after_hole,
        "the store holds bytes after a hole in the gap; the next leg opens at the hole and \
         pays for them again"
    );
}

/// The per-gap resume/pay loop. Fills the contiguous content span
/// `[gap_start, gap_start + gap_len)` — a single gap of `missing_ranges` — driving
/// the [`Pacer`] until it is fully present. `counters` persist across gaps.
///
/// Each pass reads the store's current bound ([`RangedStore::total_bytes`]) and
/// clips the gap to it, so a gap past a size a leg has just proved ends at
/// that size. Progress reports `(position, bound)`.
#[allow(clippy::too_many_arguments)]
// One sequential decide -> act -> classify loop. The five `PaceDecision` arms and
// the four-way fault classification each justify a money-relevant decision inline
// (which watermark heals a desync, when an exhaustion is fundable, why a stall is
// terminal); splitting them out would separate those from the loop state they act
// on.
#[allow(clippy::too_many_lines)]
pub(crate) async fn fill_gap<St, S, P, F>(
    store: &St,
    source: &S,
    pacer: &P,
    funder: &F,
    ctx: &Arc<Mutex<PoolContext>>,
    ledger: &Arc<PoolLedger>,
    hash: [u8; 32],
    gap_start: u64,
    gap_len: u64,
    config: &DriveConfig,
    counters: &mut DriveCounters,
    on_progress: Option<&(dyn Fn(u64, u64) + Send + Sync + '_)>,
    // Multi-source only: the shared whole-blob delivered-byte counter every lane
    // folds its own leg deltas into, so the bar reads ONE monotonic position
    // across interleaved lanes rather than each lane's divergent local
    // `base_present + received`. `None` on the single-source path, which reports
    // its own present base directly (there is only ever one lane, so that value is
    // already the whole-blob position).
    progress_agg: Option<&std::sync::atomic::AtomicU64>,
    // Multi-source only: the unit's live progress, which the unit watchdog
    // and a steal read ([`UnitProgress`]).
    unit: Option<&UnitProgress>,
    pacing_wait: Option<&dyn PacingWait>,
    downstream: Option<&(dyn Fn() -> DownstreamFrontier + Send + Sync)>,
    pool: Option<&SharedPool<'_>>,
    // Multi-source only: an end a steal lowers to its split while the gap
    // runs. The gap ends there, and a leg in flight stops there on its open
    // stream ([`BlobSource::stop`]) rather than reading on to its range's end.
    stop_at: Option<&std::sync::atomic::AtomicU64>,
) -> anyhow::Result<()>
where
    St: IngestStore,
    S: BlobSource,
    P: Pacer,
    F: Funder,
{
    // Solvency basis for the deposit gate. On the single-source path (`None`) it
    // is THIS lane's own committed amount — `deposit - own_committed`, unchanged.
    // On the multi-source path a shared reader sums EVERY lane's committed amount,
    // so each worker gates on `pool_deposit - aggregate_committed` — the true
    // SHARED remaining toward the reserved floor, so concurrent lanes drawing on
    // one pool cannot each independently believe the whole deposit is theirs (ADR
    // 039 § Payment model: one deposit backs the whole set). It replaces ONLY the
    // amount subtracted for the deposit gate — the per-leg paid-frontier math below
    // still reads THIS lane's own `committed.bytes`.
    let spent = move |own_amount: U256| pool.map_or(own_amount, |p| (p.spent)());

    // Post-top-up settle state. `awaiting_settle` is set only right after a top-up,
    // and consulted ONLY in the error-classification path below: it gates the
    // bounded settle-wait on an ACTUAL stale-resume refusal from a re-open, not
    // proactively before the retry is even attempted (the pacer always retries the
    // open immediately after a top-up). It stays armed across landed legs until a
    // non-stale fault or the settle budget ends it; `exhaustion_confirmed` is
    // per-open and resets the moment a leg lands.
    let mut awaiting_settle = false;
    let mut settle_waits = 0u32;
    // Waits on a source's stale view of a deposit already near the working
    // target (step 1b). Apart from `settle_waits`, which a top-up re-arms: this
    // path never tops up, so a landed leg re-arms it instead.
    let mut stale_view_waits = 0u32;
    let mut exhaustion_confirmed = false;

    // Paid-frontier anchor (PER-LEG, not per-gap). A "leg" is one contiguous
    // delivery from one successful open. `leg_anchor` records `(content offset the
    // leg opened at, channel-cumulative committed WIRE bytes at that moment)`. It
    // is re-anchored on every successful open (to the previous paid frontier) and
    // on a reseed (to the healed delivered frontier), and PERSISTS across a fault
    // so a post-top-up resume prices the paid frontier against the faulted leg's
    // own spend. Both the completion signal (`paid_cleared`) and the Draw resume
    // start are derived from it; see the per-pass computation at the loop top.
    //
    // Why the PAID frontier and not the store's DELIVERED frontier: `ingest_stream`
    // checkpoints delivered+verified bytes payment-agnostically (ADR 003's credit
    // window lets the node stream up to a full interval before the voucher that
    // pays for it is due), so a mid-leg exhaustion leaves the present-range
    // frontier AHEAD of the last PAID byte — and the whole blob can be delivered in
    // one leg while only part is paid. Gating on delivery would `Done` before the
    // tail is billed (under-pay). `content_paid_frontier` maps the wire an accepted
    // voucher covered on THIS leg back to the largest chunk-group content boundary
    // provably inside it, so completion tracks payment and the resumed open
    // re-delivers (idempotently) and re-bills the tail. Fresh / cross-invocation
    // resume: `paid_wire == 0`, frontier collapses to the leg start (the disk
    // frontier), nothing already-paid is re-pulled (the `cli_fetch_resume` property).
    let mut leg_anchor: Option<(u64, U256)> = None;

    // The last leg that finished cleanly, with the gap's two frontiers as the pass
    // that opened it read them. The next pass must see one of them move (#2194).
    let mut clean_leg: Option<CleanLeg> = None;

    let asked_end = gap_start.saturating_add(gap_len);

    // The start of the last hole this gap warned about, so each distinct hole
    // logs once however many passes it takes to fill.
    let mut warned_hole: Option<u64> = None;

    loop {
        // The store's DELIVERED frontier for this gap: the end of the present
        // bytes contiguous from `gap_start`, which is the start of the gap's
        // first missing range, or the gap's end when nothing is missing. Present
        // bytes past a hole do not count: the gap resumes at the hole. It caps
        // the paid frontier, so the store holds every byte below the completion
        // point and below each resume start. A store that cannot answer is this process's fault,
        // not the source's (#2213). The bound it is clipped to is the bound the
        // planner works to now: a leg that proves a smaller size shrinks it, and
        // the gap ends there.
        // A steal may have lowered the gap's end to its split since the last
        // pass.
        let asked_end = stop_at.map_or(asked_end, |end| asked_end.min(end.load(Ordering::Acquire)));
        let (still_missing, total_bytes) =
            missing_below_bound(store, gap_start, asked_end.saturating_sub(gap_start)).await?;
        let gap_end = asked_end.min(total_bytes);
        if gap_start >= gap_end {
            return Ok(());
        }
        let gap_len = gap_end - gap_start;
        let delivered_frontier =
            contiguous_frontier(&still_missing, total_bytes, gap_start, gap_end);
        warn_on_new_hole(
            &mut warned_hole,
            hash,
            gap_start,
            delivered_frontier,
            gap_end,
            ranges_content_len(&still_missing, total_bytes),
        );

        // Received-byte ceiling (#1895): enforce the source's `max_blob_size_bytes`
        // on the content that has ACTUALLY been received and BLAKE3-verified into the
        // store, never on the peer's unverified signed `total_bytes`.
        // The store holds every byte of `[gap_start, delivered_frontier)`, so once
        // `delivered_frontier` crosses the ceiling the blob is genuinely oversized —
        // abort. A hole below the ceiling holds the frontier at the hole, so the
        // check waits until a later leg fills it. The Draw arm clamps each leg so
        // this fires within one chunk group of the ceiling rather than after a
        // whole-gap `BudgetPacer` draw. `0` = unlimited (and own-origin, whose
        // engine store applies its own cap).
        let max_blob_size_bytes = source.max_blob_size_bytes();
        if max_blob_size_bytes > 0 && delivered_frontier > max_blob_size_bytes {
            return Err(anyhow::Error::new(crate::BlobTooLarge {
                reached: delivered_frontier,
                ceiling: max_blob_size_bytes,
            }));
        }

        let committed = ledger.committed();
        let deposit = locked_deposit(ctx)?;
        let remaining_deposit = deposit.saturating_sub(spent(committed.amount));

        // Anchor the leg on the first pass at `gap_start` with the current committed
        // baseline (fresh / cross-invocation: `paid_wire == 0`, so the frontier is
        // `gap_start` and nothing already-paid is re-pulled). `content_paid_frontier`
        // inverts the wire cost of ONE contiguous delivery from `leg_start`, so it
        // MUST be priced per-leg: `leg_anchor` re-anchors on every successful open to
        // the previous paid frontier, keeping the frontier monotonic and never summing
        // two legs' (proof-duplicating) wire encodings, which would map PAST the true
        // paid frontier and under-pay.
        let (leg_start, leg_baseline) = *leg_anchor.get_or_insert((gap_start, committed.bytes));
        let paid_wire_this_leg =
            u64::try_from(committed.bytes.saturating_sub(leg_baseline)).unwrap_or(u64::MAX);
        // The gap's PAID content frontier, clamped to the store's DELIVERED frontier:
        // a gap is done — and may resume — only at bytes that are BOTH paid AND
        // present, tracking the MIN of the two. `content_paid_frontier` prices ONE
        // leg's paid wire, but the `PoolLedger` is SHARED by every concurrent pull on
        // the channel (`BuyerLedgers`), so a concurrent pull's acked vouchers inflate
        // `committed.bytes` — and thus `paid_wire_this_leg` — past what THIS leg
        // delivered. Left unclamped, that overshoot makes `paid_cleared` report the
        // gap `Done` (or trips the `paid_frontier >= gap_end` spin-guard in the Draw
        // arm) before the bytes are in the store, so `drive` returns `Ok` with the
        // blob incomplete and never finalizes it: success for a blob that is not
        // there. Clamping to `delivered_frontier` is the same guard against that
        // overshoot every resumable pull needs. When the store keeps every byte a leg
        // delivers, delivery runs AHEAD of payment (ADR 003's credit window), so
        // `delivered_frontier >= paid_frontier` and the clamp is a no-op: resume
        // still starts at the true paid frontier and the delivered-but-unpaid tail is
        // still re-billed (no under-pay). The clamp bites in two cases. A
        // shared-ledger concurrent pull overshoots this leg's delivery. Or the store
        // leaves a hole before present bytes: the contiguous delivered frontier stops
        // at the hole, so the paid frontier stops there too and the next leg opens
        // at the hole.
        let paid_frontier =
            crate::sink::content_paid_frontier(leg_start, total_bytes, paid_wire_this_leg)
                .min(gap_end)
                .min(delivered_frontier);
        let paid_cleared = paid_frontier.saturating_sub(gap_start);

        // A clean leg's payment moves the paid frontier to the end of what it
        // delivered, and its bytes land in the store, so this pass sees one of the
        // two frontiers move. One that moved neither would re-open the same range
        // and pay for the same bytes to the same effect. End the gap.
        if let Some(stuck) = clean_leg.take().and_then(|leg| {
            leg.stalled(
                paid_frontier,
                delivered_frontier,
                paid_wire_this_leg,
                ledger.generation(),
            )
        }) {
            return Err(anyhow::Error::new(stuck));
        }

        let downstream_now = downstream.map_or(
            DownstreamFrontier {
                served_paid: paid_frontier,
                serve_demand: 0,
            },
            |f| f(),
        );
        let state = PaceState {
            cleared_bytes: paid_cleared,
            requested_bytes: gap_len,
            remaining_deposit,
            next_voucher_cost: counters.next_voucher_cost,
            working_deposit: config.working_deposit,
            seller_reserve: config.seller_reserve,
            // The reactive-top-up budget is a property of the POOL, not of a
            // lane: `Funder::max_topups` bounds what ONE fetch may escrow, and
            // every lane escrows into the ONE deposit. Multi-source reads the
            // count shared across lanes; single-source reads its own.
            topups_used: pool.map_or(counters.topups_used, |p| {
                p.topups_used.load(Ordering::Acquire)
            }),
            max_topups: funder.max_topups(),
            exhaustion_confirmed,
            // `pulled_frontier` is this leg's own admitted/present frontier —
            // correct on BOTH the client and node pull legs, since it is always
            // THIS leg's delivery progress, never the downstream client's. No
            // seam needed.
            pulled_frontier: delivered_frontier,
            gap_remaining: gap_end.saturating_sub(delivered_frontier),
            // `downstream.served_paid` is NOT this leg's own state — it is the
            // DOWNSTREAM client's paid frontier, which only the node's serve leg
            // advances (`FillSession::advance_served`). On the client path
            // (`downstream == None`) there is no downstream leg, so this
            // collapses to the inert local `paid_frontier`: harmless, because
            // `BudgetPacer` never reads `downstream`. The NODE pull leg
            // MUST pass `Some(reader)` here, reading the shared downstream
            // `served_paid` frontier, so its `WindowPacer` gates the pull against
            // the downstream client's payment — not against this leg's own
            // upstream paid frontier, which would be category-wrong (it would
            // make the window track the node's own credit-window lag instead of
            // the client it is serving).
            downstream: downstream_now,
        };

        match pacer.decide(&state) {
            PaceDecision::Done => return Ok(()),
            decision @ (PaceDecision::Wait | PaceDecision::WaitForMinDraw) => {
                if let Some(hook) = pacing_wait {
                    let reason = if decision == PaceDecision::Wait {
                        WaitReason::WindowFull
                    } else {
                        WaitReason::MinDraw
                    };
                    // Hand the hook the frontiers THIS decision read, so it can
                    // register its wakeup then re-check for an advance that raced the
                    // decision — closing the lost-wakeup that wedged the window-paused
                    // pull under CI scheduling gaps (#1673).
                    hook.wait(downstream_now, reason).await;
                    continue;
                }
                anyhow::bail!(
                    "gap [{gap_start}, +{gap_len}) of blob paced to Wait but no \
                     PacingWait hook was supplied: this is unreachable on the client \
                     path (BudgetPacer never returns Wait) — a WindowPacer caller \
                     must pass a PacingWait"
                );
            }
            PaceDecision::Refuse => {
                return Err(anyhow::Error::new(PoolExhausted { gap_start, gap_len }));
            }
            PaceDecision::TopUp(additional) => {
                // One top-up at a time across the pool's lanes. A sibling's
                // top-up that landed while this lane waited has raised the
                // deposit every lane reads, so decide again rather than escrow a
                // second shortfall.
                let _topping_up = match pool {
                    Some(p) => Some(p.topup_lock.lock().await),
                    None => None,
                };
                if pool.is_some() && locked_deposit(ctx)? > deposit {
                    continue;
                }
                match funder
                    .top_up(additional)
                    .await
                    .map_err(|err| err.context(TopUpFailed))?
                {
                    DepositOutcome::Added(new_deposit) => {
                        // Credit the new deposit through the shared handle so the
                        // source's next open (which clones the context) sees it.
                        // The deposit backs the whole lane set, so multi-source
                        // credits EVERY lane: a lane left on the pre-top-up value
                        // subtracts the aggregate spend from a stale deposit and
                        // walks to a false exhaustion the pool can already fund.
                        match pool {
                            Some(p) => (p.credit)(new_deposit)?,
                            None => {
                                ctx.lock()
                                    .map_err(|_| anyhow::anyhow!("channel context lock poisoned"))?
                                    .deposit = new_deposit;
                            }
                        }
                    }
                    // Typed as an untracked escrow, so the acquire loop ends the
                    // command rather than retrying into a second escrow.
                    DepositOutcome::UnknownPool => {
                        return Err(anyhow::Error::new(EscrowUntracked(format!(
                            "mid-fetch top-up of {additional} landed on-chain but no local \
                             record remains to credit it: the deposit is escrowed and \
                             untracked. Reconcile against the chain before retrying"
                        ))));
                    }
                    DepositOutcome::PoolMismatch => {
                        return Err(anyhow::Error::new(EscrowUntracked(format!(
                            "mid-fetch top-up of {additional} landed on-chain but the local \
                             record now tracks a different pool: the deposit is escrowed \
                             against the topped-up pool. Reconcile against the chain \
                             before retrying"
                        ))));
                    }
                }
                // Spend one unit of the top-up budget — the shared one when lanes
                // draw on one pool, so N lanes cannot each escrow `max_topups`.
                match pool {
                    Some(p) => {
                        p.topups_used.fetch_add(1, Ordering::AcqRel);
                    }
                    None => counters.topups_used = counters.topups_used.saturating_add(1),
                }
                // The node's watcher may not observe this top-up before the next
                // open; wait it out rather than misread the refusal.
                awaiting_settle = true;
                settle_waits = 0;
                exhaustion_confirmed = false;
            }
            // Honors `up_to_bytes` (#1608):
            // `BudgetPacer` returns the full gap remainder, so clamping to it is a
            // no-op there; a `WindowPacer` returns a tighter bound, and clamping
            // `draw_len`/`aligned` here is what actually enforces the window — the
            // open below must never request more than the pacer authorized.
            PaceDecision::Draw { up_to_bytes } => {
                // Draw the UNPAID tail `[paid_frontier, gap_end)`. This is the
                // still-missing part when payment tracks delivery, and additionally
                // the delivered-but-unpaid span `[paid_frontier, delivered_frontier)`
                // after an exhaustion — `ingest_stream` re-writes the already-present
                // bytes idempotently and the pull re-bills them, so the credit-window
                // tail the store checkpointed ahead of payment is finally paid. A
                // hole the store left before present bytes caps the paid frontier
                // at the hole, so the leg opens there and re-delivers the present
                // bytes after it the same idempotent way. The pull pays for those
                // present bytes again: a bounded over-pay, never an under-pay.
                if paid_frontier >= gap_end {
                    // Paid AND delivered to the gap end (`paid_frontier` is clamped to
                    // the delivered frontier above) — the pacer should have returned
                    // `Done`; guard against a spin.
                    return Ok(());
                }
                let resume_start = paid_frontier;
                let aligned = leg_range(
                    resume_start,
                    gap_end,
                    up_to_bytes,
                    max_blob_size_bytes,
                    total_bytes,
                )?;

                // Whole-blob content already present, so `ingest_stream`'s
                // per-range progress can be offset into overall progress: the bar
                // reports `base + received` against `total_bytes`.
                let base_present = ranges_content_len(
                    &(store
                        .present_ranges()
                        .await
                        .map_err(|e| anyhow::Error::new(e).context(crate::LocalPullFault))?),
                    total_bytes,
                );
                // This leg's own previously-reported cumulative, so the multi-source
                // aggregator folds in DELTAS (`received` is monotonic per leg, and
                // resets to 0 on each new open — hence a fresh counter per leg).
                let leg_reported = std::sync::atomic::AtomicU64::new(0);
                // `received` counts from the leg's own start.
                let leg_start = aligned.fetch_start();
                let reporter = move |received: u64| {
                    let delta =
                        received.saturating_sub(leg_reported.swap(received, Ordering::Relaxed));
                    if let Some(unit) = unit {
                        if delta > 0 {
                            unit.first_byte.get_or_init(tokio::time::Instant::now);
                        }
                        unit.verified.fetch_add(delta, Ordering::Relaxed);
                        // `SeqCst`, against the steal that lowers `stop_at`
                        // and then reads this frontier (`Work::steal`).
                        unit.frontier
                            .fetch_max(leg_start.saturating_add(received), Ordering::SeqCst);
                    }
                    let Some(cb) = on_progress else { return };
                    let position = match progress_agg {
                        // Multi-source: fold this leg's monotonic per-leg `received`
                        // into the shared whole-blob total as deltas, so the bar
                        // reads one non-decreasing position across concurrent lanes.
                        // Clamp the readout to the blob size — a bounded, idempotent
                        // tail re-fetch can re-deliver a few already-counted bytes,
                        // and the bar must never exceed 100%.
                        Some(delivered) => delivered
                            .fetch_add(delta, Ordering::Relaxed)
                            .saturating_add(delta)
                            .min(total_bytes),
                        // Single-source: this one lane's present base plus its leg
                        // progress is already the whole-blob position.
                        None => base_present.saturating_add(received),
                    };
                    cb(position, total_bytes);
                };

                // One paid leg: open -> stream into the store -> drain the pull.
                let generation_at_open = ledger.generation();
                let leg: anyhow::Result<()> = match source.open(hash, aligned.clone()).await {
                    Ok((header, reader)) => {
                        counters.next_voucher_cost = voucher_cost(&header);
                        // A new leg has opened: re-anchor the paid-frontier baseline
                        // to THIS open's start and the committed watermark BEFORE it
                        // streams, so a later top-up on this leg prices its paid
                        // frontier against this leg's own wire spend alone (the
                        // pre-#1608 CLI loop re-anchored `fetch_start_offset` /
                        // `fetch_start_committed_bytes` identically on every open).
                        leg_anchor = Some((resume_start, ledger.committed().bytes));
                        // The store is keyed by offset: the leg verifies under
                        // the size its own sender signs, whatever the bound.
                        match store
                            .ingest_stream(
                                &aligned,
                                reader,
                                Some(&reporter),
                                header.total_bytes,
                                stop_at,
                            )
                            .await
                        {
                            Ok((reader, IngestEnd::Drained)) => {
                                // Drain to stream end and recover the acked voucher
                                // watermark. It lives in the ledger the caller owns
                                // (durable persistence is the caller's job); finishing here
                                // enforces wire-byte completeness.
                                source.finish(reader).await.map(|_vp| ())
                            }
                            // A steal lowered the end to a split this leg reached:
                            // pay for the received bytes and close the stream. The
                            // paid and delivered frontiers now reach the split, the
                            // gap's end, so the next pass ends the gap without a
                            // new leg.
                            Ok((reader, IngestEnd::Stopped)) => {
                                source.stop(reader).await.map(|_vp| ())
                            }
                            Err(err) => Err(err),
                        }
                    }
                    Err(err) => Err(err),
                };

                if leg.is_ok() {
                    stale_view_waits = 0;
                }
                if let Err(err) = leg {
                    // --- fault classification (mirrors the CLI loop, REUSING the
                    // shipped predicates) ---

                    let committed = ledger.committed();

                    // 1. A stale-resume refusal while we are waiting out a top-up:
                    //    the node's watcher has not caught up yet. Sleep and retry
                    //    the same sub-range, bounded by the settle budget. An
                    //    `InsufficientDeposit` re-open (option 2 / #2013) qualifies for
                    //    the same wait: right after a `topUp` the node's chain watcher
                    //    may still read the pre-top-up `remaining − M` and refuse the
                    //    authenticated owner with this code, exactly as an honest node's
                    //    range gate refuses a stale offset with `NotFound`.
                    if awaiting_settle
                        && settle_waits < config.max_settle_waits
                        && (resume_may_be_stale(&err) || is_insufficient_deposit(&err))
                    {
                        settle_waits = settle_waits.saturating_add(1);
                        tokio::time::sleep(config.settle_backoff).await;
                        continue;
                    }
                    awaiting_settle = false;

                    // 1b. An open-time `InsufficientDeposit` refusal (option 2 / #2013):
                    //     the serving node proved us the authenticated pool owner and
                    //     told us its refundable floor `M` outruns our pool's remaining
                    //     deposit. Our own ledger says we can still afford the next
                    //     voucher — only the node's private `M` is higher than we
                    //     estimated — so `genuine_exhaustion` below would reject it. Route
                    //     it into the fund-and-retry loop directly: the pacer tops the
                    //     deposit up toward our `working_deposit` ceiling and re-opens,
                    //     clearing the node's `remaining − M ≥ window` gate. If the
                    //     ceiling (or the top-up budget) is already spent the pacer
                    //     refuses on the next pass, ending the fetch truthfully as
                    //     `PoolExhausted` rather than on an ambiguous miss. The ceiling is
                    //     the sole clamp on how much a lying node can make us escrow, so
                    //     trusting this owner-only signal is money-safe.
                    //
                    //     A refusal while the deposit already sits within the low
                    //     water of `working_deposit` is the node's stale view
                    //     instead: a refill made elsewhere (another lane's build,
                    //     a sibling's top-up) has not reached its chain watcher
                    //     yet, and no top-up the pacer would send moves it (the
                    //     pacer refuses a top-up below
                    //     [`crate::pacer::min_reactive_top_up`]). Wait it out on
                    //     its own budget. Past it, the source faults as
                    //     [`StaleDepositView`]: it cools and is asked again,
                    //     rather than read as priced out by a deposit that never
                    //     rises.
                    if is_insufficient_deposit(&err) {
                        let remaining =
                            locked_deposit(ctx)?.saturating_sub(spent(committed.amount));
                        if deposit_near_working(
                            remaining,
                            counters.next_voucher_cost,
                            config.working_deposit,
                        ) {
                            if let Some(stale) =
                                stale_view_wait(remaining, config, stale_view_waits)
                            {
                                return Err(err.context(stale));
                            }
                            stale_view_waits = stale_view_waits.saturating_add(1);
                            tokio::time::sleep(config.settle_backoff).await;
                            continue;
                        }
                        exhaustion_confirmed = true;
                        continue;
                    }

                    // 2 & 3 both read the shared context. The guard is held only
                    // across synchronous reads, never across the heal's `.await`.
                    let watermark = {
                        let guard = ctx
                            .lock()
                            .map_err(|_| anyhow::anyhow!("channel context lock poisoned"))?;
                        rejection_watermark(&err, &guard)
                    };

                    // 2. Desync heal (driver-owned, NOT a PaceDecision): an
                    //    authenticated bundle that ADVANCES our committed
                    //    watermark means the node holds a voucher we lost, an
                    //    `Underpaid` bundle BEHIND it means we hold vouchers the
                    //    node never took, and a `BytesRegression` bundle behind
                    //    on amount but ahead on bytes means the lane resumed
                    //    below a voucher the node holds — heal the ledger and
                    //    retry. A heal past the resume budget still leaves the
                    //    ledger where the node holds it, but this source kept
                    //    rejecting after each heal: mark the rejection
                    //    `HealExhausted`, so the source cools rather than the
                    //    command ending. A rejection that no heal takes stays
                    //    bare and falls through to step 4. Only a lane-watermark
                    //    reason is marked (`scopes_to_source`): a spending-cap
                    //    rejection reaches step 3's check instead.
                    let healed = match watermark {
                        Some(watermark) => heal_watermark_desync(&err, watermark, ledger).await,
                        None => None,
                    };
                    let past_budget = counters.resume_attempts >= MAX_RESUME_ATTEMPTS;
                    if healed.is_some() && past_budget && scopes_to_source(&err) {
                        return Err(err.context(HealExhausted));
                    }
                    let desync = healed.is_some() && !past_budget;

                    // 3. Genuine exhaustion (corroborated against our OWN
                    //    ledger): let the pacer fund it on the next pass. The
                    //    store already checkpointed the paid prefix, so the
                    //    retry re-opens only the un-checkpointed tail.
                    let exhausted = !desync && {
                        let guard = ctx
                            .lock()
                            .map_err(|_| anyhow::anyhow!("channel context lock poisoned"))?;
                        let remaining = guard.deposit.saturating_sub(spent(committed.amount));
                        genuine_exhaustion(
                            &err,
                            &guard,
                            committed,
                            remaining,
                            counters.next_voucher_cost,
                        )
                    };
                    let classify = (desync, exhausted);

                    if classify.0 {
                        counters.resume_attempts = counters.resume_attempts.saturating_add(1);
                        log_healed_retry(
                            hash,
                            resume_start,
                            counters.resume_attempts,
                            &err,
                            healed,
                        );
                        // A reseed means the node HOLDS vouchers for content it
                        // already delivered that our record had lost — so the
                        // delivered frontier is paid. A rebase means the node took
                        // none of the vouchers above its watermark, so the span
                        // between is delivered and unpaid; the node already chose to
                        // stop there, and the healed ledger pays only for what it
                        // delivers next. Either way, re-anchor the leg there with the
                        // healed committed baseline, so the next pass prices this
                        // gap's paid frontier AT the delivered frontier (`paid_wire
                        // delta == 0`): no re-delivery of bytes already delivered, and the
                        // reseed's jump is not mistaken for this leg's own spend
                        // (which would map past the true frontier and under-pay).
                        // `delivered_frontier` is this pass's pre-ingest snapshot
                        // (loop top); if the reseed fault arrived after some bytes
                        // were checkpointed this pass it sits slightly behind the
                        // fresh frontier. That is the SAFE direction — at worst a
                        // bounded re-deliver/re-pay of the interim span (over-pay),
                        // never under-pay — and reseed refusals almost always
                        // surface at open, before any ingest, so the two coincide.
                        leg_anchor = Some((delivered_frontier, ledger.committed().bytes));
                        continue;
                    }
                    if classify.1 {
                        exhaustion_confirmed = true;
                        continue;
                    }

                    // 4. Anything else — a stall, reset, hash mismatch, local I/O
                    //    fault, a voucher rejection no heal takes — is terminal.
                    return Err(err);
                }

                // The leg landed: a fresh, healthy open resets the per-open state.
                // The settle allowance stays armed, with whatever budget is left: a
                // landed leg proves only that the upstream admitted ONE stream, maybe
                // on headroom it computed before its watcher saw the top-up, so a
                // later re-open can still meet the same stale refusal. The next
                // top-up re-arms the budget; a stale refusal past it is terminal.
                exhaustion_confirmed = false;
                clean_leg = Some(CleanLeg {
                    offset: aligned.fetch_start(),
                    len: aligned.fetch_len(),
                    paid_frontier,
                    delivered_frontier,
                    generation: generation_at_open,
                });
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation
)] // tests
mod tests;
