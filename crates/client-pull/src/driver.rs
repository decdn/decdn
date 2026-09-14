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
//! funds, and resumes one missing range at a time. The CLI's `decdn fetch` and
//! `bundle pull` run `drive` as their fetch core (via `drive_fetch` in
//! `crates/cli/src/commands/fetch.rs`). The branches are:
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
//! is one implementer (the CLI/client backend, writing `.partial`/`.obao4`); a
//! node backend admits to the cache and tees to its downstream client
//! through the same seam.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::U256;
use bao_tree::ChunkRanges;
use decdn_bao_range::{CHUNK_GROUP_BYTES, align_range};
use decdn_incentive::DepositOutcome;

use crate::pacer::{DownstreamFrontier, PaceDecision, PaceState};
use crate::source::{BlobSource, Funder, IngestStore};
use crate::{
    Cumulative, MAX_RESUME_ATTEMPTS, Pacer, PoolContext, PoolLedger, ProgressCallback,
    UpstreamPullHeader, genuine_exhaustion, reject_empty_claim_for_nonempty_root,
    resumable_watermark, resume_may_be_stale,
};

/// The shared pool cannot fund the next voucher: its remaining deposit is below
/// the next voucher's cost and reactive top-up is disabled or exhausted (the
/// pacer returned [`PaceDecision::Refuse`]).
///
/// Typed rather than a bare string so the failover classifier
/// ([`crate::retry_disposition`]) can `downcast_ref` and rule it **terminal** for
/// BOTH fetch paths: the pool is the same deposit against every provider (ADR
/// 003), so reassigning the range to another lane — or failing over to another
/// candidate — cannot fund it. The single-source path already ended the fetch on
/// a refuse; the multi-source scheduler needs the typed shape to abort promptly
/// instead of dropping every lane one by one and masking it as "all sources
/// failed".
#[derive(Debug)]
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
pub struct SharedPool<'a> {
    /// Sum, across every lane, of the committed voucher amount — the pool's
    /// total spend so far.
    pub spent: &'a (dyn Fn() -> U256 + Send + Sync),
    /// Reactive top-ups this FETCH has spent, across every lane.
    pub topups_used: &'a AtomicU32,
    /// Credit a landed top-up's new deposit to EVERY lane's `PoolContext`, so no
    /// lane gates on a stale deposit.
    pub credit: &'a (dyn Fn(U256) -> anyhow::Result<()> + Send + Sync),
}

impl std::fmt::Debug for SharedPool<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedPool")
            .field("spent", &(self.spent)())
            .field("topups_used", &self.topups_used.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// The injected wait signal for [`PaceDecision::Wait`] (ADR 037): the
/// node hands in an implementor that resolves once its serve leg's paid frontier
/// or demand frontier has advanced (so a re-decide has a chance of finding room);
/// the client path never needs one, since `BudgetPacer` never returns `Wait`.
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
    fn wait(&self, observed: DownstreamFrontier) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
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

/// Bytes per [`bao_tree::ChunkNum`] — a 1 KiB bao chunk. A gap's byte span is its
/// chunk-range boundaries scaled by this.
const CHUNK_BYTES: u64 = 1024;

/// Deployment knobs the pure gap/pay core needs from its caller (CLI: #1497's
/// `MAX_TOPUP_SETTLE_WAITS` / `TOPUP_SETTLE_BACKOFF`; node: its own smaller
/// budgets). Kept minimal — progress and deadlines stay with the caller.
#[derive(Debug, Clone, Copy)]
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
pub const MAX_TOPUP_SETTLE_WAITS: u32 = 30;
/// Backoff between resume-open retries while waiting for the node's chain watcher
/// to observe a just-landed top-up (see [`MAX_TOPUP_SETTLE_WAITS`]).
pub const TOPUP_SETTLE_BACKOFF: Duration = Duration::from_millis(500);

/// How often a fetch's single flush owner persists the `.ranges` present record
/// while sources are still delivering. `ClientRangedStore::checkpoint` fsyncs
/// data and outboard every ~4 MiB but no longer writes the record, so this
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
/// property `ClientRangedStore::checkpoint` relies on. Returns once `fut`
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
    loop {
        tokio::select! {
            // Prefer completion: if the work is done, finish rather than flush —
            // the caller's final flush persists the terminal snapshot.
            biased;
            res = &mut fut => return res,
            _ = tick.tick() => store.flush_present_record()?,
        }
    }
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
///    `Wait` / `Done` / `Refuse`. A `Draw` opens the gap's [`AlignedRange`](decdn_bao_range::AlignedRange) through the
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
/// fault that is neither a healable desync nor a fundable exhaustion, an escrowed-
/// but-untracked top-up outcome, or a `finalize` failure.
#[allow(clippy::too_many_arguments)]
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
    on_progress: Option<&ProgressCallback>,
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
        let base_present = ranges_content_len(&store.present_ranges().await?, total_bytes);
        cb(base_present, total_bytes);
    }

    let missing = store.missing_ranges(offset, len).await?;
    let gaps = contiguous_byte_ranges(&missing, total_bytes);

    // Fill every gap while a single periodic tick flushes the `.ranges` present
    // record (spec §5.5, single-writer flush point). `ClientRangedStore::checkpoint`
    // no longer persists the record per checkpoint, so without this interval flush
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
                total_bytes,
                config,
                &mut counters,
                on_progress,
                // `None`: `drive` is the single-source path, whose one lane reports
                // its own present base directly — there is no cross-lane total to
                // aggregate. Only the multi-source scheduler passes an aggregator.
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
    let flushed = store.flush_present_record();
    outcome?;
    flushed?;

    // Promote only when the WHOLE blob is present — `finalize` verifies and
    // renames the whole `.partial`, which it cannot do while bytes outside `R`
    // are still missing. A whole-blob `R` reaches this complete; a partial `R`
    // leaves the `.partial` in place for a later fetch to finish.
    if store.is_complete().await? {
        store.finalize().await?;
    }
    Ok(())
}

/// The per-gap resume/pay loop. Fills the contiguous content span
/// `[gap_start, gap_start + gap_len)` — a single gap of `missing_ranges` — driving
/// the [`Pacer`] until it is fully present. `counters` persist across gaps.
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
    total_bytes: u64,
    config: &DriveConfig,
    counters: &mut DriveCounters,
    on_progress: Option<&ProgressCallback>,
    // Multi-source only: the shared whole-blob delivered-byte counter every lane
    // folds its own leg deltas into, so the bar reads ONE monotonic position
    // across interleaved lanes rather than each lane's divergent local
    // `base_present + received`. `None` on the single-source path, which reports
    // its own present base directly (there is only ever one lane, so that value is
    // already the whole-blob position).
    progress_agg: Option<&std::sync::atomic::AtomicU64>,
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

    let gap_end = gap_start.saturating_add(gap_len);

    loop {
        // The store's DELIVERED frontier for this gap (contiguous from `gap_start`):
        // where its present ranges end. Used only to re-anchor after a reseed and
        // for the progress bar base — NOT for completion, which is payment-based.
        let still_missing = store.missing_ranges(gap_start, gap_len).await?;
        let missing_bytes = ranges_content_len(&still_missing, total_bytes);
        let delivered_frontier = gap_end.saturating_sub(missing_bytes);

        // Received-byte ceiling (#1895): enforce the source's `max_blob_size_bytes`
        // on the content that has ACTUALLY been received and BLAKE3-verified into the
        // store, never on the peer's unverified signed `total_bytes`.
        // `delivered_frontier` is the absolute content offset present from the blob
        // start, so once it crosses the ceiling the blob is genuinely oversized —
        // abort. The Draw arm clamps each leg so this fires within one chunk group of
        // the ceiling rather than after a whole-gap `BudgetPacer` draw. `0` =
        // unlimited (and own-origin, whose engine store applies its own cap).
        let max_blob_size_bytes = source.max_blob_size_bytes();
        if max_blob_size_bytes > 0 && delivered_frontier > max_blob_size_bytes {
            return Err(anyhow::Error::new(crate::BlobTooLarge {
                received: delivered_frontier,
                ceiling: max_blob_size_bytes,
            }));
        }

        let committed = ledger.committed();
        let remaining_deposit = locked_deposit(ctx)?.saturating_sub(spent(committed.amount));

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
        // overshoot every resumable pull needs. On a solo pull and the whole client path
        // delivery runs AHEAD of payment (ADR 003's credit window), so
        // `delivered_frontier >= paid_frontier` and the clamp is a NO-OP — resume
        // still starts at the true paid frontier and the delivered-but-unpaid tail is
        // still re-billed (no under-pay). It bites ONLY when a shared-ledger
        // concurrent pull overshoots this leg's delivery.
        let paid_frontier =
            crate::sink::content_paid_frontier(leg_start, total_bytes, paid_wire_this_leg)
                .min(gap_end)
                .min(delivered_frontier);
        let paid_cleared = paid_frontier.saturating_sub(gap_start);

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
            PaceDecision::Wait => {
                if let Some(hook) = pacing_wait {
                    // Hand the hook the frontiers THIS decision read, so it can
                    // register its wakeup then re-check for an advance that raced the
                    // decision — closing the lost-wakeup that wedged the window-paused
                    // pull under CI scheduling gaps (#1673).
                    hook.wait(downstream_now).await;
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
                match funder.top_up(additional).await? {
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
                    DepositOutcome::UnknownPool => {
                        anyhow::bail!(
                            "mid-fetch top-up of {additional} landed on-chain but no local \
                             record remains to credit it: the deposit is escrowed and \
                             untracked. Reconcile against the chain before retrying"
                        );
                    }
                    DepositOutcome::PoolMismatch => {
                        anyhow::bail!(
                            "mid-fetch top-up of {additional} landed on-chain but the local \
                             record now tracks a different pool: the deposit is escrowed \
                             against the topped-up pool. Reconcile against the chain \
                             before retrying"
                        );
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
                // tail the store checkpointed ahead of payment is finally paid.
                if paid_frontier >= gap_end {
                    // Paid AND delivered to the gap end (`paid_frontier` is clamped to
                    // the delivered frontier above) — the pacer should have returned
                    // `Done`; guard against a spin.
                    return Ok(());
                }
                let resume_start = paid_frontier;
                let draw_len = gap_end.saturating_sub(resume_start).min(up_to_bytes);
                // Received-byte ceiling (#1895): cap this leg so the delivered
                // frontier can exceed `max_blob_size_bytes` by at most one chunk
                // group, at which point the loop-top check aborts. Without this a
                // `BudgetPacer` (which draws the whole gap remainder) would pull an
                // entire oversized blob before that check ever runs. `resume_start` is
                // always at or below the ceiling here — a `resume_start` past it means
                // the loop-top check already aborted — so `cap_end - resume_start` is
                // at least one chunk group and never collapses `draw_len` to the `0`
                // ("to end") sentinel. `0` = unlimited.
                let draw_len = if max_blob_size_bytes > 0 {
                    let cap_end = max_blob_size_bytes.saturating_add(CHUNK_GROUP_BYTES);
                    draw_len.min(cap_end.saturating_sub(resume_start))
                } else {
                    draw_len
                };
                let aligned = align_range(resume_start, draw_len, total_bytes)?;

                // Whole-blob content already present, so `ingest_stream`'s
                // per-range progress can be offset into overall progress: the bar
                // reports `base + received` against `total_bytes`.
                let base_present =
                    ranges_content_len(&(store.present_ranges().await?), total_bytes);
                // This leg's own previously-reported cumulative, so the multi-source
                // aggregator folds in DELTAS (`received` is monotonic per leg, and
                // resets to 0 on each new open — hence a fresh counter per leg).
                let leg_reported = std::sync::atomic::AtomicU64::new(0);
                let reporter = move |received: u64| {
                    let Some(cb) = on_progress else { return };
                    let position = match progress_agg {
                        // Multi-source: fold this leg's monotonic per-leg `received`
                        // into the shared whole-blob total as deltas, so the bar
                        // reads one non-decreasing position across concurrent lanes.
                        // Clamp the readout to the blob size — a bounded, idempotent
                        // tail re-fetch can re-deliver a few already-counted bytes,
                        // and the bar must never exceed 100%.
                        Some(delivered) => {
                            let delta =
                                received.saturating_sub(leg_reported.load(Ordering::Relaxed));
                            leg_reported.store(received, Ordering::Relaxed);
                            delivered
                                .fetch_add(delta, Ordering::Relaxed)
                                .saturating_add(delta)
                                .min(total_bytes)
                        }
                        // Single-source: this one lane's present base plus its leg
                        // progress is already the whole-blob position.
                        None => base_present.saturating_add(received),
                    };
                    cb(position, total_bytes);
                };

                // One paid leg: open -> stream into the store -> drain the pull.
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
                        match store.ingest_stream(&aligned, reader, Some(&reporter)).await {
                            Ok(reader) => {
                                // Drain to stream end and recover the acked voucher
                                // watermark. It lives in the ledger the caller owns
                                // (durable persistence is the caller's job); finishing here
                                // enforces wire-byte completeness.
                                source.finish(reader).await.map(|_vp| ())
                            }
                            Err(err) => Err(err),
                        }
                    }
                    Err(err) => Err(err),
                };

                if let Err(err) = leg {
                    // --- fault classification (mirrors the CLI loop, REUSING the
                    // shipped predicates) ---

                    let committed = ledger.committed();

                    // 1. A stale-resume refusal while we are waiting out a top-up:
                    //    the node's watcher has not caught up yet. Sleep and retry
                    //    the same sub-range, bounded by the settle budget.
                    if awaiting_settle
                        && settle_waits < config.max_settle_waits
                        && resume_may_be_stale(&err)
                    {
                        settle_waits = settle_waits.saturating_add(1);
                        tokio::time::sleep(config.settle_backoff).await;
                        continue;
                    }
                    awaiting_settle = false;

                    // 2 & 3 both read the shared context. Neither `resumable_watermark`
                    // nor `genuine_exhaustion` awaits, so the guard is held only
                    // across these synchronous predicate calls (never an `.await`).
                    let classify = {
                        let guard = ctx
                            .lock()
                            .map_err(|_| anyhow::anyhow!("channel context lock poisoned"))?;
                        let remaining = guard.deposit.saturating_sub(spent(committed.amount));

                        // 2. Desync heal (driver-owned, NOT a PaceDecision): an
                        //    authenticated bundle that ADVANCES our committed
                        //    watermark means the node holds a voucher we lost —
                        //    reseed and retry.
                        let desync = counters.resume_attempts < MAX_RESUME_ATTEMPTS
                            && resumable_watermark(&err, &guard)
                                .is_some_and(|bundle| ledger.reseed(Cumulative::from(bundle)));

                        // 3. Genuine exhaustion (corroborated against our OWN
                        //    ledger): let the pacer fund it on the next pass. The
                        //    store already checkpointed the paid prefix, so the
                        //    retry re-opens only the un-checkpointed tail.
                        let exhausted = !desync
                            && genuine_exhaustion(
                                &err,
                                &guard,
                                committed,
                                remaining,
                                counters.next_voucher_cost,
                            );
                        (desync, exhausted)
                    };

                    if classify.0 {
                        counters.resume_attempts = counters.resume_attempts.saturating_add(1);
                        // A reseed means the node HOLDS vouchers for content it
                        // already delivered that our record had lost — so the
                        // delivered frontier is paid. Re-anchor the leg there with the
                        // healed committed baseline, so the next pass prices this
                        // gap's paid frontier AT the delivered frontier (`paid_wire
                        // delta == 0`): no re-delivery of already-paid bytes, and the
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
                    //    fault — is terminal.
                    return Err(err);
                }

                // The leg landed: a fresh, healthy open resets the per-open state.
                // The settle allowance stays armed, with whatever budget is left: a
                // landed leg proves only that the upstream admitted ONE stream, maybe
                // on headroom it computed before its watcher saw the top-up, so a
                // later re-open can still meet the same stale refusal. The next
                // top-up re-arms the budget; a stale refusal past it is terminal.
                exhaustion_confirmed = false;
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
mod tests {
    use std::sync::{Arc, Mutex};

    use alloy::primitives::{Address, B256, U256};
    use alloy::signers::local::PrivateKeySigner;
    use bao_tree::io::outboard::PreOrderMemOutboard;
    use bytes::Bytes;
    use decdn_bao_range::{
        AlignedRange, CHUNK_GROUP_BYTES, IROH_BLOCK_SIZE, RangedStore, align_range,
    };
    use decdn_incentive::DepositOutcome;

    use super::{DriveConfig, contiguous_byte_ranges, drive, ranges_content_len};
    use crate::ProgressCallback;
    use crate::pacer::{BudgetPacer, PaceDecision, PaceState};
    use crate::source::{BlobSource, FakeFunder, ScriptedSource, SourceFuture};
    use crate::{
        ClientRangedStore, Cumulative, PoolContext, PoolLedger, UpstreamPullHeader,
        UpstreamVoucherRejected, VoucherProgress,
    };
    use decdn_protocol::client::VoucherRejectReason;

    const GROUP: u64 = CHUNK_GROUP_BYTES;

    /// A deterministic (xorshift) blob plus its bao root and full pre-order
    /// outboard — the same synth the ranged-store and conformance suites use, so
    /// the wire a `ScriptedSource` yields and the bao an `admit` verifies agree.
    fn synth_blob(len: usize) -> ([u8; 32], Vec<u8>, Bytes) {
        let mut plaintext = vec![0u8; len];
        let mut x: u32 = 0x9e37_79b9;
        for b in &mut plaintext {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *b = x.to_le_bytes()[0];
        }
        let ob = PreOrderMemOutboard::create(&plaintext, IROH_BLOCK_SIZE);
        (*ob.root.as_bytes(), plaintext, Bytes::from(ob.data))
    }

    /// Combined-format bao (8-byte header + body) for `aligned`, ready for
    /// `RangedStore::admit`.
    fn bao_for(root: [u8; 32], plaintext: &[u8], outboard: Bytes, aligned: &AlignedRange) -> Bytes {
        let s = aligned.fetch_start() as usize;
        let e = aligned.fetch_end() as usize;
        decdn_bao_range::encode_verified_range(root, aligned, &plaintext[s..e], outboard)
            .expect("verifies")
    }

    fn tmp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("tmp dir")
    }

    /// A fresh `.partial` store whose tempdir outlives the test (leaked, OS
    /// reclaims at exit — same pattern the ranged-store unit tests use).
    fn fresh_store(root: [u8; 32], total: u64) -> ClientRangedStore {
        let dir = tmp_dir();
        let store = ClientRangedStore::create(dir.path(), "blob", root, total).expect("create");
        std::mem::forget(dir);
        store
    }

    /// A healthy buyer context: a huge deposit so the pacer never has to top up
    /// (the money assertions exercise the gap logic, not funding).
    fn healthy_ctx() -> PoolContext {
        let signer = PrivateKeySigner::random();
        PoolContext {
            pool_id: B256::ZERO,
            // Pinned to a non-zero test provider: `send_voucher` fast-fails on
            // `Address::ZERO` (an unpinned lane), so every driver test that
            // actually signs a voucher needs a real-looking address here.
            provider: Address::repeat_byte(0xAB),
            deposit: U256::from(u128::MAX),
            client_signer: Arc::new(signer),
            voucher_domain: decdn_incentive::bind_node_id_domain(1, Address::ZERO),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        }
    }

    fn healthy_funder() -> FakeFunder {
        FakeFunder::new(3, DepositOutcome::Added(U256::from(u128::MAX)))
    }

    fn config() -> DriveConfig {
        DriveConfig {
            working_deposit: U256::from(u128::MAX),
            seller_reserve: U256::ZERO,
            max_settle_waits: 2,
            settle_backoff: std::time::Duration::from_millis(0),
        }
    }

    /// Admit `aligned` into `store` from a freshly-synthesized outboard — a
    /// pre-held range for the gap assertions.
    async fn preadmit(
        store: &ClientRangedStore,
        plaintext: &[u8],
        outboard: &Bytes,
        aligned: &AlignedRange,
    ) {
        let bao = bao_for(store.root(), plaintext, outboard.clone(), aligned);
        store.admit(aligned.clone(), bao).await.expect("preadmit");
    }

    /// Drive the whole blob and assert: the source opened EXACTLY the contiguous
    /// gaps of `missing_ranges(0, 0)` (never the held range), the total bytes
    /// opened equal the gap bytes (not the whole blob), the store finalized, and
    /// the assembled bytes are byte-exact.
    async fn assert_drives_only_gaps(total: u64, held: &[(u64, u64)], expected_gaps: usize) {
        let (root, plaintext, outboard) = synth_blob(total as usize);
        let store = fresh_store(root, total);

        for (off, len) in held {
            let aligned = align_range(*off, *len, total).expect("align held");
            preadmit(&store, &plaintext, &outboard, &aligned).await;
        }

        // The gaps we EXPECT the driver to pull, computed before the drive.
        let missing = store.missing_ranges(0, 0).await.expect("missing");
        let want_gaps = contiguous_byte_ranges(&missing, total);
        assert_eq!(
            want_gaps.len(),
            expected_gaps,
            "scenario shape: expected {expected_gaps} gaps, got {want_gaps:?}"
        );
        let want_gap_bytes: u64 = want_gaps.iter().map(|(_, l)| *l).sum();

        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(plaintext.clone())
            .expect("source")
            .paying(Arc::clone(&ledger));
        assert_eq!(source.root(), root);
        let pacer = BudgetPacer::new();
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));

        drive(
            &store,
            &source,
            &pacer,
            &funder,
            &ctx,
            &ledger,
            root,
            0,
            0,
            &config(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("drive whole blob");

        // THE MONEY ASSERTION: opened == the gaps exactly. Held ranges never
        // opened, so held bytes are never re-pulled and never re-paid.
        assert_eq!(
            source.opened_ranges(),
            want_gaps,
            "the source must open exactly the contiguous gaps, in order"
        );
        assert_eq!(
            source.opened_bytes(),
            want_gap_bytes,
            "bytes opened must equal the gap bytes, not the whole blob"
        );
        assert!(
            source.opened_bytes() < total || held.is_empty(),
            "with a held range, fewer than the whole blob's bytes must be pulled"
        );
        assert_eq!(
            funder.calls(),
            Vec::<U256>::new(),
            "healthy deposit: no top-ups"
        );

        // The blob is complete, finalized, and byte-exact.
        assert!(store.is_complete().await.expect("is_complete"));
        let got = store.read(0, 0).await.expect("read whole blob");
        assert_eq!(
            got.as_ref(),
            plaintext.as_slice(),
            "assembled bytes byte-exact"
        );
    }

    #[tokio::test]
    async fn interior_hold_leaves_two_gaps_and_pulls_only_them() {
        // Hold the middle group of a 4-group blob -> a prefix gap and a suffix gap.
        assert_drives_only_gaps(4 * GROUP, &[(GROUP, GROUP)], 2).await;
    }

    #[tokio::test]
    async fn prefix_hold_leaves_one_suffix_gap() {
        // Hold the first two groups of a 4-group blob -> a single suffix gap.
        assert_drives_only_gaps(4 * GROUP, &[(0, 2 * GROUP)], 1).await;
    }

    #[tokio::test]
    async fn disjoint_holds_leave_three_gaps() {
        // Hold groups 1 and 3 of a 5-group blob -> gaps [0], [2], [4].
        assert_drives_only_gaps(5 * GROUP, &[(GROUP, GROUP), (3 * GROUP, GROUP)], 3).await;
    }

    /// A resumed drive surfaces the already-present base on the progress bar
    /// BEFORE the first chunk is delivered: the very first `on_progress` position
    /// is the held prefix's content length (against the whole-blob total), not
    /// `0` and not `base + first-chunk`. `fill_gap`'s per-gap reporter fires only
    /// from inside `ingest_stream` once streaming begins, so without the
    /// pre-stream emit a resumed blob's bar sits at `0` through the pre-fetch
    /// window (discovery, channel open, pool resolve), then jumps to the resume
    /// point on the first delivered chunk.
    #[tokio::test]
    async fn resume_base_is_reported_before_the_first_chunk() {
        let total = 4 * GROUP;
        let (root, plaintext, outboard) = synth_blob(total as usize);
        let store = fresh_store(root, total);

        // Hold the first two groups: the resume base is two groups of content.
        let held = align_range(0, 2 * GROUP, total).expect("align held");
        preadmit(&store, &plaintext, &outboard, &held).await;
        let base_present =
            ranges_content_len(&store.present_ranges().await.expect("present"), total);
        assert_eq!(base_present, 2 * GROUP, "scenario: two groups held");

        let samples: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let cb_samples = Arc::clone(&samples);
        let on_progress: Box<ProgressCallback> = Box::new(move |received, expected| {
            if let Ok(mut s) = cb_samples.lock() {
                s.push((received, expected));
            }
        });

        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(plaintext.clone())
            .expect("source")
            .paying(Arc::clone(&ledger));
        let pacer = BudgetPacer::new();
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));

        drive(
            &store,
            &source,
            &pacer,
            &funder,
            &ctx,
            &ledger,
            root,
            0,
            0,
            &config(),
            Some(&on_progress),
            None,
            None,
            None,
        )
        .await
        .expect("drive resumes");

        let samples = samples.lock().expect("samples lock").clone();
        let first = *samples.first().expect("at least one progress sample");
        assert_eq!(
            first,
            (base_present, total),
            "the first reported position must be the resume base, emitted before \
             the first delivered chunk"
        );
        // And it never regresses and reaches the whole blob.
        let mut prev = 0u64;
        for (received, expected) in &samples {
            assert_eq!(*expected, total, "the total stays the whole-blob size");
            assert!(
                *received >= prev,
                "progress regressed: {received} after {prev}"
            );
            prev = *received;
        }
        assert_eq!(prev, total, "the final position reaches the whole blob");
    }

    #[tokio::test]
    async fn fully_held_blob_opens_nothing() {
        let total = 3 * GROUP + 123;
        let (root, plaintext, outboard) = synth_blob(total as usize);
        let store = fresh_store(root, total);
        let whole = align_range(0, 0, total).expect("align whole");
        preadmit(&store, &plaintext, &outboard, &whole).await;

        let source = ScriptedSource::new(plaintext.clone()).expect("source");
        let pacer = BudgetPacer::new();
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));

        drive(
            &store,
            &source,
            &pacer,
            &funder,
            &ctx,
            &ledger,
            root,
            0,
            0,
            &config(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("drive fully-held blob");

        assert!(
            source.opened_ranges().is_empty(),
            "a fully-held blob opens nothing"
        );
        assert!(store.is_complete().await.expect("is_complete"));
        // A fully-held blob still finalizes (promotes) when driven whole.
        let got = store.read(0, 0).await.expect("read");
        assert_eq!(got.as_ref(), plaintext.as_slice());
    }

    #[tokio::test]
    async fn ragged_tail_blob_drives_whole() {
        // A blob whose final group is partial: the single gap runs to total_bytes,
        // and the driver opens exactly it.
        assert_drives_only_gaps(2 * GROUP + 777, &[], 1).await;
    }

    #[tokio::test]
    async fn mid_gap_fault_resumes_at_the_checkpoint_not_the_whole_gap() {
        // A blob large enough to cross a 4 MiB ingest checkpoint before a scripted
        // fault lands. The first `drive` opens the whole gap and faults mid-stream
        // (a generic stall is terminal within one drive — exactly the CLI loop's
        // behaviour); the store durably checkpoints the received prefix. A SECOND
        // `drive` (the resume: a new invocation, another peer) re-opens ONLY the
        // un-checkpointed tail, never re-pulling — and never re-paying for — the
        // checkpointed prefix.
        let total: u64 = 8 * 1024 * 1024;
        let plaintext = {
            let mut v = vec![0u8; total as usize];
            let mut x: u32 = 0x1234_5678;
            for b in &mut v {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                *b = x.to_le_bytes()[0];
            }
            v
        };

        // Fault after 5 MiB of wire on any range longer than that. The whole-blob
        // open (> 5 MiB of wire) faults; the tail re-open (the checkpointed ~4 MiB
        // is already held, so < 5 MiB of wire remains) is under the threshold and
        // completes. One source instance across both drives, so `opened_ranges`
        // records both opens.
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(plaintext.clone())
            .expect("source")
            .with_fault_after(5 * 1024 * 1024, || {
                anyhow::anyhow!("scripted mid-gap stall")
            })
            .paying(Arc::clone(&ledger));
        let root = source.root();
        let store = fresh_store(root, total);

        let pacer = BudgetPacer::new();
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));

        // Drive #1: faults mid-gap and returns the terminal stall, but checkpoints
        // a durable prefix into the store first.
        let err = drive(
            &store,
            &source,
            &pacer,
            &funder,
            &ctx,
            &ledger,
            root,
            0,
            0,
            &config(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect_err("the mid-gap stall surfaces as a terminal error");
        assert!(
            format!("{err:#}").contains("scripted mid-gap stall"),
            "the parked fault must be the terminal error: {err:#}"
        );
        assert!(
            !store.is_complete().await.expect("is_complete"),
            "blob not yet complete"
        );

        // Drive #2: resume. Only the un-checkpointed tail is still missing.
        drive(
            &store,
            &source,
            &pacer,
            &funder,
            &ctx,
            &ledger,
            root,
            0,
            0,
            &config(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("resume completes the blob");

        let opened = source.opened_ranges();
        assert_eq!(
            opened.len(),
            2,
            "one faulting open + one tail re-open: {opened:?}"
        );
        // First open: the whole blob.
        assert_eq!(opened[0], (0, total));
        // Second open: a tail strictly inside the blob, starting past 0 and
        // spanning fewer bytes than the whole gap (the checkpointed prefix was
        // NOT re-pulled, hence never re-paid).
        let (tail_start, tail_len) = opened[1];
        assert!(
            tail_start > 0,
            "the re-open must skip the checkpointed prefix, got start {tail_start}"
        );
        assert!(
            tail_len < total,
            "the re-open must not re-pull the whole gap, got len {tail_len}"
        );
        assert_eq!(
            tail_start + tail_len,
            total,
            "the re-open must run to the blob end"
        );

        assert!(store.is_complete().await.expect("is_complete"));
        let got = store.read(0, 0).await.expect("read");
        assert_eq!(
            got.as_ref(),
            plaintext.as_slice(),
            "byte-exact after resume"
        );
    }

    #[tokio::test]
    async fn partial_request_pulls_only_its_gap_and_leaves_partial() {
        // R is one interior group of a 4-group blob; nothing held. The driver
        // fills exactly that group and, because the rest of the blob is still
        // missing, does NOT finalize.
        let total = 4 * GROUP;
        let (root, plaintext, _outboard) = synth_blob(total as usize);
        let store = fresh_store(root, total);

        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(plaintext.clone())
            .expect("source")
            .paying(Arc::clone(&ledger));
        let pacer = BudgetPacer::new();
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));

        drive(
            &store,
            &source,
            &pacer,
            &funder,
            &ctx,
            &ledger,
            root,
            GROUP,
            GROUP,
            &config(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("drive interior range");

        assert_eq!(source.opened_ranges(), vec![(GROUP, GROUP)]);
        assert!(
            !store.is_complete().await.expect("is_complete"),
            "blob still partial"
        );
        // The requested range is readable and byte-exact.
        let got = store
            .read(GROUP, GROUP)
            .await
            .expect("read requested range");
        assert_eq!(
            got.as_ref(),
            &plaintext[GROUP as usize..2 * GROUP as usize],
            "the requested range is byte-exact"
        );
    }

    /// The reader [`FailFirstOpen`] hands out: on the first open, a fault
    /// parked from the very first byte (so the store checkpoints NOTHING and a
    /// retry re-covers the whole range); on every later open, the real
    /// [`ScriptedSource`] reader.
    enum MaybeFaultReader {
        Fault(Option<anyhow::Error>),
        Real(<ScriptedSource as BlobSource>::Reader),
    }

    impl iroh_io::AsyncStreamReader for MaybeFaultReader {
        async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
            match self {
                Self::Fault(_) => Ok(Bytes::new()),
                Self::Real(r) => r.read_bytes(len).await,
            }
        }

        async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
            match self {
                Self::Fault(_) => Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "scripted immediate fault",
                )),
                Self::Real(r) => r.read().await,
            }
        }
    }

    impl crate::sink::StashedFault for MaybeFaultReader {
        fn take_fault(&mut self) -> Option<anyhow::Error> {
            match self {
                Self::Fault(f) => f.take(),
                Self::Real(r) => r.take_fault(),
            }
        }
    }

    /// A [`BlobSource`] wrapper that fails its FIRST open with a scripted
    /// upstream `SpendingCapExhausted` voucher rejection — the shape a genuine
    /// mid-fetch exhaustion refusal takes — and delegates every later open to
    /// the inner [`ScriptedSource`]. Regression coverage for the pacer bug where
    /// `BudgetPacer` proactively returned `Wait` after every top-up, forcing the
    /// driver to sleep the full settle budget before even retrying the open.
    struct FailFirstOpen {
        inner: ScriptedSource,
        opens: std::sync::atomic::AtomicUsize,
    }

    impl BlobSource for FailFirstOpen {
        type Reader = MaybeFaultReader;

        fn open(
            &self,
            hash: [u8; 32],
            range: AlignedRange,
        ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
            let n = self.opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                if n == 0 {
                    // A real header (so the driver prices `next_voucher_cost`),
                    // but the reader immediately parks a genuine-exhaustion
                    // fault instead of yielding any wire bytes.
                    let header = UpstreamPullHeader {
                        total_bytes: self.inner.total_bytes(),
                        rate_per_mb: 1,
                        interval_bytes: 1024 * 1024,
                        ttfb_ms: 0.0,
                    };
                    let fault = UpstreamVoucherRejected {
                        reason: VoucherRejectReason::SpendingCapExhausted,
                        bundle: None,
                    };
                    return Ok((
                        header,
                        MaybeFaultReader::Fault(Some(anyhow::Error::new(fault))),
                    ));
                }
                let (header, reader) = self.inner.open(hash, range).await?;
                Ok((header, MaybeFaultReader::Real(reader)))
            })
        }

        fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
            Box::pin(async move {
                match reader {
                    MaybeFaultReader::Fault(_) => Ok(VoucherProgress::default()),
                    MaybeFaultReader::Real(r) => self.inner.finish(r).await,
                }
            })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_top_up_is_followed_by_an_immediate_reopen_not_a_settle_wait() {
        // The buyer starts under-deposited, so the first open's genuine
        // `SpendingCapExhausted` refusal is corroborated by the buyer's OWN
        // ledger (0 remaining < any nonzero voucher cost) and the pacer tops
        // up. Before the fix, `BudgetPacer::decide` proactively returned `Wait`
        // right after that top-up, and the driver slept the WHOLE settle
        // budget (`max_settle_waits * settle_backoff`) before even retrying the
        // open. The clock is paused, so only a real `tokio::time::sleep`
        // advances it: the settle-wait costs a full `settle_backoff` of virtual
        // time, while the CPU cost of the fetch itself costs none. That makes
        // the elapsed-time assertion below exact instead of machine-dependent.
        let total = 2 * GROUP;
        let (root, plaintext, _outboard) = synth_blob(total as usize);
        let store = fresh_store(root, total);

        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let inner = ScriptedSource::new(plaintext.clone())
            .expect("source")
            .paying(Arc::clone(&ledger));
        let root = inner.root();
        let source = FailFirstOpen {
            inner,
            opens: std::sync::atomic::AtomicUsize::new(0),
        };

        let pacer = BudgetPacer::new();
        let funder = FakeFunder::new(3, DepositOutcome::Added(U256::from(u128::MAX)));

        let mut ctx = healthy_ctx();
        ctx.deposit = U256::ZERO; // under-deposited: the first refusal is genuine
        let ctx = Arc::new(Mutex::new(ctx));

        let drive_config = DriveConfig {
            working_deposit: U256::from(10_000u64),
            seller_reserve: U256::ZERO,
            max_settle_waits: 2,
            settle_backoff: std::time::Duration::from_secs(2),
        };

        let started = tokio::time::Instant::now();
        drive(
            &store,
            &source,
            &pacer,
            &funder,
            &ctx,
            &ledger,
            root,
            0,
            0,
            &drive_config,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("drive completes after one reactive top-up");
        let elapsed = started.elapsed();

        assert_eq!(
            source.opens.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "one faulting open + one immediate re-open"
        );
        assert_eq!(
            source.inner.opened_ranges().len(),
            1,
            "only the SECOND (successful) open reaches the inner scripted source"
        );
        assert_eq!(
            funder.calls().len(),
            1,
            "exactly one reactive top-up funded the genuine exhaustion"
        );
        assert!(
            elapsed < drive_config.settle_backoff,
            "the re-open must follow the top-up immediately, not after a settle-wait \
             sleep (settle_backoff was {:?} per step): elapsed {elapsed:?}",
            drive_config.settle_backoff
        );

        assert!(store.is_complete().await.expect("is_complete"));
        let got = store.read(0, 0).await.expect("read whole blob");
        assert_eq!(got.as_ref(), plaintext.as_slice());
    }

    /// A [`BlobSource`] that fails its first open with a genuine exhaustion (like
    /// [`FailFirstOpen`]), serves the next open, refuses the THIRD open as a stale
    /// resume ([`ResumeOffsetPastEnd`]), and serves every open after that. It
    /// models an upstream that admits one stream on headroom it computed before its
    /// watcher saw the top-up, then refuses the next.
    struct StaleRefusalAfterOneLeg {
        inner: FailFirstOpen,
    }

    impl BlobSource for StaleRefusalAfterOneLeg {
        type Reader = MaybeFaultReader;

        fn open(
            &self,
            hash: [u8; 32],
            range: AlignedRange,
        ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
            let n = self.inner.opens.load(std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                if n == 2 {
                    self.inner
                        .opens
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    return Err(anyhow::Error::new(crate::ResumeOffsetPastEnd {
                        total_bytes: self.inner.inner.total_bytes(),
                        byte_offset: range.fetch_start(),
                    }));
                }
                self.inner.open(hash, range).await
            })
        }

        fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
            self.inner.finish(reader)
        }
    }

    /// The settle allowance a top-up arms outlives the first landed leg: a stale
    /// refusal on a LATER re-open still settle-waits and retries instead of ending
    /// the fetch. One group per leg (a [`WindowPacer`] with a one-group window and
    /// the downstream always paid up) forces several opens after the top-up.
    #[tokio::test(start_paused = true)]
    async fn a_stale_refusal_after_a_landed_post_top_up_leg_still_settle_waits() {
        let total = 3 * GROUP;
        let (root, plaintext, _outboard) = synth_blob(total as usize);
        let store = fresh_store(root, total);

        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let inner = ScriptedSource::new(plaintext.clone())
            .expect("source")
            .paying(Arc::clone(&ledger));
        let root = inner.root();
        let source = StaleRefusalAfterOneLeg {
            inner: FailFirstOpen {
                inner,
                opens: std::sync::atomic::AtomicUsize::new(0),
            },
        };

        let pacer = crate::pacer::WindowPacer::new(GROUP);
        let funder = FakeFunder::new(3, DepositOutcome::Added(U256::from(u128::MAX)));
        let mut ctx = healthy_ctx();
        ctx.deposit = U256::ZERO; // under-deposited: the first refusal is genuine
        let ctx = Arc::new(Mutex::new(ctx));
        let drive_config = DriveConfig {
            working_deposit: U256::from(10_000u64),
            seller_reserve: U256::ZERO,
            max_settle_waits: 2,
            settle_backoff: std::time::Duration::from_secs(2),
        };
        let downstream = || super::DownstreamFrontier {
            served_paid: u64::MAX,
            serve_demand: 0,
        };

        drive(
            &store,
            &source,
            &pacer,
            &funder,
            &ctx,
            &ledger,
            root,
            0,
            0,
            &drive_config,
            None,
            None,
            Some(&downstream),
            None,
        )
        .await
        .expect("a stale refusal inside the settle budget must retry, not end the fetch");

        assert_eq!(funder.calls().len(), 1, "one top-up funded the fetch");
        assert!(
            source.inner.opens.load(std::sync::atomic::Ordering::SeqCst) >= 5,
            "fault + leg + stale refusal + retried legs"
        );
        assert!(store.is_complete().await.expect("is_complete"));
        let got = store.read(0, 0).await.expect("read whole blob");
        assert_eq!(got.as_ref(), plaintext.as_slice());
    }

    /// A mid-fetch `topUp` that mines but cannot be credited locally is
    /// terminal: the USDC is escrowed against a row that will not account for
    /// it, so continuing would spend against a deposit the driver cannot track.
    /// This is the disposition the proactive and manual legs are written to
    /// match, so it has to be pinned rather than assumed.
    #[tokio::test(start_paused = true)]
    async fn an_uncreditable_reactive_top_up_is_terminal() {
        for (outcome, expected) in [
            (DepositOutcome::UnknownPool, "no local record remains"),
            (DepositOutcome::PoolMismatch, "tracks a different pool"),
        ] {
            let total = 2 * GROUP;
            let (root, plaintext, _outboard) = synth_blob(total as usize);
            let store = fresh_store(root, total);

            let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
            let inner = ScriptedSource::new(plaintext.clone())
                .expect("source")
                .paying(Arc::clone(&ledger));
            let root = inner.root();
            let source = FailFirstOpen {
                inner,
                opens: std::sync::atomic::AtomicUsize::new(0),
            };

            let pacer = BudgetPacer::new();
            let funder = FakeFunder::new(3, outcome);

            let mut ctx = healthy_ctx();
            ctx.deposit = U256::ZERO; // under-deposited: the refusal is genuine
            let ctx = Arc::new(Mutex::new(ctx));

            let drive_config = DriveConfig {
                working_deposit: U256::from(10_000u64),
                seller_reserve: U256::ZERO,
                max_settle_waits: 2,
                settle_backoff: std::time::Duration::from_secs(2),
            };

            let err = drive(
                &store,
                &source,
                &pacer,
                &funder,
                &ctx,
                &ledger,
                root,
                0,
                0,
                &drive_config,
                None,
                None,
                None,
                None,
            )
            .await
            .expect_err("an untrackable escrow must not read as a completed fetch");
            let msg = format!("{err:#}");
            assert!(msg.contains(expected), "{outcome:?}: {msg}");
            assert!(
                msg.contains("escrowed"),
                "the operator must learn the money moved: {msg}"
            );
            assert!(
                !store.is_complete().await.expect("is_complete"),
                "{outcome:?}: the fetch must not be reported complete"
            );
        }
    }

    /// A [`Pacer`] stub for the `up_to_bytes` clamp test: `Draw { up_to_bytes }`
    /// on its first call, `Done` on every call after — so a driver that ignores
    /// the clamp and drains the whole gap in one open would still only see ONE
    /// open, while a driver that honors it opens exactly `up_to_bytes` and then
    /// (correctly) stops early, leaving the rest of the gap unfilled.
    struct OnceDrawThenDone {
        up_to_bytes: u64,
        drawn: std::sync::atomic::AtomicBool,
    }

    impl crate::pacer::Pacer for OnceDrawThenDone {
        fn decide(&self, _state: &PaceState) -> PaceDecision {
            if self.drawn.swap(true, std::sync::atomic::Ordering::SeqCst) {
                PaceDecision::Done
            } else {
                PaceDecision::Draw {
                    up_to_bytes: self.up_to_bytes,
                }
            }
        }
    }

    #[tokio::test]
    async fn draw_clamps_the_open_to_up_to_bytes() {
        // A single 4-group gap; the pacer authorizes only ONE group's worth. The
        // driver must open exactly GROUP bytes, not the whole 4*GROUP gap — this
        // is the up_to_bytes clamp.
        let total = 4 * GROUP;
        let (root, plaintext, _outboard) = synth_blob(total as usize);
        let store = fresh_store(root, total);

        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(plaintext.clone())
            .expect("source")
            .paying(Arc::clone(&ledger));
        let pacer = OnceDrawThenDone {
            up_to_bytes: GROUP,
            drawn: std::sync::atomic::AtomicBool::new(false),
        };
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));

        drive(
            &store,
            &source,
            &pacer,
            &funder,
            &ctx,
            &ledger,
            root,
            0,
            0,
            &config(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("drive stops cleanly once the stub pacer says Done");

        assert_eq!(
            source.opened_ranges(),
            vec![(0, GROUP)],
            "the driver must clamp the open to the pacer's up_to_bytes, not the \
             whole gap"
        );
        assert!(
            !store.is_complete().await.expect("is_complete"),
            "only one group of four was authorized, so the blob stays incomplete"
        );
    }

    /// A [`PacingWait`] stub that counts calls and resolves immediately.
    struct CountingWait {
        calls: std::sync::atomic::AtomicUsize,
    }

    impl super::PacingWait for CountingWait {
        fn wait(
            &self,
            _observed: super::DownstreamFrontier,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async {})
        }
    }

    /// A [`Pacer`] stub for the `Wait` test: `Wait` on the first call, then
    /// defers to [`BudgetPacer`] on every call after — modeling a window pacer
    /// that frees up room only after the driver's injected wait hook fires.
    struct WaitOnceThenBudget {
        waited: std::sync::atomic::AtomicBool,
    }

    impl crate::pacer::Pacer for WaitOnceThenBudget {
        fn decide(&self, state: &PaceState) -> PaceDecision {
            if self.waited.swap(true, std::sync::atomic::Ordering::SeqCst) {
                BudgetPacer::new().decide(state)
            } else {
                PaceDecision::Wait
            }
        }
    }

    #[tokio::test]
    async fn wait_decision_awaits_the_pacing_hook_once_then_completes() {
        let total = GROUP;
        let (root, plaintext, _outboard) = synth_blob(total as usize);
        let store = fresh_store(root, total);

        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(plaintext.clone())
            .expect("source")
            .paying(Arc::clone(&ledger));
        let pacer = WaitOnceThenBudget {
            waited: std::sync::atomic::AtomicBool::new(false),
        };
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));
        let wait_hook = CountingWait {
            calls: std::sync::atomic::AtomicUsize::new(0),
        };

        drive(
            &store,
            &source,
            &pacer,
            &funder,
            &ctx,
            &ledger,
            root,
            0,
            0,
            &config(),
            None,
            Some(&wait_hook),
            None,
            None,
        )
        .await
        .expect("drive completes after the one scripted Wait");

        assert_eq!(
            wait_hook.calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the driver must await the PacingWait hook exactly once for the \
             single scripted Wait decision"
        );
        assert!(store.is_complete().await.expect("is_complete"));
        let got = store.read(0, 0).await.expect("read whole blob");
        assert_eq!(got.as_ref(), plaintext.as_slice());
    }

    /// A [`PacingWait`] hook that simulates the downstream serve leg clearing one
    /// window's worth of payment each time it is awaited: it bumps a shared
    /// counter by `bump_bytes` and resolves immediately (no real sleep), so the
    /// test stays deterministic. The SAME counter backs the `served_paid` reader
    /// passed to `drive`, so this is the only thing that can unstick a
    /// `WindowPacer::Wait`.
    struct BumpDownstreamWait {
        served_paid: Arc<std::sync::atomic::AtomicU64>,
        bump_bytes: u64,
    }

    impl super::PacingWait for BumpDownstreamWait {
        fn wait(
            &self,
            _observed: super::DownstreamFrontier,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            self.served_paid
                .fetch_add(self.bump_bytes, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn window_pacer_gates_on_injected_served_paid() {
        // A 3-group gap with a WindowPacer window of exactly one group: the pull
        // can only run one group ahead of `served_paid` before it must `Wait`.
        // Nothing but the injected `served_paid` reader (bumped by the
        // `PacingWait` hook, standing in for the downstream serve leg clearing
        // payment) can let the drive make further progress — proving both the
        // seam wiring (`served_paid` reaches `WindowPacer` via `PaceState`) and
        // the `Wait` -> hook -> re-decide loop actually advances against it.
        let total = 3 * GROUP;
        let (root, plaintext, _outboard) = synth_blob(total as usize);
        let store = fresh_store(root, total);

        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(plaintext.clone())
            .expect("source")
            .paying(Arc::clone(&ledger));
        let pacer = crate::pacer::WindowPacer::new(GROUP);
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));

        let served_paid_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let wait_hook = BumpDownstreamWait {
            served_paid: Arc::clone(&served_paid_counter),
            bump_bytes: GROUP,
        };
        let served_paid_reader = {
            let counter = Arc::clone(&served_paid_counter);
            move || super::DownstreamFrontier {
                served_paid: counter.load(std::sync::atomic::Ordering::SeqCst),
                serve_demand: 0,
            }
        };

        drive(
            &store,
            &source,
            &pacer,
            &funder,
            &ctx,
            &ledger,
            root,
            0,
            0,
            &config(),
            None,
            Some(&wait_hook),
            Some(&served_paid_reader),
            None,
        )
        .await
        .expect("drive completes as served_paid advances one window at a time");

        // The window forced at least the two `Wait`s a 3-group gap under a
        // 1-group window needs (group 2 and group 3 each had to wait for the
        // previous group's payment to clear downstream).
        assert!(
            served_paid_counter.load(std::sync::atomic::Ordering::SeqCst) >= 2 * GROUP,
            "the injected served_paid reader must have been advanced by the wait \
             hook for the drive to complete"
        );

        assert!(store.is_complete().await.expect("is_complete"));
        let got = store.read(0, 0).await.expect("read whole blob");
        assert_eq!(
            got.as_ref(),
            plaintext.as_slice(),
            "byte-exact after a window-paced, served-paid-gated drive"
        );
    }

    /// A [`BlobSource`] whose upstream payment trails its delivery: each clean
    /// leg commits only half of the leg's wire, so the paid frontier the driver
    /// opens at stays behind the delivered frontier the pacer measures. Opens past
    /// `max_opens` fail, so a driver that never reaches a demand ends the test
    /// instead of spinning.
    struct HalfPaySource {
        inner: ScriptedSource,
        ledger: Arc<PoolLedger>,
        leg_wire: Mutex<u64>,
        max_opens: usize,
    }

    impl BlobSource for HalfPaySource {
        type Reader = crate::source::ScriptedReader;

        fn open(
            &self,
            hash: [u8; 32],
            range: AlignedRange,
        ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
            Box::pin(async move {
                let opens = self.inner.opened_ranges().len();
                anyhow::ensure!(
                    opens < self.max_opens,
                    "open budget of {} spent: {:?}",
                    self.max_opens,
                    self.inner.opened_ranges()
                );
                *self.leg_wire.lock().expect("leg wire lock") = range.wire_len();
                self.inner.open(hash, range).await
            })
        }

        fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
            Box::pin(async move {
                self.inner.finish(reader).await?;
                let half = *self.leg_wire.lock().expect("leg wire lock") / 2;
                self.ledger
                    .issue(half, 1, crate::EpochAction::Keep, |_next, _chain| async {
                        Ok(())
                    })
                    .await?;
                Ok(VoucherProgress::from_cumulative(
                    self.ledger.committed(),
                    U256::ZERO,
                ))
            })
        }
    }

    /// A [`PacingWait`] hook that counts calls and then never resolves, so a
    /// drive whose window stays closed parks on it.
    struct ParkingWait {
        calls: std::sync::atomic::AtomicUsize,
    }

    impl super::PacingWait for ParkingWait {
        fn wait(
            &self,
            _observed: super::DownstreamFrontier,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(std::future::pending())
        }
    }

    /// An in-band serve demand is fetched even when upstream payment lags
    /// delivery by a whole pull-window floor. The pacer measures the demand from
    /// the delivered frontier, but the driver opens each draw at the paid
    /// frontier, so the first demand draw stops short of the demand. Each
    /// further draw pays for part of its leg and moves the paid frontier, so a
    /// later draw covers the demand. The downstream client pays nothing
    /// throughout.
    #[tokio::test(start_paused = true)]
    async fn in_band_demand_converges_when_upstream_payment_lags_delivery() {
        use crate::pacer::{PULL_WINDOW_FLOOR, RampPacer};

        const FLOOR: u64 = PULL_WINDOW_FLOOR;
        let total = 3 * FLOOR;
        let (_root, plaintext, _outboard) = synth_blob(total as usize);

        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let inner = ScriptedSource::new(plaintext).expect("source");
        let root = inner.root();
        let store = fresh_store(root, total);
        let source = HalfPaySource {
            inner,
            ledger: Arc::clone(&ledger),
            leg_wire: Mutex::new(0),
            max_opens: 8,
        };

        // A closed window of two floors: `divisor: 0` opens the full
        // `credit_max`, and the client never pays, so the window never moves.
        // The first leg fills it, and half payment leaves the paid frontier
        // about one floor behind delivery.
        let window = 2 * FLOOR;
        let pacer = RampPacer {
            divisor: 0,
            floor: FLOOR,
            credit_max: window,
        };
        // The serve leg is parked on the first byte past the closed window: one
        // byte ahead of the pull once the window fills, and so in band.
        let demand = window + 1;
        let downstream = || super::DownstreamFrontier {
            served_paid: 0,
            serve_demand: demand,
        };
        let wait_hook = ParkingWait {
            calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));

        let outcome = tokio::time::timeout(
            std::time::Duration::from_mins(1),
            drive(
                &store,
                &source,
                &pacer,
                &funder,
                &ctx,
                &ledger,
                root,
                0,
                0,
                &config(),
                None,
                Some(&wait_hook),
                Some(&downstream),
                None,
            ),
        )
        .await;
        assert!(
            outcome.is_err(),
            "the drive must park on the closed window, not end: {outcome:?}"
        );

        let opened = source.inner.opened_ranges();
        assert_eq!(
            opened.first(),
            Some(&(0, window)),
            "the first leg fills the window: {opened:?}"
        );
        let (first_demand_start, first_demand_len) = opened[1];
        assert!(
            first_demand_start + first_demand_len < demand,
            "the first demand draw opens at the lagging paid frontier and stops \
             short of the demand, so this test exercises the lag: {opened:?}"
        );
        let covering = opened
            .iter()
            .position(|&(start, len)| start < demand && start + len >= demand)
            .expect("a later draw covers the demand");
        assert!(
            covering >= 2 && covering + 1 == opened.len(),
            "the demand converges over more than one draw and the pull stops once \
             it is covered: {opened:?}"
        );
        assert_eq!(
            wait_hook.calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the pull waits only after the demand is fetched"
        );
        assert!(
            store
                .missing_ranges(0, demand)
                .await
                .expect("missing")
                .is_empty(),
            "the demanded byte is in the store"
        );
        assert!(funder.calls().is_empty(), "no top-up was needed");
    }

    /// An [`IngestStore`] wrapper that counts `flush_present_record` calls and
    /// delegates every real operation to an inner [`ClientRangedStore`]. It lets
    /// a test observe the interval flush firing during a still-running fetch.
    struct FlushCountingStore {
        inner: ClientRangedStore,
        flushes: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl RangedStore for FlushCountingStore {
        fn total_bytes(&self) -> u64 {
            self.inner.total_bytes()
        }
        fn present_ranges(&self) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
            self.inner.present_ranges()
        }
        fn missing_ranges(
            &self,
            byte_offset: u64,
            byte_len: u64,
        ) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
            self.inner.missing_ranges(byte_offset, byte_len)
        }
        fn admit(
            &self,
            range: AlignedRange,
            bao_bytes: Bytes,
        ) -> decdn_bao_range::RangedFuture<'_, ()> {
            self.inner.admit(range, bao_bytes)
        }
        fn read(
            &self,
            byte_offset: u64,
            byte_len: u64,
        ) -> decdn_bao_range::RangedFuture<'_, Bytes> {
            self.inner.read(byte_offset, byte_len)
        }
        fn is_complete(&self) -> decdn_bao_range::RangedFuture<'_, bool> {
            self.inner.is_complete()
        }
        fn finalize(&self) -> decdn_bao_range::RangedFuture<'_, ()> {
            self.inner.finalize()
        }
    }

    impl crate::source::IngestStore for FlushCountingStore {
        fn ingest_stream<'a, R>(
            &'a self,
            range: &'a AlignedRange,
            reader: R,
            on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<R>> + 'a>>
        where
            R: crate::source::BaoRangeReader + 'a,
        {
            Box::pin(self.inner.ingest_stream(range, reader, on_progress))
        }

        fn flush_present_record(&self) -> std::io::Result<()> {
            self.flushes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.flush_present_record()
        }
    }

    /// The interval flush persists resume progress MID-fetch, not only at
    /// completion (spec §5.5): `drive_with_interval_flush` is raced against a
    /// work future that stays pending for several short intervals, and the
    /// store's `flush_present_record` must fire ONCE PER ELAPSED INTERVAL before
    /// the work resolves — so a crash between the last flush and completion loses
    /// at most one interval, never the whole in-flight fetch. The flushed record
    /// equals the durable checkpointed prefix reopened from disk.
    ///
    /// `start_paused` is what makes the flush count an arithmetic fact rather
    /// than a scheduling one. On the real clock the work future's sleep and the
    /// flush interval both race the runner: a starved runtime that is descheduled
    /// past the work's deadline finds it already elapsed, and the `biased` select
    /// in `drive_with_interval_flush` prefers completion — so the loop can exit
    /// having flushed once, failing an assertion about periodicity for a reason
    /// that has nothing to do with periodicity. Under the virtual clock time
    /// advances only to a registered deadline — here the interval's ticks and the
    /// work's own sleep, and nothing else — so the tick count is fixed by the two
    /// durations below.
    #[tokio::test(start_paused = true)]
    async fn interval_flush_persists_progress_before_completion() {
        let total = 8 * 1024 * 1024;
        let (root, plaintext, outboard) = synth_blob(total as usize);
        let dir = tmp_dir();
        let inner = ClientRangedStore::create(dir.path(), "blob", root, total).expect("create");
        // Durably checkpoint a real, verified 4 MiB prefix into the store — the
        // resume progress the interval flush must persist — without yet writing
        // the `.ranges` record (checkpoint no longer does, spec §5.5).
        let prefix = align_range(0, 4 * 1024 * 1024, total).expect("align prefix");
        preadmit(&inner, &plaintext, &outboard, &prefix).await;

        let flushes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let store = FlushCountingStore {
            inner,
            flushes: Arc::clone(&flushes),
        };

        // A work future that stays pending across several 20 ms intervals, so the
        // interval owner flushes repeatedly before the work resolves.
        let work = async {
            tokio::time::sleep(std::time::Duration::from_millis(130)).await;
            Ok(())
        };
        super::drive_with_interval_flush(&store, std::time::Duration::from_millis(20), work)
            .await
            .expect("interval-flush wrapper completes when the work future resolves");

        // Six ticks, exactly: the immediate first tick is consumed before the
        // loop, leaving deadlines at 20..=120 ms inside the work's 130 ms, and
        // the virtual clock advances only to those deadlines. 130 is not a
        // multiple of 20, so the `biased` select never has to adjudicate a tie
        // at the boundary. The flush is periodic, not a single completion flush;
        // dropping the pre-loop tick consumption would read as seven.
        let count = flushes.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            count, 6,
            "the interval owner must flush once per elapsed interval during a \
             still-running fetch"
        );

        // The interval flush is a REAL persist: reopening the store from disk
        // recovers exactly the checkpointed 4 MiB prefix — a crash right here
        // would resume from it, not refetch from zero.
        let reopened = ClientRangedStore::open(dir.path(), "blob", root, total).expect("reopen");
        let present = reopened.present_ranges().await.expect("present");
        assert_eq!(
            present,
            prefix.chunk_ranges().clone(),
            "the interval-flushed record must persist the checkpointed prefix"
        );
    }

    /// A source that claims `total_bytes == 0` for a NON-empty root (#1054) is a
    /// paid-but-wrong delivery: the empty store has no gap to pull and no chunk
    /// group for any decoder to anchor, so without the up-front root check the
    /// driver would finalize an empty blob under an arbitrary hash. The check is
    /// the driver's, so it holds for every consumer the driver serves — the CLI's
    /// ranged store and the node's cache admit alike.
    #[tokio::test]
    async fn drive_rejects_an_empty_claim_for_a_non_empty_root() {
        let wanted = *blake3::hash(b"not the empty blob").as_bytes();
        let store = fresh_store(wanted, 0);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(Vec::new()).expect("source");
        let pacer = BudgetPacer::new();
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));

        let err = drive(
            &store,
            &source,
            &pacer,
            &funder,
            &ctx,
            &ledger,
            wanted,
            0,
            0,
            &config(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect_err("an empty claim for a non-empty root must fail");
        assert!(
            err.downcast_ref::<crate::HashMismatch>().is_some(),
            "must surface the typed HashMismatch, got: {err:#}"
        );
        assert_eq!(
            source.opened_ranges(),
            Vec::new(),
            "nothing is pulled on the way out"
        );
    }

    /// The empty root IS the one hash a `total_bytes == 0` claim can carry: the
    /// driver finalizes it with nothing pulled and nothing paid.
    #[tokio::test]
    async fn drive_accepts_the_empty_blob_under_the_empty_root() {
        let root = *blake3::hash(&[]).as_bytes();
        let store = fresh_store(root, 0);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(Vec::new()).expect("source");
        let pacer = BudgetPacer::new();
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));

        drive(
            &store,
            &source,
            &pacer,
            &funder,
            &ctx,
            &ledger,
            root,
            0,
            0,
            &config(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("the empty blob under the empty root completes");
        assert_eq!(source.delivered_bytes(), 0, "nothing to pull");
        assert_eq!(ledger.committed().bytes, U256::ZERO, "nothing to pay");
    }
}
