//! The #1608 gap-driven, range-minimized fetch driver.
//!
//! [`drive`] satisfies a request `R = [offset, offset+len)` of one blob by
//! filling ONLY the gaps the store is missing, paying the minimum: held ranges
//! are read locally, never pulled, never re-paid. It is the integration keystone
//! of the #1621 ranged-store effort — the piece that turns the sourcing axis
//! ([`BlobSource`]), the pacing axis ([`Pacer`]), and the funding axis
//! ([`Funder`]) into a single fetch that pulls exactly the bytes a request needs.
//!
//! # How it folds the CLI's `fetch_blob_streaming` loop
//!
//! The pre-#1608 CLI loop (`crates/cli/src/commands/fetch.rs::fetch_blob_streaming`)
//! was one whole-tail pull with an advancing `byte_offset`. `drive` keeps its
//! money-relevant branches but re-frames them around gaps:
//!
//! - **Draw** (the happy path): open the gap's [`AlignedRange`](decdn_bao_range::AlignedRange), stream it through
//!   [`crate::ClientRangedStore::ingest_stream`] (which durably checkpoints as it goes),
//!   then [`BlobSource::finish`] to drain the pull and recover the acked voucher
//!   watermark. The store's checkpoints are what make a mid-gap fault re-enter
//!   with a SMALLER gap, so a resume never re-pulls a checkpointed prefix — the
//!   loop's own `set_len`/`seek` rewind is gone (the store owns durability now).
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
//!   watermark, exactly as the pre-#1608 CLI loop computed it — the tail is
//!   re-delivered (`ingest_stream` re-writes it idempotently) and re-billed.
//! - **Settle-wait**: after a top-up the driver retries the open immediately —
//!   the pacer sees the healed deposit and draws right away. If the node's chain
//!   watcher has not yet observed the new deposit, that retry is refused with the
//!   ambiguous [`crate::resume_may_be_stale`] shape; ONLY THEN does the driver
//!   sleep and retry, bounded by the settle-wait budget (this mirrors the legacy
//!   CLI loop, which only backs off on an actual stale-resume refusal).
//! - **Reseed** (wallet-less resync, #1481): an authenticated
//!   [`WatermarkBundle`](decdn_protocol::client::WatermarkBundle) that ADVANCES
//!   our committed watermark is a healable desync — the driver reseeds the ledger
//!   ([`PoolLedger::reseed`]) and retries. This is driver-owned, NOT a
//!   [`PaceDecision`], exactly as `node_origin::resume::decide` orders it.
//!
//! # What is deliberately NOT here (deferred to A5 / the CLI)
//!
//! - The **stale-foreign-partial restart-from-zero**. The CLI kept an opaque
//!   `.partial` with no outboard, so an ambiguous `NotFound` could mean "this file
//!   belongs to another blob" and it rewound to zero. A [`crate::ClientRangedStore`] is
//!   keyed to `(root, total_bytes)` and only ever holds bao-verified ranges, so
//!   there is no foreign-partial ambiguity to resolve — resume is always driven by
//!   the verified present set.
//! - **Progress reporting, deadlines, and durable watermark persistence** to a
//!   `BuyerChannelStore`. The acked watermark lives in the [`PoolLedger`] the
//!   caller owns; persisting it across process restarts, and drawing a progress
//!   bar, are CLI concerns (A5).
//!
//! # Store abstraction
//!
//! The store is generic over [`crate::source::IngestStore`] (`RangedStore` +
//! `ingest_stream`), not the concrete `&ClientRangedStore` — [`crate::ClientRangedStore`]
//! is one implementer (the CLI/client backend, writing `.partial`/`.obao4`); a
//! node backend (B2) admits to the cache and tees to its downstream client
//! through the same seam.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::U256;
use bao_tree::ChunkRanges;
use decdn_bao_range::align_range;
use decdn_incentive::DepositOutcome;

use crate::pacer::{PaceDecision, PaceState};
use crate::source::{BlobSource, Funder, IngestStore};
use crate::{
    Cumulative, MAX_RESUME_ATTEMPTS, Pacer, PoolContext, PoolLedger, ProgressCallback,
    UpstreamPullHeader, genuine_exhaustion, resumable_watermark, resume_may_be_stale,
};

/// The injected wait signal for [`PaceDecision::Wait`] (ADR 037, Phase B): the
/// node hands in an implementor that resolves once its serve leg's paid frontier
/// has advanced (so a re-decide has a chance of finding window room); the client
/// path never needs one, since `BudgetPacer` never returns `Wait`.
pub trait PacingWait: Send + Sync {
    /// Resolve once the caller judges it worth re-deciding (e.g. the served-paid
    /// frontier advanced, or a bounded poll interval elapsed).
    fn wait(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
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
/// budgets). Kept minimal — progress and deadlines stay with the caller (A5).
#[derive(Debug, Clone, Copy)]
pub struct DriveConfig {
    /// The reactive top-up target passed to the pacer as
    /// [`PaceState::working_deposit`]. `U256::ZERO` disables reactive top-up.
    pub working_deposit: U256,
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
/// The single source of truth for both [`drive`]'s settle-wait budget (via
/// [`DriveConfig::cli`]) and the CLI's legacy `fetch_blob_streaming` /
/// `bundle_pull` loop, so the two settle-wait policies cannot silently diverge.
pub const MAX_TOPUP_SETTLE_WAITS: u32 = 30;
/// Backoff between resume-open retries while waiting for the node's chain watcher
/// to observe a just-landed top-up (see [`MAX_TOPUP_SETTLE_WAITS`]).
pub const TOPUP_SETTLE_BACKOFF: Duration = Duration::from_millis(500);

/// Fetch-wide counters that persist ACROSS the request's gaps (a top-up budget is
/// per-fetch, not per-gap), plus the last upstream quote used to price the next
/// voucher.
#[derive(Debug, Clone, Copy)]
struct DriveCounters {
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
fn contiguous_byte_ranges(ranges: &ChunkRanges, total_bytes: u64) -> Vec<(u64, u64)> {
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
fn ranges_content_len(ranges: &ChunkRanges, total_bytes: u64) -> u64 {
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
    served_paid: Option<&(dyn Fn() -> u64 + Send + Sync)>,
) -> anyhow::Result<()>
where
    St: IngestStore,
    S: BlobSource,
    P: Pacer,
    F: Funder,
{
    let total_bytes = store.total_bytes();

    let missing = store.missing_ranges(offset, len).await?;
    let gaps = contiguous_byte_ranges(&missing, total_bytes);

    let mut counters = DriveCounters {
        topups_used: 0,
        resume_attempts: 0,
        next_voucher_cost: U256::ZERO,
    };

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
            pacing_wait,
            served_paid,
        )
        .await?;
    }

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
// on, exactly as the CLI's `fetch_blob_streaming` keeps them together.
#[allow(clippy::too_many_lines)]
async fn fill_gap<St, S, P, F>(
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
    pacing_wait: Option<&dyn PacingWait>,
    served_paid: Option<&(dyn Fn() -> u64 + Send + Sync)>,
) -> anyhow::Result<()>
where
    St: IngestStore,
    S: BlobSource,
    P: Pacer,
    F: Funder,
{
    // Per-open state; reset the moment an open succeeds. `awaiting_settle` is set
    // only right after a top-up, and consulted ONLY in the error-classification
    // path below: it gates the bounded settle-wait on an ACTUAL stale-resume
    // refusal from the re-open, not proactively before the retry is even
    // attempted (the pacer always retries the open immediately after a top-up).
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

        let committed = ledger.committed();
        let remaining_deposit = locked_deposit(ctx)?.saturating_sub(committed.amount);

        // The gap's PAID content frontier — the completion signal. Anchor the leg on
        // the first pass at `gap_start` with the current committed baseline (fresh /
        // cross-invocation: `paid_wire == 0`, so the frontier is `gap_start` and
        // nothing already-paid is re-pulled). `content_paid_frontier` inverts the
        // wire cost of ONE contiguous delivery from `leg_start`, so it MUST be priced
        // per-leg: `leg_anchor` re-anchors on every successful open to the previous
        // paid frontier, keeping the frontier monotonic and never summing two legs'
        // (proof-duplicating) wire encodings, which would map PAST the true paid
        // frontier and under-pay.
        let (leg_start, leg_baseline) = *leg_anchor.get_or_insert((gap_start, committed.bytes));
        let paid_wire_this_leg =
            u64::try_from(committed.bytes.saturating_sub(leg_baseline)).unwrap_or(u64::MAX);
        let paid_frontier =
            crate::sink::content_paid_frontier(leg_start, total_bytes, paid_wire_this_leg)
                .min(gap_end);
        let paid_cleared = paid_frontier.saturating_sub(gap_start);

        let state = PaceState {
            cleared_bytes: paid_cleared,
            requested_bytes: gap_len,
            remaining_deposit,
            next_voucher_cost: counters.next_voucher_cost,
            working_deposit: config.working_deposit,
            topups_used: counters.topups_used,
            max_topups: funder.max_topups(),
            exhaustion_confirmed,
            // `pulled_frontier` is this leg's own admitted/present frontier —
            // correct on BOTH the client and node pull legs, since it is always
            // THIS leg's delivery progress, never the downstream client's. No
            // seam needed.
            pulled_frontier: delivered_frontier,
            // `served_paid_frontier` is NOT this leg's own state — it is the
            // DOWNSTREAM client's paid frontier, which only the node's serve leg
            // (a separate, future task) can advance. On the client path
            // (`served_paid == None`) there is no downstream leg, so this
            // collapses to the inert local `paid_frontier`: harmless, because
            // `BudgetPacer` never reads `served_paid_frontier`. The NODE pull leg
            // MUST pass `Some(reader)` here, reading the shared downstream
            // `served_paid` frontier, so its `WindowPacer` gates the pull against
            // the downstream client's payment — not against this leg's own
            // upstream paid frontier, which would be category-wrong (it would
            // make the window track the node's own credit-window lag instead of
            // the client it is serving).
            served_paid_frontier: served_paid.map_or(paid_frontier, |f| f()),
        };

        match pacer.decide(&state) {
            PaceDecision::Done => return Ok(()),
            PaceDecision::Wait => {
                if let Some(hook) = pacing_wait {
                    hook.wait().await;
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
                anyhow::bail!(
                    "gap [{gap_start}, +{gap_len}) of blob cannot be funded: the remaining \
                     deposit cannot cover the next voucher and reactive top-up is \
                     disabled or exhausted"
                );
            }
            PaceDecision::TopUp(additional) => {
                match funder.top_up(additional).await? {
                    DepositOutcome::Added(new_deposit) => {
                        // Credit the new deposit through the shared handle so the
                        // source's next open (which clones the context) sees it.
                        ctx.lock()
                            .map_err(|_| anyhow::anyhow!("channel context lock poisoned"))?
                            .deposit = new_deposit;
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
                counters.topups_used = counters.topups_used.saturating_add(1);
                // The node's watcher may not observe this top-up before the next
                // open; wait it out rather than misread the refusal.
                awaiting_settle = true;
                settle_waits = 0;
                exhaustion_confirmed = false;
            }
            // Honors `up_to_bytes` (#1608 Phase B / driver.rs TODO, resolved):
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
                    // Fully paid — the pacer should have returned `Done`; guard
                    // against a spin.
                    return Ok(());
                }
                let resume_start = paid_frontier;
                let draw_len = gap_end.saturating_sub(resume_start).min(up_to_bytes);
                let aligned = align_range(resume_start, draw_len, total_bytes)?;

                // Whole-blob content already present, so `ingest_stream`'s
                // per-range progress can be offset into overall progress: the bar
                // reports `base + received` against `total_bytes`.
                let base_present =
                    ranges_content_len(&(store.present_ranges().await?), total_bytes);
                let reporter = move |received: u64| {
                    if let Some(cb) = on_progress {
                        cb(base_present.saturating_add(received), total_bytes);
                    }
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
                                // (durable persistence is A5's job); finishing here
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
                        let remaining = guard.deposit.saturating_sub(committed.amount);

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
                awaiting_settle = false;
                settle_waits = 0;
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

    use super::{DriveConfig, contiguous_byte_ranges, drive};
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
            provider: Address::ZERO,
            deposit: U256::from(u128::MAX),
            client_signer: Arc::new(signer),
            voucher_domain: decdn_incentive::bind_node_id_domain(1, Address::ZERO),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
        }
    }

    fn healthy_funder() -> FakeFunder {
        FakeFunder::new(3, DepositOutcome::Added(U256::from(u128::MAX)))
    }

    fn config() -> DriveConfig {
        DriveConfig {
            working_deposit: U256::from(u128::MAX),
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
    /// upstream `CapExceeded` voucher rejection — the shape a genuine
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
                    };
                    let fault = UpstreamVoucherRejected {
                        reason: VoucherRejectReason::CapExceeded,
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

    #[tokio::test]
    async fn a_top_up_is_followed_by_an_immediate_reopen_not_a_settle_wait() {
        // The buyer starts under-deposited, so the first open's genuine
        // `CapExceeded` refusal is corroborated by the buyer's OWN
        // ledger (0 remaining < any nonzero voucher cost) and the pacer tops
        // up. Before the fix, `BudgetPacer::decide` proactively returned `Wait`
        // right after that top-up, and the driver slept the WHOLE settle
        // budget (`max_settle_waits * settle_backoff`) before even retrying the
        // open. Set a settle_backoff large enough that a real wait would blow
        // past a tight elapsed-time budget, and assert it does not.
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
            max_settle_waits: 2,
            settle_backoff: std::time::Duration::from_secs(2),
        };

        let started = std::time::Instant::now();
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
            elapsed < std::time::Duration::from_millis(500),
            "the re-open must follow the top-up immediately, not after a settle-wait \
             sleep (settle_backoff was 2s per step): elapsed {elapsed:?}"
        );

        assert!(store.is_complete().await.expect("is_complete"));
        let got = store.read(0, 0).await.expect("read whole blob");
        assert_eq!(got.as_ref(), plaintext.as_slice());
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
        // is the up_to_bytes clamp the driver.rs:464 TODO used to skip.
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
        fn wait(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
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
    struct BumpServedPaidWait {
        served_paid: Arc<std::sync::atomic::AtomicU64>,
        bump_bytes: u64,
    }

    impl super::PacingWait for BumpServedPaidWait {
        fn wait(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
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
        let wait_hook = BumpServedPaidWait {
            served_paid: Arc::clone(&served_paid_counter),
            bump_bytes: GROUP,
        };
        let served_paid_reader = {
            let counter = Arc::clone(&served_paid_counter);
            move || counter.load(std::sync::atomic::Ordering::SeqCst)
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
}
