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
//!   [`ClientRangedStore::ingest_stream`] (which durably checkpoints as it goes),
//!   then [`BlobSource::finish`] to drain the pull and recover the acked voucher
//!   watermark. The store's checkpoints are what make a mid-gap fault re-enter
//!   with a SMALLER gap, so a resume never re-pulls a checkpointed prefix — the
//!   loop's own `set_len`/`seek` rewind is gone (the store owns durability now).
//! - **Reactive top-up**: a genuine mid-fetch exhaustion (confirmed against our
//!   OWN ledger via [`genuine_exhaustion`]) is funded through [`Funder::top_up`],
//!   then the gap is retried at its checkpointed frontier — the store already
//!   dropped the credited-but-unpaid tail, so there is no `content_paid_frontier`
//!   arithmetic to redo here (that lived in the CLI because it wrote the raw file
//!   itself; the store's `missing_ranges` supersedes it).
//! - **Settle-wait**: after a top-up the driver retries the open immediately —
//!   the pacer sees the healed deposit and draws right away. If the node's chain
//!   watcher has not yet observed the new deposit, that retry is refused with the
//!   ambiguous [`crate::resume_may_be_stale`] shape; ONLY THEN does the driver
//!   sleep and retry, bounded by the settle-wait budget (this mirrors the legacy
//!   CLI loop, which only backs off on an actual stale-resume refusal).
//! - **Reseed** (wallet-less resync, #1481): an authenticated
//!   [`WatermarkBundle`](decdn_protocol::client::WatermarkBundle) that ADVANCES
//!   our committed watermark is a healable desync — the driver reseeds the ledger
//!   ([`ChannelLedger::reseed`]) and retries. This is driver-owned, NOT a
//!   [`PaceDecision`], exactly as `node_origin::resume::decide` orders it.
//!
//! # What is deliberately NOT here (deferred to A5 / the CLI)
//!
//! - The **stale-foreign-partial restart-from-zero**. The CLI kept an opaque
//!   `.partial` with no outboard, so an ambiguous `NotFound` could mean "this file
//!   belongs to another blob" and it rewound to zero. A [`ClientRangedStore`] is
//!   keyed to `(root, total_bytes)` and only ever holds bao-verified ranges, so
//!   there is no foreign-partial ambiguity to resolve — resume is always driven by
//!   the verified present set.
//! - **Progress reporting, deadlines, and durable watermark persistence** to a
//!   `BuyerChannelStore`. The acked watermark lives in the [`ChannelLedger`] the
//!   caller owns; persisting it across process restarts, and drawing a progress
//!   bar, are CLI concerns (A5).
//!
//! # Store abstraction
//!
//! The store is the concrete `&ClientRangedStore` for now — it is the only
//! backend that exposes `missing_ranges` / `ingest_stream` / `finalize`. Phase B
//! will abstract the store-plus-sink behind a trait once the node's tee sink
//! exists; its shape is not known yet, so this does NOT pre-invent a `Sink` trait
//! (YAGNI).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::U256;
use bao_tree::ChunkRanges;
use decdn_bao_range::{RangedStore, align_range};
use decdn_incentive::DepositOutcome;

use crate::pacer::{PaceDecision, PaceState};
use crate::source::{BlobSource, Funder};
use crate::{
    ChannelContext, ChannelLedger, ClientRangedStore, Cumulative, MAX_RESUME_ATTEMPTS, Pacer,
    ProgressCallback, UpstreamPullHeader, genuine_exhaustion, resumable_watermark,
    resume_may_be_stale,
};

/// Read the channel context's current deposit through the shared handle. A tiny
/// helper so the driver never holds the lock across an `.await` — it locks,
/// copies the `U256`, and drops the guard.
fn locked_deposit(ctx: &Mutex<ChannelContext>) -> anyhow::Result<U256> {
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
/// [`ChannelLedger`]'s own `next_voucher` and the CLI's reactive branch use.
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
///    [`finalize`](ClientRangedStore::finalize) it (promoting `.partial` to its
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
pub async fn drive<S, P, F>(
    store: &ClientRangedStore,
    source: &S,
    pacer: &P,
    funder: &F,
    ctx: &Arc<Mutex<ChannelContext>>,
    ledger: &Arc<ChannelLedger>,
    hash: [u8; 32],
    offset: u64,
    len: u64,
    config: &DriveConfig,
    on_progress: Option<&ProgressCallback>,
) -> anyhow::Result<()>
where
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
async fn fill_gap<S, P, F>(
    store: &ClientRangedStore,
    source: &S,
    pacer: &P,
    funder: &F,
    ctx: &Arc<Mutex<ChannelContext>>,
    ledger: &Arc<ChannelLedger>,
    hash: [u8; 32],
    gap_start: u64,
    gap_len: u64,
    total_bytes: u64,
    config: &DriveConfig,
    counters: &mut DriveCounters,
    on_progress: Option<&ProgressCallback>,
) -> anyhow::Result<()>
where
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

    loop {
        // Recompute what is still missing IN THIS GAP each pass: a Draw's ingest
        // checkpoints advance presence, so `cleared` climbs and a mid-gap fault
        // shrinks the sub-range the next open covers.
        let still_missing = store.missing_ranges(gap_start, gap_len).await?;
        let missing_bytes = ranges_content_len(&still_missing, total_bytes);
        let cleared = gap_len.saturating_sub(missing_bytes);

        let committed = ledger.committed();
        let remaining_deposit = locked_deposit(ctx)?.saturating_sub(committed.amount);

        let state = PaceState {
            cleared_bytes: cleared,
            requested_bytes: gap_len,
            remaining_deposit,
            next_voucher_cost: counters.next_voucher_cost,
            working_deposit: config.working_deposit,
            topups_used: counters.topups_used,
            max_topups: funder.max_topups(),
            exhaustion_confirmed,
        };

        match pacer.decide(&state) {
            PaceDecision::Done => return Ok(()),
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
                    DepositOutcome::UnknownChannel => {
                        anyhow::bail!(
                            "mid-fetch top-up of {additional} landed on-chain but no local \
                             record remains to credit it: the deposit is escrowed and \
                             untracked. Reconcile against the chain before retrying"
                        );
                    }
                    DepositOutcome::ChannelMismatch => {
                        anyhow::bail!(
                            "mid-fetch top-up of {additional} landed on-chain but the local \
                             record now tracks a different channel: the deposit is escrowed \
                             against the topped-up channel. Reconcile against the chain \
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
            // TODO(#1608 Phase B): honor `up_to_bytes` by capping the open to it
            // once a sub-gap pacer (the node's `WindowPacer`, ADR 037) ships.
            // Today `BudgetPacer` returns the full gap remainder as `up_to_bytes`,
            // so drawing the whole still-missing sub-range below is equivalent and
            // this is a no-op — but a window pacer will return a tighter bound
            // that MUST clamp `sub_len`/`aligned` before the open, or the window
            // is not actually enforced.
            PaceDecision::Draw { .. } => {
                // Draw the first contiguous still-missing sub-range of this gap —
                // the whole gap on first entry, a shrunk tail after a mid-gap
                // fault checkpointed a prefix.
                let sub = contiguous_byte_ranges(&still_missing, total_bytes);
                let Some(&(sub_start, sub_len)) = sub.first() else {
                    // Nothing missing after all (a concurrent write, or an
                    // overshoot) — the gap is satisfied.
                    return Ok(());
                };
                let aligned = align_range(sub_start, sub_len, total_bytes)?;

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
    use crate::pacer::BudgetPacer;
    use crate::source::{BlobSource, FakeFunder, ScriptedSource, SourceFuture};
    use crate::{
        ChannelContext, ChannelLedger, ClientRangedStore, Cumulative, UpstreamPullHeader,
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
    fn healthy_ctx() -> ChannelContext {
        let signer = PrivateKeySigner::random();
        ChannelContext {
            channel_id: B256::ZERO,
            token: Address::ZERO,
            deposit: U256::from(u128::MAX),
            client_signer: Arc::new(signer),
            voucher_domain: decdn_incentive::bind_node_id_domain(1, Address::ZERO),
            prior_nonce: U256::ZERO,
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

        let source = ScriptedSource::new(plaintext.clone()).expect("source");
        assert_eq!(source.root(), root);
        let pacer = BudgetPacer::new();
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));
        let ledger = Arc::new(ChannelLedger::new(Cumulative::default()));

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
        let ledger = Arc::new(ChannelLedger::new(Cumulative::default()));

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
        let source = ScriptedSource::new(plaintext.clone())
            .expect("source")
            .with_fault_after(5 * 1024 * 1024, || {
                anyhow::anyhow!("scripted mid-gap stall")
            });
        let root = source.root();
        let store = fresh_store(root, total);

        let pacer = BudgetPacer::new();
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));
        let ledger = Arc::new(ChannelLedger::new(Cumulative::default()));

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

        let source = ScriptedSource::new(plaintext.clone()).expect("source");
        let pacer = BudgetPacer::new();
        let funder = healthy_funder();
        let ctx = Arc::new(Mutex::new(healthy_ctx()));
        let ledger = Arc::new(ChannelLedger::new(Cumulative::default()));

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
    /// upstream `InsufficientDeposit` voucher rejection — the shape a genuine
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
                        reason: VoucherRejectReason::InsufficientDeposit,
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
        // `InsufficientDeposit` refusal is corroborated by the buyer's OWN
        // ledger (0 remaining < any nonzero voucher cost) and the pacer tops
        // up. Before the fix, `BudgetPacer::decide` proactively returned `Wait`
        // right after that top-up, and the driver slept the WHOLE settle
        // budget (`max_settle_waits * settle_backoff`) before even retrying the
        // open. Set a settle_backoff large enough that a real wait would blow
        // past a tight elapsed-time budget, and assert it does not.
        let total = 2 * GROUP;
        let (root, plaintext, _outboard) = synth_blob(total as usize);
        let store = fresh_store(root, total);

        let inner = ScriptedSource::new(plaintext.clone()).expect("source");
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
        let ledger = Arc::new(ChannelLedger::new(Cumulative::default()));

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
}
