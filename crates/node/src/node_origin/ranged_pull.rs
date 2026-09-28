//! Ranged-drive assembly across partial holders (#1506, ADR 039).
//!
//! A whole-blob cache-miss pull does not target one provider that holds the
//! whole blob. A blob can live spread across partial holders — one node holds
//! discovery block 0, another block 1 — so this walks the still-missing gap,
//! plans it into contiguous [`CoveredRun`]s by each surviving candidate's
//! range-keyed coverage ([`plan_covered_runs`], node concentrate + sticky), and
//! drives the runs in offset order. One run is one `(signer, provider)` payment
//! lane; runs are SEQUENTIAL, so two lanes never pay concurrently.
//!
//! The loop half here is deliberately pure — a [`RunSink`] abstracts the store
//! query and the per-run drive, so the plan / sequence / repair decisions are
//! testable without a live channel or a 64 MiB transfer. The buyer wiring that
//! opens a lane, pays vouchers, scores the provider, and persists the watermark
//! lives in the real [`super::pull_leg`] sink.
//!
//! # Repair — the reassign-only tail
//!
//! When a run's drive faults on a NON-terminal error, the sink returns
//! [`RunOutcome::Reassign`]; the loop drops that source and re-plans the WHOLE
//! still-missing range against the surviving candidates. The store keeps the
//! verified bytes an aborted run already admitted, so the next
//! [`RunSink::missing`] excludes them — the replacement lane resumes at the gap
//! and never re-fetches (or re-pays for) a byte the faulted lane already
//! delivered.
//!
//! # Backpressure — wait, don't drop
//!
//! A source that refuses a run for backpressure ([`RunOutcome::Backpressure`])
//! has not shown that it cannot serve. The common cause is the source's
//! per-signer live cap: this node pays every upstream leg from one buyer signer,
//! so its OTHER pulls on the same source (other serve-misses, each with its own
//! assembly) can fill that cap, and the refusal clears as those pulls pay. When
//! another survivor covers the still-missing gap, the loop reassigns as for any
//! other fault. When no survivor does, dropping the source would end the assembly
//! and fail every serve stream attached to it, so the loop keeps the source, waits
//! ([`RunSink::backoff`]), and drives it again. A source that still refuses after
//! [`MAX_BACKPRESSURE_RETRIES`] waits without gap progress ends the assembly
//! [`AssembleOutcome::Backpressured`], after [`RunSink::backpressure_exhausted`].

use bao_tree::ChunkRanges;
use decdn_cache::FillError;
use decdn_client::{CoveredRun, SourceCoverage, plan_covered_runs};
use decdn_protocol::Coverage;

use crate::selection::MAX_PROVIDER_ATTEMPTS;

/// Upper bound on source reassignments across one assembly (#1506).
///
/// The reassign tail drops a faulted source and re-plans the remainder onto a
/// survivor; each such round costs the replacement source a resolve, an economic
/// gate, a pool open, a dial, and a drain, all in front of a client whose stream
/// is already open. Without a cap the loop would walk EVERY survivor — up to the
/// ten a probe-cache hit carries — churning that many opens on a pathological set.
/// Bounded to [`MAX_PROVIDER_ATTEMPTS`], the same budget the single-source
/// failover loop and [`super::pull_leg`]'s header handshake spend.
const MAX_REASSIGN_ATTEMPTS: usize = MAX_PROVIDER_ATTEMPTS;

/// Upper bound on consecutive backpressure waits without gap progress (#2178).
///
/// A source that no survivor can replace is kept through a backpressure refusal
/// and driven again after [`RunSink::backoff`]. The count is kept across the
/// assembly and resets whenever a round shrinks the gap, so a long pull survives
/// separate cap events. A source that refuses again after this many waits ends
/// the assembly [`AssembleOutcome::Backpressured`]: the refusal has outlasted
/// the time this node's other pulls need to pay and release the cap, so it is
/// probably not one that clears by waiting.
pub(crate) const MAX_BACKPRESSURE_RETRIES: u32 = 6;

/// One run's terminal disposition, as the driving sink saw it.
pub(crate) enum RunOutcome {
    /// The run's `[offset, offset+len)` is fully present in the store now.
    Filled,
    /// A source fault (`classify` neither `Fatal` nor `Unaffordable`): drop this
    /// run's source and re-plan its still-missing remainder against the
    /// surviving candidates. The reassign-only tail.
    Reassign,
    /// The source refused to open this run for a reason that usually clears with
    /// time — most often a per-signer live cap, or a load shed (#2178). It says
    /// nothing about whether the source can serve the range. The loop reassigns
    /// when another survivor covers the gap, and otherwise keeps the source and
    /// drives it again after a [`RunSink::backoff`].
    Backpressure,
    /// The whole assembly is over — a `Fatal` or `Unaffordable` fault
    /// (`classify`) that another lane cannot fix (a voucher rejection, an origin
    /// blacklist, an over-cap blob, the shared pool running dry). Propagate it.
    Terminal(FillError),
    /// Cooperative cancellation: the serve leg finished first (client
    /// disconnect / shutdown), so the whole pull stops.
    Cancelled,
}

impl RunOutcome {
    /// The `outcome` value the run's `upstream_stream` span records.
    pub(crate) const fn as_str(&self) -> &'static str {
        match self {
            Self::Filled => "filled",
            Self::Reassign => "reassigned",
            Self::Backpressure => "backpressure",
            Self::Terminal(_) => "terminal",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Why an assembly ended [`AssembleOutcome::Unavailable`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnavailableCause {
    /// Every candidate that covered a still-missing range faulted and was
    /// dropped, and none is left to try.
    NoSurvivors,
    /// A round dropped no source and waited on none, yet did not shrink the
    /// gap, so the next round would re-plan the same runs.
    NoProgress,
    /// No surviving candidate covers part of the still-missing range.
    Uncovered,
    /// [`MAX_REASSIGN_ATTEMPTS`] sources were dropped and re-planned. Other
    /// candidates, an origin among them, may still survive.
    ReassignBudget,
}

impl UnavailableCause {
    /// The cause as a short stable label for logs and fill errors.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NoSurvivors => "every covering candidate faulted",
            Self::NoProgress => "a round made no progress",
            Self::Uncovered => "no surviving candidate covers a still-missing range",
            Self::ReassignBudget => "the reassign budget ran out",
        }
    }
}

/// The outcome of assembling a byte range across partial holders.
pub(crate) enum AssembleOutcome {
    /// Every gap byte was pulled and admitted.
    Complete,
    /// The assembly cannot finish the gap, for the reason the
    /// [`UnavailableCause`] names.
    ///
    /// The serve leg refuses before `ok: true` when the candidates cannot cover
    /// the missing range at all, so an `Uncovered` end needs the store to have
    /// lost bytes or the candidate set to have shrunk after that check. Origins
    /// advertise all-ones coverage, and the probe round admits the namespace's
    /// origins only when the discovered holders do not span the blob. So a
    /// holder that faults after a spanning probe round, or a namespace with no
    /// reachable origin, can still end here.
    ///
    /// This runs on the serve-miss pull thread, which the serve leg spawns only
    /// AFTER it has already signed and sent `ok: true` (the response commits to
    /// `total_bytes` before any byte is pulled). So this surfaces to the client
    /// as a TRUNCATED stream — the leg fills nothing more and the serve encoder
    /// ends short — not as a signed `NotFound`.
    Unavailable(UnavailableCause),
    /// The only source for a still-missing range kept refusing for backpressure
    /// past [`MAX_BACKPRESSURE_RETRIES`] waits. Surfaces to the client the same
    /// way as [`Self::Unavailable`] — a truncated stream — but names a different
    /// cause for the operator: the holder was reachable and refused, not absent.
    Backpressured,
    /// A run faulted terminally; propagate the fault to the serve leg.
    Terminal(FillError),
    /// The pull was cancelled mid-assembly.
    Cancelled,
}

/// The store + per-run driver the assembly loop plans over. The real
/// implementation opens a buyer lane and drives [`decdn_client::drive`];
/// tests inject a fake.
///
/// The futures are deliberately NOT `Send`: the real `drive` is non-`Send`
/// (`IngestStore` fill), so the whole assembly runs on the pull leg's dedicated
/// current-thread runtime, exactly as the single-source drive did.
#[allow(
    async_fn_in_trait,
    reason = "crate-private; the drive future is non-Send by construction"
)]
pub(crate) trait RunSink {
    /// The still-missing chunk ranges of `[offset, offset+len)` — the gap to
    /// plan. Re-queried each planning round so a repaired remainder excludes the
    /// bytes earlier runs already admitted.
    async fn missing(&self, offset: u64, len: u64) -> ChunkRanges;

    /// Open a lane to `run.source_ix`'s candidate and drive `[run.offset,
    /// run.len)` of the blob into the store, reusing the shared demand-window
    /// axes so the pull never runs ahead of the downstream paid frontier.
    async fn drive_run(&self, run: CoveredRun) -> RunOutcome;

    /// Wait before the `attempt`-th (1-based) re-drive of a source that refused
    /// with [`RunOutcome::Backpressure`]. Returns `false` when the pull was
    /// cancelled during the wait.
    async fn backoff(&self, attempt: u32) -> bool;

    /// The source at `source_ix` refused again after `waits` backpressure waits
    /// without gap progress, and the assembly ends
    /// [`AssembleOutcome::Backpressured`]. The sink records it for the operator.
    fn backpressure_exhausted(&self, source_ix: usize, waits: u32);
}

/// Assemble `[offset, offset+len)` of a `total_bytes` blob across the ranked
/// `coverage` candidates (index = candidate index, ordered best-first).
///
/// Loops: derive the gap, plan it into runs by coverage, drive each run in
/// offset order, and on a non-terminal run fault drop that source and re-plan
/// the remainder. A backpressure refusal from a source no survivor can replace
/// waits and re-drives that source instead (see the module's `# Backpressure`).
/// Terminates on a fully-present gap ([`AssembleOutcome::Complete`]),
/// an uncovered range ([`AssembleOutcome::Unavailable`]), a sole source that
/// outlasts the wait budget ([`AssembleOutcome::Backpressured`]), a terminal
/// fault, or cancellation.
///
/// Two guards keep the loop finite even under a driver that violates the
/// reassign-only completion contract:
///
/// - **No-progress guard.** `surviving` shrinks only on a reassign, so a round
///   that reports every run [`RunOutcome::Filled`] yet does NOT shrink the gap
///   would re-plan an identical round forever. When a round neither drops a
///   source nor waits out a backpressure refusal, and the gap does not shrink,
///   the assembly ends [`AssembleOutcome::Unavailable`] rather than spin. The
///   waits themselves are bounded by [`MAX_BACKPRESSURE_RETRIES`].
/// - **Reassign budget.** At most [`MAX_REASSIGN_ATTEMPTS`] sources are dropped
///   and re-planned before the assembly ends `Unavailable`, so a pathological set
///   cannot churn one lane open per survivor.
pub(crate) async fn assemble<S: RunSink>(
    sink: &S,
    coverage: &[Coverage],
    offset: u64,
    len: u64,
    total_bytes: u64,
) -> AssembleOutcome {
    // Surviving candidate indices, best-first. A source that faults
    // non-terminally is dropped from here and never re-planned.
    let mut surviving: Vec<usize> = (0..coverage.len()).collect();
    // The prior round's gap measure and whether it dropped a source or waited
    // out a backpressure refusal — the two inputs the no-progress guard reads.
    let mut prev_gap_chunks: Option<u64> = None;
    let mut repaired_last_round = false;
    // Sources dropped so far, bounded by `MAX_REASSIGN_ATTEMPTS`.
    let mut reassigns = 0usize;
    // Backpressure waits since the gap last shrank, bounded by
    // `MAX_BACKPRESSURE_RETRIES`.
    let mut backoffs = 0u32;
    loop {
        let gap = sink.missing(offset, len).await;
        if gap.is_empty() {
            return AssembleOutcome::Complete;
        }
        if surviving.is_empty() {
            // Every candidate that covered a still-missing range faulted; nothing
            // left to try. Wire-identical to the no-provider miss.
            return AssembleOutcome::Unavailable(UnavailableCause::NoSurvivors);
        }
        let gap_chunks = chunk_count(&gap);
        if let Some(prev) = prev_gap_chunks {
            // Progress restores the whole backpressure budget: the budget bounds
            // a source that refuses every retry, not a long pull that meets
            // several separate cap events.
            if gap_chunks < prev {
                backoffs = 0;
            }
            // No-progress guard: a round that dropped no source, waited on none,
            // and did not shrink the gap will re-plan identically next round. End
            // rather than spin.
            if !repaired_last_round && gap_chunks >= prev {
                return AssembleOutcome::Unavailable(UnavailableCause::NoProgress);
            }
        }
        prev_gap_chunks = Some(gap_chunks);
        // `surviving` is already in ranked order and IS the rank the concentrate
        // planner ties-breaks on.
        let (runs, uncovered) = plan_over(&gap, total_bytes, coverage, &surviving);
        if !uncovered.is_empty() {
            return AssembleOutcome::Unavailable(UnavailableCause::Uncovered);
        }
        let mut faulted: Option<(usize, bool)> = None;
        for run in &runs {
            match sink.drive_run(*run).await {
                RunOutcome::Filled => {}
                // Stop this round at the first fault: the faulted source may also
                // own later planned runs, so re-plan the whole remainder rather
                // than press on with a plan built around a dead source.
                RunOutcome::Reassign => {
                    faulted = Some((run.source_ix, false));
                    break;
                }
                RunOutcome::Backpressure => {
                    faulted = Some((run.source_ix, true));
                    break;
                }
                RunOutcome::Terminal(err) => return AssembleOutcome::Terminal(err),
                RunOutcome::Cancelled => return AssembleOutcome::Cancelled,
            }
        }
        let Some((ix, backpressure)) = faulted else {
            // Every planned run filled its range; the runs partition the gap, so
            // the next `missing` is empty and the loop returns `Complete`.
            repaired_last_round = false;
            continue;
        };
        repaired_last_round = true;
        // A backpressure refusal keeps its source when no survivor can take over
        // the still-missing gap: dropping it would end the assembly over a cap
        // that usually clears with time. Past the wait budget the assembly ends
        // and names the refusal as its cause.
        if backpressure {
            let remaining = sink.missing(offset, len).await;
            if remaining.is_empty() {
                continue;
            }
            let others: Vec<usize> = surviving.iter().copied().filter(|&s| s != ix).collect();
            let (_, uncovered) = plan_over(&remaining, total_bytes, coverage, &others);
            if !uncovered.is_empty() {
                if backoffs >= MAX_BACKPRESSURE_RETRIES {
                    sink.backpressure_exhausted(ix, backoffs);
                    return AssembleOutcome::Backpressured;
                }
                backoffs += 1;
                if !sink.backoff(backoffs).await {
                    return AssembleOutcome::Cancelled;
                }
                continue;
            }
        }
        // Drop the faulted source and re-plan. The store keeps the verified bytes,
        // so `missing` next round excludes them: no re-fetch, no re-pay. Bounded
        // by the reassign budget so a pathological set cannot churn one lane open
        // per survivor.
        reassigns += 1;
        if reassigns >= MAX_REASSIGN_ATTEMPTS {
            return AssembleOutcome::Unavailable(UnavailableCause::ReassignBudget);
        }
        surviving.retain(|&s| s != ix);
    }
}

/// The part of `gap` that no candidate in `coverage` covers — the test
/// [`assemble`]'s first round applies before it drives any run. The serve leg
/// runs it before `ok: true`, so a gap no candidate can fill is refused rather
/// than committed and truncated.
pub(crate) fn uncovered(gap: &ChunkRanges, total_bytes: u64, coverage: &[Coverage]) -> ChunkRanges {
    let all: Vec<usize> = (0..coverage.len()).collect();
    plan_over(gap, total_bytes, coverage, &all).1
}

/// Plan `gap` into covered runs over the `ranked` candidate indices, returning
/// the runs and the part of the gap none of them covers.
fn plan_over(
    gap: &ChunkRanges,
    total_bytes: u64,
    coverage: &[Coverage],
    ranked: &[usize],
) -> (Vec<CoveredRun>, ChunkRanges) {
    let sources: Vec<SourceCoverage> = ranked
        .iter()
        .filter_map(|&ix| {
            coverage.get(ix).map(|c| SourceCoverage {
                source_ix: ix,
                coverage: c.clone(),
            })
        })
        .collect();
    plan_covered_runs(gap, total_bytes, &sources, ranked)
}

/// Total chunks a bounded [`ChunkRanges`] gap covers — the monotone measure the
/// no-progress guard compares across rounds. A serve-miss gap is always bounded
/// (`missing` derives it from a finite `[offset, offset + len)`), so every
/// boundary pairs; an unpaired open-ended run contributes nothing.
fn chunk_count(gap: &ChunkRanges) -> u64 {
    let boundaries = gap.boundaries();
    let mut it = boundaries.iter();
    let mut sum = 0u64;
    while let Some(a) = it.next() {
        if let Some(b) = it.next() {
            sum = sum.saturating_add(b.0.saturating_sub(a.0));
        }
    }
    sum
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use std::cell::RefCell;

    use bao_tree::{ChunkNum, ChunkRanges};
    use decdn_client::CoveredRun;
    use decdn_protocol::{Coverage, DISCOVERY_BLOCK_BYTES, num_blocks};

    use super::{
        AssembleOutcome, MAX_BACKPRESSURE_RETRIES, MAX_REASSIGN_ATTEMPTS, RunOutcome, RunSink,
        UnavailableCause, assemble,
    };

    const BAO_CHUNK_BYTES: u64 = 1024;

    fn cov(nb: u32, blocks: &[u32]) -> Coverage {
        Coverage::from_block_indices(nb, blocks.iter().copied())
    }

    /// The discovery-block index a byte offset lands in. The cast cannot truncate
    /// in practice — a blob's block count is bounded well under `u32::MAX`, the
    /// same reasoning `decdn_protocol::num_blocks` relies on.
    #[allow(clippy::cast_possible_truncation)]
    fn block_index(byte: u64) -> u32 {
        (byte / DISCOVERY_BLOCK_BYTES) as u32
    }

    /// The chunk-range span of one discovery block.
    fn block_chunks(block: u32) -> ChunkRanges {
        let per = DISCOVERY_BLOCK_BYTES / BAO_CHUNK_BYTES;
        let start = u64::from(block) * per;
        ChunkRanges::from(ChunkNum(start)..ChunkNum(start + per))
    }

    /// A scripted store + driver: models a `total_bytes` blob whose blocks fill
    /// as runs succeed, records every driven run, and returns a per-source
    /// scripted disposition.
    struct FakeSink {
        total_bytes: u64,
        /// Blocks already present (filled by a prior run or seeded held).
        present: RefCell<Vec<u32>>,
        /// Per `source_ix`: the outcome its next drive yields, popped front-first.
        /// An exhausted script yields `Filled`.
        script: RefCell<std::collections::HashMap<usize, Vec<Disposition>>>,
        /// Every `(source_ix, offset, len)` driven, in order.
        driven: RefCell<Vec<(usize, u64, u64)>>,
        /// Every `attempt` passed to `backoff`, in order.
        backoffs: RefCell<Vec<u32>>,
        /// Whether `backoff` reports the wait completed (`false` = cancelled).
        backoff_completes: bool,
        /// Every `(source_ix, waits)` passed to `backpressure_exhausted`.
        exhausted: RefCell<Vec<(usize, u32)>>,
    }

    #[derive(Clone, Copy)]
    enum Disposition {
        /// Fill the run's blocks and report `Filled`.
        Fill,
        /// Fill only the run's FIRST block, then report `Reassign` (a mid-run
        /// stall that admitted a prefix).
        PartialThenReassign,
        /// Report `Reassign` having admitted nothing.
        Reassign,
        /// Report `Backpressure` having admitted nothing.
        Backpressure,
        /// Fill only the run's FIRST block, then report `Backpressure`.
        PartialThenBackpressure,
        /// Fill the whole run, then report `Backpressure`.
        FillThenBackpressure,
        /// Report `Terminal`.
        Terminal,
        /// Report `Filled` having admitted NOTHING — a driver that violates the
        /// reassign-only completion contract. STICKY (never consumed from the
        /// script), so the run keeps reporting no-progress `Filled` and the loop
        /// would spin without the no-progress guard.
        FilledNothing,
    }

    impl FakeSink {
        fn new(total_bytes: u64) -> Self {
            Self {
                total_bytes,
                present: RefCell::new(Vec::new()),
                script: RefCell::new(std::collections::HashMap::new()),
                driven: RefCell::new(Vec::new()),
                backoffs: RefCell::new(Vec::new()),
                backoff_completes: true,
                exhausted: RefCell::new(Vec::new()),
            }
        }

        fn script(mut self, source_ix: usize, seq: &[Disposition]) -> Self {
            self.script.get_mut().insert(source_ix, seq.to_vec());
            self
        }

        /// Blocks a run spans, clamped to the blob's real block count.
        fn run_blocks(&self, run: CoveredRun) -> Vec<u32> {
            let first = block_index(run.offset);
            let end = run.offset.saturating_add(run.len);
            let last = block_index(end.saturating_sub(1));
            (first..=last)
                .filter(|&b| b < num_blocks(self.total_bytes))
                .collect()
        }
    }

    impl RunSink for FakeSink {
        async fn missing(&self, offset: u64, len: u64) -> ChunkRanges {
            let present = self.present.borrow();
            let mut gap = ChunkRanges::empty();
            let first = block_index(offset);
            let end = offset.saturating_add(len);
            let last = block_index(end.saturating_sub(1));
            for block in first..=last {
                if block < num_blocks(self.total_bytes) && !present.contains(&block) {
                    gap |= block_chunks(block);
                }
            }
            gap
        }

        async fn drive_run(&self, run: CoveredRun) -> RunOutcome {
            self.driven
                .borrow_mut()
                .push((run.source_ix, run.offset, run.len));
            let disposition = {
                let mut script = self.script.borrow_mut();
                match script.get_mut(&run.source_ix) {
                    Some(seq) => {
                        let next = seq.first().copied().unwrap_or(Disposition::Fill);
                        // Every disposition but the sticky no-progress one is consumed.
                        if !matches!(next, Disposition::FilledNothing) && !seq.is_empty() {
                            seq.remove(0);
                        }
                        next
                    }
                    None => Disposition::Fill,
                }
            };
            let blocks = self.run_blocks(run);
            match disposition {
                Disposition::Fill => {
                    self.present.borrow_mut().extend(blocks);
                    RunOutcome::Filled
                }
                Disposition::PartialThenReassign => {
                    if let Some(&first) = blocks.first() {
                        self.present.borrow_mut().push(first);
                    }
                    RunOutcome::Reassign
                }
                Disposition::Reassign => RunOutcome::Reassign,
                Disposition::Backpressure => RunOutcome::Backpressure,
                Disposition::PartialThenBackpressure => {
                    if let Some(&first) = blocks.first() {
                        self.present.borrow_mut().push(first);
                    }
                    RunOutcome::Backpressure
                }
                Disposition::FillThenBackpressure => {
                    self.present.borrow_mut().extend(blocks);
                    RunOutcome::Backpressure
                }
                Disposition::Terminal => {
                    RunOutcome::Terminal(decdn_cache::FillError::new("scripted terminal"))
                }
                // Reports Filled while admitting nothing: the gap does not shrink.
                Disposition::FilledNothing => RunOutcome::Filled,
            }
        }

        async fn backoff(&self, attempt: u32) -> bool {
            self.backoffs.borrow_mut().push(attempt);
            self.backoff_completes
        }

        fn backpressure_exhausted(&self, source_ix: usize, waits: u32) {
            self.exhausted.borrow_mut().push((source_ix, waits));
        }
    }

    /// Test A (assembly): a blob held only as A:block0 + B:block1 is assembled
    /// from BOTH — two sequential runs, each source driven only for its block.
    #[tokio::test]
    async fn assembles_across_two_partial_holders() {
        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let sink = FakeSink::new(total);
        // A (ix 0) covers block 0; B (ix 1) covers block 1.
        let coverage = vec![cov(2, &[0]), cov(2, &[1])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(outcome, AssembleOutcome::Complete));

        let driven = sink.driven.borrow();
        assert_eq!(driven.len(), 2, "one run per holder: {driven:?}");
        // A drove exactly block 0, B exactly block 1 — each paid only for its
        // own block, in offset order.
        assert_eq!(driven[0], (0, 0, DISCOVERY_BLOCK_BYTES));
        assert_eq!(driven[1], (1, DISCOVERY_BLOCK_BYTES, DISCOVERY_BLOCK_BYTES));
    }

    /// Test C (repair): a source that stalls mid-run (admits its first block
    /// then faults non-terminally) has its remaining range re-planned to another
    /// covering source; the already-admitted block is NOT re-driven.
    #[tokio::test]
    async fn repairs_a_mid_run_stall_onto_a_surviving_source() {
        let total = 2 * DISCOVERY_BLOCK_BYTES;
        // A (ix 0) covers BOTH blocks but stalls after block 0. B (ix 1) covers
        // both blocks and is healthy; A ranks first (index order).
        let sink = FakeSink::new(total).script(0, &[Disposition::PartialThenReassign]);
        let coverage = vec![cov(2, &[0, 1]), cov(2, &[0, 1])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(outcome, AssembleOutcome::Complete));

        let driven = sink.driven.borrow();
        // A drove the whole gap (both blocks), stalled after admitting block 0;
        // B then drove ONLY the remaining block 1 — block 0 is not re-driven.
        assert_eq!(driven[0].0, 0, "A drove first");
        assert_eq!(driven.last().unwrap().0, 1, "B repaired the tail");
        // No run to B ever covers block 0 (offset 0): the verified prefix is not
        // re-fetched.
        assert!(
            driven
                .iter()
                .filter(|(ix, _, _)| *ix == 1)
                .all(|(_, off, _)| *off >= DISCOVERY_BLOCK_BYTES),
            "B must not re-fetch the block A already admitted: {driven:?}"
        );
    }

    /// A run that faults with NOTHING admitted still reassigns onto a surviving
    /// source that covers the range.
    #[tokio::test]
    async fn reassigns_a_zero_progress_fault() {
        let total = DISCOVERY_BLOCK_BYTES;
        let sink = FakeSink::new(total).script(0, &[Disposition::Reassign]);
        let coverage = vec![cov(1, &[0]), cov(1, &[0])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(outcome, AssembleOutcome::Complete));
        let driven = sink.driven.borrow();
        assert_eq!(driven[0].0, 0);
        assert_eq!(driven.last().unwrap().0, 1, "B covered after A faulted");
    }

    /// A terminal run fault aborts the whole assembly — no other lane can fix a
    /// shared-pool / blacklist / over-cap fault.
    #[tokio::test]
    async fn terminal_fault_propagates() {
        let total = DISCOVERY_BLOCK_BYTES;
        let sink = FakeSink::new(total).script(0, &[Disposition::Terminal]);
        let coverage = vec![cov(1, &[0]), cov(1, &[0])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(outcome, AssembleOutcome::Terminal(_)));
        // Stopped at the terminal fault: B was never tried.
        assert_eq!(sink.driven.borrow().len(), 1);
    }

    /// A gap range no candidate covers is unavailable — the existing miss.
    #[tokio::test]
    async fn uncovered_range_is_unavailable() {
        let total = 2 * DISCOVERY_BLOCK_BYTES;
        // Only block 0 is covered by anyone; block 1 is held by nobody.
        let sink = FakeSink::new(total);
        let coverage = vec![cov(2, &[0])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(
            outcome,
            AssembleOutcome::Unavailable(UnavailableCause::Uncovered)
        ));
    }

    /// A sole covering holder that faults non-terminally is dropped, and with no
    /// survivor left the assembly ends `NoSurvivors`, under the reassign budget.
    #[tokio::test]
    async fn last_covering_holder_faulting_ends_with_no_survivors() {
        let total = DISCOVERY_BLOCK_BYTES;
        let sink = FakeSink::new(total).script(0, &[Disposition::Reassign]);
        let coverage = vec![cov(1, &[0])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(
            outcome,
            AssembleOutcome::Unavailable(UnavailableCause::NoSurvivors)
        ));
        assert_eq!(sink.driven.borrow().len(), 1);
    }

    /// No-progress guard (#1506 I2): a source that reports `Filled` while
    /// admitting NOTHING does not shrink the gap and is never dropped, so the loop
    /// would re-plan an identical round forever. The guard ends it `Unavailable`
    /// after the first fruitless round instead of spinning.
    #[tokio::test]
    async fn filled_without_progress_terminates_instead_of_spinning() {
        let total = DISCOVERY_BLOCK_BYTES;
        // The only holder covers the only block but keeps reporting Filled while
        // admitting nothing (sticky disposition).
        let sink = FakeSink::new(total).script(0, &[Disposition::FilledNothing]);
        let coverage = vec![cov(1, &[0])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(
            outcome,
            AssembleOutcome::Unavailable(UnavailableCause::NoProgress)
        ));
        // Ended after ONE fruitless run rather than re-driving it forever.
        assert_eq!(sink.driven.borrow().len(), 1);
    }

    /// Reassign budget (#1506 I4): a set of holders that all cover the range but
    /// all fault non-terminally cannot churn one lane open per survivor — the loop
    /// drops at most `MAX_REASSIGN_ATTEMPTS` sources before ending `Unavailable`.
    #[tokio::test]
    async fn reassign_budget_caps_lane_churn() {
        let total = DISCOVERY_BLOCK_BYTES;
        // Eight holders, each covering the only block, each faulting non-terminally
        // with nothing admitted. Without the budget the loop would drive all eight.
        let holders = 8usize;
        let mut sink = FakeSink::new(total);
        let mut coverage = Vec::new();
        for ix in 0..holders {
            sink = sink.script(ix, &[Disposition::Reassign]);
            coverage.push(cov(1, &[0]));
        }

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(
            outcome,
            AssembleOutcome::Unavailable(UnavailableCause::ReassignBudget)
        ));
        // Exactly `MAX_REASSIGN_ATTEMPTS` lanes were opened, not one per holder.
        assert_eq!(sink.driven.borrow().len(), MAX_REASSIGN_ATTEMPTS);
    }

    /// Runs are driven strictly in offset order and one at a time (sequential
    /// lanes): a three-block blob spread across three holders drives block 0,
    /// then 1, then 2.
    #[tokio::test]
    async fn drives_runs_in_offset_order() {
        let total = 3 * DISCOVERY_BLOCK_BYTES;
        let sink = FakeSink::new(total);
        let coverage = vec![cov(3, &[0]), cov(3, &[1]), cov(3, &[2])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(outcome, AssembleOutcome::Complete));
        let driven = sink.driven.borrow();
        let offsets: Vec<u64> = driven.iter().map(|(_, off, _)| *off).collect();
        assert_eq!(
            offsets,
            vec![0, DISCOVERY_BLOCK_BYTES, 2 * DISCOVERY_BLOCK_BYTES]
        );
    }

    /// #2178: a sole source that refuses for backpressure is kept, waited on, and
    /// driven again — not dropped, which would end the assembly `Unavailable`.
    #[tokio::test]
    async fn sole_source_backpressure_waits_and_retries() {
        let total = DISCOVERY_BLOCK_BYTES;
        let sink =
            FakeSink::new(total).script(0, &[Disposition::Backpressure, Disposition::Backpressure]);
        let coverage = vec![cov(1, &[0])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(outcome, AssembleOutcome::Complete));
        let driven = sink.driven.borrow();
        assert_eq!(driven.len(), 3, "two refusals, then the fill: {driven:?}");
        assert!(driven.iter().all(|(ix, _, _)| *ix == 0));
        assert_eq!(*sink.backoffs.borrow(), vec![1, 2]);
    }

    /// A backpressure refusal reassigns without waiting when another survivor
    /// covers the still-missing gap.
    #[tokio::test]
    async fn backpressure_reassigns_when_a_survivor_covers_the_gap() {
        let total = DISCOVERY_BLOCK_BYTES;
        let sink = FakeSink::new(total).script(0, &[Disposition::Backpressure]);
        let coverage = vec![cov(1, &[0]), cov(1, &[0])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(outcome, AssembleOutcome::Complete));
        let driven = sink.driven.borrow();
        assert_eq!(driven.len(), 2);
        assert_eq!(driven[1].0, 1, "the survivor took over");
        assert!(sink.backoffs.borrow().is_empty(), "no wait was needed");
    }

    /// A partial holder does not count as a replacement: when the survivor covers
    /// only part of the gap, the refusing source is kept and waited on.
    #[tokio::test]
    async fn backpressure_waits_when_the_survivor_covers_only_part_of_the_gap() {
        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let sink = FakeSink::new(total).script(0, &[Disposition::Backpressure]);
        // A (ix 0) holds both blocks; B (ix 1) holds block 1 only.
        let coverage = vec![cov(2, &[0, 1]), cov(2, &[1])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(outcome, AssembleOutcome::Complete));
        assert_eq!(*sink.backoffs.borrow(), vec![1]);
    }

    /// A sole source that refuses every retry ends the assembly `Backpressured`
    /// once the wait budget is spent, rather than wait forever, and the sink is
    /// told which source outlasted it.
    #[tokio::test]
    async fn backpressure_budget_bounds_the_waits() {
        let total = DISCOVERY_BLOCK_BYTES;
        let refusals = vec![Disposition::Backpressure; 32];
        let sink = FakeSink::new(total).script(0, &refusals);
        let coverage = vec![cov(1, &[0])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(outcome, AssembleOutcome::Backpressured));
        let budget = usize::try_from(MAX_BACKPRESSURE_RETRIES).unwrap();
        assert_eq!(sink.backoffs.borrow().len(), budget);
        assert_eq!(sink.driven.borrow().len(), budget + 1);
        assert_eq!(
            *sink.exhausted.borrow(),
            vec![(0, MAX_BACKPRESSURE_RETRIES)]
        );
    }

    /// A refusal that arrives after its run already admitted the whole gap needs
    /// no wait: the next round finds the gap empty and completes.
    #[tokio::test]
    async fn backpressure_after_a_full_fill_completes_without_a_wait() {
        let total = DISCOVERY_BLOCK_BYTES;
        let sink = FakeSink::new(total).script(0, &[Disposition::FillThenBackpressure]);
        let coverage = vec![cov(1, &[0])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(outcome, AssembleOutcome::Complete));
        assert!(sink.backoffs.borrow().is_empty());
        assert!(sink.exhausted.borrow().is_empty());
    }

    /// Gap progress restores the wait budget: a long pull that meets more
    /// refusals in total than the budget still completes while each round admits
    /// bytes before its refusal. (A real source cannot drip bytes this way to
    /// keep itself waited on: a refusal counts as backpressure only at the stream
    /// open, before any byte of the run arrives. This fake folds a prior run's
    /// progress and the next run's refusal into one drive.)
    #[tokio::test]
    async fn backpressure_budget_resets_on_progress() {
        let blocks = MAX_BACKPRESSURE_RETRIES + 2;
        let total = u64::from(blocks) * DISCOVERY_BLOCK_BYTES;
        let refusals = vec![Disposition::PartialThenBackpressure; usize::try_from(blocks).unwrap()];
        let sink = FakeSink::new(total).script(0, &refusals);
        let all: Vec<u32> = (0..blocks).collect();
        let coverage = vec![cov(blocks, &all)];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(outcome, AssembleOutcome::Complete));
        let backoffs = sink.backoffs.borrow();
        assert!(
            backoffs.len() > usize::try_from(MAX_BACKPRESSURE_RETRIES).unwrap(),
            "more total waits than the budget: {backoffs:?}"
        );
        assert!(
            backoffs.iter().all(|&a| a == 1),
            "each wait followed progress"
        );
    }

    /// A pull cancelled during a backpressure wait ends `Cancelled`.
    #[tokio::test]
    async fn backpressure_wait_observes_cancellation() {
        let total = DISCOVERY_BLOCK_BYTES;
        let mut sink = FakeSink::new(total).script(0, &[Disposition::Backpressure]);
        sink.backoff_completes = false;
        let coverage = vec![cov(1, &[0])];

        let outcome = assemble(&sink, &coverage, 0, total, total).await;
        assert!(matches!(outcome, AssembleOutcome::Cancelled));
        assert_eq!(sink.driven.borrow().len(), 1);
    }
}
