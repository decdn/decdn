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

use bao_tree::ChunkRanges;
use decdn_cache::FillError;
use decdn_client_pull::{CoveredRun, SourceCoverage, plan_covered_runs};
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

/// One run's terminal disposition, as the driving sink saw it.
pub(crate) enum RunOutcome {
    /// The run's `[offset, offset+len)` is fully present in the store now.
    Filled,
    /// A non-terminal fault (`retry_disposition == RetryElsewhere`): drop this
    /// run's source and re-plan its still-missing remainder against the
    /// surviving candidates. The reassign-only tail.
    Reassign,
    /// The whole assembly is over — a terminal fault (`retry_disposition ==
    /// Terminal`) that another lane cannot fix (a shared-pool voucher rejection,
    /// an origin blacklist, an over-cap blob). Propagate it.
    Terminal(FillError),
    /// Cooperative cancellation: the serve leg finished first (client
    /// disconnect / shutdown), so the whole pull stops.
    Cancelled,
}

/// The outcome of assembling a byte range across partial holders.
pub(crate) enum AssembleOutcome {
    /// Every gap byte was pulled and admitted.
    Complete,
    /// Some still-missing range no surviving candidate covers. Origins advertise
    /// all-ones coverage, so an admitted origin candidate makes this
    /// unreachable; without one it means the blob is not fully available across
    /// the known holders. This runs on the serve-miss pull thread, which the serve
    /// leg spawns only AFTER it has already signed and sent `ok: true` (the
    /// response commits to `total_bytes` before any byte is pulled). So this
    /// surfaces to the client as a TRUNCATED stream — the leg fills nothing and
    /// the serve encoder ends short — not as a signed `NotFound`. A pre-serve
    /// coverage-union refusal would be needed to answer `NotFound` here, and the
    /// coverage-union probe gather (#1506) narrows how often the gap is uncoverable
    /// in the first place rather than adding one.
    Unavailable,
    /// A run faulted terminally; propagate the fault to the serve leg.
    Terminal(FillError),
    /// The pull was cancelled mid-assembly.
    Cancelled,
}

/// The store + per-run driver the assembly loop plans over. The real
/// implementation opens a buyer lane and drives [`decdn_client_pull::drive`];
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
}

/// Assemble `[offset, offset+len)` of a `total_bytes` blob across the ranked
/// `coverage` candidates (index = candidate index, ordered best-first).
///
/// Loops: derive the gap, plan it into runs by coverage, drive each run in
/// offset order, and on a non-terminal run fault drop that source and re-plan
/// the remainder. Terminates on a fully-present gap ([`AssembleOutcome::Complete`]),
/// an uncovered range ([`AssembleOutcome::Unavailable`]), a terminal fault, or
/// cancellation.
///
/// Two guards keep the loop finite even under a driver that violates the
/// reassign-only completion contract:
///
/// - **No-progress guard.** `surviving` shrinks only on a reassign, so a round
///   that reports every run [`RunOutcome::Filled`] yet does NOT shrink the gap
///   would re-plan an identical round forever. When a round drops no source and
///   the gap does not shrink, the assembly ends [`AssembleOutcome::Unavailable`]
///   rather than spin.
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
    // The prior round's gap measure and whether it dropped a source — the two
    // inputs the no-progress guard reads.
    let mut prev_gap_chunks: Option<u64> = None;
    let mut dropped_last_round = false;
    // Sources dropped so far, bounded by `MAX_REASSIGN_ATTEMPTS`.
    let mut reassigns = 0usize;
    loop {
        let gap = sink.missing(offset, len).await;
        if gap.is_empty() {
            return AssembleOutcome::Complete;
        }
        if surviving.is_empty() {
            // Every candidate that covered a still-missing range faulted; nothing
            // left to try. Wire-identical to the no-provider miss.
            return AssembleOutcome::Unavailable;
        }
        let gap_chunks = chunk_count(&gap);
        // No-progress guard: a round that dropped no source and did not shrink the
        // gap will re-plan identically next round. End rather than spin.
        if let Some(prev) = prev_gap_chunks
            && !dropped_last_round
            && gap_chunks >= prev
        {
            return AssembleOutcome::Unavailable;
        }
        prev_gap_chunks = Some(gap_chunks);
        let sources: Vec<SourceCoverage> = surviving
            .iter()
            .filter_map(|&ix| {
                coverage.get(ix).map(|c| SourceCoverage {
                    source_ix: ix,
                    coverage: c.clone(),
                })
            })
            .collect();
        // `surviving` is already in ranked order and IS the rank the concentrate
        // planner ties-breaks on.
        let (runs, uncovered) = plan_covered_runs(&gap, total_bytes, &sources, &surviving);
        if !uncovered.is_empty() {
            return AssembleOutcome::Unavailable;
        }
        let mut faulted: Option<usize> = None;
        for run in &runs {
            match sink.drive_run(*run).await {
                RunOutcome::Filled => {}
                RunOutcome::Reassign => {
                    // Stop this round at the first fault: the faulted source may
                    // also own later planned runs, so re-plan the whole remainder
                    // rather than press on with a plan built around a dead source.
                    faulted = Some(run.source_ix);
                    break;
                }
                RunOutcome::Terminal(err) => return AssembleOutcome::Terminal(err),
                RunOutcome::Cancelled => return AssembleOutcome::Cancelled,
            }
        }
        match faulted {
            // Every planned run filled its range; the runs partition the gap, so
            // the next `missing` is empty and the loop returns `Complete`.
            None => dropped_last_round = false,
            // Drop the faulted source and re-plan. The store keeps the verified
            // bytes, so `missing` next round excludes them: no re-fetch, no
            // re-pay. Bounded by the reassign budget so a pathological set cannot
            // churn one lane open per survivor.
            Some(ix) => {
                reassigns += 1;
                if reassigns >= MAX_REASSIGN_ATTEMPTS {
                    return AssembleOutcome::Unavailable;
                }
                surviving.retain(|&s| s != ix);
                dropped_last_round = true;
            }
        }
    }
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
    use decdn_client_pull::CoveredRun;
    use decdn_protocol::{Coverage, DISCOVERY_BLOCK_BYTES, num_blocks};

    use super::{AssembleOutcome, MAX_REASSIGN_ATTEMPTS, RunOutcome, RunSink, assemble};

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
                Disposition::Terminal => {
                    RunOutcome::Terminal(decdn_cache::FillError::new("scripted terminal"))
                }
                // Reports Filled while admitting nothing: the gap does not shrink.
                Disposition::FilledNothing => RunOutcome::Filled,
            }
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
        assert!(matches!(outcome, AssembleOutcome::Unavailable));
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
        assert!(matches!(outcome, AssembleOutcome::Unavailable));
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
        assert!(matches!(outcome, AssembleOutcome::Unavailable));
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
}
