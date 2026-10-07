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
