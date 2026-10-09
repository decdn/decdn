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
//!
//! # Funding recovery
//!
//! A source that refuses this node's funding ([`RunOutcome::Unfunded`]) leaves
//! the plan, and the other survivors carry on: the refusal scopes to that
//! source. Only when no survivor can fill a still-missing range does the
//! assembly run its funding recovery step ([`RunSink::recover`], ADR 003
//! § Funding recovery). A step that raises the deposit brings the refusing
//! sources back for one more pass; a step that cannot ends the assembly
//! [`AssembleOutcome::FundingNeeded`].

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
    /// A delivery fault of this source: drop this run's source and re-plan its
    /// still-missing remainder against the surviving candidates. The
    /// reassign-only tail.
    Reassign,
    /// The source refused this node's funding: an `Unfunded` refusal, a funding
    /// rejection, or this node's pool short of the next voucher. The source
    /// leaves the plan like a reassigned one, and comes back if the assembly's
    /// funding recovery step raises the deposit ([`RunSink::recover`]).
    Unfunded,
    /// The source refused to open this run for a reason that usually clears with
    /// time — most often a per-signer live cap, or a load shed (#2178). It says
    /// nothing about whether the source can serve the range. The loop reassigns
    /// when another survivor covers the gap, and otherwise keeps the source and
    /// drives it again after a [`RunSink::backoff`].
    Backpressure,
    /// The whole assembly is over: a fatal fault another lane cannot fix (a
    /// local fault, an origin blacklist, an over-cap blob). Propagate it.
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
            Self::Unfunded => "unfunded",
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
    /// Funding needed: the only sources of a still-missing range refused this
    /// node's funding, and no funding recovery step can raise the deposit
    /// (ADR 003 § Funding recovery). Surfaces to the client like
    /// [`Self::Unavailable`] (a truncated stream) and names this node's own
    /// funding as the cause for the operator.
    FundingNeeded,
    /// The funding recovery step ran and failed on a fault in this node (an
    /// RPC, allowance or escrow fault). Surfaces to the client like
    /// [`Self::Unavailable`]; the step logged its cause for the operator.
    RecoveryFailed,
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

    /// Run the assembly's funding recovery step: no surviving source can fill
    /// a still-missing range of `gap_chunks` chunks, and at least one dropped
    /// source refused this node's funding. Returns `true` when the deposit
    /// rose (or is settling after a rise), so those sources may be asked
    /// again. `Err` names the assembly's end.
    async fn recover(&self, gap_chunks: u64) -> Result<(), RecoveryEnd>;
}

/// Why the assembly's funding recovery step lets no dropped source be asked
/// again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryEnd {
    /// No step is allowed or possible: [`AssembleOutcome::FundingNeeded`].
    FundingNeeded,
    /// The step failed on a fault in this node:
    /// [`AssembleOutcome::RecoveryFailed`].
    Failed,
}

impl From<RecoveryEnd> for AssembleOutcome {
    fn from(end: RecoveryEnd) -> Self {
        match end {
            RecoveryEnd::FundingNeeded => Self::FundingNeeded,
            RecoveryEnd::Failed => Self::RecoveryFailed,
        }
    }
}

/// Assemble `[offset, offset+len)` of a `total_bytes` blob across the ranked
/// `coverage` candidates (index = candidate index, ordered best-first).
///
/// Loops: derive the gap, plan it into runs by coverage, drive each run in
/// offset order, and on a non-terminal run fault drop that source and re-plan
/// the remainder. A backpressure refusal from a source no survivor can replace
/// waits and re-drives that source instead (see the module's `# Backpressure`).
/// A source that refuses this node's funding leaves the plan; once no survivor
/// can fill the gap, the funding recovery step decides whether those sources
/// are asked again (see the module's `# Funding recovery`).
/// Terminates on a fully-present gap ([`AssembleOutcome::Complete`]),
/// an uncovered range ([`AssembleOutcome::Unavailable`]), a sole source that
/// outlasts the wait budget ([`AssembleOutcome::Backpressured`]), funding
/// needed ([`AssembleOutcome::FundingNeeded`]), a terminal fault, or
/// cancellation.
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
    // Sources dropped because they refused this node's funding. They come back
    // only after a funding recovery step raises the deposit.
    let mut unfunded: Vec<usize> = Vec::new();
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
        let gap_chunks = chunk_count(&gap);
        if surviving.is_empty() {
            // Every candidate that covered a still-missing range faulted. When
            // some refused this node's funding, the funding recovery step
            // decides; otherwise nothing is left to try. Wire-identical to the
            // no-provider miss.
            if unfunded.is_empty() {
                return AssembleOutcome::Unavailable(UnavailableCause::NoSurvivors);
            }
            if let Err(end) = sink.recover(gap_chunks).await {
                return end.into();
            }
            surviving.append(&mut unfunded);
            surviving.sort_unstable();
            prev_gap_chunks = None;
            continue;
        }
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
            // No survivor covers a still-missing range. A source dropped for
            // refusing this node's funding may: the funding recovery step
            // decides whether it is asked again.
            if unfunded.is_empty() {
                return AssembleOutcome::Unavailable(UnavailableCause::Uncovered);
            }
            if let Err(end) = sink.recover(gap_chunks).await {
                return end.into();
            }
            surviving.append(&mut unfunded);
            surviving.sort_unstable();
            prev_gap_chunks = None;
            continue;
        }
        let mut faulted: Option<(usize, RunOutcome)> = None;
        for run in &runs {
            match sink.drive_run(*run).await {
                RunOutcome::Filled => {}
                RunOutcome::Terminal(err) => return AssembleOutcome::Terminal(err),
                RunOutcome::Cancelled => return AssembleOutcome::Cancelled,
                // Stop this round at the first fault: the faulted source may also
                // own later planned runs, so re-plan the whole remainder rather
                // than press on with a plan built around a dead source.
                outcome @ (RunOutcome::Reassign
                | RunOutcome::Unfunded
                | RunOutcome::Backpressure) => {
                    faulted = Some((run.source_ix, outcome));
                    break;
                }
            }
        }
        let Some((ix, outcome)) = faulted else {
            // Every planned run filled its range; the runs partition the gap, so
            // the next `missing` is empty and the loop returns `Complete`.
            repaired_last_round = false;
            continue;
        };
        repaired_last_round = true;
        // A funding refusal scopes to its source: it leaves the plan without
        // spending the reassign budget, and the other survivors carry on.
        if matches!(outcome, RunOutcome::Unfunded) {
            surviving.retain(|&s| s != ix);
            unfunded.push(ix);
            continue;
        }
        let backpressure = matches!(outcome, RunOutcome::Backpressure);
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
pub(super) fn plan_over(
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
mod tests;
