//! Client-only multi-source fetch orchestration (spec §5.3): fan a request out
//! across several paid [`BlobSource`]s that all write
//! into ONE shared [`IngestStore`], assembling a
//! byte-identical blob.
//!
//! One worker future per source drives [`fill_gap`]
//! over the request's gap-set. The gap-set is spread across sources by
//! discovery-block coverage ([`crate::coverage_plan::spread_segments`]): each
//! block is assigned to one covering source, rarest-cover-first, into
//! contiguous per-source runs, so every source that covers anything starts
//! with work it can actually serve. A run every covering source holds WHOLE is
//! then split evenly across those sources (`split_evenly`) so an
//! otherwise-idle source still gets a share. A freed source does not idle
//! either way: it *steals* the aligned second half of the largest remaining
//! range it also covers ([`steal_split`]), so a fast source keeps helping a
//! slow one — but never a range outside its own coverage.
//!
//! # Lane correctness — one unit per source
//!
//! Each worker holds EXACTLY ONE outstanding range at a time (structural: the
//! worker loop drives one `fill_gap` to completion before it picks the next
//! range). Two concurrent units to the same node would share one
//! `(signer, provider)` payment lane and reintroduce the concurrent-same-lane
//! voucher hazard bundle pull serializes against — so parallelism comes only
//! from having many sources, never from stacking one source.
//!
//! # Scope
//!
//! This module is the client's orchestration only. It calls `fill_gap` per
//! range and never touches the node's serve path.
//!
//! # Cancellation — closing the double-pay
//!
//! A worker's `fill_gap` runs under a [`tokio::select!`] against a per-source
//! `CancelHandle` and a progress-relative watchdog, so it can be interrupted
//! mid-fetch. Because [`crate::ClientRangedStore::ingest_stream`] durably
//! checkpoints verified groups as it streams, dropping the `fill_gap` future
//! leaves the delivered+verified prefix in the store — [`RangedStore::missing_ranges`](decdn_bao_range::RangedStore::missing_ranges)
//! then reports only the true remainder, so a cancel never loses or refetches a
//! verified byte. Two triggers drive one cancellation mechanism:
//!
//! - **Steal.** When a freed worker steals a busy victim's tail `[mid, end)`,
//!   `Work::pick` trims the victim's assignment to `[start, mid)` AND signals
//!   the victim's `CancelHandle`. The victim stops fetching past `mid`,
//!   re-queues the still-missing part of its trimmed `[start, mid)` to
//!   `pending`, and picks again — so the stolen tail is fetched (and paid for)
//!   by exactly ONE source, not two. Without this the victim's already-running
//!   `fill_gap` would keep paying to `end` — both sources paying for one tail.
//! - **Stall / fault.** A source with no verified progress within
//!   `unit_deadline` (the watchdog trips), or whose `fill_gap` returns a
//!   RETRYABLE `Err` ([`crate::retry_disposition`] ==
//!   `RetryElsewhere`: a stall, a transport reset, a node-specific refusal), has
//!   the UN-fetched remainder of its range re-queued to `pending` for a DIFFERENT
//!   source, and stops taking work. Verified bytes already stored are never
//!   refetched.
//!
//! # Terminal faults — no pointless reassignment
//!
//! Two kinds of `fill_gap` `Err` abort the whole fetch instead of reassigning the
//! range, because another lane cannot fix either:
//!
//! - What the SHARED classifier ([`crate::retry_disposition`]) rules `Terminal` —
//!   a payment-layer voucher rejection, an origin blacklist, or an over-cap blob —
//!   which is terminal on the single-source path too.
//! - A shared-pool exhaustion ([`crate::PoolExhausted`], the pacer's `Refuse`).
//!   This is terminal ONLY for this scheduler: every lane draws the ONE shared
//!   pool, so no lane can fund it. The single-source path instead fails over on a
//!   budget refusal (a cheaper provider may fit), so the shared classifier keeps
//!   `PoolExhausted` retryable and this scheduler applies the pool-scope rule
//!   itself.
//!
//! Either way the worker propagates THAT typed error out of the set, which cancels
//! the peer workers and fails `multi_source_fetch` with it — the CLI inspects the
//! type to surface the correct owner-side remedy. The range is NOT reassigned.
//!
//! If every source stops with the request still incomplete without a terminal
//! fault, the fetch returns a generic "all sources failed" error rather than
//! hanging.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use decdn_bao_range::{AlignedRange, align_range};
use decdn_protocol::Coverage;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::coverage_plan::{SourceCoverage, covers_byte_range, spread_segments};
use crate::driver::{
    DriveConfig, DriveCounters, PRESENT_RECORD_FLUSH_INTERVAL, PoolExhausted, SharedPool,
    contiguous_byte_ranges, drive_with_interval_flush, fill_gap, ranges_content_len,
};
use crate::ledgers::LaneLedgers;
use crate::retry::{RetryDisposition, retry_disposition};
use crate::segment::{split_evenly, steal_split};
use crate::source::{BlobSource, Funder, IngestStore};
use crate::{Pacer, PoolContext, PoolLedger, ProgressCallback};

/// One paid delivery lane in a multi-source fetch: a source paired with the
/// `(ctx, ledger)` that pays IT — never shared across sources.
///
/// A voucher is scoped to one on-chain `provider` (ADR 039 § Payment model): the
/// [`PoolContext::provider`](crate::PoolContext) it is signed against, and it is
/// invalid if redeemed by any other node. And a [`PoolLedger`] tracks ONE
/// `(signer, provider)` lane's cumulative watermark. So each admitted source —
/// a distinct operator, by [`admit_sources`](crate::discovery::admit_sources)'s
/// operator spread — carries its OWN `ctx` (built via
/// [`PoolContext::with_provider`](crate::PoolContext::with_provider) for that
/// source's provider and that lane's persisted prior cumulative) and its OWN
/// `ledger` (seeded from the same lane's `Cumulative`). One shared pool DEPOSIT
/// still backs every lane; the aggregate-solvency reader
/// ([`multi_source_fetch`]) sums the lanes' committed so no lane over-draws it.
pub struct SourceLane<'a, S> {
    /// The paid source this lane fetches from.
    pub source: &'a S,
    /// The buyer context that pays this source — its `provider` is this lane's
    /// payee, behind the shared `Arc<Mutex<..>>` so a reactive top-up's new
    /// deposit is visible to this lane's next open.
    pub ctx: Arc<Mutex<PoolContext>>,
    /// This lane's voucher ledger, seeded from its persisted cumulative — the
    /// per-`(signer, provider)` watermark, never shared with another lane.
    pub ledger: Arc<PoolLedger>,
    /// Which discovery blocks this source actually holds (#1506, B1's
    /// `Probed::coverage`). Drives both the initial coverage-aware spread
    /// ([`spread_segments`]) and the scheduler's internal coverage-filtered
    /// steal — this lane is never assigned, and never steals, a range
    /// outside what this says it can serve.
    pub coverage: Coverage,
}

impl<S> std::fmt::Debug for SourceLane<'_, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The source is opaque and `PoolContext` guards a signing key, so print
        // only the non-sensitive lane identity (its payee provider).
        let provider = self.ctx.lock().ok().map(|c| c.provider);
        f.debug_struct("SourceLane")
            .field("provider", &provider)
            .finish_non_exhaustive()
    }
}

/// Per-source interrupt: an edge-triggered wakeup ([`Notify`]) plus a `flag`
/// that says the wakeup means "cancel", not a stale permit. The stealer sets
/// `flag` and wakes the victim under the `Work` lock; the victim clears it on
/// its next `Work::pick`, also under the lock, so the two never race.
struct CancelHandle {
    /// `true` once a steal has claimed this source's tail; the victim must stop.
    flag: AtomicBool,
    /// Wakes the victim's `cancelled` future so it re-reads `flag` promptly.
    notify: Notify,
}

impl CancelHandle {
    fn new() -> Self {
        Self {
            flag: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }
}

/// Resolve once this handle is genuinely cancelled. A [`Notify`] permit can be
/// stored by a stale `notify_one` from a prior unit, so a wakeup alone is not
/// proof: re-read `flag` and keep waiting until it is set. This also closes the
/// lost-wakeup — `notify_one` stores a permit even if it fires before the await,
/// so a cancel signalled before the victim parks here is still observed.
async fn cancelled(handle: &CancelHandle) {
    loop {
        handle.notify.notified().await;
        if handle.flag.load(Ordering::Acquire) {
            return;
        }
    }
}

/// One planned run staged for seeding into `pending`, with the piece count it
/// will split into. `max_pieces` bounds the split to what is useful for THIS run —
/// the number of lanes that hold it whole — while the seeding loop's global budget
/// (`lanes.len()`) decides how much of that headroom each run actually uses.
/// `pieces` starts at one contiguous span and only grows to reach otherwise-idle
/// lanes.
struct RunSeed {
    offset: u64,
    len: u64,
    max_pieces: usize,
    pieces: usize,
}

/// Bytes of `[start, start+len)` the store still misses — this worker's range is
/// disjoint from every peer's, so this reflects only its own delivery frontier.
/// An error reads as "no observable progress" (`u64::MAX`), which trips the
/// watchdog and drops the source — the safe direction.
async fn missing_bytes<St>(store: &St, start: u64, len: u64) -> u64
where
    St: IngestStore,
{
    match store.missing_ranges(start, len).await {
        Ok(ranges) => contiguous_byte_ranges(&ranges, store.total_bytes())
            .iter()
            .map(|(_, l)| *l)
            .fold(0, u64::saturating_add),
        Err(_) => u64::MAX,
    }
}

/// Progress-relative stall watchdog: resolve (trip) once a full `deadline`
/// window passes with the store's missing count over `[start, len)` neither
/// shrinking nor reaching zero — i.e. no verified progress and not yet done. A
/// source that keeps delivering, however slowly, resets the window each sample
/// and never trips; a source that has fully delivered (missing == 0) is left to
/// `fill_gap`'s own completion, never tripped. A zero deadline disables the
/// watchdog.
///
/// `missing_bytes` progress is checkpoint-granular — it advances only every 4
/// MiB `INGEST_CHECKPOINT_BYTES` interval — so `unit_deadline` must sit
/// comfortably above `4 MiB / min-expected-throughput` to avoid falsely
/// reassigning a healthy-but-slow source mid-checkpoint.
async fn watchdog<St>(store: &St, start: u64, len: u64, deadline: Duration)
where
    St: IngestStore,
{
    if deadline.is_zero() {
        std::future::pending::<()>().await;
    }
    let mut prev = missing_bytes(store, start, len).await;
    loop {
        tokio::time::sleep(deadline).await;
        let now = missing_bytes(store, start, len).await;
        if now == 0 {
            // Delivered in full; `fill_gap` will finish paying and return
            // `Completed`. Keep waiting rather than tripping a done range.
            prev = now;
            continue;
        }
        if now >= prev {
            // A whole window with no shrink and bytes still missing: a stall.
            return;
        }
        prev = now;
    }
}

/// How one unit of work ended, so the worker loop knows what to do next.
enum UnitOutcome {
    /// `fill_gap` filled the range — free the lane and pick again.
    Completed,
    /// A steal claimed this source's tail — re-queue the trimmed remainder and
    /// stay live (pick again).
    Cancelled,
    /// The source stalled or hit a RETRYABLE fault — re-queue the remainder for a
    /// DIFFERENT source and stop taking work. A TERMINAL fault does not reach
    /// here: the worker returns its typed error directly, aborting the fetch.
    ///
    /// Carries the cause so the fetch can name it: `Some(e)` for a retryable
    /// `fill_gap` error, `None` for a watchdog stall, which has no error by
    /// construction. Dropping it would leave a failed fetch describable only as
    /// "all sources failed", with the node-specific refusal, transport reset, or
    /// bao mismatch that actually ended it unrecoverable — the CLI installs no
    /// tracing subscriber, so an unreturned error is a destroyed one.
    Faulted(Option<anyhow::Error>),
}

/// Why one lane stopped taking work, kept for the diagnosis a failed fetch
/// reports. `provider` is the lane's payee address — the only stable identity
/// the scheduler holds for a source.
struct LaneFault {
    provider: Option<Address>,
    /// `None` for a watchdog stall (no error exists), `Some` for a retryable fault.
    err: Option<anyhow::Error>,
}

impl LaneFault {
    /// One line naming the lane and what ended it, for the aggregate error.
    fn describe(&self) -> String {
        let who = self.provider.map_or_else(
            || "unknown provider".to_string(),
            |p| format!("provider {p}"),
        );
        match &self.err {
            Some(e) => format!("{who}: {e:#}"),
            None => format!("{who}: no verified progress within the unit deadline"),
        }
    }
}

/// Client-side knobs for the multi-source scheduler (spec §8). The blob-size
/// engagement gate and the `enabled` kill switch live at the CLI wiring layer;
/// this is what the scheduler itself consumes.
#[derive(Debug, Clone, Copy)]
pub struct MultiSourceConfig {
    /// Cap on concurrently-used holders. Enforced by the caller's admission
    /// (`discovery::admit_sources`) before it ever builds `lanes` — every
    /// lane `multi_source_fetch` is handed here is engaged, since which
    /// discovery blocks a lane serves is decided by its coverage (#1506), not
    /// by an arbitrary segment-count split.
    pub max_sources: usize,
    /// No-verified-progress deadline before a source's remaining range is
    /// reassigned. Read by the stall watchdog, which each worker races its `fill_gap`
    /// against. `Duration::ZERO` disables the watchdog.
    pub unit_deadline: Duration,
}

/// Shared work-state, guarded by one [`AsyncMutex`]. `pending` seeds with the
/// coverage-planned initial segments; `in_flight[i]` is source `i`'s
/// currently-owned range as `(start, len)` (`None` = idle), which is both the
/// tail-steal remaining-set and the "at most one source owns any range"
/// ledger.
struct Work {
    /// Segments not yet claimed by any worker (drains as workers pick). A
    /// worker only ever pops an entry its own [`Coverage`] includes (#1506):
    /// [`Work::pick`] skips past any entry it cannot serve rather than
    /// dequeuing it, so an item stays here until a covering worker is free to
    /// take it.
    pending: VecDeque<AlignedRange>,
    /// Per-source current range, `None` when the source holds nothing.
    in_flight: Vec<Option<(u64, u64)>>,
    /// Per-source interrupt handles, indexed like `in_flight`. A steal signals
    /// `cancel[victim]`; the victim clears it on its next `pick`.
    cancel: Vec<Arc<CancelHandle>>,
    /// `alive[i]` is `false` once worker `i` has permanently left the set (see
    /// [`Work::retire`]) — never coming back to `pick` again, whether because
    /// it ran out of coverable work or because it faulted.
    alive: Vec<bool>,
}

impl Work {
    /// Every worker is free and nothing is queued: the fan-out has no work left,
    /// so a worker that cannot pick may exit rather than park.
    fn all_idle(&self) -> bool {
        self.pending.is_empty() && self.in_flight.iter().all(Option::is_none)
    }

    /// Worker `i`'s in-flight slot. An out-of-range `i` is a wiring bug, not a
    /// condition to absorb: silently no-op'ing it would let the worker fetch —
    /// and pay for — a range `in_flight` never records, which a peer then reads
    /// as unowned and steals, so both pay for it.
    fn slot_mut(&mut self, i: usize) -> anyhow::Result<&mut Option<(u64, u64)>> {
        match self.in_flight.get_mut(i) {
            Some(slot) => Ok(slot),
            None => anyhow::bail!("worker index {i} out of range for in-flight slots"),
        }
    }

    /// Under the caller's lock, choose worker `i`'s next range. Pop the FIRST
    /// pending segment `coverage` includes (a worker skips past, never
    /// dequeues, an entry it cannot serve — #1506); when none remain, steal
    /// the aligned second half of the largest COVERABLE range still in flight
    /// ([`steal_split`]), trimming the victim so no other freed worker can
    /// re-steal the same tail. Records the choice in `in_flight[i]`. `Ok(None)`
    /// means there is nothing this worker can start right now — it parks until
    /// a peer changes the work state, and exits only once [`Work::all_idle`]
    /// holds.
    ///
    /// # Errors
    ///
    /// An out-of-range worker index; or the alignment error [`steal_split`]
    /// raises on an out-of-bounds range (never on the ranges this scheduler
    /// feeds it).
    fn pick(
        &mut self,
        i: usize,
        total_bytes: u64,
        coverage: &Coverage,
    ) -> anyhow::Result<Option<AlignedRange>> {
        // This worker is starting a fresh unit: clear any cancel signal left from
        // a prior unit, under the lock, so a stale `notify_one` permit cannot
        // spuriously cancel the new unit (see `cancelled`).
        match self.cancel.get(i) {
            Some(handle) => handle.flag.store(false, Ordering::Release),
            None => anyhow::bail!("worker index {i} out of range for cancel handles"),
        }
        let coverable = self.pending.iter().position(|seg| {
            covers_byte_range(coverage, seg.fetch_start(), seg.fetch_len(), total_bytes)
        });
        if let Some(pos) = coverable {
            // `pos` came from this same deque's `position`, so it is always
            // in range; `VecDeque::remove` returns `Option`, never panics.
            if let Some(seg) = self.pending.remove(pos) {
                *self.slot_mut(i)? = Some((seg.fetch_start(), seg.fetch_len()));
                return Ok(Some(seg));
            }
        }

        // Nothing pending this worker can serve: every remaining byte is
        // either in flight on a busy worker or outside this worker's own
        // coverage. Steal the aligned second half of the largest COVERABLE
        // such range. `in_flight[i]` is `None` here (cleared before this
        // pick), so this worker is excluded from the remaining set and never
        // steals from itself.
        let (owners, remaining): (Vec<usize>, Vec<(u64, u64)>) = self
            .in_flight
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| slot.map(|r| (idx, r)))
            .unzip();
        // `steal_split` returns WHICH remaining range it split, so the trim below
        // lands on that exact victim — no second argmax to agree with. The
        // predicate excludes any range this worker's `coverage` does not fully
        // include, so a narrow-coverage worker that finds nothing it can serve
        // gets `None` here and parks rather than stealing a range it cannot
        // deliver.
        let Some((v, half)) = steal_split(&remaining, total_bytes, |s, l| {
            covers_byte_range(coverage, s, l, total_bytes)
        })?
        else {
            *self.slot_mut(i)? = None;
            return Ok(None);
        };

        // Trim the victim to end at the split point, so a later freed worker sees
        // the shortened tail and cannot re-steal the half this worker just took:
        // at most one source owns any range, by construction, in the work-state.
        //
        // Every branch that cannot complete that trim DECLINES the steal instead
        // of proceeding. Handing out `half` with the victim untrimmed would leave
        // two workers owning overlapping ranges, and both would pay for the
        // overlap — the exact double-pay the trim exists to prevent.
        let trimmed = owners.get(v).copied().and_then(|victim| {
            if victim == i {
                return None;
            }
            let (start, len) = self.in_flight.get_mut(victim)?.as_mut()?;
            if half.fetch_start() <= *start {
                return None;
            }
            *len = half.fetch_start() - *start;
            Some(victim)
        });
        let Some(victim) = trimmed else {
            *self.slot_mut(i)? = None;
            return Ok(None);
        };

        // Signal the victim to STOP fetching past the split. Its already-running
        // `fill_gap` would otherwise fetch — and pay for — the tail this worker
        // just took. Set the flag then wake it, both under the caller's `Work`
        // lock, serialized against the victim's own `pick` reset above.
        // `notify_one` stores a permit if the victim is not parked yet, so the
        // signal is never lost.
        if let Some(handle) = self.cancel.get(victim) {
            handle.flag.store(true, Ordering::Release);
            handle.notify.notify_one();
        }

        *self.slot_mut(i)? = Some((half.fetch_start(), half.fetch_len()));
        Ok(Some(half))
    }

    /// Release worker `i`'s lane once its `fill_gap` returns, so a peer's steal
    /// computation stops counting the finished range and this source can be
    /// re-picked for more work.
    ///
    /// # Errors
    ///
    /// An out-of-range worker index (see [`Work::slot_mut`]).
    fn clear(&mut self, i: usize) -> anyhow::Result<()> {
        *self.slot_mut(i)? = None;
        Ok(())
    }

    /// Mark worker `i` as permanently gone — it will never call `pick` again,
    /// whether it simply ran out of coverable work or it faulted on something
    /// ELSE and dropped out mid-fetch — and drop any `pending` entry no other
    /// still-alive worker's `coverage` includes.
    ///
    /// Coverage partitions the source set (#1506), so a `pending` entry can
    /// have exactly one, a few, or NO covering worker left once one exits.
    /// Without this cleanup an item whose sole remaining coverer just retired
    /// would sit in `pending` forever: [`Work::all_idle`] never sees it
    /// resolved (nothing left alive can ever pop it) or the set fall idle
    /// (dropping it is the only way `pending` empties), so every other
    /// worker — even ones with nothing to do with this item — parks on
    /// [`Notify`] permanently. That breaks the "no worker hangs" contract
    /// [`multi_source_fetch`] documents. Dropping the orphaned entry here
    /// instead lets the fetch converge to `all_idle`; the bytes it covered
    /// surface honestly through the ordinary residual-missing check at the
    /// end of [`multi_source_fetch`], the same path an originally uncovered
    /// block takes.
    fn retire(&mut self, i: usize, coverage: &[Coverage], total_bytes: u64) {
        if let Some(a) = self.alive.get_mut(i) {
            *a = false;
        }
        let alive = self.alive.clone();
        self.pending.retain(|seg| {
            alive.iter().enumerate().any(|(j, &is_alive)| {
                is_alive
                    && coverage.get(j).is_some_and(|c| {
                        covers_byte_range(c, seg.fetch_start(), seg.fetch_len(), total_bytes)
                    })
            })
        });
    }
}

/// Re-queue worker `i`'s still-missing tail so another worker (or, after a
/// steal, this one) covers it. Takes the assignment out of `in_flight` FIRST,
/// under the lock, so no peer can steal it while the remainder is computed
/// off-lock; then pushes only the bytes `missing_ranges` still
/// reports missing — verified bytes already stored are excluded, so nothing is
/// refetched.
async fn requeue_missing<St>(store: &St, work: &AsyncMutex<Work>, i: usize) -> anyhow::Result<()>
where
    St: IngestStore,
{
    let assigned = {
        let mut w = work.lock().await;
        w.in_flight.get_mut(i).and_then(Option::take)
    };
    let Some((start, len)) = assigned else {
        return Ok(());
    };
    let total_bytes = store.total_bytes();
    let missing = store.missing_ranges(start, len).await?;
    let remainder = contiguous_byte_ranges(&missing, total_bytes);
    if remainder.is_empty() {
        return Ok(());
    }
    let mut w = work.lock().await;
    for (s, l) in remainder {
        w.pending.push_back(align_range(s, l, total_bytes)?);
    }
    Ok(())
}

/// One worker future per source: loop picking a range and driving `fill_gap`
/// over it, under a cancel/stall [`tokio::select!`], until the fan-out has no
/// work left or the source is dropped. Exactly one outstanding range at a time
/// (the loop drives one `fill_gap` to a terminal outcome before the next pick) —
/// the one-unit-per-source lane invariant, structurally.
///
/// A worker that cannot pick PARKS on `progress` rather than retiring. Nothing
/// to pick is the routine end-of-fetch shape — every in-flight range is below
/// [`crate::segment::MIN_SPLIT_SIZE`], so no split is worth a fresh stream — and
/// a worker that exited there is gone when a peer faults moments later and
/// re-queues its remainder, stranding recoverable work at healthy, already-paid
/// lanes. It exits only once [`Work::all_idle`] holds, or once IT faults.
#[allow(clippy::too_many_arguments)]
// One pick -> drive -> classify loop. The fault classification and the three
// unit outcomes each justify a money-relevant decision against the loop state
// they act on; splitting them out would separate the two.
#[allow(clippy::too_many_lines)]
async fn run_worker<St, S, P, F>(
    i: usize,
    store: &St,
    source: &S,
    pacer: &P,
    funder: &F,
    ctx: &Arc<Mutex<PoolContext>>,
    ledger: &Arc<PoolLedger>,
    hash: [u8; 32],
    total_bytes: u64,
    drive: &DriveConfig,
    work: &AsyncMutex<Work>,
    progress_wake: &Notify,
    faults: &Mutex<Vec<LaneFault>>,
    on_progress: Option<&ProgressCallback>,
    progress_agg: &AtomicU64,
    unit_deadline: Duration,
    pool: &SharedPool<'_>,
    lane_coverage: &[Coverage],
) -> anyhow::Result<()>
where
    St: IngestStore,
    S: BlobSource,
    P: Pacer,
    F: Funder,
{
    // This worker's own coverage — the predicate `Work::pick` filters both its
    // pending-pop and its steal candidates against (#1506).
    let Some(my_coverage) = lane_coverage.get(i) else {
        anyhow::bail!("worker index {i} out of range for lane coverage");
    };
    // This worker's cancel handle, cloned once so `cancelled` can await it
    // OUTSIDE the `Work` lock while a peer's `pick` signals it under the lock.
    let handle = {
        let w = work.lock().await;
        match w.cancel.get(i).map(Arc::clone) {
            Some(h) => h,
            None => anyhow::bail!("worker index {i} out of range for cancel handles"),
        }
    };
    // This lane's payee, read once for fault attribution.
    let provider = ctx.lock().ok().map(|c| c.provider);
    // Per-worker resume/quote state. The reactive-top-up budget is NOT in here —
    // it is a property of the one shared pool and lives in `pool`.
    let mut counters = DriveCounters::new();
    // Wake every parked peer: this worker changed the work state.
    let wake = || progress_wake.notify_waiters();
    loop {
        // Register for the peer-progress wakeup BEFORE reading the work state, so
        // a peer that changes it between this read and the park below cannot slip
        // between the two and leave this worker asleep on work it could take.
        let parked = progress_wake.notified();
        tokio::pin!(parked);
        parked.as_mut().enable();

        // Tiny critical section: pick a range, then DROP the guard before the
        // `fill_gap` await (the guard does not cross the await point).
        let picked = {
            let mut w = work.lock().await;
            w.pick(i, total_bytes, my_coverage)?
        };
        let Some(range) = picked else {
            // Nothing to start right now. Exit only when no peer holds anything
            // and nothing is queued; otherwise park — a peer's range is still
            // draining toward a requeue or a splittable size.
            if work.lock().await.all_idle() {
                break;
            }
            parked.await;
            continue;
        };
        let (r_start, r_len) = (range.fetch_start(), range.fetch_len());

        // Present-bytes backstop (scheduling-independent invariant: "never fetch
        // or pay for bytes already present"). Between a peer's `fill_gap`
        // returning `Ok` and its `clear(i)`, that peer's `in_flight` still
        // advertises its just-COMPLETED, already-PAID range as steal-eligible; on
        // a multi-thread runtime this worker can `pick`/steal it in that window.
        // Re-deriving the still-missing sub-ranges OFF-lock and driving ONLY
        // those closes that window structurally — a stolen already-present range
        // yields an empty set and is skipped, so `fill_gap` (which resumes from
        // its paid frontier and would re-pull the whole span) never re-pays for a
        // present byte. Interior holes never arise — a picked range is contiguous
        // and delivered front-to-back — so this is normally one suffix gap or
        // (for a stolen completed range) none.
        let gaps =
            contiguous_byte_ranges(&store.missing_ranges(r_start, r_len).await?, total_bytes);
        if gaps.is_empty() {
            work.lock().await.clear(i)?;
            wake();
            continue;
        }

        // Drive each still-missing gap OUTSIDE the lock, racing it against a steal
        // cancel and the stall watchdog. Dropping the `fill_gap` future on either
        // leaves the store's checkpointed prefix intact. `in_flight[i]` stays the
        // whole picked range so a peer's steal-trim and this worker's
        // `requeue_missing` (which recomputes the whole range's remainder) agree.
        let mut terminal: Option<UnitOutcome> = None;
        for (g_start, g_len) in gaps {
            let outcome = {
                let fill = fill_gap(
                    store,
                    source,
                    pacer,
                    funder,
                    ctx,
                    ledger,
                    hash,
                    g_start,
                    g_len,
                    total_bytes,
                    drive,
                    &mut counters,
                    on_progress,
                    // The shared whole-blob delivered counter: every lane folds its
                    // own leg deltas in, so the bar reads one monotonic position.
                    Some(progress_agg),
                    None,
                    None,
                    // Everything this lane must not treat as its own: the
                    // aggregate spend the deposit gate subtracts, the fetch-wide
                    // top-up budget, and the credit path that shows a landed
                    // top-up to EVERY lane.
                    Some(pool),
                );
                tokio::select! {
                    biased;
                    res = fill => match res {
                        Ok(()) => UnitOutcome::Completed,
                        // Classify the fault. Two conditions abort the whole fetch
                        // with THAT typed error rather than reassigning the range:
                        //
                        // - The SHARED classifier rules it terminal — a
                        //   payment-layer voucher rejection, an origin blacklist, or
                        //   an over-cap blob — which no provider or lane can fix.
                        // - It is a shared-pool exhaustion ([`PoolExhausted`], the
                        //   pacer's `Refuse`). This is terminal ONLY here, not in
                        //   the shared classifier: single-source failover tries a
                        //   cheaper provider on a budget refusal, but every lane of
                        //   THIS scheduler draws the ONE pool, so reassigning cannot
                        //   fund it. Keeping this test in the scheduler preserves
                        //   single-source failover-on-refusal (#1174).
                        //
                        // `try_join_all` cancels the peer workers, so the failed
                        // source's range is NOT reassigned. A RETRYABLE fault (a
                        // stall, a transport reset, a node-specific refusal, or a
                        // single-source-style budget refusal) faults this one
                        // source: its remainder is re-queued for a DIFFERENT lane,
                        // and the error is KEPT so a fetch that runs out of lanes
                        // can say what each one did.
                        Err(e) => {
                            let terminal = retry_disposition(&e) == RetryDisposition::Terminal
                                || e.downcast_ref::<PoolExhausted>().is_some();
                            if terminal {
                                return Err(e);
                            }
                            UnitOutcome::Faulted(Some(e))
                        }
                    },
                    () = cancelled(&handle) => UnitOutcome::Cancelled,
                    // A watchdog trip carries no error by construction — the
                    // source simply stopped making verified progress.
                    () = watchdog(store, g_start, g_len, unit_deadline) => {
                        UnitOutcome::Faulted(None)
                    }
                }
            };
            match outcome {
                UnitOutcome::Completed => {}
                other => {
                    terminal = Some(other);
                    break;
                }
            }
        }

        match terminal {
            // Every gap filled: free the lane and pick again.
            None => {
                work.lock().await.clear(i)?;
                wake();
            }
            // Stolen: re-queue the trimmed remainder and stay live. The
            // credit-window tail [paid_frontier, checkpointed_frontier) is NOT
            // re-billed here — resume is checkpoint-frontier via `missing_ranges`,
            // a bounded (<= credit window + one 4 MiB INGEST_CHECKPOINT_BYTES),
            // client-favorable gap identical to the existing single-source
            // cross-invocation resume, deliberately NOT the single-source
            // same-leg re-bill contract.
            Some(UnitOutcome::Cancelled) => {
                requeue_missing(store, work, i).await?;
                wake();
            }
            // Stalled/faulted: record why, re-queue for a different source, then
            // stop taking work.
            Some(UnitOutcome::Faulted(err)) => {
                if let Ok(mut f) = faults.lock() {
                    f.push(LaneFault { provider, err });
                }
                requeue_missing(store, work, i).await?;
                wake();
                break;
            }
            Some(UnitOutcome::Completed) => {}
        }
    }
    // This worker is leaving the set: retire it BEFORE the final wake, so a
    // peer that re-checks `pick`/`all_idle` on that wake sees both a set this
    // worker is no longer part of AND any `pending` entry only this worker
    // could have covered already dropped (`Work::retire`) — the coverage-aware
    // counterpart of "someone else still holds work".
    work.lock().await.retire(i, lane_coverage, total_bytes);
    wake();
    Ok(())
}

/// Fetch `hash`'s request `[offset, offset+len)` by fanning it out across
/// `lanes`, all writing into the one shared `store` (spec §5.3). Splits the
/// gap-set into bao-aligned segments, drives one worker per lane, and lets a
/// freed lane steal the tail of the largest range still in flight.
///
/// # Per-source payment (ADR 039 § Payment)
///
/// Each [`SourceLane`] pays with its OWN `(ctx, ledger)`: a voucher is scoped to
/// one on-chain provider and one `(signer, provider)` watermark, so lane `i`'s
/// worker signs against `lanes[i].ctx.provider` and advances `lanes[i].ledger`
/// alone. Two lanes on the SAME provider would be two concurrent voucher streams
/// on one `(signer, provider)` watermark — the hazard the one-unit-per-source
/// rule exists to prevent — so the set is checked for duplicate providers here,
/// at the boundary, rather than assumed from the caller's admission policy.
///
/// One shared pool DEPOSIT backs the whole set, and every lane draws through one
/// shared view of it. `ledgers` picks which view: `None` subtracts
/// `Σ lanes[j].ledger.committed()` over just this fetch's own lanes (a solo
/// `decdn fetch`); `Some(reg)` subtracts `reg.total_committed()`, the sum over
/// every lane a `bundle pull` run has registered — spanning this fetch's
/// concurrent siblings on the same deposit, not just its own lanes. Either way
/// the reactive-top-up budget is counted once for the fetch rather than once per
/// lane, and a landed top-up is credited to every lane the view covers. The gate
/// is evaluated at each `fill_gap` leg boundary; the hard backstop against a
/// node redeeming past the deposit stays on-chain (the pool pays first-come up
/// to its deposit), exactly as on the single-source path.
///
/// Returns once every gap is filled, or once a worker hits a TERMINAL fault (a
/// retryable one only drops that lane). Finalization is the caller's job —
/// unlike [`crate::drive`], which promotes a complete blob itself, this only
/// flushes the present record (spec §5.5 single-writer flush point) once all
/// workers finish.
///
/// # Errors
///
/// An empty `lanes` or two lanes on one provider; a TERMINAL `fill_gap` fault
/// propagated verbatim from a worker — either a shared-classifier terminal
/// ([`crate::retry_disposition`]: a payment-layer rejection, an origin blacklist,
/// or an over-cap blob) or a shared-pool exhaustion ([`crate::PoolExhausted`],
/// terminal only for this scheduler); a segmentation alignment error; an I/O
/// failure flushing the present record; or, if every source drops on RETRYABLE
/// faults with the request still incomplete, an error naming what each lane did,
/// wrapping the last real one so a caller can still downcast it.
#[allow(clippy::too_many_arguments)]
// Linear set-up (precondition check, segmentation, the shared-pool view) then
// one drive and one failure report. Flat, not complex.
#[allow(clippy::too_many_lines)]
pub async fn multi_source_fetch<St, S, P, F>(
    store: &St,
    lanes: &[SourceLane<'_, S>],
    pacer: &P,
    funder: &F,
    hash: [u8; 32],
    offset: u64,
    len: u64,
    drive: &DriveConfig,
    ms: &MultiSourceConfig,
    on_progress: Option<&ProgressCallback>,
    ledgers: Option<&LaneLedgers>,
) -> anyhow::Result<()>
where
    St: IngestStore,
    S: BlobSource,
    P: Pacer,
    F: Funder,
{
    if lanes.is_empty() {
        anyhow::bail!("multi_source_fetch requires at least one source lane");
    }
    // Defensively enforce `max_sources`. The caller's admission
    // (`admit_sources`, ADR 001) already caps the set to `max_sources` before
    // building lanes, and `lanes` arrives in rank order — so this is a no-op on
    // the normal path, but it keeps the config knob authoritative if a caller
    // ever passes an un-capped lane set, keeping the highest-ranked lanes.
    let lanes = lanes
        .get(..lanes.len().min(ms.max_sources.max(1)))
        .unwrap_or(lanes);
    // The premise the whole payment model rests on, checked rather than trusted:
    // one lane per on-chain provider. Two lanes sharing a provider share a
    // `(signer, provider)` watermark, and their concurrent voucher streams
    // regress each other — the failure the caller's operator-spread admission is
    // supposed to make impossible.
    let mut seen_providers = HashSet::with_capacity(lanes.len());
    for lane in lanes {
        let provider = lane
            .ctx
            .lock()
            .map_err(|_| anyhow::anyhow!("channel context lock poisoned"))?
            .provider;
        if !seen_providers.insert(provider) {
            anyhow::bail!(
                "multi_source_fetch requires one lane per provider: {provider} appears twice, \
                 which would run two concurrent voucher streams on one (signer, provider) lane"
            );
        }
    }
    let total_bytes = store.total_bytes();

    let missing = store.missing_ranges(offset, len).await?;
    let gaps = contiguous_byte_ranges(&missing, total_bytes);
    if gaps.is_empty() {
        // Already fully held: persist the present record and return (the caller
        // finalizes).
        store.flush_present_record()?;
        return Ok(());
    }

    // Coverage-aware spread (client planner, #1506): every admitted lane's
    // `Probed` coverage (B1) becomes its `SourceCoverage`; `rank` is simply
    // lane order, since `lanes` already arrives in the caller's admission /
    // selection-score order (`admit_sources` — ADR 001) with no re-ranking
    // done here. `spread_segments` assigns each `gap`-intersecting discovery
    // block to exactly one covering lane, rarest-cover-first, so every lane
    // that covers anything starts with coverable work.
    let lane_coverage: Vec<Coverage> = lanes.iter().map(|l| l.coverage.clone()).collect();
    let sources: Vec<SourceCoverage> = lane_coverage
        .iter()
        .enumerate()
        .map(|(source_ix, coverage)| SourceCoverage {
            source_ix,
            coverage: coverage.clone(),
        })
        .collect();
    let rank: Vec<usize> = (0..lanes.len()).collect();
    let (runs, _uncovered) = spread_segments(&missing, total_bytes, &sources, &rank);
    // `_uncovered` needs no bespoke handling here: a discovery block none of
    // `lanes` covers is simply never queued into `pending`, so it stays in
    // `missing_ranges` for the whole fetch and surfaces through the ordinary
    // residual-missing "all sources failed" check below — the same "not
    // available from this source set" outcome an orphaned mid-fetch range
    // takes via `Work::retire`.
    // Seed `pending` from the contiguous, gap-clamped runs, splitting further
    // ONLY to reach otherwise-idle lanes (#1506). The eager fan-out is a GLOBAL
    // budget — at most one contiguous span per lane — not a per-run multiplier: a
    // large multi-block gap already plans ~one run per source, so it seeds ~N spans
    // and never `blocks × N`. A gap that planned FEWER runs than lanes (a small
    // gap, or a resume tail, where several full holders tied and the planner could
    // pick only one per block) is split largest-first until every lane has a span,
    // or no run can usefully split any further. A run is split at most as many ways
    // as lanes hold it WHOLE, so every piece stays inside its holders' coverage and
    // `Work::pick`'s filter never has to refuse it; `split_evenly` keeps each piece
    // chunk-group aligned (never sub-group), which is the only floor the eager split
    // needs — it deliberately splits below `steal_split`'s `MIN_SPLIT_SIZE` so a
    // small blob no bigger than one block still engages every full holder from the
    // start.
    let mut seeds: Vec<RunSeed> = runs
        .iter()
        .map(|run| {
            let holders = lane_coverage
                .iter()
                .filter(|c| covers_byte_range(c, run.offset, run.len, total_bytes))
                .count()
                .max(1);
            RunSeed {
                offset: run.offset,
                len: run.len,
                max_pieces: holders,
                pieces: 1,
            }
        })
        .collect();
    let mut spare = lanes.len().saturating_sub(seeds.len());
    while spare > 0 {
        // The still-splittable run whose next split yields the largest piece.
        let Some(seed) = seeds
            .iter_mut()
            .filter(|s| s.pieces < s.max_pieces)
            .max_by_key(|s| s.len / u64::try_from(s.pieces + 1).unwrap_or(u64::MAX))
        else {
            break;
        };
        seed.pieces += 1;
        spare -= 1;
    }
    let mut pending: VecDeque<AlignedRange> = VecDeque::with_capacity(seeds.len());
    for seed in &seeds {
        for seg in split_evenly(seed.offset, seed.len, seed.pieces, total_bytes)? {
            pending.push_back(seg);
        }
    }

    let work = AsyncMutex::new(Work {
        pending,
        in_flight: vec![None; lanes.len()],
        cancel: (0..lanes.len())
            .map(|_| Arc::new(CancelHandle::new()))
            .collect(),
        alive: vec![true; lanes.len()],
    });
    // Wakes workers parked because nothing was pickable, whenever a peer frees,
    // re-queues, or leaves the set.
    let progress_wake = Notify::new();
    // Per-lane reasons a lane stopped, so a fetch that runs out of lanes reports
    // what each one did instead of a contentless count of unfetched bytes.
    let faults: Mutex<Vec<LaneFault>> = Mutex::new(Vec::new());

    // The three facts that belong to the POOL and not to any lane (see
    // [`SharedPool`]). Cloning the `Arc` handles keeps the closures free of the
    // borrow on `lanes` and lets every worker share one view.
    //
    // With a run registry (`ledgers: Some`), `spent`/`credit` read and write
    // EVERY lane the run has registered — spanning this fetch's concurrent
    // siblings on the same deposit, the pool-wide view a `bundle pull` run's
    // deposit gate needs. Without one (`ledgers: None`), they fold over only
    // this fetch's own lanes, exactly as a solo `decdn fetch` always has.
    let lane_ledgers: Vec<Arc<PoolLedger>> = lanes.iter().map(|l| Arc::clone(&l.ledger)).collect();
    let lane_ctxs: Vec<Arc<Mutex<PoolContext>>> =
        lanes.iter().map(|l| Arc::clone(&l.ctx)).collect();
    let spent: Box<dyn Fn() -> U256 + Send + Sync> = match ledgers {
        Some(reg) => Box::new(move || reg.total_committed()),
        None => Box::new(move || {
            lane_ledgers
                .iter()
                .map(|l| l.committed().amount)
                .fold(U256::ZERO, U256::saturating_add)
        }),
    };
    let credit: Box<dyn Fn(U256) -> anyhow::Result<()> + Send + Sync> = match ledgers {
        Some(reg) => Box::new(move |new_deposit| {
            reg.credit_all(new_deposit);
            Ok(())
        }),
        None => Box::new(move |new_deposit: U256| -> anyhow::Result<()> {
            for ctx in &lane_ctxs {
                ctx.lock()
                    .map_err(|_| anyhow::anyhow!("channel context lock poisoned"))?
                    .deposit = new_deposit;
            }
            Ok(())
        }),
    };
    let topups_used = AtomicU32::new(0);
    let pool = SharedPool {
        spent: &*spent,
        topups_used: &topups_used,
        credit: &*credit,
    };

    // ONE monotonic whole-blob delivered-byte counter behind the progress bar,
    // shared by every lane. Seeded with the bytes already present so a resumed
    // fetch's bar starts where the last run left off, then each lane folds in its
    // own leg deltas. Without this each lane reported its own `base_present +
    // received`, so the bar jumped between lanes and the smoothed rate ramped
    // without bound (the local absolute positions diverge and are non-monotonic
    // when interleaved).
    let base_present = ranges_content_len(&store.present_ranges().await?, total_bytes);
    let progress_agg = AtomicU64::new(base_present);

    // Surface the resume base on the bar immediately, before any lane opens a
    // channel. The aggregator is already seeded to `base_present` and each lane
    // folds only its leg DELTAS on top, so emitting the seed here DISPLAYS that
    // starting value — it is not a fold and cannot double-count. Without it the
    // bar sits at `0` through discovery / channel open / pool resolve, then jumps
    // to the resume point on the first delivered chunk.
    if let Some(cb) = on_progress {
        cb(base_present, total_bytes);
    }

    let workers = lanes.iter().enumerate().map(|(i, lane)| {
        run_worker(
            i,
            store,
            lane.source,
            pacer,
            funder,
            &lane.ctx,
            &lane.ledger,
            hash,
            total_bytes,
            drive,
            &work,
            &progress_wake,
            &faults,
            on_progress,
            &progress_agg,
            ms.unit_deadline,
            &pool,
            &lane_coverage,
        )
    });
    // Drive every worker to completion while a single periodic tick flushes the
    // `.ranges` present record (spec §5.5). The workers are the sole work drivers
    // and this interval owner is the sole periodic flush owner — no worker flushes,
    // preserving the single-writer property. Without it a crash mid-fetch would
    // leave `.ranges` at pre-session state and re-download (and re-pay for) the
    // whole in-flight fan-out on resume; the interval bounds that loss to one
    // `PRESENT_RECORD_FLUSH_INTERVAL`.
    let workers = futures_util::future::try_join_all(workers);
    let outcome = drive_with_interval_flush(store, PRESENT_RECORD_FLUSH_INTERVAL, async move {
        workers.await?;
        Ok(())
    })
    .await;

    // Single-writer flush point (spec §5.5): every worker has finished, so the
    // in-memory present set is final — persist the `.ranges` record once more,
    // off the per-checkpoint hot path. This runs on the FAILURE path too: the
    // bytes the fan-out did deliver are paid for, and dropping the record here
    // makes the next invocation re-fetch and re-pay for them.
    let flushed = store.flush_present_record();
    outcome?;
    flushed?;

    // No worker hangs: they either fill their ranges or drop. If every source
    // dropped with the request still incomplete, surface it as an error rather
    // than returning a false success (or hanging) — naming each lane and keeping
    // the last real error as the cause, so a caller's `downcast_ref` still
    // reaches it (an `UpstreamRefused(NotFound)` here is what the CLI turns into
    // the two-cause cache-miss diagnosis).
    let unfetched = contiguous_byte_ranges(&store.missing_ranges(offset, len).await?, total_bytes);
    if !unfetched.is_empty() {
        let bytes: u64 = unfetched
            .iter()
            .map(|(_, l)| *l)
            .fold(0, u64::saturating_add);
        let mut recorded = match faults.lock() {
            Ok(mut f) => std::mem::take(&mut *f),
            Err(_) => Vec::new(),
        };
        let detail = recorded
            .iter()
            .map(LaneFault::describe)
            .collect::<Vec<_>>()
            .join("; ");
        let summary = if detail.is_empty() {
            format!("all sources failed; {bytes} bytes unfetched")
        } else {
            format!("all sources failed; {bytes} bytes unfetched: {detail}")
        };
        // Carry the last real error as the cause. A watchdog stall has none, and
        // "every source stalled" is a different and more actionable statement
        // than "the nodes refused" — so a stall-only set stays a bare summary.
        let cause = recorded
            .iter()
            .rposition(|f| f.err.is_some())
            .and_then(|i| {
                if i < recorded.len() {
                    recorded.swap_remove(i).err
                } else {
                    None
                }
            });
        return match cause {
            Some(e) => Err(e.context(summary)),
            None => Err(anyhow::anyhow!(summary)),
        };
    }
    Ok(())
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
    use std::time::Duration;

    use alloy::primitives::{Address, B256, U256};
    use alloy::signers::local::PrivateKeySigner;
    use decdn_incentive::{DepositOutcome, LaneKey};
    use decdn_protocol::{Coverage, DISCOVERY_BLOCK_BYTES, num_blocks};

    use super::{MultiSourceConfig, SourceLane, multi_source_fetch};
    use crate::driver::DriveConfig;
    use crate::driver::PoolExhausted;
    use crate::ledgers::{LaneHandle, LaneLedgers};
    use crate::pacer::{BudgetPacer, PaceDecision, PaceState, Pacer};
    use crate::source::{FakeFunder, ScriptedSource};
    use crate::{ClientRangedStore, Cumulative, PoolContext, PoolLedger};
    use decdn_bao_range::RangedStore;

    /// A deterministic blob of `len` bytes — same synth as the `source` unit
    /// tests, so a `ScriptedSource` over it yields verifiable wire.
    fn blob(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    /// A healthy buyer context paying `provider`, with `deposit` on the pool.
    fn ctx_with(provider: u8, deposit: U256) -> PoolContext {
        let signer = PrivateKeySigner::random();
        PoolContext {
            pool_id: B256::ZERO,
            provider: Address::repeat_byte(provider),
            deposit,
            client_signer: Arc::new(signer),
            voucher_domain: decdn_incentive::bind_node_id_domain(1, Address::ZERO),
            prior_bytes_delivered: U256::ZERO,
            prior_amount: U256::ZERO,
            client_binding: None,
            capability: None,
        }
    }

    /// A shared handle to a healthy buyer context (huge deposit so the pacer never
    /// tops up) paying the given `provider`.
    fn ctx_for(provider: u8) -> Arc<Mutex<PoolContext>> {
        Arc::new(Mutex::new(ctx_with(provider, U256::from(u128::MAX))))
    }

    /// Build one paid lane: a `ScriptedSource` over `data` paying a fresh ledger,
    /// with a context pinned to `provider`. Returns the source, its ledger, and a
    /// closure that turns a borrow of the source into a [`SourceLane`] — the
    /// source must outlive the lane, so the caller owns it.
    ///
    /// Coverage defaults to the WHOLE blob, so a test built with this helper gets a
    /// full holder without spelling out coverage at each call site. Tests exercising
    /// partial coverage use [`lane_with_coverage`] instead.
    fn lane(
        source: &ScriptedSource,
        ledger: Arc<PoolLedger>,
        provider: u8,
    ) -> SourceLane<'_, ScriptedSource> {
        let full = Coverage::full(num_blocks(source.total_bytes()));
        lane_with_coverage(source, ledger, provider, full)
    }

    /// Like [`lane`], but with an explicit [`Coverage`] rather than the
    /// whole-blob default — for tests exercising partial holders (#1506).
    fn lane_with_coverage(
        source: &ScriptedSource,
        ledger: Arc<PoolLedger>,
        provider: u8,
        coverage: Coverage,
    ) -> SourceLane<'_, ScriptedSource> {
        SourceLane {
            source,
            ctx: ctx_for(provider),
            ledger,
            coverage,
        }
    }

    /// A `.partial` store whose tempdir path is returned so the test can read
    /// the finalized blob back off disk.
    fn fresh_store(root: [u8; 32], total: u64) -> (ClientRangedStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tmp dir");
        let store = ClientRangedStore::create(dir.path(), "b", root, total).expect("create");
        (store, dir)
    }

    #[tokio::test]
    async fn two_sources_fetch_large_blob_byte_identical() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        // Each source pays its OWN lane (its own provider + ledger); one shared
        // pool deposit backs both. Both hold the whole blob.
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let lanes = vec![
            lane(&src_a, Arc::clone(&ledger_a), 0xA1),
            lane(&src_b, Arc::clone(&ledger_b), 0xB2),
        ];
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 4,
                unit_deadline: Duration::from_secs(10),
            },
            None,
            None,
        )
        .await?;

        store.finalize().await?;

        // Whole blob present and byte-identical.
        let got = std::fs::read(dir.path().join("b"))?;
        assert_eq!(got.len(), data.len(), "assembled length matches");
        assert_eq!(got, data, "assembled bytes byte-identical to source blob");

        // Both sources actually contributed (work was parallelized, not all
        // from one).
        assert!(
            src_a.opened_bytes() > 0 && src_b.opened_bytes() > 0,
            "both sources must have served work: a={} b={}",
            src_a.opened_bytes(),
            src_b.opened_bytes()
        );
        // Coverage: at least the whole blob's bytes were opened across the set
        // (a tail-steal boundary may cause a bounded, idempotent re-fetch).
        assert!(
            src_a.opened_bytes() + src_b.opened_bytes() >= data.len() as u64,
            "the two sources together must cover the whole blob"
        );
        Ok(())
    }

    /// The delivery progress the bar reads is ONE monotonic whole-blob position,
    /// not each lane's divergent local `base_present + received`. Two concurrent
    /// full holders each split the blob and report through the SAME callback; the
    /// callback records every position it is handed. A progress bar must never go
    /// backwards, so the recorded sequence must be non-decreasing and end at the
    /// whole-blob size — before the aggregator fix each lane reported its own
    /// lane-local absolute position, so the sequence jumped between lanes and the
    /// smoothed rate ramped without bound.
    #[tokio::test]
    async fn progress_positions_are_monotonic_across_lanes() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, _dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let samples: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let cb_samples = Arc::clone(&samples);
        let on_progress: Box<super::ProgressCallback> = Box::new(move |received, expected| {
            if let Ok(mut s) = cb_samples.lock() {
                s.push((received, expected));
            }
        });

        let lanes = vec![
            lane(&src_a, Arc::clone(&ledger_a), 0xA1),
            lane(&src_b, Arc::clone(&ledger_b), 0xB2),
        ];
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 4,
                unit_deadline: Duration::from_secs(10),
            },
            Some(&on_progress),
            None,
        )
        .await?;

        let samples = samples.lock().expect("samples lock").clone();
        assert!(
            !samples.is_empty(),
            "progress callback must fire at least once"
        );
        // The bar can never move backwards: every reported position is >= the one
        // before it, against a stable whole-blob total.
        let mut prev = 0u64;
        for (received, expected) in &samples {
            assert_eq!(
                *expected, total,
                "the progress total must be the whole-blob size"
            );
            assert!(
                *received >= prev,
                "progress regressed: {received} after {prev} — the bar jumped backwards"
            );
            assert!(
                *received <= total,
                "progress overshot the blob size: {received} > {total}"
            );
            prev = *received;
        }
        // And it reaches the whole blob by the end.
        assert_eq!(
            prev, total,
            "the final reported position must reach the blob size"
        );
        Ok(())
    }

    /// A resumed multi-source fetch surfaces the already-present base on the bar
    /// BEFORE any lane opens a channel: the first reported position is the held
    /// prefix's content length, not `0`. The aggregator is seeded to
    /// `base_present`, but nothing emits it until the first delivered chunk, so
    /// without the pre-stream emit a resumed blob's bar sits at `0` through
    /// discovery / channel open / pool resolve, then jumps to the resume point.
    #[tokio::test]
    async fn resume_base_is_reported_before_the_first_chunk() -> anyhow::Result<()> {
        use crate::driver::ranges_content_len;
        use crate::source::BlobSource;
        use decdn_bao_range::{CHUNK_GROUP_BYTES, align_range};

        let total = 8 * CHUNK_GROUP_BYTES;
        let data = blob(total as usize);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, _dir) = fresh_store(root, total);

        // Seed a two-group prefix through the real verified ingest path, using a
        // throwaway source so the lane sources' opened-byte logs stay clean.
        let held = align_range(0, 2 * CHUNK_GROUP_BYTES, total).expect("align held");
        let seed = ScriptedSource::new(data.clone())?;
        let (_h, reader) = seed.open(root, held.clone()).await?;
        store.ingest_stream(&held, reader, None).await?;
        let base_present = ranges_content_len(&store.present_ranges().await?, total);
        assert_eq!(
            base_present,
            2 * CHUNK_GROUP_BYTES,
            "scenario: two groups held"
        );

        let samples: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let cb_samples = Arc::clone(&samples);
        let on_progress: Box<super::ProgressCallback> = Box::new(move |received, expected| {
            if let Ok(mut s) = cb_samples.lock() {
                s.push((received, expected));
            }
        });

        let lanes = vec![
            lane(&src_a, Arc::clone(&ledger_a), 0xA1),
            lane(&src_b, Arc::clone(&ledger_b), 0xB2),
        ];
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 4,
                unit_deadline: Duration::from_secs(10),
            },
            Some(&on_progress),
            None,
        )
        .await?;

        let samples = samples.lock().expect("samples lock").clone();
        let first = *samples.first().expect("at least one progress sample");
        assert_eq!(
            first,
            (base_present, total),
            "the first reported position must be the resume base, emitted before \
             any lane opens a channel"
        );
        Ok(())
    }

    /// Fan-out geometry (#1506): a large multi-block gap across N full holders
    /// seeds ~N contiguous spans — one per holder — never `blocks × N`. Before the
    /// fix the client planner assigned one run per block and the scheduler split
    /// each run by holder count, opening `blocks × N` streams (here 3 × 3 = 9) for a
    /// blob that needs only N. Every holder still contributes.
    #[tokio::test]
    async fn large_multi_block_gap_seeds_about_one_span_per_holder() -> anyhow::Result<()> {
        let total = 3 * DISCOVERY_BLOCK_BYTES;
        let data = blob(total as usize);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_c = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let src_c = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_c));
        let root = src_a.root();
        let (store, dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();
        let lanes = vec![
            lane(&src_a, Arc::clone(&ledger_a), 0xA1),
            lane(&src_b, Arc::clone(&ledger_b), 0xB2),
            lane(&src_c, Arc::clone(&ledger_c), 0xC3),
        ];
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 4,
                unit_deadline: Duration::from_secs(10),
            },
            None,
            None,
        )
        .await?;
        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical across the fan-out"
        );
        let opens =
            src_a.opened_ranges().len() + src_b.opened_ranges().len() + src_c.opened_ranges().len();
        // ~N = 3 spans, one per holder. The old `blocks × N` seeding opened 9. Allow
        // a little slack for an opportunistic tail-steal, but stay well under 9.
        assert!(
            opens <= 6,
            "fan-out seeds ~N contiguous spans, not blocks × N: {opens} opens across 3 holders"
        );
        assert!(
            src_a.opened_bytes() > 0 && src_b.opened_bytes() > 0 && src_c.opened_bytes() > 0,
            "every holder contributes: a={} b={} c={}",
            src_a.opened_bytes(),
            src_b.opened_bytes(),
            src_c.opened_bytes()
        );
        Ok(())
    }

    /// Build a `Coverage` sized for `n` discovery blocks with exactly `blocks`
    /// covered — the same shorthand `coverage_plan`'s own tests use.
    fn cov(n: u32, blocks: &[u32]) -> Coverage {
        Coverage::from_block_indices(n, blocks.iter().copied())
    }

    /// Disjoint coverage (#1506, task B2): source A holds only discovery block
    /// 0, source B holds only block 1, over a whole-blob fetch spanning exactly
    /// those two blocks. Every byte range A opens must fall inside block 0 and
    /// every range B opens must fall inside block 1 — neither is EVER handed
    /// the other's block, because `spread_segments`'s per-block assignment
    /// (each block has exactly one covering candidate here) and `Work::pick`'s
    /// coverage filter agree on the same routing.
    #[tokio::test]
    async fn disjoint_coverage_routes_each_block_to_its_only_coverer() -> anyhow::Result<()> {
        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let data = blob(total as usize);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let n = num_blocks(total);
        let lanes = vec![
            lane_with_coverage(&src_a, Arc::clone(&ledger_a), 0xA1, cov(n, &[0])),
            lane_with_coverage(&src_b, Arc::clone(&ledger_b), 0xB2, cov(n, &[1])),
        ];
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 4,
                unit_deadline: Duration::from_secs(10),
            },
            None,
            None,
        )
        .await?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical from two disjoint-coverage sources"
        );

        assert!(
            src_a
                .opened_ranges()
                .iter()
                .all(|&(s, l)| s + l <= DISCOVERY_BLOCK_BYTES),
            "source A covers only block 0 and must never be opened past it: {:?}",
            src_a.opened_ranges()
        );
        assert!(
            src_b
                .opened_ranges()
                .iter()
                .all(|&(s, _)| s >= DISCOVERY_BLOCK_BYTES),
            "source B covers only block 1 and must never be opened before it: {:?}",
            src_b.opened_ranges()
        );
        // Each source actually did its own block — this is not a degenerate
        // single-source fetch.
        assert!(src_a.opened_bytes() > 0, "A must have served block 0");
        assert!(src_b.opened_bytes() > 0, "B must have served block 1");
        Ok(())
    }

    /// A third, all-ones holder serves the one block neither of the two
    /// partial holders covers (#1506, task B2). A holds only block 0, B holds
    /// only block 1, and only O (full coverage) can serve block 2 — so O, and
    /// only O, must open bytes in block 2.
    #[tokio::test]
    async fn a_block_only_the_all_ones_source_covers_is_served_by_it() -> anyhow::Result<()> {
        let total = 3 * DISCOVERY_BLOCK_BYTES;
        let data = blob(total as usize);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_o = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let src_o = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_o));
        let root = src_a.root();
        let (store, dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let n = num_blocks(total);
        let lanes = vec![
            lane_with_coverage(&src_a, Arc::clone(&ledger_a), 0xA1, cov(n, &[0])),
            lane_with_coverage(&src_b, Arc::clone(&ledger_b), 0xB2, cov(n, &[1])),
            lane_with_coverage(&src_o, Arc::clone(&ledger_o), 0xC3, Coverage::full(n)),
        ];
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 4,
                unit_deadline: Duration::from_secs(10),
            },
            None,
            None,
        )
        .await?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical with a partial-coverage trio"
        );

        let block2_start = 2 * DISCOVERY_BLOCK_BYTES;
        assert!(
            src_o
                .opened_ranges()
                .iter()
                .any(|&(s, l)| s < total && s + l > block2_start),
            "only O covers block 2, so O must be the one that opened it: {:?}",
            src_o.opened_ranges()
        );
        assert!(
            src_a.opened_ranges().iter().all(|&(s, _)| s < block2_start),
            "A does not cover block 2 and must never open into it: {:?}",
            src_a.opened_ranges()
        );
        assert!(
            src_b.opened_ranges().iter().all(|&(s, _)| s < block2_start),
            "B does not cover block 2 and must never open into it: {:?}",
            src_b.opened_ranges()
        );
        Ok(())
    }

    /// Coverage-constrained steal (#1506, task B2): a fast source that covers
    /// ONLY block 0 finishes its own segment quickly, while the block-1-only
    /// holder is slow to start. With `pending` empty the fast source tries to
    /// steal — but the only range left in flight (block 1) is outside its own
    /// coverage, so `steal_split`'s predicate rejects it and the fast source
    /// PARKS instead of stealing work it cannot serve. The slow source still
    /// finishes block 1 on its own, and the fetch completes.
    #[tokio::test]
    async fn coverage_constrained_steal_parks_instead_of_taking_uncoverable_work()
    -> anyhow::Result<()> {
        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let data = blob(total as usize);
        let ledger_fast = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_slow = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_fast = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_fast));
        // Every leg the slow source opens stalls before its first byte, giving
        // the fast source (block 0 only) time to finish and attempt a steal.
        let src_slow = ScriptedSource::new(data.clone())?
            .slow_to_start(Duration::from_millis(200))
            .paying(Arc::clone(&ledger_slow));
        let root = src_fast.root();
        let (store, dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let n = num_blocks(total);
        let lanes = vec![
            lane_with_coverage(&src_fast, Arc::clone(&ledger_fast), 0xA1, cov(n, &[0])),
            lane_with_coverage(&src_slow, Arc::clone(&ledger_slow), 0xB2, cov(n, &[1])),
        ];
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 2,
                unit_deadline: Duration::from_secs(30),
            },
            None,
            None,
        )
        .await?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical despite the fast source finding nothing to steal"
        );

        assert!(
            src_fast
                .opened_ranges()
                .iter()
                .all(|&(s, l)| s + l <= DISCOVERY_BLOCK_BYTES),
            "the fast source covers only block 0 and must never open past it — it must \
             have parked rather than stolen block 1: {:?}",
            src_fast.opened_ranges()
        );
        assert!(
            src_slow.opened_bytes() > 0,
            "the slow source must still have served its own block 1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn one_source_below_two_holders_still_completes() -> anyhow::Result<()> {
        // With a single source the scheduler degrades to one segment, no steal,
        // and still assembles the whole blob byte-identical.
        let data = blob(20 * 1024 * 1024);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger));
        let root = src.root();
        let total = src.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let lanes = vec![lane(&src, Arc::clone(&ledger), 0xA1)];
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 4,
                unit_deadline: Duration::from_secs(10),
            },
            None,
            None,
        )
        .await?;

        store.finalize().await?;

        let got = std::fs::read(dir.path().join("b"))?;
        assert_eq!(got, data, "single-source assembly is byte-identical");
        // The lone source opened exactly the whole blob (one segment, no steal,
        // no re-fetch).
        assert_eq!(
            src.opened_bytes(),
            data.len() as u64,
            "one source, one segment: exactly the blob's bytes opened once"
        );
        Ok(())
    }

    /// Fault reassignment: `src_a` faults after ~8 MiB of its segment;
    /// `src_b` holds the whole blob and covers the reassigned remainder. The
    /// blob still assembles byte-identical, and the faulted source's verified
    /// prefix is NOT refetched (the remainder alone is reassigned).
    #[tokio::test]
    async fn faulted_source_tail_is_reassigned_and_fetch_completes() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        // src_a returns a retryable `Err` after 8 MiB of wire on any range longer
        // than that; its 32 MiB initial segment therefore delivers only a ~8 MiB
        // prefix then faults. This is the `fill_gap`-error arm, NOT the stall
        // watchdog — a wedged source that never errors is `stall_after`, covered
        // separately. src_b is healthy.
        let src_a = ScriptedSource::new(data.clone())?
            .with_fault_after(8 * 1024 * 1024, || anyhow::anyhow!("scripted fault"))
            .paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let lanes = vec![
            lane(&src_a, Arc::clone(&ledger_a), 0xA1),
            lane(&src_b, Arc::clone(&ledger_b), 0xB2),
        ];
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 4,
                unit_deadline: Duration::from_secs(10),
            },
            None,
            None,
        )
        .await?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical after reassignment"
        );

        // src_a opened its 32 MiB segment but delivered only its pre-fault prefix.
        assert!(
            src_a.opened_bytes() >= 8 * 1024 * 1024,
            "src_a opened its segment: {}",
            src_a.opened_bytes()
        );
        assert!(
            src_a.delivered_bytes() < 32 * 1024 * 1024,
            "src_a faulted, so it did NOT deliver its whole segment: {}",
            src_a.delivered_bytes()
        );
        assert!(
            src_b.opened_bytes() > 0,
            "src_b covered the reassigned tail"
        );
        // No wholesale refetch: the two together delivered about the blob size,
        // not the blob plus a re-pulled 32 MiB segment.
        let total_delivered = src_a.delivered_bytes() + src_b.delivered_bytes();
        assert!(
            total_delivered <= total + 8 * 1024 * 1024,
            "verified bytes must not be refetched: delivered {total_delivered} for a \
             {total}-byte blob"
        );
        Ok(())
    }

    /// Focused [`Work::retire`] fault coverage (#1506): the anti-hang prune, on
    /// the state directly rather than through a whole fetch.
    ///
    /// The three integration fault tests
    /// (`faulted_source_tail_is_reassigned_and_fetch_completes`,
    /// `all_sources_failing_errors_without_hang`, …) all run every lane over the
    /// SAME whole-blob coverage, so `retire`'s selective prune — drop only the
    /// `pending` entries no *surviving* lane covers, keep the ones a survivor can
    /// still serve — never actually decides anything there: every entry is
    /// coverable by every other lane. This builds the work-state by hand with
    /// DISJOINT coverage so the prune has a real choice, and asserts it makes the
    /// right one. Without the keep half the fetch would refetch nothing but
    /// needlessly, and without the drop half a survivor-uncoverable entry would
    /// sit in `pending` forever and every idle worker would park on it — the hang
    /// [`Work::retire`] exists to prevent.
    ///
    /// No blob is allocated (this pokes `Work` directly), so it stays tiny — the
    /// block boundaries are the production 64 MiB `DISCOVERY_BLOCK_BYTES`, and the
    /// `pending` entries are one-group slices inside two different blocks.
    #[tokio::test]
    async fn retire_drops_only_entries_no_surviving_lane_covers() -> anyhow::Result<()> {
        use std::collections::VecDeque;

        use decdn_bao_range::align_range;

        use super::{CancelHandle, Work};

        // A two-block blob; source 0 holds ONLY block 0, source 1 ONLY block 1.
        let total = 2 * DISCOVERY_BLOCK_BYTES;
        let coverage = vec![cov(2, &[0]), cov(2, &[1])];

        // Two queued, un-started segments: one one-group slice inside block 0
        // (only source 0 covers it) and one inside block 1 (only source 1).
        let in_block0 = align_range(0, 1, total)?;
        let in_block1 = align_range(DISCOVERY_BLOCK_BYTES, 1, total)?;
        assert_eq!(in_block0.fetch_start(), 0);
        assert_eq!(in_block1.fetch_start(), DISCOVERY_BLOCK_BYTES);

        let mut work = Work {
            pending: VecDeque::from(vec![in_block0, in_block1]),
            in_flight: vec![None, None],
            cancel: vec![Arc::new(CancelHandle::new()), Arc::new(CancelHandle::new())],
            alive: vec![true, true],
        };

        // Source 0 faults out. Its block-0 entry has no surviving coverer and must
        // be dropped; the block-1 entry is still served by the alive source 1 and
        // must stay.
        work.retire(0, &coverage, total);
        assert!(!work.alive[0], "retired worker is marked gone");
        assert!(work.alive[1], "the survivor stays alive");
        let starts: Vec<u64> = work
            .pending
            .iter()
            .map(decdn_bao_range::AlignedRange::fetch_start)
            .collect();
        assert_eq!(
            starts,
            vec![DISCOVERY_BLOCK_BYTES],
            "the block-0 entry (orphaned by source 0's exit) is dropped; the block-1 \
             entry a surviving lane still covers is kept"
        );

        // Now the last coverer of block 1 exits too: its entry is orphaned in turn,
        // so the prune empties `pending` — nothing is left for a worker to park on,
        // which is what lets the fetch converge to `all_idle` instead of hanging.
        work.retire(1, &coverage, total);
        assert!(
            work.pending.is_empty(),
            "with no lane left covering block 1, its entry is dropped too: {:?}",
            work.pending
        );
        Ok(())
    }

    /// THE double-pay test: a fast source and an artificially slow one over a
    /// 64 MiB blob, arranged so a steal DEFINITELY fires (the slow source stalls
    /// before its first byte, so the fast source finishes its own segment and
    /// steals the slow source's tail). With steal-cancellation the stolen tail is
    /// fetched by exactly ONE source, so total delivered ≈ the blob size — not
    /// ~1.5–2× it (what a bookkeeping-only steal — trim without cancel — produces).
    #[tokio::test]
    async fn forced_steal_does_not_double_fetch_the_stolen_tail() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let total = data.len() as u64;
        let ledger_fast = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_slow = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_fast = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_fast));
        // Every leg the slow source opens stalls 200 ms before its first byte —
        // long enough that the fast source (only cooperative yields) always
        // finishes first and steals, deterministically forcing the steal path.
        let src_slow = ScriptedSource::new(data.clone())?
            .slow_to_start(Duration::from_millis(200))
            .paying(Arc::clone(&ledger_slow));
        let root = src_fast.root();
        let (store, dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let lanes = vec![
            lane(&src_fast, Arc::clone(&ledger_fast), 0xA1),
            lane(&src_slow, Arc::clone(&ledger_slow), 0xB2),
        ];
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 2,
                unit_deadline: Duration::from_secs(30),
            },
            None,
            None,
        )
        .await?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical despite the forced steal"
        );

        // The steal fired: the fast source delivered strictly more than its own
        // 32 MiB initial segment (it took over the slow source's tail).
        assert!(
            src_fast.delivered_bytes() > 32 * 1024 * 1024,
            "the fast source must have stolen work beyond its own segment: fast={}",
            src_fast.delivered_bytes()
        );
        // THE no-double-pay assertion: total bytes fetched across BOTH sources is
        // within a small bounded slop of the blob size. Without cancellation both
        // sources fetch the stolen ~16 MiB tail (~80 MiB total); cancellation keeps
        // it near 64 MiB.
        let total_delivered = src_fast.delivered_bytes() + src_slow.delivered_bytes();
        assert!(
            total_delivered <= total + 8 * 1024 * 1024,
            "the stolen tail must NOT be fetched twice: delivered {total_delivered} \
             (fast={}, slow={}) for a {total}-byte blob",
            src_fast.delivered_bytes(),
            src_slow.delivered_bytes()
        );
        Ok(())
    }

    /// The multi-thread variant of the no-double-pay guard, deterministically
    /// hitting the completed-but-uncleared window on a real 2-thread runtime.
    /// `src_slow_finish` delivers its whole segment then stalls INSIDE `finish`,
    /// so its completed range stays in `in_flight` (delivered, `fill_gap` not yet
    /// returned) for the whole stall. `src_stealer` starts late, finishes its own
    /// segment, and — with `pending` empty — steals that completed, fully-present
    /// range. Without the present-bytes backstop the stealer re-opens and re-pays
    /// the stolen tail (total fetched climbs past the blob size); the backstop
    /// re-derives the still-missing sub-ranges (empty here) before driving, so the
    /// stolen completed range is skipped and total fetched stays near the blob
    /// size. This scenario also drives the store from two OS threads at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forced_steal_no_double_pay_on_multi_thread_runtime() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let total = data.len() as u64;
        let ledger_finish = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_stealer = Arc::new(PoolLedger::new(Cumulative::default()));
        // Delivers its segment fast, then holds it completed-but-uncleared for
        // 500 ms inside `finish` — the window a peer steals into.
        let src_slow_finish = ScriptedSource::new(data.clone())?
            .slow_finish(Duration::from_millis(500))
            .paying(Arc::clone(&ledger_finish));
        // Starts 100 ms late so the other source is already parked in `finish`
        // (its range delivered and present) by the time this one frees up and
        // steals it.
        let src_stealer = ScriptedSource::new(data.clone())?
            .slow_to_start(Duration::from_millis(100))
            .paying(Arc::clone(&ledger_stealer));
        let root = src_slow_finish.root();
        let (store, dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let lanes = vec![
            lane(&src_slow_finish, Arc::clone(&ledger_finish), 0xA1),
            lane(&src_stealer, Arc::clone(&ledger_stealer), 0xB2),
        ];
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 4,
                unit_deadline: Duration::from_secs(30),
            },
            None,
            None,
        )
        .await?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical on the multi-thread runtime"
        );

        let total_delivered = src_slow_finish.delivered_bytes() + src_stealer.delivered_bytes();
        // The backstop makes this exactly the blob size (every present-range steal
        // is skipped); without it the stolen tail is re-fetched, +8 MiB here.
        // A 4 MiB slop sits cleanly between the two.
        assert!(
            total_delivered <= total + 4 * 1024 * 1024,
            "a completed-but-uncleared range must not be re-fetched: delivered \
             {total_delivered} (slow_finish={}, stealer={}) for a {total}-byte blob",
            src_slow_finish.delivered_bytes(),
            src_stealer.delivered_bytes(),
        );
        Ok(())
    }

    /// Total-failure guard: every source faults immediately, so
    /// `multi_source_fetch` returns an error WITHOUT hanging (the whole body runs
    /// under a timeout).
    #[tokio::test]
    async fn all_sources_failing_errors_without_hang() -> anyhow::Result<()> {
        let data = blob(32 * 1024 * 1024);
        let total = data.len() as u64;
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?
            .with_fault_after(0, || anyhow::anyhow!("immediate fault a"))
            .paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?
            .with_fault_after(0, || anyhow::anyhow!("immediate fault b"))
            .paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, _dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let lanes = vec![
            lane(&src_a, Arc::clone(&ledger_a), 0xA1),
            lane(&src_b, Arc::clone(&ledger_b), 0xB2),
        ];
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            multi_source_fetch(
                &store,
                &lanes,
                &pacer,
                &funder,
                root,
                0,
                total,
                &DriveConfig {
                    working_deposit: U256::ZERO,
                    max_settle_waits: 0,
                    settle_backoff: Duration::from_millis(1),
                },
                &MultiSourceConfig {
                    max_sources: 2,
                    unit_deadline: Duration::from_secs(10),
                },
                None,
                None,
            ),
        )
        .await;

        let inner = result.expect("multi_source_fetch must not hang when all sources fail");
        assert!(
            inner.is_err(),
            "all sources failing must surface as an error, not a false success"
        );
        Ok(())
    }

    /// A TERMINAL fault aborts the whole fetch with THAT typed error and does NOT
    /// reassign the failed source's range to a peer. `src_terminal` (lane 0) owns
    /// the first segment and faults at byte 0 with a typed
    /// [`UpstreamVoucherRejected`] — a payment-layer rejection the shared pool
    /// hits against every provider, so no other lane can fix it. `src_peer`
    /// (lane 1) is slow to start, so it is still on its OWN second segment when the
    /// terminal fault cancels the worker set. Folding every `fill_gap` `Err` into
    /// `Faulted` would reassign the range and end as the generic "all sources
    /// failed"; the scheduler instead propagates the typed error verbatim
    /// (downcast-assertable) and leaves the failed segment unfetched.
    #[tokio::test]
    async fn terminal_fault_propagates_and_is_not_reassigned() -> anyhow::Result<()> {
        use decdn_protocol::client::VoucherRejectReason;

        let data = blob(64 * 1024 * 1024);
        let total = data.len() as u64;
        let ledger_terminal = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_peer = Arc::new(PoolLedger::new(Cumulative::default()));
        // Lane 0 faults at its first byte with a typed payment rejection. A
        // non-`SpendingCapExhausted` reason is used so the driver's exhaustion /
        // reseed self-heal (which only fires on `SpendingCapExhausted`) does not
        // intercept it — `fill_gap` returns it verbatim.
        let src_terminal = ScriptedSource::new(data.clone())?
            .with_fault_after(0, || {
                anyhow::Error::new(crate::UpstreamVoucherRejected {
                    reason: VoucherRejectReason::CapabilityExpired,
                    bundle: None,
                })
            })
            .paying(Arc::clone(&ledger_terminal));
        // Lane 1 is healthy but slow to start, so the terminal fault cancels it
        // before it could finish its own segment and steal lane 0's tail.
        let src_peer = ScriptedSource::new(data.clone())?
            .slow_to_start(Duration::from_secs(10))
            .paying(Arc::clone(&ledger_peer));
        let root = src_terminal.root();
        let (store, _dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let lanes = vec![
            lane(&src_terminal, Arc::clone(&ledger_terminal), 0xA1),
            lane(&src_peer, Arc::clone(&ledger_peer), 0xB2),
        ];
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            multi_source_fetch(
                &store,
                &lanes,
                &pacer,
                &funder,
                root,
                0,
                total,
                &DriveConfig {
                    working_deposit: U256::ZERO,
                    max_settle_waits: 0,
                    settle_backoff: Duration::from_millis(1),
                },
                &MultiSourceConfig {
                    max_sources: 2,
                    unit_deadline: Duration::from_secs(30),
                },
                None,
                None,
            ),
        )
        .await
        .expect("a terminal fault must abort promptly, not hang");

        // The typed error propagated — NOT the generic "all sources failed".
        let err = result.expect_err("a terminal fault must fail the fetch");
        assert!(
            err.downcast_ref::<crate::UpstreamVoucherRejected>()
                .is_some(),
            "the terminal error must propagate verbatim, not be masked: {err:#}"
        );

        // Lane 0 delivered nothing (it faulted at byte 0) and its range was NOT
        // reassigned: a region well inside lane 0's first segment is still entirely
        // missing. A reassigning scheduler would have had the peer fill it.
        assert_eq!(
            src_terminal.delivered_bytes(),
            0,
            "the terminal source faulted before delivering a byte"
        );
        let probe = 16 * 1024 * 1024;
        let missing =
            crate::driver::contiguous_byte_ranges(&store.missing_ranges(0, probe).await?, total);
        let missing_bytes: u64 = missing.iter().map(|(_, l)| *l).fold(0, u64::saturating_add);
        assert_eq!(
            missing_bytes, probe,
            "the failed source's range must not be reassigned to a peer"
        );
        Ok(())
    }

    /// Shared-pool exhaustion is terminal FOR THE SCHEDULER (not the shared
    /// classifier): a lane whose pacer refuses the next voucher aborts
    /// `multi_source_fetch` with the typed [`PoolExhausted`] rather than
    /// reassigning the refused range and masking it as the generic "all sources
    /// failed". Lane B faults at once (its segment is reassigned to A). A fetches
    /// its OWN 32 MiB segment (a first leg always draws — voucher cost is unpriced
    /// until the first open), then, with the pool pre-drained by a peer's
    /// `prior_spend`, the reassigned second leg is REFUSED at the leg boundary — a
    /// `PoolExhausted` — before A delivers any of it. So a whole ~32 MiB segment
    /// stays unfetched: the refused range is NOT reassigned onward.
    ///
    /// This is the multi-source counterpart to the single-source failover path,
    /// which instead RETRIES a budget refusal against a cheaper provider (asserted
    /// by `retry.rs`'s `pool_exhausted_falls_over_in_the_shared_classifier`) — the
    /// pool-scope terminality lives here, in the scheduler, not the shared
    /// classifier.
    #[tokio::test]
    async fn pool_exhaustion_aborts_the_scheduler_and_is_not_reassigned() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let total = data.len() as u64;
        let deposit = U256::from(100u64);
        // A peer lane that has already spent most of the pool on prior streams —
        // enough that A's own 32 MiB segment fits the remaining headroom, but the
        // reassigned second segment cannot.
        let prior_spend = U256::from(68u64);

        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative {
            bytes: U256::from(68u64 * 1024 * 1024),
            amount: prior_spend,
        }));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        // Lane B faults at its first byte: it contributes nothing, so its 32 MiB
        // segment is reassigned to lane A as a second leg.
        let src_b = ScriptedSource::new(data.clone())?
            .with_fault_after(0, || anyhow::anyhow!("scripted immediate fault"))
            .paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, _dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let full = Coverage::full(num_blocks(total));
        let lanes = vec![
            SourceLane {
                source: &src_a,
                ctx: Arc::new(Mutex::new(ctx_with(0xA1, deposit))),
                ledger: Arc::clone(&ledger_a),
                coverage: full.clone(),
            },
            SourceLane {
                source: &src_b,
                ctx: Arc::new(Mutex::new(ctx_with(0xB2, deposit))),
                ledger: Arc::clone(&ledger_b),
                coverage: full,
            },
        ];
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            multi_source_fetch(
                &store,
                &lanes,
                &pacer,
                &funder,
                root,
                0,
                total,
                &DriveConfig {
                    // Reactive top-up disabled: a budget refusal is a hard
                    // `PoolExhausted`, not a top-up.
                    working_deposit: U256::ZERO,
                    max_settle_waits: 0,
                    settle_backoff: Duration::from_millis(1),
                },
                &MultiSourceConfig {
                    max_sources: 2,
                    unit_deadline: Duration::from_secs(30),
                },
                None,
                None,
            ),
        )
        .await
        .expect("pool exhaustion must abort promptly, not hang");

        // The typed `PoolExhausted` propagated — the scheduler aborted rather than
        // masking it as the generic "all sources failed".
        let err = result.expect_err("a shared-pool exhaustion must fail the fetch");
        assert!(
            err.downcast_ref::<PoolExhausted>().is_some(),
            "the pool-exhaustion error must propagate verbatim from the scheduler, \
             not be masked as 'all sources failed': {err:#}"
        );

        // A whole ~32 MiB segment stays unfetched: the refused reassigned range was
        // NOT covered by any lane (A delivered only its own one segment).
        let missing =
            crate::driver::contiguous_byte_ranges(&store.missing_ranges(0, total).await?, total);
        let missing_bytes: u64 = missing.iter().map(|(_, l)| *l).fold(0, u64::saturating_add);
        assert!(
            missing_bytes >= 30 * 1024 * 1024,
            "the refused segment must stay unfetched (a whole ~32 MiB), not be \
             reassigned onward: only {missing_bytes} bytes missing"
        );
        assert!(
            src_a.delivered_bytes() < total,
            "A delivered only its own segment, never the refused reassigned one: {}",
            src_a.delivered_bytes()
        );
        Ok(())
    }

    /// The pool-wide gate: a run registry's `total_committed()` sums EVERY
    /// registered lane, including one this fetch never touches — a concurrent
    /// fetch's lane sharing the same deposit. Lane C is registered but never
    /// passed to `multi_source_fetch` in `lanes`; it carries the same prior
    /// spend as the single-fetch `pool_exhaustion_aborts_the_scheduler_and_is_not_reassigned`
    /// test's lane B. Summed the OLD way (fold over just this fetch's `lanes`,
    /// both fresh), the pool looks fully solvent and the fetch draws past the
    /// true remaining deposit; summed the pool-wide way (`Some(&reg)`), C's
    /// spend already claims most of the deposit, so the second leg's voucher is
    /// `Refuse`d exactly as it is when the spend sits on an in-fetch lane.
    #[tokio::test]
    #[allow(clippy::too_many_lines)] // mirrors the full-drive shape of the
    // adjacent `pool_exhaustion_aborts_the_scheduler_and_is_not_reassigned` test
    async fn pool_wide_spent_gates_on_other_run_lanes() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let total = data.len() as u64;
        let deposit = U256::from(100u64);
        let other_lane_spend = U256::from(68u64);

        let reg = LaneLedgers::new();
        let handle_a = reg.get_or_insert(
            LaneKey {
                pool_id: B256::ZERO,
                signer: Address::ZERO,
                provider: Address::repeat_byte(0xA1),
            },
            || LaneHandle {
                ledger: Arc::new(PoolLedger::new(Cumulative::default())),
                ctx: Arc::new(Mutex::new(ctx_with(0xA1, deposit))),
            },
        );
        let handle_b = reg.get_or_insert(
            LaneKey {
                pool_id: B256::ZERO,
                signer: Address::ZERO,
                provider: Address::repeat_byte(0xB2),
            },
            || LaneHandle {
                ledger: Arc::new(PoolLedger::new(Cumulative::default())),
                ctx: Arc::new(Mutex::new(ctx_with(0xB2, deposit))),
            },
        );
        // Lane C belongs to a DIFFERENT concurrent fetch on the same run: it is
        // registered on `reg` but never appears in this fetch's `lanes`.
        reg.get_or_insert(
            LaneKey {
                pool_id: B256::ZERO,
                signer: Address::ZERO,
                provider: Address::repeat_byte(0xC3),
            },
            || LaneHandle {
                ledger: Arc::new(PoolLedger::new(Cumulative {
                    bytes: U256::from(68u64 * 1024 * 1024),
                    amount: other_lane_spend,
                })),
                ctx: Arc::new(Mutex::new(ctx_with(0xC3, deposit))),
            },
        );

        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&handle_a.ledger));
        // Lane B faults at its first byte: it contributes nothing, so its 32 MiB
        // segment is reassigned to lane A as a second leg.
        let src_b = ScriptedSource::new(data.clone())?
            .with_fault_after(0, || anyhow::anyhow!("scripted immediate fault"))
            .paying(Arc::clone(&handle_b.ledger));
        let root = src_a.root();
        let (store, _dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let full = Coverage::full(num_blocks(total));
        let lanes = vec![
            SourceLane {
                source: &src_a,
                ctx: Arc::clone(&handle_a.ctx),
                ledger: Arc::clone(&handle_a.ledger),
                coverage: full.clone(),
            },
            SourceLane {
                source: &src_b,
                ctx: Arc::clone(&handle_b.ctx),
                ledger: Arc::clone(&handle_b.ledger),
                coverage: full,
            },
        ];
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            multi_source_fetch(
                &store,
                &lanes,
                &pacer,
                &funder,
                root,
                0,
                total,
                &DriveConfig {
                    // Reactive top-up disabled: a budget refusal is a hard
                    // `PoolExhausted`, not a top-up.
                    working_deposit: U256::ZERO,
                    max_settle_waits: 0,
                    settle_backoff: Duration::from_millis(1),
                },
                &MultiSourceConfig {
                    max_sources: 2,
                    unit_deadline: Duration::from_secs(30),
                },
                None,
                Some(&reg),
            ),
        )
        .await
        .expect("pool exhaustion must abort promptly, not hang");

        // The typed `PoolExhausted` propagated: the pool-wide view (this
        // fetch's two fresh lanes PLUS lane C's prior spend registered
        // elsewhere) is what refused the second leg, not a per-fetch sum that
        // would have seen only the two fresh lanes and called the pool
        // solvent.
        let err = result.expect_err("the pool-wide spend must refuse the second leg");
        assert!(
            err.downcast_ref::<PoolExhausted>().is_some(),
            "the pool-exhaustion error must propagate verbatim from the scheduler, \
             not be masked as 'all sources failed': {err:#}"
        );

        let missing =
            crate::driver::contiguous_byte_ranges(&store.missing_ranges(0, total).await?, total);
        let missing_bytes: u64 = missing.iter().map(|(_, l)| *l).fold(0, u64::saturating_add);
        assert!(
            missing_bytes >= 30 * 1024 * 1024,
            "the refused segment must stay unfetched (a whole ~32 MiB): only \
             {missing_bytes} bytes missing"
        );
        Ok(())
    }

    /// Part A — per-provider payment lanes. Two sources with DISTINCT providers
    /// each pay their OWN ledger: the fetch assembles byte-identical, and each
    /// lane's cumulative advances INDEPENDENTLY, tracking exactly the wire that
    /// source delivered (never the peer's). One `ctx`/`ledger` shared across
    /// sources would pay a second provider's bytes on
    /// the first provider's lane; here each lane's `committed().bytes` matches its
    /// OWN source's delivered wire, proving the lanes are separate.
    #[tokio::test]
    async fn distinct_providers_each_pay_their_own_lane() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let total = data.len() as u64;
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        // Two DISTINCT on-chain providers — the operator spread `admit_sources`
        // produces. Each lane carries its own ctx (its own `provider`) and ledger.
        let lane_a = lane(&src_a, Arc::clone(&ledger_a), 0xA1);
        let lane_b = lane(&src_b, Arc::clone(&ledger_b), 0xB2);
        let provider_a = lane_a.ctx.lock().expect("ctx").provider;
        let provider_b = lane_b.ctx.lock().expect("ctx").provider;
        assert_ne!(
            provider_a, provider_b,
            "the two lanes must pay two DIFFERENT providers"
        );

        let lanes = vec![lane_a, lane_b];
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 4,
                unit_deadline: Duration::from_secs(10),
            },
            None,
            None,
        )
        .await?;
        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical across two distinct-provider lanes"
        );

        // Each lane's ledger advanced INDEPENDENTLY, and each carries only its OWN
        // ~half of the blob — NOT the pool total. A single ledger shared across
        // sources would have BOTH sources'
        // `finish` advance the SAME cumulative to ~the whole blob's wire; two
        // separate lanes each stay strictly below the whole blob, and together
        // cover it.
        let committed_a = ledger_a.committed();
        let committed_b = ledger_b.committed();
        assert!(
            committed_a.amount > U256::ZERO && committed_b.amount > U256::ZERO,
            "both lanes must have advanced their own cumulative: a={committed_a:?} b={committed_b:?}"
        );
        assert!(
            committed_a.bytes < U256::from(total) && committed_b.bytes < U256::from(total),
            "neither lane alone may bill the whole blob — a shared ledger would: \
             a={committed_a:?} b={committed_b:?} total={total}"
        );
        assert!(
            committed_a.bytes.saturating_add(committed_b.bytes) >= U256::from(total),
            "the two independent lanes must together cover the whole blob: a={committed_a:?} \
             b={committed_b:?} total={total}"
        );
        Ok(())
    }

    /// A [`Pacer`] that records the minimum `remaining_deposit` any `decide` saw,
    /// then defers to [`BudgetPacer`]. Proves what balance the workers actually
    /// gated on.
    struct MinRemainingPacer {
        inner: BudgetPacer,
        min_remaining: Mutex<Option<U256>>,
    }

    impl Pacer for MinRemainingPacer {
        fn decide(&self, s: &PaceState) -> PaceDecision {
            if let Ok(mut g) = self.min_remaining.lock() {
                *g = Some(g.map_or(s.remaining_deposit, |m| m.min(s.remaining_deposit)));
            }
            self.inner.decide(s)
        }
    }

    /// Part B — shared-pool aggregate solvency. Two lanes draw on ONE pool
    /// deposit `D`. Each worker's `remaining_deposit` is `D - Σ committed across
    /// EVERY lane`, so the smallest balance any pacing decision saw drops below
    /// `D - max(single-lane committed)` — the floor a per-lane gate (each lane
    /// subtracting only its OWN spend) could never go under. That difference is
    /// the whole point: without the aggregate reader each of the two lanes would
    /// believe the entire deposit was its own.
    #[tokio::test]
    async fn concurrent_lanes_gate_on_the_shared_pool_balance() -> anyhow::Result<()> {
        let data = blob(8 * 1024 * 1024);
        let total = data.len() as u64;
        // A pool deposit far above the ~8-unit blob cost, so the fetch always
        // completes; the test reads the observed balances, not a refusal.
        let deposit = U256::from(1_000u64);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = MinRemainingPacer {
            inner: BudgetPacer::new(),
            min_remaining: Mutex::new(None),
        };

        // Both lanes' ctxs carry the SAME shared pool deposit `D`.
        let full = Coverage::full(num_blocks(total));
        let lanes = vec![
            SourceLane {
                source: &src_a,
                ctx: Arc::new(Mutex::new(ctx_with(0xA1, deposit))),
                ledger: Arc::clone(&ledger_a),
                coverage: full.clone(),
            },
            SourceLane {
                source: &src_b,
                ctx: Arc::new(Mutex::new(ctx_with(0xB2, deposit))),
                ledger: Arc::clone(&ledger_b),
                coverage: full,
            },
        ];
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 4,
                unit_deadline: Duration::from_secs(10),
            },
            None,
            None,
        )
        .await?;
        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "the fetch still assembles byte-identical under the shared-solvency gate"
        );

        let committed_a = ledger_a.committed().amount;
        let committed_b = ledger_b.committed().amount;
        assert!(
            committed_a > U256::ZERO && committed_b > U256::ZERO,
            "both lanes must have paid, so the aggregate exceeds either lane alone"
        );
        let max_lane = committed_a.max(committed_b);
        let aggregate = committed_a.saturating_add(committed_b);
        let min_remaining = pacer
            .min_remaining
            .lock()
            .expect("min lock")
            .expect("at least one decide ran");

        // The aggregate gate: the lowest balance a worker saw is `D - Σ committed`.
        assert_eq!(
            min_remaining,
            deposit.saturating_sub(aggregate),
            "a worker must have gated on the SHARED remaining (deposit minus every lane's spend)"
        );
        // And that is strictly below the per-lane floor `D - max_lane`, so a
        // per-lane gate could never have produced it — proving aggregation.
        assert!(
            min_remaining < deposit.saturating_sub(max_lane),
            "the shared gate must see less than a single lane's own remaining: \
             min={min_remaining:?} per_lane_floor={:?}",
            deposit.saturating_sub(max_lane)
        );
        Ok(())
    }

    /// Part B — the shared gate actually BLOCKS a lane (no collective overspend).
    ///
    /// A 64 MiB blob splits into two 32 MiB segments. Lane B's source faults at
    /// once (delivers nothing), but its ledger is PRE-SEEDED with a committed
    /// amount `PRIOR_SPEND` — a peer lane that has already drawn most of the pool
    /// on earlier streams. Lane A is healthy: it fetches its OWN 32 MiB segment
    /// (a first leg always draws — voucher cost is unpriced until the first open),
    /// then B's faulted segment is reassigned to it as a SECOND leg. By then A's
    /// worker has a priced voucher cost AND the aggregate reader reports
    /// `A_committed + PRIOR_SPEND`, which is at/over the deposit — so the second
    /// leg is REFUSED. The fetch ends incomplete, and A never bills a second
    /// segment.
    ///
    /// The bound this asserts and why: the aggregate gate stops a lane STARTING a
    /// new leg once the SHARED committed leaves less than the next voucher's cost.
    /// It is a leg-boundary gate, so the honest ceiling on total committed is
    /// `deposit + Σ (one in-flight leg per lane)` — a leg already admitted against
    /// headroom may overshoot it by its own cost (here A's first leg's small bao-
    /// proof overshoot), and the on-chain pool is the hard backstop that never
    /// redeems past its deposit. What the gate PROVABLY prevents — the unbounded
    /// growth a naive own-committed gate would allow — is a lane opening a FURTHER
    /// leg into an already-drained pool. This test pins exactly that: A is capped
    /// at ONE segment, not two.
    ///
    /// RED (documented in the task report): with a naive `deposit - own_committed`
    /// gate, A's second-leg check reads `deposit - A_committed` (headroom remains,
    /// `PRIOR_SPEND` invisible), so A DRAWS the reassigned segment, the fetch
    /// COMPLETES, and A bills ~two segments — combined committed climbs past the
    /// deposit. The aggregate gate flips every one of those.
    #[tokio::test]
    async fn shared_gate_refuses_a_second_leg_into_a_drained_pool() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let total = data.len() as u64;
        let deposit = U256::from(100u64);
        // A peer lane that has already spent most of the pool (68 of 100 units) on
        // prior streams — enough that A's own 32 MiB segment (~33 units of wire)
        // fits in the remaining headroom, but a SECOND segment cannot.
        let prior_spend = U256::from(68u64);

        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative {
            bytes: U256::from(68u64 * 1024 * 1024),
            amount: prior_spend,
        }));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        // Lane B faults at its first byte: it contributes no new bytes and issues
        // no voucher, so its committed stays exactly the pre-seeded `prior_spend`.
        // Its 32 MiB segment is then reassigned to lane A.
        let src_b = ScriptedSource::new(data.clone())?
            .with_fault_after(0, || anyhow::anyhow!("scripted immediate fault"))
            .paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let (store, _dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let full = Coverage::full(num_blocks(total));
        let lanes = vec![
            SourceLane {
                source: &src_a,
                ctx: Arc::new(Mutex::new(ctx_with(0xA1, deposit))),
                ledger: Arc::clone(&ledger_a),
                coverage: full.clone(),
            },
            SourceLane {
                source: &src_b,
                ctx: Arc::new(Mutex::new(ctx_with(0xB2, deposit))),
                ledger: Arc::clone(&ledger_b),
                coverage: full,
            },
        ];
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            multi_source_fetch(
                &store,
                &lanes,
                &pacer,
                &funder,
                root,
                0,
                total,
                &DriveConfig {
                    working_deposit: U256::ZERO,
                    max_settle_waits: 0,
                    settle_backoff: Duration::from_millis(1),
                },
                &MultiSourceConfig {
                    max_sources: 2,
                    unit_deadline: Duration::from_secs(30),
                },
                None,
                None,
            ),
        )
        .await
        .expect("must not hang");

        // The gate blocked A's second leg, so the blob never completed.
        assert!(
            result.is_err(),
            "the shared gate must refuse A's reassigned second leg once the pool is drained"
        );

        let committed_a = ledger_a.committed().amount;
        let committed_b = ledger_b.committed().amount;
        // B never delivered, so its committed is exactly the pre-seed.
        assert_eq!(
            committed_b, prior_spend,
            "the faulted peer lane billed nothing new"
        );
        // A billed its OWN one segment (~33 units of wire) and NOTHING for the
        // refused second — well under the ~66 two-segment bill a naive gate yields.
        assert!(
            committed_a > U256::ZERO && committed_a < U256::from(50u64),
            "A must bill exactly ONE segment, never the refused second: committed_a={committed_a:?}"
        );
        // A delivered only its own segment, never the reassigned one.
        assert!(
            src_a.delivered_bytes() < total,
            "A must not have delivered the whole blob — its second leg was refused: delivered={}",
            src_a.delivered_bytes()
        );
        // No-collective-overspend bound: combined committed stays within the
        // deposit plus at most one in-flight leg's overshoot per lane (here only
        // A had an in-flight first leg). A naive own-committed gate would push this
        // to `prior_spend + ~2 segments` ≈ 134, far past the ceiling.
        let combined = committed_a.saturating_add(committed_b);
        let one_leg_ceiling = deposit.saturating_add(U256::from(40u64));
        assert!(
            combined <= one_leg_ceiling,
            "combined committed must stay within deposit + one in-flight leg: \
             combined={combined:?} ceiling={one_leg_ceiling:?}"
        );
        Ok(())
    }

    // ---- stall watchdog (`watchdog`, selected at the worker's `tokio::select!`) ----

    /// A store double whose `missing_ranges` replays a scripted sequence of
    /// still-missing byte counts, so every branch of [`watchdog`] runs against an
    /// exact progress history under a paused clock — no wall-clock racing, and no
    /// dependence on a source's real delivery timing.
    ///
    /// Only `total_bytes`/`missing_ranges` are reachable from `watchdog`; the rest
    /// of the [`IngestStore`] surface returns an error rather than panicking, so a
    /// future caller that starts using one gets a failure it can see.
    struct ScriptedMissing {
        total: u64,
        /// Still-missing byte counts, one consumed per call. The LAST entry
        /// repeats forever, which is what lets a "never trips" assertion run to a
        /// timeout instead of running out of script.
        script: Mutex<std::collections::VecDeque<u64>>,
    }

    impl ScriptedMissing {
        fn new(total: u64, script: &[u64]) -> Self {
            Self {
                total,
                script: Mutex::new(script.iter().copied().collect()),
            }
        }

        fn next_missing(&self) -> u64 {
            let mut q = self.script.lock().expect("script lock");
            if q.len() > 1 {
                q.pop_front().unwrap_or(0)
            } else {
                q.front().copied().unwrap_or(0)
            }
        }
    }

    fn unsupported<T>() -> decdn_bao_range::RangedStoreError {
        let _ = std::marker::PhantomData::<T>;
        decdn_bao_range::RangedStoreError::Backend(Box::from("unsupported on ScriptedMissing"))
    }

    impl decdn_bao_range::RangedStore for ScriptedMissing {
        fn total_bytes(&self) -> u64 {
            self.total
        }

        fn present_ranges(&self) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
            Box::pin(async { Err(unsupported::<()>()) })
        }

        fn missing_ranges(
            &self,
            byte_offset: u64,
            _byte_len: u64,
        ) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
            let missing = self.next_missing();
            Box::pin(async move {
                if missing == 0 {
                    return Ok(bao_tree::ChunkRanges::empty());
                }
                // 1 KiB per bao chunk; the byte counts the script names are
                // multiples of that.
                let start = bao_tree::ChunkNum(byte_offset / 1024);
                let end = bao_tree::ChunkNum((byte_offset + missing) / 1024);
                Ok(bao_tree::ChunkRanges::from(start..end))
            })
        }

        fn admit(
            &self,
            _range: decdn_bao_range::AlignedRange,
            _bao_bytes: bytes::Bytes,
        ) -> decdn_bao_range::RangedFuture<'_, ()> {
            Box::pin(async { Err(unsupported::<()>()) })
        }

        fn read(
            &self,
            _byte_offset: u64,
            _byte_len: u64,
        ) -> decdn_bao_range::RangedFuture<'_, bytes::Bytes> {
            Box::pin(async { Err(unsupported::<()>()) })
        }

        fn is_complete(&self) -> decdn_bao_range::RangedFuture<'_, bool> {
            Box::pin(async { Err(unsupported::<()>()) })
        }

        fn finalize(&self) -> decdn_bao_range::RangedFuture<'_, ()> {
            Box::pin(async { Err(unsupported::<()>()) })
        }
    }

    impl crate::source::IngestStore for ScriptedMissing {
        fn ingest_stream<'a, R>(
            &'a self,
            _range: &'a decdn_bao_range::AlignedRange,
            _reader: R,
            _on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<R>> + 'a>>
        where
            R: crate::BaoRangeReader + 'a,
        {
            Box::pin(async { Err(anyhow::anyhow!("unsupported on ScriptedMissing")) })
        }

        fn flush_present_record(&self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// How long `watchdog` takes to trip against a scripted progress history, or
    /// `None` if it does not trip within an hour of virtual time.
    async fn watchdog_trips(script: &[u64], deadline: Duration) -> bool {
        let store = ScriptedMissing::new(64 * 1024 * 1024, script);
        tokio::time::timeout(
            Duration::from_hours(1),
            super::watchdog(&store, 0, 64 * 1024 * 1024, deadline),
        )
        .await
        .is_ok()
    }

    /// A full window with bytes still missing and the count NOT shrinking is the
    /// definition of a stall — the watchdog trips and the worker's range is
    /// reassigned. Flip the comparison to `now > prev` and a wedged source is
    /// never reassigned: the fetch hangs to the outer cap.
    #[tokio::test(start_paused = true)]
    async fn watchdog_trips_when_missing_stops_shrinking() {
        assert!(
            watchdog_trips(&[8192, 8192], Duration::from_secs(10)).await,
            "no progress across a full window must trip the watchdog"
        );
    }

    /// A fully delivered range (`missing == 0`) is left to `fill_gap`'s own
    /// completion, NEVER tripped: the source is inside `finish`, draining the
    /// vouchers for bytes it already delivered. Tripping here would reassign an
    /// ALREADY-PAID range — a direct double-pay.
    #[tokio::test(start_paused = true)]
    async fn watchdog_never_trips_a_fully_delivered_range() {
        assert!(
            !watchdog_trips(&[8192, 0], Duration::from_secs(10)).await,
            "a delivered range must never be tripped while it finishes paying"
        );
    }

    /// A source that keeps delivering, however slowly, resets the window at each
    /// sample and is never reassigned. Without the reset, a healthy-but-slow
    /// source is falsely reassigned mid-checkpoint — and the new lane re-pays the
    /// credit-window tail.
    #[tokio::test(start_paused = true)]
    async fn watchdog_window_resets_while_missing_shrinks() {
        assert!(
            !watchdog_trips(
                &[8192, 7168, 6144, 5120, 4096, 3072, 2048, 1024, 0],
                Duration::from_secs(10)
            )
            .await,
            "shrinking missing bytes must reset the window, never trip"
        );
    }

    /// A zero deadline disables the watchdog outright — reachable today via
    /// `--unit-deadline-ms 0`.
    #[tokio::test(start_paused = true)]
    async fn watchdog_zero_deadline_never_trips() {
        assert!(
            !watchdog_trips(&[8192, 8192], Duration::ZERO).await,
            "a zero unit deadline must disable the watchdog"
        );
    }

    /// The two deadlines the wedge test runs under, both derived from one
    /// measurement so they cannot drift apart.
    struct WedgeBudget {
        /// What a lane gets to shrink the missing window before the watchdog
        /// reassigns it.
        unit_deadline: Duration,
        /// What the whole fetch gets before the test calls the watchdog broken.
        outer_bound: Duration,
    }

    /// Measure one [`ClientRangedStore::INGEST_CHECKPOINT_BYTES`] checkpoint's
    /// delivery through a fresh lane on this machine, and size both of the
    /// wedge test's deadlines a safety factor above it.
    ///
    /// `unit_deadline` is the budget a lane gets to shrink the missing window
    /// before the watchdog reassigns it, so the honest rate to size it against
    /// is the time one checkpoint actually takes to land here. A fixed value is
    /// load-flaky: a contended runner routinely moves a checkpoint slower than
    /// a constant chosen for the healthy case, and the healthy lane then gets
    /// falsely reassigned.
    ///
    /// `outer_bound` scales with it, because a fixed bound against a scaled
    /// deadline only moves the flake. The watchdog needs up to TWO windows to
    /// trip — one that sees the checkpoint land and resets, one that sees
    /// nothing — and the surviving lane then refetches the wedged lane's tail,
    /// so the bound has to cover several deadlines rather than a constant.
    ///
    /// `measured` spans one whole single-lane fetch — lane open, channel
    /// bootstrap, deposit, transfer, voucher drain — not the checkpoint alone,
    /// so it OVERSTATES the per-checkpoint cost. That is the safe direction,
    /// and it is why `K` is a factor rather than a margin.
    ///
    /// The clamp guards the derivation rather than forming part of it: under
    /// `FLOOR` a fast machine gets a deadline too tight for its own scheduling
    /// jitter, and over `CEILING` the safety factor decays toward 1×. A machine
    /// slow enough to reach `CEILING` is one this test is unreliable on
    /// whatever it is handed, so the ceiling says so on stderr rather than
    /// pretending the factor still holds.
    #[expect(
        clippy::print_stderr,
        reason = "test-only calibration diagnostic surfaced in the nextest log"
    )]
    async fn calibrated_wedge_budget() -> anyhow::Result<WedgeBudget> {
        const FLOOR: Duration = Duration::from_millis(600);
        const CEILING: Duration = Duration::from_secs(5);
        const K: u32 = 10;
        let checkpoint = usize::try_from(ClientRangedStore::INGEST_CHECKPOINT_BYTES)?;

        let data = blob(checkpoint);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger));
        let root = src.root();
        let total = src.total_bytes();
        let (store, _dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();
        let lanes = vec![lane(&src, Arc::clone(&ledger), 0xBB)];
        let started = tokio::time::Instant::now();
        let fetched = multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            // A generous deadline so this calibration fetch — a healthy lane —
            // is measured, not tripped.
            &MultiSourceConfig {
                max_sources: 1,
                unit_deadline: CEILING,
            },
            None,
            None,
        )
        .await;
        // A healthy single lane with nothing to reassign to, so the only
        // plausible failure is this fetch tripping its OWN watchdog on a
        // machine slower than CEILING. Take the ceiling rather than failing the
        // wedge test under a lane-fault message about the scheduler under test.
        let measured = if fetched.is_ok() {
            started.elapsed()
        } else {
            CEILING
        };
        let wanted = measured.saturating_mul(K);
        let unit_deadline = wanted.clamp(FLOOR, CEILING);
        if wanted > CEILING {
            eprintln!(
                "wedge calibration: one checkpoint took {measured:?}, wanting {wanted:?}; \
                 capped at {CEILING:?}, so the {K}× safety factor is degraded here"
            );
        }
        Ok(WedgeBudget {
            unit_deadline,
            outer_bound: unit_deadline.saturating_mul(4) + Duration::from_secs(15),
        })
    }

    /// End-to-end: a source that opens, delivers a prefix, then WEDGES without
    /// erroring is ended by the watchdog ALONE, and its unfetched remainder is
    /// picked up by a healthy peer. `with_fault_after` cannot produce this shape —
    /// it takes the `fill_gap`-error arm instead.
    ///
    /// The segments are deliberately below `MIN_SPLIT_SIZE` (a 16 MiB blob over
    /// two lanes) so the healthy peer CANNOT steal the wedged lane's tail: with
    /// stealing unavailable, the watchdog is the only thing that can end the
    /// wedge, and without it the fetch sits for the wedge's full 120 s.
    #[tokio::test]
    async fn wedged_source_is_ended_by_the_watchdog_and_its_tail_reassigned() -> anyhow::Result<()>
    {
        let data = blob(16 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        // A delivers ~4 MiB (one INGEST_CHECKPOINT_BYTES, so `missing_ranges`
        // visibly shrinks first) and then sleeps far past the unit deadline.
        let src_a = ScriptedSource::new(data.clone())?
            .stall_after(4 * 1024 * 1024, Duration::from_mins(2))
            .paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let lanes = vec![
            lane(&src_a, Arc::clone(&ledger_a), 0xA1),
            lane(&src_b, Arc::clone(&ledger_b), 0xB2),
        ];
        let budget = calibrated_wedge_budget().await?;
        // The outer bound is what turns "the watchdog never fired" into a
        // failure rather than a two-minute wait. It rides on the same
        // measurement as the unit deadline, so a slow machine widens both.
        tokio::time::timeout(
            budget.outer_bound,
            multi_source_fetch(
                &store,
                &lanes,
                &pacer,
                &funder,
                root,
                0,
                total,
                &DriveConfig {
                    working_deposit: U256::ZERO,
                    max_settle_waits: 0,
                    settle_backoff: Duration::from_millis(1),
                },
                &MultiSourceConfig {
                    max_sources: 2,
                    // Calibrated: comfortably above a checkpoint's worth of
                    // healthy delivery time, and far under the 120 s wedge, so
                    // the lane is still stalled when the watchdog samples it.
                    unit_deadline: budget.unit_deadline,
                },
                None,
                None,
            ),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "the fetch did not finish within {:?}: the watchdog did not end the wedged lane",
                budget.outer_bound
            )
        })??;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical after the wedged source was reassigned"
        );
        assert!(
            src_a.delivered_bytes() < 8 * 1024 * 1024,
            "the wedged source must not have delivered its whole segment: {}",
            src_a.delivered_bytes()
        );
        Ok(())
    }

    // ---- lane-set preconditions and failure reporting ----

    /// Two lanes on ONE provider are refused at the boundary. They would be two
    /// concurrent voucher streams on one `(signer, provider)` watermark — the
    /// hazard the one-unit-per-source rule exists to prevent — and the scheduler
    /// checks its own premise rather than trusting the caller's admission policy.
    #[tokio::test]
    async fn lanes_sharing_one_provider_are_refused() -> anyhow::Result<()> {
        let data = blob(1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, _dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        // Same provider byte on both lanes.
        let lanes = vec![
            lane(&src_a, Arc::clone(&ledger_a), 0xA1),
            lane(&src_b, Arc::clone(&ledger_b), 0xA1),
        ];
        let err = multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 4,
                unit_deadline: Duration::from_secs(10),
            },
            None,
            None,
        )
        .await
        .expect_err("two lanes on one provider must be refused");
        assert!(
            format!("{err:#}").contains("one lane per provider"),
            "the refusal must name the duplicate-provider precondition: {err:#}"
        );
        // Nothing was fetched or billed.
        assert_eq!(src_a.delivered_bytes(), 0, "no lane may run");
        assert_eq!(src_b.delivered_bytes(), 0, "no lane may run");
        Ok(())
    }

    /// A fetch that runs out of lanes reports WHAT each lane did and keeps the
    /// last real error as its cause, so the CLI's `downcast_ref` diagnosis (the
    /// two-cause unbound-cache-miss explanation) still reaches it. Dropping the
    /// errors leaves the user of a failed multi-GB fetch with a byte count and
    /// nothing else: `decdn` installs no tracing subscriber, so an unreturned
    /// error is a destroyed one.
    #[tokio::test]
    async fn all_sources_failing_names_each_lane_and_keeps_the_cause() -> anyhow::Result<()> {
        let data = blob(8 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        // Both sources refuse with a RETRYABLE typed error, so every lane drops
        // and the request stays incomplete.
        let src_a = ScriptedSource::new(data.clone())?
            .with_fault_after(0, || {
                anyhow::Error::new(crate::UpstreamRefused::mid_stream(
                    decdn_protocol::client::StreamError::Overloaded,
                ))
            })
            .paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?
            .with_fault_after(0, || {
                anyhow::Error::new(crate::UpstreamRefused::mid_stream(
                    decdn_protocol::client::StreamError::Overloaded,
                ))
            })
            .paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, _dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let lanes = vec![
            lane(&src_a, Arc::clone(&ledger_a), 0xA1),
            lane(&src_b, Arc::clone(&ledger_b), 0xB2),
        ];
        let err = multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 4,
                unit_deadline: Duration::from_secs(10),
            },
            None,
            None,
        )
        .await
        .expect_err("every lane refused, so the fetch must fail");

        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("bytes unfetched"),
            "the summary still states what is missing: {rendered}"
        );
        // Attribution: both lanes' payee addresses are named.
        assert!(
            rendered.contains(&format!("{}", Address::repeat_byte(0xA1)))
                && rendered.contains(&format!("{}", Address::repeat_byte(0xB2))),
            "each lane must be named by its provider: {rendered}"
        );
        // The typed cause survives, which is what makes the CLI's cache-miss
        // annotation reachable on this path at all.
        assert!(
            err.downcast_ref::<crate::UpstreamRefused>().is_some(),
            "the last real error must remain downcastable: {rendered}"
        );
        Ok(())
    }

    /// A freed worker with nothing to steal PARKS rather than retiring, so it is
    /// still there to take over when a peer faults moments later.
    ///
    /// Shape: an 16 MiB blob splits into two 8 MiB segments, both below the 16 MiB
    /// `MIN_SPLIT_SIZE`, so the fast worker's `pick` finds nothing splittable —
    /// the routine end-of-fetch condition. The slow worker then faults and
    /// re-queues its remainder. A retiring worker is gone by then and the fetch
    /// bails "all sources failed" with a healthy, already-paid lane sitting idle.
    #[tokio::test]
    async fn a_parked_worker_takes_over_a_later_faulted_peers_remainder() -> anyhow::Result<()> {
        let data = blob(16 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        // A: holds back long enough for B to finish its own segment and find
        // nothing worth stealing, then delivers a prefix and faults.
        let src_a = ScriptedSource::new(data.clone())?
            .slow_to_start(Duration::from_millis(400))
            .with_fault_after(2 * 1024 * 1024, || anyhow::anyhow!("scripted fault"))
            .paying(Arc::clone(&ledger_a));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let lanes = vec![
            lane(&src_a, Arc::clone(&ledger_a), 0xA1),
            lane(&src_b, Arc::clone(&ledger_b), 0xB2),
        ];
        multi_source_fetch(
            &store,
            &lanes,
            &pacer,
            &funder,
            root,
            0,
            total,
            &DriveConfig {
                working_deposit: U256::ZERO,
                max_settle_waits: 0,
                settle_backoff: Duration::from_millis(1),
            },
            &MultiSourceConfig {
                max_sources: 2,
                unit_deadline: Duration::from_secs(30),
            },
            None,
            None,
        )
        .await?;

        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "the parked worker covered the faulted peer's remainder"
        );
        Ok(())
    }
}
