//! Client-only multi-source fetch orchestration (spec §5.3): fan a request out
//! across several paid [`BlobSource`](crate::source::BlobSource)s that all write
//! into ONE shared [`IngestStore`](crate::source::IngestStore), assembling a
//! byte-identical blob.
//!
//! One worker future per source drives [`fill_gap`](crate::driver::fill_gap)
//! over the request's gap-set. The gap-set is split into large,
//! bao-group-aligned contiguous segments (one per source), and a freed source
//! does not idle: it *steals* the aligned second half of the largest range
//! still in flight ([`steal_split`](crate::segment::steal_split)), so a fast
//! source keeps helping a slow one.
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
//!   `fill_gap` would keep paying to `end` (the Task-4 double-pay).
//! - **Stall / fault.** A source with no verified progress within
//!   `unit_deadline` (the watchdog trips), or whose `fill_gap` returns `Err`,
//!   has the UN-fetched remainder of its range re-queued to `pending` for a
//!   DIFFERENT source, and stops taking work. Verified bytes already stored are
//!   never refetched.
//!
//! If every source stops with the request still incomplete, the fetch returns
//! an error rather than hanging.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::U256;
use decdn_bao_range::{AlignedRange, align_range};
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::driver::{DriveConfig, DriveCounters, contiguous_byte_ranges, fill_gap};
use crate::segment::{initial_segments, steal_split};
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
    /// The source stalled or faulted — re-queue the remainder for a DIFFERENT
    /// source and stop taking work.
    Faulted,
}

/// Client-side knobs for the multi-source scheduler (spec §8). The blob-size
/// engagement gate and the `enabled` kill switch live at the CLI wiring layer;
/// this is what the scheduler itself consumes.
#[derive(Debug, Clone, Copy)]
pub struct MultiSourceConfig {
    /// Cap on concurrently-used holders = the initial segment count. The
    /// scheduler engages `min(max_sources, sources.len())` segments.
    pub max_sources: usize,
    /// No-verified-progress deadline before a source's remaining range is
    /// reassigned. Consumed by the reassignment path (a later task); the core
    /// scheduler does not read it.
    pub unit_deadline: Duration,
}

/// Shared work-state, guarded by one [`AsyncMutex`]. `pending` seeds with the
/// initial segments; `in_flight[i]` is source `i`'s currently-owned range as
/// `(start, len)` (`None` = idle), which is both the tail-steal remaining-set
/// and the "at most one source owns any range" ledger.
struct Work {
    /// Segments not yet claimed by any worker (drains as workers pick).
    pending: VecDeque<AlignedRange>,
    /// Per-source current range, `None` when the source holds nothing.
    in_flight: Vec<Option<(u64, u64)>>,
    /// Per-source interrupt handles, indexed like `in_flight`. A steal signals
    /// `cancel[victim]`; the victim clears it on its next `pick`.
    cancel: Vec<Arc<CancelHandle>>,
}

impl Work {
    /// Under the caller's lock, choose worker `i`'s next range. Pop a pending
    /// segment first; when none remain, steal the aligned second half of the
    /// largest range still in flight ([`steal_split`]), trimming the victim so
    /// no other freed worker can re-steal the same tail. Records the choice in
    /// `in_flight[i]`. `Ok(None)` means nothing worth a fresh stream remains —
    /// the worker exits.
    ///
    /// # Errors
    ///
    /// Propagates the alignment error [`steal_split`] raises on an
    /// out-of-bounds range (never on the ranges this scheduler feeds it).
    fn pick(&mut self, i: usize, total_bytes: u64) -> anyhow::Result<Option<AlignedRange>> {
        // This worker is starting a fresh unit: clear any cancel signal left from
        // a prior unit, under the lock, so a stale `notify_one` permit cannot
        // spuriously cancel the new unit (see `cancelled`).
        if let Some(handle) = self.cancel.get(i) {
            handle.flag.store(false, Ordering::Release);
        }
        if let Some(seg) = self.pending.pop_front() {
            if let Some(slot) = self.in_flight.get_mut(i) {
                *slot = Some((seg.fetch_start(), seg.fetch_len()));
            }
            return Ok(Some(seg));
        }

        // Nothing pending: every remaining byte is in flight on a busy worker.
        // Steal the aligned second half of the largest such range. `in_flight[i]`
        // is `None` here (cleared before this pick), so this worker is excluded
        // from the remaining set and never steals from itself.
        let remaining: Vec<(u64, u64)> = self.in_flight.iter().flatten().copied().collect();
        let Some(half) = steal_split(&remaining, total_bytes)? else {
            if let Some(slot) = self.in_flight.get_mut(i) {
                *slot = None;
            }
            return Ok(None);
        };

        // Trim the victim — the largest in-flight range, the SAME argmax
        // `steal_split` picked (both iterate `in_flight` in order and take the
        // last maximum, so they agree) — to end at the split point. A later
        // freed worker then sees the shortened tail and cannot re-steal the half
        // this worker just took: at most one source owns any range, by
        // construction, in the work-state.
        let victim = self
            .in_flight
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| slot.map(|(_, len)| (idx, len)))
            .max_by_key(|&(_, len)| len)
            .map(|(idx, _)| idx);
        if let Some(idx) = victim
            && idx != i
            && let Some(Some((start, len))) = self.in_flight.get_mut(idx)
            && half.fetch_start() > *start
        {
            *len = half.fetch_start() - *start;
            // Signal the victim to STOP fetching past the split. Its
            // already-running `fill_gap` would otherwise fetch — and pay for —
            // the tail this worker just took. Set the flag then wake it, both
            // under the caller's `Work` lock, serialized against the victim's
            // own `pick` reset above. `notify_one` stores a permit if the victim
            // is not parked yet, so the signal is never lost.
            if let Some(handle) = self.cancel.get(idx) {
                handle.flag.store(true, Ordering::Release);
                handle.notify.notify_one();
            }
        }

        if let Some(slot) = self.in_flight.get_mut(i) {
            *slot = Some((half.fetch_start(), half.fetch_len()));
        }
        Ok(Some(half))
    }

    /// Release worker `i`'s lane once its `fill_gap` returns, so a peer's steal
    /// computation stops counting the finished range and this source can be
    /// re-picked for more work.
    fn clear(&mut self, i: usize) {
        if let Some(slot) = self.in_flight.get_mut(i) {
            *slot = None;
        }
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
/// over it, under a cancel/stall [`tokio::select!`], until `Work::pick`
/// returns `None` or the source is dropped. Exactly one outstanding range at a
/// time (the loop drives one `fill_gap` to a terminal outcome before the next
/// pick) — the one-unit-per-source lane invariant, structurally.
#[allow(clippy::too_many_arguments)]
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
    on_progress: Option<&ProgressCallback>,
    unit_deadline: Duration,
    pool_spent: &(dyn Fn() -> U256 + Send + Sync),
) -> anyhow::Result<()>
where
    St: IngestStore,
    S: BlobSource,
    P: Pacer,
    F: Funder,
{
    // This worker's cancel handle, cloned once so `cancelled` can await it
    // OUTSIDE the `Work` lock while a peer's `pick` signals it under the lock.
    let handle = {
        let w = work.lock().await;
        match w.cancel.get(i).map(Arc::clone) {
            Some(h) => h,
            None => anyhow::bail!("worker index {i} out of range for cancel handles"),
        }
    };
    // Each worker owns its own counters — the reactive-top-up budget is
    // per-worker here, not shared across the set.
    let mut counters = DriveCounters::new();
    loop {
        // Tiny critical section: pick a range, then DROP the guard before the
        // `fill_gap` await (the guard does not cross the await point).
        let picked = {
            let mut w = work.lock().await;
            w.pick(i, total_bytes)?
        };
        let Some(range) = picked else {
            break;
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
            work.lock().await.clear(i);
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
                    None,
                    None,
                    // Aggregate solvency: gate this lane on the SHARED pool's
                    // remaining balance (deposit minus every lane's committed),
                    // not this one lane's spend alone.
                    Some(pool_spent),
                );
                tokio::select! {
                    biased;
                    res = fill => match res {
                        Ok(()) => UnitOutcome::Completed,
                        // A fault drops this source; the remainder goes to a peer.
                        Err(_) => UnitOutcome::Faulted,
                    },
                    () = cancelled(&handle) => UnitOutcome::Cancelled,
                    () = watchdog(store, g_start, g_len, unit_deadline) => UnitOutcome::Faulted,
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
                work.lock().await.clear(i);
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
            }
            // Stalled/faulted: re-queue for a different source, then stop.
            Some(UnitOutcome::Faulted) => {
                requeue_missing(store, work, i).await?;
                break;
            }
            Some(UnitOutcome::Completed) => {}
        }
    }
    Ok(())
}

/// Fetch `hash`'s request `[offset, offset+len)` by fanning it out across
/// `lanes`, all writing into the one shared `store` (spec §5.3). Splits the
/// gap-set into bao-aligned segments, drives one worker per lane, and lets a
/// freed lane steal the tail of the largest range still in flight.
///
/// # Per-source payment (ADR 039 § Payment model)
///
/// Each [`SourceLane`] pays with its OWN `(ctx, ledger)`: a voucher is scoped to
/// one on-chain provider and one `(signer, provider)` watermark, so lane `i`'s
/// worker signs against `lanes[i].ctx.provider` and advances `lanes[i].ledger`
/// alone. One shared pool DEPOSIT backs the whole set: every worker gates its
/// draw on `pool_deposit - Σ lanes[j].ledger.committed()` (the aggregate reader
/// built below), so concurrent lanes cannot each independently spend the whole
/// deposit. The gate is evaluated at each `fill_gap` leg boundary; the hard
/// backstop against a node redeeming past the deposit stays on-chain (the pool
/// pays first-come up to its deposit), exactly as on the single-source path.
///
/// Returns once every gap is filled (or a worker faults). Finalization is the
/// caller's job — like [`crate::drive`], this only flushes the present record
/// (spec §5.5 single-writer flush point) once all workers finish.
///
/// # Errors
///
/// An empty `lanes`, a fault from any worker's `fill_gap` (a terminal
/// source/store fault, or a `Refuse`), a segmentation alignment error, or an
/// I/O failure flushing the present record.
#[allow(clippy::too_many_arguments)]
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
    let total_bytes = store.total_bytes();

    let missing = store.missing_ranges(offset, len).await?;
    let gaps = contiguous_byte_ranges(&missing, total_bytes);
    if gaps.is_empty() {
        // Already fully held: persist the present record and return (the caller
        // finalizes).
        store.flush_present_record()?;
        return Ok(());
    }

    // At least one segment; `min` honors `max_sources`, `max(1)` guards a
    // degenerate `max_sources == 0` config from silently fetching nothing.
    let k = ms.max_sources.min(lanes.len()).max(1);
    let segs = initial_segments(&gaps, k, total_bytes)?;

    let work = AsyncMutex::new(Work {
        pending: segs.into_iter().collect(),
        in_flight: vec![None; lanes.len()],
        cancel: (0..lanes.len())
            .map(|_| Arc::new(CancelHandle::new()))
            .collect(),
    });

    // Shared aggregate-solvency reader: the sum, across EVERY lane, of the
    // committed voucher amount — the pool's total spend so far. Each worker
    // subtracts this from the shared deposit to size its own remaining balance,
    // so no lane treats the whole deposit as its own. Cloning the `Arc<PoolLedger>`
    // handles keeps the closure `'static`-free of the borrow on `lanes` and lets
    // every worker share one reader.
    let lane_ledgers: Vec<Arc<PoolLedger>> = lanes.iter().map(|l| Arc::clone(&l.ledger)).collect();
    let pool_spent = move || {
        lane_ledgers
            .iter()
            .map(|l| l.committed().amount)
            .fold(U256::ZERO, U256::saturating_add)
    };
    let pool_spent: &(dyn Fn() -> U256 + Send + Sync) = &pool_spent;

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
            on_progress,
            ms.unit_deadline,
            pool_spent,
        )
    });
    futures_util::future::try_join_all(workers).await?;

    // Single-writer flush point (spec §5.5): every worker has finished, so the
    // in-memory present set is final — persist the `.ranges` record once, off
    // the per-checkpoint hot path.
    store.flush_present_record()?;

    // No worker hangs: they either fill their ranges or drop. If every source
    // dropped with the request still incomplete, surface it as an error rather
    // than returning a false success (or hanging).
    let unfetched = contiguous_byte_ranges(&store.missing_ranges(offset, len).await?, total_bytes);
    if !unfetched.is_empty() {
        let bytes: u64 = unfetched
            .iter()
            .map(|(_, l)| *l)
            .fold(0, u64::saturating_add);
        anyhow::bail!("all sources failed; {bytes} bytes unfetched");
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
    use decdn_incentive::DepositOutcome;

    use super::{MultiSourceConfig, SourceLane, multi_source_fetch};
    use crate::driver::DriveConfig;
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
    fn lane(
        source: &ScriptedSource,
        ledger: Arc<PoolLedger>,
        provider: u8,
    ) -> SourceLane<'_, ScriptedSource> {
        SourceLane {
            source,
            ctx: ctx_for(provider),
            ledger,
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

    /// Stall/fault reassignment: `src_a` faults after ~8 MiB of its segment;
    /// `src_b` holds the whole blob and covers the reassigned remainder. The
    /// blob still assembles byte-identical, and the faulted source's verified
    /// prefix is NOT refetched (the remainder alone is reassigned).
    #[tokio::test]
    async fn stalled_source_tail_is_reassigned_and_fetch_completes() -> anyhow::Result<()> {
        let data = blob(64 * 1024 * 1024);
        let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
        let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
        // src_a faults after 8 MiB of wire on any range longer than that; its
        // 32 MiB initial segment therefore delivers only a ~8 MiB prefix then
        // faults. src_b is healthy.
        let src_a = ScriptedSource::new(data.clone())?
            .with_fault_after(8 * 1024 * 1024, || anyhow::anyhow!("scripted stall"))
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

    /// THE double-pay test: a fast source and an artificially slow one over a
    /// 64 MiB blob, arranged so a steal DEFINITELY fires (the slow source stalls
    /// before its first byte, so the fast source finishes its own segment and
    /// steals the slow source's tail). With steal-cancellation the stolen tail is
    /// fetched by exactly ONE source, so total delivered ≈ the blob size — not
    /// ~1.5–2× it (which is what Task 4's bookkeeping-only steal produced).
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
        // within a small bounded slop of the blob size. Task 4's behaviour would
        // fetch the stolen ~16 MiB tail twice (~80 MiB total); cancellation keeps
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
        // is skipped); the pre-fix code re-fetches the stolen tail, +8 MiB here.
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

    /// Part A — per-provider payment lanes. Two sources with DISTINCT providers
    /// each pay their OWN ledger: the fetch assembles byte-identical, and each
    /// lane's cumulative advances INDEPENDENTLY, tracking exactly the wire that
    /// source delivered (never the peer's). The pre-fix scheduler shared one
    /// `ctx`/`ledger` for every source, so a second provider's bytes were paid on
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
        )
        .await?;
        store.finalize().await?;
        assert_eq!(
            std::fs::read(dir.path().join("b"))?,
            data,
            "assembled byte-identical across two distinct-provider lanes"
        );

        // Each lane's ledger advanced INDEPENDENTLY, and each carries only its OWN
        // ~half of the blob — NOT the pool total. A single shared ledger (the
        // pre-fix bug, forced by the old one-ledger API) would have BOTH sources'
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
        let lanes = vec![
            SourceLane {
                source: &src_a,
                ctx: Arc::new(Mutex::new(ctx_with(0xA1, deposit))),
                ledger: Arc::clone(&ledger_a),
            },
            SourceLane {
                source: &src_b,
                ctx: Arc::new(Mutex::new(ctx_with(0xB2, deposit))),
                ledger: Arc::clone(&ledger_b),
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

        let lanes = vec![
            SourceLane {
                source: &src_a,
                ctx: Arc::new(Mutex::new(ctx_with(0xA1, deposit))),
                ledger: Arc::clone(&ledger_a),
            },
            SourceLane {
                source: &src_b,
                ctx: Arc::new(Mutex::new(ctx_with(0xB2, deposit))),
                ledger: Arc::clone(&ledger_b),
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
}
