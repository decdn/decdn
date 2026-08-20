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

use decdn_bao_range::{AlignedRange, align_range};
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::driver::{DriveConfig, DriveCounters, contiguous_byte_ranges, fill_gap};
use crate::segment::{initial_segments, steal_split};
use crate::source::{BlobSource, Funder, IngestStore};
use crate::{Pacer, PoolContext, PoolLedger, ProgressCallback};

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

        // Drive the owned range OUTSIDE the lock, racing it against a steal
        // cancel and the stall watchdog. Dropping the `fill_gap` future on
        // either leaves the store's checkpointed prefix intact.
        let outcome = {
            let fill = fill_gap(
                store,
                source,
                pacer,
                funder,
                ctx,
                ledger,
                hash,
                r_start,
                r_len,
                total_bytes,
                drive,
                &mut counters,
                on_progress,
                None,
                None,
            );
            tokio::select! {
                biased;
                res = fill => match res {
                    Ok(()) => UnitOutcome::Completed,
                    // A fault drops this source; the remainder goes to a peer.
                    Err(_) => UnitOutcome::Faulted,
                },
                () = cancelled(&handle) => UnitOutcome::Cancelled,
                () = watchdog(store, r_start, r_len, unit_deadline) => UnitOutcome::Faulted,
            }
        };

        match outcome {
            UnitOutcome::Completed => {
                work.lock().await.clear(i);
            }
            UnitOutcome::Cancelled => {
                // Stolen: re-queue the trimmed remainder and stay live.
                requeue_missing(store, work, i).await?;
            }
            UnitOutcome::Faulted => {
                // Stalled/faulted: re-queue for a different source, then stop.
                requeue_missing(store, work, i).await?;
                break;
            }
        }
    }
    Ok(())
}

/// Fetch `hash`'s request `[offset, offset+len)` by fanning it out across
/// `sources`, all writing into the one shared `store` (spec §5.3). Splits the
/// gap-set into bao-aligned segments, drives one worker per source, and lets a
/// freed source steal the tail of the largest range still in flight.
///
/// Returns once every gap is filled (or a worker faults). Finalization is the
/// caller's job — like [`crate::drive`], this only flushes the present record
/// (spec §5.5 single-writer flush point) once all workers finish.
///
/// # Errors
///
/// An empty `sources`, a fault from any worker's `fill_gap` (a terminal
/// source/store fault, or a `Refuse`), a segmentation alignment error, or an
/// I/O failure flushing the present record.
#[allow(clippy::too_many_arguments)]
pub async fn multi_source_fetch<St, S, P, F>(
    store: &St,
    sources: &[&S],
    pacer: &P,
    funder: &F,
    ctx: &Arc<Mutex<PoolContext>>,
    ledger: &Arc<PoolLedger>,
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
    if sources.is_empty() {
        anyhow::bail!("multi_source_fetch requires at least one source");
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
    let k = ms.max_sources.min(sources.len()).max(1);
    let segs = initial_segments(&gaps, k, total_bytes)?;

    let work = AsyncMutex::new(Work {
        pending: segs.into_iter().collect(),
        in_flight: vec![None; sources.len()],
        cancel: (0..sources.len())
            .map(|_| Arc::new(CancelHandle::new()))
            .collect(),
    });

    let workers = sources.iter().enumerate().map(|(i, source)| {
        run_worker(
            i,
            store,
            *source,
            pacer,
            funder,
            ctx,
            ledger,
            hash,
            total_bytes,
            drive,
            &work,
            on_progress,
            ms.unit_deadline,
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

    use super::{MultiSourceConfig, multi_source_fetch};
    use crate::driver::DriveConfig;
    use crate::pacer::BudgetPacer;
    use crate::source::{FakeFunder, ScriptedSource};
    use crate::{ClientRangedStore, Cumulative, PoolContext, PoolLedger};
    use decdn_bao_range::RangedStore;

    /// A deterministic blob of `len` bytes — same synth as the `source` unit
    /// tests, so a `ScriptedSource` over it yields verifiable wire.
    fn blob(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    /// A healthy buyer context: a huge deposit so the pacer never has to top up.
    fn healthy_ctx() -> PoolContext {
        let signer = PrivateKeySigner::random();
        PoolContext {
            pool_id: B256::ZERO,
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
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        // Both sources pay from the SAME pool/ledger (single deposit backs the
        // whole set) and hold the whole blob.
        let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let ctx = Arc::new(Mutex::new(healthy_ctx()));
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        multi_source_fetch(
            &store,
            &[&src_a, &src_b],
            &pacer,
            &funder,
            &ctx,
            &ledger,
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
        let ctx = Arc::new(Mutex::new(healthy_ctx()));
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        multi_source_fetch(
            &store,
            &[&src],
            &pacer,
            &funder,
            &ctx,
            &ledger,
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
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        // src_a faults after 8 MiB of wire on any range longer than that; its
        // 32 MiB initial segment therefore delivers only a ~8 MiB prefix then
        // faults. src_b is healthy.
        let src_a = ScriptedSource::new(data.clone())?
            .with_fault_after(8 * 1024 * 1024, || anyhow::anyhow!("scripted stall"))
            .paying(Arc::clone(&ledger));
        let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger));
        let root = src_a.root();
        let total = src_a.total_bytes();
        let (store, dir) = fresh_store(root, total);
        let ctx = Arc::new(Mutex::new(healthy_ctx()));
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        multi_source_fetch(
            &store,
            &[&src_a, &src_b],
            &pacer,
            &funder,
            &ctx,
            &ledger,
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
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_fast = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger));
        // Every leg the slow source opens stalls 200 ms before its first byte —
        // long enough that the fast source (only cooperative yields) always
        // finishes first and steals, deterministically forcing the steal path.
        let src_slow = ScriptedSource::new(data.clone())?
            .slow_to_start(Duration::from_millis(200))
            .paying(Arc::clone(&ledger));
        let root = src_fast.root();
        let (store, dir) = fresh_store(root, total);
        let ctx = Arc::new(Mutex::new(healthy_ctx()));
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        multi_source_fetch(
            &store,
            &[&src_fast, &src_slow],
            &pacer,
            &funder,
            &ctx,
            &ledger,
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

    /// Total-failure guard: every source faults immediately, so
    /// `multi_source_fetch` returns an error WITHOUT hanging (the whole body runs
    /// under a timeout).
    #[tokio::test]
    async fn all_sources_failing_errors_without_hang() -> anyhow::Result<()> {
        let data = blob(32 * 1024 * 1024);
        let total = data.len() as u64;
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src_a = ScriptedSource::new(data.clone())?
            .with_fault_after(0, || anyhow::anyhow!("immediate fault a"))
            .paying(Arc::clone(&ledger));
        let src_b = ScriptedSource::new(data.clone())?
            .with_fault_after(0, || anyhow::anyhow!("immediate fault b"))
            .paying(Arc::clone(&ledger));
        let root = src_a.root();
        let (store, _dir) = fresh_store(root, total);
        let ctx = Arc::new(Mutex::new(healthy_ctx()));
        let funder = FakeFunder::new(0, DepositOutcome::Added(U256::ZERO));
        let pacer = BudgetPacer::new();

        let result = tokio::time::timeout(
            Duration::from_secs(30),
            multi_source_fetch(
                &store,
                &[&src_a, &src_b],
                &pacer,
                &funder,
                &ctx,
                &ledger,
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
}
