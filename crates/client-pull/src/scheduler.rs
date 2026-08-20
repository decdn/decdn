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
//! range and never touches the node's serve path. Stall/fault reassignment
//! (`unit_deadline`) is a later concern — here a worker that errors propagates
//! the error and fails the fetch.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use decdn_bao_range::AlignedRange;
use tokio::sync::Mutex as AsyncMutex;

use crate::driver::{DriveConfig, DriveCounters, contiguous_byte_ranges, fill_gap};
use crate::segment::{initial_segments, steal_split};
use crate::source::{BlobSource, Funder, IngestStore};
use crate::{Pacer, PoolContext, PoolLedger, ProgressCallback};

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
            && let Some(Some((start, len))) = self.in_flight.get_mut(idx)
            && half.fetch_start() > *start
        {
            *len = half.fetch_start() - *start;
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

/// One worker future per source: loop picking a range and driving `fill_gap`
/// over it until [`Work::pick`] returns `None`. Exactly one outstanding range
/// at a time (the loop drives one `fill_gap` to completion before the next
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
) -> anyhow::Result<()>
where
    St: IngestStore,
    S: BlobSource,
    P: Pacer,
    F: Funder,
{
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

        // Drive the owned range OUTSIDE the lock. A worker error propagates and
        // fails the whole fetch (reassignment is a later task).
        fill_gap(
            store,
            source,
            pacer,
            funder,
            ctx,
            ledger,
            hash,
            range.fetch_start(),
            range.fetch_len(),
            total_bytes,
            drive,
            &mut counters,
            on_progress,
            None,
            None,
        )
        .await?;

        work.lock().await.clear(i);
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
        )
    });
    futures_util::future::try_join_all(workers).await?;

    // Single-writer flush point (spec §5.5): every worker has finished, so the
    // in-memory present set is final — persist the `.ranges` record once, off
    // the per-checkpoint hot path.
    store.flush_present_record()?;
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
}
