use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use decdn_incentive::LaneKey;
use decdn_protocol::client::StreamError;
use decdn_protocol::{Coverage, DISCOVERY_BLOCK_BYTES, num_blocks};

use super::{AcquireEnv, AcquireTarget, ConsumptionPacing, LANE_WATCHDOG, LaneLease, acquire};
use crate::driver::{PoolExhausted, ranges_content_len};
use crate::fault::{FatalScope, Fault, classify};
use crate::health::{Health, PeerHealth};
use crate::ledgers::{LaneHandle, LaneLedgers};
use crate::pacer::{BudgetPacer, PaceDecision, PaceState, Pacer};
use crate::recovery::RecoveryGate;
use crate::source::{BlobSource, FakeFunder, Funder, Recovery, ScriptedSource, ctx_with};
use crate::source_set::{NoAffordableSource, NoSourceHasBlob, SourceSet, StaticSources};
use crate::stop::{GaveUp, StopPolicy};
use crate::streamer::StreamCandidate;
use crate::{
    ClientRangedStore, Cumulative, PoolContext, PoolLedger, UpstreamRefused,
    UpstreamVoucherRejected,
};
use decdn_bao_range::RangedStore;
use std::assert_matches;

/// A deterministic blob of `len` bytes: the same synth as the `source` unit
/// tests, so a `ScriptedSource` over it yields verifiable wire.
fn blob(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// A shared handle to a healthy buyer context (huge deposit so the pacer never
/// tops up) paying the given `provider`.
fn ctx_for(provider: u8) -> Arc<Mutex<PoolContext>> {
    Arc::new(Mutex::new(ctx_with(provider, U256::from(u128::MAX))))
}

/// One paid source for [`StaticSources`]: `source` paying `ledger`, with a
/// context pinned to `provider` and a huge deposit. Hand it a clone of a
/// [`ScriptedSource`] and keep the original: clones share the opened and
/// delivered counters the assertions read.
fn candidate<S>(
    source: S,
    ledger: Arc<PoolLedger>,
    provider: u8,
    coverage: Option<Coverage>,
) -> StreamCandidate<S> {
    candidate_ctx(source, ledger, ctx_for(provider), coverage)
}

/// [`candidate`] with an explicit pool context.
fn candidate_ctx<S>(
    source: S,
    ledger: Arc<PoolLedger>,
    ctx: Arc<Mutex<PoolContext>>,
    coverage: Option<Coverage>,
) -> StreamCandidate<S> {
    StreamCandidate {
        source,
        ctx,
        ledger,
        coverage,
        lease: LaneLease::new(()),
        widen: None,
    }
}

/// A fresh funding recovery gate for one fetch.
fn recovery_gate() -> RecoveryGate {
    RecoveryGate::new()
}

/// A funder with no way to add funds: a funding recovery step ends the fetch
/// "funding needed".
fn no_funding() -> FakeFunder {
    FakeFunder::new(Recovery::Unavailable)
}

/// A `.partial` store whose tempdir path is returned so the test can read
/// the finalized blob back off disk.
fn fresh_store(root: [u8; 32], total: u64) -> (ClientRangedStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tmp dir");
    let store = ClientRangedStore::create(dir.path(), "b", root, total).expect("create");
    (store, dir)
}

/// Everything [`run_acquire_with`] takes beyond the target and the sources.
struct Knobs<'a> {
    max_lanes: usize,
    give_up_after: Option<Duration>,
    on_progress: Option<&'a (dyn Fn(u64, u64) + Send + Sync)>,
    ledgers: Option<&'a LaneLedgers>,
    pacing: Option<&'a ConsumptionPacing<'a>>,
    health: Arc<PeerHealth>,
    max_blob_bytes: u64,
}

impl Knobs<'_> {
    fn lanes(max_lanes: usize) -> Self {
        Self {
            max_lanes,
            give_up_after: None,
            on_progress: None,
            ledgers: None,
            pacing: None,
            health: Arc::default(),
            max_blob_bytes: 0,
        }
    }
}

/// Acquire the whole `root` blob from `provider`'s candidates.
async fn run_acquire_with<S: BlobSource>(
    store: &ClientRangedStore,
    provider: &StaticSources<S>,
    root: [u8; 32],
    pacer: &impl Pacer,
    funder: &impl Funder,
    knobs: Knobs<'_>,
) -> anyhow::Result<()> {
    let total = store.total_bytes();
    let mut set = SourceSet::new(provider, root, knobs.health, provider.holders());
    let stop = StopPolicy::new(
        false,
        knobs.give_up_after.or(Some(Duration::from_hours(1))),
        Arc::default(),
    );
    let gate = recovery_gate();
    acquire(
        AcquireTarget {
            store,
            hash: root,
            total_bytes: total,
            ranges: &[(0, total)],
        },
        &mut set,
        &AcquireEnv {
            pacer,
            funder,
            recovery: &gate,
            credentials: None,
            max_lanes: knobs.max_lanes,
            stop: &stop,
            on_progress: knobs.on_progress,
            ledgers: knobs.ledgers,
            pacing: knobs.pacing,
            max_blob_bytes: knobs.max_blob_bytes,
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_acquire(
    store: &ClientRangedStore,
    provider: &StaticSources<ScriptedSource>,
    root: [u8; 32],
    total: u64,
    pacer: &impl Pacer,
    funder: &impl Funder,
    max_lanes: usize,
    give_up_after: Option<Duration>,
) -> anyhow::Result<()> {
    assert_eq!(store.total_bytes(), total);
    run_acquire_with(
        store,
        provider,
        root,
        pacer,
        funder,
        Knobs {
            give_up_after,
            ..Knobs::lanes(max_lanes)
        },
    )
    .await
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
    let provider = StaticSources::new(vec![
        candidate(src_a.clone(), ledger_a, 0xA1, None),
        candidate(src_b.clone(), ledger_b, 0xB2, None),
    ])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        4,
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
/// whole-blob size.
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

    let samples: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let cb_samples = Arc::clone(&samples);
    let on_progress: Box<crate::ProgressCallback> = Box::new(move |received, expected| {
        if let Ok(mut s) = cb_samples.lock() {
            s.push((received, expected));
        }
    });

    let provider = StaticSources::new(vec![
        candidate(src_a, ledger_a, 0xA1, None),
        candidate(src_b, ledger_b, 0xB2, None),
    ])?;
    run_acquire_with(
        &store,
        &provider,
        root,
        &BudgetPacer::new(),
        &no_funding(),
        Knobs {
            on_progress: Some(&on_progress),
            ..Knobs::lanes(4)
        },
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
            "progress regressed: {received} after {prev}: the bar jumped backwards"
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

/// A resumed fetch surfaces the already-present base on the bar BEFORE any
/// lane opens a leg: the first reported position is the held prefix's
/// content length, not `0`.
#[tokio::test]
async fn resume_base_is_reported_before_the_first_chunk() -> anyhow::Result<()> {
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
    let (h, reader) = seed.open(root, held.clone()).await?;
    store
        .ingest_stream(&held, reader, None, h.total_bytes)
        .await?;
    let base_present = ranges_content_len(&store.present_ranges().await?, total);
    assert_eq!(
        base_present,
        2 * CHUNK_GROUP_BYTES,
        "scenario: two groups held"
    );

    let samples: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let cb_samples = Arc::clone(&samples);
    let on_progress: Box<crate::ProgressCallback> = Box::new(move |received, expected| {
        if let Ok(mut s) = cb_samples.lock() {
            s.push((received, expected));
        }
    });

    let provider = StaticSources::new(vec![
        candidate(src_a, ledger_a, 0xA1, None),
        candidate(src_b, ledger_b, 0xB2, None),
    ])?;
    run_acquire_with(
        &store,
        &provider,
        root,
        &BudgetPacer::new(),
        &no_funding(),
        Knobs {
            on_progress: Some(&on_progress),
            ..Knobs::lanes(4)
        },
    )
    .await?;

    let samples = samples.lock().expect("samples lock").clone();
    let first = *samples.first().expect("at least one progress sample");
    assert_eq!(
        first,
        (base_present, total),
        "the first reported position must be the resume base, emitted before \
         any lane opens a leg"
    );
    Ok(())
}

/// Fan-out geometry (#1506): a large multi-block gap across N full holders
/// seeds ~N contiguous spans (one per holder), never `blocks × N`. Every
/// holder still contributes.
///
/// The check reads each holder's FIRST open, which is its seeded span: every
/// worker picks from `pending` before any worker can finish. Later opens are
/// tail steals and requeues, whose count depends on how the lanes interleave.
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
    let provider = StaticSources::new(vec![
        candidate(src_a.clone(), ledger_a, 0xA1, None),
        candidate(src_b.clone(), ledger_b, 0xB2, None),
        candidate(src_c.clone(), ledger_c, 0xC3, None),
    ])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        4,
        None,
    )
    .await?;
    store.finalize().await?;
    assert_eq!(
        std::fs::read(dir.path().join("b"))?,
        data,
        "assembled byte-identical across the fan-out"
    );
    // N = 3 spans of 64 MiB, one per holder.
    for (name, src) in [("a", &src_a), ("b", &src_b), ("c", &src_c)] {
        let first = src.opened_ranges().first().copied();
        assert!(
            first.is_some_and(|(_, len)| len >= total / 4),
            "fan-out seeds ~N contiguous spans, not blocks × N: holder {name}'s \
             first open is {first:?} of a {total}-byte blob"
        );
    }
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
/// covered: the same shorthand `coverage_plan`'s own tests use.
fn cov(n: u32, blocks: &[u32]) -> Coverage {
    Coverage::from_block_indices(n, blocks.iter().copied())
}

/// A lane's coverage logs as inclusive block runs, a lone block as itself.
#[test]
fn coverage_logs_as_block_runs() {
    assert_eq!(
        super::block_runs(&cov(70, &[0, 1, 2, 5, 42, 43, 67])),
        "0-2,5,42-43,67"
    );
    assert_eq!(super::block_runs(&cov(4, &[])), "none");
    assert_eq!(super::block_runs(&Coverage::full(3)), "0-2");
}

/// Disjoint coverage (#1506): source A holds only discovery block 0, source
/// B holds only block 1. Every byte range A opens falls inside block 0 and
/// every range B opens falls inside block 1: neither is EVER handed the
/// other's block.
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

    let n = num_blocks(total);
    let provider = StaticSources::new(vec![
        candidate(src_a.clone(), ledger_a, 0xA1, Some(cov(n, &[0]))),
        candidate(src_b.clone(), ledger_b, 0xB2, Some(cov(n, &[1]))),
    ])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        4,
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
    assert!(src_a.opened_bytes() > 0, "A must have served block 0");
    assert!(src_b.opened_bytes() > 0, "B must have served block 1");
    Ok(())
}

/// A third, all-ones holder serves the one block neither of the two
/// partial holders covers (#1506). A holds only block 0, B holds only
/// block 1, and only O (full coverage) can serve block 2, so O, and only O,
/// opens bytes in block 2.
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

    let n = num_blocks(total);
    let provider = StaticSources::new(vec![
        candidate(src_a.clone(), ledger_a, 0xA1, Some(cov(n, &[0]))),
        candidate(src_b.clone(), ledger_b, 0xB2, Some(cov(n, &[1]))),
        candidate(src_o.clone(), ledger_o, 0xC3, Some(Coverage::full(n))),
    ])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        4,
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

/// Coverage-constrained steal (#1506): a fast source that covers ONLY
/// block 0 finishes its own segment quickly, while the block-1-only holder
/// is slow to start. The only range left in flight (block 1) is outside the
/// fast source's coverage, so it PARKS instead of stealing work it cannot
/// serve. The slow source still finishes block 1 on its own.
#[tokio::test]
async fn coverage_constrained_steal_parks_instead_of_taking_uncoverable_work() -> anyhow::Result<()>
{
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

    let n = num_blocks(total);
    let provider = StaticSources::new(vec![
        candidate(src_fast.clone(), ledger_fast, 0xA1, Some(cov(n, &[0]))),
        candidate(src_slow.clone(), ledger_slow, 0xB2, Some(cov(n, &[1]))),
    ])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        2,
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
        "the fast source covers only block 0 and must never open past it: it must \
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
    // With a single source the loop degrades to one segment, no steal, and
    // still assembles the whole blob byte-identical.
    let data = blob(20 * 1024 * 1024);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let src = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger));
    let root = src.root();
    let total = src.total_bytes();
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![candidate(src.clone(), ledger, 0xA1, None)])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        4,
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
#[tokio::test(start_paused = true)]
async fn faulted_source_tail_is_reassigned_and_fetch_completes() -> anyhow::Result<()> {
    let data = blob(64 * 1024 * 1024);
    let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
    // src_a returns an `Err` after 8 MiB of wire on any range longer than
    // that; its 32 MiB initial segment therefore delivers only a ~8 MiB
    // prefix then faults. This is the `fill_gap`-error arm, NOT the stall
    // watchdog. src_b is healthy.
    let src_a = ScriptedSource::new(data.clone())?
        .with_fault_after(8 * 1024 * 1024, || anyhow::anyhow!("scripted fault"))
        .paying(Arc::clone(&ledger_a));
    let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
    let root = src_a.root();
    let total = src_a.total_bytes();
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate(src_a.clone(), ledger_a, 0xA1, None),
        candidate(src_b.clone(), ledger_b, 0xB2, None),
    ])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        4,
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

/// A lane whose paced draws leave gaps shorter than the lane watchdog, and
/// that reports no progress callback, is never reassigned: the watchdog
/// judges the verified bytes `fill_gap` counts as each leaf lands, not the
/// store's 4 MiB checkpoints (#2209). Each wait here (the first read, then a
/// pause after 1 MiB) is half the watchdog, and the first watchdog sample
/// falls while bytes are still missing.
#[tokio::test(start_paused = true)]
async fn a_lane_pausing_under_the_deadline_is_not_reassigned() -> anyhow::Result<()> {
    let data = blob(16 * 1024 * 1024);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let src = ScriptedSource::new(data.clone())?
        .slow_to_start(LANE_WATCHDOG / 2)
        .stall_after(1024 * 1024, LANE_WATCHDOG / 2)
        .paying(Arc::clone(&ledger));
    let root = src.root();
    let total = src.total_bytes();
    let (store, dir) = fresh_store(root, total);
    let health: Arc<PeerHealth> = Arc::default();
    let provider = StaticSources::new(vec![candidate(src, ledger, 0xA1, None)])?;
    run_acquire_with(
        &store,
        &provider,
        root,
        &BudgetPacer::new(),
        &no_funding(),
        Knobs {
            health: Arc::clone(&health),
            ..Knobs::lanes(4)
        },
    )
    .await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert_eq!(
        health.health(Address::repeat_byte(0xA1)),
        Health::Healthy { streak: 0 },
        "the lane never tripped the watchdog"
    );
    Ok(())
}

/// Stamps the moment it drops, so a test can see when a lane let go of it.
struct DropStamp(Arc<Mutex<Option<std::time::Instant>>>);

impl Drop for DropStamp {
    fn drop(&mut self) {
        if let Ok(mut at) = self.0.lock() {
            *at = Some(std::time::Instant::now());
        }
    }
}

/// Every started lane's lease, the faulted lane's included, is held until
/// the acquire returns and released by then, for lanes without a
/// [`super::LaneWiden`]: such a lane has no way to take a stream back.
#[tokio::test(start_paused = true)]
async fn every_lease_is_released_when_acquire_returns() -> anyhow::Result<()> {
    let data = blob(16 * 1024 * 1024);
    let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
    let src_a = ScriptedSource::new(data.clone())?
        .with_fault_after(1024 * 1024, || anyhow::anyhow!("scripted fault"))
        .paying(Arc::clone(&ledger_a));
    let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
    let root = src_a.root();
    let total = src_a.total_bytes();
    let (store, _dir) = fresh_store(root, total);
    let dropped_a = Arc::new(Mutex::new(None));
    let dropped_b = Arc::new(Mutex::new(None));
    let mut lane_a = candidate(src_a.clone(), ledger_a, 0xA1, None);
    lane_a.lease = LaneLease::new(DropStamp(Arc::clone(&dropped_a)));
    let mut lane_b = candidate(src_b.clone(), ledger_b, 0xB2, None);
    lane_b.lease = LaneLease::new(DropStamp(Arc::clone(&dropped_b)));
    let provider = StaticSources::new(vec![lane_a, lane_b])?;
    // Past 12 MiB of the 16 MiB blob, lane a (which faults 1 MiB into every
    // open) has faulted; its lease must still be held.
    let a_released_mid_fetch: Arc<Mutex<Vec<bool>>> = Arc::default();
    let on_progress = {
        let seen = Arc::clone(&a_released_mid_fetch);
        let dropped_a = Arc::clone(&dropped_a);
        move |position: u64, _total: u64| {
            if position >= 12 * 1024 * 1024
                && let (Ok(mut seen), Ok(at)) = (seen.lock(), dropped_a.lock())
            {
                seen.push(at.is_some());
            }
        }
    };
    run_acquire_with(
        &store,
        &provider,
        root,
        &BudgetPacer::new(),
        &no_funding(),
        Knobs {
            on_progress: Some(&on_progress),
            ..Knobs::lanes(4)
        },
    )
    .await?;
    let ended = std::time::Instant::now();
    let seen = a_released_mid_fetch.lock().expect("seen lock").clone();
    assert!(!seen.is_empty(), "progress passed lane a's fault point");
    assert!(
        seen.iter().all(|released| !released),
        "the faulted lane's lease is held until the acquire returns"
    );
    assert!(
        src_a.delivered_bytes() > 0 && src_a.delivered_bytes() < total,
        "lane a delivered a prefix and faulted"
    );
    let at_a = dropped_a
        .lock()
        .expect("stamp lock")
        .expect("lane a let go");
    let at_b = dropped_b
        .lock()
        .expect("stamp lock")
        .expect("lane b let go");
    assert!(
        at_a <= ended && at_b <= ended,
        "both lanes let go before the acquire returns"
    );
    assert!(
        src_b.delivered_bytes() > src_a.delivered_bytes(),
        "lane b fetched the reassigned tail after lane a faulted"
    );
    Ok(())
}

/// An acquire that its caller drops releases each lane's lease with it
/// rather than leaving it held for the caller.
#[tokio::test]
async fn a_dropped_acquire_releases_every_lease() -> anyhow::Result<()> {
    let data = blob(8 * 1024 * 1024);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let src = ScriptedSource::new(data)?
        .stall_after(0, Duration::from_hours(1))
        .paying(Arc::clone(&ledger));
    let root = src.root();
    let total = src.total_bytes();
    let (store, _dir) = fresh_store(root, total);
    let dropped = Arc::new(Mutex::new(None));
    let mut lane = candidate(src, ledger, 0xA1, None);
    lane.lease = LaneLease::new(DropStamp(Arc::clone(&dropped)));
    let provider = StaticSources::new(vec![lane])?;
    let (pacer, funder) = (BudgetPacer::new(), no_funding());
    let fetch = run_acquire(&store, &provider, root, total, &pacer, &funder, 4, None);
    tokio::select! {
        fetched = fetch => panic!("a wedged lane cannot finish: {fetched:?}"),
        () = tokio::time::sleep(Duration::from_millis(50)) => {}
    }
    assert!(
        dropped.lock().expect("stamp lock").is_some(),
        "the dropped acquire's lease is released"
    );
    Ok(())
}

/// A parked lane keeps every `pending` entry: a cooling lane may return,
/// and a rediscovered holder may cover what no live lane does.
#[test]
fn park_keeps_every_pending_entry() -> anyhow::Result<()> {
    use std::collections::VecDeque;

    use decdn_bao_range::align_range;

    use super::Work;

    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let in_block0 = align_range(0, 1, total)?;
    let in_block1 = align_range(DISCOVERY_BLOCK_BYTES, 1, total)?;
    let mut work = Work::new(VecDeque::from(vec![in_block0, in_block1]), false);
    let a = work.add_lane(Some(cov(2, &[0])), total);
    let b = work.add_lane(Some(cov(2, &[1])), total);

    work.park(a);
    work.park(b);
    let starts: Vec<u64> = work
        .pending
        .iter()
        .map(decdn_bao_range::AlignedRange::fetch_start)
        .collect();
    assert_eq!(
        starts,
        vec![0, DISCOVERY_BLOCK_BYTES],
        "every entry stays queued"
    );
    assert!(work.uncovered(total), "no running lane covers the queue");
    work.revive(a);
    assert!(work.uncovered(total), "block 1 still has no running lane");
    work.revive(b);
    assert!(!work.uncovered(total));
    Ok(())
}

/// A work-state with three lanes over one discovery block: lane 0 holds
/// the whole block on unit 1, lanes 1 and 2 hold nothing.
fn one_victim_work(total: u64, coverage: &Coverage) -> super::Work {
    use std::collections::VecDeque;

    use super::{CancelHandle, Unit, Work};

    Work {
        pending: VecDeque::new(),
        in_flight: vec![Some((0, total)), None, None],
        cancel: (0..3).map(|_| Arc::new(CancelHandle::new())).collect(),
        live: (0..3).map(|_| Arc::new(Unit::new())).collect(),
        rates: vec![None; 3],
        alive: vec![true, true, true],
        units: vec![1, 0, 0],
        coverage: vec![coverage.clone(), coverage.clone(), coverage.clone()],
        measured: vec![true; 3],
        lane_of: (0..3).collect(),
        extra: vec![false; 3],
        providers: vec![None; 3],
        no_uncovered: vec![false; 3],
        front_first: false,
    }
}

/// A steal trims the victim and lowers the victim's end to the split, under
/// one lock, and never cancels it: the victim keeps its open stream and
/// stops there. A second steal lowers the end again. Only
/// `cancel_victim` cancels, and only the unit it names.
#[tokio::test]
async fn a_steal_lowers_the_victims_end_and_does_not_cancel_it() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering;

    let total = DISCOVERY_BLOCK_BYTES;
    let coverage = cov(1, &[0]);
    // Nothing is present: every byte in flight is missing.
    let all = [(0, total)];
    let flag = |w: &super::Work, slot: usize| {
        w.cancel
            .get(slot)
            .is_some_and(|h| h.flag.load(Ordering::Acquire))
    };
    let end = |w: &super::Work, slot: usize| {
        w.live
            .get(slot)
            .map_or(u64::MAX, |unit| unit.stop_at.load(Ordering::Acquire))
    };
    // The slot whose range ends where a stolen tail starts.
    let victim_of = |w: &super::Work, split: u64| {
        (0..w.in_flight.len()).find(|&slot| {
            w.in_flight
                .get(slot)
                .copied()
                .flatten()
                .is_some_and(|(start, len)| start + len == split)
        })
    };

    let mut work = one_victim_work(total, &coverage);
    let first = work
        .pick(1, total, &coverage, true, &all)?
        .ok_or_else(|| anyhow::anyhow!("worker 1 must steal worker 0's tail"))?;
    let split = first.range.fetch_start();
    assert_eq!(work.in_flight.first().copied().flatten(), Some((0, split)));
    assert_eq!(end(&work, 0), split, "the victim must stop at the split");
    assert!(!flag(&work, 0), "a steal must not cancel the victim");
    assert_eq!(
        first.unit.stop_at.load(Ordering::Acquire),
        u64::MAX,
        "the stealer's own end is not lowered"
    );

    // A second freed lane steals from one of the two: that victim's end
    // drops to the new split, still without a cancel.
    let second = work
        .pick(2, total, &coverage, true, &all)?
        .ok_or_else(|| anyhow::anyhow!("worker 2 must steal again"))?;
    let split = second.range.fetch_start();
    let victim = victim_of(&work, split)
        .ok_or_else(|| anyhow::anyhow!("the second steal trims a victim"))?;
    assert_eq!(end(&work, victim), split);
    assert!(
        !flag(&work, victim),
        "a second steal must not cancel either"
    );

    // A cancel names a unit: a victim on a new unit keeps it, and an idle
    // victim has nothing to cancel.
    let mut work = one_victim_work(total, &coverage);
    work.clear(0)?;
    work.pending
        .push_back(decdn_bao_range::align_range(0, 1, total)?);
    work.pick(0, total, &coverage, true, &all)?
        .ok_or_else(|| anyhow::anyhow!("the victim must pick its new unit"))?;
    work.cancel_victim(0, 1);
    assert!(
        !flag(&work, 0),
        "a victim on a new unit must not be cancelled"
    );
    work.cancel_victim(0, 2);
    assert!(flag(&work, 0), "a cancel of the unit in flight fires");
    let mut work = one_victim_work(total, &coverage);
    work.clear(0)?;
    work.cancel_victim(0, 1);
    assert!(!flag(&work, 0), "an idle victim must not be cancelled");
    Ok(())
}

/// A victim's received prefix counts as delivered: the split lies past its
/// verified frontier even when the store has not made those bytes durable,
/// so the stealer never takes bytes the victim already received.
#[tokio::test]
async fn a_steal_splits_past_the_victims_received_frontier() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering;

    let total = DISCOVERY_BLOCK_BYTES;
    let coverage = cov(1, &[0]);
    let all = [(0, total)];
    let mut work = one_victim_work(total, &coverage);
    // The victim received three quarters of its range; none of it is
    // durable yet.
    let frontier = total / 4 * 3;
    if let Some(unit) = work.live.first() {
        unit.progress.frontier.store(frontier, Ordering::Release);
    }
    let picked = work
        .pick(1, total, &coverage, true, &all)?
        .ok_or_else(|| anyhow::anyhow!("worker 1 must steal"))?;
    // No rates: the victim keeps half of what it has not received.
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;
    let want = (frontier + (total - frontier) / 2) / group * group;
    assert_eq!(picked.range.fetch_start(), want);
    assert!(picked.range.fetch_start() > frontier);
    Ok(())
}

/// A unit's rate counts from its first verified byte and leaves out the
/// time it spent parked on the consumer: a slow open, a cold first byte,
/// or the consumer's pace does not read as a slow source.
#[tokio::test(start_paused = true)]
async fn a_units_rate_runs_from_its_first_byte_and_skips_parked_time() {
    use std::sync::atomic::Ordering;

    let unit = super::Unit::new();
    // 5 s to the first byte: no rate before it.
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(unit.rate(), None);
    unit.progress
        .first_byte
        .get_or_init(tokio::time::Instant::now);
    tokio::time::advance(Duration::from_secs(1)).await;
    unit.progress.verified.store(10 * MIB, Ordering::Relaxed);
    assert_eq!(
        unit.rate(),
        Some(10 * MIB),
        "the open stays out of the rate"
    );
    // 3 s parked on the consumer, then 1 s more streaming 10 MiB.
    tokio::time::advance(Duration::from_secs(3)).await;
    unit.parked.store(
        u64::try_from(Duration::from_secs(3).as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    unit.progress.verified.store(20 * MIB, Ordering::Relaxed);
    assert_eq!(
        unit.rate(),
        Some(10 * MIB),
        "parked time stays out of the rate"
    );
}

/// A fast stealer takes a share of the victim's missing remainder in
/// proportion to the two rates, so a slow victim keeps only what it can
/// fetch while the stealer fetches the rest.
#[tokio::test(start_paused = true)]
async fn a_steal_splits_by_the_two_lanes_rates() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering;

    let total = DISCOVERY_BLOCK_BYTES;
    let coverage = cov(1, &[0]);
    let all = [(0, total)];
    let mut work = one_victim_work(total, &coverage);
    // The victim verified 1 MiB in the second after its first byte; the
    // stealer ran its last unit at 3 MiB/s.
    if let Some(unit) = work.live.first() {
        unit.progress
            .first_byte
            .get_or_init(tokio::time::Instant::now);
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    if let Some(unit) = work.live.first() {
        unit.progress.verified.store(1024 * 1024, Ordering::Relaxed);
    }
    work.record_rate(1, Some(3 * 1024 * 1024));

    let picked = work
        .pick(1, total, &coverage, true, &all)?
        .ok_or_else(|| anyhow::anyhow!("worker 1 must steal"))?;
    // The victim keeps a quarter of the block, rounded down to a group.
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;
    assert_eq!(picked.range.fetch_start(), (total / 4) / group * group);
    assert_eq!(picked.range.fetch_end(), total);
    Ok(())
}

/// #2303: a lane that covers only the tail blocks of a busy lane's range
/// steals inside those blocks. A lane whose coverage misses the range's
/// last block finds no work there and takes nothing.
#[tokio::test]
async fn a_partial_holder_steals_the_covered_tail_of_a_busy_leg() -> anyhow::Result<()> {
    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let mut work = one_victim_work(total, &cov(3, &[0, 1, 2]));
    let all = [(0, total)];

    let front_only = cov(3, &[0, 1]);
    assert!(!work.has_work_for(&front_only, total, false));
    assert!(work.pick(2, total, &front_only, true, &all)?.is_none());

    let tail_only = cov(3, &[1, 2]);
    assert!(work.has_work_for(&tail_only, total, false));
    let picked = work
        .pick(1, total, &tail_only, true, &all)?
        .ok_or_else(|| anyhow::anyhow!("worker 1 covers the victim's tail and must steal"))?;
    // No rates: the victim keeps half, which lies inside block 1.
    assert_eq!(picked.range.fetch_start(), total / 2);
    assert_eq!(picked.range.fetch_end(), total);
    assert_eq!(
        work.in_flight.first().copied().flatten(),
        Some((0, total / 2))
    );
    Ok(())
}

/// A three-block work-state for #2348's slow-victim steal: lane 0 runs
/// the whole blob at `victim_mib_s` MiB/s over `over` since its first
/// byte, its frontier at what it verified; lane 1 covers blocks 0 and 1
/// only, so it does not cover the victim range's last block, and
/// ran its last unit at `stealer_mib_s` MiB/s. Returns the work-state,
/// the stealer's coverage and the victim's frontier.
async fn slow_victim_work(
    victim_mib_s: u64,
    over: Duration,
    stealer_mib_s: Option<u64>,
) -> (super::Work, Coverage, u64) {
    use std::sync::atomic::Ordering;

    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let mut work = one_victim_work(total, &cov(3, &[0, 1, 2]));
    let verified = victim_mib_s * MIB * over.as_secs();
    if let Some(unit) = work.live.first() {
        unit.progress
            .first_byte
            .get_or_init(tokio::time::Instant::now);
    }
    tokio::time::advance(over).await;
    if let Some(unit) = work.live.first() {
        unit.progress.verified.store(verified, Ordering::Relaxed);
        unit.progress.frontier.store(verified, Ordering::Release);
    }
    work.record_rate(1, stealer_mib_s.map(|r| r * MIB));
    (work, cov(3, &[0, 1]), verified)
}

/// #2348: a lane that covers no block of a busy range's covered suffix
/// steals that range's tail when the range's lane runs at a quarter of
/// its rate or less. The split is by the two rates, past the victim's
/// frontier; the victim is trimmed and stops at the split; and the tail
/// is marked uncovered, for the stealer's node to serve by pull-through.
#[tokio::test(start_paused = true)]
async fn a_slow_victim_outside_the_covered_suffix_is_stolen_by_pull_through() -> anyhow::Result<()>
{
    use std::sync::atomic::Ordering;

    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let (mut work, stealer, frontier) = slow_victim_work(1, Duration::from_secs(40), Some(8)).await;
    let missing = [(frontier, total - frontier)];
    assert!(
        !work.has_work_for(&stealer, total, true),
        "a lane that is not running has no rate, so it starts on no slow victim"
    );
    assert!(work.steal_due(1, total, &stealer, &missing)?);
    let picked = work
        .pick(1, total, &stealer, true, &missing)?
        .ok_or_else(|| anyhow::anyhow!("worker 1 must steal the slow victim's tail"))?;
    assert!(
        picked.uncovered,
        "the tail lies outside the stealer's coverage"
    );
    assert_eq!(picked.range.fetch_end(), total);
    // The victim keeps a ninth of its missing remainder (1 : 8), rounded
    // down to a group.
    let group = decdn_bao_range::CHUNK_GROUP_BYTES;
    let keep = (total - frontier) / 9;
    assert_eq!(
        picked.range.fetch_start(),
        (frontier + keep) / group * group
    );
    let split = picked.range.fetch_start();
    assert_eq!(work.in_flight.first().copied().flatten(), Some((0, split)));
    assert_eq!(
        work.live
            .first()
            .map(|unit| unit.stop_at.load(Ordering::Acquire)),
        Some(split),
        "the victim stops at the split"
    );
    Ok(())
}

/// #2348: a lane barred from pull-through steals no slow victim's tail
/// outside its coverage.
#[tokio::test(start_paused = true)]
async fn a_barred_lane_does_not_steal_a_slow_victim() -> anyhow::Result<()> {
    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let (mut work, stealer, frontier) = slow_victim_work(1, Duration::from_secs(40), Some(8)).await;
    let missing = [(frontier, total - frontier)];
    work.set_no_uncovered(1, true);
    assert!(!work.steal_due(1, total, &stealer, &missing)?);
    assert!(work.pick(1, total, &stealer, true, &missing)?.is_none());
    Ok(())
}

/// #2348: a victim is slow only at [`super::SLOW_VICTIM_FACTOR`] times
/// slower than the stealer or more, over at least
/// [`super::SLOW_VICTIM_EVIDENCE`] of rate, and only against a stealer
/// with a rate.
#[tokio::test(start_paused = true)]
async fn a_slow_victim_steal_needs_the_rate_gap_and_the_evidence() -> anyhow::Result<()> {
    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let steals = |work: &mut super::Work, stealer: &Coverage, frontier: u64| {
        work.pick(1, total, stealer, true, &[(frontier, total - frontier)])
            .map(|p| p.is_some())
    };
    let (mut work, stealer, frontier) = slow_victim_work(1, Duration::from_secs(40), Some(2)).await;
    assert!(
        !steals(&mut work, &stealer, frontier)?,
        "a 2x gap is no slow victim"
    );
    let (mut work, stealer, frontier) = slow_victim_work(1, Duration::from_secs(40), Some(4)).await;
    assert!(steals(&mut work, &stealer, frontier)?, "a 4x gap is one");
    let (mut work, stealer, frontier) = slow_victim_work(1, Duration::from_secs(20), Some(8)).await;
    assert!(
        !steals(&mut work, &stealer, frontier)?,
        "20 s of rate is too little evidence"
    );
    let (mut work, stealer, frontier) = slow_victim_work(1, Duration::from_secs(40), None).await;
    assert!(
        !steals(&mut work, &stealer, frontier)?,
        "a stealer with no rate has nothing to compare"
    );
    Ok(())
}

/// #2348: a victim parked on the consumer now does not read as slow. Its
/// park in progress stays out of its rate's active time, as an ended park
/// does, so a long park does not stretch a short rate into evidence.
#[tokio::test(start_paused = true)]
async fn a_victim_parked_on_the_consumer_now_is_not_a_slow_victim() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering;

    let total = 3 * DISCOVERY_BLOCK_BYTES;
    // 10 MiB/s for 5 s, then parked on the consumer for 60 s and counting.
    let (mut work, stealer, frontier) = slow_victim_work(10, Duration::from_secs(5), Some(8)).await;
    if let Some(unit) = work.live.first() {
        let at = u64::try_from(Duration::from_secs(5).as_nanos()).unwrap_or(u64::MAX);
        unit.park_open_at.store(at, Ordering::Relaxed);
    }
    tokio::time::advance(Duration::from_mins(1)).await;
    let sample = work.live.first().and_then(|unit| unit.rate_sample());
    assert_eq!(
        sample,
        Some((10 * MIB, Duration::from_secs(5))),
        "the open park stays out of the rate"
    );
    let missing = [(frontier, total - frontier)];
    assert!(!work.steal_due(1, total, &stealer, &missing)?);
    assert!(work.pick(1, total, &stealer, true, &missing)?.is_none());
    Ok(())
}

/// #2348: a lane never counts its own extra worker as a slow victim: the
/// two share one node and one path.
#[tokio::test(start_paused = true)]
async fn a_lane_does_not_steal_from_its_own_extra_as_a_slow_victim() -> anyhow::Result<()> {
    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let (mut work, stealer, frontier) = slow_victim_work(1, Duration::from_secs(40), Some(8)).await;
    // Slot 0 is now lane 1's extra worker.
    if let Some(lane) = work.lane_of.first_mut() {
        *lane = 1;
    }
    if let Some(extra) = work.extra.first_mut() {
        *extra = true;
    }
    let missing = [(frontier, total - frontier)];
    assert!(work.pick(1, total, &stealer, true, &missing)?.is_none());
    Ok(())
}

/// #2348: a covered-suffix steal comes first. With a slow victim and a
/// range the stealer covers at its end both in flight, the stealer takes
/// the covered one, inside its coverage.
#[tokio::test(start_paused = true)]
async fn a_covered_suffix_steal_is_preferred_over_a_slow_victim() -> anyhow::Result<()> {
    const B: u64 = DISCOVERY_BLOCK_BYTES;
    let total = 3 * B;
    let (mut work, _, frontier) = slow_victim_work(1, Duration::from_secs(40), Some(8)).await;
    // Lane 0 (slow) now holds blocks 0-1; lane 2 holds block 2, which the
    // stealer covers.
    if let Some(slot) = work.in_flight.first_mut() {
        *slot = Some((0, 2 * B));
    }
    if let Some(slot) = work.in_flight.get_mut(2) {
        *slot = Some((2 * B, B));
    }
    let stealer = cov(3, &[2]);
    let missing = [(frontier, total - frontier)];
    let picked = work
        .pick(1, total, &stealer, true, &missing)?
        .ok_or_else(|| anyhow::anyhow!("worker 1 must steal block 2's tail"))?;
    assert!(!picked.uncovered);
    assert!(picked.range.fetch_start() >= 2 * B);
    assert_eq!(picked.range.fetch_end(), total);
    assert_eq!(
        work.in_flight.first().copied().flatten(),
        Some((0, 2 * B)),
        "the slow victim keeps its range"
    );
    Ok(())
}

/// #2303: a lane that covers only the last block of a busy lane's range
/// steals from that block's start when the split by rate lies before it,
/// past the split by rate when that lies inside it, and not at all when
/// the victim's remainder is below the floor.
#[tokio::test]
async fn a_tail_holder_steal_moves_the_split_into_its_covered_suffix() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering;

    const B: u64 = DISCOVERY_BLOCK_BYTES;
    let total = 3 * B;
    let all = [(0, total)];
    let last_only = cov(3, &[2]);
    let steal_at = |frontier: u64| -> anyhow::Result<(super::Work, Option<super::Picked>)> {
        let mut work = one_victim_work(total, &cov(3, &[0, 1, 2]));
        if let Some(unit) = work.live.first() {
            unit.progress.frontier.store(frontier, Ordering::Release);
        }
        let picked = work.pick(1, total, &last_only, true, &all)?;
        Ok((work, picked))
    };

    // Frontier at 20 MiB: the even split (106 MiB) lies before block 2,
    // so the split moves to block 2 and the victim keeps blocks 0 and 1.
    let (work, picked) = steal_at(20 * MIB)?;
    let picked = picked.ok_or_else(|| anyhow::anyhow!("block 2 is stealable"))?;
    assert_eq!(picked.range.fetch_start(), 2 * B);
    assert_eq!(picked.range.fetch_end(), total);
    assert_eq!(work.in_flight.first().copied().flatten(), Some((0, 2 * B)));
    assert_eq!(
        work.live
            .first()
            .map(|unit| unit.stop_at.load(Ordering::Acquire)),
        Some(2 * B),
        "the victim stops its open stream at the moved split"
    );

    // Frontier at 160 MiB: the victim keeps half of the 32 MiB it misses.
    let (_, picked) = steal_at(160 * MIB)?;
    let picked = picked.ok_or_else(|| anyhow::anyhow!("32 MiB is stealable"))?;
    assert_eq!(picked.range.fetch_start(), 176 * MIB);

    // Frontier at 180 MiB: 12 MiB left is below the floor.
    let (work, picked) = steal_at(180 * MIB)?;
    assert!(picked.is_none());
    assert_eq!(work.in_flight.first().copied().flatten(), Some((0, total)));
    Ok(())
}

/// #2303: a holder that covers a busy range's last block counts as having
/// work only while that range's covered suffix has enough left to steal,
/// so a lane that could only park never takes a start slot.
#[tokio::test]
async fn a_tail_holder_has_work_only_while_its_covered_suffix_is_stealable() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering;

    const B: u64 = DISCOVERY_BLOCK_BYTES;
    let total = 3 * B;
    let work = one_victim_work(total, &cov(3, &[0, 1, 2]));
    let last_only = cov(3, &[2]);
    assert!(work.has_work_for(&last_only, total, false));
    assert!(!work.has_work_for(&cov(3, &[0, 1]), total, false));
    // The victim received all but 12 MiB: below the floor, nothing to steal.
    if let Some(unit) = work.live.first() {
        unit.progress.frontier.store(180 * MIB, Ordering::Release);
    }
    assert!(!work.has_work_for(&last_only, total, false));
    assert!(
        !work.has_work_for(&cov(3, &[0, 1, 2]), total, false),
        "the floor holds for a full holder too"
    );
    Ok(())
}

/// THE double-pay test: a fast source and a held-back one over a 64 MiB
/// blob, arranged so a steal DEFINITELY fires: every leg of the slow source
/// waits before its first byte until the test sees the fast source open a
/// second range, which is the steal of the slow source's tail. The victim
/// stops at the split, so the stolen tail is fetched by exactly ONE
/// source, and total delivered ≈ the blob size.
#[tokio::test]
async fn forced_steal_does_not_double_fetch_the_stolen_tail() -> anyhow::Result<()> {
    let data = blob(64 * 1024 * 1024);
    let total = data.len() as u64;
    let ledger_fast = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_slow = Arc::new(PoolLedger::new(Cumulative::default()));
    let src_fast = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_fast));
    // The slow source delivers nothing until the gate opens, so the fast
    // source always finishes its own 32 MiB first and steals, whatever the
    // machine's speed.
    let (gate, gated) = tokio::sync::watch::channel(false);
    let src_slow = ScriptedSource::new(data.clone())?
        .gated_on(gated)
        .paying(Arc::clone(&ledger_slow));
    let root = src_fast.root();
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate(src_fast.clone(), ledger_fast, 0xA1, None),
        candidate(src_slow.clone(), ledger_slow, 0xB2, None),
    ])?;
    let (pacer, funder) = (BudgetPacer::new(), no_funding());
    let fetch = run_acquire(&store, &provider, root, total, &pacer, &funder, 2, None);
    // Open the gate once the fast source has opened a second range: its
    // steal of the slow source's tail.
    let release = async {
        while src_fast.opened_ranges().len() < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        gate.send_replace(true);
    };
    let (fetched, ()) = tokio::time::timeout(
        Duration::from_mins(5),
        futures_util::future::join(fetch, release),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the fetch or the steal never happened"))?;
    fetched?;

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
    // within a small bounded slop of the blob size.
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

/// A fast source and a slow one, each with a steady rate, over a 128 MiB
/// blob. The fast source finishes its half first and steals from the slow
/// one by the two rates, so both finish together: the slow source is
/// stolen from once, keeps its one stream, and stops that stream at the
/// split. A midpoint split would leave the slow source enough for a second
/// steal, and a third.
#[tokio::test(start_paused = true)]
async fn a_slow_victim_is_stolen_from_once() -> anyhow::Result<()> {
    let data = blob(128 * MIB as usize);
    let total = data.len() as u64;
    let ledger_fast = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_slow = Arc::new(PoolLedger::new(Cumulative::default()));
    let fast = ScriptedSource::new(data.clone())?
        .throttled(Duration::from_millis(1))
        .paying(Arc::clone(&ledger_fast));
    let slow = ScriptedSource::new(data.clone())?
        .throttled(Duration::from_millis(4))
        .paying(Arc::clone(&ledger_slow));
    let root = fast.root();
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate(fast.clone(), ledger_fast, 0xA1, None),
        candidate(slow.clone(), ledger_slow, 0xB2, None),
    ])?;
    let (pacer, funder) = (BudgetPacer::new(), no_funding());
    tokio::time::timeout(
        Duration::from_mins(5),
        run_acquire(&store, &provider, root, total, &pacer, &funder, 2, None),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the fetch stalled"))??;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    let (fast_opens, slow_opens) = (fast.opened_ranges(), slow.opened_ranges());
    assert_eq!(
        fast_opens.len(),
        2,
        "the fast source opens its half and one steal: {fast_opens:?} / {slow_opens:?}"
    );
    assert_eq!(
        slow_opens.len(),
        1,
        "the slow source keeps its one stream: {fast_opens:?} / {slow_opens:?}"
    );
    assert_eq!(
        slow.stopped_pulls(),
        1,
        "the slow source stops at the split"
    );
    let delivered = fast.delivered_bytes() + slow.delivered_bytes();
    assert!(
        delivered <= total + 2 * MIB,
        "no byte is fetched twice: delivered {delivered} of {total}"
    );
    Ok(())
}

/// #2348's shape over a three-block blob: a fast partial holder F covers
/// block 0 only; a slow partial holder S covers blocks 1 and 2 and runs
/// at a twentieth of F's rate, or at F's per-read delay times
/// `slow_per_read` ms when given. Each holder gets its own blocks; F
/// finishes block 0 in a few seconds and parks, since it does not cover
/// S's last block. `refuse` makes F refuse blocks 1 and 2 with that
/// fault; `widen` gives F's lane its stream hooks. Returns F, S, the
/// blob, the sources, and the store and its directory.
#[allow(clippy::type_complexity, reason = "a test fixture's one return tuple")]
fn slow_leg_fixture(
    refuse: Option<fn() -> anyhow::Error>,
    slow_per_read: Option<u64>,
    widen: Option<super::LaneWiden>,
) -> anyhow::Result<(
    ScriptedSource,
    ScriptedSource,
    Vec<u8>,
    StaticSources<ScriptedSource>,
    ClientRangedStore,
    tempfile::TempDir,
)> {
    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let data = blob(total as usize);
    let n = num_blocks(total);
    let ledger_fast = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_slow = Arc::new(PoolLedger::new(Cumulative::default()));
    let mut fast = ScriptedSource::new(data.clone())?
        .throttled(Duration::from_millis(1))
        .paying(Arc::clone(&ledger_fast));
    if let Some(make) = refuse {
        fast = fast.refusing_blocks(&[1, 2], make);
    }
    let slow = ScriptedSource::new(data.clone())?
        .throttled(Duration::from_millis(slow_per_read.unwrap_or(20)))
        .paying(Arc::clone(&ledger_slow));
    let root = fast.root();
    let (store, dir) = fresh_store(root, total);
    let mut fast_lane = candidate(fast.clone(), ledger_fast, 0xA1, Some(cov(n, &[0])));
    fast_lane.widen = widen;
    let provider = StaticSources::new(vec![
        fast_lane,
        candidate(slow.clone(), ledger_slow, 0xB2, Some(cov(n, &[1, 2]))),
    ])?;
    Ok((fast, slow, data, provider, store, dir))
}

/// #2348: a parked lane steals a slow leg's tail outside its own
/// coverage, by pull-through, once the slow leg's rate holds over
/// `SLOW_VICTIM_EVIDENCE`. Its parked worker finds the steal on the
/// `STEAL_RECHECK` clock: no peer event wakes it. The slow lane keeps its
/// one stream and stops it at the split, and no byte is fetched twice.
#[tokio::test(start_paused = true)]
async fn a_parked_lane_steals_a_slow_legs_tail_after_the_recheck() -> anyhow::Result<()> {
    let (fast, slow, data, provider, store, dir) = slow_leg_fixture(None, None, None)?;
    let total = data.len() as u64;
    let root = fast.root();
    let started = tokio::time::Instant::now();
    tokio::time::timeout(
        Duration::from_mins(10),
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_funding(),
            2,
            None,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the fetch stalled"))??;
    let took = started.elapsed();
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    let slow_first = slow
        .timeline()
        .first()
        .map(|&(_, opened, _)| opened)
        .ok_or_else(|| anyhow::anyhow!("the slow source opened no leg"))?;
    let stolen: Vec<_> = fast
        .timeline()
        .into_iter()
        .filter(|&(start, _, _)| start >= DISCOVERY_BLOCK_BYTES)
        .collect();
    assert_eq!(
        stolen.len(),
        1,
        "F steals S's tail once: {:?} / {:?}",
        fast.opened_ranges(),
        slow.opened_ranges()
    );
    let steal_at = stolen.first().map(|&(_, opened, _)| opened);
    assert!(
        steal_at.is_some_and(|at| at >= slow_first + super::SLOW_VICTIM_EVIDENCE),
        "the steal waits for the slow leg's evidence"
    );
    assert_eq!(slow.opened_ranges().len(), 1, "S keeps its one stream");
    assert_eq!(slow.stopped_pulls(), 1, "S stops at the split");
    let delivered = fast.delivered_bytes() + slow.delivered_bytes();
    assert!(
        delivered <= total + 2 * MIB,
        "no byte is fetched twice: delivered {delivered} of {total}"
    );
    // S alone would take 128 MiB at its rate: 160 s.
    assert!(took < Duration::from_secs(80), "the fetch took {took:?}");
    Ok(())
}

/// #2348: a slow-victim steal whose node refuses `Declined` puts the tail
/// back in the queue and bars the lane from pull-through, so it is asked
/// for that tail once. The slow lane, which covers it, finishes the fetch.
#[tokio::test(start_paused = true)]
async fn a_refused_slow_victim_steal_requeues_and_bars_the_lane() -> anyhow::Result<()> {
    fn too_large() -> anyhow::Error {
        anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::Declined))
    }
    let (fast, slow, data, provider, store, dir) = slow_leg_fixture(Some(too_large), None, None)?;
    let total = data.len() as u64;
    let root = fast.root();
    tokio::time::timeout(
        Duration::from_mins(10),
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_funding(),
            2,
            None,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the fetch stalled"))??;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    let asked = fast
        .opened_ranges()
        .into_iter()
        .filter(|&(start, _)| start >= DISCOVERY_BLOCK_BYTES)
        .count();
    assert_eq!(
        asked,
        1,
        "F is asked for S's tail once: {:?}",
        fast.opened_ranges()
    );
    assert!(
        slow.opened_ranges().len() >= 2,
        "S takes the re-queued tail on a new leg: {:?}",
        slow.opened_ranges()
    );
    Ok(())
}

/// #2348: a slow-victim steal refused with `NotFound` puts the tail back
/// in the queue each time, and the lane is barred from pull-through after
/// `ABSENT_AFTER_NOT_FOUND` such answers. The lane keeps its own covered
/// block, the fetch completes, and no byte is fetched twice.
#[tokio::test(start_paused = true)]
async fn a_slow_victim_steal_refused_as_not_found_is_bounded() -> anyhow::Result<()> {
    fn not_found() -> anyhow::Error {
        anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound))
    }
    let (fast, slow, data, provider, store, dir) = slow_leg_fixture(Some(not_found), None, None)?;
    let total = data.len() as u64;
    let root = fast.root();
    tokio::time::timeout(
        Duration::from_mins(10),
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_funding(),
            2,
            None,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the fetch stalled"))??;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    let opened = fast.opened_ranges();
    let asked = opened
        .iter()
        .filter(|&&(start, _)| start >= DISCOVERY_BLOCK_BYTES)
        .count();
    assert!(
        (1..=crate::source_set::ABSENT_AFTER_NOT_FOUND as usize).contains(&asked),
        "F is asked for S's tail {asked} times: {opened:?}"
    );
    assert!(
        opened
            .iter()
            .any(|&(start, _)| start < DISCOVERY_BLOCK_BYTES),
        "F serves its own block: {opened:?}"
    );
    let delivered = fast.delivered_bytes() + slow.delivered_bytes();
    assert!(
        delivered <= total + 2 * MIB,
        "no byte is fetched twice: delivered {delivered} of {total}"
    );
    Ok(())
}

/// #2348: while no slow-victim steal is due, a parked lane's steal
/// re-check takes no stream. S runs at a third of F's rate, under the
/// slow-victim gap, so F parks through several re-checks; its lane asks
/// for a stream to start again only on a peer's wake, and gives back
/// every stream it takes.
#[tokio::test(start_paused = true)]
async fn a_steal_recheck_takes_no_stream_while_no_steal_is_due() -> anyhow::Result<()> {
    let (widen, count) = counting_widen(1);
    let (fast, slow, data, provider, store, dir) = slow_leg_fixture(None, Some(3), Some(widen))?;
    let total = data.len() as u64;
    let root = fast.root();
    let started = tokio::time::Instant::now();
    tokio::time::timeout(
        Duration::from_mins(10),
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_funding(),
            2,
            None,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the fetch stalled"))??;
    let took = started.elapsed();
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert!(
        fast.opened_ranges()
            .iter()
            .all(|&(start, _)| start < DISCOVERY_BLOCK_BYTES),
        "no steal is due at a 3x gap: {:?}",
        fast.opened_ranges()
    );
    assert!(
        took >= 3 * super::STEAL_RECHECK,
        "F parks through several re-checks: {took:?}"
    );
    let restarts = count
        .calls()
        .iter()
        .filter(|(kind, _)| *kind == super::GrowFor::Restart)
        .count();
    assert!(
        restarts <= 2,
        "a re-check with no steal due asks for no stream: {restarts} asks, {:?}",
        count.calls()
    );
    assert_eq!(count.released(), count.granted());
    drop(slow);
    Ok(())
}

/// Admit `[start, start + len)` of `data` into `store`, as a leg that
/// delivered it would: the frontier a steal reads.
async fn admit_prefix(
    store: &ClientRangedStore,
    data: &[u8],
    start: u64,
    len: u64,
) -> anyhow::Result<()> {
    let range = decdn_bao_range::align_range(start, len, data.len() as u64)?;
    let outboard =
        bao_tree::io::outboard::PreOrderMemOutboard::create(data, decdn_bao_range::IROH_BLOCK_SIZE);
    let slice = data
        .get(usize::try_from(range.fetch_start())?..usize::try_from(range.fetch_end())?)
        .ok_or_else(|| anyhow::anyhow!("admit range out of bounds"))?;
    let bao =
        decdn_bao_range::encode_verified_range(store.root(), &range, slice, outboard.data.into())?;
    store.admit(range, bao).await?;
    Ok(())
}

/// A steal after the victim delivered part of its range: two gated full
/// holders of a 128 MiB blob each open one half. Once both have opened,
/// the victim's range is made present up to `frontier_of(start, len)`,
/// then the thief is released: it finishes its own half and may steal.
/// The thief's legs stall 1 s before their first byte, so a victim that
/// steals back does so before the thief's steal leg delivers.
/// `victim_release` opens the victim's gate. Returns the thief's and the
/// victim's opens and the victim's first range and frontier.
async fn steal_after_victim_frontier(
    frontier_of: impl Fn(u64, u64) -> u64,
    victim_release: impl AsyncFn(&ScriptedSource, &ScriptedSource, &tokio::sync::watch::Sender<bool>),
) -> anyhow::Result<StealRun> {
    let data = blob(128 * MIB as usize);
    let lt = Arc::new(PoolLedger::new(Cumulative::default()));
    let lv = Arc::new(PoolLedger::new(Cumulative::default()));
    let (thief_gate, thief_held) = tokio::sync::watch::channel(false);
    let (victim_gate, victim_held) = tokio::sync::watch::channel(false);
    let thief = ScriptedSource::new(data.clone())?
        .gated_on(thief_held)
        .slow_to_start(Duration::from_secs(1))
        .paying(Arc::clone(&lt));
    let victim = ScriptedSource::new(data.clone())?
        .gated_on(victim_held)
        .paying(Arc::clone(&lv));
    let (root, total) = (thief.root(), thief.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate(thief.clone(), lt, 0xA1, None),
        candidate(victim.clone(), lv, 0xB2, None),
    ])?;
    let (pacer, funder) = (BudgetPacer::new(), no_funding());
    let fetch = run_acquire(&store, &provider, root, total, &pacer, &funder, 2, None);
    let script = async {
        while thief.opened_ranges().is_empty() || victim.opened_ranges().is_empty() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let (start, len) = victim
            .opened_ranges()
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("the victim opened a range"))?;
        let frontier = frontier_of(start, len);
        admit_prefix(&store, &data, start, frontier - start).await?;
        thief_gate.send_replace(true);
        victim_release(&thief, &victim, &victim_gate).await;
        anyhow::Ok((start, len, frontier))
    };
    let (fetched, scripted) = tokio::time::timeout(
        Duration::from_mins(5),
        futures_util::future::join(fetch, script),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the fetch or the script stalled"))?;
    fetched?;
    let (start, len, frontier) = scripted?;
    store.finalize().await?;
    assert_eq!(
        std::fs::read(dir.path().join("b"))?,
        data,
        "assembled byte-identical after the steal"
    );
    Ok(StealRun {
        thief: thief.opened_ranges(),
        victim: victim.opened_ranges(),
        victim_stops: victim.stopped_pulls(),
        delivered: thief.delivered_bytes() + victim.delivered_bytes(),
        victim_range: (start, len),
        frontier,
    })
}

/// What [`steal_after_victim_frontier`] observed.
struct StealRun {
    thief: Vec<(u64, u64)>,
    victim: Vec<(u64, u64)>,
    /// The victim's pulls stopped before the end of their range.
    victim_stops: u32,
    /// Wire bytes both sources delivered.
    delivered: u64,
    victim_range: (u64, u64),
    frontier: u64,
}

/// A steal from a victim that delivered past its picked midpoint splits
/// the victim's MISSING remainder: the thief takes its second half (the
/// victim has no rate yet), and the victim keeps the first half on its
/// open stream. The victim opens no new stream, stops its one pull at the
/// split, and no byte past the split is fetched by both lanes.
#[tokio::test(start_paused = true)]
async fn a_steal_splits_the_victims_missing_remainder() -> anyhow::Result<()> {
    let run = steal_after_victim_frontier(
        |start, len| start + len / 8 * 5,
        async |thief, _, gate| open_when(|| thief.opened_ranges().len() >= 2, gate).await,
    )
    .await?;
    let (start, len) = run.victim_range;
    let end = start + len;
    let split = run.frontier + (end - run.frontier) / 2;
    let seen = (&run.thief, &run.victim, run.frontier);
    assert_eq!(
        run.thief.get(1).copied(),
        Some((split, end - split)),
        "the thief steals the second half of the victim's missing remainder: {seen:?}",
    );
    assert_eq!(
        run.victim,
        vec![(start, len)],
        "the victim keeps its one stream and opens no new one: {seen:?}",
    );
    assert_eq!(
        run.victim_stops, 1,
        "the victim stops its pull at the split"
    );
    // The victim streams its range from its start, the bytes the test
    // admitted included, so it re-delivers `[start, frontier)` once. Past
    // the split only the thief delivers.
    let wire = |bytes: u64| bytes + bytes / 1024 * 64 / 16;
    assert!(
        run.delivered <= wire(128 * MIB + (run.frontier - start)) + 2 * MIB,
        "a byte past the split was fetched twice: delivered {} of {}",
        run.delivered,
        128 * MIB,
    );
    Ok(())
}

/// A victim whose missing remainder is below the split floor is not
/// stolen from, however long its picked range: the thief parks and the
/// victim finishes its own range.
#[tokio::test(start_paused = true)]
async fn a_victim_with_a_small_missing_remainder_is_not_stolen_from() -> anyhow::Result<()> {
    let run = steal_after_victim_frontier(
        |start, len| start + len / 16 * 15,
        async |thief, _, gate| {
            while thief.delivered_bytes() < 64 * MIB {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            // Every task idles before virtual time moves on, so the thief
            // has picked by the time this sleep ends.
            tokio::time::sleep(Duration::from_millis(100)).await;
            gate.send_replace(true);
        },
    )
    .await?;
    assert_eq!(
        run.thief.len(),
        1,
        "the thief must not steal a 4 MiB remainder: {:?}",
        run.thief
    );
    assert_eq!(
        run.victim.len(),
        1,
        "the victim keeps its range: {:?}",
        run.victim
    );
    Ok(())
}

/// The multi-thread variant of the no-double-pay guard, deterministically
/// hitting the completed-but-uncleared window on a real 2-thread runtime.
/// `src_slow_finish` delivers its whole segment then stalls INSIDE `finish`,
/// so its completed range stays in `in_flight` for the whole stall.
/// `src_stealer` starts late, finishes its own segment, and (with `pending`
/// empty) steals that completed, fully-present range. The present-bytes
/// backstop skips it, so total fetched stays near the blob size.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forced_steal_no_double_pay_on_multi_thread_runtime() -> anyhow::Result<()> {
    let data = blob(64 * 1024 * 1024);
    let total = data.len() as u64;
    let ledger_finish = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_stealer = Arc::new(PoolLedger::new(Cumulative::default()));
    // Delivers its segment fast, then holds it completed-but-uncleared for
    // 500 ms inside `finish`: the window a peer steals into.
    let src_slow_finish = ScriptedSource::new(data.clone())?
        .slow_finish(Duration::from_millis(500))
        .paying(Arc::clone(&ledger_finish));
    // Starts 100 ms late so the other source is already parked in `finish`
    // by the time this one frees up and steals it.
    let src_stealer = ScriptedSource::new(data.clone())?
        .slow_to_start(Duration::from_millis(100))
        .paying(Arc::clone(&ledger_stealer));
    let root = src_slow_finish.root();
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate(
            src_slow_finish.clone(),
            Arc::clone(&ledger_finish),
            0xA1,
            None,
        ),
        candidate(src_stealer.clone(), ledger_stealer, 0xB2, None),
    ])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        4,
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
    assert!(
        total_delivered <= total + 4 * 1024 * 1024,
        "a completed-but-uncleared range must not be re-fetched: delivered \
         {total_delivered} (slow_finish={}, stealer={}) for a {total}-byte blob",
        src_slow_finish.delivered_bytes(),
        src_stealer.delivered_bytes(),
    );
    // The stolen range was already present, so the steal must not cancel the
    // victim: its leg reaches `finish` and bills its own lane.
    let finish_committed = ledger_finish.committed();
    assert!(
        finish_committed.bytes > U256::ZERO,
        "a victim whose tail is already present must finish and bill its leg: \
         {finish_committed:?}"
    );
    Ok(())
}

/// A node that signs a size of zero for a bounded open fails the wire
/// bound's alignment. The error is the node's, not ours: the node cools,
/// and the other source finishes the blob.
#[tokio::test(start_paused = true)]
async fn a_zero_size_signed_for_a_bounded_open_cools_only_that_node() -> anyhow::Result<()> {
    let data = blob(8 * 1024 * 1024);
    let total = data.len() as u64;
    let ledger_liar = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_honest = Arc::new(PoolLedger::new(Cumulative::default()));
    let liar = ScriptedSource::new(data.clone())?
        .refusing_opens_from(0, || {
            crate::aligned_wire_len(0, 16_384, 0)
                .err()
                .unwrap_or_else(|| anyhow::anyhow!("a zero size must not align"))
        })
        .paying(Arc::clone(&ledger_liar));
    let honest = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_honest));
    let root = honest.root();
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate(liar, ledger_liar, 0xA1, None),
        candidate(honest, ledger_honest, 0xB2, None),
    ])?;
    let health: Arc<PeerHealth> = Arc::default();
    run_acquire_with(
        &store,
        &provider,
        root,
        &BudgetPacer::new(),
        &no_funding(),
        Knobs {
            health: Arc::clone(&health),
            ..Knobs::lanes(2)
        },
    )
    .await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert!(
        !matches!(
            health.health(Address::repeat_byte(0xA1)),
            Health::Healthy { streak: 0 }
        ),
        "the node that signed zero cooled: {:?}",
        health.health(Address::repeat_byte(0xA1))
    );
    Ok(())
}

/// A fatal fault ends the whole acquire with THAT typed error and does NOT
/// reassign the failed source's range to a peer. `src_terminal` (lane 0)
/// owns the first segment and faults at byte 0 with a local fault
/// ([`crate::LocalPullFault`]): only the user can fix it, so no other lane
/// can. `src_peer` (lane 1) is slow to start, so it is still on its OWN
/// second segment when the fatal fault ends the acquire.
#[tokio::test]
async fn terminal_fault_propagates_and_is_not_reassigned() -> anyhow::Result<()> {
    let data = blob(64 * 1024 * 1024);
    let total = data.len() as u64;
    let ledger_terminal = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_peer = Arc::new(PoolLedger::new(Cumulative::default()));
    let src_terminal = ScriptedSource::new(data.clone())?
        .with_fault_after(0, || {
            anyhow::anyhow!("keystore unreadable").context(crate::LocalPullFault)
        })
        .paying(Arc::clone(&ledger_terminal));
    let src_peer = ScriptedSource::new(data.clone())?
        .slow_to_start(Duration::from_secs(10))
        .paying(Arc::clone(&ledger_peer));
    let root = src_terminal.root();
    let (store, _dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate(src_terminal.clone(), ledger_terminal, 0xA1, None),
        candidate(src_peer, ledger_peer, 0xB2, None),
    ])?;
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &no_funding(),
            2,
            None,
        ),
    )
    .await
    .expect("a fatal fault must end the acquire promptly, not hang");

    let err = result.expect_err("a fatal fault must fail the acquire");
    assert!(
        err.downcast_ref::<crate::LocalPullFault>().is_some(),
        "the fatal error must propagate verbatim, not be masked: {err:#}"
    );

    // Lane 0 delivered nothing (it faulted at byte 0) and its range was NOT
    // reassigned: a region well inside lane 0's first segment is still entirely
    // missing.
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

/// The pool-wide gate: a run registry's `total_committed()` sums EVERY
/// registered lane, including one this acquire never starts: a concurrent
/// fetch's lane sharing the same deposit. Lane C is registered but is not a
/// source here; it carries a prior spend of 68 of the 100-unit deposit.
/// Summed over just this acquire's own lanes the pool looks fully solvent;
/// summed the pool-wide way (`Some(&reg)`), C's spend already claims most
/// of the deposit, so A's second leg is refused and A is priced out.
#[tokio::test(start_paused = true)]
#[allow(clippy::too_many_lines)] // three registered lanes, then one acquire
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
    // Lane C belongs to a DIFFERENT concurrent fetch on the same run.
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
    // Lane B faults at its first byte on every open: it contributes
    // nothing, so its 32 MiB segment goes to lane A as a second leg.
    let src_b = ScriptedSource::new(data.clone())?
        .with_fault_after(0, || anyhow::anyhow!("scripted immediate fault"))
        .paying(Arc::clone(&handle_b.ledger));
    let root = src_a.root();
    let (store, _dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate_ctx(
            src_a,
            Arc::clone(&handle_a.ledger),
            Arc::clone(&handle_a.ctx),
            None,
        ),
        candidate_ctx(
            src_b,
            Arc::clone(&handle_b.ledger),
            Arc::clone(&handle_b.ctx),
            None,
        ),
    ])?;
    let health: Arc<PeerHealth> = Arc::default();
    let err = run_acquire_with(
        &store,
        &provider,
        root,
        &BudgetPacer::new(),
        &no_funding(),
        Knobs {
            give_up_after: Some(Duration::from_mins(1)),
            ledgers: Some(&reg),
            health: Arc::clone(&health),
            ..Knobs::lanes(2)
        },
    )
    .await
    .expect_err("the pool-wide spend must refuse the second leg");
    assert!(err.downcast_ref::<GaveUp>().is_some(), "{err:#}");
    assert_matches!(
        health.health(Address::repeat_byte(0xA1)),
        Health::Unaffordable { .. },
        "the pool-wide view priced A out: {:?}",
        health.health(Address::repeat_byte(0xA1))
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

/// Per-provider payment lanes. Two sources with DISTINCT providers each pay
/// their OWN ledger: the fetch assembles byte-identical, and each lane's
/// cumulative advances INDEPENDENTLY, tracking exactly the wire that source
/// delivered (never the peer's).
///
/// Each lane covers one discovery block of a two-block blob, so neither can
/// steal the other's range. The scripted double bills a leg only in
/// `finish` or `stop`, so a leg dropped mid-stream would read as unpaid
/// here and hide which ledger the lane uses.
#[tokio::test]
async fn distinct_providers_each_pay_their_own_lane() -> anyhow::Result<()> {
    let data = blob(usize::try_from(2 * DISCOVERY_BLOCK_BYTES)?);
    let total = data.len() as u64;
    let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
    let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
    let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
    let root = src_a.root();
    let (store, dir) = fresh_store(root, total);

    let lane_a = candidate(src_a, Arc::clone(&ledger_a), 0xA1, Some(cov(2, &[0])));
    let lane_b = candidate(src_b, Arc::clone(&ledger_b), 0xB2, Some(cov(2, &[1])));
    let provider_a = lane_a.ctx.lock().expect("ctx").provider;
    let provider_b = lane_b.ctx.lock().expect("ctx").provider;
    assert_ne!(
        provider_a, provider_b,
        "the two lanes must pay two DIFFERENT providers"
    );
    let provider = StaticSources::new(vec![lane_a, lane_b])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        4,
        None,
    )
    .await?;
    store.finalize().await?;
    assert_eq!(
        std::fs::read(dir.path().join("b"))?,
        data,
        "assembled byte-identical across two distinct-provider lanes"
    );

    let committed_a = ledger_a.committed();
    let committed_b = ledger_b.committed();
    assert!(
        committed_a.amount > U256::ZERO && committed_b.amount > U256::ZERO,
        "both lanes must have advanced their own cumulative: a={committed_a:?} b={committed_b:?}"
    );
    assert!(
        committed_a.bytes < U256::from(total) && committed_b.bytes < U256::from(total),
        "neither lane alone may bill the whole blob; a shared ledger would: \
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

/// Shared-pool aggregate solvency. Two lanes draw on ONE pool deposit `D`.
/// Each worker's `remaining_deposit` is `D - Σ committed across EVERY lane`,
/// so the smallest balance any pacing decision saw drops below
/// `D - max(single-lane committed)`: the floor a per-lane gate could never
/// go under.
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
    let pacer = MinRemainingPacer {
        inner: BudgetPacer::new(),
        min_remaining: Mutex::new(None),
    };

    // Both lanes' ctxs carry the SAME shared pool deposit `D`.
    let provider = StaticSources::new(vec![
        candidate_ctx(
            src_a,
            Arc::clone(&ledger_a),
            Arc::new(Mutex::new(ctx_with(0xA1, deposit))),
            None,
        ),
        candidate_ctx(
            src_b,
            Arc::clone(&ledger_b),
            Arc::new(Mutex::new(ctx_with(0xB2, deposit))),
            None,
        ),
    ])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &pacer,
        &no_funding(),
        4,
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
    // And that is strictly below the per-lane floor `D - max_lane`.
    assert!(
        min_remaining < deposit.saturating_sub(max_lane),
        "the shared gate must see less than a single lane's own remaining: \
         min={min_remaining:?} per_lane_floor={:?}",
        deposit.saturating_sub(max_lane)
    );
    Ok(())
}

/// The shared gate actually BLOCKS a lane (no collective overspend).
///
/// A 64 MiB blob splits into two 32 MiB segments. Lane B's source faults at
/// once on every open (delivers nothing), but its ledger is PRE-SEEDED with
/// a committed amount `prior_spend`: a peer lane that has already drawn
/// most of the pool on earlier streams. Lane A fetches its OWN 32 MiB
/// segment (a first leg always draws: voucher cost is unpriced until the
/// first open), then takes B's re-queued segment as a SECOND leg. By then
/// the aggregate reader reports `A_committed + prior_spend`, which is at or
/// over the deposit, so the second leg is REFUSED and A is priced out. The
/// acquire makes no further progress and gives up, and A never bills a
/// second segment.
#[tokio::test(start_paused = true)]
async fn shared_gate_refuses_a_second_leg_into_a_drained_pool() -> anyhow::Result<()> {
    let data = blob(64 * 1024 * 1024);
    let total = data.len() as u64;
    let deposit = U256::from(100u64);
    let prior_spend = U256::from(68u64);

    let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_b = Arc::new(PoolLedger::new(Cumulative {
        bytes: U256::from(68u64 * 1024 * 1024),
        amount: prior_spend,
    }));
    let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
    let src_b = ScriptedSource::new(data.clone())?
        .with_fault_after(0, || anyhow::anyhow!("scripted immediate fault"))
        .paying(Arc::clone(&ledger_b));
    let root = src_a.root();
    let (store, _dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate_ctx(
            src_a.clone(),
            Arc::clone(&ledger_a),
            Arc::new(Mutex::new(ctx_with(0xA1, deposit))),
            None,
        ),
        candidate_ctx(
            src_b,
            Arc::clone(&ledger_b),
            Arc::new(Mutex::new(ctx_with(0xB2, deposit))),
            None,
        ),
    ])?;
    let result = run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        2,
        Some(Duration::from_mins(1)),
    )
    .await;

    // The gate blocked A's second leg, so the blob never completed.
    let err = result.expect_err("the shared gate must refuse A's second leg");
    assert!(err.downcast_ref::<GaveUp>().is_some(), "{err:#}");

    let committed_a = ledger_a.committed().amount;
    let committed_b = ledger_b.committed().amount;
    assert_eq!(
        committed_b, prior_spend,
        "the faulted peer lane billed nothing new"
    );
    // A billed its OWN one segment (~33 units of wire) and NOTHING for the
    // refused second.
    assert!(
        committed_a > U256::ZERO && committed_a < U256::from(50u64),
        "A must bill exactly ONE segment, never the refused second: committed_a={committed_a:?}"
    );
    assert!(
        src_a.delivered_bytes() < total,
        "A must not have delivered the whole blob: its second leg was refused: delivered={}",
        src_a.delivered_bytes()
    );
    // No-collective-overspend bound: combined committed stays within the
    // deposit plus at most one in-flight leg's overshoot.
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
/// exact progress history under a paused clock.
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
        _claimed_total: u64,
        _stop_at: Option<&'a std::sync::atomic::AtomicU64>,
    ) -> crate::source::IngestFuture<'a, R>
    where
        R: crate::BaoRangeReader + 'a,
    {
        Box::pin(async { Err(anyhow::anyhow!("unsupported on ScriptedMissing")) })
    }

    fn flush_present_record(&self) -> crate::source::SourceFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Whether `watchdog` trips within an hour of virtual time. `missing` scripts
/// the store's still-missing counts; `verified(k)` is the unit's verified-byte
/// counter during window `k`, set halfway into that window.
async fn watchdog_trips(
    missing: &[u64],
    verified: impl Fn(u64) -> u64,
    deadline: Duration,
) -> bool {
    let store = ScriptedMissing::new(64 * 1024 * 1024, missing);
    let counter = std::sync::atomic::AtomicU64::new(0);
    let feed = async {
        if deadline.is_zero() {
            return std::future::pending().await;
        }
        tokio::time::sleep(deadline / 2).await;
        for k in 0u64..=u64::MAX {
            counter.store(verified(k), std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(deadline).await;
        }
    };
    let raced = async {
        tokio::select! {
            err = super::watchdog(&store, 0, 64 * 1024 * 1024, deadline, &counter) => {
                err.is_none()
            }
            () = feed => false,
        }
    };
    tokio::time::timeout(Duration::from_hours(1), raced)
        .await
        .unwrap_or(false)
}

/// A full window with bytes still missing and no verified byte is the
/// definition of a stall: the watchdog trips and the worker's range is
/// reassigned.
#[tokio::test(start_paused = true)]
async fn watchdog_trips_when_no_verified_byte_lands() {
    assert!(
        watchdog_trips(&[8192], |_| 0, LANE_WATCHDOG).await,
        "no verified byte across a full window must trip the watchdog"
    );
}

/// A source that verified bytes and then stops is a stall from the first
/// window without a verified byte.
#[tokio::test(start_paused = true)]
async fn watchdog_trips_when_verified_bytes_stop() {
    assert!(
        watchdog_trips(&[8192], |k| k.min(3) * 1024, LANE_WATCHDOG).await,
        "a source that stops verifying bytes must trip the watchdog"
    );
}

/// A fully delivered range (`missing == 0`) is left to `fill_gap`'s own
/// completion, NEVER tripped: the source is inside `finish`, draining the
/// vouchers for bytes it already delivered. Tripping here would reassign an
/// ALREADY-PAID range: a direct double-pay.
#[tokio::test(start_paused = true)]
async fn watchdog_never_trips_a_fully_delivered_range() {
    assert!(
        !watchdog_trips(&[0], |_| 0, LANE_WATCHDOG).await,
        "a delivered range must never be tripped while it finishes paying"
    );
}

/// A source that keeps verifying bytes resets the window at each sample, even
/// when it lands less than one 4 MiB ingest checkpoint per window, so the
/// store's missing count never moves (#2209).
#[tokio::test(start_paused = true)]
async fn watchdog_window_resets_on_verified_bytes_below_a_checkpoint() {
    assert!(
        !watchdog_trips(&[8192], |k| (k + 1) * 1024 * 1024, LANE_WATCHDOG).await,
        "verified bytes must reset the window before any checkpoint lands"
    );
}

/// A leg's first byte gets the first-byte grace, not the lane watchdog: a
/// cold miss whose first byte takes 20 s is not a stall.
#[tokio::test(start_paused = true)]
async fn watchdog_gives_the_first_byte_its_grace() {
    let store = ScriptedMissing::new(64 * 1024 * 1024, &[8192]);
    let counter = std::sync::atomic::AtomicU64::new(0);
    let start = tokio::time::Instant::now();
    let feed = async {
        tokio::time::sleep(Duration::from_secs(20)).await;
        for k in 1u64..=u64::MAX {
            counter.store(k * 1024, std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };
    let tripped = tokio::time::timeout(Duration::from_mins(2), async {
        tokio::select! {
            err = super::watchdog(&store, 0, 64 * 1024 * 1024, LANE_WATCHDOG, &counter) => {
                err.is_none()
            }
            () = feed => false,
        }
    })
    .await
    .unwrap_or(false);
    assert!(!tripped, "tripped after {:?}", start.elapsed());

    // With no first byte at all, it trips once the grace ends.
    let silent = std::sync::atomic::AtomicU64::new(0);
    let start = tokio::time::Instant::now();
    let err = super::watchdog(&store, 0, 64 * 1024 * 1024, LANE_WATCHDOG, &silent).await;
    assert!(err.is_none(), "a trip, not a store error");
    assert_eq!(start.elapsed(), super::FIRST_BYTE_GRACE);
}

/// Acquire the whole blob into `store` from `provider`'s one lane, under
/// `stop`, bounded so a regression fails instead of hanging.
async fn acquire_bounded<St: crate::source::IngestStore>(
    store: &St,
    provider: &StaticSources<ScriptedSource>,
    root: [u8; 32],
    stop: &StopPolicy,
) -> anyhow::Result<()> {
    let total = store.total_bytes();
    let mut set = SourceSet::new(provider, root, Arc::default(), provider.holders());
    let gate = recovery_gate();
    let (pacer, funder) = (BudgetPacer::new(), no_funding());
    let ranges = [(0, total)];
    let env = AcquireEnv {
        pacer: &pacer,
        funder: &funder,
        recovery: &gate,
        credentials: None,
        max_lanes: 1,
        stop,
        on_progress: None,
        ledgers: None,
        pacing: None,
        max_blob_bytes: 0,
    };
    let target = AcquireTarget {
        store,
        hash: root,
        total_bytes: total,
        ranges: &ranges,
    };
    tokio::time::timeout(Duration::from_mins(2), acquire(target, &mut set, &env))
        .await
        .map_err(|_| anyhow::anyhow!("the acquire did not end within 2 min"))?
}

/// A record write slower than the lane watchdog never pauses the lanes
/// (#2211): the write runs beside the workers. The first record tick falls
/// while the lane waits for its first byte, and its write takes 30 s. The
/// lane still lands the blob on its first open, 8 s in; awaited inline,
/// the write would leave it unpolled past its first-byte grace.
#[tokio::test(start_paused = true)]
async fn a_slow_record_write_does_not_pause_the_lanes() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&la))
        .slow_to_start(Duration::from_secs(8));
    let (root, total) = (a.root(), a.total_bytes());
    let (inner, dir) = fresh_store(root, total);
    let store = crate::source::FlushCountingStore::new(inner, Duration::from_secs(30));
    assert!(store.flush_delay > LANE_WATCHDOG);
    let provider = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None)])?;
    let stop = StopPolicy::new(false, Some(Duration::from_mins(5)), Arc::default());
    acquire_bounded(&store, &provider, root, &stop).await?;

    assert_eq!(a.opened_ranges().len(), 1, "{:?}", a.opened_ranges());
    let (_, opened, finished) = a
        .timeline()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("the lane opened its range"))?;
    let finished = finished.ok_or_else(|| anyhow::anyhow!("the leg finished"))?;
    assert!(
        finished.duration_since(opened) < LANE_WATCHDOG,
        "the leg waited on the record write: it took {:?}",
        finished.duration_since(opened)
    );
    // The tick's write and the final one.
    assert_eq!(store.flushes.load(std::sync::atomic::Ordering::SeqCst), 2);
    store.inner.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    Ok(())
}

/// A store whose first record write lands 30 s after its call and every
/// later one at once, so a final write that does not wait for the first
/// lands before it. The tempdir is handed back to keep the files alive.
fn slow_first_write(
    root: [u8; 32],
    total: u64,
) -> (crate::source::FlushCountingStore, tempfile::TempDir) {
    let (inner, dir) = fresh_store(root, total);
    let store = crate::source::FlushCountingStore {
        slow_flushes: 1,
        ..crate::source::FlushCountingStore::new(inner, Duration::from_secs(30))
    };
    (store, dir)
}

/// Assert that the final record write is the last to land, once every
/// write has had time to land, whoever awaited it.
async fn assert_final_record_lands_last(store: &crate::source::FlushCountingStore) {
    tokio::time::sleep(Duration::from_mins(1)).await;
    let flushes = store.flushes.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(flushes, 2, "the tick's write and the final one");
    assert_eq!(
        store.last_landed.load(std::sync::atomic::Ordering::SeqCst),
        flushes,
        "the final snapshot lands last"
    );
}

/// The final record write lands last when the fetch completes: the slow
/// periodic write in flight lands first, so its older snapshot cannot
/// replace the final one.
#[tokio::test(start_paused = true)]
async fn the_final_record_lands_last_when_the_fetch_completes() -> anyhow::Result<()> {
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(blob(1024 * 1024))?
        .paying(Arc::clone(&la))
        .slow_to_start(Duration::from_secs(8));
    let (store, _dir) = slow_first_write(a.root(), a.total_bytes());
    let provider = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None)])?;
    let stop = StopPolicy::new(false, Some(Duration::from_mins(5)), Arc::default());
    acquire_bounded(&store, &provider, a.root(), &stop).await?;
    assert_final_record_lands_last(&store).await;
    Ok(())
}

/// The final record write lands last when the fetch ends on an error (here
/// a give-up): the slow periodic write in flight lands first.
#[tokio::test(start_paused = true)]
async fn the_final_record_lands_last_when_the_fetch_gives_up() -> anyhow::Result<()> {
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let (_open, gate) = tokio::sync::watch::channel(false);
    let a = ScriptedSource::new(blob(1024 * 1024))?
        .paying(Arc::clone(&la))
        .gated_on(gate);
    let (store, _dir) = slow_first_write(a.root(), a.total_bytes());
    let provider = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None)])?;
    // Past the first record tick at 5 s, inside the first-byte grace.
    let stop = StopPolicy::new(false, Some(Duration::from_secs(7)), Arc::default());
    let err = acquire_bounded(&store, &provider, a.root(), &stop)
        .await
        .expect_err("a source that never delivers gives up");
    assert!(err.downcast_ref::<GaveUp>().is_some(), "{err:#}");
    assert_final_record_lands_last(&store).await;
    Ok(())
}

/// End-to-end: a source whose first byte takes 20 s serves the whole
/// blob on its first open; the lane watchdog never reassigns it.
#[tokio::test(start_paused = true)]
async fn a_slow_first_byte_is_not_a_stall() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&la))
        .slow_to_start(Duration::from_secs(20));
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None)])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        1,
        Some(Duration::from_mins(5)),
    )
    .await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert_eq!(a.opened_ranges().len(), 1, "{:?}", a.opened_ranges());
    Ok(())
}

/// A zero window turns the watchdog off, as consumption pacing does.
#[tokio::test(start_paused = true)]
async fn watchdog_zero_deadline_never_trips() {
    assert!(
        !watchdog_trips(&[8192], |_| 0, Duration::ZERO).await,
        "a zero window must disable the watchdog"
    );
}

/// End-to-end: a source that opens, delivers a prefix, then WEDGES without
/// erroring is ended by the lane watchdog ALONE, and its unfetched remainder
/// is picked up by a healthy peer.
///
/// The segments are deliberately below `MIN_SPLIT_SIZE` (a 16 MiB blob over
/// two lanes) so the healthy peer CANNOT steal the wedged lane's tail: with
/// stealing unavailable, the watchdog is the only thing that can end the
/// wedge.
#[tokio::test(start_paused = true)]
async fn wedged_source_is_ended_by_the_watchdog_and_its_tail_reassigned() -> anyhow::Result<()> {
    let data = blob(16 * 1024 * 1024);
    let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
    // A delivers ~4 MiB (one INGEST_CHECKPOINT_BYTES) and then sleeps far
    // past the lane watchdog.
    let src_a = ScriptedSource::new(data.clone())?
        .stall_after(4 * 1024 * 1024, Duration::from_hours(1))
        .paying(Arc::clone(&ledger_a));
    let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
    let root = src_a.root();
    let total = src_a.total_bytes();
    let (store, dir) = fresh_store(root, total);
    let health: Arc<PeerHealth> = Arc::default();
    let provider = StaticSources::new(vec![
        candidate(src_a.clone(), ledger_a, 0xA1, None),
        candidate(src_b, ledger_b, 0xB2, None),
    ])?;
    let started = tokio::time::Instant::now();
    run_acquire_with(
        &store,
        &provider,
        root,
        &BudgetPacer::new(),
        &no_funding(),
        Knobs {
            health: Arc::clone(&health),
            ..Knobs::lanes(2)
        },
    )
    .await?;
    assert!(
        started.elapsed() < Duration::from_mins(1),
        "the watchdog, not the hour-long wedge, ended lane a: {:?}",
        started.elapsed()
    );

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

/// A dropped acquire keeps the bytes that landed recorded: the periodic
/// flush writes the present record while lanes run, so a caller that drops
/// the acquire mid-fetch (Ctrl-C, its own stop) resumes from there.
#[tokio::test(start_paused = true)]
async fn a_dropped_acquire_keeps_landed_bytes_recorded() -> anyhow::Result<()> {
    let data = blob(32 * 1024 * 1024);
    let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
    // Each lane delivers the first 4 MiB of its half, then sleeps.
    let src_a = ScriptedSource::new(data.clone())?
        .stall_after(4 * 1024 * 1024, Duration::from_hours(1))
        .paying(Arc::clone(&ledger_a));
    let src_b = ScriptedSource::new(data.clone())?
        .stall_after(4 * 1024 * 1024, Duration::from_hours(1))
        .paying(Arc::clone(&ledger_b));
    let root = src_a.root();
    let total = src_a.total_bytes();
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate(src_a, ledger_a, 0xA1, None),
        candidate(src_b, ledger_b, 0xB2, None),
    ])?;
    {
        let (pacer, funder) = (BudgetPacer::new(), no_funding());
        let fetch = run_acquire(&store, &provider, root, total, &pacer, &funder, 2, None);
        // Past one present-record flush, short of the lane watchdog.
        tokio::select! {
            fetched = fetch => panic!("wedged lanes cannot finish: {fetched:?}"),
            () = tokio::time::sleep(LANE_WATCHDOG.saturating_sub(Duration::from_secs(2))) => {}
        }
    }
    drop(store);

    let reopened = ClientRangedStore::open(dir.path(), "b", root)?;
    let recorded = ranges_content_len(&reopened.present_ranges().await?, total);
    let landed = 2 * ClientRangedStore::checkpointed_len(4 * 1024 * 1024);
    assert!(
        recorded >= landed,
        "both lanes' landed checkpoints are recorded: {recorded}, want at least {landed}"
    );
    assert!(recorded < total);
    Ok(())
}

/// A freed worker with nothing to steal PARKS rather than ending, so it is
/// still there to take over when a peer faults moments later.
///
/// Shape: a 16 MiB blob splits into two 8 MiB segments, both below the 16 MiB
/// `MIN_SPLIT_SIZE`, so the fast worker's `pick` finds nothing splittable:
/// the routine end-of-fetch condition. The slow worker then faults and
/// re-queues its remainder, which the parked worker takes.
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
    let provider = StaticSources::new(vec![
        candidate(src_a, ledger_a, 0xA1, None),
        candidate(src_b, ledger_b, 0xB2, None),
    ])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        2,
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

/// A `WindowPacer` + `ConsumptionPacing` bounds a lane to one read-ahead
/// window ahead of an injected consumer cursor: the fetch parks when the
/// window fills and only proceeds as the cursor advances via the pacing hook.
#[tokio::test]
async fn window_pacing_gates_a_lane_on_the_consumer_cursor() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::driver::{PacingWait, WaitReason};
    use crate::pacer::{DownstreamFrontier, WindowPacer};

    /// Stands in for the consumer reading one window's worth: it bumps the
    /// shared cursor and resolves immediately, so the test is deterministic.
    struct BumpConsumer {
        cursor: Arc<AtomicU64>,
        by: u64,
    }
    impl PacingWait for BumpConsumer {
        fn wait(
            &self,
            _observed: DownstreamFrontier,
            _reason: WaitReason,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            self.cursor.fetch_add(self.by, Ordering::SeqCst);
            Box::pin(async {})
        }
    }

    let data = blob(4 * 1024 * 1024); // several windows long
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let src = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger));
    let root = src.root();
    let total = src.total_bytes();
    let (store, dir) = fresh_store(root, total);

    let window = 1024 * 1024;
    let pacer = WindowPacer::new(window);
    let cursor = Arc::new(AtomicU64::new(0));
    let reader = {
        let cursor = Arc::clone(&cursor);
        move || DownstreamFrontier {
            served_paid: cursor.load(Ordering::SeqCst),
            serve_demand: 0,
        }
    };
    let hook = BumpConsumer {
        cursor: Arc::clone(&cursor),
        by: window,
    };
    let pacing = ConsumptionPacing {
        downstream: &reader,
        pacing_wait: &hook,
    };

    let provider = StaticSources::new(vec![candidate(src, ledger, 0xA1, None)])?;
    run_acquire_with(
        &store,
        &provider,
        root,
        &pacer,
        &no_funding(),
        Knobs {
            pacing: Some(&pacing),
            ..Knobs::lanes(1)
        },
    )
    .await?;
    store.finalize().await?;

    assert_eq!(
        std::fs::read(dir.path().join("b"))?,
        data,
        "byte-identical under window pacing"
    );
    assert!(
        cursor.load(Ordering::SeqCst) >= 2 * window,
        "the window must have parked the lane and been unstuck by the consumer \
         hook (cursor={})",
        cursor.load(Ordering::SeqCst)
    );
    Ok(())
}

// ---- acquire over a SourceSet ----

/// The reported bug: one holder, one reset, then it recovers.
#[tokio::test(start_paused = true)]
async fn one_source_one_reset_then_recovery_completes() -> anyhow::Result<()> {
    let data = blob(8 * 1024 * 1024);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let src = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&ledger))
        .fault_once_after(64 * 1024, || anyhow::anyhow!("connection reset"));
    let (root, total) = (src.root(), src.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![candidate(src.clone(), ledger, 0xA1, None)])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        4,
        None,
    )
    .await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert!(
        src.opened_ranges().len() >= 2,
        "the source faulted once and was reopened: {:?}",
        src.opened_ranges()
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_reserve_source_joins_when_a_lane_cools() -> anyhow::Result<()> {
    let data = blob(8 * 1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&la))
        .with_fault_after(64 * 1024, || anyhow::anyhow!("reset"));
    let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate(a.clone(), la, 0xA1, None),
        candidate(b.clone(), lb, 0xB2, None),
    ])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        1,
        None,
    )
    .await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert!(b.delivered_bytes() > 0, "the reserve source took over");
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_cheaper_source_takes_over_after_one_is_priced_out() -> anyhow::Result<()> {
    // Source A refuses with Unfunded on open; B serves.
    let data = blob(4 * 1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&la))
        .with_fault_after(0, || {
            anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::Unfunded))
        });
    let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
    let (root, total) = (a.root(), a.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate(a, la, 0xA1, None),
        candidate(b.clone(), lb, 0xB2, None),
    ])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        1,
        None,
    )
    .await?;
    assert!(b.delivered_bytes() > 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn every_source_priced_out_stops_with_the_top_up_remedy() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data)?
        .paying(Arc::clone(&la))
        .with_fault_after(0, || {
            anyhow::Error::new(PoolExhausted {
                gap_start: 0,
                gap_len: 1,
            })
        });
    let (root, total) = (a.root(), a.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![candidate(a, la, 0xA1, None)])?;
    let err = run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        4,
        None,
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("must stop"))?;
    assert!(
        err.downcast_ref::<NoAffordableSource>().is_some(),
        "{err:#}"
    );
    Ok(())
}

/// A node's `SpendingCapExhausted` refusal acts as `Unfunded` from that
/// node whatever the pool's own accounting says (ADR 005 §`VoucherRejected`
/// semantics): a lying node costs at most one funding recovery step, never
/// the fetch. With no top-up left, the acquire stops "funding needed",
/// whether or not the deposit was spent outside the loop
/// ([`Funder::pool_spent`]).
#[tokio::test(start_paused = true)]
async fn a_cap_refusal_acts_as_unfunded_from_its_node() -> anyhow::Result<()> {
    let deposit = U256::from(1_000_000_000u64);
    for (outside, genuine) in [(U256::ZERO, true), (deposit, true)] {
        let la = Arc::new(PoolLedger::new(Cumulative::default()));
        let a = ScriptedSource::new(blob(1024 * 1024))?
            .paying(Arc::clone(&la))
            .with_fault_after(0, || {
                anyhow::Error::new(UpstreamVoucherRejected {
                    reason: decdn_protocol::client::VoucherRejectReason::SpendingCapExhausted,
                    bundle: None,
                    proof_generation: None,
                })
            });
        let (root, total) = (a.root(), a.total_bytes());
        let (store, _dir) = fresh_store(root, total);
        let ctx = Arc::new(Mutex::new(ctx_with(0xA1, deposit)));
        let provider = StaticSources::new(vec![candidate_ctx(a, la, ctx, None)])?;
        let funder = no_funding().with_pool_spent(outside);
        let err = run_acquire(
            &store,
            &provider,
            root,
            total,
            &BudgetPacer::new(),
            &funder,
            1,
            None,
        )
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("outside spend {outside}: must stop"))?;
        assert_eq!(
            err.downcast_ref::<NoAffordableSource>().is_some(),
            genuine,
            "outside spend {outside}: {err:#}"
        );
    }
    Ok(())
}

/// A lane build that may have escrowed USDC no record credits ends the
/// acquire at once. Any other build fault retries with backoff, and a retry
/// here would escrow again.
#[tokio::test(start_paused = true)]
async fn a_lane_build_that_may_have_escrowed_ends_the_acquire() -> anyhow::Result<()> {
    struct EscrowingBuild(StaticSources<ScriptedSource>);

    impl crate::SourceProvider for EscrowingBuild {
        type Source = ScriptedSource;

        fn discover(&self, hash: [u8; 32]) -> crate::SourceFuture<'_, Vec<crate::Holder>> {
            self.0.discover(hash)
        }

        fn connect<'a>(
            &'a self,
            _holder: &'a crate::Holder,
        ) -> crate::SourceFuture<'a, StreamCandidate<ScriptedSource>> {
            Box::pin(async {
                Err(crate::buyer_pool::escrowed_but_untracked(
                    "pool 0x01 topped up by 5 µUSDC",
                    alloy::primitives::TxHash::repeat_byte(0xab),
                    "disk full",
                ))
            })
        }
    }

    let data = blob(1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data)?.paying(Arc::clone(&la));
    let (root, total) = (a.root(), a.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let provider = EscrowingBuild(StaticSources::new(vec![candidate(a, la, 0xA1, None)])?);
    let mut set = SourceSet::new(&provider, root, Arc::default(), provider.0.holders());
    // A retried build would run until this gives up instead.
    let stop = StopPolicy::new(false, Some(Duration::from_mins(5)), Arc::default());
    let gate = recovery_gate();
    let err = acquire(
        AcquireTarget {
            store: &store,
            hash: root,
            total_bytes: total,
            ranges: &[(0, total)],
        },
        &mut set,
        &AcquireEnv {
            pacer: &BudgetPacer::new(),
            funder: &no_funding(),
            recovery: &gate,
            credentials: None,
            max_lanes: 1,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        },
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("must stop"))?;
    assert_eq!(classify(&err), Fault::Fatal(FatalScope::Command), "{err:#}");
    assert!(
        format!("{err:#}").contains("escrowed but untracked"),
        "{err:#}"
    );
    Ok(())
}

/// Acquire the whole `root` blob from `provider`'s holders through `funder`,
/// under `gate`.
async fn acquire_under<S: BlobSource>(
    store: &ClientRangedStore,
    provider: &StaticSources<S>,
    root: [u8; 32],
    funder: &FakeFunder,
    gate: &RecoveryGate,
) -> anyhow::Result<()> {
    let total = store.total_bytes();
    let mut set = SourceSet::new(provider, root, Arc::default(), provider.holders());
    let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
    acquire(
        AcquireTarget {
            store,
            hash: root,
            total_bytes: total,
            ranges: &[(0, total)],
        },
        &mut set,
        &AcquireEnv {
            pacer: &BudgetPacer::new(),
            funder,
            recovery: gate,
            credentials: None,
            max_lanes: 4,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        },
    )
    .await
}

fn dry_lane() -> anyhow::Error {
    anyhow::Error::new(PoolExhausted {
        gap_start: 0,
        gap_len: 1,
    })
}

/// ADR 003 § Funding recovery: the first step of a fetch is free, but a
/// source that stays priced out with no newly verified byte since that step
/// gets no second one. The fetch ends "funding needed" after exactly one
/// funding call.
#[tokio::test(start_paused = true)]
async fn no_verified_byte_since_the_last_step_ends_funding_needed() -> anyhow::Result<()> {
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(blob(1024 * 1024))?
        .paying(Arc::clone(&la))
        .with_fault_after(0, dry_lane);
    let (root, total) = (a.root(), a.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![candidate(a, la, 0xA1, None)])?;
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::MAX));
    let gate = RecoveryGate::with_settle(Duration::ZERO);
    let err = acquire_under(&store, &provider, root, &funder, &gate)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("must stop"))?;
    assert!(
        err.downcast_ref::<NoAffordableSource>().is_some(),
        "{err:#}"
    );
    assert!(
        format!("{err:#}").contains("no byte was verified"),
        "{err:#}"
    );
    assert_eq!(funder.calls().len(), 1, "one step, then the progress rule");
    Ok(())
}

/// A source that delivers verified bytes before each exhaustion earns a step
/// each time: a long fetch that pays for real bytes and drains the pool again
/// recovers again, and finishes.
#[tokio::test(start_paused = true)]
async fn verified_bytes_since_the_last_step_allow_another() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&la))
        .fault_times_after(2, 64 * 1024, dry_lane);
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![candidate(a, la, 0xA1, None)])?;
    let funder = FakeFunder::scripted(vec![
        Recovery::ToppedUp(U256::from(u128::MAX) + U256::from(1u8)),
        Recovery::ToppedUp(U256::from(u128::MAX) + U256::from(2u8)),
    ]);
    let gate = RecoveryGate::with_settle(Duration::ZERO);
    acquire_under(&store, &provider, root, &funder, &gate).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert_eq!(funder.calls().len(), 2, "one step per drain");
    Ok(())
}

/// A fetch that completes on other lanes, without the refused lane running
/// again, leaves that lane's owed span unpaid: the node's credit-window loss,
/// as for any client that ends mid-stream. No pass runs only to bill it.
#[tokio::test(start_paused = true)]
async fn a_fetch_finished_by_other_lanes_leaves_the_owed_span_unpaid() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let (la, lb) = (
        Arc::new(PoolLedger::new(Cumulative::default())),
        Arc::new(PoolLedger::new(Cumulative::default())),
    );
    let a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&la))
        .with_fault_after(128 * 1024, || {
            anyhow::Error::new(UpstreamVoucherRejected {
                reason: decdn_protocol::client::VoucherRejectReason::PoolExhausted,
                bundle: None,
                proof_generation: None,
            })
        });
    let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate(a.clone(), Arc::clone(&la), 0xA1, None),
        candidate(b, lb, 0xB2, None),
    ])?;
    acquire_under(&store, &provider, root, &no_funding(), &RecoveryGate::new()).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert_eq!(a.opened_ranges().len(), 1, "no pass runs only to bill");
    assert!(
        !la.take_unpaid(root).is_empty(),
        "the refused lane's span stays unpaid"
    );
    Ok(())
}

/// An owed span whose bill fails on a delivery fault (not a funding one) stays
/// owed: the lane bills it when its worker starts again.
#[tokio::test(start_paused = true)]
async fn an_owed_span_whose_bill_faults_stays_owed() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let half = 512 * 1024;
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&la))
        .refusing_open(0, || anyhow::anyhow!("connection reset"));
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    // The first half is present and owed: an earlier worker delivered it
    // unpaid before a funding refusal ended it.
    {
        let once = ScriptedSource::new(data.clone())?;
        let range = decdn_bao_range::align_range(0, half, total)?;
        let (_header, reader) = once.open(root, range.clone()).await?;
        store.ingest_stream(&range, reader, None, total).await?;
    }
    la.note_unpaid(root, 0, half);
    let provider = StaticSources::new(vec![candidate(a.clone(), Arc::clone(&la), 0xA1, None)])?;
    acquire_under(&store, &provider, root, &no_funding(), &RecoveryGate::new()).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    let opened = a.opened_ranges();
    assert_eq!(
        opened.iter().filter(|&&(start, _)| start == 0).count(),
        2,
        "the faulted bill and the bill on the restart: {opened:?}"
    );
    assert!(la.take_unpaid(root).is_empty(), "the span is billed");
    Ok(())
}

/// A step that settles (the pool already holds its deposit, and the sources
/// have not seen it yet) leaves the deposit where it is, and still asks the
/// priced-out source again: its refusal was its stale view of the pool.
#[tokio::test(start_paused = true)]
async fn a_settling_step_asks_the_priced_out_source_again() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&la))
        .fault_once_after(0, || {
            anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::Unfunded))
        });
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![candidate(a, la, 0xA1, None)])?;
    // The deposit the context already holds: nothing rises.
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::from(u128::MAX)));
    acquire_under(&store, &provider, root, &funder, &RecoveryGate::new()).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert_eq!(funder.calls().len(), 1, "one settling step");
    Ok(())
}

/// A pool that no longer accepts funds is replaced, and the acquire ends this
/// pass with the replacement so the caller runs the remaining work against
/// the new pool.
#[tokio::test(start_paused = true)]
async fn a_replaced_pool_ends_the_pass_with_the_replacement() -> anyhow::Result<()> {
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(blob(1024 * 1024))?
        .paying(Arc::clone(&la))
        .with_fault_after(0, dry_lane);
    let (root, total) = (a.root(), a.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![candidate(a, la, 0xA1, None)])?;
    let replaced = crate::PoolReplaced {
        closed: B256::repeat_byte(1),
        opened: B256::repeat_byte(2),
    };
    let funder = FakeFunder::new(Recovery::Replaced(replaced));
    let err = acquire_under(&store, &provider, root, &funder, &RecoveryGate::new())
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("must stop"))?;
    assert_eq!(err.downcast_ref::<crate::PoolReplaced>(), Some(&replaced));
    Ok(())
}

/// One node's `Unfunded` refusal while another node serves never triggers a
/// funding recovery step: the refusal scopes to that node.
#[tokio::test(start_paused = true)]
async fn one_unfunded_node_beside_a_serving_one_takes_no_step() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let (la, lb) = (
        Arc::new(PoolLedger::new(Cumulative::default())),
        Arc::new(PoolLedger::new(Cumulative::default())),
    );
    let a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&la))
        .with_fault_after(0, || {
            anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::Unfunded))
        });
    let b = ScriptedSource::new(data)?.paying(Arc::clone(&lb));
    let (root, total) = (a.root(), a.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate(a, la, 0xA1, None),
        candidate(b.clone(), lb, 0xB2, None),
    ])?;
    let funder = FakeFunder::new(Recovery::ToppedUp(U256::MAX));
    acquire_under(&store, &provider, root, &funder, &RecoveryGate::new()).await?;
    assert!(b.delivered_bytes() > 0);
    assert!(funder.calls().is_empty(), "no step while a node serves");
    Ok(())
}

/// An entry two running lanes cover between them is not uncovered, even
/// though neither covers it whole.
#[test]
fn an_entry_covered_only_by_two_lanes_together_is_not_uncovered() -> anyhow::Result<()> {
    use std::collections::VecDeque;

    use decdn_bao_range::align_range;

    use super::Work;

    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let both_blocks = align_range(0, total, total)?;
    let mut work = Work::new(VecDeque::from(vec![both_blocks]), false);
    let a = work.add_lane(Some(cov(2, &[0])), total);
    let b = work.add_lane(Some(cov(2, &[1])), total);
    assert!(!work.uncovered(total), "a and b cover it between them");
    work.park(b);
    assert!(work.uncovered(total), "block 1 has no running lane");
    work.revive(b);
    work.park(a);
    assert!(work.uncovered(total), "block 0 has no running lane");
    Ok(())
}

/// A lane barred from pull-through is never handed a range outside its
/// coverage, and still takes the ranges it covers. Lifting the bar hands
/// it the uncovered range again.
#[test]
fn a_barred_lane_takes_only_what_it_covers() -> anyhow::Result<()> {
    use std::collections::VecDeque;

    use decdn_bao_range::align_range;

    use super::{Picked, Work};

    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let in_block0 = align_range(0, DISCOVERY_BLOCK_BYTES, total)?;
    let in_block1 = align_range(DISCOVERY_BLOCK_BYTES, DISCOVERY_BLOCK_BYTES, total)?;
    let mut work = Work::new(VecDeque::from(vec![in_block1, in_block0]), false);
    let a_cov = cov(2, &[0]);
    let a = work.add_lane(Some(a_cov.clone()), total);
    let b = work.add_lane(Some(cov(2, &[1])), total);
    work.park(b);
    work.set_no_uncovered(a, true);
    let missing = [(0, total)];

    assert!(work.has_work_for(&a_cov, total, false), "block 0 is a's");
    let first = work.pick(a, total, &a_cov, true, &missing)?;
    assert!(
        first.is_some_and(|p| p.range.fetch_start() == 0 && !p.uncovered),
        "a takes the block it covers"
    );
    work.clear(a)?;
    assert!(
        !work.has_work_for(&a_cov, total, false),
        "only block 1 is left"
    );
    assert_eq!(
        work.growth_wanted(total, |_| false).len(),
        1,
        "the idle barred lane does not take block 1"
    );
    assert!(
        work.pick(a, total, &a_cov, true, &missing)?.is_none(),
        "a barred lane takes no uncovered range"
    );

    work.set_no_uncovered(a, false);
    let Some(Picked {
        range, uncovered, ..
    }) = work.pick(a, total, &a_cov, true, &missing)?
    else {
        anyhow::bail!("an unbarred lane takes the uncovered range");
    };
    assert_eq!(range.fetch_start(), DISCOVERY_BLOCK_BYTES);
    assert!(uncovered);
    Ok(())
}

/// #2281: narrowing a probed lane's coverage reaches its extra workers and
/// leaves a whole-blob lane alone, and widening it again when a block's
/// drop ends hands the lane the block's range once more.
#[test]
fn set_coverage_narrows_and_restores_a_probed_lane_and_its_extras() -> anyhow::Result<()> {
    use std::collections::VecDeque;

    use decdn_bao_range::align_range;

    use super::Work;

    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let in_block1 = align_range(DISCOVERY_BLOCK_BYTES, DISCOVERY_BLOCK_BYTES, total)?;
    let mut work = Work::new(VecDeque::from(vec![in_block1]), false);
    let a = work.add_lane(Some(cov(2, &[0, 1])), total);
    let x = work.add_extra(a);
    let b = work.add_lane(None, total);
    let covers1 = |work: &Work, slot: usize| work.coverage.get(slot).is_some_and(|c| c.covers(1));

    work.set_coverage(a, &cov(2, &[0]));
    assert!(
        !covers1(&work, a) && !covers1(&work, x),
        "lane and extra narrow"
    );
    work.set_coverage(b, &cov(2, &[0]));
    assert!(covers1(&work, b), "a whole-blob lane keeps its coverage");

    work.set_coverage(a, &cov(2, &[0, 1]));
    assert!(covers1(&work, a) && covers1(&work, x), "the drop ended");
    let a_cov = cov(2, &[0, 1]);
    let picked = work.pick(a, total, &a_cov, true, &[(0, total)])?;
    assert!(
        picked.is_some_and(|p| p.range.fetch_start() == DISCOVERY_BLOCK_BYTES && !p.uncovered),
        "the restored block's range is covered work for the lane again"
    );
    Ok(())
}

/// A faulted lane's remainder that only that lane covered (#2230): once
/// the lane has stopped, no running lane covers it, so a busy lane not
/// barred from pull-through may grow for it, and its extra worker takes
/// it as an uncovered range. A barred busy lane is not asked, and its
/// extra worker takes nothing outside its coverage.
#[test]
fn a_remainder_only_the_faulted_lane_covered_goes_to_a_busy_lanes_extra() -> anyhow::Result<()> {
    use std::collections::VecDeque;

    use decdn_bao_range::align_range;

    use super::{Picked, Work};

    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let in_block0 = align_range(0, DISCOVERY_BLOCK_BYTES, total)?;
    let in_block1 = align_range(DISCOVERY_BLOCK_BYTES, DISCOVERY_BLOCK_BYTES, total)?;
    let mut work = Work::new(VecDeque::from(vec![in_block0, in_block1]), false);
    let (a_cov, b_cov) = (cov(2, &[0]), cov(2, &[1]));
    let a = work.add_lane(Some(a_cov.clone()), total);
    let b = work.add_lane(Some(b_cov.clone()), total);
    let missing = [(0, total)];
    let a_took = work.pick(a, total, &a_cov, true, &missing)?;
    assert!(a_took.is_some_and(|p| p.range.fetch_start() == 0));
    let b_took = work.pick(b, total, &b_cov, true, &missing)?;
    assert!(b_took.is_some_and(|p| p.range.fetch_start() == DISCOVERY_BLOCK_BYTES));

    // Lane a faults 1 MiB in: its remainder goes back, and it stops.
    let remainder = align_range(MIB, DISCOVERY_BLOCK_BYTES - MIB, total)?;
    work.clear(a)?;
    work.pending.push_back(remainder);
    work.park(a);
    assert!(
        work.uncovered(total),
        "no running lane covers the remainder"
    );

    let candidates = |work: &Work| -> Vec<Vec<usize>> {
        work.growth_wanted(total, |_| true)
            .into_iter()
            .map(|(_, lanes)| lanes)
            .collect()
    };
    work.set_no_uncovered(b, true);
    assert_eq!(
        candidates(&work),
        vec![Vec::<usize>::new()],
        "a busy lane barred from pull-through is not asked"
    );
    work.set_no_uncovered(b, false);
    assert_eq!(
        candidates(&work),
        vec![vec![b]],
        "the busy lane that does not cover the remainder is asked for it"
    );

    let extra = work.add_extra(b);
    work.set_no_uncovered(b, true);
    assert!(
        work.pick(extra, total, &b_cov, false, &[])?.is_none(),
        "the extra worker of a barred lane takes no uncovered range"
    );
    work.set_no_uncovered(b, false);
    let Some(Picked {
        range, uncovered, ..
    }) = work.pick(extra, total, &b_cov, false, &[])?
    else {
        anyhow::bail!("the extra worker takes the remainder");
    };
    assert_eq!(
        (range.fetch_start(), range.fetch_len()),
        (MIB, DISCOVERY_BLOCK_BYTES - MIB)
    );
    assert!(uncovered, "its node serves the remainder by pull-through");

    // A lane with an extra worker running is asked again for a second
    // orphan: its `LaneWiden` bounds its extra workers (#2252).
    work.pending.push_back(align_range(0, MIB, total)?);
    assert_eq!(
        candidates(&work),
        vec![vec![b]],
        "a lane running an extra stream is asked for another"
    );
    // Once the extra ends, the lane may grow again, on the same slot.
    work.clear(extra)?;
    work.end_extra(extra);
    assert_eq!(candidates(&work), vec![vec![b]]);
    let slots = work.in_flight.len();
    assert_eq!(
        work.add_extra(b),
        extra,
        "the ended extra's slot is used again"
    );
    assert_eq!(work.in_flight.len(), slots, "no slot is added");
    Ok(())
}

/// A partial holder that keeps refusing a block outside its coverage is
/// asked for it at most [`ABSENT_AFTER_NOT_FOUND`] times while its bar
/// lasts. Its node cannot serve the block by pull-through, so it is barred
/// from it, and the whole holder serves the block once its lane builds.
///
/// [`ABSENT_AFTER_NOT_FOUND`]: crate::source_set::ABSENT_AFTER_NOT_FOUND
#[tokio::test(start_paused = true)]
async fn a_partial_holder_refusing_an_uncovered_block_is_barred_from_it() -> anyhow::Result<()> {
    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let data = blob(total as usize);
    let n = num_blocks(total);
    let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
    let src_a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&ledger_a))
        .refusing_blocks(&[1], || {
            anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound))
        });
    let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
    let root = src_a.root();
    let (store, dir) = fresh_store(root, total);
    // B's lane builds just inside the bar's time: without the bar, A
    // would be asked for block 1 again each time its cooldown ends.
    let provider = SlowBuild {
        lanes: StaticSources::new(vec![
            candidate(src_a.clone(), ledger_a, 0xA1, Some(cov(n, &[0]))),
            candidate(src_b.clone(), ledger_b, 0xB2, None),
        ])?,
        slow: Address::repeat_byte(0xB2),
        delay: crate::source_set::PULL_THROUGH_BAR.saturating_sub(Duration::from_secs(5)),
        built: std::sync::atomic::AtomicBool::new(false),
    };
    let mut set = SourceSet::new(&provider, root, Arc::default(), provider.lanes.holders());
    let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
    let gate = recovery_gate();
    acquire(
        AcquireTarget {
            store: &store,
            hash: root,
            total_bytes: total,
            ranges: &[(0, total)],
        },
        &mut set,
        &AcquireEnv {
            pacer: &BudgetPacer::new(),
            funder: &no_funding(),
            recovery: &gate,
            credentials: None,
            max_lanes: 2,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        },
    )
    .await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    let asked = src_a
        .opened_ranges()
        .into_iter()
        .filter(|&(start, _)| start >= DISCOVERY_BLOCK_BYTES)
        .count();
    assert!(
        (1..=3).contains(&asked),
        "A was asked for block 1 {asked} times"
    );
    assert!(
        src_b
            .opened_ranges()
            .iter()
            .any(|&(start, _)| start >= DISCOVERY_BLOCK_BYTES),
        "B serves block 1"
    );
    Ok(())
}

/// #2281: a partial holder whose coverage claims block 1 but which refuses
/// it, as a node with a stale coverage record does, loses the block after
/// [`ABSENT_AFTER_NOT_FOUND`] refusals, rather than being asked again each
/// time its cooldown ends. Once the block is outside its coverage, the
/// pull-through bar bounds the rest, and the whole holder serves block 1
/// once its lane builds.
///
/// [`ABSENT_AFTER_NOT_FOUND`]: crate::source_set::ABSENT_AFTER_NOT_FOUND
#[tokio::test(start_paused = true)]
async fn a_partial_holder_refusing_a_covered_block_loses_it_from_its_coverage() -> anyhow::Result<()>
{
    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let data = blob(total as usize);
    let n = num_blocks(total);
    let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
    let src_a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&ledger_a))
        .refusing_blocks(&[1], || {
            anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound))
        });
    let src_b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_b));
    let root = src_a.root();
    let (store, dir) = fresh_store(root, total);
    let provider = SlowBuild {
        lanes: StaticSources::new(vec![
            candidate(src_a.clone(), ledger_a, 0xA1, Some(cov(n, &[0, 1]))),
            candidate(src_b.clone(), ledger_b, 0xB2, None),
        ])?,
        slow: Address::repeat_byte(0xB2),
        delay: crate::source_set::PULL_THROUGH_BAR.saturating_sub(Duration::from_secs(5)),
        built: std::sync::atomic::AtomicBool::new(false),
    };
    let mut set = SourceSet::new(&provider, root, Arc::default(), provider.lanes.holders());
    let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
    let gate = recovery_gate();
    acquire(
        AcquireTarget {
            store: &store,
            hash: root,
            total_bytes: total,
            ranges: &[(0, total)],
        },
        &mut set,
        &AcquireEnv {
            pacer: &BudgetPacer::new(),
            funder: &no_funding(),
            recovery: &gate,
            credentials: None,
            max_lanes: 2,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        },
    )
    .await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    assert!(
        set.holder(Address::repeat_byte(0xA1))
            .and_then(|h| h.coverage.as_ref())
            .is_some_and(|c| !c.covers(1)),
        "block 1 left A's coverage"
    );
    let asked = src_a
        .opened_ranges()
        .into_iter()
        .filter(|&(start, len)| start + len > DISCOVERY_BLOCK_BYTES)
        .count();
    let bound = 2 * crate::source_set::ABSENT_AFTER_NOT_FOUND as usize;
    assert!(
        (1..=bound).contains(&asked),
        "A was asked for block 1 {asked} times"
    );
    assert!(
        src_b
            .opened_ranges()
            .iter()
            .any(|&(start, len)| start + len > DISCOVERY_BLOCK_BYTES),
        "B serves block 1"
    );
    Ok(())
}

/// A sole partial holder barred from the only block left gets one probe
/// once its bar ends, and ends the item when the probe draws the bar
/// again and a discovery finds no one else, as a unanimous `NotFound`
/// does.
#[tokio::test(start_paused = true)]
async fn a_sole_barred_partial_holder_ends_only_the_item() -> anyhow::Result<()> {
    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let src = ScriptedSource::new(blob(total as usize))?
        .paying(Arc::clone(&ledger))
        .refusing_blocks(&[1], || {
            anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound))
        });
    let root = src.root();
    let (store, _dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![candidate(
        src.clone(),
        ledger,
        0xA1,
        Some(cov(num_blocks(total), &[0])),
    )])?;
    let err = run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        1,
        None,
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("must stop"))?;
    assert!(err.downcast_ref::<NoSourceHasBlob>().is_some(), "{err:#}");
    assert!(err.downcast_ref::<UpstreamRefused>().is_some(), "{err:#}");
    assert_eq!(classify(&err), Fault::Fatal(FatalScope::Item));
    let asked = src
        .opened_ranges()
        .into_iter()
        .filter(|&(start, _)| start >= DISCOVERY_BLOCK_BYTES)
        .count();
    assert_eq!(
        asked,
        2 * crate::source_set::ABSENT_AFTER_NOT_FOUND as usize,
        "asked for block 1 until barred, then again after the bar ended"
    );
    Ok(())
}

/// A pull-through target (not a probed holder) that keeps saying
/// `NotFound` ends the item, and the stop carries the refusal.
#[tokio::test(start_paused = true)]
async fn unanimous_not_found_ends_only_the_item() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data)?
        .paying(Arc::clone(&la))
        .with_fault_after(0, || {
            anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound))
        });
    let (root, total) = (a.root(), a.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![candidate(a, la, 0xA1, None)])?.not_probed();
    let err = run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        4,
        None,
    )
    .await
    .err()
    .ok_or_else(|| anyhow::anyhow!("must stop"))?;
    assert!(err.downcast_ref::<NoSourceHasBlob>().is_some(), "{err:#}");
    assert!(err.downcast_ref::<UpstreamRefused>().is_some(), "{err:#}");
    assert_eq!(classify(&err), Fault::Fatal(FatalScope::Item));
    Ok(())
}

/// A sole probed holder that says `NotFound` twice (a load shed, say) and
/// then serves completes the fetch: its `NotFound` only cools it.
#[tokio::test(start_paused = true)]
async fn a_probed_holder_saying_not_found_twice_then_serving_completes() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&la))
        .fault_times_after(2, 0, || {
            anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound))
        });
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![candidate(a, la, 0xA1, None)])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        4,
        None,
    )
    .await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    Ok(())
}

/// Static lanes, one of whose builds takes `delay` and sets `built` when
/// it finishes: a lane build in the middle of an on-chain top-up.
struct SlowBuild {
    lanes: StaticSources<ScriptedSource>,
    slow: Address,
    delay: Duration,
    built: std::sync::atomic::AtomicBool,
}

impl crate::SourceProvider for SlowBuild {
    type Source = ScriptedSource;

    fn discover(&self, hash: [u8; 32]) -> crate::SourceFuture<'_, Vec<crate::Holder>> {
        self.lanes.discover(hash)
    }

    fn connect<'a>(
        &'a self,
        holder: &'a crate::Holder,
    ) -> crate::SourceFuture<'a, StreamCandidate<ScriptedSource>> {
        let slow = holder.provider == self.slow;
        Box::pin(async move {
            if slow {
                tokio::time::sleep(self.delay).await;
                self.built.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            self.lanes.connect(holder).await
        })
    }
}

/// A lane build still in flight when the loop decides to return is awaited,
/// not dropped mid-way.
#[tokio::test(start_paused = true)]
async fn a_lane_build_in_flight_is_awaited_before_acquire_returns() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&la));
    let b = ScriptedSource::new(data)?.paying(Arc::clone(&lb));
    let (root, total) = (a.root(), a.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let provider = SlowBuild {
        lanes: StaticSources::new(vec![
            candidate(a, la, 0xA1, None),
            candidate(b, lb, 0xB2, None),
        ])?,
        slow: Address::repeat_byte(0xB2),
        delay: Duration::from_secs(5),
        built: std::sync::atomic::AtomicBool::new(false),
    };
    let mut set = SourceSet::new(&provider, root, Arc::default(), provider.lanes.holders());
    let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
    let gate = recovery_gate();
    acquire(
        AcquireTarget {
            store: &store,
            hash: root,
            total_bytes: total,
            ranges: &[(0, total)],
        },
        &mut set,
        &AcquireEnv {
            pacer: &BudgetPacer::new(),
            funder: &no_funding(),
            recovery: &gate,
            credentials: None,
            max_lanes: 2,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        },
    )
    .await?;
    assert!(
        provider.built.load(std::sync::atomic::Ordering::SeqCst),
        "the slow build ran to its end before acquire returned"
    );
    Ok(())
}

/// A top-up another fetch of the run credits to a shared lane context
/// reaches this acquire: a source priced out below the new deposit is
/// usable again, so the acquire completes instead of stopping.
#[tokio::test(start_paused = true)]
async fn a_sibling_top_up_on_a_shared_lane_revives_a_priced_out_source() -> anyhow::Result<()> {
    /// Static lanes whose discovery stands in for a sibling entry: from
    /// its second call on, it tops up the shared lane context before it
    /// answers. The first call runs as the fetch starts, before any lane
    /// has run dry.
    struct SiblingTopUp {
        lanes: StaticSources<ScriptedSource>,
        ctx: Arc<Mutex<PoolContext>>,
        calls: std::sync::atomic::AtomicU32,
    }

    impl crate::SourceProvider for SiblingTopUp {
        type Source = ScriptedSource;

        fn discover(&self, hash: [u8; 32]) -> crate::SourceFuture<'_, Vec<crate::Holder>> {
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0 {
                self.ctx.lock().unwrap().deposit = U256::from(200u32);
            }
            self.lanes.discover(hash)
        }

        /// The build takes a second, so the first discovery ends before
        /// any lane names the deposit.
        fn connect<'a>(
            &'a self,
            holder: &'a crate::Holder,
        ) -> crate::SourceFuture<'a, StreamCandidate<ScriptedSource>> {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                self.lanes.connect(holder).await
            })
        }
    }

    let data = blob(1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let ctx = Arc::new(Mutex::new(ctx_with(0xA1, U256::from(100u32))));
    let a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&la))
        .fault_once_after(0, || {
            anyhow::Error::new(PoolExhausted {
                gap_start: 0,
                gap_len: 1,
            })
        });
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let provider = SiblingTopUp {
        lanes: StaticSources::new(vec![candidate_ctx(a, la, Arc::clone(&ctx), None)])?,
        ctx,
        calls: std::sync::atomic::AtomicU32::new(0),
    };
    let mut set = SourceSet::new(&provider, root, Arc::default(), provider.lanes.holders());
    let stop = StopPolicy::new(false, Some(Duration::from_mins(5)), Arc::default());
    let gate = recovery_gate();
    acquire(
        AcquireTarget {
            store: &store,
            hash: root,
            total_bytes: total,
            ranges: &[(0, total)],
        },
        &mut set,
        &AcquireEnv {
            pacer: &BudgetPacer::new(),
            funder: &no_funding(),
            recovery: &gate,
            credentials: None,
            max_lanes: 1,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        },
    )
    .await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    Ok(())
}

/// #2296: a lane whose build refilled the pool carries the new deposit,
/// and the lanes that started before it take it on as it lands. Without
/// it, the earlier lane keeps gating on the deposit it was built with, and
/// reads a gap the refill already paid for as unaffordable.
#[tokio::test(start_paused = true)]
async fn a_refill_a_later_lane_build_made_raises_the_running_lanes() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    // A starts at once and faults, so it is still running when B lands.
    let a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&la))
        .fault_once_after(0, || anyhow::anyhow!("scripted reset"));
    let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let before = U256::from(u64::MAX);
    let refilled = U256::from(u128::MAX);
    let a_ctx = Arc::new(Mutex::new(ctx_with(0xA1, before)));
    let provider = SlowBuild {
        lanes: StaticSources::new(vec![
            candidate_ctx(a, la, Arc::clone(&a_ctx), None),
            candidate_ctx(b, lb, Arc::new(Mutex::new(ctx_with(0xB2, refilled))), None),
        ])?,
        slow: Address::repeat_byte(0xB2),
        delay: Duration::from_secs(1),
        built: std::sync::atomic::AtomicBool::new(false),
    };
    let mut set = SourceSet::new(&provider, root, Arc::default(), provider.lanes.holders());
    let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
    let gate = recovery_gate();
    acquire(
        AcquireTarget {
            store: &store,
            hash: root,
            total_bytes: total,
            ranges: &[(0, total)],
        },
        &mut set,
        &AcquireEnv {
            pacer: &BudgetPacer::new(),
            funder: &no_funding(),
            recovery: &gate,
            credentials: None,
            max_lanes: 2,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        },
    )
    .await?;
    assert!(provider.built.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        a_ctx.lock().unwrap().deposit,
        refilled,
        "the lane built first takes on the deposit the later build read"
    );
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    Ok(())
}

/// #2296: the raise only ever lifts a deposit. A lane whose build read an
/// older, lower row does not lower a lane a top-up already credited.
#[tokio::test(start_paused = true)]
async fn a_lane_built_on_a_lower_deposit_never_lowers_the_running_lanes() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    // A starts at once and faults, so it is still running when B lands.
    let a = ScriptedSource::new(data.clone())?
        .paying(Arc::clone(&la))
        .fault_once_after(0, || anyhow::anyhow!("scripted reset"));
    let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let before = U256::from(u128::MAX);
    let refilled = U256::from(u64::MAX);
    let a_ctx = Arc::new(Mutex::new(ctx_with(0xA1, before)));
    let provider = SlowBuild {
        lanes: StaticSources::new(vec![
            candidate_ctx(a, la, Arc::clone(&a_ctx), None),
            candidate_ctx(b, lb, Arc::new(Mutex::new(ctx_with(0xB2, refilled))), None),
        ])?,
        slow: Address::repeat_byte(0xB2),
        delay: Duration::from_secs(1),
        built: std::sync::atomic::AtomicBool::new(false),
    };
    let mut set = SourceSet::new(&provider, root, Arc::default(), provider.lanes.holders());
    let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
    let gate = recovery_gate();
    acquire(
        AcquireTarget {
            store: &store,
            hash: root,
            total_bytes: total,
            ranges: &[(0, total)],
        },
        &mut set,
        &AcquireEnv {
            pacer: &BudgetPacer::new(),
            funder: &no_funding(),
            recovery: &gate,
            credentials: None,
            max_lanes: 2,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        },
    )
    .await?;
    assert!(provider.built.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        a_ctx.lock().unwrap().deposit,
        before,
        "a later build that read a lower deposit does not lower a running lane"
    );
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    Ok(())
}

/// An acquire that starts with no holder discovers one and completes.
#[tokio::test(start_paused = true)]
async fn an_empty_set_discovers_its_holders_and_completes() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&la));
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![candidate(a, la, 0xA1, None)])?;
    let mut set = SourceSet::new(&provider, root, Arc::default(), Vec::new());
    let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
    let gate = recovery_gate();
    acquire(
        AcquireTarget {
            store: &store,
            hash: root,
            total_bytes: total,
            ranges: &[(0, total)],
        },
        &mut set,
        &AcquireEnv {
            pacer: &BudgetPacer::new(),
            funder: &no_funding(),
            recovery: &gate,
            credentials: None,
            max_lanes: 4,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        },
    )
    .await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn only_the_listed_ranges_are_fetched() -> anyhow::Result<()> {
    let data = blob(8 * 1024 * 1024);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data)?.paying(Arc::clone(&la));
    let (root, total) = (a.root(), a.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None)])?;
    let mut set = SourceSet::new(&provider, root, Arc::default(), provider.holders());
    let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
    let gate = recovery_gate();
    let mib = 1024 * 1024;
    acquire(
        AcquireTarget {
            store: &store,
            hash: root,
            total_bytes: total,
            ranges: &[(0, mib), (4 * mib, mib)],
        },
        &mut set,
        &AcquireEnv {
            pacer: &BudgetPacer::new(),
            funder: &no_funding(),
            recovery: &gate,
            credentials: None,
            max_lanes: 4,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        },
    )
    .await?;
    assert!(
        a.opened_ranges()
            .iter()
            .all(|&(s, l)| s + l <= mib || (s >= 4 * mib && s + l <= 5 * mib)),
        "{:?}",
        a.opened_ranges()
    );
    let present = ranges_content_len(&store.present_ranges().await?, total);
    assert_eq!(present, 2 * mib, "exactly the two listed ranges landed");
    Ok(())
}

// ---- the size is a hint: grow and shrink the bound ----

const MIB: u64 = 1024 * 1024;

#[test]
fn a_claim_with_nothing_past_it_grows_by_one_seed() {
    use super::{SEED, grown_bound};
    let gib = 1 << 30;
    assert_eq!(grown_bound(gib, gib, 0), gib + SEED);
    // A claim off the SEED grid rounds up to it.
    let gb = 1_000_000_000;
    let want = (gb + SEED).div_ceil(SEED) * SEED;
    assert_eq!(grown_bound(gb, gb, 0), want);
    assert_eq!(want % SEED, 0);
}

#[test]
fn bytes_past_the_claim_grow_the_bound_twice_as_far() {
    use super::{SEED, grown_bound};
    let (gb, mb): (u64, u64) = (1_000_000_000, 1_000_000);
    let want = (gb + 100 * mb).div_ceil(SEED) * SEED;
    assert_eq!(grown_bound(gb, gb, 50 * mb), want);
}

#[test]
fn a_grown_bound_saturates_near_the_top() {
    use super::grown_bound;
    assert_eq!(grown_bound(u64::MAX - 10, 0, 0), u64::MAX);
    assert_eq!(grown_bound(1, 0, u64::MAX), u64::MAX);
    assert_eq!(grown_bound(1, u64::MAX, 1), u64::MAX);
}

/// The bounds the growth rule steps through from a first claim of `c0`
/// while every source honestly serves a `truth`-byte blob: each round
/// fetches up to the bound, so everything past `c0` below it is present.
fn predicted_growth(c0: u64, truth: u64) -> Vec<u64> {
    let mut bounds = Vec::new();
    let mut bound = c0;
    while bound < truth {
        bound = super::grown_bound(bound, c0, bound.saturating_sub(c0));
        bounds.push(bound);
    }
    bounds
}

/// Acquire the whole blob into `store` from `provider`, recording every
/// `(position, total)` the progress callback reports.
async fn acquire_recording<S: BlobSource>(
    store: &ClientRangedStore,
    provider: &StaticSources<S>,
    root: [u8; 32],
    lanes: usize,
) -> (anyhow::Result<()>, Vec<(u64, u64)>) {
    let samples: Arc<Mutex<Vec<(u64, u64)>>> = Arc::default();
    let cb_samples = Arc::clone(&samples);
    let on_progress: Box<crate::ProgressCallback> = Box::new(move |position, total| {
        if let Ok(mut s) = cb_samples.lock() {
            s.push((position, total));
        }
    });
    let result = run_acquire_with(
        store,
        provider,
        root,
        &BudgetPacer::new(),
        &no_funding(),
        Knobs {
            on_progress: Some(&on_progress),
            ..Knobs::lanes(lanes)
        },
    )
    .await;
    let samples = samples.lock().map(|s| s.clone()).unwrap_or_default();
    (result, samples)
}

/// Honest lanes over `data`, one per provider byte in `providers`.
fn honest_lanes(
    data: &[u8],
    providers: &[u8],
) -> anyhow::Result<(Vec<ScriptedSource>, StaticSources<ScriptedSource>)> {
    let mut sources = Vec::new();
    let mut candidates = Vec::new();
    for &p in providers {
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src = ScriptedSource::new(data.to_vec())?.paying(Arc::clone(&ledger));
        candidates.push(candidate(src.clone(), ledger, p, None));
        sources.push(src);
    }
    Ok((sources, StaticSources::new(candidates)?))
}

/// A first claim far below the blob grows by the rule until a leg
/// verifies the true final chunk, and the fetch completes byte-identical.
#[tokio::test(start_paused = true)]
async fn a_too_small_claim_grows_until_the_true_end_is_proven() -> anyhow::Result<()> {
    let (c0, truth) = (MIB, 70 * MIB);
    let data = blob(truth as usize);
    let (sources, provider) = honest_lanes(&data, &[0xA1, 0xB2])?;
    let root = sources
        .first()
        .map(ScriptedSource::root)
        .unwrap_or_default();
    let (store, dir) = fresh_store(root, c0);
    let (result, samples) = acquire_recording(&store, &provider, root, 2).await;
    result?;
    assert_eq!(store.proven(), Some(truth));
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    let grown: std::collections::BTreeSet<u64> = samples
        .iter()
        .map(|&(_, total)| total)
        .filter(|&total| total != c0 && total != truth)
        .collect();
    let predicted: std::collections::BTreeSet<u64> = predicted_growth(c0, truth)
        .into_iter()
        .filter(|&b| b != truth)
        .collect();
    assert_eq!(grown, predicted, "one growth round per the rule");
    assert_eq!(predicted.len(), 1);
    Ok(())
}

/// A first claim past the blob shrinks once a leg verifies the true final
/// chunk: the fetch completes and the bound ends at the proven size.
#[tokio::test(start_paused = true)]
async fn a_too_big_claim_shrinks_when_a_leg_proves_the_end() -> anyhow::Result<()> {
    let (c0, truth) = (200 * MIB, 70 * MIB);
    let data = blob(truth as usize);
    let (sources, provider) = honest_lanes(&data, &[0xA1, 0xB2])?;
    let root = sources
        .first()
        .map(ScriptedSource::root)
        .unwrap_or_default();
    let (store, dir) = fresh_store(root, c0);
    let (result, _) = acquire_recording(&store, &provider, root, 2).await;
    result?;
    assert_eq!(store.proven(), Some(truth));
    assert_eq!(store.bound(), truth, "the bound ends at the proven size");
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    Ok(())
}

/// A proven size clips the work: nothing at or past it stays pending, a
/// range across it is cut there, and a lane whose range starts past it is
/// cancelled.
#[test]
fn a_proven_size_clips_pending_and_in_flight_work() -> anyhow::Result<()> {
    use std::collections::VecDeque;
    use std::sync::atomic::Ordering;

    use decdn_bao_range::align_range;

    let old = 200 * MIB;
    let proven = 70 * MIB;
    let pending = VecDeque::from(vec![
        align_range(0, 10 * MIB, old)?,
        align_range(60 * MIB, 20 * MIB, old)?,
        align_range(100 * MIB, 20 * MIB, old)?,
    ]);
    let mut work = super::Work::new(pending, false);
    let past = work.add_lane(None, old);
    let across = work.add_lane(None, old);
    if let Some(slot) = work.in_flight.get_mut(past) {
        *slot = Some((90 * MIB, 10 * MIB));
    }
    if let Some(slot) = work.in_flight.get_mut(across) {
        *slot = Some((65 * MIB, 10 * MIB));
    }
    work.clip(proven)?;
    let pending: Vec<(u64, u64)> = work
        .pending
        .iter()
        .map(|seg| (seg.fetch_start(), seg.fetch_end()))
        .collect();
    assert_eq!(pending, vec![(0, 10 * MIB), (60 * MIB, proven)]);
    assert!(pending.iter().all(|&(_, end)| end <= proven));
    assert_eq!(
        work.in_flight.get(across).copied().flatten(),
        Some((65 * MIB, 5 * MIB))
    );
    let cancelled = |i: usize| {
        work.cancel
            .get(i)
            .is_some_and(|h| h.flag.load(Ordering::Acquire))
    };
    assert!(cancelled(past), "a lane past the proven size stops");
    assert!(!cancelled(across), "a lane across it finishes at it");
    Ok(())
}

/// A source that signs a size one byte short is never believed: its size
/// is only the first claim, the honest sources prove the true one, and the
/// fetch completes.
#[tokio::test(start_paused = true)]
async fn a_lying_small_first_claim_never_fails_the_fetch() -> anyhow::Result<()> {
    let truth = 3 * MIB + 12_345;
    let data = blob(truth as usize);
    let mut candidates = Vec::new();
    let mut root = [0; 32];
    for (p, signs) in [(0xA1, truth - 1), (0xB2, truth), (0xC3, truth)] {
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let src = ScriptedSource::new(data.clone())?
            .signing_size(signs)
            .paying(Arc::clone(&ledger));
        root = src.root();
        candidates.push(candidate(src, ledger, p, None));
    }
    let provider = StaticSources::new(candidates)?;
    let (store, dir) = fresh_store(root, truth - 1);
    let (result, _) = acquire_recording(&store, &provider, root, 3).await;
    result?;
    assert_eq!(store.proven(), Some(truth));
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    Ok(())
}

/// Coverage is a preference (#2225): two partial holders whose coverage
/// leaves the last block to nobody still complete the fetch. One of them
/// takes that block, served by pull-through, while the other is still on
/// its own covered block.
#[tokio::test(start_paused = true)]
async fn blocks_no_lane_covers_are_taken_by_a_partial_holder() -> anyhow::Result<()> {
    let total = 2 * DISCOVERY_BLOCK_BYTES + MIB;
    let data = blob(total as usize);
    let n = num_blocks(total);
    let ledger_a = Arc::new(PoolLedger::new(Cumulative::default()));
    let ledger_b = Arc::new(PoolLedger::new(Cumulative::default()));
    let src_a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&ledger_a));
    let src_b = ScriptedSource::new(data.clone())?
        .slow_to_start(Duration::from_secs(5))
        .paying(Arc::clone(&ledger_b));
    let root = src_a.root();
    let (store, dir) = fresh_store(root, total);
    let provider = StaticSources::new(vec![
        candidate(src_a.clone(), ledger_a, 0xA1, Some(cov(n, &[0]))),
        candidate(src_b.clone(), ledger_b, 0xB2, Some(cov(n, &[1]))),
    ])?;
    run_acquire(
        &store,
        &provider,
        root,
        total,
        &BudgetPacer::new(),
        &no_funding(),
        2,
        None,
    )
    .await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    let last_block = 2 * DISCOVERY_BLOCK_BYTES;
    let took_last = src_a
        .timeline()
        .into_iter()
        .chain(src_b.timeline())
        .filter(|&(start, _, _)| start >= last_block)
        .map(|(_, opened, _)| opened)
        .min()
        .ok_or_else(|| anyhow::anyhow!("nobody opened the uncovered block"))?;
    let covered_done = src_b
        .timeline()
        .into_iter()
        .filter_map(|(start, _, finished)| (start < last_block).then_some(finished).flatten())
        .max()
        .ok_or_else(|| anyhow::anyhow!("B finished no covered leg"))?;
    assert!(
        took_last < covered_done,
        "the uncovered block started before the covered work ended"
    );
    Ok(())
}

// ---- the caller's size cap bounds every claim ----

/// Acquire `data`'s blob from one honest lane into a store sized by
/// `claim`, under `cap`. Returns the result, every progress total, and
/// the store.
async fn acquire_capped(
    data: &[u8],
    claim: u64,
    cap: u64,
) -> anyhow::Result<(
    anyhow::Result<()>,
    Vec<u64>,
    ClientRangedStore,
    tempfile::TempDir,
)> {
    let (sources, provider) = honest_lanes(data, &[0xA1])?;
    let root = sources
        .first()
        .map(ScriptedSource::root)
        .unwrap_or_default();
    let (store, dir) = fresh_store(root, claim);
    let totals: Arc<Mutex<Vec<u64>>> = Arc::default();
    let seen = Arc::clone(&totals);
    let on_progress: Box<crate::ProgressCallback> = Box::new(move |_, total| {
        if let Ok(mut t) = seen.lock() {
            t.push(total);
        }
    });
    let result = run_acquire_with(
        &store,
        &provider,
        root,
        &BudgetPacer::new(),
        &no_funding(),
        Knobs {
            on_progress: Some(&on_progress),
            max_blob_bytes: cap,
            ..Knobs::lanes(1)
        },
    )
    .await;
    let totals = totals.lock().map(|t| t.clone()).unwrap_or_default();
    Ok((result, totals, store, dir))
}

/// A first claim above the cap is clamped to it before any work is sized:
/// no bound past the cap is ever planned, and a blob within the cap
/// completes.
#[tokio::test(start_paused = true)]
async fn a_claim_above_the_cap_is_clamped_to_it() -> anyhow::Result<()> {
    let data = blob(3 * MIB as usize);
    let cap = 8 * MIB;
    let (result, totals, store, dir) = acquire_capped(&data, u64::MAX / 2, cap).await?;
    result?;
    assert!(totals.iter().all(|&t| t <= cap), "{totals:?}");
    assert_eq!(store.proven(), Some(3 * MIB));
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    Ok(())
}

/// Growth never raises the bound past the cap: a short claim grows to the
/// cap at most, and a blob within it completes.
#[tokio::test(start_paused = true)]
async fn growth_stops_at_the_cap() -> anyhow::Result<()> {
    let data = blob(3 * MIB as usize);
    let cap = 5 * MIB;
    let (result, totals, store, _dir) = acquire_capped(&data, MIB, cap).await?;
    result?;
    assert!(totals.iter().all(|&t| t <= cap), "{totals:?}");
    assert!(
        totals.contains(&cap),
        "the claim grew to the cap: {totals:?}"
    );
    assert_eq!(store.proven(), Some(3 * MIB));
    Ok(())
}

/// A blob that holds bytes past the cap ends the item with `BlobTooLarge`
/// once the bound reaches the cap with no size proven, whether the claim
/// was short or above the cap.
#[tokio::test(start_paused = true)]
async fn a_blob_past_the_cap_ends_the_item() -> anyhow::Result<()> {
    let data = blob(3 * MIB as usize);
    let cap = 2 * MIB;
    for claim in [MIB, 3 * MIB] {
        let (result, totals, _store, _dir) = acquire_capped(&data, claim, cap).await?;
        let err = result
            .err()
            .ok_or_else(|| anyhow::anyhow!("a blob past the cap must fail"))?;
        assert!(
            err.downcast_ref::<crate::BlobTooLarge>().is_some(),
            "{err:#}"
        );
        assert_eq!(classify(&err), Fault::Fatal(FatalScope::Item));
        assert!(totals.iter().all(|&t| t <= cap), "{totals:?}");
    }
    Ok(())
}

/// A blob exactly at the cap completes: the cap is a ceiling, not a
/// bound it must stay under.
#[tokio::test(start_paused = true)]
async fn a_blob_exactly_at_the_cap_completes() -> anyhow::Result<()> {
    let data = blob(2 * MIB as usize);
    let (result, _, store, _dir) = acquire_capped(&data, 3 * MIB, 2 * MIB).await?;
    result?;
    assert_eq!(store.proven(), Some(2 * MIB));
    Ok(())
}

/// A cap off the chunk-group grid: a leg that ends at the cap is served to
/// its group's end and can prove a size past the cap. That proven size
/// still ends the item with `BlobTooLarge`.
#[tokio::test(start_paused = true)]
async fn a_proven_size_past_an_unaligned_cap_ends_the_item() -> anyhow::Result<()> {
    let data = blob(2 * MIB as usize + 100);
    let cap = 2 * MIB + 50;
    let (result, _, store, _dir) = acquire_capped(&data, 3 * MIB, cap).await?;
    let err = result
        .err()
        .ok_or_else(|| anyhow::anyhow!("a proven size past the cap must fail"))?;
    assert!(
        err.downcast_ref::<crate::BlobTooLarge>().is_some(),
        "{err:#}"
    );
    assert_eq!(classify(&err), Fault::Fatal(FatalScope::Item));
    assert_eq!(store.proven(), Some(2 * MIB + 100));
    Ok(())
}

// ---- a busy lane grows for a faulted lane's remainder (#2231) ----

/// Counts what a [`super::LaneWiden`] granted and got back.
#[derive(Default)]
struct WidenCount {
    granted: std::sync::atomic::AtomicUsize,
    released: std::sync::atomic::AtomicUsize,
    /// The most grants held at once.
    peak: std::sync::atomic::AtomicUsize,
    /// Each `grow` call's kind and whether it granted, in call order.
    calls: Mutex<Vec<(super::GrowFor, bool)>>,
}

impl WidenCount {
    fn granted(&self) -> usize {
        self.granted.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn released(&self) -> usize {
        self.released.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn peak(&self) -> usize {
        self.peak.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Each `grow` call's kind and whether it granted, in call order.
    fn calls(&self) -> Vec<(super::GrowFor, bool)> {
        self.calls.lock().map(|c| c.clone()).unwrap_or_default()
    }

    /// Count one `grow` call of `kind` that granted a stream or not.
    fn grew(&self, kind: super::GrowFor, granted: bool) {
        use std::sync::atomic::Ordering;
        self.granted
            .fetch_add(usize::from(granted), Ordering::SeqCst);
        let held = self.granted().saturating_sub(self.released());
        self.peak.fetch_max(held, Ordering::SeqCst);
        if let Ok(mut calls) = self.calls.lock() {
            calls.push((kind, granted));
        }
    }
}

/// A lane's widen hooks over `room` stream permits, like the CLI's permit
/// grant: `grow` hands out only what is free now, and `release` frees one.
fn counting_widen(room: usize) -> (super::LaneWiden, Arc<WidenCount>) {
    use std::sync::atomic::Ordering;
    let count = Arc::new(WidenCount::default());
    let (on_grow, on_release) = (Arc::clone(&count), Arc::clone(&count));
    let widen = super::LaneWiden::new(
        move |kind| {
            let held = on_grow.granted().saturating_sub(on_grow.released());
            let give = held < room;
            on_grow.grew(kind, give);
            give
        },
        move || {
            on_release.released.fetch_add(1, Ordering::SeqCst);
        },
    );
    (widen, count)
}

/// Static lanes that report every source fault the set records.
struct FaultLog<'a> {
    inner: &'a StaticSources<ScriptedSource>,
    faulted: Mutex<Vec<Address>>,
}

impl crate::SourceProvider for FaultLog<'_> {
    type Source = ScriptedSource;

    fn discover(&self, hash: [u8; 32]) -> crate::SourceFuture<'_, Vec<crate::Holder>> {
        self.inner.discover(hash)
    }

    fn connect<'b>(
        &'b self,
        holder: &'b crate::Holder,
    ) -> crate::SourceFuture<'b, StreamCandidate<ScriptedSource>> {
        self.inner.connect(holder)
    }

    fn on_source_fault(&self, holder: &crate::Holder) {
        if let Ok(mut f) = self.faulted.lock() {
            f.push(holder.provider);
        }
    }
}

/// Static lanes plus holders pushed after the start, each after its own
/// delay, and a count of discovery calls. Discovery returns `found`.
struct Arriving<'a> {
    inner: &'a StaticSources<ScriptedSource>,
    late: Mutex<Option<Vec<(Duration, crate::Holder)>>>,
    found: Vec<crate::Holder>,
    discovers: std::sync::atomic::AtomicUsize,
}

impl<'a> Arriving<'a> {
    fn new(
        inner: &'a StaticSources<ScriptedSource>,
        late: Vec<(Duration, crate::Holder)>,
        found: Vec<crate::Holder>,
    ) -> Self {
        Self {
            inner,
            late: Mutex::new(Some(late)),
            found,
            discovers: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn discovers(&self) -> usize {
        self.discovers.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl crate::SourceProvider for Arriving<'_> {
    type Source = ScriptedSource;

    fn discover(&self, _hash: [u8; 32]) -> crate::SourceFuture<'_, Vec<crate::Holder>> {
        self.discovers
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let found = self.found.clone();
        Box::pin(async move { Ok(found) })
    }

    fn connect<'b>(
        &'b self,
        holder: &'b crate::Holder,
    ) -> crate::SourceFuture<'b, StreamCandidate<ScriptedSource>> {
        self.inner.connect(holder)
    }

    fn arrivals(&self) -> Option<crate::SourceStream<'_, crate::Holder>> {
        use futures_util::StreamExt as _;
        let late = self.late.lock().ok()?.take()?;
        Some(Box::pin(futures_util::stream::iter(late).then(
            |(after, holder)| async move {
                tokio::time::sleep(after).await;
                holder
            },
        )))
    }
}

/// `provider`'s static holder for the lane built with `byte`.
fn holder_of(provider: &StaticSources<ScriptedSource>, byte: u8) -> anyhow::Result<crate::Holder> {
    provider
        .holders()
        .into_iter()
        .find(|h| h.provider == Address::repeat_byte(byte))
        .ok_or_else(|| anyhow::anyhow!("no holder {byte:#x}"))
}

/// A holder pushed one second in takes a free lane beside a busy one, and
/// both lanes deliver.
#[tokio::test(start_paused = true)]
async fn a_late_holder_takes_a_free_lane() -> anyhow::Result<()> {
    let data = blob(32 * MIB as usize);
    let (la, lb) = (
        Arc::new(PoolLedger::new(Cumulative::default())),
        Arc::new(PoolLedger::new(Cumulative::default())),
    );
    let a = busy(&data, &la)?;
    let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
    let (root, total) = (a.root(), a.total_bytes());
    let lanes = StaticSources::new(vec![
        candidate(a.clone(), la, 0xA1, None),
        candidate(b.clone(), lb, 0xB2, None),
    ])?;
    let provider = Arriving::new(
        &lanes,
        vec![(Duration::from_secs(1), holder_of(&lanes, 0xB2)?)],
        Vec::new(),
    );
    let (store, dir) = fresh_store(root, total);
    acquire_over(&store, &provider, vec![holder_of(&lanes, 0xA1)?], root, 2).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert!(a.delivered_bytes() > 0);
    assert!(b.delivered_bytes() > 0, "the late holder delivered");
    Ok(())
}

/// #2303: a holder of only the last block arrives while the full holder
/// streams the whole blob as one range. It steals from inside its block,
/// and the stolen part is fetched once.
#[tokio::test(start_paused = true)]
async fn a_late_tail_holder_steals_the_covered_tail_and_fetches_each_byte_once()
-> anyhow::Result<()> {
    const B: u64 = DISCOVERY_BLOCK_BYTES;
    let data = blob(2 * B as usize);
    let (la, lb) = (
        Arc::new(PoolLedger::new(Cumulative::default())),
        Arc::new(PoolLedger::new(Cumulative::default())),
    );
    let a = busy(&data, &la)?;
    let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
    let (root, total) = (a.root(), a.total_bytes());
    let lanes = StaticSources::new(vec![
        candidate(a.clone(), la, 0xA1, None),
        candidate(b.clone(), lb, 0xB2, Some(cov(2, &[1]))),
    ])?;
    let provider = Arriving::new(
        &lanes,
        vec![(Duration::from_secs(1), holder_of(&lanes, 0xB2)?)],
        Vec::new(),
    );
    let (store, dir) = fresh_store(root, total);
    acquire_over(&store, &provider, vec![holder_of(&lanes, 0xA1)?], root, 2).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert_eq!(
        a.opened_ranges().first().copied(),
        Some((0, total)),
        "the full holder starts on the whole blob, so the tail holder can only steal"
    );
    assert!(b.delivered_bytes() > 0, "the tail holder stole its block");
    assert!(
        b.opened_ranges().iter().all(|&(s, _)| s >= B),
        "the tail holder opens only inside its block: {:?}",
        b.opened_ranges()
    );
    assert!(
        a.delivered_bytes() + b.delivered_bytes() <= total + decdn_bao_range::CHUNK_GROUP_BYTES,
        "each byte is fetched once: a {} + b {} for {total}",
        a.delivered_bytes(),
        b.delivered_bytes()
    );
    Ok(())
}

/// The only starting holder refuses every stream. With a holder still to
/// arrive, the loop asks for no discovery and completes from the arrival.
#[tokio::test(start_paused = true)]
async fn a_late_holder_rescues_a_faulted_start() -> anyhow::Result<()> {
    let data = blob(8 * MIB as usize);
    let (la, lb) = (
        Arc::new(PoolLedger::new(Cumulative::default())),
        Arc::new(PoolLedger::new(Cumulative::default())),
    );
    let a = ScriptedSource::new(data.clone())?
        .refusing_opens_from(0, || anyhow::anyhow!("scripted refusal"))
        .paying(Arc::clone(&la));
    let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
    let (root, total) = (a.root(), a.total_bytes());
    let lanes = StaticSources::new(vec![
        candidate(a, la, 0xA1, None),
        candidate(b, lb, 0xB2, None),
    ])?;
    let provider = Arriving::new(
        &lanes,
        vec![(Duration::from_secs(2), holder_of(&lanes, 0xB2)?)],
        Vec::new(),
    );
    let (store, dir) = fresh_store(root, total);
    acquire_over(&store, &provider, vec![holder_of(&lanes, 0xA1)?], root, 1).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert_eq!(
        provider.discovers(),
        0,
        "no discovery while arrivals are open"
    );
    Ok(())
}

/// Arrivals that end with nothing hand the loop back to discovery.
#[tokio::test(start_paused = true)]
async fn discovery_resumes_once_arrivals_end() -> anyhow::Result<()> {
    let data = blob(8 * MIB as usize);
    let (la, lb) = (
        Arc::new(PoolLedger::new(Cumulative::default())),
        Arc::new(PoolLedger::new(Cumulative::default())),
    );
    let a = ScriptedSource::new(data.clone())?
        .refusing_opens_from(0, || anyhow::anyhow!("scripted refusal"))
        .paying(Arc::clone(&la));
    let b = ScriptedSource::new(data.clone())?.paying(Arc::clone(&lb));
    let (root, total) = (a.root(), a.total_bytes());
    let lanes = StaticSources::new(vec![
        candidate(a, la, 0xA1, None),
        candidate(b, lb, 0xB2, None),
    ])?;
    let provider = Arriving::new(&lanes, Vec::new(), vec![holder_of(&lanes, 0xB2)?]);
    let (store, dir) = fresh_store(root, total);
    acquire_over(&store, &provider, vec![holder_of(&lanes, 0xA1)?], root, 1).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert!(
        provider.discovers() >= 1,
        "discovery ran once arrivals ended"
    );
    Ok(())
}

/// Acquire the whole blob into `store` over `provider`'s lanes, with the
/// lane watchdog on.
async fn acquire_over<P: crate::SourceProvider>(
    store: &ClientRangedStore,
    provider: &P,
    holders: Vec<crate::Holder>,
    root: [u8; 32],
    lanes: usize,
) -> anyhow::Result<()> {
    let total = store.total_bytes();
    let mut set = SourceSet::new(provider, root, Arc::default(), holders);
    let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
    let gate = recovery_gate();
    let (pacer, funder) = (BudgetPacer::new(), no_funding());
    let whole = [(0, total)];
    acquire(
        AcquireTarget {
            store,
            hash: root,
            total_bytes: total,
            ranges: &whole,
        },
        &mut set,
        &AcquireEnv {
            pacer: &pacer,
            funder: &funder,
            recovery: &gate,
            credentials: None,
            max_lanes: lanes,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        },
    )
    .await
}

/// A lane that dies after its first MiB and refuses every later stream.
fn dead_after_first_mib(data: &[u8], ledger: &Arc<PoolLedger>) -> anyhow::Result<ScriptedSource> {
    Ok(ScriptedSource::new(data.to_vec())?
        .with_fault_after(MIB as usize, || anyhow::anyhow!("scripted reset"))
        .refusing_opens_from(1, || anyhow::anyhow!("scripted refusal"))
        .paying(Arc::clone(ledger)))
}

/// A lane busy with a long range: every leg pauses 5 s after its first MiB.
fn busy(data: &[u8], ledger: &Arc<PoolLedger>) -> anyhow::Result<ScriptedSource> {
    Ok(ScriptedSource::new(data.to_vec())?
        .stall_after(MIB, Duration::from_secs(5))
        .paying(Arc::clone(ledger)))
}

/// Three lanes, no reserve: one node dies mid-range while the other two
/// are busy with long ranges. A busy lane that covers the dead node's
/// remainder takes it on one extra stream, so the remainder starts before
/// either busy lane finishes its own range, and the grant comes back.
#[tokio::test(start_paused = true)]
async fn a_dead_nodes_remainder_is_taken_by_a_busy_covering_lane() -> anyhow::Result<()> {
    let data = blob(48 * MIB as usize);
    let ledgers: Vec<Arc<PoolLedger>> = (0..3)
        .map(|_| Arc::new(PoolLedger::new(Cumulative::default())))
        .collect();
    let [la, lb, lc] = ledgers.as_slice() else {
        anyhow::bail!("three ledgers");
    };
    let a = dead_after_first_mib(&data, la)?;
    let b = busy(&data, lb)?;
    let c = busy(&data, lc)?;
    let (widen_b, count_b) = counting_widen(1);
    let (widen_c, count_c) = counting_widen(1);
    let mut cand_b = candidate(b.clone(), Arc::clone(lb), 0xB2, None);
    cand_b.widen = Some(widen_b);
    let mut cand_c = candidate(c.clone(), Arc::clone(lc), 0xC3, None);
    cand_c.widen = Some(widen_c);
    let provider = StaticSources::new(vec![
        candidate(a.clone(), Arc::clone(la), 0xA1, None),
        cand_b,
        cand_c,
    ])?;
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    acquire_over(&store, &provider, provider.holders(), root, 3).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    let (dead_start, _, _) = a
        .timeline()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("A opened its range"))?;
    let dead_end = dead_start + total / 3;
    let remainder_opened = b
        .timeline()
        .into_iter()
        .chain(c.timeline())
        .filter(|&(start, _, _)| start > dead_start && start < dead_end)
        .map(|(_, opened, _)| opened)
        .min()
        .ok_or_else(|| anyhow::anyhow!("a busy lane took the remainder"))?;
    let first_finish = |src: &ScriptedSource| {
        src.timeline()
            .first()
            .and_then(|&(_, _, finished)| finished)
    };
    let busy_done = first_finish(&b)
        .into_iter()
        .chain(first_finish(&c))
        .min()
        .ok_or_else(|| anyhow::anyhow!("the busy lanes finished their ranges"))?;
    assert!(
        remainder_opened < busy_done,
        "the remainder started before either busy lane finished its own range"
    );
    let granted = count_b.granted() + count_c.granted();
    assert!(granted >= 1, "a busy lane grew one extra stream");
    assert_eq!(
        count_b.released() + count_c.released(),
        granted,
        "every extra stream gave its grant back"
    );
    Ok(())
}

/// A node that refuses the extra stream is not cooled for it: the refusal
/// stops only that stream, the lane's own worker takes the range once its
/// own range ends, the fetch completes, and the grant comes back.
#[tokio::test(start_paused = true)]
async fn a_fault_on_an_extra_stream_does_not_cool_the_node() -> anyhow::Result<()> {
    let data = blob(32 * MIB as usize);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = dead_after_first_mib(&data, &la)?;
    // B's second open is the extra stream: it refuses it.
    let b = busy(&data, &lb)?.refusing_open(1, || anyhow::anyhow!("scripted overload"));
    let (widen, count) = counting_widen(1);
    let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, None);
    cand_b.widen = Some(widen);
    let lanes = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None), cand_b])?;
    let provider = FaultLog {
        inner: &lanes,
        faulted: Mutex::new(Vec::new()),
    };
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    acquire_over(&store, &provider, lanes.holders(), root, 2).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    // A returns after its cooldown, faults again, and B may be asked again
    // once its first extra stream has ended: at most one at a time.
    assert!(count.granted() >= 1, "B grew an extra stream");
    assert_eq!(
        count.released(),
        count.granted(),
        "every grant came back, the refused one included"
    );
    let faulted = provider
        .faulted
        .lock()
        .map(|f| f.clone())
        .unwrap_or_default();
    assert!(
        !faulted.contains(&Address::repeat_byte(0xB2)),
        "the refused extra stream must not cool B: {faulted:?}"
    );
    assert!(
        faulted.contains(&Address::repeat_byte(0xA1)),
        "A's death is A's"
    );
    Ok(())
}

/// Open `gate` once `ready` holds, checking each virtual millisecond.
async fn open_when(ready: impl Fn() -> bool, gate: &tokio::sync::watch::Sender<bool>) {
    while !ready() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    gate.send_replace(true);
}

/// How many times `provider` recorded a fault for the node `byte` names.
fn faults_of(provider: &FaultLog<'_>, byte: u8) -> usize {
    provider
        .faulted
        .lock()
        .map(|f| {
            f.iter()
                .filter(|&&p| p == Address::repeat_byte(byte))
                .count()
        })
        .unwrap_or_default()
}

/// Acquire over `provider` while `release` opens the scenario's gates in
/// order. A gate that never opens ends the test with an error.
async fn acquire_gated(
    store: &ClientRangedStore,
    provider: &FaultLog<'_>,
    root: [u8; 32],
    release: impl std::future::Future<Output = ()>,
) -> anyhow::Result<()> {
    let fetch = acquire_over(store, provider, provider.inner.holders(), root, 2);
    let (fetched, ()) = tokio::time::timeout(
        Duration::from_mins(5),
        futures_util::future::join(fetch, release),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the fetch ended or stalled before a gate opened"))?;
    fetched
}

/// A lane's last live worker faults for the lane (#2231): when the lane's
/// own worker has faulted and stopped, a fault on its extra stream cools
/// the node too.
#[tokio::test(start_paused = true)]
async fn an_extra_stream_that_outlives_its_lane_faults_for_the_lane() -> anyhow::Result<()> {
    let data = blob(32 * MIB as usize);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = dead_after_first_mib(&data, &la)?;
    // B's first two readers (its own range, then the extra stream opened
    // for A's remainder) fault after 4 MiB. The own leg's fault waits
    // until the extra stream has opened, and the extra's fault waits
    // until the own worker's fault is recorded, so the extra faults with
    // no own worker left.
    let (own_gate, own_held) = tokio::sync::watch::channel(false);
    let (extra_gate, extra_held) = tokio::sync::watch::channel(false);
    let b = ScriptedSource::new(data.clone())?
        .fault_times_after(2, 4 * MIB as usize, || anyhow::anyhow!("scripted reset"))
        .holding_fault(0, own_held)
        .holding_fault(1, extra_held)
        .paying(Arc::clone(&lb));
    let (widen, count) = counting_widen(1);
    let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, None);
    cand_b.widen = Some(widen);
    let lanes = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None), cand_b])?;
    let provider = FaultLog {
        inner: &lanes,
        faulted: Mutex::new(Vec::new()),
    };
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let release = async {
        open_when(|| b.opened_ranges().len() >= 2, &own_gate).await;
        open_when(|| faults_of(&provider, 0xB2) >= 1, &extra_gate).await;
    };
    acquire_gated(&store, &provider, root, release).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    assert!(count.granted() >= 1, "B grew an extra stream");
    assert_eq!(count.released(), count.granted());
    assert_eq!(
        faults_of(&provider, 0xB2),
        2,
        "the own worker's fault and the last live worker's fault both cool B"
    );
    Ok(())
}

/// A node is charged once per outage: after its lane's own worker faulted
/// and was charged, its extra stream refused with no verified byte in
/// between does not charge it again.
#[tokio::test(start_paused = true)]
async fn an_outage_charges_the_node_once() -> anyhow::Result<()> {
    let data = blob(32 * MIB as usize);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = dead_after_first_mib(&data, &la)?;
    // B's own leg faults after 4 MiB, once the extra stream for A's
    // remainder has opened. The extra is refused only once the own
    // worker's fault is recorded, with no byte in between.
    let (own_gate, own_held) = tokio::sync::watch::channel(false);
    let (extra_gate, extra_held) = tokio::sync::watch::channel(false);
    let b = ScriptedSource::new(data.clone())?
        .fault_once_after(4 * MIB as usize, || anyhow::anyhow!("scripted reset"))
        .refusing_open(1, || anyhow::anyhow!("scripted overload"))
        .holding_fault(0, own_held)
        .holding_fault(1, extra_held)
        .paying(Arc::clone(&lb));
    let (widen, count) = counting_widen(1);
    let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, None);
    cand_b.widen = Some(widen);
    let lanes = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None), cand_b])?;
    let provider = FaultLog {
        inner: &lanes,
        faulted: Mutex::new(Vec::new()),
    };
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let release = async {
        open_when(|| b.opened_ranges().len() >= 2, &own_gate).await;
        open_when(|| faults_of(&provider, 0xB2) >= 1, &extra_gate).await;
    };
    acquire_gated(&store, &provider, root, release).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    assert!(count.granted() >= 1, "B grew an extra stream");
    assert_eq!(count.released(), count.granted());
    assert_eq!(faults_of(&provider, 0xB2), 1, "one outage, one charge");
    Ok(())
}

// ---- growth retries, and a lane's lease ends with its worker (#2230) ----

/// A lane's widen hooks over `free`, stream permits it shares with
/// sibling fetches the test plays: `grow` takes only permits free now,
/// and `release` puts one back. An extra stream leaves `keep` permits
/// free, as the CLI keeps one back for a sibling's first stream; a
/// restart may take the last one.
fn shared_widen(
    free: &Arc<std::sync::atomic::AtomicUsize>,
    keep: usize,
) -> (super::LaneWiden, Arc<WidenCount>) {
    use std::sync::atomic::Ordering;
    let count = Arc::new(WidenCount::default());
    let (on_grow, on_release) = (Arc::clone(&count), Arc::clone(&count));
    let (take, give) = (Arc::clone(free), Arc::clone(free));
    let widen = super::LaneWiden::new(
        move |kind| {
            let keep = match kind {
                super::GrowFor::Restart => 0,
                super::GrowFor::Extra => keep,
            };
            let taken = take
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                    (n > keep).then(|| n.saturating_sub(1))
                })
                .is_ok();
            on_grow.grew(kind, taken);
            taken
        },
        move || {
            give.fetch_add(1, Ordering::SeqCst);
            on_release.released.fetch_add(1, Ordering::SeqCst);
        },
    );
    (widen, count)
}

/// When a node dies, the busy lane has no free stream, and a sibling
/// fetch frees one later, the loop asks for growth again on its retry
/// clock: the remainder starts on an extra stream long before the busy
/// lane's own range ends, and before the dead node comes back from its
/// cooldown to fault again, if it does.
#[tokio::test(start_paused = true)]
async fn growth_is_asked_again_once_a_stream_frees() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let data = blob(32 * MIB as usize);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = dead_after_first_mib(&data, &la)?;
    let b = busy(&data, &lb)?;
    let free = Arc::new(AtomicUsize::new(0));
    let (widen, count) = shared_widen(&free, 0);
    let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, None);
    cand_b.widen = Some(widen);
    let lanes = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None), cand_b])?;
    let provider = FaultLog {
        inner: &lanes,
        faulted: Mutex::new(Vec::new()),
    };
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    // A sibling fetch gives its permit back a while after A's fault, when
    // the growth request A's fault made has already found none.
    let release = async {
        while faults_of(&provider, 0xA1) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        free.fetch_add(1, Ordering::SeqCst);
    };
    acquire_gated(&store, &provider, root, release).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    let (dead_start, dead_opened, _) = a
        .timeline()
        .first()
        .copied()
        .ok_or_else(|| anyhow::anyhow!("A opened its range"))?;
    let dead_end = dead_start + total / 2;
    let extra_opened = b
        .timeline()
        .into_iter()
        .skip(1)
        .filter(|&(start, _, _)| start > dead_start && start < dead_end)
        .map(|(_, opened, _)| opened)
        .min()
        .ok_or_else(|| anyhow::anyhow!("B took A's remainder"))?;
    // B's own range holds it for 5 s; the retry clock starts the
    // remainder long before that.
    assert!(
        extra_opened <= dead_opened + 2 * super::GROWTH_RETRY,
        "the remainder started on the retry clock, not when B's range ended"
    );
    if let Some(&(_, refault, _)) = a.timeline().get(1) {
        assert!(
            extra_opened < refault,
            "the remainder started before A came back to fault again"
        );
    }
    assert!(count.granted() >= 1, "B grew an extra stream");
    assert_eq!(count.released(), count.granted());
    Ok(())
}

/// While a queued range waits and every `grow` for it is refused, the
/// loop asks again at least once per [`GROWTH_RETRY`], never on a wait
/// that grows. A caller that holds a freed stream for a lane that waits
/// lets the hold lapse once the lane stops asking (#2341), so a longer
/// wait between asks would drop a hold the lane still needs.
#[tokio::test(start_paused = true)]
async fn a_refused_grow_is_asked_again_every_growth_retry() -> anyhow::Result<()> {
    use std::sync::PoisonError;
    use std::sync::atomic::{AtomicBool, Ordering};

    let data = blob(32 * MIB as usize);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = dead_after_first_mib(&data, &la)?;
    let b = busy(&data, &lb)?;
    let free = Arc::new(AtomicBool::new(false));
    let asks = Arc::new(Mutex::new(Vec::new()));
    let (grant, on_ask) = (Arc::clone(&free), Arc::clone(&asks));
    let widen = super::LaneWiden::new(
        move |kind| {
            let granted = match kind {
                super::GrowFor::Restart => true,
                super::GrowFor::Extra => grant.swap(false, Ordering::SeqCst),
            };
            if kind == super::GrowFor::Extra {
                on_ask
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push((tokio::time::Instant::now(), granted));
            }
            granted
        },
        || {},
    );
    let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, None);
    cand_b.widen = Some(widen);
    let lanes = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None), cand_b])?;
    let provider = FaultLog {
        inner: &lanes,
        faulted: Mutex::new(Vec::new()),
    };
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    // A's remainder waits for a stream for 4 s, inside B's 5 s range, so
    // only the retry clock asks for it meanwhile.
    let release = async {
        while faults_of(&provider, 0xA1) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tokio::time::sleep(Duration::from_secs(4)).await;
        free.store(true, Ordering::SeqCst);
    };
    acquire_gated(&store, &provider, root, release).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    let asks = asks.lock().unwrap_or_else(PoisonError::into_inner).clone();
    let refused: Vec<_> = asks
        .iter()
        .take_while(|(_, granted)| !granted)
        .map(|(at, _)| *at)
        .collect();
    assert!(
        refused.len() >= 4,
        "the remainder was asked for each second while it waited: {asks:?}"
    );
    for pair in refused.windows(2) {
        if let [before, after] = pair {
            assert!(
                after.saturating_duration_since(*before)
                    <= super::GROWTH_RETRY + Duration::from_millis(10),
                "a refused grow was asked again only after {:?}",
                after.saturating_duration_since(*before)
            );
        }
    }
    assert!(
        asks.iter().any(|(_, granted)| *granted),
        "the freed stream took the remainder"
    );
    Ok(())
}

/// A pass arms the growth retry when a `grow` granted nothing, or when a
/// range had no lane to ask while a running lane has a [`LaneWiden`]. A
/// fetch with no `LaneWiden`, such as `decdn fetch`, arms none for a range
/// with no lane to ask, and a pass with nothing waiting clears the wait.
#[test]
fn a_pass_retries_while_a_lane_can_still_grow() {
    use super::{GROWTH_RETRY, Growth, GrowthPass};
    let pass = |refused, no_candidate, growable| GrowthPass {
        waiting: vec![(0, MIB)],
        refused,
        no_candidate,
        growable,
    };
    let now = super::Instant::now();
    let cases = [
        (pass(0, 1, false), None),
        (pass(0, 1, true), Some(now + GROWTH_RETRY)),
        (pass(1, 0, false), Some(now + GROWTH_RETRY)),
    ];
    for (p, want) in cases {
        let mut growth = Growth::new(now);
        growth.passed(now, [0; 32], p);
        assert_eq!(growth.retry_at, want);
    }
    let mut growth = Growth::new(now);
    growth.passed(now, [0; 32], pass(0, 1, true));
    let empty = GrowthPass {
        waiting: Vec::new(),
        refused: 0,
        no_candidate: 0,
        growable: true,
    };
    growth.passed(now, [0; 32], empty);
    assert_eq!(growth.retry_at, None);
    assert!(growth.waiting.is_empty());
}

/// A lane's own worker drains every queued range in turn and never ends
/// between them, so no worker end asks for growth. The first pass runs
/// as the lane starts, before its worker holds a range, and finds no busy
/// lane to ask. The loop asks again on its retry clock, and that one pass
/// takes every stream the lane's hook grants for the queue (#2252).
#[tokio::test(start_paused = true)]
async fn a_queue_one_lane_drains_grows_an_extra_stream() -> anyhow::Result<()> {
    let data = blob(16 * MIB as usize);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data)?
        .slow_finish(Duration::from_secs(2))
        .paying(Arc::clone(&la));
    let (widen, count) = counting_widen(2);
    let mut cand_a = candidate(a.clone(), la, 0xA1, None);
    cand_a.widen = Some(widen);
    let provider = StaticSources::new(vec![cand_a])?;
    let (root, total) = (a.root(), a.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let mut set = SourceSet::new(&provider, root, Arc::default(), provider.holders());
    let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
    let gate = recovery_gate();
    let runs: Vec<(u64, u64)> = (0..6).map(|i| (i * 2 * MIB, MIB)).collect();
    acquire(
        AcquireTarget {
            store: &store,
            hash: root,
            total_bytes: total,
            ranges: &runs,
        },
        &mut set,
        &AcquireEnv {
            pacer: &BudgetPacer::new(),
            funder: &no_funding(),
            recovery: &gate,
            credentials: None,
            max_lanes: 1,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        },
    )
    .await?;
    let present = ranges_content_len(&store.present_ranges().await?, total);
    assert_eq!(present, 6 * MIB, "every queued run landed");

    let legs = a.timeline();
    let most_at_once = legs
        .iter()
        .map(|&(_, at, _)| {
            legs.iter()
                .filter(|&&(_, opened, finished)| {
                    opened <= at && finished.is_none_or(|end| at < end)
                })
                .count()
        })
        .max()
        .unwrap_or(0);
    assert_eq!(
        most_at_once, 3,
        "the own stream and both granted extras ran at once: {legs:?}"
    );
    let opened: Vec<_> = legs.iter().map(|&(_, at, _)| at).collect();
    assert_eq!(
        opened.get(1),
        opened.get(2),
        "one growth pass opened both extra streams: {legs:?}"
    );
    assert_eq!(count.peak(), 2, "the lane held both granted streams");
    // The growth pass asks for extra streams. The own worker may later
    // park while its extras finish and take its stream back as a
    // restart.
    let first_grants: Vec<_> = count
        .calls()
        .into_iter()
        .filter(|(_, granted)| *granted)
        .map(|(kind, _)| kind)
        .take(2)
        .collect();
    assert_eq!(
        first_grants,
        [super::GrowFor::Extra, super::GrowFor::Extra],
        "a busy lane asks for extra streams: {:?}",
        count.calls()
    );
    assert_eq!(count.released(), count.granted());
    Ok(())
}

/// A lane with a [`super::LaneWiden`] gives its lease back when its own
/// worker faults, while the fetch still runs, so a sibling fetch can use
/// that stream. When the lane starts again in the same acquire, it does so
/// on a stream its `grow` grants, and gives that back too.
#[tokio::test(start_paused = true)]
async fn a_lanes_lease_is_released_when_its_own_worker_ends() -> anyhow::Result<()> {
    let data = blob(32 * MIB as usize);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    // A faults once, 1 MiB in, and serves every later stream. B is busy
    // long enough for A to come back from its cooldown.
    let a = ScriptedSource::new(data.clone())?
        .fault_once_after(MIB as usize, || anyhow::anyhow!("scripted reset"))
        .paying(Arc::clone(&la));
    let b = busy(&data, &lb)?;
    let dropped_a = Arc::new(Mutex::new(None));
    let (widen, count) = counting_widen(1);
    let mut cand_a = candidate(a.clone(), la, 0xA1, None);
    cand_a.lease = LaneLease::new(DropStamp(Arc::clone(&dropped_a)));
    cand_a.widen = Some(widen);
    let provider = StaticSources::new(vec![cand_a, candidate(b.clone(), lb, 0xB2, None)])?;
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let released_mid_fetch = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let on_progress = {
        let seen = Arc::clone(&released_mid_fetch);
        let dropped_a = Arc::clone(&dropped_a);
        move |_position: u64, _total: u64| {
            if dropped_a.lock().is_ok_and(|at| at.is_some()) {
                seen.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
    };
    run_acquire_with(
        &store,
        &provider,
        root,
        &BudgetPacer::new(),
        &no_funding(),
        Knobs {
            on_progress: Some(&on_progress),
            ..Knobs::lanes(2)
        },
    )
    .await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    assert!(
        released_mid_fetch.load(std::sync::atomic::Ordering::SeqCst),
        "A's lease was released while the fetch still ran"
    );
    assert!(
        a.timeline().len() >= 2,
        "A started again after its cooldown"
    );
    assert!(
        count.granted() >= 1,
        "A started again on a stream its grow granted"
    );
    assert_eq!(
        count.peak(),
        1,
        "A's restart holds one grant, and A runs no extra beside it"
    );
    assert_eq!(count.released(), count.granted());
    Ok(())
}

/// A lane with a [`super::LaneWiden`] gives its lease back while its own
/// worker parks with nothing to take, so a sibling fetch can use that
/// stream while a peer lane drains the rest (#2252). A finishes its half
/// fast; B's half is too small to steal from and stalls, so A parks.
#[tokio::test(start_paused = true)]
async fn a_parked_lane_gives_its_stream_back() -> anyhow::Result<()> {
    let data = blob(24 * MIB as usize);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data.clone())?.paying(Arc::clone(&la));
    let b = busy(&data, &lb)?;
    let dropped_a = Arc::new(Mutex::new(None));
    let (widen, count) = counting_widen(1);
    let mut cand_a = candidate(a.clone(), la, 0xA1, None);
    cand_a.lease = LaneLease::new(DropStamp(Arc::clone(&dropped_a)));
    cand_a.widen = Some(widen);
    let provider = StaticSources::new(vec![cand_a, candidate(b.clone(), lb, 0xB2, None)])?;
    let (root, _) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, a.total_bytes());
    let released_mid_fetch = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let on_progress = {
        let seen = Arc::clone(&released_mid_fetch);
        let dropped_a = Arc::clone(&dropped_a);
        move |_position: u64, _total: u64| {
            if dropped_a.lock().is_ok_and(|at| at.is_some()) {
                seen.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
    };
    run_acquire_with(
        &store,
        &provider,
        root,
        &BudgetPacer::new(),
        &no_funding(),
        Knobs {
            on_progress: Some(&on_progress),
            ..Knobs::lanes(2)
        },
    )
    .await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    assert_eq!(a.timeline().len(), 1, "A fetched its half on one stream");
    assert!(
        released_mid_fetch.load(std::sync::atomic::Ordering::SeqCst),
        "A's lease was released while B still drained its half"
    );
    assert!(
        count
            .calls()
            .iter()
            .all(|(kind, _)| *kind == super::GrowFor::Restart),
        "a parked lane takes a stream back only as a restart: {:?}",
        count.calls()
    );
    assert_eq!(count.released(), count.granted());
    Ok(())
}

/// A node that refuses every extra stream of a busy lane is asked less
/// and less often: the lane waits from `GROWTH_RETRY`, doubling, before
/// it is asked again, whatever else wakes the loop.
#[tokio::test(start_paused = true)]
async fn a_node_refusing_every_extra_stream_is_asked_less_often() -> anyhow::Result<()> {
    let data = blob(32 * MIB as usize);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = dead_after_first_mib(&data, &la)?;
    let b = ScriptedSource::new(data.clone())?
        .stall_after(MIB, Duration::from_secs(20))
        .refusing_opens_from(1, || anyhow::anyhow!("scripted overload"))
        .paying(Arc::clone(&lb));
    let (widen, count) = counting_widen(1);
    let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, None);
    cand_b.widen = Some(widen);
    let lanes = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None), cand_b])?;
    let (root, total) = (a.root(), a.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let fetch = acquire_over(&store, &lanes, lanes.holders(), root, 2);
    let ended = tokio::time::timeout(Duration::from_secs(16), fetch).await;
    assert!(ended.is_err(), "B's own range outlasts the window");

    let extras: Vec<tokio::time::Instant> = b
        .timeline()
        .into_iter()
        .skip(1)
        .map(|(_, opened, _)| opened)
        .collect();
    let gaps: Vec<Duration> = extras
        .windows(2)
        .filter_map(|pair| Some(pair.get(1)?.duration_since(*pair.first()?)))
        .collect();
    assert!(
        (3..=5).contains(&extras.len()),
        "B's extra stream was asked at 0, 1, 3, 7 and 15 s: {gaps:?}"
    );
    for pair in gaps.windows(2) {
        if let [shorter, longer] = pair {
            assert!(
                *longer >= *shorter * 2,
                "each wait doubles the last: {gaps:?}"
            );
        }
    }
    assert_eq!(count.released(), count.granted());
    Ok(())
}

/// A busy partial holder whose node cannot serve a faulted lane's
/// remainder by pull-through refuses it on its extra stream with
/// `NotFound`. Those refusals bar it from pull-through as on its own
/// stream, so it is asked for the remainder at most
/// [`ABSENT_AFTER_NOT_FOUND`] times while the bar lasts.
///
/// [`ABSENT_AFTER_NOT_FOUND`]: crate::source_set::ABSENT_AFTER_NOT_FOUND
#[tokio::test(start_paused = true)]
async fn extra_stream_refusals_of_an_uncovered_range_bar_the_lane() -> anyhow::Result<()> {
    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let data = blob(total as usize);
    let n = num_blocks(total);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = dead_after_first_mib(&data, &la)?;
    let b = ScriptedSource::new(data.clone())?
        .stall_after(MIB, Duration::from_mins(1))
        .refusing_blocks(&[0], || {
            anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound))
        })
        .paying(Arc::clone(&lb));
    let (widen, count) = counting_widen(1);
    let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, Some(cov(n, &[1])));
    cand_b.widen = Some(widen);
    let lanes = StaticSources::new(vec![candidate(a.clone(), la, 0xA1, None), cand_b])?;
    let root = a.root();
    let (store, _dir) = fresh_store(root, total);
    let fetch = acquire_over(&store, &lanes, lanes.holders(), root, 2);
    let ended = tokio::time::timeout(Duration::from_secs(50), fetch).await;
    assert!(ended.is_err(), "B's own range outlasts the window");

    let block0 = b
        .opened_ranges()
        .into_iter()
        .filter(|&(start, _)| start < DISCOVERY_BLOCK_BYTES)
        .count();
    assert!(block0 >= 1, "B grew an extra stream for A's remainder");
    assert!(
        block0 <= crate::source_set::ABSENT_AFTER_NOT_FOUND as usize,
        "B was asked for the uncovered remainder {block0} times"
    );
    assert_eq!(count.released(), count.granted());
    Ok(())
}

/// A lane that faults and finds no free stream to start again on tries
/// again each `GROWTH_RETRY`, not on a growing build backoff, and starts
/// once a sibling fetch frees a stream. The freed stream is the last free
/// one, which an extra stream may not take: the start asks for it as a
/// restart. Every grant comes back.
#[tokio::test(start_paused = true)]
async fn a_lane_refused_a_stream_starts_again_once_one_frees() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let data = blob(16 * MIB as usize);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = ScriptedSource::new(data.clone())?
        .fault_once_after(MIB as usize, || anyhow::anyhow!("scripted reset"))
        .paying(Arc::clone(&la));
    let free = Arc::new(AtomicUsize::new(0));
    let (widen, count) = shared_widen(&free, 1);
    let mut cand_a = candidate(a.clone(), la, 0xA1, None);
    cand_a.widen = Some(widen);
    let lanes = StaticSources::new(vec![cand_a])?;
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let freed_after = Duration::from_secs(20);
    let freed = async {
        tokio::time::sleep(freed_after).await;
        free.fetch_add(1, Ordering::SeqCst);
    };
    let started = tokio::time::Instant::now();
    let fetch = acquire_over(&store, &lanes, lanes.holders(), root, 1);
    let (fetched, ()) = tokio::time::timeout(
        Duration::from_mins(5),
        futures_util::future::join(fetch, freed),
    )
    .await
    .map_err(|_| anyhow::anyhow!("the fetch stalled"))?;
    fetched?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    let (_, restarted, _) = a
        .timeline()
        .get(1)
        .copied()
        .ok_or_else(|| anyhow::anyhow!("A started again"))?;
    let late = restarted
        .duration_since(started)
        .saturating_sub(freed_after);
    assert!(
        late <= super::GROWTH_RETRY,
        "A started again within a retry of the freed stream, {late:?} after"
    );
    let calls = count.calls();
    let restarts = calls
        .iter()
        .filter(|(kind, _)| *kind == super::GrowFor::Restart)
        .count();
    assert!(
        restarts <= 25,
        "A asked for a stream to start again on about once a second: {restarts}"
    );
    let granted: Vec<_> = calls
        .iter()
        .filter(|(_, granted)| *granted)
        .map(|(kind, _)| *kind)
        .collect();
    assert_eq!(granted, [super::GrowFor::Restart], "{calls:?}");
    assert_eq!(count.released(), count.granted());
    Ok(())
}

/// The most extra streams busy lane B holds at once while two dead lanes'
/// remainders wait, with a hook that grants up to `grants` streams at once,
/// as `(peak, granted, released)`.
async fn peak_extras_for_two_remainders(grants: usize) -> anyhow::Result<(usize, usize, usize)> {
    let data = blob(48 * MIB as usize);
    let la = Arc::new(PoolLedger::new(Cumulative::default()));
    let lc = Arc::new(PoolLedger::new(Cumulative::default()));
    let lb = Arc::new(PoolLedger::new(Cumulative::default()));
    let a = dead_after_first_mib(&data, &la)?;
    let c = dead_after_first_mib(&data, &lc)?;
    let b = busy(&data, &lb)?;
    let (widen, count) = counting_widen(grants);
    let mut cand_b = candidate(b.clone(), Arc::clone(&lb), 0xB2, None);
    cand_b.widen = Some(widen);
    let lanes = StaticSources::new(vec![
        candidate(a.clone(), la, 0xA1, None),
        candidate(c.clone(), lc, 0xC3, None),
        cand_b,
    ])?;
    let (root, total) = (a.root(), a.total_bytes());
    let (store, dir) = fresh_store(root, total);
    acquire_over(&store, &lanes, lanes.holders(), root, 3).await?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    Ok((count.peak(), count.granted(), count.released()))
}

/// A busy lane runs as many extra workers as its hook grants, one per
/// waiting range: with two dead lanes' remainders queued it holds more
/// than one extra stream at once, never more than the hook grants, and
/// with a hook that grants one it never holds more (#2252).
#[tokio::test(start_paused = true)]
async fn a_busy_lane_runs_as_many_extra_streams_as_granted() -> anyhow::Result<()> {
    let (peak, granted, released) = peak_extras_for_two_remainders(3).await?;
    assert!(
        (2..=3).contains(&peak),
        "B held several extra streams at once, within its grant: {peak}"
    );
    assert_eq!(released, granted);

    let (peak, granted, released) = peak_extras_for_two_remainders(1).await?;
    assert!(granted >= 1, "B grew an extra stream");
    assert_eq!(
        peak, 1,
        "B held no more extra streams than its hook granted"
    );
    assert_eq!(released, granted);
    Ok(())
}

/// A store that fails `missing_ranges` once `fail` is set: a local disk or
/// store fault in the middle of a unit. A nonzero `shrink_to` moves the
/// bound down to it at the next `missing_ranges`, before the query runs: a
/// leg that proves a smaller size after the caller read the bound.
struct FailingMissing {
    inner: ClientRangedStore,
    fail: std::sync::atomic::AtomicBool,
    shrink_to: std::sync::atomic::AtomicU64,
}

impl RangedStore for FailingMissing {
    fn total_bytes(&self) -> u64 {
        self.inner.total_bytes()
    }

    fn present_ranges(&self) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
        self.inner.present_ranges()
    }

    fn missing_ranges(
        &self,
        byte_offset: u64,
        byte_len: u64,
    ) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Box::pin(async {
                Err(decdn_bao_range::RangedStoreError::Backend(Box::from(
                    "injected store read fault",
                )))
            });
        }
        let shrink = self.shrink_to.swap(0, std::sync::atomic::Ordering::SeqCst);
        if shrink > 0 {
            self.inner.set_bound(shrink);
        }
        self.inner.missing_ranges(byte_offset, byte_len)
    }

    fn admit(
        &self,
        range: decdn_bao_range::AlignedRange,
        bao_bytes: bytes::Bytes,
    ) -> decdn_bao_range::RangedFuture<'_, ()> {
        self.inner.admit(range, bao_bytes)
    }

    fn read(&self, offset: u64, len: u64) -> decdn_bao_range::RangedFuture<'_, bytes::Bytes> {
        self.inner.read(offset, len)
    }

    fn is_complete(&self) -> decdn_bao_range::RangedFuture<'_, bool> {
        self.inner.is_complete()
    }

    fn finalize(&self) -> decdn_bao_range::RangedFuture<'_, ()> {
        self.inner.finalize()
    }
}

impl crate::source::IngestStore for FailingMissing {
    fn ingest_stream<'a, R>(
        &'a self,
        range: &'a decdn_bao_range::AlignedRange,
        reader: R,
        on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
        claimed_total: u64,
        stop_at: Option<&'a std::sync::atomic::AtomicU64>,
    ) -> crate::source::IngestFuture<'a, R>
    where
        R: crate::BaoRangeReader + 'a,
    {
        crate::source::IngestStore::ingest_stream(
            &self.inner,
            range,
            reader,
            on_progress,
            claimed_total,
            stop_at,
        )
    }

    fn flush_present_record(&self) -> crate::source::SourceFuture<'_, ()> {
        crate::source::IngestStore::flush_present_record(&self.inner)
    }

    fn proven(&self) -> Option<u64> {
        self.inner.proven()
    }

    fn set_bound(&self, bound: u64) {
        self.inner.set_bound(bound);
    }
}

/// A store that cannot read its own record while the watchdog checks a
/// stalled unit is this process's fault (#2213): the acquire ends with
/// that error as a command-wide fatal, and the source is not cooled.
#[tokio::test(start_paused = true)]
async fn a_store_read_error_in_the_watchdog_is_our_fault() -> anyhow::Result<()> {
    let data = blob(8 * MIB as usize);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let src = ScriptedSource::new(data)?
        .stall_after(MIB, Duration::from_mins(1))
        .paying(Arc::clone(&ledger));
    let (root, total) = (src.root(), src.total_bytes());
    let (inner, _dir) = fresh_store(root, total);
    let store = FailingMissing {
        inner,
        fail: std::sync::atomic::AtomicBool::new(false),
        shrink_to: std::sync::atomic::AtomicU64::new(0),
    };
    let provider = StaticSources::new(vec![candidate(src, ledger, 0xA1, None)])?;
    let health: Arc<PeerHealth> = Arc::default();
    let mut set = SourceSet::new(&provider, root, Arc::clone(&health), provider.holders());
    let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
    let gate = recovery_gate();
    let (pacer, funder) = (BudgetPacer::new(), no_funding());
    let whole = [(0, total)];
    let env = AcquireEnv {
        pacer: &pacer,
        funder: &funder,
        recovery: &gate,
        credentials: None,
        max_lanes: 1,
        stop: &stop,
        on_progress: None,
        ledgers: None,
        pacing: None,
        max_blob_bytes: 0,
    };
    let fetch = acquire(
        AcquireTarget {
            store: &store,
            hash: root,
            total_bytes: total,
            ranges: &whole,
        },
        &mut set,
        &env,
    );
    // The unit stalls after its first MiB; the store breaks before the
    // lane watchdog's next check.
    let breaks = async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        store.fail.store(true, std::sync::atomic::Ordering::SeqCst);
        std::future::pending::<()>().await;
    };
    let err = tokio::select! {
        res = fetch => res.err().ok_or_else(|| anyhow::anyhow!("a broken store must end it"))?,
        () = breaks => unreachable!("never resolves"),
    };
    assert!(
        format!("{err:#}").contains("injected store read fault"),
        "{err:#}"
    );
    assert_eq!(classify(&err), Fault::Fatal(FatalScope::Command), "{err:#}");
    assert_eq!(
        health.health(Address::repeat_byte(0xA1)),
        Health::Healthy { streak: 0 },
        "the source is not cooled for our fault"
    );
    Ok(())
}

/// With no size proven, the end the fetch knows is the smaller of the
/// first claim and the bound; a proven size replaces both.
#[tokio::test]
async fn the_known_end_is_the_claim_or_bound_until_a_size_is_proven() -> anyhow::Result<()> {
    let data = blob(3 * MIB as usize);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let src = ScriptedSource::new(data)?.paying(Arc::clone(&ledger));
    let root = src.root();
    let (store, _dir) = fresh_store(root, 8 * MIB);
    assert_eq!(super::known_end(&store, MIB), MIB, "a short claim");
    assert_eq!(super::known_end(&store, 16 * MIB), 8 * MIB, "the bound");

    let provider = StaticSources::new(vec![candidate(src, ledger, 0xA1, None)])?;
    run_acquire(
        &store,
        &provider,
        root,
        8 * MIB,
        &BudgetPacer::new(),
        &no_funding(),
        1,
        None,
    )
    .await?;
    assert_eq!(super::known_end(&store, MIB), 3 * MIB, "the proven size");
    Ok(())
}

/// A leg proves a smaller size between a caller's bound read and its
/// missing-ranges query. The query clips to the smaller bound: a range past
/// it misses nothing, and a range across it misses only the bytes below it.
/// Neither is a store fault.
#[tokio::test]
async fn a_bound_that_shrinks_under_a_missing_query_clips_it() -> anyhow::Result<()> {
    let (inner, _dir) = fresh_store([7; 32], 8 * MIB);
    let store = FailingMissing {
        inner,
        fail: std::sync::atomic::AtomicBool::new(false),
        shrink_to: std::sync::atomic::AtomicU64::new(2 * MIB),
    };
    let past = super::store_missing(&store, 4 * MIB, 2 * MIB).await?;
    assert!(past.is_empty(), "a range past the new bound misses nothing");

    store
        .shrink_to
        .store(MIB, std::sync::atomic::Ordering::SeqCst);
    let across = super::store_missing(&store, 0, 4 * MIB).await?;
    assert_eq!(ranges_content_len(&across, store.total_bytes()), MIB);
    Ok(())
}

/// A provider that builds its one holder's lane from the credential slot's
/// current key, with one ledger per signer, as a run registry keys its lanes
/// by `(pool, signer, provider)`.
struct SlotSources {
    slot: crate::CredentialSlot,
    source: ScriptedSource,
    ledgers: Mutex<std::collections::HashMap<Address, Arc<PoolLedger>>>,
}

const SLOT_PROVIDER: u8 = 0xA1;

impl SlotSources {
    fn new(slot: &crate::CredentialSlot, source: ScriptedSource) -> Self {
        Self {
            slot: slot.clone(),
            source,
            ledgers: Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn holder() -> crate::Holder {
        crate::Holder {
            provider: Address::repeat_byte(SLOT_PROVIDER),
            coverage: None,
            rtt_ms: 0.0,
            probed_holder: true,
        }
    }

    /// Every ledger built, one per signer.
    fn ledgers(&self) -> Vec<(Address, Arc<PoolLedger>)> {
        self.ledgers
            .lock()
            .unwrap()
            .iter()
            .map(|(signer, ledger)| (*signer, Arc::clone(ledger)))
            .collect()
    }
}

impl crate::SourceProvider for SlotSources {
    type Source = ScriptedSource;

    fn discover(&self, _hash: [u8; 32]) -> crate::SourceFuture<'_, Vec<crate::Holder>> {
        Box::pin(async { Ok(vec![Self::holder()]) })
    }

    fn connect<'a>(
        &'a self,
        _holder: &'a crate::Holder,
    ) -> crate::SourceFuture<'a, StreamCandidate<ScriptedSource>> {
        let credential = self.slot.current();
        let signer = credential.signer.address();
        let ledger = Arc::clone(
            self.ledgers
                .lock()
                .unwrap()
                .entry(signer)
                .or_insert_with(|| Arc::new(PoolLedger::new(Cumulative::default()))),
        );
        let mut ctx = ctx_with(SLOT_PROVIDER, U256::from(u128::MAX));
        ctx.client_signer = credential.signer;
        ctx.capability = Some(credential.capability);
        let lane = candidate_ctx(
            self.source.clone().paying(Arc::clone(&ledger)),
            ledger,
            Arc::new(Mutex::new(ctx)),
            None,
        );
        Box::pin(async move { Ok(lane) })
    }
}

/// A capability for a fresh key, capped at `spending_cap`, a day from expiry.
fn delegate(spending_cap: u64) -> crate::Credential {
    let signer = alloy::signers::local::PrivateKeySigner::random();
    let capability = decdn_incentive::Capability {
        signer: signer.address(),
        spending_cap,
        pool_id: B256::repeat_byte(0x11),
        expiry: crate::credential::unix_now() + 24 * 60 * 60,
    }
    .sign(
        &signer,
        &decdn_incentive::bind_node_id_domain(1, Address::ZERO),
    )
    .unwrap();
    crate::Credential {
        signer: Arc::new(signer),
        capability,
    }
}

fn slot_for(credential: &crate::Credential, wait: Duration) -> crate::CredentialSlot {
    crate::CredentialSlot::new(
        Arc::clone(&credential.signer),
        credential.capability.clone(),
    )
    .unwrap()
    .with_swap_wait(wait)
}

/// Acquire the whole `root` blob from `provider` as a delegate under `slot`.
async fn acquire_delegated(
    store: &ClientRangedStore,
    provider: &SlotSources,
    root: [u8; 32],
) -> anyhow::Result<()> {
    let total = store.total_bytes();
    let mut set = SourceSet::new(provider, root, Arc::default(), vec![SlotSources::holder()]);
    let stop = StopPolicy::new(false, Some(Duration::from_hours(1)), Arc::default());
    let gate = RecoveryGate::with_settle(Duration::ZERO);
    acquire(
        AcquireTarget {
            store,
            hash: root,
            total_bytes: total,
            ranges: &[(0, total)],
        },
        &mut set,
        &AcquireEnv {
            pacer: &BudgetPacer::new(),
            funder: &no_funding(),
            recovery: &gate,
            credentials: Some(&provider.slot),
            max_lanes: 1,
            stop: &stop,
            on_progress: None,
            ledgers: None,
            pacing: None,
            max_blob_bytes: 0,
        },
    )
    .await
}

fn cap_rejection() -> anyhow::Error {
    anyhow::Error::new(UpstreamVoucherRejected {
        reason: decdn_protocol::client::VoucherRejectReason::SpendingCapExhausted,
        bundle: None,
        proof_generation: None,
    })
}

/// A swap mid-stream ends the lane on the old key at a voucher boundary, paid
/// for what it received. A new lane under the new key resumes at the
/// delivered frontier, so every delivered byte is paid once and no byte is
/// fetched twice.
#[tokio::test(start_paused = true)]
async fn a_swap_mid_stream_retires_the_old_lane_paid_and_resumes_under_the_new_key()
-> anyhow::Result<()> {
    let data = blob(4 * 1024 * 1024);
    let source = ScriptedSource::new(data.clone())?.throttled(Duration::from_millis(5));
    let (root, total) = (source.root(), source.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let (old, new) = (delegate(u64::MAX), delegate(u64::MAX));
    let slot = slot_for(&old, Duration::ZERO);
    let provider = SlotSources::new(&slot, source.clone());

    let swap = async {
        while source.delivered_bytes() < 1024 * 1024 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        slot.swap(Arc::clone(&new.signer), new.capability.clone())
            .unwrap();
    };
    let (fetched, ()) = tokio::join!(acquire_delegated(&store, &provider, root), swap);
    fetched?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);

    let opened = source.opened_ranges();
    assert_eq!(
        opened.first().map(|&(start, _)| start),
        Some(0),
        "{opened:?}"
    );
    let resumed = opened.get(1).map_or(0, |&(start, _)| start);
    assert!(
        resumed >= 1024 * 1024 / 2 && resumed < total,
        "the new lane resumes at the delivered frontier: {opened:?}"
    );
    let ledgers = provider.ledgers();
    assert_eq!(ledgers.len(), 2, "one lane per key");
    assert_eq!(
        source.delivered_bytes(),
        total,
        "no content byte is delivered twice"
    );
    // One whole-blob fetch on one lane is the price of the blob's wire.
    let baseline = {
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let once = ScriptedSource::new(data)?.paying(Arc::clone(&ledger));
        let (single, _dir) = fresh_store(root, total);
        let sources = StaticSources::new(vec![candidate(once, Arc::clone(&ledger), 0xB2, None)])?;
        acquire_under(&single, &sources, root, &no_funding(), &RecoveryGate::new()).await?;
        ledger.committed().bytes
    };
    let paid: U256 = ledgers
        .iter()
        .map(|(_, ledger)| ledger.committed().bytes)
        .fold(U256::ZERO, U256::saturating_add);
    assert!(paid >= baseline, "UNDER-PAY: paid {paid} of {baseline}");
    assert!(
        paid <= baseline + U256::from(64 * 1024),
        "DOUBLE-PAY: paid {paid} for a blob whose wire costs {baseline}"
    );
    for (signer, ledger) in &ledgers {
        assert!(
            ledger.committed().bytes > U256::ZERO,
            "the lane on {signer} paid for what it received"
        );
    }
    Ok(())
}

/// At the exhausted candidate set a delegate waits for a swap. A swap inside
/// the wait gives one more pass under the new key, and the fetch completes.
#[tokio::test(start_paused = true)]
async fn a_swap_inside_the_wait_resumes_the_fetch() -> anyhow::Result<()> {
    let data = blob(1024 * 1024);
    let source = ScriptedSource::new(data.clone())?.fault_once_after(0, cap_rejection);
    let (root, total) = (source.root(), source.total_bytes());
    let (store, dir) = fresh_store(root, total);
    let (old, new) = (delegate(u64::MAX), delegate(u64::MAX));
    let slot = slot_for(&old, Duration::from_mins(1));
    let provider = SlotSources::new(&slot, source.clone());
    let started = tokio::time::Instant::now();

    let swap = async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        slot.swap(Arc::clone(&new.signer), new.capability.clone())
            .unwrap();
    };
    let (fetched, ()) = tokio::join!(acquire_delegated(&store, &provider, root), swap);
    fetched?;
    store.finalize().await?;
    assert_eq!(std::fs::read(dir.path().join("b"))?, data);
    assert!(
        started.elapsed() < Duration::from_mins(1),
        "the swap ends the wait"
    );
    assert_eq!(slot.generation(), 1);
    Ok(())
}

/// With no swap inside the wait, the delegate ends with a typed
/// `NewCapability` once the wait runs out, naming the pool and the cause.
#[tokio::test(start_paused = true)]
async fn no_swap_inside_the_wait_ends_needing_a_new_capability() -> anyhow::Result<()> {
    let source = ScriptedSource::new(blob(1024 * 1024))?.with_fault_after(0, cap_rejection);
    let (root, total) = (source.root(), source.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    // A cap of 1 against a 1 MiB blob at 1 per MB plus one window of 1.
    let credential = delegate(1);
    let slot = slot_for(&credential, Duration::from_secs(10));
    let mut events = slot.funding_events();
    let provider = SlotSources::new(&slot, source);
    let started = tokio::time::Instant::now();

    let err = acquire_delegated(&store, &provider, root)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("must stop"))?;
    assert!(
        started.elapsed() >= Duration::from_secs(10),
        "the wait ran out"
    );
    assert_eq!(
        err.downcast_ref::<crate::FundingNeeded>(),
        Some(&crate::FundingNeeded::NewCapability {
            pool: B256::repeat_byte(0x11),
            cause: crate::CapabilityCause::CapSpent,
        }),
        "{err:#}"
    );
    assert_eq!(classify(&err), Fault::Fatal(FatalScope::Command));
    assert_matches!(
        *events.borrow_and_update(),
        Some(crate::FundingEvent::RunningLow { .. }),
        "the fetch signalled the capability running low"
    );
    Ok(())
}

/// Nodes that refuse the pool's funding while the capability still covers
/// the work end the fetch at once with `PublisherPool`: only the pool owner
/// can help, so the fetch waits for no swap.
#[tokio::test(start_paused = true)]
async fn a_pool_refusal_under_a_healthy_capability_names_the_publisher_pool() -> anyhow::Result<()>
{
    let source = ScriptedSource::new(blob(1024 * 1024))?.with_fault_after(0, || {
        anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::Unfunded))
    });
    let (root, total) = (source.root(), source.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let credential = delegate(u64::MAX);
    let slot = slot_for(&credential, Duration::from_mins(1));
    let provider = SlotSources::new(&slot, source);
    let started = tokio::time::Instant::now();

    let err = acquire_delegated(&store, &provider, root)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("must stop"))?;
    assert!(started.elapsed() < Duration::from_mins(1), "no swap wait");
    assert_eq!(
        err.downcast_ref::<crate::FundingNeeded>(),
        Some(&crate::FundingNeeded::PublisherPool {
            pool: B256::repeat_byte(0x11),
        }),
        "{err:#}"
    );
    assert_eq!(
        *slot.funding_events().borrow(),
        None,
        "the capability is fine"
    );
    Ok(())
}

/// The capability's spend counts every lane the local records hold for its
/// key, not only this fetch's: a key whose cap an earlier fetch at another
/// provider mostly spent ends needing a new capability, not blaming the pool.
#[tokio::test(start_paused = true)]
async fn spend_recorded_at_another_provider_counts_toward_the_cap() -> anyhow::Result<()> {
    let source = ScriptedSource::new(blob(1024 * 1024))?.with_fault_after(0, || {
        anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::Unfunded))
    });
    let (root, total) = (source.root(), source.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let credential = delegate(1_000);
    let slot = slot_for(&credential, Duration::ZERO);
    slot.record_spend(credential.signer.address(), U256::from(999u64));
    let provider = SlotSources::new(&slot, source);
    let err = acquire_delegated(&store, &provider, root)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("must stop"))?;
    assert_eq!(
        err.downcast_ref::<crate::FundingNeeded>(),
        Some(&crate::FundingNeeded::NewCapability {
            pool: B256::repeat_byte(0x11),
            cause: crate::CapabilityCause::CapSpent,
        }),
        "{err:#}"
    );
    Ok(())
}

/// A capability that nodes refuse while its local cap and expiry still cover
/// the work reads as revoked.
#[tokio::test(start_paused = true)]
async fn a_capability_refused_with_headroom_left_reads_as_revoked() -> anyhow::Result<()> {
    let source = ScriptedSource::new(blob(1024 * 1024))?.with_fault_after(0, cap_rejection);
    let (root, total) = (source.root(), source.total_bytes());
    let (store, _dir) = fresh_store(root, total);
    let credential = delegate(u64::MAX);
    let slot = slot_for(&credential, Duration::ZERO);
    let provider = SlotSources::new(&slot, source);
    let err = acquire_delegated(&store, &provider, root)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("must stop"))?;
    assert_eq!(
        err.downcast_ref::<crate::FundingNeeded>(),
        Some(&crate::FundingNeeded::NewCapability {
            pool: B256::repeat_byte(0x11),
            cause: crate::CapabilityCause::Revoked,
        }),
        "{err:#}"
    );
    Ok(())
}

/// An extra stream's fault line is at debug when nothing landed. Otherwise
/// it follows the fault's class (#2331). The error field is sanitized: a
/// lane build's chain error names the RPC URL, which may carry a key.
#[test]
fn extra_stream_fault_level_follows_landed_and_class() {
    #[derive(Clone, Default)]
    struct CapturedLog(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for CapturedLog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let range = |landed| crate::source_set::LaneRange {
        offset: 0,
        len: 1024,
        landed,
        past_end: false,
        uncovered: true,
    };
    let log = CapturedLog::default();
    let sink = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_writer(move || sink.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let refused = anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound));
        super::log_extra_fault(Address::ZERO, [0; 32], range(0), &refused);
        let stall = anyhow::anyhow!("no verified progress for 10s");
        super::log_extra_fault(Address::ZERO, [0; 32], range(0), &stall);
        let reset = anyhow::anyhow!("connection reset");
        super::log_extra_fault(Address::ZERO, [0; 32], range(1), &reset);
        let retry = anyhow::Error::new(crate::fault::LaneBuildFault(anyhow::anyhow!(
            "error sending request for url (https://rpc.example/v3/secret)"
        )));
        super::log_extra_fault(Address::ZERO, [0; 32], range(1), &retry);
    });
    let text = String::from_utf8_lossy(
        &log.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    )
    .into_owned();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 4, "{text}");
    let level = |i: usize| lines.get(i).copied().unwrap_or_default();
    assert!(level(0).contains(" DEBUG "), "a refusal is routine: {text}");
    assert!(level(1).contains(" DEBUG "), "nothing landed: {text}");
    assert!(level(2).contains(" WARN "), "{text}");
    assert!(level(3).contains(" INFO "), "a chain-side retry: {text}");
    assert!(level(3).contains("fault=Transient"), "{text}");
    assert!(!text.contains("secret"), "the RPC URL is stripped: {text}");
}
