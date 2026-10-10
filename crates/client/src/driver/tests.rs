use std::sync::{Arc, Mutex};

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::Bytes;
use decdn_bao_range::{AlignedRange, CHUNK_GROUP_BYTES, IROH_BLOCK_SIZE, RangedStore, align_range};

use super::{
    LegNoProgress, PoolExhausted, SharedPool, contiguous_byte_ranges, drive, first_leg,
    ranges_content_len,
};
use crate::ProgressCallback;
use crate::pacer::{BudgetPacer, PaceDecision, PaceState};
use crate::source::PrimedSource;
use crate::source::{BlobSource, FlushCountingStore, ScriptedSource, SourceFuture};
use crate::{
    ClientRangedStore, Cumulative, PoolContext, PoolLedger, UpstreamPullHeader, UpstreamRefused,
    UpstreamVoucherRejected, VoucherProgress,
};
use decdn_protocol::client::{
    StreamError, StreamResponse, StreamResponseBody, StreamResponseExt, VoucherRejectReason,
};

/// A store that fails a query ends the command as this machine's fault: no
/// source caused it. A range the store refuses as out of bounds is about the
/// request, so it is not marked.
#[test]
fn a_failed_store_query_is_a_local_fault_and_a_bad_range_is_not() {
    let failed = super::store_query_fault(decdn_bao_range::RangedStoreError::Backend(
        "store read failed".into(),
    ));
    assert!(failed.is::<crate::LocalPullFault>(), "{failed:#}");
    assert_eq!(
        crate::classify(&failed),
        crate::Fault::Fatal(crate::FatalScope::Command)
    );

    let bad_range = super::store_query_fault(decdn_bao_range::RangedStoreError::Alignment(
        decdn_bao_range::RangeVerifyError::RangeOutOfBounds {
            offset: 10,
            len: 1,
            blob_size: 4,
        },
    ));
    assert!(!bad_range.is::<crate::LocalPullFault>(), "{bad_range:#}");
}

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

/// A healthy buyer context: a huge deposit so the pacer never refuses (the
/// money assertions exercise the gap logic, not funding).
fn healthy_ctx() -> PoolContext {
    let signer = PrivateKeySigner::random();
    PoolContext {
        pool_id: B256::ZERO,
        // Pinned to a non-zero test provider: `send_voucher` fast-fails on
        // `Address::ZERO` (an unpinned lane), so every driver test that
        // actually signs a voucher needs a real-looking address here.
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

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext.clone())
        .expect("source")
        .paying(Arc::clone(&ledger));
    assert_eq!(source.root(), root);
    let pacer = BudgetPacer::new();
    let ctx = Arc::new(Mutex::new(healthy_ctx()));

    drive(
        &store, &source, &pacer, &ctx, &ledger, root, 0, 0, None, None, None, None,
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

    // The blob is complete, finalized, and byte-exact.
    assert!(store.is_complete().await.expect("is_complete"));
    let got = store.read(0, 0).await.expect("read whole blob");
    assert_eq!(
        got.as_ref(),
        plaintext.as_slice(),
        "assembled bytes byte-exact"
    );
}

/// A lane on a shared pool gates on the pool's spend across every lane, not
/// on its own: with the deposit spent elsewhere in the pool, the lane refuses
/// its next leg even though its own ledger has spent almost nothing.
#[tokio::test(start_paused = true)]
async fn a_lane_gates_on_the_pools_shared_spend() {
    let total = 3 * GROUP;
    let (root, plaintext, _) = synth_blob(total as usize);
    let store = fresh_store(root, total);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext)
        .expect("source")
        .paying(Arc::clone(&ledger));
    let ctx = Arc::new(Mutex::new(healthy_ctx()));
    // Every lane of the pool together spent the whole deposit.
    let spent = || U256::from(u128::MAX);
    let pool = SharedPool {
        spent: &spent,
        quotes: None,
    };
    let pacer = crate::pacer::WindowPacer::new(GROUP);
    let downstream = || super::DownstreamFrontier {
        served_paid: u64::MAX,
        serve_demand: 0,
    };
    let err = drive(
        &store,
        &source,
        &pacer,
        &ctx,
        &ledger,
        root,
        0,
        0,
        None,
        None,
        Some(&downstream),
        Some(&pool),
    )
    .await
    .expect_err("the shared spend refuses the next leg");
    assert!(err.downcast_ref::<PoolExhausted>().is_some(), "{err:#}");
    // The first open is free (no quote yet); the next pass refuses.
    assert_eq!(source.opened_ranges().len(), 1);
}

/// A leg whose signed size differs from the store's bound verifies under its
/// own claim: the planner's bound is too small, the leg's range lies inside
/// both sizes, and its bytes land at their offsets.
#[tokio::test]
async fn a_leg_whose_claim_differs_from_the_bound_is_ingested_under_its_claim() {
    let truth = 8 * GROUP;
    let bound = 6 * GROUP;
    let (root, plaintext, _) = synth_blob(truth as usize);
    let store = fresh_store(root, bound);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext.clone())
        .expect("source")
        .paying(Arc::clone(&ledger));
    let ctx = Arc::new(Mutex::new(healthy_ctx()));
    drive(
        &store,
        &source,
        &BudgetPacer::new(),
        &ctx,
        &ledger,
        root,
        4 * GROUP,
        2 * GROUP,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("the leg verifies under its own claim");

    let leg = align_range(4 * GROUP, 2 * GROUP, truth).expect("align");
    assert_eq!(
        &store.present_ranges().await.expect("present"),
        leg.chunk_ranges()
    );
    assert_eq!(
        store
            .read(4 * GROUP, 2 * GROUP)
            .await
            .expect("read")
            .as_ref(),
        &plaintext[(4 * GROUP) as usize..(6 * GROUP) as usize]
    );
    assert_eq!(store.proven(), None, "a non-final leg proves no size");
    assert_eq!(store.bound(), bound);
}

/// `first_leg` is exactly the range the drive opens first, both on a fresh
/// blob and on a resume, so a primed pull of it is adopted (#2063).
#[tokio::test]
async fn first_leg_is_the_range_the_drive_opens_first() {
    let total = 4 * GROUP;
    let (root, plaintext, outboard) = synth_blob(total as usize);

    let fresh = fresh_store(root, total);
    assert_eq!(
        first_leg(&fresh, &[(0, 0)], u64::MAX, 0)
            .await
            .expect("first leg"),
        Some(align_range(0, 0, total).expect("align")),
        "a fresh whole-blob drive opens the whole blob"
    );

    let resumed = fresh_store(root, total);
    preadmit(
        &resumed,
        &plaintext,
        &outboard,
        &align_range(0, GROUP, total).expect("align"),
    )
    .await;
    let want = first_leg(&resumed, &[(0, 0)], u64::MAX, 0)
        .await
        .expect("first leg")
        .expect("a gap remains");
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext.clone())
        .expect("source")
        .paying(Arc::clone(&ledger));
    drive(
        &resumed,
        &source,
        &BudgetPacer::new(),
        &Arc::new(Mutex::new(healthy_ctx())),
        &ledger,
        root,
        0,
        0,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("drive");
    assert_eq!(
        source.opened_ranges().first().copied(),
        Some((want.fetch_start(), want.fetch_len()))
    );

    // The received-byte ceiling caps the first leg exactly as it caps a
    // drawn leg: one chunk group past the ceiling.
    assert_eq!(
        first_leg(&fresh, &[(0, 0)], u64::MAX, GROUP)
            .await
            .expect("first leg"),
        Some(align_range(0, 2 * GROUP, total).expect("align"))
    );
    let full = fresh_store(root, total);
    preadmit(
        &full,
        &plaintext,
        &outboard,
        &align_range(0, 0, total).expect("align"),
    )
    .await;
    assert_eq!(
        first_leg(&full, &[(0, 0)], u64::MAX, 0)
            .await
            .expect("first leg"),
        None
    );
}

/// Under a windowed pacer, `first_leg` bounded by the window is the range
/// the drive opens first: the first decision draws one window.
#[tokio::test]
async fn a_windowed_first_leg_is_the_range_the_drive_opens_first() {
    let total = 4 * GROUP;
    let (root, plaintext, _) = synth_blob(total as usize);
    let windowed = fresh_store(root, total);
    let want = first_leg(&windowed, &[(0, 0)], 2 * GROUP, 0)
        .await
        .expect("first leg")
        .expect("a gap remains");
    assert_eq!(want, align_range(0, 2 * GROUP, total).expect("align"));
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext.clone())
        .expect("source")
        .paying(Arc::clone(&ledger));
    let downstream = || super::DownstreamFrontier {
        served_paid: u64::MAX,
        serve_demand: 0,
    };
    drive(
        &windowed,
        &source,
        &crate::pacer::WindowPacer::new(2 * GROUP),
        &Arc::new(Mutex::new(healthy_ctx())),
        &ledger,
        root,
        0,
        0,
        None,
        None,
        Some(&downstream),
        None,
    )
    .await
    .expect("drive");
    assert_eq!(
        source.opened_ranges().first().copied(),
        Some((want.fetch_start(), want.fetch_len()))
    );
}

/// A primed pull of the first leg is adopted, so the drive opens nothing
/// itself (#2063).
#[tokio::test]
async fn a_primed_first_leg_is_adopted_not_reopened() {
    let total = 4 * GROUP;
    let (root, plaintext, _) = synth_blob(total as usize);
    let store = fresh_store(root, total);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let primed = PrimedSource::new(
        ScriptedSource::new(plaintext.clone())
            .expect("source")
            .paying(Arc::clone(&ledger)),
    );
    let range = align_range(0, 0, total).expect("align");
    let (header, reader) = primed
        .inner()
        .open(root, range.clone())
        .await
        .expect("priming open");
    primed.prime(root, range, header, reader, tokio::time::Instant::now());

    drive(
        &store,
        &primed,
        &BudgetPacer::new(),
        &Arc::new(Mutex::new(healthy_ctx())),
        &ledger,
        root,
        0,
        0,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("drive");
    assert_eq!(
        primed.inner().opened_ranges(),
        vec![(0, total)],
        "only the priming open ever reached the source"
    );
    assert_eq!(
        store.read(0, 0).await.expect("read").as_ref(),
        plaintext.as_slice()
    );
}

/// An open of another range goes to the wrapped source and leaves the
/// primed pull parked; `clear` drops it; a stale one is never adopted.
#[tokio::test(start_paused = true)]
async fn a_primed_pull_answers_only_its_own_fresh_range() {
    let total = 4 * GROUP;
    let (root, plaintext, _) = synth_blob(total as usize);
    let primed = PrimedSource::new(ScriptedSource::new(plaintext).expect("source"));
    let first = align_range(0, GROUP, total).expect("align");
    let other = align_range(GROUP, GROUP, total).expect("align");
    let prime = |range: AlignedRange| {
        let primed = &primed;
        async move {
            let (h, r) = primed
                .inner()
                .open(root, range.clone())
                .await
                .expect("open");
            primed.prime(root, range, h, r, tokio::time::Instant::now());
        }
    };

    prime(first.clone()).await;
    primed.open(root, other.clone()).await.expect("other range");
    assert_eq!(primed.inner().opened_ranges().len(), 2, "delegated");
    primed.open(root, first.clone()).await.expect("adopted");
    assert_eq!(
        primed.inner().opened_ranges().len(),
        2,
        "adopted, not opened"
    );

    prime(first.clone()).await;
    primed.clear();
    primed.open(root, first.clone()).await.expect("after clear");
    assert_eq!(
        primed.inner().opened_ranges().len(),
        4,
        "cleared, so opened"
    );

    prime(first.clone()).await;
    let foreign = primed.open([0xEE; 32], first.clone()).await;
    assert!(foreign.is_err(), "another hash is never adopted");
    primed.clear();

    prime(first.clone()).await;
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    primed.open(root, first.clone()).await.expect("stale");
    assert_eq!(primed.inner().opened_ranges().len(), 7, "stale, so opened");

    // The age runs from the pull's open, not from when it was parked: a pull
    // held past the bound before `prime` is stale on arrival.
    let opened_at = tokio::time::Instant::now();
    let (h, r) = primed
        .inner()
        .open(root, first.clone())
        .await
        .expect("open");
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    primed.prime(root, first.clone(), h, r, opened_at);
    primed.open(root, first).await.expect("stale on arrival");
    assert_eq!(
        primed.inner().opened_ranges().len(),
        9,
        "stale on arrival, so opened"
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

/// A resumed drive surfaces the already-present base on the progress bar
/// BEFORE the first chunk is delivered: the very first `on_progress` position
/// is the held prefix's content length (against the whole-blob total), not
/// `0` and not `base + first-chunk`. `fill_gap`'s per-gap reporter fires only
/// from inside `ingest_stream` once streaming begins, so without the
/// pre-stream emit a resumed blob's bar sits at `0` through the pre-fetch
/// window (discovery, pool open, pool resolve), then jumps to the resume
/// point on the first delivered chunk.
#[tokio::test]
async fn resume_base_is_reported_before_the_first_chunk() {
    let total = 4 * GROUP;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let store = fresh_store(root, total);

    // Hold the first two groups: the resume base is two groups of content.
    let held = align_range(0, 2 * GROUP, total).expect("align held");
    preadmit(&store, &plaintext, &outboard, &held).await;
    let base_present = ranges_content_len(&store.present_ranges().await.expect("present"), total);
    assert_eq!(base_present, 2 * GROUP, "scenario: two groups held");

    let samples: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let cb_samples = Arc::clone(&samples);
    let on_progress: Box<ProgressCallback> = Box::new(move |received, expected| {
        if let Ok(mut s) = cb_samples.lock() {
            s.push((received, expected));
        }
    });

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext.clone())
        .expect("source")
        .paying(Arc::clone(&ledger));
    let pacer = BudgetPacer::new();
    let ctx = Arc::new(Mutex::new(healthy_ctx()));

    drive(
        &store,
        &source,
        &pacer,
        &ctx,
        &ledger,
        root,
        0,
        0,
        Some(&on_progress),
        None,
        None,
        None,
    )
    .await
    .expect("drive resumes");

    let samples = samples.lock().expect("samples lock").clone();
    let first = *samples.first().expect("at least one progress sample");
    assert_eq!(
        first,
        (base_present, total),
        "the first reported position must be the resume base, emitted before \
         the first delivered chunk"
    );
    // And it never regresses and reaches the whole blob.
    let mut prev = 0u64;
    for (received, expected) in &samples {
        assert_eq!(*expected, total, "the total stays the whole-blob size");
        assert!(
            *received >= prev,
            "progress regressed: {received} after {prev}"
        );
        prev = *received;
    }
    assert_eq!(prev, total, "the final position reaches the whole blob");
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
    let ctx = Arc::new(Mutex::new(healthy_ctx()));
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));

    drive(
        &store, &source, &pacer, &ctx, &ledger, root, 0, 0, None, None, None, None,
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
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext.clone())
        .expect("source")
        .with_fault_after(5 * 1024 * 1024, || {
            anyhow::anyhow!("scripted mid-gap stall")
        })
        .paying(Arc::clone(&ledger));
    let root = source.root();
    let store = fresh_store(root, total);

    let pacer = BudgetPacer::new();
    let ctx = Arc::new(Mutex::new(healthy_ctx()));

    // Drive #1: faults mid-gap and returns the terminal stall, but checkpoints
    // a durable prefix into the store first.
    let err = drive(
        &store, &source, &pacer, &ctx, &ledger, root, 0, 0, None, None, None, None,
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
        &store, &source, &pacer, &ctx, &ledger, root, 0, 0, None, None, None, None,
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

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext.clone())
        .expect("source")
        .paying(Arc::clone(&ledger));
    let pacer = BudgetPacer::new();
    let ctx = Arc::new(Mutex::new(healthy_ctx()));

    drive(
        &store, &source, &pacer, &ctx, &ledger, root, GROUP, GROUP, None, None, None, None,
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
/// upstream `SpendingCapExhausted` voucher rejection (the shape a mid-fetch
/// funding rejection takes) and delegates every later open to the inner
/// [`ScriptedSource`].
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
                    reason: VoucherRejectReason::SpendingCapExhausted,
                    bundle: None,
                    proof_generation: None,
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

    fn stop(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        Box::pin(async move {
            match reader {
                MaybeFaultReader::Fault(_) => Ok(VoucherProgress::default()),
                MaybeFaultReader::Real(r) => self.inner.stop(r).await,
            }
        })
    }
}

/// A source whose first opens park the queued faults, one per open — the rest
/// delegate to `inner`. Counts every open.
struct FaultingOpens {
    inner: ScriptedSource,
    faults: std::sync::Mutex<std::collections::VecDeque<anyhow::Error>>,
    opens: std::sync::atomic::AtomicUsize,
}

impl FaultingOpens {
    fn new(inner: ScriptedSource, faults: Vec<anyhow::Error>) -> Self {
        Self {
            inner,
            faults: std::sync::Mutex::new(faults.into()),
            opens: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl BlobSource for FaultingOpens {
    type Reader = MaybeFaultReader;

    fn open(
        &self,
        hash: [u8; 32],
        range: AlignedRange,
    ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
        self.opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let fault = self.faults.lock().ok().and_then(|mut f| f.pop_front());
        Box::pin(async move {
            if let Some(fault) = fault {
                let header = UpstreamPullHeader {
                    total_bytes: self.inner.total_bytes(),
                    rate_per_mb: 1,
                    interval_bytes: 1024 * 1024,
                };
                return Ok((header, MaybeFaultReader::Fault(Some(fault))));
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

    fn stop(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        Box::pin(async move {
            match reader {
                MaybeFaultReader::Fault(_) => Ok(VoucherProgress::default()),
                MaybeFaultReader::Real(r) => self.inner.stop(r).await,
            }
        })
    }
}

/// An `Underpaid` rejection carrying the node's watermark `(amount, bytes)`,
/// signed by `ctx`'s own key, for a voucher signed under `proof_generation`.
fn underpaid(
    ctx: &PoolContext,
    (amount, bytes): (u64, u64),
    proof_generation: Option<u64>,
) -> anyhow::Error {
    let signed = decdn_incentive::Voucher {
        pool_id: ctx.pool_id,
        signer: ctx.client_signer.address(),
        provider: ctx.provider,
        amount: U256::from(amount),
        bytes_delivered: U256::from(bytes),
        chain_root: B256::ZERO,
        chunk_price: U256::ZERO,
    }
    .sign(&ctx.client_signer, &ctx.voucher_domain)
    .expect("sign the node's watermark");
    anyhow::Error::new(UpstreamVoucherRejected {
        reason: VoucherRejectReason::Underpaid,
        bundle: Some(decdn_protocol::client::WatermarkBundle {
            amount,
            bytes_delivered: bytes,
            chain_root: [0u8; 32],
            verified_index: 0,
            tip: [0u8; 32],
            chunk_price: 0,
            last_signature: signed.signature.as_bytes().to_vec(),
        }),
        proof_generation,
    })
}

/// A ledger AHEAD of the node: it committed vouchers the node refused.
fn ledger_ahead_of_the_node() -> Arc<PoolLedger> {
    Arc::new(PoolLedger::new(Cumulative {
        bytes: U256::from(9_000u64),
        amount: U256::from(90u64),
    }))
}

/// Drive a two-group blob through `faults` then the scripted source, paying on
/// `ledger`. Returns the drive result, the store and the open count.
async fn drive_through_faults(
    ctx: PoolContext,
    ledger: &Arc<PoolLedger>,
    faults: impl FnOnce(&PoolContext) -> Vec<anyhow::Error>,
) -> (anyhow::Result<()>, ClientRangedStore, usize) {
    let total = 2 * GROUP;
    let (root, plaintext, _outboard) = synth_blob(total as usize);
    let store = fresh_store(root, total);
    let inner = ScriptedSource::new(plaintext)
        .expect("source")
        .paying(Arc::clone(ledger));
    let root = inner.root();
    let source = FaultingOpens::new(inner, faults(&ctx));
    let result = drive(
        &store,
        &source,
        &BudgetPacer::new(),
        &Arc::new(Mutex::new(ctx)),
        ledger,
        root,
        0,
        0,
        None,
        None,
        None,
        None,
    )
    .await;
    let opens = source.opens.load(std::sync::atomic::Ordering::SeqCst);
    (result, store, opens)
}

/// A lane whose ledger ran AHEAD of the node draws an `Underpaid` rejection
/// carrying the node's watermark, signed by our own key. The driver rebases the
/// ledger down to it and completes the fetch instead of failing it.
#[tokio::test]
async fn an_underpaid_rejection_rebases_the_ledger_and_the_fetch_completes() {
    let ledger = ledger_ahead_of_the_node();
    let (result, store, _) = drive_through_faults(healthy_ctx(), &ledger, |ctx| {
        vec![underpaid(ctx, (60, 5_000), Some(0))]
    })
    .await;
    result.expect("the rebased lane completes the fetch");

    assert!(store.is_complete().await.expect("is_complete"));
    assert_eq!(ledger.generation(), 1, "the ledger rebased down once");
    let committed = ledger.committed();
    assert!(
        committed.amount > U256::from(60u64) && committed.amount < U256::from(90u64),
        "the healed ledger builds on the node's anchor, not its own: {committed:?}"
    );
}

/// A lane seeded below the node's anchor draws a `BytesRegression` whose
/// bundle, signed by our own key, is behind on amount and ahead on bytes.
/// The driver rebases the ledger to it and completes the fetch, instead of
/// ending the command.
#[tokio::test]
async fn a_bytes_regression_ahead_on_bytes_rebases_and_the_fetch_completes() {
    let ledger = ledger_ahead_of_the_node();
    let (result, store, opens) = drive_through_faults(healthy_ctx(), &ledger, |ctx| {
        let mut err = underpaid(ctx, (80, 12_000), Some(0));
        if let Some(rejected) = err.downcast_mut::<UpstreamVoucherRejected>() {
            rejected.reason = VoucherRejectReason::BytesRegression;
        }
        vec![err]
    })
    .await;
    result.expect("the rebased lane completes the fetch");

    assert!(store.is_complete().await.expect("is_complete"));
    assert_eq!(opens, 2, "the faulted open plus the healthy one");
    assert_eq!(ledger.generation(), 1, "the ledger rebased once");
    let committed = ledger.committed();
    assert!(
        committed.bytes > U256::from(12_000u64) && committed.amount > U256::from(80u64),
        "the healed ledger builds on the node's anchor: {committed:?}"
    );
}

/// A second `Underpaid` for a voucher signed before the rebase — a sibling's
/// stale rejection — retries the leg without moving the ledger again, and the
/// fetch completes.
#[tokio::test]
async fn a_stale_underpaid_after_a_rebase_retries_without_rebasing() {
    let ledger = ledger_ahead_of_the_node();
    let (result, store, opens) = drive_through_faults(healthy_ctx(), &ledger, |ctx| {
        vec![
            underpaid(ctx, (60, 5_000), Some(0)),
            underpaid(ctx, (55, 4_500), Some(0)),
        ]
    })
    .await;
    result.expect("the stale rejection retries and the fetch completes");
    assert!(store.is_complete().await.expect("is_complete"));
    assert_eq!(
        ledger.generation(),
        1,
        "the stale rejection must not rebase"
    );
    assert!(
        opens >= 3,
        "both faulted opens plus the healthy one, got {opens}"
    );
    assert!(ledger.committed().amount > U256::from(60u64));
}

/// Retrying stale `Underpaid` rejections is bounded by `MAX_RESUME_ATTEMPTS`:
/// the driver gives up with the rejection itself rather than spinning. Each
/// rejection healed the ledger, so the last one carries [`crate::HealExhausted`]
/// and cools only this source.
#[tokio::test]
async fn repeated_underpaid_rejections_are_bounded() {
    let ledger = ledger_ahead_of_the_node();
    let (result, _store, opens) = drive_through_faults(healthy_ctx(), &ledger, |ctx| {
        (0..8)
            .map(|_| underpaid(ctx, (60, 5_000), Some(0)))
            .collect()
    })
    .await;
    let err = result.expect_err("an endless stream of rejections must end the fetch");
    assert!(
        err.downcast_ref::<UpstreamVoucherRejected>()
            .is_some_and(|r| r.reason == VoucherRejectReason::Underpaid),
        "the terminal error is the rejection itself: {err:#}"
    );
    assert!(
        err.downcast_ref::<crate::HealExhausted>().is_some(),
        "a healed rejection past the budget carries the marker: {err:#}"
    );
    assert_eq!(crate::classify(&err), crate::Fault::Source);
    let budget = usize::try_from(crate::MAX_RESUME_ATTEMPTS).unwrap_or(usize::MAX);
    assert_eq!(
        opens,
        budget + 1,
        "one open per resume attempt, plus the first"
    );
}

/// Repeated `BytesRegression` rejections are bounded by
/// `MAX_RESUME_ATTEMPTS`. The first bundle rebases the ledger and the rest
/// are stale, so every one healed it: the last one carries
/// [`crate::HealExhausted`] and cools only this source.
#[tokio::test]
async fn repeated_bytes_regression_rejections_are_bounded_and_scoped() {
    let ledger = ledger_ahead_of_the_node();
    let (result, _store, opens) = drive_through_faults(healthy_ctx(), &ledger, |ctx| {
        (0..8)
            .map(|_| {
                let mut err = underpaid(ctx, (80, 12_000), Some(0));
                if let Some(rejected) = err.downcast_mut::<UpstreamVoucherRejected>() {
                    rejected.reason = VoucherRejectReason::BytesRegression;
                }
                err
            })
            .collect()
    })
    .await;
    let err = result.expect_err("an endless stream of rejections must end the fetch");
    assert!(
        err.downcast_ref::<UpstreamVoucherRejected>()
            .is_some_and(|r| r.reason == VoucherRejectReason::BytesRegression),
        "the terminal error is the rejection itself: {err:#}"
    );
    assert!(
        err.downcast_ref::<crate::HealExhausted>().is_some(),
        "a healed rejection past the budget carries the marker: {err:#}"
    );
    assert_eq!(crate::classify(&err), crate::Fault::Source);
    assert_eq!(ledger.generation(), 1, "only the first bundle rebased");
    let budget = usize::try_from(crate::MAX_RESUME_ATTEMPTS).unwrap_or(usize::MAX);
    assert_eq!(
        opens,
        budget + 1,
        "one open per resume attempt, plus the first"
    );
}

/// A spending-cap rejection whose bundle keeps reseeding the ledger is
/// healed within the resume budget, and past it ends the drive bare: it is
/// not a lane-watermark fault, so it never carries the heal marker, and it
/// acts as `Unfunded` from its source.
#[tokio::test]
async fn a_non_lane_rejection_past_the_budget_is_never_scoped_to_the_source() {
    let reason = VoucherRejectReason::SpendingCapExhausted;
    let ledger = ledger_ahead_of_the_node();
    let (result, _store, opens) = drive_through_faults(healthy_ctx(), &ledger, |ctx| {
        (0..8u64)
            .map(|i| {
                let mut err = underpaid(ctx, (100_000 + i * 1_000, 50_000 + i * 1_000), None);
                if let Some(rejected) = err.downcast_mut::<UpstreamVoucherRejected>() {
                    rejected.reason = reason;
                }
                err
            })
            .collect()
    })
    .await;
    let err = result.expect_err("an endless stream of rejections must end the fetch");
    assert!(
        err.downcast_ref::<UpstreamVoucherRejected>()
            .is_some_and(|r| r.reason == reason),
        "{reason:?}: the terminal error is the rejection itself: {err:#}"
    );
    assert!(
        err.downcast_ref::<crate::HealExhausted>().is_none(),
        "{reason:?}: never carries the marker: {err:#}"
    );
    assert_eq!(
        crate::classify(&err),
        crate::Fault::Unaffordable,
        "{reason:?}"
    );
    let budget = usize::try_from(crate::MAX_RESUME_ATTEMPTS).unwrap_or(usize::MAX);
    assert_eq!(
        opens,
        budget + 1,
        "{reason:?}: one open per resume attempt, plus the first"
    );
}

/// A voucher rejection that no heal takes ends the drive on the first open
/// and acts as `Declined` from its source (ADR 005): an `Underpaid` with no
/// bundle has no watermark to rebase to, a `BytesRegression` with no bundle
/// is a single-signer fault, and a trailing proof whose bundle the ledger
/// covers on amount but not on bytes cannot heal.
#[tokio::test]
async fn a_rejection_no_heal_takes_declines_its_source() {
    fn bare(reason: VoucherRejectReason) -> anyhow::Error {
        anyhow::Error::new(UpstreamVoucherRejected {
            reason,
            bundle: None,
            proof_generation: None,
        })
    }
    type MakeFault = fn(&PoolContext) -> anyhow::Error;
    let cases: [(&str, MakeFault); 3] = [
        ("underpaid without a bundle", |_| {
            bare(VoucherRejectReason::Underpaid)
        }),
        ("bytes regression", |_| {
            bare(VoucherRejectReason::BytesRegression)
        }),
        ("under-fold the ledger covers on amount only", |ctx| {
            let mut err = underpaid(ctx, (90, 9_500), None);
            if let Some(rejected) = err.downcast_mut::<UpstreamVoucherRejected>() {
                rejected.reason = VoucherRejectReason::UnderFold;
            }
            err
        }),
    ];
    for (name, fault) in cases {
        let ledger = ledger_ahead_of_the_node();
        let (result, _store, opens) =
            drive_through_faults(healthy_ctx(), &ledger, |ctx| vec![fault(ctx)]).await;
        let err = result.expect_err(name);
        assert!(
            err.downcast_ref::<UpstreamVoucherRejected>().is_some(),
            "{name}: the terminal error is the rejection: {err:#}"
        );
        assert!(
            err.downcast_ref::<crate::HealExhausted>().is_none(),
            "{name}: no heal took the rejection: {err:#}"
        );
        assert_eq!(
            crate::classify(&err),
            crate::Fault::Source,
            "{name}: the source declines this fetch"
        );
        assert!(
            crate::fault::declining_rejection(&err).is_some(),
            "{name}: acts as Declined"
        );
        assert_eq!(opens, 1, "{name}: no retry");
    }
}

/// A lane never adds funds: a mid-stream `SpendingCapExhausted` on an
/// under-deposited lane ends the lane with the rejection, which the acquire
/// loop prices out at the current deposit. No re-open, no top-up.
#[tokio::test(start_paused = true)]
async fn a_mid_stream_funding_rejection_ends_the_lane_without_a_top_up() {
    let total = 2 * GROUP;
    let (root, plaintext, _outboard) = synth_blob(total as usize);
    let store = fresh_store(root, total);

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let inner = ScriptedSource::new(plaintext)
        .expect("source")
        .paying(Arc::clone(&ledger));
    let root = inner.root();
    let source = FailFirstOpen {
        inner,
        opens: std::sync::atomic::AtomicUsize::new(0),
    };

    let mut ctx = healthy_ctx();
    ctx.deposit = U256::ZERO;
    let ctx = Arc::new(Mutex::new(ctx));

    let err = drive(
        &store,
        &source,
        &BudgetPacer::new(),
        &ctx,
        &ledger,
        root,
        0,
        0,
        None,
        None,
        None,
        None,
    )
    .await
    .expect_err("a funding rejection ends the lane");

    assert_eq!(crate::classify(&err), crate::Fault::Unaffordable, "{err:#}");
    assert_eq!(
        source.opens.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "no re-open inside the lane"
    );
    assert!(!store.is_complete().await.expect("is_complete"));
}

/// A deposit that cannot cover the next voucher refuses the next leg with
/// [`PoolExhausted`] instead of funding it: one leg lands, the next pass
/// finds the deposit spent, and the lane ends priced out.
#[tokio::test(start_paused = true)]
async fn a_lane_short_of_the_next_voucher_refuses_with_pool_exhausted() {
    let total = 3 * GROUP;
    let (root, plaintext, _outboard) = synth_blob(total as usize);
    let store = fresh_store(root, total);

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext)
        .expect("source")
        .paying(Arc::clone(&ledger));
    let root = source.root();

    // One group per leg: a window pacer with the downstream always paid up.
    let pacer = crate::pacer::WindowPacer::new(GROUP);
    let downstream = || super::DownstreamFrontier {
        served_paid: u64::MAX,
        serve_demand: 0,
    };
    let mut ctx = healthy_ctx();
    ctx.deposit = U256::from(1u64);
    let ctx = Arc::new(Mutex::new(ctx));

    let err = drive(
        &store,
        &source,
        &pacer,
        &ctx,
        &ledger,
        root,
        0,
        0,
        None,
        None,
        Some(&downstream),
        None,
    )
    .await
    .expect_err("a spent deposit refuses the next leg");

    assert!(err.downcast_ref::<PoolExhausted>().is_some(), "{err:#}");
    assert_eq!(crate::classify(&err), crate::Fault::Unaffordable);
    assert_eq!(source.opened_ranges().len(), 1, "one leg, then the refusal");
    assert!(!store.is_complete().await.expect("is_complete"));
}

/// A [`BlobSource`] that refuses its FIRST open with an owner-only
/// [`StreamError::Unfunded`] (ADR 003 §Pool solvency): the serving node's
/// floor `M` beyond the buyer's estimate, and
/// serves every later open from the inner [`ScriptedSource`]. The refusal
/// arrives at the open (the node signed `ok: false`, so there is no header and
/// no reader), exactly as a real one does.
struct RefuseFirstOpenShortDeposit {
    inner: ScriptedSource,
    opens: std::sync::atomic::AtomicUsize,
    /// How many opens, from the first, it refuses.
    refusals: usize,
}

impl BlobSource for RefuseFirstOpenShortDeposit {
    type Reader = <ScriptedSource as BlobSource>::Reader;

    fn open(
        &self,
        hash: [u8; 32],
        range: AlignedRange,
    ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
        let n = self.opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            if n < self.refusals {
                // A real open-stage refusal: the node signs `StreamResponse
                // { ok: false }` with the delivery-side `Unfunded` in
                // the trailing ext, exactly the shape `open_progressive_pull`
                // builds via the crate-private `UpstreamRefused::open`. Built here
                // (rather than `mid_stream`) so the refusal carries open-stage
                // evidence, as a real one does.
                let body = StreamResponseBody {
                    hash,
                    ok: false,
                    rate_per_mb: 1,
                    total_bytes: self.inner.total_bytes(),
                    pool_id: [0u8; 32],
                    timestamp_us: 0,
                };
                // `open` retains the response as-is without re-validating the
                // signature, so an EOA-length (65-byte) placeholder slash-sig is
                // enough: this test exercises the refusal's routing, not slash
                // evidence.
                let resp = StreamResponse {
                    body,
                    slash_sig: vec![0u8; 65],
                };
                let ext = StreamResponseExt {
                    error: Some(StreamError::Unfunded),
                };
                return Err(UpstreamRefused::open(resp, &ext));
            }
            self.inner.open(hash, range).await
        })
    }

    fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        Box::pin(async move { self.inner.finish(reader).await })
    }

    fn stop(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        self.inner.stop(reader)
    }
}

/// An open-stage `Unfunded` refusal on an affordable deposit (the node's
/// private floor `M` above what the buyer holds) ends the lane as
/// unaffordable at once: the refusal scopes to that node, and only the
/// acquire loop's funding recovery step may add funds.
#[tokio::test(start_paused = true)]
async fn an_unfunded_open_refusal_ends_the_lane_as_unaffordable() {
    let total = 2 * GROUP;
    let (root, plaintext, _outboard) = synth_blob(total as usize);
    let store = fresh_store(root, total);

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let inner = ScriptedSource::new(plaintext)
        .expect("source")
        .paying(Arc::clone(&ledger));
    let root = inner.root();
    let source = RefuseFirstOpenShortDeposit {
        inner,
        opens: std::sync::atomic::AtomicUsize::new(0),
        refusals: 1,
    };

    let mut ctx = healthy_ctx();
    ctx.deposit = U256::from(1_000u64);
    let ctx = Arc::new(Mutex::new(ctx));

    let err = drive(
        &store,
        &source,
        &BudgetPacer::new(),
        &ctx,
        &ledger,
        root,
        0,
        0,
        None,
        None,
        None,
        None,
    )
    .await
    .expect_err("an Unfunded refusal ends the lane");

    assert_eq!(crate::classify(&err), crate::Fault::Unaffordable, "{err:#}");
    assert_eq!(
        source.opens.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "no re-open inside the lane"
    );
}

/// A [`Pacer`] stub for the `up_to_bytes` clamp test: `Draw { up_to_bytes }`
/// on its first call, `Done` on every call after — so a driver that ignores
/// the clamp and drains the whole gap in one open would still only see ONE
/// open, while a driver that honors it opens exactly `up_to_bytes` and then
/// (correctly) stops early, leaving the rest of the gap unfilled.
struct OnceDrawThenDone {
    up_to_bytes: u64,
    drawn: std::sync::atomic::AtomicBool,
}

impl crate::pacer::Pacer for OnceDrawThenDone {
    fn decide(&self, _state: &PaceState) -> PaceDecision {
        if self.drawn.swap(true, std::sync::atomic::Ordering::SeqCst) {
            PaceDecision::Done
        } else {
            PaceDecision::Draw {
                up_to_bytes: self.up_to_bytes,
            }
        }
    }
}

#[tokio::test]
async fn draw_clamps_the_open_to_up_to_bytes() {
    // A single 4-group gap; the pacer authorizes only ONE group's worth. The
    // driver must open exactly GROUP bytes, not the whole 4*GROUP gap — this
    // is the up_to_bytes clamp.
    let total = 4 * GROUP;
    let (root, plaintext, _outboard) = synth_blob(total as usize);
    let store = fresh_store(root, total);

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext.clone())
        .expect("source")
        .paying(Arc::clone(&ledger));
    let pacer = OnceDrawThenDone {
        up_to_bytes: GROUP,
        drawn: std::sync::atomic::AtomicBool::new(false),
    };
    let ctx = Arc::new(Mutex::new(healthy_ctx()));

    drive(
        &store, &source, &pacer, &ctx, &ledger, root, 0, 0, None, None, None, None,
    )
    .await
    .expect("drive stops cleanly once the stub pacer says Done");

    assert_eq!(
        source.opened_ranges(),
        vec![(0, GROUP)],
        "the driver must clamp the open to the pacer's up_to_bytes, not the \
         whole gap"
    );
    assert!(
        !store.is_complete().await.expect("is_complete"),
        "only one group of four was authorized, so the blob stays incomplete"
    );
}

/// A [`PacingWait`] stub that counts calls and resolves immediately.
struct CountingWait {
    calls: std::sync::atomic::AtomicUsize,
}

impl super::PacingWait for CountingWait {
    fn wait(
        &self,
        _observed: super::DownstreamFrontier,
        _reason: super::WaitReason,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async {})
    }
}

/// A [`Pacer`] stub for the `Wait` test: `Wait` on the first call, then
/// defers to [`BudgetPacer`] on every call after — modeling a window pacer
/// that frees up room only after the driver's injected wait hook fires.
struct WaitOnceThenBudget {
    waited: std::sync::atomic::AtomicBool,
}

impl crate::pacer::Pacer for WaitOnceThenBudget {
    fn decide(&self, state: &PaceState) -> PaceDecision {
        if self.waited.swap(true, std::sync::atomic::Ordering::SeqCst) {
            BudgetPacer::new().decide(state)
        } else {
            PaceDecision::Wait
        }
    }
}

#[tokio::test]
async fn wait_decision_awaits_the_pacing_hook_once_then_completes() {
    let total = GROUP;
    let (root, plaintext, _outboard) = synth_blob(total as usize);
    let store = fresh_store(root, total);

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext.clone())
        .expect("source")
        .paying(Arc::clone(&ledger));
    let pacer = WaitOnceThenBudget {
        waited: std::sync::atomic::AtomicBool::new(false),
    };
    let ctx = Arc::new(Mutex::new(healthy_ctx()));
    let wait_hook = CountingWait {
        calls: std::sync::atomic::AtomicUsize::new(0),
    };

    drive(
        &store,
        &source,
        &pacer,
        &ctx,
        &ledger,
        root,
        0,
        0,
        None,
        Some(&wait_hook),
        None,
        None,
    )
    .await
    .expect("drive completes after the one scripted Wait");

    assert_eq!(
        wait_hook.calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the driver must await the PacingWait hook exactly once for the \
         single scripted Wait decision"
    );
    assert!(store.is_complete().await.expect("is_complete"));
    let got = store.read(0, 0).await.expect("read whole blob");
    assert_eq!(got.as_ref(), plaintext.as_slice());
}

/// A [`PacingWait`] hook that simulates the downstream serve leg clearing one
/// window's worth of payment each time it is awaited: it bumps a shared
/// counter by `bump_bytes` and resolves immediately (no real sleep), so the
/// test stays deterministic. The SAME counter backs the `served_paid` reader
/// passed to `drive`, so this is the only thing that can unstick a
/// `WindowPacer::Wait`.
struct BumpDownstreamWait {
    served_paid: Arc<std::sync::atomic::AtomicU64>,
    bump_bytes: u64,
}

impl super::PacingWait for BumpDownstreamWait {
    fn wait(
        &self,
        _observed: super::DownstreamFrontier,
        _reason: super::WaitReason,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.served_paid
            .fetch_add(self.bump_bytes, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async {})
    }
}

#[tokio::test]
async fn window_pacer_gates_on_injected_served_paid() {
    // A 3-group gap with a WindowPacer window of exactly one group: the pull
    // can only run one group ahead of `served_paid` before it must `Wait`.
    // Nothing but the injected `served_paid` reader (bumped by the
    // `PacingWait` hook, standing in for the downstream serve leg clearing
    // payment) can let the drive make further progress — proving both the
    // seam wiring (`served_paid` reaches `WindowPacer` via `PaceState`) and
    // the `Wait` -> hook -> re-decide loop actually advances against it.
    let total = 3 * GROUP;
    let (root, plaintext, _outboard) = synth_blob(total as usize);
    let store = fresh_store(root, total);

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext.clone())
        .expect("source")
        .paying(Arc::clone(&ledger));
    let pacer = crate::pacer::WindowPacer::new(GROUP);
    let ctx = Arc::new(Mutex::new(healthy_ctx()));

    let served_paid_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let wait_hook = BumpDownstreamWait {
        served_paid: Arc::clone(&served_paid_counter),
        bump_bytes: GROUP,
    };
    let served_paid_reader = {
        let counter = Arc::clone(&served_paid_counter);
        move || super::DownstreamFrontier {
            served_paid: counter.load(std::sync::atomic::Ordering::SeqCst),
            serve_demand: 0,
        }
    };

    drive(
        &store,
        &source,
        &pacer,
        &ctx,
        &ledger,
        root,
        0,
        0,
        None,
        Some(&wait_hook),
        Some(&served_paid_reader),
        None,
    )
    .await
    .expect("drive completes as served_paid advances one window at a time");

    // The window forced at least the two `Wait`s a 3-group gap under a
    // 1-group window needs (group 2 and group 3 each had to wait for the
    // previous group's payment to clear downstream).
    assert!(
        served_paid_counter.load(std::sync::atomic::Ordering::SeqCst) >= 2 * GROUP,
        "the injected served_paid reader must have been advanced by the wait \
         hook for the drive to complete"
    );

    assert!(store.is_complete().await.expect("is_complete"));
    let got = store.read(0, 0).await.expect("read whole blob");
    assert_eq!(
        got.as_ref(),
        plaintext.as_slice(),
        "byte-exact after a window-paced, served-paid-gated drive"
    );
}

/// A [`PacingWait`] hook that models a caught-up, paying downstream client:
/// each wait clears one more chunk of payment, up to the pull frontier less
/// the one chunk and two group roundings that always sit between delivery
/// and the paid frontier. With `demand`, a serve leg that has drained to the
/// pull frontier also raises a serve demand one byte past it.
struct CatchingUpWait<'a> {
    source: &'a crate::source::ScriptedSource,
    served_paid: Arc<std::sync::atomic::AtomicU64>,
    serve_demand: Arc<std::sync::atomic::AtomicU64>,
    demand: bool,
}

impl CatchingUpWait<'_> {
    fn pulled(&self) -> u64 {
        self.source
            .opened_ranges()
            .iter()
            .map(|&(start, len)| start + len)
            .max()
            .unwrap_or(0)
    }
}

impl super::PacingWait for CatchingUpWait<'_> {
    fn wait(
        &self,
        _observed: super::DownstreamFrontier,
        _reason: super::WaitReason,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        use std::sync::atomic::Ordering::SeqCst;
        let pulled = self.pulled();
        let chunk = decdn_protocol::client::CHUNK_BYTES;
        let cap = pulled.saturating_sub(chunk + 2 * GROUP);
        let paid = self.served_paid.load(SeqCst);
        if paid < cap {
            self.served_paid.store((paid + chunk).min(cap), SeqCst);
        } else if self.demand {
            self.serve_demand.store(pulled + 1, SeqCst);
        }
        Box::pin(async {})
    }
}

/// With a ramped window past [`crate::pacer::MIN_DRAW_WINDOW`], a caught-up client
/// sees the pull draw at least half the window per upstream open, except the
/// last piece of the gap, and the drive completes: the minimum draw batches
/// the room without stalling the pull. The variant with a parked serve leg
/// raising demand draws just as large, so the demand floor does not
/// collapse draws back to about one chunk.
#[tokio::test(start_paused = true)]
async fn a_large_window_draws_at_least_half_per_open_and_completes() {
    use crate::pacer::{PULL_WINDOW_FLOOR, RampPacer};

    for demand in [false, true] {
        let window = 8 * PULL_WINDOW_FLOOR;
        assert!(
            window >= crate::pacer::MIN_DRAW_WINDOW,
            "premise: the minimum is on"
        );
        let total = 24 * PULL_WINDOW_FLOOR + 5 * GROUP;
        let (root, plaintext, _outboard) = synth_blob(usize::try_from(total).unwrap());
        let store = fresh_store(root, total);
        let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
        let source = ScriptedSource::new(plaintext.clone())
            .expect("source")
            .paying(Arc::clone(&ledger));
        let pacer = RampPacer {
            divisor: 0,
            floor: PULL_WINDOW_FLOOR,
            credit_max: window,
            paid_base: 0,
            paid_carried: 0,
        };
        let served_paid = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let serve_demand = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let wait_hook = CatchingUpWait {
            source: &source,
            served_paid: Arc::clone(&served_paid),
            serve_demand: Arc::clone(&serve_demand),
            demand,
        };
        let reader = {
            let (paid, dem) = (Arc::clone(&served_paid), Arc::clone(&serve_demand));
            move || super::DownstreamFrontier {
                served_paid: paid.load(std::sync::atomic::Ordering::SeqCst),
                serve_demand: dem.load(std::sync::atomic::Ordering::SeqCst),
            }
        };
        let ctx = Arc::new(Mutex::new(healthy_ctx()));

        tokio::time::timeout(
            std::time::Duration::from_mins(1),
            drive(
                &store,
                &source,
                &pacer,
                &ctx,
                &ledger,
                root,
                0,
                0,
                None,
                Some(&wait_hook),
                Some(&reader),
                None,
            ),
        )
        .await
        .expect("the drive must not wedge on the minimum draw")
        .expect("drive completes");

        assert!(store.is_complete().await.expect("is_complete"));
        let opened = source.opened_ranges();
        let (last, rest) = opened.split_last().expect("at least one open");
        assert!(
            rest.iter().all(|&(_, len)| len >= window / 2),
            "demand={demand}: every open but the last draws at least half the \
             window ({}): {opened:?}",
            window / 2
        );
        assert!(last.1 > 0);
        assert!(
            opened.len() as u64 <= total.div_ceil(window / 2) + 1,
            "demand={demand}: the open count is bounded by total / (window / 2): \
             {opened:?}"
        );
        let got = store.read(0, 0).await.expect("read whole blob");
        assert_eq!(got.as_ref(), plaintext.as_slice());
    }
}

/// A [`BlobSource`] whose upstream payment trails its delivery: each clean
/// leg commits only half of the leg's wire, so the paid frontier the driver
/// opens at stays behind the delivered frontier the pacer measures. Opens past
/// `max_opens` fail, so a driver that keeps re-opening without parking fails
/// the test instead of hanging it.
struct HalfPaySource {
    inner: ScriptedSource,
    ledger: Arc<PoolLedger>,
    leg_wire: Mutex<u64>,
    opens: std::sync::atomic::AtomicUsize,
    max_opens: usize,
}

impl BlobSource for HalfPaySource {
    type Reader = crate::source::ScriptedReader;

    fn open(
        &self,
        hash: [u8; 32],
        range: AlignedRange,
    ) -> SourceFuture<'_, (UpstreamPullHeader, Self::Reader)> {
        Box::pin(async move {
            let opens = self.opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            anyhow::ensure!(
                opens < self.max_opens,
                "open budget of {} spent: {:?}",
                self.max_opens,
                self.inner.opened_ranges()
            );
            *self.leg_wire.lock().expect("leg wire lock") = range.wire_len();
            self.inner.open(hash, range).await
        })
    }

    fn finish(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        Box::pin(async move {
            self.inner.finish(reader).await?;
            let half = *self.leg_wire.lock().expect("leg wire lock") / 2;
            self.ledger
                .issue(half, 1, crate::EpochAction::Keep, |_next, _chain| async {
                    Ok(())
                })
                .await?;
            Ok(VoucherProgress::from_cumulative(
                self.ledger.committed(),
                U256::ZERO,
            ))
        })
    }

    fn stop(&self, reader: Self::Reader) -> SourceFuture<'_, VoucherProgress> {
        self.inner.stop(reader)
    }
}

/// A [`PacingWait`] hook that counts calls and then never resolves, so a
/// drive whose window stays closed parks on it.
struct ParkingWait {
    calls: std::sync::atomic::AtomicUsize,
}

impl super::PacingWait for ParkingWait {
    fn wait(
        &self,
        _observed: super::DownstreamFrontier,
        _reason: super::WaitReason,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(std::future::pending())
    }
}

/// An in-band serve demand is fetched even when upstream payment lags
/// delivery by about one pull-window floor. The pacer measures the demand
/// from the delivered frontier, but the driver opens each draw at the paid
/// frontier, so one floor drawn from the paid frontier does not reach the
/// demand. The drive still fetches the demand within a bounded number of
/// opens of at most one floor each, stops once the demand is covered, and
/// then parks. The downstream client pays nothing throughout.
#[tokio::test(start_paused = true)]
async fn in_band_demand_converges_when_upstream_payment_lags_delivery() {
    use crate::pacer::{PULL_WINDOW_FLOOR, RampPacer};

    const FLOOR: u64 = PULL_WINDOW_FLOOR;
    // Room past `window + 2 * FLOOR`, so an over-sized demand draw is not
    // clipped by the blob end.
    let total = 5 * FLOOR;
    let (_root, plaintext, _outboard) = synth_blob(total as usize);

    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let inner = ScriptedSource::new(plaintext).expect("source");
    let root = inner.root();
    let store = fresh_store(root, total);
    let source = HalfPaySource {
        inner,
        ledger: Arc::clone(&ledger),
        leg_wire: Mutex::new(0),
        opens: std::sync::atomic::AtomicUsize::new(0),
        max_opens: 8,
    };

    // A window of two floors: `divisor: 0` fixes the window at `credit_max`,
    // and `served_paid` stays 0, so the window never slides. The first leg
    // fills it, and half payment leaves the paid frontier about one floor
    // behind delivery.
    let window = 2 * FLOOR;
    let pacer = RampPacer {
        divisor: 0,
        floor: FLOOR,
        credit_max: window,
        paid_base: 0,
        paid_carried: 0,
    };
    // The serve leg is parked on the first byte past the closed window: one
    // byte ahead of the pull once the window fills, and so in band.
    let demand = window + 1;
    // The fixture really lags: after the first leg, one floor drawn from the
    // paid frontier stops short of the demand.
    let first_leg_wire = align_range(0, window, total).expect("align").wire_len();
    let paid_after_first_leg = crate::sink::content_paid_frontier(0, total, first_leg_wire / 2);
    assert!(
        paid_after_first_leg + FLOOR < demand,
        "one floor from the paid frontier ({paid_after_first_leg}) must not reach \
         the demand ({demand}), or this test does not cover the lag"
    );
    let downstream = || super::DownstreamFrontier {
        served_paid: 0,
        serve_demand: demand,
    };
    let wait_hook = ParkingWait {
        calls: std::sync::atomic::AtomicUsize::new(0),
    };
    let ctx = Arc::new(Mutex::new(healthy_ctx()));

    let outcome = tokio::time::timeout(
        std::time::Duration::from_mins(1),
        drive(
            &store,
            &source,
            &pacer,
            &ctx,
            &ledger,
            root,
            0,
            0,
            None,
            Some(&wait_hook),
            Some(&downstream),
            None,
        ),
    )
    .await;
    assert!(
        outcome.is_err(),
        "the drive must park on the closed window, not end: {outcome:?}"
    );

    let opened = source.inner.opened_ranges();
    assert_eq!(
        opened.first(),
        Some(&(0, window)),
        "the first leg fills the window: {opened:?}"
    );
    let covering = opened
        .iter()
        .position(|&(start, len)| start < demand && start + len >= demand)
        .unwrap_or_else(|| panic!("no open covers the demand {demand}: {opened:?}"));
    assert_eq!(
        covering + 1,
        opened.len(),
        "the pull stops once the demand is covered: {opened:?}"
    );
    // After the window fills, every open is a serve-demand draw, and each
    // draws at most one floor.
    assert!(
        opened.iter().skip(1).all(|&(_, len)| len <= FLOOR),
        "each serve-demand draw is at most one floor ({FLOOR}): {opened:?}"
    );
    assert_eq!(
        wait_hook.calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the pull waits only after the demand is fetched"
    );
    assert!(
        store
            .missing_ranges(0, demand)
            .await
            .expect("missing")
            .is_empty(),
        "the demanded byte is in the store"
    );
}

/// The interval flush persists resume progress MID-fetch, not only at
/// completion: `drive_with_interval_flush` is raced against a
/// work future that stays pending for several short intervals, and the
/// store's `flush_present_record` must fire ONCE PER ELAPSED INTERVAL before
/// the work resolves — so a crash between the last flush and completion loses
/// at most one interval, never the whole in-flight fetch. The flushed record
/// equals the durable checkpointed prefix reopened from disk.
///
/// `start_paused` is what makes the flush count an arithmetic fact rather
/// than a scheduling one. On the real clock the work future's sleep and the
/// flush interval both race the runner: a starved runtime that is descheduled
/// past the work's deadline finds it already elapsed, and the `biased` select
/// in `drive_with_interval_flush` prefers completion — so the loop can exit
/// having flushed once, failing an assertion about periodicity for a reason
/// that has nothing to do with periodicity. Under the virtual clock time
/// advances only to a registered deadline — here the interval's ticks and the
/// work's own sleep, and nothing else — so the tick count is fixed by the two
/// durations below.
#[tokio::test(start_paused = true)]
async fn interval_flush_persists_progress_before_completion() {
    let total = 8 * 1024 * 1024;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let dir = tmp_dir();
    let inner = ClientRangedStore::create(dir.path(), "blob", root, total).expect("create");
    // Durably checkpoint a real, verified 4 MiB prefix into the store — the
    // resume progress the interval flush must persist — without yet writing
    // the `.ranges` record (checkpoint never does).
    let prefix = align_range(0, 4 * 1024 * 1024, total).expect("align prefix");
    preadmit(&inner, &plaintext, &outboard, &prefix).await;

    let flushes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let store = FlushCountingStore {
        flushes: Arc::clone(&flushes),
        ..FlushCountingStore::new(inner, std::time::Duration::ZERO)
    };

    // A work future that stays pending across several 20 ms intervals, so the
    // interval owner flushes repeatedly before the work resolves.
    let work = async {
        tokio::time::sleep(std::time::Duration::from_millis(130)).await;
        Ok(())
    };
    super::drive_with_interval_flush(&store, std::time::Duration::from_millis(20), work)
        .await
        .expect("interval-flush wrapper completes when the work future resolves");

    // Six ticks, exactly: the immediate first tick is consumed before the
    // loop, leaving deadlines at 20..=120 ms inside the work's 130 ms, and
    // the virtual clock advances only to those deadlines. 130 is not a
    // multiple of 20, so the `biased` select never has to adjudicate a tie
    // at the boundary. The flush is periodic, not a single completion flush;
    // dropping the pre-loop tick consumption would read as seven.
    let count = flushes.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        count, 6,
        "the interval owner must flush once per elapsed interval during a \
         still-running fetch"
    );

    // The interval flush is a REAL persist: reopening the store from disk
    // recovers exactly the checkpointed 4 MiB prefix — a crash right here
    // would resume from it, not refetch from zero.
    let reopened = ClientRangedStore::open(dir.path(), "blob", root).expect("reopen");
    let present = reopened.present_ranges().await.expect("present");
    assert_eq!(
        present,
        prefix.chunk_ranges().clone(),
        "the interval-flushed record must persist the checkpointed prefix"
    );
}

/// A slow record write never pauses the fetch. `fut` carries every lane's
/// pay loop, so the interval owner runs each flush beside it rather than
/// instead of it. Here each flush takes 50 ms against a 20 ms interval, and
/// the work is 13 sequential 10 ms steps. Run beside the work, the flushes
/// leave it finishing at 130 ms of virtual time. Awaited inline, every tick
/// would stall the work by 50 ms.
#[tokio::test(start_paused = true)]
async fn interval_flush_runs_beside_the_fetch() {
    let total = 8 * 1024 * 1024;
    let (root, _plaintext, _outboard) = synth_blob(total as usize);
    let dir = tmp_dir();
    let inner = ClientRangedStore::create(dir.path(), "blob", root, total).expect("create");
    let flushes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let store = FlushCountingStore {
        flushes: Arc::clone(&flushes),
        ..FlushCountingStore::new(inner, std::time::Duration::from_millis(50))
    };

    let started = tokio::time::Instant::now();
    let work = async move {
        for _ in 0..13 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(started.elapsed())
    };
    let work_elapsed = Arc::new(std::sync::Mutex::new(None));
    let record = Arc::clone(&work_elapsed);
    super::drive_with_interval_flush(&store, std::time::Duration::from_millis(20), async move {
        let elapsed = work.await?;
        if let Ok(mut slot) = record.lock() {
            *slot = Some(elapsed);
        }
        Ok(())
    })
    .await
    .expect("interval-flush wrapper completes");

    let elapsed = work_elapsed
        .lock()
        .ok()
        .and_then(|slot| *slot)
        .expect("the work recorded its elapsed time");
    assert!(
        elapsed < std::time::Duration::from_millis(140),
        "a slow record flush must not pause the fetch: the work took {elapsed:?}"
    );
    assert!(
        flushes.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "the interval owner must still flush during the fetch"
    );
}

/// A steal lowers the gap's end while its leg streams: the leg stops at
/// the split on its one stream, pays for the wire it read, and the gap
/// ends there with no second leg.
#[tokio::test]
async fn fill_gap_stops_its_leg_at_a_lowered_end() {
    let total = 8 * GROUP;
    let (root, plaintext, _) = synth_blob(total as usize);
    let store = fresh_store(root, total);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let (gate, held) = tokio::sync::watch::channel(false);
    let source = ScriptedSource::new(plaintext)
        .expect("source")
        .gated_on(held)
        .paying(Arc::clone(&ledger));
    let pacer = BudgetPacer::new();
    let ctx = Arc::new(Mutex::new(healthy_ctx()));
    let mut counters = super::DriveCounters::new();
    let stop_at = std::sync::atomic::AtomicU64::new(u64::MAX);
    let fill = super::fill_gap(
        &store,
        &source,
        &pacer,
        &ctx,
        &ledger,
        root,
        0,
        total,
        &mut counters,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(&stop_at),
    );
    // Lower the end once the leg is open, then let it stream.
    let steal = async {
        while source.opened_ranges().is_empty() {
            tokio::task::yield_now().await;
        }
        stop_at.store(3 * GROUP, std::sync::atomic::Ordering::Release);
        gate.send_replace(true);
    };
    let (filled, ()) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        futures_util::future::join(fill, steal),
    )
    .await
    .expect("the gap ends at the split");
    filled.expect("a stopped leg ends the gap cleanly");

    assert_eq!(
        source.opened_ranges(),
        vec![(0, total)],
        "one leg, no reopen"
    );
    assert_eq!(source.stopped_pulls(), 1, "the leg stops at the split");
    let kept = align_range(0, 3 * GROUP, total).expect("align");
    assert_eq!(
        &store.present_ranges().await.expect("present"),
        kept.chunk_ranges()
    );
    assert_eq!(
        ledger.committed().bytes,
        U256::from(kept.wire_len()),
        "the stopped leg pays for the wire up to the split"
    );
}

/// A source that claims `total_bytes == 0` for a NON-empty root (#1054) is a
/// paid-but-wrong delivery: the empty store has no gap to pull and no chunk
/// group for any decoder to anchor, so without the up-front root check the
/// driver would finalize an empty blob under an arbitrary hash. The check is
/// the driver's, so it holds for every consumer the driver serves — the CLI's
/// ranged store and the node's cache admit alike.
#[tokio::test]
async fn drive_rejects_an_empty_claim_for_a_non_empty_root() {
    let wanted = *blake3::hash(b"not the empty blob").as_bytes();
    let store = fresh_store(wanted, 0);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(Vec::new()).expect("source");
    let pacer = BudgetPacer::new();
    let ctx = Arc::new(Mutex::new(healthy_ctx()));

    let err = drive(
        &store, &source, &pacer, &ctx, &ledger, wanted, 0, 0, None, None, None, None,
    )
    .await
    .expect_err("an empty claim for a non-empty root must fail");
    assert!(
        err.downcast_ref::<crate::HashMismatch>().is_some(),
        "must surface the typed HashMismatch, got: {err:#}"
    );
    assert_eq!(
        source.opened_ranges(),
        Vec::new(),
        "nothing is pulled on the way out"
    );
}

/// The empty root IS the one hash a `total_bytes == 0` claim can carry: the
/// driver finalizes it with nothing pulled and nothing paid.
#[tokio::test]
async fn drive_accepts_the_empty_blob_under_the_empty_root() {
    let root = *blake3::hash(&[]).as_bytes();
    let store = fresh_store(root, 0);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(Vec::new()).expect("source");
    let pacer = BudgetPacer::new();
    let ctx = Arc::new(Mutex::new(healthy_ctx()));

    drive(
        &store, &source, &pacer, &ctx, &ledger, root, 0, 0, None, None, None, None,
    )
    .await
    .expect("the empty blob under the empty root completes");
    assert_eq!(source.delivered_bytes(), 0, "nothing to pull");
    assert_eq!(ledger.committed().bytes, U256::ZERO, "nothing to pay");
}

/// A store whose ingest verifies and drains every leg but keeps nothing: its
/// queries answer from an empty store, so every range stays missing after a
/// leg that returned `Ok`.
struct ForgetfulStore {
    empty: ClientRangedStore,
    sink: ClientRangedStore,
}

impl RangedStore for ForgetfulStore {
    fn total_bytes(&self) -> u64 {
        self.empty.total_bytes()
    }
    fn present_ranges(&self) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
        self.empty.present_ranges()
    }
    fn missing_ranges(
        &self,
        byte_offset: u64,
        byte_len: u64,
    ) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
        self.empty.missing_ranges(byte_offset, byte_len)
    }
    fn admit(
        &self,
        range: AlignedRange,
        bao_bytes: Bytes,
    ) -> decdn_bao_range::RangedFuture<'_, ()> {
        self.sink.admit(range, bao_bytes)
    }
    fn read(&self, byte_offset: u64, byte_len: u64) -> decdn_bao_range::RangedFuture<'_, Bytes> {
        self.empty.read(byte_offset, byte_len)
    }
    fn is_complete(&self) -> decdn_bao_range::RangedFuture<'_, bool> {
        self.empty.is_complete()
    }
    fn finalize(&self) -> decdn_bao_range::RangedFuture<'_, ()> {
        self.empty.finalize()
    }
}

impl crate::source::IngestStore for ForgetfulStore {
    fn ingest_stream<'a, R>(
        &'a self,
        range: &'a AlignedRange,
        reader: R,
        on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
        claimed_total: u64,
        stop_at: Option<&'a std::sync::atomic::AtomicU64>,
    ) -> crate::source::IngestFuture<'a, R>
    where
        R: crate::source::BaoRangeReader + 'a,
    {
        crate::source::IngestStore::ingest_stream(
            &self.sink,
            range,
            reader,
            on_progress,
            claimed_total,
            stop_at,
        )
    }

    fn flush_present_record(&self) -> crate::source::SourceFuture<'_, ()> {
        crate::source::IngestStore::flush_present_record(&self.empty)
    }
}

/// Drive `[0, total)` of `store` from `source` under a hard timeout, so a
/// drive that re-opens forever fails the test instead of hanging it.
async fn drive_bounded<St: crate::source::IngestStore>(
    store: &St,
    source: &ScriptedSource,
    ledger: &Arc<PoolLedger>,
) -> anyhow::Result<()> {
    let pacer = BudgetPacer::new();
    let ctx = Arc::new(Mutex::new(healthy_ctx()));
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        drive(
            store,
            source,
            &pacer,
            &ctx,
            ledger,
            source.root(),
            0,
            0,
            None,
            None,
            None,
            None,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("drive re-opened without end"))?
}

fn clean_leg() -> super::CleanLeg {
    super::CleanLeg {
        offset: 0,
        len: 3 * GROUP,
        paid_frontier: GROUP,
        delivered_frontier: 2 * GROUP,
        generation: 4,
    }
}

/// Either frontier moving clears a clean leg; neither moving stalls it and
/// carries the credited wire.
#[test]
fn a_clean_leg_stalls_only_when_neither_frontier_moves() {
    let leg = clean_leg();
    assert!(
        leg.stalled(2 * GROUP, 2 * GROUP, 9, 4).is_none(),
        "paid moved"
    );
    assert!(
        leg.stalled(GROUP, 3 * GROUP, 9, 4).is_none(),
        "delivered moved"
    );
    let stuck = leg.stalled(GROUP, 2 * GROUP, 9, 4).expect("neither moved");
    assert_eq!(
        (
            stuck.offset,
            stuck.len,
            stuck.paid_frontier,
            stuck.delivered_frontier
        ),
        (0, 3 * GROUP, GROUP, 2 * GROUP)
    );
    assert_eq!(stuck.paid_wire, 9);
    assert!(
        leg.stalled(0, 0, 0, 4).is_some(),
        "a frontier that fell back is no progress either"
    );
}

/// A sibling's rebase of the shared ledger during the leg voids the leg's
/// paid-frontier reading, so it cannot convict the leg.
#[test]
fn a_rebase_during_the_leg_voids_the_check() {
    assert!(clean_leg().stalled(0, 2 * GROUP, 0, 5).is_none());
}

/// #2194: a leg that streams, pays and finishes `Ok` while the store still
/// reports its range missing ends the gap with [`LegNoProgress`] after that
/// one open. It never re-opens (and re-pays) the identical range.
#[tokio::test]
async fn a_clean_leg_the_store_does_not_keep_is_never_reopened() {
    let total = 3 * GROUP;
    let (root, plaintext, _) = synth_blob(total as usize);
    let store = ForgetfulStore {
        empty: fresh_store(root, total),
        sink: fresh_store(root, total),
    };
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext)
        .expect("source")
        .paying(Arc::clone(&ledger));

    let err = drive_bounded(&store, &source, &ledger)
        .await
        .expect_err("a leg that leaves both frontiers put fails the gap");

    let stuck = err
        .downcast_ref::<LegNoProgress>()
        .unwrap_or_else(|| panic!("expected LegNoProgress, got {err:#}"));
    assert_eq!((stuck.offset, stuck.len), (0, total));
    assert!(
        stuck.paid_wire > 0,
        "the ledger paid; the store dropped the bytes"
    );
    assert_eq!(
        source.opened_ranges(),
        vec![(0, total)],
        "one open, no repeat"
    );
    assert_eq!(
        crate::classify(&err),
        crate::Fault::Source,
        "another source may still fill the gap"
    );
}

/// A store that keeps every leg it ingests except one range of the first
/// leg: that leg pays for the whole gap and leaves a hole with present bytes
/// after it. Queries answer from `view`, which holds only what was kept. A
/// `sticky` hole drops that range from every leg, so the store never keeps it.
struct HoleOnceStore {
    view: ClientRangedStore,
    sink: ClientRangedStore,
    plaintext: Vec<u8>,
    outboard: Bytes,
    hole: Mutex<Option<(u64, u64)>>,
    sticky: bool,
}

impl RangedStore for HoleOnceStore {
    fn total_bytes(&self) -> u64 {
        self.view.total_bytes()
    }
    fn present_ranges(&self) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
        self.view.present_ranges()
    }
    fn missing_ranges(
        &self,
        byte_offset: u64,
        byte_len: u64,
    ) -> decdn_bao_range::RangedFuture<'_, bao_tree::ChunkRanges> {
        self.view.missing_ranges(byte_offset, byte_len)
    }
    fn admit(
        &self,
        range: AlignedRange,
        bao_bytes: Bytes,
    ) -> decdn_bao_range::RangedFuture<'_, ()> {
        self.view.admit(range, bao_bytes)
    }
    fn read(&self, byte_offset: u64, byte_len: u64) -> decdn_bao_range::RangedFuture<'_, Bytes> {
        self.view.read(byte_offset, byte_len)
    }
    fn is_complete(&self) -> decdn_bao_range::RangedFuture<'_, bool> {
        self.view.is_complete()
    }
    fn finalize(&self) -> decdn_bao_range::RangedFuture<'_, ()> {
        self.view.finalize()
    }
}

impl crate::source::IngestStore for HoleOnceStore {
    fn ingest_stream<'a, R>(
        &'a self,
        range: &'a AlignedRange,
        reader: R,
        on_progress: Option<&'a (dyn Fn(u64) + Send + Sync)>,
        claimed_total: u64,
        stop_at: Option<&'a std::sync::atomic::AtomicU64>,
    ) -> crate::source::IngestFuture<'a, R>
    where
        R: crate::source::BaoRangeReader + 'a,
    {
        Box::pin(async move {
            let out = crate::source::IngestStore::ingest_stream(
                &self.sink,
                range,
                reader,
                on_progress,
                claimed_total,
                stop_at,
            )
            .await?;
            let (start, end) = (range.fetch_start(), range.fetch_end());
            let hole = {
                let mut hole = self.hole.lock().unwrap();
                if self.sticky { *hole } else { hole.take() }
            };
            let kept = match hole {
                Some((hole, len)) => vec![(start, hole), (hole + len, end)],
                None => vec![(start, end)],
            };
            let total = self.view.total_bytes();
            for (from, to) in kept.into_iter().filter(|(from, to)| to > from) {
                let aligned = align_range(from, to - from, total).expect("align kept");
                preadmit(&self.view, &self.plaintext, &self.outboard, &aligned).await;
            }
            Ok(out)
        })
    }

    fn flush_present_record(&self) -> crate::source::SourceFuture<'_, ()> {
        crate::source::IngestStore::flush_present_record(&self.view)
    }
}

/// #2328: a paid leg the store keeps only in part leaves a hole mid-gap with
/// present bytes after it. The next leg opens at the hole, so the gap fills
/// and the drive completes.
#[tokio::test]
async fn a_hole_behind_present_bytes_is_refilled_from_the_hole() {
    let total = 4 * GROUP;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let store = HoleOnceStore {
        view: fresh_store(root, total),
        sink: fresh_store(root, total),
        plaintext: plaintext.clone(),
        outboard,
        hole: Mutex::new(Some((GROUP, GROUP))),
        sticky: false,
    };
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext.clone())
        .expect("source")
        .paying(Arc::clone(&ledger));

    drive_bounded(&store, &source, &ledger)
        .await
        .expect("the leg after the hole fills the gap");

    assert_eq!(
        source.opened_ranges(),
        vec![(0, total), (GROUP, total - GROUP)],
        "the second leg opens at the hole"
    );
    assert!(store.view.is_complete().await.expect("complete"));
    assert_eq!(
        store.view.read(0, total).await.expect("read").as_ref(),
        plaintext.as_slice()
    );
}

/// #2328: a hole the store never keeps holds both frontiers at the hole. The
/// leg that opens there moves neither, so the gap ends with
/// [`LegNoProgress`] after two opens rather than re-paying the hole forever.
#[tokio::test]
async fn a_hole_the_store_never_keeps_ends_the_gap_after_two_opens() {
    let total = 4 * GROUP;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let store = HoleOnceStore {
        view: fresh_store(root, total),
        sink: fresh_store(root, total),
        plaintext: plaintext.clone(),
        outboard,
        hole: Mutex::new(Some((GROUP, GROUP))),
        sticky: true,
    };
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext)
        .expect("source")
        .paying(Arc::clone(&ledger));

    let err = drive_bounded(&store, &source, &ledger)
        .await
        .expect_err("a hole the store never keeps cannot complete the gap");

    let stuck = err
        .downcast_ref::<LegNoProgress>()
        .unwrap_or_else(|| panic!("expected LegNoProgress, got {err:#}"));
    assert_eq!(
        stuck.delivered_frontier, GROUP,
        "the frontier stops at the hole"
    );
    assert_eq!(
        source.opened_ranges(),
        vec![(0, total), (GROUP, total - GROUP)],
        "one open at the gap start, one at the hole, no repeat"
    );
}

/// The contiguous frontier stops at the first missing range in the gap,
/// whatever lies past it, and reaches the gap's end when nothing is missing.
#[test]
fn the_delivered_frontier_stops_at_the_first_hole() {
    let total = 8 * GROUP;
    let chunks = |from: u64, to: u64| {
        bao_tree::ChunkRanges::from(
            bao_tree::ChunkNum(from / super::CHUNK_BYTES)
                ..bao_tree::ChunkNum(to / super::CHUNK_BYTES),
        )
    };
    let frontier = |missing: &bao_tree::ChunkRanges| {
        super::contiguous_frontier(missing, total, GROUP, 7 * GROUP)
    };
    assert_eq!(frontier(&chunks(3 * GROUP, 4 * GROUP)), 3 * GROUP);
    assert_eq!(
        frontier(&(chunks(3 * GROUP, 4 * GROUP) | chunks(6 * GROUP, 7 * GROUP))),
        3 * GROUP,
        "a later hole does not move it"
    );
    assert_eq!(
        frontier(&chunks(0, 2 * GROUP)),
        GROUP,
        "clamped to the gap start"
    );
    assert_eq!(frontier(&bao_tree::ChunkRanges::empty()), 7 * GROUP);
}

/// #2194: a leg the store keeps but the ledger never pays for advances the
/// delivered frontier once. The next clean leg moves neither frontier, so the
/// gap ends with [`LegNoProgress`] after two opens rather than re-opening
/// forever.
#[tokio::test]
async fn an_unpaid_clean_leg_is_reopened_at_most_once() {
    let total = 3 * GROUP;
    let (root, plaintext, _) = synth_blob(total as usize);
    let store = fresh_store(root, total);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = ScriptedSource::new(plaintext).expect("source");

    let err = drive_bounded(&store, &source, &ledger)
        .await
        .expect_err("an unpaid leg cannot complete the gap");

    let stuck = err
        .downcast_ref::<LegNoProgress>()
        .unwrap_or_else(|| panic!("expected LegNoProgress, got {err:#}"));
    assert_eq!(
        stuck.paid_wire, 0,
        "the store kept the bytes; the ledger paid nothing"
    );
    assert_eq!(source.opened_ranges(), vec![(0, total), (0, total)]);
}

/// A source that delivers part of a range unpaid, then raises a funding
/// rejection: the credit window let it stream ahead of the voucher.
fn delivers_then_rejects_funding(plaintext: Vec<u8>, ledger: &Arc<PoolLedger>) -> ScriptedSource {
    ScriptedSource::new(plaintext)
        .expect("source")
        .paying(Arc::clone(ledger))
        .fault_once_after(3 * GROUP as usize, || {
            anyhow::Error::new(UpstreamVoucherRejected {
                reason: VoucherRejectReason::PoolExhausted,
                bundle: None,
                proof_generation: None,
            })
        })
}

/// Drive the whole blob once on `ledger`, with the lane's deposit raised.
async fn drive_whole(
    store: &ClientRangedStore,
    source: &ScriptedSource,
    ledger: &Arc<PoolLedger>,
    root: [u8; 32],
) -> anyhow::Result<()> {
    let ctx = Arc::new(Mutex::new(healthy_ctx()));
    drive(
        store,
        source,
        &BudgetPacer::new(),
        &ctx,
        ledger,
        root,
        0,
        0,
        None,
        None,
        None,
        None,
    )
    .await
}

/// A lane that a funding refusal ends keeps the span it delivered and did not
/// pay for. The next drive on the SAME lane opens at the paid frontier and
/// bills that span again, although the store holds the bytes, so the node is
/// paid for every byte it delivered.
#[tokio::test(start_paused = true)]
async fn the_same_lane_bills_its_owed_tail_after_a_funding_refusal() {
    let total = 6 * GROUP;
    let (root, plaintext, _outboard) = synth_blob(total as usize);
    let store = fresh_store(root, total);
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = delivers_then_rejects_funding(plaintext, &ledger);

    let err = drive_whole(&store, &source, &ledger, root)
        .await
        .expect_err("the funding rejection ends the lane");
    assert_eq!(crate::classify(&err), crate::Fault::Unaffordable, "{err:#}");
    let present = ranges_content_len(&store.present_ranges().await.expect("present"), total);
    assert!(present > 0, "the credit window delivered bytes unpaid");
    assert_eq!(
        ledger.committed().amount,
        U256::ZERO,
        "the lane paid nothing"
    );

    drive_whole(&store, &source, &ledger, root)
        .await
        .expect("the same lane resumes after the deposit rises");
    assert!(store.is_complete().await.expect("is_complete"));
    let opened = source.opened_ranges();
    assert_eq!(
        opened.get(1).map(|&(start, _)| start),
        Some(0),
        "the next leg opens at the paid frontier, not the delivered one: {opened:?}"
    );
    assert!(
        ledger.committed().bytes >= U256::from(total),
        "the lane paid for every byte it delivered"
    );
}

/// After a pool replacement or a key rotation the old lane is gone. A new
/// lane carries no owed span, so it resumes at the delivered frontier: the old
/// lane's unpaid span is the node's bounded credit-window loss.
#[tokio::test(start_paused = true)]
async fn a_new_lane_resumes_at_the_delivered_frontier_and_owes_nothing() {
    let total = 6 * GROUP;
    let (root, plaintext, _outboard) = synth_blob(total as usize);
    let store = fresh_store(root, total);
    let old = Arc::new(PoolLedger::new(Cumulative::default()));
    let source = delivers_then_rejects_funding(plaintext.clone(), &old);
    drive_whole(&store, &source, &old, root)
        .await
        .expect_err("the funding rejection ends the old lane");
    let missing = store.missing_ranges(0, 0).await.expect("missing");
    let delivered = contiguous_byte_ranges(&missing, total)
        .first()
        .map_or(total, |&(start, _)| start);
    assert!(delivered > 0, "the credit window delivered bytes unpaid");

    let new = Arc::new(PoolLedger::new(Cumulative::default()));
    let fresh = ScriptedSource::new(plaintext)
        .expect("source")
        .paying(Arc::clone(&new));
    drive_whole(&store, &fresh, &new, root)
        .await
        .expect("the new lane completes");
    assert_eq!(
        fresh.opened_ranges().first().map(|&(start, _)| start),
        Some(delivered),
        "the new lane opens at the delivered frontier"
    );
    assert!(
        old.take_unpaid(root)
            .first()
            .is_some_and(|&(start, _)| start == 0),
        "the old lane still records what it owes"
    );
}

/// A missing range `[a, b)` and an owed span `[b, c)` merge into one gap. A
/// drive whose gap fails before it reaches `b` keeps the whole owed span: the
/// gap's paid frontier never passed `a`.
#[tokio::test(start_paused = true)]
async fn an_owed_span_behind_a_failed_missing_range_stays_owed() {
    let total = 6 * GROUP;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let store = fresh_store(root, total);
    let (b, c) = (3 * GROUP, total);
    let held = align_range(b, c - b, total).expect("align");
    preadmit(&store, &plaintext, &outboard, &held).await;
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    ledger.note_unpaid(root, b, c);
    let source = ScriptedSource::new(plaintext)
        .expect("source")
        .paying(Arc::clone(&ledger))
        .refusing_open(0, || {
            anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::Unfunded))
        });

    drive_whole(&store, &source, &ledger, root)
        .await
        .expect_err("the open is refused");
    assert_eq!(
        ledger.take_unpaid(root),
        vec![(b, c - b)],
        "the owed span survives the failed gap"
    );
}

/// A drive the caller drops mid-gap keeps the owed span it took: the node's
/// pull leg races a drive against its cancel token, and the lane still owes
/// those bytes.
#[tokio::test(start_paused = true)]
async fn a_dropped_drive_keeps_its_owed_span() {
    let total = 6 * GROUP;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let store = fresh_store(root, total);
    let (b, c) = (3 * GROUP, total);
    let held = align_range(b, c - b, total).expect("align");
    preadmit(&store, &plaintext, &outboard, &held).await;
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    ledger.note_unpaid(root, b, c);
    // The gate never opens: the drive waits on its first read until dropped.
    let (_gate, gated) = tokio::sync::watch::channel(false);
    let source = ScriptedSource::new(plaintext)
        .expect("source")
        .paying(Arc::clone(&ledger))
        .gated_on(gated);

    tokio::select! {
        ended = drive_whole(&store, &source, &ledger, root) => {
            panic!("the gated drive ended: {ended:?}");
        }
        () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
    }
    assert_eq!(
        ledger.take_unpaid(root),
        vec![(b, c - b)],
        "the dropped drive noted its owed span back"
    );
}

/// A leg that re-delivers present bytes to bill them reports no progress for
/// them: the position counts only bytes past the delivered frontier, so the
/// funding recovery gate never takes a re-billed byte for a new one.
#[tokio::test(start_paused = true)]
async fn re_delivered_bytes_report_no_progress() {
    let total = 6 * GROUP;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let store = fresh_store(root, total);
    let owed_end = 3 * GROUP;
    let held = align_range(0, owed_end, total).expect("align");
    preadmit(&store, &plaintext, &outboard, &held).await;
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    ledger.note_unpaid(root, 0, owed_end);
    // The leg re-delivers most of the owed span, then resets.
    let source = ScriptedSource::new(plaintext)
        .expect("source")
        .paying(Arc::clone(&ledger))
        .fault_once_after(2 * GROUP as usize, || anyhow::anyhow!("connection reset"));
    let highest = std::sync::atomic::AtomicU64::new(0);
    let on_progress = |position: u64, _total: u64| {
        highest.fetch_max(position, std::sync::atomic::Ordering::Relaxed);
    };
    let ctx = Arc::new(Mutex::new(healthy_ctx()));
    drive(
        &store,
        &source,
        &BudgetPacer::new(),
        &ctx,
        &ledger,
        root,
        0,
        0,
        Some(&on_progress),
        None,
        None,
        None,
    )
    .await
    .expect_err("the leg resets");
    assert_eq!(
        highest.load(std::sync::atomic::Ordering::Relaxed),
        owed_end,
        "the re-delivered bytes moved the position past the present base"
    );
}

/// Owed spans merge into disjoint ascending ranges.
#[test]
fn owed_spans_merge_and_are_taken_once() {
    let ledger = PoolLedger::new(Cumulative::default());
    let hash = [7u8; 32];
    ledger.note_unpaid(hash, 10, 20);
    ledger.note_unpaid(hash, 15, 30);
    ledger.note_unpaid(hash, 40, 40);
    ledger.note_unpaid([8u8; 32], 0, 5);
    assert_eq!(ledger.take_unpaid(hash), vec![(10, 20)]);
    assert!(ledger.take_unpaid(hash).is_empty(), "taken once");
    assert_eq!(ledger.take_unpaid([8u8; 32]), vec![(0, 5)]);
}
