use std::sync::Arc;

use bao_tree::io::outboard::PreOrderMemOutboard;
use bytes::Bytes;
use decdn_bao_range::{IROH_BLOCK_SIZE, align_range, encode_verified_range};
use decdn_cache::{CacheEngine, FillClaim, FillError, FillSession, Hash, NodeRangedStore};
use iroh_io::{AsyncSliceReader, AsyncStreamReader};

use super::{CoherentFrameProducer, encoded_ranges, serve_end};

/// One chunk group — the alignment granularity the registry and encoder snap to.
const G: u64 = decdn_cache::CHUNK_GROUP_BYTES;

/// Deterministic pseudo-random blob (the generator the cache + admit-store tests
/// share) plus its root and pre-order outboard.
fn synth_blob(len: usize) -> ([u8; 32], Vec<u8>, Bytes) {
    let mut plaintext = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut plaintext {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    let ob = PreOrderMemOutboard::create(&plaintext, IROH_BLOCK_SIZE);
    (*ob.root.as_bytes(), plaintext, Bytes::from(ob.data))
}

/// A header-less bao-wire reader over a `Bytes` cursor (the shape
/// `admit_bao_stream` consumes; the size comes from the caller's `total_bytes`).
struct MemReader {
    wire: Bytes,
}

impl AsyncStreamReader for MemReader {
    async fn read_bytes(&mut self, len: usize) -> std::io::Result<Bytes> {
        let take = self.wire.len().min(len);
        Ok(self.wire.split_to(take))
    }

    async fn read<const L: usize>(&mut self) -> std::io::Result<[u8; L]> {
        if self.wire.len() < L {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "MemReader exhausted",
            ));
        }
        let got = self.wire.split_to(L);
        let mut out = [0u8; L];
        out.copy_from_slice(&got);
        Ok(out)
    }
}

/// The header-less verified bao wire for the aligned byte range `[off, off+len)`.
fn range_wire(
    root: [u8; 32],
    plaintext: &[u8],
    outboard: &Bytes,
    total: u64,
    off: u64,
    len: u64,
) -> (bao_tree::ChunkRanges, Bytes) {
    let aligned = align_range(off, len, total).expect("align");
    let s = aligned.fetch_start() as usize;
    let e = aligned.fetch_end() as usize;
    let combined =
        encode_verified_range(root, &aligned, &plaintext[s..e], outboard.clone()).expect("encode");
    (aligned.chunk_ranges().clone(), combined.slice(8..))
}

/// Admit one aligned range through `session` (so the cache captures its proof into
/// the per-hash outboard, exactly as the pull leg does).
async fn admit(
    engine: &CacheEngine,
    hash: Hash,
    root: [u8; 32],
    plaintext: &[u8],
    outboard: &Bytes,
    total: u64,
    off: u64,
    len: u64,
    session: &Arc<FillSession>,
) {
    let (ranges, wire) = range_wire(root, plaintext, outboard, total, off, len);
    engine
        .admit_bao_stream(hash, ranges, total, MemReader { wire }, Some(session))
        .await
        .map_err(|(_reader, e)| e)
        .expect("admit range");
}

/// `serve_end` + `encoded_ranges` (the encoder's own two-step resolution: clamp
/// the end, then align) must agree with `align_range_clamped` (the single-call
/// primitive the node's bounds gate and the client both use) over the same
/// edges `decdn_bao_range`'s own `align_range_clamped` table covers: a 1-byte
/// blob, an end exactly at the size, a start/end that both sit mid-group, an
/// end past the blob, and an offset paired with `u64::MAX`. If the two ever
/// disagreed, the encoder would serve a different span than the one the node
/// billed and the client priced.
#[test]
fn encoded_ranges_agrees_with_align_range_clamped_over_the_edges() {
    const G: u64 = decdn_cache::CHUNK_GROUP_BYTES;
    let group_total = 5 * G + 123;
    let cases: &[(u64, u64, u64)] = &[
        // A 1-byte blob, whole request.
        (0, 0, 1),
        // A 1-byte blob, an overflowing end clamps to the 1 byte.
        (0, u64::MAX, 1),
        // An end landing exactly at the size (in bounds, no clamp needed).
        (0, group_total, group_total),
        // A start and end that both sit mid-group, fully in bounds.
        (G / 2, G, group_total),
        // An end past the blob clamps to it.
        (0, group_total + G, group_total),
        // An offset paired with the widest possible overflowing end.
        (1, u64::MAX, group_total),
    ];
    for &(offset, len, total) in cases {
        let end = serve_end(offset, len, total);
        let via_encoder = encoded_ranges(offset, end, total)
            .unwrap_or_else(|e| panic!("encoded_ranges({offset}, {end}, {total}): {e}"));
        let via_clamped = decdn_bao_range::align_range_clamped(offset, len, total)
            .unwrap_or_else(|e| panic!("align_range_clamped({offset}, {len}, {total}): {e}"));
        assert_eq!(
            via_encoder,
            *via_clamped.chunk_ranges(),
            "offset={offset} len={len} total={total}: encoded_ranges disagrees with \
             align_range_clamped"
        );
    }
}

/// Drain a producer's frames to one byte vector.
async fn drain(mut producer: CoherentFrameProducer) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(frame) = producer.next_frame(1024).await? {
        out.extend_from_slice(&frame);
    }
    Ok(out)
}

/// #2328: a pull that faults leaves a hole in a range the store otherwise
/// holds, and no fill captured the held content's proof. The encode still
/// streams the held bytes before the hole, reading their proof nodes from the
/// store, less the encoded chunks still buffered (up to `ENCODE_CHANNEL_CAP`)
/// when it faults at the hole.
#[tokio::test]
async fn a_faulted_pull_still_streams_the_held_prefix() {
    let total = 32 * G;
    let hole = 24 * G;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let hash = Hash::from(root);
    let a_root = bao_tree::blake3::Hash::from(root);
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await.unwrap();
    // Content an earlier fill stored: everything but one chunk group.
    for (off, len) in [(0, hole), (hole + G, total - hole - G)] {
        let (ranges, wire) = range_wire(root, &plaintext, &outboard, total, off, len);
        engine
            .admit_bao_stream(hash, ranges, total, MemReader { wire }, None)
            .await
            .map_err(|(_reader, e)| e)
            .expect("admit held range");
    }

    let FillClaim::Owner { session, lease: _l } =
        engine.claim_fill(hash, 0, total, total, || FillSession::new(a_root, total))
    else {
        panic!("sole claimant owns its whole request");
    };
    session.mark_ended(Err(FillError::new("upstream pull died")));

    let store = NodeRangedStore::new(engine.clone(), hash, total);
    let mut producer =
        CoherentFrameProducer::new(store, Arc::clone(&session), 0, total, total).expect("align");
    let mut streamed = Vec::new();
    let err = loop {
        match producer.next_frame(1024).await {
            Ok(Some(frame)) => streamed.extend_from_slice(&frame),
            Ok(None) => panic!("a range with a hole cannot complete"),
            Err(err) => break err,
        }
    };

    let (_, whole) = range_wire(root, &plaintext, &outboard, total, 0, total);
    assert!(
        streamed.len() as u64 > hole - 4 * G,
        "most of the held prefix streams, got {} bytes",
        streamed.len()
    );
    assert_eq!(
        streamed.as_slice(),
        &whole[..streamed.len()],
        "the streamed bytes are the range's own encoding"
    );
    assert!(
        format!("{err:#}").contains("upstream pull failed before content"),
        "the encode faults at the hole: {err:#}"
    );
}

/// An encode fault is terminal, and a second call must say so rather than report
/// the range as delivered.
///
/// The distinction is the whole point: `serve_leg` reads `Ok(None)` as
/// `done_delivering` and writes `StreamEnd`, so a producer that answered a second
/// call with `None` would tell the client a truncated blob was complete — and the
/// client would pay the closing voucher for it. The cache-hit twin pins the same
/// rule in `a_mid_export_fault_propagates`.
#[tokio::test]
async fn a_coherent_encode_fault_is_terminal() {
    let total = 4 * G;
    let (root, _plaintext, _outboard) = synth_blob(total as usize);
    let hash = Hash::from(root);
    let a_root = bao_tree::blake3::Hash::from(root);
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await.unwrap();

    let FillClaim::Owner { session, lease: _l } =
        engine.claim_fill(hash, 0, total, total, || FillSession::new(a_root, total))
    else {
        panic!("sole claimant owns its whole request");
    };
    // The pull dies before admitting a byte, so no live fill covers the range and
    // the encode's first leaf read fails.
    session.mark_ended(Err(FillError::new("upstream pull died")));

    let store = NodeRangedStore::new(engine.clone(), hash, total);
    let mut producer =
        CoherentFrameProducer::new(store, Arc::clone(&session), 0, total, total).expect("align");

    let first = producer.next_frame_chunks(1024).await;
    assert!(first.is_err(), "the encode fault must surface, not park");

    let second = producer.next_frame_chunks(1024).await;
    let err = second.expect_err("a faulted producer must not answer again");
    assert!(
        err.to_string().contains("already faulted"),
        "the second call must refuse, not report the range delivered: {err}"
    );
    assert!(
        producer.queue.is_empty(),
        "a fault clears the queue and drops the queued bytes"
    );
}

/// A consumer starved on an encode parked at a LEAF the pull has not fetched
/// demands that leaf's end, so a pull whose window has closed still fetches it
/// (#1893). While encoded bytes are still buffered, the same park is look-ahead
/// and demands nothing — otherwise a client that has stopped paying could drag
/// the pull past its window. The root pair is already captured here, so only the
/// data reader parks.
#[tokio::test]
async fn a_starved_consumer_demands_the_parked_leaf_and_look_ahead_does_not() {
    let total = 2 * G;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let hash = Hash::from(root);
    let a_root = bao_tree::blake3::Hash::from(root);
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await.unwrap();

    let FillClaim::Owner { session, lease: _l } =
        engine.claim_fill(hash, 0, total, total, || FillSession::new(a_root, total))
    else {
        panic!("sole claimant owns its whole request");
    };
    // The pull has fetched the left leaf (and with it the root pair) only.
    admit(
        &engine, hash, root, &plaintext, &outboard, total, 0, G, &session,
    )
    .await;

    let store = NodeRangedStore::new(engine.clone(), hash, total);
    let mut producer =
        CoherentFrameProducer::new(store, Arc::clone(&session), 0, total, total).expect("align");

    // One small frame: the encode runs ahead into the channel (root pair, left
    // leaf) and parks on the right leaf, but the consumer still has buffered bytes.
    let first = producer
        .next_frame(1024)
        .await
        .expect("first frame")
        .expect("a first frame exists");
    assert_eq!(first.len(), 1024);
    assert_eq!(
        producer
            .parked_on
            .load(std::sync::atomic::Ordering::Acquire),
        total,
        "the encode ran ahead and parked on the right leaf"
    );
    // The left leaf's first read may park until the present-range watch yields,
    // and a consumer with nothing buffered yet publishes that — but it lies at or
    // below the pull's frontier, where a pacer ignores it. The look-ahead park on
    // the right leaf, with bytes still buffered, must not be published.
    assert!(
        session.serve_demand().get() <= G,
        "look-ahead with bytes still buffered demands nothing past the pulled leaf, \
         got {}",
        session.serve_demand().get()
    );

    let serve = tokio::spawn(drain(producer));

    let demanded = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while session.serve_demand().get() < total {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        demanded.is_ok(),
        "the parked leaf read must demand its leaf end, got {}",
        session.serve_demand().get()
    );
    assert_eq!(
        session.serve_demand().get(),
        total,
        "the demand stops at the awaited leaf's end"
    );
    assert!(
        !serve.is_finished(),
        "the encode parks until the leaf lands"
    );

    admit(
        &engine, hash, root, &plaintext, &outboard, total, G, G, &session,
    )
    .await;
    session.mark_ended(Ok(()));
    let got = serve
        .await
        .unwrap()
        .expect("serve completes once the leaf lands");
    let whole = range_wire(root, &plaintext, &outboard, total, 0, total).1;
    let mut stream = first.to_vec();
    stream.extend_from_slice(&got);
    assert_eq!(stream, whole.as_ref(), "the coherent stream is byte-exact");
}

/// A frame wider than one encoder output chunk must arrive as several `Bytes`.
/// The coherent encoder emits 64-byte proof pairs ahead of its leaves, so any
/// frame past the first proof node spans items — the zero-copy claim this path
/// rests on, and the one a coalescing rewrite would silently break.
#[tokio::test]
async fn a_frame_spanning_several_encoder_chunks_rides_uncopied() {
    let total = 2 * G;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let hash = Hash::from(root);
    let a_root = bao_tree::blake3::Hash::from(root);
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await.unwrap();

    let FillClaim::Owner { session, lease: _l } =
        engine.claim_fill(hash, 0, total, total, || FillSession::new(a_root, total))
    else {
        panic!("sole claimant owns its whole request");
    };
    admit(
        &engine, hash, root, &plaintext, &outboard, total, 0, total, &session,
    )
    .await;

    let store = NodeRangedStore::new(engine.clone(), hash, total);
    let mut producer =
        CoherentFrameProducer::new(store, Arc::clone(&session), 0, total, total).expect("align");
    let frame = producer
        .next_frame_chunks(total as usize)
        .await
        .expect("the encode runs")
        .expect("a frame");
    let chunks = frame.chunks();
    assert!(
        chunks.len() > 1,
        "a whole-range frame must stay several uncopied slices, got {}",
        chunks.len()
    );
    assert_eq!(
        chunks.iter().map(Bytes::len).sum::<usize>(),
        frame.total(),
        "the reported total counts the bytes actually handed over"
    );
}

/// A zero target must be refused by name. It cuts nothing, so `cut` below would
/// refuse it anyway — but as a `queued`/`queue` desync, blaming the bookkeeping
/// for a bad argument. `frame_target` never returns zero, so this pins the floor
/// restated where the failure would otherwise be misattributed.
#[tokio::test]
async fn a_zero_frame_target_is_refused_not_read_as_end_of_range() {
    let total = 2 * G;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let hash = Hash::from(root);
    let a_root = bao_tree::blake3::Hash::from(root);
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await.unwrap();

    let FillClaim::Owner { session, lease: _l } =
        engine.claim_fill(hash, 0, total, total, || FillSession::new(a_root, total))
    else {
        panic!("sole claimant owns its whole request");
    };
    admit(
        &engine, hash, root, &plaintext, &outboard, total, 0, total, &session,
    )
    .await;

    let store = NodeRangedStore::new(engine.clone(), hash, total);
    let mut producer =
        CoherentFrameProducer::new(store, Arc::clone(&session), 0, total, total).expect("align");
    let err = producer
        .next_frame_chunks(0)
        .await
        .expect_err("a zero target must not read as end-of-range");
    assert!(
        err.to_string().contains("zero-length frame"),
        "unexpected error: {err}"
    );
}

/// A non-empty request whose offset sits at the blob end does not align, and
/// `CoherentFrameProducer::new` fails on it at construction. That is what
/// keeps a bad request from becoming a zero-byte stream the serve loop would
/// close with `StreamEnd`.
#[tokio::test]
async fn an_unalignable_request_errors_at_construction_not_as_an_empty_range() {
    let total = 2 * G;
    let (root, _plaintext, _outboard) = synth_blob(total as usize);
    let hash = Hash::from(root);
    let a_root = bao_tree::blake3::Hash::from(root);
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await.unwrap();

    let FillClaim::Owner { session, lease: _l } =
        engine.claim_fill(hash, 0, total, total, || FillSession::new(a_root, total))
    else {
        panic!("sole claimant owns its whole request");
    };

    let store = NodeRangedStore::new(engine.clone(), hash, total);
    // An offset at the blob end cannot align: `align_range` bounds the offset
    // below the blob size.
    let Err(err) = CoherentFrameProducer::new(store, Arc::clone(&session), total, total + 1, total)
    else {
        panic!("an out-of-bounds request must error at construction");
    };
    assert!(
        err.to_string().contains("does not align"),
        "unexpected error: {err}"
    );
}

/// Two partially-overlapping serve-misses share ONE fill for the overlap: client A
/// pulls `[0,3g)`, client B (wanting `[2g,5g)`) coalesces — it opens a pull for
/// only its non-overlapping remainder `[3g,5g)` and serves the whole `[2g,5g)`,
/// reading the `[2g,3g)` overlap from A's fill (never re-pulled) and its own
/// `[3g,5g)`. B's coherent encode must AWAIT the remainder (no wedge) and produce
/// byte-identical wire to a single-pull encode of `[2g,5g)`. This is the whole
/// partial-overlap lift: without the shared per-hash outboard + registry-wide
/// termination, B's encode would fail on a node A captured or hang on A's data.
#[tokio::test]
async fn partial_overlap_shares_one_fill_and_serves_r_byte_exact() {
    let total = 8 * G;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let hash = Hash::from(root);
    let a_root = bao_tree::blake3::Hash::from(root);
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await.unwrap();

    // Client A: [0,3g) → OWNER.
    let FillClaim::Owner {
        session: a_session,
        lease: _a_lease,
    } = engine.claim_fill(hash, 0, 3 * G, total, || FillSession::new(a_root, total))
    else {
        panic!("A owns its whole request");
    };

    // A's client has paid up to 2g, so a request starting there is at (not
    // ahead of) A's paid frontier and may share A's fill for the overlap.
    a_session.advance_served(2 * G);

    // Client B: [2g,5g) overlaps A's prefix → MIXED. B owns only the remainder
    // [3g,5g) and attaches A for the [2g,3g) overlap — the "share one pull".
    let FillClaim::Mixed {
        owner: b_owner,
        attach: b_attach,
        remainder_offset,
        remainder_len,
        owner_lease: _ol,
        attach_lease: _al,
    } = engine.claim_fill(hash, 2 * G, 3 * G, total, || {
        FillSession::starting_at(a_root, total, 2 * G)
    })
    else {
        panic!("B mixes: owns the remainder, attaches the overlap sibling");
    };
    assert!(Arc::ptr_eq(&b_attach, &a_session), "B attaches A's fill");
    assert_eq!(
        (remainder_offset, remainder_len),
        (3 * G, 2 * G),
        "B opens a pull for ONLY its non-overlapping remainder"
    );

    // A fills [0,3g): the overlap [2g,3g) is now present + its proof captured.
    admit(
        &engine,
        hash,
        root,
        &plaintext,
        &outboard,
        total,
        0,
        3 * G,
        &a_session,
    )
    .await;

    // B serves the WHOLE [2g,5g) before its remainder lands: it must deliver the
    // overlap from A's fill, then PARK awaiting [3g,5g) — never wedge.
    let store = NodeRangedStore::new(engine.clone(), hash, total);
    let producer = CoherentFrameProducer::new(store, Arc::clone(&b_owner), 2 * G, 5 * G, total)
        .expect("align");
    let serve = tokio::spawn(drain(producer));
    tokio::task::yield_now().await;
    assert!(
        !serve.is_finished(),
        "B's serve must park awaiting its remainder, not complete or wedge"
    );

    // B's own remainder [3g,5g) lands; the parked encode resumes.
    admit(
        &engine,
        hash,
        root,
        &plaintext,
        &outboard,
        total,
        3 * G,
        2 * G,
        &b_owner,
    )
    .await;

    let served = tokio::time::timeout(std::time::Duration::from_secs(20), serve)
        .await
        .expect("B's serve must not hang once its remainder lands")
        .expect("serve task")
        .expect("serve produced a coherent stream");

    // Byte-identical to a single verified encode of the whole [2g,5g).
    let (_r, reference) = range_wire(root, &plaintext, &outboard, total, 2 * G, 3 * G);
    assert_eq!(
        served,
        reference.as_ref(),
        "the coalesced two-pull serve of [2g,5g) is byte-identical to a single-pull encode"
    );
}

/// The per-leaf coverage check answers from the local watch snapshot with no
/// store round trip, and folds in the takedown gate: an empty snapshot covers
/// nothing, a snapshot that includes the range covers it, and an evicted hash
/// reads as not-present — mirroring the store's own `present_ranges` guard.
#[tokio::test]
async fn covers_locally_answers_from_snapshot_and_refuses_takedown() {
    let total = 4 * G;
    let (root, _plaintext, _outboard) = synth_blob(total as usize);
    let hash = Hash::from(root);
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await.unwrap();
    let session = FillSession::new(bao_tree::blake3::Hash::from(root), total);
    let store = NodeRangedStore::new(engine.clone(), hash, total);
    let mut reader = super::AwaitingDataReader::new(store, total, session, Arc::default());

    let range = align_range(0, G, total).unwrap().chunk_ranges().clone();
    assert!(
        !reader.covers_locally(&range),
        "an empty snapshot covers nothing"
    );

    // The watch would set `present`; simulate its snapshot covering [0, G).
    reader.present = range.clone();
    assert!(
        reader.covers_locally(&range),
        "covered once the snapshot includes the range"
    );

    // A takedown makes the range read as not-present with no store hop.
    engine.evict(hash).await.unwrap();
    assert!(
        !reader.covers_locally(&range),
        "an evicted hash is refused locally, mirroring present_ranges"
    );
}

/// After a failed pull, the data reader serves a held range only while no
/// takedown gate refuses the hash: the direct store read does not apply that
/// gate, so the reader applies it first. The same held bytes read back before
/// the eviction and fail after it.
#[tokio::test]
async fn a_failed_pull_does_not_read_a_refused_hash() {
    let total = 4 * G;
    let (root, plaintext, outboard) = synth_blob(total as usize);
    let hash = Hash::from(root);
    let a_root = bao_tree::blake3::Hash::from(root);
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await.unwrap();
    let (ranges, wire) = range_wire(root, &plaintext, &outboard, total, 0, total);
    engine
        .admit_bao_stream(hash, ranges, total, MemReader { wire }, None)
        .await
        .map_err(|(_reader, e)| e)
        .expect("admit the whole blob");

    let FillClaim::Owner { session, lease: _l } =
        engine.claim_fill(hash, 0, total, total, || FillSession::new(a_root, total))
    else {
        panic!("sole claimant owns its whole request");
    };
    session.mark_ended(Err(FillError::new("upstream pull died")));

    let store = NodeRangedStore::new(engine.clone(), hash, total);
    let mut reader = super::AwaitingDataReader::new(store, total, session, Arc::default());
    let held = reader
        .read_at(0, G as usize)
        .await
        .expect("held bytes read");
    assert_eq!(held.as_ref(), &plaintext[..G as usize]);

    engine.evict(hash).await.unwrap();
    let err = reader
        .read_at(0, G as usize)
        .await
        .expect_err("a refused hash must not be served");
    assert!(
        err.to_string().contains("the hash is refused"),
        "the read fails on the takedown gate: {err}"
    );
}

/// The 0-byte blob encodes to an empty stream: no frame, no fault. Its request
/// range is empty, and bao-tree's async encoder trips a debug assertion on
/// empty ranges, so the producer must skip the encode rather than walk them.
#[tokio::test]
async fn the_empty_blob_encodes_an_empty_stream() {
    let (root, _plaintext, _outboard) = synth_blob(0);
    let hash = Hash::from(root);
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(tmp.path(), vec![], 64).await.unwrap();
    let session = FillSession::new(bao_tree::blake3::Hash::from(root), 0);
    let store = NodeRangedStore::new(engine, hash, 0);
    let producer = CoherentFrameProducer::new(store, session, 0, 0, 0).unwrap();

    let served = tokio::time::timeout(std::time::Duration::from_secs(5), drain(producer))
        .await
        .expect("the empty encode ends without waiting on a pull")
        .expect("the empty encode does not fault");
    assert!(served.is_empty(), "the 0-byte blob has an empty wire");
}
