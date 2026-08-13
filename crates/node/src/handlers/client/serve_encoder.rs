//! The coherent whole-range bao encoder that produces the decoupled serve leg's
//! downstream wire (ADR 038).
//!
//! A SINGLE [`bao_tree::io::fsm::encode_ranges_validated`] walks the requested
//! range in pre-order and emits ONE coherent verified stream — byte-identical to a
//! whole encode — while the pull leg fills the cache incrementally beside it:
//!
//! - leaf DATA comes from [`AwaitingDataReader`], which blocks on the store's
//!   present-range watch until the leaf's content lands (racing the pull's terminal
//!   signal for the no-hang guarantee), then reads it;
//! - the proof `(left, right)` hash pairs come from the serve leg's shared
//!   [`decdn_cache::SessionOutboardReader`], fed by the pull leg's capture;
//! - the encoded bytes are pushed through a bounded channel ([`ChannelWriter`]) and
//!   re-cut into `CHUNK_SIZE` `cdn/client/v1` frames by [`CoherentFrameProducer`].
//!
//! The encode future and the frame consumer run CONCURRENTLY on the one serve task
//! (the bounded channel backpressures the encoder), so `CoherentFrameProducer`
//! exposes a `next_frame()` frame-pull interface to the serve loop. Everything here
//! is `Send` (the iroh accept bound): the cache streams are `Send`, held only behind
//! `&mut self`.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bao_tree::ChunkRanges;
use bao_tree::io::fsm::encode_ranges_validated;
use bytes::{Bytes, BytesMut};
use decdn_bao_range::{RangedStore, align_range};
use decdn_cache::{FillSession, NodeRangedStore, PresentRangeWatch, ServeStore};
use decdn_protocol::CHUNK_SIZE;
use futures_util::StreamExt;
use iroh_io::{AsyncSliceReader, AsyncStreamWriter};
use tokio::sync::{Notify, mpsc};

/// How long the data reader polls for the store to MATERIALIZE the blob (admit its
/// first chunk group) before a present-range watch can be opened.
const WATCH_OPEN_RETRY: Duration = Duration::from_millis(25);

/// Ceiling on the wait for the observed bitfield to reflect a range that a CLEAN
/// pull (`outcome() == Ok`) already admitted. A clean outcome GUARANTEES the range
/// is in the store, so a still-missing read is only the store actor's observed
/// bitfield lagging the admit completion — a lag that resolves in milliseconds under
/// any scheduler speed. The wait is therefore timing-INDEPENDENT: it blocks until the
/// range is observed present, never aborting the serve on a scheduling hiccup. This
/// ceiling is deliberately generous (seconds, not milliseconds) and is expected to be
/// unreachable after a clean pull; it exists only so a pathological permanent lag
/// (e.g. the hash evicted mid-serve) surfaces an error instead of hanging forever.
const CLEAN_PULL_PRESENT_CEILING: Duration = Duration::from_secs(15);

/// Bounded backpressure between the encoder and the frame consumer: the encoder
/// parks once this many encoded chunks are buffered ahead of delivery, so the
/// upstream-paced pull is never outrun by an unbounded local encode.
const ENCODE_CHANNEL_CAP: usize = 8;

/// An [`AsyncSliceReader`] over the cache that AWAITS the pull leg filling a leaf's
/// content before reading it — the data axis of the coherent encode. Owns a
/// [`NodeRangedStore`] (a cheap `CacheEngine` handle), so the encode future can take
/// it by value and stay `'static` + `Send`.
struct AwaitingDataReader {
    store: NodeRangedStore,
    total: u64,
    /// Live present-range watch, opened lazily once the blob materializes.
    watch: Option<PresentRangeWatch>,
    /// The shared fill session: [`FillSession::range_still_live`] races the
    /// present-range watch so a range no live pull will fill fails the read rather
    /// than hanging. Under partial-overlap coalescing a leaf may be filled by a
    /// sibling pull (of the same hash), so termination consults ALL live fills, not
    /// just this session's own terminal signal.
    session: Arc<FillSession>,
    /// The per-hash liveness signal, snapshot at construction: notified whenever any
    /// fill of this hash ends or is cancelled, so a parked read re-checks liveness.
    liveness: Arc<Notify>,
}

impl AwaitingDataReader {
    fn new(store: NodeRangedStore, total: u64, session: Arc<FillSession>) -> Self {
        let liveness = session.liveness_signal();
        Self {
            store,
            total,
            watch: None,
            session,
            liveness,
        }
    }

    /// Does the store currently hold the whole byte range `[offset, offset + len)`?
    ///
    /// Takes `&mut self` (though it mutates nothing) so the future holds a
    /// `&mut AwaitingDataReader` rather than `&AwaitingDataReader` across the store
    /// await: the reader carries a `Send`-but-not-`Sync` [`PresentRangeWatch`], so
    /// `&AwaitingDataReader` is not `Send` and would make the whole serve future
    /// non-`Send` — which the iroh `ProtocolHandler::accept` bound forbids.
    async fn present_covers(&mut self, offset: u64, len: u64) -> io::Result<bool> {
        let missing = self
            .store
            .missing_ranges(offset, len)
            .await
            .map_err(io::Error::other)?;
        Ok(missing.is_empty())
    }

    /// Wait for the observed bitfield to report `[offset, offset + need)` present,
    /// after the covering pull ended CLEANLY. A clean outcome guarantees the range
    /// was admitted to the store, so a not-yet-present read is only the store actor's
    /// observed bitfield lagging the admit completion — not a missing byte. Awaiting
    /// the present-range watch (re-opening it when it closes on completion) resolves
    /// that lag under ANY scheduler speed, so the serve never aborts on a scheduling
    /// hiccup. Bounded by [`CLEAN_PULL_PRESENT_CEILING`], a generous ceiling a clean
    /// pull is never expected to reach; it exists only so a pathological permanent lag
    /// (e.g. the hash evicted mid-serve) surfaces an error instead of hanging.
    async fn await_present_after_clean_pull(&mut self, offset: u64, need: u64) -> io::Result<()> {
        let deadline = tokio::time::Instant::now() + CLEAN_PULL_PRESENT_CEILING;
        loop {
            if self.present_covers(offset, need).await? {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(io::Error::other(format!(
                    "clean pull did not surface content [{offset}, +{need}) within \
                     {CLEAN_PULL_PRESENT_CEILING:?}"
                )));
            }
            // Ensure a watch is open (it errors only until the blob materializes,
            // which a clean pull guarantees), then await its next advance — capped by
            // `WATCH_OPEN_RETRY` so a watch that already closed on completion is
            // re-opened and re-checked promptly, and the ceiling is honored even
            // without a fresh advance.
            if self.watch.is_none() {
                match self.store.observe().await {
                    Ok(w) => self.watch = Some(w),
                    Err(_not_materialized) => {
                        tokio::time::sleep(WATCH_OPEN_RETRY).await;
                        continue;
                    }
                }
            }
            let advanced = async {
                match self.watch.as_mut() {
                    Some(w) => w.next().await.map(|_ranges| ()),
                    None => None,
                }
            };
            match tokio::time::timeout(WATCH_OPEN_RETRY, advanced).await {
                Ok(Some(())) => {}             // advanced — re-check presence
                Ok(None) => self.watch = None, // watch closed on completion — re-open
                Err(_elapsed) => {}            // periodic re-check even without an advance
            }
        }
    }
}

impl AsyncSliceReader for AwaitingDataReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let need = len as u64;
        // The chunk range this read needs, for the "any live fill still covers it?"
        // termination check. An align error (never for an in-bounds encoder read)
        // yields an empty range, which reads as "no live coverer" and fails cleanly
        // rather than hanging.
        let range = align_range(offset, need, self.total)
            .map_or_else(|_| ChunkRanges::empty(), |a| a.chunk_ranges().clone());
        loop {
            if self.present_covers(offset, need).await? {
                break;
            }

            // Register the liveness waiter BEFORE re-inspecting shared state, so a
            // terminal outcome recorded concurrently cannot slip past the check. Clone
            // the `Arc` to a local so the waiter borrows it, not `self` — leaving
            // `&mut self` free for the present-range checks below.
            let liveness = Arc::clone(&self.liveness);
            let mut ended = Box::pin(liveness.notified());
            ended.as_mut().enable();

            // No live fill still covers this range. Decide on the terminal outcome:
            if !self.session.range_still_live(&range) {
                match self.session.outcome() {
                    // A clean pull admitted every byte of its range before it marked
                    // ended, so the range is authoritatively in the store. If the
                    // observed bitfield does not YET reflect the admit, that is the
                    // store actor's watch lagging the admit completion — a visibility
                    // lag, never a missing byte. WAIT for the observed range to appear
                    // rather than aborting the serve; this resolves under ANY scheduler
                    // speed (timing-independent), bounded only by a generous ceiling a
                    // clean pull is never expected to reach.
                    Some(Ok(())) => {
                        self.await_present_after_clean_pull(offset, need).await?;
                        break;
                    }
                    // A failed pull can never make the byte present — fail fast.
                    Some(Err(msg)) => {
                        return Err(io::Error::other(format!(
                            "upstream pull failed before content [{offset}, +{len}) landed: {msg}"
                        )));
                    }
                    // No recorded outcome, yet nothing live covers the range:
                    // `range_still_live` and `outcome` are read separately, so a fill
                    // can retire between them. A fresh present check settles that benign
                    // gap before declaring no coverer.
                    None => {
                        if self.present_covers(offset, need).await? {
                            break;
                        }
                        return Err(io::Error::other(format!(
                            "no live fill covers content [{offset}, +{len}); every covering pull ended"
                        )));
                    }
                }
            }

            // Ensure a watch is open; it errors until the blob materializes —
            // tolerate that with a bounded poll racing the liveness signal, then retry.
            if self.watch.is_none() {
                match self.store.observe().await {
                    Ok(w) => self.watch = Some(w),
                    Err(_not_materialized) => {
                        tokio::select! {
                            biased;
                            () = ended.as_mut() => {}
                            () = tokio::time::sleep(WATCH_OPEN_RETRY) => {}
                        }
                        continue;
                    }
                }
            }

            // Await the next present-range advance vs the pull ending, then re-check.
            let advanced = async {
                match self.watch.as_mut() {
                    Some(w) => w.next().await.map(|_ranges| ()),
                    None => None,
                }
            };
            tokio::select! {
                biased;
                () = ended.as_mut() => {}
                closed = advanced => {
                    if closed.is_none() {
                        self.watch = None; // watch stream ended; re-open next pass
                    }
                }
            }
        }

        self.store
            .read(offset, need)
            .await
            .map_err(io::Error::other)
    }

    async fn size(&mut self) -> io::Result<u64> {
        Ok(self.total)
    }
}

/// An [`AsyncStreamWriter`] that forwards the encoder's sequential output into a
/// bounded channel the frame consumer drains. A closed receiver (the serve aborted)
/// surfaces as a write error, ending the encode.
struct ChannelWriter {
    tx: mpsc::Sender<Bytes>,
}

impl AsyncStreamWriter for ChannelWriter {
    async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.write_bytes(Bytes::copy_from_slice(data)).await
    }

    async fn write_bytes(&mut self, data: Bytes) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.tx
            .send(data)
            .await
            .map_err(|_| io::Error::other("serve encode: downstream frame channel closed"))
    }

    async fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Drives the coherent whole-range encode and re-cuts its output into `CHUNK_SIZE`
/// frames. [`Self::next_frame`] yields the next
/// wire frame, `None` once the whole range is delivered, `Err` on an encode fault
/// (a gap the pull could not fill, or a proof/verify error) — on which the serve
/// leg must not send `StreamEnd`.
pub(super) struct CoherentFrameProducer {
    /// The running encode future, taken out while polled and re-stored if it parks.
    /// `None` once it has completed (its channel sender is then dropped, so the
    /// receiver drains and ends).
    enc: Option<Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>>,
    rx: mpsc::Receiver<Bytes>,
    frame_buf: BytesMut,
}

impl CoherentFrameProducer {
    /// Build the producer for request range `[offset, end)` of `hash` (a
    /// `total`-byte blob), reading data from `store` (awaiting the pull) and proof
    /// nodes from a [`decdn_cache::SessionOutboardReader`] minted from the shared `session`.
    pub(super) fn new(
        store: NodeRangedStore,
        session: Arc<FillSession>,
        offset: u64,
        end: u64,
        total: u64,
    ) -> Self {
        // The chunk-group-aligned ranges the client's verified stream covers (ADR
        // 038). `end == offset` (empty request) yields empty ranges — an empty
        // stream — handled naturally by the encoder.
        let ranges = if end > offset {
            align_range(offset, end - offset, total)
                .map_or_else(|_| ChunkRanges::empty(), |a| a.chunk_ranges().clone())
        } else {
            ChunkRanges::empty()
        };

        let outboard = session.outboard_reader();
        let data = AwaitingDataReader::new(store, total, session);
        let (tx, rx) = mpsc::channel(ENCODE_CHANNEL_CAP);
        let writer = ChannelWriter { tx };

        let enc: Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> = Box::pin(async move {
            let mut writer = writer;
            let mut data = data;
            let mut outboard = outboard;
            encode_ranges_validated(&mut data, &mut outboard, ranges.as_ref(), &mut writer)
                .await
                .map_err(|e| anyhow::anyhow!("coherent range encode failed: {e}"))
        });

        Self {
            enc: Some(enc),
            rx,
            frame_buf: BytesMut::new(),
        }
    }

    /// The next `CHUNK_SIZE` wire frame (or the shorter final remainder), `None`
    /// once the whole range is delivered.
    pub(super) async fn next_frame(&mut self) -> anyhow::Result<Option<Bytes>> {
        loop {
            if self.frame_buf.len() >= CHUNK_SIZE {
                let take = self.frame_buf.len().min(CHUNK_SIZE);
                return Ok(Some(self.frame_buf.split_to(take).freeze()));
            }
            if let Some(bytes) = self.pump().await? {
                self.frame_buf.extend_from_slice(&bytes);
            } else {
                // Encoder finished and channel drained: flush any final partial
                // frame, then signal completion.
                if self.frame_buf.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(self.frame_buf.split().freeze()));
            }
        }
    }

    /// Advance the encode and/or receive its next output chunk. `Some(bytes)` when a
    /// chunk arrives; `None` once the encoder is done AND the channel is drained; an
    /// encode fault propagates as `Err`.
    async fn pump(&mut self) -> anyhow::Result<Option<Bytes>> {
        loop {
            match self.enc.take() {
                Some(mut fut) => {
                    tokio::select! {
                        biased;
                        res = fut.as_mut() => {
                            // Encoder finished: do NOT re-store `fut`, so its channel
                            // sender drops and the receiver will drain then end.
                            res?;
                        }
                        recv = self.rx.recv() => {
                            self.enc = Some(fut); // still encoding — keep the future
                            return Ok(recv);
                        }
                    }
                }
                None => return Ok(self.rx.recv().await),
            }
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::too_many_arguments
)] // tests
mod tests {
    use std::sync::Arc;

    use bao_tree::io::outboard::PreOrderMemOutboard;
    use bytes::Bytes;
    use decdn_bao_range::{IROH_BLOCK_SIZE, align_range, encode_verified_range};
    use decdn_cache::{CacheEngine, FillClaim, FillSession, Hash, NodeRangedStore};
    use iroh_io::AsyncStreamReader;

    use super::CoherentFrameProducer;

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
        let combined = encode_verified_range(root, &aligned, &plaintext[s..e], outboard.clone())
            .expect("encode");
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
            .expect("admit range");
    }

    /// Drain a producer's frames to one byte vector.
    async fn drain(mut producer: CoherentFrameProducer) -> anyhow::Result<Vec<u8>> {
        let mut out = Vec::new();
        while let Some(frame) = producer.next_frame().await? {
            out.extend_from_slice(&frame);
        }
        Ok(out)
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
            FillSession::new(a_root, total)
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
        let producer = CoherentFrameProducer::new(store, Arc::clone(&b_owner), 2 * G, 5 * G, total);
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
}
