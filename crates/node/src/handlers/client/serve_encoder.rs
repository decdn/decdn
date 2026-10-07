//! The coherent whole-range bao encoder that produces the decoupled serve leg's
//! downstream wire (ADR 038).
//!
//! A SINGLE [`bao_tree::io::fsm::encode_ranges_validated`] walks the requested
//! range in pre-order and emits ONE coherent verified stream — byte-identical to a
//! whole encode — while the pull leg fills the cache incrementally beside it:
//!
//! - leaf DATA comes from [`AwaitingDataReader`], which blocks on the store's
//!   present-range watch until the leaf's content lands (racing the pull's terminal
//!   signal for the no-hang guarantee), then reads it; once the covering pull ends
//!   cleanly the range is durably stored, so it reads straight from the store rather
//!   than waiting on the observed bitfield, which can lag the admit;
//! - the proof `(left, right)` hash pairs come from the serve leg's shared
//!   [`decdn_cache::SessionOutboardReader`], fed by the pull leg's capture, and
//!   from the store's outboard once no live fill covers a node;
//! - the encoded bytes are pushed through a bounded channel ([`ChannelWriter`]) and
//!   cut into `cdn/client/v1` frames of a caller-chosen target size by
//!   [`CoherentFrameProducer`].
//!
//! The encode future and the frame consumer run CONCURRENTLY on the one serve task
//! (the bounded channel backpressures the encoder), so `CoherentFrameProducer`
//! exposes a `next_frame_chunks()` frame-pull interface to the serve loop. Everything here
//! is `Send` (the iroh accept bound): the cache streams are `Send`, held only behind
//! `&mut self`.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::task::Poll;
use std::time::Duration;

use bao_tree::ChunkRanges;
use bao_tree::io::fsm::encode_ranges_validated;
use bytes::Bytes;
use decdn_bao_range::{RangedStore, align_range};
use decdn_cache::{DemandSlot, FillSession, NodeRangedStore, PresentRangeWatch, ServeStore};
use futures_util::StreamExt;
use iroh_io::{AsyncSliceReader, AsyncStreamWriter};
use tokio::sync::{Notify, mpsc};

use super::wire::{FrameAccountingFault, FrameChunks, FrameQueue};

/// How long the data reader polls for the store to MATERIALIZE the blob (admit its
/// first chunk group) before a present-range watch can be opened.
const WATCH_OPEN_RETRY: Duration = Duration::from_millis(25);

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
    /// present-range watch so a read of a range no live pull will fill settles
    /// rather than hanging: served from the store when it holds the bytes, failed
    /// otherwise. Under partial-overlap coalescing a leaf may be filled by a
    /// sibling pull (of the same hash), so termination consults ALL live fills, not
    /// just this session's own terminal signal.
    session: Arc<FillSession>,
    /// The per-hash liveness signal, snapshot at construction: notified whenever any
    /// fill of this hash ends or is cancelled, so a parked read re-checks liveness.
    liveness: Arc<Notify>,
    /// The blob's present chunk ranges as last reported by `watch` — the absolute
    /// bitfield each watch item carries, not a delta. A read answers coverage
    /// against this local snapshot instead of a per-leaf `missing_ranges` store
    /// round trip; the watch only advances as the pull fills, so this grows
    /// monotonically and the encoder's forward walk reads each landed leaf with no
    /// store hop. Empty until the watch yields its first snapshot.
    present: ChunkRanges,
    /// The content end this reader waits on, written before each park into the cell
    /// it shares with the encode's outboard reader. [`CoherentFrameProducer`]
    /// stands it as serve demand only while its consumer is starved.
    parked_on: Arc<AtomicU64>,
}

impl AwaitingDataReader {
    fn new(
        store: NodeRangedStore,
        total: u64,
        session: Arc<FillSession>,
        parked_on: Arc<AtomicU64>,
    ) -> Self {
        let liveness = session.liveness_signal();
        Self {
            store,
            total,
            watch: None,
            session,
            liveness,
            present: ChunkRanges::empty(),
            parked_on,
        }
    }

    /// Does the local present-range snapshot already cover chunk `range`, and is the
    /// hash not refused? Pure in-memory: the coverage test is a `ChunkRanges`
    /// subtraction and [`NodeRangedStore::refuses`] reads only in-memory sets, so
    /// this is the zero-store-hop fast path a served leaf takes once the pull has
    /// filled it. Refusing an evicted/blacklisted hash here mirrors the guard the
    /// store's own `present_ranges` applies, so a mid-fill takedown reads as
    /// not-present exactly as before — just without the store round trip.
    fn covers_locally(&self, range: &ChunkRanges) -> bool {
        !self.store.refuses() && (range.clone() - &self.present).is_empty()
    }

    /// Authoritative store-backed presence probe for `[offset, offset + len)`, used
    /// only once no live fill covers the read and no outcome is recorded: it
    /// settles the race where a fill retires between the liveness and outcome
    /// reads. Never on the per-leaf hot path, which answers from
    /// [`Self::covers_locally`].
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
}

impl AsyncSliceReader for AwaitingDataReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let need = len as u64;
        // The chunk range this read needs, for the "any live fill still covers it?"
        // termination check. An in-bounds encoder read always aligns, so an align
        // error here is a node bug and is reported as one — the termination check
        // reads only ranges it can trust, and never attributes an alignment fault
        // to the pull leg.
        let range = align_range(offset, need, self.total)
            .map(|a| a.chunk_ranges().clone())
            .map_err(|e| {
                io::Error::other(format!(
                    "serve read [{offset}, +{need}) does not align: {e}"
                ))
            })?;
        loop {
            // Fast path: answer coverage from the watch's last snapshot, no store
            // hop. Empty until the watch yields, so the first read for a leaf falls
            // through to open the watch and await its first item below.
            if self.covers_locally(&range) {
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
                    // A clean pull admitted every byte of its range to the store
                    // BEFORE it marked ended, so the bytes are durably stored and the
                    // authoritative ranged read below returns them. Read them straight
                    // from the store rather than re-gating on the observed present
                    // bitfield: that bitfield is served through the store actor's
                    // `observe`, which can lag the durable admit by a store-actor hop
                    // when the actor is starved of CPU (a slow, coverage-instrumented
                    // run). Gating on it — as a wall-clock-bounded present-poll did —
                    // aborts a serve whose bytes are in fact readable the moment the
                    // lag exceeds the ceiling. The durable ranged read has no such lag,
                    // so a clean outcome is proof the range is readable: break to it.
                    // Timing-INDEPENDENT — the decision rests on the outcome, never a
                    // wall clock.
                    Some(Ok(())) => break,
                    // A failed pull fills no more of the range, but it can fail
                    // after it landed the bytes this read needs (a fault later in
                    // the range, or on a leg past this read). A takedown gate
                    // refuses the hash first: the ranged read below does not apply
                    // the deny/blacklist/evict/quarantine guard that
                    // `present_ranges` applies, so this arm checks it itself.
                    // Otherwise the arm attempts the authoritative ranged read
                    // directly rather than gating it on the observed present
                    // bitfield, which can lag a durable admit (see the clean arm
                    // above). The read checks the store's own current bitfield and
                    // fails with "missing range" for any absent byte, so it returns
                    // only bytes the store durably holds and never fills a gap.
                    Some(Err(msg)) => {
                        if self.store.refuses() {
                            return Err(io::Error::other(format!(
                                "upstream pull failed before content [{offset}, +{len}) \
                                 landed: {msg}; the hash is refused"
                            )));
                        }
                        return self.store.read(offset, need).await.map_err(|err| {
                            io::Error::other(format!(
                                "upstream pull failed before content [{offset}, +{len}) \
                                 landed: {msg}; the store read failed: {}",
                                decdn_cache::ErrorChain::new(&err)
                            ))
                        });
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

            // A live fill still covers the span and it is not here yet, so this read
            // is about to wait on a pull. Record what it waits on; the frame consumer
            // turns it into serve demand if it starves on this park.
            self.parked_on.store(
                offset.saturating_add(need),
                std::sync::atomic::Ordering::Relaxed,
            );

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

            // Await the next present-range advance vs the pull ending. Each watch
            // item is the blob's absolute present ranges; fold it into the local
            // snapshot so the next loop answers coverage without a store hop.
            let advanced = async {
                match self.watch.as_mut() {
                    Some(w) => w.next().await,
                    None => None,
                }
            };
            tokio::select! {
                biased;
                () = ended.as_mut() => {}
                yielded = advanced => {
                    match yielded {
                        Some(ranges) => self.present = ranges,
                        None => self.watch = None, // watch stream ended; re-open next pass
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

/// Drives the coherent whole-range encode and cuts its output into wire
/// frames. [`Self::next_frame_chunks`] yields the next wire frame, `None` once
/// the whole range is delivered, `Err` on an encode fault (a gap the pull could
/// not fill, or a proof/verify error) — on which the serve leg must not send
/// `StreamEnd`.
pub(super) struct CoherentFrameProducer {
    /// The running encode future, taken out while polled and re-stored if it parks.
    /// `None` once it has completed (its channel sender is then dropped, so the
    /// receiver drains and ends).
    enc: Option<Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>>,
    rx: mpsc::Receiver<Bytes>,
    /// Queue of encoded bytes not yet cut into a frame, kept `Bytes`-native for a
    /// vectored QUIC write without copying payload bytes. [`FrameQueue`] holds its
    /// byte count in step with the chunks.
    queue: FrameQueue,
    /// The encode faulted. Terminal: the queue is cleared and no further frame is
    /// ever cut — not because the queued bytes are suspect, but because the delivery
    /// is being abandoned, so cutting another frame would bill the client for a
    /// transfer that can never complete. The cache-hit path holds the same rule.
    faulted: bool,
    /// The blob being served, for the fault log. An encode fault is a node-side
    /// failure the client only ever sees as a short delivery, so this line is the
    /// operator's only signal — the cache-hit framer carries the same field for the
    /// same reason.
    hash: decdn_cache::Hash,
    /// This serve leg's demand on the live fills of its blob. It stands while the
    /// consumer is starved on a park, and is withdrawn once encoded bytes flow again
    /// or the producer drops.
    demand: DemandSlot,
    /// The content end the encode's data or outboard reader last parked on, shared
    /// with both readers.
    parked_on: Arc<AtomicU64>,
    /// The demand this leg has standing, `0` when none, so a starved consumer that
    /// wakes again without progress does not stand it again.
    published: u64,
}

/// The content end of a serve for `[offset, +len)` of a `total`-byte blob:
/// `len == 0` reads to the blob end, and a longer `len` clamps to it.
pub(super) fn serve_end(offset: u64, len: u64, total: u64) -> u64 {
    if len == 0 {
        total
    } else {
        offset.saturating_add(len).min(total)
    }
}

/// The chunk-group-aligned ranges the coherent encoder walks for the content span
/// `[offset, end)` of a `total`-byte blob (ADR 038): the ranges the client's
/// verified stream covers. A span with no bytes (`end <= offset`) has empty
/// ranges.
///
/// # Errors
///
/// A non-empty span that does not align: `offset` at or past the blob end, or an
/// `end` that overflows or exceeds `total`.
pub(super) fn encoded_ranges(offset: u64, end: u64, total: u64) -> anyhow::Result<ChunkRanges> {
    if end <= offset {
        return Ok(ChunkRanges::empty());
    }
    align_range(offset, end - offset, total)
        .map(|a| a.chunk_ranges().clone())
        .map_err(|e| anyhow::anyhow!("serve range [{offset}, {end}) does not align: {e}"))
}

/// The encoder's failure as a typed chain. An I/O failure comes from the data
/// reader, the outboard reader or the frame channel under the encode, and keeps its
/// own chain: for a store read, down to a [`decdn_cache::CacheError`].
/// `EncodeError`'s `Display` is its `Debug`, which would dump that chain as one
/// nested `Debug` value instead of one cause per link.
fn encode_error(err: bao_tree::io::EncodeError) -> anyhow::Error {
    match err {
        bao_tree::io::EncodeError::Io(io) => anyhow::Error::new(io),
        other => anyhow::Error::new(other),
    }
}

/// One step of [`CoherentFrameProducer::pump`]: the encode finished (or faulted), or
/// the channel yielded an encoded chunk (`None` if it closed).
enum PumpStep {
    Finished(anyhow::Result<()>),
    Item(Option<Bytes>),
}

impl CoherentFrameProducer {
    /// Build the producer for request range `[offset, end)` of `hash` (a
    /// `total`-byte blob), reading data from `store` (awaiting the pull) and proof
    /// nodes from a [`decdn_cache::SessionOutboardReader`] minted from the shared `session`.
    ///
    /// # Errors
    ///
    /// A non-empty request that does not align — `offset` at or past the blob end,
    /// or an `end` that overflows or exceeds `total` — is a bad request and fails
    /// here, so the encoder never emits a zero-byte stream that the serve loop
    /// would then close with `StreamEnd` as if a non-empty range had been
    /// delivered in full. The legitimate `end == offset` empty request is handled
    /// separately below and never errors, as does `end < offset`.
    ///
    /// `serve_leg` refuses an out-of-bounds offset before it gets here, and
    /// `dispatch`'s bounds gate refuses one with `RangeNotSatisfiable` before it
    /// signs the response, so on the serve path this is a backstop rather than the
    /// range gate.
    pub(super) fn new(
        store: NodeRangedStore,
        session: Arc<FillSession>,
        offset: u64,
        end: u64,
        total: u64,
    ) -> anyhow::Result<Self> {
        // `end == offset` (empty request) yields empty ranges — an empty stream:
        // the encode future below skips the encoder and ends at once.
        let ranges = encoded_ranges(offset, end, total)?;

        // A proof node no live fill will capture is read from the local store,
        // so a pull that faults mid-range still lets the encode stream the held
        // bytes before the hole, less the encoded chunks still buffered (up to
        // `ENCODE_CHANNEL_CAP`) when the encode faults.
        let outboard = session
            .outboard_reader()
            .with_store_fallback(store.engine().clone());
        let parked_on = outboard.parked_on();
        let demand = session.demand_slot();
        let hash = store.hash();
        let data = AwaitingDataReader::new(store, total, session, Arc::clone(&parked_on));
        let (tx, rx) = mpsc::channel(ENCODE_CHANNEL_CAP);
        let writer = ChannelWriter { tx };

        let enc: Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> = Box::pin(async move {
            let mut writer = writer;
            let mut data = data;
            let mut outboard = outboard;
            // The empty range (the 0-byte blob) has an empty wire. The async
            // encoder, unlike the sync one, does not short-circuit empty ranges
            // and trips a debug assertion walking them, so skip it. `writer`
            // still drops here, so the frame channel closes and the stream ends.
            if ranges.is_empty() {
                return Ok(());
            }
            encode_ranges_validated(&mut data, &mut outboard, ranges.as_ref(), &mut writer)
                .await
                .map_err(|e| encode_error(e).context("coherent range encode failed"))
        });

        Ok(Self {
            enc: Some(enc),
            rx,
            queue: FrameQueue::new(),
            faulted: false,
            hash,
            demand,
            parked_on,
            published: 0,
        })
    }

    /// The next wire frame as a [`FrameChunks`], totalling up to `target` bytes (or
    /// the shorter final remainder), then `None` once the whole range is delivered.
    ///
    /// Zero-copy: the frame holds reference-counted slices of the encoder's output,
    /// so the caller can send them via a single vectored QUIC write without copying
    /// payload bytes. [`FrameChunks::total`] is their total byte count, which is both
    /// what the frame header declares to the client and what the serve leg bills for.
    ///
    /// # Errors
    ///
    /// An encode fault (a gap the pull could not fill, or a proof/verify error), a
    /// `target` of zero, or any call after a fault. A fault is terminal — the queue
    /// is dropped and later calls error rather than answering `None`, because `None`
    /// is how the serve leg learns the range is complete. On any of these the serve
    /// leg must not send `StreamEnd`.
    pub(super) async fn next_frame_chunks(
        &mut self,
        target: usize,
    ) -> anyhow::Result<Option<FrameChunks>> {
        if self.faulted {
            anyhow::bail!("coherent encode already faulted; refusing to serve further frames");
        }
        // A zero target can never cut a frame: it short-circuits below before the
        // encoder is pumped even once. `frame_target` never returns zero: every term
        // it minimizes over is at least one, and the room term floors at a bao chunk
        // group. This restates that floor where the failure would otherwise be silent.
        if target == 0 {
            tracing::error!(hash = %self.hash, "serve leg asked for a zero-length frame");
            return Err(anyhow::Error::new(FrameAccountingFault)
                .context("refusing to cut a zero-length frame"));
        }
        loop {
            if self.queue.len() >= target {
                // `len >= target >= 1`, so the queue is non-empty and `cut` is `Some`.
                return Ok(self.queue.cut(target));
            }
            let pumped = match self.pump().await {
                Ok(v) => v,
                Err(e) => {
                    // `pump` poisons on an encode fault, but it is not the only way to
                    // arrive here, so make the rule hold for every route out.
                    if !self.faulted {
                        self.poison(&e);
                    }
                    return Err(e);
                }
            };
            match pumped {
                Some(bytes) => self.queue.push(bytes),
                // Encoder finished and channel drained: flush any final partial frame,
                // then signal completion. `cut` returns `None` exactly when the queue
                // is empty, which is how the serve leg learns the range is complete;
                // the queue holds its count in step with its bytes, so an empty queue
                // is a genuinely delivered range and never a desync.
                None => return Ok(self.queue.cut(self.queue.len())),
            }
        }
    }

    /// Test-only coalescing view of [`Self::next_frame_chunks`], for assertions that
    /// compare a whole frame against one contiguous slice. Copies once when the frame
    /// spans several queued chunks.
    #[cfg(test)]
    pub(super) async fn next_frame(&mut self, target: usize) -> anyhow::Result<Option<Bytes>> {
        let Some(frame) = self.next_frame_chunks(target).await? else {
            return Ok(None);
        };
        let chunks = frame.chunks();
        if let [only] = chunks {
            return Ok(Some(only.clone()));
        }
        let mut out = bytes::BytesMut::with_capacity(frame.total());
        for c in chunks {
            out.extend_from_slice(c);
        }
        Ok(Some(out.freeze()))
    }

    /// Advance the encode and/or receive its next output chunk. `Some(bytes)` when a
    /// chunk arrives; `None` once the encoder is done AND the channel is drained; an
    /// encode fault propagates as `Err`.
    async fn pump(&mut self) -> anyhow::Result<Option<Bytes>> {
        loop {
            match self.enc.take() {
                Some(mut fut) => {
                    let step = {
                        let rx = &mut self.rx;
                        let parked_on = &self.parked_on;
                        let demand = &self.demand;
                        let published = &mut self.published;
                        std::future::poll_fn(|cx| {
                            if let Poll::Ready(res) = fut.as_mut().poll(cx) {
                                return Poll::Ready(PumpStep::Finished(res));
                            }
                            if let Poll::Ready(recv) = rx.poll_recv(cx) {
                                return Poll::Ready(PumpStep::Item(recv));
                            }
                            // Starved: no encoded chunk is buffered and the encode is
                            // parked. The encode only parks here on a leaf or proof node
                            // no pull has produced (a full channel would have yielded a
                            // chunk above), so this consumer is stuck on that pull. Tell
                            // the pulls: one whose window has closed would otherwise wait
                            // for a payment this stuck consumer cannot collect (#1893).
                            // A park that still has encoded bytes behind it is look-ahead,
                            // not a stall, and publishes nothing.
                            let end = parked_on.load(std::sync::atomic::Ordering::Relaxed);
                            if end != *published {
                                *published = end;
                                demand.stand(end);
                            }
                            Poll::Pending
                        })
                        .await
                    };
                    match step {
                        PumpStep::Finished(res) => {
                            // Either way `fut` is NOT re-stored, so its channel sender
                            // drops and the receiver drains then ends. That makes a
                            // finished encoder and a faulted one look identical from
                            // here, and a later call would read the drained channel as
                            // "range complete" and answer `None` — which `serve_leg`
                            // turns into `StreamEnd` over a truncation. Poison on the
                            // way out so the terminal state cannot be forgotten.
                            self.withdraw_demand();
                            if let Err(e) = res {
                                self.poison(&e);
                                return Err(e);
                            }
                        }
                        PumpStep::Item(recv) => {
                            self.enc = Some(fut); // still encoding — keep the future
                            // Encoded bytes flow again, so this leg no longer waits on
                            // a pull.
                            self.withdraw_demand();
                            // The encode future owns the only sender, so while it is
                            // live the channel cannot close. If it ever did, `None`
                            // here would read as end-of-range rather than as the
                            // contradiction it is.
                            let Some(bytes) = recv else {
                                anyhow::bail!(
                                    "coherent encode channel closed while the encoder is still running"
                                );
                            };
                            return Ok(Some(bytes));
                        }
                    }
                }
                None => return Ok(self.rx.recv().await),
            }
        }
    }

    /// Withdraw this leg's standing demand, if any.
    fn withdraw_demand(&mut self) {
        if self.published != 0 {
            self.published = 0;
            self.demand.withdraw();
        }
    }

    /// Mark the encode terminal and drop the queued bytes.
    ///
    /// Not because those bytes are suspect, but because the delivery is being
    /// abandoned, so cutting another frame would bill the client for a transfer that
    /// can never complete. The client only ever sees a short delivery, so this log is
    /// the operator's only signal — the cache-hit framer poisons itself the same way.
    fn poison(&mut self, e: &anyhow::Error) {
        self.faulted = true;
        self.queue.clear();
        tracing::error!(
            hash = %self.hash,
            error = %format_args!("{e:#}"),
            "coherent range encode faulted; abandoning the delivery"
        );
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
mod tests;
