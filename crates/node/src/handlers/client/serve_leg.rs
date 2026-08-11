//! The decoupled downstream **serve leg** of the node serve-miss driver (#1621
//! B2 part 2, ADR 037).
//!
//! [`ClientHandler::serve_leg`] is the seller half of the two-leg serve-miss.
//! It walks the requested range `R = [offset, offset + len)` in order and
//! delivers ALL of it to the paying client: spans the store already holds are
//! read from the cache ([`ServeStore::encode_range`]); spans a concurrent
//! **pull leg** (a separate task) has not filled yet are awaited on the store's
//! live present-range watch ([`ServeStore::observe`]) until they land. Billing,
//! the credit window, takedown, and client-disconnect handling are lifted
//! verbatim from the fused `window_forward_loop`; only the byte SOURCE changes —
//! from an upstream pull tee'd through this task to the cache the pull leg fills
//! beside it.
//!
//! # Coordination with the pull leg (shared, single-task state)
//!
//! Both legs run as concurrent futures on ONE serve task, so the shared state is
//! plain `Arc`, never `tokio::spawn`ed across threads:
//!
//! - **`served_paid`** — the client's PAID *content* frontier. The serve leg
//!   stores it after every voucher batch commits (mapped from paid WIRE bytes
//!   through [`content_paid_frontier`]); the pull leg's `WindowPacer` reads it to
//!   bound `pulled − served_paid ≤ window`.
//! - **`served_paid_advanced`** — notified after each advance, so a pull leg
//!   parked in `PaceDecision::Wait` re-decides exactly when payment clears.
//! - **`pull_ended` + `pull_result`** — the pull leg records its terminal
//!   outcome in `pull_result` and THEN fires `pull_ended`. Whenever the serve
//!   leg must await a gap becoming present it races the present-range watch
//!   against `pull_ended`: a pull that ended `Err` fails the serve (a gap the
//!   pull could not fill can never be delivered); a pull that ended `Ok` means
//!   the gap must now be present. This is the no-hang guarantee.
//!
//! The serve leg owns termination: it is what fulfils `R` for the client, so its
//! completion (or error) ends the serve and drops the pull leg.
//!
//! This module lands ahead of its caller: the orchestration that runs the two
//! legs in a biased `select!` (and deletes `window_forward_loop`) is #1621 B2
//! part 2 Task 13. Until then [`ClientHandler::serve_leg`] and its helpers are
//! unreferenced outside tests, hence the module-level `dead_code` allow.
#![allow(dead_code)]

use std::sync::Mutex as StdMutex;
use std::time::Duration;

use bao_tree::ChunkRanges;
use bytes::{Bytes, BytesMut};
use decdn_bao_range::RangedStore;
use decdn_cache::{EncodeStream, NodeRangedStore, PresentRangeWatch, ServeStore};
use decdn_client_pull::sink::content_paid_frontier;
use decdn_protocol::CHUNK_SIZE;
use futures_util::StreamExt;
use tokio::sync::Notify;

use super::{
    Arc, AtomicU64, B256, BatchStop, BufferedVoucherReader, ChannelDeliveryState, ChannelId,
    ChunkData, ClientHandler, ClientMessage, Hash, MB_BYTES, Mutex, Ordering, RecvStream,
    SendStream, VecDeque,
};

/// Bytes per bao chunk (a `bao_tree::ChunkNum`): a 1 KiB chunk. Present ranges
/// are chunk-unit `ChunkRanges`; their byte span is the chunk boundary scaled by
/// this. Mirrors `decdn_client_pull::driver`'s private constant of the same name.
const CHUNK_BYTES: u64 = 1024;

/// How long the gap wait polls for the store to MATERIALIZE the blob (admit its
/// first chunk group) before a present-range watch can be opened.
///
/// [`ServeStore::observe`] errors until the blob is `Partial` (B1), so before the
/// pull leg's first admit there is no watch to await on — only this bounded poll,
/// racing `pull_ended`. Once the watch opens it drives every later advance with
/// no polling, so this interval governs ONLY the pre-first-admit window, which
/// the pull leg's discover+open latency dominates in practice.
const WATCH_OPEN_RETRY: Duration = Duration::from_millis(25);

/// How long the gap wait keeps re-checking presence AFTER the pull leg reported
/// success before it concludes a genuine hole.
///
/// A `pull_result` of `Ok` means every requested byte is authoritatively in the
/// cache — the pull leg admitted and verified it before finishing. But the store's
/// present-range view can lag the final chunk-group commit by a moment (the
/// iroh-blobs present/observe gotcha): `present_ranges` is answered by the blob
/// store actor, and the query racing `pull_ended` can be served just before the last
/// commit is applied. Failing on that first transient miss aborts a fully-delivered
/// blob with a spurious "still missing". So re-check for this long, converging as the
/// actor applies the commit (microseconds in practice); only a real inconsistency (a
/// drive that reported success without filling) still fails, at the cap.
const PULL_OK_PRESENCE_CAP: Duration = Duration::from_secs(5);
/// Poll step for [`PULL_OK_PRESENCE_CAP`]. Short enough to add no visible latency to
/// the common case (presence is usually true within a poll or two), long enough not
/// to spin the store actor.
const PULL_OK_PRESENCE_POLL: Duration = Duration::from_millis(5);

/// One ordered piece of the requested range against a present-range snapshot:
/// either bytes the store holds now, or a gap the pull leg has yet to fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Span {
    /// `[off, off + len)` is present in the store and can be encoded now.
    Present { off: u64, len: u64 },
    /// `[off, off + len)` is not yet present; it must be awaited.
    Gap { off: u64, len: u64 },
}

/// Fold a chunk-unit [`ChunkRanges`] into the contiguous byte ranges it covers,
/// ascending, each end clamped to `total` (the ragged final group). Each entry is
/// `(start, len)` with `len > 0`. Twin of `decdn_client_pull::driver`'s private
/// helper of the same name (that one is not exported).
fn contiguous_byte_ranges(ranges: &ChunkRanges, total: u64) -> Vec<(u64, u64)> {
    let boundaries = ranges.boundaries();
    let mut out = Vec::new();
    let mut it = boundaries.iter();
    while let Some(a) = it.next() {
        let start = a.0.saturating_mul(CHUNK_BYTES).min(total);
        // A boundary with no matching close is an OPEN-ENDED present range `[a, ∞)`:
        // a fully-present blob observes as `ChunkRanges{0..}`, a SINGLE (unpaired)
        // boundary. Pairing `(a, b)` alone would silently drop that final unbounded
        // run — reporting a complete blob as entirely absent, which stalls the serve
        // on its own cached bytes. Clamp the open end to `total` (the ragged final
        // group) so the completed tail is recognised as present.
        let end = match it.next() {
            Some(b) => b.0.saturating_mul(CHUNK_BYTES).min(total),
            None => total,
        };
        if end > start {
            out.push((start, end - start));
        }
    }
    out
}

/// Decompose the requested range against a present-range snapshot into the
/// ordered sequence of present/gap spans that exactly tile `[req_off, req_end)`.
///
/// `requested` is `(offset, len)`; `len == 0` means "to the end of the blob"
/// (the `align_range` / driver convention). `present` is the store's current
/// chunk-unit present ranges; `total` is the whole-blob content length. The
/// spans are contiguous, in order, and cover the whole request — a present run is
/// emitted where the store holds bytes, a gap where it does not.
fn span_plan(requested: (u64, u64), present: &ChunkRanges, total: u64) -> Vec<Span> {
    let (req_off, req_len) = requested;
    let req_end = if req_len == 0 {
        total
    } else {
        req_off.saturating_add(req_len).min(total)
    };
    let req_off = req_off.min(req_end);

    let mut out = Vec::new();
    let mut cursor = req_off;
    for (pstart, plen) in contiguous_byte_ranges(present, total) {
        let pend = pstart.saturating_add(plen);
        if pend <= cursor {
            continue; // wholly before the cursor
        }
        if pstart >= req_end {
            break; // wholly past the request
        }
        let seg_start = pstart.max(cursor);
        let seg_end = pend.min(req_end);
        if seg_start > cursor {
            out.push(Span::Gap {
                off: cursor,
                len: seg_start - cursor,
            });
        }
        if seg_end > seg_start {
            out.push(Span::Present {
                off: seg_start,
                len: seg_end - seg_start,
            });
        }
        cursor = seg_end;
        if cursor >= req_end {
            break;
        }
    }
    if cursor < req_end {
        out.push(Span::Gap {
            off: cursor,
            len: req_end - cursor,
        });
    }
    out
}

/// Is content byte `g` inside the present-range snapshot?
fn byte_present(present: &ChunkRanges, g: u64, total: u64) -> bool {
    for (start, len) in contiguous_byte_ranges(present, total) {
        if start > g {
            break;
        }
        if g < start.saturating_add(len) {
            return true;
        }
    }
    false
}

/// Peek the pull leg's terminal outcome without holding the lock across an await.
/// `None` while the pull is still running; `Some(Ok)` once it finished cleanly;
/// `Some(Err(msg))` once it failed (message flattened, since `anyhow::Error` is
/// not `Clone`). A poisoned lock is reported as a failure.
fn pull_outcome(pull_result: &StdMutex<Option<anyhow::Result<()>>>) -> Option<Result<(), String>> {
    match pull_result.lock() {
        Ok(guard) => match &*guard {
            None => None,
            Some(Ok(())) => Some(Ok(())),
            Some(Err(e)) => Some(Err(format!("{e:#}"))),
        },
        Err(_poisoned) => Some(Err("upstream pull result lock poisoned".to_string())),
    }
}

/// Produces the in-order wire-bao frames of `R` from the cache, awaiting the pull
/// leg to fill gaps. It re-frames each present span's header-less
/// [`ServeStore::encode_range`] stream into `CHUNK_SIZE` `cdn/client/v1` frames
/// (the same reframing the direct-serve `ChunkFramer` does), and blocks on the
/// present-range watch at a gap until the byte lands or the pull fails.
struct SpanProducer<'a> {
    store: &'a NodeRangedStore,
    /// Exclusive content end of the request.
    end: u64,
    /// Whole-blob content length (the store's tree size).
    total: u64,
    /// Next undelivered content byte.
    cursor: u64,
    /// The encode stream of the present span currently being drained.
    active: Option<EncodeStream>,
    /// Exclusive content end of the active present span.
    active_end: u64,
    /// Encode bytes not yet cut into a `CHUNK_SIZE` frame. Bounded by one encode
    /// item plus the sub-frame remainder — never grows with span size.
    frame_buf: BytesMut,
    /// The active encode stream has yielded its last item.
    drained: bool,
    /// The live present-range watch, opened lazily once the blob materializes.
    watch: Option<PresentRangeWatch>,
    pull_ended: Arc<Notify>,
    pull_result: Arc<StdMutex<Option<anyhow::Result<()>>>>,
}

impl<'a> SpanProducer<'a> {
    fn new(
        store: &'a NodeRangedStore,
        offset: u64,
        end: u64,
        total: u64,
        pull_ended: Arc<Notify>,
        pull_result: Arc<StdMutex<Option<anyhow::Result<()>>>>,
    ) -> Self {
        Self {
            store,
            end,
            total,
            cursor: offset,
            active: None,
            active_end: 0,
            frame_buf: BytesMut::new(),
            drained: false,
            watch: None,
            pull_ended,
            pull_result,
        }
    }

    /// The next wire frame of `R`, or `None` once the whole range is delivered.
    ///
    /// # Errors
    ///
    /// An `encode_range` fault, or — critically — a gap that the pull leg could
    /// not fill (`pull_ended` with an `Err` `pull_result`): the serve cannot
    /// deliver a hole, so it fails rather than hang.
    async fn next_frame(&mut self) -> anyhow::Result<Option<Bytes>> {
        loop {
            // 1. Drain the active present span first.
            if self.active.is_some() {
                if let Some(frame) = self.next_active_frame().await? {
                    return Ok(Some(frame));
                }
                // Span exhausted: advance past it and drop the stream.
                self.cursor = self.active_end;
                self.active = None;
                self.drained = false;
                self.frame_buf.clear();
            }

            // 2. Whole range delivered?
            if self.cursor >= self.end {
                return Ok(None);
            }

            // 3. Plan the remainder against the CURRENT present ranges and act on
            //    the first span. Re-read every pass so a gap the pull just filled
            //    is picked up.
            let present = self.store.present_ranges().await?;
            let plan = span_plan((self.cursor, self.end - self.cursor), &present, self.total);
            match plan.into_iter().next() {
                Some(Span::Present { off, len }) => {
                    let stream = self.store.encode_range(off, len).await.map_err(|e| {
                        anyhow::anyhow!("cache encode_range({off}, {len}) failed: {e}")
                    })?;
                    self.active = Some(stream);
                    // `off == self.cursor` (the plan starts at the cursor), so the
                    // span ends at `cursor + len`.
                    self.active_end = off.saturating_add(len);
                    self.drained = false;
                    self.frame_buf.clear();
                    // Loop to drain it.
                }
                Some(Span::Gap { off, .. }) => {
                    // Await the byte at the gap start becoming present, or the pull
                    // failing. On return the loop re-plans (the freshly landed
                    // prefix becomes a Present span).
                    self.await_gap_present(off).await?;
                }
                None => return Ok(None),
            }
        }
    }

    /// Cut the next `CHUNK_SIZE` frame (or the shorter span remainder) from the
    /// active encode stream. `None` once the span is exhausted. A stream fault
    /// aborts the delivery — the caller must not send `StreamEnd`.
    async fn next_active_frame(&mut self) -> anyhow::Result<Option<Bytes>> {
        let Some(stream) = self.active.as_mut() else {
            return Ok(None);
        };
        while !self.drained && self.frame_buf.len() < CHUNK_SIZE {
            match stream.next().await {
                Some(Ok(bytes)) => self.frame_buf.extend_from_slice(&bytes),
                Some(Err(e)) => {
                    // Abandon: drop the buffered remainder so no further frame bills
                    // the client for a delivery that cannot complete.
                    self.frame_buf.clear();
                    self.drained = true;
                    return Err(anyhow::anyhow!(
                        "cache encode_range stream faulted mid-serve: {e}"
                    ));
                }
                None => self.drained = true,
            }
        }
        if self.frame_buf.is_empty() {
            return Ok(None);
        }
        let take = self.frame_buf.len().min(CHUNK_SIZE);
        Ok(Some(self.frame_buf.split_to(take).freeze()))
    }

    /// Block until content byte `g` is present, or fail if the pull leg ended and
    /// could not fill it. The no-hang guarantee: every wait races the
    /// present-range watch against `pull_ended`.
    async fn await_gap_present(&mut self, g: u64) -> anyhow::Result<()> {
        loop {
            // Register the pull-ended waiter BEFORE inspecting shared state, so a
            // terminal outcome recorded concurrently cannot slip between the check
            // and the await (the pull leg records `pull_result` before firing
            // `pull_ended`; `enable()` arms the waiter for a `notify_*` that may
            // already have fired). The Arc clone keeps the borrow off `self`, so
            // the watch below can be borrowed independently.
            let pull_ended = Arc::clone(&self.pull_ended);
            let mut ended = Box::pin(pull_ended.notified());
            ended.as_mut().enable();

            // Terminal pull outcome? Checked every pass, since it is set before
            // `pull_ended` fires.
            if let Some(outcome) = pull_outcome(&self.pull_result) {
                return match outcome {
                    Err(msg) => Err(anyhow::anyhow!(
                        "upstream pull failed before filling content offset {g} of the requested \
                         range; cannot deliver the gap: {msg}"
                    )),
                    // The pull succeeded, so byte `g` is authoritatively cached. Re-check
                    // presence with a bounded wait rather than failing on a first transient
                    // miss — `present_ranges` can lag the pull's final commit by a moment
                    // (see [`PULL_OK_PRESENCE_CAP`]). Only a genuine hole fails, at the cap.
                    Ok(()) => self.await_present_after_pull(g).await,
                };
            }

            // Raced ahead of us?
            if self.present_covers(g).await? {
                return Ok(());
            }

            // Ensure a watch is open. It errors until the blob materializes (B1):
            // tolerate that with a bounded poll racing `pull_ended`, then retry.
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

            // Await the next present-range advance vs the pull ending. The watch
            // arm borrows `self.watch`; `ended` borrows the local Arc, so the two
            // borrows are disjoint.
            let advanced = async {
                match self.watch.as_mut() {
                    Some(w) => w.next().await.map(|_ranges| ()),
                    None => None,
                }
            };
            tokio::select! {
                biased;
                () = ended.as_mut() => {
                    // Loop: the terminal-outcome check at the top handles it.
                }
                closed = advanced => {
                    if closed.is_none() {
                        // The watch stream ended; re-open on the next pass.
                        self.watch = None;
                    }
                    // Loop: re-check `present_covers(g)`.
                }
            }
        }
    }

    /// Does the store currently hold content byte `g`?
    ///
    /// Takes `&mut self` (though it mutates nothing) so the future holds a
    /// `&mut SpanProducer` rather than a `&SpanProducer` across the `present_ranges`
    /// await: `SpanProducer` carries `Send`-but-not-`Sync` cache streams
    /// ([`EncodeStream`] / [`PresentRangeWatch`]), so `&SpanProducer` is not `Send`
    /// and would make the whole serve future non-`Send` — which the iroh
    /// `ProtocolHandler::accept` bound forbids. `&mut SpanProducer` only needs
    /// `SpanProducer: Send`, which holds.
    async fn present_covers(&mut self, g: u64) -> anyhow::Result<bool> {
        let present = self.store.present_ranges().await?;
        Ok(byte_present(&present, g, self.total))
    }

    /// Confirm byte `g` is present after the pull leg reported success, tolerating
    /// the store's present-range view lagging the pull's final commit
    /// ([`PULL_OK_PRESENCE_CAP`]). Returns once `g` is present; fails only if it is
    /// still missing at the cap — a genuine hole (a drive that reported success
    /// without filling the range), which must not hang the serve.
    async fn await_present_after_pull(&mut self, g: u64) -> anyhow::Result<()> {
        let mut waited = Duration::ZERO;
        loop {
            if self.present_covers(g).await? {
                return Ok(());
            }
            if waited >= PULL_OK_PRESENCE_CAP {
                return Err(anyhow::anyhow!(
                    "upstream pull reported success but content offset {g} of the requested \
                     range is still missing {PULL_OK_PRESENCE_CAP:?} after completion"
                ));
            }
            tokio::time::sleep(PULL_OK_PRESENCE_POLL).await;
            waited = waited.saturating_add(PULL_OK_PRESENCE_POLL);
        }
    }
}

impl ClientHandler {
    /// Deliver the requested range `R = [offset, offset + len)` to the paying
    /// client from the cache, awaiting a concurrent pull leg to fill any gaps —
    /// the downstream (seller) half of the decoupled serve-miss (#1621 B2, ADR
    /// 037). `len == 0` means "to the end of the blob".
    ///
    /// Delivery, billing, the credit window, group-commit voucher batching, the
    /// in-flight takedown boundary, and client-disconnect handling are lifted from
    /// `window_forward_loop`; the source is the cache
    /// ([`ServeStore::encode_range`] / [`ServeStore::observe`]) instead of an
    /// upstream pull tee'd through this task. The caller (orchestration) has
    /// already proven channel ownership, run the pre-flight gates, negotiated
    /// `interval_mb`, and signed + sent the `StreamResponse`; the pull leg fills
    /// the store beside this call. Consumes neither stream — the caller does.
    ///
    /// # Money semantics
    ///
    /// `served_paid` is the PAID *content* frontier; completion gates on payment
    /// (every delivered interval vouchered), never on delivery, so the
    /// credit-window tail the client received ahead of its voucher is always
    /// billed before `StreamEnd` (the Phase-A under-pay lesson, seller side).
    /// Vouchers meter WIRE bytes (content plus interleaved bao proof, ADR 038), so
    /// the internal `delivered`/`paid` counters and the `window` gate are wire
    /// quantities; the shared `served_paid` frontier the pull leg paces against is
    /// mapped back into content space via [`content_paid_frontier`].
    ///
    /// # Errors
    ///
    /// A client disconnect (the write / voucher-collect surfaces it), an
    /// underpayment bail, an `encode_range` fault, or a gap the pull leg could not
    /// fill. On any error the caller drops the pull leg, which stops the upstream
    /// spend and persists the buyer watermark.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn serve_leg(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        store: NodeRangedStore,
        outboard: crate::node_origin::OutboardReader,
        channel: &Arc<Mutex<ChannelDeliveryState>>,
        hash: Hash,
        channel_id: ChannelId,
        client_node_id: B256,
        rate_per_mb: u64,
        interval_mb: u64,
        offset: u64,
        len: u64,
        total_bytes: u64,
        window: u64,
        served_paid: Arc<AtomicU64>,
        served_paid_advanced: Arc<Notify>,
        pull_ended: Arc<Notify>,
        pull_result: Arc<StdMutex<Option<anyhow::Result<()>>>>,
    ) -> anyhow::Result<()> {
        // Resolve the request end. `len == 0` ⇒ to the blob end (driver
        // convention); otherwise clamp to the tree size.
        let end = if len == 0 {
            total_bytes
        } else {
            offset.saturating_add(len).min(total_bytes)
        };
        let offset = offset.min(end);

        let interval_bytes = interval_mb.saturating_mul(MB_BYTES).max(1);
        // Backpressure bound, floored at one interval so the loop can always make
        // progress (deliver a full interval, then recoup its voucher). Mirrors the
        // fused loop's window floor.
        let window = window.max(interval_bytes);
        // Group-commit cap (#1483): at most this many vouchers share one fsync,
        // bounded by how many intervals fit in the window.
        let batch_cap = usize::try_from(window / interval_bytes)
            .unwrap_or(usize::MAX)
            .max(1);

        // Read once: the funder is immutable for the channel's lifetime, and the
        // per-boundary in-flight takedown check must not re-take the channel lock
        // every batch to re-read it. This is the FUNDER (ADR 011 compliance),
        // never the channel's `voucher_signer`.
        let funder = channel.lock().await.state.client;

        // Bytes written to the wire, and bytes an accepted voucher covered — both
        // WIRE quantities (bao content + proof, ADR 038). Their gap
        // `delivered − paid` is the unrecouped credit the window caps.
        let mut delivered: u64 = 0;
        let mut paid: u64 = 0;
        // Bytes forwarded since the last COMPLETED interval (the sub-interval
        // remainder), and the completed-but-unpaid interval deltas awaiting
        // collection — together they are exactly `delivered − paid`.
        let mut unvouchered: u64 = 0;
        let mut pending: VecDeque<u64> = VecDeque::new();
        // One buffered voucher reader for the whole stream (#1483): every read
        // goes through it so pipelined vouchers buffered ahead of a batch commit
        // are not lost.
        let mut reader = BufferedVoucherReader::default();

        // The coherent whole-range bao encoder (#1621 B2 part 2, ADR 038): ONE
        // verified stream for `R`, produced incrementally — leaf data awaited from
        // the cache the pull fills, proof nodes from the shared `outboard` the pull
        // captures. Replaces the incoherent piece-wise `encode_range` producer.
        let mut producer = super::serve_encoder::CoherentFrameProducer::new(
            store,
            outboard,
            offset,
            end,
            total_bytes,
            Arc::clone(&pull_ended),
            Arc::clone(&pull_result),
        );

        // The first frame — awaiting the pull leg if `R` opens on a gap. A pull
        // that ends `Err` here fails the serve rather than hanging.
        let mut next_chunk = producer.next_frame().await?;

        loop {
            // Progress trackers for the no-progress guard below (re-homed from
            // `window_forward_loop`'s livelock guard). An iteration that delivers no
            // new byte AND clears no voucher has stalled — the client stopped paying.
            let delivered_at_iter_start = delivered;
            let mut committed_this_iter = 0usize;

            // --- deliver phase: stream frames while the window has room. Checked
            // BEFORE each send, so `delivered − paid` overshoots by at most the one
            // frame that crosses the threshold. ---
            while next_chunk.is_some() {
                if delivered.saturating_sub(paid) >= window {
                    break;
                }
                let Some(chunk) = next_chunk.take() else {
                    break;
                };
                let clen = chunk.len() as u64;
                let frame = ChunkData::new(chunk.to_vec())
                    .map_err(|e| anyhow::anyhow!("refusing to serve an invalid chunk: {e}"))?;
                // A downstream drop surfaces here as `Err` (#856 client-disconnect
                // shape); propagate so the caller drops the pull leg.
                self.write_message(send, &ClientMessage::ChunkData(frame))
                    .await?;
                delivered = delivered.saturating_add(clen);
                unvouchered = unvouchered.saturating_add(clen);
                if unvouchered >= interval_bytes {
                    pending.push_back(unvouchered);
                    unvouchered = 0;
                }
                next_chunk = producer.next_frame().await?;
            }
            let done_delivering = next_chunk.is_none();

            // Once the whole range is on the wire, fold the closing sub-interval
            // remainder into `pending` so the recoup batch drains it uniformly.
            if done_delivering && unvouchered > 0 {
                pending.push_back(unvouchered);
                unvouchered = 0;
            }

            // --- recoup phase: batch up to `batch_cap` completed intervals into
            // ONE fsynced commit, acking each voucher only after it is durable
            // (#1483, group commit). ---
            let mut deltas: Vec<u64> = Vec::with_capacity(batch_cap);
            while deltas.len() < batch_cap {
                match pending.pop_front() {
                    Some(delta) => deltas.push(delta),
                    None => break,
                }
            }
            let collected_any = !deltas.is_empty();
            if collected_any {
                // A transport drop or an underpayment bail surfaces as `Err`;
                // propagate so the caller drops the pull leg (bounding the
                // upstream spend and persisting the buyer watermark).
                let outcome = self
                    .collect_voucher_batch(
                        send,
                        recv,
                        &mut reader,
                        hash,
                        channel_id,
                        Some(channel),
                        client_node_id,
                        rate_per_mb,
                        &deltas,
                    )
                    .await?;
                committed_this_iter = outcome.committed;
                // Advance `paid` by exactly the committed prefix's WIRE bytes.
                let paid_bytes: u64 = deltas.iter().take(outcome.committed).sum();
                paid = paid.saturating_add(paid_bytes);
                if outcome.committed > 0 {
                    // Publish the PAID CONTENT frontier for the pull leg's
                    // `WindowPacer`, mapping paid WIRE back into content space (the
                    // largest chunk-group boundary provably inside the paid wire
                    // prefix — conservative, so the pull never overshoots its
                    // window). One contiguous delivery from `offset`, so `offset`
                    // is the single fetch-start.
                    let served = content_paid_frontier(offset, total_bytes, paid);
                    served_paid.store(served, Ordering::Relaxed);
                    served_paid_advanced.notify_waiters();
                }
                // Re-queue deltas the client had not paid yet (a short batch),
                // preserving order at the front.
                for &delta in deltas
                    .get(outcome.committed..)
                    .unwrap_or_default()
                    .iter()
                    .rev()
                {
                    pending.push_front(delta);
                }
                match outcome.stop {
                    // A voucher was rejected (or the commit hit `RetryLater`): the
                    // rejection was already written and any valid prefix committed
                    // + acked. Stop cleanly; the caller drops the pull leg.
                    BatchStop::Rejected => return Ok(()),
                    BatchStop::Continue => {}
                }
            }

            // Done when the whole range is on the wire and every interval — closing
            // partial included — has been paid. Gating on PAID (not delivered) is
            // what bills the credit-window tail the client received ahead of its
            // voucher.
            let done = done_delivering && pending.is_empty() && unvouchered == 0;

            // ADR 011 §On Blacklist Event: terminate an in-flight delivery at the
            // next voucher boundary once a takedown lands. Gated on `collected_any`
            // (runs only after a committed batch, so bytes already on the wire stay
            // paid) and `!done` (a complete, fully-paid delivery must finish with
            // `StreamEnd`, not a reset). Abandoning the concurrent pull of the
            // taken-down blob is the pull leg's own takedown handling, reached when
            // this return drops it.
            if collected_any && !done && self.takedown_landed(hash, Some(funder)) {
                self.terminate_for_takedown(send, recv, hash);
                return Ok(());
            }

            if done {
                break;
            }

            // No-progress guard (re-homed from `window_forward_loop`'s livelock
            // guard, window.rs): an iteration that delivered no new byte (delivery
            // blocked on the credit window, waiting for payment) AND cleared no
            // voucher (the client stopped paying — `collect_voucher_batch` timed out
            // with nothing committed) cannot make progress. The client has abandoned
            // (#856 drop-after-fill): stop cleanly. The caller then cancels the pull
            // leg, which bounds the upstream spend (#1610) and persists the buyer
            // watermark (#852). Any delivery or payment this iteration resets it, so
            // an honest-but-slow client (patience = one `collect_voucher_batch`
            // read timeout) is never dropped early.
            let made_delivery_progress = delivered > delivered_at_iter_start;
            let made_payment_progress = committed_this_iter > 0;
            if !made_delivery_progress && !made_payment_progress {
                return Ok(());
            }
        }

        // Fully delivered and fully paid: signal clean completion. Every byte was
        // bao-verified into the cache by the pull leg's admit before this leg read
        // it, so the served bytes are sound.
        self.write_message(send, &ClientMessage::StreamEnd).await?;
        let _ = send.finish();
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::{Span, span_plan};
    use bao_tree::{ChunkNum, ChunkRanges};

    /// One chunk group (16 KiB) in bytes, the granularity the store admits at.
    const GROUP: u64 = 16 * 1024;
    /// Chunks per group (`GROUP / CHUNK_BYTES`).
    const GROUP_CHUNKS: u64 = 16;

    /// A present set covering the byte range `[start_group * GROUP, end_group *
    /// GROUP)`, expressed as the chunk-unit `ChunkRanges` the store returns.
    fn present_groups(start_group: u64, end_group: u64) -> ChunkRanges {
        ChunkRanges::from(ChunkNum(start_group * GROUP_CHUNKS)..ChunkNum(end_group * GROUP_CHUNKS))
    }

    #[test]
    fn interior_hold_gives_gap_present_gap() {
        // A 4-group blob holding only the second group: prefix gap, present, suffix gap.
        let present = present_groups(1, 2);
        let plan = span_plan((0, 4 * GROUP), &present, 4 * GROUP);
        assert_eq!(
            plan,
            vec![
                Span::Gap { off: 0, len: GROUP },
                Span::Present {
                    off: GROUP,
                    len: GROUP
                },
                Span::Gap {
                    off: 2 * GROUP,
                    len: 2 * GROUP
                },
            ]
        );
    }

    #[test]
    fn prefix_hold_gives_present_then_gap() {
        // The first two groups of a 4-group blob held: a leading present run, one suffix gap.
        let present = present_groups(0, 2);
        let plan = span_plan((0, 4 * GROUP), &present, 4 * GROUP);
        assert_eq!(
            plan,
            vec![
                Span::Present {
                    off: 0,
                    len: 2 * GROUP
                },
                Span::Gap {
                    off: 2 * GROUP,
                    len: 2 * GROUP
                },
            ]
        );
    }

    #[test]
    fn nothing_held_gives_one_gap() {
        let present = ChunkRanges::empty();
        let plan = span_plan((0, 4 * GROUP), &present, 4 * GROUP);
        assert_eq!(
            plan,
            vec![Span::Gap {
                off: 0,
                len: 4 * GROUP
            }]
        );
    }

    #[test]
    fn all_held_gives_one_present() {
        let present = present_groups(0, 4);
        let plan = span_plan((0, 4 * GROUP), &present, 4 * GROUP);
        assert_eq!(
            plan,
            vec![Span::Present {
                off: 0,
                len: 4 * GROUP
            }]
        );
    }

    #[test]
    fn plan_is_scoped_to_the_requested_subrange() {
        // Hold groups 0..3 of a 4-group blob, but request only the middle two
        // groups [GROUP, 3*GROUP): the plan must cover exactly that request, all
        // present (held), never the bytes outside it.
        let present = present_groups(0, 3);
        let plan = span_plan((GROUP, 2 * GROUP), &present, 4 * GROUP);
        assert_eq!(
            plan,
            vec![Span::Present {
                off: GROUP,
                len: 2 * GROUP
            }]
        );
    }

    #[test]
    fn ragged_tail_is_clamped_to_total() {
        // A blob whose final group is partial: a whole-blob request over a fully
        // held store yields one present span clamped to `total`, not the padded
        // group boundary.
        let total = 2 * GROUP + 777;
        // The store reports the ragged final group as a full chunk range up to the
        // ceiling; `span_plan` clamps it to `total`.
        let present = present_groups(0, 3);
        let plan = span_plan((0, 0), &present, total);
        assert_eq!(
            plan,
            vec![Span::Present { off: 0, len: total }],
            "len == 0 means to-end, and the present span clamps to total"
        );
    }
}
