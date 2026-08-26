//! Blob delivery: export the requested range and stream it as paid chunks.

use std::collections::VecDeque;
use std::pin::Pin;

use bytes::Bytes;
use decdn_cache::CacheResult;
use futures_util::{Stream, StreamExt};

use super::MAX_PROOFS_PER_CHUNK;
use super::voucher::StreamAnchor;
use super::wire::{FrameAccountingFault, drain_frame};
use super::{
    Arc, B256, BufferedProofReader, CHUNK_BYTES, ClientHandler, ClientMessage, FloorReservation,
    Hash, LaneDeliveryState, LaneKey, Mutex, RecvStream, SendStream, U256, VoucherRejectReason,
    VoucherStop,
};

/// The byte stream [`CacheEngine::export_bao_range_stream`] hands back.
///
/// [`CacheEngine::export_bao_range_stream`]: decdn_cache::CacheEngine::export_bao_range_stream
type BaoExportStream = Pin<Box<dyn Stream<Item = CacheResult<Bytes>> + Send>>;

/// Re-frame the cache's bao export stream into `cdn/client/v1` wire frames
/// (#1132).
///
/// The export yields one item per bao element — a 64-byte proof pair or one
/// 16 KiB chunk group — which lines up with no particular frame size. This buffers
/// until it holds the caller's requested target, so the serve task holds
/// O(target + one export item) rather than O(blob size). It guarantees two
/// load-bearing properties:
///
/// - **Never an empty frame.** [`super::wire::chunk_frame_bufs`] refuses an empty
///   payload and [`decdn_protocol::client::encode_chunk_frame_headers`] refuses a
///   zero `payload_len` behind it (#1088), and the inactivity deadline on both
///   receive loops rests on "a frame arrived" and "bytes made progress" being the
///   same statement.
/// - **No frames at all for an empty export.** The 0-byte blob (#1054) must go
///   straight to `StreamEnd` rather than send a zero-length frame first.
struct ChunkFramer {
    stream: BaoExportStream,
    /// Export bytes not yet cut into a frame, as a queue of reference-counted
    /// chunks. The queue holds `O(target)` bytes (target + one export item at
    /// most), never `O(blob size)`, which is the whole point of the type.
    /// `Bytes` chunks are kept `Bytes`-native so a spanning frame can be sent
    /// via a vectored QUIC write without copying payload bytes.
    queue: VecDeque<Bytes>,
    /// Total bytes currently queued.
    queued: usize,
    /// The export stream has yielded its last item; `queue` is all that remains.
    drained: bool,
    /// The export faulted. Terminal: `queue` is cleared and no further frame is
    /// ever cut. Not because those bytes are suspect — `queue` only ever holds
    /// WHOLE export items, and they came from the store's own bao encoder — but
    /// because the delivery is being abandoned, so cutting another frame would
    /// bill the client for a transfer that can never complete.
    ///
    /// The producer (`export_bao_range_stream`) already refuses to yield past its
    /// own error, so this is belt-and-braces — but the framer accepts an arbitrary
    /// [`BaoExportStream`], and "an error is terminal" is not something it can
    /// check. Enforcing it here keeps the guarantee inside the type that would
    /// otherwise violate it.
    faulted: bool,
    /// The blob being served, for the fault log. A mid-export fault is a
    /// server-side store problem (actor crash, corruption) that the client can
    /// only report as a generic short delivery, so the node has to name it — this
    /// is the operator's only signal.
    hash: Hash,
}

impl ChunkFramer {
    fn new(stream: BaoExportStream, hash: Hash) -> Self {
        Self {
            stream,
            queue: VecDeque::new(),
            queued: 0,
            drained: false,
            faulted: false,
            hash,
        }
    }

    /// The next wire frame as a set of `Bytes` chunks, totalling up to `target`
    /// bytes (or the shorter final remainder once the export is exhausted), then
    /// `None`.
    ///
    /// The `usize` beside the chunks is their total byte count, which is both what
    /// the frame header declares to the client and what the serve loop bills for.
    ///
    /// This is the zero-copy serve path: the returned `Vec<Bytes>` holds
    /// reference-counted slices of the export items, so the caller can send them
    /// via a single vectored QUIC write alongside a small stack-encoded header
    /// without copying payload bytes. A frame that straddles two export items
    /// splits the boundary item with `Bytes::split_to`, which is also zero-copy.
    ///
    /// `target` is a per-call argument, not a field, because the serve loop
    /// clamps it to the credit window's remaining room. The window bound is
    /// "delivered − paid never exceeds the window by more than one frame", so a
    /// frame sized past the remaining room widens the node's unrecouped exposure
    /// by exactly that excess.
    ///
    /// # Errors
    ///
    /// Propagates an export fault. Note this includes the truncation refusal,
    /// which the streaming export can only detect after its last item — so unlike
    /// the buffered export it can fire when frames are already on the wire. The
    /// caller must abort the delivery (skipping `StreamEnd`) so the client sees a
    /// short delivery and does not pay the closing voucher.
    ///
    /// Also errors on a `target` of zero, on a `queued`/`queue` desync, and on any
    /// call after a fault. Those three are node-side bugs rather than store faults,
    /// but the caller's response is the same: abort without `StreamEnd`. A fault is
    /// terminal — the queue is dropped and later calls error rather than answering
    /// `None`, because `None` is how the serve loop learns the blob is complete.
    async fn next_frame_chunks(
        &mut self,
        target: usize,
    ) -> anyhow::Result<Option<(Vec<Bytes>, usize)>> {
        if self.faulted {
            anyhow::bail!("bao export already faulted; refusing to serve further frames");
        }
        // A zero target can never cut a frame. On an empty queue it would answer
        // `None`, which the serve loop reads as a fully delivered blob and follows
        // with `StreamEnd` over a truncated delivery; on a non-empty one the cut
        // below refuses, but names a `queued`/`queue` desync — the wrong cause.
        // `frame_target` never returns zero: every term it minimizes over is at
        // least one, and the room term floors at a bao chunk group. This restates
        // that floor where the damage would otherwise be silent or misattributed.
        if target == 0 {
            tracing::error!(hash = %self.hash, "serve loop asked for a zero-length frame");
            return Err(anyhow::Error::new(FrameAccountingFault)
                .context("refusing to cut a zero-length frame"));
        }
        while !self.drained && self.queued < target {
            match self.stream.next().await {
                Some(Ok(bytes)) => {
                    let len = bytes.len();
                    if len > 0 {
                        self.queued = self.queued.saturating_add(len);
                        self.queue.push_back(bytes);
                    }
                }
                Some(Err(e)) => {
                    // Poison, and drop the buffered remainder: this delivery is
                    // over, so any further frame would bill for a transfer that
                    // cannot complete.
                    self.faulted = true;
                    self.queue.clear();
                    self.queued = 0;
                    tracing::error!(
                        hash = %self.hash,
                        error = %e,
                        "bao export faulted mid-delivery; aborting the serve without StreamEnd \
                         (the client sees a short delivery and does not pay the closing voucher)"
                    );
                    return Err(anyhow::anyhow!("cache bao export failed: {e}"));
                }
                None => self.drained = true,
            }
        }
        if self.queued == 0 {
            // `None` is how the serve loop learns the blob is complete, so it must
            // rest on the queue itself and not only on its counter: an under-counting
            // `queued` would end a truncated delivery with `StreamEnd` and collect the
            // closing voucher for it.
            anyhow::ensure!(
                self.queue.is_empty(),
                "queued reads 0 with {} chunks still queued; refusing to report the \
                 blob as fully delivered",
                self.queue.len()
            );
            return Ok(None);
        }
        let Some(frame) = drain_frame(&mut self.queue, &mut self.queued, target) else {
            tracing::error!(
                hash = %self.hash,
                queued = self.queued,
                target,
                "queued byte count disagrees with the queue; refusing to cut a frame"
            );
            return Err(anyhow::Error::new(FrameAccountingFault).context(format!(
                "cut no chunks from {} queued bytes at target {target}; refusing to \
                 report the blob as fully delivered",
                self.queued
            )));
        };
        Ok(Some(frame))
    }

    /// Test-only coalescing view of [`Self::next_frame_chunks`], for assertions that
    /// compare a whole frame against one contiguous slice. Copies once when the frame
    /// spans several queued chunks.
    #[cfg(test)]
    async fn next_frame(&mut self, target: usize) -> anyhow::Result<Option<Bytes>> {
        let Some((chunks, total)) = self.next_frame_chunks(target).await? else {
            return Ok(None);
        };
        if chunks.len() == 1 {
            // Single chunk — no copy.
            let mut iter = chunks.into_iter();
            return Ok(iter.next());
        }
        let mut out = bytes::BytesMut::with_capacity(total);
        for c in chunks {
            out.extend_from_slice(&c);
        }
        Ok(Some(out.freeze()))
    }
}

impl ClientHandler {
    /// Stream blob bytes to the paying client behind a credit window (ADR 003
    /// §Credit window): keep delivering `CHUNK_BYTES`-sized batches while
    /// `delivered − paid ≤ credit_window`, collecting cumulative vouchers as they
    /// arrive instead of stalling a full round trip at every interval boundary. A
    /// closing voucher settles the final partial batch. Returns `Ok(())` on a
    /// clean rejection or a completed delivery.
    ///
    /// The window bounds the node's credit exposure to exactly
    /// [`ClientHandler::credit_window`] — unbilled egress already on the wire —
    /// and nowhere else; the client's exposure stays at zero because vouchers are
    /// cumulative over bytes already delivered, so it never pays ahead. With the
    /// window at one interval (the unconfigured default) this reduces to the
    /// pre-credit-window stop-and-wait cadence exactly.
    // One linear, ADR-ordered serve loop (deliver → recoup → floor/takedown/pool
    // re-checks); splitting it would scatter the ordering invariants across helpers.
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    pub(super) async fn deliver(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        hash: Hash,
        byte_offset: u64,
        byte_len: u64,
        total_bytes: u64,
        lane_key: LaneKey,
        lane: Option<&Arc<Mutex<LaneDeliveryState>>>,
        client_node_id: B256,
        rate_per_mb: u64,
        floor_reservation: Option<FloorReservation>,
    ) -> anyhow::Result<()> {
        // Owned here so the pool floor reservation reconciles at every exit —
        // success, `?`, disconnect, panic — exactly like `LaneSlot`. The serve loop
        // below keeps its `note_unpaid` current and releases it once the stream
        // repays a floor; `Drop` folds any residual unpaid loss into `dead_charge`.
        let floor_reservation = floor_reservation;
        // The client-facing `cdn/client/v1` payload is ALWAYS the bao interleaved
        // verified-stream encoding — there is no raw-byte path (ADR 038 §Serve
        // side, AC#4). `export_bao_range_stream` reads the persisted outboard and
        // emits proof+data for the chunk-group-aligned span covering the request; it
        // works on a complete blob AND on the *partial* blob an origin-tier range
        // pull imported, and never re-hashes held content. A whole-blob request
        // is `(byte_offset == 0, byte_len == 0)`, which aligns to the full chunk
        // range. Vouchers below meter the actually-served bytes — content PLUS the
        // interleaved proof nodes (ADR 038 §Payment metering) — by counting wire
        // bytes, so range delivery is billed over the span, not the whole blob.
        //
        // The STREAMING export is the load-bearing choice here (#1132). Its
        // whole-blob sibling `export_bao_range` materialises the entire aligned
        // range before the first frame goes out, so serving a 708 MB blob costs
        // ~708 MB resident per concurrent serve. Streaming bounds this task to one
        // export item plus one wire frame, independent of blob size. Do not
        // use the buffered call here.
        //
        // The export snaps to enclosing 16 KiB chunk-group boundaries (a
        // bao proof anchors whole groups); the serve side does NOT trim back to
        // `byte_offset` — trimming would break verification. The receiver decodes
        // the group-aligned superset and discards the leading bytes before
        // `byte_offset`, so a well-behaved resume requests a group-aligned offset
        // (ADR 038 §Verification model).
        // `total_bytes` is the authoritative whole-blob size the caller already
        // resolved (a `Complete` blob's size, or the whole-blob size an origin-tier
        // range pull reported). The export needs it to build the BaoTree;
        // the store cannot be relied on for it because an origin-tier range pull
        // imports a *partial* blob whose `status()` size is `None` (#823).
        let data = self
            .cache
            .export_bao_range_stream(hash, byte_offset, byte_len, total_bytes)
            .await
            .map_err(|e| anyhow::anyhow!("cache export_bao_range_stream failed: {e}"))?;

        let chunk_bytes = CHUNK_BYTES;

        // The in-flight takedown re-check (ADR 011 compliance) keys on the pool
        // FUNDER — the pool owner (`getPool.owner`), resolved from the cached
        // pool-view. `None` (no view wired or a read fault) falls back to the
        // open-time gates and the hash-denylist re-check.
        let funder = self.pool_funder(lane_key.pool_id).await;

        // Bytes written to the wire, and bytes covered by an accepted voucher.
        // Their gap `delivered − paid` is the unrecouped credit the window caps.
        let mut delivered: u64 = 0;
        let mut paid: u64 = 0;
        // Bytes forwarded since the last COMPLETED interval (the sub-interval
        // remainder), and the completed-but-unpaid interval deltas awaiting
        // collection — together they are exactly `delivered − paid`.
        let mut unvouchered: u64 = 0;
        let mut pending: VecDeque<u64> = VecDeque::new();
        // One buffered voucher reader for the whole stream: it buffers a
        // pipelined voucher across recoup calls, so every voucher read MUST go
        // through it — a second reader would lose bytes it read ahead.
        let mut reader = BufferedProofReader::default();
        // This stream's chain anchor — which epoch it has been told about, so a
        // bare reveal arriving on it can be placed (ADR 003 §Concurrent
        // Streams, Rule 1). Per stream and in memory only: a restart drops the
        // streams, and the lane's durable record keeps the strongest claim's
        // chain state.
        let mut anchor = StreamAnchor::default();

        // `ChunkFramer` yields no frames for an empty export and never a
        // zero-length frame, so the empty-payload refusal inside
        // `write_chunk_payload_multi` is the floor restated at the last door, not a
        // live failure mode — the empty blob makes no pass through the deliver phase
        // and goes straight to `StreamEnd` (#1054). The `?` on `next_frame_chunks` IS
        // live — that is where a mid-export store fault or the truncation refusal
        // surfaces, because the export streams (#1132).
        let mut chunks = ChunkFramer::new(data, hash);
        // Nothing is delivered, paid, or vouchered yet, so the opening frame may run
        // a whole interval against the whole opening window.
        let opening_window = self.credit_window(chunk_bytes, 0);
        let mut next_chunk = chunks
            .next_frame_chunks(self.frame_target(0, chunk_bytes, opening_window))
            .await
            .map_err(|e| self.meter_frame_fault(e))?;

        // Wall-clock cadence for the mid-stream pool-solvency re-check below (ADR
        // 003 §Pool solvency). Start the clock at loop entry — admission already
        // verified solvency once via `try_reserve_floor`, so the first re-check
        // falls one interval later, bounding over-delivery from admission to
        // `interval × per-stream-rate`.
        let pool_recheck_interval = self.pool_recheck_interval();
        let mut last_pool_check = std::time::Instant::now();

        loop {
            // The ramped window for the payment confirmed so far (ADR 003 §Credit
            // window). Recomputed each iteration: as `paid` advances in the recoup
            // phase the window widens, so a paying stream ramps toward `credit_max`
            // while a non-payer stays pinned at the one-interval floor.
            let window = self.credit_window(chunk_bytes, paid);

            // --- deliver phase: stream chunks while the window has room. The
            // window is checked BEFORE each send, so the frontier
            // `delivered − paid` can overshoot by at most the one chunk that
            // crosses the threshold: the credit exposure is "≤ window + one
            // chunk", exact to within a chunk. The check must stay pre-send, not
            // anticipatory (would-this-chunk-cross): an anticipatory check can
            // stop short of a full interval, and at the one-interval floor
            // (`window == interval`) it would never complete one, starving the
            // recoup phase of a voucher to collect and deadlocking the loop. ---
            // The pre-fetched frame is owned, so it cannot be copied out of
            // `next_chunk` and left behind on the window `break` — the window check
            // therefore runs BEFORE the `take`, keeping the un-sent frame in
            // `next_chunk` for the next pass and for the `done_delivering` read below.
            while next_chunk.is_some() {
                if delivered.saturating_sub(paid) >= window {
                    break;
                }
                let Some((chunk_vec, clen)) = next_chunk.take() else {
                    break;
                };
                let len = clen as u64;
                self.write_chunk_payload_multi(send, &chunk_vec, clen)
                    .await?;
                delivered = delivered.saturating_add(len);
                self.shed.record_egress(len);
                unvouchered = unvouchered.saturating_add(len);
                if unvouchered >= chunk_bytes {
                    pending.push_back(unvouchered);
                    unvouchered = 0;
                }
                // Prefetched one pass ahead of the window check above, so size it
                // against what remains AFTER the send that just happened — both
                // counters are already updated.
                let room = window.saturating_sub(delivered.saturating_sub(paid));
                next_chunk = chunks
                    .next_frame_chunks(self.frame_target(unvouchered, chunk_bytes, room))
                    .await
                    .map_err(|e| self.meter_frame_fault(e))?;
            }
            let done_delivering = next_chunk.is_none();

            // Once the whole blob is on the wire, fold the closing sub-interval
            // remainder into `pending` as a final delta so the recoup batch drains
            // it uniformly with the completed intervals.
            if done_delivering && unvouchered > 0 {
                pending.push_back(unvouchered);
                unvouchered = 0;
            }

            // Capture this iteration's maximum in-flight unpaid balance NOW — after
            // the deliver phase advanced `delivered` and before the recoup phase can
            // advance `paid` or take an early exit. A stream that dies in its first
            // iteration (a rejected first voucher returns from the recoup block, or a
            // `?` faults there) never reaches the end-of-iteration hook below, so
            // without this note its last-noted unpaid stays 0 and `Drop` would fold
            // nothing — letting "connect, take one free interval, vanish" escape the
            // `dead_charge` accounting. Noting here keeps the guard honest at every
            // exit path (ADR 003 §Pool solvency).
            if let Some(res) = floor_reservation.as_ref() {
                res.note_unpaid(decdn_incentive::min_payment(
                    delivered.saturating_sub(paid),
                    rate_per_mb,
                ));
            }

            // --- recoup phase: collect proofs until each completed chunk is
            // paid for. The proof advances the in-memory lane watermark and the
            // background flush persists it (ADR 003 §Off-chain voucher state
            // persistence). ---
            //
            // A chunk is paid by one reveal, but the payer may send housekeeping
            // vouchers ahead of it — this stream's first root voucher for an
            // epoch, or a rollover voucher when the chain is spent. Those
            // re-assert a cumulative the node already holds, so they credit
            // nothing, and the chunk they precede is still outstanding. Hence
            // the delta stays queued until something actually credits it, and
            // `MAX_PROOFS_PER_CHUNK` bounds how long a payer may keep answering
            // with proofs that pay nothing.
            let collected_any = !pending.is_empty();
            while let Some(&delta) = pending.front() {
                let mut attempts = 0u32;
                loop {
                    let stop = self
                        .commit_one_proof(
                            send,
                            recv,
                            &mut reader,
                            &mut anchor,
                            hash,
                            lane_key,
                            lane,
                            client_node_id,
                            rate_per_mb,
                            delta,
                        )
                        .await?;
                    match stop {
                        // Advance `paid` by the watermark-capped credit (rule #1),
                        // not the raw delivered delta: a benign already-satisfied
                        // voucher credits nothing and cannot reopen the credit
                        // window for unsettled bytes.
                        VoucherStop::Continue { credited_bytes } if credited_bytes > 0 => {
                            paid = paid.saturating_add(credited_bytes);
                            pending.pop_front();
                            break;
                        }
                        VoucherStop::Continue { .. } => {
                            attempts = attempts.saturating_add(1);
                            if attempts >= MAX_PROOFS_PER_CHUNK {
                                anyhow::bail!(
                                    "payer sent {attempts} proofs that credited nothing for one \
                                     outstanding chunk"
                                );
                            }
                        }
                        VoucherStop::Rejected => return Ok(()),
                    }
                }
            }

            // Reconcile the pool floor reservation against this stream's live
            // balance (ADR 003 §Pool solvency). `paid` and `delivered` are BYTE
            // counters (see their declaration above); the reservation accounts in
            // µUSDC, so every byte quantity crosses over through `min_payment` —
            // the two units are never compared directly.
            if let Some(res) = floor_reservation.as_ref() {
                // Free the pool's live reservation once cumulative payment reaches the
                // amount that was reserved (the ramp-floor credit this stream fronts).
                // Everything above it is self-funded. Matching release to the reserved
                // µUSDC keeps it correct at any `credit_ramp_divisor` — with the ramp
                // disabled the reservation is the full `credit_max`, so release waits
                // for that much paid, not a single interval.
                res.release_if_repaid(decdn_incentive::min_payment(paid, rate_per_mb));
                // Keep the drop-time reconcile honest with the CURRENT unpaid
                // balance: on an un-repaid stream `Drop` folds `min(reserved, this)`
                // (the span-capped reservation) into `dead_charge`. A fully-settled
                // stream ends `delivered == paid`, so the last note here is
                // `min_payment(0, rate) == 0` and `Drop` charges nothing.
                res.note_unpaid(decdn_incentive::min_payment(
                    delivered.saturating_sub(paid),
                    rate_per_mb,
                ));
            }

            // Done when the whole blob is on the wire and every interval, closing
            // partial included, has been paid.
            let done = done_delivering && pending.is_empty() && unvouchered == 0;

            // Mid-stream pool-solvency re-check (ADR 003 §Pool solvency),
            // symmetric with the takedown re-check below: re-read the cached pool
            // status and stop once `remaining − M` no longer covers the pool's
            // already-committed floor credit. A pool drains mid-flight — other
            // lanes redeem `remaining` down, or `dead_charge` rises — so a long
            // stream must catch that; the credit window bounds delivery ahead of
            // the last COLLECTED voucher, not ahead of the last SOLVENCY-verified
            // point, so nothing else notices a shared pool going unredeemable.
            // `new_reserve = ZERO` asks exactly that: is the total ALREADY
            // committed (this stream included) still within budget? On `false` the
            // pool can no longer fund further credit, so stop IN-BAND with a clean
            // `PoolExhausted` (not a QUIC reset, unlike a takedown) so the owner
            // learns to top up — `PoolExhausted` is post-auth, so naming the
            // condition leaks nothing an open-time refusal must hide, and it is not
            // watermark-gated (no bundle). A `None` pool view fails OPEN (the
            // on-chain redeem is the backstop), exactly like the admission gate and
            // the takedown funder resolution.
            //
            // The check runs on a WALL-CLOCK cadence, not at every chunk
            // boundary: the projection only advances as the settlement watcher
            // folds redeem events, so re-reading it faster than that returns the
            // same value (wasted `pool_floor` lock reads on the fastest streams),
            // and per-stream throughput is bounded, so the interval bounds
            // worst-case over-delivery on a drained pool to `interval ×
            // per-stream-rate`. Only the frequency changes — the reject behavior
            // and the on-chain redeem backstop are unchanged.
            if collected_any && !done && last_pool_check.elapsed() >= pool_recheck_interval {
                last_pool_check = std::time::Instant::now();
                if let Some(status) = self.pool_view_status_cached(lane_key.pool_id).await
                    && !self.pool_budget_covers_reserve(
                        lane_key.pool_id,
                        status.remaining,
                        U256::ZERO,
                    )
                {
                    self.write_reject(send, VoucherRejectReason::PoolExhausted, None)
                        .await?;
                    // A paying in-flight download is being terminated, and `dead_charge`
                    // only grows — an accumulator bug here is permanent per pool, so make
                    // the stop observable rather than a silent `Ok(())` (symmetric with the
                    // takedown re-check below, which also logs).
                    tracing::warn!(
                        pool_id = %lane_key.pool_id, %hash,
                        "mid-stream PoolExhausted: pool can no longer fund committed floor credit; owner should top up the deposit"
                    );
                    return Ok(());
                }
            }

            // ADR 011 §On Blacklist Event: in-flight streams for a blacklisted hash
            // are terminated at the next voucher boundary. Under the credit window
            // that first check lands up to one WINDOW into the stream (steady state
            // ~one interval, since each later iteration delivers roughly one
            // interval before recouping), so detection latency is window-bounded,
            // not per-MB — the one-interval floor caps it, and even a full window is
            // negligible against the takedown compliance window. Gated on
            // `collected_any` so it runs only after a committed batch. The check
            // sits AFTER a voucher so the bytes already
            // on the wire are still paid for — the takedown stops FURTHER delivery,
            // it does not retroactively make the last interval free. Only meaningful
            // while bytes remain to withhold: a takedown landing at the final
            // voucher has nothing left to stop, and terminating there would turn a
            // complete, fully-paid delivery into a reset (no `StreamEnd`) — hence the
            // `!done` guard.
            if collected_any && !done && self.takedown_landed(hash, funder) {
                self.terminate_for_takedown(send, recv, hash);
                return Ok(());
            }

            if done {
                break;
            }
        }

        // Clean completion: the whole request delivered and every interval paid. Only
        // reached here — every abnormal exit returns earlier — so mark the reservation
        // settled, making its drop fold the proportional unpaid tail (0 for a fully
        // paid stream) rather than the conservative full `reserved` (ADR 003 §Pool solvency).
        if let Some(res) = floor_reservation.as_ref() {
            res.mark_settled();
        }
        self.write_message(send, &ClientMessage::StreamEnd).await?;
        let _ = send.finish();
        // ADR 040: emit the ONE hit sighting for this served request, only after the
        // terminal StreamEnd frame is written — so a serve that fails to finish cleanly
        // is never counted. Fires exactly once per served blob (a multi-range or
        // multi-interval serve is still one sighting) and after any fill-time admission
        // read on the miss paths that reach `deliver`. This is what makes a hot RESIDENT
        // blob — served here on every hit — accumulate frequency and become promotable.
        self.cache.observe_hit(hash);
        // ADR 041: credit the realized operator margin back to the source that
        // speculatively warmed this blob (a no-op for an untagged / non-speculative
        // hash), on the same clean-completion edge as the ADR 040 hit sighting.
        self.credit_warming_serve(hash, delivered);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{BaoExportStream, ChunkFramer, Hash};
    use bytes::Bytes;
    use decdn_cache::CacheError;

    /// Build an export stream from per-item payloads.
    fn stream_of(items: Vec<Vec<u8>>) -> BaoExportStream {
        Box::pin(futures_util::stream::iter(
            items.into_iter().map(|b| Ok(Bytes::from(b))),
        ))
    }

    /// At any target, the framer must cut exactly what `slice::chunks(target)` cuts
    /// over the concatenated export: same byte sequence, same boundaries, full frames
    /// until the remainder. The client's cumulative wire-byte accounting is defined
    /// over the frame sequence, so this equivalence is the safety argument for the
    /// framer existing at all (#1132) — and it must hold for every target, because
    /// the serve loop varies the target per call to track the credit window.
    #[tokio::test]
    async fn framing_matches_slice_chunks_at_every_target() -> anyhow::Result<()> {
        // Deliberately awkward item sizes: a 64-byte proof pair, a full chunk group,
        // and remainders that straddle frame boundaries at every target below.
        let items = vec![
            vec![1u8; 64],
            vec![2u8; 16 * 1024],
            vec![3u8; 64],
            vec![4u8; 1000],
            vec![5u8; 1],
        ];
        let flat: Vec<u8> = items.iter().flatten().copied().collect();

        // A target below, at, and above one export item, plus one that exceeds the
        // whole export (so the framer must still flush a single short remainder).
        for target in [1usize, 64, 1000, 1024, 16 * 1024, 64 * 1024, 1024 * 1024] {
            let mut framer = ChunkFramer::new(stream_of(items.clone()), Hash::new(b"framing-test"));
            let mut got: Vec<Bytes> = Vec::new();
            while let Some(frame) = framer.next_frame(target).await? {
                got.push(frame);
            }

            let want: Vec<&[u8]> = flat.chunks(target).collect();
            anyhow::ensure!(
                got.len() == want.len(),
                "target {target}: framed {} chunks, slice::chunks yields {}",
                got.len(),
                want.len()
            );
            for (i, (actual, expected)) in got.iter().zip(want.iter()).enumerate() {
                anyhow::ensure!(
                    actual.as_ref() == *expected,
                    "target {target}: frame {i} differs"
                );
            }
            // Restated as an invariant rather than inferred from the comparison: no
            // frame may be empty (#1088) or exceed what the caller asked for.
            for frame in &got {
                anyhow::ensure!(
                    !frame.is_empty(),
                    "target {target}: framer emitted an empty frame"
                );
                anyhow::ensure!(
                    frame.len() <= target,
                    "target {target}: framer emitted an oversized frame"
                );
            }
        }
        Ok(())
    }

    /// An empty export (the 0-byte blob, #1054) must yield no frames at all, so
    /// the deliver phase makes no pass and the serve goes straight to `StreamEnd`.
    #[tokio::test]
    async fn an_empty_export_yields_no_frames() -> anyhow::Result<()> {
        let mut framer = ChunkFramer::new(stream_of(Vec::new()), Hash::new(b"empty-test"));
        anyhow::ensure!(
            framer.next_frame(1024).await?.is_none(),
            "an empty export must yield no frames"
        );
        Ok(())
    }

    /// A zero target must be refused by name. The serve loop reads `None` as "the
    /// blob is fully delivered", so on an empty queue a zero target would send
    /// `StreamEnd` over a truncation and let the client pay the closing voucher for
    /// it; on a non-empty queue the cut refuses anyway, but blames a `queued`/`queue`
    /// desync for what is a bad argument. `frame_target` never returns zero, so this
    /// pins the floor restated where the failure would otherwise be silent or
    /// misattributed.
    #[tokio::test]
    async fn a_zero_frame_target_is_refused_not_read_as_end_of_blob() -> anyhow::Result<()> {
        let mut framer = ChunkFramer::new(
            stream_of(vec![vec![9u8; 4096]]),
            Hash::new(b"zero-target-test"),
        );
        let err = match framer.next_frame_chunks(0).await {
            Ok(Some(_)) => anyhow::bail!("a zero target must not cut a frame"),
            Ok(None) => anyhow::bail!("a zero target must not read as end-of-blob"),
            Err(e) => e,
        };
        anyhow::ensure!(
            err.to_string().contains("zero-length frame"),
            "unexpected error: {err}"
        );
        // The bytes are still there: the refusal is about the target, not the export.
        let frame = framer.next_frame(4096).await?;
        anyhow::ensure!(frame.is_some(), "the export survives a refused target");
        Ok(())
    }

    /// A mid-export fault — including the truncation refusal, which the streaming
    /// export can only report after its last item — must propagate out of
    /// `next_frame_chunks` rather than being swallowed into a short-but-clean
    /// delivery. That is what makes the caller abort without `StreamEnd`, so the
    /// client rejects the delivery and never pays the closing voucher.
    ///
    /// The fault is also terminal: the queue is dropped and every later call errors
    /// rather than answering `None`, which the serve loop would read as a clean end
    /// of blob. The coherent twin pins the same rule in
    /// `a_coherent_encode_fault_is_terminal`.
    #[tokio::test]
    async fn a_mid_export_fault_propagates() -> anyhow::Result<()> {
        let stream: BaoExportStream = Box::pin(futures_util::stream::iter(vec![
            // 1500, deliberately NOT a multiple of the 1024 target used below: one
            // full frame is cuttable, leaving 476 bytes queued when the fault
            // lands. With a multiple the queue would already be empty at that point
            // and the fault's `queue.clear()` would be unobservable.
            Ok(Bytes::from(vec![7u8; 1500])),
            Err(CacheError::Store(anyhow::anyhow!(
                "export_bao stream for deadbeef ended without Done; refusing truncated export"
            ))),
        ]));
        let mut framer = ChunkFramer::new(stream, Hash::new(b"fault-test"));

        // The first full frame is already cuttable from the buffered bytes.
        let first = framer.next_frame(1024).await?;
        anyhow::ensure!(first.is_some(), "expected a frame before the fault");

        // Draining toward the next frame reaches the error.
        let err = loop {
            match framer.next_frame(1024).await {
                Ok(Some(_)) => {}
                Ok(None) => anyhow::bail!("framer ended cleanly; the export fault was swallowed"),
                Err(e) => break e,
            }
        };
        anyhow::ensure!(
            err.to_string().contains("refusing truncated export"),
            "fault lost its cause: {err}"
        );

        // The framer must now be POISONED. Without it, the 476 unverified bytes
        // still buffered when the export faulted would be cut into a frame and
        // put on the wire as if they were good — and the client billed for them.
        // `Ok(None)` would be just as wrong: it reads as a clean end of blob.
        let after = framer.next_frame(1024).await;
        anyhow::ensure!(
            after.is_err(),
            "a faulted framer must refuse further frames, got {:?}",
            after.map(|o| o.map(|b| b.len()))
        );
        Ok(())
    }
}
