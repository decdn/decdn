//! Blob delivery: export the requested range and stream it as paid chunks.

use std::collections::VecDeque;
use std::pin::Pin;

use bytes::Bytes;
use decdn_cache::CacheResult;
use futures_util::{Stream, StreamExt};

use super::MAX_PROOFS_PER_CHUNK;
use super::outcome::{ServeEnd, ServeStop};
use super::voucher::{OwedChunk, StreamAnchor, ensure_unpaid_bytes_tracked};
use super::wire::{FrameAccountingFault, FrameChunks, FrameQueue};
use super::{
    Arc, B256, BufferedProofReader, CHUNK_BYTES, ClientHandler, ClientMessage, FloorReservation,
    Hash, LaneDeliveryState, LaneKey, Mutex, RecvStream, SendStream, U256, VoucherRejectReason,
    VoucherStop,
};
use crate::metrics::FirstByteClock;

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
    /// Export bytes not yet cut into a frame. The queue holds `O(target)` bytes
    /// (target + one export item at most), never `O(blob size)`, which is the whole
    /// point of the type. [`FrameQueue`] keeps the running byte count in step with
    /// the chunks and keeps them `Bytes`-native, so a spanning frame rides a vectored
    /// QUIC write without copying payload bytes.
    queue: FrameQueue,
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
            queue: FrameQueue::new(),
            drained: false,
            faulted: false,
            hash,
        }
    }

    /// The next wire frame as a [`FrameChunks`], totalling up to `target` bytes (or
    /// the shorter final remainder once the export is exhausted), then `None`.
    ///
    /// [`FrameChunks::total`] is the frame's total byte count, which is both what the
    /// frame header declares to the client and what the serve loop bills for.
    ///
    /// This is the zero-copy serve path: the frame holds reference-counted slices of
    /// the export items, so the caller can send them via a single vectored QUIC write
    /// alongside a small stack-encoded header without copying payload bytes. A frame
    /// that straddles two export items splits the boundary item with
    /// `Bytes::split_to`, which is also zero-copy.
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
    /// Also errors on a `target` of zero and on any call after a fault. Both are
    /// node-side bugs rather than store faults, but the caller's response is the
    /// same: abort without `StreamEnd`. A fault is terminal — the queue is dropped
    /// and later calls error rather than answering `None`, because `None` is how the
    /// serve loop learns the blob is complete.
    async fn next_frame_chunks(&mut self, target: usize) -> anyhow::Result<Option<FrameChunks>> {
        if self.faulted {
            anyhow::bail!("bao export already faulted; refusing to serve further frames");
        }
        // A zero target can never cut a frame. On an empty queue it would answer
        // `None`, which the serve loop reads as a fully delivered blob and follows
        // with `StreamEnd` over a truncated delivery. `frame_target` never returns
        // zero: every term it minimizes over is at least one, and the room term
        // floors at a bao chunk group. This restates that floor where the damage
        // would otherwise be silent.
        if target == 0 {
            tracing::error!(hash = %self.hash, "serve loop asked for a zero-length frame");
            return Err(anyhow::Error::new(FrameAccountingFault)
                .context("refusing to cut a zero-length frame"));
        }
        while !self.drained && self.queue.len() < target {
            match self.stream.next().await {
                Some(Ok(bytes)) => self.queue.push(bytes),
                Some(Err(e)) => {
                    // Poison, and drop the buffered remainder: this delivery is
                    // over, so any further frame would bill for a transfer that
                    // cannot complete.
                    self.faulted = true;
                    self.queue.clear();
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
        // `cut` returns `None` exactly when the queue is empty, which is how the serve
        // loop learns the blob is complete; the queue holds its count in step with its
        // bytes, so an empty queue is a genuinely delivered blob and never a desync.
        Ok(self.queue.cut(target))
    }

    /// Test-only coalescing view of [`Self::next_frame_chunks`], for assertions that
    /// compare a whole frame against one contiguous slice. Copies once when the frame
    /// spans several queued chunks.
    #[cfg(test)]
    async fn next_frame(&mut self, target: usize) -> anyhow::Result<Option<Bytes>> {
        let Some(frame) = self.next_frame_chunks(target).await? else {
            return Ok(None);
        };
        let chunks = frame.chunks();
        if let [only] = chunks {
            // Single chunk — no copy.
            return Ok(Some(only.clone()));
        }
        let mut out = bytes::BytesMut::with_capacity(frame.total());
        for c in chunks {
            out.extend_from_slice(c);
        }
        Ok(Some(out.freeze()))
    }
}

impl ClientHandler {
    /// Stream blob bytes to the paying client behind a credit window (ADR 003
    /// §Credit window): keep delivering `CHUNK_BYTES`-sized batches while
    /// `delivered − paid ≤ credit_window`, collecting cumulative vouchers as they
    /// arrive instead of stalling a full round trip at every interval boundary. A
    /// closing voucher settles the final partial batch. Returns how the stream
    /// ended: `Completed`, or a mid-stream stop (a rejected voucher, a drained pool
    /// or signer cap, a takedown, a spent proof budget).
    ///
    /// # Errors
    ///
    /// A client disconnect, a rate-check bail, a store or encode fault, or a broken
    /// accounting invariant. The peer-attributable errors carry
    /// [`PeerFault`](super::wire::PeerFault) or
    /// [`ClientPaymentFault`](super::wire::ClientPaymentFault); the rest reach the
    /// dispatch sink's `error!` as node faults. An error after a voucher credited
    /// bytes also carries [`PaidProgress`](super::wire::PaidProgress).
    ///
    /// The window bounds the node's credit exposure to exactly
    /// [`ClientHandler::credit_window`] — unbilled egress already on the wire —
    /// and nowhere else; the client's exposure stays at zero because vouchers are
    /// cumulative over bytes already delivered, so it never pays ahead. With the
    /// window at one interval (the unconfigured default) this reduces to the
    /// pre-credit-window stop-and-wait cadence exactly.
    #[allow(clippy::too_many_arguments)]
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
        first_byte: FirstByteClock,
    ) -> anyhow::Result<ServeEnd> {
        // Bytes an accepted voucher credited. Owned here so every exit of the loop,
        // `?` included, passes through the one `PaidProgress` tag: the dispatch
        // sink splits a peer that left into declined or abandoned by it.
        let mut paid: u64 = 0;
        self.deliver_loop(
            send,
            recv,
            hash,
            byte_offset,
            byte_len,
            total_bytes,
            lane_key,
            lane,
            client_node_id,
            rate_per_mb,
            floor_reservation,
            first_byte,
            &mut paid,
        )
        .await
        .map_err(|e| super::wire::tag_paid_progress(e, paid))
    }

    /// The body of [`Self::deliver`]. `paid` counts the bytes accepted vouchers
    /// credited; the caller reads it when the loop ends.
    // One linear, ADR-ordered serve loop (deliver → recoup → floor/takedown/pool
    // re-checks); splitting it would scatter the ordering invariants across helpers.
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    async fn deliver_loop(
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
        first_byte: FirstByteClock,
        paid: &mut u64,
    ) -> anyhow::Result<ServeEnd> {
        // Owned here so the pool floor reservation reconciles at every exit —
        // success, `?`, disconnect, panic — exactly like `LaneSlot`. The serve loop
        // below releases it once the stream repays a floor; on any other exit the
        // guard's `Drop` frees the pool's live floor headroom.
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
        let mut first_byte = Some(first_byte);
        // Bytes forwarded since the last COMPLETED interval (the sub-interval
        // remainder), and the completed intervals whose payment is still owed —
        // together they are exactly `delivered − paid`. A proof that pays part of
        // an interval leaves its remainder owed in `pending`, so the two sides of
        // that equation stay equal.
        let mut unvouchered: u64 = 0;
        let mut pending: VecDeque<OwedChunk> = VecDeque::new();
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

        // The signer's capability `cap` the node holds on this lane, for the
        // mid-stream signer cap-headroom re-check below (ADR 003 §Pool solvency).
        // Fixed for the stream, so read once here. `None` when no lane is tracked
        // (dev/test), which skips the re-check — the fail-toward-serving direction.
        let held_signer_cap: Option<U256> = match lane {
            Some(l) => Some(l.lock().await.state.cap),
            None => None,
        };

        loop {
            // Progress trackers for the no-progress guard at the bottom of the loop.
            let delivered_at_iter_start = delivered;
            let paid_at_iter_start = *paid;

            // The ramped window for the payment confirmed so far (ADR 003 §Credit
            // window). Recomputed each iteration: as `paid` advances in the recoup
            // phase the window widens, so a paying stream ramps toward `credit_max`
            // while a non-payer stays pinned at the one-interval floor.
            let window = self.credit_window(chunk_bytes, *paid);

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
                if delivered.saturating_sub(*paid) >= window {
                    break;
                }
                let Some(frame) = next_chunk.take() else {
                    break;
                };
                let len = frame.total() as u64;
                self.write_chunk_payload_multi(send, &frame).await?;
                if let Some(clock) = first_byte.take() {
                    clock.record(&self.metrics);
                }
                delivered = delivered.saturating_add(len);
                self.shed.record_egress(len);
                self.metrics.bytes_served(len);
                unvouchered = unvouchered.saturating_add(len);
                if unvouchered >= chunk_bytes {
                    pending.push_back(OwedChunk::new(unvouchered));
                    unvouchered = 0;
                }
                // Prefetched one pass ahead of the window check above, so size it
                // against what remains AFTER the send that just happened — both
                // counters are already updated.
                let room = window.saturating_sub(delivered.saturating_sub(*paid));
                next_chunk = chunks
                    .next_frame_chunks(self.frame_target(unvouchered, chunk_bytes, room))
                    .await
                    .map_err(|e| self.meter_frame_fault(e))?;
            }
            let done_delivering = next_chunk.is_none();

            // Once the whole blob is on the wire, fold the closing sub-interval
            // remainder into `pending` as a final owed chunk so the recoup batch
            // drains it uniformly with the completed intervals.
            if done_delivering && unvouchered > 0 {
                pending.push_back(OwedChunk::new(unvouchered));
                unvouchered = 0;
            }

            // --- recoup phase: collect proofs until each completed chunk is
            // paid for. The proof advances the in-memory lane watermark and the
            // background flush persists it (ADR 003 §Off-chain voucher state
            // persistence). ---
            //
            // A whole chunk is paid by one reveal, but the payer may send
            // metering vouchers ahead of it — this stream's first root voucher
            // for an epoch, or a rollover voucher when the chain is spent. A
            // metering voucher credits no whole chunk, even when a rollover
            // advances the watermark, so the chunk it precedes is still
            // outstanding. A proof can also pay only part of a chunk: it credits
            // at most the lane headroom it finds, and a sliver voucher moves the
            // watermark by a sliver. Hence the loop reads proofs for a chunk until
            // they have credited all of it, and `MAX_PROOFS_PER_CHUNK` bounds how
            // many proofs a payer may send without settling it.
            let collected_any = !pending.is_empty();
            while let Some(mut owed) = pending.pop_front() {
                let mut attempts = 0u32;
                loop {
                    attempts = attempts.saturating_add(1);
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
                            owed,
                        )
                        .await
                        .map_err(|e| {
                            e.context(format!(
                                "proof {attempts} of at most {MAX_PROOFS_PER_CHUNK} for a {}-byte \
                                 chunk ({} bytes owed, {} more queued)",
                                owed.len(),
                                owed.remaining(),
                                pending.len()
                            ))
                        })?;
                    match stop {
                        // Advance `paid` by the watermark-capped credit (rule #1),
                        // not the raw delivered chunk: a benign already-satisfied
                        // voucher credits nothing and cannot reopen the credit
                        // window for unsettled bytes.
                        VoucherStop::Continue { credited_bytes } => {
                            *paid = paid.saturating_add(credited_bytes);
                            if owed.settle(credited_bytes)? {
                                break;
                            }
                            if attempts >= MAX_PROOFS_PER_CHUNK {
                                // A payer that spends its per-chunk proof budget
                                // without settling the chunk is at fault, not this
                                // node, so the stream ends as a stop, not an error.
                                super::wire::record_stream_error(format_args!(
                                    "payer sent {attempts} proofs without settling one \
                                     {}-byte chunk ({} bytes still owed)",
                                    owed.len(),
                                    owed.remaining()
                                ));
                                tracing::debug!(
                                    attempts,
                                    chunk_bytes = owed.len(),
                                    owed_bytes = owed.remaining(),
                                    "payer spent its proof budget without settling a chunk"
                                );
                                return Ok(ServeEnd::Stopped {
                                    stop: ServeStop::ProofBudgetExhausted,
                                    bytes: delivered,
                                });
                            }
                        }
                        VoucherStop::Rejected => {
                            return Ok(ServeEnd::Stopped {
                                stop: ServeStop::VoucherRejected,
                                bytes: delivered,
                            });
                        }
                    }
                }
            }
            ensure_unpaid_bytes_tracked(delivered, *paid, unvouchered)?;

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
                res.release_if_repaid(decdn_incentive::min_payment(*paid, rate_per_mb));
            }

            // Done when the whole blob is on the wire and every interval, closing
            // partial included, has been paid.
            let done = done_delivering && pending.is_empty() && unvouchered == 0;

            // Mid-stream pool-solvency re-check (ADR 003 §Pool solvency),
            // symmetric with the takedown re-check below: re-read the cached pool
            // status and stop once `remaining − M` no longer covers the pool's
            // already-committed floor credit. A pool drains mid-flight — other
            // lanes redeem `remaining` down — so a long
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
                    // A paying in-flight download is being terminated, and the pool solvency bound
                    // only grows — an accumulator bug here is permanent per pool, so make
                    // the stop observable rather than a silent `Ok(())` (symmetric with the
                    // takedown re-check below, which also logs).
                    tracing::warn!(
                        pool_id = %lane_key.pool_id, stopped_signer = %lane_key.signer, %hash,
                        "mid-stream PoolExhausted: pool can no longer fund the floor credit committed across its signers; owner should top up the deposit. This is the POOL level — stopped_signer names the terminated stream, not the cause; a per-signer gate refusal never reaches here"
                    );
                    return Ok(ServeEnd::Stopped {
                        stop: ServeStop::PoolExhausted,
                        bytes: delivered,
                    });
                }

                // Mid-stream SIGNER cap-headroom re-check (ADR 003 §Pool solvency),
                // the per-signer sibling of the pool re-check above and on the same
                // wall-clock cadence. A signer's `cap` is shared across every
                // provider, so a client can drain it at ANOTHER node while streaming
                // here; the pool re-check above cannot catch that — the pool's
                // `remaining` stays healthy on other signers' budgets while THIS
                // signer's `cap − spent` goes to zero. Left unchecked, this node keeps
                // delivering and collecting vouchers `redeemMany` pays `min(desired,
                // cap − spent) ≈ 0` for, eating the delivered bytes — unbounded for a
                // large blob. `signer_cap_drained_midstream` reads the drain from the
                // event-fed projection (no chain call) and stops IN-BAND with a clean
                // `SignerCapExhausted` (post-auth, so naming it leaks nothing). It
                // fails toward serving on a projection gap; the on-chain redeem
                // `min(desired, cap − spent)` is the backstop and this only BOUNDS
                // over-delivery to one interval.
                if let Some(held) = held_signer_cap
                    && self
                        .signer_cap_drained_midstream(
                            lane_key.pool_id,
                            lane_key.signer,
                            held,
                            rate_per_mb,
                        )
                        .await
                {
                    self.write_reject(send, VoucherRejectReason::SignerCapExhausted, None)
                        .await?;
                    tracing::warn!(
                        pool_id = %lane_key.pool_id, stopped_signer = %lane_key.signer, %hash,
                        "mid-stream SignerCapExhausted: this signer drained its shared cap at other nodes since admission, so its cap−spent headroom no longer covers the committed floor; owner should raise the signer's cap or delegate a fresh capability. This is the SIGNER level, distinct from PoolExhausted — the pool may still be solvent on other signers' budgets"
                    );
                    return Ok(ServeEnd::Stopped {
                        stop: ServeStop::SignerCapExhausted,
                        bytes: delivered,
                    });
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
                Self::terminate_for_takedown(send, recv, hash);
                return Ok(ServeEnd::Stopped {
                    stop: ServeStop::Takedown,
                    bytes: delivered,
                });
            }

            if done {
                break;
            }

            // No-progress guard: an iteration that delivered no byte and credited
            // no byte cannot change what the next one sees, so it would repeat
            // forever without awaiting — one runtime worker lost per stream. The
            // accounting above cannot reach this state. The recoup phase pays every
            // owed chunk before the loop repeats, so each iteration starts with
            // `delivered − paid` below one chunk, and the window is never smaller
            // than one chunk: the deliver phase sends a frame unless the blob is
            // done. Reaching it means that accounting broke, so the stream fails as
            // a node fault instead of spinning.
            if delivered == delivered_at_iter_start && *paid == paid_at_iter_start {
                return Err(anyhow::anyhow!(
                    "serve loop made no progress: {delivered} bytes delivered, {paid} paid, \
                     window {window}, blob fully delivered: {done_delivering}"
                ));
            }
        }

        // Clean completion: the whole request delivered and every interval paid. The
        // floor reservation was already released once payment reached the reserved
        // floor; the guard drops as this function returns.
        self.write_message(send, &ClientMessage::StreamEnd).await?;
        let _ = send.finish();
        // Drain the client's send half to its FIN before `recv` drops, so the
        // client's finish is not stopped by the implicit `STOP_SENDING(0)` a
        // dropped `RecvStream` issues (see `drain_recv_to_fin`).
        super::drain_recv_to_fin(recv).await;
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
        Ok(ServeEnd::Completed { bytes: delivered })
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

    /// A frame wider than one export item must arrive as several `Bytes`, not as one
    /// coalesced buffer. That is the whole zero-copy claim, and nothing else in this
    /// module observes it: every other assertion here compares byte sequences, which a
    /// framer that concatenated would satisfy exactly as well.
    #[tokio::test]
    async fn a_frame_spanning_several_export_items_rides_uncopied() -> anyhow::Result<()> {
        // Four 64-byte proof-node-sized items; one 200-byte frame spans three of them
        // and splits the fourth.
        let items = vec![vec![1u8; 64], vec![2u8; 64], vec![3u8; 64], vec![4u8; 64]];
        let mut framer = ChunkFramer::new(stream_of(items), Hash::new(b"spanning-test"));

        let frame = framer
            .next_frame_chunks(200)
            .await?
            .ok_or_else(|| anyhow::anyhow!("expected a frame"))?;
        let total = frame.total();
        anyhow::ensure!(total == 200, "expected a full 200-byte frame, got {total}");
        let chunks = frame.chunks();
        anyhow::ensure!(
            chunks.len() == 4,
            "a frame over four export items must stay four slices, got {}",
            chunks.len()
        );
        anyhow::ensure!(
            chunks.iter().map(Bytes::len).collect::<Vec<_>>() == vec![64, 64, 64, 8],
            "only the boundary item is split"
        );
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
        let err = match after {
            Err(e) => e,
            Ok(o) => anyhow::bail!(
                "a faulted framer must refuse further frames, got {:?}",
                o.map(|b| b.len())
            ),
        };
        anyhow::ensure!(
            err.to_string().contains("already faulted"),
            "the second call must refuse by name, not re-report the export fault: {err}"
        );
        anyhow::ensure!(
            framer.queue.is_empty(),
            "a fault clears the queue and drops the queued bytes"
        );
        Ok(())
    }
}
