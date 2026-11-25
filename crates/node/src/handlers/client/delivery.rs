//! Blob delivery: export the requested range and stream it as paid chunks.

use std::pin::Pin;

use bytes::Bytes;
use decdn_cache::CacheResult;
use futures_util::{Stream, StreamExt};

use super::admission::{Admission, Admitted};
use super::credit::Acquired;
use super::outcome::ServeEnd;
use super::serve_waits::{ServeWaits, Wait};
use super::voucher::StreamAnchor;
use super::wire::{FrameAccountingFault, FrameChunks, FrameQueue};
use super::{
    BufferedProofReader, CHUNK_BYTES, ClientHandler, ClientMessage, Connection, Hash, RecvStream,
    SendStream,
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
    /// frame header declares to the client and what the serve loop takes from the
    /// stream's credit.
    ///
    /// This is the zero-copy serve path: the frame holds reference-counted slices of
    /// the export items, so the caller can send them via a single vectored QUIC write
    /// alongside a small stack-encoded header without copying payload bytes. A frame
    /// that straddles two export items splits the boundary item with
    /// `Bytes::split_to`, which is also zero-copy.
    ///
    /// `target` is a per-call argument, not a field, because the serve loop
    /// clamps it to the stream's credit.
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
                        error = %e.display_chain(),
                        "bao export faulted mid-delivery; aborting the serve without StreamEnd \
                         (the client sees a short delivery and does not pay the closing voucher)"
                    );
                    return Err(anyhow::Error::from(e).context("cache bao export failed"));
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
    /// Stream blob bytes to the paying client against the stream's credit (ADR
    /// 003 § Credit Window): the node sends a frame only when the payment the
    /// stream's own proofs claimed covers it, and reads the payer's proofs while
    /// the credit falls short. Returns how the stream ended: `Completed`, a
    /// mid-stream stop (a rejected voucher, a drained pool or signer cap, a
    /// takedown, a spent proof budget), or `Refused` when the stream answered
    /// before admission and its admission refused at the payer's first proof.
    ///
    /// `admission` is [`Admission::OnFirstProof`] for an open that answered
    /// before admission: the loop waits for the payer's first proof while the
    /// store sets up its export, then admits ([`Self::admit_on_first_proof`]).
    /// A refusal goes out as a `StreamError` before any `ChunkData`.
    ///
    /// # Errors
    ///
    /// A client disconnect, a rate-check bail, a store or encode fault, or a broken
    /// accounting invariant. The peer-attributable errors carry
    /// [`PeerFault`](super::wire::PeerFault) or
    /// [`ClientPaymentFault`](super::wire::ClientPaymentFault); the rest reach the
    /// dispatch sink's `error!` as node faults. An error after a proof credited
    /// bytes also carries [`PaidProgress`](super::wire::PaidProgress), and one
    /// after a frame followed the request carries
    /// [`FrameSeen`](super::wire::FrameSeen).
    ///
    /// The node's exposure to unpaid delivery is zero: every byte it sends was
    /// covered by payment a proof on this stream had already claimed.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn deliver(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        recv: &mut RecvStream,
        hash: Hash,
        byte_offset: u64,
        byte_len: u64,
        total_bytes: u64,
        admission: Admission,
        rate_per_mb: u64,
        first_byte: FirstByteClock,
    ) -> anyhow::Result<ServeEnd> {
        // Bytes the proofs on this stream claimed for it. Owned here so every exit
        // of the loop, `?` included, passes through the one `PaidProgress` tag:
        // the dispatch sink splits a peer that left into unclaimed, declined or
        // abandoned by it and by the reader's `FrameSeen` tag.
        let mut paid: u64 = 0;
        // One buffered proof reader for the whole stream: it buffers a pipelined
        // proof across waits, so every proof read MUST go through it — a second
        // reader would lose bytes it read ahead.
        let mut reader = BufferedProofReader::default();
        self.deliver_loop(
            conn,
            send,
            recv,
            hash,
            byte_offset,
            byte_len,
            total_bytes,
            admission,
            &mut reader,
            rate_per_mb,
            first_byte,
            &mut paid,
        )
        .await
        .map_err(|e| super::wire::tag_frame_seen(super::wire::tag_paid_progress(e, paid), &reader))
    }

    /// The body of [`Self::deliver`]. `paid` counts the bytes the proofs on
    /// this stream claimed for it; the caller reads it when the loop ends.
    // One linear, ADR-ordered serve loop (cut a frame, acquire credit, send);
    // splitting it would scatter the ordering invariants across helpers.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn deliver_loop(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        recv: &mut RecvStream,
        hash: Hash,
        byte_offset: u64,
        byte_len: u64,
        total_bytes: u64,
        admission: Admission,
        reader: &mut BufferedProofReader,
        rate_per_mb: u64,
        mut first_byte: FirstByteClock,
        paid: &mut u64,
    ) -> anyhow::Result<ServeEnd> {
        // The client-facing `cdn/client/v1` payload is ALWAYS the bao interleaved
        // verified-stream encoding — there is no raw-byte path (ADR 038 §Serve
        // side, AC#4). `export_bao_range_stream` reads the persisted outboard and
        // emits proof+data for the chunk-group-aligned span covering the request; it
        // works on a complete blob AND on the *partial* blob an origin-tier range
        // pull imported, and never re-hashes held content. A whole-blob request
        // is `(byte_offset == 0, byte_len == 0)`, which aligns to the full chunk
        // range. Credit meters the actually-served bytes — content PLUS the
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
        // Splits the span's idle time by what this loop waits on, and records
        // the path's state, at every exit (#2348), the export's setup
        // included.
        //
        // An open that answered before admission waits for the payer's first
        // proof here, beside the export's setup, so the store read overlaps the
        // payer's round trip. Admission then runs once, before the first frame.
        // The empty blob sells no byte, so an open of it that answered before
        // admission skips admission: no load-shed slot, no chain read. It ends
        // with `StreamEnd` at once.
        let mut waits = ServeWaits::start(conn);
        let export = async {
            waits
                .time(
                    Wait::Store,
                    self.cache
                        .export_bao_range_stream(hash, byte_offset, byte_len, total_bytes),
                )
                .await
                .map_err(|e| anyhow::Error::from(e).context("cache export_bao_range_stream failed"))
        };
        let (data, admitted) = match admission {
            Admission::Admitted(admitted) => (export.await?, admitted),
            Admission::OnFirstProof(_) if total_bytes == 0 => {
                drop(export);
                return self
                    .complete_delivery(send, recv, &mut waits, hash, 0, None)
                    .await;
            }
            Admission::OnFirstProof(adm) => {
                // The wait for the proof is the payer's: the first-byte clock
                // leaves it out. The export's setup that outlasts it stays in.
                let proof = async {
                    self.await_first_proof(conn, recv, reader).await?;
                    anyhow::Ok(std::time::Instant::now())
                };
                let (data, proof_at) = tokio::try_join!(export, proof)?;
                first_byte.mark_first_proof_at(proof_at);
                match self.admit_on_first_proof(adm).await {
                    Ok(admitted) => (data, admitted),
                    Err(reason) => return self.refuse_admission(send, reason).await,
                }
            }
        };
        // Refresh recency once the stream is admitted and before the first
        // byte goes out, so a blob still carrying the open-time seed does not
        // rank oldest for eviction while it streams. An open the payer never
        // takes refreshes nothing, so unpaid opens cannot keep a blob resident.
        // Recency only: the frequency sighting waits for `StreamEnd`.
        self.cache.touch_recency(hash);
        let Admitted {
            lane_key,
            lane,
            mut guards,
            shed: _shed,
            pin: _pin,
        } = admitted;
        let lane = &lane;

        let chunk_bytes = CHUNK_BYTES;
        let mut first_byte = Some(first_byte);
        // Wire bytes this stream wrote. The stream's credit is `paid − delivered`.
        let mut delivered: u64 = 0;
        // This stream's chain anchor — which epoch it has been told about, so a
        // bare reveal arriving on it can be placed (ADR 003 §Concurrent
        // Streams, Rule 1). Per stream and in memory only: a restart drops the
        // streams, and the lane's durable record keeps the strongest claim's
        // chain state.
        let mut anchor = StreamAnchor::default();

        // `ChunkFramer` yields no frames for an empty export and never a
        // zero-length frame, so the empty blob makes no pass through the loop and
        // goes straight to `StreamEnd` (#1054). The `?` on `next_frame_chunks` IS
        // live — that is where a mid-export store fault or the truncation refusal
        // surfaces, because the export streams (#1132).
        let mut chunks = ChunkFramer::new(data, hash);

        loop {
            // The next frame is cut to the stream's credit and to the
            // payment-chunk boundary ([`ClientHandler::frame_target`]). The
            // opening frame is small, so the first byte leaves after a few chunk
            // groups of store read rather than a whole chunk. The cut precedes
            // the credit wait, so the store read overlaps the payer's round trip.
            let credit = paid.saturating_sub(delivered);
            let target = if delivered == 0 {
                self.first_frame_target(chunk_bytes, credit)
            } else {
                self.frame_target(delivered % chunk_bytes, chunk_bytes, credit)
            };
            let Some(frame) = waits
                .time(Wait::Store, chunks.next_frame_chunks(target))
                .await
                .map_err(|e| self.meter_frame_fault(e))?
            else {
                break;
            };
            let len = frame.total() as u64;

            // The node sends the frame only once the stream's credit covers it.
            match self
                .acquire_credit(
                    conn,
                    send,
                    recv,
                    reader,
                    &mut anchor,
                    &mut waits,
                    &mut guards,
                    hash,
                    lane_key,
                    lane,
                    rate_per_mb,
                    len,
                    delivered,
                    paid,
                )
                .await?
            {
                Acquired::Ready => {}
                Acquired::Stopped(stop) => {
                    return Ok(ServeEnd::Stopped {
                        stop,
                        bytes: delivered,
                    });
                }
            }

            waits
                .time(Wait::Send, self.write_chunk_payload_multi(send, &frame))
                .await?;
            if let Some(clock) = first_byte.take() {
                clock.record(&self.metrics);
            }
            delivered = delivered.saturating_add(len);
            self.shed.record_egress(len);
            self.metrics.bytes_served(len);
        }

        self.complete_delivery(
            send,
            recv,
            &mut waits,
            hash,
            delivered,
            Some(lane_key.deployment),
        )
        .await
    }

    /// End a delivery cleanly: the whole request is on the wire and every byte
    /// was paid for before it was sent. Writes `StreamEnd`, finishes the
    /// stream, and records the served request.
    async fn complete_delivery(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        waits: &mut ServeWaits,
        hash: Hash,
        delivered: u64,
        deployment: Option<super::Deployment>,
    ) -> anyhow::Result<ServeEnd> {
        waits
            .time(
                Wait::Send,
                self.write_message(send, &ClientMessage::StreamEnd),
            )
            .await?;
        let _ = send.finish();
        // Drain the client's send half to its FIN before `recv` drops, so its
        // finish is not stopped by the implicit `STOP_SENDING(0)` a dropped
        // `RecvStream` issues (see `drain_recv_to_fin`).
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
        // hash), on the same clean-completion edge as the ADR 040 hit sighting,
        // at the operator share of the pool that paid for it. The empty blob
        // sells no byte and names no pool.
        if let Some(deployment) = deployment {
            self.credit_warming_serve(deployment, hash, delivered);
        }
        Ok(ServeEnd::Completed { bytes: delivered })
    }
}

#[cfg(test)]
mod tests;
