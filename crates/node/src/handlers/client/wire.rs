//! Leaf helpers: rate clamping, response signing, wire writes, receipts, and the
//! frame cutting the serve loops pull their `ChunkData` payloads from.

use std::collections::VecDeque;

use bytes::Bytes;

use iroh::endpoint::WriteError;

use super::{
    B256, ClientHandler, ClientMessage, FrameError, Hash, RawReceipt, SendStream,
    ServeRejectReason, StreamError, StreamRequest, StreamResponse, StreamResponseBody,
    StreamResponseExt, StreamSlashData, U256, VoucherRejectReason, WatermarkBundle, encode_message,
    write_frame,
};

impl ClientHandler {
    /// Enqueue one audit receipt for a served-and-paid chunk (issues
    /// #248, #803). Called only after the proof advanced the lane watermark in
    /// memory, so a dropped receipt is non-fatal — the payment stands
    /// regardless, and the periodic lane flush mirrors the watermark to disk.
    ///
    /// The receipt is handed to [`ReceiptSink::record`](super::ReceiptSink::record), a **non-blocking**
    /// enqueue: the actual `write_all` runs off the hot path in the background
    /// receipt writer, so this never blocks before delivery continues and a slow or
    /// full disk cannot back-pressure delivery (the bug in #803). Receipts are
    /// enqueued in voucher-acceptance order and the single writer drains them FIFO,
    /// so the log reads in acceptance order and a graceful shutdown writes the tail
    /// (see [`crate::receipt_log`] §Durability).
    ///
    /// What crosses the seam is a [`RawReceipt`]: the two 64-char hex renders,
    /// the `uint256` decimal render, and the `SystemTime::now()` read all happen
    /// downstream in the background writer, not here (#1792 item 2). So on the
    /// delivery path this is a few `Copy` field moves and a non-blocking
    /// `try_send` — no allocation, no formatting, no syscall.
    ///
    /// The `voucher_amount` is widened to a `uint256` from the `u64` wire
    /// amount (the pool voucher's cumulative amount — its sole ordering key,
    /// there is no nonce); `client_node_id` is the iroh node id of the paying
    /// peer; `delta_bytes` is the bytes this voucher covers.
    pub(super) fn record_receipt(
        &self,
        hash: Hash,
        delta_bytes: u64,
        client_node_id: B256,
        wire_amount: u64,
    ) {
        let receipt = RawReceipt::new(hash, delta_bytes, client_node_id.0, U256::from(wire_amount));
        self.receipt_sink.record(receipt);
    }

    /// Load the configured rate and raise it to the delivery floor before
    /// signing a `StreamResponse`, logging a warning and incrementing
    /// `rate_bounds_clamp_events` on any clamp (ADR 005 §Rate bounds — the same
    /// clamp-and-warn the probe handler applies before signing a `ProbeResponse`).
    pub(super) fn clamped_rate(&self) -> u64 {
        let raw_rate = self.rate_per_mb;
        let (rate_per_mb, floor) = self.rate_bounds.raise_to_floor(raw_rate);
        if rate_per_mb != raw_rate {
            self.metrics.rate_bounds_clamped();
            tracing::warn!(
                raw_rate,
                clamped = rate_per_mb,
                floor,
                "rate_per_mb raised to the delivery floor before signing StreamResponse"
            );
        }
        rate_per_mb
    }

    /// Sign a `StreamResponse` body and assemble the frozen base plus its
    /// unsigned extension (ADR 013 §Tier 1). `error` rides in the extension, so
    /// the pair must be written together — see [`Self::write_stream_response`].
    pub(super) fn sign_response(
        &self,
        body: StreamResponseBody,
        error: Option<StreamError>,
    ) -> anyhow::Result<(StreamResponse, StreamResponseExt)> {
        let slash_sig = StreamSlashData::from_response_body(&body)
            .sign(self.eth_signer.as_ref(), &self.slash_domain)
            .map_err(|e| anyhow::anyhow!("stream slash_sig signing failed: {e}"))?
            .as_bytes()
            .to_vec();
        Ok((
            StreamResponse { body, slash_sig },
            StreamResponseExt { error },
        ))
    }

    /// Write a `StreamResponse` and its trailing extension as one frame.
    ///
    /// The two-phase encode is why this exists rather than a plain
    /// [`Self::write_message`]: the base and the extension are adjacent postcard
    /// values, and a receiver that predates a future extension field stops at the
    /// end of the base and discards the rest (ADR 013 §Tier 1).
    pub(super) async fn write_stream_response(
        &self,
        send: &mut SendStream,
        resp: &StreamResponse,
        ext: &StreamResponseExt,
    ) -> anyhow::Result<()> {
        let payload = decdn_protocol::encode_stream_response(resp, Some(ext))
            .map_err(|e| anyhow::anyhow!("encode stream response: {e}"))?;
        self.write_payload(send, &payload).await
    }

    /// Send a signed `StreamResponse { ok: false, error }` (delivery-side
    /// failure), then finish the stream. `reason` is the single source of truth:
    /// it both selects the per-reason metric (finer-grained than the wire for the
    /// seven `NotFound` cases, which collapse to one code to avoid leaking channel
    /// existence) and derives the wire `StreamError` via `wire_error()` (#876).
    /// The metric
    /// is bumped before the network write so a refusal is counted even if the
    /// client has already gone and the write fails.
    ///
    /// `rate_per_mb` is a required argument rather than something this function
    /// computes, and that is the whole point: [`Self::clamped_rate`] is
    /// side-effecting — it bumps `rate_bounds_clamped` and warns when the
    /// configured rate sits below the on-chain delivery floor — so a caller that
    /// priced the request and then refused would double-count it (#1518). Taking
    /// the price as a parameter is what stops this function recomputing it
    /// implicitly — which is the shape the bug took. It does not make the invariant
    /// fully type-checked: a caller can still pass `self.clamped_rate()` inline, or
    /// `0`, or another request's rate. What holds it today is that `serve_stream`
    /// has the crate's only production `clamped_rate()` call and threads that one
    /// value everywhere, which is a grep-verified property, not a typed one.
    pub(super) async fn respond_error(
        &self,
        send: &mut SendStream,
        req: &StreamRequest,
        reason: ServeRejectReason,
        rate_per_mb: u64,
    ) -> anyhow::Result<()> {
        match reason {
            ServeRejectReason::EvictedSinceProbe => {
                self.metrics.serve_stream_rejected_evicted_since_probe();
            }
            ServeRejectReason::CacheMiss => self.metrics.serve_stream_rejected_cache_miss(),
            ServeRejectReason::InternalError => self.metrics.serve_stream_rejected_internal_error(),
            ServeRejectReason::BlobTooLarge => self.metrics.serve_stream_rejected_blob_too_large(),
            ServeRejectReason::UnknownChannel => {
                self.metrics.serve_stream_rejected_unknown_lane();
            }
            ServeRejectReason::OwnerMismatch => self.metrics.serve_stream_rejected_owner_mismatch(),
            ServeRejectReason::InsufficientDeposit => {
                self.metrics.serve_stream_rejected_insufficient_deposit();
            }
            ServeRejectReason::LaneAtCapacity => {
                self.metrics.serve_stream_rejected_lane_at_capacity();
            }
            ServeRejectReason::LoadShedHit => self.metrics.serve_stream_rejected_load_shed_hit(),
            ServeRejectReason::LoadShedMiss => self.metrics.serve_stream_rejected_load_shed_miss(),
            ServeRejectReason::RangeNotSatisfiable => {
                self.metrics.serve_stream_rejected_range_not_satisfiable();
            }
            ServeRejectReason::HashDenied => self.metrics.serve_stream_rejected_hash_denied(),
            ServeRejectReason::ChainHashDenied => {
                self.metrics.serve_stream_rejected_chain_hash_denied();
            }
            ServeRejectReason::OriginDenied => self.metrics.serve_stream_rejected_origin_denied(),
            ServeRejectReason::ForeignNamespaceDeclined => {
                self.metrics.serve_stream_rejected_foreign_declined();
            }
        }
        let error = reason.wire_error();
        let body = StreamResponseBody {
            hash: req.hash,
            ok: false,
            rate_per_mb,
            total_bytes: 0,
            pool_id: req.pool_id,
            timestamp_us: req.timestamp_us,
        };
        let (resp, resp_ext) = self.sign_response(body, Some(error))?;
        self.write_stream_response(send, &resp, &resp_ext).await?;
        let _ = send.finish();
        Ok(())
    }

    /// Write a mid-stream `StreamError { VoucherRejected }` and finish the
    /// stream **cleanly** — no QUIC reset — so the client can read the reason
    /// (ADR 005 §`VoucherRejected` semantics). `bundle` is the wallet-less
    /// resume watermark (issue #1481): callers pass `Some` only for the three
    /// watermark-gated regression/exhaustion reasons (`AmountRegression`,
    /// `BytesRegression`, `SpendingCapExhausted`), and only after verifying the rejected
    /// voucher's signature recovered to the lane's pinned `signer` — this method
    /// does not re-derive or re-check that gate, it trusts the caller.
    pub(super) async fn write_reject(
        &self,
        send: &mut SendStream,
        reason: VoucherRejectReason,
        bundle: Option<WatermarkBundle>,
    ) -> anyhow::Result<()> {
        self.write_message(
            send,
            &ClientMessage::StreamError(StreamError::VoucherRejected { reason, bundle }),
        )
        .await?;
        let _ = send.finish();
        Ok(())
    }

    pub(super) async fn write_message(
        &self,
        send: &mut SendStream,
        msg: &ClientMessage,
    ) -> anyhow::Result<()> {
        let payload = encode_message(msg).map_err(|e| anyhow::anyhow!("encode failed: {e}"))?;
        self.write_payload(send, &payload).await
    }

    /// Frame an already-encoded payload. [`Self::write_message`] delegates here
    /// after encoding a single [`ClientMessage`].
    pub(super) async fn write_payload(
        &self,
        send: &mut SendStream,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        write_frame(send, payload).await.map_err(write_frame_error)
    }

    /// Meter a serve-side frame-accounting fault and hand the error back unchanged.
    ///
    /// Anything else — a store fault, an encode fault, a peer disconnect — passes
    /// through untouched, so the counter stays a node-bug report rather than a
    /// mixed-cause one. Wrap every `?` that can surface a [`FrameAccountingFault`].
    pub(super) fn meter_frame_fault(&self, e: anyhow::Error) -> anyhow::Error {
        if e.is::<FrameAccountingFault>() {
            self.metrics.serve_frame_accounting_fault();
        }
        e
    }

    /// Put one already-assembled `ChunkData` frame on the wire.
    ///
    /// The frame is already assembled, so no framing bug can surface here — which
    /// is what lets the cache-miss leg meter a failure as a client abandon (#856).
    /// Build `bufs` with [`chunk_frame_bufs`] first. A write that fails because the
    /// peer went away carries [`PeerFault`]; a write against this node's own
    /// finished or reset stream does not, since that one is a node bug.
    ///
    /// `write_all_chunks` empties the `Bytes` it writes, so it takes `bufs` by value:
    /// what it leaves behind still looks like a frame and is all zero-length, and no
    /// caller has any use for that. On `Err` part of the frame may already be on the
    /// wire, so the caller abandons the delivery rather than retrying.
    pub(super) async fn write_chunk_bufs(
        &self,
        send: &mut SendStream,
        mut bufs: Vec<Bytes>,
    ) -> anyhow::Result<()> {
        send.write_all_chunks(&mut bufs)
            .await
            .map_err(write_chunk_error)
    }

    /// Assemble one `ChunkData` frame and write it. The cache-hit leg's door;
    /// the miss leg splits the two halves so it can tell them apart when metering.
    ///
    /// # Errors
    ///
    /// Either a framing fault from [`chunk_frame_bufs`] — a node-side bug — or an I/O
    /// error from the write. The hit leg does not distinguish them because it meters
    /// neither; the miss leg calls the two halves separately so that it can.
    pub(super) async fn write_chunk_payload_multi(
        &self,
        send: &mut SendStream,
        frame: &FrameChunks,
    ) -> anyhow::Result<()> {
        let bufs = chunk_frame_bufs(frame).map_err(|e| self.meter_frame_fault(e))?;
        self.write_chunk_bufs(send, bufs).await
    }
}

/// The vectored write buffers for one `ChunkData` frame: the header, then the
/// payload chunks untouched.
///
/// The payload is the bao-verified stream bytes (content + interleaved proof nodes
/// per ADR 038), already split across the `Bytes` items one frame spans. Only the
/// header is copied — onto the stack, then once into a small `Bytes` — so the
/// payload reaches the wire without a coalescing copy. The bytes this produces
/// equal `encode_chunk_frame(&concat)` + `write_frame`, pinned by
/// `chunk_frame_bufs_match_the_single_buffer_encoder`.
///
/// A [`FrameChunks`] is non-empty and its `total` counts exactly the bytes in its
/// chunks: [`FrameQueue::cut`] is its only constructor and computes the total from
/// the same items it puts in the frame, so the empty-payload and count-mismatch
/// refusals live in that construction invariant, not here. `total` is what the
/// header declares to the client *and* what the serve loop bills for.
///
/// # Errors
///
/// A `total` past [`decdn_protocol::framing::MAX_MESSAGE_SIZE`] is refused one door
/// further down, by `encode_chunk_frame_headers`' own ADR 013 ceiling; the ADR 005
/// empty-frame floor lives there too. Neither is reachable while
/// `payment.frame_target_bytes` is capped at one payment chunk.
pub(super) fn chunk_frame_bufs(frame: &FrameChunks) -> anyhow::Result<Vec<Bytes>> {
    let total_len = frame.total;
    let mut hdr = [0u8; decdn_protocol::client::CHUNK_FRAME_HEADERS_MAX];
    let hdr_len =
        decdn_protocol::client::encode_chunk_frame_headers(total_len, &mut hdr).map_err(|e| {
            tracing::error!(total_len, error = %e, "chunk header encoder refused a frame");
            anyhow::Error::new(FrameAccountingFault).context(format!("encode chunk header: {e}"))
        })?;
    let Some(header) = hdr.get(..hdr_len) else {
        tracing::error!(
            hdr_len,
            cap = decdn_protocol::client::CHUNK_FRAME_HEADERS_MAX,
            "chunk header encoder reported more bytes than its buffer holds"
        );
        return Err(anyhow::Error::new(FrameAccountingFault)
            .context(format!("chunk header length {hdr_len} exceeds its buffer")));
    };
    let mut bufs = Vec::with_capacity(frame.chunks.len().saturating_add(1));
    bufs.push(Bytes::copy_from_slice(header));
    bufs.extend_from_slice(&frame.chunks);
    Ok(bufs)
}

/// Whether a finished serve stream's error is the peer's doing rather than this
/// node's — a [`PeerFault`] or a [`ClientPaymentFault`].
///
/// The dispatch sink's sole classifier: a `false` here is what routes an error to
/// `error!` and the node-fault counter.
pub(super) fn is_peer_attributable(e: &anyhow::Error) -> bool {
    e.is::<PeerFault>() || e.is::<ClientPaymentFault>()
}

/// Attribute a framed-write failure.
///
/// An oversized frame is this node's own encoding bug — it never reached the
/// wire — so it carries no marker. Everything else is the transport under a peer
/// that went away.
fn write_frame_error(e: FrameError) -> anyhow::Error {
    match e {
        FrameError::TooLarge(len) => anyhow::anyhow!("refusing to write a {len}-byte frame"),
        e => anyhow::Error::new(PeerFault).context(format!("write failed: {e}")),
    }
}

/// Attribute a vectored `ChunkData` write failure.
///
/// Writing to a stream this node already finished or reset, and a 0-RTT rejection
/// on a stream this node opened, are both faults in this node's own stream state
/// machine, so they carry no marker. A stop or a lost connection is the peer.
fn write_chunk_error(e: WriteError) -> anyhow::Error {
    match e {
        e @ (WriteError::ClosedStream | WriteError::ZeroRttRejected) => {
            anyhow::anyhow!("write chunk frame: {e}")
        }
        e => anyhow::Error::new(PeerFault).context(format!("write chunk frame: {e}")),
    }
}

/// A serve-side refusal caused by the node's own byte accounting rather than by the
/// store, the encoder, or the peer: a zero frame target, or a header the encoder
/// refused.
///
/// Every one of those is correct to refuse and therefore silent — the delivery just
/// ends, which reads to an operator as a client that hung up. Carrying the cause as a
/// type lets both serve loops meter it (`serve_frame_accounting_fault`) without
/// misfiling a store fault or a peer disconnect as a node bug. Attach it with
/// [`anyhow::Error::context`] and recover it with `anyhow::Error::is`.
#[derive(Debug)]
pub(super) struct FrameAccountingFault;

impl std::fmt::Display for FrameAccountingFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("serve-side frame accounting fault")
    }
}

impl std::error::Error for FrameAccountingFault {}

/// Marker for a serve-stream error attributable to the peer rather than to this
/// node: a hang-up, a stream reset, a read timeout, or a malformed request.
///
/// The dispatch sink logs every `serve_stream` error at one of two levels. A node
/// bug — an encode fault, an alignment error, a store fault, a framing fault —
/// is the operator's only signal that a delivery was abandoned, so it belongs at
/// `error!`. A peer that hangs up or sends garbage is routine and belongs at
/// `debug!`, below the project's default `RUST_LOG=info`. This marker is what
/// separates the two. Attach it with [`anyhow::Error::context`] and recover it
/// with `anyhow::Error::is`.
///
/// The attach sites are the peer-attributable arms of the wire writes
/// ([`ClientHandler::write_payload`], [`ClientHandler::write_chunk_bufs`]), the
/// proof reads ([`BufferedProofReader::read`](super::BufferedProofReader::read)
/// and its gather timeout), and the opening request read. A write or read fault
/// this node caused — an oversized frame it encoded, a write after its own
/// `finish`/`reset` — stays unmarked and reaches `error!`.
#[derive(Debug)]
pub(super) struct PeerFault;

impl std::fmt::Display for PeerFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("peer-side fault")
    }
}

impl std::error::Error for PeerFault {}

/// Marker for a serve-stream error that is a client-attributable payment fault:
/// a voucher that underpays for its delivered-byte span, one that fails the
/// advertised-rate check, or a payer that spends its per-chunk proof budget
/// without settling anything.
///
/// The dispatch sink files an unmarked error under "node-side fault" at
/// `error!`. A client's underpayment is neither a node bug nor a disconnect, and
/// under a misbehaving client it is noisy, so it carries its own marker and lands
/// at `debug!`. Attach it with [`anyhow::Error::context`] and recover it with
/// `anyhow::Error::is`.
///
/// It marks only the client-attributable stops. The defensive node-invariant
/// bail beside them — a `RetrySignal` from a path that touches no store — stays
/// unmarked, because that one IS a node bug.
#[derive(Debug)]
pub(super) struct ClientPaymentFault;

impl std::fmt::Display for ClientPaymentFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("client payment fault")
    }
}

impl std::error::Error for ClientPaymentFault {}

/// A queue of not-yet-framed export bytes plus its running byte count, holding the
/// two in step so a frame is cut from a count that always matches the bytes present.
///
/// Both serve framers (`ChunkFramer` on the cache-hit leg and
/// [`super::serve_encoder::CoherentFrameProducer`] on the miss leg) buffer export
/// items here until they hold a frame's worth. [`Self::push`] drops empty items, so
/// `queued == 0` holds exactly when `queue` is empty — the desync the framers once
/// guarded by hand cannot arise. The bytes stay `Bytes`-native so a spanning frame
/// rides a vectored QUIC write without a coalescing copy.
pub(super) struct FrameQueue {
    /// Export bytes not yet cut into a frame, as reference-counted chunks.
    queue: VecDeque<Bytes>,
    /// Total bytes currently queued. Equals the sum of the chunk lengths.
    queued: usize,
}

impl FrameQueue {
    pub(super) const fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            queued: 0,
        }
    }

    /// Queue one export item, keeping the byte count in step. An empty item is
    /// dropped: admitting it would let `queued == 0` coexist with a non-empty queue,
    /// the desync the type exists to rule out.
    pub(super) fn push(&mut self, b: Bytes) {
        let len = b.len();
        if len > 0 {
            self.queued = self.queued.saturating_add(len);
            self.queue.push_back(b);
        }
    }

    /// The queued byte count — what a `target`-sized cut measures itself against.
    pub(super) const fn len(&self) -> usize {
        self.queued
    }

    /// Whether the queue holds no bytes. Equal to `queue.is_empty()` by
    /// construction, since empties never enter and the count moves with the bytes.
    /// The framers read the same fact through [`Self::cut`] returning `None`, so this
    /// backs the tests that pin the count/queue equivalence directly.
    #[cfg(test)]
    pub(super) const fn is_empty(&self) -> bool {
        self.queued == 0
    }

    /// Drop every queued byte and reset the count. The fault path: a framer that
    /// abandons its delivery clears the buffered remainder so no later cut bills for
    /// a transfer that can never complete.
    pub(super) fn clear(&mut self) {
        self.queue.clear();
        self.queued = 0;
    }

    /// Cut up to `target` bytes off the front of the queue into the `Bytes` slices
    /// that make up one wire frame.
    ///
    /// Nothing is copied: whole items move across, and a frame that ends mid-item
    /// splits it with `Bytes::split_to`, which reslices the same allocation.
    ///
    /// `None` only when the queue is empty. For a caller passing `target >= 1`,
    /// `target.min(queued) == 0` implies `queued == 0`, so `None` is the end of the
    /// blob and never a bookkeeping fault — that path is unrepresentable now the
    /// count is held in step with the bytes.
    pub(super) fn cut(&mut self, target: usize) -> Option<FrameChunks> {
        let mut remaining = target.min(self.queued);
        let mut chunks: Vec<Bytes> = Vec::with_capacity(self.queue.len().min(remaining));
        let mut total = 0usize;
        while remaining > 0 {
            let front_len = self.queue.front().map_or(0, Bytes::len);
            // Empties never enter the queue, so a zero front only shows up if the
            // queue is already empty, which `remaining > 0` rules out; the branch is
            // dead but keeps the loop total on well-defined ground.
            if front_len == 0 {
                break;
            }
            if front_len <= remaining {
                let Some(bytes) = self.queue.pop_front() else {
                    break;
                };
                remaining -= front_len;
                total = total.saturating_add(front_len);
                self.queued -= front_len;
                chunks.push(bytes);
            } else {
                // Split before touching the count: `split_to` mutates the queue, so
                // the bytes it takes must be accounted whatever happens next.
                let Some(front) = self.queue.front_mut() else {
                    break;
                };
                let taken = front.split_to(remaining);
                let taken_len = taken.len();
                total = total.saturating_add(taken_len);
                self.queued -= taken_len;
                chunks.push(taken);
                remaining = 0;
            }
        }
        if chunks.is_empty() {
            None
        } else {
            Some(FrameChunks { chunks, total })
        }
    }
}

/// One wire frame's worth of payload: the `Bytes` slices that make it up and their
/// total byte count, cut off a [`FrameQueue`].
///
/// [`FrameQueue::cut`] is the only constructor, and it computes `total` from the
/// same items it moves into `chunks`, so `total` equals the sum of the chunk lengths
/// by construction. That is what lets [`chunk_frame_bufs`] drop the empty-payload and
/// count-mismatch guards: a `FrameChunks` whose header would misdeclare its own bytes
/// cannot be built.
#[derive(Debug)]
pub(super) struct FrameChunks {
    chunks: Vec<Bytes>,
    total: usize,
}

impl FrameChunks {
    /// The frame's total byte count — what its header declares to the client and
    /// what the serve loop bills for.
    pub(super) const fn total(&self) -> usize {
        self.total
    }

    /// The raw payload slices, for tests that assert the zero-copy split.
    #[cfg(test)]
    pub(super) fn chunks(&self) -> &[Bytes] {
        &self.chunks
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests
mod tests {
    use bytes::Bytes;

    use super::{
        ClientPaymentFault, FrameError, FrameQueue, PeerFault, WriteError, chunk_frame_bufs,
        is_peer_attributable, write_chunk_error, write_frame_error,
    };

    /// The whole classification rests on a marker staying recoverable under the
    /// context layers callers stack on the way up to the dispatch sink. This file
    /// re-wraps errors with `anyhow!("...: {e}")` in many places; the day one of
    /// those lands on a marked error the marker is gone, and the only thing that
    /// would notice is this test.
    #[test]
    fn a_marker_survives_the_context_layers_stacked_above_it() {
        let e = anyhow::Error::new(PeerFault)
            .context("write failed: connection lost")
            .context("serve leg gave up");
        assert!(is_peer_attributable(&e), "marker lost under context");
        // The alternate form is what the sink logs, so the cause must still read.
        let rendered = format!("{e:#}");
        assert!(
            rendered.contains("connection lost"),
            "cause dropped from the log line: {rendered}"
        );
    }

    /// An unmarked error is a node fault. Nothing else may reach `debug!`.
    #[test]
    fn an_unmarked_error_is_a_node_fault() {
        assert!(!is_peer_attributable(&anyhow::anyhow!("store read failed")));
        assert!(is_peer_attributable(
            &anyhow::Error::new(ClientPaymentFault).context("voucher underpays")
        ));
    }

    /// A frame this node encoded too large never reached the wire, so it is this
    /// node's bug — not the peer-disconnect shape the rest of `write_frame`'s
    /// errors carry.
    #[test]
    fn an_oversized_frame_is_a_node_fault_but_a_write_io_error_is_the_peer() {
        let too_large = write_frame_error(FrameError::TooLarge(1 << 30));
        assert!(
            !is_peer_attributable(&too_large),
            "an oversized frame must reach error!: {too_large}"
        );

        let io = write_frame_error(FrameError::Io(std::io::Error::other("reset by peer")));
        assert!(is_peer_attributable(&io), "a write I/O error is the peer");
    }

    /// Writing to a stream this node already finished is a fault in this node's own
    /// stream state machine; a peer that stopped the stream is not.
    #[test]
    fn a_write_after_close_is_a_node_fault_but_a_peer_stop_is_not() {
        let closed = write_chunk_error(WriteError::ClosedStream);
        assert!(
            !is_peer_attributable(&closed),
            "a write after our own finish must reach error!: {closed}"
        );

        let stopped = write_chunk_error(WriteError::Stopped(iroh::endpoint::VarInt::from_u32(0)));
        assert!(
            is_peer_attributable(&stopped),
            "a peer stop is the peer: {stopped}"
        );
    }

    /// The vectored buffers must lay down exactly the bytes the single-buffer
    /// encoder would, whatever the payload is split into.
    ///
    /// This is the claim the whole zero-copy path rests on: the serve loops build no
    /// `ChunkData` frame at all, so nothing else proves the header they emit matches
    /// `encode_chunk_frame` + `write_frame`. The chunk
    /// counts span what a real frame looks like — one queued item, a handful, and
    /// the ~130 a default 1 MiB frame spans over 64 B proof nodes and 16 KiB leaves.
    #[tokio::test]
    async fn chunk_frame_bufs_match_the_single_buffer_encoder()
    -> Result<(), Box<dyn std::error::Error>> {
        for count in [1usize, 2, 4, 5, 130] {
            for chunk_len in [1usize, 64, 16 * 1024] {
                let chunks: Vec<Bytes> = (0..count)
                    .map(|i| Bytes::from(vec![u8::try_from(i % 251).unwrap_or(0); chunk_len]))
                    .collect();
                let total: usize = chunks.iter().map(Bytes::len).sum();

                // Build the frame the only way production does: push the items onto a
                // `FrameQueue`, then cut all of them into one `FrameChunks`.
                let mut fq = FrameQueue::new();
                for c in &chunks {
                    fq.push(c.clone());
                }
                let frame = fq.cut(total).expect("a non-empty queue cuts a frame");

                let bufs = chunk_frame_bufs(&frame)?;
                let mut via_bufs = Vec::new();
                for b in &bufs {
                    via_bufs.extend_from_slice(b);
                }

                let mut concat = Vec::with_capacity(total);
                for c in &chunks {
                    concat.extend_from_slice(c);
                }
                let postcard = decdn_protocol::encode_chunk_frame(&concat)?;
                let mut via_encoder = Vec::new();
                decdn_protocol::write_frame(&mut via_encoder, &postcard).await?;

                assert_eq!(
                    via_bufs, via_encoder,
                    "diverged at {count} chunks of {chunk_len} bytes"
                );
                assert_eq!(
                    bufs.len(),
                    count + 1,
                    "the payload must ride uncopied: one buf per chunk, plus the header"
                );
            }
        }
        Ok(())
    }

    /// `push` drops an empty item so the byte count never desyncs from the queue.
    /// This is what makes `is_empty()` (count `== 0`) and an empty `queue` the same
    /// statement, which is what lets `cut` treat `None` as the end of the blob rather
    /// than a bookkeeping fault.
    #[test]
    fn push_drops_empties_so_the_count_never_desyncs() {
        let mut fq = FrameQueue::new();
        assert!(fq.is_empty());
        assert_eq!(fq.len(), 0);

        fq.push(Bytes::new());
        assert!(fq.is_empty(), "an empty item leaves the queue empty");
        assert_eq!(fq.len(), 0, "and leaves the count at zero");

        fq.push(Bytes::from_static(b"abcd"));
        fq.push(Bytes::new());
        assert_eq!(fq.len(), 4, "the empty push between real ones is a no-op");
        assert!(!fq.is_empty());
    }

    /// Cutting a frame moves whole items and splits only the one the frame ends in,
    /// leaving `len()` equal to the bytes still queued.
    ///
    /// The chunk COUNTS are the load-bearing assertions. `total` is computed from the
    /// same lengths a sum over the result would re-add, so checking one against the
    /// other proves nothing; what the zero-copy path actually rests on is that a frame
    /// spanning two queued items arrives as two `Bytes` rather than one coalesced
    /// buffer. A `cut` rewritten to concatenate would satisfy every other assertion in
    /// this file.
    #[test]
    fn cut_cuts_at_the_target_and_keeps_the_remainder() {
        let mut fq = FrameQueue::new();
        fq.push(Bytes::from_static(b"aaaa"));
        fq.push(Bytes::from_static(b"bbbb"));
        assert_eq!(fq.len(), 8, "len tracks the pushed bytes");

        let frame = fq.cut(6).expect("6 of 8 bytes");
        assert_eq!(frame.total(), 6);
        assert_eq!(
            frame.chunks().len(),
            2,
            "a frame spanning two items must stay two uncopied slices"
        );
        assert_eq!(
            frame.chunks().concat(),
            b"aaaabb",
            "the cut is an in-order prefix of the queue"
        );
        assert_eq!(fq.len(), 2, "the split remainder stays queued");

        let rest = fq.cut(6).expect("the remainder");
        assert_eq!(rest.total(), 2, "a short final frame, not a padded one");
        assert_eq!(
            rest.chunks().len(),
            1,
            "the remainder is what is left of one item"
        );
        assert_eq!(rest.chunks().concat(), b"bb");
        assert_eq!(fq.len(), 0);

        assert!(fq.cut(6).is_none(), "an empty queue is the only `None`");
    }

    /// A frame that ends exactly on an item boundary takes whole items and splits
    /// nothing — the case where an off-by-one in the `front_len <= remaining` branch
    /// would show up as a spurious extra chunk or a dropped byte.
    #[test]
    fn cut_ends_on_an_item_boundary_without_splitting() {
        let mut fq = FrameQueue::new();
        for i in 0..4u8 {
            fq.push(Bytes::from(vec![i; 4]));
        }
        assert_eq!(fq.len(), 16);

        let frame = fq.cut(8).expect("two whole items");
        assert_eq!(frame.total(), 8);
        assert_eq!(frame.chunks().len(), 2, "two items moved whole, none split");
        assert_eq!(frame.chunks().concat(), [0, 0, 0, 0, 1, 1, 1, 1]);
        assert_eq!(fq.len(), 8, "the untouched items stay queued whole");
    }

    /// A target past everything queued yields one short frame of exactly what is
    /// there, not a parked call and not a padded frame.
    #[test]
    fn cut_takes_everything_when_the_target_exceeds_the_queue() {
        let mut fq = FrameQueue::new();
        fq.push(Bytes::from_static(b"ab"));
        fq.push(Bytes::from_static(b"cde"));
        assert_eq!(fq.len(), 5);

        let frame = fq.cut(1024).expect("all 5 bytes");
        assert_eq!(frame.total(), 5);
        assert_eq!(frame.chunks().len(), 2, "both items ride uncopied");
        assert_eq!(frame.chunks().concat(), b"abcde");
        assert!(fq.is_empty());
    }

    /// `cut` returns `None` only on an empty queue. Because `push` never admits an
    /// empty item, `len() == 0` and an empty queue are the same state, so the
    /// under-counting desync a hand-maintained counter could reach — which would end a
    /// truncated delivery with `StreamEnd` — is unrepresentable here by construction.
    #[test]
    fn cut_is_none_only_on_an_empty_queue() {
        let mut fq = FrameQueue::new();
        assert!(fq.cut(8).is_none(), "an empty queue cuts nothing");

        fq.push(Bytes::from_static(b"xy"));
        assert!(fq.cut(8).is_some(), "a non-empty queue always cuts");
        assert!(fq.is_empty());
        assert!(fq.cut(8).is_none(), "drained again, back to `None`");
    }
}
