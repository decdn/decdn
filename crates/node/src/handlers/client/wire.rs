//! Leaf helpers: rate clamping, response signing, wire writes, receipts, and the
//! frame cutting the serve loops pull their `ChunkData` payloads from.

use std::collections::VecDeque;

use anyhow::Context as _;
use bytes::Bytes;

use iroh::endpoint::WriteError;

use super::{
    B256, ClientHandler, ClientMessage, FrameError, Hash, MissPath, MissRefusal, RawReceipt,
    SendStream, ServeRejectReason, StreamError, StreamRequest, StreamResponse, StreamResponseBody,
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
    /// the returned `Refused(reason)` selects the per-reason metric in the
    /// dispatch sink (finer-grained than the wire for the reasons that collapse to
    /// `NotFound`, which share one code to avoid leaking channel existence), and
    /// `wire_error()` derives the wire `StreamError` (#876).
    ///
    /// A signing or encoding fault is this node's own: it returns the error, and
    /// the dispatch sink counts it as a node fault. A write that fails because the
    /// peer already left still ends as `Refused`: the refusal is the stream's
    /// outcome whether or not the peer reads it.
    ///
    /// `rate_per_mb` is the node's quoted price, echoed into the signed refusal
    /// so a requester sees the same rate whether it is admitted or declined. It
    /// is a required argument rather than something this function reads from
    /// `self`, so a refusal always carries the exact price the caller quoted for
    /// this request and cannot silently diverge from it.
    pub(super) async fn respond_error(
        &self,
        send: &mut SendStream,
        req: &StreamRequest,
        reason: ServeRejectReason,
        rate_per_mb: u64,
    ) -> anyhow::Result<super::outcome::ServeEnd> {
        let error = reason.wire_error();
        let body = StreamResponseBody {
            hash: req.hash,
            ok: false,
            rate_per_mb,
            total_bytes: 0,
            pool_id: req.pool_id,
            timestamp_us: req.timestamp_us,
        };
        let (resp, resp_ext) = self
            .sign_response(body, Some(error))
            .with_context(|| format!("sign the {reason:?} refusal"))?;
        tolerate_departed_peer(
            self.write_stream_response(send, &resp, &resp_ext).await,
            "refusal",
        )?;
        let _ = send.finish();
        Ok(super::outcome::ServeEnd::Refused(reason))
    }

    /// Send a terminal serve-miss refusal through [`Self::respond_error`], then
    /// log it once at info. The log line names the exit that refused (`path`),
    /// which the wire answer and the per-reason metric do not carry, so an
    /// operator can see why this node answered a request `NotFound` or
    /// `InternalError`. A [`MissPath::NoLane`] line passes the
    /// `no_lane_miss_log` throttle first, because a remote peer reaches that
    /// path at will.
    ///
    /// The line follows the send, so it appears only when the stream ends as
    /// this refusal. A signing or encoding fault, or a write that fails on this
    /// node's own stream state, returns the error without the line, with the
    /// path in its context: the dispatch sink counts that end as a node fault.
    /// A peer that left before reading the refusal still gets the line, because
    /// the refusal stands as the stream's outcome.
    pub(super) async fn respond_miss(
        &self,
        send: &mut SendStream,
        req: &StreamRequest,
        refusal: MissRefusal,
        rate_per_mb: u64,
    ) -> anyhow::Result<super::outcome::ServeEnd> {
        let end = self
            .respond_error(send, req, refusal.reason, rate_per_mb)
            .await
            .with_context(|| format!("send the {} serve-miss refusal", refusal.path.as_str()))?;
        match refusal.path {
            MissPath::NoLane => {
                if let Some(suppressed) = self.no_lane_miss_log.admit() {
                    log_miss_refusal(req, refusal, Some(suppressed));
                }
            }
            _ => log_miss_refusal(req, refusal, None),
        }
        Ok(end)
    }

    /// Write a mid-stream `StreamError { VoucherRejected }` and finish the
    /// stream **cleanly** — no QUIC reset — so the client can read the reason
    /// (ADR 005 §`VoucherRejected` semantics). `bundle` is the wallet-less
    /// resume watermark (issue #1481): callers pass `Some` only for a
    /// watermark-gated reason (`VoucherRejectReason::is_watermark_gated`), and
    /// only after verifying the rejected
    /// voucher's signature recovered to the lane's pinned `signer` — this method
    /// does not re-derive or re-check that gate, it trusts the caller.
    ///
    /// Every caller ends the stream with the stop this frame names, whether or not
    /// the peer is still there to read it, so a write that fails because the peer
    /// left returns `Ok`. An encoding fault or a write to a stream this node
    /// already closed is this node's own and returns the error.
    pub(super) async fn write_reject(
        &self,
        send: &mut SendStream,
        reason: VoucherRejectReason,
        bundle: Option<WatermarkBundle>,
    ) -> anyhow::Result<()> {
        tolerate_departed_peer(
            self.write_message(
                send,
                &ClientMessage::StreamError(StreamError::VoucherRejected { reason, bundle }),
            )
            .await,
            "voucher reject",
        )?;
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
    /// is what lets the cache-miss leg meter a peer-attributable failure as a
    /// client abandon (#856).
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
/// The dispatch sink's node-fault classifier: a `false` here is what routes an
/// error to `error!` and the node-fault counter.
pub(super) fn is_peer_attributable(e: &anyhow::Error) -> bool {
    e.is::<PeerFault>() || e.is::<ClientPaymentFault>()
}

/// Record `detail` as the `error` field of the current `serve_stream` span.
///
/// An end that returns `Ok(ServeEnd)` never reaches the dispatch sink's `error`
/// record, so a site that ends the stream on a peer-side failure records the
/// cause here. The trace backend then keeps the detail that the default
/// `RUST_LOG=info` filters out of the `debug!` line.
pub(super) fn record_stream_error(detail: impl std::fmt::Display) {
    tracing::Span::current().record("error", tracing::field::display(detail));
}

/// The info line [`ClientHandler::respond_miss`] writes for one serve-miss
/// refusal. `reason` is the same snake-case value the `serve_stream` span
/// records. A pull-leg miss adds its `cause`; a coverage-gate refusal adds the
/// pull range, the candidate count and the first uncovered chunk; a throttled
/// line adds the count of lines it `suppressed`. An absent field is not
/// recorded.
fn log_miss_refusal(req: &StreamRequest, refusal: MissRefusal, suppressed: Option<u64>) {
    let cause = match refusal.path {
        MissPath::PullLegMiss(miss) => Some(miss.as_str()),
        _ => None,
    };
    let shortfall = match refusal.path {
        MissPath::CoverageGate(shortfall) => Some(shortfall),
        _ => None,
    };
    tracing::info!(
        hash = %Hash::from_bytes(req.hash),
        byte_offset = req.byte_offset,
        byte_len = req.byte_len,
        reason = super::outcome::refusal_reason(refusal.reason),
        path = refusal.path.as_str(),
        cause,
        pull_offset = shortfall.map(|s| s.pull_offset),
        pull_len = shortfall.map(|s| s.pull_len),
        candidates = shortfall.map(|s| s.candidates),
        first_uncovered_chunk = shortfall.and_then(|s| s.first_uncovered_chunk),
        suppressed,
        "serve-miss: refusing the request"
    );
}

/// Absorb a write that fails because the peer already left.
///
/// The caller has already decided how the stream ends — a refusal or a stop —
/// and that outcome stands whether or not the peer reads the frame. A
/// peer-attributable failure logs at `debug!`, rides the span, and returns `Ok`.
/// Any other failure is this node's own and returns unchanged.
fn tolerate_departed_peer(result: anyhow::Result<()>, what: &str) -> anyhow::Result<()> {
    match result {
        Err(e) if is_peer_attributable(&e) => {
            record_stream_error(format_args!("{what} write failed: {e:#}"));
            tracing::debug!(error = %format_args!("{e:#}"), "{what} write failed");
            Ok(())
        }
        other => other,
    }
}

/// Attribute a framed-write failure.
///
/// An oversized frame is this node's own encoding bug — it never reached the
/// wire — so it carries no marker. So does a write to a stream this node already
/// finished or reset, and a 0-RTT rejection on a stream this node opened: both
/// are faults in its own stream state machine, as in [`write_chunk_error`]. The
/// transport wraps the stream's [`WriteError`] inside the `io::Error`, so it is
/// recovered by downcast. Everything else is the transport under a peer that
/// went away.
fn write_frame_error(e: FrameError) -> anyhow::Error {
    match e {
        FrameError::TooLarge(len) => anyhow::anyhow!("refusing to write a {len}-byte frame"),
        FrameError::Io(io)
            if matches!(
                io.get_ref()
                    .and_then(|inner| inner.downcast_ref::<WriteError>()),
                Some(WriteError::ClosedStream | WriteError::ZeroRttRejected)
            ) =>
        {
            anyhow::anyhow!("write failed: {io}")
        }
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
/// node: a hang-up, a stream reset, a read timeout, or a malformed message — a
/// peer that left or broke the protocol.
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
/// proof reads ([`BufferedProofReader::read`](super::BufferedProofReader::read)),
/// and the proof wait's faults in [`proof_wait`](super::proof_wait). A write or read fault this node caused —
/// an oversized frame it encoded, a write after its own `finish`/`reset` — stays
/// unmarked and reaches `error!`.
#[derive(Debug)]
pub(super) struct PeerFault;

impl std::fmt::Display for PeerFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("peer-side fault")
    }
}

impl std::error::Error for PeerFault {}

/// Marker for a serve-stream error that is a client-attributable payment fault:
/// a voucher that fails the advertised-rate check with zero bytes or an overflow.
/// An underpaying voucher is not here: it is a clean `Underpaid` wire reject. The
/// dispatch sink counts it as a rejected voucher.
///
/// The dispatch sink files an unmarked error under "node-side fault" at
/// `error!`. A client's payment fault is neither a node bug nor a disconnect, and
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

/// Marker for a serve-stream error that ended after at least one voucher
/// credited bytes on the stream.
///
/// The reference requester closes every stream with code `0`, and the node does
/// not read the code, so it cannot see why a peer left. It can see how far the
/// stream got. A peer that leaves before paying declined the quote — the routine
/// shape of a header handshake — and a peer that leaves after paying abandoned a
/// delivery. Both serve loops attach
/// this marker at their outer boundary when their paid-byte count is nonzero,
/// and the dispatch sink reads it to split a peer-attributable end between
/// `decdn_serve_stream_client_declined_total` and
/// `decdn_serve_stream_client_abandoned_total`. Attach it with
/// [`anyhow::Error::context`] and recover it with `anyhow::Error::is`, or with
/// `anyhow::Error::downcast_ref` to read how much the vouchers credited.
#[derive(Debug)]
pub(super) struct PaidProgress {
    /// The wire bytes accepted vouchers credited on the stream.
    pub(super) wire_bytes: std::num::NonZeroU64,
}

impl std::fmt::Display for PaidProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("after payment")
    }
}

impl std::error::Error for PaidProgress {}

/// Tag a serve loop's error with [`PaidProgress`] when `paid` — the wire bytes
/// that accepted vouchers credited on the stream — is nonzero.
pub(super) fn tag_paid_progress(e: anyhow::Error, paid: u64) -> anyhow::Error {
    match std::num::NonZeroU64::new(paid) {
        Some(wire_bytes) => e.context(PaidProgress { wire_bytes }),
        None => e,
    }
}

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
mod tests;
