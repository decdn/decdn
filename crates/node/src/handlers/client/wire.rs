//! Small leaf helpers: rate clamping, response signing, wire writes, receipts.

use bytes::Bytes;

use super::{
    B256, ClientHandler, ClientMessage, DownloadReceipt, Hash, SendStream, ServeRejectReason,
    StreamError, StreamRequest, StreamResponse, StreamResponseBody, StreamResponseExt,
    StreamSlashData, U256, VoucherRejectReason, WatermarkBundle, encode_message, write_frame,
};

impl ClientHandler {
    /// Enqueue one audit receipt for a served-and-paid chunk (issues
    /// #248, #803). Called only after the proof advanced the lane watermark in
    /// memory, so a dropped receipt is non-fatal — the payment stands
    /// regardless, and the periodic lane flush mirrors the watermark to disk.
    ///
    /// The receipt is handed to [`ReceiptSink::record`](super::ReceiptSink::record), a **non-blocking**
    /// enqueue: the actual `write_all` + `flush` runs off the hot path in the
    /// background receipt writer, so this never blocks before delivery continues and a
    /// slow or full disk cannot back-pressure delivery (the bug in #803).
    /// Receipts are enqueued in voucher-acceptance order and the single writer
    /// drains them FIFO, preserving the audit ordering and shutdown-tail
    /// guarantees CLAUDE.md / ADR 003 require.
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
        let voucher_amount = U256::from(wire_amount);
        let receipt = DownloadReceipt::new(
            &hash,
            delta_bytes,
            &client_node_id.0,
            voucher_amount,
            crate::payment_settlement::unix_now(),
        );
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
            redirect: None,
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
        write_frame(send, payload)
            .await
            .map_err(|e| anyhow::anyhow!("write failed: {e}"))
    }

    /// Write one `ChunkData` frame without copying the payload.
    ///
    /// The payload is the bao-verified stream bytes (content + interleaved proof
    /// nodes per ADR 038). The header (framing varint + discriminant + varint of
    /// payload length) is encoded on the stack (≤11 B) and sent alongside the
    /// payload `Bytes` via a single vectored QUIC write, so the payload is never
    /// copied into a heap buffer. The wire bytes are identical to
    /// `encode_chunk_frame(&payload)` + `write_frame`; this is a transport
    /// optimization, not a format change.
    #[allow(dead_code)]
    pub(super) async fn write_chunk_payload(
        &self,
        send: &mut SendStream,
        payload: Bytes,
    ) -> anyhow::Result<()> {
        let len = payload.len();
        let mut hdr = [0u8; decdn_protocol::client::CHUNK_FRAME_HEADERS_MAX];
        let hdr_len = decdn_protocol::client::encode_chunk_frame_headers(len, &mut hdr)
            .map_err(|e| anyhow::anyhow!("encode chunk header: {e}"))?;
        // Small header is copied once into a `Bytes` (≤11 B); the payload stays
        // reference-counted.
        let header = Bytes::copy_from_slice(hdr.get(..hdr_len).unwrap_or(&[]));
        let mut bufs = [header, payload];
        send.write_all_chunks(&mut bufs)
            .await
            .map_err(|e| anyhow::anyhow!("write chunk frame: {e}"))
    }

    /// Vectored variant of [`Self::write_chunk_payload`] for a payload that is
    /// already split across multiple `Bytes` chunks (e.g. the queued export
    /// items that make up one frame). The chunks are sent together with the
    /// header in a single vectored write, so no coalescing copy is needed.
    ///
    /// `payload_chunks` must sum to `total_len` and be non-empty; `total_len`
    /// is the `ChunkData` payload length the header encodes.
    pub(super) async fn write_chunk_payload_multi(
        &self,
        send: &mut SendStream,
        payload_chunks: &[Bytes],
        total_len: usize,
    ) -> anyhow::Result<()> {
        if payload_chunks.is_empty() {
            anyhow::bail!("refusing to serve an empty chunk payload");
        }
        let actual: usize = payload_chunks.iter().map(Bytes::len).sum();
        if actual != total_len {
            anyhow::bail!("payload_chunks sum {actual} does not match total_len {total_len}");
        }
        let mut hdr = [0u8; decdn_protocol::client::CHUNK_FRAME_HEADERS_MAX];
        let hdr_len = decdn_protocol::client::encode_chunk_frame_headers(total_len, &mut hdr)
            .map_err(|e| anyhow::anyhow!("encode chunk header: {e}"))?;
        let header = Bytes::copy_from_slice(hdr.get(..hdr_len).unwrap_or(&[]));
        // Avoid a heap allocation when the frame is built from only a few
        // `Bytes` chunks (common when the credit window is small or the frame
        // was cut from a single queued item). A stack array of 5 covers header
        // + up to 4 payload chunks; larger frames fall back to a `Vec`.
        if payload_chunks.len() <= 4 {
            let mut stack: [Bytes; 5] = std::array::from_fn(|_| Bytes::new());
            if let Some(slot) = stack.get_mut(0) {
                *slot = header;
            }
            for (i, chunk) in payload_chunks.iter().enumerate() {
                if let Some(slot) = stack.get_mut(1usize.saturating_add(i)) {
                    *slot = chunk.clone();
                }
            }
            let total_bufs = 1usize.saturating_add(payload_chunks.len());
            if let Some(slice) = stack.get_mut(..total_bufs) {
                send.write_all_chunks(slice)
                    .await
                    .map_err(|e| anyhow::anyhow!("write chunk frame multi: {e}"))?;
            }
        } else {
            let mut bufs: Vec<Bytes> = Vec::with_capacity(payload_chunks.len().saturating_add(1));
            bufs.push(header);
            bufs.extend_from_slice(payload_chunks);
            send.write_all_chunks(&mut bufs[..])
                .await
                .map_err(|e| anyhow::anyhow!("write chunk frame multi: {e}"))?;
        }
        Ok(())
    }
}
