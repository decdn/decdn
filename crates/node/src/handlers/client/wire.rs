//! Small leaf helpers: rate clamping, response signing, wire writes, receipts.

use std::collections::VecDeque;

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
    /// enqueue: the actual `write_all` runs off the hot path in the background
    /// receipt writer, so this never blocks before delivery continues and a slow or
    /// full disk cannot back-pressure delivery (the bug in #803). Receipts are
    /// enqueued in voucher-acceptance order and the single writer drains them FIFO,
    /// so the log reads in acceptance order and a graceful shutdown writes the tail
    /// (see [`crate::receipt_log`] §Durability).
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

    /// Put one already-assembled `ChunkData` frame on the wire.
    ///
    /// Every error is an I/O error, which is what lets the cache-miss leg meter a
    /// failure here as a client abandon (#856) without misfiling a node-side framing
    /// bug as one. Build `bufs` with [`chunk_frame_bufs`] first.
    ///
    /// `write_all_chunks` empties the `Bytes` it writes, so `bufs` is spent
    /// afterwards.
    pub(super) async fn write_chunk_bufs(
        &self,
        send: &mut SendStream,
        bufs: &mut [Bytes],
    ) -> anyhow::Result<()> {
        send.write_all_chunks(bufs)
            .await
            .map_err(|e| anyhow::anyhow!("write chunk frame: {e}"))
    }

    /// Assemble one `ChunkData` frame and write it. The cache-hit leg's door;
    /// the miss leg splits the two halves so it can tell them apart when metering.
    pub(super) async fn write_chunk_payload_multi(
        &self,
        send: &mut SendStream,
        payload_chunks: &[Bytes],
        total_len: usize,
    ) -> anyhow::Result<()> {
        let mut bufs = chunk_frame_bufs(payload_chunks, total_len)?;
        self.write_chunk_bufs(send, &mut bufs).await
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
/// # Errors
///
/// An empty `payload_chunks`, or one whose lengths do not sum to `total_len`. Both
/// mean the framer's byte accounting disagrees with the bytes it handed over —
/// `total_len` is what the header declares to the client *and* what the serve loop
/// bills for, so a mismatch must not reach the wire. `total_len == 0` is refused
/// one door further down, by the encoder's own ADR 005 non-empty floor.
pub(super) fn chunk_frame_bufs(
    payload_chunks: &[Bytes],
    total_len: usize,
) -> anyhow::Result<Vec<Bytes>> {
    if payload_chunks.is_empty() {
        tracing::error!(total_len, "framer handed over an empty chunk payload");
        anyhow::bail!("refusing to serve an empty chunk payload");
    }
    let actual: usize = payload_chunks.iter().map(Bytes::len).sum();
    if actual != total_len {
        tracing::error!(
            actual,
            total_len,
            "framer byte count disagrees with its chunks"
        );
        anyhow::bail!("payload_chunks sum {actual} does not match total_len {total_len}");
    }
    let mut hdr = [0u8; decdn_protocol::client::CHUNK_FRAME_HEADERS_MAX];
    let hdr_len = decdn_protocol::client::encode_chunk_frame_headers(total_len, &mut hdr)
        .map_err(|e| anyhow::anyhow!("encode chunk header: {e}"))?;
    let Some(header) = hdr.get(..hdr_len) else {
        anyhow::bail!("chunk header length {hdr_len} exceeds its buffer");
    };
    let mut bufs = Vec::with_capacity(payload_chunks.len().saturating_add(1));
    bufs.push(Bytes::copy_from_slice(header));
    bufs.extend_from_slice(payload_chunks);
    Ok(bufs)
}

/// Cut up to `target` bytes off the front of `queue` into the `Bytes` slices that
/// make up one wire frame, returning them and their total. `queued` is the queue's
/// running byte count and drops by exactly what is taken.
///
/// Nothing is copied: whole items move across, and a frame that ends mid-item splits
/// it with `Bytes::split_to`, which reslices the same allocation.
///
/// `None` only when the queue is empty. Both framers hold `queued` in lockstep with
/// `queue`, and `target >= 1` at every call site, so a non-empty queue always yields
/// at least one chunk — a caller that sees `None` with bytes still queued is looking
/// at a bookkeeping bug, not at the end of the blob.
pub(super) fn drain_frame(
    queue: &mut VecDeque<Bytes>,
    queued: &mut usize,
    target: usize,
) -> Option<(Vec<Bytes>, usize)> {
    let mut remaining = target.min(*queued);
    let mut out: Vec<Bytes> = Vec::with_capacity(queue.len().min(remaining));
    let mut total = 0usize;
    while remaining > 0 {
        let front_len = queue.front().map_or(0, Bytes::len);
        if front_len == 0 {
            break;
        }
        if front_len <= remaining {
            let Some(bytes) = queue.pop_front() else {
                break;
            };
            remaining = remaining.saturating_sub(front_len);
            total = total.saturating_add(front_len);
            *queued = queued.saturating_sub(front_len);
            out.push(bytes);
        } else {
            let Some(front) = queue.front_mut() else {
                break;
            };
            let taken = front.split_to(remaining);
            total = total.saturating_add(taken.len());
            *queued = queued.saturating_sub(taken.len());
            out.push(taken);
            remaining = 0;
        }
    }
    if out.is_empty() {
        None
    } else {
        Some((out, total))
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

    use super::{chunk_frame_bufs, drain_frame};

    /// The vectored buffers must lay down exactly the bytes the single-buffer
    /// encoder would, whatever the payload is split into.
    ///
    /// This is the claim the whole zero-copy path rests on: the serve loops no
    /// longer build a `ChunkData` frame at all, so nothing else proves the header
    /// they emit still matches `encode_chunk_frame` + `write_frame`. The chunk
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

                let bufs = chunk_frame_bufs(&chunks, total)?;
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

    /// `total_len` is both what the header declares to the client and what the serve
    /// loop bills for, so a framer whose byte count disagrees with the bytes it
    /// handed over must fail loudly here rather than put a mislabelled frame on the
    /// wire and charge for it.
    #[test]
    fn chunk_frame_bufs_refuses_a_payload_that_does_not_match_its_length() {
        let chunks = vec![Bytes::from_static(b"abcd"), Bytes::from_static(b"ef")];
        assert!(chunk_frame_bufs(&chunks, 6).is_ok(), "6 is the true sum");
        assert!(chunk_frame_bufs(&chunks, 5).is_err(), "under-count refused");
        assert!(chunk_frame_bufs(&chunks, 7).is_err(), "over-count refused");
        // ADR 005 §Non-empty chunk (#1088), at the last door before the wire.
        assert!(chunk_frame_bufs(&[], 0).is_err(), "no chunks at all");
        assert!(
            chunk_frame_bufs(&[Bytes::new()], 0).is_err(),
            "one empty chunk is still a zero-length frame"
        );
    }

    /// Cutting a frame moves whole items and splits only the one the frame ends in,
    /// leaving `queued` equal to the bytes still in the queue.
    #[test]
    fn drain_frame_cuts_at_the_target_and_keeps_the_remainder() {
        let mut queue: std::collections::VecDeque<Bytes> =
            [Bytes::from_static(b"aaaa"), Bytes::from_static(b"bbbb")]
                .into_iter()
                .collect();
        let mut queued = 8usize;

        let (chunks, total) = drain_frame(&mut queue, &mut queued, 6).expect("6 of 8 bytes");
        assert_eq!(total, 6);
        assert_eq!(chunks.iter().map(Bytes::len).sum::<usize>(), total);
        assert_eq!(queued, 2, "the split remainder stays queued");

        let (rest, rest_total) = drain_frame(&mut queue, &mut queued, 6).expect("the remainder");
        assert_eq!(rest_total, 2, "a short final frame, not a padded one");
        assert_eq!(rest.iter().map(Bytes::len).sum::<usize>(), rest_total);
        assert_eq!(queued, 0);

        assert!(
            drain_frame(&mut queue, &mut queued, 6).is_none(),
            "an empty queue is the only `None`"
        );
    }
}
