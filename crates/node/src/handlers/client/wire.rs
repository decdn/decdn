//! Small leaf helpers: rate clamping, response signing, wire writes, receipts.
//! Bodies split from `mod.rs` (#1254).

use super::{
    B256, ClientHandler, ClientMessage, DownloadReceipt, Hash, Ordering, SendStream,
    ServeRejectReason, StreamError, StreamRequest, StreamResponse, StreamResponseBody,
    StreamSlashData, U256, VoucherRejectReason, WatermarkBundle, encode_message, write_frame,
};

impl ClientHandler {
    /// Enqueue one audit receipt for a served-and-paid voucher interval (issues
    /// #248, #803). Called only after `apply_voucher` committed the payment to
    /// the fsynced channel store, so a dropped receipt is non-fatal — the
    /// payment stands regardless.
    ///
    /// The receipt is handed to [`ReceiptSink::record`](super::ReceiptSink::record), a **non-blocking**
    /// enqueue: the actual `write_all` + `flush` runs off the hot path in the
    /// background receipt writer, so this never blocks before `VoucherAck` and a
    /// slow or full disk cannot back-pressure delivery (the bug in #803).
    /// Receipts are enqueued in voucher-acceptance order and the single writer
    /// drains them FIFO, preserving the audit ordering and shutdown-tail
    /// guarantees the previously-awaited inline write relied on (CLAUDE.md /
    /// ADR 003).
    ///
    /// The `voucher_nonce` is rendered as a decimal `uint256` from the
    /// big-endian wire nonce; `client_node_id` is the iroh node id of the paying
    /// peer; `delta_bytes` is the bytes this voucher covers.
    pub(super) fn record_receipt(
        &self,
        hash: Hash,
        delta_bytes: u64,
        client_node_id: B256,
        wire_nonce: [u8; 32],
    ) {
        let voucher_nonce = U256::from_be_bytes(wire_nonce);
        let receipt = DownloadReceipt::new(
            &hash,
            delta_bytes,
            &client_node_id.0,
            voucher_nonce,
            crate::payment_settlement::unix_now(),
        );
        self.receipt_sink.record(receipt);
    }

    /// Load the configured rate and raise it to the delivery floor before
    /// signing a `StreamResponse`, logging a warning and incrementing
    /// `rate_bounds_clamp_events` on any clamp (ADR 005 §Rate bounds — the same
    /// clamp-and-warn the probe handler applies before signing a `ProbeResponse`).
    pub(super) fn clamped_rate(&self) -> u64 {
        let raw_rate = self.rate_per_mb.load(Ordering::Relaxed);
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

    /// Sign a `StreamResponse` body and assemble the full message.
    pub(super) fn sign_response(
        &self,
        body: StreamResponseBody,
        error: Option<StreamError>,
        voucher_interval_mb: Option<u64>,
    ) -> anyhow::Result<StreamResponse> {
        let slash_sig = StreamSlashData::from_response_body(&body)
            .sign(self.eth_signer.as_ref(), &self.slash_domain)
            .map_err(|e| anyhow::anyhow!("stream slash_sig signing failed: {e}"))?
            .as_bytes()
            .to_vec();
        Ok(StreamResponse {
            body,
            error,
            voucher_interval_mb,
            slash_sig,
        })
    }

    /// Send a signed `StreamResponse { ok: false, error }` (delivery-side
    /// failure), then finish the stream. `reason` is the single source of truth:
    /// it both selects the per-reason metric (finer-grained than the wire for the
    /// three `NotFound` cases, which collapse to one code to avoid leaking channel
    /// existence) and derives the wire `StreamError` via `wire_error()` (#876).
    /// The metric
    /// is bumped before the network write so a refusal is counted even if the
    /// client has already gone and the write fails.
    pub(super) async fn respond_error(
        &self,
        send: &mut SendStream,
        req: &StreamRequest,
        reason: ServeRejectReason,
    ) -> anyhow::Result<()> {
        match reason {
            ServeRejectReason::EvictedSinceProbe => {
                self.metrics.serve_stream_rejected_evicted_since_probe();
            }
            ServeRejectReason::CacheMiss => self.metrics.serve_stream_rejected_cache_miss(),
            ServeRejectReason::InternalError => self.metrics.serve_stream_rejected_internal_error(),
            ServeRejectReason::BlobTooLarge => self.metrics.serve_stream_rejected_blob_too_large(),
            ServeRejectReason::UnknownChannel => {
                self.metrics.serve_stream_rejected_unknown_channel();
            }
            ServeRejectReason::OwnerMismatch => self.metrics.serve_stream_rejected_owner_mismatch(),
            ServeRejectReason::InsufficientDeposit => {
                self.metrics.serve_stream_rejected_insufficient_deposit();
            }
            ServeRejectReason::UnauthorizedOrigin => {
                self.metrics.serve_stream_rejected_unauthorized_origin();
            }
            ServeRejectReason::CooperativeCloseSigned => {
                self.metrics
                    .serve_stream_rejected_cooperative_close_signed();
            }
            ServeRejectReason::RangeNotSatisfiable => {
                self.metrics.serve_stream_rejected_range_not_satisfiable();
            }
            ServeRejectReason::HashDenied => self.metrics.serve_stream_rejected_hash_denied(),
            ServeRejectReason::ChainHashDenied => {
                self.metrics.serve_stream_rejected_chain_hash_denied();
            }
            ServeRejectReason::OriginDenied => self.metrics.serve_stream_rejected_origin_denied(),
        }
        // Slash-safety (#1130). If we advertised `has_blob: true` for this hash
        // because it is origin-held (fs directory entry / present pin) and the
        // operator has NOT refused it, we must never sign an `ok: false`
        // CacheMiss for it: a signed has_blob:true probe + a signed NotFound
        // within 30s is phantom-announcement evidence (ADR 014). A CacheMiss on
        // origin-held content means the reactive pull timed out, or the origin
        // object vanished between probe and serve — the publisher's problem, not
        // grounds to hand a requester slashable evidence. Fail SILENT — the
        // metric already fired above; drop without signing. The correct
        // consequence is a local reputation ding (ADR 008), not a bond slash.
        //
        // Scope is deliberately narrow:
        // - Only `CacheMiss`. A deliberately EVICTED or DENIED hash surfaces as
        //   `EvictedSinceProbe` / `HashDenied`, and signing THAT refusal is the
        //   intended, accountable behavior — it is exactly how an
        //   announce-then-evict is made slashable (see G-GOV-03). Suppressing it
        //   would break slashing accountability.
        // - The live `refuses` guard is belt-and-suspenders: `origin_held` is a
        //   rescan snapshot that can lag a just-issued eviction, so we re-check
        //   the live refusal set rather than trust the snapshot alone.
        // - Payment/auth rejections are untouched (not availability claims).
        if matches!(reason, ServeRejectReason::CacheMiss) {
            let store_hash = decdn_cache::Hash::from_bytes(req.hash);
            if !self.cache.refuses(store_hash) && self.cache.origin_held_size(store_hash).is_some()
            {
                tracing::debug!(
                    hash = %store_hash,
                    ?reason,
                    "origin-held serve missed; dropping without a signed ok:false to avoid phantom-slash evidence (#1130)"
                );
                return Ok(());
            }
        }

        let error = reason.wire_error();
        let rate_per_mb = self.clamped_rate();
        let body = StreamResponseBody {
            hash: req.hash,
            ok: false,
            rate_per_mb,
            total_bytes: 0,
            channel_id: req.channel_id,
            timestamp_us: req.timestamp_us,
            redirect: None,
        };
        let resp = self.sign_response(body, Some(error), None)?;
        self.write_message(send, &ClientMessage::StreamResponse(resp))
            .await?;
        let _ = send.finish();
        Ok(())
    }

    /// Write a mid-stream `StreamError { VoucherRejected }` and finish the
    /// stream **cleanly** — no QUIC reset — so the client can read the reason
    /// (ADR 005 §`VoucherRejected` semantics). `bundle` is the wallet-less
    /// resume watermark (issue #1481): callers pass `Some` only for the four
    /// gated regression/exhaustion reasons, and only after verifying the
    /// rejected voucher's signature recovered to the channel's pinned
    /// `voucher_signer` — this method does not re-derive or re-check that
    /// gate, it trusts the caller.
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
        write_frame(send, &payload)
            .await
            .map_err(|e| anyhow::anyhow!("write failed: {e}"))
    }
}
