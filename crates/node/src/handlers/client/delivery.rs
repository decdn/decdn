//! Blob delivery: export the requested range and stream it as paid chunks.
//! Bodies split from `mod.rs` (#1254).

use super::{
    Arc, B256, ChannelDeliveryState, ChannelId, ChunkData, ClientHandler, ClientMessage, Hash,
    MB_BYTES, Mutex, RecvStream, SendStream, VoucherOutcome,
};

impl ClientHandler {
    /// Stream blob bytes in `voucher_interval_mb`-sized batches, pausing to
    /// collect a cumulative voucher at each boundary and a closing voucher for
    /// the final partial batch. Returns `Ok(())` on a clean rejection or a
    /// completed delivery.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn deliver(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        hash: Hash,
        byte_offset: u64,
        byte_len: u64,
        total_bytes: u64,
        channel_id: ChannelId,
        channel: Option<&Arc<Mutex<ChannelDeliveryState>>>,
        client_node_id: B256,
        rate_per_mb: u64,
        quote_floor: u64,
        interval_mb: u64,
    ) -> anyhow::Result<()> {
        // The client-facing `cdn/client/v1` payload is ALWAYS the bao interleaved
        // verified-stream encoding — there is no raw-byte path (ADR 038 §Serve
        // side, AC#4). `export_bao_range` reads the persisted outboard and emits
        // proof+data for the chunk-group-aligned span covering the request; it
        // works on a complete blob AND on the *partial* blob an origin-tier range
        // pull imported, and never re-hashes held content. A whole-blob request
        // is `(byte_offset == 0, byte_len == 0)`, which aligns to the full chunk
        // range. Vouchers below meter the actually-served bytes — content PLUS the
        // interleaved proof nodes (ADR 038 §Payment metering) — by counting wire
        // bytes, so range delivery is billed over the span, not the whole blob.
        //
        // `export_bao_range` snaps to enclosing 16 KiB chunk-group boundaries (a
        // bao proof anchors whole groups); the serve side does NOT trim back to
        // `byte_offset` — trimming would break verification. The receiver decodes
        // the group-aligned superset and discards the leading bytes before
        // `byte_offset`, so a well-behaved resume requests a group-aligned offset
        // (ADR 038 §Verification model).
        // `total_bytes` is the authoritative whole-blob size the caller already
        // resolved (a `Complete` blob's size, or the whole-blob size an origin-tier
        // range pull reported). `export_bao_range` needs it to build the BaoTree;
        // the store cannot be relied on for it because an origin-tier range pull
        // imports a *partial* blob whose `status()` size is `None` (#823).
        let data = self
            .cache
            .export_bao_range(hash, byte_offset, byte_len, total_bytes)
            .await
            .map_err(|e| anyhow::anyhow!("cache export_bao_range failed: {e}"))?;

        let interval_bytes = interval_mb.saturating_mul(MB_BYTES);
        let mut unvouchered: u64 = 0;

        // `slice::chunks` yields no items for an empty slice and never a zero-length
        // chunk, so `ChunkData::new` cannot reject one here — the empty blob goes
        // straight to `StreamEnd` (#1054). The `?` is the type carrying the invariant,
        // not a live failure mode.
        for chunk in data.chunks(decdn_protocol::CHUNK_SIZE) {
            let frame = ChunkData::new(chunk.to_vec())
                .map_err(|e| anyhow::anyhow!("refusing to serve an invalid chunk: {e}"))?;
            self.write_message(send, &ClientMessage::ChunkData(frame))
                .await?;
            unvouchered = unvouchered.saturating_add(chunk.len() as u64);
            if unvouchered >= interval_bytes {
                match self
                    .collect_voucher(
                        send,
                        recv,
                        hash,
                        channel_id,
                        channel,
                        client_node_id,
                        rate_per_mb,
                        quote_floor,
                        unvouchered,
                    )
                    .await?
                {
                    VoucherOutcome::Accepted => unvouchered = 0,
                    VoucherOutcome::Rejected => return Ok(()),
                }
            }
        }
        // Closing voucher for the final partial batch.
        if unvouchered > 0
            && matches!(
                self.collect_voucher(
                    send,
                    recv,
                    hash,
                    channel_id,
                    channel,
                    client_node_id,
                    rate_per_mb,
                    quote_floor,
                    unvouchered,
                )
                .await?,
                VoucherOutcome::Rejected
            )
        {
            return Ok(());
        }

        self.write_message(send, &ClientMessage::StreamEnd).await?;
        let _ = send.finish();
        Ok(())
    }
}
