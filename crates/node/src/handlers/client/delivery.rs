//! Blob delivery: export the requested range and stream it as paid chunks.
//! Bodies split from `mod.rs` (#1254).

use super::{
    Arc, B256, BatchStop, BufferedVoucherReader, ChannelDeliveryState, ChannelId, ChunkData,
    ClientHandler, ClientMessage, Hash, MB_BYTES, Mutex, RecvStream, SendStream, VecDeque,
};

impl ClientHandler {
    /// Stream blob bytes to the paying client behind a credit window (ADR 003
    /// §Credit window): keep delivering `voucher_interval_mb`-sized batches while
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
        // The downstream credit window, floored at one interval so the loop can
        // always make progress and — for the unconfigured default — collapses to
        // stop-and-wait. See [`ClientHandler::credit_window`].
        let window = self.credit_window(interval_bytes);
        // Group-commit cap (#1483): at most this many vouchers share one fsync.
        // Bounded by how many intervals fit in the window — the window caps the
        // in-flight (delivered-but-unpaid) intervals — so at the one-interval
        // stop-and-wait floor this is 1 and each recoup collects a single voucher,
        // exactly the pre-batch cadence. `interval_bytes >= 1` (floored in
        // `credit_window`), so the division never divides by zero.
        let batch_cap = usize::try_from(window / interval_bytes.max(1))
            .unwrap_or(usize::MAX)
            .max(1);

        // Resolved once: a channel's funder is fixed for its lifetime, and the
        // per-boundary takedown re-check below must not take the channel lock
        // every MB just to re-read an immutable field.
        //
        // This is the FUNDER (ADR 011 compliance), never the channel's
        // `voucher_signer`, and must not be re-keyed onto it: a blacklisted
        // funder can pin a clean throwaway key as its signer, so checking the
        // signer here would silently stop enforcing takedowns.
        let funder = match channel {
            Some(chan) => Some(chan.lock().await.state.client),
            None => None,
        };

        // Bytes written to the wire, and bytes covered by an accepted voucher.
        // Their gap `delivered − paid` is the unrecouped credit the window caps.
        let mut delivered: u64 = 0;
        let mut paid: u64 = 0;
        // Bytes forwarded since the last COMPLETED interval (the sub-interval
        // remainder), and the completed-but-unpaid interval deltas awaiting
        // collection — together they are exactly `delivered − paid`.
        let mut unvouchered: u64 = 0;
        let mut pending: VecDeque<u64> = VecDeque::new();
        // One buffered voucher reader for the whole stream (#1483): it buffers
        // pipelined vouchers across recoup calls, so every voucher read MUST go
        // through it — a second reader would lose bytes it read ahead.
        let mut reader = BufferedVoucherReader::default();

        // `slice::chunks` yields no items for an empty slice and never a zero-length
        // chunk, so `ChunkData::new` cannot reject one here — the empty blob makes
        // no pass through the deliver phase and goes straight to `StreamEnd`
        // (#1054). The `?` is the type carrying the invariant, not a live failure
        // mode.
        let mut chunks = data.chunks(decdn_protocol::CHUNK_SIZE);
        let mut next_chunk = chunks.next();

        loop {
            // --- deliver phase: stream chunks while the window has room. The
            // window is checked BEFORE each send, so the frontier
            // `delivered − paid` can overshoot by at most the one chunk that
            // crosses the threshold: the credit exposure is "≤ window + one
            // chunk", exact to within a chunk — the same bound the fused
            // `window_forward_loop` documents. The check must stay pre-send, not
            // anticipatory (would-this-chunk-cross): an anticipatory check can
            // stop short of a full interval, and at the one-interval floor
            // (`window == interval`) it would never complete one, starving the
            // recoup phase of a voucher to collect and deadlocking the loop. ---
            while let Some(chunk) = next_chunk {
                if delivered.saturating_sub(paid) >= window {
                    break;
                }
                let frame = ChunkData::new(chunk.to_vec())
                    .map_err(|e| anyhow::anyhow!("refusing to serve an invalid chunk: {e}"))?;
                self.write_message(send, &ClientMessage::ChunkData(frame))
                    .await?;
                let len = chunk.len() as u64;
                delivered = delivered.saturating_add(len);
                unvouchered = unvouchered.saturating_add(len);
                if unvouchered >= interval_bytes {
                    pending.push_back(unvouchered);
                    unvouchered = 0;
                }
                next_chunk = chunks.next();
            }
            let done_delivering = next_chunk.is_none();

            // Once the whole blob is on the wire, fold the closing sub-interval
            // remainder into `pending` as a final delta so the recoup batch drains
            // it uniformly with the completed intervals.
            if done_delivering && unvouchered > 0 {
                pending.push_back(unvouchered);
                unvouchered = 0;
            }

            // --- recoup phase: batch up to `batch_cap` completed intervals into
            // ONE fsynced commit, acking each voucher only after the commit is
            // durable (#1483, group commit). When the window blocks the deliver
            // phase there is always a completed interval to collect (the
            // one-interval floor guarantees it), so the loop never spins without an
            // await. At `batch_cap == 1` (stop-and-wait) this is one voucher per
            // recoup — the pre-batch cadence. ---
            let mut deltas: Vec<u64> = Vec::with_capacity(batch_cap);
            while deltas.len() < batch_cap {
                match pending.pop_front() {
                    Some(delta) => deltas.push(delta),
                    None => break,
                }
            }
            let collected_any = !deltas.is_empty();
            if collected_any {
                let outcome = self
                    .collect_voucher_batch(
                        send,
                        recv,
                        &mut reader,
                        hash,
                        channel_id,
                        channel,
                        client_node_id,
                        rate_per_mb,
                        &deltas,
                    )
                    .await?;
                // Advance `paid` by exactly the committed prefix's bytes.
                let paid_bytes: u64 = deltas.iter().take(outcome.committed).sum();
                paid = paid.saturating_add(paid_bytes);
                // Re-queue deltas the client had not yet paid (a short batch — it
                // has not sent those vouchers yet), preserving order at the front.
                for &delta in deltas
                    .get(outcome.committed..)
                    .unwrap_or_default()
                    .iter()
                    .rev()
                {
                    pending.push_front(delta);
                }
                match outcome.stop {
                    BatchStop::Rejected => return Ok(()),
                    BatchStop::Continue => {}
                }
            }

            // Done when the whole blob is on the wire and every interval, closing
            // partial included, has been paid.
            let done = done_delivering && pending.is_empty() && unvouchered == 0;

            // ADR 011 §On Blacklist Event: in-flight streams for a blacklisted hash
            // are terminated at the next voucher boundary. Under the credit window
            // that first check lands up to one WINDOW into the stream (steady state
            // ~one interval, since each later iteration delivers roughly one
            // interval before recouping), so detection latency is window-bounded,
            // not per-MB — the one-interval floor caps it, and even a full window is
            // negligible against the takedown compliance window. Gated on
            // `collected_any` so it runs only after a committed batch — mirroring
            // `window_forward_loop`, which nests the equivalent check under its own
            // `collected_any`. The check sits AFTER a voucher so the bytes already
            // on the wire are still paid for — the takedown stops FURTHER delivery,
            // it does not retroactively make the last interval free. Only meaningful
            // while bytes remain to withhold: a takedown landing at the final
            // voucher has nothing left to stop, and terminating there would turn a
            // complete, fully-paid delivery into a reset (no `StreamEnd`) — hence the
            // `!done` guard. (The fused `window_forward_loop` deliberately omits that
            // guard: it is acquiring the blob, so a final-voucher takedown must still
            // abandon the pull to avoid promoting a taken-down blob into cache.)
            if collected_any && !done && self.takedown_landed(hash, funder) {
                self.terminate_for_takedown(send, recv, hash);
                return Ok(());
            }

            if done {
                break;
            }
        }

        self.write_message(send, &ClientMessage::StreamEnd).await?;
        let _ = send.finish();
        Ok(())
    }
}
