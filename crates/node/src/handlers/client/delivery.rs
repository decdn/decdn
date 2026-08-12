//! Blob delivery: export the requested range and stream it as paid chunks.
//! Bodies split from `mod.rs` (#1254).

use std::pin::Pin;

use bytes::{Bytes, BytesMut};
use decdn_cache::CacheResult;
use futures_util::{Stream, StreamExt};

use super::{
    Arc, B256, BatchStop, BufferedVoucherReader, ChunkData, ClientHandler, ClientMessage, Hash,
    LaneDeliveryState, LaneKey, MB_BYTES, Mutex, RecvStream, SendStream, VecDeque,
};

/// The byte stream [`CacheEngine::export_bao_range_stream`] hands back.
///
/// [`CacheEngine::export_bao_range_stream`]: decdn_cache::CacheEngine::export_bao_range_stream
type BaoExportStream = Pin<Box<dyn Stream<Item = CacheResult<Bytes>> + Send>>;

/// Re-frame the cache's bao export stream into `cdn/client/v1` wire frames
/// (#1132).
///
/// The export yields one item per bao element — a 64-byte proof pair or one
/// 16 KiB chunk group — which does not line up with [`decdn_protocol::CHUNK_SIZE`]
/// (1 KiB). This buffers just enough to cut full-size frames, so the serve task
/// holds O(one export item) rather than O(blob size). It replaces the
/// `slice::chunks` iterator that used to walk a fully-materialised export buffer,
/// and reproduces that iterator's two load-bearing properties exactly:
///
/// - **Never an empty frame.** `ChunkData` cannot hold one (#1088), and the
///   inactivity deadline on both receive loops rests on "a frame arrived" and
///   "bytes made progress" being the same statement.
/// - **No frames at all for an empty export.** The 0-byte blob (#1054) must go
///   straight to `StreamEnd` rather than send a zero-length frame first.
struct ChunkFramer {
    stream: BaoExportStream,
    /// Export bytes not yet cut into a frame. Bounded by one export item plus the
    /// sub-frame remainder — it never grows with blob size, which is the whole
    /// point of the type.
    buf: BytesMut,
    /// The export stream has yielded its last item; `buf` is all that remains.
    drained: bool,
    /// The export faulted. Terminal: `buf` is cleared and no further frame is
    /// ever cut. Not because those bytes are suspect — `buf` only ever holds
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
            buf: BytesMut::new(),
            drained: false,
            faulted: false,
            hash,
        }
    }

    /// The next wire frame: `CHUNK_SIZE` bytes, or the shorter final remainder
    /// (ADR 005 §Partial final chunk). `None` once the export is exhausted.
    ///
    /// # Errors
    ///
    /// Propagates an export fault. Note this includes the truncation refusal,
    /// which the streaming export can only detect after its last item — so unlike
    /// the buffered export it can fire when frames are already on the wire. The
    /// caller must abort the delivery (skipping `StreamEnd`) so the client sees a
    /// short delivery and does not pay the closing voucher.
    async fn next_frame(&mut self) -> anyhow::Result<Option<Bytes>> {
        if self.faulted {
            anyhow::bail!("bao export already faulted; refusing to serve further frames");
        }
        while !self.drained && self.buf.len() < decdn_protocol::CHUNK_SIZE {
            match self.stream.next().await {
                Some(Ok(bytes)) => self.buf.extend_from_slice(&bytes),
                Some(Err(e)) => {
                    // Poison, and drop the buffered remainder: this delivery is
                    // over, so any further frame would bill for a transfer that
                    // cannot complete.
                    self.faulted = true;
                    self.buf.clear();
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
        if self.buf.is_empty() {
            return Ok(None);
        }
        let take = self.buf.len().min(decdn_protocol::CHUNK_SIZE);
        Ok(Some(self.buf.split_to(take).freeze()))
    }
}

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
        lane_key: LaneKey,
        lane: Option<&Arc<Mutex<LaneDeliveryState>>>,
        client_node_id: B256,
        rate_per_mb: u64,
        interval_mb: u64,
    ) -> anyhow::Result<()> {
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
        // range before the first frame goes out, so serving a 708 MB blob cost
        // ~708 MB resident per concurrent serve. Streaming bounds this task to one
        // export item plus one wire frame, independent of blob size. Do not
        // reintroduce the buffered call here.
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

        // The in-flight takedown re-check (ADR 011 compliance) keys on the pool
        // FUNDER — the pool owner. In the shared-payment-pool model the owner is
        // a chain quantity (`getPool.owner`), not carried on the per-lane
        // [`LaneState`], so the funder is threaded to the mid-stream re-check by
        // E4 (from the cached `getPool` view). Until then the per-lane loop
        // passes `None`, so a mid-stream funder takedown is caught by the
        // open-time gates and the hash-denylist re-check, not the funder
        // re-check. BOUNDARY: E4 pool-owner threading.
        let funder = None;

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

        // `ChunkFramer` yields no frames for an empty export and never a
        // zero-length frame, so `ChunkData::new` cannot reject one here — the empty
        // blob makes no pass through the deliver phase and goes straight to
        // `StreamEnd` (#1054). The `?` on `ChunkData::new` is the type carrying the
        // invariant, not a live failure mode; the `?` on `next_frame` IS live —
        // that is where a mid-export store fault or the truncation refusal surfaces
        // now that the export streams (#1132).
        let mut chunks = ChunkFramer::new(data, hash);
        let mut next_chunk = chunks.next_frame().await?;

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
            // The pre-fetched frame is `Bytes` (not the `&[u8]` a slice iterator
            // yielded), so it cannot be copied out of `next_chunk` and left behind
            // on the window `break` — the window check therefore runs BEFORE the
            // `take`, keeping the un-sent frame in `next_chunk` for the next pass
            // and for the `done_delivering` read below.
            while next_chunk.is_some() {
                if delivered.saturating_sub(paid) >= window {
                    break;
                }
                let Some(chunk) = next_chunk.take() else {
                    break;
                };
                let len = chunk.len() as u64;
                let frame = ChunkData::new(chunk.to_vec())
                    .map_err(|e| anyhow::anyhow!("refusing to serve an invalid chunk: {e}"))?;
                self.write_message(send, &ClientMessage::ChunkData(frame))
                    .await?;
                delivered = delivered.saturating_add(len);
                unvouchered = unvouchered.saturating_add(len);
                if unvouchered >= interval_bytes {
                    pending.push_back(unvouchered);
                    unvouchered = 0;
                }
                next_chunk = chunks.next_frame().await?;
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
                        lane_key,
                        lane,
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

    /// The framer must cut exactly what `slice::chunks(CHUNK_SIZE)` cut over the
    /// concatenated export — that equivalence is the whole safety argument for
    /// replacing the buffered iterator (#1132), because the client's cumulative
    /// wire-byte accounting and the voucher intervals are defined over the frame
    /// sequence.
    #[tokio::test]
    async fn framing_matches_the_buffered_chunk_iterator() -> anyhow::Result<()> {
        // Deliberately awkward item sizes: a 64-byte proof pair, a full chunk
        // group, and remainders that straddle CHUNK_SIZE boundaries.
        let items = vec![
            vec![1u8; 64],
            vec![2u8; 16 * 1024],
            vec![3u8; 64],
            vec![4u8; 1000],
            vec![5u8; 1],
        ];
        let flat: Vec<u8> = items.iter().flatten().copied().collect();

        let mut framer = ChunkFramer::new(stream_of(items), Hash::new(b"framing-test"));
        let mut got: Vec<Bytes> = Vec::new();
        while let Some(frame) = framer.next_frame().await? {
            got.push(frame);
        }

        let want: Vec<&[u8]> = flat.chunks(decdn_protocol::CHUNK_SIZE).collect();
        anyhow::ensure!(
            got.len() == want.len(),
            "framed {} chunks, buffered iterator yields {}",
            got.len(),
            want.len()
        );
        for (i, (actual, expected)) in got.iter().zip(want.iter()).enumerate() {
            anyhow::ensure!(actual.as_ref() == *expected, "frame {i} differs");
        }
        // Restated as an invariant rather than inferred from the comparison: no
        // frame may be empty (#1088) or oversized.
        for frame in &got {
            anyhow::ensure!(!frame.is_empty(), "framer emitted an empty frame");
            anyhow::ensure!(
                frame.len() <= decdn_protocol::CHUNK_SIZE,
                "framer emitted an oversized frame"
            );
        }
        Ok(())
    }

    /// An empty export (the 0-byte blob, #1054) must yield no frames at all, so
    /// the deliver phase makes no pass and the serve goes straight to `StreamEnd`.
    #[tokio::test]
    async fn an_empty_export_yields_no_frames() -> anyhow::Result<()> {
        let mut framer = ChunkFramer::new(stream_of(Vec::new()), Hash::new(b"empty-test"));
        anyhow::ensure!(
            framer.next_frame().await?.is_none(),
            "an empty export must yield no frames"
        );
        Ok(())
    }

    /// A mid-export fault — including the truncation refusal, which the streaming
    /// export can only report after its last item — must propagate out of
    /// `next_frame` rather than being swallowed into a short-but-clean delivery.
    /// That is what makes the caller abort without `StreamEnd`, so the client
    /// rejects the delivery and never pays the closing voucher.
    #[tokio::test]
    async fn a_mid_export_fault_propagates() -> anyhow::Result<()> {
        let stream: BaoExportStream = Box::pin(futures_util::stream::iter(vec![
            // 1500, deliberately NOT a CHUNK_SIZE multiple: one full frame is
            // cuttable, leaving 476 bytes buffered when the fault lands. With a
            // multiple the buffer would be empty at that point and the `buf.clear()`
            // below would be unobservable.
            Ok(Bytes::from(vec![7u8; 1500])),
            Err(CacheError::Store(anyhow::anyhow!(
                "export_bao stream for deadbeef ended without Done; refusing truncated export"
            ))),
        ]));
        let mut framer = ChunkFramer::new(stream, Hash::new(b"fault-test"));

        // The first full frame is already cuttable from the buffered bytes.
        let first = framer.next_frame().await?;
        anyhow::ensure!(first.is_some(), "expected a frame before the fault");

        // Draining toward the next frame reaches the error.
        let err = loop {
            match framer.next_frame().await {
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
        let after = framer.next_frame().await;
        anyhow::ensure!(
            after.is_err(),
            "a faulted framer must refuse further frames, got {:?}",
            after.map(|o| o.map(|b| b.len()))
        );
        Ok(())
    }
}
