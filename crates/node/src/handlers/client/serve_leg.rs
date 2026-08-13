//! The decoupled downstream **serve leg** of the node serve-miss driver (ADR
//! 037).
//!
//! [`ClientHandler::serve_leg`] is the seller half of the two-leg serve-miss.
//! It walks the requested range `R = [offset, offset + len)` in order and
//! delivers ALL of it to the paying client. The downstream wire is produced by a
//! coherent whole-range bao encoder ([`super::serve_encoder::CoherentFrameProducer`],
//! ADR 038): it reads leaf DATA from the cache the pull leg fills — awaiting the
//! store's present-range frontier when a span has not landed yet — and proof nodes
//! from the shared outboard the pull captures, so the whole range is ONE coherent
//! verified stream. Billing, the credit window, takedown, and client-disconnect
//! handling run over the cache the pull leg fills beside this task: the byte source
//! is the local store, read as a coherent verified stream, not an upstream
//! connection.
//!
//! # Coordination with the pull leg (shared, single-task state)
//!
//! Both legs run as concurrent futures on ONE serve task, so the shared state is
//! plain `Arc`, never `tokio::spawn`ed across threads:
//!
//! - **`served_paid`** — the client's PAID *content* frontier. The serve leg
//!   stores it after every voucher batch commits (mapped from paid WIRE bytes
//!   through [`content_paid_frontier`]); the pull leg's `WindowPacer` reads it to
//!   bound `pulled − served_paid ≤ window`.
//! - **`served_paid_advanced`** — notified after each advance, so a pull leg
//!   parked in `PaceDecision::Wait` re-decides exactly when payment clears.
//! - **`pull_ended` + `pull_result`** — the pull leg records its terminal
//!   outcome in `pull_result` and THEN fires `pull_ended`. Whenever the serve
//!   leg must await a gap becoming present it races the present-range watch
//!   against `pull_ended`: a pull that ended `Err` fails the serve (a gap the
//!   pull could not fill can never be delivered); a pull that ended `Ok` means
//!   the gap must now be present. This is the no-hang guarantee.
//!
//! The serve leg owns termination: it is what fulfils `R` for the client, so its
//! completion (or error) ends the serve and drops the pull leg.

use decdn_cache::{FillSession, NodeRangedStore};
use decdn_client_pull::sink::content_paid_frontier;

use super::{
    Arc, B256, BatchStop, BufferedVoucherReader, ChannelDeliveryState, ChannelId, ChunkData,
    ClientHandler, ClientMessage, Hash, MB_BYTES, Mutex, Ordering, RecvStream, SendStream,
    VecDeque,
};

impl ClientHandler {
    /// Deliver the requested range `R = [offset, offset + len)` to the paying
    /// client from the cache, awaiting a concurrent pull leg to fill any gaps —
    /// the downstream (seller) half of the decoupled serve-miss (ADR
    /// 037). `len == 0` means "to the end of the blob".
    ///
    /// Delivery, billing, the credit window, group-commit voucher batching, the
    /// in-flight takedown boundary, and client-disconnect handling all read from
    /// the cache: a coherent whole-range bao encoder reads the cache the pull leg
    /// fills. The caller (orchestration) has
    /// already proven channel ownership, run the pre-flight gates, negotiated
    /// `interval_mb`, and signed + sent the `StreamResponse`; the pull leg fills
    /// the store beside this call. Consumes neither stream — the caller does.
    ///
    /// # Money semantics
    ///
    /// `served_paid` is the PAID *content* frontier; completion gates on payment
    /// (every delivered interval vouchered), never on delivery, so the
    /// credit-window tail the client received ahead of its voucher is always
    /// billed before `StreamEnd`.
    /// Vouchers meter WIRE bytes (content plus interleaved bao proof, ADR 038), so
    /// the internal `delivered`/`paid` counters and the `window` gate are wire
    /// quantities; the shared `served_paid` frontier the pull leg paces against is
    /// mapped back into content space via [`content_paid_frontier`].
    ///
    /// # Errors
    ///
    /// A client disconnect (the write / voucher-collect surfaces it), an
    /// underpayment bail, an `encode_range` fault, or a gap the pull leg could not
    /// fill. On any error the caller drops the pull leg, which stops the upstream
    /// spend and persists the buyer watermark.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn serve_leg(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        store: NodeRangedStore,
        session: Arc<FillSession>,
        also_pace: &[Arc<FillSession>],
        channel: &Arc<Mutex<ChannelDeliveryState>>,
        hash: Hash,
        channel_id: ChannelId,
        client_node_id: B256,
        rate_per_mb: u64,
        interval_mb: u64,
        offset: u64,
        len: u64,
        total_bytes: u64,
        window: u64,
    ) -> anyhow::Result<()> {
        // Resolve the request end. `len == 0` ⇒ to the blob end (driver
        // convention); otherwise clamp to the tree size.
        let end = if len == 0 {
            total_bytes
        } else {
            offset.saturating_add(len).min(total_bytes)
        };
        let offset = offset.min(end);

        let interval_bytes = interval_mb.saturating_mul(MB_BYTES).max(1);
        // Backpressure bound, floored at one interval so the loop can always make
        // progress (deliver a full interval, then recoup its voucher).
        let window = window.max(interval_bytes);
        // Group-commit cap (#1483): at most this many vouchers share one fsync,
        // bounded by how many intervals fit in the window.
        let batch_cap = usize::try_from(window / interval_bytes)
            .unwrap_or(usize::MAX)
            .max(1);

        // Read once: the funder is immutable for the channel's lifetime, and the
        // per-boundary in-flight takedown check must not re-take the channel lock
        // every batch to re-read it. This is the FUNDER (ADR 011 compliance),
        // never the channel's `voucher_signer`.
        let funder = channel.lock().await.state.client;

        // Bytes written to the wire, and bytes an accepted voucher covered — both
        // WIRE quantities (bao content + proof, ADR 038). Their gap
        // `delivered − paid` is the unrecouped credit the window caps.
        let mut delivered: u64 = 0;
        let mut paid: u64 = 0;
        // Bytes forwarded since the last COMPLETED interval (the sub-interval
        // remainder), and the completed-but-unpaid interval deltas awaiting
        // collection — together they are exactly `delivered − paid`.
        let mut unvouchered: u64 = 0;
        let mut pending: VecDeque<u64> = VecDeque::new();
        // One buffered voucher reader for the whole stream (#1483): every read
        // goes through it so pipelined vouchers buffered ahead of a batch commit
        // are not lost.
        let mut reader = BufferedVoucherReader::default();

        // The coherent whole-range bao encoder (ADR 038): ONE verified stream for
        // `R`, produced incrementally — leaf data awaited from the cache the pull
        // fills, proof nodes from the shared `outboard` the pull captures.
        let mut producer = super::serve_encoder::CoherentFrameProducer::new(
            store,
            Arc::clone(&session),
            offset,
            end,
            total_bytes,
        );

        // The first frame — awaiting the pull leg if `R` opens on a gap. A pull
        // that ends `Err` here fails the serve rather than hanging.
        let mut next_chunk = match producer.next_frame().await {
            Ok(frame) => frame,
            Err(e) => {
                // DIAG#1673: capture-on-failure — the very first frame faulted (a
                // gap the pull could not fill, or a proof/verify error). This is one
                // of the mid-stream close sites under investigation.
                eprintln!(
                    "DIAG#1673 serve_leg.first_frame hash={hash} off={offset} end={end} \
                     pull_outcome={:?} observers={} cancelled={} err={e:#}",
                    session.outcome(),
                    session.observer_count(),
                    session.is_cancelled(),
                );
                return Err(e);
            }
        };

        loop {
            // Progress trackers for the no-progress guard below: an iteration that
            // delivers no new byte AND clears no voucher has stalled — the client
            // stopped paying.
            let delivered_at_iter_start = delivered;
            let mut committed_this_iter = 0usize;

            // --- deliver phase: stream frames while the window has room. Checked
            // BEFORE each send, so `delivered − paid` overshoots by at most the one
            // frame that crosses the threshold. ---
            while next_chunk.is_some() {
                if delivered.saturating_sub(paid) >= window {
                    break;
                }
                let Some(chunk) = next_chunk.take() else {
                    break;
                };
                let clen = chunk.len() as u64;
                let frame = ChunkData::new(chunk.to_vec())
                    .map_err(|e| anyhow::anyhow!("refusing to serve an invalid chunk: {e}"))?;
                // A downstream drop surfaces here as `Err` (#856 client-disconnect
                // shape); meter the client-abandon, then propagate so the caller drops
                // the pull leg.
                if let Err(e) = self
                    .write_message(send, &ClientMessage::ChunkData(frame))
                    .await
                {
                    // DIAG#1673: capture-on-failure — a downstream write failed
                    // mid-stream (the #856 client-disconnect shape). Records where the
                    // serve closed so CI can distinguish a real client drop from a
                    // server-side teardown that manifests as the client's `early eof`.
                    eprintln!(
                        "DIAG#1673 serve_leg.write hash={hash} delivered={delivered} paid={paid} \
                         pull_outcome={:?} observers={} cancelled={} err={e:#}",
                        session.outcome(),
                        session.observer_count(),
                        session.is_cancelled(),
                    );
                    self.metrics.node_pull_through_client_abandoned();
                    return Err(e);
                }
                delivered = delivered.saturating_add(clen);
                unvouchered = unvouchered.saturating_add(clen);
                if unvouchered >= interval_bytes {
                    pending.push_back(unvouchered);
                    unvouchered = 0;
                }
                next_chunk = match producer.next_frame().await {
                    Ok(frame) => frame,
                    Err(e) => {
                        // DIAG#1673: capture-on-failure — a mid-range frame faulted
                        // (the coherent encoder hit a gap the pull could not fill, or
                        // a proof/verify error). A prime mid-stream close site.
                        eprintln!(
                            "DIAG#1673 serve_leg.next_frame hash={hash} delivered={delivered} \
                             paid={paid} pull_outcome={:?} observers={} cancelled={} err={e:#}",
                            session.outcome(),
                            session.observer_count(),
                            session.is_cancelled(),
                        );
                        return Err(e);
                    }
                };
            }
            let done_delivering = next_chunk.is_none();

            // Once the whole range is on the wire, fold the closing sub-interval
            // remainder into `pending` so the recoup batch drains it uniformly.
            if done_delivering && unvouchered > 0 {
                pending.push_back(unvouchered);
                unvouchered = 0;
            }

            // --- recoup phase: batch up to `batch_cap` completed intervals into
            // ONE fsynced commit, acking each voucher only after it is durable
            // (#1483, group commit). ---
            let mut deltas: Vec<u64> = Vec::with_capacity(batch_cap);
            while deltas.len() < batch_cap {
                match pending.pop_front() {
                    Some(delta) => deltas.push(delta),
                    None => break,
                }
            }
            let collected_any = !deltas.is_empty();
            if collected_any {
                // A transport drop or an underpayment bail surfaces as `Err`;
                // propagate so the caller drops the pull leg (bounding the
                // upstream spend and persisting the buyer watermark).
                let outcome = match self
                    .collect_voucher_batch(
                        send,
                        recv,
                        &mut reader,
                        hash,
                        channel_id,
                        Some(channel),
                        client_node_id,
                        rate_per_mb,
                        &deltas,
                    )
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(e) => {
                        // DIAG#1673: capture-on-failure — a transport drop or an
                        // underpayment bail (#856/#857) inside voucher collection.
                        eprintln!(
                            "DIAG#1673 serve_leg.collect_voucher hash={hash} delivered={delivered} \
                             paid={paid} pull_outcome={:?} observers={} cancelled={} err={e:#}",
                            session.outcome(),
                            session.observer_count(),
                            session.is_cancelled(),
                        );
                        // A transport drop or an underpayment bail (#856/#857): meter
                        // the client-abandon, then propagate so the caller drops
                        // the pull leg and bounds the upstream spend.
                        self.metrics.node_pull_through_client_abandoned();
                        return Err(e);
                    }
                };
                committed_this_iter = outcome.committed;
                // Advance `paid` by exactly the committed prefix's WIRE bytes.
                let paid_bytes: u64 = deltas.iter().take(outcome.committed).sum();
                paid = paid.saturating_add(paid_bytes);
                if outcome.committed > 0 {
                    // Publish the PAID CONTENT frontier for the pull leg's
                    // `WindowPacer`, mapping paid WIRE back into content space (the
                    // largest chunk-group boundary provably inside the paid wire
                    // prefix — conservative, so the pull never overshoots its
                    // window). One contiguous delivery from `offset`, so `offset`
                    // is the single fetch-start.
                    let served = content_paid_frontier(offset, total_bytes, paid);
                    // `fetch_max`, not `store`: N observers advance the SHARED frontier
                    // and the pull's `WindowPacer` binds on the MAX-over-observers paid
                    // frontier (DECISION-B), so a slower observer must not regress a
                    // faster one. Behavior-preserving for N=1 (a single contiguous
                    // delivery is already monotone, so `fetch_max == store`).
                    session
                        .served_frontier()
                        .fetch_max(served, Ordering::Relaxed);
                    session.served_advanced().notify_waiters();
                    // Under partial-overlap coalescing this serve leg is fed by more
                    // than its own pull: each attached sibling pull produces the OVERLAP
                    // this leg also consumes and bills. The sibling's `served_paid` is a
                    // contiguous paid PREFIX, but this leg consumes a SUFFIX of the
                    // sibling's covered range (starting at `offset`) — so it may only
                    // EXTEND the sibling's frontier INTO the overlap, never claim the
                    // sibling's `[start, offset)` prefix, which only the sibling's OWN
                    // observers pay for. Guard on the sibling having itself already
                    // cleared up to `offset`: only then is this leg's payment a sound
                    // prefix extension (each overlap byte is fetched once and recouped by
                    // the fastest of its shared observers — DECISION-B). Without the
                    // guard a fast overlap payer would relax the sibling pull's window
                    // over bytes no one has paid for. Empty in the common N=1 case.
                    for extra in also_pace {
                        if extra.served_frontier().load(Ordering::Relaxed) >= offset {
                            extra.served_frontier().fetch_max(served, Ordering::Relaxed);
                            extra.served_advanced().notify_waiters();
                        }
                    }
                }
                // Re-queue deltas the client had not paid yet (a short batch),
                // preserving order at the front.
                for &delta in deltas
                    .get(outcome.committed..)
                    .unwrap_or_default()
                    .iter()
                    .rev()
                {
                    pending.push_front(delta);
                }
                match outcome.stop {
                    // A voucher was rejected (or the commit hit `RetryLater`): the
                    // rejection was already written and any valid prefix committed
                    // + acked. Stop cleanly; the caller drops the pull leg. An
                    // underpaid/rejected voucher is a client-abandon (#856) — meter it
                    // so a node paying upstream for a client that won't pay is
                    // alertable.
                    BatchStop::Rejected => {
                        // DIAG#1673: capture-on-failure — a voucher was rejected /
                        // hit `RetryLater`; the serve stops WITHOUT `StreamEnd`, which
                        // the client sees as `early eof`.
                        eprintln!(
                            "DIAG#1673 serve_leg.voucher_rejected hash={hash} delivered={delivered} \
                             paid={paid} pull_outcome={:?} observers={} cancelled={}",
                            session.outcome(),
                            session.observer_count(),
                            session.is_cancelled(),
                        );
                        self.metrics.node_pull_through_client_abandoned();
                        return Ok(());
                    }
                    BatchStop::Continue => {}
                }
            }

            // Done when the whole range is on the wire and every interval — closing
            // partial included — has been paid. Gating on PAID (not delivered) is
            // what bills the credit-window tail the client received ahead of its
            // voucher.
            let done = done_delivering && pending.is_empty() && unvouchered == 0;

            // ADR 011 §On Blacklist Event: terminate an in-flight delivery at the
            // next voucher boundary once a takedown lands. Gated on `collected_any`
            // (runs only after a committed batch, so bytes already on the wire stay
            // paid) and `!done` (a complete, fully-paid delivery must finish with
            // `StreamEnd`, not a reset). Abandoning the concurrent pull of the
            // taken-down blob is the pull leg's own takedown handling, reached when
            // this return drops it.
            if collected_any && !done && self.takedown_landed(hash, Some(funder)) {
                // DIAG#1673: capture-on-failure — an in-flight takedown terminated the
                // serve mid-stream. Not expected in these tests, but recorded so CI can
                // rule it out as the mid-stream close cause.
                eprintln!(
                    "DIAG#1673 serve_leg.takedown hash={hash} delivered={delivered} paid={paid} \
                     observers={} cancelled={}",
                    session.observer_count(),
                    session.is_cancelled(),
                );
                self.terminate_for_takedown(send, recv, hash);
                return Ok(());
            }

            if done {
                break;
            }

            // No-progress guard: an iteration that delivered no new byte (delivery
            // blocked on the credit window, waiting for payment) AND cleared no
            // voucher (the client stopped paying — `collect_voucher_batch` timed out
            // with nothing committed) cannot make progress. The client has abandoned
            // (#856 drop-after-fill): stop cleanly. The caller then cancels the pull
            // leg, which bounds the upstream spend (#1610) and persists the buyer
            // watermark (#852). Any delivery or payment this iteration resets it, so
            // an honest-but-slow client (patience = one `collect_voucher_batch`
            // read timeout) is never dropped early.
            let made_delivery_progress = delivered > delivered_at_iter_start;
            let made_payment_progress = committed_this_iter > 0;
            if !made_delivery_progress && !made_payment_progress {
                // DIAG#1673: capture-on-failure — the no-progress guard fired: no new
                // byte delivered AND no voucher cleared this iteration. Under CI
                // coverage-starvation a starved pull leg could leave the serve unable to
                // deliver while the client is still paying, tripping this guard and
                // stopping WITHOUT `StreamEnd` — exactly the `early eof` symptom. Prime
                // suspect: record the full state (done_delivering, pending, unvouchered).
                eprintln!(
                    "DIAG#1673 serve_leg.no_progress hash={hash} delivered={delivered} paid={paid} \
                     done_delivering={done_delivering} pending={} unvouchered={unvouchered} \
                     next_chunk_is_some={} pull_outcome={:?} observers={} cancelled={}",
                    pending.len(),
                    next_chunk.is_some(),
                    session.outcome(),
                    session.observer_count(),
                    session.is_cancelled(),
                );
                // The client stopped paying (#856 drop-after-fill): meter the abandon,
                // then stop cleanly.
                self.metrics.node_pull_through_client_abandoned();
                return Ok(());
            }
        }

        // Fully delivered and fully paid: signal clean completion. Every byte was
        // bao-verified into the cache by the pull leg's admit before this leg read
        // it, so the served bytes are sound.
        // DIAG#1673: capture-on-failure — the clean completion marker. Present so a
        // failing CI job shows whether a serve leg reached `StreamEnd` at all (and for
        // which hash), separating "closed early" from "the OTHER leg is the failure".
        if let Err(e) = self.write_message(send, &ClientMessage::StreamEnd).await {
            eprintln!(
                "DIAG#1673 serve_leg.stream_end_write_failed hash={hash} delivered={delivered} \
                 paid={paid} err={e:#}",
            );
            return Err(e);
        }
        eprintln!(
            "DIAG#1673 serve_leg.stream_end_ok hash={hash} delivered={delivered} paid={paid}"
        );
        let _ = send.finish();
        Ok(())
    }
}
