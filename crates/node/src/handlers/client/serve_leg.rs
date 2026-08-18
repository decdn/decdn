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
    Arc, B256, BufferedVoucherReader, ChunkData, ClientHandler, ClientMessage, FloorReservation,
    Hash, LaneDeliveryState, LaneKey, Mutex, Ordering, RecvStream, SendStream, U256,
    VOUCHER_INTERVAL_BYTES, VecDeque, VoucherRejectReason, VoucherStop,
};

impl ClientHandler {
    /// Deliver the requested range `R = [offset, offset + len)` to the paying
    /// client from the cache, awaiting a concurrent pull leg to fill any gaps —
    /// the downstream (seller) half of the decoupled serve-miss (ADR
    /// 037). `len == 0` means "to the end of the blob".
    ///
    /// Delivery, billing, the credit window, per-voucher recoup, the
    /// in-flight takedown boundary, and client-disconnect handling all read from
    /// the cache: a coherent whole-range bao encoder reads the cache the pull leg
    /// fills. The caller (orchestration) has
    /// already proven channel ownership, run the pre-flight gates, and signed +
    /// sent the `StreamResponse`; the pull leg fills the store beside this call.
    /// Consumes neither stream — the caller does.
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
    // One linear, ADR-ordered miss-leg serve loop; splitting it would scatter the
    // ordering invariants across helpers (same rationale as `deliver`).
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    pub(super) async fn serve_leg(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        store: NodeRangedStore,
        session: Arc<FillSession>,
        also_pace: &[Arc<FillSession>],
        lane: &Arc<Mutex<LaneDeliveryState>>,
        hash: Hash,
        lane_key: LaneKey,
        client_node_id: B256,
        rate_per_mb: u64,
        offset: u64,
        len: u64,
        total_bytes: u64,
        window: u64,
        floor_reservation: Option<&FloorReservation>,
    ) -> anyhow::Result<()> {
        // Resolve the request end. `len == 0` ⇒ to the blob end (driver
        // convention); otherwise clamp to the tree size.
        let end = if len == 0 {
            total_bytes
        } else {
            offset.saturating_add(len).min(total_bytes)
        };
        let offset = offset.min(end);

        let interval_bytes = VOUCHER_INTERVAL_BYTES;
        // Backpressure bound, floored at one interval so the loop can always make
        // progress (deliver a full interval, then recoup its voucher).
        let window = window.max(interval_bytes);

        // The in-flight takedown re-check (ADR 011) keys on the pool FUNDER (the
        // pool owner, `getPool.owner`), resolved from the cached pool-view. `None`
        // (no view wired or a read fault) falls back to the open-time gates + the
        // hash-denylist re-check.
        let funder: Option<super::Address> = self.pool_funder(lane_key.pool_id).await;

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
        // One buffered voucher reader for the whole stream: every read goes
        // through it so a pipelined voucher buffered ahead of the current one is
        // not lost.
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
        let mut next_chunk = producer.next_frame().await?;

        loop {
            // Progress trackers for the no-progress guard below: an iteration that
            // delivers no new byte AND clears no voucher has stalled — the client
            // stopped paying.
            let delivered_at_iter_start = delivered;
            let mut credited_this_iter = 0u64;

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
                    self.metrics.node_pull_through_client_abandoned();
                    return Err(e);
                }
                delivered = delivered.saturating_add(clen);
                unvouchered = unvouchered.saturating_add(clen);
                if unvouchered >= interval_bytes {
                    pending.push_back(unvouchered);
                    unvouchered = 0;
                }
                next_chunk = producer.next_frame().await?;
            }
            let done_delivering = next_chunk.is_none();

            // Once the whole range is on the wire, fold the closing sub-interval
            // remainder into `pending` so the recoup batch drains it uniformly.
            if done_delivering && unvouchered > 0 {
                pending.push_back(unvouchered);
                unvouchered = 0;
            }

            // Reconcile the pool floor reservation the SAME way the hit path does
            // (`deliver`), so a cache-miss stream — which fronts upstream USDC — folds
            // proportional `dead_charge` too. Capture this iteration's maximum in-flight
            // unpaid balance NOW: after the deliver phase advanced `delivered` and
            // before the recoup phase can advance `paid` or take the `VoucherStop::Rejected`
            // early return / a `?` fault below. A stream that dies in its first
            // iteration never reaches the end-of-iteration hook, so without this note its
            // last-noted unpaid stays 0 and `Drop` would fold nothing — letting "connect,
            // take one free interval, vanish" escape the `dead_charge` accounting.
            // `delivered`/`paid` are WIRE BYTES (see their declaration); the reservation
            // accounts in µUSDC, so the byte quantity crosses over through `min_payment` —
            // the two units are never compared directly (ADR 003 §Pool solvency).
            if let Some(res) = floor_reservation {
                res.note_unpaid(decdn_incentive::min_payment(
                    delivered.saturating_sub(paid),
                    rate_per_mb,
                ));
            }

            // --- recoup phase: recoup each completed interval with one voucher.
            // The voucher advances the in-memory lane watermark; the background
            // flush persists it (ADR 003 §Off-chain voucher state persistence). ---
            let collected_any = !pending.is_empty();
            while let Some(delta) = pending.pop_front() {
                let stop = match self
                    .commit_one_voucher(
                        send,
                        recv,
                        &mut reader,
                        hash,
                        lane_key,
                        Some(lane),
                        client_node_id,
                        rate_per_mb,
                        delta,
                    )
                    .await
                {
                    Ok(stop) => stop,
                    Err(e) => {
                        // Transport drop or underpayment bail (#856/#857): meter the
                        // abandon, then propagate so the caller drops the pull leg.
                        self.metrics.node_pull_through_client_abandoned();
                        return Err(e);
                    }
                };
                match stop {
                    VoucherStop::Continue { credited_bytes } => {
                        // Advance `paid` by the watermark-capped credit (rule #1): a
                        // benign already-satisfied voucher raises the watermark by
                        // nothing, so it credits nothing here and cannot reopen the
                        // credit window for bytes the lane has not settled.
                        credited_this_iter = credited_this_iter.saturating_add(credited_bytes);
                        paid = paid.saturating_add(credited_bytes);
                        // Publish the PAID CONTENT frontier for the pull leg's
                        // `WindowPacer`, mapping paid WIRE back into content space (the
                        // largest chunk-group boundary provably inside the paid wire
                        // prefix — conservative, so the pull never overshoots its
                        // window). One contiguous delivery from `offset`, so `offset`
                        // is the single fetch-start.
                        let served = content_paid_frontier(offset, total_bytes, paid);
                        // `fetch_max`, not `store`: N observers advance the SHARED
                        // frontier and the pull's `WindowPacer` binds on the
                        // MAX-over-observers paid frontier (DECISION-B), so a slower
                        // observer must not regress a faster one. Behavior-preserving
                        // for N=1 (a single contiguous delivery is already monotone,
                        // so `fetch_max == store`).
                        session
                            .served_frontier()
                            .fetch_max(served, Ordering::Relaxed);
                        session.served_advanced().notify_waiters();
                        // Under partial-overlap coalescing this serve leg is fed by
                        // more than its own pull: each attached sibling pull produces
                        // the OVERLAP this leg also consumes and bills. The sibling's
                        // `served_paid` is a contiguous paid PREFIX, but this leg
                        // consumes a SUFFIX of the sibling's covered range (starting at
                        // `offset`) — so it may only EXTEND the sibling's frontier INTO
                        // the overlap, never claim the sibling's `[start, offset)`
                        // prefix, which only the sibling's OWN observers pay for. Guard
                        // on the sibling having itself already cleared up to `offset`:
                        // only then is this leg's payment a sound prefix extension (each
                        // overlap byte is fetched once and recouped by the fastest of
                        // its shared observers — DECISION-B). Without the guard a fast
                        // overlap payer would relax the sibling pull's window over bytes
                        // no one has paid for. Empty in the common N=1 case.
                        for extra in also_pace {
                            if extra.served_frontier().load(Ordering::Relaxed) >= offset {
                                extra.served_frontier().fetch_max(served, Ordering::Relaxed);
                                extra.served_advanced().notify_waiters();
                            }
                        }
                    }
                    VoucherStop::Rejected => {
                        self.metrics.node_pull_through_client_abandoned();
                        return Ok(());
                    }
                }
            }

            // Reconcile the floor reservation against this stream's live balance now
            // that `paid` has advanced (mirrors `deliver`). `paid`/`delivered` are BYTE
            // counters; the reservation accounts in µUSDC, so every byte quantity
            // crosses over through `min_payment` — never compared directly.
            if let Some(res) = floor_reservation {
                // Free the pool's live reservation once cumulative payment reaches the
                // amount reserved (the ramp-floor credit this stream fronts). Matching
                // release to the reserved µUSDC keeps it correct at any
                // `credit_ramp_divisor` — with the ramp disabled the reservation is the
                // full `credit_max`, so release waits for that much paid, not one interval.
                res.release_if_repaid(decdn_incentive::min_payment(paid, rate_per_mb));
                // Keep the drop-time reconcile honest with the CURRENT unpaid balance: on
                // an un-repaid stream `Drop` folds `min(reserved, this)` into `dead_charge`.
                // A fully-settled stream ends `delivered == paid`, so the last note here is
                // `min_payment(0, rate) == 0` and `Drop` charges nothing.
                res.note_unpaid(decdn_incentive::min_payment(
                    delivered.saturating_sub(paid),
                    rate_per_mb,
                ));
            }

            // Done when the whole range is on the wire and every interval — closing
            // partial included — has been paid. Gating on PAID (not delivered) is
            // what bills the credit-window tail the client received ahead of its
            // voucher.
            let done = done_delivering && pending.is_empty() && unvouchered == 0;

            // Mid-stream pool-solvency re-check (ADR 003 §Pool solvency),
            // symmetric with the takedown re-check below and under the same
            // `collected_any && !done` boundary gate: re-read the cached pool
            // status after each committed batch. A pool drains mid-flight — other
            // lanes redeem `remaining` down, or `dead_charge` rises — so a long
            // stream must stop once `remaining − M` no longer covers the pool's
            // already-committed floor credit. `new_reserve = ZERO` asks exactly
            // that: is the total ALREADY committed (this stream included) still
            // within budget? On `false` the pool can no longer fund further credit,
            // so stop IN-BAND with a clean `PoolExhausted` (not a QUIC reset, unlike
            // a takedown) so the owner learns to top up — `PoolExhausted` is
            // post-auth, so naming the condition leaks nothing an open-time refusal
            // must hide, and it is not watermark-gated (no bundle). A `None` pool
            // view fails OPEN (the on-chain redeem is the backstop). The caller
            // drops the concurrent pull leg when this returns, bounding the upstream
            // spend just as the takedown and no-progress exits do.
            if collected_any
                && !done
                && let Some(status) = self.pool_view_status_cached(lane_key.pool_id).await
                && !self.pool_budget_covers_reserve(lane_key.pool_id, status.remaining, U256::ZERO)
            {
                self.write_reject(send, VoucherRejectReason::PoolExhausted, None)
                    .await?;
                // Observable stop (symmetric with the takedown re-check below): this
                // terminates a paying miss-leg delivery, and `dead_charge` only grows.
                tracing::warn!(
                    pool_id = %lane_key.pool_id, %hash,
                    "mid-stream PoolExhausted: pool can no longer fund committed floor credit; owner should top up the deposit"
                );
                return Ok(());
            }

            // ADR 011 §On Blacklist Event: terminate an in-flight delivery at the
            // next voucher boundary once a takedown lands. Gated on `collected_any`
            // (runs only after a committed batch, so bytes already on the wire stay
            // paid) and `!done` (a complete, fully-paid delivery must finish with
            // `StreamEnd`, not a reset). Abandoning the concurrent pull of the
            // taken-down blob is the pull leg's own takedown handling, reached when
            // this return drops it.
            if collected_any && !done && self.takedown_landed(hash, funder) {
                self.terminate_for_takedown(send, recv, hash);
                return Ok(());
            }

            if done {
                break;
            }

            // No-progress guard: an iteration that delivered no new byte (delivery
            // blocked on the credit window, waiting for payment) AND cleared no
            // voucher (the client stopped paying — the voucher read timed out with
            // nothing committed) cannot make progress. The client has abandoned
            // (#856 drop-after-fill): stop cleanly. The caller then cancels the pull
            // leg, which bounds the upstream spend (#1610) and persists the buyer
            // watermark (#852). Any delivery or payment this iteration resets it, so
            // an honest-but-slow client (patience = one voucher read timeout) is
            // never dropped early.
            let made_delivery_progress = delivered > delivered_at_iter_start;
            let made_payment_progress = credited_this_iter > 0;
            if !made_delivery_progress && !made_payment_progress {
                // The client stopped paying (#856 drop-after-fill): meter the abandon,
                // then stop cleanly.
                self.metrics.node_pull_through_client_abandoned();
                return Ok(());
            }
        }

        // Fully delivered and fully paid: signal clean completion. Every byte was
        // bao-verified into the cache by the pull leg's admit before this leg read
        // it, so the served bytes are sound. Only reached on clean completion — every
        // abnormal exit returns earlier — so mark the reservation settled, so its drop
        // folds the proportional unpaid tail rather than the conservative full
        // `reserved` (ADR 003 §Pool solvency).
        if let Some(res) = floor_reservation {
            res.mark_settled();
        }
        self.write_message(send, &ClientMessage::StreamEnd).await?;
        let _ = send.finish();
        Ok(())
    }
}
