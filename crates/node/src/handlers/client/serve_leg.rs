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
//! # Coordination with the pull leg (shared `FillSession` state)
//!
//! The pull leg runs on its own runtime. The legs share the `FillSession` behind an
//! `Arc`, whose frontiers and wakeups work across runtimes:
//!
//! - **`served_paid`** — the client's PAID *content* frontier. The serve leg
//!   raises it after every voucher batch commits (mapped from paid WIRE bytes
//!   through [`content_paid_frontier`]); the pull leg's `WindowPacer` reads it to
//!   bound `pulled − served_paid ≤ window`, plus one floor to serve `serve_demand`.
//! - **`serve_demand`** — the content end of the span this leg's encoder waits
//!   on, for a leaf or a proof node the store does not hold yet, published only
//!   when this leg's frame producer has no encoded bytes left. The serve window
//!   meters wire and the pull window meters content, so the pull window can close
//!   while this leg still has credit room and waits on bytes. This leg waits on
//!   the first byte the pull has not fetched, so `serve_demand` lands within one
//!   group past the pull's frontier, and the pull fetches one window floor past its
//!   window, so neither leg waits on the other forever.
//! - **Downstream wakeup** — every move of `served_paid` or `serve_demand` wakes
//!   a pull leg parked in `PaceDecision::Wait` (`DownstreamWatch::past`), so it
//!   re-decides exactly when the paid frontier advances or this leg starts
//!   waiting. A voucher batch that stays inside one chunk group moves nothing and
//!   wakes nothing.
//! - **Pull outcome + liveness** — the pull records its outcome with
//!   `FillSession::mark_ended`, which fires the per-hash liveness signal. A serve
//!   read parked on a gap races the present-range watch against that signal and
//!   re-checks `range_still_live`: once no live fill covers the gap the read
//!   fails instead of hanging. This is the no-hang guarantee.
//!
//! The serve leg owns termination: it is what fulfils `R` for the client, so its
//! completion (or error) ends the serve and drops the pull leg.

use decdn_cache::{FillSession, NodeRangedStore};
use decdn_client::sink::content_paid_frontier;

use super::MAX_PROOFS_PER_CHUNK;
use super::outcome::{ServeEnd, ServeStop};
use super::voucher::{OwedChunk, StreamAnchor, ensure_unpaid_bytes_tracked};
use super::wire::chunk_frame_bufs;
use super::{
    Arc, B256, BufferedProofReader, CHUNK_BYTES, CHUNK_GROUP_BYTES, ClientHandler, ClientMessage,
    FloorReservation, Hash, LaneDeliveryState, LaneKey, Mutex, RecvStream, SendStream, U256,
    VecDeque, VoucherRejectReason, VoucherStop,
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
    /// A client disconnect (the write / voucher-collect surfaces it), a rate-check
    /// bail, an `encode_range` fault, an offset at or past the blob
    /// end, or a gap the pull leg could not fill. On any error the caller drops the
    /// pull leg, which stops the upstream spend and persists the buyer watermark.
    /// The peer-attributable errors carry
    /// [`PeerFault`](super::wire::PeerFault) or
    /// [`ClientPaymentFault`](super::wire::ClientPaymentFault); the rest reach the
    /// dispatch sink's `error!` as node faults.
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
        floor_reservation: Option<&FloorReservation>,
    ) -> anyhow::Result<ServeEnd> {
        // An offset at or past the blob end has no bytes to deliver, and the
        // clamp below would fold it into the legitimate `end == offset` empty
        // request — a signed non-empty promise answered with zero bytes and a
        // clean `StreamEnd`. Refuse it instead. `dispatch`'s bounds gate already
        // refuses the same ranges with `RangeNotSatisfiable` before it signs, so
        // this is a backstop for a caller that skips that gate; it mirrors
        // `align_range`'s own bound.
        anyhow::ensure!(
            total_bytes == 0 || offset < total_bytes,
            "serve offset {offset} is at or past the {total_bytes}-byte blob end"
        );

        // Resolve the request end. `len == 0` ⇒ to the blob end (driver
        // convention); otherwise clamp to the tree size.
        let end = if len == 0 {
            total_bytes
        } else {
            offset.saturating_add(len).min(total_bytes)
        };
        let offset = offset.min(end);
        // The wire this leg delivers starts at the chunk-group floor of `offset`
        // (`align_range` snaps the fetch start down), so paid wire maps back to
        // content from THAT boundary — `content_paid_frontier` requires a
        // group-aligned fetch start. Equal to `offset` for an aligned request.
        let fetch_start = (offset / CHUNK_GROUP_BYTES).saturating_mul(CHUNK_GROUP_BYTES);

        let chunk_bytes = CHUNK_BYTES;

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
        // remainder), and the completed intervals whose payment is still owed —
        // together they are exactly `delivered − paid`. A proof that pays part of
        // an interval leaves its remainder owed in `pending`.
        let mut unvouchered: u64 = 0;
        let mut pending: VecDeque<OwedChunk> = VecDeque::new();
        // One buffered voucher reader for the whole stream: every read goes
        // through it so a pipelined voucher buffered ahead of the current one is
        // not lost.
        let mut reader = BufferedProofReader::default();
        // This stream's chain anchor — which epoch it has been told about, so a
        // bare reveal arriving on it can be placed (ADR 003 §Concurrent
        // Streams, Rule 1). Per stream and in memory only: a restart drops the
        // streams, and the lane's durable record keeps the strongest claim's
        // chain state.
        let mut anchor = StreamAnchor::default();

        // The coherent whole-range bao encoder (ADR 038): ONE verified stream for
        // `R`, produced incrementally — leaf data awaited from the cache the pull
        // fills, proof nodes from the shared `outboard` the pull captures.
        let mut producer = super::serve_encoder::CoherentFrameProducer::new(
            store,
            Arc::clone(&session),
            offset,
            end,
            total_bytes,
        )?;

        // The first frame — awaiting the pull leg if `R` opens on a gap. A pull
        // that ends `Err` here fails the serve rather than hanging.
        let opening_window = self.credit_window(chunk_bytes, 0);
        let mut next_chunk = producer
            .next_frame_chunks(self.frame_target(0, chunk_bytes, opening_window))
            .await
            .map_err(|e| self.meter_frame_fault(e))?;

        // Wall-clock cadence for the mid-stream pool-solvency re-check below (ADR
        // 003 §Pool solvency). Start the clock at loop entry — admission already
        // verified solvency once via `try_reserve_floor`, so the first re-check
        // falls one interval later, bounding over-delivery from admission to
        // `interval × per-stream-rate`.
        let pool_recheck_interval = self.pool_recheck_interval();
        let mut last_pool_check = std::time::Instant::now();

        // The signer's capability `cap` the node holds on this lane, for the
        // mid-stream signer cap-headroom re-check below (ADR 003 §Pool solvency).
        // Fixed for the stream, so read once here.
        let held_signer_cap: U256 = lane.lock().await.state.cap;

        loop {
            // Progress trackers for the no-progress guard below.
            let delivered_at_iter_start = delivered;
            let mut credited_this_iter = 0u64;

            // The ramped window for the payment confirmed so far (ADR 003 §Credit
            // window), recomputed each iteration exactly as the cache-hit twin in
            // `deliver` does. Floored at one chunk so the loop can always make
            // progress: deliver a full chunk, then recoup the proof that pays it.
            let window = self.credit_window(chunk_bytes, paid);

            // --- deliver phase: stream frames while the window has room. Checked
            // BEFORE each send, so `delivered − paid` overshoots by at most the one
            // frame that crosses the threshold. ---
            while next_chunk.is_some() {
                if delivered.saturating_sub(paid) >= window {
                    break;
                }
                let Some(frame) = next_chunk.take() else {
                    break;
                };
                let clen_u64 = frame.total() as u64;
                // Assemble before the write, and outside the metered block: a framing
                // fault here is the node's own bug, and metering it as a client
                // abandon would file it under the peer's behaviour.
                let bufs = chunk_frame_bufs(&frame).map_err(|e| self.meter_frame_fault(e))?;
                // A downstream drop surfaces here as `Err` (#856 client-disconnect
                // shape); meter the client-abandon, then propagate so the caller drops
                // the pull leg.
                if let Err(e) = self.write_chunk_bufs(send, bufs).await {
                    self.metrics.node_pull_through_client_abandoned();
                    return Err(e);
                }
                delivered = delivered.saturating_add(clen_u64);
                self.shed.record_egress(clen_u64);
                self.metrics.bytes_served(clen_u64);
                unvouchered = unvouchered.saturating_add(clen_u64);
                if unvouchered >= chunk_bytes {
                    pending.push_back(OwedChunk::new(unvouchered));
                    unvouchered = 0;
                }
                // Prefetched one pass ahead of the window check; size it against what
                // remains after this send (see the twin in `deliver`). The room cap is
                // what keeps this from parking on upstream bytes that this loop must
                // exit to recoup before they can be pulled.
                let room = window.saturating_sub(delivered.saturating_sub(paid));
                next_chunk = producer
                    .next_frame_chunks(self.frame_target(unvouchered, chunk_bytes, room))
                    .await
                    .map_err(|e| self.meter_frame_fault(e))?;
            }
            let done_delivering = next_chunk.is_none();

            // Once the whole range is on the wire, fold the closing sub-interval
            // remainder into `pending` so the recoup batch drains it uniformly.
            if done_delivering && unvouchered > 0 {
                pending.push_back(OwedChunk::new(unvouchered));
                unvouchered = 0;
            }

            // --- recoup phase: collect proofs until each completed chunk is
            // paid for. The proof advances the in-memory lane watermark; the
            // background flush persists it (ADR 003 §Off-chain voucher state
            // persistence). ---
            //
            // The loop reads proofs for a chunk until they have credited all of it:
            // the payer may send this stream's root voucher, or a rollover voucher,
            // ahead of the reveal that pays, and a metering voucher credits no
            // whole chunk. A proof can also pay only part of a chunk, and the
            // remainder stays owed. `MAX_PROOFS_PER_CHUNK` bounds how many proofs
            // may leave a chunk unsettled — see the twin loop in `deliver` for the
            // full reasoning.
            let collected_any = !pending.is_empty();
            'chunk: while let Some(mut owed) = pending.pop_front() {
                let mut attempts = 0u32;
                loop {
                    attempts = attempts.saturating_add(1);
                    let stop = match self
                        .commit_one_proof(
                            send,
                            recv,
                            &mut reader,
                            &mut anchor,
                            hash,
                            lane_key,
                            Some(lane),
                            client_node_id,
                            rate_per_mb,
                            owed,
                        )
                        .await
                    {
                        Ok(stop) => stop,
                        Err(e) => {
                            // Transport drop or rate-check bail (#856/#857): meter the
                            // abandon, then propagate so the caller drops the pull leg.
                            // A node-side fault (a lane-store failure, a broken
                            // invariant) carries no peer marker. The dispatch sink
                            // meters it on `decdn_serve_stream_node_fault_total`
                            // instead.
                            if super::wire::is_peer_attributable(&e) {
                                self.metrics.node_pull_through_client_abandoned();
                            }
                            return Err(e.context(format!(
                                "proof {attempts} of at most {MAX_PROOFS_PER_CHUNK} for a {}-byte \
                                 chunk ({} bytes owed, {} more queued)",
                                owed.len(),
                                owed.remaining(),
                                pending.len()
                            )));
                        }
                    };
                    match stop {
                        VoucherStop::Continue { credited_bytes } => {
                            if credited_bytes > 0 {
                                // Advance `paid` by the watermark-capped credit (rule
                                // #1): a benign already-satisfied voucher raises the
                                // watermark by nothing, so it credits nothing here and
                                // cannot reopen the credit window for bytes the lane
                                // has not settled.
                                credited_this_iter =
                                    credited_this_iter.saturating_add(credited_bytes);
                                paid = paid.saturating_add(credited_bytes);
                                // Publish the PAID CONTENT frontier for the pull leg's
                                // `WindowPacer`, mapping paid WIRE back into content
                                // space (the largest chunk-group boundary provably
                                // inside the paid wire prefix — conservative, so the
                                // pull never overshoots its window). One contiguous
                                // delivery from `fetch_start`, so it is the single
                                // fetch-start.
                                let served = content_paid_frontier(fetch_start, total_bytes, paid);
                                // Forward-only, and guarded on THIS leg's start: an
                                // owning session's frontier starts at the raw
                                // `byte_offset`, at or above its group floor
                                // `fetch_start`, so the guard is a no-op and N
                                // whole-range observers advance the SHARED frontier
                                // with the pull's `WindowPacer` binding on the
                                // MAX-over-observers paid frontier. An observer
                                // ATTACHED at an offset the owner's paid prefix has not
                                // reached yet must not lift that prefix past bytes
                                // nobody paid for; its payment extends the frontier
                                // once the prefix reaches it
                                // (`FillSession::extend_served_from`).
                                session.extend_served_from(fetch_start, served);
                                // Under partial-overlap coalescing each attached sibling
                                // pull produces the OVERLAP this leg also consumes and
                                // bills; the same guard applies to every sibling.
                                for extra in also_pace {
                                    extra.extend_served_from(fetch_start, served);
                                }
                            }
                            if owed.settle(credited_bytes)? {
                                continue 'chunk;
                            }
                            if attempts >= MAX_PROOFS_PER_CHUNK {
                                self.metrics.node_pull_through_client_abandoned();
                                // A payer that spends its per-chunk proof budget
                                // without settling the chunk is a client payment
                                // fault, not a node bug.
                                return Err(anyhow::Error::new(super::wire::ClientPaymentFault)
                                    .context(format!(
                                        "payer sent {attempts} proofs without settling one \
                                         {}-byte chunk ({} bytes still owed)",
                                        owed.len(),
                                        owed.remaining()
                                    )));
                            }
                        }
                        VoucherStop::Rejected => {
                            self.metrics.node_pull_through_client_abandoned();
                            return Ok(ServeEnd::Stopped {
                                stop: ServeStop::VoucherRejected,
                                bytes: delivered,
                            });
                        }
                    }
                }
            }
            ensure_unpaid_bytes_tracked(delivered, paid, unvouchered)?;

            // Reconcile the floor reservation against this stream's live balance now
            // that `paid` has advanced (mirrors `deliver`). `paid`/`delivered` are BYTE
            // counters; the reservation accounts in µUSDC, so every byte quantity
            // crosses over through `min_payment` — never compared directly.
            if let Some(res) = floor_reservation {
                // Free the pool's live reservation once cumulative payment reaches the
                // amount reserved (the ramp-floor credit this stream fronts). Matching
                // release to the reserved µUSDC keeps it correct at any
                // `credit_ramp_divisor` — with the ramp disabled the reservation is the
                // full `credit_max`, so release waits for that much paid, not one chunk.
                res.release_if_repaid(decdn_incentive::min_payment(paid, rate_per_mb));
            }

            // Done when the whole range is on the wire and every interval — closing
            // partial included — has been paid. Gating on PAID (not delivered) is
            // what bills the credit-window tail the client received ahead of its
            // voucher.
            let done = done_delivering && pending.is_empty() && unvouchered == 0;

            // Mid-stream pool-solvency re-check (ADR 003 §Pool solvency),
            // symmetric with the takedown re-check below: re-read the cached pool
            // status and stop once `remaining − M` no longer covers the pool's
            // already-committed floor credit. A pool drains mid-flight — other
            // lanes redeem `remaining` down — so a long
            // stream must catch that; the credit window bounds delivery ahead of
            // the last COLLECTED voucher, not ahead of the last SOLVENCY-verified
            // point, so nothing else notices a shared pool going unredeemable.
            // `new_reserve = ZERO` asks exactly that: is the total ALREADY
            // committed (this stream included) still within budget? On `false` the
            // pool can no longer fund further credit, so stop IN-BAND with a clean
            // `PoolExhausted` (not a QUIC reset, unlike a takedown) so the owner
            // learns to top up — `PoolExhausted` is post-auth, so naming the
            // condition leaks nothing an open-time refusal must hide, and it is not
            // watermark-gated (no bundle). A `None` pool view fails OPEN (the
            // on-chain redeem is the backstop). The caller drops the concurrent
            // pull leg when this returns, bounding the upstream spend just as the
            // takedown and no-progress exits do.
            //
            // The check runs on a WALL-CLOCK cadence, not at every chunk
            // boundary: the projection only advances as the settlement watcher
            // folds redeem events, so re-reading it faster than that returns the
            // same value (wasted `pool_floor` lock reads on the fastest streams),
            // and per-stream throughput is bounded, so the interval bounds
            // worst-case over-delivery on a drained pool to `interval ×
            // per-stream-rate`. Only the frequency changes — the reject behavior
            // and the on-chain redeem backstop are unchanged.
            if collected_any && !done && last_pool_check.elapsed() >= pool_recheck_interval {
                last_pool_check = std::time::Instant::now();
                if let Some(status) = self.pool_view_status_cached(lane_key.pool_id).await
                    && !self.pool_budget_covers_reserve(
                        lane_key.pool_id,
                        status.remaining,
                        U256::ZERO,
                    )
                {
                    self.write_reject(send, VoucherRejectReason::PoolExhausted, None)
                        .await?;
                    self.metrics.serve_stream_midstream_pool_exhausted();
                    // Observable stop (symmetric with the takedown re-check below): this
                    // terminates a paying miss-leg delivery.
                    tracing::warn!(
                        pool_id = %lane_key.pool_id, stopped_signer = %lane_key.signer, %hash,
                        "mid-stream PoolExhausted: pool can no longer fund the floor credit committed across its signers; owner should top up the deposit. This is the POOL level — stopped_signer names the terminated stream, not the cause; a per-signer gate refusal never reaches here"
                    );
                    return Ok(ServeEnd::Stopped {
                        stop: ServeStop::PoolExhausted,
                        bytes: delivered,
                    });
                }

                // Mid-stream SIGNER cap-headroom re-check (ADR 003 §Pool solvency),
                // the per-signer sibling of the pool re-check above and on the same
                // wall-clock cadence (see the cache-hit twin in `deliver` for the full
                // rationale). A signer's `cap` is shared across every provider, so a
                // client draining it at ANOTHER node mid-stream is invisible to the
                // pool re-check — the pool's `remaining` stays healthy on other
                // signers' budgets. Reads the drain from the event-fed projection (no
                // chain call), stops IN-BAND with a clean `SignerCapExhausted`, fails
                // toward serving on a projection gap, and only BOUNDS over-delivery —
                // the on-chain `min(desired, cap − spent)` redeem is the backstop.
                if self
                    .signer_cap_drained_midstream(
                        lane_key.pool_id,
                        lane_key.signer,
                        held_signer_cap,
                        rate_per_mb,
                    )
                    .await
                {
                    self.write_reject(send, VoucherRejectReason::SignerCapExhausted, None)
                        .await?;
                    tracing::warn!(
                        pool_id = %lane_key.pool_id, stopped_signer = %lane_key.signer, %hash,
                        "mid-stream SignerCapExhausted: this signer drained its shared cap at other nodes since admission, so its cap−spent headroom no longer covers the committed floor; owner should raise the signer's cap or delegate a fresh capability. This is the SIGNER level, distinct from PoolExhausted — the pool may still be solvent on other signers' budgets"
                    );
                    return Ok(ServeEnd::Stopped {
                        stop: ServeStop::SignerCapExhausted,
                        bytes: delivered,
                    });
                }
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
                return Ok(ServeEnd::Stopped {
                    stop: ServeStop::Takedown,
                    bytes: delivered,
                });
            }

            if done {
                break;
            }

            // No-progress guard: an iteration that delivered no byte and credited
            // no byte cannot change what the next one sees, so it would repeat
            // forever — see the twin guard in `deliver`. A client that stops paying
            // never reaches it: its voucher read times out or fails, and the recoup
            // phase returns that error. Reaching it means the accounting broke, so
            // the stream fails as a node fault. The caller then drops the pull leg,
            // which bounds the upstream spend.
            if delivered == delivered_at_iter_start && credited_this_iter == 0 {
                return Err(anyhow::anyhow!(
                    "serve loop made no progress: {delivered} bytes delivered, {paid} paid, \
                     window {window}, range fully delivered: {done_delivering}"
                ));
            }
        }

        // Fully delivered and fully paid. Every byte was bao-verified into the cache by
        // the pull leg's admit before this leg read it, so the served bytes are sound.
        // The floor reservation was already released once payment reached the reserved
        // floor; the guard drops as this leg returns.
        self.write_message(send, &ClientMessage::StreamEnd).await?;
        let _ = send.finish();
        // Drain the client's send half to its FIN before `recv` drops, so the
        // client's finish is not stopped by the implicit `STOP_SENDING(0)` a
        // dropped `RecvStream` issues (see `drain_recv_to_fin`).
        super::drain_recv_to_fin(recv).await;
        // ADR 040: emit the ONE hit sighting for this served request, only after the
        // terminal StreamEnd frame is written — so a serve that fails to finish cleanly
        // is never counted. This is necessarily after the concurrent pull leg filled the
        // whole range and ran its fill-time admission read, so the serve's `observe`
        // follows the admission estimate read, preserving the ordering invariant even
        // though the two legs run concurrently. One sighting per served blob, regardless
        // of range or interval count.
        self.cache.observe_hit(hash);
        // ADR 041: credit the realized operator margin back to the source that
        // speculatively warmed this blob (a no-op for an untagged / non-speculative
        // hash), on the same clean-completion edge as the ADR 040 hit sighting.
        self.credit_warming_serve(hash, delivered);
        Ok(ServeEnd::Completed { bytes: delivered })
    }
}
