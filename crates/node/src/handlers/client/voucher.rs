//! Voucher collection / payment loop + cooperative-close authorization.
//! Bodies split from `mod.rs` (#1254). Group-commit batching added in #1483.

use super::{
    Arc, B256, BatchOutcome, BatchStop, BufferedVoucherReader, ChannelDeliveryState, ChannelId,
    ChannelState, ClientHandler, ClientMessage, CooperativeClose, CooperativeCloseAuth,
    CooperativeCloseAuthExt, CooperativeCloseRequest, DEFAULT_TOLERANCE_BPS, Hash, Mutex,
    RateError, RecvStream, SendStream, SignedVoucher, U256, VOUCHER_READ_TIMEOUT,
    VoucherRejectReason, WatermarkBundle, encode_cooperative_close_auth, verify_rate,
    voucher_reject_reason, wire_voucher_to_signed,
};

/// A voucher that passed the node-side verify half against the advancing
/// candidate and is awaiting the batch's single durable commit (#1483).
struct StagedVoucher {
    /// Bytes this voucher pays for (its interval delta) — for `paid` accounting,
    /// the audit receipt, and per-region / seed-leech crediting.
    delta_bytes: u64,
    /// The client's wire nonce (big-endian), for the audit receipt (#248/#803).
    wire_nonce: [u8; 32],
    /// Whether accepting this voucher skipped nonce values (#747 gap metric).
    gapped: bool,
}

/// A voucher that passed the node-side verify half, carrying the advanced
/// candidate state, its cumulative byte watermark, and the staged bookkeeping.
struct VerifiedVoucher {
    next_state: ChannelState,
    new_bytes: U256,
    staged: StagedVoucher,
}

/// Why the node-side verify half stopped the batch at a voucher (#1483). The
/// valid prefix is committed + acked before this is acted on.
enum VerifyStop {
    /// Reject cleanly with this wire reason, then finish the stream (#751).
    /// The optional [`WatermarkBundle`] rides the wallet-less-resume path
    /// (#1481 §5): it is `Some` only for a watermark-gated regression/exhaustion
    /// reason whose rejected voucher recovers to the channel's pinned
    /// `voucher_signer`, and carries the node's true watermark so an authorized
    /// funder can re-seed and resume. Every other reason carries `None`.
    Reject(VoucherRejectReason, Option<WatermarkBundle>),
    /// Fail the stream — there is no wire reason for this fault (a buyer
    /// underpayment), and the client is blocked awaiting `VoucherAck` so it
    /// cannot resend mid-stream (ADR 003 §Voucher withholding).
    Bail(String),
}

impl ClientHandler {
    /// Collect, durably commit, and acknowledge a **batch** of cumulative
    /// vouchers with a single fsync (#1483, group commit).
    ///
    /// `deltas` are the completed interval sizes the serve loop has delivered and
    /// not yet recouped, drained front-to-back (a closing partial is just the
    /// last entry). The batch reads at most `deltas.len()` vouchers: the first
    /// blocking (so the loop makes progress and parks awaiting a voucher exactly
    /// as before), the rest gathered under [`ClientHandler::commit_interval`] so
    /// a client that pauses payment is never waited on longer than that. Each
    /// voucher is verified against an advancing candidate; because vouchers are
    /// cumulative, the whole batch commits as ONE `store.record` of the final
    /// candidate — the highest voucher supersedes every earlier one, so a single
    /// fsync amortizes across the batch with no loss.
    ///
    /// **Durability ordering (ADR 003 §Off-chain voucher state persistence).**
    /// Nothing is acknowledged before it is durable: the fsynced commit runs
    /// first, then a `VoucherAck` is written per committed voucher. A commit
    /// failure fails the WHOLE batch — every voucher gets `RetryLater`, none an
    /// ack — and in-memory state is left unchanged, so the client resends the
    /// batch on a fresh stream. Delaying the ack by one commit interval is free
    /// throughput-wise because #1477's credit window keeps it off the delivery
    /// critical path; it spends window headroom, sized by
    /// `credit_window >= throughput * (RTT + commit_interval)`.
    ///
    /// On a mid-batch verify rejection, the valid prefix is committed + acked
    /// first (one fsync), then the offending voucher's rejection is written —
    /// faithful to the pre-batch per-voucher order. Returns the number of
    /// committed vouchers (so the loop advances `paid` and re-queues any deltas
    /// the client had not yet sent) and whether the stream must end.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn collect_voucher_batch(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        reader: &mut BufferedVoucherReader,
        hash: Hash,
        channel_id: ChannelId,
        channel: Option<&Arc<Mutex<ChannelDeliveryState>>>,
        client_node_id: B256,
        rate_per_mb: u64,
        deltas: &[u64],
    ) -> anyhow::Result<BatchOutcome> {
        // Unknown channel (#327 boundary): `serve_stream` refuses an unknown
        // channel pre-serve, so this arm is unreachable from the sole callers
        // (which always forward `Some`); kept as a defensive backstop.
        let Some(channel) = channel else {
            self.write_reject(send, VoucherRejectReason::WrongChannel, None)
                .await?;
            return Ok(BatchOutcome {
                committed: 0,
                stop: BatchStop::Rejected,
            });
        };

        // (1) GATHER — read up to `deltas.len()` wire vouchers WITHOUT holding
        // the per-channel lock (a network read must not block same-channel
        // streams). The first read blocks under `VOUCHER_READ_TIMEOUT` (a stalled
        // client still errors out); the rest wait only `commit_interval`, so a
        // client that stops paying flushes the batch it has instead of stalling.
        // The reader is cancellation-safe, so a `commit_interval` timeout mid-frame
        // buffers the partial for the next call rather than tearing the stream.
        let mut wires = Vec::with_capacity(deltas.len());
        {
            let first = tokio::time::timeout(VOUCHER_READ_TIMEOUT, reader.read(recv))
                .await
                .map_err(|_| {
                    anyhow::anyhow!("voucher read timed out after {VOUCHER_READ_TIMEOUT:?}")
                })??;
            wires.push(first);
        }
        let commit_interval = self.commit_interval();
        while wires.len() < deltas.len() {
            match tokio::time::timeout(commit_interval, reader.read(recv)).await {
                Ok(Ok(v)) => wires.push(v),
                Ok(Err(e)) => return Err(e),
                Err(_elapsed) => break,
            }
        }

        // (2) VERIFY under the per-channel lock (so the watermark checked is the
        // watermark committed) against an advancing candidate. The candidate is a
        // CLONE — `guard.state` is only swapped after the durable commit below,
        // preserving the #527 invariant.
        let guard = channel.lock().await;

        // Channel-level gates, checked once for the batch (ADR 003). A batch spans
        // milliseconds, so a mid-batch expiry / cooperative-close race is bounded
        // exactly as the pre-batch per-interval check bounded it.
        if crate::payment_settlement::is_expired(
            crate::payment_settlement::unix_now(),
            guard.state.expires_at,
        ) {
            drop(guard);
            self.write_reject(send, VoucherRejectReason::Expired, None)
                .await?;
            return Ok(BatchOutcome {
                committed: 0,
                stop: BatchStop::Rejected,
            });
        }
        if guard.state.cooperative_close_signed() {
            drop(guard);
            self.write_reject(send, VoucherRejectReason::CooperativeCloseSigned, None)
                .await?;
            return Ok(BatchOutcome {
                committed: 0,
                stop: BatchStop::Rejected,
            });
        }

        let mut candidate = guard.state.clone();
        let mut candidate_bytes = guard.bytes_delivered_cumulative;
        let mut staged: Vec<StagedVoucher> = Vec::with_capacity(wires.len());
        // A verify rejection/bail encountered mid-batch — the valid prefix in
        // `staged` is committed first, then this is emitted.
        let mut pending_stop: Option<VerifyStop> = None;

        for (wire, &delta_bytes) in wires.iter().zip(deltas.iter()) {
            match self.verify_voucher(&candidate, candidate_bytes, wire, rate_per_mb, delta_bytes) {
                Ok(v) => {
                    candidate = v.next_state;
                    candidate_bytes = v.new_bytes;
                    staged.push(v.staged);
                }
                Err(stop) => {
                    pending_stop = Some(stop);
                    break;
                }
            }
        }

        // (3) COMMIT the verified prefix with ONE fsync, then ack after.
        let committed = staged.len();
        if committed > 0 {
            match self
                .commit_batch(
                    send,
                    guard,
                    channel_id,
                    hash,
                    client_node_id,
                    candidate,
                    candidate_bytes,
                    &staged,
                )
                .await?
            {
                // Durable commit failed → the WHOLE batch is RetryLater; nothing
                // was acked and in-memory state did not advance. A pending verify
                // rejection is overridden — durability failure is the actionable
                // signal, and the client must resend the same vouchers.
                CommitOutcome::StoreFailed => {
                    // `RetryLater` is never a watermark-gated reason, so no
                    // bundle is ever attached here (#1481 §5).
                    self.write_reject(send, VoucherRejectReason::RetryLater, None)
                        .await?;
                    return Ok(BatchOutcome {
                        committed: 0,
                        stop: BatchStop::Rejected,
                    });
                }
                CommitOutcome::Committed => {}
            }
        } else {
            // No voucher verified (the first one was rejected/bailed): drop the
            // lock before emitting the rejection, matching the committed path.
            drop(guard);
        }

        // (4) Emit any pending verify rejection/bail AFTER the prefix is durable.
        match pending_stop {
            None => Ok(BatchOutcome {
                committed,
                stop: BatchStop::Continue,
            }),
            Some(VerifyStop::Reject(reason, bundle)) => {
                // The wallet-less-resume bundle (if any) was built inside
                // `verify_voucher` while the per-channel guard was still held —
                // it reports the committed-prefix watermark for a gated reason
                // whose voucher recovered to the pinned signer (#1481 §5).
                self.write_reject(send, reason, bundle).await?;
                Ok(BatchOutcome {
                    committed,
                    stop: BatchStop::Rejected,
                })
            }
            Some(VerifyStop::Bail(msg)) => Err(anyhow::anyhow!(msg)),
        }
    }

    /// The node-side verify half for ONE voucher, evaluated against the advancing
    /// candidate (`state` / `cumulative_bytes`) with no durable side effect. Two
    /// distinct rate checks, mirroring the pre-batch `collect_voucher`:
    /// - the per-delta **advertised-rate** check bails on a genuine underpayment
    ///   (no wire reason, the client is blocked awaiting an ack so cannot resend);
    /// - the cumulative **live-floor** check rejects cleanly with
    ///   `RateFloorRaised` when a governance floor raise made the quote stale
    ///   (#1382), else bails.
    ///
    /// See [`Self::collect_voucher_batch`] for the durable-commit half.
    fn verify_voucher(
        &self,
        state: &ChannelState,
        cumulative_bytes: U256,
        wire: &decdn_protocol::client::Voucher,
        rate_per_mb: u64,
        delta_bytes: u64,
    ) -> Result<VerifiedVoucher, VerifyStop> {
        let new_bytes = cumulative_bytes.saturating_add(U256::from(delta_bytes));
        let amount = U256::from_be_bytes(wire.amount);
        let amount_delta = amount.saturating_sub(state.last_amount());

        // Advertised-rate check (ADR 003 §Voucher withholding). Match every
        // `RateError` arm (#845) so a future variant is a build failure here.
        // `ZeroBytes`/`Overflow` cannot occur (`delta_bytes > 0`) but are rejected
        // defensively.
        match verify_rate(
            amount_delta,
            U256::from(delta_bytes),
            rate_per_mb,
            DEFAULT_TOLERANCE_BPS,
        ) {
            Ok(()) => {}
            Err(RateError::Underpayment { .. }) => {
                return Err(VerifyStop::Bail(format!(
                    "voucher underpays for {delta_bytes} delivered bytes"
                )));
            }
            Err(e @ (RateError::ZeroBytes | RateError::Overflow)) => {
                return Err(VerifyStop::Bail(format!(
                    "voucher fails rate check for {delta_bytes} delivered bytes: {e}"
                )));
            }
        }

        // Hard per-byte price floor (#846), checked on the CUMULATIVE watermark
        // the voucher carries (mirrors the on-chain `_advanceClaimWatermark`
        // `RateFloorViolation` guard at ZERO tolerance). Snapshot the live floor
        // once so the check and the rejection classification cannot disagree.
        let live_floor = self.rate_bounds.floor();
        match verify_rate(amount, new_bytes, live_floor, 0) {
            Ok(()) => {}
            Err(RateError::Underpayment { .. }) => {
                self.metrics.voucher_rate_floor_rejected();
                if live_floor > rate_per_mb {
                    // A governance floor raise landed between the signed quote and
                    // this voucher (#1382): the buyer is honest, its quote is
                    // stale. Surface the typed re-quote signal in-band.
                    // `RateFloorRaised` is not a watermark-gated reason (#1481 §5),
                    // so no bundle is attached.
                    return Err(VerifyStop::Reject(
                        VoucherRejectReason::RateFloorRaised,
                        None,
                    ));
                }
                // The floor did NOT rise above the quote, yet the cumulative
                // payment is still under it — a genuine underpayment the per-delta
                // tolerance let through. The buyer's fault; fail the stream.
                return Err(VerifyStop::Bail(format!(
                    "voucher below the cumulative rate floor for {delta_bytes} delivered bytes"
                )));
            }
            Err(e @ (RateError::ZeroBytes | RateError::Overflow)) => {
                return Err(VerifyStop::Bail(format!(
                    "voucher fails floor check for {delta_bytes} delivered bytes: {e}"
                )));
            }
        }

        let Ok(signed) = wire_voucher_to_signed(wire, state.channel_id, state.token, new_bytes)
        else {
            // `BadSignature` is not a watermark-gated reason (#1481 §5).
            return Err(VerifyStop::Reject(VoucherRejectReason::BadSignature, None));
        };

        // Validate + advance the candidate in memory only (no store). Cumulative
        // vouchers mean the returned `next_state` supersedes `state`, so the batch
        // records only the final candidate (one fsync). A validation error maps to
        // its wire reject reason; `stage_voucher` never touches a store, so it can
        // never surface `RetryLater` here (that is reserved for the commit).
        match state.stage_voucher(&signed, &self.voucher_domain) {
            Ok((next_state, applied)) => Ok(VerifiedVoucher {
                next_state,
                new_bytes,
                staged: StagedVoucher {
                    delta_bytes,
                    wire_nonce: wire.nonce,
                    gapped: applied.is_gapped(),
                },
            }),
            Err(e) => {
                // Map to the wire reject reason; a non-mappable store/validation
                // error falls back to `RetryLater` (never watermark-gated). This
                // preserves the pre-batch `voucher_reject_reason` classification.
                let reason = voucher_reject_reason(&e).unwrap_or(VoucherRejectReason::RetryLater);
                // Wallet-less resume (#1481 §5): for a gated regression/exhaustion
                // reason whose rejected voucher recovers to the pinned signer,
                // attach the node's true watermark so an authorized funder can
                // re-seed and resume. `signed` is the SAME reconstructed voucher
                // just validated above; `state` is the advancing candidate under
                // the still-held per-channel guard, so its `last_*` is exactly the
                // committed-prefix watermark the client should resume from.
                let bundle = self.watermark_bundle_for_reject(reason, &signed, state);
                Err(VerifyStop::Reject(reason, bundle))
            }
        }
    }

    /// Build the wallet-less-resume [`WatermarkBundle`] for a rejected voucher
    /// (#1481 §5), or `None` when the voucher is not eligible. Returns `Some`
    /// only when ALL hold:
    /// - `reason` is one of the four watermark-gated regression/exhaustion
    ///   reasons (`StaleNonce` / `AmountRegression` / `BytesRegression` /
    ///   `InsufficientDeposit`) — every other reason is never gated;
    /// - the `rejected` voucher's signature recovers to `state.voucher_signer`,
    ///   the channel's pinned signer — otherwise anyone who guessed the
    ///   chain-derivable `channel_id` could pull a channel's private watermark
    ///   with a garbage voucher;
    /// - the channel has a prior accepted voucher (`last_signature` is `Some`)
    ///   to echo back.
    ///
    /// The watermark reported is `state`'s last-accepted amount / nonce / bytes.
    /// In the batched flow `state` is the advancing candidate, so this is the
    /// committed-prefix watermark; the caller reads it while still holding the
    /// per-channel guard, before the commit swaps it into the live state.
    fn watermark_bundle_for_reject(
        &self,
        reason: VoucherRejectReason,
        rejected: &SignedVoucher,
        state: &ChannelState,
    ) -> Option<WatermarkBundle> {
        if !reason.is_watermark_gated() {
            return None;
        }
        let recovered = rejected.recover_signer(&self.voucher_domain).ok()?;
        if recovered != state.voucher_signer {
            return None;
        }
        let last_signature = state.last_signature()?;
        Some(WatermarkBundle {
            amount: state.last_amount().to_be_bytes(),
            nonce: state.last_nonce().to_be_bytes(),
            bytes_delivered: state.last_bytes_delivered().to_be_bytes(),
            last_signature: last_signature.to_vec(),
        })
    }

    /// Durably commit the advanced `candidate` (ONE fsynced `store.record`, held
    /// on the blocking pool so it never blocks a runtime worker), swap it into
    /// the live channel state, then write one `VoucherAck` per staged voucher and
    /// run each voucher's post-commit bookkeeping (#1483).
    ///
    /// `guard` is the per-channel lock, still held from verification so no
    /// concurrent voucher advances the watermark between the value checked and
    /// the value committed; it is dropped once the commit lands, before any
    /// network write. On a store failure `guard.state` is NOT advanced and
    /// [`CommitOutcome::StoreFailed`] is returned so the caller rejects the whole
    /// batch with `RetryLater` — never acking un-durable state (#527, ADR 003).
    #[allow(clippy::too_many_arguments)]
    async fn commit_batch(
        &self,
        send: &mut SendStream,
        mut guard: tokio::sync::MutexGuard<'_, ChannelDeliveryState>,
        channel_id: ChannelId,
        hash: Hash,
        client_node_id: B256,
        candidate: ChannelState,
        candidate_bytes: U256,
        staged: &[StagedVoucher],
    ) -> anyhow::Result<CommitOutcome> {
        let store = Arc::clone(&self.channel_state_store);
        let (record_res, candidate) = tokio::task::spawn_blocking(move || {
            let res = store.record(&candidate);
            (res, candidate)
        })
        .await
        .map_err(|e| anyhow::anyhow!("voucher batch commit task failed: {e}"))?;

        if let Err(e) = record_res {
            // Transient store failure (#527): in-memory state did not advance.
            // Surface `RetryLater` to the caller (which finishes the stream
            // cleanly); MUST NOT `VoucherAck` (ADR 003 §355).
            drop(guard);
            tracing::warn!(error = %e, "channel store batch commit failed; rejecting with RetryLater");
            return Ok(CommitOutcome::StoreFailed);
        }

        // Durable. Advance in-memory state to the final cumulative watermark and
        // release the lock before any network write.
        guard.state = candidate;
        guard.bytes_delivered_cumulative = candidate_bytes;
        drop(guard);

        // Post-commit, per-voucher bookkeeping + one `VoucherAck` each. All
        // side effects here are best-effort and off the durability path.
        for s in staged {
            self.record_receipt(hash, s.delta_bytes, client_node_id, s.wire_nonce);
            if let Some(acc) = self.region_accountant.as_ref() {
                acc.record_served(&client_node_id.0, s.delta_bytes).await;
            }
            if let Some(gov) = self.leech_governor.as_ref() {
                gov.record_served(&client_node_id.0, s.delta_bytes);
            }
            if s.gapped {
                self.metrics.voucher_nonce_gap();
            }
            self.write_message(send, &ClientMessage::VoucherAck).await?;
        }

        // Channel-level, once per batch: stamp the admin last-voucher clock and
        // hint the settlement service that the accrued claim advanced (#749/#327).
        if let Some(activity) = self.voucher_activity.as_ref() {
            activity.touch(channel_id);
        }
        if let Some(tx) = self.redeem_hint.as_ref()
            && let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(channel_id)
        {
            self.metrics.redeem_hint_dropped();
        }
        Ok(CommitOutcome::Committed)
    }

    /// Answer a [`CooperativeCloseRequest`] (ADR 003 §Cooperative close): sign a
    /// `CooperativeClose` waiver over the channel's current voucher watermark and
    /// reply with a [`CooperativeCloseAuth`] so the client can settle on-chain
    /// without the dispute window.
    ///
    /// Best-effort by design: an unknown channel, or one with no accepted voucher
    /// yet (`last_nonce == 0` — nothing to waive; the client uses the zero-voucher
    /// close path), is answered by finishing the stream with no auth, and the
    /// client falls back to `closeChannel`. Signing happens before the waiver flag
    /// is persisted, and the flag is persisted (durably, mirroring #527) before
    /// the auth is sent — so a store failure leaves the channel still serveable
    /// and the node has not handed out a waiver it won't remember. Once flagged,
    /// the node serves no further bytes on the channel (the `serve_stream` and
    /// `collect_voucher_batch` gates).
    pub(super) async fn handle_cooperative_close(
        &self,
        mut send: SendStream,
        req: CooperativeCloseRequest,
    ) -> anyhow::Result<()> {
        let channel_id = ChannelId::from(req.channel_id);
        let Some(channel) = self.channels.lock().await.get(&channel_id).cloned() else {
            // Unknown channel — no waiver to give. Finish cleanly; client falls back.
            let _ = send.finish();
            return Ok(());
        };

        let (auth, ext) = {
            // Hold the per-channel lock across read-watermark → sign → persist so
            // a concurrent voucher cannot advance the watermark between the value
            // we sign and the flag we set. Signing is local and fast.
            let mut guard = channel.lock().await;

            // Ownership gate. Signing a waiver is a DURABLE, one-way commitment —
            // the node then serves no further bytes on the channel (the
            // `serve_stream` and `collect_voucher` gates) — and `channel_id =
            // keccak256(client, provider, nonce)` is chain-derivable, so this
            // request MUST prove control of the channel's `voucher_signer` key.
            // The requester signs an off-chain EIP-712 `CooperativeCloseRequest`
            // (recovered under the same `PaymentChannel` voucher domain); we
            // refuse unless it recovers to `state.voucher_signer`. Without this,
            // any peer that can name a channel id could force the node to sign a
            // waiver and permanently freeze the channel.
            //
            // Checked against `voucher_signer`, not the funder `client` — a
            // SIGNER question, matching the delivery path's owner-match gate and
            // the on-chain `cooperativeClose`, which verifies the client voucher
            // against `ch.voucherSigner` (never `ch.client`). The funder role
            // authorizes nothing by signature, so a delegated channel's funder
            // could neither complete the on-chain close nor should it be able to
            // freeze the channel. This is the cooperative-close analogue of the
            // owner-match gate the cooperative-close branch returns above, before
            // that gate runs.
            //
            // Decline == finish the stream with no waiver, byte-for-byte the
            // unknown-channel / zero-voucher decline below, so an unauthorized
            // request stays wire-indistinguishable and leaks neither channel
            // existence nor the watermark. The otherwise-invisible refusal is
            // metered so operators can see the probing.
            let authorized_signer = guard.state.voucher_signer;
            let authorized = matches!(
                decdn_incentive::recover_coop_close_request(
                    B256::from(req.channel_id),
                    &req.client_signature,
                    &self.voucher_domain,
                ),
                Ok(recovered) if recovered == authorized_signer
            );
            if !authorized {
                drop(guard);
                self.metrics.cooperative_close_request_unauthorized();
                tracing::warn!(
                    %channel_id,
                    "cooperative-close request without a valid voucher-signer signature; declining"
                );
                let _ = send.finish();
                return Ok(());
            }

            if guard.state.last_nonce() == U256::ZERO {
                // No voucher accepted yet: nothing to settle. Finish; client
                // falls back to the zero-voucher close.
                drop(guard);
                let _ = send.finish();
                return Ok(());
            }
            let close = CooperativeClose {
                channel_id,
                amount: guard.state.last_amount(),
                nonce: guard.state.last_nonce(),
                bytes_delivered: guard.state.last_bytes_delivered(),
                token: guard.state.token,
            };
            let signed = close
                .sign(self.eth_signer.as_ref(), &self.voucher_domain)
                .map_err(|e| anyhow::anyhow!("cooperative-close waiver signing failed: {e}"))?;
            // Persist the no-longer-serving flag before returning the waiver. On a
            // store failure this propagates (no auth sent) and the channel stays
            // serveable — safe. `mark_cooperative_close_signed` performs a
            // synchronous fsynced redb write, which must not block a runtime
            // worker — run it on the blocking pool against a clone and commit back
            // only on `Ok`, mirroring `apply_voucher` / `update_channel_deposit`.
            let mut candidate = guard.state.clone();
            let store = Arc::clone(&self.channel_state_store);
            let (mark_res, candidate) = tokio::task::spawn_blocking(move || {
                let res = candidate.mark_cooperative_close_signed(&*store);
                (res, candidate)
            })
            .await
            .map_err(|e| anyhow::anyhow!("cooperative-close mark task failed: {e}"))?;
            mark_res?;
            guard.state = candidate;
            let auth = CooperativeCloseAuth {
                channel_id: req.channel_id,
                amount: guard.state.last_amount().to_be_bytes(),
                nonce: guard.state.last_nonce().to_be_bytes(),
                bytes_delivered: guard.state.last_bytes_delivered().to_be_bytes(),
                signature: signed.signature.as_bytes().to_vec(),
            };
            // Echo the client's own last-accepted voucher signature (#1495). It
            // is the signature over exactly the tuple declared above — both are
            // read from the same `guard.state` under the same guard, and the
            // store writes them together on accept — so a client whose persisted
            // watermark lags can verify against its own key that the tuple is one
            // it already signed, instead of dead-ending on the over-claim
            // refusal with its deposit stranded. Same value and same purpose as
            // `WatermarkBundle::last_signature` on the fetch path (#1481).
            //
            // `None` (a hydrated state carrying no signature) sends the bare
            // pre-#1495 auth: the client cannot verify and keeps its refusal.
            let ext = guard
                .state
                .last_signature()
                .map(|last_signature| CooperativeCloseAuthExt {
                    last_signature: last_signature.to_vec(),
                });
            (auth, ext)
        };

        let payload = encode_cooperative_close_auth(&auth, ext.as_ref())
            .map_err(|e| anyhow::anyhow!("cooperative-close auth encode failed: {e}"))?;
        self.write_payload(&mut send, &payload).await?;
        let _ = send.finish();
        Ok(())
    }
}

/// Outcome of the durable half of a voucher batch ([`ClientHandler::commit_batch`]).
enum CommitOutcome {
    /// The batch fsynced, state advanced, and every voucher was acked.
    Committed,
    /// The fsynced `store.record` failed; in-memory state is unchanged and no
    /// voucher was acked. The caller rejects the whole batch with `RetryLater`.
    StoreFailed,
}
