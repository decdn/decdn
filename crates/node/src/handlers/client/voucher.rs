//! Voucher collection / payment loop + cooperative-close authorization.
//! Bodies split from `mod.rs` (#1254).

use super::{
    Arc, B256, ChannelDeliveryState, ChannelId, ClientHandler, ClientMessage, CooperativeClose,
    CooperativeCloseAuth, CooperativeCloseRequest, DEFAULT_TOLERANCE_BPS, Hash, Mutex, RateError,
    RecvStream, SendStream, U256, VoucherOutcome, VoucherRejectReason, read_voucher, verify_rate,
    voucher_reject_reason, wire_voucher_to_signed,
};

impl ClientHandler {
    /// Read and apply one cumulative voucher covering `delta_bytes` of newly
    /// delivered bytes. A permanent voucher rejection writes a `StreamError` and
    /// finishes the stream cleanly (no reset). A transient store-write failure
    /// is likewise surfaced cleanly as `VoucherRejectReason::RetryLater` so the
    /// client resends the same voucher on a fresh stream (ADR 003 §332). Only an
    /// underpayment fails the stream: no wire reason exists for it, and the
    /// client is blocked awaiting `VoucherAck` so it cannot resend mid-stream.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn collect_voucher(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        hash: Hash,
        channel_id: ChannelId,
        channel: Option<&Arc<Mutex<ChannelDeliveryState>>>,
        client_node_id: B256,
        rate_per_mb: u64,
        quote_floor: u64,
        delta_bytes: u64,
    ) -> anyhow::Result<VoucherOutcome> {
        let wire = read_voucher(recv).await?;

        // Unknown channel (#327 boundary). Since #848, `serve_stream` refuses an
        // unknown channel pre-serve, so this arm is unreachable from the sole
        // caller (`deliver` always forwards `Some`); kept as a defensive backstop
        // — reject with the closest mid-stream reason.
        let Some(channel) = channel else {
            self.write_reject(send, VoucherRejectReason::WrongChannel)
                .await?;
            return Ok(VoucherOutcome::Rejected);
        };

        let mut guard = channel.lock().await;

        // Expiry gate (#327): once the channel has passed its on-chain
        // `expiresAt`, `withdraw`/`closeChannel` revert and the client can
        // `reclaimExpired` for a full refund — any further delivery would be
        // unpaid. Refuse rather than accept a voucher we could never redeem.
        // (The settlement sweep normally closes + retires channels well before
        // this; this is the defense-in-depth for a node that was down through
        // the close window.) Surface it in-band as `VoucherRejectReason::Expired`
        // and finish the stream cleanly (#751) so the client sees an actionable
        // reason instead of an opaque connection drop.
        if crate::payment_settlement::is_expired(
            crate::payment_settlement::unix_now(),
            guard.state.expires_at,
        ) {
            drop(guard);
            self.write_reject(send, VoucherRejectReason::Expired)
                .await?;
            return Ok(VoucherOutcome::Rejected);
        }

        // Cooperative-close gate (ADR 003 §Cooperative close): if the node signed
        // a waiver for this channel (possibly on a separate stream while this one
        // was mid-flight), it committed to settling at the watermark as of that
        // moment and must serve no further bytes. Reject in-band so the client
        // stops and settles, rather than delivering past the amount waived to.
        // Bounds the loss from signing mid-stream to one voucher interval, so no
        // "don't sign while delivering" interlock is needed.
        if guard.state.cooperative_close_signed() {
            drop(guard);
            self.write_reject(send, VoucherRejectReason::CooperativeCloseSigned)
                .await?;
            return Ok(VoucherOutcome::Rejected);
        }

        let new_bytes = guard
            .bytes_delivered_cumulative
            .saturating_add(U256::from(delta_bytes));
        let amount = U256::from_be_bytes(wire.amount);
        let amount_delta = amount.saturating_sub(guard.state.last_amount());

        // Rate enforcement (ADR 003 §Voucher withholding). There is no wire
        // reason code for underpayment, so an underpaying voucher fails the
        // stream rather than looping — looping would deadlock, since the client
        // is blocked awaiting `VoucherAck` and cannot send a corrected voucher.
        //
        // Match every `RateError` arm explicitly (#845): a non-`Underpayment`
        // result was previously treated as acceptable and silently passed to
        // `apply_voucher` (the only backstop). An exhaustive `match` makes a
        // future variant a build failure here instead. `ZeroBytes`/`Overflow`
        // cannot occur at this call site — `collect_voucher` runs only when
        // `unvouchered > 0`, so `delta_bytes > 0` — but are rejected defensively.
        match verify_rate(
            amount_delta,
            U256::from(delta_bytes),
            rate_per_mb,
            DEFAULT_TOLERANCE_BPS,
        ) {
            Ok(()) => {}
            Err(RateError::Underpayment { .. }) => {
                drop(guard);
                anyhow::bail!("voucher underpays for {delta_bytes} delivered bytes");
            }
            Err(e @ (RateError::ZeroBytes | RateError::Overflow)) => {
                drop(guard);
                anyhow::bail!("voucher fails rate check for {delta_bytes} delivered bytes: {e}");
            }
        }

        // Hard per-byte price floor (#846), checked on the CUMULATIVE watermark
        // the voucher carries (`amount` / `new_bytes`) — not the per-voucher
        // delta — so it mirrors the on-chain `PaymentChannel`
        // `_advanceClaimWatermark` `RateFloorViolation` guard exactly: the chain
        // floors `bytesDelivered <= mulDiv(amount, BYTES_PER_MB, deliveryFloor)`
        // on the cumulative claim, never on a single delta. The advertised check
        // above is per-delta at 1% tolerance; this is the cumulative floor at
        // ZERO tolerance, so a synced node never countersigns a voucher it then
        // cannot redeem — including the case where an earlier under-floor voucher
        // (accepted while the floor was 0) drags the watermark below the floor
        // even though the latest delta alone would clear it. A per-delta check
        // would both miss that (false accept) and reject a delta drawing down an
        // earlier overpayment surplus the chain would settle (false reject).
        //
        // Scope follows the delivery floor sourced from on-chain `getRateBounds()`
        // and tracked by the `RateBoundsUpdated` watcher (#1172, ADR 005), but
        // pinned to the value snapshotted when this stream's `StreamResponse` was
        // signed (`quote_floor`, threaded from `clamped_rate`) rather than re-read
        // live here (#1382). Quote and acceptance must agree by construction: a
        // governance floor raise landing between the signed quote and a voucher
        // for it would otherwise reject a voucher paying exactly the rate this
        // node itself quoted, failing an in-flight paid stream the buyer paid
        // correctly (`rate_bounds.rs` §quote-then-verify). The watcher's ~1s
        // cadence and multi-interval streams make that race reachable in practice.
        // `PaymentChannel` enforces `newFloor >= MIN_DEPOSIT_FLOOR (1)`, and the
        // runtime overwrites the config seed from chain before serving, so the
        // quoted floor is always live — the former `0`-floor "free-serving config"
        // (#864) is no longer reachable once the chain read lands. `new_bytes >=
        // delta_bytes > 0`, so `ZeroBytes` cannot occur; match every arm anyway
        // (#845) so a future `RateError` variant is a build failure rather than a
        // silent accept.
        match verify_rate(amount, new_bytes, quote_floor, 0) {
            Ok(()) => {}
            Err(RateError::Underpayment { .. }) => {
                self.metrics.voucher_rate_floor_rejected();
                drop(guard);
                anyhow::bail!(
                    "voucher below protocol rate floor for {delta_bytes} delivered bytes"
                );
            }
            Err(e @ (RateError::ZeroBytes | RateError::Overflow)) => {
                drop(guard);
                anyhow::bail!("voucher fails floor check for {delta_bytes} delivered bytes: {e}");
            }
        }

        let Ok(signed) = wire_voucher_to_signed(&wire, channel_id, guard.state.token, new_bytes)
        else {
            drop(guard);
            self.write_reject(send, VoucherRejectReason::BadSignature)
                .await?;
            return Ok(VoucherOutcome::Rejected);
        };

        // `apply_voucher` performs a synchronous fsynced redb write (store
        // trait §Durability), which must not block a runtime worker — run it on
        // the blocking pool against a clone. The per-channel guard is held
        // across the await so same-channel streams still serialize (ADR 003
        // §concurrent streams); the clone is committed back to in-memory state
        // only on `Ok`, preserving the strict-durability invariant (#527).
        let mut candidate = guard.state.clone();
        let signed_c = signed.clone();
        let domain = self.voucher_domain.clone();
        let store = Arc::clone(&self.channel_state_store);
        let (apply_res, candidate) = tokio::task::spawn_blocking(move || {
            let res = candidate.apply_voucher(&signed_c, &domain, &*store);
            (res, candidate)
        })
        .await
        .map_err(|e| anyhow::anyhow!("voucher apply task failed: {e}"))?;

        match apply_res {
            Ok(applied) => {
                guard.state = candidate;
                guard.bytes_delivered_cumulative = new_bytes;
                drop(guard);
                // Stamp the in-memory last-voucher clock for
                // `admin_v1_channels` (issue #749). Best-effort: an
                // unset clock (no admin surface) just skips. Done
                // after the guard drop — the activity map has its own
                // lock and doesn't need the per-channel guard.
                if let Some(activity) = self.voucher_activity.as_ref() {
                    activity.touch(channel_id);
                }
                // Audit receipt for this served-and-paid interval (issues #248,
                // #803): a non-blocking enqueue before `VoucherAck`; the write
                // happens off the hot path in the background receipt writer.
                self.record_receipt(hash, delta_bytes, client_node_id, wire.nonce);
                // Per-region bandwidth accounting (#750). Best-effort: an
                // unset accountant (tests / no admin surface) skips.
                // `delta_bytes` is exactly the bytes paid for this interval.
                if let Some(acc) = self.region_accountant.as_ref() {
                    acc.record_served(&client_node_id.0, delta_bytes).await;
                }
                // Credit the served bytes against the seed-leech caps (#856) for
                // BOTH cache-hit and window-paced pull-through serves: this
                // recoups the node-wide unrecouped-leech budget and raises the
                // paying peer's per-peer share-ratio allowance. Best-effort —
                // unset (tests / feature off) just skips.
                if let Some(gov) = self.leech_governor.as_ref() {
                    gov.record_served(&client_node_id.0, delta_bytes);
                }
                // Demand-quality feedback (#820): if this blob was obtained by
                // speculative prefetch, credit the served bytes to the policy's
                // `served / acquired` ratio so the auto-throttle reflects whether
                // prefetched content is actually being consumed.
                if let Some(pf) = self.prefetch_engine.as_ref() {
                    pf.note_served_if_prefetched(
                        *hash.as_bytes(),
                        delta_bytes,
                        crate::payment_settlement::unix_now(),
                    );
                }
                // Nonce-gap signal (#747): the voucher was accepted, but its
                // nonce skipped values past the prior `last_nonce + 1`. The
                // structured `tracing::warn!` already fired inside
                // `apply_voucher`; here we surface the rate to operators via
                // `decdn_voucher_nonce_gaps_total` for alerting.
                if applied.is_gapped() {
                    self.metrics.voucher_nonce_gap();
                }
                // Hint the on-chain settlement service that this channel's
                // accrued claim advanced (#327). Best-effort: an unset or
                // full hint channel just skips — the next voucher re-hints, the
                // redeemer self-tick sweeps, and shutdown closes any residual
                // claim. Only `Full` is counted (a saturated queue is a real
                // dropped hint; a sustained rate is the signal worth watching,
                // #751). `Closed` — the redeemer aborted during shutdown
                // `quiesce_redeemer` — is expected, not a fault, so it is
                // deliberately left uncounted; don't "fix" this to count both.
                if let Some(tx) = self.redeem_hint.as_ref()
                    && let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) =
                        tx.try_send(channel_id)
                {
                    self.metrics.redeem_hint_dropped();
                }
                self.write_message(send, &ClientMessage::VoucherAck).await?;
                Ok(VoucherOutcome::Accepted)
            }
            Err(e) => {
                drop(guard);
                if let Ok(reason) = voucher_reject_reason(&e) {
                    self.write_reject(send, reason).await?;
                    Ok(VoucherOutcome::Rejected)
                } else {
                    // Transient store failure (#527, `RetrySignal`): in-memory
                    // state did not advance. Surface it in-band as `RetryLater`
                    // and finish the stream cleanly (MUST NOT `VoucherAck`, ADR
                    // 003 §332) so the client resends the same voucher on a
                    // fresh stream rather than seeing an opaque connection drop.
                    tracing::warn!(error = %e, "channel store write failed; rejecting with RetryLater");
                    self.write_reject(send, VoucherRejectReason::RetryLater)
                        .await?;
                    Ok(VoucherOutcome::Rejected)
                }
            }
        }
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
    /// `collect_voucher` gates).
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

        let auth = {
            // Hold the per-channel lock across read-watermark → sign → persist so
            // a concurrent voucher cannot advance the watermark between the value
            // we sign and the flag we set. Signing is local and fast.
            let mut guard = channel.lock().await;
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
            CooperativeCloseAuth {
                channel_id: req.channel_id,
                amount: guard.state.last_amount().to_be_bytes(),
                nonce: guard.state.last_nonce().to_be_bytes(),
                bytes_delivered: guard.state.last_bytes_delivered().to_be_bytes(),
                signature: signed.signature.as_bytes().to_vec(),
            }
        };

        self.write_message(&mut send, &ClientMessage::CooperativeCloseAuth(auth))
            .await?;
        let _ = send.finish();
        Ok(())
    }
}
