//! Per-lane voucher verification and per-voucher in-memory watermark advance.
//!
//! The lane store buffers `record()` in memory and the durability half runs on
//! a background flush timer (ADR 003 §Off-chain voucher state persistence), so
//! the serve loop advances one voucher at a time — nothing to amortize into a
//! batch. [`ClientHandler::commit_one_voucher`] reads one voucher, verifies it
//! under the per-lane lock, and records it; a pre-redeem flush covers the
//! settlement path.

use super::{
    Arc, B256, BufferedVoucherReader, ClientHandler, DEFAULT_TOLERANCE_BPS, Hash,
    LaneDeliveryState, LaneKey, LaneState, Mutex, RateError, RecvStream, RetrySignal, SendStream,
    SignedVoucher, U256, VOUCHER_READ_TIMEOUT, VoucherRejectReason, VoucherStop, WatermarkBundle,
    verify_rate, voucher_reject_reason, wire_voucher_to_signed,
};

/// A voucher that passed the node-side verify half, carrying the advanced
/// candidate state, its cumulative byte watermark, and the receipt amount.
struct VerifiedVoucher {
    next_state: LaneState,
    new_bytes: U256,
    /// The voucher's cumulative amount (big-endian) for the audit receipt.
    amount: [u8; 32],
}

/// Why the node-side verify half rejected or bailed on a voucher. The optional
/// [`WatermarkBundle`] rides the wallet-less-resume path (#1481 §5).
#[derive(Debug)]
enum VerifyStop {
    /// Reject cleanly with this wire reason, then finish the stream (#751).
    /// The optional [`WatermarkBundle`] rides the wallet-less-resume path
    /// (#1481 §5): it is `Some` only for a watermark-gated regression/exhaustion
    /// reason whose rejected voucher recovers to the lane's pinned `signer`, and
    /// carries the node's true watermark so an authorized funder can re-seed and
    /// resume. Every other reason carries `None`.
    Reject(VoucherRejectReason, Option<WatermarkBundle>),
    /// Fail the stream — there is no wire reason for this fault (a buyer
    /// underpayment), and delivery simply stops (ADR 003 §Voucher withholding).
    Bail(String),
}

impl ClientHandler {
    /// Read ONE cumulative voucher, verify it under the per-lane lock, and — on
    /// success — advance the in-memory lane watermark and record it to the pool
    /// store (buffered; the background flush makes it durable, ADR 003
    /// §Off-chain voucher state persistence). Acceptance is implicit: no positive
    /// message is written; the caller keeps delivering.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn commit_one_voucher(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        reader: &mut BufferedVoucherReader,
        hash: Hash,
        lane_key: LaneKey,
        lane: Option<&Arc<Mutex<LaneDeliveryState>>>,
        client_node_id: B256,
        rate_per_mb: u64,
        delta_bytes: u64,
    ) -> anyhow::Result<VoucherStop> {
        // Unknown lane: `serve_stream` refuses one pre-serve, so this is a
        // defensive backstop matching the sole callers (which forward `Some`).
        let Some(lane) = lane else {
            self.write_reject(send, VoucherRejectReason::WrongPool, None)
                .await?;
            return Ok(VoucherStop::Rejected);
        };

        // (1) READ one wire voucher (blocking under VOUCHER_READ_TIMEOUT) WITHOUT
        // holding the per-lane lock — a network read must not block same-lane
        // streams. The reader is cancellation-safe.
        let wire = tokio::time::timeout(VOUCHER_READ_TIMEOUT, reader.read(recv))
            .await
            .map_err(|_| {
                anyhow::anyhow!("voucher read timed out after {VOUCHER_READ_TIMEOUT:?}")
            })??;

        // (2) VERIFY + ADVANCE under the per-lane lock (the watermark checked is
        // the watermark stored).
        let mut guard = lane.lock().await;

        // Capability-expiry gate (ADR 003 §Capability delegation). `expiry == 0`
        // means "not tracked" and never expires.
        let expiry = guard.state.expiry;
        if expiry != 0 && crate::payment_settlement::unix_now() >= expiry {
            drop(guard);
            self.write_reject(send, VoucherRejectReason::CapExceeded, None)
                .await?;
            return Ok(VoucherStop::Rejected);
        }

        let verified = match self.verify_voucher(
            &guard.state,
            guard.bytes_delivered_cumulative,
            &wire,
            rate_per_mb,
            delta_bytes,
        ) {
            Ok(v) => v,
            Err(VerifyStop::Reject(reason, bundle)) => {
                drop(guard);
                self.write_reject(send, reason, bundle).await?;
                return Ok(VoucherStop::Rejected);
            }
            Err(VerifyStop::Bail(msg)) => {
                drop(guard);
                return Err(anyhow::anyhow!(msg));
            }
        };

        // Advance in-memory, then record to the buffered store (a cheap map write;
        // durability is the background flush's job). A poisoned store mutex is the
        // only failure path and is treated as a serve fault.
        guard.state = verified.next_state.clone();
        guard.bytes_delivered_cumulative = verified.new_bytes;
        if let Err(e) = self.channel_state_store.record(&verified.next_state) {
            drop(guard);
            return Err(anyhow::anyhow!("lane store record failed: {e}"));
        }
        drop(guard);

        // (3) Post-acceptance bookkeeping (best-effort, off the durability path).
        self.record_receipt(hash, delta_bytes, client_node_id, verified.amount);
        if let Some(acc) = self.region_accountant.as_ref() {
            acc.record_served(&client_node_id.0, delta_bytes).await;
        }
        if let Some(activity) = self.voucher_activity.as_ref() {
            activity.touch(lane_key);
        }
        Ok(VoucherStop::Continue)
    }

    /// The node-side verify half for ONE voucher, evaluated against the current
    /// lane watermark (`state` / `cumulative_bytes`) with no durable side effect.
    /// Two distinct rate checks:
    /// - the per-delta **advertised-rate** check bails on a genuine underpayment
    ///   (no wire reason; delivery just stops);
    /// - the cumulative **live-floor** check rejects cleanly with
    ///   `RateFloorRaised` when a governance floor raise made the quote stale
    ///   (#1382), else bails.
    fn verify_voucher(
        &self,
        state: &LaneState,
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
        // the voucher carries (mirrors the on-chain `redeem` `RateFloorViolation`
        // guard at ZERO tolerance). Snapshot the live floor once so the check and
        // the rejection classification cannot disagree.
        let live_floor = self.rate_bounds.floor();
        match verify_rate(amount, new_bytes, live_floor, 0) {
            Ok(()) => {}
            Err(RateError::Underpayment { .. }) => {
                self.metrics.voucher_rate_floor_rejected();
                if live_floor > rate_per_mb {
                    // A governance floor raise landed between the signed quote and
                    // this voucher (#1382): the buyer is honest, its quote is
                    // stale. Surface the typed re-quote signal in-band.
                    return Err(VerifyStop::Reject(
                        VoucherRejectReason::RateFloorRaised,
                        None,
                    ));
                }
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

        // Reconstruct the signed voucher from wire + lane context (`pool_id`,
        // `signer`, `provider`, and the cumulative `bytes_delivered` are not on
        // the wire — ADR 005 §Voucher wire format).
        let Ok(signed) =
            wire_voucher_to_signed(wire, state.pool_id, state.signer, state.provider, new_bytes)
        else {
            return Err(VerifyStop::Reject(VoucherRejectReason::BadSignature, None));
        };

        // Validate + advance the lane state in memory. `stage_voucher` never
        // touches a store, so `Err(RetrySignal)` below can never surface here.
        match state.stage_voucher(&signed, &self.voucher_domain) {
            Ok((next_state, _applied)) => Ok(VerifiedVoucher {
                next_state,
                new_bytes,
                amount: wire.amount,
            }),
            Err(e) => {
                // Map to the wire reject reason. `Err(RetrySignal)` (a transient
                // store failure) cannot occur here — `stage_voucher` touches no
                // store — so this is defensive: abort the stream rather than
                // inventing a wire reason for a fault this path cannot produce.
                let reason = match voucher_reject_reason(&e) {
                    Ok(reason) => reason,
                    Err(RetrySignal) => {
                        return Err(VerifyStop::Bail(
                            "stage_voucher touches no store; unexpected RetrySignal".to_string(),
                        ));
                    }
                };
                // Wallet-less resume (#1481 §5): for a gated regression/exhaustion
                // reason whose rejected voucher recovers to the pinned signer,
                // attach the node's true watermark so an authorized funder can
                // re-seed and resume.
                let bundle = self.watermark_bundle_for_reject(reason, &signed, state);
                Err(VerifyStop::Reject(reason, bundle))
            }
        }
    }

    /// Build the wallet-less-resume [`WatermarkBundle`] for a rejected voucher
    /// (#1481 §5), or `None` when the voucher is not eligible. Returns `Some`
    /// only when ALL hold:
    /// - `reason` is one of the watermark-gated regression/exhaustion reasons
    ///   (`AmountRegression` / `BytesRegression` / `CapExceeded`);
    /// - the `rejected` voucher's signature recovers to `state.signer`, the
    ///   lane's pinned capability signer — otherwise anyone who guessed the
    ///   chain-derivable `pool_id` could pull a lane's private watermark;
    /// - the lane has a prior accepted voucher (`last_signature` is `Some`) to
    ///   echo back.
    ///
    /// The watermark reported is `state`'s last-accepted amount / bytes, read
    /// while still holding the per-lane guard, before the caller swaps in the
    /// newly verified state.
    fn watermark_bundle_for_reject(
        &self,
        reason: VoucherRejectReason,
        rejected: &SignedVoucher,
        state: &LaneState,
    ) -> Option<WatermarkBundle> {
        if !reason.is_watermark_gated() {
            return None;
        }
        let recovered = rejected.recover_signer(&self.voucher_domain).ok()?;
        if recovered != state.signer {
            return None;
        }
        let last_signature = state.last_signature()?;
        Some(WatermarkBundle {
            amount: state.last_amount().to_be_bytes(),
            bytes_delivered: state.last_bytes_delivered().to_be_bytes(),
            last_signature: last_signature.to_vec(),
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use alloy::primitives::{Address, B256, U256};
    use alloy::signers::local::PrivateKeySigner;
    use decdn_incentive::store::PoolStateStore;
    use decdn_incentive::{LaneKey, LaneState};
    use tokio::sync::Mutex;

    use super::super::{LaneDeliveryState, handler_over_store};
    use crate::metrics::Metrics;

    /// A well-formed voucher verifies against a fresh lane and advances the
    /// candidate state to the voucher's cumulative amount/bytes — the pure,
    /// no-I/O half of [`super::ClientHandler::commit_one_voucher`]. The
    /// read→verify→record path over real streams (durability included) is
    /// covered end to end by the `client_loopback` integration family.
    #[tokio::test]
    async fn verify_voucher_advances_candidate() {
        let metrics = Arc::new(Metrics::new());
        let store = Arc::new(decdn_incentive::store::MemoryPoolStateStore::new())
            as Arc<dyn PoolStateStore>;
        let (handler, _dir) = handler_over_store(&metrics, store).await;

        // `handler_over_store` builds all three EIP-712 domains from this literal,
        // so the voucher signer must sign over the same one.
        let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
        let signer_key = PrivateKeySigner::random();
        let signer = signer_key.address();
        let pool_id = B256::repeat_byte(0x21);
        let provider = Address::repeat_byte(0x55);

        // Seed a fresh lane at a zero watermark with an ample cap.
        let lane_key = LaneKey {
            pool_id,
            signer,
            provider,
        };
        let seed = LaneState::hydrate(
            pool_id,
            signer,
            provider,
            U256::MAX,
            0,
            U256::ZERO,
            U256::ZERO,
            None,
        );
        let lane = Arc::new(Mutex::new(LaneDeliveryState {
            state: seed,
            bytes_delivered_cumulative: U256::ZERO,
            active_streams: Arc::new(AtomicU32::new(0)),
        }));
        handler
            .lanes
            .lock()
            .await
            .insert(lane_key, Arc::clone(&lane));

        // A cumulative voucher paying exactly one MB from zero, signed by the
        // lane's pinned signer over the lane context.
        let rate_per_mb = 1_000_000u64;
        let delta = decdn_incentive::rate::BYTES_PER_MB;
        let new_bytes = U256::from(delta);
        let amount = decdn_incentive::min_payment(delta, rate_per_mb);
        let signed_voucher = decdn_incentive::Voucher {
            pool_id,
            signer,
            provider,
            amount,
            bytes_delivered: new_bytes,
        }
        .sign(&signer_key, &domain)
        .expect("sign voucher");
        let wire = decdn_protocol::client::Voucher {
            signature: signed_voucher.signature.as_bytes().to_vec(),
            amount: amount.to_be_bytes(),
        };

        let snapshot = lane.lock().await.state.clone();
        let verified = handler
            .verify_voucher(&snapshot, U256::ZERO, &wire, rate_per_mb, delta)
            .expect("a well-formed voucher verifies against a fresh lane");
        assert_eq!(
            verified.new_bytes, new_bytes,
            "the candidate advances to the voucher's cumulative bytes"
        );
        assert_eq!(
            verified.next_state.last_amount(),
            amount,
            "the candidate advances to the voucher's cumulative amount"
        );
        assert_eq!(
            verified.amount,
            amount.to_be_bytes(),
            "the receipt amount matches the voucher's cumulative amount"
        );
    }
}
