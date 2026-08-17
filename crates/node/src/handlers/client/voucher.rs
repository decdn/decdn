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
use decdn_incentive::PoolError;

/// A voucher that passed the node-side verify half, carrying the advanced
/// candidate state, its cumulative byte watermark, and the receipt amount.
#[derive(Debug)]
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

/// Rule #1 credit cap: advance a lane's `paid_credited` by at most the amount the
/// watermark advanced, and return the wire bytes to credit the serve loop's `paid`.
/// A benign already-satisfied voucher (watermark unchanged, `paid_credited` already at
/// it) credits zero, so it cannot reopen the credit window for unsettled bytes.
fn credit_advance(
    paid_credited: U256,
    delta_bytes: u64,
    watermark: U256,
) -> anyhow::Result<(U256, u64)> {
    let new_credited = (paid_credited + U256::from(delta_bytes)).min(watermark);
    let credited = new_credited.saturating_sub(paid_credited);
    let credited_bytes = u64::try_from(credited).map_err(|_| {
        anyhow::anyhow!("credited bytes exceeded u64 — paid-frontier invariant broke")
    })?;
    Ok((new_credited, credited_bytes))
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
        // means "not tracked" and never expires. An expired grant surfaces as
        // `CapabilityExpired` — distinct from a cap-exhausted `SpendingCapExhausted`,
        // since the fix is a fresh capability, not a cap raise.
        let expiry = guard.state.expiry;
        if expiry != 0 && crate::payment_settlement::unix_now() >= expiry {
            drop(guard);
            self.write_reject(send, VoucherRejectReason::CapabilityExpired, None)
                .await?;
            return Ok(VoucherStop::Rejected);
        }

        let verified = match self.verify_voucher(
            &guard.state,
            guard.bytes_delivered_cumulative,
            &wire,
            rate_per_mb,
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
        // Rule #1 cap: credit paid headroom by at most the amount the watermark
        // advanced. `paid_credited` is monotone and bounded by the settled
        // watermark, so a benign already-satisfied voucher (watermark unchanged)
        // credits nothing and cannot reopen the credit window for bytes no voucher
        // settled. Computed under the guard so the read of `paid_credited` and its
        // store cannot interleave with another voucher.
        let (new_credited, credited_bytes) =
            credit_advance(guard.paid_credited, delta_bytes, verified.new_bytes)?;
        guard.paid_credited = new_credited;
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
        // Hint the settlement service that this lane's accrued claim advanced
        // (#749/#327). Best-effort: an absent sender or a full channel just skips.
        if let Some(tx) = self.redeem_hint.as_ref()
            && let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(lane_key)
        {
            self.metrics.redeem_hint_dropped();
        }
        Ok(VoucherStop::Continue { credited_bytes })
    }

    /// The node-side verify half for ONE voucher, evaluated against the current
    /// lane watermark (`state` / `cumulative_bytes`) with no durable side effect.
    /// `bytes_delivered` is self-describing: it comes straight off the wire, so
    /// verification does not depend on the order same-lane streams settle in.
    /// `stage_voucher` runs first (signature + amount/bytes monotonicity); the two
    /// rate checks then run on the advance path against the aggregate span
    /// (`applied.amount_delta()` / `applied.bytes_delta()`), which is
    /// order-independent because it is measured against the lane watermark:
    /// - the per-span **advertised-rate** check bails on a genuine underpayment
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
    ) -> Result<VerifiedVoucher, VerifyStop> {
        // Self-describing: the voucher's cumulative bytes come from the WIRE (ADR
        // 005 §Voucher wire format), so verification does not depend on the order
        // same-lane streams settle in.
        let new_bytes = U256::from_be_bytes(wire.bytes_delivered);

        // Reconstruct the signed voucher from wire + lane context. `pool_id`,
        // `signer`, and `provider` are fixed for the lane; `amount`/`bytes_delivered`
        // ride the wire.
        let Ok(signed) = wire_voucher_to_signed(wire, state.pool_id, state.signer, state.provider)
        else {
            return Err(VerifyStop::Reject(VoucherRejectReason::BadSignature, None));
        };

        // `stage_voucher` verifies the signature, then the amount/bytes monotonicity
        // guards, and returns the advanced candidate. It touches no store, so it can
        // never surface `RetrySignal` here.
        match state.stage_voucher(&signed, &self.voucher_domain) {
            Ok((next_state, applied)) => {
                // ADVANCE: this voucher raises the lane watermark. Rate-check the
                // aggregate span it covers (`applied.*_delta()` is measured against
                // the lane watermark, so it is order-independent).
                let amount = U256::from_be_bytes(wire.amount);

                // Advertised-rate check (ADR 003 §Voucher withholding). Match every
                // `RateError` arm (#845) so a future variant is a build failure here.
                match verify_rate(
                    applied.amount_delta(),
                    applied.bytes_delta(),
                    rate_per_mb,
                    DEFAULT_TOLERANCE_BPS,
                ) {
                    Ok(()) => {}
                    Err(RateError::Underpayment { .. }) => {
                        return Err(VerifyStop::Bail(
                            "voucher underpays for its delivered-byte span".to_string(),
                        ));
                    }
                    Err(e @ (RateError::ZeroBytes | RateError::Overflow)) => {
                        return Err(VerifyStop::Bail(format!("voucher fails rate check: {e}")));
                    }
                }

                // Hard per-byte price floor (#846) on the cumulative watermark the
                // voucher carries (mirrors on-chain `redeem` at zero tolerance).
                let live_floor = self.rate_bounds.floor();
                match verify_rate(amount, new_bytes, live_floor, 0) {
                    Ok(()) => {}
                    Err(RateError::Underpayment { .. }) => {
                        self.metrics.voucher_rate_floor_rejected();
                        if live_floor > rate_per_mb {
                            return Err(VerifyStop::Reject(
                                VoucherRejectReason::RateFloorRaised,
                                None,
                            ));
                        }
                        return Err(VerifyStop::Bail(
                            "voucher below the cumulative rate floor".to_string(),
                        ));
                    }
                    Err(e @ (RateError::ZeroBytes | RateError::Overflow)) => {
                        return Err(VerifyStop::Bail(format!("voucher fails floor check: {e}")));
                    }
                }

                Ok(VerifiedVoucher {
                    next_state,
                    new_bytes,
                    amount: wire.amount,
                })
            }
            Err(PoolError::AmountRegression { last, .. }) => {
                // A voucher at-or-below the lane watermark: a concurrent same-lane
                // sibling already settled this cumulative. Benign — treat as
                // ALREADY-SATISFIED: do not advance the watermark, do not reject.
                // The one exception is a DIVERGENT voucher at the SAME amount
                // claiming MORE bytes — same money, more bytes — which is a
                // single-signer fault (#1699 rule 4).
                let amount = U256::from_be_bytes(wire.amount);
                if amount == last && new_bytes > state.last_bytes_delivered() {
                    return Err(VerifyStop::Reject(
                        VoucherRejectReason::BytesRegression,
                        None,
                    ));
                }
                Ok(VerifiedVoucher {
                    next_state: state.clone(),
                    new_bytes: cumulative_bytes,
                    amount: wire.amount,
                })
            }
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
    ///   (`AmountRegression` / `BytesRegression` / `SpendingCapExhausted`);
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
#[allow(clippy::expect_used, clippy::panic)]
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
            paid_credited: U256::ZERO,
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
            bytes_delivered: new_bytes.to_be_bytes(),
        };

        let snapshot = lane.lock().await.state.clone();
        let verified = handler
            .verify_voucher(&snapshot, U256::ZERO, &wire, rate_per_mb)
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

    /// Rule #1 credit cap: a benign already-satisfied voucher (watermark
    /// unchanged) on a lane with no slack credits ZERO — this is what blocks the
    /// free-download leech (a client that delivers a window then pays only (0,0)
    /// vouchers must not have its window reopened).
    #[test]
    fn credit_advance_benign_credits_zero() {
        let (new_credited, credited) = super::credit_advance(U256::ZERO, 500, U256::ZERO)
            .expect("credit_advance is infallible for in-range values");
        assert_eq!(new_credited, U256::ZERO);
        assert_eq!(
            credited, 0,
            "a benign voucher with no watermark slack must credit ZERO (leech block)"
        );
    }

    /// Rule #1 credit cap: an advance whose watermark grew to cover the delta
    /// credits the full delta — no behavior change for honest flows.
    #[test]
    fn credit_advance_full_delta() {
        let (new_credited, credited) = super::credit_advance(U256::ZERO, 500, U256::from(500u64))
            .expect("credit_advance is infallible for in-range values");
        assert_eq!(new_credited, U256::from(500u64));
        assert_eq!(credited, 500, "an advance credits the full delivered delta");
    }

    /// Rule #1 credit cap: a voucher whose watermark is already fully credited
    /// credits nothing further, and `paid_credited` does not advance past it.
    #[test]
    fn credit_advance_already_credited() {
        let (new_credited, credited) =
            super::credit_advance(U256::from(500u64), 500, U256::from(500u64))
                .expect("credit_advance is infallible for in-range values");
        assert_eq!(new_credited, U256::from(500u64));
        assert_eq!(
            credited, 0,
            "an already fully credited watermark credits nothing further"
        );
    }

    /// A voucher at-or-below the lane watermark — a concurrent sibling raced ahead
    /// — is `AlreadySatisfied`: `verify_voucher` returns Ok WITHOUT advancing the
    /// candidate watermark. It must NOT reject (that would kill an honest lagging
    /// stream, #1699).
    #[tokio::test]
    async fn stale_voucher_is_benign_and_does_not_regress_watermark() {
        use alloy::primitives::B256;
        use alloy::signers::local::PrivateKeySigner;

        let metrics = Arc::new(Metrics::new());
        let store = Arc::new(decdn_incentive::store::MemoryPoolStateStore::new())
            as Arc<dyn PoolStateStore>;
        let (handler, _dir) = handler_over_store(&metrics, store).await;

        let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
        let signer_key = PrivateKeySigner::random();
        let signer = signer_key.address();
        let pool_id = B256::repeat_byte(0x21);
        let provider = Address::repeat_byte(0x55);

        let rate_per_mb = 1_000_000u64;
        // Seed the lane already advanced to 2 MB (a sibling settled it).
        let two_mb = decdn_incentive::rate::BYTES_PER_MB * 2;
        let high_amount = decdn_incentive::min_payment(two_mb, rate_per_mb);
        let seed = LaneState::hydrate(
            pool_id,
            signer,
            provider,
            U256::MAX,
            0,
            high_amount,
            U256::from(two_mb),
            Some([9u8; 65]),
        );

        // A LOWER cumulative voucher: 1 MB. Signed correctly by the lane signer.
        let one_mb = decdn_incentive::rate::BYTES_PER_MB;
        let low_amount = decdn_incentive::min_payment(one_mb, rate_per_mb);
        let signed_low = decdn_incentive::Voucher {
            pool_id,
            signer,
            provider,
            amount: low_amount,
            bytes_delivered: U256::from(one_mb),
        }
        .sign(&signer_key, &domain)
        .expect("sign low voucher");
        let wire = decdn_protocol::client::Voucher {
            signature: signed_low.signature.as_bytes().to_vec(),
            amount: low_amount.to_be_bytes(),
            bytes_delivered: U256::from(one_mb).to_be_bytes(),
        };

        // verify against the high watermark; the sibling's watermark already
        // covers this stream's delivered.
        let verified = handler
            .verify_voucher(&seed, U256::from(two_mb), &wire, rate_per_mb)
            .expect("a superseded but well-signed voucher is benign, not a reject");
        assert_eq!(
            verified.new_bytes,
            U256::from(two_mb),
            "the candidate watermark must NOT regress to the stale voucher"
        );
        assert_eq!(
            verified.next_state.last_amount(),
            high_amount,
            "the candidate amount must stay at the sibling-settled watermark"
        );
    }

    /// The single-signer guard (#1699 rule 4): a voucher at the SAME amount but a
    /// HIGHER `bytes_delivered` — same money, more bytes claimed — is a divergent
    /// fault, not a benign supersede.
    #[tokio::test]
    async fn divergent_voucher_at_equal_amount_is_rejected() {
        use alloy::primitives::B256;
        use alloy::signers::local::PrivateKeySigner;

        let metrics = Arc::new(Metrics::new());
        let store = Arc::new(decdn_incentive::store::MemoryPoolStateStore::new())
            as Arc<dyn PoolStateStore>;
        let (handler, _dir) = handler_over_store(&metrics, store).await;

        let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
        let signer_key = PrivateKeySigner::random();
        let signer = signer_key.address();
        let pool_id = B256::repeat_byte(0x21);
        let provider = Address::repeat_byte(0x55);

        let rate_per_mb = 1_000_000u64;
        let one_mb = decdn_incentive::rate::BYTES_PER_MB;
        let amount = decdn_incentive::min_payment(one_mb, rate_per_mb);
        // Seed at (amount, 1 MB).
        let seed = LaneState::hydrate(
            pool_id,
            signer,
            provider,
            U256::MAX,
            0,
            amount,
            U256::from(one_mb),
            Some([9u8; 65]),
        );
        // Same amount, but claims 2 MB of bytes.
        let two_mb = one_mb * 2;
        let divergent_voucher = decdn_incentive::Voucher {
            pool_id,
            signer,
            provider,
            amount,
            bytes_delivered: U256::from(two_mb),
        }
        .sign(&signer_key, &domain)
        .expect("sign divergent voucher");
        let wire = decdn_protocol::client::Voucher {
            signature: divergent_voucher.signature.as_bytes().to_vec(),
            amount: amount.to_be_bytes(),
            bytes_delivered: U256::from(two_mb).to_be_bytes(),
        };

        let err = handler
            .verify_voucher(&seed, U256::from(one_mb), &wire, rate_per_mb)
            .expect_err("a divergent equal-amount voucher must be rejected");
        match err {
            super::VerifyStop::Reject(reason, _) => assert_eq!(
                reason,
                decdn_protocol::client::VoucherRejectReason::BytesRegression,
                "divergent equal-amount voucher rejects as BytesRegression"
            ),
            super::VerifyStop::Bail(msg) => panic!("expected a Reject, got Bail({msg})"),
        }
    }
}
