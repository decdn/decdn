//! Per-lane voucher collection / payment loop with group-commit batching.

use super::{
    Arc, B256, BatchOutcome, BatchStop, BufferedVoucherReader, ClientHandler,
    DEFAULT_TOLERANCE_BPS, Hash, LaneDeliveryState, LaneKey, LaneState, Mutex, RateError,
    RecvStream, RetrySignal, SendStream, SignedVoucher, U256, VOUCHER_READ_TIMEOUT,
    VoucherRejectReason, WatermarkBundle, verify_rate, voucher_reject_reason,
    wire_voucher_to_signed,
};
use decdn_incentive::PoolError;

/// A voucher that passed the node-side verify half against the advancing
/// candidate and is awaiting the batch's single durable commit (#1483).
#[derive(Debug)]
struct StagedVoucher {
    /// Bytes this voucher pays for (its interval delta) — for `paid` accounting,
    /// the audit receipt, and per-region / seed-leech crediting.
    delta_bytes: u64,
    /// The voucher's cumulative amount (big-endian), for the audit receipt
    /// (#248/#803). Amount is the pool voucher's sole ordering key — there is no
    /// nonce.
    amount: [u8; 32],
}

/// A voucher that passed the node-side verify half, carrying the advanced
/// candidate state, its cumulative byte watermark, and the staged bookkeeping.
#[derive(Debug)]
struct VerifiedVoucher {
    next_state: LaneState,
    new_bytes: U256,
    staged: StagedVoucher,
}

/// Why the node-side verify half stopped the batch at a voucher (#1483). The
/// valid prefix is committed before this is acted on.
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
    /// Collect, durably commit, and continue serving a **batch** of cumulative
    /// vouchers with a single fsync (#1483, group commit).
    ///
    /// `deltas` are the completed interval sizes the serve loop has delivered and
    /// not yet recouped, drained front-to-back (a closing partial is just the
    /// last entry). The batch reads at most `deltas.len()` vouchers: the first
    /// blocking (so the loop makes progress and parks awaiting a voucher exactly
    /// as before), the rest gathered under [`ClientHandler::commit_interval`].
    /// Each voucher is verified against an advancing candidate; because vouchers
    /// are cumulative, the whole batch commits as ONE `store.record` of the final
    /// candidate — the highest voucher supersedes every earlier one, so a single
    /// fsync amortizes across the batch with no loss.
    ///
    /// **Durability ordering (ADR 003 §Off-chain voucher state persistence).**
    /// Acceptance is **implicit**: the fsynced commit runs first, then the node
    /// simply keeps delivering — no positive `VoucherAck` is written; only a
    /// rejection is ever signalled. A commit failure fails the WHOLE batch — every
    /// voucher gets `RetryLater`, and in-memory state is left unchanged — so the
    /// client resends the batch on a fresh stream (#527). Delaying the durable
    /// swap by one commit interval is free throughput-wise because #1477's credit
    /// window keeps it off the delivery critical path.
    ///
    /// On a mid-batch verify rejection, the valid prefix is committed first (one
    /// fsync), then the offending voucher's rejection is written. Returns the
    /// number of committed vouchers (so the loop advances `paid` and re-queues any
    /// deltas the client had not yet sent) and whether the stream must end.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn collect_voucher_batch(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        reader: &mut BufferedVoucherReader,
        hash: Hash,
        lane_key: LaneKey,
        lane: Option<&Arc<Mutex<LaneDeliveryState>>>,
        client_node_id: B256,
        rate_per_mb: u64,
        deltas: &[u64],
    ) -> anyhow::Result<BatchOutcome> {
        // Unknown lane (#327 boundary): `serve_stream` refuses an unknown lane
        // pre-serve, so this arm is unreachable from the sole callers (which
        // always forward `Some`); kept as a defensive backstop.
        let Some(lane) = lane else {
            self.write_reject(send, VoucherRejectReason::WrongPool, None)
                .await?;
            return Ok(BatchOutcome {
                committed: 0,
                credited_bytes: 0,
                stop: BatchStop::Rejected,
            });
        };

        // (1) GATHER — read up to `deltas.len()` wire vouchers WITHOUT holding
        // the per-lane lock (a network read must not block same-lane streams).
        // The first read blocks under `VOUCHER_READ_TIMEOUT`; the rest wait only
        // `commit_interval`, so a client that stops paying flushes the batch it
        // has instead of stalling. The reader is cancellation-safe.
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

        // (2) VERIFY under the per-lane lock (so the watermark checked is the
        // watermark committed) against an advancing candidate. The candidate is a
        // CLONE — `guard.state` is only swapped after the durable commit below,
        // preserving the #527 invariant.
        let guard = lane.lock().await;

        // Capability-expiry gate, checked once for the batch (ADR 003
        // §Capability delegation). A batch spans milliseconds, so a mid-batch
        // expiry race is bounded exactly as the pre-batch per-interval check
        // bounded it. `expiry == 0` means "not tracked" and never expires. An
        // expired grant surfaces as `CapExceeded` (its cap is exhausted for all
        // practical purposes — the on-chain `redeem` would revert identically).
        let expiry = guard.state.expiry;
        if expiry != 0 && crate::payment_settlement::unix_now() >= expiry {
            drop(guard);
            self.write_reject(send, VoucherRejectReason::CapExceeded, None)
                .await?;
            return Ok(BatchOutcome {
                committed: 0,
                credited_bytes: 0,
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

        // (3) COMMIT the verified prefix with ONE fsync, then keep serving.
        let committed = staged.len();
        let credited_bytes = if committed > 0 {
            match self
                .commit_batch(
                    guard,
                    lane_key,
                    hash,
                    client_node_id,
                    candidate,
                    candidate_bytes,
                    &staged,
                )
                .await?
            {
                // Durable commit failed → the WHOLE batch is RetryLater; in-memory
                // state did not advance. A pending verify rejection is overridden —
                // durability failure is the actionable signal, and the client must
                // resend the same vouchers.
                CommitOutcome::StoreFailed => {
                    self.write_reject(send, VoucherRejectReason::RetryLater, None)
                        .await?;
                    return Ok(BatchOutcome {
                        committed: 0,
                        credited_bytes: 0,
                        stop: BatchStop::Rejected,
                    });
                }
                CommitOutcome::Committed { credited_bytes } => credited_bytes,
            }
        } else {
            // No voucher verified (the first one was rejected/bailed): drop the
            // lock before emitting the rejection, matching the committed path.
            drop(guard);
            0
        };

        // (4) Emit any pending verify rejection/bail AFTER the prefix is durable.
        match pending_stop {
            None => Ok(BatchOutcome {
                committed,
                credited_bytes,
                stop: BatchStop::Continue,
            }),
            Some(VerifyStop::Reject(reason, bundle)) => {
                self.write_reject(send, reason, bundle).await?;
                Ok(BatchOutcome {
                    committed,
                    credited_bytes,
                    stop: BatchStop::Rejected,
                })
            }
            Some(VerifyStop::Bail(msg)) => Err(anyhow::anyhow!(msg)),
        }
    }

    /// The node-side verify half for ONE voucher, evaluated against the advancing
    /// candidate `state` with no durable side effect. `bytes_delivered` is
    /// self-describing: it comes straight off the wire, so verification does not
    /// depend on the order same-lane streams settle in. `stage_voucher` runs
    /// first (signature + amount/bytes monotonicity); the two rate checks then
    /// run on the advance path against the aggregate span
    /// (`applied.amount_delta()` / `applied.bytes_delta()`), which is
    /// order-independent because it is measured against the lane watermark:
    /// - the per-span **advertised-rate** check bails on a genuine underpayment
    ///   (no wire reason; delivery just stops);
    /// - the cumulative **live-floor** check rejects cleanly with
    ///   `RateFloorRaised` when a governance floor raise made the quote stale
    ///   (#1382), else bails.
    ///
    /// See [`Self::collect_voucher_batch`] for the durable-commit half.
    fn verify_voucher(
        &self,
        state: &LaneState,
        cumulative_bytes: U256,
        wire: &decdn_protocol::client::Voucher,
        rate_per_mb: u64,
        delta_bytes: u64,
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
                    staged: StagedVoucher {
                        delta_bytes,
                        amount: wire.amount,
                    },
                })
            }
            Err(PoolError::AmountRegression { last, .. }) => {
                // A voucher at-or-below the lane watermark: a concurrent same-lane
                // sibling already settled this cumulative. Benign — treat as
                // ALREADY-SATISFIED: do not advance the watermark, do not reject,
                // and stage this stream's own delta so its headroom + per-hash
                // receipt still progress (the watermark already covers this stream's
                // delivered). The one exception is a DIVERGENT voucher at the SAME
                // amount claiming MORE bytes — same money, more bytes — which is a
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
                    staged: StagedVoucher {
                        delta_bytes,
                        amount: wire.amount,
                    },
                })
            }
            Err(e) => {
                // Map to the wire reject reason. `Err(RetrySignal)` (a transient
                // store failure) cannot occur here — `stage_voucher` touches no
                // store — so a defensive fallback maps it to `RetryLater`.
                let reason = match voucher_reject_reason(&e) {
                    Ok(reason) => reason,
                    Err(RetrySignal) => VoucherRejectReason::RetryLater,
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
    /// The watermark reported is `state`'s last-accepted amount / bytes. In the
    /// batched flow `state` is the advancing candidate, so this is the
    /// committed-prefix watermark; the caller reads it while still holding the
    /// per-lane guard, before the commit swaps it into the live state.
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

    /// Durably commit the advanced `candidate` (ONE fsynced `store.record`, held
    /// on the blocking pool so it never blocks a runtime worker), swap it into
    /// the live lane state, then run each voucher's post-commit bookkeeping
    /// (#1483). Acceptance is **implicit** — no positive message is written; the
    /// caller simply keeps delivering.
    ///
    /// `guard` is the per-lane lock, still held from verification so no
    /// concurrent voucher advances the watermark between the value checked and
    /// the value committed; it is dropped once the commit lands. On a store
    /// failure `guard.state` is NOT advanced and [`CommitOutcome::StoreFailed`]
    /// is returned so the caller rejects the whole batch with `RetryLater` (#527,
    /// ADR 003).
    #[allow(clippy::too_many_arguments)]
    async fn commit_batch(
        &self,
        mut guard: tokio::sync::MutexGuard<'_, LaneDeliveryState>,
        lane_key: LaneKey,
        hash: Hash,
        client_node_id: B256,
        candidate: LaneState,
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
            // Surface `RetryLater` to the caller; MUST NOT continue delivering on
            // un-durable state.
            drop(guard);
            tracing::warn!(error = %e, "lane store batch commit failed; rejecting with RetryLater");
            return Ok(CommitOutcome::StoreFailed);
        }

        // Durable. Advance in-memory state to the final cumulative watermark and
        // release the lock before any further work.
        guard.state = candidate;
        guard.bytes_delivered_cumulative = candidate_bytes;

        // Rule #1 cap: credit paid headroom by at most the amount the watermark
        // advanced. `paid_credited` is monotone and bounded by the settled
        // watermark, so a benign already-satisfied voucher (candidate_bytes
        // unchanged) credits nothing and cannot reopen the credit window for
        // bytes no voucher settled. Computed under the guard so the read of
        // `paid_credited` and its store cannot interleave with another batch.
        let total_delta: u64 = staged.iter().map(|s| s.delta_bytes).sum();
        let new_credited = (guard.paid_credited + U256::from(total_delta)).min(candidate_bytes);
        let credited = new_credited.saturating_sub(guard.paid_credited);
        guard.paid_credited = new_credited;
        // `credited <= total_delta <= u64::MAX` by construction.
        let credited_bytes = u64::try_from(credited).unwrap_or(u64::MAX);
        drop(guard);

        // Post-commit, per-voucher bookkeeping. All side effects here are
        // best-effort and off the durability path. No positive ack is written —
        // acceptance is implicit and the caller keeps delivering.
        for s in staged {
            self.record_receipt(hash, s.delta_bytes, client_node_id, s.amount);
            if let Some(acc) = self.region_accountant.as_ref() {
                acc.record_served(&client_node_id.0, s.delta_bytes).await;
            }
        }

        // Lane-level, once per batch: stamp the admin last-voucher clock and hint
        // the settlement service that the accrued claim advanced (#749/#327).
        if let Some(activity) = self.voucher_activity.as_ref() {
            activity.touch(lane_key);
        }
        if let Some(tx) = self.redeem_hint.as_ref()
            && let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(lane_key)
        {
            self.metrics.redeem_hint_dropped();
        }
        Ok(CommitOutcome::Committed { credited_bytes })
    }
}

/// Outcome of the durable half of a voucher batch ([`ClientHandler::commit_batch`]).
enum CommitOutcome {
    /// The batch fsynced and in-memory state advanced. `credited_bytes` is the
    /// watermark-capped wire bytes the serve loop advances `paid` by (rule #1).
    Committed { credited_bytes: u64 },
    /// The fsynced `store.record` failed; in-memory state is unchanged. The
    /// caller rejects the whole batch with `RetryLater`.
    StoreFailed,
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use alloy::primitives::{Address, U256};
    use decdn_incentive::store::{PoolStateStore, StoreError};
    use decdn_incentive::{LaneKey, LaneState};
    use tokio::sync::Mutex;

    use super::super::{LaneDeliveryState, handler_over_store};
    use super::{CommitOutcome, StagedVoucher};
    use crate::metrics::Metrics;
    use decdn_cache::Hash;

    /// A [`PoolStateStore`] whose `record` always fails, to drive the #527
    /// durable-commit failure path. Reads succeed (empty) so hydration is clean.
    #[derive(Debug)]
    struct FailingRecordStore;

    impl PoolStateStore for FailingRecordStore {
        fn load_all(&self) -> Result<Vec<LaneState>, StoreError> {
            Ok(Vec::new())
        }
        fn record(&self, _state: &LaneState) -> Result<(), StoreError> {
            Err(StoreError::Backend("injected record failure".to_string()))
        }
        fn forget(&self, _key: LaneKey) -> Result<(), StoreError> {
            Ok(())
        }
        fn get(&self, _key: LaneKey) -> Result<Option<LaneState>, StoreError> {
            Ok(None)
        }
    }

    /// #527: a failed durable `record` in `commit_batch` leaves the in-memory lane
    /// state UNCHANGED and reports [`CommitOutcome::StoreFailed`] (which the batch
    /// path turns into `RetryLater` with `committed == 0`). Serving MUST NOT
    /// continue on un-durable state — a restart would otherwise reopen the
    /// voucher-replay window.
    #[tokio::test]
    async fn store_failure_rejects_batch_with_retry_later_no_state_advance() {
        let metrics = Arc::new(Metrics::new());
        let (handler, _dir) = handler_over_store(
            &metrics,
            Arc::new(FailingRecordStore) as Arc<dyn PoolStateStore>,
        )
        .await;

        let lane_key = LaneKey {
            pool_id: alloy::primitives::B256::repeat_byte(0x33),
            signer: Address::repeat_byte(0x44),
            provider: Address::repeat_byte(0x55),
        };
        // Seed the live lane at amount/bytes zero.
        let seed = LaneState::hydrate(
            lane_key.pool_id,
            lane_key.signer,
            lane_key.provider,
            U256::from(1_000_000u64), // cap
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

        // An advanced candidate the commit would swap in on success.
        let candidate = LaneState::hydrate(
            lane_key.pool_id,
            lane_key.signer,
            lane_key.provider,
            U256::from(1_000_000u64),
            0,
            U256::from(500u64),
            U256::from(500u64),
            Some([7u8; 65]),
        );
        let staged = vec![StagedVoucher {
            delta_bytes: 500,
            amount: U256::from(500u64).to_be_bytes(),
        }];

        let guard = lane.lock().await;
        let outcome = handler
            .commit_batch(
                guard,
                lane_key,
                Hash::from_bytes([9u8; 32]),
                alloy::primitives::B256::repeat_byte(0x66),
                candidate,
                U256::from(500u64),
                &staged,
            )
            .await
            .expect("commit_batch returns Ok(StoreFailed), never Err, on a store fault");
        assert!(
            matches!(outcome, CommitOutcome::StoreFailed),
            "a record failure must surface as StoreFailed"
        );

        // The in-memory lane state did NOT advance past the seed.
        let after = lane.lock().await;
        assert_eq!(
            after.state.last_amount(),
            U256::ZERO,
            "the committed watermark must stay at the seed after a store failure"
        );
        assert_eq!(
            after.bytes_delivered_cumulative,
            U256::ZERO,
            "the cumulative byte counter must not advance on an un-durable batch"
        );
    }

    /// #527 success twin of the store-failure test: a valid signed voucher
    /// verifies against a fresh lane (advancing the in-memory CANDIDATE to its
    /// cumulative amount/bytes without touching the store), and a successful
    /// `commit_batch` then advances the LIVE lane's persisted watermark.
    #[tokio::test]
    async fn staged_voucher_advances_candidate_lane() {
        use alloy::primitives::B256;
        use alloy::signers::local::PrivateKeySigner;

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

        // verify_voucher advances the CANDIDATE in memory only (no store write).
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

        // A successful commit advances the LIVE lane's durable watermark.
        let guard = lane.lock().await;
        let outcome = handler
            .commit_batch(
                guard,
                lane_key,
                Hash::from_bytes([1u8; 32]),
                B256::repeat_byte(0x66),
                verified.next_state,
                verified.new_bytes,
                &[verified.staged],
            )
            .await
            .expect("commit_batch returns Ok on a clean record");
        match outcome {
            CommitOutcome::Committed { credited_bytes } => assert_eq!(
                credited_bytes, delta,
                "an advance commit credits the full delivered delta"
            ),
            CommitOutcome::StoreFailed => panic!("a clean record must commit the batch"),
        }

        let after = lane.lock().await;
        assert_eq!(
            after.state.last_amount(),
            amount,
            "the committed watermark advanced to the voucher amount"
        );
        assert_eq!(
            after.bytes_delivered_cumulative, new_bytes,
            "the cumulative byte counter advanced to the voucher's bytes"
        );
    }

    /// Rule #1: `commit_batch` credits a stream's paid headroom by at most the
    /// amount the watermark advanced. A BENIGN voucher (candidate watermark
    /// unchanged) on a lane with no slack credits ZERO — this is what blocks the
    /// free-download leech (a client that delivers a window then pays only (0,0)
    /// vouchers must not have its window reopened).
    #[tokio::test]
    async fn benign_commit_with_no_slack_credits_zero() {
        let metrics = Arc::new(Metrics::new());
        let store = Arc::new(decdn_incentive::store::MemoryPoolStateStore::new())
            as Arc<dyn PoolStateStore>;
        let (handler, _dir) = handler_over_store(&metrics, store).await;

        let lane_key = LaneKey {
            pool_id: alloy::primitives::B256::repeat_byte(0x33),
            signer: Address::repeat_byte(0x44),
            provider: Address::repeat_byte(0x55),
        };
        // Fresh lane at (0,0), paid_credited 0.
        let seed = LaneState::hydrate(
            lane_key.pool_id,
            lane_key.signer,
            lane_key.provider,
            U256::from(1_000_000u64),
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

        // A benign commit: the candidate watermark is UNCHANGED (still 0 bytes),
        // but a real delivered delta of 500 wire bytes is staged.
        let candidate = LaneState::hydrate(
            lane_key.pool_id,
            lane_key.signer,
            lane_key.provider,
            U256::from(1_000_000u64),
            0,
            U256::ZERO,
            U256::ZERO,
            None,
        );
        let staged = vec![StagedVoucher {
            delta_bytes: 500,
            amount: U256::ZERO.to_be_bytes(),
        }];

        let guard = lane.lock().await;
        let outcome = handler
            .commit_batch(
                guard,
                lane_key,
                Hash::from_bytes([9u8; 32]),
                alloy::primitives::B256::repeat_byte(0x66),
                candidate,
                U256::ZERO,
                &staged,
            )
            .await
            .expect("commit_batch returns Ok");
        match outcome {
            CommitOutcome::Committed { credited_bytes } => assert_eq!(
                credited_bytes, 0,
                "a benign commit with no watermark slack must credit ZERO (leech block)"
            ),
            CommitOutcome::StoreFailed => panic!("clean store must commit"),
        }
        assert_eq!(
            lane.lock().await.paid_credited,
            U256::ZERO,
            "paid_credited must not advance past the watermark"
        );
    }

    /// Rule #1 twin: an ADVANCE commit (candidate watermark grew to cover the
    /// delta) credits the full delta — no behavior change for honest flows.
    #[tokio::test]
    async fn advance_commit_credits_full_delta() {
        let metrics = Arc::new(Metrics::new());
        let store = Arc::new(decdn_incentive::store::MemoryPoolStateStore::new())
            as Arc<dyn PoolStateStore>;
        let (handler, _dir) = handler_over_store(&metrics, store).await;

        let lane_key = LaneKey {
            pool_id: alloy::primitives::B256::repeat_byte(0x33),
            signer: Address::repeat_byte(0x44),
            provider: Address::repeat_byte(0x55),
        };
        let seed = LaneState::hydrate(
            lane_key.pool_id,
            lane_key.signer,
            lane_key.provider,
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

        // Candidate watermark advanced to 500 bytes, staged delta 500.
        let candidate = LaneState::hydrate(
            lane_key.pool_id,
            lane_key.signer,
            lane_key.provider,
            U256::MAX,
            0,
            U256::from(500u64),
            U256::from(500u64),
            Some([7u8; 65]),
        );
        let staged = vec![StagedVoucher {
            delta_bytes: 500,
            amount: U256::from(500u64).to_be_bytes(),
        }];

        let guard = lane.lock().await;
        let outcome = handler
            .commit_batch(
                guard,
                lane_key,
                Hash::from_bytes([1u8; 32]),
                alloy::primitives::B256::repeat_byte(0x66),
                candidate,
                U256::from(500u64),
                &staged,
            )
            .await
            .expect("commit_batch returns Ok");
        match outcome {
            CommitOutcome::Committed { credited_bytes } => assert_eq!(
                credited_bytes, 500,
                "an advance commit credits the full delivered delta"
            ),
            CommitOutcome::StoreFailed => panic!("clean store must commit"),
        }
        assert_eq!(lane.lock().await.paid_credited, U256::from(500u64));
    }

    /// A voucher at-or-below the lane watermark — a concurrent sibling raced ahead
    /// — is `AlreadySatisfied`: `verify_voucher` returns Ok WITHOUT advancing the
    /// candidate watermark, and stages this stream's delta so its own headroom and
    /// receipt still progress. It must NOT reject (that would kill an honest lagging
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

        // verify against the high watermark. delta_bytes is this stream's own
        // pending interval (1 MB), which the sibling's watermark already covers.
        let verified = handler
            .verify_voucher(&seed, U256::from(two_mb), &wire, rate_per_mb, one_mb)
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
        assert_eq!(
            verified.staged.delta_bytes, one_mb,
            "this stream's delta is still staged for its own receipt + headroom"
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
            .verify_voucher(&seed, U256::from(one_mb), &wire, rate_per_mb, one_mb)
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
