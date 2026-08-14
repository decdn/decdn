//! Per-lane voucher collection / payment loop with group-commit batching.

use super::{
    Arc, B256, BatchOutcome, BatchStop, BufferedVoucherReader, ClientHandler,
    DEFAULT_TOLERANCE_BPS, Hash, LaneDeliveryState, LaneKey, LaneState, Mutex, RateError,
    RecvStream, RetrySignal, SendStream, SignedVoucher, U256, VOUCHER_READ_TIMEOUT,
    VoucherRejectReason, WatermarkBundle, verify_rate, voucher_reject_reason,
    wire_voucher_to_signed,
};

/// A voucher that passed the node-side verify half against the advancing
/// candidate and is awaiting the batch's single durable commit (#1483).
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
        if committed > 0 {
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
    /// distinct rate checks:
    /// - the per-delta **advertised-rate** check bails on a genuine underpayment
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

        // Validate + advance the candidate in memory only (no store). Cumulative
        // vouchers mean the returned `next` supersedes `state`, so the batch
        // records only the final candidate (one fsync). `stage_voucher` never
        // touches a store, so it can never surface `RetryLater` here.
        match state.stage_voucher(&signed, &self.voucher_domain) {
            Ok((next_state, _applied)) => Ok(VerifiedVoucher {
                next_state,
                new_bytes,
                staged: StagedVoucher {
                    delta_bytes,
                    amount: wire.amount,
                },
            }),
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
        Ok(CommitOutcome::Committed)
    }
}

/// Outcome of the durable half of a voucher batch ([`ClientHandler::commit_batch`]).
enum CommitOutcome {
    /// The batch fsynced and in-memory state advanced.
    Committed,
    /// The fsynced `store.record` failed; in-memory state is unchanged. The
    /// caller rejects the whole batch with `RetryLater`.
    StoreFailed,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
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
        assert!(
            matches!(outcome, CommitOutcome::Committed),
            "a clean record commits the batch"
        );

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
}
