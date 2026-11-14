//! Per-lane voucher verification and per-voucher in-memory watermark advance.
//!
//! The lane store buffers `record()` in memory and the durability half runs on
//! a background flush timer (ADR 003 §Off-chain voucher state persistence), so
//! the serve loop advances one proof at a time — nothing to amortize into a
//! batch. [`ClientHandler::commit_one_proof`] reads one proof, verifies it
//! under the per-lane lock, and records it; a pre-redeem flush covers the
//! settlement path.
//!
//! A proof is one of two things, and they cost very differently (ADR 003 §Two
//! payment resolutions). A **voucher** is signed: it settles any residual
//! exactly, and costs an `ecrecover` on this path. A **`ChunkPreimage`** is not:
//! it advances the lane's claim by one whole chunk for the price of a single
//! keccak, with no signature to recover and nothing to acknowledge. That
//! asymmetry is the point of the hash chain — signatures become O(1) per
//! transfer plus one per rollover, rather than one per metering interval.

use super::proof_wait::ChunkProofs;
use super::{
    Arc, B256, BufferedProofReader, ClientHandler, Connection, DEFAULT_TOLERANCE_BPS, Hash,
    LaneDeliveryState, LaneKey, LaneState, Mutex, Ordering, Proof, RateError, RecvStream,
    RetrySignal, SendStream, SignedVoucher, U256, VoucherRejectReason, VoucherStop,
    WatermarkBundle, verify_rate, voucher_reject_reason, wire_voucher_to_signed,
};
use decdn_incentive::{PoolError, VoucherError};

/// One stream's hash-chain anchor: the `chain_root` it has been told about
/// (ADR 003 §Concurrent Streams, Rule 1).
///
/// A released preimage is a bare 33 bytes — it names no chain. Its worth comes
/// entirely from the voucher whose `chain_root` it satisfies, so the node needs
/// something per stream to place it against. This is that something, and it is
/// deliberately **per stream** rather than lane-wide: after a rollover the payer
/// sends the new root voucher on each stream immediately before that stream's
/// next reveal, so a fast stream that has already adopted the new root cannot
/// invalidate a slower sibling still finishing the old one. QUIC orders within a stream, so each stream reads its
/// own old-chain reveals against its own old root.
///
/// A lane-wide reading would also not be *decidable*: "has this lane been told
/// about a chain" has no useful answer when one stream has and another has not.
/// Per stream, it does.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct StreamAnchor {
    /// The root this stream has carried a voucher for, or `None` if it has
    /// carried none — or if the last one it carried was sealed.
    root: Option<B256>,
    /// The anchoring voucher signed an amount below the lane's signed
    /// watermark: the payer's ledger trails the node's, as a restarted payer's
    /// does. An honest stream anchors at the watermark, never below it.
    trailing: bool,
}

impl StreamAnchor {
    /// Re-anchor to whatever an accepted voucher committed. A sealed voucher
    /// (`chain_root == 0`) clears the anchor: it meters nothing, so a reveal
    /// arriving afterwards on this stream would have no chain to belong to.
    fn adopt(&mut self, chain_root: B256, trailing: bool) {
        self.root = (!chain_root.is_zero()).then_some(chain_root);
        self.trailing = trailing && self.root.is_some();
    }
}

/// A voucher that passed the node-side verify half, carrying the advanced
/// candidate state, its cumulative byte watermark, and the receipt amount.
#[derive(Debug)]
struct VerifiedVoucher {
    next_state: LaneState,
    new_bytes: U256,
    /// The voucher's cumulative amount, for the audit receipt (#248/#803).
    /// Amount is the pool voucher's sole ordering key — there is no nonce.
    amount: u64,
}

/// Why the node-side verify half rejected or bailed on a voucher. The optional
/// [`WatermarkBundle`] rides the wallet-less-resume path (#1481 §5).
#[derive(Debug)]
enum VerifyStop {
    /// Reject cleanly with this wire reason, then finish the stream (#751).
    /// The optional [`WatermarkBundle`] rides the wallet-less-resume path
    /// (#1481 §5): it is `Some` only for a watermark-gated reason
    /// ([`VoucherRejectReason::is_watermark_gated`]) whose rejected voucher
    /// recovers to the lane's pinned `signer`, and carries the node's true
    /// watermark so the payer can re-seed (or, on `Underpaid`, rebase) and
    /// resume. Every other reason carries `None`.
    Reject(VoucherRejectReason, Option<WatermarkBundle>),
    /// Fail the stream with no wire reason, and delivery simply stops: a voucher
    /// that fails the rate check with zero bytes or an overflow, or a broken node
    /// invariant.
    ///
    /// The error carries its own attribution: a client-attributable stop is
    /// marked [`ClientPaymentFault`](super::wire::ClientPaymentFault), and a
    /// node-invariant stop carries no marker so the dispatch sink logs it at
    /// `error!`.
    Bail(anyhow::Error),
}

/// Whether a voucher's signed `chunk_price` is the price this node quoted
/// (ADR 003 §Chunk Cadence).
///
/// `chunk_price` is signed by the **payer**, and a released preimage carries no
/// price of its own — its whole value is inherited from the voucher that opened
/// the chain. A voucher signed at the governance floor against a node quoting
/// ten times that would meter every later chunk at a tenth of the quote, and no
/// per-tick moment would reveal it. So this is a plain equality check rather
/// than a tolerance band: a node's rate is fixed for its lifecycle rather than
/// hot-reloaded, and there is nothing here for rounding to explain away.
///
/// A **sealed** voucher meters no chunk and MUST carry a zero price. That is
/// what keeps `chain_root == 0 ⟺ chunk_price == 0` true on both sides of the
/// wire, and it is why nothing downstream — here or on-chain — needs a branch
/// on the zero root.
fn chunk_price_matches_quote(wire: &decdn_protocol::client::Voucher, rate_per_mb: u64) -> bool {
    let expected = if wire.chain_root == [0u8; 32] {
        0
    } else {
        rate_per_mb
    };
    wire.chunk_price == expected
}

/// The bytes a proof added to a lane's paid frontier: the lane's paid bytes after
/// the proof minus the paid bytes before it. A proof that folds nothing adds zero.
fn paid_gain(before: U256, after: U256) -> anyhow::Result<u64> {
    u64::try_from(after.saturating_sub(before))
        .map_err(|_| anyhow::anyhow!("a proof added more than u64 bytes to a lane"))
}

/// Log one rejected payment proof at `info!`: the proof kind (`voucher` or
/// `reveal`), the wire reason, the lane, and whether the reject frame carries
/// a watermark bundle.
///
/// Each rejection ends a paid stream and counts once on
/// `decdn_serve_stream_voucher_rejected_total`, which has no reason split. This
/// line, with [`log_rate_check_reject`], gives the cause of every count at the
/// level nodes run at.
///
/// `with_watermark` is true only for a watermark-gated reason
/// ([`VoucherRejectReason::is_watermark_gated`]) on a lane that holds a prior
/// signature. The payer then rebases from the bundle: a bundle-carrying
/// `AmountRegression`, for example, is a payer whose ledger trails the node's.
/// `BadSignature` and `WrongSigner` are payer faults and never carry a bundle.
fn log_proof_reject(
    proof: &'static str,
    lane_key: &LaneKey,
    reason: VoucherRejectReason,
    with_watermark: bool,
) {
    tracing::info!(
        proof,
        ?reason,
        pool_id = %lane_key.pool_id,
        signer = %lane_key.signer,
        with_watermark,
        "rejecting a payment proof"
    );
}

/// Log a voucher that fails the rate check with zero bytes or an overflow, at
/// `info!` and under the same message as [`log_proof_reject`].
///
/// This stop writes no reject frame: the stream ends as a
/// [`ClientPaymentFault`](super::wire::ClientPaymentFault) bail. The dispatch
/// sink still counts it on `decdn_serve_stream_voucher_rejected_total`, so it
/// logs here with `reason=RateCheck` to keep a cause beside every count.
fn log_rate_check_reject(lane_key: &LaneKey, error: &anyhow::Error) {
    tracing::info!(
        proof = "voucher",
        reason = tracing::field::display("RateCheck"),
        pool_id = %lane_key.pool_id,
        signer = %lane_key.signer,
        with_watermark = false,
        %error,
        "rejecting a payment proof"
    );
}

impl ClientHandler {
    /// Read ONE cumulative voucher, verify it under the per-lane lock, and — on
    /// success — advance the in-memory lane watermark and record it to the pool
    /// store (buffered; the background flush makes it durable, ADR 003
    /// §Off-chain voucher state persistence). Acceptance is implicit: no positive
    /// message is written; the caller keeps delivering.
    // One coherent hot-path unit under the per-lane guard: read, verify, advance,
    // then record. Splitting it would scatter state that must stay atomic under
    // the lock.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn commit_one_proof(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        recv: &mut RecvStream,
        reader: &mut BufferedProofReader,
        anchor: &mut StreamAnchor,
        hash: Hash,
        lane_key: LaneKey,
        lane: Option<&Arc<Mutex<LaneDeliveryState>>>,
        client_node_id: B256,
        rate_per_mb: u64,
        proofs: &ChunkProofs,
    ) -> anyhow::Result<VoucherStop> {
        // Unknown lane: `serve_stream` refuses one pre-serve, so this is a
        // defensive backstop matching the sole callers (which forward `Some`).
        let Some(lane) = lane else {
            return self
                .reject_proof(
                    send,
                    "voucher",
                    &lane_key,
                    VoucherRejectReason::WrongPool,
                    None,
                )
                .await;
        };

        // (1) READ one proof (bounded by the proof wait, which counts `conn`'s
        // transport progress and the wait's ceiling in `proofs`) WITHOUT
        // holding the per-lane lock — a network read must not block same-lane
        // streams. The reader is cancellation-safe.
        let proof = super::proof_wait::await_proof(reader.read(recv), conn, proofs).await?;

        let wire = match proof {
            Proof::Voucher(wire) => wire,
            Proof::Preimage(preimage) => {
                return self
                    .commit_one_preimage(
                        send,
                        anchor,
                        hash,
                        lane_key,
                        lane,
                        client_node_id,
                        preimage,
                    )
                    .await;
            }
        };

        // (1b) The node MUST check the price it is being paid before it meters
        // anything against this voucher (ADR 003 §Chunk Cadence).
        if !chunk_price_matches_quote(&wire, rate_per_mb) {
            return self
                .reject_proof(
                    send,
                    "voucher",
                    &lane_key,
                    VoucherRejectReason::ChunkPriceMismatch,
                    None,
                )
                .await;
        }

        // (2) RECOVER + verify the signature WITHOUT holding the per-lane lock.
        // The `ecrecover` is the most expensive op on the serve path, and
        // signature validity depends only on the voucher bytes and the lane's
        // IMMUTABLE pinned identity — never the mutable watermark. `lane_key`
        // carries that identity (`pool_id`/`signer`/`provider`), so this needs no
        // guard, and concurrent same-lane streams (#1697) recover in parallel
        // instead of serializing on the crypto (#1735). A malformed or high-`s`
        // signature (#836) is rejected here as `BadSignature`; a well-formed
        // signature recovering to the wrong address is `WrongSigner`. Neither
        // reason is watermark-gated, so both carry no wallet-less-resume bundle
        // (#1481 §5).
        let Ok(signed) =
            wire_voucher_to_signed(&wire, lane_key.pool_id, lane_key.signer, lane_key.provider)
        else {
            return self
                .reject_proof(
                    send,
                    "voucher",
                    &lane_key,
                    VoucherRejectReason::BadSignature,
                    None,
                )
                .await;
        };
        if let Err(e) = signed.verify_signer(lane_key.signer, &self.voucher_domain) {
            let reason = match e {
                VoucherError::InvalidSignature => VoucherRejectReason::BadSignature,
                VoucherError::WrongSigner { .. } => VoucherRejectReason::WrongSigner,
            };
            return self
                .reject_proof(send, "voucher", &lane_key, reason, None)
                .await;
        }

        // (3) RE-CHECK the watermark-dependent guards + ADVANCE under the per-lane
        // lock (the watermark checked is the watermark stored). The signature is
        // already verified above; only the monotonicity check against the LIVE
        // watermark and the advance need the lock, and they must stay atomic — two
        // streams reading the same watermark and both advancing would lose one.
        let mut guard = lane.lock().await;

        // Capability-expiry gate (ADR 003 §Revocation). The node stops accepting
        // vouchers a margin BEFORE the expiry: one redeem interval plus the
        // redeemer's landing slack. The preimage path applies the same gate, so
        // the lane's claim is final when the margin starts, and the redeemer's
        // cutoff sweep redeems it before the contract stops paying the
        // capability. A grant inside the margin surfaces as `CapabilityExpired` —
        // distinct from a cap-exhausted `SpendingCapExhausted`, since the fix is
        // a fresh capability, not a cap raise.
        if self.inside_expiry_margin(guard.state.expiry) {
            drop(guard);
            return self
                .reject_proof(
                    send,
                    "voucher",
                    &lane_key,
                    VoucherRejectReason::CapabilityExpired,
                    None,
                )
                .await;
        }

        let verified =
            match Self::verify_voucher(&guard.state, guard.paid_bytes, &signed, &wire, rate_per_mb)
            {
                Ok(v) => v,
                Err(VerifyStop::Reject(reason, bundle)) => {
                    drop(guard);
                    return self
                        .reject_proof(send, "voucher", &lane_key, reason, bundle)
                        .await;
                }
                Err(VerifyStop::Bail(e)) => {
                    drop(guard);
                    if e.is::<super::wire::ClientPaymentFault>() {
                        log_rate_check_reject(&lane_key, &e);
                    }
                    return Err(e);
                }
            };

        // A stale SEALED voucher — at or below the signed watermark, opening no
        // chain — is a payer whose ledger trails the node's: a restarted payer
        // re-signing a watermark it lost. A concurrent sibling's stale sealed
        // voucher finds the lane still holding the credit a deeper voucher paid.
        // On a lane with no credit the stale voucher can pay nothing and nothing
        // else is coming, so reject it with the watermark bundle at once: the
        // payer reseeds and resumes rather than wait out the proof timeout.
        if wire.chain_root == [0u8; 32]
            && U256::from(wire.amount) <= guard.state.last_amount()
            && guard.state.credit_bytes() == 0
        {
            let reason = VoucherRejectReason::AmountRegression;
            let bundle = Self::watermark_bundle_for_reject(reason, &guard.state);
            drop(guard);
            return self
                .reject_proof(send, "voucher", &lane_key, reason, bundle)
                .await;
        }

        // The voucher's full value is lane credit: the lane's credit is its paid
        // bytes minus its delivered bytes (`LaneState::credit_bytes`), so raising
        // the watermark raises the credit of every stream on the lane. A voucher at
        // or below the signed watermark raises nothing, and the serve loop keeps
        // waiting, bounded by the proof budget.
        let credited_bytes = paid_gain(guard.paid_bytes, verified.new_bytes)?;

        // Advance in-memory, then record to the buffered store (a cheap map write;
        // durability is the background flush's job). A poisoned store mutex is the
        // only failure path and is treated as a serve fault. The candidate state
        // MOVES into the lane and `record` borrows it from there, so this proof
        // clones `LaneState` once (inside the store) rather than twice — the
        // remaining clone is the store's own and is the buffer resharding's to
        // remove (issue #1792 item 3; `verified.{new_bytes,amount}` are `Copy`, so
        // the partial move leaves them readable below).
        let trailing = U256::from(wire.amount) < guard.state.last_amount();
        guard.state = verified.next_state;
        guard.paid_bytes = verified.new_bytes;
        // This stream now knows which chain the payer is metering against, so a
        // bare reveal arriving on it afterwards is placeable. A sealed voucher
        // clears the anchor instead — it opens no chain to reveal against.
        anchor.adopt(B256::from(wire.chain_root), trailing);
        if let Err(e) = self.pool_state_store.record(&guard.state) {
            drop(guard);
            return Err(anyhow::anyhow!("lane store record failed: {e}"));
        }

        // Stamp the lane's last-voucher liveness clock (issue #1733) while the
        // per-lane guard is STILL held — a plain field write on the lane state,
        // no separate global lock. Best-effort diagnostic (drives the admin
        // "seconds since last voucher" readout); it gates nothing, so the
        // relaxed store needs no ordering against the record above.
        guard
            .last_voucher_at
            .store(self.coarse_clock.unix_millis(), Ordering::Relaxed);
        drop(guard);
        self.metrics.voucher_received();

        // (3) Post-acceptance bookkeeping (best-effort, off the durability path).
        //
        // Only a proof that actually CREDITED bytes gets a receipt. Under
        // `PayWord` a stream sends an anchor voucher before its first reveal of
        // an epoch. That voucher adds nothing to the lane's paid frontier, and
        // logging it would put a payment in the audit log that this stream never
        // took.
        if credited_bytes > 0 {
            self.record_receipt(hash, credited_bytes, client_node_id, verified.amount);
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

    /// Log a rejected payment proof, write the `VoucherRejected` frame, and
    /// stop the stream. Every proof rejection on both proof paths goes through
    /// here, so each one leaves exactly one [`log_proof_reject`] line.
    async fn reject_proof(
        &self,
        send: &mut SendStream,
        proof: &'static str,
        lane_key: &LaneKey,
        reason: VoucherRejectReason,
        bundle: Option<WatermarkBundle>,
    ) -> anyhow::Result<VoucherStop> {
        log_proof_reject(proof, lane_key, reason, bundle.is_some());
        self.write_reject(send, reason, bundle).await?;
        Ok(VoucherStop::Rejected)
    }

    /// Whether a capability that expires at `expiry` is inside this node's
    /// expiry margin now, read off the coarse clock. Both proof paths refuse a
    /// proof inside the margin with `CapabilityExpired`, and admission refuses
    /// a lane inside it.
    pub(super) fn inside_expiry_margin(&self, expiry: u64) -> bool {
        decdn_common::config::inside_capability_expiry_margin(
            expiry,
            self.capability_expiry_margin_secs,
            self.coarse_clock.unix_seconds(),
        )
    }

    /// Accept ONE released hash-chain preimage: place it against this stream's
    /// anchor, fold it into the lane under the per-lane lock, and record the
    /// advance (ADR 003 §Hash-chain metering).
    ///
    /// This is the cheap tick the whole design exists for. There is no
    /// signature to recover, no acknowledgement to write, and nothing durable to
    /// commit before the next chunk goes out — the cost is one keccak per step
    /// walked, and a duplicate or out-of-order reveal costs not even that.
    /// Acceptance is implicit, exactly as for a voucher: the node simply keeps
    /// delivering.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn commit_one_preimage(
        &self,
        send: &mut SendStream,
        anchor: &mut StreamAnchor,
        hash: Hash,
        lane_key: LaneKey,
        lane: &Arc<Mutex<LaneDeliveryState>>,
        client_node_id: B256,
        preimage: decdn_protocol::client::ChunkPreimage,
    ) -> anyhow::Result<VoucherStop> {
        // Index 0 names the root and resolves to the voucher's own `amount`, so
        // it proves nothing the voucher does not already say and never travels
        // the wire. There is no over-long check to make on the other side: the
        // index is a `u8` and `MAX_CHAIN_LENGTH` is 255, so an index past the end
        // of the chain cannot be encoded — a stronger guarantee than a runtime
        // comparison.
        if preimage.index == 0 {
            return self
                .reject_proof(
                    send,
                    "reveal",
                    &lane_key,
                    VoucherRejectReason::ChainIndexZero,
                    None,
                )
                .await;
        }
        // Rule 1: a reveal on a stream that carries no anchor cannot be placed,
        // because a bare preimage does not name its chain. Not fatal — the payer
        // sends this epoch's root voucher on this stream and resends, and the
        // resend is free because a metering voucher at or below the watermark
        // is already-satisfied and pays no whole chunk.
        let Some(root) = anchor.root else {
            return self
                .reject_proof(
                    send,
                    "reveal",
                    &lane_key,
                    VoucherRejectReason::UnanchoredPreimage,
                    None,
                )
                .await;
        };

        // Optimistic PayWord walk (issue #1792 item 5). Rule 2 still holds — the
        // frontier a reveal walks from is the frontier it advances, and two
        // streams that both advanced the same frontier would lose one reveal — so
        // the advance itself stays atomic under the lock. What moves OFF the lock
        // is the walk: snapshot the frontier (O(1), hashes nothing), release the
        // lock, run the up-to-255-keccak `verify_forward` with no lane contention,
        // then re-lock and apply. `advance_preimage_verified` trusts that walk
        // while the frontier is unchanged and re-hashes under the lock on the rare
        // same-lane race, so a payer releasing sparse indices cannot make
        // sibling streams wait out its keccak walk (ADR 005 §Payment lanes). A
        // reveal at or below the frontier hashes nothing at all, on either path.
        let index = preimage.index;
        let reveal = B256::from(preimage.preimage);
        let walked = {
            let guard = lane.lock().await;
            guard.state.preimage_frontier(root, index)
        };
        let walked_ok = match walked {
            Some((verified_index, tip)) => {
                decdn_incentive::chain::verify_forward(reveal, index - verified_index, tip)
            }
            // Nothing to walk (folds nothing); the value is unused — the apply
            // returns the covered outcome before it consults `walked_ok`.
            None => false,
        };

        let mut guard = lane.lock().await;
        // The voucher path's capability-expiry gate. Without it a stream
        // anchored before the margin could keep extending the claim after the
        // redeemer's cutoff sweep has read it.
        if self.inside_expiry_margin(guard.state.expiry) {
            drop(guard);
            return self
                .reject_proof(
                    send,
                    "reveal",
                    &lane_key,
                    VoucherRejectReason::CapabilityExpired,
                    None,
                )
                .await;
        }
        let (next_state, applied) = match guard
            .state
            .advance_preimage_verified(root, index, reveal, walked, walked_ok)
        {
            Ok(advanced) => advanced,
            Err(e) => {
                let Ok(reason) = voucher_reject_reason(&e) else {
                    drop(guard);
                    return Err(anyhow::anyhow!(
                        "advance_preimage_verified touches no store; unexpected RetrySignal"
                    ));
                };
                // A hash-chain mismatch is terminal and carries no watermark
                // bundle: a preimage has no signature of its own, and a payment
                // watermark cannot repair a wrong seed or a wrong chain. But this
                // path can also raise `SpendingCapExhausted`, when the reveal
                // would push the claim past the capability's cap — and that one
                // IS recoverable, by exactly the route a voucher takes: the payer
                // reads the bundle, raises the cap, and resumes. So ask the same
                // gate the voucher path asks rather than assuming; it answers
                // `None` for every chain-specific reason on its own.
                let bundle = Self::watermark_bundle_for_reject(reason, &guard.state);
                drop(guard);
                return self
                    .reject_proof(send, "reveal", &lane_key, reason, bundle)
                    .await;
            }
        };

        // A reveal that advanced nothing — at or below the frontier, or naming a
        // superseded epoch — advances the lane's claim by nothing, so nothing is
        // recorded and no redeem hint fires. It credits nothing: for an honest
        // payer the lane already holds the money it names, because a concurrent
        // same-lane sibling raced the shared chain index ahead, or a sibling's
        // rollover voucher folded this very reveal into the watermark. That money
        // is already lane credit, which the serve loop reads before it sends. The
        // proof budget bounds a payer that keeps sending proofs that credit
        // nothing. A stream anchored below the lane's watermark is a payer whose
        // ledger trails the node's (a restarted payer): on a lane with no credit
        // its reveal pays nothing and no sibling's money waits behind it, so
        // reject it with the watermark bundle at once and let the payer reseed
        // rather than wait out the proof timeout.
        if !applied.advanced() {
            if anchor.trailing && guard.state.credit_bytes() == 0 {
                let reason = VoucherRejectReason::AmountRegression;
                let bundle = Self::watermark_bundle_for_reject(reason, &guard.state);
                drop(guard);
                return self
                    .reject_proof(send, "reveal", &lane_key, reason, bundle)
                    .await;
            }
            drop(guard);
            return Ok(VoucherStop::Continue { credited_bytes: 0 });
        }

        let owed_bytes = next_state.owed_bytes();
        // Compute the receipt amount (an O(1) read) BEFORE moving the candidate
        // into the lane, so the move can avoid a second full `LaneState` clone
        // this reveal (issue #1792 item 3). The store's own clone inside `record`
        // remains, to be removed with the buffer resharding.
        let amount = u64::try_from(next_state.owed()).unwrap_or(u64::MAX);
        // The reveal's full value is lane credit, as a voucher's is.
        let credited_bytes = paid_gain(guard.paid_bytes, owed_bytes)?;
        guard.state = next_state;
        guard.paid_bytes = owed_bytes;
        if let Err(e) = self.pool_state_store.record(&guard.state) {
            drop(guard);
            return Err(anyhow::anyhow!("lane store record failed: {e}"));
        }
        guard
            .last_voucher_at
            .store(self.coarse_clock.unix_millis(), Ordering::Relaxed);
        drop(guard);
        self.metrics.preimage_reveal_received();

        // Post-acceptance bookkeeping, off the durability path. The receipt
        // amount is the lane's new total claim — the same number a voucher's
        // receipt carries, so the audit log reads uniformly across both proof
        // kinds.
        //
        // The byte figure is `credited_bytes`: what the reveal added to the lane's
        // paid frontier. A reveal walks from the LANE's frontier, so one arriving
        // ahead of a sibling's covers every index between and pays for more than
        // one chunk. That is what an audit log must say (#248/#803). A reveal that
        // advanced nothing logs no payment. `amount` is the lane's new total claim,
        // computed above before the candidate moved into the lane.
        if credited_bytes > 0 {
            self.record_receipt(hash, credited_bytes, client_node_id, amount);
        }
        if let Some(tx) = self.redeem_hint.as_ref()
            && let Err(tokio::sync::mpsc::error::TrySendError::Full(_)) = tx.try_send(lane_key)
        {
            self.metrics.redeem_hint_dropped();
        }
        Ok(VoucherStop::Continue { credited_bytes })
    }

    /// The node-side, watermark-dependent verify half for ONE voucher whose
    /// signature the caller ALREADY verified against the lane's pinned `signer`
    /// outside the per-lane lock (#1735). Evaluated against the current lane
    /// watermark (`state` / `cumulative_bytes`) with no durable side effect.
    /// `bytes_delivered` is self-describing: it comes straight off the wire, so
    /// verification does not depend on the order same-lane streams settle in.
    /// `advance_presigned` runs first (amount/bytes monotonicity against the live
    /// watermark); the per-span **advertised-rate** check then runs on the advance
    /// path against the aggregate span (`applied.amount_delta()` /
    /// `applied.bytes_delta()`), which is order-independent because it is measured
    /// against the lane watermark, and rejects an underpayment as
    /// [`VoucherRejectReason::Underpaid`] carrying the lane's last-accepted
    /// watermark, so a payer whose local watermark diverged can rebase to it.
    /// Delivery stops either way. There is no cumulative rate-floor check: the
    /// delivery floor is a soft floor that `PaymentPool.redeem` enforces by
    /// clamping credited bytes, not by rejecting, so a sub-floor cumulative never
    /// stops the stream here.
    fn verify_voucher(
        state: &LaneState,
        cumulative_bytes: U256,
        signed: &SignedVoucher,
        wire: &decdn_protocol::client::Voucher,
        rate_per_mb: u64,
    ) -> Result<VerifiedVoucher, VerifyStop> {
        // Self-describing: the voucher's cumulative bytes come from the WIRE (ADR
        // 005 §Voucher wire format), so verification does not depend on the order
        // same-lane streams settle in.
        let new_bytes = U256::from(wire.bytes_delivered);

        // `signed` is the reconstruction of `wire` (the caller builds it via
        // `wire_voucher_to_signed`). `advance_presigned` reads `signed.*` while the
        // rate/floor checks below read `wire.*`, so the two MUST agree — a mismatch
        // would verify inconsistent values. Cheap invariant guard for tests/debug.
        debug_assert_eq!(
            signed.voucher.amount,
            U256::from(wire.amount),
            "verify_voucher: signed/wire amount must match"
        );
        debug_assert_eq!(
            signed.voucher.bytes_delivered, new_bytes,
            "verify_voucher: signed/wire bytes_delivered must match"
        );

        // `advance_presigned` re-checks the amount/bytes monotonicity guards
        // against the LIVE watermark and returns the advanced candidate. It skips
        // the signature (already verified by the caller) and touches no store, so
        // it can never surface `RetrySignal` here.
        match state.advance_presigned(signed) {
            Ok((next_state, applied)) => {
                // ADVANCE: this voucher raises the lane watermark. Rate-check the
                // aggregate span it covers (`applied.*_delta()` is measured against
                // the lane watermark, so it is order-independent).
                //
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
                        // `state` is the pre-advance lane, so the bundle reports the
                        // last voucher this node ACCEPTED. An honest payer only gets
                        // here when its own watermark diverged from ours; the bundle
                        // is what lets it rebase instead of wedging the lane.
                        let reason = VoucherRejectReason::Underpaid;
                        return Err(VerifyStop::Reject(
                            reason,
                            Self::watermark_bundle_for_reject(reason, state),
                        ));
                    }
                    Err(e @ (RateError::ZeroBytes | RateError::Overflow)) => {
                        return Err(VerifyStop::Bail(
                            anyhow::Error::new(super::wire::ClientPaymentFault)
                                .context(format!("voucher fails rate check: {e}")),
                        ));
                    }
                }

                // No cumulative-rate floor check here. The delivery floor is a
                // soft floor: `PaymentPool.redeem` settles a sub-floor voucher's
                // `cumulative` and clamps only the byte count it credits toward
                // vote weight (ADR 003 § Rate-floor enforcement). The node never
                // gates a voucher on the floor — it is redemption-time contract
                // state, not a wire check. The advertised-rate check above still
                // protects this node's per-delta revenue at its own quote.
                // The byte watermark this voucher settles is the CHAIN-EXTENDED
                // one: a rollover voucher's own `bytes_delivered` already folds
                // the retired chain in, but a voucher that merely re-asserts the
                // live root leaves the frontier's reveals on top of it. Reading
                // the signed half alone would leave the node's credit accounting
                // a whole chain behind the money it is owed.
                let new_bytes = next_state.owed_bytes().max(new_bytes);
                Ok(VerifiedVoucher {
                    next_state,
                    new_bytes,
                    amount: wire.amount,
                })
            }
            Err(PoolError::AmountRegression { last, .. }) => {
                // A voucher at-or-below the lane's SIGNED watermark: a concurrent
                // same-lane sibling already settled this cumulative. Treat it as
                // ALREADY-SATISFIED: do not advance the watermark. It credits
                // nothing. The one exception here is a DIVERGENT voucher at the SAME amount
                // claiming DIFFERENT bytes — the same money for more bytes or for
                // fewer — which is a single-signer fault (#1699 rule 4). Every
                // honest voucher at the signed amount re-signs the signed bytes.
                // One with fewer bytes cannot adopt a new root either, so it would
                // anchor its stream to a chain the lane never took.
                //
                // A rollover that falls short of the live chain's claim is not this
                // arm, even at exactly the signed watermark: the lane returns
                // `UnderFold`, and the catch-all below rejects it with the bundle
                // stating the fold owed.
                let amount = U256::from(wire.amount);
                if amount == last && new_bytes != state.last_bytes_delivered() {
                    return Err(VerifyStop::Reject(
                        VoucherRejectReason::BytesRegression,
                        None,
                    ));
                }
                // Already-satisfied on the money axis — but the voucher may
                // still be this lane's FIRST root voucher, whose `amount` is the
                // cumulative the lane already holds because nothing has been
                // metered yet. Install the chain it names, or the reveals that
                // follow would fold nothing.
                let next_state = state.adopt_chain(signed).unwrap_or_else(|| state.clone());
                Ok(VerifiedVoucher {
                    next_state,
                    new_bytes: cumulative_bytes,
                    amount: wire.amount,
                })
            }
            Err(e) => {
                // Map to the wire reject reason. `Err(RetrySignal)` (a transient
                // store failure) cannot occur here — `advance_presigned` touches no
                // store — so this is defensive: abort the stream rather than
                // inventing a wire reason for a fault this path cannot produce.
                let reason = match voucher_reject_reason(&e) {
                    Ok(reason) => reason,
                    Err(RetrySignal) => {
                        // Unmarked: a `RetrySignal` from a path that touches no
                        // store is a broken node invariant, so it belongs at
                        // `error!` in the dispatch sink.
                        return Err(VerifyStop::Bail(anyhow::anyhow!(
                            "advance_presigned touches no store; unexpected RetrySignal"
                        )));
                    }
                };
                // The wire reason drops the lane's detail (for an `UnderFold`, the
                // axis and the shortfall), so record it here.
                tracing::debug!(error = %e, ?reason, "voucher fails lane validation");
                // Wallet-less resume (#1481 §5): for a watermark-gated reason,
                // attach the node's true watermark so an authorized funder can
                // reseed and resume. The pinned-signer gate is already enforced
                // by the caller's lock-free `verify_signer` (#1735).
                let bundle = Self::watermark_bundle_for_reject(reason, state);
                Err(VerifyStop::Reject(reason, bundle))
            }
        }
    }

    /// Build the wallet-less-resume [`WatermarkBundle`] for a rejected voucher
    /// (#1481 §5), or `None` when the voucher is not eligible. Returns `Some`
    /// only when BOTH hold:
    /// - `reason` is watermark-gated
    ///   ([`VoucherRejectReason::is_watermark_gated`]);
    /// - the lane has a prior accepted voucher (`last_signature` is `Some`) to
    ///   echo back.
    ///
    /// The pinned-signer gate — a bundle leaks a lane's private watermark, so
    /// only a request that recovers to `state.signer` may pull it, otherwise
    /// anyone who guessed the chain-derivable `pool_id` could — is enforced by
    /// the caller: [`Self::commit_one_proof`] recovers and verifies the signer
    /// against `state.signer` OUTSIDE the per-lane lock before this runs (#1735).
    /// A voucher that recovers to a different address is rejected as `WrongSigner`
    /// and never reaches here, so re-recovering under the lock would only re-do
    /// the `ecrecover` in the critical section this method must stay out of.
    ///
    /// The watermark reported is `state`'s last-accepted amount / bytes, read
    /// while still holding the per-lane guard, before the caller swaps in the
    /// newly verified state.
    fn watermark_bundle_for_reject(
        reason: VoucherRejectReason,
        state: &LaneState,
    ) -> Option<WatermarkBundle> {
        if !reason.is_watermark_gated() {
            return None;
        }
        let last_signature = state.last_signature()?;
        let chain = state.chain();
        Some(WatermarkBundle {
            amount: u64::try_from(state.last_amount()).ok()?,
            bytes_delivered: u64::try_from(state.last_bytes_delivered()).ok()?,
            // The chain half of the watermark. Without it a re-seeding signer
            // would drop the frontier: it would re-sign from `amount` alone and
            // discard `verified_index` chunks the node has already proved. With
            // it, the signer folds `verified_index × chunk_price` back in — the
            // same fold a rollover does (ADR 005 §Watermark bundle).
            chain_root: chain.chain_root.into(),
            verified_index: chain.verified_index,
            tip: chain.tip.into(),
            chunk_price: u64::try_from(chain.chunk_price).ok()?,
            last_signature: last_signature.to_vec(),
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests;
