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

use super::{
    Arc, B256, BufferedProofReader, ClientHandler, DEFAULT_TOLERANCE_BPS, Hash, LaneDeliveryState,
    LaneKey, LaneState, Mutex, Ordering, Proof, RateError, RecvStream, RetrySignal, SendStream,
    SignedVoucher, U256, VOUCHER_READ_TIMEOUT, VoucherRejectReason, VoucherStop, WatermarkBundle,
    verify_rate, voucher_reject_reason, wire_voucher_to_signed,
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
}

impl StreamAnchor {
    /// Re-anchor to whatever an accepted voucher committed. A sealed voucher
    /// (`chain_root == 0`) clears the anchor: it meters nothing, so a reveal
    /// arriving afterwards on this stream would have no chain to belong to.
    fn adopt(&mut self, chain_root: B256) {
        self.root = (!chain_root.is_zero()).then_some(chain_root);
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

/// One delivered chunk of a stream that the payer has not fully paid yet.
///
/// A proof credits at most the headroom the lane holds (`credit_advance`), so it
/// can pay part of a chunk. The unpaid remainder stays owed: the recoup loop takes
/// the chunk off its queue and reads proofs for it until [`Self::settle`] reports
/// it paid. The serve loop's credit window rests on this. The remainders of the
/// queued chunks and of the chunk in hand, plus the bytes not yet cut into a
/// chunk, are exactly `delivered − paid`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct OwedChunk {
    /// The chunk's delivered wire bytes. Never zero.
    len: u64,
    /// The part of `len` that no proof has credited yet.
    remaining: u64,
}

impl OwedChunk {
    /// A delivered chunk of `len` wire bytes, with nothing paid yet. Both serve
    /// loops cut a chunk only from bytes they delivered, so `len` is never zero.
    pub(super) const fn new(len: u64) -> Self {
        debug_assert!(len > 0, "an owed chunk holds at least one delivered byte");
        Self {
            len,
            remaining: len,
        }
    }

    /// The chunk's delivered wire bytes. This, not [`Self::remaining`], decides
    /// whether it is a whole chunk (`voucher_credit_delta`).
    pub(super) const fn len(self) -> u64 {
        self.len
    }

    /// The bytes of the chunk that are still unpaid.
    pub(super) const fn remaining(self) -> u64 {
        self.remaining
    }

    /// Apply a credit of `credited` bytes and report whether the chunk is now
    /// fully paid.
    ///
    /// # Errors
    ///
    /// A credit larger than [`Self::remaining`]. Every proof path caps its credit
    /// at `remaining`, so this is a node accounting fault. The caller has already
    /// added the credit to `paid`, so it must end the stream rather than let the
    /// excess reopen the credit window.
    pub(super) fn settle(&mut self, credited: u64) -> anyhow::Result<bool> {
        self.remaining = self.remaining.checked_sub(credited).ok_or_else(|| {
            anyhow::anyhow!(
                "a proof credited {credited} bytes against a {}-byte chunk that owes {}",
                self.len,
                self.remaining
            )
        })?;
        Ok(self.remaining == 0)
    }
}

/// Check the serve loop's byte accounting after a recoup phase.
///
/// The recoup phase pays every owed chunk before it returns, so the only unpaid
/// bytes left are the ones not yet cut into a chunk: `delivered − paid` must equal
/// `unvouchered`. A mismatch is a node accounting fault. A shortfall would let the
/// stream end with bytes nobody paid for, or fill the credit window with nothing
/// left to collect. An excess would let `paid` run ahead of `delivered`.
///
/// # Errors
///
/// The accounting does not balance.
pub(super) fn ensure_unpaid_bytes_tracked(
    delivered: u64,
    paid: u64,
    unvouchered: u64,
) -> anyhow::Result<()> {
    if delivered.checked_sub(paid) == Some(unvouchered) {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "serve accounting broke: {delivered} bytes delivered, {paid} paid, \
             {unvouchered} not yet cut into a chunk"
        ))
    }
}

/// The share of an outstanding chunk a voucher may take from lane headroom
/// (ADR 003 §Concurrent Streams).
///
/// A **metering** voucher — one that carries a live `chain_root` — never pays a
/// whole chunk. The payer pays every whole chunk with a reveal and sends a
/// metering voucher only to open, re-anchor, or roll a chain. Those vouchers can
/// fold a reveal that a sibling stream carries and this node has not read yet.
/// If such a voucher took the chunk from headroom, the sibling's reveal would
/// then find no headroom left, and the sibling would wait for a proof the payer
/// never sends. So a metering voucher credits nothing against a whole chunk: it
/// advances the watermark and anchors its stream, and the stream waits for its
/// reveal.
///
/// A sealed voucher (`chain_root == 0`) and a voucher that settles a partial
/// closing chunk (`len < CHUNK_BYTES`) take what the chunk still owes: each is
/// the payment for the bytes it answers.
///
/// The chunk's delivered length decides which case applies, not the part still
/// owed. A whole chunk that a proof paid in part is still a whole chunk, so its
/// remainder waits for a reveal exactly as the whole chunk did.
fn voucher_credit_delta(wire: &decdn_protocol::client::Voucher, owed: OwedChunk) -> u64 {
    if wire.chain_root != [0u8; 32] && owed.len() >= super::CHUNK_BYTES {
        0
    } else {
        owed.remaining()
    }
}

impl ClientHandler {
    /// Read ONE cumulative voucher, verify it under the per-lane lock, and — on
    /// success — advance the in-memory lane watermark and record it to the pool
    /// store (buffered; the background flush makes it durable, ADR 003
    /// §Off-chain voucher state persistence). Acceptance is implicit: no positive
    /// message is written; the caller keeps delivering.
    // One coherent hot-path unit under the per-lane guard: read, verify, advance,
    // credit, heal a fallen-behind payer, then record. Splitting it would scatter
    // state that must stay atomic under the lock.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn commit_one_proof(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        reader: &mut BufferedProofReader,
        anchor: &mut StreamAnchor,
        hash: Hash,
        lane_key: LaneKey,
        lane: Option<&Arc<Mutex<LaneDeliveryState>>>,
        client_node_id: B256,
        rate_per_mb: u64,
        owed: OwedChunk,
    ) -> anyhow::Result<VoucherStop> {
        // Unknown lane: `serve_stream` refuses one pre-serve, so this is a
        // defensive backstop matching the sole callers (which forward `Some`).
        let Some(lane) = lane else {
            self.write_reject(send, VoucherRejectReason::WrongPool, None)
                .await?;
            return Ok(VoucherStop::Rejected);
        };

        // (1) READ one proof (blocking under VOUCHER_READ_TIMEOUT) WITHOUT
        // holding the per-lane lock — a network read must not block same-lane
        // streams. The reader is cancellation-safe.
        let proof = tokio::time::timeout(VOUCHER_READ_TIMEOUT, reader.read(recv))
            .await
            .map_err(|_| {
                anyhow::Error::new(super::wire::PeerFault).context(format!(
                    "proof read timed out after {VOUCHER_READ_TIMEOUT:?}"
                ))
            })??;

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
                        owed,
                    )
                    .await;
            }
        };

        // (1b) The node MUST check the price it is being paid before it meters
        // anything against this voucher (ADR 003 §Chunk Cadence).
        if !chunk_price_matches_quote(&wire, rate_per_mb) {
            self.write_reject(send, VoucherRejectReason::ChunkPriceMismatch, None)
                .await?;
            return Ok(VoucherStop::Rejected);
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
            self.write_reject(send, VoucherRejectReason::BadSignature, None)
                .await?;
            return Ok(VoucherStop::Rejected);
        };
        if let Err(e) = signed.verify_signer(lane_key.signer, &self.voucher_domain) {
            let reason = match e {
                VoucherError::InvalidSignature => VoucherRejectReason::BadSignature,
                VoucherError::WrongSigner { .. } => VoucherRejectReason::WrongSigner,
            };
            self.write_reject(send, reason, None).await?;
            return Ok(VoucherStop::Rejected);
        }

        // (3) RE-CHECK the watermark-dependent guards + ADVANCE under the per-lane
        // lock (the watermark checked is the watermark stored). The signature is
        // already verified above; only the monotonicity check against the LIVE
        // watermark and the advance need the lock, and they must stay atomic — two
        // streams reading the same watermark and both advancing would lose one.
        let mut guard = lane.lock().await;

        // The lane's authoritative cumulative BEFORE this voucher, captured for the
        // wallet-less-resume check below: a voucher strictly under it is a payer
        // whose watermark fell behind ours, not a normal advance.
        let prior_last_amount = guard.state.last_amount();

        // Capability-expiry gate (ADR 003 §Capability delegation). `expiry == 0`
        // means "not tracked" and never expires. An expired grant surfaces as
        // `CapabilityExpired` — distinct from a cap-exhausted `SpendingCapExhausted`,
        // since the fix is a fresh capability, not a cap raise.
        let expiry = guard.state.expiry;
        if expiry != 0 && self.coarse_clock.unix_seconds() >= expiry {
            drop(guard);
            self.write_reject(send, VoucherRejectReason::CapabilityExpired, None)
                .await?;
            return Ok(VoucherStop::Rejected);
        }

        let verified = match Self::verify_voucher(
            &guard.state,
            guard.bytes_delivered_cumulative,
            &signed,
            &wire,
            rate_per_mb,
        ) {
            Ok(v) => v,
            Err(VerifyStop::Reject(reason, bundle)) => {
                drop(guard);
                tracing::debug!(
                    ?reason,
                    with_watermark = bundle.is_some(),
                    "rejecting voucher"
                );
                self.write_reject(send, reason, bundle).await?;
                return Ok(VoucherStop::Rejected);
            }
            Err(VerifyStop::Bail(e)) => {
                drop(guard);
                return Err(e);
            }
        };

        // Advance in-memory, then record to the buffered store (a cheap map write;
        // durability is the background flush's job). A poisoned store mutex is the
        // only failure path and is treated as a serve fault. The candidate state
        // MOVES into the lane and `record` borrows it from there, so this proof
        // clones `LaneState` once (inside the store) rather than twice — the
        // remaining clone is the store's own and is the buffer resharding's to
        // remove (issue #1792 item 3; `verified.{new_bytes,amount}` are `Copy`, so
        // the partial move leaves them readable below).
        guard.state = verified.next_state;
        guard.bytes_delivered_cumulative = verified.new_bytes;
        // This stream now knows which chain the payer is metering against, so a
        // bare reveal arriving on it afterwards is placeable. A sealed voucher
        // clears the anchor instead — it opens no chain to reveal against.
        anchor.adopt(B256::from(wire.chain_root));
        // Rule #1 cap: credit this stream from lane headroom — proved bytes no
        // stream has claimed yet. `paid_credited` is monotone and bounded by the
        // settled watermark, so a voucher can never reopen the credit window for
        // bytes no proof settled. A metering voucher takes no whole chunk from
        // that headroom (`voucher_credit_delta`): the reveal it precedes, or a
        // sibling's reveal it folded, pays the chunk. Computed under the guard so
        // the read of `paid_credited` and its store cannot interleave with
        // another proof.
        let (new_credited, credited_bytes) = credit_advance(
            guard.paid_credited,
            voucher_credit_delta(&wire, owed),
            verified.new_bytes,
        )?;
        guard.paid_credited = new_credited;
        if let Err(e) = self.channel_state_store.record(&guard.state) {
            drop(guard);
            return Err(anyhow::anyhow!("lane store record failed: {e}"));
        }

        // Wallet-less resume (#1946): decide the heal BEFORE stamping this as an
        // accepted voucher — a rejected voucher must not advance the last-voucher
        // liveness diagnostic. Built under the guard; the reject is sent without it.
        let resume_bundle = Self::stale_resume_bundle(
            &guard.state,
            &guard.active_streams,
            wire.amount,
            prior_last_amount,
        );
        if let Some(bundle) = resume_bundle {
            drop(guard);
            self.write_reject(send, VoucherRejectReason::AmountRegression, Some(bundle))
                .await?;
            return Ok(VoucherStop::Rejected);
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
        // an epoch. That voucher is a metering voucher against a whole chunk, so
        // it credits nothing, and logging it would put a payment in the audit
        // log that this stream never took.
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
    #[allow(clippy::too_many_arguments)]
    async fn commit_one_preimage(
        &self,
        send: &mut SendStream,
        anchor: &mut StreamAnchor,
        hash: Hash,
        lane_key: LaneKey,
        lane: &Arc<Mutex<LaneDeliveryState>>,
        client_node_id: B256,
        preimage: decdn_protocol::client::ChunkPreimage,
        owed: OwedChunk,
    ) -> anyhow::Result<VoucherStop> {
        // Index 0 names the root and resolves to the voucher's own `amount`, so
        // it proves nothing the voucher does not already say and never travels
        // the wire. There is no over-long check to make on the other side: the
        // index is a `u8` and `MAX_CHAIN_LENGTH` is 255, so an index past the end
        // of the chain cannot be encoded — a stronger guarantee than a runtime
        // comparison.
        if preimage.index == 0 {
            self.write_reject(send, VoucherRejectReason::ChainIndexZero, None)
                .await?;
            return Ok(VoucherStop::Rejected);
        }
        // Rule 1: a reveal on a stream that carries no anchor cannot be placed,
        // because a bare preimage does not name its chain. Not fatal — the payer
        // sends this epoch's root voucher on this stream and resends, and the
        // resend is free because an at-or-below-watermark voucher is
        // already-satisfied rather than rejected.
        let Some(root) = anchor.root else {
            self.write_reject(send, VoucherRejectReason::UnanchoredPreimage, None)
                .await?;
            return Ok(VoucherStop::Rejected);
        };

        // Optimistic PayWord walk (issue #1792 item 5). Rule 2 still holds — the
        // frontier a reveal walks from is the frontier it advances, and two
        // streams that both advanced the same frontier would lose one reveal — so
        // the advance itself stays atomic under the lock. What moves OFF the lock
        // is the walk: snapshot the frontier (O(1), hashes nothing), release the
        // lock, run the up-to-255-keccak `verify_forward` with no lane contention,
        // then re-lock and apply. `advance_preimage_verified` trusts that walk
        // while the frontier is unchanged and re-hashes under the lock on the rare
        // same-lane race, so a payer releasing sparse indices can no longer make
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
                self.write_reject(send, reason, bundle).await?;
                return Ok(VoucherStop::Rejected);
            }
        };

        // A reveal that advanced nothing — at or below the frontier, or naming a
        // superseded epoch — advances the lane's claim by nothing, so nothing is
        // recorded and no redeem hint fires. But this stream still DELIVERED the
        // bytes `owed` still names, and the lane already holds the money for
        // them: a concurrent same-lane sibling raced the shared chain index
        // ahead, or a sibling's rollover voucher folded this very reveal into the
        // watermark.
        // So credit this stream from that lane headroom rather than throttling it
        // to a `MAX_PROOFS_PER_CHUNK` stall. Without it, cross-stream reveal
        // reordering starves the slower stream: its reveals keep landing below the
        // frontier, credit nothing, and its unpaid frontier is stopped as
        // `ProofBudgetExhausted` (the concurrent same-lane voucher-starvation bug).
        //
        // The headroom is this reveal's to take because no metering voucher takes
        // a whole chunk from it (`voucher_credit_delta`). A sibling's rollover
        // that folds this reveal advances the watermark and credits its own
        // stream nothing, so the headroom it opens waits here for the reveal the
        // payer did send.
        //
        // The credit is bounded by the lane's proven `owed_bytes`, and
        // `paid_credited` is monotone, so total credit across every same-lane
        // stream can never exceed money the node can redeem — no double-pay, no
        // under-pay. If the payer has genuinely under-paid, the headroom is short,
        // `credit_advance` credits only what exists, and the rest of the chunk
        // stays owed.
        if !applied.advanced() {
            // Frontier unchanged, so `guard.state`'s owed claim is what a sibling
            // has already paid the lane for. Read the receipt amount before the
            // drop, mirroring the advanced path below.
            let owed_bytes = guard.state.owed_bytes();
            let amount = u64::try_from(guard.state.owed()).unwrap_or(u64::MAX);
            let (new_credited, credited_bytes) =
                credit_advance(guard.paid_credited, owed.remaining(), owed_bytes)?;
            guard.paid_credited = new_credited;
            guard
                .last_voucher_at
                .store(self.coarse_clock.unix_millis(), Ordering::Relaxed);
            drop(guard);
            // Only bytes actually credited get a receipt, exactly as the advanced
            // reveal and benign voucher paths gate it; receipts sum to
            // `paid_credited`, so this cannot double-count.
            if credited_bytes > 0 {
                self.record_receipt(hash, credited_bytes, client_node_id, amount);
            }
            return Ok(VoucherStop::Continue { credited_bytes });
        }

        let owed_bytes = next_state.owed_bytes();
        // Compute the receipt amount (an O(1) read) BEFORE moving the candidate
        // into the lane, so the move can avoid a second full `LaneState` clone
        // this reveal (issue #1792 item 3). The store's own clone inside `record`
        // remains, to be removed with the buffer resharding.
        let amount = u64::try_from(next_state.owed()).unwrap_or(u64::MAX);
        guard.state = next_state;
        guard.bytes_delivered_cumulative = owed_bytes;
        // The same rule #1 cap the voucher path uses, against the CHAIN-EXTENDED
        // frontier: a reveal is what pays for these bytes, so they are as settled
        // as a signature's.
        let (new_credited, credited_bytes) =
            credit_advance(guard.paid_credited, owed.remaining(), owed_bytes)?;
        guard.paid_credited = new_credited;
        if let Err(e) = self.channel_state_store.record(&guard.state) {
            drop(guard);
            return Err(anyhow::anyhow!("lane store record failed: {e}"));
        }
        guard
            .last_voucher_at
            .store(self.coarse_clock.unix_millis(), Ordering::Relaxed);
        drop(guard);

        // Post-acceptance bookkeeping, off the durability path. The receipt
        // amount is the lane's new total claim — the same number a voucher's
        // receipt carries, so the audit log reads uniformly across both proof
        // kinds.
        //
        // The byte figure is `credited_bytes`, not what this stream still owes,
        // for the same reason the voucher path logs it: the two diverge. A reveal
        // walks from the LANE's frontier, so one arriving ahead of a sibling's
        // covers every index between and pays for more than one stream's chunk,
        // while `credit_advance` caps it below what the chunk still owes whenever
        // the watermark does not reach that far. `credited_bytes` is what the lane actually
        // charged for, which is what an audit log must say (#248/#803). Gated the
        // same way, so a reveal that advanced the frontier but credited nothing
        // against the cap logs no payment. `amount` is the lane's new total claim,
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
                // A voucher at-or-below the lane watermark: a concurrent same-lane
                // sibling already settled this cumulative. Benign — treat as
                // ALREADY-SATISFIED: do not advance the watermark, do not reject.
                // The one exception is a DIVERGENT voucher at the SAME amount
                // claiming MORE bytes — same money, more bytes — which is a
                // single-signer fault (#1699 rule 4).
                let amount = U256::from(wire.amount);
                if amount == last && new_bytes > state.last_bytes_delivered() {
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
                // Wallet-less resume (#1481 §5): for a gated regression/exhaustion
                // reason, attach the node's true watermark so an authorized funder
                // can re-seed and resume. The pinned-signer gate is already
                // enforced by the caller's lock-free `verify_signer` (#1735).
                let bundle = Self::watermark_bundle_for_reject(reason, state);
                Err(VerifyStop::Reject(reason, bundle))
            }
        }
    }

    /// The wallet-less-resume watermark to hand a payer whose voucher fell BEHIND
    /// our watermark (#1946), or `None` to keep serving under the benign-swallow.
    ///
    /// A voucher STRICTLY below our last-accepted `amount` is a payer re-signing
    /// from a stale anchor — typically a client that crashed before it persisted
    /// vouchers we already accepted (its watermark persists once, at end of fetch).
    /// Every such voucher credits nothing, so delivery wedges at the one-chunk
    /// credit floor. Returning the node's authoritative watermark lets the payer
    /// reseed and resume; it re-verifies the bundle against its own key first, and
    /// its reseed is a no-op unless the bundle actually advances it.
    ///
    /// Gated on `active_streams <= 1`: a concurrent same-lane sibling (#1697)
    /// legitimately sends out-of-order sub-watermark vouchers whose bytes another
    /// stream's advance already covers, and the terminal reject the caller builds
    /// from this would tear down that honest stream. For the multi-stream lane the
    /// caller keeps the benign-swallow; a genuinely wedged one still recovers via
    /// the `VOUCHER_READ_TIMEOUT` backstop. The count is maintained off-chain too
    /// (see the admission gate), so this holds without pool-status data.
    ///
    /// `state` is read while the caller still holds the per-lane guard, AFTER it has
    /// swapped in the newly verified state; for a benign sub-watermark voucher that
    /// swap leaves the watermark (amount / bytes / signature) unchanged, so the
    /// bundle still reports our true frontier.
    fn stale_resume_bundle(
        state: &LaneState,
        active_streams: &std::sync::atomic::AtomicU32,
        wire_amount: u64,
        prior_last_amount: U256,
    ) -> Option<WatermarkBundle> {
        if U256::from(wire_amount) >= prior_last_amount
            || active_streams.load(Ordering::Relaxed) > 1
        {
            return None;
        }
        Self::watermark_bundle_for_reject(VoucherRejectReason::AmountRegression, state)
    }

    /// Build the wallet-less-resume [`WatermarkBundle`] for a rejected voucher
    /// (#1481 §5), or `None` when the voucher is not eligible. Returns `Some`
    /// only when BOTH hold:
    /// - `reason` is one of the watermark-gated regression/exhaustion reasons
    ///   (`AmountRegression` / `BytesRegression` / `SpendingCapExhausted`);
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
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, AtomicU64};

    use alloy::primitives::{Address, B256, U256};
    use alloy::signers::local::PrivateKeySigner;
    use decdn_incentive::store::PoolStateStore;
    use decdn_incentive::{LaneKey, LaneState};
    use tokio::sync::Mutex;

    use super::super::{LaneDeliveryState, handler_over_store};
    use crate::metrics::Metrics;

    /// A well-formed voucher verifies against a fresh lane and advances the
    /// candidate state to the voucher's cumulative amount/bytes — the pure,
    /// no-I/O half of [`super::ClientHandler::commit_one_proof`]. The
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
            decdn_incentive::LaneChain::NONE,
        );
        let lane = Arc::new(Mutex::new(LaneDeliveryState {
            state: seed,
            bytes_delivered_cumulative: U256::ZERO,
            paid_credited: U256::ZERO,
            active_streams: Arc::new(AtomicU32::new(0)),
            last_voucher_at: AtomicU64::new(0),
        }));
        handler.lanes.insert(lane_key, Arc::clone(&lane));

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
            chain_root: B256::ZERO,
            chunk_price: U256::ZERO,
        }
        .sign(&signer_key, &domain)
        .expect("sign voucher");
        let wire = decdn_protocol::client::Voucher {
            signature: signed_voucher.signature.as_bytes().to_vec(),
            amount: u64::try_from(amount).expect("amount fits u64 in this test"),
            bytes_delivered: u64::try_from(new_bytes).expect("bytes fit u64 in this test"),
            chain_root: [0u8; 32],
            chunk_price: 0,
        };

        let snapshot = lane.lock().await.state.clone();
        let verified = super::ClientHandler::verify_voucher(
            &snapshot,
            U256::ZERO,
            &signed_voucher,
            &wire,
            rate_per_mb,
        )
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
            u64::try_from(amount).expect("amount fits u64 in this test"),
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

    /// A wire voucher naming `chain_root`, for the credit-share tests. Only the
    /// root matters to [`super::voucher_credit_delta`]; the other fields are
    /// placeholders.
    fn wire_voucher_with_root(chain_root: [u8; 32]) -> decdn_protocol::client::Voucher {
        decdn_protocol::client::Voucher {
            signature: vec![0u8; 65],
            amount: 0,
            bytes_delivered: 0,
            chain_root,
            chunk_price: 0,
        }
    }

    /// A metering voucher (live root) takes no whole chunk from lane headroom,
    /// even when the headroom covers it: the reveal it precedes, or a sibling's
    /// reveal it folded, pays that chunk.
    #[test]
    fn a_metering_voucher_credits_no_whole_chunk() {
        let chunk = super::super::CHUNK_BYTES;
        let wire = wire_voucher_with_root([0x5E; 32]);
        let delta = super::voucher_credit_delta(&wire, super::OwedChunk::new(chunk));
        assert_eq!(delta, 0, "a metering voucher pays no whole chunk");
        let (new_credited, credited) =
            super::credit_advance(U256::ZERO, delta, U256::from(4 * chunk))
                .expect("credit_advance is infallible for in-range values");
        assert_eq!(new_credited, U256::ZERO, "the headroom stays unclaimed");
        assert_eq!(credited, 0);
    }

    /// A metering voucher that settles a partial closing chunk is the payment
    /// for those bytes, so it credits what the chunk still owes.
    #[test]
    fn a_metering_voucher_settles_a_closing_partial() {
        let partial = super::super::CHUNK_BYTES - 1;
        let wire = wire_voucher_with_root([0x5E; 32]);
        assert_eq!(
            super::voucher_credit_delta(&wire, super::OwedChunk::new(partial)),
            partial,
            "a closing residual under one chunk credits in full"
        );
    }

    /// A sealed voucher (zero root) meters no chain, so nothing else can pay the
    /// chunk it answers: it credits what the chunk still owes.
    #[test]
    fn a_sealed_voucher_credits_a_whole_chunk() {
        let chunk = super::super::CHUNK_BYTES;
        let wire = wire_voucher_with_root([0u8; 32]);
        assert_eq!(
            super::voucher_credit_delta(&wire, super::OwedChunk::new(chunk)),
            chunk,
            "a sealed voucher pays its whole chunk"
        );
    }

    /// A proof that pays part of a chunk leaves the rest owed, and the chunk is
    /// settled only when its remainder reaches zero (#2132).
    #[test]
    fn an_owed_chunk_settles_only_when_fully_paid() -> anyhow::Result<()> {
        let mut owed = super::OwedChunk::new(1000);
        assert!(!owed.settle(0)?, "a zero credit settles nothing");
        assert!(!owed.settle(400)?, "a partial credit leaves the rest owed");
        assert_eq!(owed.remaining(), 600);
        assert_eq!(owed.len(), 1000, "the delivered length does not change");
        assert!(owed.settle(600)?, "paying the remainder settles the chunk");
        assert_eq!(owed.remaining(), 0);
        Ok(())
    }

    /// A credit larger than what the chunk still owes is a node accounting fault,
    /// never a quiet settle: the serve loop has already added it to `paid`.
    #[test]
    fn an_over_credit_is_refused() {
        let mut owed = super::OwedChunk::new(10);
        assert!(owed.settle(11).is_err(), "an over-credit must not settle");
    }

    /// The post-recoup accounting check passes only when the unpaid bytes are
    /// exactly the ones not yet cut into a chunk.
    #[test]
    fn unpaid_bytes_must_all_be_tracked() {
        assert!(super::ensure_unpaid_bytes_tracked(1000, 900, 100).is_ok());
        assert!(
            super::ensure_unpaid_bytes_tracked(1000, 800, 100).is_err(),
            "a shortfall nothing tracks is refused"
        );
        assert!(
            super::ensure_unpaid_bytes_tracked(1000, 1001, 0).is_err(),
            "paid running ahead of delivered is refused"
        );
    }

    /// A whole chunk that a proof paid in part is still a whole chunk: a metering
    /// voucher takes nothing from its remainder, so the remainder cannot eat the
    /// headroom a sibling's reveal needs. A sealed voucher takes exactly the
    /// remainder, never the chunk's full length.
    #[test]
    fn a_partly_paid_whole_chunk_keeps_its_whole_chunk_rule() {
        let chunk = super::super::CHUNK_BYTES;
        let mut owed = super::OwedChunk::new(chunk);
        assert!(
            !owed
                .settle(chunk / 4)
                .expect("a partial credit fits the chunk")
        );
        let remainder = chunk - chunk / 4;

        let metering = wire_voucher_with_root([0x5E; 32]);
        assert_eq!(
            super::voucher_credit_delta(&metering, owed),
            0,
            "a metering voucher pays no part of a whole chunk"
        );
        let sealed = wire_voucher_with_root([0u8; 32]);
        assert_eq!(
            super::voucher_credit_delta(&sealed, owed),
            remainder,
            "a sealed voucher pays what the chunk still owes"
        );
    }

    /// A partly paid closing partial chunk credits only its remainder, from any
    /// voucher.
    #[test]
    fn a_partly_paid_closing_partial_credits_its_remainder() {
        let partial = super::super::CHUNK_BYTES / 2;
        let mut owed = super::OwedChunk::new(partial);
        assert!(!owed.settle(100).expect("a partial credit fits the chunk"));
        let metering = wire_voucher_with_root([0x5E; 32]);
        assert_eq!(super::voucher_credit_delta(&metering, owed), partial - 100);
    }

    /// A voucher at-or-below the lane watermark — a concurrent sibling raced ahead
    /// — is `AlreadySatisfied`: `verify_voucher` returns Ok WITHOUT advancing the
    /// candidate watermark. It must NOT reject (that would kill an honest lagging
    /// stream, #1699).
    #[test]
    fn stale_voucher_is_benign_and_does_not_regress_watermark() {
        use alloy::primitives::B256;
        use alloy::signers::local::PrivateKeySigner;

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
            decdn_incentive::LaneChain::NONE,
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
            chain_root: B256::ZERO,
            chunk_price: U256::ZERO,
        }
        .sign(&signer_key, &domain)
        .expect("sign low voucher");
        let wire = decdn_protocol::client::Voucher {
            signature: signed_low.signature.as_bytes().to_vec(),
            amount: u64::try_from(low_amount).expect("amount fits u64 in this test"),
            bytes_delivered: one_mb,
            chain_root: [0u8; 32],
            chunk_price: 0,
        };

        // verify against the high watermark; the sibling's watermark already
        // covers this stream's delivered.
        let verified = super::ClientHandler::verify_voucher(
            &seed,
            U256::from(two_mb),
            &signed_low,
            &wire,
            rate_per_mb,
        )
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
    #[test]
    fn divergent_voucher_at_equal_amount_is_rejected() {
        use alloy::primitives::B256;
        use alloy::signers::local::PrivateKeySigner;

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
            decdn_incentive::LaneChain::NONE,
        );
        // Same amount, but claims 2 MB of bytes.
        let two_mb = one_mb * 2;
        let divergent_voucher = decdn_incentive::Voucher {
            pool_id,
            signer,
            provider,
            amount,
            bytes_delivered: U256::from(two_mb),
            chain_root: B256::ZERO,
            chunk_price: U256::ZERO,
        }
        .sign(&signer_key, &domain)
        .expect("sign divergent voucher");
        let wire = decdn_protocol::client::Voucher {
            signature: divergent_voucher.signature.as_bytes().to_vec(),
            amount: u64::try_from(amount).expect("amount fits u64 in this test"),
            bytes_delivered: two_mb,
            chain_root: [0u8; 32],
            chunk_price: 0,
        };

        let err = super::ClientHandler::verify_voucher(
            &seed,
            U256::from(one_mb),
            &divergent_voucher,
            &wire,
            rate_per_mb,
        )
        .expect_err("a divergent equal-amount voucher must be rejected");
        match err {
            super::VerifyStop::Reject(reason, _) => assert_eq!(
                reason,
                decdn_protocol::client::VoucherRejectReason::BytesRegression,
                "divergent equal-amount voucher rejects as BytesRegression"
            ),
            super::VerifyStop::Bail(e) => panic!("expected a Reject, got Bail({e})"),
        }
    }

    /// #1735: signature validity is hoisted OUT of the per-lane lock. A voucher
    /// signed by the wrong key is rejected by the (lock-free) signature recovery
    /// — `verify_signer` against the lane's pinned signer — while the inside-lock
    /// `advance_presigned` no longer inspects the signature at all: it would
    /// happily advance the same voucher. This is exactly what lets concurrent
    /// same-lane streams recover in parallel and only briefly serialize on the
    /// advance.
    #[test]
    fn wrong_signer_is_rejected_without_the_lane_lock() {
        use alloy::primitives::{Address, B256, U256};
        use alloy::signers::local::PrivateKeySigner;

        let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
        let pinned_key = PrivateKeySigner::random();
        let pinned_signer = pinned_key.address();
        let wrong_key = PrivateKeySigner::random();
        let pool_id = B256::repeat_byte(0x21);
        let provider = Address::repeat_byte(0x55);

        // A fresh lane pinned to `pinned_signer` at a zero watermark.
        let seed = LaneState::hydrate(
            pool_id,
            pinned_signer,
            provider,
            U256::MAX,
            0,
            U256::ZERO,
            U256::ZERO,
            None,
            decdn_incentive::LaneChain::NONE,
        );

        // A well-formed, monotone voucher — but SIGNED BY THE WRONG KEY. Its
        // `signer` field still names the pinned signer; only the signature is
        // forged, so recovery lands on `wrong_key`'s address.
        let rate_per_mb = 1_000_000u64;
        let one_mb = decdn_incentive::rate::BYTES_PER_MB;
        let amount = decdn_incentive::min_payment(one_mb, rate_per_mb);
        let forged = decdn_incentive::Voucher {
            pool_id,
            signer: pinned_signer,
            provider,
            amount,
            bytes_delivered: U256::from(one_mb),
            chain_root: B256::ZERO,
            chunk_price: U256::ZERO,
        }
        .sign(&wrong_key, &domain)
        .expect("sign with the wrong key");

        // OUTSIDE the lock: the signature recovery rejects it as WrongSigner. This
        // is the reject the handler performs before ever taking the guard.
        let err = forged
            .verify_signer(pinned_signer, &domain)
            .expect_err("a wrong-key voucher must fail signature recovery");
        assert!(
            matches!(err, decdn_incentive::VoucherError::WrongSigner { .. }),
            "wrong-key voucher recovers to a different signer: {err:?}"
        );

        // INSIDE the (would-be) lock: `advance_presigned` does NOT re-check the
        // signature — it advances the same forged voucher against the live
        // watermark. The safety of moving recovery out rests on this: the accept
        // decision is fully made by the lock-free recovery above.
        let (next, _applied) = seed
            .advance_presigned(&forged)
            .expect("advance_presigned ignores the signature and advances");
        assert_eq!(
            next.last_bytes_delivered(),
            U256::from(one_mb),
            "advance_presigned advanced the watermark without inspecting the signature"
        );
    }

    /// #1735: the monotonicity check and the watermark advance stay atomic under
    /// the lock. Two same-lane streams that both recovered their vouchers against
    /// the SAME zero snapshot cannot both advance: once the first advances the
    /// live watermark, the second — re-checked against that LIVE watermark inside
    /// the lock, not against its stale snapshot — is a benign `AmountRegression`
    /// rather than a second advance that would lose the first's update.
    #[test]
    fn concurrent_advances_recheck_the_live_watermark_no_lost_update() {
        use alloy::primitives::{Address, B256, U256};
        use alloy::signers::local::PrivateKeySigner;

        let domain = alloy::sol_types::eip712_domain! { name: "t", version: "1", };
        let signer_key = PrivateKeySigner::random();
        let signer = signer_key.address();
        let pool_id = B256::repeat_byte(0x21);
        let provider = Address::repeat_byte(0x55);

        let rate_per_mb = 1_000_000u64;
        let one_mb = decdn_incentive::rate::BYTES_PER_MB;
        let two_mb = one_mb * 2;

        // Both streams see the same zero-watermark snapshot when they recover.
        let snapshot = LaneState::hydrate(
            pool_id,
            signer,
            provider,
            U256::MAX,
            0,
            U256::ZERO,
            U256::ZERO,
            None,
            decdn_incentive::LaneChain::NONE,
        );

        let mk = |bytes: u64| {
            let amount = decdn_incentive::min_payment(bytes, rate_per_mb);
            decdn_incentive::Voucher {
                pool_id,
                signer,
                provider,
                amount,
                bytes_delivered: U256::from(bytes),
                chain_root: B256::ZERO,
                chunk_price: U256::ZERO,
            }
            .sign(&signer_key, &domain)
            .expect("sign voucher")
        };
        let voucher_hi = mk(two_mb); // the winner: advances to 2 MB
        let voucher_lo = mk(one_mb); // the straggler: recovered against zero too

        // First stream advances the live watermark to 2 MB.
        let (after_hi, _) = snapshot
            .advance_presigned(&voucher_hi)
            .expect("the higher cumulative voucher advances from zero");
        assert_eq!(after_hi.last_bytes_delivered(), U256::from(two_mb));

        // Second stream re-checks against the LIVE (2 MB) watermark — NOT its own
        // zero snapshot — so its lower cumulative is a regression, not an advance.
        // Advancing it against the stale snapshot would regress the watermark and
        // lose the first stream's update.
        let err = after_hi
            .advance_presigned(&voucher_lo)
            .expect_err("a straggler below the live watermark must not advance");
        assert!(
            matches!(err, decdn_incentive::PoolError::AmountRegression { .. }),
            "straggler is a benign amount regression against the live watermark: {err:?}"
        );

        // Sanity: against its own stale snapshot the straggler WOULD have advanced
        // — proving the re-check against the live watermark is what prevents the
        // lost update.
        snapshot
            .advance_presigned(&voucher_lo)
            .expect("against the stale zero snapshot the straggler advances");
    }
}
