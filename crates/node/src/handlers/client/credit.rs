//! Lane credit on the serve path (ADR 003 § Credit Window).
//!
//! A node sends a frame only when its lane's confirmed payment covers it. Both
//! serve loops call [`ClientHandler::acquire_credit`] before each send. It debits
//! the lane's credit once, atomically, and reads the payer's proofs while the
//! credit falls short. It also runs the checks that must hold before each send: the
//! takedown check and the pool and signer re-checks.

use std::time::{Duration, Instant};

use futures_util::FutureExt;

use super::outcome::ServeStop;
use super::proof_wait::ChunkProofs;
use super::serve_waits::{ServeWaits, Wait};
use super::voucher::StreamAnchor;
use super::{
    Address, Arc, B256, BufferedProofReader, ClientHandler, Connection, Hash, LaneDeliveryState,
    LaneKey, MAX_PROOFS_PER_CHUNK, Mutex, RecvStream, SendStream, U256, VoucherRejectReason,
    VoucherStop,
};

/// What a stream holds for the checks that run before each send: the pool funder
/// for the takedown check, the signer's held cap for the signer re-check, and the
/// clock of the wall-clock re-checks.
#[derive(Debug)]
pub(super) struct SendGuards {
    /// The pool's funder (`getPool.owner`), or `None` when no pool view is wired.
    funder: Option<Address>,
    /// The cap the node holds on the lane, or `None` when no re-check applies.
    held_signer_cap: Option<U256>,
    /// When the pool and signer re-checks last ran.
    last_pool_check: Instant,
    /// How often they run.
    interval: Duration,
}

/// How a wait for credit ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Acquired {
    /// The lane's credit covered the frame. With `charge` the node debited it.
    Ready,
    /// The stream ended before the frame could go out. The caller returns the stop
    /// as the stream's end.
    Stopped(ServeStop),
}

impl ClientHandler {
    /// The checks state for one stream. The clock starts now, because admission
    /// already verified solvency once.
    pub(super) async fn send_guards(
        &self,
        lane: &Arc<Mutex<LaneDeliveryState>>,
        lane_key: &LaneKey,
    ) -> SendGuards {
        SendGuards {
            funder: self.pool_funder(lane_key.pool_id).await,
            held_signer_cap: Some(lane.lock().await.state.cap),
            last_pool_check: Instant::now(),
            interval: self.pool_recheck_interval(),
        }
    }

    /// The checks that run before each send (ADR 003 § Credit Window, Takedown
    /// latency; § Pool solvency, re-checks). They run before the debit, so a stop
    /// leaves the lane's credit on the lane.
    ///
    /// - The takedown check runs before every frame. A takedown that lands
    ///   mid-stream stops delivery at the next frame (ADR 011 § On Blacklist
    ///   Event). It resets the stream, and a client that resumes gets the signed
    ///   refusal from the open-time gate.
    /// - The pool re-check and the signer re-check run on a wall-clock cadence,
    ///   not on every frame. The pool view refreshes only as the node folds
    ///   on-chain redemptions, so reading it faster returns the same value. Each
    ///   stop is in-band, with a clean `PoolExhausted` or `SignerCapExhausted`, so
    ///   the owner learns to fund a pool or raise a cap.
    ///
    /// Returns the stop, or `None` when the stream may send.
    pub(super) async fn pre_send_checks(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        hash: Hash,
        lane_key: &LaneKey,
        rate_per_mb: u64,
        guards: &mut SendGuards,
    ) -> anyhow::Result<Option<ServeStop>> {
        if self.takedown_landed(hash, guards.funder) {
            Self::terminate_for_takedown(send, recv, hash);
            return Ok(Some(ServeStop::Takedown));
        }
        if guards.last_pool_check.elapsed() < guards.interval {
            return Ok(None);
        }
        guards.last_pool_check = Instant::now();
        if let Some(stop) = self.pool_recheck_stop(lane_key, &hash).await {
            self.write_reject(send, VoucherRejectReason::PoolExhausted, None)
                .await?;
            return Ok(Some(stop));
        }
        if let Some(held) = guards.held_signer_cap
            && self
                .signer_cap_drained_midstream(lane_key.pool_id, lane_key.signer, held, rate_per_mb)
                .await
        {
            self.write_reject(send, VoucherRejectReason::SignerCapExhausted, None)
                .await?;
            tracing::warn!(
                pool_id = %lane_key.pool_id, stopped_signer = %lane_key.signer, %hash,
                "mid-stream SignerCapExhausted: this signer drained its shared cap at other nodes since admission, so its cap−spent headroom no longer covers a chunk; owner should raise the signer's cap or delegate a fresh capability. This is the SIGNER level, distinct from PoolExhausted — the pool may still be solvent on other signers' budgets"
            );
            return Ok(Some(ServeStop::SignerCapExhausted));
        }
        Ok(None)
    }

    /// Debit `len` bytes from the lane's credit before a send, reading the payer's
    /// proofs while the credit falls short (ADR 003 § Credit Window).
    ///
    /// A stream on a lane that holds credit debits at once and reads no proof. When
    /// the credit falls short, the node reads one proof at a time and tries the
    /// debit again after each. A proof that raises the lane's paid frontier raises
    /// the credit of every stream on the lane, so a sibling's proof can cover this
    /// frame too.
    ///
    /// The wait is bounded. [`MAX_PROOFS_PER_CHUNK`] proofs that leave the credit
    /// short end the stream as [`ServeStop::ProofBudgetExhausted`], because a payer
    /// that sends proofs that credit nothing is at fault, not this node. The
    /// per-frame proof-wait ceiling and the no-progress timeout of
    /// [`super::proof_wait`] end a payer that sends nothing.
    ///
    /// `paid` counts the bytes that accepted proofs added to the lane, for the
    /// stream's end tag.
    ///
    /// With `charge` false the call waits until the lane holds `len` bytes of credit
    /// and debits nothing. The cache-miss loop uses it to learn that the lane has
    /// paid for something before it asks the pull leg for bytes.
    ///
    /// # Errors
    ///
    /// A proof read that faults (a transport drop, a stalled or over-ceiling wait),
    /// a lane-store failure, or a broken accounting invariant. A peer-attributable
    /// error carries its marker.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn acquire_credit(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        recv: &mut RecvStream,
        reader: &mut BufferedProofReader,
        anchor: &mut StreamAnchor,
        waits: &mut ServeWaits,
        guards: &mut SendGuards,
        hash: Hash,
        lane_key: LaneKey,
        lane: &Arc<Mutex<LaneDeliveryState>>,
        client_node_id: B256,
        rate_per_mb: u64,
        len: u64,
        charge: bool,
        paid: &mut u64,
    ) -> anyhow::Result<Acquired> {
        // One proof budget and one proof-wait ceiling for the whole wait.
        let mut proofs = ChunkProofs::start();
        // Ticks when any proof on the lane raises its credit, so a sibling's
        // payment wakes this wait once this stream has paid on itself.
        let mut credit_rx = lane.lock().await.credit_tx.subscribe();
        // Commit the proofs the payer already delivered on this stream before
        // the first debit: a funded stream's proofs are credited near arrival
        // rather than waiting unread until a sibling's higher proof supersedes
        // them and they pay for nothing. Each drained proof spends the frame's
        // proof budget, which only ever stops a stream on a shortfall below,
        // so a funded stream is never stopped for its drained proofs — while a
        // payer that slivers a frame across many proofs is cut at the same
        // budget whether the node waited for them or found them buffered. A
        // drain-time rejection stops the stream with nothing debited.
        if let Some(stopped) = self
            .credit_buffered_proofs(
                send,
                recv,
                reader,
                anchor,
                hash,
                lane_key,
                lane,
                conn.stable_id(),
                client_node_id,
                rate_per_mb,
                &mut proofs,
                paid,
            )
            .await?
        {
            return Ok(stopped);
        }
        loop {
            // Mark the current tick seen before the debit, so a credit rise
            // after it wakes the proof read below.
            credit_rx.borrow_and_update();
            if let Some(stop) = self
                .pre_send_checks(send, recv, hash, &lane_key, rate_per_mb, guards)
                .await?
            {
                // A pool or signer stop is no verdict on the payer's proofs, and
                // the payer counts the ones it sent as lane credit. Read and
                // credit the proofs still in flight, so the credit the payer
                // counts is the credit the lane holds. A takedown resets the
                // stream, so nothing more arrives on it.
                if stop != ServeStop::Takedown {
                    Box::pin(self.settle_trailing_proofs(
                        conn,
                        send,
                        recv,
                        reader,
                        anchor,
                        hash,
                        lane_key,
                        lane,
                        client_node_id,
                        rate_per_mb,
                    ))
                    .await;
                }
                return Ok(Acquired::Stopped(stop));
            }
            let covered = if charge {
                self.debit_lane(lane, len).await
            } else {
                let credit = self.lane_credit(lane).await;
                if credit >= len { Ok(()) } else { Err(credit) }
            };
            let credit = match covered {
                Ok(()) => return Ok(Acquired::Ready),
                Err(credit) => credit,
            };
            if proofs.exhausted() {
                self.reject_spent_budget(send, lane, proofs.attempts(), len, credit)
                    .await?;
                return Ok(Acquired::Stopped(ServeStop::ProofBudgetExhausted));
            }
            let attempt = proofs.next_attempt();
            // Timed by hand, not through `ServeWaits::time`: nesting this large
            // future in another trips `large_futures`. The wait ends before `?`, so
            // a proof that times out still counts, and a cancel counts it on the
            // guard's drop.
            waits.begin(Wait::Proof);
            let committed = self
                .commit_one_proof(
                    conn,
                    send,
                    recv,
                    reader,
                    anchor,
                    hash,
                    lane_key,
                    Some(lane),
                    client_node_id,
                    rate_per_mb,
                    &proofs,
                    // A stream wakes for a sibling's proof only once its own
                    // payer has paid on it, so a stream held open unread cannot
                    // draw the credit its siblings paid for.
                    (*paid > 0).then_some(&mut credit_rx),
                )
                .await;
            waits.end();
            let stop = committed.map_err(|e| {
                e.context(format!(
                    "proof {attempt} of at most {MAX_PROOFS_PER_CHUNK} while the lane's \
                     {credit} bytes of credit fell short of a {len}-byte frame"
                ))
            })?;
            match stop {
                VoucherStop::Continue { credited_bytes } => {
                    *paid = paid.saturating_add(credited_bytes);
                }
                VoucherStop::Rejected => {
                    return Ok(Acquired::Stopped(ServeStop::VoucherRejected));
                }
                VoucherStop::Woken => proofs.refund_attempt(),
            }
        }
    }

    /// End a stream whose proof budget is spent while the lane's credit does
    /// not cover its next frame: reject it `AmountRegression` with the lane's
    /// watermark bundle, so a payer whose ledger trails the node's — a
    /// restarted payer re-proving a chain the lane already folded — reseeds
    /// from the lane's signed state and resumes, instead of retrying the stale
    /// chain against the budget forever.
    ///
    /// The bundle gate holds by invariant: a timed-out proof wait ends the
    /// stream as an error before the budget check, so every attempt the budget
    /// counted committed a proof — a signer-verified voucher, or a reveal
    /// placed against an anchor such a voucher set on this stream.
    async fn reject_spent_budget(
        &self,
        send: &mut SendStream,
        lane: &Arc<Mutex<LaneDeliveryState>>,
        attempts: u32,
        len: u64,
        credit: u64,
    ) -> anyhow::Result<()> {
        super::wire::record_stream_error(format_args!(
            "payer sent {attempts} proofs without covering a {len}-byte frame \
             ({credit} bytes of credit)"
        ));
        let bundle = {
            let guard = lane.lock().await;
            Self::watermark_bundle_for_reject(VoucherRejectReason::AmountRegression, &guard.state)
        };
        tracing::debug!(
            attempts,
            frame_bytes = len,
            credit_bytes = credit,
            with_watermark = bundle.is_some(),
            "payer spent its proof budget without covering a frame"
        );
        self.write_reject(send, VoucherRejectReason::AmountRegression, bundle)
            .await
    }

    /// Read, without waiting, the proofs the payer has already delivered on
    /// this stream and credit them to the lane, spending the frame's proof
    /// budget (`proofs`) for each one.
    ///
    /// The node waits on a stream's proofs only while the lane's credit falls
    /// short, so a funded stream's proofs would otherwise sit unread until a
    /// sibling's higher proof supersedes them — and then spend the proof
    /// budget in [`Self::acquire_credit`] crediting nothing. Draining before
    /// each frame commits them near arrival, so that backlog never builds.
    /// Spending the budget here keeps the per-frame bound one number: the
    /// budget stops a stream only on a shortfall, so a funded stream is never
    /// stopped for its drained proofs, while a payer that slivers a frame
    /// across many proofs is cut at [`MAX_PROOFS_PER_CHUNK`] whether the node
    /// waited for its proofs or found them buffered.
    ///
    /// The drain never waits: a partial frame stays buffered for the next read
    /// (the reader is cancellation-safe), and a read error is left for the
    /// waiting read in [`Self::acquire_credit`], which is the stream's error
    /// surface — a transport end is sticky and re-surfaces there when the
    /// credit falls short.
    ///
    /// Returns `Some` when a drained proof was rejected: the stream stops, and
    /// the caller has debited nothing.
    #[allow(clippy::too_many_arguments)]
    async fn credit_buffered_proofs(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        reader: &mut BufferedProofReader,
        anchor: &mut StreamAnchor,
        hash: Hash,
        lane_key: LaneKey,
        lane: &Arc<Mutex<LaneDeliveryState>>,
        conn_id: usize,
        client_node_id: B256,
        rate_per_mb: u64,
        proofs: &mut ChunkProofs,
        paid: &mut u64,
    ) -> anyhow::Result<Option<Acquired>> {
        while !proofs.exhausted() {
            let Some(read) = reader.read(recv).now_or_never() else {
                return Ok(None);
            };
            let Ok(proof) = read else {
                return Ok(None);
            };
            proofs.next_attempt();
            let committed = Box::pin(self.commit_wire_proof(
                send,
                conn_id,
                anchor,
                hash,
                lane_key,
                lane,
                client_node_id,
                rate_per_mb,
                proof,
            ))
            .await?;
            match committed {
                VoucherStop::Continue { credited_bytes } => {
                    *paid = paid.saturating_add(credited_bytes);
                }
                VoucherStop::Rejected => {
                    return Ok(Some(Acquired::Stopped(ServeStop::VoucherRejected)));
                }
                // The drain passes no wake channel, so a commit cannot end as
                // `Woken`; treat it as a no-op drain end.
                VoucherStop::Woken => return Ok(None),
            }
        }
        Ok(None)
    }

    /// After a clean `StreamEnd`, or after a pool or signer stop, read and credit
    /// the proofs the payer sent that the node has not read, until the payer's
    /// FIN or [`super::POST_END_DRAIN_TIMEOUT`].
    ///
    /// The node reads a proof only while the lane's credit falls short, so a
    /// stream that finishes on credit a sibling paid leaves its own proofs
    /// unread. The payer counts them as lane credit. Dropping them would leave
    /// the lane short of what the payer believes it paid, and a sibling stream
    /// would wait for a proof the payer never sends.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn settle_trailing_proofs(
        &self,
        conn: &Connection,
        send: &mut SendStream,
        recv: &mut RecvStream,
        reader: &mut BufferedProofReader,
        anchor: &mut StreamAnchor,
        hash: Hash,
        lane_key: LaneKey,
        lane: &Arc<Mutex<LaneDeliveryState>>,
        client_node_id: B256,
        rate_per_mb: u64,
    ) {
        let settle = async {
            loop {
                let proofs = ChunkProofs::start();
                let committed = self
                    .commit_one_proof(
                        conn,
                        send,
                        recv,
                        reader,
                        anchor,
                        hash,
                        lane_key,
                        Some(lane),
                        client_node_id,
                        rate_per_mb,
                        &proofs,
                        None,
                    )
                    .await;
                // The payer's FIN, a fault, or a rejection ends the settle.
                match committed {
                    Ok(VoucherStop::Continue { .. }) => {}
                    Ok(_) => break,
                    // The payer's FIN lands here too, so debug, not warn — but
                    // a lane-store fault mid-drain leaves the lane holding
                    // less credit than the payer counts, which this drain
                    // exists to prevent, so it must not vanish without a line.
                    Err(e) => {
                        tracing::debug!(
                            error = %format_args!("{e:#}"),
                            "the post-end proof drain stopped on a read or \
                             commit end"
                        );
                        break;
                    }
                }
            }
        };
        if tokio::time::timeout(super::POST_END_DRAIN_TIMEOUT, Box::pin(settle))
            .await
            .is_err()
        {
            // A drain cut off with proofs unread leaves the lane short of what
            // the payer counts; the payer heals it at its next silent stall.
            tracing::debug!("the post-end proof drain hit its timeout with proofs possibly unread");
        }
    }
}
