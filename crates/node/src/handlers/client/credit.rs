//! Lane credit on the serve path (ADR 003 § Credit Window).
//!
//! A node sends a frame only when its lane's confirmed payment covers it. Both
//! serve loops call [`ClientHandler::acquire_credit`] before each send. It debits
//! the lane's credit once, atomically, and reads the payer's proofs while the
//! credit falls short. It also runs the checks that must hold before each send: the
//! takedown check and the pool and signer re-checks.

use std::time::{Duration, Instant};

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
        loop {
            // Mark the current tick seen before the debit, so a credit rise
            // after it wakes the proof read below.
            credit_rx.borrow_and_update();
            if let Some(stop) = self
                .pre_send_checks(send, recv, hash, &lane_key, rate_per_mb, guards)
                .await?
            {
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
                let attempts = proofs.attempts();
                super::wire::record_stream_error(format_args!(
                    "payer sent {attempts} proofs without covering a {len}-byte frame \
                     ({credit} bytes of credit)"
                ));
                tracing::debug!(
                    attempts,
                    frame_bytes = len,
                    credit_bytes = credit,
                    "payer spent its proof budget without covering a frame"
                );
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
}
