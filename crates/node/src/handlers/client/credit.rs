//! Stream credit on the serve path (ADR 003 § Credit Window).
//!
//! A node sends a frame only when the stream's confirmed payment covers it.
//! Credit belongs to the stream: each proof on it claims a share of the lane's
//! payment for it ([`super::LaneDeliveryState::claim`]), and nothing carries
//! between streams. Both serve loops call [`ClientHandler::acquire_credit`]
//! before each send. It reads the payer's proofs while the stream's credit
//! falls short of the frame. It also runs the checks that must hold before each
//! send: the takedown check and the pool and signer re-checks.

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
    /// The stream's credit covers the frame.
    Ready,
    /// The stream ended before the frame could go out. The caller returns the stop
    /// as the stream's end.
    Stopped(ServeStop),
}

impl ClientHandler {
    /// The checks state for one stream. `funder` is the pool owner the
    /// stream's admission read, or `None` when no pool view is wired. The clock
    /// starts now, because admission already verified solvency once.
    pub(super) async fn send_guards(
        &self,
        lane: &Arc<Mutex<LaneDeliveryState>>,
        funder: Option<Address>,
    ) -> SendGuards {
        SendGuards {
            funder,
            held_signer_cap: Some(lane.lock().await.state.cap),
            last_pool_check: Instant::now(),
            interval: self.pool_recheck_interval(),
        }
    }

    /// The checks that run before each send (ADR 003 § Credit Window, Takedown
    /// latency; § Pool solvency, re-checks).
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

    /// Wait until the stream's credit covers a `len`-byte frame, reading the
    /// payer's proofs while it falls short (ADR 003 § Credit Window).
    ///
    /// The stream's credit is `paid − delivered`: `paid` counts the bytes the
    /// proofs on this stream claimed for it, and `delivered` the bytes it has
    /// already sent. The caller adds the frame to `delivered` once it is on the
    /// wire. When the
    /// credit falls short, the node reads one proof at a time and checks again
    /// after each. A proof the node has not read yet costs nothing: whenever it
    /// is read, it claims its share for this stream.
    ///
    /// The wait is bounded. [`MAX_PROOFS_PER_CHUNK`] proofs that leave the credit
    /// short end the stream as [`ServeStop::ProofBudgetExhausted`], because a payer
    /// that sends proofs that credit nothing is at fault, not this node. The
    /// per-frame proof-wait ceiling and the no-progress timeout of
    /// [`super::proof_wait`] end a payer that sends nothing.
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
        delivered: u64,
        paid: &mut u64,
    ) -> anyhow::Result<Acquired> {
        // One proof budget and one proof-wait ceiling for the whole wait.
        let mut proofs = ChunkProofs::start();
        loop {
            if let Some(stop) = self
                .pre_send_checks(send, recv, hash, &lane_key, rate_per_mb, guards)
                .await?
            {
                return Ok(Acquired::Stopped(stop));
            }
            let credit = paid.saturating_sub(delivered);
            if credit >= len {
                return Ok(Acquired::Ready);
            }
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
                )
                .await;
            waits.end();
            let stop = committed.map_err(|e| {
                e.context(format!(
                    "proof {attempt} of at most {MAX_PROOFS_PER_CHUNK} while the stream's \
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
            }
        }
    }

    /// End a stream whose proof budget is spent while its credit does not
    /// cover its next frame: reject it `AmountRegression` with the lane's
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
}
