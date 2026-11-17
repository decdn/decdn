//! Funding admission for one serve stream (ADR 003 § Pool solvency, ADR 005
//! § Open-time refusal classes).
//!
//! The funding gates read the pool, the signer's registration and the lane:
//! pool status, signer authorization, the funder deny, capability intake, the
//! lane and its registration clamp, a closing pool, an expiring capability, and
//! the pool deposit gate. Each runs once per stream, at one of two points.
//!
//! - An open that does no fill work (a hit, a partial that covers the span, or
//!   the node's own origin with a proven size) answers before admission. Its
//!   gates run when the payer's first proof is on the stream
//!   ([`ClientHandler::admit_on_first_proof`]). An open the payer does not take
//!   costs no chain read, and a refusal rides in a `StreamError` frame before
//!   any `ChunkData`.
//! - A miss that pulls through runs the same gates
//!   ([`ClientHandler::funding_gates`]) before any upstream work, and refuses
//!   with a signed `ok: false`.
//!
//! The node holds the blob (`InUse`) for the whole stream, so an open that
//! waits for its first proof keeps its blob. [`UnadmittedStreams`] bounds how
//! many such opens one connection holds.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::credit::SendGuards;
use super::outcome::ServeEnd;
use super::proof_wait::ChunkProofs;
use super::{
    Address, Arc, B256, BufferedProofReader, CHUNK_BYTES, ClientHandler, Connection, Hash,
    LaneDeliveryState, LaneKey, MAX_UNADMITTED_STREAMS_PER_CONNECTION, Mutex, PublicKey,
    RecvStream, SendStream, ServeRejectReason, StreamRequest, StreamRequestExt,
};
use crate::pool_view::{Lifecycle, PoolStatus};
use decdn_incentive::payment_pool::SignerAuthorization;

/// What the funding gates read and resolved for a request.
pub(super) struct Funding {
    /// The pool's status, or `None` when no pool view is wired.
    pub(super) pool_status: Option<PoolStatus>,
    /// The request's lane, after capability intake, or `None` when the request
    /// names no lane this node holds.
    pub(super) known_lane: Option<Arc<Mutex<LaneDeliveryState>>>,
}

/// An open that answered before admission: what its funding gates need when
/// the payer's first proof arrives. It holds one of its connection's
/// [`UnadmittedStreams`] slots until admission consumes it or the stream ends.
pub(super) struct DeferredAdmission {
    /// The stream's request.
    req: StreamRequest,
    /// The request's extension, for its capability.
    ext: StreamRequestExt,
    /// The verified client binding the request resolved, if any.
    verified_client: Option<Address>,
    /// The lane the request names, or `None` when it carries no binding.
    lane_key: Option<LaneKey>,
    /// The requesting peer, for the refusal logs.
    peer: PublicKey,
    /// The quoted price.
    rate_per_mb: u64,
    /// The blob size the answer states, for the deposit gate's price.
    total_bytes: u64,
    /// The connection's slot for this unadmitted stream.
    _slot: UnadmittedSlot,
}

impl DeferredAdmission {
    /// The admission an open that answered before admission runs at its first
    /// proof.
    #[allow(clippy::too_many_arguments)]
    pub(super) const fn new(
        req: StreamRequest,
        ext: StreamRequestExt,
        verified_client: Option<Address>,
        lane_key: Option<LaneKey>,
        peer: PublicKey,
        rate_per_mb: u64,
        total_bytes: u64,
        slot: UnadmittedSlot,
    ) -> Self {
        Self {
            req,
            ext,
            verified_client,
            lane_key,
            peer,
            rate_per_mb,
            total_bytes,
            _slot: slot,
        }
    }
}

/// A stream that passed admission: the lane it pays on and the checks that run
/// before each send.
pub(super) struct Admitted {
    /// The lane's key.
    pub(super) lane_key: LaneKey,
    /// The lane.
    pub(super) lane: Arc<Mutex<LaneDeliveryState>>,
    /// The takedown, pool and signer checks that run before each send.
    pub(super) guards: SendGuards,
}

/// Where a serve loop's stream stands on admission.
pub(super) enum Admission {
    /// Admitted before the response.
    Admitted(Admitted),
    /// Answered before admission. The serve loop admits at the first proof.
    OnFirstProof(DeferredAdmission),
}

/// The count of one connection's streams that answered an open and have not
/// yet passed admission. The accept path resets a new stream while the count
/// is at [`MAX_UNADMITTED_STREAMS_PER_CONNECTION`]. The bound is soft by the
/// streams already past that check and not yet answered, which the
/// per-connection stream cap bounds.
#[derive(Debug, Default)]
pub(super) struct UnadmittedStreams {
    /// Live [`UnadmittedSlot`]s.
    count: AtomicUsize,
}

impl UnadmittedStreams {
    /// Whether the connection holds its cap of unadmitted streams.
    pub(super) fn at_cap(&self) -> bool {
        self.count.load(Ordering::Acquire) >= MAX_UNADMITTED_STREAMS_PER_CONNECTION
    }

    /// Count one more unadmitted stream until the returned slot drops.
    pub(super) fn take(self: &Arc<Self>) -> UnadmittedSlot {
        self.count.fetch_add(1, Ordering::AcqRel);
        UnadmittedSlot {
            streams: Arc::clone(self),
        }
    }
}

/// One unadmitted stream's place in its connection's [`UnadmittedStreams`].
/// Dropping it frees the place, on every exit of the stream.
#[derive(Debug)]
pub(super) struct UnadmittedSlot {
    /// The connection's count.
    streams: Arc<UnadmittedStreams>,
}

impl Drop for UnadmittedSlot {
    fn drop(&mut self) {
        self.streams.count.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ClientHandler {
    /// The funding gates up to the deposit gate, for a request that names
    /// `lane_key` (ADR 003 § Pool solvency, § Capability delegation): the pool
    /// status, the signer's authorization, the funder deny, capability intake,
    /// the lane and its registration clamp, a closing pool and a capability
    /// inside the expiry margin. Returns what they read, or the refusal.
    ///
    /// With no pool view wired (dev/test) the pool gates pass and no lane
    /// registers from a capability.
    pub(super) async fn funding_gates(
        &self,
        req: &StreamRequest,
        ext: &StreamRequestExt,
        verified_client: Option<Address>,
        lane_key: Option<LaneKey>,
        rate_per_mb: u64,
    ) -> Result<Funding, ServeRejectReason> {
        let pool_id = B256::from(req.pool_id);
        // The pool's `getPool` view (owner, remaining, lifecycle), read once and
        // reused by every gate below. The wired view answers from its projection
        // and confirms a pool it has not folded with one `getPool`. `None` with a
        // view wired means the pool does not exist, is closed, or the read
        // faulted, so the node refuses rather than serve a pool it cannot
        // confirm.
        let pool_status = match self.pool_view.as_ref() {
            Some(view) => view.status(pool_id).await,
            None => None,
        };
        if self.pool_view.is_some() && pool_status.is_none() {
            return Err(ServeRejectReason::PoolUnconfirmed);
        }

        // On-chain registration is write-once per `(pool, signer)`, so a
        // registered signer's `cap` and `expiry` bound every voucher this node
        // can redeem. Refuse a registered signer whose registration expired or
        // whose `cap - spent` cannot pay one chunk. The registered terms clamp
        // the lane below. A `getAuthorization` fault with no earlier registered
        // read refuses as unconfirmed, never as an exhausted cap.
        let mut registered_terms: Option<(u64, u64)> = None;
        if let (Some(view), Some(signer)) = (self.pool_view.as_ref(), verified_client) {
            let floor_micro = decdn_incentive::min_payment(CHUNK_BYTES, rate_per_mb);
            let auth = view.signer_authorization(pool_id, signer).await;
            if let Some(SignerAuthorization::Registered { cap, expiry, .. }) = auth {
                registered_terms = Some((cap, expiry));
            }
            let now = self.coarse_clock.unix_seconds();
            if let Some(reason) = super::dispatch::signer_refusal(auth, floor_micro, now) {
                return Err(reason);
            }
        }

        // ADR 011 § On Blacklist Event: refuse a pool whose funder
        // (`getPool.owner`) is on the local `denied_origins` or the on-chain
        // origin blacklist. Spend authority is a signer question and compliance
        // a funder question, so a clean delegate signer does not launder a
        // blacklisted funder.
        if let Some(status) = pool_status
            && self.content_deny.is_origin_denied(&status.owner)
        {
            return Err(ServeRejectReason::OriginDenied);
        }

        // Capability intake (ADR 003 § Capability delegation): a capability
        // whose owner signature verifies against the pool owner registers the
        // lane and persists the grant. It runs only with a confirmed pool, so
        // the owner is known.
        if let (Some(signer), Some(capability), Some(status)) =
            (verified_client, ext.capability.as_ref(), pool_status)
        {
            self.intake_capability(pool_id, signer, status.owner, capability);
        }

        // The request's one lane read, after intake, so a lane its own
        // capability just registered is visible. Every later decision for this
        // stream uses this answer.
        let known_lane = match lane_key {
            Some(key) => self.lanes.get(&key).map(|e| Arc::clone(e.value())),
            None => None,
        };
        #[cfg(any(test, feature = "test-support"))]
        self.fire_lane_after_resolution();

        // Hold the signer's registered terms on the lane: the voucher path
        // refuses past the lane's `cap` and `expiry`.
        if let (Some(lane), Some((cap, expiry))) = (known_lane.as_ref(), registered_terms) {
            self.clamp_lane_to_registration(lane, cap, expiry).await;
        }

        // A `Closing` pool takes no top-up, and its redemption ends at the
        // dispute deadline, so this node does not serve on it. A requester that
        // holds a lane hears `Unfunded`, anyone else `NotFound`.
        if let Some(status) = pool_status
            && matches!(status.lifecycle, Lifecycle::Closing { .. })
        {
            return Err(ServeRejectReason::PoolClosing {
                proven: known_lane.is_some(),
            });
        }
        // A lane whose capability is inside the expiry margin cannot pay for a
        // voucher, so admitting it would only stop at the first one.
        if let Some(lane) = known_lane.as_ref() {
            let expiry = lane.lock().await.state.expiry;
            if self.inside_expiry_margin(expiry) {
                return Err(ServeRejectReason::CapabilityExpired);
            }
        }
        Ok(Funding {
            pool_status,
            known_lane,
        })
    }

    /// Admit an open that answered before admission, once its payer's first
    /// proof is on the stream: the funding gates, then the lane the stream
    /// pays on, then the deposit gate. Returns the admitted lane and the
    /// stream's send checks, or the refusal.
    ///
    /// A request with no verified binding names no lane and refuses
    /// `OwnerMismatch`; a bound request with no lane refuses `UnknownLane`.
    /// Both sign `NotFound`, so a requester cannot map lane existence.
    pub(super) async fn admit_on_first_proof(
        &self,
        adm: DeferredAdmission,
    ) -> Result<Admitted, ServeRejectReason> {
        let funding = self
            .funding_gates(
                &adm.req,
                &adm.ext,
                adm.verified_client,
                adm.lane_key,
                adm.rate_per_mb,
            )
            .await?;
        let Some(lane_key) = adm.lane_key else {
            self.warn_unbound_request(&adm.peer);
            return Err(ServeRejectReason::OwnerMismatch);
        };
        let Some(lane) = funding.known_lane else {
            if let Some(suppressed) = self.unknown_lane_warn.admit() {
                tracing::warn!(
                    peer = %adm.peer,
                    ?lane_key,
                    suppressed,
                    interval = ?self.unknown_lane_warn.interval(),
                    "stream request on unknown lane; refusing at admission"
                );
            }
            return Err(ServeRejectReason::UnknownLane);
        };
        // The pool's `remaining − M` must cover the request's chunk-group-aligned
        // span at this node's rate, capped at one chunk. The pull-through tiers in
        // `window.rs` run the same gate.
        let guard_bytes =
            super::dispatch::aligned_span(adm.req.byte_offset, adm.req.byte_len, adm.total_bytes)
                .min(CHUNK_BYTES);
        if let Some(status) = funding.pool_status
            && !self.pool_remaining_covers_window(status.remaining, guard_bytes, adm.rate_per_mb)
        {
            self.log_deposit_refusal(
                B256::from(adm.req.pool_id),
                Hash::from_bytes(adm.req.hash),
                status
                    .remaining
                    .saturating_sub(self.pool_min_remaining_deposit),
                decdn_incentive::min_payment(guard_bytes, adm.rate_per_mb),
            );
            return Err(ServeRejectReason::InsufficientDeposit);
        }
        let guards = self
            .send_guards(&lane, funding.pool_status.map(|s| s.owner))
            .await;
        Ok(Admitted {
            lane_key,
            lane,
            guards,
        })
    }

    /// Wait until the payer's first proof is buffered on `reader`, under the
    /// proof wait's no-progress timeout and ceiling ([`super::proof_wait`]).
    /// The proof stays buffered for the serve loop.
    ///
    /// # Errors
    ///
    /// The proof wait's faults and the reader's, all peer-attributable.
    pub(super) async fn await_first_proof(
        &self,
        conn: &Connection,
        recv: &mut RecvStream,
        reader: &mut BufferedProofReader,
    ) -> anyhow::Result<()> {
        let proofs = ChunkProofs::start();
        Box::pin(super::proof_wait::await_proof(
            reader.fill_one(recv),
            conn,
            &proofs,
        ))
        .await
        .map_err(|e| e.context("waiting for the payer's first proof"))
    }

    /// End a stream whose admission refused at its first proof: write the
    /// refusal's wire class as a `StreamError` before any `ChunkData` and
    /// finish the stream cleanly. The stream ends as `Refused(reason)`, so it
    /// counts on its reason's counter.
    ///
    /// # Errors
    ///
    /// An encoding fault or a write to a stream this node already closed.
    pub(super) async fn refuse_admission(
        &self,
        send: &mut SendStream,
        reason: ServeRejectReason,
    ) -> anyhow::Result<ServeEnd> {
        self.write_midstream_refusal(send, reason).await?;
        Ok(ServeEnd::Refused(reason))
    }
}
