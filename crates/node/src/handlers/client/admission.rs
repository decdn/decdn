//! Funding admission for one serve stream (ADR 003 § Pool solvency, ADR 005
//! § Open-time refusal classes).
//!
//! The funding gates read the pool, the signer's registration and the lane:
//! pool status, signer authorization, the funder deny, capability intake, the
//! lane, a closing pool, an expiring capability, and
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
//! An open that answers before admission holds no load-shed slot: it takes
//! the node-wide and per-client slots at admission, so unpaid opens cannot shed
//! paid serves. The empty blob sells no byte and skips admission.
//!
//! The node holds the blob (`InUse`) for the whole stream, so an open that
//! waits for its first proof keeps its blob. [`Places`] bounds how many
//! streams one connection holds before admission, and how many pins such
//! streams hold across the node; an open past the pin budget answers with no
//! pin, and admission pins its blob.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::credit::SendGuards;
use super::outcome::ServeEnd;
use super::{
    Address, Arc, B256, BufferedProofReader, CHUNK_BYTES, ClientHandler, Connection, Deployment,
    Hash, LaneDeliveryState, LaneId, Mutex, PublicKey, RecvStream, SendStream, ServeRejectReason,
    StreamRequest, StreamRequestExt,
};
use crate::load_shed::{RequestClass, ShedSlot};
use crate::pool_view::{Lifecycle, PoolStatus};

/// What the funding gates read and resolved for a request.
pub(super) struct Funding {
    /// The pool's status, or `None` when no pool view is wired.
    pub(super) pool_status: Option<PoolStatus>,
    /// The request's lane, after capability intake, or `None` when the request
    /// names no lane this node holds.
    pub(super) known_lane: Option<Arc<Mutex<LaneDeliveryState>>>,
}

/// An open that answered before admission: what its load-shed and funding
/// gates need when the payer's first proof arrives. It holds its stream's
/// [`UnadmittedSlot`] until admission consumes it or the stream ends.
pub(super) struct DeferredAdmission {
    /// The stream's request. Boxed: its pool account makes it the largest
    /// part of the admission.
    req: Box<StreamRequest>,
    /// The request's extension, for its capability.
    ext: StreamRequestExt,
    /// The accepted `PaymentPool` contract the request names.
    deployment: Deployment,
    /// The verified client binding the request resolved, if any.
    verified_client: Option<Address>,
    /// The lane the request names, or `None` when it carries no binding.
    lane_key: Option<LaneId>,
    /// The requesting peer, for the refusal logs.
    peer: PublicKey,
    /// The quoted price.
    rate_per_mb: u64,
    /// The blob size the answer states, for the deposit gate's price.
    total_bytes: u64,
    /// The serve tier the load-shed gate admits the stream under.
    class: RequestClass,
    /// The requesting peer's node id, the load-shed gate's per-client key.
    client_node_id: B256,
    /// The connection's slot for this unadmitted stream.
    slot: Option<UnadmittedSlot>,
}

impl DeferredAdmission {
    /// The admission an open that answered before admission runs at its first
    /// proof.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        req: StreamRequest,
        ext: StreamRequestExt,
        deployment: Deployment,
        verified_client: Option<Address>,
        lane_key: Option<LaneId>,
        peer: PublicKey,
        rate_per_mb: u64,
        total_bytes: u64,
        class: RequestClass,
        client_node_id: B256,
        slot: Option<UnadmittedSlot>,
    ) -> Self {
        Self {
            req: Box::new(req),
            ext,
            deployment,
            verified_client,
            lane_key,
            peer,
            rate_per_mb,
            total_bytes,
            class,
            client_node_id,
            slot,
        }
    }
}

/// A stream that passed admission: the lane it pays on and the checks that run
/// before each send.
pub(super) struct Admitted {
    /// The lane's key.
    pub(super) lane_key: LaneId,
    /// The lane.
    pub(super) lane: Arc<Mutex<LaneDeliveryState>>,
    /// The takedown, pool and signer checks that run before each send.
    pub(super) guards: SendGuards,
    /// The load-shed slot admission took, held for the rest of the stream, or
    /// `None` when the stream holds its slot elsewhere.
    pub(super) shed: Option<ShedSlot>,
    /// The blob pin admission took for the rest of the stream, or `None` when
    /// the stream holds its pin elsewhere. An open past the node's pin budget
    /// answered without one, so its pin starts here.
    pub(super) pin: Option<decdn_cache::InUse>,
}

/// Where a serve loop's stream stands on admission.
pub(super) enum Admission {
    /// Admitted before the response.
    Admitted(Admitted),
    /// Answered before admission. The serve loop admits at the first proof.
    OnFirstProof(DeferredAdmission),
}

/// A count of places under a fixed cap: one connection's streams that have
/// not passed admission (`decdn_protocol`'s
/// `MAX_UNADMITTED_STREAMS_PER_CONNECTION`), or the node's blob pins held by
/// such streams ([`MAX_UNADMITTED_PINS`](super::MAX_UNADMITTED_PINS)). A place
/// is held until the returned [`Place`] drops. The bound is exact: a burst of
/// takes cannot pass it.
#[derive(Debug)]
pub(super) struct Places {
    /// Live [`Place`]s.
    count: AtomicUsize,
    /// The most places held at once.
    cap: usize,
}

impl Places {
    /// No place held, under `cap`.
    pub(super) const fn new(cap: usize) -> Self {
        Self {
            count: AtomicUsize::new(0),
            cap,
        }
    }

    /// Take one place, held until the returned place drops, or `None` when
    /// the count is at its cap.
    pub(super) fn try_take(self: &Arc<Self>) -> Option<Place> {
        self.count
            .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.cap).then(|| count.saturating_add(1))
            })
            .ok()?;
        Some(Place {
            places: Arc::clone(self),
        })
    }
}

/// One place in a [`Places`] count. Dropping it frees the place.
#[derive(Debug)]
pub(super) struct Place {
    /// The count it holds a place in.
    places: Arc<Places>,
}

impl Drop for Place {
    fn drop(&mut self) {
        self.places.count.fetch_sub(1, Ordering::AcqRel);
    }
}

/// One unadmitted stream's place in its connection's count of such streams,
/// and the place its blob pin holds in the node's pin budget, if it took one.
/// The stream accepts with the first and takes the second when it pins its
/// blob. Both free when the stream is admitted or ends.
#[derive(Debug)]
pub(super) struct UnadmittedSlot {
    /// The stream's place among its connection's unadmitted streams.
    _stream: Place,
    /// The place its blob pin holds in the node's pin budget.
    pin: Option<Place>,
}

impl UnadmittedSlot {
    /// The slot of a stream that holds `stream`, with no pin yet.
    pub(super) const fn new(stream: Place) -> Self {
        Self {
            _stream: stream,
            pin: None,
        }
    }

    /// Count this stream's blob pin against the node's budget.
    pub(super) fn count_pin(&mut self, pin: Place) {
        self.pin = Some(pin);
    }

    /// Free the pin budget this stream holds: it holds no pin any more.
    pub(super) fn release_pin(&mut self) {
        self.pin = None;
    }
}

impl ClientHandler {
    /// The funding gates up to the deposit gate, for a request that names
    /// `lane_key` on the accepted pool `deployment` (ADR 003 § Pool solvency,
    /// § Capability delegation): the pool status, the signer's authorization,
    /// the funder deny, capability verification, a closing pool, then the
    /// grant's lane registration, the lane, and the expiry margin of the lane
    /// or, with no lane, of the grant. A refused request registers no lane.
    /// Returns what they read, or the refusal.
    ///
    /// With no pool view wired for `deployment` (dev/test) the pool gates pass
    /// and no lane registers from a capability. A chain read the gates make is
    /// charged to `peer`, the requester.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn funding_gates(
        &self,
        peer: &PublicKey,
        req: &StreamRequest,
        ext: &StreamRequestExt,
        deployment: Deployment,
        verified_client: Option<Address>,
        lane_key: Option<LaneId>,
        rate_per_mb: u64,
    ) -> Result<Funding, ServeRejectReason> {
        let pool_id = B256::from(req.pool_id);
        let pool_view = self.pool_view(deployment);
        // The pool's `getPool` view (owner, remaining, lifecycle), read once and
        // reused by every gate below. The wired view answers from its projection
        // and confirms a pool it has not folded with one `getPool`. `None` with a
        // view wired means the pool does not exist, is closed, or the read
        // faulted, so the node refuses rather than serve a pool it cannot
        // confirm.
        let pool_status = match pool_view {
            Some(view) => view.admit_status(pool_id, peer).await,
            None => None,
        };
        if pool_view.is_some() && pool_status.is_none() {
            return Err(ServeRejectReason::PoolUnconfirmed);
        }

        // Refuse a registered signer whose registration expired or whose
        // `cap - spent` cannot pay one chunk. A `getAuthorization` fault with no
        // earlier registered read refuses as unconfirmed, never as an exhausted
        // cap.
        if let (Some(view), Some(signer)) = (pool_view, verified_client) {
            let floor_micro = decdn_incentive::min_payment(CHUNK_BYTES, rate_per_mb);
            let auth = view.admit_signer_authorization(pool_id, signer, peer).await;
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
        // whose owner signature verifies against the pool owner authorizes a
        // lane. It runs only with a confirmed pool, so the owner is known. The
        // grant registers its lane only after the closing and expiry gates
        // below, so a refused request leaves no lane behind.
        let grant = match (verified_client, ext.capability.as_ref(), pool_status) {
            (Some(signer), Some(capability), Some(status)) => {
                self.verify_capability(deployment, pool_id, signer, status.owner, capability)
            }
            _ => None,
        };

        // A `Closing` pool takes no top-up, and its redemption ends at the
        // dispute deadline, so this node does not serve on it. A requester that
        // holds a lane, or presents a verified grant for one, hears `Unfunded`;
        // anyone else `NotFound`.
        if let Some(status) = pool_status
            && matches!(status.lifecycle, Lifecycle::Closing { .. })
        {
            let holds_lane = lane_key.is_some_and(|key| self.lanes.contains_key(&key));
            return Err(ServeRejectReason::PoolClosing {
                proven: grant.is_some() || holds_lane,
            });
        }

        // A grant inside the expiry margin cannot pay for a voucher, so it
        // registers no lane. The request still runs on a lane this node already
        // holds, under that lane's own expiry check below.
        let expiring_grant = grant
            .as_ref()
            .is_some_and(|lane| self.inside_expiry_margin(lane.expiry));
        if let Some(lane) = grant
            && !expiring_grant
        {
            self.register_grant(deployment, lane);
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

        // A lane whose capability is inside the expiry margin cannot pay for a
        // voucher, so admitting it would only stop at the first one. With no
        // lane, a request whose grant is inside the margin refuses the same way.
        match known_lane.as_ref() {
            Some(lane) => {
                let expiry = lane.lock().await.state.expiry;
                if self.inside_expiry_margin(expiry) {
                    return Err(ServeRejectReason::CapabilityExpired);
                }
            }
            None if expiring_grant => return Err(ServeRejectReason::CapabilityExpired),
            None => {}
        }
        Ok(Funding {
            pool_status,
            known_lane,
        })
    }

    /// Admit an open that answered before admission, once its payer's first
    /// proof is on the stream: the load-shed gate, the funding gates, then the
    /// lane the stream pays on, then the deposit gate. Returns the admitted
    /// lane, the stream's send checks and its load-shed slot, or the refusal.
    ///
    /// The load-shed gate runs first, so a loaded node reads no chain state
    /// for a stream it sheds. It refuses `LoadShedHit` or `LoadShedMiss` by the
    /// open's tier; both sign `NotFound`. A request with no verified binding
    /// names no lane and refuses `OwnerMismatch`; a bound request with no lane
    /// refuses `UnknownLane`. Both sign `NotFound`, so a requester cannot map
    /// lane existence.
    pub(super) async fn admit_on_first_proof(
        &self,
        mut adm: DeferredAdmission,
    ) -> Result<Admitted, ServeRejectReason> {
        // The payer's first proof admits the stream: it leaves its
        // connection's unadmitted streams, and its pin leaves the node's pin
        // budget. A paid stream keeps its blob pinned for its life.
        drop(adm.slot.take());
        let pin = self.cache.hold_in_use(Hash::from_bytes(adm.req.hash));
        let shed = match self.shed.try_admit(adm.class, adm.client_node_id) {
            Ok(slot) => slot,
            Err(reason) => {
                self.metrics.load_shed_refused(reason);
                tracing::debug!(
                    ?reason,
                    hash = %Hash::from_bytes(adm.req.hash),
                    class = ?adm.class,
                    "load-shed refusing a stream at admission"
                );
                return Err(match adm.class {
                    RequestClass::CacheHit => ServeRejectReason::LoadShedHit,
                    RequestClass::CacheMiss => ServeRejectReason::LoadShedMiss,
                });
            }
        };
        let funding = self
            .funding_gates(
                &adm.peer,
                &adm.req,
                &adm.ext,
                adm.deployment,
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
            shed: Some(shed),
            pin: Some(pin),
        })
    }

    /// Wait until the payer's first proof is buffered on `reader`, under the
    /// fixed first-proof deadline
    /// ([`await_first_proof`](super::proof_wait::await_first_proof)), which
    /// traffic on sibling streams does not extend. The proof stays buffered
    /// for the serve loop.
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
        Box::pin(super::proof_wait::await_first_proof(
            reader.fill_one(recv),
            conn,
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
