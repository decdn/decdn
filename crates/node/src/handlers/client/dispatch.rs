//! Connection accept loop + per-stream dispatch state machine.
//! See `mod.rs` for the struct + shared types.

use super::{
    APP_ERR_MALFORMED_MESSAGE, APP_ERR_NO_ERROR, APP_ERR_RATE_LIMITED, APP_IDLE_TIMEOUT, Address,
    Arc, B256, CHUNK_BYTES, CHUNK_GROUP_BYTES, CacheError, ClientHandler, Connection, FillOutcome,
    FirstMessage, FloorReservation, Hash, LaneKey, LaneSlot, Mutex, OwnedSemaphorePermit,
    REJECTION_CLOSE_TIMEOUT, RecvStream, RejectReason, Semaphore, SendStream, ServeRejectReason,
    StreamReadError, StreamResponseBody, U256, VarInt, read_first_message, reset_stream,
    verify_binding,
};
use futures_util::StreamExt as _;
use std::sync::atomic::Ordering;

impl ClientHandler {
    /// Accept the connection-level rate-limit permit, then serve each inbound
    /// bidi stream concurrently under a per-connection stream cap.
    ///
    /// Streams run as concurrent futures on this task (via `FuturesUnordered`)
    /// rather than `tokio::spawn`, because the iroh `ProtocolHandler::accept`
    /// signature borrows `&self` — spawned tasks would need a `'static` handle
    /// the trait does not hand us. Cooperative concurrency is sufficient for
    /// the I/O-bound delivery path.
    ///
    /// The loop also enforces the ADR 005 §Connection lifetime application-layer
    /// idle-close: once no stream is in flight, a connection with no new stream
    /// for [`APP_IDLE_TIMEOUT`] is closed (any activity re-arms the clock). This
    /// reclaims a peer that keeps the QUIC connection alive with keep-alive PINGs
    /// but sends no streams — which the transport idle timer never reaps.
    #[allow(clippy::cognitive_complexity)] // linear accept/select loop; splitting obscures it.
    pub(super) async fn serve(&self, conn: Connection) -> anyhow::Result<()> {
        let _permit = match self.limiter.acquire(&conn) {
            Ok(p) => p,
            Err(reason) => {
                conn.close(
                    VarInt::from_u32(APP_ERR_RATE_LIMITED),
                    reason.as_str().as_bytes(),
                );
                if reason != RejectReason::GlobalFull {
                    let _ = tokio::time::timeout(REJECTION_CLOSE_TIMEOUT, conn.closed()).await;
                }
                return Ok(());
            }
        };
        let _guard = self.metrics.connection_guard();

        let stream_sem = Arc::new(Semaphore::new(self.max_concurrent_streams));
        // Connection-scoped verified client binding (ADR 005 §Client identity
        // binding): the recovered Ethereum address is cached for the
        // connection's lifetime once a valid `BindNodeId` arrives.
        let bound_addr: Arc<Mutex<Option<Address>>> = Arc::new(Mutex::new(None));
        let client_node_id = B256::from(*conn.remote_id().as_bytes());

        let idle_timeout = self.idle_timeout.unwrap_or(APP_IDLE_TIMEOUT);
        let mut inflight = futures_util::stream::FuturesUnordered::new();
        loop {
            tokio::select! {
                biased;
                accepted = conn.accept_bi() => match accepted {
                    Ok((send, recv)) => {
                        let permit = Arc::clone(&stream_sem).try_acquire_owned().ok();
                        let bound = Arc::clone(&bound_addr);
                        inflight.push(self.serve_stream(send, recv, permit, bound, client_node_id));
                    }
                    // The connection closed (client done) or errored — stop
                    // accepting new streams. Not a handler fault.
                    Err(_) => break,
                },
                // ADR 005 §Connection lifetime: close the connection after
                // `idle_timeout` elapses with no stream — measured from the last
                // stream's close, or from accept if none ever opened.
                // Gated on `is_empty()` so an active stream keeps the connection
                // open; the fresh `sleep` per iteration re-arms on any activity,
                // so the clock counts from the last stream's close. `biased`
                // polls `accept_bi` first, so a stream arriving at the deadline
                // wins over the close.
                () = tokio::time::sleep(idle_timeout), if inflight.is_empty() => {
                    // A clean lifecycle close, not a fault — use the no-error
                    // code. Metered + logged so the reaper's firing rate (the
                    // streamless-keep-alive abuse pattern) is visible to operators.
                    self.metrics.client_idle_close();
                    tracing::debug!(?idle_timeout, "client connection idle-closed");
                    conn.close(VarInt::from_u32(APP_ERR_NO_ERROR), b"idle");
                    break;
                }
                Some(res) = inflight.next(), if !inflight.is_empty() => {
                    if let Err(e) = res {
                        self.log_stream_end(&e);
                    }
                }
            }
        }
        // Drain any streams still finishing after the connection closed.
        while let Some(res) = inflight.next().await {
            if let Err(e) = res {
                self.log_stream_end(&e);
            }
        }
        Ok(())
    }

    /// File one finished serve stream's error by who caused it.
    ///
    /// A node-side fault — an encode fault, an alignment error, a store fault, a
    /// framing fault — is the operator's only signal that a delivery was
    /// abandoned, since the client only ever sees a short stream. It logs at
    /// `error!` and bumps `decdn_serve_stream_node_fault_total`, so the rate is
    /// alertable rather than only greppable. A peer-attributable error
    /// ([`PeerFault`](super::wire::PeerFault)) or a client payment fault
    /// ([`ClientPaymentFault`](super::wire::ClientPaymentFault)) is routine and
    /// logs at `debug!`.
    ///
    /// `{e:#}` rather than `{e}`: the marker sits in the chain, so the alternate
    /// form is what prints the cause beside it.
    fn log_stream_end(&self, e: &anyhow::Error) {
        if super::wire::is_peer_attributable(e) {
            tracing::debug!(error = %format_args!("{e:#}"), "client stream ended with error");
        } else {
            self.metrics.serve_stream_node_fault();
            tracing::error!(
                error = %format_args!("{e:#}"),
                "client stream ended with a node-side fault"
            );
        }
    }

    /// Serve one delivery stream end to end.
    ///
    /// Kept as one linear, ADR-ordered sequence (read → bind → blob gate →
    /// channel → sign → deliver); splitting it would scatter the ADR-005
    /// ordering invariants across helpers — same rationale as the probe
    /// handler's `serve`.
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    pub(super) async fn serve_stream(
        &self,
        mut send: SendStream,
        mut recv: RecvStream,
        permit: Option<OwnedSemaphorePermit>,
        bound_addr: Arc<Mutex<Option<Address>>>,
        client_node_id: B256,
    ) -> anyhow::Result<()> {
        let first = match read_first_message(&mut recv).await {
            Ok(first) => first,
            Err(StreamReadError { err, app_code }) => {
                reset_stream(&mut send, &mut recv, app_code);
                // A request-read failure is peer-side (a timeout, a malformed
                // frame, a decode fault), not a node-side bug — mark it so the
                // dispatch sink logs it at `debug!` rather than `error!`. The
                // marker rides as context so `err` keeps its own chain.
                return Err(err.context(super::wire::PeerFault));
            }
        };

        // Stream-cap exhausted: reset the stream with no signed response.
        // Signing a `StreamResponse` per rejected request would let a request
        // flood amplify into CPU exhaustion (an ECDSA signature per reject) — the
        // cap exists to shed load, not to add work to the reject path.
        if permit.is_none() {
            reset_stream(&mut send, &mut recv, APP_ERR_RATE_LIMITED);
            return Ok(());
        }

        let FirstMessage::Delivery(req, ext) = first;
        let _stream_guard = self.metrics.inbound_stream_guard();

        // Verify an ephemeral client binding if present (ADR 005 §Client
        // identity binding) and remember the recovered address for the
        // connection's lifetime (a binding may be sent once and omitted on
        // later requests). A malformed binding is a client fault — reset.
        let mut verified_client: Option<Address> = *bound_addr.lock().await;
        if let Some(binding) = &ext.binding {
            let addr_bytes = binding.ethereum_address;
            match verify_binding(
                client_node_id,
                decdn_incentive::EPHEMERAL_BINDING_NONCE,
                &binding.binding_signature,
                &self.bind_domain,
            ) {
                Ok(recovered) if recovered.as_slice() == addr_bytes => {
                    verified_client = Some(recovered);
                    *bound_addr.lock().await = Some(recovered);
                }
                Ok(recovered) => {
                    tracing::warn!(
                        claimed = %Address::from(addr_bytes),
                        recovered = %recovered,
                        "client binding signature recovered a different address"
                    );
                    reset_stream(&mut send, &mut recv, APP_ERR_MALFORMED_MESSAGE);
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(error = %e, "client binding signature invalid");
                    reset_stream(&mut send, &mut recv, APP_ERR_MALFORMED_MESSAGE);
                    return Ok(());
                }
            }
        }

        let hash = Hash::from_bytes(req.hash);

        // The request's price, resolved ONCE and threaded to every refusal and to
        // the window tier. `clamped_rate` is side-effecting — it bumps
        // `rate_bounds_clamped` and warns when the configured rate sits below the
        // on-chain delivery floor — so calling it twice double-counts one request
        // (#1518). `respond_error` takes the rate as a required argument precisely
        // so that cannot happen: there is no way to sign a refusal without the
        // caller having priced the request exactly once.
        //
        // Deliberately BELOW the binding block. Every exit above this point — a
        // first-message read error, the stream-cap shed, a malformed binding —
        // returns without signing a `StreamResponse`, so none of them ever quotes
        // a rate, and pricing them would meter a clamp for a request that never
        // had a price.
        let rate_per_mb = self.clamped_rate();

        // Local-denylist gate (ADR 011 §Local Denylist, §On Blacklist Event
        // step 2: "reject any new StreamRequest for the hash immediately").
        //
        // This sits ABOVE the availability check on purpose. `is_evicted` below
        // is only consulted on the `Ok(false)` arm — it answers "we used to have
        // this" — so a denylisted hash the node still HOLDS would sail straight
        // past it into delivery. The denylist is a refusal to serve, not a
        // statement about what is in the store, so it must be answered before
        // the store is asked.
        //
        // Governance entries are gated here too, on their own set — the
        // blacklist watcher denies before it evicts. Two sets, ONE wire code:
        // ADR 011 §StreamRequest Response requires that a client cannot tell a
        // governance takedown from this operator's own denylist, and answering
        // the governance case from the eviction arm below (`EvictedSinceProbe`)
        // leaked exactly that — it made `HashBlacklisted` a unique fingerprint
        // for "this operator privately denied it", which is the probe the ADR
        // forecloses. The reasons stay distinct only so the operator's own
        // metrics can tell them apart, which no client can read.
        //
        // Governance is checked second because the local list is the cheaper and
        // far more common hit; both are one atomic load and a hash-set probe.
        if self.cache.is_denied(hash) {
            return self
                .respond_error(&mut send, &req, ServeRejectReason::HashDenied, rate_per_mb)
                .await;
        }
        if self.cache.is_chain_denied(hash) {
            return self
                .respond_error(
                    &mut send,
                    &req,
                    ServeRejectReason::ChainHashDenied,
                    rate_per_mb,
                )
                .await;
        }

        // Origin-only policy (#1759). When the operator opts out of foreign
        // relay, the own/foreign decision is backend-authoritative: the request's
        // `namespace_id` is a routing hint, not a trust anchor (ADR 002), so the
        // node asks its OWN backend — a memoized `HEAD`/`HeadObject` — whether it
        // holds the object named by this content hash. The probe is memoized
        // (short negative TTL), so a foreign-hash flood costs at most one
        // backend round-trip per hash per negative-TTL window. Above the
        // cache-hit branch on purpose: a foreign blob already sitting in this
        // node's cache (e.g. seeded by an earlier relay) is still declined, so
        // the policy is categorical rather than "foreign misses only".
        //
        // Three-way outcome (#1766): `Present` is own content and falls through
        // to the normal serve path. `Absent` (a genuine 404, or no origin
        // configured at all) is a real foreign-content answer, declined under
        // its own reason so the operator's metrics separate policy declines
        // from real cache misses. `Fault` (a transport error or a timed-out
        // `HEAD`) is NOT an absence — signing an authoritative `NotFound` would
        // tell a paying client this node's own content is gone and would hide
        // the operator's backend outage — so it surfaces as `InternalError`
        // instead, matching the store-fault handling on the `has()` path below.
        if !self.relay_foreign_namespaces {
            match self.cache.origin_probe_presence(hash).await {
                decdn_cache::OriginPresence::Present(_) => {}
                decdn_cache::OriginPresence::Absent => {
                    return self
                        .respond_error(
                            &mut send,
                            &req,
                            ServeRejectReason::ForeignNamespaceDeclined,
                            rate_per_mb,
                        )
                        .await;
                }
                decdn_cache::OriginPresence::Fault => {
                    return self
                        .respond_error(
                            &mut send,
                            &req,
                            ServeRejectReason::InternalError,
                            rate_per_mb,
                        )
                        .await;
                }
            }
        }

        // Resolve the lane key early. The seller keys a lane by
        // `(pool_id, bound_signer, this operator)` (brief §E1): `pool_id` from
        // the request, the signer from the verified client binding, the provider
        // from this node's own Ethereum identity. An unbound request cannot name
        // a lane, so it resolves to `None` — which every downstream gate treats
        // as "not my business" (it cannot make this node spend, because
        // `pull_authorized` refuses it before every fill tier).
        let self_operator = self.eth_signer.address();
        let lane_key = verified_client.map(|signer| LaneKey {
            pool_id: B256::from(req.pool_id),
            signer,
            provider: self_operator,
        });

        // Cached `getPool` view (owner + remaining), read ONCE and reused by the
        // funder gate here, the capability owner check, and the floor-`M` solvency
        // gates below. `None` — no pool-view wired, an unknown pool, or a read
        // fault — makes the fail-open gates fail open: a transient RPC blip must
        // not refuse paying clients, and the open-time hash gates plus the first
        // voucher's on-chain `redeem` still carry compliance and revenue.
        let pool_status = match self.pool_view.as_ref() {
            Some(view) => view.status(B256::from(req.pool_id)).await,
            None => None,
        };

        // Serve-path origin-blacklist gate (ADR 011 §On Blacklist Event): refuse a
        // pool whose FUNDER (`getPool.owner`) is on the operator's local
        // `denied_origins` or the on-chain origin blacklist. The funding address is
        // the pool OWNER from `getPool.owner`, threaded here via the cached
        // pool-view. The two questions are kept distinct — spend authority is a
        // signer question, compliance a funder question — so a blacklisted funder
        // is refused here even under a clean delegate signer.
        if let Some(status) = pool_status
            && self.content_deny.is_origin_denied(&status.owner)
        {
            return self
                .respond_error(
                    &mut send,
                    &req,
                    ServeRejectReason::OriginDenied,
                    rate_per_mb,
                )
                .await;
        }

        // Capability intake + lane registration (ADR 003 §Capability delegation).
        // A bound request whose extension carries a `capability` whose owner
        // signature verifies against the pool owner (a) registers the lane so the
        // voucher path serves `(pool_id, signer, this operator)` and (b) persists
        // the owner-signed grant for the redeemer. A forged-owner grant is dropped
        // — never registered, never persisted — so it cannot revert the redeemer's
        // `redeemMany` batch. Best-effort otherwise.
        if let (Some(signer), Some(capability)) = (verified_client, ext.capability.as_ref()) {
            self.intake_capability(
                B256::from(req.pool_id),
                signer,
                pool_status.map(|s| s.owner),
                capability,
            )
            .await;
        }

        // Resolve the live lane AFTER intake, so a lane just registered from this
        // request's own capability is visible to the spend + serve gates below.
        // The resolved `Arc` is KEPT rather than dropped: the cache-miss arm below
        // reuses the same lane, and re-resolving it would take the lane's shard
        // lock again for no reason.
        let known_lane = match lane_key {
            Some(key) => self.lanes.get(&key).map(|e| Arc::clone(e.value())),
            None => None,
        };

        // Per-lane concurrent-stream admission cap (#1697). Runs ONCE here, before
        // any serve-path branch, so every delivered stream — cache hit, backend-origin
        // miss, window pull-through miss, or buffered miss — is counted. Each same-lane
        // stream ALREADY in flight reserves one credit-window floor of pool headroom on
        // top of this stream's own per-path floor gate, so a lane cannot put more unpaid
        // downstream egress in flight than its refundable-floor headroom covers.
        //
        // A stream's window is FIXED at admission; this never re-divides a live stream's
        // share — it gates NEW admissions only. `n_active == 0` (the single-stream case)
        // applies NO surcharge, so a lone stream is admitted exactly as before the cap;
        // its own per-path floor gate is the only solvency check it faces.
        //
        // Checked-and-incremented under the lane lock so two simultaneous opens
        // serialize and neither admits into the same last slot (TOCTOU → N+1). Only
        // capability-bearing streams reach here with `known_lane` set — intake registers
        // the lane from this request's own capability above, so concurrent first-streams
        // on a fresh lane share one counter. `LaneSlot`'s drop releases the slot on every
        // exit (success, `?`, disconnect, panic).
        let mut lane_slot: Option<LaneSlot> = None;
        if let (Some(lane), Some(status)) = (known_lane.as_ref(), pool_status) {
            let floor = self.credit_window(CHUNK_BYTES, 0);
            let guard = lane.lock().await;
            let active = guard.active_streams.clone();
            let n_active = active.load(Ordering::Relaxed);
            let reserved = floor.saturating_mul(u64::from(n_active).saturating_add(1));
            if n_active > 0
                && !self.pool_remaining_covers_window(status.remaining, reserved, rate_per_mb)
            {
                drop(guard);
                let headroom = status
                    .remaining
                    .saturating_sub(self.pool_min_remaining_deposit);
                self.log_deposit_refusal(
                    B256::from(req.pool_id),
                    hash,
                    headroom,
                    decdn_incentive::min_payment(reserved, rate_per_mb),
                );
                return self
                    .respond_error(
                        &mut send,
                        &req,
                        ServeRejectReason::LaneAtCapacity,
                        rate_per_mb,
                    )
                    .await;
            }
            active.fetch_add(1, Ordering::Relaxed);
            drop(guard);
            lane_slot = Some(LaneSlot::new(active));
        }
        let _lane_slot = lane_slot;

        // Per-pool cumulative floor-credit admission reservation (ADR 003 §Pool
        // solvency, stateful-B). Unlike the per-lane #1697 cap above, it sums floor
        // credit across ALL distinct lanes on the pool and bounds it to
        // `remaining − M`, closing the fan-out hole where many distinct signers each
        // draw one un-vouchered floor on the same pool. The reservation is span-capped
        // to what THIS request can draw — at most one voucher-interval floor, less for
        // a bounded range or tail resume — and held for the stream's lifetime; the
        // `FloorReservation` guard reconciles it to the actual unpaid loss at stream
        // end.
        //
        // The guard is opened at the point the billed span is knowable, NOT here: an
        // open-ended tail (`byte_len == 0`, `byte_offset > 0`) only knows its span
        // once `total_bytes` is resolved, so reserving a full floor here would refuse
        // a tail resume the channel funded for its tail. The miss legs open it at the
        // pre-spend gate below (before any USDC fronting, so fan-out stays gated); the
        // direct-serve leg opens it at the floor-`M` gate, where `total_bytes` gives
        // the exact aligned span. Both use `try_reserve_floor`, whose budget check and
        // `live_reservation += reserved` increment run under ONE `pool_floor` lock, so
        // two concurrent admissions on a near-exhausted pool cannot both pass. A
        // refusal collapses to `InsufficientDeposit` → wire `NotFound`,
        // indistinguishable from any other miss (no balance leak).
        //
        // Held at fn scope so the reservation reconciles on EVERY exit via `Drop`.
        // Every serve path that reaches a serve loop MOVES it in and threads it through:
        // the direct-serve path hands it to `deliver`, and the
        // `serve_via_backend_origin` / `serve_via_window_pull_through` miss legs take it
        // by value and pass it by reference into their shared `serve_leg`. All three
        // note the live unpaid balance each iteration and release the reservation once
        // the stream repays one floor, so `Drop` reconciles to the actual unpaid loss —
        // the miss legs fold proportional `dead_charge` for the upstream USDC they front,
        // exactly like the hit path. On a refusal before the serve loop the guard drops
        // with `note_unpaid` at 0, so no dead charge is folded.
        let mut floor_reservation: Option<FloorReservation> = None;

        // Set by the origin-tier range pull-through below (#823) when a
        // bounded/offset cache-miss request was filled as a *partial* blob.
        // Carries the authoritative whole-blob size (from the origin size
        // probe) past the size gate, which can't `inspect` a partial blob.
        let mut range_pulled_size: Option<u64> = None;

        // Blob availability gate. A store fault is NOT an absence: `Ok(false)`
        // means the node genuinely lacks the blob (NotFound / EvictedSinceProbe),
        // but `Err` is a transient local store failure that must not masquerade
        // as a signed `NotFound` — a paying client would treat that as
        // authoritative and stop asking. Surface it as `InternalError` and log.
        // The initial `None` is unread on every live path (both `Ok` arms below
        // either shed and return or overwrite it, and `Err` returns too) — kept
        // anyway so the slot's declared type and its `Drop`-at-fn-scope binding
        // below read the same as the `lane_slot` admission guard above.
        #[allow(unused_assignments)]
        let mut shed_slot: Option<crate::load_shed::ShedSlot> = None;
        match self.cache.has(hash).await {
            Ok(true) => {
                match self
                    .shed
                    .try_admit(crate::load_shed::RequestClass::CacheHit, client_node_id)
                {
                    Ok(slot) => shed_slot = Some(slot),
                    Err(reason) => {
                        tracing::debug!(
                            ?reason,
                            %hash,
                            class = "hit",
                            "load-shed refusing new serve"
                        );
                        return self
                            .respond_error(
                                &mut send,
                                &req,
                                ServeRejectReason::LoadShedHit,
                                rate_per_mb,
                            )
                            .await;
                    }
                }
            }
            Ok(false) => {
                // Eviction is sticky and authoritative — never pull-fill a
                // hash an operator deliberately evicted (#279).
                if self.cache.is_evicted(hash) {
                    return self
                        .respond_error(
                            &mut send,
                            &req,
                            ServeRejectReason::EvictedSinceProbe,
                            rate_per_mb,
                        )
                        .await;
                }
                // The shed gate runs before the channel-ownership refusal below,
                // so an unbound / unknown-lane request can transiently hold a
                // `ShedSlot` until that refusal returns it. This is bounded by
                // the `ConnectionLimiter` global + per-source caps and is
                // self-limiting: once the node is pressured, further such
                // requests shed right here without acquiring a slot at all.
                // Keeping the gate here — ahead of channel-ownership and any
                // fill — preserves "shed before committing serve resources /
                // before any origin spend".
                match self
                    .shed
                    .try_admit(crate::load_shed::RequestClass::CacheMiss, client_node_id)
                {
                    Ok(slot) => shed_slot = Some(slot),
                    Err(reason) => {
                        tracing::debug!(
                            ?reason,
                            %hash,
                            class = "miss",
                            "load-shed refusing new serve"
                        );
                        return self
                            .respond_error(
                                &mut send,
                                &req,
                                ServeRejectReason::LoadShedMiss,
                                rate_per_mb,
                            )
                            .await;
                    }
                }
                // Node-to-node cache-miss pull-through (#831). Fronting upstream
                // USDC egress is privileged: gate it on the request PROVING
                // ownership of the named channel — a verified client binding
                // (`verified_client`) whose address is the channel's authorized
                // client. Channel *existence* cannot gate spend (channel ids are
                // public on-chain via `ChannelOpened`, so any leech could name
                // one); only proven ownership can. An unbound request, or one
                // for a channel it does not own, gets a plain `NotFound` and
                // cannot make this node spend — closing the proxy-abuse /
                // griefing vector where an unpaid client drains the buyer
                // deposit. (Multi-hop node→node pulls therefore require the
                // downstream requester to send a binding; both the direct-client
                // `decdn fetch` (#1115) and the node→node requester
                // (`node_origin`, #1117) now do, so chained pull-through works.)
                // On a successful fill, fall through to the normal size-gate +
                // delivery path; otherwise it stays a `NotFound`.
                //
                // Origin-tier range pull-through (#823, ADR 037 §Origin-tier
                // pull-through). When the
                // request is a bounded/offset range, scope the cache-miss origin
                // fetch to exactly the requested span (fetch `[offset, offset+len)`
                // + the `{H}.obao4` outboard, bao-verify, import a partial blob)
                // instead of pulling the whole blob to serve a slice. Gated on
                // the same pull-authorization as the whole-blob fill. Best-effort:
                // any decline (unknown origin size, no published outboard, no
                // `Range` support, verify failure) leaves `range_pulled_size` as
                // `None` and falls through to the whole-blob path below, which is
                // always correct (ADR 037 §"Fallback is always correct").
                // The fault latch (#1129). Declared BEFORE the range tier, not after
                // it: the range pull can hit a `CacheError::Store` of its own, and a
                // latch that only starts at the local tier would drop it. Today the
                // local tier happens to re-detect such a fault (it re-walks the same
                // origin chain), but that is a coincidence of the current tier
                // ordering, not an invariant — and this is the one bug the file
                // exists to prevent. Latch every tier.
                // Pre-spend deposit floor (#1519). Every fill tier below spends:
                // the range and local tiers front the operator's own origin
                // egress, and the buffered tier's `cache.populate` walks the paid
                // `Peer` origin and fronts real upstream USDC. (The range tier is
                // own-egress-only because `NodeOrigin` does not implement
                // `Origin::fetch_range` — `pull_through_range` iterates every
                // origin with no `local_only` filter, so the day it does, that tier
                // starts fronting upstream USDC too. Nothing would fail.) All three are gated
                // on channel OWNERSHIP (`pull_authorized`) and none on solvency,
                // so before this floor a dust-deposit channel could name N absent
                // hashes, make the node pay for each, and be refused afterwards by
                // the serve-path gate — the attacker gains nothing, but the
                // operator still pays. Refuse here instead, before any of it.
                //
                // The floor is one credit window, CAPPED BY THE REQUEST'S SPAN
                // when the request bounds itself. A bounded range carries
                // `byte_len`, so its billed size is knowable without `total_bytes`
                // (align it out to chunk groups exactly as the serve path does —
                // `export_bao_range_stream` serves the aligned superset). Pricing
                // such a request at a whole window would refuse a client that can
                // comfortably pay for the range it asked for, and it would do so
                // ONLY on a cache miss — the serve gate prices the same request at
                // the aligned span — so the same request would be served warm and
                // refused cold. Worse, `decdn fetch` reads a refused resume as a
                // possibly-stale partial, rewinds to zero and re-pays for the whole
                // blob, so mispricing a bounded request doubles a user's bill.
                //
                // For an UNBOUNDED request (`byte_len == 0`: whole blob, or a tail
                // from an offset) the billed size genuinely is unknowable pre-fill,
                // so the window stands. Be clear about the residual that leaves:
                // this guard prices at `paid = 0`, i.e. the ramp floor — one
                // chunk (`CHUNK_BYTES`, a fixed 1 MiB) — not the fully-ramped
                // `credit_max` ceiling (64 MiB by default),
                // since a cold request has confirmed no payment yet. A channel
                // funded for the blob but not for a floor chunk is refused
                // cold and served warm. Closing that needs the origin size probe
                // to run before the floor, which is a larger change than this one.
                //
                // `window.rs` keeps its own guard. Its window is exactly
                // `self.credit_window(chunk_bytes, 0)` — the same ramp-floor
                // computation this site uses — so the two guards are redundant at
                // this floor. It is also the tier that fronts UPSTREAM spend (the
                // pull leg's `RampPacer`, #1669, paces against the SAME ramp as it
                // pays). Do not delete it on the strength of this floor alone.
                //
                // Pre-spend floor reservation (shared-payment-pool model). Open the
                // per-pool `FloorReservation` HERE, before any fill tier fronts USDC,
                // so `remaining − M` must cover this pool's already-committed floor
                // credit plus this stream's floor before the node spends: see
                // [`ClientHandler::try_reserve_floor`]. The reserved amount is one
                // interval (the ramp floor at `paid = 0`), capped by the request's own
                // aligned span when it bounds itself. A bounded range carries its
                // `byte_len`, so its span is knowable without `total_bytes`; an
                // open-ended request (`byte_len == 0`, whole blob or tail) reserves the
                // full floor — the miss path cannot resolve a tail's span pre-fill, so
                // an unbounded tail is refused cold and served warm through the
                // direct-serve gate, which does know `total_bytes`.
                //
                // `remaining` comes from the cached `getPool` view resolved above;
                // a `None` view fails open (the on-chain `redeem` is the backstop).
                // Skipped for an unknown lane — `pull_authorized` refuses those
                // before every tier, so no spend happens there anyway.
                if known_lane.is_some()
                    && let Some(status) = pool_status
                {
                    // Reserve the un-self-funded credit this stream fronts before it
                    // pays: the ramp floor at `paid = 0` (one chunk normally, the
                    // full `credit_max` when `credit_ramp_divisor == 0`), span-capped
                    // for a bounded request. `release_live_repaid` frees it once
                    // cumulative payment REACHES this reserved amount (see the serve
                    // loop), so release stays matched to what was reserved at any divisor.
                    let window = self.credit_window(CHUNK_BYTES, 0);
                    let reserved_bytes = if req.byte_len > 0 {
                        aligned_span(req.byte_offset, req.byte_len, u64::MAX).min(window)
                    } else {
                        window
                    };
                    let reserved = decdn_incentive::min_payment(reserved_bytes, rate_per_mb);
                    let pool_id = B256::from(req.pool_id);
                    match self.try_reserve_floor(pool_id, status.remaining, reserved) {
                        None => {
                            let headroom = status
                                .remaining
                                .saturating_sub(self.pool_min_remaining_deposit);
                            self.log_deposit_refusal(pool_id, hash, headroom, reserved);
                            return self
                                .respond_error(
                                    &mut send,
                                    &req,
                                    ServeRejectReason::InsufficientDeposit,
                                    rate_per_mb,
                                )
                                .await;
                        }
                        Some(guard) => floor_reservation = Some(guard),
                    }
                }

                let mut fault_seen = false;
                if (req.byte_offset > 0 || req.byte_len > 0)
                    && self.pull_authorized(&req, verified_client)
                {
                    let (size, range_outcome) = self.try_range_pull_through(hash, &req).await;
                    range_pulled_size = size;
                    fault_seen |= range_outcome.is_fault();
                }

                let mut locally_filled = false;

                // Own-origin serve-miss via the two decoupled legs.
                // When the node's OWN configured fs/http/s3 origin can prove it
                // serves `hash` — it knows the size AND publishes the {H}.obao4
                // outboard — serve the whole blob by running the local pull leg (fill
                // the cache from origin) beside the serve leg (stream the filling
                // cache to the paying client), exactly like the node→node window path
                // but with NO upstream, NO channel, and NO payment on the ingest side.
                // Time-to-first-byte does not wait for the whole blob to land.
                //
                // Whole-blob only (offset==0 && len==0): ranged/resumed own-origin
                // serve-miss is not yet wired through the two-leg spine, so a bounded
                // request never routes here.
                //
                // Serviceability is confirmed by `origin_size` +
                // `origin_fetch_outboard_bytes` (an origin publishes the outboard) —
                // NOT by proving a Range/206 `fetch_range` works. All three shipped
                // adapters (fs/http/s3) support Range whenever they publish an
                // outboard, so this holds in practice; a custom Origin that publishes
                // an outboard but refuses Range would sign `ok:true` then fail the
                // stream. Acceptable for the shipped backends within this path's scope.
                //
                // Best-effort degrade (ADR 037 §"Fallback is always correct"): no
                // published outboard / no origin size / no origins => fall through to
                // `try_local_populate` below.
                // Once serviceable, `serve_via_backend_origin` claims the fill itself
                // (`CacheEngine::claim_fill`): the first same-hash miss OWNS
                // the local origin pull; a concurrent one ATTACHES as an observer and
                // streams the same filling cache to its own client (no double origin
                // egress). The registry is range-aware, so this coalescing is not
                // limited to the whole-blob case.
                if range_pulled_size.is_none()
                    && !locally_filled
                    && req.byte_offset == 0
                    && req.byte_len == 0
                    && self.pull_authorized(&req, verified_client)
                {
                    match self.cache.origin_size(hash).await {
                        Ok(Some(total)) => {
                            match self.cache.origin_fetch_outboard_bytes(hash, total).await {
                                // Serviceable: size known and an origin publishes the
                                // outboard. Enter the orchestration directly — it claims
                                // the fill (owner-or-attach) internally after signing the
                                // response, so no coalescing decision happens here.
                                Ok(Some(_)) => {
                                    // Boxed: the serve future is large
                                    // (clippy::large_futures). `pull_authorized`
                                    // (checked in the `if` above) guarantees a
                                    // lane, so the extraction always matches.
                                    if let (Some(lk), Some(ln)) = (lane_key, known_lane.as_ref()) {
                                        return Box::pin(self.serve_via_backend_origin(
                                            send,
                                            recv,
                                            &req,
                                            hash,
                                            client_node_id,
                                            lk,
                                            ln,
                                            total,
                                            pool_status.map(|s| s.remaining),
                                            rate_per_mb,
                                            floor_reservation,
                                        ))
                                        .await;
                                    }
                                }
                                // Size known but no published outboard — not
                                // serviceable via the range encoder. Degrade to the
                                // buffered local populate below.
                                Ok(None) => {}
                                // A genuine origin transport fault while fetching the
                                // outboard. Latch it (#1129) so a later-tier miss
                                // reports InternalError not NotFound, then fall
                                // through — another source may still serve.
                                Err(e) => {
                                    tracing::debug!(%hash, error = %e, "own-origin outboard probe faulted; falling through");
                                    fault_seen = true;
                                }
                            }
                        }
                        // No origin knows the size, or no origin is configured at
                        // all — both a clean fall-through (degrade). `origin_size`
                        // already returns Ok(None) for most declines, so only
                        // NoOrigin and transport faults reach the Err arms.
                        Ok(None) | Err(CacheError::NoOrigin { .. }) => {}
                        // Any other origin fault latches `fault_seen` (#1129) so a
                        // later-tier miss reports InternalError not NotFound.
                        Err(e) => {
                            tracing::debug!(%hash, error = %e, "own-origin size probe faulted; falling through");
                            fault_seen = true;
                        }
                    }
                }

                // Reactive LOCAL-origin populate (#1116). Before any node→node
                // path, try to fill from the node's OWN configured fs/http/s3
                // origin (`populate_local` never touches the paid `Peer` origin).
                // This lets a cache-only operator (node→node disabled) reactively
                // serve its own content, and — when node→node IS enabled — prefers
                // the local origin over the paid peer window path for a whole-blob
                // request the operator can satisfy itself. Gated on the SAME proven
                // channel ownership as the paid paths (`pull_authorized`): an S3
                // origin has egress cost, and the following delivery is billed
                // per-voucher. A local miss leaves the blob absent and falls through
                // to the node→node branches below, unchanged.
                //
                // A local HARD FAULT (#1129 — the operator's own S3/fs origin
                // errored, rather than simply not having the blob) also falls
                // through to the node→node branches: another source may legitimately
                // still serve. But it is LATCHED in `fault_seen`, because if no later
                // tier fills, the terminal refusal must report a degraded node
                // (`InternalError`) rather than an empty one (`NotFound`). Every
                // terminal MISS below therefore goes through
                // `FillOutcome::miss_reason` — including the window path's leech
                // shed. (The channel-class refusals — `UnknownChannel`,
                // `InsufficientDeposit` — keep their own reasons: they are
                // client-attributable and would refuse regardless of origin
                // health.)
                if range_pulled_size.is_none()
                    && !locally_filled
                    && let Some(timeout) = self.local_populate
                    && self.pull_authorized(&req, verified_client)
                {
                    let local = self.try_local_populate(hash, timeout).await;
                    fault_seen |= local.is_fault();
                    locally_filled = local.is_filled();
                }

                // Window-paced pull-through (#856, ADR 037) is the preferred path
                // when its provider is set: instead of buffering the whole
                // blob via `populate` and only THEN serving (fronting 100% of the
                // upstream cost before any downstream voucher), it runs the pull
                // leg (fill the cache from upstream) beside the serve leg (stream
                // the filling cache to the paying client), so the per-request
                // speculative exposure is bounded to the ramped credit window
                // (#1669).
                //
                // It requires `byte_offset == 0 && byte_len == 0` (a whole-blob
                // request). This is a conservative constraint on the ROUTING, not a
                // limit of the serve leg: `serve_leg` clamps delivery to
                // `[offset, offset + len)` and bills only the wire it delivers, and
                // the pull leg is range-minimized (it pulls only
                // `missing_ranges(offset, len)`), so the two-leg spine is
                // range-correct. Ranged and resumed serve-miss through that spine is
                // simply not yet wired end-to-end, so a bounded or resumed request
                // falls to the buffered path below, which serves exactly the
                // requested span via `export_range` (#823).
                if range_pulled_size.is_some() || locally_filled {
                    // The requested span/blob is already present — a verified
                    // partial blob from the range pull, or the whole blob just
                    // filled from a local origin (#1116). Skip the node→node fill
                    // and fall through to the size gate + delivery (which serves a
                    // partial via `export_range`).
                } else if let Some(origin) = self.pull_through_origin.as_ref()
                    && req.byte_offset == 0
                    && req.byte_len == 0
                    && self.pull_authorized(&req, verified_client)
                {
                    // Boxed: the serve future is large; keep it off the
                    // `serve_stream` stack frame (clippy::large_futures). The
                    // orchestration claims the fill (owner-or-attach) internally after
                    // signing the response, so two concurrent same-hash misses share one
                    // upstream pull (no double spend, #305) without a decision here.
                    // `pull_authorized` (the `if` above) guarantees a lane, so the
                    // extraction always matches.
                    if let (Some(lk), Some(ln)) = (lane_key, known_lane.as_ref()) {
                        return Box::pin(self.serve_via_window_pull_through(
                            send,
                            recv,
                            &req,
                            hash,
                            client_node_id,
                            lk,
                            ln,
                            Arc::clone(origin),
                            pool_status.map(|s| s.remaining),
                            fault_seen,
                            rate_per_mb,
                            floor_reservation,
                        ))
                        .await;
                    }
                } else {
                    // Buffered pull-through (#831): used when
                    // the window provider is unset or for a resumed request.
                    let buffered = match self.pull_through {
                        Some(timeout) if self.pull_authorized(&req, verified_client) => {
                            self.try_pull_through(hash, timeout).await
                        }
                        // No pull-through configured, or the request is not
                        // authorized to make this node spend: nothing was attempted,
                        // so this tier contributes no new information.
                        _ => FillOutcome::CleanMiss,
                    };
                    if !buffered.is_filled() {
                        let reason = FillOutcome::miss_reason(fault_seen || buffered.is_fault());
                        return self
                            .respond_error(&mut send, &req, reason, rate_per_mb)
                            .await;
                    }
                }
            }
            Err(e) => {
                tracing::warn!(%hash, error = %e, "cache `has` lookup failed on delivery path");
                return self
                    .respond_error(
                        &mut send,
                        &req,
                        ServeRejectReason::InternalError,
                        rate_per_mb,
                    )
                    .await;
            }
        }
        // Held at fn scope so the slot lives across `deliver` / `serve_via_*` and
        // releases its admission counters on every exit, including the boxed
        // miss-serve return paths below.
        let _shed_slot = shed_slot;

        // Size gate. An origin-tier range pull (#823) imported only a *partial*
        // blob, so `inspect`/`has` can't report the whole-blob size — but the
        // origin size probe already gave us the authoritative total, which the
        // client needs for resume math. Use it directly in that case. Otherwise
        // `has` just confirmed the blob is present and complete, so an `inspect`
        // error — or a `None` size (a `Partial`/`NotFound` status) — is a real
        // store fault, NOT a zero-length blob. Advertising `total_bytes: 0` for
        // a non-empty blob would sign a `StreamResponse` the delivery then
        // contradicts, and the receiver (expecting 0 bytes) would abort on the
        // first chunk. Surface the fault instead; only a genuinely complete,
        // zero-length blob yields `total_bytes == 0`.
        let total_bytes = if let Some(total) = range_pulled_size {
            total
        } else {
            let size = match self.cache.inspect(hash).await {
                Ok(preview) => preview.size_bytes,
                Err(e) => {
                    tracing::warn!(%hash, error = %e, "cache `inspect` failed on delivery path");
                    return self
                        .respond_error(
                            &mut send,
                            &req,
                            ServeRejectReason::InternalError,
                            rate_per_mb,
                        )
                        .await;
                }
            };
            let Some(total_bytes) = size else {
                tracing::warn!(
                    %hash,
                    "blob present per `has` but `inspect` reports no size; treating as fault"
                );
                return self
                    .respond_error(
                        &mut send,
                        &req,
                        ServeRejectReason::InternalError,
                        rate_per_mb,
                    )
                    .await;
            };
            total_bytes
        };
        if self.max_blob_size_bytes > 0 && total_bytes > self.max_blob_size_bytes {
            return self
                .respond_error(
                    &mut send,
                    &req,
                    ServeRejectReason::BlobTooLarge,
                    rate_per_mb,
                )
                .await;
        }

        // Bounded-range bounds check (ADR 005 §Bounded byte ranges). Reject an
        // out-of-bounds range with a `StreamError` *before* signing the success
        // response below — otherwise the client accepts a signed `ok: true` that
        // `deliver`'s `export_range` then aborts mid-stream. A whole-blob request
        // (`byte_offset == 0 && byte_len == 0`) is always in bounds for a present
        // blob; `byte_len == 0` on a non-zero offset is the in-bounds whole-tail
        // read. Mirrors `range_pull::align_range`'s bound check on the origin
        // tier so the serve tier rejects the same ranges.
        if req.byte_offset > 0 || req.byte_len > 0 {
            let out_of_bounds = req.byte_offset >= total_bytes
                || (req.byte_len > 0
                    && req
                        .byte_offset
                        .checked_add(req.byte_len)
                        .is_none_or(|end| end > total_bytes));
            if out_of_bounds {
                return self
                    .respond_error(
                        &mut send,
                        &req,
                        ServeRejectReason::RangeNotSatisfiable,
                        rate_per_mb,
                    )
                    .await;
            }
        }

        // Resolve the lane (must be pre-persisted — see module docs / #327).
        // The lane is keyed by `(pool_id, bound_signer, this operator)`; a
        // request with no verified binding cannot name a lane, and a bound client
        // whose signer has no lane for this pool resolves to `None`. Both are
        // refused pre-serve as an unknown lane: a binding that does not match the
        // lane's signer resolves to no lane. The mid-stream reason cannot ride in
        // the initial `StreamResponse`, so use the delivery-side `NotFound` here
        // (avoids leaking lane existence).
        let Some(lane_key) = lane_key else {
            tracing::warn!("stream request with no verified binding; refusing pre-serve");
            return self
                .respond_error(
                    &mut send,
                    &req,
                    ServeRejectReason::OwnerMismatch,
                    rate_per_mb,
                )
                .await;
        };
        let Some(lane) = known_lane else {
            tracing::warn!(
                ?lane_key,
                "stream request on unknown lane; refusing pre-serve"
            );
            return self
                .respond_error(
                    &mut send,
                    &req,
                    ServeRejectReason::UnknownChannel,
                    rate_per_mb,
                )
                .await;
        };

        // Pre-flight floor gate — the direct-serve twin of the pull-through guard
        // in `window.rs` (keep the two in step). Without it the node signs
        // `ok: true` and streams a full interval before the first voucher's
        // pool-solvency check can fire, so a pool that cannot cover even that
        // first interval gets it free on every request (#1516).
        //
        // `guard_bytes` is the chunk-group-aligned span (what `export_bao_range_stream`
        // bills), capped by the credit-window floor at `paid = 0` — the ramp has not
        // started yet on a fresh request. Here `total_bytes` is known, so it is the
        // EXACT billed span, including for an open-ended tail (`byte_len == 0`,
        // `byte_offset > 0`) the miss gate could only price at the full floor.
        //
        // A direct-serve HIT reaches this gate with no reservation yet (the miss
        // legs, which spend, open theirs pre-fill above). Open it HERE, span-capped
        // to `guard_bytes`, via [`ClientHandler::try_reserve_floor`] — its
        // `remaining − M ≥ committed + reserved` check both admits the stream and
        // bounds the pool's cumulative cross-lane floor credit. A miss-fill stream
        // already holds its reservation, so re-validate solvency against the pool's
        // already-committed floor credit (`live_reservation + dead_charge`) via
        // [`ClientHandler::pool_budget_covers_reserve`] with `new_reserve = 0` — the
        // same stateful check the mid-stream gate applies, so a `dead_charge` that
        // grew since the reservation refuses here rather than serving a free interval.
        //
        // `remaining` comes from the cached `getPool` view resolved above; a `None`
        // view fails open (the on-chain `redeem` is the backstop). Either way, refuse
        // `InsufficientDeposit` when the pool can no longer cover the span-capped
        // floor.
        // Reserve the ramp-floor credit exposure (`credit_window` at `paid = 0`),
        // span-capped by the request. `release_live_repaid` frees it once payment
        // reaches this reserved amount, so release matches reserved at any divisor.
        let chunk_bytes = CHUNK_BYTES;
        let guard_bytes = aligned_span(req.byte_offset, req.byte_len, total_bytes)
            .min(self.credit_window(chunk_bytes, 0));
        if let Some(status) = pool_status {
            let refused = if floor_reservation.is_none() {
                let reserved = decdn_incentive::min_payment(guard_bytes, rate_per_mb);
                match self.try_reserve_floor(B256::from(req.pool_id), status.remaining, reserved) {
                    Some(guard) => {
                        floor_reservation = Some(guard);
                        false
                    }
                    None => true,
                }
            } else {
                !self.pool_budget_covers_reserve(
                    B256::from(req.pool_id),
                    status.remaining,
                    U256::ZERO,
                )
            };
            if refused {
                let headroom = status
                    .remaining
                    .saturating_sub(self.pool_min_remaining_deposit);
                self.log_deposit_refusal(
                    B256::from(req.pool_id),
                    hash,
                    headroom,
                    decdn_incentive::min_payment(guard_bytes, rate_per_mb),
                );
                return self
                    .respond_error(
                        &mut send,
                        &req,
                        ServeRejectReason::InsufficientDeposit,
                        rate_per_mb,
                    )
                    .await;
            }
        }

        // Build and sign the success response.
        let body = StreamResponseBody {
            hash: req.hash,
            ok: true,
            rate_per_mb,
            total_bytes,
            pool_id: req.pool_id,
            timestamp_us: req.timestamp_us,
            redirect: None,
        };
        let (resp, resp_ext) = self.sign_response(body, None)?;
        self.write_stream_response(&mut send, &resp, &resp_ext)
            .await?;

        // Stream the blob, collecting vouchers at each interval boundary.
        self.deliver(
            &mut send,
            &mut recv,
            hash,
            req.byte_offset,
            req.byte_len,
            total_bytes,
            lane_key,
            Some(&lane),
            client_node_id,
            rate_per_mb,
            floor_reservation,
        )
        .await
    }
}

/// Content bytes a `(byte_offset, byte_len)` request will actually be BILLED for,
/// i.e. the chunk-group-aligned superset `export_bao_range_stream` serves.
///
/// Mirrors `decdn_bao_range::align_range`: the start floors to its group
/// boundary, the end ceils to one and clamps to the blob. The serve path does not
/// trim back to `byte_offset` (trimming would break bao verification — the
/// receiver discards the leading bytes itself), so the payer covers the whole
/// aligned range. A pre-flight price computed on the *requested* span would
/// under-reserve by up to two groups.
///
/// `byte_len == 0` means "to the end of the blob" (whole blob when `byte_offset`
/// is also 0). Total-saturating throughout: the caller's range bounds check has
/// already rejected an out-of-bounds request, and a zero-length blob correctly
/// yields 0.
fn aligned_span(byte_offset: u64, byte_len: u64, total_bytes: u64) -> u64 {
    let end = if byte_len > 0 {
        byte_offset.saturating_add(byte_len).min(total_bytes)
    } else {
        total_bytes
    };
    let start = (byte_offset / CHUNK_GROUP_BYTES).saturating_mul(CHUNK_GROUP_BYTES);
    let aligned_end = end
        .div_ceil(CHUNK_GROUP_BYTES)
        .saturating_mul(CHUNK_GROUP_BYTES)
        .min(total_bytes);
    aligned_end.saturating_sub(start)
}

#[cfg(test)]
mod aligned_span_tests {
    use super::{CHUNK_GROUP_BYTES, aligned_span};

    const G: u64 = CHUNK_GROUP_BYTES;

    #[test]
    fn whole_blob_is_the_blob() {
        assert_eq!(aligned_span(0, 0, 5 * G), 5 * G);
        // A partial final group is clamped to the blob, not rounded past it.
        assert_eq!(aligned_span(0, 0, 5 * G + 1), 5 * G + 1);
    }

    #[test]
    fn zero_length_blob_prices_nothing() {
        // #1054: must yield a zero ceiling so an empty blob still serves.
        assert_eq!(aligned_span(0, 0, 0), 0);
    }

    #[test]
    fn a_tiny_range_is_priced_as_the_group_it_touches() {
        // The regression this helper exists for: pricing `byte_len` directly
        // would reserve 2 bytes for a request that bills a full 16 KiB group.
        assert_eq!(aligned_span(0, 2, 10 * G), G);
        assert_eq!(aligned_span(1, 1, 10 * G), G);
    }

    #[test]
    fn a_range_straddling_a_boundary_pays_both_groups() {
        // Worst case: one byte either side of a boundary spans two whole groups.
        assert_eq!(aligned_span(G - 1, 2, 10 * G), 2 * G);
    }

    #[test]
    fn an_already_aligned_range_gains_nothing() {
        assert_eq!(aligned_span(G, G, 10 * G), G);
        assert_eq!(aligned_span(2 * G, 3 * G, 10 * G), 3 * G);
    }

    #[test]
    fn a_whole_tail_runs_from_its_group_start_to_the_blob_end() {
        // `byte_len == 0` with a non-zero offset is the resume shape.
        assert_eq!(aligned_span(3 * G, 0, 10 * G), 7 * G);
        // Mid-group offset floors back to the group start.
        assert_eq!(aligned_span(3 * G + 5, 0, 10 * G), 7 * G);
    }

    #[test]
    fn a_range_past_the_blob_end_clamps_to_the_blob() {
        // The caller's bounds check rejects these, but the helper must not
        // over-price if it is ever reached with a partial final group.
        assert_eq!(aligned_span(0, u64::MAX, 3 * G + 7), 3 * G + 7);
    }
}
