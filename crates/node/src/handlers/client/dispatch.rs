//! Connection accept loop + per-stream dispatch state machine.
//! See `mod.rs` for the struct + shared types.

use super::{
    APP_ERR_MALFORMED_MESSAGE, APP_ERR_NO_ERROR, APP_ERR_RATE_LIMITED, APP_IDLE_TIMEOUT, Address,
    Arc, B256, CHUNK_BYTES, CHUNK_GROUP_BYTES, CacheError, ClientHandler, Connection, FillOutcome,
    FirstMessage, FloorRefusal, FloorRefusalSite, FloorReservation, Hash, LaneKey, LaneSlot,
    OwnedSemaphorePermit, PublicKey, REJECTION_CLOSE_TIMEOUT, RecvStream, RejectReason, Semaphore,
    SendStream, ServeRejectReason, StreamReadError, StreamRequest, StreamResponseBody, U256,
    VarInt, read_first_message, reset_stream, verify_binding,
};
use arc_swap::ArcSwapOption;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use tokio::task::{JoinError, JoinSet};

use futures_util::FutureExt as _;
use tracing::Instrument as _;

use super::outcome::{ErrEnd, ResetCause, ServeEnd};
use crate::load_shed::RequestClass;
use crate::metrics::FirstByteClock;

/// The root span for one inbound serve stream.
///
/// The request fields (`hash`, `pool_id`, `byte_offset`, `byte_len`) are
/// recorded once the client binding verifies, so a stream reset before that has
/// none. `outcome` / `reason` / `bytes` are recorded once the stream ends
/// ([`ServeEnd::record`]), or `outcome` / `error` when it ends on an error.
/// `peer` and `local_node_id` render as lowercase-hex iroh ids, the same as the
/// requester's `open_progressive_pull` span records them, so one trace query
/// joins the two sides of a transfer on `hash`, `pool_id`, `byte_offset` and
/// the swapped ids.
pub(super) fn serve_stream_span(peer: PublicKey, local_node_id: PublicKey) -> tracing::Span {
    tracing::info_span!(
        "serve_stream",
        otel.kind = "server",
        otel.status_code = tracing::field::Empty,
        direction = "inbound",
        %peer,
        %local_node_id,
        hash = tracing::field::Empty,
        pool_id = tracing::field::Empty,
        byte_offset = tracing::field::Empty,
        byte_len = tracing::field::Empty,
        outcome = tracing::field::Empty,
        reason = tracing::field::Empty,
        error = tracing::field::Empty,
        bytes = tracing::field::Empty,
    )
}

/// Records `outcome = "cancelled"` on a serve stream's span when its task is
/// dropped before the stream ends — an abort on shutdown, while the task waits
/// on the client. Disarmed (set to `None`) once the stream ends, so the span's
/// outcome is still recorded exactly once.
struct CancelledMark(Option<tracing::Span>);

impl Drop for CancelledMark {
    fn drop(&mut self) {
        if let Some(span) = self.0.take() {
            span.record("outcome", "cancelled");
        }
    }
}

/// Record the request fields of a [`serve_stream_span`].
pub(super) fn record_request(span: &tracing::Span, req: &StreamRequest) {
    span.record("hash", tracing::field::display(Hash::from_bytes(req.hash)));
    span.record("pool_id", tracing::field::display(B256::from(req.pool_id)));
    span.record("byte_offset", req.byte_offset);
    span.record("byte_len", req.byte_len);
}

impl ClientHandler {
    /// Accept the connection-level rate-limit permit, then serve each inbound
    /// bidi stream on its OWN spawned task under a per-connection stream cap.
    ///
    /// Each stream runs as a `tokio::spawn`ed task (via a [`JoinSet`]) rather
    /// than as a cooperative future on this accept task, so every synchronous
    /// per-stream CPU cost — the two open-time ecrecovers and the response ECDSA
    /// signature, the per-voucher ecrecover, the up-to-255-keccak preimage walk,
    /// the per-frame copies — runs off the accept loop and off its sibling
    /// streams. A voucher-heavy stream does not stall the others or delay
    /// acceptance of new streams, and effective per-connection throughput is not
    /// bounded by one core's worth of serialized CPU (ADR 005 §Concurrent stream
    /// limits, #1788).
    ///
    /// The handler is shared as `Arc<Self>` so each task holds the `'static`
    /// handle a spawn needs; the iroh `ProtocolHandler::accept` `&self` borrow is
    /// bridged by [`super::ClientProtocol`], which owns the `Arc` and hands
    /// `serve` a clone.
    ///
    /// The loop also enforces the ADR 005 §Connection lifetime application-layer
    /// idle-close: once no stream is in flight, a connection with no new stream
    /// for [`APP_IDLE_TIMEOUT`] is closed (any activity re-arms the clock). This
    /// reclaims a peer that keeps the QUIC connection alive with keep-alive PINGs
    /// but sends no streams — which the transport idle timer never reaps.
    #[allow(clippy::cognitive_complexity)] // linear accept/select loop; splitting obscures it.
    pub(super) async fn serve(self: Arc<Self>, conn: Connection) -> anyhow::Result<()> {
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
        // connection's lifetime once a valid `BindNodeId` arrives. Lock-free
        // (`ArcSwapOption`) so the per-stream tasks — which now run concurrently
        // on separate tasks (#1788) — read and publish it without an await point
        // or a shared mutex (#1788 item 3). The cell is only a FALLBACK: every
        // spend-authorizing decision uses the binding on its OWN stream when the
        // request carries one, and reads this cell only when the request omits
        // it. Publishes are last-writer-wins (as under the prior mutex); a client
        // that re-sends its one identity per ADR 005 just re-publishes the same
        // address, and a client that races two different identities only muddies
        // the fallback for its own unbound streams — no lane it cannot already
        // sign for.
        let bound_addr: Arc<ArcSwapOption<Address>> = Arc::new(ArcSwapOption::empty());
        let peer = conn.remote_id();

        let idle_timeout = self.idle_timeout.unwrap_or(APP_IDLE_TIMEOUT);
        let mut inflight: JoinSet<()> = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                accepted = conn.accept_bi() => match accepted {
                    Ok((send, recv)) => {
                        let permit = Arc::clone(&stream_sem).try_acquire_owned().ok();
                        let bound = Arc::clone(&bound_addr);
                        let this = Arc::clone(&self);
                        // Boxed: the serve future is large (clippy::large_futures),
                        // and spawning the boxed future keeps it off the accept
                        // task's stack frame. Each task holds its own `Arc<Self>`.
                        let span = serve_stream_span(peer, self.node_id);
                        let serve = Box::pin(Arc::clone(&this).serve_stream(
                            send,
                            recv,
                            permit,
                            bound,
                            peer,
                        ));
                        inflight.spawn(
                            async move {
                                let span = tracing::Span::current();
                                let mut unended = CancelledMark(Some(span.clone()));
                                // Caught only to mark the span, then resumed, so the
                                // `JoinSet` still sees the panic and meters it.
                                let ended = AssertUnwindSafe(serve).catch_unwind().await;
                                unended.0 = None;
                                match ended {
                                    // Reason first, then the completed / failed
                                    // count, on every end: a scrape between the
                                    // two never shows an unclaimed failure.
                                    Ok(Ok(end)) => {
                                        end.record(&span);
                                        let completed = end.meter(&this.metrics);
                                        this.metrics.inbound_stream_ended(completed);
                                    }
                                    Ok(Err(e)) => {
                                        span.record("outcome", "failed");
                                        span.record(
                                            "error",
                                            tracing::field::display(format_args!("{e:#}")),
                                        );
                                        this.log_stream_end(&e, &span);
                                        this.metrics.inbound_stream_ended(false);
                                    }
                                    Err(panic) => {
                                        span.record("outcome", "panicked");
                                        this.metrics.serve_stream_node_fault();
                                        this.metrics.inbound_stream_ended(false);
                                        span.record("otel.status_code", "ERROR");
                                        std::panic::resume_unwind(panic);
                                    }
                                }
                            }
                            .instrument(span),
                        );
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
                Some(joined) = inflight.join_next(), if !inflight.is_empty() => {
                    Self::note_joined_stream(joined);
                }
            }
        }
        // Drain any streams still finishing after the connection closed.
        while let Some(joined) = inflight.join_next().await {
            Self::note_joined_stream(joined);
        }
        Ok(())
    }

    /// File one finished per-stream task by its join outcome.
    ///
    /// The task logs its own error inside its span ([`Self::log_stream_end`]),
    /// so only a [`JoinError`] reaches here. It means the task did not return a
    /// value — the `JoinSet`
    /// isolates that from the connection, which keeps serving its other streams —
    /// and splits two ways: a PANIC is a node-side bug, logged at `error!` (the
    /// task's own panic arm already metered it on
    /// `decdn_serve_stream_node_fault_total`, before the failed count, so its rate
    /// is alertable); a CANCELLATION is a benign teardown artifact (the drain path
    /// never aborts, so this only arises on runtime shutdown), logged at `debug!`
    /// and not counted.
    fn note_joined_stream(joined: Result<(), JoinError>) {
        match joined {
            Ok(()) => {}
            Err(join_err) if join_err.is_panic() => {
                tracing::error!(error = %join_err, "client stream task panicked");
            }
            Err(join_err) => {
                tracing::debug!(error = %join_err, "client stream task cancelled");
            }
        }
    }

    /// File one finished serve stream's error by who caused it, and count it on
    /// exactly one reason counter.
    ///
    /// A node-side fault — an encode fault, an alignment error, a store fault, a
    /// framing fault — is the operator's only signal that a delivery was
    /// abandoned, since the client only ever sees a short stream. It logs at
    /// `error!` and bumps `decdn_serve_stream_node_fault_total`, so the rate is
    /// alertable rather than only greppable. A client payment fault
    /// ([`ClientPaymentFault`](super::wire::ClientPaymentFault)) is a rejected
    /// voucher and bumps `decdn_serve_stream_voucher_rejected_total`. A
    /// peer-attributable error ([`PeerFault`](super::wire::PeerFault)) is a peer
    /// that left or broke the protocol: `decdn_serve_stream_client_abandoned_total`
    /// when the serve loop tagged it [`PaidProgress`](super::wire::PaidProgress),
    /// otherwise `decdn_serve_stream_client_declined_total`. All three log at
    /// `debug!`. [`ErrEnd::of`] holds the classification.
    ///
    /// `{e:#}` rather than `{e}`: the marker sits in the chain, so the alternate
    /// form is what prints the cause beside it. A node-side fault also marks the
    /// stream's `span` as an error for the trace backend.
    fn log_stream_end(&self, e: &anyhow::Error, span: &tracing::Span) {
        let end = ErrEnd::of(e);
        end.meter(&self.metrics);
        if end == ErrEnd::NodeFault {
            span.record("otel.status_code", "ERROR");
            tracing::error!(
                error = %format_args!("{e:#}"),
                "client stream ended with a node-side fault"
            );
        } else {
            tracing::debug!(error = %format_args!("{e:#}"), "client stream ended with error");
        }
    }

    /// Serve one delivery stream end to end. Returns how the stream ended
    /// ([`ServeEnd`]); an `Err` is recorded on the span as `outcome = failed`.
    ///
    /// Kept as one linear, ADR-ordered sequence (read → bind → blob gate →
    /// channel → sign → deliver); splitting it would scatter the ADR-005
    /// ordering invariants across helpers — same rationale as the probe
    /// handler's `serve`.
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    pub(super) async fn serve_stream(
        self: Arc<Self>,
        mut send: SendStream,
        mut recv: RecvStream,
        permit: Option<OwnedSemaphorePermit>,
        bound_addr: Arc<ArcSwapOption<Address>>,
        peer: PublicKey,
    ) -> anyhow::Result<ServeEnd> {
        let client_node_id = B256::from(*peer.as_bytes());
        let first = match read_first_message(&mut recv).await {
            Ok(first) => first,
            Err(StreamReadError { err, app_code }) => {
                reset_stream(&mut send, &mut recv, app_code);
                // A request-read failure is peer-side (a timeout, a malformed
                // frame, a decode fault), not a node-side bug, so it logs at
                // `debug!` and ends as a reset rather than an error.
                super::wire::record_stream_error(format_args!("{err:#}"));
                tracing::debug!(error = %format_args!("{err:#}"), "client request unreadable");
                return Ok(ServeEnd::Reset(ResetCause::RequestUnreadable));
            }
        };
        // The time-to-first-byte window opens on the decoded request: the accept
        // and the request read run at the peer's pace, so they stay outside it.
        let request_decoded_at = std::time::Instant::now();

        // Stream-cap exhausted: reset the stream with no signed response.
        // Signing a `StreamResponse` per rejected request would let a request
        // flood amplify into CPU exhaustion (an ECDSA signature per reject) — the
        // cap exists to shed load, not to add work to the reject path.
        if permit.is_none() {
            reset_stream(&mut send, &mut recv, APP_ERR_RATE_LIMITED);
            return Ok(ServeEnd::Reset(ResetCause::StreamCapFull));
        }

        let FirstMessage::Delivery(req, ext) = first;
        let _stream_guard = self.metrics.inbound_stream_guard();

        // Verify an ephemeral client binding if present (ADR 005 §Client
        // identity binding) and remember the recovered address for the
        // connection's lifetime (a binding may be sent once and omitted on
        // later requests). A malformed binding is a client fault — reset.
        let mut verified_client: Option<Address> = bound_addr.load().as_deref().copied();
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
                    bound_addr.store(Some(Arc::new(recovered)));
                }
                Ok(recovered) => {
                    if let Some(suppressed) = self.binding_warn.admit() {
                        tracing::warn!(
                            %peer,
                            claimed = %Address::from(addr_bytes),
                            recovered = %recovered,
                            suppressed,
                            interval = ?self.binding_warn.interval(),
                            "client binding signature recovered a different address"
                        );
                    }
                    reset_stream(&mut send, &mut recv, APP_ERR_MALFORMED_MESSAGE);
                    return Ok(ServeEnd::Reset(ResetCause::BadBinding));
                }
                Err(e) => {
                    if let Some(suppressed) = self.binding_warn.admit() {
                        tracing::warn!(
                            %peer,
                            error = %e,
                            suppressed,
                            interval = ?self.binding_warn.interval(),
                            "client binding signature invalid"
                        );
                    }
                    reset_stream(&mut send, &mut recv, APP_ERR_MALFORMED_MESSAGE);
                    return Ok(ServeEnd::Reset(ResetCause::BadBinding));
                }
            }
        }

        let hash = Hash::from_bytes(req.hash);
        record_request(&tracing::Span::current(), &req);

        // The request's price: the node's configured `rate_per_mb`, quoted
        // verbatim and threaded to every refusal and to the window tier. The
        // delivery floor never rewrites it — a sub-floor rate still sells, and
        // settlement clamps only the vote-weight byte credit (ADR 003
        // § Rate-floor enforcement).
        let rate_per_mb = self.rate_per_mb;

        // Chain-staleness gate (ADR 011 §Serving while chain-stale). Every gate
        // below this point — the local/governance deny-set, the funder
        // blacklist, the pool-solvency and signer-cap checks — answers from a
        // projection that only advances while the chain watchers reach the RPC.
        // Once the node has been unable to read the chain for longer than
        // `chain_staleness_grace_sec`, all of them silently pass on stale state,
        // so a takedown that landed during the blind window would go unenforced
        // and serving it is slashable. Refuse here, ABOVE those gates, rather
        // than sign a `StreamResponse` the node can no longer vouch for. It
        // collapses to `NotFound`, so the client re-routes to a peer whose reads
        // are live. `None` (no blacklist watcher wired — dev/test) disables the
        // gate, the same fail-open shape the pool-view gates use with no chain.
        if self
            .chain_freshness
            .as_ref()
            .is_some_and(crate::chain_freshness::ChainFreshness::is_stale)
        {
            return self
                .respond_error(&mut send, &req, ServeRejectReason::ChainStale, rate_per_mb)
                .await;
        }

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

        // Pool `getPool` view (owner + remaining), read ONCE and reused by the
        // funder gate here, the capability owner check, and the floor-`M` solvency
        // gates below. Read AFTER the origin-only gate, not beside it: a declined
        // request must not pay for a read it never uses. The wired `PoolView`
        // answers from the in-memory projection on a hit and, on a miss (a pool
        // opened before the watcher's cold-start head), does ONE `getPool` to
        // confirm the pool before this serve is admitted. A `None` here therefore
        // means the pool does not exist on-chain, is closed/reclaimed, or the read
        // faulted — the refuse-on-`None` gate just below turns that into a
        // rejection rather than a fail-open serve. With NO pool-view wired
        // (dev/test, no chain) the read is `None` and the downstream gates keep
        // their prior fail-open behavior.
        let pool_status = match self.pool_view.as_ref() {
            Some(view) => view.status(B256::from(req.pool_id)).await,
            None => None,
        };

        // Confirm-before-serve (ADR 003 §Pool solvency): when a pool-view is wired,
        // the admit-path `status()` above did a `getPool` on a projection miss, so a
        // `None` here means the pool does not exist on-chain, is closed/reclaimed, or
        // the read faulted. Refuse rather than fail open — a node must not admit a
        // serve against a pool it cannot confirm is live and solvent. `pool_status`
        // is therefore guaranteed `Some` past this point whenever `pool_view` is
        // wired, which lets the capability-owner and floor gates below rely on it.
        // With NO pool-view (dev/test, no chain wired) `pool_status` stays `None` and
        // the pre-#-gates keep their prior fail-open behavior.
        if self.pool_view.is_some() && pool_status.is_none() {
            return self
                .respond_error(
                    &mut send,
                    &req,
                    ServeRejectReason::PoolUnconfirmed,
                    rate_per_mb,
                )
                .await;
        }

        // Signer cap-headroom confirm (ADR 003 §Pool solvency): a capability whose
        // signer has already drawn its shared `cap` at other nodes is uncashable here.
        // Confirm the signer's on-chain `cap - spent` covers a floor before admitting,
        // so a "spent" capability sprayed to a fresh node is refused, not served for
        // vouchers this node can never redeem. Unregistered signer or no chain wired ->
        // u64::MAX (never refuse); a getAuthorization fault -> None -> refuse.
        if let (Some(view), Some(signer)) = (self.pool_view.as_ref(), verified_client) {
            let floor_micro =
                decdn_incentive::min_payment(self.credit_window(CHUNK_BYTES, 0), rate_per_mb);
            let headroom_ok = view
                .signer_cap_headroom_micro(B256::from(req.pool_id), signer)
                .await
                .is_some_and(|h| U256::from(h) >= floor_micro);
            if !headroom_ok {
                return self
                    .respond_error(
                        &mut send,
                        &req,
                        ServeRejectReason::SignerCapExhausted,
                        rate_per_mb,
                    )
                    .await;
            }
        }

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
        // Intake runs only with a confirmed pool: `pool_status` is `Some` whenever a
        // pool-view is wired (the refuse-on-`None` gate above returned otherwise), so
        // the owner is known here. With no pool-view wired (dev/test) `pool_status` is
        // `None` and intake is skipped — the no-chain path registers no lane from a
        // capability owner it cannot confirm.
        if let (Some(signer), Some(capability), Some(status)) =
            (verified_client, ext.capability.as_ref(), pool_status)
        {
            self.intake_capability(B256::from(req.pool_id), signer, status.owner, capability);
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

        // Per-lane active-stream counter. Runs ONCE here, before any serve-path branch,
        // so every delivered stream — cache hit, backend-origin miss, window
        // pull-through miss, or buffered miss — is counted. The count is not a solvency
        // gate: the per-pool ceiling and the per-signer live cap (both in
        // `try_reserve_floor`, ADR 003 §Pool solvency) bound un-vouchered floor across
        // and within lanes. The count exists for the wallet-less-resume heal
        // (`commit_one_proof`), which reads `active_streams` to tell a lone wedged
        // stream from a concurrent-sibling out-of-order voucher without depending on
        // chain data.
        //
        // Incremented under the lane lock so concurrent first-streams on a fresh lane
        // share one counter. `LaneSlot`'s drop releases the slot on every exit (success,
        // `?`, disconnect, panic).
        let mut lane_slot: Option<LaneSlot> = None;
        if let Some(lane) = known_lane.as_ref() {
            let guard = lane.lock().await;
            let active = guard.active_streams.clone();
            active.fetch_add(1, Ordering::Relaxed);
            drop(guard);
            lane_slot = Some(LaneSlot::new(active));
        }
        let _lane_slot = lane_slot;

        // Per-pool cumulative floor-credit admission reservation (ADR 003 §Pool
        // solvency, stateful-B). It sums floor credit across ALL distinct lanes on
        // the pool and bounds it to `remaining − M`, closing the fan-out hole where
        // many distinct signers each draw one un-vouchered floor on the same pool; a
        // per-signer live cap `k · one window` bounds any one signer's share
        // underneath it. The reservation is span-capped
        // to what THIS request can draw — at most one voucher-interval floor, less for
        // a bounded range or tail resume — and held for the stream's lifetime; the
        // `FloorReservation` guard releases it once the stream repays that floor, and
        // frees the pool's floor headroom on any exit via `Drop`.
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
        // pool-floor refusal reports `InsufficientDeposit` → wire
        // `StreamError::InsufficientDeposit`: this gate runs past the lane-ownership
        // proof, so its audience is the proven owner and it speaks the true reason
        // for the owner's top-up loop (option 2 / #2013), not the ambiguous
        // `NotFound` that unauthenticated misses collapse to.
        //
        // Held at fn scope so the reservation is released on EVERY exit via `Drop`.
        // Every serve path that reaches a serve loop MOVES it in and threads it through:
        // the direct-serve path hands it to `deliver`, and the
        // `serve_via_backend_origin` / `serve_via_window_pull_through` miss legs take it
        // by value and pass it by reference into their shared `serve_leg`. All three
        // release the reservation once the stream repays one floor; any other exit
        // (disconnect, `?`, panic) drops the guard, whose `Drop` frees the pool's live
        // floor headroom so an abandoned stream never holds it past its own lifetime.
        // No abandonment charge survives the drop — bounding un-vouchered floor is the
        // admission-time job of the pool ceiling and the per-signer live cap.
        let mut floor_reservation: Option<FloorReservation> = None;

        // Blob availability gate. A store fault is NOT an absence: an
        // `Unavailable` audit means the node genuinely lacks the blob (NotFound
        // / EvictedSinceProbe), but `Err` is a transient local store failure
        // that must not masquerade as a signed `NotFound` — a paying client
        // would treat that as authoritative and stop asking. Surface it as
        // `InternalError` and log. `serve_audit` also carries the complete
        // blob's size in the same store contact, so the delivery size gate
        // below needn't `inspect` again on a cache hit (#1789 item 7 part B).
        let audit = match self.cache.serve_audit(hash).await {
            Ok(audit) => audit,
            Err(e) => {
                tracing::warn!(
                    %hash,
                    error = %e,
                    "cache `serve_audit` lookup failed on delivery path"
                );
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
        let mut hit_size = audit.hit_size();
        // The initial `None` is unread on every live path (both branches below
        // either shed and return or overwrite it, and the audit `Err` returns
        // too) — kept anyway so the slot's declared type and its
        // `Drop`-at-fn-scope binding below read the same as the `lane_slot`
        // admission guard above.
        #[allow(unused_assignments)]
        let mut shed_slot: Option<crate::load_shed::ShedSlot> = None;
        // The gate's class. The shed gate admits under it and the first-byte clock
        // records under it. The class is fixed here, because a buffered fill below
        // serves a miss through the cache-hit `deliver`, and that loop cannot tell.
        let request_class;
        if audit.is_serveable() {
            request_class = RequestClass::CacheHit;
            // Serve-path hit rate (see `Metrics::serve_cache_hit`). Metered on the
            // availability decision itself, ahead of the shed gate below, so the
            // ratio stays a property of the store rather than of current pressure.
            self.metrics.serve_cache_hit();
            match self.shed.try_admit(request_class, client_node_id) {
                Ok(slot) => shed_slot = Some(slot),
                Err(reason) => {
                    self.metrics.load_shed_refused(reason);
                    tracing::debug!(
                        ?reason,
                        %hash,
                        class = "hit",
                        "load-shed refusing new serve"
                    );
                    return self
                        .respond_error(&mut send, &req, ServeRejectReason::LoadShedHit, rate_per_mb)
                        .await;
                }
            }
        } else {
            // A withdrawn hash is never pull-filled. An operator eviction is
            // sticky and authoritative (#279). A stored-corruption quarantine
            // holds the corrupt entry until GC reclaims it, so a fill would pay
            // for bytes that can never serve. Both answer `EvictedSinceProbe`,
            // because an earlier probe may have signed `has_blob: true` for
            // the hash.
            if audit.is_withdrawn() {
                return self
                    .respond_error(
                        &mut send,
                        &req,
                        ServeRejectReason::EvictedSinceProbe,
                        rate_per_mb,
                    )
                    .await;
            }
            // Partial-serve gate (#1506): a blob that isn't `Complete` may
            // still fully cover the requested span — the counterpart to the
            // will-serve coverage a partial holder now advertises over
            // `cdn/probe/v1` / the DHT. When it does, serve straight from
            // cache instead of running the miss-path fill legs below: treat
            // it exactly like the `audit.is_serveable()` cache-hit branch
            // above (same `CacheHit` shed class, same `hit_size`-driven size
            // gate + `deliver` downstream), just reached via a different
            // audit outcome. An uncovered span is NOT a decline — it simply
            // falls through to the ordinary miss path, unchanged.
            if let Some(size) = self
                .partial_hit_size(hash, req.byte_offset, req.byte_len)
                .await
            {
                // A covering partial is a hit for hit-rate purposes, counted
                // apart so the payoff of partial-holder advertisement stays
                // legible (see `Metrics::serve_cache_partial_hit`).
                self.metrics.serve_cache_partial_hit();
                request_class = RequestClass::CacheHit;
                match self.shed.try_admit(request_class, client_node_id) {
                    Ok(slot) => shed_slot = Some(slot),
                    Err(reason) => {
                        self.metrics.load_shed_refused(reason);
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
                hit_size = Some(size);
            } else {
                // The shed gate runs before the channel-ownership refusal below,
                // so an unbound / unknown-lane request can transiently hold a
                // `ShedSlot` until that refusal returns it. This is bounded by
                // the `ConnectionLimiter` global + per-source caps and is
                // self-limiting: once the node is pressured, further such
                // requests shed right here without acquiring a slot at all.
                // Keeping the gate here — ahead of channel-ownership and any
                // fill — preserves "shed before committing serve resources /
                // before any origin spend".
                //
                // The miss sibling of the two hit counters above, on the same
                // pre-shed footing: the gate cannot satisfy this request from
                // held bytes. It records that classification only — the shed
                // gate below, the floor reservation, and `pull_authorized` can
                // each end the request before any fill tier runs.
                self.metrics.serve_cache_miss();
                request_class = RequestClass::CacheMiss;
                match self.shed.try_admit(request_class, client_node_id) {
                    Ok(slot) => shed_slot = Some(slot),
                    Err(reason) => {
                        self.metrics.load_shed_refused(reason);
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
                // Every request shape — whole blob, bounded, resumed — takes the same
                // two-leg spine on its preferred tiers: the spine signs first and its
                // pull leg fetches only the requested span's missing chunk groups
                // (ADR 037 §Origin-tier pull-through). The FALLBACK tiers below it
                // (`try_local_populate`, the buffered pull-through) still import the
                // whole blob before the size gate answers; they serve exactly the
                // requested span at delivery.
                // The fault latch (#1129): declared before the first tier so every
                // tier's `CacheError::Store` lands in it.
                // Pre-spend deposit floor (#1519). Every fill tier below spends:
                // the own-origin spine and the local tier front the operator's own
                // origin egress, and the peer spine and the buffered tier front real
                // upstream USDC. All are gated
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
                    && let Some(key) = lane_key
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
                    match self.try_reserve_floor(
                        pool_id,
                        key.signer,
                        status.remaining,
                        rate_per_mb,
                        reserved,
                    ) {
                        Err(refusal) => {
                            self.log_floor_refusal(
                                refusal,
                                FloorRefusalSite {
                                    pool_id,
                                    signer: key.signer,
                                    hash,
                                    remaining: status.remaining,
                                    ceiling: reserved,
                                },
                            );
                            return self
                                .respond_error(
                                    &mut send,
                                    &req,
                                    ServeRejectReason::from(refusal),
                                    rate_per_mb,
                                )
                                .await;
                        }
                        Ok(guard) => {
                            floor_reservation = Some(guard);
                        }
                    }
                }

                let mut fault_seen = false;

                let mut locally_filled = false;

                // Own-origin serve-miss via the two decoupled legs.
                // When the node's OWN configured fs/http/s3 origin can prove it
                // serves `hash` — it knows the size AND publishes the {H}.obao4
                // outboard — serve the request by running the local pull leg (fill
                // the cache from origin) beside the serve leg (stream the filling
                // cache to the paying client), exactly like the node→node window path
                // but with NO upstream, NO channel, and NO payment on the ingest side.
                // Time-to-first-byte does not wait for the whole blob to land.
                //
                // Any request shape routes here: the serve leg clamps to
                // `[byte_offset, end)` and the pull leg fills only that span's missing
                // chunk groups, so a bounded request costs exactly its aligned span
                // in origin egress.
                //
                // Serviceability is confirmed before the response is signed, by
                // `origin_size` (a live probe of the data object) and
                // `origin_range_serviceable` (an origin publishes the outboard AND
                // serves ranged reads of the data). The range half reads one chunk
                // group until the origin has served a clean range window once, then
                // reads nothing. So an origin that publishes an outboard but declines
                // `Range` degrades here instead of failing a signed stream.
                //
                // Best-effort degrade (ADR 037 §"Fallback is always correct"): no
                // published outboard / no ranged reads / no origin size / no origins
                // => fall through to `try_local_populate` below.
                // Once serviceable, `serve_via_backend_origin` claims the fill itself
                // (`CacheEngine::claim_fill`): the first same-hash miss OWNS
                // the local origin pull; a concurrent one ATTACHES as an observer and
                // streams the same filling cache to its own client (no double origin
                // egress). The registry is range-aware, so this coalescing is not
                // limited to the whole-blob case.
                if self.pull_authorized(&req, verified_client) {
                    match self.cache.origin_size(hash).await {
                        Ok(Some(total)) => {
                            match self.cache.origin_range_serviceable(hash, total).await {
                                // Serviceable: size known and an origin publishes the
                                // outboard and serves ranges. Enter the orchestration
                                // directly — it claims the fill (owner-or-attach)
                                // internally after signing the response, so no
                                // coalescing decision happens here.
                                Ok(true) => {
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
                                            FirstByteClock::new(request_decoded_at, request_class),
                                        ))
                                        .await;
                                    }
                                }
                                // Size known but no origin publishes the outboard
                                // and serves ranges — not serviceable via the range
                                // encoder. Degrade to the buffered local populate
                                // below.
                                Ok(false) => {}
                                // A genuine origin fault while probing the outboard
                                // or the first range window. Latch it (#1129) so a
                                // later-tier miss reports InternalError not NotFound,
                                // then fall through — another source may still serve.
                                Err(e) => {
                                    self.metrics.node_pull_through_error();
                                    tracing::warn!(%hash, error = %format_args!("{e:#}"), "own-origin range probe faulted; falling through");
                                    fault_seen = true;
                                }
                            }
                        }
                        // No origin knows the size, or no origin is configured at
                        // all — both a clean fall-through (degrade). `origin_size`
                        // returns Ok(None) only for clean declines; a probe that
                        // ends on a transport fault reaches the Err arm below.
                        Ok(None) | Err(CacheError::NoOrigin { .. }) => {}
                        // Any other origin fault latches `fault_seen` (#1129) so a
                        // later-tier miss reports InternalError not NotFound.
                        Err(e) => {
                            self.metrics.node_pull_through_error();
                            tracing::warn!(%hash, error = %e, "own-origin size probe faulted; falling through");
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
                if !locally_filled
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
                // Any request shape routes here. `serve_leg` clamps delivery to
                // `[offset, offset + len)` and bills only the wire it delivers; the
                // pull leg pulls only `missing_ranges(offset, len)` upstream, so a
                // bounded or resumed request fronts exactly its span.
                if locally_filled {
                    // The whole blob just filled from a local origin (#1116). Skip
                    // the node→node fill and fall through to the size gate +
                    // delivery.
                } else if let Some(origin) = self.pull_through_origin.as_ref()
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
                            FirstByteClock::new(request_decoded_at, request_class),
                        ))
                        .await;
                    }
                } else {
                    // Buffered pull-through (#831): used when no window provider is
                    // set.
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
        }
        // Held at fn scope so the slot lives across `deliver` / `serve_via_*` and
        // releases its admission counters on every exit, including the boxed
        // miss-serve return paths below.
        let _shed_slot = shed_slot;

        // Size gate. A plain cache hit
        // carries its size from the `serve_audit` above (#1789 item 7 part B),
        // so it skips the redundant `inspect` store hop entirely — including a
        // genuinely empty blob, which audits as serveable at size 0.
        //
        // The remaining branch is a miss the fill legs above just completed, so
        // the blob is on disk now: an `inspect` error — or a `None` size
        // (`NotFound`, or a `Partial` whose last chunk has not validated) — means
        // the fill did not land what it reported, which is a real store fault
        // and NOT a zero-length blob.
        // Advertising `total_bytes: 0` for a non-empty blob would sign a
        // `StreamResponse` the delivery then contradicts, and the receiver
        // (expecting 0 bytes) would abort on the first chunk. Surface the fault
        // instead.
        let total_bytes = if let Some(size) = hit_size {
            size
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
                    "just-filled blob reports no size to `inspect`; treating as fault"
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
        // Bounded-range bounds check (ADR 005 §Bounded byte ranges). Reject an
        // out-of-bounds range with a `StreamError` *before* signing the success
        // response below — otherwise the client accepts a signed `ok: true` that
        // `deliver`'s `export_range` then aborts mid-stream. A whole-blob request
        // (`byte_offset == 0 && byte_len == 0`) is always in bounds for a present
        // blob; `byte_len == 0` on a non-zero offset is the in-bounds whole-tail
        // read. Mirrors `align_range`'s bound check, shared with the two-leg
        // spine via `range_out_of_bounds`.
        if range_out_of_bounds(req.byte_offset, req.byte_len, total_bytes) {
            return self
                .respond_error(
                    &mut send,
                    &req,
                    ServeRejectReason::RangeNotSatisfiable,
                    rate_per_mb,
                )
                .await;
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
            if let Some(suppressed) = self.unbound_request_warn.admit() {
                tracing::warn!(
                    %peer,
                    suppressed,
                    interval = ?self.unbound_request_warn.interval(),
                    "stream request with no verified binding; refusing pre-serve"
                );
            }
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
            if let Some(suppressed) = self.unknown_lane_warn.admit() {
                tracing::warn!(
                    %peer,
                    ?lane_key,
                    suppressed,
                    interval = ?self.unknown_lane_warn.interval(),
                    "stream request on unknown lane; refusing pre-serve"
                );
            }
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
        // two-level check — `remaining − M ≥ committed + reserved` pool-wide, and
        // this signer's own live cap — both admits the stream and bounds the pool's
        // cumulative cross-lane LIVE floor credit. A miss-fill stream
        // already holds its reservation, so re-validate solvency against the pool's
        // already-committed LIVE floor reservation via
        // [`ClientHandler::pool_budget_covers_reserve`] with `new_reserve = 0` — the
        // same stateful check the mid-stream gate applies, so a pool-wide live
        // reservation that grew since this one was taken refuses here rather than
        // serving a free interval.
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
                match self.try_reserve_floor(
                    B256::from(req.pool_id),
                    lane_key.signer,
                    status.remaining,
                    rate_per_mb,
                    reserved,
                ) {
                    Ok(guard) => {
                        floor_reservation = Some(guard);
                        None
                    }
                    Err(refusal) => Some(refusal),
                }
            } else {
                // A miss-fill stream already holds its reservation, counted at both
                // levels, so this re-validates rather than reserves (`new_reserve =
                // 0`) — and at the POOL level only, for the reason
                // [`ClientHandler::pool_budget_covers_reserve`] gives: the pool
                // ceiling shrinks as co-tenants draw the pool down, so re-testing an
                // already-admitted reservation against the per-signer cap could
                // refuse a stream it let through pre-fill. Here that is strictly
                // worse than serving: the fill already fronted upstream USDC, so
                // refusing loses that spend for nothing. The per-signer cap did its
                // job pre-fill; this gate only asks whether the pool can still pay.
                (!self.pool_budget_covers_reserve(
                    B256::from(req.pool_id),
                    status.remaining,
                    U256::ZERO,
                ))
                .then_some(FloorRefusal::PoolExhausted)
            };
            if let Some(refusal) = refused {
                self.log_floor_refusal(
                    refusal,
                    FloorRefusalSite {
                        pool_id: B256::from(req.pool_id),
                        signer: lane_key.signer,
                        hash,
                        remaining: status.remaining,
                        ceiling: decdn_incentive::min_payment(guard_bytes, rate_per_mb),
                    },
                );
                return self
                    .respond_error(
                        &mut send,
                        &req,
                        ServeRejectReason::from(refusal),
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
            FirstByteClock::new(request_decoded_at, request_class),
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
pub(super) fn aligned_span(byte_offset: u64, byte_len: u64, total_bytes: u64) -> u64 {
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

/// Whether `[byte_offset, byte_offset + byte_len)` (`byte_len == 0` = to the
/// blob end) lies outside a `total_bytes`-byte blob. Mirrors
/// [`decdn_bao_range::align_range`]'s bound check so every serve tier — the
/// direct-serve gate and the two-leg spine — refuses the same ranges with
/// `RangeNotSatisfiable` BEFORE it signs a response. A whole-blob request
/// (`0, 0`) is always in bounds, including for the empty blob.
pub(super) const fn range_out_of_bounds(byte_offset: u64, byte_len: u64, total_bytes: u64) -> bool {
    if byte_offset == 0 && byte_len == 0 {
        return false;
    }
    if byte_offset >= total_bytes {
        return true;
    }
    if byte_len == 0 {
        return false;
    }
    match byte_offset.checked_add(byte_len) {
        Some(end) => end > total_bytes,
        None => true,
    }
}

#[cfg(test)]
mod range_helper_tests {
    use super::{CHUNK_GROUP_BYTES, aligned_span, range_out_of_bounds};

    #[test]
    fn range_out_of_bounds_mirrors_align_range() {
        let total = 100 * 1024;
        // Whole blob and in-bounds tails / bounds are satisfiable.
        assert!(!range_out_of_bounds(0, 0, total));
        assert!(!range_out_of_bounds(16 * 1024, 0, total));
        assert!(!range_out_of_bounds(16 * 1024, 32 * 1024, total));
        assert!(!range_out_of_bounds(0, total, total));
        // Offset at/past the end, an end past the blob, or an overflowing end.
        assert!(range_out_of_bounds(total, 0, total));
        assert!(range_out_of_bounds(total + 1, 0, total));
        assert!(range_out_of_bounds(16 * 1024, total, total));
        assert!(range_out_of_bounds(u64::MAX, 1, total));
        assert!(range_out_of_bounds(1, u64::MAX, total));
        // The empty blob is addressable only as (0, 0).
        assert!(!range_out_of_bounds(0, 0, 0));
        assert!(range_out_of_bounds(0, 1, 0));
    }

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
