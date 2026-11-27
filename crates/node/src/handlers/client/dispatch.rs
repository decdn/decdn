//! Connection accept loop + per-stream dispatch state machine.
//! See `mod.rs` for the struct + shared types.

use super::{
    APP_ERR_MALFORMED_MESSAGE, APP_ERR_NO_ERROR, APP_ERR_RATE_LIMITED, APP_IDLE_TIMEOUT, Address,
    Arc, B256, CHUNK_BYTES, CHUNK_GROUP_BYTES, ClientHandler, Connection, Deployment, FillOutcome,
    FirstMessage, Hash, LaneId, LaneKey, MissPath, MissRefusal, OwnedSemaphorePermit, PublicKey,
    RecvStream, Semaphore, SendStream, ServeRejectReason, StreamReadError, StreamRequest, U256,
    VarInt, read_first_message, reset_stream, verify_binding,
};
use arc_swap::ArcSwapOption;
use std::panic::AssertUnwindSafe;
use tokio::task::{JoinError, JoinSet};

use futures_util::FutureExt as _;
use tracing::Instrument as _;

use super::admission::{Admission, Admitted, DeferredAdmission, Places, UnadmittedSlot};
use super::outcome::{ErrEnd, ResetCause, ServeEnd};
use crate::load_shed::RequestClass;
use crate::metrics::FirstByteClock;
use decdn_incentive::payment_pool::SignerAuthorization;

/// The root span for one inbound serve stream.
///
/// The request fields (`hash`, `pool_id`, `byte_offset`, `byte_len`) are
/// recorded once the client binding verifies, so a stream reset before that has
/// none. `outcome` / `reason` / `bytes` are recorded once the stream ends
/// ([`ServeEnd::record`]), or `outcome` / `error` when it ends on an error.
/// `peer` and `local_node_id` render as lowercase-hex iroh ids, the same as the
/// requester's `open_progressive_pull` span records them, so one trace query
/// joins the two sides of a transfer on `hash`, `pool_id`, `byte_offset` and
/// the swapped ids. The wait totals and the path's state
/// (`store_wait_ns` … `path_congestion_events`) are recorded as the serve
/// loop ends ([`super::serve_waits`]); a stream refused before its serve loop
/// has none.
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
        store_wait_ns = tracing::field::Empty,
        send_wait_ns = tracing::field::Empty,
        proof_wait_ns = tracing::field::Empty,
        conn_lost_packets = tracing::field::Empty,
        conn_lost_bytes = tracing::field::Empty,
        conn_sent_bytes = tracing::field::Empty,
        path_kind = tracing::field::Empty,
        path_rtt_us = tracing::field::Empty,
        path_cwnd = tracing::field::Empty,
        path_congestion_events = tracing::field::Empty,
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
        // The connection's streams that have not passed admission
        // (`MAX_UNADMITTED_STREAMS_PER_CONNECTION`).
        let unadmitted = Arc::new(Places::new(
            decdn_protocol::client::MAX_UNADMITTED_STREAMS_PER_CONNECTION,
        ));
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
                        // Reserved before the request is read, so a burst of
                        // opens cannot pass the cap. A stream the stream cap
                        // resets reserves none.
                        let unadmitted_slot = permit
                            .as_ref()
                            .and_then(|_| unadmitted.try_take())
                            .map(UnadmittedSlot::new);
                        let this = Arc::clone(&self);
                        // Boxed: the serve future is large (clippy::large_futures),
                        // and spawning the boxed future keeps it off the accept
                        // task's stack frame. Each task holds its own `Arc<Self>`.
                        let span = serve_stream_span(peer, self.node_id);
                        let serve = Box::pin(Arc::clone(&this).serve_stream(
                            conn.clone(),
                            send,
                            recv,
                            permit,
                            bound,
                            unadmitted_slot,
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
    /// `decdn_serve_stream_client_declined_total` when it tagged it
    /// [`FrameSeen`](super::wire::FrameSeen), and otherwise
    /// `decdn_serve_stream_unclaimed_total`. All of them log at `debug!`.
    /// [`ErrEnd::of`] holds the classification.
    ///
    /// `{e:#}` rather than `{e}`: the marker sits in the chain, so the alternate
    /// form is what prints the cause beside it. A stream that ended after payment
    /// also logs `paid_wire_bytes`, the wire bytes its vouchers credited. A
    /// node-side fault also marks the stream's `span` as an error for the trace
    /// backend.
    fn log_stream_end(&self, e: &anyhow::Error, span: &tracing::Span) {
        let end = ErrEnd::of(e);
        end.meter(&self.metrics);
        let paid_wire_bytes = e
            .downcast_ref::<super::wire::PaidProgress>()
            .map(|paid| paid.wire_bytes.get());
        if end == ErrEnd::NodeFault {
            span.record("otel.status_code", "ERROR");
            tracing::error!(
                error = %format_args!("{e:#}"),
                paid_wire_bytes,
                "client stream ended with a node-side fault"
            );
        } else {
            tracing::debug!(
                error = %format_args!("{e:#}"),
                paid_wire_bytes,
                "client stream ended with error"
            );
        }
    }

    /// Pin `hash` against capacity eviction for an open that has not passed
    /// funding admission, counting the pin in `slot` against the node's pin
    /// budget ([`super::MAX_UNADMITTED_PINS`]). Past the budget the open takes
    /// no pin, and its blob stays evictable until admission pins it.
    fn pin_unadmitted(
        &self,
        hash: Hash,
        slot: &mut Option<UnadmittedSlot>,
    ) -> Option<decdn_cache::InUse> {
        let slot = slot.as_mut()?;
        let Some(place) = self.unadmitted_pins.try_take() else {
            tracing::debug!(%hash, "unadmitted pin budget spent; the open takes no pin");
            return None;
        };
        slot.count_pin(place);
        Some(self.cache.hold_in_use(hash))
    }

    /// Serve one delivery stream end to end. Returns how the stream ended
    /// ([`ServeEnd`]); an `Err` is recorded on the span as `outcome = failed`.
    /// `conn` is the stream's connection: each proof wait reads its transport
    /// progress ([`proof_wait`](super::proof_wait)).
    ///
    /// Kept as one linear, ADR-ordered sequence (stream caps → read → timestamp
    /// gate → bind → deny gates → pin → blob gate → tier → sign → admit →
    /// deliver); splitting it would scatter the ADR-005 ordering invariants
    /// across helpers.
    ///
    /// An open that does no fill work (a hit, a partial that covers the span,
    /// or the node's own origin with a proven size) answers `ok: true` before
    /// funding admission, which runs at the payer's first proof
    /// ([`super::admission`]). A miss answers `NotFound` at once unless the
    /// request sets `pull_through`; one that sets it runs the funding gates,
    /// then the fill tiers. The stream holds its blob against capacity
    /// eviction (`InUse`) while it lives.
    #[allow(
        clippy::cognitive_complexity,
        clippy::too_many_lines,
        clippy::too_many_arguments
    )]
    pub(super) async fn serve_stream(
        self: Arc<Self>,
        conn: Connection,
        mut send: SendStream,
        mut recv: RecvStream,
        permit: Option<OwnedSemaphorePermit>,
        bound_addr: Arc<ArcSwapOption<Address>>,
        unadmitted_slot: Option<UnadmittedSlot>,
        peer: PublicKey,
    ) -> anyhow::Result<ServeEnd> {
        // Stream-cap exhausted: reset the stream before reading anything and with
        // no signed response (ADR 005 §Concurrent stream limits). Reading first
        // would let every over-cap stream buffer a request frame for the read
        // timeout; signing a `StreamResponse` per rejected request would let a
        // request flood amplify into CPU exhaustion (an ECDSA signature per
        // reject). The cap exists to shed load, not to add work to the reject path.
        if permit.is_none() {
            reset_stream(&mut send, &mut recv, APP_ERR_RATE_LIMITED);
            return Ok(ServeEnd::Reset(ResetCause::StreamCapFull));
        }
        // Unadmitted cap, on the same footing as the stream cap: a connection
        // that holds its cap of streams not yet admitted gets no more of them.
        // An open that answers before admission pins its blob, so without the
        // cap one peer could hold many blobs against capacity eviction for
        // free. The slot was reserved at accept; it moves into the stream's
        // deferred admission, or drops once the stream is admitted at open or
        // ends.
        let Some(unadmitted_slot) = unadmitted_slot else {
            reset_stream(&mut send, &mut recv, APP_ERR_RATE_LIMITED);
            return Ok(ServeEnd::Reset(ResetCause::UnadmittedCapFull));
        };
        let mut unadmitted_slot = Some(unadmitted_slot);
        // Per-request limiter (ADR 005 §Request rate limiting), on the same
        // footing as the stream cap: refuse before the request is read, with no
        // signed response. Most refusals after the request is read sign a
        // response, and an origin-only node probes its backend for an unseen
        // hash before any payment check, so an unpaid flood must stop here.
        if let Some(limiter) = &self.request_limiter
            && let Err(layer) =
                limiter.check(&crate::dht::routing::NodeId::from_bytes(*peer.as_bytes()))
        {
            tracing::debug!(layer = layer.as_str(), %peer, "client request rate-limited");
            reset_stream(&mut send, &mut recv, APP_ERR_RATE_LIMITED);
            return Ok(ServeEnd::Reset(ResetCause::RateLimited(layer)));
        }

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

        let FirstMessage::Delivery(req, ext) = first;
        let _stream_guard = self.metrics.inbound_stream_guard();

        // Clock-skew gate (`crate::handlers::timestamp_skew`): every signed
        // `StreamResponseBody` on this stream — the serve, the window tiers,
        // and every `respond_error` refusal — echoes `req.timestamp_us` into
        // the EIP-712 signed set (ADR 005 § Signer binding), which a pull
        // receipt carries as the approximate serve time, so the node signs
        // only timestamps consistent with its own clock. Checked once here, directly on the decoded request:
        // everything signed later on this stream reuses this same request's
        // timestamp. A skewed request resets with NO signed response — a
        // `respond_error` would itself sign the echoed timestamp — mirroring
        // the bad-binding arm below, but with its own code: the request
        // decodes fine, and the remedy is a clock fix, not a protocol fix
        // (ADR 013 §Application Error Codes).
        let gate_now_us = crate::handlers::timestamp_skew::now_us();
        if let Some(skew_us) =
            crate::handlers::timestamp_skew::excessive_skew_us(req.timestamp_us, gate_now_us)
        {
            if let Some(suppressed) = self.timestamp_skew_warn.admit() {
                tracing::warn!(
                    %peer,
                    timestamp_us = req.timestamp_us,
                    now_us = gate_now_us,
                    skew_us,
                    bound_us = crate::handlers::timestamp_skew::MAX_REQUEST_TIMESTAMP_SKEW_US,
                    suppressed,
                    interval = ?self.timestamp_skew_warn.interval(),
                    "stream request timestamp beyond the clock-skew bound; resetting \
                     (the requester's clock is wrong — or this node's, if every \
                     request refuses)"
                );
            }
            reset_stream(&mut send, &mut recv, decdn_protocol::APP_ERR_TIMESTAMP_SKEW);
            return Ok(ServeEnd::Reset(ResetCause::TimestampSkew));
        }

        // Payment pool routing (ADR 005 § Payment pool routing). The request
        // names the pool it pays from; every pool read, voucher domain and lane
        // below belongs to that pool's route. A pool this node does not accept
        // is refused `Declined` with the accepted pools listed, before any pool
        // read or chain call, so the requester can retry on one it holds.
        let Some(deployment) = Deployment::from_pool_account(&req.payment_pool)
            .filter(|deployment| self.pool_route(*deployment).is_some())
        else {
            return self
                .respond_error(
                    &mut send,
                    &req,
                    ServeRejectReason::UnacceptedPool,
                    self.rate_per_mb,
                )
                .await;
        };
        let pool_view = self
            .pool_route(deployment)
            .and_then(|route| route.pool_view.clone());

        // Verify an ephemeral client binding if present (ADR 005 §Client
        // identity binding) and remember the recovered address for the
        // connection's lifetime (a binding may be sent once and omitted on
        // later requests). A malformed binding is a client fault — reset.
        let mut verified_client: Option<Address> = bound_addr.load().as_deref().copied();
        if let Some(binding) = &ext.binding {
            let addr_bytes = binding.ethereum_address;
            match verify_binding(
                client_node_id,
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
        // the floor prices only the vote-weight credit the governance chain's
        // `FeeRouter` grants on a sweep (ADR 003 § Rate-floor enforcement).
        let rate_per_mb = self.rate_per_mb;

        // Chain-staleness gate (ADR 011 §Serving while chain-stale). Every gate
        // below this point — the local/governance deny-set, the funder
        // blacklist, the pool-solvency and signer-cap checks — answers from a
        // projection that only advances while the chain watchers reach the RPC.
        // Once the node has been unable to read the chain for longer than
        // `chain_staleness_grace_sec`, all of them silently pass on stale state,
        // so a takedown that landed during the blind window would go
        // unenforced. Refuse here, ABOVE those gates, rather
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
        // blacklist watcher denies before it evicts. Two sets, ONE wire code,
        // `Declined`, shared with the eviction arm below: ADR 011 §StreamRequest
        // Response requires that a client cannot tell a governance takedown from
        // this operator's own denylist. The reasons stay distinct only so the
        // operator's own metrics can tell them apart, which no client can read.
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
        // `pull_authorized` refuses it before every fill tier, and admission
        // refuses it `OwnerMismatch`).
        let self_operator = self.eth_signer.address();
        let lane_key = verified_client.map(|signer| LaneId {
            deployment,
            key: LaneKey {
                pool_id: B256::from(req.pool_id),
                signer,
                provider: self_operator,
            },
        });

        // Backend and chain work only for a request that names a lane. A
        // request with no verified binding can name none, so it can never be
        // served; the lane gate below would refuse it `OwnerMismatch`. Refuse it
        // here instead, before an origin-only node's `{H}.sizeproof` read and before
        // an unseen pool costs it a `getPool`: a stream of unbound requests for
        // fresh hashes or random `pool_id`s would otherwise spend one of each.
        // A bound request reads the chain at admission: at its first proof for
        // an open that does no fill work, before the fill tiers for a miss that
        // pulls through. The per-request limiter and the admit-read cap bound
        // what bound requests can spend. With no pool-view wired (dev/test) the
        // admission refuses it.
        if pool_view.is_some() && lane_key.is_none() {
            self.warn_unbound_request(&peer);
            return self
                .respond_error(
                    &mut send,
                    &req,
                    ServeRejectReason::OwnerMismatch,
                    rate_per_mb,
                )
                .await;
        }

        // Origin-only policy (#1759). When the operator opts out of foreign
        // relay, the own/foreign decision is backend-authoritative: the request's
        // `namespace_id` is a routing hint, not a trust anchor (ADR 002), so the
        // node asks its OWN backend whether it publishes a verifying
        // `{H}.sizeproof` for this content hash. The probe is memoized
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
        // from real cache misses. `Fault` (a transport error, a permanent
        // fault such as an HTTP 403, an open breaker, a timed-out walk or a
        // proof that does not verify) is NOT an absence, so it refuses for
        // `InternalError`, matching the store-fault handling on the `has()`
        // path below. Both sign `Declined`: this node will not serve the hash
        // during this fetch.
        if !self.relay_foreign_namespaces {
            match self.cache.origin_presence(hash).await {
                decdn_cache::OriginPresence::Present(_) => {}
                decdn_cache::OriginPresence::Absent => {
                    // The probe reads a withdrawn hash as absent. Keep the
                    // operator's metric on the withdrawal: it is this node's own
                    // content, not a foreign hash. Both reasons sign `Declined`.
                    let reason = if self.cache.is_evicted(hash) || self.cache.is_quarantined(hash) {
                        ServeRejectReason::Withdrawn
                    } else {
                        ServeRejectReason::ForeignNamespaceDeclined
                    };
                    return self
                        .respond_error(&mut send, &req, reason, rate_per_mb)
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

        // Hold the blob against capacity eviction while this stream lives
        // (`InUse`), so an open that answers before admission still has its
        // blob when the payer's first proof arrives. A takedown `evict` still
        // removes a held blob. A miss drops the hold below: it holds nothing.
        let mut pin = self.pin_unadmitted(hash, &mut unadmitted_slot);

        // Blob availability gate. A store fault is NOT an absence: an
        // `Unavailable` audit means the node genuinely lacks the blob (NotFound
        // / Withdrawn), but `Err` is a transient local store failure
        // that must not masquerade as a signed `NotFound`. Refuse it for
        // `InternalError` (wire `Declined`) and log. `serve_audit` also carries the complete
        // blob's size in the same store contact, so the delivery size gate
        // below needn't `inspect` again on a cache hit (#1789 item 7 part B).
        let audit = match self.cache.serve_audit(hash).await {
            Ok(audit) => audit,
            Err(e) => {
                tracing::warn!(
                    %hash,
                    error = %e.display_chain(),
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
        // A withdrawn hash is never served and never pull-filled. An operator
        // eviction is sticky and authoritative (#279). A stored-corruption
        // quarantine holds the corrupt entry until GC reclaims it, so a fill
        // would pay for bytes that can never serve. Both answer `Withdrawn`.
        if audit.is_withdrawn() {
            drop(pin.take());
            return self
                .respond_error(&mut send, &req, ServeRejectReason::Withdrawn, rate_per_mb)
                .await;
        }
        let mut hit_size = audit.hit_size();
        // A covering partial hit answers with the blocks it holds and the
        // span it serves, not the whole blob (`partial_hit_coverage`).
        let mut partial_hit = false;
        // What the funding gates read for a miss that pulls through. Such a
        // stream is admitted before its fill tiers; every other stream that
        // reaches the response is admitted at its first proof.
        let mut admitted_at_open: Option<super::admission::Funding> = None;
        // The load-shed slot of a miss that pulls through, held at fn scope.
        // An open that answers before admission takes its slot at admission
        // (`super::admission`), so an unpaid open holds none.
        let mut shed_slot: Option<crate::load_shed::ShedSlot> = None;
        // The serve tier. The shed gate admits under it and the first-byte clock
        // records under it. The class is fixed here, because a buffered fill below
        // serves a miss through the cache-hit `deliver`, and that loop cannot tell.
        // The tier pick ends the clock's admission phase.
        let request_class;
        let admitted_at = std::time::Instant::now();
        // A node serves a hash only while it holds a live origin attestation
        // for it (ADR 022 § Origin Attestation). A hit checks here. A miss is
        // checked once its fill lands (below) or by the own-origin arm, and
        // every served answer is checked again when it is signed.
        let partial_size = if audit.is_serveable() {
            None
        } else {
            self.partial_hit_size(hash, req.byte_offset, req.byte_len)
                .await
        };
        if (audit.is_serveable() || partial_size.is_some()) && !self.attested(hash).await {
            drop(pin.take());
            return self
                .respond_error(&mut send, &req, ServeRejectReason::Unattested, rate_per_mb)
                .await;
        }
        if audit.is_serveable() {
            request_class = RequestClass::CacheHit;
            // Serve-path hit rate (see `Metrics::serve_cache_hit`). Metered on the
            // availability decision itself, ahead of the shed gate, so the ratio
            // stays a property of the store rather than of current pressure.
            self.metrics.serve_cache_hit();
        } else if let Some(size) = partial_size {
            // Partial-serve gate (#1506): a blob that isn't `Complete` may
            // still fully cover the requested span. When it does, serve
            // straight from cache instead of running the miss-path fill legs
            // below: treat it exactly like the `audit.is_serveable()` cache-hit
            // branch above (same `CacheHit` shed class, same `hit_size`-driven
            // size gate + `deliver` downstream), just reached via a different
            // audit outcome. An uncovered span is NOT a decline — it simply
            // takes the miss path below.
            //
            // A covering partial is a hit for hit-rate purposes, counted apart
            // so the payoff of partial-holder advertisement stays legible (see
            // `Metrics::serve_cache_partial_hit`).
            self.metrics.serve_cache_partial_hit();
            request_class = RequestClass::CacheHit;
            hit_size = Some(size);
            partial_hit = true;
        } else {
            // A miss holds nothing to pin. A fill below that lands the blob
            // takes a new hold before delivery.
            drop(pin.take());
            if let Some(slot) = unadmitted_slot.as_mut() {
                slot.release_pin();
            }
            // The miss sibling of the two hit counters above, on the same
            // pre-shed footing: the gate cannot satisfy this request from held
            // bytes. It records that classification only — the pull-through
            // flag, the shed gate, the funding gates and `pull_authorized` can
            // each end the request before any fill tier runs.
            self.metrics.serve_cache_miss();
            request_class = RequestClass::CacheMiss;

            // The fault latch (#1129): declared before the first tier so every
            // tier's `CacheError::Store` lands in it.
            let mut fault_seen = false;

            // Own-origin serve-miss via the two decoupled legs. When the node's
            // OWN configured fs/http/s3 origin publishes a `{H}.sizeproof` that
            // verifies, the node holds the blob for the network's purposes: it
            // answers `ok: true` with full coverage, whether or not the request
            // sets `pull_through`, and admits at the first proof. Then the local
            // pull leg fills the cache from origin beside the serve leg that
            // streams it to the paying client, with NO upstream, NO lane, and NO
            // payment on the ingest side. Time-to-first-byte does not wait for
            // the whole blob to land.
            //
            // Any request shape routes here: the serve leg clamps to
            // `[byte_offset, end)` and the pull leg fills only that span's missing
            // chunk groups, so a bounded request costs exactly its aligned span
            // in origin egress.
            //
            // Before the response is signed, the node reads one object per
            // hash, memoised for the probe TTL: the origin's `{H}.sizeproof`
            // (ADR 005 § Pull-through). It states the size and proves
            // it, and the engine keeps the proof for the response. The outboard
            // and the range windows are read only after admission, in
            // `serve_via_backend_origin`. The origin publishes `{H}`,
            // `{H}.obao4` and `{H}.sizeproof` (ADR 037 § Origin-tier
            // pull-through), so a missing or broken object there is an origin
            // fault: a `StreamError` before any `ChunkData`.
            //
            // `serve_via_backend_origin` claims the fill itself
            // (`CacheEngine::claim_fill`) after admission: the first same-hash miss
            // OWNS the local origin pull; a concurrent one ATTACHES as an observer
            // and streams the same filling cache to its own client (no double
            // origin egress).
            match self.cache.origin_presence(hash).await {
                // The memo says present but the kept proof cannot be read
                // back: an origin fault, latched like any other (#1129).
                decdn_cache::OriginPresence::Present(total)
                    if self.cache.size_proof(hash, total).await.is_none() =>
                {
                    tracing::warn!(
                        %hash,
                        total,
                        "the origin presence memo says present but no size proof of that \
                         size can be read back; treating it as an origin fault"
                    );
                    self.metrics.node_pull_through_error();
                    fault_seen = true;
                }
                decdn_cache::OriginPresence::Present(_) if !self.attested(hash).await => {
                    // The own backend holds the blob, but this node holds no
                    // origin seat to attest it with, and no live attestation
                    // from another origin (ADR 022 § Origin Attestation).
                    return self
                        .respond_error(&mut send, &req, ServeRejectReason::Unattested, rate_per_mb)
                        .await;
                }
                decdn_cache::OriginPresence::Present(total) => {
                    let adm = DeferredAdmission::new(
                        req.clone(),
                        ext,
                        deployment,
                        verified_client,
                        lane_key,
                        peer,
                        rate_per_mb,
                        total,
                        request_class,
                        client_node_id,
                        unadmitted_slot.take(),
                    );
                    // Boxed: the serve future is large
                    // (clippy::large_futures).
                    return Box::pin(self.serve_via_backend_origin(
                        &conn,
                        send,
                        recv,
                        &req,
                        hash,
                        adm,
                        total,
                        rate_per_mb,
                        FirstByteClock::new(request_decoded_at, admitted_at, request_class),
                    ))
                    .await;
                }
                // No origin holds the blob, or no origin is configured at all:
                // nothing is served from the origin.
                decdn_cache::OriginPresence::Absent => {}
                // An origin fault latches `fault_seen` (#1129) so a later-tier
                // miss reports InternalError not NotFound. The probe logs the
                // cause when it memoises the fault.
                decdn_cache::OriginPresence::Fault => {
                    self.metrics.node_pull_through_error();
                    fault_seen = true;
                }
            }

            // A miss whose request does not ask for pull-through answers
            // `NotFound` at once, with no pool, chain or upstream work: the
            // requester asked what this node holds, not for a fill. The
            // refusal advertises the blocks the store holds whole, so a
            // partial holder that does not cover the span still reads as a
            // holder of its blocks. An own-origin probe fault above is logged
            // and metered there; it does not change this answer.
            if !ext.pull_through {
                let coverage = self.miss_coverage(hash, &req).await;
                return self
                    .respond_refusal(
                        &mut send,
                        &req,
                        ServeRejectReason::CacheMiss,
                        rate_per_mb,
                        coverage,
                    )
                    .await;
            }

            // A miss that pulls through is admitted at open: the shed gate,
            // then the funding gates, before any fill tier, so no tier fronts
            // origin egress or upstream USDC for a request they refuse. The shed
            // slot is held for the rest of the stream.
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
            // Admitted at its funding gates: the stream leaves the unadmitted
            // count before the gates read the chain.
            drop(unadmitted_slot.take());
            let funding = match self
                .funding_gates(
                    &peer,
                    &req,
                    &ext,
                    deployment,
                    verified_client,
                    lane_key,
                    rate_per_mb,
                )
                .await
            {
                Ok(funding) => funding,
                Err(reason) => {
                    return self
                        .respond_error(&mut send, &req, reason, rate_per_mb)
                        .await;
                }
            };
            let pool_status = funding.pool_status;

            // Node-to-node cache-miss pull-through (#831). Fronting upstream
            // USDC egress is privileged: gate it on the request PROVING
            // voucher authority in the named pool — a verified client
            // binding (`verified_client`) whose address signs a known lane
            // of that pool (`pull_authorized`). Pool *existence* cannot gate
            // spend (pool ids are public on-chain via `PoolOpened`, so any
            // leech could name one); only proven lane authority can. An
            // unbound request, or one with no lane in that pool, gets a
            // plain `NotFound` and cannot make this node spend — closing
            // the proxy-abuse / griefing vector where an unpaid client
            // drains the buyer deposit. (Multi-hop node→node pulls therefore require the
            // downstream requester to send a binding; both the direct-client
            // `decdn get` (#1115) and the node→node requester
            // (`node_origin`, #1117) do, so chained pull-through works.)
            // On a successful fill, fall through to the normal size-gate +
            // delivery path; otherwise it stays a `NotFound`.
            //
            // Every request shape — whole blob, bounded, resumed — takes the same
            // two-leg spine on its preferred tiers: the spine signs first and its
            // pull leg fetches only the requested span's missing chunk groups
            // (ADR 037 §Origin-tier pull-through). The buffered pull-through
            // below it imports the whole blob before the size gate answers; it
            // serves exactly the requested span at delivery.
            // `authority` is this request's one answer to `pull_authorized`,
            // taken from the lane the funding gates resolved. Every tier below
            // reads it rather than the live registry, so the request sees one
            // answer throughout: every tier runs on its lane, or none runs and the
            // request refuses on the `no_lane` path. That holds in both
            // directions. A lane a sibling stream registers after the
            // resolution does not authorize this request. A lane `forget_lane`
            // drops after it (a reclaimed pool) still does: the upstream spend
            // stays bounded by the credit the lane holds.
            //
            // Every fill tier below fronts the operator's own origin egress or
            // real upstream USDC. So the pre-spend deposit floor (#1519) runs
            // HERE, before any tier: a pool whose `remaining − M` cannot cover
            // one chunk (capped by the request's own aligned span when it bounds
            // itself) is refused before the node spends, so a dust-deposit pool
            // cannot make the node pay for absent hashes. `window.rs` keeps its
            // own guard for the pull-through tier. A `None` view fails open (the
            // on-chain `redeem` is the backstop). With no authority no tier
            // spends, so the gate is skipped.
            let authority = Self::pull_authorized(lane_key, funding.known_lane.as_ref());
            if authority.is_some()
                && let Some(status) = pool_status
            {
                let guard_bytes = if req.byte_len > 0 {
                    aligned_span(req.byte_offset, req.byte_len, u64::MAX).min(CHUNK_BYTES)
                } else {
                    CHUNK_BYTES
                };
                if !self.pool_remaining_covers_window(status.remaining, guard_bytes, rate_per_mb) {
                    self.log_deposit_refusal(
                        B256::from(req.pool_id),
                        hash,
                        status
                            .remaining
                            .saturating_sub(self.pool_min_remaining_deposit),
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

            // Window-paced pull-through (#856, ADR 037) is the preferred path
            // when its provider is set: instead of buffering the whole
            // blob via `populate` and only THEN serving (fronting 100% of the
            // upstream cost before any downstream voucher), it runs the pull
            // leg (fill the cache from upstream) beside the serve leg (stream
            // the filling cache to the paying client), so the pull draws
            // only what the downstream lane has paid for, plus the small
            // `PULL_AHEAD_BYTES` bound.
            //
            // Any request shape routes here. `serve_leg` clamps delivery to
            // `[offset, offset + len)` and bills only the wire it delivers; the
            // pull leg pulls only `missing_ranges(offset, len)` upstream, so a
            // bounded or resumed request fronts exactly its span.
            if let Some(origin) = self.pull_through_origin.as_ref()
                && let Some((lk, ln)) = authority
            {
                // Boxed: the serve future is large; keep it off the
                // `serve_stream` stack frame (clippy::large_futures). The
                // orchestration claims the fill (owner-or-attach) internally after
                // signing the response, so two concurrent same-hash misses share one
                // upstream pull (no double spend, #305) without a decision here.
                return Box::pin(self.serve_via_window_pull_through(
                    &conn,
                    send,
                    recv,
                    &req,
                    hash,
                    client_node_id,
                    lk,
                    ln,
                    Arc::clone(origin),
                    pool_status,
                    fault_seen,
                    rate_per_mb,
                    FirstByteClock::new(request_decoded_at, admitted_at, request_class),
                ))
                .await;
            }
            // Buffered pull-through (#831): reached when no window provider
            // is set, or when the request has no spend authority. This arm
            // is where every unauthorized miss refuses on the `no_lane` path.
            // A request not authorized to make this node spend, or a node
            // with no pull-through configured, attempts nothing here, so this
            // tier contributes no new information. The two keep separate
            // paths: only the first is a remote peer's to trigger.
            let (buffered, path) = match self.pull_through {
                _ if authority.is_none() => (FillOutcome::CleanMiss, MissPath::NoLane),
                Some(timeout) => (
                    self.try_pull_through(hash, timeout).await,
                    MissPath::BufferedMiss,
                ),
                None => (FillOutcome::CleanMiss, MissPath::NoPullThrough),
            };
            if !buffered.is_filled() {
                let refusal = MissRefusal::new(fault_seen || buffered.is_fault(), path);
                return self
                    .respond_miss(&mut send, &req, refusal, rate_per_mb)
                    .await;
            }
            // The fill tiers land the blob, not an attestation for it. The
            // serving rule binds the landed blob as it binds a hit, so a seated
            // origin signs its own here, and a node that can neither sign nor
            // holds a live one refuses (ADR 005 § Serving rule).
            if !self.attested(hash).await {
                return self
                    .respond_error(&mut send, &req, ServeRejectReason::Unattested, rate_per_mb)
                    .await;
            }
            // The blob landed: hold it for the rest of the stream.
            pin = Some(self.cache.hold_in_use(hash));
            admitted_at_open = Some(funding);
        }
        // Held at fn scope so the slot lives across `deliver` / `serve_via_*` and
        // releases its admission counters on every exit, including the boxed
        // miss-serve return paths below.
        let _shed_slot = shed_slot;
        let _pin = pin;

        // Size gate. A plain cache hit carries its size from the `serve_audit`
        // above (#1789 item 7 part B), so it skips the redundant `inspect`
        // store hop entirely — including a genuinely empty blob, which audits
        // as serveable at size 0.
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
                    tracing::warn!(%hash, error = %e.display_chain(), "cache `inspect` failed on delivery path");
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
        // Bounded-range bounds check (ADR 005 §Bounded byte ranges). Reject a
        // request whose START has no bytes to serve with a
        // `StreamError` *before* signing the success response below. Otherwise
        // the client accepts a signed `ok: true` that `deliver`'s `export_range`
        // then aborts mid-stream. A whole-blob request (`byte_offset == 0 &&
        // byte_len == 0`) is always in bounds for a present blob; `byte_len == 0`
        // on a non-zero offset is the in-bounds whole-tail read. An END past the
        // blob is not out of bounds: it is served clamped to the blob's end.
        // Mirrors `align_range_clamped`'s bound check, shared with the own-origin
        // twin via `range_out_of_bounds`.
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
        // The response states `total_bytes`, so its proof must be at hand
        // before any reservation: a complete blob or a covering partial built
        // or kept it above. With none, refuse now with a signed answer rather
        // than fail the signing later (ADR 005 § Bounded byte ranges).
        if self.cache.size_proof(hash, total_bytes).await.is_none() {
            return self
                .respond_error(
                    &mut send,
                    &req,
                    ServeRejectReason::InternalError,
                    rate_per_mb,
                )
                .await;
        }

        let admission = match admitted_at_open {
            // A miss the fill tiers above landed: its funding gates already ran.
            // The fill tiers ran on a proven lane (`pull_authorized`), so the
            // lane resolves here; the refusals below are defensive.
            Some(funding) => {
                let Some(lane_key) = lane_key else {
                    self.warn_unbound_request(&peer);
                    return self
                        .respond_error(
                            &mut send,
                            &req,
                            ServeRejectReason::OwnerMismatch,
                            rate_per_mb,
                        )
                        .await;
                };
                let Some(lane) = funding.known_lane else {
                    return self
                        .respond_error(&mut send, &req, ServeRejectReason::UnknownLane, rate_per_mb)
                        .await;
                };
                // Pool admission gate (ADR 003 § Pool solvency) on the landed
                // blob's size: the pool's `remaining − M` must cover the
                // request's chunk-group-aligned span at this node's rate, capped
                // at one chunk. Admission runs the same gate for an open that
                // answers before it.
                let guard_bytes =
                    aligned_span(req.byte_offset, req.byte_len, total_bytes).min(CHUNK_BYTES);
                if let Some(status) = funding.pool_status
                    && !self.pool_remaining_covers_window(
                        status.remaining,
                        guard_bytes,
                        rate_per_mb,
                    )
                {
                    self.log_deposit_refusal(
                        B256::from(req.pool_id),
                        hash,
                        status
                            .remaining
                            .saturating_sub(self.pool_min_remaining_deposit),
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
                let guards = self
                    .send_guards(&lane, funding.pool_status.map(|s| s.owner))
                    .await;
                Admission::Admitted(Admitted {
                    lane_key,
                    lane,
                    guards,
                    // The fn-scope `_shed_slot` and `_pin` hold this stream's
                    // slot and pin.
                    shed: None,
                    pin: None,
                })
            }
            // An open that does no fill work answers before admission, which
            // runs at the payer's first proof.
            None => Admission::OnFirstProof(DeferredAdmission::new(
                req.clone(),
                ext,
                deployment,
                verified_client,
                lane_key,
                peer,
                rate_per_mb,
                total_bytes,
                request_class,
                client_node_id,
                unadmitted_slot.take(),
            )),
        };

        // Build and sign the success response. A complete blob serves every
        // block; a covering partial serves its own.
        let coverage = if partial_hit {
            self.partial_hit_coverage(hash, &req, total_bytes).await
        } else {
            super::fill::full_coverage(total_bytes)
        };
        let super::wire::Served::Signed(resp, resp_ext) = self
            .sign_served(&req, rate_per_mb, total_bytes, coverage)
            .await?
        else {
            return self
                .respond_error(&mut send, &req, ServeRejectReason::Unattested, rate_per_mb)
                .await;
        };
        self.write_stream_response(&mut send, &resp, &resp_ext)
            .await?;
        let mut first_byte = FirstByteClock::new(request_decoded_at, admitted_at, request_class);
        first_byte.mark_responded();

        // Stream the blob against the stream's credit.
        Box::pin(self.deliver(
            &conn,
            &mut send,
            &mut recv,
            hash,
            req.byte_offset,
            req.byte_len,
            total_bytes,
            admission,
            rate_per_mb,
            first_byte,
        ))
        .await
    }
}

/// Content bytes a `(byte_offset, byte_len)` request will actually be BILLED for,
/// i.e. the chunk-group-aligned superset `export_bao_range_stream` serves.
///
/// Mirrors `decdn_bao_range::align_range_clamped`: the start floors to its group
/// boundary, the end ceils to one and clamps to the blob: an end past the blob
/// clamps rather than being an error here. The serve path does not trim back to
/// `byte_offset` (trimming would break bao verification; the receiver discards
/// the leading bytes itself), so the payer covers the whole aligned range. A
/// pre-flight price computed on the *requested* span would under-reserve by up
/// to two groups.
///
/// `byte_len == 0` means "to the end of the blob" (whole blob when `byte_offset`
/// is also 0). Total-saturating throughout: the caller's range bounds check has
/// already rejected a start at or past the blob end, and a zero-length blob
/// correctly yields 0.
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
/// blob end) has no start to serve from a `total_bytes`-byte blob. Mirrors
/// [`decdn_bao_range::align_range_clamped`]'s bound check so every serve tier
/// (the direct-serve gate and the own-origin twin) refuses the same starts with
/// `RangeNotSatisfiable` BEFORE it signs a response. A whole-blob request
/// (`0, 0`) is always in bounds, including for the empty blob. An end past the
/// blob is NOT out of bounds here: it is served clamped to the blob's end, so
/// only a start at or past a non-empty blob's end refuses.
pub(super) fn range_out_of_bounds(byte_offset: u64, byte_len: u64, total_bytes: u64) -> bool {
    decdn_bao_range::align_range_clamped(byte_offset, byte_len, total_bytes).is_err()
}

/// The admit-gate verdict on a signer's on-chain authorization (ADR 003
/// §Capability delegation). `None` from the view means the read faulted with no
/// earlier registered read of the signer, which is a chain problem and refuses as
/// [`ServeRejectReason::PoolUnconfirmed`]. A registration at or past its expiry
/// at `now` refuses as [`ServeRejectReason::CapabilityExpired`]. A live
/// registration whose headroom cannot pay `floor_micro` refuses as
/// [`ServeRejectReason::SignerCapExhausted`]. `None` from this function admits.
pub(super) fn signer_refusal(
    auth: Option<SignerAuthorization>,
    floor_micro: U256,
    now: u64,
) -> Option<ServeRejectReason> {
    match auth {
        None => Some(ServeRejectReason::PoolUnconfirmed),
        Some(SignerAuthorization::Registered { expiry, .. }) if now >= expiry => {
            Some(ServeRejectReason::CapabilityExpired)
        }
        Some(auth) if !auth.covers(floor_micro, now) => Some(ServeRejectReason::SignerCapExhausted),
        Some(_) => None,
    }
}

#[cfg(test)]
mod signer_refusal_tests;

#[cfg(test)]
mod range_helper_tests;
