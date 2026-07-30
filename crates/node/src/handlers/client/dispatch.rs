//! Connection accept loop + per-stream dispatch state machine.
//! Bodies split from `mod.rs` (#1254); see there for the struct + shared types.

use super::{
    APP_ERR_MALFORMED_MESSAGE, APP_ERR_NO_ERROR, APP_ERR_RATE_LIMITED, APP_IDLE_TIMEOUT, Address,
    Arc, B256, CHUNK_GROUP_BYTES, ChannelId, ClientHandler, ClientMessage, Connection, FillOutcome,
    FirstMessage, Hash, MB_BYTES, Mutex, OwnedSemaphorePermit, REJECTION_CLOSE_TIMEOUT, RecvStream,
    RejectReason, Semaphore, SendStream, ServeRejectReason, StreamReadError, StreamResponseBody,
    TeeOpen, VarInt, min_payment, read_first_message, reset_stream, verify_binding,
};
use futures_util::StreamExt as _;

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
                        tracing::debug!(error = %e, "client stream ended with error");
                    }
                }
            }
        }
        // Drain any streams still finishing after the connection closed.
        while let Some(res) = inflight.next().await {
            if let Err(e) = res {
                tracing::debug!(error = %e, "client stream ended with error");
            }
        }
        Ok(())
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
                return Err(err);
            }
        };

        // Stream-cap exhausted: reset the stream with no signed response.
        // Signing a `StreamResponse` (or a cooperative-close waiver) per rejected
        // request would let a request flood amplify into CPU exhaustion (an ECDSA
        // signature per reject) — the cap exists to shed load, not to add work to
        // the reject path.
        if permit.is_none() {
            reset_stream(&mut send, &mut recv, APP_ERR_RATE_LIMITED);
            return Ok(());
        }

        // A cooperative-close request is a standalone sign-and-reply exchange,
        // not a delivery (ADR 003 §Cooperative close).
        let (req, ext) = match first {
            FirstMessage::Delivery(req, ext) => (req, ext),
            FirstMessage::CooperativeClose(cc) => {
                return self.handle_cooperative_close(send, cc).await;
            }
        };
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
                .respond_error(&mut send, &req, ServeRejectReason::HashDenied)
                .await;
        }
        if self.cache.is_chain_denied(hash) {
            return self
                .respond_error(&mut send, &req, ServeRejectReason::ChainHashDenied)
                .await;
        }

        // Origin-blacklist gate (ADR 011 §On Blacklist Event: "stops accepting
        // any StreamRequest that presents a channel funded by that operator
        // address"). `state.client` is that funding address — the same field the
        // in-flight takedown re-checks read. This gate is keyed on the FUNDER
        // and must NEVER be re-keyed onto `voucher_signer`: a blacklisted funder
        // can pin a clean throwaway key as its signer, so checking the signer
        // would let it buy delivery behind a delegate. The owner-mismatch gate
        // further down asks the unrelated *signer* question.
        //
        // This must sit ABOVE the availability check, not after channel
        // resolution: every cache-miss arm below `return`s its own refusal, so a
        // gate placed downstream is simply never reached on a miss and the
        // blacklisted funder gets `NotFound` instead. That is the one answer
        // `wire_error`'s doc says must never be given here — a client told
        // `NotFound` retries elsewhere and pays again, when in fact every node
        // will refuse it. It also silently under-counted the operator's own
        // compliance metric by the whole cache-miss fraction.
        //
        // Resolving the channel early is a `HashMap` lookup, so the cost is
        // nil; `pull_authorized` keeps its own check as the spend-side backstop.
        if let Some(channel) = self
            .channels
            .lock()
            .await
            .get(&ChannelId::from(req.channel_id))
            .cloned()
        {
            let funder = channel.lock().await.state.client;
            if self.content_deny.is_origin_denied(&funder) {
                tracing::warn!(%funder, "refusing delivery on a channel funded by a blacklisted origin");
                return self
                    .respond_error(&mut send, &req, ServeRejectReason::OriginDenied)
                    .await;
            }
        }

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
        match self.cache.has(hash).await {
            Ok(true) => {}
            Ok(false) => {
                // Eviction is sticky and authoritative — never pull-fill a
                // hash an operator deliberately evicted (#279).
                if self.cache.is_evicted(hash) {
                    return self
                        .respond_error(&mut send, &req, ServeRejectReason::EvictedSinceProbe)
                        .await;
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
                let mut fault_seen = false;
                if (req.byte_offset > 0 || req.byte_len > 0)
                    && self.pull_authorized(&req, verified_client).await
                {
                    let (size, range_outcome) = self.try_range_pull_through(hash, &req).await;
                    range_pulled_size = size;
                    fault_seen |= range_outcome.is_fault();
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
                let mut locally_filled = false;
                if range_pulled_size.is_none()
                    && let Some(timeout) = self.local_populate
                    && self.pull_authorized(&req, verified_client).await
                {
                    let local = self.try_local_populate(hash, timeout).await;
                    fault_seen |= local.is_fault();
                    locally_filled = local.is_filled();
                }

                // Window-paced pull-through (#856, ADR 037) is the preferred path
                // when its provider is set: instead of buffering the whole
                // blob via `populate` and only THEN serving (fronting 100% of the
                // upstream cost before any downstream voucher), it fuses the
                // upstream pull with downstream delivery so the per-request
                // exposure is bounded to `pull_ahead_bytes`. It requires an
                // offset-0 request (the tee imports the FULL-blob bao stream —
                // its verifying decoder walks `ChunkRanges::all()` — and the
                // window loop streams/bills the entire blob); a resumed miss
                // falls back to the buffered path.
                //
                // It also requires `byte_len == 0` (a whole-blob/whole-tail
                // request): the window loop streams and bills the entire blob, so
                // routing a *bounded* `byte_len > 0` request here (e.g. when the
                // range pull declined for lack of an outboard) would over-deliver
                // and over-bill the whole blob to a client that asked for a
                // prefix. A bounded request whose range pull declines therefore
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
                    && self.pull_authorized(&req, verified_client).await
                {
                    match self.cache.open_tee_sink(hash) {
                        TeeOpen::Owner(tee) => {
                            // Boxed: the fused serve future is large; keep it off
                            // the `serve_stream` stack frame (clippy::large_futures).
                            return Box::pin(self.serve_via_window_pull_through(
                                send,
                                recv,
                                &req,
                                &ext,
                                hash,
                                client_node_id,
                                Arc::clone(origin),
                                tee,
                                fault_seen,
                            ))
                            .await;
                        }
                        // A concurrent fill for this hash is already running
                        // (#305): do NOT open a second upstream pull (no double
                        // spend). Wait for it via the coalescing `populate`, then
                        // fall through to serve from the store; if it does not
                        // land, report the miss.
                        TeeOpen::InFlight => {
                            let coalesced = self.await_coalesced_fill(hash).await;
                            if !coalesced.is_filled() {
                                let reason =
                                    FillOutcome::miss_reason(fault_seen || coalesced.is_fault());
                                return self.respond_error(&mut send, &req, reason).await;
                            }
                        }
                    }
                } else {
                    // Buffered pull-through (#831): the pre-#856 path, used when
                    // the window provider is unset or for a resumed request.
                    let buffered = match self.pull_through {
                        Some(timeout) if self.pull_authorized(&req, verified_client).await => {
                            self.try_pull_through(hash, timeout).await
                        }
                        // No pull-through configured, or the request is not
                        // authorized to make this node spend: nothing was attempted,
                        // so this tier contributes no new information.
                        _ => FillOutcome::CleanMiss,
                    };
                    if !buffered.is_filled() {
                        let reason = FillOutcome::miss_reason(fault_seen || buffered.is_fault());
                        return self.respond_error(&mut send, &req, reason).await;
                    }
                }
            }
            Err(e) => {
                tracing::warn!(%hash, error = %e, "cache `has` lookup failed on delivery path");
                return self
                    .respond_error(&mut send, &req, ServeRejectReason::InternalError)
                    .await;
            }
        }

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
                        .respond_error(&mut send, &req, ServeRejectReason::InternalError)
                        .await;
                }
            };
            let Some(total_bytes) = size else {
                tracing::warn!(
                    %hash,
                    "blob present per `has` but `inspect` reports no size; treating as fault"
                );
                return self
                    .respond_error(&mut send, &req, ServeRejectReason::InternalError)
                    .await;
            };
            total_bytes
        };
        if self.max_blob_size_bytes > 0 && total_bytes > self.max_blob_size_bytes {
            return self
                .respond_error(&mut send, &req, ServeRejectReason::BlobTooLarge)
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
                    .respond_error(&mut send, &req, ServeRejectReason::RangeNotSatisfiable)
                    .await;
            }
        }

        // Resolve the channel (must be pre-persisted — see module docs / #327).
        let channel_id = ChannelId::from(req.channel_id);
        let channel = self.channels.lock().await.get(&channel_id).cloned();

        // An unknown / never-opened channel is refused *before* any bytes are
        // signed or served. Otherwise up to one voucher interval (the negotiated
        // cadence, by default 1 MB) — or the entire blob, if smaller — ships free
        // before `collect_voucher` rejects with `WrongChannel` mid-stream (#848).
        // The mid-stream `WrongChannel` reason cannot ride in the initial
        // `StreamResponse`, so use the delivery-side `NotFound` here (also
        // mirrors the owner-mismatch gate below and avoids leaking channel
        // existence).
        let Some(channel) = channel else {
            tracing::warn!(%channel_id, "stream request on unknown channel; refusing pre-serve");
            return self
                .respond_error(&mut send, &req, ServeRejectReason::UnknownChannel)
                .await;
        };

        // A channel with a signed cooperative-close waiver is being settled at
        // its final watermark — the node committed to serving no further bytes
        // on it (ADR 003 §Cooperative close). Refuse new delivery, collapsing to
        // `NotFound` so it stays wire-indistinguishable from an unknown channel.
        // The in-flight backstop is in `collect_voucher` (a stream already
        // running when the waiver was signed stops at its next voucher).
        if channel.lock().await.state.cooperative_close_signed() {
            return self
                .respond_error(&mut send, &req, ServeRejectReason::CooperativeCloseSigned)
                .await;
        }

        // A verified client binding MUST match the channel's pinned
        // `voucher_signer`. This is a SIGNER question, not a funder one: the
        // signer is the only identity whose vouchers this channel accepts, so a
        // binding for anything else — including the channel's own funder, when
        // it delegated signing — would fail `WrongSigner` at the first voucher
        // regardless. Refuse before delivering any bytes, closing the leech for
        // bound clients. Unbound connections fall back to the voucher-signature
        // gate; an on-chain NodeId→address lookup that would close the residual
        // for unbound peers is out of scope (#327).
        if let Some(client) = verified_client {
            let authorized_signer = channel.lock().await.state.voucher_signer;
            if client != authorized_signer {
                tracing::warn!(%client, %authorized_signer, "binding does not authorize this channel");
                return self
                    .respond_error(&mut send, &req, ServeRejectReason::OwnerMismatch)
                    .await;
            }
        }

        // Honor a client voucher-interval proposal (ADR 003 §Voucher Interval
        // Negotiation): accept the smaller of the proposal and our configured
        // cadence, never below 1 MB.
        let interval_mb = match ext.voucher_interval_mb {
            Some(proposed) => self.voucher_interval_mb.min(proposed).max(1),
            None => self.voucher_interval_mb,
        };

        let rate_per_mb = self.clamped_rate();

        // Pre-flight deposit gate — the direct-serve twin of the pull-through
        // guard in `window.rs` (keep the two in step). Without it the node signs
        // `ok: true` and streams a full credit window before `stage_voucher`'s
        // `AmountExceedsDeposit` can fire at the first voucher boundary, so a
        // channel that cannot cover even that first window gets it free on every
        // request (#1516).
        //
        // The quantity is remaining HEADROOM, not the gross deposit: both the
        // off-chain check (`ChannelState::stage_voucher`) and the on-chain one
        // (`PaymentChannel._advanceClaimWatermark`) compare the *cumulative*
        // voucher amount against the deposit, so a long-lived channel with most
        // of its deposit already claimed has only `deposit - last_amount` left.
        //
        // The ceiling is the CREDIT WINDOW, not one interval: `deliver` streams
        // while `delivered - paid < credit_window(interval_bytes)`, so with
        // `credit_window_bytes` configured (#1477) the node fronts a whole window
        // before it collects anything. Gating on one interval would let enabling
        // a credit window silently re-open the hole.
        //
        // ... but capped by the request's own span, or a legitimately funded
        // sub-interval fetch (a small blob, or a bounded range) would be refused
        // for not covering a window it will never use.
        //
        // The span is the CHUNK-GROUP-ALIGNED one, not the requested one, because
        // that is what gets billed: `export_bao_range_stream` snaps the range out
        // to the enclosing 16 KiB group boundaries (a bao proof anchors whole
        // groups) and `deliver` does NOT trim back — the receiver discards the
        // leading bytes itself. Pricing the *requested* span would under-reserve
        // by up to two groups (~32 KiB, ~3% of a default 1 MiB window), which for
        // a two-byte range request is four orders of magnitude. That is not the
        // by-design looseness below; it is the wrong quantity.
        //
        // What remains loose is only bao's proof interleave: `guard_bytes` counts
        // CONTENT bytes while billing counts WIRE bytes. State the residual as an
        // ABSOLUTE bound, because the fraction is misleading — the boundary proof
        // is `64 * log2(blob_groups / range_groups)`, so it amortizes to ~0.4% of
        // a whole blob or a window-sized range but reaches ~8% of a *single*
        // 16 KiB group of a multi-gigabyte blob. Either way it is at most ~1.3 KiB
        // per request, against the 1 MiB (or wider) window that used to ship free.
        // Deliberate, and in the safe direction — the gate can only ever *serve* a
        // request it should have refused, never refuse one that could pay — and
        // the mid-stream ceiling remains the exact authority.
        //
        // A zero-length blob yields a zero ceiling and passes (#1054).
        //
        // The bounds check above rejected `byte_offset >= total_bytes`, so the
        // span arithmetic never clamps to zero. That is the load-bearing property,
        // not underflow: a clamped span of 0 would zero the ceiling and wave every
        // request through.
        //
        // This is a per-request floor, NOT a reservation: two concurrent streams
        // on one channel can each pass and jointly exceed the headroom. The
        // mid-stream ceiling bounds each STREAM to one window of unbilled egress,
        // but nothing caps the aggregate at the deposit — what bounds N is the
        // per-connection stream cap. Same in `window.rs`.
        let interval_bytes = interval_mb.saturating_mul(MB_BYTES).max(1);
        let guard_bytes = aligned_span(req.byte_offset, req.byte_len, total_bytes)
            .min(self.credit_window(interval_bytes));
        let ceiling = min_payment(guard_bytes, rate_per_mb);
        let (deposit, last_amount) = {
            let guard = channel.lock().await;
            (guard.state.deposit, guard.state.last_amount())
        };
        if deposit.saturating_sub(last_amount) < ceiling {
            return self
                .respond_error_at_rate(
                    &mut send,
                    &req,
                    ServeRejectReason::InsufficientDeposit,
                    rate_per_mb,
                )
                .await;
        }

        // Build and sign the success response.
        let body = StreamResponseBody {
            hash: req.hash,
            ok: true,
            rate_per_mb,
            total_bytes,
            channel_id: req.channel_id,
            timestamp_us: req.timestamp_us,
            redirect: None,
        };
        let resp = self.sign_response(body, None, Some(interval_mb))?;
        self.write_message(&mut send, &ClientMessage::StreamResponse(resp))
            .await?;

        // Stream the blob, collecting vouchers at each interval boundary.
        self.deliver(
            &mut send,
            &mut recv,
            hash,
            req.byte_offset,
            req.byte_len,
            total_bytes,
            channel_id,
            Some(&channel),
            client_node_id,
            rate_per_mb,
            interval_mb,
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
