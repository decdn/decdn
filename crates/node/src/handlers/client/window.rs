//! Window-paced pull-through serve path (#856, ADR 037).

use std::sync::atomic::Ordering;

use alloy::primitives::U256;

use super::{
    Arc, B256, ChannelId, ClientHandler, ClientMessage, FillOutcome, Hash, MB_BYTES, NodeOrigin,
    RecvStream, SendStream, ServeRejectReason, StreamRequest, StreamRequestExt, StreamResponseBody,
    WINDOW_PULL_FALLBACK_DEADLINE, min_payment,
};

impl ClientHandler {
    /// Serve a cache miss by running the two decoupled serve-miss legs (#856, ADR
    /// 037): the pull leg fills the cache from upstream for only the missing ranges
    /// while the serve leg streams the filling cache to the paying client, pacing the
    /// upstream spend by the downstream's vouchers so per-request speculative
    /// exposure is bounded to `pull_ahead_bytes` rather than the whole blob. The
    /// caller has already proven channel ownership and confirmed `byte_offset == 0`.
    /// This path claims
    /// the fill itself (`CacheEngine::claim_fill`) after signing the response, so
    /// two concurrent same-hash misses share ONE upstream pull (the owner drives
    /// it; the second attaches as an observer). Terminal: consumes `send`/`recv`.
    ///
    /// `fault_seen` carries whether an EARLIER tier (the reactive local-origin
    /// populate) hit a backend fault for this request (#1129). This path
    /// is the last tier, so all three of its MISS exits — the leech shed, no
    /// openable provider, and the open deadline — refuse via
    /// [`FillOutcome::miss_reason`], reporting `InternalError` when this node is
    /// degraded rather than merely empty.
    ///
    /// The no-openable-provider exit adds a SECOND source of that fault: the pull's
    /// own [`PullMiss`](crate::node_origin::PullMiss), which says whether the
    /// candidate walk failed on a fault in THIS node — a buyer key that cannot
    /// sign, a deadline config that cannot run, a channel store it cannot read
    /// (#1560). The two are OR'd, because they are the same claim from different
    /// tiers: this node, not the content, is why the request cannot be answered.
    ///
    /// The open-deadline exit is the one hole left. `tokio::time::timeout` DROPS the
    /// walk, so any miss it had latched dies with the cancelled future and that
    /// arm can only report `fault_seen`. Left as-is deliberately: a deadline expiry is
    /// not a fault on its own ([`ClientHandler::on_pull_through_timeout`] treats the
    /// buffered twin the same way), and closing it needs a latch the caller owns rather
    /// than one living inside the future.
    ///
    /// The leech shed is included deliberately. `StreamError::NotFound`'s own doc
    /// does sanction it ("declines to pull through … seed-leech caps"), so a bare
    /// `CacheMiss` there is defensible in isolation — but it is the wrong code once
    /// the local origin has already faulted: the ONLY reason this request reached
    /// the paid peer path at all is that the node's own origin is down, and the
    /// operator needs that on the reject metric, not a `cache_miss` tally.
    ///
    /// The channel-class refusals (`UnknownChannel`, `InsufficientDeposit`) keep
    /// their own reasons: they are client-attributable and would have refused
    /// regardless of origin health, and they collapse to `NotFound` deliberately so
    /// a prober cannot map out other clients' channel balances
    /// ([`ServeRejectReason::wire_error`]).
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn serve_via_window_pull_through(
        &self,
        mut send: SendStream,
        mut recv: RecvStream,
        req: &StreamRequest,
        ext: &StreamRequestExt,
        hash: Hash,
        client_node_id: B256,
        origin: Arc<NodeOrigin>,
        fault_seen: bool,
        rate_per_mb: u64,
    ) -> anyhow::Result<()> {
        // The ADR 011 OPEN-TIME deny gates are already discharged on the only
        // path that reaches here: `serve_stream` refuses a denylisted hash
        // before the availability check, and this branch is entered only behind
        // `pull_authorized`, which refuses a blacklisted funding origin. Keep it
        // that way — if this function ever gains a second caller, that caller
        // owes both checks, because this is a spend-and-serve path.
        //
        // They cover the request, not the stream: a takedown landing after this
        // point is caught per MB boundary inside the serve leg (`serve_leg`, ADR 011
        // §On Blacklist Event, in-flight termination), which matters most here
        // because this path is simultaneously *acquiring* the blob upstream.
        //
        // Resolve the owning channel (existence + ownership already proven by
        // `pull_authorized`) — needed for the deposit guard and the downstream
        // voucher collection.
        let channel_id = ChannelId::from(req.channel_id);
        let Some(channel) = self.channels.lock().await.get(&channel_id).cloned() else {
            return self
                .respond_error(
                    &mut send,
                    req,
                    ServeRejectReason::UnknownChannel,
                    rate_per_mb,
                )
                .await;
        };

        // (1) Pre-flight deposit guard: refuse the speculative pull if the channel
        // provably cannot pay the cost it would front. Twin of the direct-serve
        // gate in `dispatch.rs` (#1516) — keep the two in step; they differ only
        // in the ceiling, because this path also fronts the *upstream* spend.
        //
        // With a finite
        // `max_blob_size_bytes` the ceiling is the worst-case whole-blob cost. When
        // the size cap is unbounded (`0`) there is no whole-blob ceiling, so the
        // guard falls back to the per-request speculative *window* cost — it must
        // never fully fail open, or disabling the size cap would silently disable
        // deposit protection and let a near-empty channel trigger an unbounded
        // speculative pull (#856). The window is `pull_ahead_bytes` floored at one
        // voucher interval, matching the serve leg (`serve_leg`).
        //
        // `guard_bytes` is a CONTENT-byte ceiling while billing is in bao WIRE
        // bytes (the proof overhead makes wire slightly higher — a fraction that
        // shrinks with blob size, well under 1% past a few groups, ADR 038), so the
        // guard is a hair loose. Benign: it only under-reserves by that proof
        // fraction, and a channel that exhausts mid-stream is bounded to one window
        // of upstream spend by the window loop regardless; the true wire ceiling is
        // enforced downstream by the `cumulative <= expected_wire_bytes` overrun
        // check. Not widened to keep the ceiling legible as "the blob size cap".
        let guard_bytes = if self.max_blob_size_bytes > 0 {
            self.max_blob_size_bytes
        } else {
            let interval_bytes = self.voucher_interval_mb.saturating_mul(MB_BYTES).max(1);
            self.pull_ahead_bytes
                .as_ref()
                .map_or(decdn_common::config::DEFAULT_PULL_AHEAD_BYTES, |b| b.get())
                .max(interval_bytes)
                // Match the effective loop window: the credit window (#1477) can
                // widen `pulled − served_paid` past `pull_ahead_bytes`, so the
                // deposit floor must cover it too, or a near-empty channel could
                // trigger a speculative pull it cannot pay for.
                .max(self.credit_window(interval_bytes))
        };
        let ceiling = min_payment(guard_bytes, rate_per_mb);
        let (deposit, last_amount) = {
            let guard = channel.lock().await;
            (guard.state.deposit, guard.state.last_amount())
        };
        let headroom = deposit.saturating_sub(last_amount);
        if headroom < ceiling {
            self.log_deposit_refusal(channel_id, hash, headroom, ceiling);
            return self
                .respond_error(
                    &mut send,
                    req,
                    ServeRejectReason::InsufficientDeposit,
                    rate_per_mb,
                )
                .await;
        }

        // (2) Seed-leech admission: global unrecouped budget + per-peer share
        // ratio. `may_pull` bumps its own pause metric on refusal.
        let peer = client_node_id.0;
        if !self.leech_admit(&peer) {
            // A shed under the leech caps is a miss, not a client fault — so it
            // honors a fault latched by an earlier tier (#1129). See this function's
            // doc for why the shed is included where the channel-class refusals are
            // not.
            let reason = FillOutcome::miss_reason(fault_seen);
            return self
                .respond_error(&mut send, req, reason, rate_per_mb)
                .await;
        }

        // (3) Discover an upstream, open a channel, and read the blob header — ONE
        // discovery shared by both legs, with open-time candidate fallback preserved
        // (`open_pull_leg`). Bounded by the pull-through deadline so a slow/absent
        // upstream can't pin the stream. The namespace (ADR 005 §Namespace routing)
        // drives the origin-directory fallback inside `discover` on a total DHT miss
        // and is threaded onto the node-to-node leg so a directory-discovered cold
        // origin's own pull-through gate resolves (#1401); big-endian to the on-chain
        // `uint256` shape.
        let deadline = self.pull_through.unwrap_or(WINDOW_PULL_FALLBACK_DEADLINE);
        let namespace_id = U256::from_be_bytes(req.namespace_id);
        let target =
            match tokio::time::timeout(deadline, origin.open_pull_leg(hash, namespace_id)).await {
                Ok(Ok(target)) => target,
                // No upstream provider could be opened — a clean miss on THIS tier, or
                // a latched earlier-tier / local fault honored per #1129 / #1560 (a
                // walk that failed on our own broken buyer key is not evidence the blob
                // is absent).
                Ok(Err(miss)) => {
                    let reason = FillOutcome::miss_reason(fault_seen || miss.is_local_fault());
                    return self
                        .respond_error(&mut send, req, reason, rate_per_mb)
                        .await;
                }
                Err(_elapsed) => {
                    self.metrics.node_pull_through_timeout();
                    let reason = FillOutcome::miss_reason(fault_seen);
                    return self
                        .respond_error(&mut send, req, reason, rate_per_mb)
                        .await;
                }
            };
        let total_bytes = target.total_bytes;

        // (4) Size gate on the upstream-claimed total. (`open_pull_leg` already refuses
        // an oversized header via its `max_blob_size_bytes`; this is a belt-and-braces
        // wire-reason check.)
        if self.max_blob_size_bytes > 0 && total_bytes > self.max_blob_size_bytes {
            return self
                .respond_error(&mut send, req, ServeRejectReason::BlobTooLarge, rate_per_mb)
                .await;
        }

        // (5) Voucher-interval negotiation (ADR 003), then sign + send the response up
        // front — it commits to `total_bytes`, now known from the header handshake.
        let interval_mb = match ext.voucher_interval_mb {
            Some(proposed) => self.voucher_interval_mb.min(proposed).max(1),
            None => self.voucher_interval_mb,
        };
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

        // (6) The two decoupled legs (ADR 037). The SERVE
        // leg runs HERE on the accept task — it MUST be `Send` (the iroh
        // `ProtocolHandler::accept` bound), and it is (its cache streams are `Send`).
        // The PULL leg's `drive` is non-`Send`, so it runs OFF this task on a dedicated
        // current-thread runtime, coordinating only through the `Send + Sync`
        // `FillSession`: the shared PAID content frontier the pull's `WindowPacer` reads,
        // the captured-outboard the serve leg's coherent encoder reads, and the pull's
        // terminal signal the serve leg races so a pull that cannot fill a gap fails the
        // serve (no hang). `Notify` wakers + atomics are runtime-agnostic.
        let interval_bytes = interval_mb.saturating_mul(MB_BYTES).max(1);
        // The pacing window: at least `pull_ahead_bytes` (the ADR 037 upstream exposure
        // knob), the downstream `credit_window` (#1477), and one interval — the exact
        // bound the two-leg serve/pull driver paces against.
        let window = self
            .pull_ahead_bytes
            .as_ref()
            .map_or(decdn_common::config::DEFAULT_PULL_AHEAD_BYTES, |b| b.get())
            .max(interval_bytes)
            .max(self.credit_window(interval_bytes));

        // (6a) Atomically claim the fill (ADR 038): under one registry lock,
        // decide whether this miss OWNS a fresh pull for `hash` or ATTACHES as an
        // observer to a live one. Two concurrent same-hash misses therefore share ONE
        // upstream pull (no double spend, #305) while each keeps its own per-channel
        // voucher stream. `make_session` builds the shared `FillSession` and seeds its
        // PAID content frontier ONLY on the owner branch — it is a SYNC seed (no await
        // in the closure, which runs under the registry lock). The frontier is an
        // ABSOLUTE content offset, seeded to the request's content start
        // (`req.byte_offset`) so a non-zero-offset request does not show a window of
        // phantom lead and immediately `Wait`.
        let byte_offset = req.byte_offset;
        let byte_len = req.byte_len;
        let root = bao_tree::blake3::Hash::from(*hash.as_bytes());
        let claim = self
            .cache
            .claim_fill(hash, byte_offset, byte_len, total_bytes, || {
                let session = decdn_cache::FillSession::new(root, total_bytes);
                session
                    .served_frontier()
                    .store(byte_offset, Ordering::Relaxed);
                session
            });

        // Resolve the claim into a uniform shape: the session to serve from, the
        // optional local pull to drive for it (byte range), the sibling frontiers to
        // also pace (DECISION-B), and the leases to hold for the serve's lifetime.
        // `Owner` drives a pull for the whole request; `Mixed` drives one for only the
        // remainder and attaches a sibling for the overlap; `Attach` drives none. The
        // freshly-handshaked `target` feeds whichever pull we own (unused, a free
        // handshake spent, when we purely attach).
        let (serve_session, pull_range, also_pace, leases) = plan_serve(claim, req);

        // Seed the shared per-hash outboard with proof for held ranges no pull admits
        // (`seed_held_outboard`; idempotent). OUTSIDE the claim lock, so awaits are
        // safe. Every serve leg seeds, so its encoder is self-sufficient regardless of
        // owner/attach ordering.
        self.seed_held_outboard(hash, &serve_session).await;
        // The serve leg reads the cache the pull leg fills — same engine, same hash.
        let serve_store = decdn_cache::NodeRangedStore::new(self.cache.clone(), hash, total_bytes);

        // Spawn the off-task pull leg iff this claim owns one, on its own
        // current-thread runtime. All inputs are owned + `'static`; it shares only the
        // session Arc. Its cancel token is the SERVE session's — lease-driven, not a
        // fresh local token. The handle is PARKED on the session (`set_pull_handle`) so
        // whichever observer leaves LAST joins it (the owner-join hand-off, #1664),
        // freeing an owner whose own client finishes first from parking in the join.
        if let Some((pull_offset, pull_len)) = pull_range {
            let deps_lock = origin.deps_arc();
            let engine = self.cache.clone();
            let session = Arc::clone(&serve_session);
            let cancel = serve_session.cancel_token().clone();
            // Seed-leech cap (ADR 037): enforced in the pull leg's pacer. The served
            // client is the accounting key.
            let leech_governor = self.leech_governor.clone();
            let client_peer = client_node_id.0;
            let spawned = std::thread::Builder::new()
                .name("serve-miss-pull".to_string())
                .spawn(move || {
                    match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt.block_on(crate::node_origin::run_pull_leg(
                            deps_lock,
                            target,
                            engine,
                            hash,
                            pull_offset,
                            pull_len,
                            window,
                            Arc::clone(&session),
                            leech_governor,
                            client_peer,
                            cancel,
                        )),
                        Err(e) => {
                            // The pull could not start: record a terminal error and
                            // wake the serve leg so it fails a gap rather than hanging.
                            session.mark_ended(Err(decdn_cache::FillError::new(format!(
                                "serve-miss pull runtime build failed: {e}"
                            ))));
                        }
                    }
                });
            match spawned {
                Ok(handle) => serve_session.set_pull_handle(handle),
                Err(e) => {
                    // The OS refused the thread: fail any attached observer fast
                    // (`mark_ended`), release every lease (no handle parked, so nothing
                    // to join), and fail the serve.
                    serve_session.mark_ended(Err(decdn_cache::FillError::new(format!(
                        "could not spawn serve-miss pull thread: {e}"
                    ))));
                    for lease in leases {
                        let _ = lease.release();
                    }
                    return Err(anyhow::anyhow!(
                        "could not spawn serve-miss pull thread: {e}"
                    ));
                }
            }
        }

        // Run the serve leg on THIS (accept) task and await it. It owns termination.
        // It mints its outboard reader from the shared per-hash outboard and reads the
        // registry-wide fill liveness for the no-hang guarantee, pacing every pull it
        // draws from (its own plus any attached sibling).
        let serve_result = self
            .serve_leg(
                &mut send,
                &mut recv,
                serve_store,
                Arc::clone(&serve_session),
                &also_pace,
                &channel,
                hash,
                channel_id,
                client_node_id,
                rate_per_mb,
                interval_mb,
                req.byte_offset,
                req.byte_len,
                total_bytes,
                window,
            )
            .await;

        // Teardown (LEASE-driven, #1610): release every lease. A last-out release
        // cancels the pull (if still running) and hands back the session's parked
        // pull-thread handle for THIS caller to join off-task — never under the map
        // lock, never by cancelling the token directly. A non-last-out release returns
        // at once, leaving the pull filling for the remaining observers. At most one
        // handle comes back per lease (our own pull, plus a coalesced sibling's pull if
        // we are its last observer); each `SettleOnDrop` persists the buyer watermark
        // (#852). Join each off the async worker via `spawn_blocking`.
        let mut to_join = Vec::new();
        for lease in leases {
            if let Some(handle) = lease.release() {
                to_join.push(handle);
            }
        }
        for handle in to_join {
            let _ = tokio::task::spawn_blocking(move || {
                let _ = handle.join();
            })
            .await;
        }
        serve_result
    }

    /// Serve a cache miss from the node's OWN configured fs/http/s3 origin by
    /// running the two decoupled serve-miss legs — the LOCAL twin
    /// of [`Self::serve_via_window_pull_through`] with every paid-upstream axis
    /// stripped. The local pull leg fetches + verifies + stores each missing range
    /// straight out of this node's origin ([`decdn_cache::CacheEngine::origin_encode_range`]
    /// behind a [`crate::node_origin::BackendSource`]) while the serve leg streams
    /// the filling cache to the paying client; there is no counterparty, no channel,
    /// and no payment on the ingest side, so no discovery, no `PeerSource`, no
    /// `NodeFunder`, and no upstream counterparty.
    ///
    /// Whole-blob only (`byte_offset == 0 && byte_len == 0`): dispatch gates it
    /// there, and `total_bytes` is the origin-probe size the caller already
    /// confirmed serviceable (`origin_size` + a published `{H}.obao4` outboard). The
    /// caller has proven channel ownership (`pull_authorized`). Terminal: consumes
    /// `send`/`recv`.
    ///
    /// Like the peer twin, this claims the fill itself (`CacheEngine::claim_fill`)
    /// after signing the response: it either OWNS a fresh local pull for `hash` or
    /// ATTACHES as an observer to a live same-hash fill (any source). Two concurrent
    /// whole-blob own-origin misses therefore drive ONE origin fetch — the owner pulls
    /// from origin while the attaching observer streams the same filling cache to its
    /// own client — so the node eats the S3 egress once, not twice (a real dollar
    /// saving). Range-aware own-origin de-dup is a deferred follow-up; the registry is
    /// range-aware already, so nothing changes when own-origin gains partial serving.
    ///
    /// `fault_seen` carries whether an EARLIER tier (the reactive local-origin
    /// populate) hit a backend fault for this request (#1129), so the leech shed
    /// reports `InternalError` rather than a bare `CacheMiss` when this node is
    /// degraded. The ADR 011 open-time deny gates are already discharged (a
    /// denylisted hash is refused before the availability check; this branch is
    /// entered only behind `pull_authorized`, which refuses a blacklisted funding
    /// origin). A takedown that lands mid-stream is caught per interval inside
    /// [`Self::serve_leg`].
    ///
    /// Faults are scored LOCAL (never against a provider — there is none): the serve
    /// leg and `run_local_pull_leg` own their own scoring, so this orchestration
    /// signs nothing about providers.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn serve_via_backend_origin(
        &self,
        mut send: SendStream,
        mut recv: RecvStream,
        req: &StreamRequest,
        ext: &StreamRequestExt,
        hash: Hash,
        client_node_id: B256,
        total_bytes: u64,
        fault_seen: bool,
        rate_per_mb: u64,
    ) -> anyhow::Result<()> {
        // Mark that the own-origin serve-miss tier fired for this request, before
        // any admission guard below — the tier-selection signal (#1130), not a
        // success signal; an early reject still counts as
        // this tier having been entered.
        self.metrics.local_outboard_serve();

        // Resolve the owning channel (existence + ownership already proven by
        // `pull_authorized`) — needed for the deposit guard and the downstream
        // voucher collection.
        let channel_id = ChannelId::from(req.channel_id);
        let Some(channel) = self.channels.lock().await.get(&channel_id).cloned() else {
            return self
                .respond_error(
                    &mut send,
                    req,
                    ServeRejectReason::UnknownChannel,
                    rate_per_mb,
                )
                .await;
        };

        // (1) Pre-flight deposit guard: refuse the serve if the channel provably
        // cannot pay the downstream cost. Same ceiling logic as the peer twin
        // (`serve_via_window_pull_through`): a finite `max_blob_size_bytes` is the
        // worst-case whole-blob cost; an unbounded cap falls back to the per-request
        // window (`pull_ahead_bytes` floored at one interval, widened to the credit
        // window). The local path fronts no UPSTREAM spend, but the DOWNSTREAM
        // credit-window egress is billed per voucher exactly as the peer path, so the
        // guard is kept identical rather than loosened — a near-empty channel must
        // still be refused before the serve leg streams a window ahead of payment.
        let guard_bytes = if self.max_blob_size_bytes > 0 {
            self.max_blob_size_bytes
        } else {
            let interval_bytes = self.voucher_interval_mb.saturating_mul(MB_BYTES).max(1);
            self.pull_ahead_bytes
                .as_ref()
                .map_or(decdn_common::config::DEFAULT_PULL_AHEAD_BYTES, |b| b.get())
                .max(interval_bytes)
                .max(self.credit_window(interval_bytes))
        };
        let ceiling = min_payment(guard_bytes, rate_per_mb);
        let (deposit, last_amount) = {
            let guard = channel.lock().await;
            (guard.state.deposit, guard.state.last_amount())
        };
        let headroom = deposit.saturating_sub(last_amount);
        if headroom < ceiling {
            self.log_deposit_refusal(channel_id, hash, headroom, ceiling);
            return self
                .respond_error(
                    &mut send,
                    req,
                    ServeRejectReason::InsufficientDeposit,
                    rate_per_mb,
                )
                .await;
        }

        // (2) Seed-leech admission: global unrecouped budget + per-peer share ratio.
        // `leech_admit` bumps its own pause metric on refusal. A shed here is a miss,
        // not a client fault, so it honors an earlier-tier fault (#1129).
        let peer = client_node_id.0;
        if !self.leech_admit(&peer) {
            let reason = FillOutcome::miss_reason(fault_seen);
            return self
                .respond_error(&mut send, req, reason, rate_per_mb)
                .await;
        }

        // (3) Size gate on the origin-claimed total (belt-and-braces: dispatch's
        // `origin_size` probe already produced `total_bytes`; refuse an oversized
        // blob with the wire-parity reason before signing anything).
        if self.max_blob_size_bytes > 0 && total_bytes > self.max_blob_size_bytes {
            return self
                .respond_error(&mut send, req, ServeRejectReason::BlobTooLarge, rate_per_mb)
                .await;
        }

        // (4) Voucher-interval negotiation (ADR 003), then sign + send the response
        // up front — it commits to `total_bytes`, which the caller already read from
        // the origin size probe.
        let interval_mb = match ext.voucher_interval_mb {
            Some(proposed) => self.voucher_interval_mb.min(proposed).max(1),
            None => self.voucher_interval_mb,
        };
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

        // (5) The two decoupled legs (ADR 037). Identical coordination
        // shape to the peer twin: the SERVE leg runs HERE on the accept task (it must
        // be `Send`, the iroh `ProtocolHandler::accept` bound, and it is); the LOCAL
        // pull leg's `drive` is non-`Send`, so it runs OFF this task on a dedicated
        // current-thread runtime, coordinating only through the `Send + Sync`
        // `FillSession` (the shared PAID frontier, the captured outboard, the pull's
        // terminal signal). #1610 — ingest only behind a waiting, paying client.
        let interval_bytes = interval_mb.saturating_mul(MB_BYTES).max(1);
        // The pacing window: at least `pull_ahead_bytes`, the downstream
        // `credit_window` (#1477), and one interval — the same bound the peer twin
        // computes.
        let window = self
            .pull_ahead_bytes
            .as_ref()
            .map_or(decdn_common::config::DEFAULT_PULL_AHEAD_BYTES, |b| b.get())
            .max(interval_bytes)
            .max(self.credit_window(interval_bytes));

        // (5a) Atomically claim the fill (ADR 038): under one registry lock,
        // OWN a fresh local pull for `hash` or ATTACH as an observer to a live same-hash
        // fill (any source — a peer pull and an own-origin pull for the same hash
        // coalesce, the byte fetched once). Two concurrent whole-blob own-origin misses
        // therefore drive ONE origin fetch. `make_session` builds the session and seeds
        // its PAID frontier ONLY on the owner branch, a SYNC seed (no await under the
        // registry lock). The frontier is an ABSOLUTE content offset seeded to the
        // request start (`req.byte_offset`) so it does not show a window of phantom lead.
        let byte_offset = req.byte_offset;
        let byte_len = req.byte_len;
        let root = bao_tree::blake3::Hash::from(*hash.as_bytes());
        let claim = self
            .cache
            .claim_fill(hash, byte_offset, byte_len, total_bytes, || {
                let session = decdn_cache::FillSession::new(root, total_bytes);
                session
                    .served_frontier()
                    .store(byte_offset, Ordering::Relaxed);
                session
            });

        // Resolve the claim into the uniform serve shape (twin of the peer path,
        // `plan_serve`): the session to serve from, the optional local pull range, the
        // sibling frontiers to also pace, and the leases to hold. `Owner` drives a
        // local origin pull for the whole request; `Mixed` for only the remainder,
        // attaching a sibling for the overlap; `Attach` drives none — the S3-egress
        // saving, since the origin is fetched once and the observer streams the cache
        // the owner fills.
        let (serve_session, pull_range, also_pace, leases) = plan_serve(claim, req);

        // Seed the shared per-hash outboard with proof for held ranges no pull admits.
        // OUTSIDE the claim lock; idempotent, so every serve leg is self-sufficient.
        self.seed_held_outboard(hash, &serve_session).await;
        // The serve leg reads the cache the pull leg fills — same engine, same hash.
        let serve_store = decdn_cache::NodeRangedStore::new(self.cache.clone(), hash, total_bytes);

        // Spawn the off-task local pull leg iff this claim owns one, on its own
        // current-thread runtime. All inputs are owned + `'static`; it shares only the
        // session Arc. Its cancel token is the SERVE session's — lease-driven. The
        // `BackendSource` carries a FRESH local bookkeeping `ChannelLedger` (seed ZERO)
        // that `run_local_pull_leg` reads back via `source.ledger()` and hands to
        // `drive` as the completion frontier (THE CRUX — a completion counter, never
        // payment).
        if let Some((pull_offset, pull_len)) = pull_range {
            let engine = self.cache.clone();
            let metrics = Arc::clone(&self.metrics);
            let session = Arc::clone(&serve_session);
            let cancel = serve_session.cancel_token().clone();
            let leech_governor = self.leech_governor.clone();
            let client_peer = client_node_id.0;
            let ledger = Arc::new(decdn_client_pull::ChannelLedger::new(
                decdn_client_pull::Cumulative::default(),
            ));
            let source = crate::node_origin::BackendSource::new(
                engine.clone(),
                *hash.as_bytes(),
                total_bytes,
                ledger,
            );
            let spawned = std::thread::Builder::new()
                .name("serve-miss-local-pull".to_string())
                .spawn(move || {
                    match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt.block_on(crate::node_origin::run_local_pull_leg(
                            metrics,
                            engine,
                            source,
                            hash,
                            pull_offset,
                            pull_len,
                            window,
                            total_bytes,
                            Arc::clone(&session),
                            leech_governor,
                            client_peer,
                            cancel,
                        )),
                        Err(e) => {
                            // The pull could not start: record a terminal error and
                            // wake the serve leg so it fails a gap rather than hanging.
                            session.mark_ended(Err(decdn_cache::FillError::new(format!(
                                "serve-miss local pull runtime build failed: {e}"
                            ))));
                        }
                    }
                });
            match spawned {
                // Park the handle so whichever observer leaves LAST joins it (#1664).
                Ok(handle) => serve_session.set_pull_handle(handle),
                Err(e) => {
                    // The OS refused the thread: fail any attached observer fast
                    // (`mark_ended`), release every lease (nothing parked to join), and
                    // fail the serve.
                    serve_session.mark_ended(Err(decdn_cache::FillError::new(format!(
                        "could not spawn serve-miss local pull thread: {e}"
                    ))));
                    for lease in leases {
                        let _ = lease.release();
                    }
                    return Err(anyhow::anyhow!(
                        "could not spawn serve-miss local pull thread: {e}"
                    ));
                }
            }
        }

        // Run the serve leg on THIS (accept) task and await it. It owns termination —
        // mid-stream takedown and client-disconnect are both handled inside it — and
        // paces every pull it draws from (its own plus any attached sibling).
        let serve_result = self
            .serve_leg(
                &mut send,
                &mut recv,
                serve_store,
                Arc::clone(&serve_session),
                &also_pace,
                &channel,
                hash,
                channel_id,
                client_node_id,
                rate_per_mb,
                interval_mb,
                req.byte_offset,
                req.byte_len,
                total_bytes,
                window,
            )
            .await;

        // Teardown (LEASE-driven, #1610): release every lease. A last-out release
        // cancels the pull (if still running) and hands back the session's parked
        // pull-thread handle for THIS caller to join off-task — a non-last-out release
        // returns at once, leaving the pull filling for the remaining observers. Join
        // each returned handle off the async worker; never cancel a token directly.
        let mut to_join = Vec::new();
        for lease in leases {
            if let Some(handle) = lease.release() {
                to_join.push(handle);
            }
        }
        for handle in to_join {
            let _ = tokio::task::spawn_blocking(move || {
                let _ = handle.join();
            })
            .await;
        }
        serve_result
    }

    /// Seed the shared fill session's outboard with proof nodes for ranges this
    /// node ALREADY holds. The pull leg captures only ranges it ADMITS, but a
    /// range-minimized serve-miss can start with held ranges (ADR 037 §held ranges
    /// read locally); their proof nodes are never admitted, so a coherent encoder
    /// that `load`s a held span's node would otherwise park until the pull ends and
    /// then fail "outboard node never captured". `outboard_pairs` over the present
    /// ranges emits exactly those nodes (plus the right-spine). Idempotent (capture
    /// overwrites the same bytes), so EVERY serve leg — the owner AND each attached
    /// observer — seeds the held-range proof its own encoder reads, rather than an
    /// observer depending on the owner's seed-before-spawn ordering. Best-effort.
    async fn seed_held_outboard(&self, hash: Hash, session: &Arc<decdn_cache::FillSession>) {
        if let Ok(present) = self.cache.present_ranges(hash).await
            && !present.chunk_ranges().is_empty()
            && let Ok(pairs) = self
                .cache
                .outboard_pairs(hash, present.chunk_ranges())
                .await
        {
            for (node, pair) in pairs {
                session.capture(node, pair);
            }
        }
    }
}

/// The uniform serve shape [`plan_serve`] resolves a [`decdn_cache::FillClaim`] into:
/// the session to serve `R` from, the byte range of the local pull to drive for it
/// (`None` when purely attaching), the sibling fill frontiers to also pace
/// (DECISION-B), and the observer leases to hold for the serve leg's lifetime.
type ServePlan = (
    Arc<decdn_cache::FillSession>,
    Option<(u64, u64)>,
    Vec<Arc<decdn_cache::FillSession>>,
    Vec<decdn_cache::ObserverLease>,
);

/// Resolve a [`decdn_cache::FillClaim`] into the uniform [`ServePlan`] both serve-miss
/// orchestrations execute.
///
/// - [`Owner`](decdn_cache::FillClaim::Owner) → serve from the owner, drive a pull for
///   the WHOLE request (`req.byte_offset, req.byte_len`), pace nothing else, hold the
///   owner lease.
/// - [`Mixed`](decdn_cache::FillClaim::Mixed) → serve from the remainder owner, drive
///   a pull for only the contiguous remainder, pace the attached sibling too (its pull
///   produces the overlap this serve leg also bills), hold both leases.
/// - [`Attach`](decdn_cache::FillClaim::Attach) → serve from the sibling, drive NO
///   pull, pace nothing else, hold the attach lease.
fn plan_serve(claim: decdn_cache::FillClaim, req: &StreamRequest) -> ServePlan {
    match claim {
        decdn_cache::FillClaim::Owner { session, lease } => (
            session,
            Some((req.byte_offset, req.byte_len)),
            Vec::new(),
            vec![lease],
        ),
        decdn_cache::FillClaim::Mixed {
            owner,
            owner_lease,
            attach,
            attach_lease,
            remainder_offset,
            remainder_len,
        } => (
            owner,
            Some((remainder_offset, remainder_len)),
            vec![attach],
            vec![owner_lease, attach_lease],
        ),
        decdn_cache::FillClaim::Attach { session, lease } => {
            (session, None, Vec::new(), vec![lease])
        }
    }
}
