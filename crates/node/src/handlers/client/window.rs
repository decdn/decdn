//! Window-paced pull-through serve path (#856, ADR 037).

use alloy::primitives::U256;

use super::dispatch::{aligned_span, range_out_of_bounds};

use crate::node_origin::{PrimeLeg, PullLegTarget};

use super::{
    Arc, B256, CHUNK_BYTES, ClientHandler, FillOutcome, FloorReservation, Hash, LaneDeliveryState,
    LaneKey, Mutex, NodeOrigin, RecvStream, SendStream, ServeRejectReason, StreamRequest,
    StreamResponseBody, WINDOW_PULL_FALLBACK_DEADLINE,
};

/// Release a miss leg's floor reservation on a refusal taken BEFORE the serve
/// loop runs — the pre-flight floor-`M` gate, the size gate, or an upstream that
/// refused the header handshake. Nothing was fronted upstream and no byte
/// was delivered, so [`FloorReservation::release_unspent`] frees the live
/// reservation promptly and marks the guard so its `Drop` is a clean no-op
/// (ADR 003 §Pool solvency). A no-op when no lane was known (no reservation was
/// taken).
fn release_reservation_unspent(reservation: Option<&FloorReservation>) {
    if let Some(reservation) = reservation {
        reservation.release_unspent();
    }
}

/// End an owning serve-miss for a hash the engine refuses once its held-range
/// seeding is done.
///
/// `seed_held_outboard` validates the held ranges. When they fail, the engine
/// quarantines the hash. A takedown can also land between dispatch and this
/// point. Either way the serve leg can never deliver the hash
/// (`covers_locally` refuses it). Spawning the pull would pay
/// upstream or origin egress for bytes that never go on the wire, so the owner
/// skips it. It fails the session so attached observers stop, releases every
/// lease, and frees the floor reservation, because no byte was delivered. The
/// response is already sent, so the serve fails mid-stream.
fn abort_withdrawn_fill(
    hash: Hash,
    serve_session: &decdn_cache::FillSession,
    leases: Vec<decdn_cache::ObserverLease>,
    floor_reservation: Option<&FloorReservation>,
) -> anyhow::Error {
    serve_session.mark_ended(Err(decdn_cache::FillError::new(
        "the hash is refused: withdrawn or denied during the serve-miss",
    )));
    for lease in leases {
        let _ = lease.release();
    }
    release_reservation_unspent(floor_reservation);
    anyhow::anyhow!("serve-miss for {hash}: the hash is refused after seeding; skipped the pull")
}

impl ClientHandler {
    /// Serve a cache miss by running the two decoupled serve-miss legs (#856, ADR
    /// 037): the pull leg fills the cache from upstream for only the missing ranges
    /// while the serve leg streams the filling cache to the paying client, pacing the
    /// upstream spend by the downstream's vouchers so per-request speculative
    /// exposure is bounded to the ramped credit window (#1669) rather than the
    /// whole blob. The caller has already proven channel ownership. The request may
    /// be whole-blob, bounded, or resumed: the serve leg clamps delivery to
    /// `[byte_offset, end)` and the pull leg fills only that span's missing chunk
    /// groups.
    /// This path claims
    /// the fill itself (`CacheEngine::claim_fill`) after signing the response, so
    /// two concurrent same-hash misses share ONE upstream pull (the owner drives
    /// it; the second attaches as an observer). Terminal: consumes `send`/`recv`.
    ///
    /// `fault_seen` carries whether an EARLIER tier (the reactive local-origin
    /// populate) hit a backend fault for this request (#1129). This path
    /// is the last tier, so both of its MISS exits — no openable provider, and the
    /// open deadline — refuse via [`FillOutcome::miss_reason`], reporting
    /// `InternalError` when this node is degraded rather than merely empty.
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
        hash: Hash,
        client_node_id: B256,
        lane_key: LaneKey,
        lane: &Arc<Mutex<LaneDeliveryState>>,
        origin: Arc<NodeOrigin>,
        pool_remaining: Option<U256>,
        fault_seen: bool,
        rate_per_mb: u64,
        floor_reservation: Option<FloorReservation>,
    ) -> anyhow::Result<super::outcome::ServeEnd> {
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
        // The owning lane is resolved and its ownership proven by the caller
        // (`pull_authorized` + the serve gate), and threaded in as `lane` /
        // `lane_key` for the downstream voucher collection.
        //
        // The floor-M guard, the response signature, and the serve/pull window
        // all price against the fixed voucher accounting interval.
        let chunk_bytes = CHUNK_BYTES;
        // The pre-flight reservation is the ramp floor — one chunk. In-
        // stream exposure is bounded by the ramped credit window, which the serve
        // loop and the pull-leg `RampPacer` both enforce (#1669).
        let credit_floor = self.credit_window(chunk_bytes, 0);

        // The pull leg paces in CONTENT bytes while the client pays in WIRE bytes,
        // and two chunk-group roundings sit between the two (see
        // `PULL_WINDOW_FLOOR`). Its window therefore floors one chunk HIGHER than
        // the reservation and the serve loop, which both meter in wire and need no
        // such allowance.
        let pacing_floor = credit_floor.max(decdn_client::PULL_WINDOW_FLOOR);

        // Pre-flight floor-M guard (shared-payment-pool model) — the pull-through
        // twin of the `dispatch.rs` direct-serve gate. Refuse the speculative pull
        // when the pool's on-chain remaining (`getPool.deposit − totalRedeemed`)
        // minus the refundable floor `M` can no longer cover the reserved floor,
        // so the node never fronts upstream USDC for a pool that cannot cover it.
        // The floor is one credit window, CAPPED BY THE REQUEST'S ALIGNED SPAN
        // when the request bounds itself — the same pricing `dispatch.rs` reserved,
        // so a request accepted there is never refused here for a sub-window span.
        // `pool_remaining` is the cached `getPool.remaining` threaded from the
        // serve gate; `None` (no pool-view, unknown pool, or a read fault) fails
        // open — the on-chain `redeem` is the backstop.
        let guard_bytes = if req.byte_len > 0 {
            aligned_span(req.byte_offset, req.byte_len, u64::MAX).min(credit_floor)
        } else {
            credit_floor
        };
        if let Some(remaining) = pool_remaining
            && !self.pool_remaining_covers_window(remaining, guard_bytes, rate_per_mb)
        {
            let headroom = remaining.saturating_sub(self.pool_min_remaining_deposit);
            self.log_deposit_refusal(
                B256::from(req.pool_id),
                hash,
                headroom,
                decdn_incentive::min_payment(guard_bytes, rate_per_mb),
            );
            release_reservation_unspent(floor_reservation.as_ref());
            return self
                .respond_error(
                    &mut send,
                    req,
                    ServeRejectReason::InsufficientDeposit,
                    rate_per_mb,
                )
                .await;
        }

        // (3) Learn the blob geometry. PEEK the in-flight fill registry first: if a
        // live pull for this hash already runs, it already knows `total_bytes` from
        // its own header handshake, so this miss can coalesce onto it and SKIP the
        // expensive discovery + channel-open + header handshake (`open_pull_leg`)
        // entirely. The peek is ADVISORY — the authoritative own-vs-attach decision
        // stays in the atomic `claim_fill` below. A concurrent last observer can
        // retire the peeked session between the peek and the claim, so `claim_fill`
        // can still return `Owner`; that branch opens the pull leg LATE (step 6b) —
        // it alone needs a `target`. The namespace (ADR 005 §Namespace routing)
        // drives the origin-directory fallback inside `discover` on a total DHT miss
        // and is threaded onto the node-to-node leg so a directory-discovered cold
        // origin's own pull-through gate resolves (#1401); big-endian to the on-chain
        // `uint256` shape.
        let deadline = self.pull_through.unwrap_or(WINDOW_PULL_FALLBACK_DEADLINE);
        let namespace_id = U256::from_be_bytes(req.namespace_id);
        // The first leg a pull of `[offset, +len)` opens, so the handshake can
        // open that leg instead of a whole-blob open it drops (#2063).
        let prime_for = |offset: u64, len: u64| {
            PrimeLeg::new(
                offset,
                len,
                self.credit_ramp_divisor,
                pacing_floor,
                self.credit_max,
            )
        };
        let mut target: Option<PullLegTarget> = None;
        let total_bytes = match self.cache.in_flight_total(hash) {
            Some(total) => total,
            None => {
                // No live fill to coalesce onto — handshake upstream to learn the
                // geometry and secure the pull target this miss will own. ONE discovery
                // shared by both legs, open-time candidate fallback preserved. The
                // claim is not made yet; with no live fill it expects to own the whole
                // request, so the handshake is cut for that pull (step 6a closes the
                // prime when the claim does not own it).
                match self
                    .open_pull_leg_bounded(
                        origin.as_ref(),
                        hash,
                        namespace_id,
                        deadline,
                        fault_seen,
                        Some(prime_for(req.byte_offset, req.byte_len)),
                    )
                    .await
                {
                    Ok(opened) => {
                        let total = opened.total_bytes;
                        target = Some(opened);
                        total
                    }
                    Err(reason) => {
                        release_reservation_unspent(floor_reservation.as_ref());
                        return self
                            .respond_error(&mut send, req, reason, rate_per_mb)
                            .await;
                    }
                }
            }
        };

        // Deliberately NO bounds gate on this total (#1895, deflation direction):
        // `total_bytes` is the peer's signed but unverified handshake value, so
        // refusing a bounded/resumed request against it with a client-attributable
        // `RangeNotSatisfiable` would let a holder UNDER-report a blob's size and
        // make every relay refuse valid ranges — the mirror of the inflation
        // attack the received-byte ceiling exists for. A total that genuinely
        // cannot satisfy the request surfaces in `serve_leg`'s own bounds check
        // as a stream fault attributed to the pull, never to the client. The
        // own-origin twin keeps its pre-signature gate: that total is this
        // node's own origin probe.

        // (4) No size gate on the CLAIMED total (#1895): `total_bytes` is the peer's
        // signed but unverified handshake value, so refusing on it would let a holder
        // inflate a small blob's size to make every finite-ceiling relay refuse to
        // pull/cache/serve while it monopolises the traffic. The `max_blob_size_bytes`
        // ceiling is enforced instead on the bytes that ACTUALLY arrive, inside the
        // pull leg's receive loop (`UpstreamPull::next_chunk`), which aborts the fill
        // once received bytes cross it. A lie is inert (it cannot produce bytes that
        // verify against the true root); an honest giant is streamed and paid for only
        // up to one ceiling before the fill aborts and the serve fails a gap.

        // (5) The signed `StreamResponse` commits to `total_bytes` (now known). It is
        // deferred to step (7), AFTER the fill is claimed — so a peeked geometry that
        // raced to `Owner` opens its pull leg (and can still cleanly refuse) before we
        // promise `ok: true`.

        // (6) The two decoupled legs (ADR 037). The SERVE
        // leg runs HERE on the accept task — it MUST be `Send` (the iroh
        // `ProtocolHandler::accept` bound), and it is (its cache streams are `Send`).
        // The PULL leg's `drive` is non-`Send`, so it runs OFF this task on a dedicated
        // current-thread runtime, coordinating only through the `Send + Sync`
        // `FillSession`: the shared PAID content frontier the pull's `WindowPacer` reads,
        // the captured-outboard the serve leg's coherent encoder reads, and the pull's
        // terminal signal the serve leg races so a pull that cannot fill a gap fails the
        // serve (no hang). `Notify` wakers + atomics are runtime-agnostic. `interval_bytes`
        // and the pacing `credit_floor` were resolved with the floor-M guard above.

        // (6a) Atomically claim the fill (ADR 038): under one registry lock,
        // decide whether this miss OWNS a fresh pull for `hash` or ATTACHES as an
        // observer to a live one. Two concurrent same-hash misses whose starts sit
        // at or behind the live fill's paid frontier therefore share ONE upstream
        // pull (no double spend, #305) while each keeps its own per-channel voucher
        // stream; a request AHEAD of that frontier owns its own pull instead
        // (#2062 — attaching would starve it behind the other client's payments),
        // at the cost of fetching the overlap twice (`fill_not_coalesced`,
        // decdn#2069 §4). `make_session` builds the shared `FillSession` only on an
        // owning branch (`Owner` or `Mixed`), with its PAID content frontier at the
        // request's ABSOLUTE content start (`req.byte_offset`), so a non-zero-offset
        // request does not show a window of phantom lead and immediately `Wait`.
        let byte_offset = req.byte_offset;
        let byte_len = req.byte_len;
        let root = bao_tree::blake3::Hash::from(*hash.as_bytes());
        let claim = self
            .cache
            .claim_fill(hash, byte_offset, byte_len, total_bytes, || {
                decdn_cache::FillSession::starting_at(root, total_bytes, byte_offset)
            });

        // Resolve the claim into a uniform shape: the session to serve from, the
        // optional local pull to drive for it (byte range), the sibling frontiers to
        // also pace, and the leases to hold for the serve's lifetime.
        // `Owner` drives a pull for the whole request; `Mixed` drives one for only the
        // remainder and attaches a sibling for the overlap; `Attach` drives none.
        let (serve_session, pull_range, also_pace, leases) = plan_serve(claim, req);
        // Only an owning claim pulls from a fresh frontier at its range's start; a
        // mixed claim's remainder paces behind the attached overlap, so its first
        // leg is not the one the handshake could predict.
        let owns = also_pace.is_empty();
        if let Some(target) = target.as_mut() {
            target.keep_prime_for(owns, pull_range);
        }

        // (6b) Owning-race repair: the advisory step-3 peek planned to ATTACH and
        // secured no `target`, but the peeked session retired before this claim, so the
        // atomic decision drives a fresh pull (`Owner`, or a `Mixed` remainder). Open the
        // pull leg now, BEFORE signing `ok: true` (step 7), so a no-provider / deadline
        // miss still refuses cleanly rather than committing to a stream it can't fill.
        if pull_range.is_some() && target.is_none() {
            let prime = pull_range
                .filter(|_| owns)
                .map(|(offset, len)| prime_for(offset, len));
            match self
                .open_pull_leg_bounded(
                    origin.as_ref(),
                    hash,
                    namespace_id,
                    deadline,
                    fault_seen,
                    prime,
                )
                .await
            {
                Ok(opened) => target = Some(opened),
                Err(reason) => {
                    release_reservation_unspent(floor_reservation.as_ref());
                    return self
                        .respond_error(&mut send, req, reason, rate_per_mb)
                        .await;
                }
            }
        }

        // (7) Sign + send the response now that the fill mechanism is secured for every
        // branch (attach: the owner fills; own/mixed: `target` is set by step 3 or 6b).
        // It commits to `total_bytes`, known from the peek or the header handshake.
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

        // Seed the shared per-hash outboard with proof for held ranges no pull admits
        // (`seed_held_outboard`; idempotent). OUTSIDE the claim lock, so awaits are
        // safe. Every serve leg seeds, so its encoder is self-sufficient regardless of
        // owner/attach ordering.
        self.seed_held_outboard(hash, &serve_session).await;
        if pull_range.is_some() && self.cache.refuses(hash) {
            return Err(abort_withdrawn_fill(
                hash,
                &serve_session,
                leases,
                floor_reservation.as_ref(),
            ));
        }
        // The serve leg reads the cache the pull leg fills — same engine, same hash.
        let serve_store = decdn_cache::NodeRangedStore::new(self.cache.clone(), hash, total_bytes);

        // Spawn the off-task pull leg iff this claim owns one, on its own
        // current-thread runtime. All inputs are owned + `'static`; it shares only the
        // session Arc. Its cancel token is the SERVE session's — lease-driven, not a
        // fresh local token. The handle is PARKED on the session (`set_pull_handle`) so
        // whichever observer leaves LAST joins it (the owner-join hand-off, #1664),
        // freeing an owner whose own client finishes first from parking in the join.
        if let Some((pull_offset, pull_len)) = pull_range {
            let Some(target) = target else {
                // Unreachable: step 6b secures a `target` for every owning claim (Owner
                // or Mixed remainder). Fail the serve rather than panic (anti-panic
                // policy) — the response is already sent, so a mid-stream error is the
                // honest outcome; carry the hash + namespace for production debugging.
                serve_session.mark_ended(Err(decdn_cache::FillError::new(
                    "serve-miss owning claim without a pull target",
                )));
                for lease in leases {
                    let _ = lease.release();
                }
                return Err(anyhow::anyhow!(
                    "serve-miss owning claim without a pull target (hash {hash}, namespace {namespace_id})"
                ));
            };
            let deps_lock = origin.deps_arc();
            let engine = self.cache.clone();
            let session = Arc::clone(&serve_session);
            let cancel = serve_session.cancel_token().clone();
            // The pull leg's `RampPacer` (#1669): the same ramp the serve loop
            // computes from its own paid frontier, so the pull never runs further
            // ahead of the downstream served-paid frontier than the ramped credit
            // window allows.
            let credit_ramp_divisor = self.credit_ramp_divisor;
            let credit_max = self.credit_max;
            // The pull runs on its own thread and runtime, which starts with no
            // span: open its span here, under the serve stream's, and enter it
            // there so the pull's spans and events stay in this trace.
            let pull_span = tracing::info_span!("serve_miss_pull", tier = "node", %hash);
            let spawned = std::thread::Builder::new()
                .name("serve-miss-pull".to_string())
                .spawn(move || {
                    let _entered = pull_span.enter();
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
                            credit_ramp_divisor,
                            pacing_floor,
                            credit_max,
                            Arc::clone(&session),
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
        //
        // The pool floor reservation (opened at the dispatch pre-spend gate) is OWNED
        // here and passed by reference: the serve leg releases it once the stream
        // repays one floor, exactly like the hit path. Holding ownership across the
        // `await` keeps the guard alive for the whole serve; its `Drop` frees the
        // pool's live floor headroom AFTER `serve_leg` returns, at this function's
        // scope end.
        let serve_result = self
            .serve_leg(
                &mut send,
                &mut recv,
                serve_store,
                Arc::clone(&serve_session),
                &also_pace,
                lane,
                hash,
                lane_key,
                client_node_id,
                rate_per_mb,
                req.byte_offset,
                req.byte_len,
                total_bytes,
                floor_reservation.as_ref(),
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
    /// straight out of this node's origin ([`decdn_cache::CacheEngine::origin_range_wire`]
    /// behind a [`crate::node_origin::BackendSource`]) while the serve leg streams
    /// the filling cache to the paying client; there is no counterparty, no channel,
    /// and no payment on the ingest side, so no discovery, no `PeerSource`, no
    /// `NodeFunder`, and no upstream counterparty.
    ///
    /// `total_bytes` is the origin-probe size the caller already confirmed
    /// serviceable (`origin_size` + a published `{H}.obao4` outboard). The
    /// request may be whole-blob, bounded, or resumed: the serve leg clamps delivery
    /// to `[byte_offset, end)` and the local pull leg fills only that span's missing
    /// chunk groups, so a bounded request pulls exactly its aligned span from
    /// origin. The caller has proven channel ownership (`pull_authorized`).
    /// Terminal: consumes `send`/`recv`.
    ///
    /// Like the peer twin, this claims the fill itself (`CacheEngine::claim_fill`)
    /// after signing the response: it either OWNS a fresh local pull for `hash` or
    /// ATTACHES as an observer to a live same-hash fill (any source). Two concurrent
    /// own-origin misses for overlapping spans therefore drive ONE origin fetch for
    /// the overlap — the owner pulls from origin while the attaching observer streams
    /// the same filling cache to its own client — so the node eats the S3 egress
    /// once, not twice (a real dollar saving).
    ///
    /// The ADR 011 open-time deny gates are already discharged (a denylisted hash
    /// is refused before the availability check; this branch is entered only
    /// behind `pull_authorized`, which refuses a blacklisted funding origin). A
    /// takedown that lands mid-stream is caught per interval inside
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
        hash: Hash,
        client_node_id: B256,
        lane_key: LaneKey,
        lane: &Arc<Mutex<LaneDeliveryState>>,
        total_bytes: u64,
        pool_remaining: Option<U256>,
        rate_per_mb: u64,
        floor_reservation: Option<FloorReservation>,
    ) -> anyhow::Result<super::outcome::ServeEnd> {
        // Mark that the own-origin serve-miss tier fired for this request, before
        // any admission guard below — the tier-selection signal (#1130), not a
        // success signal; an early reject still counts as
        // this tier having been entered.
        self.metrics.local_outboard_serve();

        // The owning lane is resolved and its ownership proven by the caller;
        // `lane` / `lane_key` are threaded in for the downstream voucher
        // collection.
        //
        // The floor-M guard, the response signature, and the serve/pull window
        // price against the fixed voucher accounting interval.
        let chunk_bytes = CHUNK_BYTES;
        // The pre-flight reservation is the ramp floor — one chunk, the
        // same bound the peer twin computes. In-stream exposure is bounded by the
        // ramped credit window, which the serve loop and the pull-leg `RampPacer`
        // both enforce (#1669).
        let credit_floor = self.credit_window(chunk_bytes, 0);

        // The pull leg paces in CONTENT bytes while the client pays in WIRE bytes,
        // and two chunk-group roundings sit between the two (see
        // `PULL_WINDOW_FLOOR`). Its window therefore floors one chunk HIGHER than
        // the reservation and the serve loop, which both meter in wire and need no
        // such allowance.
        let pacing_floor = credit_floor.max(decdn_client::PULL_WINDOW_FLOOR);

        // Pre-flight floor-M guard (shared-payment-pool model) — the own-origin
        // twin of the peer path and of `dispatch.rs`. Refuse the serve when the
        // pool's on-chain remaining minus the refundable floor `M` cannot cover
        // the reserved floor. `pool_remaining` is the cached `getPool.remaining`
        // threaded from the serve gate; `None` fails open (on-chain `redeem` is
        // the backstop). The own-origin leg fronts no upstream USDC, but delivery
        // is billed per voucher, so a pool that cannot cover the floor is refused
        // here for wire-parity with the peer path rather than served for free.
        //
        // (2) Bounds gate first, on the probed geometry, BEFORE the guard and the
        // signature: an offset at or past the end, or an end past the blob, is
        // `RangeNotSatisfiable` here exactly as on the direct-serve path — never
        // `InsufficientDeposit` (the range, not the pool, is the problem), and
        // never a signed `ok: true` that turns a bad range into a stream failure.
        // This total is the node's OWN origin probe, so unlike the peer twin the
        // gate cannot be steered by a lying counterparty (#1895).
        if range_out_of_bounds(req.byte_offset, req.byte_len, total_bytes) {
            release_reservation_unspent(floor_reservation.as_ref());
            return self
                .respond_error(
                    &mut send,
                    req,
                    ServeRejectReason::RangeNotSatisfiable,
                    rate_per_mb,
                )
                .await;
        }

        // The floor is capped by the request's aligned span — priced against the
        // PROBED total (in hand here, unlike dispatch's pre-fill reservation), so
        // a resumed tail near the blob end span-caps too, and a request accepted
        // at dispatch is never refused here for a sub-window span. In bounds per
        // the gate above, so `aligned_span` never saturates.
        let guard_bytes =
            aligned_span(req.byte_offset, req.byte_len, total_bytes).min(credit_floor);
        if let Some(remaining) = pool_remaining
            && !self.pool_remaining_covers_window(remaining, guard_bytes, rate_per_mb)
        {
            let headroom = remaining.saturating_sub(self.pool_min_remaining_deposit);
            self.log_deposit_refusal(
                B256::from(req.pool_id),
                hash,
                headroom,
                decdn_incentive::min_payment(guard_bytes, rate_per_mb),
            );
            release_reservation_unspent(floor_reservation.as_ref());
            return self
                .respond_error(
                    &mut send,
                    req,
                    ServeRejectReason::InsufficientDeposit,
                    rate_per_mb,
                )
                .await;
        }

        // (3) No size gate on the origin-claimed total (#1895): this own-origin leg
        // has no untrusted counterparty, but the ceiling still binds on RECEIVED bytes
        // for wire-parity with the peer path. Its local pull leg's admission enforces
        // the engine cap on the bytes it actually stores, so an over-ceiling origin
        // blob fails the fill there rather than being refused up front on the probe
        // size. Keeping both legs' ceiling-on-received keeps `max_blob_size_mb` one
        // consistent rule across the serve-miss tiers.

        // (4) Sign + send the response up front — it commits to `total_bytes`,
        // which the caller already read from the origin size probe, and to the
        // interval negotiated above.
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

        // (5) The two decoupled legs (ADR 037). Identical coordination
        // shape to the peer twin: the SERVE leg runs HERE on the accept task (it must
        // be `Send`, the iroh `ProtocolHandler::accept` bound, and it is); the LOCAL
        // pull leg's `drive` is non-`Send`, so it runs OFF this task on a dedicated
        // current-thread runtime, coordinating only through the `Send + Sync`
        // `FillSession` (the shared PAID frontier, the captured outboard, the pull's
        // terminal signal). #1610 — ingest only behind a waiting, paying client.
        // `interval_bytes` and the pacing `credit_floor` were resolved with the floor-M
        // guard above.

        // (5a) Atomically claim the fill (ADR 038): under one registry lock,
        // OWN a fresh local pull for `hash` or ATTACH as an observer to a live same-hash
        // fill (any source — a peer pull and an own-origin pull for the same hash
        // coalesce, the byte fetched once). Two concurrent own-origin misses for
        // overlapping spans therefore drive ONE origin fetch for the overlap. `make_session` builds the session only on an
        // owning branch (`Owner` or `Mixed`), with its PAID frontier at the request's
        // ABSOLUTE content start (`req.byte_offset`) so it shows no phantom lead.
        let byte_offset = req.byte_offset;
        let byte_len = req.byte_len;
        let root = bao_tree::blake3::Hash::from(*hash.as_bytes());
        let claim = self
            .cache
            .claim_fill(hash, byte_offset, byte_len, total_bytes, || {
                decdn_cache::FillSession::starting_at(root, total_bytes, byte_offset)
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
        if pull_range.is_some() && self.cache.refuses(hash) {
            return Err(abort_withdrawn_fill(
                hash,
                &serve_session,
                leases,
                floor_reservation.as_ref(),
            ));
        }
        // The serve leg reads the cache the pull leg fills — same engine, same hash.
        let serve_store = decdn_cache::NodeRangedStore::new(self.cache.clone(), hash, total_bytes);

        // Spawn the off-task local pull leg iff this claim owns one, on its own
        // current-thread runtime. All inputs are owned + `'static`; it shares only the
        // session Arc. Its cancel token is the SERVE session's — lease-driven. The
        // `BackendSource` carries a FRESH local bookkeeping `PoolLedger` (seed ZERO)
        // that `run_local_pull_leg` reads back via `source.ledger()` and hands to
        // `drive` as the completion frontier (THE CRUX — a completion counter, never
        // payment).
        if let Some((pull_offset, pull_len)) = pull_range {
            let engine = self.cache.clone();
            let metrics = Arc::clone(&self.metrics);
            let session = Arc::clone(&serve_session);
            let cancel = serve_session.cancel_token().clone();
            let credit_ramp_divisor = self.credit_ramp_divisor;
            let credit_max = self.credit_max;
            // The unpaid local-origin leg: a throwaway ledger that never signs
            // and never meters, because `BackendSource` quotes rate 0.
            let ledger = Arc::new(decdn_client::PoolLedger::new(
                decdn_client::Cumulative::default(),
            ));
            let source = crate::node_origin::BackendSource::new(
                engine.clone(),
                *hash.as_bytes(),
                total_bytes,
                ledger,
            );
            // The pull runs on its own thread and runtime, which starts with no
            // span: open its span here, under the serve stream's, and enter it
            // there so the pull's spans and events stay in this trace.
            let pull_span = tracing::info_span!("serve_miss_pull", tier = "local", %hash);
            let spawned = std::thread::Builder::new()
                .name("serve-miss-local-pull".to_string())
                .spawn(move || {
                    let _entered = pull_span.enter();
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
                            credit_ramp_divisor,
                            pacing_floor,
                            credit_max,
                            total_bytes,
                            Arc::clone(&session),
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
        //
        // The pool floor reservation (opened at the dispatch pre-spend gate) is OWNED
        // here and passed by reference so the serve leg releases it exactly like the
        // hit path once the stream repays its floor. Holding ownership across the
        // `await` keeps the guard alive for the whole serve; its `Drop` frees the
        // pool's live floor headroom AFTER `serve_leg` returns, at this function's
        // scope end.
        let serve_result = self
            .serve_leg(
                &mut send,
                &mut recv,
                serve_store,
                Arc::clone(&serve_session),
                &also_pace,
                lane,
                hash,
                lane_key,
                client_node_id,
                rate_per_mb,
                req.byte_offset,
                req.byte_len,
                total_bytes,
                floor_reservation.as_ref(),
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

    /// Discover + open a bounded upstream pull leg for `hash`, classifying every
    /// failure into the wire reject reason the caller must respond with. Factored out
    /// so [`Self::serve_via_window_pull_through`] can open the leg from TWO points —
    /// up front on a cold miss (step 3), or late when a peeked fill retired between
    /// the advisory peek and the atomic `claim_fill` (step 6b) — without duplicating
    /// the timeout + [`crate::node_origin::PullMiss`] classification.
    ///
    /// `Ok(target)` is the bound leg (its `total_bytes` is the header-handshaked blob
    /// length). `Err(reason)` is:
    /// - a clean miss on THIS tier, or a latched earlier-tier / local fault honored
    ///   per #1129 / #1560 (a walk that failed on our own broken buyer key is not
    ///   evidence the blob is absent) — [`FillOutcome::miss_reason`] of
    ///   `fault_seen || miss.is_local_fault()`;
    /// - the pull-through deadline elapsing (a slow/absent upstream must not pin the
    ///   stream) — the timeout metric fires and the reason is `miss_reason(fault_seen)`.
    ///
    /// `prime` is the pull this miss expects to own, when it can say: the handshake
    /// then opens that pull's first leg for the pull leg to adopt (#2063).
    async fn open_pull_leg_bounded(
        &self,
        origin: &NodeOrigin,
        hash: Hash,
        namespace_id: U256,
        deadline: std::time::Duration,
        fault_seen: bool,
        prime: Option<PrimeLeg>,
    ) -> Result<PullLegTarget, ServeRejectReason> {
        match tokio::time::timeout(deadline, origin.open_pull_leg(hash, namespace_id, prime)).await
        {
            Ok(Ok(target)) => Ok(target),
            Ok(Err(miss)) => Err(FillOutcome::miss_reason(
                fault_seen || miss.is_local_fault(),
            )),
            Err(_elapsed) => {
                self.metrics.node_pull_through_timeout();
                Err(FillOutcome::miss_reason(fault_seen))
            }
        }
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
            // Seed the whole held-range proof under one wake, not one per node.
            session.capture_many(pairs);
        }
    }
}

/// The uniform serve shape [`plan_serve`] resolves a [`decdn_cache::FillClaim`] into:
/// the session to serve `R` from, the byte range of the local pull to drive for it
/// (`None` when purely attaching), the sibling fill frontiers to also pace (the
/// pull paces on the furthest paid frontier), and the observer leases to hold
/// for the serve leg's lifetime.
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
