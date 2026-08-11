//! Window-paced pull-through serve path (#856, ADR 037).
//! Bodies split from `mod.rs` (#1254).

use std::sync::atomic::AtomicU64;

use alloy::primitives::U256;
use tokio::sync::Notify;

use super::{
    Arc, B256, ChannelId, ClientHandler, ClientMessage, FillOutcome, Hash, MB_BYTES, NodeOrigin,
    RecvStream, SendStream, ServeRejectReason, StreamRequest, StreamRequestExt, StreamResponseBody,
    TeeReservation, WINDOW_PULL_FALLBACK_DEADLINE, min_payment,
};

impl ClientHandler {
    /// Serve a cache miss by fusing a window-paced upstream pull with downstream
    /// delivery (#856, ADR 037): forward each upstream chunk to the paying client
    /// and tee it into the cache, pacing the upstream spend by the downstream's
    /// vouchers so per-request speculative exposure is bounded to
    /// `pull_ahead_bytes` rather than the whole blob. The caller has already
    /// proven channel ownership, confirmed `byte_offset == 0`, and claimed the
    /// tee sink. Terminal: consumes `send`/`recv`.
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
        tee: TeeReservation,
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
            tee.abandon();
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
            tee.abandon();
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
            tee.abandon();
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
                    tee.abandon();
                    let reason = FillOutcome::miss_reason(fault_seen || miss.is_local_fault());
                    return self
                        .respond_error(&mut send, req, reason, rate_per_mb)
                        .await;
                }
                Err(_elapsed) => {
                    tee.abandon();
                    self.metrics.node_pull_through_timeout();
                    let reason = FillOutcome::miss_reason(fault_seen);
                    return self
                        .respond_error(&mut send, req, reason, rate_per_mb)
                        .await;
                }
            };
        let total_bytes = target.total_bytes;

        // (4) Size gate on the upstream-claimed total. (`open_pull_leg` already refuses
        // an oversized header via its `max_blob_size_bytes`; this is the belt-and-braces
        // wire-reason parity with the old path.)
        if self.max_blob_size_bytes > 0 && total_bytes > self.max_blob_size_bytes {
            tee.abandon();
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

        // (6) The two decoupled legs (ADR 037, #1621 B2 part 2, Strategy B). The SERVE
        // leg runs HERE on the accept task — it MUST be `Send` (the iroh
        // `ProtocolHandler::accept` bound), and it is (its cache streams are `Send`).
        // The PULL leg's `drive` is non-`Send` (its `IngestStore` fill is, deliberately,
        // Task 6), so it runs OFF this task on a dedicated current-thread runtime,
        // coordinating only through `Send + Sync` shared state:
        //   - `served_paid` — the client's PAID content frontier; the serve leg stores
        //     it, the pull leg's `WindowPacer` reads it to bound `pulled − served_paid`.
        //   - `served_paid_advanced` — notified on each advance, so a parked pull
        //     re-decides exactly when payment clears.
        //   - `pull_ended` + `pull_result` — the pull leg records its terminal outcome
        //     then fires the notify; the serve leg races it against the present-range
        //     watch so a pull that could not fill a gap fails the serve (no hang).
        // `Notify` wakers + atomics are runtime-agnostic, so this coordination crosses
        // the two runtimes safely; the cache-store actor and iroh endpoint are reached
        // through their own channels. When the serve leg returns, the token is cancelled
        // and the pull thread JOINED — never detached: an orphan pull would keep paying
        // upstream for a blob no client waits on (#1610).
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

        // The PAID content frontier the pull leg's `WindowPacer` bounds against
        // (`pulled_frontier − served_paid_frontier ≤ window`). `pulled_frontier` is an
        // ABSOLUTE content offset, so seed this to the request's content start —
        // `req.byte_offset` — not a bare 0, or a non-zero-offset request would show a
        // full window of phantom lead and immediately `Wait`/stall. Dispatch currently
        // gates this path to `byte_offset == 0` (so this is 0 today), but the
        // absolute-frontier invariant is made explicit here rather than relying on that.
        let served_paid = Arc::new(AtomicU64::new(req.byte_offset));
        let served_paid_advanced = Arc::new(Notify::new());
        let pull_ended = Arc::new(Notify::new());
        let pull_result: Arc<std::sync::Mutex<Option<anyhow::Result<()>>>> =
            Arc::new(std::sync::Mutex::new(None));
        let cancel = tokio_util::sync::CancellationToken::new();

        // The whole-tree bao outboard the two legs share (#1621 B2 part 2, ADR 038):
        // the pull leg captures each admitted range's proof nodes into `ob_writer`,
        // and the serve leg's coherent whole-range encoder reads them through a reader
        // minted from `ob_factory`. The pull can start capturing before the serve leg
        // wires its reader, which is why construction and reader-minting are split.
        let (ob_writer, ob_factory) = crate::node_origin::shared_outboard(
            bao_tree::blake3::Hash::from(*hash.as_bytes()),
            total_bytes,
        );
        // Seed the outboard with proof nodes for ranges B ALREADY holds. The pull leg
        // only captures ranges it ADMITS, but a range-minimized serve-miss can start
        // with bytes already present (ADR 037 §held ranges read locally) — without
        // this seed the coherent encoder would `load` a held span's proof node that
        // was never captured, park until `pull_ended`, then fail "outboard node never
        // captured". `outboard_pairs` over the present ranges emits exactly those
        // nodes (plus the right-spine). Best-effort: on a full miss `present` is empty
        // and this is a no-op; a seed error just leaves those nodes for the pull to
        // re-capture as it re-verifies.
        if let Ok(present) = self.cache.present_ranges(hash).await
            && !present.chunk_ranges().is_empty()
            && let Ok(pairs) = self
                .cache
                .outboard_pairs(hash, present.chunk_ranges())
                .await
        {
            for (node, pair) in pairs {
                ob_writer.save(node, pair);
            }
        }

        // The serve leg reads the cache the pull leg fills — same engine, same hash.
        let serve_store = decdn_cache::NodeRangedStore::new(self.cache.clone(), hash, total_bytes);

        // Spawn the off-task pull leg on its own current-thread runtime. All inputs are
        // owned + `'static`; it shares only the coordination Arcs above.
        let pull_thread = {
            let deps_lock = origin.deps_arc();
            let engine = self.cache.clone();
            let served_paid = Arc::clone(&served_paid);
            let served_paid_advanced = Arc::clone(&served_paid_advanced);
            let pull_ended = Arc::clone(&pull_ended);
            let pull_result = Arc::clone(&pull_result);
            let cancel = cancel.clone();
            let offset = req.byte_offset;
            let len = req.byte_len;
            let outboard_writer = ob_writer;
            // Seed-leech cap (ADR 037): re-homed into the pull leg's pacer. The served
            // client is the accounting key.
            let leech_governor = self.leech_governor.clone();
            let client_peer = client_node_id.0;
            std::thread::Builder::new()
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
                            offset,
                            len,
                            window,
                            served_paid,
                            served_paid_advanced,
                            Arc::clone(&pull_ended),
                            pull_result.clone(),
                            outboard_writer,
                            leech_governor,
                            client_peer,
                            cancel,
                        )),
                        Err(e) => {
                            // The pull could not start: record a terminal error and wake
                            // the serve leg so it fails a gap rather than hanging.
                            if let Ok(mut guard) = pull_result.lock() {
                                *guard = Some(Err(anyhow::anyhow!(
                                    "serve-miss pull runtime build failed: {e}"
                                )));
                            }
                            pull_ended.notify_waiters();
                        }
                    }
                })
        };
        let pull_thread = match pull_thread {
            Ok(handle) => handle,
            Err(e) => {
                // The OS refused the thread: fail the serve cleanly (release the tee).
                tee.abandon();
                return Err(anyhow::anyhow!(
                    "could not spawn serve-miss pull thread: {e}"
                ));
            }
        };

        // Mint the serve leg's outboard reader from the shared factory, bound to the
        // pull's terminal signals for the coherent encoder's no-hang guarantee.
        let outboard_reader = ob_factory.reader(Arc::clone(&pull_ended), Arc::clone(&pull_result));

        // Run the serve leg on THIS (accept) task and await it. It owns termination.
        let serve_result = self
            .serve_leg(
                &mut send,
                &mut recv,
                serve_store,
                outboard_reader,
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
                Arc::clone(&served_paid),
                Arc::clone(&served_paid_advanced),
                Arc::clone(&pull_ended),
                Arc::clone(&pull_result),
            )
            .await;

        // Teardown: cancel the pull (stop paying upstream for a blob the client no
        // longer waits on, #1610), then JOIN — bounded, since cancellation makes
        // `drive` drop promptly and the pull thread's `SettleOnDrop` persists the buyer
        // watermark (#852). Joined off the async worker via `spawn_blocking`.
        cancel.cancel();
        let _ = tokio::task::spawn_blocking(move || {
            let _ = pull_thread.join();
        })
        .await;

        // Release the in-flight coalescing slot: the `TeeReservation` was held only as
        // the double-pull guard (the pull leg fills via the cache, not the tee), and its
        // `abandon` fires the `Notify` any coalesced `InFlight` waiter is parked on
        // (engine.rs:3975/3987 — the reservation's own teardown wakes waiters).
        tee.abandon();
        serve_result
    }

    /// Serve a cache miss from the node's OWN configured fs/http/s3 origin by
    /// running the two decoupled serve-miss legs (Flow A, FA.3a) — the LOCAL twin
    /// of [`Self::serve_via_window_pull_through`] with every paid-upstream axis
    /// stripped. The local pull leg fetches + verifies + stores each missing range
    /// straight out of this node's origin ([`decdn_cache::CacheEngine::origin_encode_range`]
    /// behind a [`crate::node_origin::BackendSource`]) while the serve leg streams
    /// the filling cache to the paying client; there is no counterparty, no channel,
    /// and no payment on the ingest side, so no discovery, no `PeerSource`, no
    /// `NodeFunder`, and no upstream tee.
    ///
    /// Whole-blob only (`byte_offset == 0 && byte_len == 0`): dispatch gates it
    /// there, and `total_bytes` is the origin-probe size the caller already
    /// confirmed serviceable (`origin_size` + a published `{H}.obao4` outboard). The
    /// caller has proven channel ownership (`pull_authorized`). Terminal: consumes
    /// `send`/`recv`.
    ///
    /// The one structural simplification vs the peer twin: there is NO
    /// [`TeeReservation`] to hold or abandon. The peer path holds the tee only as a
    /// double-pull coalescing guard; the local pull fills exclusively via the cache
    /// (`NodeAdmitStore`), never a tee, so nothing here claims or releases one. B3
    /// (a later PR) owns range-aware coalescing for own-origin misses — until then a
    /// second concurrent whole-blob own-origin miss simply opens its own local pull,
    /// which is correct if wasteful, matching how the peer path already behaves.
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
        // any admission guard below — the tier-selection signal (#1130, reused
        // unchanged by Flow A), not a success signal; an early reject still counts as
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

        // (5) The two decoupled legs (ADR 037, Flow A FA.3a). Identical coordination
        // shape to the peer twin: the SERVE leg runs HERE on the accept task (it must
        // be `Send`, the iroh `ProtocolHandler::accept` bound, and it is); the LOCAL
        // pull leg's `drive` is non-`Send` (its `IngestStore` fill is deliberately
        // non-`Send`), so it runs OFF this task on a dedicated current-thread runtime,
        // coordinating only through `Send + Sync` shared state:
        //   - `served_paid` — the client's PAID content frontier; the serve leg stores
        //     it, the pull leg's `WindowPacer` reads it to bound `pulled − served_paid`
        //     (#1610 — ingest only behind a waiting, paying client — plus the
        //     storage/egress exposure bound).
        //   - `served_paid_advanced` — notified on each advance so a parked pull
        //     re-decides exactly when payment clears.
        //   - `pull_ended` + `pull_result` — the pull leg records its terminal outcome
        //     then fires the notify; the serve leg races it against the present-range
        //     watch so a pull that could not fill a gap fails the serve (no hang).
        // When the serve leg returns, the token is cancelled and the pull thread
        // JOINED — never detached (an orphan local pull would keep draining our own
        // origin for a blob no client waits on, #1610).
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

        // The PAID content frontier the pull leg's `WindowPacer` bounds against.
        // `pulled_frontier` is an ABSOLUTE content offset, so seed this to the
        // request's content start (`req.byte_offset`) rather than a bare 0 — dispatch
        // gates this path to `byte_offset == 0` today, but the absolute-frontier
        // invariant is made explicit here rather than relying on that.
        let served_paid = Arc::new(AtomicU64::new(req.byte_offset));
        let served_paid_advanced = Arc::new(Notify::new());
        let pull_ended = Arc::new(Notify::new());
        let pull_result: Arc<std::sync::Mutex<Option<anyhow::Result<()>>>> =
            Arc::new(std::sync::Mutex::new(None));
        let cancel = tokio_util::sync::CancellationToken::new();

        // The whole-tree bao outboard the two legs share (ADR 038): the pull leg
        // captures each admitted range's proof nodes into `ob_writer`, and the serve
        // leg's coherent whole-range encoder reads them through a reader minted from
        // `ob_factory`. Construction and reader-minting are split so the pull can
        // start capturing before the serve leg wires its reader.
        let (ob_writer, ob_factory) = crate::node_origin::shared_outboard(
            bao_tree::blake3::Hash::from(*hash.as_bytes()),
            total_bytes,
        );
        // Seed the outboard with proof nodes for ranges this node ALREADY holds. The
        // pull leg only captures ranges it ADMITS, but a range-minimized serve-miss
        // can start with bytes already present (ADR 037 §held ranges read locally) —
        // without this seed the coherent encoder would `load` a held span's proof
        // node that was never captured, park until `pull_ended`, then fail "outboard
        // node never captured". The same interior-hold seed as the peer twin;
        // best-effort (a full miss makes `present` empty and this a no-op).
        if let Ok(present) = self.cache.present_ranges(hash).await
            && !present.chunk_ranges().is_empty()
            && let Ok(pairs) = self
                .cache
                .outboard_pairs(hash, present.chunk_ranges())
                .await
        {
            for (node, pair) in pairs {
                ob_writer.save(node, pair);
            }
        }

        // The serve leg reads the cache the pull leg fills — same engine, same hash.
        let serve_store = decdn_cache::NodeRangedStore::new(self.cache.clone(), hash, total_bytes);

        // Spawn the off-task local pull leg on its own current-thread runtime. All
        // inputs are owned + `'static`; it shares only the coordination Arcs above.
        // The `BackendSource` carries a FRESH local bookkeeping `ChannelLedger` (seed
        // ZERO) that `run_local_pull_leg` reads back via `source.ledger()` and hands
        // to `drive` as the completion frontier (THE CRUX — see the `BackendSource`
        // module docs; it is a completion counter, never payment).
        let pull_thread = {
            let engine = self.cache.clone();
            let metrics = Arc::clone(&self.metrics);
            let served_paid = Arc::clone(&served_paid);
            let served_paid_advanced = Arc::clone(&served_paid_advanced);
            let pull_ended = Arc::clone(&pull_ended);
            let pull_result = Arc::clone(&pull_result);
            let cancel = cancel.clone();
            let offset = req.byte_offset;
            let len = req.byte_len;
            let outboard_writer = ob_writer;
            // Seed-leech cap (ADR 037): re-homed into the pull leg's pacer. The served
            // client is the accounting key.
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
            std::thread::Builder::new()
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
                            offset,
                            len,
                            window,
                            total_bytes,
                            served_paid,
                            served_paid_advanced,
                            Arc::clone(&pull_ended),
                            pull_result.clone(),
                            outboard_writer,
                            leech_governor,
                            client_peer,
                            cancel,
                        )),
                        Err(e) => {
                            // The pull could not start: record a terminal error and
                            // wake the serve leg so it fails a gap rather than hanging.
                            if let Ok(mut guard) = pull_result.lock() {
                                *guard = Some(Err(anyhow::anyhow!(
                                    "serve-miss local pull runtime build failed: {e}"
                                )));
                            }
                            pull_ended.notify_waiters();
                        }
                    }
                })
        };
        let pull_thread = match pull_thread {
            Ok(handle) => handle,
            Err(e) => {
                // The OS refused the thread: fail the serve cleanly. There is no tee
                // to release on the local path.
                return Err(anyhow::anyhow!(
                    "could not spawn serve-miss local pull thread: {e}"
                ));
            }
        };

        // Mint the serve leg's outboard reader from the shared factory, bound to the
        // pull's terminal signals for the coherent encoder's no-hang guarantee.
        let outboard_reader = ob_factory.reader(Arc::clone(&pull_ended), Arc::clone(&pull_result));

        // Run the serve leg on THIS (accept) task and await it. It owns termination —
        // mid-stream takedown and client-disconnect are both handled inside it.
        let serve_result = self
            .serve_leg(
                &mut send,
                &mut recv,
                serve_store,
                outboard_reader,
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
                Arc::clone(&served_paid),
                Arc::clone(&served_paid_advanced),
                Arc::clone(&pull_ended),
                Arc::clone(&pull_result),
            )
            .await;

        // Teardown: cancel the pull (stop draining our own origin for a blob the
        // client no longer waits on, #1610), then JOIN — bounded, since cancellation
        // makes `drive` drop promptly. Joined off the async worker via
        // `spawn_blocking`. There is NO tee to abandon on the local path.
        cancel.cancel();
        let _ = tokio::task::spawn_blocking(move || {
            let _ = pull_thread.join();
        })
        .await;

        serve_result
    }
}
