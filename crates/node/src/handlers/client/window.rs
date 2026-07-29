//! Window-paced pull-through serve path (#856, ADR 037).
//! Bodies split from `mod.rs` (#1254).

use alloy::primitives::U256;

use super::{
    Arc, B256, BatchStop, BufferedVoucherReader, Bytes, CacheError, ChannelDeliveryState,
    ChannelId, ChunkData, ClientHandler, ClientMessage, FillOutcome, Hash, MB_BYTES, Mutex,
    NodeOrigin, NodeProgressivePull, RecvStream, SendStream, ServeRejectReason, StreamRequest,
    StreamRequestExt, StreamResponseBody, TeeReservation, TeeSink, TeeVerdict, VecDeque,
    WINDOW_PULL_FALLBACK_DEADLINE, min_payment,
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
    /// populate) hit a transient backend fault for this request (#1129). This path
    /// is the last tier, so all three of its MISS exits — the leech shed, no
    /// openable provider, and the open deadline — refuse via
    /// [`FillOutcome::miss_reason`], reporting `InternalError` when this node is
    /// degraded rather than merely empty.
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
    #[allow(clippy::too_many_arguments)]
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
    ) -> anyhow::Result<()> {
        // The ADR 011 OPEN-TIME deny gates are already discharged on the only
        // path that reaches here: `serve_stream` refuses a denylisted hash
        // before the availability check, and this branch is entered only behind
        // `pull_authorized`, which refuses a blacklisted funding origin. Keep it
        // that way — if this function ever gains a second caller, that caller
        // owes both checks, because this is a spend-and-serve path.
        //
        // They cover the request, not the stream: a takedown landing after this
        // point is caught per MB boundary inside `window_forward_loop` (ADR 011
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
                .respond_error(&mut send, req, ServeRejectReason::UnknownChannel)
                .await;
        };

        let rate_per_mb = self.clamped_rate();

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
        // voucher interval, matching `window_forward_loop`.
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
        if deposit.saturating_sub(last_amount) < ceiling {
            tee.abandon();
            return self
                .respond_error(&mut send, req, ServeRejectReason::InsufficientDeposit)
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
            return self.respond_error(&mut send, req, reason).await;
        }

        // (3) Open the progressive upstream pull, bounded by the pull-through
        // deadline so a slow/absent upstream can't pin the stream.
        let deadline = self.pull_through.unwrap_or(WINDOW_PULL_FALLBACK_DEADLINE);
        // The client's namespace routing hint (ADR 005 §Namespace routing). On this
        // progressive serve path it drives the origin-directory fallback inside the
        // pull's `discover` on a total DHT miss, and is threaded onto the resulting
        // node-to-node leg so a directory-discovered cold origin's own pull-through
        // gate resolves (#1401). (The pull-through authorized-origin gate in
        // `dispatch` also reads `req.namespace_id`, upstream of this path.)
        // `NO_NAMESPACE` (0) → no authorized origins (ADR 002 §Namespace 0).
        // Converted big-endian to the on-chain `uint256` shape the directory keys on.
        let namespace_id = U256::from_be_bytes(req.namespace_id);
        let (header, pull) =
            match tokio::time::timeout(deadline, origin.open_progressive_pull(hash, namespace_id))
                .await
            {
                Ok(Some(pair)) => pair,
                // No upstream provider could be opened. That is a clean miss on THIS
                // tier — but if an earlier tier faulted, the request as a whole is
                // still unresolved-by-fault, so honor that (#1129).
                Ok(None) => {
                    tee.abandon();
                    self.maybe_spawn_background_fill(hash);
                    let reason = FillOutcome::miss_reason(fault_seen);
                    return self.respond_error(&mut send, req, reason).await;
                }
                Err(_elapsed) => {
                    tee.abandon();
                    self.metrics.node_pull_through_timeout();
                    self.maybe_spawn_background_fill(hash);
                    let reason = FillOutcome::miss_reason(fault_seen);
                    return self.respond_error(&mut send, req, reason).await;
                }
            };
        let total_bytes = header.total_bytes;

        // (4) Size gate on the upstream-claimed total.
        if self.max_blob_size_bytes > 0 && total_bytes > self.max_blob_size_bytes {
            pull.abandon(None);
            tee.abandon();
            return self
                .respond_error(&mut send, req, ServeRejectReason::BlobTooLarge)
                .await;
        }

        // (5) Voucher-interval negotiation (ADR 003), then sign + send the
        // response up front — it commits to `total_bytes`, now known from the
        // upstream header.
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

        // Frame the tee's verifying decoder now that the whole-blob content size
        // is known from the upstream header (ADR 038) — the forwarded wire is
        // header-less. `total_bytes` is the CONTENT size; the tee decodes the
        // interleaved bao back to that many plaintext bytes.
        let tee = tee.begin(total_bytes);

        // (6) Fused window-paced loop. Boxed to keep the large loop future off
        // this frame (clippy::large_futures).
        Box::pin(self.window_forward_loop(
            &mut send,
            &mut recv,
            hash,
            channel_id,
            &channel,
            client_node_id,
            rate_per_mb,
            interval_mb,
            total_bytes,
            pull,
            tee,
        ))
        .await
    }

    /// The fused pull-forward-pay loop (#856). Pulls upstream chunks (teeing each
    /// to the cache and forwarding to the client) but keeps the unrecouped frontier
    /// (`pulled − paid`) within the window, collecting one downstream voucher per
    /// interval to recoup before pulling further. A client that drops or underpays
    /// costs at most one window of upstream spend.
    ///
    /// Precise bound: the window is checked at the top of the pull phase, *before*
    /// fetching the next chunk, so the realized frontier can overshoot by up to one
    /// upstream `CHUNK_SIZE` payload (the chunk that crosses the threshold). The
    /// documented `pull_ahead_bytes` exposure is therefore exact only to within one
    /// chunk — negligible at the default ~1 MiB window vs `CHUNK_SIZE`, but the
    /// "≤ one window" claims elsewhere mean "≤ window + one chunk".
    // The pull-ahead / recoup / finalize phases are one linear flow; splitting
    // them across helpers would scatter the shared loop state (frontier counters,
    // pending intervals) and obscure the bound, so the length/complexity is
    // intrinsic.
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    pub(super) async fn window_forward_loop(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        hash: Hash,
        channel_id: ChannelId,
        channel: &Arc<Mutex<ChannelDeliveryState>>,
        client_node_id: B256,
        rate_per_mb: u64,
        interval_mb: u64,
        total_bytes: u64,
        mut pull: NodeProgressivePull,
        mut tee: TeeSink,
    ) -> anyhow::Result<()> {
        let interval_bytes = interval_mb.saturating_mul(MB_BYTES).max(1);
        // The fused window bounds a single quantity: `pulled − served_paid`, which
        // — because every pulled chunk is forwarded downstream immediately — is at
        // once the UPSTREAM speculative spend and the DOWNSTREAM unbilled egress.
        // So it must satisfy both bounds: at least `pull_ahead_bytes` (the upstream
        // exposure knob, #856) AND at least the downstream `credit_window` (#1477),
        // which is what pipelines the client-facing leg past the pre-#1477
        // one-interval barrier. Both already floor at one interval, so the loop can
        // always make progress (pull a full interval, then collect its voucher).
        let window = self
            .pull_ahead_bytes
            .unwrap_or(Bytes::new(decdn_common::config::DEFAULT_PULL_AHEAD_BYTES))
            .max(Bytes::new(interval_bytes))
            .max(Bytes::new(self.credit_window(interval_bytes)));
        // Group-commit cap (#1483): at most this many downstream vouchers share
        // one fsync. Bounded by how many intervals fit in the window, so at a
        // one-interval window it is 1 and each recoup collects a single voucher.
        let batch_cap = usize::try_from(window.get() / interval_bytes.max(1))
            .unwrap_or(usize::MAX)
            .max(1);
        let peer = client_node_id.0;
        // Read once: the funder is immutable for the channel's lifetime, and the
        // per-boundary in-flight takedown check below must not re-take the
        // channel lock every MB just to re-read it.
        //
        // This is the FUNDER (ADR 011 compliance), never the channel's
        // `voucher_signer`, and must not be re-keyed onto it: a blacklisted
        // funder can pin a clean throwaway key as its signer, so checking the
        // signer here would silently stop enforcing takedowns.
        let funder = channel.lock().await.state.client;
        // Typed total so the window-budget comparisons below stay `Bytes`-vs-`Bytes`.
        // The forwarded/metered quantities are WIRE bytes (the bao verified-stream:
        // content plus interleaved proof, ADR 038), so the pull budget is the
        // bao-encoded size, not the content `total_bytes`.
        let total = Bytes::new(pull.expected_wire_bytes());

        // The tee's verifying decoder was framed with the content size at
        // `TeeReservation::begin` (ADR 038), so the forwarded wire is header-less:
        // every `tee.write` below is pure bao interleaved bytes.

        let mut pulled = Bytes::default();
        let mut served_paid = Bytes::default();
        // Bytes forwarded since the last completed interval (the sub-interval
        // remainder), and the completed-but-unpaid interval deltas awaiting
        // collection — together these are the unrecouped frontier. Deliberately
        // `u64`, not `Bytes`: these are voucher-domain deltas consumed by
        // `collect_voucher` (rate × MB), not window-budget byte quantities.
        let mut unvouchered: u64 = 0;
        let mut pending: VecDeque<u64> = VecDeque::new();
        let mut upstream_done = false;
        // One buffered voucher reader for the whole stream (#1483): every voucher
        // read goes through it so pipelined vouchers buffered ahead of a batch
        // commit are not lost.
        let mut reader = BufferedVoucherReader::default();

        loop {
            let pulled_at_iter_start = pulled;
            // --- pull-ahead phase: forward chunks until the window is reached,
            // the caps refuse, or the upstream is exhausted ---
            let mut window_hit = false;
            while !upstream_done && pulled < total {
                if pulled.saturating_sub(served_paid) >= window {
                    window_hit = true;
                    break;
                }
                if !self.leech_admit(&peer) {
                    break;
                }
                match pull.next_chunk().await {
                    Ok(Some(chunk)) => {
                        let len = chunk.len() as u64;
                        // Charge the speculative spend to the seed-leech governor
                        // and advance the window frontier the instant the upstream
                        // is paid for this chunk (inside `next_chunk`), BEFORE the
                        // fallible tee/forward below. Recording only after a
                        // successful forward would under-count already-paid bytes
                        // on the abandon paths, leaving the abuse caps blind to
                        // spend the node really incurred (#856).
                        pulled = pulled.saturating_add(Bytes::new(len));
                        self.leech_record_pulled(&peer, len);
                        if let Err(e) = tee.write(&chunk).await {
                            // Classify by what actually killed the write (#915
                            // review). `TeeSink::write` surfaces the ended import
                            // task's own verdict: a `VerifyFailed` (or a whole-blob
                            // `HashMismatch`) means the tee's bao decoder REJECTED
                            // forwarded bytes — a corrupt/lying upstream, scored as
                            // such — while anything else is a genuinely LOCAL store
                            // fault (failing `data_dir`), metered separately so an
                            // operator can tell the two apart. (`HashMismatch` can't
                            // arise on a fully-decoded tee stream today, but routing
                            // it as corruption keeps a future decoder change from
                            // silently landing it in the local-fault arm below.)
                            // Either way the abandon persists the buyer watermark
                            // (#852) for what we paid upstream, and no `StreamEnd` is
                            // sent (the returned error resets the stream; the
                            // downstream's own decoder rejects the bytes).
                            tee.abandon();
                            if matches!(
                                e,
                                CacheError::VerifyFailed { .. } | CacheError::HashMismatch { .. }
                            ) {
                                pull.abandon_corrupt();
                                self.metrics.node_pull_through_upstream_verify_failed();
                                tracing::warn!(
                                    %hash, %channel_id, served_paid = served_paid.get(),
                                    pulled = pulled.get(), error = %e,
                                    "window pull-through upstream failed bao verification mid-stream; abandoning, not caching"
                                );
                                return Err(anyhow::anyhow!(
                                    "upstream bao verification failed mid-stream: {e}"
                                ));
                            }
                            pull.abandon(None);
                            self.metrics.node_pull_through_local_tee_failed();
                            return Err(anyhow::anyhow!("cache tee write failed: {e}"));
                        }
                        // Relayed verbatim from a frame the upstream receive loop already
                        // decoded, so it is non-empty and within `CHUNK_SIZE` — but this
                        // path re-frames it rather than forwarding the value, so it must
                        // re-establish that rather than assume it. `ChunkData::new` is the
                        // gate; a zero-length relay is a bug here, not something to emit.
                        let frame = match ChunkData::new(chunk.to_vec()) {
                            Ok(frame) => frame,
                            Err(e) => {
                                self.abandon_window_serve(pull, tee);
                                return Err(anyhow::anyhow!(
                                    "refusing to relay an invalid chunk downstream: {e}"
                                ));
                            }
                        };
                        if let Err(e) = self
                            .write_message(send, &ClientMessage::ChunkData(frame))
                            .await
                        {
                            // Downstream dropped mid-pull (the #856 shape): stop
                            // the upstream spend and persist the buyer watermark
                            // (#852, via `abandon`) before surfacing the error.
                            self.abandon_window_serve(pull, tee);
                            return Err(e);
                        }
                        unvouchered = unvouchered.saturating_add(len);
                        if unvouchered >= interval_bytes {
                            pending.push_back(unvouchered);
                            unvouchered = 0;
                        }
                    }
                    // Upstream ended before the promised total — a short delivery.
                    // Stop pulling; `pull.finish()` below surfaces it.
                    Ok(None) => upstream_done = true,
                    Err(e) => {
                        // Upstream fault mid-pull: abandon both sides and reset the
                        // downstream stream so the client retries elsewhere.
                        pull.abandon(Some(&e));
                        tee.abandon();
                        return Err(e);
                    }
                }
            }
            if window_hit {
                self.metrics.node_pull_through_window_paused();
            }

            // --- recoup phase: batch up to `batch_cap` completed downstream
            // intervals into ONE fsynced commit, acking each voucher only after
            // the commit is durable (#1483, group commit) — a completed interval
            // (drained in order), else the closing partial once the whole blob is
            // pulled. A short upstream never earns a closing voucher. ---
            let done_pulling = upstream_done || pulled >= total;
            if done_pulling && pulled >= total && unvouchered > 0 {
                // Fold the closing partial into `pending` so the batch drains it
                // uniformly with the completed intervals.
                pending.push_back(unvouchered);
                unvouchered = 0;
            }
            let mut deltas: Vec<u64> = Vec::with_capacity(batch_cap);
            while deltas.len() < batch_cap {
                match pending.pop_front() {
                    Some(delta) => deltas.push(delta),
                    None => break,
                }
            }
            let collected_any = !deltas.is_empty();
            if collected_any {
                match self
                    .collect_voucher_batch(
                        send,
                        recv,
                        &mut reader,
                        hash,
                        channel_id,
                        Some(channel),
                        client_node_id,
                        rate_per_mb,
                        &deltas,
                    )
                    .await
                {
                    Ok(outcome) => {
                        let paid_bytes: u64 = deltas.iter().take(outcome.committed).sum();
                        served_paid = served_paid.saturating_add(Bytes::new(paid_bytes));
                        // Re-queue deltas the client had not paid yet (short batch),
                        // preserving order at the front of `pending`.
                        for &delta in deltas
                            .get(outcome.committed..)
                            .unwrap_or_default()
                            .iter()
                            .rev()
                        {
                            pending.push_front(delta);
                        }
                        match outcome.stop {
                            BatchStop::Rejected => {
                                self.abandon_window_serve(pull, tee);
                                return Ok(());
                            }
                            BatchStop::Continue => {
                                // ADR 011 §On Blacklist Event: terminate an
                                // in-flight delivery at the next boundary once a
                                // takedown lands. This path needs it at least as
                                // much as the buffered one — it is simultaneously
                                // *pulling* the blacklisted blob from upstream, so
                                // continuing would both serve and re-acquire content
                                // under a removal order. Abandoning the pull is what
                                // stops the upstream spend.
                                if self.takedown_landed(hash, Some(funder)) {
                                    self.abandon_window_serve(pull, tee);
                                    self.terminate_for_takedown(send, recv, hash);
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // A transport drop (the #856 client-disconnect shape) or an
                        // underpayment bail. Stop the upstream spend, PERSIST the
                        // buyer watermark for what we paid (#852, via `abandon`),
                        // and surface the error so the stream resets.
                        self.abandon_window_serve(pull, tee);
                        return Err(e);
                    }
                }
            }

            // Livelock guard (#856): an iteration that neither pulled a chunk (a
            // seed-leech cap denied the speculative pull) nor collected a voucher,
            // with the blob not yet fully pulled, cannot make progress — the cap
            // will keep denying with nothing to recoup. Refuse to continue the
            // speculative pull (ADR 037 §Seed-leech caps) rather than spin with no
            // await point (which would starve the runtime): drop the partial fill
            // and reset the stream so the client retries (per-request loss stays
            // bounded by the window). The pause cause is already metered by
            // `may_pull`.
            let made_pull_progress = pulled > pulled_at_iter_start;
            if !made_pull_progress && !collected_any && !done_pulling {
                // A seed-leech cap is denying the pull with nothing to recoup. Drop
                // the partial fill and reset the stream (no `StreamEnd`); the pause
                // cause is already metered by `may_pull`. Log the partial progress
                // so a throttled-but-not-dead serve is visible to an operator.
                tracing::debug!(
                    %hash, %channel_id, pulled = pulled.get(), served_paid = served_paid.get(),
                    "window pull-through throttled by seed-leech cap with nothing to recoup; dropping partial fill"
                );
                pull.abandon(None);
                tee.abandon();
                let _ = send.finish();
                return Ok(());
            }

            // Done when there is nothing left to pull and nothing left to collect
            // (full delivery), or the upstream came up short (no closing voucher).
            if pending.is_empty() && done_pulling && (unvouchered == 0 || pulled < total) {
                break;
            }
        }

        // Finalize. Under ADR 038 the cached copy is verified by the tee's bao
        // decoder against the content root, so `pull.finish(..)` only checks that
        // the forwarded WIRE stream was complete; the integrity verdict comes from
        // `tee.finish()`. Settle the TEE FIRST (all tee writes are done; a
        // truncated fill cannot promote — decoder EOF and the temp-tag hash check
        // both fail closed) and feed its verdict into `pull.finish(..)`, so a
        // wire-complete-but-corrupt upstream is scored `Corruption` — not
        // `Delivered` — before anything is gossiped (#915 review). Three outcomes:
        //   - short upstream     → `pull.finish(..)` Err (tee refused to promote)
        //   - corrupt upstream   → `pull.finish(..)` Ok, tee `VerifyFailed`
        //     (or a whole-blob `HashMismatch` — unreachable on a fully-decoded tee
        //     stream today, but scored as corruption defensively so a future
        //     decoder change can't reclassify it as a benign local fault)
        //   - local store fault  → `pull.finish(..)` Ok, tee other Err
        let tee_result = tee.finish().await;
        let verdict = if matches!(
            tee_result,
            Err(CacheError::VerifyFailed { .. } | CacheError::HashMismatch { .. })
        ) {
            TeeVerdict::Corrupt
        } else {
            TeeVerdict::Verified
        };
        match (pull.finish(verdict).await, tee_result) {
            (Err(e), _) => {
                // Short/incomplete upstream WIRE stream: do NOT promote (the tee
                // result — an inevitable truncation error — was already refused
                // above). The protocol has no mid-stream delivery-fault code (only
                // `VoucherRejected` rides mid-stream; the delivery-side
                // `StreamError` codes are initial-`StreamResponse`-only — ADR 005
                // §domain split), so we do NOT emit a frame: closing the send side
                // WITHOUT a `StreamEnd` sentinel is itself the signal, and the
                // downstream's own bao decoder rejects the truncated bytes.
                self.metrics.node_pull_through_upstream_verify_failed();
                tracing::warn!(
                    %hash, %channel_id, served_paid = served_paid.get(), total_bytes, error = %e,
                    "window pull-through upstream delivered short; not caching, no StreamEnd sent"
                );
                let _ = send.finish();
                Ok(())
            }
            (Ok(()), Ok(())) => {
                // Honest upstream, cached: the promote succeeded, so this node
                // is now a discoverable holder. Signal clean completion.
                self.write_message(send, &ClientMessage::StreamEnd).await?;
                // A failed `finish()` here means the clean `StreamEnd` may not
                // have reached the wire even though we counted the bytes as
                // served — log it rather than discard silently.
                if let Err(e) = send.finish() {
                    tracing::debug!(%hash, error = %e, "window pull-through send.finish failed after StreamEnd");
                }
                Ok(())
            }
            (Ok(()), Err(CacheError::VerifyFailed { .. } | CacheError::HashMismatch { .. })) => {
                // The teed bao failed verification against the content root: a
                // corrupt/lying upstream forwarded bytes that do not hash to
                // `hash` (ADR 038). `pull.finish(Corrupt)` above already scored
                // the provider `Corruption` (with its identity) in place of
                // `Delivered`. The downstream's own decoder rejects the bytes
                // too. Do NOT promote and do NOT send `StreamEnd` — closing
                // without the sentinel is the delivery-failure signal (as in
                // the short-upstream arm above).
                self.metrics.node_pull_through_upstream_verify_failed();
                tracing::warn!(
                    %hash, %channel_id, served_paid = served_paid.get(), total_bytes,
                    "window pull-through upstream served bytes that failed bao verification; not caching, no StreamEnd sent"
                );
                let _ = send.finish();
                Ok(())
            }
            (Ok(()), Err(other)) => {
                // Honest upstream (the forwarded bytes verified for the client),
                // but a LOCAL store fault rejected the promote (cap breach,
                // disk). The bytes were already served and paid, so delivery
                // completes cleanly — only the warm-cache benefit is forfeit.
                // Meter it so a node paying upstream egress but caching nothing
                // is alertable.
                self.metrics.node_pull_through_tee_finalize_failed();
                tracing::warn!(%hash, error = %other, "window pull-through tee finalize failed; blob served but not cached");
                self.write_message(send, &ClientMessage::StreamEnd).await?;
                if let Err(e) = send.finish() {
                    tracing::debug!(%hash, error = %e, "window pull-through send.finish failed after StreamEnd");
                }
                Ok(())
            }
        }
    }

    /// The #856 abandonment path: the downstream client underpaid or dropped
    /// mid-pull. Stop the upstream spend immediately (exposure ≤ one window),
    /// drop the partial fill (persisting the buyer watermark via `pull.abandon`,
    /// #852), and meter it as a client-abandon.
    ///
    /// This helper only does the teardown + metering; it neither writes a wire
    /// frame nor decides the caller's return value. Its three call sites differ:
    /// the voucher-rejected arm returns `Ok(())` after `collect_voucher` already
    /// wrote the rejection; the `collect_voucher` `Err` arm and the downstream
    /// `write_message` failure both propagate `Err` and may not have written any
    /// frame (an underpayment `bail!` has no wire reject code). Do not read this
    /// as "a rejection was always sent."
    pub(super) fn abandon_window_serve(&self, pull: NodeProgressivePull, tee: TeeSink) {
        pull.abandon(None);
        tee.abandon();
        self.metrics.node_pull_through_client_abandoned();
    }
}
