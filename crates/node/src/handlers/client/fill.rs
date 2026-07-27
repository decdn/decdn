//! Cache-fill tiers + background warm machinery for the serve-miss path.
//! Bodies split from `mod.rs` (#1254).

use super::{
    Address, Arc, CacheError, ChannelId, ClientHandler, Duration, FillOutcome, Hash,
    MAX_BACKGROUND_FILL_MB, RangePullOutcome, StreamRequest, WarmOutcome, WarmVerdict,
    arm_background_fill, is_clean_miss, with_optional_deadline,
};

impl ClientHandler {
    /// Spawn a detached background cache-fill for `hash` (#859), unless one is already
    /// running for it, every warm slot is busy, or the feature is unset. The
    /// foreground delivery path has already given up; this re-pulls from scratch under
    /// a far larger budget than the foreground had ([`BACKGROUND_FILL_HARD_CAP`](super::BACKGROUND_FILL_HARD_CAP)), so a
    /// slow-but-available upstream still warms the cache, however large the blob.
    /// Best-effort: never blocks the caller and never affects the foreground result.
    #[allow(clippy::too_many_lines)] // one linear spawn, every arm carrying its own rationale
    pub(super) fn maybe_spawn_background_fill(&self, hash: Hash) {
        let Some(bg) = self.background_fill.as_ref() else {
            return;
        };
        // Dedup: only the first miss for a hash claims it and receives the guard
        // that releases the claim when the spawned task ends.
        let Some(guard) = arm_background_fill(&bg.inflight, hash) else {
            return;
        };
        // Memory ceiling across distinct hashes: reserve this warm's worst-case blob
        // footprint up front ([`MAX_BACKGROUND_FILL_MB`]). Taken AFTER the dedup claim so
        // a repeat miss on an already-warming hash never consumes budget — and dropped
        // with `guard` if we shed, so the hash stays unclaimed and a later miss retries.
        let Ok(permit) = Arc::clone(&bg.slots).try_acquire_many_owned(bg.reserve_mb) else {
            self.metrics.node_pull_through_background_shed();
            tracing::debug!(
                %hash,
                reserve_mb = bg.reserve_mb,
                pool_mb = MAX_BACKGROUND_FILL_MB,
                "background cache-fill shed: no room in the warm memory budget"
            );
            return;
        };
        let cache = self.cache.clone();
        let metrics = Arc::clone(&self.metrics);
        let cancel = bg.cancel.clone();
        let budget = bg.budget;
        self.metrics.node_pull_through_background_spawned();
        tokio::spawn(async move {
            // All three dropped on task exit (any branch, INCLUDING a panic): the guard
            // clears the inflight entry, the permit returns the warm slot, and the outcome
            // guard meters the one case no arm below can — a panic (#1145 review).
            let _guard = guard;
            let _permit = permit;
            // Nothing awaits this task's `JoinHandle` — it is spawned and forgotten — so a
            // panic inside `cache.populate`, the tee, or the decoder reaches nobody: no
            // counter, no log, and `spawned` is permanently one ahead of its outcomes. The
            // gap then reads as an in-flight warm rather than a crash. `Drop` runs on the
            // unwind, so a guard is the one thing that can still see it.
            let mut outcome = WarmOutcome::new(hash, &metrics);
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    outcome.record(WarmVerdict::Cancelled);
                    tracing::debug!(%hash, "background cache-fill cancelled on shutdown");
                }
                result = with_optional_deadline(budget, cache.populate(hash)) => match result {
                    Ok(Ok(())) => {
                        outcome.record(WarmVerdict::Succeeded);
                        tracing::debug!(%hash, "background cache-fill populated blob");
                    }
                    // A CLEAN MISS is not a fault, and folding the two together meant this
                    // counter could never fire for the emergency it would need to signal
                    // (#1145 review). A warm that finds no provider is routine and expected;
                    // a warm that failed because the store is corrupt or the disk is full is
                    // an operator's problem. Both used to increment the same counter and both
                    // logged at `debug!` — below the project's default `RUST_LOG=info` — so
                    // there was no threshold on it anyone could alert on.
                    Ok(Err(e)) if is_clean_miss(&e) => {
                        outcome.record(WarmVerdict::Missed);
                        tracing::debug!(%hash, error = %e, "background cache-fill found nothing to warm");
                    }
                    Ok(Err(e)) => {
                        outcome.record(WarmVerdict::Failed);
                        tracing::warn!(%hash, error = %e, "background cache-fill FAILED; this is a fault, not a miss");
                    }
                    Err(_) => {
                        // `warn!`, not `debug!`: this fires only at the absolute
                        // backstop, which an honest transfer cannot reach. Hitting it
                        // means an upstream trickled bytes for an hour without
                        // finishing — a pathological peer, or a badly mis-sized
                        // `max_blob_size_mb`. Either way the operator wants to know.
                        outcome.record(WarmVerdict::Failed);
                        tracing::warn!(%hash, ?budget, "background cache-fill hit its absolute cap; abandoning");
                    }
                },
            }
        });
    }

    /// Whether `req` is authorized to trigger a paid pull-through (#831): it must
    /// carry a verified client binding (`verified_client`) whose recovered
    /// address is the named channel's pinned `voucher_signer`. Channel
    /// *existence* is public (on-chain `ChannelOpened`), so it cannot authorize
    /// spend — only proof of *voucher authority* can, since only the pinned
    /// signer can produce a voucher this channel will accept. An unbound
    /// request, or a binding that does not match that signer, must not make this
    /// node front upstream USDC. Mirrors the binding gate in `dispatch.rs`,
    /// applied *before* any spend.
    ///
    /// Also the earliest origin-blacklist gate (ADR 011), on BOTH addresses. The
    /// serve-path gate in `dispatch.rs` refuses the delivery, but it runs after
    /// this: without the same check here, a blacklisted origin's request would
    /// still have made this node front upstream USDC egress and warm its cache
    /// on that origin's behalf, only to refuse the delivery afterwards. Refusing
    /// to *spend* is the part that actually costs the operator.
    pub(super) async fn pull_authorized(
        &self,
        req: &StreamRequest,
        verified_client: Option<Address>,
    ) -> bool {
        let Some(client) = verified_client else {
            return false;
        };
        if self.content_deny.is_origin_denied(&client) {
            return false;
        }
        let chan = self
            .channels
            .lock()
            .await
            .get(&ChannelId::from(req.channel_id))
            .cloned();
        match chan {
            Some(chan) => {
                let state = &chan.lock().await.state;
                // Ownership is a *signer* question: only the pinned voucher
                // signer can pay for this upstream pull, so only it can
                // authorize the spend.
                if state.voucher_signer != client {
                    return false;
                }
                // Compliance is a *funder* question (ADR 011), and must NEVER be
                // re-keyed onto the signer: never front upstream USDC on a
                // channel funded by a blacklisted address, even when a clean
                // delegate key is doing the signing. The `is_origin_denied`
                // early return above covers the blacklisted *signer* case.
                !self.content_deny.is_origin_denied(&state.client)
            }
            None => false,
        }
    }

    /// Attempt to fill a cache miss by pulling from an upstream node (#831). The
    /// cache engine's `NodeOrigin` (last in the origin chain) does the discovery
    /// → probe → ranked paid pull → populate; here we trigger it via
    /// `cache.populate` (which fills the store WITHOUT returning the blob or
    /// bumping `bytes_returned` — this is an internal fill, not client egress),
    /// bounded by `timeout` so a slow upstream can't pin the delivery path. The
    /// subsequent normal delivery streams the populated bytes from the store and
    /// accounts the served-bytes metrics there. Returns whether the blob is now
    /// present locally, and — when it is not — whether the cause was a clean miss
    /// or a hard fault (#1129).
    pub(super) async fn try_pull_through(&self, hash: Hash, timeout: Duration) -> FillOutcome {
        match tokio::time::timeout(timeout, self.cache.populate(hash)).await {
            Ok(Ok(())) => FillOutcome::Filled,
            // A clean miss — no origin/provider had it — is the normal
            // unfillable case (`NotFound`/`NoOrigin`); log at debug and move on.
            Ok(Err(e @ (CacheError::NotFound { .. } | CacheError::NoOrigin { .. }))) => {
                tracing::debug!(%hash, error = %e, "node-to-node pull-through found no source");
                FillOutcome::CleanMiss
            }
            // A TRANSIENT backend fault — the node is degraded, not empty. Report
            // it as a fault so the caller can refuse `InternalError` if no further
            // tier fills (#1129), rather than reporting a broken origin as a miss.
            Ok(Err(e @ (CacheError::OriginError { .. } | CacheError::Store(_)))) => {
                self.metrics.node_pull_through_error();
                tracing::warn!(%hash, error = %e, "node-to-node pull-through hit a transient backend fault");
                FillOutcome::HardFault
            }
            // Everything else (`BlobTooLarge`, `HashMismatch`, `VerifyFailed`,
            // `EvictionLimitExceeded`) is DETERMINISTIC: it will recur on every
            // request for this hash, so it is not evidence the node is degraded.
            // Reporting it as `InternalError` ("do not retry this node") would
            // steer clients off a healthy node permanently over one bad blob — and
            // for `BlobTooLarge` it would also contradict the size gate, which
            // refuses the very same condition with the dedicated `BlobTooLarge`
            // code when the blob happens to be in the store. Meter it (the operator
            // still needs to see it) but let it fall through as a plain miss.
            Ok(Err(e)) => {
                self.metrics.node_pull_through_error();
                tracing::warn!(%hash, error = %e, "node-to-node pull-through hit a permanent cache-engine error");
                FillOutcome::CleanMiss
            }
            Err(_) => self.on_pull_through_timeout(hash, timeout).await,
        }
    }

    /// Attempt to fill a cache miss from the node's OWN configured origins only
    /// (#1116), via [`CacheEngine::populate_local`](super::CacheEngine::populate_local) — which skips the paid `Peer`
    /// node→node origin, so this fronts no upstream USDC. Bounded by `timeout`
    /// like [`Self::try_pull_through`]. A clean miss (`NotFound`/`NoOrigin` — the
    /// operator's origin lacks it, or only a `Peer` origin is configured) is the
    /// normal unfillable case; the caller then falls through to the node→node
    /// paths. A [`FillOutcome::HardFault`] means the operator's OWN origin faulted
    /// (an S3 5xx, an open breaker, an fs I/O error) — the caller may still try a
    /// further tier, but must not let a later clean miss launder the fault into a
    /// signed `NotFound` (#1129). Unlike `try_pull_through`, a timeout does NOT
    /// spawn node→node background fill — this path is local-only.
    pub(super) async fn try_local_populate(&self, hash: Hash, timeout: Duration) -> FillOutcome {
        match tokio::time::timeout(timeout, self.cache.populate_local(hash)).await {
            Ok(Ok(())) => FillOutcome::Filled,
            Ok(Err(e @ (CacheError::NotFound { .. } | CacheError::NoOrigin { .. }))) => {
                tracing::debug!(%hash, error = %e, "reactive local-origin pull-through found no source");
                FillOutcome::CleanMiss
            }
            // Transient — the operator's own origin is down. See
            // [`Self::try_pull_through`] for why the split is exactly these two
            // variants and not a catch-all.
            Ok(Err(e @ (CacheError::OriginError { .. } | CacheError::Store(_)))) => {
                self.metrics.node_pull_through_error();
                tracing::warn!(%hash, error = %e, "reactive local-origin pull-through hit a transient backend fault");
                FillOutcome::HardFault
            }
            // Deterministic (`BlobTooLarge` / `HashMismatch` / `VerifyFailed`):
            // recurs every request, so it is not evidence this node is degraded.
            Ok(Err(e)) => {
                self.metrics.node_pull_through_error();
                tracing::warn!(%hash, error = %e, "reactive local-origin pull-through hit a permanent cache-engine error");
                FillOutcome::CleanMiss
            }
            Err(_) => self.on_local_populate_timeout(hash, timeout).await,
        }
    }

    /// Handle a reactive local-origin populate deadline expiry (#1116). Serves the
    /// blob if a concurrent fill landed it in the store at the instant the
    /// deadline fired (so a node with node→node OFF doesn't report `CacheMiss` for
    /// a blob that is now present), otherwise meters the timeout and reports the
    /// miss. Unlike [`Self::on_pull_through_timeout`] it spawns NO background warm
    /// — this path is local-only and must not kick off a node→node pull.
    ///
    /// A deadline expiry is a [`FillOutcome::CleanMiss`], not a fault — see
    /// [`Self::on_pull_through_timeout`] for why.
    pub(super) async fn on_local_populate_timeout(
        &self,
        hash: Hash,
        timeout: Duration,
    ) -> FillOutcome {
        match self.cache.has(hash).await {
            Ok(true) => return FillOutcome::Filled,
            Ok(false) => {}
            Err(e) => {
                // A store-lookup fault at the deadline is a store error, not a
                // timeout: meter it as an error and return — do NOT also count a
                // timeout or emit a misleading "timed out" line for it. It is a
                // genuine local store fault, so the node is degraded, not empty.
                self.metrics.node_pull_through_error();
                tracing::warn!(%hash, error = %e, "reactive local-origin pull-through store lookup failed after deadline");
                return FillOutcome::HardFault;
            }
        }
        self.metrics.node_pull_through_timeout();
        tracing::debug!(%hash, ?timeout, "reactive local-origin pull-through timed out");
        FillOutcome::CleanMiss
    }

    /// Attempt to fill a bounded/offset cache-miss request by pulling only the
    /// requested byte span from origin (#823, ADR 037 §Origin-tier
    /// pull-through), rather than the whole
    /// blob. Returns `Some(total_blob_size)` when the span is now present as a
    /// verified partial blob (the size is the authoritative whole-blob length
    /// the size gate advertises to the client), or `None` to degrade to the
    /// whole-blob pull-through path.
    ///
    /// The blob's exact total size is needed up front to anchor the requested
    /// sub-range in the bao tree, so this first probes the origin
    /// ([`CacheEngine::origin_size`](super::CacheEngine::origin_size) — a cheap `HEAD`/`HeadObject`/stat). An
    /// unknown size, or an origin that can't serve a verified range
    /// ([`RangePullOutcome::Unsupported`]), degrades to `None`. Every failure
    /// mode here is best-effort: the caller falls back to a whole-blob pull,
    /// which is always correct.
    ///
    /// The second element is the tier's [`FillOutcome`] for the FAULT LATCH only
    /// (#1129) — never `Filled`, since "did this tier fill?" is carried by the
    /// `Option`. A `CacheError::Store` here is a real local fault and must latch:
    /// degrading to the whole-blob path is right for SERVICE (that path may fill
    /// from a different store route), but if nothing ends up filling, the refusal
    /// must report a degraded node, not an empty one.
    pub(super) async fn try_range_pull_through(
        &self,
        hash: Hash,
        req: &StreamRequest,
    ) -> (Option<u64>, FillOutcome) {
        let blob_size = match self.cache.origin_size(hash).await {
            Ok(Some(size)) => size,
            Ok(None) => return (None, FillOutcome::CleanMiss),
            Err(e) => {
                tracing::debug!(%hash, error = %e, "origin size probe failed; degrading to whole-blob pull");
                return (None, FillOutcome::CleanMiss);
            }
        };
        match self
            .cache
            .pull_through_range(hash, req.byte_offset, req.byte_len, blob_size)
            .await
        {
            Ok(RangePullOutcome::Served) => (Some(blob_size), FillOutcome::CleanMiss),
            Ok(RangePullOutcome::Unsupported) => (None, FillOutcome::CleanMiss),
            // A local store fault (`pull_through_range` fail-fasts on
            // `CacheError::Store` — disk-full / IO / a fault in the partial
            // `import_bao_bytes` path — rather than masking it behind another
            // origin) is a genuine local problem. It still degrades to the
            // whole-blob path (so the client isn't denied service if that path
            // can fill from a different store route), but it is metered and
            // LATCHED as a fault: a fault localized to the partial-import path
            // would otherwise be silently masked by a whole-blob fallback that
            // then cleanly misses, leaving the range optimization quietly disabled
            // AND reporting a broken store as an empty cache.
            Err(e @ CacheError::Store(_)) => {
                self.metrics.node_pull_through_error();
                tracing::warn!(%hash, error = %e, "range pull-through hit a local store fault; degrading to whole-blob pull");
                (None, FillOutcome::HardFault)
            }
            // An out-of-bounds range or a logically-evicted hash surfaces as an
            // error; degrade to the whole-blob path (which re-applies the same
            // eviction guard and bound checks) rather than failing the stream.
            // Deterministic, so not a fault (see [`FillOutcome::HardFault`]).
            Err(e) => {
                tracing::debug!(%hash, error = %e, "range pull-through declined; degrading to whole-blob pull");
                (None, FillOutcome::CleanMiss)
            }
        }
    }

    /// Wait for a concurrent fill of `hash` to land, then report whether the blob
    /// is now present (#856 coalescing). Used when [`CacheEngine::open_tee_sink`](super::CacheEngine::open_tee_sink)
    /// reported [`TeeOpen::InFlight`](super::TeeOpen::InFlight): another request is already pulling this
    /// hash, so we MUST NOT open a second upstream pull (no double spend). The
    /// coalescing [`CacheEngine::populate`](super::CacheEngine::populate) (via [`Self::try_pull_through`]) waits
    /// on the same in-flight entry and re-checks presence; if no pull-through
    /// deadline is configured it degrades to a plain presence check.
    pub(super) async fn await_coalesced_fill(&self, hash: Hash) -> FillOutcome {
        match self.pull_through {
            Some(timeout) => self.try_pull_through(hash, timeout).await,
            // No pull-through configured: the coalesced fill either landed or it
            // did not. A `has` *error* is a real store fault, not a clean miss —
            // surface it (like `on_pull_through_timeout`) rather than silently
            // reclassifying it as "blob absent" and reporting a clean `NotFound`.
            None => match self.cache.has(hash).await {
                Ok(true) => FillOutcome::Filled,
                Ok(false) => FillOutcome::CleanMiss,
                Err(e) => {
                    self.metrics.node_pull_through_error();
                    tracing::warn!(%hash, error = %e, "coalesced-fill store lookup failed; treating as a fault");
                    FillOutcome::HardFault
                }
            },
        }
    }

    /// Whether a speculative pull may proceed for `peer` under the seed-leech caps
    /// (#856). Always `true` when no governor is wired. Like
    /// [`LeechGovernor::poll_admission`](super::LeechGovernor::poll_admission) this is a stateful, advisory poll (it
    /// touches the peer's LRU entry and does not reserve budget), not a pure read.
    pub(super) fn leech_admit(&self, peer: &[u8; 32]) -> bool {
        self.leech_governor
            .as_ref()
            .is_none_or(|g| g.poll_admission(peer))
    }

    /// Account `bytes` speculatively pulled for `peer` under the seed-leech caps
    /// (#856). No-op when no governor is wired.
    pub(super) fn leech_record_pulled(&self, peer: &[u8; 32], bytes: u64) {
        if let Some(g) = self.leech_governor.as_ref() {
            g.record_pulled(peer, bytes);
        }
    }

    /// Handle a foreground pull-through deadline expiry (#859). Serves the blob if
    /// it landed in the store in the race; otherwise meters the abandoned pull and
    /// spawns a background warm before reporting the miss.
    ///
    /// A deadline expiry itself is a [`FillOutcome::CleanMiss`], NOT a
    /// [`FillOutcome::HardFault`] (#1129): we do not KNOW that anything is broken.
    /// The blob may well exist upstream and we simply ran out of patience — which
    /// is exactly why we spawn the background warm below. Reporting a slow upstream
    /// as `InternalError` ("do not retry this node") would steer clients off a
    /// perfectly healthy node because someone ELSE was slow. Only a genuine
    /// store/origin *error* is a fault, including the `has`-lookup error below.
    pub(super) async fn on_pull_through_timeout(
        &self,
        hash: Hash,
        timeout: Duration,
    ) -> FillOutcome {
        // Race: the fill may have landed in the store at the instant the outer
        // deadline fired. If so, serve it — this was NOT an abandoned pull, so do
        // not count a timeout. A `has` *error* is a real store fault, not a clean
        // race-loss: surface it like the populate-engine-error arm above rather
        // than silently treating the store as empty, then fall through to the warm.
        let mut faulted = false;
        match self.cache.has(hash).await {
            Ok(true) => return FillOutcome::Filled,
            Ok(false) => {}
            Err(e) => {
                // A store-lookup fault at the deadline is a store error, NOT a
                // timeout. Meter it as an error only — counting it as a timeout too
                // would contaminate `node_pull_through_timeout`, whose whole job is
                // to separate "slow/wedged upstream" from "broken store", and send
                // an operator chasing the network while the disk is dying. Same rule
                // `on_local_populate_timeout` states; the two used to disagree.
                self.metrics.node_pull_through_error();
                tracing::warn!(%hash, error = %e, "node-to-node pull-through store lookup failed after deadline");
                faulted = true;
            }
        }
        if !faulted {
            // Genuinely abandoned at the deadline (metered here, after the race
            // check, so a blob that landed in time isn't over-counted as a timeout):
            // this distinguishes a slow/wedged upstream from "not on network".
            self.metrics.node_pull_through_timeout();
            tracing::debug!(%hash, ?timeout, "node-to-node pull-through timed out");
        }
        // The foreground future was dropped (its partial pull discarded); keep
        // warming the cache in the background for future requests. Best-effort and
        // non-blocking — the client still gets a refusal now. Spawned on BOTH paths:
        // a store blip at the deadline is no reason to abandon the warm (this is why
        // the fault arm above cannot simply early-return like its local-only
        // sibling, which has no background warm to reach).
        self.maybe_spawn_background_fill(hash);
        if faulted {
            FillOutcome::HardFault
        } else {
            FillOutcome::CleanMiss
        }
    }
}
