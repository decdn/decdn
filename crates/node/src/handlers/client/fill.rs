//! Cache-fill tiers for the serve-miss path.
//! Bodies split from `mod.rs` (#1254).

use super::{
    Address, B256, CacheError, ClientHandler, Duration, FillOutcome, Hash, LaneKey,
    RangePullOutcome, StreamRequest,
};

impl ClientHandler {
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
    /// Also a spend-side origin-blacklist gate (ADR 011), keyed on the channel's
    /// FUNDER — the same subject `dispatch.rs`'s serve gate uses, so the two
    /// paths agree. ADR 011 sanctions the *money*: a takedown names the address
    /// that funded the delivery, not whichever throwaway key happened to sign
    /// the vouchers. The two questions are deliberately not conflated — spend
    /// authority is a *signer* question, compliance is a *funder* question — so
    /// a blacklisted delegate signer over a clean funder is NOT refused here,
    /// and a clean delegate signer never launders a blacklisted funder.
    ///
    /// Refusing the *spend* is the part that actually costs the operator:
    /// without this check a blacklisted funder's request could still make this
    /// node front upstream USDC egress and warm its cache on that funder's
    /// behalf, only for the delivery to be refused afterwards.
    pub(super) async fn pull_authorized(
        &self,
        req: &StreamRequest,
        verified_client: Option<Address>,
    ) -> bool {
        let Some(client) = verified_client else {
            return false;
        };
        // Spend authority is a *signer* question: only the capability's pinned
        // signer can pay for this upstream pull, so only it can authorize the
        // spend. In the shared-payment-pool model the lane is keyed by that
        // signer directly, so authority reduces to "does a lane exist for
        // `(pool_id, bound_signer, this operator)`?" — the seller resolves the
        // lane from exactly that triple (brief §E1).
        let lane_key = LaneKey {
            pool_id: B256::from(req.pool_id),
            signer: client,
            provider: self.eth_signer.address(),
        };
        // This is a lane-membership (spend-authority) check only. The spend-side
        // origin-blacklist gate (ADR 011) keys on the pool FUNDER
        // (`getPool.owner`), and it runs at the serve gate's open-time funder
        // check in `dispatch.rs`, which precedes every fill tier — a blacklisted
        // funder is refused there before this authority check is ever reached, so
        // no fill fronts USDC on a blacklisted funder's behalf.
        self.lanes.lock().await.contains_key(&lane_key)
    }

    /// Map a failed fill to its [`FillOutcome`], and log/meter it. Shared by
    /// both fill tiers: `tier` names which one for the log line, and is the only
    /// thing that differs between them — the classification itself must NOT,
    /// since the client-visible consequence of each error is a property of the
    /// error, not of which tier hit it.
    ///
    /// The four-way split is load-bearing and each boundary has cost a bug:
    ///
    /// - **Clean miss** (`NotFound` / `NoOrigin`): nobody had it. Not evidence of
    ///   anything; leave the terminal answer to the other tiers.
    /// - **Hard fault** (`OriginError` / `Store`): this node is degraded, not
    ///   empty — the caller must refuse `InternalError` rather than sign a
    ///   `NotFound` that claims the blob does not exist (#1129). Deliberately
    ///   narrow: only these two. Note it is NOT necessarily transient — since
    ///   #1560 the node-origin also surfaces permanent buyer-side faults (broken
    ///   signer, unusable deadline config, unreadable channel store) as
    ///   `OriginPullError::Permanent` through this arm, and those recur for every
    ///   hash until an operator acts. So the log line names neither lifetime.
    /// - **Too large** (`BlobTooLarge`, #1678): the blob does not fit this node's
    ///   disk budget. Deterministic like the arm below, but it earns its own
    ///   outcome because the client can act on it — `BlobTooLarge` means "never
    ///   ask me for this blob", where a miss means "maybe later".
    /// - **Everything else** (`HashMismatch`, `VerifyFailed`,
    ///   `EvictionLimitExceeded`): deterministic for this hash but not evidence
    ///   the node is broken. Reporting `InternalError` would steer clients off a
    ///   healthy node permanently over one bad blob. Metered, then treated as a
    ///   plain miss.
    fn classify_fill_error(&self, hash: Hash, err: &CacheError, tier: &str) -> FillOutcome {
        let outcome = FillOutcome::for_cache_error(err);
        if FillOutcome::is_reportable_fill_error(err) {
            self.metrics.node_pull_through_error();
            tracing::warn!(%hash, error = %err, "{tier}: {}", outcome.fill_failure_note());
        } else {
            tracing::debug!(%hash, error = %err, "{tier}: {}", outcome.fill_failure_note());
        }
        outcome
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
            Ok(Err(e)) => self.classify_fill_error(hash, &e, "node-to-node pull-through"),
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
    /// signed `NotFound` (#1129). A timeout is a clean miss: nothing is warmed in
    /// the background (#1610 removed the detached warm), so the blob is re-fetched
    /// on the next real client request (see [`Self::on_local_populate_timeout`]).
    pub(super) async fn try_local_populate(&self, hash: Hash, timeout: Duration) -> FillOutcome {
        match tokio::time::timeout(timeout, self.cache.populate_local(hash)).await {
            Ok(Ok(())) => FillOutcome::Filled,
            Ok(Err(e)) => self.classify_fill_error(hash, &e, "reactive local-origin pull-through"),
            Err(_) => self.on_local_populate_timeout(hash, timeout).await,
        }
    }

    /// Handle a reactive local-origin populate deadline expiry (#1116). Serves the
    /// blob if a concurrent fill landed it in the store at the instant the
    /// deadline fired (so a node with node→node OFF doesn't report `CacheMiss` for
    /// a blob that is now present), otherwise meters the timeout and reports the
    /// miss.
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

    /// Whether a speculative pull may proceed for `peer` under the seed-leech caps
    /// (#856). Always `true` when no governor is wired. Like
    /// [`LeechGovernor::poll_admission`](super::LeechGovernor::poll_admission) this is a stateful, advisory poll (it
    /// touches the peer's LRU entry and does not reserve budget), not a pure read.
    pub(super) fn leech_admit(&self, peer: &[u8; 32]) -> bool {
        self.leech_governor
            .as_ref()
            .is_none_or(|g| g.poll_admission(peer))
    }

    /// Handle a foreground pull-through deadline expiry (#859). Serves the blob if
    /// it landed in the store in the race; otherwise meters the abandoned pull and
    /// reports the miss.
    ///
    /// A deadline expiry itself is a [`FillOutcome::CleanMiss`], NOT a
    /// [`FillOutcome::HardFault`] (#1129): we do not KNOW that anything is broken.
    /// The blob may well exist upstream and we simply ran out of patience. Reporting
    /// a slow upstream as `InternalError` ("do not retry this node") would steer
    /// clients off a perfectly healthy node because someone ELSE was slow. Only a
    /// genuine store/origin *error* is a fault, including the `has`-lookup error
    /// below.
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
        if faulted {
            FillOutcome::HardFault
        } else {
            FillOutcome::CleanMiss
        }
    }
}
