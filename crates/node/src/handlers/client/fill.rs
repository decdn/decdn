//! Cache-fill tiers for the serve-miss path.

use decdn_protocol::{Coverage, byte_range_blocks, num_blocks};

use super::{
    Arc, CacheError, ClientHandler, Duration, FillOutcome, Hash, LaneDeliveryState, LaneId, Mutex,
    StreamRequest,
};
use tracing::Instrument as _;

impl ClientHandler {
    /// The lane that authorizes this request to make the node spend on a cache
    /// miss, or `None` when it may not. Every fill tier (own-origin, local
    /// populate, window and buffered pull-through, #831) and the pre-spend floor
    /// reservation are gated on it. Authority needs a verified client
    /// binding whose recovered address is the lane's `signer`.
    /// Pool *existence* is public (on-chain `PoolOpened`), so it cannot
    /// authorize spend — only proof of *voucher authority* can, since only the
    /// that signer can produce a voucher this lane will accept. Any capability
    /// holder can sign vouchers the pool accepts, but each on its own lane. An unbound
    /// request, or a binding that does not match that signer, must not make this
    /// node front upstream USDC or its own origin's egress.
    ///
    /// Spend authority is a *signer* question. In the shared-payment-pool model
    /// the lane is keyed by that signer directly, so authority reduces to "does
    /// a lane exist for `(pool_id, bound_signer, this operator)`?" — the seller
    /// resolves the lane from exactly that triple (brief §E1). `lane_key` is
    /// that triple (`None` for an unbound request) and `known_lane` is the lane
    /// `serve_stream` resolved for it, once, after the request's own capability
    /// intake. Authority is derived from that single resolution and never from a
    /// second registry read: a lane a sibling stream registers while this request
    /// is in flight cannot flip the answer between fill tiers (#2315). The
    /// returned pair is what the serve legs take, so a tier that is authorized
    /// always holds the lane it serves on.
    ///
    /// The spend-side origin-blacklist gate (ADR 011) is a separate *funder*
    /// question. ADR 011 sanctions the *money*: a takedown names the address that
    /// funded the delivery (`getPool.owner`), not whichever throwaway key happened
    /// to sign the vouchers. So a blacklisted delegate signer over a clean funder
    /// is not refused, and a clean delegate signer never launders a blacklisted
    /// funder. That gate is the funder check of the funding gates
    /// (`ClientHandler::funding_gates`), which run before every fill tier — a
    /// blacklisted funder is refused there before this authority is consulted,
    /// so no fill fronts USDC or origin egress on a blacklisted funder's
    /// behalf.
    pub(super) fn pull_authorized(
        lane_key: Option<LaneId>,
        known_lane: Option<&Arc<Mutex<LaneDeliveryState>>>,
    ) -> Option<(LaneId, &Arc<Mutex<LaneDeliveryState>>)> {
        lane_key.zip(known_lane)
    }

    /// Attempt to fill a cache miss by pulling from an upstream node (#831). The
    /// cache engine's `NodeOrigin` (last in the origin chain) does the discovery
    /// → discovery opens → ranked paid pull → populate; here we trigger it via
    /// `cache.populate` (which fills the store without returning the blob: an
    /// internal fill, not client egress),
    /// bounded by `timeout` so a slow upstream can't pin the delivery path. The
    /// subsequent normal delivery streams the populated bytes from the store and
    /// accounts the served-bytes metrics there. Returns whether the blob is now
    /// present locally, and — when it is not — whether the cause was a clean miss
    /// or a hard fault (#1129).
    pub(super) async fn try_pull_through(&self, hash: Hash, timeout: Duration) -> FillOutcome {
        let span = fill_span("node", hash);
        let outcome = self
            .node_pull_through(hash, timeout)
            .instrument(span.clone())
            .await;
        span.record("outcome", outcome.as_str());
        outcome
    }

    /// The body of [`Self::try_pull_through`], run inside its span.
    async fn node_pull_through(&self, hash: Hash, timeout: Duration) -> FillOutcome {
        match tokio::time::timeout(timeout, self.cache.populate(hash)).await {
            Ok(Ok(())) => FillOutcome::Filled,
            // A clean miss — no origin/provider had it — is the normal
            // unfillable case (`NotFound`/`NoOrigin`); log at debug and move on.
            Ok(Err(e @ (CacheError::NotFound { .. } | CacheError::NoOrigin { .. }))) => {
                tracing::debug!(%hash, error = %e.display_chain(), "node-to-node pull-through found no source");
                FillOutcome::CleanMiss
            }
            // A backend fault — the node is degraded, not empty. Report it as a fault so
            // the caller can refuse `InternalError` if no further tier fills (#1129),
            // rather than reporting a broken origin as a miss.
            //
            // This arm names neither lifetime: the node-origin also surfaces its own
            // buyer-side faults here (a broken signer, an unusable deadline config, an
            // unreadable pool store) as `OriginPullError::Permanent`, and those recur
            // for every hash until an operator acts. Telling an operator to wait for a
            // permanent defect to pass is worse than saying nothing.
            Ok(Err(
                e @ (CacheError::OriginError { .. }
                | CacheError::Store(_)
                | CacheError::Internal(_)),
            )) => {
                self.metrics.node_pull_through_error();
                tracing::warn!(%hash, error = %e.display_chain(), "node-to-node pull-through hit a backend fault");
                FillOutcome::HardFault
            }
            // Everything else (`BlobTooLarge`, `HashMismatch`, `VerifyFailed`,
            // `EvictionLimitExceeded`) is DETERMINISTIC: it will recur on every
            // request for this hash, so it is not evidence the node is degraded.
            // Reporting it as `InternalError` (wire `Declined`) would drop this
            // node for the hash over one bad upstream copy.
            // Meter it (the operator still needs to see it) but let it fall
            // through as a plain miss.
            Ok(Err(e)) => {
                self.metrics.node_pull_through_error();
                tracing::warn!(%hash, error = %e.display_chain(), "node-to-node pull-through hit a permanent cache-engine error");
                FillOutcome::CleanMiss
            }
            Err(_) => self.on_pull_through_timeout(hash, timeout).await,
        }
    }

    /// The partial-serve gate (#1506): whether `[byte_offset, byte_offset +
    /// byte_len)` is already fully present in the LOCAL cache of a blob
    /// `serve_audit` reports unserveable (not `Complete`) — the counterpart
    /// to the will-serve coverage a partial holder advertises in its discovery
    /// answers and DHT records. Returns the blob's total size when so, letting the caller
    /// skip straight to the size gate + delivery instead of running the
    /// miss-path fill legs; `None` otherwise.
    ///
    /// `None` covers three cases the caller treats identically — this is a
    /// best-effort optimization, never a decline path, so all fall through
    /// to the ordinary miss path unchanged: the requested span is genuinely
    /// not fully cached, the hash is absent/evicted/refused, or the node has
    /// no proof of the size.
    ///
    /// The size comes from the `observe()` bitfield
    /// ([`CacheEngine::present_ranges`](super::CacheEngine::present_ranges)),
    /// the SAME source [`CacheEngine::coverage`](super::CacheEngine::coverage)
    /// sizes a partial from when deciding what to advertise. Unlike
    /// `status()`/`inspect`, the bitfield knows a `Partial` blob's size as
    /// soon as any chunk carries it, not only once the FINAL chunk validates
    /// — so a front-prefix partial (holds the front, not the tail) sizes its
    /// range here too, matching what it already advertises as covered.
    /// Sizing serve and advertise from the same source matters: two different
    /// sources here would let the node advertise a block it then refuses to
    /// serve (#1506).
    ///
    /// Reuses [`CacheEngine::missing_ranges`](super::CacheEngine::missing_ranges)'s
    /// own size contract — the same `blob_size` the own-origin spine resolves
    /// via [`CacheEngine::origin_presence`](super::CacheEngine::origin_presence) for the
    /// ORIGIN-backed case; here the size comes from the LOCAL store's own
    /// bitfield, since a purely local partial has no origin to probe.
    pub(super) async fn partial_hit_size(
        &self,
        hash: Hash,
        byte_offset: u64,
        byte_len: u64,
    ) -> Option<u64> {
        let size = self.cache.present_ranges(hash).await.ok()?.size();
        if size == 0 {
            return None;
        }
        // The node serves under a size only with its proof (ADR 005 § Bounded
        // byte ranges). A partial with no kept proof and no tail falls
        // through to the miss path, whose upstream pull brings the proof.
        self.cache.size_proof(hash, size).await?;
        let missing = self
            .cache
            .missing_ranges(hash, byte_offset, byte_len, size)
            .await
            .ok()?;
        missing.is_empty().then_some(size)
    }

    /// The coverage a miss of `req` that asks for no pull-through refuses
    /// with: the discovery blocks the store holds whole, so a partial holder
    /// still reads as a holder of those blocks. The span the request asked
    /// for is a miss, so the held blocks do not cover it; a coverage that
    /// would ([`decdn_protocol::client::StreamResponseExt::consistent_with`])
    /// is left out, and so is one a store read fails to give.
    pub(super) async fn miss_coverage(&self, hash: Hash, req: &StreamRequest) -> Coverage {
        let (held, size) = match self.cache.coverage_sized(hash).await {
            Ok(sized) => sized,
            Err(e) => {
                tracing::debug!(
                    %hash,
                    error = %decdn_cache::ErrorChain::new(&e),
                    "miss: the store coverage read failed; refusing with no coverage"
                );
                return Coverage::empty();
            }
        };
        let Some(size) = size else {
            return Coverage::empty();
        };
        let coverage = Coverage::for_wire(num_blocks(size), held.covered_blocks());
        let refusal = decdn_protocol::client::StreamResponseExt::refused(
            decdn_protocol::client::StreamError::NotFound,
            coverage.clone(),
        );
        let body = decdn_protocol::client::StreamResponseBody {
            hash: req.hash,
            ok: false,
            rate_per_mb: 0,
            timestamp_us: req.timestamp_us,
        };
        if refusal.consistent_with(&body, req.byte_offset, req.byte_len) {
            coverage
        } else {
            Coverage::empty()
        }
    }

    /// The coverage a covering partial hit of `req` answers with: the
    /// discovery blocks the store holds whole, plus every block the requested
    /// span touches.
    ///
    /// The node holds every byte of the span ([`Self::partial_hit_size`]), but
    /// the span can hold only part of a block, and the store counts a block
    /// only when it holds all of it. The span's blocks keep the answer
    /// covering what it serves. A store read that fails leaves the span's
    /// blocks alone.
    pub(super) async fn partial_hit_coverage(
        &self,
        hash: Hash,
        req: &StreamRequest,
        total_bytes: u64,
    ) -> Coverage {
        let held = self.cache.coverage(hash).await.unwrap_or_else(|e| {
            tracing::warn!(
                %hash,
                error = %decdn_cache::ErrorChain::new(&e),
                "partial hit: the store coverage read failed; advertising the served span only"
            );
            Coverage::empty()
        });
        let end = if req.byte_len == 0 {
            total_bytes
        } else {
            req.byte_offset
                .saturating_add(req.byte_len)
                .min(total_bytes)
        };
        let span = byte_range_blocks(req.byte_offset, end).filter_map(|i| u32::try_from(i).ok());
        Coverage::for_wire(num_blocks(total_bytes), held.covered_blocks().chain(span))
    }

    /// Handle a foreground pull-through deadline expiry (#859). Serves the blob if
    /// it landed in the store in the race; otherwise meters the abandoned pull and
    /// reports the miss.
    ///
    /// A deadline expiry itself is a [`FillOutcome::CleanMiss`], NOT a
    /// [`FillOutcome::HardFault`] (#1129): we do not KNOW that anything is broken.
    /// The blob may well exist upstream and we simply ran out of patience. Reporting
    /// a slow upstream as `InternalError` (wire `Declined`) would drop a healthy
    /// node for the hash because someone ELSE was slow. Only a
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
                // an operator chasing the network while the disk is dying.
                self.metrics.node_pull_through_error();
                tracing::warn!(%hash, error = %e.display_chain(), "node-to-node pull-through store lookup failed after deadline");
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

/// The span around one cache-fill attempt for a serve miss. `tier` is `node`
/// (a paid pull from an upstream deCDN node) or `local` (the operator's own
/// origin). `outcome` is recorded once the attempt ends.
fn fill_span(tier: &'static str, hash: Hash) -> tracing::Span {
    tracing::info_span!(
        "pull_through",
        tier,
        %hash,
        outcome = tracing::field::Empty,
    )
}

/// The coverage a node answers with when it serves every block of a
/// `total_bytes`-byte blob: a complete hit, an own-origin fill or a
/// pull-through. Cut at the wire bound ([`Coverage::for_wire`]), so a blob
/// past `MAX_DISCOVERABLE_BLOB_BYTES` still gets an answer the payer decodes.
pub(super) fn full_coverage(total_bytes: u64) -> Coverage {
    let blocks = num_blocks(total_bytes);
    Coverage::for_wire(blocks, 0..blocks)
}
