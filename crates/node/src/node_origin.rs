//! `NodeOrigin` — the node-to-node cache-miss pull-through origin (#831, ADR
//! 001/022).
//!
//! `CacheEngine` ingests blobs only through the [`Origin`] trait, so the
//! production shape of "on a miss, discover a provider, open a paid channel,
//! pull, and populate the cache" is an [`Origin`] implementation injected into
//! the engine's origin chain (appended last, so configured HTTP/FS/S3 origins
//! are tried first and the paid network pull is the final fallback). On
//! [`Origin::fetch`] this:
//!
//! 1. discovers providers for the hash (DHT [`crate::dht::find_providers`],
//!    with the origin-directory fallback),
//! 2. probes each candidate for rate + RTT and ranks them by the combined
//!    local+network reputation score ([`crate::selection::rank_candidates`]),
//! 3. opens (or reuses) a buyer payment channel to the best candidate and pulls
//!    via [`crate::client_requester::stream_fetch`], falling back through up to
//!    [`crate::selection::MAX_PROVIDER_ATTEMPTS`] providers,
//! 4. records the per-provider [`Outcome`] into the local reputation score and
//!    the observation buffer, so the gossip publisher emits reports about the
//!    upstreams this node pulled from (ADR 008 §Local Score / §Gossip Protocol).
//!
//! The cache engine verifies the returned bytes against the content hash and
//! ingests them, and `stream_fetch` verifies every chunk group of the bao
//! verified-stream against the content root (ADR 038), so a dishonest provider
//! is detected (and scored [`Outcome::Corruption`]) rather than surfaced to the
//! caller. On the window path the corruption detector is the cache TEE's
//! verifying decoder; its verdict reaches the scorer via
//! [`NodeProgressivePull::finish`]'s [`TeeVerdict`] / `abandon_corrupt` (#915).
//!
//! # Deferred initialisation
//!
//! The engine is constructed early in runtime bring-up — before the endpoint,
//! DHT, buyer-channel service, and reputation handles `NodeOrigin` depends on
//! exist — and takes its origin chain only at construction (there is no
//! `add_origin`). So `NodeOrigin` is built empty, placed in the chain, and its
//! dependencies are injected later via a write-once [`OnceLock`] (see
//! [`NodeOrigin::provision`]). Until that set lands — the feature is off, or the
//! buyer bootstrap failed — `fetch` returns [`OriginFetch::NotFound`], a clean
//! miss that leaves the handler behaving exactly as it did before pull-through.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use bytes::Bytes;
use decdn_cache::origin::{Origin, OriginFetch};
use decdn_cache::{Hash, OriginKind, OriginPullError};
use decdn_protocol::ReportMetrics;
use iroh::{Endpoint, EndpointAddr, PublicKey};
use tracing::{debug, warn};

use decdn_reputation::{
    LocalReputation, NetworkReputation, NetworkReputationConfig, ObservationBuffer, Outcome,
    combined_score,
};

use decdn_incentive::ChannelOpenFailureReason;

use crate::buyer_channel::ChannelOpener;
use crate::client_requester::{
    BlobTooLargeClaim, ChannelContext, HashMismatch, PullTimeout, UpstreamPull, UpstreamPullHeader,
    UpstreamVoucherRejected, VoucherProgress, open_progressive_pull as open_progressive_upstream,
    sign_client_binding, stream_fetch_tracked,
};
use crate::dht::negative_cache::Hash as DhtHash;
use crate::dht::routing::{NodeId as DhtNodeId, RoutingTable};
use crate::dht::{
    LookupConfig, NegativeProbeCache, NodeAddressResolver, OriginDirectory, StakerSet,
};
use crate::metrics::Metrics;
use crate::probe_client::probe_once;
use crate::selection::{Candidate, MAX_PROVIDER_ATTEMPTS, rank_candidates};

/// Per-candidate probe timeout. Short relative to the pull timeout — a probe is
/// a single unpaid round trip, so a slow candidate is dropped quickly rather
/// than burning the caller's miss-latency budget on it.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Record a buyer channel open/reuse failure on `err` to the metrics in `deps`,
/// emitting a structured-log line with the failure-class `reason` (#966).
///
/// Bumps the unlabeled `node_pull_channel_open_failures` total and, when the
/// error chain carries a [`ChannelOpenFailureReason`] (attached by the
/// `open_channel` kernel for the three `openChannel`-tx failure classes), the
/// matching `decdn_channel_open_failures_{reason}_total` sibling counter. A
/// failure with no attached reason — a store fault or an unreclaimed-expired
/// channel that aborted before the `openChannel` tx — still lands in the
/// unlabeled total and logs `reason="unclassified"`.
fn record_channel_open_failure(deps: &NodeOriginDeps, provider_addr: Address, err: &anyhow::Error) {
    deps.metrics.node_pull_channel_open_failure();
    let reason = err.downcast_ref::<ChannelOpenFailureReason>().copied();
    if let Some(reason) = reason {
        deps.metrics.channel_open_failure_by_reason(reason);
    }
    debug!(
        %provider_addr,
        reason = reason.map_or("unclassified", ChannelOpenFailureReason::as_label),
        %err,
        "node-origin: buyer channel open/reuse failed"
    );
}

/// Post-pull cost observer (#820). Invoked once per pull that acked any voucher
/// — a successful delivery OR a paid-but-failed one (whose watermark still
/// advanced, #852) — so the prefetch budget accounts for all spend, not just
/// successes. It fires for ALL pulls (demand-miss and prefetch alike), so the
/// observer itself filters to the pulls it cares about. `[u8; 32]` keeps this
/// trait free of any cache/DHT hash type.
pub trait AcquisitionObserver: Send + Sync + std::fmt::Debug {
    /// `micro_usdc` and `bytes` are the deltas for *this* pull (the channel
    /// watermark minus its prior cumulative), not the channel running totals.
    /// One pull is one blob/stream, so `bytes` is this pull's delivered length —
    /// bao WIRE bytes (content plus interleaved proof, ADR 038), the same unit the
    /// voucher watermark advances in, since it is derived from that watermark.
    fn on_pull(&self, hash: [u8; 32], micro_usdc: u64, bytes: u64);
}

/// Tuning knobs for the node-to-node pull, resolved from `[cache]` config.
#[derive(Debug, Clone)]
pub struct NodeOriginConfig {
    /// How many discovered providers to probe before ranking.
    pub probe_fanout: usize,
    /// Wall-clock bound on a single upstream pull (`stream_fetch`).
    pub pull_timeout: Duration,
    /// Buyer-side blob-size ceiling (`cache.max_blob_size_mb` × MB), `0` = unlimited.
    /// Mirrors the serving-side `BlobTooLarge` gate; rejects an oversized server
    /// `total_bytes` claim before buffering (#840).
    pub max_blob_size_bytes: u64,
    /// ADR 015 master switch (`network.enable_0rtt`) for the probe handshake.
    pub enable_0rtt: bool,
    /// Desired deposit for a freshly-opened buyer channel
    /// (`blockchain.buyer_deposit_micro_usdc`); ignored when a channel is reused.
    pub deposit_hint: U256,
    /// DHT lookup tuning.
    pub lookup: LookupConfig,
}

/// Dependencies the orchestration needs, injected once after runtime bring-up
/// completes (see the module docs). Built and handed to [`NodeOrigin::provision`]
/// by the runtime.
pub struct NodeOriginDeps {
    /// The node's shared iroh endpoint (dials probes + pulls).
    pub endpoint: Endpoint,
    /// DHT routing table handle for `find_providers`.
    pub routing_table: Arc<Mutex<RoutingTable>>,
    /// Active-staker set (lookup integrity filter).
    pub staker_set: Arc<dyn StakerSet>,
    /// On-chain origin-directory fallback when the DHT returns no providers.
    pub origin_directory: Arc<dyn OriginDirectory>,
    /// Resolves a provider `NodeId` to its bonded operator Ethereum address.
    pub addr_resolver: Arc<dyn NodeAddressResolver>,
    /// Buyer-side payment-channel service (opens/reuses the upstream channel).
    pub buyer: Arc<dyn ChannelOpener>,
    /// This node's DHT id, the lookup requester.
    pub self_id: DhtNodeId,
    /// EIP-712 domain verifying the delivery `slash_sig` (ADR 014 §1).
    pub slash_domain: Eip712Domain,
    /// `CapacityBond` EIP-712 bind domain (ADR 005). Used to sign the client
    /// identity binding this node attaches — over its OWN iroh `NodeId` with
    /// the buyer key — to every node→node pull, so an upstream node can prove
    /// this node owns the named channel and, on its own cache miss, chain a
    /// further reactive origin pull (`pull_authorized`, #1117). Built from
    /// `chain_id` + the `CapacityBond` address, identical to the domain the
    /// serving side verifies against.
    pub bind_domain: Eip712Domain,
    /// Local per-peer reputation score store (folded on each pull outcome).
    pub local_rep: Arc<LocalReputation>,
    /// Outbound observation buffer the gossip publisher drains.
    pub obs_buffer: Arc<ObservationBuffer>,
    /// Aggregated network reputation (read for the combined selection score).
    pub network_rep: Arc<NetworkReputation>,
    /// Reputation blend weights for `combined_score`.
    pub rep_cfg: NetworkReputationConfig,
    /// Requester-side negative-probe cache (drops known-absent providers).
    pub negative_cache: NegativeProbeCache,
    /// Node metrics for the paid-pull observability counters (#831).
    pub metrics: Arc<Metrics>,
    /// Per-region byte accountant; the inbound (`bytes_in`) counterpart of the
    /// serve path's `record_served`. Fed on each delivered pull (#858).
    pub region_accountant: Arc<crate::region_accounting::RegionAccountant>,
    /// Resolved pull tuning.
    pub config: NodeOriginConfig,
    /// Optional post-pull cost observer (#820). `None` on a node without
    /// prefetch; `Some` feeds the prefetch acquisition ledger.
    pub acquisition_observer: Option<Arc<dyn AcquisitionObserver>>,
}

impl std::fmt::Debug for NodeOriginDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeOriginDeps")
            .field("self_id", &self.self_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Node-to-node pull-through [`Origin`]. Cheap to clone via the shared inner
/// [`Arc`]; the runtime holds one and injects its dependencies once.
#[derive(Debug, Clone)]
pub struct NodeOrigin {
    deps: Arc<OnceLock<NodeOriginDeps>>,
}

impl NodeOrigin {
    /// Build an unprovisioned origin. `fetch` is a clean miss until
    /// [`Self::provision`] supplies the dependencies.
    #[must_use]
    pub fn new() -> Self {
        Self {
            deps: Arc::new(OnceLock::new()),
        }
    }

    /// Supply the dependencies, enabling pull-through. Idempotent-safe: a second
    /// call is ignored with a warning (the write-once `OnceLock` keeps the first
    /// set), so a misconfigured double-provision can't silently swap deps under
    /// an in-flight fetch.
    pub fn provision(&self, deps: NodeOriginDeps) {
        if self.deps.set(deps).is_err() {
            warn!("NodeOrigin provisioned more than once; keeping the first dependency set");
        }
    }

    /// Open a window-paced progressive pull for `hash` (#856), the streaming
    /// counterpart of [`Origin::fetch`]'s buffered pull. Runs the same
    /// discover → probe → rank → open-channel pipeline, but instead of buffering
    /// the whole blob it returns the upstream header (so the caller can sign its
    /// own `StreamResponse`) and a live [`NodeProgressivePull`] the caller drives
    /// chunk-by-chunk — forwarding each chunk to the paying downstream client and
    /// teeing it into the cache — so per-request speculative exposure is bounded
    /// to the caller's window rather than the entire upstream cost.
    ///
    /// Candidate fallback happens at OPEN time only: it walks the ranked
    /// candidates until one successfully opens (handshake + verified response),
    /// because once the caller starts forwarding it is committed to that
    /// upstream's `total_bytes`. Returns `None` if pull-through is unprovisioned,
    /// no provider is reachable, or every candidate declined.
    pub async fn open_progressive_pull(
        &self,
        hash: Hash,
    ) -> Option<(UpstreamPullHeader, NodeProgressivePull)> {
        let deps = self.deps.get()?;
        let hash_bytes = *hash.as_bytes();
        let providers = discover(deps, hash_bytes).await;
        if providers.is_empty() {
            deps.metrics.node_pull_no_providers();
            debug!(%hash, "node-origin: no providers discovered for window-paced pull");
            return None;
        }
        deps.metrics.node_pull_attempt();
        let ranked = probe_and_rank(deps, providers, hash_bytes).await;
        for candidate in ranked.iter().take(MAX_PROVIDER_ATTEMPTS) {
            if let Some(opened) = self.open_from_candidate(deps, candidate, hash_bytes).await {
                return Some(opened);
            }
        }
        None
    }

    /// Resolve, open/reuse a channel, and open a progressive upstream pull from
    /// one candidate. Returns the header + driver on a clean open; `None`
    /// (try the next candidate) on an unresolvable address, channel-open failure,
    /// or a declined/erroring response (classified like the buffered path).
    async fn open_from_candidate(
        &self,
        deps: &NodeOriginDeps,
        candidate: &Candidate,
        hash_bytes: [u8; 32],
    ) -> Option<(UpstreamPullHeader, NodeProgressivePull)> {
        let Ok(pk) = PublicKey::from_bytes(&candidate.node_id) else {
            return None;
        };
        let Some(provider_addr) = deps
            .addr_resolver
            .address_of(&DhtNodeId::from_bytes(candidate.node_id))
        else {
            debug!("node-origin: candidate has no resolvable operator address; skipping");
            return None;
        };
        let ctx = match deps
            .buyer
            .open_or_reuse_channel(provider_addr, deps.config.deposit_hint)
            .await
        {
            Ok(ctx) => ctx,
            Err(err) => {
                record_channel_open_failure(deps, provider_addr, &err);
                return None;
            }
        };
        // #1117: bind the request so the upstream can chain a reactive pull.
        let ctx = bind_upstream_ctx(deps, ctx)?;
        match open_progressive_upstream(
            &deps.endpoint,
            EndpointAddr::new(pk),
            &ctx,
            &deps.slash_domain,
            provider_addr,
            hash_bytes,
            0,
            now_micros(),
            deps.config.max_blob_size_bytes,
        )
        .await
        {
            Ok((header, pull)) => Some((
                header,
                NodeProgressivePull {
                    deps: Arc::clone(&self.deps),
                    pull,
                    pk,
                    provider_addr,
                    channel_id: ctx.channel_id,
                    started: Instant::now(),
                    delivered: 0,
                    node_id: candidate.node_id,
                    hash_bytes,
                    prior_amount: ctx.prior_amount,
                    prior_bytes_delivered: ctx.prior_bytes_delivered,
                },
            )),
            Err(err) => {
                // No bytes were forwarded and no voucher was paid yet, so there is
                // nothing to persist; just classify and try the next candidate.
                classify_pull_failure(deps, pk, provider_addr, &err);
                None
            }
        }
    }
}

/// The cache tee's integrity verdict for a window pull-through fill (#915,
/// ADR 038). Under bao streaming the TEE's verifying decoder — not the wire
/// pull — is the corruption detector (`UpstreamPull::finish` checks only
/// wire-byte completeness), so the serve handler settles the tee first and
/// passes its verdict into [`NodeProgressivePull::finish`] for reputation
/// scoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeeVerdict {
    /// The teed bao stream verified against the content root — or failed only
    /// for a local, non-integrity reason (store fault, size cap), which says
    /// nothing bad about the upstream.
    Verified,
    /// The teed bao stream FAILED verification: the upstream served bytes that
    /// do not hash to the requested root (`CacheError::HashMismatch`).
    Corrupt,
}

/// A live window-paced node→node pull (#856) handed to the `cdn/client/v1`
/// serve path. Wraps the [`UpstreamPull`] transport with the node-origin
/// bookkeeping (buyer-watermark persistence #852, reputation scoring, region
/// accounting) so the serve handler only has to pump chunks and call one
/// terminal method. Obtain via [`NodeOrigin::open_progressive_pull`].
#[derive(Debug)]
pub struct NodeProgressivePull {
    deps: Arc<OnceLock<NodeOriginDeps>>,
    pull: UpstreamPull,
    pk: PublicKey,
    provider_addr: Address,
    channel_id: B256,
    started: Instant,
    /// Bytes pulled (and forwarded) on this stream — the region/reputation count.
    delivered: u64,
    /// Candidate node id, for region accounting.
    node_id: [u8; 32],
    /// Blob hash, for the prefetch acquisition-ledger feed (#820).
    hash_bytes: [u8; 32],
    /// Channel cumulative amount/bytes BEFORE this pull, so the finalize path can
    /// compute this pull's spend/byte delta for the prefetch ledger (#820).
    prior_amount: U256,
    prior_bytes_delivered: U256,
}

impl NodeProgressivePull {
    /// The promised **wire** byte count of this pull — the bao-encoded size of
    /// the blob (content plus interleaved proof, ADR 038), forwarded verbatim to
    /// the downstream client. The window serve loop uses it as the pull budget
    /// `total`, since the forwarded/metered quantities are wire bytes.
    #[must_use]
    pub const fn expected_wire_bytes(&self) -> u64 {
        self.pull.expected_wire_bytes()
    }

    /// Read and forward the next upstream chunk, paying the upstream per voucher
    /// interval. `Ok(None)` signals the upstream `StreamEnd`. Tracks delivered
    /// bytes for the success-path region/reputation accounting.
    ///
    /// # Errors
    ///
    /// Propagates [`UpstreamPull::next_chunk`] errors (chunk-size / overrun /
    /// mid-stream `StreamError` / voucher rejection).
    pub async fn next_chunk(&mut self) -> anyhow::Result<Option<Bytes>> {
        let chunk = self.pull.next_chunk().await?;
        if let Some(bytes) = &chunk {
            self.delivered = self.delivered.saturating_add(bytes.len() as u64);
        }
        Ok(chunk)
    }

    /// Finalize a cleanly-completed pull: run the upstream wire-byte
    /// completeness check, persist the buyer watermark (#852), and score the
    /// provider. Under ADR 038 the wire `finish` carries no content
    /// verification — the CACHE TEE's bao decoder is the integrity detector —
    /// so the caller passes the tee's verdict in and the score reflects it:
    /// `Delivered` + region accounting for a verified fill, `Corruption` for a
    /// wire-complete stream whose bytes failed bao verification (the
    /// paid-but-corrupt case the old whole-blob hasher used to catch here).
    ///
    /// # Errors
    ///
    /// A short wire delivery or stream/protocol error from
    /// [`UpstreamPull::finish`] (classified as a transport failure — a tee
    /// `Corrupt` verdict cannot normally co-occur with a wire error, since a
    /// truncated tee feed classifies as transport, not corruption). The
    /// watermark is persisted either way. A `Corrupt` verdict on a complete
    /// wire returns `Ok` — the caller already holds the tee error and decides
    /// the wire-protocol consequence (no `StreamEnd`).
    pub async fn finish(self, tee_verdict: TeeVerdict) -> anyhow::Result<()> {
        let Self {
            deps,
            pull,
            pk,
            provider_addr,
            channel_id,
            started,
            delivered,
            node_id,
            hash_bytes,
            prior_amount,
            prior_bytes_delivered,
        } = self;
        // Capture the acked watermark before `finish` consumes the pull so a
        // paid-but-corrupt delivery is still persisted (#852).
        let watermark = pull.progress();
        let verify = pull.finish().await;
        let elapsed = started.elapsed();
        let Some(deps) = deps.get() else {
            // Unprovisioned under us (cannot happen in practice — we got here via
            // a provisioned open) — surface the verify result without scoring.
            return verify.map(|_| ());
        };
        persist_buyer_progress(deps, provider_addr, channel_id, &watermark);
        // Feed the prefetch ledger (#820) on both Ok and Err — see the buffered
        // path; the window path is demand-miss today, so the observer no-ops, but
        // wiring it keeps the ledger correct if a prefetch pull is ever routed here.
        feed_acquisition_observer(
            deps,
            hash_bytes,
            &watermark,
            prior_amount,
            prior_bytes_delivered,
        );
        match verify {
            Ok(_) => {
                match tee_verdict {
                    TeeVerdict::Verified => {
                        record_outcome(
                            deps,
                            pk,
                            &Outcome::Delivered {
                                bytes: delivered,
                                elapsed,
                            },
                        );
                        deps.region_accountant
                            .record_pulled(&node_id, delivered)
                            .await;
                    }
                    TeeVerdict::Corrupt => {
                        // Paid-but-corrupt: the upstream delivered the promised
                        // wire bytes but they failed bao verification. Score the
                        // corruption against the PROVIDER (it is the party that
                        // served the bytes) so the observation propagates via
                        // gossip — without this, a lying upstream banks a
                        // `Delivered` while the downstream client blames US for
                        // the corrupt forward (#915 review).
                        warn!(
                            provider = %pk, %provider_addr, delivered,
                            "window pull-through upstream served wire-complete but bao-corrupt bytes; scoring Corruption"
                        );
                        record_outcome(deps, pk, &Outcome::Corruption);
                    }
                }
                Ok(())
            }
            Err(err) => {
                classify_pull_failure(deps, pk, provider_addr, &err);
                Err(err)
            }
        }
    }

    /// Abandon the pull because the teed bao stream failed verification
    /// MID-fill (#915): the tee's import rejected a chunk group while the
    /// forward loop was still writing, so the upstream was paid for bytes that
    /// do not hash to the content root. Persists the watermark (#852), scores
    /// the provider `Corruption`, and closes the upstream connection. The
    /// mid-stream sibling of the `TeeVerdict::Corrupt` arm of [`Self::finish`].
    pub fn abandon_corrupt(self) {
        let Self {
            deps,
            pull,
            pk,
            provider_addr,
            channel_id,
            hash_bytes,
            prior_amount,
            prior_bytes_delivered,
            ..
        } = self;
        let watermark = pull.abort();
        let Some(deps) = deps.get() else {
            return;
        };
        persist_buyer_progress(deps, provider_addr, channel_id, &watermark);
        feed_acquisition_observer(
            deps,
            hash_bytes,
            &watermark,
            prior_amount,
            prior_bytes_delivered,
        );
        warn!(
            provider = %pk, %provider_addr,
            "window pull-through upstream served bao-corrupt bytes mid-stream; scoring Corruption"
        );
        record_outcome(deps, pk, &Outcome::Corruption);
    }

    /// Abandon the pull (the downstream client dropped, underpaid, or a
    /// `next_chunk` errored). Persists whatever was paid (#852) and, when a
    /// `cause` error is supplied, scores the provider for it. Closes the upstream
    /// connection so we stop receiving and paying immediately.
    pub fn abandon(self, cause: Option<&anyhow::Error>) {
        let Self {
            deps,
            pull,
            pk,
            provider_addr,
            channel_id,
            hash_bytes,
            prior_amount,
            prior_bytes_delivered,
            ..
        } = self;
        let watermark = pull.abort();
        let Some(deps) = deps.get() else {
            return;
        };
        persist_buyer_progress(deps, provider_addr, channel_id, &watermark);
        // A paid-but-abandoned pull still advanced the watermark (#852); feed its
        // spend to the prefetch ledger (#820) for the same reason as the buffered
        // path's Err arm — see `feed_acquisition_observer`.
        feed_acquisition_observer(
            deps,
            hash_bytes,
            &watermark,
            prior_amount,
            prior_bytes_delivered,
        );
        if let Some(err) = cause {
            classify_pull_failure(deps, pk, provider_addr, err);
        }
    }
}

impl Default for NodeOrigin {
    fn default() -> Self {
        Self::new()
    }
}

impl Origin for NodeOrigin {
    fn fetch(
        &self,
        hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        let deps_lock = Arc::clone(&self.deps);
        Box::pin(async move {
            let Some(deps) = deps_lock.get() else {
                // Pull-through not provisioned (feature disabled or the buyer
                // bootstrap failed). A clean miss — the engine surfaces NotFound
                // to the handler, which behaves as it did pre-#831. The engine
                // enforces `max_bytes` on whatever any provisioned pull returns,
                // so it is not consulted here.
                return Ok(OriginFetch::NotFound);
            };
            let hash_bytes = *hash.as_bytes();
            let providers = discover(deps, hash_bytes).await;
            if providers.is_empty() {
                deps.metrics.node_pull_no_providers();
                debug!(%hash, "node-origin: no providers discovered for cache-miss pull");
                return Ok(OriginFetch::NotFound);
            }
            deps.metrics.node_pull_attempt();
            let ranked = probe_and_rank(deps, providers, hash_bytes).await;
            match try_pull(deps, &ranked, hash_bytes).await {
                Some(bytes) => Ok(OriginFetch::found_one_shot(bytes)),
                None => Ok(OriginFetch::NotFound),
            }
        })
    }

    fn kind(&self) -> OriginKind {
        OriginKind::Peer
    }
}

/// Discover candidate providers for `hash`: the DHT iterative lookup first,
/// falling back to the on-chain origin directory when the lookup converges
/// empty (ADR 022 §`FIND_VALUE` Flow).
async fn discover(deps: &NodeOriginDeps, hash_bytes: [u8; 32]) -> Vec<DhtNodeId> {
    let target = DhtHash::from_bytes(hash_bytes);
    let providers = crate::dht::find_providers(
        &deps.endpoint,
        &deps.routing_table,
        &deps.staker_set,
        &deps.negative_cache,
        deps.self_id,
        target,
        deps.config.lookup,
    )
    .await;
    if providers.is_empty() {
        deps.origin_directory.lookup_origins(&target)
    } else {
        providers
    }
}

/// Probe up to `probe_fanout` providers for rate + RTT, build a [`Candidate`]
/// for each that reports holding the blob, and rank them by the combined
/// reputation-weighted selection score.
async fn probe_and_rank(
    deps: &NodeOriginDeps,
    providers: Vec<DhtNodeId>,
    hash_bytes: [u8; 32],
) -> Vec<Candidate> {
    let now_secs = crate::payment_settlement::unix_now();
    // Probe candidates CONCURRENTLY so the probe phase is bounded by a single
    // `PROBE_TIMEOUT` rather than `fanout × PROBE_TIMEOUT`: a few slow or
    // unreachable peers must not burn the whole pull budget before a healthy
    // provider is even tried. `probe_candidate`'s side effects (reputation
    // record, negative-cache insert) are all behind locks, so concurrent runs
    // are safe; ranking afterwards makes result order irrelevant.
    let probes = providers
        .into_iter()
        .take(deps.config.probe_fanout)
        .map(|peer| probe_candidate(deps, peer, hash_bytes, now_secs));
    let candidates: Vec<Candidate> = futures_util::future::join_all(probes)
        .await
        .into_iter()
        .flatten()
        .collect();
    rank_candidates(candidates)
        .into_iter()
        .map(|r| r.candidate)
        .collect()
}

/// Probe a single provider, returning a ranked-ready [`Candidate`] iff it
/// responds, validates, and reports holding the blob. Side effects: a failed
/// probe scores the provider [`Outcome::Unreachable`]; a reachable-but-absent
/// provider is recorded in the negative-probe cache.
// Straight-line probe → classify → build; the tracing macros and the three
// sequential drop-conditions inflate the cognitive-complexity metric past the
// threshold (same inflation noted in `chain_staker_set`), and splitting the
// validation further would obscure the flow rather than clarify it.
#[allow(clippy::cognitive_complexity)]
async fn probe_candidate(
    deps: &NodeOriginDeps,
    peer: DhtNodeId,
    hash_bytes: [u8; 32],
    now_secs: u64,
) -> Option<Candidate> {
    let Ok(pk) = PublicKey::from_bytes(peer.as_bytes()) else {
        // A staker-filtered routing entry should always decode; a failure
        // implies upstream state corruption — skip rather than panic.
        return None;
    };
    let (resp, rtt_ms) = match probe_once(
        &deps.endpoint,
        EndpointAddr::new(pk),
        hash_bytes,
        now_micros(),
        deps.config.enable_0rtt,
        None,
        PROBE_TIMEOUT,
    )
    .await
    {
        Ok(ok) => ok,
        Err(err) => {
            // A failed probe is a reachability signal: the upstream could not be
            // reached for this interaction (ADR 008 §Local Score).
            debug!(%err, "node-origin: probe failed; scoring provider unreachable");
            record_outcome(deps, pk, &Outcome::Unreachable);
            return None;
        }
    };
    // Shape + echoed-hash validation (ADR 005 / ADR 014 §1); a malformed or
    // off-hash response is dropped, not scored.
    if resp.validate().is_err() || resp.body.hash != hash_bytes {
        return None;
    }
    if !resp.body.has_blob {
        // Reachable but does not hold the blob — not a reputation event (no
        // delivery attempted); cache the negative so the next lookup for this
        // hash skips it within the TTL.
        deps.negative_cache
            .record_failure(peer, DhtHash::from_bytes(hash_bytes));
        return None;
    }
    Some(Candidate {
        node_id: *peer.as_bytes(),
        rate_per_mb: resp.body.rate_per_mb,
        rtt_ms: ms_to_u32(rtt_ms),
        reputation: combined_reputation(deps, pk, now_secs),
        // Region drives only the geo-diversity tie-break tier; left empty here
        // (we do not consult the peer table on this path). A follow-up can
        // populate it from the NodeAnnounce region.
        region: String::new(),
        stake: None,
    })
}

/// Walk the ranked candidates (best-first), opening a channel and pulling from
/// each until one delivers, bounded by [`MAX_PROVIDER_ATTEMPTS`]. Records a
/// reputation outcome for every candidate that reaches `stream_fetch`;
/// candidates skipped earlier for an unresolvable operator address or a local
/// channel-open failure are intentionally not scored (neither is the provider's
/// fault).
async fn try_pull(
    deps: &NodeOriginDeps,
    ranked: &[Candidate],
    hash_bytes: [u8; 32],
) -> Option<Bytes> {
    for candidate in ranked.iter().take(MAX_PROVIDER_ATTEMPTS) {
        if let Some(bytes) = pull_from_candidate(deps, candidate, hash_bytes).await {
            return Some(bytes);
        }
    }
    None
}

/// Attempt a single paid pull from one candidate: resolve its operator address,
/// open/reuse a buyer channel, `stream_fetch`, and record the reputation
/// outcome. Returns the bytes on success, `None` (try the next) otherwise.
// Sequential resolve → open → fetch → classify pipeline; the tracing macros and
// the success/failure classification inflate the cognitive-complexity + line
// metrics past threshold (same inflation noted in `chain_staker_set`). Splitting
// it would scatter a single linear flow across helpers.
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
/// Attach this node's ADR 005 client identity binding to an upstream pull's
/// `ChannelContext` (#1117). Signs over our OWN endpoint `NodeId` with the
/// channel's buyer key (`ctx.client_signer`) under the `CapacityBond` bind
/// domain, so the upstream can prove we own the named channel and, on its own
/// cache miss, chain a further reactive origin pull (`pull_authorized`). A
/// signing failure drops the candidate rather than sending an unbound request
/// the upstream would refuse to chain — try the next provider instead.
fn bind_upstream_ctx(deps: &NodeOriginDeps, ctx: ChannelContext) -> Option<ChannelContext> {
    let own_node_id = B256::from(*deps.endpoint.id().as_bytes());
    match sign_client_binding(&ctx.client_signer, own_node_id, &deps.bind_domain) {
        Ok(binding) => Some(ctx.with_client_binding(binding)),
        Err(err) => {
            warn!(
                error = %err,
                "node-origin: failed to sign client identity binding; skipping candidate"
            );
            None
        }
    }
}

async fn pull_from_candidate(
    deps: &NodeOriginDeps,
    candidate: &Candidate,
    hash_bytes: [u8; 32],
) -> Option<Bytes> {
    let Ok(pk) = PublicKey::from_bytes(&candidate.node_id) else {
        return None;
    };
    let Some(provider_addr) = deps
        .addr_resolver
        .address_of(&DhtNodeId::from_bytes(candidate.node_id))
    else {
        // No bonded address → we cannot safely open a channel to, or verify the
        // `slash_sig` of, this provider. Skip rather than guess.
        debug!("node-origin: candidate has no resolvable operator address; skipping");
        return None;
    };
    let ctx = match deps
        .buyer
        .open_or_reuse_channel(provider_addr, deps.config.deposit_hint)
        .await
    {
        Ok(ctx) => ctx,
        Err(err) => {
            // A channel-open failure is OUR payment-side problem, not the
            // provider's fault — don't tar its reputation; just try the next.
            record_channel_open_failure(deps, provider_addr, &err);
            return None;
        }
    };
    // #1117: bind the request so the upstream can chain a reactive pull.
    let ctx = bind_upstream_ctx(deps, ctx)?;
    let started = Instant::now();
    let mut progress = VoucherProgress::default();
    let result = stream_fetch_tracked(
        &deps.endpoint,
        EndpointAddr::new(pk),
        &ctx,
        &deps.slash_domain,
        provider_addr,
        hash_bytes,
        0,
        now_micros(),
        deps.config.pull_timeout,
        deps.config.max_blob_size_bytes,
        &mut progress,
    )
    .await;
    // Capture the delivery duration before persistence: `record_progress` does a
    // blocking fsync'd store write, and folding it into `elapsed` would inflate
    // the delivery-speed reputation signal for a reason unrelated to the network
    // pull.
    let elapsed = started.elapsed();
    persist_buyer_progress(deps, provider_addr, ctx.channel_id, &progress);
    // Surface this pull's cost to the prefetch ledger (#820); see the helper for
    // why this fires on both success and a paid-but-failed delivery.
    feed_acquisition_observer(
        deps,
        hash_bytes,
        &progress,
        ctx.prior_amount,
        ctx.prior_bytes_delivered,
    );
    match result {
        Ok(bytes) => {
            record_outcome(
                deps,
                pk,
                &Outcome::Delivered {
                    bytes: bytes.len() as u64,
                    elapsed,
                },
            );
            // Inbound counterpart of the serve path's `record_served` (#858).
            // `bytes` is the DECODED content buffer (`decode_verified_range` trims
            // the bao proof and the pre-`byte_offset` bytes, ADR 038), so
            // `bytes.len()` is CONTENT bytes — slightly under the WIRE bytes the
            // voucher watermark advanced in (the proof overhead the buyer paid).
            // Region accounting attributes the delivered content, which is the
            // right unit for a locality signal. Note this tracks *delivered*
            // bytes, not *spent*: a partially-paid failed pull (the `Err` arm)
            // still persists its voucher watermark above (#852) but is
            // intentionally not region-counted, so `bytes_in` diverges from
            // on-chain spend on failed pulls by design.
            deps.region_accountant
                .record_pulled(&candidate.node_id, bytes.len() as u64)
                .await;
            Some(bytes)
        }
        Err(err) => {
            classify_pull_failure(deps, pk, provider_addr, &err);
            None
        }
    }
}

/// Persist whatever the upstream acked, regardless of Ok/Err (#852): a
/// mid-stream failure or a paid-but-corrupt (hash-mismatch) delivery can still
/// have advanced the upstream's accepted-voucher watermark. Skipping this lets
/// the channel re-sign a stale voucher on its next reuse and be rejected. The
/// bytes are already paid for, so a persist failure must not fail the pull;
/// surface it loudly instead (it breaks the next reuse). Shared by the buffered
/// [`pull_from_candidate`] and the window-paced [`NodeProgressivePull`] (#856).
fn persist_buyer_progress(
    deps: &NodeOriginDeps,
    provider_addr: Address,
    channel_id: B256,
    progress: &VoucherProgress,
) {
    if let Some((nonce, bytes_delivered, amount)) = progress.acked()
        && let Err(err) =
            deps.buyer
                .record_progress(provider_addr, channel_id, nonce, bytes_delivered, amount)
    {
        deps.metrics.node_pull_progress_persist_failure();
        warn!(%provider_addr, %err, "node-origin: failed to persist buyer voucher progress");
    }
}

/// Classify a failed pull and fold the appropriate (or no) reputation outcome,
/// shared by the buffered and window-paced paths (#856). Buyer-side faults are
/// exonerated (don't tar the provider); a hash mismatch is `Corruption`;
/// everything else is `Unreachable`.
// Straight-line downcast → classify → log chain; the tracing macros inflate the
// cognitive-complexity metric past threshold (same inflation noted on the
// pre-extraction `pull_from_candidate`). Splitting the four sentinel arms would
// scatter one linear classification across helpers.
#[allow(clippy::cognitive_complexity)]
fn classify_pull_failure(
    deps: &NodeOriginDeps,
    pk: PublicKey,
    provider_addr: Address,
    err: &anyhow::Error,
) {
    // An oversized-blob claim is OUR ceiling, not the provider's fault — it may
    // legitimately serve larger blobs to nodes configured with a higher
    // `max_blob_size`. Record it for observability but don't tar reputation (#840).
    if err.downcast_ref::<BlobTooLargeClaim>().is_some() {
        deps.metrics.node_pull_too_large();
        debug!(%provider_addr, %err, "node-origin: upstream claimed an oversized blob; rejected before buffering");
        return;
    }
    // OUR own per-candidate deadline firing — a possibly mis-sized local
    // `pull_timeout`, not evidence the provider is unreachable. Don't tar its
    // reputation locally or over gossip (#857).
    if err.downcast_ref::<PullTimeout>().is_some() {
        deps.metrics.node_pull_timeout();
        debug!(%provider_addr, %err, "node-origin: pull hit our local deadline; not tarring upstream reputation");
        return;
    }
    // The upstream rejected a voucher WE presented — a stale nonce (#852),
    // deposit exhaustion, or a channel mismatch. That is our payment-side fault,
    // not the provider's, so skip the candidate without recording a reputation
    // observation (#857). Reason-agnostic: every `VoucherRejectReason` abandons.
    if err.downcast_ref::<UpstreamVoucherRejected>().is_some() {
        deps.metrics.node_pull_voucher_rejected();
        debug!(%provider_addr, %err, "node-origin: upstream rejected our voucher (our payment fault); not tarring upstream reputation");
        return;
    }
    // A bao verification failure (the typed `HashMismatch` sentinel, matched by
    // `downcast_ref` — not a brittle message string) means the peer was reachable
    // and paid but served wrong bytes → Corruption; everything else is an
    // unreachable/transport failure. The one residual buyer-side error that still
    // lands here is a failure to sign/encode our OWN voucher (`self_pay`): a
    // catastrophic local fault (a broken signer), not the routine honest-provider
    // mis-scoring #857 fixes, so it is intentionally not exonerated — a single
    // stray `Unreachable` is negligible next to a node whose payment side is dead.
    let outcome = if err.downcast_ref::<HashMismatch>().is_some() {
        Outcome::Corruption
    } else {
        Outcome::Unreachable
    };
    debug!(%provider_addr, %err, ?outcome, "node-origin: upstream pull failed");
    record_outcome(deps, pk, &outcome);
}

/// The combined local+network reputation for `pk` at `now_secs`, as the `f32`
/// the selection score consumes. An unseen peer scores neutral (local
/// `initial_score`), so a cold provider ranks neither favoured nor excluded
/// (ADR 008 §Cold-Start).
#[allow(clippy::cast_possible_truncation)] // reputation ∈ [0,1]; f32 has ample precision for a ranking weight.
fn combined_reputation(deps: &NodeOriginDeps, pk: PublicKey, now_secs: u64) -> f32 {
    let local = deps.local_rep.score(pk);
    let network = deps.network_rep.score(pk, now_secs);
    combined_score(Some(local), network, &deps.rep_cfg) as f32
}

/// Feed the prefetch acquisition ledger (#820) with THIS pull's spend/byte
/// deltas, for BOTH a successful and a paid-but-failed delivery (the watermark —
/// and thus real spend — can advance even on a mid-stream failure #852, and the
/// rolling-1h budget must account for every micro-USDC spent, not just the
/// successes, or the gate silently under-counts and can be overspent). Deltas are
/// the acked watermark minus the channel's prior cumulative (the delta for THIS
/// pull, not the running total). No-op when no observer is attached or nothing
/// was acked.
///
/// Fires for ALL pull paths — the buffered `pull_from_candidate` AND the
/// window-paced `NodeProgressivePull` finalize — so the ledger never silently
/// misses spend that a future routing change pushes onto the window path. The
/// observer itself filters to prefetch-initiated pulls; a demand-miss pull (the
/// entire window-paced serve path today) is a harmless no-op inside `on_pull`.
fn feed_acquisition_observer(
    deps: &NodeOriginDeps,
    hash_bytes: [u8; 32],
    progress: &VoucherProgress,
    prior_amount: U256,
    prior_bytes_delivered: U256,
) {
    if let Some(obs) = &deps.acquisition_observer
        && let Some((_, bytes_delivered, amount)) = progress.acked()
    {
        let spent = narrow_pull_delta(amount.saturating_sub(prior_amount), "spend", &deps.metrics);
        let acquired = narrow_pull_delta(
            bytes_delivered.saturating_sub(prior_bytes_delivered),
            "bytes",
            &deps.metrics,
        );
        obs.on_pull(hash_bytes, spent, acquired);
    }
}

/// Narrow a per-pull `U256` micro-USDC / byte delta to `u64` for the prefetch
/// ledger (#820). A real per-pull delta never approaches `u64::MAX`; an overflow
/// signals upstream voucher-accounting corruption, so log it loudly, bump
/// `node_pull_delta_overflow` so it is alertable (not just log-grep-able), and
/// fall back to `0` — the same under-count-not-over-count direction the rest of
/// the prefetch ledger uses. Clamping HIGH (`u64::MAX`) would instead poison the
/// rolling budget sum and silently pin the gate to permanent exhaustion.
fn narrow_pull_delta(value: U256, field: &str, metrics: &Metrics) -> u64 {
    u64::try_from(value).unwrap_or_else(|_| {
        metrics.node_pull_delta_overflow();
        warn!(%value, field, "node-origin: per-pull prefetch {field} delta exceeds u64; recording 0");
        0
    })
}

/// Fold a pull/probe outcome into BOTH the local EWMA score and the outbound
/// observation buffer (ADR 008 §Local Score + §Gossip Protocol). The buffer
/// feed is what makes the node *emit* reports about its upstreams.
fn record_outcome(deps: &NodeOriginDeps, pk: PublicKey, outcome: &Outcome) {
    deps.local_rep.record(pk, *outcome);
    let report = match *outcome {
        Outcome::Delivered { bytes, elapsed } => {
            deps.metrics.node_pull_success();
            ReportMetrics {
                delivery_speed: Some(bytes_per_sec(bytes, elapsed)),
                uptime_observed: Some(true),
                data_correct: Some(true),
            }
        }
        Outcome::Corruption => {
            deps.metrics.node_pull_corruption();
            ReportMetrics {
                delivery_speed: None,
                uptime_observed: Some(true),
                data_correct: Some(false),
            }
        }
        Outcome::Unreachable => {
            deps.metrics.node_pull_unreachable();
            ReportMetrics {
                delivery_speed: None,
                uptime_observed: Some(false),
                data_correct: None,
            }
        }
        // `Outcome` is `#[non_exhaustive]`: a future variant defaults to an
        // all-`None` (no-signal) report rather than mis-attributing one of the
        // three known shapes. Add an explicit arm when such a variant lands.
        _ => ReportMetrics {
            delivery_speed: None,
            uptime_observed: None,
            data_correct: None,
        },
    };
    deps.obs_buffer.observe(pk, report);
}

/// Bytes-per-second as a saturating `u32`, with a 1 ms floor on elapsed so a
/// sub-millisecond local-loopback delivery can't divide by ~zero.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)] // throughput metric; saturation + sign-safe (bytes ≥ 0, secs > 0) by construction.
fn bytes_per_sec(bytes: u64, elapsed: Duration) -> u32 {
    let secs = elapsed.as_secs_f64().max(0.001);
    let bps = bytes as f64 / secs;
    if bps >= f64::from(u32::MAX) {
        u32::MAX
    } else {
        bps as u32
    }
}

/// Round-trip-time milliseconds as a saturating `u32` for the selection score.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // RTT ≥ 0; saturated below u32::MAX.
fn ms_to_u32(rtt_ms: f64) -> u32 {
    if rtt_ms >= f64::from(u32::MAX) {
        u32::MAX
    } else if rtt_ms <= 0.0 {
        0
    } else {
        rtt_ms as u32
    }
}

/// Current Unix time in microseconds for probe/stream request correlation. A
/// pre-epoch clock saturates to `0` (the server's recency check rejects it)
/// rather than panicking.
fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// The failure-class `reason` (#966) the `open_channel` kernel attaches to
    /// the `anyhow` error chain must survive the additional `.context(...)`
    /// layers `open_and_persist` / `open_or_reuse_channel` wrap around it —
    /// `record_channel_open_failure`'s `downcast_ref` walks the whole chain, so
    /// the metric label is recovered regardless of how deep the reason sits.
    #[test]
    fn failure_reason_survives_context_wrapping() {
        for reason in [
            ChannelOpenFailureReason::InsufficientDeposit,
            ChannelOpenFailureReason::ContractRevert,
            ChannelOpenFailureReason::RpcError,
        ] {
            // Approximate the real chain: a base error, the kernel's typed
            // reason, then the caller's wrapping `.context` layers. The exact
            // ordering differs from the submit path — there the kernel attaches
            // the reason *after* its own `.context("submit openChannel")` — but
            // `downcast_ref` walks the whole chain irrespective of layer order,
            // which is exactly what this test pins down.
            let err = anyhow::anyhow!("openChannel send failed: transport down")
                .context(reason)
                .context("submit openChannel")
                .context("persist newly-opened buyer channel");
            let recovered = err.downcast_ref::<ChannelOpenFailureReason>().copied();
            assert_eq!(
                recovered,
                Some(reason),
                "reason {reason:?} must be recoverable from the wrapped chain"
            );
        }

        // An error with no attached reason (e.g. a pure store fault) downcasts
        // to `None`, so the helper logs `unclassified` and only the unlabeled
        // total moves.
        let storeless = anyhow::anyhow!("redb write failed").context("persist buyer channel");
        assert!(
            storeless
                .downcast_ref::<ChannelOpenFailureReason>()
                .is_none()
        );
    }

    /// An unprovisioned `NodeOrigin` is a clean miss for any hash, so wiring it
    /// into the engine chain before its dependencies exist (or with the feature
    /// off) never disturbs the existing miss behaviour.
    #[tokio::test]
    async fn unprovisioned_fetch_is_not_found() {
        let origin = NodeOrigin::new();
        let got = origin.fetch(Hash::new(b"anything"), 1 << 20).await.unwrap();
        assert!(matches!(got, OriginFetch::NotFound));
        assert_eq!(origin.kind(), OriginKind::Peer);
    }

    /// Delivered → speed reported, reachable + correct.
    #[test]
    fn delivered_maps_to_full_positive_metrics() {
        // 1 MiB in 100 ms ≈ 10.5 MB/s.
        let speed = bytes_per_sec(1_048_576, Duration::from_millis(100));
        assert!(speed > 9_000_000 && speed < 12_000_000, "speed = {speed}");
    }

    /// A sub-millisecond elapsed can't divide by zero; the 1 ms floor bounds it.
    #[test]
    fn bytes_per_sec_floors_tiny_elapsed() {
        let speed = bytes_per_sec(1024, Duration::from_nanos(1));
        assert_eq!(speed, bytes_per_sec(1024, Duration::from_millis(1)));
    }

    #[test]
    fn ms_to_u32_saturates_and_floors() {
        assert_eq!(ms_to_u32(-5.0), 0);
        assert_eq!(ms_to_u32(42.9), 42);
        assert_eq!(ms_to_u32(f64::from(u32::MAX) + 1.0), u32::MAX);
    }

    /// The whole #857 fix hinges on `pull_from_candidate` recovering the buyer-side
    /// sentinels via `downcast_ref` after they round-trip through `anyhow::Error`
    /// (the timeout path even double-wraps via `??`). Pin that contract at the
    /// boundary — using the SAME `downcast_ref` call production uses — so a future
    /// change to the sentinel type or a switch away from `downcast_ref` fails here,
    /// a localized failure, rather than as a confusing "honest provider got tarred"
    /// assertion three layers up. The last case proves `downcast_ref` still finds
    /// the sentinel through a `.context()` layer (anyhow walks the chain), so a
    /// future wrap in the propagation path would not silently break classification.
    #[test]
    fn buyer_side_sentinels_survive_anyhow_downcast() {
        let timeout: anyhow::Error = anyhow::Error::new(PullTimeout {
            after: Duration::from_secs(3),
        });
        assert!(timeout.downcast_ref::<PullTimeout>().is_some());
        assert!(timeout.downcast_ref::<HashMismatch>().is_none());

        let rejected: anyhow::Error = anyhow::Error::new(UpstreamVoucherRejected {
            reason: decdn_protocol::client::VoucherRejectReason::StaleNonce,
        });
        assert!(rejected.downcast_ref::<UpstreamVoucherRejected>().is_some());
        assert!(rejected.downcast_ref::<PullTimeout>().is_none());

        // Even with an added context layer, the plain `downcast_ref` the
        // orchestrator uses still recovers the sentinel (no `root_cause()` needed).
        let wrapped = timeout.context("added context in some future propagation path");
        assert!(wrapped.downcast_ref::<PullTimeout>().is_some());
    }
}
