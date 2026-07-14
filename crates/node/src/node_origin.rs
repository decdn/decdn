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
use decdn_protocol::client::{StreamError, VoucherRejectReason};
use iroh::{Endpoint, EndpointAddr, PublicKey};
use tracing::{debug, warn};

use decdn_reputation::{
    LocalReputation, NetworkReputation, NetworkReputationConfig, ObservationBuffer, Outcome,
    combined_score,
};

use decdn_incentive::ChannelOpenFailureReason;

use crate::buyer_channel::{ChannelOpenPending, ChannelOpener, OpenReported, OpenSlotReserved};
use crate::client_requester::{
    BlobTooLargeClaim, ChannelContext, ChannelLedger, Cumulative, HashMismatch, LocalPullFault,
    PullDeadlines, PullStalled, PullTimeout, UpstreamPull, UpstreamPullHeader, UpstreamRefused,
    UpstreamVoucherRejected, VoucherProgress, open_progressive_pull as open_progressive_upstream,
    sign_client_binding, stream_fetch_shared,
};
use crate::dht::negative_cache::Hash as DhtHash;
use crate::dht::routing::{NodeId as DhtNodeId, RoutingTable};
use crate::dht::{
    LookupConfig, NegativeProbeCache, NodeAddressResolver, OriginDirectory, StakerSet,
};
use crate::metrics::Metrics;
use crate::probe_client::probe_once;
use crate::selection::{Candidate, MAX_PROVIDER_ATTEMPTS, PROBE_TIMEOUT, rank_candidates};

/// Record a buyer channel open/reuse failure on `err` to the metrics in `deps`,
/// emitting a structured-log line with the failure-class `reason` (#966).
///
/// Bumps the unlabeled `node_pull_channel_open_failures` total and, when the
/// error chain carries a [`ChannelOpenFailureReason`] (attached by the
/// `open_channel` kernel for the three `openChannel`-tx failure classes), the
/// matching `decdn_channel_open_failures_{reason}_total` sibling counter.
///
/// Three outcomes are NOT failures and return before that: [`ChannelOpenPending`] (the
/// open outlived our budget and continues in the background), [`OpenSlotReserved`] (a
/// reconcile holds the slot; retry), and anything the detached open task has already
/// reported ([`OpenReported`]) — which since the #1145 review includes every one of
/// `run_open`'s legs, store faults and unreclaimable-expired channels included. So the
/// unlabeled arm below is now genuinely a *residual*: an open/reuse failure raised
/// outside the open task itself.
// The arms are a flat sentinel ladder; splitting it would scatter one decision.
#[allow(clippy::cognitive_complexity)]
fn record_channel_open_failure(deps: &NodeOriginDeps, provider_addr: Address, err: &anyhow::Error) {
    // Not a failure at all: the open ran past our per-candidate budget and is still
    // going in the background (#1143). Meter it apart from real failures — a
    // sustained rate means this node's chain lane is too slow for its
    // `node_pull_timeout_sec`, which is a very different diagnosis from a reverting
    // or under-funded open.
    if err.downcast_ref::<ChannelOpenPending>().is_some() {
        deps.metrics.node_pull_channel_open_pending();
        debug!(%provider_addr, %err, "node-origin: channel open still in flight; trying the next candidate");
        return;
    }
    // Also not a failure: a reconcile scan holds this provider's open slot while it
    // re-hydrates the row, and tells us to retry. Self-clearing, and it happens at
    // every boot — counting it as a channel-open FAILURE turned each restart into a
    // spike of `unclassified` failures an operator would chase. Same verdict, and the
    // same counter, as a pending open: try another candidate, nothing is wrong.
    if err.downcast_ref::<OpenSlotReserved>().is_some() {
        deps.metrics.node_pull_channel_open_pending();
        debug!(%provider_addr, %err, "node-origin: a reconcile holds the provider's open slot; trying the next candidate");
        return;
    }
    // The detached open task already logged and metered this one (#1143). It has to
    // be the reporter, because when an open fails, every caller may already have
    // timed out and left — so if the caller were the reporter, the failure would go
    // unobserved exactly when it is least affordable. This arm keeps the caller that
    // DID happen to still be waiting from double-counting it.
    if err.downcast_ref::<OpenReported>().is_some() {
        debug!(%provider_addr, %err, "node-origin: buyer channel open failed (reported by the open task)");
        return;
    }
    deps.metrics.node_pull_channel_open_failure();
    let reason = err.downcast_ref::<ChannelOpenFailureReason>().copied();
    if let Some(reason) = reason {
        deps.metrics.channel_open_failure_by_reason(reason);
    }
    // `warn!`, not `debug!`. Everything the open task raises is `OpenReported` and
    // returned above, so what reaches here is raised OUTSIDE the task — which makes this
    // arm node-local faults, not peer behaviour: a store read fault on the reuse fast
    // path, a poisoned `opens_in_flight` mutex (which wedges every open for the life of
    // the process), or a channel that opened on-chain and is somehow not live in the
    // store. None of those are things an operator should have to scrape debug logs to
    // see; at the default `RUST_LOG=info` a `debug!` here meant watching
    // `node_pull_channel_open_failures_total` climb with no line explaining any of it.
    warn!(
        %provider_addr,
        reason = reason.map_or("unclassified", ChannelOpenFailureReason::as_label),
        %err,
        "node-origin: buyer channel open/reuse failed (raised outside the open task — \
         suspect this node's store or lock state, not the peer)"
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
    /// Wall-clock bound on the STREAM-OPEN stage of a single upstream pull: connect,
    /// handshake, and the signed `StreamResponse`. Bounded work, so a slow one is a stall.
    ///
    /// It does NOT bound the buyer-channel open, which is a separate, earlier stage on its
    /// own budget (`selection::CHANNEL_OPEN_CALLER_BUDGET`, 5 s) — raising this to give a
    /// slow L2 more room does nothing. That the two are sequential stages is exactly why
    /// `outer_pull_deadline` budgets both, plus the stall window, for every candidate.
    pub pull_timeout: Duration,
    /// INACTIVITY bound on the STREAMING stage (#1134). Reset on every byte
    /// received, so it trips only on a silent upstream — never on a large blob or
    /// a slow link. Deliberately not a wall clock: bounding the bytes by wall clock
    /// caps the blob size this node can pull through at `pull_timeout × link
    /// speed`, which is the bug this replaced.
    pub stall_timeout: Duration,
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
            // Bounded by its OWN budget, not the per-candidate one (#1143). A wedged
            // open no longer consumes the whole outer deadline: we stop waiting and try
            // candidate #2, while the open keeps running in the background and its
            // channel is reused if it lands. It is deliberately the smaller budget —
            // this stage and the stream open below are sequential, and
            // `outer_pull_deadline` has to cover both for every candidate.
            .open_or_reuse_channel(
                provider_addr,
                deps.config.deposit_hint,
                crate::selection::CHANNEL_OPEN_CALLER_BUDGET,
            )
            .await
        {
            Ok(ctx) => ctx,
            Err(err) => {
                record_channel_open_failure(deps, provider_addr, &err);
                return None;
            }
        };
        // #1117: bind the request so the upstream can chain a reactive pull.
        let ctx = match bind_upstream_ctx(deps, ctx) {
            Ok(ctx) => ctx,
            Err(err) => {
                // A LOCAL signing fault — classify it so it is metered as ours and the
                // peer is not scored for a key WE cannot use. No channel: the bind failed
                // before we could present a voucher on one.
                classify_pull_failure(deps, pk, provider_addr, hash_bytes, None, &err);
                return None;
            }
        };
        // Bound the open on the SAME per-candidate budget the buffered path gives
        // each candidate (`stream_fetch_tracked`). Without it, a candidate that
        // accepts the connection and then goes quiet blocks here indefinitely and
        // consumes the caller's whole outer deadline — which is deliberately sized
        // at `MAX_PROVIDER_ATTEMPTS × (channel open + pull_timeout + stall) + slack` so the
        // caller's fallback loop (`open_progressive_pull`, above) can still reach
        // candidates #2..N (#859) — so the serve path would refuse a blob an honest
        // fallback holds. The typed `PullTimeout` flows into the `Err` arm's
        // `classify_pull_failure`, which exonerates the peer (our deadline is not
        // evidence it is bad, #857) and meters `node_pull_timeout`, exactly as on
        // the buffered path.
        //
        // This bounds the OPEN stage — bounded work (connect, handshake, verified
        // response), so a slow one really is a stall. The STREAMING stage that
        // follows is bounded by inactivity instead (`stall_timeout`, carried into
        // the returned `UpstreamPull`, #1134): a wall clock over the bytes would
        // cap the blob size this node can pull through.
        match tokio::time::timeout(
            deps.config.pull_timeout,
            open_progressive_upstream(
                &deps.endpoint,
                EndpointAddr::new(pk),
                &ctx,
                &deps.slash_domain,
                provider_addr,
                hash_bytes,
                0,
                now_micros(),
                deps.config.max_blob_size_bytes,
                PullDeadlines::new(deps.config.pull_timeout, deps.config.stall_timeout),
            ),
        )
        .await
        .unwrap_or_else(|_| {
            // Name the STAGE in the log: this is the progressive OPEN timing out
            // (handshake + verified response header), not a mid-delivery stall on
            // the buffered path. The `.context` layer does not affect
            // classification — `classify_pull_failure`'s `downcast_ref` walks the
            // anyhow chain (pinned by `buyer_side_sentinels_survive_anyhow_downcast`).
            Err(anyhow::Error::new(PullTimeout {
                after: deps.config.pull_timeout,
            })
            .context("progressive upstream open"))
        }) {
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
                classify_pull_failure(
                    deps,
                    pk,
                    provider_addr,
                    hash_bytes,
                    Some(ctx.channel_id),
                    &err,
                );
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
                classify_pull_failure(deps, pk, provider_addr, hash_bytes, Some(channel_id), &err);
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
            classify_pull_failure(deps, pk, provider_addr, hash_bytes, Some(channel_id), err);
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
    let target = DhtHash::from_bytes(hash_bytes);
    // Probe candidates CONCURRENTLY so the probe phase is bounded by a single
    // `PROBE_TIMEOUT` rather than `fanout × PROBE_TIMEOUT`: a few slow or
    // unreachable peers must not burn the whole pull budget before a healthy
    // provider is even tried. `probe_candidate`'s side effects (reputation
    // record, negative-cache insert) are all behind locks, so concurrent runs
    // are safe; ranking afterwards makes result order irrelevant.
    let probes = providers
        .into_iter()
        // Drop peers already known to answer "no" for THIS hash within the TTL.
        // `find_providers` applies the same filter, but only to what the DHT lookup
        // returns — it cannot cover the origin-directory fallback (which runs when the
        // lookup converges empty), nor an entry recorded AFTER discovery, which is
        // exactly what a pull-time refusal is (#1145 review). This is the one chokepoint
        // every candidate passes through regardless of how it was found.
        //
        // Filtered BEFORE `take`, so a suppressed peer does not consume a probe-fanout
        // slot that a viable provider could have used.
        .filter(|peer| !deps.negative_cache.contains_active(peer, &target))
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

/// Attach this node's ADR 005 client identity binding to an upstream pull's
/// `ChannelContext` (#1117). Signs over our OWN endpoint `NodeId` with the
/// channel's buyer key (`ctx.client_signer`) under the `CapacityBond` bind
/// domain, so the upstream can prove we own the named channel and, on its own
/// cache miss, chain a further reactive origin pull (`pull_authorized`). A
/// signing failure drops the candidate rather than sending an unbound request
/// the upstream would refuse to chain — try the next provider instead.
///
/// Returns the error rather than swallowing it, so the caller can hand it to
/// [`classify_pull_failure`]. `sign_client_binding` marks it [`LocalPullFault`], and
/// that classification only means anything if the error actually reaches the classifier:
/// this signs with `ctx.client_signer` — the SAME key that signs vouchers — and runs
/// BEFORE the stream open on both pull paths. So a node with a broken buyer key fails
/// here, on every candidate, and never reaches the voucher-signing `LocalPullFault` deeper
/// in `stream_fetch_tracked`. Swallowed here, `node_pull_local_fault` stayed at zero in
/// precisely the emergency its doc describes ("a node that cannot sign a voucher cannot
/// pay for anything"), and an operator alerting on it got a false all-clear.
fn bind_upstream_ctx(deps: &NodeOriginDeps, ctx: ChannelContext) -> anyhow::Result<ChannelContext> {
    let own_node_id = B256::from(*deps.endpoint.id().as_bytes());
    let binding = sign_client_binding(&ctx.client_signer, own_node_id, &deps.bind_domain)?;
    Ok(ctx.with_client_binding(binding))
}

/// Attempt a single paid pull from one candidate: resolve its operator address,
/// open/reuse a buyer channel, `stream_fetch`, and record the reputation
/// outcome. Returns the bytes on success, `None` (try the next) otherwise.
// Sequential resolve → open → fetch → classify pipeline; the tracing macros and
// the success/failure classification inflate the cognitive-complexity + line
// metrics past threshold (same inflation noted in `chain_staker_set`). Splitting
// it would scatter a single linear flow across helpers.
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
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
        // Same channel-open bound as the window path (#1143) — see there.
        .open_or_reuse_channel(
            provider_addr,
            deps.config.deposit_hint,
            crate::selection::CHANNEL_OPEN_CALLER_BUDGET,
        )
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
    let ctx = match bind_upstream_ctx(deps, ctx) {
        Ok(ctx) => ctx,
        Err(err) => {
            // As on the window path: our signing fault, metered as ours, peer unscored.
            // No channel: the bind failed before we could present a voucher on one.
            classify_pull_failure(deps, pk, provider_addr, hash_bytes, None, &err);
            return None;
        }
    };
    let started = Instant::now();
    // The ledger is CALLER-owned, and the watermark is settled from it by a `Drop`
    // guard rather than after the await (#1145 review). Both halves of that matter.
    //
    // A `&mut VoucherProgress` out-param can only be copied back on a RETURN, and
    // this pull's defining property since #1134 is that it need not return: it runs
    // with `hard_cap: None`, so nothing inside it ends a slow-but-progressing
    // transfer, and everything that does end one is external and DROPS the future —
    // the foreground `outer_pull_deadline`, the background warm's
    // `BACKGROUND_FILL_HARD_CAP`, and `pull_through_bg_shutdown` on restart. On every
    // one of those paths the copy-back never ran and the acked watermark died with
    // the frame, while the USDC it recorded had already left the node. The next pull
    // then re-signed a stale nonce, the upstream rejected `StaleNonce`, and the
    // channel was wedged until it expired.
    //
    // `Drop` is the one thing that runs on both paths, so the persist lives there and
    // nowhere else — one path, no second copy to forget. It reads
    // `ChannelLedger::committed` (the sync mirror) because a `Drop` cannot await.
    let ledger = Arc::new(ChannelLedger::new(Cumulative {
        nonce: ctx.prior_nonce,
        bytes: ctx.prior_bytes_delivered,
        amount: ctx.prior_amount,
    }));
    let settle = SettleOnDrop {
        deps,
        hash_bytes,
        provider_addr,
        channel_id: ctx.channel_id,
        prior_nonce: ctx.prior_nonce,
        prior_amount: ctx.prior_amount,
        prior_bytes_delivered: ctx.prior_bytes_delivered,
        ledger: Arc::clone(&ledger),
    };

    // Streaming is bounded by INACTIVITY, with no overall wall-clock cap (#1134).
    //
    // `pull_timeout` used to wrap this whole fetch, which quietly capped the blob
    // size a node could pull through at roughly `pull_timeout × link speed` — at
    // the 20 s default, any blob needing more than ~20 s of transfer was
    // unfetchable on this path, and the background warm that should have rescued it
    // was capped by the same budget. The stall bound catches the thing a deadline
    // should catch (an upstream that stops delivering) without penalising size or
    // link speed.
    //
    // The FOREGROUND serve path is still bounded — the delivery handler wraps the
    // whole `discover → probe → rank → pull` in `outer_pull_deadline`, so a client
    // never waits longer than that; on expiry it gets a clean miss and the transfer
    // continues in a detached background warm. So "no hard cap here" does not mean
    // "a client can wait forever".
    let result = stream_fetch_shared(
        &deps.endpoint,
        EndpointAddr::new(pk),
        &ctx,
        &ledger,
        &deps.slash_domain,
        provider_addr,
        hash_bytes,
        0,
        now_micros(),
        PullDeadlines::new(deps.config.pull_timeout, deps.config.stall_timeout),
        deps.config.max_blob_size_bytes,
    )
    .await;
    // Capture the delivery duration BEFORE the guard settles: `record_progress` does
    // a blocking fsync'd store write, and folding it into `elapsed` would inflate the
    // delivery-speed reputation signal for a reason unrelated to the network pull.
    let elapsed = started.elapsed();
    drop(settle);
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
            classify_pull_failure(
                deps,
                pk,
                provider_addr,
                hash_bytes,
                Some(ctx.channel_id),
                &err,
            );
            None
        }
    }
}

/// Settles what the buffered pull paid — on EVERY way out of it, including a drop.
///
/// This is a `Drop` guard rather than a pair of calls after the await because the
/// pull can end without returning, and on that path the money is already gone
/// (#1145 review). See the comment at its construction in [`pull_from_candidate`]
/// for which cancellations are reachable and why each one is by design.
///
/// Both things it does are settlement of a completed payment, so both belong here:
///
/// - the voucher watermark, so the next reuse of this channel signs the nonce the
///   upstream actually committed to (#852);
/// - the prefetch ledger's view of the pull's cost (#820), which is just as real on
///   a cancelled pull as on a returned one — the bytes were bought either way.
///
/// It deliberately does NOT record a reputation outcome. Reputation is a judgement
/// about the peer and needs the pull's result to make it; a drop has no result, and
/// a cancelled transfer is our decision, not the provider's misconduct.
struct SettleOnDrop<'a> {
    deps: &'a NodeOriginDeps,
    hash_bytes: [u8; 32],
    provider_addr: Address,
    channel_id: B256,
    prior_nonce: U256,
    prior_amount: U256,
    prior_bytes_delivered: U256,
    ledger: Arc<ChannelLedger>,
}

impl Drop for SettleOnDrop<'_> {
    fn drop(&mut self) {
        // `committed`, not `snapshot`: a `Drop` cannot await. The mirror is written
        // under the issuance lock at the instant of commit, so it holds exactly the
        // vouchers the upstream acked and no more.
        let progress = VoucherProgress::from_cumulative(self.ledger.committed(), self.prior_nonce);
        persist_buyer_progress(self.deps, self.provider_addr, self.channel_id, &progress);
        feed_acquisition_observer(
            self.deps,
            self.hash_bytes,
            &progress,
            self.prior_amount,
            self.prior_bytes_delivered,
        );
    }
}

/// Persist whatever the upstream acked, regardless of Ok/Err (#852): a
/// mid-stream failure or a paid-but-corrupt (hash-mismatch) delivery can still
/// have advanced the upstream's accepted-voucher watermark. Skipping this lets
/// the channel re-sign a stale voucher on its next reuse and be rejected. The
/// bytes are already paid for, so a persist failure must not fail the pull;
/// surface it loudly instead (it breaks the next reuse). Shared by the buffered
/// [`pull_from_candidate`] (via [`SettleOnDrop`]) and the window-paced
/// [`NodeProgressivePull`] (#856).
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

/// What a refusal tells us about the peer, and therefore what we should do with it
/// (#1144, refined in the #1145 review).
///
/// Deliberately an exhaustive match rather than a `matches!`: a new `StreamError` variant
/// must not silently inherit any of these — not "honest" (a future failure code would go
/// unscored), not "fault" (honest peers get tarred), and not "durable" (a transient
/// condition would suppress a healthy peer for minutes). It has to be a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefusalVerdict {
    /// The peer reports its OWN degradation. Score its reputation.
    NodeFault,
    /// A true, lasting fact about this (peer, hash) pair. Suppress the pair for the full
    /// [`NegativeProbeCache`] TTL — asking again soon would get the same answer.
    DurableMiss,
    /// Transient, or not attributable to the peer at all. Suppress the pair only briefly
    /// ([`REFUSAL_SUPPRESSION_TTL`]) — long enough that a peer serving nothing stops
    /// burning a candidate slot on every miss in a retry burst, short enough that we do
    /// not blackhole a healthy peer over a condition that has already passed.
    Transient,
    /// OUR fault. Score nothing, suppress nothing.
    OurFault,
}

/// How long a (peer, hash) pair is suppressed after a refusal we cannot attribute to the
/// peer, or that we expect to pass on its own ([`RefusalVerdict::Transient`]).
///
/// Much shorter than the negative cache's own TTL (5 min), and the asymmetry is the
/// point. A probe's `has_blob: false` is an authoritative statement about content the
/// peer just checked. A `NotFound` *refusal* is not: `ServeRejectReason::wire_error`
/// deliberately collapses SEVEN reject reasons onto the wire `NotFound` so that a probing
/// client cannot map out other clients' channel balances — and three of the seven are
/// ours or transient (`InsufficientDeposit`, `UnknownChannel` while the upstream's chain
/// watcher catches up, `CooperativeCloseSigned`). We cannot tell them apart, and we must
/// not: the collapse is a privacy property, not an oversight.
///
/// So the refusal is suppressed on the assumption it may be *us*. At the full TTL, a
/// deposit that ran dry for one pull — or the pre-observation window right after we open
/// a channel — blackholed a perfectly healthy upstream for five minutes.
const REFUSAL_SUPPRESSION_TTL: Duration = Duration::from_secs(30);

const fn classify_refusal(error: &StreamError) -> RefusalVerdict {
    match error {
        // The one code by which a node reports its OWN degradation: "unexpected
        // failure; do not retry THIS node" (#1129).
        StreamError::InternalError => RefusalVerdict::NodeFault,
        // Honest and durable: `EvictedSinceProbe` is a race the peer is being truthful
        // about, and `BlobTooLarge` is deterministic for this blob. Asking this peer for
        // this hash again inside the TTL gets the same answer, so don't spend a candidate
        // slot finding out.
        StreamError::EvictedSinceProbe | StreamError::BlobTooLarge => RefusalVerdict::DurableMiss,
        // Honest but NOT durable, and — for `NotFound` — not even attributable: see
        // `REFUSAL_SUPPRESSION_TTL`. `Overloaded` is backpressure, which the code's own
        // policy says to respect rather than punish; suppressing the peer for five
        // minutes over a load spike lasting seconds is punishing it.
        StreamError::NotFound | StreamError::Overloaded => RefusalVerdict::Transient,
        // `VoucherRejected` is OUR payment-side fault, so: score nothing, suppress nothing.
        // The peer did nothing wrong and it still holds the blob.
        //
        // This arm is REACHABLE, and the comment here used to say it was not (#1145 review).
        // The old reasoning covered only the OPEN stage, where `StreamResponse::validate`
        // does reject `VoucherRejected` in the `error` field as a mid-stream-only code. But
        // this same PR added three MID-STREAM arms that wrap any `ClientMessage::StreamError`
        // into `UpstreamRefused` — and `VoucherRejected` is precisely the code designated for
        // mid-stream. Only `self_pay`'s ack-wait special-cases it, so one arriving outside a
        // voucher round trip lands here as a live refusal.
        //
        // The verdict is right either way, which is what made the false "cannot happen" worth
        // correcting rather than shrugging at: a reader who believed it would delete this arm
        // as dead code, and a mid-stream `VoucherRejected` would then fall to a verdict that
        // blames the peer for our own payment fault. Note it does land on
        // `node_pull_refused_total` rather than `node_pull_voucher_rejected_total`, which
        // slightly understates the latter.
        StreamError::VoucherRejected { .. } => RefusalVerdict::OurFault,
    }
}

/// What a failed pull was actually caused by — the whole classification decision, as a
/// value.
///
/// Separated from the *acting* on it ([`classify_pull_failure`]) so the decision can be
/// tested without a live `NodeOriginDeps`, which needs an iroh endpoint. That matters more
/// than it sounds. The ladder below is an ORDERED chain of `downcast_ref`s whose order is
/// load-bearing and invisible to the compiler, ending in a catch-all that scores the peer
/// `Unreachable`. Every mis-attribution this module has shipped was an error falling one
/// arm further than it should and landing there: an honest `NotFound` refusal (#1144), a
/// mid-stream `StreamError`, a broken local signer. Making the decision a pure function
/// means "which arm does this error land in?" is a question a test can just ask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PullVerdict {
    /// The blob is over OUR configured ceiling (#840) — it may be fine for other nodes.
    OversizeClaim,
    /// OUR deadline fired: a possibly mis-sized local budget, not evidence about the peer.
    OurDeadline,
    /// The peer went SILENT mid-stream (#1134). Unlike [`Self::OurDeadline`] this IS about
    /// the peer: a clock that resets on every byte can only fire on one that stopped.
    Stalled,
    /// The peer rejected a voucher we presented, and the channel is FINISHED: the deposit
    /// is spent, it expired, the upstream has signed its cooperative close, or our
    /// watermark has desynced from the nonce it committed (#1145 review).
    ///
    /// Split from [`Self::OurVoucherRetryable`] because the two want opposite actions and
    /// collapsing them is what made a drained deposit invisible. `UpstreamVoucherRejected`
    /// carries a `VoucherRejectReason` whose eleven variants prescribe *different*
    /// remedies — resend, top up, rotate, stop — and the classifier used to discard it with
    /// a bare `.is_some()`, so every one of them became "skip this candidate, say nothing".
    /// The channel was then handed straight back on the next miss (`try_reuse_live` gates
    /// only on expiry), so the provider stayed top-ranked and could not serve a byte for
    /// the ~90 days until the channel expired — while the only signal was a `debug!` line
    /// nobody sees at the default log level.
    OurDeadChannel(VoucherRejectReason),
    /// The peer rejected a voucher we presented, but the channel is FINE: a transient
    /// node-side persist fault upstream (`RetryLater`). ADR 003 has the upstream's state
    /// not advance in this case, so the SAME voucher can be resent on a fresh stream —
    /// nothing to rotate, nothing to top up (#1145 review).
    OurVoucherRetryable(VoucherRejectReason),
    /// The peer refused delivery, carrying the wire code's own verdict (#1144).
    Refused(RefusalVerdict),
    /// A fault in THIS node — a broken signer, a bad encode, a bad range. It says nothing
    /// about the peer, and a node in this state would otherwise tar every honest provider
    /// it meets.
    OurLocalFault,
    /// Reachable, paid, and served the wrong bytes.
    Corruption,
    /// Everything else: a failed dial, a dropped connection, a bad slash signature, an
    /// unexpected frame. The residual — and the arm that actually scores a dead node.
    Unreachable,
}

/// Retire a buyer channel an upstream has told us can never pay again, so the next pull to
/// that provider opens a fresh one (#1145 review).
///
/// Retiring only the LOCAL row is the whole action, and it is enough: the on-chain deposit
/// stays escrowed and is recovered by the ordinary settlement sweep / `reclaimExpired`, the
/// same as for any channel we stop using. What we must not do is keep the row, because
/// `try_reuse_live` gates on expiry alone and would hand the dead channel straight back.
///
/// `channel` is `None` only for failures that happen before a channel exists, which cannot
/// produce a voucher rejection — so reaching that arm means the ladder has changed and a
/// dead channel is about to be silently kept. Say so rather than skip quietly.
// As `classify_pull_failure`: the tracing macros inflate the cognitive-complexity metric.
#[allow(clippy::cognitive_complexity)]
fn retire_dead_channel(
    deps: &NodeOriginDeps,
    provider_addr: Address,
    reason: VoucherRejectReason,
    channel: Option<B256>,
) {
    let Some(channel_id) = channel else {
        warn!(
            %provider_addr, ?reason,
            "node-origin: upstream rejected our voucher on a pull with no channel — this \
             should be unreachable; the channel cannot be retired and will be reused"
        );
        return;
    };
    match deps.buyer.retire_channel(provider_addr, channel_id) {
        // Not retired because the row is already a DIFFERENT channel: a concurrent open
        // replaced it while this pull was in flight, so the dead one is gone anyway and the
        // replacement is not ours to throw away. Nothing to do, and nothing wrong.
        Ok(false) => debug!(
            %provider_addr, %channel_id, ?reason,
            "node-origin: dead channel was already replaced by a newer open"
        ),
        Ok(true) => {
            deps.metrics.node_pull_channel_retired();
            warn!(
                %provider_addr, %channel_id, ?reason,
                "node-origin: upstream rejected our voucher on terms this channel cannot \
                 recover from; retired it — the next pull to this provider opens a fresh one"
            );
        }
        // The store write failed, so the dead row is still there and the next pull WILL
        // reuse it and be rejected again. Nothing here can fix that, but an operator can,
        // and this is the only place that knows.
        Err(err) => {
            deps.metrics.node_pull_progress_persist_failure();
            warn!(
                %provider_addr, %channel_id, ?reason, %err,
                "node-origin: could not retire a dead buyer channel; it will be reused and \
                 rejected again until it expires"
            );
        }
    }
}

/// What a voucher rejection tells us, and therefore what to do about it (#1145 review).
///
/// Exhaustive on purpose, like [`classify_refusal`]: a new `VoucherRejectReason` must break
/// this build rather than silently inherit a verdict. The reasons are not variations on one
/// theme — three genuinely different things arrive on this wire code:
///
/// - **Our signer is broken.** `BadSignature`/`WrongSigner` mean the upstream could not
///   verify a signature WE produced. That is not a payment problem, it is a defect in this
///   node, and it will hit every candidate we try — so it belongs in the loud
///   [`PullVerdict::OurLocalFault`] arm, which exists for exactly this. Routing it to the
///   payment bucket also *hid* it: the ladder checks `UpstreamVoucherRejected` before
///   `LocalPullFault`, so a broken buyer key produced a `debug!` about payments instead of
///   the `warn!` about a node that cannot pay anyone.
/// - **The channel is finished.** Spent, expired, cooperatively closed, or desynced from
///   the upstream's committed nonce. No future voucher on it can be accepted.
/// - **Try again.** `RetryLater` alone: the voucher was valid and the upstream's state did
///   not advance (ADR 003), so the same voucher can go out on a fresh stream.
const fn voucher_verdict(reason: VoucherRejectReason) -> PullVerdict {
    match reason {
        VoucherRejectReason::BadSignature | VoucherRejectReason::WrongSigner => {
            PullVerdict::OurLocalFault
        }
        VoucherRejectReason::RetryLater => PullVerdict::OurVoucherRetryable(reason),
        // Everything else is terminal FOR THIS CHANNEL, though for two different underlying
        // reasons — the money ran out (`InsufficientDeposit`, `Expired`,
        // `CooperativeCloseSigned`) or our accounting drifted from the upstream's
        // (`StaleNonce`, `AmountRegression`, `BytesRegression`, `WrongChannel`,
        // `WrongToken`). Both are unrecoverable on the existing row and both are repaired
        // the same way: drop it and open a fresh channel, which re-derives the nonce, the
        // token, and the deposit from scratch.
        VoucherRejectReason::WrongChannel
        | VoucherRejectReason::WrongToken
        | VoucherRejectReason::StaleNonce
        | VoucherRejectReason::AmountRegression
        | VoucherRejectReason::BytesRegression
        | VoucherRejectReason::InsufficientDeposit
        | VoucherRejectReason::Expired
        | VoucherRejectReason::CooperativeCloseSigned => PullVerdict::OurDeadChannel(reason),
    }
}

/// The ordered sentinel ladder. Pure: no metrics, no reputation, no I/O.
fn pull_verdict(err: &anyhow::Error) -> PullVerdict {
    if err.downcast_ref::<BlobTooLargeClaim>().is_some() {
        return PullVerdict::OversizeClaim;
    }
    if err.downcast_ref::<PullTimeout>().is_some() {
        return PullVerdict::OurDeadline;
    }
    if err.downcast_ref::<PullStalled>().is_some() {
        return PullVerdict::Stalled;
    }
    if let Some(rejected) = err.downcast_ref::<UpstreamVoucherRejected>() {
        return voucher_verdict(rejected.reason);
    }
    // Ahead of `UpstreamRefused` and the catch-all, deliberately: a local signing or encode
    // fault surfaces while we are talking to a peer, and every arm below this one blames
    // the peer to some degree. A node with a broken buyer key hits this on EVERY candidate,
    // so getting the order wrong here does not mis-score one provider — it gossips the
    // whole candidate list as unreachable on the strength of our own defect.
    if err.downcast_ref::<LocalPullFault>().is_some() {
        return PullVerdict::OurLocalFault;
    }
    if let Some(refused) = err.downcast_ref::<UpstreamRefused>() {
        return PullVerdict::Refused(classify_refusal(&refused.error));
    }
    if err.downcast_ref::<HashMismatch>().is_some() {
        return PullVerdict::Corruption;
    }
    PullVerdict::Unreachable
}

/// Classify a failed pull and fold the appropriate (or no) reputation outcome,
/// shared by the buffered and window-paced paths (#856). Buyer-side faults are
/// exonerated (don't tar the provider); an honest refusal is exonerated too
/// (#1144 — a peer that answers is reachable, whatever it answers), except
/// `InternalError`, by which a peer reports its own degradation; a hash mismatch
/// is `Corruption`; everything else is `Unreachable`.
///
/// [`pull_verdict`] makes the decision; this acts on it.
///
/// `channel` is the buyer channel the pull was paying on, or `None` for the failures that
/// happen before there is one to pay on (a channel open that never completed, a local
/// binding-signature fault). It is `Option` rather than plumbed unconditionally because the
/// distinction is real: only a pull that presented a voucher can have one rejected, so only
/// those sites can reach [`PullVerdict::OurDeadChannel`] and need a channel to retire.
// The tracing macros inflate the cognitive-complexity metric past threshold.
#[allow(clippy::cognitive_complexity)]
fn classify_pull_failure(
    deps: &NodeOriginDeps,
    pk: PublicKey,
    provider_addr: Address,
    hash_bytes: [u8; 32],
    channel: Option<B256>,
    err: &anyhow::Error,
) {
    let suppress = |ttl: Option<Duration>| {
        let node = DhtNodeId::from_bytes(*pk.as_bytes());
        let hash = DhtHash::from_bytes(hash_bytes);
        match ttl {
            Some(ttl) => deps.negative_cache.record_failure_with_ttl(node, hash, ttl),
            None => deps.negative_cache.record_failure(node, hash),
        }
    };

    match pull_verdict(err) {
        // OUR ceiling, not the provider's fault — it may legitimately serve larger blobs to
        // nodes configured with a higher `max_blob_size`. Metered, not scored (#840).
        PullVerdict::OversizeClaim => {
            deps.metrics.node_pull_too_large();
            debug!(%provider_addr, %err, "node-origin: upstream claimed an oversized blob; rejected before buffering");
        }
        // A possibly mis-sized local budget, not evidence the provider is unreachable
        // (#857).
        PullVerdict::OurDeadline => {
            deps.metrics.node_pull_timeout();
            debug!(%provider_addr, %err, "node-origin: pull hit our local deadline; not tarring upstream reputation");
        }
        // Unlike `OurDeadline`, this DOES score the peer, and that split is the whole reason
        // the two sentinels exist. A whole-transfer deadline could not tell a dead peer from
        // a big blob on a slow link, so it fired on healthy transfers and had to be
        // exonerating. A stall deadline resets on every byte, so it can only fire on a
        // provider that stopped delivering — which is what `Unreachable` means (#1134).
        PullVerdict::Stalled => {
            deps.metrics.node_pull_stalled();
            debug!(%provider_addr, %err, "node-origin: upstream stalled mid-stream; scoring unreachable");
            record_outcome(deps, pk, &Outcome::Unreachable);
        }
        // Our payment-side fault either way — the provider is not scored (#857). What
        // separates the two arms is whether the CHANNEL survives it (#1145 review).
        //
        // The channel is finished: spent, expired, closed, or desynced from the nonce the
        // upstream committed. Retire the row so the next pull opens a fresh one. Without
        // this, `try_reuse_live` — which gates only on expiry — hands the dead channel back
        // on the very next miss, and keeps handing it back: the provider stays top-ranked,
        // burns a `MAX_PROVIDER_ATTEMPTS` slot every time, and cannot serve a byte until the
        // channel expires ~90 days later. `warn!`, not `debug!`, for the same reason the
        // local-fault arm is: at the default `RUST_LOG=info` a `debug!` is invisible, and
        // this is a node that cannot pay a provider it will keep on choosing.
        PullVerdict::OurDeadChannel(reason) => {
            deps.metrics.node_pull_voucher_rejected();
            retire_dead_channel(deps, provider_addr, reason, channel);
        }
        // The channel is fine: the upstream hit a transient persist fault and its state did
        // not advance (ADR 003), so the same voucher can be resent on a fresh stream. Skip
        // the candidate this once and leave the channel alone — rotating here would throw
        // away a healthy channel over a hiccup.
        PullVerdict::OurVoucherRetryable(reason) => {
            deps.metrics.node_pull_voucher_rejected();
            debug!(
                %provider_addr, ?reason, %err,
                "node-origin: upstream asked us to retry the voucher; channel left intact"
            );
        }
        // A refusal proves the peer is reachable and answering, so it is not `Unreachable`
        // on its own — which is what every refusal used to score, tarring a node exactly as
        // hard for honestly saying it lacks a blob as for being dead (#1144).
        //
        // Exonerating it is not the same as ignoring it, though. Every candidate that got
        // this far answered `has_blob = true` at probe, so a refusal is a peer contradicting
        // itself, and recording nothing let a peer that advertises everything and serves
        // nothing keep winning the ranker and burn a `MAX_PROVIDER_ATTEMPTS` slot on every
        // miss, forever. The negative cache is the right instrument — scoped to (peer, hash),
        // TTL'd, reputation-neutral — and `RefusalVerdict` decides what the suppression is
        // worth: five minutes for a peer that truthfully says the blob is gone, far less for
        // a `NotFound` that may well have been our own empty deposit.
        PullVerdict::Refused(verdict) => {
            deps.metrics.node_pull_refused();
            match verdict {
                RefusalVerdict::NodeFault => {
                    debug!(%provider_addr, %err, "node-origin: upstream reports itself degraded; scoring unreachable");
                    record_outcome(deps, pk, &Outcome::Unreachable);
                }
                RefusalVerdict::DurableMiss => {
                    suppress(None);
                    debug!(%provider_addr, %err, "node-origin: upstream does not have this blob; negative-caching this (peer, hash) for the full TTL without tarring reputation");
                }
                RefusalVerdict::Transient => {
                    suppress(Some(REFUSAL_SUPPRESSION_TTL));
                    debug!(%provider_addr, %err, ttl = ?REFUSAL_SUPPRESSION_TTL, "node-origin: upstream refused for a reason we cannot attribute to it; briefly suppressing this (peer, hash) without tarring reputation");
                }
                RefusalVerdict::OurFault => {
                    debug!(%provider_addr, %err, "node-origin: upstream refused on OUR payment fault; exonerating and leaving it selectable");
                }
            }
        }
        // A broken signer, a bad encode, a bad range — none of it says anything about the
        // peer, and a node in this state walks the whole candidate list tarring every honest
        // provider it meets with an `Unreachable` (an EWMA hit AND a gossiped observation)
        // on the strength of its own defect. `warn!`, not `debug!`: a node that cannot sign
        // cannot pay, so this is operator-actionable — and it is about US.
        PullVerdict::OurLocalFault => {
            deps.metrics.node_pull_local_fault();
            warn!(
                %provider_addr, %err,
                "node-origin: LOCAL buyer-side fault during a pull (signer/encode/range) — this node \
                 cannot pay; exonerating the upstream"
            );
        }
        PullVerdict::Corruption => {
            debug!(%provider_addr, %err, "node-origin: upstream served corrupt bytes; scoring corruption");
            record_outcome(deps, pk, &Outcome::Corruption);
        }
        PullVerdict::Unreachable => {
            debug!(%provider_addr, %err, "node-origin: upstream pull failed; scoring unreachable");
            record_outcome(deps, pk, &Outcome::Unreachable);
        }
    }
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
    use decdn_protocol::VoucherRejectReason;

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

        // `LocalPullFault` (#1145 review) is the one sentinel attached as a CONTEXT
        // layer rather than as the error itself — `anyhow!("voucher signing failed")
        // .context(LocalPullFault)` — and it is then wrapped again on the way up. If
        // this downcast ever stopped working, the exoneration arm would silently stop
        // firing and a node with a broken signer would go back to gossiping
        // `Unreachable` about every honest provider it tried. That failure is invisible
        // at the call site, so pin it here.
        let local = anyhow::anyhow!("voucher signing failed: bad key")
            .context(LocalPullFault)
            .context("self_pay");
        assert!(
            local.downcast_ref::<LocalPullFault>().is_some(),
            "a local fault must stay recoverable through the context layers above it"
        );
        assert!(
            local.downcast_ref::<PullStalled>().is_none(),
            "and must not be confused with a peer-attributable sentinel"
        );

        // The refusal sentinel (#1144) carries the wire code through the same
        // channel, so `classify_pull_failure` can split an honest `NotFound` from
        // a self-reported `InternalError` instead of folding both to Unreachable.
        let refused: anyhow::Error = anyhow::Error::new(UpstreamRefused {
            error: StreamError::NotFound,
        });
        let recovered = refused
            .downcast_ref::<UpstreamRefused>()
            .map(|r| r.error.clone());
        assert_eq!(recovered, Some(StreamError::NotFound));
        assert!(refused.downcast_ref::<UpstreamVoucherRejected>().is_none());
    }

    /// The #1144 split, asserted on the real predicate `classify_pull_failure`
    /// consults: a refusal is proof the peer ANSWERED, so only the one code by
    /// which a peer reports its own degradation may score it. The `NotFound` case
    /// is the heart of the issue — a healthy-but-empty node used to take an
    /// `Unreachable` hit (local EWMA + a gossiped observation) for honestly saying
    /// so.
    #[test]
    fn only_internal_error_refusals_are_scored() {
        // The exonerated codes: every one is an honest answer from a reachable
        // node. `NotFound` is NODE-scoped, not blob-scoped (seven
        // `ServeRejectReason`s collapse onto it), so it cannot even be read as
        // "this peer lacks this blob" with confidence — only as "this peer won't
        // serve it", which is no evidence of a fault.
        for error in [
            StreamError::NotFound,
            StreamError::EvictedSinceProbe,
            StreamError::Overloaded,
            StreamError::BlobTooLarge,
            StreamError::VoucherRejected {
                reason: VoucherRejectReason::RetryLater,
            },
        ] {
            assert_ne!(
                classify_refusal(&error),
                RefusalVerdict::NodeFault,
                "{error:?} is an honest refusal and must not tar the provider"
            );
        }
        // The one code that IS evidence of a degraded peer (#1129): "unexpected
        // failure; do not retry THIS node".
        assert_eq!(
            classify_refusal(&StreamError::InternalError),
            RefusalVerdict::NodeFault
        );
    }

    /// The #1145-review refinement: exonerating a refusal is not the same as believing
    /// it. How long we suppress a (peer, hash) must match how much the refusal actually
    /// proves — and for the codes below it proves rather little.
    #[test]
    fn only_a_durable_refusal_earns_the_full_suppression_ttl() {
        // A peer that truthfully says the blob is gone, or is over its ceiling, will say
        // the same thing in a minute. Worth the full TTL.
        for error in [StreamError::EvictedSinceProbe, StreamError::BlobTooLarge] {
            assert_eq!(
                classify_refusal(&error),
                RefusalVerdict::DurableMiss,
                "{error:?} is a lasting fact about this (peer, hash)"
            );
        }
        // `NotFound` is the one that matters. `wire_error` collapses `InsufficientDeposit`
        // and `UnknownChannel` — an empty deposit of OURS, and the window where the
        // upstream's chain watcher has not yet seen the channel WE just opened — onto it,
        // deliberately, so channel balances cannot be probed. At the full TTL either one
        // blackholed a healthy peer for five minutes over a condition that had already
        // passed. `Overloaded` is a load spike, which the policy says to respect.
        for error in [StreamError::NotFound, StreamError::Overloaded] {
            assert_eq!(
                classify_refusal(&error),
                RefusalVerdict::Transient,
                "{error:?} is not durable evidence about this (peer, hash)"
            );
        }
        // And the brief suppression has to actually be brief — a `REFUSAL_SUPPRESSION_TTL`
        // raised to the cache's own TTL would silently restore the bug.
        assert!(
            REFUSAL_SUPPRESSION_TTL < Duration::from_mins(5),
            "the transient TTL must stay well under the negative cache's own"
        );
    }

    /// A refusal that is OUR fault must leave the peer entirely untouched — not scored,
    /// and not suppressed either. It still holds the blob; the problem is our voucher.
    #[test]
    fn our_own_payment_fault_neither_scores_nor_suppresses_the_peer() {
        assert_eq!(
            classify_refusal(&StreamError::VoucherRejected {
                reason: VoucherRejectReason::BadSignature,
            }),
            RefusalVerdict::OurFault
        );
    }

    /// A fault in THIS node must never be scored against the peer we happened to be
    /// talking to when it surfaced.
    ///
    /// The stakes are why this is pinned rather than assumed: the buyer key that signs the
    /// ADR 005 client binding is the same key that signs vouchers, and the binding is signed
    /// BEFORE the stream opens, on every candidate. So a node whose signer is broken does not
    /// mis-score one provider — it walks the entire candidate list handing out `Unreachable`
    /// (a local EWMA hit AND a gossiped observation) to every honest peer it meets, on the
    /// strength of its own defect. The catch-all is only ever one misplaced arm away.
    ///
    /// **What this test does and does not prove.** It pins the LADDER: that a `LocalPullFault`
    /// buried under the context layers the real call stack adds still beats every arm below
    /// it. It does NOT prove any production site attaches the marker — a pure function over
    /// an `anyhow::Error` cannot, and the version of this test that pretended otherwise was
    /// the reason the whole review round exists. That one hand-built the error WITH
    /// `.context(LocalPullFault)` and then asserted the ladder found `LocalPullFault`: true
    /// by construction, unfailable, and green even with every marker stripped from the crate.
    ///
    /// The wiring is guarded where the wiring lives:
    /// - `the_range_helpers_mark_their_own_faults_as_local` (in `decdn-client-pull`) drives
    ///   the REAL `aligned_wire_len` / `decode_verified_range` into their REAL errors and
    ///   asserts the marker is on them, never attaching it itself.
    /// - `node_origin_an_unverifiable_voucher_is_a_local_fault_not_a_payment_one` drives a
    ///   real pull whose signature the upstream cannot verify — the production shape of "our
    ///   buyer key is broken" — and asserts `node_pull_local_fault_total` moves while the
    ///   peer is left unscored.
    #[test]
    fn a_local_fault_outranks_every_arm_that_blames_the_peer() {
        // Marker under the context layers the real call stack adds on the way out — the
        // shape production produces, though (necessarily) assembled here.
        let err = anyhow::anyhow!("voucher signing failed: signer unavailable")
            .context(LocalPullFault)
            .context("bind the upstream request")
            .context("pull from candidate");

        assert_eq!(
            pull_verdict(&err),
            PullVerdict::OurLocalFault,
            "a local fault must outrank the catch-all — reaching it gossips every honest \
             provider as unreachable"
        );

        // And it must outrank the arm that sits directly below it. `UpstreamRefused` is the
        // one that would otherwise catch a local fault raised while a refusal was in flight,
        // and it exonerates the peer for the WRONG reason — quietly, and without the
        // `node_pull_local_fault_total` an operator needs to see that this node is broken.
        let refused_too = anyhow::anyhow!("encode failed")
            .context(LocalPullFault)
            .context(UpstreamRefused {
                error: StreamError::NotFound,
            });
        assert_eq!(
            pull_verdict(&refused_too),
            PullVerdict::OurLocalFault,
            "a local fault must win over a refusal on the same chain: the refusal is a \
             symptom, the broken node is the cause"
        );
    }

    /// A refusal that arrives MID-STREAM carries the same wire code, and therefore the same
    /// meaning, as one that arrives at the open. It used to be stringified
    /// (`bail!(\"stream failed: {e:?}\")`), which fell through every downcast to the
    /// catch-all and scored the peer `Unreachable` — the exact mis-attribution #1144 fixed
    /// at the open stage, reappearing one stage later.
    #[test]
    fn a_mid_stream_refusal_is_judged_by_its_wire_code_not_the_catch_all() {
        for (error, want) in [
            (StreamError::NotFound, RefusalVerdict::Transient),
            (StreamError::Overloaded, RefusalVerdict::Transient),
            (StreamError::EvictedSinceProbe, RefusalVerdict::DurableMiss),
            (StreamError::InternalError, RefusalVerdict::NodeFault),
        ] {
            // Exactly what the receive loops now raise — wrapped, because a real one comes
            // up through the pull path's `.context` layers and `downcast_ref` must still
            // find it.
            let err = anyhow::Error::new(UpstreamRefused {
                error: error.clone(),
            })
            .context("receive and pay")
            .context("pull from candidate");
            assert_eq!(
                pull_verdict(&err),
                PullVerdict::Refused(want),
                "a mid-stream {error:?} must be judged as a refusal, not fall to the catch-all"
            );
        }
    }

    /// The residual arm has to stay reachable — it is how a genuinely dead node gets
    /// scored, and an over-eager sentinel above it would silently stop scoring anyone.
    #[test]
    fn an_unrecognised_failure_still_scores_the_peer() {
        let err = anyhow::anyhow!("connection refused").context("dial provider");
        assert_eq!(pull_verdict(&err), PullVerdict::Unreachable);
    }

    /// The ladder's ORDER is load-bearing and invisible to the compiler. A stall is the one
    /// timeout that scores the peer; our own deadline is the one that must not.
    #[test]
    fn our_deadline_and_their_silence_get_opposite_verdicts() {
        let ours = anyhow::Error::new(PullTimeout {
            after: Duration::from_secs(20),
        })
        .context("pull from candidate");
        let theirs = anyhow::Error::new(PullStalled {
            after: Duration::from_secs(20),
        })
        .context("pull from candidate");
        assert_eq!(pull_verdict(&ours), PullVerdict::OurDeadline);
        assert_eq!(pull_verdict(&theirs), PullVerdict::Stalled);
    }
}
