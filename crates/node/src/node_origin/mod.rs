//! `NodeOrigin` — the node-to-node cache-miss pull-through origin (#831, ADR
//! 001/022).
//!
//! A whole-blob cache miss reaches the network through the engine's origin
//! chain, so the production shape of "on a miss, discover a provider, open a
//! paid channel, pull, and populate the cache" is an [`Origin`] implementation
//! injected into that chain (appended last, so configured HTTP/FS/S3 origins
//! are tried first and the paid network pull is the final fallback). On
//! [`Origin::fetch`] this:
//!
//! 1. discovers providers for the hash (DHT [`crate::dht::find_providers`],
//!    with the origin-directory fallback),
//! 2. probes each candidate for rate + RTT and ranks them by the combined
//!    local+network reputation score ([`crate::selection::rank_candidates`]),
//! 3. opens (or reuses) a buyer payment channel to the best candidate and streams
//!    the whole blob straight into the local cache via the gap-driven
//!    [`decdn_client_pull::drive`] loop (#1682 — resumable, so a channel that
//!    runs dry mid-blob is topped up and the pull continues at the paid frontier;
//!    no whole-blob buffer is ever held in RAM), falling back through up to
//!    [`crate::selection::MAX_PROVIDER_ATTEMPTS`] providers,
//! 4. records the per-provider [`Outcome`] into the local reputation score
//!    (ADR 008 §Local Score Calculation).
//!
//! Every chunk group of the bao verified-stream is verified against the content
//! root as it lands and admitted straight into the cache (ADR 038, via
//! `admit_bao_stream`), so a dishonest provider is detected (and scored
//! [`Outcome::Corruption`]) rather than surfaced to the caller. On the window
//! path the corruption detector is the cache TEE's verifying decoder; its
//! verdict reaches the scorer via the pull leg's post-drive `record_outcome`
//! (#915).
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

mod abandon_drain;
mod admit_store;
mod backend_source;
mod funder;
mod pull_leg;
mod ranged_pull;

use abandon_drain::{ConnDrain, as_observer, drain_abandoned};
// Public only so the integration-test teardown helper can pin its own deadline
// above this cap. Not part of the crate's surface.
#[doc(hidden)]
pub use abandon_drain::ABANDON_DRAIN_CAP;
pub(crate) use admit_store::NodeAdmitStore;
#[allow(
    unused_imports,
    reason = "wired by the own-origin serve-miss orchestration"
)]
pub(crate) use backend_source::BackendSource;
pub(crate) use funder::NodeFunder;
use funder::{SETTLE_POLL_STEP, settle_wait_budget};
#[allow(
    unused_imports,
    reason = "wired by the own-origin serve-miss orchestration"
)]
pub(crate) use pull_leg::{PullLegTarget, run_local_pull_leg, run_pull_leg};

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy::dyn_abi::Eip712Domain;
use alloy::primitives::{Address, B256, U256};
use decdn_cache::origin::{Origin, OriginFetch};
use decdn_cache::{Hash, OriginKind, OriginPullError};
use decdn_client_pull::driver::DriveConfig;
use decdn_client_pull::{BudgetPacer, PeerSource, drive};
use decdn_protocol::client::{StreamError, VoucherRejectReason};
use iroh::{Endpoint, EndpointAddr, PublicKey};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use decdn_reputation::{LocalReputation, Outcome};

use decdn_incentive::PoolOpenFailureReason;
use decdn_protocol::client::NO_NAMESPACE;

use crate::buyer_channel::{OpenReported, PoolOpenPending, PoolOpener};
use crate::buyer_ledgers::BuyerLedgers;
use crate::client_requester::probe::probe_once;
use crate::client_requester::{
    BlobTooLarge, Cumulative, HashMismatch, LocalPullFault, PoolContext, PoolLedger, PullDeadlines,
    PullStalled, PullTimeout, RateAboveCeiling, ResumeOffsetPastEnd, UpstreamRateLimited,
    UpstreamRefused, UpstreamVoucherRejected, VoucherProgress, effective_rate_ceiling,
    open_progressive_pull as open_progressive_upstream, sign_client_binding,
};
use crate::dht::negative_cache::Hash as DhtHash;
use crate::dht::routing::{NodeId as DhtNodeId, RoutingTable};
use crate::dht::{
    LookupConfig, NegativeProbeCache, NodeAddressResolver, OriginDirectory, PositiveProbeCache,
    ProbedProvider, StakerSet,
};
use crate::metrics::Metrics;
use crate::selection::{
    Candidate, MAX_PROVIDER_ATTEMPTS, PROBE_EARLY_EXIT_CANDIDATES, PROBE_TIMEOUT, rank_candidates,
};

/// How a probe round decides it has collected enough holders (#1506).
#[derive(Clone, Copy)]
pub(crate) enum ProbeGather {
    /// Single-source failover: stop once [`PROBE_EARLY_EXIT_CANDIDATES`] holders
    /// answer, because the single-source pull loop tries at most that many.
    EarlyExit,
    /// Ranged assembly: stop once the admitted holders' coverage UNION spans the
    /// blob, so a set of partial holders whose fastest answers all cover the same
    /// discovery block is not mistaken for enough. A holder that reports its blob
    /// size (`ProbeResponseExt.total_bytes`) pins the block count the union must
    /// span; absent any size the round drains to the probe-fanout ceiling. Either
    /// way the fanout `take` is the upper bound, so the gather stays bounded.
    CoverageUnion,
}

/// Record a buyer channel open/reuse failure on `err` to the metrics in `deps`,
/// emitting a structured-log line with the failure-class `reason` (#966).
///
/// Bumps the unlabeled `node_pull_pool_open_failures` total and, when the
/// error chain carries a [`PoolOpenFailureReason`] (attached by the
/// `open_channel` kernel for the three `openChannel`-tx failure classes), the
/// matching `decdn_pool_open_failures_{reason}_total` sibling counter.
///
/// Three outcomes are NOT failures and return before that: [`PoolOpenPending`] (the
/// open outlived our budget and continues in the background), a reserved slot (a
/// reconcile holds the slot; retry), and anything the detached open task has already
/// reported ([`OpenReported`]) — which includes every one of `run_open`'s legs,
/// store faults and unreclaimable-expired channels included. So the
/// unlabeled arm below is genuinely a *residual*: an open/reuse failure raised
/// outside the open task itself.
///
/// Returns the [`PullMiss`] this failure is (#1560), so a channel open that failed because
/// of a fault in THIS node is not answered to the client as an absent blob. That matters
/// more than the buyer-key case #1560 was filed for: the loudest node-wide buyer faults in
/// this crate all land here, each of which would otherwise sign every client a
/// clean `NotFound` — a poisoned `opens_in_flight` mutex ("this node can no longer
/// open a buyer channel to ANY
/// provider and must be restarted"), an unreadable channel store ("can neither open nor
/// reuse a channel to any provider until the store recovers"), a store WRITE that leaves a
/// deposit untracked, a panicked open task, and a wallet that cannot fund a deposit.
///
/// **Attribution is decided at the raising site, not here.** Every one of those legs is
/// typed [`LocalPullFault`] where it is raised, and this function only reads the marker.
/// That split is not stylistic: [`OpenReported`] means "already logged and metered, do not
/// restate" — it says nothing about whose fault the failure is — and every leg of the open
/// task attaches it. So a ladder that tried to classify by [`PoolOpenFailureReason`]
/// here would never run: the `OpenReported` arm ends the walk first.
///
/// Consequently the unmarked legs are the deliberate `Clean` ones: a pending open, a
/// reconcile-held slot, an unreclaimable expired channel, a `ContractRevert` (deterministic
/// on-chain, possibly specific to this provider), and an `RpcError` (transient at the
/// network layer by its own definition). Refusing `InternalError` for any of those would
/// steer clients off a node that is fine.
// The arms are a flat sentinel ladder; splitting it would scatter one decision.
#[allow(clippy::cognitive_complexity)]
fn record_pool_open_failure(
    deps: &NodeOriginDeps,
    provider_addr: Address,
    err: &anyhow::Error,
) -> PullMiss {
    // Not a failure at all: the open ran past our per-candidate budget and is still
    // going in the background (#1143). Meter it apart from real failures — a sustained rate
    // means this node's chain lane is too slow for `CHANNEL_OPEN_CALLER_BUDGET`, which is a
    // very different diagnosis from a reverting or under-funded open.
    //
    // NOT `node_pull_timeout_sec`. The channel open has its OWN budget;
    // `DEFAULT_NODE_PULL_TIMEOUT_SEC`'s own doc says outright that "raising this to
    // give a slow L2 more room does nothing: that is the channel open." That knob is
    // inert for this symptom.
    if err.downcast_ref::<PoolOpenPending>().is_some() {
        deps.metrics.node_pull_pool_open_pending();
        debug!(%provider_addr, %err, "node-origin: pool open still in flight; trying the next candidate");
        return PullMiss::Clean;
    }
    // Ahead of `OpenReported`, deliberately, and this ordering is load-bearing (#1560). The
    // node-wide buyer faults are reported by the open path AND typed `LocalPullFault` there,
    // so they carry BOTH markers; `OpenReported` alone would end the walk one arm below with
    // a `debug!` and a clean miss, which is how a node that "must be restarted" kept telling
    // clients the content does not exist. `a_node_wide_channel_open_fault_refuses_rather_
    // than_reporting_an_absent_blob` builds its fixtures with both markers precisely so
    // swapping these two arms fails.
    //
    // Metered here rather than at the raising site's counter alone, so the same emergency
    // reads on `node_pull_local_fault_total` whichever leg of the buyer path raised it. Note
    // this fires ONCE PER CANDIDATE, so one node-wide fault moves the counter by up to
    // `MAX_PROVIDER_ATTEMPTS` per request — the rate is the signal, not the absolute.
    //
    // It also logs despite `OpenReported`, which normally means "already reported, stay
    // quiet". Deliberate: the raising site's line says what broke, and this one says what it
    // COST — that a client was refused rather than told the blob is missing.
    if err.downcast_ref::<LocalPullFault>().is_some() {
        deps.metrics.node_pull_local_fault();
        warn!(
            %provider_addr, %err,
            "node-origin: LOCAL buyer-side fault opening a channel — this node cannot pay \
             any provider; exonerating the upstream and refusing rather than reporting a miss"
        );
        return PullMiss::LocalFault;
    }
    // The detached open task already logged and metered this one (#1143). It has to
    // be the reporter, because when an open fails, every caller may already have
    // timed out and left — so if the caller were the reporter, the failure would go
    // unobserved exactly when it is least affordable. This arm keeps the caller that
    // DID happen to still be waiting from double-counting it.
    if err.downcast_ref::<OpenReported>().is_some() {
        debug!(%provider_addr, %err, "node-origin: buyer channel open failed (reported by the open task)");
        // Reported by the open path and NOT typed as ours there, so it is one of the legs
        // that leaves this node able to pay somebody else: a `ContractRevert`, an `RpcError`,
        // or an expired channel that could not be reclaimed. Another candidate may still
        // deliver, and if none does, `NotFound` is a true statement about what we could
        // obtain. Everything the open path knows to be node-wide arrives marked and returned
        // one arm above — the marker is the contract, not this arm's guesswork.
        return PullMiss::Clean;
    }
    deps.metrics.node_pull_pool_open_failure();
    let reason = err.downcast_ref::<PoolOpenFailureReason>().copied();
    if let Some(reason) = reason {
        deps.metrics.pool_open_failure_by_reason(reason);
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
        reason = reason.map_or("unclassified", PoolOpenFailureReason::as_label),
        %err,
        "node-origin: buyer channel open/reuse failed (raised outside the open task — \
         suspect this node's store or lock state, not the peer)"
    );
    // The residual is node-local by elimination: every leg of the open task returns above
    // (marked or `OpenReported`), so what reaches here was raised outside it and points at
    // this node's own state. In practice the only such leg today is a supervisor aborted at
    // runtime shutdown, where "do not retry this node" is if anything the more useful answer.
    //
    // This is a catch-all that defaults to the LOUDER verdict, which is the opposite
    // discipline from `PullMiss::for_verdict` (deliberately catch-all-free so no future
    // variant inherits an answer). The inversion is intended and is the policy for this
    // ladder: attribution here comes from a marker the raising site attaches, so an unmarked
    // arrival is by definition a leg nobody classified — and an unclassified failure of our
    // own buyer machinery should surface, not be signed away as absent content. A future leg
    // that is genuinely per-provider must say so by returning `OpenReported` without the
    // marker, exactly as the existing ones do.
    deps.metrics.node_pull_local_fault();
    PullMiss::LocalFault
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
    /// THROUGHPUT-FLOOR window on the STREAMING stage (#1797). Bytes are counted off the
    /// QUIC stream sub-frame, so a pull aborts only when throughput over this window falls
    /// below [`Self::min_throughput_bps`] — never on a large blob or a big frame.
    /// Deliberately not a wall clock: bounding the bytes by wall clock caps the blob size
    /// this node can pull through at `pull_timeout × link speed`, which is the bug this
    /// avoids.
    pub stall_window: Duration,
    /// Minimum sustained upstream throughput (bytes/sec) over [`Self::stall_window`]; `0`
    /// disables the throughput test and leaves idle detection (#1797).
    pub min_throughput_bps: u64,
    /// Buyer-side blob-size ceiling (`cache.max_blob_size_mb` × MB), `0` = unlimited.
    /// Enforced on the bytes that ACTUALLY arrive on the miss-pull leg, never on the
    /// peer's unverified `total_bytes` claim: the pull aborts with `BlobTooLarge`
    /// once cumulative received bytes cross it (#1895).
    pub max_blob_size_bytes: u64,
    /// Buyer-side ABSOLUTE per-MB rate ceiling (`cache.max_rate_per_mb`), `0` =
    /// unlimited (#1375). Combined via [`effective_rate_ceiling`] with the
    /// probe-relative bound (the rate the chosen candidate advertised) so the node
    /// refuses a stream quote that exceeds the lower of the two before paying — and
    /// retains the signed over-quote as rate-manipulation evidence.
    pub max_rate_per_mb: u64,
    /// The deposit a freshly-opened buyer channel escrows, and the target a
    /// mid-pull reactive top-up raises an exhausted channel toward
    /// (`blockchain.buyer_working_deposit_micro_usdc`, #1530). The proactive
    /// low-water refill (`crate::buyer_channel::refill_decision`) targets the
    /// same deposit; the two legs differ only in what triggers them.
    ///
    /// `U256::ZERO` disables the reactive top-up entirely. Config never resolves
    /// to zero (the resolver rejects it), but the value flows into the paid leg's
    /// [`decdn_client_pull::driver::DriveConfig`], where zero switches the
    /// pacer's reactive arm off.
    pub working_deposit: U256,
    /// This node's estimate of an upstream's refundable floor `M` (ADR 003 § Pool
    /// solvency): its own `blockchain.pool_min_remaining_deposit_micro_usdc`, the
    /// floor it keeps on the pools it serves. An upstream refuses a new stream once
    /// the pool's remaining deposit, less its `M`, cannot cover a window, and the
    /// refusal reads as a plain miss. The pull leg's pacer therefore tops up while
    /// the deposit still covers this floor plus the next voucher, so a mid-pull
    /// re-open is not refused. It only triggers a top-up; it never refuses a draw.
    pub seller_reserve: U256,
    /// How often this node's chain watcher polls for events
    /// (`blockchain.event_poll_interval_ms`), used to size the post-top-up settle
    /// wait (#1530).
    ///
    /// The wait is for the UPSTREAM's watcher, not ours, but every node in the
    /// network runs the same default cadence and this is the only local reading of
    /// it we have. See `funder::settle_wait_budget`.
    pub event_poll_interval: Duration,
    /// DHT lookup tuning.
    pub lookup: LookupConfig,
    /// This node's self-attested region (`identity.region`), or `None` when
    /// unset. Used to apply the ADR 030 latency-vs-claim penalty: a probed peer
    /// that claims *this* region yet answers slower than the latency ceiling
    /// (`REGION_LATENCY_MAX_MS`) is a region-spoofing signal. `None` disables the
    /// penalty (nothing to compare against).
    pub own_region: Option<String>,
    /// ADR 041 buy-side profitability gate: the most this node pays upstream,
    /// per MB, on a cache-miss relay leg. `select_policy(&cfg.cache.serve_economics)`
    /// picks `OffPolicy` (no ceiling) or `MarginPolicy` at construction.
    pub serve_economics: Arc<dyn crate::serve_economics::ServeEconomicsPolicy>,
    /// Live operator fee-share (basis points) from `FeeRouter.getShares()[0]`,
    /// the `(1 - f)` numerator the `margin` serve-economics policy amortizes
    /// over. Seeded from chain at startup and kept current by the fee-shares
    /// watcher (`crate::fee_shares_watcher::route`).
    pub operator_shares: crate::fee_shares::OperatorShares,
    /// ADR 040 shared frequency estimator, when built (`cache.eviction_policy`
    /// / `cache.admission_policy` == `"tinylfu"`, or `cache.serve_economics.policy`
    /// == `"margin"`). Feeds the `margin` policy's heat-estimate ceiling input;
    /// `None` when no consumer needs it.
    pub frequency_estimator: Option<Arc<dyn decdn_cache::FrequencyEstimator>>,
    /// This node's live delivery-rate floor clamp — the same handle the probe
    /// and client handlers hold. Combined with `sell_rate_base` to derive the
    /// node's current sell rate `P_sell`, the serve-economics policy's other
    /// input.
    pub sell_rate_bounds: crate::rate_bounds::RateBounds,
    /// This node's configured base served rate per MB (`payment.rate_per_mb`),
    /// BEFORE the on-chain floor clamp — the same base value the probe handler
    /// is constructed with.
    pub sell_rate_base: u64,
    /// ADR 041 per-source warming allowance: bounds the loss from speculative
    /// above-floor buys per upstream source node. The buy loop reads
    /// `available(source)` to pick the warm-at-market vs amortized-floor regime,
    /// and debits the full buy cost on a successful speculative pull. The SAME
    /// `Arc` is shared with the eviction path (which forgets a dropped hash's
    /// tag) and with the warming-credit aggregator, which applies the realized
    /// margin the serve path enqueues on each re-serve.
    pub warming: Arc<crate::warming_allowance::WarmingAllowance>,
}

/// ADR 030 default heuristic (RTT > 150ms to a same-claimed-region node). The
/// value compared against it is `probe_once`'s observed probe latency, which on
/// the cold path also spans QUIC connection setup — not a bare network round
/// trip — so 150ms is a deliberately generous ceiling that a truly in-region
/// peer clears even with a full handshake. The threshold is a documented
/// constant, deliberately not yet a governance knob per ADR 030 (which defers
/// threshold/sample/decay tuning to a later ADR 008 revision).
const REGION_LATENCY_MAX_MS: u32 = 150;

/// Whether the ADR 030 latency-vs-claim penalty applies to a probed peer.
///
/// It fires only when the peer self-attests the observer's *own* region
/// (`own_region`) yet the observed probe latency exceeds [`REGION_LATENCY_MAX_MS`]
/// — the canonical region-spoofing signal. An unset own region (`None`) or an
/// unknown peer region (empty) disables it: there is nothing to contradict.
fn region_latency_penalty_applies(own_region: Option<&str>, claimed: &str, rtt_ms: u32) -> bool {
    !claimed.is_empty() && own_region == Some(claimed) && rtt_ms > REGION_LATENCY_MAX_MS
}

impl NodeOriginConfig {
    /// The stage bounds for one upstream pull: a wall clock on the open, a throughput floor
    /// on the stream, no overall cap (#1797).
    ///
    /// # Errors
    ///
    /// `DeadlineError::ZeroBudget` if either duration is zero. `config`'s own validator
    /// already rejects that at startup, so this is the second lock on a door that must not
    /// open: a zero window makes the throughput floor unsatisfiable and abandons every
    /// upstream on its first read. Both call sites route the error to [`LocalPullFault`],
    /// which meters it as OUR emergency and scores no peer (#1145 review).
    fn deadlines(&self) -> anyhow::Result<PullDeadlines> {
        PullDeadlines::new(
            self.pull_timeout,
            self.stall_window,
            self.min_throughput_bps,
        )
        .map_err(|err| {
            anyhow::anyhow!(
                "node pull deadlines are unusable ({err}); \
                     check cache.node_pull_timeout_sec and cache.node_pull_stall_window_sec"
            )
            .context(LocalPullFault)
        })
    }
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
    pub buyer: Arc<dyn PoolOpener>,
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
    /// Requester-side negative-probe cache (drops known-absent providers).
    pub negative_cache: NegativeProbeCache,
    /// Requester-side positive probe cache (ADR 001 §Probe cache): lets a repeat
    /// miss for a hash inside the TTL skip the DHT lookup and probe fanout
    /// (#1165). Beside the negative cache, and by value for the same reason:
    /// `NodeOrigin` holds its deps behind `Arc<OnceLock<NodeOriginDeps>>`, so
    /// every concurrent pull already shares one instance through that `Arc`, and
    /// the cache's own `Mutex` gives it interior mutability behind `&self`. A
    /// second `Arc` here would buy nothing but a pointer chase.
    pub probe_cache: PositiveProbeCache,
    /// Node metrics for the paid-pull observability counters (#831).
    pub metrics: Arc<Metrics>,
    /// The `CapacityBond` registry's `NodeId → regionHint` projection (ADR 030),
    /// read on the selection/pull path to apply the region-latency penalty to a
    /// same-region peer that answers slower than the ceiling.
    pub registry_regions: Arc<std::sync::RwLock<HashMap<DhtNodeId, String>>>,
    /// Resolved pull tuning.
    pub config: NodeOriginConfig,
    /// The live voucher ledger of each provider's current channel, shared by every
    /// concurrent pull on it (#1145 review). Not a cache — see [`BuyerLedgers`] for
    /// why a per-pull ledger collides on the same cumulative amount and what that
    /// costs.
    pub ledgers: Arc<BuyerLedgers>,
    /// Providers suppressed from ranking because one rejected our voucher on a reason this
    /// lane cannot recover from — mapped to the first Unix second at which the provider is
    /// rankable again (#1145 review). The horizon is a fixed
    /// `WEDGED_PROVIDER_SUPPRESSION_SECS` window, not a channel deadline: the buyer pool is
    /// shared across every provider, so it carries no per-provider expiry to key one on.
    /// Keyed by the peer's [`DhtNodeId`], so `probe_and_rank` can skip a wedged provider for
    /// ALL hashes, not just the one that wedged it. In-memory: on restart the first miss
    /// re-wedges and re-suppresses within one pull.
    pub wedged_providers: Arc<Mutex<HashMap<DhtNodeId, u64>>>,
    /// The cache engine this origin admits into. Set at `provision`, after the
    /// engine exists. The node-to-node pull streams straight into it via
    /// `admit_bao_stream`, so a whole blob never lands in RAM (#1682).
    pub engine: decdn_cache::CacheEngine,
}

impl std::fmt::Debug for NodeOriginDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeOriginDeps")
            .field("self_id", &self.self_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl NodeOriginDeps {
    /// Suppress `pk` from ranking for [`WEDGED_PROVIDER_SUPPRESSION_SECS`], so
    /// `probe_and_rank` and `cached_candidates` skip the provider for ALL hashes until that
    /// window elapses (#1145 review). The window is owned here rather than passed in because
    /// it is the same for every lane-terminal reason: the pool the lane draws on has no
    /// per-provider deadline to key one on, so there is nothing for a caller to vary it by.
    ///
    /// A re-wedge RESTARTS the window rather than extending the original — the horizon a
    /// provider earns is measured from its most recent rejection, not its first.
    fn record_wedged(&self, pk: &PublicKey) {
        let node = DhtNodeId::from_bytes(*pk.as_bytes());
        record_wedged_at(
            &mut self
                .wedged_providers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            node,
            crate::payment_settlement::unix_now(),
        );
    }

    /// True if `peer` is wedged and still inside its suppression horizon. Prunes horizons that
    /// have passed on read: the window is a cooling-off period, not a verdict, so once it
    /// elapses the provider is worth ranking again and paying for a sweep task to say so would
    /// buy nothing a read cannot.
    fn provider_is_wedged(&self, peer: &DhtNodeId, now_secs: u64) -> bool {
        let mut wedged = self
            .wedged_providers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        prune_and_check_wedged(&mut wedged, peer, now_secs)
    }
}

/// Suppress `node` for [`WEDGED_PROVIDER_SUPPRESSION_SECS`] measured from `now_secs`.
///
/// `insert` overwrites, so re-wedging a provider already in the map restarts its window from
/// `now_secs` instead of leaving it on the older horizon. That is the intended reading of a
/// second rejection: the provider earned a fresh window, not the remainder of its first.
///
/// Free-standing for the same reason as [`prune_and_check_wedged`] — it is the write half of
/// the same policy, and pairing them is what lets one test pin the horizon end to end.
fn record_wedged_at(wedged: &mut HashMap<DhtNodeId, u64>, node: DhtNodeId, now_secs: u64) {
    wedged.insert(
        node,
        now_secs.saturating_add(WEDGED_PROVIDER_SUPPRESSION_SECS),
    );
}

/// Drop every suppression horizon `now_secs` has passed, then report whether `peer` still has
/// one. The horizon is exclusive: a provider is rankable again ON the second its window ends.
///
/// Free-standing over the map rather than a method, so the window semantics are testable
/// without a live [`NodeOriginDeps`] — which needs an iroh endpoint, and would be a fixture far
/// larger than the assertion.
fn prune_and_check_wedged(
    wedged: &mut HashMap<DhtNodeId, u64>,
    peer: &DhtNodeId,
    now_secs: u64,
) -> bool {
    wedged.retain(|_, expires_at| *expires_at > now_secs);
    wedged.contains_key(peer)
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

    /// A clone of the write-once dependency handle, for the off-task serve-miss pull
    /// leg (#1621 B2 part 2): the pull runs on its OWN current-thread runtime (its
    /// `drive` is non-`Send`, which the iroh `ProtocolHandler::accept` bound forbids
    /// on the serve task), so it cannot borrow `&self`. It captures this `Arc` and
    /// reads the deps via `get()` on the pull thread — the same handle
    /// [`SettleOnDrop`] already carries across a drop.
    pub(crate) fn deps_arc(&self) -> Arc<OnceLock<NodeOriginDeps>> {
        Arc::clone(&self.deps)
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
            let target = DhtHash::from_bytes(hash_bytes);
            // ONE budget for the whole fetch, spent across both phases — see
            // `PullOutcome`.
            let mut budget = MAX_PROVIDER_ATTEMPTS;
            // `node_pull_attempts` counts pull ORCHESTRATIONS ("found ≥1 candidate
            // to try"), and is the denominator for the success / corruption /
            // unreachable rates. One `fetch` is one orchestration however many
            // candidate lists it walks, so a probe-cache hit that exhausts its
            // providers and falls through to the cold path must still meter
            // exactly once — otherwise every such fetch inflates the denominator
            // and quietly deflates every rate built on it.
            let mut attempt_metered = false;
            // Latched across BOTH walks, like `attempt_metered` — see
            // `miss_answer` for what it buys.
            let mut miss = PullMiss::Clean;

            // ADR 001 §Probe cache: "On a cache miss the requester checks the probe
            // cache first; if a valid entry exists, it skips DHT lookup and goes
            // straight to selection."
            if let Some(cached) = cached_candidates(deps, target).await {
                deps.metrics.probe_cache_hit();
                deps.metrics.node_pull_attempt();
                attempt_metered = true;
                let outcome = try_pull(&deps_lock, deps, &cached, hash_bytes, budget).await;
                match outcome.payload {
                    Ok(()) => return Ok(OriginFetch::AlreadyAdmitted),
                    Err(failed) => miss = miss.or(failed),
                }
                budget = budget.saturating_sub(outcome.attempts);
                // Every cached provider we had budget to try failed to deliver.
                // The entry has been disproved by the only evidence that outranks
                // a probe — actual pulls — so drop it rather than let it keep
                // hitting for the rest of its TTL. Any untried tail beyond
                // `budget` is forfeited with it: a fresh probe is cheaper than
                // trusting a list whose top-ranked members just failed.
                deps.probe_cache.invalidate(&target);
                if budget == 0 {
                    // The cached candidates ate the whole fetch-wide budget.
                    // Running a fresh lookup + probe now would either exceed the
                    // worst case `outer_pull_deadline` is sized for, or discover
                    // providers it has no attempts left to try. The entry is gone,
                    // so the next fetch goes cold.
                    //
                    // Answered through `miss_answer` like the cold-path arm below,
                    // so an exhausted budget spent on OUR faults is not signed to a
                    // client as an absent blob (#1560).
                    debug!(%hash, "node-origin: probe-cache candidates exhausted the attempt budget");
                    return miss_answer(miss);
                }
            } else {
                deps.metrics.probe_cache_miss();
            }

            // ADR 001 §Probe cache: "if all fail, run a fresh DHT lookup + probe."
            // This is the generic `Origin::fetch` path (the buffered `populate` fill),
            // a hash-only pull with no client namespace, so it takes no on-chain
            // origin-directory fallback (`NO_NAMESPACE`). The namespace-aware
            // client-serve serve-miss path is `open_pull_leg`.
            let providers = discover(deps, hash_bytes, U256::ZERO).await;
            if providers.is_empty() {
                // `node_pull_no_providers` means "the blob is unavailable on the
                // network, NOT a pull failure" — mutually exclusive with
                // `node_pull_attempts` per fetch. A hit-then-fallthrough fetch
                // already TRIED cached providers (and metered the attempt), so an
                // empty re-discovery here is a pull story, not an availability
                // one; metering both would break that exclusivity.
                if !attempt_metered {
                    deps.metrics.node_pull_no_providers();
                }
                debug!(%hash, "node-origin: no providers discovered for cache-miss pull");
                return miss_answer(miss);
            }
            if !attempt_metered {
                deps.metrics.node_pull_attempt();
            }
            // Writes the probe cache at its tail. The buffered fill is a
            // single-source pull (`try_pull`), so one working holder is enough.
            let ranked = probe_and_rank(deps, providers, hash_bytes, ProbeGather::EarlyExit).await;
            match try_pull(&deps_lock, deps, &ranked, hash_bytes, budget)
                .await
                .payload
            {
                Ok(()) => Ok(OriginFetch::AlreadyAdmitted),
                Err(failed) => miss_answer(miss.or(failed)),
            }
        })
    }

    fn kind(&self) -> OriginKind {
        OriginKind::Peer
    }
}

/// The buffered pull's answer for a walk that delivered nothing (#1560).
///
/// A [`PullMiss::Clean`] is a `NotFound`: no provider had it, or every one of them
/// refused, stalled, or timed out. That is a true statement about what this node
/// could obtain, and the engine surfaces it as `CacheError::NotFound`.
///
/// A [`PullMiss::LocalFault`] is not. The blob may exist on every candidate we
/// asked; what failed is US — a buyer key that cannot sign, a deadline config that
/// cannot run, a signature the upstream cannot verify. Reported as a miss it
/// becomes a signed wire `NotFound`, which is exactly the false claim about content
/// that `StreamError::InternalError` ("unexpected failure; do not retry this node")
/// exists to keep a broken node from making. So it surfaces as an origin error
/// instead, and the serve path's existing `CacheError::OriginError` →
/// `FillOutcome::HardFault` → `ServeRejectReason::InternalError` chain does the
/// rest.
///
/// [`OriginPullError::Permanent`] rather than `Transient`, on three counts: the
/// retry loop must not re-run a pull whose signer is broken; `Permanent` records
/// `OriginOutcome::Available` on the per-origin circuit breaker, so our own defect
/// does not trip a breaker that describes the PEER origin's health; and the engine's
/// chain walk lets any error outrank a `NotFound` from another origin, which is the
/// precedence this fix wants.
///
/// A [`PullMiss::BelowMargin`] answers the same `NotFound` as [`PullMiss::Clean`]:
/// this node's serve-economics buy ceiling is a local policy decision, not a fact
/// about the content, and it must never reach the wire as a distinct code — that
/// would let a client fingerprint this node's pricing floor by probing for it.
fn miss_answer(miss: PullMiss) -> Result<OriginFetch, OriginPullError> {
    match miss {
        PullMiss::Clean | PullMiss::BelowMargin => Ok(OriginFetch::NotFound),
        PullMiss::LocalFault => Err(OriginPullError::Permanent(anyhow::anyhow!(
            "node-origin: a LOCAL buyer-side fault hit at least one attempted candidate \
             and none delivered; this node cannot pay, so it refuses rather than signing \
             a NotFound for content that may well exist (#1560)"
        ))),
    }
}

/// Discover candidate providers for `hash`: the DHT iterative lookup first,
/// falling back to the on-chain origin directory keyed on `namespace_id` when the
/// lookup converges empty (ADR 022 §`FIND_VALUE` Flow). `namespace_id` is the
/// namespace the serving node received on the client `StreamRequest`. Within the
/// pull it is consumed here, at the directory fallback; a DHT-discovered *holder*
/// already has the bytes and needs no namespace, but a directory-discovered *cold
/// origin* is then reached with this same namespace so its own pull-through gate
/// resolves (#1401, threaded by the progressive client-serve path). Origin backends
/// (S3/HTTP/FS) are hash-keyed and never see it. `NO_NAMESPACE` (0) resolves to no
/// authorized origins, so a hash-only pull (the buffered fill) simply gets no
/// directory fallback (ADR 002 §Namespace 0).
async fn discover(
    deps: &NodeOriginDeps,
    hash_bytes: [u8; 32],
    namespace_id: U256,
) -> Vec<DhtNodeId> {
    let target = DhtHash::from_bytes(hash_bytes);
    // `find_providers` carries each holder's range-keyed `Coverage` alongside
    // its `NodeId` (ADR 039-adjacent partial-holder discovery) — a STALE
    // DHT-lookup hint, not a live observation. It is dropped here rather than
    // used to prune candidates before probing: the authoritative coverage a
    // ranked candidate carries onward comes from its fresh `ProbeResponseExt`
    // instead (`probe_candidate`, #1506). A later task may use this hint to
    // skip probing a candidate whose stale coverage already misses the whole
    // pull range; today every discovered candidate is still probed.
    let providers: Vec<DhtNodeId> = crate::dht::find_providers(
        &deps.endpoint,
        &deps.routing_table,
        &deps.staker_set,
        &deps.negative_cache,
        deps.self_id,
        target,
        deps.config.lookup,
        Some(&deps.metrics),
    )
    .await
    .into_iter()
    .map(|(node, _coverage)| node)
    .collect();
    if providers.is_empty() {
        deps.origin_directory.lookup_origins(namespace_id).await
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
    gather: ProbeGather,
) -> Vec<Candidate> {
    use futures_util::stream::StreamExt;
    let now_secs = crate::payment_settlement::unix_now();
    let target = DhtHash::from_bytes(hash_bytes);
    // Probe candidates CONCURRENTLY so the probe phase is bounded by a single
    // `PROBE_TIMEOUT` rather than `fanout × PROBE_TIMEOUT`: a few slow or
    // unreachable peers must not burn the whole pull budget before a healthy
    // provider is even tried. `probe_candidate`'s side effects (reputation
    // record, negative-cache insert) are all behind locks, so concurrent runs
    // are safe; ranking afterwards makes result order irrelevant.
    let mut probes: futures_util::stream::FuturesUnordered<_> = providers
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
        // Also drop WEDGED providers, for ALL hashes until the suppression window elapses
        // (#1145 review): a provider whose voucher we just failed to pay on lane-terminal terms
        // is one we cannot pay for any blob right now. Nothing else SUPPRESSES it — the
        // negative-cache entry the wedge writes alongside this one is keyed on (peer, hash), so
        // it lapses for any other blob, and the arm deliberately scores no reputation. Without
        // this the peer is re-selected on the next miss for a different hash and re-wedged —
        // burning a candidate slot each time.
        .filter(|peer| !deps.provider_is_wedged(peer, now_secs))
        .take(deps.config.probe_fanout)
        .map(|peer| probe_candidate(deps, peer, hash_bytes))
        .collect();
    // Collect answers as they arrive and stop early once enough good candidates are in
    // hand: waiting on `join_all` would pace the whole round to the SLOWEST probe — a dead
    // peer that only resolves at its `PROBE_TIMEOUT` — even when the fastest few already
    // answered. `PROBE_TIMEOUT` still bounds each probe, so a sparse round that never reaches
    // the stop condition simply drains to the ceiling; a healthy round selects at the speed
    // of its fastest good answers.
    //
    // The stop condition depends on `gather`: [`ProbeGather::EarlyExit`] stops at a fixed
    // count of holders (single-source failover), while [`ProbeGather::CoverageUnion`] stops
    // once the admitted holders' coverage union spans the blob (ranged assembly, #1506) — a
    // set of partial holders whose fastest answers all cover the same block must not stop
    // short of the holders that cover the rest.
    let mut candidates: Vec<Candidate> = Vec::new();
    // Union tracking, used only by `CoverageUnion`: the covered discovery blocks seen so far
    // and the largest block count a holder has reported for this blob.
    let mut union_blocks: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    let mut target_blocks: u32 = 0;
    while let Some(result) = probes.next().await {
        if let Some((candidate, total_bytes)) = result {
            let done = match gather {
                ProbeGather::EarlyExit => {
                    candidates.push(candidate);
                    candidates.len() >= PROBE_EARLY_EXIT_CANDIDATES
                }
                ProbeGather::CoverageUnion => {
                    // A holder's own reported size pins how many blocks the blob has;
                    // ignore its coverage bits past that (untrusted wire may set spurious
                    // high bits — see `Coverage`).
                    if let Some(bytes) = total_bytes {
                        let holder_blocks = decdn_protocol::num_blocks(bytes);
                        target_blocks = target_blocks.max(holder_blocks);
                        for block in candidate.coverage.covered_blocks() {
                            if block < holder_blocks {
                                union_blocks.insert(block);
                            }
                        }
                    }
                    candidates.push(candidate);
                    // Complete once every block `0..target_blocks` is in the union. With no
                    // holder-reported size (`target_blocks == 0`) this stays false and the
                    // round drains to the probe-fanout ceiling.
                    target_blocks > 0 && (0..target_blocks).all(|b| union_blocks.contains(&b))
                }
            };
            if done {
                break;
            }
        }
    }
    // Cancel any probes still pending: the loop has enough (or the set is drained). Their
    // reputation / negative-cache side effects simply do not run for peers we never needed.
    drop(probes);
    let ranked = rank(candidates);
    // ADR 001 §Probe cache: retain the top 10 by selection score, so a repeat
    // miss for this hash inside the TTL skips the lookup and the probe fanout.
    // `insert` does the truncation; `ranked` is already in selection order,
    // which is the ordering that claim depends on.
    //
    // The triple plus the probe's UNSIGNED coverage is stored (#1506) — never
    // the signed `ProbeResponse` (its `slash_sig` is another node's slashable
    // statement, and #1165's "no evidence retention" is that this cache must not
    // become an evidence locker; coverage carries no such author), and never
    // `reputation`, which `cached_candidates` recomputes.
    deps.probe_cache.insert(
        target,
        ranked
            .iter()
            .map(|c| ProbedProvider {
                node_id: DhtNodeId::from_bytes(c.node_id),
                rate_per_mb: c.rate_per_mb,
                rtt_ms: c.rtt_ms,
                coverage: c.coverage.clone(),
            })
            .collect(),
    );
    ranked
}

/// `rank_candidates` reduced to the best-first `Candidate` list both pull paths
/// consume. Shared by the cold path and the probe-cache-hit path so a change to
/// what "ranked" means cannot apply to one and not the other.
fn rank(candidates: Vec<Candidate>) -> Vec<Candidate> {
    rank_candidates(candidates)
        .into_iter()
        .map(|r| r.candidate)
        .collect()
}

/// Probe a single provider, returning a ranked-ready [`Candidate`] paired with
/// the holder's reported blob size (`ProbeResponseExt.total_bytes`, for the
/// coverage-union gather) iff it responds, validates, and reports holding the
/// blob. Side effects: a failed probe scores the provider [`Outcome::Unreachable`],
/// except a probe the provider shed with `APP_ERR_RATE_LIMITED`, which suppresses
/// the pair for [`REFUSAL_SUPPRESSION_TTL`] and scores nothing (#1986); a
/// same-region claim answered slower than [`REGION_LATENCY_MAX_MS`] scores
/// [`Outcome::RegionLatencyMismatch`] (ADR 030); a reachable-but-absent provider
/// is recorded in the negative-probe cache.
// Straight-line probe → classify → build; the tracing macros and the three
// sequential drop-conditions inflate the cognitive-complexity metric past the
// threshold (same inflation noted in `chain_staker_set`), and splitting the
// validation further would obscure the flow rather than clarify it.
#[allow(clippy::cognitive_complexity)]
async fn probe_candidate(
    deps: &NodeOriginDeps,
    peer: DhtNodeId,
    hash_bytes: [u8; 32],
) -> Option<(Candidate, Option<u64>)> {
    let Ok(pk) = PublicKey::from_bytes(peer.as_bytes()) else {
        // A staker-filtered routing entry should always decode; a failure
        // implies upstream state corruption — skip rather than panic.
        return None;
    };
    let probe_ts = now_micros();
    let (resp, resp_ext, rtt_ms) = match probe_once(
        &deps.endpoint,
        EndpointAddr::new(pk),
        hash_bytes,
        probe_ts,
        PROBE_TIMEOUT,
    )
    .await
    {
        Ok(ok) => ok,
        Err(err) => {
            // A `0x10` shed is the peer ANSWERING — "not now" — not the peer being
            // unreachable (#1986). It gets what the handler-level `Overloaded` refusal
            // gets in `classify_refusal`: the pair is suppressed for the short TTL so
            // a retry burst stops re-spending a probe slot on it, and no reputation
            // outcome is recorded. A `global-full` shed skips the node's close ack-wait
            // and can still arrive as a bare drop; that residue scores below.
            if err.downcast_ref::<UpstreamRateLimited>().is_some() {
                debug!(%err, "node-origin: probe shed by the upstream's rate limiter; suppressing briefly, not scoring");
                deps.metrics.node_upstream_rate_limited();
                deps.negative_cache.record_failure_with_ttl(
                    peer,
                    DhtHash::from_bytes(hash_bytes),
                    REFUSAL_SUPPRESSION_TTL,
                );
                return None;
            }
            // A failed probe is a reachability signal: the upstream could not be
            // reached for this interaction (ADR 008 §Local Score).
            debug!(
                peer = %pk,
                provider_addr = ?deps.addr_resolver.address_of(&peer),
                %err,
                "node-origin: probe failed; scoring provider unreachable"
            );
            record_outcome(deps, pk, &Outcome::Unreachable);
            return None;
        }
    };
    // Shape, echoed-field correlation, and `slash_sig` recovery to the peer's
    // registered operator address (ADR 005 §Signer binding, ADR 014 §1). Selection
    // reads `has_blob` and `rate_per_mb` off this response and then PAYS the winner,
    // so an unrecovered signature would let a node win on a rate it never committed
    // to — and leave nothing to slash when it declines to honour it.
    //
    // A failure is requester-local policy, never an attributable fault: dropped
    // without a reputation event, exactly as for a timeout, because a signature that
    // does not recover attributes nothing to anyone (ADR 008).
    let Some(provider_addr) = deps.addr_resolver.address_of(&peer) else {
        debug!("node-origin: probed peer has no resolvable operator address; skipping");
        return None;
    };
    if let Err(err) = crate::client_requester::probe::verify_probe_response(
        &resp,
        provider_addr,
        &deps.slash_domain,
        hash_bytes,
        probe_ts,
    ) {
        debug!(%err, "node-origin: dropping an unverifiable probe response");
        return None;
    }
    // #1506: `has_blob` and `coverage.is_empty()` are a biconditional by
    // construction on an honest responder. Neither field is in the signed
    // set (`coverage` is unsigned, and this mismatch has no attributable
    // author to slash — same reasoning as an unrecovered `slash_sig` above),
    // so a violation is dropped rather than scored to reputation.
    if !resp_ext.consistent_with(resp.body.has_blob) {
        debug!(
            has_blob = resp.body.has_blob,
            "node-origin: dropping a probe response with has_blob/coverage mismatch"
        );
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
    let rtt = ms_to_u32(rtt_ms);
    // Peer's self-attested region (ADR 030), resolved from the on-chain
    // `CapacityBond` registry projection; empty when the registry has no
    // region hint for this peer.
    let region = crate::dht::capacity_bond_registry::region_of(
        &deps.registry_regions,
        DhtNodeId::from_bytes(*peer.as_bytes()),
    )
    .unwrap_or_default();
    // ADR 030 canonical latency-vs-claim penalty: a peer that self-attests THIS
    // node's own region yet answers slower than the latency ceiling is spoofing
    // its region. A local-only signal — folded straight into the local EWMA — so
    // it self-corrects as fast as we probe and needs no new protocol surface.
    if region_latency_penalty_applies(deps.config.own_region.as_deref(), &region, rtt) {
        // Log with the disambiguating context (the metric alone cannot tell a
        // spoofer from a mis-set local `identity.region`): peer, both regions,
        // and the observed latency vs ceiling.
        debug!(
            peer = %pk,
            own_region = deps.config.own_region.as_deref().unwrap_or(""),
            claimed_region = %region,
            rtt_ms = rtt,
            ceiling_ms = REGION_LATENCY_MAX_MS,
            "node-origin: same-region claim contradicted by probe latency; \
             applying ADR 030 latency penalty"
        );
        deps.metrics.node_region_latency_penalty();
        record_outcome(deps, pk, &Outcome::RegionLatencyMismatch);
    }
    // The holder's reported blob size (`ProbeResponseExt.total_bytes`), read
    // before `coverage` is moved into the candidate. The coverage-union gather
    // (#1506) uses it to know how many discovery blocks the union must span.
    let total_bytes = resp_ext.total_bytes;
    Some((
        Candidate {
            node_id: *peer.as_bytes(),
            rate_per_mb: resp.body.rate_per_mb,
            rtt_ms: rtt,
            reputation: peer_reputation(deps, pk),
            // Drives the geo-diversity tie-break tier (selection.rs) and the
            // latency-vs-claim penalty above.
            region,
            // No on-chain stake lookup is wired yet (#1470 / ADR 019). `0` is a
            // placeholder here, not an observation — which is exactly why tier 2 is
            // a uniform no-op until the lookup lands. When it does, a failed read
            // must be resolved here (retry, or drop the candidate) rather than
            // passed through as `0`; see `Candidate::stake`.
            stake: 0,
            // The fresh, probe-confirmed coverage (#1506) — never the stale DHT
            // hint `discover` drops. The `consistent_with` check above already
            // guarantees this is non-empty whenever `has_blob` is true, which is
            // the only way execution reaches here.
            coverage: resp_ext.coverage,
        },
        total_bytes,
    ))
}

/// Rebuild ranked [`Candidate`]s from a probe-cache hit (ADR 001 §Probe cache).
///
/// `None` means "no usable cached candidate", covering three cases the caller has
/// no reason to distinguish: no entry, an expired entry, and an entry whose every
/// provider is currently suppressed. All three cost a fresh lookup + probe and all
/// three count as `probe_cache_miss` — a hit that saves no network work is not a
/// hit in any sense a dashboard cares about.
///
/// The cache stores the ADR triple plus the probe's unsigned `Coverage`. `reputation`
/// and `region` are rebuilt here, FRESH, and the result re-ranked. That is what ADR
/// 001's "goes straight to selection" means: skip discovery and probing — not skip
/// the selection algorithm. Caching a `Candidate` whole would have been less code and would have
/// frozen `reputation` for the TTL, letting a node that failed three pulls in
/// the meantime keep the rank it earned before them; the fields we decline to
/// cache are the ones that MOVE.
async fn cached_candidates(deps: &NodeOriginDeps, target: DhtHash) -> Option<Vec<Candidate>> {
    let providers = deps.probe_cache.get(&target)?;
    let now_secs = crate::payment_settlement::unix_now();
    let mut candidates = Vec::with_capacity(providers.len());
    for provider in providers {
        // An entry can be stale INSIDE its own TTL, so the same two filters
        // `probe_and_rank` applies are applied here — this is the other
        // chokepoint every candidate passes through. A pull-time refusal recorded
        // a negative for this exact (peer, hash) seconds ago
        // (`classify_pull_failure`), and a wedged provider is out of ranking for ANY hash.
        if deps
            .negative_cache
            .contains_active(&provider.node_id, &target)
            || deps.provider_is_wedged(&provider.node_id, now_secs)
        {
            continue;
        }
        // ADR 001 §Probe cache: "verify the selected node_id is still active in
        // the local registry cache" before opening `cdn/client/v1`. The cold path
        // applies this inside `find_providers` (`filter_active_stakers`); the hit
        // path bypasses `find_providers`, so re-apply it here. `addr_resolver` is
        // NOT this guard — the address binding survives ejection/unbonding (only
        // deregistration clears it), which would otherwise let a node ejected inside the
        // TTL win a paid pull the cold path denies (#1223).
        //
        // NOTE: an origin from a populated `StaticOriginDirectory` (tests) that is
        // not a staker is skipped here and falls through to the cold path, where
        // that unfiltered fallback re-serves it — such directories get correctness,
        // not hit acceleration. Production directories are chain-backed and
        // staker-filtered, so their origins are stakers and this check is complete.
        if !deps.staker_set.is_active(&provider.node_id) {
            continue;
        }
        let Ok(pk) = PublicKey::from_bytes(provider.node_id.as_bytes()) else {
            // Matches `probe_candidate`: a key that does not decode implies
            // upstream state corruption — skip rather than panic.
            continue;
        };
        let region =
            crate::dht::capacity_bond_registry::region_of(&deps.registry_regions, provider.node_id)
                .unwrap_or_default();
        // Deliberately NOT re-running the ADR 030 region-latency penalty here,
        // unlike `probe_candidate`. That penalty reads a probe's `rtt` as EVIDENCE
        // against a self-attested region claim, and we already scored this rtt
        // once — at probe time, when it was evidence. A cache hit performs no
        // probe and produces no new evidence, so re-recording would charge one
        // probe's latency to the peer again on every hit inside the TTL: an EWMA
        // beaten down by a single stale sample replayed as many times as the
        // blob happens to be requested. Popularity is not guilt.
        candidates.push(Candidate {
            node_id: *provider.node_id.as_bytes(),
            rate_per_mb: provider.rate_per_mb,
            rtt_ms: provider.rtt_ms,
            reputation: peer_reputation(deps, pk),
            region,
            // See `probe_candidate` above: `0` is a placeholder, not a lookup.
            stake: 0,
            // The probe's UNSIGNED coverage, stored in the cache entry alongside
            // the ADR 001 triple (#1506). ≤15s fresh (the entry's TTL), so a
            // cache-hit candidate is range-planned against a real holder's
            // blocks rather than reading as covering nothing. This is not the
            // evidence-retention the cache's module doc forbids — coverage is
            // outside the `slash_sig` set and has no author to slash.
            coverage: provider.coverage.clone(),
        });
    }
    if candidates.is_empty() {
        return None;
    }
    Some(rank(candidates))
}

/// What one walk of a ranked candidate list consumed and produced.
///
/// The `attempts` count is the load-bearing half (#1165). [`MAX_PROVIDER_ATTEMPTS`]
/// is a budget for the whole FETCH, not for each list, and a fetch that hits the
/// probe cache walks two lists: the cached providers, then — if they all fail — a
/// freshly discovered one. Handing each list its own [`MAX_PROVIDER_ATTEMPTS`]
/// would double the worst case to six sequential pulls, silently blowing through
/// [`crate::selection::outer_pull_deadline`], which budgets for exactly three
/// (`(open + pull + stall) × MAX_PROVIDER_ATTEMPTS + slack`, 172s at defaults).
/// The deadline is enforced OUTSIDE this path, so nothing would fail loudly: the
/// fetch would just be killed mid-pull by a timeout sized for a world it no
/// longer lived in — the #859 starvation that formula exists to prevent.
///
/// So the caller carries a remaining-attempts budget across both phases and this
/// reports what was spent.
///
/// Generic over the payload so both walkers share it: [`try_pull`] returns
/// `PullOutcome<Bytes>` (the buffered blob) and the pull leg's candidate walk
/// returns the opened leg. A plain type parameter — no future is generic here,
/// only the value a successful walk hands back.
struct PullOutcome<T> {
    /// The delivered payload, or why the walk produced none.
    payload: Result<T, PullMiss>,
    /// Candidates actually TRIED — i.e. per-candidate attempts, whether or
    /// not they delivered. Never exceeds the `budget` passed in.
    attempts: usize,
}

/// Why an attempt — one candidate, or a whole walk of them — produced no payload
/// (#1560).
///
/// The only distinction the callers need is whether the failure was OURS, because
/// that is what decides the answer this node signs to its own client. Collapsing
/// every non-hit onto a clean `NotFound` — no providers, all refused, all stalled,
/// and a LOCAL fault alike — makes a false statement about the content in the last
/// case: the blob may well exist and be perfectly reachable; it is
/// this node that cannot sign a voucher for it — and `StreamError::InternalError`
/// ("unexpected failure; do not retry this node") exists precisely to keep a
/// broken node from laundering its own defect into a signed claim about content.
///
/// Deliberately NOT a per-verdict taxonomy. The crate-private `PullVerdict`
/// already carries the full one, and it answers a different question ("what does
/// this failure say about the PEER?"); this answers "what may we tell the
/// client?". Only its `OurLocalFault` arm changes that answer: a wedged lane to
/// ONE provider is not node-wide degradation, and it already has its
/// own remedy (keep the row, suppress the provider).
///
/// `#[must_use]`: a classifier's verdict dropped rather than propagated silently
/// reintroduces the #1560 mis-attribution at that call site — this node's own
/// fault signed to a client as an absent blob. Dropping a `PullVerdict` is
/// legitimate at the two
/// mid-stream sites (the answer is already on the wire) and they say so with `let _ =`,
/// which satisfies this attribute; dropping a `PullMiss` never is.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PullMiss {
    /// Nothing about the failure blames this node — the honest empty answer, and
    /// the one a wire `NotFound` is true of.
    Clean,
    /// The attempt failed on a fault in THIS node: a broken signer, a bad
    /// encode/range, an unusable deadline config, or an upstream that could not
    /// VERIFY a signature we produced. Says nothing about whether the content
    /// exists.
    LocalFault,
    /// Candidates existed but every one quoted above the serve-economics buy
    /// ceiling; the node declined an unprofitable relay. Wire-identical to
    /// [`Self::Clean`].
    BelowMargin,
}

impl PullMiss {
    /// Which miss a classified failure is.
    ///
    /// Exhaustive on purpose, like [`classify_refusal`] and [`voucher_verdict`]: a
    /// new [`PullVerdict`] must DECIDE whether it is honest enough to sign a
    /// `NotFound` over, rather than inheriting [`Self::Clean`] by falling into a
    /// catch-all — which is exactly how the local-fault case went unnoticed.
    // `match_same_arms`: the `Refused(OurFault)` arm reaches `Clean` like the block above it,
    // but for an unrelated reason — node-wide-but-not-broken, rather than not-about-this-node.
    // Merging them would erase the only rationale that makes a future variant's arm
    // decidable, which is the same trade `classify_refusal` makes for the same reason.
    #[allow(clippy::match_same_arms)]
    const fn for_verdict(verdict: PullVerdict) -> Self {
        match verdict {
            PullVerdict::OurLocalFault => Self::LocalFault,
            // Every other verdict is either about the peer (`Refused` of the first three
            // kinds, `Stalled`, `RateLimited`, `Corruption`, `Unreachable`), about OUR
            // configuration of what we will accept from it (`Oversize`, `RateCeiling`,
            // `OurDeadline`), or about one lane to one provider (the two voucher arms).
            // None of them is evidence that THIS node is broken for every client and
            // every blob, so none earns an `InternalError`: a node with one wedged lane
            // is still a healthy node that simply cannot serve this blob right now.
            PullVerdict::Oversize
            | PullVerdict::RateCeiling
            | PullVerdict::OurDeadline
            | PullVerdict::Stalled
            | PullVerdict::RateLimited
            | PullVerdict::OurDeadLane(_)
            | PullVerdict::OurVoucherRetryable(_)
            | PullVerdict::Refused(
                RefusalVerdict::NodeFault
                | RefusalVerdict::DurableMiss(_)
                | RefusalVerdict::Transient,
            )
            | PullVerdict::Corruption
            | PullVerdict::Unreachable => Self::Clean,
            // Spelled out rather than folded into `Refused(_)` above, because it is the
            // one refusal `classify_refusal` calls "everything about us": our operator
            // address is on the ADR 011 blacklist, so every peer refuses identically and
            // the condition is node-wide, not per-provider.
            //
            // Reachable ONLY for `OriginBlacklisted`, though `classify_refusal` maps two
            // variants here: `pull_verdict` unwraps a `VoucherRejected` to `voucher_verdict`
            // first, and its own comment calls that a defensive backstop. If that unwrap
            // ever stops happening, the reasoning below does not transfer — a rejected
            // voucher is a statement about one channel, not about a governance list.
            //
            // It stays `Clean` anyway, on two counts. `InternalError` means "UNEXPECTED
            // failure" — a governance blacklist is a deterministic policy state, not a
            // defect. And "do not retry this node" is the wrong advice: a blacklisted node
            // still serves everything already in its cache perfectly well, so steering
            // clients off it wholesale costs them the hits it CAN serve. `NotFound` — "not
            // here, try elsewhere" — is both true of this blob and the better instruction.
            PullVerdict::Refused(RefusalVerdict::OurFault) => Self::Clean,
        }
    }

    /// Fold another attempt's miss into this one: a local fault LATCHES, and an
    /// economics refusal outranks a clean miss.
    ///
    /// A walk reports a local fault if ANY of its attempts hit one, mirroring the
    /// serve path's `fault_seen` latch — a later candidate's clean miss must not
    /// overwrite an earlier fault of ours, or the walk reports a degraded node as a
    /// merely-empty one. The fold runs on every failure, but its result only ever
    /// DECIDES anything when nothing delivered: a candidate that faults locally
    /// followed by one that succeeds is a plain success, and the walk returns the
    /// payload without consulting the latch at all.
    ///
    /// Below a local fault, a [`Self::BelowMargin`] outranks a [`Self::Clean`] the
    /// same way, for the same reason at one rung down: it preserves the "an
    /// economics refusal happened somewhere in this walk" signal so the buy loop
    /// can tell a below-margin miss apart from a genuinely empty one, rather than
    /// letting a later candidate's honest miss erase the fact that an earlier one
    /// quoted above this node's buy ceiling.
    const fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::LocalFault, _) | (_, Self::LocalFault) => Self::LocalFault,
            (Self::BelowMargin, _) | (_, Self::BelowMargin) => Self::BelowMargin,
            (Self::Clean, Self::Clean) => Self::Clean,
        }
    }

    /// Whether this miss is a fault in this node.
    pub const fn is_local_fault(self) -> bool {
        matches!(self, Self::LocalFault)
    }
}

/// The ADR 041 buy-side economic decision for one ranked candidate.
enum EconGate {
    /// The candidate's quote is above this node's buy ceiling: skip it. The walk
    /// records the skip as a [`PullMiss::BelowMargin`], so an exhausted walk whose
    /// only failures were economic answers `NotFound` — distinct from a clean
    /// no-provider miss for observability, identical on the wire.
    Skip,
    /// The candidate is buyable. `rate_ceiling` is the effective per-MB ceiling to
    /// enforce on the stream (the lower of the candidate's probe rate, the static
    /// `max_rate_per_mb`, and — under the `margin` policy — the economic buy cap, so
    /// a bait-and-switch above the cap aborts mid-pull). `speculative` is true when
    /// the quote sits above the amortized profit-guaranteed floor: a successful pull
    /// then debits the source's warming allowance by the full buy cost.
    Allow {
        rate_ceiling: u64,
        speculative: bool,
    },
}

/// The ADR 041 buy ceiling for one candidate: derive this node's sell rate and the
/// policy's two-regime buy cap, fold the cap into the effective stream ceiling, and
/// decide whether to buy at all.
///
/// `source` is the candidate's node id — the warming-allowance key. `heat` is the
/// ADR 040 frequency estimate for the hash, computed once per miss. `candidate_rate`
/// is the probe-advertised per-MB rate.
///
/// With `OffPolicy` (no economic gate) the only bound is the static
/// `max_rate_per_mb`, and no allowance is touched (never speculative). Under
/// `MarginPolicy` the cap is `max(sell, amortized)` while the source has warming
/// allowance, else the amortized floor; a quote above the cap is refused, and a
/// quote merely above the amortized floor is allowed but flagged speculative.
fn economic_ceiling(
    deps: &NodeOriginDeps,
    source: crate::warming_allowance::SourceId,
    heat: u32,
    candidate_rate: u64,
) -> EconGate {
    let (sell, _floor) = deps
        .config
        .sell_rate_bounds
        .raise_to_floor(deps.config.sell_rate_base);
    let operator_bps = deps.config.operator_shares.bps();
    let warm = deps.config.warming.available(source);
    let mk = |warming_available| crate::serve_economics::ServeEconomicsCtx {
        sell_rate_per_mb: sell,
        operator_bps,
        heat_estimate: heat,
        warming_available,
    };
    // `ceiling` is the actual buy cap under the live warming regime; `amortized_floor`
    // is the profit-guaranteed price (the spent-allowance regime). A buy above the
    // floor is speculative and debits the source's allowance on success.
    let ceiling = deps.config.serve_economics.max_buy_per_mb(&mk(warm));
    let amortized_floor = deps.config.serve_economics.max_buy_per_mb(&mk(false));
    match ceiling {
        // No economic gate: only the static ceiling applies, no allowance interaction.
        None => EconGate::Allow {
            rate_ceiling: effective_rate_ceiling(candidate_rate, deps.config.max_rate_per_mb),
            speculative: false,
        },
        Some(max_buy) if candidate_rate > max_buy => {
            deps.metrics.serve_economics_refused();
            EconGate::Skip
        }
        Some(max_buy) => {
            // Fold `max_buy` into the pull-commit ceiling so a bait-and-switch above
            // the cap aborts mid-pull, then take the lower of that and the quote.
            let bound = effective_rate_ceiling(deps.config.max_rate_per_mb, max_buy);
            let speculative = candidate_rate > amortized_floor.unwrap_or(candidate_rate);
            EconGate::Allow {
                rate_ceiling: effective_rate_ceiling(candidate_rate, bound),
                speculative,
            }
        }
    }
}

/// Bytes to whole MB (round up), the unit the warming allowance accounts in.
const fn mb_of(bytes: u64) -> u64 {
    bytes.div_ceil(decdn_protocol::MB_BYTES)
}

/// The ADR 040 frequency estimate for `hash_bytes` — the `margin` policy's heat
/// input. `0` (cold) when no estimator is wired.
fn heat_of(deps: &NodeOriginDeps, hash_bytes: [u8; 32]) -> u32 {
    deps.config
        .frequency_estimator
        .as_ref()
        .map_or(0, |e| e.estimate(Hash::from(hash_bytes)))
}

/// Walk the ranked candidates (best-first), opening a channel and pulling from
/// each until one delivers, bounded by `budget` remaining attempts. Records a
/// reputation outcome for every candidate that reaches the wire;
/// candidates skipped earlier for an unresolvable operator address or a local
/// channel-open failure are intentionally not scored (neither is the provider's
/// fault). `budget` is the fetch-wide [`MAX_PROVIDER_ATTEMPTS`] remainder rather
/// than the constant itself — see [`PullOutcome`].
async fn try_pull(
    deps_lock: &Arc<OnceLock<NodeOriginDeps>>,
    deps: &NodeOriginDeps,
    ranked: &[Candidate],
    hash_bytes: [u8; 32],
    budget: usize,
) -> PullOutcome<()> {
    let mut attempts = 0;
    let mut miss = PullMiss::Clean;
    for candidate in ranked.iter().take(budget) {
        attempts += 1;
        match pull_from_candidate(deps_lock, deps, candidate, hash_bytes).await {
            Ok(()) => {
                return PullOutcome {
                    payload: Ok(()),
                    attempts,
                };
            }
            Err(failed) => miss = miss.or(failed),
        }
    }
    PullOutcome {
        payload: Err(miss),
        attempts,
    }
}

/// Attach this node's ADR 005 client identity binding to an upstream pull's
/// `PoolContext` (#1117). Signs over our OWN endpoint `NodeId` with the
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
/// in the paid-pull requester's voucher leg (`UpstreamPull::pay_one`). Swallowed here, `node_pull_local_fault` stayed at zero in
/// precisely the emergency its doc describes ("a node that cannot sign a voucher cannot
/// pay for anything"), and an operator alerting on it got a false all-clear.
fn bind_upstream_ctx(deps: &NodeOriginDeps, ctx: PoolContext) -> anyhow::Result<PoolContext> {
    let own_node_id = B256::from(*deps.endpoint.id().as_bytes());
    let binding = sign_client_binding(&ctx.client_signer, own_node_id, &deps.bind_domain)?;
    Ok(ctx.with_client_binding(binding))
}

/// The voucher ledger this pull must issue through: the LANE's, shared with every other
/// concurrent pull on it — never a fresh one per pull.
///
/// One call site per pull path, so neither can quietly go back to minting its own. Both did,
/// and the caller-owned-ledger entrypoints exist precisely
/// because that is broken: N concurrent pulls each seeded from `ctx.prior_*` all sign the
/// same next cumulative watermark and collide, the upstream accepts one and rejects the rest
/// as a regression. `ctx.prior_*` is only a SEED — it loses to a live ledger, which is at
/// least as far along as the row it was read from. See [`BuyerLedgers`].
fn lane_ledger(
    deps: &NodeOriginDeps,
    provider_addr: Address,
    ctx: &PoolContext,
) -> Arc<PoolLedger> {
    deps.ledgers.get_or_seed(
        decdn_incentive::LaneKey {
            pool_id: ctx.pool_id,
            signer: ctx.client_signer.address(),
            provider: provider_addr,
        },
        Cumulative {
            bytes: ctx.prior_bytes_delivered,
            amount: ctx.prior_amount,
        },
    )
}

/// Attempt a single paid pull from one candidate: resolve its operator address,
/// open/reuse a buyer channel, and stream the whole blob into `deps.engine` via
/// the gap-driven [`drive`] loop, recording the reputation outcome. On success
/// the blob is admitted and durable in the store — no whole-blob buffer is ever
/// held in RAM (#1682). Returns the [`PullMiss`] this failure is on a miss (try
/// the next candidate either way — a local fault latches, it does not abort the
/// walk, #1560).
// Sequential resolve → open → fetch → classify pipeline; the tracing macros and
// the success/failure classification inflate the cognitive-complexity + line
// metrics past threshold (same inflation noted in `chain_staker_set`). Splitting
// it would scatter a single linear flow across helpers.
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
async fn pull_from_candidate(
    deps_lock: &Arc<OnceLock<NodeOriginDeps>>,
    deps: &NodeOriginDeps,
    candidate: &Candidate,
    hash_bytes: [u8; 32],
) -> Result<(), PullMiss> {
    let Ok(pk) = PublicKey::from_bytes(&candidate.node_id) else {
        return Err(PullMiss::Clean);
    };
    let Some(provider_addr) = deps
        .addr_resolver
        .address_of(&DhtNodeId::from_bytes(candidate.node_id))
    else {
        // No bonded address → we cannot safely open a channel to, or verify the
        // `slash_sig` of, this provider. Skip rather than guess.
        debug!("node-origin: candidate has no resolvable operator address; skipping");
        return Err(PullMiss::Clean);
    };
    // ADR 041 buy-side gate: refuse a candidate quoting above this node's buy ceiling
    // BEFORE opening a channel. A skip folds into the walk as `BelowMargin`.
    let heat = heat_of(deps, hash_bytes);
    let (rate_ceiling, speculative) =
        match economic_ceiling(deps, candidate.node_id.into(), heat, candidate.rate_per_mb) {
            EconGate::Allow {
                rate_ceiling,
                speculative,
            } => (rate_ceiling, speculative),
            EconGate::Skip => return Err(PullMiss::BelowMargin),
        };
    let ctx = match deps
        .buyer
        // Same channel-open bound as the window path (#1143) — see there.
        .open_or_reuse_pool(provider_addr, crate::selection::CHANNEL_OPEN_CALLER_BUDGET)
        .await
    {
        Ok(ctx) => ctx,
        Err(err) => {
            // A channel-open failure is OUR payment-side problem, not the
            // provider's fault — don't tar its reputation; just try the next.
            // Whether it is also a fault that must change our WIRE answer is a
            // per-reason question the classifier owns (#1560).
            return Err(record_pool_open_failure(deps, provider_addr, &err));
        }
    };
    // #1117: bind the request so the upstream can chain a reactive pull.
    let ctx = match bind_upstream_ctx(deps, ctx) {
        Ok(ctx) => ctx,
        Err(err) => {
            // As on the window path: our signing fault, metered as ours, peer unscored.
            // No channel: the bind failed before we could present a voucher on one.
            let verdict = classify_pull_failure(deps, pk, provider_addr, hash_bytes, None, &err);
            return Err(PullMiss::for_verdict(verdict));
        }
    };
    let started = Instant::now();
    // The ledger is CALLER-owned, and the watermark is settled from it by a `Drop`
    // guard ([`SettleOnDrop`]) rather than by a copy-back after the await (#1145
    // review). Both halves of that matter.
    //
    // A `&mut VoucherProgress` out-param can only be copied back on a RETURN, and
    // this pull's defining property is that it need not return: it runs
    // with `hard_cap: None`, so nothing inside it ends a slow-but-progressing
    // transfer, and everything that does end one is external and DROPS the future —
    // the foreground `outer_pull_deadline`, or the serve future being dropped (client
    // disconnect, node shutdown). On every one of those paths the copy-back would never run
    // and the acked watermark would die with the frame, while the USDC it recorded
    // has already left the node. The next pull would then re-sign a cumulative
    // watermark the upstream has already advanced past, the upstream would reject it
    // as a regression, and the lane would wedge.
    //
    // The settle guard lives on the PULL THREAD, not here (see the `spawn_blocking`
    // block below): `drive` advances the shared lane `PoolLedger` from that thread,
    // and a cooperative cancel drops the `drive` future there, so the guard must
    // persist the FINAL watermark AFTER the drive has fully stopped advancing it.
    // Settling from this outer future instead would race the still-running thread and
    // persist a STALE cumulative, wedging the lane exactly as the copy-back did. So
    // nothing pre-`drive` on this outer future settles: the channel-open, bind, and
    // free header handshake below issue no voucher.
    // As on the window path: a zero budget is our own misconfiguration, metered as ours.
    // Checked BEFORE the ledger, so a pull that cannot legally run never reaches the
    // wire and has nothing to settle.
    let deadlines = match deps.config.deadlines() {
        Ok(deadlines) => deadlines,
        Err(err) => {
            let verdict = classify_pull_failure(deps, pk, provider_addr, hash_bytes, None, &err);
            return Err(PullMiss::for_verdict(verdict));
        }
    };
    let ledger = lane_ledger(deps, provider_addr, &ctx);

    // Streaming is bounded by INACTIVITY, with no overall wall-clock cap (#1134).
    //
    // An overall `pull_timeout` around this whole fetch would quietly cap the blob
    // size a node can pull through at roughly `pull_timeout × link speed` — at
    // the 20 s default, any blob needing more than ~20 s of transfer would be
    // unfetchable on this path. The stall bound catches the thing a deadline
    // should catch (an upstream that stops delivering) without penalising size or
    // link speed.
    //
    // The FOREGROUND serve path is still bounded — the delivery handler wraps the
    // whole `discover → probe → rank → pull` in `outer_pull_deadline`, so a client
    // never waits longer than that; on expiry it gets a clean miss and nothing
    // continues (#1610 removed the detached warm). So "no hard cap here" does not mean
    // "a client can wait forever".
    let _stream_guard = deps.metrics.outbound_stream_guard();

    // Header handshake: a free whole-tail open to read the committed `total_bytes`,
    // then abort — no bytes pulled, no voucher. `NO_NAMESPACE`, as this hash-only
    // populate path carries no served-client namespace. `rate_ceiling` folds the ADR
    // 041 buy cap in (computed above with the skip decision).
    let (header, probe) = match open_progressive_upstream(
        &deps.endpoint,
        EndpointAddr::new(pk),
        &ctx,
        Arc::clone(&ledger),
        &deps.slash_domain,
        provider_addr,
        hash_bytes,
        NO_NAMESPACE,
        0,
        now_micros(),
        deps.config.max_blob_size_bytes,
        rate_ceiling,
        deadlines,
        0,
        // Outer runtime: nothing to strand, so no dial observer.
        None,
    )
    .await
    {
        Ok(pair) => pair,
        Err(err) => {
            // No bytes pulled, no voucher paid — nothing to settle.
            let verdict =
                classify_pull_failure(deps, pk, provider_addr, hash_bytes, Some(ctx.pool_id), &err);
            return Err(PullMiss::for_verdict(verdict));
        }
    };
    let total_bytes = header.total_bytes;
    let _ = probe.abort();

    // Run `drive()` on a dedicated blocking-pool thread with its own current-thread
    // runtime, exactly as the window-paced serve-miss pull leg does
    // (`node_origin::pull_leg::run_pull_leg`). `drive`'s future is non-`Send`
    // categorically — `IngestStore::ingest_stream` is a
    // return-position-impl-trait-in-trait with no `Send` bound, for any
    // `R: BaoRangeReader` (see `decdn_client_pull::source::IngestStore`'s docs) — which
    // `Origin::fetch`'s `+ Send` trait bound forbids inline. Every axis is therefore
    // captured OWNED: no `FillSession` (no serve leg reads beside a populate) and no
    // window/leech pacer (this tier has no downstream paid frontier to pace against, so
    // it pulls the whole blob under a plain [`BudgetPacer`]).
    //
    // Cancellation + settle + drain mirror `run_pull_leg`, and each half is
    // load-bearing:
    //
    // - CANCELLATION. `spawn_blocking` tasks are never aborted when their `JoinHandle`
    //   is dropped, so a bare thread would keep pulling and PAYING vouchers for a blob
    //   nobody awaits once the `fetch` future is dropped (`outer_pull_deadline` expiry,
    //   client disconnect, node shutdown — #1610). The outer future holds a
    //   `cancel.drop_guard()`, so dropping it cancels the token; the pull thread runs
    //   `drive` under a `select!` against `cancel.cancelled()` and stops.
    // - ON-THREAD SETTLE. `drive` advances the shared lane `PoolLedger` from the pull
    //   thread, so the [`SettleOnDrop`] guard lives THERE, holding the shared deps
    //   `Arc`, and persists the FINAL watermark (#852) only after `drive` has fully
    //   stopped advancing it. Settling from the outer future would race the still-running
    //   thread and persist a STALE cumulative, wedging the lane.
    // - ABANDON DRAIN. A cancelled or errored `drive` returns without a graceful
    //   cooperative close, stranding the upstream iroh connection whose QUIC driver
    //   lives on this pull-thread runtime; dropping the runtime with no drain hangs the
    //   node's `Endpoint::close()`. Wait on those paths for the connection to actually
    //   reach drained (see the `abandon_drain` module), under its own ceiling. The
    //   clean `Ok` path skips the wait and carries the same residual as #1675.
    let hash = Hash::from(hash_bytes);
    let endpoint = deps.endpoint.clone();
    let slash_domain = deps.slash_domain.clone();
    let engine = deps.engine.clone();
    let buyer = Arc::clone(&deps.buyer);
    let metrics = Arc::clone(&deps.metrics);
    let ledger_for_drive = Arc::clone(&ledger);
    let deps_for_thread = Arc::clone(deps_lock);
    let max_blob_size_bytes = deps.config.max_blob_size_bytes;
    let drive_config = DriveConfig {
        working_deposit: deps.config.working_deposit,
        seller_reserve: deps.config.seller_reserve,
        max_settle_waits: settle_wait_budget(deps.config.event_poll_interval),
        settle_backoff: SETTLE_POLL_STEP,
    };
    // The outer `fetch`-side future owns the drop guard: dropping this future (deadline
    // expiry / disconnect / shutdown) cancels the token, which the pull thread selects
    // on to stop the drive.
    let cancel = CancellationToken::new();
    let cancel_for_thread = cancel.clone();
    let _cancel_guard = cancel.drop_guard();
    // Set on the drive thread the first time a reactive top-up escrows any headroom, so
    // the refuse-metering below can tell a pull that never funded itself (an extortion
    // `SpendingCapExhausted` to meter) from one that did (already metered by `NodeFunder`,
    // as a success or as a short landing).
    let reactive_funded = Arc::new(AtomicBool::new(false));
    let reactive_funded_for_thread = Arc::clone(&reactive_funded);
    let join = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok::<_, std::io::Error>(rt.block_on(async move {
            let store = NodeAdmitStore::new(engine, hash, total_bytes, None);
            // Capture the lane seed before `ctx` moves behind the mutex, so the
            // on-thread settle can tell whether this stream advanced the watermark.
            let pool_id = ctx.pool_id;
            let prior_amount = ctx.prior_amount;
            let ctx = Arc::new(std::sync::Mutex::new(ctx));
            // #852: persist the buyer watermark on EVERY exit — clean completion, a
            // terminal drive error, or a cooperative cancel — from THIS pull thread,
            // AFTER the `drive` below has fully stopped advancing the shared ledger.
            let _settle = SettleOnDrop {
                deps: Arc::clone(&deps_for_thread),
                provider_addr,
                pool_id,
                prior_amount,
                ledger: Arc::clone(&ledger_for_drive),
            };
            let abandoned = ConnDrain::default();
            let observer = abandoned.observer();
            let source = PeerSource::new(
                &endpoint,
                EndpointAddr::new(pk),
                Arc::clone(&ctx),
                Arc::clone(&ledger_for_drive),
                &slash_domain,
                provider_addr,
                NO_NAMESPACE,
                max_blob_size_bytes,
                rate_ceiling,
                deadlines,
            )
            .with_dial_observer(as_observer(&observer));
            let pacer = BudgetPacer::new();
            let funder = NodeFunder::new(
                buyer,
                Arc::clone(&ctx),
                Arc::clone(&metrics),
                reactive_funded_for_thread,
            );
            // Whole blob: offset 0, len `total_bytes`. `drive` derives missing ranges
            // from the ranged store, so a mid-pull top-up resumes by re-deriving gaps
            // — no truncate, no rewind buffer. `drive` finalizes the store when the
            // whole blob is present, so the caller holds no whole-blob buffer of its
            // own on success.
            let cancelled;
            let result = tokio::select! {
                biased;
                r = drive(
                    &store,
                    &source,
                    &pacer,
                    &funder,
                    &ctx,
                    &ledger_for_drive,
                    hash_bytes,
                    0,
                    total_bytes,
                    &drive_config,
                    None,
                    None,
                    None,
                    // Single-source candidate pull: one lane is the whole pool.
                    None,
                ) => {
                    cancelled = false;
                    r
                }
                () = cancel_for_thread.cancelled() => {
                    cancelled = true;
                    Ok(())
                }
            };
            // Wait out a stranded upstream connection on the cancel/`Err` paths only,
            // so `Endpoint::close()` cannot hang on the runtime this thread is about
            // to drop. The `_settle` guard drops AFTER this, persisting the final
            // watermark.
            if cancelled || result.is_err() {
                drain_abandoned(&abandoned, provider_addr, &metrics).await;
            }
            (result, pool_id, cancelled)
        }))
    })
    .await;
    let elapsed = started.elapsed();
    // A genuine cooperative cancel (the `fetch` future was dropped) is neither a clean
    // delivery nor a fault: do not score or classify it. The two THREAD-LEVEL failure
    // shapes — the blocking task panicked (`JoinError`) or its dedicated runtime failed
    // to build (`io::Error`, resource exhaustion) — say nothing about the PROVIDER, but
    // they are OUR fault and must not launder into a clean miss (#1560), so they map to
    // [`PullMiss::LocalFault`] directly rather than falling through the transport-fault
    // classifier onto `PullMiss::Clean`.
    let (result, pool_id, cancelled) = match join {
        Ok(Ok(triple)) => triple,
        Ok(Err(err)) => {
            warn!(%err, "node-origin: pull-thread runtime build failed; local fault");
            return Err(PullMiss::LocalFault);
        }
        Err(join_err) => {
            warn!(%join_err, "node-origin: pull thread panicked; local fault");
            return Err(PullMiss::LocalFault);
        }
    };
    if cancelled {
        // The pull was abandoned mid-transfer; the on-thread settle already persisted
        // the watermark. Nothing to score.
        return Err(PullMiss::Clean);
    }
    match result {
        Ok(()) => {
            // Content delivered by this provider for a from-zero whole-blob pull is
            // `total_bytes`. `paid_wait` is not subtracted here, matching the
            // gap-driven `run_pull_leg` path (settle waits are rare and the driver
            // bounds them).
            // ADR 041: a clean speculative pull debits the source's warming allowance
            // by the full buy cost (whole MB at the candidate's buy rate).
            if speculative {
                deps.config.warming.debit_speculative(
                    candidate.node_id.into(),
                    Hash::from_bytes(hash_bytes),
                    candidate.rate_per_mb.saturating_mul(mb_of(total_bytes)),
                );
            }
            record_outcome(
                deps,
                pk,
                &Outcome::Delivered {
                    bytes: total_bytes,
                    elapsed,
                },
            );
            Ok(())
        }
        Err(err) => {
            // Meter a refused reactive top-up (#1600): the upstream ended the pull with
            // `SpendingCapExhausted` while OUR ledger still had headroom — an attempt to make us
            // escrow more USDC on its unsupported word. The driver's `genuine_exhaustion`
            // saw the contradiction and never issued a `TopUp`, so `NodeFunder` was never
            // called and nothing else meters this; without it, a lying peer is invisible.
            // Guarded on `working_deposit != 0` (reactive top-up enabled) and on this pull
            // NOT having funded itself — a pull that escrowed any headroom was already
            // metered by `NodeFunder` (`node_pull_reactive_topup`, or
            // `node_pull_reactive_topup_refused` for a short landing) and is not being
            // extorted. A pull whose every top-up failed or added nothing stays unfunded;
            // its refusal here counts the upstream's claim, a separate event from the
            // funding outcome `NodeFunder` metered. No escrow, no bytes: the fetch still
            // misses.
            if !deps.config.working_deposit.is_zero()
                && !reactive_funded.load(Ordering::Relaxed)
                && err
                    .downcast_ref::<UpstreamVoucherRejected>()
                    .is_some_and(|r| r.reason == VoucherRejectReason::SpendingCapExhausted)
            {
                deps.metrics.node_pull_reactive_topup_refused();
            }
            let verdict =
                classify_pull_failure(deps, pk, provider_addr, hash_bytes, Some(pool_id), &err);
            Err(PullMiss::for_verdict(verdict))
        }
    }
}

/// Settles what a pull paid — on EVERY way out of it, including a drop.
///
/// This is a `Drop` guard rather than a pair of calls after the await because the
/// pull can end without returning, and on that path the money is already gone
/// (#1145 review). See the comment at its construction in [`pull_from_candidate`]
/// for which cancellations are reachable and why each one is by design.
///
/// It settles the voucher watermark, so the next reuse of this channel signs the
/// nonce the upstream actually committed to (#852) — just as real on a cancelled
/// pull as on a returned one, since the bytes were bought either way.
///
/// It deliberately does NOT record a reputation outcome. Reputation is a judgement
/// about the peer and needs the pull's result to make it; a drop has no result, and
/// a cancelled transfer is our decision, not the provider's misconduct.
///
/// `Debug` is hand-written: `NodeOriginDeps` is not `Debug`-derivable (it is a bag of trait
/// objects), and the guard is a field of `Debug`-deriving pull state.
///
/// BOTH pull paths settle through this and only this. Settling in terminal
/// methods instead would lose the watermark: the serve loop drives a pull as a
/// future on the iroh `accept` task, and a node shutdown or a downstream reset
/// DROPS it, so no terminal method runs (#1145 review).
struct SettleOnDrop {
    /// The runtime deps, reached through the shared `OnceLock`. Holding the `Arc`
    /// (not a borrow) lets the guard cross onto a pull thread and outlive the future
    /// that spawned it — both pull paths drop it off their original stack.
    /// `get()` is `None` only if the node was never provisioned, in which case there
    /// is no store to persist to and nothing to settle.
    deps: Arc<OnceLock<NodeOriginDeps>>,
    provider_addr: Address,
    pool_id: B256,
    /// The lane's cumulative amount the pull started from, so a `Drop` can tell
    /// whether this stream advanced the watermark past its seed before persisting.
    prior_amount: U256,
    ledger: Arc<PoolLedger>,
}

impl std::fmt::Debug for SettleOnDrop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettleOnDrop")
            .field("provider_addr", &self.provider_addr)
            .field("pool_id", &self.pool_id)
            .finish_non_exhaustive()
    }
}

impl Drop for SettleOnDrop {
    fn drop(&mut self) {
        let Some(deps) = self.deps.get() else {
            return;
        };
        // `settlement`, not `snapshot`: a `Drop` cannot await. And not `committed`
        // either — that is the watermark of what the upstream ACKED, and the drop we are
        // handling can land inside the ack wait, where a voucher is committed upstream
        // and unacked here (ADR 003 persists before it acks). `settlement` adds back the
        // voucher still on the wire, which is what we actually owe (#1122). Settling at
        // `committed` there re-signs a spent cumulative on the next reuse, which the
        // upstream rejects as a regression — stranding lane progress.
        let progress = VoucherProgress::from_ledger(&self.ledger, self.prior_amount);
        persist_buyer_progress(deps, self.provider_addr, self.pool_id, &progress);
    }
}

/// Persist whatever the upstream acked, regardless of Ok/Err (#852): a
/// mid-stream failure or a paid-but-corrupt (hash-mismatch) delivery can still
/// have advanced the upstream's accepted-voucher watermark. Skipping this lets
/// the channel re-sign a stale voucher on its next reuse and be rejected. The
/// bytes are already paid for, so a persist failure must not fail the pull;
/// surface it loudly instead (it breaks the next reuse). Both the buffered
/// [`pull_from_candidate`] and the window-paced pull leg reach it via
/// [`SettleOnDrop`].
fn persist_buyer_progress(
    deps: &NodeOriginDeps,
    provider_addr: Address,
    pool_id: B256,
    progress: &VoucherProgress,
) {
    if let Some((bytes_delivered, amount)) = progress.advanced()
        && let Err(err) =
            deps.buyer
                .record_progress(provider_addr, pool_id, bytes_delivered, amount)
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
    DurableMiss(DurableMissCause),
    /// Transient, or not attributable to the peer at all. Suppress the pair only briefly
    /// ([`REFUSAL_SUPPRESSION_TTL`]) — long enough that a peer serving nothing stops
    /// burning a candidate slot on every miss in a retry burst, short enough that we do
    /// not blackhole a healthy peer over a condition that has already passed.
    Transient,
    /// OUR fault. Score nothing, suppress nothing.
    OurFault,
}

/// Why a [`RefusalVerdict::DurableMiss`] is durable.
///
/// The verdict itself answers "what does this refusal say about the peer?", and
/// both causes give the same answer: asking this peer for this hash again inside
/// the TTL gets the same reply, so suppress it and don't spend a candidate slot
/// finding out. They are NOT the same thing to an operator, though —
/// `EvictedSinceProbe` is a peer contradicting its own signed `has_blob: true`
/// and is the ADR 001-mandated `decdn_probe_post_eviction_failures_total`
/// signal, while `BlobTooLarge` is a static fact about the blob that says
/// nothing about anyone's hold mechanism. A payload rather than a fourth
/// `RefusalVerdict` variant, because a variant would claim the two mean
/// different things about the peer, and they do not (#1165).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurableMissCause {
    /// The peer held the blob at probe time and lost it to cache pressure
    /// before we opened the stream — a hold-mechanism failure (ADR 005).
    EvictedSinceProbe,
    /// The blob is over the peer's ceiling — deterministic for this blob.
    BlobTooLarge,
    /// The peer will not serve this hash: it is on the governance blacklist or
    /// on that operator's local denylist (ADR 011). The peer does not say which,
    /// and must not — but either way it is a policy decision, not a cache state,
    /// so it will not change inside the TTL.
    HashBlacklisted,
}

/// How long a (peer, hash) pair is suppressed after a refusal we cannot attribute to the
/// peer, or that we expect to pass on its own ([`RefusalVerdict::Transient`]).
///
/// Much shorter than the negative cache's own TTL (5 min), and the asymmetry is the
/// point. A probe's `has_blob: false` is an authoritative statement about content the
/// peer just checked. A `NotFound` *refusal* is not: `ServeRejectReason::wire_error`
/// deliberately collapses several reject reasons onto the wire `NotFound` so that a probing
/// client cannot map out other clients' remaining pool balances — and some of them are ours
/// or transient: `InsufficientDeposit`, `UnknownChannel` (while the upstream's chain watcher
/// catches up), and `RangeNotSatisfiable` (our own bad range computation). We cannot tell
/// them apart, and we must not: the collapse is a privacy property, not an oversight.
///
/// So the refusal is suppressed on the assumption it may be *us*. At the full TTL, a
/// pool that ran dry for one pull — or the pre-observation window right after we open
/// a pool — blackholed a perfectly healthy upstream for five minutes.
const REFUSAL_SUPPRESSION_TTL: Duration = Duration::from_secs(30);

// `match_same_arms`: `VoucherRejected` and `OriginBlacklisted` both map to
// `OurFault`, and `EvictedSinceProbe`/`BlobTooLarge`/`HashBlacklisted` all map to
// `DurableMiss`, but merging them would erase why each reaches that verdict —
// which is the only thing that makes a future variant's arm decidable. Each arm
// carries its own reasoning; keep them apart.
#[allow(clippy::match_same_arms)]
const fn classify_refusal(error: &StreamError) -> RefusalVerdict {
    match error {
        // The one code by which a node reports its OWN degradation: "unexpected
        // failure; do not retry THIS node" (#1129).
        StreamError::InternalError => RefusalVerdict::NodeFault,
        // Honest and durable: `EvictedSinceProbe` is a race the peer is being truthful
        // about, and `BlobTooLarge` is deterministic for this blob. Asking this peer for
        // this hash again inside the TTL gets the same answer, so don't spend a candidate
        // slot finding out.
        StreamError::EvictedSinceProbe => {
            RefusalVerdict::DurableMiss(DurableMissCause::EvictedSinceProbe)
        }
        StreamError::BlobTooLarge => RefusalVerdict::DurableMiss(DurableMissCause::BlobTooLarge),
        // Honest but NOT durable, and — for `NotFound` — not even attributable: see
        // `REFUSAL_SUPPRESSION_TTL`. `Overloaded` is backpressure, which the code's own
        // policy says to respect rather than punish; suppressing the peer for five
        // minutes over a load spike lasting seconds is punishing it.
        StreamError::NotFound | StreamError::Overloaded => RefusalVerdict::Transient,
        // `VoucherRejected` never reaches here any more: `pull_verdict` unwraps it out of
        // `UpstreamRefused` and routes it to `voucher_verdict`, which is the only place that
        // decides what a rejected voucher costs the channel (#1145 review).
        //
        // The arm stays because the match is exhaustive on purpose — a new `StreamError` must
        // break this build — and because `RefusalVerdict::OurFault` is still the right answer
        // to the question THIS function asks ("what does this refusal say about the peer?"):
        // nothing. It just is not the whole answer, and this function is not the one that can
        // give it. Routing a mid-stream rejection through here alone is what let a wedged
        // channel skip its remedy entirely and be handed back on every subsequent miss.
        StreamError::VoucherRejected { .. } => RefusalVerdict::OurFault,
        // Policy, not cache state, and it will not lapse inside the TTL. Note we
        // cannot tell a governance entry from the peer's own local denylist —
        // the wire code deliberately does not distinguish them (ADR 011
        // §StreamRequest Response) — but both are durable for this pair, which
        // is the only question this function asks.
        StreamError::HashBlacklisted => {
            RefusalVerdict::DurableMiss(DurableMissCause::HashBlacklisted)
        }
        // Says nothing about the peer and everything about us: OUR operator
        // address is blacklisted, so every peer will refuse identically.
        // `OurFault` — scoring the peer would punish it for reporting our own
        // status, and suppressing the pair would waste the entry, since the next
        // peer refuses too. There is no remedy at this layer; lifting the entry
        // is a governance action.
        StreamError::OriginBlacklisted => RefusalVerdict::OurFault,
    }
}

/// What a failed pull was actually caused by — the whole classification decision, as a
/// value.
///
/// Separated from the *acting* on it ([`classify_pull_failure`]) so the decision can be
/// tested without a live `NodeOriginDeps`, which needs an iroh endpoint. That matters more
/// than it sounds. The ladder below is an ORDERED chain of `downcast_ref`s whose order is
/// load-bearing and invisible to the compiler, ending in a catch-all that scores the peer
/// `Unreachable`. Every mis-attribution this module is prone to is an error falling one
/// arm further than it should and landing there: an honest `NotFound` refusal (#1144),
/// a mid-stream `StreamError`, a broken local signer. Making the decision a pure function
/// means "which arm does this error land in?" is a question a test can just ask.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PullVerdict {
    /// The bytes that actually arrived crossed OUR configured ceiling (#1895) — the
    /// blob may be fine for other nodes with a wider cap. The peer's `total_bytes`
    /// claim never triggers this; only received bytes do.
    Oversize,
    /// The provider quoted a per-MB rate above the buyer's effective ceiling — the lower
    /// of its own probe rate and our configured absolute cap (#1375). We refused before
    /// paying; the signed over-quote is retained on the `RateAboveCeiling` error for a
    /// caller to replay to `SlashJudge`, but this node does not auto-submit a challenge
    /// (deferred). Reputation-neutral, and metered + suppressed: an over-*config* quote is
    /// our tight policy, not proof the provider is bad; an over-*probe* quote is a possible
    /// bait-and-switch we do not adjudicate here (and it is only slashable when the probe
    /// bound, not the config bound, was exceeded). Either way the rate is durable for this
    /// (peer, hash) — re-probing gets the same quote — so we suppress the pair (like
    /// [`Self::OurDeadline`]) rather than tar the peer, and count it (like
    /// [`Self::Oversize`], which meters but does not suppress).
    RateCeiling,
    /// OUR deadline fired: a possibly mis-sized local budget, not evidence about the peer.
    OurDeadline,
    /// The peer went SILENT mid-stream (#1134). Unlike [`Self::OurDeadline`] this IS about
    /// the peer: a clock that resets on every byte can only fire on one that stopped.
    Stalled,
    /// The peer shed the connection or stream at the transport with
    /// `APP_ERR_RATE_LIMITED` (ADR 013 §Application Error Codes) before any signed
    /// message existed (#1986). The transport-level twin of
    /// `Refused(Transient)` for `StreamError::Overloaded`, and it gets the same
    /// treatment: backpressure is respected, not punished. Metered and suppressed for
    /// [`REFUSAL_SUPPRESSION_TTL`], never scored — the peer answered, it just declined
    /// the work, and the `global-full` layer is not about this caller at all.
    RateLimited,
    /// The peer rejected a voucher and this lane to this provider is finished, but the
    /// shared pool deposit is NOT: it still backs every other lane and is refundable through
    /// the pool's own grace-window close and reclaim.
    ///
    /// Split from [`Self::OurVoucherRetryable`] because the two want opposite actions, and
    /// collapsing them makes a drained lane invisible. `UpstreamVoucherRejected` carries a
    /// `VoucherRejectReason` whose variants prescribe *different* remedies — resend, top
    /// up, suppress, stop — so discarding the reason with a bare `.is_some()` would collapse
    /// every one of them to "skip this candidate, say nothing".
    ///
    /// The pool row is the node's handle on the deposit, and the recovery sweeps enumerate
    /// the store (`load_all`), so it is KEPT rather than deleted. The provider is SUPPRESSED
    /// for a bounded window instead: the pool has no per-provider deadline to key a horizon
    /// on — the shared deposit fans out across every provider — so a lane-terminal rejection
    /// takes the provider out of ranking for a fixed window while the pool's deposit stays
    /// intact and available to every other lane.
    OurDeadLane(VoucherRejectReason),
    /// The peer rejected a voucher we presented, but the peer is FINE — the fault is in
    /// OUR buyer pool. `PoolExhausted` lands here: the pool WE fund the upstream from can
    /// no longer cover further credit (ADR 003 §Pool solvency). The remedy is a top-up of
    /// our pool and a retry, keeping the healthy peer rather than suppressing it.
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

/// A buyer lane that can no longer pay but whose shared pool DEPOSIT is still escrowed.
/// Keep the pool row, stop using the provider, and say so loudly.
///
/// The pool row is not bookkeeping — it is the node's handle on the money. The pool's
/// grace-window close and reclaim refund the deposit and need the `pool_id`; the recovery
/// sweeps find their work by enumerating the store (`load_all`). A row this function deleted
/// would therefore be a deposit that nothing in this codebase can ever see again.
///
/// So this deliberately does NOT retire the pool. What it does instead:
///
/// - **Suppress the PROVIDER, for all hashes, for a bounded window.** The pool row survives
///   and backs every other lane, so a lane-terminal rejection must not take the pool down —
///   only this provider. Two suppressions cover it: a short per-`(peer, hash)` negative-cache
///   entry ([`REFUSAL_SUPPRESSION_TTL`]) for the immediate same-blob retry, and a
///   provider-wide entry in `wedged_providers` keyed by the peer and held for
///   [`WEDGED_PROVIDER_SUPPRESSION_SECS`], so `probe_and_rank` drops the provider from
///   ranking for every hash until then. The pool has no per-provider deadline to key the
///   horizon on — the shared deposit fans out across every provider — so the window is fixed.
/// - **Score nothing.** The peer behaved correctly; our accounting is what broke.
///
/// The cost is this provider for the suppression window; the pool's deposit is untouched and
/// stays available to every other lane.
fn wedged_channel(
    deps: &NodeOriginDeps,
    pk: PublicKey,
    provider_addr: Address,
    hash_bytes: [u8; 32],
    reason: VoucherRejectReason,
    channel: Option<B256>,
) {
    deps.metrics.node_pull_pool_wedged();
    // Immediate cover: suppress this (peer, hash) for the short refusal TTL so a retry for the
    // SAME blob does not re-present the same voucher before the provider-wide horizon lands.
    deps.negative_cache.record_failure_with_ttl(
        DhtNodeId::from_bytes(*pk.as_bytes()),
        DhtHash::from_bytes(hash_bytes),
        REFUSAL_SUPPRESSION_TTL,
    );
    // Provider-wide: a provider whose voucher this lane cannot pay is one we cannot pay for any
    // hash right now, so take it out of ranking for a bounded window. The horizon itself belongs
    // to `record_wedged` — the pool has no per-provider deadline to key one on (the deposit fans
    // out across every provider), so there is nothing for this call site to choose.
    deps.record_wedged(&pk);
    warn!(
        %provider_addr, pool_id = ?channel, ?reason,
        "node-origin: upstream rejected our voucher on terms this lane cannot recover from; \
         suppressing the provider for a bounded window while the pool's own deposit is \
         unaffected (#1122)"
    );
}

/// How long a provider that rejected a voucher on a lane-terminal reason is kept out of
/// ranking (seconds — a duration, not an epoch stamp; the map VALUE is the epoch second).
/// Bounded because the pool has no per-provider deadline to key the horizon on — the shared
/// deposit outlives any single lane.
const WEDGED_PROVIDER_SUPPRESSION_SECS: u64 = 3600;

/// What a voucher rejection tells us, and therefore what to do about it.
///
/// Exhaustive on purpose, like [`classify_refusal`]: a new `VoucherRejectReason` must break
/// this build rather than silently inherit a verdict. The reasons are not variations on one
/// theme — three genuinely different things arrive on this wire code:
///
/// - **Our signer is broken.** `BadSignature`/`WrongSigner` mean the upstream could not
///   verify a signature WE produced. That is not a payment problem, it is a defect in this
///   node, and it hits every candidate we try — so it belongs in the loud
///   [`PullVerdict::OurLocalFault`] arm, which exists for exactly this. Routing it to the
///   payment bucket also *hides* it: the ladder checks `UpstreamVoucherRejected` before
///   `LocalPullFault`, so a broken buyer key produces a `debug!` about payments instead of
///   the `warn!` about a node that cannot pay anyone.
/// - **This lane to this provider is finished, but the pool's DEPOSIT is not gone.** The
///   signer's spending cap is exhausted (`SpendingCapExhausted`) or its capability expired
///   (`CapabilityExpired`), our accounting drifted (`AmountRegression`/`BytesRegression`),
///   or the voucher was addressed to the wrong pool or a different provider
///   (`WrongPool`/`WrongProvider`). No further voucher on this lane is accepted, but the
///   pool row still holds a deposit worth keeping — so the provider is suppressed for a
///   bounded window and the pool row is KEPT rather than deleted.
/// - **Try again.** `PoolExhausted` — the pool WE fund the upstream from can no longer
///   cover further credit (ADR 003 §Pool solvency). Every upstream returns it, so it is a
///   statement about us, not the peer. Top up our pool (see `genuine_exhaustion`) and
///   retry rather than suppress a healthy peer.
///
/// Wallet-less resume: this classifier does NOT special-case a bundled
/// `SpendingCapExhausted`/`AmountRegression`/`BytesRegression`, and it does not need to.
/// The gap-driven `decdn_client_pull::drive` loop (this node's own cache-miss buyer leg)
/// already retries a resumable rejection in its own loop before it can ever surface here: it
/// reseeds the pool's ledger and reopens the pull, transparently, and this classifier sees
/// only the FINAL outcome. The loop also answers a genuine `SpendingCapExhausted` with an
/// on-chain top-up (via [`NodeFunder`]) rather than a terminal error. So by the time
/// `pull_verdict` downcasts an error to `UpstreamVoucherRejected` and
/// reaches this function, the rejection is genuinely terminal: either the reason was never
/// gated, it carried no bundle, the bundle failed shape validation, or the bounded resume
/// attempts were exhausted. `OurDeadLane` remains the correct verdict for every lane-terminal
/// reason in that case — the lane really is unusable, and the deposit worth keeping the pool
/// row for is not what needs reclaiming.
const fn voucher_verdict(reason: VoucherRejectReason) -> PullVerdict {
    match reason {
        // Our own signing or metering is broken, and it hits every candidate.
        // `BadPreimage` and `ChainIndexZero` belong here for the same reason
        // a bad signature does: both mean this node released a proof no upstream
        // can accept — a wrong seed, a wrong chain, or an index that never
        // travels the wire — so suppressing the peer would blame the wrong party
        // and hide a buyer-side bug. `ChunkPriceMismatch` is the same shape: we
        // signed a price that is not the rate this node was quoted.
        VoucherRejectReason::BadSignature
        | VoucherRejectReason::WrongSigner
        | VoucherRejectReason::BadPreimage
        | VoucherRejectReason::ChainIndexZero
        | VoucherRejectReason::ChunkPriceMismatch => PullVerdict::OurLocalFault,

        // Our OWN buyer pool, not the upstream — retry, do not suppress the peer.
        // `PoolExhausted` says the pool WE fund the upstream from can no longer cover
        // further credit; it is a statement about us, so every upstream returns it and
        // routing it to `OurDeadLane` would walk the candidate list suppressing each
        // healthy peer for an hour, outliving any top-up. The remedy is a top-up of our
        // pool (see `genuine_exhaustion`) and a retry, so keep the peer and try again.
        // Not fatal, and not the peer's fault: this stream had not carried the
        // current epoch's `chain_root` voucher before its first reveal. The fix
        // is to re-anchor and resend, which is what a retry does — and the
        // resend costs nothing, because an at-or-below-watermark voucher is
        // already-satisfied rather than rejected.
        VoucherRejectReason::PoolExhausted | VoucherRejectReason::UnanchoredPreimage => {
            PullVerdict::OurVoucherRetryable(reason)
        }
        // Terminal for THIS lane while the pool row is still worth keeping. The signer's
        // cap is spent — either the upstream's voucher-amount check
        // (`SpendingCapExhausted`) or its mid-stream cross-provider cap-headroom
        // re-check (`SignerCapExhausted`) — or its capability expired
        // (`CapabilityExpired`), our accounting drifted
        // (`AmountRegression`/`BytesRegression`), or the voucher named the wrong pool or a
        // different provider (`WrongPool`/`WrongProvider`). None of these has surrendered
        // the pool row's value outright — a mis-addressed or drifted voucher spends
        // nothing, and an exhausted cap or expired capability means too little for THIS
        // signer right now — so the provider is suppressed and the pool row is KEPT.
        VoucherRejectReason::WrongPool
        | VoucherRejectReason::WrongProvider
        | VoucherRejectReason::AmountRegression
        | VoucherRejectReason::BytesRegression
        | VoucherRejectReason::SpendingCapExhausted
        | VoucherRejectReason::SignerCapExhausted
        | VoucherRejectReason::CapabilityExpired => PullVerdict::OurDeadLane(reason),
    }
}

/// The ordered sentinel ladder. Pure: no metrics, no reputation, no I/O.
fn pull_verdict(err: &anyhow::Error) -> PullVerdict {
    if err.downcast_ref::<BlobTooLarge>().is_some() {
        return PullVerdict::Oversize;
    }
    if err.downcast_ref::<RateAboveCeiling>().is_some() {
        return PullVerdict::RateCeiling;
    }
    // Above the peer-blaming arms, alongside the other local faults: the upstream
    // answered honestly ("the blob is only N bytes"), and the offset it refused is one
    // WE computed — a resume frontier that overran the blob (#1530). Without an arm of
    // its own it falls through to `Unreachable`, which marks an honest peer down with a
    // local EWMA hit (ADR 008 scoring is local-only) on the strength of our own
    // arithmetic. `ResumeOffsetPastEnd`'s own doc says it: "this is a statement about
    // the offset, not about the peer".
    if err.downcast_ref::<ResumeOffsetPastEnd>().is_some() {
        return PullVerdict::OurLocalFault;
    }
    if err.downcast_ref::<PullTimeout>().is_some() {
        return PullVerdict::OurDeadline;
    }
    if err.downcast_ref::<PullStalled>().is_some() {
        return PullVerdict::Stalled;
    }
    // Ahead of the catch-all, deliberately: a `0x10` close is the ONE transport failure
    // that is a statement by the peer rather than an absence of one (#1986). Left to the
    // residual it scores `Unreachable` for exactly the condition the handler-level
    // `Overloaded` refusal is exonerated for, and a `global-full` shed scores every
    // concurrent prober at once.
    if err.downcast_ref::<UpstreamRateLimited>().is_some() {
        return PullVerdict::RateLimited;
    }
    if let Some(rejected) = err.downcast_ref::<UpstreamVoucherRejected>() {
        return voucher_verdict(rejected.reason);
    }
    // Ahead of `UpstreamRefused` and the catch-all, deliberately: a local signing or encode
    // fault surfaces while we are talking to a peer, and every arm below this one blames
    // the peer to some degree. A node with a broken buyer key hits this on EVERY candidate,
    // so getting the order wrong here does not mis-score one provider — it tars the whole
    // candidate list with a local `Unreachable` EWMA hit (ADR 008 scoring is local-only,
    // no cross-node propagation) on the strength of our own defect.
    if err.downcast_ref::<LocalPullFault>().is_some() {
        return PullVerdict::OurLocalFault;
    }
    if let Some(refused) = err.downcast_ref::<UpstreamRefused>() {
        // A `VoucherRejected` arriving as a mid-stream refusal is the SAME event as one
        // arriving in reply to a voucher, and must get the same remedy (#1145 review).
        //
        // The client's `resolve_voucher_slot` types a `VoucherRejected` into
        // `UpstreamVoucherRejected` wherever it lands, so this unwrap is a defensive backstop
        // for any `VoucherRejected` that still reaches here inside `UpstreamRefused`: were it
        // left folded into the refusal ladder it would be ruled `OurFault` — score nothing,
        // suppress nothing, do nothing — and skip the whole channel remedy above, leaving a
        // wedged or settled channel in the store to be handed straight back on the next miss,
        // forever, on a `debug!` line invisible at the project's default `RUST_LOG=info`.
        //
        // Unwrap it here rather than in `classify_refusal`, because the answer is not a
        // refusal verdict at all: it is a statement about our CHANNEL, and `voucher_verdict`
        // is the one place that decides what a rejected voucher costs.
        if let StreamError::VoucherRejected { reason, .. } = refused.error() {
            return voucher_verdict(*reason);
        }
        return PullVerdict::Refused(classify_refusal(refused.error()));
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
/// `InternalError`, by which a peer reports its own degradation; a transport-level
/// rate-limit shed is exonerated the same way (#1986); a hash mismatch is
/// `Corruption`; everything else is `Unreachable`.
///
/// [`pull_verdict`] makes the decision; this acts on it, and RETURNS it so the
/// caller can act on it too (#1560). The two consumers want different halves of the
/// same answer: this function asks "what does this failure say about the PEER?" and
/// spends the reputation/suppression/channel remedies accordingly, while the caller
/// asks "what may we tell our own client?" and folds the verdict into a
/// [`PullMiss`]. Returning it is what stops the second question being answered by
/// silence: without it every non-hit looks identical to the serve path, and a fault in
/// this node is signed to a client as a clean `NotFound` about the content.
///
/// A caller that has already answered on the wire may drop the verdict: by then
/// the `StreamResponse` is signed `ok: true` and sent, so the failure is an abort, not a
/// refusal code, and there is no answer left to pick. A caller that has NOT yet answered
/// owes the verdict a [`PullMiss`]; dropping it there signs this node's own fault to a
/// client as an absent blob (#1560).
///
/// `channel` is the buyer pool the pull was paying from, or `None` for the failures that
/// happen before there is one to pay from (a pool open that never completed, a local
/// binding-signature fault). It is `Option` rather than plumbed unconditionally because the
/// distinction is real: only a pull that presented a voucher can have one rejected, so only
/// those sites can reach [`PullVerdict::OurDeadLane`] and name the pool to suppress against.
// The tracing macros inflate the cognitive-complexity metric past threshold.
#[allow(clippy::cognitive_complexity)]
fn classify_pull_failure(
    deps: &NodeOriginDeps,
    pk: PublicKey,
    provider_addr: Address,
    hash_bytes: [u8; 32],
    channel: Option<B256>,
    err: &anyhow::Error,
) -> PullVerdict {
    let suppress = |ttl: Option<Duration>| {
        let node = DhtNodeId::from_bytes(*pk.as_bytes());
        let hash = DhtHash::from_bytes(hash_bytes);
        match ttl {
            Some(ttl) => deps.negative_cache.record_failure_with_ttl(node, hash, ttl),
            None => deps.negative_cache.record_failure(node, hash),
        }
    };

    let verdict = pull_verdict(err);
    match verdict {
        // OUR ceiling, not the provider's fault — it may legitimately serve larger blobs to
        // nodes configured with a higher `max_blob_size`. Metered, not scored (#1895). The
        // abort fires once the RECEIVED bytes cross the ceiling, so we paid the upstream for
        // the prefix we took (bounded to roughly one ceiling), never for the peer's claim.
        PullVerdict::Oversize => {
            deps.metrics.node_pull_too_large();
            debug!(%provider_addr, %err, "node-origin: upstream blob crossed our size ceiling on received bytes; pull aborted");
        }
        // The provider quoted above our effective rate ceiling (#1375). We refused before
        // paying; the signed over-quote is retained ON the `RateAboveCeiling` error for a
        // caller to act on, though this handler does not itself submit a challenge
        // (auto-slashing is deferred). Metered like `Oversize` so the refusal is
        // operator-visible, then suppressed for the full durable TTL (as `OurDeadline` does,
        // NOT `Oversize`, which only meters): the quote is a lasting fact about this
        // (peer, hash) — re-probing gets the same rate. Reputation-neutral: whether it is a
        // bait-and-switch or just our tight config we do not adjudicate here, so we do not
        // tar the peer.
        PullVerdict::RateCeiling => {
            deps.metrics.node_pull_rate_above_ceiling();
            suppress(Some(REFUSAL_SUPPRESSION_TTL));
            debug!(%provider_addr, %err, "node-origin: upstream quoted above our rate ceiling; refused before paying, suppressing the pair");
        }
        // A possibly mis-sized local budget, not evidence the provider is unreachable
        // (#857). Unscored — but NOT ignored (#1145 review).
        //
        // Exonerating and doing nothing are different things, and collapsing them left the
        // cheapest griefer in the protocol unanswered. A peer that probes honestly
        // (`has_blob: true`, low rate, low RTT → ranks #1), accepts the stream, signs a
        // valid `StreamResponse`, and then sends NOTHING lands here — our deadline fired, so
        // it is `OurDeadline`. It was then neither scored nor suppressed, so it stayed
        // top-ranked and burned a full budget on EVERY subsequent miss, for every hash,
        // forever, at zero cost to itself. That is precisely the hole this PR closed for
        // refusals ("a peer that advertises everything and serves nothing burned a candidate
        // slot on every miss forever"), left open one stage over for a peer that does not
        // even bother to answer.
        //
        // Suppression is the instrument that fits: scoped to (peer, hash), TTL'd, and
        // reputation-neutral — so it costs an honest-but-slow peer one TTL and costs a
        // silent one its permanent free lunch, without broadcasting a judgement about either.
        // That neutrality is what lets it be applied to a verdict we cannot attribute: we
        // are not saying the peer is bad, only that we will not keep waiting on it.
        PullVerdict::OurDeadline => {
            deps.metrics.node_pull_timeout();
            suppress(Some(REFUSAL_SUPPRESSION_TTL));
            debug!(%provider_addr, %err, "node-origin: pull hit our local deadline; suppressing briefly, not tarring upstream reputation");
        }
        // A throughput-floor abort is non-attributable (#1797), the same class as
        // `OurDeadline`: a stream that falls below the floor may be slow because of the link,
        // congestion, or our own slow consumption, none of which the peer can be blamed for,
        // and a throughput signal is spoofable in both directions (ADR 005 §Non-empty, ADR
        // 008). So it is metered and the `(peer, hash)` pair is suppressed — which stops a
        // peer that accepts a stream and then stalls from burning a candidate slot on every
        // miss — but no `Outcome` is recorded and reputation is untouched. `PullStalled` and
        // `PullTimeout` stay distinct metrics; the only difference from `OurDeadline` here is
        // the counter.
        PullVerdict::Stalled => {
            deps.metrics.node_pull_stalled();
            suppress(Some(REFUSAL_SUPPRESSION_TTL));
            debug!(%provider_addr, %err, "node-origin: upstream fell below the throughput floor; suppressing briefly, not tarring upstream reputation");
        }
        // The peer shed us at the transport with `APP_ERR_RATE_LIMITED` (#1986): the same
        // event as a handler-level `Overloaded` refusal, one layer down, and it earns the
        // same remedy. Suppressing the `(peer, hash)` pair briefly stops a shedding peer
        // from burning a candidate slot on every retry of this miss; scoring it would tar
        // a peer for honestly saying "not now" — and, for the node-wide `global-full`
        // layer, tar every peer probing it in that window at once.
        PullVerdict::RateLimited => {
            deps.metrics.node_upstream_rate_limited();
            suppress(Some(REFUSAL_SUPPRESSION_TTL));
            debug!(%provider_addr, %err, ttl = ?REFUSAL_SUPPRESSION_TTL, "node-origin: upstream shed the pull at the transport (rate-limited); suppressing briefly, not tarring upstream reputation");
        }
        // Our payment-side fault — the provider is not scored (#857). What separates this arm
        // from the retryable one below is what it costs the LANE.
        //
        // This lane can no longer pay, but the shared pool DEPOSIT is still escrowed: a
        // desync, a drained-cap balance, a mis-addressed voucher. The pool row is the handle
        // on that money (the recovery sweeps enumerate the store), so it is KEPT and the
        // provider is suppressed instead of the row being deleted. `warn!`, not `debug!`, for
        // the same reason the local-fault arm is: at the default `RUST_LOG=info` a `debug!`
        // is invisible, and money is at stake.
        PullVerdict::OurDeadLane(reason) => {
            deps.metrics.node_pull_voucher_rejected();
            wedged_channel(deps, pk, provider_addr, hash_bytes, reason, channel);
        }
        // The peer is fine, the fault is our buyer pool: `PoolExhausted` means the deposit we
        // fund the upstream from can no longer cover further credit, cleared by a top-up and a
        // retry. Skip the candidate this once and leave the peer alone — suppressing here would
        // throw away a healthy provider over our own funding gap.
        PullVerdict::OurVoucherRetryable(reason) => {
            deps.metrics.node_pull_voucher_rejected();
            debug!(
                %provider_addr, ?reason, %err,
                "node-origin: upstream rejected the voucher but the channel is healthy; \
                 left intact for a retry"
            );
        }
        // A refusal proves the peer is reachable and answering, so it is not `Unreachable`
        // on its own; scoring it `Unreachable` would tar a node exactly as hard for honestly
        // saying it lacks a blob as for being dead (#1144).
        //
        // Exonerating it is not the same as ignoring it, though. Every candidate that got
        // this far answered `has_blob = true` at probe, so a refusal is a peer contradicting
        // itself, and recording nothing would let a peer that advertises everything and serves
        // nothing keep winning the ranker and burn a `MAX_PROVIDER_ATTEMPTS` slot on every
        // miss, forever. The negative cache is the right instrument — scoped to (peer, hash),
        // TTL'd, reputation-neutral — and `RefusalVerdict` decides what the suppression is
        // worth: five minutes for a peer that truthfully says the blob is gone, far less for
        // a `NotFound` that may well have been our own empty deposit.
        PullVerdict::Refused(verdict) => {
            deps.metrics.node_pull_refused();
            match verdict {
                RefusalVerdict::NodeFault => {
                    debug!(peer = %pk, %provider_addr, %err, "node-origin: upstream reports itself degraded; scoring unreachable");
                    record_outcome(deps, pk, &Outcome::Unreachable);
                }
                RefusalVerdict::DurableMiss(cause) => {
                    // Exhaustive on the cause, not an `if ==` — a new cause added
                    // tomorrow must DECIDE its telemetry here, the same discipline
                    // `RefusalVerdict`'s own doc demands of new `StreamError`s.
                    // Two causes emit nothing, for unrelated reasons; merging them
                    // would lose both rationales (`clippy::match_same_arms`).
                    #[allow(clippy::match_same_arms)]
                    match cause {
                        // ADR 001 §Probe cache mandates tracking this rate; ADR
                        // 005 says a correct hold mechanism should make it rare,
                        // so a sustained rate is a remote implementation bug, not
                        // a tuning knob. The `monitoring/grafana-dashboard.json`
                        // panel scraping this metric predates the emitter (#1165).
                        DurableMissCause::EvictedSinceProbe => {
                            deps.metrics.probe_post_eviction_failure();
                        }
                        // A static fact about the blob vs. the peer's ceiling —
                        // says nothing about any hold mechanism; no telemetry.
                        DurableMissCause::BlobTooLarge => {}
                        // A takedown the peer is complying with. Expected
                        // behaviour, not a fault, and deliberately ambiguous
                        // between governance and the peer's local denylist —
                        // there is nothing here an operator could action, and a
                        // metric would only invite reading peers' local policy
                        // off the aggregate. No telemetry.
                        DurableMissCause::HashBlacklisted => {}
                    }
                    suppress(None);
                    debug!(%provider_addr, ?cause, %err, "node-origin: upstream does not have this blob; negative-caching this (peer, hash) for the full TTL without tarring reputation");
                }
                RefusalVerdict::Transient => {
                    // Metered (#1520). This arm is where a *buyer-side* problem
                    // lands: a seller refusing our channel for insufficient
                    // deposit signs `NotFound`, deliberately indistinguishable
                    // from an honest miss, so without a counter here a node whose
                    // own deposit cannot buy anything sees every pull refused with
                    // nothing in its telemetry saying why.
                    deps.metrics.node_pull_refused_unattributable();
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
        // provider it meets with an `Unreachable` (a local EWMA hit; ADR 008 scoring is
        // local-only, with no cross-node propagation) on the strength of its own defect. `warn!`, not
        // `debug!`: a node that cannot sign cannot pay, so this is operator-actionable —
        // and it is about US.
        PullVerdict::OurLocalFault => {
            deps.metrics.node_pull_local_fault();
            warn!(
                %provider_addr, %err,
                "node-origin: LOCAL buyer-side fault during a pull (signer/encode/range) — this node \
                 cannot pay; exonerating the upstream"
            );
        }
        PullVerdict::Corruption => {
            debug!(peer = %pk, %provider_addr, %err, "node-origin: upstream served corrupt bytes; scoring corruption");
            record_outcome(deps, pk, &Outcome::Corruption);
        }
        PullVerdict::Unreachable => {
            debug!(peer = %pk, %provider_addr, %err, "node-origin: upstream pull failed; scoring unreachable");
            record_outcome(deps, pk, &Outcome::Unreachable);
        }
    }
    verdict
}

/// The local reputation for `pk`, as the `f32` the selection score consumes.
/// Reputation is local-only (ADR 008): each node ranks peers from its own
/// observations, with no network aggregation. An unseen peer scores neutral
/// (local `initial_score`), so a cold provider ranks neither favoured nor
/// excluded (ADR 008 §Cold-Start).
#[allow(clippy::cast_possible_truncation)] // reputation ∈ [0,1]; f32 has ample precision for a ranking weight.
fn peer_reputation(deps: &NodeOriginDeps, pk: PublicKey) -> f32 {
    deps.local_rep.score(pk) as f32
}

/// Tracing target of the per-peer reputation event [`record_outcome`] emits on
/// every recorded outcome. The `decdn_node_pull_{success,unreachable,corruption}_total`
/// counters are aggregates; this event names the peer and outcome behind each fold,
/// including ADR 030 region penalties, which have no counter, and the score the peer
/// now holds. Idle decay moves scores without an event. It logs at `debug!`, because
/// a dead peer is re-probed on every cache miss. Enable it alone with
/// `RUST_LOG=info,decdn::reputation=debug`. `RUST_LOG` applies at startup only: a
/// config reload replaces the filter with `observability.log_level` and drops the
/// per-target setting.
const REPUTATION_LOG_TARGET: &str = "decdn::reputation";

/// Fold a pull/probe outcome into the local EWMA reputation score (ADR 008
/// §Local Score Calculation) and bump the matching delivery metric.
fn record_outcome(deps: &NodeOriginDeps, pk: PublicKey, outcome: &Outcome) {
    fold_outcome(&deps.local_rep, &deps.metrics, pk, outcome);
}

/// The body of [`record_outcome`], taking the reputation table and the metrics
/// directly so the fold, the metric, and the attribution event are testable
/// without a full [`NodeOriginDeps`].
fn fold_outcome(local_rep: &LocalReputation, metrics: &Metrics, pk: PublicKey, outcome: &Outcome) {
    let score = local_rep.record(pk, *outcome);
    debug!(
        target: REPUTATION_LOG_TARGET,
        peer = %pk, ?outcome, score,
        "node-origin: reputation outcome recorded"
    );
    match *outcome {
        Outcome::Delivered { .. } => metrics.node_pull_success(),
        Outcome::Corruption => metrics.node_pull_corruption(),
        Outcome::Unreachable => metrics.node_pull_unreachable(),
        // Region-latency mismatch (ADR 030) folds into the local score above but
        // has no delivery metric; `Outcome` is `#[non_exhaustive]`, so any future
        // variant likewise records into the score without a metric until an
        // explicit arm lands.
        _ => {}
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

    /// The wedge is a FIXED [`WEDGED_PROVIDER_SUPPRESSION_SECS`] window measured from the
    /// rejection, not a channel deadline — the buyer pool is shared across every provider and
    /// carries no per-provider expiry to key one on. Round-trips the write and the read halves
    /// so the horizon's DERIVATION is pinned, not just its comparison: nothing else in the
    /// workspace distinguishes 3600s from any other future instant, because both integration
    /// tests observe the wedge milliseconds after it is recorded.
    ///
    /// Fail-on-revert — each mutation run, with the assertion it actually trips:
    /// - derive the horizon from anything but `WEDGED_PROVIDER_SUPPRESSION_SECS` in
    ///   `record_wedged_at` → "the horizon is wedge time + the window";
    /// - flip `retain`'s `>` to `>=`, or drop the `retain` entirely → both trip "the elapsed
    ///   sibling must go in the same read", which reaches them before the boundary assertion
    ///   does because a stale entry survives that prune either way;
    /// - swap `insert` for `entry().or_insert()` → "a re-wedge must restart the window".
    ///
    /// The two map-state assertions pin what the booleans cannot: that the READ is what bounds
    /// the map. There is deliberately no sweep task, so a prune that stopped happening would
    /// leak an entry per wedged provider forever.
    #[test]
    fn a_wedge_lifts_when_its_fixed_suppression_window_elapses() {
        let peer = DhtNodeId::from_bytes([7u8; 32]);
        let other = DhtNodeId::from_bytes([9u8; 32]);
        let wedged_at = 1_000_000u64;
        let horizon = wedged_at + WEDGED_PROVIDER_SUPPRESSION_SECS;

        // Inside the window: suppressed, and the entry survives the read.
        let mut map = HashMap::new();
        record_wedged_at(&mut map, peer, wedged_at);
        assert_eq!(
            map.get(&peer),
            Some(&horizon),
            "the horizon is wedge time + the window"
        );
        assert!(
            prune_and_check_wedged(&mut map, &peer, horizon - 1),
            "a provider must stay suppressed for the whole window"
        );
        assert_eq!(map.len(), 1, "an unexpired horizon must survive the prune");

        // A live wedge is per-peer, and one read prunes every elapsed entry, not just the one
        // asked about. `other` elapsed a second ago; `peer` has not.
        record_wedged_at(&mut map, other, wedged_at - 1);
        assert!(
            prune_and_check_wedged(&mut map, &peer, horizon - 1),
            "a live horizon must survive a prune that drops a sibling"
        );
        assert_eq!(map.len(), 1, "the elapsed sibling must go in the same read");
        assert!(
            !prune_and_check_wedged(&mut map, &other, horizon - 1),
            "an elapsed peer must not inherit a live peer's suppression"
        );

        // ON the horizon second: rankable again, and the read pruned the entry.
        let mut map = HashMap::new();
        record_wedged_at(&mut map, peer, wedged_at);
        assert!(
            !prune_and_check_wedged(&mut map, &peer, horizon),
            "the boundary is exclusive: rankable ON the second the window ends"
        );
        assert!(map.is_empty(), "an elapsed horizon must be pruned on read");

        // A re-wedge restarts the window from the newer rejection rather than inheriting the
        // older horizon — reachable whenever a lifted provider is ranked and wedges again.
        let mut map = HashMap::new();
        record_wedged_at(&mut map, peer, wedged_at);
        record_wedged_at(&mut map, peer, wedged_at + 1_000);
        assert!(
            prune_and_check_wedged(&mut map, &peer, horizon),
            "a re-wedge must restart the window, not inherit the older horizon"
        );
    }

    /// The provider-wide window must outlive the per-`(peer, hash)` negative-cache entry the
    /// same wedge writes. At or below it, `wedged_providers` buys nothing the negative cache
    /// does not already give for the blob that wedged it, and the filter plus both integration
    /// tests guarding it become dead weight while still passing green.
    ///
    /// Bounding a policy constant is in-convention in this module — see
    /// `only_a_durable_refusal_earns_the_full_suppression_ttl`.
    #[test]
    fn the_provider_wide_window_outlives_the_per_hash_one() {
        assert!(
            WEDGED_PROVIDER_SUPPRESSION_SECS > REFUSAL_SUPPRESSION_TTL.as_secs(),
            "the provider-wide window ({WEDGED_PROVIDER_SUPPRESSION_SECS}s) must outlive the \
             per-(peer, hash) one ({}s), or the wedge filter buys nothing",
            REFUSAL_SUPPRESSION_TTL.as_secs()
        );
    }

    #[test]
    fn region_penalty_only_for_same_region_slow_peer() {
        // Same region, slow → penalized.
        assert!(region_latency_penalty_applies(
            Some("DE"),
            "DE",
            REGION_LATENCY_MAX_MS + 1
        ));
        // Same region but at/under the ceiling → not penalized (boundary is >).
        assert!(!region_latency_penalty_applies(
            Some("DE"),
            "DE",
            REGION_LATENCY_MAX_MS
        ));
        // Different region, however slow → not penalized (the claim is plausible).
        assert!(!region_latency_penalty_applies(Some("DE"), "US", 5000));
        // Own region unset → penalty disabled (nothing to compare against).
        assert!(!region_latency_penalty_applies(None, "DE", 5000));
        // Peer region unknown (not in the peer table) → no claim to contradict.
        assert!(!region_latency_penalty_applies(Some("DE"), "", 5000));
    }

    /// The failure-class `reason` (#966) the `open_channel` kernel attaches to
    /// the `anyhow` error chain must survive the additional `.context(...)`
    /// layers `open_and_persist` / `open_or_reuse_pool` wrap around it —
    /// `record_pool_open_failure`'s `downcast_ref` walks the whole chain, so
    /// the metric label is recovered regardless of how deep the reason sits.
    #[test]
    fn failure_reason_survives_context_wrapping() {
        for reason in [
            PoolOpenFailureReason::InsufficientDeposit,
            PoolOpenFailureReason::ContractRevert,
            PoolOpenFailureReason::RpcError,
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
            let recovered = err.downcast_ref::<PoolOpenFailureReason>().copied();
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
        assert!(storeless.downcast_ref::<PoolOpenFailureReason>().is_none());
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
            reason: decdn_protocol::client::VoucherRejectReason::SpendingCapExhausted,
            bundle: None,
        });
        assert!(rejected.downcast_ref::<UpstreamVoucherRejected>().is_some());
        assert!(rejected.downcast_ref::<PullTimeout>().is_none());

        let shed: anyhow::Error = anyhow::Error::new(UpstreamRateLimited { label: None });
        assert!(shed.downcast_ref::<UpstreamRateLimited>().is_some());
        assert!(shed.downcast_ref::<PullStalled>().is_none());

        // Even with an added context layer, the plain `downcast_ref` the
        // orchestrator uses still recovers the sentinel (no `root_cause()` needed).
        let wrapped = timeout.context("added context in some future propagation path");
        assert!(wrapped.downcast_ref::<PullTimeout>().is_some());

        // `LocalPullFault` (#1145 review) is the one sentinel attached as a CONTEXT
        // layer rather than as the error itself — `anyhow!("voucher signing failed")
        // .context(LocalPullFault)` — and it is then wrapped again on the way up. If
        // this downcast ever stopped working, the exoneration arm would silently stop
        // firing and a node with a broken signer would go back to recording a local
        // `Unreachable` EWMA hit against every honest provider it tried. That failure is invisible
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
        let refused: anyhow::Error =
            anyhow::Error::new(UpstreamRefused::mid_stream(StreamError::NotFound));
        let recovered = refused
            .downcast_ref::<UpstreamRefused>()
            .map(|r| r.error().clone());
        assert_eq!(recovered, Some(StreamError::NotFound));
        assert!(refused.downcast_ref::<UpstreamVoucherRejected>().is_none());
    }

    /// Asserted on the real predicate `classify_pull_failure` consults: a refusal is
    /// proof the peer ANSWERED, so only the one code by
    /// which a peer reports its own degradation may score it. The
    /// `NotFound` case is the heart of the issue: a healthy-but-empty node must not take
    /// an `Unreachable` hit (local EWMA; ADR 008 has no cross-node propagation) for
    /// honestly saying so.
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
                reason: VoucherRejectReason::PoolExhausted,
                bundle: None,
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

    /// A `PoolExhausted` rejection is a statement about OUR buyer pool, not the peer:
    /// the deposit we fund the upstream from can no longer cover further credit. The
    /// peer did nothing wrong, so this must be judged retryable with the peer KEPT —
    /// not an `OurDeadLane` (which would suppress a healthy provider needlessly) nor
    /// an `OurLocalFault` (which would tar the peer for our own funding gap). A top-up
    /// and retry is the fix, which is exactly what `OurVoucherRetryable` drives.
    #[test]
    fn a_pool_exhaustion_is_a_retryable_topup_not_a_dead_channel() {
        assert_eq!(
            voucher_verdict(VoucherRejectReason::PoolExhausted),
            PullVerdict::OurVoucherRetryable(VoucherRejectReason::PoolExhausted),
            "our own drained pool keeps the healthy peer and retries after a top-up"
        );
    }

    /// The #1145-review refinement: exonerating a refusal is not the same as believing
    /// it. How long we suppress a (peer, hash) must match how much the refusal actually
    /// proves — and for the codes below it proves rather little.
    #[test]
    fn only_a_durable_refusal_earns_the_full_suppression_ttl() {
        // A peer that truthfully says the blob is gone, or is over its ceiling, will say
        // the same thing in a minute. Worth the full TTL.
        for (error, cause) in [
            (
                StreamError::EvictedSinceProbe,
                DurableMissCause::EvictedSinceProbe,
            ),
            (StreamError::BlobTooLarge, DurableMissCause::BlobTooLarge),
        ] {
            assert_eq!(
                classify_refusal(&error),
                RefusalVerdict::DurableMiss(cause),
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
                bundle: None,
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
    /// (a local EWMA hit; ADR 008 scoring is local-only) to every honest peer it meets, on the
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
    ///   the REAL `aligned_wire_len` into its REAL error and asserts the marker is on it,
    ///   never attaching it itself.
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
            "a local fault must outrank the catch-all — reaching it tars every honest \
             provider as unreachable"
        );

        // And it must outrank the arm that sits directly below it. `UpstreamRefused` is the
        // one that would otherwise catch a local fault raised while a refusal was in flight,
        // and it exonerates the peer for the WRONG reason — quietly, and without the
        // `node_pull_local_fault_total` an operator needs to see that this node is broken.
        let refused_too = anyhow::anyhow!("encode failed")
            .context(LocalPullFault)
            .context(UpstreamRefused::mid_stream(StreamError::NotFound));
        assert_eq!(
            pull_verdict(&refused_too),
            PullVerdict::OurLocalFault,
            "a local fault must win over a refusal on the same chain: the refusal is a \
             symptom, the broken node is the cause"
        );
    }

    /// Only a fault in THIS node may stop a failed pull answering `NotFound` (#1560).
    ///
    /// The asymmetry is the whole point, and both halves of it can regress silently. Widen
    /// it and a node with one wedged lane to one provider tells every client "do not
    /// retry this node" — steering traffic off a node that is fine for every other provider
    /// and every other blob. Narrow it (or let a future verdict fall into a catch-all) and
    /// we are back to the bug: a broken buyer key signs a client a `NotFound` about content
    /// that exists and is reachable, and the client caches OUR defect as a fact about the
    /// blob.
    ///
    /// The build-break guarantee lives in `PullMiss::for_verdict`'s catch-all-free `match`,
    /// NOT here: this test iterates a hand-written list, so a new [`PullVerdict`] variant
    /// would compile fine and simply go untested. What the test pins is the DECISION each
    /// existing variant made — the thing a future refactor could flip without noticing.
    ///
    /// Known edges, stated precisely because the guarantee is narrower than it looks:
    /// `for_verdict` matches `OurDeadLane(_)` / `OurVoucherRetryable(_)` on their payloads,
    /// so a new `VoucherRejectReason` inherits `Clean` without a build break — acceptable,
    /// because `voucher_verdict` IS exhaustive over all ten and already routes the node-wide
    /// reasons (`BadSignature`, `WrongSigner`) to `OurLocalFault` before this function sees
    /// them. `RefusalVerdict`'s four discriminants are spelled out so a FIFTH does break the
    /// build; its `DurableMiss(_)` payload is not, so a new `DurableMissCause` still inherits
    /// `Clean`.
    #[test]
    fn only_our_own_fault_may_withhold_a_not_found() {
        let reason = VoucherRejectReason::SpendingCapExhausted;
        for verdict in [
            PullVerdict::Oversize,
            PullVerdict::RateCeiling,
            PullVerdict::OurDeadline,
            PullVerdict::Stalled,
            PullVerdict::OurDeadLane(reason),
            PullVerdict::OurVoucherRetryable(reason),
            PullVerdict::Refused(RefusalVerdict::NodeFault),
            PullVerdict::Refused(RefusalVerdict::Transient),
            PullVerdict::Refused(RefusalVerdict::OurFault),
            PullVerdict::Refused(RefusalVerdict::DurableMiss(DurableMissCause::BlobTooLarge)),
            PullVerdict::Corruption,
            PullVerdict::Unreachable,
            PullVerdict::RateLimited,
        ] {
            assert_eq!(
                PullMiss::for_verdict(verdict),
                PullMiss::Clean,
                "{verdict:?} is not an UNEXPECTED failure of this node, so it must still \
                 answer a clean miss"
            );
        }

        assert_eq!(
            PullMiss::for_verdict(PullVerdict::OurLocalFault),
            PullMiss::LocalFault,
            "a broken signer / encode / range is the one verdict that makes a `NotFound` a \
             false claim about the content"
        );
    }

    /// A [`PullMiss::BelowMargin`] answers the same wire code as [`PullMiss::Clean`]:
    /// declining an unprofitable relay must not leak the serve-economics floor to the
    /// client as a distinct signal (that would let a client infer this node's buy
    /// ceiling by probing for the wire difference).
    #[test]
    fn below_margin_is_wire_identical_to_clean() {
        assert!(matches!(
            miss_answer(PullMiss::Clean),
            Ok(OriginFetch::NotFound)
        ));
        assert!(matches!(
            miss_answer(PullMiss::BelowMargin),
            Ok(OriginFetch::NotFound)
        ));
    }

    /// A [`PullMiss::BelowMargin`] never masks, and is never masked by, a
    /// [`PullMiss::LocalFault`] in the fold: `LocalFault` outranks everything else
    /// regardless of position, in both argument orders. A [`PullMiss::BelowMargin`]
    /// DOES outrank a plain [`PullMiss::Clean`], again in both orders, because the
    /// fold's job is to carry the strongest signal seen anywhere in the walk forward
    /// — losing a below-margin classification to a later clean miss would report a
    /// genuinely empty walk when an economics refusal actually happened partway
    /// through it.
    #[test]
    fn below_margin_never_masks_or_is_masked_by_a_local_fault() {
        assert_eq!(
            PullMiss::LocalFault.or(PullMiss::BelowMargin),
            PullMiss::LocalFault,
            "a local fault must survive being folded against a later economics refusal"
        );
        assert_eq!(
            PullMiss::BelowMargin.or(PullMiss::LocalFault),
            PullMiss::LocalFault,
            "a local fault reached later in the walk must still win over an earlier \
             economics refusal"
        );
        assert_eq!(
            PullMiss::BelowMargin.or(PullMiss::Clean),
            PullMiss::BelowMargin,
            "an economics refusal must survive being folded against a later clean miss"
        );
        assert_eq!(
            PullMiss::Clean.or(PullMiss::BelowMargin),
            PullMiss::BelowMargin,
            "an economics refusal reached later in the walk must still win over an \
             earlier clean miss"
        );
    }

    /// A local fault LATCHES across a walk: one candidate's fault is not erased by the next
    /// candidate's honest miss.
    ///
    /// The order matters in both directions, which is why both are asserted. A walk folds
    /// left-to-right over whatever order the ranker produced, so a fault at position 1
    /// followed by clean misses, and clean misses followed by a fault at position 3, are the
    /// same story and must reach the same answer — otherwise the wire code an operator sees
    /// depends on where in the ranking the broken pull happened to land.
    #[test]
    fn a_local_fault_survives_the_rest_of_the_walk() {
        assert_eq!(
            PullMiss::Clean.or(PullMiss::Clean),
            PullMiss::Clean,
            "a walk of honest misses is an honest miss"
        );
        assert_eq!(
            PullMiss::LocalFault.or(PullMiss::Clean),
            PullMiss::LocalFault,
            "a later clean miss must not overwrite an earlier fault of ours"
        );
        assert_eq!(
            PullMiss::Clean.or(PullMiss::LocalFault),
            PullMiss::LocalFault,
            "a fault reached late in the walk counts the same as one reached first"
        );
        assert_eq!(
            PullMiss::LocalFault.or(PullMiss::LocalFault),
            PullMiss::LocalFault
        );
    }

    /// A refusal that arrives MID-STREAM carries the same wire code, and therefore the same
    /// meaning, as one that arrives at the open. Stringifying it
    /// (`bail!(\"stream failed: {e:?}\")`) would fall through every downcast to the
    /// catch-all and score the peer `Unreachable` — the same mis-attribution #1144
    /// forecloses at the open stage, one stage later.
    #[test]
    fn a_mid_stream_refusal_is_judged_by_its_wire_code_not_the_catch_all() {
        for (error, want) in [
            (StreamError::NotFound, RefusalVerdict::Transient),
            (StreamError::Overloaded, RefusalVerdict::Transient),
            (
                StreamError::EvictedSinceProbe,
                RefusalVerdict::DurableMiss(DurableMissCause::EvictedSinceProbe),
            ),
            (StreamError::InternalError, RefusalVerdict::NodeFault),
        ] {
            // Exactly what the receive loops now raise — wrapped, because a real one comes
            // up through the pull path's `.context` layers and `downcast_ref` must still
            // find it.
            let err = anyhow::Error::new(UpstreamRefused::mid_stream(error.clone()))
                .context("receive and pay")
                .context("pull from candidate");
            assert_eq!(
                pull_verdict(&err),
                PullVerdict::Refused(want),
                "a mid-stream {error:?} must be judged as a refusal, not fall to the catch-all"
            );
        }
    }

    /// ADR 001 §Probe cache mandates tracking the `EvictedSinceProbe` rate, and
    /// ADR 005 explains why it is not just "a candidate failed": it is a peer
    /// contradicting its own recent availability answer. Both causes
    /// are `DurableMiss` — they say the same thing about the peer — but only one
    /// is that signal, and a shared unpayloaded variant cannot tell them apart.
    #[test]
    fn only_an_eviction_carries_the_post_eviction_cause() {
        assert_eq!(
            classify_refusal(&StreamError::EvictedSinceProbe),
            RefusalVerdict::DurableMiss(DurableMissCause::EvictedSinceProbe)
        );
        assert_eq!(
            classify_refusal(&StreamError::BlobTooLarge),
            RefusalVerdict::DurableMiss(DurableMissCause::BlobTooLarge)
        );
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

    /// A transport-level `APP_ERR_RATE_LIMITED` shed (#1986) is the same event as
    /// the handler-level `Overloaded` refusal, and must get the same verdict: brief
    /// suppression, no reputation. Before this arm the sentinel fell through the
    /// ladder to `Unreachable` — a 0.0 EWMA sample for a peer that answered, and
    /// one every prober in a `GlobalFull` window recorded at once.
    #[test]
    fn a_rate_limit_shed_is_suppressed_not_scored() {
        let shed = anyhow::Error::new(UpstreamRateLimited {
            label: Some("global-full".to_owned()),
        })
        .context("open_bi failed")
        .context("pull from candidate");
        assert_eq!(pull_verdict(&shed), PullVerdict::RateLimited);
        assert_eq!(
            PullMiss::for_verdict(PullVerdict::RateLimited),
            PullMiss::Clean,
            "a peer shedding load says nothing about THIS node, so the client gets an \
             honest miss"
        );
    }

    /// Shared in-memory sink for the captured tracing output.
    #[derive(Clone, Default)]
    struct CapturedLog(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Every recorded outcome names its peer and the score it leaves. The
    /// `node_pull_*` counters are unlabeled aggregates, so without this event an
    /// `Unreachable` penalty is a counter tick that no operator can attribute.
    #[test]
    fn a_reputation_penalty_names_the_peer_and_its_new_score() -> anyhow::Result<()> {
        // `docs/runbook.md` names this filter string verbatim.
        assert_eq!(REPUTATION_LOG_TARGET, "decdn::reputation");
        let local_rep = LocalReputation::new(decdn_reputation::LocalReputationConfig::default())?;
        let metrics = Metrics::new();
        let pk = iroh::SecretKey::generate().public();

        let log = CapturedLog::default();
        let sink = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .with_writer(move || sink.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            fold_outcome(&local_rep, &metrics, pk, &Outcome::Unreachable);
        });

        let text = String::from_utf8(
            log.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )?;
        let line = text
            .lines()
            .find(|l| l.contains(REPUTATION_LOG_TARGET))
            .ok_or_else(|| anyhow::anyhow!("no `{REPUTATION_LOG_TARGET}` event; got:\n{text}"))?;
        assert!(line.contains(&format!("peer={pk}")), "{line}");
        assert!(line.contains("outcome=Unreachable"), "{line}");
        let logged: f64 = line
            .split_once("score=")
            .and_then(|(_, rest)| rest.split_whitespace().next())
            .ok_or_else(|| anyhow::anyhow!("no score field: {line}"))?
            .parse()?;
        assert!(
            (logged - local_rep.score(pk)).abs() < 1e-3,
            "the event carries the post-fold score: {line}"
        );
        let scrape = metrics.encode()?;
        assert!(
            scrape
                .lines()
                .any(|l| l == "decdn_node_pull_unreachable_total 1"),
            "the aggregate still counts the penalty:\n{scrape}"
        );
        Ok(())
    }
}
