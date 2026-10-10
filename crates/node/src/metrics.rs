//! OpenMetrics/Prometheus metrics and a minimal `/metrics` HTTP server.
//!
//! Metrics live in an [`iroh_metrics::Registry`] so we can surface both our
//! `decdn_*` counters and iroh's own transport metrics through a single
//! endpoint. Output is `OpenMetrics` text, which Prometheus scrapers accept.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use alloy::primitives::U256;
use bytes::Bytes;
use decdn_cache::{CacheEngine, CacheMetrics};
use decdn_incentive::PoolOpenFailureReason;
use decdn_protocol::Region;
use http_body_util::Full;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use iroh::Endpoint;
use iroh::metrics::EndpointMetrics;
use iroh_metrics::{
    Counter, EncodeLabelSet, EncodeLabelValue, Family, Gauge, Histogram, MetricsGroup,
    MetricsSource, Registry,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, oneshot};

use crate::chain_events::resumable_watcher::WatcherHook;
use crate::load_shed::RequestClass;

/// Bucket upper bounds of `decdn_probe_collection_latency_seconds`, per the
/// registry row in `adr/appendix-observability.md`. The collection window has a
/// 500 ms ceiling, so the top finite bucket leaves room for a slow scheduler.
const PROBE_COLLECTION_BUCKETS: [f64; 8] = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5];

/// Bucket upper bounds shared by the time-to-first-byte histograms. A cache hit
/// resolves in the low buckets. A serve miss includes discovery and an upstream
/// fill — the whole blob, on a buffered fill — and a pull leg includes its open
/// and a stall window, so the top bucket reaches past the outer pull deadline at
/// the default timeouts (`selection::outer_pull_deadline`). A value in `+Inf`
/// alone pins `histogram_quantile` at the top finite bound.
const FIRST_BYTE_BUCKETS: [f64; 15] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

/// Bucket upper bounds of `decdn_rpc_request_duration_seconds`. The top finite
/// bucket is the node's per-call RPC deadline
/// ([`crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT`]): a request that reaches
/// the deadline is dropped and counts as `timeout` without a duration sample.
const RPC_REQUEST_BUCKETS: [f64; 10] = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

/// Cap concurrent `/metrics` connections. Prevents a trivial `DoS` where a
/// peer opens many sockets to the operational-data endpoint and exhausts
/// tasks.
const MAX_METRICS_CONNECTIONS: usize = 32;

/// Every counter that names why an inbound `cdn/client/v1` stream failed. Each
/// failed inbound stream counts on exactly one of them, so
/// `decdn_streams_failed_total{direction="inbound"}` minus their sum is zero. The
/// serve handler counts every end in one place, so a scrape never sees a failure
/// before its reason. The "Unattributed stream failures" panels of the reference
/// dashboards in `decdn/devops` subtract this list by hand, so a new reason here
/// needs the same edit there.
pub const INBOUND_FAILURE_REASONS: &[&str] = &[
    "decdn_serve_stream_rejected_bad_binding_total",
    "decdn_serve_stream_rejected_blob_too_large_total",
    "decdn_serve_stream_rejected_cache_miss_total",
    "decdn_serve_stream_rejected_chain_hash_denied_total",
    "decdn_serve_stream_rejected_chain_stale_total",
    "decdn_serve_stream_rejected_evicted_since_probe_total",
    "decdn_serve_stream_rejected_foreign_declined_total",
    "decdn_serve_stream_rejected_hash_denied_total",
    "decdn_serve_stream_rejected_insufficient_deposit_total",
    "decdn_serve_stream_rejected_internal_error_total",
    "decdn_serve_stream_rejected_load_shed_hit_total",
    "decdn_serve_stream_rejected_load_shed_miss_total",
    "decdn_serve_stream_rejected_origin_denied_total",
    "decdn_serve_stream_rejected_owner_mismatch_total",
    "decdn_serve_stream_rejected_pool_closing_total",
    "decdn_serve_stream_rejected_pool_unconfirmed_total",
    "decdn_serve_stream_rejected_pull_loop_guard_total",
    "decdn_serve_stream_rejected_range_not_satisfiable_total",
    "decdn_serve_stream_rejected_signer_cap_exhausted_total",
    "decdn_serve_stream_rejected_signer_floor_at_cap_total",
    "decdn_serve_stream_rejected_stream_cap_full_total",
    "decdn_serve_stream_rejected_unknown_lane_total",
    "decdn_serve_stream_request_unreadable_total",
    "decdn_serve_stream_midstream_pool_exhausted_total",
    "decdn_serve_stream_midstream_signer_cap_exhausted_total",
    "decdn_serve_stream_terminated_takedown_total",
    "decdn_serve_stream_voucher_rejected_total",
    "decdn_serve_stream_proof_budget_exhausted_total",
    "decdn_serve_stream_client_declined_total",
    "decdn_serve_stream_client_abandoned_total",
    "decdn_serve_stream_node_fault_total",
];

#[derive(
    Debug,
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    EncodeLabelValue,
)]
enum StreamDirection {
    Inbound,
    Outbound,
}

#[derive(
    Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, EncodeLabelSet,
)]
struct StreamLabels {
    direction: StreamDirection,
}

/// Why a probe for a present blob got **no** eviction hold, as the `reason`
/// label on `decdn_probe_hold_unavailable_total`.
///
/// Holds are best-effort, so this is not the same as "answered
/// `has_blob: false`" — `exhausted` and `stake_lane_reserved` still advertise.
///
/// The three values are not interchangeable — each has a different operator
/// remedy, which is why the reference `DecdnProbeHoldViolations` alert filters
/// on `reason="exhausted"` alone. The derive renders variants in `snake_case`, so
/// these encode as `reason="exhausted"` / `"disabled"` / `"stake_lane_reserved"`.
///
/// Not to be confused with [`decdn_cache::ProbeHoldOutcome::Unavailable`],
/// which is the one probe-hold outcome that emits **no** metric at all — a
/// blob genuinely absent is a true negative, not a refusal. The shared word is
/// inverted between the two types: here it selects *for* the counter, there it
/// selects *against* it.
#[derive(
    Debug,
    Clone,
    Copy,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    EncodeLabelValue,
)]
pub enum ProbeHoldUnavailableReason {
    /// Blob present, but **all** hold slots were live (`max_probe_holds`
    /// reached) — genuine budget pressure. The node still advertises
    /// `has_blob: true` and forgoes only the hold, so the blob may be
    /// LRU-evicted before the pull lands. This is the "raise `max_probe_holds`"
    /// signal and the only value the `DecdnProbeHoldViolations` alert fires on.
    Exhausted,
    /// Blob present, but the eviction-hold path is **disabled by config**
    /// (`max_probe_holds == 0`) — an intentional operator choice, not budget
    /// pressure (#739). The one reason that also suppresses the advertisement:
    /// the node answers `has_blob: false` for store-backed content (origin-held
    /// content takes no hold and is unaffected). Raising `max_probe_holds` is
    /// the remedy only if the disable was unintended; alerting on it would be
    /// nonsensical.
    Disabled,
    /// An end-client probe (a requester that is *not* a registered operator)
    /// hit the stake-lane-reserved end-client ceiling
    /// (`max_probe_holds - cache.stake_lane_reserved_holds`), keeping hold
    /// headroom for node-to-node cache-miss probes per ADR 003 §Admission and
    /// Priority (#757). The reservation is a content-independent *admission*
    /// decision, so unlike the two above it is decided before any hold attempt
    /// — but the handler still consults the cache afterwards to answer
    /// honestly, and advertises the blob if it is present. Zero unless
    /// `cache.stake_lane_reserved_holds > 0`.
    StakeLaneReserved,
}

impl ProbeHoldUnavailableReason {
    /// Every variant, in the order [`Metrics::new`] materializes them.
    ///
    /// This is what makes "every reason exports at zero" structural rather
    /// than a convention. [`Metrics::new`] destructures `ALL.map(…)` into its
    /// three cached handles, so adding a variant here changes the array length
    /// and **fails to compile** at that pattern — you cannot add a reason
    /// without materializing its child. The recorder's exhaustive `match` is
    /// the second gate; without this array a fourth variant could satisfy the
    /// compiler with a `_ =>` arm calling `get_or_create`, silently
    /// reintroducing the lazily-created series this design exists to prevent.
    ///
    /// Order is load-bearing (it binds handles positionally); a reorder is
    /// caught by `probe_hold_unavailable_increments_only_the_named_reason`.
    const ALL: [Self; 3] = [Self::Exhausted, Self::Disabled, Self::StakeLaneReserved];
}

#[derive(
    Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, EncodeLabelSet,
)]
struct ProbeHoldUnavailableLabels {
    reason: ProbeHoldUnavailableReason,
}

/// The `node_region` label on `decdn_staker_set_active_by_region`.
///
/// The label is `node_region`, not `region`: every reference dashboard scopes
/// its selectors on a scrape-side `region` target label. Under the default
/// `honor_labels: false`, Prometheus renames a clashing metric label to
/// `exported_region`, so the metric's own label would be lost.
#[derive(
    Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, EncodeLabelSet,
)]
struct NodeRegionLabels {
    node_region: String,
}

/// The `method` label on `decdn_rpc_requests_total` and
/// `decdn_rpc_request_duration_seconds`: the JSON-RPC method of one request
/// the node's alloy providers sent.
///
/// A closed set keeps the label's cardinality fixed. The variants are the
/// methods the node's reads, fillers and settlement path call; any other
/// method name counts as [`Self::Other`], so a request never mints a series
/// from a string it did not choose. Values render as the JSON-RPC method name
/// (`eth_getLogs`), not in `snake_case`.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RpcMethod {
    EthBlockNumber,
    EthCall,
    EthChainId,
    EthEstimateGas,
    EthFeeHistory,
    EthGasPrice,
    EthGetBalance,
    EthGetBlockByNumber,
    EthGetCode,
    EthGetLogs,
    EthGetTransactionByHash,
    EthGetTransactionCount,
    EthGetTransactionReceipt,
    EthMaxPriorityFeePerGas,
    EthSendRawTransaction,
    Other,
}

impl RpcMethod {
    /// Every variant. [`Metrics::new`] materializes one child per entry, so
    /// each series exports at zero from startup.
    pub(crate) const ALL: [Self; 16] = [
        Self::EthBlockNumber,
        Self::EthCall,
        Self::EthChainId,
        Self::EthEstimateGas,
        Self::EthFeeHistory,
        Self::EthGasPrice,
        Self::EthGetBalance,
        Self::EthGetBlockByNumber,
        Self::EthGetCode,
        Self::EthGetLogs,
        Self::EthGetTransactionByHash,
        Self::EthGetTransactionCount,
        Self::EthGetTransactionReceipt,
        Self::EthMaxPriorityFeePerGas,
        Self::EthSendRawTransaction,
        Self::Other,
    ];

    /// The label value: the JSON-RPC method name, or `other`.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::EthBlockNumber => "eth_blockNumber",
            Self::EthCall => "eth_call",
            Self::EthChainId => "eth_chainId",
            Self::EthEstimateGas => "eth_estimateGas",
            Self::EthFeeHistory => "eth_feeHistory",
            Self::EthGasPrice => "eth_gasPrice",
            Self::EthGetBalance => "eth_getBalance",
            Self::EthGetBlockByNumber => "eth_getBlockByNumber",
            Self::EthGetCode => "eth_getCode",
            Self::EthGetLogs => "eth_getLogs",
            Self::EthGetTransactionByHash => "eth_getTransactionByHash",
            Self::EthGetTransactionCount => "eth_getTransactionCount",
            Self::EthGetTransactionReceipt => "eth_getTransactionReceipt",
            Self::EthMaxPriorityFeePerGas => "eth_maxPriorityFeePerGas",
            Self::EthSendRawTransaction => "eth_sendRawTransaction",
            Self::Other => "other",
        }
    }

    /// Map a request's method name onto the closed set. An unlisted name,
    /// including the literal `other`, is [`Self::Other`].
    pub(crate) fn from_name(name: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|method| *method != Self::Other && method.as_str() == name)
            .unwrap_or(Self::Other)
    }
}

impl EncodeLabelValue for RpcMethod {
    fn encode_label_value(&self) -> iroh_metrics::LabelValue<'_> {
        iroh_metrics::LabelValue::Str(std::borrow::Cow::Borrowed(self.as_str()))
    }
}

/// The `outcome` label on `decdn_rpc_requests_total`: how one JSON-RPC
/// request ended. Each request counts under exactly one value. The derive
/// renders the variants in `snake_case`.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, EncodeLabelValue)]
pub(crate) enum RpcOutcome {
    /// The provider answered with a result.
    Ok,
    /// The provider answered with a revert: JSON-RPC error code `3`, or a
    /// message that names a revert. The provider worked; the call itself
    /// failed, so this is not a provider fault.
    Reverted,
    /// The provider answered with any other JSON-RPC error response, such as
    /// invalid params, a range cap on `eth_getLogs`, or a provider-side
    /// internal error.
    RpcError,
    /// The provider throttled the request: HTTP 429, an HTTP error with a
    /// `Retry-After` delay, or a JSON-RPC error with code 429 or rate-limit
    /// wording.
    RateLimited,
    /// No usable JSON-RPC reply arrived: a refused or reset connection, an HTTP
    /// error without a JSON-RPC body, an unreadable body, or a batch reply
    /// that left this request out.
    TransportError,
    /// The request did not resolve in time: the HTTP client timed out, or the
    /// caller dropped the request before a reply arrived. In this node the
    /// caller drops a request when its per-call deadline fires.
    Timeout,
}

impl RpcOutcome {
    /// Every variant. [`Metrics::new`] materializes one child per
    /// `(method, outcome)` pair, so each series exports at zero from startup.
    pub(crate) const ALL: [Self; 6] = [
        Self::Ok,
        Self::Reverted,
        Self::RpcError,
        Self::RateLimited,
        Self::TransportError,
        Self::Timeout,
    ];
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, EncodeLabelSet)]
struct RpcRequestLabels {
    method: RpcMethod,
    outcome: RpcOutcome,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, EncodeLabelSet)]
struct RpcMethodLabels {
    method: RpcMethod,
}

/// Which buy-ceiling regime an ADR 041 serve-economics refusal was decided
/// under, dispatched to the matching sibling counter by
/// [`Metrics::serve_economics_refused`].
///
/// Not a metric label: per `adr/appendix-observability.md` § Reason splits the
/// convention is sibling counters, and the two regimes have distinct operator
/// remedies. `Warming` means the source still had allowance, so the ceiling was
/// the market price `max(sell, amortized)` and the market itself is above it —
/// the market is too hot to relay into. `Amortized` means the source's warming
/// allowance is spent (or warming is off), so the ceiling had already dropped
/// to the grief-proof floor — the node's own sell rate or margin config is too
/// tight. The regime is also emitted as a structured `debug!` field at the
/// refusal site so a single event carries the candidate rate and ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServeEconomicsRegime {
    /// The source had warming allowance: the ceiling was the market price and
    /// the market is above it.
    Warming,
    /// The source's warming allowance is spent (or warming is off): the ceiling
    /// was the amortized floor and this node's serve economics are too tight.
    Amortized,
}

/// Build a [`WatcherHook`] that invokes one `&self` recorder on a shared
/// `Metrics`, deduping the per-watcher `Box::new(move || metrics.foo())`
/// closures each watcher site would otherwise define (#1251). All
/// six `LogSink` sites route their liveness (`*_tick`) and panic
/// (`*_task_panicked`) hooks — and their down-family established/backoff edges —
/// through this helper (blacklist composes it with its readiness-gate closures).
/// Pass the recorder as a method path,
/// e.g. `metric_hook(&metrics, Metrics::slash_watcher_cycle_established)`.
pub(crate) fn metric_hook(metrics: &Arc<Metrics>, record: fn(&Metrics)) -> WatcherHook {
    let metrics = Arc::clone(metrics);
    Box::new(move || record(&metrics))
}

/// deCDN-specific counters and gauges surfaced at `/metrics`.
///
/// The group name becomes the metric-name prefix, so fields appear as
/// e.g. `decdn_probe_requests_total`.
#[derive(Debug, Serialize, Deserialize, MetricsGroup)]
#[metrics(default, name = "decdn")]
pub struct DecdnMetrics {
    /// Total probe requests served.
    pub probe_requests: Counter,
    /// Currently open QUIC connections.
    pub active_connections: Gauge,
    /// Currently open paid delivery streams, split by node direction.
    streams_active: Family<StreamLabels, Gauge>,
    /// Currently open inbound serve lanes (distinct `(pool, signer, provider)` keys).
    pub lanes_open: Gauge,
    /// Raw USDC still recoverable from the pools currently paying this node:
    /// `Σ (deposit − totalRedeemed)` over the distinct pools the redeemer
    /// planned lanes against, refreshed once per redeemer self-tick beside
    /// [`Self::unredeemed_usdc`]. Already-redeemed funds have left the pool, so
    /// this is the ceiling those pools can still pay — not their lifetime
    /// deposits. Operator-visible name: `decdn_pool_deposit_usdc`.
    pub pool_deposit_usdc: Gauge,
    /// Raw USDC in this node's own buyer wallet — what it can still escrow as a
    /// deposit when it opens a payment pool on its cache-miss leg.
    ///
    /// The buyer leg is the one part of the node that spends rather than earns,
    /// and nothing else reports on it: an operator who never funds this wallet
    /// sees a node that serves perfectly and silently buys nothing, because
    /// every `openPool` reverts on the ERC-20 transfer.
    ///
    /// Read at bootstrap and once per reclaim sweep (`RECLAIM_SWEEP_INTERVAL`),
    /// so it lags a spend — or an operator top-up, which is the reading that
    /// matters — by up to that long.
    ///
    /// Zero is meaningful only on a node with
    /// `cache.node_to_node_pull_through_enabled` on; a cache-only node never
    /// opens a pool and has no reason to hold USDC.
    pub buyer_wallet_usdc: Gauge,
    /// `decdn_buyer_lane_seed_failures_total`: this node could not establish a
    /// lane's already-paid watermark, so it refused the pull rather than
    /// resuming the lane from zero.
    ///
    /// A lane resumed from zero is stranded permanently, not for one pull: the
    /// pull persists its own progress on every exit path, which gives the lane a
    /// local row and stops the reseed ever running for that provider again. The
    /// node refuses instead, and this counts how often. A sustained rate means
    /// the chain lane or the buyer store is unhealthy and this node is buying
    /// nothing from the affected providers.
    ///
    /// Each refusal also moves `decdn_node_pull_pool_open_failures_total` once:
    /// this counter is the lane-seed share of that total. A buyer-store read
    /// fault under the seed lock counts here too.
    pub buyer_lane_seed_failures: Counter,
    /// `decdn_buyer_pool_adoption_failures_total`: bootstrap could not tell
    /// whether this node already owns a payment pool on chain, so it left the
    /// first cache miss to open one.
    ///
    /// Every increment is a chance that the node escrows a second deposit beside
    /// one it already holds. Adoption runs once per process, so this cannot
    /// self-correct before the next restart. Pair with
    /// `decdn_buyer_wallet_usdc`: the two together are what distinguishes "no
    /// pool to adopt" from "could not look".
    pub buyer_pool_adoption_failures: Counter,
    /// Total raw USDC this node holds in accepted vouchers that it has not yet
    /// redeemed on-chain — the sum of `owed − paid` across every inbound lane
    /// the redeemer plans to collect. Refreshed once per redeemer self-tick
    /// (`redeem_interval_secs`), not on every voucher, so it lags live accrual
    /// by up to one interval. Operator-visible name: `decdn_unredeemed_usdc`.
    pub unredeemed_usdc: Gauge,
    /// `cdn/client/v1` connections closed by the application-layer idle reaper
    /// (ADR 005 §Connection lifetime): no stream for `APP_IDLE_TIMEOUT` after the
    /// last one closed. A sustained rate flags peers parking streamless
    /// keep-alive'd connections — the abuse pattern the reaper exists to reclaim,
    /// invisible in `active_connections` alone. Operator-visible name:
    /// `decdn_client_idle_close_total`.
    pub client_idle_close: Counter,
    /// OTLP span-export batches whose export call failed (connect error,
    /// non-OK gRPC status, or timeout), counted at the batch exporter. Spans
    /// the batch queue drops when full, and spans a collector rejects inside
    /// an OK partial-success reply, are not counted: `0` does not mean no
    /// traces were lost. Stays `0` when `observability.otlp_endpoint` is unset
    /// or the node emits no spans. Operator-visible name:
    /// `decdn_otlp_export_failures_total`.
    pub otlp_export_failures: Counter,
    /// Seconds since node start.
    pub node_uptime_seconds: Gauge,
    /// JSON-RPC endpoint reachability per the watchdog task. `1` =
    /// reachable, `0` = unreachable. `adr/appendix-observability.md` names operational gauges in
    /// the `decdn_*` family; this is the per-tick mirror of the startup
    /// `check_rpc_reachability` probe so dashboards/alerts can fire on
    /// a sustained outage rather than relying on a one-shot startup line.
    pub rpc_healthy: Gauge,
    /// Connections rejected because the global concurrency semaphore was
    /// exhausted. Field has no `_total` suffix because the `OpenMetrics`
    /// encoder appends it automatically; the operator-visible name is
    /// `decdn_dispatch_rejected_global_total`.
    pub dispatch_rejected_global: Counter,
    /// Connections rejected by the per-source rate limiter. Operator-
    /// visible name: `decdn_dispatch_rejected_per_source_total`.
    pub dispatch_rejected_per_source: Counter,
    /// Currently in-flight QUIC handler tasks holding a dispatch permit.
    pub dispatch_in_flight: Gauge,
    /// Connections accepted on a relay-only path (no resolvable peer
    /// IP) while the per-source layer was enabled. The per-source rate
    /// limit cannot be enforced for these — operators chasing
    /// `dispatch_rejected_per_source` anomalies need this counter to
    /// distinguish "the layer didn't fire" from "the layer wasn't
    /// applicable." Operator-visible name:
    /// `decdn_dispatch_per_source_skipped_no_addr_total`.
    pub dispatch_per_source_skipped_no_addr: Counter,
    /// Inbound connections (all ALPNs) that arrived with a direct IP path.
    /// Operator-visible name: `decdn_inbound_connections_direct_total`.
    pub inbound_connections_direct: Counter,
    /// Inbound connections (all ALPNs) that arrived over the relay only, with
    /// no direct IP path at accept time. Expected on a node behind NAT; on a
    /// node with a public address it means dialers did not find or reach that
    /// address. Operator-visible name:
    /// `decdn_inbound_connections_relayed_total`.
    pub inbound_connections_relayed: Counter,
    /// `1` when the host's default route has a public source address (ADR 001
    /// § Node Discovery), `0` when the node is behind NAT. Sampled at
    /// bring-up. Operator-visible name: `decdn_node_public_address`.
    pub node_public_address: Gauge,
    /// `decdn_probe_hold_unavailable_total{reason}` per the canonical metric
    /// registry (`adr/appendix-observability.md` — the authoritative naming
    /// source, superseding informal ADR-005 references): a probe for a present
    /// blob that got no eviction hold, broken out by cause on the `reason`
    /// label (#1443, collapsing the former `probe_hold_violations` /
    /// `probe_holds_disabled` / `probe_stake_lane_reserved` counters).
    ///
    /// For `exhausted` / `stake_lane_reserved` the node still advertises
    /// (`has_blob: true`) and simply forgoes the hold — the blob may be
    /// LRU-evicted before the pull, costing one wasted round trip. That is not
    /// a slash (ADR 014 pairs no offense with a miss) and not a reputation
    /// penalty either: a `NotFound` is classified `Transient` and suppresses
    /// the (peer, hash) pair without scoring the peer. `disabled` is the
    /// operator opt-out (`max_probe_holds == 0`) that answers `has_blob: false`
    /// for store-backed content. See
    /// [`ProbeHoldUnavailableReason`] for what each value means and which one
    /// the "raise `max_probe_holds`" alert fires on; all three children are
    /// materialized at startup by [`Metrics::new`] so each series is exported
    /// at zero rather than appearing only on first increment. Field has no
    /// `_total` suffix because the `OpenMetrics` encoder appends it.
    ///
    /// **This is the documented exception, not the convention** (#1475) — the
    /// one labeled *reason split*, not the crate's only `Family`:
    /// `streams_active` and `staker_set_active_by_region` label other axes.
    /// Every other reason-style split in this crate — `dispatch_rejected_*`, `probe_rate_limit_rejected_*`,
    /// and `pool_open_failures_*` — fans out to sibling unlabeled counters,
    /// and that stays the default for a new split: sibling counters need no
    /// `EncodeLabelSet` type, no pre-materialization to keep a series
    /// exporting at zero, and no alert rewrite when a reason is added.
    ///
    /// The exception is earned because the three values share one *aggregate*
    /// and one budget axis: an operator asks "are probe holds unavailable?"
    /// first and drills into which value second — the shape a label serves and
    /// sibling counters make awkward. They pointedly do NOT share an alert:
    /// `DecdnProbeHoldViolations` filters to `reason="exhausted"` precisely
    /// because [`ProbeHoldUnavailableReason::Disabled`] is an intentional
    /// operator choice and alerting on it would be nonsensical, and
    /// `StakeLaneReserved` has its own knob. Being able to express that filter
    /// is itself part of what the label buys. A split whose values have
    /// unrelated remedies *and* no meaningful aggregate gains nothing from a
    /// label and should stay siblings.
    probe_hold_unavailable: Family<ProbeHoldUnavailableLabels, Counter>,
    /// `decdn_probe_hold_slots_used` (registry): current active
    /// probe-triggered eviction holds (distinct held blobs), ADR 005
    /// §Probe-triggered eviction hold. Sampled from the cache engine at
    /// scrape time, so it tracks hold expiry with no probe traffic; pair
    /// with `probe_hold_slots_max` for a saturation ratio.
    pub probe_hold_slots_used: Gauge,
    /// `decdn_probe_hold_slots_max` (registry, mandatory): the configured
    /// `max_probe_holds` budget. Set once at startup. Pairs with
    /// `probe_hold_slots_used` so dashboards can alert on a saturation
    /// ratio rather than an absolute count.
    pub probe_hold_slots_max: Gauge,
    /// `cdn/dht/v1` requests rejected by the per-peer (`NodeId`) token
    /// bucket (ADR 022 §DHT Rate Limiting). One Counter per layer to match
    /// the existing `dispatch_rejected_*` convention: a plain counter field
    /// carries no label dimension (a labeled series would need a `Family`). Operator-visible name:
    /// `decdn_dht_rate_limit_rejected_per_peer_total`.
    pub dht_rate_limit_rejected_per_peer: Counter,
    /// `cdn/dht/v1` requests rejected by the per-IP token bucket. Sibling
    /// to `dht_rate_limit_rejected_per_peer` — see its docs. Operator-
    /// visible name: `decdn_dht_rate_limit_rejected_per_ip_total`.
    pub dht_rate_limit_rejected_per_ip: Counter,
    /// `cdn/dht/v1` requests rejected by the global token bucket. Sibling
    /// to `dht_rate_limit_rejected_per_peer` — see its docs. Operator-
    /// visible name: `decdn_dht_rate_limit_rejected_global_total`.
    pub dht_rate_limit_rejected_global: Counter,
    /// `retain_recent` sweeps of the per-IP DHT keyed-limiter map that
    /// actually ran (single-flight CAS won, layer enabled). Bump from
    /// both the lazy-prune path in `DhtRateLimiter::check` and the
    /// periodic `gc_per_ip` GC task. Persistent growth without a matching
    /// drop in `decdn_dht_rate_limit_tracked_per_ip` means the limiter
    /// is observing churn faster than `retain_recent` can release entries
    /// — investigate cap saturation. Operator-visible name:
    /// `decdn_dht_rate_limit_prune_sweeps_per_ip_total` (#645).
    pub dht_rate_limit_prune_sweeps_per_ip: Counter,
    /// Sibling of `dht_rate_limit_prune_sweeps_per_ip` for the per-peer
    /// (`NodeId`) keyed-limiter map. Operator-visible name:
    /// `decdn_dht_rate_limit_prune_sweeps_per_peer_total` (#645).
    pub dht_rate_limit_prune_sweeps_per_peer: Counter,
    /// Current size of the per-IP DHT keyed-limiter map after the most
    /// recent prune (lazy or periodic). Pair with
    /// `decdn_dht_rate_limit_prune_sweeps_per_ip_total` to detect cap
    /// saturation. Updated post-prune so the value is at most one
    /// `DHT_RATE_LIMIT_GC_INTERVAL` stale on quiet nodes; reads `0`
    /// until the first successful prune (lazy or periodic) after
    /// startup. Operator-visible name:
    /// `decdn_dht_rate_limit_tracked_per_ip` (#645).
    pub dht_rate_limit_tracked_per_ip: Gauge,
    /// Sibling of `dht_rate_limit_tracked_per_ip` for the per-peer map.
    /// Operator-visible name: `decdn_dht_rate_limit_tracked_per_peer` (#645).
    pub dht_rate_limit_tracked_per_peer: Gauge,
    /// `cdn/probe/v1` requests rejected by the per-peer (`NodeId`) token
    /// bucket (ADR 005 §Probe rate limiting). One Counter per layer to match
    /// the existing `dht_rate_limit_rejected_*` / `dispatch_rejected_*`
    /// convention; a plain counter field carries no label dimension (a labeled
    /// series would need a `Family`);
    /// operators recover the rolled-up rate with
    /// `sum(rate({__name__=~"decdn_probe_rate_limit_rejected_(per_peer|per_ip|global)_total"}[1m]))`.
    /// Operator-visible name: `decdn_probe_rate_limit_rejected_per_peer_total`.
    pub probe_rate_limit_rejected_per_peer: Counter,
    /// `cdn/probe/v1` requests rejected by the per-IP token bucket. Sibling
    /// to `probe_rate_limit_rejected_per_peer` — see its docs. Operator-
    /// visible name: `decdn_probe_rate_limit_rejected_per_ip_total`.
    pub probe_rate_limit_rejected_per_ip: Counter,
    /// `cdn/probe/v1` requests rejected by the global token bucket. Sibling
    /// to `probe_rate_limit_rejected_per_peer` — see its docs. Operator-
    /// visible name: `decdn_probe_rate_limit_rejected_global_total`.
    pub probe_rate_limit_rejected_global: Counter,
    /// `retain_recent` sweeps of the per-IP probe keyed-limiter map that
    /// actually ran (single-flight CAS won, layer enabled). Bumped from both
    /// the lazy-prune path in the limiter's `check` and the periodic
    /// `gc_per_ip` GC task. Operator-visible name:
    /// `decdn_probe_rate_limit_prune_sweeps_per_ip_total` (#645).
    pub probe_rate_limit_prune_sweeps_per_ip: Counter,
    /// Sibling of `probe_rate_limit_prune_sweeps_per_ip` for the per-peer
    /// (`NodeId`) keyed-limiter map. Operator-visible name:
    /// `decdn_probe_rate_limit_prune_sweeps_per_peer_total` (#645).
    pub probe_rate_limit_prune_sweeps_per_peer: Counter,
    /// Current size of the per-IP probe keyed-limiter map after the most
    /// recent prune (lazy or periodic). Pair with
    /// `decdn_probe_rate_limit_prune_sweeps_per_ip_total` to detect cap
    /// saturation. Operator-visible name:
    /// `decdn_probe_rate_limit_tracked_per_ip` (#645).
    pub probe_rate_limit_tracked_per_ip: Gauge,
    /// Sibling of `probe_rate_limit_tracked_per_ip` for the per-peer map.
    /// Operator-visible name: `decdn_probe_rate_limit_tracked_per_peer` (#645).
    pub probe_rate_limit_tracked_per_peer: Gauge,
    /// `cdn/dht/v1` request handling failed after the request was admitted
    /// by the rate limiter — frame decode error, response write error,
    /// read timeout, etc. Tracked separately from the rate-limit
    /// rejections so an operator running with default `RUST_LOG=info` can
    /// see the rate of "I accepted this request and then it broke"
    /// failures without scraping debug-level logs. Operator-visible name:
    /// `decdn_dht_requests_failed_total`.
    pub dht_requests_failed: Counter,
    /// `Store` rejected because `holder != authenticated NodeId` (ADR
    /// 022 §STORE Flow line 140). This is the lying-`holder` attack
    /// signal — a non-zero value means at least one peer is trying to
    /// publish records on behalf of someone else's `NodeId`. Operator-
    /// visible name: `decdn_dht_store_rejected_holder_mismatch_total`.
    pub dht_store_rejected_holder_mismatch: Counter,
    /// `Store` rejected because the holder is not in the active-staker
    /// set (ADR 022 §STORE Flow line 140). Sustained growth from many
    /// distinct holders without matching `decdn_dht_store_accepted`
    /// growth is a Sybil-attempt indicator. Operator-visible name:
    /// `decdn_dht_store_rejected_non_staked_total`.
    pub dht_store_rejected_non_staked: Counter,
    /// `Store` rejected because the publisher is at the per-publisher
    /// quota (ADR 022 §Content Records and TTL — 200-record hard cap).
    /// Normal load shouldn't trip this. Operator-visible name:
    /// `decdn_dht_store_rejected_quota_total`.
    pub dht_store_rejected_quota: Counter,
    /// `Store` admitted to the record store (newly inserted OR
    /// refreshed). Pair with the three `dht_store_rejected_*` counters
    /// to compute admission rate. Operator-visible name:
    /// `decdn_dht_store_accepted_total`.
    pub dht_store_accepted: Counter,
    /// `BatchStore` requests that passed stage-1 rate limiting and the
    /// batch-level `holder == authenticated NodeId` check, i.e. reached
    /// per-hash admission (ADR 022 §STORE Flow Batched STORE, #648). The
    /// per-hash outcomes still land in the shared `dht_store_*` counters
    /// above, so this counts batches, not hashes — pair the two to see
    /// average admitted batch size and batch adoption. Operator-visible
    /// name: `decdn_dht_batch_store_received_total`.
    pub dht_batch_store_received: Counter,
    /// Hashes in a `BatchStore` acked `false` purely because the
    /// two-stage rate-limit budget was exhausted before reaching them
    /// (the `n - k` tail, ADR 022 §Batch token accounting / AC 18) — NOT
    /// counted as a per-hash `Store` rejection because they were never
    /// processed. Sustained growth means publishers are sending batches
    /// larger than the per-peer burst allows in one window; the publisher
    /// retries the tail. Operator-visible name:
    /// `decdn_dht_batch_store_hashes_deferred_rate_limit_total`.
    pub dht_batch_store_hashes_deferred_rate_limit: Counter,
    /// Times the republisher lost its cache-commit event window — the
    /// `subscribe_inserts` broadcast channel overflowed and reported
    /// `Lagged`. Counts lag *events*, not distinct store walks: a lag
    /// arriving while a sweep runs is folded into that sweep's next pass,
    /// so this can outrun the number of walks performed. The cache does not
    /// retain the missed hashes, so without a sweep a blob committed inside
    /// the window stays undiscoverable until the node restarts (ADR 022
    /// §Bootstrap). A nonzero rate means commits are outrunning the
    /// scheduler; discovery still converges, but up to one cold-start
    /// window late. Operator-visible name:
    /// `decdn_dht_republish_lag_sweeps_total`.
    pub dht_republish_lag_sweeps: Counter,

    /// Lag events folded into a sweep that was already running or already
    /// queued, rather than starting a walk of their own. The sibling that splits
    /// `decdn_dht_republish_lag_sweeps_total` into the lags that claimed an idle
    /// slot and the lags that did not.
    ///
    /// **Not** a walk count, and the difference is not one either: the slot has
    /// a single queued position, so any number of lags arriving behind a running
    /// worker all count here and collapse into one further pass. The ratio is
    /// what reads: coalesced climbing at the lag rate, with the difference flat,
    /// means the slot is never released. Operator-visible name:
    /// `decdn_dht_republish_lag_sweeps_coalesced_total`.
    pub dht_republish_lag_sweeps_coalesced: Counter,

    /// Lag-sweep walks that died in a panic instead of completing. The walk
    /// runs in its own task, so a panic releases the slot through the normal
    /// path and a queued request still runs — but the dead pass seeded only
    /// what it reached before it died. A rate here with
    /// `decdn_dht_republish_sweep_reseeded_total` flat means every sweep dies
    /// before repairing anything, which nothing else surfaces.
    /// Operator-visible name:
    /// `decdn_dht_republish_lag_sweep_panics_total`.
    pub dht_republish_lag_sweep_panics: Counter,
    /// Hashes a lag sweep newly scheduled for republish. Seeding is
    /// idempotent, so this counts the repair, not the walk: a sweep that
    /// finds every held hash already scheduled adds zero. Operator-visible
    /// name: `decdn_dht_republish_sweep_reseeded_total`.
    pub dht_republish_sweep_reseeded: Counter,
    /// Bulk republish seeds that could not walk the local blob store and
    /// covered only the origin-held half — both the boot-time cold start
    /// and the lag sweep, which share one derivation. The seed degrades
    /// rather than failing, so nothing else surfaces this: blobs held only
    /// in the store stay un-republished until a later sweep re-walks the
    /// store successfully, or until a restart. A boot-path failure is not
    /// permanent — the next lag sweep recovers it — but nothing schedules
    /// one, so a node with no further lag stays degraded for its lifetime.
    /// Operator-visible name:
    /// `decdn_dht_republish_seed_store_walk_failures_total`.
    pub dht_republish_seed_store_walk_failures: Counter,

    /// Stored hashes a bulk republish seed could not put to its origin — the
    /// ownership test an origin-only node applies to every blob in its store,
    /// answered by neither "yours" nor "not yours".
    ///
    /// The origin-side twin of
    /// `decdn_dht_republish_seed_store_walk_failures_total`, and it degrades the
    /// same way: a hash that cannot be confirmed is left out of the announce set
    /// rather than advertised, because announcing on a transport blip risks
    /// offering content the serve gate then refuses. So an unreachable remote
    /// origin quietly shrinks the announce set of a node that holds the content.
    /// Distinct from `decdn_cache_origin_probe_failures_total`, which counts the
    /// rescan's probes rather than the seed's. Operator-visible name:
    /// `decdn_dht_republish_seed_origin_probe_failures_total`.
    pub dht_republish_seed_origin_probe_failures: Counter,
    /// Redeem hints the voucher-accept path could not enqueue because the
    /// bounded advisory channel was full (`try_send` → `Full`), counted once
    /// per dropped hint (#751). A hint is advisory — the next voucher re-hints
    /// and the redeemer self-tick / shutdown close still redeem — so a few drops
    /// are benign, but a sustained non-zero rate means the redeemer is not
    /// keeping up with channel fan-out and threshold redemption is leaning on
    /// the slow self-tick. Operator-visible name:
    /// `decdn_redeem_hints_dropped_total`.
    pub redeem_hints_dropped: Counter,
    /// Redeem hints the redeemer dropped because their lane is parked (#2340).
    /// A lane parks when its hint-path redemption hits a chain fault — a
    /// failed pre-redeem watermark read or a `redeemMany` that did not land,
    /// an RPC fault or a revert alike. Each sweep (self-tick or serve cutoff)
    /// retries the parked lanes with every other lane, and only a sweep that
    /// finishes without a chain fault releases them. So an RPC outage costs one
    /// failed attempt per lane per interval: the sweep's batched retry. Hints
    /// for a parked lane stay dropped until a sweep finishes without a chain
    /// fault. A non-zero rate tracks chain faults on the settlement path; read it beside
    /// `decdn_onchain_tx_send_failed_total` and
    /// `decdn_redemption_reconcile_failures_total`. Operator-visible name:
    /// `decdn_redeem_hints_parked_total`.
    pub redeem_hints_parked: Counter,
    /// Download-receipt audit writes dropped because the bounded writer queue
    /// was full (`try_send` → `Full`, #803), counted once per dropped receipt.
    /// The receipt log is audit-only and the payment already advanced the lane
    /// watermark, so a drop never affects settlement — but a sustained non-zero
    /// rate means the receipt writer cannot keep up with disk I/O (a slow or full
    /// `data_dir`, the end-state of #802) and audit/dispute records are being
    /// lost. Operator-visible name:
    /// `decdn_receipt_writes_dropped_total`.
    pub receipt_writes_dropped: Counter,
    /// ADR 041 warming serve credits dropped before they reached the ledger,
    /// counted once per dropped credit. Either the bounded aggregator queue was
    /// full (`try_send` → `Full`) or the aggregator was gone (`Closed`). A drop
    /// is conservative — it leaves the source's ledger lower than reality,
    /// never higher — so it can only block speculative warming,
    /// never over-fund it. But a sustained non-zero rate means warming is being
    /// throttled by lost bookkeeping rather than by real losses, and a rate that
    /// tracks the serve rate exactly means the aggregator task is gone and every
    /// source will drift to blocked. Operator-visible name:
    /// `decdn_warming_credits_dropped_total`.
    pub warming_credits_dropped: Counter,
    /// ADR 041 warming serve credits the aggregator applied to a source ledger.
    /// The success sibling of `warming_credits_dropped`, and the reason a zero
    /// drop count means something: zero drops beside zero applies is a node
    /// whose serve path never reached the ledger at all — an unwired
    /// [`crate::warming_allowance::WarmingCreditSink`] leaves the inert
    /// `NoopWarmingCreditSink` in place, which drops nothing because it enqueues
    /// nothing. A node serving tagged blobs must show this climbing.
    /// Operator-visible name: `decdn_warming_credits_applied_total`.
    pub warming_credits_applied: Counter,
    /// Redemption steps that failed: a `redeemMany` chunk that reverted
    /// on-chain or that the RPC refused at `send`, a lane whose plan failed,
    /// or a hint-path lane that could not be read from the lane store. A
    /// chunk whose receipt stays unconfirmed after the by-hash fetch does not
    /// count here; it counts into `decdn_onchain_tx_receipt_failed_total` or
    /// `decdn_onchain_tx_timeout_total`. Each failure is otherwise only a
    /// single `warn!`; a sustained rate means accrued earnings are not being
    /// redeemed and warrants investigating the RPC / wallet. Operator-visible
    /// name: `decdn_redemption_failures_total`.
    pub redemption_failures: Counter,
    /// Lanes the redeemer held or dropped for the pool's chain-observed
    /// solvency (`pool_is_redeemable`, ADR 003): a drained `Open` pool is held
    /// for a top-up, and a drained `Closing` pool, or one whose close deadline
    /// falls within the redeemer's landing slack, is dropped.
    /// Counted once per skipped lane per planning pass. A sustained non-zero
    /// rate means real unredeemed value is stuck behind pools this node can no
    /// longer collect from — worth checking against `pool_deposit_usdc` for
    /// which pools are dry. Operator-visible name:
    /// `decdn_redemption_skipped_insolvent_total`.
    pub redemption_skipped_insolvent: Counter,
    /// Lanes with value owed that the redeemer skipped because the signer's
    /// capability has expired, or expires within the redeemer's landing slack
    /// (ADR 003 §Revocation). The contract pays 0 for an expired capability, so
    /// the redemption would only spend gas. Counted once per skipped lane per
    /// planning pass. A lane with nothing owed is not counted. The serve path
    /// stops accepting vouchers one redeem interval plus that slack before
    /// expiry, so a sustained rate means a lane earned near its capability's
    /// expiry and no sweep redeemed it in time: check for failed or
    /// floor-deferred redemptions. Operator-visible name:
    /// `decdn_redemption_skipped_expired_total`.
    pub redemption_skipped_expired: Counter,
    /// Lanes dropped from a redeem batch by the pre-submit on-chain watermark
    /// reconciliation because the chain already shows them settled to their
    /// claim value — a `redeemMany` the contract would silently no-op, caught
    /// before it costs gas. Counted once per dropped lane. A sustained non-zero
    /// rate means the event-fed paid cache is lagging the chain (or a second
    /// actor is redeeming the same lanes); a burst right after a restart is the
    /// expected catch-up. Operator-visible name:
    /// `decdn_redemption_reconciled_skip_total`.
    pub redemption_reconciled_skip: Counter,
    /// Pre-submit on-chain watermark batches that landed and were reconciled
    /// against the chain. Counted once per `getWatermarks` batch that decoded,
    /// so a redeem sweep over more lanes than the read batch caps contributes
    /// one per batch. This is the *attempt* signal
    /// `redemption_reconciled_skip` cannot give: that counter is legitimately
    /// zero on a healthy sweep with nothing to drop, so zero alone cannot
    /// separate "the reconciliation ran and found nothing" from "the
    /// reconciliation never ran". Read the two together — a redeemer planning
    /// lanes with this flat at zero is not reconciling at all. Operator-visible
    /// name: `decdn_redemption_reconcile_ok_total`.
    pub redemption_reconcile_ok: Counter,
    /// Pre-submit on-chain watermark batches that did not land — an RPC error,
    /// a `timed` timeout, or a return whose length did not match the batch.
    /// Counted once per failed batch. The read is fail-open (the contract's own
    /// `claimed <= w.amount` guard is the backstop), so a sustained rate costs
    /// gas rather than correctness: the seller submits `redeemMany` batches it
    /// never checked against the chain. A sustained non-zero rate means the
    /// chain RPC is rejecting or timing out the batch read — check the
    /// endpoint's `eth_call` response-size and gas ceilings against the read
    /// batch size. Operator-visible name:
    /// `decdn_redemption_reconcile_failures_total`.
    pub redemption_reconcile_failures: Counter,
    /// Buyer-side reclaim-sweep passes (`reclaim_once`) that failed — a failed
    /// buyer store read, a failed `getPool`/`reclaim` RPC, a receipt wait, an
    /// on-chain revert, or a failed store write when clearing the local record
    /// after a reclaim (#906). Each is otherwise only a single `warn!` per hourly
    /// sweep; a sustained rate means a closed pool's residual is not being
    /// recovered past its dispute deadline (check the gas wallet / RPC).
    /// Operator-visible name:
    /// `decdn_buyer_reclaim_failures_total`.
    pub buyer_reclaim_failures: Counter,
    /// Buyer-pool rows omitted from a successful store hydration because
    /// their persisted values could not be decoded. Counted once per skipped
    /// row per load attempt, so a persistent malformed row keeps the alert
    /// active while healthy rows continue through buyer maintenance. Operator-
    /// visible name:
    /// `decdn_buyer_pool_store_skipped_undecodable_records_total`.
    pub buyer_pool_store_skipped_undecodable_records: Counter,
    /// Background low-water top-ups (#1146) that landed: the node's buyer pool,
    /// whose remaining deposit had fallen below its 20% low-water mark, was
    /// re-funded to the working deposit, so sustained miss pulls to every provider
    /// keep flowing instead of silently stranding on a spent-down pool. The
    /// healthy signal of the auto-refill path. Operator-visible name:
    /// `decdn_buyer_topup_ok_total`.
    pub buyer_topup_ok: Counter,
    /// Background low-water top-ups (#1146) that did NOT cleanly land: the standing
    /// allowance re-approval failed, the `topUp` submission/receipt errored or
    /// reverted, OR the `topUp` landed on-chain but the local pool row vanished
    /// or rotated during the RPC, or the credit itself faulted (escrowed-but-untracked
    /// — `fund_pool` logs the tx at error! for reconcile). Folding the untracked case in here — rather than
    /// counting it as `buyer_topup_ok` — means an operator alerting on this metric
    /// sees stranded deposits. The refill is best-effort (the pool is simply left
    /// un-topped), but a sustained rate means the node's pool is not being refilled
    /// and pull-through to every provider degrades as its deposit drains (check the
    /// gas wallet / RPC / USDC balance). Operator-
    /// visible name: `decdn_buyer_topup_failure_total`.
    pub buyer_topup_failure: Counter,
    /// Lane-store reconciliation the settlement watcher could not apply from a
    /// poll tick: a `forget_lane` store write that failed while dropping a
    /// reclaimed pool's lanes on a `PoolReclaimed` event. The failure is
    /// swallowed (logged, and the cursor advances past it), so a non-zero rate
    /// flags a struggling lane store or RPC rather than a stuck settlement.
    /// Operator-visible name: `decdn_watcher_persist_failures_total`.
    pub watcher_persist_failures: Counter,
    /// Lane-store background flushes that failed (the dirty set is retained and
    /// retried next tick). A sustained non-zero rate means the store cannot be
    /// fsynced — the frontier-only replay surface is widening. Operator-visible
    /// name: `decdn_lane_flush_failures_total`.
    pub lane_flush_failures: Counter,
    /// Slashes detected against this node's operator by the slash watcher
    /// (`SlashJudge.Slashed`), counting each distinct `slashId` once across the
    /// bring-up backfill and the live stream (#1032). A non-zero value means the
    /// operator was slashed and should consider `decdn appeal slash` within the
    /// 30-day window. Operator-visible name: `decdn_slashes_detected_total`.
    pub slashes_detected: Counter,
    /// `decdn_slash_resync_failures_total`: times the slash watcher's periodic
    /// re-enumeration could not read chain state and kept the detected-slash
    /// set it already had. The resync reports success upward so the event tail
    /// keeps running, so a persistently failing repair moves nothing else.
    /// Pairs with the `warn!` in `slash_watcher`'s `SlashSink::on_tick_complete`.
    /// Field has no `_total` suffix because the `OpenMetrics` encoder appends it.
    pub slash_resync_failures: Counter,
    /// `decdn_slash_watcher_restarts_total` (#1032): distinct drift windows the
    /// slash-detection watcher has entered, bumped once on the edge into the
    /// error/backoff state. Pairs with `slash_watcher_down_seconds` to tell one
    /// long outage from repeated flapping. Mirrors `staker_set_watcher_restarts`.
    pub slash_watcher_restarts: Counter,
    /// `decdn_slash_watcher_down_seconds` (#1032): seconds the slash-detection
    /// watcher has been stuck in its per-tick backoff loop (`0` on a healthy
    /// cycle), recomputed at scrape from `slash_watcher_down_since`. Mirrors the
    /// other chain watchers — since `admin_v1_slashes` is always wired, this is
    /// the only signal that a wedged watcher could be silently missing slashes.
    pub slash_watcher_down_seconds: Gauge,
    /// `decdn_staker_set_watcher_restarts_total` (#783): distinct drift
    /// windows the [`crate::dht::chain_staker_set`] watcher has entered —
    /// bumped once on the *transition* from a healthy cycle into the
    /// error/backoff state, NOT on every backoff iteration of one continuous
    /// outage. Each increment therefore brackets exactly one window in which
    /// the cached active-staker set can drift from chain state — the module's
    /// documented mid-run degradation. Because the stake-lane probe
    /// reservation (#757) and the DHT `Store` admission path both read that
    /// cached set, sustained restarts are revenue-impacting, not just a
    /// discovery-health blip. The loop-level `tracing::warn!` in
    /// `multiplexed_poller::run` (`"watcher RPC error; restarting after backoff"`)
    /// is emitted in the same failed-tick `Err` arm that fires the `on_backoff`
    /// hook this counter hangs off, and carries the underlying error; this
    /// counter is the alertable rate. An idle poll tick (head has not advanced /
    /// no new logs) is NOT an error and does not bump this. Field has no
    /// `_total` suffix because the `OpenMetrics` encoder appends it.
    pub staker_set_watcher_restarts: Counter,
    /// `decdn_staker_set_watcher_resolve_failures_total` (#788): times an
    /// operator-indexed event (`Reinstated` / `UnbondingRequested`) was
    /// dropped because the follow-up `nodeIdOf(operator)` RPC failed. A dropped
    /// resolution leaves the cached active set out of sync with chain state for
    /// that operator until a later event, or until the watcher's cadence-gated
    /// `getRegisteredNodes` re-enumeration corrects it — exactly the silent
    /// drift these #783/#788 metrics exist to surface. That resync shortens the
    /// window rather than bounding it: it is skipped while the route is errored,
    /// and a failed read defers another interval.
    /// Unlike a stream-level error, this does NOT trip a backoff/restart, so it
    /// would otherwise move no metric at all. Pairs with the per-failure
    /// `warn!` in [`crate::dht::capacity_bond_registry`]'s
    /// `RegistrySink::on_operator_change`. Field has no `_total` suffix because
    /// the `OpenMetrics` encoder appends it.
    pub staker_set_watcher_resolve_failures: Counter,

    /// `decdn_capacity_bond_registry_resync_failures_total`: times the
    /// capacity-bond registry's periodic re-enumeration of `getRegisteredNodes`
    /// could not read chain state and kept the projections it already had.
    ///
    /// That re-enumeration is the only systematic repair for an active-staker
    /// set that has drifted from chain state, and it deliberately reports
    /// success upward — returning an error would mark the watcher route errored
    /// and stall event pickup, trading a stale set for no updates at all. So a
    /// persistently failing resync moves nothing else. Two behaviours compound
    /// it, both correlated with the RPC fault that caused the drift: the resync
    /// is skipped entirely while the route is errored, and a failed read stamps
    /// the cadence clock anyway, deferring the next attempt a further interval.
    /// Pairs with the liveness gauge below, which is what catches the skipped
    /// case. Field has no `_total` suffix because the `OpenMetrics` encoder
    /// appends it.
    pub capacity_bond_registry_resync_failures: Counter,
    /// `decdn_staker_set_watcher_down_seconds` (#783, semantics corrected
    /// #788): true downtime — seconds the staker-set watcher has been in the
    /// error/backoff state, i.e. failing its `eth_getLogs` poll tick. Reads `0`
    /// for the entire life of any established cycle, however long or quiet (a
    /// healthy poll loop persists indefinitely, so this must NOT measure cycle
    /// age), and climbs only while the loop is between a failed cycle and the
    /// next successful re-establishment. Reads `0` until the first cycle is
    /// established after bootstrap. Recomputed at scrape time from a monotonic
    /// `down_since` timestamp. A poisoned lock reports `i64::MAX` (not `0`) so
    /// it trips the alert rather than masking an in-progress outage — the
    /// conservative direction for a downtime gauge. This is the drift-window
    /// depth gauge that `decdn_rpc_healthy` (a different watchdog) does not
    /// cover.
    pub staker_set_watcher_down_seconds: Gauge,
    /// `decdn_staker_set_active_count` (#783): current cached active-staker
    /// set size, sampled on every membership change the watcher applies. Pairs
    /// with `decdn_staker_set_watcher_down_seconds` to spot a collapse —
    /// e.g. the count holding flat while down-seconds climbs means the cache
    /// is frozen, not that the network genuinely lost operators.
    pub staker_set_active_count: Gauge,
    /// `decdn_staker_set_active_by_region{node_region}`: active-staker set
    /// size per operator-declared region (ADR 030), the source of the
    /// reference network map. Each node's registry holds the whole network, so
    /// one scrape maps every active node; aggregate across nodes with `max`,
    /// not `sum`. A region that drops to zero loses its series rather than
    /// exporting `0`, and the name is absent while no active node has a valid
    /// region. Label values pass [`Region::parse`], so the ISO
    /// allowlist bounds cardinality and an unparsable on-chain `regionHint`
    /// never reaches a label. Like `streams_active{direction}`, this labels a
    /// dimension, not a reason split, so the sibling-counter convention does
    /// not apply.
    staker_set_active_by_region: Family<NodeRegionLabels, Gauge>,
    /// `decdn_staker_set_active_unknown_region`: active stakers whose
    /// `regionHint` is empty or not an accepted region code. With the
    /// per-region family it sums to `decdn_staker_set_active_count` after each
    /// completed watcher tick. The count moves on each event and the region
    /// split at the end of the tick, so the two can differ between ticks and
    /// while the watcher is in backoff.
    pub staker_set_active_unknown_region: Gauge,
    /// `decdn_node_address_directory_size` (#831): current count of cached
    /// `NodeId → operator address` bindings the node-to-node pull path resolves
    /// against ([`crate::dht::node_address::ChainNodeAddressDirectory`]).
    /// Sampled on every `NodeRegistered` / `NodeDeregistered` the watcher
    /// applies. A binding the watcher misses (drift window) means the
    /// orchestrator cannot pay that provider and skips it, so a collapsed or
    /// frozen count here directly caps reachable upstream providers.
    pub node_address_directory_size: Gauge,
    /// `decdn_node_pull_attempts_total` (#831): node-to-node cache-miss pull
    /// orchestrations that found ≥1 candidate provider to try. Denominator for
    /// the success/corruption/unreachable rates below.
    pub node_pull_attempts: Counter,
    /// `decdn_node_pull_first_byte_seconds`: time to first byte of one paid
    /// node-to-node pull leg, from the start of its open (a dial included, when
    /// the node holds no warm connection to the peer) to the first bao bytes
    /// read off the upstream stream. One observation per leg. An adopted header
    /// handshake keeps the start of the handshake open, so its window includes
    /// the handshake. A leg that reads no bytes records nothing.
    #[default(Histogram::new(FIRST_BYTE_BUCKETS.to_vec()))]
    pub node_pull_first_byte_seconds: Histogram,
    /// `decdn_node_pull_success_total` (#831): pulls that delivered verified
    /// bytes from an upstream node.
    pub node_pull_success: Counter,
    /// `decdn_node_pull_no_providers_total` (#831): misses where discovery
    /// (DHT plus the origin-directory fallback) surfaced no provider — the blob
    /// is unavailable on the network, not a pull failure.
    pub node_pull_no_providers: Counter,
    /// `decdn_probe_cache_hits_total` (#1165): cache-miss pulls whose candidate
    /// walk STARTED from a live ADR 001 §Probe cache entry with at least one
    /// still-selectable provider. The DHT lookup and probe fanout are skipped —
    /// unless every cached provider fails, in which case the same fetch either
    /// falls through to a fresh lookup + probe (if attempt budget remains) or
    /// returns a clean miss (if the cached providers exhausted the budget first,
    /// the common case since an entry holds up to 10 providers but the budget is
    /// 3) — either way it stays counted here (the hit is "the cache had
    /// something worth trying", not "the cache delivered"). Field has no
    /// `_total` suffix because the
    /// `OpenMetrics` encoder appends it. With `probe_cache_misses` this is the
    /// hit ratio the TTL exists to buy; a ratio near zero means the TTL is
    /// shorter than the request inter-arrival time for hot blobs and the cache
    /// is pure overhead.
    pub probe_cache_hits: Counter,
    /// `decdn_probe_cache_misses_total` (#1165): cache-miss pulls that had to run
    /// a fresh DHT lookup + probe. Counts an entry that was absent, expired, OR
    /// fully suppressed (every cached provider negative-cached, wedged, no longer
    /// an active staker, or otherwise unselectable), and on the ranged pull leg an
    /// entry whose providers do not cover every block of the blob (#2195) — all
    /// cost the same network work, which is what this measures.
    pub probe_cache_misses: Counter,
    /// `decdn_probe_collection_latency_seconds` (ADR 001 §Probe response
    /// collection): the time from the start of the concurrent probes, dials
    /// included, to the end of collection — the early stop once enough holders
    /// answer, or the drain of the probe set, which `PROBE_TIMEOUT` bounds per
    /// probe. A round that sends no probe records nothing. Each probe round
    /// records one sample, so a ranged miss whose holders do not span the blob,
    /// and that then probes origin-directory candidates, records two (#2195).
    #[default(Histogram::new(PROBE_COLLECTION_BUCKETS.to_vec()))]
    pub probe_collection_latency_seconds: Histogram,
    /// `decdn_dht_lookup_round_ceiling_total` (#1145 review): a `find_providers` lookup was
    /// TRUNCATED at `MAX_LOOKUP_ROUNDS` while still finding closer nodes.
    ///
    /// Not an error — the providers already found are usable and the pull proceeds with them
    /// — but not nothing either, and a bare `debug!` would be invisible at the
    /// project's default `RUST_LOG=info`, so it is a counter. The ceiling exists because
    /// discovery's worst case
    /// has to be FINITE for `PULL_THROUGH_OUTER_SLACK` to budget for it at all; the cost is
    /// that a lookup which needs more rounds silently returns a smaller candidate set.
    ///
    /// So a sustained rate means this node's round ceiling is too low for its network size:
    /// every lookup is cut short, and `MAX_PROVIDER_ATTEMPTS` is choosing from a worse set of
    /// providers than the DHT could have offered. Kademlia converges in `O(log n)` rounds, so
    /// this is the metric that says "your network outgrew the constant".
    pub dht_lookup_round_ceiling: Counter,
    /// `decdn_node_pull_corruption_total` (#831): an upstream served
    /// hash-mismatched bytes for a paid pull (Byzantine / buggy provider). A
    /// sustained nonzero rate means a peer is taking payment for wrong content;
    /// the cache engine rejects the bytes, but the USDC was still spent.
    pub node_pull_corruption: Counter,
    /// `decdn_node_pull_unreachable_total` (#831): a probe or pull to a
    /// candidate failed at the transport (scored [`Outcome::Unreachable`]).
    ///
    /// [`Outcome::Unreachable`]: decdn_reputation::Outcome::Unreachable
    pub node_pull_unreachable: Counter,
    /// `decdn_node_region_latency_penalty_total` (#1177): a probed peer
    /// self-attested this node's own region yet answered slower than the ADR 030
    /// latency ceiling, so it was penalized in local reputation
    /// ([`Outcome::RegionLatencyMismatch`]). A sustained rate points at
    /// region-spoofing peers (or a genuinely mis-set local `identity.region`).
    ///
    /// [`Outcome::RegionLatencyMismatch`]: decdn_reputation::Outcome::RegionLatencyMismatch
    pub node_region_latency_penalty: Counter,
    /// `decdn_node_pull_pool_open_failures_total` (#831): a buyer
    /// `open_or_reuse_pool` failed before a pull could start. This is the
    /// node's own payment-side fault (gas, RPC, buyer store), NOT the
    /// provider's — a sustained rate means node→node buying is wedged. This is
    /// the *unlabeled total* across all causes; the
    /// `pool_open_failures_*_total` family below (#966) breaks the
    /// `openPool`-tx failures out by cause so an operator can tell a
    /// misconfiguration (`insufficient_deposit`) from infrastructure
    /// (`rpc_error`). It also covers store, open-task, capability-signing and
    /// lane-seed causes the by-reason family does not, so the two are not
    /// expected to sum equal. `decdn_buyer_lane_seed_failures_total` breaks the
    /// lane-seed refusals out.
    ///
    /// **One failure moves this counter once.** A site increments it if and only
    /// if it marks the error `OpenReported`, which is what stops the classifier
    /// in `node_origin` from restating a failure the open task already counted.
    /// Against `node_pull_attempts_total` this reads above 1.0 legitimately: a
    /// pull orchestration meters one attempt and may open a lane per candidate
    /// and per assembled run. A clean 2× ratio is the signature of a leg that
    /// meters without marking (#2072).
    pub node_pull_pool_open_failures: Counter,
    /// `decdn_pool_open_failures_insufficient_deposit_total` (#966): a buyer
    /// `openPool` tx reverted because the node's USDC balance/allowance could
    /// not cover the deposit, or the deposit was zero — either as requested, or
    /// as the balance delta actually received under a fee-on-transfer token.
    /// Both zero cases revert the same argument-less `ZeroAmount`, so this
    /// counter cannot separate them; the wallet balance is what distinguishes a
    /// misconfigured deposit from a token that shaved it. A token that reverts
    /// in the older `Error(string)` style lands here too, matched on its message
    /// rather than a custom-error selector. A *misconfiguration*
    /// signal either way — the fix is operator-side (fund the wallet, raise the
    /// configured deposit), not infrastructure. A plain counter
    /// field carries no label dimension (a labeled series would need a `Family`),
    /// so the issue's `{reason=…}` split is realized as
    /// three sibling counters (mirroring `dht_rate_limit_rejected_*`); the
    /// `reason` value is the field-name token. The `OpenMetrics` encoder appends
    /// the `_total` suffix.
    pub pool_open_failures_insufficient_deposit: Counter,
    /// `decdn_pool_open_failures_contract_revert_total` (#966): a buyer
    /// `openPool` tx reverted on-chain for a reason other than insufficient
    /// deposit (provider not active, a paused contract, a mined revert whose
    /// reason is not recoverable from the receipt). The deposit was not
    /// escrowed; the cause is on-chain state, not this node's wallet or RPC.
    pub pool_open_failures_contract_revert: Counter,
    /// `decdn_pool_open_failures_rpc_error_total` (#966): a buyer
    /// `openPool` submit or receipt wait failed at the transport layer (no
    /// revert data) — connectivity, a timed-out receipt, a nonce blip. A
    /// *transient infrastructure* signal; retrying typically clears it. Pair
    /// with the two reverting counters above to tell "operator under-funded the
    /// wallet" from "the RPC endpoint is flaky".
    pub pool_open_failures_rpc_error: Counter,
    /// `decdn_node_pull_too_large_total` (#840): the bytes received from a
    /// selected upstream crossed this node's `max_blob_size` ceiling, so the buyer
    /// aborted the pull (#1895). The upstream's `total_bytes` claim plays no part.
    /// Like a pool-open failure this is a buyer-side policy decision, NOT
    /// necessarily provider misbehavior (the provider may legitimately serve
    /// larger blobs to nodes with a higher ceiling), so it does not tar the
    /// provider's reputation. A sustained rate
    /// means this node's ceiling is below the content it is trying to warm.
    pub node_pull_too_large: Counter,
    /// `decdn_node_pull_rate_above_ceiling_total` (#1375): a selected upstream
    /// signed an open-stage `StreamResponse` quoting a per-MB rate above this
    /// node's effective buyer ceiling (the lower of the candidate's probe rate and
    /// the configured `cache.max_rate_per_mb`), so the buyer refused before paying
    /// any voucher. Sibling of [`Self::node_pull_too_large`]: a buyer-side policy
    /// decision that does NOT tar the provider (an over-*config* quote is our tight
    /// policy; an over-*probe* quote is a possible bait-and-switch we do not
    /// adjudicate here). A sustained rate is the operator's signal that nodes are
    /// being probed for rate manipulation — the one refusal on this path that
    /// guards a slashable offense, so it earns a counter of its own.
    pub node_pull_rate_above_ceiling: Counter,
    /// `decdn_serve_economics_refused_total` (ADR 041): candidates existed for a
    /// cache-miss buy but every one quoted above this node's serve-economics buy
    /// ceiling, so the buyer declined an unprofitable relay rather than paying for
    /// bytes it cannot resell for a margin. Sibling of
    /// [`Self::node_pull_rate_above_ceiling`]: a buyer-side policy decision, so it
    /// does not tar the provider's reputation. The node still signs the client a
    /// `NotFound` for this — the pricing floor never reaches the wire — so a
    /// sustained rate is visible only here, and means this node's serve economics
    /// (or the market it is buying into) are too tight to relay profitably.
    ///
    /// The aggregate over both regime siblings below, mirroring the
    /// `pool_open_failures_*` shape: bumped on every refusal beside the regime
    /// counter, so a dashboard can rate the total and drill into the split.
    pub serve_economics_refused: Counter,
    /// `decdn_serve_economics_refused_warming_total` (ADR 041): the subset of
    /// [`Self::serve_economics_refused`] where the source still had warming
    /// allowance, so the ceiling was the market price `max(sell, amortized)` and
    /// the market itself is above it. Remedy: the market is too hot to relay
    /// into — there is no local config fix. See [`ServeEconomicsRegime`].
    pub serve_economics_refused_warming: Counter,
    /// `decdn_serve_economics_refused_amortized_total` (ADR 041): the subset of
    /// [`Self::serve_economics_refused`] where the source's warming allowance was
    /// spent (or warming is off), so the ceiling was already the amortized floor.
    /// Remedy: this node's own sell rate or margin config is too tight. A rising
    /// split toward this regime is the operator-actionable one. See
    /// [`ServeEconomicsRegime`].
    pub serve_economics_refused_amortized: Counter,
    /// `decdn_node_pull_timeout_total` (#857): a buyer→upstream pull hit one of this node's
    /// own deadlines. Like a pool-open failure this is a buyer-side condition (a possibly
    /// mis-sized local budget), NOT evidence the provider is unreachable, so it does NOT tar
    /// the provider's local reputation. Distinct from `node_pull_through_timeouts` (the
    /// delivery handler's own serving deadline).
    ///
    /// It fires on **two** budgets, and they have different remedies (#1145 review):
    ///
    /// - the STREAM-OPEN stage exceeding `node_pull_timeout_sec`; and
    /// - a pull that has received no first byte within `node_pull_stall_window_sec`. Before
    ///   the first chunk, the throughput floor is measuring the server's time-to-FIRST-byte,
    ///   which scales with blob size (the serve path materialises the whole bao encoding
    ///   before it can emit chunk #1) — so it is our deadline, not the peer's fault, and it
    ///   lands here rather than on `node_pull_stalled_total`.
    ///
    /// A sustained rate therefore means one of those two is too tight for the upstreams this
    /// node selects — and for the large-blob case it is `node_pull_stall_window_sec`, not
    /// `node_pull_timeout_sec`, that wants raising.
    ///
    /// The peer is not scored, but it IS suppressed briefly: see `REFUSAL_SUPPRESSION_TTL`.
    /// Exonerating a peer and ignoring it are different things, and a peer that accepts a
    /// stream and then says nothing lands here.
    pub node_pull_timeout: Counter,
    /// `decdn_node_pull_voucher_rejected_total` (#857): an upstream rejected a
    /// voucher this node presented mid-pull (an amount or bytes regression, a
    /// spent capability cap, or a voucher addressed to the wrong pool or
    /// provider). This is the node's own payment-side fault, NOT the provider's,
    /// so it does NOT tar the provider's reputation. A sustained rate means this
    /// node's buyer lanes are drifting out of sync with what upstreams accept.
    pub node_pull_voucher_rejected: Counter,
    /// `decdn_node_pull_pool_wedged_total` (#1145 review): an upstream rejected our
    /// voucher on terms this lane cannot recover from, while the pool's deposit is STILL
    /// ESCROWED — an amount or bytes regression, a spent capability cap, an expired
    /// capability, or a voucher addressed to the wrong pool or provider.
    ///
    /// The pool row is KEPT (it is the only thing that can still reclaim the deposit) and
    /// the PROVIDER is suppressed for a fixed one-hour window. The deposit is not
    /// stranded: it is shared across every lane, so it stays available to every other
    /// provider throughout, and the pool has no expiry of its own to wait on.
    ///
    /// **A sustained rate is a provider this node keeps failing to pay.** A spike means
    /// buyer watermarks are drifting out of sync with what upstreams have committed — the
    /// desync tracked in #1122 — and the deposit sizing and pool count should be reviewed
    /// alongside it. One tick costs an hour of that provider's capacity, not the deposit.
    pub node_pull_pool_wedged: Counter,
    /// `decdn_node_pull_refused_total` (#1144): a selected upstream refused
    /// delivery up front (a `StreamResponse` with `ok == false`). Counts every
    /// wire class, and none of them tars the provider's reputation. It is not
    /// split by class (a labeled series would need a `Family`). Most refusals
    /// are honest and benign: a `NotFound` is simply a healthy-but-empty node, so
    /// a sustained rate here usually means content discovery is steering this node
    /// at upstreams that do not hold the blob — not that the upstreams are bad.
    pub node_pull_refused: Counter,
    /// `decdn_node_pull_refused_unattributable_total` (#1520): the subset of
    /// [`Self::node_pull_refused`] whose wire code this node cannot pin on the
    /// upstream — `NotFound` and `Unfunded`, i.e. `RefusalVerdict::Transient`.
    /// Those briefly suppress the `(peer, hash)` pair without touching reputation,
    /// except an open-stage `NotFound` on a node-origin leg, which is backpressure
    /// and suppresses nothing unless it outlasts the wait budget (#2178).
    ///
    /// Split out because it is the shape a *buyer-side* misconfiguration takes:
    /// `Unfunded` is what a seller signs when it refuses OUR pool for its
    /// deposit, and `NotFound` when our signer sits at its floor cap, so a node
    /// whose own deposit is too small to buy anything sees 100% of its pulls
    /// refused here.
    ///
    /// Read it with one caveat: `NotFound` also carries the PEER's load shed,
    /// which the code respects rather than punishes. So a network-wide load event
    /// drives this ratio to ~1 for a reason no local change fixes. A rate
    /// approaching `node_pull_refused` therefore means "nobody is serving me",
    /// which is *usually* local (deposit, binding) but is worth confirming against
    /// peer health first; a small fraction is the healthy "that peer did not
    /// have it".
    pub node_pull_refused_unattributable: Counter,
    /// `decdn_node_upstream_rate_limited_total` (#1986): a probe or pull to a
    /// candidate was shed at the transport with `APP_ERR_RATE_LIMITED` (`0x10`,
    /// ADR 013 §Application Error Codes) — the upstream's connection limiter,
    /// probe limiter, or per-connection stream cap refused the work before any
    /// signed message existed. The transport-level twin of a load-shed `NotFound`
    /// refusal, treated the same way: NO reputation outcome is recorded, and the
    /// `(peer, hash)` pair is suppressed for the short refusal TTL except on a
    /// node-origin leg, where the shed is backpressure (#2178). So this is the
    /// only trace the event leaves at the default `RUST_LOG=info`.
    ///
    /// A sustained rate is peer-side load, not a local fault — but a rate that
    /// tracks `node_pull_unreachable` is worth a look: a `global-full` shed skips
    /// the upstream's close ack-wait and can still arrive as a bare drop, which
    /// lands in the unreachable counter instead of here.
    pub node_upstream_rate_limited: Counter,
    /// `decdn_node_pull_backpressure_backoffs_total` (#2178): a ranged run's
    /// upstream refused the stream open for backpressure (`NotFound` or a
    /// transport rate-limit), no other candidate covered the
    /// still-missing gap, and the assembly waited to ask the same upstream again
    /// instead of dropping it. The common cause is this node's own other pulls
    /// filling the upstream's per-signer live cap: the refusal clears as those
    /// pulls pay.
    ///
    /// Each tick is one completed wait, and each wait follows one refusal already
    /// counted in `node_pull_refused_unattributable` or
    /// `node_upstream_rate_limited`. A sustained rate most often means an
    /// upstream's cap is too small for the concurrency this node's clients drive
    /// through it.
    pub node_pull_backpressure_backoffs: Counter,
    /// `decdn_node_pull_backpressure_exhausted_total` (#2178): the only holder of
    /// a still-missing range kept refusing for backpressure through the whole
    /// wait budget, so the assembly ended short and every serve stream attached
    /// to it was truncated. The holder is then suppressed for the hash for the
    /// short refusal TTL, and a `warn!` names it.
    ///
    /// Zero is the expected value. A non-zero rate means a refusal that waiting
    /// does not clear: a holder whose cap never releases, or one of the refusals
    /// the seller collapses onto `NotFound` that is not a cap at all.
    pub node_pull_backpressure_exhausted: Counter,
    /// `decdn_node_pull_leg_no_progress_total` (#2194): a pull leg streamed and
    /// finished cleanly, yet moved neither the gap's paid frontier nor the store's
    /// delivered frontier. The drive ends the gap rather than re-open and re-pay the
    /// same range. An upstream leg is not scored: its `(peer, hash)` pair is
    /// suppressed briefly and the run re-plans onto another source. A `warn!` names
    /// the range, both frontiers and the channel's paid wire since the leg opened.
    ///
    /// Zero is the expected value. A tick means this node's store did not keep a
    /// leg's bytes, its ledger did not record the leg's payment, or the upstream
    /// ended the stream without taking the leg's final proof.
    pub node_pull_leg_no_progress: Counter,
    /// `decdn_node_pull_stalled_total` (#1797): an upstream's throughput fell below the
    /// floor mid-stream — the bytes across `node_pull_stall_window_sec` dropped under
    /// `node_pull_min_throughput_bps` — so the pull was abandoned after at least one byte had
    /// arrived. Split from `node_pull_timeout` (the same abort before the first byte) only as
    /// a metric: BOTH are requester-local policy and neither tars the provider's reputation,
    /// because a sub-floor stream may be slow for reasons the peer cannot be blamed for and a
    /// throughput signal is spoofable. A sustained rate points at flaky upstreams or a
    /// `node_pull_min_throughput_bps` too high (or `node_pull_stall_window_sec` too tight)
    /// for the network.
    pub node_pull_stalled: Counter,
    /// `decdn_node_pull_local_fault_total` (#1145 review): a pull failed for a reason
    /// that is OURS — a broken signer, an encode fault, a bad range computation, an
    /// unusable deadline config, a buyer pool open this node's own state defeated
    /// (an unreadable or unwritable buyer pool store, a poisoned open lock, a panicked open
    /// task, a wallet that cannot fund a deposit; #1560), or a cache store that cannot
    /// take the pulled bytes (a full or read-only disk, a cache code bug; #2286) — and
    /// the upstream was exonerated.
    ///
    /// The only counter here that says nothing about the network. Any sustained rate is
    /// an emergency: a node that cannot sign a voucher or store a byte cannot complete a
    /// pull, so every pull it attempts will fail. These failures land here, not on the
    /// honest providers the node tries: scored against them, they would show as a node
    /// steadily blaming a healthy network in its own reputation scores.
    ///
    /// Counted PER CANDIDATE, not per request: one node-wide fault moves this by up to
    /// `MAX_PROVIDER_ATTEMPTS` for a single client request, because the walk tries each
    /// candidate and fails identically on all of them. Read the rate, not the absolute,
    /// and do not infer the number of affected requests from it.
    pub node_pull_local_fault: Counter,
    /// `decdn_node_pull_pool_open_pending_total` (#1143): a buyer pool open
    /// was still in flight when the per-candidate budget expired, so the pull moved
    /// to the next candidate while the open continued in the background.
    ///
    /// NOT a failure — kept separate from `node_pull_pool_open_failures` because
    /// the diagnosis is different: a failure says the tx reverted or the wallet is
    /// under-funded, whereas this says the open is simply *not done yet*. It scores no
    /// reputation: a wedged open is our lane, not evidence about the peer.
    ///
    /// # Two distinct causes, one counter
    ///
    /// 1. the node's own chain lane (a slow L2, a stuck nonce) is slower than
    ///    `POOL_OPEN_CALLER_BUDGET` — the interesting one; and
    /// 2. a boot or idle **reconcile** holds the provider's open slot.
    ///
    /// The verdict is the same for both — try the next candidate, score nothing — which
    /// is why they share a counter. But the *diagnosis* is not: reconcile runs at every
    /// boot, so a restart produces a burst here that means nothing is wrong. Read a
    /// sustained rate as a chain-lane signal only once it outlives a restart.
    pub node_pull_pool_open_pending: Counter,
    /// `decdn_node_pull_progress_persist_failures_total` (#852): a pull paid ≥1
    /// voucher but persisting the buyer lane's resume watermark
    /// (`record_progress`) failed. The bytes were delivered, but the lane's
    /// stored `bytes`/`amount` now lags what the upstream accepted. The next
    /// reuse of this lane signs from that stale anchor; the upstream rejects it
    /// with its watermark bundle and the ledger reseeds from that, so each reuse
    /// costs one rejected voucher. A sustained rate means the buyer store is
    /// failing writes.
    pub node_pull_progress_persist_failures: Counter,
    /// `decdn_node_pull_progress_dropped_total` (#1145 review): a pull paid ≥1
    /// voucher, but by the time the watermark was written the node's pool had
    /// been replaced by a newer open — so the write was skipped rather than clobber
    /// the replacement, and that voucher's progress is gone.
    ///
    /// Sibling of `node_pull_progress_persist_failures_total`, which only counts the
    /// `Err` path; this is the `Ok`-but-dropped path, which is otherwise invisible.
    /// The rare benign case is a pool replaced mid-pull. A *sustained* rate means
    /// a `pool_id`-plumbing bug, and every tick is real USDC whose watermark was
    /// discarded — so this is the counter to alert on, not just to look at.
    pub node_pull_progress_dropped: Counter,
    /// `decdn_node_pull_progress_superseded_total` (#1145 review): a pull's watermark write was
    /// REGRESSED because a CONCURRENT pull on the same shared lane ledger had already
    /// persisted a higher (correct) watermark. Benign and EXPECTED under the shared
    /// `BuyerLedgers` — concurrent pulls on one lane are routine — because the monotonic
    /// store keeps the winner's higher value, so no voucher is lost. Split from
    /// `node_pull_progress_persist_failures_total` (a real store-write failure that leaves the
    /// watermark lagging) so ordinary settle races do not drown out a genuine persist fault.
    pub node_pull_progress_superseded: Counter,
    /// `decdn_node_pull_recovery_step_total` (#1530): a node→node miss fill ran a
    /// funding recovery step (ADR 003 § Funding recovery) that funds the next
    /// pass: every candidate refused this node's funding, so the step topped the
    /// buyer pool up, opened a new pool in place of one that accepts no more
    /// funds, or settled a pool that already holds its working deposit while the
    /// upstreams catch up.
    ///
    /// Expected to be RARE once `buyer_working_deposit_micro_usdc` is sized for the
    /// blobs this node pulls: the low-water refill should refill a
    /// pool long before a fill runs it dry. A sustained rate means the working
    /// deposit is too small for the blob sizes in play, and every tick is a
    /// transaction plus a settlement wait a client sat through.
    ///
    /// Distinct from `decdn_buyer_topup_ok_total`, which counts on-chain top-ups from
    /// BOTH legs: this one isolates the recovery step, so the low-water refill's
    /// routine traffic cannot hide it.
    pub node_pull_recovery_step: Counter,
    /// `decdn_node_pull_recovery_step_refused_total` (#1530): a fill's funding
    /// recovery step had no way to fund the next pass: the working deposit is
    /// zero (funding off), the top-up landed nothing, or the funding
    /// transaction failed (allowance, revert, RPC, or a mined `topUp` the local
    /// pool row could not credit).
    ///
    /// A failed funding transaction also ticks `decdn_buyer_topup_failure_total`.
    pub node_pull_recovery_step_refused: Counter,
    /// `decdn_node_pull_through_timeouts_total` (#831): cache-miss pull-through
    /// attempts the delivery handler abandoned at its deadline. Distinguishes a
    /// slow/wedged upstream from a genuine miss (both otherwise return
    /// `NotFound`). Covers BOTH reactive fill tiers — node→node (#831) and the
    /// local-origin populate (#1116) — so it is not solely a node→node-health
    /// signal; a cache-only operator's own wedged origin bumps it too.
    pub node_pull_through_timeouts: Counter,
    /// `decdn_node_pull_through_errors_total` (#831): cache-engine errors (not
    /// clean misses) hit while filling a miss on the delivery path — a real
    /// store/pull fault, surfaced as `NotFound` to the client but logged + bumped
    /// here so it isn't silent. Covers BOTH reactive fill tiers: node→node (#831)
    /// and the local-origin populate (#1116).
    pub node_pull_through_errors: Counter,
    /// `decdn_node_pull_through_window_paused_total` (#856): times the
    /// window-paced serve loop paused the upstream pull because the per-request
    /// unrecouped frontier (`bytes pulled − bytes paid`) reached the ramped
    /// credit window (ADR 003 §Credit window, #1669) and it waited for the
    /// downstream voucher to clear. A high rate is benign (the window is
    /// doing its job pacing speculation); a flat zero under real pull-through
    /// traffic means the window never binds.
    pub node_pull_through_window_paused: Counter,
    /// `decdn_node_pull_through_min_draw_waits_total` (#2061): times the
    /// window-paced serve loop paused the upstream pull because the ramped
    /// window had room, but less than the minimum draw of half the window, and
    /// no serve leg was parked at the pull's frontier. Each draw opens a new
    /// upstream request, so the pause batches the room into fewer, larger draws.
    /// A high rate is benign; it grows with the number of vouchers per draw.
    pub node_pull_through_min_draw_waits: Counter,
    /// `decdn_node_pull_through_wait_seconds`: how long one window-paced pause
    /// (window full or minimum draw) held the upstream pull before a downstream
    /// payment or a parked serve leg released it, or its leg cancelled it. One
    /// observation per pause.
    /// Most pauses end within a voucher round trip; a tail near the top bucket
    /// means a payer that stalls or a pacing regression that parks the pull
    /// with no serve leg left to wake it. A pause past `PULL_WAIT_WARN_AFTER` (45 s)
    /// also logs a warning.
    #[default(Histogram::new(FIRST_BYTE_BUCKETS.to_vec()))]
    pub node_pull_through_wait_seconds: Histogram,
    /// `decdn_node_pull_through_client_abandoned_total` (#856): window-paced
    /// serves the requesting client dropped or underpaid mid-pull, so the node
    /// aborted the upstream pull and abandoned the partial fill. The per-request
    /// loss is bounded to the ramped credit window; a sustained rate flags a leech.
    /// A node-side fault on the serve leg is not an abandon: it counts on
    /// `decdn_serve_stream_node_fault_total` only. Nor is an owed chunk that passes
    /// its proof-wait ceiling while its connection still sends: a slow path or a
    /// busy sibling stream can cause that, and the stream ends on the declined or
    /// abandoned serve reason counter only. Every abandon here also ends the
    /// inbound stream on one serve reason counter: a spent per-chunk proof budget
    /// on `decdn_serve_stream_proof_budget_exhausted_total`, a rejected voucher on
    /// `decdn_serve_stream_voucher_rejected_total`, and a client that drops on
    /// `decdn_serve_stream_client_declined_total` before it pays or
    /// `decdn_serve_stream_client_abandoned_total` after.
    pub node_pull_through_client_abandoned: Counter,
    /// `decdn_node_pull_through_upstream_verify_failed_total` (#856/#915): a
    /// serve-miss pull ingested upstream bytes that failed bao verification against
    /// the content root (ADR 038) — a chunk group rejected mid-stream (the
    /// bait-and-switch case), a whole-blob root mismatch, or an over-delivery past
    /// the promised wire. The partial fill is never promoted, the
    /// upstream is scored `Corruption`, and the
    /// client's own bao decoder rejects the forwarded bytes. Distinct from
    /// `node_pull_corruption` (the buffered orchestration's own check). A sustained
    /// rate means clients are being served
    /// corrupt-upstream bytes through this node. Field has no `_total` suffix
    /// because the `OpenMetrics` encoder appends it.
    pub node_pull_through_upstream_verify_failed: Counter,
    /// `decdn_local_outboard_serves_total` (#1130): times the node served a
    /// whole-blob cache miss by streaming straight from its own configured
    /// origin (http/s3/fs) into the paying client while filling
    /// the local cache store beside it — the stream-while-store path, entered only when
    /// the origin publishes a `{H}.obao4` pre-order outboard alongside the
    /// object. Incremented once per entry into `serve_via_backend_origin`,
    /// before any admission guard, so it also counts requests this tier later
    /// rejects (deposit/leech/size) — it marks which SERVE TIER fired, not
    /// whether the fill ultimately succeeded. A miss that instead falls back to
    /// the buffered `populate_local` path (no outboard, or this tier declined)
    /// never bumps this counter, which is what lets a test or dashboard tell
    /// the two fill strategies apart deterministically instead of by timing.
    pub local_outboard_serves: Counter,
    /// `decdn_serve_cache_hit_total`: paid serves the blob-availability gate
    /// classified as servable from a COMPLETE locally-held blob, before any fill
    /// tier runs. One bump per request the gate classifies, in
    /// `handlers::client::dispatch`.
    ///
    /// Counted BEFORE the load-shed admission call, so a shed refusal still
    /// records the availability decision it refused. That keeps the hit rate a
    /// property of the store rather than of the node's current pressure — a
    /// node shedding hard would otherwise report a hit rate that collapses
    /// exactly when an operator most needs to read it.
    ///
    /// This is the serve-path twin of `decdn_cache_hits_total`, and the two are
    /// not interchangeable: the cache-crate counter meters
    /// `CacheEngine::get()`, the whole-blob buffered read, which the paid serve
    /// path never calls — it streams through `export_bao_range_stream` instead.
    /// A node that serves only paying clients therefore holds
    /// `decdn_cache_hits_total` at zero however full its store is. Its partner
    /// `decdn_cache_misses_total` is NOT confined to `get` — the fill tiers bump
    /// it too — so that pair reads as a populated, permanently-0% ratio rather
    /// than as an empty one, which is why the serve path needs its own family.
    ///
    /// Sibling of `serve_cache_partial_hit` and `serve_cache_miss`. At most one
    /// of the three is bumped per request, and two requests that reach
    /// `serve_audit` bump none: a withdrawn hash (operator evict or corruption
    /// quarantine) is refused for `evicted_since_probe`, and a `serve_audit` store
    /// fault is refused for `internal_error`. Neither is an availability class — one
    /// is a refusal, the other is the store failing to answer — so the ratio's
    /// denominator deliberately excludes both. Field has no `_total` suffix
    /// because the `OpenMetrics` encoder appends it.
    pub serve_cache_hit: Counter,
    /// `decdn_serve_cache_partial_hit_total`: paid serves admitted from a blob
    /// that is not `Complete` but whose held chunk groups already cover the
    /// requested span (#1506). Counted apart from `serve_cache_hit` because the
    /// two answer different questions — this one is the payoff of the
    /// range-keyed partial-holder advertisement a node makes over
    /// `cdn/probe/v1` and the DHT, and folding it into the plain hit would hide
    /// whether that mechanism carries any traffic. Both are hits for hit-rate
    /// purposes.
    pub serve_cache_partial_hit: Counter,
    /// `decdn_serve_cache_miss_total`: paid serves the gate could not satisfy
    /// from locally-held bytes. Counts the ADMISSION decision and nothing
    /// downstream of it — a fill tier may never run at all, because the bump
    /// precedes the load-shed gate, the pre-spend floor reservation, and the
    /// `pull_authorized` check that every tier is gated on. A shed refusal, a
    /// floor refusal and an unbound request therefore all count here having
    /// asked no origin and no peer. Where a tier does run and satisfies the
    /// blob, the request still counts here; the refusal siblings
    /// (`serve_stream_rejected_*`) say whether it ended in a refusal.
    ///
    /// Also absorbs one non-absence: `partial_hit_size` resolves the local
    /// bitfield with `.ok()?`, so a store fault while answering "do my held
    /// ranges cover this span?" reads as "they do not" and lands here. That is
    /// the conflation the `serve_audit` `Err` arm exists to avoid one branch
    /// earlier, and it bounds how clean a reading of this counter can be.
    pub serve_cache_miss: Counter,
    /// `decdn_serve_first_byte_hit_seconds`: time to first byte of a paid serve
    /// the availability gate classed as a hit (complete or partial), from the
    /// decoded request to the first `ChunkData` frame written. The first frame
    /// rides the opening credit window, so no client payment round trip is in
    /// the window; one `getPool` chain read is, when the pool view has no entry
    /// for the pool. A stream that ends before any frame records nothing.
    /// Sibling of `serve_first_byte_miss_seconds` on the cache-class axis.
    #[default(Histogram::new(FIRST_BYTE_BUCKETS.to_vec()))]
    pub serve_first_byte_hit_seconds: Histogram,
    /// `decdn_serve_first_byte_miss_seconds`: time to first byte of a paid serve
    /// the availability gate classed as a miss — the same window as
    /// `serve_first_byte_hit_seconds`, which here also covers discovery and the
    /// upstream or origin fill that produces the first frame. A window-paced
    /// fill streams its first frame early; a buffered fill completes the whole
    /// blob first, so on that tier the value grows with blob size.
    #[default(Histogram::new(FIRST_BYTE_BUCKETS.to_vec()))]
    pub serve_first_byte_miss_seconds: Histogram,
    /// `decdn_serve_admission_seconds`: the admission phase of a paid serve's
    /// time to first byte, from the decoded request to the load-shed admit. It
    /// covers the binding check, the deny gates, the `getPool` and
    /// `getAuthorization` chain reads on a view miss, and the availability
    /// audit. The class is unknown until the audit ends, so this phase has no
    /// hit/miss siblings. Records only for a stream that writes its first frame,
    /// so the three phase histograms and the total share one population.
    #[default(Histogram::new(FIRST_BYTE_BUCKETS.to_vec()))]
    pub serve_admission_seconds: Histogram,
    /// `decdn_serve_signer_auth_cached_total`: admit-path signer confirms the
    /// signer-auth cache answered, with no `getAuthorization`. A `Registered`
    /// read answers for good; an `Unregistered` one for a short window.
    pub serve_signer_auth_cached: Counter,
    /// `decdn_serve_signer_auth_first_read_total`: admit-path signer confirms for
    /// a `(pool, signer)` the node holds no read of: a new signer, or one
    /// evicted from the bounded cache. Each waits on a `getAuthorization`, its
    /// own or one already in flight for the pair.
    pub serve_signer_auth_first_read: Counter,
    /// `decdn_serve_signer_auth_reread_total`: admit-path signer confirms that
    /// re-read an `Unregistered` signer, because the read aged out or the
    /// projection has since folded a redemption by it. Each waits on a
    /// `getAuthorization`, its own or one already in flight for the pair.
    pub serve_signer_auth_reread: Counter,
    /// `decdn_serve_response_hit_seconds`: the response phase of a paid hit,
    /// from the load-shed admit to the signed `StreamResponse` written.
    #[default(Histogram::new(FIRST_BYTE_BUCKETS.to_vec()))]
    pub serve_response_hit_seconds: Histogram,
    /// `decdn_serve_response_miss_seconds`: the response phase of a paid miss,
    /// from the load-shed admit to the signed `StreamResponse` written. It
    /// covers the floor reservation, the origin size and range probes, and on a
    /// node-to-node fill the upstream discovery, pool open and handshake. A
    /// buffered fill imports the whole blob in this phase.
    #[default(Histogram::new(FIRST_BYTE_BUCKETS.to_vec()))]
    pub serve_response_miss_seconds: Histogram,
    /// `decdn_serve_first_frame_hit_seconds`: the first-frame phase of a paid
    /// hit, from the `StreamResponse` written to the first `ChunkData` frame
    /// written. It covers the read and encode of the whole first frame.
    #[default(Histogram::new(FIRST_BYTE_BUCKETS.to_vec()))]
    pub serve_first_frame_hit_seconds: Histogram,
    /// `decdn_serve_first_frame_miss_seconds`: the first-frame phase of a paid
    /// miss, from the `StreamResponse` written to the first `ChunkData` frame
    /// written. On a window-paced fill it covers the pull leg filling the whole
    /// first frame from upstream or origin.
    #[default(Histogram::new(FIRST_BYTE_BUCKETS.to_vec()))]
    pub serve_first_frame_miss_seconds: Histogram,
    /// `decdn_origin_directory_get_origins_failures_total`: `getOrigins`
    /// lookups that failed on a cold-namespace cache miss. The directory fails
    /// closed on each (resolves no origins for that request, does not cache
    /// the failure), so this is the drift signal to alert on — sustained
    /// failures mean real requests are silently losing their origin fallback.
    pub origin_directory_get_origins_failures: Counter,
    /// `decdn_origin_directory_cache_size`: current namespace count held in
    /// the lazy origin directory cache (positive + negative entries).
    /// Bounded by the configured cache capacity (LRU eviction).
    pub origin_directory_cache_size: Gauge,
    /// Paid-delivery (`serve_stream`) requests refused because the blob was
    /// deliberately evicted between probe and stream (#279). One `Counter` per
    /// reason — like the `dispatch_rejected_*` convention — because a plain counter
    /// field carries no label dimension, and the wire `StreamError` deliberately
    /// conflates the three `NotFound` reasons (`cache_miss`, `unknown_lane`,
    /// `owner_mismatch`) (#876). Visible name:
    /// `decdn_serve_stream_rejected_evicted_since_probe_total`.
    pub serve_stream_rejected_evicted_since_probe: Counter,
    /// `serve_stream` requests refused because the blob is absent and
    /// pull-through was unavailable or failed to fill it. Visible name:
    /// `decdn_serve_stream_rejected_cache_miss_total`.
    pub serve_stream_rejected_cache_miss: Counter,
    /// `serve_stream` requests refused by a local store fault (`has`/`inspect`
    /// error, or a present blob reporting no size) — signed to the client as
    /// `Declined`, not a signed absence. Visible name:
    /// `decdn_serve_stream_rejected_internal_error_total`.
    pub serve_stream_rejected_internal_error: Counter,
    /// `serve_stream` requests refused on an unknown / never-opened lane
    /// (#848). Wire-indistinguishable from `cache_miss`/`owner_mismatch` (all
    /// signed as `NotFound` to avoid leaking lane existence), so this
    /// server-side counter is the only place the distinction lives — a rising
    /// value isolates an unknown-lane abuse campaign. Visible name:
    /// `decdn_serve_stream_rejected_unknown_lane_total`.
    pub serve_stream_rejected_unknown_lane: Counter,
    /// `serve_stream` requests refused because a verified client binding does
    /// not authorize the named channel (#327). Visible name:
    /// `decdn_serve_stream_rejected_owner_mismatch_total`.
    pub serve_stream_rejected_owner_mismatch: Counter,
    /// `serve_stream` requests reset because the client binding failed to
    /// verify: the signature is invalid, or it recovers a different address
    /// than the one claimed. No signed response is sent. Visible name:
    /// `decdn_serve_stream_rejected_bad_binding_total`.
    pub serve_stream_rejected_bad_binding: Counter,
    /// Probe requests this node could not read: the read timed out, the frame
    /// or the message failed to decode, or the peer sent a response on the
    /// server stream. The connection closes with the matching ADR 013 code.
    /// Visible name: `decdn_probe_read_faults_total`.
    pub probe_read_faults: Counter,
    /// `serve_stream` requests refused before any bytes are served because the
    /// requesting channel's remaining deposit could not cover what the node
    /// would front at its rate. (The refusal itself is signed — as an `ok: false`
    /// response; what is never signed is an `ok: true`.) **Three** guards bump
    /// this. Not a sequence any one request walks: a cache HIT reaches only (3),
    /// and the window tier returns out of `serve_stream`, so (2) and (3) are
    /// mutually exclusive. A miss passes (1) and then at most one of (2)/(3):
    /// 1. the cache-miss **floor** (#1519) — one credit window, applied above
    ///    every fill tier so none of them fronts origin egress or upstream USDC
    ///    for a channel that cannot pay for a single interval;
    /// 2. the **window** tier's speculative ceiling (#856) — the whole-blob cost
    ///    when `max_blob_size_bytes` is finite, else the window cost;
    /// 3. the **direct-serve** ceiling (#1516) —
    ///    `min(credit window, chunk-group-aligned request span)`.
    ///
    /// It does not distinguish them, so a spike cannot be attributed to a layer
    /// from this series alone — the log line at the refusal site is what says
    /// which. Guards (1) and (3) also split by CAP: both run the two-level floor
    /// check, and its per-signer arm bumps `serve_stream_rejected_signer_floor_at_cap`
    /// instead, so this counter is the pool-ceiling half of them.
    /// Signed as `Unfunded`: every guard runs past the lane-ownership proof, so
    /// its audience is the proven requester (ADR 005 §Open-time refusal classes).
    /// Visible name: `decdn_serve_stream_rejected_insufficient_deposit_total`.
    pub serve_stream_rejected_insufficient_deposit: Counter,
    /// Delivery refused at admission because a wired `PoolView` could not confirm
    /// the pool on-chain — the pool has no on-chain record, is closed/reclaimed, or
    /// the admit-path `getPool` faulted — or could not confirm the voucher signer:
    /// the admit-path `getAuthorization` faulted and the node holds no cached
    /// registered read of the signer. Signed as `NotFound`, like a cache miss,
    /// so this counter is the only place the reason lives;
    /// kept distinct from `insufficient_deposit` so a real drained-pool refusal is
    /// not conflated with an unconfirmed or unreachable pool. Visible name:
    /// `decdn_serve_stream_rejected_pool_unconfirmed_total`.
    pub serve_stream_rejected_pool_unconfirmed: Counter,
    /// Delivery refused at admission because the request's pool is `Closing`
    /// on-chain: it takes no top-up, and its redemption ends at the dispute
    /// deadline (ADR 003 §Pool solvency). Signed as `Unfunded` to a requester
    /// that holds a lane on the pool, so its owner opens a new pool, and as
    /// `NotFound` to anyone else. Visible name:
    /// `decdn_serve_stream_rejected_pool_closing_total`.
    pub serve_stream_rejected_pool_closing: Counter,
    /// Delivery refused at admission because the request's voucher signer
    /// cannot pay: it is registered on-chain with `cap − spent` below a serve
    /// floor or with an expired registration (a "spent" capability the node
    /// could never cash), or its lane's capability is inside the node's expiry
    /// margin. A `getAuthorization` fault never counts here (see
    /// `pool_unconfirmed`). Signed as `Unfunded`: the requester holds a verified
    /// binding for a registered signer or a lane, so it is proven (ADR 005
    /// §Open-time refusal classes). A rising value flags capabilities presented
    /// whose signer has drained its shared `cap` at other nodes. Visible name:
    /// `decdn_serve_stream_rejected_signer_cap_exhausted_total`.
    pub serve_stream_rejected_signer_cap_exhausted: Counter,
    /// A live serve was stopped MID-STREAM because the request's voucher signer
    /// drained its shared on-chain `cap` at other nodes AFTER admission — the
    /// event-fed projection's `signer_spent` rose until the `cap − spent` headroom
    /// the node holds on the lane no longer covers a serve floor (ADR 003 §Pool
    /// solvency). The mid-stream twin of `serve_stream_rejected_signer_cap_exhausted`
    /// (which is the admit-time refusal), kept distinct so a running value flags
    /// cross-node cap drain on long streams — the large-blob over-delivery this
    /// re-check bounds — rather than an open-time refusal. The stream stops in-band
    /// with a `VoucherRejected { SignerCapExhausted }`, which the requester treats
    /// as `Unfunded` from this node. Visible name:
    /// `decdn_serve_stream_midstream_signer_cap_exhausted_total`.
    pub serve_stream_midstream_signer_cap_exhausted: Counter,
    /// Delivery refused because ONE capability signer hit its per-signer live
    /// concurrency cap of un-vouchered reservation, while the pool itself can still
    /// pay (ADR 003 §Pool solvency, per-signer floor isolation).
    /// Signed as `NotFound`, because it clears by waiting rather than by funding
    /// (ADR 005 §Open-time refusal classes), so this counter is the only place
    /// the reason survives — a rising value means one signer runs
    /// more concurrent un-vouchered streams than its share covers while the pool as a
    /// whole is solvent. The two remedies differ: a pool shortfall clears with a
    /// top-up, a signer at its share does not. Visible name:
    /// `decdn_serve_stream_rejected_signer_floor_at_cap_total`.
    pub serve_stream_rejected_signer_floor_at_cap: Counter,
    /// Whole-blob requests from an active staker refused while this node's own
    /// upstream open for the blob was in progress (#2224): the guard that stops
    /// partial holders pulling a blob from each other in a loop. Signed as
    /// `NotFound`, so this counter is the only place the reason lives. A rate
    /// that tracks node-to-node pulls of one blob shows the guard breaking loops;
    /// the debug line `refusing a whole-blob request from a node` names the
    /// requester. Visible name: `decdn_serve_stream_rejected_pull_loop_guard_total`.
    pub serve_stream_rejected_pull_loop_guard: Counter,
    /// Delivery refused because the requested bounded range starts at or past
    /// this node's size claim for the blob (ADR 005 §Bounded byte ranges).
    /// Signed as `Declined`: the size is this node's claim, and another node
    /// can disagree. A rising value flags clients issuing ranges past the end,
    /// or nodes that disagree on a blob's size. Visible name:
    /// `decdn_serve_stream_rejected_range_not_satisfiable_total`.
    pub serve_stream_rejected_range_not_satisfiable: Counter,
    /// Delivery refused because a pull-through request starts at or past this
    /// node's `max_blob_size` ceiling (ADR 005 §`max_blob_size` enforcement).
    /// The node refuses before it commits to the stream, so no byte is bought
    /// upstream. Signed as `Declined`. A rising value means clients ask
    /// this node for blobs above its ceiling. Visible name:
    /// `decdn_serve_stream_rejected_blob_too_large_total`.
    pub serve_stream_rejected_blob_too_large: Counter,
    /// Delivery refused because the blob is on this operator's local denylist
    /// (ADR 011 §Local Denylist). Signed as `Declined`. Deliberately
    /// counts ONLY the local list; the governance blacklist has its own
    /// counter, [`Self::serve_stream_rejected_chain_hash_denied`]. Splitting
    /// them here is safe where the wire code must not: this is the operator's
    /// own gauge of their own denylist, not something a client can probe. A
    /// rising value after a takedown is the confirmation the order is being
    /// discharged. Visible name:
    /// `decdn_serve_stream_rejected_hash_denied_total`.
    pub serve_stream_rejected_hash_denied: Counter,
    /// Delivery refused because the blob is on the *governance* blacklist (ADR
    /// 011 §On Blacklist Event). Also signed as `Declined` — identically
    /// to the local list, which is the ADR's requirement — so this counter is
    /// the only place the two are distinguishable, and it is readable by the
    /// operator alone. Visible name:
    /// `decdn_serve_stream_rejected_chain_hash_denied_total`.
    ///
    /// Not to be confused with `serve_stream_rejected_evicted_since_probe`,
    /// which sees only evictions with no blacklist entry behind them (corruption
    /// recovery, a manual `decdn node evict`).
    pub serve_stream_rejected_chain_hash_denied: Counter,
    /// Delivery refused because the channel's funding address is blacklisted as
    /// an origin — local `denied_origins` or the on-chain `ContentBlacklist`
    /// (ADR 011 §On Blacklist Event). Signed as `Declined`. Visible
    /// name: `decdn_serve_stream_rejected_origin_denied_total`.
    pub serve_stream_rejected_origin_denied: Counter,
    /// Delivery declined by the origin-only policy (#1759,
    /// `cache.relay_foreign_namespaces = false`): a memoized live probe of this
    /// node's own backend genuinely does not hold the hash. Signed as
    /// `Declined`: this node will not relay the hash, and another node may
    /// (ADR 005 §Open-time refusal classes). Distinct from a backend FAULT
    /// during that same probe, which is never counted here: a fault is not an
    /// absence and lands in
    /// [`Self::serve_stream_rejected_internal_error`] instead. Visible name:
    /// `decdn_serve_stream_rejected_foreign_declined_total`.
    pub serve_stream_rejected_foreign_declined: Counter,
    /// A serve refused because the node has been unable to reach the chain for
    /// longer than `blockchain.chain_staleness_grace_sec` (ADR 011 § Serving
    /// while chain-stale): its deny-set, pool-solvency, and signer-cap guards
    /// are all reading stale state, so it refuses rather than sign a serve it
    /// cannot vouch for. Signed as `NotFound`, identically to
    /// [`Self::serve_stream_rejected_cache_miss`] — the client should re-route
    /// to a peer whose chain reads are live — so this counter is the only place
    /// an operator sees the staleness refusals. Visible name:
    /// `decdn_serve_stream_rejected_chain_stale_total`.
    pub serve_stream_rejected_chain_stale: Counter,
    /// An ALREADY-RUNNING delivery cut off at an MB boundary because a takedown
    /// landed after the stream opened (ADR 011 §On Blacklist Event). Visible
    /// name: `decdn_serve_stream_terminated_takedown_total`.
    ///
    /// Distinct from the `rejected_*` family above, which counts refusals at
    /// stream open. This one is the operator's evidence that the compliance
    /// window was honored for traffic already in flight — the case that would
    /// otherwise keep a multi-GB blob flowing for minutes after the order took
    /// effect, which is the slashable one.
    pub serve_stream_terminated_takedown: Counter,
    /// `decdn_serve_stream_rejected_load_shed_hit_total`: cache-hit serves shed
    /// under node overload (egress saturation).
    pub serve_stream_rejected_load_shed_hit: Counter,
    /// `decdn_serve_stream_rejected_load_shed_miss_total`: cache-miss serves shed
    /// under node overload (concurrency pressure or per-client fairness).
    pub serve_stream_rejected_load_shed_miss: Counter,
    /// `decdn_serve_frame_accounting_fault_total`: a serve leg refused to cut or
    /// frame a `ChunkData` because its own byte accounting did not add up — a zero
    /// frame target, a `queued`/`queue` desync, a payload whose chunks disagree with
    /// the length the header would declare, or a header the encoder rejected.
    ///
    /// Every one of those is a node-side bug, never peer behaviour, and each aborts
    /// the delivery without `StreamEnd` so the client neither receives a mislabelled
    /// frame nor pays the closing voucher. The refusals are correct; this counter
    /// exists because they are otherwise invisible — the stream simply ends, which
    /// reads to an operator as a client that hung up. Any nonzero value is a
    /// **latent-bug report, not a degradation**: alert on `> 0` and file it rather
    /// than tuning anything.
    pub serve_frame_accounting_fault: Counter,
    /// `decdn_serve_stream_node_fault_total`: a `cdn/client/v1` delivery ended on a
    /// fault this node caused — an encode fault, an alignment error, a store fault,
    /// a framing fault — rather than on a peer hang-up or a client payment fault.
    ///
    /// The client sees only a short stream, so without this counter the whole class
    /// is visible solely in the `error!` log line beside it. Any nonzero value is a
    /// **latent-bug report, not a degradation**: alert on `> 0` and file it. It is a
    /// superset of `decdn_serve_frame_accounting_fault_total`, which meters one of
    /// the four classes on its own.
    pub serve_stream_node_fault: Counter,
    /// `decdn_serve_stream_proof_budget_exhausted_total`: a `cdn/client/v1`
    /// delivery ended because the payer sent `MAX_PROOFS_PER_CHUNK` proofs for one
    /// chunk and none of them settled it. Counted for both the cache-hit and the
    /// miss serve loop; on the miss loop the same exit also counts on
    /// `decdn_node_pull_through_client_abandoned_total`.
    ///
    /// Each such proof credits part of the chunk or nothing, so a payer that
    /// answers one chunk with zero-credit vouchers or partial payments ends here.
    /// This budget is what stops such a payer from holding a stream open (#2132).
    /// It is the payer's fault, not a node fault, so the serve loop logs it only
    /// at `debug!` and ends the stream as a stop; this counter is what makes the
    /// rate visible at the default log level. Each bump ends exactly one inbound
    /// stream as failed.
    pub serve_stream_proof_budget_exhausted: Counter,
    /// `decdn_serve_stream_rejected_stream_cap_full_total`: `cdn/client/v1`
    /// streams reset with `RATE_LIMITED` because the connection already had
    /// `max_concurrent_streams` streams in flight. No signed response is sent.
    /// A client that opens more concurrent streams than the cap drives it.
    pub serve_stream_rejected_stream_cap_full: Counter,
    /// `decdn_serve_stream_request_unreadable_total`: `cdn/client/v1` streams
    /// reset because the node could not read the first request: the read timed
    /// out, the peer reset the stream or dropped the connection, or the frame or
    /// its extensions failed to decode. Peer-side, logged at `debug!`.
    pub serve_stream_request_unreadable: Counter,
    /// `decdn_serve_stream_voucher_rejected_total`: paying `cdn/client/v1`
    /// streams the node stopped because it rejected a voucher or a chunk
    /// preimage: for example a wrong pool or signer, a bad signature, an
    /// underpayment, a regression, an expired capability, or a voucher that fails
    /// the rate check. Counted for both the cache-hit and the miss serve loop; on
    /// the miss loop the same exit also counts on
    /// `decdn_node_pull_through_client_abandoned_total`. A rejection counts here
    /// even when the peer leaves before it reads the reject frame.
    pub serve_stream_voucher_rejected: Counter,
    /// `decdn_serve_stream_client_declined_total`: `cdn/client/v1` streams the
    /// requester left before any voucher credited a byte. The routine shapes are a
    /// requester that reads the signed `StreamResponse` and does not take the
    /// quote, and the header handshake a downstream node's miss pull opens and
    /// closes without adopting. A peer that sends a malformed proof before paying
    /// counts here too. The reference requester closes every stream with code `0`,
    /// and the node does not read the code, so it sees only that the peer left,
    /// not why. Peer-side, logged at `debug!`.
    pub serve_stream_client_declined: Counter,
    /// `decdn_serve_stream_client_abandoned_total`: paying `cdn/client/v1`
    /// streams the requester left or broke the protocol on after at least one
    /// voucher credited bytes: it stopped reading, reset the stream, dropped the
    /// connection, stopped sending proofs, or sent a malformed proof. A fully paid
    /// stream whose `StreamEnd` write fails counts here too. Peer-side, logged at `debug!`. A sustained rise against
    /// `decdn_streams_completed_total{direction="inbound"}` flags clients that
    /// give up mid-delivery.
    pub serve_stream_client_abandoned: Counter,
    /// `decdn_load_shed_egress_bps`: current measured egress EWMA, bytes/sec.
    pub load_shed_egress_bps: Gauge,
    /// `decdn_load_shed_pressure_active`: 1 while the load-shed policy considers
    /// the node pressured, else 0.
    pub load_shed_pressure_active: Gauge,
    /// `decdn_load_shed_streams_in_flight`: node-wide serves in flight, sampled
    /// from the shed controller's live counter beside the egress EWMA. Read it
    /// against the configured high-water mark to see how close the node is to
    /// shedding on concurrency — the `pressure_active` gauge only says whether
    /// the mark is crossed, not the headroom.
    pub load_shed_streams_in_flight: Gauge,
    /// `decdn_load_shed_refused_node_at_capacity_total`: new serves shed because
    /// node-wide concurrency was above the high-water mark. The
    /// [`crate::load_shed::ShedReason::NodeAtCapacity`] sibling — per
    /// `adr/appendix-observability.md` § Reason splits each shed reason is a
    /// sibling counter, because the remedy differs: this one says add capacity or
    /// raise `max_concurrent_serves_high`. Orthogonal to the hit/miss split in
    /// `serve_stream_rejected_load_shed_{hit,miss}`, which counts the same
    /// refusals by cache class.
    pub load_shed_refused_node_at_capacity: Counter,
    /// `decdn_load_shed_refused_egress_saturated_total`: new serves shed because
    /// measured egress reached the configured budget
    /// ([`crate::load_shed::ShedReason::EgressSaturated`]). Remedy: raise
    /// `egress_budget_mbps` or add bandwidth. See
    /// [`Self::load_shed_refused_node_at_capacity`].
    pub load_shed_refused_egress_saturated: Counter,
    /// `decdn_load_shed_refused_client_at_capacity_total`: new serves shed
    /// because one client already held its fair share while the node was
    /// pressured ([`crate::load_shed::ShedReason::ClientAtCapacity`]). This is
    /// fairness working as designed, not node distress — remedy is usually none.
    /// See [`Self::load_shed_refused_node_at_capacity`].
    pub load_shed_refused_client_at_capacity: Counter,
    /// `decdn_warming_speculative_blocked_total` (ADR 041): times an above-floor
    /// (speculative) warming buy was downgraded to the amortized floor because
    /// the upstream source's per-source allowance was spent. Counted only when
    /// warming is enabled (`warming_budget > 0`), so a node with warming off does
    /// not report every source as blocked. A sustained rate means one or more
    /// sources are being griefed down — pair it with
    /// [`Self::warming_sources_blocked`] for how many.
    pub warming_speculative_blocked: Counter,
    /// `decdn_warming_sources_blocked` (ADR 041): how many upstream sources
    /// currently hold no warming allowance (spent ledger, refill projected
    /// forward), sampled beside the egress EWMA. A rising gauge with a rising
    /// [`Self::warming_speculative_blocked`] is warming being throttled by real
    /// per-source losses; either at zero means warming is unconstrained.
    pub warming_sources_blocked: Gauge,

    // ---- Uniform watcher liveness + panic surface (#1316, #1320) ----
    //
    // The `*_down_seconds` family below is edge-triggered off a tick *error*, so
    // a watcher that panicked while healthy, wedged in an await, or exited
    // cleanly leaves `down_since == None` and reads a healthy `0` forever — a
    // dead watcher is byte-identical to a live one. These two families close
    // that gap for all five chain-event watchers: `*_last_tick_timestamp_seconds`
    // is a *positive* liveness signal a dead task cannot advance, and
    // `*_task_panicked_total` makes an otherwise-discarded task panic visible.
    /// `decdn_slash_watcher_last_tick_timestamp_seconds` (#1316): Unix time of
    /// the slash watcher's last successful poll tick, stamped every tick (not
    /// edge-triggered). Alert on staleness — `time() - value > 3 ×
    /// poll_interval` — to catch a panicked, wedged, or cleanly-exited task that
    /// `slash_watcher_down_seconds` (error-triggered) cannot. `0` until the
    /// first successful tick.
    pub slash_watcher_last_tick_timestamp_seconds: Gauge,
    /// `decdn_staker_set_watcher_last_tick_timestamp_seconds` (#1316): Unix time
    /// of the staker-set watcher's last successful poll tick. Staleness
    /// semantics as `slash_watcher_last_tick_timestamp_seconds`.
    pub staker_set_watcher_last_tick_timestamp_seconds: Gauge,
    /// `decdn_capacity_bond_registry_last_resync_timestamp_seconds`: Unix time
    /// of the capacity-bond registry's last successful re-enumeration of
    /// `getRegisteredNodes`.
    ///
    /// Distinct from the tick gauge above: a tick is the event tail, this is the
    /// authoritative re-read that repairs a drifted set. It is the only signal
    /// that catches a resync being *skipped* — the reconcile does not run at all
    /// while the route is errored, which emits nothing, not even the failure
    /// counter. Stamped by the bootstrap enumeration too, which is the same read
    /// against the same contract; without that a node whose every later resync
    /// fails would hold the gauge at `0` and never trip a `> 0`-guarded alert.
    /// The guard is still needed for the window before bootstrap completes, and
    /// the threshold must exceed `REGISTRY_RESYNC_INTERVAL`.
    pub capacity_bond_registry_last_resync_timestamp_seconds: Gauge,
    /// `decdn_blacklist_watcher_last_tick_timestamp_seconds` (#1316, #1320): Unix
    /// time of the blacklist watcher's last successful poll tick. This is the
    /// signal that distinguishes a live blacklist loop from a dead one — a dead
    /// loop keeps serving content blacklisted after the failure (slashable via
    /// `SlashJudge.submitBlacklistChallenge`) while `blacklist_watcher_down_seconds`
    /// reads `0`.
    pub blacklist_watcher_last_tick_timestamp_seconds: Gauge,
    /// `decdn_settlement_watcher_last_tick_timestamp_seconds` (#1316): Unix time
    /// of the payment-settlement watcher's last successful poll tick.
    pub settlement_watcher_last_tick_timestamp_seconds: Gauge,
    /// `decdn_fee_shares_watcher_last_tick_timestamp_seconds`: Unix time of the
    /// fee-shares watcher's last successful poll tick. Load-bearing: the
    /// watcher's `getShares()` poll failure and undecodable-log paths both
    /// return `Ok` by design, so a watcher stuck in RPC backoff — or dead — is
    /// otherwise indistinguishable from a healthy one while the node keeps
    /// signing quotes against a stale operator fee share. Alert on this gauge
    /// going stale.
    pub fee_shares_watcher_last_tick_timestamp_seconds: Gauge,
    /// `decdn_slash_watcher_task_panicked_total` (#1316): the slash watcher task
    /// unwound on a panic. Bumped from a `Drop` guard in `multiplexed_poller::run`
    /// — the only thing that still runs on the unwind, since nothing awaits the
    /// detached task. Any non-zero value is a bug in this node.
    pub slash_watcher_task_panicked: Counter,
    /// `decdn_staker_set_watcher_task_panicked_total` (#1316): the staker-set
    /// watcher task unwound on a panic. See `slash_watcher_task_panicked`.
    pub staker_set_watcher_task_panicked: Counter,
    /// `decdn_blacklist_watcher_task_panicked_total` (#1316, #1283): the
    /// blacklist watcher task unwound on a panic.
    pub blacklist_watcher_task_panicked: Counter,
    /// `decdn_fee_shares_watcher_task_panicked_total`: the fee-shares watcher
    /// task unwound on a panic. Any non-zero value is a bug in this node.
    pub fee_shares_watcher_task_panicked: Counter,
    /// `decdn_settlement_watcher_task_panicked_total` (#1316): the
    /// payment-settlement watcher task unwound on a panic.
    pub settlement_watcher_task_panicked: Counter,

    // ---- Shared chain-event poller: eth_getLogs window span ----
    /// `decdn_chain_get_logs_span`: block span of the chain-event poller's next
    /// `eth_getLogs` window. It starts at `blockchain.get_logs_max_block_span`,
    /// drops to half a window the RPC provider rejects for its range, and
    /// doubles back after 32 accepted windows, up to the ceiling. A caught-up
    /// node climbs back to the ceiling after a transient rejection, so a low
    /// value is a recent rejection, not proof of a cap: read the rejection
    /// counter for that. The lowest values over a range with rejections show
    /// the provider's cap.
    pub chain_get_logs_span: Gauge,
    /// `decdn_chain_get_logs_range_rejections_total`: `eth_getLogs` windows the
    /// RPC provider rejected for their block range or result count. Each one
    /// costs one wasted request. Rejections that keep coming mean the provider
    /// caps `eth_getLogs`; set `blockchain.get_logs_max_block_span` to the
    /// lowest span the gauge reaches while they come, and they stop.
    pub chain_get_logs_range_rejections: Counter,
    /// `decdn_chain_get_logs_retries_total` (#2161): in-tick retries of
    /// `eth_getLogs` windows after a transient provider error. One window adds
    /// up to two, including a window that fails every retry and is deferred
    /// (`decdn_chain_get_logs_deferred_total`). A retry that succeeds does not
    /// count as watcher downtime, so a provider that the retries absorb shows
    /// here and not in the watchers' down-family. Rate limits and permanent
    /// errors are not retried.
    pub chain_get_logs_retries: Counter,
    /// `decdn_chain_get_logs_deferred_total` (#2253): `eth_getLogs` windows
    /// that failed every in-tick retry, so the poller ended the tick there and
    /// resumed the window on the next tick. A deferral is not watcher downtime;
    /// the tick fails, and the watchers go down, only when no tick has reached
    /// head for the stall budget (2 min).
    pub chain_get_logs_deferred: Counter,
    /// `decdn_chain_boot_read_retries_total` (#2159): retries of a boot-time
    /// chain read (the registry, slash, `usdc()` self-check and blacklist
    /// bootstraps, and the best-effort fee-share reads) after a transient
    /// error; one read can retry many times. The metrics
    /// listener binds after these reads, so the value becomes visible once boot
    /// completes. A rise after a restart points at the RPC provider.
    pub chain_boot_read_retries: Counter,

    // ---- Down-family parity for the watchers that lacked it (#1283, #1316) ----
    /// `decdn_blacklist_watcher_restarts_total` (#1283): distinct drift windows
    /// the blacklist watcher entered, bumped once on the edge into the
    /// error/backoff state. Mirrors `slash_watcher_restarts`.
    pub blacklist_watcher_restarts: Counter,
    /// `decdn_blacklist_watcher_down_seconds` (#1283): seconds the blacklist
    /// watcher has been failing its chain read (`0` on a healthy cycle),
    /// recomputed at scrape from `blacklist_watcher_down_since`. Tracks
    /// chain-read outages only — an enforcement failure that leaves a blacklisted
    /// blob servable is a separate signal (`blacklist_enforcement_failures`).
    pub blacklist_watcher_down_seconds: Gauge,
    /// `decdn_settlement_watcher_restarts_total` (#1316): distinct drift windows
    /// the payment-settlement watcher entered. Mirrors `slash_watcher_restarts`.
    pub settlement_watcher_restarts: Counter,
    /// `decdn_settlement_watcher_down_seconds` (#1316): seconds the settlement
    /// watcher has been failing its chain read, recomputed at scrape from
    /// `settlement_watcher_down_since`.
    pub settlement_watcher_down_seconds: Gauge,

    /// `decdn_blacklist_enforcement_failures_total` (#1319): distinct hashes a
    /// batched re-scope could NOT re-verify or evict this pass (`Recheck::Failed`
    /// — a disk error or a scope `eth_call` failure). An increase means a pass
    /// did not fully enforce the deny-set, so a blacklisted blob may still be
    /// servable and slashable, even while `blacklist_watcher_down_seconds` reads
    /// `0`. Answers a different question than the down-family, which tracks
    /// chain-read outages only. A boot that retries its enforcement pass also
    /// adds to it; the node serves only after a clean pass, so that increase does
    /// not mean a served blob. Pairs with the aggregate `warn!` in `rescan`.
    pub blacklist_enforcement_failures: Counter,
    /// `decdn_blacklist_reenumeration_failures_total`: times the blacklist
    /// watcher's periodic re-enumeration could not read chain state and kept the
    /// deny-set it already had. That re-enumeration is the backstop for a lost
    /// tail event and for a region/ripening transition, which emits no event.
    /// It reports success upward so the event tail keeps running, so a
    /// persistently failing backstop moves nothing else. Pairs with the `warn!`
    /// in `blacklist_watcher`'s `BlacklistSink::on_tick_complete`.
    pub blacklist_reenumeration_failures: Counter,

    // ---- Stream outcomes and volume ----
    /// `decdn_streams_completed_total{direction}`: paid streams that delivered
    /// the whole request. `inbound` counts streams this node served. `outbound`
    /// counts node-to-node pulls this node made: one per upstream candidate it
    /// opened a stream to on the buffered miss path, one per assembled range on
    /// the streaming miss path. Every ended stream counts once, in this family
    /// or in `streams_failed`; a stream cancelled by shutdown counts in
    /// neither. The reason split lives in the sibling counters
    /// (`serve_stream_rejected_*`, `node_pull_*`).
    streams_completed: Family<StreamLabels, Counter>,
    /// `decdn_streams_failed_total{direction}`: paid streams that ended without
    /// delivering the whole request — refused, stopped mid-stream, reset,
    /// panicked, or ended on an error. Routine outcomes count here too: a cache
    /// miss refusal, a requester that declines the quote — including the header
    /// handshake a downstream node's miss pull opens and does not adopt — and a
    /// client that leaves mid-stream. So read the reason siblings, not a raw
    /// ratio, for health. See `streams_completed`.
    ///
    /// Every `inbound` failure counts on exactly one sibling in
    /// [`INBOUND_FAILURE_REASONS`], so `inbound` minus their sum is zero. A
    /// positive residual is a failure no reason claims, which is a bug.
    streams_failed: Family<StreamLabels, Counter>,
    /// `decdn_bytes_served_total`: payload bytes this node wrote to clients and
    /// downstream nodes on `cdn/client/v1`, counted per frame as it is written.
    pub bytes_served: Counter,
    /// `decdn_bytes_received_total`: payload bytes this node admitted from
    /// upstream nodes in paid node-to-node pulls, counted per verified range
    /// (chunk-group aligned, so it can exceed the requested bytes).
    pub bytes_received: Counter,

    // ---- Settlement and on-chain transactions ----
    /// `decdn_pool_redemptions_total`: lane claims this node redeemed on-chain in
    /// a landed `redeemMany` — one cumulative voucher per lane, however many
    /// vouchers the lane accepted.
    pub pool_redemptions: Counter,
    /// `decdn_vouchers_received_total`: signed vouchers this node accepted as
    /// the payee on an inbound stream. `PayWord` preimage reveals are not
    /// vouchers and do not count; a `PayWord` stream counts its anchor voucher.
    pub vouchers_received: Counter,
    /// `decdn_preimage_reveals_received_total`: `PayWord` hash-chain preimage
    /// reveals this node accepted as the payee on an inbound stream, counted
    /// when a reveal advances the lane's chain frontier. Each one also hints the
    /// redeemer, so this rate bounds the redeemer's hint rate beside
    /// `decdn_vouchers_received_total`.
    pub preimage_reveals_received: Counter,
    /// `decdn_pool_grace_closes_total`: pools that entered the owner-close grace
    /// window while this node still held unredeemed vouchers on them, read from
    /// the flushed lane store when the close event arrives. Each one is revenue
    /// that must redeem before the grace window ends. A pool already closing at
    /// startup is not counted.
    pub pool_grace_closes: Counter,
    /// `decdn_onchain_tx_landed_total`: settlement transactions (`redeemMany`)
    /// mined and succeeded. The outcome family — `landed`, `reverted`,
    /// `send_failed`, `receipt_failed`, `timeout` — counts every transaction
    /// sent through `send_and_await_receipt` exactly once;
    /// `onchain_tx_receipt_recovered` sits beside it and is not an outcome.
    /// Buyer-side pool transactions go through `decdn-client` and are not
    /// counted here.
    pub onchain_tx_landed: Counter,
    /// `decdn_onchain_tx_reverted_total`: node transactions mined and reverted.
    pub onchain_tx_reverted: Counter,
    /// `decdn_onchain_tx_send_failed_total`: node transactions the RPC refused at
    /// `send` — none was issued. An oversize `redeemMany` the redeemer then
    /// halves and retries counts here once too.
    pub onchain_tx_send_failed: Counter,
    /// `decdn_onchain_tx_receipt_failed_total`: issued node transactions whose
    /// receipt wait failed and whose receipt no by-hash fetch found. The
    /// transaction is unconfirmed and may still mine.
    pub onchain_tx_receipt_failed: Counter,
    /// `decdn_onchain_tx_timeout_total`: issued node transactions whose receipt
    /// did not arrive inside the caller's bound and whose receipt no by-hash
    /// fetch found. The transaction is unconfirmed and may still mine.
    pub onchain_tx_timeout: Counter,
    /// `decdn_onchain_tx_receipt_recovered_total`: node transactions whose
    /// receipt wait failed or timed out but whose receipt a by-hash fetch then
    /// found. Each one also counts once as landed or reverted, so this sits
    /// beside the outcome family, not inside it. A steady rate points at a
    /// lagging or load-balanced RPC provider.
    pub onchain_tx_receipt_recovered: Counter,

    // ---- DHT ----
    /// `decdn_dht_findvalue_queries_total`: `FIND_VALUE` lookups this node ran
    /// to discover providers.
    pub dht_findvalue_queries: Counter,
    /// `decdn_dht_lookup_round_timeouts_total`: lookup rounds that hit the round
    /// timeout and aborted their in-flight RPCs.
    pub dht_lookup_round_timeouts: Counter,
    /// `decdn_dht_store_published_total`: DHT `STORE` records a peer accepted
    /// from this node.
    pub dht_store_published: Counter,
    /// `decdn_dht_routing_table_size`: distinct entries in the local Kademlia
    /// routing table, set after bootstrap and on every bucket-refresh tick.
    pub dht_routing_table_size: Gauge,
    /// `decdn_dht_bucket_refresh_failures_total`: bucket refreshes that failed:
    /// a `FIND_NODE` RPC error, a routing-table peer that is not a valid public
    /// key, or a panicked refresh task.
    pub dht_bucket_refresh_failures: Counter,
    /// `decdn_dht_bootstrap_find_node_failures_total`: bootstrap `FIND_NODE`
    /// RPCs against a seed that failed.
    pub dht_bootstrap_find_node_failures: Counter,

    // ---- Fee-shares watcher parity ----
    /// `decdn_fee_shares_watcher_unregistered`: `1` when the startup
    /// `PaymentPool.feeRouter()` read failed, so the fee-shares watcher is not
    /// running and the operator share stays at the floor for the life of the
    /// process; `0` otherwise. The watcher's own down gauges read healthy in
    /// that state, since it never started.
    pub fee_shares_watcher_unregistered: Gauge,
    /// `decdn_fee_shares_watcher_poll_failures_total`: authoritative
    /// `getShares()` re-reads that failed. The watcher keeps the current share and
    /// its tick still succeeds, so this is the only signal that the safety-net
    /// re-read is not landing.
    pub fee_shares_watcher_poll_failures: Counter,
    /// `decdn_fee_shares_watcher_restarts_total`: distinct drift windows the
    /// fee-shares watcher entered. Mirrors `slash_watcher_restarts`.
    pub fee_shares_watcher_restarts: Counter,
    /// `decdn_fee_shares_watcher_down_seconds`: seconds the fee-shares watcher
    /// has been failing its chain read, recomputed at scrape from
    /// `fee_shares_watcher_down_since`.
    pub fee_shares_watcher_down_seconds: Gauge,

    // ---- Operator-path failures ----
    /// `decdn_config_reload_failures_total`: SIGHUP or admin config reloads that
    /// failed. Sections committed before the failure stay applied; the rest keep
    /// their previous values.
    pub config_reload_failures: Counter,
    /// `decdn_receipt_write_failures_total`: download-receipt audit records the
    /// writer failed to persist (an I/O error or a panicked write task). Audit
    /// only; settlement is unaffected.
    pub receipt_write_failures: Counter,
    /// `decdn_serve_stream_midstream_pool_exhausted_total`: paying streams this
    /// node stopped mid-delivery because the pool could no longer fund the
    /// floor credit committed across its signers. The pool-level sibling of
    /// `serve_stream_midstream_signer_cap_exhausted`.
    pub serve_stream_midstream_pool_exhausted: Counter,
}

/// Per-request JSON-RPC metrics for the node's chain provider, registered under
/// the same `decdn` prefix as [`DecdnMetrics`].
///
/// A group of its own because a labelled histogram cannot deserialize, and
/// [`DecdnMetrics`] derives `Deserialize`. The `crate::rpc_metrics` transport
/// layer records into it for every request of every provider the runtime
/// builds, so these are the denominator the provider's error ratio needs.
#[derive(Debug, MetricsGroup)]
#[metrics(default, name = "decdn")]
struct RpcMetrics {
    /// `decdn_rpc_requests_total{method,outcome}`: JSON-RPC requests the node's
    /// providers sent, by method and by how each ended. Each request of a batch
    /// counts once. Every `(method, outcome)` pair exports at zero from
    /// startup.
    ///
    /// Labelled rather than split into sibling counters because every outcome
    /// shares one aggregate: the error ratio is a sum over `outcome` divided by
    /// the total. See [`RpcOutcome`] for each value.
    rpc_requests: Family<RpcRequestLabels, Counter>,
    /// `decdn_rpc_request_duration_seconds{method}`: time from sending one
    /// JSON-RPC request to its reply or transport error. A request the caller
    /// drops records no sample. Buckets: [`RPC_REQUEST_BUCKETS`].
    #[default(Family::with_constructor(|| Histogram::new(RPC_REQUEST_BUCKETS.to_vec())))]
    rpc_request_duration_seconds: Family<RpcMethodLabels, Histogram>,
}

/// Aggregated deCDN node metrics.
#[derive(Debug)]
pub struct Metrics {
    registry: Arc<RwLock<Registry>>,
    decdn: Arc<DecdnMetrics>,
    rpc: Arc<RpcMetrics>,
    cache: Arc<CacheMetrics>,
    inbound_streams: Arc<Gauge>,
    outbound_streams: Arc<Gauge>,
    inbound_completed: Arc<Counter>,
    inbound_failed: Arc<Counter>,
    outbound_completed: Arc<Counter>,
    outbound_failed: Arc<Counter>,
    /// Materialized `probe_hold_unavailable` children, one per
    /// [`ProbeHoldUnavailableReason`]. Held here for the same reason as the
    /// stream gauges: `Family` creates a child series lazily on first
    /// `get_or_create`, so without this each `reason` would be absent from
    /// `/metrics` until it first fired — an operator's dashboard and alert
    /// would show a gap rather than a zero. Creating them at startup keeps
    /// the property that all three series are exported at zero.
    probe_hold_exhausted: Arc<Counter>,
    probe_hold_disabled: Arc<Counter>,
    probe_hold_stake_lane_reserved: Arc<Counter>,
    /// Regions that currently have a `staker_set_active_by_region` child.
    /// `Family` cannot enumerate its children, so this is what tells
    /// [`Self::staker_set_active_by_region`] which ones to remove.
    published_regions: Mutex<BTreeSet<Region>>,
    started_at: Instant,
    /// Monotonic instant at which the staker-set watcher entered its current
    /// error/backoff window (#783, downtime semantics #788). `None` whenever a
    /// cycle is established (healthy) — including from bootstrap until the
    /// first cycle and after every successful re-establishment. `Some(t)` only
    /// while the loop is between a failed cycle and the next success. Backs the
    /// `staker_set_watcher_down_seconds` gauge, recomputed from this at scrape
    /// time so the value reads exactly `0` across an arbitrarily long healthy
    /// cycle and climbs only through an actual outage. A poisoned lock reports
    /// `i64::MAX` down-seconds (the conservative, alerting direction for a
    /// downtime gauge — reporting `0` would mask an in-progress outage).
    staker_set_watcher_down_since: Mutex<Option<Instant>>,
    /// `Instant` the slash-detection watcher entered its current error/backoff
    /// window (#1032). `None` whenever a cycle is established. Backs the
    /// `slash_watcher_down_seconds` gauge, recomputed at scrape time. Mirrors
    /// `staker_set_watcher_down_since`.
    slash_watcher_down_since: Mutex<Option<Instant>>,
    /// `Instant` the blacklist watcher entered its current error/backoff window
    /// (#1283). `None` whenever a cycle is established. Backs the
    /// `blacklist_watcher_down_seconds` gauge, recomputed at scrape time. Mirrors
    /// `staker_set_watcher_down_since`.
    blacklist_watcher_down_since: Mutex<Option<Instant>>,
    /// `Instant` the payment-settlement watcher entered its current error/backoff
    /// window (#1316). `None` whenever a cycle is established. Backs the
    /// `settlement_watcher_down_seconds` gauge. Mirrors
    /// `staker_set_watcher_down_since`.
    settlement_watcher_down_since: Mutex<Option<Instant>>,
    /// `Instant` the fee-shares watcher entered its current error/backoff
    /// window. `None` whenever a cycle is established. Backs the
    /// `fee_shares_watcher_down_seconds` gauge. Mirrors
    /// `staker_set_watcher_down_since`.
    fee_shares_watcher_down_since: Mutex<Option<Instant>>,
    /// Cache engine whose live probe-hold count backs the
    /// `probe_hold_slots_used` gauge, sampled once per scrape in
    /// [`Self::encode`]. Unset until runtime bring-up attaches the cache
    /// through [`Self::attach_probe_holds`]; the gauge then reads `0`.
    probe_holds: OnceLock<CacheEngine>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Saturating `usize` → `i64` for gauge values: a count too large to fit an
/// `i64` reports `i64::MAX` rather than wrapping. Saturation is the
/// conservative direction for the sizes and counts these gauges carry, and it
/// keeps the workspace `unwrap_used` deny satisfied without pushing a
/// `Result` onto every recorder signature.
fn sat(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// Saturating `u64` → `i64` for gauge values; see [`sat`].
fn sat_u64(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

fn sat_u256(n: U256) -> i64 {
    u64::try_from(n)
        .ok()
        .and_then(|raw| i64::try_from(raw).ok())
        .unwrap_or(i64::MAX)
}

/// Current wall-clock time as whole seconds since the Unix epoch, saturating.
///
/// This is deliberately `SystemTime` (wall clock), not the monotonic `Instant`
/// every other timer here uses: it backs the `*_last_tick_timestamp_seconds`
/// liveness gauges, whose alerting expression compares them against Prometheus's
/// `time()` (also wall clock). A pre-epoch clock (`Err`) reads `0` — i.e.
/// "never ticked", the alerting direction. Anti-panic: no `unwrap`/`expect`.
fn unix_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

impl Metrics {
    /// Create the registry and register deCDN's metric group plus the
    /// cache crate's `decdn_cache_*` group. The cache handle is shared
    /// with the engine via [`Self::cache_metrics`] so engine-side bumps
    /// land in the same encoder output.
    pub fn new() -> Self {
        let decdn = Arc::new(DecdnMetrics::default());
        let inbound_streams = decdn.streams_active.get_or_create(&StreamLabels {
            direction: StreamDirection::Inbound,
        });
        let outbound_streams = decdn.streams_active.get_or_create(&StreamLabels {
            direction: StreamDirection::Outbound,
        });
        // Both directions of both outcome families export at zero from a fresh
        // registry, so a rate over them never starts absent.
        let inbound = StreamLabels {
            direction: StreamDirection::Inbound,
        };
        let outbound = StreamLabels {
            direction: StreamDirection::Outbound,
        };
        let inbound_completed = decdn.streams_completed.get_or_create(&inbound);
        let inbound_failed = decdn.streams_failed.get_or_create(&inbound);
        let outbound_completed = decdn.streams_completed.get_or_create(&outbound);
        let outbound_failed = decdn.streams_failed.get_or_create(&outbound);
        // Materialize every `reason` child up front so all three series export
        // at zero from a fresh registry (see the field docs on `Metrics`).
        // Driven off `ALL` and destructured positionally so a new variant
        // cannot compile without being materialized here — see `ALL`'s docs.
        let [
            probe_hold_exhausted,
            probe_hold_disabled,
            probe_hold_stake_lane_reserved,
        ] = ProbeHoldUnavailableReason::ALL.map(|reason| {
            decdn
                .probe_hold_unavailable
                .get_or_create(&ProbeHoldUnavailableLabels { reason })
        });
        // Materialize every RPC child so each `(method, outcome)` series and
        // each per-method histogram exports at zero from a fresh registry.
        let rpc = Arc::new(RpcMetrics::default());
        for method in RpcMethod::ALL {
            for outcome in RpcOutcome::ALL {
                rpc.rpc_requests
                    .get_or_create(&RpcRequestLabels { method, outcome });
            }
            rpc.rpc_request_duration_seconds
                .get_or_create(&RpcMethodLabels { method });
        }
        let cache = Arc::new(CacheMetrics::default());
        let mut registry = Registry::default();
        registry.register(decdn.clone() as Arc<dyn MetricsGroup>);
        registry.register(rpc.clone() as Arc<dyn MetricsGroup>);
        // Cache metrics live under the `decdn_cache` prefix so they
        // share the `decdn_*` family the rest of the metrics use.
        registry
            .sub_registry_with_prefix("decdn")
            .register(cache.clone() as Arc<dyn MetricsGroup>);
        Self {
            registry: Arc::new(RwLock::new(registry)),
            decdn,
            rpc,
            cache,
            inbound_streams,
            outbound_streams,
            inbound_completed,
            inbound_failed,
            outbound_completed,
            outbound_failed,
            probe_hold_exhausted,
            probe_hold_disabled,
            probe_hold_stake_lane_reserved,
            published_regions: Mutex::new(BTreeSet::new()),
            started_at: Instant::now(),
            staker_set_watcher_down_since: Mutex::new(None),
            slash_watcher_down_since: Mutex::new(None),
            blacklist_watcher_down_since: Mutex::new(None),
            settlement_watcher_down_since: Mutex::new(None),
            fee_shares_watcher_down_since: Mutex::new(None),
            probe_holds: OnceLock::new(),
        }
    }

    /// Attach the cache engine whose probe holds back
    /// `decdn_probe_hold_slots_used`. Each scrape samples
    /// [`CacheEngine::probe_hold_slots_used`], which sweeps expired holds, so
    /// the gauge falls back to `0` once holds lapse even with no probe
    /// traffic. Only the first attach takes effect.
    pub(crate) fn attach_probe_holds(&self, cache: CacheEngine) {
        // A second attach is a no-op: the runtime builds one cache per
        // process, so the first engine is the live one.
        let _ = self.probe_holds.set(cache);
    }

    /// Shared `Arc<CacheMetrics>` for wiring into [`decdn_cache::CacheEngine`].
    pub fn cache_metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.cache)
    }

    /// Publish the count of inbound lanes the node holds open.
    ///
    /// Lane-scoped on purpose: the pool deposit behind those lanes is a
    /// pool-level on-chain quantity that no lane carries, so it is published
    /// separately by [`Self::set_pool_deposit_usdc`] on the redeemer's tick.
    /// A lane carries no pool deposit, so a lane-scoped caller could only ever
    /// pass zero for one (#2072).
    pub(crate) fn set_lanes_open(&self, open: usize) {
        self.decdn.lanes_open.set(sat(open));
    }

    /// Publish the on-chain value still recoverable from the pools currently
    /// paying this node — `Σ (deposit − totalRedeemed)` over the distinct pools
    /// the redeemer planned lanes against.
    ///
    /// Called once per redeemer self-tick beside
    /// [`Self::set_unredeemed_usdc`], so the two money gauges share a cadence
    /// and can be read against each other: `unredeemed` is what the node has
    /// earned and not yet cashed, and this is the ceiling the pools can still
    /// pay it.
    pub(crate) fn set_pool_deposit_usdc(&self, remaining: U256) {
        self.decdn.pool_deposit_usdc.set(sat_u256(remaining));
    }

    /// Publish the total raw USDC held in accepted-but-unredeemed vouchers.
    /// Called once per redeemer self-tick from the settlement sweep, so the
    /// gauge tracks what the node plans to redeem as of the last interval.
    pub(crate) fn set_unredeemed_usdc(&self, total: U256) {
        self.decdn.unredeemed_usdc.set(sat_u256(total));
    }

    /// Publish the USDC balance of this node's own buyer wallet.
    pub(crate) fn set_buyer_wallet_usdc(&self, balance: U256) {
        self.decdn.buyer_wallet_usdc.set(sat_u256(balance));
    }

    /// Count a refused pull whose lane watermark could not be established.
    pub(crate) fn buyer_lane_seed_failure(&self) {
        self.decdn.buyer_lane_seed_failures.inc();
    }

    /// Count a bootstrap that could not determine whether this node already owns
    /// a payment pool.
    pub(crate) fn buyer_pool_adoption_failure(&self) {
        self.decdn.buyer_pool_adoption_failures.inc();
    }

    /// Register iroh's transport metrics under the `decdn_iroh_` prefix so
    /// `socket_*`, `net_report_*`, etc. come out as
    /// `decdn_iroh_socket_*`, matching `adr/appendix-observability.md`'s naming convention.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned.
    pub fn register_iroh_endpoint(&self, ep: &Endpoint) -> anyhow::Result<()> {
        self.register_iroh_metrics(ep.metrics())
    }

    /// Register an [`EndpointMetrics`] set under the `decdn_iroh_` prefix.
    ///
    /// Split out from [`Self::register_iroh_endpoint`] so
    /// `iroh_metrics_export_under_the_decdn_iroh_prefix` can register
    /// the same group from an `EndpointMetrics::default()` and pin the
    /// `decdn_iroh_*` names without standing up a socket. Without an endpoint
    /// those series are absent from `Metrics::new().encode()`.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned.
    fn register_iroh_metrics(&self, metrics: &EndpointMetrics) -> anyhow::Result<()> {
        let mut reg = self
            .registry
            .write()
            .map_err(|_| anyhow::anyhow!("metrics registry lock poisoned"))?;
        reg.sub_registry_with_prefix("decdn_iroh")
            .register_all(metrics);
        Ok(())
    }

    /// Record a buyer `openPool`-tx failure broken out by cause (#966): bumps
    /// the `decdn_pool_open_failures_{reason}_total` sibling counter for
    /// `reason`. Pairs with the structured `reason` field on the `warn!`/`debug!`
    /// in [`crate::node_origin`]. Distinct from
    /// [`Self::node_pull_pool_open_failure`], the unlabeled total (which also
    /// counts store, open-task, capability-signing and lane-seed causes that never
    /// reach the `openPool` tx).
    pub fn pool_open_failure_by_reason(&self, reason: PoolOpenFailureReason) {
        match reason {
            PoolOpenFailureReason::InsufficientDeposit => {
                self.decdn.pool_open_failures_insufficient_deposit.inc();
            }
            PoolOpenFailureReason::ContractRevert => {
                self.decdn.pool_open_failures_contract_revert.inc();
            }
            PoolOpenFailureReason::RpcError => {
                self.decdn.pool_open_failures_rpc_error.inc();
            }
        }
    }

    /// Record an ADR 041 serve-economics refusal: bumps the
    /// `decdn_serve_economics_refused_total` aggregate and the matching regime
    /// sibling. Enum-dispatch shape of [`Self::pool_open_failure_by_reason`];
    /// both counters are plain siblings on `self.decdn`, so they export at zero
    /// from a fresh registry without pre-materialization. Pairs with the
    /// structured `debug!` at the refusal site in [`crate::node_origin`].
    pub fn serve_economics_refused(&self, regime: ServeEconomicsRegime) {
        self.decdn.serve_economics_refused.inc();
        match regime {
            ServeEconomicsRegime::Warming => self.decdn.serve_economics_refused_warming.inc(),
            ServeEconomicsRegime::Amortized => self.decdn.serve_economics_refused_amortized.inc(),
        };
    }

    /// Record a load-shed refusal broken out by cause into its sibling counter
    /// (`decdn_load_shed_refused_*_total`). Per `adr/appendix-observability.md`
    /// § Reason splits each shed reason is a sibling, not a label, because the
    /// remedies differ. The exhaustive `match` is the gate: a new
    /// [`crate::load_shed::ShedReason`] variant fails to compile until it is
    /// given a counter here. Pairs with the structured `debug!` at each shed
    /// site in [`crate::handlers::client`].
    pub fn load_shed_refused(&self, reason: crate::load_shed::ShedReason) {
        use crate::load_shed::ShedReason;
        match reason {
            ShedReason::NodeAtCapacity => self.decdn.load_shed_refused_node_at_capacity.inc(),
            ShedReason::EgressSaturated => self.decdn.load_shed_refused_egress_saturated.inc(),
            ShedReason::ClientAtCapacity => self.decdn.load_shed_refused_client_at_capacity.inc(),
        };
    }

    /// Record a probe that could not be answered from a guaranteed eviction
    /// hold, broken out by cause on the `reason` label of
    /// `decdn_probe_hold_unavailable_total` (#1443). Hand-written rather than
    /// a `recorders!` entry because the pre-materialized child handles live on
    /// `Metrics`, whereas `recorders!` only reaches `self.decdn.$field`. Same
    /// enum-dispatch shape as [`Self::pool_open_failure_by_reason`], though
    /// that one's counters *are* siblings on `self.decdn`. Pairs with the
    /// structured `debug!` at each call site in [`crate::handlers::probe`].
    pub fn probe_hold_unavailable(&self, reason: ProbeHoldUnavailableReason) {
        match reason {
            ProbeHoldUnavailableReason::Exhausted => self.probe_hold_exhausted.inc(),
            ProbeHoldUnavailableReason::Disabled => self.probe_hold_disabled.inc(),
            ProbeHoldUnavailableReason::StakeLaneReserved => {
                self.probe_hold_stake_lane_reserved.inc()
            }
        };
    }

    /// Record one JSON-RPC request on `decdn_rpc_requests_total{method,outcome}`
    /// and, when it resolved, its round trip on
    /// `decdn_rpc_request_duration_seconds{method}`. `elapsed` is `None` for a
    /// request the caller dropped. The `crate::rpc_metrics` transport layer is
    /// the one caller.
    pub(crate) fn rpc_request(
        &self,
        method: RpcMethod,
        outcome: RpcOutcome,
        elapsed: Option<Duration>,
    ) {
        self.rpc
            .rpc_requests
            .get_or_create(&RpcRequestLabels { method, outcome })
            .inc();
        if let Some(elapsed) = elapsed {
            self.rpc
                .rpc_request_duration_seconds
                .get_or_create(&RpcMethodLabels { method })
                .observe(elapsed.as_secs_f64());
        }
    }

    /// Publish the active-staker set size per region: one
    /// `decdn_staker_set_active_by_region` child per entry in `counts`, plus
    /// `unknown` on `decdn_staker_set_active_unknown_region`. A region absent
    /// from `counts` has its child removed, so the map shows no stale country.
    /// Stale children are removed after the live ones are set, so a concurrent
    /// scrape never sees the family emptied part-way through an update. It can
    /// still see some regions at their new value and others at the old one.
    pub fn staker_set_active_by_region(&self, counts: &BTreeMap<Region, usize>, unknown: usize) {
        // Held for the whole update so two publishers cannot interleave their
        // set and remove passes. A poisoned lock still holds a whole set, since
        // it is only ever replaced in one assignment; at worst it misses a
        // child the panicking call created.
        let mut published = match self.published_regions.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!("published-regions Mutex poisoned; recovering inner state");
                self.published_regions.clear_poison();
                poisoned.into_inner()
            }
        };
        let family = &self.decdn.staker_set_active_by_region;
        for (region, count) in counts {
            family
                .get_or_create(&NodeRegionLabels {
                    node_region: region.as_str().to_owned(),
                })
                .set(sat(*count));
        }
        for stale in published.iter().filter(|r| !counts.contains_key(r)) {
            family.remove(&NodeRegionLabels {
                node_region: stale.as_str().to_owned(),
            });
        }
        *published = counts.keys().copied().collect();
        self.decdn
            .staker_set_active_unknown_region
            .set(sat(unknown));
    }

    /// Current value of the `dispatch_in_flight` gauge as a `u64`. Read
    /// by `admin_v1_health.in_flight_streams` (issue #604) so the
    /// `decdn node drain --wait` client can observe in-flight client
    /// streams reach 0 during a graceful drain.
    ///
    /// A negative gauge reading is forbidden by the `Permit`
    /// acquire/release RAII pair: `dispatch_permit_acquired` runs once
    /// before each `Permit` exists and `dispatch_permit_released` runs
    /// once on `Drop`. If the conversion fails the pair has been
    /// broken — we both `debug_assert!` (loud failure in tests/dev),
    /// log `tracing::error!` (visible in production), and return 0
    /// rather than wrapping to a huge `u64`. **Returning 0 here would
    /// be read by `decdn node drain --wait` as "drain complete" while
    /// real streams are still in flight**, so the loud log is the
    /// operator-actionable signal that something broke upstream.
    #[must_use]
    pub fn dispatch_in_flight_value(&self) -> u64 {
        let raw = self.decdn.dispatch_in_flight.get();
        if let Ok(v) = u64::try_from(raw) {
            return v;
        }
        debug_assert!(false, "dispatch_in_flight gauge went negative: {raw}");
        tracing::error!(
            raw,
            "dispatch_in_flight gauge is negative; the Permit \
             acquire/release pair is unbalanced. Reporting 0 to \
             avoid wraparound — `decdn node drain --wait` clients \
             may see a premature 'complete' signal until the \
             underlying accounting bug is fixed."
        );
        0
    }

    /// Read the current value of the `rpc_healthy` gauge. Test-only —
    /// non-test callers should rely on the `OpenMetrics` endpoint rather
    /// than reaching into individual gauges.
    #[cfg(test)]
    pub(crate) fn rpc_healthy_value(&self) -> i64 {
        self.decdn.rpc_healthy.get()
    }

    /// RAII guard that increments `active_connections` on construction and
    /// decrements it on drop, so the gauge stays correct even if the handler
    /// future is cancelled between open and close.
    pub fn connection_guard(&self) -> ConnectionGuard<'_> {
        ConnectionGuard::new(self)
    }

    /// Count one inbound paid-delivery stream until the returned guard drops.
    pub(crate) fn inbound_stream_guard(&self) -> StreamGuard {
        StreamGuard::new(Arc::clone(&self.inbound_streams))
    }

    /// Count one outbound paid-delivery stream until the returned guard drops.
    pub(crate) fn outbound_stream_guard(&self) -> StreamGuard {
        StreamGuard::new(Arc::clone(&self.outbound_streams))
    }

    /// An inbound serve stream ended: `completed` when it delivered the whole
    /// request, otherwise failed (`decdn_streams_{completed,failed}_total`).
    pub(crate) fn inbound_stream_ended(&self, completed: bool) {
        if completed {
            self.inbound_completed.inc();
        } else {
            self.inbound_failed.inc();
        }
    }

    /// An outbound node-to-node pull from one candidate ended: `completed` when
    /// it filled the blob, otherwise failed.
    pub(crate) fn outbound_stream_ended(&self, completed: bool) {
        if completed {
            self.outbound_completed.inc();
        } else {
            self.outbound_failed.inc();
        }
    }

    /// Render the registry as `OpenMetrics` text — exactly the body the
    /// public `/metrics` HTTP endpoint serves. `pub` (not `pub(crate)`)
    /// so integration tests in sibling crates can assert on the exported
    /// series without scraping over TCP; it exposes no data the
    /// unauthenticated `/metrics` endpoint doesn't already.
    pub fn encode(&self) -> anyhow::Result<String> {
        let uptime = i64::try_from(self.started_at.elapsed().as_secs()).unwrap_or(i64::MAX);
        self.decdn.node_uptime_seconds.set(uptime);

        // Recompute the per-watcher down-seconds gauges from their `down_since`,
        // mirroring `uptime_seconds` above (staker_set #783, slash #1032). See
        // `refresh_watcher_down_seconds` for the healthy-vs-outage and
        // poisoned-lock semantics.
        self.refresh_watcher_down_seconds();

        if let Some(cache) = self.probe_holds.get() {
            self.decdn
                .probe_hold_slots_used
                .set(sat(cache.probe_hold_slots_used()));
        }

        let reg = self
            .registry
            .read()
            .map_err(|_| anyhow::anyhow!("metrics registry lock poisoned"))?;
        reg.encode_openmetrics_to_string()
            .map_err(|e| anyhow::anyhow!("openmetrics encode failed: {e}"))
    }
}

/// Generate the trivial forwarding recorders on [`Metrics`].
///
/// Each entry spells out both names — `method => field.op(value)` — because the
/// two diverge for many of them: the method reads as a singular event while the
/// field is plural (`probe_request` vs `probe_requests`), or the field is a
/// semantic rename of the action (`connection_opened` bumps
/// `active_connections`). Deriving one name from the
/// other would bind the wrong series, so neither is ever synthesized. (Field
/// names also carry their own encoder convention — most omit the `_total`
/// suffix the `OpenMetrics` encoder appends, though a few spell it out — but
/// that governs the exported name, not the method-to-field mapping here.)
///
/// Docs pass through as `$meta`, so multi-line rationale stays byte-identical
/// and keeps its call-site span for `clippy` and `rustdoc`.
///
/// What belongs here is a body that is a single `self.decdn.field.op(expr)` —
/// including a one-expression transform of the argument (`sat(n)`,
/// `i64::from(flag)`), which several entries do. Recorders whose body needs
/// more than that — branching, multiple statements, or touching `Metrics`'s own
/// `Mutex` state — do not fit this shape: the per-watcher downtime recorders
/// (which edge-trigger off a `Mutex<Option<Instant>>` `down_since` field) have
/// their own `watcher_downtime_recorders!` family below; anything else stays
/// hand-written in the `impl` block above. There is no
/// `Counter`/`Gauge` token to pick because the op is written explicitly per
/// entry; the one dangerous confusion, calling `dec()` on a `Counter`, does not
/// compile because `Counter` has no `dec()` (a wrong `set`/`inc` on the right
/// kind still would, so the entries are the source of truth).
///
/// Note that `rustfmt` does not format macro-invocation bodies, so the entry
/// table below is hand-maintained: keep it at one entry per line, within the
/// 100-column limit, and `cargo fmt` will neither help nor complain.
macro_rules! recorders {
    ($(
        $(#[$meta:meta])*
        $method:ident $(($arg:ident: $ty:ty))? => $field:ident . $op:ident ( $($val:expr)? ) ;
    )*) => {
        impl Metrics {
            $(
                $(#[$meta])*
                pub fn $method(&self $(, $arg: $ty)?) {
                    self.decdn.$field.$op($($val)?);
                }
            )*
        }
    };
}

/// Generate the per-watcher downtime recorders on [`Metrics`].
///
/// The chain watchers (`slash`, `staker_set`, `blacklist`, `settlement`) share
/// one downtime state machine: a `*_backoff_started` recorder that, on the
/// `None -> Some` edge into an error window, stamps the watcher's
/// `Mutex<Option<Instant>>` `down_since` field and bumps its `*_restarts`
/// counter exactly once per drift window; and a `*_cycle_established` recorder
/// that clears `down_since` on recovery. A poisoned lock skips the update; the
/// scrape-time recompute then reports the `*_down_seconds` gauge as `i64::MAX`
/// — the safe alerting direction, never `0` (which would mask an outage).
///
/// These cannot live in `recorders!` because their bodies branch and touch
/// `Metrics`'s own `Mutex` state rather than a single `self.decdn.field.op(v)`.
/// Each row spells out both method names (production hooks call them by exact
/// name, and this crate adds no `paste`) and labels the three backing field
/// idents (`down_since` on `Metrics`; `restarts`/`down_seconds` on `self.decdn`)
/// so positions aren't counted. Docs pass through as `$meta`, so each
/// recorder's rationale stays byte-identical and keeps its call-site span for
/// `clippy` and `rustdoc`.
///
/// Note that `rustfmt` does not format macro-invocation bodies, so the table
/// below is hand-maintained: keep the multi-line docs tidy and the field lines
/// within the 100-column limit — `cargo fmt` will neither help nor complain.
macro_rules! watcher_downtime_recorders {
    ($(
        $(#[$backoff_meta:meta])*
        $backoff:ident,
        $(#[$established_meta:meta])*
        $established:ident,
        down_since: $down_since:ident,
        restarts: $restarts:ident,
        down_seconds: $down_seconds:ident;
    )*) => {
        impl Metrics {
            $(
                $(#[$backoff_meta])*
                // Poison note: on a poisoned `down_since` lock the whole block is
                // skipped (anti-panic policy), so `*_restarts_total` is NOT
                // incremented and, because std poison is sticky, stays frozen for
                // the process's life. The paired `*_down_seconds` gauge covers this
                // — `refresh_watcher_down_seconds` reports `i64::MAX` on a poisoned
                // lock — so alert on the gauge for outages. The counter drives only
                // the flapping rule, which a frozen counter silences while the
                // stalled rule fires on the `i64::MAX` gauge
                // (appendix-observability § Watcher Liveness, #1322).
                pub fn $backoff(&self) {
                    if let Ok(mut down_since) = self.$down_since.lock()
                        && down_since.is_none()
                    {
                        *down_since = Some(Instant::now());
                        self.decdn.$restarts.inc();
                    }
                }

                $(#[$established_meta])*
                pub fn $established(&self) {
                    if let Ok(mut down_since) = self.$down_since.lock() {
                        *down_since = None;
                    }
                }
            )*

            /// Recompute every watcher's `*_down_seconds` gauge from its
            /// `down_since` (true downtime), mirroring `uptime_seconds`. Called
            /// once per scrape from [`Self::encode`]. `None` (healthy cycle,
            /// including pre-bootstrap) reads `0` regardless of how long the
            /// cycle has been live; the value climbs once an error window opens
            /// and until the matching `*_cycle_established` clears `down_since`.
            /// A poisoned lock reports `i64::MAX` — the conservative, alerting
            /// direction for a downtime gauge, since reporting `0` would MASK an
            /// in-progress outage.
            fn refresh_watcher_down_seconds(&self) {
                $(
                    self.decdn.$down_seconds.set(match self.$down_since.lock() {
                        Ok(down_since) => down_since
                            .map(|t| t.elapsed().as_secs())
                            .map_or(0, |s| i64::try_from(s).unwrap_or(i64::MAX)),
                        Err(_) => i64::MAX,
                    });
                )*
            }
        }
    };
}

recorders! {
    /// A `cdn/probe/v1` request was served.
    probe_request => probe_requests.inc();

    /// Publish the configured `max_probe_holds` budget (registry-mandatory
    /// `decdn_probe_hold_slots_max`). Called once at runtime bring-up.
    probe_hold_slots_max(max: usize) => probe_hold_slots_max.set(sat(max));

    /// A redeem hint was dropped because the bounded advisory channel was full
    /// (`try_send` → `Full`, #751). Advisory, so a few drops are benign; a
    /// sustained rate means the redeemer is not keeping up with fan-out.
    redeem_hint_dropped => redeem_hints_dropped.inc();

    /// A redeem hint was dropped because its lane is parked after a chain
    /// fault on the hint path (#2340). The next sweep retries the lane.
    redeem_hint_parked => redeem_hints_parked.inc();

    /// A download-receipt audit write was dropped because the bounded writer
    /// queue was full (`try_send` → `Full`, #803). Audit-only, so a drop never
    /// affects settlement; a sustained rate means the writer is not keeping up
    /// with disk I/O and audit records are being lost.
    receipt_write_dropped => receipt_writes_dropped.inc();

    /// An ADR 041 warming serve credit was dropped before it reached the ledger
    /// — the bounded aggregator queue was full, or the aggregator was gone. The
    /// drop is conservative (the source ledger stays lower than reality), but a
    /// sustained rate throttles speculative warming.
    warming_credit_dropped => warming_credits_dropped.inc();

    /// An ADR 041 warming serve credit reached the ledger. Paired with
    /// `warming_credit_dropped` so an operator can tell "no credits dropped"
    /// from "no credits at all".
    warming_credit_applied => warming_credits_applied.inc();

    /// A redemption step failed: a `redeemMany` chunk reverted or was refused
    /// at `send`, or a lane could not be planned or loaded. Pairs with the
    /// `warn!` at each site.
    redemption_failure => redemption_failures.inc();

    /// A lane was held or dropped by `pool_is_redeemable` because its pool's
    /// chain-observed solvency ruled it out this pass (ADR 003).
    redemption_skipped_insolvent => redemption_skipped_insolvent.inc();

    /// A lane with value owed was skipped this pass because its signer's
    /// capability has expired, or expires within the redeemer's landing slack
    /// (ADR 003 §Revocation).
    redemption_skipped_expired => redemption_skipped_expired.inc();

    /// `n` lanes were held or dropped by `pool_is_redeemable` in one planning
    /// pass (ADR 003); the batched form of `redemption_skipped_insolvent`.
    redemption_skipped_insolvent_by(n: u64) => redemption_skipped_insolvent.inc_by(n);

    /// `n` lanes were dropped from a redeem batch by the pre-submit on-chain
    /// watermark reconciliation because the chain already shows them settled to
    /// their claim value (the `redeemMany` no-op guard, avoided before it costs
    /// gas).
    redemption_reconciled_skip_by(n: u64) => redemption_reconciled_skip.inc_by(n);

    /// A pre-submit on-chain watermark batch landed and was reconciled against
    /// the chain — the attempt signal that makes a zero
    /// `redemption_reconciled_skip` readable.
    redemption_reconcile_ok => redemption_reconcile_ok.inc();

    /// A pre-submit on-chain watermark batch did not land (RPC error, timeout,
    /// or a mismatched return length). Fail-open: the lanes it could not read
    /// are submitted unchanged.
    redemption_reconcile_failure => redemption_reconcile_failures.inc();

    /// A buyer-side reclaim-sweep pass (`reclaim_once`) failed — a store read, an
    /// RPC/receipt error, an on-chain revert, or a failed store write when
    /// clearing the local record (#906). Pairs with the per-pass `warn!` in
    /// `reclaim_once`.
    buyer_reclaim_failure => buyer_reclaim_failures.inc();

    /// Buyer-pool rows skipped as undecodable during one successful store
    /// hydration (#1271). Counted once per skipped row per load attempt (each of
    /// the startup, reconcile, and reclaim loads bumps it), so a persistent
    /// malformed row keeps the escrowed-but-untracked alert active. The
    /// `usize` count is saturated into the `u64` counter.
    buyer_pool_store_skipped_undecodable_records(count: usize)
        => buyer_pool_store_skipped_undecodable_records.inc_by(u64::try_from(count).unwrap_or(u64::MAX));

    /// A background low-water top-up (#1146) landed: a reused buyer pool below
    /// its 20% low-water mark was re-funded to the working deposit. Pairs with the
    /// `info!` in `spawn_refill_if_low`.
    buyer_topup_ok => buyer_topup_ok.inc();

    /// A background low-water top-up (#1146) did not cleanly land — the allowance
    /// re-approval or the `topUp` submit/receipt errored or reverted, OR the `topUp`
    /// landed on-chain but the local row vanished/rotated during the RPC
    /// (`DepositOutcome::UnknownPool` / `PoolMismatch`, i.e. escrowed-but-
    /// untracked). Folding the untracked case in here keeps stranded deposits
    /// visible on this counter. Best-effort, so the pool is left un-topped; pairs
    /// with the `warn!` (or `top_up`'s own error!/warn!) around the call in
    /// `spawn_refill_if_low`.
    buyer_topup_failure => buyer_topup_failure.inc();

    /// The settlement watcher failed to forget a reclaimed pool's lane state
    /// (`forget_lane`) on a `PoolReclaimed` event, in `PoolSettlementSink`
    /// (`payment_settlement.rs`). The failure is swallowed — logged with a
    /// per-site `warn!` ("failed to forget reclaimed lane") and the cursor
    /// advances — so it is a lane-store health signal, not a stuck settlement.
    watcher_persist_failure => watcher_persist_failures.inc();

    /// A lane-store background flush failed — the dirty in-memory set is
    /// retained and retried on the next timer tick, or a final flush on
    /// shutdown failed. Pairs with the `warn!`s in the background flush task
    /// and the shutdown flush in `runtime/mod.rs`.
    lane_flush_failure => lane_flush_failures.inc();

    /// A distinct slash against this node's operator was detected by the slash
    /// watcher (#1032). Counts each `slashId` once (backfill + live dedup).
    slash_detected => slashes_detected.inc();
    /// A slash-watcher resync failed and the current detected-slash set was
    /// kept. Bumps `slash_resync_failures_total`.
    slash_resync_failure => slash_resync_failures.inc();

    /// A `nodeIdOf(operator)` resolution for an operator-indexed event failed,
    /// dropping the membership change (#788, [`crate::dht::chain_staker_set`]).
    /// Bumps `staker_set_watcher_resolve_failures_total`. Pairs with the
    /// per-failure `warn!` in [`crate::dht::capacity_bond_registry`]'s
    /// `RegistrySink::on_operator_change`.
    staker_set_watcher_resolve_failure => staker_set_watcher_resolve_failures.inc();

    /// Publish the current cached active-staker set size (#783). Sampled on
    /// every membership change the watcher applies, so the gauge tracks the
    /// cached view — which under a watcher outage is exactly the (possibly
    /// stale) set that admission decisions read.
    staker_set_active_count(count: usize) => staker_set_active_count.set(sat(count));

    /// Publish the current cached `NodeId → operator address` binding count
    /// (#831). Sampled on every binding change the node-address watcher applies,
    /// so it tracks the cached view the pull path resolves against.
    node_address_directory_size(count: usize) => node_address_directory_size.set(sat(count));

    /// A node-to-node pull orchestration found ≥1 candidate and is attempting a
    /// fill (#831).
    node_pull_attempt => node_pull_attempts.inc();

    /// A node-to-node pull delivered verified bytes (#831).
    node_pull_success => node_pull_success.inc();

    /// A cache miss surfaced no provider from discovery (#831).
    node_pull_no_providers => node_pull_no_providers.inc();

    /// A cache-miss pull was served from the ADR 001 probe cache: no DHT lookup,
    /// no probe fanout (#1165).
    probe_cache_hit => probe_cache_hits.inc();

    /// A cache-miss pull found no usable probe-cache entry and ran a fresh
    /// lookup + probe (#1165).
    probe_cache_miss => probe_cache_misses.inc();

    /// One probe round's collection window ended (ADR 001 §Probe response
    /// collection): `elapsed` runs from the start of the concurrent probes to
    /// the end of collection.
    probe_collection_latency(elapsed: Duration) =>
        probe_collection_latency_seconds.observe(elapsed.as_secs_f64());

    /// One node-to-node pull leg read its first bao bytes `elapsed` after its
    /// open started.
    node_pull_first_byte(elapsed: Duration) =>
        node_pull_first_byte_seconds.observe(elapsed.as_secs_f64());

    /// A DHT lookup was truncated at `MAX_LOOKUP_ROUNDS` while still finding closer nodes
    /// (#1145 review). A sustained rate means the ceiling is too low for the network size.
    dht_lookup_round_ceiling => dht_lookup_round_ceiling.inc();

    /// An upstream served hash-mismatched bytes for a paid pull (#831).
    node_pull_corruption => node_pull_corruption.inc();

    /// A probe or pull to a candidate failed at the transport (#831).
    node_pull_unreachable => node_pull_unreachable.inc();

    /// A probed peer claimed this node's own region but exceeded the ADR 030
    /// latency ceiling, so it took the local reputation penalty (#1177).
    node_region_latency_penalty => node_region_latency_penalty.inc();

    /// A buyer pool open/reuse failed before a pull could start (#831).
    node_pull_pool_open_failure => node_pull_pool_open_failures.inc();

    /// A selected upstream claimed a `total_bytes` above this node's
    /// `max_blob_size` ceiling and the buyer rejected it before buffering
    /// (#840). A buyer-side policy decision, so it does not score the provider.
    node_pull_too_large => node_pull_too_large.inc();

    /// A selected upstream quoted a per-MB rate above this node's effective buyer
    /// ceiling and the buyer refused before paying (#1375). A buyer-side policy
    /// decision, so it does not score the provider.
    node_pull_rate_above_ceiling => node_pull_rate_above_ceiling.inc();

    /// Record a paid-delivery (`serve_stream`) request refused because the
    /// blob was evicted between probe and stream (#876).
    serve_stream_rejected_evicted_since_probe => serve_stream_rejected_evicted_since_probe.inc();

    /// Record a `serve_stream` request refused on a cache miss that
    /// pull-through could not fill (#876).
    serve_stream_rejected_cache_miss => serve_stream_rejected_cache_miss.inc();

    /// Record a `serve_stream` request refused by a local store fault,
    /// signed as `Declined` (#876).
    serve_stream_rejected_internal_error => serve_stream_rejected_internal_error.inc();

    /// Record a `serve_stream` request refused on an unknown lane (#876).
    serve_stream_rejected_unknown_lane => serve_stream_rejected_unknown_lane.inc();

    /// Record a `serve_stream` request refused because the client binding did
    /// not authorize the named channel (#876).
    serve_stream_rejected_owner_mismatch => serve_stream_rejected_owner_mismatch.inc();
    /// A client binding failed to verify and the stream was reset.
    serve_stream_rejected_bad_binding => serve_stream_rejected_bad_binding.inc();
    /// A probe request could not be read.
    probe_read_fault => probe_read_faults.inc();

    /// Record a `serve_stream` cache-miss refused by the pre-flight deposit guard
    /// (#856): the requesting channel could not cover the worst-case blob cost,
    /// so no upstream pull was started.
    serve_stream_rejected_insufficient_deposit => serve_stream_rejected_insufficient_deposit.inc();

    /// Record a `serve_stream` admission refused because a wired `PoolView` could
    /// not confirm the pool on-chain (absent, closed, or the admit `getPool`
    /// faulted), or a `getAuthorization` fault left the signer with no cached
    /// registered read — kept distinct from a real deposit-exhaustion refusal.
    serve_stream_rejected_pool_unconfirmed => serve_stream_rejected_pool_unconfirmed.inc();

    /// Record a `serve_stream` admission refused because its pool is `Closing`.
    serve_stream_rejected_pool_closing => serve_stream_rejected_pool_closing.inc();

    /// Record an admit signer confirm the signer-auth cache answered.
    serve_signer_auth_cached => serve_signer_auth_cached.inc();

    /// Record an admit signer confirm for a `(pool, signer)` with no held read.
    serve_signer_auth_first_read => serve_signer_auth_first_read.inc();

    /// Record an admit signer confirm that re-reads an `Unregistered` signer.
    serve_signer_auth_reread => serve_signer_auth_reread.inc();

    /// Record a `serve_stream` admission refused because the request's voucher
    /// signer has drained its shared on-chain `cap` (`cap − spent` below the serve
    /// floor) or its registration has expired — a capability this node could never
    /// cash.
    serve_stream_rejected_signer_cap_exhausted => serve_stream_rejected_signer_cap_exhausted.inc();

    /// Record a live serve stopped mid-stream because its voucher signer drained
    /// its shared on-chain `cap` at other nodes after admission, so the headroom the
    /// node holds on the lane no longer covers a serve floor.
    serve_stream_midstream_signer_cap_exhausted => serve_stream_midstream_signer_cap_exhausted.inc();

    /// Record a `serve_stream` delivery refused because this capability signer hit its
    /// per-signer live concurrency cap of un-vouchered reservation, while the pool as a
    /// whole can still pay.
    serve_stream_rejected_signer_floor_at_cap => serve_stream_rejected_signer_floor_at_cap.inc();

    /// Record a whole-blob request from an active staker refused while this
    /// node's own upstream open for the blob was in progress (#2224).
    serve_stream_rejected_pull_loop_guard => serve_stream_rejected_pull_loop_guard.inc();

    /// Record a `serve_stream` delivery refused because the requested bounded
    /// range is out of bounds for the blob (ADR 005 §Bounded byte ranges).
    serve_stream_rejected_range_not_satisfiable
        => serve_stream_rejected_range_not_satisfiable.inc();

    /// Record a `serve_stream` pull-through refused because the requested range
    /// starts at or past the node's `max_blob_size` ceiling (ADR 005
    /// §`max_blob_size` enforcement).
    serve_stream_rejected_blob_too_large => serve_stream_rejected_blob_too_large.inc();

    /// Record a `serve_stream` delivery refused because the blob is on the
    /// operator's local denylist (ADR 011 §Local Denylist).
    serve_stream_rejected_hash_denied => serve_stream_rejected_hash_denied.inc();

    /// Record a `serve_stream` delivery refused because the blob is on the
    /// governance blacklist (ADR 011 §On Blacklist Event).
    serve_stream_rejected_chain_hash_denied => serve_stream_rejected_chain_hash_denied.inc();

    /// Record a `serve_stream` delivery refused because the channel's funding
    /// address is a blacklisted origin (ADR 011 §On Blacklist Event).
    serve_stream_rejected_origin_denied => serve_stream_rejected_origin_denied.inc();

    /// Record a `serve_stream` delivery declined by the origin-only policy
    /// (#1759, #1766): a live probe of this node's own backend genuinely does
    /// not hold the hash.
    serve_stream_rejected_foreign_declined => serve_stream_rejected_foreign_declined.inc();

    /// Record a `serve_stream` request refused because the node's chain reads
    /// are stale past `blockchain.chain_staleness_grace_sec` (ADR 011 § Serving
    /// while chain-stale).
    serve_stream_rejected_chain_stale => serve_stream_rejected_chain_stale.inc();

    /// Record an in-flight delivery cut off at an MB boundary because a takedown
    /// landed after the stream opened (ADR 011 §On Blacklist Event).
    serve_stream_terminated_takedown => serve_stream_terminated_takedown.inc();

    /// Record a `serve_stream` cache-hit request refused by the load-shed policy
    /// (egress saturation).
    serve_stream_rejected_load_shed_hit => serve_stream_rejected_load_shed_hit.inc();

    /// Record a `serve_stream` cache-miss request refused by the load-shed policy
    /// (concurrency pressure or per-client fairness).
    serve_stream_rejected_load_shed_miss => serve_stream_rejected_load_shed_miss.inc();

    /// Record a serve leg refusing to cut or frame a `ChunkData` because its own
    /// byte accounting did not add up. Node-side bug, never peer behaviour.
    serve_frame_accounting_fault => serve_frame_accounting_fault.inc();

    /// Record a `cdn/client/v1` delivery that ended on a node-side fault rather than
    /// on a peer hang-up or a client payment fault. Node-side bug, never peer
    /// behaviour.
    serve_stream_node_fault => serve_stream_node_fault.inc();

    /// Record a `cdn/client/v1` delivery that ended because the payer spent its
    /// per-chunk proof budget without settling the chunk. The payer's fault, not a
    /// node fault.
    serve_stream_proof_budget_exhausted => serve_stream_proof_budget_exhausted.inc();

    /// Record a `cdn/client/v1` stream reset because the connection's stream cap
    /// was full.
    serve_stream_rejected_stream_cap_full => serve_stream_rejected_stream_cap_full.inc();

    /// Record a `cdn/client/v1` stream reset because its first request could not
    /// be read. Peer-side.
    serve_stream_request_unreadable => serve_stream_request_unreadable.inc();

    /// Record a paying `cdn/client/v1` stream stopped on a rejected voucher.
    serve_stream_voucher_rejected => serve_stream_voucher_rejected.inc();

    /// Record a `cdn/client/v1` stream the requester left before any voucher
    /// credited a byte. Peer-side.
    serve_stream_client_declined => serve_stream_client_declined.inc();

    /// Record a paying `cdn/client/v1` stream the requester left after a voucher
    /// credited bytes. Peer-side.
    serve_stream_client_abandoned => serve_stream_client_abandoned.inc();

    /// Record the current measured egress EWMA, bytes/sec.
    load_shed_egress_bps(bps: i64) => load_shed_egress_bps.set(bps);

    /// Record whether the load-shed policy considers the node pressured (1 for yes, 0 for no).
    load_shed_pressure_active(active: bool) => load_shed_pressure_active.set(i64::from(active));

    /// Publish node-wide serves in flight, sampled from the shed controller
    /// beside the egress EWMA. `u32 -> i64` is lossless.
    load_shed_streams_in_flight(n: u32) => load_shed_streams_in_flight.set(i64::from(n));

    /// Record an above-floor warming buy downgraded to the amortized floor
    /// because the source's per-source allowance was spent (ADR 041).
    warming_speculative_blocked => warming_speculative_blocked.inc();

    /// Publish how many upstream sources currently hold no warming allowance
    /// (ADR 041), sampled beside the egress EWMA.
    warming_sources_blocked(n: usize) => warming_sources_blocked.set(sat(n));

    /// The window-paced serve loop paused the upstream pull at the ramped credit
    /// window to wait for the downstream voucher to clear (#856, #1669).
    node_pull_through_window_paused => node_pull_through_window_paused.inc();

    /// The window-paced serve loop paused the upstream pull until the ramped
    /// window opened to the minimum draw (#2061).
    node_pull_through_min_draw_waits => node_pull_through_min_draw_waits.inc();

    /// A window-paced pause of the upstream pull ended after `elapsed`.
    node_pull_through_wait(elapsed: Duration) =>
        node_pull_through_wait_seconds.observe(elapsed.as_secs_f64());

    /// A window-paced serve was abandoned because the requesting client dropped
    /// or underpaid mid-pull (#856).
    node_pull_through_client_abandoned => node_pull_through_client_abandoned.inc();

    /// A serve-miss pull ingested upstream bytes that failed bao verification
    /// against the content root (#856/#915).
    node_pull_through_upstream_verify_failed => node_pull_through_upstream_verify_failed.inc();

    /// The node entered `serve_via_backend_origin` — the stream-while-store
    /// serve tier fired for this request (#1130). Counted at entry, before any
    /// admission guard; distinguishes this tier from the buffered
    /// `populate_local` fallback regardless of this request's eventual outcome.
    local_outboard_serve => local_outboard_serves.inc();

    /// The blob-availability gate classified a paid serve as servable from a
    /// complete local blob. Bumped before the shed gate and before any fill
    /// tier can run.
    serve_cache_hit => serve_cache_hit.inc();
    /// The gate classified a paid serve as servable from a partial blob whose
    /// held chunk groups cover the requested span (#1506).
    serve_cache_partial_hit => serve_cache_partial_hit.inc();
    /// The gate could not satisfy a paid serve locally, so a fill tier runs.
    serve_cache_miss => serve_cache_miss.inc();

    /// A buyer→upstream pull hit this node's own `pull_timeout` deadline (#857).
    /// A buyer-side condition, so it does not score the provider's reputation.
    node_pull_timeout => node_pull_timeout.inc();

    /// An upstream rejected a voucher this node presented mid-pull (#857) — a
    /// buyer payment-side fault, so it does not score the provider's reputation.
    node_pull_voucher_rejected => node_pull_voucher_rejected.inc();

    /// A lane can no longer pay but the pool's deposit is still escrowed, so the row was
    /// KEPT for the reclaim sweep and the provider suppressed instead (#1145 review).
    /// Money at rest — see the counter's docs.
    node_pull_pool_wedged => node_pull_pool_wedged.inc();

    /// A selected upstream refused delivery up front (#1144). Counts every wire
    /// class; none scores the provider's reputation.
    node_pull_refused => node_pull_refused.inc();

    /// A refusal this node cannot attribute to the upstream (#1520) — `NotFound`
    /// or `Unfunded`. A rate approaching `node_pull_refused` means nobody will
    /// serve us, which is usually our own deposit or binding, not their fault.
    node_pull_refused_unattributable => node_pull_refused_unattributable.inc();

    /// An upstream shed a probe or pull at the transport with `APP_ERR_RATE_LIMITED`
    /// (#1986). Suppressed briefly, never scored — see the counter's docs.
    node_upstream_rate_limited => node_upstream_rate_limited.inc();

    /// A ranged run completed one wait on an upstream's backpressure refusal
    /// instead of dropping the sole covering source (#2178) — see the counter's
    /// docs.
    node_pull_backpressure_backoffs => node_pull_backpressure_backoffs.inc();

    /// The sole covering source outlasted the backpressure wait budget and the
    /// assembly ended short (#2178) — see the counter's docs.
    node_pull_backpressure_exhausted => node_pull_backpressure_exhausted.inc();

    /// A clean upstream leg moved neither frontier and the run dropped its source
    /// (#2194) — see the counter's docs.
    node_pull_leg_no_progress => node_pull_leg_no_progress.inc();

    /// An upstream went silent mid-stream (#1134); the pull was abandoned and the
    /// provider scored `Unreachable`.
    node_pull_stalled => node_pull_stalled.inc();

    /// A pull failed for a LOCAL reason (#1145 review) — signer, encode, range, or
    /// this node's own cache store — so the upstream was exonerated. Says nothing
    /// about the network; any sustained rate means this node cannot complete a pull.
    node_pull_local_fault => node_pull_local_fault.inc();

    /// A buyer pool open outlived the per-candidate budget (#1143). The open
    /// continues in the background; the pull moves on. No reputation effect.
    node_pull_pool_open_pending => node_pull_pool_open_pending.inc();

    /// A pull paid ≥1 voucher but persisting the buyer lane resume watermark
    /// failed (#852); the lane's stored progress now lags the upstream.
    node_pull_progress_persist_failure => node_pull_progress_persist_failures.inc();

    /// A paid voucher's watermark was DROPPED because the node's pool had been
    /// replaced by a newer open before the write landed (#1145 review).
    node_pull_progress_dropped => node_pull_progress_dropped.inc();

    /// A concurrent settle on the shared lane ledger persisted a higher watermark first, so
    /// this write was superseded (benign under `BuyerLedgers`; #1145 review).
    node_pull_progress_superseded => node_pull_progress_superseded.inc();

    /// A miss fill's funding recovery step funded the next pass (#1530).
    node_pull_recovery_step => node_pull_recovery_step.inc();

    /// A miss fill's funding recovery step had no way to fund the next pass,
    /// or its funding tx failed (#1530).
    node_pull_recovery_step_refused => node_pull_recovery_step_refused.inc();

    /// The delivery handler abandoned a pull-through at its deadline (#831).
    node_pull_through_timeout => node_pull_through_timeouts.inc();

    /// A cache-engine error (not a clean miss) was hit filling a miss (#831).
    node_pull_through_error => node_pull_through_errors.inc();

    /// A `getOrigins` lookup failed on a cold-namespace cache miss in the lazy
    /// origin directory. The lookup fails closed (resolves no origins for
    /// that request) and the failure is not cached, so this is the drift
    /// signal to alert on.
    origin_directory_get_origins_failure => origin_directory_get_origins_failures.inc();

    /// Publish the current namespace count held in the lazy origin directory
    /// cache (positive + negative entries), after an insert.
    origin_directory_cache_size(count: usize)
        => origin_directory_cache_size.set(sat(count));
    /// A connection was accepted and handed to a handler.
    connection_opened => active_connections.inc();
    /// A connection closed. Pairs with `connection_opened`.
    connection_closed => active_connections.dec();

    /// A `cdn/client/v1` connection was reaped by the application-layer idle
    /// closer (ADR 005 §Connection lifetime).
    client_idle_close => client_idle_close.inc();

    /// An OTLP span-export batch failed (`commands::otlp`'s counting exporter).
    otlp_export_failure => otlp_export_failures.inc();

    /// Set the RPC health gauge. `true` -> 1 (reachable), `false` -> 0
    /// (unreachable). Driven by the watchdog task spawned in
    /// `runtime::run`.
    rpc_healthy(ok: bool) => rpc_healthy.set(i64::from(ok));

    /// Record a connection rejected by the global concurrency semaphore.
    dispatch_rejected_global => dispatch_rejected_global.inc();

    /// Record a connection rejected by the per-source rate limiter.
    dispatch_rejected_per_source => dispatch_rejected_per_source.inc();

    /// Increment the in-flight dispatch permit gauge.
    dispatch_permit_acquired => dispatch_in_flight.inc();

    /// Decrement the in-flight dispatch permit gauge.
    dispatch_permit_released => dispatch_in_flight.dec();

    /// Record a relay-only connection accepted while the per-source
    /// layer was enabled but no peer IP could be resolved at accept
    /// time.
    dispatch_per_source_skipped_no_addr => dispatch_per_source_skipped_no_addr.inc();

    /// Record an inbound connection that arrived with a direct IP path.
    inbound_connection_direct => inbound_connections_direct.inc();

    /// Record an inbound connection that arrived over the relay only.
    inbound_connection_relayed => inbound_connections_relayed.inc();

    /// Set whether the host has a public address on its default route.
    node_public_address(public: bool) => node_public_address.set(i64::from(public));

    /// Record a `cdn/dht/v1` request rejected at the per-peer layer.
    dht_rate_limit_rejected_per_peer => dht_rate_limit_rejected_per_peer.inc();

    /// Record a `cdn/dht/v1` request rejected at the per-IP layer.
    dht_rate_limit_rejected_per_ip => dht_rate_limit_rejected_per_ip.inc();

    /// Record a `cdn/dht/v1` request rejected at the global layer.
    dht_rate_limit_rejected_global => dht_rate_limit_rejected_global.inc();

    /// Record a `retain_recent` sweep of the per-IP DHT keyed-limiter
    /// map (#645).
    dht_rate_limit_prune_sweep_per_ip => dht_rate_limit_prune_sweeps_per_ip.inc();

    /// Record a `retain_recent` sweep of the per-peer DHT keyed-limiter
    /// map (#645).
    dht_rate_limit_prune_sweep_per_peer => dht_rate_limit_prune_sweeps_per_peer.inc();

    /// Set the per-IP DHT keyed-limiter tracked-size gauge (#645). The
    /// `try_from(...).unwrap_or(i64::MAX)` clamp matches the existing
    /// gauge-set pattern elsewhere in this module and stays within the
    /// workspace's anti-panic policy.
    dht_rate_limit_tracked_per_ip_set(n: usize) => dht_rate_limit_tracked_per_ip.set(sat(n));

    /// Set the per-peer DHT keyed-limiter tracked-size gauge (#645).
    dht_rate_limit_tracked_per_peer_set(n: usize) => dht_rate_limit_tracked_per_peer.set(sat(n));

    /// Record a `cdn/probe/v1` request rejected at the per-peer layer.
    probe_rate_limit_rejected_per_peer => probe_rate_limit_rejected_per_peer.inc();

    /// Record a `cdn/probe/v1` request rejected at the per-IP layer.
    probe_rate_limit_rejected_per_ip => probe_rate_limit_rejected_per_ip.inc();

    /// Record a `cdn/probe/v1` request rejected at the global layer.
    probe_rate_limit_rejected_global => probe_rate_limit_rejected_global.inc();

    /// Record a `retain_recent` sweep of the per-IP probe keyed-limiter
    /// map (#645).
    probe_rate_limit_prune_sweep_per_ip => probe_rate_limit_prune_sweeps_per_ip.inc();

    /// Record a `retain_recent` sweep of the per-peer probe keyed-limiter
    /// map (#645).
    probe_rate_limit_prune_sweep_per_peer => probe_rate_limit_prune_sweeps_per_peer.inc();

    /// Set the per-IP probe keyed-limiter tracked-size gauge (#645).
    probe_rate_limit_tracked_per_ip_set(n: usize) => probe_rate_limit_tracked_per_ip.set(sat(n));

    /// Set the per-peer probe keyed-limiter tracked-size gauge (#645).
    probe_rate_limit_tracked_per_peer_set(n: usize)
        => probe_rate_limit_tracked_per_peer.set(sat(n));

    /// Record a `cdn/dht/v1` request that was admitted by the rate
    /// limiter but failed after that (frame decode, write, encode,
    /// timeout, etc).
    dht_request_failed => dht_requests_failed.inc();

    /// Record a `Store` rejected by the `holder != authenticated NodeId`
    /// check (ADR 022 §STORE Flow line 140 — lying-holder attack).
    dht_store_rejected_holder_mismatch => dht_store_rejected_holder_mismatch.inc();

    /// Record a `Store` rejected by the active-staker filter (ADR 022
    /// §STORE Flow line 140 — non-staked publisher).
    dht_store_rejected_non_staked => dht_store_rejected_non_staked.inc();

    /// Record a `Store` rejected by the per-publisher quota (ADR 022
    /// §Content Records and TTL — 200-record hard cap).
    dht_store_rejected_quota => dht_store_rejected_quota.inc();

    /// Record a `Store` admitted to the record store (newly inserted or
    /// refreshed).
    dht_store_accepted => dht_store_accepted.inc();

    /// Record a `BatchStore` that reached per-hash admission (passed
    /// stage-1 rate limiting + the batch-level holder check, #648).
    dht_batch_store_received => dht_batch_store_received.inc();

    /// Record `count` `BatchStore` hashes deferred (acked `false`)
    /// because the two-stage rate-limit budget ran out before reaching
    /// them (ADR 022 §Batch token accounting, #648).
    dht_batch_store_hashes_deferred_rate_limit(count: u64)
        => dht_batch_store_hashes_deferred_rate_limit.inc_by(count);

    /// Record a republish lag sweep starting (the cache-commit broadcast
    /// channel reported `Lagged`).
    dht_republish_lag_sweep => dht_republish_lag_sweeps.inc();
    /// Record a lag folded into an already-running or already-queued sweep
    /// rather than starting a walk of its own.
    dht_republish_lag_sweep_coalesced => dht_republish_lag_sweeps_coalesced.inc();
    /// Record a lag-sweep walk that died in a panic instead of completing.
    dht_republish_lag_sweep_panicked => dht_republish_lag_sweep_panics.inc();
    /// Record `count` hashes a lag sweep newly scheduled for republish.
    dht_republish_sweep_reseeded(count: u64) => dht_republish_sweep_reseeded.inc_by(count);
    /// Record a bulk republish seed (boot cold start or lag sweep) that could
    /// not walk the store and covered only the origin-held half.
    dht_republish_seed_store_walk_failure => dht_republish_seed_store_walk_failures.inc();
    /// Record `count` stored hashes a bulk republish seed could not put to its
    /// origin, leaving them out of the announce set.
    dht_republish_seed_origin_probe_failures(count: u64) =>
        dht_republish_seed_origin_probe_failures.inc_by(count);

    /// Stamp the slash watcher's `*_last_tick_timestamp_seconds` liveness gauge
    /// with the current wall-clock time (#1316). The `on_tick_success` hook,
    /// fired on EVERY successful poll tick so a dead task's gauge goes stale.
    slash_watcher_tick => slash_watcher_last_tick_timestamp_seconds.set(unix_now_secs());
    /// Record the chain-event poller's current `eth_getLogs` window span.
    chain_get_logs_span(span: u64) => chain_get_logs_span.set(sat_u64(span));
    /// Count one `eth_getLogs` window the provider rejected for its range.
    chain_get_logs_range_rejected => chain_get_logs_range_rejections.inc();
    /// Count one in-tick retry of a transiently failed `eth_getLogs` window.
    chain_get_logs_retried => chain_get_logs_retries.inc();
    /// Count one `eth_getLogs` window deferred to the next tick.
    chain_get_logs_deferred => chain_get_logs_deferred.inc();
    /// Count one boot-time chain read retried after a transient error.
    chain_boot_read_retried => chain_boot_read_retries.inc();
    /// Stamp the staker-set watcher's liveness gauge (#1316). See `slash_watcher_tick`.
    staker_set_watcher_tick => staker_set_watcher_last_tick_timestamp_seconds.set(unix_now_secs());
    /// A capacity-bond registry re-enumeration failed and the previous
    /// projections were kept. Bumps
    /// `capacity_bond_registry_resync_failures_total`. Pairs with the `warn!` in
    /// [`crate::dht::capacity_bond_registry`]'s `RegistrySink::on_tick_complete`.
    capacity_bond_registry_resync_failure => capacity_bond_registry_resync_failures.inc();
    /// Stamp the capacity-bond registry's re-enumeration liveness gauge on a
    /// successful resync. Reads `0` until the first one lands, so any alert on
    /// it needs a `> 0` guard.
    capacity_bond_registry_resync =>
        capacity_bond_registry_last_resync_timestamp_seconds.set(unix_now_secs());
    /// Stamp the blacklist watcher's liveness gauge (#1316, #1320).
    blacklist_watcher_tick => blacklist_watcher_last_tick_timestamp_seconds.set(unix_now_secs());
    /// Stamp the payment-settlement watcher's liveness gauge (#1316).
    settlement_watcher_tick => settlement_watcher_last_tick_timestamp_seconds.set(unix_now_secs());
    /// Stamp the fee-shares watcher's liveness gauge. See `slash_watcher_tick`;
    /// this one matters because the fee-shares watcher's failure paths
    /// deliberately return `Ok`, so this gauge is the only signal that it is
    /// still polling.
    fee_shares_watcher_tick => fee_shares_watcher_last_tick_timestamp_seconds.set(unix_now_secs());

    /// Record that the slash watcher task unwound on a panic (#1316). Bumped from
    /// the `Drop` guard in `multiplexed_poller::run` via the `on_task_panic` hook.
    slash_watcher_task_panicked => slash_watcher_task_panicked.inc();
    /// Record that the staker-set watcher task unwound on a panic (#1316).
    staker_set_watcher_task_panicked => staker_set_watcher_task_panicked.inc();
    /// Record that the blacklist watcher task unwound on a panic (#1316, #1283).
    blacklist_watcher_task_panicked => blacklist_watcher_task_panicked.inc();
    /// Record that the fee-shares watcher task unwound on a panic.
    fee_shares_watcher_task_panicked => fee_shares_watcher_task_panicked.inc();
    /// Record that the payment-settlement watcher task unwound on a panic (#1316).
    settlement_watcher_task_panicked => settlement_watcher_task_panicked.inc();

    /// Record `count` hashes a blacklist re-scope could not enforce this pass
    /// (#1319 — `Recheck::Failed`). One aggregated bump per pass, not per hash.
    blacklist_enforcement_failure(count: u64) => blacklist_enforcement_failures.inc_by(count);
    /// A blacklist periodic re-enumeration failed and the current deny-set was
    /// kept. Bumps `blacklist_reenumeration_failures_total`.
    blacklist_reenumeration_failure => blacklist_reenumeration_failures.inc();

    /// Count `bytes` of payload written to a client on `cdn/client/v1`.
    bytes_served(bytes: u64) => bytes_served.inc_by(bytes);
    /// Count `bytes` of payload admitted from an upstream node in a paid pull.
    bytes_received(bytes: u64) => bytes_received.inc_by(bytes);

    /// A landed `redeemMany` redeemed `vouchers` vouchers on-chain.
    pool_redemptions(vouchers: u64) => pool_redemptions.inc_by(vouchers);
    /// An inbound stream accepted a voucher as the payee.
    voucher_received => vouchers_received.inc();
    /// An inbound stream accepted a `PayWord` preimage reveal that advanced its
    /// lane's chain frontier.
    preimage_reveal_received => preimage_reveals_received.inc();
    /// A pool entered the owner-close grace window while this node held
    /// unredeemed vouchers on it.
    pool_grace_close => pool_grace_closes.inc();
    /// A node transaction mined and succeeded.
    onchain_tx_landed => onchain_tx_landed.inc();
    /// A node transaction mined and reverted.
    onchain_tx_reverted => onchain_tx_reverted.inc();
    /// The RPC refused a node transaction at `send`.
    onchain_tx_send_failed => onchain_tx_send_failed.inc();
    /// An issued node transaction's receipt wait failed and no by-hash fetch
    /// found the receipt.
    onchain_tx_receipt_failed => onchain_tx_receipt_failed.inc();
    /// An issued node transaction's receipt did not arrive in time and no
    /// by-hash fetch found it.
    onchain_tx_timeout => onchain_tx_timeout.inc();
    /// A failed or lapsed receipt wait was resolved by fetching the receipt by hash.
    onchain_tx_receipt_recovered => onchain_tx_receipt_recovered.inc();

    /// A `FIND_VALUE` provider lookup started.
    dht_findvalue_query => dht_findvalue_queries.inc();
    /// A lookup round hit its timeout and aborted its in-flight RPCs.
    dht_lookup_round_timeout => dht_lookup_round_timeouts.inc();
    /// `count` peers accepted a `STORE` record from this node.
    dht_store_published(count: u64) => dht_store_published.inc_by(count);
    /// Publish the routing table's entry count.
    dht_routing_table_size(entries: usize) => dht_routing_table_size.set(sat(entries));
    /// A bucket refresh failed.
    dht_bucket_refresh_failure => dht_bucket_refresh_failures.inc();
    /// `count` bootstrap `FIND_NODE` RPCs against seeds failed.
    dht_bootstrap_find_node_failures(count: u64) => dht_bootstrap_find_node_failures.inc_by(count);

    /// The fee-shares watcher's authoritative `getShares()` re-read failed.
    fee_shares_watcher_poll_failure => fee_shares_watcher_poll_failures.inc();
    /// Record whether the fee-shares watcher is unregistered because the startup
    /// `feeRouter()` read failed.
    fee_shares_watcher_unregistered(unregistered: bool) => fee_shares_watcher_unregistered.set(i64::from(unregistered));

    /// A config reload failed and kept the previous values.
    config_reload_failure => config_reload_failures.inc();
    /// The receipt writer failed to persist one audit record.
    receipt_write_failure => receipt_write_failures.inc();
    /// A paying stream stopped mid-delivery on `PoolExhausted`.
    serve_stream_midstream_pool_exhausted => serve_stream_midstream_pool_exhausted.inc();
}

watcher_downtime_recorders! {
    /// The slash-detection watcher's cycle errored and the loop is about to back
    /// off (#1032). Stamps `slash_watcher_down_since` (once per drift window) so
    /// `slash_watcher_down_seconds` climbs until the next healthy cycle. Mirrors
    /// [`Self::staker_set_watcher_backoff_started`]; a poisoned lock skips the
    /// update (the gauge then reads `i64::MAX` at scrape — the safe alerting
    /// direction).
    slash_watcher_backoff_started,
    /// Mark the slash-detection watcher cycle established (#1032): clear
    /// `slash_watcher_down_since` so `slash_watcher_down_seconds` reads `0` for
    /// the life of the cycle. Mirrors [`Self::staker_set_watcher_cycle_established`].
    slash_watcher_cycle_established,
    down_since: slash_watcher_down_since,
    restarts: slash_watcher_restarts,
    down_seconds: slash_watcher_down_seconds;

    /// A staker-set watcher poll tick failed with an error and the loop is
    /// about to back off (#783, [`crate::dht::chain_staker_set`]).
    /// Stamps `down_since` so `staker_set_watcher_down_seconds` begins to climb,
    /// and — on the *transition* from a healthy cycle into the error state
    /// (`down_since` was `None`) — bumps `staker_set_watcher_restarts_total`
    /// exactly once per drift window, rather than once per backoff iteration of
    /// one continuous outage. A poisoned lock is treated as "skip the update"
    /// rather than panicking (anti-panic policy); the gauge's poison fallback
    /// (`i64::MAX`) still keeps the alert tripped. This method is the
    /// `on_backoff` hook wired in [`crate::dht::chain_staker_set`]; it pairs with
    /// the loop-level `warn!` in `multiplexed_poller::run` (`"watcher RPC error;
    /// restarting after backoff"`).
    staker_set_watcher_backoff_started,
    /// Mark the staker-set watcher's poll cycle as established (#783,
    /// downtime semantics #788): a poll tick succeeded and logs are flowing
    /// again (the `on_established` hook). Clears `down_since` to `None` so
    /// `staker_set_watcher_down_seconds`
    /// reads `0` for the entire life of this cycle, however long. A poisoned
    /// lock is treated as "skip the update" rather than panicking (anti-panic
    /// policy); the gauge's poison fallback (`i64::MAX`) then keeps the alert
    /// tripped — the safe (alerting) direction.
    staker_set_watcher_cycle_established,
    down_since: staker_set_watcher_down_since,
    restarts: staker_set_watcher_restarts,
    down_seconds: staker_set_watcher_down_seconds;

    /// The blacklist watcher's poll tick errored and the loop is about to back
    /// off (#1283). Stamps `blacklist_watcher_down_since` once per drift window so
    /// `blacklist_watcher_down_seconds` climbs until the next healthy cycle.
    /// Tracks chain-read outages only; an enforcement failure that leaves a
    /// blacklisted blob servable is the separate `blacklist_enforcement_failures`
    /// counter (#1319). Mirrors [`Self::slash_watcher_backoff_started`].
    blacklist_watcher_backoff_started,
    /// Mark the blacklist watcher cycle established (#1283): clear
    /// `blacklist_watcher_down_since` so `blacklist_watcher_down_seconds` reads
    /// `0` for the life of the cycle. Mirrors [`Self::slash_watcher_cycle_established`].
    blacklist_watcher_cycle_established,
    down_since: blacklist_watcher_down_since,
    restarts: blacklist_watcher_restarts,
    down_seconds: blacklist_watcher_down_seconds;

    /// The payment-settlement watcher's poll tick errored and the loop is about
    /// to back off (#1316). Stamps `settlement_watcher_down_since` once per drift
    /// window. Mirrors [`Self::slash_watcher_backoff_started`].
    settlement_watcher_backoff_started,
    /// Mark the payment-settlement watcher cycle established (#1316): clear
    /// `settlement_watcher_down_since`. Mirrors [`Self::slash_watcher_cycle_established`].
    settlement_watcher_cycle_established,
    down_since: settlement_watcher_down_since,
    restarts: settlement_watcher_restarts,
    down_seconds: settlement_watcher_down_seconds;

    /// The fee-shares watcher's poll tick errored and the loop is about to back
    /// off. Stamps `fee_shares_watcher_down_since` once per drift window.
    /// Mirrors [`Self::slash_watcher_backoff_started`].
    fee_shares_watcher_backoff_started,
    /// Mark the fee-shares watcher cycle established: clear
    /// `fee_shares_watcher_down_since`. Mirrors [`Self::slash_watcher_cycle_established`].
    fee_shares_watcher_cycle_established,
    down_since: fee_shares_watcher_down_since,
    restarts: fee_shares_watcher_restarts,
    down_seconds: fee_shares_watcher_down_seconds;
}

/// Bind the `/metrics` HTTP listener synchronously so startup can fail fast
/// if the port is unavailable. The returned listener is consumed by [`serve`].
///
/// Emits a `WARN` if `addr` is non-loopback (#579). The `OpenMetrics`
/// surface exposes pull-through byte volumes and GC/connection stats —
/// useful reconnaissance for anyone who can reach it. The default config
/// binds loopback (`appendix-local-admin-http` calls metrics
/// "loopback-only"), but `observability.metrics_bind` is operator-
/// settable to `0.0.0.0` for containerised deployments
/// (`crates/common/src/config/types.rs` — `ObservabilityConfig::metrics_bind`).
/// We warn but do not reject so that documented container workflows
/// keep working.
///
/// # Errors
///
/// Returns an error if the underlying `crate::net::bind_reuseaddr` fails —
/// socket creation, `bind` (port in use, permissions), or `listen`.
pub fn bind(addr: SocketAddr) -> anyhow::Result<TcpListener> {
    // `SO_REUSEADDR` so a restart rebinds this fixed port immediately instead of
    // racing a `TIME_WAIT` remnant from the prior process (see `crate::net`).
    let listener = crate::net::bind_reuseaddr(addr)
        .map_err(|e| anyhow::anyhow!("metrics bind {addr} failed: {e}"))?;
    // `to_canonical()` unwraps IPv4-mapped IPv6 (e.g.
    // `::ffff:127.0.0.1`) so an operator binding the dual-stack
    // form of loopback doesn't get a false "non-loopback" warning.
    // `Ipv6Addr::is_loopback()` only matches `::1`.
    let ip = addr.ip().to_canonical();
    if !ip.is_loopback() {
        // `is_unspecified()` (`0.0.0.0` / `::`) is the common
        // containerised case; we call it out by name so an operator
        // grepping startup logs sees the intent. A public IP falls
        // through to the generic non-loopback message.
        if ip.is_unspecified() {
            tracing::warn!(
                %addr,
                "metrics server is binding all interfaces (non-loopback); the OpenMetrics \
                 endpoint exposes pull-through byte volumes and GC/connection stats — gate \
                 it behind a private network or reverse proxy if reachable from outside the \
                 host"
            );
        } else {
            tracing::warn!(
                %addr,
                "metrics server is binding a non-loopback address; the OpenMetrics \
                 endpoint exposes pull-through byte volumes and GC/connection stats — \
                 restrict reachability to trusted scrapers"
            );
        }
    }
    tracing::info!(%addr, "metrics server listening");
    Ok(listener)
}

/// Serve `/metrics` over HTTP on the pre-bound `listener` until `shutdown`
/// fires. The shutdown receiver is consumed; send `()` to stop the accept
/// loop.
///
/// Per-connection tasks are spawned with `tokio::spawn` and **detached** —
/// they are not tracked or awaited during shutdown. In practice scrapes
/// complete in milliseconds, and dropping an in-flight `/metrics` response
/// is harmless (the scraper will retry on its next interval). This is a
/// deliberate choice: tracking an unbounded `JoinSet` alongside the accept
/// loop would add complexity without a consumer that cares about the
/// guarantee.
#[allow(clippy::cognitive_complexity)] // Accept+permit+spawn reads linearly.
pub async fn serve(
    listener: TcpListener,
    metrics: Arc<Metrics>,
    mut shutdown: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let limiter = Arc::new(Semaphore::new(MAX_METRICS_CONNECTIONS));

    loop {
        let (stream, peer) = tokio::select! {
            biased;
            _ = &mut shutdown => {
                tracing::debug!("metrics server shutdown signal received");
                return Ok(());
            }
            res = listener.accept() => match res {
                Ok(pair) => pair,
                Err(err) => {
                    tracing::warn!(error = %err, "metrics accept failed");
                    continue;
                }
            },
        };

        let Ok(permit) = Arc::clone(&limiter).try_acquire_owned() else {
            tracing::warn!(
                %peer,
                limit = MAX_METRICS_CONNECTIONS,
                "metrics connection rejected: at capacity",
            );
            drop(stream);
            continue;
        };

        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let _permit = permit; // released when task finishes
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                let metrics = Arc::clone(&metrics);
                async move { handle(req, metrics) }
            });
            if let Err(err) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await
            {
                tracing::debug!(error = %err, "metrics connection ended");
            }
        });
    }
}

#[allow(clippy::unnecessary_wraps, clippy::needless_pass_by_value)] // hyper service_fn signature requires Result and owned req.
fn handle(
    req: Request<hyper::body::Incoming>,
    metrics: Arc<Metrics>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    if req.uri().path() != "/metrics" {
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from_static(b"not found\n")))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::new()))));
    }

    match metrics.encode() {
        Ok(body) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header(
                "content-type",
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )
            .body(Full::new(Bytes::from(body)))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))),
        Err(err) => {
            tracing::warn!(error = %err, "metrics encode error");
            Ok(Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::new(Bytes::from_static(b"encode error\n")))
                .unwrap_or_else(|_| Response::new(Full::new(Bytes::new()))))
        }
    }
}

/// RAII guard for the `active_connections` gauge.
///
/// Increments the gauge on construction, decrements on drop — so the count
/// stays correct even if the handler future is cancelled (e.g. during
/// shutdown) between open and close.
#[derive(Debug)]
pub struct ConnectionGuard<'a> {
    metrics: &'a Metrics,
}

impl<'a> ConnectionGuard<'a> {
    fn new(metrics: &'a Metrics) -> Self {
        metrics.connection_opened();
        Self { metrics }
    }
}

impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        self.metrics.connection_closed();
    }
}

/// One-shot time-to-first-byte clock for one inbound paid serve.
///
/// The clock starts when the request is decoded and records into the
/// `decdn_serve_first_byte_{hit,miss}_seconds` sibling that matches the
/// [`RequestClass`] the availability gate admitted the serve under. A partial
/// hit is a [`RequestClass::CacheHit`]. [`Self::record`] consumes the clock, so a stream
/// records at most once. A stream that ends before its first frame drops the
/// clock and records nothing.
///
/// The clock also splits the total into three phases: admission (decoded to
/// load-shed admit), response (admit to the signed `StreamResponse` written)
/// and first frame (response written to the first frame written). Each phase
/// records into its own histogram, and a `debug` event carries all three.
#[derive(Debug)]
pub(crate) struct FirstByteClock {
    started: Instant,
    admitted: Instant,
    responded: Option<Instant>,
    class: RequestClass,
}

impl FirstByteClock {
    /// A clock that started at `started` and passed the load-shed gate at
    /// `admitted`, for a serve of class `class`.
    pub(crate) const fn new(started: Instant, admitted: Instant, class: RequestClass) -> Self {
        Self {
            started,
            admitted,
            responded: None,
            class,
        }
    }

    /// Mark the signed `StreamResponse` written.
    pub(crate) fn mark_responded(&mut self) {
        self.responded = Some(Instant::now());
    }

    /// Record the time since the clock started and each phase: the first frame
    /// is written. Without a response mark, only the admission phase and the
    /// total record.
    pub(crate) fn record(self, metrics: &Metrics) {
        let now = Instant::now();
        let total = now.saturating_duration_since(self.started);
        let admission = self.admitted.saturating_duration_since(self.started);
        let m = &metrics.decdn;
        m.serve_admission_seconds.observe(admission.as_secs_f64());
        let phases = self.responded.map(|responded| {
            (
                responded.saturating_duration_since(self.admitted),
                now.saturating_duration_since(responded),
            )
        });
        let (first_byte, response, first_frame) = match self.class {
            RequestClass::CacheHit => (
                &m.serve_first_byte_hit_seconds,
                &m.serve_response_hit_seconds,
                &m.serve_first_frame_hit_seconds,
            ),
            RequestClass::CacheMiss => (
                &m.serve_first_byte_miss_seconds,
                &m.serve_response_miss_seconds,
                &m.serve_first_frame_miss_seconds,
            ),
        };
        first_byte.observe(total.as_secs_f64());
        if let Some((resp, frame)) = phases {
            response.observe(resp.as_secs_f64());
            first_frame.observe(frame.as_secs_f64());
        }
        tracing::debug!(
            class = ?self.class,
            total_ms = total.as_millis(),
            admission_ms = admission.as_millis(),
            response_ms = phases.map(|(r, _)| r.as_millis()),
            first_frame_ms = phases.map(|(_, f)| f.as_millis()),
            "serve first byte"
        );
    }
}

/// Drop-safe accounting for a paid delivery stream in one direction.
#[derive(Debug)]
pub(crate) struct StreamGuard {
    gauge: Arc<Gauge>,
}

impl StreamGuard {
    fn new(gauge: Arc<Gauge>) -> Self {
        gauge.inc();
        Self { gauge }
    }
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
