//! OpenMetrics/Prometheus metrics and a minimal `/metrics` HTTP server.
//!
//! Metrics live in an [`iroh_metrics::Registry`] so we can surface both our
//! `decdn_*` counters and iroh's own transport metrics through a single
//! endpoint. Output is `OpenMetrics` text, which Prometheus scrapers accept.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use alloy::primitives::U256;
use bytes::Bytes;
use decdn_cache::CacheMetrics;
use decdn_incentive::PoolOpenFailureReason;
use http_body_util::Full;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use iroh::Endpoint;
use iroh_metrics::{
    Counter, EncodeLabelSet, EncodeLabelValue, Family, Gauge, MetricsGroup, MetricsSource, Registry,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, oneshot};

use crate::chain_events::resumable_watcher::WatcherHook;

/// Cap concurrent `/metrics` connections. Prevents a trivial `DoS` where a
/// peer opens many sockets to the operational-data endpoint and exhausts
/// tasks.
const MAX_METRICS_CONNECTIONS: usize = 32;

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
/// remedy, which is why the alert in `monitoring/prometheus-alerts.yml` filters
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
#[derive(Debug, Default, Serialize, Deserialize, MetricsGroup)]
#[metrics(name = "decdn")]
pub struct DecdnMetrics {
    /// Total probe requests served.
    pub probe_requests: Counter,
    /// Currently open QUIC connections.
    pub active_connections: Gauge,
    /// Currently open paid delivery streams, split by node direction.
    streams_active: Family<StreamLabels, Gauge>,
    /// Currently open inbound serve lanes (distinct `(pool, signer, provider)` keys).
    pub lanes_open: Gauge,
    /// Total raw USDC deposits across the pools currently paying this node.
    pub pool_deposit_usdc: Gauge,
    /// `cdn/client/v1` connections closed by the application-layer idle reaper
    /// (ADR 005 §Connection lifetime): no stream for `APP_IDLE_TIMEOUT` after the
    /// last one closed. A sustained rate flags peers parking streamless
    /// keep-alive'd connections — the abuse pattern the reaper exists to reclaim,
    /// invisible in `active_connections` alone. Operator-visible name:
    /// `decdn_client_idle_close_total`.
    pub client_idle_close: Counter,
    /// Seconds since node start.
    pub node_uptime_seconds: Gauge,
    /// JSON-RPC endpoint reachability per the watchdog task. `1` =
    /// reachable, `0` = unreachable. ADR 020 names operational gauges in
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
    /// one labeled *reason split*, which is not the same as the crate's only
    /// `Family` — `streams_active` is another. Every other reason-style split
    /// in this crate — `dispatch_rejected_*`, `probe_rate_limit_rejected_*`,
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
    /// §Probe-triggered eviction hold. Sampled from the cache engine on
    /// each probe; pair with `probe_hold_slots_max` for a saturation ratio.
    pub probe_hold_slots_used: Gauge,
    /// `decdn_probe_hold_slots_max` (registry, mandatory): the configured
    /// `max_probe_holds` budget. Set once at startup. Pairs with
    /// `probe_hold_slots_used` so dashboards can alert on a saturation
    /// ratio rather than an absolute count.
    pub probe_hold_slots_max: Gauge,
    /// Times the node clamped `rate_per_mb` to the configured delivery
    /// bounds before signing a `ProbeResponse` (ADR 005 §Rate bounds
    /// validation). Operator-visible name:
    /// `decdn_rate_bounds_clamp_events_total`.
    pub rate_bounds_clamp_events: Counter,
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
    /// Accepted vouchers whose nonce skipped one or more values past the
    /// previously-accepted nonce (`voucher.nonce > last_nonce + 1`), counted
    /// once per gapped voucher (#747). The voucher is still accepted —
    /// vouchers are cumulative, so on-chain settlement is unaffected — but a
    /// non-zero rate flags dropped vouchers (per-voucher deliveries the node
    /// never billed) or a client resetting/forking its counter (a replay-probe
    /// signal). The precise skipped-count rides the paired `tracing::warn!` in
    /// `ChannelState::apply_voucher`. Operator-visible name:
    /// `decdn_voucher_nonce_gaps_total`.
    pub voucher_nonce_gaps: Counter,
    /// Redeem hints the voucher-accept path could not enqueue because the
    /// bounded advisory channel was full (`try_send` → `Full`), counted once
    /// per dropped hint (#751). A hint is advisory — the next voucher re-hints
    /// and the redeemer self-tick / shutdown close still redeem — so a few drops
    /// are benign, but a sustained non-zero rate means the redeemer is not
    /// keeping up with channel fan-out and threshold redemption is leaning on
    /// the slow self-tick. Operator-visible name:
    /// `decdn_redeem_hints_dropped_total`.
    pub redeem_hints_dropped: Counter,
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
    /// is conservative — it leaves the source's ledger more negative than
    /// reality, never more positive — so it can only block speculative warming,
    /// never over-fund it. But a sustained non-zero rate means warming is being
    /// throttled by lost bookkeeping rather than by real losses, and a rate that
    /// tracks the serve rate exactly means the aggregator task is gone and every
    /// source will drift to blocked. Operator-visible name:
    /// `decdn_warming_credits_dropped_total`.
    pub warming_credits_dropped: Counter,
    /// Redemption attempts (`try_redeem`) that returned an error — a failed
    /// `getChannel`/`withdraw` RPC or receipt wait (#751). Each is otherwise
    /// only a single `warn!`; a sustained rate means accrued earnings are not
    /// being withdrawn and warrants investigating the RPC / wallet. Operator-
    /// visible name: `decdn_redemption_failures_total`.
    pub redemption_failures: Counter,
    /// Lanes the redeemer held or dropped for the pool's chain-observed
    /// solvency (`pool_is_redeemable`, ADR 003): a drained `Open` pool is held
    /// for a top-up, a drained or past-deadline `Closing` pool is dropped.
    /// Counted once per skipped lane per planning pass. A sustained non-zero
    /// rate means real unredeemed value is stuck behind pools this node can no
    /// longer collect from — worth checking against `pool_deposit_usdc` for
    /// which pools are dry. Operator-visible name:
    /// `decdn_redemption_skipped_insolvent_total`.
    pub redemption_skipped_insolvent: Counter,
    /// Buyer-side reclaim-sweep attempts (`try_reclaim`) that failed — a failed
    /// `getChannel`/`reclaimExpired` RPC, a receipt wait, an on-chain revert, or
    /// a failed store write when clearing the local record after a reclaim/drop
    /// (#906). Each is otherwise only a single `warn!` per hourly sweep; a
    /// sustained rate means an expired channel's refundable deposit is not being
    /// recovered (check the gas wallet / RPC). Pairs with the `error!`
    /// escalation once the same channel fails `RECLAIM_ESCALATION_THRESHOLD`
    /// consecutive sweeps. Operator-visible name:
    /// `decdn_buyer_reclaim_failures_total`.
    pub buyer_reclaim_failures: Counter,
    /// Buyer-pool rows omitted from a successful store hydration because
    /// their persisted values could not be decoded. Counted once per skipped
    /// row per load attempt, so a persistent malformed row keeps the alert
    /// active while healthy rows continue through buyer maintenance. Operator-
    /// visible name:
    /// `decdn_buyer_pool_store_skipped_undecodable_records_total`.
    pub buyer_pool_store_skipped_undecodable_records: Counter,
    /// Background low-water top-ups (#1146) that landed: a reused buyer channel
    /// whose remaining deposit had fallen below its 20% low-water mark was
    /// re-funded to the working deposit, so sustained miss pulls to that provider
    /// keep flowing instead of silently stranding on a spent-down channel. The
    /// healthy signal of the auto-refill path. Operator-visible name:
    /// `decdn_buyer_topup_ok_total`.
    pub buyer_topup_ok: Counter,
    /// Background low-water top-ups (#1146) that did NOT cleanly land: the standing
    /// allowance re-approval failed, the `topUp` submission/receipt errored or
    /// reverted, OR the `topUp` landed on-chain but the local channel row vanished
    /// or rotated during the RPC (escrowed-but-untracked — `top_up` logs the tx at
    /// error!/warn! for reconcile). Folding the untracked case in here — rather than
    /// counting it as `buyer_topup_ok` — means an operator alerting on this metric
    /// sees stranded deposits. The refill is best-effort (the channel is simply left
    /// un-topped), but a sustained rate means reused channels
    /// are not being refilled and pull-through to busy providers will degrade as
    /// their deposits drain (check the gas wallet / RPC / USDC balance). Operator-
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
    /// Floor dead-charge writes that failed: a drop-time persist (`record_loss`
    /// on a `FloorReservation` drop) or a reclaimed pool's `forget_loss`. Both
    /// are best-effort — the in-memory accumulator stays authoritative for the
    /// running process — so a failed persist widens the restart-time re-grant
    /// window instead of breaking delivery (a restart hydrates a stale total and
    /// hands the affected pools back free-floor budget they already consumed),
    /// and a failed forget leaves the closed pool's row with no tombstone, open
    /// to permanent re-insertion by a late persist (#1781's error-path
    /// residual). A sustained rate usually means the shared
    /// `lanes.redb` file refuses writes (a prior failed commit latches redb
    /// until the file is closed and reopened — restart the node) or is corrupt;
    /// pairs with the per-failure `warn!`/`error!` lines ("floor dead-charge
    /// persist failed" / "pool dead-charge forget failed"). Operator-visible
    /// name: `decdn_floor_loss_persist_failures_total`.
    pub floor_loss_persist_failures: Counter,
    /// Slashes detected against this node's operator by the slash watcher
    /// (`SlashJudge.Slashed`), counting each distinct `slashId` once across the
    /// bring-up backfill and the live stream (#1032). A non-zero value means the
    /// operator was slashed and should consider `decdn appeal slash` within the
    /// 30-day window. Operator-visible name: `decdn_slashes_detected_total`.
    pub slashes_detected: Counter,
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
    /// an active staker, or otherwise unselectable) — all three cost the same
    /// network work, which is what this measures.
    pub probe_cache_misses: Counter,
    /// `decdn_probe_post_eviction_failures_total` (ADR 001 §Probe cache,
    /// ADR 005 §`EvictedSinceProbe` semantics; #1165): an upstream answered
    /// `StreamError::EvictedSinceProbe` — it held the blob when it signed
    /// `has_blob: true` and lost it to cache pressure before we opened the
    /// stream.
    ///
    /// ADR 001 mandates tracking this rate; ADR 005 says why it matters more than
    /// "a candidate failed": a node using probe-triggered eviction holds
    /// correctly should *rarely* emit this, because a held blob is invisible to
    /// the LRU driver. A sustained rate above ~1% therefore indicates a remote
    /// hold-mechanism FAILURE — an implementation bug or resource exhaustion —
    /// not a budget-configuration issue, which would surface as `has_blob: false`
    /// at probe time and never reach a stream request.
    ///
    /// Narrower than the `RefusalVerdict::DurableMiss` arm that fires it, which
    /// also covers `BlobTooLarge` — hence `DurableMissCause`.
    pub probe_post_eviction_failures: Counter,
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
    /// `open_or_reuse_channel` failed before a pull could start. This is the
    /// node's own payment-side fault (gas, RPC, expired channel), NOT the
    /// provider's — a sustained rate means node→node buying is wedged. This is
    /// the *unlabeled total* across all causes; the
    /// `pool_open_failures_*_total` family below (#966) breaks the
    /// `openChannel`-tx failures out by cause so an operator can tell a
    /// misconfiguration (`insufficient_deposit`) from infrastructure
    /// (`rpc_error`). It also covers store/expired-reclaim causes the by-reason
    /// family does not, so the two are not expected to sum equal.
    pub node_pull_pool_open_failures: Counter,
    /// `decdn_pool_open_failures_insufficient_deposit_total` (#966): a buyer
    /// `openChannel` tx reverted because the node's USDC balance/allowance could
    /// not cover the deposit, or the deposit was zero — either as requested, or
    /// as the balance delta actually received under a fee-on-transfer token.
    /// Both zero cases revert the same argument-less `ZeroAmount`, so this
    /// counter cannot separate them; the wallet balance is what distinguishes a
    /// misconfigured deposit from a token that shaved it. A *misconfiguration*
    /// signal either way — the fix is operator-side (fund the wallet, raise the
    /// configured deposit), not infrastructure. A plain counter
    /// field carries no label dimension (a labeled series would need a `Family`),
    /// so the issue's `{reason=…}` split is realized as
    /// three sibling counters (mirroring `dht_rate_limit_rejected_*`); the
    /// `reason` value is the field-name token. The `OpenMetrics` encoder appends
    /// the `_total` suffix.
    pub pool_open_failures_insufficient_deposit: Counter,
    /// `decdn_pool_open_failures_contract_revert_total` (#966): a buyer
    /// `openChannel` tx reverted on-chain for a reason other than insufficient
    /// deposit (provider not active, a paused contract, a mined revert whose
    /// reason is not recoverable from the receipt). The deposit was not
    /// escrowed; the cause is on-chain state, not this node's wallet or RPC.
    pub pool_open_failures_contract_revert: Counter,
    /// `decdn_pool_open_failures_rpc_error_total` (#966): a buyer
    /// `openChannel` submit or receipt wait failed at the transport layer (no
    /// revert data) — connectivity, a timed-out receipt, a nonce blip. A
    /// *transient infrastructure* signal; retrying typically clears it. Pair
    /// with the two reverting counters above to tell "operator under-funded the
    /// wallet" from "the RPC endpoint is flaky".
    pub pool_open_failures_rpc_error: Counter,
    /// `decdn_node_pull_too_large_total` (#840): a selected upstream claimed a
    /// `total_bytes` above this node's `max_blob_size` ceiling, so the buyer
    /// rejected it before buffering. Like a channel-open failure this is a
    /// buyer-side policy decision, NOT necessarily provider misbehavior (the
    /// provider may legitimately serve larger blobs to nodes with a higher
    /// ceiling), so it does not tar the provider's reputation. A sustained rate
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
    pub serve_economics_refused: Counter,
    /// `decdn_node_pull_timeout_total` (#857): a buyer→upstream pull hit one of this node's
    /// own deadlines. Like a channel-open failure this is a buyer-side condition (a possibly
    /// mis-sized local budget), NOT evidence the provider is unreachable, so it does NOT tar
    /// the provider's local reputation. Distinct from `node_pull_through_timeouts` (the
    /// delivery handler's own serving deadline).
    ///
    /// It fires on **two** budgets, and they have different remedies (#1145 review):
    ///
    /// - the STREAM-OPEN stage exceeding `node_pull_timeout_sec`; and
    /// - a pull that has received no first byte within `node_pull_stall_timeout_sec`. Before
    ///   the first chunk, that clock is measuring the server's time-to-FIRST-byte, which
    ///   scales with blob size (the serve path materialises the whole bao encoding before it
    ///   can emit chunk #1) — so it is our deadline, not the peer's fault, and it lands here
    ///   rather than on `node_pull_stalled_total`.
    ///
    /// A sustained rate therefore means one of those two is too tight for the upstreams this
    /// node selects — and for the large-blob case it is `node_pull_stall_timeout_sec`, not
    /// `node_pull_timeout_sec`, that wants raising. (The doc named only the latter, which is
    /// the wrong knob for the flagship scenario it described.)
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
    /// `decdn_node_pull_abandon_drain_timeout_total` (#1779): an abandoned pull leg's upstream
    /// connection did not reach its drained state before the leg's drain ceiling, so
    /// the per-serve runtime was dropped with that connection's QUIC driver still on
    /// it.
    ///
    /// The connection is now STRANDED: with no driver it can never reach drained, and
    /// `Endpoint::close()` waits on exactly that. **Any sustained rate means this
    /// node's shutdown will hang**, and each tick is one upstream this node closed
    /// discourteously — the peer holds the connection open until its own idle timeout.
    ///
    /// Zero is the expected value at every load: the drain waits on the transition
    /// itself, not on a fixed span, so it ceilings out only when a connection is
    /// genuinely stuck. A non-zero rate points at the per-serve pull-leg runtimes
    /// (#1675), not at the peer.
    pub node_pull_abandon_drain_timeout: Counter,
    /// `decdn_node_pull_refused_total` (#1144): a selected upstream refused
    /// delivery up front (a `StreamResponse` with `ok == false`). Counts every
    /// wire code, including the `InternalError` that DOES tar the provider's
    /// reputation — so this is a refusal counter, not an exoneration counter, and
    /// it is deliberately not split by code (a labeled series would need a `Family`,
    /// and a per-code counter set is not yet worth five more series). Most refusals
    /// are honest and benign: a `NotFound` is simply a healthy-but-empty node, so
    /// a sustained rate here usually means content discovery is steering this node
    /// at upstreams that do not hold the blob — not that the upstreams are bad.
    pub node_pull_refused: Counter,
    /// `decdn_node_pull_refused_unattributable_total` (#1520): the subset of
    /// [`Self::node_pull_refused`] whose wire code this node cannot pin on the
    /// upstream — `NotFound` and `Overloaded`, i.e. `RefusalVerdict::Transient`.
    /// Those briefly suppress the `(peer, hash)` pair without touching reputation.
    ///
    /// Split out because it is the shape a *buyer-side* misconfiguration takes:
    /// `NotFound` is what a seller signs when it refuses OUR channel for
    /// insufficient deposit (the reject reasons collapse deliberately, see
    /// [`Self::serve_stream_rejected_insufficient_deposit`]), so a node whose own
    /// deposit is too small to buy anything sees 100% of its pulls refused with
    /// nothing in its own telemetry saying so. Before this the arm bumped no
    /// counter at all and logged at `debug`.
    ///
    /// Read it with one caveat: `Transient` is `NotFound | Overloaded`, and
    /// `Overloaded` is the PEER's backpressure — not local, and the code's own
    /// policy is to respect rather than punish it. So a network-wide load event
    /// drives this ratio to ~1 for a reason no local change fixes. A rate
    /// approaching `node_pull_refused` therefore means "nobody is serving me",
    /// which is *usually* local (deposit, binding) but is worth confirming against
    /// peer health first; a small fraction is the healthy "that peer did not
    /// have it".
    pub node_pull_refused_unattributable: Counter,
    /// `decdn_node_pull_stalled_total` (#1134): an upstream went silent mid-stream
    /// — no byte of progress within `node_pull_stall_timeout_sec` — so the pull was
    /// abandoned. UNLIKE `node_pull_timeout` (our own budget expiring, which is not
    /// evidence about the peer), this one DOES tar the provider's reputation: the
    /// clock resets on every byte received, so it can only fire on a provider that
    /// stopped delivering while we waited. A sustained rate points at flaky
    /// upstreams or a `node_pull_stall_timeout_sec` too tight for the network.
    pub node_pull_stalled: Counter,
    /// `decdn_node_pull_local_fault_total` (#1145 review): a pull failed for a reason
    /// that is OURS — a broken signer, an encode fault, a bad range computation, an
    /// unusable deadline config, or a buyer channel open this node's own state defeated
    /// (an unreadable or unwritable channel store, a poisoned open lock, a panicked open
    /// task, a wallet that cannot fund a deposit; #1560) — and the upstream was exonerated.
    ///
    /// The only counter here that says nothing about the network. Any sustained rate is
    /// an emergency: a node that cannot sign a voucher cannot pay for anything, so every
    /// pull it attempts will fail. Before this existed those failures were scored against
    /// whichever honest providers the node happened to try, so the symptom was a node
    /// steadily blaming a healthy network in its own reputation scores.
    ///
    /// Counted PER CANDIDATE, not per request: one node-wide fault moves this by up to
    /// `MAX_PROVIDER_ATTEMPTS` for a single client request, because the walk tries each
    /// candidate and fails identically on all of them. Read the rate, not the absolute,
    /// and do not infer the number of affected requests from it.
    pub node_pull_local_fault: Counter,
    /// `decdn_node_pull_pool_open_pending_total` (#1143): a buyer channel open
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
    ///    `CHANNEL_OPEN_CALLER_BUDGET` — the interesting one; and
    /// 2. a boot or idle **reconcile** holds the provider's open slot.
    ///
    /// The verdict is the same for both — try the next candidate, score nothing — which
    /// is why they share a counter. But the *diagnosis* is not: reconcile runs at every
    /// boot, so a restart produces a burst here that means nothing is wrong. Read a
    /// sustained rate as a chain-lane signal only once it outlives a restart.
    pub node_pull_pool_open_pending: Counter,
    /// `decdn_node_pull_progress_persist_failures_total` (#852): a pull paid ≥1
    /// voucher but persisting the buyer channel's resume watermark
    /// (`record_progress`) failed. The bytes were delivered, but the channel's
    /// stored `nonce`/`bytes`/`amount` now lags what the upstream accepted — the
    /// next reuse of this channel will re-sign a stale voucher and be rejected. A
    /// non-zero count means a provider is at risk of becoming unusable until
    /// channel rotation.
    pub node_pull_progress_persist_failures: Counter,
    /// `decdn_node_pull_progress_dropped_total` (#1145 review): a pull paid ≥1
    /// voucher, but by the time the watermark was written the provider's slot had
    /// been replaced by a newer open — so the write was skipped rather than clobber
    /// the replacement, and that voucher's progress is gone.
    ///
    /// Sibling of `node_pull_progress_persist_failures_total`, which only counts the
    /// `Err` path; this is the `Ok`-but-dropped path, which is otherwise invisible.
    /// The rare benign case is a channel rotating mid-pull. A *sustained* rate means
    /// a `channel_id`-plumbing bug, and every tick is real USDC whose watermark was
    /// discarded — so this is the counter to alert on, not just to look at.
    pub node_pull_progress_dropped: Counter,
    /// `decdn_node_pull_progress_superseded_total` (#1145 review): a pull's watermark write was
    /// REGRESSED because a CONCURRENT pull on the same shared channel ledger had already
    /// persisted a higher (correct) watermark. Benign and EXPECTED under the shared
    /// `BuyerLedgers` — concurrent pulls on one channel are routine — because the monotonic
    /// store keeps the winner's higher value, so no voucher is lost. Split from
    /// `node_pull_progress_persist_failures_total` (a real store-write failure that leaves the
    /// watermark lagging) so ordinary settle races do not drown out a genuine persist fault.
    pub node_pull_progress_superseded: Counter,
    /// `decdn_node_pull_reactive_topup_total` (#1530): a node→node miss pull hit its
    /// channel's deposit ceiling mid-stream, funded the shortfall on-chain, and
    /// resumed at the paid frontier.
    ///
    /// Expected to be RARE once `buyer_working_deposit_micro_usdc` is sized for the
    /// blobs this node pulls — the proactive low-water refill should refill a
    /// channel long before a single pull outruns it. A sustained rate means the
    /// working deposit is too small for the blob sizes in play, and every tick is a
    /// transaction plus a settlement wait a client sat through.
    ///
    /// Distinct from `decdn_buyer_topup_ok_total`, which counts on-chain top-ups from
    /// BOTH legs: this one isolates the reactive leg, so the proactive refill's
    /// routine traffic cannot hide it.
    pub node_pull_reactive_topup: Counter,
    /// `decdn_node_pull_reactive_topup_refused_total` (#1530): a mid-pull top-up was
    /// NOT performed, or was performed and added nothing.
    ///
    /// Three causes, one adversarial and two operational:
    ///
    /// - an upstream claiming `InsufficientDeposit` while our OWN ledger still covers
    ///   the next voucher — a lying or buggy peer trying to make us escrow more USDC
    ///   than we owe. `genuine_exhaustion` validates every claim against our own
    ///   accounting and we decline to fund an uncorroborated one; this counter is the
    ///   only place that becomes visible, so a sustained rate against one provider is
    ///   worth alerting on;
    /// - a funding transaction that failed (allowance, revert, RPC);
    /// - a top-up that landed but added no headroom.
    ///
    /// The three share a counter because the pull's outcome is identical — it ends on
    /// the original exhaustion — but only the first says anything about the peer. Read
    /// it alongside `decdn_buyer_topup_failure_total`, which the latter two also tick
    /// and the first does not.
    pub node_pull_reactive_topup_refused: Counter,
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
    /// `decdn_node_pull_through_client_abandoned_total` (#856): window-paced
    /// serves the requesting client dropped or underpaid mid-pull, so the node
    /// aborted the upstream pull and abandoned the partial fill. The per-request
    /// loss is bounded to the ramped credit window; a sustained rate flags a leech.
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
    /// error, or a present blob reporting no size) — surfaced to the client as
    /// `InternalError`, not a signed absence. Visible name:
    /// `decdn_serve_stream_rejected_internal_error_total`.
    pub serve_stream_rejected_internal_error: Counter,
    /// `serve_stream` requests refused because the blob exceeds the configured
    /// `max_blob_size_bytes`. Visible name:
    /// `decdn_serve_stream_rejected_blob_too_large_total`.
    pub serve_stream_rejected_blob_too_large: Counter,
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
    /// which. Wire-indistinguishable from `cache_miss` (signed as `NotFound`), so
    /// this server-side counter is the only place the *reason* lives at all — now
    /// more load-bearing, since a third refusal path routes through it.
    /// Visible name: `decdn_serve_stream_rejected_insufficient_deposit_total`.
    pub serve_stream_rejected_insufficient_deposit: Counter,
    /// Delivery refused because the lane already has enough concurrent
    /// same-lane streams in flight that admitting one more would exceed the
    /// pool's refundable-floor headroom. Wire-indistinguishable from
    /// `insufficient_deposit` (both signed as `NotFound`), so this counter is
    /// the only place the distinction lives — a rising value signals a lane
    /// experiencing sustained concurrency pressure. Visible name:
    /// `decdn_serve_stream_rejected_lane_at_capacity_total`.
    pub serve_stream_rejected_lane_at_capacity: Counter,
    /// Delivery refused because ONE capability signer's un-vouchered floor credit
    /// — its live reservations plus its permanent `dead_charge` — already fills
    /// its share of the pool budget, while the pool itself can still pay (ADR 003
    /// §Pool solvency, per-signer floor isolation). Wire-indistinguishable from
    /// `insufficient_deposit` (both signed as `NotFound`), so this counter is the
    /// only place the distinction lives — a rising value separates one signer
    /// abandoning streams on a shared pool from the pool genuinely running dry.
    /// Visible name:
    /// `decdn_serve_stream_rejected_signer_floor_at_cap_total`.
    pub serve_stream_rejected_signer_floor_at_cap: Counter,
    /// Delivery refused because the requested bounded range
    /// `[byte_offset, byte_offset + byte_len)` is out of bounds for the blob
    /// (ADR 005 §Bounded byte ranges: the node MUST reject an overflowing or
    /// past-EOF range). Wire-indistinguishable from `cache_miss` (signed as
    /// `NotFound`), so this server-side counter is the only place the
    /// distinction lives — a rising value flags clients issuing malformed
    /// ranges. Visible name:
    /// `decdn_serve_stream_rejected_range_not_satisfiable_total`.
    pub serve_stream_rejected_range_not_satisfiable: Counter,
    /// Delivery refused because the blob is on this operator's local denylist
    /// (ADR 011 §Local Denylist). Signed as `HashBlacklisted`. Deliberately
    /// counts ONLY the local list; the governance blacklist has its own
    /// counter, [`Self::serve_stream_rejected_chain_hash_denied`]. Splitting
    /// them here is safe where the wire code must not: this is the operator's
    /// own gauge of their own denylist, not something a client can probe. A
    /// rising value after a takedown is the confirmation the order is being
    /// discharged. Visible name:
    /// `decdn_serve_stream_rejected_hash_denied_total`.
    pub serve_stream_rejected_hash_denied: Counter,
    /// Delivery refused because the blob is on the *governance* blacklist (ADR
    /// 011 §On Blacklist Event). Also signed as `HashBlacklisted` — identically
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
    /// (ADR 011 §On Blacklist Event). Signed as `OriginBlacklisted`. Visible
    /// name: `decdn_serve_stream_rejected_origin_denied_total`.
    pub serve_stream_rejected_origin_denied: Counter,
    /// Delivery declined by the origin-only policy (#1759,
    /// `cache.relay_foreign_namespaces = false`): a memoized live probe of this
    /// node's own backend genuinely does not hold the hash. Signed as
    /// `NotFound`, identically to [`Self::serve_stream_rejected_cache_miss`] —
    /// a client cannot tell a policy decline from a real miss, which is the
    /// point — so this counter is the only place an operator can separate the
    /// two. Distinct from a backend FAULT during that same probe, which is
    /// never counted here: a fault is not an absence and is signed
    /// `InternalError`, landing in
    /// [`Self::serve_stream_rejected_internal_error`] instead. Visible name:
    /// `decdn_serve_stream_rejected_foreign_declined_total`.
    pub serve_stream_rejected_foreign_declined: Counter,
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
    /// `decdn_load_shed_egress_bps`: current measured egress EWMA, bytes/sec.
    pub load_shed_egress_bps: Gauge,
    /// `decdn_load_shed_pressure_active`: 1 while the load-shed policy considers
    /// the node pressured, else 0.
    pub load_shed_pressure_active: Gauge,

    // ---- Uniform watcher liveness + panic surface (#1316, #1320) ----
    //
    // The `*_down_seconds` family below is edge-triggered off a tick *error*, so
    // a watcher that panicked while healthy, wedged in an await, or exited
    // cleanly leaves `down_since == None` and reads a healthy `0` forever — a
    // dead watcher is byte-identical to a live one. These two families close
    // that gap for all six chain-event watchers: `*_last_tick_timestamp_seconds`
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
    /// `decdn_rate_bounds_watcher_last_tick_timestamp_seconds` (#1172): Unix time
    /// of the rate-bounds watcher's last successful poll tick. Load-bearing: the
    /// watcher's `getRateBounds()` poll failure and undecodable-log paths both
    /// return `Ok` by design (an `Err` would stall the cursor), so a watcher stuck
    /// in RPC backoff — or dead — is otherwise indistinguishable from a healthy
    /// one while the node keeps signing quotes against stale, economically
    /// load-bearing bounds. Alert on this gauge going stale.
    pub rate_bounds_watcher_last_tick_timestamp_seconds: Gauge,
    /// `decdn_fee_shares_watcher_last_tick_timestamp_seconds`: Unix time of the
    /// fee-shares watcher's last successful poll tick. Load-bearing for the
    /// same reason as `rate_bounds_watcher_last_tick_timestamp_seconds`: the
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
    /// `decdn_rate_bounds_watcher_task_panicked_total` (#1172): the rate-bounds
    /// watcher task unwound on a panic. Any non-zero value is a bug in this node.
    pub rate_bounds_watcher_task_panicked: Counter,
    /// `decdn_fee_shares_watcher_task_panicked_total`: the fee-shares watcher
    /// task unwound on a panic. Any non-zero value is a bug in this node.
    pub fee_shares_watcher_task_panicked: Counter,
    /// `decdn_settlement_watcher_task_panicked_total` (#1316): the
    /// payment-settlement watcher task unwound on a panic.
    pub settlement_watcher_task_panicked: Counter,

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
    /// — a disk error or a scope `eth_call` failure). Non-zero means the deny-set
    /// is not fully enforced and a blacklisted blob may still be servable and
    /// slashable, even while `blacklist_watcher_down_seconds` reads `0`. Answers
    /// a different question than the down-family, which tracks chain-read outages
    /// only. Pairs with the aggregate `warn!` in `rescan`.
    pub blacklist_enforcement_failures: Counter,
}

/// Aggregated deCDN node metrics.
#[derive(Debug)]
pub struct Metrics {
    registry: Arc<RwLock<Registry>>,
    decdn: Arc<DecdnMetrics>,
    cache: Arc<CacheMetrics>,
    inbound_streams: Arc<Gauge>,
    outbound_streams: Arc<Gauge>,
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
        let cache = Arc::new(CacheMetrics::default());
        let mut registry = Registry::default();
        registry.register(decdn.clone() as Arc<dyn MetricsGroup>);
        // Cache metrics live under the `decdn_cache` prefix so they
        // share the `decdn_*` family the rest of the metrics use.
        registry
            .sub_registry_with_prefix("decdn")
            .register(cache.clone() as Arc<dyn MetricsGroup>);
        Self {
            registry: Arc::new(RwLock::new(registry)),
            decdn,
            cache,
            inbound_streams,
            outbound_streams,
            probe_hold_exhausted,
            probe_hold_disabled,
            probe_hold_stake_lane_reserved,
            started_at: Instant::now(),
            staker_set_watcher_down_since: Mutex::new(None),
            slash_watcher_down_since: Mutex::new(None),
            blacklist_watcher_down_since: Mutex::new(None),
            settlement_watcher_down_since: Mutex::new(None),
        }
    }

    /// Shared `Arc<CacheMetrics>` for wiring into [`decdn_cache::CacheEngine`].
    pub fn cache_metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.cache)
    }

    /// Replace the inbound lane gauges with a snapshot from the live store.
    pub(crate) fn set_inbound_lane_snapshot(&self, open: usize, deposit: U256) {
        self.decdn.lanes_open.set(sat(open));
        self.decdn.pool_deposit_usdc.set(sat_u256(deposit));
    }

    /// Register iroh's transport metrics under the `decdn_iroh_` prefix so
    /// `magicsock_*`, `net_report_*`, etc. come out as
    /// `decdn_iroh_magicsock_*`, matching ADR 020's naming convention.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned.
    pub fn register_iroh_endpoint(&self, ep: &Endpoint) -> anyhow::Result<()> {
        let mut reg = self
            .registry
            .write()
            .map_err(|_| anyhow::anyhow!("metrics registry lock poisoned"))?;
        reg.sub_registry_with_prefix("decdn_iroh")
            .register_all(ep.metrics());
        Ok(())
    }

    /// Record a buyer `openChannel`-tx failure broken out by cause (#966): bumps
    /// the `decdn_pool_open_failures_{reason}_total` sibling counter for
    /// `reason`. Pairs with the structured `reason` field on the `warn!`/`debug!`
    /// in [`crate::node_origin`]. Distinct from
    /// [`Self::node_pull_pool_open_failure`], the unlabeled total (which also
    /// counts store/expired-reclaim causes that never reach the `openChannel`
    /// tx).
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
                // lock — so alert on the gauge, never on `rate(*_restarts_total)`
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
    probe_request => probe_requests.inc();

    /// Publish the current count of active probe holds (ADR 005).
    probe_hold_slots(used: usize) => probe_hold_slots_used.set(sat(used));

    /// Publish the configured `max_probe_holds` budget (registry-mandatory
    /// `decdn_probe_hold_slots_max`). Called once at runtime bring-up.
    probe_hold_slots_max(max: usize) => probe_hold_slots_max.set(sat(max));

    /// The node clamped `rate_per_mb` to the configured delivery bounds
    /// before signing (ADR 005 §Rate bounds validation).
    rate_bounds_clamped => rate_bounds_clamp_events.inc();

    /// An accepted voucher skipped one or more nonce values past
    /// `last_nonce + 1` (#747). Counted once per gapped voucher; the precise
    /// skip count rides the paired `tracing::warn!` in `apply_voucher`.
    voucher_nonce_gap => voucher_nonce_gaps.inc();

    /// A redeem hint was dropped because the bounded advisory channel was full
    /// (`try_send` → `Full`, #751). Advisory, so a few drops are benign; a
    /// sustained rate means the redeemer is not keeping up with fan-out.
    redeem_hint_dropped => redeem_hints_dropped.inc();

    /// A download-receipt audit write was dropped because the bounded writer
    /// queue was full (`try_send` → `Full`, #803). Audit-only, so a drop never
    /// affects settlement; a sustained rate means the writer is not keeping up
    /// with disk I/O and audit records are being lost.
    receipt_write_dropped => receipt_writes_dropped.inc();

    /// An ADR 041 warming serve credit was dropped before it reached the ledger
    /// — the bounded aggregator queue was full, or the aggregator was gone. The
    /// drop is conservative (the source ledger stays more negative than
    /// reality), but a sustained rate throttles speculative warming.
    warming_credit_dropped => warming_credits_dropped.inc();

    /// A redemption attempt (`try_redeem`) failed with an RPC/receipt error
    /// (#751). Pairs with the `warn!` in `redeemer_loop`.
    redemption_failure => redemption_failures.inc();

    /// A lane was held or dropped by `pool_is_redeemable` because its pool's
    /// chain-observed solvency ruled it out this pass (ADR 003).
    redemption_skipped_insolvent => redemption_skipped_insolvent.inc();

    /// `n` lanes were held or dropped by `pool_is_redeemable` in one planning
    /// pass (ADR 003); the batched form of `redemption_skipped_insolvent`.
    redemption_skipped_insolvent_by(n: u64) => redemption_skipped_insolvent.inc_by(n);

    /// A buyer-side reclaim-sweep attempt (`try_reclaim`) failed — an RPC/receipt
    /// error, an on-chain revert, or a failed store write when clearing the local
    /// record (#906). Pairs with the per-attempt `warn!` in `try_reclaim` and the
    /// threshold `error!` in `reclaim_once`.
    buyer_reclaim_failure => buyer_reclaim_failures.inc();

    /// Buyer-pool rows skipped as undecodable during one successful store
    /// hydration (#1271). Counted once per skipped row per load attempt (each of
    /// the startup, reconcile, and reclaim loads bumps it), so a persistent
    /// malformed row keeps the escrowed-but-untracked alert active. The
    /// `usize` count is saturated into the `u64` counter.
    buyer_pool_store_skipped_undecodable_records(count: usize)
        => buyer_pool_store_skipped_undecodable_records.inc_by(u64::try_from(count).unwrap_or(u64::MAX));

    /// A background low-water top-up (#1146) landed: a reused buyer channel below
    /// its 20% low-water mark was re-funded to the working deposit. Pairs with the
    /// `info!` in `spawn_refill_if_low`.
    buyer_topup_ok => buyer_topup_ok.inc();

    /// A background low-water top-up (#1146) did not cleanly land — the allowance
    /// re-approval or the `topUp` submit/receipt errored or reverted, OR the `topUp`
    /// landed on-chain but the local row vanished/rotated during the RPC
    /// (`DepositOutcome::UnknownProvider` / `ChannelMismatch`, i.e. escrowed-but-
    /// untracked). Folding the untracked case in here keeps stranded deposits
    /// visible on this counter. Best-effort, so the channel is left un-topped; pairs
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

    /// A floor dead-charge write failed: a drop-time persist (`record_loss` on a
    /// `FloorReservation` drop, leaving the durable total behind the in-memory
    /// accumulator until a later drop re-persists it) or a reclaimed pool's
    /// `forget_loss` (leaving the row deletable by nothing and untombstoned).
    /// Pairs with the `warn!`/`error!` lines in `handlers/client/mod.rs`
    /// ("floor dead-charge persist failed" / "pool dead-charge forget failed").
    floor_loss_persist_failure => floor_loss_persist_failures.inc();

    /// A distinct slash against this node's operator was detected by the slash
    /// watcher (#1032). Counts each `slashId` once (backfill + live dedup).
    slash_detected => slashes_detected.inc();

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

    /// An upstream refused a stream with `EvictedSinceProbe` after answering
    /// `has_blob: true` at probe (ADR 001 §Probe cache; #1165).
    probe_post_eviction_failure => probe_post_eviction_failures.inc();

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

    /// A buyer channel open/reuse failed before a pull could start (#831).
    node_pull_pool_open_failure => node_pull_pool_open_failures.inc();

    /// A selected upstream claimed a `total_bytes` above this node's
    /// `max_blob_size` ceiling and the buyer rejected it before buffering
    /// (#840). A buyer-side policy decision, so it does not score the provider.
    node_pull_too_large => node_pull_too_large.inc();

    /// A selected upstream quoted a per-MB rate above this node's effective buyer
    /// ceiling and the buyer refused before paying (#1375). A buyer-side policy
    /// decision, so it does not score the provider.
    node_pull_rate_above_ceiling => node_pull_rate_above_ceiling.inc();

    /// A cache-miss buy had candidates but every one quoted above this node's
    /// serve-economics buy ceiling, so the buyer declined an unprofitable relay
    /// (ADR 041). A buyer-side policy decision, so it does not score the provider.
    serve_economics_refused => serve_economics_refused.inc();

    /// Record a paid-delivery (`serve_stream`) request refused because the
    /// blob was evicted between probe and stream (#876).
    serve_stream_rejected_evicted_since_probe => serve_stream_rejected_evicted_since_probe.inc();

    /// Record a `serve_stream` request refused on a cache miss that
    /// pull-through could not fill (#876).
    serve_stream_rejected_cache_miss => serve_stream_rejected_cache_miss.inc();

    /// Record a `serve_stream` request refused by a local store fault,
    /// surfaced as `InternalError` (#876).
    serve_stream_rejected_internal_error => serve_stream_rejected_internal_error.inc();

    /// Record a `serve_stream` request refused because the blob exceeds
    /// `max_blob_size_bytes` (#876).
    serve_stream_rejected_blob_too_large => serve_stream_rejected_blob_too_large.inc();

    /// Record a `serve_stream` request refused on an unknown lane (#876).
    serve_stream_rejected_unknown_lane => serve_stream_rejected_unknown_lane.inc();

    /// Record a `serve_stream` request refused because the client binding did
    /// not authorize the named channel (#876).
    serve_stream_rejected_owner_mismatch => serve_stream_rejected_owner_mismatch.inc();

    /// Record a `serve_stream` cache-miss refused by the pre-flight deposit guard
    /// (#856): the requesting channel could not cover the worst-case blob cost,
    /// so no upstream pull was started.
    serve_stream_rejected_insufficient_deposit => serve_stream_rejected_insufficient_deposit.inc();

    /// Record a `serve_stream` delivery refused because the lane already has
    /// too many concurrent same-lane streams in flight and the pool's
    /// refundable floor cannot cover the reserved cost of another.
    serve_stream_rejected_lane_at_capacity => serve_stream_rejected_lane_at_capacity.inc();

    /// Record a `serve_stream` delivery refused because this capability signer's
    /// un-vouchered floor credit already fills its share of the pool budget,
    /// while the pool as a whole can still pay.
    serve_stream_rejected_signer_floor_at_cap => serve_stream_rejected_signer_floor_at_cap.inc();

    /// Record a `serve_stream` delivery refused because the requested bounded
    /// range is out of bounds for the blob (ADR 005 §Bounded byte ranges).
    serve_stream_rejected_range_not_satisfiable
        => serve_stream_rejected_range_not_satisfiable.inc();

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

    /// Record the current measured egress EWMA, bytes/sec.
    load_shed_egress_bps(bps: i64) => load_shed_egress_bps.set(bps);

    /// Record whether the load-shed policy considers the node pressured (1 for yes, 0 for no).
    load_shed_pressure_active(active: bool) => load_shed_pressure_active.set(i64::from(active));

    /// The window-paced serve loop paused the upstream pull at the ramped credit
    /// window to wait for the downstream voucher to clear (#856, #1669).
    node_pull_through_window_paused => node_pull_through_window_paused.inc();

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

    /// An abandoned pull leg's upstream connection did not drain inside its ceiling
    /// (#1779), so its QUIC driver is stranded on a runtime about to be dropped.
    /// Endpoint close will block — see the counter's docs.
    node_pull_abandon_drain_timeout => node_pull_abandon_drain_timeout.inc();

    /// A selected upstream refused delivery up front (#1144). Counts every wire
    /// code; only `InternalError` also scores the provider's reputation.
    node_pull_refused => node_pull_refused.inc();

    /// A refusal this node cannot attribute to the upstream (#1520) — `NotFound`
    /// or `Overloaded`. A rate approaching `node_pull_refused` means nobody will
    /// serve us, which is usually our own deposit or binding, not their fault.
    node_pull_refused_unattributable => node_pull_refused_unattributable.inc();

    /// An upstream went silent mid-stream (#1134); the pull was abandoned and the
    /// provider scored `Unreachable`.
    node_pull_stalled => node_pull_stalled.inc();

    /// A pull failed for a LOCAL reason (#1145 review) — signer, encode, range — so
    /// the upstream was exonerated. Says nothing about the network; any sustained
    /// rate means this node cannot pay for anything.
    node_pull_local_fault => node_pull_local_fault.inc();

    /// A buyer channel open outlived the per-candidate budget (#1143). The open
    /// continues in the background; the pull moves on. No reputation effect.
    node_pull_pool_open_pending => node_pull_pool_open_pending.inc();

    /// A pull paid ≥1 voucher but persisting the buyer channel resume watermark
    /// failed (#852); the channel's stored progress now lags the upstream.
    node_pull_progress_persist_failure => node_pull_progress_persist_failures.inc();

    /// A paid voucher's watermark was DROPPED because the provider's channel slot had
    /// been replaced by a newer open before the write landed (#1145 review).
    node_pull_progress_dropped => node_pull_progress_dropped.inc();

    /// A concurrent settle on the shared channel ledger persisted a higher watermark first, so
    /// this write was superseded (benign under `BuyerLedgers`; #1145 review).
    node_pull_progress_superseded => node_pull_progress_superseded.inc();

    /// A miss pull funded its exhausted channel mid-stream and resumed at the paid
    /// frontier (#1530).
    node_pull_reactive_topup => node_pull_reactive_topup.inc();

    /// A mid-pull top-up was declined or added no headroom: an upstream claiming
    /// exhaustion our own ledger contradicts, a failed funding tx, or a top-up that
    /// credited nothing (#1530).
    node_pull_reactive_topup_refused => node_pull_reactive_topup_refused.inc();

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
    connection_opened => active_connections.inc();
    connection_closed => active_connections.dec();

    /// A `cdn/client/v1` connection was reaped by the application-layer idle
    /// closer (ADR 005 §Connection lifetime).
    client_idle_close => client_idle_close.inc();

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
    /// Stamp the rate-bounds watcher's liveness gauge (#1172). See
    /// `slash_watcher_tick`; this one matters because the rate-bounds watcher's
    /// failure paths deliberately return `Ok`, so this gauge is the only signal
    /// that it is still polling.
    rate_bounds_watcher_tick => rate_bounds_watcher_last_tick_timestamp_seconds.set(unix_now_secs());
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
    /// Record that the rate-bounds watcher task unwound on a panic (#1172).
    rate_bounds_watcher_task_panicked => rate_bounds_watcher_task_panicked.inc();
    /// Record that the fee-shares watcher task unwound on a panic.
    fee_shares_watcher_task_panicked => fee_shares_watcher_task_panicked.inc();
    /// Record that the payment-settlement watcher task unwound on a panic (#1316).
    settlement_watcher_task_panicked => settlement_watcher_task_panicked.inc();

    /// Record `count` hashes a blacklist re-scope could not enforce this pass
    /// (#1319 — `Recheck::Failed`). One aggregated bump per pass, not per hash.
    blacklist_enforcement_failure(count: u64) => blacklist_enforcement_failures.inc_by(count);
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
                    tracing::warn!(%err, "metrics accept failed");
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
                tracing::debug!(%err, "metrics connection ended");
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
            tracing::warn!(%err, "metrics encode error");
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
mod tests {
    use std::fs;
    use std::path::Path;
    use std::time::Duration;

    use super::*;

    /// Match an exact `<name> <value>` metric line, anchored against
    /// surrounding lines so `decdn_cache_hits_total 1` doesn't
    /// accidentally substring-match into a future
    /// `decdn_cache_hits_total_foo` series or the `OpenMetrics`
    /// `_created` companion line.
    fn has_metric_line(text: &str, name: &str, value: u64) -> bool {
        let needle = format!("{name} {value}");
        text.lines().any(|l| l == needle)
    }

    /// Parse the `u64` value of an exact-named series from encoded
    /// `OpenMetrics` text. Applies the same whole-line discipline as
    /// [`has_metric_line`] — the name must be followed by a single space — so
    /// `decdn_x 5` never matches a `decdn_x_total`/`decdn_x_foo` sibling.
    /// Returns `None` if the series is absent or its value doesn't parse.
    fn metric_value(text: &str, name: &str) -> Option<u64> {
        text.lines()
            .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.parse::<u64>().ok())
    }

    #[test]
    fn key_rotation_metrics_export_canonical_names_at_zero() {
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();

        for name in [
            "decdn_streams_active{direction=\"inbound\"}",
            "decdn_streams_active{direction=\"outbound\"}",
            "decdn_lanes_open",
            "decdn_pool_deposit_usdc",
            "decdn_node_uptime_seconds",
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "canonical gauge {name} should be exposed at zero:\n{text}"
            );
        }
        assert!(
            !text
                .lines()
                .any(|line| line.starts_with("decdn_uptime_seconds ")),
            "retired uptime name must not be exported:\n{text}"
        );
    }

    #[test]
    fn probe_hold_unavailable_exports_every_reason_at_zero() {
        // Pins the pre-materialization in `Metrics::new` (#1443). A `Family`
        // creates each child series lazily on `get_or_create`, so without that
        // step a `reason` would be missing from `/metrics` until it first
        // fired — a dashboard gap where the three `reason` series of
        // `decdn_probe_hold_unavailable_total` read absent instead of zero, and
        // a silent hole in the `DecdnProbeHoldViolations` alert's
        // input. Also pins the rendered series text (label name, snake_case
        // value encoding, and the `_total` suffix the encoder appends). Note
        // that pinning it *here* does not tie it to the copies in
        // `monitoring/` — that is what
        // `alert_and_dashboard_selectors_match_the_exported_series` below does.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();

        for reason in ["exhausted", "disabled", "stake_lane_reserved"] {
            let name = format!("decdn_probe_hold_unavailable_total{{reason=\"{reason}\"}}");
            assert!(
                has_metric_line(&text, &name, 0),
                "{name} should be exposed at zero from a fresh registry:\n{text}"
            );
        }

        // None of these three names is a valid export; probe-hold reasons ship
        // only as `reason` labels on the collapsed counter
        // `decdn_probe_hold_unavailable_total`.
        for retired in [
            "decdn_probe_hold_violations_total",
            "decdn_probe_holds_disabled_total",
            "decdn_probe_stake_lane_reserved_total",
        ] {
            assert!(
                !text.lines().any(|line| line.starts_with(retired)),
                "retired metric name {retired} must not be exported:\n{text}"
            );
        }
    }

    /// Every series name the encoder actually emits, label suffixes stripped.
    ///
    /// Built from **sample** lines, not `# TYPE` lines. In `OpenMetrics` a
    /// counter's TYPE line carries the *unsuffixed* stem (`# TYPE
    /// decdn_cache_hits counter`) while the sample is `decdn_cache_hits_total
    /// 0` — so parsing TYPE would blind the gate to the `_total` suffix, which
    /// is the single most common way a documented name goes wrong here. (The
    /// convention is that a counter field omits `_total` and lets the encoder
    /// append it; the encoder only appends when it is absent, so a field that
    /// spells it out explicitly still exports correctly. Follow the
    /// convention in new code, but it is not a hard rule.)
    ///
    /// Truncating at the first `{` folds labelled families down to their base
    /// name, so `decdn_streams_active{direction="inbound"} 0` registers as
    /// `decdn_streams_active` — a name [`has_metric_line`] cannot match
    /// because it requires an exact value-bearing line.
    ///
    /// Histograms get their `_bucket`/`_sum`/`_count` samples folded back to
    /// the base name too. There is no bare `name` sample for a histogram, so
    /// without this the first `live` histogram row would fail the registry
    /// gate with a message telling its author to mark a genuinely-shipping
    /// metric `planned`. The exporter registers no histograms today; this is
    /// here so that stays a non-event when one lands.
    fn exported_series(text: &str) -> std::collections::HashSet<String> {
        text.lines()
            .filter(|l| !l.starts_with('#') && !l.is_empty())
            .filter_map(|l| {
                let name = l.split(['{', ' ']).next()?;
                (!name.is_empty()).then(|| name.to_string())
            })
            .flat_map(|name| {
                let base = ["_bucket", "_sum", "_count"]
                    .iter()
                    .find_map(|sfx| name.strip_suffix(sfx))
                    .map(str::to_string);
                std::iter::once(name).chain(base)
            })
            .collect()
    }

    /// Scan free-form text (YAML, JSON, markdown) for `decdn_`-prefixed
    /// identifiers. Hand-rolled rather than regex: the node crate has no
    /// `regex` dependency and this is a two-line character scan.
    fn decdn_names_in(text: &str) -> std::collections::BTreeSet<String> {
        let bytes = text.as_bytes();
        let mut out = std::collections::BTreeSet::new();
        let mut i = 0usize;
        while let Some(rel) = text.get(i..).and_then(|s| s.find("decdn_")) {
            let start = i + rel;
            let mut end = start;
            while bytes
                .get(end)
                .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
            {
                end += 1;
            }
            if let Some(name) = text.get(start..end) {
                out.insert(name.to_string());
            }
            i = end.max(start + 1);
        }
        out
    }

    /// Every `decdn_*` name in `monitoring/` must resolve to a real exported
    /// series. This is the blanket assertion the single-selector test below
    /// could not carry until #1513: `DecdnHighStreamErrorRate` divided by
    /// `decdn_streams_completed_total`, which no field produces, shipped as a
    /// rule that could never fire.
    ///
    /// **What this does not prove.** A name that resolves may still sit at a
    /// permanent zero because nothing increments it; the gate is about the
    /// name, not the wiring. `docs/runbook.md § ContentBlacklist compliance` is the live example.
    #[test]
    fn monitoring_selectors_are_exported() {
        // Names ending in `_` are filtered below: no exported series ends with
        // an underscore, so a trailing one means the scan stopped at a
        // wildcard — a prose `decdn_serve_stream_rejected_*`, or the inner
        // pattern of the `{__name__=~"decdn_.+_task_panicked_total"}` matcher.
        //
        // Belt-and-braces against a name that is not a series at all. Only
        // `decdn_health` qualifies today (a Prometheus `job=` label in the
        // blackbox-probe example at prometheus-alerts.yml's watcher group), and
        // it currently sits on a `#` line that the strip below already removes
        // — so this is unreachable unless that example migrates out of a
        // comment. Kept rather than deleted because a `job=` label is a
        // legitimate non-series `decdn_*` token that the scanner cannot
        // distinguish structurally.
        const NOT_SERIES: [&str; 1] = ["decdn_health"];

        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let exported = exported_series(&Metrics::new().encode().unwrap());

        for file in [
            "monitoring/prometheus-alerts.yml",
            "monitoring/grafana-dashboard.json",
        ] {
            let text = fs::read_to_string(root.join(file)).unwrap();
            // Drop whole-line YAML comments before scanning. A retired series
            // has to stay nameable in prose — the comments explaining why
            // `decdn_streams_failed_total` was removed are the record of that
            // decision, and a gate that forbade writing the name down would
            // push the next maintainer to delete the explanation instead of
            // the rule. Everything YAML actually evaluates survives the strip,
            // including block-scalar `expr: |` bodies. JSON has no comments, so
            // this is a no-op for the dashboard.
            let live: String = text
                .lines()
                .filter(|l| !l.trim_start().starts_with('#'))
                .collect::<Vec<_>>()
                .join("\n");
            let names = decdn_names_in(&live);
            // Floor sized just under the smaller file's real count (17 in
            // the alerts, 27 in the dashboard). A loose floor is the same
            // failure this gate exists to stop: a shape change that silently
            // drops most of the coverage while the test stays green.
            assert!(
                names.len() >= 15,
                "{file} yielded only {} names — the scanner or the file shape changed",
                names.len()
            );
            let stale: Vec<&String> = names
                .iter()
                .filter(|n| !n.ends_with('_'))
                .filter(|n| !NOT_SERIES.contains(&n.as_str()) && !exported.contains(*n))
                .collect();
            assert!(
                stale.is_empty(),
                "{file} references series the exporter does not emit: {stale:?}\n\
                 Rename them to the real field, or delete the alert/panel — do not \
                 add a waiver."
            );
        }
    }

    /// Every `live` row in the ADR metric registry must resolve to an exported
    /// series. `adr/appendix-observability.md` calls itself the canonical
    /// registry, so a row naming a series the node never emits sends operators
    /// off to build a dashboard that renders `(no data)` — which is what a
    /// stale registry row did until #1513.
    ///
    /// Rows whose Status column says `planned` are skipped: the registry is
    /// allowed to record design intent, it is just not allowed to do so
    /// silently. That column is the allowlist, and it is the reason this gate
    /// can be absolute rather than carrying a hand-maintained skip list that
    /// would rot the same way the names did.
    #[test]
    fn adr_registry_names_are_exported() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let text = fs::read_to_string(root.join("adr/appendix-observability.md")).unwrap();
        let exported = exported_series(&Metrics::new().encode().unwrap());

        let mut checked = 0usize;
        let mut stale: Vec<String> = Vec::new();
        for line in text.lines() {
            // A registry row is `| <name> | <Type> | <Tier> | <Status> | … |`.
            // Keying on the *Type* cell is what separates registry rows from
            // the other `decdn_*`-bearing tables in this file — the alert
            // thresholds (`| Metric | Warning | Critical | Action |`), the
            // health-endpoint JSON mapping, and the informal-name
            // cross-reference — without hard-coding section boundaries.
            let cells: Vec<&str> = line.split('|').map(str::trim).collect();
            let (Some(metric), Some(kind), Some(status)) =
                (cells.get(1), cells.get(2), cells.get(4))
            else {
                continue;
            };
            if !matches!(*kind, "Counter" | "Gauge" | "Histogram") {
                continue;
            }
            let Some(name) = metric.strip_prefix('`').and_then(|m| m.split('`').next()) else {
                continue;
            };
            // Documentation forms, not single series: label suffixes and
            // brace-expanded shorthand (`..._{per_peer,per_ip}_total`), and the
            // per-watcher templates (`decdn_<watcher>_task_panicked_total`),
            // which stand in for one row per watcher rather than naming one.
            if name.contains('{') || name.contains('<') {
                continue;
            }
            if *status == "planned" {
                continue;
            }
            assert_eq!(
                *status, "live",
                "row for {name} has Status {status:?}; the only values are `live` and `planned`"
            );
            checked += 1;
            if !exported.contains(name) {
                stale.push(name.to_string());
            }
        }

        // Floor sized just under the real count (47 live rows today). `> 20`
        // would tolerate a table-shape change that silently dropped more than
        // half the registry — the exact rot this gate exists to catch.
        assert!(
            checked >= 40,
            "only {checked} registry rows parsed — the table shape changed and this \
             gate silently stopped covering the registry"
        );
        assert!(
            stale.is_empty(),
            "adr/appendix-observability.md documents series the exporter does not emit: \
             {stale:?}\nEither correct the name or mark the row `planned` in its Status \
             column."
        );
    }

    #[test]
    fn alert_and_dashboard_selectors_match_the_exported_series() {
        // Nothing in CI validates `monitoring/` against the code — there is no
        // promtool step and no reference to the directory in any workflow — so
        // the alert and the dashboard hard-code a series name that only this
        // test ties back to the encoder. Without it, renaming the metric, the
        // `reason` label key, or the `Exhausted` variant updates the tests
        // above, passes CI green, and leaves `DecdnProbeHoldViolations`
        // querying a series that no longer exists: the page for probe-hold
        // budget pressure then silently never fires again.
        //
        // Scoped to the one series — it asserts the *query line*, which the blanket
        // name gate below cannot. The blanket "every `decdn_*` in monitoring/ is
        // exported" check lives in `monitoring_selectors_are_exported`.
        const SELECTOR: &str = "decdn_probe_hold_unavailable_total{reason=\"exhausted\"";

        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let alerts = fs::read_to_string(root.join("monitoring/prometheus-alerts.yml")).unwrap();
        let dashboard = fs::read_to_string(root.join("monitoring/grafana-dashboard.json")).unwrap();

        // Match the query line specifically, not the file. Both files also
        // name the series in prose (the alert's `description`, the dashboard's
        // `legendFormat`), and a whole-file `contains` would let that prose
        // mask a rename of the actual query — verified: mutating only the
        // `expr:` left a file-wide check green.
        assert!(
            alerts
                .lines()
                .any(|line| line.contains("expr:") && line.contains(SELECTOR)),
            "no `expr:` in monitoring/prometheus-alerts.yml queries {SELECTOR}"
        );
        // Grafana stores the query inside a JSON string, so the quotes around
        // the label value arrive backslash-escaped.
        let escaped = SELECTOR.replace('"', "\\\"");
        assert!(
            dashboard
                .lines()
                .any(|line| line.contains("\"expr\":") && line.contains(&escaped)),
            "no `expr` in monitoring/grafana-dashboard.json queries {SELECTOR}"
        );
        assert!(
            Metrics::new()
                .encode()
                .unwrap()
                .lines()
                .any(|line| line.starts_with(SELECTOR)),
            "the exporter no longer produces {SELECTOR}"
        );
    }

    #[test]
    fn probe_hold_unavailable_increments_only_the_named_reason() {
        // The whole point of the label is that each value keeps its own
        // remedy: `exhausted` drives "raise max_probe_holds", `disabled` is an
        // intentional operator choice, and `stake_lane_reserved` is a priority
        // decision taken before the cache is even consulted. Bumping one must
        // never move another, or the alert filter re-fires on the deliberate
        // cases the #739/#757 split existed to keep out.
        let metrics = Metrics::new();
        metrics.probe_hold_unavailable(ProbeHoldUnavailableReason::Exhausted);
        let text = metrics.encode().unwrap();

        assert!(
            has_metric_line(
                &text,
                "decdn_probe_hold_unavailable_total{reason=\"exhausted\"}",
                1
            ),
            "the named reason must increment:\n{text}"
        );
        for untouched in ["disabled", "stake_lane_reserved"] {
            let name = format!("decdn_probe_hold_unavailable_total{{reason=\"{untouched}\"}}");
            assert!(
                has_metric_line(&text, &name, 0),
                "{name} must stay at zero:\n{text}"
            );
        }
    }

    #[test]
    fn uptime_counts_from_construction_without_any_bring_up_hook() {
        // Regression guard (#1264): `decdn_node_uptime_seconds` derives solely
        // from `started_at`, which `Metrics::new()` stamps at construction, and
        // `encode()` recomputes it every scrape. There is no recorder that
        // marks "started" — a re-introduced `set(0)` (or any pre-scrape write)
        // would be unobservable and is exactly the dead surface this issue
        // removed. Backdate `started_at` and scrape WITHOUT calling any hook:
        // uptime must reflect the backdate rather than reset to 0.
        //
        // `Instant` is monotonic (boot-relative), so the backdate stays small
        // to keep `checked_sub` from underflowing to `None` on a freshly-booted
        // CI host, and the check is a range rather than an exact second count so
        // scrape-time `as_secs()` truncation can't race it.
        const BACKDATE_SECS: u64 = 60;
        let mut metrics = Metrics::new();
        metrics.started_at = Instant::now()
            .checked_sub(Duration::from_secs(BACKDATE_SECS))
            .unwrap_or_else(Instant::now);

        let text = metrics.encode().unwrap();
        let uptime = metric_value(&text, "decdn_node_uptime_seconds").unwrap();
        assert!(
            (BACKDATE_SECS..3600).contains(&uptime),
            "uptime must count from construction (started_at ~{BACKDATE_SECS}s ago), got {uptime}:\n{text}"
        );
    }

    #[test]
    fn stream_guards_track_direction_and_drop_on_error() {
        fn inbound_error(metrics: &Metrics) -> Result<(), ()> {
            let _guard = metrics.inbound_stream_guard();
            let text = metrics.encode().unwrap();
            assert!(has_metric_line(
                &text,
                "decdn_streams_active{direction=\"inbound\"}",
                1
            ));
            Err(())
        }

        let metrics = Metrics::new();
        assert!(inbound_error(&metrics).is_err());
        assert!(has_metric_line(
            &metrics.encode().unwrap(),
            "decdn_streams_active{direction=\"inbound\"}",
            0
        ));

        {
            let _first = metrics.outbound_stream_guard();
            let _second = metrics.outbound_stream_guard();
            assert!(has_metric_line(
                &metrics.encode().unwrap(),
                "decdn_streams_active{direction=\"outbound\"}",
                2
            ));
        }
        assert!(has_metric_line(
            &metrics.encode().unwrap(),
            "decdn_streams_active{direction=\"outbound\"}",
            0
        ));
    }

    #[test]
    fn cache_metrics_counters_start_at_zero() {
        // Pinning down the OpenMetrics shape — a fresh registry must
        // expose the cache counters at zero so dashboards built before
        // any fetch has fired don't render `(no data)`.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        for name in [
            "decdn_cache_origin_fetches_total",
            "decdn_cache_origin_retry_exhausted_total",
            "decdn_cache_origin_fallback_total",
            "decdn_cache_hits_total",
            "decdn_cache_misses_total",
            "decdn_cache_bytes_returned_total",
            "decdn_cache_pull_through_bytes_total",
            // GC counters (#518). The Rust struct fields are `gc_runs`
            // / `gc_bytes_reclaimed`; the OpenMetrics encoder appends
            // `_total`. Asserting the suffixed forms locks in the
            // exported names — a regression that re-renamed the
            // struct fields to include `_total` would emit
            // `..._total_total`, breaking dashboards/alerts that
            // reference the names below.
            "decdn_cache_gc_runs_total",
            "decdn_cache_gc_bytes_reclaimed_total",
            // Circuit-breaker counters (#963). Auto-exposed via the
            // `MetricsGroup` derive; pin the exported names so dashboards
            // tracking origin-outage load-shed don't silently lose them.
            "decdn_cache_circuit_breaker_trips_total",
            "decdn_cache_circuit_breaker_recoveries_total",
            "decdn_cache_circuit_breaker_short_circuits_total",
            // Eviction-driver counters (#1173). Same `_total`-suffix trap as
            // the GC pair above: the struct fields are `evictions`,
            // `evictions_bytes`, `evictions_starved`, `size_measure_failures`,
            // `evicted_operator`.
            "decdn_cache_evictions_total",
            "decdn_cache_evictions_bytes_total",
            "decdn_cache_evictions_starved_total",
            "decdn_cache_size_measure_failures_total",
            "decdn_cache_evicted_operator_total",
            // Best-effort tag-deletion failures. The array is hand-maintained, so
            // a new `CacheMetrics` field is covered only if someone adds it.
            "decdn_cache_tag_drop_failures_total",
            // In-flight coalescing mutex poison (#1517). The struct field is
            // `inflight_mutex_poisoned`. Any nonzero value is a bug report, so
            // the series must exist from a fresh registry — an operator has to
            // be able to alert on `> 0` before it has ever fired.
            "decdn_cache_inflight_mutex_poisoned_total",
            // Origin-rescan probe faults. The struct field is
            // `origin_probe_failures`. The announce set shrinks silently
            // without it, so an operator has to be able to alert on `> 0`
            // before it has ever fired.
            "decdn_cache_origin_probe_failures_total",
            // The other leg of the same rescan. The struct field is
            // `origin_enumerate_failures`.
            "decdn_cache_origin_enumerate_failures_total",
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "counter {name} should be exposed at zero on a fresh registry:\n{text}"
            );
        }

        // Cache-health GAUGES (#1173) take the opposite naming rule: the
        // encoder appends `_total` only to counters, so these must appear
        // WITHOUT a suffix. Asserting both families together pins the
        // distinction that `crates/cache/src/metrics.rs`'s module doc describes.
        for name in [
            "decdn_cache_bytes",
            "decdn_cache_size_limit_bytes",
            "decdn_cache_pinned_count",
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "gauge {name} should be exposed at zero on a fresh registry:\n{text}"
            );
        }
    }

    #[test]
    fn quic_0rtt_and_session_ticket_series_are_not_exposed() {
        // The probe path is a plain QUIC handshake: no early-data
        // classification and no session-ticket working-set proxy, so no
        // metric may name either. A partial revert that reintroduces one
        // counter but not its recorder shows up here.
        //
        // The needles are matched against the whole exposition, which also
        // carries iroh's transport metrics — so an upstream series named
        // `*0rtt*` would trip this too. That breadth is deliberate: this
        // node claims to expose no early-data accounting at all, whoever
        // registered it.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        for needle in ["0rtt", "session_ticket"] {
            assert!(
                !text.contains(needle),
                "exposition must not carry a {needle} series:\n{text}"
            );
        }
    }

    #[test]
    fn voucher_nonce_gap_metric_starts_at_zero_and_increments() {
        // #747. The struct field is `voucher_nonce_gaps`; the OpenMetrics
        // encoder appends `_total`, so the exported name is
        // `decdn_voucher_nonce_gaps_total` — the operator-visible name the
        // `apply_voucher` docs and any alert reference. Asserting the suffixed
        // form locks it in: re-naming the field to include `_total` would emit
        // `..._total_total` (the same footgun the cache GC counters guard
        // against above). The counter is event-scoped — `voucher_nonce_gap()`
        // bumps it once per gapped voucher regardless of gap size — so two
        // calls must read exactly 2.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_voucher_nonce_gaps_total", 0),
            "voucher nonce-gap counter should be exposed at zero on a fresh registry:\n{text}"
        );

        metrics.voucher_nonce_gap();
        metrics.voucher_nonce_gap();

        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_voucher_nonce_gaps_total", 2),
            "expected 2 gap events (one bump each, not gap-size weighted):\n{text}"
        );
    }

    #[test]
    fn buyer_pool_skipped_undecodable_metric_starts_at_zero_and_increments() {
        // #1271. The struct field is `buyer_pool_store_skipped_undecodable_records`;
        // the OpenMetrics encoder appends `_total`, so the exported name is
        // `decdn_buyer_pool_store_skipped_undecodable_records_total` — the
        // operator-visible name the observability appendix and any escrowed-but-
        // untracked alert reference. Pin the suffixed form: a rename that re-added
        // `_total` would emit `..._total_total` (the same footgun the cache GC and
        // voucher-nonce counters guard against), silently dropping the alert. The
        // counter takes a per-load skipped count, so `inc_by(2)` must read 2.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(
                &text,
                "decdn_buyer_pool_store_skipped_undecodable_records_total",
                0
            ),
            "skipped-undecodable counter should be exposed at zero on a fresh registry:\n{text}"
        );

        metrics.buyer_pool_store_skipped_undecodable_records(2);

        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(
                &text,
                "decdn_buyer_pool_store_skipped_undecodable_records_total",
                2
            ),
            "expected the per-load skipped count (2) to increment the counter:\n{text}"
        );
    }

    #[test]
    fn serve_stream_rejected_counters_start_at_zero_and_increment_per_reason() {
        // #876. Each `serve_stream` reject branch maps to a distinct counter
        // because a plain counter field carries no label dimension and the wire
        // `StreamError` deliberately conflates the three `NotFound` reasons.
        // The exported names carry the encoder-appended `_total` suffix. All
        // must be exposed at zero on a fresh registry (so dashboards don't read
        // `(no data)`) and each method must bump exactly its own counter.
        let metrics = Metrics::new();
        let reasons = [
            "decdn_serve_stream_rejected_evicted_since_probe_total",
            "decdn_serve_stream_rejected_cache_miss_total",
            "decdn_serve_stream_rejected_internal_error_total",
            "decdn_serve_stream_rejected_blob_too_large_total",
            "decdn_serve_stream_rejected_unknown_lane_total",
            "decdn_serve_stream_rejected_owner_mismatch_total",
            "decdn_serve_stream_rejected_insufficient_deposit_total",
            "decdn_serve_stream_rejected_lane_at_capacity_total",
            "decdn_serve_stream_rejected_signer_floor_at_cap_total",
            // Completed in #1520. These four always exported (the fields have
            // existed as long as their siblings) — what was missing was any
            // assertion pinning it, so a rename could have silently broken a
            // dashboard without failing this test.
            "decdn_serve_stream_rejected_range_not_satisfiable_total",
            "decdn_serve_stream_rejected_hash_denied_total",
            "decdn_serve_stream_rejected_chain_hash_denied_total",
            "decdn_serve_stream_rejected_origin_denied_total",
            "decdn_serve_stream_rejected_foreign_declined_total",
        ];
        let text = metrics.encode().unwrap();
        for name in reasons {
            assert!(
                has_metric_line(&text, name, 0),
                "reject counter {name} should be exposed at zero on a fresh registry:\n{text}"
            );
        }

        metrics.serve_stream_rejected_evicted_since_probe();
        metrics.serve_stream_rejected_cache_miss();
        metrics.serve_stream_rejected_internal_error();
        metrics.serve_stream_rejected_blob_too_large();
        metrics.serve_stream_rejected_unknown_lane();
        metrics.serve_stream_rejected_owner_mismatch();
        metrics.serve_stream_rejected_insufficient_deposit();
        metrics.serve_stream_rejected_lane_at_capacity();
        metrics.serve_stream_rejected_signer_floor_at_cap();
        metrics.serve_stream_rejected_range_not_satisfiable();
        metrics.serve_stream_rejected_hash_denied();
        metrics.serve_stream_rejected_chain_hash_denied();
        metrics.serve_stream_rejected_origin_denied();
        metrics.serve_stream_rejected_foreign_declined();

        let text = metrics.encode().unwrap();
        for name in reasons {
            assert!(
                has_metric_line(&text, name, 1),
                "reject counter {name} should read exactly 1 after one bump:\n{text}"
            );
        }
    }

    #[test]
    fn load_shed_metrics_appear_in_scrape() {
        let metrics = Metrics::new();
        metrics.serve_stream_rejected_load_shed_hit();
        metrics.serve_stream_rejected_load_shed_miss();
        metrics.load_shed_egress_bps(1_234);
        metrics.load_shed_pressure_active(true);
        let text = metrics.encode().unwrap();
        assert!(
            text.contains("decdn_serve_stream_rejected_load_shed_hit_total 1"),
            "{text}"
        );
        assert!(
            text.contains("decdn_serve_stream_rejected_load_shed_miss_total 1"),
            "{text}"
        );
        assert!(text.contains("decdn_load_shed_egress_bps 1234"), "{text}");
        assert!(text.contains("decdn_load_shed_pressure_active 1"), "{text}");
    }

    #[test]
    fn receipt_writes_dropped_metric_starts_at_zero_and_increments() {
        // #803. The struct field is `receipt_writes_dropped`; the OpenMetrics
        // encoder appends `_total`, so the exported name is
        // `decdn_receipt_writes_dropped_total` — the operator-visible name an
        // alert on lost audit records references. Must be exposed at zero on a
        // fresh registry and bump once per dropped receipt.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_receipt_writes_dropped_total", 0),
            "receipt-write-dropped counter should be exposed at zero on a fresh registry:\n{text}"
        );

        metrics.receipt_write_dropped();

        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_receipt_writes_dropped_total", 1),
            "expected one dropped-receipt event:\n{text}"
        );
    }

    #[test]
    fn warming_credits_dropped_metric_starts_at_zero_and_increments() {
        // ADR 041. Exposed at zero on a fresh registry so an alert can be
        // written against it before the first drop ever happens, and bumped once
        // per credit that never reached the ledger. A rate that tracks the serve
        // rate means the aggregator is gone and warming will stop node-wide.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_warming_credits_dropped_total", 0),
            "warming-credit-dropped counter should be exposed at zero on a fresh registry:\n{text}"
        );

        metrics.warming_credit_dropped();

        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_warming_credits_dropped_total", 1),
            "expected one dropped-warming-credit event:\n{text}"
        );
    }

    #[test]
    fn staker_set_watcher_metrics_start_at_zero_and_increment() {
        // #783. The struct field is `staker_set_watcher_restarts`; the
        // OpenMetrics encoder appends `_total`, so the exported name is
        // `decdn_staker_set_watcher_restarts_total` — the operator-visible
        // name any alert references. Asserting the suffixed form locks it in:
        // re-naming the field to include `_total` would emit `..._total_total`
        // (the same footgun the cache GC counters guard against).
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_restarts_total", 0),
            "watcher restart counter should be exposed at zero on a fresh registry:\n{text}"
        );
        // Both gauges are exposed at zero so dashboards don't render `(no
        // data)` before the watcher's first event. `down_seconds` reads 0
        // before any cycle is established (a quiet "not yet up").
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 0),
            "watcher down-seconds gauge should start at zero:\n{text}"
        );
        assert!(
            has_metric_line(&text, "decdn_staker_set_active_count", 0),
            "active-count gauge should start at zero:\n{text}"
        );

        // The resolve-failure counter (#788) is also exposed at zero.
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_resolve_failures_total", 0),
            "resolve-failure counter should start at zero:\n{text}"
        );

        // Two distinct drift windows (each `backoff_started` is an edge into
        // the error state; `cycle_established` between them closes the first).
        metrics.staker_set_watcher_backoff_started();
        metrics.staker_set_watcher_cycle_established();
        metrics.staker_set_watcher_backoff_started();
        metrics.staker_set_watcher_resolve_failure();
        metrics.staker_set_active_count(7);

        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_restarts_total", 2),
            "expected 2 restart events:\n{text}"
        );
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_resolve_failures_total", 1),
            "expected 1 resolve failure:\n{text}"
        );
        assert!(
            has_metric_line(&text, "decdn_staker_set_active_count", 7),
            "expected active-count gauge to report 7:\n{text}"
        );
    }

    #[test]
    fn staker_set_watcher_down_seconds_reads_zero_across_a_long_healthy_cycle() {
        // CRITICAL-fix regression guard (#788): `down_seconds` measures true
        // downtime, NOT cycle age. A healthy poll loop persists
        // indefinitely, so the gauge must read 0 for the entire life of an
        // established cycle — even when the node has been up (and the cycle
        // live) for a while. We simulate "a while" by backdating the metrics'
        // `started_at` well into the past: under the OLD age-based model the
        // gauge climbed with cycle age and would read ~3600 here; under the
        // downtime model `down_since` is `None`, so it stays 0.
        let mut metrics = Metrics::new();
        metrics.started_at = Instant::now()
            .checked_sub(Duration::from_hours(1))
            .unwrap_or_else(Instant::now);
        metrics.staker_set_watcher_cycle_established();

        let text = metrics.encode().unwrap();
        // Sanity: the node really is "old" (uptime reflects the backdate), so a
        // gauge that tracked age would be non-zero.
        assert!(
            has_metric_line(&text, "decdn_node_uptime_seconds", 3600),
            "uptime should reflect the backdated start:\n{text}"
        );
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 0),
            "down-seconds must read 0 across a long healthy cycle (true downtime, not age):\n{text}"
        );

        // An error opens the drift window: down-seconds is now driven by
        // `down_since` (not cycle age). An immediate scrape reads ~0s of
        // *downtime*; backdating `down_since` proves it then climbs.
        metrics.staker_set_watcher_backoff_started();
        if let Ok(mut down_since) = metrics.staker_set_watcher_down_since.lock() {
            *down_since = Instant::now().checked_sub(Duration::from_secs(150));
        }
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 150),
            "down-seconds should climb to the downtime depth once in backoff:\n{text}"
        );

        // Re-establishing the cycle clears the window back to 0.
        metrics.staker_set_watcher_cycle_established();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 0),
            "down-seconds should reset to 0 once filters re-establish:\n{text}"
        );
    }

    #[test]
    fn slash_watcher_down_seconds_tracks_true_downtime() {
        // Mirrors the staker-set guard for the slash watcher (#1032): the gauge
        // measures downtime, not cycle age, so a long healthy cycle reads 0, a
        // backoff window climbs, and re-establishing clears it.
        let mut metrics = Metrics::new();
        metrics.started_at = Instant::now()
            .checked_sub(Duration::from_hours(1))
            .unwrap_or_else(Instant::now);
        metrics.slash_watcher_cycle_established();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_slash_watcher_down_seconds", 0),
            "down-seconds must read 0 across a long healthy cycle:\n{text}"
        );

        metrics.slash_watcher_backoff_started();
        if let Ok(mut down_since) = metrics.slash_watcher_down_since.lock() {
            *down_since = Instant::now().checked_sub(Duration::from_secs(150));
        }
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_slash_watcher_down_seconds", 150),
            "down-seconds should climb to the downtime depth once in backoff:\n{text}"
        );
        // The restart counter bumps exactly once per drift window (edge-triggered).
        metrics.slash_watcher_backoff_started();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_slash_watcher_restarts_total", 1),
            "restarts must bump once per drift window, not per call:\n{text}"
        );

        metrics.slash_watcher_cycle_established();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_slash_watcher_down_seconds", 0),
            "down-seconds should reset to 0 once the cycle re-establishes:\n{text}"
        );
    }

    #[test]
    #[allow(clippy::panic)] // deliberately poison the lock, mirroring `dispatch::tests`.
    fn poisoned_down_since_reports_i64_max_not_zero() {
        // The load-bearing invariant of the down-seconds gauges: a poisoned
        // `down_since` must report `i64::MAX`, never `0`, because reporting `0`
        // would MASK an in-progress outage (#783/#1032). The watchers'
        // scrape recompute goes through one shared `refresh_watcher_down_seconds`
        // template, so poisoning any single lock exercises that conservative-alerting
        // fallback for all of them.
        let metrics = Arc::new(Metrics::new());
        let for_thread = Arc::clone(&metrics);
        // Poison `slash_watcher_down_since` from a panicking thread holding the
        // lock (the house pattern — see `dispatch::tests`).
        let join = std::thread::spawn(move || {
            let _g = for_thread.slash_watcher_down_since.lock().unwrap();
            panic!("intentional");
        });
        let _ = join.join();
        assert!(metrics.slash_watcher_down_since.is_poisoned());

        let i64_max = u64::try_from(i64::MAX).unwrap();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_slash_watcher_down_seconds", i64_max),
            "a poisoned down_since must report i64::MAX, not 0:\n{text}"
        );
        // The two healthy watchers still read 0 in the same scrape — the poison
        // is isolated to its own row of the shared recompute.
        assert!(
            has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 0),
            "a poisoned slash lock must not perturb the staker-set gauge:\n{text}"
        );
        // The watchers brought to down-family parity (#1283/#1316) share
        // the same recompute row, so they too read a clean 0 under the poison.
        for name in [
            "decdn_blacklist_watcher_down_seconds",
            "decdn_settlement_watcher_down_seconds",
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "a poisoned slash lock must not perturb {name}:\n{text}"
            );
        }
    }

    #[test]
    fn watcher_liveness_gauges_stamp_wall_clock_on_tick() {
        // The `*_watcher_tick` recorders (the `on_tick_success` hooks) stamp a
        // wall-clock timestamp, so a live watcher's gauge is non-zero and a dead
        // one's stays 0 — the positive liveness signal the error-triggered
        // down-seconds gauge cannot provide (#1316/#1320).
        let metrics = Metrics::new();
        let before = unix_now_secs();

        metrics.slash_watcher_tick();
        metrics.staker_set_watcher_tick();
        metrics.blacklist_watcher_tick();
        metrics.settlement_watcher_tick();

        let text = metrics.encode().unwrap();
        for name in [
            "decdn_slash_watcher_last_tick_timestamp_seconds",
            "decdn_staker_set_watcher_last_tick_timestamp_seconds",
            "decdn_blacklist_watcher_last_tick_timestamp_seconds",
            "decdn_settlement_watcher_last_tick_timestamp_seconds",
        ] {
            let floor = u64::try_from(before).unwrap();
            assert!(
                metric_value(&text, name).is_some_and(|stamped| stamped >= floor),
                "liveness gauge {name} must be exported and stamped with the current time:\n{text}"
            );
        }
    }

    #[test]
    #[allow(clippy::type_complexity)] // a compact table of (recorder, recorder, gauge, counter).
    fn new_watchers_down_seconds_track_true_downtime() {
        // The watchers brought to down-family parity (#1283/#1316) share
        // the `watcher_downtime_recorders!` template, so one compact pass per
        // watcher confirms the wiring: healthy reads 0, backoff climbs, one
        // restart per drift window, re-establish clears.
        let cases: [(fn(&Metrics), fn(&Metrics), &str, &str); 2] = [
            (
                Metrics::blacklist_watcher_cycle_established,
                Metrics::blacklist_watcher_backoff_started,
                "decdn_blacklist_watcher_down_seconds",
                "decdn_blacklist_watcher_restarts_total",
            ),
            (
                Metrics::settlement_watcher_cycle_established,
                Metrics::settlement_watcher_backoff_started,
                "decdn_settlement_watcher_down_seconds",
                "decdn_settlement_watcher_restarts_total",
            ),
        ];
        for (established, backoff, down_seconds, restarts) in cases {
            let metrics = Metrics::new();
            established(&metrics);
            let text = metrics.encode().unwrap();
            assert!(
                has_metric_line(&text, down_seconds, 0),
                "{down_seconds} must read 0 on a healthy cycle:\n{text}"
            );
            backoff(&metrics);
            backoff(&metrics); // second call: same drift window, no extra restart.
            let text = metrics.encode().unwrap();
            assert!(
                has_metric_line(&text, restarts, 1),
                "{restarts} must bump once per drift window:\n{text}"
            );
            established(&metrics);
            let text = metrics.encode().unwrap();
            assert!(
                has_metric_line(&text, down_seconds, 0),
                "{down_seconds} must reset to 0 once re-established:\n{text}"
            );
        }
    }

    #[tokio::test]
    async fn engine_bumps_surface_in_openmetrics_output() {
        use std::sync::Arc;

        use bytes::Bytes;
        use decdn_cache::{
            CacheEngine, Origin, OriginFetch, OriginKind, OriginPullError, PinnedHashes,
            RetryPolicy,
        };
        use iroh_blobs::Hash;

        // Minimal in-memory origin: returns the prearranged payload for
        // its hash, NotFound otherwise. Mirrors the StubOrigin used in
        // crates/cache tests but is local to this integration test so
        // we don't need to expose the cache crate's test fixtures.
        #[derive(Debug)]
        struct StubOrigin {
            data: Bytes,
            hash: Hash,
        }
        impl Origin for StubOrigin {
            fn kind(&self) -> OriginKind {
                OriginKind::Http
            }
            fn fetch(
                &self,
                hash: Hash,
                _max_bytes: u64,
            ) -> std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = Result<OriginFetch, OriginPullError>>
                        + Send
                        + '_,
                >,
            > {
                let result = if hash == self.hash {
                    Ok(OriginFetch::found_one_shot(self.data.clone()))
                } else {
                    Ok(OriginFetch::NotFound)
                };
                Box::pin(async move { result })
            }
        }

        let payload = b"hello /metrics integration".to_vec();
        let hash = Hash::new(&payload);
        let stub = StubOrigin {
            data: Bytes::from(payload.clone()),
            hash,
        };

        let metrics = Arc::new(Metrics::new());
        let cache_handle = metrics.cache_metrics();
        let tmp = tempfile::tempdir().unwrap();
        let engine = CacheEngine::open_full(
            tmp.path(),
            vec![Arc::new(stub) as Arc<dyn decdn_cache::Origin>],
            10,
            PinnedHashes::empty(),
            RetryPolicy::default(),
            decdn_cache::CircuitBreakerPolicy::default(),
            Some(Arc::clone(&cache_handle)),
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();

        // 1 miss (pull-through) + 1 hit.
        let _ = engine.get(hash).await.unwrap();
        let _ = engine.get(hash).await.unwrap();

        let text = metrics.encode().unwrap();
        let payload_len = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        for (name, expected) in [
            ("decdn_cache_hits_total", 1u64),
            ("decdn_cache_misses_total", 1),
            ("decdn_cache_pull_through_bytes_total", payload_len),
            ("decdn_cache_bytes_returned_total", payload_len * 2),
        ] {
            assert!(
                has_metric_line(&text, name, expected),
                "counter {name} should report {expected} after 1 miss + 1 hit:\n{text}"
            );
        }
    }

    /// `bind` accepts both loopback and non-loopback addresses (the
    /// non-loopback path emits a `WARN` per #579 but does not reject).
    /// We can't easily intercept the tracing emission without a
    /// dedicated capture subscriber, so this is a smoke test of both
    /// branches plus IPv6 loopback — a future refactor that narrowed
    /// the predicate to e.g. `addr.ip() == Ipv4Addr::LOCALHOST` would
    /// regress on `::1` and break here visibly.
    #[tokio::test]
    async fn bind_accepts_loopback_and_warns_on_non_loopback() {
        use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

        // IPv4 loopback: warn-free.
        let v4_loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = bind(v4_loopback).unwrap();
        let bound = listener.local_addr().unwrap();
        assert!(
            bound.ip().is_loopback(),
            "IPv4 loopback bind should resolve to a loopback addr: got {bound}"
        );
        drop(listener);

        // IPv6 loopback `::1`: also warn-free. Some hosts disable
        // IPv6; skip rather than fail if the bind itself errors.
        let v6_loopback = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
        if let Ok(listener) = bind(v6_loopback) {
            let bound = listener.local_addr().unwrap();
            assert!(
                bound.ip().is_loopback(),
                "IPv6 loopback bind should resolve to a loopback addr: got {bound}"
            );
        }

        // Unspecified (`0.0.0.0`): allowed, but the bind path WARNs.
        // Bind succeeds (a regression that rejected unspecified
        // would surface as a `bind` error here). `local_addr()`
        // echoes the requested IP so `is_unspecified()` is the
        // direct post-bind assertion.
        let unspecified = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
        let listener = bind(unspecified).unwrap();
        let bound = listener.local_addr().unwrap();
        assert!(
            bound.ip().is_unspecified(),
            "0.0.0.0 bind should resolve to the unspecified addr: got {bound}"
        );
        drop(listener);
    }

    #[test]
    fn cache_metrics_handle_shares_atomic_with_registered_group() {
        // Sanity: the Arc<CacheMetrics> handed to the engine must be
        // the same one the registry reads from at scrape time. A bug
        // that built two Arcs would surface as cache bumps never
        // appearing in the scrape output.
        let metrics = Metrics::new();
        let handle = metrics.cache_metrics();
        handle.origin_fetches.inc();
        handle.origin_retry_exhausted.inc();
        handle.origin_fallback.inc();
        handle.hits.inc();
        handle.misses.inc();
        handle.bytes_returned.inc_by(1024);
        handle.pull_through_bytes.inc_by(2048);
        // GC counters (#518). The struct fields are `gc_runs` /
        // `gc_bytes_reclaimed`; bumping them here and asserting the
        // `..._total`-suffixed exported names round-trip locks in the
        // encoder behavior that motivated the field-name shape.
        handle.gc_runs.inc();
        handle.gc_bytes_reclaimed.inc_by(4096);
        let text = metrics.encode().unwrap();
        for (name, expected) in [
            ("decdn_cache_origin_fetches_total", 1u64),
            ("decdn_cache_origin_retry_exhausted_total", 1),
            ("decdn_cache_hits_total", 1),
            ("decdn_cache_misses_total", 1),
            ("decdn_cache_bytes_returned_total", 1024),
            ("decdn_cache_pull_through_bytes_total", 2048),
            ("decdn_cache_gc_runs_total", 1),
            ("decdn_cache_gc_bytes_reclaimed_total", 4096),
        ] {
            assert!(
                has_metric_line(&text, name, expected),
                "counter {name} should report {expected}:\n{text}"
            );
        }
    }

    #[test]
    fn pool_open_failures_by_reason_label_distinct_counters() {
        // The three buyer `openChannel` failure classes (#966) must each land
        // in their own `decdn_pool_open_failures_{reason}_total` sibling
        // counter — that label split is the whole point of the issue, so a
        // bump on one reason must NOT leak into another.
        let metrics = Metrics::new();

        // Fresh registry: every reason exposed at zero so dashboards don't
        // render `(no data)` before the first failure.
        let text = metrics.encode().unwrap();
        for name in [
            "decdn_pool_open_failures_insufficient_deposit_total",
            "decdn_pool_open_failures_contract_revert_total",
            "decdn_pool_open_failures_rpc_error_total",
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "reason counter {name} should start at zero:\n{text}"
            );
        }

        // Bump each reason a distinct number of times so a cross-wired counter
        // is caught by the mismatched count, not just a nonzero value.
        metrics.pool_open_failure_by_reason(PoolOpenFailureReason::InsufficientDeposit);
        metrics.pool_open_failure_by_reason(PoolOpenFailureReason::InsufficientDeposit);
        metrics.pool_open_failure_by_reason(PoolOpenFailureReason::ContractRevert);
        metrics.pool_open_failure_by_reason(PoolOpenFailureReason::RpcError);
        metrics.pool_open_failure_by_reason(PoolOpenFailureReason::RpcError);
        metrics.pool_open_failure_by_reason(PoolOpenFailureReason::RpcError);

        let text = metrics.encode().unwrap();
        for (name, expected) in [
            ("decdn_pool_open_failures_insufficient_deposit_total", 2u64),
            ("decdn_pool_open_failures_contract_revert_total", 1),
            ("decdn_pool_open_failures_rpc_error_total", 3),
        ] {
            assert!(
                has_metric_line(&text, name, expected),
                "reason counter {name} should report {expected}:\n{text}"
            );
        }

        // The by-reason family is independent of the unlabeled total — bumping
        // a reason does NOT touch `node_pull_pool_open_failures` (that total
        // is bumped separately, and also covers non-tx causes).
        assert!(
            has_metric_line(&text, "decdn_node_pull_pool_open_failures_total", 0),
            "unlabeled total must not move when only the by-reason helper is called:\n{text}"
        );
    }
}
