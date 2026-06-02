//! OpenMetrics/Prometheus metrics and a minimal `/metrics` HTTP server.
//!
//! Metrics live in an [`iroh_metrics::Registry`] so we can surface both our
//! `decdn_*` counters and iroh's own transport metrics through a single
//! endpoint. Output is `OpenMetrics` text, which Prometheus scrapers accept.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use bytes::Bytes;
use decdn_cache::CacheMetrics;
use http_body_util::Full;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use iroh::Endpoint;
use iroh_metrics::{Counter, Gauge, MetricsGroup, MetricsSource, Registry};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, oneshot};

/// Cap concurrent `/metrics` connections. Prevents a trivial `DoS` where a
/// peer opens many sockets to the operational-data endpoint and exhausts
/// tasks.
const MAX_METRICS_CONNECTIONS: usize = 32;

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
    /// Seconds since node start.
    pub uptime_seconds: Gauge,
    /// `NodeAnnounce` messages published to any gossip topic.
    pub gossip_announces_published_total: Counter,
    /// `NodeAnnounce`-bearing gossip envelopes received on any topic.
    pub gossip_announces_received_total: Counter,
    /// Incoming gossip envelopes rejected by validation (any reason).
    pub gossip_announces_rejected_total: Counter,
    /// Current peer-table size.
    pub gossip_peer_table_size: Gauge,
    /// Successful subscriber reconnections after a stream drop.
    pub gossip_subscriber_reconnections_total: Counter,
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
    /// QUIC 0-RTT connection attempts on `cdn/probe/v1` — a cached
    /// session ticket existed and early data was sent (ADR 015
    /// §Observability). Operator-visible name:
    /// `decdn_quic_0rtt_attempts_total`.
    pub quic_0rtt_attempts: Counter,
    /// 0-RTT attempts the server accepted (early data processed without a
    /// full handshake). Operator-visible name:
    /// `decdn_quic_0rtt_accepted_total`.
    pub quic_0rtt_accepted: Counter,
    /// 0-RTT attempts the server rejected; the client fell back to a
    /// 1-RTT handshake and re-sent the request. Operator-visible name:
    /// `decdn_quic_0rtt_rejected_total`.
    pub quic_0rtt_rejected: Counter,
    /// Approximate 0-RTT working-set size (ADR 015 §Observability).
    /// rustls owns the real session stores and exposes no size API, so
    /// this is a *proxy*: the number of distinct remote endpoints that
    /// completed a probe handshake on the 0-RTT-enabled server path —
    /// cold clients included, since the server still issues a
    /// `NewSessionTicket` to each. It is **not** a mirror of any specific
    /// rustls cache: server-side resumption state lives in rustls's
    /// internal, default-sized server store (iroh's `max_tls_tickets`
    /// knob sizes only the *client* `ClientSessionMemoryCache`). The
    /// value saturates at `SESSION_TICKET_CACHE_CEILING` because the
    /// backing set is bounded there for memory safety, not because it
    /// tracks a cache of that size — close enough at deployment scales
    /// where the cap is rarely hit organically.
    pub quic_session_ticket_cache_size: Gauge,
    /// Distinct *new* peers dropped from the tracking set because it hit
    /// `SESSION_TICKET_CACHE_CEILING`. Zero under organic load at
    /// expected deployment scales; a rising value means the
    /// unauthenticated probe handler is being fed many distinct node ids
    /// — i.e. it distinguishes a Sybil-style saturation from the gauge
    /// legitimately reaching the ceiling. Operator-visible name:
    /// `decdn_quic_session_ticket_peers_dropped_total`.
    pub quic_session_ticket_peers_dropped: Counter,
    /// `decdn_probe_hold_violations_total` per the canonical metric registry
    /// (`adr/appendix-observability.md` — the authoritative naming source,
    /// superseding informal ADR-005 references). The registry's alert
    /// remediation for this counter is "reduce load or increase
    /// `max_probe_holds`", i.e. it is the budget-pressure signal: this code
    /// increments it when the blob is present but the
    /// [`crate::handlers::probe`] hold could not be guaranteed (budget
    /// exhausted), so the node answers `has_blob: false`. That is an
    /// availability degradation, never a safety fault — the node loses
    /// revenue but never signs a phantom announcement (the hold mechanism
    /// makes the registry's literal "evicted after signing `has_blob:true`"
    /// case unreachable by construction, so this counter surfaces the
    /// budget-pressure cause the operator can actually act on).
    pub probe_hold_violations: Counter,
    /// `decdn_probe_holds_disabled_total` (#739): probes answered
    /// `has_blob: false` for a *present* blob because the eviction-hold path
    /// is **disabled by config** (`max_probe_holds == 0`), as opposed to
    /// genuine slot exhaustion. Split out from `probe_hold_violations` so an
    /// intentional operator disable does not trip that counter's "increase
    /// `max_probe_holds`" alert — a nonsensical remedy when holds are
    /// deliberately off. Field has no `_total` suffix because the
    /// `OpenMetrics` encoder appends it.
    pub probe_holds_disabled: Counter,
    /// `decdn_probe_stake_lane_reserved_total` (#757): an end-client probe
    /// (a requester that is *not* a registered operator) answered
    /// `has_blob: false` because the hold budget had reached the end-client
    /// ceiling (`max_probe_holds - cache.stake_lane_reserved_holds`),
    /// reserving the remaining slots for stake-lane (node-to-node
    /// cache-miss) probes per ADR 003 §Admission and Priority. Unlike the
    /// two counters above, this fires *before* `try_probe_hold`, so the
    /// cache is **not consulted** — the blob may or may not be present; the
    /// reservation is a content-independent admission decision. Distinct
    /// from `probe_hold_violations` (genuine exhaustion of the *whole*
    /// budget, checked after a confirmed-present blob) and
    /// `probe_holds_disabled` (`max_probe_holds == 0`): this is a deliberate
    /// priority decision, not budget pressure or a disable, so it must not
    /// trip either of those counters' alerts. Zero whenever the reservation
    /// is unconfigured (`stake_lane_reserved_holds == 0`). Field has no
    /// `_total` suffix because the `OpenMetrics` encoder appends it.
    pub probe_stake_lane_reserved: Counter,
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
    /// the existing `dispatch_rejected_*` convention since the metrics
    /// backend doesn't support per-field labels. Operator-visible name:
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
    /// Redemption attempts (`try_redeem`) that returned an error — a failed
    /// `getChannel`/`withdraw` RPC or receipt wait (#751). Each is otherwise
    /// only a single `warn!`; a sustained rate means accrued earnings are not
    /// being withdrawn and warrants investigating the RPC / wallet. Operator-
    /// visible name: `decdn_redemption_failures_total`.
    pub redemption_failures: Counter,
    /// Channel-lifecycle persists the settlement watcher swallowed: a failed
    /// `register_open_channel` / `update_channel_deposit` / `forget_channel`
    /// from the live event stream (#751). The watcher logs and continues (the
    /// channel stays observable on-chain and a later event or the bring-up
    /// backfill re-drives it), but a non-zero rate flags a struggling channel
    /// store. Operator-visible name: `decdn_watcher_persist_failures_total`.
    pub watcher_persist_failures: Counter,
    /// `decdn_staker_set_watcher_restarts_total` (#783): distinct drift
    /// windows the [`crate::dht::chain_staker_set`] watcher has entered —
    /// bumped once on the *transition* from a healthy cycle into the
    /// error/backoff state, NOT on every backoff iteration of one continuous
    /// outage. Each increment therefore brackets exactly one window in which
    /// the cached active-staker set can drift from chain state — the module's
    /// documented mid-run degradation. Because the stake-lane probe
    /// reservation (#757) and the DHT `Store` admission path both read that
    /// cached set, sustained restarts are revenue-impacting, not just a
    /// discovery-health blip. The paired `tracing::warn!` in `watcher_loop`
    /// carries the underlying error; this counter is the alertable rate. A
    /// clean stream end (filter expiry / provider rotation) is NOT an error
    /// and does not bump this. Field has no `_total` suffix because the
    /// `OpenMetrics` encoder appends it.
    pub staker_set_watcher_restarts: Counter,
    /// `decdn_staker_set_watcher_resolve_failures_total` (#788): times an
    /// operator-indexed event (`Reinstated` / `UnbondingRequested`) was
    /// dropped because the follow-up `nodeIdOf(operator)` RPC failed. A dropped
    /// resolution leaves the cached active set out of sync with chain state for
    /// that operator until a later event or the (future) resync path corrects
    /// it — exactly the silent drift these #783/#788 metrics exist to surface.
    /// Unlike a stream-level error, this does NOT trip a backoff/restart, so it
    /// would otherwise move no metric at all. Pairs with the per-failure
    /// `warn!` in `apply_operator_change`. Field has no `_total` suffix because
    /// the `OpenMetrics` encoder appends it.
    pub staker_set_watcher_resolve_failures: Counter,
    /// `decdn_staker_set_watcher_down_seconds` (#783, semantics corrected
    /// #788): true downtime — seconds the staker-set watcher has been in the
    /// error/backoff state with no established event filters. Reads `0` for the
    /// entire life of any established cycle, however long or quiet (a healthy
    /// filter stream persists indefinitely, so this must NOT measure cycle
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
}

/// Self-imposed cap on the distinct-peer tracking set (and hence the
/// `quic_session_ticket_cache_size` gauge). It is **not** a rustls cache
/// size — the server-side ticket store is rustls-internal and untouched
/// by iroh's `max_tls_tickets`. We reuse
/// [`decdn_protocol::SESSION_TICKET_CACHE_SIZE`] (the value that *does*
/// size the client-side `ClientSessionMemoryCache`) purely so the node's
/// 0-RTT memory budget is described by one number across client and
/// server roles.
const SESSION_TICKET_CACHE_CEILING: usize = decdn_protocol::SESSION_TICKET_CACHE_SIZE;

/// Aggregated deCDN node metrics.
#[derive(Debug)]
pub struct Metrics {
    registry: Arc<RwLock<Registry>>,
    decdn: Arc<DecdnMetrics>,
    cache: Arc<CacheMetrics>,
    started_at: Instant,
    /// Distinct remote endpoint ids with a completed 0-RTT-eligible
    /// handshake. Backs the approximate `quic_session_ticket_cache_size`
    /// gauge (rustls exposes no session-store size API). Bounded at
    /// `SESSION_TICKET_CACHE_CEILING` entries by `note_session_ticket_peer`
    /// — the insert path is fed by the unauthenticated probe handler, so
    /// the cap is what stops an unbounded-distinct-peer memory leak.
    session_ticket_peers: Mutex<HashSet<[u8; 32]>>,
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
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create the registry and register deCDN's metric group plus the
    /// cache crate's `decdn_cache_*` group. The cache handle is shared
    /// with the engine via [`Self::cache_metrics`] so engine-side bumps
    /// land in the same encoder output.
    pub fn new() -> Self {
        let decdn = Arc::new(DecdnMetrics::default());
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
            started_at: Instant::now(),
            session_ticket_peers: Mutex::new(HashSet::new()),
            staker_set_watcher_down_since: Mutex::new(None),
        }
    }

    /// Shared `Arc<CacheMetrics>` for wiring into [`decdn_cache::CacheEngine`].
    pub fn cache_metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.cache)
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

    pub fn started(&self) {
        self.decdn.uptime_seconds.set(0);
    }

    pub fn probe_request(&self) {
        self.decdn.probe_requests.inc();
    }

    /// A probe answered `has_blob: false` despite the bytes being present,
    /// because the eviction hold could not be guaranteed (ADR 005 §Hold
    /// budget).
    pub fn probe_hold_violation(&self) {
        self.decdn.probe_hold_violations.inc();
    }

    /// A probe answered `has_blob: false` for a present blob because the
    /// eviction-hold path is disabled by config (`max_probe_holds == 0`) —
    /// an intentional operator decision, not budget pressure (#739, ADR 005
    /// §Hold budget).
    pub fn probe_holds_disabled(&self) {
        self.decdn.probe_holds_disabled.inc();
    }

    /// An end-client probe answered `has_blob: false` because the hold
    /// budget reached the stake-lane-reserved end-client ceiling, before any
    /// cache lookup (#757, ADR 003 §Admission and Priority). A deliberate,
    /// content-independent priority decision — not budget pressure or a
    /// config disable.
    pub fn probe_stake_lane_reserved(&self) {
        self.decdn.probe_stake_lane_reserved.inc();
    }

    /// Publish the current count of active probe holds (ADR 005).
    pub fn probe_hold_slots(&self, used: usize) {
        self.decdn
            .probe_hold_slots_used
            .set(i64::try_from(used).unwrap_or(i64::MAX));
    }

    /// Publish the configured `max_probe_holds` budget (registry-mandatory
    /// `decdn_probe_hold_slots_max`). Called once at runtime bring-up.
    pub fn probe_hold_slots_max(&self, max: usize) {
        self.decdn
            .probe_hold_slots_max
            .set(i64::try_from(max).unwrap_or(i64::MAX));
    }

    /// The node clamped `rate_per_mb` to the configured delivery bounds
    /// before signing (ADR 005 §Rate bounds validation).
    pub fn rate_bounds_clamped(&self) {
        self.decdn.rate_bounds_clamp_events.inc();
    }

    /// An accepted voucher skipped one or more nonce values past
    /// `last_nonce + 1` (#747). Counted once per gapped voucher; the precise
    /// skip count rides the paired `tracing::warn!` in `apply_voucher`.
    pub fn voucher_nonce_gap(&self) {
        self.decdn.voucher_nonce_gaps.inc();
    }

    /// A redeem hint was dropped because the bounded advisory channel was full
    /// (`try_send` → `Full`, #751). Advisory, so a few drops are benign; a
    /// sustained rate means the redeemer is not keeping up with fan-out.
    pub fn redeem_hint_dropped(&self) {
        self.decdn.redeem_hints_dropped.inc();
    }

    /// A redemption attempt (`try_redeem`) failed with an RPC/receipt error
    /// (#751). Pairs with the `warn!` in `redeemer_loop`.
    pub fn redemption_failure(&self) {
        self.decdn.redemption_failures.inc();
    }

    /// The settlement watcher swallowed a channel-lifecycle persist failure
    /// (`register_open_channel` / `update_channel_deposit` / `forget_channel`,
    /// #751). Pairs with the per-site `warn!` in `run_watcher_once`.
    pub fn watcher_persist_failure(&self) {
        self.decdn.watcher_persist_failures.inc();
    }

    /// The staker-set watcher's event stream terminated with an error and the
    /// loop is about to back off (#783, [`crate::dht::chain_staker_set`]).
    /// Stamps `down_since` so `staker_set_watcher_down_seconds` begins to climb,
    /// and — on the *transition* from a healthy cycle into the error state
    /// (`down_since` was `None`) — bumps `staker_set_watcher_restarts_total`
    /// exactly once per drift window, rather than once per backoff iteration of
    /// one continuous outage. A poisoned lock is treated as "skip the update"
    /// rather than panicking (anti-panic policy); the gauge's poison fallback
    /// (`i64::MAX`) still keeps the alert tripped. Pairs with the per-error
    /// `warn!` in `watcher_loop`.
    pub fn staker_set_watcher_backoff_started(&self) {
        if let Ok(mut down_since) = self.staker_set_watcher_down_since.lock()
            && down_since.is_none()
        {
            // Edge into the error state: open a fresh drift window and count it.
            *down_since = Some(Instant::now());
            self.decdn.staker_set_watcher_restarts.inc();
        }
    }

    /// A `nodeIdOf(operator)` resolution for an operator-indexed event failed,
    /// dropping the membership change (#788, [`crate::dht::chain_staker_set`]).
    /// Bumps `staker_set_watcher_resolve_failures_total`. Pairs with the
    /// per-failure `warn!` in `apply_operator_change`.
    pub fn staker_set_watcher_resolve_failure(&self) {
        self.decdn.staker_set_watcher_resolve_failures.inc();
    }

    /// Mark the staker-set watcher's event-stream cycle as established (#783,
    /// downtime semantics #788): the filters were (re)opened and events can
    /// flow. Clears `down_since` to `None` so `staker_set_watcher_down_seconds`
    /// reads `0` for the entire life of this cycle, however long. A poisoned
    /// lock is treated as "skip the update" rather than panicking (anti-panic
    /// policy); the gauge then keeps climbing, which is the safe (alerting)
    /// direction.
    pub fn staker_set_watcher_cycle_established(&self) {
        if let Ok(mut down_since) = self.staker_set_watcher_down_since.lock() {
            *down_since = None;
        }
    }

    /// Publish the current cached active-staker set size (#783). Sampled on
    /// every membership change the watcher applies, so the gauge tracks the
    /// cached view — which under a watcher outage is exactly the (possibly
    /// stale) set that admission decisions read.
    pub fn staker_set_active_count(&self, count: usize) {
        self.decdn
            .staker_set_active_count
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }

    pub fn connection_opened(&self) {
        self.decdn.active_connections.inc();
    }

    pub fn connection_closed(&self) {
        self.decdn.active_connections.dec();
    }

    pub fn gossip_published(&self, _topic: &str) {
        self.decdn.gossip_announces_published_total.inc();
    }

    pub fn gossip_received(&self, _topic: &str) {
        self.decdn.gossip_announces_received_total.inc();
    }

    pub fn gossip_rejected(&self, _reason: &'static str) {
        self.decdn.gossip_announces_rejected_total.inc();
    }

    pub fn gossip_peer_table_size(&self, n: i64) {
        self.decdn.gossip_peer_table_size.set(n);
    }

    pub fn gossip_reconnected(&self, _topic: &str) {
        self.decdn.gossip_subscriber_reconnections_total.inc();
    }

    /// Set the RPC health gauge. `true` -> 1 (reachable), `false` -> 0
    /// (unreachable). Driven by the watchdog task spawned in
    /// `runtime::run`.
    pub fn rpc_healthy(&self, ok: bool) {
        self.decdn.rpc_healthy.set(i64::from(ok));
    }

    /// Record a connection rejected by the global concurrency semaphore.
    pub fn dispatch_rejected_global(&self) {
        self.decdn.dispatch_rejected_global.inc();
    }

    /// Record a connection rejected by the per-source rate limiter.
    pub fn dispatch_rejected_per_source(&self) {
        self.decdn.dispatch_rejected_per_source.inc();
    }

    /// Increment the in-flight dispatch permit gauge.
    pub fn dispatch_permit_acquired(&self) {
        self.decdn.dispatch_in_flight.inc();
    }

    /// Decrement the in-flight dispatch permit gauge.
    pub fn dispatch_permit_released(&self) {
        self.decdn.dispatch_in_flight.dec();
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

    /// Record a relay-only connection accepted while the per-source
    /// layer was enabled but no peer IP could be resolved at accept
    /// time.
    pub fn dispatch_per_source_skipped_no_addr(&self) {
        self.decdn.dispatch_per_source_skipped_no_addr.inc();
    }

    /// Record a `cdn/dht/v1` request rejected at the per-peer layer.
    pub fn dht_rate_limit_rejected_per_peer(&self) {
        self.decdn.dht_rate_limit_rejected_per_peer.inc();
    }

    /// Record a `cdn/dht/v1` request rejected at the per-IP layer.
    pub fn dht_rate_limit_rejected_per_ip(&self) {
        self.decdn.dht_rate_limit_rejected_per_ip.inc();
    }

    /// Record a `cdn/dht/v1` request rejected at the global layer.
    pub fn dht_rate_limit_rejected_global(&self) {
        self.decdn.dht_rate_limit_rejected_global.inc();
    }

    /// Record a `retain_recent` sweep of the per-IP DHT keyed-limiter
    /// map (#645).
    pub fn dht_rate_limit_prune_sweep_per_ip(&self) {
        self.decdn.dht_rate_limit_prune_sweeps_per_ip.inc();
    }

    /// Record a `retain_recent` sweep of the per-peer DHT keyed-limiter
    /// map (#645).
    pub fn dht_rate_limit_prune_sweep_per_peer(&self) {
        self.decdn.dht_rate_limit_prune_sweeps_per_peer.inc();
    }

    /// Set the per-IP DHT keyed-limiter tracked-size gauge (#645). The
    /// `try_from(...).unwrap_or(i64::MAX)` clamp matches the existing
    /// gauge-set pattern elsewhere in this module and stays within the
    /// workspace's anti-panic policy.
    pub fn dht_rate_limit_tracked_per_ip_set(&self, n: usize) {
        self.decdn
            .dht_rate_limit_tracked_per_ip
            .set(i64::try_from(n).unwrap_or(i64::MAX));
    }

    /// Set the per-peer DHT keyed-limiter tracked-size gauge (#645).
    pub fn dht_rate_limit_tracked_per_peer_set(&self, n: usize) {
        self.decdn
            .dht_rate_limit_tracked_per_peer
            .set(i64::try_from(n).unwrap_or(i64::MAX));
    }

    /// Record a `cdn/dht/v1` request that was admitted by the rate
    /// limiter but failed after that (frame decode, write, encode,
    /// timeout, etc).
    pub fn dht_request_failed(&self) {
        self.decdn.dht_requests_failed.inc();
    }

    /// Record a `Store` rejected by the `holder != authenticated NodeId`
    /// check (ADR 022 §STORE Flow line 140 — lying-holder attack).
    pub fn dht_store_rejected_holder_mismatch(&self) {
        self.decdn.dht_store_rejected_holder_mismatch.inc();
    }

    /// Record a `Store` rejected by the active-staker filter (ADR 022
    /// §STORE Flow line 140 — non-staked publisher).
    pub fn dht_store_rejected_non_staked(&self) {
        self.decdn.dht_store_rejected_non_staked.inc();
    }

    /// Record a `Store` rejected by the per-publisher quota (ADR 022
    /// §Content Records and TTL — 200-record hard cap).
    pub fn dht_store_rejected_quota(&self) {
        self.decdn.dht_store_rejected_quota.inc();
    }

    /// Record a `Store` admitted to the record store (newly inserted or
    /// refreshed).
    pub fn dht_store_accepted(&self) {
        self.decdn.dht_store_accepted.inc();
    }

    /// Record a `BatchStore` that reached per-hash admission (passed
    /// stage-1 rate limiting + the batch-level holder check, #648).
    pub fn dht_batch_store_received(&self) {
        self.decdn.dht_batch_store_received.inc();
    }

    /// Record `count` `BatchStore` hashes deferred (acked `false`)
    /// because the two-stage rate-limit budget ran out before reaching
    /// them (ADR 022 §Batch token accounting, #648).
    pub fn dht_batch_store_hashes_deferred_rate_limit(&self, count: u64) {
        self.decdn
            .dht_batch_store_hashes_deferred_rate_limit
            .inc_by(count);
    }

    /// Record a 0-RTT connection attempt (ADR 015): a cached session
    /// ticket existed and early data was sent.
    pub fn record_0rtt_attempt(&self) {
        self.decdn.quic_0rtt_attempts.inc();
    }

    /// Record that the server accepted a 0-RTT attempt.
    pub fn record_0rtt_accepted(&self) {
        self.decdn.quic_0rtt_accepted.inc();
    }

    /// Record that the server rejected a 0-RTT attempt and the client
    /// fell back to a 1-RTT handshake.
    pub fn record_0rtt_rejected(&self) {
        self.decdn.quic_0rtt_rejected.inc();
    }

    /// Note a remote endpoint with which a 0-RTT-eligible handshake
    /// completed, refreshing the approximate
    /// `quic_session_ticket_cache_size` gauge. Idempotent per peer; a
    /// poisoned lock is treated as "skip the update" rather than
    /// panicking (anti-panic policy).
    ///
    /// The tracking set is itself bounded at `SESSION_TICKET_CACHE_CEILING`,
    /// not just the gauge value: this is called from the *unauthenticated*
    /// probe handler, so a peer presenting many distinct node ids (cheap
    /// to generate) would otherwise grow the set without limit — a slow
    /// memory-exhaustion vector on untrusted input. Once the set is full
    /// new peers are no longer tracked (re-noting an already-tracked peer
    /// stays a no-op) and `quic_session_ticket_peers_dropped` is bumped so
    /// the saturation is distinguishable from organic growth; the gauge
    /// then sits at the ceiling. The ceiling is the node's own memory
    /// bound, not a rustls cache size (see `SESSION_TICKET_CACHE_CEILING`).
    pub fn note_session_ticket_peer(&self, remote_id: [u8; 32]) {
        let Ok(mut peers) = self.session_ticket_peers.lock() else {
            return;
        };
        if peers.len() < SESSION_TICKET_CACHE_CEILING {
            peers.insert(remote_id);
        } else if !peers.contains(&remote_id) {
            // Set is full AND this is a genuinely new peer: the memory
            // bound is engaging on (untrusted) input. Surface it so a
            // Sybil-style flood is distinguishable from organic
            // saturation. Re-noting an already-tracked peer is a
            // legitimate no-op and must NOT count as a drop, or the
            // counter becomes noise.
            self.decdn.quic_session_ticket_peers_dropped.inc();
        }
        let size = peers.len();
        self.decdn
            .quic_session_ticket_cache_size
            .set(i64::try_from(size).unwrap_or(i64::MAX));
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

    /// Render the registry as `OpenMetrics` text — exactly the body the
    /// public `/metrics` HTTP endpoint serves. `pub` (not `pub(crate)`)
    /// so integration tests in sibling crates can assert on the exported
    /// series without scraping over TCP; it exposes no data the
    /// unauthenticated `/metrics` endpoint doesn't already.
    pub fn encode(&self) -> anyhow::Result<String> {
        let uptime = i64::try_from(self.started_at.elapsed().as_secs()).unwrap_or(i64::MAX);
        self.decdn.uptime_seconds.set(uptime);

        // Recompute the staker-set watcher down-seconds gauge from `down_since`
        // (true downtime), mirroring `uptime_seconds`. `None` (healthy cycle,
        // including pre-bootstrap) reads `0` regardless of how long the cycle
        // has been live. A poisoned lock reports `i64::MAX` — the conservative,
        // alerting direction for a downtime gauge: reporting `0` would MASK an
        // in-progress outage. Once an error window opens the value climbs until
        // the next `staker_set_watcher_cycle_established` clears `down_since`.
        let down_seconds = match self.staker_set_watcher_down_since.lock() {
            Ok(down_since) => down_since
                .map(|t| t.elapsed().as_secs())
                .map_or(0, |s| i64::try_from(s).unwrap_or(i64::MAX)),
            Err(_) => i64::MAX,
        };
        self.decdn.staker_set_watcher_down_seconds.set(down_seconds);

        let reg = self
            .registry
            .read()
            .map_err(|_| anyhow::anyhow!("metrics registry lock poisoned"))?;
        reg.encode_openmetrics_to_string()
            .map_err(|e| anyhow::anyhow!("openmetrics encode failed: {e}"))
    }
}

/// Bind the `/metrics` HTTP listener synchronously so startup can fail fast
/// if the port is unavailable. The returned listener is consumed by [`serve`].
///
/// Emits a `WARN` if `addr` is non-loopback (#579). The `OpenMetrics`
/// surface exposes peer-table size, gossip rejection reasons,
/// pull-through byte volumes, GC/connection stats — useful
/// reconnaissance for anyone who can reach it. The default config
/// binds loopback (`appendix-local-admin-http` calls metrics
/// "loopback-only"), but `observability.metrics_bind` is operator-
/// settable to `0.0.0.0` for containerised deployments
/// (`crates/common/src/config/types.rs` — `ObservabilityConfig::metrics_bind`).
/// We warn but do not reject so that documented container workflows
/// keep working.
///
/// # Errors
///
/// Returns an error if the `TcpListener::bind` call fails (port in use,
/// permissions, etc.).
pub async fn bind(addr: SocketAddr) -> anyhow::Result<TcpListener> {
    let listener = TcpListener::bind(addr)
        .await
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
                 endpoint exposes peer-table size, gossip rejection reasons, pull-through \
                 byte volumes, and GC/connection stats — gate it behind a private network \
                 or reverse proxy if reachable from outside the host"
            );
        } else {
            tracing::warn!(
                %addr,
                "metrics server is binding a non-loopback address; the OpenMetrics \
                 endpoint exposes peer-table size, gossip rejection reasons, pull-through \
                 byte volumes, and GC/connection stats — restrict reachability to trusted \
                 scrapers"
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
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
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "counter {name} should be exposed at zero on a fresh registry:\n{text}"
            );
        }
    }

    #[test]
    fn quic_0rtt_metrics_start_at_zero_and_increment() {
        let metrics = Metrics::new();

        // Fresh registry: ADR 015 §Observability metrics exposed at zero
        // so dashboards don't render `(no data)` before the first probe.
        let text = metrics.encode().unwrap();
        for name in [
            "decdn_quic_0rtt_attempts_total",
            "decdn_quic_0rtt_accepted_total",
            "decdn_quic_0rtt_rejected_total",
            "decdn_quic_session_ticket_peers_dropped_total",
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "0-RTT counter {name} should start at zero:\n{text}"
            );
        }
        assert!(
            has_metric_line(&text, "decdn_quic_session_ticket_cache_size", 0),
            "session-ticket gauge should start at zero:\n{text}"
        );

        metrics.record_0rtt_attempt();
        metrics.record_0rtt_attempt();
        metrics.record_0rtt_accepted();
        metrics.record_0rtt_rejected();

        let text = metrics.encode().unwrap();
        assert!(has_metric_line(&text, "decdn_quic_0rtt_attempts_total", 2));
        assert!(has_metric_line(&text, "decdn_quic_0rtt_accepted_total", 1));
        assert!(has_metric_line(&text, "decdn_quic_0rtt_rejected_total", 1));
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
        // downtime, NOT cycle age. A healthy filter stream persists
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
            has_metric_line(&text, "decdn_uptime_seconds", 3600),
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
    fn session_ticket_gauge_counts_distinct_peers_and_is_idempotent() {
        let metrics = Metrics::new();

        metrics.note_session_ticket_peer([1u8; 32]);
        metrics.note_session_ticket_peer([2u8; 32]);
        // Re-noting the same peer must not double-count (the real rustls
        // cache holds one ticket entry per peer).
        metrics.note_session_ticket_peer([1u8; 32]);

        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_quic_session_ticket_cache_size", 2),
            "expected 2 distinct peers, got:\n{text}"
        );
    }

    #[test]
    fn session_ticket_set_is_bounded_against_unbounded_distinct_peers() {
        // Regression: the insert path is fed by the unauthenticated probe
        // handler, so the tracking set MUST stay bounded under a flood of
        // distinct node ids — not just the gauge value.
        let metrics = Metrics::new();
        for i in 0..(SESSION_TICKET_CACHE_CEILING + 50) {
            let mut id = [0u8; 32];
            let tag = u64::try_from(i).unwrap().to_le_bytes();
            id.iter_mut().zip(tag).for_each(|(dst, src)| *dst = src);
            metrics.note_session_ticket_peer(id);
        }

        let ceiling = u64::try_from(SESSION_TICKET_CACHE_CEILING).unwrap();
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, "decdn_quic_session_ticket_cache_size", ceiling),
            "gauge must saturate at the ceiling, got:\n{text}"
        );
        // The set itself stopped growing at the ceiling (the leak fix),
        // not merely the reported gauge.
        let len = metrics.session_ticket_peers.lock().unwrap().len();
        assert_eq!(len, SESSION_TICKET_CACHE_CEILING);
        // The 50 distinct peers beyond the ceiling were each counted as a
        // drop, so the saturation is observable (not silent).
        assert!(
            has_metric_line(&text, "decdn_quic_session_ticket_peers_dropped_total", 50),
            "expected 50 dropped peers, got:\n{text}"
        );
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
        let listener = bind(v4_loopback).await.unwrap();
        let bound = listener.local_addr().unwrap();
        assert!(
            bound.ip().is_loopback(),
            "IPv4 loopback bind should resolve to a loopback addr: got {bound}"
        );
        drop(listener);

        // IPv6 loopback `::1`: also warn-free. Some hosts disable
        // IPv6; skip rather than fail if the bind itself errors.
        let v6_loopback = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
        if let Ok(listener) = bind(v6_loopback).await {
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
        let listener = bind(unspecified).await.unwrap();
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
}
