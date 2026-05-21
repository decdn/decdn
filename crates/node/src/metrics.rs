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
    /// `cdn/dht/v1` request handling failed after the request was admitted
    /// by the rate limiter — frame decode error, response write error,
    /// read timeout, etc. Tracked separately from the rate-limit
    /// rejections so an operator running with default `RUST_LOG=info` can
    /// see the rate of "I accepted this request and then it broke"
    /// failures without scraping debug-level logs. Operator-visible name:
    /// `decdn_dht_requests_failed_total`.
    pub dht_requests_failed: Counter,
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

    /// Record a `cdn/dht/v1` request that was admitted by the rate
    /// limiter but failed after that (frame decode, write, encode,
    /// timeout, etc).
    pub fn dht_request_failed(&self) {
        self.decdn.dht_requests_failed.inc();
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
