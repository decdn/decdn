//! OpenMetrics/Prometheus metrics and a minimal `/metrics` HTTP server.
//!
//! Metrics live in an [`iroh_metrics::Registry`] so we can surface both our
//! `decdn_*` counters and iroh's own transport metrics through a single
//! endpoint. Output is `OpenMetrics` text, which Prometheus scrapers accept.

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
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
}

/// Aggregated deCDN node metrics.
#[derive(Debug)]
pub struct Metrics {
    registry: Arc<RwLock<Registry>>,
    decdn: Arc<DecdnMetrics>,
    cache: Arc<CacheMetrics>,
    started_at: Instant,
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

    /// Read the current value of the `rpc_healthy` gauge. Test-only —
    /// production code should rely on the `OpenMetrics` endpoint rather
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

    pub(crate) fn encode(&self) -> anyhow::Result<String> {
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
/// # Errors
///
/// Returns an error if the `TcpListener::bind` call fails (port in use,
/// permissions, etc.).
pub async fn bind(addr: SocketAddr) -> anyhow::Result<TcpListener> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("metrics bind {addr} failed: {e}"))?;
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
            "decdn_cache_hits_total",
            "decdn_cache_misses_total",
            "decdn_cache_bytes_returned_total",
            "decdn_cache_pull_through_bytes_total",
        ] {
            assert!(
                has_metric_line(&text, name, 0),
                "counter {name} should be exposed at zero on a fresh registry:\n{text}"
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
            Some(Arc::new(stub)),
            10,
            PinnedHashes::empty(),
            RetryPolicy::default(),
            Some(Arc::clone(&cache_handle)),
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
        handle.hits.inc();
        handle.misses.inc();
        handle.bytes_returned.inc_by(1024);
        handle.pull_through_bytes.inc_by(2048);
        let text = metrics.encode().unwrap();
        for (name, expected) in [
            ("decdn_cache_origin_fetches_total", 1u64),
            ("decdn_cache_origin_retry_exhausted_total", 1),
            ("decdn_cache_hits_total", 1),
            ("decdn_cache_misses_total", 1),
            ("decdn_cache_bytes_returned_total", 1024),
            ("decdn_cache_pull_through_bytes_total", 2048),
        ] {
            assert!(
                has_metric_line(&text, name, expected),
                "counter {name} should report {expected}:\n{text}"
            );
        }
    }
}
