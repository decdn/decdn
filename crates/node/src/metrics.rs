//! OpenMetrics/Prometheus metrics and a minimal `/metrics` HTTP server.
//!
//! Metrics live in an [`iroh_metrics::Registry`] so we can surface both our
//! `decdn_*` counters and iroh's own transport metrics through a single
//! endpoint. Output is `OpenMetrics` text, which Prometheus scrapers accept.

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use bytes::Bytes;
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
    /// Origin pull-through retries fired (#285). Each transient failure
    /// that the cache engine retries bumps this once. Operator-visible
    /// name: `decdn_cache_origin_retry_attempts_total`.
    pub cache_origin_retry_attempts: Counter,
    /// Origin fetches that succeeded only after at least one retry
    /// (#285). The "resilience delivered" counter — operators tuning
    /// `cache.origin_retry.max_retries` watch this to size the budget.
    /// Operator-visible name: `decdn_cache_origin_retry_success_after_retry_total`.
    pub cache_origin_retry_success_after_retry: Counter,
    /// Origin fetches that gave up after exhausting `max_retries`
    /// (#285). High values mean the retry budget is too small or the
    /// origin is genuinely down — distinct from `cache_origin_retry_attempts`
    /// because operators care about the *terminal* failure rate.
    /// Operator-visible name: `decdn_cache_origin_retry_exhausted_total`.
    pub cache_origin_retry_exhausted: Counter,
    /// Cumulative milliseconds slept across all origin-retry backoffs
    /// (#285). Combined with `cache_origin_retry_attempts` this gives
    /// average backoff per retry. Operator-visible name:
    /// `decdn_cache_origin_retry_sleep_ms_total`.
    pub cache_origin_retry_sleep_ms: Counter,
}

/// Aggregated deCDN node metrics.
#[derive(Debug)]
pub struct Metrics {
    registry: Arc<RwLock<Registry>>,
    decdn: Arc<DecdnMetrics>,
    started_at: Instant,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create the registry and register deCDN's metric group.
    pub fn new() -> Self {
        let decdn = Arc::new(DecdnMetrics::default());
        let mut registry = Registry::default();
        registry.register(decdn.clone() as Arc<dyn MetricsGroup>);
        Self {
            registry: Arc::new(RwLock::new(registry)),
            decdn,
            started_at: Instant::now(),
        }
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

    /// Record one origin-retry attempt (#285). Called when the cache
    /// engine's retry loop fires a retry after a transient failure;
    /// `sleep_ms` is the upcoming backoff sleep. Bumps both the attempt
    /// counter and the cumulative sleep counter so operators can compute
    /// average backoff per retry.
    pub fn cache_retry_attempt(&self, sleep_ms: u64) {
        self.decdn.cache_origin_retry_attempts.inc();
        self.decdn.cache_origin_retry_sleep_ms.inc_by(sleep_ms);
    }

    /// Record an origin fetch that succeeded after at least one retry (#285).
    pub fn cache_retry_success_after_retry(&self) {
        self.decdn.cache_origin_retry_success_after_retry.inc();
    }

    /// Record an origin fetch that gave up after exhausting `max_retries`
    /// (#285).
    pub fn cache_retry_exhausted(&self) {
        self.decdn.cache_origin_retry_exhausted.inc();
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

/// Plug the [`Metrics`] handle into [`decdn_cache::CacheEngine`]'s retry
/// loop (#285). The cache crate stays metrics-free; this adapter sits at
/// the node-runtime seam and bumps the right counter for each
/// [`decdn_cache::RetryOutcome`] variant.
///
/// Construct via [`Self::new`]; the inner [`Arc<Metrics>`] is private so
/// callers handed an observer for retry-tracking can't pull the metrics
/// handle back out.
#[derive(Debug)]
pub struct MetricsRetryObserver {
    metrics: Arc<Metrics>,
}

impl MetricsRetryObserver {
    /// Wrap a [`Metrics`] handle so the cache engine can use it as a
    /// [`decdn_cache::RetryObserver`].
    pub const fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

impl decdn_cache::RetryObserver for MetricsRetryObserver {
    fn observe(&self, outcome: decdn_cache::RetryOutcome, attempt: u32, sleep_ms: u64) {
        match outcome {
            // The retry loop emits TransientRetry *before* sleeping, so
            // the `sleep_ms` is the upcoming backoff. Bump attempts +
            // accumulate the sleep here.
            decdn_cache::RetryOutcome::TransientRetry => {
                self.metrics.cache_retry_attempt(sleep_ms);
            }
            decdn_cache::RetryOutcome::SuccessAfterRetry => {
                self.metrics.cache_retry_success_after_retry();
            }
            // ExhaustedTransient with `attempt == 0` is the
            // disabled-policy case (`max_retries = 0`): the operator
            // opted out of retry, so a single transient failure is not
            // a "retry budget burned through" event — it's just a
            // failure. Bumping `cache_retry_exhausted` for it would
            // make the counter mean "transient failure" (already
            // covered by future origin-error breakdown counters) and
            // ruin alerts that page on actual exhaustion. Only count
            // exhaustion when at least one retry actually fired.
            decdn_cache::RetryOutcome::ExhaustedTransient if attempt > 0 => {
                self.metrics.cache_retry_exhausted();
            }
            // No-op arms collapsed: all three reasons we don't bump a
            // counter share the same body, so clippy folds them into a
            // single wildcard. Listed in the comment for reviewers:
            //
            // - `ExhaustedTransient` with `attempt == 0`: disabled-policy
            //   case (`max_retries = 0`). Operator opted out of retry,
            //   so a single transient failure is not a "retry budget
            //   burned through" event — it's just a failure. Bumping
            //   `cache_retry_exhausted` here would make the counter
            //   mean "transient failure" and ruin alerts that page on
            //   actual exhaustion.
            // - `Success` (first-try) and `Permanent`: not operationally
            //   interesting at the retry-resilience layer. A future
            //   origin-error breakdown counter will subsume Permanent;
            //   first-try Success is the dominant path and a counter
            //   for it would add noise without information.
            // - `_`: `RetryOutcome` is `#[non_exhaustive]`; future
            //   variants no-op until this match is updated.
            _ => {}
        }
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
    use decdn_cache::RetryObserver as _;

    #[test]
    fn metrics_retry_observer_records_each_outcome() {
        // Drive every RetryOutcome variant through the observer once,
        // then assert each counter ended in the expected state by
        // scraping the OpenMetrics text. Regression guard against an
        // accidental rewire (e.g. swapping Success and SuccessAfterRetry).
        let metrics = Arc::new(Metrics::new());
        let observer = MetricsRetryObserver::new(Arc::clone(&metrics));

        // 2 transient retries with cumulative sleep 100 + 200 ms
        observer.observe(decdn_cache::RetryOutcome::TransientRetry, 0, 100);
        observer.observe(decdn_cache::RetryOutcome::TransientRetry, 1, 200);
        // 1 success-after-retry
        observer.observe(decdn_cache::RetryOutcome::SuccessAfterRetry, 2, 0);
        // 1 exhausted on a separate fetch (attempt > 0 so it counts)
        observer.observe(decdn_cache::RetryOutcome::ExhaustedTransient, 3, 0);
        // Disabled-policy "exhaustion" with `attempt == 0` must NOT bump
        // the counter — the operator opted out of retry, so it isn't a
        // budget-burn event.
        observer.observe(decdn_cache::RetryOutcome::ExhaustedTransient, 0, 0);
        // Success and Permanent should NOT bump anything in the retry
        // counter family.
        observer.observe(decdn_cache::RetryOutcome::Success, 0, 0);
        observer.observe(decdn_cache::RetryOutcome::Permanent, 0, 0);

        let text = metrics.encode().unwrap();
        // OpenMetrics encoder appends `_total` to counters; the
        // `decdn_` prefix comes from the MetricsGroup `name` attr.
        assert!(
            text.contains("decdn_cache_origin_retry_attempts_total 2"),
            "missing or wrong attempts counter:\n{text}"
        );
        assert!(
            text.contains("decdn_cache_origin_retry_success_after_retry_total 1"),
            "missing or wrong success_after_retry counter:\n{text}"
        );
        assert!(
            text.contains("decdn_cache_origin_retry_exhausted_total 1"),
            "missing or wrong exhausted counter:\n{text}"
        );
        assert!(
            text.contains("decdn_cache_origin_retry_sleep_ms_total 300"),
            "missing or wrong sleep-ms counter:\n{text}"
        );
    }

    #[test]
    fn metrics_retry_counters_start_at_zero() {
        // Pinning down the OpenMetrics shape — a fresh registry must
        // expose the four retry counters at zero so dashboards built
        // before any retry has fired don't render `(no data)`.
        let metrics = Metrics::new();
        let text = metrics.encode().unwrap();
        for name in [
            "decdn_cache_origin_retry_attempts_total",
            "decdn_cache_origin_retry_success_after_retry_total",
            "decdn_cache_origin_retry_exhausted_total",
            "decdn_cache_origin_retry_sleep_ms_total",
        ] {
            assert!(
                text.contains(&format!("{name} 0")),
                "counter {name} should be exposed at zero on a fresh registry:\n{text}"
            );
        }
    }
}
