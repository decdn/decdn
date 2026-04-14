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
use tokio::sync::Semaphore;

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
}

/// Aggregated deCDN node metrics.
pub struct Metrics {
    registry: Arc<RwLock<Registry>>,
    decdn: Arc<DecdnMetrics>,
    started_at: Instant,
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

    fn encode(&self) -> anyhow::Result<String> {
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

/// Serve `/metrics` over HTTP on `addr` until the listener errors.
#[allow(clippy::cognitive_complexity)] // Accept+permit+spawn reads linearly.
pub async fn serve(addr: SocketAddr, metrics: Arc<Metrics>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("metrics bind {addr} failed: {e}"))?;
    tracing::info!(%addr, "metrics server listening");

    let limiter = Arc::new(Semaphore::new(MAX_METRICS_CONNECTIONS));

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                tracing::warn!(%err, "metrics accept failed");
                continue;
            }
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
