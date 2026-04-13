//! Prometheus metrics and a minimal `/metrics` HTTP server.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use http_body_util::Full;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prometheus::{Encoder, IntCounter, IntGauge, Registry, TextEncoder};
use tokio::net::TcpListener;

/// Aggregated deCDN node metrics.
pub struct Metrics {
    registry: Registry,
    probe_requests_total: IntCounter,
    active_connections: IntGauge,
    started_at: Instant,
    uptime_seconds: IntGauge,
}

impl Metrics {
    /// Create and register all metrics.
    ///
    /// # Errors
    ///
    /// Returns an error if the Prometheus registry rejects a metric registration.
    pub fn new() -> anyhow::Result<Self> {
        let registry = Registry::new();

        let probe_requests_total =
            IntCounter::new("decdn_probe_requests_total", "Total probe requests served")
                .map_err(|e| anyhow::anyhow!("counter construction failed: {e}"))?;
        registry
            .register(Box::new(probe_requests_total.clone()))
            .map_err(|e| anyhow::anyhow!("register probe counter: {e}"))?;

        let active_connections = IntGauge::new(
            "decdn_active_connections",
            "Currently open QUIC connections",
        )
        .map_err(|e| anyhow::anyhow!("gauge construction failed: {e}"))?;
        registry
            .register(Box::new(active_connections.clone()))
            .map_err(|e| anyhow::anyhow!("register active_connections: {e}"))?;

        let uptime_seconds = IntGauge::new("decdn_uptime_seconds", "Seconds since node start")
            .map_err(|e| anyhow::anyhow!("gauge construction failed: {e}"))?;
        registry
            .register(Box::new(uptime_seconds.clone()))
            .map_err(|e| anyhow::anyhow!("register uptime: {e}"))?;

        Ok(Self {
            registry,
            probe_requests_total,
            active_connections,
            started_at: Instant::now(),
            uptime_seconds,
        })
    }

    pub fn started(&self) {
        self.uptime_seconds.set(0);
    }

    pub fn probe_request(&self) {
        self.probe_requests_total.inc();
    }

    pub fn connection_opened(&self) {
        self.active_connections.inc();
    }

    pub fn connection_closed(&self) {
        self.active_connections.dec();
    }

    fn encode(&self) -> anyhow::Result<Vec<u8>> {
        let uptime = i64::try_from(self.started_at.elapsed().as_secs()).unwrap_or(i64::MAX);
        self.uptime_seconds.set(uptime);

        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buf = Vec::new();
        encoder
            .encode(&metric_families, &mut buf)
            .map_err(|e| anyhow::anyhow!("prometheus encode failed: {e}"))?;
        Ok(buf)
    }
}

/// Serve `/metrics` over HTTP on `addr` until the listener errors.
pub async fn serve(addr: SocketAddr, metrics: Arc<Metrics>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("metrics bind {addr} failed: {e}"))?;
    tracing::info!(%addr, "metrics server listening");

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                tracing::warn!(%err, "metrics accept failed");
                continue;
            }
        };
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
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
            .header("content-type", "text/plain; version=0.0.4")
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
