//! Node runtime: owns the iroh endpoint, metrics server, and handler dispatch.

pub mod dispatch;
pub mod endpoint;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::oneshot;
use tokio::task::JoinSet;

use crate::config::ResolvedConfig;
use crate::handlers::{Handler, probe::ProbeHandler};
use crate::{identity, metrics};

/// Ceiling on how long we wait for spawned tasks to drain after the endpoint
/// and metrics server have been signalled to stop. Sized comfortably larger
/// than `HANDSHAKE_TIMEOUT` (5s) + `PROBE_CLOSE_TIMEOUT` (3s) so handlers
/// finish naturally; `abort_all` only fires as a safety net.
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(15);

/// Build the endpoint, register handlers, spawn the metrics server, and run
/// until a shutdown signal (SIGINT / SIGTERM) is received.
#[allow(clippy::cognitive_complexity)] // Startup wiring reads top-to-bottom; splitting hurts clarity.
pub async fn run(cfg: ResolvedConfig) -> anyhow::Result<()> {
    let metrics = Arc::new(metrics::Metrics::new());
    metrics.started();

    let secret_key = identity::load_or_generate(&cfg.data_dir)?;
    tracing::info!(node_id = %secret_key.public(), "loaded node identity");

    let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(ProbeHandler::new(
        secret_key.public(),
        cfg.rate_per_mb,
        Arc::clone(&metrics),
    ))];

    let ep = endpoint::build(&secret_key, cfg.bind_port, &handlers)
        .await
        .context("failed to build iroh endpoint")?;

    metrics
        .register_iroh_endpoint(&ep)
        .context("failed to register iroh metrics")?;

    // Bind metrics to loopback by default: /metrics is an unauthenticated HTTP
    // endpoint that leaks operational data. Operators who want to scrape from
    // another host should front it with a reverse proxy or run node_exporter
    // alongside. ADR 020 leaves the bind address operator-configurable; exposing
    // that as a CLI flag is tracked as a follow-up.
    //
    // Bind *synchronously* so a port-in-use or permissions failure aborts
    // startup via `?` rather than silently leaving the node without /metrics.
    let metrics_addr = std::net::SocketAddr::from(([127, 0, 0, 1], cfg.metrics_port));
    let metrics_listener = metrics::bind(metrics_addr)
        .await
        .context("failed to bind metrics listener")?;

    let mut tasks = JoinSet::new();
    let (metrics_stop_tx, metrics_stop_rx) = oneshot::channel::<()>();

    let metrics_handle = Arc::clone(&metrics);
    tasks.spawn(async move {
        if let Err(err) = metrics::serve(metrics_listener, metrics_handle, metrics_stop_rx).await {
            tracing::error!(%err, "metrics server exited with error");
        }
    });

    let dispatch_ep = ep.clone();
    let dispatch_metrics = Arc::clone(&metrics);
    tasks.spawn(async move {
        dispatch::run(dispatch_ep, handlers, dispatch_metrics).await;
    });

    tracing::info!(
        bind_port = cfg.bind_port,
        metrics_port = cfg.metrics_port,
        "node runtime ready"
    );

    shutdown_signal().await;
    tracing::info!("shutdown signal received; closing endpoint");

    // Stop accepting new work. `ep.close()` delivers CONNECTION_CLOSE to each
    // active peer; the dispatch task's accept loop then falls through and its
    // internal JoinSet drain waits on per-connection handlers to finish
    // (bounded by their own timeouts). The oneshot unblocks the metrics
    // accept loop.
    ep.close().await;
    let _ = metrics_stop_tx.send(());

    let drain = async { while tasks.join_next().await.is_some() {} };
    if tokio::time::timeout(SHUTDOWN_DEADLINE, drain).await.is_ok() {
        tracing::info!("graceful shutdown complete");
    } else {
        tracing::warn!(
            deadline = ?SHUTDOWN_DEADLINE,
            "graceful shutdown timed out; aborting remaining tasks",
        );
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    tracing::info!("node stopped");
    Ok(())
}

/// Wait for either SIGINT or (on Unix) SIGTERM.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(err) => {
                tracing::warn!(%err, "failed to install SIGTERM handler; falling back to SIGINT only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
