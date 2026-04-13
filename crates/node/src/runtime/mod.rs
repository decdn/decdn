//! Node runtime: owns the iroh endpoint, metrics server, and handler dispatch.

pub mod dispatch;
pub mod endpoint;

use std::sync::Arc;

use anyhow::Context;
use tokio::task::JoinSet;

use crate::config::ResolvedConfig;
use crate::handlers::{Handler, probe::ProbeHandler};
use crate::{identity, metrics};

/// Build the endpoint, register handlers, spawn the metrics server, and run
/// until a shutdown signal (SIGINT / SIGTERM) is received.
#[allow(clippy::cognitive_complexity)] // Startup wiring reads top-to-bottom; splitting hurts clarity.
pub async fn run(cfg: ResolvedConfig) -> anyhow::Result<()> {
    let metrics = Arc::new(metrics::Metrics::new()?);
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

    let mut tasks = JoinSet::new();

    let metrics_addr = std::net::SocketAddr::from(([0, 0, 0, 0], cfg.metrics_port));
    let metrics_handle = Arc::clone(&metrics);
    tasks.spawn(async move {
        if let Err(err) = metrics::serve(metrics_addr, metrics_handle).await {
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

    ep.close().await;
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}

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
