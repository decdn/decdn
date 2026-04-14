//! Node runtime: owns the iroh endpoint, metrics server, and handler dispatch.

pub mod dispatch;
pub mod endpoint;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use decdn_cache::{CacheEngine, HttpOrigin, Origin};
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

    let secret_key = identity::load_or_generate(&cfg.identity.data_dir)?;
    tracing::info!(node_id = %secret_key.public(), "loaded node identity");

    let cache = build_cache(&cfg).await?;
    tracing::info!(
        cache_dir = %cfg.cache.cache_dir.display(),
        has_origin = cfg.cache.origin_url.is_some(),
        "cache engine ready",
    );

    let handlers: Vec<Arc<dyn Handler>> = vec![Arc::new(ProbeHandler::new(
        secret_key.public(),
        cfg.payment.rate_per_mb,
        Arc::clone(&metrics),
    ))];

    let ep = endpoint::build(&secret_key, cfg.network.bind_port, &handlers)
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
    let metrics_addr = std::net::SocketAddr::from(([127, 0, 0, 1], cfg.observability.metrics_port));
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
        bind_port = cfg.network.bind_port,
        metrics_port = cfg.observability.metrics_port,
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

    // Flush the cache store before the drain deadline so in-flight writes
    // hit disk. Intentionally *not* gated by `SHUTDOWN_DEADLINE`: a slow
    // flush is preferable to a lost write, and the watchdog at the outer
    // process level catches a truly stuck shutdown.
    if let Err(err) = cache.shutdown().await {
        tracing::warn!(%err, "cache shutdown failed");
    }

    let drain = async {
        while let Some(result) = tasks.join_next().await {
            log_join_result(result, "shutdown");
        }
    };
    if tokio::time::timeout(SHUTDOWN_DEADLINE, drain).await.is_ok() {
        tracing::info!("graceful shutdown complete");
    } else {
        tracing::warn!(
            deadline = ?SHUTDOWN_DEADLINE,
            "graceful shutdown timed out; aborting remaining tasks",
        );
        tasks.abort_all();
        while let Some(result) = tasks.join_next().await {
            log_join_result(result, "abort");
        }
    }

    tracing::info!("node stopped");
    Ok(())
}

/// Log a `JoinError` from a shutdown-drained task with a phase label so a
/// panicked or cancelled spawned task (dispatch, metrics) is visible rather
/// than silently swallowed.
fn log_join_result(result: Result<(), tokio::task::JoinError>, phase: &'static str) {
    if let Err(err) = result {
        if err.is_cancelled() {
            tracing::debug!(phase, "task cancelled during shutdown");
        } else {
            tracing::warn!(phase, %err, "task failed during shutdown");
        }
    }
}

/// Construct the cache engine from resolved config. The `HttpOrigin` is
/// only built when `origin_url` is set; otherwise the engine serves only
/// already-cached content and cache misses surface as `CacheError::NoOrigin`.
async fn build_cache(cfg: &ResolvedConfig) -> anyhow::Result<CacheEngine> {
    let origin: Option<Arc<dyn Origin>> = cfg
        .cache
        .origin_url
        .as_deref()
        .map(|url| -> anyhow::Result<Arc<dyn Origin>> {
            Ok(Arc::new(
                HttpOrigin::new(url).context("invalid cache.origin_url")?,
            ))
        })
        .transpose()?;
    CacheEngine::open(&cfg.cache.cache_dir, origin, cfg.cache.max_blob_size_mb)
        .await
        .context("failed to open cache engine")
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
