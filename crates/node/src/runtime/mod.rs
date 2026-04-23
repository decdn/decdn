//! Node runtime: owns the iroh endpoint, metrics server, and protocol router.

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use decdn_cache::{CacheEngine, FilesystemOrigin, HttpOrigin, Origin};
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, SecretKey};
use iroh_gossip::ALPN as GOSSIP_ALPN;
use iroh_gossip::net::Gossip;
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinSet;

use decdn_gossip::{GossipMetrics, GossipRuntimeConfig, GossipService, PeerTable};

use crate::admin;
use crate::config::ResolvedConfig;
use crate::handlers::probe::ProbeHandler;
use crate::{identity, metrics};

/// Ceiling on how long we wait for spawned tasks to drain after the endpoint
/// and metrics server have been signalled to stop. Sized comfortably larger
/// than the sum of the probe handler's accept + close timeouts (see
/// `handlers::probe::ACCEPT_BI_TIMEOUT` + `PROBE_READ_TIMEOUT` +
/// `PROBE_CLOSE_TIMEOUT`) so in-flight handlers finish naturally; the
/// `abort_all` branch only fires as a safety net.
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(15);

/// Build the endpoint, register handlers on a `Router`, spawn the metrics
/// server and gossip tasks, and run until a shutdown signal is received.
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
pub async fn run(cfg: ResolvedConfig) -> anyhow::Result<()> {
    let node_metrics = Arc::new(metrics::Metrics::new());
    node_metrics.started();

    let secret_key = identity::load_or_generate(&cfg.identity.data_dir)?;
    tracing::info!(node_id = %secret_key.public(), "loaded node identity");

    let cache = build_cache(&cfg).await?;
    tracing::info!(
        cache_dir = %cfg.cache.cache_dir.display(),
        has_origin = cfg.cache.origin_url.is_some() || cfg.cache.origin_path.is_some(),
        "cache engine ready",
    );

    let ep = build_endpoint(&secret_key, cfg.network.bind_port)
        .await
        .context("failed to build iroh endpoint")?;

    node_metrics
        .register_iroh_endpoint(&ep)
        .context("failed to register iroh metrics")?;

    let gossip = Gossip::builder().spawn(ep.clone());

    let probe_handler = Arc::new(ProbeHandler::new(
        secret_key.public(),
        cfg.payment.rate_per_mb,
        Arc::clone(&node_metrics),
    ));

    let router = Router::builder(ep.clone())
        .accept(ProbeHandler::ALPN, probe_handler)
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    // Bind metrics to loopback: /metrics is unauthenticated HTTP and leaks
    // operational data. Operators who want to scrape from another host should
    // front it with a reverse proxy.
    let metrics_addr = std::net::SocketAddr::from(([127, 0, 0, 1], cfg.observability.metrics_port));
    let metrics_listener = metrics::bind(metrics_addr)
        .await
        .context("failed to bind metrics listener")?;

    let mut tasks = JoinSet::new();
    let (metrics_stop_tx, metrics_stop_rx) = oneshot::channel::<()>();

    let metrics_handle = Arc::clone(&node_metrics);
    tasks.spawn(async move {
        if let Err(err) = metrics::serve(metrics_listener, metrics_handle, metrics_stop_rx).await {
            tracing::error!(%err, "metrics server exited with error");
        }
    });

    let peer_table = Arc::new(RwLock::new(PeerTable::new(
        cfg.gossip.peer_ttl_sec.saturating_mul(1_000_000),
    )));

    // Admin HTTP surface (ADR 025). Bound before the gossip service spawns so
    // startup fails fast on a port collision rather than after side-effectful
    // subscriptions have registered.
    let admin_stop_tx = if let Some(admin_port) = cfg.observability.admin_port {
        let admin_addr = std::net::SocketAddr::from(([127, 0, 0, 1], admin_port));
        let admin_listener = admin::bind(admin_addr)
            .await
            .context("failed to bind admin listener")?;
        let (tx, rx) = oneshot::channel::<()>();
        let state = admin::AdminState::new(Arc::clone(&peer_table));
        tasks.spawn(async move {
            if let Err(err) = admin::serve(admin_listener, state, rx).await {
                tracing::error!(%err, "admin server exited with error");
            }
        });
        Some(tx)
    } else {
        tracing::info!("admin server disabled (observability.admin_port = 0)");
        None
    };

    let gossip_runtime_cfg = GossipRuntimeConfig {
        announce_interval_sec: cfg.gossip.announce_interval_sec,
        subscribe_global: cfg.gossip.subscribe_global,
        region: cfg.identity.region.clone(),
        allowlist: cfg.gossip.allowlist.iter().copied().collect::<HashSet<_>>(),
    };
    let gossip_metrics: Arc<dyn GossipMetrics> =
        Arc::new(NodeGossipMetrics::new(Arc::clone(&node_metrics)));
    // Keep the gossip JoinHandles outside the JoinSet: dropping a
    // JoinHandle *detaches* the task in tokio (it keeps running), so if we
    // only held wrappers inside `tasks` an `abort_all()` would cancel the
    // wrapper but leak the inner gossip loop. Storing the handles lets us
    // call `.abort()` on each explicitly during shutdown.
    //
    // TODO: GossipService should own its own shutdown (e.g. accept a
    // CancellationToken or expose `shutdown().await`) so the runtime
    // doesn't have to reach in with `.abort()`. Tracked for follow-up;
    // PoC keeps the parent-driven abort to stay minimal.
    let gossip_handles = GossipService::spawn(
        ep.clone(),
        secret_key.clone(),
        gossip.clone(),
        gossip_runtime_cfg,
        Arc::clone(&peer_table),
        gossip_metrics,
    )
    .await;

    // Startup banner (#274). One structured INFO event per restart lets
    // operators correlate log streams across a fleet and across restarts
    // without stitching multiple lines together. Field names are stable —
    // log aggregators key on them.
    tracing::info!(
        event = "startup_banner",
        node_id = %secret_key.public(),
        version = env!("CARGO_PKG_VERSION"),
        region = cfg.identity.region.as_deref().unwrap_or(""),
        bind_port = cfg.network.bind_port,
        metrics_port = cfg.observability.metrics_port,
        admin_port = ?cfg.observability.admin_port,
        rate_per_mb = cfg.payment.rate_per_mb,
        cache_dir = %cfg.cache.cache_dir.display(),
        has_origin = cfg.cache.origin_url.is_some() || cfg.cache.origin_path.is_some(),
        subscribe_global = cfg.gossip.subscribe_global,
        "node runtime ready"
    );

    let signal = shutdown_signal().await;
    tracing::info!(signal = %signal, "shutdown signal received; closing router");

    // Signal the HTTP accept loops to stop *before* awaiting
    // `router.shutdown()`. Router shutdown can block indefinitely if a
    // protocol handler is slow, and while it's blocked the metrics/admin
    // servers would otherwise keep accepting fresh loopback connections —
    // wasting the outer `SHUTDOWN_DEADLINE` budget and emitting misleading
    // "still serving" signals. The accept loops are cheap to unwind, so
    // stopping them first is strictly cleaner.
    if metrics_stop_tx.send(()).is_err() {
        // Receiver dropped → the metrics server task already exited on
        // its own. The spawn closure logs its own error on abnormal exit
        // (see `metrics server exited with error` above), so this branch
        // is purely informational: under a healthy shutdown we'd have
        // been the ones signaling it.
        tracing::warn!("metrics server exited before shutdown signal was sent");
    }
    if let Some(tx) = admin_stop_tx
        && tx.send(()).is_err()
    {
        tracing::warn!("admin server exited before shutdown signal was sent");
    }

    // Router::shutdown waits for ProtocolHandler::shutdown on each handler,
    // then closes the endpoint. After this returns we can safely abort
    // gossip's infinite loops (publisher / subscriber / TTL sweeper) so
    // the drain phase actually finishes rather than hitting the 15s
    // timeout every time.
    if let Err(err) = router.shutdown().await {
        tracing::warn!(%err, "router shutdown reported an error");
    }
    for handle in &gossip_handles {
        handle.abort();
    }

    // Flush the cache store before the drain deadline so in-flight writes
    // hit disk. Intentionally *not* gated by `SHUTDOWN_DEADLINE`: a slow
    // flush is preferable to a lost write, and the watchdog at the outer
    // process level catches a truly stuck shutdown. On failure we still
    // finish the task drain cleanly (so metrics/dispatch don't leak) and
    // then bubble the error out of `run()` — supervisors need a non-zero
    // exit to know the store may be inconsistent.
    let cache_shutdown_err = cache.shutdown().await.err();
    if let Some(err) = cache_shutdown_err.as_ref() {
        tracing::error!(%err, "cache shutdown failed; store state may be inconsistent");
    }

    let drain = async {
        while let Some(result) = tasks.join_next().await {
            log_join_result(result, "shutdown");
        }
        // Await aborted gossip tasks so the runtime doesn't return while
        // they're still unwinding. `abort()` then `await` resolves with
        // `JoinError::is_cancelled()`, which is the expected path and not
        // logged; panics in the gossip loops still surface as warnings.
        for handle in gossip_handles {
            if let Err(err) = handle.await
                && !err.is_cancelled()
            {
                tracing::warn!(%err, "gossip task panicked during shutdown");
            }
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
    match cache_shutdown_err {
        None => Ok(()),
        Some(err) => Err(anyhow::Error::from(err).context("cache shutdown failed")),
    }
}

/// Build an iroh `Endpoint` bound to `bind_port`. ALPNs are set by the
/// `Router` when it spawns.
async fn build_endpoint(secret_key: &SecretKey, bind_port: u16) -> anyhow::Result<Endpoint> {
    let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, bind_port);
    Endpoint::builder(presets::N0)
        .secret_key(secret_key.clone())
        .bind_addr(bind_addr)
        .map_err(|e| anyhow::anyhow!("invalid bind addr {bind_addr}: {e}"))?
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("endpoint bind failed: {e}"))
}

/// Adapter: implements `decdn_gossip::GossipMetrics` against the node's
/// Prometheus registry.
#[derive(Debug)]
struct NodeGossipMetrics {
    metrics: Arc<metrics::Metrics>,
}

impl NodeGossipMetrics {
    const fn new(metrics: Arc<metrics::Metrics>) -> Self {
        Self { metrics }
    }
}

impl GossipMetrics for NodeGossipMetrics {
    fn inc_published(&self, topic: &str) {
        self.metrics.gossip_published(topic);
    }
    fn inc_received(&self, topic: &str) {
        self.metrics.gossip_received(topic);
    }
    fn inc_rejected(&self, reason: &'static str) {
        self.metrics.gossip_rejected(reason);
    }
    fn set_peer_table_size(&self, n: i64) {
        self.metrics.gossip_peer_table_size(n);
    }
}

/// Log a `JoinError` from a shutdown-drained task with a phase label.
fn log_join_result(result: Result<(), tokio::task::JoinError>, phase: &'static str) {
    if let Err(err) = result {
        if err.is_cancelled() {
            tracing::debug!(phase, "task cancelled during shutdown");
        } else {
            tracing::warn!(phase, %err, "task failed during shutdown");
        }
    }
}

/// Construct the cache engine from resolved config. At most one of
/// `origin_url` and `origin_path` is set (guaranteed by
/// `config::resolve_cache`); neither-set means the engine serves only
/// already-cached content and cache misses surface as
/// `CacheError::NoOrigin`.
async fn build_cache(cfg: &ResolvedConfig) -> anyhow::Result<CacheEngine> {
    let origin: Option<Arc<dyn Origin>> =
        match (cfg.cache.origin_url.clone(), cfg.cache.origin_path.clone()) {
            (Some(url), None) => Some(Arc::new(
                HttpOrigin::new(url).context("failed to build HTTP origin client")?,
            )),
            (None, Some(path)) => Some(Arc::new(
                FilesystemOrigin::new(path)
                    .await
                    .context("failed to open filesystem origin")?,
            )),
            (None, None) => None,
            // resolve_cache enforces this mutex; this arm is unreachable in
            // practice but a typed fallback is safer than unwrap() or unreachable!().
            (Some(_), Some(_)) => {
                anyhow::bail!("cache.origin_url and cache.origin_path are mutually exclusive")
            }
        };
    CacheEngine::open(&cfg.cache.cache_dir, origin, cfg.cache.max_blob_size_mb)
        .await
        .context("failed to open cache engine")
}

/// Which OS signal triggered shutdown. Returned by [`shutdown_signal`] so
/// the "shutdown signal received" log line records the cause (SIGINT vs.
/// SIGTERM) — operators need that distinction for post-incident analysis,
/// and a future drain path can branch on it (immediate on SIGINT, graceful
/// on SIGTERM). `Sigterm` is unreachable on non-unix targets.
#[derive(Debug, Clone, Copy)]
enum ShutdownSignal {
    Sigint,
    #[cfg(unix)]
    Sigterm,
}

impl std::fmt::Display for ShutdownSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sigint => f.write_str("SIGINT"),
            #[cfg(unix)]
            Self::Sigterm => f.write_str("SIGTERM"),
        }
    }
}

/// Wait for either SIGINT or (on Unix) SIGTERM; return which one fired.
async fn shutdown_signal() -> ShutdownSignal {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(err) => {
                tracing::warn!(%err, "failed to install SIGTERM handler; falling back to SIGINT only");
                let _ = tokio::signal::ctrl_c().await;
                return ShutdownSignal::Sigint;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => ShutdownSignal::Sigint,
            _ = term.recv() => ShutdownSignal::Sigterm,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        ShutdownSignal::Sigint
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Operators grep `signal=SIGINT` / `signal=SIGTERM` in the structured
    // "shutdown signal received" log line; a rename here would silently
    // break dashboards and runbooks.
    #[test]
    fn shutdown_signal_display_is_stable() {
        assert_eq!(ShutdownSignal::Sigint.to_string(), "SIGINT");
        #[cfg(unix)]
        assert_eq!(ShutdownSignal::Sigterm.to_string(), "SIGTERM");
    }
}
