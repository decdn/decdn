//! Node runtime: owns the iroh endpoint, metrics server, and protocol router.

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, SecretKey};
use iroh_gossip::ALPN as GOSSIP_ALPN;
use iroh_gossip::net::Gossip;
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinSet;

use decdn_gossip::{GossipMetrics, GossipRuntimeConfig, GossipService, PeerTable};

use crate::config::ResolvedConfig;
use crate::handlers::probe::ProbeHandler;
use crate::{identity, metrics};

/// Ceiling on how long we wait for spawned tasks to drain after the endpoint
/// and metrics server have been signalled to stop.
const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(15);

/// Build the endpoint, register handlers on a `Router`, spawn the metrics
/// server and gossip tasks, and run until a shutdown signal is received.
#[allow(clippy::cognitive_complexity)]
pub async fn run(cfg: ResolvedConfig) -> anyhow::Result<()> {
    let node_metrics = Arc::new(metrics::Metrics::new());
    node_metrics.started();

    let secret_key = identity::load_or_generate(&cfg.identity.data_dir)?;
    tracing::info!(node_id = %secret_key.public(), "loaded node identity");

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
    let gossip_runtime_cfg = GossipRuntimeConfig {
        announce_interval_sec: cfg.gossip.announce_interval_sec,
        peer_ttl_sec: cfg.gossip.peer_ttl_sec,
        subscribe_global: cfg.gossip.subscribe_global,
        region: cfg.identity.region.clone(),
        allowlist: cfg.gossip.allowlist.iter().copied().collect::<HashSet<_>>(),
    };
    let gossip_metrics: Arc<dyn GossipMetrics> =
        Arc::new(NodeGossipMetrics::new(Arc::clone(&node_metrics)));
    let gossip_handles = GossipService::spawn(
        ep.clone(),
        secret_key.clone(),
        gossip.clone(),
        gossip_runtime_cfg,
        Arc::clone(&peer_table),
        gossip_metrics,
    );
    for handle in gossip_handles {
        tasks.spawn(async move {
            if let Err(err) = handle.await {
                tracing::warn!(%err, "gossip task exited");
            }
        });
    }

    tracing::info!(
        bind_port = cfg.network.bind_port,
        metrics_port = cfg.observability.metrics_port,
        "node runtime ready"
    );

    shutdown_signal().await;
    tracing::info!("shutdown signal received; closing router");

    // Router::shutdown waits for ProtocolHandler::shutdown on each handler,
    // then closes the endpoint. Unblock the metrics accept loop too.
    if let Err(err) = router.shutdown().await {
        tracing::warn!(%err, "router shutdown reported an error");
    }
    let _ = metrics_stop_tx.send(());

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
