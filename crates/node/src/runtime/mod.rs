//! Node runtime: owns the iroh endpoint, metrics server, and protocol router.

pub mod reload;

pub use reload::{LogLevelSetter, ReloadSnapshot, RuntimeReloadState};

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
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
use crate::dispatch::ConnectionLimiter;
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
///
/// SIGHUP triggers a hot-reload of mutable config fields via
/// [`RuntimeReloadState`] (see issue #236). Other signals
/// (SIGINT/SIGTERM) trigger graceful shutdown.
///
/// The signal streams (SIGHUP, SIGTERM) are registered **once** before
/// the select loop and reused on every iteration. Re-creating
/// `tokio::signal::unix::Signal` each iteration would race with signal
/// delivery: a SIGHUP that arrives while `reload_runtime_config(..)` is
/// running would have nowhere to land if the future holding the
/// `Signal` had already been dropped, and would be silently lost. The
/// persistent stream queues the signal until the next `recv()` call
/// (kernel-managed, with coalescing) so concurrent or rapidly-repeated
/// signals are observed deterministically.
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
pub async fn run(
    cfg: ResolvedConfig,
    config_path: Option<PathBuf>,
    reload_state: Arc<RuntimeReloadState>,
) -> anyhow::Result<()> {
    // Captured at the very top of `run()`, before any `await` or I/O,
    // so `admin_v1_health.uptime_s` reflects the entire process lifetime
    // — including the RPC reachability preflight below (which can spend
    // up to its 5s timeout on flaky networks). Operators reasoning about
    // "how long has this node been up?" want every second since `decdn
    // run` was invoked, not just everything after the admin server bound.
    let started_at = std::time::Instant::now();

    // Preflight: verify RPC endpoint is reachable before committing to
    // port binding. A 5-second timeout keeps startup responsive on flaky
    // networks while still catching typos and dead endpoints early.
    check_rpc_reachability(&cfg.blockchain.rpc_url).await?;

    let node_metrics = Arc::new(metrics::Metrics::new());
    node_metrics.started();

    let secret_key = identity::load_or_generate(&cfg.identity.data_dir)?;
    tracing::info!(node_id = %secret_key.public(), "loaded node identity");

    let cache = build_cache(&cfg).await?;
    // Attach the cache to the reload state so SIGHUP handlers can swap
    // the pinned-hashes set atomically (#276). Done immediately after
    // `build_cache` succeeds so a SIGHUP delivered during the rest of
    // startup will still find a target.
    reload_state.attach_cache(Some(cache.clone()));
    tracing::info!(
        cache_dir = %cfg.cache.cache_dir.display(),
        has_origin = cfg.cache.origin_url.is_some() || cfg.cache.origin_path.is_some(),
        pinned_hashes = cfg.cache.pinned_hashes.len(),
        "cache engine ready",
    );

    let ep = build_endpoint(&secret_key, cfg.network.bind_port)
        .await
        .context("failed to build iroh endpoint")?;

    node_metrics
        .register_iroh_endpoint(&ep)
        .context("failed to register iroh metrics")?;

    let gossip = Gossip::builder().spawn(ep.clone());

    let limiter = Arc::new(ConnectionLimiter::new(
        &cfg.security,
        Arc::clone(&node_metrics),
    ));
    // Attach the limiter to the reload state so SIGHUP / admin reloads
    // can forward `[security]` changes via `ConnectionLimiter::reload`
    // (#235). Done immediately after construction so a SIGHUP delivered
    // during the rest of startup still finds a target.
    reload_state.attach_limiter(Some(Arc::clone(&limiter)));

    let probe_handler = Arc::new(ProbeHandler::new(
        secret_key.public(),
        reload_state.rate_per_mb(),
        Arc::clone(&node_metrics),
        limiter,
    ));

    let router = Router::builder(ep.clone())
        .accept(ProbeHandler::ALPN, probe_handler)
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    let metrics_addr = std::net::SocketAddr::new(
        cfg.observability.metrics_bind,
        cfg.observability.metrics_port,
    );
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

    // RPC connectivity watchdog (issue #283). Updates `decdn_rpc_healthy`
    // each tick; a sustained transition fires an alert. `interval == 0`
    // disables the watchdog entirely (operators can opt out for offline
    // dev). Spawned outside the `JoinSet` because we drive it via its own
    // `oneshot` and an explicit `await` during drain — same shape as the
    // gossip handles, since `JoinSet::abort_all` cancels eagerly and we'd
    // rather let the watchdog observe its `shutdown` arm.
    //
    // Seed the gauge to `1` here unconditionally: the startup
    // `check_rpc_reachability` above already established the endpoint is
    // reachable, and registered Prometheus gauges otherwise default to 0
    // — which alerts would (correctly, by their own logic) read as an
    // outage. Seeding before the watchdog-spawn branch covers the
    // `interval == 0` case too.
    node_metrics.rpc_healthy(true);
    let rpc_watchdog = if cfg.blockchain.rpc_watchdog_interval_sec > 0 {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .context("failed to build HTTP client for RPC watchdog")?;
        let interval = Duration::from_secs(cfg.blockchain.rpc_watchdog_interval_sec);
        let (tx, rx) = oneshot::channel::<()>();
        let handle = spawn_rpc_watchdog(
            client,
            cfg.blockchain.rpc_url.clone(),
            interval,
            Arc::clone(&node_metrics),
            rx,
        );
        Some((tx, handle))
    } else {
        tracing::info!("RPC watchdog disabled (blockchain.rpc_watchdog_interval_sec = 0)");
        None
    };

    let peer_table = Arc::new(RwLock::new(PeerTable::new(
        cfg.gossip.peer_ttl_sec.saturating_mul(1_000_000),
    )));

    // Admin HTTP surface (ADR 025). Bind here — *before* the gossip service
    // spawns — so startup fails fast on a port collision rather than after
    // side-effectful subscriptions have registered. The `serve` task is
    // spawned later, once gossip has produced its `AnnounceTrigger`, so the
    // `admin_v1_announce` method has somewhere to forward to.
    let admin_listener = if let Some(admin_port) = cfg.observability.admin_port {
        let admin_addr = std::net::SocketAddr::from(([127, 0, 0, 1], admin_port));
        Some(
            admin::bind(admin_addr)
                .await
                .context("failed to bind admin listener")?,
        )
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
    let decdn_gossip::GossipHandles {
        tasks: gossip_handles,
        announce_trigger,
    } = GossipService::spawn(
        ep.clone(),
        secret_key.clone(),
        gossip.clone(),
        gossip_runtime_cfg,
        Arc::clone(&peer_table),
        gossip_metrics,
    )
    .await
    .context("gossip service failed to start")?;

    // Drain trigger for `admin_v1_drain` (issue #244). Constructed
    // unconditionally so the admin handler always has a live target —
    // there is no "drain disabled" state analogous to "no region" or
    // "no config path". The `Arc` is shared with the admin state (via
    // `Arc::clone`) and the select loop arm below.
    let drain_trigger = Arc::new(admin::DrainTrigger::new());

    // Now that gossip is up and we know whether the publisher produced an
    // `AnnounceTrigger`, spawn the admin serve task with the full state.
    // Bind happened earlier (see `admin_listener` above) so a port collision
    // would have failed startup before any side-effectful subscribes ran.
    let admin_stop_tx = if let Some(listener) = admin_listener {
        let (tx, rx) = oneshot::channel::<()>();
        // Build the reload hook only when a config file path was passed
        // (CLI-only invocation has nothing on disk to re-read). The
        // hook hands `admin_v1_reload` the same `RuntimeReloadState` and
        // file path the SIGHUP arm uses, so both paths converge on a
        // single mutex-serialised reload — see `admin::AdminRpcImpl::reload`
        // and the SIGHUP arm of the select loop below.
        let reload_hook = config_path.as_ref().map(|path| admin::ReloadHook {
            reload_state: Arc::clone(&reload_state),
            config_path: path.clone(),
        });
        let state = admin::AdminState::new(
            Arc::clone(&peer_table),
            *secret_key.public().as_bytes(),
            started_at,
            cache.clone(),
            announce_trigger,
            reload_hook,
            Arc::clone(&drain_trigger),
        );
        tasks.spawn(async move {
            if let Err(err) = admin::serve(listener, state, rx).await {
                tracing::error!(%err, "admin server exited with error");
            }
        });
        Some(tx)
    } else {
        None
    };

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
        metrics_addr = %metrics_addr,
        admin_port = ?cfg.observability.admin_port,
        rate_per_mb = cfg.payment.rate_per_mb,
        cache_dir = %cfg.cache.cache_dir.display(),
        has_origin = cfg.cache.origin_url.is_some() || cfg.cache.origin_path.is_some(),
        subscribe_global = cfg.gossip.subscribe_global,
        "node runtime ready"
    );

    // Install signal streams once, before entering the select loop.
    // tokio docs are explicit that `Signal::recv` is the supported way
    // to await repeated signals, and re-creating the stream per signal
    // is not — see `tokio::signal::unix::signal` for the reasoning.
    let mut shutdown_streams = ShutdownStreams::install();
    let mut hup_stream = HupStream::install();

    let signal = loop {
        tokio::select! {
            sig = shutdown_streams.recv() => break sig,
            () = hup_stream.recv() => {
                match config_path.as_deref() {
                    Some(path) => {
                        if let Err(err) = reload_state.reload(path).await {
                            tracing::warn!(%err, "config reload error");
                        }
                    }
                    None => {
                        tracing::warn!(
                            "SIGHUP received but no config file path is in use; ignoring"
                        );
                    }
                }
            }
            () = drain_trigger.wait() => {
                tracing::info!("admin_v1_drain received; initiating graceful shutdown");
                break ShutdownSignal::AdminDrain;
            }
        }
    };
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
    let rpc_watchdog_handle = if let Some((tx, handle)) = rpc_watchdog {
        if tx.send(()).is_err() {
            tracing::warn!("RPC watchdog exited before shutdown signal was sent");
        }
        Some(handle)
    } else {
        None
    };

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
        // Await the RPC watchdog. We signalled it via oneshot above, so
        // a healthy run resolves cleanly here. A panic surfaces as a
        // warning; cancellation is silent (matches gossip handling).
        if let Some(handle) = rpc_watchdog_handle
            && let Err(err) = handle.await
            && !err.is_cancelled()
        {
            tracing::warn!(%err, "RPC watchdog task panicked during shutdown");
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
    fn inc_reconnected(&self, topic: &str) {
        self.metrics.gossip_reconnected(topic);
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
                HttpOrigin::new(url)
                    .context("failed to build HTTP origin client")?
                    .with_decompress_mode(cfg.cache.decompress),
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
    CacheEngine::open_with_pinned(
        &cfg.cache.cache_dir,
        origin,
        cfg.cache.max_blob_size_mb,
        cfg.cache.pinned_hashes.clone(),
    )
    .await
    .context("failed to open cache engine")
}

/// Which OS signal (or admin RPC call) triggered shutdown. Returned by
/// [`ShutdownStreams::recv`] or synthesised by the `drain_trigger` arm so
/// the "shutdown signal received" log line records the cause (SIGINT,
/// SIGTERM, or admin drain) — operators grep that field for post-incident
/// analysis. `Sigterm` is unreachable on non-unix targets.
#[derive(Debug, Clone, Copy)]
enum ShutdownSignal {
    Sigint,
    #[cfg(unix)]
    Sigterm,
    /// Graceful shutdown requested via `admin_v1_drain` (issue #244).
    AdminDrain,
}

impl std::fmt::Display for ShutdownSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sigint => f.write_str("SIGINT"),
            #[cfg(unix)]
            Self::Sigterm => f.write_str("SIGTERM"),
            Self::AdminDrain => f.write_str("admin-drain"),
        }
    }
}

/// Persistent SIGHUP stream, installed once at startup. The
/// `Signal` instance lives on `self` for the lifetime of the runtime
/// so successive SIGHUPs delivered while a reload is in progress are
/// queued by tokio (kernel-coalesced) rather than dropped. Re-creating
/// the `Signal` per iteration of the select loop would race signal
/// delivery — that's the bug this struct exists to prevent.
///
/// On non-Unix targets `recv` returns a `pending` future since SIGHUP
/// doesn't exist there.
struct HupStream {
    #[cfg(unix)]
    hup: Option<tokio::signal::unix::Signal>,
}

impl HupStream {
    fn install() -> Self {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let hup = match signal(SignalKind::hangup()) {
                Ok(s) => Some(s),
                Err(err) => {
                    tracing::warn!(%err, "failed to install SIGHUP handler; hot-reload disabled");
                    None
                }
            };
            Self { hup }
        }
        #[cfg(not(unix))]
        {
            Self {}
        }
    }

    /// Wait for the next SIGHUP on the persistent stream. If
    /// installation failed (`None`) or the platform isn't Unix, this
    /// future never resolves.
    ///
    /// `Signal::recv` resolves to `Option<()>`. `None` indicates the
    /// underlying stream has been closed (the driver dropped, signal
    /// handler torn down) — once that happens the future would resolve
    /// *immediately, forever*, and the runtime select-loop would
    /// hot-spin. Park on `pending` in that branch instead so hot-reload
    /// is silently disabled rather than turning into a spin-loop.
    async fn recv(&mut self) {
        #[cfg(unix)]
        {
            // Three cases:
            //   1. install succeeded + stream yielded a real signal →
            //      return so the caller runs the reload arm.
            //   2. install succeeded but the stream is now closed →
            //      `recv()` returns `None`, which would otherwise
            //      resolve *immediately, forever* and hot-spin the
            //      runtime's select. Take ownership, log once, and park
            //      on `pending` for the rest of the process.
            //   3. install failed at startup (`self.hup` is `None`) →
            //      already logged a warning at install time; just park
            //      on `pending`.
            if let Some(stream) = self.hup.as_mut() {
                if stream.recv().await.is_some() {
                    return;
                }
                self.hup = None;
                tracing::warn!("SIGHUP stream closed; hot-reload disabled for this process");
            }
            std::future::pending::<()>().await;
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<()>().await
        }
    }
}

/// Persistent SIGINT/SIGTERM streams. Installed once at startup so
/// rapid-fire shutdown signals (e.g. operator pressing Ctrl-C twice)
/// are coalesced by the kernel rather than potentially lost between
/// re-registrations of `Signal`.
struct ShutdownStreams {
    /// `tokio::signal::ctrl_c` is itself a one-shot future that
    /// re-registers internally — but `tokio::signal::unix::signal`
    /// (used for SIGTERM) is not. We park `ctrl_c()` inside `recv`
    /// directly and rely on tokio's own implementation for SIGINT.
    #[cfg(unix)]
    term: Option<tokio::signal::unix::Signal>,
}

impl ShutdownStreams {
    fn install() -> Self {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let term = match signal(SignalKind::terminate()) {
                Ok(s) => Some(s),
                Err(err) => {
                    tracing::warn!(
                        %err,
                        "failed to install SIGTERM handler; falling back to SIGINT only",
                    );
                    None
                }
            };
            Self { term }
        }
        #[cfg(not(unix))]
        {
            Self {}
        }
    }

    /// Wait for the next SIGINT or (on Unix) SIGTERM and report which
    /// one fired. The persistent SIGTERM stream lives on `self`, so
    /// successive calls observe successive signals — there's no
    /// race window between iterations of the runtime's select loop.
    ///
    /// `Signal::recv` resolves to `Option<()>`. A `None` from the
    /// SIGTERM arm would otherwise resolve immediately forever and
    /// short-circuit the select to a spurious shutdown; demote that
    /// branch to a `pending` future so we wait for a real SIGINT
    /// instead. (`tokio::signal::ctrl_c` re-registers internally and
    /// is safe to await directly.)
    async fn recv(&mut self) -> ShutdownSignal {
        #[cfg(unix)]
        {
            if let Some(term) = self.term.as_mut() {
                let race = tokio::select! {
                    _ = tokio::signal::ctrl_c() => Some(ShutdownSignal::Sigint),
                    res = term.recv() => res.map(|()| ShutdownSignal::Sigterm),
                };
                if let Some(sig) = race {
                    return sig;
                }
                // SIGTERM arm closed (`None`). Drop the stream so we
                // don't keep racing a future that resolves immediately
                // forever, then wait for a real SIGINT.
                tracing::warn!("SIGTERM stream closed; falling back to SIGINT only");
                self.term = None;
            }
            let _ = tokio::signal::ctrl_c().await;
            ShutdownSignal::Sigint
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            ShutdownSignal::Sigint
        }
    }
}

/// Verify that the JSON-RPC endpoint is reachable by sending a lightweight
/// `net_version` request with a short timeout. Logs a warning and returns an
/// error if the endpoint does not respond, letting operators catch typos and
/// dead endpoints before the node binds ports and joins the gossip network.
async fn check_rpc_reachability(rpc_url: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("failed to build HTTP client for RPC check")?;
    probe_rpc(&client, rpc_url).await?;
    tracing::info!("RPC endpoint reachable");
    Ok(())
}

/// Issue a single `net_version` JSON-RPC probe against `rpc_url` using the
/// given client. Returns `Ok(())` on a 2xx response, an error otherwise.
/// Extracted so the startup check and the watchdog share identical
/// success/failure semantics — a deviation between the two would mean
/// "startup says healthy, gauge says unhealthy" or vice versa.
pub(crate) async fn probe_rpc(client: &reqwest::Client, rpc_url: &str) -> anyhow::Result<()> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "net_version",
        "params": [],
        "id": 1
    });

    let resp = client
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .context("blockchain.rpc_url is not reachable (timeout or connection refused)")?;

    anyhow::ensure!(
        resp.status().is_success(),
        "blockchain.rpc_url returned unexpected status {}; \
         verify the endpoint is a valid JSON-RPC server",
        resp.status()
    );

    Ok(())
}

/// Spawn the RPC connectivity watchdog. On each tick of `interval`, calls
/// [`probe_rpc`] and updates `metrics.rpc_healthy(...)`. Logs a `warn`
/// when health transitions down and an `info` when it transitions back
/// up; steady-state ticks are silent. Stops when `shutdown` resolves.
///
/// The caller is responsible for seeding the gauge before this is
/// spawned — `run()` does so right after the successful
/// `check_rpc_reachability` startup probe. The watchdog only writes the
/// gauge on tick boundaries. The previous-state tracking starts
/// assuming the endpoint is healthy for the same reason, which prevents
/// a spurious "recovered" log on the first tick.
fn spawn_rpc_watchdog(
    client: reqwest::Client,
    rpc_url: String,
    interval: Duration,
    metrics: Arc<metrics::Metrics>,
    mut shutdown: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Caller seeded the gauge from the startup probe; see fn docs.
        let mut prev_healthy = true;
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    tracing::debug!("RPC watchdog shutdown signal received");
                    return;
                }
                () = tokio::time::sleep(interval) => {}
            }

            let now_healthy = match probe_rpc(&client, &rpc_url).await {
                Ok(()) => {
                    if !prev_healthy {
                        // Deliberately omit the URL: the node configures one
                        // RPC endpoint, and it may carry basic-auth credentials
                        // in the userinfo component.
                        tracing::info!("RPC endpoint healthy");
                    }
                    true
                }
                Err(err) => {
                    if prev_healthy {
                        // URL omitted for the same reason as the recovery log
                        // above; `%err` keeps the actionable context.
                        tracing::warn!(%err, "RPC endpoint unhealthy");
                    }
                    false
                }
            };
            metrics.rpc_healthy(now_healthy);
            prev_healthy = now_healthy;
        }
    })
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // Operators grep `signal=SIGINT` / `signal=SIGTERM` / `signal=admin-drain`
    // in the structured "shutdown signal received" log line; a rename here
    // would silently break dashboards and runbooks.
    #[test]
    fn shutdown_signal_display_is_stable() {
        assert_eq!(ShutdownSignal::Sigint.to_string(), "SIGINT");
        #[cfg(unix)]
        assert_eq!(ShutdownSignal::Sigterm.to_string(), "SIGTERM");
        assert_eq!(ShutdownSignal::AdminDrain.to_string(), "admin-drain");
    }

    /// Mount a JSON-RPC `200 OK` POST handler. The mount lives on
    /// `server` until the next `server.reset().await`; callers flip
    /// state by resetting and mounting `mount_unhealthy` instead.
    async fn mount_healthy(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(server)
            .await;
    }

    /// Mount a JSON-RPC `500` POST handler. Used to flip the watchdog
    /// from healthy to unhealthy without tearing the listener down.
    async fn mount_unhealthy(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(500))
            .mount(server)
            .await;
    }

    /// Poll `metrics.rpc_healthy_value()` until it equals `expected` or
    /// the deadline expires. Returns the observed value either way so
    /// failures show what we actually saw rather than just timing out.
    async fn wait_for_gauge(metrics: &Arc<metrics::Metrics>, expected: i64) -> i64 {
        let deadline = std::time::Instant::now() + Duration::from_millis(1500);
        loop {
            let v = metrics.rpc_healthy_value();
            if v == expected || std::time::Instant::now() >= deadline {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = false)]
    async fn rpc_watchdog_tracks_endpoint_transitions() {
        // Healthy -> unhealthy -> healthy transitions, all observed via
        // the `rpc_healthy` gauge. Tight 100ms tick keeps the test under
        // 5s wall-clock while still exercising multiple poll cycles.
        let server = MockServer::start().await;
        mount_healthy(&server).await;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let metrics = Arc::new(metrics::Metrics::new());
        // Mirror `run()`: the caller seeds the gauge from the startup
        // probe, so the watchdog can assume `prev_healthy = true` without
        // emitting a spurious "recovered" log on the first tick.
        metrics.rpc_healthy(true);
        let (tx, rx) = oneshot::channel::<()>();
        let handle = spawn_rpc_watchdog(
            client,
            server.uri(),
            Duration::from_millis(100),
            Arc::clone(&metrics),
            rx,
        );

        // Caller-seeded above; the wait still confirms the loop is
        // running and has observed at least one healthy probe.
        assert_eq!(wait_for_gauge(&metrics, 1).await, 1, "should be healthy");

        // Flip to unhealthy. wiremock's last-mounted response wins for
        // matching POSTs, so the next probe sees a 500.
        server.reset().await;
        mount_unhealthy(&server).await;
        assert_eq!(
            wait_for_gauge(&metrics, 0).await,
            0,
            "should detect unhealthy",
        );

        // Bring it back. Watchdog should recover within a few ticks.
        server.reset().await;
        mount_healthy(&server).await;
        assert_eq!(
            wait_for_gauge(&metrics, 1).await,
            1,
            "should detect recovery",
        );

        // Clean shutdown.
        let _ = tx.send(());
        let join_res = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(join_res.is_ok(), "watchdog should exit on shutdown signal");
    }

    /// Smoke test for the post-fixup `ShutdownStreams::recv` contract:
    /// a real SIGTERM raised from inside the test process must resolve
    /// the future to `ShutdownSignal::Sigterm`. nextest runs each test
    /// in its own process so the signal cannot leak across tests.
    /// `nix::sys::signal::raise` keeps the workspace `unsafe_code =
    /// "forbid"` lint clean.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_streams_recv_resolves_on_sigterm() {
        use std::time::Duration;

        use nix::sys::signal::{Signal, raise};

        let mut streams = ShutdownStreams::install();
        // Spawn the raise on a separate task so `recv()` is awaiting
        // on the SIGTERM stream by the time the signal arrives. The
        // small sleep gives `install()` a chance to register tokio's
        // handler — without it the kernel could deliver SIGTERM with
        // the default disposition (terminate the process) before the
        // tokio handler is in place. 20ms is far longer than the
        // install path needs.
        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            raise(Signal::SIGTERM).expect("raise SIGTERM");
        });
        let signal = tokio::time::timeout(Duration::from_millis(500), streams.recv())
            .await
            .expect("ShutdownStreams::recv did not resolve within 500ms of SIGTERM");
        assert!(matches!(signal, ShutdownSignal::Sigterm));
    }
}
