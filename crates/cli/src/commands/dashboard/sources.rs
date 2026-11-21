//! The pollers behind `GET /_status`. Each reads one source on its own
//! schedule and keeps what it last saw in [`Sources`]; `/_status` reads only
//! that, so however many pages poll, each source is asked at one rate.
//!
//! - The pull records, every [`PULLS_EVERY`].
//! - The gateway's `/_status`, every [`GATEWAY_EVERY`].
//! - The node's health, status and `/metrics`, every [`NODE_FAST_EVERY`].
//! - The node's lanes, earnings, buyer pools and slashes, every
//!   [`NODE_SLOW_EVERY`]: `lanes` may read the chain for a pool's owner, and
//!   `earnings` reads the whole settlement ledger.

use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

use decdn_common::admin::{
    AdminRpcClient as _, BUYER_POOL_UNAVAILABLE_CODE, DHT_UNAVAILABLE_CODE,
    EARNINGS_UNAVAILABLE_CODE, EarningsRequest, SLASH_DETECTION_UNAVAILABLE_CODE,
};
use jsonrpsee::core::client::Error as JsonRpcClientError;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use reqwest::StatusCode;
use url::Url;

use super::super::activity;
use super::super::node::{classify_client_error, is_connection_refused};
use super::super::node_top::{Snapshot, SnapshotDelta, fetch_metrics, hit_rate, parse_openmetrics};
use super::status::{
    GatewaySection, GatewayState, NodeMetrics, NodeSection, NodeState, Part, PullView,
    PullsSection, Read,
};

/// How often the pull records are read.
pub(super) const PULLS_EVERY: Duration = Duration::from_secs(1);

/// How often the gateway is asked.
pub(super) const GATEWAY_EVERY: Duration = Duration::from_secs(2);

/// How often the node's cheap reads run.
pub(super) const NODE_FAST_EVERY: Duration = Duration::from_secs(2);

/// How often the node's costly reads run.
pub(super) const NODE_SLOW_EVERY: Duration = Duration::from_secs(15);

/// How long a cheap read may take.
const FAST_TIMEOUT: Duration = Duration::from_secs(3);

/// How long a costly read may take: a pool-owner read on the chain alone
/// may take ten seconds.
const SLOW_TIMEOUT: Duration = Duration::from_secs(20);

/// How many ended runs the page lists.
const RECENT_PULLS: usize = 50;

/// What every poller last saw.
pub(super) struct Sources {
    pub(super) pulls: RwLock<PullsSection>,
    pub(super) gateway: RwLock<GatewaySection>,
    pub(super) node: RwLock<NodeSection>,
}

/// Where each source is.
pub(super) struct Targets {
    /// The pull records' directory; `None` with no home directory.
    pub(super) pulls_dir: Option<PathBuf>,
    /// The gateway's base URL.
    pub(super) gateway: Url,
    /// The node's admin base URL.
    pub(super) admin_url: String,
    /// The node's `/metrics` URL.
    pub(super) metrics_url: String,
}

impl Sources {
    /// Nothing seen yet, at `targets`.
    pub(super) fn new(targets: &Targets) -> Self {
        Self {
            pulls: RwLock::new(PullsSection {
                dir: targets
                    .pulls_dir
                    .as_ref()
                    .map(|d| d.display().to_string())
                    .unwrap_or_default(),
                ..PullsSection::default()
            }),
            gateway: RwLock::new(GatewaySection {
                url: targets.gateway.to_string(),
                updated_unix_ms: None,
                state: GatewayState::Waiting,
            }),
            node: RwLock::new(NodeSection {
                admin_url: targets.admin_url.clone(),
                metrics_url: targets.metrics_url.clone(),
                state: NodeState::Waiting,
                health: Part::waiting(),
                status: Part::waiting(),
                metrics: Part::waiting(),
                lanes: Part::waiting(),
                earnings: Part::waiting(),
                pools: Part::waiting(),
                slashes: Part::waiting(),
            }),
        }
    }
}

/// Start every poller. They run until the process exits.
pub(super) fn spawn(sources: &Arc<Sources>, targets: Targets) -> anyhow::Result<()> {
    let http = http_client()?;
    let gateway_status = targets
        .gateway
        .join("_status")
        .map_err(|e| anyhow::anyhow!("--gateway {}: {e}", targets.gateway))?;
    let fast = admin_client(&targets.admin_url, FAST_TIMEOUT)?;
    let slow = admin_client(&targets.admin_url, SLOW_TIMEOUT)?;
    if let Some(dir) = targets.pulls_dir {
        supervise("pull records", poll_pulls(Arc::clone(sources), dir));
    }
    supervise(
        "gateway",
        poll_gateway(Arc::clone(sources), http.clone(), gateway_status),
    );
    supervise(
        "node",
        poll_node_fast(
            Arc::clone(sources),
            (fast, http),
            targets.admin_url.clone(),
            targets.metrics_url,
        ),
    );
    supervise(
        "node ledgers",
        poll_node_slow(Arc::clone(sources), slow, targets.admin_url),
    );
    Ok(())
}

/// Run `poller` and log when it ends, which it does only by a panic: its
/// section then keeps its last read, and the page shows it growing old.
fn supervise(name: &'static str, poller: impl Future<Output = ()> + Send + 'static) {
    let task = tokio::spawn(poller);
    tokio::spawn(async move {
        if let Err(e) = task.await {
            tracing::warn!("the dashboard's {name} poller stopped: {e}");
        }
    });
}

/// Read the pull records in `dir` every [`PULLS_EVERY`].
async fn poll_pulls(sources: Arc<Sources>, dir: PathBuf) {
    every(PULLS_EVERY, move || {
        let sources = Arc::clone(&sources);
        let dir = dir.clone();
        async move {
            match tokio::task::spawn_blocking(move || read_pulls(&dir)).await {
                Ok(section) => *write(&sources.pulls) = section,
                Err(e) => tracing::warn!("the dashboard's read of the pull records failed: {e}"),
            }
        }
    })
    .await;
}

/// Ask the gateway's `/_status` at `url` every [`GATEWAY_EVERY`].
async fn poll_gateway(sources: Arc<Sources>, http: reqwest::Client, url: Url) {
    every(GATEWAY_EVERY, move || {
        let sources = Arc::clone(&sources);
        let http = http.clone();
        let url = url.clone();
        async move { gateway_tick(&sources, &http, url).await }
    })
    .await;
}

/// One ask of the gateway at `url`, into `sources`.
pub(super) async fn gateway_tick(sources: &Sources, http: &reqwest::Client, url: Url) {
    let state = ask_gateway(http, url).await;
    let mut gateway = write(&sources.gateway);
    gateway.state = state;
    gateway.updated_unix_ms = Some(activity::now_ms());
}

/// Read the node's health, status and `/metrics` every [`NODE_FAST_EVERY`].
async fn poll_node_fast(
    sources: Arc<Sources>,
    (admin, http): (HttpClient, reqwest::Client),
    admin_url: String,
    metrics_url: String,
) {
    let mut interval = tokio::time::interval(NODE_FAST_EVERY);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last: Option<(Snapshot, Instant)> = None;
    loop {
        interval.tick().await;
        let (health, status, metrics) =
            tokio::join!(admin.health(), admin.status(), scrape(&http, &metrics_url),);
        let metrics = metrics.map(|snapshot| {
            let now = Instant::now();
            let rates = last
                .as_ref()
                .map(|(prev, at)| SnapshotDelta::between(prev, &snapshot, now - *at));
            last = Some((snapshot.clone(), now));
            NodeMetrics {
                hit_rate: hit_rate(snapshot.serve_cache_hits, snapshot.serve_cache_misses),
                snapshot,
                rates,
            }
        });
        let refused = matches!(
            &health,
            Err(JsonRpcClientError::Transport(inner)) if is_connection_refused(inner.as_ref())
        );
        let health = part(&admin_url, FAST_TIMEOUT, health);
        let mut node = write(&sources.node);
        node.state = node_state(&health, refused);
        node.health = health;
        node.status = part(&admin_url, FAST_TIMEOUT, status);
        node.metrics = Part::now(match metrics {
            Ok(value) => Read::Ok { value },
            Err(e) => Read::Error {
                message: format!("{e:#}"),
            },
        });
    }
}

/// Read the node's lanes, earnings, buyer pools and slashes every
/// [`NODE_SLOW_EVERY`].
async fn poll_node_slow(sources: Arc<Sources>, admin: HttpClient, admin_url: String) {
    every(NODE_SLOW_EVERY, move || {
        let sources = Arc::clone(&sources);
        let admin = admin.clone();
        let admin_url = admin_url.clone();
        async move {
            let (lanes, earnings, pools, slashes) = tokio::join!(
                admin.lanes(),
                admin.earnings(EarningsRequest::default()),
                admin.pools(),
                admin.slashes(),
            );
            let mut node = write(&sources.node);
            node.lanes = part(&admin_url, SLOW_TIMEOUT, lanes);
            node.earnings = part(&admin_url, SLOW_TIMEOUT, earnings);
            node.pools = part(&admin_url, SLOW_TIMEOUT, pools);
            node.slashes = part(&admin_url, SLOW_TIMEOUT, slashes);
        }
    })
    .await;
}

/// Run `tick` now and then every `period`, each run after the last ends.
async fn every<F, Fut>(period: Duration, mut tick: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        tick().await;
    }
}

/// The client every HTTP read uses.
pub(super) fn http_client() -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(FAST_TIMEOUT)
        .build()
        .map_err(|e| anyhow::anyhow!("build the dashboard's HTTP client: {e}"))
}

fn admin_client(url: &str, timeout: Duration) -> anyhow::Result<HttpClient> {
    HttpClientBuilder::default()
        .request_timeout(timeout)
        .build(url)
        .map_err(|e| anyhow::anyhow!("build an admin client for {url}: {e}"))
}

fn write<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

/// Whether a node answers, from what its health call gave and whether the
/// call's connection was `refused`.
pub(super) fn node_state<T>(health: &Part<T>, refused: bool) -> NodeState {
    match health.read() {
        Read::Ok { .. } => NodeState::Up,
        Read::Waiting => NodeState::Waiting,
        Read::Error { .. } | Read::Unconfigured { .. } if refused => NodeState::Absent,
        Read::Error { message } | Read::Unconfigured { message } => NodeState::Error {
            message: message.clone(),
        },
    }
}

/// The records in `dir`: the running ones apart, newest first, and at most
/// [`RECENT_PULLS`] that ended.
pub(super) fn read_pulls(dir: &Path) -> PullsSection {
    let now = activity::now_ms();
    let section = PullsSection {
        dir: dir.display().to_string(),
        updated_unix_ms: Some(now),
        ..PullsSection::default()
    };
    let (records, unreadable) = match activity::read_records(dir) {
        Ok(read) => read,
        Err(e) => {
            return PullsSection {
                error: Some(format!("cannot list {}: {e}", dir.display())),
                ..section
            };
        }
    };
    let (active, ended): (Vec<_>, Vec<_>) = records
        .into_iter()
        .map(|record| PullView {
            stale: record.is_stale(now),
            record,
        })
        .partition(|view| view.record.state.is_running());
    PullsSection {
        active,
        recent: ended.into_iter().take(RECENT_PULLS).collect(),
        unreadable,
        ..section
    }
}

/// Ask the gateway at `url` (its `/_status`) for its state.
pub(super) async fn ask_gateway(http: &reqwest::Client, url: Url) -> GatewayState {
    let response = match http.get(url).send().await {
        Ok(response) => response,
        Err(e) if is_connection_refused(&e) => return GatewayState::Absent,
        Err(e) => {
            return GatewayState::Error { message: chain(e) };
        }
    };
    match response.status() {
        StatusCode::OK => match response.json::<serde_json::Value>().await {
            Ok(status) if status.get("counters").is_some() => GatewayState::Up { status },
            Ok(_) => GatewayState::Error {
                message: "the answer is not a gateway's /_status".to_owned(),
            },
            Err(e) => GatewayState::Error { message: chain(e) },
        },
        status => GatewayState::Error {
            message: format!("HTTP {status}"),
        },
    }
}

/// `e` with every cause it carries, the URL left out.
fn chain(e: reqwest::Error) -> String {
    format!("{:#}", anyhow::Error::new(e.without_url()))
}

/// Scrape the node's `/metrics` at `url`.
async fn scrape(http: &reqwest::Client, url: &str) -> anyhow::Result<Snapshot> {
    let body = fetch_metrics(http, url).await?;
    Ok(Snapshot::from_metrics(&parse_openmetrics(&body)))
}

/// The part one admin read gives: what the node answered, or why not.
pub(super) fn part<T>(
    url: &str,
    timeout: Duration,
    result: Result<T, JsonRpcClientError>,
) -> Part<T> {
    let read = match result {
        Ok(value) => Read::Ok { value },
        Err(JsonRpcClientError::Call(obj))
            if matches!(
                obj.code(),
                SLASH_DETECTION_UNAVAILABLE_CODE
                    | BUYER_POOL_UNAVAILABLE_CODE
                    | EARNINGS_UNAVAILABLE_CODE
                    | DHT_UNAVAILABLE_CODE
            ) =>
        {
            Read::Unconfigured {
                message: obj.message().to_owned(),
            }
        }
        Err(e) => {
            let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
            Read::Error {
                message: format!("{:#}", classify_client_error(url, timeout_ms, e)),
            }
        }
    };
    Part::now(read)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests;
