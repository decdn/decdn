//! `GET /_status`: one JSON read of what the gateway holds and did, for the
//! dashboard and the operator. A loopback gateway answers it on its own host
//! behind the gateway token; any gateway answers it on `--metrics-listen`.

use std::net::SocketAddr;
use std::time::Duration;

use alloy::primitives::Address;
use decdn_common::redact::sanitize_err_chain;
use serde::Serialize;

use super::allowance::BucketView;
use super::metrics::Counters;
use super::pointer::Held;
use super::worker::PoolSnapshot;
use super::{ActivePullView, Capability, Gateway, dashboard, now_secs};

/// How long `/_status` waits for the worker to read the store.
const POOL_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Which gateway answers.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Mode {
    /// Serves one user's browser from a loopback address.
    Loopback,
    /// Serves the public from `--public-url`.
    Public,
}

/// The body of `GET /_status`.
#[derive(Serialize)]
pub(super) struct Status {
    mode: Mode,
    /// The `decdn` version.
    version: &'static str,
    /// Whether the binary embeds the dashboard's build, not its fallback.
    dashboard_built: bool,
    /// When the gateway started, Unix seconds.
    started_unix: u64,
    uptime_secs: u64,
    /// The main listener's address.
    listen: SocketAddr,
    chain_id: u64,
    payment_pool: Address,
    counters: Counters,
    /// The pulls in flight, oldest first.
    active_pulls: Vec<ActivePullView>,
    /// The capabilities a loopback gateway holds; a public one holds none.
    capabilities: Vec<Capability>,
    /// A public gateway's allowance buckets; `None` on loopback.
    allowance: Option<Vec<BucketView>>,
    /// A public gateway's pointers; `None` on loopback.
    pointers: Option<Vec<Held>>,
    /// What the buyer-pool store holds; `None` when it could not be read.
    pools: Option<PoolSnapshot>,
    /// Why `pools` is `None`.
    pool_error: Option<String>,
}

impl Gateway {
    /// Gather the status. The store read runs on the pull worker, bounded
    /// by [`POOL_QUERY_TIMEOUT`].
    pub(super) async fn status(&self) -> Status {
        let delegates: Vec<_> = self
            .delegates
            .lock()
            .await
            .values()
            .filter_map(|d| d.capability.as_ref())
            .map(|r| (r.grant.pool_id, r.grant.signer))
            .collect();
        let (pools, pool_error) =
            match tokio::time::timeout(POOL_QUERY_TIMEOUT, self.pulls.pools(delegates)).await {
                Ok(Ok(pools)) => (Some(pools), None),
                Ok(Err(e)) => (None, Some(sanitize_err_chain(&e))),
                Err(_) => (None, Some("the store read timed out".to_owned())),
            };
        let now = now_secs();
        Status {
            mode: if self.public() {
                Mode::Public
            } else {
                Mode::Loopback
            },
            version: env!("CARGO_PKG_VERSION"),
            dashboard_built: dashboard::built(),
            started_unix: self.started_unix,
            uptime_secs: self.started.elapsed().as_secs(),
            listen: self.listen,
            chain_id: self.chain.chain_id,
            payment_pool: self.chain.payment_pool,
            counters: self.metrics.snapshot(),
            active_pulls: self.active_pulls(),
            capabilities: self.capabilities().await,
            allowance: self.public.as_ref().map(|p| p.allowance.view(now)),
            pointers: self.public.as_ref().map(|p| p.pointers.all()),
            pools,
            pool_error,
        }
    }
}
