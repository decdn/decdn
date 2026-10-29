//! `GET /_status` of `decdn dashboard`: one JSON read of every source, as the
//! pollers last saw it. The page polls it; nothing here calls a source.

use decdn_common::admin::{
    BuyerPoolsResponse, EarningsResponse, HealthResponse, LanesResponse, SlashesResponse,
    StatusResponse,
};
use serde::Serialize;

use super::super::activity::{PullRecord, Unreadable};
use super::super::node_top::{Snapshot, SnapshotDelta};

/// The body of `GET /_status`.
#[derive(Debug, Clone, Serialize)]
pub(super) struct Status {
    /// The `decdn` version.
    pub(super) version: &'static str,
    /// When the dashboard answered, Unix milliseconds: the clock each
    /// section's `updated_unix_ms` is measured against.
    pub(super) now_unix_ms: u64,
    /// Whether the binary embeds the page's build, not its fallback.
    pub(super) dashboard_built: bool,
    /// When the dashboard started, Unix seconds.
    pub(super) started_unix: u64,
    pub(super) pulls: PullsSection,
    pub(super) gateway: GatewaySection,
    pub(super) node: NodeSection,
}

/// The `decdn get` runs this host recorded.
#[derive(Debug, Clone, Default, Serialize)]
pub(super) struct PullsSection {
    /// The directory the records live in.
    pub(super) dir: String,
    /// When the records were last read, Unix milliseconds; `None` before the
    /// first read.
    pub(super) updated_unix_ms: Option<u64>,
    /// The runs whose record says `running`, newest first.
    pub(super) active: Vec<PullView>,
    /// The runs that ended, newest first.
    pub(super) recent: Vec<PullView>,
    /// The `*.json` files in the directory that did not read as a record,
    /// sorted by path, each with why.
    pub(super) unreadable: Vec<Unreadable>,
    /// Why the directory cannot be listed; the lists are then empty.
    pub(super) error: Option<String>,
}

/// One run's record, as the page shows it.
#[derive(Debug, Clone, Serialize)]
pub(super) struct PullView {
    #[serde(flatten)]
    pub(super) record: PullRecord,
    /// The record says `running`, but its heartbeat stopped: the run exited
    /// without its last write.
    pub(super) stale: bool,
}

/// The gateway at `--gateway`.
#[derive(Debug, Clone, Serialize)]
pub(super) struct GatewaySection {
    /// The gateway's base URL.
    pub(super) url: String,
    /// When the gateway was last asked, Unix milliseconds; `None` before the
    /// first ask.
    pub(super) updated_unix_ms: Option<u64>,
    #[serde(flatten)]
    pub(super) state: GatewayState,
}

/// What the gateway answered.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(super) enum GatewayState {
    /// Not asked yet.
    Waiting,
    /// No gateway listens at the URL.
    Absent,
    /// The gateway asks for a token the dashboard does not hold.
    Unauthorized,
    /// The gateway answered something else, or not in time.
    Error { message: String },
    /// The gateway's own `/_status` body, passed through.
    Up { status: serde_json::Value },
}

/// The node at `--admin-url` and `--metrics-url`.
#[derive(Debug, Clone, Serialize)]
pub(super) struct NodeSection {
    /// The node's admin base URL.
    pub(super) admin_url: String,
    /// The node's metrics URL.
    pub(super) metrics_url: String,
    /// Whether a node answers: what its health call says.
    #[serde(flatten)]
    pub(super) state: NodeState,
    pub(super) health: Part<HealthResponse>,
    pub(super) status: Part<StatusResponse>,
    pub(super) metrics: Part<NodeMetrics>,
    pub(super) lanes: Part<LanesResponse>,
    pub(super) earnings: Part<EarningsResponse>,
    pub(super) pools: Part<BuyerPoolsResponse>,
    pub(super) slashes: Part<SlashesResponse>,
}

/// Whether a node answers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(super) enum NodeState {
    /// Not asked yet.
    Waiting,
    /// Nothing listens at the admin URL.
    Absent,
    /// The node answers.
    Up,
    /// The health call failed other than by a refused connection: no answer
    /// in time, or an answer that is not a node's. The other reads may still
    /// have answered.
    Error { message: String },
}

/// One of the node's reads, as the dashboard last made it.
#[derive(Debug, Clone, Serialize)]
pub(super) struct Part<T> {
    /// When the read was last made, Unix milliseconds; `None` before the
    /// first.
    updated_unix_ms: Option<u64>,
    #[serde(flatten)]
    read: Read<T>,
}

impl<T> Part<T> {
    /// A read not made yet.
    pub(super) const fn waiting() -> Self {
        Self {
            updated_unix_ms: None,
            read: Read::Waiting,
        }
    }

    /// A read made now that gave `read`.
    pub(super) fn now(read: Read<T>) -> Self {
        Self {
            updated_unix_ms: Some(super::super::activity::now_ms()),
            read,
        }
    }

    /// What the read gave.
    pub(super) const fn read(&self) -> &Read<T> {
        &self.read
    }
}

/// What a read of the node gave.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(super) enum Read<T> {
    /// Not made yet.
    Waiting,
    /// The node answered.
    Ok { value: T },
    /// The node runs without what this read reports: no slash detection,
    /// no buyer pool, no settlement ledger, no DHT.
    Unconfigured { message: String },
    /// The read failed.
    Error { message: String },
}

/// The node's `/metrics`, as `decdn node top` reads it.
#[derive(Debug, Clone, Serialize)]
pub(super) struct NodeMetrics {
    #[serde(flatten)]
    pub(super) snapshot: Snapshot,
    /// Per-second rates since the scrape before; `None` on the first.
    pub(super) rates: Option<SnapshotDelta>,
    /// Cache hits over hits and misses since the node started; `None` before
    /// either.
    pub(super) hit_rate: Option<f64>,
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::too_many_lines
)]
mod tests;
