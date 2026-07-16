//! Shared chain-event log streaming for the on-chain watchers.
//!
//! Every watcher drives its log source through `resumable_watcher::run`, a
//! single `eth_getLogs` polling loop that unifies historical backfill and the
//! live tail into one resumable, cursor-persisting loop (#1092). It replaced the
//! per-watcher `watch_logs` (`eth_newFilter` + `eth_getFilterChanges`) streams,
//! which the default public Arbitrum Sepolia RPC and most keyless endpoints
//! reject with `-32601` (#1106). No `watch_logs`/`eth_newFilter` call remains on
//! the node's hot path.

pub(crate) mod backfill;
pub(crate) mod resumable_watcher;
// Public because it appears in the `pub` watcher `bootstrap` signatures, which
// external integration tests (`tests/anvil_settlement_e2e.rs`) call.
pub mod shared_head;

pub(crate) use backfill::{
    MAX_BACKFILL_BLOCK_SPAN, REORG_MARGIN_BLOCKS, backfill_windows, check_backfill_range,
};

use std::future::Future;
use std::time::Duration;

use anyhow::Result;

/// Default first backoff after a failing poll tick, doubled (bounded by
/// [`WATCHER_MAX_BACKOFF`]) on each successive failure and reset on a clean
/// tick. Shared by every watcher so the recovery cadence can't drift between
/// them (#1092); a watcher that genuinely needs a different cap overrides
/// `max_backoff` explicitly at its `WatcherConfig` site (e.g. the slash watcher).
pub(crate) const WATCHER_INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Default ceiling for the per-tick exponential backoff. See
/// [`WATCHER_INITIAL_BACKOFF`].
pub(crate) const WATCHER_MAX_BACKOFF: Duration = Duration::from_mins(1);

/// Fallback per-RPC-call timeout when a caller does not set its own. The alloy
/// HTTP provider has no request timeout of its own, so a provider that keeps the
/// connection open but never responds would otherwise wedge the tick forever —
/// silently stopping event processing and blocking graceful shutdown. A bounded
/// default makes such a call fail fast into the retry/backoff path instead.
pub(crate) const DEFAULT_RPC_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Apply a per-call timeout to an RPC future, mapping its error into `anyhow`. A
/// timeout is a retryable error (the tick backs off). A `None` config uses
/// [`DEFAULT_RPC_CALL_TIMEOUT`].
///
/// This bounds only the calls it wraps — `resumable_watcher`'s own `get_logs`
/// and `shared_head`'s `get_block_number`. A follow-up RPC a
/// [`resumable_watcher::LogSink::apply`] issues (`getOrigins`, `nodeIdOf`,
/// `getChannel`, …) is NOT routed through here and stays unbounded unless the
/// sink wraps it itself (as `blacklist_watcher::scope_check` does), so a provider
/// that stalls one of those can still wedge a tick. Bounding every sink-internal
/// read is a tracked follow-up.
pub(crate) async fn timed<T, E, F>(timeout: Option<Duration>, what: &str, fut: F) -> Result<T>
where
    F: Future<Output = std::result::Result<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    let d = timeout.unwrap_or(DEFAULT_RPC_CALL_TIMEOUT);
    tokio::time::timeout(d, fut)
        .await
        .map_err(|_| anyhow::anyhow!("{what} timed out after {d:?}"))?
        .map_err(anyhow::Error::new)
}
