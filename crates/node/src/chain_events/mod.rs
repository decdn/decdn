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

use std::future::IntoFuture;
use std::time::Duration;

use anyhow::Result;
use tokio::task::JoinHandle;

/// Aborts a spawned watcher task on drop, so a node-restart cycle never leaks a
/// chain-poll task. Shared by every watcher (and the buyer-channel service): the
/// definition was copy-pasted seven times before, which made it look like each
/// watcher had its own teardown policy when they were byte-identical.
#[derive(Debug)]
pub(crate) struct AbortOnDrop(pub(crate) JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

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
/// This bounds only the calls it wraps — `resumable_watcher`'s own `get_logs`,
/// `shared_head`'s `get_block_number`, and any sink read that wraps itself (as
/// `capacity_bond_registry`'s `nodeIdOf` and `blacklist_watcher::scope_check` do).
/// A follow-up RPC a [`resumable_watcher::LogSink::apply`] issues (`getOrigins`,
/// `getChannel`, …) is NOT routed through here automatically and stays unbounded
/// otherwise, so a provider that stalls one of those can still wedge a tick.
/// Bounding every remaining sink-internal read is a tracked follow-up.
///
/// Takes `IntoFuture`, not `Future`, so an alloy `.call()` (which returns an
/// `EthCall`, not a future) can be wrapped directly rather than each caller
/// spelling `.into_future()`. Every `Future` is an `IntoFuture`, so this is a
/// strict widening.
pub(crate) async fn timed<T, E, F>(timeout: Option<Duration>, what: &str, fut: F) -> Result<T>
where
    F: IntoFuture<Output = std::result::Result<T, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    let d = timeout.unwrap_or(DEFAULT_RPC_CALL_TIMEOUT);
    tokio::time::timeout(d, fut)
        .await
        .map_err(|_| anyhow::anyhow!("{what} timed out after {d:?}"))?
        .map_err(anyhow::Error::new)
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

    /// A call that never resolves must fail at [`DEFAULT_RPC_CALL_TIMEOUT`] rather
    /// than wedge the tick forever, and the error must name the call so the
    /// caller's context composes onto something diagnostic. `start_paused` lets
    /// tokio auto-advance to the deadline, so this costs no wall-clock time.
    ///
    /// This is the mechanism every self-bounding sink read relies on (the
    /// `nodeIdOf` / `scope_check` pattern), so it is pinned here rather than at
    /// each call site.
    #[tokio::test(start_paused = true)]
    async fn timed_bounds_a_hanging_call_at_the_default() {
        let hang = std::future::pending::<std::result::Result<u64, std::io::Error>>();
        let err = timed(None, "nodeIdOf", hang)
            .await
            .err()
            .map(|e| format!("{e:#}"));
        assert!(
            err.as_ref()
                .is_some_and(|e| e.contains("nodeIdOf timed out after")),
            "a hanging call must time out and name itself: {err:?}"
        );
    }

    /// An explicit timeout overrides the default.
    #[tokio::test(start_paused = true)]
    async fn timed_honours_an_explicit_timeout() {
        let hang = std::future::pending::<std::result::Result<u64, std::io::Error>>();
        let started = tokio::time::Instant::now();
        let _ = timed(Some(Duration::from_millis(50)), "get_logs", hang).await;
        assert!(
            started.elapsed() < DEFAULT_RPC_CALL_TIMEOUT,
            "explicit timeout must win over the 10s default"
        );
    }

    /// A call that succeeds inside the deadline passes its value through.
    #[tokio::test(start_paused = true)]
    async fn timed_passes_a_prompt_success_through() {
        let ok = async { Ok::<u64, std::io::Error>(7) };
        assert_eq!(timed(None, "get_block_number", ok).await.ok(), Some(7));
    }
}
