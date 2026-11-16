//! Chain-state reads shared by the node's chain-state snapshots.
//!
//! The node reads every piece of chain state it keeps by `eth_call`. It reads
//! no event log. Each snapshot (the `CapacityBond` registry, this operator's
//! slashes, the `OriginAssignment` seats, the `ContentBlacklist` deny index and
//! the live payment pools) is re-read by its own `refresh::run_refresh_loop`.
//! This module holds what those loops share: the per-call timeout (`timed`),
//! the boot retry ([`boot_retry`]) and the pinned snapshot block
//! (`snapshot::snapshot_block`).

// Public because `BootRetry` appears in `pub` bootstrap signatures.
pub mod boot_retry;
pub mod refresh;
pub mod snapshot;

use std::future::IntoFuture;
use std::time::Duration;

use anyhow::Result;

/// First backoff after a failed boot read, doubled (bounded by
/// [`WATCHER_MAX_BACKOFF`]) on each successive failure.
pub(crate) const WATCHER_INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Ceiling for the boot read's exponential backoff. See
/// [`WATCHER_INITIAL_BACKOFF`].
pub(crate) const WATCHER_MAX_BACKOFF: Duration = Duration::from_mins(1);

/// Fallback per-RPC-call timeout when a caller does not set its own. The alloy
/// HTTP provider has no request timeout of its own, so a provider that keeps the
/// connection open but never responds would otherwise wedge a refresh loop
/// forever and block graceful shutdown. A bounded default makes such a call
/// fail fast instead.
pub(crate) const DEFAULT_RPC_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Apply a per-call timeout to an RPC future, folding the elapsed case and the
/// call's own error into one `anyhow::Error`. A `None` config uses
/// [`DEFAULT_RPC_CALL_TIMEOUT`].
///
/// **Every** chain read on a refresh loop's path routes through here: the
/// snapshot block read, the registry, slash, origin-seat and blacklist
/// snapshots, the pool refresh and the redeemer's reads. A *new* read that does
/// not wrap itself here reintroduces the wedge this exists to prevent: the
/// alloy HTTP provider has no request timeout, so a provider that holds the
/// connection open and never responds stalls the loop forever, with no metric
/// and no graceful-shutdown path.
///
/// This helper guarantees only that a call is *bounded*. What a timeout **means**
/// is the call site's decision, and each documents its own. The serve-admission
/// reads in `ResolvingPoolView` (`getPool`, `getAuthorization`) refuse the
/// request. The `admin_v1_lanes` owner read (`ChainPoolOwners`) reports that
/// lane's owner as unknown for the one call.
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

/// A provider whose RPCs never resolve, for proving a read is bounded.
///
/// Every `timed` call site is a claim that *this* read cannot wedge its loop.
/// The only way to test that claim is to drive the production read path against
/// a provider that stalls — asserting the pure helper (as
/// `timed_bounds_a_hanging_call_at_the_default` does) proves the mechanism but
/// not the wiring.
///
/// alloy's `Asserter` cannot express a stall: `MockTransport::map_request` pops
/// a queued response or errors immediately on an empty queue — it never pends.
/// So this is the smallest transport that can: `poll_ready` is always ready and
/// `call` returns `pending()`, so a request is dispatched and then hangs with no
/// I/O involved. Under `#[tokio::test(start_paused = true)]` the clock
/// auto-advances whenever the runtime goes idle, and waiting on I/O does *not*
/// inhibit that, so a wiremock server with a long response delay would fail the
/// test vacuously. A never-resolving in-process future has nothing to race: the
/// deadline is the only pending timer, so firing it is the assertion.
#[cfg(test)]
#[allow(clippy::panic)]
pub(crate) mod test_support;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests;
