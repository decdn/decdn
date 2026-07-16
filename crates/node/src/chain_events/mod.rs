//! Shared chain-event log streaming for the on-chain watchers.
//!
//! Every watcher drives its log source through `resumable_watcher::run`, a
//! single `eth_getLogs` polling loop that unifies historical backfill and the
//! live tail into one resumable, cursor-persisting loop (#1092). It replaced the
//! per-watcher `watch_logs` (`eth_newFilter` + `eth_getFilterChanges`) streams,
//! which the default public Arbitrum Sepolia RPC and most keyless endpoints
//! reject with `-32601` (#1106). No `watch_logs`/`eth_newFilter` call remains on
//! the node's hot path.
//!
//! # Watchers scan to head, deliberately
//!
//! Every watcher's scan upper bound is the head block itself. There is no
//! confirmation lag and no knob for one: a `WatcherConfig::confirmations` field
//! existed, documented a `head - confirmations` bound, and was passed `0` by all
//! six watchers — so it never lagged anything, while its doc claimed otherwise
//! (#1227). Reading from the unstable tip is safe here for two reasons:
//!
//! - **Every sink is idempotent under re-scan.** `resumable_watcher`'s module
//!   doc does not merely observe this, it *requires* it (dedup by id / set
//!   insertion / authoritative re-read) — the retry and reorg-rewind paths
//!   already re-deliver logs. A re-mined event is therefore re-applied
//!   harmlessly, which is the same property a confirmation lag would buy.
//! - **Reorgs on the target chain are shallow.** See `backfill`'s
//!   `REORG_MARGIN_BLOCKS`, which carries the sizing rationale for an
//!   Arbitrum-Sepolia-class L2 and is the single place that claim lives.
//!
//! The two `CursorPolicy::Persisted` watchers additionally rewind `reorg_margin`
//! blocks on resume, covering a shallow reorg *across* a restart — the case
//! re-scan idempotency alone cannot reach, because the cursor is durable.
//!
//! The concrete cost of a lag is why it is zero rather than merely unnecessary:
//! it would delay channel registration by `confirmations` blocks, so a client's
//! first request on a freshly-opened channel would be rejected as unknown.
//!
//! If a future chain needs a lag, reintroduce it as a config knob with a
//! **non-zero default and a test asserting a live production config engages
//! it** — not a silently-zero field whose unit test exercises the arithmetic
//! rather than the wiring.

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

/// Apply a per-call timeout to an RPC future, folding the elapsed case and the
/// call's own error into one `anyhow::Error`. A `None` config uses
/// [`DEFAULT_RPC_CALL_TIMEOUT`].
///
/// **Every** chain read on a watcher's path routes through here: the loop's own
/// `get_logs`, `shared_head`'s `get_block_number`, and each follow-up RPC a
/// [`resumable_watcher::LogSink`] issues from `apply` / `on_tick_complete`
/// (`getChannel`, `getOrigins`, `nodeIdOf`, `get_block`,
/// `isHashBlacklistedForOperator`). This is not automatic — the loop cannot see
/// a sink's own reads, so a *new* sink read that does not wrap itself here
/// reintroduces the wedge this exists to prevent: the alloy HTTP provider has no
/// request timeout, so a provider that holds the connection open and never
/// responds stalls the tick forever, with no backoff, no metric, and no
/// graceful-shutdown path.
///
/// This helper guarantees only that a call is *bounded*. What a timeout **means**
/// is the call site's decision, and each documents its own — the two live shapes
/// being fail-the-tick-and-back-off (`getChannel`, reputation's `nodeIdOf`,
/// origin's `getOrigins`) and degrade-and-continue (`slash`'s `get_block`, the
/// registry's `nodeIdOf`, origin's `nodeIdOf`).
///
/// `nodeIdOf` is the same read at three sites under two policies, which is
/// deliberate and not a bug to unify: reputation fails the tick because a
/// skipped settlement is a silent accounting gap nothing re-derives, while the
/// registry and origin count-and-skip because their projections self-heal on
/// the operator's next event. See `reputation_indexer::resolve_binding`.
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
/// Every `timed` call site is a claim that *this* read cannot wedge the tick.
/// The only way to test that claim is to drive the production read path against
/// a provider that stalls — asserting the pure helper (as
/// `timed_bounds_a_hanging_call_at_the_default` does) proves the mechanism but
/// not the wiring, which is exactly the gap that let `confirmations` ship
/// documented-but-disabled.
///
/// alloy's `Asserter` cannot express a stall: `MockTransport::map_request` pops
/// a queued response or errors immediately on an empty queue — it never pends.
/// So this is the smallest transport that can: `poll_ready` is always ready and
/// `call` returns `pending()`, so a request is dispatched and then hangs with no
/// I/O involved. That last part matters, but not for the reason you might
/// expect. Under `#[tokio::test(start_paused = true)]` the clock auto-advances
/// whenever the runtime goes idle, and waiting on I/O does *not* inhibit that —
/// only `spawn_blocking` does (it is the sole caller of tokio's
/// `inhibit_auto_advance`; `park_thread_timeout` otherwise polls the I/O driver
/// with a zero timeout and then advances). So a wiremock server with a long
/// response delay does not hang the test — it fails it *vacuously*: the clock
/// races to the 10s deadline while the HTTP round-trip is still in flight, so
/// `timed` fires on virtual time rather than on the stall being tested, and the
/// test no longer distinguishes a bounded read from a slow one. A never-resolving
/// in-process future has nothing to race: the deadline is the only pending timer,
/// so firing it is the assertion.
#[cfg(test)]
#[allow(clippy::panic)]
pub(crate) mod test_support {
    use super::DEFAULT_RPC_CALL_TIMEOUT;
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::rpc::client::RpcClient;
    use alloy::transports::{TransportError, TransportFut};
    use alloy_json_rpc::{RequestPacket, ResponsePacket};
    use std::future::Future;
    use std::task::{Context, Poll};
    use std::time::Duration;

    /// Dispatches every request into a future that never completes.
    #[derive(Clone, Debug)]
    struct HangingTransport;

    impl tower::Service<RequestPacket> for HangingTransport {
        type Response = ResponsePacket;
        type Error = TransportError;
        type Future = TransportFut<'static>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: RequestPacket) -> Self::Future {
            Box::pin(std::future::pending())
        }
    }

    /// A `Provider` on which every RPC hangs forever. Pair with
    /// `#[tokio::test(start_paused = true)]` so the deadline fires instantly.
    pub(crate) fn hanging_provider() -> impl Provider + Clone {
        // `is_local = false`: the local flag only relaxes alloy's own polling
        // cadences, and nothing here should imply this endpoint is fast.
        ProviderBuilder::new().connect_client(RpcClient::new(HangingTransport, false))
    }

    /// Backstop deadline for [`bounded`]: strictly longer than any production
    /// `timed` bound (the longest is [`DEFAULT_RPC_CALL_TIMEOUT`]), so a working
    /// bound always fires first and this can never mask a real result.
    const GUARD: Duration = DEFAULT_RPC_CALL_TIMEOUT.saturating_mul(6);

    /// Run `fut` (a read against [`hanging_provider`]) under a backstop deadline.
    ///
    /// Failure legibility, not correctness: if a `timed` wrap is ever dropped
    /// from the site under test, the read hangs forever and the test hangs with
    /// it — and nothing cuts that short, since the repo carries no nextest
    /// config at all and the built-in `slow-timeout` only warns (it sets no
    /// `terminate-after`), so CI would stall for its whole run rather than
    /// fail. Wrapping
    /// here turns that regression into a millisecond failure that names the
    /// site. Both deadlines are virtual under `start_paused`, so this costs no
    /// wall-clock in the passing case.
    pub(crate) async fn bounded<T>(what: &str, fut: impl Future<Output = T>) -> T {
        // `unwrap_or_else` rather than a `match` with an `Err(_)` arm: the only
        // error here is `Elapsed`, and matching it as a wildcard trips
        // `clippy::match_wild_err_arm` (fatal under CI's `-D warnings`), whose
        // suggested `.expect(msg)` is itself denied by the anti-panic policy.
        tokio::time::timeout(GUARD, fut)
            .await
            .unwrap_or_else(|_elapsed| {
                panic!(
                    "{what} did not complete within {GUARD:?} of virtual time — its \
                     `chain_events::timed` bound is missing, so a stalled provider \
                     wedges the watcher tick forever"
                )
            })
    }
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

    /// The hanging provider really hangs, and `timed` really bounds it through a
    /// *real* alloy provider rather than a bare `pending()` future.
    ///
    /// This pins the test-support seam itself: every wiring test in the watcher
    /// modules is only as good as this. If a future alloy bump makes
    /// `HangingTransport` resolve or reject, this fails here — one clear failure
    /// — instead of silently turning five wiring tests into tautologies.
    #[tokio::test(start_paused = true)]
    async fn hanging_provider_stalls_until_timed_fires() {
        use alloy::providers::Provider;

        let provider = super::test_support::hanging_provider();
        let err = timed(None, "get_block_number", provider.get_block_number())
            .await
            .err()
            .map(|e| format!("{e:#}"));
        assert!(
            err.as_ref()
                .is_some_and(|e| e.contains("get_block_number timed out after")),
            "a hanging provider must fail at the deadline, not resolve or error early: {err:?}"
        );
    }
}
