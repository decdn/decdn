use super::DEFAULT_RPC_CALL_TIMEOUT;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::client::RpcClient;
use alloy::transports::{TransportError, TransportFut};
use alloy_json_rpc::{RequestPacket, ResponsePacket};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

/// Dispatches every request into a future that never completes, counting
/// each dispatch.
#[derive(Clone, Debug, Default)]
struct HangingTransport {
    /// Requests dispatched so far.
    calls: Arc<AtomicUsize>,
}

impl tower::Service<RequestPacket> for HangingTransport {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: RequestPacket) -> Self::Future {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Box::pin(std::future::pending())
    }
}

/// A `Provider` on which every RPC hangs forever. Pair with
/// `#[tokio::test(start_paused = true)]` so the deadline fires instantly.
pub(crate) fn hanging_provider() -> impl Provider + Clone {
    counting_hanging_provider().0
}

/// A [`hanging_provider`] plus a count of the requests dispatched to it.
pub(crate) fn counting_hanging_provider() -> (impl Provider + Clone, Arc<AtomicUsize>) {
    let transport = HangingTransport::default();
    let calls = Arc::clone(&transport.calls);
    // `is_local = false`: the local flag only relaxes alloy's own polling
    // cadences, and nothing here should imply this endpoint is fast.
    let provider = ProviderBuilder::new().connect_client(RpcClient::new(transport, false));
    (provider, calls)
}

/// Backstop deadline for [`bounded`]: strictly longer than any production
/// `timed` bound (the longest is [`DEFAULT_RPC_CALL_TIMEOUT`]), so a working
/// bound always fires first and this can never mask a real result.
const GUARD: Duration = DEFAULT_RPC_CALL_TIMEOUT.saturating_mul(6);

/// Run `fut` (a read against [`hanging_provider`]) under a backstop deadline.
///
/// Failure legibility, not correctness: if a `timed` wrap is ever dropped
/// from the site under test, the read hangs forever and the test hangs with
/// it. The only other bound is the nextest backstop in
/// `.config/nextest.toml`, which terminates a `decdn-node` test after 180s
/// of wall-clock (`terminate-after = 3` at a 60s period) and retries it
/// twice, so the regression costs minutes of CI and reports only a
/// timeout. Wrapping here turns it into a millisecond failure that names
/// the site. Both deadlines are virtual under `start_paused`, so this costs
/// no wall-clock in the passing case.
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
