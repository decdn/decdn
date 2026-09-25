//! One shared, TTL-cached `eth_blockNumber` read for every chain watcher.
//!
//! Without a shared read, every tick would cost one `eth_blockNumber` *per
//! watcher* — ~7 head reads per `event_poll_interval` against a single endpoint,
//! on top of each watcher's `eth_getLogs`. [`SharedHead`] collapses them: the
//! first caller within a TTL window issues the RPC, everyone else reads the
//! cache.
//!
//! Two properties matter:
//!
//! - **Monotonicity — why a stale head is safe.** [`SharedHead`] only ever
//!   returns a head at-or-before the true head at the call instant; it can never
//!   report a head *ahead* of the chain. Every consumer of the head in
//!   `resumable_watcher` clamps *against* it (`resolve_persisted_start`,
//!   `resolve_head_window_start`, and the `from > to` idle branch), so a head
//!   that lags only ever *widens* an already-idempotent rescan or defers work to
//!   the next tick — it can never open a gap. Note this is a property of the
//!   cache direction, not a coincidence: a cache that could run *ahead* would
//!   skip blocks, which is exactly the #751/#762 hazard (anchoring a durable
//!   checkpoint floor too high). The cost is bounded observation latency (≤ TTL),
//!   not correctness.
//! - **Single-flight, including failures.** The mutex is held across the RPC, so
//!   concurrent callers queue and then read the fresh entry rather than each
//!   issuing their own read. Failures are cached for the same TTL, which is what
//!   keeps a wedged provider from serializing timeouts: without it, N watchers
//!   queued behind a stalled endpoint would each wait the full
//!   `chain_events::DEFAULT_RPC_CALL_TIMEOUT` in turn, so the last one's tick — and
//!   therefore its `*_backoff_started` gauge — would be delayed by N × 10s. With
//!   it, one caller pays the timeout and the rest fail instantly, so every
//!   watcher enters backoff at the same moment.

use std::sync::Arc;
use std::time::Duration;

use alloy::eips::BlockId;
use alloy::primitives::Address;
use alloy::providers::Provider;
use anyhow::Result;
use async_trait::async_trait;
use tokio::time::Instant;
use tracing::info;

use super::timed;

/// A source of the current chain head block number.
///
/// Implementations MAY serve from a short TTL cache. The returned value is always
/// at-or-before the true head at the call instant and never ahead of it — callers
/// may rely on that direction (see the module doc), but MUST NOT assume the value
/// is exactly current. A caller needing the head for a *deadline* decision ("has
/// block X been reached yet?") rather than a scan upper bound would be delayed by
/// up to the TTL; no watcher does that today.
///
/// `Err` keeps the provider's typed cause in its chain. A watcher tick treats
/// every `Err` as retryable and fails into backoff; a boot read classifies it
/// (`boot_retry::is_permanent_boot_error`), so a deterministic failure such as a
/// rejected API key fails boot at once.
#[async_trait]
pub trait HeadSource: Send + Sync {
    /// The current chain head, at or before the true head and never ahead
    /// of it. `Err` keeps its typed cause in its chain.
    async fn head(&self) -> Result<u64>;
}

/// How far below the reported head an enumeration snapshot pins its reads.
///
/// A load-balanced RPC reports the head of its freshest upstream, then routes
/// each call to any upstream. An `eth_call` pinned to that exact block fails on
/// every upstream that has not reached it yet, while an older block resolves on
/// all of them. So the reported head is not a block every upstream can serve.
/// 256 blocks is about 64s on Arbitrum, above the 40–200 block upstream lag seen
/// behind a hosted balancer (#2164).
///
/// The margin is a block count, sized for Arbitrum's ~250ms blocks and a
/// provider that serves state that far back. A full node that prunes state
/// within 256 blocks (geth keeps 128) answers every pinned read with a
/// transient error, so a boot against one spends its retry budget.
///
/// The margin costs no correctness. A boot enumeration seeds its tail cursor at
/// the snapshot block and every sink is idempotent, so the first tail tick
/// replays `[snapshot, head]` as a harmless overlap. A periodic re-enumeration
/// keeps every entry the tail changed after the snapshot block (see each
/// watcher's fold).
pub const SNAPSHOT_LAG_MARGIN_BLOCKS: u64 = 256;

/// The block an enumeration of `contract` pins its reads to:
/// [`SNAPSHOT_LAG_MARGIN_BLOCKS`] below the head.
///
/// A contract with no code at that block was deployed inside the margin — a
/// fresh local chain, or a boot right after a deploy. Reads pinned there would
/// decode an empty return, so the snapshot takes the head instead. That head pin
/// is exposed to upstream lag again; a failed read retries, and the lagged pin
/// returns once the chain is `SNAPSHOT_LAG_MARGIN_BLOCKS` past the deploy.
pub(crate) async fn snapshot_block<P: Provider>(
    provider: &P,
    head: &dyn HeadSource,
    contract: Address,
) -> Result<u64> {
    let head = head.head().await?;
    let lagged = head.saturating_sub(SNAPSHOT_LAG_MARGIN_BLOCKS);
    let code = timed(
        None,
        "eth_getCode",
        provider
            .get_code_at(contract)
            .block_id(BlockId::number(lagged)),
    )
    .await?;
    if code.is_empty() {
        info!(
            %contract,
            lagged,
            head,
            "no contract code at the lagged snapshot block; pinning the snapshot at head"
        );
        return Ok(head);
    }
    Ok(lagged)
}

/// A cached head read plus the instant it was taken.
struct CachedHead {
    at: Instant,
    /// `Arc` because `anyhow::Error` is not `Clone` and one failed read is
    /// replayed to every caller that arrives within the TTL.
    result: std::result::Result<u64, Arc<anyhow::Error>>,
}

/// A failed head read, shared by every caller inside the TTL.
///
/// The failure is kept whole behind an `Arc` (`anyhow::Error` is not `Clone`)
/// and exposed as this error's [`source`](std::error::Error::source), so a
/// caller's `err.chain()` still reaches the typed cause — the provider's
/// `TransportError` — and can classify it.
#[derive(Debug)]
struct HeadReadFailed(Arc<anyhow::Error>);

impl std::fmt::Display for HeadReadFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("chain head read failed")
    }
}

impl std::error::Error for HeadReadFailed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&**self.0)
    }
}

/// TTL-cached, single-flight [`HeadSource`] over one provider.
pub struct SharedHead<P> {
    provider: P,
    ttl: Duration,
    rpc_call_timeout: Option<Duration>,
    cached: tokio::sync::Mutex<Option<CachedHead>>,
}

/// Hand-written rather than derived: `P` is a provider and carries no `Debug`
/// bound, and the cached entry sits behind an async mutex this must not block on.
impl<P> std::fmt::Debug for SharedHead<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedHead")
            .field("ttl", &self.ttl)
            .field("rpc_call_timeout", &self.rpc_call_timeout)
            .finish_non_exhaustive()
    }
}

impl<P: Provider> SharedHead<P> {
    /// Cache head reads for half the watcher poll interval.
    ///
    /// Half, rather than a full interval, because the watchers are not
    /// phase-aligned: they bootstrap sequentially and their ticks drift across
    /// the interval, so it is the TTL — not the single-flight — that collapses
    /// the reads. A full-interval TTL would let a watcher act on a head an entire
    /// tick old, silently stretching the effective cadence of a documented knob
    /// toward 2×; half an interval bounds the added staleness below one tick for
    /// one extra RPC per interval. It also self-scales: an operator who drops
    /// `event_poll_interval_ms` to 250 ms for a local anvil gets a 125 ms TTL and
    /// a near-live head, with no second knob to tune.
    pub fn new(provider: P, poll_interval: Duration) -> Self {
        Self::with_ttl(provider, poll_interval / 2, None)
    }

    /// [`Self::new`] with an explicit TTL and per-call timeout. `Duration::ZERO`
    /// disables caching (every call re-reads).
    pub fn with_ttl(provider: P, ttl: Duration, rpc_call_timeout: Option<Duration>) -> Self {
        Self {
            provider,
            ttl,
            rpc_call_timeout,
            cached: tokio::sync::Mutex::new(None),
        }
    }
}

#[async_trait]
impl<P: Provider> HeadSource for SharedHead<P> {
    async fn head(&self) -> Result<u64> {
        // Held across the RPC: that IS the single-flight. A caller that queues
        // here finds the entry already refreshed and returns it, rather than
        // issuing a duplicate read.
        let mut guard = self.cached.lock().await;
        if let Some(entry) = guard.as_ref()
            && entry.at.elapsed() < self.ttl
        {
            return match &entry.result {
                Ok(head) => Ok(*head),
                // Every caller inside the TTL gets the same failure, typed
                // cause included, and renders the same text the first did.
                Err(err) => Err(HeadReadFailed(Arc::clone(err)).into()),
            };
        }
        let result = timed(
            self.rpc_call_timeout,
            "get_block_number",
            self.provider.get_block_number(),
        )
        .await;
        match result {
            Ok(head) => {
                *guard = Some(CachedHead {
                    at: Instant::now(),
                    result: Ok(head),
                });
                Ok(head)
            }
            Err(err) => {
                let shared = Arc::new(err);
                *guard = Some(CachedHead {
                    at: Instant::now(),
                    result: Err(Arc::clone(&shared)),
                });
                Err(HeadReadFailed(shared).into())
            }
        }
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
    use alloy::primitives::{Bytes, U64};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;

    const TTL: Duration = Duration::from_secs(4);

    fn mocked() -> (Asserter, impl Provider + Clone) {
        let asserter = Asserter::new();
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        (asserter, provider)
    }

    /// Two calls inside the TTL cost one RPC. The unconsumed queue is the proof:
    /// a second read would have popped a response that was never pushed.
    #[tokio::test]
    async fn ttl_serves_cached_head_without_a_second_rpc() {
        let (asserter, provider) = mocked();
        asserter.push_success(&U64::from(25));
        let head = SharedHead::with_ttl(provider, TTL, None);

        assert_eq!(head.head().await.ok(), Some(25));
        assert_eq!(head.head().await.ok(), Some(25), "second call is cached");
        assert_eq!(asserter.read_q().len(), 0, "exactly one RPC was issued");
    }

    /// `Duration::ZERO` disables caching entirely. This pins the invariant
    /// `resumable_watcher`'s mocked `run_tick` tests depend on: they queue
    /// head/log responses in strict order, so a cached head would desync the
    /// queue and mis-serve a head as a `get_logs` response.
    #[tokio::test]
    async fn zero_ttl_always_rereads() {
        let (asserter, provider) = mocked();
        asserter.push_success(&U64::from(25));
        asserter.push_success(&U64::from(30));
        let head = SharedHead::with_ttl(provider, Duration::ZERO, None);

        assert_eq!(head.head().await.ok(), Some(25));
        assert_eq!(head.head().await.ok(), Some(30), "no caching at ZERO ttl");
        assert_eq!(asserter.read_q().len(), 0);
    }

    /// Once the TTL lapses the next call re-reads.
    #[tokio::test(start_paused = true)]
    async fn expired_ttl_rereads() {
        let (asserter, provider) = mocked();
        asserter.push_success(&U64::from(25));
        asserter.push_success(&U64::from(30));
        let head = SharedHead::with_ttl(provider, TTL, None);

        assert_eq!(head.head().await.ok(), Some(25));
        tokio::time::advance(TTL + Duration::from_millis(1)).await;
        assert_eq!(head.head().await.ok(), Some(30), "re-read after the ttl");
        assert_eq!(asserter.read_q().len(), 0);
    }

    /// The single-flight proof: two concurrent callers, one pushed response.
    /// Without the mutex held across the RPC the second would pop an empty queue.
    #[tokio::test]
    async fn concurrent_calls_collapse_to_one_rpc() {
        let (asserter, provider) = mocked();
        asserter.push_success(&U64::from(25));
        let head = SharedHead::with_ttl(provider, TTL, None);

        let (a, b) = tokio::join!(head.head(), head.head());
        assert_eq!(a.ok(), Some(25));
        assert_eq!(b.ok(), Some(25));
        assert_eq!(asserter.read_q().len(), 0, "one RPC served both callers");
    }

    /// The convoy mitigation: a failed read is cached, so queued callers fail
    /// instantly instead of each paying their own RPC timeout in turn. Every
    /// watcher therefore enters backoff at the same moment.
    #[tokio::test]
    async fn error_fails_every_waiter_from_one_rpc() {
        let (asserter, provider) = mocked();
        asserter.push_failure_msg("boom");
        let head = SharedHead::with_ttl(provider, TTL, None);

        let (a, b) = tokio::join!(head.head(), head.head());
        assert!(a.is_err(), "first caller sees the failure");
        assert!(b.is_err(), "second caller replays the cached failure");
        assert_eq!(asserter.read_q().len(), 0, "one RPC, not two");
    }

    /// The typed provider error survives into the caller's error chain, for the
    /// first caller and for every caller the cache replays it to, so a boot read
    /// can tell a deterministic head failure from a transient one.
    #[tokio::test]
    async fn a_failure_keeps_its_typed_cause_and_text() {
        let (asserter, provider) = mocked();
        asserter.push_failure_msg("boom");
        let head = SharedHead::with_ttl(provider, TTL, None);

        for caller in ["first", "cached"] {
            let err = head.head().await.unwrap_err();
            assert!(
                err.chain()
                    .any(<dyn std::error::Error>::is::<alloy::transports::TransportError>),
                "{caller} caller lost the typed cause: {err:#}"
            );
            let text = format!("{err:#}");
            assert!(text.starts_with("chain head read failed: "), "{text}");
            assert!(text.contains("boom"), "{text}");
        }
    }

    /// A cached failure must not outlive its TTL — the watcher has to recover.
    #[tokio::test(start_paused = true)]
    async fn cached_error_does_not_poison_past_the_ttl() {
        let (asserter, provider) = mocked();
        asserter.push_failure_msg("boom");
        asserter.push_success(&U64::from(25));
        let head = SharedHead::with_ttl(provider, TTL, None);

        assert!(head.head().await.is_err());
        tokio::time::advance(TTL + Duration::from_millis(1)).await;
        assert_eq!(head.head().await.ok(), Some(25), "recovers after the ttl");
    }

    /// The end-to-end proof of the reduction, over real HTTP through the real
    /// `multiplexed_poller::run` loop rather than inferred from the unit tests
    /// above: N separate poller loops sharing one [`SharedHead`] must issue far
    /// fewer `eth_blockNumber` calls than one-per-loop-per-tick.
    ///
    /// Asserts both bounds that matter: the TTL bound (what the cache promises)
    /// and `< WATCHERS * ticks` (what it replaces). The second is the one that
    /// would catch a regression where `head` is accidentally rebuilt per loop.
    /// Discards logs — the counting test measures RPC counts, not projection state.
    struct NullSink;

    impl super::super::resumable_watcher::LogSink for NullSink {
        async fn apply(&mut self, _log: alloy::rpc::types::Log) -> Result<()> {
            Ok(())
        }
    }

    /// A JSON-RPC mock that echoes the request id and counts calls per method.
    struct CountingRpc {
        head_hits: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl wiremock::Respond for CountingRpc {
        fn respond(&self, req: &wiremock::Request) -> wiremock::ResponseTemplate {
            use std::sync::atomic::Ordering;
            let body: serde_json::Value =
                serde_json::from_slice(&req.body).unwrap_or(serde_json::Value::Null);
            let id = body.get("id").cloned().unwrap_or(serde_json::json!(0));
            let result = match body.get("method").and_then(serde_json::Value::as_str) {
                Some("eth_blockNumber") => {
                    self.head_hits.fetch_add(1, Ordering::SeqCst);
                    serde_json::json!("0x64")
                }
                Some("eth_getLogs") => serde_json::json!([]),
                // Alloy probes chain id when building the provider.
                _ => serde_json::json!("0x1"),
            };
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": id, "result": result,
            }))
        }
    }

    /// Stand up the counting JSON-RPC mock; returns a provider pointed at it and
    /// the `eth_blockNumber` counter. The server is returned so the caller keeps
    /// it alive for the duration of the test.
    async fn counting_server() -> (
        wiremock::MockServer,
        impl Provider + Clone,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let head_hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("POST"))
            .respond_with(CountingRpc {
                head_hits: Arc::clone(&head_hits),
            })
            .mount(&server)
            .await;
        let url = server.uri().parse().expect("mock server uri parses");
        let provider = ProviderBuilder::new().connect_http(url);
        (server, provider, head_hits)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn watchers_sharing_one_head_collapse_the_block_number_reads() {
        use super::super::multiplexed_poller::{MultiplexedPollerBuilder, Route, SinkSource, run};
        use super::super::resumable_watcher::CursorStart;
        use alloy::primitives::{Address, B256};
        use std::sync::atomic::Ordering;
        use tokio_util::sync::CancellationToken;

        const WATCHERS: usize = 6;
        const INTERVAL: Duration = Duration::from_millis(100);
        const RUN_FOR: Duration = Duration::from_millis(550);

        let (_server, provider, head_hits) = counting_server().await;
        // The one thing under test: a single head source behind every loop.
        let head: Arc<dyn HeadSource> = Arc::new(SharedHead::new(provider.clone(), INTERVAL));
        let shutdown = CancellationToken::new();

        // N separate single-route poller loops, each reading head through the one
        // shared source. Each loop's own head reads collapse onto the cache.
        let watchers: Vec<_> = (0..WATCHERS)
            .map(|_| {
                let route = Route {
                    addresses: vec![Address::ZERO],
                    topic0s: vec![B256::ZERO],
                    start: CursorStart::HeadMinusWindow { window_blocks: 0 },
                    sink: SinkSource::Ready(Box::new(NullSink)),
                    label: "count-test",
                    on_established: None,
                    on_backoff: None,
                    on_tick_success: None,
                    on_task_panic: None,
                };
                let built = MultiplexedPollerBuilder::new(Arc::clone(&head), INTERVAL)
                    .max_backfill_span(10_000)
                    .route(route)
                    .build();
                let poller = built.unwrap_or_else(|e| panic!("build must succeed: {e:#}"));
                tokio::spawn(run(provider.clone(), poller, shutdown.clone()))
            })
            .collect();

        let started = tokio::time::Instant::now();
        tokio::time::sleep(RUN_FOR).await;
        shutdown.cancel();
        for w in watchers {
            let _ = w.await;
        }
        let elapsed = started.elapsed();

        let heads = head_hits.load(Ordering::SeqCst);

        // Ticks are derived from wall time, NOT from the `get_logs` count: the
        // mock serves a static head, so after each watcher's first tick every
        // later tick takes the `from > to` idle branch and issues no `get_logs`
        // at all. It still reads head every tick, which is exactly what is being
        // counted here.
        let per_ms = |d: Duration| d.as_millis().max(1);
        let ticks = (per_ms(RUN_FOR) / per_ms(INTERVAL)) as usize + 1;
        let unshared = WATCHERS * ticks;

        // The TTL bound: at most one head read per TTL window, +1 for a partial
        // window at each end. Derived from MEASURED elapsed rather than the
        // intended `RUN_FOR`: the sleep can overshoot, and after `cancel()` each
        // watcher may still finish an in-flight tick (`run_tick` reads head before
        // it checks shutdown). Every extra TTL window legitimately permits one
        // more read, so bounding by the intended duration would fail under CI
        // scheduling jitter while the cache is behaving perfectly.
        let ttl_bound = (per_ms(elapsed) / per_ms(INTERVAL / 2)) as usize + 2;
        assert!(
            heads <= ttl_bound,
            "head reads ({heads}) must respect the ttl bound ({ttl_bound}) \
             over {elapsed:?}"
        );
        // The bound that matters: strictly fewer than one head read per watcher
        // per tick, which is what sharing replaces. This is the assertion that
        // fails if `head` is ever accidentally rebuilt per watcher. Deliberately
        // still derived from `RUN_FOR`: an overshooting run means MORE real ticks,
        // so this under-count can only make the assertion harder to pass.
        assert!(
            heads < unshared,
            "sharing must beat one-head-per-watcher-per-tick \
             (heads={heads}, unshared would be {unshared} = {WATCHERS} watchers x {ticks} ticks)"
        );
    }

    /// The cached failure replays the *original* message, not a placeholder —
    /// the shared `Arc` behind `HeadReadFailed` must not lose it, or the
    /// second-through-Nth watcher would log a less diagnostic error than the
    /// first for the very same fault. (`timed` only attaches its `get_block_number`
    /// label on the timeout path; a transport error propagates raw and
    /// `run_tick` composes `read head block` on top.)
    #[tokio::test]
    async fn cached_error_replays_the_original_message() {
        let (asserter, provider) = mocked();
        asserter.push_failure_msg("boom");
        let head = SharedHead::with_ttl(provider, TTL, None);

        let first = head.head().await.err().map(|e| format!("{e:#}"));
        let cached = head.head().await.err().map(|e| format!("{e:#}"));
        assert!(
            first.as_ref().is_some_and(|e| e.contains("boom")),
            "first caller sees the transport error: {first:?}"
        );
        assert_eq!(cached, first, "the cached replay is the same error text");
        assert_eq!(asserter.read_q().len(), 0, "one RPC, not two");
    }

    /// A snapshot sits the lag margin below the reported head while the
    /// contract has code there.
    #[tokio::test]
    async fn snapshot_block_sits_the_margin_below_head() {
        let (asserter, provider) = mocked();
        asserter.push_success(&U64::from(1_000));
        asserter.push_success(&Bytes::from_static(&[0x60, 0x80]));
        let head = SharedHead::with_ttl(provider.clone(), Duration::ZERO, None);

        let block = snapshot_block(&provider, &head, Address::ZERO).await.ok();

        assert_eq!(block, Some(1_000 - SNAPSHOT_LAG_MARGIN_BLOCKS));
        assert_eq!(asserter.read_q().len(), 0);
    }

    /// A contract deployed inside the margin has no code at the lagged block, so
    /// the snapshot takes the head — including a head below the margin, whose
    /// lagged block saturates at genesis.
    #[tokio::test]
    async fn snapshot_block_takes_the_head_when_the_contract_is_younger() {
        let (asserter, provider) = mocked();
        asserter.push_success(&U64::from(1_000));
        asserter.push_success(&Bytes::new());
        asserter.push_success(&U64::from(SNAPSHOT_LAG_MARGIN_BLOCKS - 1));
        asserter.push_success(&Bytes::new());
        let head = SharedHead::with_ttl(provider.clone(), Duration::ZERO, None);

        assert_eq!(
            snapshot_block(&provider, &head, Address::ZERO).await.ok(),
            Some(1_000)
        );
        assert_eq!(
            snapshot_block(&provider, &head, Address::ZERO).await.ok(),
            Some(SNAPSHOT_LAG_MARGIN_BLOCKS - 1)
        );
        assert_eq!(asserter.read_q().len(), 0);
    }
}
