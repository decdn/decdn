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
