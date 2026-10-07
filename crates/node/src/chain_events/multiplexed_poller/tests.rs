use super::*;
use crate::chain_events::resumable_watcher::{Checkpoint, ColdStart};
use crate::chain_events::shared_head::SharedHead;
use alloy::primitives::{Bytes, U64, address, b256};
use alloy::providers::ProviderBuilder;
use decdn_incentive::{CheckpointKey, KeyedCheckpointStore, StoreError};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Build a log matching `(addr, topic0)` at `block`, per the brief's
/// harness note: `alloy_primitives::Log::new_unchecked` plus the `Log`
/// wrapper's `Default` for every other field.
fn log_at(addr: Address, topic0: B256, block: u64) -> Log {
    Log {
        inner: alloy::primitives::Log::new_unchecked(addr, vec![topic0], Bytes::new()),
        block_number: Some(block),
        ..Default::default()
    }
}

/// A `LogSink` that records every `(address, topic0, block)` it applies,
/// optionally erroring on the N-th apply or on `on_tick_complete`.
/// `ErasedSink` reaches every scripted sink through the blanket impl.
#[derive(Default)]
struct ScriptedSink {
    applied: Vec<(Address, B256, Option<u64>)>,
    fail_apply_on: Option<usize>,
    fail_tick_complete: bool,
    tick_completes: usize,
    /// One entry per `on_recovered`, holding `tick_completes` as it stood at
    /// that moment — so a test can pin the ordering against the reconcile,
    /// not just the count.
    recovered_at: Vec<usize>,
}

impl LogSink for ScriptedSink {
    async fn apply(&mut self, log: Log) -> Result<()> {
        if self.fail_apply_on == Some(self.applied.len()) {
            anyhow::bail!("scripted sink failure");
        }
        let topic0 = log.topic0().copied().unwrap_or_default();
        self.applied.push((log.address(), topic0, log.block_number));
        Ok(())
    }

    async fn on_tick_complete(&mut self) -> Result<()> {
        self.tick_completes += 1;
        if self.fail_tick_complete {
            anyhow::bail!("scripted reconcile failure");
        }
        Ok(())
    }

    fn on_recovered(&mut self) {
        self.recovered_at.push(self.tick_completes);
    }
}

fn no_shutdown() -> CancellationToken {
    CancellationToken::new()
}

/// Assert a `build()` result succeeded and hand it back, without
/// `.expect()` (denied workspace-wide, tests included). A call site
/// destructures the `Ok` via `let Ok(x) = assert_built(built) else {
/// return };` — the preceding `assert!` already fails the test loudly, so
/// the `else` arm is unreachable in practice.
fn assert_built(built: Result<MultiplexedPoller>) -> Result<MultiplexedPoller> {
    assert!(
        built.is_ok(),
        "build must succeed: {}",
        built
            .as_ref()
            .err()
            .map(|e| format!("{e:#}"))
            .unwrap_or_default()
    );
    built
}

const ADDR_A: Address = address!("0x1111111111111111111111111111111111111111");
const ADDR_B: Address = address!("0x2222222222222222222222222222222222222222");
const TOPIC_A: B256 = b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
const TOPIC_B: B256 = b256!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

/// A route on `(addr, topic0)` starting live-from-head (no backfill),
/// wrapping a fresh `ScriptedSink`. The common shape most tests build on.
fn head_route(
    label: &'static str,
    addr: Address,
    topic0: B256,
) -> (Route, Arc<std::sync::Mutex<ScriptedSink>>) {
    seeded_route(
        label,
        addr,
        topic0,
        CursorStart::HeadMinusWindow { window_blocks: 0 },
    )
}

/// Records every delivery through an `Arc<Mutex<_>>` handle so tests can
/// assert on the sink after it has been boxed into the route.
struct MutexSink(Arc<std::sync::Mutex<ScriptedSink>>);

impl LogSink for MutexSink {
    async fn apply(&mut self, log: Log) -> Result<()> {
        let mut guard = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let fail = guard.fail_apply_on == Some(guard.applied.len());
        if fail {
            anyhow::bail!("scripted sink failure");
        }
        let topic0 = log.topic0().copied().unwrap_or_default();
        guard
            .applied
            .push((log.address(), topic0, log.block_number));
        Ok(())
    }

    async fn on_tick_complete(&mut self) -> Result<()> {
        let mut guard = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.tick_completes += 1;
        if guard.fail_tick_complete {
            anyhow::bail!("scripted reconcile failure");
        }
        Ok(())
    }

    fn on_recovered(&mut self) {
        let mut guard = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let seen = guard.tick_completes;
        guard.recovered_at.push(seen);
    }
}

fn seeded_route(
    label: &'static str,
    addr: Address,
    topic0: B256,
    start: CursorStart,
) -> (Route, Arc<std::sync::Mutex<ScriptedSink>>) {
    let recorded = Arc::new(std::sync::Mutex::new(ScriptedSink::default()));
    let route = Route {
        addresses: vec![addr],
        topic0s: vec![topic0],
        start,
        sink: SinkSource::Ready(Box::new(MutexSink(Arc::clone(&recorded)))),
        label,
        on_established: None,
        on_backoff: None,
        on_tick_success: None,
        on_task_panic: None,
    };
    (route, recorded)
}

// --- Step 2/3: ErasedSink blanket adapter --------------------------------

#[tokio::test]
async fn erased_sink_forwards_apply_and_tick_complete() {
    let mut sink: Box<dyn ErasedSink> = Box::new(ScriptedSink::default());
    let log = log_at(ADDR_A, TOPIC_A, 1);
    assert!(sink.apply(log).await.is_ok());
    assert!(sink.on_tick_complete().await.is_ok());

    // Downcasting isn't available (no `Any` bound), so drive counts
    // through a second, directly-observable sink instance instead.
    let recorded = Arc::new(std::sync::Mutex::new(ScriptedSink::default()));
    let mut erased: Box<dyn ErasedSink> = Box::new(MutexSink(Arc::clone(&recorded)));
    let applied = erased.apply(log_at(ADDR_A, TOPIC_A, 1)).await;
    assert!(
        applied.is_ok(),
        "apply must forward through the blanket impl: {applied:?}"
    );
    let completed = erased.on_tick_complete().await;
    assert!(
        completed.is_ok(),
        "on_tick_complete must forward through the blanket impl: {completed:?}"
    );
    let guard = recorded
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(
        guard.applied.len(),
        1,
        "apply must forward through the blanket impl"
    );
    assert_eq!(
        guard.tick_completes, 1,
        "on_tick_complete must forward through the blanket impl"
    );
    drop(guard);

    erased.on_recovered();
    let guard = recorded
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(
        guard.recovered_at.len(),
        1,
        "on_recovered must forward through the blanket impl"
    );
}

// --- Step 4/5: build() dup-key rejection + key_index -----------------------

#[test]
fn duplicate_route_key_is_a_build_error() {
    let head: Arc<dyn HeadSource> = Arc::new(StaticHead(100));
    let (route_a, _) = head_route("a", ADDR_A, TOPIC_A);
    let (route_b, _) = head_route("b", ADDR_A, TOPIC_A); // same (address, topic0)

    let built = MultiplexedPollerBuilder::new(head, Duration::from_secs(1))
        .route(route_a)
        .route(route_b)
        .build();
    assert!(
        built.is_err(),
        "two routes claiming the same (address, topic0) must fail to build"
    );
}

#[test]
fn distinct_route_keys_build_successfully() {
    let head: Arc<dyn HeadSource> = Arc::new(StaticHead(100));
    let (route_a, _) = head_route("a", ADDR_A, TOPIC_A);
    let (route_b, _) = head_route("b", ADDR_A, TOPIC_B); // same address, different topic0

    let built = MultiplexedPollerBuilder::new(head, Duration::from_secs(1))
        .route(route_a)
        .route(route_b)
        .build();
    assert!(
        built.is_ok(),
        "distinct (address, topic0) keys must build: {}",
        built.err().map(|e| format!("{e:#}")).unwrap_or_default()
    );
}

/// A `HeadSource` that always reports a fixed head — used where a test only
/// needs `build()` to succeed and never drives a tick.
struct StaticHead(u64);

#[async_trait]
impl HeadSource for StaticHead {
    async fn head(&self) -> Result<u64> {
        Ok(self.0)
    }
}

// --- Mocked-provider tick harness (mirrors resumable_watcher's) -----------

/// `SharedHead::with_ttl(.., Duration::ZERO, None)`: queued head/`get_logs`
/// responses are consumed in strict order (see `resumable_watcher.rs`'s
/// `tick_cfg` doc for why a non-zero TTL would desync the queue).
fn shared_head<P: Provider + 'static>(provider: P) -> Arc<dyn HeadSource> {
    Arc::new(SharedHead::with_ttl(provider, Duration::ZERO, None))
}

// --- Step 6/7: demux by (address, topic0) ----------------------------------

#[tokio::test]
async fn demux_routes_logs_by_address_and_topic0() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

    // Two routes on the SAME address, different topic0s (models
    // PaymentPool settlement + rate-bounds sharing one contract).
    let (route_a, sink_a) = head_route("a", ADDR_A, TOPIC_A);
    let (route_b, sink_b) = head_route("b", ADDR_A, TOPIC_B);
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(10)
            .route(route_a)
            .route(route_b)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    // head=1: both HeadMinusWindow{0} routes float to floor=1, one window
    // [1,1] carries one log per topic.
    asserter.push_success(&U64::from(1));
    asserter.push_success(&vec![
        log_at(ADDR_A, TOPIC_A, 1),
        log_at(ADDR_A, TOPIC_B, 1),
    ]);

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_ok(), "tick must succeed: {result:?}");

    let a = sink_a
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let b = sink_b
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(
        a.applied,
        vec![(ADDR_A, TOPIC_A, Some(1))],
        "route A gets only topic A"
    );
    assert_eq!(
        b.applied,
        vec![(ADDR_A, TOPIC_B, Some(1))],
        "route B gets only topic B"
    );
}

// --- Step 8: per-route floor gate -------------------------------------------

#[tokio::test]
async fn route_below_its_floor_ignores_old_logs() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

    // Route A floors at head (HeadMinusWindow{0}); route B seeds low, so
    // the merged scan range is dragged down to B's floor and includes
    // blocks below A's.
    let (route_a, sink_a) = head_route("a", ADDR_A, TOPIC_A);
    let (route_b, sink_b) = seeded_route("b", ADDR_B, TOPIC_B, CursorStart::Seeded { at: 0 });
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(10)
            .route(route_a)
            .route(route_b)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    // head=5: A floors at 5, B floors at 0 -> merged range [0,5], one
    // window. A log for A's key sits at block 2, below A's floor of 5.
    asserter.push_success(&U64::from(5));
    asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 2)]);

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_ok(), "tick must succeed: {result:?}");

    let a = sink_a
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let b = sink_b
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(
        a.applied.is_empty(),
        "route A must gate out a log below its own floor"
    );
    assert!(
        b.applied.is_empty(),
        "route B saw no matching log this tick"
    );
    drop(a);
    drop(b);
    assert_eq!(
        poller.routes.first().and_then(|r| r.cursor),
        Some(6),
        "route A's cursor still advances past the window despite gating the log"
    );
}

// --- Step 9: error isolation + sibling advance + idempotent re-scan --------

#[tokio::test]
async fn route_error_isolates_and_siblings_advance() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

    let (route_a, sink_a) = head_route("a", ADDR_A, TOPIC_A);
    let (route_b, sink_b) = head_route("b", ADDR_B, TOPIC_B);
    sink_a
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .fail_apply_on = Some(0);
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(10)
            .route(route_a)
            .route(route_b)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    // Tick 1: head=1, one window carrying a log for each route. A's sink
    // errors; B's succeeds.
    asserter.push_success(&U64::from(1));
    asserter.push_success(&vec![
        log_at(ADDR_A, TOPIC_A, 1),
        log_at(ADDR_B, TOPIC_B, 1),
    ]);
    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(
        result.is_err(),
        "a route error must fail the tick (drives loop backoff)"
    );
    assert_eq!(
        poller.routes.first().and_then(|r| r.cursor),
        Some(1),
        "route A holds its cursor at its floor after erroring"
    );
    assert_eq!(
        poller.routes.get(1).and_then(|r| r.cursor),
        Some(2),
        "route B advances past the window despite A's error"
    );
    assert!(
        sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .applied
            .is_empty(),
        "route A's failed apply is not recorded"
    );
    assert_eq!(
        sink_b
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .applied
            .len(),
        1,
        "route B's log applied despite A's sibling error"
    );

    // Tick 2 (retry): A no longer errors. Same head; the merged range is
    // still [1,1] (A's floor). A re-applies the window's log (idempotent
    // re-scan); B's floor has moved to 2, so it gates the same log out.
    sink_a
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .fail_apply_on = None;
    asserter.push_success(&U64::from(1));
    asserter.push_success(&vec![
        log_at(ADDR_A, TOPIC_A, 1),
        log_at(ADDR_B, TOPIC_B, 1),
    ]);
    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_ok(), "retry tick should complete: {result:?}");
    assert_eq!(
        sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .applied
            .len(),
        1,
        "route A re-applies the window's log on retry"
    );
    assert_eq!(
        sink_b
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .applied
            .len(),
        1,
        "route B does not re-apply — its floor already covers this range"
    );
}

/// A shared-read failure arms the recovery edge for every route.
///
/// `fail_whole_tick` returns before `notify_recovered_routes` and
/// `fire_route_hooks` ever run, so it is the *only* place the edge is armed
/// for a whole-RPC outage — the most common real one, and the one a
/// cadence-gated sink most needs the forced re-read after. Nothing else
/// covers this leg: a per-route apply failure takes an entirely different
/// path through `fire_route_hooks`.
#[tokio::test]
async fn a_whole_tick_failure_arms_the_recovery_edge_for_every_route() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

    let (route_a, sink_a) = head_route("a", ADDR_A, TOPIC_A);
    let (route_b, sink_b) = head_route("b", ADDR_B, TOPIC_B);
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .route(route_a)
            .route(route_b)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    // Tick 1: the shared head read fails, so no route errored individually.
    asserter.push_failure_msg("head is down");
    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_err(), "a head-read failure must fail the tick");

    // Tick 2: the RPC is back.
    asserter.push_success(&U64::from(1));
    asserter.push_success(&Vec::<Log>::new());
    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_ok(), "recovery tick should complete: {result:?}");

    for (label, sink) in [("a", &sink_a), ("b", &sink_b)] {
        let guard = sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            guard.recovered_at.len(),
            1,
            "route {label} must be told it recovered from a whole-tick failure, \
             or a cadence-gated sink waits out its full interval after the \
             outage that made it stale"
        );
        assert_eq!(
            guard.recovered_at.first().copied(),
            Some(0),
            "route {label}'s recovery must land before that tick's reconcile"
        );
    }
}

/// A route that recovers must be told, on the recovery tick and before that
/// tick's reconcile.
///
/// A sink whose authoritative re-read is cadence-gated repairs itself here.
/// The cadence alone is at its weakest in exactly this scenario: the
/// reconcile is skipped while the route is errored, so the repair does not
/// run during the outage at all, and a repair whose own read then fails
/// defers itself a further interval. A route that never errored must not be
/// told it recovered — its bootstrap read just ran.
#[tokio::test]
async fn a_recovered_route_is_notified_before_its_reconcile() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

    let (route_a, sink_a) = head_route("a", ADDR_A, TOPIC_A);
    let (route_b, sink_b) = head_route("b", ADDR_B, TOPIC_B);
    sink_a
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .fail_apply_on = Some(0);
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(10)
            .route(route_a)
            .route(route_b)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    // Tick 1: A's apply errors, B is clean.
    asserter.push_success(&U64::from(1));
    asserter.push_success(&vec![
        log_at(ADDR_A, TOPIC_A, 1),
        log_at(ADDR_B, TOPIC_B, 1),
    ]);
    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_err(), "a route error must fail the tick");
    assert!(
        sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recovered_at
            .is_empty(),
        "an errored route has not recovered yet"
    );

    // Tick 2: A comes back.
    sink_a
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .fail_apply_on = None;
    asserter.push_success(&U64::from(1));
    asserter.push_success(&vec![
        log_at(ADDR_A, TOPIC_A, 1),
        log_at(ADDR_B, TOPIC_B, 1),
    ]);
    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_ok(), "retry tick should complete: {result:?}");

    {
        let guard_a = sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            guard_a.recovered_at.len(),
            1,
            "the recovery edge fires exactly once"
        );
        // A's reconcile is skipped on the errored tick, so it has run once —
        // this tick's — by the end. The recovery must have been seen before it.
        assert_eq!(guard_a.tick_completes, 1);
        assert_eq!(
            guard_a.recovered_at.first().copied(),
            Some(0),
            "on_recovered must run before this tick's reconcile, so the sink can \
             force it to re-read now rather than a cadence later"
        );
    }

    assert!(
        sink_b
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recovered_at
            .is_empty(),
        "a route that never errored must not be told it recovered"
    );

    // Tick 3: A stays healthy, so the edge does not re-fire.
    asserter.push_success(&U64::from(1));
    asserter.push_success(&Vec::<Log>::new());
    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_ok(), "third tick should complete: {result:?}");
    assert_eq!(
        sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recovered_at
            .len(),
        1,
        "the edge is consumed, not re-fired on every healthy tick"
    );
}

// --- Step 10: one get_logs per tick for N routes ----------------------------

/// A JSON-RPC mock counting `eth_blockNumber` and `eth_getLogs` hits.
struct CountingRpc {
    head_hits: Arc<AtomicUsize>,
    getlogs_hits: Arc<AtomicUsize>,
}

impl wiremock::Respond for CountingRpc {
    fn respond(&self, req: &wiremock::Request) -> wiremock::ResponseTemplate {
        let body: serde_json::Value =
            serde_json::from_slice(&req.body).unwrap_or(serde_json::Value::Null);
        let id = body.get("id").cloned().unwrap_or(serde_json::json!(0));
        let result = match body.get("method").and_then(serde_json::Value::as_str) {
            Some("eth_blockNumber") => {
                // Strictly increasing head: every real RPC (i.e. every
                // TTL window, not every tick) advances the chain by one
                // block, so — unlike a static head, which idles after
                // tick 1 and would pass even an unmerged N-loop
                // regression — every poller tick has a non-empty merged
                // range and must issue a `get_logs`.
                let n = self.head_hits.fetch_add(1, Ordering::SeqCst);
                serde_json::json!(format!("0x{:x}", 100 + n))
            }
            Some("eth_getLogs") => {
                self.getlogs_hits.fetch_add(1, Ordering::SeqCst);
                serde_json::json!([])
            }
            _ => serde_json::json!("0x1"),
        };
        wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "jsonrpc": "2.0", "id": id, "result": result,
        }))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn n_routes_share_one_getlogs_per_tick() {
    const ROUTES: usize = 4;
    const INTERVAL: Duration = Duration::from_millis(100);
    const RUN_FOR: Duration = Duration::from_millis(350);

    let head_hits = Arc::new(AtomicUsize::new(0));
    let getlogs_hits = Arc::new(AtomicUsize::new(0));
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(CountingRpc {
            head_hits: Arc::clone(&head_hits),
            getlogs_hits: Arc::clone(&getlogs_hits),
        })
        .mount(&server)
        .await;
    let parsed_url = server.uri().parse();
    assert!(
        parsed_url.is_ok(),
        "mock server uri must parse: {}",
        parsed_url
            .as_ref()
            .err()
            .map(ToString::to_string)
            .unwrap_or_default()
    );
    let Ok(url) = parsed_url else { return };
    let provider = ProviderBuilder::new().connect_http(url);

    let head: Arc<dyn HeadSource> = Arc::new(SharedHead::new(provider.clone(), INTERVAL));
    let mut builder = MultiplexedPollerBuilder::new(head, INTERVAL).max_backfill_span(10_000);
    // 4 routes across 2 addresses, all HeadMinusWindow{0} (live tail).
    for (i, (addr, topic0)) in [
        (ADDR_A, TOPIC_A),
        (ADDR_A, TOPIC_B),
        (ADDR_B, TOPIC_A),
        (ADDR_B, TOPIC_B),
    ]
    .into_iter()
    .enumerate()
    {
        let (route, _) = head_route(
            Box::leak(format!("route-{i}").into_boxed_str()),
            addr,
            topic0,
        );
        builder = builder.route(route);
    }
    let Ok(poller) = assert_built(builder.build()) else {
        return;
    };

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(run(provider, poller, shutdown.clone()));
    tokio::time::sleep(RUN_FOR).await;
    shutdown.cancel();
    let _ = handle.await;

    let getlogs = getlogs_hits.load(Ordering::SeqCst);
    let heads = head_hits.load(Ordering::SeqCst);
    let per_ms = |d: Duration| d.as_millis().max(1);
    let ticks = (per_ms(RUN_FOR) / per_ms(INTERVAL)) as usize + 2;
    // The head advances by one block on every real `eth_blockNumber` RPC
    // (see `CountingRpc`), so — unlike a static head, under which every
    // tick after the first is idle and issues no `get_logs` at all, and
    // the old `< ROUTES * ticks` bound would pass even an unmerged
    // 4-loop regression — every tick here has a non-empty merged range
    // and must issue exactly one `get_logs`. A regression back to one
    // loop per route would issue up to `ROUTES` times as many; bounding
    // close to the tick count (rather than the much looser `ROUTES *
    // ticks`) is what actually discriminates merged from unmerged.
    assert!(
        getlogs <= ticks + 2,
        "merged polling must track ~1 get_logs per tick, not ROUTES per tick \
         (getlogs={getlogs}, ticks~={ticks}, heads={heads}, unmerged would approach {})",
        ROUTES * ticks
    );
    assert!(
        getlogs >= 1,
        "the poller must have issued at least one get_logs"
    );
}

// --- Step 11: idle tick reconciles all routes -------------------------------

#[tokio::test]
async fn idle_tick_reconciles_all_routes() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

    // Both routes seeded past head -> no get_logs issued this tick.
    let (route_a, sink_a) = seeded_route("a", ADDR_A, TOPIC_A, CursorStart::Seeded { at: 100 });
    let (route_b, sink_b) = seeded_route("b", ADDR_B, TOPIC_B, CursorStart::Seeded { at: 100 });
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .route(route_a)
            .route(route_b)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    asserter.push_success(&U64::from(5)); // head < both cursors: idle
    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_ok(), "idle tick must succeed: {result:?}");
    assert_eq!(
        sink_a
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tick_completes,
        1,
        "route A's on_tick_complete must fire on an idle tick"
    );
    assert_eq!(
        sink_b
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tick_completes,
        1,
        "route B's on_tick_complete must fire on an idle tick"
    );
}

// --- Step 12: shutdown flushes every persisting route -----------------------

#[derive(Default)]
struct FlushCountingStore {
    flushes: AtomicUsize,
}

impl KeyedCheckpointStore for FlushCountingStore {
    fn load_checkpoint(&self, _key: CheckpointKey) -> std::result::Result<Option<u64>, StoreError> {
        Ok(None)
    }
    fn record_checkpoint(
        &self,
        _key: CheckpointKey,
        _block: u64,
    ) -> std::result::Result<(), StoreError> {
        Ok(())
    }
    fn flush_checkpoint(&self, _key: CheckpointKey) -> std::result::Result<(), StoreError> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn cancelling_shutdown_flushes_every_persisting_route() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let store = Arc::new(FlushCountingStore::default());

    let (persisting, _) = seeded_route(
        "persisting",
        ADDR_A,
        TOPIC_A,
        CursorStart::FromCheckpoint {
            checkpoint: Checkpoint {
                store: Arc::clone(&store) as Arc<dyn KeyedCheckpointStore>,
                key: CheckpointKey::PoolOpened,
            },
            reorg_margin: 0,
            cold_start: ColdStart::Head,
        },
    );
    let (ephemeral, _) = head_route("ephemeral", ADDR_B, TOPIC_B); // no checkpoint: flush is a no-op
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .route(persisting)
            .route(ephemeral)
            .build();
    let Ok(poller) = assert_built(built) else {
        return;
    };

    let shutdown = CancellationToken::new();
    // Cancel before spawning: the loop's first act is the biased select on
    // the token, so the flush arm is taken deterministically.
    shutdown.cancel();
    let task = tokio::spawn(run(provider, poller, shutdown));
    assert!(task.await.is_ok(), "run must return, not hang or panic");
    assert_eq!(
        store.flushes.load(Ordering::SeqCst),
        1,
        "the persisting route must flush its checkpoint exactly once"
    );
}

// --- Step 13: head-read failure fails the tick + fires on_backoff for all --

#[tokio::test]
async fn head_read_failure_fails_the_tick_and_backs_off_every_route() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

    let backoff_a = Arc::new(AtomicUsize::new(0));
    let backoff_b = Arc::new(AtomicUsize::new(0));
    let (mut route_a, _) = head_route("a", ADDR_A, TOPIC_A);
    let (mut route_b, _) = head_route("b", ADDR_B, TOPIC_B);
    {
        let counter = Arc::clone(&backoff_a);
        route_a.on_backoff = Some(Box::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        }));
    }
    {
        let counter = Arc::clone(&backoff_b);
        route_b.on_backoff = Some(Box::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        }));
    }
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .route(route_a)
            .route(route_b)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    asserter.push_failure_msg("head is down");
    let err = run_tick(&provider, &mut poller, &no_shutdown())
        .await
        .err()
        .map(|e| format!("{e:#}"));
    assert!(
        err.as_ref().is_some_and(|e| e.contains("read head block")),
        "head failure must fail the tick with its context: {err:?}"
    );
    assert_eq!(
        backoff_a.load(Ordering::SeqCst),
        1,
        "route A must fire on_backoff"
    );
    assert_eq!(
        backoff_b.load(Ordering::SeqCst),
        1,
        "route B must fire on_backoff"
    );
}

// --- Step 14: panic in a route sink fires every route's on_task_panic ------

#[tokio::test]
#[allow(clippy::panic)] // deliberately panic inside a tick to exercise the guard.
async fn run_task_panic_fires_every_routes_panic_hook() {
    struct PanicOnApply;
    impl LogSink for PanicOnApply {
        async fn apply(&mut self, _log: Log) -> Result<()> {
            panic!("intentional tick panic");
        }
    }

    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

    let panicked_a = Arc::new(AtomicUsize::new(0));
    let panicked_b = Arc::new(AtomicUsize::new(0));
    let route_a = {
        let counter = Arc::clone(&panicked_a);
        Route {
            addresses: vec![ADDR_A],
            topic0s: vec![TOPIC_A],
            start: CursorStart::HeadMinusWindow { window_blocks: 0 },
            sink: SinkSource::Ready(Box::new(PanicOnApply)),
            label: "a",
            on_established: None,
            on_backoff: None,
            on_tick_success: None,
            on_task_panic: Some(Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            })),
        }
    };
    let (route_b, _) = head_route("b", ADDR_B, TOPIC_B);
    let route_b = Route {
        on_task_panic: Some({
            let counter = Arc::clone(&panicked_b);
            Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            })
        }),
        ..route_b
    };

    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .route(route_a)
            .route(route_b)
            .build();
    let Ok(poller) = assert_built(built) else {
        return;
    };

    asserter.push_success(&U64::from(1));
    asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 1)]);

    let shutdown = CancellationToken::new();
    let joined = tokio::spawn(run(provider, poller, shutdown)).await;
    assert!(joined.is_err(), "the poller task must have panicked");
    assert_eq!(
        panicked_a.load(Ordering::SeqCst),
        1,
        "the panicking route's own on_task_panic must fire"
    );
    assert_eq!(
        panicked_b.load(Ordering::SeqCst),
        1,
        "a sibling route's on_task_panic must ALSO fire — the whole task died"
    );
}

// --- Coverage: shutdown suppresses the established/backoff edges, not liveness ---
//
// Each pair below mirrors `resumable_watcher`'s
// `shutdown_suppresses_the_established_edge` /
// `shutdown_suppresses_the_backoff_edge`: a positive case that proves the
// hook *can* fire, and a negative case that isolates the
// `!shutdown.is_cancelled()` guard as the ONLY thing standing between an
// otherwise-identical tick and that hook firing. A test that cancels
// shutdown only on a SECOND tick (after the route is already established)
// is vacuous for the established edge — `!r.established` is already false
// by then, so the guard is never reached — and a test whose tick
// succeeds is vacuous for the backoff edge, since that hook only fires
// from the `errored` branch. Both negative cases below avoid that: the
// established case cancels before the route's very first tick (so
// `!r.established` is still true and only the shutdown guard withholds
// the fire), and the backoff case drives a genuinely failing tick (so
// `r.errored` is true and only the shutdown guard withholds the fire).

/// Positive case: an uncancelled first tick fires `on_established` once,
/// alongside the per-tick `on_tick_success` liveness stamp.
#[tokio::test]
async fn established_edge_fires_on_first_healthy_tick_when_not_cancelled() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

    let established = Arc::new(AtomicUsize::new(0));
    let tick_success = Arc::new(AtomicUsize::new(0));
    let (mut route_a, _) = head_route("a", ADDR_A, TOPIC_A);
    {
        let counter = Arc::clone(&established);
        route_a.on_established = Some(Box::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        }));
    }
    {
        let counter = Arc::clone(&tick_success);
        route_a.on_tick_success = Some(Box::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        }));
    }

    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(10)
            .route(route_a)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    asserter.push_success(&U64::from(1));
    asserter.push_success(&Vec::<Log>::new());
    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_ok(), "tick must succeed: {result:?}");
    assert_eq!(
        established.load(Ordering::SeqCst),
        1,
        "on_established must fire once on an uncancelled first healthy tick"
    );
    assert_eq!(
        tick_success.load(Ordering::SeqCst),
        1,
        "on_tick_success must fire on the same tick"
    );
}

/// Cancels a shared shutdown token from inside `on_tick_complete` —
/// i.e. strictly *after* the per-window boundary check (`run_tick`'s
/// `if shutdown.is_cancelled() { return Ok(()); }`, which only runs
/// between windows) but *before* `fire_route_hooks` reads the token.
/// Optionally fails that same call, so the cancellation and the route's
/// `errored` transition land in the same tick. Mirrors
/// `resumable_watcher.rs`'s `CancelOnNthTick` — cancelling *before*
/// calling `run_tick` at all would instead trip the window-boundary
/// check and return early, short-circuiting the tick before it ever
/// reaches reconcile or hook-firing (proven the hard way: an earlier
/// draft of these tests cancelled up front and both went vacuous the
/// other way, asserting on a tick that never ran far enough to prove
/// anything).
struct CancelInReconcile {
    shutdown: CancellationToken,
    bail: bool,
}

impl LogSink for CancelInReconcile {
    async fn apply(&mut self, _log: Log) -> Result<()> {
        Ok(())
    }
    async fn on_tick_complete(&mut self) -> Result<()> {
        self.shutdown.cancel();
        if self.bail {
            anyhow::bail!("cancelled mid-tick reconcile");
        }
        Ok(())
    }
}

/// Negative case: shutdown becomes cancelled *during* the route's very
/// first tick (from its `on_tick_complete`, run strictly before
/// `fire_route_hooks`), so `!r.established` is still `true` and the tick
/// itself succeeds — the ONLY thing that can withhold `on_established`
/// is the `!shutdown.is_cancelled()` guard. If that guard were deleted
/// this assertion would fail (the edge would fire), which is what makes
/// this non-vacuous, unlike a cancel-on-the-second-tick construction
/// (where the route is already established and the guard is never
/// reached) or a cancel-before-calling-`run_tick` construction (where
/// the window-boundary check returns early and no hook fires at all —
/// see `CancelInReconcile`'s doc). `on_tick_success` still fires — the
/// tick itself succeeds; only the edge-triggered readiness signal is
/// suppressed (fail-open guard).
#[tokio::test]
async fn shutdown_during_first_tick_suppresses_established_edge() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

    let established = Arc::new(AtomicUsize::new(0));
    let tick_success = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new(); // NOT cancelled yet
    let route_a = {
        let counter = Arc::clone(&established);
        let tick_counter = Arc::clone(&tick_success);
        Route {
            addresses: vec![ADDR_A],
            topic0s: vec![TOPIC_A],
            start: CursorStart::HeadMinusWindow { window_blocks: 0 },
            sink: SinkSource::Ready(Box::new(CancelInReconcile {
                shutdown: shutdown.clone(),
                bail: false,
            })),
            label: "a",
            on_established: Some(Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            })),
            on_backoff: None,
            on_tick_success: Some(Box::new(move || {
                tick_counter.fetch_add(1, Ordering::SeqCst);
            })),
            on_task_panic: None,
        }
    };

    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(10)
            .route(route_a)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    asserter.push_success(&U64::from(1));
    asserter.push_success(&Vec::<Log>::new());
    let result = run_tick(&provider, &mut poller, &shutdown).await;
    assert!(result.is_ok(), "the tick itself still succeeds: {result:?}");
    assert_eq!(
        established.load(Ordering::SeqCst),
        0,
        "on_established must be suppressed once shutdown is cancelled \
         mid-tick, even though the route was never established before"
    );
    assert_eq!(
        tick_success.load(Ordering::SeqCst),
        1,
        "on_tick_success still fires — liveness is unconditional"
    );
}

/// Positive case: an uncancelled tick whose sink genuinely errors fires
/// `on_backoff`.
#[tokio::test]
async fn backoff_edge_fires_on_a_failing_tick_when_not_cancelled() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

    let backoff = Arc::new(AtomicUsize::new(0));
    let (mut route_a, sink_a) = head_route("a", ADDR_A, TOPIC_A);
    sink_a
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .fail_apply_on = Some(0);
    {
        let counter = Arc::clone(&backoff);
        route_a.on_backoff = Some(Box::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
        }));
    }

    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(10)
            .route(route_a)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    asserter.push_success(&U64::from(1));
    asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 1)]);
    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_err(), "the route's sink error must fail the tick");
    assert_eq!(
        backoff.load(Ordering::SeqCst),
        1,
        "on_backoff must fire once on an uncancelled failing tick"
    );
}

/// Negative case: shutdown becomes cancelled *during* the same tick that
/// makes the route error (`CancelInReconcile { bail: true }` cancels the
/// token and fails `on_tick_complete` in one call), so `r.errored` is
/// genuinely `true` and the backoff branch IS entered — the ONLY thing
/// that can withhold `on_backoff` is the `!shutdown.is_cancelled()`
/// guard. If that guard were deleted this assertion would fail (the edge
/// would fire), unlike a construction whose tick never actually errors
/// (where the backoff branch is never reached regardless of the guard)
/// or one that cancels before calling `run_tick` (which trips the
/// window-boundary check and returns early before the sink — and so the
/// error — is ever reached; see `CancelInReconcile`'s doc).
#[tokio::test]
async fn shutdown_during_failing_tick_suppresses_backoff_edge() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

    let backoff = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new(); // NOT cancelled yet
    let route_a = {
        let counter = Arc::clone(&backoff);
        Route {
            addresses: vec![ADDR_A],
            topic0s: vec![TOPIC_A],
            start: CursorStart::HeadMinusWindow { window_blocks: 0 },
            sink: SinkSource::Ready(Box::new(CancelInReconcile {
                shutdown: shutdown.clone(),
                bail: true,
            })),
            label: "a",
            on_established: None,
            on_backoff: Some(Box::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            })),
            on_tick_success: None,
            on_task_panic: None,
        }
    };

    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(10)
            .route(route_a)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    asserter.push_success(&U64::from(1));
    asserter.push_success(&Vec::<Log>::new());
    let result = run_tick(&provider, &mut poller, &shutdown).await;
    assert!(
        result.is_err(),
        "the route still errors this tick — shutdown suppresses only the \
         hook, not the tick's Err outcome: {result:?}"
    );
    assert_eq!(
        backoff.load(Ordering::SeqCst),
        0,
        "on_backoff must be suppressed once shutdown is cancelled mid-tick, \
         even though the route genuinely errored"
    );
}

// --- Coverage: a None block_number applies rather than being gated ---------

/// The floor gate is `log.block_number.is_some_and(|b| b < tick_floor)`:
/// a `None` block makes `is_some_and` false, so the log proceeds to
/// `apply` rather than being silently dropped. `log_at` always sets
/// `Some`, so this builds the log directly to exercise the `None` arm.
#[tokio::test]
async fn none_block_number_log_is_applied_not_gated() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());

    let (route_a, sink_a) = head_route("a", ADDR_A, TOPIC_A);
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(10)
            .route(route_a)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    let mut log = log_at(ADDR_A, TOPIC_A, 1);
    log.block_number = None; // e.g. a pending-tag response shape

    asserter.push_success(&U64::from(1));
    asserter.push_success(&vec![log]);

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_ok(), "tick must succeed: {result:?}");

    let a = sink_a
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(
        a.applied,
        vec![(ADDR_A, TOPIC_A, None)],
        "a log with no block_number must be applied, not silently dropped by the floor gate"
    );
}

// --- Coverage: a later-window error holds the cursor at that window's start ---

/// Minimal in-memory durable store for this test — mirrors
/// `resumable_watcher.rs`'s test-only `MemoryCheckpointStore` (private to
/// that module, so duplicated here rather than reused).
#[derive(Default)]
struct MemoryCheckpointStore {
    stored: std::sync::Mutex<std::collections::HashMap<CheckpointKey, u64>>,
}

impl KeyedCheckpointStore for MemoryCheckpointStore {
    fn load_checkpoint(&self, key: CheckpointKey) -> std::result::Result<Option<u64>, StoreError> {
        Ok(self
            .stored
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .copied())
    }

    fn record_checkpoint(
        &self,
        key: CheckpointKey,
        block: u64,
    ) -> std::result::Result<(), StoreError> {
        self.stored
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key, block);
        Ok(())
    }
}

/// A route whose floor sits several windows behind head must, on a
/// mid-backfill sink error, hold its cursor at the *failed* window's
/// start — not the original floor, and not an un-scanned later window —
/// while the windows that already completed stay advanced and persisted.
/// Mirrors `resumable_watcher.rs`'s
/// `sink_error_leaves_cursor_and_checkpoint_at_last_completed_window`,
/// carried over to the per-route `tick_floor`/`errored` machinery.
#[tokio::test]
async fn multi_window_error_holds_cursor_at_failed_windows_start() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let store = Arc::new(MemoryCheckpointStore::default());
    // Pre-recorded cursor 1 with a zero reorg margin: the first tick's floor
    // resolves to block 1, giving the three-window script below. The memory
    // store's record is infallible; assert rather than expect (anti-panic lint).
    assert!(
        store
            .record_checkpoint(CheckpointKey::PoolOpened, 1)
            .is_ok()
    );

    let (route_a, sink_a) = seeded_route(
        "a",
        ADDR_A,
        TOPIC_A,
        CursorStart::FromCheckpoint {
            checkpoint: Checkpoint {
                store: Arc::clone(&store) as Arc<dyn KeyedCheckpointStore>,
                key: CheckpointKey::PoolOpened,
            },
            reorg_margin: 0,
            cold_start: ColdStart::Head,
        },
    );
    // Fail on the 2nd apply (0-indexed): window [1,1] succeeds, window
    // [2,2] fails, window [3,3] is never applied (route already errored).
    sink_a
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .fail_apply_on = Some(1);

    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(1)
            .route(route_a)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    // floor=1, head=3, span=1 -> three windows: [1,1], [2,2], [3,3]. All
    // three windows are scanned in this one tick (the window set is fixed
    // from the tick's start floor before any route errors), so all three
    // `get_logs` responses are queued regardless of the mid-tick error.
    asserter.push_success(&U64::from(3));
    asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 1)]);
    asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 2)]);
    asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 3)]);

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_err(), "the failed window must fail the tick");

    assert_eq!(
        poller.routes.first().and_then(|r| r.cursor),
        Some(2),
        "cursor holds at the failed window's start (2): past the completed \
         window [1,1] but not into the un-scanned window [3,3]"
    );
    let persisted = store
        .load_checkpoint(CheckpointKey::PoolOpened)
        .ok()
        .flatten();
    assert_eq!(
        persisted,
        Some(1),
        "only the completed window [1,1] is durably persisted"
    );
    let applied = sink_a
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .applied
        .len();
    assert_eq!(
        applied, 1,
        "only window [1,1]'s log applied; the failed and un-scanned windows did not"
    );
}

// --- Provider range caps: the window shrinks on a range rejection ------------

fn error_payload(code: i64, message: &str) -> Option<alloy_json_rpc::ErrorPayload> {
    serde_json::from_value(serde_json::json!({ "code": code, "message": message })).ok()
}

fn rpc_error(code: i64, message: &str) -> anyhow::Error {
    error_payload(code, message).map_or_else(
        || anyhow::anyhow!("unbuildable error payload"),
        |payload| {
            anyhow::Error::new(TransportError::ErrorResp(payload))
                .context("multiplexed get_logs [1, 20]")
        },
    )
}

#[test]
fn range_rejection_matches_provider_range_and_result_caps() {
    // dRPC's free tier rejects ranges above ~100-178 blocks, whatever the text says.
    assert!(is_range_rejection(&rpc_error(
        35,
        "ranges over 10000 blocks are not supported on free plan"
    )));
    // Alchemy free tier.
    assert!(is_range_rejection(&rpc_error(
        -32600,
        "Under the Free tier plan, you can make eth_getLogs requests with up to a 10 \
         block range."
    )));
    // Infura result cap: -32005 is also its rate-limit code.
    assert!(is_range_rejection(&rpc_error(
        -32005,
        "query returned more than 10000 results"
    )));
    // Ankr and Chainstack.
    assert!(is_range_rejection(&rpc_error(
        -32600,
        "block range is too wide"
    )));
    assert!(is_range_rejection(&rpc_error(
        -32000,
        "Block range limit exceeded"
    )));
    // geth / Erigon result cap.
    assert!(is_range_rejection(&rpc_error(
        -32000,
        "query exceeds max results 20000"
    )));
    // QuickNode.
    assert!(is_range_rejection(&rpc_error(
        -32614,
        "eth_getLogs is limited to a 10,000 range"
    )));
}

#[test]
fn range_rejection_ignores_every_other_failure() {
    // Rate limits back off; they never shrink the window.
    assert!(!is_range_rejection(&rpc_error(429, "Too Many Requests")));
    assert!(!is_range_rejection(&rpc_error(
        -32005,
        "project ID request rate exceeded"
    )));
    // A lagging load-balanced backend: a smaller window is not the fix.
    assert!(!is_range_rejection(&rpc_error(
        -32000,
        "block number is out of range"
    )));
    assert!(!is_range_rejection(&rpc_error(-32000, "unknown block")));
    assert!(!is_range_rejection(&rpc_error(-32603, "internal error")));
    // Head-lag range wording without a limit word.
    assert!(!is_range_rejection(&rpc_error(
        -32000,
        "block range extends beyond current head block"
    )));
    assert!(!is_range_rejection(&rpc_error(
        -32602,
        "invalid block range params"
    )));
    // "results" alone is not a result limit.
    assert!(!is_range_rejection(&rpc_error(
        -32000,
        "failed to marshal results"
    )));
    // Not a JSON-RPC error response at all.
    assert!(!is_range_rejection(&anyhow::Error::new(
        alloy::transports::TransportErrorKind::custom_str("connection reset")
    )));
    assert!(!is_range_rejection(&anyhow::anyhow!(
        "get_logs timed out after 10s"
    )));
}

/// A route resuming from a checkpoint at `from`, so the first tick backfills
/// `[from, head]`.
fn checkpoint_route(
    from: u64,
) -> (
    Route,
    Arc<std::sync::Mutex<ScriptedSink>>,
    Arc<MemoryCheckpointStore>,
) {
    let store = Arc::new(MemoryCheckpointStore::default());
    assert!(
        store
            .record_checkpoint(CheckpointKey::PoolOpened, from)
            .is_ok()
    );
    let (route, sink) = seeded_route(
        "a",
        ADDR_A,
        TOPIC_A,
        CursorStart::FromCheckpoint {
            checkpoint: Checkpoint {
                store: Arc::clone(&store) as Arc<dyn KeyedCheckpointStore>,
                key: CheckpointKey::PoolOpened,
            },
            reorg_margin: 0,
            cold_start: ColdStart::Head,
        },
    );
    (route, sink, store)
}

#[tokio::test]
async fn a_range_rejection_halves_the_window_and_the_tick_completes() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let (route, _sink, store) = checkpoint_route(1);
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .route(route)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    // head=20, default span: [1, 20] is rejected, then [1, 10] and [11, 20].
    asserter.push_success(&U64::from(20));
    if let Some(payload) = error_payload(
        -32600,
        "you can make eth_getLogs requests with up to a 10 block range",
    ) {
        asserter.push_failure(payload);
    }
    asserter.push_success(&Vec::<Log>::new());
    asserter.push_success(&Vec::<Log>::new());

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(
        result.is_ok(),
        "the halved windows must complete the tick: {result:?}"
    );
    assert_eq!(
        poller.span.current(),
        10,
        "the span is halved from the 20-block window"
    );
    assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(21));
    assert_eq!(
        store
            .load_checkpoint(CheckpointKey::PoolOpened)
            .ok()
            .flatten(),
        Some(20)
    );
}

/// A result cap that trips on a later, dense window after earlier windows
/// already applied and persisted: the retry resumes at the rejected
/// window's start, so no log is skipped or applied twice.
#[tokio::test]
async fn a_mid_backfill_rejection_retries_from_the_rejected_window() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let (route, sink, store) = checkpoint_route(1);
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(10)
            .route(route)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    // head=30, span 10: [1,10] ok, [11,20] rejected, then span 5:
    // [11,15], [16,20], [21,25], [26,30].
    asserter.push_success(&U64::from(30));
    asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 5)]);
    if let Some(payload) = error_payload(-32005, "query returned more than 10000 results") {
        asserter.push_failure(payload);
    }
    asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 12)]);
    asserter.push_success(&vec![log_at(ADDR_A, TOPIC_A, 18)]);
    asserter.push_success(&Vec::<Log>::new());
    asserter.push_success(&Vec::<Log>::new());

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(
        result.is_ok(),
        "the retried windows must complete the tick: {result:?}"
    );
    assert_eq!(poller.span.current(), 5);
    let applied: Vec<Option<u64>> = sink
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .applied
        .iter()
        .map(|(_, _, block)| *block)
        .collect();
    assert_eq!(
        applied,
        vec![Some(5), Some(12), Some(18)],
        "each log applied exactly once, in order"
    );
    assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(31));
    assert_eq!(
        store
            .load_checkpoint(CheckpointKey::PoolOpened)
            .ok()
            .flatten(),
        Some(30)
    );
}

/// The span hook sees every shrink and regrow and the rejection hook counts
/// every range rejection: they drive `decdn_chain_get_logs_span` and
/// `decdn_chain_get_logs_range_rejections_total`.
#[tokio::test]
async fn span_hooks_report_the_shrink_and_the_regrow() {
    use crate::chain_events::backfill::SPAN_REGROW_AFTER_WINDOWS;
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let (route, _sink, _store) = checkpoint_route(1);
    let spans = Arc::new(std::sync::Mutex::new(Vec::new()));
    let rejections = Arc::new(AtomicUsize::new(0));
    let spans_hook = Arc::clone(&spans);
    let rejections_hook = Arc::clone(&rejections);
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(20)
            .on_span_change(Box::new(move |span| {
                spans_hook
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(span);
            }))
            .on_range_rejection(Box::new(move || {
                rejections_hook.fetch_add(1, Ordering::SeqCst);
            }))
            .route(route)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    // [1, 20] is rejected (span 10), then 32 full 10-block windows regrow it
    // to the 20-block ceiling on the last one.
    let windows = u64::from(SPAN_REGROW_AFTER_WINDOWS);
    asserter.push_success(&U64::from(windows * 10));
    if let Some(payload) = error_payload(35, "ranges over 10000 blocks are not supported") {
        asserter.push_failure(payload);
    }
    for _ in 0..windows {
        asserter.push_success(&Vec::<Log>::new());
    }

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_ok(), "the tick must complete: {result:?}");
    assert_eq!(rejections.load(Ordering::SeqCst), 1);
    assert_eq!(
        spans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
        vec![10, 20],
        "one shrink to 10, then one regrow back to the ceiling"
    );
    assert_eq!(poller.span.current(), 20);

    // Tick 2: the provider rejects the regrown 20-block window again. The
    // hooks report the shrink back to 10 and count the second rejection.
    asserter.push_success(&U64::from(windows * 10 + 40));
    if let Some(payload) = error_payload(35, "ranges over 10000 blocks are not supported") {
        asserter.push_failure(payload);
    }
    for _ in 0..4 {
        asserter.push_success(&Vec::<Log>::new());
    }
    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_ok(), "the second tick must complete: {result:?}");
    assert_eq!(rejections.load(Ordering::SeqCst), 2);
    assert_eq!(
        spans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
        vec![10, 20, 10],
    );
}

/// `run` reports the starting span before its first tick, so the gauge
/// reads the ceiling on a node that never sees a rejection.
#[tokio::test]
async fn run_reports_the_starting_span() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let spans = Arc::new(std::sync::Mutex::new(Vec::new()));
    let spans_hook = Arc::clone(&spans);
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(150)
            .on_span_change(Box::new(move |span| {
                spans_hook
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(span);
            }))
            .build();
    let Ok(poller) = assert_built(built) else {
        return;
    };
    // An already-cancelled token: `run` reports, then returns without a tick.
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    run(provider, poller, shutdown).await;
    assert_eq!(
        spans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
        vec![150]
    );
}

/// A rate limit is not retried in place: it fails the tick into the loop's
/// backoff at once, and the window span stays.
#[tokio::test]
async fn a_rate_limit_backs_off_and_keeps_the_window() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let Some(RetryPoller {
        mut poller,
        backoff,
        retries,
        ..
    }) = retry_poller(provider.clone(), DEFAULT_GET_LOGS_MAX_BLOCK_SPAN)
    else {
        return;
    };

    asserter.push_success(&U64::from(20));
    push_rpc_failure(&asserter, 429, "Too Many Requests");
    // A retry would take this success and complete the tick.
    asserter.push_success(&Vec::<Log>::new());

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(
        result.is_err(),
        "a rate limit fails the tick into the backoff path"
    );
    assert_eq!(retries.load(Ordering::SeqCst), 0);
    assert_eq!(backoff.load(Ordering::SeqCst), 1);
    assert_eq!(poller.span.current(), DEFAULT_GET_LOGS_MAX_BLOCK_SPAN);
    assert_eq!(
        poller.routes.first().and_then(|r| r.cursor),
        Some(1),
        "cursor held"
    );
}

#[tokio::test]
async fn a_rejected_one_block_window_fails_the_tick_instead_of_looping() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let (route, _sink, _store) = checkpoint_route(1);
    let rejections = Arc::new(AtomicUsize::new(0));
    let rejections_hook = Arc::clone(&rejections);
    let built =
        MultiplexedPollerBuilder::new(shared_head(provider.clone()), Duration::from_secs(1))
            .max_backfill_span(1)
            .on_range_rejection(Box::new(move || {
                rejections_hook.fetch_add(1, Ordering::SeqCst);
            }))
            .route(route)
            .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    asserter.push_success(&U64::from(1));
    if let Some(payload) = error_payload(35, "ranges over 10000 blocks are not supported") {
        asserter.push_failure(payload);
    }

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(
        result.is_err(),
        "a one-block window cannot shrink: back off"
    );
    assert_eq!(poller.span.current(), 1);
    assert_eq!(
        rejections.load(Ordering::SeqCst),
        1,
        "a provider that rejects even one block is still counted"
    );
}

// --- Transient window failures: the window retries inside the tick -------

/// A poller over one checkpoint route resuming at block 1, with the route's
/// `on_backoff` and `on_tick_success` and the poller's `on_window_retry`,
/// `on_window_deferred` and `on_range_rejection` counted.
struct RetryPoller {
    poller: MultiplexedPoller,
    backoff: Arc<AtomicUsize>,
    ticks: Arc<AtomicUsize>,
    retries: Arc<AtomicUsize>,
    deferred: Arc<AtomicUsize>,
    rejections: Arc<AtomicUsize>,
    store: Arc<MemoryCheckpointStore>,
}

fn counting_hook(counter: &Arc<AtomicUsize>) -> WatcherHook {
    let counter = Arc::clone(counter);
    Box::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    })
}

fn retry_poller<P: Provider + Clone + 'static>(provider: P, span: u64) -> Option<RetryPoller> {
    let (mut route, _sink, store) = checkpoint_route(1);
    let backoff = Arc::new(AtomicUsize::new(0));
    let ticks = Arc::new(AtomicUsize::new(0));
    let retries = Arc::new(AtomicUsize::new(0));
    let deferred = Arc::new(AtomicUsize::new(0));
    let rejections = Arc::new(AtomicUsize::new(0));
    route.on_backoff = Some(counting_hook(&backoff));
    route.on_tick_success = Some(counting_hook(&ticks));
    let built = MultiplexedPollerBuilder::new(shared_head(provider), Duration::from_secs(1))
        .max_backfill_span(span)
        .on_window_retry(counting_hook(&retries))
        .on_window_deferred(counting_hook(&deferred))
        .on_range_rejection(counting_hook(&rejections))
        .route(route)
        .build();
    assert_built(built).ok().map(|poller| RetryPoller {
        poller,
        backoff,
        ticks,
        retries,
        deferred,
        rejections,
        store,
    })
}

fn push_rpc_failure(asserter: &alloy::providers::mock::Asserter, code: i64, message: &str) {
    if let Some(payload) = error_payload(code, message) {
        asserter.push_failure(payload);
    }
}

#[test]
fn transient_window_errors_exclude_range_permanent_and_rate_limit() {
    // dRPC free plan: a lagging upstream at the tip, and a slow call.
    assert!(is_transient_window_error(&rpc_error(
        19,
        "Temporary internal error. Please retry"
    )));
    assert!(is_transient_window_error(&rpc_error(
        30,
        "Request timeout on the free plan"
    )));
    assert!(is_transient_window_error(&rpc_error(
        -32603,
        "internal error"
    )));
    assert!(is_transient_window_error(&anyhow::Error::new(
        alloy::transports::TransportErrorKind::custom_str("connection reset")
    )));
    assert!(is_transient_window_error(&anyhow::anyhow!(
        "get_logs timed out after 10s"
    )));
    // A load-balanced backend that lags head: range wording, no limit.
    assert!(is_transient_window_error(&rpc_error(
        -32000,
        "block range extends beyond current head block"
    )));
    assert!(is_transient_window_error(&rpc_error(
        -32000,
        "block number is out of range"
    )));
    assert!(is_transient_window_error(&anyhow::Error::new(
        alloy::transports::TransportErrorKind::http_error(503, String::new())
    )));
    // A range rejection shrinks the window instead.
    assert!(!is_transient_window_error(&rpc_error(
        35,
        "ranges over 10000 blocks are not supported on free plan"
    )));
    // A deterministic JSON-RPC error fails the same way on every retry.
    assert!(!is_transient_window_error(&rpc_error(
        -32602,
        "invalid params"
    )));
    assert!(!is_transient_window_error(&rpc_error(
        -32601,
        "the method eth_getLogs does not exist"
    )));
    assert!(!is_transient_window_error(&anyhow::Error::new(
        alloy::transports::TransportErrorKind::http_error(401, "invalid API key".into())
    )));
    // A rate limit goes to the loop's backoff.
    assert!(!is_transient_window_error(&rpc_error(
        429,
        "Too Many Requests"
    )));
    assert!(!is_transient_window_error(&rpc_error(
        -32005,
        "project ID request rate exceeded"
    )));
    assert!(!is_transient_window_error(&anyhow::Error::new(
        alloy::transports::TransportErrorKind::http_error(429, String::new())
    )));
    assert!(!is_transient_window_error(&anyhow::Error::new(
        alloy::transports::TransportErrorKind::http_error_with_retry_after(
            503,
            String::new(),
            Some(Duration::from_secs(5)),
        )
    )));
}

/// A window that fails once with a transient error and then succeeds
/// completes the tick: no route is marked down.
#[tokio::test(start_paused = true)]
async fn a_transient_window_error_retries_and_the_tick_succeeds() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let Some(RetryPoller {
        mut poller,
        backoff,
        retries,
        store,
        ..
    }) = retry_poller(provider.clone(), DEFAULT_GET_LOGS_MAX_BLOCK_SPAN)
    else {
        return;
    };

    asserter.push_success(&U64::from(20));
    push_rpc_failure(&asserter, 19, "Temporary internal error. Please retry");
    asserter.push_success(&Vec::<Log>::new());

    let started = tokio::time::Instant::now();
    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(
        result.is_ok(),
        "the retried window completes the tick: {result:?}"
    );
    assert!(
        started.elapsed() >= GET_LOGS_WINDOW_RETRY_DELAY,
        "the retry waits out the delay first"
    );
    assert_eq!(retries.load(Ordering::SeqCst), 1);
    assert_eq!(
        backoff.load(Ordering::SeqCst),
        0,
        "a retried window is not downtime"
    );
    assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(21));
    assert_eq!(
        store
            .load_checkpoint(CheckpointKey::PoolOpened)
            .ok()
            .flatten(),
        Some(20)
    );
    assert!(
        poller.routes.iter().all(|r| r.established && !r.recovering),
        "a retried window leaves the route established"
    );
}

/// A window that fails on every attempt is deferred after
/// `GET_LOGS_WINDOW_RETRIES` retries: the tick ends without an error, the
/// cursor holds at the window's start, and no route goes down or earns a
/// tick stamp.
#[tokio::test(start_paused = true)]
async fn a_window_that_keeps_failing_defers_the_tick() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let Some(RetryPoller {
        mut poller,
        backoff,
        ticks,
        retries,
        deferred,
        store,
        ..
    }) = retry_poller(provider.clone(), DEFAULT_GET_LOGS_MAX_BLOCK_SPAN)
    else {
        return;
    };

    asserter.push_success(&U64::from(20));
    for _ in 0..3 {
        push_rpc_failure(&asserter, 30, "Request timeout on the free plan");
    }

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(
        result.is_ok(),
        "a deferred window does not fail the tick: {result:?}"
    );
    assert_eq!(retries.load(Ordering::SeqCst), 2);
    assert_eq!(deferred.load(Ordering::SeqCst), 1);
    assert_eq!(
        backoff.load(Ordering::SeqCst),
        0,
        "a deferral is not downtime"
    );
    assert_eq!(ticks.load(Ordering::SeqCst), 0, "no window completed");
    assert_eq!(
        poller.routes.first().and_then(|r| r.cursor),
        Some(1),
        "the cursor holds at the deferred window's start"
    );
    assert_eq!(
        store
            .load_checkpoint(CheckpointKey::PoolOpened)
            .ok()
            .flatten(),
        Some(1),
        "the checkpoint holds where it was"
    );
    assert!(
        poller.routes.iter().all(|r| !r.recovering),
        "no route is marked down"
    );
    assert!(poller.stalled_since.is_some(), "the stall clock starts");
}

/// Deferred ticks that do not reach head for `GET_LOGS_STALL_BUDGET` fail
/// the tick: the error names the stall, and every route backs off. A failed
/// tick does not restart the clock, so the next deferral fails at once, and
/// only a tick that reaches head clears the stall and recovers the routes.
#[tokio::test(start_paused = true)]
async fn a_stall_past_the_budget_fails_the_tick() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let Some(RetryPoller {
        mut poller,
        backoff,
        deferred,
        ..
    }) = retry_poller(provider.clone(), DEFAULT_GET_LOGS_MAX_BLOCK_SPAN)
    else {
        return;
    };

    for _ in 0..3 {
        asserter.push_success(&U64::from(20));
        for _ in 0..3 {
            push_rpc_failure(&asserter, 19, "Temporary internal error. Please retry");
        }
    }
    asserter.push_success(&U64::from(20));
    asserter.push_success(&Vec::<Log>::new());

    let first = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(first.is_ok(), "the first stalled tick defers: {first:?}");
    tokio::time::advance(GET_LOGS_STALL_BUDGET).await;
    let second = run_tick(&provider, &mut poller, &no_shutdown()).await;
    let err = second.err().map(|e| format!("{e:#}"));
    assert!(
        err.as_ref()
            .is_some_and(|e| e.contains("no poll tick reached head")),
        "a stall past the budget fails the tick: {err:?}"
    );
    assert_eq!(deferred.load(Ordering::SeqCst), 2);
    assert_eq!(backoff.load(Ordering::SeqCst), 1);
    assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(1));
    assert!(
        poller.routes.iter().all(|r| r.recovering),
        "every route is marked down"
    );

    tokio::time::advance(Duration::from_secs(1)).await;
    let third = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(third.is_err(), "the next deferral fails at once: {third:?}");
    assert_eq!(backoff.load(Ordering::SeqCst), 2);

    tokio::time::advance(Duration::from_secs(1)).await;
    let fourth = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(
        fourth.is_ok(),
        "a tick that reaches head succeeds: {fourth:?}"
    );
    assert!(
        poller.stalled_since.is_none(),
        "reaching head clears the stall"
    );
    assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(21));
    assert!(
        poller.routes.iter().all(|r| r.established && !r.recovering),
        "every route recovers"
    );
}

/// Ticks that each complete a window but stop short of head do not clear
/// the stall clock: past the budget the tick fails, with the completed
/// window already persisted. Sparse progress cannot hide a growing lag.
#[tokio::test(start_paused = true)]
async fn progress_short_of_head_does_not_restart_the_stall_clock() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let Some(RetryPoller {
        mut poller,
        backoff,
        store,
        ..
    }) = retry_poller(provider.clone(), 10)
    else {
        return;
    };

    // Tick 1: [1,10] completes, [11,20] is deferred. Tick 2, past the
    // budget: [11,20] completes, [21,30] is deferred and the tick fails.
    asserter.push_success(&U64::from(20));
    asserter.push_success(&Vec::<Log>::new());
    for _ in 0..3 {
        push_rpc_failure(&asserter, 19, "Temporary internal error. Please retry");
    }
    asserter.push_success(&U64::from(40));
    asserter.push_success(&Vec::<Log>::new());
    for _ in 0..3 {
        push_rpc_failure(&asserter, 19, "Temporary internal error. Please retry");
    }

    let first = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(first.is_ok(), "tick 1 defers after progress: {first:?}");
    tokio::time::advance(GET_LOGS_STALL_BUDGET).await;
    let second = run_tick(&provider, &mut poller, &no_shutdown()).await;
    let err = second.err().map(|e| format!("{e:#}"));
    assert!(
        err.as_ref()
            .is_some_and(|e| e.contains("no poll tick reached head")),
        "progress short of head still fails past the budget: {err:?}"
    );
    assert_eq!(backoff.load(Ordering::SeqCst), 1);
    assert_eq!(
        poller.routes.first().and_then(|r| r.cursor),
        Some(21),
        "the window completed before the failure stays applied"
    );
    assert_eq!(
        store
            .load_checkpoint(CheckpointKey::PoolOpened)
            .ok()
            .flatten(),
        Some(20)
    );
}

/// A tick that reaches head clears the stall clock, so a later deferral
/// starts a fresh budget.
#[tokio::test(start_paused = true)]
async fn reaching_head_restarts_the_stall_clock() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let Some(RetryPoller {
        mut poller,
        backoff,
        deferred,
        ..
    }) = retry_poller(provider.clone(), DEFAULT_GET_LOGS_MAX_BLOCK_SPAN)
    else {
        return;
    };

    // Tick 1 defers [1, 20]; tick 2 completes it; tick 3 defers [21, 40].
    asserter.push_success(&U64::from(20));
    for _ in 0..3 {
        push_rpc_failure(&asserter, 19, "Temporary internal error. Please retry");
    }
    asserter.push_success(&U64::from(20));
    asserter.push_success(&Vec::<Log>::new());
    asserter.push_success(&U64::from(40));
    for _ in 0..3 {
        push_rpc_failure(&asserter, 19, "Temporary internal error. Please retry");
    }

    let first = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(first.is_ok(), "tick 1 defers: {first:?}");
    tokio::time::advance(Duration::from_secs(1)).await;
    let second = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(second.is_ok(), "tick 2 completes: {second:?}");
    assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(21));
    assert!(
        poller.stalled_since.is_none(),
        "reaching head clears the stall clock"
    );
    tokio::time::advance(GET_LOGS_STALL_BUDGET).await;
    let third = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(third.is_ok(), "tick 3 starts a fresh budget: {third:?}");
    assert_eq!(deferred.load(Ordering::SeqCst), 2);
    assert_eq!(backoff.load(Ordering::SeqCst), 0);
}

/// A deterministic JSON-RPC error fails the tick at once.
#[tokio::test(start_paused = true)]
async fn a_permanent_window_error_is_not_retried() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let Some(RetryPoller {
        mut poller,
        backoff,
        retries,
        ..
    }) = retry_poller(provider.clone(), DEFAULT_GET_LOGS_MAX_BLOCK_SPAN)
    else {
        return;
    };

    asserter.push_success(&U64::from(20));
    push_rpc_failure(&asserter, -32602, "invalid params");

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    let err = result.err().map(|e| format!("{e:#}"));
    assert!(
        err.as_ref().is_some_and(|e| !e.contains("in-tick retries")),
        "a permanent error fails the tick without a retry: {err:?}"
    );
    assert_eq!(retries.load(Ordering::SeqCst), 0);
    assert_eq!(backoff.load(Ordering::SeqCst), 1);
}

/// A range rejection takes the shrink path, not the retry path.
#[tokio::test(start_paused = true)]
async fn a_range_rejection_is_not_retried() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let Some(RetryPoller {
        mut poller,
        retries,
        ..
    }) = retry_poller(provider.clone(), DEFAULT_GET_LOGS_MAX_BLOCK_SPAN)
    else {
        return;
    };

    asserter.push_success(&U64::from(20));
    push_rpc_failure(
        &asserter,
        35,
        "ranges over 10000 blocks are not supported on free plan",
    );
    asserter.push_success(&Vec::<Log>::new());
    asserter.push_success(&Vec::<Log>::new());

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(
        result.is_ok(),
        "the halved windows complete the tick: {result:?}"
    );
    assert_eq!(retries.load(Ordering::SeqCst), 0);
    assert_eq!(poller.span.current(), 10);
}

/// Shutdown during the retry sleep ends the tick at once, with no backoff
/// edge, no retry counted and no further `get_logs`: the queued success
/// would have advanced the cursor.
#[tokio::test(start_paused = true)]
async fn shutdown_during_a_retry_sleep_ends_the_tick() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let Some(RetryPoller {
        mut poller,
        backoff,
        retries,
        ..
    }) = retry_poller(provider.clone(), DEFAULT_GET_LOGS_MAX_BLOCK_SPAN)
    else {
        return;
    };

    asserter.push_success(&U64::from(20));
    push_rpc_failure(&asserter, 19, "Temporary internal error. Please retry");
    asserter.push_success(&Vec::<Log>::new());

    let shutdown = CancellationToken::new();
    let cancel = async {
        tokio::time::sleep(GET_LOGS_WINDOW_RETRY_DELAY / 2).await;
        shutdown.cancel();
    };
    let started = tokio::time::Instant::now();
    let (result, ()) = tokio::join!(run_tick(&provider, &mut poller, &shutdown), cancel);
    assert!(result.is_ok(), "shutdown ends the tick cleanly: {result:?}");
    assert!(
        started.elapsed() < GET_LOGS_WINDOW_RETRY_DELAY,
        "the retry sleep does not outlast shutdown"
    );
    assert_eq!(retries.load(Ordering::SeqCst), 0);
    assert_eq!(backoff.load(Ordering::SeqCst), 0);
    assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(1));
}

/// Each window gets its own retry budget: two windows that each need two
/// retries complete one tick.
#[tokio::test(start_paused = true)]
async fn each_window_gets_its_own_retry_budget() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let Some(RetryPoller {
        mut poller,
        backoff,
        retries,
        ..
    }) = retry_poller(provider.clone(), 10)
    else {
        return;
    };

    // head=20, span 10: [1,10] and [11,20], each failing twice first.
    asserter.push_success(&U64::from(20));
    for _ in 0..2 {
        push_rpc_failure(&asserter, 19, "Temporary internal error. Please retry");
        push_rpc_failure(&asserter, 19, "Temporary internal error. Please retry");
        asserter.push_success(&Vec::<Log>::new());
    }

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(result.is_ok(), "both windows complete the tick: {result:?}");
    assert_eq!(retries.load(Ordering::SeqCst), 4);
    assert_eq!(backoff.load(Ordering::SeqCst), 0);
    assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(21));
}

/// A later window that spends its retries is deferred, and the earlier
/// window keeps its progress. The tick still counts as a success, because
/// a window completed.
#[tokio::test(start_paused = true)]
async fn a_later_window_that_spends_its_retries_defers_and_keeps_earlier_progress() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let Some(RetryPoller {
        mut poller,
        backoff,
        ticks,
        retries,
        deferred,
        store,
        ..
    }) = retry_poller(provider.clone(), 10)
    else {
        return;
    };

    // [1,10] fails twice then succeeds; [11,20] fails on every attempt.
    asserter.push_success(&U64::from(20));
    push_rpc_failure(&asserter, 19, "Temporary internal error. Please retry");
    push_rpc_failure(&asserter, 19, "Temporary internal error. Please retry");
    asserter.push_success(&Vec::<Log>::new());
    for _ in 0..3 {
        push_rpc_failure(&asserter, 30, "Request timeout on the free plan");
    }

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(
        result.is_ok(),
        "the exhausted window is deferred: {result:?}"
    );
    assert_eq!(retries.load(Ordering::SeqCst), 4);
    assert_eq!(deferred.load(Ordering::SeqCst), 1);
    assert_eq!(backoff.load(Ordering::SeqCst), 0);
    assert_eq!(
        ticks.load(Ordering::SeqCst),
        1,
        "a completed window earns the stamp"
    );
    assert!(
        poller.stalled_since.is_some(),
        "a tick that stops short of head starts the stall clock"
    );
    assert!(
        poller.routes.iter().all(|r| r.established),
        "the route stays established"
    );
    assert_eq!(
        poller.routes.first().and_then(|r| r.cursor),
        Some(11),
        "the cursor holds at the deferred window's start"
    );
    assert_eq!(
        store
            .load_checkpoint(CheckpointKey::PoolOpened)
            .ok()
            .flatten(),
        Some(10),
        "the completed window [1,10] stays persisted"
    );
}

/// A retried window can still meet the provider's range cap: the
/// rejection shrinks the span, and the halved windows complete the tick.
#[tokio::test(start_paused = true)]
async fn a_retried_window_still_shrinks_on_a_range_rejection() {
    let asserter = alloy::providers::mock::Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let Some(RetryPoller {
        mut poller,
        backoff,
        retries,
        rejections,
        ..
    }) = retry_poller(provider.clone(), DEFAULT_GET_LOGS_MAX_BLOCK_SPAN)
    else {
        return;
    };

    asserter.push_success(&U64::from(20));
    push_rpc_failure(&asserter, 19, "Temporary internal error. Please retry");
    push_rpc_failure(
        &asserter,
        35,
        "ranges over 10000 blocks are not supported on free plan",
    );
    asserter.push_success(&Vec::<Log>::new());
    asserter.push_success(&Vec::<Log>::new());

    let result = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(
        result.is_ok(),
        "the halved windows complete the tick: {result:?}"
    );
    assert_eq!(retries.load(Ordering::SeqCst), 1);
    assert_eq!(rejections.load(Ordering::SeqCst), 1);
    assert_eq!(backoff.load(Ordering::SeqCst), 0);
    assert_eq!(poller.span.current(), 10);
    assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(21));
}

#[test]
fn a_zero_span_is_a_build_error() {
    let head: Arc<dyn HeadSource> = Arc::new(StaticHead(0));
    let built = MultiplexedPollerBuilder::new(head, Duration::from_secs(1))
        .max_backfill_span(0)
        .build();
    assert!(built.is_err(), "a zero span would scan no blocks");
}

/// A head the test moves between ticks.
struct MovingHead(Arc<std::sync::atomic::AtomicU64>);

#[async_trait]
impl HeadSource for MovingHead {
    async fn head(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

/// A JSON-RPC mock with a provider-side `eth_getLogs` range cap. It answers
/// dRPC's free-tier error for any window wider than `cap` blocks and records
/// every requested window as `(from, to, accepted)`.
struct CappedGetLogsRpc {
    cap: u64,
    windows: Arc<std::sync::Mutex<Vec<(u64, u64, bool)>>>,
}

fn hex_block(value: Option<&serde_json::Value>) -> Option<u64> {
    let text = value?.as_str()?.strip_prefix("0x")?;
    u64::from_str_radix(text, 16).ok()
}

impl wiremock::Respond for CappedGetLogsRpc {
    fn respond(&self, req: &wiremock::Request) -> wiremock::ResponseTemplate {
        let body: serde_json::Value =
            serde_json::from_slice(&req.body).unwrap_or(serde_json::Value::Null);
        let id = body.get("id").cloned().unwrap_or(serde_json::json!(0));
        let filter = body.get("params").and_then(|p| p.get(0));
        let from = hex_block(filter.and_then(|f| f.get("fromBlock")));
        let to = hex_block(filter.and_then(|f| f.get("toBlock")));
        let (Some(from), Some(to)) = (from, to) else {
            return wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": id,
                "error": { "code": -32602, "message": "missing block bounds" },
            }));
        };
        let accepted = to - from < self.cap;
        self.windows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((from, to, accepted));
        let payload = if accepted {
            serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": [] })
        } else {
            serde_json::json!({
                "jsonrpc": "2.0", "id": id,
                "error": {
                    "code": 35,
                    "message": "ranges over 10000 blocks are not supported on free plan",
                },
            })
        };
        wiremock::ResponseTemplate::new(200).set_body_json(payload)
    }
}

/// A provider capping `eth_getLogs` at 150 blocks and a 1 651-block gap
/// behind head. The poller must shrink its window over
/// the real HTTP error path, finish the backfill in one tick, and keep the
/// learned span so the next tick sends no rejected request.
#[tokio::test]
async fn a_capped_provider_backfill_recovers_and_the_span_sticks() {
    const CAP: u64 = 150;
    let windows = Arc::new(std::sync::Mutex::new(Vec::new()));
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(CappedGetLogsRpc {
            cap: CAP,
            windows: Arc::clone(&windows),
        })
        .mount(&server)
        .await;
    let parsed_url = server.uri().parse();
    assert!(parsed_url.is_ok(), "mock server uri must parse");
    let Ok(url) = parsed_url else { return };
    let provider = ProviderBuilder::new().connect_http(url);

    let head_block = Arc::new(std::sync::atomic::AtomicU64::new(2_000));
    let head: Arc<dyn HeadSource> = Arc::new(MovingHead(Arc::clone(&head_block)));
    let (route, _sink, store) = checkpoint_route(350);
    let built = MultiplexedPollerBuilder::new(head, Duration::from_secs(1))
        .route(route)
        .build();
    let Ok(mut poller) = assert_built(built) else {
        return;
    };

    let first = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(
        first.is_ok(),
        "the capped backfill must complete: {first:?}"
    );
    assert!(
        poller.span.current() <= CAP,
        "span {} over the cap",
        poller.span.current()
    );
    assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(2_001));
    assert_eq!(
        store
            .load_checkpoint(CheckpointKey::PoolOpened)
            .ok()
            .flatten(),
        Some(2_000)
    );
    let recorded = windows
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let accepted: Vec<(u64, u64)> = recorded
        .iter()
        .filter(|w| w.2)
        .map(|w| (w.0, w.1))
        .collect();
    let mut next = 350;
    for (from, to) in &accepted {
        assert_eq!(
            *from, next,
            "accepted windows must be contiguous: {accepted:?}"
        );
        next = to + 1;
    }
    assert_eq!(next, 2_001, "accepted windows must cover the whole gap");

    // Tick 2: 100 new blocks fit the learned span, so nothing is rejected.
    head_block.store(2_100, Ordering::SeqCst);
    let rejected_before = recorded.iter().filter(|w| !w.2).count();
    let second = run_tick(&provider, &mut poller, &no_shutdown()).await;
    assert!(second.is_ok(), "the live tail must complete: {second:?}");
    let rejected_after = windows
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .filter(|w| !w.2)
        .count();
    assert_eq!(
        rejected_after, rejected_before,
        "the learned span must stick"
    );
    assert_eq!(poller.routes.first().and_then(|r| r.cursor), Some(2_101));
}
