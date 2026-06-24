//! Engine-level integration tests for the per-origin circuit-breaker
//! (#963).
//!
//! The breaker *state machine* is unit-tested in
//! `decdn_cache::circuit_breaker`'s own `mod tests` with a manual clock.
//! These tests exercise the breaker **wired into the cache-miss path**:
//! that an OPEN breaker short-circuits BEFORE the retry/backoff loop (so
//! no backoff is incurred and the origin is never even called), that the
//! cooldown→half-open→closed recovery cycle works end-to-end through
//! `CacheEngine::get`, and that permanent per-object errors (404) never
//! trip the breaker.
//!
//! Determinism: the breaker cooldown is driven by an injected
//! [`decdn_cache::ManualClock`] (via `open_full_with_clock`), so no test
//! sleeps for a real cooldown. The retry-backoff schedule is set large
//! on purpose so that *if* a short-circuit ever leaked into the retry
//! loop, the `tokio::time::timeout` guards below would fire — proving
//! fast-fail incurs no backoff.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_const_for_fn
)] // tests

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use decdn_cache::{
    CacheEngine, CacheError, CacheMetrics, CircuitBreakerPolicy, Hash, ManualClock, Origin,
    OriginFetch, OriginKind, OriginPullError, PinnedHashes, RetryPolicy,
};

/// A programmable origin whose behaviour for the target hash is flipped
/// at runtime via an atomic flag, and which counts every `fetch` call.
///
/// - `unavailable = true`  → returns `OriginPullError::Transient` (a 5xx
///   storm / connect failure — the kind that, after exhausting the retry
///   budget, the breaker counts as origin-unavailable).
/// - `unavailable = false` → serves the payload for the target hash.
///
/// The fetch counter is the load-bearing assertion for "fast-fail incurs
/// no backoff": while the breaker is OPEN, `get` must short-circuit
/// *before* calling `fetch`, so the counter does not move.
#[derive(Debug)]
struct ControllableOrigin {
    payload: bytes::Bytes,
    target: Hash,
    unavailable: AtomicBool,
    fetches: AtomicUsize,
}

impl ControllableOrigin {
    fn new(payload: &[u8]) -> Self {
        Self {
            target: Hash::new(payload),
            payload: bytes::Bytes::from(payload.to_vec()),
            unavailable: AtomicBool::new(true),
            fetches: AtomicUsize::new(0),
        }
    }
    fn hash(&self) -> Hash {
        self.target
    }
    fn set_unavailable(&self, v: bool) {
        self.unavailable.store(v, Ordering::SeqCst);
    }
    fn fetches(&self) -> usize {
        self.fetches.load(Ordering::SeqCst)
    }
}

impl Origin for ControllableOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }

    fn fetch(
        &self,
        hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let unavailable = self.unavailable.load(Ordering::SeqCst);
        let target = self.target;
        let payload = self.payload.clone();
        Box::pin(async move {
            if unavailable {
                Err(OriginPullError::Transient(anyhow::anyhow!(
                    "synthetic origin-unavailable"
                )))
            } else if hash == target {
                Ok(OriginFetch::found_one_shot(payload))
            } else {
                Ok(OriginFetch::NotFound)
            }
        })
    }
}

/// An origin that always returns a definitive `NotFound` (the 404 case)
/// and counts fetches. Permanent per-object answers must NEVER trip the
/// breaker.
#[derive(Debug)]
struct NotFoundOrigin {
    fetches: AtomicUsize,
}

impl NotFoundOrigin {
    fn new() -> Self {
        Self {
            fetches: AtomicUsize::new(0),
        }
    }
    fn fetches(&self) -> usize {
        self.fetches.load(Ordering::SeqCst)
    }
}

impl Origin for NotFoundOrigin {
    fn kind(&self) -> OriginKind {
        OriginKind::Http
    }
    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(OriginFetch::NotFound) })
    }
}

/// Retry policy with a *deliberately huge* backoff so that any
/// short-circuit leaking into the retry loop would block the test for
/// seconds — the `tokio::time::timeout` guards then fail fast instead of
/// silently passing. `max_retries = 2` means 3 attempts per pull when the
/// loop *does* run (CLOSED / HALF-OPEN trials).
fn slow_retry() -> RetryPolicy {
    RetryPolicy {
        max_retries: 2,
        initial_backoff_ms: 60_000, // 60s — never actually slept in a passing test
        max_backoff_ms: 60_000,
        jitter_ratio: 0.0,
        buffered_max_bytes: 0,
    }
}

/// Breaker that trips after 2 consecutive origin-unavailable outcomes,
/// 5s cooldown (driven by the manual clock), single half-open trial.
fn breaker_policy() -> CircuitBreakerPolicy {
    CircuitBreakerPolicy {
        enabled: true,
        failure_threshold: 2,
        cooldown_ms: 5_000,
        half_open_max_calls: 1,
    }
}

async fn build(
    origin: Arc<ControllableOrigin>,
    retry: RetryPolicy,
    breaker: CircuitBreakerPolicy,
) -> anyhow::Result<(
    CacheEngine,
    Arc<CacheMetrics>,
    ManualClock,
    tempfile::TempDir,
)> {
    let tmp = tempfile::tempdir()?;
    let metrics = Arc::new(CacheMetrics::default());
    let clock = ManualClock::new();
    let engine = CacheEngine::open_full_with_clock(
        tmp.path(),
        vec![origin as Arc<dyn Origin>],
        16,
        PinnedHashes::empty(),
        retry,
        breaker,
        Some(Arc::clone(&metrics)),
        Duration::ZERO,
        Arc::new(clock.clone()),
    )
    .await?;
    Ok((engine, metrics, clock, tmp))
}

/// Closed → open on repeated transient failures, and — the headline —
/// once OPEN, a miss fast-fails WITHOUT calling the origin or incurring
/// any backoff.
#[tokio::test]
async fn opens_on_repeated_transient_then_fast_fails_with_no_backoff() -> anyhow::Result<()> {
    let origin = Arc::new(ControllableOrigin::new(b"cb-payload-1"));
    let hash = origin.hash();
    // `slow_retry` has `max_retries = 0`-equivalent backoff that would
    // block 60s if it ever ran during a short-circuit. We DON'T want the
    // closed-state failures to take 60s either, so use `disabled` retry
    // for the trip phase: each failing `get` is a single attempt.
    let (engine, metrics, _clk, _tmp) = build(
        Arc::clone(&origin),
        RetryPolicy::disabled(),
        breaker_policy(),
    )
    .await?;

    // Two consecutive transient failures trip the breaker (threshold = 2).
    for _ in 0..2 {
        let err = engine.get(hash).await.unwrap_err();
        assert!(
            matches!(err, CacheError::OriginError { .. }),
            "transient-exhausted should surface as OriginError, got {err:?}"
        );
    }
    assert_eq!(
        metrics.circuit_breaker_trips.get(),
        1,
        "breaker should have tripped"
    );
    let fetches_at_trip = origin.fetches();
    assert_eq!(
        fetches_at_trip, 2,
        "origin called once per failing miss before the trip"
    );

    // Now OPEN. A miss must short-circuit: no new origin call, fast error,
    // short-circuit counter bumped. The timeout proves no backoff sleep.
    let res = tokio::time::timeout(Duration::from_secs(1), engine.get(hash))
        .await
        .expect("OPEN breaker miss must return immediately, not block on backoff");
    let err = res.unwrap_err();
    assert!(
        matches!(err, CacheError::OriginError { .. }),
        "open-breaker miss should surface OriginError, got {err:?}"
    );
    assert_eq!(
        origin.fetches(),
        fetches_at_trip,
        "OPEN breaker must NOT call the origin (fast-fail before the fetch)"
    );
    assert_eq!(
        metrics.circuit_breaker_short_circuits.get(),
        1,
        "the short-circuited miss should be counted"
    );
    Ok(())
}

/// Fast-fail-incurs-no-backoff, proved a second way: a 60s backoff retry
/// policy under a *paused* Tokio clock. Paused time auto-advances to the
/// next pending timer only when the runtime is otherwise idle, so the
/// trip-phase backoff sleeps resolve without real wall-time. Once OPEN,
/// the miss must resolve with the clock STILL paused — i.e. it never
/// registered a backoff timer at all (a short-circuit, not a slept
/// retry). If the breaker leaked into the retry loop, the `get` would
/// hang forever (no real time, no other task to trigger auto-advance)
/// and the `timeout` — which uses the same paused clock — would fire.
#[tokio::test(start_paused = true)]
async fn open_breaker_skips_the_60s_backoff_loop() -> anyhow::Result<()> {
    let origin = Arc::new(ControllableOrigin::new(b"cb-payload-2"));
    let hash = origin.hash();
    let (engine, _metrics, _clk, _tmp) =
        build(Arc::clone(&origin), slow_retry(), breaker_policy()).await?;

    // Trip the breaker. Each failing miss runs the full retry budget; the
    // paused clock auto-advances through the 60s backoff sleeps instantly
    // because nothing else is runnable.
    for _ in 0..2 {
        let r = engine.get(hash).await;
        assert!(r.is_err());
    }
    let fetches_at_trip = origin.fetches();

    // OPEN now. The miss must resolve under the paused clock. A leaked
    // backoff would register a 60s timer; with no concurrent task to keep
    // the runtime busy, `timeout` (also on the paused clock) would win and
    // we'd see an elapsed/timeout error instead of the breaker's fast
    // `OriginError`.
    let res = tokio::time::timeout(Duration::from_secs(30), engine.get(hash))
        .await
        .expect("OPEN breaker miss must resolve without registering a backoff timer");
    assert!(res.is_err());
    assert_eq!(
        origin.fetches(),
        fetches_at_trip,
        "no origin call while OPEN — the retry loop never ran"
    );
    Ok(())
}

/// Full recovery cycle through `get`: open → (cooldown) → half-open trial
/// succeeds → closed, and subsequent misses are served normally.
#[tokio::test]
async fn half_open_trial_success_recovers_to_closed() -> anyhow::Result<()> {
    let origin = Arc::new(ControllableOrigin::new(b"cb-payload-3"));
    let hash = origin.hash();
    let (engine, metrics, clk, _tmp) = build(
        Arc::clone(&origin),
        RetryPolicy::disabled(),
        breaker_policy(),
    )
    .await?;

    // Trip it.
    for _ in 0..2 {
        let _ = engine.get(hash).await;
    }
    assert_eq!(metrics.circuit_breaker_trips.get(), 1);

    // Still OPEN before cooldown: short-circuit, origin untouched.
    let fetches_open = origin.fetches();
    let _ = engine.get(hash).await;
    assert_eq!(origin.fetches(), fetches_open, "still OPEN: no origin call");

    // Make the origin healthy and advance the manual clock past the
    // cooldown so the next miss is admitted as a HALF-OPEN trial.
    origin.set_unavailable(false);
    clk.advance(Duration::from_secs(5));

    // Half-open trial pull: hits the origin once, succeeds, closes.
    let got = engine.get(hash).await?;
    assert_eq!(&got[..], b"cb-payload-3");
    assert_eq!(
        origin.fetches(),
        fetches_open + 1,
        "exactly one trial fetch while half-open"
    );
    assert_eq!(
        metrics.circuit_breaker_recoveries.get(),
        1,
        "trial success closed it"
    );

    // Now CLOSED and the blob is cached: a follow-up get is a local hit.
    let got2 = engine.get(hash).await?;
    assert_eq!(&got2[..], b"cb-payload-3");
    Ok(())
}

/// Half-open trial *fails* → breaker re-opens and the cooldown restarts;
/// a subsequent immediate miss short-circuits again.
#[tokio::test]
async fn half_open_trial_failure_reopens() -> anyhow::Result<()> {
    let origin = Arc::new(ControllableOrigin::new(b"cb-payload-4"));
    let hash = origin.hash();
    let (engine, metrics, clk, _tmp) = build(
        Arc::clone(&origin),
        RetryPolicy::disabled(),
        breaker_policy(),
    )
    .await?;

    for _ in 0..2 {
        let _ = engine.get(hash).await;
    }
    assert_eq!(metrics.circuit_breaker_trips.get(), 1);

    // Cooldown elapses, but the origin is STILL unavailable.
    clk.advance(Duration::from_secs(5));
    let fetches_before_trial = origin.fetches();
    let _ = engine.get(hash).await; // half-open trial — fails
    assert_eq!(
        origin.fetches(),
        fetches_before_trial + 1,
        "the half-open trial did call the origin once"
    );
    // Trial failure re-opens (a second trip).
    assert_eq!(
        metrics.circuit_breaker_trips.get(),
        2,
        "failed trial re-opened the breaker"
    );

    // Immediately after re-open (no further cooldown): short-circuit again.
    let fetches_after_trial = origin.fetches();
    let _ = engine.get(hash).await;
    assert_eq!(
        origin.fetches(),
        fetches_after_trial,
        "re-opened breaker short-circuits the very next miss (cooldown restarted)"
    );
    Ok(())
}

/// Permanent per-object errors (404 → `NotFound`) must NOT trip the
/// breaker no matter how many pile up: a missing object is not an outage.
#[tokio::test]
async fn permanent_not_found_never_trips_breaker() -> anyhow::Result<()> {
    let origin = Arc::new(NotFoundOrigin::new());
    let tmp = tempfile::tempdir()?;
    let metrics = Arc::new(CacheMetrics::default());
    let clock = ManualClock::new();
    let engine = CacheEngine::open_full_with_clock(
        tmp.path(),
        vec![Arc::clone(&origin) as Arc<dyn Origin>],
        16,
        PinnedHashes::empty(),
        RetryPolicy::disabled(),
        breaker_policy(),
        Some(Arc::clone(&metrics)),
        Duration::ZERO,
        Arc::new(clock),
    )
    .await?;

    let hash = Hash::new(b"never-exists");
    for _ in 0..20 {
        let err = engine.get(hash).await.unwrap_err();
        assert!(
            matches!(err, CacheError::NotFound { .. }),
            "404 should surface NotFound, got {err:?}"
        );
    }
    assert_eq!(
        metrics.circuit_breaker_trips.get(),
        0,
        "404s must never trip the breaker"
    );
    assert_eq!(
        metrics.circuit_breaker_short_circuits.get(),
        0,
        "no short-circuit: the breaker stayed CLOSED throughout"
    );
    // Every miss reached the origin — the breaker never shed load.
    assert_eq!(
        origin.fetches(),
        20,
        "all 20 misses hit the origin (no fast-fail)"
    );
    Ok(())
}

/// A disabled breaker policy reproduces pre-#963 behaviour: every miss
/// runs the retry loop and hits the origin, even under a sustained
/// outage — the breaker never short-circuits.
#[tokio::test]
async fn disabled_breaker_never_short_circuits() -> anyhow::Result<()> {
    let origin = Arc::new(ControllableOrigin::new(b"cb-payload-5"));
    let hash = origin.hash();
    let (engine, metrics, _clk, _tmp) = build(
        Arc::clone(&origin),
        RetryPolicy::disabled(),
        CircuitBreakerPolicy::disabled(),
    )
    .await?;

    for _ in 0..10 {
        let _ = engine.get(hash).await;
    }
    assert_eq!(metrics.circuit_breaker_trips.get(), 0);
    assert_eq!(metrics.circuit_breaker_short_circuits.get(), 0);
    assert_eq!(
        origin.fetches(),
        10,
        "disabled breaker: every miss still calls the origin"
    );
    Ok(())
}

/// Concurrency: many concurrent misses against a dead origin while the
/// breaker is OPEN must all fast-fail, and the origin must not be hit
/// more than the pre-trip attempts. Guards against a coalescing/race
/// regression where a waiter becomes a new owner and bypasses the
/// breaker.
#[tokio::test]
async fn concurrent_misses_while_open_all_fast_fail() -> anyhow::Result<()> {
    let origin = Arc::new(ControllableOrigin::new(b"cb-payload-6"));
    let hash = origin.hash();
    let (engine, _metrics, _clk, _tmp) = build(
        Arc::clone(&origin),
        RetryPolicy::disabled(),
        breaker_policy(),
    )
    .await?;

    // Trip the breaker (sequential, to make the trip deterministic).
    for _ in 0..2 {
        let _ = engine.get(hash).await;
    }
    let fetches_at_trip = origin.fetches();

    // Fire 50 concurrent misses while OPEN. All must return (fast) and
    // none may call the origin.
    let mut handles = Vec::new();
    for _ in 0..50 {
        let e = engine.clone();
        handles.push(tokio::spawn(async move { e.get(hash).await }));
    }
    let results = tokio::time::timeout(Duration::from_secs(5), async {
        let mut out = Vec::new();
        for h in handles {
            out.push(h.await.expect("task join"));
        }
        out
    })
    .await
    .expect("all concurrent OPEN-breaker misses must return promptly");

    assert!(
        results.iter().all(Result::is_err),
        "every concurrent miss fast-failed"
    );
    assert_eq!(
        origin.fetches(),
        fetches_at_trip,
        "no concurrent miss called the origin while OPEN"
    );
    Ok(())
}
