use super::*;

// `RetryPolicy` value-type tests (defaults / disabled) live in the
// `decdn-config-types` leaf crate alongside the struct (#578). What stays
// here is the `delay_for` backoff math, which lives in this crate because it
// needs `rand`.

#[test]
fn delay_for_doubles_until_max_cap() {
    let p = RetryPolicy {
        max_retries: 10,
        initial_backoff_ms: 100,
        max_backoff_ms: 800,
        jitter_ratio: 0.0, // deterministic
    };
    let d0 = delay_for(p, 0);
    let d1 = delay_for(p, 1);
    let d2 = delay_for(p, 2);
    let d3 = delay_for(p, 3); // saturates at cap
    let d4 = delay_for(p, 4); // still saturated
    assert_eq!(d0.as_millis(), 100);
    assert_eq!(d1.as_millis(), 200);
    assert_eq!(d2.as_millis(), 400);
    assert_eq!(d3.as_millis(), 800);
    assert_eq!(d4.as_millis(), 800);
}

#[test]
fn delay_for_clamps_jitter_above_one() {
    // Even with a misconfigured ratio > 1.0 the math must not panic.
    let p = RetryPolicy {
        max_retries: 1,
        initial_backoff_ms: 100,
        max_backoff_ms: 1000,
        jitter_ratio: 5.0,
    };
    for _ in 0..32 {
        let d = delay_for(p, 0);
        let ms = d.as_millis();
        assert!((50..150).contains(&ms), "jitter sample out of range: {ms}");
    }
}

#[test]
fn delay_for_handles_huge_attempt_without_panic() {
    // Anti-panic regression: a misconfigured high `attempt` must not
    // overflow the shift.
    let p = RetryPolicy {
        max_retries: u32::MAX,
        initial_backoff_ms: 1,
        max_backoff_ms: 60_000,
        jitter_ratio: 0.0,
    };
    // 10_000 is well above 64; `checked_shl` returns None and the
    // saturating multiply collapses to `max_backoff_ms`.
    let d = delay_for(p, 10_000);
    assert_eq!(d.as_millis(), 60_000);
}

#[test]
fn delay_for_jitter_stays_within_equal_jitter_band() {
    // Equal jitter (per the doc on `jitter_ratio`) produces delays in
    // [base*(1 - r/2), base*(1 + r/2)). The existing
    // `delay_for_clamps_jitter_above_one` only exercises the clamp
    // boundary (r > 1.0); pin a few non-clamped ratios so a future
    // tweak to the formula trips here.
    // base = 1000 ms; bounds precomputed to avoid float→int casts.
    let cases: &[(f64, u128, u128)] = &[
        (0.1, 950, 1050),  // ±5%
        (0.25, 875, 1125), // ±12.5%
        (0.5, 750, 1250),  // ±25%
        (1.0, 500, 1500),  // ±50% (full equal-jitter)
    ];
    for &(jitter_ratio, lower, upper) in cases {
        let p = RetryPolicy {
            max_retries: 1,
            initial_backoff_ms: 1000,
            max_backoff_ms: 10_000,
            jitter_ratio,
        };
        for _ in 0..256 {
            let ms = delay_for(p, 0).as_millis();
            assert!(
                (lower..upper).contains(&ms),
                "ratio={jitter_ratio}: sample {ms} not in [{lower}, {upper})"
            );
        }
    }
}

#[test]
fn delay_for_zero_jitter_is_deterministic() {
    // With ratio=0.0 the equal-jitter factor collapses to exactly 1.0,
    // so every call must return the same value — what tests that pin
    // an expected sleep schedule rely on.
    let p = RetryPolicy {
        max_retries: 5,
        initial_backoff_ms: 100,
        max_backoff_ms: 10_000,
        jitter_ratio: 0.0,
    };
    let baseline = delay_for(p, 2);
    assert_eq!(baseline.as_millis(), 400);
    for _ in 0..16 {
        assert_eq!(delay_for(p, 2), baseline);
    }
}

#[test]
fn delay_for_zero_initial_backoff_is_zero_regardless_of_jitter() {
    // Edge case: `initial_backoff_ms = 0` collapses the base to 0 at
    // every attempt, and the jitter factor scales 0 to 0. Pin this so
    // an operator who disables backoff via initial=0 (rather than
    // `max_retries = 0`) gets a predictable schedule.
    let p = RetryPolicy {
        max_retries: 3,
        initial_backoff_ms: 0,
        max_backoff_ms: 10_000,
        jitter_ratio: 0.5,
    };
    for attempt in 0..8 {
        assert_eq!(delay_for(p, attempt), Duration::ZERO);
    }
}

/// A retry policy with no backoff, so the tests run instantly.
const fn instant(max_retries: u32) -> RetryPolicy {
    RetryPolicy {
        max_retries,
        initial_backoff_ms: 0,
        max_backoff_ms: 0,
        jitter_ratio: 0.0,
    }
}

/// A breaker that trips after one unavailable read and stays open.
fn one_strike_breaker() -> Arc<OriginBreaker> {
    Arc::new(OriginBreaker::new(
        decdn_config_types::CircuitBreakerPolicy {
            enabled: true,
            failure_threshold: 1,
            cooldown_ms: 60_000,
            half_open_max_calls: 1,
        },
        Arc::new(crate::circuit_breaker::ManualClock::new()),
        None,
    ))
}

/// A transient fault is retried, and a read that then succeeds answers.
#[tokio::test]
async fn read_guard_retries_a_transient_fault() {
    let guard = ReadGuard::new(None, instant(3), None);
    let calls = std::sync::atomic::AtomicU32::new(0);
    let got = guard
        .run(Hash::new(b"flaky"), || {
            let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(OriginPullError::Transient(anyhow::anyhow!("blip")))
                } else {
                    Ok(n)
                }
            }
        })
        .await;
    assert_eq!(got.ok(), Some(2));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
}

/// A transient fault that exhausts the budget stays transient, and it trips
/// the breaker; the next read short-circuits with no read at all.
#[tokio::test]
async fn read_guard_keeps_an_exhausted_fault_transient_and_trips_the_breaker() {
    let breaker = one_strike_breaker();
    let guard = ReadGuard::new(Some(Arc::clone(&breaker)), instant(2), None);
    let calls = std::sync::atomic::AtomicU32::new(0);
    let read = || {
        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async { Err::<(), _>(OriginPullError::Transient(anyhow::anyhow!("down"))) }
    };
    let first = guard.run(Hash::new(b"down"), read).await;
    assert!(first.is_err_and(|e| e.is_transient()));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
    assert_eq!(breaker.state(), crate::circuit_breaker::BreakerState::Open);

    let second = guard.run(Hash::new(b"down"), read).await;
    assert!(second.is_err_and(|e| e.is_transient()));
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "an open breaker reads nothing"
    );
}

/// A permanent fault returns at once and does not trip the breaker: the
/// origin answered.
#[tokio::test]
async fn read_guard_returns_a_permanent_fault_at_once() {
    let breaker = one_strike_breaker();
    let guard = ReadGuard::new(Some(Arc::clone(&breaker)), instant(3), None);
    let calls = std::sync::atomic::AtomicU32::new(0);
    let got = guard
        .run(Hash::new(b"broken"), || {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err::<(), _>(OriginPullError::Permanent(anyhow::anyhow!("403"))) }
        })
        .await;
    assert!(got.is_err_and(|e| !e.is_transient()));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        breaker.state(),
        crate::circuit_breaker::BreakerState::Closed
    );
}
