use super::*;

// `RetryPolicy` value-type tests (defaults / disabled) moved to the
// `decdn-config-types` leaf crate alongside the struct (#578). What
// stays here is the `delay_for` backoff math, which lives in this
// crate because it needs `rand`.

#[test]
fn delay_for_doubles_until_max_cap() {
    let p = RetryPolicy {
        max_retries: 10,
        initial_backoff_ms: 100,
        max_backoff_ms: 800,
        jitter_ratio: 0.0,     // deterministic
        buffered_max_bytes: 0, // not exercised by delay_for
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
        buffered_max_bytes: 0,
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
        buffered_max_bytes: 0,
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
            buffered_max_bytes: 4 << 20,
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
        buffered_max_bytes: 4 << 20,
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
        buffered_max_bytes: 4 << 20,
    };
    for attempt in 0..8 {
        assert_eq!(delay_for(p, attempt), Duration::ZERO);
    }
}
