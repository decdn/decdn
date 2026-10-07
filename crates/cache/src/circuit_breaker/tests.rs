use super::*;

fn breaker(policy: CircuitBreakerPolicy) -> (OriginBreaker, ManualClock) {
    let clock = ManualClock::new();
    let b = OriginBreaker::new(policy, Arc::new(clock.clone()), None);
    (b, clock)
}

fn policy() -> CircuitBreakerPolicy {
    CircuitBreakerPolicy {
        enabled: true,
        failure_threshold: 3,
        cooldown_ms: 1_000,
        half_open_max_calls: 1,
    }
}

/// Acquire, asserting admission, and return the trial guard so the
/// caller can `record` (or deliberately drop) it. `Admission` no
/// longer derives `PartialEq` (the guard holds a `&` and a `Drop`),
/// so admission assertions go through `matches!`.
fn admit(b: &OriginBreaker) -> TrialGuard<'_> {
    match b.acquire() {
        Admission::Proceed(_, guard) => guard,
        Admission::ShortCircuit => panic!("expected admission, got short-circuit"),
    }
}

/// Assert the breaker short-circuits right now.
fn assert_short_circuit(b: &OriginBreaker) {
    assert!(
        matches!(b.acquire(), Admission::ShortCircuit),
        "expected short-circuit"
    );
}

/// Drive one admitted attempt to the given outcome, asserting it was
/// admitted. Keeps the state-machine tests terse without discarding
/// the `#[must_use]` admission silently.
fn drive(b: &OriginBreaker, outcome: OriginOutcome) {
    admit(b).record(outcome);
}

#[test]
fn starts_closed_and_proceeds() {
    let (b, _clk) = breaker(policy());
    assert_eq!(b.state(), BreakerState::Closed);
    assert!(matches!(
        b.acquire(),
        Admission::Proceed(BreakerState::Closed, _)
    ));
}

#[test]
fn trips_open_after_threshold_consecutive_failures() {
    let (b, _clk) = breaker(policy());
    for _ in 0..2 {
        drive(&b, OriginOutcome::Unavailable);
        assert_eq!(
            b.state(),
            BreakerState::Closed,
            "still closed below threshold"
        );
    }
    drive(&b, OriginOutcome::Unavailable);
    assert_eq!(b.state(), BreakerState::Open, "threshold reached -> open");
}

#[test]
fn success_resets_consecutive_failure_count() {
    let (b, _clk) = breaker(policy());
    drive(&b, OriginOutcome::Unavailable);
    drive(&b, OriginOutcome::Unavailable);
    // An available outcome resets the count; we should NOT trip on
    // the next failure.
    drive(&b, OriginOutcome::Available);
    drive(&b, OriginOutcome::Unavailable);
    assert_eq!(b.state(), BreakerState::Closed);
}

#[test]
fn open_short_circuits_until_cooldown() {
    let (b, clk) = breaker(policy());
    // Trip it.
    for _ in 0..3 {
        drive(&b, OriginOutcome::Unavailable);
    }
    assert_eq!(b.state(), BreakerState::Open);
    // While open and before cooldown, acquire short-circuits.
    assert_short_circuit(&b);
    clk.advance(Duration::from_millis(999));
    assert_short_circuit(&b);
    // Once cooldown elapses, the next acquire goes half-open.
    clk.advance(Duration::from_millis(1));
    assert!(matches!(
        b.acquire(),
        Admission::Proceed(BreakerState::HalfOpen, _)
    ));
    assert_eq!(b.state(), BreakerState::HalfOpen);
}

#[test]
fn half_open_success_closes() {
    let (b, clk) = breaker(policy());
    for _ in 0..3 {
        drive(&b, OriginOutcome::Unavailable);
    }
    clk.advance(Duration::from_secs(1));
    admit(&b).record(OriginOutcome::Available);
    assert_eq!(b.state(), BreakerState::Closed, "trial success -> closed");
    let _ = &clk;
}

#[test]
fn half_open_failure_reopens() {
    let (b, clk) = breaker(policy());
    for _ in 0..3 {
        drive(&b, OriginOutcome::Unavailable);
    }
    clk.advance(Duration::from_secs(1));
    admit(&b).record(OriginOutcome::Unavailable);
    assert_eq!(b.state(), BreakerState::Open, "trial failure -> open");
    // And the cooldown timer restarts: an immediate acquire short-circuits.
    assert_short_circuit(&b);
}

#[test]
fn half_open_caps_trial_concurrency() {
    let p = CircuitBreakerPolicy {
        half_open_max_calls: 1,
        ..policy()
    };
    let (b, clk) = breaker(p);
    for _ in 0..3 {
        drive(&b, OriginOutcome::Unavailable);
    }
    clk.advance(Duration::from_secs(1));
    // First acquire admits the single trial — hold the guard so its
    // slot stays reserved across the second acquire.
    let _trial = admit(&b);
    // Second concurrent acquire (trial still in flight) short-circuits.
    assert_short_circuit(&b);
}

#[test]
fn half_open_allows_multiple_trials_when_configured() {
    let p = CircuitBreakerPolicy {
        half_open_max_calls: 3,
        ..policy()
    };
    let (b, clk) = breaker(p);
    for _ in 0..3 {
        drive(&b, OriginOutcome::Unavailable);
    }
    clk.advance(Duration::from_secs(1));
    // Three trials admitted (guards held so slots stay reserved),
    // fourth short-circuits.
    let _t1 = admit(&b);
    let _t2 = admit(&b);
    let _t3 = admit(&b);
    assert_short_circuit(&b);
}

#[test]
fn permanent_outcome_does_not_trip() {
    // A 404 / permanent per-object error is `Available` — the origin
    // is reachable, it just can't serve this object. Repeated such
    // outcomes must NEVER trip the breaker.
    let (b, _clk) = breaker(policy());
    for _ in 0..100 {
        drive(&b, OriginOutcome::Available);
    }
    assert_eq!(b.state(), BreakerState::Closed);
}

#[test]
fn disabled_policy_always_proceeds_and_never_trips() {
    let (b, _clk) = breaker(CircuitBreakerPolicy::disabled());
    for _ in 0..100 {
        let guard = match b.acquire() {
            Admission::Proceed(BreakerState::Closed, guard) => guard,
            other => panic!("disabled policy must proceed closed, got {other:?}"),
        };
        guard.record(OriginOutcome::Unavailable);
        // disabled record is a no-op; loop just proves no trip.
    }
    assert_eq!(b.state(), BreakerState::Closed);
}

#[test]
fn zero_threshold_policy_is_inactive() {
    let p = CircuitBreakerPolicy {
        enabled: true,
        failure_threshold: 0,
        ..policy()
    };
    let (b, _clk) = breaker(p);
    for _ in 0..100 {
        drive(&b, OriginOutcome::Unavailable);
    }
    assert_eq!(b.state(), BreakerState::Closed);
}

#[test]
fn metrics_count_trip_recovery_and_short_circuit() {
    let metrics = Arc::new(CacheMetrics::default());
    let clock = ManualClock::new();
    let b = OriginBreaker::new(
        policy(),
        Arc::new(clock.clone()),
        Some(Arc::clone(&metrics)),
    );
    // Trip.
    for _ in 0..3 {
        drive(&b, OriginOutcome::Unavailable);
    }
    assert_eq!(metrics.circuit_breaker_trips.get(), 1);
    // Short-circuit a miss while open.
    assert_short_circuit(&b);
    assert_eq!(metrics.circuit_breaker_short_circuits.get(), 1);
    // Recover.
    clock.advance(Duration::from_secs(1));
    admit(&b).record(OriginOutcome::Available);
    assert_eq!(metrics.circuit_breaker_recoveries.get(), 1);
}

/// Cancellation safety (#963): a HALF-OPEN trial whose guard is
/// dropped *without* `record` — exactly what happens when the
/// surrounding pull-through future is cancelled mid-await — must
/// release its reserved `half_open_in_flight` slot so a subsequent
/// trial is admitted. Before the RAII guard, the slot leaked and the
/// breaker stuck HALF-OPEN forever, short-circuiting every later
/// trial.
#[test]
fn dropped_trial_guard_releases_half_open_slot() {
    let (b, clk) = breaker(policy());
    // Trip to OPEN.
    for _ in 0..3 {
        drive(&b, OriginOutcome::Unavailable);
    }
    clk.advance(Duration::from_secs(1));

    // First acquire goes HALF-OPEN and reserves the single trial
    // slot. Simulate cancellation: drop the guard WITHOUT recording,
    // the way an aborted future would.
    {
        let guard = admit(&b);
        assert_eq!(b.state(), BreakerState::HalfOpen);
        // While the trial is in flight, the slot is taken: a
        // concurrent acquire short-circuits (budget == 1).
        assert_short_circuit(&b);
        drop(guard); // <-- cancellation point: no `record`.
    }

    // The slot must have been reclaimed by `Drop`. A subsequent
    // trial is admitted instead of being starved forever.
    let guard = admit(&b);
    assert_eq!(
        b.state(),
        BreakerState::HalfOpen,
        "still probing; slot was reclaimed, not leaked"
    );
    // And it resolves normally: a success closes the breaker.
    guard.record(OriginOutcome::Available);
    assert_eq!(b.state(), BreakerState::Closed, "trial success -> closed");
}

/// A dropped HALF-OPEN trial guard releases exactly one slot — it
/// must not also let `record` double-release, nor must a recorded
/// trial's `Drop` over-release. After a multi-slot HALF-OPEN cycle
/// where one trial is recorded and others cancelled, the budget is
/// fully reclaimed (all slots free) rather than driven negative or
/// stuck.
#[test]
fn mixed_recorded_and_cancelled_trials_reclaim_full_budget() {
    let p = CircuitBreakerPolicy {
        half_open_max_calls: 3,
        ..policy()
    };
    let (b, clk) = breaker(p);
    for _ in 0..3 {
        drive(&b, OriginOutcome::Unavailable);
    }
    clk.advance(Duration::from_secs(1));

    // Admit all three trials, holding their guards.
    let g1 = admit(&b);
    let g2 = admit(&b);
    let g3 = admit(&b);
    // Budget exhausted: a fourth acquire short-circuits.
    assert_short_circuit(&b);

    // Cancel two (drop without record), record one as Available.
    drop(g1);
    drop(g2);
    g3.record(OriginOutcome::Available);
    // The recorded success closed the breaker; the two cancellations
    // released their slots without underflow.
    assert_eq!(b.state(), BreakerState::Closed);

    // A fresh CLOSED→OPEN→HALF-OPEN cycle still admits the full
    // budget, proving no slot leaked from the cancelled pair.
    for _ in 0..3 {
        drive(&b, OriginOutcome::Unavailable);
    }
    clk.advance(Duration::from_secs(1));
    let _t1 = admit(&b);
    let _t2 = admit(&b);
    let _t3 = admit(&b);
    assert_short_circuit(&b);
}
