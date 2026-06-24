//! Per-origin half-open circuit-breaker for pull-through (#963).
//!
//! When an origin is fully unavailable, every cache miss would otherwise
//! run the full retry cycle (up to `max_retries` with exponential
//! backoff up to `max_backoff_ms`), serializing that backoff cost across
//! all concurrent misses with no way to fast-fail until the origin
//! recovers. This module adds a per-origin breaker so the cache fails
//! fast and sheds load during a sustained outage, then probes for
//! recovery.
//!
//! ## State machine
//!
//! Standard three-state breaker, one instance per origin in the
//! fallback chain:
//!
//! - **CLOSED** — requests flow into the retry loop. Consecutive
//!   origin-unavailable failures are counted; on reaching
//!   `failure_threshold` the breaker trips to OPEN. Any success (or a
//!   permanent per-object answer like 404) resets the count to zero.
//! - **OPEN** — every miss fast-fails *before* the retry/backoff loop
//!   (no backoff incurred) until `cooldown_ms` has elapsed since the
//!   trip, at which point the next acquire transitions to HALF-OPEN.
//! - **HALF-OPEN** — up to `half_open_max_calls` trial pulls are
//!   admitted (each running its normal retry budget). A trial success
//!   closes the breaker; a trial failure re-opens it and resets the
//!   cooldown. Trials beyond the in-flight budget fast-fail like OPEN.
//!
//! ## Failure classification
//!
//! Only **origin-unavailable** outcomes count toward the threshold —
//! transient failures that exhausted the retry budget. **Permanent
//! per-object** outcomes (HTTP 404, other 4xx, decode/cap breaches) are
//! recorded as *available*: the origin responded, it just can't serve
//! *this* object. Tripping the breaker on 404s would shed load for
//! content the origin is perfectly able to serve. See
//! [`OriginOutcome`].
//!
//! ## Clock injection
//!
//! Timekeeping goes through the [`Clock`] trait so tests drive the
//! cooldown deterministically with [`ManualClock`] instead of real
//! `sleep`s. Production uses [`SystemClock`] (monotonic `Instant`).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

use decdn_config_types::CircuitBreakerPolicy;

use crate::metrics::CacheMetrics;

/// Monotonic time source for the breaker, injectable so tests can drive
/// the cooldown deterministically rather than sleeping.
///
/// The single method returns an opaque "now" as a [`Duration`] since an
/// arbitrary fixed epoch — the breaker only ever compares two such
/// values, never reads wall-clock time, so a `Duration` from a private
/// origin is sufficient and keeps the manual clock a trivial counter.
pub trait Clock: Send + Sync + std::fmt::Debug {
    /// Monotonic now, as elapsed time since this clock's fixed epoch.
    fn now(&self) -> Duration;
}

/// Production clock: monotonic [`Instant`] measured from the clock's
/// construction. Never goes backwards, immune to wall-clock jumps.
#[derive(Debug)]
pub struct SystemClock {
    epoch: Instant,
}

impl SystemClock {
    /// Construct a system clock whose epoch is the moment of
    /// construction.
    #[must_use]
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        self.epoch.elapsed()
    }
}

/// Test clock: a manually-advanced counter. Deterministic — no real
/// time passes. Cloneable so a test can hold one handle and the breaker
/// another, both observing the same advancing value.
#[derive(Debug, Clone)]
pub struct ManualClock {
    now: Arc<Mutex<Duration>>,
}

impl ManualClock {
    /// Construct a manual clock starting at `t = 0`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            now: Arc::new(Mutex::new(Duration::ZERO)),
        }
    }

    /// Advance the clock by `delta`. Saturates rather than panics on
    /// overflow (a test advancing past `Duration::MAX` is a test bug,
    /// not a reason to violate the anti-panic policy).
    pub fn advance(&self, delta: Duration) {
        let mut g = self.now.lock().unwrap_or_else(PoisonError::into_inner);
        *g = g.saturating_add(delta);
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The health of a single per-origin pull-through attempt, as seen by
/// the breaker. The engine maps each origin's
/// `run_with_retry` result onto exactly one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OriginOutcome {
    /// The origin responded usefully — bytes committed, a definitive
    /// `NotFound` (404), or a permanent per-object error (other 4xx,
    /// decode/cap breach). The origin is *reachable*; the breaker treats
    /// all of these as a success signal and resets the failure count.
    Available,
    /// The origin was unavailable — a transient failure that exhausted
    /// the retry budget (5xx storm, connect/timeout/reset). This is the
    /// only outcome that counts toward the trip threshold.
    Unavailable,
}

/// The three breaker states. Exposed for tests and observability; the
/// engine only ever inspects it via [`OriginBreaker::state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    /// Requests flow; consecutive failures counted.
    Closed,
    /// Fast-failing all misses until the cooldown elapses.
    Open,
    /// Admitting a limited number of trial pulls to probe recovery.
    HalfOpen,
}

/// Whether the breaker admits a pull-through attempt right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Proceed with the (retry-looped) origin fetch. Carries the state
    /// the breaker was in at admission so the caller knows whether this
    /// is a normal CLOSED request or a HALF-OPEN trial (only for
    /// logging — `record` handles the transition either way).
    Proceed(BreakerState),
    /// Short-circuit: the breaker is OPEN (or HALF-OPEN with its trial
    /// budget exhausted). The caller must fast-fail this miss WITHOUT
    /// running the retry/backoff loop.
    ShortCircuit,
}

/// Internal mutable state, guarded by a single mutex. Kept small and
/// `Copy` so the critical section is a few field writes.
#[derive(Debug, Clone, Copy)]
struct State {
    phase: BreakerState,
    /// Consecutive `Unavailable` outcomes while CLOSED. Reset to 0 on
    /// any `Available`.
    consecutive_failures: u32,
    /// When the breaker last opened (clock time). Cooldown is measured
    /// from here. Only meaningful while `phase == Open`.
    opened_at: Duration,
    /// Trial pulls admitted while HALF-OPEN and not yet resolved. Caps
    /// the half-open concurrency at `half_open_max_calls`.
    half_open_in_flight: u32,
}

/// Per-origin circuit-breaker. One instance fronts each origin's
/// pull-through retry loop in [`crate::CacheEngine`].
///
/// Cheap to construct and `Send + Sync`; the engine holds one per origin
/// in a parallel `Vec`. A disabled policy (`!policy.is_active()`) makes
/// every [`Self::acquire`] return [`Admission::Proceed`] and every
/// [`Self::record`] a no-op, so an opted-out operator pays nothing but a
/// mutex-free `is_active` check on the hot path.
#[derive(Debug)]
pub struct OriginBreaker {
    policy: CircuitBreakerPolicy,
    clock: Arc<dyn Clock>,
    state: Mutex<State>,
    metrics: Option<Arc<CacheMetrics>>,
}

impl OriginBreaker {
    /// Construct a breaker for one origin under `policy`, using `clock`
    /// for cooldown timekeeping and bumping transition counters on
    /// `metrics` when present.
    #[must_use]
    pub fn new(
        policy: CircuitBreakerPolicy,
        clock: Arc<dyn Clock>,
        metrics: Option<Arc<CacheMetrics>>,
    ) -> Self {
        Self {
            policy,
            clock,
            metrics,
            state: Mutex::new(State {
                phase: BreakerState::Closed,
                consecutive_failures: 0,
                opened_at: Duration::ZERO,
                half_open_in_flight: 0,
            }),
        }
    }

    /// Current state, for tests and observability. Note this may run the
    /// lazy OPEN→HALF-OPEN cooldown check is *not* applied here — it
    /// reports the raw stored phase. Use [`Self::acquire`] to drive
    /// transitions.
    #[must_use]
    pub fn state(&self) -> BreakerState {
        self.lock().phase
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Decide whether to admit a pull-through attempt for this origin
    /// *now*, applying the lazy OPEN→HALF-OPEN cooldown transition.
    ///
    /// Returns [`Admission::ShortCircuit`] when the breaker is OPEN
    /// (cooldown not yet elapsed) or HALF-OPEN with no remaining trial
    /// budget — the caller must fast-fail the miss without running the
    /// retry loop. Otherwise returns [`Admission::Proceed`]; if that
    /// admission was a HALF-OPEN trial, a [`Self::record`] call MUST
    /// follow to release the trial slot and resolve the probe.
    ///
    /// A disabled policy always returns `Proceed(Closed)`.
    #[must_use]
    pub fn acquire(&self) -> Admission {
        if !self.policy.is_active() {
            return Admission::Proceed(BreakerState::Closed);
        }
        let now = self.clock.now();
        let mut st = self.lock();
        match st.phase {
            BreakerState::Closed => Admission::Proceed(BreakerState::Closed),
            BreakerState::Open => {
                if self.cooldown_elapsed(&st, now) {
                    // Transition to HALF-OPEN and admit this caller as
                    // the first trial.
                    st.phase = BreakerState::HalfOpen;
                    st.half_open_in_flight = 1;
                    Admission::Proceed(BreakerState::HalfOpen)
                } else {
                    self.short_circuit();
                    Admission::ShortCircuit
                }
            }
            BreakerState::HalfOpen => {
                if st.half_open_in_flight < self.policy.half_open_max_calls {
                    st.half_open_in_flight = st.half_open_in_flight.saturating_add(1);
                    Admission::Proceed(BreakerState::HalfOpen)
                } else {
                    // Trial budget already in flight — shed extra load
                    // until a trial resolves.
                    self.short_circuit();
                    Admission::ShortCircuit
                }
            }
        }
    }

    /// Record the outcome of a pull-through attempt that was admitted by
    /// [`Self::acquire`]. Drives the CLOSED→OPEN, HALF-OPEN→CLOSED, and
    /// HALF-OPEN→OPEN transitions.
    ///
    /// A disabled policy is a no-op. MUST be called exactly once for
    /// every `Proceed` admission (and never for a `ShortCircuit`), so
    /// the HALF-OPEN in-flight trial budget is correctly released.
    pub fn record(&self, outcome: OriginOutcome) {
        if !self.policy.is_active() {
            return;
        }
        let now = self.clock.now();
        let mut st = self.lock();
        match st.phase {
            BreakerState::Closed => match outcome {
                OriginOutcome::Available => st.consecutive_failures = 0,
                OriginOutcome::Unavailable => {
                    st.consecutive_failures = st.consecutive_failures.saturating_add(1);
                    if st.consecutive_failures >= self.policy.failure_threshold {
                        self.open(&mut st, now);
                    }
                }
            },
            BreakerState::HalfOpen => {
                // Release the trial slot this outcome belongs to.
                st.half_open_in_flight = st.half_open_in_flight.saturating_sub(1);
                match outcome {
                    OriginOutcome::Available => self.close(&mut st),
                    OriginOutcome::Unavailable => self.open(&mut st, now),
                }
            }
            BreakerState::Open => {
                // A record landing while OPEN means a request admitted as
                // a HALF-OPEN trial raced a re-open (another trial failed
                // first). Fold its signal in without double-counting the
                // transition: a failure keeps us OPEN and refreshes the
                // cooldown; a success is ignored (the breaker already
                // decided this origin is unhealthy this cycle — one
                // straggler success doesn't override a concurrent
                // failure's re-open).
                if outcome == OriginOutcome::Unavailable {
                    st.opened_at = now;
                }
            }
        }
    }

    /// True once `cooldown_ms` has elapsed since the breaker opened.
    /// A zero cooldown means "transition on the very next acquire".
    fn cooldown_elapsed(&self, st: &State, now: Duration) -> bool {
        let cooldown = Duration::from_millis(self.policy.cooldown_ms);
        now.saturating_sub(st.opened_at) >= cooldown
    }

    /// Transition to OPEN, resetting the cooldown timer and clearing the
    /// half-open trial budget. Bumps the trip counter.
    fn open(&self, st: &mut State, now: Duration) {
        st.phase = BreakerState::Open;
        st.opened_at = now;
        st.consecutive_failures = 0;
        st.half_open_in_flight = 0;
        if let Some(m) = &self.metrics {
            m.circuit_breaker_trips.inc();
        }
    }

    /// Transition to CLOSED, clearing all counters. Bumps the recovery
    /// counter.
    fn close(&self, st: &mut State) {
        st.phase = BreakerState::Closed;
        st.consecutive_failures = 0;
        st.half_open_in_flight = 0;
        if let Some(m) = &self.metrics {
            m.circuit_breaker_recoveries.inc();
        }
    }

    /// Bump the short-circuit (load-shed) counter.
    fn short_circuit(&self) {
        if let Some(m) = &self.metrics {
            m.circuit_breaker_short_circuits.inc();
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
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

    /// Drive one admitted attempt to the given outcome, asserting it was
    /// admitted. Keeps the state-machine tests terse without discarding
    /// the `#[must_use]` admission silently.
    fn drive(b: &OriginBreaker, outcome: OriginOutcome) {
        assert!(
            matches!(b.acquire(), Admission::Proceed(_)),
            "expected admission"
        );
        b.record(outcome);
    }

    #[test]
    fn starts_closed_and_proceeds() {
        let (b, _clk) = breaker(policy());
        assert_eq!(b.state(), BreakerState::Closed);
        assert!(matches!(
            b.acquire(),
            Admission::Proceed(BreakerState::Closed)
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
        assert_eq!(b.acquire(), Admission::ShortCircuit);
        clk.advance(Duration::from_millis(999));
        assert_eq!(
            b.acquire(),
            Admission::ShortCircuit,
            "cooldown not yet elapsed"
        );
        // Once cooldown elapses, the next acquire goes half-open.
        clk.advance(Duration::from_millis(1));
        assert!(matches!(
            b.acquire(),
            Admission::Proceed(BreakerState::HalfOpen)
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
        let adm = b.acquire();
        assert!(matches!(adm, Admission::Proceed(BreakerState::HalfOpen)));
        b.record(OriginOutcome::Available);
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
        assert!(matches!(
            b.acquire(),
            Admission::Proceed(BreakerState::HalfOpen)
        ));
        b.record(OriginOutcome::Unavailable);
        assert_eq!(b.state(), BreakerState::Open, "trial failure -> open");
        // And the cooldown timer restarts: an immediate acquire short-circuits.
        assert_eq!(b.acquire(), Admission::ShortCircuit);
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
        // First acquire admits the single trial.
        assert!(matches!(
            b.acquire(),
            Admission::Proceed(BreakerState::HalfOpen)
        ));
        // Second concurrent acquire (trial still in flight) short-circuits.
        assert_eq!(b.acquire(), Admission::ShortCircuit);
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
        // Three trials admitted, fourth short-circuits.
        assert!(matches!(b.acquire(), Admission::Proceed(_)));
        assert!(matches!(b.acquire(), Admission::Proceed(_)));
        assert!(matches!(b.acquire(), Admission::Proceed(_)));
        assert_eq!(b.acquire(), Admission::ShortCircuit);
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
            assert!(matches!(
                b.acquire(),
                Admission::Proceed(BreakerState::Closed)
            ));
            b.record(OriginOutcome::Unavailable);
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
        assert_eq!(b.acquire(), Admission::ShortCircuit);
        assert_eq!(metrics.circuit_breaker_short_circuits.get(), 1);
        // Recover.
        clock.advance(Duration::from_secs(1));
        let _ = b.acquire();
        b.record(OriginOutcome::Available);
        assert_eq!(metrics.circuit_breaker_recoveries.get(), 1);
    }
}
