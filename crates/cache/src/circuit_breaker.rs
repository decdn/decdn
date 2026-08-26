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
/// `run_with_retry_classified` result onto exactly one of these.
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
///
/// A `Proceed` carries a [`TrialGuard`] that owns any HALF-OPEN trial
/// slot reserved by [`OriginBreaker::acquire`]. The guard MUST be kept
/// alive across the (possibly-cancelled) origin fetch and then resolved
/// with [`TrialGuard::record`]; if it is dropped without `record` — the
/// case when the surrounding future is cancelled mid-await — its `Drop`
/// releases the reserved slot so a stuck breaker can never leak its
/// half-open budget. See [`TrialGuard`].
#[derive(Debug)]
#[must_use = "a Proceed admission carries a TrialGuard that must be recorded or dropped"]
pub enum Admission<'b> {
    /// Proceed with the (retry-looped) origin fetch. Carries the state
    /// the breaker was in at admission so the caller knows whether this
    /// is a normal CLOSED request or a HALF-OPEN trial (only for
    /// logging — `record` handles the transition either way), plus the
    /// [`TrialGuard`] that releases the trial slot on cancellation.
    Proceed(BreakerState, TrialGuard<'b>),
    /// Short-circuit: the breaker is OPEN (or HALF-OPEN with its trial
    /// budget exhausted). The caller must fast-fail this miss WITHOUT
    /// running the retry/backoff loop.
    ShortCircuit,
}

/// RAII guard for one admitted pull-through attempt.
///
/// Returned inside [`Admission::Proceed`] by [`OriginBreaker::acquire`].
/// It exists to make the HALF-OPEN trial budget *cancellation-safe*: a
/// half-open trial reserves a `half_open_in_flight` slot at admission,
/// and that slot must be returned even if the surrounding async fetch is
/// cancelled (client disconnect / timeout) before its outcome is known.
///
/// Resolution happens exactly one of two ways:
///
/// - [`TrialGuard::record`] commits the outcome — driving the
///   CLOSED→OPEN, HALF-OPEN→CLOSED, and HALF-OPEN→OPEN transitions — and
///   defuses the guard so `Drop` does nothing.
/// - The guard is **dropped without `record`** (the future was
///   cancelled): `Drop` releases the reserved HALF-OPEN slot (a no-op
///   for a CLOSED admission, which reserves no slot) without recording
///   any state transition. The breaker neither closes nor re-opens on a
///   cancelled trial — it simply reclaims the budget so a later trial is
///   admitted.
///
/// The guard borrows the breaker, so it cannot outlive it; the engine
/// holds each breaker for the whole pull-through call, which strictly
/// outlives the guard.
#[derive(Debug)]
pub struct TrialGuard<'b> {
    /// The breaker this admission belongs to, against which
    /// [`Self::record`] commits the outcome. `None` only for a fully
    /// inert guard (disabled policy): no breaker, no slot, no recording.
    breaker: Option<&'b OriginBreaker>,
    /// Whether this admission reserved a HALF-OPEN trial slot that
    /// `Drop` must reclaim if `record` is never called (cancellation).
    /// `false` for a CLOSED admission, which reserves no slot — dropping
    /// it must NOT touch `half_open_in_flight`. Cleared to `false` by
    /// [`Self::record`] so a recorded trial is not also released on drop.
    owns_slot: bool,
}

impl TrialGuard<'_> {
    /// A fully inert guard: no breaker, no reserved slot. Dropping it is
    /// a no-op and `record` does nothing. Used for disabled policies,
    /// where there is neither a slot to release nor a transition to
    /// record.
    const fn inert() -> Self {
        Self {
            breaker: None,
            owns_slot: false,
        }
    }

    /// Commit the outcome of the admitted attempt and defuse the guard.
    ///
    /// Drives the breaker's state transitions (the private
    /// `OriginBreaker::record_committed`) and clears `owns_slot` so the
    /// guard's `Drop` does not also release the HALF-OPEN slot — `record`
    /// already accounts for the slot it resolves. For an inert guard
    /// (disabled policy) this is a no-op.
    pub fn record(mut self, outcome: OriginOutcome) {
        // Defuse first: from here `record_committed` owns the slot
        // release, so `Drop` (running at end of scope) must not also
        // reclaim it.
        self.owns_slot = false;
        if let Some(breaker) = self.breaker {
            breaker.record_committed(outcome);
        }
    }
}

impl Drop for TrialGuard<'_> {
    fn drop(&mut self) {
        // Reached without a `record` call only on cancellation. Release
        // the reserved HALF-OPEN slot — but only if this admission owned
        // one (CLOSED admissions and inert guards own none) — without
        // recording a transition, so a cancelled trial reclaims its
        // budget instead of leaking it.
        if self.owns_slot
            && let Some(breaker) = self.breaker
        {
            breaker.release_cancelled_trial();
        }
    }
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
/// every [`Self::acquire`] return [`Admission::Proceed`] with an inert
/// guard whose [`TrialGuard::record`] is a no-op, so an opted-out
/// operator pays nothing but a mutex-free `is_active` check on the hot
/// path.
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
    /// retry loop. Otherwise returns [`Admission::Proceed`] carrying a
    /// [`TrialGuard`]; if that admission was a HALF-OPEN trial, the guard
    /// owns the reserved trial slot and MUST be resolved — either by
    /// [`TrialGuard::record`] (which drives the transition) or by being
    /// dropped on cancellation (which reclaims the slot). Either way the
    /// slot is never leaked.
    ///
    /// A disabled policy always returns `Proceed(Closed, _)` with an
    /// inert guard.
    pub fn acquire(&self) -> Admission<'_> {
        if !self.policy.is_active() {
            return Admission::Proceed(BreakerState::Closed, TrialGuard::inert());
        }
        let now = self.clock.now();
        let mut st = self.lock();
        match st.phase {
            // A CLOSED admission reserves no trial slot, so its guard is
            // inert: only the recorded outcome matters, and dropping it
            // on cancellation must not touch `half_open_in_flight`.
            BreakerState::Closed => Admission::Proceed(BreakerState::Closed, self.closed_guard()),
            BreakerState::Open => {
                if self.cooldown_elapsed(&st, now) {
                    // Transition to HALF-OPEN and admit this caller as
                    // the first trial, handing it a slot-owning guard.
                    st.phase = BreakerState::HalfOpen;
                    st.half_open_in_flight = 1;
                    Admission::Proceed(BreakerState::HalfOpen, self.trial_guard())
                } else {
                    self.short_circuit();
                    Admission::ShortCircuit
                }
            }
            BreakerState::HalfOpen => {
                if st.half_open_in_flight < self.policy.half_open_max_calls {
                    st.half_open_in_flight = st.half_open_in_flight.saturating_add(1);
                    Admission::Proceed(BreakerState::HalfOpen, self.trial_guard())
                } else {
                    // Trial budget already in flight — shed extra load
                    // until a trial resolves.
                    self.short_circuit();
                    Admission::ShortCircuit
                }
            }
        }
    }

    /// Guard for a CLOSED (active-policy) admission: it carries the
    /// breaker so `record` drives the CLOSED→OPEN transition, but owns no
    /// half-open slot, so dropping it on cancellation must NOT touch
    /// `half_open_in_flight` (a CLOSED request reserved none).
    const fn closed_guard(&self) -> TrialGuard<'_> {
        TrialGuard {
            breaker: Some(self),
            owns_slot: false,
        }
    }

    /// Guard for a HALF-OPEN trial admission: carries the breaker and
    /// owns the reserved slot, so a cancelled (dropped-without-record)
    /// trial releases it.
    const fn trial_guard(&self) -> TrialGuard<'_> {
        TrialGuard {
            breaker: Some(self),
            owns_slot: true,
        }
    }

    /// Release a HALF-OPEN trial slot reserved at admission when the
    /// trial was cancelled before producing an outcome — `record` was
    /// never called, so [`TrialGuard::drop`] reclaims the budget here.
    ///
    /// No state transition: a cancelled probe is inconclusive, so the
    /// breaker neither closes nor re-opens. It only returns the slot so a
    /// subsequent trial is admitted. Releasing while not HALF-OPEN (a
    /// concurrent re-open already cleared `half_open_in_flight` to 0)
    /// saturates at 0 rather than underflowing.
    fn release_cancelled_trial(&self) {
        let mut st = self.lock();
        st.half_open_in_flight = st.half_open_in_flight.saturating_sub(1);
    }

    /// Record the committed outcome of a pull-through attempt that was
    /// admitted by [`Self::acquire`]. Drives the CLOSED→OPEN,
    /// HALF-OPEN→CLOSED, and HALF-OPEN→OPEN transitions.
    ///
    /// Reached only through [`TrialGuard::record`], so a disabled policy
    /// never gets here (its admission carries an inert guard whose
    /// `record` short-circuits). The HALF-OPEN arm releases the trial
    /// slot this outcome resolves; the guard is already defused, so there
    /// is no double-release.
    fn record_committed(&self, outcome: OriginOutcome) {
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
}
