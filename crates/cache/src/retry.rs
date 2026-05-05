//! Origin pull-through retry policy with exponential backoff and jitter (#285).
//!
//! Wraps a single [`Origin::fetch`] call in a bounded retry loop. The
//! adapter classifies each failure as [`OriginPullError::Transient`] or
//! [`OriginPullError::Permanent`]; this module only decides *how many*
//! transient retries to run and *how long* to sleep between them.
//!
//! The loop is deliberately not built around a generic `backoff` crate —
//! the workspace has neither one nor a need for one, and a 50-line inline
//! implementation keeps the anti-panic guarantees explicit (saturating math,
//! bounded shift, clamped jitter).
//!
//! ## Coalescing interaction
//!
//! [`crate::CacheEngine::get`] coalesces concurrent misses for the same hash
//! through a single owner task. The retry loop runs *inside* that owner;
//! waiters block on the shared `Notify` until the owner either succeeds or
//! exhausts.
//!
//! No *parallel* fan-out — at any instant exactly one owner is running the
//! retry loop, regardless of how many waiters have piled up. After an owner
//! exhausts, however, one waiter may become the next owner and run its own
//! full retry budget, so a sustained outage can produce up to `N`
//! *sequential* retry budgets across `N` concurrent waiters. The total
//! origin load is bounded by `N * (1 + max_retries)` per hash (versus the
//! `N` of pre-#285 behaviour). Acceptable at `PoC` scale; a "negative-cache"
//! window in the inflight slot would close this gap and is tracked as
//! follow-up work.

use std::sync::Arc;
use std::time::Duration;

use iroh_blobs::Hash;

use crate::error::OriginPullError;
use crate::origin::{Origin, OriginFetch};

/// Default values applied when the operator omits a `cache.origin_retry`
/// field. See [`RetryPolicy::default`] for the rationale on each.
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_INITIAL_BACKOFF_MS: u64 = 100;
const DEFAULT_MAX_BACKOFF_MS: u64 = 10_000;
const DEFAULT_JITTER_RATIO: f64 = 0.1;

/// Sanity ceiling on `max_retries`. Anything higher is almost always a
/// misconfiguration — a 16-retry budget at the default schedule already
/// saturates at the `max_backoff_ms` cap and contributes ~minutes of
/// latency to a sustained-failure request. `RetryPolicy::new` rejects
/// values above this; `RetryPolicy::disabled` and `RetryPolicy::default`
/// are below it by construction.
pub const MAX_RETRIES_CEILING: u32 = 16;

/// Sanity ceiling on `initial_backoff_ms` and `max_backoff_ms`. Five
/// minutes is well above any realistic operator backoff and well within
/// the f64 mantissa (the value roundtrips through `as f64` losslessly),
/// so the f64 → u64 cast inside the backoff calculation cannot lose
/// precision in practice. Tightening the runtime invariant here means
/// the `cast_precision_loss`-allow on the cast is a documented choice,
/// not a "we hope nobody configures something pathological" promise.
pub const MAX_BACKOFF_MS_CEILING: u64 = 300_000;

/// Cap the shift amount used when computing `initial_backoff * 2^attempt`.
/// Two concerns split out:
///
/// 1. **Avoids `1u64 << 64` UB** — Rust panics on shift amounts ≥ the
///    operand width, so any unbounded `attempt` must be clamped before
///    the shift.
/// 2. **Lets the math saturate against `max_backoff_ms` early** — at
///    `1u64 << 20` the shifted value is already ~1M× the initial
///    backoff, well past `max_backoff_ms` for any realistic operator
///    config, so the eventual `min(_, max_backoff_ms)` collapses
///    everything past this point to the cap.
const MAX_SHIFT: u32 = 20;

/// Validation failure raised by [`RetryPolicy::new`]. Each variant maps
/// to one of the field-level invariants the type advertises in its
/// rustdoc; carrying the offending value in the variant lets callers
/// build precise operator-facing error messages without re-parsing a
/// string.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum RetryPolicyError {
    /// `max_retries` exceeds [`MAX_RETRIES_CEILING`].
    #[error("max_retries={got} exceeds ceiling of {ceiling}; lower it or set 0 to disable retry")]
    MaxRetriesTooHigh {
        /// The value that was rejected.
        got: u32,
        /// The ceiling that was breached. Always [`MAX_RETRIES_CEILING`];
        /// carried so error messages survive a future change to the
        /// constant without re-rendering tests.
        ceiling: u32,
    },
    /// `initial_backoff_ms > max_backoff_ms` — the schedule would
    /// saturate at the cap from the very first retry, silently making
    /// `initial_backoff_ms` a no-op.
    #[error(
        "initial_backoff_ms ({initial}) must be <= max_backoff_ms ({max}); \
         otherwise the schedule never grows"
    )]
    InitialAboveMax {
        /// The configured initial backoff.
        initial: u64,
        /// The configured max backoff.
        max: u64,
    },
    /// `max_backoff_ms` exceeds [`MAX_BACKOFF_MS_CEILING`].
    #[error(
        "max_backoff_ms={got} exceeds ceiling of {ceiling}; \
         no realistic operator policy needs a backoff above 5 minutes"
    )]
    MaxBackoffTooHigh {
        /// The value that was rejected.
        got: u64,
        /// The ceiling that was breached. Always [`MAX_BACKOFF_MS_CEILING`].
        ceiling: u64,
    },
    /// `jitter_ratio` is outside `0.0..=1.0` or non-finite (`NaN`,
    /// `±∞`).
    #[error("jitter_ratio={got} must be a finite number in 0.0..=1.0")]
    JitterOutOfRange {
        /// The value that was rejected.
        got: f64,
    },
}

/// Origin retry policy. Hot-swappable on SIGHUP via
/// [`crate::CacheEngine::set_retry_policy`]; loaded once per origin fetch
/// from the engine's `ArcSwap` slot.
///
/// `max_retries == 0` disables retry entirely and reproduces the
/// pre-issue-#285 behaviour exactly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    /// Number of *retries* after the initial attempt. Total attempts =
    /// `max_retries + 1`. `0` disables retry.
    pub max_retries: u32,
    /// Backoff before the first retry, in milliseconds. Subsequent
    /// retries double up to `max_backoff_ms`.
    pub initial_backoff_ms: u64,
    /// Ceiling on any single backoff sleep, in milliseconds. The
    /// exponential schedule saturates here.
    pub max_backoff_ms: u64,
    /// Equal-jitter ratio in `0.0..=1.0`. The actual sleep is
    /// `base * (1 - ratio/2 + rand[0,1) * ratio)` so a ratio of `0.0` is
    /// fully deterministic and `1.0` spreads the sleep uniformly over
    /// `[0.5*base, 1.5*base)` — the AWS "equal jitter" recipe, which
    /// halves the deterministic floor and adds the same width on top.
    pub jitter_ratio: f64,
}

impl Default for RetryPolicy {
    /// Defaults: `3` retries, `100ms` initial backoff doubling to a cap of
    /// `10s`, with `10%` equal-jitter. Worst-case extra latency on a
    /// fully-failing origin is ~700ms — three sleeps `100 + 200 + 400 ms`
    /// (± up to 5% jitter each) between the four total attempts. The
    /// schedule never produces a fourth sleep because the loop exits on
    /// `attempt == max_retries` before sleeping.
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            initial_backoff_ms: DEFAULT_INITIAL_BACKOFF_MS,
            max_backoff_ms: DEFAULT_MAX_BACKOFF_MS,
            jitter_ratio: DEFAULT_JITTER_RATIO,
        }
    }
}

impl RetryPolicy {
    /// Build a [`RetryPolicy`], enforcing the invariants documented on
    /// the struct. The same checks run at config-resolution time
    /// (`crates/node/src/config/mod.rs::resolve_origin_retry`) — both
    /// paths share this constructor so tests, RPCs, or any future
    /// programmatic builder cannot bypass validation by populating the
    /// fields directly.
    ///
    /// Direct field construction is still allowed for ergonomics
    /// (`RetryPolicy { ... }`); use that only when bounds are guaranteed
    /// by construction (e.g. [`RetryPolicy::default`],
    /// [`RetryPolicy::disabled`], or constants in tests). Anywhere
    /// operator input could reach the policy, route through `new`.
    pub fn new(
        max_retries: u32,
        initial_backoff_ms: u64,
        max_backoff_ms: u64,
        jitter_ratio: f64,
    ) -> Result<Self, RetryPolicyError> {
        if max_retries > MAX_RETRIES_CEILING {
            return Err(RetryPolicyError::MaxRetriesTooHigh {
                got: max_retries,
                ceiling: MAX_RETRIES_CEILING,
            });
        }
        if max_backoff_ms > MAX_BACKOFF_MS_CEILING {
            return Err(RetryPolicyError::MaxBackoffTooHigh {
                got: max_backoff_ms,
                ceiling: MAX_BACKOFF_MS_CEILING,
            });
        }
        if initial_backoff_ms > max_backoff_ms {
            return Err(RetryPolicyError::InitialAboveMax {
                initial: initial_backoff_ms,
                max: max_backoff_ms,
            });
        }
        if !jitter_ratio.is_finite() || !(0.0..=1.0).contains(&jitter_ratio) {
            return Err(RetryPolicyError::JitterOutOfRange { got: jitter_ratio });
        }
        Ok(Self {
            max_retries,
            initial_backoff_ms,
            max_backoff_ms,
            jitter_ratio,
        })
    }

    /// A policy that performs no retries — equivalent to the pre-#285
    /// behaviour. Useful in tests and for operators who want to opt out.
    pub const fn disabled() -> Self {
        Self {
            max_retries: 0,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            jitter_ratio: 0.0,
        }
    }

    /// Compute the backoff delay before the `attempt`-th retry (0-indexed:
    /// `attempt = 0` is the first retry, immediately after the initial
    /// failure). The schedule is `min(initial * 2^attempt, max)` with
    /// equal-jitter applied.
    ///
    /// Saturating math throughout — anti-panic policy forbids
    /// `unwrap`/`expect`/`indexing_slicing`, and the shift amount is
    /// bounded by [`MAX_SHIFT`] to avoid `1u64 << 64` on a misconfigured
    /// `max_retries`.
    fn delay_for(self, attempt: u32) -> Duration {
        let shift = attempt.min(MAX_SHIFT);
        let multiplier: u64 = 1u64 << shift;
        let exp = self.initial_backoff_ms.saturating_mul(multiplier);
        let base = exp.min(self.max_backoff_ms);

        // Equal jitter: factor lies in [1 - r/2, 1 + r/2).
        let r = self.jitter_ratio.clamp(0.0, 1.0);
        let factor = 1.0_f64 - (r / 2.0) + rand::random::<f64>() * r;
        // Clamp before cast — anti-panic / cast_possible_truncation lints
        // require explicit bounding when going f64 -> u64. The
        // `u64::MAX as f64` upper bound is a known-imprecise cast (f64
        // mantissa is 52 bits) but it's only a ceiling — in practice
        // `base` is bounded by `max_backoff_ms` which any sane operator
        // configures well under 2^52.
        #[allow(clippy::cast_precision_loss)]
        let base_f = base as f64;
        #[allow(clippy::cast_precision_loss)]
        let upper = u64::MAX as f64;
        let scaled = (base_f * factor).clamp(0.0, upper);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let ms = scaled as u64;
        Duration::from_millis(ms)
    }
}

/// Outcome reported to a [`RetryObserver`] for each step of the retry loop.
/// The node crate uses this to bump Prometheus counters; tests assert on
/// the sequence of outcomes a given origin produces.
///
/// Marked `#[non_exhaustive]` because this is a cross-crate trait input —
/// future variants (e.g. `Cancelled` for shutdown-cancelled fetches)
/// should be addable without breaking out-of-tree observer impls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetryOutcome {
    /// Initial attempt (or any attempt where `attempt == 0`) succeeded —
    /// no retry was needed. Reported once per fetch.
    Success,
    /// Final attempt succeeded after at least one preceding transient
    /// failure. Operators care about this number — it represents the
    /// resilience the retry policy actually delivered.
    SuccessAfterRetry,
    /// A transient failure was observed and a retry will follow after the
    /// reported `sleep_ms` backoff. One emission per retry-eligible
    /// failure (so a fetch that retries 3 times emits this 3 times).
    TransientRetry,
    /// All allowed retries were spent on transient failures and the loop
    /// is giving up. Distinct from `Permanent` because the failure mode
    /// is different — operators tuning `max_retries` look at this counter.
    ExhaustedTransient,
    /// A non-retriable failure surfaced from the adapter; the loop
    /// returns immediately without sleeping.
    Permanent,
}

/// Hook for observing retry-loop activity. Default is a no-op so the cache
/// crate stays metrics-free; the node crate wires in an implementation that
/// bumps `DecdnMetrics` counters.
///
/// `observe` is fire-and-forget — implementations must not block (the loop
/// drives the origin fetch task) and must not panic.
pub trait RetryObserver: std::fmt::Debug + Send + Sync + 'static {
    /// Record a single retry-loop event.
    ///
    /// `attempt` is the 0-indexed iteration counter at the time the
    /// outcome was decided: `0` for the initial fetch, `1` after one
    /// retry has run, `N` after `N` retries. Read per variant:
    ///
    /// - [`RetryOutcome::Success`] always carries `attempt == 0`
    ///   (success on the initial fetch).
    /// - [`RetryOutcome::SuccessAfterRetry`] carries the count of
    ///   retries that ran before this success (so `attempt == N` means
    ///   "succeeded on the (N+1)th adapter call after N retries").
    /// - [`RetryOutcome::TransientRetry`] carries the iteration that
    ///   just failed; the retry incremented from this value is about
    ///   to start.
    /// - [`RetryOutcome::ExhaustedTransient`] carries the final
    ///   iteration index, which equals `policy.max_retries` (so total
    ///   adapter calls = `attempt + 1`).
    /// - [`RetryOutcome::Permanent`] carries the iteration that
    ///   surfaced the permanent failure (typically `0` for first-call
    ///   permanent errors; non-zero if a permanent failure surfaces
    ///   *after* one or more transient retries).
    ///
    /// `sleep_ms` is the backoff that will be slept after this event;
    /// non-zero only for `TransientRetry`.
    fn observe(&self, _outcome: RetryOutcome, _attempt: u32, _sleep_ms: u64) {}
}

/// No-op [`RetryObserver`]; used by [`crate::CacheEngine::open`] and any
/// caller that doesn't need metrics.
#[derive(Debug, Default)]
pub struct NoopObserver;

impl RetryObserver for NoopObserver {}

/// Drive a single origin fetch through the retry policy, surfacing the
/// final outcome to the observer.
///
/// The loop returns:
/// - `Ok(OriginFetch::Found)` / `Ok(OriginFetch::NotFound)` on success
///   (`NotFound` is a deterministic answer, never retried).
/// - `Err(OriginPullError::Permanent)` on any non-retriable failure or
///   when `max_retries` is exhausted (the final transient is rewrapped
///   into the same variant the engine collapses into `CacheError`).
///
/// The observer is called *after* each adapter call resolves, *before*
/// any sleep — so an implementation that crashes mid-sleep still records
/// the retry that motivated the sleep.
pub async fn retry_fetch(
    origin: &Arc<dyn Origin>,
    hash: Hash,
    max_bytes: u64,
    policy: RetryPolicy,
    observer: &Arc<dyn RetryObserver>,
) -> Result<OriginFetch, OriginPullError> {
    // `attempt` counts iterations (0-indexed). The loop runs at most
    // `max_retries + 1` adapter calls (the initial try plus
    // `max_retries` retries). The most recent failure flows through
    // pattern-binding `e` directly into the ExhaustedTransient rewrap
    // — no separate `last_err` slot needed.
    let mut attempt: u32 = 0;
    loop {
        match origin.fetch(hash, max_bytes).await {
            Ok(found) => {
                let outcome = if attempt == 0 {
                    RetryOutcome::Success
                } else {
                    RetryOutcome::SuccessAfterRetry
                };
                observer.observe(outcome, attempt, 0);
                return Ok(found);
            }
            Err(OriginPullError::Permanent(e)) => {
                observer.observe(RetryOutcome::Permanent, attempt, 0);
                return Err(OriginPullError::Permanent(e));
            }
            Err(OriginPullError::Transient(e)) => {
                if attempt >= policy.max_retries {
                    observer.observe(RetryOutcome::ExhaustedTransient, attempt, 0);
                    // Surface the exhaustion event via tracing —
                    // observers without a metrics adapter would
                    // otherwise see only the per-retry warns and never
                    // an explicit "exhausted" event, since the
                    // per-retry warn fires *between* attempts and is
                    // silent on the final one. The underlying error's
                    // own `Display` (HTTP status, FS errno, etc.)
                    // travels intact through `OriginPullError::Permanent`
                    // into `CacheError::OriginError.source`; we
                    // intentionally do *not* wrap it with `.context()`
                    // here because that would displace the topmost
                    // message and obscure the operator-actionable
                    // failure detail.
                    tracing::error!(
                        %hash,
                        attempts = attempt.saturating_add(1),
                        max_retries = policy.max_retries,
                        err = %e,
                        "origin fetch exhausted retry budget; surfacing as permanent",
                    );
                    // After exhaustion the failure is no longer "retriable
                    // by policy" — collapse to `Permanent` so callers that
                    // re-check `is_transient()` don't misinterpret a
                    // burned-out retry as still-eligible.
                    return Err(OriginPullError::Permanent(e));
                }
                let sleep = policy.delay_for(attempt);
                // `Duration::as_millis()` returns `u128`; the largest
                // representable u64 milliseconds is ~580M years, well
                // beyond `MAX_BACKOFF_MS_CEILING`, so the conversion is
                // saturating-safe by construction. `unwrap_or(u64::MAX)`
                // is purely defensive against a future change that
                // removes the ceiling.
                let sleep_ms = u64::try_from(sleep.as_millis()).unwrap_or(u64::MAX);
                tracing::warn!(
                    %hash,
                    attempt = attempt + 1,
                    of = policy.max_retries,
                    sleep_ms,
                    err = %e,
                    "origin fetch transient failure; retrying after backoff",
                );
                observer.observe(RetryOutcome::TransientRetry, attempt, sleep_ms);
                tokio::time::sleep(sleep).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // tests
mod tests {
    use super::*;

    #[test]
    fn default_values_match_documented_constants() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_retries, 3);
        assert_eq!(p.initial_backoff_ms, 100);
        assert_eq!(p.max_backoff_ms, 10_000);
        assert!((p.jitter_ratio - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn disabled_has_zero_retries() {
        let p = RetryPolicy::disabled();
        assert_eq!(p.max_retries, 0);
    }

    #[test]
    fn default_and_disabled_are_distinct() {
        // Footgun guard: `default()` is *not* `disabled()` — defaults
        // are opt-out (3 retries on by default per #285). A test
        // author writing `RetryPolicy::default()` to "get a no-op"
        // would actually retry 3 times with real sleeps. Pin the
        // distinction so a future "make default = disabled" change
        // is a deliberate, test-visible decision.
        assert_ne!(RetryPolicy::default(), RetryPolicy::disabled());
        assert!(RetryPolicy::default().max_retries > 0);
    }

    #[test]
    fn new_accepts_in_bounds_values() {
        let p = RetryPolicy::new(5, 50, 1_000, 0.25).expect("in-bounds");
        assert_eq!(p.max_retries, 5);
        assert_eq!(p.initial_backoff_ms, 50);
        assert_eq!(p.max_backoff_ms, 1_000);
        assert!((p.jitter_ratio - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn new_rejects_max_retries_above_ceiling() {
        let err = RetryPolicy::new(MAX_RETRIES_CEILING + 1, 100, 1000, 0.0).unwrap_err();
        assert!(matches!(err, RetryPolicyError::MaxRetriesTooHigh { .. }));
    }

    #[test]
    fn new_rejects_max_backoff_above_ceiling() {
        let err = RetryPolicy::new(3, 100, MAX_BACKOFF_MS_CEILING + 1, 0.0).unwrap_err();
        assert!(matches!(err, RetryPolicyError::MaxBackoffTooHigh { .. }));
    }

    #[test]
    fn new_rejects_initial_above_max() {
        let err = RetryPolicy::new(3, 2_000, 1_000, 0.0).unwrap_err();
        assert!(matches!(err, RetryPolicyError::InitialAboveMax { .. }));
    }

    #[test]
    fn new_rejects_jitter_out_of_range() {
        for bad in [-0.1, 1.5, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let err = RetryPolicy::new(3, 100, 1_000, bad)
                .expect_err(&format!("expected rejection for jitter={bad}"));
            assert!(
                matches!(err, RetryPolicyError::JitterOutOfRange { .. }),
                "wrong variant for {bad}: {err:?}"
            );
        }
    }

    #[test]
    fn delay_for_doubles_until_max_cap() {
        let p = RetryPolicy {
            max_retries: 10,
            initial_backoff_ms: 100,
            max_backoff_ms: 800,
            jitter_ratio: 0.0, // deterministic
        };
        let d0 = p.delay_for(0);
        let d1 = p.delay_for(1);
        let d2 = p.delay_for(2);
        let d3 = p.delay_for(3); // saturates at cap
        let d4 = p.delay_for(4); // still saturated
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
        // Run a handful of times — with the clamped ratio of 1.0 the
        // factor lies in [0.5, 1.5), so each duration is in
        // [50, 150) ms. Assert both bounds so a regression that
        // accidentally drops the lower clamp (or widens the spread)
        // surfaces here.
        for _ in 0..32 {
            let d = p.delay_for(0);
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
        // 10_000 is well above MAX_SHIFT; the result must saturate at the
        // configured `max_backoff_ms` rather than panic.
        let d = p.delay_for(10_000);
        assert_eq!(d.as_millis(), 60_000);
    }
}
