//! Origin pull-through retry policy with exponential backoff and jitter (#285).
//!
//! Wraps a single [`Origin::fetch`] call in a bounded retry loop. The
//! adapter classifies each failure as [`OriginPullError::Transient`] or
//! [`OriginPullError::Permanent`]; this module only decides *how many*
//! transient retries to run and *how long* to sleep between them.
//!
//! ## Coalescing interaction
//!
//! [`crate::CacheEngine::get`] coalesces concurrent misses for the same hash
//! through a single owner task. The retry loop runs *inside* that owner;
//! waiters block on the shared `Notify` until the owner either succeeds or
//! exhausts. After an owner exhausts, however, one waiter may become the
//! next owner and run its own full retry budget — a sustained outage can
//! produce up to `N` *sequential* retry budgets across `N` concurrent
//! waiters. Acceptable at `PoC` scale.

use std::sync::Arc;
use std::time::Duration;

use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};

use crate::error::OriginPullError;
use crate::metrics::CacheMetrics;
use crate::origin::{Origin, OriginFetch};

/// Default values applied when the operator omits a `cache.origin_retry`
/// field.
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_INITIAL_BACKOFF_MS: u64 = 100;
const DEFAULT_MAX_BACKOFF_MS: u64 = 10_000;
const DEFAULT_JITTER_RATIO: f64 = 0.1;

/// Origin retry policy. Field-level validation lives at config-resolve time
/// (`crates/node/src/config/mod.rs::resolve_origin_retry`); this struct
/// just carries the values.
///
/// `max_retries == 0` disables retry entirely and reproduces the
/// pre-issue-#285 behaviour exactly.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
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
    /// `[0.5*base, 1.5*base)` — the AWS "equal jitter" recipe.
    pub jitter_ratio: f64,
}

impl Default for RetryPolicy {
    /// Defaults: `3` retries, `100ms` initial backoff doubling to a cap of
    /// `10s`, with `10%` equal-jitter. Worst-case extra latency on a
    /// fully-failing origin is ~700ms — three sleeps `100 + 200 + 400 ms`
    /// (± up to 5% jitter each) between the four total attempts.
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
    /// Saturating math throughout: `checked_shl` handles huge `attempt`
    /// values without panicking on `1u64 << 64`, and the eventual
    /// `min(_, max_backoff_ms)` collapses everything to the cap.
    fn delay_for(self, attempt: u32) -> Duration {
        let multiplier = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
        let exp = self.initial_backoff_ms.saturating_mul(multiplier);
        let base = exp.min(self.max_backoff_ms);

        // Equal jitter: factor lies in [1 - r/2, 1 + r/2).
        let r = self.jitter_ratio.clamp(0.0, 1.0);
        let factor = 1.0_f64 - (r / 2.0) + rand::random::<f64>() * r;
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

/// Drive a single origin fetch through the retry policy.
///
/// The loop returns:
/// - `Ok(OriginFetch::Found)` / `Ok(OriginFetch::NotFound)` on success
///   (`NotFound` is a deterministic answer, never retried).
/// - `Err(OriginPullError::Permanent)` on any non-retriable failure or
///   when `max_retries` is exhausted (the final transient is rewrapped
///   into the same variant the engine collapses into `CacheError`).
///
/// `metrics`, when present, has its `origin_retry_exhausted_total`
/// counter bumped when the budget is burned through. Per-attempt
/// visibility is via `tracing::warn!`/`tracing::error!` lines.
pub async fn retry_fetch(
    origin: &Arc<dyn Origin>,
    hash: Hash,
    max_bytes: u64,
    policy: RetryPolicy,
    metrics: Option<&Arc<CacheMetrics>>,
) -> Result<OriginFetch, OriginPullError> {
    let mut attempt: u32 = 0;
    loop {
        match origin.fetch(hash, max_bytes).await {
            Ok(found) => return Ok(found),
            Err(OriginPullError::Permanent(e)) => return Err(OriginPullError::Permanent(e)),
            Err(OriginPullError::Transient(e)) => {
                if attempt >= policy.max_retries {
                    // Only count exhaustion when at least one retry actually
                    // fired. With `max_retries = 0` (operator opted out)
                    // a single transient failure is just a failure, not a
                    // burned-out retry budget — bumping the counter would
                    // ruin alerts that page on actual exhaustion.
                    if attempt > 0
                        && let Some(m) = metrics
                    {
                        m.origin_retry_exhausted.inc();
                    }
                    tracing::error!(
                        %hash,
                        attempts = attempt.saturating_add(1),
                        max_retries = policy.max_retries,
                        err = %e,
                        "origin fetch exhausted retry budget; surfacing as permanent",
                    );
                    return Err(OriginPullError::Permanent(e));
                }
                let sleep = policy.delay_for(attempt);
                let sleep_ms = u64::try_from(sleep.as_millis()).unwrap_or(u64::MAX);
                tracing::warn!(
                    %hash,
                    attempt = attempt + 1,
                    of = policy.max_retries,
                    sleep_ms,
                    err = %e,
                    "origin fetch transient failure; retrying after backoff",
                );
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
        // 10_000 is well above 64; `checked_shl` returns None and the
        // saturating multiply collapses to `max_backoff_ms`.
        let d = p.delay_for(10_000);
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
                let ms = p.delay_for(0).as_millis();
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
        let baseline = p.delay_for(2);
        assert_eq!(baseline.as_millis(), 400);
        for _ in 0..16 {
            assert_eq!(p.delay_for(2), baseline);
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
            assert_eq!(p.delay_for(attempt), Duration::ZERO);
        }
    }
}
