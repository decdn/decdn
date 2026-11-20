//! Origin pull-through retry policy (#285) — the value type only.
//!
//! The retry *loop* (`run_with_retry`, jitter sleep) lives in
//! `decdn-cache`; this crate carries just the operator-facing config struct
//! so `decdn-common`/the CLI can describe the policy without linking the
//! cache engine.

use serde::{Deserialize, Serialize};

/// Default values applied when the operator omits a `cache.origin_retry`
/// field.
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_INITIAL_BACKOFF_MS: u64 = 100;
const DEFAULT_MAX_BACKOFF_MS: u64 = 10_000;
const DEFAULT_JITTER_RATIO: f64 = 0.1;

/// Origin retry policy. Field-level validation lives at config-resolve
/// time (`crates/common/src/config/mod.rs::resolve_origin_retry`); this
/// struct just carries the values.
///
/// `max_retries == 0` disables retry entirely.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
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
    /// A policy that performs no retries: one attempt per origin.
    /// Useful in tests and for operators who want to opt out.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            max_retries: 0,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            jitter_ratio: 0.0,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // tests
mod tests;
