//! Per-origin circuit-breaker policy (#963) — the value type only.
//!
//! The breaker *state machine* (CLOSED → OPEN → HALF-OPEN transitions,
//! the injectable clock, the per-origin slot table) lives in
//! `decdn-cache`; this crate carries just the operator-facing config
//! struct so `decdn-common`/the CLI can describe the policy without
//! linking the cache engine — same split as [`crate::RetryPolicy`].

use serde::{Deserialize, Serialize};

/// Default values applied when the operator omits a
/// `cache.circuit_breaker` field.
///
/// Defaults are deliberately conservative: the breaker only trips after
/// a *sustained* run of transient failures (not a single blip), and the
/// cooldown is short enough that a recovered origin is re-probed within
/// seconds. They are tuned for "shed the retry-backoff storm during a
/// real outage" — see the field docs for the per-knob rationale.
const DEFAULT_FAILURE_THRESHOLD: u32 = 5;
const DEFAULT_COOLDOWN_MS: u64 = 30_000;
const DEFAULT_HALF_OPEN_MAX_CALLS: u32 = 1;

/// Per-origin circuit-breaker policy (#963). Field-level validation
/// lives at config-resolve time
/// (`crates/common/src/config/mod.rs::resolve_circuit_breaker`); this
/// struct just carries the values.
///
/// The breaker fronts each origin's pull-through retry loop. After
/// `failure_threshold` *consecutive* transient/origin-unavailable
/// failures it trips OPEN and fast-fails every subsequent miss for
/// `cooldown_ms` (no retry/backoff incurred). Once the cooldown elapses
/// it goes HALF-OPEN and admits up to `half_open_max_calls` trial pulls;
/// a trial success closes the breaker, a trial failure re-opens it.
///
/// Permanent per-object failures (HTTP 404 → `NotFound`, other 4xx /
/// decode / cap breaches → `Permanent`) deliberately do **not** count
/// toward the threshold: a missing object is not an origin outage, and
/// tripping the breaker on 404s would shed load for content the origin
/// is perfectly able to serve.
///
/// `enabled == false` (or `failure_threshold == 0`) disables the breaker
/// entirely, reproducing the pre-#963 behaviour exactly — every miss
/// runs the full retry/backoff loop regardless of origin health.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CircuitBreakerPolicy {
    /// Master switch. `false` disables the breaker, so every cache miss
    /// runs the full retry/backoff loop (pre-#963 behaviour). The
    /// thresholds below are ignored while disabled.
    pub enabled: bool,
    /// Consecutive transient/origin-unavailable failures that trip the
    /// breaker from CLOSED to OPEN. `0` is treated as "disabled" (same
    /// as `enabled = false`) so an operator can't accidentally configure
    /// a breaker that trips on the very first blip. Defaults to `5`:
    /// high enough to ride out a single flaky request, low enough that a
    /// genuine outage trips within a handful of misses.
    pub failure_threshold: u32,
    /// How long the breaker stays OPEN (fast-failing every miss) before
    /// transitioning to HALF-OPEN to probe for recovery, in
    /// milliseconds. Defaults to `30_000` (30s): long enough to shed the
    /// retry-storm during a real outage, short enough that a recovered
    /// origin resumes serving promptly.
    pub cooldown_ms: u64,
    /// Maximum number of trial pulls admitted while HALF-OPEN. The first
    /// `half_open_max_calls` misses after the cooldown are allowed
    /// through (each running its normal retry budget); further misses
    /// fast-fail until a trial resolves. A single trial success closes
    /// the breaker; a single trial failure re-opens it (resetting the
    /// cooldown). Defaults to `1`: the standard half-open single-probe
    /// recipe — admit exactly one request to test the waters without
    /// re-flooding a still-sick origin.
    pub half_open_max_calls: u32,
}

impl Default for CircuitBreakerPolicy {
    /// Defaults: breaker **on**, trips after `5` consecutive
    /// origin-unavailable failures, stays OPEN for `30s`, then admits
    /// `1` half-open trial. See the field docs for per-knob rationale.
    fn default() -> Self {
        Self {
            enabled: true,
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            cooldown_ms: DEFAULT_COOLDOWN_MS,
            half_open_max_calls: DEFAULT_HALF_OPEN_MAX_CALLS,
        }
    }
}

impl CircuitBreakerPolicy {
    /// A policy with the breaker disabled — every miss runs the full
    /// retry/backoff loop (pre-#963 behaviour). Useful in tests and for
    /// operators who want to opt out.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            failure_threshold: 0,
            cooldown_ms: 0,
            half_open_max_calls: 0,
        }
    }

    /// True when the breaker is active: explicitly enabled AND configured
    /// with a non-zero failure threshold. A `failure_threshold == 0`
    /// collapses to disabled regardless of `enabled` so the breaker can
    /// never trip on the first failure.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.enabled && self.failure_threshold > 0
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // tests
mod tests;
