//! Origin pull-through retry policy (#285) — the value type only.
//!
//! The retry *loop* (`run_with_retry`, jitter sleep, io-error
//! classification) lives in `decdn-cache`; this crate carries just the
//! operator-facing config struct so `decdn-common`/the CLI can describe
//! the policy without linking the cache engine.

use serde::{Deserialize, Serialize};

/// Default values applied when the operator omits a `cache.origin_retry`
/// field.
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_INITIAL_BACKOFF_MS: u64 = 100;
const DEFAULT_MAX_BACKOFF_MS: u64 = 10_000;
const DEFAULT_JITTER_RATIO: f64 = 0.1;
/// Default per-fetch memory budget for the buffer-then-commit path.
/// At or below this advertised `size_hint`, the engine's per-attempt
/// closure drains the origin body into a `BytesMut` before handing
/// it to iroh-blobs, so mid-stream transient `io::Error`s can be
/// retried (pre-#271 semantics for small blobs). Above the threshold
/// the engine takes the streaming path and uses abort + restart
/// instead — no memory amplification, but disk-amp cost per failed
/// attempt until iroh-blobs GC sweeps. 4 MiB covers typical web
/// assets while keeping per-fetch RSS predictable.
const DEFAULT_BUFFERED_MAX_BYTES: u64 = 4 << 20;

/// `#[serde(default = ...)]` shim — `Default::default()` on the whole
/// struct can't be used field-by-field, so missing fields in a partial
/// `[cache.origin_retry]` section route through this helper. Public
/// because the `#[serde(default = "...")]` attribute references it by
/// name from the struct definition in this crate.
#[must_use]
pub const fn default_buffered_max_bytes() -> u64 {
    DEFAULT_BUFFERED_MAX_BYTES
}

/// Origin retry policy. Field-level validation lives at config-resolve
/// time (`crates/common/src/config/mod.rs::resolve_origin_retry`); this
/// struct just carries the values.
///
/// `max_retries == 0` disables retry entirely and reproduces the
/// pre-issue-#285 behaviour exactly. `buffered_max_bytes == 0` separately
/// disables the buffer-then-commit body-phase path (#519), forcing all
/// body-phase failures through the abort+restart streaming path.
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
    /// Per-fetch memory budget for the buffer-then-commit body-phase
    /// retry path. When the origin advertises a `size_hint` at or
    /// below this value, the engine drains the stream into a
    /// `BytesMut` before committing — drain errors get classified
    /// and re-feed the retry loop, restoring pre-#271 mid-stream
    /// retry semantics for small blobs. Above the threshold (or
    /// when `size_hint` is `None`) the engine uses streaming
    /// abort+restart instead; memory stays bounded but each failed
    /// attempt strands up to `max_blob_bytes` of partial-import bytes
    /// until iroh-blobs GC reclaims them.
    ///
    /// `0` disables the buffer path entirely (all body-phase failures
    /// go through the streaming abort+restart path).
    #[serde(default = "default_buffered_max_bytes")]
    pub buffered_max_bytes: u64,
}

impl Default for RetryPolicy {
    /// Defaults: `3` retries, `100ms` initial backoff doubling to a cap of
    /// `10s`, with `10%` equal-jitter. Worst-case extra latency on a
    /// fully-failing origin is ~700ms — three sleeps `100 + 200 + 400 ms`
    /// (± up to 5% jitter each) between the four total attempts.
    /// `buffered_max_bytes` defaults to 4 MiB (the internal
    /// `DEFAULT_BUFFERED_MAX_BYTES` constant).
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            initial_backoff_ms: DEFAULT_INITIAL_BACKOFF_MS,
            max_backoff_ms: DEFAULT_MAX_BACKOFF_MS,
            jitter_ratio: DEFAULT_JITTER_RATIO,
            buffered_max_bytes: DEFAULT_BUFFERED_MAX_BYTES,
        }
    }
}

impl RetryPolicy {
    /// A policy that performs no retries — equivalent to the pre-#285
    /// behaviour. Useful in tests and for operators who want to opt out.
    /// Also sets `buffered_max_bytes = 0` so the body-phase buffer path
    /// is disabled in lock-step (a `disabled()` policy that still
    /// buffered would surprise operators reading the field name).
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            max_retries: 0,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            jitter_ratio: 0.0,
            buffered_max_bytes: 0,
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
        assert_eq!(p.buffered_max_bytes, DEFAULT_BUFFERED_MAX_BYTES);
        assert_eq!(p.buffered_max_bytes, 4 << 20);
    }

    #[test]
    fn disabled_has_zero_retries() {
        let p = RetryPolicy::disabled();
        assert_eq!(p.max_retries, 0);
        // `disabled()` must also disable the buffer path (#519) — the
        // field name promises "no retry"; an operator setting the
        // policy to disabled and still seeing buffered drains would
        // be surprised.
        assert_eq!(p.buffered_max_bytes, 0);
    }

    #[test]
    fn partial_section_fills_missing_fields_from_defaults() {
        // Operator-facing wire contract: a partial `[cache.origin_retry]`
        // (here as JSON, same serde path as the TOML config) must fill
        // every omitted field from `#[serde(default)]` / the named
        // `default = "default_buffered_max_bytes"` shim — NOT zero them.
        // A regression dropping either attribute would silently disable
        // retry or the #519 buffer path; pin it here.
        let p: RetryPolicy = serde_json::from_str(r#"{"max_retries": 5}"#).expect("deserialise");
        let d = RetryPolicy::default();
        assert_eq!(p.max_retries, 5, "explicit field must win");
        assert_eq!(p.initial_backoff_ms, d.initial_backoff_ms);
        assert_eq!(p.max_backoff_ms, d.max_backoff_ms);
        assert!((p.jitter_ratio - d.jitter_ratio).abs() < f64::EPSILON);
        // The load-bearing one: omitted `buffered_max_bytes` must route
        // through `default_buffered_max_bytes()` (4 MiB), not default to 0.
        assert_eq!(p.buffered_max_bytes, 4 << 20);
        assert_eq!(p.buffered_max_bytes, d.buffered_max_bytes);
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
}
