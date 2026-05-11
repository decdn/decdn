//! Origin pull-through retry policy with exponential backoff and jitter (#285).
//!
//! Wraps a single pull-through attempt in a bounded retry loop. The adapter
//! (or the engine's per-attempt closure) classifies each failure as
//! [`OriginPullError::Transient`] or [`OriginPullError::Permanent`]; this
//! module only decides *how many* transient retries to run and *how long*
//! to sleep between them.
//!
//! ## Body-phase retry (#519)
//!
//! The loop also covers body-phase failures via two paths, both gated on
//! the same `max_retries` budget:
//!
//! - **Buffer-then-commit (small blobs):** when the adapter advertises a
//!   `size_hint` at or below [`RetryPolicy::buffered_max_bytes`] (default
//!   4 MiB), [`retry_fetch`] drains the stream into a bounded `BytesMut`
//!   before returning. Drain errors are classified via the internal
//!   `classify_io_error` helper and re-fed to the loop — restoring
//!   pre-#271 retry semantics for small blobs at a bounded memory cost.
//! - **Abort + restart (large blobs):** the engine's per-attempt closure
//!   commits to `iroh-blobs::add_stream` directly. On mid-stream
//!   `io::Error`, the partial `TempTag` is dropped (iroh-blobs GC
//!   reclaims the bytes at `cache.gc_interval_sec` cadence) and the
//!   error is classified, restarting the loop from the headers phase.
//!   Memory stays bounded; disk amplification is
//!   `(1 + max_retries) * max_blob_bytes` worst case until GC.
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

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};

use crate::error::{OriginError, OriginPullError};
use crate::metrics::CacheMetrics;
use crate::origin::{BlobTooLargeMarker, Origin, OriginByteStream, OriginFetch};

/// Default values applied when the operator omits a `cache.origin_retry`
/// field.
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_INITIAL_BACKOFF_MS: u64 = 100;
const DEFAULT_MAX_BACKOFF_MS: u64 = 10_000;
const DEFAULT_JITTER_RATIO: f64 = 0.1;
/// Default per-fetch memory budget for the buffer-then-commit path (#519).
/// At or below this advertised `size_hint`, [`retry_fetch`] drains the
/// origin body into a `BytesMut` before handing it back, so mid-stream
/// transient `io::Error`s can be retried (pre-#271 semantics for small
/// blobs). Above the threshold the engine takes the streaming path and
/// uses abort + restart instead — no memory amplification, but
/// disk-amp cost per failed attempt until iroh-blobs GC sweeps. 4 MiB
/// covers typical web assets while keeping per-fetch RSS predictable.
const DEFAULT_BUFFERED_MAX_BYTES: u64 = 4 << 20;

/// `#[serde(default = ...)]` shim — `Default::default()` on the whole
/// struct can't be used field-by-field, so missing fields in a partial
/// `[cache.origin_retry]` section route through these helpers.
#[allow(clippy::missing_const_for_fn)] // serde requires `fn`, not `const fn`
pub(crate) fn default_buffered_max_bytes() -> u64 {
    DEFAULT_BUFFERED_MAX_BYTES
}

/// Origin retry policy. Field-level validation lives at config-resolve time
/// (`crates/node/src/config/mod.rs::resolve_origin_retry`); this struct
/// just carries the values.
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
    /// retry path (#519). When the origin advertises a `size_hint` at
    /// or below this value, [`retry_fetch`] drains the stream into a
    /// `BytesMut` before returning — drain errors get classified and
    /// re-feed the retry loop, restoring pre-#271 mid-stream retry
    /// semantics for small blobs. Above the threshold (or when
    /// `size_hint` is `None`) the engine uses streaming abort+restart
    /// instead; memory stays bounded but each failed attempt strands
    /// up to `max_blob_bytes` of partial-import bytes until
    /// iroh-blobs GC reclaims them.
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
    pub const fn disabled() -> Self {
        Self {
            max_retries: 0,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
            jitter_ratio: 0.0,
            buffered_max_bytes: 0,
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

/// Drive a per-attempt closure through the retry policy.
///
/// This is the single retry-loop primitive in the cache crate. Both
/// [`retry_fetch`] (which wraps just `origin.fetch`) and the engine's
/// `pull_through_attempt` (which wraps origin.fetch + stream-and-commit)
/// build on it; sharing the loop ensures the same `max_retries` budget,
/// exhaustion-counter accounting, and tracing shape applies to headers-
/// phase, drain-phase, and streaming-phase failures uniformly.
///
/// The closure is re-invoked for each attempt — callers must not capture
/// state that can only be consumed once. Returns:
/// - `Ok(T)` on first success.
/// - `Err(OriginPullError::Permanent)` on any non-retriable failure or
///   when `max_retries` is exhausted (the final transient is rewrapped
///   into the same variant the engine collapses into `CacheError`).
///
/// `metrics`, when present, has its `origin_retry_exhausted_total`
/// counter bumped when the budget is burned through. Per-attempt
/// visibility is via `tracing::warn!`/`tracing::error!` lines.
pub(crate) async fn run_with_retry<F, Fut, T>(
    policy: RetryPolicy,
    metrics: Option<&Arc<CacheMetrics>>,
    hash: Hash,
    mut attempt_fn: F,
) -> Result<T, OriginPullError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, OriginPullError>>,
{
    let mut attempt: u32 = 0;
    loop {
        match attempt_fn().await {
            Ok(value) => return Ok(value),
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

/// Drive a single origin fetch through the retry policy, with optional
/// small-blob buffer-then-commit (#519).
///
/// Returns:
/// - `Ok(OriginFetch::NotFound)` when the origin signals not-found
///   (deterministic answer, never retried).
/// - `Ok(OriginFetch::Found { stream, size_hint })` when the origin
///   advertised a `size_hint > policy.buffered_max_bytes` or set it to
///   `None` — caller takes the streaming path.
/// - `Ok(OriginFetch::Found { stream: <one-shot>, size_hint: Some(n) })`
///   when the body fit under `buffered_max_bytes` and was drained
///   in-loop. The returned stream yields the buffered bytes once and
///   then completes.
/// - `Err(OriginPullError::Permanent)` on permanent or exhausted
///   failure.
pub async fn retry_fetch(
    origin: &Arc<dyn Origin>,
    hash: Hash,
    max_bytes: u64,
    policy: RetryPolicy,
    metrics: Option<&Arc<CacheMetrics>>,
) -> Result<OriginFetch, OriginPullError> {
    run_with_retry(policy, metrics, hash, || async {
        let fetch = origin.fetch(hash, max_bytes).await?;
        let OriginFetch::Found { stream, size_hint } = fetch else {
            return Ok(OriginFetch::NotFound);
        };
        if should_buffer(size_hint, policy.buffered_max_bytes) {
            // Pre-stream cap: a `size_hint` over `max_bytes` should
            // already have surfaced as `OriginPullError::Permanent`
            // inside the adapter, but enforce here too so the drain
            // budget can never exceed the engine-level blob cap.
            let drain_cap = policy.buffered_max_bytes.min(max_bytes);
            match drain_to_bytes(stream, drain_cap, metrics).await {
                Ok(bytes) => Ok(OriginFetch::found_one_shot(bytes)),
                Err(e) => Err(classify_io_error(e)),
            }
        } else {
            Ok(OriginFetch::Found { stream, size_hint })
        }
    })
    .await
}

/// True when the origin's advertised `size_hint` is small enough to
/// buffer in memory for body-phase retry classification. Unknown
/// (`None`) hint falls through to streaming — buffering an unknown-size
/// body would let a lying origin overrun the operator's memory budget.
const fn should_buffer(size_hint: Option<u64>, buffered_max_bytes: u64) -> bool {
    if buffered_max_bytes == 0 {
        return false;
    }
    match size_hint {
        Some(n) => n <= buffered_max_bytes,
        None => false,
    }
}

/// Drain `stream` into a contiguous `Bytes`, capping the buffer at
/// `cap` bytes. Mirrors [`OriginFetch::collect_to_bytes`] but adds:
///
/// - A running cap: chunks past `cap` produce an `io::Error` whose
///   inner is a [`BlobTooLargeMarker`], so the caller can downcast and
///   surface `CacheError::BlobTooLarge` instead of a generic origin
///   error.
/// - Per-chunk `pull_through_bytes` metric bumps for egress-accounting
///   parity with the streaming path (every byte the origin sent is
///   billed, even on a drain that ultimately fails).
async fn drain_to_bytes(
    mut stream: OriginByteStream,
    cap: u64,
    metrics: Option<&Arc<CacheMetrics>>,
) -> io::Result<Bytes> {
    // Bound the upfront allocation regardless of `cap` so a hostile
    // origin advertising `Content-Length: 10 TiB` (truncated to `cap`
    // by the caller, but still potentially large) can't pre-commit a
    // huge virtual region before the first byte arrives. Matches the
    // 1 MiB initial-capacity hint used in `OriginFetch::collect_to_bytes`.
    const INITIAL_CAP: usize = 1 << 20; // 1 MiB
    let initial = usize::try_from(cap).unwrap_or(INITIAL_CAP).min(INITIAL_CAP);
    let mut buf = BytesMut::with_capacity(initial);
    let mut total: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if let Some(m) = metrics {
            m.pull_through_bytes
                .inc_by(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        }
        total = total.saturating_add(chunk.len() as u64);
        if total > cap {
            return Err(io::Error::other(BlobTooLargeMarker { max_bytes: cap }));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf.freeze())
}

/// Classify a body-phase `io::Error` into the retry loop's
/// [`OriginPullError`] taxonomy. Used by both the drain path
/// ([`drain_to_bytes`] → [`retry_fetch`]) and the streaming path
/// (engine's side-channel reader) so the classification is consistent
/// across the two body-phase entry points (#519).
///
/// The decision tree:
///
/// 1. If the `io::Error` wraps a typed inner via
///    `io::Error::other(typed)`, recover the typed variant first:
///    [`BlobTooLargeMarker`] → `Permanent(BlobTooLarge)`,
///    [`OriginError`] (e.g. `DecompressionFailed`) → `Permanent`.
///    These are deterministic protocol or cap violations — retry
///    won't help and would just waste budget.
///
/// 2. Otherwise dispatch on `io::ErrorKind`. The Transient set covers
///    the failure modes that the pre-#271 reqwest path classified as
///    `is_body()`/`is_connect()`/`is_timeout()`, plus their tokio /
///    SDK / filesystem equivalents. `ErrorKind::Other` defaults to
///    Transient because all three origin adapters wrap reqwest / SDK
///    body errors via `io::Error::other(...)` — pre-#271 those would
///    have been transient `reqwest::Error`s and retried. Anything not
///    in the Transient set is classified Permanent (fail-fast over
///    retry-storm for unrecognised modes).
pub(crate) fn classify_io_error(e: io::Error) -> OriginPullError {
    // Consume the error up front so we can chain `downcast` on the
    // inner box without re-checking. `io::Error::into_inner` returns
    // `Option<Box<dyn Error + Send + Sync>>`; if the inner is absent,
    // there's no typed marker to recover and we go straight to the
    // `kind()`-based fallback.
    let kind = e.kind();
    let Some(inner) = e.into_inner() else {
        return classify_kind_only(kind, None);
    };
    // Try BlobTooLargeMarker first. `Box::downcast` returns the
    // original boxed value back through `Err` on a type mismatch,
    // letting us walk through alternatives without losing ownership.
    let inner = match inner.downcast::<BlobTooLargeMarker>() {
        Ok(marker) => {
            return OriginPullError::Permanent(anyhow::anyhow!(
                "origin body exceeded max_blob_bytes={}",
                marker.max_bytes
            ));
        }
        Err(b) => b,
    };
    let inner = match inner.downcast::<OriginError>() {
        Ok(typed) => return OriginPullError::Permanent(anyhow::Error::from(*typed)),
        Err(b) => b,
    };
    // Inner wasn't a typed marker; fall back to `kind()` classification
    // but preserve the inner so the operator-visible source chain
    // isn't lost.
    classify_kind_only(kind, Some(inner))
}

/// Classify a body-phase failure by `io::ErrorKind` alone. Used when
/// no typed marker is recoverable from the inner — the kind is the
/// best signal we have for whether the failure is transport-class
/// (retry-eligible) or programmatic (fail-fast).
fn classify_kind_only(
    kind: io::ErrorKind,
    inner: Option<Box<dyn std::error::Error + Send + Sync>>,
) -> OriginPullError {
    let rebuilt = match inner {
        Some(b) => io::Error::new(kind, b),
        None => io::Error::from(kind),
    };
    match kind {
        io::ErrorKind::Interrupted
        | io::ErrorKind::TimedOut
        | io::ErrorKind::WouldBlock
        | io::ErrorKind::ResourceBusy
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionRefused
        | io::ErrorKind::NotConnected
        | io::ErrorKind::UnexpectedEof
        | io::ErrorKind::Other => OriginPullError::Transient(anyhow::Error::new(rebuilt)),
        _ => OriginPullError::Permanent(anyhow::Error::new(rebuilt)),
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
            jitter_ratio: 0.0,     // deterministic
            buffered_max_bytes: 0, // not exercised by delay_for
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
            buffered_max_bytes: 0,
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
            buffered_max_bytes: 0,
        };
        // 10_000 is well above 64; `checked_shl` returns None and the
        // saturating multiply collapses to `max_backoff_ms`.
        let d = p.delay_for(10_000);
        assert_eq!(d.as_millis(), 60_000);
    }
}
