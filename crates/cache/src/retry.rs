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
//!   4 MiB), the engine's per-attempt closure drains the stream into a
//!   bounded `BytesMut` before handing it to iroh-blobs. Drain errors are
//!   classified via the internal `classify_io_error` helper (which
//!   recognises typed `BlobTooLargeMarker` and `OriginError::*` as
//!   Permanent, treats `io::ErrorKind::{ConnectionReset, TimedOut,
//!   UnexpectedEof, …}` and the catch-all `Other` as Transient) and
//!   re-fed to the loop — restoring pre-#271 retry semantics for small
//!   blobs at a bounded memory cost.
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
use decdn_config_types::RetryPolicy;
use futures_util::StreamExt;
use iroh_blobs::Hash;

use crate::error::{OriginError, OriginPullError};
use crate::metrics::CacheMetrics;
use crate::origin::{BlobTooLargeMarker, Origin, OriginByteStream, OriginFetch};

/// Compute the backoff delay before the `attempt`-th retry (0-indexed:
/// `attempt = 0` is the first retry, immediately after the initial
/// failure). The schedule is `min(initial * 2^attempt, max)` with
/// equal-jitter applied.
///
/// Lives here (not on [`RetryPolicy`], which moved to the
/// `decdn-config-types` leaf crate) because it needs `rand` for the
/// jitter sample — keeping `rand` out of the leaf is the point of #578.
///
/// Saturating math throughout: `checked_shl` handles huge `attempt`
/// values without panicking on `1u64 << 64`, and the eventual
/// `min(_, max_backoff_ms)` collapses everything to the cap.
fn delay_for(policy: RetryPolicy, attempt: u32) -> Duration {
    let multiplier = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
    let exp = policy.initial_backoff_ms.saturating_mul(multiplier);
    let base = exp.min(policy.max_backoff_ms);

    // Equal jitter: factor lies in [1 - r/2, 1 + r/2).
    let r = policy.jitter_ratio.clamp(0.0, 1.0);
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
                let sleep = delay_for(policy, attempt);
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
pub(crate) const fn should_buffer(size_hint: Option<u64>, buffered_max_bytes: u64) -> bool {
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
pub(crate) async fn drain_to_bytes(
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
/// ([`drain_to_bytes`]) and the streaming path (engine's
/// side-channel reader) so the classification is consistent
/// across the two body-phase entry points.
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
/// 2. Otherwise dispatch on `io::ErrorKind`. The Transient set:
///    `Interrupted`, `TimedOut`, `WouldBlock`, `ResourceBusy`,
///    `BrokenPipe`, `ConnectionReset`, `ConnectionAborted`,
///    `ConnectionRefused`, `NotConnected`, `UnexpectedEof`, and the
///    catch-all `Other`. `UnexpectedEof` is the load-bearing kind
///    for mid-body resets behind a flaky LB — reqwest surfaces
///    truncated Content-Length responses through it. `Other`
///    defaults to Transient because all three origin adapters wrap
///    reqwest / SDK body errors via `io::Error::other(...)` —
///    pre-#271 those would have been transient `reqwest::Error`s
///    and retried. Anything not in the Transient set is classified
///    Permanent (fail-fast over retry-storm for unrecognised
///    modes).
pub(crate) fn classify_io_error(e: io::Error) -> OriginPullError {
    // Peek the inner *without* consuming so we can preserve the
    // original `e` (and its `raw_os_error` / Display) when no typed
    // marker matches. `io::Error::from_raw_os_error` / `last_os_error`
    // produce errors whose `into_inner()` returns `None` — consuming
    // up front would silently strip the OS error code.
    let has_typed_marker = e
        .get_ref()
        .is_some_and(|r| r.is::<BlobTooLargeMarker>() || r.is::<OriginError>());
    if !has_typed_marker {
        let kind = e.kind();
        return classify_by_kind(kind, e);
    }
    // We confirmed a typed marker via `is::<>`; consume and chain
    // `Box::downcast` (returns the original box back through `Err`
    // on a mismatch). Both downcasts can only fail if a third typed
    // variant slipped in between the `get_ref` peek and the consume
    // — defensive fall-back rewraps and routes through `classify_by_kind`.
    let kind = e.kind();
    let Some(inner) = e.into_inner() else {
        // Structurally unreachable: `get_ref` returned `Some` so the
        // Repr is `Custom`, and `Custom`'s `into_inner` always returns
        // `Some`. If std's invariant ever changes, fall through.
        return classify_by_kind(kind, io::Error::from(kind));
    };
    let inner = match inner.downcast::<BlobTooLargeMarker>() {
        Ok(marker) => {
            return OriginPullError::Permanent(anyhow::anyhow!(
                "origin body exceeded max_blob_bytes={}",
                marker.max_bytes
            ));
        }
        Err(b) => b,
    };
    match inner.downcast::<OriginError>() {
        Ok(typed) => OriginPullError::Permanent(anyhow::Error::from(*typed)),
        Err(b) => classify_by_kind(kind, io::Error::new(kind, b)),
    }
}

/// Classify a body-phase failure by `io::ErrorKind` alone. Takes the
/// original `io::Error` so `raw_os_error()` and any Display payload
/// flow through unchanged into the surfaced `anyhow::Error`. Used
/// when the typed-marker downcast didn't fire.
fn classify_by_kind(kind: io::ErrorKind, e: io::Error) -> OriginPullError {
    let err = anyhow::Error::new(e);
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
        | io::ErrorKind::Other => OriginPullError::Transient(err),
        _ => OriginPullError::Permanent(err),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // tests
mod tests {
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
}
