//! Origin retry policy with exponential backoff and jitter (#285).
//!
//! `ReadGuard` runs each read of an origin's ranged objects under the
//! origin's circuit breaker and this retry loop.
//!
//! Wraps a single pull-through attempt in a bounded retry loop. The adapter
//! (or the engine's per-attempt closure) classifies each failure as
//! [`OriginPullError::Transient`] or [`OriginPullError::Permanent`]; this
//! module only decides *how many* transient retries to run and *how long*
//! to sleep between them.
//!
//! ## Coalescing interaction and the retry-amplification bound (#1615)
//!
//! [`crate::CacheEngine::populate`] coalesces concurrent misses for the same hash
//! through a single owner task (#305). The retry loop runs *inside* that owner;
//! waiters block on the shared `Notify` until the owner either succeeds or
//! exhausts. After an owner exhausts, one waiter re-elects itself as the next
//! owner and runs its own full retry budget — so, taken alone, a burst of `N`
//! concurrent waiters on a down origin could drive up to `N` *sequential* full
//! retry budgets.
//!
//! That amplification is bounded, but by the per-origin circuit-breaker
//! ([`crate::circuit_breaker`], #963) rather than by anything in this module.
//! Each budget-exhaustion against a down origin records one `Unavailable`
//! outcome; after `failure_threshold` consecutive exhaustions the breaker
//! trips OPEN and every later waiter short-circuits *before* the retry loop
//! runs. The worst case is therefore `failure_threshold` full budgets per
//! origin per cooldown window — a constant, independent of `N`. The
//! integration test `coalesced_owner_exhaustion_bounded_by_circuit_breaker`
//! pins this, and also pins the O(`N`) fallback that returns when an operator
//! disables the breaker.
//!
//! A tighter per-hash bound — the owner's failure *shared* to all waiters so
//! not even `failure_threshold` budgets run — is deliberately NOT implemented.
//! The circuit-breaker already sheds the outage load, so a shared budget is
//! not worth its risk on this hot, cancellation-sensitive path.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use decdn_config_types::RetryPolicy;
use iroh_blobs::Hash;

use crate::circuit_breaker::{Admission, OriginBreaker, OriginOutcome};
use crate::error::OriginPullError;
use crate::metrics::CacheMetrics;

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

/// Whether a terminal pull-through failure left the origin looking
/// *unreachable* (a transient that exhausted the retry budget) or merely
/// unable to serve *this* object (an immediate permanent — 4xx, decode,
/// cap). Used by the per-origin circuit-breaker (#963) to decide whether
/// an attempt counts toward the trip threshold; the plain `Result` the
/// engine folds this into collapses both into `OriginPullError::Permanent`,
/// erasing the distinction the breaker needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TerminalFailure {
    /// The terminal error was a transient that exhausted `max_retries`
    /// (or a `max_retries = 0` single transient). The origin looks
    /// unavailable.
    TransientExhausted,
    /// The terminal error was an immediate `Permanent` — the origin
    /// responded but can't serve this object. Not an outage.
    Permanent,
}

/// The retry loop, with a terminal-failure classification for the
/// circuit-breaker (#963). On `Ok`, the second tuple element is `None`.
/// On `Err`, it carries whether the failure was a transient-exhaustion
/// (origin-unavailable) or an immediate permanent (per-object).
pub(crate) async fn run_with_retry_classified<F, Fut, T>(
    policy: RetryPolicy,
    metrics: Option<&Arc<CacheMetrics>>,
    hash: Hash,
    mut attempt_fn: F,
) -> (Result<T, OriginPullError>, Option<TerminalFailure>)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, OriginPullError>>,
{
    let mut attempt: u32 = 0;
    loop {
        match attempt_fn().await {
            Ok(value) => return (Ok(value), None),
            Err(OriginPullError::Permanent(e)) => {
                return (
                    Err(OriginPullError::Permanent(e)),
                    Some(TerminalFailure::Permanent),
                );
            }
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
                        error = %format_args!("{e:#}"),
                        "origin fetch exhausted retry budget; surfacing as permanent",
                    );
                    return (
                        Err(OriginPullError::Permanent(e)),
                        Some(TerminalFailure::TransientExhausted),
                    );
                }
                let sleep = delay_for(policy, attempt);
                let sleep_ms = u64::try_from(sleep.as_millis()).unwrap_or(u64::MAX);
                tracing::warn!(
                    %hash,
                    attempt = attempt + 1,
                    of = policy.max_retries,
                    sleep_ms,
                    error = %format_args!("{e:#}"),
                    "origin fetch transient failure; retrying after backoff",
                );
                tokio::time::sleep(sleep).await;
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

/// What every read of one origin's ranged objects runs under: that origin's
/// circuit breaker (#963) and the retry policy (`cache.origin_retry`). The
/// `{H}.sizeproof`, `{H}.obao4` and range window reads all go through
/// [`Self::run`], so one transport blip does not refuse a paid stream, and
/// a down origin trips its breaker and stops costing backoff.
#[derive(Debug, Clone)]
pub(crate) struct ReadGuard {
    /// The origin's breaker; `None` reads with no breaker.
    breaker: Option<Arc<OriginBreaker>>,
    policy: RetryPolicy,
    metrics: Option<Arc<CacheMetrics>>,
}

impl ReadGuard {
    /// A guard over `breaker` under `policy`, metered to `metrics`.
    pub(crate) const fn new(
        breaker: Option<Arc<OriginBreaker>>,
        policy: RetryPolicy,
        metrics: Option<Arc<CacheMetrics>>,
    ) -> Self {
        Self {
            breaker,
            policy,
            metrics,
        }
    }

    /// A guard with no breaker and no retry: each read runs once.
    #[cfg(test)]
    pub(crate) const fn none() -> Self {
        Self::new(None, RetryPolicy::disabled(), None)
    }

    /// Run `read` for `hash` under the breaker and the retry policy, to
    /// `deadline` (`None` runs unbounded).
    ///
    /// An open breaker answers [`ReadFailure::Origin`] with an
    /// [`OriginPullError::Transient`] at once, with no read and no backoff. A
    /// transient fault is retried with backoff ([`run_with_retry_classified`]).
    /// One that exhausts the budget stays [`OriginPullError::Transient`], so a
    /// caller that waits out a transient fault (the presence memo, the rescan
    /// carry-forward) still can, and it counts toward the breaker's trip
    /// threshold. A permanent fault returns at once and counts as a reachable
    /// origin.
    ///
    /// `deadline` covers the read and every retry. A read still running at
    /// the deadline is cut off and answers [`ReadFailure::Deadline`]. It
    /// counts toward the trip threshold as an unavailable origin, so an
    /// origin that stalls trips its breaker as one that errors does.
    pub(crate) async fn run<T, F, Fut>(
        &self,
        hash: Hash,
        deadline: Option<tokio::time::Instant>,
        read: F,
    ) -> Result<T, ReadFailure>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, OriginPullError>>,
    {
        let guard = match self.breaker.as_deref().map(OriginBreaker::acquire) {
            Some(Admission::ShortCircuit) => {
                return Err(ReadFailure::Origin(OriginPullError::Transient(
                    anyhow::anyhow!(
                        "the origin's circuit breaker is open after repeated failures; the \
                         node reads it again after the cooldown (cache.circuit_breaker)"
                    ),
                )));
            }
            Some(Admission::Proceed(_state, guard)) => Some(guard),
            None => None,
        };
        let attempts = run_with_retry_classified(self.policy, self.metrics.as_ref(), hash, read);
        let finished = match deadline {
            Some(at) => tokio::time::timeout_at(at, attempts).await.ok(),
            None => Some(attempts.await),
        };
        let Some((outcome, terminal)) = finished else {
            if let Some(guard) = guard {
                guard.record(OriginOutcome::Unavailable);
            }
            return Err(ReadFailure::Deadline);
        };
        if let Some(guard) = guard {
            guard.record(match terminal {
                Some(TerminalFailure::TransientExhausted) => OriginOutcome::Unavailable,
                Some(TerminalFailure::Permanent) | None => OriginOutcome::Available,
            });
        }
        match (outcome, terminal) {
            (Err(e), Some(TerminalFailure::TransientExhausted)) => Err(ReadFailure::Origin(
                OriginPullError::Transient(e.into_inner()),
            )),
            (outcome, _) => outcome.map_err(ReadFailure::Origin),
        }
    }
}

/// Why a [`ReadGuard::run`] read failed.
#[derive(Debug)]
pub(crate) enum ReadFailure {
    /// The origin answered with an error, or its breaker is open.
    Origin(OriginPullError),
    /// The read and its retries ran past the caller's deadline. A
    /// waitable fault: the origin may answer given longer.
    Deadline,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // tests
mod tests;
