//! A windowed gate for `warn!` lines that a remote peer or a routine condition
//! can fire at any rate.
//!
//! Warning per occurrence is farmable into log spam by exactly the peer that
//! triggers it. A one-shot latch is wrong in the other direction: these
//! conditions fire legitimately and repeatedly, so a permanently latched warning
//! is as invisible as none. Hence a window, with the swallowed count carried on
//! the line so a reader can tell one misbehaving peer from a node refusing
//! everyone.
//!
//! Unkeyed on purpose. A per-peer limiter means an unboundedly growing map with
//! its own prune sweeps and metrics (`crate::rate_limit`) — a lot of machinery to
//! rate-limit a log line. The aggregate answers the triage question. Every gate
//! sits beside a counter that records each event, throttled or not, so a
//! swallowed line never hides an event from the metrics.
//!
//! Causes with different remedies own separate throttles. Sharing one window
//! would let whichever cause fires more often hold it open and silence the
//! other entirely, and would mix both causes into the `suppressed` count each
//! line reports. Causes with one remedy may share a window.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Milliseconds on a monotonic clock, plus one so `0` stays free as the
/// never-warned sentinel. Monotonic so a wall-clock step (NTP, VM migration)
/// can neither open the window early nor hold it shut.
fn now_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let elapsed = START.get_or_init(Instant::now).elapsed().as_millis();
    u64::try_from(elapsed).unwrap_or(u64::MAX).saturating_add(1)
}

/// One `warn!` per `interval`, counting what it swallows in between.
#[derive(Debug)]
pub(crate) struct WarnThrottle {
    interval: Duration,
    /// [`now_ms`] at the last admitted line. `0` means no line was ever
    /// admitted.
    last_warn_ms: AtomicU64,
    /// Events recorded since the last admitted line, including the one that
    /// will be admitted next.
    suppressed: AtomicU64,
}

impl WarnThrottle {
    /// A throttle that admits one line per `interval`. The first event is always
    /// admitted.
    pub(crate) const fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_warn_ms: AtomicU64::new(0),
            suppressed: AtomicU64::new(0),
        }
    }

    /// The window this throttle enforces, for the `interval` field of the line.
    pub(crate) const fn interval(&self) -> Duration {
        self.interval
    }

    /// Record one event and decide whether it gets a `warn!`. Returns
    /// `Some(suppressed_since_last)` when the caller should warn, and `None` when
    /// the line is swallowed.
    ///
    /// Racing callers never both warn for one window: the loser of the
    /// compare-exchange returns `None`, and its event is still counted — on the
    /// winner's line or on the next one — because every caller counts before it
    /// checks.
    pub(crate) fn admit(&self) -> Option<u64> {
        self.suppressed.fetch_add(1, Ordering::Relaxed);
        let now_ms = now_ms();
        let last = self.last_warn_ms.load(Ordering::Relaxed);
        if !should_warn_now(now_ms, last, self.interval) {
            return None;
        }
        if self
            .last_warn_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return None;
        }
        Some(self.suppressed.swap(0, Ordering::Relaxed).saturating_sub(1))
    }

    /// Force the window open so the next [`Self::admit`] warns. Resets to the
    /// never-warned sentinel, which opens the gate whatever the clock reads;
    /// the swallowed count is untouched.
    #[cfg(test)]
    pub(crate) fn force_open(&self) {
        self.last_warn_ms.store(0, Ordering::Relaxed);
    }
}

/// Whether a `warn!` is due: `interval` has elapsed since `last_warn_ms`, or
/// nothing has ever been warned (`last_warn_ms == 0`).
///
/// Pure and millisecond-based so the window is testable without sleeping. A
/// `now_ms` behind `last_warn_ms` yields `false` (via the saturating
/// subtraction), suppressing rather than spamming. [`now_ms`] is monotonic, so
/// that case needs a racing caller that read the clock before the winner
/// stamped it.
fn should_warn_now(now_ms: u64, last_warn_ms: u64, interval: Duration) -> bool {
    if last_warn_ms == 0 {
        return true;
    }
    let elapsed = now_ms.saturating_sub(last_warn_ms);
    elapsed >= u64::try_from(interval.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
