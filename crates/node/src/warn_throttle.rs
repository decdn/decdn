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
    /// compare-exchange returns `None`, and the winner's count already includes
    /// the loser's event, because every caller counts before it checks.
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
mod tests {
    use super::*;

    #[test]
    fn warn_fires_on_the_first_event_then_waits_out_the_window() {
        let interval = Duration::from_mins(5);
        // Nothing warned yet: the very first event must be visible, not swallowed
        // until a window elapses from process start.
        assert!(should_warn_now(0, 0, interval));
        assert!(should_warn_now(1_000_000, 0, interval));

        let last = 1_000_000;
        // Inside the window — suppressed.
        assert!(!should_warn_now(last, last, interval));
        assert!(!should_warn_now(last + 299_999, last, interval));
        // Exactly at the boundary, and past it — due.
        assert!(should_warn_now(last + 300_000, last, interval));
        assert!(should_warn_now(last + 600_000, last, interval));
    }

    #[test]
    fn warn_suppresses_when_now_reads_behind_the_last_line() {
        // A caller that read the clock before a racing winner stamped it sees
        // `now < last`. The saturating subtraction yields 0 elapsed: suppress.
        let interval = Duration::from_mins(5);
        assert!(!should_warn_now(500, 1_000_000, interval));
    }

    /// Racing callers: exactly one warns per window, and every event lands on
    /// a line. The winner's `swap` races the losers' `fetch_add`s, so the
    /// split between the winner's count and its successor's is scheduling-
    /// dependent; their sum is not, and that is what the test pins. Kills a
    /// `fetch_add` moved after the gate (losers uncounted) on every run, and a
    /// plain `store` in place of the compare-exchange (two winners) only on
    /// the runs where two threads land inside the load-to-store window.
    #[test]
    fn racing_callers_admit_one_line_and_count_every_event() {
        const THREADS: u64 = 8;
        const EVENTS_PER_THREAD: u64 = 500;
        let throttle = WarnThrottle::new(Duration::from_mins(5));
        let admitted = std::sync::atomic::AtomicU64::new(0);
        let reported = std::sync::atomic::AtomicU64::new(0);
        // Release every thread at once so they contend for the first window,
        // rather than the first-spawned thread finishing before the rest start.
        let start = std::sync::Barrier::new(usize::try_from(THREADS).unwrap_or(usize::MAX));
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    start.wait();
                    for _ in 0..EVENTS_PER_THREAD {
                        if let Some(suppressed) = throttle.admit() {
                            admitted.fetch_add(1, Ordering::Relaxed);
                            reported.fetch_add(suppressed, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(admitted.load(Ordering::Relaxed), 1, "one line per window");
        throttle.force_open();
        // Two lines were admitted; every other event is suppressed on one of
        // them, never on both and never on neither.
        let winner = reported.load(Ordering::Relaxed);
        assert_eq!(
            throttle.admit().map(|successor| winner + successor),
            Some(THREADS * EVENTS_PER_THREAD - 1),
            "the winner's count plus its successor's covers every swallowed event"
        );
    }

    /// The suppressed count IS the feature — it is what distinguishes one
    /// misbehaving peer from a node refusing everyone.
    ///
    /// Mutants this kills: dropping the `-1` (every line off by one); moving the
    /// `fetch_add` after the gate (the first warn reports 0 forever and nothing
    /// accumulates); `swap` → `load` (the count grows monotonically and
    /// "suppressed since the last line" becomes meaningless).
    #[test]
    fn admit_reports_exactly_what_it_swallowed() {
        let throttle = WarnThrottle::new(Duration::from_mins(5));

        // First event is always visible, and has swallowed nothing.
        assert_eq!(throttle.admit(), Some(0));
        // Inside the window: silent, but counting.
        assert_eq!(throttle.admit(), None);
        assert_eq!(throttle.admit(), None);

        throttle.force_open();
        assert_eq!(
            throttle.admit(),
            Some(2),
            "the line must report the two it swallowed, not counting itself"
        );

        // And the counter reset, so the next window starts from zero.
        assert_eq!(throttle.admit(), None);
        throttle.force_open();
        assert_eq!(throttle.admit(), Some(1));
    }

    /// Two throttles never share a window: one cause firing constantly must not
    /// silence the other.
    #[test]
    fn separate_throttles_keep_separate_windows() {
        let a = WarnThrottle::new(Duration::from_mins(5));
        let b = WarnThrottle::new(Duration::from_mins(5));
        assert_eq!(a.admit(), Some(0));
        assert_eq!(a.admit(), None);
        assert_eq!(
            b.admit(),
            Some(0),
            "b's first event warns despite a's open window"
        );
    }
}
