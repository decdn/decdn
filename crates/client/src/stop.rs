//! When a command gives up (ADR 039 § Failure handling: reassign-only tail,
//! the stop policy).
//!
//! The clock measures time since the last verified byte anywhere in the
//! command, never time since the start. A command in a terminal has no limit:
//! the human watches the bar and presses Ctrl-C. A script gets
//! [`SCRIPT_GIVE_UP`](crate::stop::SCRIPT_GIVE_UP). `--give-up-after-secs`
//! overrides both.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

use tokio::time::{Duration, Instant};

/// The no-progress limit when stderr is not a terminal.
pub const SCRIPT_GIVE_UP: Duration = Duration::from_mins(10);

/// The instant of the command's last verified byte.
///
/// A [`ClockHold`] stops the clock: while any hold is alive the command counts
/// as making progress, for time spent waiting on the command's own consumer
/// rather than on a source.
#[derive(Debug)]
pub struct ProgressClock {
    last: Mutex<Instant>,
    holds: AtomicUsize,
}

impl ProgressClock {
    /// A clock whose last progress is now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            last: Mutex::new(Instant::now()),
            holds: AtomicUsize::new(0),
        }
    }

    /// Record a verified byte now.
    pub fn tick(&self) {
        *self.last.lock().unwrap_or_else(PoisonError::into_inner) = Instant::now();
    }

    /// The instant of the last verified byte, or now while a hold is alive.
    #[must_use]
    pub fn last(&self) -> Instant {
        if self.holds.load(Ordering::Acquire) > 0 {
            return Instant::now();
        }
        *self.last.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stop the clock until the returned hold drops. The drop counts as
    /// progress, so the no-progress limit restarts from it.
    #[must_use]
    pub fn hold(&self) -> ClockHold<'_> {
        self.holds.fetch_add(1, Ordering::AcqRel);
        ClockHold(self)
    }
}

/// Stops a [`ProgressClock`] while it lives. See [`ProgressClock::hold`].
#[derive(Debug)]
pub struct ClockHold<'a>(&'a ProgressClock);

impl Drop for ClockHold<'_> {
    fn drop(&mut self) {
        self.0.tick();
        self.0.holds.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Default for ProgressClock {
    fn default() -> Self {
        Self::new()
    }
}

/// When the command stops for lack of progress.
#[derive(Debug, Clone)]
pub struct StopPolicy {
    /// The no-progress limit. `None` waits until the caller interrupts.
    pub give_up_after: Option<Duration>,
    /// The command-wide progress clock.
    pub clock: std::sync::Arc<ProgressClock>,
}

impl StopPolicy {
    /// The policy for a command whose stderr `is_terminal`, with the user's
    /// optional `--give-up-after-secs` override.
    #[must_use]
    pub fn new(
        is_terminal: bool,
        give_up_after: Option<Duration>,
        clock: std::sync::Arc<ProgressClock>,
    ) -> Self {
        let give_up_after = give_up_after.or((!is_terminal).then_some(SCRIPT_GIVE_UP));
        Self {
            give_up_after,
            clock,
        }
    }

    /// Resolve once the command has made no progress for the limit. Never
    /// resolves without a limit.
    pub async fn expired(&self) -> GaveUp {
        let Some(limit) = self.give_up_after else {
            return std::future::pending().await;
        };
        loop {
            let deadline = self.clock.last() + limit;
            tokio::time::sleep_until(deadline).await;
            if self.clock.last() + limit <= Instant::now() {
                return GaveUp { idle: limit };
            }
        }
    }
}

/// The command made no progress for `idle` and stopped. A rerun resumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GaveUp {
    /// How long the command went without a verified byte.
    pub idle: Duration,
}

impl std::fmt::Display for GaveUp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let secs = self.idle.as_secs();
        if secs >= 60 && secs.is_multiple_of(60) {
            write!(f, "no progress for {}m; rerun to resume", secs / 60)
        } else {
            write!(f, "no progress for {secs}s; rerun to resume")
        }
    }
}

impl std::error::Error for GaveUp {}

#[cfg(test)]
mod tests {
    use super::{ProgressClock, SCRIPT_GIVE_UP, StopPolicy};
    use std::sync::Arc;
    use tokio::time::{Duration, Instant};

    #[test]
    fn a_terminal_waits_forever_and_a_script_gives_up_after_ten_minutes() {
        let clock = Arc::new(ProgressClock::new());
        assert_eq!(
            StopPolicy::new(true, None, Arc::clone(&clock)).give_up_after,
            None
        );
        assert_eq!(
            StopPolicy::new(false, None, Arc::clone(&clock)).give_up_after,
            Some(SCRIPT_GIVE_UP)
        );
        let custom = Some(Duration::from_secs(30));
        assert_eq!(
            StopPolicy::new(true, custom, Arc::clone(&clock)).give_up_after,
            custom
        );
        assert_eq!(StopPolicy::new(false, custom, clock).give_up_after, custom);
    }

    #[tokio::test(start_paused = true)]
    async fn it_expires_after_the_limit_with_no_progress() {
        let clock = Arc::new(ProgressClock::new());
        let policy = StopPolicy::new(false, Some(Duration::from_mins(1)), clock);
        let start = Instant::now();
        let gave_up = policy.expired().await;
        assert_eq!(Instant::now() - start, Duration::from_mins(1));
        assert_eq!(gave_up.idle, Duration::from_mins(1));
    }

    #[tokio::test(start_paused = true)]
    async fn progress_pushes_the_deadline_out() {
        let clock = Arc::new(ProgressClock::new());
        let policy = StopPolicy::new(false, Some(Duration::from_mins(1)), Arc::clone(&clock));
        let start = Instant::now();
        let ticker = async {
            tokio::time::sleep(Duration::from_secs(50)).await;
            clock.tick();
        };
        let ((), gave_up) = tokio::join!(ticker, policy.expired());
        assert_eq!(Instant::now() - start, Duration::from_secs(110));
        assert_eq!(gave_up.idle, Duration::from_mins(1));
    }

    #[tokio::test(start_paused = true)]
    async fn a_hold_stops_the_clock_and_its_drop_restarts_the_limit() {
        let clock = Arc::new(ProgressClock::new());
        let policy = StopPolicy::new(false, Some(Duration::from_mins(1)), Arc::clone(&clock));
        let start = Instant::now();
        let holder = async {
            let hold = clock.hold();
            tokio::time::sleep(Duration::from_mins(5)).await;
            drop(hold);
        };
        let ((), gave_up) = tokio::join!(holder, policy.expired());
        assert_eq!(Instant::now() - start, Duration::from_mins(6));
        assert_eq!(gave_up.idle, Duration::from_mins(1));
    }

    #[test]
    fn the_message_names_the_idle_time_and_the_remedy() {
        let gave_up = super::GaveUp {
            idle: Duration::from_mins(10),
        };
        assert_eq!(gave_up.to_string(), "no progress for 10m; rerun to resume");
        let short = super::GaveUp {
            idle: Duration::from_secs(45),
        };
        assert_eq!(short.to_string(), "no progress for 45s; rerun to resume");
    }

    /// A limit that is not a whole number of minutes prints in seconds, so
    /// 90 s never reads as "1m".
    #[test]
    fn a_limit_between_whole_minutes_prints_in_seconds() {
        let odd = super::GaveUp {
            idle: Duration::from_secs(90),
        };
        assert_eq!(odd.to_string(), "no progress for 90s; rerun to resume");
        let whole = super::GaveUp {
            idle: Duration::from_mins(2),
        };
        assert_eq!(whole.to_string(), "no progress for 2m; rerun to resume");
    }
}
