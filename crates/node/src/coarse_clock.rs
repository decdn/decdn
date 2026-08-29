//! A coarse wall clock read off the per-lane hot path (issue #1792 item 4).
//!
//! The voucher/preimage accept path reads the wall clock twice while it holds
//! the per-lane [`Mutex`](tokio::sync::Mutex): the capability-expiry gate and
//! the `last_voucher_at` liveness stamp. Both tolerate roughly one-second
//! resolution — expiry is an on-chain grant deadline, and the stamp only drives
//! the admin "seconds since last voucher" readout — so paying a
//! `SystemTime::now()` syscall inside that critical section is pure per-proof
//! overhead that lengthens the most contended lock hold on the paid path.
//!
//! [`CoarseClock`] replaces those syscalls with one relaxed [`AtomicU64`] load.
//! A background task ([`CoarseClock::spawn_refresher`]) stamps the current wall
//! clock into the cell on a timer; the hot path just reads it.
//!
//! **Fallback.** Until a refresher runs the cell holds `0`, and every read falls
//! back to a live `SystemTime::now()`. So a clock with no refresher attached
//! (every unit and loopback test, which build a handler without one) reports the
//! exact wall clock and behaves precisely as the pre-#1792 code did — the
//! coarsening is a production-only optimization, not a semantic change. `0` is a
//! safe sentinel because a real Unix-millisecond reading is never zero (that is
//! 1970), and even a refresher that somehow wrote `0` would only cost one live
//! read until its next tick.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Live wall clock in milliseconds since the Unix epoch, or `0` if the system
/// clock is before the epoch. The refresher writes this into the cell, and a
/// cell still at its `0` sentinel falls back to it. Mirrors the handler's
/// `unix_millis`; kept local so this module stays a self-contained utility with
/// no dependency back into the handler.
fn live_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// A wall clock the paid-delivery hot path can read with a single relaxed atomic
/// load instead of a `SystemTime::now()` syscall (issue #1792 item 4).
///
/// Refreshed on a timer by [`Self::spawn_refresher`]; reads fall back to a live
/// clock until (and if ever) a refresher runs. See the module docs.
#[derive(Debug, Default)]
pub struct CoarseClock {
    /// Unix milliseconds as of the last refresher tick. `0` means "never
    /// refreshed" and routes every read to [`live_millis`].
    millis: AtomicU64,
}

impl CoarseClock {
    /// A clock with no refresher: every read is live until [`Self::spawn_refresher`]
    /// starts stamping the cell.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            millis: AtomicU64::new(0),
        }
    }

    /// Unix milliseconds — the refreshed value, or a live reading while the cell
    /// still holds its `0` sentinel.
    #[must_use]
    pub fn unix_millis(&self) -> u64 {
        match self.millis.load(Ordering::Relaxed) {
            0 => live_millis(),
            coarse => coarse,
        }
    }

    /// Unix seconds, floored from [`Self::unix_millis`]. Matches
    /// `SystemTime::now().duration_since(UNIX_EPOCH).as_secs()` for the same
    /// instant, so it is a drop-in for the expiry gate's prior
    /// `payment_settlement::unix_now()`.
    #[must_use]
    pub fn unix_seconds(&self) -> u64 {
        self.unix_millis() / 1000
    }

    /// Stamp the current wall clock into the cell. Only [`Self::spawn_refresher`]
    /// calls this.
    fn tick(&self) {
        self.millis.store(live_millis(), Ordering::Relaxed);
    }

    /// Spawn a background task that refreshes `clock` every `interval` until the
    /// last strong reference to it is dropped.
    ///
    /// The task holds only a [`Weak`](std::sync::Weak), so it is
    /// **self-terminating** and needs no [`StopHandle`](crate::stop_handle::StopHandle)
    /// wiring: once every [`ClientHandler`](crate::handlers::client::ClientHandler)
    /// holding the clock is gone (runtime shutdown), the next `upgrade()` fails
    /// and the loop exits — at most one `interval` later. It also owns no queue
    /// and awaits no sender, so the detached-task hazard `StopHandle` exists to
    /// prevent does not apply here.
    ///
    /// The first tick fires immediately (a fresh [`tokio::time::interval`] is
    /// ready at once), so the cell leaves its `0` sentinel as soon as the task is
    /// scheduled; reads before then are live, not wrong.
    pub fn spawn_refresher(clock: &Arc<Self>, interval: Duration) {
        let weak = Arc::downgrade(clock);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // A refresh missed under load is stale, not owed — coalesce rather
            // than fire a burst of catch-up ticks.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let Some(clock) = weak.upgrade() else {
                    break;
                };
                clock.tick();
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// With no refresher the clock reads live — a non-zero millisecond value in
    /// the same ballpark as `SystemTime::now()`, so a handler built without a
    /// refresher (every test) sees the exact wall clock.
    #[test]
    fn unrefreshed_clock_reads_live() {
        let clock = CoarseClock::new();
        let before = live_millis();
        let got = clock.unix_millis();
        let after = live_millis();
        assert!(
            before <= got && got <= after,
            "live fallback {got} must sit within [{before}, {after}]"
        );
        assert!(got > 0, "a live reading is never the zero sentinel");
    }

    /// A stamped cell is what reads return, and seconds floor from it exactly.
    #[test]
    fn ticked_cell_is_read_back_and_seconds_floor() {
        let clock = CoarseClock::new();
        clock.tick();
        let ms = clock.millis.load(Ordering::Relaxed);
        assert!(ms > 0, "tick must leave a real reading, not the sentinel");
        assert_eq!(clock.unix_millis(), ms, "reads return the stamped value");
        assert_eq!(
            clock.unix_seconds(),
            ms / 1000,
            "seconds floor from the stored milliseconds"
        );
    }

    /// The refresher stamps the cell shortly after it is spawned and stops once
    /// the last strong reference drops (the `Weak` upgrade fails), so it leaks no
    /// task past the clock's lifetime.
    #[tokio::test]
    async fn refresher_stamps_then_self_terminates() {
        let clock = Arc::new(CoarseClock::new());
        assert_eq!(
            clock.millis.load(Ordering::Relaxed),
            0,
            "fresh clock starts at the sentinel"
        );
        CoarseClock::spawn_refresher(&clock, Duration::from_millis(5));
        // Let the immediate first tick land.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            clock.millis.load(Ordering::Relaxed) > 0,
            "the refresher must leave the sentinel promptly"
        );
        // Drop the only strong ref; the task's next upgrade fails and it exits.
        let weak = Arc::downgrade(&clock);
        drop(clock);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(
            weak.strong_count(),
            0,
            "no strong reference may outlive the dropped clock"
        );
    }
}
