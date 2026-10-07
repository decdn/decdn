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
