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
