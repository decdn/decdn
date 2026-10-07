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
    const THREADS: usize = 8;
    const EVENTS_PER_THREAD: u64 = 500;
    let throttle = WarnThrottle::new(Duration::from_mins(5));
    let admitted = std::sync::atomic::AtomicU64::new(0);
    let reported = std::sync::atomic::AtomicU64::new(0);
    // Release every thread at once so they contend for the first window,
    // rather than the first-spawned thread finishing before the rest start.
    let start = std::sync::Barrier::new(THREADS);
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
    // Two lines in total: the winner's inside the scope and the successor's
    // here. Every other event is counted on exactly one of them, so the two
    // counts sum to every event but the two lines themselves.
    let winner = reported.load(Ordering::Relaxed);
    let expected = u64::try_from(THREADS)
        .ok()
        .map(|threads| threads * EVENTS_PER_THREAD - 1);
    assert_eq!(
        throttle.admit().map(|successor| winner + successor),
        expected,
        "the winner's count plus its successor's covers every swallowed event"
    );
}

/// The gate is atomic: only the first event of a fresh throttle can race,
/// so the test above contends for it once per run and a plain `store` in
/// place of the compare-exchange survives most runs. Repeating the
/// first-window race on a fresh throttle each round, with a spin release
/// instead of a `Barrier` (whose condvar wake-up skew dwarfs the
/// load-to-store window), kills that mutant on every run.
#[test]
fn racing_first_events_never_both_warn() {
    use std::sync::atomic::AtomicBool;

    const THREADS: usize = 8;
    const ROUNDS: usize = 100;
    for round in 0..ROUNDS {
        let throttle = WarnThrottle::new(Duration::from_mins(5));
        let admitted = std::sync::atomic::AtomicU64::new(0);
        let ready = std::sync::atomic::AtomicUsize::new(0);
        let go = AtomicBool::new(false);
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    ready.fetch_add(1, Ordering::Relaxed);
                    while !go.load(Ordering::Relaxed) {
                        std::hint::spin_loop();
                    }
                    if throttle.admit().is_some() {
                        admitted.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
            while ready.load(Ordering::Relaxed) < THREADS {
                std::hint::spin_loop();
            }
            go.store(true, Ordering::Relaxed);
        });
        assert_eq!(
            admitted.load(Ordering::Relaxed),
            1,
            "round {round}: exactly one racing first event warns"
        );
    }
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
