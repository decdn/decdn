use super::*;

#[test]
fn holder_counts_accumulate() {
    let timings = FetchTimings::start();
    timings.set_holders_start(2);
    timings.holder_joined();
    timings.holder_joined();
    assert_eq!(timings.holders(), (2, 2));
}

#[test]
fn each_mark_keeps_its_first_instant() {
    let started = Instant::now();
    let timings = FetchTimings::started_at(started);
    timings.mark_at(Mark::Resolved, started + Duration::from_millis(40));
    timings.mark_at(Mark::Resolved, started + Duration::from_millis(90));
    timings.mark_at(Mark::FirstByte, started + Duration::from_millis(250));

    let s = timings.snapshot();
    assert_eq!(s.resolved, Some(40));
    assert_eq!(s.first_byte, Some(250));
}

#[test]
fn an_unreached_mark_is_absent() {
    let started = Instant::now();
    let timings = FetchTimings::started_at(started);
    timings.mark_at(Mark::Endpoint, started + Duration::from_millis(3));

    let s = timings.snapshot();
    assert_eq!(s.endpoint, Some(3));
    assert_eq!(s.probe_start, None);
    assert_eq!(s.lane, None);
    assert_eq!(s.first_byte, None);
}
