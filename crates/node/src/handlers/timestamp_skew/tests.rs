use super::{MAX_REQUEST_TIMESTAMP_SKEW_US, excessive_skew_us, now_us};

const NOW: u64 = 1_700_000_000_000_000;

#[test]
fn a_timestamp_at_now_passes() {
    assert_eq!(excessive_skew_us(NOW, NOW), None);
}

#[test]
fn the_bound_is_inclusive_on_both_sides() {
    assert_eq!(
        excessive_skew_us(NOW - MAX_REQUEST_TIMESTAMP_SKEW_US, NOW),
        None
    );
    assert_eq!(
        excessive_skew_us(NOW + MAX_REQUEST_TIMESTAMP_SKEW_US, NOW),
        None
    );
}

#[test]
fn one_microsecond_past_the_bound_is_refused_both_ways() {
    let past = NOW - MAX_REQUEST_TIMESTAMP_SKEW_US - 1;
    let future = NOW + MAX_REQUEST_TIMESTAMP_SKEW_US + 1;
    assert_eq!(
        excessive_skew_us(past, NOW),
        Some(MAX_REQUEST_TIMESTAMP_SKEW_US + 1)
    );
    assert_eq!(
        excessive_skew_us(future, NOW),
        Some(MAX_REQUEST_TIMESTAMP_SKEW_US + 1)
    );
}

#[test]
fn extreme_timestamps_do_not_overflow() {
    assert_eq!(excessive_skew_us(u64::MAX, 0), Some(u64::MAX));
    assert_eq!(excessive_skew_us(0, u64::MAX), Some(u64::MAX));
}

#[test]
fn now_us_reads_a_current_clock() {
    // Loose sanity bound: after 2020-01-01 and before year ~2200.
    let now = now_us();
    assert!(now > 1_577_836_800_000_000);
    assert!(now < 7_258_118_400_000_000);
}
