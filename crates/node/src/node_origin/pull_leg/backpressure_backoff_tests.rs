use std::time::Duration;

use super::{BACKPRESSURE_BACKOFF_MAX, backpressure_backoff};
use crate::node_origin::ranged_pull::MAX_BACKPRESSURE_RETRIES;

/// The schedule ADR 039 states: 250 ms, doubling, to a 4 s limit.
#[test]
fn backoff_doubles_from_250_ms_to_a_4_s_limit() {
    let schedule: Vec<Duration> = (1..=7).map(backpressure_backoff).collect();
    let expected: Vec<Duration> = [250, 500, 1000, 2000, 4000, 4000, 4000]
        .into_iter()
        .map(Duration::from_millis)
        .collect();
    assert_eq!(schedule, expected);
    assert_eq!(backpressure_backoff(0), Duration::from_millis(250));
    assert_eq!(backpressure_backoff(u32::MAX), BACKPRESSURE_BACKOFF_MAX);
}

/// The whole budget is the 11.75 s the constant docs name.
#[test]
fn the_wait_budget_totals_11_75_s() {
    let total: Duration = (1..=MAX_BACKPRESSURE_RETRIES)
        .map(backpressure_backoff)
        .sum();
    assert_eq!(total, Duration::from_millis(11_750));
}
