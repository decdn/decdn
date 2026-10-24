use super::*;

const DAY: u64 = 86_400;

#[test]
fn charges_count_against_the_days_allowance() {
    let allowance = Allowance::new(100);
    let now = 10 * DAY + 100;
    assert_eq!(allowance.remaining(now), Ok(100));
    assert!(allowance.charge(60, now), "60 of 100 leaves room");
    assert_eq!(allowance.remaining(now), Ok(40));
    // The charge that reaches the bound says so, and the next request learns
    // when the day turns.
    assert!(
        !allowance.charge(60, now + 5),
        "120 of 100 is past the bound"
    );
    assert_eq!(allowance.remaining(now + 5), Err(DAY - 105));
}

#[test]
fn the_allowance_resets_with_the_utc_day() {
    let allowance = Allowance::new(100);
    let now = 10 * DAY + DAY - 1;
    assert!(!allowance.charge(100, now));
    assert_eq!(allowance.remaining(now), Err(1));
    assert_eq!(allowance.remaining(now + 1), Ok(100));
    // A charge in the new day starts the new day's count.
    assert!(allowance.charge(10, now + 1));
    assert_eq!(allowance.remaining(now + 1), Ok(90));
}

#[test]
fn an_unlimited_allowance_never_runs_out() {
    let allowance = Allowance::new(u64::MAX);
    assert!(allowance.charge(u64::MAX, 0));
    assert!(allowance.charge(1, 0));
    assert_eq!(allowance.remaining(0), Ok(u64::MAX));
}
