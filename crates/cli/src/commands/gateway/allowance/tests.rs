use super::*;

const DAY: u64 = 86_400;

#[test]
fn charges_count_against_the_days_allowance() {
    let allowance = Allowance::with_namespaces(100, []);
    let now = 10 * DAY + 100;
    assert_eq!(allowance.remaining(None, now), Ok(100));
    assert!(allowance.charge(None, 60, now), "60 of 100 leaves room");
    assert_eq!(allowance.remaining(None, now), Ok(40));
    // The charge that reaches the bound says so, and the next request learns
    // when the day turns.
    assert!(
        !allowance.charge(None, 60, now + 5),
        "120 of 100 is past the bound"
    );
    assert_eq!(allowance.remaining(None, now + 5), Err(DAY - 105));
}

#[test]
fn the_allowance_resets_with_the_utc_day() {
    let allowance = Allowance::with_namespaces(100, []);
    let now = 10 * DAY + DAY - 1;
    assert!(!allowance.charge(None, 100, now));
    assert_eq!(allowance.remaining(None, now), Err(1));
    assert_eq!(allowance.remaining(None, now + 1), Ok(100));
    // A charge in the new day starts the new day's count.
    assert!(allowance.charge(None, 10, now + 1));
    assert_eq!(allowance.remaining(None, now + 1), Ok(90));
}

#[test]
fn an_unlimited_allowance_never_runs_out() {
    let allowance = Allowance::with_namespaces(u64::MAX, []);
    assert!(allowance.charge(None, u64::MAX, 0));
    assert!(allowance.charge(None, 1, 0));
    assert_eq!(allowance.remaining(None, 0), Ok(u64::MAX));
}

/// A namespace with an allowance of its own spends that and nothing else:
/// its bucket replaces the shared one for its requests, and the shared
/// bucket does not move.
#[test]
fn a_namespace_allowance_replaces_the_shared_one_for_its_requests() {
    let allowance = Allowance::with_namespaces(100, [(7, 30)]);
    let now = 10 * DAY;
    assert_eq!(allowance.remaining(Some(7), now), Ok(30));
    assert!(!allowance.charge(Some(7), 30, now));
    assert!(allowance.remaining(Some(7), now).is_err());
    assert_eq!(
        allowance.remaining(None, now),
        Ok(100),
        "the shared bucket is untouched"
    );
    assert!(!allowance.charge(None, 100, now));
    assert!(allowance.remaining(Some(7), now).is_err());
}

/// A namespace without an allowance of its own shares the one bucket.
#[test]
fn a_namespace_without_its_own_allowance_shares() {
    let allowance = Allowance::with_namespaces(100, [(7, 30)]);
    let now = 10 * DAY;
    assert!(allowance.charge(Some(9), 60, now));
    assert_eq!(allowance.remaining(None, now), Ok(40));
    assert_eq!(allowance.remaining(Some(9), now), Ok(40));
    assert_eq!(allowance.remaining(Some(7), now), Ok(30));
}
