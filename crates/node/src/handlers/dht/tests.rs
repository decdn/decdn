use super::*;

// These tests call the real `accept_budget_reached` gate the `serve`
// loop checks at the top of its body, so a future edit to the predicate
// or either const can't silently disable the dispatch-permit-hygiene
// guard (#845). They do NOT exercise the loop wiring end-to-end: the
// loopback harness in `tests/dht_loopback.rs` proves the accept loop
// serves multiple streams on one connection (the store→find roundtrip),
// but does not drive a connection to the 256-stream cap or the 5-minute
// age deadline — driving 256 live round-trips, with the cap a private
// const the integration crate can't reference, isn't worth the wall
// clock. The predicate test below is the behavioral guard for the gate.

/// The gate stays open below both bounds and trips at each: the served
/// count reaching `MAX_DHT_REQUESTS_PER_CONN`, or the connection age
/// reaching `MAX_DHT_CONN_AGE`. Exercises the actual function the loop
/// calls, covering both the count and the (otherwise untested) age branch.
#[test]
fn accept_budget_reached_trips_at_each_bound() {
    // Open while under both bounds (a 1-second age is well under the
    // multi-minute deadline; avoid `MAX_DHT_CONN_AGE - …` so clippy's
    // `unchecked_time_subtraction` doesn't push an `.unwrap()` here).
    assert!(!accept_budget_reached(0, Duration::ZERO));
    assert!(!accept_budget_reached(
        MAX_DHT_REQUESTS_PER_CONN - 1,
        Duration::from_secs(1)
    ));
    // Count bound: trips exactly at the cap and stays tripped above it.
    assert!(accept_budget_reached(
        MAX_DHT_REQUESTS_PER_CONN,
        Duration::ZERO
    ));
    assert!(accept_budget_reached(u32::MAX, Duration::ZERO));
    // Age bound: trips at the deadline regardless of a low served count.
    assert!(accept_budget_reached(0, MAX_DHT_CONN_AGE));
    assert!(accept_budget_reached(
        0,
        MAX_DHT_CONN_AGE + Duration::from_secs(1)
    ));
}

/// Both bounds must be finite and positive: a zero count cap would serve
/// nothing, `u32::MAX` would reopen the unbounded-permit hole #845 closes,
/// and a zero age deadline would break every connection before its first
/// stream.
#[test]
fn budget_bounds_are_sane() {
    // Bind to locals so the comparisons aren't const-folded (clippy's
    // `assertions_on_constants` is fatal under CI's `CARGO_BUILD_WARNINGS=deny`).
    let count_cap = MAX_DHT_REQUESTS_PER_CONN;
    let age = MAX_DHT_CONN_AGE;
    assert!(count_cap > 0, "a zero count cap serves nothing");
    assert!(
        count_cap < u32::MAX,
        "an unbounded count cap reopens the dispatch-permit-pinning hole (#845)"
    );
    assert!(
        age > Duration::ZERO,
        "a zero age deadline would close every connection immediately"
    );
}
