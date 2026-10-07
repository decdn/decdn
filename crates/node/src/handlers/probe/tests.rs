use super::{fold_origin_held, stake_lane_reserved_out};

/// A store-backed `has_blob: true` is preserved untouched, even if the
/// origin also holds the blob — the store size wins (it's already held).
#[test]
fn fold_origin_held_keeps_store_hit() {
    assert_eq!(
        fold_origin_held((true, Some(100)), Some(200)),
        (true, Some(100))
    );
    assert_eq!(fold_origin_held((true, None), Some(200)), (true, None));
}

/// A store miss + origin hold → advertise with the origin's size (#1130).
#[test]
fn fold_origin_held_promotes_origin_content() {
    assert_eq!(
        fold_origin_held((false, None), Some(4096)),
        (true, Some(4096))
    );
}

/// A store miss with no origin hold stays a true negative.
#[test]
fn fold_origin_held_absent_stays_false() {
    assert_eq!(fold_origin_held((false, None), None), (false, None));
}

/// With no reservation configured (`reserved == 0`) the gate is a
/// no-op: even a non-stake requester at a full hold budget is never
/// reserved out. This is the default-off invariant — single-lane
/// operators must be entirely unaffected (#757).
#[test]
fn no_reservation_never_reserves_out() {
    assert!(!stake_lane_reserved_out(0, 256, 256, || false));
    assert!(!stake_lane_reserved_out(0, 256, 0, || false));
}

/// A stake-lane (registered-operator) requester is never reserved out,
/// even with the budget fully consumed — the reservation exists to
/// protect exactly these node-to-node cache-miss probes.
#[test]
fn stake_lane_requester_never_reserved_out() {
    assert!(!stake_lane_reserved_out(8, 256, 256, || true));
    assert!(!stake_lane_reserved_out(8, 256, 255, || true));
}

/// An end-client probe is admitted while hold usage is below the
/// end-client ceiling (`max_holds - reserved`) and refused once usage
/// reaches it, reserving the last `reserved` slots for the stake lane.
#[test]
fn end_client_refused_at_reserved_ceiling() {
    // reserved=8, max=256 => end-client ceiling is 248.
    assert!(!stake_lane_reserved_out(8, 256, 247, || false));
    assert!(stake_lane_reserved_out(8, 256, 248, || false));
    assert!(stake_lane_reserved_out(8, 256, 256, || false));
}

/// Holds disabled (`max_holds == 0`) short-circuits to `false` so the
/// reservation gate never pre-empts the `HoldsDisabled` outcome — the
/// `saturating_sub` would otherwise yield a `0` ceiling and refuse
/// every end-client, mis-attributing an intentional disable.
#[test]
fn holds_disabled_is_not_a_reservation_refusal() {
    assert!(!stake_lane_reserved_out(8, 0, 0, || false));
}

/// Reserving the entire budget (`reserved >= max_holds`) yields a `0`
/// ceiling: every end-client probe is reserved out whenever holds are
/// enabled. An aggressive but valid "stake lane only" configuration.
#[test]
fn reserving_full_budget_excludes_all_end_clients() {
    assert!(stake_lane_reserved_out(256, 256, 0, || false));
    assert!(stake_lane_reserved_out(512, 256, 0, || false));
}

/// The `is_stake_lane` closure is consulted only after the cheap ceiling
/// guards pass, so the staker-set lookup is skipped on the uncongested
/// common path (#757 review). Each cheap guard failing must short-circuit
/// before the closure runs.
#[test]
fn is_stake_lane_lookup_is_skipped_below_ceiling() {
    let consulted = std::cell::Cell::new(false);
    let probe = || {
        consulted.set(true);
        false
    };
    // reserved == 0 (default-off), holds disabled, and usage below the
    // end-client ceiling each short-circuit before the lookup.
    assert!(!stake_lane_reserved_out(0, 256, 256, probe));
    assert!(!stake_lane_reserved_out(8, 0, 0, probe));
    assert!(!stake_lane_reserved_out(8, 256, 247, probe));
    assert!(!consulted.get(), "staker-set lookup ran below the ceiling");

    // At the ceiling the lookup is required and must run.
    assert!(stake_lane_reserved_out(8, 256, 248, probe));
    assert!(consulted.get(), "staker-set lookup skipped at the ceiling");
}
