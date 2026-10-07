use super::*;
use crate::load_shed::{LoadShedPolicy, PressureSnapshot, RequestClass, ShedDecision, ShedReason};

fn params() -> Params {
    Params {
        egress_budget_bps: 1_000,
        max_serves_high: 100,
        max_serves_low: 80,
        per_client_cap: 10,
    }
}
fn snap(n: u32, c: u32, bps: u64) -> PressureSnapshot {
    PressureSnapshot {
        streams_in_flight: n,
        client_streams_in_flight: c,
        egress_bps: bps,
    }
}

#[test]
fn admits_everything_below_high_water_and_under_budget() {
    let p = ResourcePressure::new(params());
    assert_eq!(
        p.decide(RequestClass::CacheMiss, &snap(50, 9, 500)),
        ShedDecision::Admit
    );
    assert_eq!(
        p.decide(RequestClass::CacheHit, &snap(50, 9, 500)),
        ShedDecision::Admit
    );
    assert!(!p.pressure_active());
}

#[test]
fn lone_client_is_never_capped_at_low_load() {
    // Fairness invariant: no speculative reservation. A single client with
    // many streams but below high-water is not capped.
    let p = ResourcePressure::new(params());
    assert_eq!(
        p.decide(RequestClass::CacheMiss, &snap(50, 50, 500)),
        ShedDecision::Admit
    );
}

#[test]
fn sheds_miss_first_above_high_water_but_keeps_hits() {
    let p = ResourcePressure::new(params());
    // Cross the high-water mark.
    assert_eq!(
        p.decide(RequestClass::CacheMiss, &snap(120, 1, 500)),
        ShedDecision::Shed(ShedReason::NodeAtCapacity)
    );
    // Hit still admitted while egress is under budget.
    assert_eq!(
        p.decide(RequestClass::CacheHit, &snap(120, 1, 500)),
        ShedDecision::Admit
    );
    assert!(p.pressure_active());
}

#[test]
fn egress_saturation_sheds_even_hits() {
    let p = ResourcePressure::new(params());
    assert_eq!(
        p.decide(RequestClass::CacheHit, &snap(50, 1, 1_000)),
        ShedDecision::Shed(ShedReason::EgressSaturated)
    );
}

#[test]
fn per_client_cap_engages_only_under_pressure() {
    let p = ResourcePressure::new(params());
    // Trip pressure, then a client over its share is shed even for a hit.
    assert_eq!(
        p.decide(RequestClass::CacheHit, &snap(120, 10, 500)),
        ShedDecision::Shed(ShedReason::ClientAtCapacity)
    );
}

#[test]
fn hysteresis_holds_pressure_between_the_marks() {
    let p = ResourcePressure::new(params());
    // Trip at high-water.
    let _ = p.decide(RequestClass::CacheMiss, &snap(100, 1, 500));
    assert!(p.pressure_active());
    // Between low and high: still pressured (no flap), so miss still sheds.
    assert_eq!(
        p.decide(RequestClass::CacheMiss, &snap(90, 1, 500)),
        ShedDecision::Shed(ShedReason::NodeAtCapacity)
    );
    // Drop to/below low-water: pressure clears, miss admits again.
    assert_eq!(
        p.decide(RequestClass::CacheMiss, &snap(80, 1, 500)),
        ShedDecision::Admit
    );
    assert!(!p.pressure_active());
}

#[test]
fn zero_budget_disables_egress_ceiling() {
    let mut pr = params();
    pr.egress_budget_bps = 0;
    let p = ResourcePressure::new(pr);
    assert_eq!(
        p.decide(RequestClass::CacheHit, &snap(50, 1, u64::MAX)),
        ShedDecision::Admit
    );
}

#[test]
fn zero_per_client_cap_disables_client_fairness() {
    let mut pr = params();
    pr.per_client_cap = 0;
    let p = ResourcePressure::new(pr);
    // Pressured, huge client count, but per-client disabled: a hit still admits.
    assert_eq!(
        p.decide(RequestClass::CacheHit, &snap(120, 9_999, 500)),
        ShedDecision::Admit
    );
}
