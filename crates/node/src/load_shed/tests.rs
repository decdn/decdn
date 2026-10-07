use super::*;

fn snap(n: u32, c: u32, bps: u64) -> PressureSnapshot {
    PressureSnapshot {
        streams_in_flight: n,
        client_streams_in_flight: c,
        egress_bps: bps,
    }
}

#[test]
fn always_admit_admits_every_class_under_any_pressure() {
    let p = AlwaysAdmit;
    assert_eq!(
        p.decide(RequestClass::CacheHit, &snap(10_000, 9_999, u64::MAX)),
        ShedDecision::Admit
    );
    assert_eq!(
        p.decide(RequestClass::CacheMiss, &snap(10_000, 9_999, u64::MAX)),
        ShedDecision::Admit
    );
    assert!(!p.pressure_active());
}
