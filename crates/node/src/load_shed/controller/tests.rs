use super::*;
use crate::load_shed::RequestClass;
use alloy::primitives::B256;
use decdn_common::config::{LoadShedPolicyKind, ResolvedLoadShed};

fn cfg(kind: LoadShedPolicyKind) -> ResolvedLoadShed {
    ResolvedLoadShed {
        policy: kind,
        egress_budget_mbps: 0,
        max_concurrent_serves_high: 2,
        max_concurrent_serves_low: 1,
        per_client_serve_cap: 0,
    }
}
fn client(n: u8) -> B256 {
    B256::from([n; 32])
}

#[test]
fn resource_pressure_sheds_miss_when_node_full() {
    let c = LoadShedController::from_config(&cfg(LoadShedPolicyKind::ResourcePressure));
    assert_eq!(c.node_in_flight(), 0);
    // Hold two slots to reach the high-water mark of 2.
    let _s1 = c
        .try_admit(RequestClass::CacheHit, client(1))
        .expect("first admit");
    let _s2 = c
        .try_admit(RequestClass::CacheHit, client(1))
        .expect("second admit");
    // The gauge accessor reflects the two held slots.
    assert_eq!(c.node_in_flight(), 2);
    // A new miss is shed; a new hit is still admitted.
    assert_eq!(
        c.try_admit(RequestClass::CacheMiss, client(2)).err(),
        Some(ShedReason::NodeAtCapacity)
    );
    assert!(c.try_admit(RequestClass::CacheHit, client(2)).is_ok());
    assert!(c.pressure_active());
}

#[test]
fn always_admit_never_sheds() {
    let c = LoadShedController::from_config(&cfg(LoadShedPolicyKind::AlwaysAdmit));
    let mut held = Vec::new();
    for _ in 0..50 {
        held.push(
            c.try_admit(RequestClass::CacheMiss, client(1))
                .expect("always admit"),
        );
    }
    assert!(!c.pressure_active());
}

#[test]
fn reload_swaps_policy_live() {
    let c = LoadShedController::from_config(&cfg(LoadShedPolicyKind::ResourcePressure));
    c.reload(&cfg(LoadShedPolicyKind::AlwaysAdmit));
    let _a = c
        .try_admit(RequestClass::CacheMiss, client(1))
        .expect("admit under swapped policy");
    let _b = c
        .try_admit(RequestClass::CacheMiss, client(1))
        .expect("admit 2");
    let _d = c
        .try_admit(RequestClass::CacheMiss, client(1))
        .expect("admit 3 — always-admit ignores caps");
}

#[test]
fn egress_meter_feeds_snapshot() {
    let c = LoadShedController::from_config(&ResolvedLoadShed {
        policy: LoadShedPolicyKind::ResourcePressure,
        egress_budget_mbps: 1, // 1 Mbps = 125_000 B/s ceiling
        max_concurrent_serves_high: 1_000,
        max_concurrent_serves_low: 900,
        per_client_serve_cap: 0,
    });
    c.record_egress(200_000);
    c.sample_egress(1); // instant 200_000 B/s > 125_000 ceiling
    assert_eq!(
        c.try_admit(RequestClass::CacheHit, client(1)).err(),
        Some(ShedReason::EgressSaturated)
    );
}
