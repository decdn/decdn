use super::*;
use crate::policy::{
    AdmissionContext, AdmissionDecision, AdmissionPolicy, EvictionContext, EvictionPolicy, Segment,
};
use crate::{EvictionCandidates, Hash};
use std::collections::HashMap;
use std::time::{Duration, Instant};

fn h(b: u8) -> Hash {
    Hash::from([b; 32])
}

fn ctx<'a>(
    candidates: &'a EvictionCandidates,
    sizes: &'a HashMap<Hash, u64>,
    segments: &'a HashMap<Hash, Segment>,
    total_bytes: u64,
    target_bytes: u64,
    budget: u64,
) -> EvictionContext<'a> {
    EvictionContext {
        candidates,
        sizes,
        segments,
        total_bytes,
        target_bytes,
        budget,
        cache_bytes: 0,
    }
}

#[test]
fn lru_orders_oldest_access_first() {
    let now = Instant::now();
    let mut map = HashMap::new();
    map.insert(h(1), now); // newest
    map.insert(h(2), now.checked_sub(Duration::from_mins(1)).unwrap_or(now)); // oldest
    map.insert(
        h(3),
        now.checked_sub(Duration::from_secs(30)).unwrap_or(now),
    );
    let candidates = EvictionCandidates::from_map_for_test(map);
    let sizes = HashMap::new();
    let segments = HashMap::new();
    // Sizes empty (all 0) so `freed` never grows: with total > target and a
    // budget above the candidate count, the whole set is planned oldest-first.
    let plan = LruEviction.plan(&ctx(&candidates, &sizes, &segments, 1, 0, 100));
    assert_eq!(plan.evict, vec![h(2), h(3), h(1)]);
    assert!(plan.promote.is_empty());
}

/// Blobs sharing one recency — the open-time seed — release largest first,
/// so the sweep reaches its target in fewer releases.
#[test]
fn lru_breaks_equal_recency_largest_first() {
    let seed = Instant::now();
    let mut map = HashMap::new();
    map.insert(h(1), seed);
    map.insert(h(2), seed);
    map.insert(h(3), seed);
    let candidates = EvictionCandidates::from_map_for_test(map);
    let mut sizes = HashMap::new();
    sizes.insert(h(1), 10u64);
    sizes.insert(h(2), 300u64);
    sizes.insert(h(3), 50u64);
    let segments = HashMap::new();
    let plan = LruEviction.plan(&ctx(&candidates, &sizes, &segments, 360, 0, 100));
    assert_eq!(plan.evict, vec![h(2), h(3), h(1)]);
}

#[test]
fn lru_plan_matches_recency_order() {
    let now = Instant::now();
    let mut map = HashMap::new();
    map.insert(
        h(1),
        now.checked_sub(Duration::from_secs(10)).unwrap_or(now),
    );
    map.insert(
        h(2),
        now.checked_sub(Duration::from_secs(30)).unwrap_or(now),
    ); // oldest
    map.insert(h(3), now); // newest
    let candidates = EvictionCandidates::from_map_for_test(map);
    let mut sizes = HashMap::new();
    sizes.insert(h(1), 100u64);
    sizes.insert(h(2), 100u64);
    sizes.insert(h(3), 100u64);
    let segments = HashMap::new();

    // total 300, target 150 → free 150 bytes worth: evict oldest two, stop.
    let plan = LruEviction.plan(&ctx(&candidates, &sizes, &segments, 300, 150, 100));
    assert_eq!(plan.evict, vec![h(2), h(1)], "oldest-first toward target");
    assert!(plan.promote.is_empty());

    // Budget caps the sweep at one release regardless of remaining overage.
    let capped = LruEviction.plan(&ctx(&candidates, &sizes, &segments, 300, 0, 1));
    assert_eq!(capped.evict, vec![h(2)], "budget stops after one");
}

#[test]
#[allow(clippy::panic)]
fn always_admit_stores_to_main() {
    let ctx = AdmissionContext {
        hash: h(1),
        known_size: None,
    };
    match AlwaysAdmit.admit(&ctx) {
        AdmissionDecision::Store {
            segment: Segment::Main,
        } => {}
        other => panic!("expected Store/Main, got {other:?}"),
    }
}
