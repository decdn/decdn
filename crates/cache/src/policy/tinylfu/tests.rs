use super::*;
fn h(i: u32) -> Hash {
    Hash::new(i.to_le_bytes())
}

#[test]
fn observe_and_estimate_round_trip() {
    let est = TinyLfuEstimator::new(1024);
    for _ in 0..3 {
        est.observe(h(42u32));
    }
    assert!(est.estimate(h(42u32)) >= 3);
    assert_eq!(est.estimate(h(43u32)), 0);
}

#[test]
fn tinylfu_evicts_least_frequent_first_not_least_recent() {
    let freq: std::sync::Arc<dyn FrequencyEstimator> =
        std::sync::Arc::new(TinyLfuEstimator::new(4096));
    let hot = h(1u32);
    let cold = h(2u32);
    for _ in 0..20 {
        freq.observe(hot);
    } // hot: high frequency, touched long ago
    freq.observe(cold); // cold: low frequency, touched just now
    let now = std::time::Instant::now();
    let mut map = std::collections::HashMap::new();
    map.insert(
        hot,
        now.checked_sub(std::time::Duration::from_secs(100))
            .unwrap_or(now),
    ); // LRU would evict hot
    map.insert(cold, now);
    let candidates = crate::EvictionCandidates::from_map_for_test(map);
    let sizes = std::collections::HashMap::new();
    let segments = std::collections::HashMap::new();
    // Empty sizes (all 0) so `freed` never grows: with total > target and a
    // wide budget the whole set is planned in least-frequent-first order.
    let plan = TinyLfuEviction::new(freq, 2, 10).plan(&EvictionContext {
        candidates: &candidates,
        sizes: &sizes,
        segments: &segments,
        total_bytes: 1,
        target_bytes: 0,
        budget: 100,
        cache_bytes: 0,
    });
    assert_eq!(
        plan.evict.first().copied(),
        Some(cold),
        "least-frequent must go first"
    );
    assert!(plan.promote.is_empty());
}

/// Blobs with equal frequency and one shared recency — the open-time seed —
/// release largest first.
#[test]
fn tinylfu_breaks_equal_frequency_and_recency_largest_first() {
    let freq: std::sync::Arc<dyn FrequencyEstimator> =
        std::sync::Arc::new(TinyLfuEstimator::new(4096));
    let seed = std::time::Instant::now();
    let mut map = std::collections::HashMap::new();
    let mut sizes = std::collections::HashMap::new();
    for (i, size) in [(1u32, 10u64), (2, 300), (3, 50)] {
        map.insert(h(i), seed);
        sizes.insert(h(i), size);
    }
    let candidates = crate::EvictionCandidates::from_map_for_test(map);
    let segments = std::collections::HashMap::new();
    let plan = TinyLfuEviction::new(freq, 2, 10).plan(&EvictionContext {
        candidates: &candidates,
        sizes: &sizes,
        segments: &segments,
        total_bytes: 360,
        target_bytes: 0,
        budget: 100,
        cache_bytes: 0,
    });
    assert_eq!(plan.evict, vec![h(2), h(3), h(1)]);
}

#[test]
fn probation_admission_first_sight_is_probation() {
    let freq: Arc<dyn FrequencyEstimator> = Arc::new(TinyLfuEstimator::new(4096));
    let pol = crate::policy::ProbationAdmission {
        freq: freq.clone(),
        promotion_threshold: 2,
    };
    let ctx = crate::policy::AdmissionContext {
        hash: h(1u32),
        known_size: None,
    };
    assert!(matches!(
        pol.admit(&ctx),
        crate::policy::AdmissionDecision::Store {
            segment: crate::policy::Segment::Probation
        }
    ));
    freq.observe(h(1u32));
    freq.observe(h(1u32));
    assert!(matches!(
        pol.admit(&ctx),
        crate::policy::AdmissionDecision::Store {
            segment: crate::policy::Segment::Main
        }
    ));
}

#[test]
fn plan_promotes_hot_probation_member() {
    let freq: Arc<dyn FrequencyEstimator> = Arc::new(TinyLfuEstimator::new(4096));
    let hot = h(1u32);
    let cold = h(2u32);
    freq.observe(hot);
    freq.observe(hot); // hot: estimate >= threshold(2)
    // cold: estimate 0 (< threshold)
    let now = std::time::Instant::now();
    let mut cmap = std::collections::HashMap::new();
    cmap.insert(hot, now);
    cmap.insert(cold, now);
    let candidates = crate::EvictionCandidates::from_map_for_test(cmap);
    let sizes = std::collections::HashMap::new();
    let mut segments = std::collections::HashMap::new();
    segments.insert(hot, super::super::Segment::Probation);
    segments.insert(cold, super::super::Segment::Probation);
    let plan = TinyLfuEviction::new(freq, 2, 100).plan(&EvictionContext {
        candidates: &candidates,
        sizes: &sizes,
        segments: &segments,
        total_bytes: 0,
        target_bytes: 0,
        budget: 100,
        cache_bytes: 1000,
    });
    assert!(plan.promote.contains(&(hot, super::super::Segment::Main)));
    assert!(!plan.promote.iter().any(|(h, _)| *h == cold));
}

#[test]
fn plan_caps_probation() {
    let freq: Arc<dyn FrequencyEstimator> = Arc::new(TinyLfuEstimator::new(4096));
    let a = h(1u32);
    let b = h(2u32);
    // both cold, no promotion. sizes push probation footprint over cap.
    let now = std::time::Instant::now();
    let mut cmap = std::collections::HashMap::new();
    cmap.insert(a, now);
    cmap.insert(b, now);
    let candidates = crate::EvictionCandidates::from_map_for_test(cmap);
    let mut sizes = std::collections::HashMap::new();
    sizes.insert(a, 60u64);
    sizes.insert(b, 60u64);
    let mut segments = std::collections::HashMap::new();
    segments.insert(a, super::super::Segment::Probation);
    segments.insert(b, super::super::Segment::Probation);
    // cache_bytes=1000, probation_target_pct=10 -> limit=100. footprint=120 > 100.
    // total_bytes under target_bytes so global loop wouldn't evict anything.
    let plan = TinyLfuEviction::new(freq, 2, 10).plan(&EvictionContext {
        candidates: &candidates,
        sizes: &sizes,
        segments: &segments,
        total_bytes: 10,
        target_bytes: 1000,
        budget: 100,
        cache_bytes: 1000,
    });
    assert!(!plan.evict.is_empty(), "cap must evict from probation");
    assert!(plan.evict.contains(&a) || plan.evict.contains(&b));
}
