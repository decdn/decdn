use super::*;
use crate::dht::staker_set::ConfigStakerSet;
use std::collections::HashSet as StdHashSet;

fn nid(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}
fn h(byte: u8) -> Hash {
    Hash::from_bytes([byte; 32])
}
/// Wrap test nodes in `CloserNodes` (all test inputs are within cap).
fn closer(nodes: Vec<NodeId>) -> decdn_protocol::dht::CloserNodes {
    decdn_protocol::dht::CloserNodes::try_new(nodes)
        .expect("test closer_nodes within MAX_CLOSER_NODES")
}
fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).expect("test literal is non-zero")
}
/// A one-block-full `Coverage`, used by every test that doesn't care
/// about the specific coverage value.
fn cov() -> Coverage {
    Coverage::full(1)
}
/// A wire `Provider` for `node` with `cov()`.
fn provider(node: NodeId) -> decdn_protocol::dht::Provider {
    decdn_protocol::dht::Provider {
        node,
        coverage: cov(),
    }
}

#[test]
fn filter_xor_closer_drops_at_or_beyond_responder_distance() {
    // target = 0x00…00. Distance from peer X is just X itself.
    let target = h(0);
    let responder = nid(0x10);
    // 0x05/0x01 closer; 0x10 equal → drop; 0x20 further → drop.
    let input = vec![nid(0x05), nid(0x10), nid(0x20), nid(0x01)];
    let kept = filter_xor_closer(input, &target, &responder);
    let kept_set: StdHashSet<NodeId> = kept.into_iter().collect();
    assert_eq!(kept_set, StdHashSet::from([nid(0x05), nid(0x01)]));
}

#[test]
fn filter_active_stakers_drops_non_staked() {
    let mut staked = StdHashSet::new();
    staked.insert(nid(1));
    staked.insert(nid(3));
    let set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(staked));
    let input = vec![nid(1), nid(2), nid(3), nid(4)];
    let kept = filter_active_stakers(input, set.as_ref());
    assert_eq!(kept, vec![nid(1), nid(3)]);
}

#[test]
fn filter_negative_cache_drops_only_active_pairs_for_target() {
    let cache = NegativeProbeCache::new();
    let target = h(0xAA);
    cache.record_failure(nid(1), target);
    cache.record_failure(nid(2), h(0xBB)); // different target → no effect

    let input = vec![nid(1), nid(2), nid(3)];
    let kept = filter_negative_cache(input, &target, &cache);
    assert_eq!(kept, vec![nid(2), nid(3)]);
}

#[test]
fn lookup_state_pick_returns_closest_alpha_marks_queried() {
    let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
    let target = h(0);
    let cfg = LookupConfig {
        alpha: nz(2),
        k: nz(20),
        round_timeout: Duration::from_secs(1),
    };
    let mut state = LookupState::new(&routing, &target, nid(0xFF), cfg);
    state.add_candidate(nid(0x10));
    state.add_candidate(nid(0x02));
    state.add_candidate(nid(0x40));

    let first = state.pick_round_batch();
    assert_eq!(first, vec![nid(0x02), nid(0x10)]);
    let second = state.pick_round_batch();
    assert_eq!(second, vec![nid(0x40)]);
    let third = state.pick_round_batch();
    assert!(third.is_empty());
}

#[test]
fn lookup_state_add_candidate_tracks_strictly_closer() {
    let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
    let target = h(0);
    let cfg = LookupConfig::default();
    let mut state = LookupState::new(&routing, &target, nid(0xFF), cfg);

    // best_queried_distance starts at 0xFF…FF, so any peer wins.
    assert!(state.add_candidate(nid(0x80)));
    let _ = state.pick_round_batch();
    assert!(state.add_candidate(nid(0x40)));
    // 0xC0 is further than 0x80, so not strictly closer.
    assert!(!state.add_candidate(nid(0xC0)));
}

#[test]
fn lookup_state_record_provider_dedupes() {
    let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
    let cfg = LookupConfig::default();
    let mut state = LookupState::new(&routing, &h(0), nid(0xFF), cfg);
    state.record_provider(nid(1), cov());
    state.record_provider(nid(2), cov());
    state.record_provider(nid(1), cov());
    let providers_in_order: Vec<NodeId> = state.providers.keys().copied().collect();
    assert_eq!(providers_in_order, vec![nid(1), nid(2)]);
}

#[test]
fn into_randomised_providers_truncates_to_k_and_shuffles() {
    // With 20 providers truncated to 10, a real Fisher-Yates
    // yields ~16 distinct orderings across 16 runs (the sample
    // space is 20!/10! ≈ 6.7e11, collisions are negligible).
    // A 1-or-2-element-cycle shuffle yields ≤ 6 orderings; a
    // fully degenerate (identity) shuffle yields 1. Threshold
    // of 8 catches both classes while leaving margin for a
    // real-but-unlucky shuffle to pass.
    let canonical = (1u8..=20).map(nid).collect::<Vec<_>>();
    let mut seen_orderings: StdHashSet<Vec<NodeId>> = StdHashSet::new();
    for _ in 0..16 {
        let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
        let cfg = LookupConfig {
            alpha: nz(3),
            k: nz(10),
            round_timeout: Duration::from_secs(1),
        };
        let mut state = LookupState::new(&routing, &h(0), nid(0xFF), cfg);
        for &p in &canonical {
            state.record_provider(p, cov());
        }
        let out = state.into_randomised_providers();
        assert_eq!(out.len(), 10);
        assert!(
            out.iter().all(|(p, _)| canonical.contains(p)),
            "shuffle invented elements not in the canonical set"
        );
        let ids: Vec<NodeId> = out.into_iter().map(|(p, _)| p).collect();
        seen_orderings.insert(ids);
    }
    assert!(
        seen_orderings.len() >= 8,
        "providers produced fewer than 8 distinct orderings across 16 runs — \
         shuffle is degenerate or only permutes a tiny prefix"
    );
}

/// ADR 022 § Lookup integrity mandates that the negative-cache filter applies
/// only to `providers`, never `closer_nodes`. A peer that
/// previously denied holding `target` may still legitimately
/// route towards it, so dropping it from `closer_nodes` would
/// silently strand lookups whose only path passes through that
/// peer. Pin the asymmetry against a future "tidy-up" that
/// extends Filter-3 to both fields.
#[test]
fn fold_response_negative_cache_does_not_drop_closer_nodes() {
    let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
    let target = h(0);
    let cfg = LookupConfig::default();
    let mut state = LookupState::new(&routing, &target, nid(0xFF), cfg);

    let staked: StdHashSet<NodeId> = [nid(0x05), nid(0x10), nid(0x20)].into_iter().collect();
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(staked));

    let cache = NegativeProbeCache::new();
    // Both 0x05 and 0x10 are flagged negative for `target`.
    cache.record_failure(nid(0x05), target);
    cache.record_failure(nid(0x10), target);

    // Responder 0x20 returns 0x05 + 0x10 in BOTH fields. Per
    // ADR 022 § Lookup integrity only `providers` is filtered.
    let resp = decdn_protocol::dht::FindValueResponse {
        hash: target,
        providers: vec![provider(nid(0x05)), provider(nid(0x10))],
        closer_nodes: closer(vec![nid(0x05), nid(0x10)]),
    };
    fold_response(
        &target,
        staker_set.as_ref(),
        &cache,
        resp,
        nid(0x20),
        &mut state,
    );

    // Providers were dropped by Filter 3.
    assert_eq!(state.providers.len(), 0);
    // Closer_nodes survived — they appear as candidates.
    assert!(state.candidates.values().any(|v| v == &nid(0x05)));
    assert!(state.candidates.values().any(|v| v == &nid(0x10)));
}

/// A round can surface providers while no candidate is strictly
/// closer than the current best. `fold_response` must still fold
/// the providers into state, and the convergence signal it
/// returns is independent of provider presence.
#[test]
fn fold_response_records_providers_even_when_no_closer_node() {
    let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
    let target = h(0);
    let cfg = LookupConfig::default();
    let mut state = LookupState::new(&routing, &target, nid(0xFF), cfg);

    let staked: StdHashSet<NodeId> = [nid(0x05), nid(0x07)].into_iter().collect();
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(staked));
    let cache = NegativeProbeCache::new();

    // Responder at 0x05 returns provider 0x07 and a closer_node
    // 0x07 that is NOT strictly closer than the responder
    // (0x07 > 0x05 in XOR to target=0x00), so Filter 1 drops it.
    let resp = decdn_protocol::dht::FindValueResponse {
        hash: target,
        providers: vec![provider(nid(0x07))],
        closer_nodes: closer(vec![nid(0x07)]),
    };
    let observed_closer = fold_response(
        &target,
        staker_set.as_ref(),
        &cache,
        resp,
        nid(0x05),
        &mut state,
    );

    assert!(!observed_closer, "no candidate was strictly closer");
    assert_eq!(state.providers.len(), 1, "provider must still be folded");
    assert!(state.providers.contains_key(&nid(0x07)));
}

/// Drives F1 + F2 + F3 + `LookupState`'s self-filter on a single
/// response. The per-filter tests cover each in isolation; this
/// one defends against filter-reorder, short-circuit-on-empty,
/// or wrong-field-routing regressions that none of the unit
/// tests would individually catch.
#[test]
fn fold_response_all_filters_compose() {
    // target = 0x00, requester = 0x01 (so self appears strictly
    // closer than the responder 0x40 and would survive F1 if
    // the self-filter weren't running). Stakers include only
    // the would-be survivors plus requester / responder.
    let target = h(0x00);
    let requester = nid(0x01);
    let responder = nid(0x40);

    let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
    let cfg = LookupConfig::default();
    let mut state = LookupState::new(&routing, &target, requester, cfg);

    let staked: StdHashSet<NodeId> = [requester, responder, nid(0x10), nid(0x12)]
        .into_iter()
        .collect();
    let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(staked));

    // (nid(0x10), target) flagged negative — should drop from
    // providers but NOT from closer_nodes.
    let cache = NegativeProbeCache::new();
    cache.record_failure(nid(0x10), target);

    let resp = decdn_protocol::dht::FindValueResponse {
        hash: target,
        // providers: [self, non-staked, neg-cached, survivor]
        providers: vec![
            provider(requester),
            provider(nid(0x05)),
            provider(nid(0x10)),
            provider(nid(0x12)),
        ],
        // closer_nodes: [self, non-staked, neg-cached (kept!),
        //                not-strictly-closer (0x80 > 0x40),
        //                survivor]
        closer_nodes: closer(vec![requester, nid(0x05), nid(0x10), nid(0x80), nid(0x12)]),
    };
    fold_response(
        &target,
        staker_set.as_ref(),
        &cache,
        resp,
        responder,
        &mut state,
    );

    // Providers: F2 drops 0x05, F3 drops 0x10, self-filter
    // drops requester → only 0x12 survives.
    let providers: Vec<NodeId> = state.providers.keys().copied().collect();
    assert_eq!(providers, vec![nid(0x12)]);

    // Closer_nodes: F1 drops 0x80, F2 drops 0x05, self-filter
    // drops requester → 0x10 (negative-cached but allowed here)
    // and 0x12 survive.
    let mut candidates: Vec<NodeId> = state.candidates.values().copied().collect();
    candidates.sort_unstable();
    assert_eq!(candidates, vec![nid(0x10), nid(0x12)]);
}

/// `have_enough_providers` is `>= k`, not `> k`. Pin the
/// boundary so a regression at the comparison ships red.
#[test]
fn have_enough_providers_is_inclusive_at_k() {
    let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
    let cfg = LookupConfig {
        alpha: nz(3),
        k: nz(2),
        round_timeout: Duration::from_secs(1),
    };
    let mut state = LookupState::new(&routing, &h(0), nid(0xFF), cfg);
    assert!(!state.have_enough_providers());
    state.record_provider(nid(1), cov());
    assert!(!state.have_enough_providers());
    state.record_provider(nid(2), cov());
    assert!(state.have_enough_providers());
    state.record_provider(nid(3), cov());
    assert!(state.have_enough_providers());
}

/// `add_candidate` and `record_provider` reject `requester_id`
/// even if the wire layer somehow let it through. The self-
/// filter lives on `LookupState` itself (not in
/// `process_response`) precisely so this can't regress.
#[test]
fn lookup_state_rejects_self_in_candidates_and_providers() {
    let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
    let cfg = LookupConfig::default();
    let me = nid(0xAA);
    let mut state = LookupState::new(&routing, &h(0), me, cfg);

    // Both methods silently no-op for self.
    assert!(!state.add_candidate(me));
    assert!(state.candidates.is_empty());
    state.record_provider(me, cov());
    assert!(state.providers.is_empty());

    // Non-self still flows through.
    assert!(state.add_candidate(nid(0x42)));
    state.record_provider(nid(0x43), cov());
    assert_eq!(state.candidates.len(), 1);
    assert_eq!(state.providers.len(), 1);
}
