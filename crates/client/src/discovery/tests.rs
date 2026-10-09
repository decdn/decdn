use super::*;
use alloy::primitives::{Bytes, FixedBytes};
use std::assert_matches;

fn node_info(node_id: [u8; 32], active: bool) -> CapacityBond::NodeInfo {
    CapacityBond::NodeInfo {
        nodeId: FixedBytes::from(node_id),
        ethAddress: Address::repeat_byte(0xab),
        active,
        lastMultiaddrUpdate: 0,
        multiaddrs: Bytes::new(),
        regionHint: "US".to_string(),
    }
}

#[test]
fn skips_inactive_nodes() {
    // A valid ed25519 key: the all-zero point is rejected by `from_bytes`,
    // so derive a real one from a known secret.
    let key = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
    let bytes = *key.as_bytes();
    assert!(candidate_from(&node_info(bytes, true), true).is_some());
    assert!(
        candidate_from(&node_info(bytes, true), false).is_none(),
        "inactive nodes must be filtered out"
    );
}

#[test]
fn candidate_carries_registry_multiaddrs_for_dialing() {
    let key = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
    let mut info = node_info(*key.as_bytes(), true);
    info.multiaddrs = Bytes::from(
        decdn_incentive::node_register::pack_multiaddrs(&[
            "/ip4/203.0.113.10/udp/4433/quic-v1".to_string()
        ])
        .unwrap(),
    );
    let cand = candidate_from(&info, true).expect("active candidate");
    assert_eq!(
        cand.dial_addrs(),
        vec!["203.0.113.10:4433".parse().unwrap()]
    );
}

#[test]
fn with_dial_addrs_attaches_registry_address_to_target() {
    let key = iroh::SecretKey::from_bytes(&[3u8; 32]).public();
    let mut info = node_info(*key.as_bytes(), true);
    info.multiaddrs = Bytes::from(
        decdn_incentive::node_register::pack_multiaddrs(&[
            "/ip4/198.51.100.7/udp/5000/quic-v1".to_string()
        ])
        .unwrap(),
    );
    let cand = candidate_from(&info, true).expect("active candidate");
    let target = with_dial_addrs(iroh::EndpointAddr::new(cand.node_id), &cand);
    let want: std::net::SocketAddr = "198.51.100.7:5000".parse().unwrap();
    assert!(
        target.ip_addrs().any(|a| *a == want),
        "direct addr attached"
    );
}

#[test]
fn with_dial_addrs_is_noop_without_registry_addresses() {
    // Empty `multiaddrs` (or store-projected candidate): no direct hint is
    // added, so the dial falls back to discovery exactly as before.
    let cand = candidate(1, "US");
    let target = with_dial_addrs(iroh::EndpointAddr::new(cand.node_id), &cand);
    assert_eq!(target.ip_addrs().count(), 0);
}

/// The filter keys on the on-chain `isActive` (the `active[]` column of
/// `getRegisteredNodes`), NOT the raw `NodeInfo.active` registration flag. A
/// node mid-unbonding is still `NodeInfo.active == true` but `isActive ==
/// false`, and the client must drop it up front rather than defer to probing.
#[test]
fn excludes_stale_active_node_when_isactive_false() {
    let key = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
    let info = node_info(*key.as_bytes(), true); // raw registration flag true
    assert!(
        candidate_from(&info, true).is_some(),
        "isActive true → kept"
    );
    assert!(
        candidate_from(&info, false).is_none(),
        "stale-active (isActive false) dropped despite NodeInfo.active == true"
    );
}

#[test]
fn carries_eth_address_and_region() {
    let key = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    let c = candidate_from(&node_info(*key.as_bytes(), true), true).unwrap();
    assert_eq!(c.eth_address, Address::repeat_byte(0xab));
    assert_eq!(c.region_hint, Region::parse("US"));
}

/// An unrecognized on-chain hint must cost the node its locality bonus, not
/// its place in the candidate set — `CapacityBond` accepts any string up to
/// 16 bytes and ADR 030 declines to tighten it (#1348).
#[test]
fn an_unparseable_region_keeps_the_candidate_with_no_region() {
    let key = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    let mut info = node_info(*key.as_bytes(), true);
    info.regionHint = "not-a-region".to_string();
    let c = candidate_from(&info, true).unwrap();
    assert_eq!(c.region_hint, None, "unparseable, not rejected");
    assert_eq!(c.eth_address, Address::repeat_byte(0xab));
}

/// `region` is parsed, not stored raw, so a fixture written with stray case
/// or whitespace produces the SAME candidate as its canonical spelling.
/// That is the property the derived `Eq` depends on.
fn candidate(seed: u8, region: &str) -> NodeCandidate {
    NodeCandidate {
        node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
        eth_address: Address::repeat_byte(seed),
        region_hint: Region::parse(region),
        multiaddrs: Bytes::new(),
    }
}

/// Runs of the shuffle-then-sort used by the selection tests below. The
/// distribution checks want enough draws that a real shuffle shows variety
/// while a degenerate one (identity, or a tiny-prefix permutation) cannot.
/// The weakest check is the "≥ 3 distinct tails of the 20 possible" one in
/// `select_keeps_region_first_while_sampling_the_rest`, which a correct
/// shuffle fails with probability ≈ 2e-14; the membership-set checks are
/// far tighter.
const SHUFFLE_RUNS: usize = 16;

#[test]
fn select_puts_same_region_first_and_caps_at_k() {
    // Regions are on-chain self-attested ISO 3166-1 alpha-2 codes (ADR 030);
    // seed 4 carries stray case + whitespace, which `Region::parse`
    // normalizes at the boundary so the comparison here is plain equality.
    let cands = vec![
        candidate(1, "DE"),
        candidate(2, "US"),
        candidate(3, "DE"),
        candidate(4, " us "),
    ];
    let us: HashSet<Address> = [2u8, 4].into_iter().map(Address::repeat_byte).collect();
    let de: HashSet<Address> = [1u8, 3].into_iter().map(Address::repeat_byte).collect();
    // Which DE candidate takes the one non-US slot is random; with only
    // two of them a distribution check here would be too weak to keep
    // (see `select_keeps_region_first_while_sampling_the_rest`), so this
    // test holds the invariants only.
    for _ in 0..SHUFFLE_RUNS {
        let out = select_candidates(cands.clone(), Some("US"), 3);
        assert_eq!(out.len(), 3, "capped at k");
        // Both US entries come first, in either order.
        let first_two: HashSet<Address> = out[..2].iter().map(|c| c.eth_address).collect();
        assert_eq!(
            first_two, us,
            "same-region candidates fill the leading slots"
        );
        assert!(
            de.contains(&out[2].eth_address),
            "the last slot is a DE candidate"
        );
    }
}

/// The production shape for a client in a well-populated region: more
/// same-region candidates than `k`. Every pick is same-region, and WHICH
/// same-region candidates are picked varies — the sample inside the
/// matching group is random, not its leading `k` in input order.
#[test]
fn select_samples_within_an_oversized_region_group() {
    let mut cands: Vec<NodeCandidate> = (1u8..=20).map(|s| candidate(s, "US")).collect();
    cands.extend((21u8..=23).map(|s| candidate(s, "DE")));
    let us: HashSet<Address> = (1u8..=20).map(Address::repeat_byte).collect();
    let mut seen_sets: HashSet<Vec<Address>> = HashSet::new();
    for _ in 0..SHUFFLE_RUNS {
        let out = select_candidates(cands.clone(), Some("US"), 10);
        assert_eq!(out.len(), 10, "capped at k");
        let mut ids: Vec<Address> = out.iter().map(|c| c.eth_address).collect();
        assert!(
            ids.iter().all(|a| us.contains(a)),
            "a non-US candidate was picked while US candidates remained"
        );
        ids.sort_unstable();
        seen_sets.insert(ids);
    }
    // C(20, 10) = 184,756 possible sets; a real shuffle across 16 runs
    // shows ~16, a leading-k prefix shows 1.
    assert!(
        seen_sets.len() >= 8,
        "the same-region sample produced only {} distinct sets across {SHUFFLE_RUNS} runs",
        seen_sets.len()
    );
}

/// Region-first survives the shuffle when the same-region group is smaller
/// than `k`: every same-region candidate is always selected and always
/// leads, and the remaining slots are a random draw from the rest.
#[test]
fn select_keeps_region_first_while_sampling_the_rest() {
    let mut cands: Vec<NodeCandidate> = (1u8..=3).map(|s| candidate(s, "US")).collect();
    cands.extend((4u8..=8).map(|s| candidate(s, "DE")));
    let us: HashSet<Address> = (1u8..=3).map(Address::repeat_byte).collect();
    let de: HashSet<Address> = (4u8..=8).map(Address::repeat_byte).collect();
    let mut de_pairs_seen: HashSet<Vec<Address>> = HashSet::new();
    for _ in 0..SHUFFLE_RUNS {
        let out = select_candidates(cands.clone(), Some("US"), 5);
        assert_eq!(out.len(), 5, "capped at k");
        let lead: HashSet<Address> = out[..3].iter().map(|c| c.eth_address).collect();
        assert_eq!(lead, us, "all same-region candidates lead");
        let tail: Vec<Address> = out[3..].iter().map(|c| c.eth_address).collect();
        assert!(
            tail.iter().all(|a| de.contains(a)),
            "the tail is drawn from the rest"
        );
        de_pairs_seen.insert(tail);
    }
    // 5 DE candidates, 2 slots, ordered: 20 possible tails. A real shuffle
    // across 16 runs shows well over 2; a degenerate one shows 1.
    assert!(
        de_pairs_seen.len() >= 3,
        "the non-region tail produced only {} distinct draws across {SHUFFLE_RUNS} runs",
        de_pairs_seen.len()
    );
}

/// The normalization lives in the type, so ` us ` and `US` are the same
/// VALUE, not merely two things a comparison happens to fold together. This
/// is what makes the derived `Eq` on `NodeCandidate` correct: without it the
/// same node under two spellings would compare unequal, so any
/// `dedup`/`contains`/`retain` over candidates would silently fail to
/// dedupe.
#[test]
fn candidates_differing_only_in_region_spelling_are_equal() {
    assert_eq!(candidate(1, " us "), candidate(1, "US"));
    assert_ne!(candidate(1, "US"), candidate(1, "DE"));
    assert_ne!(
        candidate(1, "US"),
        candidate(1, "nonsense"),
        "an unparseable hint is None, which is not the same as any region"
    );
}

/// Without a usable client region there is no region preference, so the
/// result is a uniform random sample of `k` — never the first `k` of the
/// input, which is registry array order or filesystem order.
#[test]
fn select_without_region_is_a_random_sample_capped_at_k() {
    // 10 US + 10 DE candidates, cut to the production `SELECT_K` (5).
    //
    // - Orderings: 20!/15! ≈ 1.9e6 possible; a real shuffle shows ~16
    //   across 16 runs, a shuffle of a 2- or 3-element prefix ≤ 6, an
    //   identity shuffle 1. Mirrors `decdn-node`'s `dht::lookup` check
    //   (`into_randomised_providers_truncates_to_k_and_shuffles`).
    // - Membership sets: orderings alone cannot tell "shuffle then
    //   truncate" from "truncate then shuffle", which permutes one fixed
    //   prefix. C(20, 5) = 15,504 possible sets; a fixed prefix shows 1.
    // - Every candidate is left out at least once: a shuffle that never
    //   moves the first entry (the slot registry order hands out) keeps
    //   it on every run. A real shuffle keeps a given candidate on all
    //   16 runs with p = 0.25^16 ≈ 2.3e-10, ≈ 5e-9 across all 20.
    // - Some sample mixes both regions: a fallback region preference
    //   would fill all 5 slots from one region every run. A real shuffle
    //   draws a single-region set with p ≈ 0.033, so 16 in a row ≈ 1.6e-24.
    let mut cands: Vec<NodeCandidate> = (1u8..=10).map(|s| candidate(s, "US")).collect();
    cands.extend((11u8..=20).map(|s| candidate(s, "DE")));
    let canonical: HashSet<Address> = cands.iter().map(|c| c.eth_address).collect();
    let us: HashSet<Address> = (1u8..=10).map(Address::repeat_byte).collect();
    // No region, a blank one, and an unrecognized code all skip the region
    // sort rather than sorting against a value that means nothing.
    for region in [None, Some("  "), Some("not-a-region")] {
        let mut seen_orderings: HashSet<Vec<Address>> = HashSet::new();
        let mut seen_sets: HashSet<Vec<Address>> = HashSet::new();
        let mut left_out: HashSet<Address> = HashSet::new();
        let mut mixed_regions = false;
        for _ in 0..SHUFFLE_RUNS {
            let out = select_candidates(cands.clone(), region, SELECT_K);
            assert_eq!(out.len(), SELECT_K, "capped at k");
            let ids: Vec<Address> = out.iter().map(|c| c.eth_address).collect();
            assert!(
                ids.iter().all(|a| canonical.contains(a)),
                "selection invented candidates not in the input"
            );
            left_out.extend(canonical.iter().filter(|&a| !ids.contains(a)));
            let us_picked = ids.iter().filter(|&a| us.contains(a)).count();
            mixed_regions |= us_picked != 0 && us_picked != SELECT_K;
            let mut set = ids.clone();
            set.sort_unstable();
            seen_orderings.insert(ids);
            seen_sets.insert(set);
        }
        assert!(
            seen_orderings.len() >= 8,
            "region {region:?}: fewer than 8 distinct orderings across \
             {SHUFFLE_RUNS} runs — shuffle is degenerate or only permutes a \
             tiny prefix"
        );
        assert!(
            seen_sets.len() >= 8,
            "region {region:?}: fewer than 8 distinct membership sets across \
             {SHUFFLE_RUNS} runs — the truncate ran before the shuffle"
        );
        assert_eq!(
            left_out, canonical,
            "region {region:?}: a candidate was selected on every run — the \
             shuffle leaves an input position in place"
        );
        assert!(
            mixed_regions,
            "region {region:?}: every sample came from one region — a region \
             preference applied without a usable client region"
        );
    }
}

/// A candidate whose hint did not parse sorts into the "rest" bucket — it
/// must never be treated as matching the client's region. With two
/// candidates the region sort fully determines the order, so this holds
/// exactly on every run regardless of the shuffle.
#[test]
fn select_never_promotes_an_unparseable_region() {
    let cands = vec![candidate(1, "nonsense"), candidate(2, "US")];
    for _ in 0..SHUFFLE_RUNS {
        let out = select_candidates(cands.clone(), Some("US"), 5);
        assert_eq!(out[0].eth_address, Address::repeat_byte(2));
        assert_eq!(out[1].eth_address, Address::repeat_byte(1));
    }
}

/// With allowlist `["US"]`, a `Some(DE)` candidate is dropped, a
/// `Some(US)` candidate is kept, and a `None`-region candidate is kept
/// (absent-region-include, per the brief).
#[test]
fn region_allowlist_filters_present_regions_only() {
    let us = candidate(1, "US");
    let de = candidate(2, "DE");
    let none = candidate(3, "nonsense");
    let allow = [Region::parse("US").expect("valid")];
    let out =
        select_candidates_filtered(vec![us.clone(), de.clone(), none.clone()], None, 10, &allow);
    let addrs: Vec<_> = out.iter().map(|c| c.eth_address).collect();
    assert!(addrs.contains(&us.eth_address));
    assert!(addrs.contains(&none.eth_address));
    assert!(!addrs.contains(&de.eth_address));
}

/// An empty allowlist is a no-op: every candidate survives, regardless of
/// region.
#[test]
fn empty_region_allowlist_keeps_everything() {
    let cands = vec![
        candidate(1, "US"),
        candidate(2, "DE"),
        candidate(3, "nonsense"),
    ];
    let out = select_candidates_filtered(cands.clone(), None, 10, &[]);
    assert_eq!(out.len(), cands.len());
}

/// An iroh node id derived from a small seed, for [`cand`] fixtures — mirrors
/// [`candidate`]'s own key derivation but named to match the brief's helper
/// naming (`pk`/`addr`/`cand`) for the `admit_sources` tests below.
fn pk(seed: u8) -> PublicKey {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

/// A distinct Ethereum address per seed, for [`cand`] fixtures — the
/// "operator" identity `admit_sources` spreads across.
fn addr(seed: u8) -> Address {
    Address::repeat_byte(seed)
}

/// Build a [`NodeCandidate`] from an already-derived node id and address,
/// plus a region code (parsed the same way [`candidate`] does). Distinct
/// from `candidate(seed, region)` above because `admit_sources` tests need
/// the node id and operator address to vary independently (several nodes
/// under the same operator).
fn cand(node_id: PublicKey, eth_address: Address, region: Option<&str>) -> NodeCandidate {
    NodeCandidate {
        node_id,
        eth_address,
        region_hint: region.and_then(Region::parse),
        multiaddrs: Bytes::new(),
    }
}

#[test]
fn admit_sources_admits_one_node_per_operator_in_rank_order() {
    // Ranked: [op1/us, op1/us, op2/eu, op3/us]. max=3 → one node per
    // operator, in rank order: the FIRST op1 node, then op2, then op3 —
    // never the second op1 node, which would share op1's voucher lane.
    let ranked = vec![
        cand(pk(1), addr(1), Some("US")),
        cand(pk(2), addr(1), Some("US")),
        cand(pk(3), addr(2), Some("EU")),
        cand(pk(4), addr(3), Some("US")),
    ];
    let out = admit_sources(ranked, 3);
    // Identity, not just count: reversing the skip would still yield three
    // distinct operators, but from the wrong (lower-ranked) nodes.
    assert_eq!(
        out.iter().map(|c| c.node_id).collect::<Vec<_>>(),
        vec![pk(1), pk(3), pk(4)]
    );
}

/// The set SHRINKS rather than repeating an operator. Two nodes of one
/// operator would become two payment lanes on one `(signer, provider)`
/// watermark — concurrent voucher streams that regress each other, and two
/// watermark writes colliding under one `LaneKey`.
#[test]
fn admit_sources_never_repeats_an_operator() {
    let ranked = vec![
        cand(pk(1), addr(1), Some("US")),
        cand(pk(2), addr(1), Some("US")),
        cand(pk(3), addr(1), Some("EU")),
    ];
    let out = admit_sources(ranked, 4);
    assert_eq!(out.len(), 1, "one operator admits one source, not three");
    assert_eq!(
        out[0].node_id,
        pk(1),
        "the rank-first node of that operator"
    );
}

/// A spare slot left by the operator-distinctness rule is NOT filled with a
/// repeat operator: with two operators behind four nodes and `max = 4`, the
/// admitted set is two, not four.
#[test]
fn admit_sources_leaves_slots_empty_rather_than_repeating() {
    let ranked = vec![
        cand(pk(1), addr(1), None),
        cand(pk(2), addr(2), None),
        cand(pk(3), addr(1), None),
        cand(pk(4), addr(2), None),
    ];
    let out = admit_sources(ranked, 4);
    assert_eq!(
        out.iter().map(|c| c.eth_address).collect::<Vec<_>>(),
        vec![addr(1), addr(2)]
    );
}

/// `max_sources` larger than the candidate count returns everything, not a
/// padded or truncated set.
#[test]
fn admit_sources_max_larger_than_candidates_returns_all() {
    let ranked = vec![
        cand(pk(1), addr(1), Some("US")),
        cand(pk(2), addr(2), Some("EU")),
    ];
    let out = admit_sources(ranked.clone(), 10);
    assert_eq!(out.len(), 2);
    assert_eq!(out, ranked, "rank order preserved when nothing is dropped");
}

/// `max_sources == 0` is a valid, non-panicking request for nothing.
#[test]
fn admit_sources_zero_max_returns_empty() {
    let ranked = vec![cand(pk(1), addr(1), Some("US"))];
    assert_eq!(admit_sources(ranked, 0), Vec::new());
}

/// A [`Probed`] holder carries the coverage its probe reported, and a
/// **partial** holder (a proper subset of the blob's blocks) is admitted
/// exactly like a full one: `admit_sources` operates on `NodeCandidate`
/// rank order and operator identity only, so it has no way to see —
/// and must not need to see — that one holder's coverage is a strict
/// subset of another's. Two operators here each answered for a disjoint
/// half of the blob; both are admitted, and each one's `Probed` still
/// carries its own half, not the other's or the full blob's.
#[test]
fn partial_holders_carry_their_own_coverage_and_are_admitted_like_full_holders() {
    let op1 = cand(pk(1), addr(1), Some("US"));
    let op2 = cand(pk(2), addr(2), Some("US"));

    // op1 holds only block 0; op2 holds only block 1 — both partial, of a
    // 2-block blob, and disjoint.
    let probed = [
        Probed {
            candidate: op1.clone(),
            rtt_ms: 10.0,
            total_bytes: Some(128 * 1024 * 1024),
            coverage: Coverage::from_block_indices(2, [0].into_iter()),
        },
        Probed {
            candidate: op2.clone(),
            rtt_ms: 12.0,
            total_bytes: Some(128 * 1024 * 1024),
            coverage: Coverage::from_block_indices(2, [1].into_iter()),
        },
    ];

    // Operator-dedup, unchanged: both are distinct operators, so both are
    // admitted — coverage never enters the admission decision.
    let admitted = admit_sources(vec![op1.clone(), op2.clone()], 2);
    assert_eq!(
        admitted.iter().map(|c| c.node_id).collect::<Vec<_>>(),
        vec![pk(1), pk(2)],
        "a partial holder is admitted exactly like a full holder"
    );

    // Each admitted candidate's own probed coverage is still its own —
    // never full, never the other holder's block.
    let cov = |node_id: PublicKey| {
        probed
            .iter()
            .find(|p| p.candidate.node_id == node_id)
            .map(|p| p.coverage.clone())
    };
    let cov1 = cov(pk(1)).expect("op1 was probed");
    let cov2 = cov(pk(2)).expect("op2 was probed");
    assert!(cov1.covers(0) && !cov1.covers(1), "op1 holds only block 0");
    assert!(cov2.covers(1) && !cov2.covers(0), "op2 holds only block 1");
    assert_ne!(cov1, Coverage::full(2), "op1 is a partial holder, not full");
}

#[test]
fn retry_backoff_is_the_adr_012_schedule() {
    // ADR 012 § Bootstrap step 3: "retry 3× exponential backoff
    // (1 s, 5 s, 30 s)". A tripwire on the table, not coverage of the loop
    // that reads it — that is `a_failing_page_is_retried_on_the_adr_schedule`
    // below, which binds the two together.
    assert_eq!(
        REGISTRY_RETRY_BACKOFF,
        [
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::from_secs(30),
        ]
    );
}

/// A retryable transport failure — a refused connection, a reset, a timeout.
fn transient() -> alloy::contract::Error {
    alloy::contract::Error::TransportError(alloy::transports::RpcError::NullResp)
}

/// A failure that will never succeed on retry.
fn permanent() -> alloy::contract::Error {
    alloy::contract::Error::ContractNotDeployed
}

/// A JSON-RPC error *response* — HTTP 200 with an error body, so the HTTP
/// status check never sees it.
///
/// Built by deserializing the wire shape: `ErrorPayload` is not re-exported
/// through `alloy::transports` (only `RpcError` is), so the variant's own
/// type inference is what names it here.
fn error_resp(code: i64, message: &str) -> alloy::contract::Error {
    alloy::contract::Error::TransportError(alloy::transports::RpcError::ErrorResp(
        serde_json::from_value(serde_json::json!({ "code": code, "message": message })).unwrap(),
    ))
}

#[tokio::test(start_paused = true)]
async fn a_rate_limited_page_is_retried_not_abandoned() {
    // Regression: the rate limit must consume the full 1/5/30 s schedule
    // rather than falling through to the cache on the first response.
    let calls = std::cell::Cell::new(0usize);
    let start = tokio::time::Instant::now();

    let out = paginate_with_retry(|_offset| {
        let n = calls.get();
        calls.set(n.saturating_add(1));
        async move {
            match n {
                // Rate-limited twice, then the provider lets us through.
                0 => Err(error_resp(429, "Too Many Requests")),
                1 => Err(error_resp(-32005, "exceeded project rate limit")),
                _ => Ok(active_page(vec![node_info(valid_node_id(3), true)])),
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(calls.get(), 3, "two rate limits were retried, not surfaced");
    assert_eq!(
        tokio::time::Instant::now() - start,
        REGISTRY_RETRY_BACKOFF[0] + REGISTRY_RETRY_BACKOFF[1],
        "backed off on the first two steps of the schedule"
    );
    assert_eq!(out.len(), 1);
}

fn valid_node_id(seed: u8) -> [u8; 32] {
    *iroh::SecretKey::from_bytes(&[seed; 32]).public().as_bytes()
}

/// Wrap a page of `NodeInfo` with an all-`true` `active[]` of matching length,
/// mirroring what `getRegisteredNodes` returns for a page of active operators.
/// These pagination tests exercise the retry/cursor control flow, not the
/// active-filter, so every entry is active.
fn active_page(infos: Vec<CapacityBond::NodeInfo>) -> (Vec<CapacityBond::NodeInfo>, Vec<bool>) {
    let n = infos.len();
    (infos, vec![true; n])
}

#[tokio::test(start_paused = true)]
async fn a_failing_page_is_retried_on_the_adr_schedule() {
    // Paused time auto-advances on idle, so the whole 36 s schedule runs
    // instantly and the elapsed virtual time is itself assertable.
    let calls = std::cell::Cell::new(0usize);
    let start = tokio::time::Instant::now();

    let err = paginate_with_retry(|_offset| {
        calls.set(calls.get().saturating_add(1));
        async { Err(transient()) }
    })
    .await
    .unwrap_err();

    assert_eq!(
        calls.get(),
        1 + REGISTRY_RETRY_BACKOFF.len(),
        "one initial call plus one per backoff step"
    );
    assert_eq!(
        tokio::time::Instant::now() - start,
        REGISTRY_RETRY_BACKOFF.iter().sum::<Duration>(),
        "slept exactly 1 + 5 + 30 s"
    );
    assert!(format!("{err:#}").contains("failed after 3 retries"));
}

/// The retry budget is spent across the WHOLE read, not refilled per page
/// (#1349). Without this, a fully-failing N-page read cost 36 s × N — a
/// figure the caller cannot bound, since the page count follows how many
/// nodes are registered.
///
/// The fixture puts retries on both sides of a page boundary: page 0 fails
/// twice then succeeds with a FULL page (so pagination continues), and
/// page 1 fails forever. Only the third backoff step is left for page 1, so
/// the read ends after 5 calls having slept the schedule exactly once.
/// Under the old per-page reset it would be 7 calls and 42 s.
#[tokio::test(start_paused = true)]
async fn the_retry_budget_is_spent_across_pages_not_refilled() {
    let calls = std::cell::Cell::new(0usize);
    let start = tokio::time::Instant::now();

    let err = paginate_with_retry(|offset| {
        let n = calls.get();
        calls.set(n.saturating_add(1));
        async move {
            match (offset, n) {
                // Page 0: two transient failures burn steps 1 and 2 …
                (0, 0 | 1) => Err(transient()),
                // … then a full page, which is what makes the loop ask for
                // a second page rather than terminating on a short one.
                (0, _) => Ok(active_page(
                    (0..PAGE_SIZE)
                        .map(|i| {
                            // `u8` seeds wrap past 255; PAGE_SIZE is 100, so
                            // every id here is distinct regardless.
                            node_info(valid_node_id(u8::try_from(i).unwrap_or(0)), true)
                        })
                        .collect(),
                )),
                // Page 1 never succeeds.
                _ => Err(transient()),
            }
        }
    })
    .await
    .unwrap_err();

    assert_eq!(
        calls.get(),
        5,
        "3 calls for page 0 (2 failures + success), then 2 for page 1 \
         (initial + the single remaining retry) — not 7"
    );
    assert_eq!(
        tokio::time::Instant::now() - start,
        REGISTRY_RETRY_BACKOFF.iter().sum::<Duration>(),
        "the schedule is slept once for the whole read, not once per page"
    );
    let msg = format!("{err:#}");
    assert!(msg.contains("across the whole registry read"), "{msg}");
    assert!(
        msg.contains(&format!("offset={PAGE_SIZE}")),
        "the error names the page that ran the budget out: {msg}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_permanent_failure_is_not_retried() {
    // A typo'd capacity_bond_address must not cost the user 36 s of silence
    // before being told what is wrong.
    let calls = std::cell::Cell::new(0usize);
    let start = tokio::time::Instant::now();

    let err = paginate_with_retry(|_offset| {
        calls.set(calls.get().saturating_add(1));
        async { Err(permanent()) }
    })
    .await
    .unwrap_err();

    assert_eq!(calls.get(), 1, "a deterministic failure is not repeated");
    assert_eq!(tokio::time::Instant::now() - start, Duration::ZERO);
    assert!(format!("{err:#}").contains("will not succeed on retry"));
    assert!(format!("{err:#}").contains("capacity_bond_address"));
}

/// `getRegisteredNodes` returns `page` and `active` as equal-length arrays by
/// construction (the contract fills both in one loop). A divergence means an
/// ABI/decoder fault, and zipping would silently truncate — dropping
/// candidates without a trace. Fail loudly instead.
#[tokio::test]
async fn mismatched_page_and_active_lengths_error_rather_than_truncate() {
    let err = paginate_with_retry(|_offset| async {
        Ok((vec![node_info(valid_node_id(1), true)], Vec::<bool>::new()))
    })
    .await
    .unwrap_err();
    assert!(
        format!("{err:#}").contains("mismatched"),
        "expected a length-mismatch error, got: {err:#}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_retry_that_succeeds_resumes_pagination() {
    // The offsets requested are the assertion: a retry must re-request the
    // same page, and only a full page may advance the cursor.
    let seen = std::cell::RefCell::new(Vec::new());
    let attempt = std::cell::Cell::new(0usize);

    let out = paginate_with_retry(|offset| {
        seen.borrow_mut().push(offset);
        let n = attempt.get();
        attempt.set(n.saturating_add(1));
        async move {
            match n {
                // The first page fails once, then serves a full page…
                0 => Err(transient()),
                1 => Ok(active_page(
                    (0..PAGE_SIZE)
                        .map(|_| node_info(valid_node_id(1), true))
                        .collect(),
                )),
                // …and the short second page ends the read.
                _ => Ok(active_page(vec![node_info(valid_node_id(2), true)])),
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(
        *seen.borrow(),
        vec![0, 0, PAGE_SIZE],
        "the retry re-requests offset 0, then the cursor advances by one page"
    );
    assert_eq!(out.len(), usize::try_from(PAGE_SIZE).unwrap() + 1);
}

/// Seed the peer store at `data_dir` with `peers`' identities, as
/// `resolve_bootstrap` itself does on a live read.
fn seed_store(data_dir: &Path, peers: &[NodeCandidate], now: u64) {
    let store = crate::PeerStore::open(data_dir);
    for cand in peers {
        store.upsert_identity(cand, now).unwrap();
    }
}

/// The identities the store at `data_dir` currently holds, projected back
/// to [`NodeCandidate`]s (order-independent — callers compare as sets).
fn store_candidates(data_dir: &Path) -> Vec<NodeCandidate> {
    crate::PeerStore::open(data_dir)
        .load_all()
        .into_iter()
        .map(|r| r.as_candidate())
        .collect()
}

#[test]
fn successful_read_persists_identities_and_warns_about_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let peers = vec![candidate(5, "FR")];
    let out = resolve_bootstrap(Ok(peers.clone()), dir.path()).unwrap();
    assert!(out.warning().is_none(), "a healthy bootstrap is quiet");
    assert_eq!(out.into_peers(), peers);
    assert_eq!(store_candidates(dir.path()), peers);
}

#[test]
fn registry_failure_falls_back_to_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let peers = vec![candidate(3, "US"), candidate(4, "DE")];
    seed_store(dir.path(), &peers, now_secs());

    let out = resolve_bootstrap(Err(anyhow::anyhow!("rpc down")), dir.path()).unwrap();
    let warning = out.warning().unwrap();
    assert!(warning.contains("could not reach the node registry"));
    assert!(
        warning.contains("rpc down"),
        "the warning names the cause, not just the symptom: {warning}"
    );
    assert!(warning.contains("deactivated or slashed"));
    assert!(
        warning.contains("identity up to"),
        "the warning names the staleness of the fallback identities: {warning}"
    );
    assert_matches!(&out, Bootstrap::Cached { .. });
    let mut got = out.into_peers();
    let mut want = peers;
    got.sort_by_key(|c| c.node_id);
    want.sort_by_key(|c| c.node_id);
    assert_eq!(got, want);
}

/// ADR 012 requires the cached-fallback warning to say how old the
/// identities it serves are (§ Bootstrap, step 7). Seed a record whose
/// identity was last confirmed hours ago and check the warning reports
/// that age, not just that the fallback happened.
#[test]
fn registry_failure_warning_reports_identity_staleness() {
    let dir = tempfile::tempdir().unwrap();
    let now = now_secs();
    let stale_secs = 3 * 3_600; // 3 hours old
    seed_store(
        dir.path(),
        &[candidate(9, "JP")],
        now.saturating_sub(stale_secs),
    );

    let out = resolve_bootstrap(Err(anyhow::anyhow!("rpc down")), dir.path()).unwrap();
    let warning = out.warning().unwrap();
    assert!(
        warning.contains("identity up to 3h old") || warning.contains("identity up to 2h old"),
        "the warning reports a coarse age around the seeded staleness: {warning}"
    );
}

/// Identity past the prune horizon is dropped from the fallback set — a
/// node absent from the registry this long has likely left the bond set.
#[test]
fn registry_failure_excludes_prunable_identities_from_the_fallback() {
    let cfg = crate::StoreConfig::default();
    let dir = tempfile::tempdir().unwrap();
    let store = crate::PeerStore::open(dir.path());
    let now = now_secs();
    // `resolve_bootstrap` stamps its own `now_secs()`, so both fixtures are
    // anchored to it: one seen long enough ago to be prunable, one seen
    // "just now" and not.
    store
        .upsert_identity(
            &candidate(1, "DE"),
            now.saturating_sub(cfg.identity_prune_secs + 1),
        )
        .unwrap();
    store.upsert_identity(&candidate(2, "DE"), now).unwrap();

    let out = resolve_bootstrap(Err(anyhow::anyhow!("rpc down")), dir.path()).unwrap();
    assert_eq!(out.into_peers(), vec![candidate(2, "DE")]);
}

/// The `--timeout-ms` bound must not cost a client its outage protection
/// (#1349). Timing out is one more way for the registry read to fail, so it
/// has to land on the store-fallback path like any other failure.
///
/// This is the regression a naive fix reintroduces: wrapping
/// `bootstrap_nodes` in `tokio::time::timeout` from the CALL SITE cancels
/// `resolve_bootstrap` along with the read, so a client holding a
/// perfectly good peer store gets a hard error instead of a working
/// fetch. Any `--timeout-ms` below the schedule's own 36 s hits this,
/// which is exactly the range the flag was widened for.
#[tokio::test(start_paused = true)]
async fn a_timed_out_registry_read_still_falls_back_to_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let peers = vec![candidate(7, "US")];
    seed_store(dir.path(), &peers, now_secs());

    // A listener that is never accepted: the kernel completes the
    // handshake into the backlog and buffers the request, so the RPC call
    // can never get a reply or an error. With no error there is no retry
    // backoff, and with no reply there is no result, so the 5 s cap is the
    // only way out. Paused time makes that cap instant.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rpc_url = format!("http://{}", listener.local_addr().unwrap());
    let out = bootstrap_nodes(
        &rpc_url,
        Address::repeat_byte(0x11),
        dir.path(),
        Duration::from_secs(5),
    )
    .await
    .expect("a timeout with a usable store must not fail the fetch");

    let warning = out.warning().unwrap();
    assert!(
        warning.contains("did not finish within"),
        "the warning names the deadline as the cause: {warning}"
    );
    assert!(
        warning.contains("--timeout-ms"),
        "and names the flag that set it: {warning}"
    );
    assert_eq!(
        out.into_peers(),
        peers,
        "the stored peers are what the fetch proceeds with"
    );
}

#[test]
fn registry_failure_without_a_store_reports_the_adr_error() {
    let dir = tempfile::tempdir().unwrap();
    let err = resolve_bootstrap(Err(anyhow::anyhow!("rpc down")), dir.path()).unwrap_err();
    assert_eq!(
        format!("{err}"),
        "Cannot reach bootstrap sources. Check network connectivity and RPC endpoint \
         configuration.",
        "the ADR 012 § Bootstrap step 4 wording, verbatim"
    );
    assert_eq!(format!("{err}"), BOOTSTRAP_UNREACHABLE);
    // The registry failure stays in the chain as the cause, and `main()`
    // renders `{err:#}`, so this is what the user actually sees.
    assert!(format!("{err:#}").contains("rpc down"));
}

#[test]
fn an_empty_store_adds_nothing_to_the_error() {
    // With no peer store entries there is nothing to report, so the chain
    // must not gain a spurious layer on a fresh install.
    let dir = tempfile::tempdir().unwrap();
    let err = resolve_bootstrap(Err(anyhow::anyhow!("rpc down")), dir.path()).unwrap_err();
    let rendered = sanitize_err_chain(&err);
    assert_eq!(rendered, format!("{BOOTSTRAP_UNREACHABLE}: rpc down"));
}

#[test]
fn an_empty_registry_read_leaves_a_populated_store_intact() {
    let dir = tempfile::tempdir().unwrap();
    let peers = vec![candidate(6, "US")];
    seed_store(dir.path(), &peers, now_secs());
    assert!(
        resolve_bootstrap(Ok(Vec::new()), dir.path())
            .unwrap()
            .into_peers()
            .is_empty()
    );
    assert_eq!(store_candidates(dir.path()), peers);
}

#[test]
fn an_empty_registry_read_does_not_prune_a_stale_identity() {
    // Seed an identity old enough to clear `IDENTITY_PRUNE_SECS` relative
    // to "now", so it would be pruned if `resolve_bootstrap` ran
    // `prune_and_cap` on this empty-but-successful read. It must not: an
    // emptied registry is not a reason to discard the last known-good
    // identities, so the stale entry must survive untouched.
    let dir = tempfile::tempdir().unwrap();
    let peers = vec![candidate(7, "DE")];
    let stale_seen_at = now_secs()
        .saturating_sub(crate::peer_store::IDENTITY_PRUNE_SECS)
        .saturating_sub(1);
    seed_store(dir.path(), &peers, stale_seen_at);

    assert!(
        resolve_bootstrap(Ok(Vec::new()), dir.path())
            .unwrap()
            .into_peers()
            .is_empty()
    );
    assert_eq!(
        store_candidates(dir.path()),
        peers,
        "the stale identity must still be in the store: an empty successful \
         read must not prune"
    );
}

fn warming(seed: u8, rtt_ms: f64) -> WarmingCandidate {
    WarmingCandidate {
        node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
        eth_address: Address::repeat_byte(seed),
        rtt_ms,
        multiaddrs: Bytes::new(),
    }
}

#[test]
fn proxy_warming_no_op_when_best_holder_is_near() {
    // Best holder RTT 40ms is below the 100ms threshold: holders aren't
    // distant, so warming does not engage even with a fast candidate.
    let cands = vec![warming(1, 5.0)];
    assert!(proxy_warming_order(40.0, 100.0, 20.0, &cands).is_empty());
}

#[test]
fn proxy_warming_no_op_when_no_candidate_clears_margin() {
    // Best holder is distant (300ms > 100ms threshold) but the nearest
    // candidate (290ms) only beats it by 10ms, below the 20ms margin.
    let cands = vec![warming(1, 290.0)];
    assert!(proxy_warming_order(300.0, 100.0, 20.0, &cands).is_empty());
}

#[test]
fn proxy_warming_picks_qualifying_candidates_nearest_first() {
    // Best holder 300ms; threshold 100, margin 20. Candidates at 30 and 80ms
    // both clear the margin; 290ms does not. Ordered nearest-first.
    let cands = vec![warming(1, 80.0), warming(2, 290.0), warming(3, 30.0)];
    let order = proxy_warming_order(300.0, 100.0, 20.0, &cands);
    assert_eq!(order.len(), 2);
    assert_eq!(order[0].eth_address, Address::repeat_byte(3)); // 30ms first
    assert_eq!(order[1].eth_address, Address::repeat_byte(1)); // then 80ms
}

/// A NaN best-holder RTT must fail closed (route direct) rather than sneak
/// past the trigger comparison, and must never panic the sort.
///
/// The companion region guarantee (ADR 037 §"Ranking key is measured RTT
/// only") is enforced structurally, not by this test: `WarmingCandidate`
/// has no region field, so `proxy_warming_order` cannot consult one. Adding
/// such a field would require editing the struct — a visible, reviewable
/// change — which is the point.
#[test]
fn nan_best_holder_rtt_fails_closed() {
    let cands = vec![warming(1, 10.0)];
    assert!(proxy_warming_order(f64::NAN, 100.0, 20.0, &cands).is_empty());
}

/// A non-finite candidate RTT must be filtered out rather than sorted
/// first — `total_cmp` orders NaN, so an unfiltered NaN would win.
#[test]
fn non_finite_candidate_rtt_is_filtered_out() {
    let cands = vec![warming(1, f64::NAN), warming(2, 30.0)];
    let order = proxy_warming_order(300.0, 100.0, 20.0, &cands);
    assert_eq!(order.len(), 1);
    assert_eq!(order[0].eth_address, Address::repeat_byte(2));
}
