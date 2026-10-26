use super::*;
use crate::targets::tests_support::node_key;

const MIB: u64 = 1024 * 1024;

fn cand(seed: u8, operator: u8) -> NodeCandidate {
    NodeCandidate {
        node_id: node_key(seed),
        eth_address: Address::repeat_byte(operator),
        region_hint: None,
        multiaddrs: alloy::primitives::Bytes::new(),
    }
}

fn answer(seed: u8, operator: u8, rtt_ms: f64, rate_per_mb: u64, has_blob: bool) -> NodeAnswer {
    NodeAnswer {
        candidate: cand(seed, operator),
        rtt_ms,
        rate_per_mb,
        has_blob,
        total_bytes: has_blob.then_some(10 * MIB),
        coverage: decdn_protocol::Coverage::empty(),
    }
}

fn census_of(answers: Vec<NodeAnswer>) -> HashCensus {
    HashCensus {
        hash: [7; 32],
        answers,
        unreachable: 0,
        rate_limited: 0,
        unverifiable: 0,
        unusable: 0,
    }
}

const fn no_warming() -> ProxyWarmingParams {
    ProxyWarmingParams {
        enabled: false,
        rtt_threshold_ms: 150.0,
        margin_ms: 30.0,
    }
}

fn order_seeds(order: &[&NodeAnswer]) -> Vec<PublicKey> {
    order.iter().map(|a| a.candidate.node_id).collect()
}

/// Low and high price every byte at the extreme rates; likely splits the
/// bytes evenly over the first `max_sources` nodes and rounds each share
/// up, as a lane pays it.
#[test]
fn price_splits_likely_over_the_first_sources() {
    let blob = PricedBlob {
        wire: 3 * MIB,
        rates: vec![10, 20, 30, 40, 50],
    };
    let got = price(&[blob], 0, 4).unwrap();
    assert_eq!(got.low, U256::from(30));
    assert_eq!(got.high, U256::from(150));
    // 3 MiB / 4 = 0.75 MiB per lane: 7.5 → 8, 15, 22.5 → 23, 30.
    assert_eq!(got.likely, U256::from(8 + 15 + 23 + 30));
}

/// A remainder that does not split evenly rides the first lane.
#[test]
fn price_puts_the_split_remainder_on_the_first_lane() {
    let blob = PricedBlob {
        wire: MIB + 1,
        rates: vec![1000, 1],
    };
    let got = price(&[blob], 0, 4).unwrap();
    // 524_289 bytes at 1000 → 501 (rounded up); 524_288 at 1 → 1.
    assert_eq!(got.likely, U256::from(502));
}

/// Per-lane rounding never lifts likely above high.
#[test]
fn price_keeps_likely_within_high() {
    let blob = PricedBlob {
        wire: 3 * MIB,
        rates: vec![10, 10, 10, 10],
    };
    let got = price(&[blob], 0, 4).unwrap();
    // Four lanes of 0.75 MiB at 10 round up to 8 each: 32 > 30.
    assert_eq!(got.high, U256::from(30));
    assert_eq!(got.likely, U256::from(30));
    assert!(got.low <= got.likely);
}

/// A blob with no rate still costs its bytes: they count as unsampled.
#[test]
fn price_counts_a_rateless_blob_as_unsampled() {
    let blobs = [
        PricedBlob {
            wire: MIB,
            rates: vec![10],
        },
        PricedBlob {
            wire: 3 * MIB,
            rates: Vec::new(),
        },
    ];
    let got = price(&blobs, 0, 4).unwrap();
    assert_eq!(got.low, U256::from(40));
    assert_eq!(got.high, U256::from(40));
    assert_eq!(got.likely, U256::from(40));
}

/// A max of zero sources still prices one lane.
#[test]
fn price_uses_one_lane_at_least() {
    let blob = PricedBlob {
        wire: MIB,
        rates: vec![10, 20],
    };
    assert_eq!(price(&[blob], 0, 0).unwrap().likely, U256::from(10));
}

/// With no rate anywhere there is no price.
#[test]
fn price_is_none_without_rates() {
    let blob = PricedBlob {
        wire: MIB,
        rates: Vec::new(),
    };
    assert_eq!(price(&[blob], 0, 4), None);
    assert_eq!(price(&[], MIB, 4), None);
}

/// Unsampled bytes cost the sampled blobs' mean likely rate, and count in
/// low and high at the extreme rates.
#[test]
fn price_extends_the_sample_to_unsampled_bytes() {
    let blobs = [
        PricedBlob {
            wire: MIB,
            rates: vec![10],
        },
        PricedBlob {
            wire: MIB,
            rates: vec![30],
        },
    ];
    let got = price(&blobs, 2 * MIB, 4).unwrap();
    assert_eq!(got.low, U256::from(40));
    assert_eq!(got.high, U256::from(120));
    // Sampled 2 MiB cost 40, so 2 more MiB at the same mean cost 40.
    assert_eq!(got.likely, U256::from(80));
}

/// The serve order is the pull's: holders nearest first, then nothing
/// else, with one node per operator and none above the ceiling.
#[test]
fn serve_order_is_holders_by_rtt_within_the_ceiling() {
    let census = census_of(vec![
        answer(1, 1, 40.0, 10, true),
        answer(2, 2, 10.0, 99, true),
        answer(3, 3, 20.0, 12, true),
        // Same operator as node 3, farther: dropped.
        answer(4, 3, 30.0, 5, true),
        // A non-holder does not serve while a holder does.
        answer(5, 5, 1.0, 1, false),
    ]);
    let order = census.serve_order(50, no_warming());
    assert_eq!(order_seeds(&order), vec![node_key(3), node_key(1)]);
    let all = census.serve_order(0, no_warming());
    assert_eq!(
        order_seeds(&all),
        vec![node_key(2), node_key(3), node_key(1)]
    );
}

/// The pull keeps one node per operator before the ceiling refuses it, so
/// an operator whose nearest node is above the ceiling drops out, even when
/// its farther node is below.
#[test]
fn serve_order_drops_an_operator_whose_nearest_node_is_above_the_ceiling() {
    let census = census_of(vec![
        answer(1, 1, 10.0, 90, true),
        answer(2, 1, 30.0, 5, true),
        answer(3, 3, 20.0, 12, true),
    ]);
    let order = census.serve_order(50, no_warming());
    assert_eq!(order_seeds(&order), vec![node_key(3)]);
}

/// With no holder, the non-holders serve through pull-through, nearest
/// first.
#[test]
fn serve_order_falls_back_to_pull_through_sources() {
    let census = census_of(vec![
        answer(1, 1, 30.0, 10, false),
        answer(2, 2, 10.0, 20, false),
    ]);
    let order = census.serve_order(0, no_warming());
    assert_eq!(order_seeds(&order), vec![node_key(2), node_key(1)]);
    assert_eq!(census.holders(), 0);
    assert_eq!(census.size_hint(), None);
}

/// A nearer non-holder leads the order when warming engages, as it leads
/// the pull's.
#[test]
fn serve_order_leads_with_a_warming_proxy() {
    let census = census_of(vec![
        answer(1, 1, 200.0, 10, true),
        answer(2, 2, 20.0, 15, false),
    ]);
    let warming = ProxyWarmingParams {
        enabled: true,
        rtt_threshold_ms: 150.0,
        margin_ms: 30.0,
    };
    let order = census.serve_order(0, warming);
    assert_eq!(order_seeds(&order), vec![node_key(2), node_key(1)]);
}

/// The size hint is the nearest holder's.
#[test]
fn size_hint_is_the_nearest_holders() {
    let mut far = answer(1, 1, 50.0, 10, true);
    far.total_bytes = Some(5);
    let mut near = answer(2, 2, 5.0, 10, true);
    near.total_bytes = Some(7);
    assert_eq!(census_of(vec![far, near]).size_hint(), Some(7));
}

/// The tally sorts each outcome under its hash and counts each failure.
#[test]
fn tally_sorts_outcomes_by_hash() {
    let hashes = [[1; 32], [2; 32]];
    let holder = ProbeOutcome::Holder(
        discovery::Probed {
            candidate: cand(1, 1),
            rtt_ms: 12.0,
            total_bytes: Some(99),
            coverage: decdn_protocol::Coverage::empty(),
        },
        (node_key(1), 12.0, 17),
    );
    let non_holder = ProbeOutcome::NonHolder(
        discovery::WarmingCandidate {
            node_id: node_key(2),
            eth_address: Address::repeat_byte(2),
            rtt_ms: 8.0,
            multiaddrs: alloy::primitives::Bytes::new(),
        },
        (node_key(2), 8.0, 21),
    );
    let outcomes = vec![
        (0, cand(1, 1), holder),
        (1, cand(2, 2), non_holder),
        (1, cand(3, 3), ProbeOutcome::Unreachable),
        (0, cand(4, 4), ProbeOutcome::RateLimited),
        (0, cand(5, 5), ProbeOutcome::Unverifiable),
        (1, cand(6, 6), ProbeOutcome::Unusable),
        // An index past the hashes is dropped.
        (9, cand(7, 7), ProbeOutcome::Unreachable),
    ];
    let census = tally(6, &hashes, outcomes, false);
    assert_eq!(census.probed, 6);
    let [first, second] = census.hashes.as_slice() else {
        panic!("two hashes");
    };
    assert_eq!(first.hash, [1; 32]);
    assert_eq!(first.answers.len(), 1);
    assert_eq!(first.answers[0].rate_per_mb, 17);
    assert_eq!(first.answers[0].total_bytes, Some(99));
    assert!(first.answers[0].has_blob);
    assert_eq!((first.rate_limited, first.unverifiable), (1, 1));
    assert_eq!(second.answers.len(), 1);
    assert_eq!(second.answers[0].rate_per_mb, 21);
    assert!(!second.answers[0].has_blob);
    assert_eq!((second.unreachable, second.unusable), (1, 1));
}

/// The peer-store harvest takes one sample per node, at its lowest RTT.
#[test]
fn samples_keep_each_nodes_nearest_answer() {
    let census = Census {
        probed: 1,
        hashes: vec![
            census_of(vec![answer(1, 1, 30.0, 10, true)]),
            census_of(vec![answer(1, 1, 20.0, 20, false)]),
        ],
        outage_fallback: false,
    };
    assert_eq!(census.samples(), vec![(node_key(1), 20.0, 20)]);
}

/// A census errors only when it probed a hash and no node answered any:
/// one answered hash is enough, and no hash at all is no error. The error
/// counts nodes, not probes.
#[test]
fn unanswered_needs_every_probed_hash_silent() {
    let silent = |unreachable| HashCensus {
        unreachable,
        ..census_of(Vec::new())
    };
    let none = Census {
        probed: 3,
        hashes: Vec::new(),
        outage_fallback: false,
    };
    assert!(unanswered(&none).is_none());
    let partial = Census {
        probed: 3,
        hashes: vec![census_of(vec![answer(1, 1, 5.0, 10, true)]), silent(3)],
        outage_fallback: false,
    };
    assert!(unanswered(&partial).is_none());
    let all = Census {
        probed: 3,
        hashes: vec![silent(3), silent(3)],
        outage_fallback: false,
    };
    let err = format!("{}", unanswered(&all).unwrap());
    assert!(err.contains("none of the 3 probed node(s)"), "{err}");
}

/// A signature that does not recover is a configuration fault, named so.
#[test]
fn unanswered_names_unverifiable_answers() {
    let census = Census {
        probed: 2,
        hashes: vec![HashCensus {
            unverifiable: 2,
            ..census_of(Vec::new())
        }],
        outage_fallback: false,
    };
    let err = unanswered(&census).unwrap();
    assert!(
        err.downcast_ref::<crate::targets::ResolveConfigFault>()
            .is_some(),
        "{err}"
    );
}

/// The store's rate stands for a node unless its last failure still
/// suppresses it; an unknown latency sorts as infinite.
#[test]
fn store_rate_skips_a_suppressed_node() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let store = crate::PeerStore::open(dir.path());
    let cfg = crate::StoreConfig::default();
    let now = 1_000_000;
    store.record_sample(&node_key(1), 12.0, 7, now, &cfg)?;
    assert_eq!(store_rate(&store, &node_key(1), now), Some((12.0, 7)));
    store.record_failure(&node_key(1), now)?;
    assert_eq!(store_rate(&store, &node_key(1), now), None);
    assert_eq!(store_rate(&store, &node_key(2), now), None);
    Ok(())
}

/// Hint rates keep the given order, keep the first node of each operator,
/// and then drop rates above the ceiling.
#[test]
fn hint_rates_filter_like_the_pull() {
    let hint = |operator: u8, rate_per_mb: u64| RateHint {
        provider: Address::repeat_byte(operator),
        rate_per_mb,
    };
    let hints = [hint(1, 30), hint(2, 90), hint(1, 5), hint(3, 10)];
    assert_eq!(hint_rates(&hints, 50), vec![30, 10]);
    assert_eq!(hint_rates(&hints, 0), vec![30, 90, 10]);
    // The operator's first hint is above the ceiling: its cheaper second
    // hint does not take its place.
    assert_eq!(
        hint_rates(&[hint(1, 90), hint(1, 5)], 50),
        Vec::<u64>::new()
    );
}

/// The wire bytes carry the bao proof on top of the content.
#[test]
fn wire_bytes_add_the_proof() {
    assert_eq!(wire_bytes(0).unwrap(), 0);
    let wire = wire_bytes(MIB).unwrap();
    assert!(wire > MIB && wire < MIB + MIB / 100, "{wire}");
}
