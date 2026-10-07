use super::*;
use rand::SeedableRng;

#[test]
fn probe_timeout_matches_the_adr_collection_ceiling() {
    assert_eq!(PROBE_TIMEOUT, Duration::from_millis(500));
}

// #859 regression guard: the derived outer pull-through deadline must strictly
// exceed the sum of what all `MAX_PROVIDER_ATTEMPTS` candidates can actually
// cost, so the outer `tokio::time::timeout` can never preempt the fallback loop
// when early candidates stall.
//
// Asserted against the REAL worst case per candidate — all THREE sequential stages
// (channel open, stream open, one silent streaming window) — not against the
// formula's own arithmetic. Restated with any stage missing, this test passes while
// the loop silently cannot reach the last candidate. Both of the deadline's shipped
// versions were wrong in exactly that way, one stage apart.
//
// `stall` is swept independently of `per` because it is the term an operator can
// raise on its own: a formula that ignores it looks fine at defaults and starves the
// loop at `node_pull_stall_window_sec = 120`.
#[test]
fn outer_pull_deadline_exceeds_what_every_candidate_can_actually_cost() {
    let attempts = u32::try_from(MAX_PROVIDER_ATTEMPTS).unwrap_or(u32::MAX);
    for per_secs in [1_u64, 5, 20, 60] {
        for stall_secs in [1_u64, 5, 20, 120] {
            let per = Duration::from_secs(per_secs);
            let stall = Duration::from_secs(stall_secs);
            let outer = outer_pull_deadline(per, stall);
            // One candidate = channel open, then stream open, then a silent stream.
            let worst_candidate = CHANNEL_OPEN_CALLER_BUDGET
                .saturating_add(per)
                .saturating_add(stall);
            let all_candidates = worst_candidate.saturating_mul(attempts);
            assert!(
                outer > all_candidates,
                "outer {outer:?} must exceed {MAX_PROVIDER_ATTEMPTS}×{worst_candidate:?} \
                 (channel open + stream open + stall), or the loop cannot reach the \
                 last candidate"
            );
            assert_eq!(outer, all_candidates + PULL_THROUGH_OUTER_SLACK);
        }
    }
}

// There is deliberately NO unit test here asserting that the slack covers the
// `discover → probe → rank` overhead it is named for (#1145 review).
//
// `PULL_THROUGH_OUTER_SLACK` is DEFINED as
// `PROBE_TIMEOUT + DEFAULT_ROUND_TIMEOUT × MAX_LOOKUP_ROUNDS`, so a test that recomputes
// that expression and asserts the slack is at least as big asserts `A >= A`. It
// would pass with any value of any of the three constants — which is precisely the sin
// its own doc comment accused its predecessor of ("proves only that the slack is
// whatever the slack is"), restated one level up. Deriving the constant is what MAKES it
// correct by construction; there is nothing left for arithmetic to check.
//
// What can still break is the assumption underneath: that `find_providers` actually
// honours `MAX_LOOKUP_ROUNDS`. An unbounded cost cannot be budgeted for by any constant,
// however generous, so if that loop stops terminating the slack is worthless no matter
// what it evaluates to. That is a property of the LOOP, not of this arithmetic, and it is
// guarded where it lives — `dht_lookup::find_providers_stops_at_the_round_ceiling_even_
// while_still_finding_closer_nodes` walks a chain of servers that keeps revealing closer
// nodes and asserts the lookup is cut off before it reaches a record six hops away.

// The defaults an operator actually runs: `node_pull_timeout_sec = 20` and
// `node_pull_stall_window_sec = 20` (both `DEFAULT_*` in decdn-common, which this
// crate does not depend on — hence the literals). Pinned because the worst-case client
// wait is a user-visible number RESTATED IN PROSE elsewhere, and it moved three times
// while the formula was corrected — most recently when the slack stopped being a guess
// (#1145 review): 10 s -> 0.5 + 4×8 = 32.5 s, so 145 s -> 167.5 s.
//
// The sites that restate it, so the next person to move it can find them all — this list
// is the whole reason the number keeps going stale, and the previous version of this
// comment named the wrong ones ("the CLI help and the metrics docs"; the metrics docs
// never quoted it):
//
//   - `common::config` (the `node_pull_timeout_sec` / `node_pull_stall_window_sec` docs)
//   - `cli::commands::config` (the DEFAULT_CONFIG template)
#[test]
fn outer_pull_deadline_at_defaults_is_167_5s() {
    let outer = outer_pull_deadline(Duration::from_secs(20), Duration::from_secs(20));
    assert_eq!(
        outer,
        Duration::from_millis(167_500),
        "(5s + 20s + 20s) × 3 + (500ms + 4 × 8s)"
    );
}

// A zero per-candidate budget still yields a positive outer deadline (the
// channel-open budgets + the slack), and a saturating multiply can't panic on
// absurd inputs.
#[test]
fn outer_pull_deadline_handles_edges() {
    let attempts = u32::try_from(MAX_PROVIDER_ATTEMPTS).unwrap_or(u32::MAX);
    assert_eq!(
        outer_pull_deadline(Duration::ZERO, Duration::ZERO),
        CHANNEL_OPEN_CALLER_BUDGET.saturating_mul(attempts) + PULL_THROUGH_OUTER_SLACK
    );
    // Saturates to MAX rather than overflowing/panicking — guards against a
    // future switch to non-saturating arithmetic. Checked on each term
    // independently, since either one alone can saturate the sum.
    assert_eq!(
        outer_pull_deadline(Duration::MAX, Duration::ZERO),
        Duration::MAX
    );
    assert_eq!(
        outer_pull_deadline(Duration::ZERO, Duration::MAX),
        Duration::MAX
    );
}

// ADR 001 multiplier table: rep=1.0 → 1×, 0.8 → 1.56×, 0.5 → 4×, 0.3 → 11.1×, 0.1 → 100×.

#[test]
fn score_reputation_1_0_is_baseline() {
    let s = compute_score(100, 10, 1.0);
    assert!((s - 1000.0).abs() < 1e-9, "got {s}");
}

#[test]
fn score_reputation_0_5_is_4x_baseline() {
    let baseline = compute_score(100, 10, 1.0);
    let s = compute_score(100, 10, 0.5);
    assert!(
        (s / baseline - 4.0).abs() < 1e-3,
        "got ratio {}",
        s / baseline
    );
}

#[test]
fn score_reputation_0_1_is_100x_baseline() {
    let baseline = compute_score(100, 10, 1.0);
    let s = compute_score(100, 10, 0.1);
    assert!(
        (s / baseline - 100.0).abs() < 1e-3,
        "got ratio {}",
        s / baseline
    );
}

#[test]
fn score_reputation_0_0_clamps_to_floor() {
    let at_zero = compute_score(100, 10, 0.0);
    let at_floor = compute_score(100, 10, 0.1);
    assert!((at_zero - at_floor).abs() < 1e-9);
}

#[test]
fn score_negative_reputation_clamps_to_floor() {
    // Defensive: LocalReputation::score() is in [0,1] by construction, but
    // if a bug produces a negative we must still not panic and must clamp.
    let s = compute_score(100, 10, -0.5);
    let at_floor = compute_score(100, 10, 0.1);
    assert!((s - at_floor).abs() < 1e-9);
}

#[test]
fn score_nan_reputation_clamps_to_floor() {
    // Defensive, and load-bearing on a subtle IEEE detail: `f32::max`
    // implements `maxNum`, which *ignores* NaN and returns the other
    // operand — so `NaN.max(0.1)` is `0.1` and no NaN can reach the
    // score. That matters because a NaN score would sort last under
    // `total_cmp` and then be swept into the preceding tie group by
    // `tie_group_end` (`next > pivot` is false for NaN), letting a
    // garbage candidate win a tie-break tier. Note `f32::clamp` does
    // NOT have this property — it propagates NaN — so a future
    // "cleanup" to `.clamp(REPUTATION_FLOOR, 1.0)` would silently
    // reintroduce the hazard. This test is what catches that.
    let s = compute_score(100, 10, f32::NAN);
    let at_floor = compute_score(100, 10, 0.1);
    assert!(s.is_finite(), "NaN reputation must not produce a NaN score");
    assert!((s - at_floor).abs() < 1e-9);
}

#[test]
fn score_reputation_above_1_0_clamps_to_ceiling() {
    // Defensive, and the mirror of the floor clamp (#1458). `Candidate.
    // reputation` is documented as `[0.0, 1.0]`, but the field is a plain
    // `f32` — the domain is a convention, not a type. Today's only producer
    // (`peer_reputation` → `LocalReputation::score`) is in-range by
    // construction; a future one need not be, and without the ceiling an
    // out-of-domain value buys rank instead of losing it.
    let baseline = compute_score(100, 10, 1.0);
    let above = compute_score(100, 10, 10.0);
    assert!((above - baseline).abs() < 1e-9, "got {above} vs {baseline}");
}

#[test]
fn score_reputation_one_ulp_above_domain_clamps_to_ceiling() {
    // `f32::EPSILON` is exactly one ULP at 1.0, so unclamped this scores
    // ~999.99976 — outside the 1e-9 tolerance. Pins the ceiling as exact at
    // 1.0 with no tolerance band, which a sloppier guard (`if rep > 2.0`)
    // would not give.
    let baseline = compute_score(100, 10, 1.0);
    let above = compute_score(100, 10, 1.0 + f32::EPSILON);
    assert!((above - baseline).abs() < 1e-9, "got {above} vs {baseline}");
}

#[test]
fn score_infinite_reputation_clamps_to_ceiling() {
    // The degenerate end of the same hole: `INFINITY` squared is
    // `INFINITY`, so an unclamped score is `x / inf` = `0.0` — the minimum
    // achievable score, which sorts first under `total_cmp` AND which
    // `tie_group_end` gives a group of its own (the `pivot == 0.0` branch),
    // so no tie-break tier can dislodge it. Clamped, it must be
    // indistinguishable from a perfect-reputation candidate.
    let baseline = compute_score(100, 10, 1.0);
    let s = compute_score(100, 10, f32::INFINITY);
    // Note `is_finite` alone would NOT have caught the bug — `0.0` is
    // finite. It guards a future impl that yields NaN; `s > 0.0` is what
    // fails against the unclamped version.
    assert!(s.is_finite(), "got {s}");
    assert!(
        s > 0.0,
        "infinite reputation must not score 0.0 and sort first"
    );
    assert!((s - baseline).abs() < 1e-9, "got {s} vs {baseline}");
}

#[test]
fn score_floor_value_is_zero_point_one() {
    // Pin REPUTATION_FLOOR's actual value (not just clamping behavior). At
    // rate=100, rtt=10, rep clamps to ~0.1: score ≈ 1000 / 0.01 ≈ 100_000.
    // The 1e-2 tolerance accounts for the f32→f64 promotion of 0.1
    // (f32(0.1) ≈ 0.10000000149f64, so the squared denominator is
    // slightly above 0.01 → score ≈ 99999.997). Test still catches a
    // drift to e.g. 0.05 (→ 400_000) or 0.2 (→ 25_000).
    let at_floor = compute_score(100, 10, 0.0);
    assert!((at_floor - 100_000.0).abs() < 1e-2, "got {at_floor}");
}

#[test]
fn score_handles_max_inputs_without_overflow() {
    // u64::MAX × u32::MAX is well within f64 range (~1.6e28 < 1.8e308).
    let s = compute_score(u64::MAX, u32::MAX, 1.0);
    assert!(s.is_finite(), "got {s}");
}

fn make_candidate(node_id: u8, rate: u64, rtt: u32, rep: f32) -> Candidate {
    Candidate {
        node_id: [node_id; 32],
        rate_per_mb: rate,
        rtt_ms: rtt,
        reputation: rep,
        region: "US".to_string(),
        stake: 0,
        coverage: Coverage::empty(),
        total_bytes_hint: None,
    }
}

fn with_region(mut c: Candidate, region: &str) -> Candidate {
    c.region = region.to_string();
    c
}

fn with_stake(mut c: Candidate, stake: u64) -> Candidate {
    c.stake = stake;
    c
}

fn with_coverage(mut c: Candidate, coverage: Coverage) -> Candidate {
    c.coverage = coverage;
    c
}

/// #1506: `rank_candidates` must carry each candidate's fresh
/// probe-confirmed coverage through unchanged — ranking reorders on
/// score alone, but the caller downstream (the ranged-drive loop)
/// needs to read which blocks each ranked candidate holds.
/// Two providers with disjoint single-block coverage ({block0} vs
/// {block1}) pin that the field rides alongside the candidate rather
/// than being dropped or averaged during ranking.
#[test]
fn rank_candidates_carries_each_candidates_fresh_probe_coverage() {
    let block0 = Coverage::from_block_indices(2, [0].into_iter());
    let block1 = Coverage::from_block_indices(2, [1].into_iter());
    // Distinct scores so ordering is deterministic without the tie-breaker.
    let holder0 = with_coverage(make_candidate(1, 100, 10, 1.0), block0.clone());
    let holder1 = with_coverage(make_candidate(2, 200, 10, 1.0), block1.clone());
    let out = rank_candidates(vec![holder0, holder1]);

    assert_eq!(out.len(), 2);
    let ranked0 = out
        .iter()
        .find(|r| r.candidate.node_id[0] == 1)
        .expect("holder0 present in ranked output");
    let ranked1 = out
        .iter()
        .find(|r| r.candidate.node_id[0] == 2)
        .expect("holder1 present in ranked output");
    assert_eq!(ranked0.candidate.coverage, block0);
    assert_eq!(ranked1.candidate.coverage, block1);
}

#[test]
fn higher_stake_wins_when_geo_tied() {
    // Same score, same region. Higher stake wins at tier 2.
    let small_stake = with_stake(with_region(make_candidate(1, 100, 10, 1.0), "US"), 1_000);
    let big_stake = with_stake(with_region(make_candidate(2, 100, 10, 1.0), "US"), 10_000);
    let out = rank_candidates(vec![small_stake, big_stake]);
    assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
}

#[test]
fn tier_two_prunes_the_loser_then_tier_three_randomizes_the_survivors() {
    // Two top-stake candidates plus one strictly lower: tier 2 must retain
    // BOTH leaders and drop the laggard, then tier 3 picks uniformly between
    // the survivors. This covers the tier-2 → tier-3 handoff — `retain`
    // actually removing someone, and the pool it leaves being larger than
    // one. A version where every stake is equal would exercise neither:
    // `retain` would keep everyone and this would collapse into a plain
    // tier-3 uniformity test.
    let a = with_stake(with_region(make_candidate(1, 100, 10, 1.0), "US"), 1_000);
    let b = with_stake(with_region(make_candidate(2, 100, 10, 1.0), "US"), 1_000);
    let laggard = with_stake(with_region(make_candidate(3, 100, 10, 1.0), "US"), 0);
    let mut winners = std::collections::HashSet::new();
    for seed in 0u64..32 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let out = rank_candidates_with_rng(vec![a.clone(), b.clone(), laggard.clone()], &mut rng);
        let first = out.first().map(|r| r.candidate.node_id[0]);
        assert_ne!(
            first,
            Some(3),
            "seed {seed}: the lower-stake candidate must never survive tier 2"
        );
        if let Some(id) = first {
            winners.insert(id);
        }
        if winners.len() == 2 {
            break;
        }
    }
    assert_eq!(
        winners.len(),
        2,
        "expected both top-stake candidates to win across 32 seeds, saw {winners:?}"
    );
}

#[test]
fn higher_stake_wins_when_all_regions_unseen() {
    // Two candidates fully tied on score; one US, one DE. At the start
    // of a fresh tie group `emitted_regions` is empty, so the geo tier
    // (tier 1) finds every region "unseen" — `any_unseen` is true but
    // the retain predicate keeps every candidate (nothing is in
    // `emitted_regions` yet). The geo tier is effectively a no-op for
    // the first pick from a fresh group, and stake (tier 2) decides —
    // US (10k) outranks DE (1k).
    //
    // This pins geo tier 1 as "prefer regions not yet emitted in this
    // group", not "prefer any specific region per se" — a future tweak
    // that biased the first pick toward, say, the alphabetically-first
    // region would change this outcome.
    let us_rich = with_stake(with_region(make_candidate(1, 100, 10, 1.0), "US"), 10_000);
    let de_poor = with_stake(with_region(make_candidate(2, 100, 10, 1.0), "DE"), 1_000);
    let out = rank_candidates(vec![us_rich, de_poor]);
    assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(1));
}

#[test]
fn rank_empty_returns_empty() {
    let out = rank_candidates(vec![]);
    assert!(out.is_empty());
}

#[test]
fn rank_single_candidate_returns_it() {
    let c = make_candidate(1, 100, 10, 1.0);
    let out = rank_candidates(vec![c.clone()]);
    assert_eq!(out.len(), 1);
    assert_eq!(out.first().map(|r| r.candidate.node_id), Some(c.node_id));
}

#[test]
fn rank_orders_by_score_ascending() {
    // c_cheap: rate=1, rtt=10, rep=1.0 → score 10
    // c_mid:   rate=10, rtt=10, rep=1.0 → score 100
    // c_dear:  rate=100, rtt=10, rep=1.0 → score 1000
    let c_dear = make_candidate(3, 100, 10, 1.0);
    let c_cheap = make_candidate(1, 1, 10, 1.0);
    let c_mid = make_candidate(2, 10, 10, 1.0);
    let out = rank_candidates(vec![c_dear, c_cheap, c_mid]);
    let ids: Vec<u8> = out.iter().map(|r| r.candidate.node_id[0]).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn within_1_percent_counts_as_tied() {
    // score_a = 1000, score_b = 1005 → within 0.5%, should tie-break.
    // Tier 1 (geo) is neutral (same region); tier 2 (stake) decides.
    let high_score = with_stake(with_region(make_candidate(1, 100, 10, 1.0), "US"), 1_000); // score 1000
    let low_score = with_stake(with_region(make_candidate(2, 1005, 1, 1.0), "US"), 10_000); // score 1005
    let out = rank_candidates(vec![high_score, low_score]);
    // Tied → higher-stake (id 2) wins despite higher raw score.
    assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
}

#[test]
fn outside_1_percent_does_not_tie_break() {
    // 1000 vs 1020 → 2% gap, no tie-break. Higher stake on the dearer
    // candidate can't pull it ahead.
    let cheap = with_stake(with_region(make_candidate(1, 100, 10, 1.0), "US"), 1_000); // score 1000
    let dear = with_stake(with_region(make_candidate(2, 1020, 1, 1.0), "US"), 10_000); // score 1020
    let out = rank_candidates(vec![cheap, dear]);
    assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(1));
}

#[test]
fn at_1_percent_boundary_counts_as_tied() {
    // 1000 vs 1010 → exactly 1.0% gap. tie_group_end uses `> TIE_THRESHOLD`,
    // so 1.0% is *inclusive* (still a tie). Pins the boundary against a
    // future change to `>=` that would silently exclude exact-1% pairs.
    // Stake on the dearer candidate proves the tie-break ran.
    let high_score = with_stake(with_region(make_candidate(1, 100, 10, 1.0), "US"), 1_000); // score 1000
    let low_score = with_stake(with_region(make_candidate(2, 1010, 1, 1.0), "US"), 10_000); // score 1010
    let out = rank_candidates(vec![high_score, low_score]);
    assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
}

#[test]
fn zero_score_candidates_group_together_for_tie_break() {
    // Two zero-score candidates (rate=0, both have score 0.0) must form a
    // single tie group so the stake tier picks between them. Without the
    // zero-pivot fix, each becomes its own singleton group and stake is
    // skipped entirely.
    let zero_low_stake = with_stake(with_region(make_candidate(1, 0, 10, 1.0), "US"), 1_000); // score 0, low stake
    let zero_high_stake = with_stake(with_region(make_candidate(2, 0, 10, 1.0), "US"), 10_000); // score 0, high stake
    let out = rank_candidates(vec![zero_low_stake, zero_high_stake]);
    // Tied at 0.0 → higher-stake (id 2) wins.
    assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
}

#[test]
fn zero_score_beats_positive_score() {
    // A zero-score candidate is strictly better than any positive-score
    // candidate and must not be tie-grouped with one.
    let zero = make_candidate(1, 0, 10, 1.0); // score 0
    let positive = make_candidate(2, 1, 1, 1.0); // score 1
    let out = rank_candidates(vec![positive, zero]);
    assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(1));
}

#[test]
fn sub_millisecond_rtt_keeps_the_price_signal() {
    // Both probes truncate to 0 ms. The cheaper peer must win every time,
    // not by the tier-3 coin flip.
    for _ in 0..64 {
        let dear = make_candidate(1, 10, 0, 0.5);
        let cheap = make_candidate(2, 5, 0, 0.5);
        let out = rank_candidates(vec![dear, cheap]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
    }
}

#[test]
fn sub_millisecond_rtt_does_not_outrank_a_cheaper_peer() {
    // 0 ms (a sub-ms probe) scores as 1 ms: 10 × 1 = 10 vs 5 × 1 = 5.
    let dear_fast = make_candidate(1, 10, 0, 0.5);
    let cheap = make_candidate(2, 5, 1, 0.5);
    let out = rank_candidates(vec![dear_fast, cheap]);
    assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
}

#[test]
fn geo_diversity_prefers_unseen_region() {
    // Three candidates fully tied by score; two in US, one in DE.
    // With tier-3 randomness the first pick may be any of the three, so the
    // test verifies the geo invariant directly: WHEN a US candidate is picked
    // first (the path where the geo tier matters), the second pick MUST be DE
    // — the only unseen region left in the tie group. Iterating seeds keeps
    // the test robust to RNG implementation changes.
    let us1 = with_region(make_candidate(1, 100, 10, 1.0), "US");
    let us2 = with_region(make_candidate(2, 100, 10, 1.0), "US");
    let de = with_region(make_candidate(3, 100, 10, 1.0), "DE");
    let mut tested_first_us = false;
    for seed in 0u64..32 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let out = rank_candidates_with_rng(vec![us1.clone(), us2.clone(), de.clone()], &mut rng);
        let regions: Vec<String> = out.iter().map(|r| r.candidate.region.clone()).collect();
        assert_eq!(regions.len(), 3);
        if regions.first().map(String::as_str) == Some("US") {
            assert_eq!(
                regions.get(1).map(String::as_str),
                Some("DE"),
                "seed {seed}: after first US pick, geo tier must surface DE next"
            );
            tested_first_us = true;
        }
    }
    assert!(
        tested_first_us,
        "expected at least one seed in 0..32 to produce first-pick=US"
    );
}

#[test]
fn geo_diversity_only_applies_within_tie_group() {
    // Distinct scores → geo tier irrelevant, raw score order wins.
    let us = with_region(make_candidate(1, 1000, 10, 1.0), "US"); // score 10000
    let de = with_region(make_candidate(2, 100, 10, 1.0), "DE"); // score 1000
    let out = rank_candidates(vec![us, de]);
    assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
}

#[test]
fn random_breaks_full_ties_deterministically_with_seed() {
    let a = make_candidate(1, 100, 10, 1.0);
    let b = make_candidate(2, 100, 10, 1.0);
    let mut rng_1 = rand::rngs::StdRng::seed_from_u64(42);
    let out_1 = rank_candidates_with_rng(vec![a.clone(), b.clone()], &mut rng_1);
    let mut rng_2 = rand::rngs::StdRng::seed_from_u64(42);
    let out_2 = rank_candidates_with_rng(vec![a, b], &mut rng_2);
    let ids_1: Vec<u8> = out_1.iter().map(|r| r.candidate.node_id[0]).collect();
    let ids_2: Vec<u8> = out_2.iter().map(|r| r.candidate.node_id[0]).collect();
    assert_eq!(ids_1, ids_2, "same seed must produce same order");
}

#[test]
fn random_tier_yields_different_orders_for_different_seeds() {
    let a = make_candidate(1, 100, 10, 1.0);
    let b = make_candidate(2, 100, 10, 1.0);
    let mut saw_ab = false;
    let mut saw_ba = false;
    for seed in 0u64..32 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let out = rank_candidates_with_rng(vec![a.clone(), b.clone()], &mut rng);
        match out.first().map(|r| r.candidate.node_id[0]) {
            Some(1) => saw_ab = true,
            Some(2) => saw_ba = true,
            _ => {}
        }
        if saw_ab && saw_ba {
            break;
        }
    }
    assert!(saw_ab && saw_ba, "expected both orderings across 32 seeds");
}

#[test]
fn geo_tier_resets_between_tie_groups() {
    // Two tie groups, each with two candidates. Tier 1 group has score ~10
    // (rate 1, rtt 10, rep 1.0). Tier 2 group has score ~1000 (rate 100,
    // rtt 10, rep 1.0). Within each group candidates are tied by score and
    // load. Each group has one US and one non-US candidate. If geo tracking
    // leaked across groups, the second group's first pick would be biased
    // away from whatever region the first group emitted; with proper
    // per-group reset, both groups behave independently.
    //
    // Concretely: pick a seeded RNG that, for the FIRST group,
    // deterministically picks US first (so group_regions={US} at that
    // moment). Then verify the SECOND group's first pick can be either US
    // or non-US (i.e., across many seeds we observe both — proving group 2
    // wasn't biased by group 1's US).
    let g1_us = with_region(make_candidate(1, 1, 10, 1.0), "US");
    let g1_de = with_region(make_candidate(2, 1, 10, 1.0), "DE");
    let g2_us = with_region(make_candidate(3, 100, 10, 1.0), "US");
    let g2_jp = with_region(make_candidate(4, 100, 10, 1.0), "JP");

    // Run with many seeds. We want to find at least one seed where group 1
    // emits US first AND group 2 emits US first. With per-group reset that's
    // possible (~1/4 of seeds); with cross-group leak group 2 always avoids
    // US after group 1 emits US, so we'd never see this.
    let mut saw_g2_us_after_g1_us = false;
    for seed in 0u64..64 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let out = rank_candidates_with_rng(
            vec![g1_us.clone(), g1_de.clone(), g2_us.clone(), g2_jp.clone()],
            &mut rng,
        );
        let regions: Vec<String> = out.iter().map(|r| r.candidate.region.clone()).collect();
        // Group 1 (lower score) is at indices 0..2; group 2 at 2..4.
        // We need: regions[0] == "US" (group 1 first emit) AND
        //         regions[2] == "US" (group 2 first emit).
        let g1_first_us = regions.first().map(String::as_str) == Some("US");
        let g2_first_us = regions.get(2).map(String::as_str) == Some("US");
        if g1_first_us && g2_first_us {
            saw_g2_us_after_g1_us = true;
            break;
        }
    }
    assert!(
        saw_g2_us_after_g1_us,
        "geo tier should reset between tie groups; without reset, group 2 would never pick US first after group 1 picked US"
    );
}

#[test]
fn rank_candidates_keeps_negative_reputation_candidate() {
    // Reputation is a graded weight, never a veto (ADR 001): a candidate
    // is only ever penalized by the `compute_score` clamp, never removed
    // from the pool. `score_negative_reputation_clamps_to_floor` covers
    // the clamp itself; this pins the list-membership half.
    let neg = make_candidate(1, 100, 10, -0.5);
    let out = rank_candidates(vec![neg]);
    assert_eq!(out.len(), 1);
    assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(1));
}

#[test]
fn rank_candidates_denies_out_of_domain_reputation_the_top_slot() {
    // The list-membership/ordering half of the ceiling clamp (#1458), the
    // mirror of `rank_candidates_keeps_negative_reputation_candidate`
    // above. `score_infinite_reputation_clamps_to_ceiling` covers the
    // arithmetic; this covers what no `compute_score` test can reach —
    // pre-clamp, rep = INFINITY scored 0.0, which `total_cmp` sorts first
    // and `tie_group_end` then hands a tie group of its own, so the bogus
    // candidate took the top slot outright. Post-clamp it is scored on its
    // price and latency like anyone else: 1000 vs the honest peer's 500,
    // far outside TIE_THRESHOLD, so the order is deterministic and the
    // random tie-break tier never runs.
    let bogus = make_candidate(1, 100, 10, f32::INFINITY);
    let honest = make_candidate(2, 50, 10, 1.0);
    let out = rank_candidates(vec![bogus, honest]);
    assert_eq!(out.len(), 2, "reputation is a weight, never a veto");
    assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
}
