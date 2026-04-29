//! Provider selection algorithm (ADR 001 §Node Selection Algorithm + ADR 008 §9).
//!
//! `rank_candidates` returns the input list ordered best-first (lowest score
//! first) with the four-tier tie-breaker applied. The caller iterates the
//! result in order and stops after `MAX_PROVIDER_ATTEMPTS` failed providers.

use decdn_protocol::gossip::LoadHint;
use rand::RngExt;
use std::collections::HashSet;

/// Maximum providers to attempt before reporting a fetch failure to the
/// caller (issue #322 — "max 3 provider attempts before returning error").
pub const MAX_PROVIDER_ATTEMPTS: usize = 3;

/// Reputation floor in the score denominator (ADR 001).
const REPUTATION_FLOOR: f32 = 0.1;

/// Score-equivalence threshold for tie-break activation (ADR 001 — "scores
/// within 1% of each other").
const TIE_THRESHOLD: f64 = 0.01;

/// A candidate provider produced by content discovery, ready to be ranked.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// Iroh `NodeId` (Ed25519 public key) of the candidate.
    pub node_id: [u8; 32],
    /// Quoted price in token base units per MB, from the most recent
    /// `ProbeResponse` (`decdn_protocol::message::ProbeResponse::rate_per_mb`).
    pub rate_per_mb: u64,
    /// Round-trip latency observed during probing.
    pub rtt_ms: u32,
    /// Local reputation in `[0.0, 1.0]` from the reputation engine.
    pub reputation: f32,
    /// Most recent advertised load from `NodeAnnounce`. Used by the
    /// load tie-break tier.
    pub load: LoadHint,
    /// ISO 3166-1 alpha-2 region from `NodeAnnounce`. Used by the
    /// geo-diversity tie-break tier.
    pub region: String,
    /// On-chain stake in TOKEN base units. `None` until on-chain stake
    /// lookup is wired (out of scope for issue #322); when populated,
    /// higher stake wins the stake tie-break tier.
    pub stake: Option<u64>,
}

/// A candidate paired with its computed selection score. Lower score is better.
#[derive(Debug, Clone)]
pub struct RankedCandidate {
    pub candidate: Candidate,
    pub score: f64,
}

/// Compute the unified selection score (ADR 001). Lower is better.
///
/// `score = rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)`
///
/// Reputation is clamped to [`REPUTATION_FLOOR`] before squaring; this both
/// prevents division by zero and caps the worst-case multiplier at 100×.
#[allow(clippy::cast_precision_loss)]
// f64 has 53-bit mantissa; ULP-level imprecision on huge u64 rates does not
// affect ordering decisions here.
fn compute_score(rate_per_mb: u64, rtt_ms: u32, reputation: f32) -> f64 {
    let rate = rate_per_mb as f64;
    let rtt = f64::from(rtt_ms);
    let rep_clamped = reputation.max(REPUTATION_FLOOR);
    let rep = f64::from(rep_clamped);
    rate * rtt / (rep * rep)
}

/// Rank candidates by selection score, lowest (best) first.
///
/// Within-1%-score tie groups are reordered by the four-tier ADR 008
/// tie-breaker (load → geo → stake → random). The random tier uses a
/// fresh thread-local RNG; for deterministic tests use
/// [`rank_candidates_with_rng`].
pub fn rank_candidates(candidates: Vec<Candidate>) -> Vec<RankedCandidate> {
    let mut rng = rand::rng();
    rank_candidates_with_rng(candidates, &mut rng)
}

/// Variant of [`rank_candidates`] taking an explicit RNG. Internal — used by
/// tests for deterministic random tie-breaking.
fn rank_candidates_with_rng(
    candidates: Vec<Candidate>,
    rng: &mut impl rand::Rng,
) -> Vec<RankedCandidate> {
    let mut ranked: Vec<RankedCandidate> = candidates
        .into_iter()
        .map(|c| RankedCandidate {
            score: compute_score(c.rate_per_mb, c.rtt_ms, c.reputation),
            candidate: c,
        })
        .collect();
    ranked.sort_by(|a, b| a.score.total_cmp(&b.score));
    apply_tiebreaker(&mut ranked, rng);
    ranked
}

/// Walk the score-sorted slice and emit each within-1% tie group using the
/// ADR 008 §9 tie-break tiers. Geo diversity is stateful — once a region has
/// been emitted to the output, candidates in unseen regions are preferred for
/// the next pick within the current tie group.
fn apply_tiebreaker(ranked: &mut Vec<RankedCandidate>, rng: &mut impl rand::Rng) {
    let mut emitted_regions: HashSet<String> = HashSet::new();
    let mut output: Vec<RankedCandidate> = Vec::with_capacity(ranked.len());

    let mut start = 0;
    while start < ranked.len() {
        let end = tie_group_end(ranked, start);
        // Drain the tie group from the input slice. We re-pick into `output`
        // one at a time, refreshing `emitted_regions` between picks so geo
        // diversity reflects what has actually been emitted.
        let mut group: Vec<RankedCandidate> = ranked
            .get(start..end)
            .map_or_else(Vec::new, <[RankedCandidate]>::to_vec);
        while !group.is_empty() {
            let pick_idx = pick_best_in_group(&group, &emitted_regions, rng);
            // pick_best_in_group always returns a valid index when the slice
            // is non-empty; defensive default is index 0.
            let pick = if pick_idx < group.len() {
                group.remove(pick_idx)
            } else {
                group.remove(0)
            };
            emitted_regions.insert(pick.candidate.region.clone());
            output.push(pick);
        }
        start = end;
    }
    *ranked = output;
}

/// Return the index in `group` of the candidate that wins the tie under the
/// load → geo (relative to `emitted_regions`) → stake → random tiers.
fn pick_best_in_group(
    group: &[RankedCandidate],
    emitted_regions: &HashSet<String>,
    rng: &mut impl rand::Rng,
) -> usize {
    // Find candidates with the lowest load.
    let load_winner_load = group
        .iter()
        .map(|r| &r.candidate.load)
        .min_by(|a, b| compare_load(**a, **b));
    let load_winner_load = match load_winner_load {
        Some(l) => *l,
        None => return 0,
    };
    // Build the set of candidates tied at the lowest load.
    let load_tied: Vec<usize> = group
        .iter()
        .enumerate()
        .filter(|(_, r)| compare_load(r.candidate.load, load_winner_load).is_eq())
        .map(|(i, _)| i)
        .collect();

    // Tier 2: prefer regions not in `emitted_regions`. If any load-tied
    // candidate is in an unseen region, restrict to those.
    let geo_pool: Vec<usize> = load_tied
        .iter()
        .copied()
        .filter(|i| {
            group
                .get(*i)
                .is_some_and(|r| !emitted_regions.contains(&r.candidate.region))
        })
        .collect();
    let pool = if geo_pool.is_empty() {
        load_tied.clone()
    } else {
        geo_pool
    };

    // Tier 3: higher stake wins. `None` is treated as the lowest possible
    // stake (since on-chain integration is deferred — see ADR 023 wiring).
    let max_stake = pool
        .iter()
        .filter_map(|i| group.get(*i).and_then(|r| r.candidate.stake))
        .max();
    let stake_pool: Vec<usize> = match max_stake {
        Some(top) => pool
            .iter()
            .copied()
            .filter(|i| group.get(*i).and_then(|r| r.candidate.stake) == Some(top))
            .collect(),
        // No candidate in pool has a known stake → all are equal in this tier.
        None => pool.clone(),
    };
    let pool = if stake_pool.is_empty() {
        pool
    } else {
        stake_pool
    };

    // Tier 4: random tie-break. Uniformly pick from the remaining pool.
    if pool.is_empty() {
        0
    } else {
        let idx = rng.random_range(0..pool.len());
        // pool.get(idx) is always Some — random_range stays within 0..pool.len();
        // the unwrap_or(0) keeps us off the indexing_slicing-denied path without
        // using expect/unwrap.
        pool.get(idx).copied().unwrap_or(0)
    }
}

/// Return the exclusive end index of the tie group beginning at `start`.
/// Two adjacent candidates are in the same group iff their relative score
/// difference is ≤ [`TIE_THRESHOLD`].
fn tie_group_end(ranked: &[RankedCandidate], start: usize) -> usize {
    let pivot = match ranked.get(start) {
        Some(r) => r.score,
        None => return start,
    };
    let mut end = start + 1;
    while end < ranked.len() {
        let next = match ranked.get(end) {
            Some(r) => r.score,
            None => break,
        };
        let denom = pivot.min(next);
        if denom <= 0.0 || (next - pivot).abs() / denom > TIE_THRESHOLD {
            break;
        }
        end += 1;
    }
    end
}

/// Tier 1: lower load wins. Compare `bandwidth_utilization` first, then
/// `active_streams` to break sub-ties.
fn compare_load(a: LoadHint, b: LoadHint) -> core::cmp::Ordering {
    a.bandwidth_utilization
        .cmp(&b.bandwidth_utilization)
        .then(a.active_streams.cmp(&b.active_streams))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]
mod tests {
    use super::*;
    use rand::SeedableRng;

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
        // Defensive: ReputationEngine::score() returns f32 in [0,1], but if a
        // bug produces a negative we must still not panic and must clamp.
        let s = compute_score(100, 10, -0.5);
        let at_floor = compute_score(100, 10, 0.1);
        assert!((s - at_floor).abs() < 1e-9);
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
            load: LoadHint {
                active_streams: 0,
                bandwidth_utilization: 0,
            },
            region: "US".to_string(),
            stake: None,
        }
    }

    fn with_load(mut c: Candidate, active_streams: u32, util: u8) -> Candidate {
        c.load = LoadHint {
            active_streams,
            bandwidth_utilization: util,
        };
        c
    }

    fn with_region(mut c: Candidate, region: &str) -> Candidate {
        c.region = region.to_string();
        c
    }

    fn with_stake(mut c: Candidate, stake: Option<u64>) -> Candidate {
        c.stake = stake;
        c
    }

    #[test]
    fn higher_stake_wins_when_load_and_geo_tied() {
        // Same score, same load, same region. Higher stake wins.
        let small_stake = with_stake(
            with_region(with_load(make_candidate(1, 100, 10, 1.0), 0, 50), "US"),
            Some(1_000),
        );
        let big_stake = with_stake(
            with_region(with_load(make_candidate(2, 100, 10, 1.0), 0, 50), "US"),
            Some(10_000),
        );
        let out = rank_candidates(vec![small_stake, big_stake]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
    }

    #[test]
    fn some_stake_beats_none_stake() {
        let known = with_stake(
            with_region(with_load(make_candidate(1, 100, 10, 1.0), 0, 50), "US"),
            Some(1_000),
        );
        let unknown = with_stake(
            with_region(with_load(make_candidate(2, 100, 10, 1.0), 0, 50), "US"),
            None,
        );
        let out = rank_candidates(vec![unknown, known]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(1));
    }

    #[test]
    fn stake_tier_only_runs_when_load_and_geo_tied() {
        // Different load → stake doesn't matter, lower load wins.
        let busy_rich = with_stake(
            with_region(with_load(make_candidate(1, 100, 10, 1.0), 0, 90), "US"),
            Some(10_000),
        );
        let idle_poor = with_stake(
            with_region(with_load(make_candidate(2, 100, 10, 1.0), 0, 10), "US"),
            Some(1_000),
        );
        let out = rank_candidates(vec![busy_rich, idle_poor]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
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
    fn tied_scores_lower_utilization_wins() {
        // Both score exactly the same. busy has higher bandwidth_utilization.
        let busy = with_load(make_candidate(1, 100, 10, 1.0), 5, 90);
        let idle = with_load(make_candidate(2, 100, 10, 1.0), 5, 10);
        let out = rank_candidates(vec![busy, idle]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
    }

    #[test]
    fn tied_scores_break_streams_when_util_equal() {
        // Equal bandwidth_utilization → fall back to active_streams.
        let many = with_load(make_candidate(1, 100, 10, 1.0), 50, 50);
        let few = with_load(make_candidate(2, 100, 10, 1.0), 1, 50);
        let out = rank_candidates(vec![many, few]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
    }

    #[test]
    fn within_1_percent_counts_as_tied() {
        // score_a = 1000, score_b = 1005 → within 0.5%, should tie-break by load.
        let high_load = with_load(make_candidate(1, 100, 10, 1.0), 0, 90); // score 1000
        let low_load = with_load(make_candidate(2, 1005, 1, 1.0), 0, 10); // score 1005
        let out = rank_candidates(vec![high_load, low_load]);
        // Tied → lower-load (id 2) wins despite higher raw score.
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
    }

    #[test]
    fn outside_1_percent_does_not_tie_break() {
        // 1000 vs 1020 → 2% gap, no tie-break.
        let cheap = with_load(make_candidate(1, 100, 10, 1.0), 0, 90); // score 1000
        let dear = with_load(make_candidate(2, 1020, 1, 1.0), 0, 10); // score 1020
        let out = rank_candidates(vec![cheap, dear]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(1));
    }

    #[test]
    fn geo_diversity_prefers_unseen_region() {
        // Three candidates fully tied by score AND load; two in US, one in DE.
        // With tier-4 randomness the first pick may be any of the three, so the
        // test verifies the geo invariant directly: WHEN a US candidate is picked
        // first (the path where the geo tier matters), the second pick MUST be DE
        // — the only unseen region left in the tie group. Iterating seeds keeps
        // the test robust to RNG implementation changes.
        let us1 = with_region(with_load(make_candidate(1, 100, 10, 1.0), 0, 50), "US");
        let us2 = with_region(with_load(make_candidate(2, 100, 10, 1.0), 0, 50), "US");
        let de = with_region(with_load(make_candidate(3, 100, 10, 1.0), 0, 50), "DE");
        let mut tested_first_us = false;
        for seed in 0u64..32 {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let out =
                rank_candidates_with_rng(vec![us1.clone(), us2.clone(), de.clone()], &mut rng);
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
}
