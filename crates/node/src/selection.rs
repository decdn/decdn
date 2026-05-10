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
const REPUTATION_FLOOR: f64 = 0.1;

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
    let rep = f64::from(reputation).max(REPUTATION_FLOOR);
    rate * rtt / (rep * rep)
}

/// Rank candidates by selection score, lowest (best) first.
///
/// Within-1%-score tie groups are reordered by the four-tier ADR 008
/// tie-breaker (load → geo → stake → random). The random tier uses a
/// fresh thread-local RNG; tests inside this module use the private
/// `rank_candidates_with_rng` variant for determinism.
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
/// ADR 008 §9 tie-break tiers. Geo diversity is scoped to the current tie
/// group: candidates within a single within-1% group are spread across
/// regions, but the tracker is reset between groups so unrelated tie groups
/// don't bias each other's geo tier.
fn apply_tiebreaker(ranked: &mut Vec<RankedCandidate>, rng: &mut impl rand::Rng) {
    let mut output: Vec<RankedCandidate> = Vec::with_capacity(ranked.len());

    while !ranked.is_empty() {
        // Geo diversity is scoped to the current tie group: candidates within
        // a single within-1% group are spread across regions, but the tracker
        // is reset between groups so unrelated tie groups don't bias each
        // other's geo tier. ADR 008 §9 lists the four tiers; per-group scoping
        // is this implementation's interpretation of "within a tie".
        let mut group_regions: HashSet<String> = HashSet::new();
        let end = tie_group_end(ranked, 0);
        // tie_group_end's loop guard bounds `end` at `ranked.len()`, so the
        // min() is belt-and-suspenders to keep `Vec::drain` from panicking
        // even on a future invariant break.
        let split_at = end.min(ranked.len());
        debug_assert_eq!(
            end,
            split_at,
            "tie_group_end exceeded len: {end} > {}",
            ranked.len()
        );
        // Drain (move) the front tie group out of `ranked` into `group` —
        // avoids the per-element clone of `to_vec()` on the slice.
        let mut group: Vec<RankedCandidate> = ranked.drain(..split_at).collect();
        while !group.is_empty() {
            let pick_idx = pick_best_in_group(&group, &group_regions, rng);
            // pick_best_in_group always returns a valid index when the slice
            // is non-empty (loop guard above ensures non-emptiness). The safe
            // fallback exists only to satisfy clippy::indexing_slicing; the
            // debug_assert surfaces any future invariant break in tests. Order
            // in `group` does not matter — pick_best_in_group rescans from
            // scratch each iteration — so swap_remove is safe and O(1).
            debug_assert!(
                pick_idx < group.len(),
                "pick_best_in_group returned {pick_idx} for group of len {}",
                group.len()
            );
            let pick = if pick_idx < group.len() {
                group.swap_remove(pick_idx)
            } else {
                group.swap_remove(0)
            };
            group_regions.insert(pick.candidate.region.clone());
            output.push(pick);
        }
    }
    *ranked = output;
}

/// Return the index in `group` of the candidate that wins the tie under the
/// load → geo (relative to `emitted_regions`) → stake → random tiers.
///
/// Filters a single index pool in place across the four tiers, so the function
/// allocates exactly one Vec per call regardless of group size or tier depth.
///
/// **INVARIANT:** the returned index is independent of element order in
/// `group`. The caller (`apply_tiebreaker`) relies on this to use `swap_remove`
/// for O(1) removal, which reorders surviving elements. Any future
/// optimization that reuses state across calls must preserve this property.
fn pick_best_in_group(
    group: &[RankedCandidate],
    emitted_regions: &HashSet<String>,
    rng: &mut impl rand::Rng,
) -> usize {
    debug_assert!(
        !group.is_empty(),
        "pick_best_in_group called with empty group"
    );
    if group.is_empty() {
        return 0;
    }
    // `pool` is constructed as `0..group.len()` and only ever shrunk via
    // `retain`, so every `usize` it holds is a valid `group` index. This
    // makes the `group.get(*i)` calls below provably `Some` and the
    // final `unwrap_or(0)` provably unreachable in release builds.
    let mut pool: Vec<usize> = (0..group.len()).collect();

    // Tier 1: lowest load wins.
    if let Some(min_load) = pool
        .iter()
        .filter_map(|i| group.get(*i).map(|r| r.candidate.load))
        .min_by(|a, b| compare_load(*a, *b))
    {
        pool.retain(|i| {
            group
                .get(*i)
                .is_some_and(|r| compare_load(r.candidate.load, min_load).is_eq())
        });
    }

    // Tier 2: prefer regions not in `emitted_regions`. If at least one
    // candidate in the pool is in an unseen region, restrict to those.
    let any_unseen = pool.iter().any(|i| {
        group
            .get(*i)
            .is_some_and(|r| !emitted_regions.contains(&r.candidate.region))
    });
    if any_unseen {
        pool.retain(|i| {
            group
                .get(*i)
                .is_some_and(|r| !emitted_regions.contains(&r.candidate.region))
        });
    }

    // Tier 3: higher stake wins. `None` is treated as the lowest possible
    // stake (since on-chain integration is deferred — see ADR 001 "Contract
    // Interface: Node Registry" / ADR 019 for the staking-registry interface
    // that will populate `Candidate.stake`).
    let max_stake = pool
        .iter()
        .filter_map(|i| group.get(*i).and_then(|r| r.candidate.stake))
        .max();
    if let Some(top) = max_stake {
        pool.retain(|i| group.get(*i).and_then(|r| r.candidate.stake) == Some(top));
    }

    // Tier 4: random tie-break. Uniformly pick from the remaining pool.
    if pool.is_empty() {
        0
    } else {
        let idx = rng.random_range(0..pool.len());
        // pool.get(idx) is always Some — random_range stays within 0..pool.len().
        // The unwrap_or(0) keeps us off the indexing_slicing-denied path without
        // using expect/unwrap; debug_assert surfaces any invariant break in tests.
        let picked = pool.get(idx).copied();
        debug_assert!(
            picked.is_some(),
            "random_range({}) returned out-of-bounds idx {idx}",
            pool.len()
        );
        picked.unwrap_or(0)
    }
}

/// Return the exclusive end index of the tie group beginning at `start`.
/// Two adjacent candidates are in the same group iff their relative score
/// difference is ≤ [`TIE_THRESHOLD`]. Two candidates with score `0.0` are
/// always grouped together; a zero-score candidate is strictly better than
/// any positive-score candidate and ends the group.
///
/// **Termination contract:** returns `> start` whenever `start < ranked.len()`,
/// so the caller's drain-the-front loop in [`apply_tiebreaker`] always makes
/// progress on a non-empty `ranked`.
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
        // Sort is ascending, so next >= pivot. Equal scores stay in the
        // group; otherwise the relative diff against the (positive) pivot
        // decides. A pivot of 0.0 with a strictly larger next ends the
        // group — zero is strictly better than any positive score.
        if next > pivot && (pivot == 0.0 || (next - pivot) / pivot > TIE_THRESHOLD) {
            break;
        }
        end += 1;
    }
    end
}

/// Tier 1: lower load wins. Compare `bandwidth_utilization` first, then
/// `active_streams` to break sub-ties.
///
/// Bandwidth utilization is the primary signal because it directly reflects
/// how saturated a node's outgoing pipe is; `active_streams` is a coarser
/// proxy (a node serving many small streams may have low utilization, while
/// one serving a few large ones may be saturated). ADR 001 / ADR 008 §9 list
/// "lower load" without prescribing the sub-field order — this is this
/// implementation's interpretation.
fn compare_load(a: LoadHint, b: LoadHint) -> core::cmp::Ordering {
    a.bandwidth_utilization
        .cmp(&b.bandwidth_utilization)
        .then(a.active_streams.cmp(&b.active_streams))
}

/// Convenience wrapper around [`rank_candidates`]: returns the top `n`
/// ranked candidates, where the caller will iterate them in order and stop
/// after the first successful pull.
///
/// For the standard fetch path use `n = MAX_PROVIDER_ATTEMPTS`. The function
/// returns fewer than `n` results when the candidate pool is smaller.
pub fn top_n(candidates: Vec<Candidate>, n: usize) -> Vec<RankedCandidate> {
    let mut ranked = rank_candidates(candidates);
    ranked.truncate(n);
    ranked
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
        // Any sub-floor reputation (0.0, 0.01, etc.) must clamp to
        // REPUTATION_FLOOR (0.1f64) and produce identical scores. We compare
        // two sub-floor inputs rather than one sub-floor and 0.1 directly,
        // because 0.1f32 promotes to ~0.10000000149f64 — slightly above the
        // f64 floor — and would skip clamping.
        let at_zero = compute_score(100, 10, 0.0);
        let at_below_floor = compute_score(100, 10, 0.01);
        assert!((at_zero - at_below_floor).abs() < 1e-9);
    }

    #[test]
    fn score_negative_reputation_clamps_to_floor() {
        // Defensive: ReputationEngine::score() returns f32 in [0,1], but if a
        // bug produces a negative we must still not panic and must clamp.
        let s = compute_score(100, 10, -0.5);
        let at_below_floor = compute_score(100, 10, 0.01);
        assert!((s - at_below_floor).abs() < 1e-9);
    }

    #[test]
    fn score_floor_value_is_zero_point_one() {
        // Pin REPUTATION_FLOOR's actual value (not just clamping behavior). At
        // rate=100, rtt=10, rep clamps to 0.1: score = 1000 / 0.01 = 100_000.
        // Breaks if the floor drifts off 0.1 (e.g., to 0.05 → 400_000).
        let at_floor = compute_score(100, 10, 0.0);
        assert!((at_floor - 100_000.0).abs() < 1e-3, "got {at_floor}");
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
    fn some_zero_stake_beats_none_stake() {
        // Zero-stake operators are still "known" — `Some(0)` should beat `None`
        // (treated as "not yet looked up") at tier 3. `max_stake` returns
        // `Some(0)` for the pool, then retain keeps only `stake == Some(0)`,
        // which drops the `None` entry.
        let known_zero = with_stake(
            with_region(with_load(make_candidate(1, 100, 10, 1.0), 0, 50), "US"),
            Some(0),
        );
        let unknown = with_stake(
            with_region(with_load(make_candidate(2, 100, 10, 1.0), 0, 50), "US"),
            None,
        );
        let out = rank_candidates(vec![unknown, known_zero]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(1));
    }

    #[test]
    fn three_way_zero_stake_falls_through_to_random_tier() {
        // Two `Some(0)` candidates plus one `None`: tier 3 retains both
        // `Some(0)` entries (dropping `None`), then tier 4 picks randomly
        // between the two survivors. Across enough seeds we should see both
        // survivors win first — confirms tier 3 doesn't accidentally
        // short-circuit on a single Some(0) winner.
        let a = with_stake(
            with_region(with_load(make_candidate(1, 100, 10, 1.0), 0, 50), "US"),
            Some(0),
        );
        let b = with_stake(
            with_region(with_load(make_candidate(2, 100, 10, 1.0), 0, 50), "US"),
            Some(0),
        );
        let unknown = with_stake(
            with_region(with_load(make_candidate(3, 100, 10, 1.0), 0, 50), "US"),
            None,
        );
        let mut first_was_a = false;
        let mut first_was_b = false;
        for seed in 0u64..32 {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let out =
                rank_candidates_with_rng(vec![a.clone(), b.clone(), unknown.clone()], &mut rng);
            // The unknown (None stake) must never win first — tier 3 drops it.
            assert_ne!(
                out.first().map(|r| r.candidate.node_id[0]),
                Some(3),
                "seed {seed}: None-stake candidate should never beat Some(0)"
            );
            match out.first().map(|r| r.candidate.node_id[0]) {
                Some(1) => first_was_a = true,
                Some(2) => first_was_b = true,
                _ => {}
            }
            if first_was_a && first_was_b {
                break;
            }
        }
        assert!(
            first_was_a && first_was_b,
            "expected both Some(0) candidates to win across 32 seeds"
        );
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
    fn at_1_percent_boundary_counts_as_tied() {
        // 1000 vs 1010 → exactly 1.0% gap. tie_group_end uses `> TIE_THRESHOLD`,
        // so 1.0% is *inclusive* (still a tie). Pins the boundary against a
        // future change to `>=` that would silently exclude exact-1% pairs.
        let high_load = with_load(make_candidate(1, 100, 10, 1.0), 0, 90); // score 1000
        let low_load = with_load(make_candidate(2, 1010, 1, 1.0), 0, 10); // score 1010
        let out = rank_candidates(vec![high_load, low_load]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
    }

    #[test]
    fn zero_score_candidates_group_together_for_tie_break() {
        // Two zero-score candidates (rate=0, both have score 0.0) must form a
        // single tie group so the load tier picks between them. Without the
        // zero-pivot fix, each becomes its own singleton group and load is
        // skipped entirely.
        let zero_high_load = with_load(make_candidate(1, 0, 10, 1.0), 0, 90); // score 0, high load
        let zero_low_load = with_load(make_candidate(2, 0, 10, 1.0), 0, 10); // score 0, low load
        let out = rank_candidates(vec![zero_high_load, zero_low_load]);
        // Tied at 0.0 → lower-load (id 2) wins.
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

    #[test]
    fn top_n_returns_at_most_n() {
        let cs: Vec<Candidate> = (0..5)
            .map(|i| make_candidate(i, 100 + u64::from(i), 10, 1.0))
            .collect();
        let out = top_n(cs, MAX_PROVIDER_ATTEMPTS);
        assert_eq!(out.len(), MAX_PROVIDER_ATTEMPTS);
    }

    #[test]
    fn top_n_caps_at_input_length_when_smaller() {
        let cs = vec![make_candidate(1, 100, 10, 1.0)];
        let out = top_n(cs, MAX_PROVIDER_ATTEMPTS);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn top_n_returns_lowest_score_first() {
        let cheap = make_candidate(1, 1, 10, 1.0); // score 10
        let mid = make_candidate(2, 10, 10, 1.0); // score 100
        let dear = make_candidate(3, 100, 10, 1.0); // score 1000
        let out = top_n(vec![dear, cheap, mid], 2);
        let ids: Vec<u8> = out.iter().map(|r| r.candidate.node_id[0]).collect();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn top_n_with_zero_returns_empty() {
        // Boundary: n = 0 produces an empty result. Locks the contract against
        // a future change like `truncate(n.max(1))`.
        let cs: Vec<Candidate> = (0..3).map(|i| make_candidate(i, 100, 10, 1.0)).collect();
        let out = top_n(cs, 0);
        assert!(out.is_empty());
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
        let g1_us = with_region(with_load(make_candidate(1, 1, 10, 1.0), 0, 50), "US");
        let g1_de = with_region(with_load(make_candidate(2, 1, 10, 1.0), 0, 50), "DE");
        let g2_us = with_region(with_load(make_candidate(3, 100, 10, 1.0), 0, 50), "US");
        let g2_jp = with_region(with_load(make_candidate(4, 100, 10, 1.0), 0, 50), "JP");

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
}
