# Node Selection Algorithm Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the unified node selection algorithm specified in ADR 001 — `score = rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)` — with the ADR 008 four-tier tie-breaker (load → geo → stake → random) and a `MAX_PROVIDER_ATTEMPTS` cap of 3.

**Architecture:** A pure-function library module at `crates/node/src/selection.rs`. It takes a `Vec<Candidate>` (the caller is responsible for fetching reputation from the reputation engine and load/region from the peer table) and returns a `Vec<RankedCandidate>` ordered best-first. No on-the-wire types are introduced; no caller integration is in scope (per ADR 001 the caller will live in the future cache-miss-pull path).

**Tech Stack:** Rust 2024 (MSRV 1.85), `decdn-protocol` for `LoadHint`, `rand` for the deterministic random tie-break (already a `decdn-node` dep), inline `#[cfg(test)]` tests using `cargo nextest`.

---

## Context

GitHub issue [#322](https://github.com/decdn/decdn/issues/322) is `poc-blocking`. ADR 001 §Node Selection Algorithm specifies a unified score that combines price (`rate_per_mb`), latency (`rtt_ms`), and reputation, with reputation squared in the denominator and clamped to a 0.1 floor so a reputation of 0.5 (the neutral default) costs 4× and a reputation of 0.1 costs 100× relative to a perfect 1.0. Tie-breaking (within 1% of best score) is specified in ADR 008 §9 as a four-tier rule: lower load → geographic diversity → higher stake → random.

The selection algorithm is the missing decision step between content discovery (probe fan-out / DHT — ADR 022) and the paid pull (`cdn/client/v1`). Today no peer-selection logic exists in the codebase (`crates/cache` has only origin pull-through; the probe handler only *serves* responses). This issue adds the pure ranking function and the data shape; the future cache-miss-pull path will wire it up.

The output is a ranked list — the caller iterates in order until a pull succeeds, capped at `MAX_PROVIDER_ATTEMPTS = 3` per the issue's "fallback" requirement. Stake is not present in `NodeAnnounce` (it lives on-chain in `StakingRegistry`); for PoC the caller passes `stake: None` and the stake tier is a no-op. Reputation is supplied per-candidate as an `f32` — the `ReputationEngine` trait (ADR 023 §2) is out of scope for this issue and will land in a follow-up.

## File Structure

| Path | Action | Responsibility |
|---|---|---|
| `crates/node/src/selection.rs` | **Create** | All selection types, score formula, ranking, tie-break, public helpers, inline tests |
| `crates/node/src/lib.rs` | **Modify** (line 7-14, add `pub mod selection;`) | Expose the new module |

Single file, ~250 lines, inline `#[cfg(test)]` tests. Splitting into submodules is premature for this scope.

**Reuse:**

- `decdn_protocol::gossip::LoadHint` (`crates/protocol/src/gossip.rs:96-103`) — already carries `active_streams: u32` and `bandwidth_utilization: u8`; do not redefine.
- `rand::SeedableRng` + `rand::rngs::StdRng` — already in workspace (`Cargo.toml:68`); use `StdRng::seed_from_u64` in tests for determinism, `StdRng::from_entropy` (or `thread_rng`) at the public entry point.

**Anti-panic discipline (workspace lints, `Cargo.toml:96-99`):** No `unwrap()`, `expect()`, `panic!()`, or raw indexing. Use `total_cmp` for `f64` ordering (returns `Ordering` directly with no `Option`), and `.get()` for any slice access.

---

## Task 1: Module skeleton + `Candidate` and `RankedCandidate` types

**Files:**

- Create: `crates/node/src/selection.rs`
- Modify: `crates/node/src/lib.rs` (add module declaration)

- [ ] **Step 1: Create `crates/node/src/selection.rs` with the type surface**

```rust
//! Provider selection algorithm (ADR 001 §Node Selection Algorithm + ADR 008 §9).
//!
//! `rank_candidates` returns the input list ordered best-first (lowest score
//! first) with the four-tier tie-breaker applied. The caller iterates the
//! result in order and stops after `MAX_PROVIDER_ATTEMPTS` failed providers.

use decdn_protocol::gossip::LoadHint;

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
    /// Iroh NodeId (Ed25519 public key) of the candidate.
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
```

- [ ] **Step 2: Wire the module into the node library**

Modify `crates/node/src/lib.rs`. Insert `pub mod selection;` in alphabetical order with the other `pub mod` lines (between `pub mod runtime;` and the closing of the module list — after the existing `pub mod runtime;`).

- [ ] **Step 3: Verify the crate still builds**

Run: `cargo build -p decdn-node`
Expected: clean build, no warnings.

- [ ] **Step 4: Verify clippy is happy with the skeleton**

Run: `cargo clippy -p decdn-node --all-targets -- -D warnings`
Expected: passes. (The constants are used in later tasks; if clippy flags `dead_code` on them now, leave it — it disappears once Task 2 lands. If the skeleton fails clippy outright, fix before committing.)

- [ ] **Step 5: Commit**

```bash
git add crates/node/src/selection.rs crates/node/src/lib.rs
git commit -m "feat(selection): add Candidate / RankedCandidate types + module skeleton

Skeleton for the node-selection algorithm specified in ADR 001 and
ADR 008. Issue #322."
```

---

## Task 2: `compute_score` with reputation-floor clamping

**Files:**

- Modify: `crates/node/src/selection.rs` (append function + tests)

- [ ] **Step 1: Append the failing tests**

Append to `crates/node/src/selection.rs`:

```rust
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]
mod tests {
    use super::*;

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
        assert!((s / baseline - 4.0).abs() < 1e-3, "got ratio {}", s / baseline);
    }

    #[test]
    fn score_reputation_0_1_is_100x_baseline() {
        let baseline = compute_score(100, 10, 1.0);
        let s = compute_score(100, 10, 0.1);
        assert!((s / baseline - 100.0).abs() < 1e-3, "got ratio {}", s / baseline);
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
}
```

- [ ] **Step 2: Run the tests; confirm they fail**

Run: `cargo nextest run -p decdn-node selection::tests::score_`
Expected: all six fail with `cannot find function 'compute_score' in this scope`.

- [ ] **Step 3: Implement `compute_score`**

Append to `crates/node/src/selection.rs` (above the `#[cfg(test)]` block):

```rust
/// Compute the unified selection score (ADR 001). Lower is better.
///
/// `score = rate_per_mb × rtt_ms × (1 / max(reputation, 0.1)²)`
///
/// Reputation is clamped to [`REPUTATION_FLOOR`] before squaring; this both
/// prevents division by zero and caps the worst-case multiplier at 100×.
#[allow(clippy::cast_precision_loss)] // f64 has 53-bit mantissa; ULP-level
                                       // imprecision on huge u64 rates does
                                       // not affect ordering decisions here.
fn compute_score(rate_per_mb: u64, rtt_ms: u32, reputation: f32) -> f64 {
    let rate = rate_per_mb as f64;
    let rtt = f64::from(rtt_ms);
    let rep = f64::from(reputation).max(REPUTATION_FLOOR);
    rate * rtt / (rep * rep)
}
```

- [ ] **Step 4: Re-run the tests; confirm they pass**

Run: `cargo nextest run -p decdn-node selection::tests::score_`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/node/src/selection.rs
git commit -m "feat(selection): compute_score with reputation floor clamp

Implements ADR 001's unified score formula. Reputation is clamped to a
0.1 floor so a node at 0.0 reputation scores identically to one at 0.1
(100x multiplier vs perfect reputation). Issue #322."
```

---

## Task 3: `rank_candidates` — basic ascending sort by score

**Files:**

- Modify: `crates/node/src/selection.rs`

- [ ] **Step 1: Append the failing tests**

Inside the existing `mod tests` block, append:

```rust
fn make_candidate(node_id: u8, rate: u64, rtt: u32, rep: f32) -> Candidate {
    Candidate {
        node_id: [node_id; 32],
        rate_per_mb: rate,
        rtt_ms: rtt,
        reputation: rep,
        load: LoadHint { active_streams: 0, bandwidth_utilization: 0 },
        region: "US".to_string(),
        stake: None,
    }
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
```

- [ ] **Step 2: Run the tests; confirm they fail**

Run: `cargo nextest run -p decdn-node selection::tests::rank_`
Expected: 3 failures — `cannot find function 'rank_candidates'`.

- [ ] **Step 3: Implement `rank_candidates`**

Append above the `#[cfg(test)]` block in `crates/node/src/selection.rs`:

```rust
/// Rank candidates by selection score, lowest (best) first.
///
/// Within-1%-score tie groups are reordered by the four-tier ADR 008
/// tie-breaker (load → geo → stake → random). The random tier uses a
/// fresh thread-local RNG; for deterministic tests use
/// [`rank_candidates_with_rng`].
pub fn rank_candidates(candidates: Vec<Candidate>) -> Vec<RankedCandidate> {
    let mut rng = rand::rngs::StdRng::from_entropy();
    rank_candidates_with_rng(candidates, &mut rng)
}

/// Variant of [`rank_candidates`] taking an explicit RNG. Internal — used by
/// tests for deterministic random tie-breaking.
fn rank_candidates_with_rng(
    candidates: Vec<Candidate>,
    _rng: &mut impl rand::Rng,
) -> Vec<RankedCandidate> {
    let mut ranked: Vec<RankedCandidate> = candidates
        .into_iter()
        .map(|c| RankedCandidate {
            score: compute_score(c.rate_per_mb, c.rtt_ms, c.reputation),
            candidate: c,
        })
        .collect();
    ranked.sort_by(|a, b| a.score.total_cmp(&b.score));
    ranked
}
```

Update the imports at the top of `selection.rs` to add `use rand::SeedableRng;` if needed for `from_entropy`. The tie-break logic lands in Tasks 4–7; the `_rng` parameter is unused for now (the underscore prefix suppresses the lint).

- [ ] **Step 4: Re-run the tests; confirm they pass**

Run: `cargo nextest run -p decdn-node selection::tests::rank_`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/node/src/selection.rs
git commit -m "feat(selection): rank_candidates basic score-ascending sort

No tie-breaking yet — that lands in tasks 4-7. Uses f64::total_cmp so
the comparator is total even though score values are always finite.
Issue #322."
```

---

## Task 4: Tie-break tier 1 — lower load wins (within 1% score)

**Files:**

- Modify: `crates/node/src/selection.rs`

- [ ] **Step 1: Append the failing tests**

Inside `mod tests`:

```rust
fn with_load(mut c: Candidate, active_streams: u32, util: u8) -> Candidate {
    c.load = LoadHint { active_streams, bandwidth_utilization: util };
    c
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
    let dear = with_load(make_candidate(2, 1020, 1, 1.0), 0, 10);  // score 1020
    let out = rank_candidates(vec![cheap, dear]);
    assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(1));
}
```

- [ ] **Step 2: Run; confirm `tied_scores_*` and `within_1_percent_*` fail**

Run: `cargo nextest run -p decdn-node selection::tests::tied_ selection::tests::within_ selection::tests::outside_`
Expected: `tied_scores_lower_utilization_wins`, `tied_scores_break_streams_when_util_equal`, `within_1_percent_counts_as_tied` fail; `outside_1_percent_does_not_tie_break` passes already (basic sort handles it).

- [ ] **Step 3: Implement tie-grouping + load tie-break**

Replace the body of `rank_candidates_with_rng` in `crates/node/src/selection.rs` with:

```rust
fn rank_candidates_with_rng(
    candidates: Vec<Candidate>,
    _rng: &mut impl rand::Rng,
) -> Vec<RankedCandidate> {
    let mut ranked: Vec<RankedCandidate> = candidates
        .into_iter()
        .map(|c| RankedCandidate {
            score: compute_score(c.rate_per_mb, c.rtt_ms, c.reputation),
            candidate: c,
        })
        .collect();
    ranked.sort_by(|a, b| a.score.total_cmp(&b.score));
    apply_tiebreaker(&mut ranked);
    ranked
}

/// Walk the score-sorted slice and re-order each within-1% tie group using
/// the load tie-break (Tier 1 of ADR 008 §9). Tiers 2–4 are layered on top
/// in subsequent tasks.
fn apply_tiebreaker(ranked: &mut [RankedCandidate]) {
    let mut start = 0;
    while start < ranked.len() {
        let end = tie_group_end(ranked, start);
        if let Some(group) = ranked.get_mut(start..end) {
            group.sort_by(|a, b| compare_load(&a.candidate.load, &b.candidate.load));
        }
        start = end;
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
fn compare_load(a: &LoadHint, b: &LoadHint) -> core::cmp::Ordering {
    a.bandwidth_utilization
        .cmp(&b.bandwidth_utilization)
        .then(a.active_streams.cmp(&b.active_streams))
}
```

- [ ] **Step 4: Re-run; confirm all `tied_/within_/outside_` tests pass**

Run: `cargo nextest run -p decdn-node selection`
Expected: all green (existing rank/score tests + the four new ones).

- [ ] **Step 5: Commit**

```bash
git add crates/node/src/selection.rs
git commit -m "feat(selection): tie-break tier 1 (lower load wins within 1% score)

Adds tie-group detection on the 1% score-equivalence threshold from
ADR 001 and orders tied candidates by LoadHint (bandwidth_utilization
first, then active_streams). Tiers 2-4 follow. Issue #322."
```

---

## Task 5: Tie-break tier 2 — geographic diversity

**Files:**

- Modify: `crates/node/src/selection.rs`

- [ ] **Step 1: Append the failing tests**

Inside `mod tests`:

```rust
fn with_region(mut c: Candidate, region: &str) -> Candidate {
    c.region = region.to_string();
    c
}

#[test]
fn geo_diversity_prefers_unseen_region() {
    // Three candidates, all tied by score AND load. Two are in US, one in DE.
    // Expected order: any one US, then DE, then the other US (geo prefers
    // unseen region for the second pick).
    let us1 = with_region(with_load(make_candidate(1, 100, 10, 1.0), 0, 50), "US");
    let us2 = with_region(with_load(make_candidate(2, 100, 10, 1.0), 0, 50), "US");
    let de = with_region(with_load(make_candidate(3, 100, 10, 1.0), 0, 50), "DE");
    let out = rank_candidates(vec![us1, us2, de]);
    let regions: Vec<String> = out.iter().map(|r| r.candidate.region.clone()).collect();
    assert_eq!(regions.first().map(String::as_str), Some("US"));
    assert_eq!(regions.get(1).map(String::as_str), Some("DE"));
    assert_eq!(regions.get(2).map(String::as_str), Some("US"));
}

#[test]
fn geo_diversity_only_applies_within_tie_group() {
    // Distinct scores → geo tier irrelevant, raw score order wins.
    let us = with_region(make_candidate(1, 1000, 10, 1.0), "US"); // score 10000
    let de = with_region(make_candidate(2, 100, 10, 1.0), "DE");  // score 1000
    let out = rank_candidates(vec![us, de]);
    assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
}
```

- [ ] **Step 2: Run; confirm `geo_diversity_prefers_unseen_region` fails**

Run: `cargo nextest run -p decdn-node selection::tests::geo_`
Expected: `geo_diversity_prefers_unseen_region` fails (current pure-load sort doesn't track regions); `geo_diversity_only_applies_within_tie_group` passes.

- [ ] **Step 3: Replace `apply_tiebreaker` with a stateful per-pick implementation**

In `crates/node/src/selection.rs`, replace the `apply_tiebreaker` function (and import `std::collections::HashSet` at the top of the file):

```rust
/// Walk the score-sorted slice and emit each within-1% tie group using the
/// ADR 008 §9 tie-break tiers. Geo diversity is stateful — once a region has
/// been emitted to the output, candidates in unseen regions are preferred for
/// the next pick within the current tie group.
fn apply_tiebreaker(ranked: &mut Vec<RankedCandidate>) {
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
            let pick_idx = pick_best_in_group(&group, &emitted_regions);
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
/// load → geo (relative to `emitted_regions`) tiers. (Stake + random tiers
/// land in tasks 6 and 7.)
fn pick_best_in_group(
    group: &[RankedCandidate],
    emitted_regions: &HashSet<String>,
) -> usize {
    // Find candidates with the lowest load.
    let load_winner_load = group
        .iter()
        .map(|r| &r.candidate.load)
        .min_by(|a, b| compare_load(a, b));
    let load_winner_load = match load_winner_load {
        Some(l) => *l,
        None => return 0,
    };
    // Build the set of candidates tied at the lowest load.
    let load_tied: Vec<usize> = group
        .iter()
        .enumerate()
        .filter(|(_, r)| compare_load(&r.candidate.load, &load_winner_load).is_eq())
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
    let pool = if geo_pool.is_empty() { &load_tied } else { &geo_pool };

    // Defensive: pool is non-empty if group is non-empty (load_tied always
    // contains at least the candidate that supplied load_winner_load).
    pool.first().copied().unwrap_or(0)
}
```

Add at the top of the file (with the existing `use` lines):

```rust
use std::collections::HashSet;
```

- [ ] **Step 4: Re-run; confirm all geo tests pass and existing tests still pass**

Run: `cargo nextest run -p decdn-node selection`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add crates/node/src/selection.rs
git commit -m "feat(selection): tie-break tier 2 (geographic diversity)

Refactors apply_tiebreaker into a stateful per-pick loop that prefers
candidates in regions not yet emitted. Tier-1 load winners across
multiple regions are now distributed before duplicates of the same
region. Issue #322."
```

---

## Task 6: Tie-break tier 3 — higher stake wins (`Option<u64>`)

**Files:**

- Modify: `crates/node/src/selection.rs`

- [ ] **Step 1: Append the failing tests**

Inside `mod tests`:

```rust
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
```

- [ ] **Step 2: Run; confirm `higher_stake_*` and `some_stake_*` fail**

Run: `cargo nextest run -p decdn-node selection::tests::higher_stake selection::tests::some_stake selection::tests::stake_tier`
Expected: first two fail; third passes (already covered by load tier).

- [ ] **Step 3: Extend `pick_best_in_group` with the stake tier**

Replace the tail of `pick_best_in_group` (everything after the geo-pool computation) with:

```rust
    let pool = if geo_pool.is_empty() { &load_tied } else { &geo_pool };

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
            .filter(|i| {
                group
                    .get(*i)
                    .and_then(|r| r.candidate.stake)
                    == Some(top)
            })
            .collect(),
        // No candidate in pool has a known stake → all are equal in this tier.
        None => pool.clone(),
    };
    let pool = if stake_pool.is_empty() { pool } else { &stake_pool };

    pool.first().copied().unwrap_or(0)
```

(The previous `pool.first().copied().unwrap_or(0)` line is replaced by the block above ending in the same expression. The variable shadowing is intentional and matches Rust idiom.)

- [ ] **Step 4: Re-run; confirm all stake tests pass and existing tests still pass**

Run: `cargo nextest run -p decdn-node selection`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add crates/node/src/selection.rs
git commit -m "feat(selection): tie-break tier 3 (higher stake wins)

Stake is Option<u64> because on-chain stake lookup is deferred (not in
NodeAnnounce). When all pool candidates have stake = None the tier is
a no-op and the next tier decides. Issue #322."
```

---

## Task 7: Tie-break tier 4 — seeded random final tiebreaker

**Files:**

- Modify: `crates/node/src/selection.rs`

- [ ] **Step 1: Append the failing tests**

Inside `mod tests` (the `unwrap_used` allow on the test module covers `SeedableRng` ergonomics):

```rust
use rand::SeedableRng;

#[test]
fn random_breaks_full_ties_deterministically_with_seed() {
    // Two perfectly identical candidates (modulo node_id). With a fixed seed
    // the same RNG output is reproduced every run.
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
    // Run with many seeds and ensure both possible orderings are seen at
    // least once — guards against accidentally biasing the selector.
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
```

Note: `rank_candidates_with_rng` is currently private. Make it `pub(crate)` so the test module (a child of `selection`) can see it. It's already in scope via `use super::*;`; no visibility change needed if the test stays in the same module. Confirm by reading `mod tests { use super::*; ... }`.

- [ ] **Step 2: Run; confirm both fail**

Run: `cargo nextest run -p decdn-node selection::tests::random_`
Expected: `random_breaks_full_ties_deterministically_with_seed` either passes by accident (current code is deterministic) or fails (depends on hash iteration order); `random_tier_yields_different_orders_for_different_seeds` fails because the current `pool.first()` always returns the same index.

- [ ] **Step 3: Plumb the RNG through `apply_tiebreaker` and `pick_best_in_group`**

In `crates/node/src/selection.rs`:

1. Remove the underscore from `_rng` in `rank_candidates_with_rng` and pass it to `apply_tiebreaker`:

```rust
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
```

1. Update `apply_tiebreaker` signature and pass `rng` to `pick_best_in_group`:

```rust
fn apply_tiebreaker(ranked: &mut Vec<RankedCandidate>, rng: &mut impl rand::Rng) {
    // ... existing body, but the inner `pick_best_in_group(&group, &emitted_regions)`
    //     call becomes `pick_best_in_group(&group, &emitted_regions, rng)`.
}
```

1. Replace the tail of `pick_best_in_group` to add the random tier:

```rust
fn pick_best_in_group(
    group: &[RankedCandidate],
    emitted_regions: &HashSet<String>,
    rng: &mut impl rand::Rng,
) -> usize {
    // ... existing tiers 1-3 unchanged, ending with `let pool = ...` ...

    // Tier 4: random uniform pick from `pool`.
    if pool.is_empty() {
        0
    } else {
        let idx = rng.gen_range(0..pool.len());
        pool.get(idx).copied().unwrap_or(0)
    }
}
```

Adjust the `pool` shadowing chain so the final `pool` at the random tier holds the stake-narrowed slice. The full function body is approximately:

```rust
fn pick_best_in_group(
    group: &[RankedCandidate],
    emitted_regions: &HashSet<String>,
    rng: &mut impl rand::Rng,
) -> usize {
    let load_winner_load = group
        .iter()
        .map(|r| &r.candidate.load)
        .min_by(|a, b| compare_load(a, b));
    let load_winner_load = match load_winner_load {
        Some(l) => *l,
        None => return 0,
    };
    let load_tied: Vec<usize> = group
        .iter()
        .enumerate()
        .filter(|(_, r)| compare_load(&r.candidate.load, &load_winner_load).is_eq())
        .map(|(i, _)| i)
        .collect();

    let geo_pool: Vec<usize> = load_tied
        .iter()
        .copied()
        .filter(|i| {
            group
                .get(*i)
                .is_some_and(|r| !emitted_regions.contains(&r.candidate.region))
        })
        .collect();
    let pool = if geo_pool.is_empty() { load_tied.clone() } else { geo_pool };

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
        None => pool.clone(),
    };
    let pool = if stake_pool.is_empty() { pool } else { stake_pool };

    if pool.is_empty() {
        0
    } else {
        let idx = rng.gen_range(0..pool.len());
        pool.get(idx).copied().unwrap_or(0)
    }
}
```

(The earlier `&load_tied` / `&geo_pool` references become owned `Vec<usize>` to avoid borrow-checker friction with the chained shadowing.)

- [ ] **Step 4: Re-run; confirm random tests pass and previous tests still pass**

Run: `cargo nextest run -p decdn-node selection`
Expected: all green. The `geo_diversity_prefers_unseen_region` test from Task 5 is now non-deterministic in the *first* US pick, but the assertion only checks the regions in order (US, DE, US) — so both possible US picks for slot 0 satisfy it.

- [ ] **Step 5: Commit**

```bash
git add crates/node/src/selection.rs
git commit -m "feat(selection): tie-break tier 4 (random) and complete ADR 008 §9

When all four tiers tie, picks uniformly at random from the remaining
pool. The internal rank_candidates_with_rng entry point keeps tests
deterministic via a seeded StdRng. Issue #322."
```

---

## Task 8: `top_n` helper + `MAX_PROVIDER_ATTEMPTS` integration

**Files:**

- Modify: `crates/node/src/selection.rs`

- [ ] **Step 1: Append the failing tests**

Inside `mod tests`:

```rust
#[test]
fn top_n_returns_at_most_n() {
    let cs: Vec<Candidate> = (0..5).map(|i| make_candidate(i, 100 + u64::from(i), 10, 1.0)).collect();
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
```

- [ ] **Step 2: Run; confirm they fail**

Run: `cargo nextest run -p decdn-node selection::tests::top_n_`
Expected: 3 failures — `cannot find function 'top_n'`.

- [ ] **Step 3: Implement `top_n`**

Append to `crates/node/src/selection.rs` (above the `#[cfg(test)]` block):

```rust
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
```

- [ ] **Step 4: Re-run; confirm all top_n tests pass**

Run: `cargo nextest run -p decdn-node selection`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add crates/node/src/selection.rs
git commit -m "feat(selection): add top_n helper and MAX_PROVIDER_ATTEMPTS = 3

top_n(candidates, MAX_PROVIDER_ATTEMPTS) is the canonical entry point
for the future cache-miss-pull caller — issue #322's 'max 3 provider
attempts before returning error to caller' requirement."
```

---

## Task 9: Final verification — clippy, formatting, full nextest

**Files:** none modified.

- [ ] **Step 1: Run rustfmt check**

Run: `cargo fmt --check`
Expected: no diff. If diff appears, run `cargo fmt` and amend the previous commit with `git commit -a --amend --no-edit`.

- [ ] **Step 2: Run clippy with workspace lints across all targets**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean. The anti-panic clippy denies (unwrap_used / expect_used / panic / indexing_slicing) are the most likely failure surface — the only `unwrap_used` allow in the implementation should be inside the `#[cfg(test)]` module's `#[allow(...)]`, none in production code.

- [ ] **Step 3: Run full workspace nextest**

Run: `cargo nextest run`
Expected: all crates green, including the new selection tests in `decdn-node`.

- [ ] **Step 4: Run cargo deny (license + advisory audit)**

Run: `cargo deny check`
Expected: pass. No new dependencies were added (we use `rand`, `decdn-protocol`, and `std`); this should not regress.

- [ ] **Step 5: Final commit if formatting was adjusted, otherwise no-op**

If steps 1–4 made no changes, this task ends with no new commit — it is a verification-only task.

---

## Verification (end-to-end)

Run from the repo root after Task 9:

```bash
cargo build && cargo clippy --all-targets -- -D warnings && cargo fmt --check && cargo nextest run
```

All four must pass. Manual sanity check (optional, REPL-style):

```bash
cargo nextest run -p decdn-node selection -- --no-capture
```

The `selection::tests::*` block should report ≥ 19 tests (6 score + 3 rank + 4 tie-1 + 2 tie-2 + 3 tie-3 + 2 tie-4 + 3 top_n).

There is no integration test or runtime path to exercise: per the issue, "Integration: called after content discovery returns candidate nodes" — content discovery does not yet have a caller wired into the cache miss path. This issue lands the ranking library; the caller is the subject of a follow-up issue (likely related to `cdn/dht/v1` and `cdn/client/v1` handler work).

## Out of scope (intentional)

- **`ReputationEngine` trait** (ADR 023 §2) — selection takes `reputation: f32` per `Candidate`; the trait will be introduced when the reputation EWMA implementation lands. The selection module is forward-compatible: a future caller obtains `f32` from `engine.score(node_id)` and writes it into `Candidate`.
- **On-chain stake lookup** — `Candidate.stake: Option<u64>` is `None` for PoC. When the on-chain registry integration provides stake, callers populate this field and the existing tie-break code starts using it without further changes.
- **Caller integration in cache-miss / DHT pull path** — no such path exists today. This module exposes `pub` types and `top_n` ready for that work.
