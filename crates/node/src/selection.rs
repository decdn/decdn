//! Provider selection algorithm (ADR 001 § Node Selection Algorithm + ADR 008
//! § Tie-Breaking).
//!
//! `rank_candidates` returns the input list ordered best-first (lowest score
//! first) with the three-tier tie-breaker applied. The caller iterates the
//! result in order and stops after `MAX_PROVIDER_ATTEMPTS` failed providers.

use rand::RngExt;
use std::collections::HashSet;
use std::time::Duration;

use crate::dht::lookup::{DEFAULT_ROUND_TIMEOUT, MAX_LOOKUP_ROUNDS};

/// Maximum providers to attempt before reporting a fetch failure to the
/// caller (issue #322 — "max 3 provider attempts before returning error").
pub const MAX_PROVIDER_ATTEMPTS: usize = 3;

/// Per-candidate probe timeout. Because candidates are probed concurrently, this also
/// bounds the whole probe-collection phase. The budget covers connection setup plus one
/// unpaid probe request/response; warm or 0-RTT connections (ADR 015) complete in a single
/// round trip, so the 500 ms ceiling accommodates inter-continental RTTs while still
/// dropping a slow or unreachable candidate before it burns the caller's miss-latency
/// budget (ADR 001 § Probe response collection).
///
/// Lives here, beside the deadline arithmetic that has to budget for it, rather than in
/// `node_origin` where it is used (#1145 review). Every term of [`outer_pull_deadline`] is
/// then visible in one file, which is what stops the next one from being guessed.
pub const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// One-time headroom added on top of the `MAX_PROVIDER_ATTEMPTS` sequential per-candidate
/// costs when computing the outer pull-through deadline (#859). It covers the *one-time*
/// `discover → probe → rank` overhead: work that runs under the outer deadline but does not
/// scale with the attempt count.
///
/// **Derived, not chosen** (#1145 review). This used to be a flat 10 s described as "a
/// tunable judgement value… small relative to one per-candidate budget", and it was smaller
/// than the thing it was named for:
///
/// - probing is concurrent, so it costs one [`PROBE_TIMEOUT`] — 500 ms; but
/// - discovery is `find_providers`, whose rounds are each bounded by
///   [`DEFAULT_ROUND_TIMEOUT`] (8 s) and whose round count is capped by
///   [`MAX_LOOKUP_ROUNDS`] (4).
///
/// At those defaults, four discovery rounds plus the probe phase can cost 32.5 s against
/// the old 10 s budget. The outer deadline was therefore short of what the fetch could
/// actually spend, and the `tokio::time::timeout` around
/// `discover → probe → rank → pull` could fire while candidate #3 was still in its stall
/// window — the #859 fallback starvation this whole formula exists to prevent, reachable at
/// the defaults.
///
/// The fix is in two halves, and both are necessary: [`MAX_LOOKUP_ROUNDS`] makes discovery's
/// worst case finite, and this makes it a TERM. An unbounded cost cannot be budgeted for by
/// any constant, however generous.
pub const PULL_THROUGH_OUTER_SLACK: Duration =
    PROBE_TIMEOUT.saturating_add(DEFAULT_ROUND_TIMEOUT.saturating_mul(MAX_LOOKUP_ROUNDS));

/// How long a pull is willing to WAIT on a buyer-channel open before giving up on
/// that candidate — not how long the open itself is allowed to take (#1143).
///
/// Since #1143 the `openChannel` runs in a detached task that owns the tx, so a
/// caller that stops waiting costs nothing: the open continues, the channel lands,
/// and the next pull to that provider reuses it. What the caller buys by waiting is
/// only the chance to use the channel on *this* pull. That makes a short budget the
/// right trade — a cache miss must fall through to another candidate in seconds,
/// while an `openChannel` may legitimately need minutes to mine on a slow L2.
///
/// Deliberately much smaller than `cache.node_pull_timeout_sec`: the channel open, the
/// stream open, and the streaming stage are SEQUENTIAL stages of one candidate attempt,
/// and [`outer_pull_deadline`] has to cover all three for every candidate.
pub const CHANNEL_OPEN_CALLER_BUDGET: Duration = Duration::from_secs(5);

/// Outer deadline for a node-to-node pull-through, derived from the two configured
/// per-candidate budgets: the stream-open timeout (`cache.node_pull_timeout_sec`) and
/// the streaming inactivity timeout (`cache.node_pull_stall_timeout_sec`).
///
/// The delivery handler wraps the whole `discover → probe → rank → pull` fetch
/// in a single `tokio::time::timeout`. For the sequential `MAX_PROVIDER_ATTEMPTS`
/// fallback loop to actually reach candidates #2..N when candidate #1 *stalls*,
/// this outer deadline must strictly exceed the sum of all per-candidate costs —
/// otherwise both clocks (sourced from the same config value before #859) expire
/// together and the outer timeout cancels the whole fetch at the instant candidate
/// #1's own timeout fires, killing the fallback.
///
/// # What one candidate actually costs
///
/// THREE sequential bounded stages:
///
/// 1. the **channel open** — [`CHANNEL_OPEN_CALLER_BUDGET`] (#1143); then
/// 2. the **stream open** — connect → handshake → verified `StreamResponse`,
///    bounded by `per_candidate` (#1134); then
/// 3. **streaming**, which for a peer that opens honestly and then goes SILENT costs
///    one full inactivity window (`stall`) before the pull gives up on it (#1134).
///
/// So the worst case for a candidate is `CHANNEL_OPEN_CALLER_BUDGET + per_candidate + stall`,
/// and that is what this must budget `MAX_PROVIDER_ATTEMPTS` of, plus
/// [`PULL_THROUGH_OUTER_SLACK`] of one-time discovery overhead.
///
/// Every stage has to be a term here, and each one was learned the same way. Budgeting
/// only `per_candidate` let a cold cache against a slow L2 burn the whole deadline on
/// candidates #1 and #2 and never dial #3. Adding the channel open but not the stall
/// window left the same hole for a peer that goes silent mid-stream instead of failing
/// to open — and made it *worse*, because `stall` is operator-tunable: at
/// `node_pull_stall_timeout_sec = 120` a single silent candidate outlasts the whole
/// two-term deadline on its own. Taking `stall` as an argument is what keeps the two
/// from drifting apart again.
///
/// # What this deadline does not bound
///
/// A candidate's streaming stage is bounded by *inactivity* rather than a wall clock,
/// because a wall clock over the bytes caps the blob size a node can pull through
/// (#1134). The `stall` term above is therefore the cost of a candidate that **stops**
/// — not a ceiling on one that keeps going. A candidate that is **slow but progressing**
/// resets that clock on every byte and can consume the whole outer deadline on its own.
///
/// That is correct: it is succeeding, and falling through mid-stream would restart the
/// download from zero against another peer. The foreground request gives up (a clean miss)
/// while a detached background warm RE-PULLS the blob from scratch under a far larger
/// backstop (`BACKGROUND_FILL_HARD_CAP`), for exactly this reason.
///
/// "Re-pulls from scratch", not "the transfer continues" (#1145 review): the warm calls
/// `cache.populate(hash)`, which starts a new pull. Nothing hands it the bytes the
/// foreground had already paid for. That does not change the conclusion — the acquisition
/// still completes, under an hour-long cap rather than the client's — but it does undercut
/// the "restart from zero" argument in the sentence above, which is worth being honest
/// about: the warm restarts from zero too. What falling through actually costs is the
/// *paid* bytes, and what it buys is the client a faster answer.
///
/// So this is best read as a bound on how long a **client** waits, not on how long
/// an acquisition takes.
#[must_use]
pub fn outer_pull_deadline(per_candidate: Duration, stall: Duration) -> Duration {
    let attempts = u32::try_from(MAX_PROVIDER_ATTEMPTS).unwrap_or(u32::MAX);
    CHANNEL_OPEN_CALLER_BUDGET
        .saturating_add(per_candidate)
        .saturating_add(stall)
        .saturating_mul(attempts)
        .saturating_add(PULL_THROUGH_OUTER_SLACK)
}

/// Reputation floor in the score denominator (ADR 001).
const REPUTATION_FLOOR: f32 = 0.1;

/// Default minimum-reputation rejection floor (issue #441). At this value —
/// and at any non-positive value, since reputation is domain `[0.0, 1.0]` —
/// filtering is disabled: every candidate is ranked, preserving pre-#441
/// semantics including the defensive negative-reputation clamp in
/// `compute_score`.
///
/// This is distinct from `REPUTATION_FLOOR`: that only clamps the score
/// *denominator* (capping the worst-case multiplier at 100×), whereas this is
/// a hard pre-filter that removes sub-floor candidates outright so price and
/// RTT can never override a poor reputation (ADR 001 §Node Selection
/// Algorithm).
pub const DEFAULT_MIN_REPUTATION: f32 = 0.0;

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
    // Clamp in f32 (input's domain), then promote once for the f64 score
    // arithmetic. Promoting first would let `f32(0.1)` slip just above the
    // floor (it rounds to ~0.10000000149f64), an unintuitive boundary.
    let rep = f64::from(reputation.max(REPUTATION_FLOOR));
    rate * rtt / (rep * rep)
}

/// Rank candidates by selection score, lowest (best) first.
///
/// Within-1%-score tie groups are reordered by the three-tier ADR 008
/// tie-breaker (geo → stake → random). The random tier uses a fresh
/// thread-local RNG; tests inside this module use the private
/// `rank_candidates_with_rng` variant for determinism.
pub fn rank_candidates(candidates: Vec<Candidate>) -> Vec<RankedCandidate> {
    rank_candidates_with_floor(candidates, DEFAULT_MIN_REPUTATION)
}

/// Drop candidates whose reputation is below `min_reputation` (issue #441).
///
/// Applied *before* scoring so a cheap, low-latency node can never win on
/// price when its reputation is below the client's floor. Any non-positive
/// `min_reputation` (the default `0.0` ([`DEFAULT_MIN_REPUTATION`]), or an
/// out-of-domain negative) disables filtering entirely — no candidate is
/// removed, preserving pre-#441 ranking semantics including the defensive
/// negative-reputation clamp in [`compute_score`]. The boundary is inclusive
/// (`reputation >= min_reputation`): a node exactly at the floor is kept. With
/// an active floor the comparison alone decides membership, so a `NaN`
/// reputation is rejected (any `NaN` comparison is false) while `+∞` would be
/// kept; symmetrically a `NaN` *floor* is fail-closed (drops every
/// candidate). Non-finite values cannot reach this from config today —
/// per-client wiring is deferred (issue #441).
fn apply_reputation_floor(candidates: Vec<Candidate>, min_reputation: f32) -> Vec<Candidate> {
    if min_reputation <= 0.0 {
        return candidates;
    }
    candidates
        .into_iter()
        .filter(|c| c.reputation >= min_reputation)
        .collect()
}

/// [`rank_candidates`] with a configurable minimum-reputation rejection floor
/// (issue #441). Candidates below `min_reputation` are removed before ranking;
/// `0.0` disables the floor. See `apply_reputation_floor` for the exact
/// boundary semantics.
pub fn rank_candidates_with_floor(
    candidates: Vec<Candidate>,
    min_reputation: f32,
) -> Vec<RankedCandidate> {
    let mut rng = rand::rng();
    rank_candidates_with_floor_and_rng(candidates, min_reputation, &mut rng)
}

/// Floor-filtering variant taking an explicit RNG. Internal — used by tests
/// for deterministic random tie-breaking on the surviving candidates.
fn rank_candidates_with_floor_and_rng(
    candidates: Vec<Candidate>,
    min_reputation: f32,
    rng: &mut impl rand::Rng,
) -> Vec<RankedCandidate> {
    rank_candidates_with_rng(apply_reputation_floor(candidates, min_reputation), rng)
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
/// ADR 008 § Tie-Breaking tiers. Geo diversity is scoped to the current tie
/// group: candidates within a single within-1% group are spread across
/// regions, but the tracker is reset between groups so unrelated tie groups
/// don't bias each other's geo tier.
fn apply_tiebreaker(ranked: &mut Vec<RankedCandidate>, rng: &mut impl rand::Rng) {
    let mut output: Vec<RankedCandidate> = Vec::with_capacity(ranked.len());

    while !ranked.is_empty() {
        // Geo diversity is scoped to the current tie group: candidates within
        // a single within-1% group are spread across regions, but the tracker
        // is reset between groups so unrelated tie groups don't bias each
        // other's geo tier. ADR 008 § Tie-Breaking lists the three tiers;
        // per-group scoping is this implementation's interpretation of
        // "within a tie".
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
/// geo (relative to `emitted_regions`) → stake → random tiers.
///
/// Filters a single index pool in place across the three tiers, so the
/// function allocates exactly one Vec per call regardless of group size or
/// tier depth.
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

    // Tier 1: prefer regions not in `emitted_regions`. If at least one
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

    // Tier 2: higher stake wins. `None` is treated as the lowest possible
    // stake (since on-chain integration is deferred — see ADR 001 "Contract
    // Interface: Node Registry" / ADR 019 for the capacity-bond interface
    // that will populate `Candidate.stake`).
    let max_stake = pool
        .iter()
        .filter_map(|i| group.get(*i).and_then(|r| r.candidate.stake))
        .max();
    if let Some(top) = max_stake {
        pool.retain(|i| group.get(*i).and_then(|r| r.candidate.stake) == Some(top));
    }

    // Tier 3: random tie-break. Uniformly pick from the remaining pool.
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

/// Convenience wrapper around [`rank_candidates`]: returns the top `n`
/// ranked candidates, where the caller will iterate them in order and stop
/// after the first successful pull.
///
/// For the standard fetch path use `n = MAX_PROVIDER_ATTEMPTS`. The function
/// returns fewer than `n` results when the candidate pool is smaller.
pub fn top_n(candidates: Vec<Candidate>, n: usize) -> Vec<RankedCandidate> {
    top_n_with_floor(candidates, n, DEFAULT_MIN_REPUTATION)
}

/// [`top_n`] with a configurable minimum-reputation rejection floor (issue
/// #441). The floor is applied first, then the survivors are ranked and
/// truncated to `n` — so `n` bounds the *reputable* result set, not the raw
/// candidate pool.
pub fn top_n_with_floor(
    candidates: Vec<Candidate>,
    n: usize,
    min_reputation: f32,
) -> Vec<RankedCandidate> {
    let mut ranked = rank_candidates_with_floor(candidates, min_reputation);
    ranked.truncate(n);
    ranked
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]
mod tests {
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
    // loop at `node_pull_stall_timeout_sec = 120`.
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
    // There was one, and it could not fail. `PULL_THROUGH_OUTER_SLACK` is now DEFINED as
    // `PROBE_TIMEOUT + DEFAULT_ROUND_TIMEOUT × MAX_LOOKUP_ROUNDS`, so a test that recomputes
    // that expression and asserts the slack is at least as big is asserting `A >= A`. It
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
    // `node_pull_stall_timeout_sec = 20` (both `DEFAULT_*` in decdn-common, which this
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
    //   - `common::config` (the `node_pull_timeout_sec` / `node_pull_stall_timeout_sec` docs)
    //   - `cli::commands::config` (the DEFAULT_CONFIG template)
    //   - `handlers::client::BACKGROUND_FILL_HARD_CAP` + `runtime` (as a RATIO against it)
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
        // Defensive: ReputationEngine::score() returns f32 in [0,1], but if a
        // bug produces a negative we must still not panic and must clamp.
        let s = compute_score(100, 10, -0.5);
        let at_floor = compute_score(100, 10, 0.1);
        assert!((s - at_floor).abs() < 1e-9);
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
            stake: None,
        }
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
    fn higher_stake_wins_when_geo_tied() {
        // Same score, same region. Higher stake wins at tier 2.
        let small_stake = with_stake(
            with_region(make_candidate(1, 100, 10, 1.0), "US"),
            Some(1_000),
        );
        let big_stake = with_stake(
            with_region(make_candidate(2, 100, 10, 1.0), "US"),
            Some(10_000),
        );
        let out = rank_candidates(vec![small_stake, big_stake]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
    }

    #[test]
    fn some_stake_beats_none_stake() {
        let known = with_stake(
            with_region(make_candidate(1, 100, 10, 1.0), "US"),
            Some(1_000),
        );
        let unknown = with_stake(with_region(make_candidate(2, 100, 10, 1.0), "US"), None);
        let out = rank_candidates(vec![unknown, known]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(1));
    }

    #[test]
    fn some_zero_stake_beats_none_stake() {
        // Zero-stake operators are still "known" — `Some(0)` should beat `None`
        // (treated as "not yet looked up") at tier 2. `max_stake` returns
        // `Some(0)` for the pool, then retain keeps only `stake == Some(0)`,
        // which drops the `None` entry.
        let known_zero = with_stake(with_region(make_candidate(1, 100, 10, 1.0), "US"), Some(0));
        let unknown = with_stake(with_region(make_candidate(2, 100, 10, 1.0), "US"), None);
        let out = rank_candidates(vec![unknown, known_zero]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(1));
    }

    #[test]
    fn three_way_zero_stake_falls_through_to_random_tier() {
        // Two `Some(0)` candidates plus one `None`: tier 2 retains both
        // `Some(0)` entries (dropping `None`), then tier 3 picks randomly
        // between the two survivors. Across enough seeds we should see both
        // survivors win first — confirms tier 2 doesn't accidentally
        // short-circuit on a single Some(0) winner.
        let a = with_stake(with_region(make_candidate(1, 100, 10, 1.0), "US"), Some(0));
        let b = with_stake(with_region(make_candidate(2, 100, 10, 1.0), "US"), Some(0));
        let unknown = with_stake(with_region(make_candidate(3, 100, 10, 1.0), "US"), None);
        let mut first_was_a = false;
        let mut first_was_b = false;
        for seed in 0u64..32 {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let out =
                rank_candidates_with_rng(vec![a.clone(), b.clone(), unknown.clone()], &mut rng);
            // The unknown (None stake) must never win first — tier 2 drops it.
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
        let us_rich = with_stake(
            with_region(make_candidate(1, 100, 10, 1.0), "US"),
            Some(10_000),
        );
        let de_poor = with_stake(
            with_region(make_candidate(2, 100, 10, 1.0), "DE"),
            Some(1_000),
        );
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
        let high_score = with_stake(
            with_region(make_candidate(1, 100, 10, 1.0), "US"),
            Some(1_000),
        ); // score 1000
        let low_score = with_stake(
            with_region(make_candidate(2, 1005, 1, 1.0), "US"),
            Some(10_000),
        ); // score 1005
        let out = rank_candidates(vec![high_score, low_score]);
        // Tied → higher-stake (id 2) wins despite higher raw score.
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
    }

    #[test]
    fn outside_1_percent_does_not_tie_break() {
        // 1000 vs 1020 → 2% gap, no tie-break. Higher stake on the dearer
        // candidate can't pull it ahead.
        let cheap = with_stake(
            with_region(make_candidate(1, 100, 10, 1.0), "US"),
            Some(1_000),
        ); // score 1000
        let dear = with_stake(
            with_region(make_candidate(2, 1020, 1, 1.0), "US"),
            Some(10_000),
        ); // score 1020
        let out = rank_candidates(vec![cheap, dear]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(1));
    }

    #[test]
    fn at_1_percent_boundary_counts_as_tied() {
        // 1000 vs 1010 → exactly 1.0% gap. tie_group_end uses `> TIE_THRESHOLD`,
        // so 1.0% is *inclusive* (still a tie). Pins the boundary against a
        // future change to `>=` that would silently exclude exact-1% pairs.
        // Stake on the dearer candidate proves the tie-break ran.
        let high_score = with_stake(
            with_region(make_candidate(1, 100, 10, 1.0), "US"),
            Some(1_000),
        ); // score 1000
        let low_score = with_stake(
            with_region(make_candidate(2, 1010, 1, 1.0), "US"),
            Some(10_000),
        ); // score 1010
        let out = rank_candidates(vec![high_score, low_score]);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(2));
    }

    #[test]
    fn zero_score_candidates_group_together_for_tie_break() {
        // Two zero-score candidates (rate=0, both have score 0.0) must form a
        // single tie group so the stake tier picks between them. Without the
        // zero-pivot fix, each becomes its own singleton group and stake is
        // skipped entirely.
        let zero_low_stake = with_stake(
            with_region(make_candidate(1, 0, 10, 1.0), "US"),
            Some(1_000),
        ); // score 0, low stake
        let zero_high_stake = with_stake(
            with_region(make_candidate(2, 0, 10, 1.0), "US"),
            Some(10_000),
        ); // score 0, high stake
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

    // --- Minimum-reputation rejection floor (issue #441) ---

    #[test]
    fn floor_drops_cheapest_subfloor_node() {
        // `bad` has the lowest raw score by far (rate 1, rtt 1) but a poor
        // reputation; `good` is expensive/slow but reputable. Without a floor
        // `bad` wins on price; with a 0.5 floor it must be rejected outright.
        let bad = make_candidate(1, 1, 1, 0.1);
        let good = make_candidate(2, 100, 10, 0.9);

        let unfiltered = rank_candidates(vec![bad.clone(), good.clone()]);
        assert_eq!(
            unfiltered.first().map(|r| r.candidate.node_id[0]),
            Some(1),
            "without a floor the cheap low-rep node wins"
        );

        let filtered = rank_candidates_with_floor(vec![bad, good], 0.5);
        let ids: Vec<u8> = filtered.iter().map(|r| r.candidate.node_id[0]).collect();
        assert_eq!(ids, vec![2], "sub-floor node must be dropped entirely");
    }

    #[test]
    fn default_rank_candidates_preserves_negative_reputation() {
        // Default path (DEFAULT_MIN_REPUTATION == 0.0) disables filtering, so
        // the pre-#441 defensive negative-reputation clamp behavior is intact:
        // the candidate is still ranked, not dropped.
        assert_eq!(DEFAULT_MIN_REPUTATION, 0.0);
        let neg = make_candidate(1, 100, 10, -0.5);
        let out = rank_candidates(vec![neg]);
        assert_eq!(out.len(), 1);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(1));
    }

    #[test]
    fn floor_boundary_is_inclusive() {
        // reputation exactly at the floor is kept (>=).
        let at = make_candidate(1, 100, 10, 0.5);
        let out = rank_candidates_with_floor(vec![at], 0.5);
        assert_eq!(out.len(), 1);
        assert_eq!(out.first().map(|r| r.candidate.node_id[0]), Some(1));
    }

    #[test]
    fn floor_drops_just_below() {
        let below = make_candidate(1, 100, 10, 0.49);
        let out = rank_candidates_with_floor(vec![below], 0.5);
        assert!(out.is_empty());
    }

    #[test]
    fn floor_all_below_returns_empty() {
        let a = make_candidate(1, 100, 10, 0.1);
        let b = make_candidate(2, 100, 10, 0.2);
        assert!(rank_candidates_with_floor(vec![a.clone(), b.clone()], 0.5).is_empty());
        assert!(top_n_with_floor(vec![a, b], MAX_PROVIDER_ATTEMPTS, 0.5).is_empty());
    }

    #[test]
    fn floor_rejects_nan_reputation() {
        // A NaN reputation is unusable; an active floor must reject it
        // (NaN >= x is false).
        let nan_rep = make_candidate(1, 100, 10, f32::NAN);
        let out = rank_candidates_with_floor(vec![nan_rep], 0.5);
        assert!(out.is_empty());
    }

    #[test]
    fn top_n_with_floor_truncates_after_filtering() {
        // Five reputable candidates plus two sub-floor ones; n = 3 must yield
        // exactly the three lowest-score reputable candidates. Rates double
        // each step so scores are >1% apart — no within-tie random reordering,
        // making the survivor order deterministic.
        let mut cs: Vec<Candidate> = (0..5)
            .map(|i| make_candidate(i, 100u64 << i, 10, 0.9))
            .collect();
        cs.push(make_candidate(10, 1, 1, 0.1));
        cs.push(make_candidate(11, 2, 1, 0.2));
        let out = top_n_with_floor(cs, 3, 0.5);
        let ids: Vec<u8> = out.iter().map(|r| r.candidate.node_id[0]).collect();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    #[test]
    fn floor_preserves_survivor_ordering() {
        // Among survivors the usual ascending-score order still holds.
        let cheap = make_candidate(1, 1, 10, 0.9); // score ~12.3
        let mid = make_candidate(2, 10, 10, 0.9); // score ~123
        let subfloor = make_candidate(3, 1, 1, 0.1); // cheapest raw, dropped
        let out = rank_candidates_with_floor(vec![mid, subfloor, cheap], 0.5);
        let ids: Vec<u8> = out.iter().map(|r| r.candidate.node_id[0]).collect();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn negative_floor_disables_filtering_like_zero() {
        // Pins the `min_reputation <= 0.0` disable branch for the negative
        // case. Two assertions, two distinct paths:
        //   - disabled path (floor -0.5): out-of-domain negative behaves like
        //     the `0.0` default — no filtering, so even the sub-zero-reputation
        //     candidate is retained.
        //   - active path (floor 0.5): the same sub-floor candidate IS dropped.
        // Together they guard against a future `== 0.0` / `< 0.0` guard
        // regression that would conflate the two.
        let poor = make_candidate(1, 100, 10, -0.5);
        let good = make_candidate(2, 100, 10, 0.9);

        let disabled = rank_candidates_with_floor(vec![poor.clone(), good.clone()], -0.5);
        let mut ids: Vec<u8> = disabled.iter().map(|r| r.candidate.node_id[0]).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2], "negative floor must keep every candidate");

        let active = rank_candidates_with_floor(vec![poor, good], 0.5);
        let ids: Vec<u8> = active.iter().map(|r| r.candidate.node_id[0]).collect();
        assert_eq!(
            ids,
            vec![2],
            "an active floor still drops the sub-floor node"
        );
    }

    #[test]
    fn floor_then_tiebreak_runs_deterministically() {
        // Covers `rank_candidates_with_floor_and_rng` directly and the
        // floor↔tie-break interaction: after the sub-floor node is removed,
        // two fully score-tied survivors must still flow through the
        // three-tier breaker. They differ only by stake, so the stake tier
        // decides deterministically (independent of the RNG seed) — the
        // higher-stake node ranks first and the sub-floor node is absent,
        // for every seed.
        let poor = with_stake(make_candidate(1, 100, 10, 0.9), Some(1_000));
        let rich = with_stake(make_candidate(2, 100, 10, 0.9), Some(10_000));
        let subfloor = make_candidate(3, 1, 1, 0.1); // cheapest raw, below floor
        for seed in 0u64..8 {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let out = rank_candidates_with_floor_and_rng(
                vec![poor.clone(), subfloor.clone(), rich.clone()],
                0.5,
                &mut rng,
            );
            let ids: Vec<u8> = out.iter().map(|r| r.candidate.node_id[0]).collect();
            assert_eq!(
                ids,
                vec![2, 1],
                "seed {seed}: floor drops #3, stake tier orders #2<#1"
            );
        }
    }

    #[test]
    fn floor_above_domain_rejects_all() {
        // Documented contract: a floor above the [0.0, 1.0] reputation domain
        // rejects every candidate, including a perfectly-reputable one.
        let perfect = make_candidate(1, 100, 10, 1.0);
        assert!(rank_candidates_with_floor(vec![perfect], 1.5).is_empty());
    }

    #[test]
    fn nan_floor_is_fail_closed() {
        // A `NaN` floor must drop every candidate, not silently disable
        // filtering. This relies on two subtle facts: `NaN <= 0.0` is false
        // (so the disable early-return is skipped) and `reputation >= NaN` is
        // false for all reputations (so the filter retains nothing). Pins the
        // documented fail-closed contract against a future guard change (e.g.
        // `min_reputation <= 0.0 || min_reputation.is_nan()`) that would flip
        // a malformed floor into disabled filtering.
        let perfect = make_candidate(1, 100, 10, 1.0);
        let neutral = make_candidate(2, 100, 10, 0.5);
        assert!(rank_candidates_with_floor(vec![perfect, neutral], f32::NAN).is_empty());
    }
}
