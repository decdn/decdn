//! Provider selection algorithm (ADR 001 § Node Selection Algorithm + ADR 008
//! § Tie-Breaking).
//!
//! `rank_candidates` returns the input list ordered best-first (lowest score
//! first) with the three-tier tie-breaker applied. The caller iterates the
//! result in order and stops after `MAX_PROVIDER_ATTEMPTS` failed providers.

use rand::RngExt;
use std::collections::HashSet;
use std::time::Duration;

use decdn_protocol::Coverage;

use crate::dht::lookup::{DEFAULT_ROUND_TIMEOUT, MAX_LOOKUP_ROUNDS};

/// Maximum providers to attempt before reporting a fetch failure to the
/// caller (issue #322 — "max 3 provider attempts before returning error").
pub const MAX_PROVIDER_ATTEMPTS: usize = 3;

/// Per-candidate probe timeout. Because candidates are probed concurrently, this also
/// bounds the whole probe-collection phase. The budget covers connection setup plus one
/// unpaid probe request/response. `probe_once` dials a fresh connection every call, and
/// the protocol has no 0-RTT path, so even a resumable session pays a handshake round trip
/// before the request goes out — every probe costs two RTTs, not one. At the 250-300 ms
/// inter-continental RTTs this budget is sized against, that puts the furthest candidates
/// at or past this ceiling. Dropping them is the intended trade (a slow candidate must not
/// burn the caller's miss-latency budget), but this is the first term to revisit if
/// distant-region selection looks too sparse. Both the figure and the trade are ADR 001
/// § Probe response collection.
///
/// What is left of the budget after the exchange goes to `probe_once`'s wait for hole
/// punching to select a direct path. A candidate whose answer arrives in time is kept even
/// when that wait runs out: it ranks on its exchange RTT, which can be the relay detour, and
/// that RTT still feeds the ADR 030 region-latency penalty.
///
/// Lives here, beside the deadline arithmetic that has to budget for it, rather than in
/// `node_origin` where it is used (#1145 review). Every term of [`outer_pull_deadline`] is
/// then visible in one file, which is what stops the next one from being guessed.
pub const PROBE_TIMEOUT: Duration = Duration::from_millis(500);

/// How many blob-holding candidates a SINGLE-SOURCE probe round collects before
/// it stops waiting on the rest. [`PROBE_TIMEOUT`] stays the ceiling — a sparse
/// round that never reaches this count still waits it out — but when this many
/// good providers have already answered, the round selects among them instead of
/// waiting the full window for a straggler or a dead peer that will only time
/// out. The count leaves at least one alternate in hand for per-blob failover
/// while keeping cold-miss discovery latency at the speed of the fastest good
/// answers rather than the ceiling. ADR 001 § Probe response collection.
///
/// Equal to [`MAX_PROVIDER_ATTEMPTS`] on purpose: the single-source pull loop
/// tries at most that many providers, so collecting that many viable candidates
/// already fills the failover budget — waiting for more only serves stragglers
/// the loop would never reach.
///
/// This fixed count is CORRECT only when one working provider is enough — the
/// buffered single-source pull. The ranged-drive assembly path (#1506, ADR 039)
/// needs a candidate set whose coverage UNION spans the requested range, not a
/// fixed count: three responders that all hold discovery block 0 do not cover a
/// three-block blob, yet stopping at this count would drop the block-1/block-2
/// holders in the same fanout. That path gathers by coverage union instead (see
/// `node_origin::probe_and_rank`'s `ProbeGather::CoverageUnion`), bounded only by
/// the probe fanout.
pub const PROBE_EARLY_EXIT_CANDIDATES: usize = MAX_PROVIDER_ATTEMPTS;

/// One-time headroom added on top of the `MAX_PROVIDER_ATTEMPTS` sequential per-candidate
/// costs when computing the outer pull-through deadline (#859). It covers the *one-time*
/// `discover → probe → rank` overhead: work that runs under the outer deadline but does not
/// scale with the attempt count.
///
/// **Derived, not chosen** (#1145 review). A flat constant would be smaller than the thing
/// it is named for:
///
/// - probing is concurrent, so it costs one [`PROBE_TIMEOUT`] — 500 ms; but
/// - discovery is `find_providers`, whose rounds are each bounded by
///   [`DEFAULT_ROUND_TIMEOUT`] (8 s) and whose round count is capped by
///   [`MAX_LOOKUP_ROUNDS`] (4).
///
/// At those defaults, four discovery rounds plus the probe phase can cost 32.5 s. A flat
/// budget short of that leaves the outer deadline short of what the fetch can actually spend,
/// so the `tokio::time::timeout` around `discover → probe → rank → pull` can fire while
/// candidate #3 is still in its stall window — the #859 fallback starvation this formula
/// exists to prevent, reachable at the defaults.
///
/// The fix is in two halves, and both are necessary: [`MAX_LOOKUP_ROUNDS`] makes discovery's
/// worst case finite, and this makes it a TERM. An unbounded cost cannot be budgeted for by
/// any constant, however generous.
pub const PULL_THROUGH_OUTER_SLACK: Duration =
    PROBE_TIMEOUT.saturating_add(DEFAULT_ROUND_TIMEOUT.saturating_mul(MAX_LOOKUP_ROUNDS));

/// How long a pull is willing to WAIT on a buyer-channel open before giving up on
/// that candidate — not how long the open itself is allowed to take (#1143).
///
/// The `openChannel` runs in a detached task that owns the tx, so a
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
/// the streaming throughput-floor window (`cache.node_pull_stall_window_sec`).
///
/// The delivery handler wraps the whole `discover → probe → rank → pull` fetch
/// in a single `tokio::time::timeout`. For the sequential `MAX_PROVIDER_ATTEMPTS`
/// fallback loop to actually reach candidates #2..N when candidate #1 *stalls*,
/// this outer deadline must strictly exceed the sum of all per-candidate costs —
/// otherwise both clocks expire
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
///    one full throughput-floor window (`stall`) before the pull gives up on it (#1797).
///
/// So the worst case for a candidate is `CHANNEL_OPEN_CALLER_BUDGET + per_candidate + stall`,
/// and that is what this must budget `MAX_PROVIDER_ATTEMPTS` of, plus
/// [`PULL_THROUGH_OUTER_SLACK`] of one-time discovery overhead.
///
/// Every stage has to be a term here. Budgeting only `per_candidate` would let a cold
/// cache against a slow L2 burn the whole deadline on candidates #1 and #2 and never
/// dial #3. Covering the channel open but not the stall window would leave the same hole
/// for a peer that goes silent mid-stream instead of failing to open — and a worse one,
/// because `stall` is operator-tunable: at `node_pull_stall_window_sec = 120` a single
/// silent candidate outlasts a two-term deadline on its own. Taking `stall` as an
/// argument keeps this deadline in step with `node_pull_stall_window_sec`.
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
/// download from zero against another peer, throwing away the bytes already paid for.
/// On expiry the foreground request gives up with a clean miss and nothing continues in
/// the background — there is no detached warm, so a pull only ever runs while a
/// client is waiting, and the blob is re-acquired on the next real client request.
///
/// So this bounds how long a **client** waits; no acquisition outlives that wait.
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

/// Reputation ceiling in the score denominator — the top of
/// [`Candidate::reputation`]'s documented domain (#1458). Named rather than
/// written inline so the code, the doc prose, and the tests that pin it move
/// together, the way [`REPUTATION_FLOOR`] already does.
const REPUTATION_CEILING: f32 = 1.0;

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
    /// ISO 3166-1 alpha-2 region self-attested by the peer on-chain (ADR 030),
    /// resolved from the `CapacityBond` registry projection. Used by the
    /// geo-diversity tie-break tier.
    pub region: String,
    /// On-chain stake in TOKEN base units; higher stake wins the stake
    /// tie-break tier. `0` means *known to hold no bond* — there is no
    /// "not looked up" state, by design (#1470).
    ///
    /// This type has no *encoding* for a failed read — not the same as
    /// preventing one. A chain read succeeds for some peers and fails for
    /// others, and a mixed population is the steady state for an RPC read, not
    /// an edge case; folding a failure to `0` here would silently sink a
    /// well-staked peer below a provably-unbonded one. So the lookup layer must
    /// resolve the failure where it is still visible: retry it, or drop the
    /// candidate. **Nothing in the type enforces that today** — a future caller
    /// can still write `stake: 0` on a failed read and it will compile. The
    /// constructor-level enforcement (a `Stake` obtainable only from a
    /// successful read, with `Candidate`'s fields made private) lands with the
    /// lookup itself; see #1470. Nothing populates this yet — on-chain
    /// integration is deferred; see ADR 019 for the capacity-bond interface
    /// that will.
    pub stake: u64,
    /// Which discovery blocks (`decdn_protocol::coverage`) this candidate is
    /// confirmed to hold, per its fresh `ProbeResponseExt.coverage` (#1506).
    /// This is the PROBE-confirmed value, never the stale DHT-lookup hint —
    /// discovery's ranking and selection consume only what a live probe just
    /// verified. Ranking itself ignores this field; it rides alongside the
    /// rank/RTT/rate fields for a range-aware caller to read after selection.
    pub coverage: Coverage,
    /// The blob size the candidate reported on that probe
    /// (`ProbeResponseExt.total_bytes`), when it knows it. UNSIGNED: a sizing
    /// hint, never a commitment. The pull leg cuts its first upstream open from
    /// it. When that open runs past the blob's end, the pull leg opens the whole
    /// blob instead; when the signed size differs from the hint, it drops the
    /// pull.
    pub total_bytes_hint: Option<u64>,
}

/// A candidate paired with its computed selection score. Lower score is better.
#[derive(Debug, Clone)]
pub struct RankedCandidate {
    /// The peer that was scored.
    pub candidate: Candidate,
    /// Its selection score. Lower is better.
    pub score: f64,
}

/// Compute the unified selection score (ADR 001). Lower is better.
///
/// `score = rate_per_mb × max(rtt_ms, 1) × (1 / clamp(reputation, 0.1, 1.0)²)`
///
/// The RTT has a floor of 1 ms. A probe faster than 1 ms truncates to
/// `rtt_ms = 0`, and a zero factor would make the score 0 for any rate: two
/// such peers would tie whatever they quote, and a dear one would outrank every
/// cheaper peer at 1 ms or more. With the floor, sub-millisecond peers rank on
/// rate and reputation, and only `rate_per_mb = 0` gives a score of 0.
///
/// ADR 001 § Node Selection Algorithm states the denominator as
/// `max(reputation, 0.1)`; the upper bound here is a defensive extension
/// (#1458), a no-op for any input inside the ADR's `[0.0, 1.0]` domain.
///
/// Reputation is clamped between [`REPUTATION_FLOOR`] and
/// [`REPUTATION_CEILING`] before squaring — both ends, and both ends matter.
/// The floor prevents division by zero and caps the worst-case penalty
/// multiplier at 100×; the ceiling stops an out-of-domain value from buying an
/// unearned *bonus*. Without it, `reputation = 10.0` divides the score by 100
/// and outranks a perfect-reputation peer by 100×, and `f32::INFINITY` yields
/// score `0.0` — which [`tie_group_end`] treats as a tie group of one, so it
/// takes the top slot outright, beyond the reach of every tie-break tier.
#[allow(clippy::cast_precision_loss)]
// f64 has 53-bit mantissa; ULP-level imprecision on huge u64 rates does not
// affect ordering decisions here.
fn compute_score(rate_per_mb: u64, rtt_ms: u32, reputation: f32) -> f64 {
    let rate = rate_per_mb as f64;
    let rtt = f64::from(rtt_ms.max(1));
    // Clamp in f32 (input's domain), then promote once for the f64 score
    // arithmetic. Promoting first would let `f32(0.1)` slip just above the
    // floor (it rounds to ~0.10000000149f64), an unintuitive boundary.
    //
    // `.max().min()` rather than `.clamp(..)`, and floor FIRST: `f32::max` and
    // `f32::min` are documented to ignore NaN and return the other operand, so
    // a NaN reputation lands on the floor. `f32::clamp` propagates NaN instead,
    // and `.min()` first would send NaN to the CEILING — either way a garbage
    // reputation becomes a perfect peer. Hence the waiver below: taking clippy's
    // `manual_clamp` suggestion would turn a NaN-total function into a
    // NaN-propagating one and fail `score_nan_reputation_clamps_to_floor`.
    #[allow(clippy::manual_clamp)]
    let rep = f64::from(reputation.max(REPUTATION_FLOOR).min(REPUTATION_CEILING));
    rate * rtt / (rep * rep)
}

/// Rank candidates by selection score, lowest (best) first.
///
/// Within-1%-score tie groups are reordered by the three-tier ADR 008
/// tie-breaker (geo → stake → random). The random tier uses a fresh
/// thread-local RNG; tests inside this module use the private
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

    // Tier 2: higher stake wins. Every value here is meant to be a completed
    // observation — a failed on-chain read must be resolved at the lookup layer
    // (retried, or the candidate dropped) rather than reaching
    // `Candidate.stake` — so this tier can rank on the numbers alone. See the
    // field doc for why that boundary matters, and for the fact that nothing
    // enforces it yet (#1470).
    //
    // Uniformly a no-op today: every production construction site passes `0`
    // because on-chain integration is deferred (ADR 003 § Node Registry for the
    // contract surface, ADR 019 for the capacity-bond interface that will
    // populate it).
    let max_stake = pool
        .iter()
        .filter_map(|i| group.get(*i).map(|r| r.candidate.stake))
        .max();
    if let Some(top) = max_stake {
        pool.retain(|i| group.get(*i).map(|r| r.candidate.stake) == Some(top));
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::float_cmp)]
mod tests;
