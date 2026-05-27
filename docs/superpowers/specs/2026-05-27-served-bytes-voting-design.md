# Served-Bytes Voting Weight — Design Spec

**Status:** Draft 2026-05-27 (sibling to `2026-05-27-remove-operator-emissions-design.md`; this spec is the design narrative for [ADR 036](../../../adr/036-served-bytes-voting-weight.md), which is the canonical ADR.)
**Supersedes (on acceptance):** the voting-weight clauses of [ADR 009 § Production: Operator-Weighted DAO Governance](../../../adr/009-governance.md#production-operator-weighted-dao-governance) and [ADR 026 § Governance](../../../adr/026-tokenomics.md#governance).
**Relationship to v2.2 emissions spec:** Independent — v2.2 spec concerns supply / distribution / emissions removal. This spec concerns governance vote weight only. Both apply concurrently; both lead to ADR edits in the same `docs/work-token-tokenomics-spec` branch.

## Summary

Replace `vote_weight = declared_capacity_Mbps × age_ramp(months_bonded)` with `vote_weight = min(served_bytes_window, voteCapBps × total_bytes_window) × age_ramp`. The served-bytes counter is the existing `FeeRouter.bytesPerEpoch` (currently labeled analytics-only under v2.2), promoted to governance-canonical. Trailing window default `N = 13` epochs (~1 quarter at 1-week epochs), governable `[4, 26]`. Existing per-operator cap (5%, `[1%, 25%]`) and tenure ramp (`age_ramp_months = 6`, `[1, 24]`) carry forward.

Three contract surface deltas: `FeeRouter` gains a global `totalBytesPerEpoch` counter, two trailing-window helpers, a governable `windowEpochs` setter; `CapacityBond` gains a `slashedAtEpoch[op]` watermark stamped on every slash; `DecdnGovernor._getVotes` rewrites to read FeeRouter for bytes, CapacityBond for tenure + slash zero-out.

## Motivation

The v2.2 governance design (ADR 009 + ADR 026 § Governance, as of 2026-05-27 pre-this-spec) computes vote weight as `CapacityBond.capacityAt(op, ts) × age_ramp(op, ts)`. The capacity factor is operator-asserted at registration time. It is gated by the capacity-bond curve and the capacity-shortfall slashing path ([ADR 026 § Capacity-shortfall slashing](../../../adr/026-tokenomics.md#capacity-shortfall-slashing)), but those bind only weakly to actual delivery:

- A deeply-bonded edge-tier operator who happens to serve light traffic this quarter still votes at full capacity weight.
- The capacity-shortfall slashing path triggers on a 4-week rolling probe window — by the time delivery is measurably below `min_delivery_ratio × declared_capacity`, the operator has already voted on multiple proposals.
- The capacity number itself is what operators *declared*, not what they *did*. Probe attestation backstops this only over multi-week windows.

The fix is to anchor vote weight in measured delivery. The FeeRouter already records `bytesDelivered` on every settlement and increments `bytesPerEpoch[operator][epoch]` inline. Today that counter has no on-chain consumer; promoting it to be the governance-canonical vote-weight source is the cleanest possible change — no new accounting flow, just a new reader.

The narrative shift: voting power belongs to operators who are *currently serving the network*, not to operators who once declared a high capacity tier.

## Design Decisions

User-resolved during the planning phase (`/home/thiras/.claude/plans/voting-should-be-weight-deep-beacon.md`):

### Time window: trailing N epochs (rolling)

Picked over cumulative-lifetime (oldest operators dominate forever; fresh entrants can never catch up) and EWMA-decayed (requires per-settlement accumulator writes; harder to audit when governance disputes arise). Trailing-N is fixed-width, append-only, and easy to reason about in a dispute ("here are the 13 epoch totals").

Default N=13 at 1-week epochs ≈ one quarter, mirroring quarterly governance cadences. Bounded `[4, 26]`:

- **Floor (4 epochs, ~1 month).** Below this, vote weight is too reactive to single-week bursts and easily gamed by wash-trading concentrated in a single epoch. Statistical signal on a 1–3-week window is also thin for small operators.
- **Ceiling (26 epochs, ~6 months).** Beyond this, the trailing sum lags the operator set's actual composition — an operator who exited service 5 months ago still carries half-weight, which is bad for governance responsiveness, especially in bootstrap.
- **Gas at ceiling.** `_getVotes` does O(N) cold SLOADs per call via the FeeRouter `bytesInWindow` helper. At N=26 the per-voter cost is ~55K gas on L2 — still cheap, still bounded.

### Tenure: multiply by existing `age_ramp`

Picked over dropping `age_ramp` (let bytes be the ramp). Defense-in-depth: even with high traffic, a fresh operator votes at a fraction of bytes-weighted share for the first 6 months. This costs nothing — `age_ramp` already exists in ADR 009 — and meaningfully limits "buy your way to instant governance" via burst-traffic.

### Cap: keep 5% per-operator (governable `[1%, 25%]`), applied to bytes-weight

Real CDN traffic skews power-law, so bytes-weighting concentrates more readily than capacity-weighting. The 5% per-operator cap (carry-forward from ADR 009's gauge-share-cap heritage) is load-bearing as the primary concentration defense. Cap applies before the `age_ramp` multiplication so the cap is *about traffic concentration*, not about tenure-adjusted weight.

### Slashing: explicit zero-out (recommended)

On slash (`CapacityBond.slash` or `slashCapacityShortfall`), CapacityBond stamps `slashedAtEpoch[op] = epoch(block.timestamp)`. `DecdnGovernor._getVotes` returns 0 whenever `slashedAtEpoch[op]` falls inside the trailing window. Once the window slides past the slash, the operator's vote weight recovers from forward served-bytes accrual.

Picked over natural-decay-only (let the rolling window do it). Natural decay leaves a slashed operator voting with their accumulated window bytes for up to N weeks — a meaningful immediate-response gap. The zero-out costs one storage slot per operator and one SLOAD per vote-cast.

Successful slash-appeal **reversal** via [ADR 028](../../../adr/028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation)'s `reverseAppeal` path (the path that determines the operator was wrongly slashed) clears `slashedAtEpoch`. `ratifyAppeal` (which only authorizes SafetyReserve USDC restitution without modifying the on-chain slash per ADR 028's existing stance) does **not** clear the field — consistent with ADR 028's "on-chain slash is not modified" semantics.

## Contract surface delta

Net additive: one new mapping on FeeRouter, one new mapping on CapacityBond, two new helper functions on FeeRouter, one new role grant, one renamed/rewritten `_getVotes` on DecdnGovernor. No deletions. Detail in [ADR 036 § Contracts and surface](../../../adr/036-served-bytes-voting-weight.md#contracts-and-surface) and the [ADR 016 amendments](../../../adr/016-contract-interactions.md#contract-feerouter).

`CapacityBond.totalVotingWeightAt(ts)` becomes dead surface — deprecated for one revision, removed in the next. No removal-coupled migration.

## Wash-trading threat model

Bytes-weighted voting is gameable by operators self-paying for delivery. An attacker who controls a client wallet pays themselves to serve bytes; under the four-bucket FeeRouter split, 60% of the per-byte USDC fee returns to the operator. The remaining 40% (25% burn + 10% treasury + 5% safety) is real USDC the attacker pays into the protocol with no offsetting revenue.

**Cost-to-buy-cap analysis.**

| Network revenue / week (`R`) | Cap (`voteCapBps`) | Window (`N`) | Sustained attacker spend / window |
| ----------------------------:| ------------------:| ------------:| --------------------------------:|
| $5K                          | 5%                 | 13           | ~$1,300                          |
| $25K                         | 5%                 | 13           | ~$6,500                          |
| $100K                        | 5%                 | 13           | ~$26K                            |
| $25K                         | 1% (floor)         | 13           | ~$1,300                          |
| $25K                         | 5%                 | 4 (floor)    | ~$2,000                          |

Cost = `0.40 × R × cap × N`. The attack is bounded but not prevented; the cap is the primary lever. At early-mainnet revenue, holding 5% vote weight against the cap costs an attacker low thousands of dollars per quarter. The cap floor (1%) is the first tightening response if attacks materialize; tightening the cap is preferable to shortening the window (which would make governance more vulnerable to burst-traffic manipulation).

Future-deferred mitigations: probe-verified-delivery multiplier (gate raw voucher bytes on `min_delivery_ratio` threshold); non-linear wash-trade-detection penalty; raise burn share. None taken at this spec — current parameters calibrated for the testnet-and-early-mainnet regime where attacker cost-of-capital is high relative to expected governance return.

## Alternatives considered (and rejected)

1. **Cumulative-lifetime served bytes (no decay).** Mirrors the FeeRouter operator-leg's denominator exactly. **Rejected:** oldest operators dominate forever; fresh entrants cannot catch up; vote weight does not reflect *current* contribution.
2. **EWMA-decayed served bytes.** Single accumulator, exponentially weighted, single `_getVotes` SLOAD instead of O(N). **Rejected:** requires per-settlement on-chain writes; couples vote weight to settlement timing; harder to reason about in dispute.
3. **No slashing zero-out — let rolling window do it.** Slashed operators retain accumulated weight and vote for up to N weeks until the window decays past the slash. **Rejected:** meaningful immediate-response gap; the zero-out costs one storage slot per operator.
4. **Probe-verified delivery cap multiplier.** Use `min(voucher_bytes, probe_capacity × epoch_length × min_delivery_ratio)` as the per-epoch input. **Rejected for now:** ties vote weight to probe attestation throughput, a separate roadmap concern; available as a future tightening lever.
5. **Replace `age_ramp` with first-served-bytes ramp.** `age_ramp_months` measured from the operator's first settled byte rather than first bond. **Rejected:** more complex, doesn't change the buy-your-way-to-instant-governance defense semantically, and `firstBondedAt` is a clean existing primitive.

## Sequencing

Spec acceptance → three follow-up PRs in order:

1. **Spec PR.** This document + ADR 036. Single ADR add, surgical edits to ADR 009 / ADR 016 / ADR 026, sibling note on the v2.2 emissions spec, CLAUDE.md ADR-counter bump.
2. **Implementation PR (`contracts/`).** Add `totalBytesPerEpoch` mapping + `bytesInWindow` / `totalBytesInWindow` helpers + `windowEpochs` + `setWindowEpochs` to FeeRouter; add `slashedAtEpoch` + `clearSlashedAtEpoch` to CapacityBond; rewrite `DecdnGovernor._getVotes`. Add `APPEAL_REVERSAL_ROLE` to the deployment script. Tests: unit-test the trailing-window arithmetic, slash zero-out, appeal-reversal restoration, the per-operator cap calculation. Branch off the existing contracts skeleton; not on the docs branch.
3. **Notebook / analytics PR.** Update `finance/notebooks/_shared/params.py` if it tracks the voting-weight formula (the `windowEpochs` parameter is new; the rest carries forward). Add a wash-trading cost-to-buy-cap notebook if useful for fundraising / litepaper.

Implementation PR is independent of the v2.2 emissions implementation. Both can ship in parallel.

## Open questions

1. **Quorum denominator approximation.** The strictly-correct quorum denominator is `Σ_op vote_weight(op, t)` (capped + ramped). The Governor uses `FeeRouter.totalBytesInWindow` (unramped, uncapped) as a tractable upper bound, accepting a mildly-conservative quorum / threshold bar. If this proves operationally problematic (proposals failing quorum for arithmetic-rounding reasons during the bootstrap phase), the alternative is an off-chain-computed Merkle-rooted denominator submitted with each proposal. Defer.
2. **Indexer impact.** Dashboards tracking `Settled` events need to add a running `totalBytesPerEpoch` aggregate to mirror the new global counter. Minor; document in the indexer / observability appendix.
3. **L2 sequencer behavior on `windowEpochs` change.** When governance changes `windowEpochs`, in-flight proposals (created under the old value, voting under the new value) read the new value at vote-cast time. This is consistent with how the per-operator cap behaves today on governance changes, but is worth calling out in the operations playbook so proposers aren't surprised.

## References

- ADR 036 (canonical decision document, sibling to this spec)
- ADR 009 § Production: Operator-Weighted DAO Governance (amended)
- ADR 016 § Contract: FeeRouter, § Contract: CapacityBond, § Cross-Contract Call Graph, § Deployment Order, § Access Control Matrix (amended)
- ADR 026 § Governance, § Governable parameters with safety bounds (amended)
- ADR 028 § Contract surface (`reverseAppeal` path now additionally clears `slashedAtEpoch`)
- Sibling spec: v2.2 emissions removal at `2026-05-27-remove-operator-emissions-design.md`
