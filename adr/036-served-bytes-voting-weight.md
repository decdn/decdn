# ADR 036: Served-Bytes Voting Weight

**Date:** 2026-05-27
**Status:** Draft

## Context

DAO voting weight could be keyed on declared capacity — `declared_capacity_Mbps × age_ramp(months_bonded)`, sourced from `CapacityBond.declaredMbps × age_ramp` — but declared capacity is operator-asserted at registration time and only loosely tied to actual delivery. A deeply-bonded but lightly-serving operator would then carry a vote weight that does not reflect their real contribution to the network.

The on-chain raw material to fix this already exists. [ADR 016 § Contract: FeeRouter](016-contract-interactions.md#contract-feerouter) populates `bytesPerEpoch[operator][epoch]` inline on every `routeSettlement`. This ADR makes that counter the canonical voting-weight source.

The change is governance-only — no impact on payment-channel mechanics ([ADR 003](003-payments.md#adr-003-payment-model)), on the three-bucket fee split ([ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split)), on the capacity-bond curve ([ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)), on `CapacityBond`'s registration / slashing semantics, or on `SlashAppeal` / `BuybackBurner`. `CapacityBond` remains the registration gate and the source of `firstBondedAt` for the age-ramp.

## Decision

DAO vote weight is the trailing-window sum of an operator's served bytes, multiplied by the existing tenure ramp, with the existing per-operator cap applied against the bytes-weighted total.

### Formula

```
vote_weight(op, t) = min(
    served_bytes_window(op, t),
    voteCapBps × total_bytes_window(t) / 10_000
) × age_ramp(op, t)

served_bytes_window(op, t) = Σ_{e = epoch(t)-N+1 .. epoch(t)} FeeRouter.bytesPerEpoch[op][e]

total_bytes_window(t)      = Σ_{e = epoch(t)-N+1 .. epoch(t)} FeeRouter.totalBytesPerEpoch[e]

age_ramp(op, t) = min(
    (t - CapacityBond.firstBondedAt[op]) / (age_ramp_months × seconds_per_month),
    1.0
)

epoch(t) = uint64(t / EPOCH_LENGTH)                     // EPOCH_LENGTH = 1 week, immutable
N        = windowEpochs                                  // default 13 (~1 quarter)
```

Where `t` is the OpenZeppelin Governor timepoint (timestamp clock per ERC-6372, consistent with [ADR 009 § Production](009-governance.md#production-operator-weighted-dao-governance)).

### Behaviors that follow from the formula

- **A registered operator with zero served bytes in the trailing window has zero vote.** The bond gates eligibility to vote; it does not directly grant weight.
- **A fresh operator who serves heavily on day 1 still ramps in over `age_ramp_months`.** `age_ramp` is the tenure-buy-in defense; it stays defense-in-depth on top of bytes.
- **Per-operator cap is computed against the bytes-weighted total at the same timepoint**, not against any historical or capacity-derived total. The cap clamp applies pre-multiplication by `age_ramp`.
- **`quorum(t)` and `proposalThreshold(t)` use `FeeRouter.totalBytesInWindow(epoch(t), N)` as the denominator — an *upper-bound proxy* for `Σ_op vote_weight(op, t)`, not the exact sum.** The exact sum applies the per-operator cap and the `age_ramp` multiplier (both `≤ 1`), so `Σ_op vote_weight ≤ totalBytesInWindow` always. Calibrating quorum against the proxy is intentionally conservative — it makes quorum strictly harder to reach than against the true Σ — and avoids the gas of summing per-operator capped contributions on every `castVote`. The proxy is exact when no operator is above the cap and all operators are past `age_ramp_months` of tenure (the steady state).

### Slashing zero-out

On any slash invocation (`CapacityBond.slash`), `CapacityBond` stamps `slashedAtEpoch[op] = epoch(block.timestamp)`. The Governor's `_getVotes(op, t)` returns zero whenever `slashedAtEpoch[op] >= epoch(t) - N + 1` — i.e., whenever the slash falls inside the current trailing window. Once the window slides past the slash, the operator's vote weight recovers based on their forward served-bytes accrual.

On a **granted** slash appeal via [ADR 028 § Contract surface](028-slashing-appeals.md#contract-surface)'s `grantAppeal` path (the path that determines the operator was wrongly slashed), `SlashAppeal` calls `CapacityBond.settleAppealGranted`, which refunds the escrowed TOKEN and **recomputes** `slashedAtEpoch[op]` (the internal `_recomputeSlashedAtEpoch`), clearing it to zero only when no slash stands. An **upheld** appeal (`upholdAppeal` / `rejectAppeal`) leaves the field stamped — the slash stands.

**Multi-slash watermark semantics.** `slashedAtEpoch[op]` is a single scalar but represents the **max epoch among the operator's still-standing slashes** — those NOT in the `Reversed` state (`Escrowed` / `AppealOpen` / `Upheld` all still stand). Because slashes are minted in non-decreasing epoch order, a new slash always raises the watermark to its own epoch. A granted appeal marks one record `Reversed` and re-derives the watermark as the max epoch over the operator's remaining non-`Reversed` records, clearing to zero only if none remain. This closes the multi-outstanding-slash hole where granting the appeal of one slash (e.g. the most recent) would otherwise clear the watermark while an *earlier* slash still stands — wrongly restoring vote weight and `claimVestedCredit` eligibility for the unresolved slash. The re-derivation scans the operator's own slash records; the per-operator record list is bounded in practice because a slashed operator auto-ejects below `minBond / 2`.

This is one storage slot per operator on `CapacityBond` and one read on every vote-cast. It restores the immediate-vote-removal-on-slash signal that the bytes window alone cannot deliver (an active operator who is slashed today would otherwise continue voting with their accumulated window bytes for up to N weeks).

### Contracts and surface

The full Solidity surface is documented in [ADR 016 § Contract: FeeRouter](016-contract-interactions.md#contract-feerouter) and [ADR 016 § Contract: CapacityBond](016-contract-interactions.md#contract-capacitybond). Highlights:

**`FeeRouter` additions:**

- `mapping(uint64 => uint256) totalBytesPerEpoch` — incremented inline in `routeSettlement` alongside the existing per-operator counter (one warm-slot SSTORE after the first settlement in the epoch).
- `function totalBytesPerEpoch(uint64 epoch) external view returns (uint256);` — public getter, mirrors the per-operator getter.
- `function bytesInWindow(address op, uint64 endEpoch, uint64 N) external view returns (uint256);` — O(N) trailing-sum helper.
- `function totalBytesInWindow(uint64 endEpoch, uint64 N) external view returns (uint256);` — global O(N) trailing-sum helper. Co-locating the loop with the storage keeps `windowEpochs` as a single governance-mutable source of truth and makes `_getVotes` a constant number of external calls.
- `uint64 public windowEpochs;` — storage; constructor takes `windowEpochsDefault`.
- `function setWindowEpochs(uint64 n) external;` — `GOVERNANCE_ROLE`, bounded `[4, 26]` (see [§ Governable parameters](#governable-parameters-with-safety-bounds)).

**`CapacityBond` additions:**

- `mapping(address => uint64) slashedAtEpoch;` — set by `slash()` to the max epoch among the operator's still-standing slashes, recomputed by the `grantAppeal` flow (`settleAppealGranted` → `_recomputeSlashedAtEpoch`) per [ADR 028](028-slashing-appeals.md#contract-surface); cleared to zero only when no slash stands.
- `mapping(address => uint256[]) _operatorSlashIds;` — internal per-operator slash index backing the watermark re-derivation above.
- `function slashedAtEpoch(address op) external view returns (uint64);` — public getter for the Governor.

**`DecdnGovernor._getVotes`** (pseudocode):

```solidity
function _getVotes(address op, uint256 timepoint, bytes memory) override returns (uint256) {
    uint64 endEpoch = uint64(timepoint / EPOCH_LENGTH);
    uint64 windowStart = endEpoch + 1 > windowEpochs ? endEpoch + 1 - windowEpochs : 0;
    if (capacityBond.slashedAtEpoch(op) >= windowStart) {
        return 0;
    }
    uint256 served = feeRouter.bytesInWindow(op, endEpoch, windowEpochs);
    uint256 total  = feeRouter.totalBytesInWindow(endEpoch, windowEpochs);
    uint256 cap    = (total * voteCapBps) / 10_000;
    uint256 capped = served < cap ? served : cap;
    uint256 ramp   = ageRampScaled(capacityBond.firstBondedAt(op), timepoint, ageRampMonths);
    return (capped * ramp) / 1e18;
}
```

`quorum(t)` and `proposalThreshold(t)` use `feeRouter.totalBytesInWindow(epoch(t), windowEpochs)` × 4% / 0.1% respectively. The capped-and-ramped total weight (not raw bytes) is the strictly correct denominator, but is O(active_operators × N) to compute; the Governor uses the unramped, uncapped total bytes as a tractable upper bound and accepts the resulting quorum / threshold conservativeness.

The shipped `CapacityBond` exposes no `totalVotingWeightAt(ts)` aggregate getter. Under this ADR the Governor derives total voting weight from FeeRouter epoch accounting (`totalBytesInWindow`), not from a CapacityBond aggregate read.

`IVotes`/IERC-5805 is not used: voting weight is derived from FeeRouter epoch accounting, not from per-account checkpoint structures, so `GovernorVotes` / `GovernorVotesQuorumFraction` are unused.

### Governable parameters with safety bounds

| Parameter           | Default | Min | Max  | Rationale |
| ------------------- | ------: | --: | ---: | --------- |
| `windowEpochs` (N)  |      13 |   4 |   26 | <4 (1 month) too reactive to single-burst wash-trading and statistically thin for small operators; >26 (6 months) lags actual operator-set composition and pushes `_getVotes` toward ~55K gas of cold SLOADs per voter per `castVote` on L2 |
| `voteCapBps`        |    500  | 100 | 2500 | Carry forward from [ADR 009](009-governance.md#governable-parameters-with-safety-bounds); cap now applied against bytes-weighted total |
| `age_ramp_months`   |      6  |   1 |   24 | Carry forward from [ADR 009](009-governance.md#governable-parameters-with-safety-bounds) and [ADR 026 § Governable parameters](026-tokenomics.md#governable-parameters-with-safety-bounds) |

Cross-parameter invariant (informational, not enforced at the contract layer): `windowEpochs ≤ age_ramp_months × 4.33` keeps the age-ramp horizon ≥ the bytes window. Enforcement at the contract layer would over-constrain governance flexibility and is not warranted; document the invariant and let governance honor it.

## Threat Model

### Wash-trading as vote-buying

Bytes-weighted voting is gameable by operators self-paying for delivery. An attacker who controls a client wallet pays themselves to serve bytes; under the three-bucket FeeRouter split ([ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split)), 60% of the per-byte USDC fee returns to the operator. The remaining 40% (30% burn + 10% treasury) is real USDC the attacker pays into the protocol with no offsetting revenue.

**Attack cost to reach the 5% cap from zero.** Let `R` be the network's network-wide USDC revenue per epoch. An attacker holding `cap` share of the trailing-window bytes generates `R × cap × N` revenue across the window (assuming uniform per-byte pricing). Wash trading rate × per-byte cost is `0.40 × R × cap × N` in attacker-paid USDC across the window. At `R = $25K/week`, `cap = 5%`, `N = 13`: attacker burns `0.40 × $25K × 0.05 × 13 ≈ $6,500` per quarter to hold full 5% vote.

Cost scales linearly with network revenue and with the cap. Defenses:

1. **5% per-operator cap (`voteCapBps`).** Bounds the maximum vote any single attacker can buy. Cap is governable `[1%, 25%]`; raising the floor (lowering to 1%) is the first tightening lever if attacks materialize.
2. **`age_ramp` floor.** A brand-new bonded operator who wash-trades heavily still votes at a fraction of their bytes-weighted share for the first `age_ramp_months` months. Combined with the cap, this limits the speed-to-influence of a fresh attacker.
3. **Rolling-window decay.** Sustained wash trading is required across the full window to maintain the cap; a stopping attacker decays out over N epochs.

The attack is bounded but not eliminated. Future tightening options (deferred to a follow-up ADR if observed): raise the burn share, gate vote weight on probe-verified delivery threshold, introduce a non-linear wash-trade-detection penalty. The current parameters are calibrated for the testnet-and-early-mainnet regime where attacker cost-of-capital is high relative to expected governance return.

### Instant-governance via bond-then-serve-burst

Defended by `age_ramp`. A fresh operator who bonds at `t=0` and serves the entire network's traffic at `t=0+ε` votes at `(t / age_ramp_months × seconds_per_month)` of full weight for the first `age_ramp_months` months. With default `age_ramp_months = 6`, day-1 vote weight is ≈ 0.5% of full weight; week-1 is ≈ 3%. This forces sustained activity across both the bytes axis (rolling window) and the time axis (ramp).

### Slashed-but-still-voting

Defended by the slashing zero-out. Without zero-out, a slashed operator continues voting with their accumulated window bytes for up to N weeks. With zero-out, slashing immediately revokes vote weight for the remainder of the window. This is a tighter response than a `declaredMbps × age_ramp` mechanism, which would only reduce vote weight by the bond-reduction ratio.

### Concentration

The per-operator cap (`voteCapBps`, default 5%) is the primary concentration defense. Real delivery skews power-law in CDN markets, so bytes-weighting concentrates more readily than capacity-weighting. The cap is governable `[1%, 25%]`; governance can tighten if observed concentration warrants. Lowering the cap is preferable to lowering `windowEpochs`, because window-shortening would make governance more vulnerable to burst-traffic manipulation.

## Consequences

### Positive

- **Voting weight tracks demonstrated network contribution.** An operator who isn't serving has no vote; an operator who is serving heavily has weight proportional to that service (up to the cap).
- **Reuses existing on-chain accounting.** No new fundamental data flow; `bytesPerEpoch` is already populated. Marginal contract surface is two integer mappings, one trailing-sum helper, one tenure-ramp lookup.
- **Strengthens skin-in-the-game story.** Combined with [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split)'s per-byte operator payment, governance influence and economic upside both scale with actual delivery.
- **Slashing-zero-out tightens response time.** Slash → immediate vote zero, recovers naturally over `windowEpochs`. Cleaner than a declared-capacity-proportional reduction.

### Negative

- **Wash-trading is bounded but not zero-cost prevented.** ~40% of attacker-paid USDC is forfeit per cycle (burn + treasury + safety legs). At cap floor (1%) and default parameters, attacker cost is non-trivial but not unaffordable. See [§ Threat Model — Wash-trading](#wash-trading-as-vote-buying).
- **Vote weight is more volatile than capacity-weighted weight.** A heavy traffic week ramps an operator's vote in days; a quiet quarter decays it out. Governance proposers cannot assume a fixed voting set; quorum computations must re-read the FeeRouter window state.
- **No on-chain aggregate vote-weight getter.** `CapacityBond` exposes no `totalVotingWeightAt(ts)`; indexers and dashboards must compute totals from the FeeRouter-derived window (`totalBytesInWindow`) rather than a single contract read.
- **`FeeRouter` migration becomes a governance-snapshot reset.** `epochLength` is constructor-immutable per [ADR 016 § No proxy deployment patterns](016-contract-interactions.md#no-proxy-deployment-patterns), so any future `FeeRouter` migration starts with empty `bytesPerEpoch`; vote weight resets to zero for all operators until traffic refills the window. Operational consideration, not a soundness concern — the existing `FeeRouter` migration path already requires state migration.
- **Indexer / off-chain reader impact.** Dashboards tracking `Settled` events need to add a running `totalBytesPerEpoch` aggregate to mirror the contract's new global counter. Minor.

### Risks

- **Network in low-revenue phase has cheap attack economics.** At `R = $5K/week`, wash-trading 5% cap costs only ~$1,300 / quarter. Governance should be prepared to tighten `voteCapBps` toward 1% during the bootstrap-multisig phase if observed.
- **Operator with one large publisher could naturally exceed cap.** A legitimate edge-tier operator serving a single high-volume publisher hits the 5% cap easily. They are capped at 5% vote weight — same as a wash trader — which is the design intent (concentration defense applies uniformly). The cap should not be confused with a punishment for honest top carriers.
- **`age_ramp` and `windowEpochs` interaction window.** A new operator who serves heavily in their first month builds a saturated bytes window before their age-ramp completes. The multiplicative combination still gates them under both axes, but the per-axis behavior diverges from intuition. Document for governance proposers.

## Cross-ADR Impact

- **[ADR 009 — Governance Model](009-governance.md#adr-009-governance-model):** §Production: Operator-Weighted DAO Governance sources voting weight from this ADR's formula, with its clock anchored to `FeeRouter` epoch accounting and its quorum calibration reading `FeeRouter.totalBytesInWindow`. Its Governable Parameters table carries `windowEpochs`, and its Consequences > Negative notes wash-trading-as-vote-buying.
- **[ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md#adr-016-smart-contract-interaction-model):** Contract Inventory `DecdnGovernor` row reads `FeeRouter` for vote weight. `classDiagram` `FeeRouter` exposes `totalBytesPerEpoch`, `bytesInWindow`, `totalBytesInWindow`; the `Governor` vote-weight edges target `FeeRouter`. `Contract: FeeRouter` section carries the `bytesPerEpoch` / `totalBytesPerEpoch` mappings, getters, and `setWindowEpochs`. `Contract: CapacityBond` section carries `slashedAtEpoch`. Cross-Contract Call Graph + Complete Call Table list `Governor → FeeRouter: bytesInWindow / totalBytesInWindow`. Deployment Order step 13 (`DecdnGovernor`) lists `FeeRouter` as a non-zero constructor input.
- **[ADR 026 — Tokenomics](026-tokenomics.md#adr-026-tokenomics):** §Governance states the served-bytes `vote_weight` formula and points its `Voting source` row at `FeeRouter` + this ADR. §Governable parameters with safety bounds carries `windowEpochs`.
- **[ADR 028 — Slashing Appeals](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation):** the `grantAppeal` path (`settleAppealGranted`) clears `CapacityBond.slashedAtEpoch[op]` alongside refunding the operator's escrowed TOKEN (see [ADR 028 § Contract surface](028-slashing-appeals.md#contract-surface)); no other appeal-flow change.

## References

- [ADR 003 — Payment Model](003-payments.md#adr-003-payment-model)
- [ADR 009 — Governance Model](009-governance.md#adr-009-governance-model)
- [ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md#adr-016-smart-contract-interaction-model)
- [ADR 026 — Tokenomics](026-tokenomics.md#adr-026-tokenomics)
- [ADR 028 — Slashing Appeals](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation)
