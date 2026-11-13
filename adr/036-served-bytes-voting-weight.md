# ADR 036: Served-Bytes Voting Weight

**Date:** 2026-05-27
**Status:** Accepted

## Vote window

The vote window ends at the **last fully-elapsed epoch** as of the proposal-snapshot timepoint — `endEpoch(t) = epoch(t) == 0 ? ∅ : epoch(t) - 1` (the `∅` epoch-0 case ⇒ empty window ⇒ weight 0; see § Formula) — rather than at `epoch(t)`, the in-progress epoch. `FeeRouter` only ever increments the epoch of the sweep's arrival block, so an in-progress epoch's tally keeps changing after a proposal snapshot. Reading it in `_getVotes`/`quorum`/`proposalThreshold` violates the OpenZeppelin `Governor` snapshot invariant (weight must be immutable after the snapshot): an operator could sweep bytes mid-vote and raise its own already-counted weight, and two voters casting at different times would see different totals/quorum. Counting only elapsed epochs — whose buckets are immutable forever — keeps voting deterministic with no change to `FeeRouter`.

Two effects follow, both bounded by one epoch (≤ 1 week):

- **Served-byte / quorum lag.** The current in-progress epoch's bytes are excluded from vote weight and from the quorum denominator until that epoch elapses.
- **Slash-immediacy lag for same-epoch slashes.** A slash stamped in the in-progress epoch does not zero a proposal snapshotted earlier in that same epoch; it takes effect once the epoch elapses (the slash window tracks the byte window — both end at `endEpoch(t)`). Slashes from prior epochs zero immediately.

## Context

Declared capacity (`CapacityBond.declaredMbps`) is operator-asserted at registration and only loosely tied to actual delivery. Keying DAO vote weight primarily on it would let a deeply-bonded but lightly-serving operator carry weight that does not reflect their real contribution to the network. But keying it purely on delivered bytes, with no reference to declared capacity, would let a lightly-bonded operator inflate its own weight by self-dealing byte volume — serving traffic to accounts it controls — without committing bonded capacity. Delivered bytes drive vote weight; declared capacity caps it per epoch.

Served bytes arrive at the governance-chain `FeeRouter` as the bytes feed in the hook data of each sweep ([ADR 016 § Contract: FeeRouter](016-contract-interactions.md#contract-feerouter)). The router writes `bytesPerEpoch[operator][epoch]` from it. This counter is the canonical voting-weight source.

The weight rules touch only governance: they leave payment-pool mechanics ([ADR 003](003-payments.md#adr-003-payment-model)), the fee split ([ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split)), the capacity-bond curve ([ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve)), `CapacityBond`'s registration / slashing semantics, and `SlashAppeal` / `BuybackBurner` unchanged. `CapacityBond` remains the registration gate and the source of `firstBondedAt` for the age-ramp.

## Decision

DAO vote weight is the trailing-window sum of an operator's served bytes, multiplied by the existing tenure ramp, with the existing per-operator cap applied against the bytes-weighted total.

### Formula

```
vote_weight(op, t) = min(
    capped_served_window(op, t),
    voteCapBps × others_bytes_window(op, t) / (10_000 − voteCapBps)
) × age_ramp(op, t)

others_bytes_window(op, t) = total_bytes_window(t) − Σ_{e = endEpoch(t)-N+1 .. endEpoch(t)} FeeRouter.bytesPerEpoch[op][e]

capped_served_window(op, t) = Σ_{e = endEpoch(t)-N+1 .. endEpoch(t)} min(
    FeeRouter.bytesPerEpoch[op][e],
    CapacityBond.declaredMbpsAtEpoch(op, e) × epochLength × 125_000
)

total_bytes_window(t)      = Σ_{e = endEpoch(t)-N+1 .. endEpoch(t)} FeeRouter.totalBytesPerEpoch[e]

age_ramp(op, t) = min(
    (t - CapacityBond.firstBondedAt[op]) / (age_ramp_months × seconds_per_month),
    1.0
)

epoch(t)    = uint64(t / EPOCH_LENGTH)                  // EPOCH_LENGTH = 1 week, immutable
endEpoch(t) = epoch(t) == 0 ? ∅ : epoch(t) - 1          // last fully-elapsed epoch; ∅ ⇒ weight 0
N           = windowEpochs                               // default 13 (~1 quarter)
```

Where `t` is the OpenZeppelin Governor timepoint (timestamp clock per ERC-6372, consistent with [ADR 009 § Production](009-governance.md#production-operator-weighted-dao-governance)).

Each epoch's counted bytes are capped at what the tier the operator declared at that epoch's close could physically deliver: `declaredMbpsAtEpoch(op, e)` samples `CapacityBond`'s checkpointed `declaredMbps` history at `(e+1) × epochLength − 1`, the last instant of epoch `e`. `125_000 = 1e6 / 8` converts megabits/s to bytes/s, so `declaredMbpsAtEpoch(op, e) × epochLength × 125_000` is the maximum bytes the declared tier could deliver across the whole epoch. Because the cap reads checkpointed history rather than the live `declaredMbps` value, a later `declareMbps` call cannot raise the cap for epochs that have already elapsed.

### Behaviors that follow from the formula

- **A registered operator with zero served bytes in the trailing window has zero vote.** The bond gates eligibility to vote; it does not directly grant weight.
- **Declared capacity caps vote weight per epoch; it never grants weight.** An operator's counted bytes for epoch `e` cannot exceed what `declaredMbpsAtEpoch(op, e)` could physically deliver over that epoch. This is a deliberate consequence: an operator that declared zero capacity in an epoch contributes zero vote weight for that epoch, however many bytes it served.
- **A fresh operator who serves heavily on day 1 still ramps in over `age_ramp_months`.** `age_ramp` is the tenure-buy-in defense; it stays defense-in-depth on top of bytes.
- **Four `FeeRouter` checks gate every sweep.** Each check blocks one failure.
  - *Active at arrival.* The rebate and the weight credit go to the operator only if `CapacityBond.isActive(operator)` is true when the sweep arrives. Otherwise the rebate goes to the treasury and the credit is 0. The USDC is never lost. An unbonded address cannot raise the quorum total.
  - *USDC-backed credit.* `credit = min(bytes, net × 1 MiB / deliveryFloor)`. Bytes without USDC behind them earn no weight.
  - *Capacity clamp.* The credit for an operator in one epoch cannot exceed `CapacityBond.declaredMbps(operator) × EPOCH_LENGTH × 125_000` minus what `bytesPerEpoch[operator][epoch]` already holds. The clamp binds the operator's counter and the network total alike, so a byte that no declared capacity could carry enters neither. The total sets quorum, proposal threshold and every operator's share cap, so pumping it needs declared capacity, which needs bond. At 1 Gbps an operator credits at most about 75 TB per epoch. The Governor's per-epoch minimum stays. It reads the tier declared at the epoch's close.
  - *Arrival-epoch stamping.* The credit goes to the epoch of the arrival block (`block.timestamp / EPOCH_LENGTH`). A late sweep never changes a closed epoch, so proposal snapshots stay immutable.
- **Tenure starts at the first `minBond` crossing.** `firstBondedAt` records the first time the operator's active bond reached `minBond`. A dust bond below `minBond` does not start the ramp, so an operator cannot pre-age an address for the cost of one base unit.
- **Per-operator cap is computed against the bytes-weighted total at the same timepoint**, not against any historical or capacity-derived total. The cap clamp applies pre-multiplication by `age_ramp`.
- **`quorum(t)` and `proposalThreshold(t)` use `FeeRouter.totalBytesInWindow(endEpoch(t), N)` as the denominator — an *upper-bound proxy* for `Σ_op vote_weight(op, t)`, not the exact sum.** The exact sum applies the per-operator cap and the `age_ramp` multiplier (both `≤ 1`), so `Σ_op vote_weight ≤ totalBytesInWindow` always. Calibrating quorum against the proxy is intentionally conservative — it makes quorum strictly harder to reach than against the true Σ — and avoids the gas of summing per-operator capped contributions on every `castVote`. The proxy is exact when no operator is above the cap and all operators are past `age_ramp_months` of tenure (the steady state).

### Slashing zero-out

Every `CapacityBond.slash` appends a `SlashRecord` (with its `slashedAt` timestamp and escrow `status`) to the operator's slash list. The Governor's `_getVotes(op, t)` returns zero whenever the operator has a **standing** slash — one NOT in the `Reversed` state (`Escrowed` / `AppealOpen` / `Upheld` all still stand) — with `slashedAt` in `[(endEpoch(t) - N + 1) × epochLength, t]`. The lower edge is the start of the trailing byte window. The upper edge is the timepoint itself, so a slash in the in-progress epoch zeroes every snapshot taken after it, and a slash after `t` never zeroes that snapshot. Once the window slides past the slash, the operator's vote weight recovers based on their forward served-bytes accrual.

The Governor walks the operator's slash list newest first through `operatorSlashCount`, `operatorSlashIdAt` and `getSlashRecord`, and stops at the first slash before the window. Every record counts, so a later slash cannot hide an earlier in-window one. On a **granted** slash appeal via [ADR 028 § Contract surface](028-slashing-appeals.md#contract-surface)'s `grantAppeal` path, `CapacityBond.settleAppealGranted` marks that record `Reversed`, so it no longer zeroes the vote while any other standing slash still does. An **upheld** appeal (`upholdAppeal` / `rejectAppeal`) leaves the record standing. The walk is bounded in practice because a slashed operator auto-ejects below `minBond / 2`.

`CapacityBond` also keeps `slashedAtEpoch[op]`, the max epoch among the operator's standing slashes plus one (0 for "unslashed"), as an indexer read. The Governor does not read it.

This restores the immediate-vote-removal-on-slash signal that the bytes window alone cannot deliver (an active operator who is slashed today would otherwise continue voting with their accumulated window bytes for up to N weeks).

### Contracts and surface

The full Solidity surface is documented in [ADR 016 § Contract: FeeRouter](016-contract-interactions.md#contract-feerouter) and [ADR 016 § Contract: CapacityBond](016-contract-interactions.md#contract-capacitybond). Highlights:

**Governance-chain `FeeRouter` counters:**

- `mapping(uint64 => uint256) totalBytesPerEpoch` — incremented on each sweep alongside the per-operator counter (one warm-slot SSTORE after the first sweep in the epoch).
- `function totalBytesPerEpoch(uint64 epoch) external view returns (uint256);` — public getter, mirrors the per-operator getter.
- `function bytesInWindow(address op, uint64 endEpoch, uint64 N) external view returns (uint256);` — O(N) trailing-sum helper.
- `function totalBytesInWindow(uint64 endEpoch, uint64 N) external view returns (uint256);` — global O(N) trailing-sum helper. Co-locating the loop with the storage keeps `windowEpochs` as a single governance-mutable source of truth and makes `_getVotes` a constant number of external calls.
- `uint64 public windowEpochs;` — storage; constructor takes `windowEpochsDefault`.
- `function setWindowEpochs(uint64 n) external;` — `GOVERNANCE_ROLE`, bounded `[4, 26]` (see [§ Governable parameters](#governable-parameters-with-safety-bounds)).

**`CapacityBond` additions:**

- `mapping(address => uint256[]) _operatorSlashIds;` — per-operator slash index, read through `operatorSlashCount(op)` / `operatorSlashIdAt(op, i)`.
- `function getSlashRecord(uint256 slashId) external view returns (SlashRecord memory);` — the record with its `slashedAt` and `status`, read by the Governor's slash walk.
- `mapping(address => uint64) slashedAtEpoch;` and its getter — the max epoch among the operator's standing slashes plus one, recomputed by the `grantAppeal` flow (`settleAppealGranted` → `_recomputeSlashedAtEpoch`) per [ADR 028](028-slashing-appeals.md#contract-surface). An indexer read; the Governor does not use it.

**`DecdnGovernor._getVotes`** (pseudocode):

```solidity
function _getVotes(address op, uint256 timepoint, bytes memory) override returns (uint256) {
    // endEpoch is the last fully-elapsed epoch; cur == 0 ⇒ no window yet.
    uint64 cur = uint64(timepoint / EPOCH_LENGTH);
    if (cur == 0) return 0;
    uint64 endEpoch = cur - 1;
    uint64 windowStart = endEpoch + 1 > windowEpochs ? endEpoch + 1 - windowEpochs : 0;
    // Any standing slash in [window start, timepoint] zeroes the vote. Walk
    // newest first; stop at the first slash before the window.
    uint256 windowStartTime = windowStart * EPOCH_LENGTH;
    for (uint256 i = capacityBond.operatorSlashCount(op); i > 0; i--) {
        SlashRecord memory r = capacityBond.getSlashRecord(capacityBond.operatorSlashIdAt(op, i - 1));
        if (r.slashedAt < windowStartTime) break;
        if (r.slashedAt <= timepoint && r.status != SlashStatus.Reversed) return 0;
    }
    uint256 served = feeRouter.bytesInWindow(op, endEpoch, windowEpochs);
    uint256 total  = feeRouter.totalBytesInWindow(endEpoch, windowEpochs);
    uint256 others = total - feeRouter.bytesInWindow(op, endEpoch, windowEpochs);
    uint256 cap    = (others * voteCapBps) / (10_000 - voteCapBps);
    uint256 capped = served < cap ? served : cap;
    uint256 ramp   = ageRampScaled(capacityBond.firstBondedAt(op), timepoint, ageRampMonths);
    return (capped * ramp) / 1e18;
}
```

`quorum(t)` and `proposalThreshold(t)` use `feeRouter.totalBytesInWindow(endEpoch(t), windowEpochs)` × 4% / 0.1% respectively (both return 0 before the first epoch elapses). The capped-and-ramped total weight (not raw bytes) is the strictly correct denominator, but is O(active_operators × N) to compute; the Governor uses the unramped total bytes as a tractable upper bound and accepts the resulting quorum / threshold conservativeness. The total holds no share cap, but `FeeRouter` clamps each operator's epoch bytes to its declared capacity before they enter it.

The shipped `CapacityBond` exposes no `totalVotingWeightAt(ts)` aggregate getter. Under this ADR the Governor derives total voting weight from FeeRouter epoch accounting (`totalBytesInWindow`), not from a CapacityBond aggregate read.

`IVotes`/IERC-5805 is not used: voting weight is derived from FeeRouter epoch accounting, not from per-account checkpoint structures, so `GovernorVotes` / `GovernorVotesQuorumFraction` are unused.

### Governable parameters with safety bounds

| Parameter           | Default | Min | Max  | Rationale |
| ------------------- | ------: | --: | ---: | --------- |
| `windowEpochs` (N)  |      13 |   4 |   26 | <4 (1 month) too reactive to single-burst wash-trading and statistically thin for small operators; >26 (6 months) lags actual operator-set composition and pushes `_getVotes` toward ~55K gas of cold SLOADs per voter per `castVote` |
| `deliveryFloor`     |       1 |   1 | 1000 | Net USDC micro-units per MB of weight, on the governance-chain `FeeRouter`. Prices each credited byte; see [§ Wash-trading as vote-buying](#wash-trading-as-vote-buying) |
| `voteCapBps`        |   1000  | 100 | 1000 | Launches at the 10% ceiling and is decrease-only: governance can lower it toward the 1% floor but never raise it, so the cap only ever decentralizes vote weight. A higher launch value keeps quorum reachable while the operator set is thin. Cap applied against bytes-weighted total |
| `age_ramp_months`   |      6  |   1 |   24 | Carry forward from [ADR 009](009-governance.md#governable-parameters-with-safety-bounds) and [ADR 026 § Governable parameters](026-tokenomics.md#governable-parameters-with-safety-bounds) |

Cross-parameter invariant (informational, not enforced at the contract layer): `windowEpochs ≤ age_ramp_months × 4.33` keeps the age-ramp horizon ≥ the bytes window. Enforcement at the contract layer would over-constrain governance flexibility and is not warranted; document the invariant and let governance honor it.

## Threat Model

### Wash-trading as vote-buying

Bytes-weighted voting is gameable by operators self-paying for delivery. An attacker who controls a client wallet pays themselves to serve bytes. The operator share returns to the attacker on the payment chain. The rebate returns on the governance chain, so it is not a cost. The attacker's real cost is `net`: the USDC that stays with the protocol (buyback and treasury) after each sweep ([ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split)).

The governance-chain `FeeRouter` enforces the deterrent in its credit formula, for any pool on any chain: `credit = min(bytes, net × 1 MiB / deliveryFloor)`. Each credited byte costs at least `deliveryFloor` micro-units of `net`. A sweep that carries more bytes than its `net` pays for earns no extra weight. A pool with a low or zero protocol share earns little or no weight. A forged pool must pay real USDC at a price that only governance sets.

**Attack cost to reach the 10% cap from zero.** Let `R` be the network-wide USDC revenue per epoch and `s` the fraction of each redemption that becomes `net`. An attacker holding `cap` share of the trailing-window bytes generates `R × cap × N` revenue across the window (assuming the attacker pays the *market* per-byte rate). Wash-trading cost is `s × R × cap × N` in attacker-paid USDC across the window. With the steady example setting of [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split), `s = 0.40`. At `R = $25K/week`, `cap = 10%`, `N = 13`: the attacker burns `0.40 × $25K × 0.10 × 13 ≈ $13,000` per quarter to hold full 10% vote. With the launch example setting, `s = 0.10` and the cost is `≈ $3,250` per quarter.

This figure is an **upper bound on a naive attacker**. A cost-minimizing attacker pays at the `deliveryFloor` rate, not the market rate. Raising `deliveryFloor` toward the market rate raises the attack cost toward the figure above, at the price of constraining legitimate low-rate pricing. This is a governance trade-off.

If `net` is zero (`buybackBps` and the treasury remainder both zero), the router mints no new weight. When the trailing window empties, quorum and proposal threshold reach 0. Governance can reverse this setting.

Cost scales linearly with network revenue and with the cap. Defenses:

1. **10% per-operator cap (`voteCapBps`).** Bounds the maximum vote any single attacker can buy. The cap is sized from the other operators' bytes, so an operator's own bytes, self-dealt or not, never raise its own cap: at the cap its weight is exactly `voteCapBps` of (others + cap). The cap launches at 10% and is decrease-only within `[1%, 10%]`; lowering it toward 1% is the first tightening lever if attacks materialize.
2. **`age_ramp` floor.** A brand-new bonded operator who wash-trades heavily still votes at a fraction of their bytes-weighted share for the first `age_ramp_months` months. Combined with the cap, this limits the speed-to-influence of a fresh attacker.
3. **Rolling-window decay.** Sustained wash trading is required across the full window to maintain the cap; a stopping attacker decays out over N epochs.

The attack is bounded but not eliminated. Future tightening options (deferred to a follow-up ADR if observed): raise `buybackBps` or `deliveryFloor`, gate vote weight on a probe-verified delivery threshold, introduce a non-linear wash-trade-detection penalty. The current parameters are calibrated for the testnet-and-early-mainnet regime where attacker cost-of-capital is high relative to expected governance return.

### Instant-governance via bond-then-serve-burst

Defended by `age_ramp`. A fresh operator who bonds at `t=0` and serves the entire network's traffic at `t=0+ε` votes at `(t / age_ramp_months × seconds_per_month)` of full weight for the first `age_ramp_months` months. With default `age_ramp_months = 6`, day-1 vote weight is ≈ 0.5% of full weight; week-1 is ≈ 3%. This forces sustained activity across both the bytes axis (rolling window) and the time axis (ramp).

### Slashed-but-still-voting

Defended by the slashing zero-out. Without zero-out, a slashed operator continues voting with their accumulated window bytes for up to N weeks. With zero-out, slashing immediately revokes vote weight for the remainder of the window. This is a tighter response than relying on the declared-capacity cap alone, which would only reduce vote weight by the bond-reduction ratio.

### Concentration

The per-operator cap (`voteCapBps`, launch 10%) is the primary concentration defense. Real delivery skews power-law in CDN markets, so bytes-weighting concentrates more readily than capacity-weighting. The cap launches at its 10% ceiling and is decrease-only within `[1%, 10%]`; governance can tighten it if observed concentration warrants but can never re-concentrate weight. Lowering the cap is preferable to lowering `windowEpochs`, because window-shortening would make governance more vulnerable to burst-traffic manipulation.

## Consequences

### Positive

- **Voting weight tracks demonstrated network contribution.** An operator who isn't serving has no vote; an operator who is serving heavily has weight proportional to that service (up to the cap).
- **Reuses the sweep data flow.** The bytes feed already rides each sweep. Marginal contract surface is two integer mappings, one trailing-sum helper, one tenure-ramp lookup.
- **Strengthens skin-in-the-game story.** Combined with [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split)'s per-byte operator payment, governance influence and economic upside both scale with actual delivery.
- **Slashing-zero-out tightens response time.** Slash → immediate vote zero, recovers naturally over `windowEpochs`. Cleaner than a declared-capacity-proportional reduction.

### Negative

- **Wash-trading is bounded but not zero-cost prevented.** The `net` share of attacker-paid USDC (buyback and treasury) is forfeit per cycle. At cap floor (1%) and default parameters, attacker cost is non-trivial but not unaffordable. See [§ Threat Model — Wash-trading](#wash-trading-as-vote-buying).
- **Vote weight is more volatile than capacity-weighted weight.** A heavy traffic week ramps an operator's vote in days; a quiet quarter decays it out. Governance proposers cannot assume a fixed voting set; quorum computations must re-read the FeeRouter window state.
- **No on-chain aggregate vote-weight getter.** `CapacityBond` exposes no `totalVotingWeightAt(ts)`; indexers and dashboards must compute totals from the FeeRouter-derived window (`totalBytesInWindow`) rather than a single contract read.
- **A governance-chain `FeeRouter` replacement resets vote weight.** `epochLength` is constructor-immutable per [ADR 016 § No proxy deployment patterns](016-contract-interactions.md#no-proxy-deployment-patterns), so a replacement `FeeRouter` starts with empty `bytesPerEpoch`. Vote weight is zero for all operators until sweeps refill the window. Pools name their `FeeRouter` as an immutable, so each pool deployment also names the replacement.
- **Indexer / off-chain reader impact.** Dashboards track the router's sweep-receipt event and keep a running `totalBytesPerEpoch` aggregate. Minor.

### Risks

- **Network in low-revenue phase has cheap attack economics.** At `R = $5K/week`, wash-trading the 10% cap costs only ~$2,600 / quarter. Governance should be prepared to tighten `voteCapBps` toward 1% during the bootstrap-multisig phase if observed.
- **Operator with one large publisher could naturally exceed cap.** A legitimate edge-tier operator serving a single high-volume publisher hits the 10% cap easily. They are capped at 10% vote weight — same as a wash trader — which is the design intent (concentration defense applies uniformly). The cap should not be confused with a punishment for honest top carriers.
- **`age_ramp` and `windowEpochs` interaction window.** A new operator who serves heavily in their first month builds a saturated bytes window before their age-ramp completes. The multiplicative combination still gates them under both axes, but the per-axis behavior diverges from intuition. Document for governance proposers.

## Cross-ADR Impact

- **[ADR 009 — Governance Model](009-governance.md#adr-009-governance-model):** §Production: Operator-Weighted DAO Governance sources voting weight from this ADR's formula, with its clock anchored to governance-chain `FeeRouter` epoch accounting and its quorum calibration reading `FeeRouter.totalBytesInWindow`. Its Governable Parameters table carries `windowEpochs`, and its Consequences > Negative notes wash-trading-as-vote-buying.
- **[ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md#adr-016-smart-contract-interaction-model):** Contract Inventory `DecdnGovernor` row reads `FeeRouter` for vote weight. `classDiagram` `FeeRouter` exposes `totalBytesPerEpoch`, `bytesInWindow`, `totalBytesInWindow`; the `Governor` vote-weight edges target `FeeRouter`. `Contract: FeeRouter` section carries the sweep receiver, the `bytesPerEpoch` / `totalBytesPerEpoch` mappings, getters, `deliveryFloor` and `setWindowEpochs`. `Contract: CapacityBond` section carries `slashedAtEpoch`. Cross-Contract Call Graph + Complete Call Table list `Governor → FeeRouter: bytesInWindow / totalBytesInWindow`. Deployment Order step 14 (`DecdnGovernor`) lists `FeeRouter` as a non-zero constructor input.
- **[ADR 026 — Tokenomics](026-tokenomics.md#adr-026-tokenomics):** §Governance states the served-bytes `vote_weight` formula and points its `Voting source` row at `FeeRouter` + this ADR. §Governable parameters with safety bounds carries `windowEpochs`.
- **[ADR 028 — Slashing Appeals](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation):** the `grantAppeal` path (`settleAppealGranted`) clears `CapacityBond.slashedAtEpoch[op]` alongside refunding the operator's escrowed TOKEN (see [ADR 028 § Contract surface](028-slashing-appeals.md#contract-surface)); no other appeal-flow change.

## References

- [ADR 003 — Payment Model](003-payments.md#adr-003-payment-model)
- [ADR 009 — Governance Model](009-governance.md#adr-009-governance-model)
- [ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md#adr-016-smart-contract-interaction-model)
- [ADR 026 — Tokenomics](026-tokenomics.md#adr-026-tokenomics)
- [ADR 028 — Slashing Appeals](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation)
