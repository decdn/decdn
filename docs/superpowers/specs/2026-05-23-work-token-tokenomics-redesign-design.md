# Work-Token Tokenomics Redesign — Design Spec

**Status:** Draft (brainstorming output, awaiting user review).
**Supersedes (if adopted):** ADRs 026, 034, 035. Substantial edits to 009 and 016. Minor edits to 003, 028, 032.

## Summary

Replace the current Curve-style ve-gauge tokenomics (ADR 026 + 033 + 034 + 035) with a **work-token model** modeled on Livepeer / Helium / Filecoin: every node operator must bond TOKEN proportional to the bandwidth capacity they serve, in a single capacity-gated contract. No passive yield to holders. Governance is operator-only.

The redesign is motivated by four user-stated priorities: (i) smaller contract surface, (ii) strong token value accrual, (iii) operator decentralization, (iv) regulatory defensibility. Work-token hits all four; the current ve-gauge design trades (iv) against (ii)–(iii) by routing real yield to passive ve-lockers via the delegator pool.

The redesign is net *subtractive* in code: two contracts (`VotingEscrow`, `DelegatorBuyer`) are deleted, `FeeRouter` simplifies from six buckets to four with no epoch / claim / snapshot machinery, and `StakingRegistry` is renamed to `CapacityBond` with one added piece of capacity-curve logic. The `SafetyReserve`, slashing primitive, payment channels, and POL/BuybackBurner all carry over largely unchanged.

## Motivation and design objectives

The brainstorming session named four objectives, all weighted equally:

1. **Simplicity / smaller contract surface.** ADR 026's design splits into four cooperating contracts (`FeeRouter`, `VotingEscrow`, `SafetyReserve`, `DelegatorBuyer`) with epoch-bucket accounting, pull-claim windows, ve-snapshot reads, and a one-shot pre-launch gauge cutover. The audit surface and operational complexity are non-trivial.
2. **Token value accrual.** TOKEN demand must scale with network usage. ADR 026 layers three prongs (gauge lock, delegator buyback, burn) for this.
3. **Operator decentralization.** Structurally favor a diverse operator set over a few large operators.
4. **Regulatory defensibility.** Avoid framing that looks like an investment contract under the Howey test — particularly the "solely from the efforts of others" prong.

Objectives (2) and (4) are in tension in the current design: the delegator pool's purpose is precisely to route TOKEN to passive lockers, which is the kind of cashflow-rights claim that weakens (4). Work-token resolves the tension by making the lock *itself* the work-input rather than a separately-yielding instrument.

## Section 1. Core mechanism — `CapacityBond`

Every operator must bond TOKEN proportional to the bandwidth capacity they claim. The bond is the only TOKEN-side requirement on operators — no separate flat minimum stake, no optional ve-lock for additional yield.

### Bond lifecycle

- `CapacityBond.register(declaredMbps)` deposits the bond and emits `CapacityClaimed(operator, Mbps)`.
- The probe service samples the operator over a 7-day window using the existing `cdn/probe/v1` ALPN (ADR 013). If 95th-percentile sustained delivery rate falls below `min_delivery_ratio × declaredMbps` (default 70% — operator must deliver at least 70% of declared capacity), registration auto-reverts the bond minus a fixed probe-cost fee (~50 TOKEN) deposited to the treasury.
- Re-registration at a lower tier is permitted at any time; re-registration at a higher tier requires a new probe window.
- Unbonding window: 14 days, slashable during unbonding. Longer than ADR 026's 7-day window because capacity-claim verification overlap is the binding consideration here, not pure security.

### What the bond grants

- The right to register as an active operator at the declared capacity tier.
- 100% of the operator share of the fee split (60% — see Section 3).
- Governance voting weight (Section 4).

### What the bond does NOT grant

- No yield. No gauge boost. No "working_bytes" formula. No ve-balance time decay. No DelegatorBuyer pipeline. No epoch-snapshot accounting. None of that contract surface exists in the redesigned system.
- No optional locking for additional yield. The bond is binary: lock to operate; don't lock to not operate.

### Slashing

- Same tier escalation (5% / 15% / 50%) and lifetime offense counter as ADR 026 § Slashing and burn, applied to the `CapacityBond` instead of a separate stake.
- Distribution unchanged: 50% challenger / 30% SafetyReserve / 20% burn.
- Auto-ejection at 50% of minimum bond for the operator's declared tier replaces ADR 026's "50% of minimum stake."

### Capacity-shortfall slashing (new — replaces wash-trade defense)

- If 4-week rolling verified delivery is below `min_delivery_ratio × declared_capacity`, the operator is auto-downgraded to the next lower tier; the bond delta (current_tier_bond − new_tier_bond) is forfeit to SafetyReserve.
- Deterministic on probe data; no governance vote needed. Probe data is on-chain by virtue of the existing `cdn/probe/v1` attestation flow (ADR 013).
- `min_delivery_ratio` default 70% (operator must deliver ≥70% of declared), governable within [50%, 90%]. Lower values are more forgiving (allow more shortfall); higher values are stricter.
- This obviates the need for ADR 034's per-operator gauge-share cap as a wash-trading defense: you cannot inflate apparent network share by faking traffic, because actual delivery is measured against your declared tier.

## Section 2. Lock-to-capacity curve

The decentralization lever lives in the curve shape: super-linear bond requirement makes high-capacity operators pay more per Mbps.

### Curve

```
bond_required(Mbps) = k × Mbps^α
```

| Parameter | Default | Governable range | Notes |
|---|---:|---|---|
| α (exponent) | 1.2 | [1.0, 1.8] | 1.0 = linear (no decentralization pressure); 2.0 = quadratic |
| k (bond constant, TOKEN) | 12.6 | bounded by 1G tier ∈ [10K, 200K TOKEN] | Picked so `bond_required(1000) ≈ 50,000 TOKEN` |
| `MAX_CAPACITY_PER_OPERATOR` | 200 Gbps | [50, 1000] | Prevents one operator cornering edge-tier capacity |
| `min_delivery_ratio` | 70% | [50%, 90%] | Min delivery as fraction of declared; see Section 1 |

### Worked numbers at α=1.2, k=12.6

| Tier | Capacity | Bond | Bond per Mbps | Ratio vs 1G |
|---|---:|---:|---:|---:|
| Entry | 1 Gbps | 50,000 TOKEN | 50 | 1.0× |
| Mid | 10 Gbps | 795,000 TOKEN | 80 | 1.6× |
| Edge | 100 Gbps | 12,600,000 TOKEN | 126 | 2.5× |

The 2.5× per-Mbps ratio at α=1.2 matches ADR 026's gauge-boost ratio (`1 / boostFloor = 1 / 0.4 = 2.5×`). Decentralization pressure is preserved; the mechanism just shifts from yield-haircut-to-not-lock (ADR 026) to capital-cost-to-operate (this design). Picking k and α this way makes the redesign neutral on decentralization, strictly better on simplicity and regulatory defensibility.

### Supply impact at S2-scale buildout

Assumed reference distribution: 100 × 1G ops + 30 × 10G ops + 5 × 100G ops.

```
100 × 50,000     =   5,000,000 TOKEN
 30 × 795,000    =  23,850,000 TOKEN
  5 × 12,600,000 =  63,000,000 TOKEN
                 = ~91.85M TOKEN bonded
```

At 1B total supply, that's ~9% bonded at S2. At S3 (10× scale) ~30%. Matches ADR 026's target ve-lock rate of 30–50% as the natural mechanical equivalent.

## Section 3. Revenue split — four buckets, not six

`FeeRouter.routeSettlement(operator, bytesDelivered, amount)` splits incoming USDC across four buckets, all transferred in the settlement transaction.

| Destination | Share | Δ vs ADR 026 | Mechanic |
|---|---:|---|---|
| Operator base (direct, per-byte) | 60% | +20pp (was 40%) | Same-tx USDC transfer to operator |
| Buyback-and-burn | 25% | +20pp (was 5%) | TWAP USDC→TOKEN via Balancer V3 80/20 (ADR 018); TOKEN burned |
| Protocol treasury | 10% | +5pp (was 5%) | Same-tx to Timelock-custodied wallet |
| Safety & insurance reserve | 5% | +2pp (was 3%) | Same-tx to `SafetyReserve` (ADR 033) |
| **Total** | **100%** | | |

### What's deleted from the contract surface

- ❌ Gauge pool bucket (40%) — folded into operator base + burn.
- ❌ Delegator pool bucket (7%) + `DelegatorBuyer` contract entirely.
- ❌ Per-epoch gauge accumulator + ve-snapshot machinery in `FeeRouter`.
- ❌ `claimBoost(epochs[])` / `claimDelegator(epochs[])` pull-claim paths.
- ❌ Pre-launch gauge accumulator + `enableGauge()` one-shot setter.
- ❌ 26-epoch claim window, gauge-share cap, working_bytes formula, boostFloor parameter.

### What stays

- ✅ `BuybackBurner` (ADR 018) unchanged, just receives 5× more flow.
- ✅ `SafetyReserve` (ADR 033) unchanged.
- ✅ Treasury, slashing, payment channels (ADR 003), POL (ADR 018).
- ✅ Per-epoch bytes-delivered counter retained for analytics only; no settlement logic depends on it.

### Same-transaction guarantees

All four buckets transfer in the settlement transaction. There are no epoch buckets, no pull-based claims, no claim windows. `FeeRouter.routeSettlement` does its full work in one tx, recovering the [ADR 003 § FeeRouter Integration](../../../adr/003-payments.md) one-tx invariant for every bucket.

### Operator-aligned share = 85%

60% direct + 25% burn (the burn benefits every bond-holder by raising TOKEN's mechanical demand). Versus ADR 026's 80% (40% direct + 40% gauge); alignment is *higher* under work-token because burn benefits all bond-holders, whereas the gauge pool benefited only the ve-weighted subset.

### Value accrual mechanism

The redesigned model has two prongs replacing ADR 026's three:

- **Capacity-growth lock demand.** Every new operator or tier upgrade is a new buyer of TOKEN to bond. The demand is mechanically tied to network capacity growth, not to a promise of yield.
- **Burn at 5× current rate.** 25% of fees vs ADR 026's 5%. Direct deflationary pressure.

The first prong is mechanically larger at scale than any of ADR 026's individual prongs; the second is a direct 5× multiplier on the deflationary lever. Net token-demand pull is at least as strong as the current three-prong design.

### Governable bounds

| Parameter | Default | Min | Max |
|---|---:|---:|---:|
| Operator base share | 60% | 40% | 90% |
| Burn share | 25% | 5% | 50% |
| Treasury share | 10% | 0% | 30% |
| Safety share | 5% | 0% | 20% |

Sum-to-100% enforced on every governance update. The 40% floor on operator base preserves the ADR 026 cashflow invariant (operators always receive enough liquid USDC to cover infrastructure costs).

## Section 4. Governance — operator-only, capacity-weighted

This is the redesign's strongest commitment, with the largest political cost.

### Voting weight

```
vote_weight(operator) = declared_capacity_Mbps × age_ramp(months_bonded)
age_ramp = min(months_bonded / 6, 1.0)
```

- Fresh bonds vote at zero; full weight at 6 months; half-weight at 3 months.
- Defends against "buy your way to instant governance" attacks (a hostile party cannot register a fleet of operators and immediately vote them).
- Vote weight scales with capacity (Mbps), not bond size. At α=1.2 raw bond would concentrate voting in edge-tier ops at ~252:1 vs entry-tier; capacity-based gives ~100:1, matching ADR 026's ve-linear concentration shape.
- **Per-operator voting cap = 5% of total voting weight.** Direct mirror of ADR 026's gauge-share cap, repurposed for governance.

### Governance parameters

| Parameter | Value | Source |
|---|---|---|
| Quorum | 4% of total voting weight | matches ADR 026 |
| Proposal threshold | 0.1% of total voting weight | matches ADR 026 |
| Voting delay | 1 day | matches ADR 009 |
| Voting period | 7 days | matches ADR 009 |
| Timelock | 48 hours | matches ADR 009 |
| Total latency | ~10 days | matches ADR 026 |

### Non-operator TOKEN holders have ZERO voting weight

Seed backers (22%), team (17%), treasury (25%), POL holders (10%) — all hold TOKEN, but **none can vote** unless they also bond it to operate.

**Why this is right:**

- The moment seed/team get to vote on bond parameters, fee splits, etc., the regulatory framing weakens (now there's a "common enterprise" voting on profit-like decisions).
- Operator-only governance is the cleanest regulatory posture, mirroring how Filecoin's storage-provider class and Helium's hotspot class are the load-bearing voting constituency in those networks.
- It enforces the work-token framing structurally rather than rhetorically.

**The political cost:**

- Seed investors and team members lose direct governance say. This needs to be agreed up-front in term-sheet renegotiation.
- Mitigation: any TOKEN holder who *also* operates a node votes like any other operator. Seed/team are not excluded from governance — they're excluded from *passive* governance.

### Bootstrap governance — temporary multisig phase

The voting set is narrow at launch (likely <50 operators in the first 6–12 months). Direct application of capacity-weighted governance pre-bootstrap risks hostile takeover via a cheap operator-fleet setup.

- For the first 6–12 months, governance runs through a multisig with hard-cap pause powers (extends ADR 009's emergency-multisig pattern).
- Transition to full operator-weighted governance is auto-triggered when **active operator count ≥ 30** AND **total declared capacity ≥ 100 Gbps**. Both thresholds governable.
- Before transition, the multisig can execute parameter changes within the safety bounds in Sections 2 and 3.

### Delegation

- Operators may delegate voting weight to another address via EIP-712 signed delegation (Governor Bravo pattern). The bond itself cannot be delegated — only the voting power.
- This is the *only* mechanism by which a non-operator address gains vote weight, and it requires an operator's explicit signature.

## Section 5. Bootstrap and supply distribution

The redesign needs more liquid TOKEN at genesis (operators must acquire to bond) and a new bucket to channel the $1M+ pre-seed USDC subsidy into operator-bond growth.

### Revised allocation (1B fixed)

| Bucket | Share | Δ vs ADR 026 | Vesting / mechanic |
|---|---:|---|---|
| Protocol treasury | 25% | −5pp | 4-year linear |
| Seed backers | 22% | −2pp | 3-year linear, 6mo cliff |
| Team & core contributors | 17% | −2pp | 4-year linear, 12mo cliff |
| Community & ecosystem | 12% | −3pp | 4-year linear |
| Genesis liquidity (POL) | 10% | unchanged | Balancer V3 80/20 (ADR 018), genesis-liquid |
| Public sale / airdrop | 2% | unchanged | Genesis-liquid |
| **Operator bootstrap (NEW)** | **12%** | **+12pp** | Vested-into-bond via service delivery; cannot be sold pre-bond |
| **Total** | **100%** | | |

### The Operator bootstrap mechanism (12% = 120M TOKEN)

1. Operator registers at the entry tier (1 Gbps) with their own bond (50K TOKEN, buyable from POL or sale at genesis).
2. After each month of verified service (probe-attested), the operator earns a TOKEN grant proportional to delivered bytes × capacity tier × a diminishing-returns curve.
3. The grant is auto-deposited into the operator's `CapacityBond`. The operator cannot withdraw it as liquid TOKEN — only as upgraded capacity claim (which then requires probe verification).
4. Total grants per operator capped at `bond_required(next_tier) − bond_required(current_tier)`. The bootstrap pays for tier-upgrade *deltas*; the operator self-paces.
5. Program terminates when 120M TOKEN exhausted, or by governance vote after the network reaches the same thresholds that trigger the governance transition (active operator count ≥ 30 AND total declared capacity ≥ 100 Gbps).

### Pre-seed USDC ($1M+) deployment

| Use | Approx allocation | Notes |
|---|---|---|
| Operator infrastructure subsidies (direct USDC) | ~60% | Covers VPS/bandwidth for first 12 months for early operators |
| Genesis POL seed (USDC side of 80/20 Balancer) | ~25% | Pairs with the 100M TOKEN POL allocation |
| `SafetyReserve` genesis pre-fund (USDC) | ~10% | Covers incidents before fee inflows reach steady state |
| Audits, legal, contingency | ~5% | Operational, not protocol-bound |

### Genesis-day operator math (1 Gbps operator, no starting TOKEN)

- Genesis: buys 50K TOKEN at POL discovery price (~$0.05–0.10 → $2,500–$5,000 capital outlay).
- Months 1–12: earns USDC fees from delivery + pre-seed USDC subsidy → roughly cost-neutral on bandwidth.
- Months 6–12: earns bootstrap TOKEN grants → bond grows 50K → 200K → 795K (climbs 1G → ~3G → 10G tier).
- Month 12: operating at 10G tier, paid in USDC fees, governance-eligible (with age-ramp at full weight from month 6).

### Why the new bucket works

- The 12% Operator bootstrap is the bridge between "operator has no capital" and "operator runs profitable 10G+ node." Without it, only well-capitalized operators can scale.
- Grants are bonded, not sold, so they don't dump price.
- The cap on grants per operator (limited to tier-upgrade deltas) prevents one operator vacuuming the whole bucket.
- The 12pp drawn from treasury (−5) + seed (−2) + team (−2) + community (−3) is modest in relative terms (all four take ~10–20% relative cuts) and all retain their vesting protections.

## Section 6. Regulatory framing and ADR delta

### Howey-test framing

The U.S. Howey test asks four prongs:

1. Investment of money.
2. In a common enterprise.
3. With expectation of profit.
4. **Solely from the efforts of others.**

Work-token is positioned to break prong 4 cleanly:

> Under work-token design, holding TOKEN passively confers neither revenue nor governance privilege. Both flow only from operating a node, which requires providing infrastructure, bandwidth, and operational labor — the *holder's own efforts*. Passive holding earns nothing.

ADR 026 has two structural problems on prong 4:

- The 7% delegator pool TWAP-buys TOKEN and routes it to ve-lockers based purely on their lock duration. "Real yield in TOKEN" is the explicit framing in ADR 026 § Context. That looks like investment-contract income to a regulator.
- The 40% gauge pool rewards lock duration as a separate axis from operator work. An operator who locks more earns more without doing more delivery work — the lock itself is the income-generating instrument.

Work-token eliminates both vectors.

### Precedents informing the framing

- **Livepeer (LPT, 2018→present).** Work-token; orchestrators perform video transcoding; never charged. The "must perform work" framing is what we mirror.
- **Helium (HNT).** 2024 SEC settlement was scoped to subscriber-side claims; the work-token mechanism for hotspot operators was not the target of the action.
- **Filecoin (FIL).** Storage providers bond collateral; widely cited as the canonical operator-bond model.

**Caveat:** the spec records design intent. Actual deployment requires counsel review. The work-token framing improves the regulatory posture; it does not eliminate risk.

### ADR delta

| ADR | Status | Disposition under redesign |
|---|---|---|
| 003 (Payment model) | Minor edit | FeeRouter integration updated to 4-bucket; otherwise unchanged |
| 009 (Governance model) | Substantial rewrite | Voting source ve → capacity-weighted; non-operator voting removed; multisig bootstrap phase added |
| 016 (Contract interactions) | Substantial rewrite | FeeRouter simplified; VotingEscrow + DelegatorBuyer removed; StakingRegistry → CapacityBond |
| 018 (Liquidity strategy) | Unchanged | BuybackBurner sees 5× flow but unchanged shape |
| 026 (Tokenomics) | **Substantial rewrite** | Six-bucket → four-bucket; gauge + delegator deleted; allocation table updated with operator-bootstrap bucket |
| 028 (Slashing appeals) | Minor edit | Slashing applies to CapacityBond; locked-for-implementation status needs unlock |
| 032 (SafetyReserve appeals) | Minor edit | Capacity reads replace ve-supply reads where relevant |
| 033 (SafetyReserve) | Unchanged | Self-contained; unaffected |
| **034 (Gauge boost + VotingEscrow)** | **RETIRE → `adr/_history/`** | Mechanism replaced entirely by CapacityBond curve |
| **035 (Delegator pool)** | **RETIRE → `adr/_history/`** | Mechanism deleted entirely |

### Contract surface delta (ADR 016 perspective)

| Contract | Status | Notes |
|---|---|---|
| `PaymentChannel` | unchanged | |
| `FeeRouter` | major simplification | 6 buckets → 4; deleted: epoch buckets, pre-launch gauge, gauge formula, delegator swap path, claim windows |
| `StakingRegistry` | renamed → `CapacityBond` | Adds capacity-curve `bond = k × Mbps^α`, capacity-shortfall slashing |
| `VotingEscrow` | **DELETED** | Entire contract removed |
| `DelegatorBuyer` | **DELETED** | Entire contract removed |
| `BuybackBurner` | unchanged | Same logic, 5× volume |
| `SafetyReserve` | unchanged | |
| `DecdnGovernor` (merged in PR #671) | rewrite | Reads `CapacityBond.capacityAt × age_ramp` instead of `VotingEscrow.balanceOfAt`. Same OZ Governor base. |
| `Timelock` | unchanged | OpenZeppelin pattern |
| `TOKEN` (ERC20Burnable) | unchanged | |

Net effect: two contracts deleted, one renamed and extended, one substantially simplified. Solidity LOC after redesign is meaningfully smaller than the current ADR 016 inventory.

### Existing-PR impact

The `DecdnGovernor` commit (1597283 / PR #671) was merged five commits before this spec was drafted. Under redesign, the OpenZeppelin Governor base is reusable; only the voting-weight source plug needs replacement (`VotingEscrow.balanceOfAt` → `CapacityBond.capacityAt × age_ramp`). No need to revert the PR; the rewrite is a focused edit on the source-plug module.

The recently merged `StakingRegistry.bindNodeId` / `reclaimNodeId` work (PR #668) is preserved — node-id binding is orthogonal to the bond-vs-stake distinction and applies cleanly to `CapacityBond`.

## Open questions / things to settle during implementation planning

1. **Lock-amount-per-Mbps governance volatility.** k=12.6 is a discovered constant for the chosen 1G target bond (50K TOKEN). Governance changes to k can shift the entire bond curve. Consider whether k should be immutable post-genesis or governable within a tighter band than α.
2. **Probe verification cost at scale.** Section 1's 7-day initial probe window assumes probe throughput is non-binding. With 100+ operators registering concurrently in the first months, probe scheduling may need a queue/throttle.
3. **Bootstrap-grant gaming.** Section 5's auto-deposit-into-bond mechanism needs careful design to prevent operators from cycling: register at 1G, earn grant, upgrade, drop service to default, repeat. The capacity-shortfall slashing partially defends but may not be sufficient on the bootstrap path specifically.
4. **Liquid-bond wrappers.** A third-party contract could pool operator bonds and issue liquid receipts (analog to Convex/Lido). This isn't strictly possible under work-token because the bond is tied to a specific operator identity and capacity claim, but a registry of "bond-financed operators" backed by such wrappers is plausible. Disposition: out of scope for this spec; flag for future ADR if seen.
5. **Cross-chain TOKEN holders.** TOKEN may be bridged. Bridged holders can't operate on the canonical L2 and so can't vote — this is consistent with operator-only governance but worth being explicit about.
6. **The "any TOKEN holder votes" softer governance variant.** If political resistance to operator-only voting is too high, a fallback is to allow non-operator TOKEN holders to vote by locking TOKEN in a simple time-locked contract (a stripped-down `VotingEscrow` with no yield, only voting weight; no gauge, no delegator pool). This adds one contract but avoids the full ve-gauge complexity of ADR 026 and preserves the work-token framing for operators while giving passive holders a governance say. Disposition: present option; defer unless requested.

## Acceptance criteria for "this spec is implementable"

- An ADR 026 rewrite is produced replacing the six-bucket FeeRouter with four-bucket and deleting all gauge / delegator / ve references. Cross-links from 003, 009, 016, 028, 032 are updated.
- `FeeRouter`, `CapacityBond` (renamed from `StakingRegistry`), and `DecdnGovernor` contracts have updated Solidity interfaces in ADR 016 § Contract Inventory.
- ADRs 034 and 035 are moved to `adr/_history/` with retirement notes citing this spec.
- The k=12.6 and α=1.2 constants, the 60/25/10/5 default split, and the 5% per-operator voting cap are encoded in the canonical params table.
- A bootstrap-grant mechanic is sketched at contract-interface granularity (the auto-deposit path is implementable without inventing new primitives — it's `CapacityBond.depositForOperator(operator, amount)` called by a treasury-keyed minter limited to the 120M bucket).

## What this spec is NOT

- Not a full ADR rewrite. The ADR rewrites are downstream artifacts of this spec being accepted.
- Not a contract specification. Solidity interfaces will be in the ADR rewrites, not here.
- Not a legal opinion. The Howey framing is design intent; counsel review is required before deployment.
- Not a transition / migration plan. The current ADRs are Draft status — nothing is deployed — so there is no on-chain migration. The transition is purely an ADR-set rewrite + Solidity scaffolding adjustment.

## Recommendation

**Adopt option C (work-token) as specified in Sections 1–6.** The trade-offs the design makes against ADR 026 are intentional and favor the four user-stated priorities. The single significant political commit is Section 4's operator-only voting; that needs sign-off from the team and seed-investor stakeholders before the ADR rewrites land.

If Section 4 cannot be made to fly with stakeholders, the design's regulatory crispness is reduced but the rest of the redesign (Sections 1, 2, 3, 5, 6) still hold and produce a meaningfully simpler, more value-accruing, and more decentralization-aligned system than ADR 026. In that fallback, voting reverts to raw-bond-weighted with delegation to any address.
