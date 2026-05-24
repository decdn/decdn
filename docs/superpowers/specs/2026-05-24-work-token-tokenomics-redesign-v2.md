# Work-Token Tokenomics Redesign — Design Spec v2 (13-Group Distribution)

**Status:** Draft (brainstorming output, awaiting user review).
**Supersedes (if adopted):** v1 spec (`2026-05-23-work-token-tokenomics-redesign-design.md`). ADRs 026, 034, 035. Substantial edits to 009 and 016. Minor edits to 003, 028, 032.
**Relationship to v1:** Sections 1–4 and 6 are unchanged in substance. Section 5 (allocation) is replaced with a 13-group distribution to align with conventional fundraising-tool group taxonomies. The work-token mechanics (CapacityBond, four-bucket FeeRouter, operator-only governance) are identical to v1.

## Summary

Replace the current Curve-style ve-gauge tokenomics (ADR 026 + 033 + 034 + 035) with a **work-token model** modeled on Livepeer / Helium / Filecoin: every node operator must bond TOKEN proportional to the bandwidth capacity they serve, in a single capacity-gated contract. No passive yield to holders. Governance is operator-only.

The redesign is motivated by four user-stated priorities: (i) smaller contract surface, (ii) strong token value accrual, (iii) operator decentralization, (iv) regulatory defensibility. Work-token hits all four; the current ve-gauge design trades (iv) against (ii)–(iii) by routing real yield to passive ve-lockers via the delegator pool.

The redesign is net *subtractive* in code: two contracts (`VotingEscrow`, `DelegatorBuyer`) are deleted, `FeeRouter` simplifies from six buckets to four with no epoch / claim / snapshot machinery, and `StakingRegistry` is renamed to `CapacityBond` with one added piece of capacity-curve logic. The `SafetyReserve`, slashing primitive, payment channels, and POL/BuybackBurner all carry over largely unchanged.

**v2 change over v1:** The supply distribution moves from 7 conceptual buckets to a 13-group taxonomy aligned with conventional fundraising-tool categories (Core Contributors / Private Investors / Treasury / Public Sale / Ecosystem Incentives / Marketing / Liquidity Provision). The "Operator bootstrap" v1 bucket (12%) is absorbed and expanded into a 20% "Operator Service Emissions" allocation (externally labeled "Staking Rewards" for fundraising-tool compatibility). One bucket — "Liquidity Mining Rewards" 15% — carries genuine tension with work-token framing and is reinterpreted as a 12-month sunsetting bootstrap subsidy rather than recurring yield (see Section 5).

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
- Eligibility to receive Operator Service Emissions (Section 5, "Staking Rewards" bucket) for verified delivery.
- Governance voting weight (Section 4).

### What the bond does NOT grant

- No passive yield. No gauge boost. No "working_bytes" formula. No ve-balance time decay. No DelegatorBuyer pipeline. No epoch-snapshot accounting. None of that contract surface exists in the redesigned system.
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

### Supply impact across scale scenarios

Scale scenarios denote total monthly traffic delivered by the network. Peak capacity assumes ~30% average-of-peak utilization (`peak_Gbps ≈ PB/month × 10.3`). Operator mixes are illustrative — the design imposes no preferred mix.

| Scenario | Monthly traffic | Peak capacity | Representative operator mix | Total bonded (α=1.2) | % of 1B supply |
|---|---:|---:|---|---:|---:|
| S1 | 1 PB | ~10 Gbps | 10 × 1G | 500K TOKEN | 0.05% |
| S2 | 10 PB | ~100 Gbps | 50 × 1G + 5 × 10G | 6.475M TOKEN | ~0.6% |
| S3 | 100 PB | ~1 Tbps | 100 × 1G + 30 × 10G + 5 × 100G | 91.85M TOKEN | ~9.2% |
| S4 | 500 PB | ~5 Tbps | 500 × 1G + 100 × 10G + 30 × 100G | 482.5M TOKEN | ~48% |

**Observations:**

- S1 and S2 leave essentially all TOKEN liquid — bond demand is sub-1% of supply, so price impact from bonding is minimal. The Operator Service Emissions bucket (200M / 20% — see Section 5) covers operator-tier-upgrade economics through ~S3 even without external TOKEN buys.
- S3 is the healthy steady-state zone — ~9% bonded gives meaningful demand without supply-lockup pressure on payment-channel topups, governance, or new-operator entry.
- S4 is the design-tension scenario. At default α=1.2 the bond curve absorbs nearly half of supply, leaving a thin liquid float. Governance has the α lever for this: at α=1.1 the same S4 mix bonds ~325M (~33%); at α=1.0 (linear, no concentration pressure) ~225M (~23%). This is precisely why α is governable within [1.0, 1.8].
- Reference: ADR 026 targeted a 30–50% ve-lock rate as steady state. Under work-token at default α=1.2, that range is approached between S3 and S4 — and the equivalent lever is α-tuning rather than lock-duration incentives.

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
- ✅ Per-epoch bytes-delivered counter retained for analytics and for the Operator Service Emissions distribution (Section 5).

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

Per the v2 distribution (Section 5): Core Contributors (15%), Private Investors (16%), Treasury (15%), Public Sale (3%), Airdrops/Testnet (6%), Marketing (6%), Liquidity Provision (4%) — all hold or receive TOKEN, but **none can vote** unless they also bond it to operate.

**Why this is right:**

- The moment seed/team/holder classes get to vote on bond parameters, fee splits, etc., the regulatory framing weakens (now there's a "common enterprise" voting on profit-like decisions).
- Operator-only governance is the cleanest regulatory posture, mirroring how Filecoin's storage-provider class and Helium's hotspot class are the load-bearing voting constituency in those networks.
- It enforces the work-token framing structurally rather than rhetorically.

**The political cost is smaller than it appears.**

- Per `internal/Legal/entity-structure-design.md` § Pattern A (Legal Fiction Separation), the entity design **already** excludes investors from DAO voting — this is a pre-existing structural choice independent of tokenomics. DAO governance is permissionless and no-KYC (entity-structure-design.md:406); investor influence sits at the Labs (C-Corp) equity layer: Series A+ board seats, standard preferred-stock protective provisions (veto on sale, new equity issuance, debt above threshold), and indirect TOKEN exposure via Labs' ~15% treasury allocation (entity-structure-design.md:168, 180–181).
- Work-token does not narrow investor power *relative to that existing entity design*. It narrows DAO voting from "ve-lockers" to "operators" — a change to who-among-active-participants votes, not a removal of an investor right that ever existed in entity design.
- Term-sheet language for the Private Investors (7%) tranche should not promise the ADR 026 passive-governance-via-lock path, because that was never a designed-in investor right — it was an artifact of ve-tokenomics that the entity design explicitly excluded from investor channels.
- Mitigation if any investor wants direct DAO signal: any TOKEN holder who *also* operates a node votes like any other operator. This path is open to investors, team, treasury, and seed equally.
- Non-operator holder protection lives at the contract level (immutable share floors per Section 3), not at the governance level. Operators cannot vote to push the operator-base share above 90% or burn below 5%; non-operator value accrual is structurally guaranteed within those bounds.

### Bootstrap governance — temporary multisig phase

The voting set is narrow at launch (likely <50 operators in the first 6–12 months). Direct application of capacity-weighted governance pre-bootstrap risks hostile takeover via a cheap operator-fleet setup.

- For the first 6–12 months, governance runs through a multisig with hard-cap pause powers (extends ADR 009's emergency-multisig pattern).
- Transition to full operator-weighted governance is auto-triggered when **active operator count ≥ 30** AND **total declared capacity ≥ 100 Gbps**. Both thresholds governable.
- Before transition, the multisig can execute parameter changes within the safety bounds in Sections 2 and 3.

### Delegation

- Operators may delegate voting weight to another address via EIP-712 signed delegation (Governor Bravo pattern). The bond itself cannot be delegated — only the voting power.
- This is the *only* mechanism by which a non-operator address gains vote weight, and it requires an operator's explicit signature.

## Section 5. Supply distribution — 13 groups, 1B fixed supply

The v2 distribution adopts a 13-group taxonomy aligned with conventional fundraising-tool categories. Two groups carry work-token-specific reinterpretations (flagged inline); the remainder are conventional.

### Allocation table

| # | Group | Allocation | Type | Category | Vesting / mechanic |
|---|---|---:|---|---|---|
| 1 | Core Contributors | 12% | Internal | Core Contributors | 4-year linear, 12mo cliff |
| 2 | Advisors | 3% | Internal | Core Contributors | 2-year linear, 6mo cliff |
| 3 | Seed Investors | 9% | Internal | Private Investors | 3-year linear, 6mo cliff |
| 4 | Private Investors | 7% | Internal | Private Investors | 3-year linear, 6mo cliff |
| 5 | DAO Treasury | 15% | Internal | Treasury | 4-year linear unlock to Timelock-controlled wallet |
| 6 | Public Sale | 3% | Internal | Public Sale | Genesis-liquid (or 6mo lockup if regulatory posture requires) |
| 7 | **Staking Rewards** *(see §5.1)* | 20% | External | Ecosystem Incentives | Service-based emission; auto-deposited into `CapacityBond`; not withdrawable as liquid TOKEN |
| 8 | **Liquidity Mining Rewards** *(see §5.2)* | 15% | External | Ecosystem Incentives | 12-month sunsetting bootstrap subsidy; no recurring yield |
| 9 | Airdrops | 3% | External | Ecosystem Incentives | Genesis-claim window (12 weeks); unclaimed sweeps to Treasury |
| 10 | Incentivized Testnet Rewards | 3% | External | Ecosystem Incentives | Genesis-claim window (12 weeks) for pre-launch testnet participants |
| 11 | Misc. Marketing, PR, and KOLs | 3% | Internal | Marketing | Treasury-managed, ad-hoc spend within annual budget cap |
| 12 | Exchange Partnerships | 3% | External | Marketing | Milestone-based to CEX listings, market-makers |
| 13 | Liquidity Provision, Market Making | 4% | External | Liquidity Provision | Genesis-liquid to MM partners (complementary to POL on Balancer 80/20) |
| | **Total** | **100%** | | | |

### Categorical rollup

| Category | Allocation | # Groups |
|---|---:|---:|
| Core Contributors | 15% | 2 |
| Private Investors | 16% | 2 |
| Treasury | 15% | 1 |
| Public Sale | 3% | 1 |
| Ecosystem Incentives | 41% | 4 |
| Marketing | 6% | 2 |
| Liquidity Provision | 4% | 1 |
| **Total** | **100%** | **13** |

| Type | Allocation | # Groups |
|---|---:|---:|
| Internal | 52% | 7 |
| External | 48% | 6 |
| **Total** | **100%** | **13** |

### Small adjustments from the provided distribution

The provided 13-group input summed to 104%. To reconcile to 100% while honoring "small adjustments," four 1pp trims were applied across internal/POL groups (where work-token thesis is least sensitive to the cut):

| Group | Provided | Adopted | Δ |
|---|---:|---:|---:|
| Core Contributors | 13% | 12% | −1pp |
| Seed Investors | 10% | 9% | −1pp |
| Private Investors | 8% | 7% | −1pp |
| Liquidity Provision, Market Making | 5% | 4% | −1pp |
| All other groups | unchanged | | 0 |

### §5.1 Staking Rewards (20%) — Operator Service Emissions

Under conventional crypto-token semantics, "Staking Rewards" denotes passive yield to any TOKEN-locker. **Under work-token, that meaning is incompatible with the regulatory framing in Section 6.** The bucket is therefore reinterpreted as **Operator Service Emissions**:

- 200M TOKEN distributed to active operators based on verified service delivery (probe-attested bytes × capacity tier × diminishing-returns curve).
- Auto-deposited into the operator's `CapacityBond` on a monthly (or per-epoch) cadence. Operators **cannot withdraw the granted TOKEN as liquid** until they unbond fully and exit operator status (subject to the 14-day unbonding window in Section 1).
- Subsidies stack with the operator's tier-upgrade economics: an operator can use granted TOKEN to bond up to a higher tier (which then requires probe re-verification per Section 1) instead of buying TOKEN at market.
- Emission schedule is front-loaded: ~40% in years 1–2 (S1→S3 bootstrap), tapering to ~20% in years 3–4 and ~10% per year thereafter until the 200M bucket exhausts (≈year 6 at default emission curve).
- Bucket sunsets when exhausted or by governance vote after the transition thresholds in Section 4 (active operator count ≥ 30 AND total declared capacity ≥ 100 Gbps) are met for ≥6 months.

**Why this is consistent with work-token:** Operators are *doing work* (delivering bytes verified by the probe network). Granting them TOKEN for that work is payment for labor, not passive yield to a holder. The Filecoin / Helium / Livepeer precedents all use service-based emissions of this shape; none of them have lost the "utility-token-by-service" framing as a result.

**Public-facing label:** The bucket retains the "Staking Rewards" label in fundraising materials and on tokenomics-tool exports for recognizability, but term-sheet language and the canonical ADR refer to it as **Operator Service Emissions** to align with the work-token framing. The label disagreement is documented in fundraising one-pagers as a glossary entry: *"Staking = operator bonding to deliver bandwidth. Stakers are not passive lockers."*

### §5.2 Liquidity Mining Rewards (15%) — Bootstrap LP Subsidy (sunsetting)

This is the bucket with the largest residual tension against the work-token thesis. Conventional LM rewards are by definition passive yield to LPs (LPs sit, earn). Under strict work-token reading, "passive holding earns nothing" extends to LP positions.

The v2 spec adopts a **time-bounded bootstrap interpretation** rather than a permanent LM program:

- 150M TOKEN allocated as a 12-month LP incentive program, distributed pro-rata to LPs on the Balancer V3 80/20 TOKEN/USDC pool (ADR 018).
- **Hard sunset at month 12.** No governance vote can extend the program. The bucket is encoded with a fixed emission schedule that zeros out at month 12; any unspent balance sweeps to Treasury.
- **No reinitialization possible without a token redeployment.** The contract emits all 150M over the 12-month window and is then permanently disabled.
- After month 12, LPs earn only from trading fees (real but small under standard AMM mechanics). The protocol no longer subsidizes LP positions.

**Regulatory framing:** A time-bounded bootstrap subsidy is materially different from a recurring yield program. Bootstrap subsidies are common in token launches (Compound 2020, dYdX 2021, GMX 2022) and have historically not triggered the same Howey-prong-4 concerns as ongoing yield. The 12-month sunset is the load-bearing design choice; without it, this bucket is the single largest regulatory weakness in the v2 distribution.

**Alternative if any residual tension is unacceptable:** Redeploy the 15% (150M TOKEN) as additional **Protocol-Owned Liquidity** held by treasury, deployed into the Balancer 80/20 pool as a treasury position. No external LP earns rewards; no LM exists. POL grows from 4% (group 13) + 15% (group 8) = 19% effective POL. This is the strictest work-token reading. Disposition: present as a fallback; default to the bootstrap-LM interpretation above.

### Vesting and unlock cliff diagram

```
Year:  0  1  2  3  4  5  6
       ────────────────────────────────────────────────
1  Core         ▁▂▃▄▅▆▇█  (12mo cliff, 4y linear)
2  Advisors    ▂▃▄▅▆█      (6mo cliff, 2y linear)
3  Seed         ▂▃▄▅▆█      (6mo cliff, 3y linear)
4  Private      ▂▃▄▅▆█      (6mo cliff, 3y linear)
5  Treasury    ▁▂▃▄▅▆▇█    (4y linear unlock)
6  Public       ██           (genesis-liquid)
7  Staking      ▆▆▅▄▃▂▁     (front-loaded service emission)
8  LM Boot      ████░░░░     (12mo program, then ZERO)
9  Airdrops    ██           (12-week claim window)
10 Testnet     ██           (12-week claim window)
11 Marketing    ▂▂▂▂▂▂▂▂    (ad-hoc, treasury-managed)
12 Exchange    ▁▂▃▂▁        (milestone-clustered around listings)
13 LP/MM       ██           (genesis-liquid to MM partners)
```

### Genesis-liquid float (TGE Day 1)

| Source | Allocation |
|---|---:|
| Public Sale | 3% |
| Airdrops (claimed) | up to 3% over 12 weeks |
| Incentivized Testnet (claimed) | up to 3% over 12 weeks |
| Liquidity Provision, MM | 4% |
| **Total liquid float at TGE** | **~10–13%** |

This is meaningfully more than v1's ~2% genesis-liquid (Public sale alone). The increased float supports POL-pool depth, market-maker order books, and price discovery within reasonable bid/ask spreads.

### Pre-seed USDC ($1M+) deployment

| Use | Approx allocation | Notes |
|---|---|---|
| Operator infrastructure subsidies (direct USDC) | ~55% | Covers VPS/bandwidth for first 12 months for early operators; pairs with the Staking Rewards bucket to make first-year operator unit economics positive |
| Genesis POL seed (USDC side of 80/20 Balancer) | ~25% | Pairs with the 4% LP/MM allocation and any LM-bootstrap LP positions |
| `SafetyReserve` genesis pre-fund (USDC) | ~10% | Covers incidents before fee inflows reach steady state |
| LM bootstrap program backing (USDC arm of incentive pool, optional) | ~5% | Only if dual-asset LM design chosen; otherwise rolls to treasury |
| Audits, legal, contingency | ~5% | Operational, not protocol-bound |

### Genesis-day operator math (1 Gbps operator, no starting TOKEN)

- Genesis: buys 50K TOKEN at POL discovery price (~$0.05–0.10 → $2,500–$5,000 capital outlay).
- Months 1–12: earns USDC fees from delivery + pre-seed USDC subsidy → roughly cost-neutral on bandwidth.
- Months 1–24: earns Operator Service Emissions (Staking Rewards bucket) → bond grows 50K → ~200K → ~795K (climbs 1G → ~3G → 10G tier).
- Month 12: operating at 5–10G tier, paid in USDC fees, governance-eligible (with age-ramp at full weight from month 6).
- Month 24: established mid-tier operator with bond financed primarily by service delivery, not capital injection.

### v1 → v2 distribution delta

| v1 bucket | v1 share | v2 mapping | v2 share | Δ |
|---|---:|---|---:|---:|
| Protocol treasury | 25% | DAO Treasury | 15% | −10pp |
| Seed backers | 22% | Seed (9) + Private Investors (7) | 16% | −6pp |
| Team & core contributors | 17% | Core Contributors (12) + Advisors (3) | 15% | −2pp |
| Community & ecosystem | 12% | Airdrops (3) + Testnet (3) + Marketing (6) | 12% | 0 |
| Genesis liquidity (POL) | 10% | LP/MM (4) | 4% | −6pp |
| Public sale / airdrop | 2% | Public Sale | 3% | +1pp |
| Operator bootstrap | 12% | Staking Rewards (Operator Service Emissions) | 20% | +8pp |
| (none in v1) | 0% | Liquidity Mining Rewards (bootstrap, sunsetting) | 15% | +15pp |
| **Total** | **100%** | | **100%** | 0 |

Headlines: Treasury (−10pp) and Seed (−6pp) are the largest concessions, redeployed into Operator Service Emissions (+8pp) and the new sunsetting LM program (+15pp). POL allocation tightens (−6pp) and is partially compensated by the new LM program providing additional pool-side TOKEN during the bootstrap window.

### Why this distribution works under work-token

- **Operator Service Emissions (20%)** is mechanically the same as v1's Operator bootstrap (12%) but more generous. It absorbs the operator-side bootstrap concern and adds runway through year 4 rather than year 2.
- **Bootstrap LM (15%)** is the only design tension worth flagging; the 12-month sunset is what keeps it work-token-compatible. The fallback (redeploy as POL) is documented above if any residual tension is unacceptable.
- **Smaller treasury (15% vs 25%)** is sustainable because the larger Operator Service Emissions bucket reduces the operational-grant calls treasury would otherwise have to fund.
- **Smaller seed/private (16% vs 22%)** is consistent with Section 4's reframe: investor influence is at the Labs equity layer; smaller TOKEN allocation here is offset by Labs equity upside and the 15% Labs treasury position (entity design § Labs).

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

**v2 caveat on prong 4:** The 15% Bootstrap LM bucket (§5.2) introduces a 12-month window during which LPs earn TOKEN passively. This is a real prong-4 exposure for that window. The mitigation is the hard sunset (no governance can extend it), which characterizes the program as a one-time launch subsidy rather than ongoing yield — but counsel should weigh whether the launch-subsidy framing survives in the target jurisdiction. If not, the fallback (redeploy as POL) restores the cleanest framing.

### Precedents informing the framing

- **Livepeer (LPT, 2018→present).** Work-token; orchestrators perform video transcoding; never charged. The "must perform work" framing is what we mirror.
- **Helium (HNT).** 2024 SEC settlement was scoped to subscriber-side claims; the work-token mechanism for hotspot operators was not the target of the action.
- **Filecoin (FIL).** Storage providers bond collateral; widely cited as the canonical operator-bond model. Filecoin also runs a service-emissions program (block rewards) which is the direct precedent for the §5.1 Staking Rewards reinterpretation.

**Caveat:** the spec records design intent. Actual deployment requires counsel review. The work-token framing improves the regulatory posture; it does not eliminate risk.

### ADR delta

| ADR | Status | Disposition under redesign |
|---|---|---|
| 003 (Payment model) | Minor edit | FeeRouter integration updated to 4-bucket; otherwise unchanged |
| 009 (Governance model) | Substantial rewrite | Voting source ve → capacity-weighted; non-operator voting removed; multisig bootstrap phase added |
| 016 (Contract interactions) | Substantial rewrite | FeeRouter simplified; VotingEscrow + DelegatorBuyer removed; StakingRegistry → CapacityBond; add `OperatorEmissions` distribution contract; add `BootstrapLMRewards` sunsetting contract |
| 018 (Liquidity strategy) | Minor edit | BuybackBurner sees 5× flow but unchanged shape; add LM bootstrap subsection |
| 026 (Tokenomics) | **Substantial rewrite** | Six-bucket → four-bucket; gauge + delegator deleted; allocation table updated with 13-group v2 distribution |
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
| `OperatorEmissions` (NEW) | NEW | Distributes Staking Rewards bucket per verified service; auto-deposits to `CapacityBond` |
| `BootstrapLMRewards` (NEW) | NEW, time-bombed | Distributes Bootstrap LM bucket over 12 months; hard-sunsets and becomes inert |
| `VotingEscrow` | **DELETED** | Entire contract removed |
| `DelegatorBuyer` | **DELETED** | Entire contract removed |
| `BuybackBurner` | unchanged | Same logic, 5× volume |
| `SafetyReserve` | unchanged | |
| `DecdnGovernor` (merged in PR #671) | rewrite | Reads `CapacityBond.capacityAt × age_ramp` instead of `VotingEscrow.balanceOfAt`. Same OZ Governor base. |
| `Timelock` | unchanged | OpenZeppelin pattern |
| `TOKEN` (ERC20Burnable) | unchanged | |

Net effect vs v1: two contracts deleted (`VotingEscrow`, `DelegatorBuyer`), two added (`OperatorEmissions`, `BootstrapLMRewards`), one renamed and extended (`StakingRegistry` → `CapacityBond`), one substantially simplified (`FeeRouter`). The two new contracts are narrowly-scoped distribution contracts with no governance hooks — they are not in the same complexity class as the deleted `VotingEscrow`/`DelegatorBuyer` pair.

### Existing-PR impact

The `DecdnGovernor` commit (1597283 / PR #671) was merged before this spec was drafted. Under redesign, the OpenZeppelin Governor base is reusable; only the voting-weight source plug needs replacement (`VotingEscrow.balanceOfAt` → `CapacityBond.capacityAt × age_ramp`). No need to revert the PR; the rewrite is a focused edit on the source-plug module.

The recently merged `StakingRegistry.bindNodeId` / `reclaimNodeId` work (PR #668) is preserved — node-id binding is orthogonal to the bond-vs-stake distinction and applies cleanly to `CapacityBond`.

## Open questions / things to settle during implementation planning

1. **Lock-amount-per-Mbps governance volatility.** k=12.6 is a discovered constant for the chosen 1G target bond (50K TOKEN). Governance changes to k can shift the entire bond curve. Consider whether k should be immutable post-genesis or governable within a tighter band than α.
2. **Probe verification cost at scale.** Section 1's 7-day initial probe window assumes probe throughput is non-binding. With 100+ operators registering concurrently in the first months, probe scheduling may need a queue/throttle.
3. **Service-emission curve design.** Section §5.1's "front-loaded, tapering" curve is described qualitatively. The exact mathematical form (exponential decay? linear taper? step function?) needs specification with explicit per-month emission rates. Recommend modeling in `finance/notebooks/` before locking the curve in contract.
4. **Bootstrap LM design.** §5.2's 12-month bootstrap LM needs concrete parameter design: emission curve over the 12-month window (front-loaded? flat? declining?), reward distribution math (per-block? per-snapshot?), and pool-eligibility (Balancer 80/20 only, or any TOKEN-paired pool?). Recommend defaulting to Balancer-only with flat per-block emission and a single-pool restriction to avoid mercenary-LP arbitrage across pools.
5. **Liquid-bond wrappers.** A third-party contract could pool operator bonds and issue liquid receipts (analog to Convex/Lido). This isn't strictly possible under work-token because the bond is tied to a specific operator identity and capacity claim, but a registry of "bond-financed operators" backed by such wrappers is plausible. Disposition: out of scope for this spec; flag for future ADR if seen.
6. **Cross-chain TOKEN holders.** TOKEN may be bridged. Bridged holders can't operate on the canonical L2 and so can't vote — this is consistent with operator-only governance but worth being explicit about. Particularly relevant for the Airdrops bucket if airdrop recipients are on other chains.
7. **Should non-operator TOKEN holders (including private investors) have a DAO governance path?** Investigated and resolved in v1. Per `internal/Legal/entity-structure-design.md` § Pattern A, the existing entity design already excludes investors and other non-operator holders from DAO voting by structure — DAO governance is permissionless and no-KYC, with investor influence routed to the Labs (C-Corp) equity layer (board seats, protective provisions, treasury exposure). Work-token's operator-only DAO is *consistent* with that design. **Disposition: no separate investor-governance mechanism. Operator-only DAO + Labs board seats + immutable contract-level non-operator-holder floors (Section 3) remains the recommended split.**
8. **§5.2 fallback decision deadline.** The Bootstrap LM (sunsetting) vs additional-POL (no LM) choice is the largest single regulatory decision in v2. Recommend resolving this with counsel before TGE rather than as a post-launch governance vote — the choice changes which contract is deployed at TGE (`BootstrapLMRewards` vs additional POL deposit) and cannot easily be reversed.

## Acceptance criteria for "this spec is implementable"

- An ADR 026 rewrite is produced replacing the six-bucket FeeRouter with four-bucket, the seven-bucket allocation with the 13-group v2 allocation, and deleting all gauge / delegator / ve references. Cross-links from 003, 009, 016, 028, 032 are updated.
- `FeeRouter`, `CapacityBond` (renamed from `StakingRegistry`), `OperatorEmissions` (NEW), `BootstrapLMRewards` (NEW, time-bombed), and `DecdnGovernor` contracts have updated Solidity interfaces in ADR 016 § Contract Inventory.
- ADRs 034 and 035 are moved to `adr/_history/` with retirement notes citing this spec.
- The k=12.6 and α=1.2 constants, the 60/25/10/5 default fee split, the 5% per-operator voting cap, and the 13-group allocation table are encoded in the canonical params table.
- Counsel-driven decision on the §5.2 Bootstrap LM design is captured in ADR 026 § allocation prior to TGE.

## What this spec is NOT

- Not a full ADR rewrite. The ADR rewrites are downstream artifacts of this spec being accepted.
- Not a contract specification. Solidity interfaces will be in the ADR rewrites, not here.
- Not a legal opinion. The Howey framing is design intent; counsel review is required before deployment.
- Not a transition / migration plan. The current ADRs are Draft status — nothing is deployed — so there is no on-chain migration. The transition is purely an ADR-set rewrite + Solidity scaffolding adjustment.

## Recommendation

**Adopt v2 (this spec) as the canonical work-token allocation.** The 13-group taxonomy aligns with fundraising-tool exports and conventional category names while preserving the work-token mechanics from v1 in Sections 1–4. The two design tensions worth explicit decision are:

1. **§5.2 Bootstrap LM (15%) vs additional POL.** Default is sunsetting LM; fallback is POL redeployment. Counsel-driven decision; resolve before TGE.
2. **§5.1 Staking Rewards (20%) label semantics.** Default keeps the conventional label externally while documenting "Operator Service Emissions" internally and in ADR canonical text. If audit or counsel finds the dual labeling problematic, drop the conventional label and use "Operator Service Emissions" everywhere — at modest fundraising-tool-integration cost.

If v1's 7-bucket distribution is preferred over v2's 13-group taxonomy for reasons unrelated to fundraising-tool integration, both specs are internally consistent — the v1 spec remains in this directory and either can be adopted. The mechanics (Sections 1–4, 6) are identical between v1 and v2.
