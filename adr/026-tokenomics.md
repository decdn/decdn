# ADR 026: Tokenomics

**Date:** 2026-05-25 (substantial rewrite under [spec v2.1 — work-token redesign](../docs/superpowers/specs/2026-05-24-work-token-tokenomics-redesign-v2.1.md))
**Status:** Draft

## Context

The economic model — sitting on top of paid byte delivery ([ADR 003](003-payments.md#adr-003-payment-model)) and the slashing primitive ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)) — must hold up against four objectives, all weighted equally:

1. **Simplicity / smaller contract surface.** Audit burden and operational complexity scale with the number of cooperating contracts, epoch-bucket accounting paths, and pull-claim windows.
2. **Token value accrual.** TOKEN demand must scale with network usage; the demand mechanism must be mechanically tied to capacity growth rather than to a promise of yield.
3. **Operator decentralization.** Structurally favor a diverse operator set over a few large operators.
4. **Regulatory defensibility.** Avoid framing that resembles an investment contract under the Howey test — particularly the "solely from the efforts of others" prong.

This ADR is the canonical economic-model umbrella. It implements a **work-token model** (Livepeer / Helium / Filecoin lineage): every node operator bonds TOKEN proportional to the bandwidth capacity it serves, in a single capacity-gated contract. There is no passive yield to holders. Governance is operator-only. The previous Curve-style ve-gauge tokenomics (this ADR's pre-rewrite body plus retired [ADR 034](_history/034-gauge-boost-voting-escrow.md) and [ADR 035](_history/035-delegator-pool.md)) is replaced wholesale.

### Inputs assumed by this ADR

Pre-launch design with no holder-compensation or contract-migration concerns. ~$1M+ pre-seed USDC capital secured (planning target $3M); program structure is operational and tracked separately. 2026 unmetered-bandwidth provider economics (1 Gbps VPS, 10 Gbps dedicated, 100 Gbps edge tiers); dedicated-bandwidth nodes are realistic at every scale band the protocol is sized for.

## Decision

The protocol's economic model is defined by the following sections.

### Supply and distribution

**Supply.** 1,000,000,000 TOKEN, fixed at genesis. No post-genesis minting function exists on the production token contract.

#### Burnability

TOKEN is `ERC20Burnable`; any contract may burn TOKEN it holds via `burn` / `burnFrom`. Burns reduce `totalSupply` and emit `Transfer(from, address(0), amount)`. The [§ Slashing and burn](#slashing-and-burn) slashing-burn path uses this; future contract surfaces that need a TOKEN sink integrate via the same standard interface without contract changes.

#### Allocation

Eleven groups summing to 100%. The v2.1 13-group taxonomy is collapsed: Operator Service Emissions (was group 7, 20%) is removed entirely; Liquidity Mining Rewards (was group 8 at 0%) is removed; Airdrops (was group 9 at 3%) is removed at TGE (DAO may fund a later airdrop discretionarily from the operational Treasury sub-bucket); Incentivized Testnet Rewards (was group 10 at 3%) is absorbed into the Genesis Bond Credits program described in [§ Genesis Bond Credits](#genesis-bond-credits). See the v2.2 design spec `docs/superpowers/specs/2026-05-27-remove-operator-emissions-design.md` for the full v2.1→v2.2 delta.

| # | Group | Allocation | Type | Category | Vesting / mechanic |
|---|---|---:|---|---|---|
| 1 | Core Contributors | 15% | Internal | Core Contributors | 4-year linear, 12mo cliff |
| 2 | DAO Treasury | 15% | Internal | Treasury | 5pp earmarked at TGE for [§ Genesis Bond Credits](#genesis-bond-credits); 10pp 4-year linear unlock to Timelock-controlled wallet (operational Treasury) |
| 3 | Protocol Owned Liquidity | 15% | External | Liquidity Provision | Treasury-owned position on Balancer V3 80/20 per [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol) |
| 4 | App Incentives | 14% | External | Ecosystem Incentives | 4-year linear unlock to Timelock-controlled multisig; publisher rebates + integration grants per [§ App Incentives](#app-incentives) |
| 5 | Seed Investors | 11% | Internal | Private Investors | 3-year linear, 6mo cliff |
| 6 | Private Investors | 9% | Internal | Private Investors | 3-year linear, 6mo cliff |
| 7 | Market Making | 5% | External | Liquidity Provision | Genesis-liquid, MM-partner-allocated |
| 8 | Misc. Marketing, PR, and KOLs | 5% | Internal | Marketing | Treasury-managed, ad-hoc spend within annual budget cap |
| 9 | Public Sale | 5% | External | Public Sale | Genesis-liquid (or 6mo lockup if regulatory posture requires) |
| 10 | Advisors | 3% | Internal | Core Contributors | 2-year linear, 6mo cliff |
| 11 | Exchange Partnerships | 3% | External | Marketing | Milestone-based to CEX listings, market-makers |
| | **Total** | **100%** | | | |

**Categorical rollup.**

| Category | Allocation | # Groups |
|---|---:|---:|
| Core Contributors | 18% | 2 |
| Private Investors | 20% | 2 |
| Treasury | 15% | 1 |
| Public Sale | 5% | 1 |
| Ecosystem Incentives | 14% | 1 |
| Marketing | 8% | 2 |
| Liquidity Provision | 20% | 2 |
| **Total** | **100%** | **11** |

**Internal / External rollup.** Internal (Core, Advisors, Seed, Private, Treasury, Misc Marketing) = 58%; External (POL, App Incentives, Market Making, Public Sale, Exchange Partnerships) = 42%. Public Sale's Type changes from Internal (v2.1) to External (v2.2) to reflect open-window distribution.

**Genesis liquid float (TGE Day 1).**

| Source | Allocation |
|---|---:|
| Public Sale | 3% |
| Airdrops (claimed) | up to 3% over 12 weeks |
| Incentivized Testnet (claimed) | up to 3% over 12 weeks |
| Market-Maker partner allocation | 4% |
| **Total liquid float at TGE** | **~10–13%** |

The 15pp POL position is "in the pool" but not floating in the sense of being available to circulate — it sits as a treasury-owned LP position, removable only by governance.

#### No auto-bond on vest

Vesting contracts release TOKEN unlocked into the recipient's wallet. Bonding into `CapacityBond` is opt-in and requires operating a node. Recipients who do not operate hold liquid TOKEN; they have no passive-yield path and no governance weight (see [§ Governance](#governance)). This is the regulatory-cleanliness pillar: passive holding earns nothing.

#### No protocol-issued node-bootstrap fund

Bootstrap supply-side incentive is funded externally via $1M+ pre-seed USDC capital, eliminating TOKEN-price reflexivity in subsidy purchasing power. Program structure is operational and tracked separately. See [§ Bootstrap mechanism — pre-seed USDC](#bootstrap-mechanism--pre-seed-usdc).

### Capacity-bond curve

Every operator must bond TOKEN proportional to the bandwidth capacity it declares. The bond is the only TOKEN-side requirement on operators — no separate flat minimum stake, no optional lock for additional yield.

**Bond formula.**

```
bond_required(Mbps) = k × Mbps^α
```

| Parameter | Default | Governable range | Notes |
|---|---:|---|---|
| α (exponent) | 1.2 | [1.0, 1.8] | 1.0 = linear (no decentralization pressure); 1.8 = strong concentration penalty |
| k (bond constant, TOKEN) | 12.6 | bounded by 1G tier ∈ [10K, 200K TOKEN] | Picked so `bond_required(1000) ≈ 50,000 TOKEN` |
| `MAX_CAPACITY_PER_OPERATOR` | 200 Gbps | [50, 1000] Gbps | Prevents one operator cornering edge-tier capacity |
| `min_delivery_ratio` | 70% | [50%, 90%] | Min sustained delivery as fraction of declared capacity (95th-percentile over 7-day probe window). Lower = more forgiving; higher = stricter |

**Worked numbers at α=1.2, k=12.6.**

| Tier | Capacity | Bond | Bond per Mbps | Ratio vs 1G |
|---|---:|---:|---:|---:|
| Entry | 1 Gbps | 50,000 TOKEN | 50 | 1.0× |
| Mid | 10 Gbps | 795,000 TOKEN | 80 | 1.6× |
| Edge | 100 Gbps | 12,600,000 TOKEN | 126 | 2.5× |

The super-linear curve makes high-capacity operators pay more per Mbps. The 2.5× per-Mbps ratio at α=1.2 matches the prior design's decentralization-pressure target.

**Bond lifecycle.**

- `CapacityBond.register(declaredMbps)` deposits the bond and emits `CapacityClaimed(operator, Mbps)`.
- The probe service samples the operator over a 7-day window using the existing `cdn/probe/v1` ALPN ([ADR 013](013-schema-evolution.md#adr-013-schema-evolution)). If 95th-percentile sustained delivery falls below `min_delivery_ratio × declaredMbps`, registration auto-reverts the bond minus a fixed probe-cost fee (~50 TOKEN) deposited to the treasury.
- Re-registration at a lower tier is permitted at any time; re-registration at a higher tier requires a new probe window.
- **Unbonding window: 14 days, slashable during unbonding.** Longer than the prior 7-day stake window because capacity-claim-verification overlap is the binding consideration here, not pure security.

**Supply impact across scale scenarios.**

Scale scenarios denote total monthly traffic delivered by the network. Peak capacity assumes ~30% average-of-peak utilization (`peak_Gbps ≈ PB/month × 10.3`). Operator mixes are illustrative — the design imposes no preferred mix.

| Scenario | Monthly traffic | Peak capacity | Representative operator mix | Total bonded (α=1.2) | % of 1B supply |
|---|---:|---:|---|---:|---:|
| S1 | 1 PB | ~10 Gbps | 10 × 1G | 500K TOKEN | 0.05% |
| S2 | 10 PB | ~100 Gbps | 50 × 1G + 5 × 10G | 6.475M TOKEN | ~0.6% |
| S3 | 100 PB | ~1 Tbps | 100 × 1G + 30 × 10G + 5 × 100G | 91.85M TOKEN | ~9.2% |
| S4 | 500 PB | ~5 Tbps | 500 × 1G + 100 × 10G + 30 × 100G | 482.5M TOKEN | ~48% |

S1 and S2 leave essentially all TOKEN liquid; the [§ Operator Service Emissions](#operator-service-emissions) bucket covers operator-tier-upgrade economics through ~S3 without external TOKEN buys. S3 is the steady-state zone — ~9% bonded gives meaningful demand without supply-lockup pressure. S4 is the design-tension zone: at default α=1.2 the curve absorbs ~half of supply; α-tuning is the lever (at α=1.0 the same S4 mix bonds ~23%).

### What the bond grants and does not grant

**Grants:**

- Right to register as an active operator at the declared capacity tier.
- 100% of the operator share of the fee split (60% — see [§ FeeRouter split](#feerouter-split)).
- Eligibility to receive Operator Service Emissions (see [§ Operator Service Emissions](#operator-service-emissions)) for verified delivery.
- Governance voting weight (see [§ Governance](#governance)).

**Does NOT grant:**

- No passive yield. No gauge boost. No `working_bytes` formula. No vote-escrow time decay. No delegator pool. No epoch-snapshot accounting. None of that contract surface exists.
- No optional locking for additional yield. The bond is binary: lock to operate; don't lock to not operate.

### FeeRouter split

`FeeRouter.routeSettlement(operator, bytesDelivered, amount)` splits incoming USDC across four buckets, all transferred in the settlement transaction.

| Destination | Share | Mechanic |
|---|---:|---|
| Operator base (direct, per-byte) | 60% | Same-tx USDC transfer to operator |
| Buyback-and-burn | 25% | TWAP USDC→TOKEN via Balancer V3 80/20 ([ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol)); TOKEN burned |
| Protocol treasury | 10% | Same-tx to Timelock-custodied wallet |
| Safety & insurance reserve | 5% | Same-tx to `SafetyReserve` ([ADR 033](033-safety-insurance-reserve.md#adr-033-safety-and-insurance-reserve)) |
| **Total** | **100%** | |

**Same-transaction guarantees.** All four buckets transfer in the settlement transaction. There are no epoch buckets, no pull-based claims, no claim windows. `FeeRouter.routeSettlement` does its full work in one tx, recovering the [ADR 003 § FeeRouter Integration](003-payments.md#feerouter-integration) one-tx invariant for every bucket. Per-epoch byte counters are retained for analytics and to feed the [§ Operator Service Emissions](#operator-service-emissions) distribution; they no longer drive bucket payouts.

**Node-to-node cache-miss paid pulls bypass the router.** Direct peer USDC payment, no skim. Internal cost-recovery flow, not net protocol revenue.

**Gross client rate.** $0.01/GB — at parity with Bunny.net's budget tier and 7–20× cheaper than major traditional CDNs. No deCDN-specific premium. The router's 40% non-base skim is absorbed by operator net revenue, recovered through TOKEN-economy exposure (capacity-growth lock demand, deflationary burn) and externally-funded pre-seed USDC subsidies.

**Operator-aligned share = 85%.** 60% direct + 25% burn (the burn benefits every bond-holder by raising TOKEN's mechanical demand). The burn benefits all bond-holders, whereas the previous design's gauge pool benefited only the ve-weighted subset.

**Value accrual mechanism.** Two prongs:

- **Capacity-growth lock demand.** Every new operator or tier upgrade is a new buyer of TOKEN to bond. The demand is mechanically tied to network capacity growth, not to a promise of yield.
- **Burn at 5× the prior design's rate.** 25% of fees, direct deflationary pressure.

The first prong is mechanically larger at scale than any individual prong of the prior three-prong (gauge / delegator / burn) design; the second is a direct 5× multiplier on the deflationary lever.

### Operator Service Emissions

Group 7 (20% / 200M TOKEN) is distributed as service emissions to active operators based on verified delivery. The bucket is interpreted as **payment for work** — operators are delivering bytes verified by the probe network — not as passive yield to holders. This is the canonical bucket alignment with the work-token framing.

- Distribution: probe-attested bytes × capacity tier × diminishing-returns curve.
- Auto-deposited into the operator's `CapacityBond` on a monthly (or per-epoch) cadence. Operators **cannot withdraw the granted TOKEN as liquid** until they unbond fully and exit operator status (subject to the 14-day unbonding window).
- Subsidies stack with the operator's tier-upgrade economics: an operator can use granted TOKEN to bond up to a higher tier (which then requires probe re-verification) instead of buying TOKEN at market.
- Emission schedule is front-loaded: ~40% in years 1–2 (S1→S3 bootstrap), tapering to ~20% in years 3–4 and ~10% per year thereafter until the 200M bucket exhausts (≈year 6 at default emission curve).
- Bucket sunsets when exhausted or by governance vote after the transition thresholds in [§ Governance](#governance) (active operator count ≥ 30 AND total declared capacity ≥ 100 Gbps) are met for ≥6 months.

**Public-facing label:** the bucket retains the "Staking Rewards" label in fundraising materials and on tokenomics-tool exports for recognizability. Term-sheet language and this ADR refer to it as **Operator Service Emissions** to align with the work-token framing. The label disagreement is documented in fundraising one-pagers as a glossary entry.

Precedent: Filecoin's storage-provider block rewards, Livepeer's orchestrator subsidies, and Helium's hotspot subsidies are direct analogs.

### Capacity-shortfall slashing

A new deterministic slashing path that replaces the prior design's wash-trading defense.

- If 4-week rolling verified delivery is below `min_delivery_ratio × declared_capacity`, the operator is auto-downgraded to the next-lower tier; the bond delta (`current_tier_bond − new_tier_bond`) is forfeit to `SafetyReserve`.
- Deterministic on probe data; no governance vote needed. Probe data is on-chain by virtue of the existing `cdn/probe/v1` attestation flow.
- `min_delivery_ratio` default 70%, governable within [50%, 90%]. Lower values are more forgiving; higher values are stricter.
- This obviates the need for the prior design's per-operator gauge-share cap as a wash-trading defense: an operator cannot inflate apparent network share by faking traffic, because actual delivery is measured against the declared tier.

### Slashing and burn

**Slashing rates.** 5% / 15% / 50% escalation tiers, lifetime offense counter (`uint32`, monotonically increasing), increasing reset periods, challenge-bond mechanics — unchanged from the prior design. Applied to the `CapacityBond` instead of a separate stake.

**Auto-ejection.** At 50% of minimum bond for the operator's declared tier (replaces the prior "50% of minimum stake").

**Slashing distribution.** **50% challenger / 30% SafetyReserve / 20% burn** — unchanged. The challenger share is the deterrent that pays for active enforcement; the SafetyReserve share funds user-harm incident recourse beyond pure deflation; the burn share preserves the deflationary deterrent at a level governance can recalibrate within [§ Governable parameters with safety bounds](#governable-parameters-with-safety-bounds).

**Buyback-and-burn inflow.** **25% of routed USDC** flows to `BuybackBurner` from `FeeRouter` (5× the prior design's 5%). [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol) mechanics (Balancer V3 80/20 swap, TWAP, `minTokenOut`, POL custody) are inherited; the 5× volume increase implications are addressed in that ADR.

**Operational constraint.** Burn must be TWAP-limited and liquidity-aware. Mature burn budgets can exceed available market depth, especially at low TOKEN prices — the [ADR 018 § TWAP policy](018-liquidity-strategy.md#twap-policy-subswapcount--1) governs the per-epoch liquidity ceiling (`epochLiquidityCapFraction`, default 10%, bounded `[1%, 30%]`).

### Governance

This is the design's strongest commitment.

**Voting weight.**

```
vote_weight(operator) = declared_capacity_Mbps × age_ramp(months_bonded)
age_ramp = min(months_bonded / 6, 1.0)
```

- Fresh bonds vote at zero; full weight at 6 months; half-weight at 3 months.
- Defends against "buy your way to instant governance" attacks (a hostile party cannot register a fleet of operators and immediately vote them).
- Vote weight scales with capacity (Mbps), not bond size. At α=1.2 raw bond would concentrate voting in edge-tier operators at ~252:1 vs entry-tier; capacity-based gives ~100:1.
- **Per-operator voting cap = 5% of total voting weight.** Direct mirror of the prior gauge-share cap, repurposed for governance.

**Non-operator TOKEN holders have ZERO voting weight.** Per the [§ Allocation](#allocation): Core Contributors (15%), Private Investors (16%), Treasury (15%), Public Sale (3%), Airdrops/Testnet (6%), Marketing (6%), Liquidity Provision (19%) — all hold or receive TOKEN, but **none can vote** unless they also bond it to operate.

**Why this is right:**

- The moment seed/team/holder classes get to vote on bond parameters, fee splits, etc., the regulatory framing weakens (now there's a "common enterprise" voting on profit-like decisions).
- Operator-only governance mirrors how Filecoin's storage-provider class and Helium's hotspot class are the load-bearing voting constituency in those networks.
- It enforces the work-token framing structurally rather than rhetorically.

**Non-operator holder protection** lives at the contract level (immutable share floors per [§ Governable parameters with safety bounds](#governable-parameters-with-safety-bounds)), not at the governance level. Operators cannot vote to push the operator-base share above 90% or burn below 5%; non-operator value accrual is structurally guaranteed within those bounds.

**Investor disposition is consistent with the existing entity design.** Per the entity-structure design § Pattern A (Legal Fiction Separation), the existing structure already excludes investors and other non-operator holders from DAO voting by structure — DAO governance is permissionless and no-KYC; investor influence is routed to the Labs (C-Corp) equity layer (Series A+ board seats, standard preferred-stock protective provisions, indirect TOKEN exposure via Labs' ~15% treasury allocation). Work-token does not narrow investor power *relative to that existing design*; it narrows DAO voting from "ve-lockers" to "operators" — a change to who-among-active-participants votes, not a removal of an investor right that ever existed in entity design. Term-sheet language should not promise the prior ve-lock passive-governance path, because that was never a designed-in investor right.

**Governance parameters.**

| Parameter | Value | Source |
|---|---|---|
| Voting source | `CapacityBond.capacityAt × age_ramp` | this ADR |
| Proposal threshold | 0.1% of total voting weight | matches [ADR 009](009-governance.md#adr-009-governance-model) |
| Quorum | 4% of total voting weight | matches [ADR 009](009-governance.md#adr-009-governance-model) |
| Voting delay | 1 day | matches [ADR 009](009-governance.md#adr-009-governance-model) |
| Voting period | 7 days | matches [ADR 009](009-governance.md#adr-009-governance-model) |
| Timelock | 48 hours | matches [ADR 009](009-governance.md#adr-009-governance-model) |
| Total governance latency | ≈10 days | matches [ADR 009](009-governance.md#adr-009-governance-model) |
| Delegation | EIP-712 (Governor Bravo pattern) — voting power delegable, bond itself non-delegable | this ADR |
| Per-operator voting cap | 5% of total voting weight | this ADR |

**Bootstrap governance — temporary multisig phase.**

The voting set is narrow at launch (likely <50 operators in the first 6–12 months). Direct application of capacity-weighted governance pre-bootstrap risks hostile takeover via a cheap operator-fleet setup.

- For the first 6–12 months, governance runs through a multisig with hard-cap pause powers (extends [ADR 009](009-governance.md#adr-009-governance-model)'s emergency-multisig pattern).
- Transition to full operator-weighted governance is auto-triggered when **active operator count ≥ 30** AND **total declared capacity ≥ 100 Gbps**. Both thresholds governable.
- Before transition, the multisig can execute parameter changes within the safety bounds in [§ Governable parameters with safety bounds](#governable-parameters-with-safety-bounds).

### Operator economics

**Bond is the only TOKEN-side requirement.** No separate flat minimum stake, no optional lock for additional yield.

**Revenue streams.**

1. **60% of every channel settlement** — direct USDC, same-tx, per-byte.
2. **Operator Service Emissions** (TOKEN-denominated) — auto-deposited into `CapacityBond`; not withdrawable until full unbond; sized at 20% of supply distributed over ~6 years.

**Genesis-day operator math (1 Gbps operator, no starting TOKEN).**

- Genesis: buys 50K TOKEN at POL discovery price (~$0.05–0.10 → $2,500–$5,000 capital outlay).
- Months 1–12: earns USDC fees from delivery + pre-seed USDC subsidy → roughly cost-neutral on bandwidth.
- Months 1–24: earns Operator Service Emissions → bond grows 50K → ~200K → ~795K (climbs 1G → ~3G → 10G tier).
- Month 12: operating at 5–10G tier, paid in USDC fees, governance-eligible (with age-ramp at full weight from month 6).
- Month 24: established mid-tier operator with bond financed primarily by service delivery, not capital injection.

### Safety and insurance reserve (5% bucket)

The 5% safety bucket is held in `SafetyReserve`, a governance-gated incident reserve covering incorrect-slashing / appeal reversals, relay / sequencer / payment-channel downtime, and bad-data incidents. Full specification is in [ADR 033](033-safety-insurance-reserve.md#adr-033-safety-and-insurance-reserve); the bucket share grows from the prior 3% to 5% under the v2.1 four-bucket split.

### POL allocation

The 19% Liquidity Provision allocation has two sub-allocations:

- **4pp Market-Maker partner allocation** (genesis-liquid). Distributed to vetted market-maker partners under standard MM agreements for two-sided quoting on CEXes and DEX aggregators.
- **15pp Protocol-Owned Liquidity** (treasury-deployed). Held by DAO Treasury and deployed as a single-sided 80% TOKEN position on the Balancer V3 80/20 pool per [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol), paired with the USDC arm from the pre-seed bootstrap. Earns trading fees (yield flows to Treasury, not to per-holder claims). Cannot be withdrawn without a governance proposal (timelock + quorum).

POL is *protocol-owned*; it doesn't create passive yield to any external holder. Trading-fee yield is treasury-direct (not re-routed through `FeeRouter`), preserving the FeeRouter's strict per-byte-settlement accounting. The deeper-than-typical 19% allocation is defensible for an infrastructure protocol focused on liquidity depth as a primary value-accrual lever; the absence of a Liquidity Mining program (15pp formerly deployed there is now POL) eliminates the residual Howey prong-4 exposure that an LP-token-yield program would carry.

### Bootstrap mechanism — pre-seed USDC

Bootstrap supply-side incentive is **$1M+ pre-seed USDC capital** (planning target: $3M), externally raised. USDC denomination insulates subsidy purchasing power from TOKEN price. The protocol commits to the funding mechanism (USDC, externally raised) and the size floor ($1M); the operational program structure is tracked separately as a foundation/team operational concern, not as a protocol decision.

Approximate use of pre-seed USDC:

| Use | Approx allocation | Notes |
|---|---:|---|
| Operator infrastructure subsidies (direct USDC) | ~55% | Covers VPS/bandwidth for first 12 months for early operators; pairs with [§ Operator Service Emissions](#operator-service-emissions) to make first-year operator unit economics positive |
| Genesis POL seed (USDC side of 80/20 Balancer) | ~30% | Pairs with the 15pp treasury-owned TOKEN POL position; the deeper POL allocation under v2.1 motivates a larger USDC-side seed |
| `SafetyReserve` genesis pre-fund (USDC) | ~10% | Covers incidents before fee inflows reach steady state |
| Audits, legal, contingency | ~5% | Operational, not protocol-bound |

[ADR 019](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow) is the canonical onboarding flow.

### Governable parameters with safety bounds

Router shares, capacity-curve parameters, and capacity-shortfall thresholds are governable, gated by 48-hour timelock per [ADR 009](009-governance.md#adr-009-governance-model), and bounded as below. Sum-to-100% across the four router shares is enforced on every governance update; updates that violate the sum or exceed any individual bound revert.

| Parameter | Default | Min | Max |
|---|---:|---:|---:|
| Operator base share | 60% | 40% | 90% |
| Burn share | 25% | 5% | 50% |
| Treasury share | 10% | 0% | 30% |
| Safety share | 5% | 0% | 20% |
| α (capacity-curve exponent) | 1.2 | 1.0 | 1.8 |
| k (capacity-curve constant, TOKEN) | 12.6 | bounded by 1G bond ∈ [10K, 200K] | — |
| `MAX_CAPACITY_PER_OPERATOR` | 200 Gbps | 50 Gbps | 1000 Gbps |
| `min_delivery_ratio` | 70% | 50% | 90% |
| `age_ramp_months` | 6 | 1 | 24 |
| Per-operator voting cap | 5% | 1% | 25% |
| Multisig-bootstrap transition: operator-count threshold | 30 | 10 | 200 |
| Multisig-bootstrap transition: capacity threshold | 100 Gbps | 10 Gbps | 1000 Gbps |
| Unbonding window | 14 days | 7 days | 60 days |

The 40% floor on the operator base share preserves the cashflow invariant — operators always receive enough liquid USDC to cover infrastructure costs even under extreme governance proposals. The 5% floor on burn and 0% floor on treasury / safety let governance simplify the launch configuration without dropping deflationary pressure entirely.

#### Setter contract-level bound enforcement

Parameter setters on `FeeRouter` and `CapacityBond` are role-gated via `AccessControl` and bound-checked at the contract level — bounds are enforced regardless of caller. A future automated controller granted the parameter-setter role operates within the same bounds; out-of-range writes revert. This makes the bounds above effective for any caller (governance proposals or additive controllers), without trusting the caller to self-clamp.

## Consequences

### Positive

- **Smaller contract surface.** Net subtractive vs the prior design: `VotingEscrow` and `DelegatorBuyer` are deleted; `FeeRouter` simplifies from six buckets to four with no epoch / claim / snapshot machinery; `StakingRegistry` is renamed `CapacityBond` with one added piece of capacity-curve logic; `OperatorEmissions` is a new but small contract. `SafetyReserve`, slashing primitive, payment channels, and POL/BuybackBurner carry over largely unchanged.
- **Cleaner regulatory posture on Howey prong 4.** Passive holding earns nothing. No delegator pool, no ve-lock yield, no per-holder claim on revenue. Operator-only governance + entity design Pattern A's existing exclusion of investors from DAO voting closes both the cashflow-rights and common-enterprise vectors.
- **Mechanical value-accrual lever.** Capacity-growth lock demand scales with network throughput; 25% burn provides 5× the prior design's deflationary pressure.
- **No cashflow crisis at the operator layer.** 60% liquid USDC per settlement is comfortably above infrastructure-cost coverage at the reference 1 Gbps / 30K GB/mo node.
- **USDC pre-seed eliminates TOKEN-price reflexivity in bootstrap.** Subsidy purchasing power does not collapse with TOKEN price.
- **Safety reserve creates enterprise-tier credibility.** Funded SLA-failure compensation makes the Enterprise tier sellable rather than purely best-effort decentralized.
- **Slashing funds user recourse.** 30% of slashed bond funds incident payouts via `SafetyReserve`; 20% burns; 50% rewards the challenger.
- **Wash-trading is structurally defeated.** Capacity-shortfall slashing measures actual delivery against the declared tier; faking traffic does not raise revenue share because revenue is per-byte at the FeeRouter, not pooled.

### Negative

- **Capital-cost-to-operate at edge tier.** The super-linear curve makes 100 Gbps + tiers expensive: ~12.6M TOKEN bond at the 100G tier. Mitigated by the Operator Service Emissions runway (operators can grow tiers using granted TOKEN rather than market buys) and α-tunability.
- **Governance bootstrap depends on multisig discipline.** First 6–12 months run through a multisig; capacity-weighted DAO voting kicks in only when the transition thresholds are met. Pre-transition parameter changes are constrained to the [§ Governable parameters with safety bounds](#governable-parameters-with-safety-bounds).
- **Smaller external LP base in year 1.** v2.1 forgoes the v2 Bootstrap LM subsidy that would have attracted external LPs and broadened the holder base. POL provides depth; external LP growth depends on organic trading-fee yield.
- **Per-byte burn flow may exceed market depth at low TOKEN prices.** 5× volume increase on `BuybackBurner` — [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol)'s per-epoch liquidity cap is now load-bearing.
- **Operator-only DAO is politically narrow.** Investors, team, and treasury hold TOKEN but cannot vote unless they also operate. This is the deliberate regulatory-cleanliness commitment; consistent with the entity design Pattern A.

### Risks

- **Capacity-claim verification at scale.** The 7-day initial probe window assumes probe throughput is non-binding. With 100+ operators registering concurrently in the first months, probe scheduling may need a queue/throttle. Tracked in [§ Deferred & Open](#deferred--open).
- **k governance volatility.** k=12.6 is a discovered constant for the chosen 1G target bond (50K TOKEN). Governance changes to k can shift the entire bond curve. The k bound is parameterized via the 1G-tier bond range rather than as a raw range to constrain volatility; see [§ Deferred & Open](#deferred--open).
- **Service-emission curve precision.** The front-loaded / tapering shape is described qualitatively; the exact mathematical form needs specification with explicit per-month emission rates. Recommend modeling in `finance/notebooks/` before locking in contract. Tracked in [§ Deferred & Open](#deferred--open).
- **POL governance surface.** The 19% POL position is large and needs explicit governance controls — see [ADR 018 § POL Governance](018-liquidity-strategy.md#pol-governance) for the canonical specification.
- **Convex-capture-style wrappers.** A third-party contract could pool operator bonds and issue liquid receipts (analog to Convex/Lido). This is structurally limited because the bond is tied to a specific operator identity and capacity claim, but a registry of "bond-financed operators" backed by such wrappers is plausible. Tracked in [§ Deferred & Open](#deferred--open).

## Cross-ADR Impact

- **[ADR 003 — Payment Model](003-payments.md#adr-003-payment-model):** `FeeRouter.routeSettlement` ABI and bucket count change (6 → 4). The same-tx settlement invariant is *strengthened* (now applies to all four buckets). Minor edits to §FeeRouter Integration.
- **[ADR 009 — Governance Model](009-governance.md#adr-009-governance-model):** Voting-weight source replaced — was `VotingEscrow.balanceOfAt`, now `CapacityBond.capacityAt × age_ramp`. Non-operator holders zero-weighted. Multisig bootstrap phase formalized with transition thresholds.
- **[ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md#adr-016-smart-contract-interaction-model):** `VotingEscrow` and `DelegatorBuyer` removed from Contract Inventory; `StakingRegistry` renamed `CapacityBond`; new `OperatorEmissions` contract added. FeeRouter simplifies. Class diagrams updated.
- **[ADR 018 — Liquidity Strategy](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol):** POL grows 10% → 19% (15pp redeployment from former LM bucket); BuybackBurner sees 5× volume. New §POL Governance section formalizes rebalance / withdraw / fee-accounting rules.
- **[ADR 028 — Slashing Appeals](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation):** Slashing applies to `CapacityBond` (not `StakingRegistry`); status flipped from Locked-for-implementation back to Draft pending the CapacityBond rebase.
- **[ADR 032 — SafetyReserve Appeal-Surface Contract Surface](032-safety-reserve-appeals-contract.md#adr-032-safetyreserve-appeal-surface-contract-surface):** Capacity reads replace ve-supply reads where relevant.
- **[ADR 034 — Gauge Boost and Voting Escrow](_history/034-gauge-boost-voting-escrow.md):** RETIRED. The gauge-boost mechanism, `VotingEscrow` contract, and per-operator gauge-share cap are replaced by the capacity-bond curve. Body archived verbatim in `_history/`.
- **[ADR 035 — Delegator Pool](_history/035-delegator-pool.md):** RETIRED. The 7% delegator bucket and `DelegatorBuyer` pipeline are deleted entirely; the freed 7pp is absorbed into the four-bucket split. Body archived verbatim in `_history/`.

## Deferred & Open

1. **k governance volatility.** k=12.6 is a discovered constant for the chosen 1G-tier target bond (50K TOKEN). The current bound parameterizes k indirectly via the 1G-tier bond range [10K, 200K TOKEN]; an alternative is to make k immutable post-genesis and only governable via a one-shot setter behind a higher quorum. Recommend modeling impact in finance notebooks before locking the convention.
2. **Probe-verification throughput at scale.** The 7-day initial probe window assumes probe throughput is non-binding. With 100+ operators registering concurrently in the first months, probe scheduling may need a queue/throttle. Resolution depends on probe-network capacity-planning analysis not yet complete.
3. **Service-emission curve form.** The front-loaded-tapering shape ([§ Operator Service Emissions](#operator-service-emissions)) is described qualitatively. The exact mathematical form (exponential decay vs linear taper vs step function) needs specification with explicit per-month emission rates. Recommend modeling in `finance/notebooks/` before locking the curve in contract.
4. **Liquid-bond wrappers.** A third-party contract could pool operator bonds and issue liquid receipts (analog to Convex/Lido). This isn't strictly possible under work-token because the bond is tied to a specific operator identity and capacity claim, but a registry of "bond-financed operators" backed by such wrappers is plausible. Flag for future ADR if observed.
5. **Cross-chain TOKEN holders.** TOKEN may be bridged. Bridged holders cannot operate on the canonical L2 and so cannot vote — this is consistent with operator-only governance but worth being explicit about. Particularly relevant for the Airdrops bucket if airdrop recipients are on other chains.
6. **POL trading-fee accounting.** The 19% POL position earns trading fees that flow to Treasury directly (not re-routed through `FeeRouter`). The default is to keep `FeeRouter` accounting strictly tied to per-byte settlement; a future revisiting ADR may consider routing POL fees through the four-bucket split if that improves predictability of treasury yield.
