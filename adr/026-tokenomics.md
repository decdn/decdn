# ADR 026: Tokenomics

**Date:** 2026-05-27
**Status:** Draft

## Context

The economic model — sitting on top of paid byte delivery ([ADR 003](003-payments.md#adr-003-payment-model)) and the slashing primitive ([ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)) — must hold up against four objectives, all weighted equally:

1. **Simplicity / smaller contract surface.** Audit burden and operational complexity scale with the number of cooperating contracts, epoch-bucket accounting paths, and pull-claim windows.
2. **Token value accrual.** TOKEN demand must scale with network usage; the demand mechanism must be mechanically tied to capacity growth rather than to a promise of yield.
3. **Operator decentralization.** Structurally favor a diverse operator set over a few large operators.
4. **Regulatory defensibility.** Avoid framing that resembles an investment contract under the Howey test — particularly the "solely from the efforts of others" prong.

This ADR is the canonical economic-model umbrella. It implements a **work-token model** (Livepeer / Helium / Filecoin lineage): every node operator bonds TOKEN proportional to the bandwidth capacity it serves, in a single capacity-gated contract. There is no passive yield to holders. Governance is operator-only.

### Inputs assumed by this ADR

Pre-launch design with no holder-compensation or contract-migration concerns. ~$1M+ pre-seed USDC capital secured (planning target $3M); program structure is operational and tracked separately. 2026 unmetered-bandwidth provider economics (1 Gbps VPS, 10 Gbps dedicated, 100 Gbps edge tiers); dedicated-bandwidth nodes are realistic at every scale band the protocol is sized for.

## Decision

The protocol's economic model is defined by the following sections.

### Supply and distribution

**Supply.** 1,000,000,000 TOKEN, fixed at genesis. No post-genesis minting function exists on the production token contract.

#### Burnability

TOKEN is `ERC20Burnable`; any contract may burn TOKEN it holds via `burn` / `burnFrom`. Burns reduce `totalSupply` and emit `Transfer(from, address(0), amount)`. The [§ Slashing and burn](#slashing-and-burn) slashing-burn path uses this; future contract surfaces that need a TOKEN sink integrate via the same standard interface without contract changes.

#### Allocation

Eleven groups summing to 100%.

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

**Internal / External rollup.** Internal (Core, Advisors, Seed, Private, Treasury, Misc Marketing) = 58%; External (POL, App Incentives, Market Making, Public Sale, Exchange Partnerships) = 42%.

**Full initial unlock (TGE Day 1).** Exactly three groups unlock 100% at genesis:

| Group | Allocation |
|---|---:|
| Protocol Owned Liquidity | 15% |
| Market Making | 5% |
| Public Sale | 5% |
| **Total fully unlocked at TGE** | **25%** |

This is the *unlock* set, not the *circulating sell-side float*. Of the 25% unlocked, only ~5–10% is circulating float — POL sits as a non-circulating treasury-owned LP position (removable only by governance) and the Market-Making allocation is a market-neutral two-sided position; both are "Not a seller" per [§ Sell-pressure profile](#sell-pressure-profile). Public Sale (≤5%) is the only fully-unlocked group that is a net seller.

The two remaining groups with no formal cliff/linear schedule — Misc. Marketing, PR, and KOLs (ad-hoc Treasury spend within the annual budget cap) and Exchange Partnerships (milestone-gated to CEX listings) — are **not** part of the TGE unlock set: neither is dumped at genesis. Misc Marketing releases only as it is spent; Exchange Partnerships releases only as milestones are met. Hence three fully-unlocked groups, not five.

**Genesis liquid float (TGE Day 1).**

| Source | Allocation |
|---|---:|
| Public Sale | up to 5% (genesis-liquid or 6mo lockup if regulatory posture requires) |
| Market-Maker partner allocation | 5% |
| **Total liquid float at TGE** | **~5–10%** |

The 15pp POL position is "in the pool" but not floating in the sense of being available to circulate — it sits as a treasury-owned LP position, removable only by governance.

#### Sell-pressure profile

A qualitative economic-modeling lens over the [§ Allocation](#allocation) table — not an on-chain mechanic. Each group is typed by the sell pressure its tokens are expected to exert once unlocked, derived from the vesting column and the holder's incentive. The Allocation column below mirrors [§ Allocation](#allocation) — edit percentages there first:

| Group | Allocation | Sell-pressure type | Potential seller |
|---|---:|---|---|
| Core Contributors | 15% | Moderate | Yes |
| DAO Treasury | 15% | Conservative | Yes |
| Protocol Owned Liquidity | 15% | Not a seller | No |
| App Incentives | 14% | Moderate | Yes |
| Seed Investors | 11% | Aggressive | Yes |
| Private Investors | 9% | Aggressive | Yes |
| Market Making | 5% | Not a seller | No |
| Misc. Marketing, PR, and KOLs | 5% | Not a seller | No |
| Public Sale | 5% | Aggressive | Yes |
| Advisors | 3% | Aggressive | Yes |
| Exchange Partnerships | 3% | Not a seller | No |

**Rollup.** Potential sellers = 72% (7 groups: Core Contributors, DAO Treasury, App Incentives, Seed, Private, Public Sale, Advisors); not-a-seller = 28% (4 groups: POL, Market Making, Misc Marketing, Exchange Partnerships). The Aggressive groups are the investor, advisor, and public-sale allocations whose tokens reach the holder fastest — short-cliff (Seed/Private 6mo, Advisors 6mo) or genesis-liquid (Public Sale) — relative to their cost basis; the not-a-seller groups are protocol-owned or market-neutral positions (POL is governance-locked liquidity; Market Making is two-sided; Misc Marketing and Exchange Partnerships are spent into the ecosystem rather than sold).

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

**Worked numbers at α=1.2, k=12.6.**

| Tier | Capacity | Bond | Bond per Mbps | Ratio vs 1G |
|---|---:|---:|---:|---:|
| Entry | 1 Gbps | 50,000 TOKEN | 50 | 1.0× |
| Mid | 10 Gbps | 795,000 TOKEN | 80 | 1.6× |
| Edge | 100 Gbps | 12,600,000 TOKEN | 126 | 2.5× |

The super-linear curve makes high-capacity operators pay more per Mbps. At α=1.2 the 100G tier pays 2.5× the 1G per-Mbps rate — the curve's decentralization-pressure target.

**Bond lifecycle.**

- `CapacityBond.register(declaredMbps)` deposits the bond and emits `CapacityClaimed(operator, Mbps)`. Declared capacity is operator-self-attested; it is not verified at registration. Vote weight, per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight), is sourced from `FeeRouter.bytesInWindow` (proven delivered bytes), not from declared capacity, so over-declaration does not translate into governance influence; the bond cost is the primary structural disincentive against tier inflation.
- Re-registration at a different tier is permitted at any time, subject to the same `bond_required(declaredMbps)` deposit/refund.
- **Unbonding window: 14 days, slashable during unbonding.** Sized to exceed the [ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence) 5-day `SlashJudge` evidence-presentation window with a 9-day safety margin so misbehavior detected just before unbond initiation still has bond available to slash; the `MAX_EVIDENCE_AGE_US < unbondingPeriod` invariant is enforced as a paired cross-parameter check on the `SlashJudge` and `CapacityBond` setters per [ADR 014 § Interaction with unbonding period](014-on-chain-verification.md#interaction-with-unbonding-period).

**Supply impact across scale scenarios.**

Scale scenarios denote total monthly traffic delivered by the network. Peak capacity assumes ~30% average-of-peak utilization (`peak_Gbps ≈ PB/month × 10.3`). Operator mixes are illustrative — the design imposes no preferred mix.

| Scenario | Monthly traffic | Peak capacity | Representative operator mix | Total bonded (α=1.2) | % of 1B supply |
|---|---:|---:|---|---:|---:|
| S1 | 1 PB | ~10 Gbps | 10 × 1G | 500K TOKEN | 0.05% |
| S2 | 10 PB | ~100 Gbps | 50 × 1G + 5 × 10G | 6.475M TOKEN | ~0.6% |
| S3 | 100 PB | ~1 Tbps | 100 × 1G + 30 × 10G + 5 × 100G | 91.85M TOKEN | ~9.2% |
| S4 | 500 PB | ~5 Tbps | 500 × 1G + 100 × 10G + 30 × 100G | 482.5M TOKEN | ~48% |

S1 and S2 leave essentially all TOKEN liquid; [§ Genesis Bond Credits](#genesis-bond-credits) (50M / 5%) covers operator tier upgrades for the testnet-eligible cohort through year 2 without external TOKEN buys; non-testnet operators bond TOKEN purchased on market. S3 is the steady-state zone — ~9% bonded gives meaningful demand without supply-lockup pressure. S4 is the design-tension zone: at default α=1.2 the curve absorbs ~half of supply; α-tuning is the lever (at α=1.0 the same S4 mix bonds ~23%).

### What the bond grants and does not grant

**Grants:**

- Right to register as an active operator at the declared capacity tier.
- 100% of the operator share of the fee split (60% — see [§ FeeRouter split](#feerouter-split)).
- Eligibility to receive [§ Genesis Bond Credits](#genesis-bond-credits) if the operator participated in the pre-launch incentivized testnet.
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

**Same-transaction guarantees.** All four buckets transfer in the settlement transaction. There are no epoch buckets, no pull-based claims, no claim windows. `FeeRouter.routeSettlement` does its full work in one tx, recovering the [ADR 003 § FeeRouter Integration](003-payments.md#feerouter-integration) one-tx invariant for every bucket. Per-epoch byte counters do not drive bucket payouts; they are read by `DecdnGovernor` as the served-bytes voting-weight source per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight).

**Node-to-node cache-miss paid pulls bypass the router.** Direct peer USDC payment, no skim. Internal cost-recovery flow, not net protocol revenue.

**Gross client rate.** $0.01/GB — at parity with Bunny.net's budget tier and 7–20× cheaper than major traditional CDNs. No deCDN-specific premium. The router's 40% non-base skim is absorbed by operator net revenue, recovered through TOKEN-economy exposure (capacity-growth lock demand, deflationary burn) and externally-funded pre-seed USDC subsidies.

**Operator-aligned share = 85%.** 60% direct + 25% burn. The burn raises TOKEN's mechanical demand and benefits every bond-holder uniformly.

**Value accrual mechanism.** Two prongs:

- **Capacity-growth lock demand.** Every new operator or tier upgrade is a new buyer of TOKEN to bond. The demand is mechanically tied to network capacity growth, not to a promise of yield.
- **Deflationary burn.** 25% of routed USDC is swapped to TOKEN and burned per [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol).

### Genesis Bond Credits

Group 2 carves 5pp / 50M TOKEN at TGE for **Genesis Bond Credits** to verified pre-launch testnet operators. Purpose: fund a bounded year-1 operator-tier-upgrade runway with no ongoing emission. A retroactive, one-shot grant — not a service-conditional distribution schedule.

**Eligibility.** Pre-launch incentivized-testnet operators only. Per-operator allocation is computed at TGE as a weighted score of measured testnet contribution:

```
score(op) = bytes_delivered(op) × uptime_ratio(op) × probe_success_ratio(op)
allocation(op) = 50M × score(op) / Σ score(all eligible operators)
```

Exact normalization, minimum thresholds, and per-operator caps are open questions — see [§ Deferred & Open](#deferred--open). No post-TGE application window; no anchor-operator discretionary carve.

**Distribution at TGE.** Treasury executes a single batched `CapacityBond.grantGenesisCredit(operator, amount)` per eligible operator within a one-shot TGE window (default: 30 days post-deploy). After the window closes, the grant function is permanently disabled on-chain. TOKEN is auto-deposited directly into the operator's `CapacityBond` position; never enters the operator's wallet at any point before vest.

**Vesting.** 24 months from TGE, linear by epoch. Vest accrues only if the operator is `isActive(op) && !isSlashed(op)` during the epoch. No minimum bytes-delivered threshold — non-delivery is already handled by the existing slashing pipeline.

**Slashing.** The full pending credit (`pendingCredit.total - pendingCredit.vested`) is slashable on the same terms as the operator's voluntary bond. Slashed amounts route to `SafetyReserve` per [ADR 033](033-safety-insurance-reserve.md#adr-033-safety-and-insurance-reserve). Vested-but-unclaimed credit is no longer slashable — it has functionally become voluntarily-bonded TOKEN; this is reflected in [ADR 028](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation).

**Exit before 24mo.** On voluntary unbond, the vested portion follows the standard 14-day unbonding window; the unvested portion is transferred back to the operational Treasury via the existing Treasury reference. The grant is one-shot per operator — an exited operator who re-registers does not recover the forfeited unvested portion.

**Howey framing.** This is a retroactive grant for prior verifiable work (testnet), with a retention-style vesting cliff. Structurally distinct from ongoing service emission: the work that earned the grant is complete at TGE; the cliff is a retention incentive, not payment for ongoing service. Shape matches founding-employee RSU grants, not yield to passive bonders.

### App Incentives

Group 4 (14% / 140M TOKEN) funds demand-side adoption — publishers serving content via deCDN and apps that integrate deCDN as their CDN backend. A customer-acquisition incentive aimed at consumers of the service, not at TOKEN bonders.

**Distribution mechanism.** 4-year linear unlock into a dedicated Timelock-controlled multisig (separate wallet from operational Treasury for accounting cleanliness). No new on-chain contract at launch; both sub-programs are Treasury-multisig-administered. A future `PublisherRebateRouter` contract may subsume the rebate flow post-launch — see [§ Deferred & Open](#deferred--open).

**Sub-programs (governance norm, not on-chain enforced).** The 10/4 split below is a DAO-rebalanceable norm within the 14% envelope.

1. **Publisher Rebates (~10pp / 100M TOKEN indicative).** Quarterly TOKEN rebate to enrolled publishers whose USDC fee contributions exceed a minimum threshold. Mechanism:
   - Publishers self-identify by signing a rebate-program agreement and completing light KYC.
   - Each quarter, Treasury multisig audits served-bytes attributed to each enrolled publisher using `FeeRouter` accounting data and probe-verified delivery.
   - Rebate paid in TOKEN, denominated as a DAO-tunable fraction of the publisher's quarterly USDC FeeRouter contribution.
   - Indicative quarterly budget: (140M × 10/14) / 16 quarters ≈ 6.25M / quarter; unused budget rolls forward.

2. **Integration Grants (~4pp / 40M TOKEN indicative).** Milestone-based lump-sum grants to projects integrating deCDN as their CDN backend. Targets: CMS plugins, framework adapters, hosting platforms, language SDKs beyond Rust. Per-project ceiling is DAO-tunable; indicative range 50K–500K TOKEN.

**Howey framing.** Publisher Rebates are TOKEN-denominated discounts to paying customers — analogous to airline frequent-flyer miles or AWS cloud credits, not investment contracts. Integration Grants are milestone-based work-for-hire payouts. Neither is conditional on the recipient holding TOKEN; neither generates an expectation of profit from "the efforts of others."

**Constraints.** App Incentives recipients are **not** eligible for [§ Genesis Bond Credits](#genesis-bond-credits) and vice versa. Co-marketing spend (case studies, conferences, advertising) is funded from the separate Misc. Marketing bucket (group 8), not from App Incentives.

### Slashing and burn

**Slashing rates.** 5% / 15% / 50% escalation tiers, lifetime offense counter (`uint32`, monotonically increasing), increasing reset periods, challenge-bond mechanics. Applied to the `CapacityBond`.

**Auto-ejection.** At 50% of minimum bond for the operator's declared tier.

**Slashing distribution.** **50% challenger / 30% SafetyReserve / 20% burn**. The challenger share is the deterrent that pays for active enforcement; the SafetyReserve share funds user-harm incident recourse beyond pure deflation; the burn share preserves the deflationary deterrent at a level governance can recalibrate within [§ Governable parameters with safety bounds](#governable-parameters-with-safety-bounds).

**Buyback-and-burn inflow.** **25% of routed USDC** flows to `BuybackBurner` from `FeeRouter`. [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol) specifies the Balancer V3 80/20 swap, TWAP, `minTokenOut`, POL custody, and per-epoch liquidity cap.

**Operational constraint.** Burn must be TWAP-limited and liquidity-aware. Mature burn budgets can exceed available market depth, especially at low TOKEN prices — the [ADR 018 § TWAP policy](018-liquidity-strategy.md#twap-policy-subswapcount--1) governs the per-epoch liquidity ceiling (`epochLiquidityCapFraction`, default 10%, bounded `[1%, 30%]`).

### Governance

This is the design's strongest commitment.

**Voting weight.**

```
vote_weight(op, t) = min(
    served_bytes_window(op, t),
    voteCapBps × total_bytes_window(t) / 10_000
) × age_ramp(op, t)

served_bytes_window(op, t) = Σ_{e = epoch(t)-N+1 .. epoch(t)} FeeRouter.bytesPerEpoch[op][e]
age_ramp(op, t)            = min((t − CapacityBond.firstBondedAt[op]) / (age_ramp_months × seconds_per_month), 1.0)
N                          = windowEpochs                                               // default 13 (~1 quarter)
```

- Fresh bonds vote at zero (no served bytes); full weight requires both `age_ramp_months` of tenure and sustained delivery across the `windowEpochs` trailing window.
- Defends against "buy your way to instant governance" attacks on both axes: `age_ramp` gates speed-to-influence by tenure, and the rolling bytes window requires sustained activity.
- Vote weight scales with demonstrated served bytes, not declared capacity, not bond size — see [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) for the full derivation.
- **Per-operator voting cap = 5% of total bytes-weighted weight** (governable `[1%, 25%]`). Single biggest carrier still capped at 5%; cap is the primary defense against bytes-weighted concentration in a power-law-skewed CDN traffic distribution.
- **Slashing zero-out.** Any slash stamps `CapacityBond.slashedAtEpoch[op]`; vote weight is zero for the operator while `slashedAtEpoch[op]` falls inside the trailing window. Successful slash-appeal reversal via [ADR 028](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation)'s `reverseAppeal` clears the field. See [ADR 036 § Slashing zero-out](036-served-bytes-voting-weight.md#slashing-zero-out).

**Non-operator TOKEN holders have ZERO voting weight.** Per the [§ Allocation](#allocation) categorical rollup: Core Contributors (18%), Private Investors (20%), Treasury (15%), Public Sale (5%), Ecosystem Incentives / App Incentives (14%), Marketing (8%), Liquidity Provision (20%) — 100% of supply — **none can vote** unless they also bond TOKEN to operate. Served-bytes voting weight per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) further ties weight to delivered bytes, so a TOKEN holder who bonds without actually delivering bytes still accrues zero vote weight.

**Why this is right:**

- The moment seed/team/holder classes get to vote on bond parameters, fee splits, etc., the regulatory framing weakens (now there's a "common enterprise" voting on profit-like decisions).
- Operator-only governance mirrors how Filecoin's storage-provider class and Helium's hotspot class are the load-bearing voting constituency in those networks.
- It enforces the work-token framing structurally rather than rhetorically.

**Non-operator holder protection** lives at the contract level (immutable share floors per [§ Governable parameters with safety bounds](#governable-parameters-with-safety-bounds)), not at the governance level. Operators cannot vote to push the operator-base share above 90% or burn below 5%; non-operator value accrual is structurally guaranteed within those bounds.

**Investor disposition is consistent with the existing entity design.** Per the entity-structure design § Pattern A (Legal Fiction Separation), the existing structure already excludes investors and other non-operator holders from DAO voting by structure — DAO governance is permissionless and no-KYC; investor influence is routed to the Labs (C-Corp) equity layer (Series A+ board seats, standard preferred-stock protective provisions, indirect TOKEN exposure via Labs' ~15% treasury allocation). DAO voting is restricted to operators — a change to who-among-active-participants votes, not a removal of an investor right that ever existed in entity design. Term-sheet language should not promise a ve-lock passive-governance path, because that was never a designed-in investor right.

**Governance parameters.**

| Parameter | Value | Source |
|---|---|---|
| Voting source | `FeeRouter.bytesInWindow` + `FeeRouter.totalBytesInWindow` × `age_ramp(CapacityBond.firstBondedAt)`; zeroed for `windowEpochs` after slash via `CapacityBond.slashedAtEpoch` | [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) |
| Proposal threshold | 0.1% of total bytes-weighted weight | matches [ADR 009](009-governance.md#adr-009-governance-model) |
| Quorum | 4% of total bytes-weighted weight | matches [ADR 009](009-governance.md#adr-009-governance-model) |
| Voting delay | 1 day | matches [ADR 009](009-governance.md#adr-009-governance-model) |
| Voting period | 7 days | matches [ADR 009](009-governance.md#adr-009-governance-model) |
| Timelock | 48 hours | matches [ADR 009](009-governance.md#adr-009-governance-model) |
| Total governance latency | ≈10 days | matches [ADR 009](009-governance.md#adr-009-governance-model) |
| Delegation | EIP-712 (Governor Bravo pattern) — voting power delegable, bond itself non-delegable | this ADR |
| Per-operator voting cap | 5% of total bytes-weighted weight | this ADR ([ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) for full formula) |
| `windowEpochs` (served-bytes trailing window) | 13 (~1 quarter) | [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) |

**Bootstrap governance — temporary multisig phase.**

The voting set is narrow at launch (likely <50 operators in the first 6–12 months). Direct application of capacity-weighted governance pre-bootstrap risks hostile takeover via a cheap operator-fleet setup.

- For the first 6–12 months, governance runs through a multisig with hard-cap pause powers (extends [ADR 009](009-governance.md#adr-009-governance-model)'s emergency-multisig pattern).
- Transition to full operator-weighted governance is auto-triggered when **active operator count ≥ 30** AND **total declared capacity ≥ 100 Gbps**. Both thresholds governable.
- Before transition, the multisig can execute parameter changes within the safety bounds in [§ Governable parameters with safety bounds](#governable-parameters-with-safety-bounds).

### Operator economics

**Bond is the only TOKEN-side requirement.** No separate flat minimum stake, no optional lock for additional yield.

**Revenue streams.**

1. **60% of every channel settlement** — direct USDC, same-tx, per-byte.
2. **Genesis Bond Credits** (TOKEN-denominated, testnet-eligible operators only) — auto-deposited into `CapacityBond` at TGE; vests linearly over 24mo via continued operation; not withdrawable as liquid until vested and unbonded; sized at 5% of supply / 50M TOKEN.

**Genesis-day operator math (1 Gbps operator, no starting TOKEN).**

- Genesis: buys 50K TOKEN at POL discovery price (~$0.05–0.10 → $2,500–$5,000 capital outlay).
- Months 1–12: earns USDC fees from delivery + pre-seed USDC subsidy → roughly cost-neutral on bandwidth.
- Months 1–24 (testnet-eligible operator): Genesis Bond Credit auto-bonded at TGE; vests linearly. For a median testnet operator at ~500K TOKEN credit, bond effectively climbs from voluntary 50K → 50K+vested as continued operation accrues. Non-testnet operator at the same scale must purchase TOKEN on market to climb 1G → ~3G → 10G tier and is excluded from credit eligibility.
- Month 12: operating at 5–10G tier, paid in USDC fees, governance-eligible (with age-ramp at full weight from month 6).
- Month 24: established mid-tier operator with bond financed primarily by service delivery, not capital injection.

### Safety and insurance reserve (5% bucket)

The 5% safety bucket is held in `SafetyReserve`, a governance-gated incident reserve covering incorrect-slashing / appeal reversals, relay / sequencer / payment-channel downtime, and bad-data incidents. Full specification is in [ADR 033](033-safety-insurance-reserve.md#adr-033-safety-and-insurance-reserve).

### Liquidity Provision allocation (POL + Market Making)

The 20% Liquidity Provision category splits across two top-level groups:

- **Market Making — 5pp / 50M TOKEN** (group 7, genesis-liquid). Distributed to vetted market-maker partners under standard MM agreements for two-sided quoting on CEXes and DEX aggregators.
- **Protocol-Owned Liquidity — 15pp / 150M TOKEN** (group 3, treasury-deployed). Held by DAO Treasury and deployed as a single-sided 80% TOKEN position on the Balancer V3 80/20 pool per [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol), paired with the USDC arm from the pre-seed bootstrap. Earns trading fees (yield flows to Treasury, not to per-holder claims). Cannot be withdrawn without a governance proposal (timelock + quorum).

POL is *protocol-owned*; it doesn't create passive yield to any external holder. Trading-fee yield is treasury-direct (not re-routed through `FeeRouter`), preserving the FeeRouter's strict per-byte-settlement accounting. The deeper-than-typical 20% combined allocation is defensible for an infrastructure protocol focused on liquidity depth as a primary value-accrual lever; the absence of a Liquidity Mining program eliminates the residual Howey prong-4 exposure that an LP-token-yield program would carry.

### Bootstrap mechanism — pre-seed USDC

Bootstrap supply-side incentive is **$1M+ pre-seed USDC capital** (planning target: $3M), externally raised. USDC denomination insulates subsidy purchasing power from TOKEN price. The protocol commits to the funding mechanism (USDC, externally raised) and the size floor ($1M); the operational program structure is tracked separately as a foundation/team operational concern, not as a protocol decision.

Approximate use of pre-seed USDC:

| Use | Approx allocation | Notes |
|---|---:|---|
| Operator infrastructure subsidies (direct USDC) | ~55% | Covers VPS/bandwidth for first 12 months for early operators; pairs with [§ Genesis Bond Credits](#genesis-bond-credits) (for testnet-eligible operators) to make first-year operator unit economics positive |
| Genesis POL seed (USDC side of 80/20 Balancer) | ~30% | Pairs with the 15pp treasury-owned TOKEN POL position; sized to support the 15pp POL allocation |
| `SafetyReserve` genesis pre-fund (USDC) | ~10% | Covers incidents before fee inflows reach steady state |
| Audits, legal, contingency | ~5% | Operational, not protocol-bound |

[ADR 019](019-node-onboarding.md#adr-019-node-onboarding-and-bootstrapping-flow) is the canonical onboarding flow.

### Governable parameters with safety bounds

Router shares and capacity-curve parameters are governable, gated by 48-hour timelock per [ADR 009](009-governance.md#adr-009-governance-model), and bounded as below. Sum-to-100% across the four router shares is enforced on every governance update; updates that violate the sum or exceed any individual bound revert.

| Parameter | Default | Min | Max |
|---|---:|---:|---:|
| Operator base share | 60% | 40% | 90% |
| Burn share | 25% | 5% | 50% |
| Treasury share | 10% | 0% | 30% |
| Safety share | 5% | 0% | 20% |
| α (capacity-curve exponent) | 1.2 | 1.0 | 1.8 |
| k (capacity-curve constant, TOKEN) | 12.6 | bounded by 1G bond ∈ [10K, 200K] | — |
| `MAX_CAPACITY_PER_OPERATOR` | 200 Gbps | 50 Gbps | 1000 Gbps |
| `age_ramp_months` | 6 | 1 | 24 |
| Per-operator voting cap | 5% | 1% | 25% |
| `windowEpochs` (served-bytes voting window, on `FeeRouter`) | 13 | 4 | 26 |
| Multisig-bootstrap transition: operator-count threshold | 30 | 10 | 200 |
| Multisig-bootstrap transition: capacity threshold | 100 Gbps | 10 Gbps | 1000 Gbps |
| Unbonding window | 14 days | 7 days | 60 days |

The 40% floor on the operator base share preserves the cashflow invariant — operators always receive enough liquid USDC to cover infrastructure costs even under extreme governance proposals. The 5% floor on burn and 0% floor on treasury / safety let governance simplify the launch configuration without dropping deflationary pressure entirely.

#### Setter contract-level bound enforcement

Parameter setters on `FeeRouter` and `CapacityBond` are role-gated via `AccessControl` and bound-checked at the contract level — bounds are enforced regardless of caller. A future automated controller granted the parameter-setter role operates within the same bounds; out-of-range writes revert. This makes the bounds above effective for any caller (governance proposals or additive controllers), without trusting the caller to self-clamp.

## Consequences

### Positive

- **Smaller contract surface.** The contract set is `CapacityBond` (operator registry, capacity-curve bond, slashing, `PendingCredit` vesting for Genesis Bond Credits), `FeeRouter` (four-bucket same-tx split), `SafetyReserve`, `BuybackBurner`, `DecdnGovernor`, `TimelockController`, `PaymentChannel`, and `TOKEN`. No epoch / claim / snapshot machinery; no separate emissions contract.
- **Cleaner regulatory posture on Howey prong 4.** Passive holding earns nothing. No delegator pool, no ve-lock yield, no per-holder claim on revenue. Operator-only governance + entity design Pattern A's existing exclusion of investors from DAO voting closes both the cashflow-rights and common-enterprise vectors.
- **Mechanical value-accrual lever.** Capacity-growth lock demand scales with network throughput; the 25% burn share is the second deflationary prong.
- **No cashflow crisis at the operator layer.** 60% liquid USDC per settlement is comfortably above infrastructure-cost coverage at the reference 1 Gbps / 30K GB/mo node.
- **USDC pre-seed eliminates TOKEN-price reflexivity in bootstrap.** Subsidy purchasing power does not collapse with TOKEN price.
- **Safety reserve creates enterprise-tier credibility.** Funded SLA-failure compensation makes the Enterprise tier sellable rather than purely best-effort decentralized.
- **Slashing funds user recourse.** 30% of slashed bond funds incident payouts via `SafetyReserve`; 20% burns; 50% rewards the challenger.
- **Wash-trading is structurally defeated.** Operator revenue is per-byte at the `FeeRouter` (the operator base is paid by the client, not pooled), so faking traffic does not raise revenue. Governance vote weight is sourced from `FeeRouter.bytesInWindow` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight) — proven delivered bytes, not declared capacity — so over-declaring a tier does not translate into governance influence either. The super-linear bond curve makes over-declared capacity a dead-capital drag with no governance or revenue upside.

### Negative

- **Capital-cost-to-operate at edge tier.** The super-linear curve makes 100 Gbps + tiers expensive: ~12.6M TOKEN bond at the 100G tier. Mitigated for testnet-eligible operators by the [§ Genesis Bond Credits](#genesis-bond-credits) program and by α-tunability; non-testnet operators must buy TOKEN on market to climb tiers, by design.
- **Governance bootstrap depends on multisig discipline.** First 6–12 months run through a multisig; capacity-weighted DAO voting kicks in only when the transition thresholds are met. Pre-transition parameter changes are constrained to the [§ Governable parameters with safety bounds](#governable-parameters-with-safety-bounds).
- **Smaller external LP base in year 1.** No Liquidity Mining subsidy means external LP growth depends on organic trading-fee yield. POL provides the depth.
- **Per-byte burn flow may exceed market depth at low TOKEN prices.** [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol)'s per-epoch liquidity cap on `BuybackBurner` is load-bearing.
- **Operator-only DAO is politically narrow.** Investors, team, and treasury hold TOKEN but cannot vote unless they also operate. This is the deliberate regulatory-cleanliness commitment; consistent with the entity design Pattern A.

### Risks

- **k governance volatility.** k=12.6 is a discovered constant for the chosen 1G target bond (50K TOKEN). Governance changes to k can shift the entire bond curve. The k bound is parameterized via the 1G-tier bond range rather than as a raw range to constrain volatility; see [§ Deferred & Open](#deferred--open).
- **Genesis Bond Credit weighting precision.** The testnet-contribution score formula in [§ Genesis Bond Credits](#genesis-bond-credits) is described in skeletal form; exact normalization, minimum thresholds, and per-operator caps need specification before the TGE grant window opens. Recommend modeling in `finance/notebooks/` against testnet telemetry. Tracked in [§ Deferred & Open](#deferred--open).
- **POL governance surface.** The 15% POL position (group 3) is large — and combined with the 5% MM allocation (group 7) the 20% Liquidity-Provision category is at the upper end of the typical 5–15% DeFi range — and needs explicit governance controls. See [ADR 018 § POL Governance](018-liquidity-strategy.md#pol-governance) for the canonical specification.
- **Convex-capture-style wrappers.** A third-party contract could pool operator bonds and issue liquid receipts (analog to Convex/Lido). This is structurally limited because the bond is tied to a specific operator identity and capacity claim, but a registry of "bond-financed operators" backed by such wrappers is plausible. Tracked in [§ Deferred & Open](#deferred--open).

## Cross-ADR Impact

- **[ADR 003 — Payment Model](003-payments.md#adr-003-payment-model):** `FeeRouter.routeSettlement` distributes to four buckets, and the same-tx settlement invariant holds across all four (see §FeeRouter Integration).
- **[ADR 009 — Governance Model](009-governance.md#adr-009-governance-model):** Voting-weight source is `FeeRouter`-derived served-bytes weight per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight). Non-operator holders carry zero weight. The multisig bootstrap phase has explicit transition thresholds.
- **[ADR 036 — Served-Bytes Voting Weight](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight):** Defines the canonical DAO voting-weight formula the §Governance "Voting weight" section above points to. Vote weight is `FeeRouter.bytesInWindow × age_ramp`, capped per-operator at `voteCapBps` against the bytes-weighted total, zeroed if `CapacityBond.slashedAtEpoch` falls inside the trailing window. `windowEpochs` (default 13) is a governable parameter. Because vote weight derives from proven delivered bytes rather than declared capacity, the design has no capacity-shortfall slashing path, no `min_delivery_ratio`, and no registration probe gate; the `cdn/probe/v1` ALPN serves [ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence) phantom-blob evidence and operator latency/availability discovery, not declared-capacity enforcement.
- **[ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md#adr-016-smart-contract-interaction-model):** `CapacityBond` holds the `PendingCredit` vesting state and exposes `grantGenesisCredit` / `accrueGenesisVest` / `claimVestedCredit`. `FeeRouter` is the four-bucket settlement distributor. Class diagrams reflect this surface.
- **[ADR 018 — Liquidity Strategy](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol):** POL is 15% (group 3); MM is 5% (group 7); combined Liquidity-Provision category is 20%. `BuybackBurner` receives 25% of routed USDC at every settlement. §POL Governance formalizes rebalance / withdraw / fee-accounting rules.
- **[ADR 028 — Slashing Appeals](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation):** Slashing applies to `CapacityBond`. `PendingCredit` is slashable alongside `bondedAmount` per [§ Genesis Bond Credits](#genesis-bond-credits).
- **[ADR 032 — SafetyReserve Appeal-Surface Contract Surface](032-safety-reserve-appeals-contract.md#adr-032-safetyreserve-appeal-surface-contract-surface):** Reads operator capacity from `CapacityBond` where relevant.
- **[ADR 034 — Gauge Boost and Voting Escrow](_history/034-gauge-boost-voting-escrow.md):** RETIRED. The gauge-boost mechanism, `VotingEscrow` contract, and per-operator gauge-share cap are replaced by the capacity-bond curve. Body archived verbatim in `_history/`.
- **[ADR 035 — Delegator Pool](_history/035-delegator-pool.md):** RETIRED. The 7% delegator bucket and `DelegatorBuyer` pipeline are deleted entirely; the freed 7pp is absorbed into the four-bucket split. Body archived verbatim in `_history/`.

## Deferred & Open

1. **k governance volatility.** k=12.6 is a discovered constant for the chosen 1G-tier target bond (50K TOKEN). The current bound parameterizes k indirectly via the 1G-tier bond range [10K, 200K TOKEN]; an alternative is to make k immutable post-genesis and only governable via a one-shot setter behind a higher quorum. Recommend modeling impact in finance notebooks before locking the convention.
2. **Testnet-contribution weighting formula.** The score formula in [§ Genesis Bond Credits](#genesis-bond-credits) is given in skeletal form. Exact normalization, minimum thresholds, and per-operator caps need specification before the TGE grant window opens. Recommend modeling in `finance/notebooks/` against testnet telemetry.
3. **PublisherRebateRouter trigger.** If publisher-rebate volume grows large enough (e.g., > 4M TOKEN rebated per quarter for two consecutive quarters), a programmatic `PublisherRebateRouter` contract may replace the Treasury-multisig flow. Deferred to post-launch.
4. **App Incentives 10/4 split governance.** The publisher-rebate / integration-grant split (100M / 40M indicative) is a governance norm, not on-chain enforced. Confirm DAO can rebalance within the 14% envelope without requiring an ADR amendment.
5. **Liquid-bond wrappers.** A third-party contract could pool operator bonds and issue liquid receipts (analog to Convex/Lido). This isn't strictly possible under work-token because the bond is tied to a specific operator identity and capacity claim, but a registry of "bond-financed operators" backed by such wrappers is plausible. Flag for future ADR if observed.
6. **Cross-chain TOKEN holders.** TOKEN may be bridged. Bridged holders cannot operate on the canonical L2 and so cannot vote — this is consistent with operator-only governance but worth being explicit about. Most relevant for any holder cohort distributed without an operating expectation (Public Sale, MM-partner allocations).
7. **POL trading-fee accounting.** The 15% POL position (group 3) earns trading fees that flow to Treasury directly (not re-routed through `FeeRouter`). The default is to keep `FeeRouter` accounting strictly tied to per-byte settlement; a future revisiting ADR may consider routing POL fees through the four-bucket split if that improves predictability of treasury yield.
