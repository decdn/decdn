# ADR 030: Pre-Seed USDC Deployment Program

**Date:** 2026-04-25
**Status:** Draft
**Funds:** [ADR 026](026-tokenomics-v3.md) §10 (bootstrap mechanism)
**Replaces:** ADR 004's 200M-TOKEN node-bootstrap fund

---

## Context

[ADR 004](004-tokenomics.md) allocated **200M TOKEN** as a node-bootstrap fund. This is
reflexive: TOKEN price drops collapse subsidy purchasing power exactly when subsidies
are most needed. Single-asset reflexivity was the dominant tail risk for early operator
recruitment ([ADR 026](026-tokenomics-v3.md) §Context, item 4).

[ADR 026](026-tokenomics-v3.md) §10 replaces the 200M-TOKEN fund with **$1M+ pre-seed
USDC capital** (planning target $3M), externally raised and USD-denominated. ADR 026
commits the funding mechanism and the size floor; it forward-references this ADR for
the program structure.

This ADR is the program charter — capital structure, the five funded programs with
per-program eligibility / allocation / success metrics / termination triggers,
reporting cadence, `SafetyReserve` coordination, and the program-wide wind-down trigger.

The pre-seed program runs **in parallel** with the canonical self-funded onboarding
flow in [ADR 019](019-node-onboarding.md). It does not replace any phase of onboarding;
it supplies stake, hardware, regional capital, or SLA backing to operators who would
otherwise be filtered out at Phase 1 (server provisioning) or Phase 2 (on-chain stake).

**Sizing context.** Per the economic-model spec §0–§3: S0 (Bootstrap, ~178 nodes)
treasury USDC inflow ~$20K/mo; S1 (Early, ~1,778 nodes) ~$200K/mo (self-funding); S2
(Growth) ~$2M/mo. Pre-seed is sized for the S0→S1 transition — where operator-side
reflexivity bites hardest and treasury inflow has not yet caught up.

---

## Decision

### 1. Capital structure

| Item | Value |
| --- | --- |
| Floor | $1,000,000 USDC |
| Planning target | $3,000,000 USDC |
| Source | External pre-seed round (off-chain, raised by founding team / treasury) |
| Denomination | USDC throughout — no TOKEN substitution at any disbursement stage |
| Custody | Multisig USDC wallet, DAO-controlled; same Timelock-custodied wallet pattern as the [ADR 026](026-tokenomics-v3.md) §2 treasury share, with a dedicated sub-account so program flows are auditable separately from organic inflow |
| Spending authority | Standard governance proposal per [ADR 009](009-governance.md), or fast-track emergency-multisig authorization within the hard caps below |

**No protocol issuance.** This pool is **externally raised USDC**. The protocol does not
mint TOKEN to fund it; ADR 004's 200M-TOKEN bootstrap supply does not exist under
[ADR 026](026-tokenomics-v3.md) §1. **Raising at least the $1M floor is a prerequisite
for v3 mainnet launch.** Contingency: governance can re-allocate from the 30% Protocol
Treasury bucket per [ADR 026](026-tokenomics-v3.md) §1 at the cost of development
runway.

**Hard caps (immutable at deploy time)** for the multisig fast-track path, paralleling
the `SafetyReserve` fast-track gates per [ADR 009](009-governance.md):

| Path | Per-incident cap | Per-30-day rolling cap |
| --- | --- | --- |
| Multisig fast-track | $50,000 | $250,000 |
| Standard governance proposal | program-allocation-bounded | program-allocation-bounded |

Disbursements above either cap require a full governance proposal.

### 2. Five funded programs

The pool funds five distinct, non-overlapping programs:

| # | Program | Default share | Range ($1M floor / $3M target) |
| --- | --- | ---: | ---: |
| 2a | Protocol-Owned Operators (POOs) | **30%** | $300K – $900K |
| 2b | Hardware-leasing subsidies | **25%** | $250K – $750K |
| 2c | Staking loans | **15%** | $150K – $450K |
| 2d | Regional-deploy grants | **20%** | $200K – $600K |
| 2e | Enterprise SLA guarantee fund | **10%** | $100K – $300K |
| | **Total** | **100%** | $1M – $3M |

#### 2a. Protocol-Owned Operators (POOs) — 30%

**Charter.** DAO operates nodes directly in priority regions. Operator stake (50K
TOKEN per node, [ADR 026](026-tokenomics-v3.md) §7) sources from the protocol-treasury
TOKEN bucket; hardware, bandwidth, ops, and stake-equivalent USDC reserves come from
this pool. POO revenue (40% direct USDC + share of the 40% gauge pool,
[ADR 026](026-tokenomics-v3.md) §2) flows back to the treasury — the self-sustaining
flywheel from preseed-capital strategy §1.

**Eligibility (region scope; operator is the DAO).** Both: aggregate observed client
demand > 100 GB/sec (sustained 7-day average); independent operator coverage < 3
distinct operators with `final_score ≥ 0.7` per [ADR 008](008-reputation.md).

Initial priority regions per preseed-capital strategy §1 and market-dynamics §5:
Brazil, Southeast Asia (ID/SG/VN), India, West Africa, Eastern Europe. Specific
country selection is the first DAO vote authorizing POO disbursement.

**Allocation.** 30%. At $1M, ~6–10 POO nodes / 12 months on tier B/D infrastructure
(economic-model spec §3); at $3M, ~20–30 nodes. Tier A (1G VPS) excluded — too small
to materially seed a region.

**Success metrics.**

- POOs generate ≥ $X/mo in 40% direct-USDC settlement within 6 months of activation
  (X sized at deploy time against chosen tier).
- Cumulative POO net revenue back to treasury exceeds 50% of capital deployed to
  this program within 24 months.
- Median client-observed regional latency drops ≥ 30% within 6 months.

**Termination trigger.** A regional POO winds down when independent operator coverage
exceeds **3 distinct operators per 100 GB/sec demand at `final_score ≥ 0.7` for 90
consecutive days**. Wind-down returns stake and retained USDC to treasury. Program
continues until the trigger fires in every activated region.

#### 2b. Hardware-leasing subsidies — 25%

**Charter.** USDC-denominated hardware lease (or lease-to-own) subsidies for verified
high-rep operators in regions with high bandwidth fixed costs (preseed-capital strategy
§4). Insulates the operator-margin sensitivity flagged in
[ADR 026](026-tokenomics-v3.md) §Risks: USDC subsidy is TOKEN-price-independent.

**Eligibility.** All of: `final_score ≥ 0.7` per [ADR 008](008-reputation.md) for
≥ 90 days on a prior node (or referral from a `final_score ≥ 0.8` operator with
co-signed performance guarantee — same pattern as §2c); operates in a region where
the required tier's dollar cost exceeds the equivalent in lower-cost regions by
≥ 1.5× per economic-model spec §3; commits to a 12-month minimum term with claw-back
if recipient deregisters or is auto-ejected ([ADR 026](026-tokenomics-v3.md) §8)
before term.

**Allocation.** 25%. Per-recipient cap: $25K (~12 months tier B 10G dedicated at
high regional cost). Lease-to-own conversion at month 18; lease-only mode also
supported.

**Success metrics.**

- ≥ 99% uptime and `final_score ≥ 0.7` for 90% of lease term.
- Delivered byte volume ≥ regional median for chosen tier.
- ≥ 60% lease-to-own conversion at month 18.

**Termination trigger.** Per-region: program stops new leases when **both** the
median per-byte settlement rate in the region falls within ±10% of the network median
(capacity no longer binding), and the 90th-percentile regional operator clears Case B
margins ([ADR 026](026-tokenomics-v3.md) §7) without subsidy for a rolling 90 days.
In-flight leases continue regardless of regional termination.

#### 2c. Staking loans — 15%

**Charter.** The pool lends the **50,000 TOKEN minimum stake**
([ADR 026](026-tokenomics-v3.md) §7) to verified high-rep operators in underserved
regions, denominated in USDC at lend time (default 30-day TOKEN/USDC TWAP). Loans are
**collateralized by future earnings**: the recipient's 40% direct-USDC stream and
gauge-pool payouts ([ADR 026](026-tokenomics-v3.md) §2) route to the program recovery
account until the loan is repaid. Targets geographic-need recruitment over wealth-based
recruitment.

**Eligibility.** All of:

- `final_score ≥ 0.7` per [ADR 008](008-reputation.md) for ≥ 90 days on a prior
  node, **or** a `final_score ≥ 0.8` referrer (≥ 180 days) co-signs and is jointly
  liable for default recovery up to 25% of principal.
- Operates in an explicitly-designated underserved region (governance-maintained;
  same list pattern as §2a).
- No prior pre-seed-loan default. A single prior default is permanent
  disqualification.

**Allocation.** 15% of pool. At $0.05 TOKEN, 50K TOKEN ≈ $2,500 USDC equivalent;
$150K supports ~60 loans, $450K ~180. Per-region principal cap: **20% of program
allocation** (caps geographic concentration).

**Repayment.**

- 100% of recipient's 40% direct-USDC settlement and gauge-pool payouts route to
  the recovery account until repaid. Recipient retains delegator-pool TOKEN
  ([ADR 026](026-tokenomics-v3.md) §6) and any voluntary ve-lock yields.
- Repayment in USDC at TOKEN/USDC TWAP at repayment time (TOKEN-equivalent
  alternative permitted at the same rate).
- Default = recipient auto-ejected per [ADR 026](026-tokenomics-v3.md) §8 or
  voluntarily deregisters before repayment. On default, the recovery account
  claims any remaining staked TOKEN plus the referrer's 25% co-signed liability.

**Success metrics.**

- ≥ 80% repaid in full within 24 months of disbursement.
- Lifetime default rate < 15%. Defaults > 20% in any rolling 90-day window pause
  new disbursements pending governance review.
- No region holds > 30% of outstanding principal.

**Termination trigger.** Stops new applications when **either**:

- TOKEN secondary-market depth supports retail purchase of the 50K stake at
  ≤ 5% slippage on a single lot for a rolling 90 days (capital-access friction
  is gone), **or**
- Cumulative defaults exceed 25% of disbursed principal (program risk exceeds
  tolerance).

In-flight loans continue regardless of program-level termination.

#### 2d. Regional-deploy grants — 20%

**Charter.** Community-voted regional gauges + DAO-directed deployment grants for
high-demand / high-cost regions (Brazil, Southeast Asia, West Africa). ve-lockers
signal where capacity is most valuable via the gauge mechanism (market-dynamics §5);
the pool funds qualified grant applications against those signals. **Distinct from
POOs**: §2a operates DAO-owned nodes; §2d subsidizes independent operators.

**Eligibility.** All of:

- Commits to a minimum-uptime SLA (default: 99% over rolling 30-day window).
- `final_score ≥ 0.7` per [ADR 008](008-reputation.md) for ≥ 90 days (referrer
  pattern from §2c also applies).
- Targets a governance-designated regional-grant priority. Auto-eligibility: a
  gauge accumulating ≥ 5% of weekly veTOKEN vote weight for 4 consecutive weeks
  joins the priority list on the next quarterly cycle (governance-overrideable).

A single operator may receive grants in multiple regions, but not multiple in the
same region within 12 months.

**Allocation.** 20% of pool. Per-grant cap: $15K (~6 months tier C 10G dedicated at
high regional cost). Per-operator lifetime cap: $50K. Milestone disbursement: 50%
on activation, 50% on month-6 SLA satisfaction.

**Success metrics.**

- Median client-observed regional latency and 95p availability reach
  governance-set thresholds within 9 months (defaults: 75 ms median, 99.5% 95p;
  per-region overrideable where underlying internet infra cannot support the
  default).
- ≥ 70% grantee retention (active operator) at end of SLA term.
- Auto-eligible regional gauges sustain ≥ 3% weekly vote weight for ≥ 6 of the
  12 months following activation.

**Termination trigger.** Continuous program; priorities rotate as demand shifts.
Regional removal from the priority list when **both**:

- Auto-eligibility condition no longer met, and
- Median regional latency holds below the threshold for 90 consecutive days.

Program funding ends only at the §6 global wind-down.

#### 2e. Enterprise SLA guarantee fund — 10%

**Charter.** Pre-seed capital reserved as **paired backing for the `SafetyReserve`**
([ADR 026](026-tokenomics-v3.md) §5). The `SafetyReserve` accumulates organically from
the 3% router share; in the early period when organic accumulation is small ($28K/mo
at S0 per economic-model spec §2), pre-seed capital provides depth for contracts whose
worst-case payout exceeds the early `SafetyReserve` balance. Implements the "Guarantee
Fund" idea (preseed-capital §3) and Enterprise SLA tier framing (market-dynamics §2).

**Eligibility (Enterprise contract scope).** A contract draws on the paired allocation
when **both**:

- The contract's maximum SLA-failure compensation exceeds the `SafetyReserve`
  balance at signing, and
- The Enterprise client signed the standard SLA template specifying
  `SafetyReserve` + pre-seed pairing as the recourse mechanism.

Eligible payout categories follow the canonical `SafetyReserve` list per
[ADR 026](026-tokenomics-v3.md) §5; this program does not introduce new payout
categories. ADR 032 (Bandwidth Futures / Enterprise SLA tier, deferred to v2) will
define the contract template; until then, individual Enterprise contracts are
case-by-case under DAO governance.

**Allocation.** 10% of pool — $100K (floor) covers a single $100K incident at full
scale per economic-model spec §7; $300K (target) covers a single $1M incident at 30%
or three $100K incidents in series.

**Coordination with `SafetyReserve` (avoid double-funding).** A single incident draws
on **at most one** funding source, in this priority order, enforced by the
`SafetyReserve.payout` resolution logic (added to [ADR 016](016-contract-interactions.md)
on acceptance):

1. Organic `SafetyReserve` balance, until exhausted.
2. Slashing-replenished `SafetyReserve` balance (same wallet, same gates;
   [ADR 026](026-tokenomics-v3.md) §8 routes 30% of slashed stake here).
3. Pre-seed paired allocation, only when 1+2 are insufficient for the authorized
   payout.

The `SafetyReserve` evidence-bundle, multisig-fast-track-or-governance, 48-hour
appeal, and post-incident-reporting gates per [ADR 009](009-governance.md) and
[ADR 026](026-tokenomics-v3.md) §5 apply unchanged to the paired allocation. No
separate authorization path.

**Success metrics.**

- Enterprise YoY retention ≥ 80%, conditioned on SLA template in effect.
- Net new Enterprise contracts via existing-client referral ≥ 25%/year.
- Cumulative paired-allocation payouts stay within program allocation (no
  cross-program raids).

**Termination trigger.** The pairing ends when organic `SafetyReserve` inflow
sustains a balance ≥ maximum aggregate Enterprise SLA exposure for **6 consecutive
months**. Remaining allocation returns to treasury (or, by governance vote, may
redirect to other pre-seed programs whose triggers have not fired).

### 3. Allocation flexibility

The 30/25/15/20/10 default is a starting point. Governance may rebalance within:

| Constraint | Value |
| --- | --- |
| Maximum per-program shift per quarter | ±10 percentage points |
| Maximum cumulative deviation from default | ±20 percentage points per program |
| Sum-to-100% across the five programs | Enforced; non-conforming proposals revert |
| Timelock | Per [ADR 009](009-governance.md): 48-hour timelock, 7-day voting period |

Rebalancing **cannot** introduce a new program (this ADR is the canonical list; new
programs require an ADR amendment). Rebalancing **cannot** reduce a non-terminated
program below 0%.

When a program's termination trigger fires, its remaining allocation auto-redistributes
to the four remaining programs in proportion to their then-current allocations.
Governance may override within the same bounds.

### 4. Reporting

The DAO publishes a **quarterly pre-seed program report** to a public registry — same
pattern as the `SafetyReserve` post-incident registry per [ADR 026](026-tokenomics-v3.md)
§5. Each report contains:

- Capital deployed per program (cumulative + Q-over-Q delta) and remaining.
- Per-program success-metric values vs. targets.
- Termination triggers met (or rationale for not).
- Governance rebalancing actions in the quarter.
- Cross-program flows from termination-trigger redistributions.

Reports are signed by the multisig and published on-chain via a quarterly-report
event emitted by the program-accounting contract. Off-chain mirrors are documentation
hygiene, not protocol invariants.

### 5. Coordination with `SafetyReserve` (summary)

§2e defines the rules. One-line summary: **a single incident draws on at most one
funding source** (organic `SafetyReserve`, slashing-replenished `SafetyReserve`, or
pre-seed paired allocation) in that priority order. No double-funding. The
`SafetyReserve` payout flow's gates apply unchanged to the paired allocation.

### 6. Termination of the pre-seed program as a whole

The program winds down — stops accepting new disbursements across all five
sub-programs, completes in-flight commitments to term, returns remaining capital to
treasury — when **all** of the following hold for **6 consecutive months**:

- Treasury USDC inflow per [ADR 026](026-tokenomics-v3.md) §2 (5% of routed USDC)
  exceeds combined run-rate operating expenditure plus a 50% safety margin. Per
  economic-model spec §2, this is S2 Growth scale (~$2M/mo treasury inflow,
  $1.97M/mo surplus net of $33K/mo team burn).
- Each program's per-program termination trigger has fired (or has been
  governance-waived as no longer relevant).
- The `SafetyReserve` organic balance sustains the §2e Enterprise pairing trigger.

At wind-down, remaining capital across all five programs returns to the DAO treasury.
Future tokenomics decisions may redeploy treasury funds to a successor program by
ADR amendment, but the **pre-seed program is closed** at that point. In-flight
commitments (active loans, leases, regional grants) continue to term regardless.

---

## Consequences

### Positive

- **Eliminates TOKEN-price reflexivity in bootstrap.** USDC throughout removes the
  largest tail risk in [ADR 026](026-tokenomics-v3.md) §Context.
- **Five distinct levers, non-overlapping.** Each program targets a different
  recruitment friction (capital access, geographic coverage, enterprise
  credibility); §3 lets the DAO tune the mix as different frictions dominate at
  different scales.
- **POO flywheel is self-sustaining.** §2a's metric — cumulative POO net revenue
  back to treasury exceeding 50% of capital deployed within 24 months — makes the
  program net-positive in dollar terms before network-wide externalities.
- **Tight `SafetyReserve` coordination.** §2e and §5's single-source rule prevent
  double-funding by construction.
- **Hard caps preserve safety without slowing legitimate spend.** §1 fast-track
  caps ($50K incident, $250K rolling) bound worst-case multisig exposure;
  standard governance is unbounded per-incident.
- **Wind-down is principled.** §6 ties termination to a measurable treasury-inflow
  regime; capital returns to treasury when organic flows sustain continuing
  programs.

### Negative

- **Governance overhead.** Five programs, quarterly reports, regional priority
  lists, per-program eligibility verification — substantial recurring DAO
  workload. Delegating verification to a multisig-supervised program-operator
  team (`SafetyReserve` pattern) helps, but the footprint is real.
- **POO political optics.** A DAO operating competitor nodes in regions where it
  also subsidizes independent operators is a governance-credibility risk. The
  §2a termination trigger forces wind-down where independent coverage matures —
  POOs are a seeding tool, not a permanent fixture.
- **Staking-loan repayment risk.** Defaults bounded by the 25% pause-trigger and
  15% target rate, but a regional shock could cluster defaults. The 30%-per-region
  principal cap and referrer co-signed liability provide two loss layers.
- **Hardware-lease claw-back has limits.** A subsidized operator deregistering
  mid-term forfeits ownership conversion, but recovering in-progress monthly
  subsidy depends on operator cooperation. Realistic expected loss on a
  defaulted lease: 30–50% of subsidy paid to date.
- **Pre-seed must actually be raised.** $1M floor is a hard prerequisite for v3
  mainnet. Unlike ADR 004's TOKEN bootstrap, the protocol cannot mint this fund
  into existence. **On the critical path for v3 launch.**
- **USDC concentration risk.** $1M–$3M in a single stablecoin. A depeg or
  regulatory action impairs funding directly. Mitigation is at the DAO-treasury
  level (USDC / USDT / DAI diversification), not in this ADR.

### Risks

- **Sybil exploitation of staking loans.** Multiple loans via fake referrer
  relationships. Mitigation: the `final_score ≥ 0.8` for ≥ 180-day referrer
  threshold ([ADR 008](008-reputation.md)) is hard to fake; co-signed 25%
  liability creates a self-policing incentive.
- **Regional-grant capture.** A small clique rotating gauge votes to keep a
  region perpetually on the priority list. Mitigation: auto-eligibility is
  governance-overrideable; sustained patterns trigger governance review.
- **Program complexity invites mismanagement.** Five programs with separate
  state is a lot to track. §3's bounds and §4's quarterly reporting force
  regular per-program review; a program nobody reports on is itself a
  governance-failure signal.
- **POO governance-weight concentration.** POOs hold operator stake and may
  ve-lock for gauge boost. POO ve-positions are governance-policy bound to
  abstain on regional-priority votes affecting their own regions; enforcement
  is policy, not on-chain.
- **Multisig compromise.** Signer collusion or compromise is a $1M–$3M loss.
  Standard multisig hygiene per [ADR 009](009-governance.md); not a new
  mitigation.
- **Slow ramp on the wind-down trigger.** If treasury inflow plateaus at S1
  rather than reaching S2, the program runs longer than designed. Year-3
  governance review should re-evaluate structure if S2 has not been reached.

---

## ADRs to update on acceptance

| ADR | What changes |
| --- | --- |
| [ADR 008 — Reputation](008-reputation.md) | Document the `final_score ≥ 0.7` (90-day) and `final_score ≥ 0.8` (180-day) eligibility thresholds used by §2a–§2d. The reputation system is unchanged; this records that pre-seed program eligibility is a non-protocol consumer of `final_score`. |
| [ADR 019 — Node Onboarding](019-node-onboarding.md) | Replace the existing "Pre-seed USDC Bootstrap Programs" section's forward-reference with a back-reference to this ADR. Document that Phase 2's stake source for staking-loan recipients is the pre-seed recovery account, and note the hardware-leasing path's relationship to Phase 1 step 1 (server provisioning). No phase-sequence change — pre-seed paths deliver capital, not protocol-flow shortcuts. |
| [ADR 026 — Tokenomics v3](026-tokenomics-v3.md) | §10's forward-reference resolves; bootstrap-program structure now lives here. §5's `SafetyReserve` payout flow gains the §2e pre-seed paired allocation as a third-priority funding source per the §5 coordination rule. |

Additional ancillary updates: [ADR 009](009-governance.md) (the §1 hard caps on
multisig fast-track parallel the existing `SafetyReserve` pattern; ADR 009's
emergency-multisig section should reference this ADR's caps as a second instance
of the pattern), [ADR 016](016-contract-interactions.md) (program-accounting
contract for §4's public registry — single-purpose, emits the quarterly-report
event; minimal interface).
