# ADR 030: Pre-Seed USDC Deployment Program

**Date:** 2026-04-25
**Status:** Draft
**Funds:** [ADR 026](026-gauge-boost-tokenomics.md) §10 (bootstrap mechanism)

---

## Context

Bootstrap supply-side incentive — capital that recruits operators in the gap between mainnet launch and treasury self-funding — is **$1M+ externally-raised USDC** (planning target $3M), per [ADR 026 §10](026-gauge-boost-tokenomics.md#10-bootstrap-mechanism-pre-seed-usdc). USDC denomination insulates subsidy purchasing power from TOKEN price; single-asset reflexivity in subsidy capacity was a tail risk that this denomination removes. ADR 026 commits the mechanism (USDC, externally raised) and the floor; this ADR is the program charter — capital structure, the five funded programs with per-program eligibility / allocation / success metrics / termination triggers, reporting cadence, `SafetyReserve` coordination, and the program-wide wind-down trigger.

A TOKEN-denominated bootstrap variant was considered and rejected; see [Alternatives Considered](#alternatives-considered).

The pre-seed program runs **in parallel** with the canonical self-funded onboarding
flow in [ADR 019](019-node-onboarding.md). It does not replace any phase of onboarding;
it supplies stake, hardware, regional capital, or SLA backing to operators who would
otherwise be filtered out at Phase 1 (server provisioning) or Phase 2 (on-chain stake).

**Sizing context.** S0 (Bootstrap) treasury USDC inflow is small and not self-funding; S1 (Early) reaches self-funding; S2 (Growth) generates surplus. Pre-seed is sized for the S0→S1 transition where operator-side reflexivity bites hardest and treasury inflow has not yet caught up. Absolute scale figures (node counts, monthly inflows by scenario) live in `finance/notebooks/` and the economic-model spec §§0–3.

---

## Decision

### 1. Capital structure

| Item | Value |
| --- | --- |
| Floor | $1,000,000 USDC |
| Planning target | $3,000,000 USDC |
| Source | External pre-seed round (off-chain, raised by founding team / treasury) |
| Denomination | USDC throughout — no TOKEN substitution at any disbursement stage |
| Custody | Dedicated Timelock-custodied USDC account, DAO-controlled; same Timelock-custodied wallet pattern as the [ADR 026](026-gauge-boost-tokenomics.md) §2 treasury share, with a dedicated sub-account so program flows are auditable separately from organic inflow |
| Spending authority | Standard governance proposal per [ADR 009](009-governance.md); the [ADR 009](009-governance.md) emergency multisig has **no withdrawal authority** over this pool |

**No protocol issuance.** This pool is **externally raised USDC**. The protocol does not
mint TOKEN to fund it. **Raising at least the $1M floor is a prerequisite for mainnet
launch.** Contingency: governance can re-allocate from the 30% Protocol Treasury bucket
per [ADR 026](026-gauge-boost-tokenomics.md) §1 at the cost of development runway.

**Authority clarification.** This program does **not** introduce a separate emergency
withdrawal path. Unlike `SafetyReserve` payouts in [ADR 009](009-governance.md), this
pool is not spendable via emergency-multisig fast-track authorization; all disbursements
remain subject to standard governance approvals and the per-program allocation limits in
this ADR. Disbursements are bounded only by the per-program allocation ranges in §2 and
the rebalancing rules in §3.

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
TOKEN per node, [ADR 026](026-gauge-boost-tokenomics.md) §7) sources from the protocol-treasury
TOKEN bucket; hardware, bandwidth, ops, and stake-equivalent USDC reserves come from
this pool. POO revenue (40% direct USDC + share of the 40% gauge pool,
[ADR 026](026-gauge-boost-tokenomics.md) §2) flows back to the treasury — the self-sustaining
flywheel from preseed-capital strategy §1.

**Eligibility (region scope; operator is the DAO).** Both: aggregate observed client
demand > 100 GB/sec (sustained 7-day average); independent operator coverage < 3
distinct operators with `final_score ≥ 0.7` per [ADR 008](008-reputation.md).

Initial priority regions per preseed-capital strategy §1 and market-dynamics §5:
Brazil, Southeast Asia (ID/SG/VN), India, West Africa, Eastern Europe. Specific
country selection is the first DAO vote authorizing POO disbursement.

**Allocation.** 30%. Sizing per economic-model spec §3 — supports tens of POO nodes / 12 months on tier B/D infrastructure across the floor-to-target range. Tier A (1G VPS) excluded as too small to materially seed a region.

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
[ADR 026](026-gauge-boost-tokenomics.md) §Risks: USDC subsidy is TOKEN-price-independent.

**Eligibility.** All of: `final_score ≥ 0.7` per [ADR 008](008-reputation.md) for
≥ 90 days on a prior node (or referral from a `final_score ≥ 0.8` operator with
co-signed performance guarantee — same pattern as §2c); operates in a region where
the required tier's dollar cost exceeds the equivalent in lower-cost regions by
≥ 1.5× per economic-model spec §3; commits to a 12-month minimum term with claw-back
if recipient deregisters or is auto-ejected ([ADR 026](026-gauge-boost-tokenomics.md) §8)
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
margins ([ADR 026](026-gauge-boost-tokenomics.md) §7) without subsidy for a rolling 90 days.
In-flight leases continue regardless of regional termination.

#### 2c. Staking loans — 15%

**Charter.** The pool lends the **50,000 TOKEN minimum stake**
([ADR 026](026-gauge-boost-tokenomics.md) §7) to verified high-rep operators in underserved
regions, denominated in USDC at lend time (default 30-day TOKEN/USDC TWAP). Loans are
**collateralized by future earnings**: the recipient's 40% direct-USDC stream and
gauge-pool payouts ([ADR 026](026-gauge-boost-tokenomics.md) §2) route to the program recovery
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

**Allocation.** 15% of pool. Loan principal scales with TOKEN/USDC at lend time; the floor-to-target range supports tens to low-hundreds of loans (sizing arithmetic in `finance/notebooks/`). Per-region principal cap: **20% of program allocation** (geographic-concentration cap).

**Repayment.**

- 100% of recipient's 40% direct-USDC settlement and gauge-pool payouts route to
  the recovery account until repaid. Recipient retains delegator-pool TOKEN
  ([ADR 026](026-gauge-boost-tokenomics.md) §6) and any voluntary ve-lock yields.
- Repayment in USDC at TOKEN/USDC TWAP at repayment time (TOKEN-equivalent
  alternative permitted at the same rate).
- Default = recipient auto-ejected per [ADR 026](026-gauge-boost-tokenomics.md) §8 or
  voluntarily deregisters before repayment. On default, the recovery account
  claims any remaining staked TOKEN plus the referrer's 25% co-signed liability.

**Implementation surface — payout redirection.** The "100% routes to recovery account" rule in the repayment bullet above requires a redirection mechanism, since `FeeRouter.routeSettlement` ([ADR 016](016-contract-interactions.md)) otherwise sends the 40% base share directly to the operator address. The mechanism:

- `StakingRegistry.setPayoutDestination(address operator, address recoveryAccount, uint256 expiresAt)` — gated by a new `LOAN_GRANTOR_ROLE` held by this program's contract; sets a per-operator redirect destination with an explicit expiry timestamp. Set on loan disbursement.
- `StakingRegistry.clearPayoutDestination(address operator)` — same role; called on loan repayment, and also (no-op) callable by anyone after `expiresAt` to clean up stale entries.
- `StakingRegistry.payoutDestinationOf(address operator) view returns (address dest, uint256 expiresAt)` — public view.
- `StakingRegistry.payoutDestinationAt(address operator, uint64 epochId) view returns (address dest)` — historical view, returns the destination that was active at the boundary timestamp of `epochId`. Implementation: per-operator destination history is appended on each `setPayoutDestination` / `clearPayoutDestination` call, with `(epochId, dest)` records; reads do an O(log n) binary search over the history.

**Routing rules — pinned to commit time, not claim time:**

- `FeeRouter.routeSettlement(operator, ...)`: reads `payoutDestinationOf(operator)` and routes the 40% base share to `dest` if `dest != address(0) && block.timestamp < expiresAt`, else to operator. Pin point is the settlement transaction (which is contemporaneous with byte delivery).
- `FeeRouter.claimBoost(operator, epochs[])`: for each epoch in the call, reads `payoutDestinationAt(operator, epochId)` — i.e., the destination that was active **at the epoch boundary** of the epoch being claimed. This prevents the bypass where an operator waits until the loan is repaid and the destination is cleared before claiming gauge-boost rewards for epochs that occurred during the loan period. Funds for an epoch are routed to whichever destination was effective when the bytes were earned, not when the claim is filed.
- `FeeRouter.claimDelegator(epochs[])`: claims by ve-locker, not by operator. The redirect does **not** apply — delegator-pool yield was never the borrower's revenue stream and was never pledged to the loan recovery.

This is an additive interface — it does not change `FeeRouter`'s six-bucket split, settlement timing, or any other invariant. Operators who never take a staking loan see no behaviour change. The expiry bound prevents the LOAN_GRANTOR from indefinitely siphoning revenue past the loan's intended duration; loan recipients can audit the on-chain destination history + expiry at any time.

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
([ADR 026](026-gauge-boost-tokenomics.md) §5). The `SafetyReserve` accumulates organically from
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
[ADR 026](026-gauge-boost-tokenomics.md) §5; this program does not introduce new payout
categories. ADR 032 (Bandwidth Futures / Enterprise SLA tier, post-launch follow-up) will
define the contract template; until then, individual Enterprise contracts are
case-by-case under DAO governance.

**Allocation.** 10% of pool. Coverage capacity (single high-severity incident vs. multiple lower-severity in series) per economic-model spec §7.

**Coordination with `SafetyReserve` (avoid double-funding).** A single incident draws
on **exactly one of two funding sources** for resolution, enforced by the
`SafetyReserve.payout` resolution logic (added to [ADR 016](016-contract-interactions.md)
on acceptance):

1. `SafetyReserve`, treated as a **single source** from the contract's perspective.
   Its available balance may include both:
   - organic `SafetyReserve` balance; and
   - slashing-replenished `SafetyReserve` balance (same wallet, same gates;
     [ADR 026](026-gauge-boost-tokenomics.md) §8 routes 30% of slashed stake here).
2. Pre-seed paired allocation, only when the authorized payout exceeds the total
   `SafetyReserve` balance available at decision time.

The `SafetyReserve` evidence-bundle, multisig-fast-track-or-governance, 48-hour
appeal, and post-incident-reporting gates per [ADR 009](009-governance.md) and
[ADR 026](026-gauge-boost-tokenomics.md) §5 apply unchanged to the paired allocation. No
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

Governance rebalancing is bounded: **±10 pp per program per quarter, ±20 pp cumulative**, sum-to-100% enforced, [ADR 009](009-governance.md) timelock applied. New programs require an ADR amendment; terminated programs' allocations auto-redistribute pro-rata to the surviving programs (governance-overrideable within the same bounds).

### 4. Reporting

Quarterly pre-seed program report (capital deployed, success-metric deltas, triggers met, rebalancing actions, cross-program flows) authorized via standard governance proposal and published on-chain via the program-accounting contract — same pattern as the `SafetyReserve` post-incident registry ([ADR 026](026-gauge-boost-tokenomics.md) §5). Off-chain mirrors are documentation hygiene, not protocol invariants.

### 5. Coordination with `SafetyReserve` (summary)

§2e is the rule. **A single incident draws on exactly one of two funding sources** — `SafetyReserve` (single balance, organic + slashing-replenished commingled in the same wallet), or pre-seed paired allocation when the authorized payout exceeds the total `SafetyReserve` balance at decision time. No double-funding. The `SafetyReserve` evidence-bundle / multisig-fast-track / appeal / reporting gates apply unchanged to the paired allocation.

### 6. Termination of the pre-seed program as a whole

The program winds down — stops accepting new disbursements across all five
sub-programs, completes in-flight commitments to term, returns remaining capital to
treasury — when **all** of the following hold for **6 consecutive months**:

- Treasury USDC inflow per [ADR 026](026-gauge-boost-tokenomics.md) §2 (5% of routed USDC)
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

- **Eliminates TOKEN-price reflexivity in bootstrap.** USDC throughout removes the largest tail risk from [ADR 026](026-gauge-boost-tokenomics.md) §Context.
- **Five non-overlapping recruitment levers**, governance-tunable at the §3 bounds as different frictions dominate.
- **POO flywheel is self-sustaining** by design — net revenue back to treasury exceeds capital deployed within 24 months (§2a metric).
- **`SafetyReserve` two-source rule** (`SafetyReserve` as a single commingled balance, plus pre-seed paired allocation only when payout exceeds it) prevents double-funding by construction (§2e, §5).
- **Wind-down is principled** — §6 ties termination to a measurable treasury-inflow regime.

### Negative

- **Recurring DAO workload.** Five programs, quarterly reports, eligibility verification — delegable to a multisig-supervised team but real.
- **POO political optics.** DAO operating competitor nodes is a credibility risk; §2a's wind-down trigger forces exit where independent coverage matures.
- **Staking-loan default risk.** Bounded by the 25% pause-trigger and 15% target; the 30%-per-region cap + 25% referrer co-sign give two loss layers.
- **Hardware-lease claw-back is partial.** Realistic loss on a mid-term default: 30–50% of subsidy paid.
- **Pre-seed must actually be raised.** The $1M floor is a hard prerequisite for mainnet — on the critical path.
- **USDC concentration risk.** Mitigated at the DAO-treasury level (stablecoin diversification), not in this ADR.

### Risks

- **Sybil exploitation of staking loans** via fake referrer relationships. The `final_score ≥ 0.8 / 180-day` referrer threshold and 25% co-signed liability make it self-policing.
- **Regional-grant capture** by a clique rotating gauge votes. Mitigation: auto-eligibility is governance-overrideable; sustained patterns trigger review.
- **POO governance-weight concentration.** POO ve-positions are policy-bound to abstain on regional-priority votes affecting their own regions; enforcement is policy, not on-chain.
- **Timelock / governance-key compromise** is a $1M–$3M loss. Standard [ADR 009](009-governance.md) Timelock + governance-multisig hygiene applies.
- **Slow ramp on the §6 wind-down trigger** if treasury inflow plateaus at S1 — year-3 governance review re-evaluates structure if S2 has not been reached.

---

## Alternatives Considered

### TOKEN-denominated node-bootstrap fund

A protocol-issued multi-hundred-million-TOKEN bootstrap fund (the original [ADR 004](004-tokenomics.md) shape: 200M TOKEN allocated for operator subsidies) was considered.

Rejected because:

- **Reflexive purchasing power.** Subsidies denominated in TOKEN are most valuable when TOKEN price is healthy and least valuable when subsidies are most needed. Single-asset reflexivity was the dominant tail risk for early operator recruitment.
- **Concentrated dilution.** A multi-hundred-million-TOKEN allocation is non-trivial dilution that ties to bootstrap duration rather than network outcomes.
- **TOKEN-price-independent program structure.** USDC-denominated programs (POOs, hardware-leasing, staking loans, regional grants, Enterprise SLA fund) can be sized against dollar-denominated regional infrastructure costs; the same programs in TOKEN need a separate mental model that depends on TOKEN price at every disbursement.

The chosen design routes externally-raised USDC into outcome-targeted programs and reserves the protocol-treasury TOKEN bucket ([ADR 026 §1](026-gauge-boost-tokenomics.md#1-supply-and-distribution)) for governance-driven uses.

---

## ADRs to update on acceptance

Cross-cutting deltas live in each touched ADR. Touched: [008](008-reputation.md) (program-eligibility consumer of `final_score`), [016](016-contract-interactions.md) (program-accounting contract for §4 reporting), [019](019-node-onboarding.md) (back-reference to this ADR), [026](026-gauge-boost-tokenomics.md) (§10 resolved; §5 gains §2e pairing as second-priority funding source).
