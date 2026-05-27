# Work-Token Tokenomics — No-Emission Variant (v2.2)

**Status:** Draft 2026-05-27 (proposes superseding v2.1 as canonical work-token tokenomics).
**Supersedes (on acceptance):** v2.1 spec (`2026-05-24-work-token-tokenomics-redesign-v2.1.md`); requires substantial rewrite of ADR 026, targeted edit to ADR 016, minor edit to ADR 028. No changes to ADRs 003, 009, 018, 033.
**Relationship to v2.1:** Identical work-token core (CapacityBond, 4-bucket FeeRouter, capacity-weighted governance, POL/BuybackBurner, SafetyReserve). Three changes: (i) the 20% Operator Service Emissions bucket is eliminated entirely; (ii) Airdrop and Incentivized Testnet buckets are eliminated as standalone groups; (iii) the 11-group distribution table replaces v2.1's 13-group table. Operator bootstrap is preserved via a bounded one-time **Genesis Bond Credits** program carved from the DAO Treasury bucket; demand-side adoption is supported via a new **App Incentives** bucket.

## Summary

Remove every ongoing TOKEN-denominated payment to bonded operators. The v2.1 Operator Service Emissions bucket (20% / 200M TOKEN, distributed over ~6 years per verified delivery) is eliminated. Operators are paid in USDC via the existing FeeRouter flow only; TOKEN value accrues to bonded operators solely through the BuybackBurner deflationary loop. No emission contract, no service-conditional TOKEN distribution at any time-step.

Year-1 operator bootstrap is preserved via a bounded, retroactive, one-time grant: **Genesis Bond Credits** (50M TOKEN / 5% of supply), carved at TGE from the DAO Treasury, granted only to incentivized-testnet operators weighted by measured testnet contribution, auto-bonded into CapacityBond, vesting 24 months conditional on continued operation. This is RSU-shaped retention vesting on a retroactive merit grant — not ongoing service emission.

Demand-side adoption gains a new bucket: **App Incentives** (14% / 140M TOKEN). Two sub-programs run by Treasury multisig: publisher rebates (TOKEN-denominated rebate against USDC paid into FeeRouter) and integration grants (milestone-based payouts to projects integrating deCDN). Both are customer-acquisition incentives to consumers of the service — different Howey prong from emissions to bonders.

Contract surface is net-subtractive vs v2.1: `OperatorEmissions` is deleted, `CapacityBond` gains a small vesting extension, no new contracts are added.

## Motivation

v2.1's §5.1 reinterpreted "Staking Rewards" as **Operator Service Emissions** — payment-for-work rather than passive yield — citing Filecoin / Helium / Livepeer precedent. The reinterpretation is defensible but not zero-risk: a recipient who is also a TOKEN bonder receiving ongoing TOKEN distribution from the protocol over six years presents a passive-yield-shaped surface, however the bucket is labeled in fundraising materials.

This redesign eliminates that surface entirely. The work-token Howey posture under v2.2 is:

- Operators bond TOKEN to deliver bytes.
- Operators are paid in USDC, per byte, via FeeRouter.
- TOKEN value accrues to bonded operators only through buyback-and-burn deflation (Section 3 of ADR 003 / ADR 018), driven by USDC fee flow.
- No TOKEN-denominated yield stream exists to any bonder at any time-step.
- The single non-trivial exception — Genesis Bond Credits — is a one-shot retroactive grant at TGE on testnet contribution, with retention-style vesting. This is functionally analogous to an RSU grant to a founding employee for prior work, not ongoing yield.

The trade-off is operator bootstrap aggressiveness. v2.1's 200M-TOKEN bucket front-loaded ~80M into years 1–2 to seed operator tier upgrades without market TOKEN buys. v2.2 replaces this with a smaller (50M) one-time grant restricted to a smaller cohort (testnet operators). Years 2+ require operators to buy TOKEN on market to climb tiers. This is intentional: it caps the per-operator subsidy and forces post-bootstrap operator growth through the USDC→buyback→deflation flywheel rather than direct TOKEN distribution.

## Section 1. Distribution table

11 groups, sums to 100%. Total supply unchanged at 1B TOKEN.

| # | Group                          | %   | Type     | Category              | Vesting / notes                                                                                          |
|---|--------------------------------|-----|----------|-----------------------|----------------------------------------------------------------------------------------------------------|
| 1 | Core Contributors              | 15% | Internal | Core Contributors     | 4-year linear, 12mo cliff                                                                                |
| 2 | DAO Treasury                   | 15% | Internal | Treasury              | 5pp earmarked at TGE for Genesis Bond Credits (Section 2); 10pp 4-year linear unlock to Timelock wallet  |
| 3 | Protocol Owned Liquidity       | 15% | External | Liquidity Provision   | Treasury-owned position on Balancer V3 80/20 per ADR 018                                                 |
| 4 | App Incentives                 | 14% | External | Ecosystem Incentives  | 4-year unlock to Timelock-controlled multisig; sub-programs per Section 3                                |
| 5 | Seed Investors                 | 11% | Internal | Private Investors     | 3-year linear, 6mo cliff                                                                                 |
| 6 | Private Investors              | 9%  | Internal | Private Investors     | 3-year linear, 6mo cliff                                                                                 |
| 7 | Market Making                  | 5%  | External | Liquidity Provision   | Genesis-liquid, MM-partner-allocated                                                                     |
| 8 | Misc. Marketing, PR, KOLs      | 5%  | Internal | Marketing             | Treasury-managed, ad-hoc spend within annual budget cap                                                  |
| 9 | Public Sale                    | 5%  | External | Public Sale           | Genesis-liquid (or 6mo lockup if regulatory posture requires)                                            |
| 10 | Advisors                      | 3%  | Internal | Core Contributors     | 2-year linear, 6mo cliff                                                                                 |
| 11 | Exchange Partnerships         | 3%  | External | Marketing             | Milestone-based to CEX listings, market-makers                                                           |
|    | **Total**                     | **100%** |     |                       |                                                                                                          |

**Internal / External rollup.** Internal (Core, Advisors, Seed, Private, Treasury, Misc Marketing) = 58%; External (POL, App Incentives, Market Making, Public Sale, Exchange Partnerships) = 42%.

**Treasury sub-allocation at TGE.** The 15% DAO Treasury bucket splits on-chain at TGE into:

- **5pp / 50M TOKEN — Genesis Bond Credits.** Transferred into the `CapacityBond` contract via a one-shot batched grant call; never enters the operational Treasury wallet. Section 2.
- **10pp / 100M TOKEN — Operational Treasury.** Standard Timelock-controlled wallet; funds USDC operator subsidies, App Incentives multisig payouts, and discretionary grants. 4-year linear unlock.

**Comparison to v2.1.**

| Change vs v2.1                                                                                | Delta   |
|------------------------------------------------------------------------------------------------|---------|
| Operator Service Emissions (group 7 in v2.1) eliminated                                       | −20pp   |
| Liquidity Mining Rewards (group 8 in v2.1, already 0%) formally removed                       | 0pp     |
| Airdrops (group 9 in v2.1) eliminated as standalone group                                     | −3pp    |
| Incentivized Testnet Rewards (group 10 in v2.1) absorbed into Genesis Bond Credits (within Treasury) | −3pp   |
| App Incentives (new) added                                                                    | +14pp   |
| Core Contributors 12 → 15                                                                     | +3pp    |
| Seed Investors 9 → 11                                                                         | +2pp    |
| Private Investors 7 → 9                                                                       | +2pp    |
| Public Sale 3 → 5                                                                             | +2pp    |
| Misc. Marketing, PR, KOLs 3 → 5                                                               | +2pp    |
| Market Making 4 → 5 (split from v2.1 group 13)                                                | +1pp    |
| POL 15 → 15 (split from v2.1 group 13)                                                        | 0pp     |
| **Net**                                                                                       | **0pp** |

**Removed-bucket replacements.** The 3pp airdrop and 3pp testnet rewards do not literally route into Treasury 1:1 — they were absorbed into Treasury reallocation alongside the operator-emissions reallocation. The mechanisms they served are preserved differently:

- Testnet rewards → Genesis Bond Credits program (larger, testnet-conditional, auto-bonded).
- Community airdrop → not pre-committed at TGE. DAO may vote to fund a targeted airdrop later from operational Treasury if ecosystem-growth needs warrant. This is discretionary, not a structural commitment.

## Section 2. Genesis Bond Credits

**Purpose.** Replace the year-1 operator-tier-upgrade runway that Operator Service Emissions previously provided. A bounded, one-time, retroactive grant — not an emission schedule.

**Allocation.** 50M TOKEN (5% of supply), carved at TGE from the DAO Treasury bucket. The operational Treasury keeps the remaining 100M / 10pp.

**Eligibility.** Pre-launch incentivized-testnet operators only. Per-operator allocation is computed at TGE as a weighted score of measured testnet contribution:

```
score(op) = bytes_delivered(op) × uptime_ratio(op) × probe_success_ratio(op)
allocation(op) = 50M × score(op) / Σ score(all eligible operators)
```

Exact normalization, minimum thresholds, and per-operator caps are determined in the implementation plan (open question — Section 8). No post-TGE application window. No anchor-operator discretionary carve. No retroactive eligibility for non-testnet operators.

**Distribution at TGE.** Treasury executes a single batched `CapacityBond.grantGenesisCredit(operator, amount)` per eligible operator within a one-shot TGE window (default: 30 days post-deploy). After the window closes, the grant function is permanently disabled on-chain. Any TOKEN not granted within the window flows to the operational Treasury bucket.

The credit is auto-deposited into the operator's `CapacityBond` position; TOKEN never enters the operator's wallet at any point before vest.

**Vesting.** 24 months from TGE, linear by epoch (epoch length per ADR 026 conventions). Vest accrues only if the operator is registered and unslashed during that epoch. "Continued operation" is defined as `CapacityBond.isActive(op) && !isSlashed(op)`. No minimum bytes-delivered threshold — non-delivery is already handled by the existing slashing pipeline (probe failures → slash → vest pauses).

**Slashing.** The full pending credit (`pendingCredit.total - pendingCredit.vested`) is slashable on the same terms as the operator's voluntary bond. The slashing primitive in `CapacityBond._slash` (per ADR 028) extends to iterate over both voluntary and credit-pending portions of the position. Slashed amounts route to SafetyReserve per ADR 033.

**Exit before 24mo.** On voluntary unbond:

- Vested portion stays in the operator's `CapacityBond` position and follows the standard 14-day unbonding window.
- Unvested portion (`pendingCredit.total - pendingCredit.vested`) is transferred back to the operational Treasury wallet via the existing Treasury reference.

**Re-registration.** An operator who exited and lost unvested credit cannot recover it by re-registering. The grant is one-shot per operator.

**Howey framing.** This is a retroactive grant for prior verifiable work (testnet), with a retention condition (RSU-style cliff vesting), bounded in time (24mo) and amount (50M aggregate, one-shot). It is structurally distinct from ongoing service emission: the work that earned the grant is complete at TGE; the vesting cliff is purely a retention incentive, not a payment for ongoing service. This shape matches founding-employee RSU grants, not yield to passive bonders.

## Section 3. App Incentives

**Purpose.** Subsidize demand-side adoption — publishers who serve content via deCDN and apps that integrate deCDN as their CDN backend. A customer-acquisition incentive aimed at consumers of the service, not at TOKEN bonders.

**Allocation.** 140M TOKEN (14% of supply). 4-year linear unlock into a dedicated Timelock-controlled multisig (separate wallet from operational Treasury to keep accounting clean).

**Two sub-programs.** The 10/4 split below is a governance norm, not an on-chain enforcement — the DAO can rebalance over time within the 14% envelope.

### 3.1 Publisher Rebates (~10pp / 100M TOKEN indicative)

Quarterly TOKEN rebate to publishers whose USDC spend on the network exceeds a minimum threshold. Mechanism:

- Publishers self-identify by signing a rebate-program agreement and completing light KYC.
- Each quarter, Treasury multisig audits served-bytes attributed to each enrolled publisher (using FeeRouter accounting data and probe-verified delivery).
- Rebate paid in TOKEN, denominated as a fraction of the publisher's USDC FeeRouter contribution that quarter. The exact rebate ratio is a DAO-tunable parameter; on-chain enforcement is the program's overall budget cap. Indicative quarterly budget = (140M × 10/14) / 16 quarters ≈ 6.25M / quarter; unused budget rolls forward.
- Rebate budget unused in a quarter rolls forward.

**Howey framing.** The publisher is a paying customer; the rebate is a TOKEN-denominated discount on services they purchased. They are not bonders; the rebate is not conditional on holding TOKEN; they have no expectation of profit from "the efforts of others." This is the same shape as airline frequent-flyer miles or AWS cloud credits — a customer-loyalty program, not an investment contract.

### 3.2 Integration Grants (~4pp / 40M TOKEN indicative)

Milestone-based lump-sum grants to projects that integrate deCDN as their CDN backend. Standard grant-program shape: application → milestone definition → on-delivery payout via Treasury multisig. Targets:

- CMS plugins (WordPress, Ghost, Decap, Sanity)
- Framework adapters (Next.js, Astro, SvelteKit)
- Hosting platforms (Vercel, Netlify, Cloudflare Pages CDN-backend swap)
- Language SDKs beyond Rust (TypeScript, Go, Python)

Per-project grant ceiling is a DAO-tunable parameter; the sub-program's 40M aggregate cap is the binding on-chain constraint. Indicative per-project range: 50K–500K TOKEN depending on scope.

### 3.3 Constraints

- App Incentives recipients are **not** eligible for Genesis Bond Credits (and vice versa). The cohorts are explicitly disjoint: Genesis Credits are operator-side / testnet-conditional; App Incentives are demand-side / customer-program.
- Co-marketing spend (case studies, conferences, advertising) is funded from the separate 5% **Misc. Marketing, PR, KOLs** bucket, not from App Incentives. This keeps App Incentives as a pure demand-side subsidy and avoids double-counting marketing spend.
- No new contract is deployed for either sub-program at launch. Treasury multisig handles both. If publisher-rebate volume grows large enough to warrant programmatic distribution, a future `PublisherRebateRouter` contract can subsume the rebate flow — deferred to post-launch and out of scope here.

## Section 4. Contract surface impact

Net-subtractive vs v2.1: one contract deleted, one extended in place, no contracts added.

### Deleted

**`OperatorEmissions`.** The contract introduced in v2.1 to stream the 20% Operator Service Emissions bucket per verified delivery. All its logic — epoch accounting hook, emission-curve math, auto-deposit into CapacityBond — is removed entirely from the inventory and the implementation plan.

### Extended

**`CapacityBond`** gains genesis-credit fields and three external entrypoints. Indicative Solidity sketch (precise interface in ADR 016 update):

```solidity
struct PendingCredit {
    uint128 total;       // total credit granted at TGE
    uint128 vested;      // amount accrued via continued-operation vesting
    uint64  grantedAt;   // TGE timestamp (immutable per operator)
}

mapping(address => PendingCredit) public pendingCredit;

/// One-shot grant entrypoint, callable only by Treasury within the TGE window.
/// Permanently disables after `grantGenesisCreditWindowEnd`.
function grantGenesisCredit(address op, uint256 amount) external onlyTreasuryAtTGE;

/// Permissionless, idempotent. Updates `pendingCredit[op].vested` based on elapsed
/// epochs and the operator's active+unslashed history. Cheap; batchable in any tx.
function accrueGenesisVest(address op) external;

/// Moves the currently-vested portion from `pendingCredit[op].vested` into the
/// operator's bonded amount. Callable by the operator at any time.
function claimVestedCredit(address op) external;
```

Behavioral notes:

- `grantGenesisCredit` requires `pendingCredit[op].total == 0` (one-shot per operator) and that the TGE window has not closed.
- `accrueGenesisVest` is the only function the protocol calls during slashing or unbond paths to ensure `vested` reflects current state.
- `CapacityBond._slash` (the slashing primitive referenced by ADR 028) iterates the operator's full position including `pendingCredit.total - pendingCredit.vested`. Slashed amounts continue to flow to SafetyReserve unchanged.
- On voluntary unbond, the contract computes the unvested portion and transfers it to the operational Treasury wallet via an existing reference. No new external dependency.

### Unchanged

- `FeeRouter` — already 4-bucket post-v2.1. No further changes.
- `SafetyReserve`, `BuybackBurner`, payment channels, `DecdnGovernor`, `Timelock` — untouched.
- `VotingEscrow`, `DelegatorBuyer` — already removed in v2.1; stay removed.

### Net delta vs v2.1 contract inventory

v2.1 added one new contract (`OperatorEmissions`) net of v1. v2.2 removes that contract and extends `CapacityBond` in place. **Net new contracts vs v1: zero.** Audit-surface impact is smaller than v2.1.

## Section 5. Operator bootstrap narrative

The economic story for an operator joining at TGE+0 under v2.2:

**Year 1 (testnet-eligible operator).**

- Receives Genesis Bond Credit auto-deposited into CapacityBond. For a median testnet operator, this might be on the order of 200K–1M TOKEN (sensitive to the testnet-contribution weighting formula in Section 8).
- 50% of the credit vests over the year; remaining 50% in year 2.
- Eligible for USDC infrastructure subsidies from operational Treasury (VPS/bandwidth grants — same mechanism as v2.1 §6).
- Earns USDC per byte delivered via FeeRouter operator share.
- Net: positive unit economics on day 1 even at modest delivery volume.

**Year 1 (non-testnet operator).**

- No Genesis Credit. Must purchase TOKEN on market to bond.
- Still eligible for USDC infrastructure subsidies on case-by-case basis.
- Earns USDC per byte delivered.
- Tier upgrades require continued TOKEN purchases.
- Slower bootstrap than testnet operators; intentional design.

**Year 2.**

- Genesis Credits fully vest by month 24.
- USDC infrastructure subsidies taper per operational Treasury budget.
- All tier upgrades require market TOKEN buys.
- The buyback-burn deflationary loop (driven by network USDC fee flow) provides the long-term TOKEN value-accrual story.

**Year 3+.**

- Steady state: USDC fees in, BuybackBurner deflation, no TOKEN-side subsidy. Operator economics determined entirely by per-byte unit economics and TOKEN price appreciation.

This is a tighter bootstrap than v2.1's 6-year emission curve. The trade-off is intentional: a smaller, time-bounded subsidy reduces dilution and eliminates the passive-yield-shaped surface. Testnet contribution is the gating signal for who gets the subsidy at all.

## Section 6. Regulatory posture

v2.2's Howey-prong analysis:

1. **Investment of money.** Bonders deposit TOKEN. Yes, satisfied.
2. **Common enterprise.** Operators participate in a shared network. Yes, satisfied.
3. **Expectation of profit.** Bonders expect TOKEN to appreciate via buyback-burn. Present, but the expectation is from deflation driven by their own and other operators' work, not from a managerial yield distribution.
4. **Solely from the efforts of others.** **This is the prong v2.2 strengthens.** Under v2.1 §5.1, Operator Service Emissions distributed TOKEN to bonders for verified delivery — even with the "payment for work" reinterpretation, the distribution itself was performed by the protocol (managerial) and accrued to bonders ongoing. Under v2.2, **no such distribution exists**. The only TOKEN flow to bonders is buyback-burn-driven price appreciation, which depends on each operator's own work (delivering bytes that generate USDC fees). The Genesis Bond Credit is a retroactive, one-shot, retention-vested grant — structurally an RSU, not yield.

v2.2's posture is **cleaner than v1, v2, and v2.1** on prong 4. Comparison:

| Variant | Passive-yield-shaped surface                                              | Howey-prong-4 cleanliness |
|---------|----------------------------------------------------------------------------|----------------------------|
| v1      | DelegatorBuyer (passive ve-locker yield)                                  | Worst                      |
| v2      | LM Rewards (passive LP yield) + Operator Service Emissions                | Middle                     |
| v2.1    | Operator Service Emissions only (reinterpreted as service emission)       | Better than v2             |
| v2.2    | None ongoing; only retroactive RSU-shaped Genesis Bond Credits             | Best                       |

This does not constitute legal advice; it is a structural posture comparison. Legal review per `internal/Legal/entity-structure-design.md` should re-validate prong 4 under v2.2.

## Section 7. ADR and downstream impact

### ADR rebase

| ADR                          | Change                | Scope                                                                                                                                                                                       |
|------------------------------|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **026 (Tokenomics)**         | **Substantial rewrite** | Replace distribution table with the 11-group table (Section 1). Replace "Operator Service Emissions" section with two new sections: "Genesis Bond Credits" and "App Incentives." Update operator-bootstrap narrative. Update internal/external rollup. |
| **016 (Contract interactions)** | Targeted edit         | Remove `OperatorEmissions` from the Contract Inventory. Add `PendingCredit` struct + three functions to the `CapacityBond` interface. Update class diagram.                                  |
| **028 (Slashing appeals)**   | Minor edit            | Note that `pendingCredit.total - pendingCredit.vested` is slashable on the same terms as voluntarily bonded amount; appeals flow unchanged.                                                  |
| 003 (Payments)               | No change             | FeeRouter is already 4-bucket; v2.2 does not touch the fee split.                                                                                                                            |
| 009 (Governance)             | No change             | Capacity-weighted voting is unaffected by emission removal.                                                                                                                                  |
| 018 (Liquidity strategy)     | No change             | POL still 15%, MM still 5%. Same buyback-burn shape; v2.2 likely sees somewhat lower TOKEN sell-pressure than v2.1 (no emission stream).                                                     |
| 033 (SafetyReserve)          | No change             | Receives slashed credits via the existing slashing pathway.                                                                                                                                  |

### Downstream materializations

- **`finance/notebooks/_shared/params.py`.** Rebase the `Allocation` dataclass to the 11-group table. Remove operator-emission fields. Add `genesis_bond_credit_total` (50M), `app_incentives_total` (140M), and the Treasury sub-allocation split (50M/100M). Re-run any notebooks that read these constants: `node-economics`, `bootstrap-runway`, `token-flows`, and any S1–S4 scenario notebooks. Expect the operator-bootstrap runway notebook to need substantive narrative changes because the year-1-2 economics curve changes shape.
- **`internal/Legal/entity-structure-design.md`.** Refresh any "passive-yield posture" or "service-emission" paragraph to reflect v2.2. Cite this spec.
- **`.github/profile/README.md`, `website/`, `internal/Fundraising/` materials.** No edits in this spec's scope. The workspace-wide single-source-of-truth rule (CLAUDE.md) handles propagation after ADR 026 is accepted: downstream surfaces re-cite the updated ADR.

### Sequencing

Spec acceptance → three follow-up PRs in order:

1. **Spec PR.** This document. Single file, no code/ADR changes.
2. **ADR PR.** ADR 026 rewrite, ADR 016 targeted edit, ADR 028 minor edit. All three together because 026 is the load-bearing change and 016/028 reference it. v2.1 spec status downgraded to "Superseded by 2026-05-27" in the same PR.
3. **params + notebooks PR.** `finance/notebooks/_shared/params.py` rebase + affected notebook re-runs in `finance/`. After PR 2 lands.

Implementation plan (smart-contract surface for `CapacityBond` extension, removal of `OperatorEmissions` references, test updates) is the next step after this spec is accepted — produced by invoking the `writing-plans` skill.

## Section 8. Open questions and deferred

1. **Testnet-contribution weighting formula.** Section 2 gives the form `score = bytes × uptime × probe_success`. The exact normalization (e.g., log-scaling on bytes to dampen whale operators? minimum eligibility thresholds? per-operator caps?) is deferred to the implementation plan. Recommend modeling in `finance/notebooks/` against testnet telemetry before locking in.
2. **App Incentives 10/4 split as governance norm.** The 100M / 40M split between publisher rebates and integration grants is indicative, not on-chain enforced. Confirm DAO governance can rebalance this within the 14% envelope without requiring an ADR.
3. **Post-launch `PublisherRebateRouter` decision criteria.** When (if ever) should the publisher-rebate flow move from Treasury-multisig to a programmatic contract? Define a quarterly-volume threshold (e.g., > 4M TOKEN rebated per quarter for two consecutive quarters) as a trigger for an implementation PR.
4. **Public-sale framing.** v2.1 had a 3% Public Sale at "Genesis-liquid (or 6mo lockup if regulatory posture requires)". v2.2 grows this to 5%. The 6mo lockup question scales with the larger bucket — re-evaluate with legal before TGE.
5. **Genesis Bond Credit grant-window length.** Default 30 days post-deploy. Confirm sufficient for batched on-chain grants across the testnet operator set; alternatively allow Treasury to extend once via Timelock vote.
6. **Slashing of vested-but-unclaimed credit.** If an operator has accrued vested credit but has not called `claimVestedCredit`, and is subsequently slashed, what fraction is slashable? Recommended: the vested portion is no longer slashable (it has effectively become voluntarily-bonded TOKEN); only `pendingCredit.total - pendingCredit.vested` is slashable. Confirm with ADR 028 authors during the ADR PR.

## Section 9. Precedents and reasoning

The v2.2 design is closest in shape to Helium's Light Hotspot model and Filecoin's storage-provider bond — operators bond network-utility-tokens to commit capacity, get paid in a stable instrument (USDC here; HNT/FIL there), and benefit from token deflation when the network generates revenue. Both Helium and Filecoin do run service-emission programs, which v2.2 declines to mirror.

The cleanest precedent for v2.2's specific posture is closer to Akash Network's USDC-payment work-token: operators bond AKT, earn USDC for deployments, and AKT value derives from network-revenue burn. Akash does not run an emission program to bonders. v2.2 follows this template more closely than v2.1 did.

The Genesis Bond Credit shape (retroactive, time-bounded, retention-vested) has direct precedent in TGE-era founding-employee RSU grants and in early-validator delegations on multiple Cosmos-zone chains. Both are routinely structured as one-shot grants with multi-year vest and forfeiture-on-exit; both have survived regulatory review without being characterized as ongoing yield.
