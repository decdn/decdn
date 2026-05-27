# Remove Operator Emissions (v2.2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the v2.2 no-emission tokenomics rewrite — eliminate the 20% Operator Service Emissions bucket, replace with Genesis Bond Credits (5% / 50M TOKEN carved from DAO Treasury) + App Incentives (14%), and update the affected ADRs and downstream `params.py` materialization.

**Architecture:** Documentation-and-data-only change. No Rust or Solidity code changes (the contract surface lives only in ADR text at this stage — the actual contracts have not been implemented). Work splits into three sequenced PRs:

1. **PR-spec** (already landed as commit `dd02d45`): the v2.2 design doc.
2. **PR-adr**: rewrites/edits to ADRs 026, 016, 028 + decdn/CLAUDE.md ADR note + v2.1 spec status downgrade.
3. **PR-params**: rebase `finance/notebooks/_shared/params.py` and re-run affected notebooks.

**Tech Stack:** Markdown (ADRs and specs), Python 3 dataclasses (`finance/notebooks/_shared/params.py`), Jupyter notebooks (`finance/notebooks/*.ipynb`), pre-commit hooks (markdownlint, cargo-deny, ADR reference hygiene).

**Source of truth:** The spec at `decdn/docs/superpowers/specs/2026-05-27-remove-operator-emissions-design.md` (commit `dd02d45`). All design-content decisions are locked there; this plan does not re-litigate them.

**Conventions:**

- Run pre-commit hooks before each commit (`pre-commit run --files <paths>` or just `git commit` which triggers the hook).
- Use HEREDOC commit messages with the standard `Co-Authored-By` trailer.
- One logical commit per task. Do not amend; if a hook fails, fix and commit again.
- For Markdown lists, always leave a blank line before and after (the `MD032/blanks-around-lists` rule failed in PR-spec).
- Match existing ADR style — Title Case section headers (`## Decision`, `### Subsection`), inline `[ADR 026 § Section](026-tokenomics.md#section)` link format.

---

## File Inventory

**Modified in PR-adr:**

- `decdn/docs/superpowers/specs/2026-05-24-work-token-tokenomics-redesign-v2.1.md` — status downgrade only.
- `decdn/adr/026-tokenomics.md` — substantial rewrite (~10 sections touched).
- `decdn/adr/016-contract-interactions.md` — targeted edit; remove `OperatorEmissions` everywhere, extend `CapacityBond` interface with `PendingCredit`.
- `decdn/adr/028-slashing-appeals.md` — minor edit; update single reference + add a note about unvested-credit slashability.
- `decdn/CLAUDE.md` — ADR note paragraph update (status of ADR 026 / canonical-source statement).

**Modified in PR-params:**

- `finance/notebooks/_shared/params.py` — rebase `SUPPLY_ALLOCATION`, remove `OperatorEmissionsParams`, add `GenesisBondCreditParams` and `AppIncentivesParams`, update `BootstrapParams` docstrings.
- `finance/notebooks/01_node_economics.ipynb` — re-run; expect changes in any cell that references operator-emission revenue.
- `finance/notebooks/02_bootstrap_runway.ipynb` — re-run; narrative changes likely (year-1 economics shape).
- `finance/notebooks/04_token_flows.ipynb` — re-run; new buckets show up in flow diagram.

**Not touched in this plan:**

- Workspace files outside `decdn/` and `finance/` (website, internal, .github). The SoT rule says these pick up changes via the ADR-driven update flow; that sweep is out of scope here.
- Rust crates (`crates/incentive/`, `crates/reputation/`, etc.). No code currently references Operator Emissions by name.
- Solidity contracts under `contracts/` — empty, no code yet.

---

## PR-adr · Phase 1: v2.1 spec status downgrade

### Task 1: Downgrade v2.1 spec status to "Superseded"

**Files:**

- Modify: `decdn/docs/superpowers/specs/2026-05-24-work-token-tokenomics-redesign-v2.1.md:3` (Status line)

- [ ] **Step 1: Open the v2.1 spec and locate the Status line**

The current line 3 reads:

```markdown
**Status:** Accepted 2026-05-25 (canonical work-token tokenomics; supersedes v1, v2, and ADRs 026/034/035 as the rewrite commits land).
```

- [ ] **Step 2: Replace the Status line**

Replace with:

```markdown
**Status:** Superseded 2026-05-27 by `2026-05-27-remove-operator-emissions-design.md` (v2.2 no-emission variant). Previously Accepted 2026-05-25; retained as the historical record of the 13-group / 20%-emission variant that v2.2 obsoletes.
```

- [ ] **Step 3: Run pre-commit**

```bash
pre-commit run --files decdn/docs/superpowers/specs/2026-05-24-work-token-tokenomics-redesign-v2.1.md
```

Expected: All hooks pass.

- [ ] **Step 4: Commit**

```bash
git add decdn/docs/superpowers/specs/2026-05-24-work-token-tokenomics-redesign-v2.1.md
git commit -m "$(cat <<'EOF'
docs(specs): mark v2.1 tokenomics spec as Superseded by v2.2

The v2.2 no-emission design (2026-05-27) replaces v2.1's Operator
Service Emissions with bounded Genesis Bond Credits and adds the App
Incentives bucket. v2.1 retained as historical record.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## PR-adr · Phase 2: ADR 026 substantial rewrite

ADR 026 is the load-bearing tokenomics document. We rewrite it in five commits, smallest-blast-radius first.

### Task 2: ADR 026 — replace distribution table and rollups

**Files:**

- Modify: `decdn/adr/026-tokenomics.md:33-67` (Allocation section: intro paragraph, table, categorical rollup, internal/external rollup)

- [ ] **Step 1: Read the current allocation section**

Currently lines 33–67 contain the 13-group v2.1 distribution. Confirm the section boundaries by reading lines 33–80 (the section ends before `**Genesis liquid float (TGE Day 1).**`).

- [ ] **Step 2: Replace lines 33–67 with the 11-group v2.2 distribution**

Replace the entire block starting at `#### Allocation` through (and including) the Internal/External rollup paragraph with:

````markdown
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
````

- [ ] **Step 3: Verify table sums and rollup math**

```bash
# Table sums: 15+15+15+14+11+9+5+5+5+3+3 = 100
# Internal: 15+15+11+9+5+3 = 58
# External: 15+14+5+5+3 = 42
# Categorical: 18+20+15+5+14+8+20 = 100
```

- [ ] **Step 4: Run pre-commit**

```bash
pre-commit run --files decdn/adr/026-tokenomics.md
```

Expected: All hooks pass. Note: if `adr reference hygiene` flags broken anchors (the new section anchors `#genesis-bond-credits` and `#app-incentives` don't exist yet), that's expected; they're created in Task 3 and Task 4. If the hook is strict and fails, stage the file but defer the commit until Task 4 — or land Tasks 2/3/4 as one commit. The hook is configured under `.pre-commit-config.yaml`; check whether it gates broken local anchors.

- [ ] **Step 5: Commit (or defer until Task 4 if anchor hook fails)**

```bash
git add decdn/adr/026-tokenomics.md
git commit -m "$(cat <<'EOF'
docs(adr): rewrite ADR 026 distribution table for v2.2 (11 groups)

Replace the v2.1 13-group table with the v2.2 11-group table. Remove
Operator Service Emissions (20%), Liquidity Mining (0%), Airdrops (3%),
and Incentivized Testnet (3%) as standalone groups. Add App Incentives
(14%) as a new External / Ecosystem Incentives line. Public Sale Type
changes Internal→External; remaining groups absorb the freed allocation
per the v2.2 spec table. Anchors for §Genesis Bond Credits and §App
Incentives land in subsequent commits.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 3: ADR 026 — replace § Operator Service Emissions with § Genesis Bond Credits

**Files:**

- Modify: `decdn/adr/026-tokenomics.md:177-189` (the entire `### Operator Service Emissions` section)

- [ ] **Step 1: Read the current § Operator Service Emissions section**

```bash
sed -n '175,195p' decdn/adr/026-tokenomics.md
```

This shows the current section (lines 177–189) plus 2 lines of context.

- [ ] **Step 2: Replace the section heading and body**

Replace lines 177–189 inclusive (`### Operator Service Emissions` through the Filecoin/Livepeer/Helium precedent line) with:

````markdown
### Genesis Bond Credits

Group 2 carves 5pp / 50M TOKEN at TGE for **Genesis Bond Credits** to verified pre-launch testnet operators. Purpose: replace the year-1 operator-tier-upgrade runway that v2.1's Operator Service Emissions previously provided, without ongoing emission. A bounded, retroactive, one-shot grant — not a service-conditional distribution schedule.

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

**Howey framing.** This is a retroactive grant for prior verifiable work (testnet), with a retention-style vesting cliff. Structurally distinct from ongoing service emission: the work that earned the grant is complete at TGE; the cliff is a retention incentive, not payment for ongoing service. Shape matches founding-employee RSU grants, not yield to passive bonders. See the v2.2 design spec § 6 for the full Howey-prong comparison across v1/v2/v2.1/v2.2.

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
````

- [ ] **Step 3: Run pre-commit**

```bash
pre-commit run --files decdn/adr/026-tokenomics.md
```

Expected: All hooks pass (with Task 2's commit landed, the anchors `#genesis-bond-credits` and `#app-incentives` referenced from the distribution table now resolve).

- [ ] **Step 4: Commit**

```bash
git add decdn/adr/026-tokenomics.md
git commit -m "$(cat <<'EOF'
docs(adr): ADR 026 — replace §Operator Service Emissions with §Genesis Bond Credits + §App Incentives

Delete the v2.1 §Operator Service Emissions section. Add two new
sections matching the v2.2 spec:

  §Genesis Bond Credits — 5pp/50M TOKEN carve from Group 2 (Treasury),
    testnet-conditional, auto-bonded into CapacityBond, 24mo vest via
    continued operation, slashable, unvested-on-exit returns to Treasury.

  §App Incentives — 14% / 140M TOKEN demand-side program (publisher
    rebates + integration grants), Treasury-multisig administered.

Resolves the spec §8 open question on slashing of vested-but-unclaimed
credit: vested portion is no longer slashable once it has functionally
become voluntarily-bonded TOKEN.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 4: ADR 026 — sweep remaining references to Operator Service Emissions

**Files:**

- Modify: `decdn/adr/026-tokenomics.md` (multiple lines: 134, 142, 162, 262–278, 292–300, 337–348, 366, 377)

- [ ] **Step 1: Locate every remaining reference**

```bash
grep -n "Operator Service Emission\|operator emission\|Group 7\|service emission\|Staking Reward\|OperatorEmissions" decdn/adr/026-tokenomics.md
```

Expected matches in roughly these locations (line numbers will shift after Tasks 2-3):

- The "S1–S4 leaves essentially all TOKEN liquid" paragraph (was line 134) — references the §Operator Service Emissions bucket as the bootstrap cover.
- The bullet list in § What the bond grants (was line 142) — "Eligibility to receive Operator Service Emissions".
- The §FeeRouter split closing paragraph (was line 162) — "Per-epoch byte counters are retained for analytics and to feed the §Operator Service Emissions distribution".
- § Operator economics (was lines 262–278) — the year-by-year narrative.
- § Bootstrap mechanism — pre-seed USDC table (was line 300).
- § Consequences > Positive (was line 337) — the "smaller contract surface" bullet.
- § Consequences > Negative or Risks (was line 348) — the "capital cost to operate at edge tier" mitigation citing emissions runway.
- § Cross-ADR Impact (was line 366) — the ADR-016 row.
- § Deferred & Open (was line 377) — the "service-emission curve form" open question.

- [ ] **Step 2: Edit each reference per the rule set below**

For each reference:

- **"S1–S4 bootstrap" paragraph:** Replace `the [§ Operator Service Emissions](#operator-service-emissions) bucket covers operator-tier-upgrade economics through ~S3 without external TOKEN buys` with `[§ Genesis Bond Credits](#genesis-bond-credits) (50M / 5%) covers operator tier upgrades for the testnet-eligible cohort through year 2 without external TOKEN buys; non-testnet operators bond TOKEN purchased on market`.
- **§ What the bond grants bullet list:** Replace `- Eligibility to receive Operator Service Emissions (see [§ Operator Service Emissions](#operator-service-emissions)) for verified delivery.` with `- Eligibility to receive [§ Genesis Bond Credits](#genesis-bond-credits) if the operator participated in the pre-launch incentivized testnet.`
- **§ FeeRouter split closing paragraph:** Replace `Per-epoch byte counters are retained for analytics and to feed the [§ Operator Service Emissions](#operator-service-emissions) distribution; they no longer drive bucket payouts.` with `Per-epoch byte counters are retained for analytics only; they no longer drive bucket payouts and no longer feed any TOKEN-distribution contract under v2.2.`
- **§ Operator economics narrative:** Find the numbered list that includes `2. **Operator Service Emissions** (TOKEN-denominated) — auto-deposited into CapacityBond; not withdrawable until full unbond; sized at 20% of supply distributed over ~6 years.` and the "Months 1–24: earns Operator Service Emissions" worked example. Replace both with:

  ```markdown
  2. **Genesis Bond Credits** (TOKEN-denominated, testnet-eligible operators only) — auto-deposited into CapacityBond at TGE; vests linearly over 24mo via continued operation; not withdrawable as liquid until vested and unbonded; sized at 5% of supply / 50M TOKEN.
  ```

  And for the worked example:

  ```markdown
  - Months 1–24 (testnet-eligible operator): Genesis Bond Credit auto-bonded at TGE; vests linearly. For a median testnet operator at ~500K TOKEN credit, bond effectively climbs from voluntary 50K → 50K+vested as continued operation accrues. Non-testnet operator at the same scale must purchase TOKEN on market to climb 1G → ~3G → 10G tier and is excluded from credit eligibility.
  ```

- **§ Bootstrap mechanism — pre-seed USDC table:** Replace the right-hand cell `Covers VPS/bandwidth for first 12 months for early operators; pairs with [§ Operator Service Emissions](#operator-service-emissions) to make first-year operator unit economics positive` with `Covers VPS/bandwidth for first 12 months for early operators; pairs with [§ Genesis Bond Credits](#genesis-bond-credits) (for testnet-eligible operators) to make first-year operator unit economics positive`.
- **§ Consequences > Positive (smaller contract surface bullet):** Replace `OperatorEmissions is a new but small contract` with `OperatorEmissions (v2.1's new contract) is deleted; CapacityBond gains a small PendingCredit vesting extension instead`. The rest of the bullet (Net subtractive vs prior design, VotingEscrow/DelegatorBuyer deletions, FeeRouter simplification) is unchanged.
- **§ Consequences > Capital-cost-to-operate bullet:** Replace `Mitigated by the Operator Service Emissions runway (operators can grow tiers using granted TOKEN rather than market buys) and α-tunability.` with `Mitigated for testnet-eligible operators by the [§ Genesis Bond Credits](#genesis-bond-credits) program and by α-tunability; non-testnet operators must buy TOKEN on market to climb tiers, by design.`
- **§ Cross-ADR Impact (ADR 016 row):** Replace `\`VotingEscrow\` and \`DelegatorBuyer\` removed from Contract Inventory; \`StakingRegistry\` renamed \`CapacityBond\`; new \`OperatorEmissions\` contract added. FeeRouter simplifies. Class diagrams updated.` with `OperatorEmissions removed from Contract Inventory (v2.1's new contract is deleted under v2.2); CapacityBond extended with PendingCredit vesting + grantGenesisCredit/accrueGenesisVest/claimVestedCredit. VotingEscrow and DelegatorBuyer stay removed. FeeRouter unchanged from v2.1. Class diagrams updated.`
- **§ Deferred & Open (service-emission curve item):** Replace the entire numbered item `3. **Service-emission curve form.** ...` with:

  ```markdown
  3. **Testnet-contribution weighting formula.** The score formula in [§ Genesis Bond Credits](#genesis-bond-credits) is given in skeletal form. Exact normalization, minimum thresholds, and per-operator caps need specification before the TGE grant window opens. Recommend modeling in `finance/notebooks/` against testnet telemetry.
  4. **PublisherRebateRouter trigger.** If publisher-rebate volume grows large enough (e.g., > 4M TOKEN rebated per quarter for two consecutive quarters), a programmatic `PublisherRebateRouter` contract may replace the Treasury-multisig flow. Deferred to post-launch.
  5. **App Incentives 10/4 split governance.** The publisher-rebate / integration-grant split (100M / 40M indicative) is a governance norm, not on-chain enforced. Confirm DAO can rebalance within the 14% envelope without requiring an ADR amendment.
  ```

  (If the original list had items 4+ already, renumber as needed so the new items append cleanly.)

- [ ] **Step 3: Verify no stale references remain**

```bash
grep -n "Operator Service Emission\|operator emission\|Group 7\|service emission\|Staking Reward\|OperatorEmissions" decdn/adr/026-tokenomics.md
```

Expected: no matches except possibly inside a historical/comparison aside (e.g., "v2.1's OperatorEmissions"). The only acceptable matches are explicit "was" or "v2.1's" references.

- [ ] **Step 4: Run pre-commit**

```bash
pre-commit run --files decdn/adr/026-tokenomics.md
```

Expected: All hooks pass. The `adr reference hygiene` hook should be happy because all `#genesis-bond-credits` and `#app-incentives` anchors point to sections that exist as of Task 3.

- [ ] **Step 5: Commit**

```bash
git add decdn/adr/026-tokenomics.md
git commit -m "$(cat <<'EOF'
docs(adr): ADR 026 — sweep stale Operator Service Emissions references for v2.2

Update each remaining reference in ADR 026 to point at §Genesis Bond
Credits or §App Incentives as appropriate. Touched:

  - S1–S4 supply-table commentary
  - §What the bond grants (eligibility bullet)
  - §FeeRouter split (bytesPerEpoch no longer drives any payout)
  - §Operator economics (year-by-year narrative; worked example)
  - §Bootstrap mechanism — pre-seed USDC (pairing line)
  - §Consequences (smaller-contract-surface bullet; capital-cost mitigation)
  - §Cross-ADR Impact (ADR 016 row)
  - §Deferred & Open (new items: testnet-weighting formula, App
    Incentives split governance, PublisherRebateRouter trigger)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## PR-adr · Phase 3: ADR 016 rewrite

ADR 016 has the densest concentration of `OperatorEmissions` references (~30). We rewrite in four commits by section.

### Task 5: ADR 016 — header note + Contract Inventory + class diagram

**Files:**

- Modify: `decdn/adr/016-contract-interactions.md:12` (the bold header callout)
- Modify: `decdn/adr/016-contract-interactions.md:16-36` (Contract Inventory table — remove the OperatorEmissions row, update the CapacityBond row)
- Modify: `decdn/adr/016-contract-interactions.md:38-90` (Contract Architecture class diagram and its lead-in paragraph)

- [ ] **Step 1: Replace the header callout (around line 12)**

Old:

```markdown
> **[ADR 026](026-tokenomics.md#adr-026-tokenomics) driver.** The contract surface in this ADR follows the v2.1 work-token rewrite of [ADR 026](026-tokenomics.md#adr-026-tokenomics). `FeeRouter` ships as a four-bucket settlement distributor; `CapacityBond` (renamed from the prior `StakingRegistry`, with the capacity-curve lock-to-capacity logic added) is the operator-registry contract; `OperatorEmissions` is a new contract that distributes the 20% Operator Service Emissions bucket; `SafetyReserve` and `BuybackBurner` are wired with adjusted flows. The retired `VotingEscrow` and `DelegatorBuyer` contracts are not part of the v2.1 surface. Read [ADR 026](026-tokenomics.md#adr-026-tokenomics) first for the economic model; this ADR is the integration view.
```

New:

```markdown
> **[ADR 026](026-tokenomics.md#adr-026-tokenomics) driver.** The contract surface in this ADR follows the v2.2 no-emission rewrite of [ADR 026](026-tokenomics.md#adr-026-tokenomics). `FeeRouter` ships as a four-bucket settlement distributor; `CapacityBond` (renamed from the prior `StakingRegistry`, with the capacity-curve lock-to-capacity logic added, plus a `PendingCredit` extension that holds and vests Genesis Bond Credits) is the operator-registry contract; `SafetyReserve` and `BuybackBurner` are wired with adjusted flows. The retired `VotingEscrow`, `DelegatorBuyer`, and `OperatorEmissions` contracts are not part of the v2.2 surface (`OperatorEmissions` was v2.1-only and is deleted under v2.2). Read [ADR 026](026-tokenomics.md#adr-026-tokenomics) first for the economic model; this ADR is the integration view.
```

- [ ] **Step 2: Update Contract Inventory table**

Locate the table row at line ~26 starting with `| OperatorEmissions |` — **delete the entire row**.

Locate the `CapacityBond` row (it'll be near `OperatorEmissions` in the table). Update its `Notes` cell to append: `; under v2.2 also holds and vests PendingCredit positions for Genesis Bond Credits per [ADR 026 § Genesis Bond Credits](026-tokenomics.md#genesis-bond-credits)`.

- [ ] **Step 3: Update the class-diagram lead-in paragraph (line ~38)**

Old:

```markdown
The diagram below shows the full contract surface and its primary call relationships. `CapacityBond` is the operator-registry contract; `Governor` reads `capacityAt × age_ramp` from it as the voting-weight source. `OperatorEmissions` distributes the 20% Operator Service Emissions bucket and writes back into `CapacityBond` so granted TOKEN is bonded, not liquid.
```

New:

```markdown
The diagram below shows the full contract surface and its primary call relationships. `CapacityBond` is the operator-registry contract; `Governor` reads `capacityAt × age_ramp` from it as the voting-weight source. Under v2.2, `CapacityBond` also holds the 50M TOKEN Genesis Bond Credit allocation in `PendingCredit` positions per operator and vests them over 24mo via continued operation; there is no separate emissions contract.
```

- [ ] **Step 4: Update the class diagram (around lines 40–88)**

Inside the `classDiagram` block:

a. **Update the `CapacityBond` class block** — append three methods:

   ```
   +grantGenesisCredit(op, amount)
   +accrueGenesisVest(op)
   +claimVestedCredit(op)
   ```

   Remove the existing `+depositGrant(op, amount)` line — that was the OperatorEmissions hook and is replaced by the genesis-credit entrypoints.

b. **Delete the `OperatorEmissions` class block entirely:**

   ```
   class OperatorEmissions {
       +distribute(epoch)
       +setEmissionCurve(curve)
   }
   ```

c. **Delete the two relationship lines:**

   ```
   OperatorEmissions ..> FeeRouter : bytesPerEpoch (read)
   OperatorEmissions ..> CapacityBond : depositGrant (TOKEN auto-bond)
   ```

d. **Add one new relationship line** (Treasury → CapacityBond for the one-shot TGE grant):

   ```
   Treasury ..> CapacityBond : grantGenesisCredit (TGE one-shot)
   ```

- [ ] **Step 5: Run pre-commit**

```bash
pre-commit run --files decdn/adr/016-contract-interactions.md
```

Expected: pre-commit may flag any remaining references to `OperatorEmissions` further down in the document (Tasks 6–8 will fix). If the ADR-reference-hygiene hook is strict about within-document forward references, defer the commit until Task 8. Otherwise commit now.

- [ ] **Step 6: Commit**

```bash
git add decdn/adr/016-contract-interactions.md
git commit -m "$(cat <<'EOF'
docs(adr): ADR 016 — remove OperatorEmissions from Inventory, diagram, header

First slice of the ADR 016 v2.2 rewrite. Removes OperatorEmissions from
the Contract Inventory table, the Contract Architecture classDiagram,
and the header §ADR 026 driver callout. Extends CapacityBond's diagram
methods to expose grantGenesisCredit / accrueGenesisVest /
claimVestedCredit. Adds Treasury→CapacityBond grantGenesisCredit edge.

Sweep of remaining OperatorEmissions references in deployment order,
call table, fund flow, access control, and reentrancy lands in
subsequent commits.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 6: ADR 016 — delete § Contract: OperatorEmissions, extend § Contract: CapacityBond

**Files:**

- Modify: `decdn/adr/016-contract-interactions.md` — find and delete the entire `#### Contract: OperatorEmissions` section (was lines 202–253 in the v2.1 layout)
- Modify: `decdn/adr/016-contract-interactions.md` — find the `#### Contract: CapacityBond` section and extend its Solidity interface block

- [ ] **Step 1: Locate the OperatorEmissions contract section**

```bash
grep -n "^#### Contract: OperatorEmissions\|^#### Contract:" decdn/adr/016-contract-interactions.md
```

This shows the section heading and the next `#### Contract:` heading after it (which marks the end of the OperatorEmissions section).

- [ ] **Step 2: Delete the entire OperatorEmissions section**

Remove from `#### Contract: OperatorEmissions` through (but not including) the next `#### Contract: <Whatever>` heading. This typically spans ~50 lines including the interface block, the `EmissionCurve` struct, the bullets, and the bytesPerEpoch consumer note.

- [ ] **Step 3: Locate the CapacityBond contract section's interface block**

```bash
grep -n "^#### Contract: CapacityBond\|interface ICapacityBond" decdn/adr/016-contract-interactions.md
```

- [ ] **Step 4: Extend the `ICapacityBond` interface block**

Inside the interface block, after the existing function declarations and before the closing brace, append:

```solidity
    // ============================================================
    // Genesis Bond Credits (v2.2 § Genesis Bond Credits in ADR 026)
    // ============================================================

    /// Per-operator pending credit accounting. `total` is set once at TGE
    /// by the Treasury via `grantGenesisCredit`; `vested` accrues over
    /// 24mo via `accrueGenesisVest`. The operator may claim the vested
    /// portion into bonded TOKEN via `claimVestedCredit`. Unvested
    /// portion is slashable on the same terms as voluntarily-bonded
    /// TOKEN.
    struct PendingCredit {
        uint128 total;
        uint128 vested;
        uint64  grantedAt;
    }

    function pendingCredit(address operator) external view returns (PendingCredit memory);

    /// One-shot grant at TGE. Callable only by Treasury within the
    /// `GENESIS_CREDIT_WINDOW` (default 30 days post-deploy). After the
    /// window closes, the function permanently reverts. Requires
    /// `pendingCredit(op).total == 0` (one grant per operator). Pulls
    /// TOKEN from Treasury via `safeTransferFrom`.
    function grantGenesisCredit(address operator, uint256 amount) external;

    /// Permissionless, idempotent. Updates `pendingCredit[op].vested`
    /// to reflect epochs since `grantedAt` during which the operator
    /// was `isActive(op) && !isSlashed(op)`. Safe to call from any
    /// bond-mutating tx as a refresh.
    function accrueGenesisVest(address operator) external;

    /// Moves the currently-vested portion from `pendingCredit[op].vested`
    /// into the operator's `bondedAmount`. Callable by the operator at
    /// any time. After claim, the credit is functionally voluntary bond
    /// (per ADR 026 §Genesis Bond Credits, vested-claimed credit is no
    /// longer separately slashable as pending credit).
    function claimVestedCredit(address operator) external;
```

Also add a note in the prose surrounding this interface block:

```markdown
The Genesis Bond Credit entrypoints replace v2.1's external `depositGrant(operator, amount)` hook that was called by the (now-deleted) `OperatorEmissions` contract. The 50M TOKEN Genesis Bond Credit allocation is held by `CapacityBond` itself (transferred in at TGE via the batched `grantGenesisCredit` calls), with per-operator vesting tracked in the `pendingCredit` mapping. Slashing of an operator's position applies to both `bondedAmount` and `pendingCredit[op].total - pendingCredit[op].vested` simultaneously.
```

- [ ] **Step 5: Run pre-commit and commit**

```bash
pre-commit run --files decdn/adr/016-contract-interactions.md
git add decdn/adr/016-contract-interactions.md
git commit -m "$(cat <<'EOF'
docs(adr): ADR 016 — extend CapacityBond with PendingCredit; delete OperatorEmissions contract section

Replace v2.1's separate OperatorEmissions contract with a small
CapacityBond extension. CapacityBond gains:

  - PendingCredit{total, vested, grantedAt} per operator
  - grantGenesisCredit(op, amount): TGE-window one-shot, Treasury-only
  - accrueGenesisVest(op): permissionless, idempotent vesting accrual
  - claimVestedCredit(op): operator-callable promotion to bondedAmount

Deletes the entire §Contract: OperatorEmissions interface block,
EmissionCurve struct, and bytesPerEpoch consumer notes.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 7: ADR 016 — Deployment Order, Cross-Contract Call Graph, Off-chain Read API

**Files:**

- Modify: `decdn/adr/016-contract-interactions.md` — § Deployment Order and Initialization Dependencies (was lines 254–397), § Cross-Contract Call Graph (was lines 398–515), and surrounding sections.

- [ ] **Step 1: Locate every remaining `OperatorEmissions` reference**

```bash
grep -n "OperatorEmissions\|OE\[\"" decdn/adr/016-contract-interactions.md
```

This will list lines in the deployment order Mermaid graph, the constructor-dependencies table, the post-deployment-init enumerated list, the cross-contract call graph Mermaid, the call table, and the off-chain read API section.

- [ ] **Step 2: For each match, apply the corresponding rule**

| Match pattern                                                                                     | Action                                                                                                                            |
|---------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| Mermaid node like `OE["8. OperatorEmissions"]` in the deployment graph                            | Delete the node and its edges. Renumber subsequent nodes if any.                                                                  |
| Constructor-deps table row for `OperatorEmissions`                                                | Delete the row.                                                                                                                  |
| `Post-Deployment Initialization` enumerated step `Grant BOND_GRANTOR_ROLE on CapacityBond to OperatorEmissions` | Replace with: `Grant GENESIS_GRANTOR_ROLE on CapacityBond to Treasury (one-shot, scoped to GENESIS_CREDIT_WINDOW). This authorizes Treasury to call CapacityBond.grantGenesisCredit(operator, amount) within the TGE window per [ADR 026 § Genesis Bond Credits](026-tokenomics.md#genesis-bond-credits).` |
| Mermaid node `OE["OperatorEmissions"]` in cross-contract call graph                               | Delete the node and any edges (`OperatorEmissions --> CapacityBond : depositGrant`, etc.).                                       |
| Call-table rows where Caller = `OperatorEmissions` or Callee = `OperatorEmissions`                | Delete the rows. Add one new row: `\| Treasury \| CapacityBond \| grantGenesisCredit(op, amount) (TGE one-shot; allocates Genesis Bond Credit) \| GENESIS_GRANTOR_ROLE on CapacityBond \| Yes \|` |
| Off-Chain Read API mention of `OperatorEmissions.distribute(epoch)` as the only consumer of `bytesPerEpoch` | Replace with: `Under v2.2, bytesPerEpoch has no on-chain consumer; it is retained on FeeRouter as an analytics counter only, suitable for off-chain dashboards and the testnet-contribution score calculation that gates Genesis Bond Credit grants.` |

- [ ] **Step 3: Run pre-commit and commit**

```bash
pre-commit run --files decdn/adr/016-contract-interactions.md
git add decdn/adr/016-contract-interactions.md
git commit -m "$(cat <<'EOF'
docs(adr): ADR 016 — sweep OperatorEmissions from deployment, call graph, read API

Update deployment-order Mermaid, constructor-deps table, post-deploy
init steps, cross-contract call graph, call table, and Off-Chain Read
API to remove OperatorEmissions. Replace BOND_GRANTOR_ROLE granted to
OperatorEmissions with GENESIS_GRANTOR_ROLE granted to Treasury,
scoped to the GENESIS_CREDIT_WINDOW (default 30 days post-deploy).

bytesPerEpoch is now retained for off-chain analytics only — no on-chain
consumer remains under v2.2.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 8: ADR 016 — Fund Flow, Holdings Summary, Access Control, Reentrancy, Launch vs Steady-State

**Files:**

- Modify: `decdn/adr/016-contract-interactions.md` — § Fund Flow Diagrams (was lines 517–598), § Access Control Matrix (was lines 599–653), § Reentrancy Analysis (was lines 654–757), § Launch vs Steady-State Configuration (was lines 777–797), § Consequences (was lines 798–823).

- [ ] **Step 1: Locate remaining matches**

```bash
grep -n "OperatorEmissions\|OE\[\"\|BOND_GRANTOR\|depositGrant" decdn/adr/016-contract-interactions.md
```

After Tasks 5–7, the remaining matches should be concentrated in the diagrams and tables in this set of sections.

- [ ] **Step 2: Apply per-section rules**

- **Fund Flow Mermaid (TOKEN Flow diagram):** Delete the `OE["OperatorEmissions<br/>(200M TOKEN bucket)"]` node and its edges. Replace with one node/edge representing the Treasury→CapacityBond TGE flow: `T["Treasury (Genesis Bond Credit carve, 50M)"] -- "grantGenesisCredit (TGE, batched)" --> CB`.
- **Contracts Holding Funds Summary table:**
  - `CapacityBond` row: append to the "Source" cell the text "+ Treasury grantGenesisCredit writes at TGE (50M / Genesis Bond Credits)". Update the "Exit Path" cell to: "unbond() after 14-day unbonding window (governs both voluntary bond and any claimed-vested credit); unvested credit on exit returns to Treasury via the existing Treasury reference".
  - `OperatorEmissions` row: delete.
- **Access Control Matrix table:**
  - Row with `BOND_GRANTOR_ROLE | CapacityBond | depositGrant(operator, amount) | OperatorEmissions | OperatorEmissions; granted post-deploy ...`: replace with `GENESIS_GRANTOR_ROLE | CapacityBond | grantGenesisCredit(operator, amount) | Treasury | Treasury; granted post-deploy as a one-shot, time-boxed authorization scoped to the GENESIS_CREDIT_WINDOW (default 30 days); auto-revokes at window close per ADR 026 §Genesis Bond Credits`.
  - Row with `GOVERNANCE_ROLE | ... OperatorEmissions | ... setEmissionCurve, sunsetBucket (OperatorEmissions) ...`: remove the OperatorEmissions parts; do not add a replacement for `setEmissionCurve`/`sunsetBucket` (no emission curve exists under v2.2).
- **Reentrancy Analysis:**
  - `#### OperatorEmissions` subsection: delete entirely (the `distribute(epoch)` analysis).
  - `#### CapacityBond` subsection's `depositGrant` row: replace with three rows for the new entrypoints:

    ```markdown
    | `grantGenesisCredit(operator, amount)` | `IERC20.safeTransferFrom(treasury, this, amount)` (one external call to TOKEN — trusted IERC20) | `nonReentrant`, checks-effects-interactions, `GENESIS_GRANTOR_ROLE` (held by Treasury), TGE-window guard |
    | `accrueGenesisVest(operator)` | None (state change only; reads CapacityBond own state) | View-style internal accrual; safe to call from any tx |
    | `claimVestedCredit(operator)` | None (internal accounting promotion; no external call) | `nonReentrant` (defensive; no external call but kept for invariant) |
    ```

- **OpenZeppelin Framework Usage table** (`| AccessControl | CapacityBond, OperatorEmissions, ... |`): remove `OperatorEmissions`.
- **Launch vs Steady-State Configuration table** (the `| OperatorEmissions.distribute | ... |` row): delete the row. No replacement needed — there is no v2.2 schedule for emissions because there are no emissions.
- **§ Consequences:**
  - The bullet starting `- v2.1 net-subtractive: \`VotingEscrow\` and \`DelegatorBuyer\` are deleted; \`FeeRouter\` simplifies ... one new contract (\`OperatorEmissions\`); ...` — replace with: `v2.2 net-subtractive vs both prior designs: VotingEscrow and DelegatorBuyer remain deleted; FeeRouter simplifies from six buckets to four with no epoch / claim / snapshot machinery; OperatorEmissions (v2.1's new contract) is deleted; StakingRegistry is renamed CapacityBond with the capacity-curve logic and the PendingCredit vesting extension. Net new contracts vs v1 is zero.`
  - The bullet that lists "three fund-holding contracts (CapacityBond, SafetyReserve, OperatorEmissions)": change the count from three to two and drop OperatorEmissions from the parenthesized list, leaving "two fund-holding contracts (CapacityBond, SafetyReserve)".
- **References block at the bottom of ADR 016** (the line citing `ADR 026 — Tokenomics`): replace `(v2.1 work-token rewrite)` with `(v2.2 no-emission rewrite)`.

- [ ] **Step 3: Verify no stale references remain**

```bash
grep -n "OperatorEmissions\|BOND_GRANTOR\|depositGrant\|EmissionCurve\|setEmissionCurve\|sunsetBucket" decdn/adr/016-contract-interactions.md
```

Expected: zero matches, or only a "v2.1's OperatorEmissions (deleted under v2.2)" historical aside if you've kept one.

- [ ] **Step 4: Run pre-commit and commit**

```bash
pre-commit run --files decdn/adr/016-contract-interactions.md
git add decdn/adr/016-contract-interactions.md
git commit -m "$(cat <<'EOF'
docs(adr): ADR 016 — sweep OperatorEmissions from fund flow, access control, reentrancy

Final slice of the v2.2 rewrite for ADR 016. Updates TOKEN Flow Mermaid,
Contracts Holding Funds Summary, Access Control Matrix (BOND_GRANTOR_ROLE
becomes GENESIS_GRANTOR_ROLE on Treasury, time-scoped to TGE window),
Reentrancy Analysis (CapacityBond depositGrant entrypoint replaced by
grantGenesisCredit / accrueGenesisVest / claimVestedCredit), OpenZeppelin
usage table, Launch vs Steady-State table, and §Consequences narrative.

Net new contracts vs v1 is zero. Two fund-holding contracts under v2.2:
CapacityBond (now also holds the 50M Genesis Bond Credit allocation) and
SafetyReserve.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## PR-adr · Phase 4: ADR 028 + decdn/CLAUDE.md

### Task 9: ADR 028 — update Operator-Emissions reference + add unvested-credit slashing note

**Files:**

- Modify: `decdn/adr/028-slashing-appeals.md:183` (the reference to `OperatorEmissions` distribution in the reputation-hit-persists note)
- Modify: `decdn/adr/028-slashing-appeals.md` — add a short note in the relevant slashing-mechanics section about how the pending unvested portion of Genesis Bond Credits is slashable

- [ ] **Step 1: Locate the existing reference**

```bash
grep -n "OperatorEmissions\|Operator Service Emission\|pendingCredit\|Genesis Bond Credit" decdn/adr/028-slashing-appeals.md
```

- [ ] **Step 2: Replace the reference text**

The reference text in the current ADR 028 reads:

```markdown
The hit lowers selection probability ([ADR 001 node selection](001-network.md#node-selection-algorithm)), reducing real `bytes_delivered` and therefore the operator's share of the `OperatorEmissions` distribution per [ADR 026 § Operator Service Emissions](026-tokenomics.md#operator-service-emissions). For high-volume operators this can be a far larger economic loss than the restituted slash.
```

Replace with:

```markdown
The hit lowers selection probability ([ADR 001 node selection](001-network.md#node-selection-algorithm)), reducing real `bytes_delivered` and therefore the operator's USDC fee revenue under the four-bucket FeeRouter split per [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split). For high-volume operators this can be a far larger economic loss than the restituted slash.
```

- [ ] **Step 3: Add the unvested-credit slashing note**

Locate the §Scope or §Mechanics section of ADR 028 that lists what is slashable. Append a short subsection or paragraph:

```markdown
**Genesis Bond Credits (v2.2).** A slashed operator's pending unvested Genesis Bond Credit (`pendingCredit[op].total - pendingCredit[op].vested` on `CapacityBond` per [ADR 026 § Genesis Bond Credits](026-tokenomics.md#genesis-bond-credits)) is subject to the same slashing rates and distribution as voluntarily-bonded TOKEN. The vested-but-unclaimed portion is no longer separately slashable — it has functionally become voluntarily-bonded TOKEN and is slashed as part of `bondedAmount`. The slashing primitive in `CapacityBond._slash` iterates both pools atomically; no separate appeal path applies. Appeals flow is unchanged regardless of which pool the slash originated from.
```

- [ ] **Step 4: Run pre-commit and commit**

```bash
pre-commit run --files decdn/adr/028-slashing-appeals.md
git add decdn/adr/028-slashing-appeals.md
git commit -m "$(cat <<'EOF'
docs(adr): ADR 028 — drop OperatorEmissions reference; note PendingCredit slashability

Update the reputation-hit-persists rationale to cite FeeRouter USDC
revenue rather than the (deleted) OperatorEmissions distribution.

Add an explicit note that under v2.2, an operator's pending unvested
Genesis Bond Credit is slashable on the same terms as voluntarily-bonded
TOKEN; vested-but-unclaimed credit is no longer separately slashable
because it has functionally become voluntary bond. Appeals flow
unchanged.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 10: decdn/CLAUDE.md — update the ADR note

**Files:**

- Modify: `decdn/CLAUDE.md` (the "ADR note" paragraph in the Project Overview section)

- [ ] **Step 1: Locate the ADR note paragraph**

```bash
grep -n "ADR note\|026 (tokenomics) was superseded\|substantially rewritten under the v2.1" decdn/CLAUDE.md
```

- [ ] **Step 2: Update the canonical-status statement**

Find the substring:

```markdown
004 (tokenomics) was superseded by ADR 026, which was substantially rewritten under the v2.1 work-token spec at `docs/superpowers/specs/2026-05-24-work-token-tokenomics-redesign-v2.1.md`
```

Replace with:

```markdown
004 (tokenomics) was superseded by ADR 026, which was rewritten twice — first under the v2.1 work-token spec at `docs/superpowers/specs/2026-05-24-work-token-tokenomics-redesign-v2.1.md` (now Superseded) and again under the v2.2 no-emission spec at `docs/superpowers/specs/2026-05-27-remove-operator-emissions-design.md` (canonical). The v2.2 rewrite eliminated the OperatorEmissions contract and added Genesis Bond Credits + App Incentives mechanisms
```

Also: if there's a `Next ADR number is 036` line, no change needed (no new ADR was created in this rewrite).

- [ ] **Step 3: Run pre-commit and commit**

```bash
pre-commit run --files decdn/CLAUDE.md
git add decdn/CLAUDE.md
git commit -m "$(cat <<'EOF'
docs(claude-md): update ADR note to mark ADR 026 v2.2 canonical

Note that ADR 026 has been rewritten twice — first under v2.1 (now
Superseded) and again under v2.2 (canonical) — so future ADR work
references the right canonical state of the tokenomics document.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 11: Open PR-adr

- [ ] **Step 1: Confirm the branch state**

```bash
git status
git log --oneline origin/main..HEAD
```

Expected: clean working tree; 10 commits ahead of main (tasks 1–10).

- [ ] **Step 2: Push the branch and open the PR**

```bash
git push -u origin docs/work-token-tokenomics-spec
gh pr create --title "docs(adr): land v2.2 no-emission tokenomics — rewrite ADR 026, 016, 028" --body "$(cat <<'EOF'
## Summary

- Rewrites ADR 026 to remove the 20% Operator Service Emissions bucket and replace it with Genesis Bond Credits (5%, testnet-conditional, 24mo vest in CapacityBond) and App Incentives (14%, demand-side: publisher rebates + integration grants).
- Rewrites ADR 016 to remove the OperatorEmissions contract everywhere (Inventory, class diagram, deployment order, call graph, fund flow, access control, reentrancy, consequences) and extend CapacityBond with the PendingCredit vesting interface.
- Minor edit to ADR 028 to point at FeeRouter USDC revenue instead of OperatorEmissions, and to note that unvested PendingCredit is slashable.
- Updates `decdn/CLAUDE.md` ADR note to mark v2.2 as canonical and v2.1 as Superseded.
- Downgrades the v2.1 spec status to Superseded (still retained as historical record).

Driven by the v2.2 design spec at `decdn/docs/superpowers/specs/2026-05-27-remove-operator-emissions-design.md` (commit dd02d45).

## Test plan

- [ ] All pre-commit hooks pass on every commit (markdownlint, adr reference hygiene, etc.)
- [ ] No remaining references to OperatorEmissions, Operator Service Emissions, BOND_GRANTOR_ROLE, depositGrant, EmissionCurve, setEmissionCurve, sunsetBucket in `decdn/adr/026-tokenomics.md` or `decdn/adr/016-contract-interactions.md` (verify with grep)
- [ ] All anchor links in the new sections (`#genesis-bond-credits`, `#app-incentives`) resolve
- [ ] Distribution table sums to 100; Internal+External rollup sums to 100; Categorical rollup sums to 100
- [ ] Review the ADR 026 §Operator economics worked example for arithmetic consistency

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## PR-params · Phase 5: params.py rebase and notebook re-runs

### Task 12: params.py — replace SUPPLY_ALLOCATION with the v2.2 11-group table

**Files:**

- Modify: `finance/notebooks/_shared/params.py:40-72` (the `SupplyBucket` NamedTuple class and the `SUPPLY_ALLOCATION` tuple/list)

- [ ] **Step 1: Read the current SUPPLY_ALLOCATION**

```bash
sed -n '38,75p' finance/notebooks/_shared/params.py
```

- [ ] **Step 2: Replace the SUPPLY_ALLOCATION block with the v2.2 11-group table**

Replace the existing `SupplyBucket` declarations (the entries inside `SUPPLY_ALLOCATION = (...)` or `[...]`) with the 11-group table. Field shape: `SupplyBucket(name, fraction, cliff_months, linear_months)`.

```python
# v2.2 11-group distribution per ADR 026 (canonical) and
# docs/superpowers/specs/2026-05-27-remove-operator-emissions-design.md.
# Fractions sum to 1.0. Replaces the v2.1 13-group table; removes
# Operator Service Emissions, Liquidity Mining, Airdrops, and
# Incentivized Testnet as standalone groups.
SUPPLY_ALLOCATION: tuple[SupplyBucket, ...] = (
    SupplyBucket("Core Contributors",          0.15, 12, 48),
    SupplyBucket("DAO Treasury",               0.15,  0, 48),   # see Treasury sub-allocation below
    SupplyBucket("Protocol Owned Liquidity",   0.15,  0,  0),   # POL position, not freely floating
    SupplyBucket("App Incentives",             0.14,  0, 48),   # 4-year linear to multisig
    SupplyBucket("Seed Investors",             0.11,  6, 36),
    SupplyBucket("Private Investors",          0.09,  6, 36),
    SupplyBucket("Market Making",              0.05,  0,  0),   # genesis-liquid to MM partner
    SupplyBucket("Misc. Marketing, PR, KOLs",  0.05,  0,  0),
    SupplyBucket("Public Sale",                0.05,  0,  0),   # External type under v2.2
    SupplyBucket("Advisors",                   0.03,  6, 24),
    SupplyBucket("Exchange Partnerships",      0.03,  0,  0),   # milestone-based
)
```

- [ ] **Step 3: Verify the new table sums to 1.0**

```bash
python -c "
import sys; sys.path.insert(0, 'finance/notebooks/_shared')
from params import SUPPLY_ALLOCATION
total = sum(b.fraction for b in SUPPLY_ALLOCATION)
print(f'sum = {total:.4f}')
assert abs(total - 1.0) < 1e-9, f'expected 1.0, got {total}'
print(f'group count = {len(SUPPLY_ALLOCATION)}')
"
```

Expected output:

```
sum = 1.0000
group count = 11
```

- [ ] **Step 4: Stage but do not commit yet** — Task 13 also touches this file.

### Task 13: params.py — add Treasury sub-allocation, replace OperatorEmissionsParams with GenesisBondCreditParams, add AppIncentivesParams

**Files:**

- Modify: `finance/notebooks/_shared/params.py:360-410` (the `OperatorEmissionsParams` dataclass)
- Modify: `finance/notebooks/_shared/params.py` — add a `TreasurySubAllocationParams` dataclass near the top of the params section
- Modify: `finance/notebooks/_shared/params.py` — add a `GenesisBondCreditParams` dataclass replacing OperatorEmissionsParams
- Modify: `finance/notebooks/_shared/params.py` — add an `AppIncentivesParams` dataclass

- [ ] **Step 1: Delete the `OperatorEmissionsParams` dataclass**

Find `@dataclass(frozen=True)` immediately followed by `class OperatorEmissionsParams:` (around line 360–410). Delete the entire class block.

- [ ] **Step 2: Add `TreasurySubAllocationParams`**

In the same region (just below the `SUPPLY_ALLOCATION` block is a natural location), add:

```python
@dataclass(frozen=True)
class TreasurySubAllocationParams:
    """ADR 026 § Genesis Bond Credits — Treasury Group 2 sub-allocation
    at TGE.

    The 15% / 150M TOKEN DAO Treasury bucket splits on-chain at TGE:
      - genesis_bond_credit_pp: 5pp / 50M TOKEN earmarked for Genesis
        Bond Credits to verified testnet operators. Transferred into
        CapacityBond at TGE via batched grantGenesisCredit calls;
        never enters the operational Treasury wallet.
      - operational_pp: 10pp / 100M TOKEN to the Timelock-controlled
        operational Treasury wallet (USDC subsidies, App Incentives
        multisig payouts, discretionary grants).

    Reference: ADR 026 §Genesis Bond Credits and v2.2 spec §1.
    """
    treasury_total_pp: float = 0.15
    genesis_bond_credit_pp: float = 0.05
    operational_pp: float = 0.10

    def __post_init__(self) -> None:
        if abs(self.genesis_bond_credit_pp + self.operational_pp - self.treasury_total_pp) > 1e-9:
            raise ValueError(
                f"Treasury sub-allocation must sum to {self.treasury_total_pp}, "
                f"got {self.genesis_bond_credit_pp + self.operational_pp}"
            )


TREASURY_SUB_ALLOCATION = TreasurySubAllocationParams()
```

- [ ] **Step 3: Add `GenesisBondCreditParams`**

```python
@dataclass(frozen=True)
class GenesisBondCreditParams:
    """ADR 026 §Genesis Bond Credits — bounded, one-shot, retroactive
    grant to pre-launch incentivized-testnet operators.

    Replaces v2.1's OperatorEmissionsParams. The credit is:
      - sized at 50M TOKEN (5% of 1B supply, carved from Group 2 Treasury)
      - granted only to testnet participants, weighted by measured
        contribution (bytes × uptime × probe_success); exact weighting
        is deferred per ADR 026 §Deferred & Open
      - distributed at TGE via CapacityBond.grantGenesisCredit (one-shot,
        permanently disabled after grant_window_days post-deploy)
      - auto-deposited into CapacityBond as PendingCredit (never
        enters operator wallet at any point before vest)
      - vested linearly over 24 months, conditional on continued
        operation (`isActive(op) && !isSlashed(op)` per epoch)
      - slashable on the same terms as voluntary bond while unvested
      - returned to operational Treasury on voluntary unbond before vest

    Public-facing fundraising materials may continue to use the
    "Staking Rewards" label for tool compatibility, but the protocol-
    level and term-sheet name is "Genesis Bond Credits".

    Reference: ADR 026 §Genesis Bond Credits.
    """
    total_supply_share: float = 0.05               # 5% of supply
    total_tokens: int = 50_000_000                 # 50M TOKEN
    vesting_months: int = 24                       # linear vest
    grant_window_days: int = 30                    # post-deploy TGE window
    treasury_carve_pp: float = 0.05                # carved from Group 2 Treasury (15%)


GENESIS_BOND_CREDIT = GenesisBondCreditParams()
```

- [ ] **Step 4: Add `AppIncentivesParams`**

```python
@dataclass(frozen=True)
class AppIncentivesParams:
    """ADR 026 §App Incentives — 14% / 140M TOKEN demand-side
    customer-acquisition program.

    Two sub-programs run by Treasury multisig (no on-chain contract
    at launch):
      - Publisher Rebates (~10pp / 100M TOKEN indicative): quarterly
        TOKEN rebate proportional to publishers' USDC FeeRouter
        contribution; analogous to airline frequent-flyer miles or
        AWS cloud credits.
      - Integration Grants (~4pp / 40M TOKEN indicative): milestone-
        based lump-sum grants to projects integrating deCDN as their
        CDN backend (CMS plugins, framework adapters, hosting,
        non-Rust SDKs).

    The 10/4 split is a governance norm, rebalanceable within the
    14% envelope. No on-chain enforcement of the sub-program split.

    Reference: ADR 026 §App Incentives.
    """
    total_supply_share: float = 0.14               # 14% of supply
    total_tokens: int = 140_000_000                # 140M TOKEN
    unlock_period_months: int = 48                 # 4-year linear unlock to multisig
    publisher_rebate_pp_indicative: float = 0.10   # ~10pp of 14%
    integration_grant_pp_indicative: float = 0.04  # ~4pp of 14%
    indicative_quarterly_rebate_budget_tokens: int = 6_250_000  # (140M × 10/14) / 16 quarters


APP_INCENTIVES = AppIncentivesParams()
```

- [ ] **Step 5: Update any docstrings referencing the deleted class**

The `TokenomicsParams` docstring (around line 75-81) mentions `OperatorEmissionsParams`. Replace `emissions in OperatorEmissionsParams; bootstrap in BootstrapParams.` with `Genesis Bond Credits in GenesisBondCreditParams; App Incentives in AppIncentivesParams; bootstrap in BootstrapParams.`

The `BootstrapParams` docstring (around line 795-810) mentions `OperatorEmissionsParams (Group 7, ...)`. Replace with `GenesisBondCreditParams (Group 2 sub-allocation, 5% / 50M TOKEN one-shot retroactive grant)`. The bootstrap-runway commentary in the docstring may also need a tweak — the year-1-2 narrative is now a one-shot grant rather than a multi-year emission curve. Adjust to match.

- [ ] **Step 6: Verify imports and class references throughout params.py**

```bash
grep -n "OperatorEmissionsParams\|OPERATOR_EMISSIONS\b" finance/notebooks/_shared/params.py
```

Expected: zero matches. If any remain (e.g., in a `_warn_retired_shim` helper or in the `__all__` export list), update them. If `OperatorEmissionsParams` is still re-exported via `_warn_retired_shim`, replace with a shim pointing at `GenesisBondCreditParams`:

```python
# Backwards-compat shim — removed by ADR 026 v2.2. Notebooks that still
# import OperatorEmissionsParams get a clear deprecation pointer to the
# replacement.
def OperatorEmissionsParams(*args, **kwargs):  # noqa: N802
    _warn_retired_shim("OperatorEmissionsParams", "GenesisBondCreditParams")
    return GenesisBondCreditParams(*args, **kwargs) if not args and not kwargs else GenesisBondCreditParams()
```

If `_warn_retired_shim` does not already exist as a helper, skip this step — just delete the symbol cleanly. Check with:

```bash
grep -n "_warn_retired_shim\|def _warn_retired_shim" finance/notebooks/_shared/params.py
```

- [ ] **Step 7: Verify params.py imports and runs cleanly**

```bash
python -c "
import sys; sys.path.insert(0, 'finance/notebooks/_shared')
import params
print('OK')
print(f'GenesisBondCreditParams: {params.GENESIS_BOND_CREDIT}')
print(f'AppIncentivesParams: {params.APP_INCENTIVES}')
print(f'TreasurySubAllocation: {params.TREASURY_SUB_ALLOCATION}')
"
```

Expected: `OK` plus the three param dataclass dumps. No exceptions.

- [ ] **Step 8: Commit params.py changes**

```bash
git add finance/notebooks/_shared/params.py
git commit -m "$(cat <<'EOF'
finance(params): rebase params.py to ADR 026 v2.2 — Genesis Bond Credits + App Incentives

Replace the v2.1 13-group SUPPLY_ALLOCATION with the v2.2 11-group
table. Delete OperatorEmissionsParams. Add three new dataclasses:

  - TreasurySubAllocationParams: 15% Group 2 splits at TGE into
    5pp Genesis Bond Credits + 10pp operational Treasury.
  - GenesisBondCreditParams: 50M TOKEN one-shot retroactive grant
    to verified testnet operators, auto-bonded into CapacityBond,
    24mo vest via continued operation.
  - AppIncentivesParams: 140M TOKEN demand-side program (publisher
    rebates + integration grants), Treasury-multisig administered.

Update TokenomicsParams and BootstrapParams docstrings to point at
the new dataclasses. Backwards-compat shim provided for
OperatorEmissionsParams imports where _warn_retired_shim exists.

Driven by ADR 026 §Genesis Bond Credits and §App Incentives (v2.2).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 14: Re-run affected notebooks

**Files:**

- Modify: `finance/notebooks/01_node_economics.ipynb` (re-run)
- Modify: `finance/notebooks/02_bootstrap_runway.ipynb` (re-run; expect markdown narrative tweaks)
- Modify: `finance/notebooks/04_token_flows.ipynb` (re-run)

- [ ] **Step 1: Locate notebooks that reference the deleted symbols**

```bash
grep -l "OperatorEmissions\|operator_emission\|service_emission\|Staking Reward\|airdrop\|testnet_reward\|OPERATOR_EMISSIONS" finance/notebooks/*.ipynb
```

This will list any notebooks that import or reference the removed symbols. Expected: at least `01_node_economics.ipynb`, `02_bootstrap_runway.ipynb`, `04_token_flows.ipynb`. There may be more — handle each.

- [ ] **Step 2: For each affected notebook, open and update**

For each `.ipynb` listed in Step 1:

a. Open the notebook (e.g., `jupyter lab finance/notebooks/01_node_economics.ipynb` or edit via `jupyter nbconvert --to script` round-trip).

b. Locate cells that import `OperatorEmissionsParams` — replace with `GenesisBondCreditParams` (or remove the import if the notebook does not need it).

c. Locate cells that reference `OperatorEmissionsParams` instance fields (e.g., `op_emissions.curve(...)`, `op_emissions.front_load_factor`). These need narrative updates because v2.2 has no emission curve. Common rewrites:

- "Operator earns ~X TOKEN/month from emissions" → "Operator receives ~X TOKEN as Genesis Bond Credit at TGE (testnet-eligible only); vests linearly over 24mo".
- Year-1 revenue charts that include an emission curve → remove the emission series, add a single one-time grant for the testnet cohort.
- Bootstrap-runway charts → narrative changes more dramatically; year-1 and year-2 are now a one-shot grant + USDC fees only.

d. Re-run all cells.

e. Save the notebook.

- [ ] **Step 3: Verify no stale references remain**

```bash
grep -l "OperatorEmissions\|operator_emission\|service_emission\|Staking Reward\|OPERATOR_EMISSIONS" finance/notebooks/*.ipynb
```

Expected: zero matches.

- [ ] **Step 4: Commit notebook updates**

```bash
git add finance/notebooks/01_node_economics.ipynb finance/notebooks/02_bootstrap_runway.ipynb finance/notebooks/04_token_flows.ipynb
# add any additional notebooks listed in Step 1
git commit -m "$(cat <<'EOF'
finance(notebooks): re-run for ADR 026 v2.2 (no emissions; Genesis Bond Credits)

Update each affected notebook to import GenesisBondCreditParams in
place of OperatorEmissionsParams. Rebuild any operator-emission charts
into Genesis-Credit charts (one-shot at TGE, 24mo linear vest, testnet-
eligible cohort only). Bootstrap-runway narrative reshaped: year-1/year-2
TOKEN side is a one-shot grant rather than a multi-year emission curve.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 15: Open PR-params

- [ ] **Step 1: Confirm branch state**

```bash
git status
git log --oneline origin/main..HEAD
```

- [ ] **Step 2: Push and open PR**

The branch is the same as PR-adr's. If PR-adr is already merged, branch off main with a new branch first:

```bash
git checkout -b finance/params-v2.2-rebase main
# cherry-pick the Tasks 12–14 commits if needed
git push -u origin finance/params-v2.2-rebase
gh pr create --title "finance(params): rebase params.py and notebooks to ADR 026 v2.2" --body "$(cat <<'EOF'
## Summary

- Rebases `finance/notebooks/_shared/params.py` to the v2.2 11-group SUPPLY_ALLOCATION.
- Deletes `OperatorEmissionsParams`; adds `TreasurySubAllocationParams`, `GenesisBondCreditParams`, `AppIncentivesParams`.
- Re-runs affected notebooks (01_node_economics, 02_bootstrap_runway, 04_token_flows, and any others touching the deleted symbols).

Driven by the ADR 026 v2.2 rewrite (merged in PR-adr).

## Test plan

- [ ] `python -c "from finance.notebooks._shared.params import *; print('OK')"` runs without error
- [ ] `SUPPLY_ALLOCATION` sums to 1.0; group count is 11
- [ ] `TreasurySubAllocationParams.__post_init__` invariant holds (5+10=15)
- [ ] No notebook contains stale imports of `OperatorEmissionsParams` or references to the deleted symbols
- [ ] Notebooks render without errors; charts for year-1 economics reflect one-shot grant + vest, not multi-year emission

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review Checklist

After implementing all tasks, verify:

- [ ] **Spec coverage:** Every section of `decdn/docs/superpowers/specs/2026-05-27-remove-operator-emissions-design.md` is implemented:
  - Section 1 (Distribution table) → Task 2
  - Section 2 (Genesis Bond Credits) → Tasks 3, 6, 13
  - Section 3 (App Incentives) → Tasks 3, 13
  - Section 4 (Contract surface) → Tasks 5–8
  - Section 5 (Operator bootstrap narrative) → Task 4
  - Section 6 (Regulatory posture) → Task 3 (the §Howey framing paragraphs land inside §Genesis Bond Credits and §App Incentives)
  - Section 7 (ADR/spec impact + sequencing) → Tasks 1, 9, 10, 11
  - Section 8 (Open questions) → Task 4 (relocated to ADR 026 §Deferred & Open); Tasks 13–14 leave the testnet-weighting formula deferred
- [ ] **No remaining stale references:** `grep -rn "OperatorEmissions\|Operator Service Emission\|setEmissionCurve\|sunsetBucket\|BOND_GRANTOR\|depositGrant" decdn/adr/ decdn/CLAUDE.md decdn/docs/superpowers/specs/2026-05-27-remove-operator-emissions-design.md finance/notebooks/` returns zero matches (or only historical "v2.1's OperatorEmissions" comparison asides).
- [ ] **Anchor consistency:** Every `#genesis-bond-credits` and `#app-incentives` link inside `decdn/adr/026-tokenomics.md` resolves to a real section heading.
- [ ] **Distribution arithmetic:**
  - Table: `15+15+15+14+11+9+5+5+5+3+3 = 100` ✓
  - Internal: `15+15+11+9+5+3 = 58` ✓
  - External: `15+14+5+5+3 = 42` ✓
  - Categorical: `18+20+15+5+14+8+20 = 100` ✓
  - Treasury sub-allocation: `5+10 = 15` ✓
- [ ] **`SUPPLY_ALLOCATION.sum == 1.0`** verified via the Python check in Task 12.
- [ ] **Pre-commit hooks pass on every commit** — no `--no-verify` bypasses anywhere.

If any item fails, fix in a follow-up commit on the same branch. Do not amend; the existing CI/review flow assumes immutable commits.

---

## Open Risks and Mitigations

1. **Pre-commit `adr reference hygiene` hook may flag broken anchors mid-rewrite.** Tasks 2–4 split ADR 026 into three commits; the first commit references anchors created by the second. If the hook fails on intermediate states, either (a) reorder so anchors are created before being referenced, or (b) land Tasks 2–4 as a single commit. The plan documents option (b) in Task 2 Step 4.

2. **Notebook re-runs may surface deeper economic-modeling assumptions baked into v2.1's emission curve.** If a notebook's bootstrap-runway model breaks because it assumes a multi-year emission stream, document the broken cell explicitly and either (a) rebuild the model around the one-shot grant + vest, or (b) mark the cell as deferred-rework in the notebook's lead-in markdown and skip its execution. Do not silently hide failures.

3. **Workspace-wide SoT sweep is out of scope.** The CLAUDE.md SoT rule requires downstream materializations (website, internal Fundraising/Legal docs, .github/profile/README) to be updated after the ADR change. This plan does not include that sweep — it's a follow-up workspace-level task once both PRs are merged. Flag this in the PR-adr description so reviewers know.

4. **Solidity implementation does not yet exist.** All `CapacityBond` interface changes in ADR 016 are documentation-only; the actual `contracts/` directory is empty. When the real `CapacityBond.sol` is implemented (out of scope for this plan), the v2.2 interface from ADR 016 is the source of truth.
