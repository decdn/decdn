# ADR 009: Governance Model

**Date:** 2026-05-27
**Status:** Draft

## Context

[ADR 026](026-tokenomics.md#adr-026-tokenomics) defines a work-token tokenomics model. The governance-relevant primitives are: (i) a single `CapacityBond` contract that holds operator bonds proportional to declared bandwidth (`bond = k × Mbps^α`), exposes `firstBondedAt(operator)` as the source of the `age_ramp` tenure factor, and exposes `slashedAtEpoch(operator)` for the slash-aware voting-weight zero-out; (ii) a three-bucket `FeeRouter` whose share parameters are governable within hard-coded bounds (no epoch buckets, no claim windows) and whose per-operator `bytesPerEpoch` accounting is the source of served-bytes voting weight per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight); (iii) a `SlashAppeal` contract whose grant/uphold decisions on escrowed slashes are governance- and emergency-multisig-gated per [ADR 028](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation).

Governance — how protocol parameters are changed, who can change them, and what safety mechanisms exist — is a separate concern. The governance contracts (`DecdnGovernor`, `TimelockController`) ship in the day-one single-audit-pass surface ([ADR 016 § Contract Inventory](016-contract-interactions.md#contract-inventory)); what differs by phase is the governance *process*, not the contract surface. In the PoC, parameters are changed through a single admin key. At launch, a bootstrap multisig replaces the admin key while the operator set is too thin for operator-weighted DAO voting (served-bytes-weighted per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)) to be safe. Once the operator set reaches the transition thresholds, full operator-weighted DAO governance activates.

This ADR covers:

1. The PoC governance model (admin key)
2. The bootstrap-multisig phase (first 6–12 months post-launch, until operator set is large enough)
3. The production governance model (OpenZeppelin Governor against `CapacityBond`)
4. Governable parameters and their hardcoded safety bounds (including the three `FeeRouter` shares and `CapacityBond` curve parameters)
5. Emergency multisig design
6. Slash-appeal authorization rules (`SlashAppeal`)

## Decision

### PoC: Admin Key

A single deployer address (EOA or multisig) has admin rights on all contracts. Can update any parameter. No voting, no timelock.

### Bootstrap-multisig phase

Post-launch, the voting set is narrow (likely <50 active operators in the first 6–12 months). Direct application of capacity-weighted DAO voting pre-bootstrap risks hostile takeover via a cheap operator-fleet setup. The multisig replaces the PoC admin key in this phase and operates the protocol within the safety bounds in [§ Governable Parameters with Safety Bounds](#governable-parameters-with-safety-bounds).

**Composition.** 5-of-9 multisig (separate from the [emergency multisig](#emergency-multisig)) with geographically and organizationally diverse signers. Signer set publicly disclosed.

**Capabilities.** All parameter updates within the safety bounds; `FeeRouter.setShares` / dependency-address setters; `CapacityBond` parameter setters (α, k, MAX_CAPACITY_PER_OPERATOR, age_ramp_months, unbonding window); slash-appeal grant/uphold on `SlashAppeal` (per [§ Slash-Appeal Authorization](#slash-appeal-authorization)); standard 48-hour timelock on every parameter change.

**Transition thresholds.** The bootstrap-multisig phase ends when **active operator count ≥ 30** AND **total declared capacity ≥ 100 Gbps**. Both thresholds are governable within bounds (operator count `[10, 200]`, capacity `[10 Gbps, 1000 Gbps]`). At the transition, governance executes a one-shot setter that revokes the bootstrap-multisig's `GOVERNANCE_ROLE` across the role-gated contracts and transfers it to the `TimelockController` controlled by `DecdnGovernor` proposals. The Governor's vote-weight source (`FeeRouter.bytesInWindow` + `CapacityBond.firstBondedAt` + `CapacityBond.slashedAtEpoch` per [ADR 036 § Formula](036-served-bytes-voting-weight.md#formula)) is wired at deployment, not at transition. The transition setter cannot be reversed; the bootstrap-multisig cannot be reinstated.

**Duration.** ~6–12 months expected; the threshold-based gate makes this data-driven rather than calendar-driven.

### Production: Operator-Weighted DAO Governance

Based on OpenZeppelin Governor, sourcing voting weight from `FeeRouter` served-bytes accounting and `CapacityBond` tenure data per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight):

| Parameter | Value |
| --- | --- |
| Voting source | `FeeRouter.bytesInWindow(operator, epoch(ts), windowEpochs)` (served-bytes trailing-window sum), per-operator-capped, multiplied by `age_ramp` derived from `CapacityBond.firstBondedAt(operator)` — full formula in [ADR 036 § Formula](036-served-bytes-voting-weight.md#formula) |
| Voting weight | `min(served_bytes_window(op), voteCapBps × total_bytes_window) × age_ramp(op)` — zero at registration; ramps with served-bytes and tenure jointly; zeroed for `windowEpochs` epochs after any slash per [ADR 036 § Slashing zero-out](036-served-bytes-voting-weight.md#slashing-zero-out) |
| Voting delay | 1 day between proposal creation and the vote snapshot |
| Proposal threshold | 0.1% of total voting weight at proposal snapshot |
| Voting period | 7 days |
| Quorum | 4% of total voting weight at proposal snapshot |
| Timelock | 48 hours between vote passing and execution |
| Per-operator voting cap | 5% of total voting weight (now applied against the bytes-weighted total) |
| Vote delegation | EIP-712 signed delegation (Governor Bravo pattern) — voting power is delegable; the bond itself is not |

The clock is timestamp-based (ERC-6372 `mode=timestamp`) to align with `FeeRouter`'s epoch-keyed accounting (epoch length immutable, 1 week — see [ADR 016 § Contract: FeeRouter](016-contract-interactions.md#contract-feerouter)). Voting weight is computed from `FeeRouter`'s `bytesInWindow` / `totalBytesInWindow` helpers and `CapacityBond.firstBondedAt` / `slashedAtEpoch` directly, not through an `IVotes`/IERC-5805 surface — vote weight is derived from FeeRouter epoch accounting rather than from per-account checkpoint structures, so the Governor supplies the vote source, quorum, and per-operator cap as thin overrides rather than via OZ's `GovernorVotes` / `GovernorVotesQuorumFraction` modules.

Quorum and proposal threshold are calibrated against `FeeRouter.totalBytesInWindow(epoch(ts), windowEpochs)` — the unramped, uncapped sum of served bytes across all operators over the trailing window — **not** total TOKEN supply and **not** the per-operator-capped weighted total. The Governor uses the unramped total as a tractable upper bound for the strictly-correct capped-and-ramped sum (which would be O(active_operators × N) to compute); the resulting quorum / threshold is mildly conservative. Voting weight tracks active operator service delivery rather than passive holdings; calibrating against it avoids the failure mode where the quorum bar trivially exceeds engaged voting power as TOKEN circulates.

**Non-operator TOKEN holders carry zero voting weight.** Traders, passive holders, vesting recipients (Core Contributors, Advisors, Seed, Private, Treasury, Public Sale, Airdrop / Testnet, Marketing, LP/MM/POL), and any TOKEN not bonded into `CapacityBond` have no vote. Per [ADR 026 § Governance](026-tokenomics.md#governance), this is the work-token regulatory-cleanliness commitment: passive holding earns nothing — neither revenue nor governance privilege.

#### Non-operator holder protection

Non-operator value accrual is structurally guaranteed at the *contract* level, not at the governance level. Operators cannot vote to push the operator-base share above the 90% upper bound, drop burn below the 5% lower bound, or otherwise expropriate non-operator-aligned shares — see the immutable [§ Governable Parameters with Safety Bounds](#governable-parameters-with-safety-bounds) below. The cashflow invariant (40% floor on the operator base) ensures clients still receive paid delivery; the 5% floor on burn preserves the deflationary lever; the 0% floor on treasury permits governance to simplify the split without dropping operator-aligned cashflow.

#### Investor disposition (Open Q #7 resolved)

The entity design already excludes investors and other non-operator holders from DAO voting by structure — DAO governance is permissionless and no-KYC, with investor influence routed to the Labs (C-Corp) equity layer (Series A+ board seats, standard preferred-stock protective provisions, indirect TOKEN exposure via Labs' ~15% treasury allocation). Source: `internal/Legal/entity-structure-design.md` § Pattern A (Legal Fiction Separation).

Work-token does *not* narrow investor power *relative to that existing design*. DAO voting is restricted to operators — a change to who-among-active-participants votes, not a removal of an investor right that ever existed in entity design. Term-sheet language for Private Investors should not promise a ve-lock passive-governance path, because that was never a designed-in investor right.

Mitigation if any investor wants direct DAO signal: any TOKEN holder who *also* operates a node votes like any other operator. This path is open to investors, team, treasury, and seed equally.

### Governance permanence: no standing veto

The bootstrap → operator-DAO handoff is irreversible (see [Bootstrap-multisig phase](#bootstrap-multisig-phase)): once `GOVERNANCE_ROLE` moves to the `TimelockController`, no standing party can veto, reverse, or pre-empt an operator vote. This is a deliberate choice, not an omission — a permanent on-chain veto authority over governance is rejected.

A permanent veto would re-introduce a central controller, which weakens the work-token regulatory posture rather than protecting it. The *Howey* "efforts of others" prong and the "sufficient decentralization" argument for secondary trading both depend on no party holding ongoing managerial control over the protocol (per [ADR 026 § Governance](026-tokenomics.md#governance) and the entity design in `internal/Legal/entity-structure-design.md` § Pattern A). A standing veto is precisely the control that posture is built to avoid, so it cannot be the mechanism that makes the handoff legally workable.

Legal viability of the handoff rests on the off-chain legal wrapper, not on an on-chain controller. The governance constituency (bonded operators) votes on-chain; a legal entity (the deCDN Verein) executes those votes and carries the statutory duties. Its board is bound to implement any passing on-chain proposal **except where doing so would violate applicable law** — a narrow legal-compliance veto that lives at the execution layer, scoped to illegality, not a discretionary override of governance. Entity structure, board obligations, and the law-compliance carve-out are canonical in `internal/Legal/entity-structure-design.md` § Pattern A (Legal Fiction Separation).

The one capability retained on-chain in perpetuity is the **narrow unlawful-content-removal** power (see [Emergency Multisig](#emergency-multisig)): it discharges a permanent, time-critical legal duty, is scoped to blocking specific unlawful content, and touches no economic, treasury, or governance lever — so it is distinguishable from the managerial control the decentralization posture forbids. Final calibration of this compliance boundary is subject to the counsel review required before deployment (see [Consequences](#consequences)).

### Governable Parameters with Safety Bounds

All economic parameters across the protocol are governable within hardcoded safety bounds. Safety bounds are immutable — even a governance attack cannot set parameters outside these ranges.

#### FeeRouter shares (per [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds))

The three `FeeRouter` shares are governable within the bounds below. **Sum-to-100% invariant:** every governance update that modifies any share **must** leave the three shares summing to exactly 100% (10000 bps); updates that violate the sum or that exceed any individual bound revert at the contract layer.

| Parameter | Default | Min | Max |
| --- | ---: | ---: | ---: |
| Operator base share | 60% | 40% | 90% |
| Burn share | 30% | 5% | 50% |
| Treasury share | 10% | 0% | 30% |

The 40% floor on the operator-base share is the cashflow invariant: operators always receive enough liquid USDC to cover at least a meaningful fraction of infrastructure costs even under extreme governance proposals. The 5% floor on burn preserves the deflationary lever; the 0% floor on treasury lets governance simplify the split.

#### CapacityBond curve and governance parameters (per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve))

| Parameter | Default | Min | Max |
| --- | ---: | ---: | ---: |
| α (capacity-curve exponent) | 1.2 | 1.0 | 1.8 |
| k (capacity-curve constant, TOKEN) | 12.6 | bounded by 1G-tier bond ∈ [10K, 200K TOKEN] | — |
| `MAX_CAPACITY_PER_OPERATOR` | 200 Gbps | 50 Gbps | 1000 Gbps |
| `age_ramp_months` | 6 | 1 | 24 |
| Per-operator voting cap | 5% | 1% | 25% |
| `windowEpochs` (served-bytes trailing window, on `FeeRouter`) | 13 | 4 | 26 |
| Multisig-bootstrap transition: operator-count threshold | 30 | 10 | 200 |
| Multisig-bootstrap transition: capacity threshold | 100 Gbps | 10 Gbps | 1000 Gbps |
| Unbonding window | 14 days | 7 days | 60 days |

The α range upper-bounds at 1.8 to prevent a concentration penalty so steep that mid-tier operators are economically barred from upgrading; the lower bound at 1.0 ensures decentralization pressure is never fully disabled. k is parameterized via the 1G-tier bond range to constrain governance volatility — direct changes to k can shift the entire bond curve, so the bound is on the *resulting bond* rather than on k itself. The 5% per-operator voting cap (governable range `[1%, 25%]`) is the primary defense against bytes-weighted concentration per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight). The `windowEpochs` bounds are pinned in [ADR 036 § Governable parameters with safety bounds](036-served-bytes-voting-weight.md#governable-parameters-with-safety-bounds): below 4 epochs (~1 month) vote weight is too reactive to single-burst wash trading; above 26 epochs (~6 months) the trailing window lags actual operator-set composition and `DecdnGovernor._getVotes` cold-SLOAD gas rises to ~55K per voter per `castVote`.

#### Other protocol parameters

| Parameter | Contract | Min | Max |
| --- | --- | --- | --- |
| Slash percentage (per offense) | CapacityBond | 5% | 50% |
| Multiaddr update cooldown | CapacityBond | 0 (disabled) | 86400 seconds (1 day) |
| Max multiaddr size | CapacityBond | 64 bytes | 1024 bytes |
| Dispute window (default: 48h) | PaymentChannel | 12 hours | 72 hours (3 days) |
| Rate floor/ceiling | PaymentChannel | Floor ≥ 1 base unit | Ceiling > floor |
| Max voucher interval | PaymentChannel | 1 MB | 1024 MB (~1 GB) |
| Min deposit | PaymentChannel | 1 base unit | No max |
| Challenge bond | SlashJudge | 1 TOKEN | 1,000 TOKEN |
| Base slash reset period | CapacityBond | 30 days | 365 days |
| Compliance window | ContentBlacklist | 1 hour | 7 days |
| Assignment timelock | OriginAssignment | 24 hours | 14 days |
| Max origins per namespace | OriginAssignment | 1 | 50 |
| Default-open max origins | OriginAssignment | 20 | 500 |
| Max namespaces per publisher | PublisherRegistry | 1 | 1000 |
| Namespace transfer timelock | PublisherRegistry | 24 hours | 30 days |
| Max evidence age | SlashJudge | 1 day | 30 days |

There is no settlement-time fee skim or discount-stake mechanic on the channel contract. Burn is a fixed share of the `FeeRouter` split (governable within the burn-share bound above). There is no flat minimum-stake parameter; the only TOKEN-side requirement on operators is the capacity-bond curve.

The 7-day voting period balances responsiveness with participation. Combined with the 1-day voting delay and the 48-hour timelock, the total governance delay is ~10 days minimum — longer than the standard OpenZeppelin Governor defaults, reflecting operator-class participation cadence.

CapacityBond parameters are defined in [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve) and [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn); fee-routing parameters are governed by [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds). Payment channel parameters are defined in [ADR 003](003-payments.md#adr-003-payment-model). This ADR defines the governance mechanism that controls them.

**Treasury disbursement:** Spending from the protocol treasury wallet (the destination of the 10% `FeeRouter` treasury share per [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split)) — including operational expenditures, ecosystem grants, audits, any discretionary incident restitution (per [ADR 026 § Incident recourse](026-tokenomics.md#incident-recourse-no-standing-reserve)), and any onward transfers — requires a standard governance proposal in production. During the bootstrap-multisig phase, the multisig directs treasury spending within the safety bounds. The emergency multisig cannot withdraw treasury funds (see [Emergency Multisig](#emergency-multisig)); it has no fund-movement authority — its only fast-track power is deciding slash appeals on `SlashAppeal`, which moves only escrowed slash funds along the deterministic grant/uphold paths.

**Safety bound rationale:**

- **Slash 5%–50% per offense:** A 1% slash is economically negligible and provides no deterrence. A 100% single-offense slash enables governance to fully confiscate the bond, which is disproportionate. The 5%–50% range ensures each individual slash is meaningful but not existential. Full ejection (effectively 100% loss) is still possible through **cumulative** slashing per [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn).
- **Base slash reset period 30–365 days:** Bounds apply to the base reset period only; the hardcoded escalation multiplier (1×/2×/4×) on lifetime offense count is not governable, to prevent flattening the anti-gaming curve.
- **Max evidence age 1–30 days and Unbonding period 7–60 days:** The individual bounds permit a configuration where evidence age ≥ unbonding period, which would let an operator commit any of the three on-chain offenses ([ADR 014 § `Slashed` event and `slashId` allocation](014-on-chain-verification.md#slashed-event-and-slashid-allocation)), initiate unbonding, and complete withdrawal before the evidence window opens. **Cross-parameter invariant:** `SlashJudge` and `CapacityBond` enforce `MAX_EVIDENCE_AGE_US < unbondingPeriod * 1_000_000` at the contract layer on every `setMaxEvidenceAge` / `setUnbondingPeriod` call; updates that would violate this revert. The check applies at deployment as well. See [ADR 014 § Interaction with unbonding period](014-on-chain-verification.md#interaction-with-unbonding-period) for the exact revert conditions.
- **Rate floor ≥ 1 base unit:** A zero floor allows free-riding nodes that advertise zero rates to attract traffic without generating protocol fees. The minimum of 1 USDC base unit ($0.000001/MB for 6-decimal USDC) is negligibly small but prevents true zero-rate abuse.
- **Dispute window 12h–72h:** A 30-minute window is too short for fraud detectors to respond to a stale close. A 7-day window locks client funds for an unacceptably long period. PoC deploys at 48 hours to guarantee 24 hours of effective dispute response time under worst-case L2 sequencer censorship (forced-inclusion delay ≤ 24h). See [ADR 003](003-payments.md#adr-003-payment-model) and [Appendix: Fraud Detection](appendix-fraud-detection.md#appendix-permissionless-stale-close-detection).
- **Min deposit floor ≥ 1 base unit:** prevents dust channels that cost more in gas to settle than they contain.
- **Assignment timelock 24 hours–14 days:** Lower bound ensures publishers and the broader community have at least one full business day to surface concerns about a proposed origin set. Upper bound prevents governance from making assignments effectively unusable through delay.
- **Max origins per namespace 1–50:** Upper bound prevents storage-cost griefing and unbounded gas in `getOrigins` view calls.
- **Default-open max origins 20–500 (default 100):** The default-open allow-list authorizes operators to serve as origin for any unclaimed hash; the cap is correspondingly larger than the per-namespace cap to allow geographic and operator-class diversity.
- **Max namespaces per publisher 1–1000:** Anti-squatting limit.
- **Namespace transfer timelock 24 hours–30 days:** Lower bound prevents instant key-compromise transfers; upper bound prevents governance from blocking legitimate ownership changes.
- **α range 1.0–1.8 and k bounded by 1G-tier bond [10K, 200K TOKEN]:** The α floor at 1.0 (linear) prevents disabling decentralization pressure entirely. The α ceiling at 1.8 prevents a concentration penalty so steep that mid-tier operators face >5× per-Mbps capital cost vs entry-tier and effectively cannot upgrade. The k bound is parameterized via the 1G-tier bond to constrain governance volatility — k=12.6 gives ≈50K TOKEN at 1 Gbps; the [10K, 200K] range allows governance to halve or quadruple the entry-tier bond without rewriting the curve from scratch.
- **`age_ramp_months` 1–24:** The age-ramp defends against "buy your way to instant governance" attacks. 1 month is the operational floor; 24 months is high enough to materially delay hostile-fleet votes but low enough that legitimate new operators reach full weight in a reasonable time.
- **Per-operator voting cap 1%–25%:** The 1% floor prevents governance from making any single operator's vote vanishingly small (which would push the network toward de facto majority-of-cap voting); the 25% ceiling keeps any single operator's vote bounded. The cap is the primary defense against bytes-weighted concentration — real CDN traffic skews power-law, so the cap floor (1%) is the first tightening lever if observed concentration warrants per [ADR 036 § Threat Model — Concentration](036-served-bytes-voting-weight.md#concentration).
- **`windowEpochs` 4–26:** The trailing-window length over which served bytes are summed for voting weight. Below 4 epochs (~1 month) vote weight is too reactive to single-burst wash trading and statistically thin for small operators; above 26 epochs (~6 months) the trailing window lags actual operator-set composition (an operator who exited service ~5 months ago still carries half-weight) and `_getVotes` cold-SLOAD gas rises to ~55K per voter per `castVote`. Per [ADR 036 § Governable parameters with safety bounds](036-served-bytes-voting-weight.md#governable-parameters-with-safety-bounds).
- **Multisig transition thresholds:** Operator count `[10, 200]` and capacity `[10 Gbps, 1000 Gbps]` give governance flexibility to delay or advance the transition based on operator-set diversity that's not visible at deploy time.

### Slash-Appeal Authorization

There is no standing insurance reserve (the `SafetyReserve` contract was retired — see [ADR 026 § Incident recourse](026-tokenomics.md#incident-recourse-no-standing-reserve)). The only governance-gated incident surface is the **slash appeal**: a slashed operator's escrowed TOKEN is refunded (`grantAppeal`) or distributed 50/50 (`upholdAppeal`) by a two-stage decision. The flow lives in the `SlashAppeal` contract per [ADR 028](028-slashing-appeals.md#adr-028-slashing-appeals-and-dispute-escalation):

1. **Filing.** The slashed operator (and only the operator — `msg.sender` must equal the operator recorded in the slash) files `openSlashAppeal(slashId, evidenceBundleHash)` within the 30-day filing window, posting a TOKEN appeal bond. This locks the slash's escrow on `CapacityBond`. Filing is operator-restricted so a challenger cannot burn the operator's one-shot appeal slot with a junk filing — see [ADR 028 § Decision](028-slashing-appeals.md#decision).
2. **Multisig review.** The emergency multisig `fastTrackAppeal`s (grants interim relief) or `rejectAppeal`s within the 14-day review window. There are no USDC hard caps because no reserve funds are at risk — the only funds in play are the operator's own escrowed slash and the appeal bond.
3. **Governor ratification.** The Governor `grantAppeal`s (operator vindicated → escrow refunded + `slashedAtEpoch` cleared) or `upholdAppeal`s (slash stands → escrow 50% challenger / 50% burn) within the 14-day ratification window.
4. **Lapse handling.** A permissionless `cleanupExpiredAppeal` resolves appeals the multisig or Governor let lapse (review-window lapse → upheld; ratification-window lapse → operator-favorable grant).

The emergency multisig moves no protocol funds in this flow — escrow movement is performed deterministically by `CapacityBond`'s settle hooks; the multisig only decides fast-track-vs-reject.

### Emergency Multisig

- 3-of-5 multisig with known, trusted signers (separate from the bootstrap-governance multisig above)
- Capabilities (exhaustive list):
  1. **Pause contracts** — halt all contract execution for exploit response and critical bug mitigation
  2. **Emergency content blacklisting** — add hashes and origin operators to the `ContentBlacklist` contract via `emergencyAdd` and `emergencyAddOrigin` (see [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting))
  3. **Regional body suspension** — suspend a compromised regional governance body via `suspendRegionalBody` (see [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)); must be ratified or reversed by governance within 14 days. Sub-modes for the blacklist-entry-appeal flow are `ContentBlacklist.fastTrackAppeal(appealId)` / `unFastTrackAppeal(appealId)` / `rejectAppeal(appealId)` / `rejectAppealAsPerjury(appealId)` — see [ADR 011 § Blacklist Entry Appeals](011-content-takedown.md#blacklist-entry-appeals) and [ADR 031](031-content-blacklist-appeals-contract.md#adr-031-contentblacklist-appeal-contract-surface)
  4. **Slash-appeal fast-track** — `SlashAppeal.fastTrackAppeal(slashId)` (grants interim relief on an open appeal) and `SlashAppeal.rejectAppeal(slashId)` (denies an open appeal). These decide the appeal only; the escrow movement is performed deterministically by `CapacityBond` settle hooks. No fund-movement authority and no USDC hard caps (no reserve exists). See [Slash-Appeal Authorization](#slash-appeal-authorization) and [ADR 028 § Contract surface](028-slashing-appeals.md#contract-surface)
- Cannot change parameters, withdraw funds, or bypass governance for non-emergency actions
- Used for exploit response, critical bug mitigation, and time-critical content removal (e.g., CSAM, actively-exploited material)
- Sunset (capability-split): the emergency authority sunsets **by capability**, because some emergency powers discharge time-bound launch risk while others discharge permanent legal duties. The two are decoupled at the contract layer rather than sharing one deadline.
  - **Exploit-response pause sunsets hard.** `pauseDeadline = deployTimestamp + 365 days` is hardcoded in the constructor as an immutable value; after the deadline `pause()` reverts with `"PauseExpired"`. The pause is a protocol-wide brake — the most centralization-sensitive emergency power — so it is the capability that expires, for every caller including governance itself: there is no role carve-out on the deadline, so a timelock/governance `pause()` reverts identically once the window closes. Post-sunset exploit response routes through deploying a new contract version with a fresh deadline via the standard timelock + governance upgrade path. Governance cannot modify the immutable deadline, so the sunset cannot be silently extended.
  - **Unlawful-content removal does not sunset.** Emergency blacklisting (`emergencyAdd`, `emergencyAddOrigin`) and the regional-body suspension that backs it (`suspendRegionalBody`) discharge a permanent legal duty — removal of CSAM, court-ordered, sanctions-driven, and other unlawful content — that has no expiry and is time-critical: a removal order can carry a one-hour clock that the ~10-day governance cycle cannot meet. This authority is deliberately narrow — it can block specific unlawful hashes and origins and nothing else, with no parameter, treasury, fund-movement, or governance-veto power — so a permanent grant does not re-centralize protocol control. It remains subject to the blacklist-entry appeal process (see [ADR 011 § Blacklist Entry Appeals](011-content-takedown.md#blacklist-entry-appeals) and [ADR 031](031-content-blacklist-appeals-contract.md#adr-031-contentblacklist-appeal-contract-surface)). The legal entity that holds this authority and its appeal-override duties are specified in `internal/Legal/entity-structure-design.md` § Pattern A.
  - **Contract-layer enforcement.** Every pausable contract inherits a shared `SunsettingPausable` base that fixes `pauseDeadline = block.timestamp + 365 days` as an immutable at its own construction and reverts `PauseExpired` from `pause()` once that deadline passes. The unlawful-content-removal powers carry no such deadline: they live on the non-pausable `ContentBlacklist`, so the blacklist path and the pause path hold independent lifetimes by construction rather than by splitting a shared deadline.
- Signers should be geographically and organizationally diverse

## Consequences

### Positive

- Hardcoded safety bounds on all governable parameters limit the damage a governance attack can cause
- Emergency multisig provides rapid exploit response without giving any party unilateral control over funds or parameters
- The governance handoff is irreversible and carries no standing veto, keeping ongoing managerial control out of any single party's hands — the structural basis of the work-token regulatory posture. Legal-compliance authority is held off-chain by the legal wrapper's board and scoped to illegality, not a discretionary override of governance
- Capability-split sunset: the protocol-wide pause expires, while the only permanent on-chain emergency authority is the narrow unlawful-content-removal power — preventing the emergency role from becoming a standing controller while preserving a permanent, time-critical compliance duty that the ~10-day governance cycle cannot discharge
- Bootstrap-multisig phase prevents hostile takeover during the thin-operator-set launch window without locking the operator set out of governance forever — the threshold-based transition is data-driven, not calendar-driven
- Operator-only DAO voting structurally enforces the work-token regulatory posture (Howey prong 4); non-operator value accrual is preserved at the contract level via immutable share floors
- PoC can operate with a simple admin key; the governance contracts ship day-one in the single-audit-pass surface ([ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model))

### Negative

- Served-bytes-weighted governance shifts capture risk from large TOKEN holders to high-traffic operators; safety bounds limit damage but cannot prevent rent-seeking within allowed parameter ranges (e.g., setting the operator base share to the 90% maximum). The 5% per-operator voting cap and the age-ramp partially mitigate concentration. Real CDN traffic skews power-law, so the cap is load-bearing and may need tightening toward the 1% floor during early mainnet per [ADR 036 § Risks](036-served-bytes-voting-weight.md#risks).
- Served-bytes-weighted voting is gameable by operators self-paying for delivery — an attacker forfeits ~40% of paid USDC (30% buyback-burn + 10% treasury, per the [FeeRouter split](026-tokenomics.md#feerouter-split)) and recoups 60% as the operator base. The attack is bounded by the per-operator cap and `age_ramp`, but cost-to-buy-5%-vote is non-zero. Per [ADR 036 § Threat Model — Wash-trading as vote-buying](036-served-bytes-voting-weight.md#wash-trading-as-vote-buying).
- 1-day voting delay + 7-day voting period + 48-hour timelock means ~10 days minimum to respond to non-emergency issues via governance
- Governance participation typically skews low; 4% quorum (against total voting weight) may be difficult to reach consistently, especially during the bootstrap-multisig phase where served-bytes-weighted voting isn't yet active
- Non-operator TOKEN holders — including investors, team, treasury, and vesting recipients — have zero DAO vote unless they also operate a node. This is the deliberate regulatory-cleanliness commitment but it materially narrows the political base of the DAO. See [Investor disposition (Open Q #7 resolved)](#investor-disposition-open-q-7-resolved) above for the resolution.
- Regulatory framing improves under work-token (Howey prong 4 is broken) but is not eliminated; counsel review required before deployment per [ADR 026 § Regulatory framing and ADR delta](026-tokenomics.md#cross-adr-impact).
- The permanent on-chain unlawful-content-removal power is a residual centralization surface. It is bounded — it can only block specific unlawful hashes/origins, moves no funds, and is subject to the blacklist appeal process — but counsel must confirm at deployment that a perpetual narrow compliance capability is distinguishable from managerial control and does not weaken the *Howey* prong-4 / sufficient-decentralization posture. The alternative (let the capability sunset and rely solely on the legal wrapper's off-chain board to discharge permanent takedown and sanctions duties) is the open counsel question this boundary is calibrated against.
