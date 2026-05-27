# ADR 009: Governance Model

**Date:** 2026-05-27
**Status:** Draft

## Context

[ADR 026](026-tokenomics.md#adr-026-tokenomics) replaces the prior ve-gauge tokenomics with a work-token model. The governance-relevant primitives are: (i) a single `CapacityBond` contract that holds operator bonds proportional to declared bandwidth (`bond = k × Mbps^α`), exposes `firstBondedAt(operator)` as the source of the `age_ramp` tenure factor, exposes `slashedAtEpoch(operator)` for the slash-aware voting-weight zero-out, and runs the deterministic capacity-shortfall slashing path; (ii) a four-bucket `FeeRouter` whose share parameters are governable within hard-coded bounds (no epoch buckets, no claim windows) and whose per-operator `bytesPerEpoch` accounting is the source of served-bytes voting weight per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight); (iii) a `SafetyReserve` contract whose payouts are gated by governance-authorized rules.

Governance — how protocol parameters are changed, who can change them, and what safety mechanisms exist — is a separate concern. The governance contracts (`DecdnGovernor`, `TimelockController`) ship in the day-one single-audit-pass surface ([ADR 016 § Contract Inventory](016-contract-interactions.md#contract-inventory)); what differs by phase is the governance *process*, not the contract surface. In the PoC, parameters are changed through a single admin key. At launch, a bootstrap multisig replaces the admin key while the operator set is too thin for capacity-weighted DAO voting to be safe. Once the operator set reaches the transition thresholds, full operator-weighted DAO governance activates.

This ADR covers:

1. The PoC governance model (admin key)
2. The bootstrap-multisig phase (first 6–12 months post-launch, until operator set is large enough)
3. The production governance model (OpenZeppelin Governor against `CapacityBond`)
4. Governable parameters and their hardcoded safety bounds (including the four `FeeRouter` shares and `CapacityBond` curve parameters)
5. Emergency multisig design
6. SafetyReserve payout authorization rules

## Decision

### PoC: Admin Key

A single deployer address (EOA or multisig) has admin rights on all contracts. Can update any parameter. No voting, no timelock.

### Bootstrap-multisig phase

Post-launch, the voting set is narrow (likely <50 active operators in the first 6–12 months). Direct application of capacity-weighted DAO voting pre-bootstrap risks hostile takeover via a cheap operator-fleet setup. The multisig replaces the PoC admin key in this phase and operates the protocol within the safety bounds in [§ Governable Parameters with Safety Bounds](#governable-parameters-with-safety-bounds).

**Composition.** 5-of-9 multisig (separate from the [emergency multisig](#emergency-multisig)) with geographically and organizationally diverse signers. Signer set publicly disclosed.

**Capabilities.** All parameter updates within the safety bounds; `FeeRouter.setShares` / dependency-address setters; `CapacityBond` parameter setters (α, k, MAX_CAPACITY_PER_OPERATOR, min_delivery_ratio, age_ramp_months, unbonding window); `SafetyReserve.payout` authorization (subject to the four gates in [§ SafetyReserve Payout Authorization](#safetyreserve-payout-authorization)); standard 48-hour timelock on every parameter change.

**Transition thresholds.** The bootstrap-multisig phase ends when **active operator count ≥ 30** AND **total declared capacity ≥ 100 Gbps**. Both thresholds are governable within bounds (operator count `[10, 200]`, capacity `[10 Gbps, 1000 Gbps]`). At the transition, governance executes a one-shot setter that flips `DecdnGovernor` to read from `CapacityBond` as the voting-weight source and revokes the bootstrap-multisig's `GOVERNANCE_ROLE`. The transition setter cannot be reversed; the bootstrap-multisig cannot be reinstated.

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

Non-operator value accrual is structurally guaranteed at the *contract* level, not at the governance level. Operators cannot vote to push the operator-base share above the 90% upper bound, drop burn below the 5% lower bound, or otherwise expropriate non-operator-aligned shares — see the immutable [§ Governable Parameters with Safety Bounds](#governable-parameters-with-safety-bounds) below. The cashflow invariant (40% floor on the operator base) ensures clients still receive paid delivery; the 5% floor on burn preserves the deflationary lever; the 0% floor on treasury and safety permits governance to simplify the split without dropping operator-aligned cashflow.

#### Investor disposition (Open Q #7 resolved)

The entity design already excludes investors and other non-operator holders from DAO voting by structure — DAO governance is permissionless and no-KYC, with investor influence routed to the Labs (C-Corp) equity layer (Series A+ board seats, standard preferred-stock protective provisions, indirect TOKEN exposure via Labs' ~15% treasury allocation). Source: `internal/Legal/entity-structure-design.md` § Pattern A (Legal Fiction Separation).

Work-token does *not* narrow investor power *relative to that existing design*. DAO voting is restricted to operators — a change to who-among-active-participants votes, not a removal of an investor right that ever existed in entity design. Term-sheet language for Private Investors should not promise a ve-lock passive-governance path, because that was never a designed-in investor right.

Mitigation if any investor wants direct DAO signal: any TOKEN holder who *also* operates a node votes like any other operator. This path is open to investors, team, treasury, and seed equally.

### Governable Parameters with Safety Bounds

All economic parameters across the protocol are governable within hardcoded safety bounds. Safety bounds are immutable — even a governance attack cannot set parameters outside these ranges.

#### FeeRouter shares (per [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds))

The four `FeeRouter` shares are governable within the bounds below. **Sum-to-100% invariant:** every governance update that modifies any share **must** leave the four shares summing to exactly 100% (10000 bps); updates that violate the sum or that exceed any individual bound revert at the contract layer.

| Parameter | Default | Min | Max |
| --- | ---: | ---: | ---: |
| Operator base share | 60% | 40% | 90% |
| Burn share | 25% | 5% | 50% |
| Treasury share | 10% | 0% | 30% |
| Safety share | 5% | 0% | 20% |

The 40% floor on the operator-base share is the cashflow invariant: operators always receive enough liquid USDC to cover at least a meaningful fraction of infrastructure costs even under extreme governance proposals. The 5% floor on burn preserves the deflationary lever; the 0% floors on treasury and safety let governance simplify the split.

#### CapacityBond curve and governance parameters (per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve))

| Parameter | Default | Min | Max |
| --- | ---: | ---: | ---: |
| α (capacity-curve exponent) | 1.2 | 1.0 | 1.8 |
| k (capacity-curve constant, TOKEN) | 12.6 | bounded by 1G-tier bond ∈ [10K, 200K TOKEN] | — |
| `MAX_CAPACITY_PER_OPERATOR` | 200 Gbps | 50 Gbps | 1000 Gbps |
| `min_delivery_ratio` | 70% | 50% | 90% |
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

**Treasury disbursement:** Spending from the protocol treasury wallet (the destination of the 10% `FeeRouter` treasury share per [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split)) — including operational expenditures, ecosystem grants, audits, and any onward transfers — requires a standard governance proposal in production. During the bootstrap-multisig phase, the multisig directs treasury spending within the safety bounds. The emergency multisig cannot withdraw treasury funds (see [Emergency Multisig](#emergency-multisig)); its only fund-movement authority is the SafetyReserve fast-track path, which is bounded by hard caps and the four payout gates.

**Safety bound rationale:**

- **Slash 5%–50% per offense:** A 1% slash is economically negligible and provides no deterrence. A 100% single-offense slash enables governance to fully confiscate the bond, which is disproportionate. The 5%–50% range ensures each individual slash is meaningful but not existential. Full ejection (effectively 100% loss) is still possible through **cumulative** slashing per [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn). Capacity-shortfall slashing ([ADR 026 § Capacity-shortfall slashing](026-tokenomics.md#capacity-shortfall-slashing)) is a separate deterministic path that auto-downgrades operators with sustained delivery below `min_delivery_ratio × declared_capacity`; its bound is the `min_delivery_ratio` parameter above.
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
- **`min_delivery_ratio` 50%–90%:** Below 50% the capacity-shortfall slashing is too lax to deter capacity inflation; above 90% honest operators with transient probe failures (network blips, NTP drift) get auto-downgraded.
- **`age_ramp_months` 1–24:** The age-ramp defends against "buy your way to instant governance" attacks. 1 month is the operational floor; 24 months is high enough to materially delay hostile-fleet votes but low enough that legitimate new operators reach full weight in a reasonable time.
- **Per-operator voting cap 1%–25%:** The 1% floor prevents governance from making any single operator's vote vanishingly small (which would push the network toward de facto majority-of-cap voting); the 25% ceiling keeps any single operator's vote bounded. The cap is the primary defense against bytes-weighted concentration — real CDN traffic skews power-law, so the cap floor (1%) is the first tightening lever if observed concentration warrants per [ADR 036 § Threat Model — Concentration](036-served-bytes-voting-weight.md#concentration).
- **`windowEpochs` 4–26:** The trailing-window length over which served bytes are summed for voting weight. Below 4 epochs (~1 month) vote weight is too reactive to single-burst wash trading and statistically thin for small operators; above 26 epochs (~6 months) the trailing window lags actual operator-set composition (an operator who exited service ~5 months ago still carries half-weight) and `_getVotes` cold-SLOAD gas rises to ~55K per voter per `castVote`. Per [ADR 036 § Governable parameters with safety bounds](036-served-bytes-voting-weight.md#governable-parameters-with-safety-bounds).
- **Multisig transition thresholds:** Operator count `[10, 200]` and capacity `[10 Gbps, 1000 Gbps]` give governance flexibility to delay or advance the transition based on operator-set diversity that's not visible at deploy time.

### SafetyReserve Payout Authorization

The 5% `SafetyReserve` bucket introduced by [ADR 026 § Safety and insurance reserve (5% bucket)](026-tokenomics.md#safety-and-insurance-reserve-5-bucket) is a governance-gated incident reserve, not a passive yield source. Payouts cover SLA-failure compensation, incorrect-slashing reversals, relay/sequencer/payment-channel downtime, bad-data incidents, and capacity-shortfall-slashing reversals — see [ADR 026 § Safety and insurance reserve (5% bucket)](026-tokenomics.md#safety-and-insurance-reserve-5-bucket) for the full eligibility list.

`SafetyReserve.payout(bundle, recipient, amount)` checks all four of the following gates; absence of any of them causes the call to revert. There is no path for unattested or unreviewed payouts.

1. **Attested incident bundle.** The caller must supply a cryptographic evidence bundle identifying the failure mode, the harmed party, and the proposed payout amount.
2. **Authorization.** Either (a) a successful governance proposal that authorizes the specific bundle, or (b) emergency-multisig fast-track approval — the multisig may execute payouts under hard caps (per-incident and per-rolling-window USDC ceilings configured at deploy time and immutable thereafter; see [Emergency Multisig](#emergency-multisig)). Multisig fast-track is intended for time-critical incidents and does not bypass the other three gates.
3. **48-hour appeal window.** After authorization, the bundle enters a 48-hour on-chain appeal window during which any party may submit a counter-bundle challenging the original. Successful challenges revert the authorization. The appeal window cannot be shortened.
4. **Post-incident reporting.** On payout settlement, `SafetyReserve` writes an immutable record to its public on-chain registry (incident hash, payout amount, recipient, authorization path used, links to the evidence bundle and any successful appeals). Operators of the registry MUST publish a human-readable post-incident report referencing the on-chain record; the registry tracks completion of these reports and exposes outstanding-report counts as a public metric.

The emergency multisig's fast-track authority over gate 2 is constrained by the same hard caps the multisig is bound by elsewhere in this ADR — it cannot withdraw treasury funds and cannot bypass the appeal window.

### Emergency Multisig

- 3-of-5 multisig with known, trusted signers (separate from the bootstrap-governance multisig above)
- Capabilities (exhaustive list):
  1. **Pause contracts** — halt all contract execution for exploit response and critical bug mitigation
  2. **Emergency content blacklisting** — add hashes and origin operators to the `ContentBlacklist` contract via `emergencyAdd` and `emergencyAddOrigin` (see [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting))
  3. **Regional body suspension** — suspend a compromised regional governance body via `suspendRegionalBody` (see [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)); must be ratified or reversed by governance within 14 days. Sub-modes for the blacklist-entry-appeal flow are `ContentBlacklist.fastTrackAppeal(appealId)` / `unFastTrackAppeal(appealId)` / `rejectAppeal(appealId)` / `rejectAppealAsPerjury(appealId)` — see [ADR 011 § Blacklist Entry Appeals](011-content-takedown.md#blacklist-entry-appeals) and [ADR 031](031-content-blacklist-appeals-contract.md#adr-031-contentblacklist-appeal-contract-surface)
  4. **SafetyReserve fast-track authorization** — authorize gate 2 of a `SafetyReserve.payout` flow under immutable hard caps (per-incident and per-rolling-window USDC ceilings). Sub-modes for the slash-appeal flow are `SafetyReserve.fastTrackAppeal(appealId)` (grants interim relief on an open appeal) and `SafetyReserve.rejectAppeal(appealId)` (denies an open appeal); both are bounded by the same hard caps. Does **not** bypass the evidence-bundle, 48-hour appeal, or post-incident-reporting gates (see [SafetyReserve Payout Authorization](#safetyreserve-payout-authorization) and [ADR 028 § Contract surface](028-slashing-appeals.md#contract-surface))
- Cannot change parameters, withdraw funds (other than gated SafetyReserve payouts), or bypass governance for non-emergency actions
- Used for exploit response, critical bug mitigation, and time-critical content removal (e.g., CSAM, actively-exploited material)
- Sunset: `pauseDeadline = deployTimestamp + 365 days` is hardcoded in the constructor as an immutable value. After the deadline, `pause()` reverts with `"PauseExpired"`. Emergency blacklisting capability follows the same sunset schedule. **Extension mechanism:** governance cannot modify the immutable deadline. To extend pause/blacklist capability, governance must deploy a new contract version with a new deadline and migrate via the standard contract upgrade path (timelock + governance vote). This ensures the sunset cannot be silently extended.
- Signers should be geographically and organizationally diverse

## Consequences

### Positive

- Hardcoded safety bounds on all governable parameters limit the damage a governance attack can cause
- Emergency multisig provides rapid exploit response without giving any party unilateral control over funds or parameters
- Sunset clause on the emergency multisig prevents permanent centralization
- Bootstrap-multisig phase prevents hostile takeover during the thin-operator-set launch window without locking the operator set out of governance forever — the threshold-based transition is data-driven, not calendar-driven
- Operator-only DAO voting structurally enforces the work-token regulatory posture (Howey prong 4); non-operator value accrual is preserved at the contract level via immutable share floors
- PoC can operate with a simple admin key; the governance contracts ship day-one in the single-audit-pass surface ([ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model))

### Negative

- Served-bytes-weighted governance shifts capture risk from large TOKEN holders to high-traffic operators; safety bounds limit damage but cannot prevent rent-seeking within allowed parameter ranges (e.g., setting the operator base share to the 90% maximum). The 5% per-operator voting cap and the age-ramp partially mitigate concentration. Real CDN traffic skews power-law, so the cap is load-bearing and may need tightening toward the 1% floor during early mainnet per [ADR 036 § Risks](036-served-bytes-voting-weight.md#risks).
- Served-bytes-weighted voting is gameable by operators self-paying for delivery — an attacker forfeits ~40% of paid USDC (burn + treasury + safety legs) and recoups 60% as the operator base. The attack is bounded by the per-operator cap and `age_ramp`, but cost-to-buy-5%-vote is non-zero. Per [ADR 036 § Threat Model — Wash-trading as vote-buying](036-served-bytes-voting-weight.md#wash-trading-as-vote-buying).
- 1-day voting delay + 7-day voting period + 48-hour timelock means ~10 days minimum to respond to non-emergency issues via governance
- Governance participation typically skews low; 4% quorum (against total voting weight) may be difficult to reach consistently, especially during the bootstrap-multisig phase where served-bytes-weighted voting isn't yet active
- Non-operator TOKEN holders — including investors, team, treasury, and vesting recipients — have zero DAO vote unless they also operate a node. This is the deliberate regulatory-cleanliness commitment but it materially narrows the political base of the DAO. See [Investor disposition (Open Q #7 resolved)](#investor-disposition-open-q-7-resolved) above for the resolution.
- Regulatory framing improves under work-token (Howey prong 4 is broken) but is not eliminated; counsel review required before deployment per [ADR 026 § Regulatory framing and ADR delta](026-tokenomics.md#cross-adr-impact).
