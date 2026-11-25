# ADR 009: Governance Model

**Date:** 2026-05-27
**Status:** Draft

## Context

[ADR 026](026-tokenomics.md#adr-026-tokenomics) defines a work-token tokenomics model. The governance-relevant primitives are: (i) a single `CapacityBond` contract that holds operator bonds proportional to declared bandwidth (`bond = k × Mbps^α`), and exposes `firstBondedAt(operator)` as the source of the `age_ramp` tenure factor; (ii) a governance-chain `FeeRouter` that receives sweeps from payment-chain pools, splits each swept protocol share into a governable `rebateBps` and a treasury remainder, and whose per-operator `bytesPerEpoch` accounting is the source of served-bytes voting weight per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight).

Governance — how protocol parameters are changed, who can change them, and what safety mechanisms exist — is a separate concern. The governance contracts (`DecdnGovernor`, `TimelockController`) ship in the day-one single-audit-pass surface ([ADR 016 § Contract Inventory](016-contract-interactions.md#contract-inventory)); what differs by phase is the governance *process*, not the contract surface. In the PoC, parameters are changed through a single admin key. At launch, a bootstrap multisig replaces the admin key while the operator set is too thin for operator-weighted DAO voting (served-bytes-weighted per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)) to be safe. When the bootstrap multisig judges the operator set broad enough, it executes a one-shot transition to full operator-weighted DAO governance.

This ADR covers:

1. The PoC governance model (admin key)
2. The bootstrap-multisig phase (first 6–12 months post-launch, until operator set is large enough)
3. The production governance model (OpenZeppelin Governor against `CapacityBond`)
4. Governable parameters and their hardcoded safety bounds (including the `FeeRouter` split and `CapacityBond` curve parameters)
5. Emergency multisig design

## Decision

### PoC: Admin Key

A single deployer address (EOA or multisig) has admin rights on all contracts. Can update any parameter. No voting, no timelock.

### Bootstrap-multisig phase

Post-launch, the voting set is narrow (likely <50 active operators in the first 6–12 months). Direct application of capacity-weighted DAO voting pre-bootstrap risks hostile takeover via a cheap operator-fleet setup. The multisig replaces the PoC admin key in this phase and operates the protocol within the safety bounds in [§ Governable Parameters with Safety Bounds](#governable-parameters-with-safety-bounds).

**Composition.** 5-of-9 multisig (separate from the [emergency multisig](#emergency-multisig)) with geographically and organizationally diverse signers. Signer set publicly disclosed.

**Capabilities.** All parameter updates within the safety bounds; `FeeRouter` split and dependency-address setters; `CapacityBond` parameter setters (α, k, MAX_CAPACITY_PER_OPERATOR); the `DecdnGovernor` `age_ramp_months` setter (`setAgeRampMonths`); standard 48-hour timelock on every parameter change.

**Mechanism.** The multisig never holds `GOVERNANCE_ROLE` on the role-gated contracts. The `TimelockController` holds it from deployment onward — in this phase and in production alike — which is what puts the 48-hour delay on every parameter change above regardless of who requested it. What the bootstrap phase confines is the right to *schedule*: `DeployProtocol` (run with `BOOTSTRAP_MULTISIG` set) grants the bootstrap multisig the Timelock's `PROPOSER_ROLE` + `CANCELLER_ROLE` and grants `DecdnGovernor` neither, so no operator vote can reach execution while the voting set is too thin to be safe. `DecdnGovernor.propose` also reverts until the Governor holds the Timelock's `PROPOSER_ROLE`, so no proposal passed by the thin early electorate waits, unqueued and unexpiring, to execute after the transition. The Governor is nonetheless deployed and its vote-weight sources are wired at deployment, not at transition (`FeeRouter.bytesInWindow` + `CapacityBond.firstBondedAt` per [ADR 036 § Formula](036-served-bytes-voting-weight.md#formula)). Deploying without `BOOTSTRAP_MULTISIG` skips the phase entirely and seats the Governor as proposer at genesis.

**Transition.** The bootstrap-multisig phase ends when the bootstrap multisig schedules a single Timelock batch — a manual decision it takes when it judges the operator set broad and diverse enough for operator-weighted DAO voting to be safe. There is no threshold automation and no bespoke contract: the batch targets the `TimelockController` itself, which self-administers its own roles, and grants `PROPOSER_ROLE` + `CANCELLER_ROLE` to `DecdnGovernor` before revoking both from the multisig. `contracts/script/TransitionToGovernor.s.sol` prints the exact `scheduleBatch` / `executeBatch` calldata and refuses to emit a batch against a chain that is not mid-bootstrap. Grant-before-revoke ordering mirrors the deploy-time handoff for readability; `executeBatch` runs all four legs in one transaction, so the intermediate state is never observable and either order reaches the same result. No standing party can reverse the transition: executing the batch strips the multisig's `PROPOSER_ROLE`, so it can no longer schedule anything — including a proposal restoring itself. Only the inheriting DAO could re-grant the role, by its own vote.

**Duration.** ~6–12 months expected; the multisig executes the transition when it judges the operator set ready rather than on a fixed calendar.

### Production: Operator-Weighted DAO Governance

Based on OpenZeppelin Governor, on the governance chain (Ethereum L1, [ADR 016 § Deployment Tiers](016-contract-interactions.md#deployment-tiers)), sourcing voting weight from the governance-chain `FeeRouter` served-bytes accounting (fed by sweeps) and `CapacityBond` tenure data per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight):

| Parameter | Value |
| --- | --- |
| Voting source | `FeeRouter.bytesInWindow(operator, epoch(ts), windowEpochs)` (served-bytes trailing-window sum), per-operator-capped, multiplied by `age_ramp` derived from `CapacityBond.firstBondedAt(operator)` — full formula in [ADR 036 § Formula](036-served-bytes-voting-weight.md#formula) |
| Voting weight | `min(served_bytes_window(op), voteCapBps × total_bytes_window) × age_ramp(op)` — zero at registration; ramps with served-bytes and tenure jointly |
| Voting delay | 1 day between proposal creation and the vote snapshot |
| Proposal threshold | 0.1% of total voting weight at proposal snapshot |
| Voting period | 7 days |
| Quorum | 4% of total voting weight at proposal snapshot |
| Timelock | 48 hours between vote passing and execution |
| Per-operator voting cap | 10% of total voting weight (applied against the bytes-weighted total); launches at the 10% ceiling and only ever decreases |
| Vote delegation | EIP-712 signed delegation (Governor Bravo pattern) — voting power is delegable; the bond itself is not |

The clock is timestamp-based (ERC-6372 `mode=timestamp`) to align with `FeeRouter`'s epoch-keyed accounting (epoch length immutable, 1 week — see [ADR 016 § Contract: FeeRouter](016-contract-interactions.md#contract-feerouter)). Voting weight is computed from `FeeRouter`'s `bytesInWindow` / `totalBytesInWindow` helpers and `CapacityBond.firstBondedAt` directly, not through an `IVotes`/IERC-5805 surface — vote weight is derived from FeeRouter epoch accounting rather than from per-account checkpoint structures, so the Governor supplies the vote source, quorum, and per-operator cap as thin overrides rather than via OZ's `GovernorVotes` / `GovernorVotesQuorumFraction` modules.

Quorum and proposal threshold are calibrated against `FeeRouter.totalBytesInWindow(epoch(ts), windowEpochs)` — the unramped sum of served bytes across all operators over the trailing window, each operator's epoch bytes clamped to its declared capacity — **not** total TOKEN supply and **not** the per-operator-capped weighted total. The Governor uses the unramped total as a tractable upper bound for the strictly-correct capped-and-ramped sum (which would be O(active_operators × N) to compute); the resulting quorum / threshold is mildly conservative. Voting weight tracks active operator service delivery rather than passive holdings; calibrating against it avoids the failure mode where the quorum bar trivially exceeds engaged voting power as TOKEN circulates.

**Non-operator TOKEN holders carry zero voting weight.** Traders, passive holders, vesting recipients (Core Contributors, Advisors, Seed, Private, Treasury, Public Sale, Airdrop / Testnet, Marketing), and any TOKEN not bonded into `CapacityBond` have no vote. Per [ADR 026 § Governance](026-tokenomics.md#governance), this is the work-token regulatory-cleanliness commitment: passive holding earns nothing — neither revenue nor governance privilege.

#### Non-operator holder protection

The operator guarantee is contract-level. Each `PaymentPool` fixes `operatorBps` as an immutable, so governance cannot lower the operator's share of a redemption ([ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split)). Governance sets only how the swept protocol share divides: see [§ Governable Parameters with Safety Bounds](#governable-parameters-with-safety-bounds).

#### Investor disposition

The entity design already excludes investors and other non-operator holders from DAO voting by structure — DAO governance is permissionless and no-KYC, with investor influence routed to the Labs (C-Corp) equity layer (Series A+ board seats, standard preferred-stock protective provisions, indirect TOKEN exposure via Labs' ~15% treasury allocation).

Work-token does *not* narrow investor power *relative to that existing design*. DAO voting is restricted to operators — a change to who-among-active-participants votes, not a removal of an investor right that ever existed in entity design. Term-sheet language for Private Investors should not promise a ve-lock passive-governance path, because that was never a designed-in investor right.

Mitigation if any investor wants direct DAO signal: any TOKEN holder who *also* operates a node votes like any other operator. This path is open to investors, team, treasury, and seed equally.

### Governance permanence: no standing veto

The bootstrap → operator-DAO handoff is irreversible (see [Bootstrap-multisig phase](#bootstrap-multisig-phase)): once `PROPOSER_ROLE` moves from the bootstrap multisig to `DecdnGovernor`, no standing party can veto, reverse, or pre-empt an operator vote. This is a deliberate choice, not an omission — a permanent on-chain veto authority over governance is rejected.

A permanent veto would re-introduce a central controller, which weakens the work-token regulatory posture rather than protecting it. The *Howey* "efforts of others" prong and the "sufficient decentralization" argument for secondary trading both depend on no party holding ongoing managerial control over the protocol (per [ADR 026 § Governance](026-tokenomics.md#governance) and the entity design in `internal/Legal/entity-structure-design.md` § Pattern A). A standing veto is precisely the control that posture is built to avoid, so it cannot be the mechanism that makes the handoff legally workable.

Legal viability of the handoff rests on the off-chain legal wrapper, not on an on-chain controller. The governance constituency (bonded operators) votes on-chain; a legal entity (the deCDN Verein) executes those votes and carries the statutory duties. Its board is bound to implement any passing on-chain proposal **except where doing so would violate applicable law** — a narrow legal-compliance veto that lives at the execution layer, scoped to illegality, not a discretionary override of governance. Entity structure, board obligations, and the law-compliance carve-out are canonical in `internal/Legal/entity-structure-design.md` § Pattern A (Legal Fiction Separation).

The one capability retained on-chain in perpetuity is the **narrow unlawful-content-removal** power (see [Emergency Multisig](#emergency-multisig)): it discharges a permanent, time-critical legal duty, is scoped to blocking specific unlawful content, and touches no economic, treasury, or governance lever — so it is distinguishable from the managerial control the decentralization posture forbids. Final calibration of this compliance boundary is subject to the counsel review required before deployment (see [Consequences](#consequences)).

### Governable Parameters with Safety Bounds

All economic parameters across the protocol are governable within hardcoded safety bounds. Safety bounds are immutable — even a governance attack cannot set parameters outside these ranges.

#### FeeRouter split (per [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split))

The governance-chain `FeeRouter` splits each swept protocol share. Governance sets the split through the Timelock. The rebate is at most 99%, so the treasury keeps at least 1% and vote weight stays earnable ([ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics)).

| Parameter | Meaning |
| --- | --- |
| `rebateBps` | Share returned to the operator |
| Treasury remainder | The rest goes to the Timelock treasury |
| `treasury` | Destination address of the remainder |

The treasury address is never zero. The operator's own guarantee is the immutable `operatorBps` of each pool, which governance cannot change.

#### CapacityBond curve and governance parameters (per [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve))

| Parameter | Default | Min | Max |
| --- | ---: | ---: | ---: |
| α (capacity-curve exponent) | 1.2 | 1.0 | 1.8 |
| k (capacity-curve constant, TOKEN) | 12.6 | bounded by 1G-tier bond ∈ [10K, 200K TOKEN] | — |
| `MAX_CAPACITY_PER_OPERATOR` | 200 Gbps | 50 Gbps | 1000 Gbps |
| `age_ramp_months` | 6 | 1 | 24 |
| Per-operator voting cap | 10% | 1% | 10% |
| `windowEpochs` (served-bytes trailing window, on `FeeRouter`) | 13 | 4 | 26 |
| `deliveryFloor` (treasury-leg USDC micro-units per MiB of weight, on `FeeRouter`) | 1 | 1 | 1000 |

The α range upper-bounds at 1.8 to prevent a concentration penalty so steep that mid-tier operators are economically barred from upgrading; the lower bound at 1.0 ensures decentralization pressure is never fully disabled. k is parameterized via the 1G-tier bond range to constrain governance volatility — direct changes to k can shift the entire bond curve, so the bound is on the *resulting bond* rather than on k itself. The per-operator voting cap launches at its 10% ceiling and is decrease-only within `[1%, 10%]`; it is the primary defense against bytes-weighted concentration per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight). The `windowEpochs` bounds are pinned in [ADR 036 § Governable parameters with safety bounds](036-served-bytes-voting-weight.md#governable-parameters-with-safety-bounds): below 4 epochs (~1 month) vote weight is too reactive to single-burst wash trading; above 26 epochs (~6 months) the trailing window lags actual operator-set composition and `DecdnGovernor._getVotes` cold-SLOAD gas rises to ~55K per voter per `castVote`.

#### Other protocol parameters

| Parameter | Contract | Min | Max |
| --- | --- | --- | --- |
| Multiaddr update cooldown | CapacityBond | 0 (disabled) | 86400 seconds (1 day) |
| Max multiaddr size | CapacityBond | 64 bytes | 1024 bytes |
| Compliance window | ContentBlacklist | 1 hour | 7 days |
| Max origins per namespace | OriginAssignment | 1 | 50 |
| Max namespaces per publisher | PublisherRegistry | 1 | 1000 |

`PaymentPool` has no governance parameters. Its dispute window and operator share are immutables fixed per deployment (the payment-chain admission rule is in [ADR 016 § Deployment Tiers](016-contract-interactions.md#deployment-tiers)). There is no settlement-time fee skim or discount-stake mechanic on the payment contract. The only TOKEN-side requirement on operators is the capacity bond. At entry, it must meet the larger of the capacity-bond curve and the global `minBond` floor ([ADR 026 § Minimum bond is non-retroactive](026-tokenomics.md#minimum-bond-is-non-retroactive)).

The 7-day voting period balances responsiveness with participation. Combined with the 1-day voting delay and the 48-hour timelock, the total governance delay is ~10 days minimum — longer than the standard OpenZeppelin Governor defaults, reflecting operator-class participation cadence.

CapacityBond parameters are defined in [ADR 026 § Capacity-bond curve](026-tokenomics.md#capacity-bond-curve) and [ADR 026 § Minimum bond is non-retroactive](026-tokenomics.md#minimum-bond-is-non-retroactive); fee-routing parameters are governed by [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds). Payment pool mechanics are defined in [ADR 003](003-payments.md#adr-003-payment-model). This ADR defines the governance mechanism that controls them.

**Treasury disbursement:** Spending from the protocol treasury wallet (the destination of the treasury share of each swept protocol share on the governance chain, per [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split)) — including operational expenditures, ecosystem grants, audits, any discretionary incident restitution (per [ADR 026 § Incident recourse](026-tokenomics.md#incident-recourse-no-standing-reserve)), and any onward transfers — requires a standard governance proposal in production. During the bootstrap-multisig phase, the multisig directs treasury spending within the safety bounds. The emergency multisig cannot withdraw treasury funds (see [Emergency Multisig](#emergency-multisig)); it has no fund-movement authority.

**Safety bound rationale:**

- **Grace window per payment chain:** The `disputeWindow` of each pool is an immutable set per deployment. It must stay above that chain's censorship window so censorship cannot burn the whole window. A window that is too long locks owner funds for an unacceptably long period. See [ADR 003](003-payments.md#adr-003-payment-model).
- **Max origins per namespace 1–50:** Upper bound prevents storage-cost griefing and unbounded gas in `getOrigins` view calls.
- **Max namespaces per publisher 1–1000:** Anti-squatting limit.
- **α range 1.0–1.8 and k bounded by 1G-tier bond [10K, 200K TOKEN]:** The α floor at 1.0 (linear) prevents disabling decentralization pressure entirely. The α ceiling at 1.8 prevents a concentration penalty so steep that mid-tier operators face >5× per-Mbps capital cost vs entry-tier and effectively cannot upgrade. The k bound is parameterized via the 1G-tier bond to constrain governance volatility — k=12.6 gives ≈50K TOKEN at 1 Gbps; the [10K, 200K] range allows governance to halve or quadruple the entry-tier bond without rewriting the curve from scratch.
- **`age_ramp_months` 1–24:** The age-ramp defends against "buy your way to instant governance" attacks. 1 month is the operational floor; 24 months is high enough to materially delay hostile-fleet votes but low enough that legitimate new operators reach full weight in a reasonable time. The ramp counts only continuous bonding at or above the activation floor. An unbond below the activation floor ends the tenure, and the next entry restarts the ramp ([ADR 036 § Behaviors that follow from the formula](036-served-bytes-voting-weight.md#behaviors-that-follow-from-the-formula)).
- **Per-operator voting cap, decrease-only within 1%–10%:** The cap launches at its 10% ceiling and governance can only lower it, never raise it, so it only ever decentralizes vote weight. Launching at the ceiling keeps quorum reachable while the operator set is thin. The 1% floor prevents governance from making any single operator's vote vanishingly small (which would push the network toward de facto majority-of-cap voting). The cap is the primary defense against bytes-weighted concentration — real CDN traffic skews power-law, so lowering the cap toward its 1% floor is the first tightening lever if observed concentration warrants per [ADR 036 § Threat Model — Concentration](036-served-bytes-voting-weight.md#concentration).
- **`windowEpochs` 4–26:** The trailing-window length over which served bytes are summed for voting weight. Below 4 epochs (~1 month) vote weight is too reactive to single-burst wash trading and statistically thin for small operators; above 26 epochs (~6 months) the trailing window lags actual operator-set composition (an operator who exited service ~5 months ago still carries half-weight) and `_getVotes` cold-SLOAD gas rises to ~55K per voter per `castVote`. Per [ADR 036 § Governable parameters with safety bounds](036-served-bytes-voting-weight.md#governable-parameters-with-safety-bounds).

### Emergency Multisig

- 3-of-5 multisig with known, trusted signers (separate from the bootstrap-governance multisig above). On Ethereum the deploy refuses an emergency multisig that is the deployer or holds no code; testnet deploys may seat the deployer. The same multisig is the immutable `pauser` of each payment-chain `PaymentPool`. That pauser blocks only `openPool` and `topUp`.
- Capabilities (exhaustive list):
  1. **Pause contracts** — halt new intake and serving for exploit response and critical bug mitigation. Pause never halts an exit. A paused `CapacityBond` still accepts `unbond`, `deregisterNode`, and an inactive operator's `declareMbps(0)` tier release, and a paused `PaymentPool` still accepts `reclaim`, `closePool`, `redeemMany` and `sweep`, so a captured PAUSER cannot trap operator bond or client deposits
  2. **Emergency content blacklisting** — add hashes and origin operators to the `ContentBlacklist` contract via `emergencyAdd` and `emergencyAddOrigin` (see [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting))
  3. **Regional body suspension** — suspend a compromised regional governance body via `suspendRegionalBody` (see [ADR 011](011-content-takedown.md#adr-011-content-takedown-and-hash-blacklisting)); must be ratified or reversed by governance within 14 days
- Cannot change parameters, withdraw funds, or bypass governance for non-emergency actions
- Used for exploit response, critical bug mitigation, and time-critical content removal (e.g., CSAM, actively-exploited material)
- Sunset (capability-split): the emergency authority sunsets **by capability**, because some emergency powers discharge time-bound launch risk while others discharge permanent legal duties. The two are decoupled at the contract layer rather than sharing one deadline.
  - **Exploit-response pause sunsets hard.** `pauseDeadline` is fixed at each contract's own construction time (`block.timestamp + 365 days`) as an immutable value; after the deadline `pause()` reverts with the custom error `PauseExpired`. The pause is a protocol-wide brake — the most centralization-sensitive emergency power — so it is the capability that expires, for every caller including governance itself: there is no role carve-out on the deadline, so a timelock/governance `pause()` reverts identically once the window closes. Post-sunset exploit response routes through deploying a new contract version with a fresh deadline via the standard timelock + governance upgrade path. Governance cannot modify the immutable deadline, so the sunset cannot be silently extended.
  - **Unlawful-content removal does not sunset.** Emergency blacklisting (`emergencyAdd`, `emergencyAddOrigin`) and the regional-body suspension that backs it (`suspendRegionalBody`) discharge a permanent legal duty — removal of CSAM, court-ordered, sanctions-driven, and other unlawful content — that has no expiry and is time-critical: a removal order can carry a one-hour clock that the ~10-day governance cycle cannot meet. This authority is deliberately narrow — it can block specific unlawful hashes and origins and nothing else, with no parameter, treasury, fund-movement, or governance-veto power — so a permanent grant does not re-centralize protocol control. A wrongful entry remains removable via the ordinary removal path (see [ADR 011 § Removing a Wrongful Entry](011-content-takedown.md#removing-a-wrongful-entry)). The legal entity that holds this authority and its appeal-override duties are specified in `internal/Legal/entity-structure-design.md` § Pattern A.
  - **Contract-layer enforcement.** Every pausable protocol contract inherits a shared `SunsettingPausable` base that fixes `pauseDeadline = block.timestamp + 365 days` as an immutable at its own construction and reverts `PauseExpired` from `pause()` once that deadline passes. The unlawful-content-removal powers carry no such deadline: they live on the non-pausable `ContentBlacklist`, so the blacklist path and the pause path hold independent lifetimes by construction rather than by splitting a shared deadline.
- Signers should be geographically and organizationally diverse

## Consequences

### Positive

- Hardcoded safety bounds on all governable parameters limit the damage a governance attack can cause
- Emergency multisig provides rapid exploit response without giving any party unilateral control over funds or parameters
- The governance handoff is irreversible and carries no standing veto, keeping ongoing managerial control out of any single party's hands — the structural basis of the work-token regulatory posture. Legal-compliance authority is held off-chain by the legal wrapper's board and scoped to illegality, not a discretionary override of governance
- Capability-split sunset: the protocol-wide pause expires, while the only permanent on-chain emergency authority is the narrow unlawful-content-removal power — preventing the emergency role from becoming a standing controller while preserving a permanent, time-critical compliance duty that the ~10-day governance cycle cannot discharge
- Bootstrap-multisig phase prevents hostile takeover during the thin-operator-set launch window without locking the operator set out of governance forever — the multisig executes the one-shot transition when it judges the operator set ready
- Operator-only DAO voting structurally enforces the work-token regulatory posture (Howey prong 4); non-operator value accrual is preserved at the contract level via the immutable `operatorBps`
- PoC can operate with a simple admin key; the governance contracts ship day-one in the single-audit-pass surface ([ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model))

### Negative

- Served-bytes-weighted governance shifts capture risk from large TOKEN holders to high-traffic operators; safety bounds limit damage but cannot prevent rent-seeking within allowed parameter ranges (e.g., setting the treasury remainder of the swept protocol share to its maximum). The 10% per-operator voting cap and the age-ramp partially mitigate concentration. Real CDN traffic skews power-law, so the cap is load-bearing and may need tightening toward the 1% floor during early mainnet per [ADR 036 § Risks](036-served-bytes-voting-weight.md#risks).
- Served-bytes-weighted voting is gameable by operators self-paying for delivery — the attacker's cost is the USDC that stays with the protocol (the treasury leg), enforced on the governance chain by the credit formula. The attack is bounded by the per-operator cap and `age_ramp`, but cost-to-buy-10%-vote is non-zero. Per [ADR 036 § Threat Model — Wash-trading as vote-buying](036-served-bytes-voting-weight.md#wash-trading-as-vote-buying).
- 1-day voting delay + 7-day voting period + 48-hour timelock means ~10 days minimum to respond to non-emergency issues via governance
- Governance participation typically skews low; 4% quorum (against total voting weight) may be difficult to reach consistently, especially during the bootstrap-multisig phase where served-bytes-weighted voting isn't yet active
- Non-operator TOKEN holders — including investors, team, treasury, and vesting recipients — have zero DAO vote unless they also operate a node. This is the deliberate regulatory-cleanliness commitment but it materially narrows the political base of the DAO. See [Investor disposition](#investor-disposition) above.
- Regulatory framing improves under work-token (Howey prong 4 is broken) but is not eliminated; counsel review required before deployment per [ADR 026 § Regulatory framing and ADR delta](026-tokenomics.md#cross-adr-impact).
- The permanent on-chain unlawful-content-removal power is a residual centralization surface. It is bounded — it can only block specific unlawful hashes/origins, moves no funds, and a wrongful entry remains removable via [ADR 011 § Removing a Wrongful Entry](011-content-takedown.md#removing-a-wrongful-entry) — but counsel must confirm at deployment that a perpetual narrow compliance capability is distinguishable from managerial control and does not weaken the *Howey* prong-4 / sufficient-decentralization posture. The alternative (let the capability sunset and rely solely on the legal wrapper's off-chain board to discharge permanent takedown and sanctions duties) is the open counsel question this boundary is calibrated against.
