# ADR 009: Governance Model

**Date:** 2026-03-29
**Status:** Draft

## Context

[ADR 026](026-gauge-boost-tokenomics.md) introduces three tokenomics primitives that this ADR depends on: a `VotingEscrow` contract (vote-escrowed TOKEN with linear decay), a six-bucket `FeeRouter` whose share parameters are governable within hard-coded bounds, and a `SafetyReserve` contract whose payouts are gated by governance-authorized rules. These are the economic primitives the production governance model assumes.

Governance — how protocol parameters are changed, who can change them, and what safety mechanisms exist — is a separate concern. During the PoC, governance is a single admin key. Production governance (ve-weighted voting, emergency multisig, parameter safety bounds, and SafetyReserve payout authorization) is complex enough to warrant its own ADR and will be implemented post-PoC.

This ADR covers:

1. The PoC governance model (admin key)
2. The production governance model (OpenZeppelin Governor against `VotingEscrow`)
3. Governable parameters and their hardcoded safety bounds (including the `FeeRouter` shares and `boostFloor`)
4. Emergency multisig design
5. SafetyReserve payout authorization rules

## Decision

### PoC: Admin Key

A single deployer address (EOA or multisig) has admin rights on all contracts. Can update any parameter. No voting, no timelock.

### Production: ve-Weighted Governance

Based on OpenZeppelin Governor, sourcing voting weight from `VotingEscrow` (per [ADR 026](026-gauge-boost-tokenomics.md) §4):

| Parameter | Value |
| --- | --- |
| Voting source | `VotingEscrow.balanceOfAt(user, ts)` |
| Voting weight | ve-balance — equals `amount × remaining_lock_time / 4y`, decaying linearly to zero at lock expiry |
| Proposal threshold | 0.1% of total ve-supply at proposal snapshot |
| Voting period | 7 days |
| Quorum | 4% of total ve-supply at proposal snapshot |
| Timelock | 48 hours between vote passing and execution |
| Vote delegation | Supported — Governor Bravo delegation pattern, applied to ve-balance |

Quorum and proposal threshold are calibrated against `VotingEscrow.totalSupplyAt(ts)`, **not** total TOKEN supply. ve-supply tracks active commitment rather than passive holdings; calibrating against it avoids the failure mode where the quorum bar trivially exceeds engaged voting power as TOKEN circulates.

Traders, passive holders, and any TOKEN that has not been locked into `VotingEscrow` carry zero voting weight. Operator stake held in `StakingRegistry` is also not voting weight; per [ADR 026](026-gauge-boost-tokenomics.md) §4, operator stake and ve-positions are independent contracts with disjoint roles.

**Delegation.** ve-balance is delegatable using the Governor Bravo `delegate(address)` pattern: the underlying ve-position remains non-transferable (per [ADR 026](026-gauge-boost-tokenomics.md) §4) but its voting weight may be assigned to another address for the purpose of casting votes. The delegate's vote weight at proposal snapshot equals the sum of `VotingEscrow.balanceOfAt(delegator, ts)` over all delegators that have delegated to them, plus the delegate's own ve-balance if not delegated elsewhere. Delegation is revocable at any time and takes effect at the next snapshot.

#### Bootstrap consideration

Under [ADR 026](026-gauge-boost-tokenomics.md), vesting contracts release TOKEN unlocked and locking into `VotingEscrow` is opt-in (no auto-ve-lock-on-vest). Consequently the early ve-supply is concentrated in self-locked seed/team/treasury positions plus POL/airdrop recipients who choose to lock, and is materially smaller than it would be under an auto-lock model. Quorum measured against `totalSupplyAt` is robust to this — a small ve-supply means a small absolute quorum bar — but the population of distinct lockers may be too thin to resist concentration. **Recommendation:** the protocol treasury should fund a ve-lock-on-claim airdrop (sourced from the community / ecosystem allocation or pre-seed) during the first 6–12 months post-launch, structured so participants receive TOKEN only by locking it in `VotingEscrow`. Sizing is open and tracked in the [ADR 026](026-gauge-boost-tokenomics.md) source design spec's open-question list.

### Governable Parameters with Safety Bounds

All economic parameters across the protocol are governable within hardcoded safety bounds. Safety bounds are immutable — even a governance attack cannot set parameters outside these ranges.

#### FeeRouter shares and boost floor (per [ADR 026](026-gauge-boost-tokenomics.md) §11)

The six `FeeRouter` shares and the `boostFloor` parameter are governable within the bounds below. **Sum-to-100% invariant:** every governance update that modifies any share **must** leave the six shares summing to exactly 100% (10000 bps); updates that violate the sum or that exceed any individual bound revert at the contract layer.

| Parameter | Default | Min | Max |
| --- | ---: | ---: | ---: |
| Node base share | 40% | 20% | 80% |
| Gauge boost share | 40% | 0% | 60% |
| Delegator share | 7% | 0% | 30% |
| Burn share | 5% | 0% | 25% |
| Treasury share | 5% | 0% | 20% |
| Safety share | 3% | 0% | 15% |
| `boostFloor` | 0.4 | 0.2 | 0.8 |

The 20% floor on the node-base share is the cashflow invariant defined in [ADR 016 §FeeRouter (production)](016-contract-interactions.md#feerouter-production); the rationale is load-bearing for governance and is not re-derived here. The `boostFloor` bounds prevent governance from collapsing the gauge pool to a winner-take-all distribution (lower bound) or flattening it into uselessness (upper bound). See [ADR 026](026-gauge-boost-tokenomics.md) §3 for the full gauge-boost formula.

#### Other protocol parameters

| Parameter | Contract | Min | Max |
| --- | --- | --- | --- |
| Minimum stake | StakingRegistry | 100 TOKEN | 100,000 TOKEN |
| Slash percentage (per offense) | StakingRegistry | 5% | 50% |
| Unbonding period | StakingRegistry | 3 days | 30 days |
| Multiaddr update cooldown | StakingRegistry | 0 (disabled) | 86400 seconds (1 day) |
| Max multiaddr size | StakingRegistry | 64 bytes | 1024 bytes |
| Dispute window (PoC default: 48h) | StablePaymentChannel (PoC) / PaymentChannel (production) | 12 hours | 72 hours (3 days) |
| Rate floor/ceiling | StablePaymentChannel (PoC) / PaymentChannel per-token (production, [ADR 010](010-multi-token.md)) | Floor ≥ 1 base unit | Ceiling > floor |
| Max voucher interval | StablePaymentChannel (PoC) / PaymentChannel (production) | 1 MB | 1024 MB (~1 GB) |
| Min deposit | StablePaymentChannel (PoC) / PaymentChannel (production) | 1 base unit | No max |
| Challenge bond | SlashJudge | 1 TOKEN | 1,000 TOKEN |
| Base slash reset period | StakingRegistry | 30 days | 365 days |
| Compliance window | ContentBlacklist | 1 hour | 7 days |
| Minimum origin redundancy | OriginAssignment | 1 | 10 |
| Assignment timelock | OriginAssignment | 24 hours | 14 days |
| Max origins per namespace | OriginAssignment | 1 | 50 |
| Default-open min redundancy | OriginAssignment | 5 | 500 |
| Default-open max origins | OriginAssignment | 20 | 500 |
| Max namespaces per publisher | PublisherRegistry | 1 | 1000 |
| Namespace transfer timelock | PublisherRegistry | 24 hours | 30 days |
| Max evidence age | SlashJudge | 1 day | 30 days |
| MMR retention buffer | FeeRouter | 1 day | 30 days |

There is no settlement-time fee skim or discount-stake mechanic on the channel contract — accordingly there are no "Protocol fee %" / "Discounted fee %" / "Burn percentage of fees" parameters. Burn is a fixed share of the [ADR 026](026-gauge-boost-tokenomics.md) `FeeRouter` split (governable within the burn-share bound above).

The 7-day voting period balances responsiveness with participation. Combined with the 48-hour timelock, the total governance delay is 9 days minimum — longer than the standard OpenZeppelin Governor defaults, reflecting ve-weighted participation cadence.

Staking and slashing parameters are defined in [ADR 026 §7-§8](026-gauge-boost-tokenomics.md#7-operator-economics-and-minimum-stake); fee-routing parameters are governed by [ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds). Payment channel parameters are defined in [ADR 003](003-payments.md). This ADR defines the governance mechanism that controls them.

**Treasury disbursement:** Spending from the protocol treasury wallet (the destination of the 5% `FeeRouter` treasury share per [ADR 026](026-gauge-boost-tokenomics.md) §2) — including operational expenditures, ecosystem grants, audits, and any onward transfers — requires a standard governance proposal in production. During the PoC, the admin key holder directs treasury spending. The emergency multisig cannot withdraw treasury funds (see [Emergency Multisig](#emergency-multisig)); its only fund-movement authority is the SafetyReserve fast-track path, which is bounded by hard caps and the four payout gates.

**Safety bound rationale:**

- **Slash 5%–50% per offense:** A 1% slash is economically negligible and provides no deterrence. A 100% single-offense slash enables governance to fully confiscate stake, which is disproportionate and discourages staking. The 5%–50% range ensures each individual slash is meaningful but not existential. Full ejection (effectively 100% loss) is still possible through **cumulative** slashing: three offenses at the production schedule (5% + 15% + 50% = 70% cumulative) trigger auto-ejection when stake drops below the 50% threshold ([ADR 026 §8](026-gauge-boost-tokenomics.md#8-slashing-and-burn)). To prevent gaming the escalation reset (misbehaving once per reset period to always receive the minimum penalty), each lifetime offense increases the clean period required to drop one escalation tier (90 → 180 → 360 days). The reset period multiplier schedule (1×/2×/4×) is hardcoded, not governable, to prevent governance from flattening the anti-gaming curve; only the base reset period is governable.
- **Base slash reset period 30–365 days:** These bounds apply to the **base** reset period only. The base period (default 90 days) is multiplied by a hardcoded factor derived from lifetime offense count (1×/2×/4×). A 30-day minimum prevents governance from making the base reset trivially short (re-enabling gaming). A 365-day maximum on the base period prevents effectively permanent escalation while keeping the system governable; under the fixed multiplier schedule this implies a maximum **effective** reset period of up to 1,460 days (4 × 365) when lifetime offenses ≥ 3.
- **Rate floor ≥ 1 base unit:** A zero floor allows free-riding nodes that advertise zero rates to attract traffic without generating protocol fees. The minimum of 1 base unit of the payment token (e.g., $0.000001/MB for 6-decimal USDC) is negligibly small but prevents true zero-rate abuse. For the PoC this is 1 USDC base unit; in production, `addToken` enforces a per-token floor ≥ 1 base unit at token registration time ([ADR 010](010-multi-token.md)).
- **Dispute window 12h–72h:** A 30-minute window is too short for the local in-process dispute monitor (or any third-party fraud detector) to respond to a stale close. A 7-day window locks client funds for an unacceptably long period. The 12h–72h range balances responsiveness with fund liquidity. The PoC deploys at 48 hours to guarantee 24 hours of effective dispute response time under worst-case L2 sequencer censorship (forced inclusion delay ≤ 24h). Governance must not set the dispute window below the chosen L2's maximum forced-inclusion delay — on an L2 with ~24h forced inclusion, the 12h floor is not safe (see [ADR 003](003-payments.md) and [Appendix: Fraud Detection](appendix-fraud-detection.md)). The 12h floor remains for L2s with shorter forced-inclusion paths.
- **Min deposit floor ≥ 1 base unit:** prevents dust channels that cost more in gas to settle than they contain.
- **Minimum origin redundancy 1–10:** Lower bound of 1 keeps the system useful for hobbyist publishers who genuinely have only one server. Upper bound of 10 prevents governance from setting a floor so high that it locks small publishers out of the registered-namespace path entirely. The default (3) is a typical durability target — enough that single-node failure or maintenance does not deny availability — and is easy for content owners to satisfy without disproportionate infrastructure cost ([ADR 011 § Origin Assignment Authority](011-content-takedown.md#origin-assignment-authority)). **Cross-parameter invariant:** `OriginAssignment` enforces `1 ≤ minRedundancy ≤ maxOriginsPerNamespace` at the contract layer on every `setMinRedundancy` / `setMaxOriginsPerNamespace` call; updates that would violate this revert. Without the cross-check, governance could set `minRedundancy` above `maxOriginsPerNamespace` (or shrink the max below the existing min) and brick the contract — no proposal could ever satisfy both bounds.
- **Assignment timelock 24 hours–14 days:** Lower bound of 24 hours ensures publishers and the broader community have at least one full business day to surface concerns about a proposed origin set (e.g., one of the proposed operators is a known bad actor). Upper bound of 14 days prevents governance from making assignments effectively unusable through delay; 14 days is consistent with the existing 7-day voting period plus a ratification buffer.
- **Max origins per namespace 1–50:** Upper bound prevents storage-cost griefing on the contract (each origin entry consumes storage) and unbounded gas in `getOrigins` view calls. Lower bound of 1 supports default-redundancy-of-1 use cases when set together with `minRedundancy = 1`.
- **Default-open min redundancy 5–500 (default 10) and Default-open max origins 20–500 (default 100):** The default-open allow-list ([ADR 011 § Default-open allow-list](011-content-takedown.md#default-open-allow-list)) authorizes operators to serve as origin for *any* unclaimed hash, so its surface area is the entire long tail rather than a single publisher's content set. The redundancy floor is materially higher than the registered-namespace floor (default 10 vs 3) because under-redundant default-open is a network-wide availability problem, not a per-publisher one; the upper bound of 500 ensures governance can always satisfy the floor even if the cap is set tight. The cap is correspondingly larger than the per-registered cap (default 100, max 500) to allow geographic and operator-class diversity at the cost of bounded `getOrigins(0)` view gas. **Cross-parameter invariants:** `OriginAssignment` enforces `5 ≤ defaultOpenMinRedundancy ≤ defaultOpenMaxOrigins ≤ 500` and `defaultOpenMinRedundancy ≥ minRedundancy` at the contract layer on every relevant setter; updates that would violate either revert. The first invariant prevents the same brick-the-contract failure mode as the registered floor / cap pair; the second pins the default-open floor at or above the registered floor so the broader-impact set always has at least the redundancy of an individual registered namespace.
- **Max namespaces per publisher 1–1000:** Anti-squatting limit. Lower bound of 1 supports the minimal case (one publisher, one namespace). Upper bound of 1000 is large enough that no realistic content owner is constrained but small enough that namespace registration cannot be used as a denial-of-service vector against the registry.
- **Namespace transfer timelock 24 hours–30 days:** Lower bound prevents instant key-compromise transfers (a stolen publisher key cannot immediately exfiltrate a valuable namespace before the legitimate owner can react). Upper bound prevents governance from making transfers so slow that legitimate ownership changes (acquisitions, key rotations) become operationally infeasible.

### SafetyReserve Payout Authorization

The 3% `SafetyReserve` bucket introduced by [ADR 026](026-gauge-boost-tokenomics.md) §5 is a governance-gated incident reserve, not a passive yield source. Payouts cover SLA-failure compensation, incorrect-slashing reversals, relay/sequencer/payment-channel downtime, and bad-data incidents — see [ADR 026](026-gauge-boost-tokenomics.md) §5 for the full eligibility list.

`SafetyReserve.payout(bundle, recipient, amount)` checks all four of the following gates; absence of any of them causes the call to revert. There is no path for unattested or unreviewed payouts.

1. **Attested incident bundle.** The caller must supply a cryptographic evidence bundle identifying the failure mode, the harmed party, and the proposed payout amount. Bundle attestation rules and accepted evidence types are tracked in the [ADR 026](026-gauge-boost-tokenomics.md) source design spec.
2. **Authorization.** Either (a) a successful governance proposal that authorizes the specific bundle, or (b) emergency-multisig fast-track approval — the multisig may execute payouts under hard caps (per-incident and per-rolling-window USDC ceilings configured at deploy time and immutable thereafter; see [Emergency Multisig](#emergency-multisig)). Multisig fast-track is intended for time-critical incidents (active SLA breaches, ongoing outages) and does not bypass the other three gates.
3. **48-hour appeal window.** After authorization, the bundle enters a 48-hour on-chain appeal window during which any party may submit a counter-bundle challenging the original. Successful challenges revert the authorization. The appeal window cannot be shortened (including by emergency multisig); only the gate-2 authorization step has a fast path.
4. **Post-incident reporting.** On payout settlement, `SafetyReserve` writes an immutable record to its public on-chain registry (incident hash, payout amount, recipient, authorization path used, links to the evidence bundle and any successful appeals). Operators of the registry MUST publish a human-readable post-incident report referencing the on-chain record; the registry tracks completion of these reports and exposes outstanding-report counts as a public metric.

The emergency multisig's fast-track authority over gate 2 is constrained by the same hard caps the multisig is bound by elsewhere in this ADR — it cannot withdraw treasury funds and cannot bypass the appeal window. Cross-reference [ADR 026](026-gauge-boost-tokenomics.md) §5 for the canonical rules; this ADR is the authority for the multisig's role in fast-track authorization and the immutable hard caps.

### Emergency Multisig

- 3-of-5 multisig with known, trusted signers
- Capabilities (exhaustive list):
  1. **Pause contracts** — halt all contract execution for exploit response and critical bug mitigation
  2. **Emergency content blacklisting** — add hashes and origin operators to the `ContentBlacklist` contract via `emergencyAdd` and `emergencyAddOrigin` (see [ADR 011](011-content-takedown.md))
  3. **Regional body suspension** — suspend a compromised regional governance body via `suspendRegionalBody` (see [ADR 011](011-content-takedown.md)); must be ratified or reversed by governance within 14 days
  4. **SafetyReserve fast-track authorization** — authorize gate 2 of a `SafetyReserve.payout` flow under immutable hard caps (per-incident and per-rolling-window USDC ceilings); does **not** bypass the evidence-bundle, 48-hour appeal, or post-incident-reporting gates (see [SafetyReserve Payout Authorization](#safetyreserve-payout-authorization))
- Cannot change parameters, withdraw funds (other than gated SafetyReserve payouts), or bypass governance for non-emergency actions
- Used for exploit response, critical bug mitigation, and time-critical content removal (e.g., CSAM, actively-exploited material)
- Sunset: `pauseDeadline = deployTimestamp + 365 days` is hardcoded in the constructor as an immutable value. After the deadline, `pause()` reverts with `"PauseExpired"`. Emergency blacklisting capability follows the same sunset schedule (`blacklistDeadline = deployTimestamp + 365 days`). **Extension mechanism:** governance cannot modify the immutable deadline. To extend pause/blacklist capability, governance must deploy a new contract version with a new deadline and migrate via the standard contract upgrade path (timelock + governance vote). This ensures the sunset cannot be silently extended — a new deployment is a visible, auditable event.
- Signers should be geographically and organizationally diverse

## Consequences

**Positive:**

- Hardcoded safety bounds on all governable parameters limit the damage a governance attack can cause
- Emergency multisig provides rapid exploit response without giving any party unilateral control over funds or parameters
- Sunset clause on the multisig prevents permanent centralization
- PoC can operate with a simple admin key; governance contracts are additive post-PoC

**Negative:**

- ve-weighted governance shifts capture risk from large TOKEN holders to large ve-lockers; safety bounds limit damage but cannot prevent rent-seeking within allowed parameter ranges (e.g., setting the gauge-boost share to the 60% maximum). Operators who lock heavily for gauge boost (per [ADR 026](026-gauge-boost-tokenomics.md) §3) also accumulate disproportionate governance weight; this concentration is partially offset by team / seed / treasury vesting acting as a counterweight during the first ~3 years.
- 7-day voting period + 48-hour timelock means 9 days minimum to respond to non-emergency issues via governance
- Governance participation typically skews low; 4% quorum (against ve-supply) may be difficult to reach consistently, especially during the thin-ve-supply bootstrap window where the absolute quorum bar is small but the population of distinct lockers is also small
- Without auto-ve-lock-on-vest ([ADR 026](026-gauge-boost-tokenomics.md) §1), early ve-supply is concentrated in self-locked seed/team/treasury and POL/airdrop participants who choose to lock; a treasury-funded ve-lock-on-claim airdrop is recommended in the first 6–12 months to broaden the active voter base
- Regulatory risk: governance voting rights may contribute to TOKEN being classified as a security in some jurisdictions (see also [ADR 026](026-gauge-boost-tokenomics.md))
