# ADR 009: Governance Model

**Date:** 2026-03-29
**Status:** Draft

## Context

[ADR 004](004-tokenomics.md) defines the dual-currency token model — staking, slashing, fee discounts, buyback mechanics, and node unit economics. These are the economic primitives that the PoC implements.

Governance — how protocol parameters are changed, who can change them, and what safety mechanisms exist — is a separate concern. During the PoC, governance is a single admin key. Production governance (token-weighted voting, emergency multisig, parameter safety bounds) is complex enough to warrant its own ADR and will be implemented post-PoC.

This ADR covers:

1. The PoC governance model (admin key)
2. The production governance model (OpenZeppelin Governor)
3. Governable parameters and their hardcoded safety bounds
4. Emergency multisig design
## Decision

### PoC: Admin Key

A single deployer address (EOA or multisig) has admin rights on all contracts. Can update any parameter. No voting, no timelock.

### Production: Token-Weighted Governance

Based on OpenZeppelin Governor:

| Parameter | Value |
| --- | --- |
| Voting token | TOKEN (staked or unstaked) |
| Proposal threshold | 100,000 TOKEN (0.01% of supply) |
| Voting period | 3 days |
| Quorum | 4% of total supply |
| Timelock | 2 days between vote passing and execution |
| Vote delegation | Supported |

### Governable Parameters with Safety Bounds

All economic parameters across the protocol are governable within hardcoded safety bounds. Safety bounds are immutable — even a governance attack cannot set parameters outside these ranges.

| Parameter | Contract | Min | Max |
| --- | --- | --- | --- |
| Protocol fee % | StablePaymentChannel | 0% (0 bps) | 20% (2000 bps) |
| Minimum stake | StakingRegistry | 100 TOKEN | 100,000 TOKEN |
| Slash percentages | StakingRegistry | 5% | 50% |
| Unbonding period | StakingRegistry | 3 days | 30 days |
| Multiaddr update cooldown | StakingRegistry | 0 (disabled) | 86400 seconds (1 day) |
| Max multiaddr size | StakingRegistry | 64 bytes | 1024 bytes |
| Dispute window | StablePaymentChannel | 12 hours | 72 hours (3 days) |
| Rate floor/ceiling | StablePaymentChannel | Floor ≥ 1 base unit | Ceiling > floor |
| Max voucher interval | StablePaymentChannel | 1 MB | 1024 MB (~1 GB) |
| Challenge bond | StakingRegistry | 1 TOKEN | 1,000 TOKEN |
| Burn percentage of fees | StablePaymentChannel | 0% | 100% |

Staking, slashing, and fee parameters are defined in [ADR 004](004-tokenomics.md). Payment channel parameters are defined in [ADR 003](003-payments.md). This ADR defines the governance mechanism that controls them.

**Safety bound rationale:**
- **Slash 5%–50%:** A 1% slash is economically negligible (10 TOKEN at minimum stake) and provides no deterrence. A 100% slash enables governance to fully confiscate stake, which is disproportionate for minor offenses and discourages staking. The 5%–50% range ensures slashing is meaningful but not existential for a single offense.
- **Rate floor ≥ 1 base unit:** A zero floor allows free-riding nodes that advertise zero rates to attract traffic without generating protocol fees. The minimum of 1 USDC base unit ($0.000001/MB) is negligibly small but prevents true zero-rate abuse.
- **Dispute window 12h–72h:** A 30-minute window is too short for watchtowers or human operators to respond to a stale close. A 7-day window locks client funds for an unacceptably long period. The 12h–72h range balances responsiveness with fund liquidity.
- **Min deposit floor ≥ 1 base unit:** prevents dust channels that cost more in gas to settle than they contain.

### Emergency Multisig

- 3-of-5 multisig with known, trusted signers
- Capabilities (exhaustive list):
  1. **Pause contracts** — halt all contract execution for exploit response and critical bug mitigation
  2. **Emergency content blacklisting** — add hashes and origin operators to the `ContentBlacklist` contract via `emergencyAdd` and `emergencyAddOrigin` (see [ADR 011](011-content-takedown.md))
- Cannot change parameters, withdraw funds, or bypass governance for non-emergency actions
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

- Token-weighted governance is vulnerable to large-holder capture; safety bounds limit damage but cannot prevent rent-seeking within allowed parameter ranges (e.g., setting protocol fee to the 20% maximum)
- 3-day voting period + 2-day timelock means 5 days minimum to respond to non-emergency issues via governance
- Governance participation typically skews low; 4% quorum may be difficult to reach consistently
- Regulatory risk: governance voting rights may contribute to TOKEN being classified as a security in some jurisdictions (see also [ADR 004](004-tokenomics.md))
