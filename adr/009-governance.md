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
| Slash percentages | StakingRegistry | 1% | 100% |
| Unbonding period | StakingRegistry | 3 days | 30 days |
| Multiaddr update cooldown | StakingRegistry | 0 (disabled) | 86400 seconds (1 day) |
| Max multiaddr size | StakingRegistry | 64 bytes | 1024 bytes |
| Dispute window | StablePaymentChannel | 30 minutes | 7 days |
| Rate floor/ceiling | StablePaymentChannel | Floor > 0 | Ceiling > floor |
| Max voucher interval | StablePaymentChannel | 1 MB | 1024 MB (~1 GB) |
| Challenge bond | StakingRegistry | 1 TOKEN | 1,000 TOKEN |
| Burn percentage of fees | StablePaymentChannel | 0% | 100% |

Staking, slashing, and fee parameters are defined in [ADR 004](004-tokenomics.md). Payment channel parameters are defined in [ADR 003](003-payments.md). This ADR defines the governance mechanism that controls them.

### Emergency Multisig

- 3-of-5 multisig with known, trusted signers
- Can ONLY pause contracts (not change parameters or withdraw funds)
- Used for exploit response and critical bug mitigation
- Sunset: after 12 months, the pause function is permanently disabled (or requires governance vote to extend)
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
