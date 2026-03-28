# ADR 004: Dual-Currency Token Model

**Date:** 2026-03-28
**Status:** Draft

## Context

The network needs an economic mechanism that:

1. Incentivizes edge nodes to join and behave honestly (staking with slashing)
2. Gives token holders a voice in protocol parameters (governance)
3. Pays edge nodes reliably without exposing them to asset volatility (see ADR 003)
4. Creates sustainable token demand as the network grows

A single-token model where edge nodes are paid in the native token fails constraint 3: a provider's infrastructure costs are USD-denominated, so a volatile payment token makes their P&L unpredictable. Reactive rate adjustment is insufficient — gossip propagates rate changes slowly, mid-session rate changes are impossible, and rate churn degrades user experience.

## Decision

Use a **dual-currency model**: USDC for operational payments, AUDIO (native ERC-20) for network-specific economic functions where aligned incentives matter.

| Function | Currency | Rationale |
|----------|----------|-----------|
| Delivery payments | USDC | Predictable unit economics for operators |
| Provider staking | AUDIO | Stake must appreciate with network health |
| Governance voting | AUDIO | Power reflects network commitment, not purchasing power |
| Fee discounts | AUDIO | Direct financial incentive to hold more AUDIO |
| Slashing | AUDIO | Already denominated in stake |

**Token supply:** 1B AUDIO, fixed at genesis, no post-genesis minting. Deflationary pressure comes from two sources: 100% of slashed stake is burned; 20% of protocol fees (collected in USDC) are used to buy AUDIO on the open market and burn it.

**Staking:** Edge nodes must stake a minimum of 1,000 AUDIO to register on the network. Stake is slashable only for serving data that fails BLAKE3 hash verification (provable, objective offense). Going offline or having a cache miss is not slashable — these are handled by reputation and client fallback logic. Unbonding period is 7 days; stake remains slashable during unbonding to prevent slash-then-run.

**Fee discount:** Providers staking ≥10× the minimum (10,000 AUDIO) pay a 1.5% protocol fee instead of 3%. This creates a direct financial return on holding more AUDIO and rewards long-term network commitment.

**Governance:** Token-weighted voting using OpenZeppelin Governor. All economic parameters (fee %, rate bounds, slash percentages, dispute window) are governable within hardcoded safety bounds. Safety bounds are immutable — even a governance attack cannot set fees to 100% or stake to 0.

## Consequences

**Positive:**

- AUDIO transitions from a medium of exchange (bad for volatile assets) to a productive capital asset: stake it to operate, hold more for fee discounts, vote with it
- Buyback creates continuous buy-side demand proportional to network usage — more delivery volume → more USDC fees → more AUDIO purchased and burned
- Hardcoded safety bounds on all governable parameters limit the damage a governance attack can cause
- PoC can use a freely mintable testnet token with the same contracts; no supply constraints or distribution mechanics required during development

**Negative:**

- Bootstrapping requires token demand before organic streaming revenue is sufficient; a 200M AUDIO provider bootstrap fund is allocated for this but its adequacy is unproven
- Two-token UX: edge node operators need both USDC (to fund client payment channels) and AUDIO (to stake). Client software should abstract this with integrated DEX swaps but adds complexity
- The AUDIO/USDC Uniswap pool may be thin at launch, making buyback execution sensitive to pool depth; `maxBuybackAmount` and `minAudioOut` parameters mitigate sandwich risk but require active governance attention
- Token-weighted governance is vulnerable to large-holder capture; safety bounds limit damage but cannot prevent rent-seeking within allowed parameter ranges (e.g., setting protocol fee to the 20% maximum)
- Regulatory risk: a token with staking, governance, and economic utility may be classified as a security in some jurisdictions. Legal review is required before production token distribution.
