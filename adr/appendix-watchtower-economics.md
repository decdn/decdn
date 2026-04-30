# Appendix: Watchtower Operating Economics

> **This is an appendix, not a core protocol ADR.** The protocol mechanism — heartbeat batching, fee structure, dispute-gas bonus, and on-chain accountability — lives in [ADR 007](007-watchtower.md). This appendix contains the side-business cost model and break-even analysis that previously lived in ADR 007 §"Break-Even Economics" but are operational, not protocol-definitional.

## Context

[ADR 007 §5 Fee Model](007-watchtower.md) specifies the protocol-level fee mechanism: `max(0.1% × deposit, 0.50 USDC)` per 30-day monitoring period, plus a 2× gas-cost dispute bonus. That is normative.

How a prospective watchtower operator should reason about the *viability of running a watchtower as a business* — fixed monthly costs, channel volume needed to break even, whether the role sustains a standalone operator or only a co-located one — is operator-decision territory and changes with L2 gas markets and channel-deposit distributions. Mixing that into the protocol ADR fragments the protocol-vs-operations boundary the ADR set is trying to maintain ([README §Decision-record context](README.md#decision-record-context)).

This appendix preserves that analysis as operator-facing reference material, with the explicit caveat that the numbers are illustrative and a serious operator should re-derive them against current market conditions before committing infrastructure.

## Monthly cost model (per watchtower)

Heartbeats are batched per [ADR 007 §5 Break-Even Economics → Batched heartbeats](007-watchtower.md): one on-chain transaction per 6-hour window covers all active escrows for a given watchtower. This makes heartbeat gas cost fixed rather than per-channel.

| Cost component | Monthly estimate | Notes |
| --- | --- | --- |
| Heartbeat gas (120 batched tx) | $1.20–$2.40 | Fixed cost, amortised across all channels |
| Infrastructure (VPS + monitoring) | $20–$50 | Shared with node operation if co-located |
| Dispute gas (rare) | $0.05–$0.10 per event | Covered by 2× gas bonus from escrow |

## Break-even at various fee levels

| Average fee/channel/month | Fixed cost assumption | Channels to break even |
| --- | --- | --- |
| $0.50 (minimum, deposits ≤ 500 USDC) | $25 | ~50 |
| $1.00 (deposits ~1,000 USDC) | $25 | ~25 |
| $5.00 (deposits ~5,000 USDC) | $25 | ~5 |

## Implication

Watchtower operation is viable as a side activity for existing node operators — who already run infrastructure and monitor the chain — but unlikely to sustain a standalone business at PoC scale. This is acceptable: the PoC does not implement watchtowers (see [ADR 007 §8 PoC Scope](007-watchtower.md#8-poc-scope)), and production economics improve with channel volume and deposit sizes.

## Caveats

- L2 gas prices have moved across orders of magnitude since deCDN's design phase. The $1.20–$2.40 heartbeat-gas estimate above assumes Arbitrum One pricing in the cents-per-tx range; re-derive against current sequencer fees when sizing a real deployment.
- The break-even table assumes a 100% subscription rate against the watchtower's announced fee. In practice the watched-party side will choose between competing watchtowers on price, redundancy fit, and reputation.
- Co-located node-and-watchtower operation amortises the $20–$50 infrastructure component across both roles. A standalone watchtower carries the full cost.
- The rolling 6-hour heartbeat window is a protocol parameter (`heartbeatInterval`, default 6h, governable 1h–24h per [ADR 009](009-governance.md)). A shorter interval increases gas and tightens the break-even point; a longer interval relaxes both.

## Cross-references

- [ADR 007 — Watchtower Design (protocol mechanism)](007-watchtower.md)
- [ADR 009 — Governance, watchtower-fee-rate / heartbeat-interval bounds](009-governance.md)
- [ADR 026 — Tokenomics-level fee distribution context](026-gauge-boost-tokenomics.md)
