# Appendix: Governance Chain and Payment Chain Admission

> **This is an appendix, not a core protocol ADR.** The protocol has two tiers. The governance chain is Ethereum L1. A payment chain is any chain that meets the admission rule below. [ADR 016 § Deployment Tiers](016-contract-interactions.md#deployment-tiers) defines the tiers. [ADR 003](003-payments.md#adr-003-payment-model) defines the payment model that runs on payment chains.

## Governance chain

Ethereum L1 holds TOKEN, the bond, slashing, governance, the treasury, and the buyback. Payments are chain-neutral, so the trust root does not depend on any one payment chain.

Uniswap V3 and native private relays are available on L1. The buyback path uses both ([ADR 018 § Buyback execution via Uniswap V3](018-liquidity-strategy.md#buyback-execution-via-uniswap-v3)).

Operators pay L1 gas for these actions:

- `registerNode` (Ed25519 verification) costs about 1M gas.
- Bond operations, slash challenges, and votes also run on the governance chain.

These are one-time or rare actions. The frequent actions, payment and redemption, run on the payment chains, where gas is cheap. Operators need gas on each payment chain where they redeem and sweep.

## Payment chain admission

A chain is a payment chain when it meets all four rules:

1. It runs the EVM.
2. It has native USDC with 6 decimals. Bridged USDC.e is not admitted.
3. It supports Circle CCTP V2 with hooks.
4. Its sequencer forced-inclusion delay is below the pool's `disputeWindow` ([ADR 003 § L2 sequencer censorship](003-payments.md#l2-sequencer-censorship)).

The ADRs hold the rule, not a list of chains. Pool constants are set per deployment and live in the deployment manifests.

## Sweep cost

Each redemption on a payment chain adds the writes that accrue the protocol share to `pending[operator]`. Whoever calls `sweep` pays the sweep gas. `minSweep` keeps dust sweeps uneconomic.

A standard CCTP transfer from an L2 reaches Ethereum in about 15 to 20 minutes. The attestation expires about 24 hours after Circle signs it, and `receiveSweep` is permissionless, so any party can receive a pending sweep in time. Circle can pause message receipt. That is the same trust root as USDC itself.

[ADR 026](026-tokenomics.md#adr-026-tokenomics) adds three gates. Measure each one in integration testing before mainnet.

1. **Per-redemption gas overhead.** The pending-accrual writes MUST keep aggregate per-redemption gas within the operator P&L bounds of [ADR 026 § Operator economics](026-tokenomics.md#operator-economics).
2. **Keeper gas on the governance chain.** One buyback keeper call runs per epoch through `BuybackBurner` ([ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-uniswap-v3-5050-pol)). Its gas MUST stay a small fraction of the inflow that the call routes.
3. **Private relay on L1.** The buyback swap path needs private-relay routing for its MEV defense.
