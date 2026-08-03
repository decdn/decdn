# Appendix: Production L2 Deployment Target

> **This is an appendix, not a core protocol ADR.** The protocol depends on
> Arbitrum-class L2 properties (forced-inclusion delay ≤ 24h per
> [ADR 003 § L2 sequencer censorship](003-payments.md#l2-sequencer-censorship),
> gas-cost calibration consistent with
> [ADR 003 § Deposit Economics](003-payments.md#deposit-economics), Balancer V3
> Router availability per [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol)), but *which*
> Arbitrum-class L2 is a deployment decision. This appendix records the canonical
> selection (Arbitrum One) and the comparison against alternatives; a future Base,
> OP Mainnet, or other OP-Stack/Nitro deployment would require re-validating these
> property constraints.

## Context

The PoC runs on **Arbitrum Sepolia**. TOKEN is canonical on one L2 — all staking, channel settlements, and governance happen on this chain. This appendix selects the production L2.

The choice affects the criteria enumerated below. Dependent ADRs: **[ADR 003](003-payments.md#adr-003-payment-model) / [ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence)** gas estimates assume Arbitrum-class L2 fee markets; [ADR 003 § L2 sequencer
censorship](003-payments.md#l2-sequencer-censorship) assumes
forced-inclusion delay ≤ 24 hours (Arbitrum value), which lower-bounds the dispute
window; **[ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol)**'s Balancer V3 Router address (canonical: see
[ADR 018 §"Buyback execution via Balancer V3"](018-liquidity-strategy.md#buyback-execution-via-balancer-v3))
is labelled the Arbitrum mainnet address.

## Candidate Chains

Three OP-Stack / Nitro L2s evaluated: **Arbitrum One**, **Base**, **OP Mainnet**.

### Evaluation Criteria

| Factor | Relevance to deCDN |
|--------|--------------------|
| Gas costs at current fee market | Directly affects channel open/close/settle economics ([ADR 003 § Deposit Economics](003-payments.md#deposit-economics)) |
| Sequencer forced-inclusion delay | Hard lower-bound on dispute window; must exceed this value ([ADR 003 § L2 sequencer censorship](003-payments.md#l2-sequencer-censorship)) |
| Native USDC availability | Eliminates Circle bridge counterparty risk for payment channels |
| Balancer V3 deployment | [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol) requires a Balancer V3 Weighted Pool for TOKEN/USDC POL |
| Aggregator routing density | Affects buyback execution quality and CoW Swap solver availability ([ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol)) |
| DeFi ecosystem depth | Thin overall liquidity amplifies TOKEN/USDC pool slippage |
| Arbitrum Sepolia continuity | Sepolia → mainnet same-family migration avoids contract redesign |
| OpenZeppelin Governor compatibility | Governor + TimelockController deployment must be straightforward |

### Comparison Table

| Factor | Arbitrum One | Base | OP Mainnet |
|--------|-------------|------|-----------|
| **L2 architecture** | Nitro (WASM fraud proofs) | OP Stack (fault proofs, Bedrock) | OP Stack (fault proofs, Bedrock) |
| **Typical gas cost (simple transfer)** | ~$0.01–0.05 | ~$0.01–0.05 | ~$0.01–0.05 |
| **Typical gas cost (contract call)** | ~$0.05–0.15 | ~$0.05–0.15 | ~$0.05–0.15 |
| **EIP-4844 blob fee** | Yes (since Dencun; Nitro uses separate calldata compression on top) | Yes | Yes |
| **Sequencer forced-inclusion delay** | ~24 h (Arbitrum delayed inbox) | ~24 h (OP inbox) | ~24 h (OP inbox) |
| **L1 finality window** | ~7 days (Nitro fraud-proof window) | ~7 days (fault-proof window) | ~7 days (fault-proof window) |
| **Native USDC (Circle CCTP)** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Balancer V3 deployment** | ✅ Deployed | ✅ Deployed | ✅ Deployed |
| **Balancer V3 aggregator coverage** | High (1inch, Paraswap, CoW) | Medium (maturing) | Medium (maturing) |
| **Arbitrum Sepolia → mainnet migration** | ✅ Same chain family | ❌ Cross-family re-deploy | ❌ Cross-family re-deploy |
| **DeFi TVL / liquidity depth (early 2026)** | Highest among the three | Second; growing fast | Third |
| **OpenZeppelin Governor + Timelock** | Battle-tested, widely deployed | Battle-tested | Battle-tested |
| **Token bridge from L1** | Arbitrum native bridge + 3rd-party | Base native bridge + 3rd-party | OP native bridge + 3rd-party |

All three chains are viable; differentiating factors below (table holds per-chain
values — prose covers only the non-table rationale and cross-ADR consequences).

### Gas and Fee Characteristics

Post-EIP-4844 all three use blob data (Nitro adds calldata compression). The
[ADR 003 § Deposit Economics](003-payments.md#deposit-economics) gas estimates
(`openChannel` ~$0.05, `closeChannel` ~$0.10, `settleChannel` ~$0.08) are calibrated
for Arbitrum and stay accurate for 2026 fee markets on all three within an order of
magnitude.

### Sequencer Censorship and Dispute Window

[ADR 003 § L2 sequencer censorship](003-payments.md#l2-sequencer-censorship) sets
the default dispute window at **48 hours** for a 24-hour effective response window
under worst-case censorship (forced-inclusion ≤ 24 h); with all three at ~24 h, 48 h
is adequate and no [ADR 003](003-payments.md#adr-003-payment-model) parameter change is required. [ADR 009](009-governance.md#adr-009-governance-model) governance bounds
(48 h–72 h) apply identically; the 48-hour floor equals the default, so the baseline
window can only be tightened upward and never dropped below the forced-inclusion delay (≤ 24 h).

### Balancer V3 and Liquidity

Balancer V3 is deployed on all three; Arbitrum is preferred because [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol) already
embedded the Arbitrum mainnet Balancer V3 Router address (canonical:
[ADR 018](018-liquidity-strategy.md#buyback-execution-via-balancer-v3)), CoW Swap
solver coverage of Balancer V3 pools is most mature on Arbitrum One ([ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol)
requires operator CoW-routing verification pre-production, most likely to succeed on
Arbitrum), and Arbitrum's largest-of-three DeFi TVL reduces TOKEN/USDC pool slippage
for buybacks on thin early-production liquidity.

### Migration Continuity

Arbitrum Sepolia → Arbitrum One is same-family (Nitro architecture, tooling,
JSON-RPC, chain ID family); Sepolia-tested scripts/contracts work unchanged modulo
address substitution. Base or OP Mainnet would require re-testing the full
deployment pipeline against a different architecture.

## Decision

Deploy deCDN production contracts on Arbitrum One (chain ID 42161).

Rationale:

1. **Cross-ADR calibration** — Balancer V3 Router address, CoW Swap routing
   assumptions, and aggregator density ([ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol)), gas estimates ([ADR 003](003-payments.md#adr-003-payment-model)), and the
   forced-inclusion window ([ADR 003 § L2 sequencer censorship](003-payments.md#l2-sequencer-censorship)) are all calibrated
   for Arbitrum One; changing chains would require re-validating all of them.
2. **Highest DeFi liquidity depth** — thinner competition for TOKEN/USDC pool depth
   in early production; better buyback execution quality.
3. **PoC continuity** — Arbitrum Sepolia → Arbitrum One, no architecture changes.
4. **Mature ecosystem** — Governor + TimelockController, Foundry, Etherscan, and
   block-explorer APIs all well-established on Arbitrum One.

### Not Selected

**Base** — strongest alternative (rapidly growing DeFi TVL, Coinbase retail
onboarding). Re-evaluate if: Arbitrum One gas rises significantly vs Base; Base's
Balancer V3 CoW routing reaches Arbitrum parity; regulatory pressure shifts the
token's liquidity centre to Base. **OP Mainnet** — not selected; DeFi ecosystem and
aggregator coverage smaller than both for this protocol's needs.

### No Cross-Chain Channels in v1

TOKEN is canonical on Arbitrum One; staking, channel settlements, and governance all
happen there. Other-chain users bridge assets to Arbitrum One via standard ERC-20
bridges (Arbitrum native bridge, or LayerZero / Wormhole) before interacting.
Cross-chain payment channels (spanning two L2s) are explicitly excluded from v1 —
they would require atomic-swap or bridge-aware channel logic, out of scope for
initial production.

## Consequences

### Cross-ADR alignment

Canonical reference for Arbitrum-specific assumptions elsewhere in the ADR set:

| ADR | Assumption |
|-----|------------|
| [ADR 003 § Deposit Economics](003-payments.md#deposit-economics) | Gas table calibrated against Arbitrum One fee market |
| [ADR 003 § L2 Sequencer Censorship](003-payments.md#l2-sequencer-censorship) | Forced-inclusion delay ≤ 24 h (Arbitrum value) |
| [ADR 018 § Buyback execution via Balancer V3](018-liquidity-strategy.md#buyback-execution-via-balancer-v3) | Balancer V3 Router address is the Arbitrum One deployment |
| [ADR 016 § Deployment Order](016-contract-interactions.md#deployment-order-and-initialization-dependencies) | "Arbitrum mainnet" in the BuybackBurner row |

### Chain-Specific Constants

| Constant | Value | Source |
|----------|-------|--------|
| Chain ID | **42161** | Arbitrum One |
| Native token (gas) | ETH | — |
| Native USDC (CCTP) address | `0xaf88d065e77c8cC2239327C5EDb3A432268e5831` | Circle CCTP on Arbitrum One |
| Balancer V3 Vault | `0xbA1333333333a1BA1108E8412f11850A5C319bA9` | Balancer deployments registry |
| Balancer V3 Router | `0xEAedc32a51c510d35ebC11088fD5fF2b47aACF2E` | Balancer deployments registry (`Router v2`) |
| Permit2 | `0x000000000022D473030F116dDEE9F6B43aC78BA3` | Canonical Permit2 (same address on every chain deCDN targets) |
| Sequencer forced-inclusion delay | ~24 h | Arbitrum delayed inbox |
| L1 finality window | ~7 days | Nitro fraud-proof window |
| Block time | ~250 ms | Arbitrum Nitro |
| RIP-7212 Ed25519 precompile | Not deployed | Verified 2026-04 against both Arbitrum One and Sepolia |
| Testnet sibling | Arbitrum Sepolia | Used for PoC; same Nitro architecture and tooling |
| Reference gas price (early 2026) | ~$0.50 per 1M gas | Underlying rate behind the per-operation costs cited in [ADR 003](003-payments.md#adr-003-payment-model) and [ADR 014](014-on-chain-verification.md#adr-014-on-chain-verification-for-slashing-evidence) |

> **Address verification.** Addresses MUST be re-confirmed against the Arbitrum One
> deployment registry and Arbiscan at deployment time. Balancer V3 addresses correct
> as of 2026-04-08 (Balancer may add router versions). USDC is native Circle Arbitrum
> USDC — do not substitute USDC.e (bridged).

### Deployment Runbook Impact

The production deployment runbook ([ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model)) substitutes Arbitrum Sepolia addresses
for Arbitrum One mainnet equivalents at each step. No contract logic changes.

**Buyback approvals.** The Balancer V3 buyback path approves **Permit2**, never the
Vault: `BuybackBurnerBalancerV3` issues a scoped per-swap Permit2 allowance to the
Router and resets it to `0` after each swap, so no standing USDC allowance is
required or expected. Granting a standing allowance to the Vault buys nothing and is
never consumed. [ADR 018 § Buyback execution via Balancer V3](018-liquidity-strategy.md#buyback-execution-via-balancer-v3)
is authoritative for the mechanism; the canonical Permit2 address is in the
[§ Chain-Specific Constants](#chain-specific-constants) table above.

### Tokenomics Validation Requirements

[ADR 026](026-tokenomics.md#adr-026-tokenomics) adds two gas-cost surfaces to re-validate
before mainnet — economic, not architectural: they do not invalidate the Arbitrum
One choice but must be quantified on the chosen L2 with measured (not estimated)
numbers, additive to this appendix's selection criteria. Gas tables are intentionally
omitted — measure during integration testing on Arbitrum Sepolia, re-confirm against
Arbitrum One fee markets at deployment. Three MUST gates:

1. **`FeeRouter` per-settlement overhead.** Every `settleChannel` routes through
   `FeeRouter.routeSettlement(operator, bytesDelivered, amount)` — three
   `safeTransfer` legs (60/30/10 same-tx per [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split)),
   one inline write to the FeeRouter-internal `bytesPerEpoch[operator][epoch]`
   served-bytes counter (epoch derived from `block.timestamp`) consumed by `DecdnGovernor._getVotes` per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight).
   Overhead **~5–10K gas** atop the settlement tx (dominated by the `bytesPerEpoch` counter SSTOREs); at 100K settlements/year
   (medium operator) a small fraction of total cost. Aggregate per-settlement gas
   (USDC-equivalent, incl. this overhead) MUST stay within the operator P&L
   affordability bounds of [ADR 026 § Operator economics](026-tokenomics.md#operator-economics).

2. **Per-epoch keeper-call gas economics.** One TWAP-protected USDC→TOKEN swap
   keeper call per epoch via `BuybackBurner` (30% buyback-and-burn flow per
   [ADR 026 § Slashing and burn](026-tokenomics.md#slashing-and-burn) and
   [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol)) —
   at a 1-week epoch, **52+ swaps/year minimum**, via the Balancer V3 80/20
   TOKEN/USDC pool with TWAP windows, `minOut`, mandatory private-RPC routing, and
   a per-epoch liquidity cap that binds under sustained network revenue at the
   25% burn share. Per-epoch keeper costs MUST be not cost-prohibitive at S2/S3
   scale and a small fraction of the inflow each call routes.

3. **Private-RPC gate.** The L2 MUST support private-RPC routing (Flashbots-style
   bundles) for the hardened MEV-protection requirement in
   [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol) (private RPC and per-epoch liquidity caps
   both mandatory); without a viable private-RPC route the swap path's MEV defense
   is not deployable as designed.

### Re-evaluation Triggers

This decision SHOULD be revisited by governance if:

- Sustained average `settleChannel` gas on Arbitrum One exceeds **$1.00** for >30
  days (per-channel economics materially worse than alternatives).
- A critical Arbitrum fraud-proof vulnerability is disclosed and not patched within
  90 days.
- Arbitrum One sequencer censorship is demonstrated at scale (forced-inclusion delay
  exceeds 48 h in practice), invalidating the dispute-window safety margin.
- Regulatory action targets Offchain Labs specifically, creating operational risk
  for the protocol's Arbitrum One contracts.
