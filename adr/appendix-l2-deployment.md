# Appendix: Production L2 Deployment Target

> **This is an appendix, not a core protocol ADR.** The protocol depends on Arbitrum-class L2 properties (forced-inclusion delay ≤ 24h per [ADR 003 § L2 sequencer censorship](003-payments.md#l2-sequencer-censorship), gas-cost calibration consistent with [ADR 003 § Deposit Economics](003-payments.md#deposit-economics), Balancer V3 Router availability per [ADR 018](018-liquidity-strategy.md)), but the choice of *which* Arbitrum-class L2 is a deployment decision. This appendix records the canonical selection (Arbitrum One) and the comparison against alternatives. A future deployment on Base, OP Mainnet, or another OP-Stack/Nitro chain would require re-validating the property constraints.

## Context

The PoC runs on **Arbitrum Sepolia**. TOKEN is canonical on one L2 — all staking, channel settlements, and governance happen on this chain. This ADR selects the production L2.

The choice affects:

- Gas costs and EIP-4844 data-fee characteristics
- Sequencer forced-inclusion delay (lower-bounds the dispute window — see [ADR 003 § L2 sequencer censorship](003-payments.md#l2-sequencer-censorship))
- Balancer V3 liquidity venue deployment and router addresses (ADR 018)
- USDC availability and canonical form (bridged vs native)
- DeFi ecosystem depth for TOKEN/USDC liquidity (ADR 018)
- Bridge tooling for users moving assets from Ethereum L1

Several other ADRs depend on the chain selection:

- **ADR 003 / ADR 014** — gas estimates assume Arbitrum-class L2 fee markets
- **ADR 003 § L2 sequencer censorship** — forced-inclusion delay assumed ≤ 24 hours (Arbitrum value)
- **ADR 018** — Balancer V3 Router address (canonical: see [ADR 018 §"Buyback execution via Balancer V3"](018-liquidity-strategy.md#buyback-execution-via-balancer-v3)) labelled as the Arbitrum mainnet address

## Candidate Chains

Three OP-Stack / Nitro L2s on Ethereum were evaluated: **Arbitrum One**, **Base**, and **OP Mainnet**.

### Evaluation Criteria

| Factor | Relevance to deCDN |
|--------|--------------------|
| Gas costs at current fee market | Directly affects channel open/close/settle economics (ADR 003 §Deposit Economics) |
| Sequencer forced-inclusion delay | Hard lower-bound on dispute window; must exceed this value (ADR 003/007) |
| Native USDC availability | Eliminates Circle bridge counterparty risk for payment channels |
| Balancer V3 deployment | ADR 018 requires a Balancer V3 Weighted Pool for TOKEN/USDC POL |
| Aggregator routing density | Affects buyback execution quality and CoW Swap solver availability (ADR 018) |
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

All three chains are viable. The differentiating factors are explored below.

### Gas and Fee Characteristics

Arbitrum Nitro uses WASM-based fraud proofs and its own calldata compression scheme, historically producing lower L1 data fees than OP Stack for calldata-heavy transactions. Post-EIP-4844 (blob transactions), all three chains use blob data for L2 state submissions, significantly reducing L1 data costs on all candidates. The gas estimates in [ADR 003 §Deposit Economics](003-payments.md#deposit-economics) (`openChannel` ~$0.05, `closeChannel` ~$0.10, `settleChannel` ~$0.08) are calibrated for Arbitrum and remain accurate for 2026 fee markets on any of the three candidates within an order of magnitude.

### Sequencer Censorship and Dispute Window

ADR 003 and ADR 007 set the default dispute window at **48 hours** to guarantee a 24-hour effective response window under worst-case sequencer censorship (forced-inclusion delay ≤ 24 h). All three candidates have a ~24 h forced-inclusion delay, so the 48-hour dispute window is adequate on all three. No change to ADR 003 or ADR 007 parameters is required by the chain selection.

ADR 009 governance bounds on the dispute window (12 h–72 h) apply identically across all candidates. The 12-hour floor is unsafe on any chain where the forced-inclusion delay exceeds 12 hours, but this is a governance guardrail issue independent of which chain is chosen here.

### Balancer V3 and Liquidity

ADR 018 committed to a Balancer V3 80/20 TOKEN/USDC Weighted Pool as the POL venue. Balancer V3 is deployed on Arbitrum One, Base, and OP Mainnet. However:

- ADR 018 already embedded the Arbitrum mainnet Balancer V3 Router address (canonical reference: [ADR 018](018-liquidity-strategy.md#buyback-execution-via-balancer-v3)).
- CoW Swap solver coverage of Balancer V3 pools is most mature on Arbitrum One. ADR 018 requires the operator to verify CoW routing availability before production, but the probability of a successful verification is highest on Arbitrum.
- Arbitrum has the largest overall DeFi TVL of the three, reducing TOKEN/USDC pool slippage for buybacks on thin early-production liquidity.

### Migration Continuity

The PoC is on Arbitrum Sepolia. Deploying production on Arbitrum One is a same-family migration: same Nitro architecture, same tooling, same JSON-RPC interface, same chain ID family. Scripts and contracts tested on Sepolia work unchanged on Arbitrum One modulo address substitution. Migrating to Base or OP Mainnet would require re-testing the full deployment pipeline against a different architecture.

## Decision

**Deploy deCDN production contracts on Arbitrum One (chain ID 42161).**

Rationale:

1. **ADR 018 alignment** — the Balancer V3 Router address, CoW Swap routing assumptions, and aggregator density arguments in ADR 018 are calibrated for Arbitrum One. Changing chains would require re-validating all three.
2. **Highest DeFi liquidity depth** — thinner competition for TOKEN/USDC pool depth during early production; better buyback execution quality.
3. **PoC continuity** — Arbitrum Sepolia → Arbitrum One is a straight path with no architecture changes.
4. **Cross-ADR consistency** — gas estimates (ADR 003), forced-inclusion window (ADR 007), and Balancer V3 contract addresses (ADR 018) are all calibrated against Arbitrum One.
5. **Mature ecosystem** — Governor + TimelockController deployments, Foundry support, Etherscan explorer, and block-explorer APIs are all well-established on Arbitrum One.

### Not Selected

**Base** is the strongest alternative. Its DeFi TVL is growing rapidly and Coinbase's involvement gives it strong retail onboarding. It should be re-evaluated if:

- Arbitrum One gas costs increase significantly relative to Base.
- Base's Balancer V3 CoW routing coverage reaches parity with Arbitrum.
- Regulatory pressure shifts the token's liquidity centre to Base.

**OP Mainnet** is not selected. Its DeFi ecosystem and aggregator coverage are smaller than both Arbitrum One and Base for the specific needs of this protocol.

### No Cross-Chain Channels in v1

TOKEN is canonical on Arbitrum One. Staking, channel settlements, and governance all happen on this chain. Users on other chains use standard ERC-20 bridges (Arbitrum native bridge or cross-chain protocols such as LayerZero / Wormhole) to move assets to Arbitrum One before interacting. Cross-chain payment channels (channels spanning two L2s) are explicitly excluded from v1 — they would require atomic swap or bridge-aware channel logic that is out of scope for initial production.

## Consequences

### Cross-ADR alignment

The chain selection here is the canonical reference for Arbitrum-specific assumptions elsewhere in the ADR set:

| ADR | Assumption |
|-----|------------|
| ADR 003 § Deposit Economics | Gas table calibrated against Arbitrum One fee market |
| ADR 007 § L2 Sequencer Censorship | Forced-inclusion delay ≤ 24 h (Arbitrum value) |
| ADR 018 § Buyback execution | Balancer V3 Router address is the Arbitrum One deployment |
| ADR 016 § Deployment | "Arbitrum mainnet" in the BuybackBurner row |

### Chain-Specific Constants

| Constant | Value | Source |
|----------|-------|--------|
| Chain ID | **42161** | Arbitrum One |
| Native token (gas) | ETH | — |
| Native USDC (CCTP) address | `0xaf88d065e77c8cC2239327C5EDb3A432268e5831` | Circle CCTP on Arbitrum One |
| Balancer V3 Vault | `0xbA1333333333a1BA1108E8412f11850A5C319bA9` | Balancer deployments registry |
| Balancer V3 Router | `0xEAedc32a51c510d35ebC11088fD5fF2b47aACF2E` | Balancer deployments registry (`Router v2`) |
| Sequencer forced-inclusion delay | ~24 h | Arbitrum delayed inbox |
| L1 finality window | ~7 days | Nitro fraud-proof window |
| Block time | ~250 ms | Arbitrum Nitro |
| RIP-7212 Ed25519 precompile | Not deployed | Verified 2026-04 against both Arbitrum One and Sepolia |
| Testnet sibling | Arbitrum Sepolia | Used for PoC; same Nitro architecture and tooling |
| Reference gas price (early 2026) | ~$0.50 per 1M gas | Underlying rate behind the per-operation costs cited in ADR 003 and ADR 014 |

> **Address verification.** Contract addresses MUST be re-confirmed against the
> Arbitrum One deployment registry and Arbiscan at deployment time. The Balancer V3
> addresses above are correct as of 2026-04-08; Balancer may add router versions.
> USDC address is native USDC from Circle's Arbitrum deployment — do not substitute
> USDC.e (bridged).

### Deployment Runbook Impact

The production deployment runbook (ADR 016) should substitute Arbitrum Sepolia addresses for Arbitrum One mainnet equivalents at each step. No contract logic changes are required.

### Tokenomics Validation Requirements

[ADR 026](026-gauge-boost-tokenomics.md) and [ADR 027](027-distinct-client-receipts.md) introduce two new gas-cost surfaces that the L2 selection in this ADR must be re-validated against before mainnet deployment. Both are economic, not architectural — they do not invalidate the Arbitrum One choice above, but they must be quantified on the chosen L2 with measured (not estimated) gas numbers prior to mainnet launch.

1. **`FeeRouter` per-settlement overhead.** Every `settleChannel` call routes through `FeeRouter.routeSettlement(operator, client, bytesDelivered, amount, epochId)`, which (a) increments an operator's per-epoch `bytesPerEpoch` counter and (b) updates `distinctClientCount` / `_seenClient` for the diversity gate per [ADR 027 §1](027-distinct-client-receipts.md#1-feerouter-per-epoch-bookkeeping). Combined overhead: **~25K gas** on top of the existing settlement transaction (2 SLOADs + 1–2 SSTOREs depending on cold/warm and first-time-seen). At 100K settlements/year for a medium operator, this is a small fraction of the total settlement-tx cost; actual cost on Arbitrum One must be measured against current fee markets rather than estimated. The aggregate per-settlement gas must remain within operator-affordability bounds — cite the operator P&L cases in [ADR 026 §7](026-gauge-boost-tokenomics.md) as the affordability reference.

2. **Per-epoch keeper-call gas economics.** The design requires **two TWAP-protected USDC→TOKEN swap keeper calls per epoch**: one for `BuybackBurner` (5% buyback-and-burn flow, inflow source per [ADR 018](018-liquidity-strategy.md)) and one for the delegator-pool USDC→TOKEN conversion (7% delegator-pool flow per [ADR 026 §6](026-gauge-boost-tokenomics.md)). At a 1-week epoch length, that is **52+ swap executions per year minimum**, both routed through the Balancer V3 80/20 TOKEN/USDC pool with TWAP windows, `minOut` bounds, and per-epoch liquidity caps. Per-epoch keeper costs must be sized at S2/S3 scale (the regimes where liquidity caps bind per [ADR 026 §8](026-gauge-boost-tokenomics.md) and the economic-model spec) and confirmed not to be cost-prohibitive against the inflow each call routes.

3. **Combined L2-selection validation gate.** Before mainnet deployment, the selection criteria in this ADR MUST additionally verify that:
   - Aggregate per-settlement gas (USDC-equivalent per settlement, including the `FeeRouter` diversity-counter overhead) is within the operator-affordability bounds set by the operator P&L cases in [ADR 026 §7](026-gauge-boost-tokenomics.md).
   - Per-epoch keeper costs for the two TWAP swaps are not cost-prohibitive at S2/S3 network scale and remain a small fraction of the inflow each call routes.
   - The chosen L2 supports private-RPC routing (Flashbots-style transaction bundles) to satisfy the hardened MEV-protection requirement in [ADR 018](018-liquidity-strategy.md) (private RPC required, per-epoch liquidity caps required — both mandatory, not optional). If the L2 lacks a viable private-RPC route, the swap path's MEV defense is not deployable as designed.

These requirements are additive to (not a replacement for) the selection criteria in this ADR. Specific gas tables are intentionally omitted here — they must be measured during contract integration testing on Arbitrum Sepolia and re-confirmed against Arbitrum One fee markets at deployment time, not estimated in advance.

### Re-evaluation Triggers

This decision SHOULD be revisited by governance if:

- Arbitrum One sustained average gas cost for `settleChannel` exceeds **$1.00** for more than 30 days (makes per-channel economics materially worse than alternatives).
- A critical security vulnerability is disclosed in Arbitrum's fraud-proof system and not patched within 90 days.
- Arbitrum One sequencer censorship is demonstrated at scale (forced-inclusion delay exceeds 48 h in practice), invalidating the dispute window safety margin.
- Regulatory action targets Offchain Labs specifically and creates operational risk for the protocol's contracts on Arbitrum One.
