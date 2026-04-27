# ADR 021: Production L2 Chain Selection

**Date:** 2026-04-08
**Status:** Draft

## Context

The PoC runs on **Arbitrum Sepolia**. The production chain has been explicitly deferred
since ADR 004:

> "TOKEN is canonical on one L2 (the production chain). All staking, channel settlements,
> and governance happen on this chain. This decision is deferred until after PoC
> validation." — ADR 004 § Multi-Chain

This ADR makes the deferral concrete. The choice affects:

- Gas costs and EIP-4844 data-fee characteristics
- Sequencer forced-inclusion delay (lower-bounds the dispute window — ADR 003/007)
- Balancer V3 liquidity venue deployment and router addresses (ADR 018)
- USDC availability and canonical form (bridged vs native)
- DeFi ecosystem depth for TOKEN/USDC liquidity (ADR 018)
- Bridge tooling for users moving assets from Ethereum L1

Several prior ADRs have implicitly assumed Arbitrum mainnet without stating it explicitly:

- **ADR 004** — gas estimates ("assume Arbitrum average gas price as of early 2026")
- **ADR 007** — forced-inclusion delay assumed ≤ 24 hours (Arbitrum value)
- **ADR 018** — Balancer V3 Router address `0xEAedc32a51c510d35ebC11088fD5fF2b47aACF2E`
  labelled as the Arbitrum mainnet address

Formalizing this decision resolves the ambiguity and allows remaining mainnet planning to
proceed.

## Candidate Chains

Three OP-Stack / Nitro L2s on Ethereum were evaluated: **Arbitrum One**, **Base**, and
**OP Mainnet**.

### Evaluation Criteria

| Factor | Relevance to deCDN |
|--------|--------------------|
| Gas costs at current fee market | Directly affects channel open/close/settle economics (ADR 004 gas table) |
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

Arbitrum Nitro uses WASM-based fraud proofs and its own calldata compression scheme,
historically producing lower L1 data fees than OP Stack for calldata-heavy transactions.
Post-EIP-4844 (blob transactions), all three chains use blob data for L2 state
submissions, significantly reducing L1 data costs on all candidates. The gas estimates
in ADR 004 (`stake` ~$0.05, `openChannel` ~$0.05, `closeChannel` ~$0.10,
`settleChannel` ~$0.08) are calibrated for Arbitrum and remain accurate for 2026 fee
markets on any of the three candidates within an order of magnitude.

### Sequencer Censorship and Dispute Window

ADR 003 and ADR 007 set the default dispute window at **48 hours** to guarantee a 24-hour
effective response window under worst-case sequencer censorship (forced-inclusion delay
≤ 24 h). All three candidates have a ~24 h forced-inclusion delay, so the 48-hour
dispute window is adequate on all three. No change to ADR 003 or ADR 007 parameters is
required by the chain selection.

ADR 009 governance bounds on the dispute window (12 h–72 h) apply identically across
all candidates. The 12-hour floor is unsafe on any chain where the forced-inclusion
delay exceeds 12 hours, but this is a governance guardrail issue independent of which
chain is chosen here.

### Balancer V3 and Liquidity

ADR 018 committed to a Balancer V3 80/20 TOKEN/USDC Weighted Pool as the POL venue.
Balancer V3 is deployed on Arbitrum One, Base, and OP Mainnet. However:

- ADR 018 already embedded the Arbitrum mainnet Balancer V3 Router address
  (`0xEAedc32a51c510d35ebC11088fD5fF2b47aACF2E` / `Router v2` in Balancer's registry).
- CoW Swap solver coverage of Balancer V3 pools is most mature on Arbitrum One. ADR 018
  requires the operator to verify CoW routing availability before production, but the
  probability of a successful verification is highest on Arbitrum.
- Arbitrum has the largest overall DeFi TVL of the three, reducing TOKEN/USDC pool
  slippage for buybacks on thin early-production liquidity.

### Migration Continuity

The PoC is on Arbitrum Sepolia. Deploying production on Arbitrum One is a same-family
migration: same Nitro architecture, same tooling, same JSON-RPC interface, same chain ID
family. Scripts and contracts tested on Sepolia work unchanged on Arbitrum One modulo
address substitution. Migrating to Base or OP Mainnet would require re-testing the full
deployment pipeline against a different architecture.

## Decision

**Deploy deCDN production contracts on Arbitrum One (chain ID 42161).**

Rationale:

1. **ADR 018 alignment** — the Balancer V3 Router address, CoW Swap routing assumptions,
   and aggregator density arguments in ADR 018 are calibrated for Arbitrum One. Changing
   chains would require re-validating all three.
2. **Highest DeFi liquidity depth** — thinner competition for TOKEN/USDC pool depth
   during early production; better buyback execution quality.
3. **PoC continuity** — Arbitrum Sepolia → Arbitrum One is a straight path with no
   architecture changes.
4. **Prior ADR consistency** — ADR 004 gas estimates, ADR 007 forced-inclusion window,
   and ADR 018 contract addresses all assume Arbitrum One. This ADR retroactively
   formalises those assumptions rather than forcing updates to multiple ADRs.
5. **Mature ecosystem** — Governor + TimelockController deployments, Foundry support,
   Etherscan explorer, block explorer APIs, and watchtower infrastructure are all
   well-established on Arbitrum One.

### Not Selected

**Base** is the strongest alternative. Its DeFi TVL is growing rapidly and Coinbase's
involvement gives it strong retail onboarding. It should be re-evaluated if:

- Arbitrum One gas costs increase significantly relative to Base.
- Base's Balancer V3 CoW routing coverage reaches parity with Arbitrum.
- Regulatory pressure shifts the token's liquidity centre to Base.

**OP Mainnet** is not selected. Its DeFi ecosystem and aggregator coverage are smaller
than both Arbitrum One and Base for the specific needs of this protocol.

### No Cross-Chain Channels in v1

TOKEN is canonical on Arbitrum One. Staking, channel settlements, and governance all
happen on this chain. Users on other chains use standard ERC-20 bridges (Arbitrum native
bridge or cross-chain protocols such as LayerZero / Wormhole) to move assets to Arbitrum
One before interacting. Cross-chain payment channels (channels spanning two L2s) are
explicitly excluded from v1 — they would require atomic swap or bridge-aware channel
logic that is out of scope for initial production.

## Consequences

### Updates to Prior ADRs

This ADR resolves the deferral in ADR 004 and formalises implicit assumptions in ADRs
004, 007, and 018. No numerical parameters in those ADRs change.

| ADR | Prior state | After this ADR |
|-----|-------------|----------------|
| ADR 004 § Multi-Chain | "Production L2 deferred" | Resolved: Arbitrum One |
| ADR 004 § Gas Cost Breakdown | "Assume Arbitrum" (implicit) | Explicit |
| ADR 007 § L2 Sequencer Censorship | "forced inclusion delay ≤ 24 h" | Confirmed for Arbitrum One |
| ADR 018 § Buyback execution | Arbitrum mainnet Router address embedded | Chain selection now explicit |
| ADR 016 § Deployment | "Arbitrum mainnet" in BuybackBurner row | Chain selection now explicit |

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
| Reference gas price (early 2026) | ~$0.50 per 1M gas | Underlying rate behind the per-operation costs tabulated in [ADR 004 § Gas Cost Breakdown](004-tokenomics.md#gas-cost-breakdown); ADR 003 and ADR 014 derive their estimates from the same rate |

> **Address verification.** Contract addresses MUST be re-confirmed against the
> Arbitrum One deployment registry and Arbiscan at deployment time. The Balancer V3
> addresses above are correct as of 2026-04-08; Balancer may add router versions.
> USDC address is native USDC from Circle's Arbitrum deployment — do not substitute
> USDC.e (bridged).

### Deployment Runbook Impact

The production deployment runbook (ADR 016) should substitute Arbitrum Sepolia addresses
for Arbitrum One mainnet equivalents at each step. No contract logic changes are required.

### Re-evaluation Triggers

This decision SHOULD be revisited by governance if:

- Arbitrum One sustained average gas cost for `settleChannel` exceeds **$1.00** for
  more than 30 days (makes per-channel economics materially worse than alternatives).
- A critical security vulnerability is disclosed in Arbitrum's fraud-proof system and
  not patched within 90 days.
- Arbitrum One sequencer censorship is demonstrated at scale (forced-inclusion delay
  exceeds 48 h in practice), invalidating the dispute window safety margin.
- Regulatory action targets Offchain Labs specifically and creates operational risk for
  the protocol's contracts on Arbitrum One.
