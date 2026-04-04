# ADR 018: Liquidity Strategy (Balancer 80/20 POL)

**Date:** 2026-04-04
**Status:** Draft

## Context

[ADR 004](004-tokenomics.md) identifies thin TOKEN/USDC pool liquidity as the reason buyback execution is deferred to production (see [ADR 004 — BuybackBurner Contract](004-tokenomics.md#buybackburner-contract) and [ADR 004 Consequences](004-tokenomics.md#consequences)). [ADR 016](016-contract-interactions.md) recommends TWAP execution as MEV mitigation but does not specify how pool depth is created in the first place.

Prior ADRs named "Uniswap V3" as the DEX venue without comparing alternatives. That choice was implicit, not reasoned. This ADR makes the venue decision explicit and supersedes those mentions.

Concretely, the following questions were unanswered before this ADR:

1. How is the 10% genesis `Liquidity (DEX + CEX)` allocation ([ADR 004 Token Distribution](004-tokenomics.md#token-distribution-production)) actually deployed into the pool?
2. Is liquidity mercenary (LP rewards / liquidity mining) or protocol-owned?
3. Which venue and pool type — Uniswap V3 concentrated, Uniswap V2 full-range, Balancer weighted, or other?
4. How does buyback execution interact with the pool to avoid self-inflicted price impact?

The treasury is TOKEN-rich (200M TOKEN bootstrap fund, 100M TOKEN liquidity allocation) and USDC-poor — at PoC scale there is no meaningful USDC reserve to pair 50/50 against TOKEN. The protocol team is small and does not have bandwidth to operate a concentrated-liquidity keeper stack. Both constraints favour a venue that minimizes USDC requirements and operational burden.

## Decision

**Use a Balancer V2 weighted pool (80% TOKEN / 20% USDC, 1% swap fee) as the canonical TOKEN/USDC venue. Seed it as Protocol-Owned Liquidity (POL) from the 10% genesis liquidity allocation. The DAO treasury holds the BPT (Balancer pool token) directly; no LP rewards, no liquidity mining, no dedicated `LiquidityManager` contract.**

This reverses the implicit Uniswap V3 choice in prior ADRs.

### Venue: Balancer V2 weighted pool vs Uniswap V3 concentrated

| Factor | Balancer V2 (80/20 weighted) | Uniswap V3 (concentrated) |
| --- | --- | --- |
| USDC required to pair 100M TOKEN at $0.01 anchor | ~$250K | ~$1M (50/50 range) [^v3-range] |
| Operational burden | None — set weights once | Active range management, keeper infra, rebalance transactions |
| Behavior when price exits anticipated range | Pool continues trading across full curve | Position becomes 100% one asset, earns zero fees |
| IL for a 2× price move | ~3.3% (80/20) | ~5.7% (50/50); position may be fully converted if out of range |
| MEV protection for buybacks | Native via CoW Swap batch-auction routing (Balancer weighted pools are a supported CoW Swap settlement venue) | Requires custom TWAP + private mempool (Flashbots Protect) |
| Fee capture per TVL (active conditions) | Lower | Higher (if well-managed and in-range) |
| Aggregator routing density on Arbitrum | Lower | Higher |
| Security surface | Balancer Vault architecture; weighted pools are among the simpler/safer Balancer pool types | Uniswap V3 core — extensively audited and battle-tested |

[^v3-range]: The $1M V3 figure assumes a near-spot range of roughly equivalent effective depth to the Balancer 80/20 position at the same anchor price. It is an order-of-magnitude comparison, not an exact requirement — V3 USDC-side requirements depend entirely on the chosen range width, which this ADR does not fix.

The first three rows dominate the decision for a TOKEN-rich, USDC-poor treasury with a small team. Uniswap V3's wins (fee capture, routing density) presuppose active range management the protocol team cannot provide during the PoC and early production.

**Weights rationale.** 80/20 TOKEN-heavy lets the treasury seed the pool with ~1/4 the USDC of a 50/50 position while retaining comparable near-spot depth for small trades. It also reduces IL for a given TOKEN price move by roughly 1.75× compared to 50/50 (at a 2× price move: ~3.3% vs ~5.7%), which aligns the DAO's LP position with the protocol's own upside thesis — if TOKEN appreciates with network usage, the DAO keeps more of the upside. Derivation: `IL = r^w_TOKEN / (w_TOKEN·r + w_USDC) − 1`; at `r=2`, `w_TOKEN=0.8` gives `2^0.8 / 1.8 − 1 ≈ −3.27%` and `w_TOKEN=0.5` gives `√2 / 1.5 − 1 ≈ −5.72%`.

**Fee tier rationale.** 1% is appropriate for a long-tail asset with limited trading activity. Lower tiers (0.3%, 0.05%) assume volume sufficient to compensate LPs, which TOKEN will not have at PoC scale.

### Protocol-Owned Liquidity mechanics

- **Source:** The genesis `Liquidity (DEX + CEX)` allocation (10%, 100M TOKEN — see [ADR 004 Token Distribution](004-tokenomics.md#token-distribution-production)) funds the TOKEN side. The USDC side is drawn from the treasury.
- **PoC seed:** A minimal position for integration testing. Exact amount is chosen by governance at deployment; the target range of $5K–$20K USDC equivalent per side reflects expected PoC scale (20–100 nodes). The range is wide because the point estimate depends on observed PoC traffic once nodes come online; deployment scripts should accept the seed size as a parameter rather than hardcoding it.
- **Production seed:** Sized so that a single `maxBuybackAmount` swap causes less than `slippageBps` price impact. This makes buyback execution well-conditioned on its own pool.
- **Custody:** BPT is held by the DAO treasury address. PoC: admin key. Production: Governor + `TimelockController`. No withdraw path to an EOA — liquidity exit requires a governance proposal through the timelock. This invariant is enforced by BPT being held at the Timelock address and by the absence of any bespoke withdraw function; Balancer itself has no protocol-level lockup, so custody discipline is the sole enforcement mechanism.
- **No `LiquidityManager` contract.** A weighted pool's curve handles rebalancing implicitly via arbitrage. There is no range to manage, no `rebalance()` keeper, no `KEEPER_ROLE` for liquidity operations. The only privileged operation on the BPT position is emergency withdrawal by governance, which uses the standard timelock path — no bespoke contract required.
- **No liquidity mining in v1.** Mercenary LPs exit when rewards stop and consume TOKEN supply for a benefit that POL provides more reliably. Liquidity mining can be introduced by a future ADR if organic pool depth proves insufficient despite POL seeding.

### Buyback execution via Balancer

The canonical `IBuybackBurner` interface is defined in [ADR 003 — BuybackBurner](003-payments.md#buybackburner); that block is the single source of truth and is not duplicated here to avoid cross-ADR drift.

The core `executeBuyback(address token, uint256 amount, uint256 minTokenOut)` call pattern is **unchanged** for the Balancer venue. What this ADR adds is one additional configuration setter, `setPoolId(bytes32 poolId)`, so the same interface can select a venue-specific pool identifier symmetrically across Balancer and any future Uniswap V3 deployment. Deployment configuration for the Balancer venue:

- `setSwapRouter(address)` is set to the **Balancer V2 Vault address** on the production L2 (Arbitrum).
- `setPoolId(bytes32)` is set to the `poolId` of the deployed 80/20 TOKEN/USDC weighted pool. The setter is venue-symmetric — a future Uniswap V3 deployment could either reuse this setter with the pool address cast to `bytes32` or introduce a distinct setter; that choice is left to the future ADR and is not forward-committed here.
- USDC must be approved to the Vault address before the first `executeBuyback()` call (standard Balancer pattern — approvals are to the Vault, not to the pool).

**Single-swap (non-TWAP) execution.** When `subSwapCount = 1`, `executeBuyback()` calls `Vault.swap(SingleSwap, FundManagement, limit, deadline)` once with `poolId` from storage, `assetIn = USDC`, `assetOut = TOKEN`, `kind = GIVEN_IN`, `amount` from the caller, and `limit = minTokenOut` (the caller-supplied sandwich guard applied directly). The existing `maxBuybackAmount` and `slippageBps` guards apply unchanged — `amount` MUST be ≤ `maxBuybackAmount`.

**TWAP policy (`subSwapCount > 1`).** Balancer's smoother curve reduces the need for TWAP compared to V3's concentrated bands but does not eliminate it for large buybacks. When splitting:

- **`minTokenOut` is the aggregate minimum** TOKEN output required across the full buyback request, not a per-sub-swap minimum. The contract MUST track cumulative TOKEN received and MUST ensure the final cumulative output is ≥ `minTokenOut` for the split request to be considered satisfied. A naive per-sub-swap guard of `minTokenOut / subSwapCount` is rejected because integer truncation allows the aggregate to fall below the caller's requested minimum.
- **Per-sub-swap guard** is derived from the remaining required output and remaining sub-swaps: `remainingMinOut = minTokenOut − cumulativeReceived`; `perSubSwapLimit = ceilDiv(remainingMinOut, remainingSubSwaps)`. This keeps the aggregate bound tight even under uneven per-sub-swap fills.
- **`maxBuybackAmount` is a per-transaction cap**, consistent with its definition at [ADR 016 — BuybackBurner reentrancy row](016-contract-interactions.md#buybackburner). Each sub-swap (being its own transaction) MAY be as large as `maxBuybackAmount`; the *total* buyback is bounded by the caller-supplied `amount`, not by `maxBuybackAmount × subSwapCount`. Dividing the per-transaction cap by the split count would unnecessarily restrict the protocol's ability to process accumulated fees.
- **Spacing.** Sub-swaps are separated by at least `subSwapMinBlockGap` blocks.
- **Alternative: CoW Swap routing.** The keeper MAY route the buyback through CoW Swap instead of calling the Vault directly. CoW provides batch-auction MEV protection natively and routes through the Balancer pool when it is best-execution. CoW routing is the recommended production path.
- **Partial execution.** If a later sub-swap reverts (e.g., its derived `remainingMinOut` cannot be satisfied), earlier sub-swaps stand and their output is retained; residual USDC remains in the contract until the next execution. The original split request MUST NOT be treated as having satisfied `minTokenOut` unless cumulative output across its sub-swaps meets the aggregate minimum.

### Parameter Table

| Parameter | PoC | Production | Governable |
| --- | --- | --- | --- |
| Venue | Balancer V2 Vault (address on L2) | Balancer V2 Vault (address on L2) | Yes (via `setSwapRouter`) |
| Pool identifier (`poolId`, `bytes32`) | Deployed pool's `poolId` | Deployed pool's `poolId` | Yes (via `setPoolId`) |
| Pool weights | 80% TOKEN / 20% USDC | 80% TOKEN / 20% USDC | No (fixed at pool creation) |
| Pool swap fee | 1% (100 bps) | 1% (100 bps) | Governable on the Balancer pool itself |
| PoC seed size | $5K–$20K USDC equivalent per side | N/A | N/A |
| Production seed size | N/A | Sized so `maxBuybackAmount` causes < `slippageBps` impact | Via governance proposal (treasury disbursement) |
| `minBuybackAmount` | N/A (execution disabled) | 100 USDC | Yes |
| `maxBuybackAmount` | N/A (execution disabled) | Set so a single swap causes < `slippageBps` impact | Yes |
| `slippageBps` | N/A (execution disabled) | 200 bps (2%) | Yes |
| TWAP `subSwapCount` | 1 (no splitting) | 4 (default, governable) [^subswap-rationale] | Yes |
| TWAP `subSwapMinBlockGap` | N/A | 10 blocks (~2 minutes on Arbitrum) | Yes |

[^subswap-rationale]: A default of 4 balances MEV mitigation against gas overhead and keeper complexity. 2 sub-swaps provides marginal splitting benefit; ≥8 multiplies keeper gas and `ceilDiv` rounding artifacts without proportionate MEV improvement on a weighted pool (where curvature is already smoother than V3 concentrated bands). Governance may tune this once production buyback volumes are observed.
| Execution activation | Disabled — fees accumulate only | Governance vote required to enable | — |

### Activation Criteria (Production)

Supersedes the activation criteria list in [ADR 004 — BuybackBurner Contract](004-tokenomics.md#buybackburner-contract). Buyback execution should be enabled by governance vote only when all of the following hold:

1. The Balancer 80/20 TOKEN/USDC pool has been seeded with POL at the production seed size.
2. A single swap of size `maxBuybackAmount` against the pool causes less than `slippageBps` price impact.
3. Accumulated USDC in the `BuybackBurner` contract exceeds `minBuybackAmount`.
4. Either (a) a keeper with `KEEPER_ROLE` is operational and configured to call `executeBuyback()` on a schedule, or (b) the governance process is prepared to trigger executions manually.

Criteria 1–3 are quantitative; governance voters verify them off-chain before enabling execution.

### Why not Uniswap V3, and when to revisit

Uniswap V3 concentrated liquidity remains a reasonable choice *if and when*:

- The treasury has sufficient USDC reserves to seed a 50/50 concentrated range without depleting operational runway, and
- The team has bandwidth to operate a range-management keeper, and
- Fee revenue from active management materially exceeds the operational cost.

None of these hold at PoC or early production. A future ADR may introduce a supplemental V3 position alongside the Balancer pool once the treasury accumulates USDC from organic fee flow. Because `IBuybackBurner` is venue-agnostic, adding a second venue does not require changes to this ADR.

**Quantitative revisit triggers (placeholders).** Governance SHOULD consider revisiting the venue decision when any of the following hold. The specific thresholds are placeholders to be tuned in the revisiting ADR once production data is available:

- Treasury USDC reserves exceed 12 months of projected operational runway, freeing capital for a 50/50 V3 range without risking bootstrap subsidies.
- Monthly Balancer LP fee revenue on the POL position falls below a threshold of protocol fee revenue (placeholder: 5%) for a sustained period (placeholder: 3 months), indicating the pool is under-trafficked.
- The `BuybackBurner` has been unable to execute a buyback within `slippageBps` for a sustained period due to pool depth, even after POL top-ups.
- Aggregator routing or CoW Swap coverage of the Balancer pool regresses materially, reducing effective MEV protection or price discovery.

These are governance-policy guidelines, not on-chain enforcement. The revisiting ADR is responsible for setting concrete thresholds informed by production data.

## Consequences

**Positive:**

- Seeds pool with ~1/4 the USDC of an equivalent 50/50 V3 position — critical for a USDC-poor treasury.
- Zero keeper / range-management operational burden. No `LiquidityManager` contract, no `KEEPER_ROLE` for liquidity operations, no off-chain monitoring infrastructure beyond what already exists for `executeBuyback()`.
- IL profile (80/20 weighted) aligns with the protocol's TOKEN-upside thesis; the DAO keeps more of TOKEN's appreciation than a 50/50 position would.
- Native MEV protection available via CoW Swap batch-auction routing (a structural property of CoW's solver competition, independent of market-share specifics), without the protocol needing to run a private-mempool integration or custom TWAP infrastructure.
- `IBuybackBurner` remains venue-agnostic — switching or adding venues later is a deployment-time configuration change, not an interface change.
- POL is non-extractable by LPs because there are no external LPs. The DAO cannot be rugged by mercenary liquidity leaving at the worst moment.

**Negative:**

- Fee capture per dollar of TVL is lower than a well-managed V3 concentrated position. The DAO earns less from LP fees than it theoretically could.
- Aggregator routing density for Balancer pools on Arbitrum is lower than for Uniswap V3. Some traders may not find the pool via their preferred aggregator. CoW Swap mitigates this for the protocol's own buybacks but not for third-party traders.
- Balancer's Vault architecture is more complex than Uniswap V3 core, with a larger overall security surface. As of this ADR's date (2026-04-04), publicly disclosed Balancer Vault incidents have primarily affected ComposableStable, Boosted, and linear-wrapper pool families rather than standard weighted pools. This ADR relies on that historical pattern and mandates tracking Balancer security advisories before enabling buyback execution; a future advisory affecting weighted pools would require reassessing the venue choice under the revisit triggers above.
- Treasury bears impermanent loss directly. Unlike mercenary LPs who absorb IL in exchange for rewards, the DAO absorbs IL without an explicit reward stream. Buyback-and-burn provides an indirect reward loop (fees → buyback → TOKEN appreciation → LP position value), but the loop is weak at PoC scale. The worst-case IL exposure is bounded by the 10% genesis `Liquidity (DEX + CEX)` allocation plus whatever USDC the treasury chooses to pair against it (see [ADR 004 Token Distribution](004-tokenomics.md#token-distribution-production)).
- One new interface method (`setPoolId(bytes32)`) must be added to `IBuybackBurner`. This is a minor interface extension but must be reflected in the ADR 003 interface block.

## References

- [ADR 003 — Payment Channels (IBuybackBurner interface)](003-payments.md#buybackburner)
- [ADR 004 — Dual-Currency Token Model](004-tokenomics.md)
- [ADR 009 — Governance Model](009-governance.md)
- [ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md)
