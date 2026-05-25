# ADR 018: Liquidity Strategy (Balancer 80/20 POL)

**Date:** 2026-05-25 (rewrite under [spec v2.1 — work-token redesign](../docs/superpowers/specs/2026-05-24-work-token-tokenomics-redesign-v2.1.md))
**Status:** Draft

## Context

> **[ADR 026](026-tokenomics.md#adr-026-tokenomics) alignment.** Under v2.1, this ADR's `BuybackBurner` is router-driven (25% of routed USDC at settlement, not manual treasury transfers). The pool design is unchanged — Balancer V3, 80/20 TOKEN/USDC, 1% swap fee, MEV protection via TWAP + `minTokenOut` + Flashbots-style private-RPC routing + per-epoch liquidity cap, POL custody by the Timelock. **The genesis liquidity allocation grows to 19% of supply under v2.1** (4pp Market-Maker partner + 15pp Protocol-Owned Liquidity), up from 10% in the prior tokenomics. The 15pp POL growth absorbs the 15pp redeployed from the retired Liquidity Mining Rewards bucket.

Buyback execution is deferred to production because thin TOKEN/USDC pool liquidity at genesis cannot absorb buyback flow without unacceptable slippage. [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model) recommends TWAP execution as MEV mitigation but does not specify how pool depth is created.

Questions answered by this ADR:

1. How is the 19% genesis liquidity-provision allocation ([ADR 026 § Allocation](026-tokenomics.md#allocation)) actually deployed?
2. Is liquidity mercenary (LP rewards / liquidity mining) or protocol-owned?
3. Which venue and pool type — Uniswap V3 concentrated, Uniswap V2 full-range, Balancer weighted, or other?
4. How does buyback execution interact with the pool to avoid self-inflicted price impact?
5. Who can rebalance / withdraw POL, and how do POL trading-fee earnings flow?

The treasury holds the 15pp Protocol-Owned Liquidity allocation (150M of the 1B supply) and is USDC-poor at PoC scale. The 4pp Market-Maker partner allocation (40M TOKEN) is distributed to vetted MM partners (e.g., GSR, Wintermute, Auros, Flowdesk) under standard MM agreements for two-sided quoting on CEXes and DEX aggregators and is genesis-liquid. The small protocol team has no bandwidth to operate a concentrated-liquidity keeper stack; this favours a venue that minimizes USDC requirements and operational burden.

## Decision

Use a Balancer V3 weighted pool (80% TOKEN / 20% USDC, 1% swap fee) as the canonical TOKEN/USDC venue. Seed it as Protocol-Owned Liquidity (POL) from the 15pp Protocol-Owned Liquidity allocation. The DAO treasury holds the BPT (Balancer pool token) directly; no LP rewards, no liquidity mining, no dedicated `LiquidityManager` contract.

> deCDN uses Balancer **V3** for its new Vault architecture; the affirmative security justification is in [§ Consequences](#consequences). The 80/20 weighted-POL decision is a property of weighted pools generally and is not version-specific.

### Venue: Balancer V3 weighted pool vs Uniswap V3 concentrated

| Factor | Balancer V3 (80/20 weighted) | Uniswap V3 (concentrated) |
| --- | --- | --- |
| USDC required to pair 150M TOKEN at $0.01 anchor | ~$375K | ~$1.5M (50/50 range) [^v3-range] |
| Operational burden | None — set weights once | Active range management, keeper infra, rebalance transactions |
| Behavior when price exits anticipated range | Pool continues trading across full curve | Position becomes 100% one asset, earns zero fees |
| IL for a 2× price move | ~3.3% (80/20) | ~5.7% (50/50); position may be fully converted if out of range |
| MEV protection for buybacks | TWAP + `minTokenOut` + **mandatory Flashbots-style private-RPC routing** + per-epoch liquidity cap (per [ADR 026](026-tokenomics.md#adr-026-tokenomics) hardening); CoW Swap batch-auction routing as an independent conditional add-on, pending verification that CoW solvers route through Balancer V3 weighted pools (see Consequences) | Requires custom TWAP + private mempool (Flashbots Protect) |
| Fee capture per TVL (active conditions) | Lower | Higher (if well-managed and in-range) |
| Aggregator routing density on Arbitrum | Lower, and V3-specific (V3 ecosystem is newer than V2; aggregator coverage still maturing) | Higher |
| Security surface | Balancer V3 Vault architecture (transient accounting via `unlock`/settle, unified scaling); standard Weighted Pools are the simplest V3 pool type with no hooks | Uniswap V3 core — extensively audited and battle-tested |

[^v3-range]: The $1.5M V3 figure assumes a near-spot range of roughly equivalent effective depth to the Balancer 80/20 position at the same anchor price — an order-of-magnitude comparison, not an exact requirement. V3 USDC-side requirements depend entirely on the chosen range width, which this ADR does not fix.

The first three rows dominate the decision for a TOKEN-rich, USDC-poor treasury with a small team. Uniswap V3's wins (fee capture, routing density) presuppose active range management the team cannot provide during PoC and early production.

**Weights rationale.** 80/20 TOKEN-heavy seeds the pool with ~1/4 the USDC of a 50/50 position while retaining comparable near-spot depth for small trades. It also reduces IL for a given TOKEN price move by ~1.75× vs 50/50 (at 2×: ~3.3% vs ~5.7%), aligning the DAO's LP position with the protocol's upside thesis. Derivation: `IL = r^w_TOKEN / (w_TOKEN·r + w_USDC) − 1`; at `r=2`, `w_TOKEN=0.8` gives `2^0.8 / 1.8 − 1 ≈ −3.27%` and `w_TOKEN=0.5` gives `√2 / 1.5 − 1 ≈ −5.72%`.

**Fee tier rationale.** 1% suits a long-tail asset with limited trading activity. Lower tiers (0.3%, 0.05%) assume volume sufficient to compensate LPs, which TOKEN will not have at PoC scale.

### Protocol-Owned Liquidity mechanics

- **Source:** The 15pp Protocol-Owned Liquidity allocation (150M TOKEN per [ADR 026 § Allocation](026-tokenomics.md#allocation)) funds the TOKEN side. The USDC side is drawn from the pre-seed USDC bootstrap (~30% of the $1M+ pre-seed pool, ~$300K nominal — sized larger than the prior 10%-POL design's USDC seed because the TOKEN-side allocation is ~1.5× larger).
- **PoC seed:** A minimal position for integration testing. Exact amount is chosen by governance at deployment; the target range of $5K–$20K USDC equivalent per side reflects expected PoC scale.
- **Production seed:** Sized so that a single `maxBuybackAmount` swap causes less than `slippageBps` price impact, making buyback execution well-conditioned on its own pool.
- **Custody:** BPT is held by the DAO treasury address. PoC: admin key. Production: Governor + `TimelockController`. No withdraw path to an EOA — liquidity exit requires a governance proposal through the timelock. This invariant is enforced by BPT being held at the Timelock address and the absence of any bespoke withdraw function; Balancer has no protocol-level lockup, so custody discipline is the sole enforcement mechanism.
- **No `LiquidityManager` contract.** A weighted pool's curve handles rebalancing implicitly via arbitrage. There is no range to manage, no `rebalance()` keeper, no `KEEPER_ROLE` for liquidity operations.
- **No liquidity mining.** The retired Liquidity Mining Rewards bucket (15pp under prior designs) is redeployed as additional POL under v2.1. Mercenary LPs exit when rewards stop and consume TOKEN supply for a benefit POL provides more reliably. A future ADR may reintroduce LM if external-LP-attraction strategic priority changes, but the v2.1 design is intentionally LM-free for regulatory cleanliness (no per-holder passive yield).

### POL Governance (resolves spec Open Q #4 and #8)

The 19% combined POL+MM allocation is on the high end for general DeFi (typical 5–15%) and is large enough that governance controls on its operation are load-bearing.

**Rebalance authority.** The 80/20 weight is fixed at pool creation per Balancer V3 weighted-pool semantics; the curve handles intra-pool rebalancing via arbitrage. Governance may *change the pool* (deploy a new pool with different weights, migrate POL there) only via a standard governance proposal under the 48-hour timelock. There is no per-block rebalance keeper.

**Withdraw authority.** POL withdraw — partial or full removal of BPT from the Timelock-custodied position — requires:

1. A standard governance proposal authorizing the specific withdraw amount and recipient address.
2. The 48-hour timelock delay (no fast-track path, even for emergency multisig).
3. **Per-rolling-window withdraw cap:** at most 10% of the POL position may be withdrawn in any 30-day window via a single proposal. Larger withdraws require multiple proposals spaced ≥30 days apart. This bound is `immutable` — even a supermajority cannot wholesale-exit POL without an extended series of timelocked votes, making any "pull the rug" path externally observable months in advance.

Permitted operations within these bounds:

- **Top-up:** depositing additional USDC or TOKEN to deepen the position. Permissionless (anyone may add liquidity to the public Balancer pool); the protocol may execute via governance proposal.
- **Partial withdraw to fund subsidy programs:** capped at 10% per 30-day window, governance-authorized, recipient must be the protocol treasury or a treasury-controlled address.
- **Venue migration:** deploying a new pool and migrating POL into it requires governance, with the 10%/30-day withdraw cap binding the migration speed.

**Trading-fee accounting.** POL earns trading fees from third-party swaps against the pool (and from the protocol's own buyback swaps). Accrued fees flow to the Timelock-custodied BPT position; they are **not** re-routed through `FeeRouter` (this preserves `FeeRouter`'s strict per-byte-settlement accounting and resolves Open Q #8 — POL trading-fee yield is treasury-direct revenue). Governance may withdraw accrued trading fees to the treasury via the same 10%/30-day-cap proposal path. The expected baseline yield from POL trading fees at PoC scale is modest (under-trafficked pool); the strategic purpose of POL is depth and price stability, not yield.

**Why POL-heavy is consistent with work-token.** POL is *protocol-owned*; it doesn't create passive yield to any external holder. Trading-fee yield flows to treasury (operator-governed); no individual holder receives passive returns. The 19% combined allocation is defensible for an infrastructure protocol focused on liquidity depth as a primary value-accrual lever, comparable to Olympus Pro / OHM-style POL-heavy strategies but without the rebase mechanics that made those problematic. Removing LM eliminates the Howey-prong-4 exposure that an LP-token-yield program would carry.

### Buyback inflow source and rate (router-driven per [ADR 026](026-tokenomics.md#adr-026-tokenomics))

Under v2.1, `BuybackBurner` no longer relies on manual treasury transfers. The `FeeRouter` contract receives the full operator USDC balance from `PaymentChannel.settleChannel` at every settlement and atomically forwards **25% of routed USDC directly to `BuybackBurner` in the same transaction**, alongside the other three buckets (60% operator, 10% treasury, 5% safety). The full router split is in [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split).

**Implications for this ADR:**

- **Inflow source.** USDC arrives at the `BuybackBurner` address from the `FeeRouter` via a same-transaction `transfer` inside `routeSettlement`. Manual treasury transfers as the funding pattern are obsolete.
- **Inflow rate is 5× the prior design** (25% under v2.1 vs 5% under the previous tokenomics). This is the load-bearing change for the per-epoch liquidity cap below: at 5× volume, the cap is binding much earlier under sustained network revenue and the operator must size `maxBuybackAmount`, `subSwapCount`, and the cap fraction accordingly. The keeper schedule (TWAP cadence) is also recalibrated against this rate.
- **Mechanics unchanged.** The Balancer V3 80/20 pool, the swap call pattern, the TWAP + `minTokenOut` defense, the POL custody model, and the per-transaction `maxBuybackAmount` cap all carry over unmodified. Only the source of USDC and its rate change.

### Buyback execution via Balancer V3

The canonical `IBuybackBurner` interface is defined in [ADR 003 — BuybackBurner](003-payments.md#buybackburner) and is not duplicated here to avoid cross-ADR drift.

The core `executeBuyback(uint256 amount, uint256 minTokenOut)` call pattern is **unchanged** for the Balancer V3 venue (the swapped-in asset is USDC, fixed at deployment). This ADR adds one configuration setter, `setPool(address pool)`, so the same interface selects a venue-specific pool identifier symmetrically across Balancer V3 and any future Uniswap V3 deployment. Deployment configuration for the Balancer V3 venue:

- `setSwapRouter(address)` is set to the router contract used for the **Balancer V3 Vault** on the production L2. On Arbitrum mainnet this is `0xEAedc32a51c510d35ebC11088fD5fF2b47aACF2E`. Balancer's deployment registry labels this contract **`Router v2`** — that label is the second iteration of the Balancer V3 Router artifact (a contract versioning within Balancer V3), **not** a reference to Balancer V2 protocol routing. Sepolia and other testnet addresses MUST be pulled from [`balancer-deployments/addresses/arbitrum-sepolia.json`](https://github.com/balancer/balancer-deployments) at deployment time and not hardcoded in this ADR.
- `setPool(address)` is set to the contract address of the deployed 80/20 TOKEN/USDC Weighted Pool. V3 identifies pools by contract address directly; the V2 `bytes32 poolId` abstraction is gone. The setter is venue-symmetric — a Uniswap V3 deployment would hold the V3 pool contract address here. The pool address MAY also be provided via the `BuybackBurner` constructor (see [ADR 016 § Constructor Dependencies](016-contract-interactions.md#deployment-order-and-initialization-dependencies)); constructor-time and setter-time semantics are equivalent, and `executeBuyback()` MUST revert if the configured pool address is the zero address.
- **Approvals footgun — the contract self-approves the Vault.** The `BuybackBurner` contract itself MUST issue `USDC.approve(vaultAddress, type(uint256).max)` (or a bounded allowance refreshed via an admin function) as part of its deployment / initialization sequence. The approval target is the **Balancer V3 Vault** address (`0xbA1333333333a1BA1108E8412f11850A5C319bA9` on Arbitrum mainnet), *not* the Router, even though `executeBuyback()` *calls* the Router. This "call the Router, but approve the Vault" split is the single most common V2→V3 integration mistake.

#### Single-swap (non-TWAP) execution

When `subSwapCount = 1`, `executeBuyback()` calls `Router.swapSingleTokenExactIn(pool, USDC, TOKEN, amount, minTokenOut, deadline, false, "")` on the Balancer V3 Router with the stored `pool` address, `tokenIn = USDC`, `tokenOut = TOKEN`, `exactAmountIn = amount` (from the caller), `minAmountOut = minTokenOut` (the caller-supplied sandwich guard applied directly), `wethIsEth = false`, and `userData = ""`. The existing `maxBuybackAmount` and `slippageBps` guards apply unchanged — `amount` MUST be ≤ `maxBuybackAmount`.

#### TWAP policy (`subSwapCount > 1`)

Balancer's smoother curve reduces the need for TWAP versus V3's concentrated bands but does not eliminate it for large buybacks. TWAP + `minTokenOut` + **mandatory private-RPC routing** + **per-epoch liquidity cap** form the MEV defense stack. CoW Swap (below) is a conditional add-on, not a substitute. When splitting:

- **`minTokenOut` is the aggregate minimum** TOKEN output across the full buyback request, not a per-sub-swap minimum. The contract MUST track cumulative TOKEN received and MUST ensure the final cumulative output is ≥ `minTokenOut` for the split request to be satisfied. A naive per-sub-swap guard of `minTokenOut / subSwapCount` is rejected because integer truncation allows the aggregate to fall below the caller's requested minimum.
- **Per-sub-swap guard** is derived from remaining required output and remaining sub-swaps: `remainingMinOut = minTokenOut − cumulativeReceived`; `perSubSwapLimit = ceilDiv(remainingMinOut, remainingSubSwaps)`.
- **`maxBuybackAmount` is a per-transaction cap.** Each sub-swap (its own transaction) MAY be as large as `maxBuybackAmount`; the *total* buyback is bounded by the caller-supplied `amount`, not by `maxBuybackAmount × subSwapCount`.
- **Spacing.** Sub-swaps are separated by at least `subSwapMinBlockGap` blocks.
- **Required: Flashbots-style private-RPC routing.** The keeper MUST submit every buyback sub-swap through a private-RPC bundle endpoint that bypasses the public mempool — Flashbots Protect, MEV-Share, or the equivalent on the production L2. Public-mempool routing is **not** an acceptable fallback: programmatic buybacks are a known MEV target (front-run-and-dump around `executeBuyback` calls), and TWAP + `minTokenOut` alone do not prevent a searcher from observing the pending transaction and skewing the pool against the protocol within the slippage budget. This is a hard requirement; the keeper configuration MUST refuse to publish to a public RPC.
- **Required: per-epoch liquidity cap.** Independent of `maxBuybackAmount` (a per-transaction cap), `BuybackBurner` MUST enforce a per-epoch aggregate cap on the USDC notional swapped through the Balancer V3 pool across all buyback executions in a single 1-week epoch. The cap is sized as a fixed fraction of in-pool USDC depth at epoch start, so even a worst-case sustained-execution profile cannot exhaust pool depth. With the v2.1 5× volume increase, this cap is much more frequently binding than under the prior design; sizing must be conservative. The fraction (`epochLiquidityCapFraction`, default 10%, bounded `[1%, 30%]`) is governance-tunable through the 48-hour timelock per [ADR 009](009-governance.md#adr-009-governance-model).
- **Conditional add-on: CoW Swap routing.** The keeper MAY route the buyback through CoW Swap instead of calling the Router directly, **if CoW solvers route through the deployed Balancer V3 weighted pool at best-execution time**. **Before enabling CoW routing in production, the operator MUST verify via CoW's `/api/v1/quote` endpoint that the target pool is reachable and that quoted prices are within `slippageBps` of the Router-direct path.** If CoW routing is unavailable or regressed, fall back to direct Router + TWAP + private-RPC.
- **Partial execution.** If a later sub-swap reverts, earlier sub-swaps stand and their output is retained; residual USDC remains in the contract until the next execution. The original split request MUST NOT be treated as having satisfied `minTokenOut` unless cumulative output across its sub-swaps meets the aggregate minimum.

### Parameter Table

| Parameter | Value |
| --- | --- |
| Venue (`setSwapRouter`) | Balancer V3 Router (address on L2) |
| Token approvals spender | Balancer V3 Vault (distinct from Router) |
| Pool identifier (`pool`, `address`) | Deployed pool contract address |
| Pool weights | 80% TOKEN / 20% USDC |
| Pool swap fee | 1% (100 bps) |
| POL TOKEN-side allocation | 150M (15% of supply) |
| Initial pool seed size | Sized so `maxBuybackAmount` causes < `slippageBps` impact (~$300K nominal USDC seed at launch) |
| `minBuybackAmount` | 100 USDC |
| `maxBuybackAmount` | Set so a single swap causes < `slippageBps` impact |
| `slippageBps` | 200 bps (2%) |
| TWAP `subSwapCount` | 4 (default) [^subswap-rationale] |
| TWAP `subSwapMinBlockGap` | 10 blocks (~2 minutes on Arbitrum) |
| Private-RPC routing | **Required** — Flashbots Protect / MEV-Share equivalent on the production L2; public-mempool routing is not an acceptable fallback |
| Per-epoch liquidity cap | **Required** — `epochLiquidityCapFraction` of in-pool USDC depth at epoch start (default 10%, bounded `[1%, 30%]` per [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds)) |
| POL withdraw cap | 10% of POL per 30-day window per governance proposal |
| Execution activation | Disabled at launch — governance vote required to enable |

Per-parameter governability (which parameters are mutable, by whom, and within what safety bounds) lives in [ADR 026 § Governable parameters with safety bounds](026-tokenomics.md#governable-parameters-with-safety-bounds).

[^subswap-rationale]: A default of 4 balances MEV mitigation against gas overhead and keeper complexity. 2 sub-swaps provides marginal splitting benefit; ≥8 multiplies keeper gas and `ceilDiv` rounding artifacts without proportionate MEV improvement on a weighted pool. With the v2.1 5× volume increase governance may consider raising the default to 6–8; this is left as production tuning.

### Activation Criteria (Production)

Buyback execution should be enabled by governance vote only when all of the following hold:

1. The Balancer V3 80/20 TOKEN/USDC Weighted Pool has been deployed and seeded with POL at the production seed size, and its contract address has been set via `setPool(address)`.
2. The `BuybackBurner` contract has been configured with the Balancer V3 Router address via `setSwapRouter(address)`, and USDC has been approved to the Balancer V3 **Vault** address (not the Router — see the approvals footgun in [Buyback execution via Balancer V3](#buyback-execution-via-balancer-v3)).
3. A single swap of size `maxBuybackAmount` against the pool causes less than `slippageBps` price impact.
4. Accumulated USDC in the `BuybackBurner` contract exceeds `minBuybackAmount`.
5. Either (a) a keeper with `KEEPER_ROLE` is operational and configured to call `executeBuyback()` on a schedule, or (b) governance is prepared to trigger executions manually.
6. **Required: private-RPC routing in place.** The keeper MUST be configured to submit through Flashbots Protect or the equivalent private-bundle endpoint on the production L2 before execution is enabled. A pre-flight check that public-mempool publication is refused MUST be part of activation runbook sign-off.
7. **Required: per-epoch liquidity cap configured.** The per-epoch liquidity cap MUST be set on-chain to a value calibrated against epoch-start in-pool USDC depth and against the v2.1 5× inflow rate.
8. **(If CoW routing is used as an add-on)** The operator has verified via CoW's `/api/v1/quote` endpoint that CoW solvers route through the deployed Balancer V3 pool and that quoted prices are within `slippageBps` of the Router-direct path. If this fails, disable CoW routing and fall back to direct Router + TWAP + private-RPC; this does not block activation.

Criteria 1–4 are quantitative; governance voters verify them off-chain before enabling execution. Criterion 2 is a one-time deployment check. Criteria 6 and 7 are hard structural requirements; criterion 8 is venue-integration health.

## Consequences

### Positive

- Seeds pool with ~1/4 the USDC of an equivalent 50/50 V3 position — critical for a USDC-poor treasury.
- Zero keeper / range-management operational burden. No `LiquidityManager` contract, no `KEEPER_ROLE` for liquidity operations.
- IL profile (80/20 weighted) aligns with the protocol's TOKEN-upside thesis.
- TWAP + `minTokenOut` guards provide deterministic on-chain MEV protection. Combined with the mandatory private-RPC routing and per-epoch liquidity cap, programmatic-buyback front-running is closed off both by transaction-graph invisibility and execution shape.
- Balancer V3's new Vault architecture specifically mitigates the bug class exploited in V2 (see Negative).
- `IBuybackBurner` remains venue-agnostic — switching or adding venues later is a deployment-time configuration change, not an interface change.
- POL is non-extractable by external LPs because there are no external LPs. The DAO cannot be rugged by mercenary liquidity leaving at the worst moment.
- **v2.1 POL-heavy strategy is regulatorily clean.** No Liquidity Mining program means no per-holder passive yield from holding LP tokens. POL trading-fee yield flows treasury-direct; there is no participant who earns TOKEN passively. This is the design's affirmative posture on Howey prong 4 (no income "solely from the efforts of others").
- **POL Governance bounds** (10% / 30-day withdraw cap; immutable) make any "rug pull" path months-long and externally observable — much stronger custody discipline than typical DAO-controlled positions.

### Negative

- Fee capture per dollar of TVL is lower than a well-managed V3 concentrated position.
- Aggregator routing density for Balancer V3 pools on Arbitrum is lower than for Uniswap V3 (and materially lower than for Balancer V2). V3 is newer; aggregator coverage and solver integrations are still maturing. A transient concern that should improve as V3 ages.
- **Security — realized V2 event, V3 shorter track record.** On 2025-11-03, Balancer V2 Composable Stable Pools were exploited for ~$125M across multiple chains. Root cause (per Certora, Trail of Bits, OpenZeppelin post-mortems): a rounding-direction bug in `_upscale`/`_downscale` latent since 2021. The exploit affected V2 Composable Stable Pools specifically, **not** V2 standard Weighted Pools and **not** Balancer V3. Certora explicitly cites V3's new Vault architecture as mitigating this bug class. This ADR uses V3 (not V2) and further restricts the choice to **standard V3 Weighted Pools with no hooks**. **Residual risk:** V3 has been in production materially less time than V2 and has fewer integration-hours behind its audits. The protocol team MUST track Balancer security advisories.
- Treasury bears impermanent loss directly. With the 19% allocation under v2.1 (vs 10% prior), absolute IL exposure scales accordingly. Buyback-and-burn provides an indirect reward loop (fees → buyback → TOKEN appreciation → LP position value), and the 5× higher burn flow under v2.1 strengthens that loop. Worst-case IL exposure is bounded by the 15% Protocol-Owned Liquidity allocation plus the paired USDC seed.
- One new interface method (`setPool(address)`) must be added to `IBuybackBurner` — a minor extension, but must be reflected in the [ADR 003](003-payments.md#adr-003-payment-model) interface block. V3's approvals footgun must also be documented in deployment runbooks.
- **Mandatory private-RPC dependency.** A third-party operational dependency on the chosen private-bundle provider for the production L2 — provider downtime or de-listing of the protocol's bundles is a new failure mode. Mitigated by selecting a provider with a strong uptime track record and keeping the keeper code provider-agnostic.
- **5× higher buyback flow vs prior design.** The per-epoch liquidity cap is binding more frequently; pool depth and cap sizing must scale with revenue growth or burns will queue. This is the principal operational risk introduced by v2.1's 25% burn share.
- **Smaller external-LP base in year 1 vs an LM-enabled design.** v2.1 forgoes the LM subsidy that would have attracted external LPs and broadened the holder base. POL provides depth; external LP growth depends on organic trading-fee yield alone, which is modest at PoC scale. This is the deliberate cost of the cleanest regulatory posture.
- **POL accumulation can be politically charged.** A 15pp treasury-controlled LP position is large relative to the 1B fixed supply. Governance discipline on the 10%/30-day withdraw cap matters; a supermajority intent on dismantling POL faces a months-long, externally-observable process — but the cap can in principle be reduced via the same governance path (subject to its own immutability constraint: the cap *itself* is immutable, so changing it requires a contract redeployment, which IS observable and slow).

## References

- [ADR 003 — Payment Channels (IBuybackBurner interface)](003-payments.md#buybackburner)
- [ADR 009 — Governance Model](009-governance.md#adr-009-governance-model)
- [ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md#adr-016-smart-contract-interaction-model)
- [ADR 026 — Tokenomics](026-tokenomics.md#adr-026-tokenomics) — source of the four-bucket FeeRouter split (60/25/10/5), the 25% buyback share, the 19% POL allocation, the POL governance bounds, and the v2.1 work-token framing
