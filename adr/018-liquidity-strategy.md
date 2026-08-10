# ADR 018: Liquidity Strategy (Balancer 80/20 POL)

**Date:** 2026-05-27
**Status:** Draft

> **Amendment (2026-05-30, [#685](https://github.com/decdn/decdn/issues/685)).** POL TOKEN-side allocation lowered **15pp → 10pp (150M → 100M TOKEN)**; combined POL+MM Liquidity-Provision category drops **20% → 15%** (top of the 5–15% DeFi norm). MM unchanged at 5pp. Knock-ons reflected below: the 80/20 USDC seed scales down ~⅓ (~$375K → ~$250K nominal, the $0.01-anchor pairing for the smaller TOKEN side), and the per-epoch buyback liquidity cap loses ~⅓ of its absolute headroom because pool depth shrinks proportionally while the 30% router inflow is unchanged — see [§ Buyback per-epoch-cap headroom](#buyback-per-epoch-cap-headroom-685) and re-evaluated [Activation Criterion 7](#activation-criteria-production). The freed 5pp of TOKEN moved to App Incentives per [ADR 026 § Allocation](026-tokenomics.md#allocation).

> **Amendment (2026-06-13, [#686](https://github.com/decdn/decdn/issues/686)).** The concrete swap binding shipped as `BuybackBurnerBalancerV3` (a subclass of the abstract `BuybackBurner`), resolving three under-specified points detailed in the sections below:
>
> - **Router storage.** The base `BuybackBurner` is venue-neutral and stores only USDC and TOKEN. The subclass owns the whole venue set — the Router (`swapRouter`), Permit2, the Vault, and the pool — all four constructor-set. The Router and Permit2 are zero-rejected; the Vault and pool may be zero and wired later. Governance rotates the Router via `setSwapRouter` and the other two via `setVault` / `setPool`; Permit2 is immutable. The Router is the swap call target, and approvals go to **Permit2**, not the Vault ([§ Buyback execution via Balancer V3](#buyback-execution-via-balancer-v3)).
> - **On-chain `minOut` floor mechanism.** Balancer V3 ships **no built-in pool oracle** (V2's was removed) and TOKEN has no external feed at launch, so the on-chain TWAP floor is a **self-maintained cumulative-price accumulator** (Uniswap-V2 `price0CumulativeLast` style) over the pool's marginal spot, sampled before each swap, advanceable permissionlessly via `poke()`, and **fail-closed** (`executeBuyback` reverts `TwapNotReady` until the accumulator spans the governed `twapMinWindow`). Sparse updates weaken the average, so the floor is necessary-not-sufficient and stays layered under the mandatory keeper-side private-RPC routing and the per-epoch liquidity cap (§ TWAP policy).
> - **Effective minimum.** The keeper-supplied `minTokenOut` is **not** applied raw; the contract enforces `minTokenOut >= twapFloor`, so the effective minimum is `max(twapFloor, keeper minTokenOut)` (§ Single-swap execution).
> `executeBuyback` performs exactly one Router swap; the TWAP sub-swap cadence (count, block-gap, aggregate-`minOut` tracking) is keeper-orchestrated across calls per § TWAP policy.

## Context

> **[ADR 026](026-tokenomics.md#adr-026-tokenomics) alignment.** `BuybackBurner` is router-driven: it receives 30% of routed USDC same-tx from `FeeRouter` at every settlement. The pool design is Balancer V3, 80/20 TOKEN/USDC, 1% swap fee, MEV protection via TWAP + `minTokenOut` + Flashbots-style private-RPC routing + per-epoch liquidity cap, POL custody by the Timelock. **The combined Liquidity-Provision category is 15% of supply** (5pp Market-Maker partner / group 7 + 10pp Protocol-Owned Liquidity / group 3 per [ADR 026 § Allocation](026-tokenomics.md#allocation)).

Buyback execution is deferred to production because thin TOKEN/USDC pool liquidity at genesis cannot absorb buyback flow without unacceptable slippage. [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model) recommends TWAP execution as MEV mitigation but does not specify how pool depth is created.

Questions answered by this ADR:

1. How is the 15% combined Liquidity-Provision allocation ([ADR 026 § Allocation](026-tokenomics.md#allocation)) actually deployed across POL and MM?
2. Is liquidity mercenary (LP rewards / liquidity mining) or protocol-owned?
3. Which venue and pool type — Uniswap V3 concentrated, Uniswap V2 full-range, Balancer weighted, or other?
4. How does buyback execution interact with the pool to avoid self-inflicted price impact?
5. Who can rebalance / withdraw POL, and how do POL trading-fee earnings flow?

The treasury holds the 10pp Protocol-Owned Liquidity allocation (100M of the 1B supply) and is USDC-poor at PoC scale. The 5pp Market-Maker partner allocation (50M TOKEN) is distributed to vetted MM partners (e.g., GSR, Wintermute, Auros, Flowdesk) under standard MM agreements for two-sided quoting on CEXes and DEX aggregators and is genesis-liquid. The small protocol team has no bandwidth to operate a concentrated-liquidity keeper stack; this favours a venue that minimizes USDC requirements and operational burden.

## Decision

Use a Balancer V3 weighted pool (80% TOKEN / 20% USDC, 1% swap fee) as the canonical TOKEN/USDC venue. Seed it as Protocol-Owned Liquidity (POL) from the 10pp Protocol-Owned Liquidity allocation. The DAO treasury holds the BPT (Balancer pool token) directly; no LP rewards, no liquidity mining, no dedicated `LiquidityManager` contract.

> deCDN uses Balancer **V3** for its new Vault architecture; the affirmative security justification is in [§ Consequences](#consequences). The 80/20 weighted-POL decision is a property of weighted pools generally and is not version-specific.

### Venue: Balancer V3 weighted pool vs Uniswap V3 concentrated

| Factor | Balancer V3 (80/20 weighted) | Uniswap V3 (concentrated) |
| --- | --- | --- |
| USDC required to pair 100M TOKEN at $0.01 anchor | ~$250K | ~$1M (50/50 range) [^v3-range] |
| Operational burden | None — set weights once | Active range management, keeper infra, rebalance transactions |
| Behavior when price exits anticipated range | Pool continues trading across full curve | Position becomes 100% one asset, earns zero fees |
| IL for a 2× price move | ~3.3% (80/20) | ~5.7% (50/50); position may be fully converted if out of range |
| MEV protection for buybacks | TWAP + `minTokenOut` + **mandatory Flashbots-style private-RPC routing** + per-epoch liquidity cap (per [ADR 026](026-tokenomics.md#adr-026-tokenomics) hardening); CoW Swap batch-auction routing as an independent conditional add-on, pending verification that CoW solvers route through Balancer V3 weighted pools (see Consequences) | Requires custom TWAP + private mempool (Flashbots Protect) |
| Fee capture per TVL (active conditions) | Lower | Higher (if well-managed and in-range) |
| Aggregator routing density on Arbitrum | Lower, and V3-specific (V3 ecosystem is newer than V2; aggregator coverage still maturing) | Higher |
| Security surface | Balancer V3 Vault architecture (transient accounting via `unlock`/settle, unified scaling); standard Weighted Pools are the simplest V3 pool type with no hooks | Uniswap V3 core — extensively audited and battle-tested |

[^v3-range]: The ~$1M V3 figure assumes a near-spot range of roughly equivalent effective depth to the Balancer 80/20 position at the same anchor price — an order-of-magnitude comparison, not an exact requirement. V3 USDC-side requirements depend entirely on the chosen range width, which this ADR does not fix.

The first three rows dominate the decision for a TOKEN-rich, USDC-poor treasury with a small team. Uniswap V3's wins (fee capture, routing density) presuppose active range management the team cannot provide during PoC and early production.

**Weights rationale.** 80/20 TOKEN-heavy seeds the pool with ~1/4 the USDC of a 50/50 position while retaining comparable near-spot depth for small trades. It also reduces IL for a given TOKEN price move by ~1.75× vs 50/50 (at 2×: ~3.3% vs ~5.7%), aligning the DAO's LP position with the protocol's upside thesis. Derivation: `IL = r^w_TOKEN / (w_TOKEN·r + w_USDC) − 1`; at `r=2`, `w_TOKEN=0.8` gives `2^0.8 / 1.8 − 1 ≈ −3.27%` and `w_TOKEN=0.5` gives `√2 / 1.5 − 1 ≈ −5.72%`.

**Fee tier rationale.** 1% suits a long-tail asset with limited trading activity. Lower tiers (0.3%, 0.05%) assume volume sufficient to compensate LPs, which TOKEN will not have at PoC scale.

### Venue-neutral burner selection

Balancer V3 80/20 is the canonical POL venue, but the on-chain `BuybackBurner` is venue-neutral. An abstract `GuardedBuybackBurner` holds the shared MEV-defense stack — the self-maintained TWAP floor, the `slippageBps` / `minBuybackAmount` / `maxBuybackAmount` band, and the per-epoch USDC liquidity cap — and defers the venue to a concrete subclass: `BuybackBurnerBalancerV3` (Balancer V3 Vault + Router) or `BuybackBurnerUniswapV3` (Uniswap V3 `SwapRouter02` + a single V3 pool). Both subclasses take the same constructor shape, the same roles, and the same `FeeRouter` wiring, so the venue is a deploy-time choice and `FeeRouter.buybackBurner` is swappable in a single governance call with no `FeeRouter` change.

The Uniswap subclass sources its `minOut` floor from the same hand-rolled cumulative-price accumulator as the Balancer subclass (not the pool's native `observe()` oracle), so a freshly-seeded pool whose observation cardinality is still 1 prices the floor; it reads the pool's `slot0` marginal spot, its fee tier, and its in-pool USDC balance for the shared guards. The two subclasses therefore share one TWAP implementation and differ only in the venue read and the swap call.

The venue is selected at deploy time to match the target network. On the Arbitrum Sepolia initial network Uniswap V3 is deployed and Balancer V3 is not, so Uniswap V3 is the runnable venue there; the Balancer path targets networks where Balancer V3 is live. Because the burner is swappable, a network may launch on one venue and migrate to the other later through the standard `setBuybackBurner` governance call under the 48-hour timelock.

### Protocol-Owned Liquidity mechanics

- **Source:** The 10pp Protocol-Owned Liquidity allocation (100M TOKEN per [ADR 026 § Allocation](026-tokenomics.md#allocation)) funds the TOKEN side. The USDC side is sized to the **~$250K** needed to pair the full 100M TOKEN at the $0.01 anchor in the 80/20 pool (100M × $0.01 = $1M TOKEN value = 80% of pool depth ⇒ ~$250K USDC for the remaining 20%; the venue-comparison figure above), down ~⅓ from the ~$375K required under the prior 15pp / 150M POL. It is funded from the pre-seed USDC bootstrap POL-seed bucket — ~20% of the raise per [ADR 026 § Bootstrap](026-tokenomics.md#bootstrap-mechanism--pre-seed-usdc) — which spans ~$200K at the $1M floor to ~$600K at the $3M target, covering the ~$250K requirement at any raise above ~$1.25M.
- **Initial pool seed:** Sized so that a single `maxBuybackAmount` swap causes less than `slippageBps` price impact, making buyback execution well-conditioned on its own pool.
- **Custody:** BPT is held by the DAO treasury address. PoC: admin key. Production: Governor + `TimelockController`. No withdraw path to an EOA — liquidity exit requires a governance proposal through the timelock. This invariant is enforced by BPT being held at the Timelock address and the absence of any bespoke withdraw function; Balancer has no protocol-level lockup, so custody discipline is the sole enforcement mechanism.
- **No `LiquidityManager` contract.** A weighted pool's curve handles rebalancing implicitly via arbitrage. There is no range to manage, no `rebalance()` keeper, no `KEEPER_ROLE` for liquidity operations.
- **No liquidity mining.** Mercenary LPs exit when rewards stop and consume TOKEN supply for a benefit POL provides more reliably. A future ADR may reintroduce LM if external-LP-attraction strategic priority changes; the current design is intentionally LM-free for regulatory cleanliness (no per-holder passive yield).

### POL Governance

The 15% combined POL+MM allocation sits at the top of the typical 5–15% DeFi range and is large enough that governance controls on its operation are load-bearing.

**Rebalance authority.** The 80/20 weight is fixed at pool creation per Balancer V3 weighted-pool semantics; the curve handles intra-pool rebalancing via arbitrage. Governance may *change the pool* (deploy a new pool with different weights, migrate POL there) only via a standard governance proposal under the 48-hour timelock. There is no per-block rebalance keeper.

**Withdraw authority.** POL withdraw — partial or full removal of BPT from the Timelock-custodied position — requires:

1. A standard governance proposal authorizing the specific withdraw amount and recipient address.
2. The 48-hour timelock delay (no fast-track path, even for emergency multisig).
3. **Per-rolling-window withdraw cap:** at most 10% of the POL position may be withdrawn in any 30-day window via a single proposal. Larger withdraws require multiple proposals spaced ≥30 days apart. This bound is `immutable` — even a supermajority cannot wholesale-exit POL without an extended series of timelocked votes, making any "pull the rug" path externally observable months in advance.

Permitted operations within these bounds:

- **Top-up:** depositing additional USDC or TOKEN to deepen the position. Permissionless (anyone may add liquidity to the public Balancer pool); the protocol may execute via governance proposal.
- **Partial withdraw to fund subsidy programs:** capped at 10% per 30-day window, governance-authorized, recipient must be the protocol treasury or a treasury-controlled address.
- **Venue migration:** deploying a new pool and migrating POL into it requires governance, with the 10%/30-day withdraw cap binding the migration speed.

**Trading-fee accounting.** POL earns trading fees from third-party swaps against the pool (and from the protocol's own buyback swaps). Accrued fees flow to the Timelock-custodied BPT position; they are **not** re-routed through `FeeRouter` (this preserves `FeeRouter`'s strict per-byte-settlement accounting — POL trading-fee yield is treasury-direct revenue). Governance may withdraw accrued trading fees to the treasury via the same 10%/30-day-cap proposal path. The expected baseline yield from POL trading fees at PoC scale is modest (under-trafficked pool); the strategic purpose of POL is depth and price stability, not yield.

**Why POL-heavy is consistent with work-token.** POL is *protocol-owned*; it doesn't create passive yield to any external holder. Trading-fee yield flows to treasury (operator-governed); no individual holder receives passive returns. The 15% combined allocation is defensible for an infrastructure protocol focused on liquidity depth as a primary value-accrual lever, comparable to Olympus Pro / OHM-style POL strategies but without the rebase mechanics that made those problematic. Removing LM eliminates the Howey-prong-4 exposure that an LP-token-yield program would carry.

### Buyback inflow source and rate (router-driven per [ADR 026](026-tokenomics.md#adr-026-tokenomics))

`BuybackBurner` is router-fed. The `FeeRouter` contract receives the paid USDC from `PaymentChannel.redeem` at every redemption and atomically forwards **30% of routed USDC directly to `BuybackBurner` in the same transaction**, alongside the other two buckets (60% operator, 10% treasury). The full router split is in [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split).

**Implications for this ADR:**

- **Inflow source.** USDC arrives at the `BuybackBurner` address from the `FeeRouter` via a same-transaction `transfer` inside `routeSettlement`. No manual treasury transfers.
- **Inflow rate is 30% of routed USDC.** The per-epoch liquidity cap below must be sized against this — at this rate the cap is binding under sustained network revenue and the operator must size `maxBuybackAmount`, `subSwapCount`, and the cap fraction accordingly. The keeper schedule (TWAP cadence) is calibrated against this rate.
- **Pool and execution mechanics.** Balancer V3 80/20 pool, swap call pattern, TWAP + `minTokenOut` defense, POL custody model, and per-transaction `maxBuybackAmount` cap — all specified below.

### Buyback execution via Balancer V3

The `BuybackBurner` member surface is defined in [ADR 003 — BuybackBurner](003-payments.md#buybackburner) and is not duplicated here to avoid cross-ADR drift.

The core `executeBuyback(uint256 amount, uint256 minTokenOut)` call pattern is **unchanged** for the Balancer V3 venue (the swapped-in asset is USDC, fixed at deployment). Each concrete venue subclass carries a `setPool(address pool)` setter, so the same base surface selects a venue-specific pool identifier symmetrically across Balancer V3 and any future Uniswap V3 deployment. Deployment configuration for the Balancer V3 venue:

- `setSwapRouter(address)` is set to the router contract used for the **Balancer V3 Vault** on the production L2. On Arbitrum mainnet this is `0xEAedc32a51c510d35ebC11088fD5fF2b47aACF2E`. Balancer's deployment registry labels this contract **`Router v2`** — that label is the second iteration of the Balancer V3 Router artifact (a contract versioning within Balancer V3), **not** a reference to Balancer V2 protocol routing. Sepolia and other testnet addresses MUST be pulled from [`balancer-deployments/addresses/arbitrum-sepolia.json`](https://github.com/balancer/balancer-deployments) at deployment time and not hardcoded in this ADR.
- `setPool(address)` is set to the contract address of the deployed 80/20 TOKEN/USDC Weighted Pool. V3 identifies pools by contract address directly; the V2 `bytes32 poolId` abstraction is gone. The setter is venue-symmetric — a Uniswap V3 deployment would hold the V3 pool contract address here. The pool address MAY also be provided via the `BuybackBurner` constructor (see [ADR 016 § Constructor Dependencies](016-contract-interactions.md#deployment-order-and-initialization-dependencies)); constructor-time and setter-time semantics are equivalent, and `executeBuyback()` reverts `PoolNotWired` while either the pool or the Vault address is zero.
- **Approvals footgun — the contract approves Permit2, not the Router and not the Vault.** `executeBuyback()` *calls* the Router, but a Balancer V3 Router does not pull `tokenIn` through a plain ERC20 allowance. It pulls through **Permit2** (`permit2.transferFrom`), so the spend needs two legs, and neither of them is an allowance to the Router or the Vault directly. This indirection is the single most common V2→V3 integration mistake. The shipped `BuybackBurnerBalancerV3` ([#686](https://github.com/decdn/decdn/issues/686)) uses a **scoped per-swap allowance** rather than a standing `type(uint256).max` one: `usdc.forceApprove(permit2, amountIn)`, then `permit2.approve(usdc, swapRouter, amountIn, block.timestamp)` immediately before the swap, and both reset to `0` immediately after. Confinement comes from that reset plus Permit2's own amount-decrement on transfer. Together they drive both the USDC→Permit2 ERC20 allowance and the Permit2→Router allowance to `0` within the transaction, so neither survives across calls. Permit2 sits at the canonical `0x000000000022D473030F116dDEE9F6B43aC78BA3` on every chain deCDN targets, but the constructor takes it as an argument (zero-rejected) rather than hardcoding it, so tests can substitute a mock. The contract holds the Router address in a governance-mutable `swapRouter` slot (constructor-set, `setSwapRouter`). It holds the **Balancer V3 Vault** address (`0xbA1333333333a1BA1108E8412f11850A5C319bA9` on Arbitrum mainnet) in a separate `balancerVault` slot. The Vault is **read-only** here: `isPoolRegistered`, `getStaticSwapFeePercentage`, and the scaled-18 leg balances behind the TWAP. The Router settles to the Vault; the burner never approves it.

#### Single-swap (non-TWAP) execution

When `subSwapCount = 1`, `executeBuyback()` calls `Router.swapSingleTokenExactIn(pool, USDC, TOKEN, amount, minTokenOut, deadline, false, "")` on the Balancer V3 Router with the stored `pool` address, `tokenIn = USDC`, `tokenOut = TOKEN`, `exactAmountIn = amount` (from the caller), `minAmountOut = minTokenOut`, `wethIsEth = false`, and `userData = ""`. The keeper-supplied `minTokenOut` is **not applied raw**: the contract first enforces `minTokenOut >= twapFloor`, where `twapFloor` is the `slippageBps`-discounted TWAP-derived minimum (§ TWAP policy), so the effective minimum passed to the Router is `max(twapFloor, keeper minTokenOut)`. The existing `maxBuybackAmount` and `minBuybackAmount` band applies — `amount` MUST be in `[minBuybackAmount, maxBuybackAmount]`. The shipped `BuybackBurnerBalancerV3` performs exactly **one** Router swap per `executeBuyback` call regardless of `subSwapCount`; the multi-swap cadence below is keeper-orchestrated across separate calls.

#### TWAP policy (`subSwapCount > 1`)

Balancer's smoother curve reduces the need for TWAP versus V3's concentrated bands but does not eliminate it for large buybacks. TWAP + `minTokenOut` + **mandatory private-RPC routing** + **per-epoch liquidity cap** form the MEV defense stack. CoW Swap (below) is a conditional add-on, not a substitute. When splitting:

- **`minTokenOut` is the aggregate minimum** TOKEN output across the full buyback request, not a per-sub-swap minimum. Because the shipped contract executes one swap per `executeBuyback` call, this aggregate tracking is a **keeper-side** obligation across the call sequence: the keeper computes a per-call `minTokenOut` (clamped on-chain to `max(twapFloor, …)`) and ensures the cumulative output across the request's calls meets the intended aggregate. A naive per-sub-swap guard of `minTokenOut / subSwapCount` is rejected because integer truncation allows the aggregate to fall below the requested minimum. The contract's own per-call defense is the on-chain `twapFloor`.
- **Per-sub-swap guard** is derived from remaining required output and remaining sub-swaps: `remainingMinOut = minTokenOut − cumulativeReceived`; `perSubSwapLimit = ceilDiv(remainingMinOut, remainingSubSwaps)`.
- **`maxBuybackAmount` is a per-transaction cap.** Each sub-swap (its own transaction) MAY be as large as `maxBuybackAmount`; the *total* buyback is bounded by the caller-supplied `amount`, not by `maxBuybackAmount × subSwapCount`.
- **Spacing.** Sub-swaps are separated by at least `subSwapMinBlockGap` blocks.
- **Required: Flashbots-style private-RPC routing.** The keeper MUST submit every buyback sub-swap through a private-RPC bundle endpoint that bypasses the public mempool — Flashbots Protect, MEV-Share, or the equivalent on the production L2. Public-mempool routing is **not** an acceptable fallback: programmatic buybacks are a known MEV target (front-run-and-dump around `executeBuyback` calls), and TWAP + `minTokenOut` alone do not prevent a searcher from observing the pending transaction and skewing the pool against the protocol within the slippage budget. This is a hard requirement; the keeper configuration MUST refuse to publish to a public RPC.
- **Required: per-epoch liquidity cap.** Independent of `maxBuybackAmount` (a per-transaction cap), `BuybackBurner` MUST enforce a per-epoch aggregate cap on the USDC notional swapped through the Balancer V3 pool across all buyback executions in a single 1-week epoch. The cap is sized as a fixed fraction of in-pool USDC depth at epoch start, so even a worst-case sustained-execution profile cannot exhaust pool depth. At the 30% inflow rate this cap binds frequently under sustained network revenue; sizing must be conservative. The fraction (`epochLiquidityCapFraction`, default 10%, bounded `[1%, 30%]`) is governance-tunable through the 48-hour timelock per [ADR 009](009-governance.md#adr-009-governance-model).
- **Conditional add-on: CoW Swap routing.** The keeper MAY route the buyback through CoW Swap instead of calling the Router directly, **if CoW solvers route through the deployed Balancer V3 weighted pool at best-execution time**. **Before enabling CoW routing in production, the operator MUST verify via CoW's `/api/v1/quote` endpoint that the target pool is reachable and that quoted prices are within `slippageBps` of the Router-direct path.** If CoW routing is unavailable or regressed, fall back to direct Router + TWAP + private-RPC.
- **Partial execution.** If a later sub-swap reverts, earlier sub-swaps stand and their output is retained; residual USDC remains in the contract until the next execution. The original split request MUST NOT be treated as having satisfied `minTokenOut` unless cumulative output across its sub-swaps meets the aggregate minimum.

### Parameter Table

| Parameter | Value |
| --- | --- |
| Venue (`setSwapRouter`) | Balancer V3 Router (address on L2) |
| Token approvals spender | Permit2 (`0x0000…78BA3`) — scoped per swap, never the Vault; see [§ Buyback execution via Balancer V3](#buyback-execution-via-balancer-v3) |
| Pool identifier (`pool`, `address`) | Deployed pool contract address |
| Pool weights | 80% TOKEN / 20% USDC |
| Pool swap fee | 1% (100 bps) |
| POL TOKEN-side allocation | 100M (10% of supply) |
| Initial pool seed size | Sized so `maxBuybackAmount` causes < `slippageBps` impact (~$250K nominal USDC seed at launch — the $0.01-anchor pairing for 100M TOKEN) |
| `minBuybackAmount` | 100 USDC |
| `maxBuybackAmount` | Set so a single swap causes < `slippageBps` impact; **required non-zero** — a zero ceiling reverts every non-zero buyback until governance raises it, stalling the buyback program while the FeeRouter keeps accruing into the burner |
| `slippageBps` | 200 bps (2%), bounded `[0, 1000 bps]` — the TWAP floor scales by `(10000 - slippageBps)`, so a tolerance near 100% collapses it to nothing and disables the MEV stack silently. `0` is accepted but is **not an operating point**: it leaves no tolerance for the price impact this parameter exists to absorb, so buybacks then land only when spot happens to beat the TWAP |
| TWAP `subSwapCount` | 4 (default) [^subswap-rationale] |
| TWAP `subSwapMinBlockGap` | 10 blocks (~2 minutes on Arbitrum) |
| Private-RPC routing | **Required** — Flashbots Protect / MEV-Share equivalent on the production L2; public-mempool routing is not an acceptable fallback |
| Per-epoch liquidity cap | **Required** — `epochLiquidityCapFraction` of in-pool USDC depth at epoch start (default 10%, bounded `[1%, 30%]`; see [§ TWAP policy](#twap-policy-subswapcount--1)) |
| POL withdraw cap | 10% of POL per 30-day window per governance proposal |
| Execution activation | Disabled at launch — governance vote required to enable |

The `BuybackBurner` setters listed above (`setSwapRouter`, `setPool`, `setSlippageTolerance`, `setMinBuybackAmount`, `setMaxBuybackAmount`, plus the keeper-rotation `setKeeper` per [ADR 003](003-payments.md#buybackburner)) are `GOVERNANCE_ROLE`-gated through the standard 48-hour timelock per [ADR 009](009-governance.md#adr-009-governance-model), as is `setEpochLiquidityCapFraction`. Three parameters carry explicit safety bounds, enforced at both the constructor and the setter so neither a mis-set deploy env var nor a governance proposal can disable a guard: `epochLiquidityCapFraction` is bounded `[1%, 30%]` (noted in [§ TWAP policy](#twap-policy-subswapcount--1)), `slippageBps` is bounded `[0, 1000 bps]`, and `maxBuybackAmount` must be non-zero. The last two exist because both failures are silent — an out-of-band `slippageBps` leaves the MEV stack nominally configured but scaled to nothing, and a zero `maxBuybackAmount` leaves a burner that accrues the buyback bucket and reverts every attempt to spend it. Both are recoverable by a further governance call, so the cost is a timelock-delayed stall rather than a permanent loss. Pool weights, pool swap fee, POL TOKEN-side allocation, private-RPC routing, and POL withdraw cap are structural — not parameter-tunable in-contract; changes require pool redeployment, governance proposal, or a hard requirement on keeper configuration. The `subSwapCount` and `subSwapMinBlockGap` TWAP parameters are constructor-set immutables on `BuybackBurner`; changing them requires redeploying the contract (per [ADR 016 § No proxy deployment patterns](016-contract-interactions.md#no-proxy-deployment-patterns)).

[^subswap-rationale]: A default of 4 balances MEV mitigation against gas overhead and keeper complexity. 2 sub-swaps provides marginal splitting benefit; ≥8 multiplies keeper gas and `ceilDiv` rounding artifacts without proportionate MEV improvement on a weighted pool. At higher buyback volume governance may consider raising the default to 6–8 via a `BuybackBurner` redeploy; this is left as production tuning.

### Activation Criteria (Production)

Buyback execution should be enabled by governance vote only when all of the following hold:

1. The Balancer V3 80/20 TOKEN/USDC Weighted Pool has been deployed and seeded with POL at the initial pool seed size, and its contract address has been set via `setPool(address)`.
2. The `BuybackBurner` contract has been configured with the Balancer V3 Router address via `setSwapRouter(address)` and with the Balancer V3 Vault address via `setVault(address)`. No standing USDC approval is required or expected: the burner approves **Permit2** per swap and resets to `0` afterwards (see the approvals footgun in [§ Buyback execution via Balancer V3](#buyback-execution-via-balancer-v3)).
3. A single swap of size `maxBuybackAmount` against the pool causes less than `slippageBps` price impact.
4. Accumulated USDC in the `BuybackBurner` contract exceeds `minBuybackAmount`.
5. Either (a) a keeper with `KEEPER_ROLE` is operational and configured to call `executeBuyback()` on a schedule, or (b) governance is prepared to trigger executions manually.
6. **Required: private-RPC routing in place.** The keeper MUST be configured to submit through Flashbots Protect or the equivalent private-bundle endpoint on the production L2 before execution is enabled. A pre-flight check that public-mempool publication is refused MUST be part of activation runbook sign-off.
7. **Required: per-epoch liquidity cap configured.** The per-epoch liquidity cap MUST be set on-chain to a value calibrated against epoch-start in-pool USDC depth and the 30% router inflow rate. Calibration MUST confirm the cap clears expected per-epoch buyback inflow at the seeded depth; if it does not, governance raises `epochLiquidityCapFraction` toward its 30% ceiling (accepting more per-epoch price impact) and/or tops up POL depth before enabling execution. The 10pp POL sizing narrows this per-epoch-cap headroom — see [§ Buyback per-epoch-cap headroom](#buyback-per-epoch-cap-headroom-685).
8. **(If CoW routing is used as an add-on)** The operator has verified via CoW's `/api/v1/quote` endpoint that CoW solvers route through the deployed Balancer V3 pool and that quoted prices are within `slippageBps` of the Router-direct path. If this fails, disable CoW routing and fall back to direct Router + TWAP + private-RPC; this does not block activation.

Criteria 1–4 are quantitative; governance voters verify them off-chain before enabling execution. Criterion 2 is a one-time deployment check. Criteria 6 and 7 are hard structural requirements; criterion 8 is venue-integration health.

### Deploy-time genesis activation

The Activation Criteria above gate the production path: activation is a deliberate event scheduled through the 48-hour timelock. At genesis that path is unbootstrappable. Voting weight is served-bytes-derived (per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)) and a network that has served no bytes has no weight, so no proposal reaches quorum; and the deploy leaves the deployer with no privileged role on exit. Activating the buyback bucket at genesis therefore happens in-script, while the deployer still holds `GOVERNANCE_ROLE`, before the handoff — it is genesis configuration, not a contract bootstrap mechanism, and it adds no contract surface.

The deploy script carries an off-by-default genesis-activation option for this. When it is off, the launch is dormant as described in [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics): `FeeRouter.buybackBurner` is the zero address and the split is `[9000, 0, 1000]`. When it is on, the script runs the same coherent bundle a governance activation would, in dependency order, before the role handoff:

1. Create and seed the venue's TOKEN/USDC pool as protocol-owned liquidity, custodied by the Timelock — the seed position NFT (Uniswap) or the BPT (Balancer) is minted to the Timelock. For Uniswap V3 the script creates the pool through the `NonfungiblePositionManager` and seeds a full-range position. For Balancer V3 it creates the 80/20 weighted pool through the `WeightedPoolFactory` and seeds it through the Router (Permit2), the same Permit2 flow described in [§ Buyback execution via Balancer V3](#buyback-execution-via-balancer-v3). The seed is sized by a chosen USDC amount and a target TOKEN price: the paired TOKEN amount is derived so the pool initializes at that price (applying the venue's value weights — 50/50 for Uniswap, 80/20 for Balancer), so a genesis operator supplies only the USDC they hold and the intended price, not a hand-computed ratio. Both seeds pull the TOKEN and USDC legs from the deployer, so the deployer must hold them at deploy time.
2. Deploy the concrete burner with its `GOVERNANCE_ROLE` / `DEFAULT_ADMIN_ROLE` held by the Timelock and the emergency multisig holding `PAUSER_ROLE`.
3. Set the steady-state split and destinations atomically — `setSharesAndDestinations([6000, 3000, 1000], {buybackBurner, treasury})` — with the buyback destination set before its non-zero share, satisfying the cross-validation invariant.
4. Grant the keeper `KEEPER_ROLE`.

The pool must be seeded before it is wired, and a keeper is required: an activated bucket with no keeper accrues USDC that can never be swapped.

Deploy-time genesis activation is a testnet and genesis convenience, distinct from the production Activation Criteria. It does not wait on a matured private-RPC keeper, a per-epoch cap calibrated against measured depth, or a governance vote. Mainnet leaves the option off and activates through the governance-gated criteria above once those conditions hold. One consequence carries over regardless of path: the TWAP accumulator is fail-closed until it spans `twapMinWindow` (default 1800 seconds), so buybacks revert `TwapNotReady` on a freshly-seeded pool until the window matures. The keeper warms the accumulator via `poke()` before the first buyback.

## Consequences

### Positive

- Seeds pool with ~1/4 the USDC of an equivalent 50/50 V3 position — critical for a USDC-poor treasury.
- Zero keeper / range-management operational burden. No `LiquidityManager` contract, no `KEEPER_ROLE` for liquidity operations.
- IL profile (80/20 weighted) aligns with the protocol's TOKEN-upside thesis.
- TWAP + `minTokenOut` guards provide deterministic on-chain MEV protection. Combined with the mandatory private-RPC routing and per-epoch liquidity cap, programmatic-buyback front-running is closed off both by transaction-graph invisibility and execution shape.
- Balancer V3's new Vault architecture specifically mitigates the bug class exploited in V2 (see Negative).
- The base `BuybackBurner` remains venue-agnostic — switching or adding venues later is a subclass-and-deployment change, not a change to the venue-neutral base surface.
- POL is non-extractable by external LPs because there are no external LPs. The DAO cannot be rugged by mercenary liquidity leaving at the worst moment.
- **POL-heavy strategy is regulatorily clean.** No Liquidity Mining program means no per-holder passive yield from holding LP tokens. POL trading-fee yield flows treasury-direct; there is no participant who earns TOKEN passively. This is the design's affirmative posture on Howey prong 4 (no income "solely from the efforts of others").
- **POL Governance bounds** (10% / 30-day withdraw cap; immutable) make any "rug pull" path months-long and externally observable — much stronger custody discipline than typical DAO-controlled positions.

### Negative

- Fee capture per dollar of TVL is lower than a well-managed V3 concentrated position.
- Aggregator routing density for Balancer V3 pools on Arbitrum is lower than for Uniswap V3 (and materially lower than for Balancer V2). V3 is newer; aggregator coverage and solver integrations are still maturing. A transient concern that should improve as V3 ages.
- **Security — realized V2 event, V3 shorter track record.** On 2025-11-03, Balancer V2 Composable Stable Pools were exploited for ~$125M across multiple chains. Root cause (per Certora, Trail of Bits, OpenZeppelin post-mortems): a rounding-direction bug in `_upscale`/`_downscale` latent since 2021. The exploit affected V2 Composable Stable Pools specifically, **not** V2 standard Weighted Pools and **not** Balancer V3. Certora explicitly cites V3's new Vault architecture as mitigating this bug class. This ADR uses V3 (not V2) and further restricts the choice to **standard V3 Weighted Pools with no hooks**. **Residual risk:** V3 has been in production materially less time than V2 and has fewer integration-hours behind its audits. The protocol team MUST track Balancer security advisories.
- Treasury bears impermanent loss directly. At the 15% combined Liquidity-Provision allocation, absolute IL exposure is proportionate (and ~⅓ smaller than at the prior 20% combined sizing). Buyback-and-burn provides an indirect reward loop (fees → buyback → TOKEN appreciation → LP position value), which the 30% burn share supports. Worst-case IL exposure is bounded by the 10% Protocol-Owned Liquidity allocation plus the paired USDC seed.
- V3's approvals footgun must be documented in deployment runbooks — see [Appendix: Production L2 Deployment § Deployment Runbook Impact](appendix-l2-deployment.md#deployment-runbook-impact).
- **Mandatory private-RPC dependency.** A third-party operational dependency on the chosen private-bundle provider for the production L2 — provider downtime or de-listing of the protocol's bundles is a new failure mode. Mitigated by selecting a provider with a strong uptime track record and keeping the keeper code provider-agnostic.
- **Buyback flow drives the per-epoch liquidity cap.** Pool depth and cap sizing must scale with revenue growth or burns will queue. This is the principal operational risk introduced by the 30% burn share, and it is **tightened** by the [#685](https://github.com/decdn/decdn/issues/685) move to 10pp POL — see [§ Buyback per-epoch-cap headroom](#buyback-per-epoch-cap-headroom-685).
- **Smaller external-LP base in year 1 vs an LM-enabled design.** With no LM subsidy, external LP growth depends on organic trading-fee yield alone, which is modest at PoC scale. POL provides depth. This is the deliberate cost of the cleanest regulatory posture.
- **POL accumulation can be politically charged.** A 10pp treasury-controlled LP position is still sizable relative to the 1B fixed supply (lowered from 15pp per [#685](https://github.com/decdn/decdn/issues/685) partly to narrow this optics surface). Governance discipline on the 10%/30-day withdraw cap matters; a supermajority intent on dismantling POL faces a months-long, externally-observable process — but the cap can in principle be reduced via the same governance path (subject to its own immutability constraint: the cap *itself* is immutable, so changing it requires a contract redeployment, which IS observable and slow).

### Buyback per-epoch-cap headroom (#685)

Lowering POL from 15pp to 10pp ([#685](https://github.com/decdn/decdn/issues/685)) reduces the TOKEN side of the 80/20 pool to 100M. At the fixed 80/20 weight and a fixed anchor price, the USDC side is pinned at `(20/80) × TOKEN_value`, so epoch-start in-pool USDC depth shrinks by ~⅓ (the ~$375K → ~$250K seed). The per-epoch liquidity cap is a fraction of that depth (`epochLiquidityCapFraction`, default 10%, bounded `[1%, 30%]`), so the **absolute USDC notional the cap admits per epoch falls by ~⅓** — while buyback **inflow** (30% of routed USDC per [ADR 026](026-tokenomics.md#adr-026-tokenomics)) is independent of POL size and scales with network revenue. The "burns will queue" risk therefore binds at a lower revenue level than under the prior 15pp sizing.

Levers, in order of preference:

1. **Calibrate `epochLiquidityCapFraction` upward within `[1%, 30%]`.** The default 10% has 3× headroom to the 30% ceiling; raising it recovers per-epoch throughput at the cost of more per-epoch price impact. This is a timelocked governance parameter, not a redeploy.
2. **Top up POL depth.** Permissionless add-liquidity (or a governance-authorized treasury deposit) deepens the pool and lifts the absolute cap; bounded only by available USDC.
3. **Defer burns across epochs.** Residual USDC remains in `BuybackBurner` between executions (per [§ TWAP policy](#twap-policy-subswapcount--1) partial-execution semantics); short queues self-clear once revenue and depth re-balance.

**Withdraw-cap re-check.** The 10%/30-day immutable withdraw cap ([§ POL Governance](#pol-governance)) is a *fraction* of the POL position, so it auto-scales with allocation — at 10pp it bounds a proportionally smaller absolute outflow while preserving the same months-long, externally-observable exit profile. It remains the correct immutable bound at the lower allocation; no change.

**Follow-up (open).** The exact revenue level at which the default-10% cap begins to queue burns at the 10pp-seeded depth is a quantitative question for the `finance/notebooks` buyback model (issue #685 acceptance criterion). Until that is run, activation sign-off (Criterion 7) should set `epochLiquidityCapFraction` conservatively against measured epoch-start depth rather than assuming the 10% default clears inflow.

## References

- [ADR 003 — Payment Channels (BuybackBurner member surface)](003-payments.md#buybackburner)
- [ADR 009 — Governance Model](009-governance.md#adr-009-governance-model)
- [ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md#adr-016-smart-contract-interaction-model)
- [ADR 026 — Tokenomics](026-tokenomics.md#adr-026-tokenomics) — source of the three-bucket FeeRouter split (60/30/10), the 30% buyback share, the 10% POL allocation (group 3) within the 15% combined Liquidity-Provision category, and the POL governance bounds
