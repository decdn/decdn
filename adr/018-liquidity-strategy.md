# ADR 018: Liquidity Strategy (Uniswap V3 50/50 POL)

**Status:** Accepted

> POL holds a 10pp / 100M TOKEN allocation. The combined POL+MM Liquidity-Provision category is 15% (top of the 5–15% DeFi norm), with MM at 5pp. The USDC arm pairs the POL TOKEN at the $0.01 anchor in a 50/50 Uniswap V3 pool (`TODO(pol-resize)`: the USDC seed size follows the POL allocation resize). Per-epoch buyback liquidity-cap headroom scales with pool depth, and each buyback swap adds USDC to the pool, so seed depth is a floor that glides up — see [§ Buyback per-epoch-cap headroom](#buyback-per-epoch-cap-headroom) and [Activation Criterion 7](#activation-criteria-production).

> The concrete swap binding is `BuybackBurnerUniswapV3`, a subclass of the abstract `GuardedBuybackBurner`, which subclasses the abstract `BuybackBurner`. It fixes three under-specified points detailed in the sections below:
>
> - **Router storage.** The base `BuybackBurner` is venue-neutral and stores only USDC and TOKEN. The subclass owns the venue set — the `SwapRouter02` (`swapRouter`) and the pool — both constructor-set. The Router is zero-rejected; the pool may be zero and wired later. Governance rotates the Router via `setSwapRouter` and the pool via `setPool`. `SwapRouter02` pulls `tokenIn` through a direct ERC20 allowance, so the burner approves the Router itself, scoped per swap ([§ Buyback execution via Uniswap V3](#buyback-execution-via-uniswap-v3)).
> - **On-chain `minOut` floor mechanism.** The on-chain TWAP floor is a **self-maintained cumulative-price accumulator** (Uniswap-V2 `price0CumulativeLast` style) over the pool's `slot0` marginal spot, sampled before each swap, advanceable permissionlessly via `poke()`, and **fail-closed** (`executeBuyback` reverts `TwapNotReady` until the accumulator spans the governed `twapMinWindow`). It does not read the pool's native `observe()` oracle, so a freshly-seeded pool whose observation cardinality is still 1 still prices the floor. Sparse updates weaken the average, so the floor is necessary-not-sufficient and stays layered under the mandatory keeper-side private-RPC routing and the per-epoch liquidity cap (§ TWAP policy).
> - **Effective minimum.** The keeper-supplied `minTokenOut` is **not** applied raw; the contract enforces `minTokenOut >= twapFloor`, so the effective minimum is `max(twapFloor, keeper minTokenOut)` (§ Single-swap execution).
> `executeBuyback` performs exactly one Router swap; the TWAP sub-swap cadence (count, block-gap, aggregate-`minOut` tracking) is keeper-orchestrated across calls per § TWAP policy.

## Context

> **[ADR 026](026-tokenomics.md#adr-026-tokenomics) alignment.** `BuybackBurner` is router-driven: it receives 30% of routed USDC same-tx from `FeeRouter` at every settlement. The pool design is Uniswap V3, 50/50 TOKEN/USDC, full-range, 1% swap fee, MEV protection via TWAP + `minTokenOut` + Flashbots-style private-RPC routing + per-epoch liquidity cap, POL custody by the Timelock. **The combined Liquidity-Provision category is 15% of supply** (5pp Market-Maker partner / group 7 + 10pp Protocol-Owned Liquidity / group 3 per [ADR 026 § Allocation](026-tokenomics.md#allocation)).

Buyback execution is deferred to production because thin TOKEN/USDC pool liquidity at genesis cannot absorb buyback flow without unacceptable slippage. [ADR 016](016-contract-interactions.md#adr-016-smart-contract-interaction-model) recommends TWAP execution as MEV mitigation but does not specify how pool depth is created.

Questions answered by this ADR:

1. How is the 15% combined Liquidity-Provision allocation ([ADR 026 § Allocation](026-tokenomics.md#allocation)) deployed across POL and MM?
2. Is liquidity mercenary (LP rewards / liquidity mining) or protocol-owned?
3. Which venue and pool shape?
4. How does buyback execution interact with the pool to avoid self-inflicted price impact?
5. Who can rebalance / withdraw POL, and how do POL trading-fee earnings flow?

The treasury holds the 10pp Protocol-Owned Liquidity allocation (100M of the 1B supply). The 5pp Market-Maker partner allocation (50M TOKEN) is distributed to vetted MM partners (e.g., GSR, Wintermute, Auros, Flowdesk) under standard MM agreements for two-sided quoting on CEXes and DEX aggregators and is genesis-liquid. The small protocol team has no bandwidth to operate an active concentrated-liquidity keeper stack; this favours a pool shape that needs no range management.

## Decision

Use a Uniswap V3 TOKEN/USDC pool with a single **full-range 50/50** position as the canonical TOKEN/USDC venue. Seed it as Protocol-Owned Liquidity (POL) from the 10pp Protocol-Owned Liquidity allocation. The DAO treasury holds the position NFT directly; no LP rewards, no liquidity mining, no dedicated `LiquidityManager` contract.

Uniswap V3 is deployed on every target network, including the Arbitrum Sepolia initial network, so the buyback path runs against it from launch. Uniswap V3 core is extensively audited and battle-tested.

### Venue: full-range 50/50 vs a concentrated band

A full-range Uniswap V3 position spans the whole tick range. It behaves like a constant-product curve: it never exits its range, it keeps trading across the whole curve, and it always quotes a spot for the buyback TWAP. This removes the operational burden a concentrated band carries.

| Factor | Full-range 50/50 | Concentrated band |
| --- | --- | --- |
| Operational burden | None — seed once | Active range management, keeper infra, rebalance transactions |
| Behavior when price exits anticipated range | Continues trading across the full curve | Position becomes 100% one asset, earns zero fees |
| Fee capture per TVL (active conditions) | Lower | Higher (if well-managed and in-range) |
| Spot availability for the TWAP | Always quotes a spot | None when out of range |

The team cannot provide active range management during PoC and early production, so the full-range position is the right shape. A concentrated band's fee-capture win presupposes management the team cannot supply.

**Fee tier rationale.** 1% suits a long-tail asset with limited trading activity. Lower tiers (0.3%, 0.05%) assume volume sufficient to compensate LPs, which TOKEN does not have at PoC scale.

### Venue-neutral burner selection

The on-chain `BuybackBurner` is venue-neutral. An abstract `GuardedBuybackBurner` holds the shared MEV-defense stack — the self-maintained TWAP floor, the `slippageBps` / `minBuybackAmount` / `maxBuybackAmount` band, and the per-epoch USDC liquidity cap — and defers the venue to a concrete subclass. `BuybackBurnerUniswapV3` binds the Uniswap V3 `SwapRouter02` and a single V3 pool: it reads the pool's `slot0` marginal spot, its fee tier, and its in-pool USDC balance for the shared guards, and performs one `exactInputSingle` swap per call.

The burner keeps the abstract base and a single concrete subclass so a future venue is a subclass-and-deployment change, not a change to the venue-neutral base surface. `FeeRouter.buybackBurner` is swappable in a single governance call with no `FeeRouter` change. There is no venue enum, no venue-string dispatch, and no venue-selection library, because there is one venue; a second venue reintroduces the subclass, not the dispatch.

### Protocol-Owned Liquidity mechanics

- **Source:** The 10pp Protocol-Owned Liquidity allocation (100M TOKEN per [ADR 026 § Allocation](026-tokenomics.md#allocation)) funds the TOKEN side. The USDC side pairs the full 100M TOKEN at the $0.01 anchor in the 50/50 pool. `TODO(pol-resize)`: the USDC seed size follows the POL allocation resize; it is funded from the pre-seed USDC bootstrap POL-seed bucket ([ADR 026 § Bootstrap](026-tokenomics.md#bootstrap-mechanism--pre-seed-usdc)).
- **Initial pool seed:** Sized so that a single `maxBuybackAmount` swap causes less than `slippageBps` price impact, making buyback execution well-conditioned on its own pool.
- **Custody:** The position NFT is held by the DAO treasury address. PoC: admin key. Production: Governor + `TimelockController`. No withdraw path to an EOA — liquidity exit requires a governance proposal through the timelock. This invariant is enforced by the position NFT being held at the Timelock address and the absence of any bespoke withdraw function; custody discipline is the sole enforcement mechanism.
- **No `LiquidityManager` contract.** A full-range position never exits its range, so the constant-product curve handles rebalancing implicitly via arbitrage. There is no range to manage, no `rebalance()` keeper, no `KEEPER_ROLE` for liquidity operations.
- **No liquidity mining.** Mercenary LPs exit when rewards stop and consume TOKEN supply for a benefit POL provides more reliably. A future ADR may reintroduce LM if external-LP-attraction strategic priority changes; the current design is intentionally LM-free for regulatory cleanliness (no per-holder passive yield).

### POL Governance

The 15% combined POL+MM allocation sits at the top of the typical 5–15% DeFi range and is large enough that governance controls on its operation are load-bearing.

**Rebalance authority.** The 50/50 full-range position spans the whole curve; the constant-product curve handles intra-pool rebalancing via arbitrage. Governance may *change the pool* (deploy a new pool, migrate POL there) only via a standard governance proposal under the 48-hour timelock. There is no per-block rebalance keeper.

**Withdraw authority.** POL withdraw — partial or full removal of liquidity from the Timelock-custodied position — requires:

1. A standard governance proposal authorizing the specific withdraw amount and recipient address.
2. The 48-hour timelock delay (no fast-track path, even for emergency multisig).
3. **Per-rolling-window withdraw cap:** at most 10% of the POL position may be withdrawn in any 30-day window via a single proposal. Larger withdraws require multiple proposals spaced ≥30 days apart. This bound makes any "pull the rug" path externally observable months in advance.

Permitted operations within these bounds:

- **Top-up:** depositing additional USDC or TOKEN to deepen the position. Permissionless (anyone may add liquidity to the public pool); the protocol may execute via governance proposal.
- **Partial withdraw to fund subsidy programs:** capped at 10% per 30-day window, governance-authorized, recipient must be the protocol treasury or a treasury-controlled address.
- **Venue migration:** deploying a new pool and migrating POL into it requires governance, with the 10%/30-day withdraw cap binding the migration speed.

**Trading-fee accounting.** POL earns trading fees from third-party swaps against the pool (and from the protocol's own buyback swaps). Accrued fees stay in the Timelock-custodied position; they are **not** re-routed through `FeeRouter` (this preserves `FeeRouter`'s strict per-byte-settlement accounting — POL trading-fee yield is treasury-direct revenue). Governance may collect accrued trading fees to the treasury via the same 10%/30-day-cap proposal path. The expected baseline yield from POL trading fees at PoC scale is modest (under-trafficked pool); the strategic purpose of POL is depth and price stability, not yield.

**Why POL-heavy is consistent with work-token.** POL is *protocol-owned*; it doesn't create passive yield to any external holder. Trading-fee yield flows to treasury (operator-governed); no individual holder receives passive returns. The 15% combined allocation is defensible for an infrastructure protocol focused on liquidity depth as a primary value-accrual lever. Removing LM eliminates the Howey-prong-4 exposure that an LP-token-yield program would carry.

### Buyback inflow source and rate (router-driven per [ADR 026](026-tokenomics.md#adr-026-tokenomics))

`BuybackBurner` is router-fed. The `FeeRouter` contract receives the paid USDC from `PaymentPool.redeem` at every redemption and atomically forwards **30% of routed USDC directly to `BuybackBurner` in the same transaction**, alongside the other two buckets (60% operator, 10% treasury). The full router split is in [ADR 026 § FeeRouter split](026-tokenomics.md#feerouter-split).

**Implications for this ADR:**

- **Inflow source.** USDC arrives at the `BuybackBurner` address from the `FeeRouter` via a same-transaction `transfer` inside `routeSettlement`. No manual treasury transfers.
- **Inflow rate is 30% of routed USDC.** The per-epoch liquidity cap below must be sized against this — at this rate the cap is binding under sustained network revenue and the operator must size `maxBuybackAmount` and the cap fraction accordingly. The keeper schedule (TWAP cadence) is calibrated against this rate.
- **Pool and execution mechanics.** Uniswap V3 50/50 full-range pool, swap call pattern, TWAP + `minTokenOut` defense, POL custody model, and per-transaction `maxBuybackAmount` cap — all specified below.

### Buyback execution via Uniswap V3

The `BuybackBurner` member surface is defined in [ADR 003 — BuybackBurner](003-payments.md#buybackburner) and is not duplicated here to avoid cross-ADR drift.

The core `executeBuyback(uint256 amount, uint256 minTokenOut)` call pattern swaps in USDC (fixed at deployment). Deployment configuration:

- `setSwapRouter(address)` is set to the Uniswap V3 `SwapRouter02` on the target network.
- `setPool(address)` is set to the deployed 50/50 TOKEN/USDC V3 pool. Uniswap V3 identifies pools by contract address directly. The pool address MAY also be provided via the `BuybackBurner` constructor (see [ADR 016 § Constructor Dependencies](016-contract-interactions.md#deployment-order-and-initialization-dependencies)); constructor-time and setter-time semantics are equivalent, and `executeBuyback()` reverts `PoolNotWired` while the pool or the Router address is zero.
- **Approvals — the burner approves the Router directly, scoped per swap.** A `SwapRouter02` pulls `tokenIn` through a plain ERC20 allowance, so the burner uses a **scoped per-swap allowance** rather than a standing `type(uint256).max` one: `usdc.forceApprove(swapRouter, amountIn)` immediately before the swap, then `usdc.forceApprove(swapRouter, 0)` immediately after. Confinement comes from that reset, so no standing allowance survives across calls. The contract holds the Router address in a governance-mutable `swapRouter` slot (constructor-set, `setSwapRouter`).

#### Single-swap (non-TWAP) execution

Each `executeBuyback()` call makes exactly **one** `SwapRouter02.exactInputSingle` swap with the stored `pool`'s fee tier, `tokenIn = USDC`, `tokenOut = TOKEN`, `amountIn = amount` (from the caller), `amountOutMinimum = minTokenOut`, and `sqrtPriceLimitX96 = 0`. The keeper-supplied `minTokenOut` is **not applied raw**: the contract first enforces `minTokenOut >= twapFloor`, where `twapFloor` is the `slippageBps`-discounted TWAP-derived minimum (§ TWAP policy), so the effective minimum passed to the Router is `max(twapFloor, keeper minTokenOut)`. The existing `maxBuybackAmount` and `minBuybackAmount` band applies — `amount` MUST be in `[minBuybackAmount, maxBuybackAmount]`. There is exactly one Router swap per `executeBuyback` call; the multi-swap cadence below is keeper-orchestrated across separate calls.

#### TWAP policy (multi-call sub-swaps)

A full-range position is a constant-product curve, so a large buyback still moves price and needs splitting. TWAP + `minTokenOut` + **mandatory private-RPC routing** + **per-epoch liquidity cap** form the MEV defense stack. CoW Swap (below) is a conditional add-on, not a substitute. When splitting:

- **`minTokenOut` is the aggregate minimum** TOKEN output across the full buyback request, not a per-sub-swap minimum. Because the shipped contract executes one swap per `executeBuyback` call, this aggregate tracking is a **keeper-side** obligation across the call sequence: the keeper computes a per-call `minTokenOut` (clamped on-chain to `max(twapFloor, …)`) and ensures the cumulative output across the request's calls meets the intended aggregate. A naive per-sub-swap guard of `minTokenOut / (number of sub-swaps)` is rejected because integer truncation allows the aggregate to fall below the requested minimum. The contract's own per-call defense is the on-chain `twapFloor`.
- **Per-sub-swap guard** is derived from remaining required output and remaining sub-swaps: `remainingMinOut = minTokenOut − cumulativeReceived`; `perSubSwapLimit = ceilDiv(remainingMinOut, remainingSubSwaps)`.
- **`maxBuybackAmount` is a per-transaction cap.** Each sub-swap (its own transaction) MAY be as large as `maxBuybackAmount`; the *total* buyback is bounded by the caller-supplied `amount`, not by `maxBuybackAmount × (number of sub-swaps)`.
- **Spacing.** Sub-swaps are separated by at least the keeper's configured block gap (default ~10 blocks on Arbitrum).
- **Required: Flashbots-style private-RPC routing.** The keeper MUST submit every buyback sub-swap through a private-RPC bundle endpoint that bypasses the public mempool — Flashbots Protect, MEV-Share, or the equivalent on the production L2. Public-mempool routing is **not** an acceptable fallback: programmatic buybacks are a known MEV target (front-run-and-dump around `executeBuyback` calls), and TWAP + `minTokenOut` alone do not prevent a searcher from observing the pending transaction and skewing the pool against the protocol within the slippage budget. This is a hard requirement; the keeper configuration MUST refuse to publish to a public RPC.
- **Required: per-epoch liquidity cap.** Independent of `maxBuybackAmount` (a per-transaction cap), `BuybackBurner` MUST enforce a per-epoch aggregate cap on the USDC notional swapped through the pool across all buyback executions in a single 1-week epoch. The cap is sized as a fixed fraction of in-pool USDC depth at epoch start, so even a worst-case sustained-execution profile cannot exhaust pool depth. At the 30% inflow rate this cap binds frequently under sustained network revenue; sizing must be conservative. The fraction (`epochLiquidityCapFraction`, default 10%, bounded `[1%, 30%]`) is governance-tunable through the 48-hour timelock per [ADR 009](009-governance.md#adr-009-governance-model).
- **Conditional add-on: CoW Swap routing.** The keeper MAY route the buyback through CoW Swap instead of calling the Router directly, **if CoW solvers route through the deployed Uniswap V3 pool at best-execution time**. **Before enabling CoW routing in production, the operator MUST verify via CoW's `/api/v1/quote` endpoint that the target pool is reachable and that quoted prices are within `slippageBps` of the Router-direct path.** If CoW routing is unavailable or regressed, fall back to direct Router + TWAP + private-RPC.
- **Partial execution.** If a later sub-swap reverts, earlier sub-swaps stand and their output is retained; residual USDC remains in the contract until the next execution. The original split request MUST NOT be treated as having satisfied `minTokenOut` unless cumulative output across its sub-swaps meets the aggregate minimum.

### Parameter Table

| Parameter | Value |
| --- | --- |
| Venue (`setSwapRouter`) | Uniswap V3 `SwapRouter02` (address on the target network) |
| Token approvals spender | `SwapRouter02` — direct ERC20 allowance, scoped per swap; see [§ Buyback execution via Uniswap V3](#buyback-execution-via-uniswap-v3) |
| Pool identifier (`pool`, `address`) | Deployed pool contract address |
| Position range | Full-range (50/50 constant-product depth) |
| Pool swap fee | 1% (10000 = 1% fee tier) |
| POL TOKEN-side allocation | 100M (10% of supply) |
| Initial pool seed size | Sized so `maxBuybackAmount` causes < `slippageBps` impact (`TODO(pol-resize)`: USDC seed follows the POL allocation resize) |
| `minBuybackAmount` | 100 USDC |
| `maxBuybackAmount` | Set so a single swap causes < `slippageBps` impact; **required non-zero** — a zero ceiling reverts every non-zero buyback until governance raises it, stalling the buyback program while the FeeRouter keeps accruing into the burner |
| `slippageBps` | 200 bps (2%), bounded `[0, 1000 bps]` — the TWAP floor scales by `(10000 - slippageBps)`, so a tolerance near 100% collapses it to nothing and disables the MEV stack silently. `0` is accepted but is **not an operating point**: it leaves no tolerance for the price impact this parameter exists to absorb, so buybacks then land only when spot happens to beat the TWAP |
| TWAP sub-swap cadence | Keeper-side off-chain config — the contract does exactly one Router swap per call, and the keeper spaces and sizes the sub-swaps across calls (default 4 sub-swaps, ~10-block spacing on Arbitrum) [^subswap-rationale] |
| Private-RPC routing | **Required** — Flashbots Protect / MEV-Share equivalent on the production L2; public-mempool routing is not an acceptable fallback |
| Per-epoch liquidity cap | **Required** — `epochLiquidityCapFraction` of in-pool USDC depth at epoch start (default 10%, bounded `[1%, 30%]`; see [§ TWAP policy](#twap-policy-multi-call-sub-swaps)) |
| POL withdraw cap | 10% of POL per 30-day window per governance proposal |
| Execution activation | Disabled at launch — governance vote required to enable |

The `BuybackBurner` setters listed above (`setSwapRouter`, `setPool`, `setSlippageTolerance`, `setMinBuybackAmount`, `setMaxBuybackAmount`, plus the keeper-rotation `setKeeper` per [ADR 003](003-payments.md#buybackburner)) are `GOVERNANCE_ROLE`-gated through the standard 48-hour timelock per [ADR 009](009-governance.md#adr-009-governance-model), as is `setEpochLiquidityCapFraction`. Three parameters carry explicit safety bounds, enforced at both the constructor and the setter so neither a mis-set deploy env var nor a governance proposal can disable a guard: `epochLiquidityCapFraction` is bounded `[1%, 30%]` (noted in [§ TWAP policy](#twap-policy-multi-call-sub-swaps)), `slippageBps` is bounded `[0, 1000 bps]`, and `maxBuybackAmount` must be non-zero. The last two exist because both failures are silent — an out-of-band `slippageBps` leaves the MEV stack nominally configured but scaled to nothing, and a zero `maxBuybackAmount` leaves a burner that accrues the buyback bucket and reverts every attempt to spend it. Both are recoverable by a further governance call, so the cost is a timelock-delayed stall rather than a permanent loss. Position range, pool swap fee, POL TOKEN-side allocation, private-RPC routing, and POL withdraw cap are structural — not parameter-tunable in-contract; changes require pool redeployment, governance proposal, or a hard requirement on keeper configuration. The TWAP sub-swap cadence (count and block-gap) is not an on-chain parameter: the contract does exactly one Router swap per call, and the keeper holds the cadence in its own off-chain config, so retuning it needs no contract change.

[^subswap-rationale]: A default of 4 balances MEV mitigation against gas overhead and keeper complexity. 2 sub-swaps provides marginal splitting benefit; ≥8 multiplies keeper gas and `ceilDiv` rounding artifacts without proportionate MEV improvement. At higher buyback volume the keeper may raise the count to 6–8 in its own config; this is left as production tuning and needs no contract change.

### Activation Criteria (Production)

Buyback execution should be enabled by governance vote only when all of the following hold:

1. The Uniswap V3 50/50 TOKEN/USDC pool has been deployed and seeded with POL at the initial pool seed size, and its contract address has been set via `setPool(address)`.
2. The `BuybackBurner` contract has been configured with the `SwapRouter02` address via `setSwapRouter(address)`. No standing USDC approval is required or expected: the burner approves the Router per swap and resets to `0` afterwards (see [§ Buyback execution via Uniswap V3](#buyback-execution-via-uniswap-v3)).
3. A single swap of size `maxBuybackAmount` against the pool causes less than `slippageBps` price impact.
4. Accumulated USDC in the `BuybackBurner` contract exceeds `minBuybackAmount`.
5. Either (a) a keeper with `KEEPER_ROLE` is operational and configured to call `executeBuyback()` on a schedule, or (b) governance is prepared to trigger executions manually.
6. **Required: private-RPC routing in place.** The keeper MUST be configured to submit through Flashbots Protect or the equivalent private-bundle endpoint on the production L2 before execution is enabled. A pre-flight check that public-mempool publication is refused MUST be part of activation runbook sign-off.
7. **Required: per-epoch liquidity cap configured.** The per-epoch liquidity cap MUST be set on-chain to a value calibrated against epoch-start in-pool USDC depth and the 30% router inflow rate. Calibration MUST confirm the cap clears expected per-epoch buyback inflow at the seeded depth; if it does not, governance raises `epochLiquidityCapFraction` toward its 30% ceiling (accepting more per-epoch price impact) and/or tops up POL depth before enabling execution. See [§ Buyback per-epoch-cap headroom](#buyback-per-epoch-cap-headroom).
8. **(If CoW routing is used as an add-on)** The operator has verified via CoW's `/api/v1/quote` endpoint that CoW solvers route through the deployed Uniswap V3 pool and that quoted prices are within `slippageBps` of the Router-direct path. If this fails, disable CoW routing and fall back to direct Router + TWAP + private-RPC; this does not block activation.

Criteria 1–4 are quantitative; governance voters verify them off-chain before enabling execution. Criterion 2 is a one-time deployment check. Criteria 6 and 7 are hard structural requirements; criterion 8 is venue-integration health.

### Deploy-time genesis activation

The Activation Criteria above gate the production path: activation is a deliberate event scheduled through the 48-hour timelock. At genesis that path is unbootstrappable. Voting weight is served-bytes-derived (per [ADR 036](036-served-bytes-voting-weight.md#adr-036-served-bytes-voting-weight)) and a network that has served no bytes has no weight, so no proposal reaches quorum; and the deploy leaves the deployer with no privileged role on exit. Activating the buyback bucket at genesis therefore happens in-script, while the deployer still holds `GOVERNANCE_ROLE`, before the handoff — it is genesis configuration, not a contract bootstrap mechanism, and it adds no contract surface.

The deploy script carries an off-by-default genesis-activation option for this. When it is off, the launch is dormant as described in [ADR 016 § Tunable Economics](016-contract-interactions.md#tunable-economics): `FeeRouter.buybackBurner` is the zero address and the split is `[9000, 0, 1000]`. When it is on, the script runs the same coherent bundle a governance activation would, in dependency order, before the role handoff:

1. Create and seed the TOKEN/USDC pool as protocol-owned liquidity, custodied by the Timelock — the seed position NFT is minted to the Timelock. The script creates the pool through the `NonfungiblePositionManager` and seeds a full-range position. The seed is sized by a chosen USDC amount and a target TOKEN price: the paired TOKEN amount is derived so the 50/50 pool initializes at that price, so a genesis operator supplies only the USDC they hold and the intended price, not a hand-computed ratio. Both legs are pulled from the deployer, so the deployer must hold them at deploy time.
2. Deploy the concrete burner with `GOVERNANCE_ROLE` held by the Timelock and the emergency multisig holding `PAUSER_ROLE`, then renounce the burner's `DEFAULT_ADMIN_ROLE` so no master admin key survives on it (#2028) — the genesis path renounces it in the handoff loop; the `ActivateBuyback` runbook prints a Timelock `renounceRole` step. `PAUSER_ROLE` and `KEEPER_ROLE` remain `GOVERNANCE_ROLE`-administered, so both stay rotatable.
3. Set the steady-state split and destinations atomically — `setSharesAndDestinations([6000, 3000, 1000], {buybackBurner, treasury})` — with the buyback destination set before its non-zero share, satisfying the cross-validation invariant.
4. Grant the keeper `KEEPER_ROLE`.

The pool must be seeded before it is wired, and a keeper is required: an activated bucket with no keeper accrues USDC that can never be swapped.

Deploy-time genesis activation is a testnet and genesis convenience, distinct from the production Activation Criteria. It does not wait on a matured private-RPC keeper, a per-epoch cap calibrated against measured depth, or a governance vote. Mainnet leaves the option off and activates through the governance-gated criteria above once those conditions hold. One consequence carries over regardless of path: the TWAP accumulator is fail-closed until it spans `twapMinWindow` (default 1800 seconds), so buybacks revert `TwapNotReady` on a freshly-seeded pool until the window matures. The keeper warms the accumulator via `poke()` before the first buyback.

## Consequences

### Positive

- Zero keeper / range-management operational burden. A full-range position never exits its range, so there is no `LiquidityManager` contract and no `KEEPER_ROLE` for liquidity operations.
- The full-range pool always quotes a spot for the buyback TWAP, even at a freshly-seeded observation cardinality of 1, because the burner reads `slot0` into its own accumulator rather than the pool oracle.
- TWAP + `minTokenOut` guards provide deterministic on-chain MEV protection. Combined with the mandatory private-RPC routing and per-epoch liquidity cap, programmatic-buyback front-running is closed off both by transaction-graph invisibility and execution shape.
- Uniswap V3 core is extensively audited and has a long production track record.
- The base `BuybackBurner` remains venue-agnostic — switching or adding venues later is a subclass-and-deployment change, not a change to the venue-neutral base surface.
- POL is non-extractable by external LPs because there are no external LPs. The DAO cannot be rugged by mercenary liquidity leaving at the worst moment.
- **POL-heavy strategy is regulatorily clean.** No Liquidity Mining program means no per-holder passive yield from holding LP tokens. POL trading-fee yield flows treasury-direct; there is no participant who earns TOKEN passively. This is the design's affirmative posture on Howey prong 4 (no income "solely from the efforts of others").
- **POL Governance bounds** (10% / 30-day withdraw cap) make any "rug pull" path months-long and externally observable — much stronger custody discipline than typical DAO-controlled positions.

### Negative

- Fee capture per dollar of TVL is lower than a well-managed concentrated position.
- Treasury bears impermanent loss directly. Buyback-and-burn provides an indirect reward loop (fees → buyback → TOKEN appreciation → LP position value), which the 30% burn share supports. Worst-case IL exposure is bounded by the 10% Protocol-Owned Liquidity allocation plus the paired USDC seed.
- **Mandatory private-RPC dependency.** A third-party operational dependency on the chosen private-bundle provider for the production L2 — provider downtime or de-listing of the protocol's bundles is a new failure mode. Mitigated by selecting a provider with a strong uptime track record and keeping the keeper code provider-agnostic.
- **Buyback flow drives the per-epoch liquidity cap.** Pool depth and cap sizing must scale with revenue growth or burns will queue. This is the principal operational risk introduced by the 30% burn share — see [§ Buyback per-epoch-cap headroom](#buyback-per-epoch-cap-headroom).
- **Smaller external-LP base in year 1 vs an LM-enabled design.** With no LM subsidy, external LP growth depends on organic trading-fee yield alone, which is modest at PoC scale. POL provides depth. This is the deliberate cost of the cleanest regulatory posture.
- **POL accumulation can be politically charged.** A 10pp treasury-controlled LP position is sizable relative to the 1B fixed supply. Governance discipline on the 10%/30-day withdraw cap matters; a supermajority intent on dismantling POL faces a months-long, externally-observable process.

### Buyback per-epoch-cap headroom

The per-epoch liquidity cap is a fraction of epoch-start in-pool USDC depth (`epochLiquidityCapFraction`, default 10%, bounded `[1%, 30%]`), so the **absolute USDC notional the cap admits per epoch is bounded by that depth** — while buyback **inflow** (30% of routed USDC per [ADR 026](026-tokenomics.md#adr-026-tokenomics)) is independent of POL size and scales with network revenue. The "burns will queue" risk binds once inflow outpaces the cap.

**The glidepath.** Each buyback swap sends USDC into the pool and takes TOKEN out to burn, so the pool's USDC depth **grows** with every execution. The seed depth is therefore a floor, not a fixed ceiling: it glides up as buybacks run. Because the contract re-snapshots epoch-start depth at the first swap of each epoch, the per-epoch cap tracks *current* depth automatically — the admitted notional rises epoch over epoch as the pool deepens. `TODO(pol-resize)`: the concrete epoch-start seed depth follows the POL allocation resize.

Levers, in order of preference:

1. **Calibrate `epochLiquidityCapFraction` upward within `[1%, 30%]`.** The default 10% has 3× headroom to the 30% ceiling; raising it recovers per-epoch throughput at the cost of more per-epoch price impact. This is a timelocked governance parameter, not a redeploy.
2. **Top up POL depth.** Permissionless add-liquidity (or a governance-authorized treasury deposit) deepens the pool and lifts the absolute cap; bounded only by available USDC.
3. **Defer burns across epochs.** Residual USDC remains in `BuybackBurner` between executions (per [§ TWAP policy](#twap-policy-multi-call-sub-swaps) partial-execution semantics); short queues self-clear as revenue and the gliding depth re-balance.

**Withdraw-cap re-check.** The 10%/30-day withdraw cap ([§ POL Governance](#pol-governance)) is a *fraction* of the POL position, so it auto-scales with allocation — it bounds a proportional absolute outflow while preserving the same months-long, externally-observable exit profile.

**Follow-up (open).** The exact revenue level at which the default-10% cap begins to queue burns at the seeded depth is a quantitative question for the `finance/notebooks` buyback model. Until that is run, activation sign-off (Criterion 7) should set `epochLiquidityCapFraction` conservatively against measured epoch-start depth rather than assuming the 10% default clears inflow.

## References

- [ADR 003 — Payment Model (BuybackBurner member surface)](003-payments.md#buybackburner)
- [ADR 009 — Governance Model](009-governance.md#adr-009-governance-model)
- [ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md#adr-016-smart-contract-interaction-model)
- [ADR 026 — Tokenomics](026-tokenomics.md#adr-026-tokenomics) — source of the three-bucket FeeRouter split (60/30/10), the 30% buyback share, the 10% POL allocation (group 3) within the 15% combined Liquidity-Provision category, and the POL governance bounds
