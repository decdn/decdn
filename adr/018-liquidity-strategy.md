# ADR 018: Liquidity Strategy (Balancer 80/20 POL)

**Date:** 2026-04-04
**Status:** Draft

## Context

> **ADR 026 alignment.** Under [ADR 026](026-gauge-boost-tokenomics.md), this ADR's `BuybackBurner` is fed router-driven (5% of routed USDC at settlement, not manual treasury transfers), and a parallel delegator-pool USDC→TOKEN swap path runs through the same Balancer V3 80/20 pool. The pool design — Balancer V3, 80/20 TOKEN/USDC, 1% swap fee, MEV protection via TWAP + `minTokenOut`, POL custody by the Timelock, and the activation criteria — is unchanged. The genesis liquidity allocation is **100M TOKEN at genesis (10% of the 1B supply)**. Both swap paths use **Flashbots-style private-RPC routing and per-epoch liquidity caps** (mandatory, not optional).

Buyback execution is deferred to production because thin TOKEN/USDC pool liquidity at genesis cannot absorb buyback flow without unacceptable slippage. [ADR 016](016-contract-interactions.md) recommends TWAP execution as MEV mitigation but does not specify how pool depth is created in the first place.

Concretely, the following questions are answered by this ADR:

1. How is the 10% genesis `Liquidity (DEX + CEX)` allocation ([ADR 026 §1](026-gauge-boost-tokenomics.md#1-supply-and-distribution)) actually deployed into the pool?
2. Is liquidity mercenary (LP rewards / liquidity mining) or protocol-owned?
3. Which venue and pool type — Uniswap V3 concentrated, Uniswap V2 full-range, Balancer weighted, or other?
4. How does buyback execution interact with the pool to avoid self-inflicted price impact?

The treasury holds the 100M TOKEN POL allocation per [ADR 026 §1](026-gauge-boost-tokenomics.md#1-supply-and-distribution) and is USDC-poor at PoC scale — there is no meaningful USDC reserve to pair 50/50 against TOKEN. The protocol team is small and does not have bandwidth to operate a concentrated-liquidity keeper stack. Both constraints favour a venue that minimizes USDC requirements and operational burden.

## Decision

**Use a Balancer V3 weighted pool (80% TOKEN / 20% USDC, 1% swap fee) as the canonical TOKEN/USDC venue. Seed it as Protocol-Owned Liquidity (POL) from the 10% genesis liquidity allocation. The DAO treasury holds the BPT (Balancer pool token) directly; no LP rewards, no liquidity mining, no dedicated `LiquidityManager` contract.**

This reverses the implicit Uniswap V3 choice in prior ADRs.

> **V2 vs V3 note.** An earlier draft of this ADR targeted Balancer V2. It was migrated to V3 before merge after the 2025-11-03 V2 Composable Stable Pool exploit (~$125M, per Certora / Trail of Bits / OpenZeppelin post-mortems) demonstrated a latent V2 codebase risk not present in V3's new Vault architecture. See the security section in Consequences for the affirmative justification. The core 80/20 weighted POL decision (USDC efficiency, IL alignment, zero-keeper posture) is a property of weighted pools in general and is not version-specific.

### Venue: Balancer V3 weighted pool vs Uniswap V3 concentrated

| Factor | Balancer V3 (80/20 weighted) | Uniswap V3 (concentrated) |
| --- | --- | --- |
| USDC required to pair 100M TOKEN at $0.01 anchor | ~$250K | ~$1M (50/50 range) [^v3-range] |
| Operational burden | None — set weights once | Active range management, keeper infra, rebalance transactions |
| Behavior when price exits anticipated range | Pool continues trading across full curve | Position becomes 100% one asset, earns zero fees |
| IL for a 2× price move | ~3.3% (80/20) | ~5.7% (50/50); position may be fully converted if out of range |
| MEV protection for buybacks | TWAP + `minTokenOut` + **mandatory Flashbots-style private-RPC routing** + per-epoch liquidity cap (per [ADR 026](026-gauge-boost-tokenomics.md) hardening); CoW Swap batch-auction routing as an independent conditional add-on, pending verification that CoW solvers route through Balancer V3 weighted pools (see Consequences) | Requires custom TWAP + private mempool (Flashbots Protect) |
| Fee capture per TVL (active conditions) | Lower | Higher (if well-managed and in-range) |
| Aggregator routing density on Arbitrum | Lower, and V3-specific (V3 ecosystem is newer than V2; aggregator coverage still maturing) | Higher |
| Security surface | Balancer V3 Vault architecture (transient accounting via `unlock`/settle, unified scaling); standard Weighted Pools are the simplest V3 pool type with no hooks | Uniswap V3 core — extensively audited and battle-tested |

[^v3-range]: The $1M V3 figure assumes a near-spot range of roughly equivalent effective depth to the Balancer 80/20 position at the same anchor price. It is an order-of-magnitude comparison, not an exact requirement — V3 USDC-side requirements depend entirely on the chosen range width, which this ADR does not fix.

The first three rows dominate the decision for a TOKEN-rich, USDC-poor treasury with a small team. Uniswap V3's wins (fee capture, routing density) presuppose active range management the protocol team cannot provide during the PoC and early production.

**Weights rationale.** 80/20 TOKEN-heavy lets the treasury seed the pool with ~1/4 the USDC of a 50/50 position while retaining comparable near-spot depth for small trades. It also reduces IL for a given TOKEN price move by roughly 1.75× compared to 50/50 (at a 2× price move: ~3.3% vs ~5.7%), which aligns the DAO's LP position with the protocol's own upside thesis — if TOKEN appreciates with network usage, the DAO keeps more of the upside. Derivation: `IL = r^w_TOKEN / (w_TOKEN·r + w_USDC) − 1`; at `r=2`, `w_TOKEN=0.8` gives `2^0.8 / 1.8 − 1 ≈ −3.27%` and `w_TOKEN=0.5` gives `√2 / 1.5 − 1 ≈ −5.72%`.

**Fee tier rationale.** 1% is appropriate for a long-tail asset with limited trading activity. Lower tiers (0.3%, 0.05%) assume volume sufficient to compensate LPs, which TOKEN will not have at PoC scale.

### Protocol-Owned Liquidity mechanics

- **Source:** The `Genesis liquidity (POL)` allocation (10% of supply = 100M TOKEN of the 1B supply per [ADR 026 §1](026-gauge-boost-tokenomics.md#1-supply-and-distribution)) funds the TOKEN side. The USDC side is drawn from the treasury.
- **PoC seed:** A minimal position for integration testing. Exact amount is chosen by governance at deployment; the target range of $5K–$20K USDC equivalent per side reflects expected PoC scale (20–100 nodes). The range is wide because the point estimate depends on observed PoC traffic once nodes come online; deployment scripts should accept the seed size as a parameter rather than hardcoding it.
- **Production seed:** Sized so that a single `maxBuybackAmount` swap causes less than `slippageBps` price impact. This makes buyback execution well-conditioned on its own pool.
- **Custody:** BPT is held by the DAO treasury address. PoC: admin key. Production: Governor + `TimelockController`. No withdraw path to an EOA — liquidity exit requires a governance proposal through the timelock. This invariant is enforced by BPT being held at the Timelock address and by the absence of any bespoke withdraw function; Balancer itself has no protocol-level lockup, so custody discipline is the sole enforcement mechanism.
- **No `LiquidityManager` contract.** A weighted pool's curve handles rebalancing implicitly via arbitrage. There is no range to manage, no `rebalance()` keeper, no `KEEPER_ROLE` for liquidity operations. The only privileged operation on the BPT position is emergency withdrawal by governance, which uses the standard timelock path — no bespoke contract required.
- **No liquidity mining in v1.** Mercenary LPs exit when rewards stop and consume TOKEN supply for a benefit that POL provides more reliably. Liquidity mining can be introduced by a future ADR if organic pool depth proves insufficient despite POL seeding.

### Buyback inflow source and rate (router-driven per ADR 026)

Under [ADR 026](026-gauge-boost-tokenomics.md), `BuybackBurner` no longer relies on manual treasury transfers to accumulate USDC. The `FeeRouter` contract receives the full operator USDC balance from `PaymentChannel.settleChannel` at every settlement and atomically forwards **5% of routed USDC directly to `BuybackBurner` in the same transaction**, alongside the other fee buckets. The full router split lives in [ADR 026 §2](026-gauge-boost-tokenomics.md#2-feerouter-split-40407553) (six buckets summing to 100%); this ADR cross-references rather than duplicating the table to avoid drift.

**Implications for this ADR:**

- **Inflow source.** The contract receiving USDC at the `BuybackBurner` address is now the `FeeRouter` (via a same-transaction `transfer` call inside `routeSettlement`). Manual treasury transfers as the funding pattern for `BuybackBurner` are obsolete. Anywhere prior drafts of this ADR or [ADR 016](016-contract-interactions.md) imply a manual or batched-treasury inflow path, the operative pattern is now router-driven, per-settlement inflow.
- **Inflow rate.** [ADR 026](026-gauge-boost-tokenomics.md) routes 5% of gross routed USDC (the full operator balance, post-settlement) to `BuybackBurner`. Production sizing of `maxBuybackAmount`, the keeper schedule, and the slippage-budget assumptions in the [Activation Criteria](#activation-criteria-production) below should be tuned against this inflow rate.
- **Mechanics unchanged.** The Balancer V3 80/20 pool, the swap call pattern, the TWAP + `minTokenOut` defense, the POL custody model, and the per-transaction `maxBuybackAmount` cap all carry over from this ADR's existing design with no modification. Only the source of USDC and its rate change.

### Buyback execution via Balancer V3

The canonical `IBuybackBurner` interface is defined in [ADR 003 — BuybackBurner](003-payments.md#buybackburner); that block is the single source of truth and is not duplicated here to avoid cross-ADR drift.

The core `executeBuyback(address token, uint256 amount, uint256 minTokenOut)` call pattern is **unchanged** for the Balancer V3 venue. What this ADR adds is one additional configuration setter, `setPool(address pool)`, so the same interface can select a venue-specific pool identifier symmetrically across Balancer V3 and any future Uniswap V3 deployment (both identify pools by contract address). Deployment configuration for the Balancer V3 venue:

- `setSwapRouter(address)` is set to the router contract used for the **Balancer V3 Vault** on the production L2. On Arbitrum mainnet this is `0xEAedc32a51c510d35ebC11088fD5fF2b47aACF2E`. Balancer's deployment registry labels this contract **`Router v2`** — that label is the second iteration of the Balancer V3 Router artifact (a contract versioning within Balancer V3), **not** a reference to Balancer V2 protocol routing. This is the correct router address to use for the Balancer V3 venue on Arbitrum. Sepolia and other testnet addresses MUST be pulled from [`balancer-deployments/addresses/arbitrum-sepolia.json`](https://github.com/balancer/balancer-deployments) at deployment time and not hardcoded in this ADR.
- `setPool(address)` is set to the contract address of the deployed 80/20 TOKEN/USDC Weighted Pool. V3 identifies pools by their contract address directly; the V2 `bytes32 poolId` abstraction is gone. The setter is venue-symmetric — a Uniswap V3 deployment would hold the V3 pool contract address here, with no bytes-cast gymnastics. The pool address MAY also be provided via the `BuybackBurner` constructor (see [ADR 016 — Constructor Arguments](016-contract-interactions.md#2-deployment-order-and-initialization-dependencies) row 5); constructor-time and setter-time semantics are equivalent, and `executeBuyback()` MUST revert if the configured pool address is the zero address, regardless of how it got there.
- **Approvals footgun — the contract self-approves the Vault.** The `BuybackBurner` contract itself MUST issue `USDC.approve(vaultAddress, type(uint256).max)` (or a bounded allowance refreshed via an admin function) as part of its deployment / initialization sequence. This is NOT a runbook step executed by a multisig against the `BuybackBurner`'s USDC balance — the `BuybackBurner` is the `msg.sender` of the Router call, and the V3 Vault pulls input tokens from the allowance held by that `msg.sender`. The approval target is the **Balancer V3 Vault** address (`0xbA1333333333a1BA1108E8412f11850A5C319bA9` on Arbitrum mainnet), *not* the Router, even though `executeBuyback()` *calls* the Router. This "call the Router, but approve the Vault" split is the single most common V2→V3 integration mistake; a contract that approves the Router instead of the Vault will revert on every swap. The approval itself is issued by `BuybackBurner` code, not by a human operator.

#### Single-swap (non-TWAP) execution

When `subSwapCount = 1`, `executeBuyback()` calls `Router.swapSingleTokenExactIn(pool, USDC, TOKEN, amount, minTokenOut, deadline, false, "")` on the Balancer V3 Router with the stored `pool` address, `tokenIn = USDC`, `tokenOut = TOKEN`, `exactAmountIn = amount` (from the caller), `minAmountOut = minTokenOut` (the caller-supplied sandwich guard applied directly), `wethIsEth = false` (USDC/TOKEN, neither is WETH), and `userData = ""` (no hook parameters for a standard weighted pool). The existing `maxBuybackAmount` and `slippageBps` guards apply unchanged — `amount` MUST be ≤ `maxBuybackAmount`. V3's Router function name encodes swap kind (`ExactIn`); there is no separate `kind = GIVEN_IN` field as there was in V2's `SingleSwap` struct.

#### TWAP policy (`subSwapCount > 1`)

Balancer's smoother curve reduces the need for TWAP compared to V3's concentrated bands but does not eliminate it for large buybacks. TWAP + `minTokenOut` + **mandatory private-RPC routing** + **per-epoch liquidity caps** form the MEV defense stack. CoW Swap (below) is a conditional add-on, not a substitute, and is independent of the mandatory private-RPC requirement. When splitting:

- **`minTokenOut` is the aggregate minimum** TOKEN output required across the full buyback request, not a per-sub-swap minimum. The contract MUST track cumulative TOKEN received and MUST ensure the final cumulative output is ≥ `minTokenOut` for the split request to be considered satisfied. A naive per-sub-swap guard of `minTokenOut / subSwapCount` is rejected because integer truncation allows the aggregate to fall below the caller's requested minimum.
- **Per-sub-swap guard** is derived from the remaining required output and remaining sub-swaps: `remainingMinOut = minTokenOut − cumulativeReceived`; `perSubSwapLimit = ceilDiv(remainingMinOut, remainingSubSwaps)`. This keeps the aggregate bound tight even under uneven per-sub-swap fills.
- **`maxBuybackAmount` is a per-transaction cap**, consistent with its definition at [ADR 016 — BuybackBurner reentrancy row](016-contract-interactions.md#buybackburner). Each sub-swap (being its own transaction) MAY be as large as `maxBuybackAmount`; the *total* buyback is bounded by the caller-supplied `amount`, not by `maxBuybackAmount × subSwapCount`. Dividing the per-transaction cap by the split count would unnecessarily restrict the protocol's ability to process accumulated fees.
- **Spacing.** Sub-swaps are separated by at least `subSwapMinBlockGap` blocks.
- **Required: Flashbots-style private-RPC routing (per [ADR 026](026-gauge-boost-tokenomics.md) and the market-dynamics analysis §1).** The keeper MUST submit every buyback sub-swap (and every delegator-pool sub-swap, see [Delegator-pool swap path](#delegator-pool-swap-path-parallel-usdctoken-conversion-on-the-same-pool) below) through a private-RPC bundle endpoint that bypasses the public mempool — Flashbots Protect, MEV-Share, or the equivalent on the production L2. Public-mempool routing is **not** an acceptable fallback: programmatic buybacks are a known MEV target (front-run-and-dump pattern around `executeBuyback` calls), and TWAP + `minTokenOut` alone do not prevent a searcher from observing the pending transaction and skewing the pool against the protocol within the slippage budget. This is a hard requirement; the keeper configuration MUST refuse to publish to a public RPC.
- **Required: per-epoch liquidity cap.** Independent of `maxBuybackAmount` (a per-transaction cap), the contract MUST enforce a per-epoch aggregate cap on the USDC notional that may be swapped through the Balancer V3 pool across all buyback executions in a single 1-week epoch. The cap is sized as a fixed fraction (governance-set) of in-pool USDC depth at epoch start, so that even a worst-case sustained-execution profile cannot exhaust pool depth. This is a hard requirement, not a tunable nicety; the cap also bounds the delegator-pool swap path's pressure on the same pool (see below). Calibration of the cap fraction is deferred to the operator runbook (and tracked via the `finance/notebooks/` analyses); it is not parameterized in [ADR 026 §11](026-gauge-boost-tokenomics.md#11-governable-parameters-with-safety-bounds).
- **Conditional add-on: CoW Swap routing.** The keeper MAY route the buyback through CoW Swap instead of calling the Router directly, **if CoW solvers route through the deployed Balancer V3 weighted pool at best-execution time**. This is a structural MEV-protection layer (batch auctions, no public mempool exposure) that complements TWAP but is NOT guaranteed to be available for Balancer V3 as of this ADR's date: Balancer's V2-only aggregator fee-discount program was explicitly not extended to V3, and CoW's solver routing coverage of V3 weighted pools is still maturing. **Before enabling CoW routing in production, the operator MUST verify via CoW's `/api/v1/quote` endpoint that the target pool is reachable and that quoted prices are within `slippageBps` of the Router-direct path.** If CoW routing is unavailable or regressed, fall back to direct Router + TWAP.
- **Partial execution.** If a later sub-swap reverts (e.g., its derived `remainingMinOut` cannot be satisfied), earlier sub-swaps stand and their output is retained; residual USDC remains in the contract until the next execution. The original split request MUST NOT be treated as having satisfied `minTokenOut` unless cumulative output across its sub-swaps meets the aggregate minimum.

### Delegator-pool swap path (parallel USDC→TOKEN conversion on the same pool)

[ADR 026 §6](026-gauge-boost-tokenomics.md#6-delegator-pool-usdc-token-conversion) introduces a 7% delegator-pool bucket that is also denominated in TOKEN, not USDC. The conversion path runs through **the same Balancer V3 80/20 pool** this ADR seeds for buyback execution. From this ADR's perspective, the delegator-pool swap is a second consumer of the same pool with the same MEV-defense requirements as `BuybackBurner`.

**Mechanics.**

1. `FeeRouter` accumulates 7% of routed USDC into the delegator-pool epoch bucket per epoch.
2. At epoch rollover (or via keeper trigger within the epoch), the bucket's USDC is swapped to TOKEN against the same Balancer V3 80/20 pool, under the same execution rules as `BuybackBurner`: TWAP splitting, `minTokenOut` aggregate guard, `subSwapMinBlockGap` spacing, mandatory private-RPC routing, and the per-epoch liquidity cap (which spans both swap paths combined).
3. The acquired TOKEN is held in the per-epoch delegator bucket until claimed pro-rata by ve-balance (mechanic per [ADR 026 §6](026-gauge-boost-tokenomics.md#6-delegator-pool-usdc-token-conversion); not this ADR's concern).

**Implementation per [ADR 016 § Shared swap helper](016-contract-interactions.md#shared-swap-helper-buybackburner--delegatorbuyer).** A parallel `DelegatorBuyer` contract handles the delegator-pool path. It shares the Balancer V3 swap execution — TWAP splitting, `minTokenOut` aggregate guard, `subSwapMinBlockGap` spacing, private-RPC routing, and combined per-epoch-cap accounting — with `BuybackBurner` through an internal `BalancerV3SwapHelper` abstract contract inherited by both. Separate contract addresses preserve independent governance setters (`FeeRouter.setBuybackBurner` / `setDelegatorBuyer`) and divergent downstream value flows (burn vs deposit-back).

#### Liquidity-cap interaction

The per-epoch liquidity cap defined above is a **single pool-wide budget** shared across both the buyback and delegator paths. Under sustained heavy network revenue, the two flows together MUST NOT exceed the cap; the implementing contract(s) MUST track combined epoch-to-date notional. Sizing rationale: Balancer V3's curve is smoother than V3 concentrated bands, but two independent keepers each calling near-`maxBuybackAmount` repeatedly within an epoch can still chew through pool depth.

### Parameter Table

| Parameter | PoC | Production | Governable |
| --- | --- | --- | --- |
| Venue (`setSwapRouter`) | Balancer V3 Router (address on L2) | Balancer V3 Router (address on L2) | Yes (via `setSwapRouter`) |
| Token approvals spender | Balancer V3 Vault (distinct from Router — approvals go to the Vault even though calls go to the Router) | Balancer V3 Vault | No (structural V3 requirement) |
| Pool identifier (`pool`, `address`) | Deployed pool contract address | Deployed pool contract address | Yes (via `setPool`) |
| Pool weights | 80% TOKEN / 20% USDC | 80% TOKEN / 20% USDC | No (fixed at pool creation) |
| Pool swap fee | 1% (100 bps) | 1% (100 bps) | Governable on the Balancer pool itself |
| PoC seed size | $5K–$20K USDC equivalent per side | N/A | N/A |
| Production seed size | N/A | Sized so `maxBuybackAmount` causes < `slippageBps` impact | Via governance proposal (treasury disbursement) |
| `minBuybackAmount` | N/A (execution disabled) | 100 USDC | Yes |
| `maxBuybackAmount` | N/A (execution disabled) | Set so a single swap causes < `slippageBps` impact | Yes |
| `slippageBps` | N/A (execution disabled) | 200 bps (2%) | Yes |
| TWAP `subSwapCount` | 1 (no splitting) | 4 (default, governable) [^subswap-rationale] | Yes |
| TWAP `subSwapMinBlockGap` | N/A | 10 blocks (~2 minutes on Arbitrum) | Yes |
| Private-RPC routing | N/A (execution disabled) | **Required** — Flashbots Protect / MEV-Share equivalent on the production L2; public-mempool routing is not an acceptable fallback for either buyback or delegator-pool swaps | No (hard requirement; only the endpoint URL is operator-configurable) |
| Per-epoch liquidity cap (combined buyback + delegator) | N/A (execution disabled) | **Required** — fixed fraction of in-pool USDC depth at epoch start (governance-set; placeholder default to be set in implementing ADR) | Yes (the fraction; presence of the cap is structural) |
| Execution activation | Disabled — fees accumulate only | Governance vote required to enable | — |

[^subswap-rationale]: A default of 4 balances MEV mitigation against gas overhead and keeper complexity. 2 sub-swaps provides marginal splitting benefit; ≥8 multiplies keeper gas and `ceilDiv` rounding artifacts without proportionate MEV improvement on a weighted pool (where curvature is already smoother than V3 concentrated bands). Governance may tune this once production buyback volumes are observed.

### Activation Criteria (Production)

Buyback execution should be enabled by governance vote only when all of the following hold:

1. The Balancer V3 80/20 TOKEN/USDC Weighted Pool has been deployed and seeded with POL at the production seed size, and its contract address has been set via `setPool(address)`.
2. The `BuybackBurner` contract has been configured with the Balancer V3 Router address via `setSwapRouter(address)`, and USDC has been approved to the Balancer V3 **Vault** address (not the Router — see the approvals footgun in [Buyback execution via Balancer V3](#buyback-execution-via-balancer-v3)).
3. A single swap of size `maxBuybackAmount` against the pool causes less than `slippageBps` price impact.
4. Accumulated USDC in the `BuybackBurner` contract exceeds `minBuybackAmount`.
5. Either (a) a keeper with `KEEPER_ROLE` is operational and configured to call `executeBuyback()` (and the delegator-pool swap entry point per [Delegator-pool swap path](#delegator-pool-swap-path-parallel-usdctoken-conversion-on-the-same-pool)) on a schedule, or (b) the governance process is prepared to trigger executions manually.
6. **Required: private-RPC routing in place.** The keeper(s) — buyback and delegator-pool — MUST be configured to submit through Flashbots Protect or the equivalent private-bundle endpoint on the production L2 before execution is enabled. A pre-flight check that public-mempool publication is refused MUST be part of activation runbook sign-off.
7. **Required: per-epoch liquidity cap configured and shared.** The combined-path per-epoch liquidity cap MUST be set on-chain to a value calibrated against epoch-start in-pool USDC depth, and MUST be enforced across both the `BuybackBurner` and `DelegatorBuyer` swap entry points via the shared `BalancerV3SwapHelper` per [ADR 016 § Shared swap helper](016-contract-interactions.md#shared-swap-helper-buybackburner--delegatorbuyer).
8. **(If CoW routing is to be used as an add-on)** The operator has verified via CoW's `/api/v1/quote` endpoint that CoW solvers route through the deployed Balancer V3 pool and that quoted prices are within `slippageBps` of the Router-direct path. If this verification fails, disable CoW routing and fall back to direct Router + TWAP + private-RPC; this does not block activation. CoW does not substitute for the criterion-6 private-RPC requirement on the non-CoW path.

Criteria 1–4 are quantitative; governance voters verify them off-chain before enabling execution. Criterion 2 is a one-time deployment check. Criteria 6 and 7 are hard structural requirements (non-negotiable); criterion 8 is venue-integration health, not a gating requirement.

## Alternatives Considered

The Uniswap V3 alternative — including the *if and when to revisit* triggers governance can act on — is recorded in [`_history/alternatives-pre-launch.md` § ADR 018 — Liquidity Strategy](_history/alternatives-pre-launch.md#adr-018--liquidity-strategy).

## Consequences

**Positive:**

- Seeds pool with ~1/4 the USDC of an equivalent 50/50 V3 position — critical for a USDC-poor treasury.
- Zero keeper / range-management operational burden. No `LiquidityManager` contract, no `KEEPER_ROLE` for liquidity operations, no off-chain monitoring infrastructure beyond what already exists for `executeBuyback()`.
- IL profile (80/20 weighted) aligns with the protocol's TOKEN-upside thesis; the DAO keeps more of TOKEN's appreciation than a 50/50 position would.
- TWAP + `minTokenOut` guards provide deterministic on-chain MEV protection. Combined with the [ADR 026](026-gauge-boost-tokenomics.md) hardening — mandatory Flashbots-style private-RPC routing and a per-epoch liquidity cap shared across the buyback and delegator-pool swap paths — programmatic-buyback front-running (the canonical MEV pattern flagged in the market-dynamics analysis) is closed off both by transaction-graph invisibility (private RPC) and by execution shape (TWAP, slippage guard, hard cap on per-epoch pool pressure). CoW Swap batch-auction routing is available as an independent conditional add-on if CoW solvers route through the deployed pool (verified per the activation criteria), but it is not a load-bearing assumption.
- Balancer V3's new Vault architecture (transient accounting via `unlock`/settle, unified scaling) specifically mitigates the bug class exploited in V2 (see Negative section for details). Choosing V3 over V2 materially reduces smart-contract risk.
- `IBuybackBurner` remains venue-agnostic — switching or adding venues later is a deployment-time configuration change, not an interface change.
- POL is non-extractable by LPs because there are no external LPs. The DAO cannot be rugged by mercenary liquidity leaving at the worst moment.

**Negative:**

- Fee capture per dollar of TVL is lower than a well-managed V3 concentrated position. The DAO earns less from LP fees than it theoretically could.
- Aggregator routing density for Balancer V3 pools on Arbitrum is lower than for Uniswap V3, *and* materially lower than for Balancer V2 (V3 is newer; aggregator coverage and solver integrations are still maturing as of this ADR's date). Third-party traders using their preferred aggregator may not find the pool. This is a transient concern that should improve as V3 ages, but is a real friction today.
- **Security — realized V2 event, V3 shorter track record.** On 2025-11-03, Balancer V2 Composable Stable Pools were exploited for ~$125M across multiple chains. Root cause (per Certora, Trail of Bits, OpenZeppelin post-mortems): a rounding-direction bug in `_upscale`/`_downscale` latent since 2021, exploited via `EXACT_OUT` + `batchSwap` to manipulate BPT pricing. The exploit affected V2 Composable Stable Pools specifically, **not** V2 standard Weighted Pools and **not** Balancer V3. Certora explicitly cites V3's new Vault architecture (transient accounting via `unlock`/settle, unified scaling) as mitigating this bug class. This ADR uses V3 (not V2) as the affirmative response to that incident, and further restricts the choice to **standard V3 Weighted Pools with no hooks** — the simplest V3 pool family. V3's pool taxonomy as of 2026-04 on Arbitrum includes standard pool families (Weighted, Stable, Stable Surge, LBP, ReClamm, Gyro 2CLP/ECLP), "Boosted" variants (weighted or stable pools composed of ERC-4626 wrapped tokens via the BufferRouter — this is a composition pattern, not a separate pool family), and hooks-based custom pools. Third-party hook-based families such as QuantAMM exist on other chains but are not deployed on Arbitrum per the `balancer/balancer-deployments` registry snapshot. None of these other than **standard V3 Weighted Pools with no hooks** are in scope for this ADR. **Residual risk:** V3 has been in production for materially less time than V2 and has fewer integration-hours behind its audits. As of 2026-04-04 there are no publicly disclosed V3 Vault incidents, but the protocol team MUST track Balancer security advisories (especially any affecting Weighted Pools) and treat a future advisory as a trigger to reassess the venue under the revisit criteria above.
- Treasury bears impermanent loss directly. Unlike mercenary LPs who absorb IL in exchange for rewards, the DAO absorbs IL without an explicit reward stream. Buyback-and-burn provides an indirect reward loop (fees → buyback → TOKEN appreciation → LP position value), but the loop is weak at PoC scale. The worst-case IL exposure is bounded by the 10% genesis `Liquidity (DEX + CEX)` allocation plus whatever USDC the treasury chooses to pair against it (see [ADR 026 §1](026-gauge-boost-tokenomics.md#1-supply-and-distribution)).
- One new interface method (`setPool(address)`) must be added to `IBuybackBurner`. This is a minor interface extension but must be reflected in the ADR 003 interface block. V3's approvals footgun (approve the Vault, call the Router) must also be documented in deployment runbooks to prevent a common integration mistake.
- **Mandatory private-RPC dependency.** [ADR 026](026-gauge-boost-tokenomics.md) makes Flashbots-style private-RPC routing a hard requirement (was: optional add-on). This introduces a third-party operational dependency on the chosen private-bundle provider for the production L2 — provider downtime or de-listing of the protocol's bundles is a new failure mode. Mitigated by selecting a provider with a strong uptime track record on the production L2 and by keeping the keeper code provider-agnostic (so a swap of providers is configuration, not a redeployment). The cost of this dependency is judged proportionate to the MEV exposure the higher per-revenue inflow rate (~8× the prior manual-treasury rate) creates.
- **Two consumers on one pool.** The delegator-pool swap path (per [ADR 026 §6](026-gauge-boost-tokenomics.md#6-delegator-pool-usdc-token-conversion)) routes a parallel TWAP USDC→TOKEN flow through the same Balancer V3 pool as buybacks. The combined-path per-epoch liquidity cap defends pool depth, but the contract surface, keeper coordination, and audit scope grow. The combined-path notional tracking — shared between `BuybackBurner` and the parallel `DelegatorBuyer` via [ADR 016 § Shared swap helper](016-contract-interactions.md#shared-swap-helper-buybackburner--delegatorbuyer) — is a non-trivial new invariant.

## References

- [ADR 003 — Payment Channels (IBuybackBurner interface)](003-payments.md#buybackburner)
- [ADR 009 — Governance Model](009-governance.md)
- [ADR 016 — Smart Contract Interaction Model](016-contract-interactions.md)
- [ADR 026 — Gauge-Boost Tokenomics](026-gauge-boost-tokenomics.md) — Source of the router-driven inflow, the 5%-of-routed-USDC buyback rate, the parallel delegator-pool swap path, and the hardened MEV-mitigation requirements applied to this ADR
