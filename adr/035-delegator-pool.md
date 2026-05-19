# ADR 035: Delegator Pool

**Date:** 2026-04-25
**Status:** Draft

## Context

The 7% delegator bucket of the `FeeRouter` six-bucket split ([ADR 026 §2](026-tokenomics.md#2-feerouter-split-40407553)) flows through a USDC→TOKEN buy-and-distribute pipeline (the `DelegatorBuyer` contract) rather than direct USDC distribution. This ADR specifies that pipeline: its distinction from buyback-and-burn, why it is TOKEN-denominated, MEV/slippage handling, and the `DelegatorBuyer` interface. The economic-model umbrella is [ADR 026](026-tokenomics.md#adr-026-tokenomics).

## Decision

### Delegator pool — USDC → TOKEN conversion

The 7% delegator bucket flows through a USDC→TOKEN buy-and-distribute pipeline rather than direct USDC distribution.

1. `FeeRouter` accumulates 7% of routed USDC into the delegator-pool epoch bucket per epoch.
2. At epoch rollover (or via keeper trigger within the epoch), the bucket's USDC is swapped for TOKEN against the Balancer V3 80/20 pool ([ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol)) under the same TWAP + minOut + per-epoch liquidity-cap protections as `BuybackBurner`. Implementation is a parallel `DelegatorBuyer` contract per [ADR 016 § Shared swap helper](016-contract-interactions.md#shared-swap-helper-buybackburner--delegatorbuyer); the two contracts share the swap execution path through an internal `BalancerV3SwapHelper` abstract contract while preserving distinct downstream destinations and governance setters.
3. The acquired TOKEN is held in the delegator-pool epoch bucket as TOKEN.
4. Delegators / ve-lockers call `FeeRouter.claimDelegator(epochs[])`. Payout per locker = `ve_i / total_ve_at_epoch_boundary × token_in_delegator_bucket[epoch]`.

#### Distinction from buyback-and-burn

Both are buy-side market pressure on USDC→TOKEN. Burn removes TOKEN from circulation; the delegator pool routes TOKEN to long-term ve-locked holders. Both are required.

#### Why TOKEN-denominated, not USDC?

Routes acquired TOKEN to the participants with the longest commitment horizon and couples ve-locker yield to TOKEN value rather than to network revenue alone — when network revenue grows, TOKEN buy pressure grows, ve-locker positions appreciate. This is the model's primary "real yield in TOKEN" lever; an alternative pattern — USDC distribution to a passive ve-pool — was considered and rejected.

#### MEV / slippage

TWAP windows + per-epoch liquidity caps + private-RPC routing (Flashbots-style bundles) for the swap. Same defenses as the [ADR 018](018-liquidity-strategy.md#adr-018-liquidity-strategy-balancer-8020-pol) buyback flow; per-epoch liquidity caps are a hard requirement on this path, not optional.

#### Contract: DelegatorBuyer

```solidity
interface IDelegatorBuyer {
    // ─── Per-epoch USDC → TOKEN swap (FeeRouter-only) ─────────────────
    // Called by `FeeRouter.executeDelegatorSwap`. `msg.sender == feeRouter`
    // is the only auth check (single-purpose helper trusting FeeRouter
    // exclusively; no post-deploy role grants). Swaps `amountIn` USDC for
    // at least `minOut` TOKEN against the configured Balancer V3 pool,
    // then deposits the TOKEN back via
    // `IFeeRouter.depositDelegatorTokens(epoch, amount)` — see
    // [ADR 016 § Contract: FeeRouter](016-contract-interactions.md#contract-feerouter).
    // Same Vault-scoped self-approval pattern as `BuybackBurner`.
    function swapDelegatorBucket(
        uint64 epochId,
        uint256 amountIn,
        uint256 minOut
    ) external;

    // ─── Read views ───────────────────────────────────────────────────
    function feeRouter() external view returns (address);
    function pool() external view returns (address);
    function slippageToleranceBps() external view returns (uint256);
    function minSwapAmount() external view returns (uint256);
    function maxSwapAmount() external view returns (uint256);

    // ─── Governance setters ───────────────────────────────────────────
    function setFeeRouter(address newFeeRouter) external;
    function setPool(address newPool) external;
    function setSlippageToleranceBps(uint256 bps) external;
    function setMinSwapAmount(uint256 amount) external;
    function setMaxSwapAmount(uint256 amount) external;

    // ─── Pause control ────────────────────────────────────────────────
    function pause() external;
    function unpause() external;

    // ─── Events ───────────────────────────────────────────────────────
    event DelegatorSwapped(uint64 indexed epochId, uint256 amountIn, uint256 amountOut);
    event FeeRouterUpdated(address indexed oldRouter, address indexed newRouter);
    event PoolUpdated(address indexed oldPool, address indexed newPool);
    event SlippageToleranceUpdated(uint256 oldBps, uint256 newBps);
    event MinSwapAmountUpdated(uint256 oldValue, uint256 newValue);
    event MaxSwapAmountUpdated(uint256 oldValue, uint256 newValue);
}
```

**Notes:**

- **Parallel contract to `BuybackBurner`** per [ADR 016 § Shared swap helper](016-contract-interactions.md#shared-swap-helper-buybackburner--delegatorbuyer). The two contracts share the Balancer V3 swap execution path through an internal `BalancerV3SwapHelper` abstract contract while keeping separate addresses, separate governance setters on `FeeRouter`, and divergent downstream value flows (burn vs deposit-back).
- **`msg.sender == feeRouter` as the sole auth check.** No `KEEPER_ROLE` on `DelegatorBuyer` because there are no other legitimate callers — keepers trigger swaps via `FeeRouter.executeDelegatorSwap(epoch, minOut)` (which holds `KEEPER_ROLE` on `FeeRouter`), and `FeeRouter` then calls `swapDelegatorBucket` here. Single trust boundary; one role grant fewer post-deploy.
- **`setFeeRouter` carve-out** matches the [ADR 016 § No proxy deployment patterns](016-contract-interactions.md#no-proxy-deployment-patterns) carve-out for non-signing helper addresses: `DelegatorBuyer` has no domain-separator-bound state, so re-pointing the configured `FeeRouter` is safe under the standard 48h timelock.
