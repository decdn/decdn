// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { Math } from "@openzeppelin/contracts/utils/math/Math.sol";

import { BuybackBurner } from "./BuybackBurner.sol";
import { GuardedBuybackBurner } from "./GuardedBuybackBurner.sol";
import { IUniswapV3SwapRouter } from "./interfaces/IUniswapV3SwapRouter.sol";
import { IUniswapV3Pool } from "./interfaces/IUniswapV3Pool.sol";

/// @title BuybackBurnerUniswapV3
/// @notice Concrete `GuardedBuybackBurner` that binds a Uniswap V3 `SwapRouter02`
///         and a single USDC/TOKEN pool, implementing the venue hooks behind the
///         shared MEV stack ([ADR 018](../adr/018-liquidity-strategy.md)): a
///         single `exactInputSingle` USDC->TOKEN swap with a scoped router
///         approval, the pool's `slot0` marginal spot feeding the inherited TWAP
///         floor, the fee-tier-derived swap fee, and the in-pool USDC balance
///         feeding the inherited per-epoch cap.
/// @dev    Initial-network venue on Arbitrum Sepolia, where Uniswap V3 is live
///         but Balancer V3 is not. The `FeeRouter.buybackBurner` is swappable in
///         one governance call, so this can be replaced by
///         `BuybackBurnerBalancerV3` later with no FeeRouter change.
///
///         TWAP source: like the Balancer burner this uses the inherited
///         hand-rolled accumulator (NOT the pool's native `observe()` oracle) —
///         it samples `_spotPrice` from `slot0.sqrtPriceX96`, advanceable
///         permissionlessly via `poke()`, fail-closed until `twapMinWindow`
///         matures. This keeps both burners on one TWAP implementation and works
///         on a freshly-seeded pool whose observation cardinality is still 1.
///
///         Token-pull: `SwapRouter02` pulls `tokenIn` via a direct ERC20
///         allowance (no Permit2 leg), so the swap scopes `forceApprove(router,
///         amountIn)` and resets it to 0 — the V2-style approval the base
///         invariant #2 contemplates.
contract BuybackBurnerUniswapV3 is GuardedBuybackBurner {
    using SafeERC20 for IERC20;

    /// @dev 2**96 — the Q64.96 fixed-point denominator of `slot0.sqrtPriceX96`.
    uint256 internal constant Q96 = 1 << 96;

    /// @dev `fee()` is in hundredths of a bip (1e-6); scale to a 1e18 fraction.
    uint256 internal constant FEE_TO_E18 = 1e12;

    // -----------------------------------------------------------------
    // Governance-mutable venue
    // -----------------------------------------------------------------

    /// @notice Uniswap V3 `SwapRouter02` (the swap call + token-pull target). Set
    ///         in the constructor and rotatable via `setSwapRouter`.
    IUniswapV3SwapRouter public swapRouter;

    /// @notice The USDC/TOKEN Uniswap V3 pool. Marginal spot + fee-tier source;
    ///         may be zero at deploy (governance wires it via `setPool`).
    address public pool;

    // -----------------------------------------------------------------
    // Events / Errors
    // -----------------------------------------------------------------

    event SwapRouterUpdated(address indexed oldAddr, address indexed newAddr);
    event PoolUpdated(address indexed oldAddr, address indexed newAddr);

    error PoolStateInvalid();

    // -----------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------

    /// @param swapRouter_ Uniswap V3 `SwapRouter02`. Non-zero.
    /// @param pool_ USDC/TOKEN V3 pool (may be zero — governance wires it later
    ///        via `setPool`; `executeBuyback` reverts `PoolNotWired` until set).
    struct Config {
        IUniswapV3SwapRouter swapRouter_;
        address pool_;
        uint256 twapMinWindow_;
        uint256 maxBuybackAmount_;
        uint256 minBuybackAmount_;
        uint256 slippageBps_;
        uint256 epochLiquidityCapFraction_;
    }

    constructor(IERC20 usdc_, ERC20Burnable token_, address admin, Config memory cfg)
        GuardedBuybackBurner(
            usdc_,
            token_,
            admin,
            GuardParams({
                twapMinWindow_: cfg.twapMinWindow_,
                maxBuybackAmount_: cfg.maxBuybackAmount_,
                minBuybackAmount_: cfg.minBuybackAmount_,
                slippageBps_: cfg.slippageBps_,
                epochLiquidityCapFraction_: cfg.epochLiquidityCapFraction_
            })
        )
    {
        if (address(cfg.swapRouter_) == address(0)) revert ZeroAddress();
        swapRouter = cfg.swapRouter_;
        pool = cfg.pool_;
        // Fail-fast on a mis-wired pool supplied at deploy, symmetric with
        // `setPool` and the Balancer burner's constructor validation: a non-zero
        // pool must read as a USDC/TOKEN pair with a live price (`_spotPrice`
        // reverts otherwise). Zero stays the deferred-wiring path.
        if (cfg.pool_ != address(0)) _spotPrice();
    }

    // -----------------------------------------------------------------
    // Venue hooks
    // -----------------------------------------------------------------

    /// @inheritdoc BuybackBurner
    function _requireWired() internal view override {
        if (pool == address(0) || address(swapRouter) == address(0)) revert PoolNotWired();
    }

    /// @inheritdoc GuardedBuybackBurner
    /// @dev Scoped router approval (base header invariant #2, V2-style): a
    ///      `SwapRouter02` pulls `tokenIn` via a direct ERC20 allowance, so
    ///      approve exactly `amountIn` to the router, swap, then reset to 0 so no
    ///      standing allowance survives. The fee tier comes from the bound pool;
    ///      `sqrtPriceLimitX96 = 0` leaves the keeper `minOut`/inherited TWAP
    ///      floor as the sole slippage gate (plus the ADR 018 private-RPC bundle).
    function _swap(uint256 amountIn, uint256 minOut) internal override returns (uint256 tokenOut) {
        uint24 fee = IUniswapV3Pool(pool).fee();
        usdc.forceApprove(address(swapRouter), amountIn);
        tokenOut = swapRouter.exactInputSingle(
            IUniswapV3SwapRouter.ExactInputSingleParams({
                tokenIn: address(usdc),
                tokenOut: address(token),
                fee: fee,
                recipient: address(this),
                amountIn: amountIn,
                amountOutMinimum: minOut,
                sqrtPriceLimitX96: 0
            })
        );
        usdc.forceApprove(address(swapRouter), 0);
    }

    /// @inheritdoc GuardedBuybackBurner
    function _swapFeeE18() internal view override returns (uint256) {
        return uint256(IUniswapV3Pool(pool).fee()) * FEE_TO_E18;
    }

    /// @inheritdoc GuardedBuybackBurner
    /// @dev Marginal TOKEN-per-USDC spot (1e18 fixed point) from `slot0`. The
    ///      pool prices `token1/token0` in raw base units via `sqrtPriceX96`; we
    ///      resolve the USDC/TOKEN ordering, square the sqrt-price into a Q96
    ///      ratio (`mulDiv` avoids the 2^320 overflow of `sqrtP * sqrtP`), then
    ///      renormalize USDC's `usdcTo18` decimal gap so the result is TOKEN(1e18)
    ///      per USDC(1e18) — matching the units the inherited `_twapFloor` expects.
    ///      Reverts `PoolStateInvalid` if the pool is not a USDC/TOKEN pair or its
    ///      price is zero (the TWAP floor relies on this revert, never zero).
    // Only `sqrtPriceX96` is consumed; slither attributes the tuple-destructure
    // unused-return to the enclosing function, so the waiver sits here.
    // slither-disable-next-line unused-return
    function _spotPrice() internal view override returns (uint256) {
        (uint160 sqrtP,,,,,,) = IUniswapV3Pool(pool).slot0();
        if (sqrtP == 0) revert PoolStateInvalid();
        address t0 = IUniswapV3Pool(pool).token0();
        address t1 = IUniswapV3Pool(pool).token1();

        // priceX96 = (sqrtP^2) / 2^96 = (token1_raw per token0_raw) * 2^96.
        uint256 priceX96 = Math.mulDiv(uint256(sqrtP), uint256(sqrtP), Q96);

        if (t0 == address(usdc) && t1 == address(token)) {
            // price_raw = TOKEN_raw per USDC_raw = priceX96 / 2^96. TOKEN is 18
            // dec (TOKEN_raw == TOKEN_18); USDC_18 = USDC_raw * usdcTo18, so
            // spot = price_raw * 1e18 / usdcTo18 = priceX96 * 1e18 / (2^96 * usdcTo18).
            uint256 spot = Math.mulDiv(priceX96, WAD, Q96 * usdcTo18);
            // Guard the fail-closed invariant symmetrically with the inverted
            // branch: a spot that floor-divides to 0 (an implausibly cheap TOKEN)
            // would collapse the TWAP floor to 0 (fail-open) — revert instead.
            if (spot == 0) revert PoolStateInvalid();
            return spot;
        }
        if (t0 == address(token) && t1 == address(usdc)) {
            // price_raw = USDC_raw per TOKEN_raw = priceX96 / 2^96. Invert and
            // renormalize: spot = 1e18 * 2^96 / (priceX96 * usdcTo18).
            uint256 denom = priceX96 * usdcTo18;
            if (denom == 0) revert PoolStateInvalid();
            return Math.mulDiv(WAD, Q96, denom);
        }
        revert PoolStateInvalid();
    }

    /// @inheritdoc GuardedBuybackBurner
    /// @dev In-pool USDC depth proxied by the pool's USDC token balance — the V3
    ///      pool custodies its reserves directly, so `balanceOf(pool)` is the
    ///      per-epoch cap denominator (analogous to the Vault live balance the
    ///      Balancer burner reads). Like that read it is an instantaneous balance
    ///      (flash-loan-shrinkable — a throughput-griefing, not fund-loss, risk;
    ///      see `_accruePerEpochCap`).
    function _usdcDepthRaw() internal view override returns (uint256) {
        return usdc.balanceOf(pool);
    }

    // -----------------------------------------------------------------
    // Governance setters (GOVERNANCE_ROLE, via the 48h timelock)
    // -----------------------------------------------------------------

    function setSwapRouter(address newRouter) external onlyRole(GOVERNANCE_ROLE) {
        if (newRouter == address(0)) revert ZeroAddress();
        address old = address(swapRouter);
        swapRouter = IUniswapV3SwapRouter(newRouter);
        emit SwapRouterUpdated(old, newRouter);
    }

    /// @notice Wire/rotate the Uniswap V3 pool. The new pool must be a USDC/TOKEN
    ///         pair with a non-zero price, else this reverts `PoolStateInvalid`
    ///         (via `_spotPrice`) instead of silently accepting a mis-wired or
    ///         substituted pool. `newPool == address(0)` stays the documented
    ///         "not wired" state (validation skipped); prefer `pause()` to disable
    ///         without unwiring.
    // slither-disable-next-line missing-zero-check
    function setPool(address newPool) external onlyRole(GOVERNANCE_ROLE) {
        address old = pool;
        pool = newPool;
        emit PoolUpdated(old, newPool);
        // Fail-fast on a mis-wired pool: a non-zero pool must read as a
        // USDC/TOKEN pair with a live price (`_spotPrice` reverts otherwise).
        if (newPool != address(0)) _spotPrice();
        // A pool rotation changes the spot/depth source: reset the inherited
        // TWAP accumulator and per-epoch cap so they re-derive against the new
        // pool (fail-closed `TwapNotReady` + fresh depth snapshot) rather than
        // carrying stale state from the old pool.
        if (newPool != old) _resetGuardState();
    }
}
