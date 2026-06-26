// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IUniswapV3Pool
/// @notice Minimal read surface of a Uniswap V3 pool used by
///         `BuybackBurnerUniswapV3`: the current `slot0` marginal price, the
///         token ordering, and the fee tier. The marginal spot from
///         `slot0.sqrtPriceX96` feeds the inherited TWAP accumulator; `token0`/
///         `token1` resolve the USDC/TOKEN leg ordering; `fee` is the tier passed
///         to the router and converted to the swap-fee fraction for the floor.
interface IUniswapV3Pool {
    /// @return sqrtPriceX96 The current price as a Q64.96 sqrt of `token1/token0`
    ///         (raw base units). Remaining fields (tick, oracle bookkeeping, lock)
    ///         are unused here.
    function slot0()
        external
        view
        returns (
            uint160 sqrtPriceX96,
            int24 tick,
            uint16 observationIndex,
            uint16 observationCardinality,
            uint16 observationCardinalityNext,
            uint8 feeProtocol,
            bool unlocked
        );

    function token0() external view returns (address);

    function token1() external view returns (address);

    /// @return The pool fee in hundredths of a bip (1e-6), e.g. `3000` = 0.30%.
    function fee() external view returns (uint24);
}
