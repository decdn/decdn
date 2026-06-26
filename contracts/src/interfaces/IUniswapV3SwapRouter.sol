// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IUniswapV3SwapRouter
/// @notice Minimal interface for the Uniswap V3 `SwapRouter02` `exactInputSingle`
///         entry point (the deadline-less revision; `SwapRouter02` dropped the
///         per-call `deadline` field its predecessor carried). The router pulls
///         `tokenIn` from `msg.sender` via a standard ERC20 allowance, so the
///         caller approves the router directly (no Permit2 leg required for this
///         path).
interface IUniswapV3SwapRouter {
    /// @param tokenIn The token being swapped in (USDC here).
    /// @param tokenOut The token being received (TOKEN here).
    /// @param fee The pool fee tier identifying the {tokenIn, tokenOut} pool.
    /// @param recipient Who receives `tokenOut`.
    /// @param amountIn Exact input amount.
    /// @param amountOutMinimum Slippage floor; the swap reverts below it.
    /// @param sqrtPriceLimitX96 Price-limit guard; `0` disables it.
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24 fee;
        address recipient;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }

    function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);
}
