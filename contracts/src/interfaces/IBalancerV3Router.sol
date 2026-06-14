// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

/// @title IBalancerV3Router
/// @notice Minimal Balancer V3 Router surface used by `BuybackBurnerBalancerV3`
///         to execute the single-token exact-in USDC->TOKEN swap (ADR 018
///         § Buyback execution via Balancer V3). Only `swapSingleTokenExactIn`
///         is vendored — the buyback path never batches or queries on-chain.
/// @dev    The Router is the call target, but token approvals go to the
///         **Vault** (the Vault pulls input tokens from `msg.sender`); see the
///         approvals footgun in ADR 018 and the scoped-approval invariant on
///         `BuybackBurner._performSwap`.
interface IBalancerV3Router {
    function swapSingleTokenExactIn(
        address pool,
        IERC20 tokenIn,
        IERC20 tokenOut,
        uint256 exactAmountIn,
        uint256 minAmountOut,
        uint256 deadline,
        bool wethIsEth,
        bytes calldata userData
    ) external payable returns (uint256 amountOut);
}
