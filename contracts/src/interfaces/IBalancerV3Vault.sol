// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

/// @title IBalancerV3Vault
/// @notice Minimal Balancer V3 Vault read surface used by
///         `BuybackBurnerBalancerV3` to (a) snapshot in-pool USDC depth for the
///         per-epoch liquidity cap and (b) derive the instantaneous spot price
///         feeding the TWAP accumulator (ADR 018 § TWAP policy).
/// @dev    `getCurrentLiveBalances` returns balances scaled to 18 decimals
///         (yield-fee / rate / decimal adjusted), ordered to match
///         `getPoolTokens`. V3 exposes no built-in price oracle, so the spot
///         is reconstructed off these reads plus the pool's normalized weights.
interface IBalancerV3Vault {
    /// @notice Whether `pool` is registered with this Vault. Lets
    ///         `BuybackBurnerBalancerV3` fail pool wiring fast (at deploy and at
    ///         governance update) against an unregistered / substituted pool,
    ///         rather than only surfacing the mistake at swap time.
    function isPoolRegistered(address pool) external view returns (bool);

    function getPoolTokens(address pool) external view returns (IERC20[] memory tokens);

    function getCurrentLiveBalances(address pool) external view returns (uint256[] memory balancesLiveScaled18);

    /// @notice Static swap-fee percentage for `pool`, 1e18-scaled (1% = 1e16).
    ///         The `minOut` floor subtracts this from the marginal estimate so
    ///         the floor tracks fee-reduced realized output, not the fee-free
    ///         marginal price.
    function getStaticSwapFeePercentage(address pool) external view returns (uint256);
}
