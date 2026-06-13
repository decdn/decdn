// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IBalancerV3WeightedPool
/// @notice Minimal Balancer V3 weighted-pool surface. Normalized weights live
///         on the pool contract (not the Vault) and are needed to convert the
///         Vault's live balances into a spot price for the 80/20 TOKEN/USDC
///         pool (TOKEN per USDC): `spot = (balTOKEN18 / wTOKEN) / (balUSDC18 / wUSDC)`.
/// @dev    Weights are 1e18-scaled and sum to 1e18, ordered to match
///         `IBalancerV3Vault.getPoolTokens`.
interface IBalancerV3WeightedPool {
    function getNormalizedWeights() external view returns (uint256[] memory);
}
