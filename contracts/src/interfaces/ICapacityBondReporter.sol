// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ICapacityBondReporter
/// @notice Settlement-reporter surface that `FeeRouter` calls into on every
///         `routeSettlement`. Includes `epochLength()` so the router's
///         constructor can assert that the deployer-passed `epochLength_`
///         matches `CapacityBond.EPOCH_LENGTH` — without this check, a
///         mismatched deployment silently mis-anchors `DecdnGovernor` epoch
///         arithmetic, which mixes epoch indices from both contracts.
interface ICapacityBondReporter {
    function recordSettlement(address operator) external;
    function epochLength() external view returns (uint64);
}
