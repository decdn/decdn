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
    /// @dev Declared `view` (not `pure`) so test mocks can return a
    ///      constructor-stored value per instance. The production implementor
    ///      `CapacityBond.epochLength()` narrows to `pure` since `EPOCH_LENGTH`
    ///      is a `constant`; the FeeRouter constructor's `bondEpoch ==
    ///      epochLength_` assertion catches deploy-time mismatches regardless.
    function epochLength() external view returns (uint64);
}
