// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ICapacityBondEpoch
/// @notice Deploy-time epoch-length view `FeeRouter`'s constructor asserts
///         against, so the deployer-passed `epochLength_` is proven to match
///         `CapacityBond.EPOCH_LENGTH`. Without this check, a mismatched
///         deployment silently mis-anchors `DecdnGovernor` epoch arithmetic,
///         which mixes epoch indices from both contracts.
interface ICapacityBondEpoch {
    /// @dev Declared `view` (not `pure`) so test mocks can return a
    ///      constructor-stored value per instance. The production implementor
    ///      `CapacityBond.epochLength()` narrows to `pure` since `EPOCH_LENGTH`
    ///      is a `constant`; the FeeRouter constructor's `bondEpoch ==
    ///      epochLength_` assertion catches deploy-time mismatches regardless.
    function epochLength() external view returns (uint64);
}
