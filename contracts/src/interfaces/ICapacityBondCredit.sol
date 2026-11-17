// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ICapacityBondCredit
/// @notice The `CapacityBond` reads `FeeRouter.deposit` needs to credit a
///         sweep (ADR 016 § Contract: FeeRouter, ADR 036 § Capacity clamp).
interface ICapacityBondCredit {
    /// @notice True when `operator` holds a registered, non-ejected node.
    ///         Gates the rebate and the weight credit.
    function isActive(address operator) external view returns (bool);

    /// @notice The operator's current declared capacity in Mbps. Bounds the
    ///         weight credit per epoch.
    function declaredMbps(address operator) external view returns (uint256);

    /// @notice The bond's epoch length. `FeeRouter` asserts it equals its own.
    function epochLength() external view returns (uint64);
}
