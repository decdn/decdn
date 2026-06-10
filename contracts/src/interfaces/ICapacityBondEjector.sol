// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ICapacityBondEjector
/// @notice Symmetric eject / un-eject surface that `ContentBlacklist` calls
///         into to eject an operator after a final operator-blacklist entry,
///         and to release that ejection when governance lifts the blacklist
///         (ADR 011 § Content Takedown + ADR 031). Declared as a standalone
///         interface so the consumer can type its `capacityBond` immutable
///         narrowly and so slither's `missing-inheritance` detector can verify
///         that `CapacityBond` provides the surface it implements.
interface ICapacityBondEjector {
    function ejectNode(address operator) external;

    function unEjectNode(address operator) external;
}
