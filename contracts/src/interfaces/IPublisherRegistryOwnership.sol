// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IPublisherRegistryOwnership
/// @notice Consumer-side view of `PublisherRegistry.ownerOf` used by
///         `OriginAssignment.proposeAssignment` / `revokeAssignment` to validate
///         that the caller owns the namespace (ADR 011 § Cross-contract
///         integration). `ownerOf` is the `PublisherRegistry` public mapping
///         getter; `address(0)` denotes an unassigned id (including the reserved
///         default-open id `0`).
interface IPublisherRegistryOwnership {
    function ownerOf(uint256 namespaceId) external view returns (address);
}
