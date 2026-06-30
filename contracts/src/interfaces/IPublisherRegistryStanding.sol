// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IPublisherRegistryStanding
/// @notice Minimal `PublisherRegistry` read surface required by `ContentBlacklist`
///         for the ADR 031 Publisher standing check at appeal filing: confirm the
///         filer owns the declared namespace AND that the namespace claimed the
///         disputed hash. Kept separate from `IPublisherRegistryOwnership` (which
///         `OriginAssignment` depends on and needs only `ownerOf`).
interface IPublisherRegistryStanding {
    function ownerOf(uint256 namespaceId) external view returns (address);
    function hasClaimed(uint256 namespaceId, bytes32 blake3Hash) external view returns (bool);
}
