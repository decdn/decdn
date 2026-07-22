// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IPublisherRegistryStanding
/// @notice Minimal `PublisherRegistry` read surface required by `ContentBlacklist`
///         for the ADR 031 Publisher standing check at appeal filing: confirm the
///         filer owns the namespace it declares. The hash→namespace association is
///         off-chain (ADR 002 § Hash-to-namespace association), so standing proves
///         only namespace control, not that the disputed hash falls under it. Kept
///         separate from `IPublisherRegistryOwnership` (the `OriginAssignment`
///         consumer view) per the codebase's per-consumer interface convention.
interface IPublisherRegistryStanding {
    function ownerOf(uint256 namespaceId) external view returns (address);
}
