// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IPublisherRegistryOwnership
/// @notice Consumer-side view of `PublisherRegistry` used by `OriginAssignment`
///         (ADR 011 § Cross-contract integration):
///         - `ownerOf` validates that the caller owns the namespace it seats an
///           origin for (`addOrigin` / `removeOrigin`). It is the
///           `PublisherRegistry` public mapping getter; `address(0)` denotes an
///           unassigned id (including the reserved no-namespace id `0`).
///         - `namespaceCount` proves the caller is a publisher at all, which is
///           the entry condition for `requestVetting`. It is likewise a public
///           mapping getter. It counts namespaces the address owns **now**, not
///           namespaces it ever created: `PublisherRegistry` decrements it when
///           a transfer finalizes, so it reads `0` both for an address that
///           never created one and for an address that transferred them all
///           away.
interface IPublisherRegistryOwnership {
    function ownerOf(uint256 namespaceId) external view returns (address);

    function namespaceCount(address publisher) external view returns (uint256);
}
