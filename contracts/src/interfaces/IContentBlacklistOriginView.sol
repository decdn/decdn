// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IContentBlacklistOriginView
/// @notice Consumer-side view of `ContentBlacklist.isOriginBlacklisted` used by
///         `OriginAssignment.activateAssignment` (operator re-validation) and
///         `pruneBlacklistedAssignment` (permissionless cleanup) per ADR 011
///         § Interaction with ContentBlacklist. `isOriginBlacklisted` is the
///         `ContentBlacklist` public mapping getter.
interface IContentBlacklistOriginView {
    function isOriginBlacklisted(address operator) external view returns (bool);
}
