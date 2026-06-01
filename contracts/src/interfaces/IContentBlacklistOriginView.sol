// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IContentBlacklistOriginView
/// @notice Consumer-side view of `ContentBlacklist`'s two operator-blacklist
///         mappings used by `OriginAssignment.activateAssignment` (operator
///         re-validation) and `pruneBlacklistedAssignment` (permissionless
///         cleanup) per ADR 011 § Interaction with ContentBlacklist. Both are
///         `ContentBlacklist` public mapping getters. Consulting BOTH closes the
///         stale-authorization gap: an operator blacklisted via either the
///         origin or the operator mapping must be rejected / prunable (M-2).
interface IContentBlacklistOriginView {
    function isOriginBlacklisted(address operator) external view returns (bool);
    function isOperatorBlacklisted(address operator) external view returns (bool);
}
