// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title Roles
/// @notice Centralized role identifiers for deCDN AccessControl.
/// @dev Keep these in sync with ADR 016 §5 (Access Control Matrix). Role IDs are
///      keccak256 hashes of their ASCII names so that grants across contracts
///      reference the same bytes32 value.
library Roles {
    bytes32 internal constant BLACKLIST_ROLE = keccak256("BLACKLIST_ROLE");
    bytes32 internal constant SLASH_ROLE = keccak256("SLASH_ROLE");
    bytes32 internal constant KEEPER_ROLE = keccak256("KEEPER_ROLE");
    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 internal constant EMERGENCY_ROLE = keccak256("EMERGENCY_ROLE");
    /// @dev Granted to PaymentChannel contracts so they can stamp
    ///      `lastSettlementAt` on the registry — used by clients to bias
    ///      cold-start bootstrap toward proven deliverers (ADR 016 §3
    ///      Off-Chain Read API).
    bytes32 internal constant SETTLEMENT_REPORTER_ROLE = keccak256("SETTLEMENT_REPORTER_ROLE");
}
