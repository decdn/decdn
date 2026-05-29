// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ISlashAppeal
/// @notice Slash-appeal state machine surface (ADR 028). An operator who was
///         slashed may file an appeal within `CapacityBond`'s filing window;
///         the emergency multisig fast-tracks or rejects it, and the Governor
///         grants (operator vindicated) or upholds (slash stands) it. All
///         escrow movement is delegated to `CapacityBond` via
///         `ICapacityBondSlashEscrow`; this contract owns only the dispute
///         logic and the TOKEN appeal bond.
interface ISlashAppeal {
    enum AppealStatus {
        None,
        Open,
        FastTracked,
        Resolved
    }

    function openSlashAppeal(uint256 slashId, bytes32 evidenceBundleHash) external;
    function fastTrackAppeal(uint256 slashId) external;
    function rejectAppeal(uint256 slashId) external;
    function grantAppeal(uint256 slashId) external;
    function upholdAppeal(uint256 slashId) external;
    function cleanupExpiredAppeal(uint256 slashId) external;
}
