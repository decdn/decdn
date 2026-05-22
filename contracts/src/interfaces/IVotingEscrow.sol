// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title IVotingEscrow
/// @notice Minimal vote-source surface of `VotingEscrow` consumed by
///         `DecdnGovernor`. The Governor reads ve-balance (not raw TOKEN) as
///         voting weight per ADR 026 § Voting weight = ve-balance, and calibrates
///         quorum / proposal threshold against total ve-supply per ADR 009.
/// @dev    `VotingEscrow` (contracts/src/VotingEscrow.sol) already conforms to
///         this surface. `balanceOfAt` / `totalSupplyAt` are timestamp-keyed
///         (the Governor runs on a timestamp clock) and revert on future
///         lookups; the Governor only ever queries them at past snapshots.
interface IVotingEscrow {
    /// @notice ve-balance of `account` at `timestamp` (linear-decayed).
    function balanceOfAt(address account, uint256 timestamp) external view returns (uint256);

    /// @notice Total ve-supply at `timestamp`.
    function totalSupplyAt(uint256 timestamp) external view returns (uint256);

    /// @notice Current total ve-supply.
    function totalSupply() external view returns (uint256);
}
