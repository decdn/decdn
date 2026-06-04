// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ISlashJudgeEvidenceView
/// @notice Minimal read-only view of `SlashJudge` consumed by `CapacityBond`
///         to enforce the paired `maxEvidenceAgeUs < unbondingPeriod * 1e6`
///         invariant on its `setUnbondingPeriod` setter (ADR 014 § Interaction
///         with unbonding period). Mirrors the `ICapacityBondSlasher` narrow-
///         interface convention so `CapacityBond` links no `SlashJudge`
///         bytecode.
interface ISlashJudgeEvidenceView {
    /// @notice Current evidence-age ceiling in microseconds. `CapacityBond`
    ///         reads this to reject an `unbondingPeriod` lowered to/below it.
    function maxEvidenceAgeUs() external view returns (uint256);
}
