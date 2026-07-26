// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ICapacityBondSlasher
/// @notice Consumer-side view of the `CapacityBond` surface `SlashJudge` needs
///         (ADR 014 § Integration with Existing Contracts): the `SLASH_ROLE`
///         slash entrypoint, the NodeId-binding lookup used to confirm the
///         challenged address is a registered operator, and the unbonding-period
///         read for the paired `MAX_EVIDENCE_AGE_US < unbondingPeriod` invariant.
///         Focused — mirroring `ICapacityBondEpoch` / `ICapacityBondEjector`.
interface ICapacityBondSlasher {
    /// @notice Slash `operator`, escrowing the slashed TOKEN under `slashId` and
    ///         recording `challenger` for the 50% finality leg (escrow-on-slash,
    ///         ADR 026 / ADR 028). Returns the minted `slashId` and the combined
    ///         slashed amount. Caller must hold `SLASH_ROLE`.
    function slash(address operator, address challenger, uint8 offenseType)
        external
        returns (uint256 slashId, uint256 slashAmount);

    /// @notice The operator's bound NodeId and active flag. `nodeId == bytes32(0)`
    ///         means the address never registered (the binding survives
    ///         deregistration, so a deregistered-but-still-bonded offender stays
    ///         slashable — `active` is not relied on for the registration gate).
    function nodeIdOf(address operator) external view returns (bytes32 nodeId, bool active);

    /// @notice Unbonding window in seconds. Read by `setMaxEvidenceAge` and the
    ///         constructor to enforce `maxEvidenceAgeUs < unbondingPeriod * 1e6`.
    function unbondingPeriod() external view returns (uint256);
}
