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
    /// @param  offenseType ADR 014 offense taxonomy index, persisted on the slash
    ///         record so a consumer can attribute an offense to a `slashId`
    ///         without joining two events by transaction ordering.
    /// @param  evidenceHash The `SlashJudge` evidence digest this slash resolved,
    ///         persisted on the record for the same reason.
    function slash(address operator, address challenger, uint8 offenseType, bytes32 evidenceHash)
        external
        returns (uint256 slashId, uint256 slashAmount);

    /// @notice The operator's bound NodeId and active flag. `nodeId == bytes32(0)`
    ///         means the address holds no binding: it never registered, or
    ///         `reclaimNodeId` cleared the binding. The binding survives
    ///         deregistration; `active` is not relied on for the registration gate.
    function nodeIdOf(address operator) external view returns (bytes32 nodeId, bool active);

    /// @notice The operator's active bond, the first slashable leg.
    function activeBond(address operator) external view returns (uint256);

    /// @notice The operator's queued unbonding request, the second slashable leg.
    function unbondingOf(address operator) external view returns (uint256 amount, uint256 unlockAt);

    /// @notice Unbonding window in seconds. Read by `setMaxEvidenceAge` and the
    ///         constructor to enforce `maxEvidenceAgeUs < unbondingPeriod * 1e6`.
    function unbondingPeriod() external view returns (uint256);
}
