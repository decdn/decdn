// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { SlashRecord } from "../SlashEscrowLib.sol";

/// @title ICapacityBond
/// @notice Read surface of `CapacityBond` consumed by `DecdnGovernor` (for
///         vote weight per ADR 036) and `SlashAppeal` (to validate appeals
///         against a specific slash record per ADR 028). The escrow-mutating
///         appeal hooks live in `ICapacityBondSlashEscrow`.
/// @dev    The full `CapacityBond` surface (register / unbond / slash /
///         region attestation) lives on the concrete contract. This interface
///         declares only the cross-contract read entrypoints other contracts in
///         the system need to know about.
interface ICapacityBond {
    /// @notice Timestamp at which the operator's `activeBond` first reached
    ///         `minBond` — set by the first `bond()` call that crosses it.
    ///         Never overwritten by subsequent re-bonds. Source of the
    ///         `age_ramp` numerator on `DecdnGovernor` per ADR 036 § Formula.
    ///         Returns 0 if the operator has never reached `minBond`.
    function firstBondedAt(address operator) external view returns (uint64);

    /// @notice `declaredMbps` in effect at the END of `epoch`
    ///         (`(epoch + 1) × EPOCH_LENGTH − 1`). `DecdnGovernor` caps each
    ///         epoch's vote-weight bytes at this capacity per ADR 036 § Formula
    ///         — declared capacity caps delivery-based weight, it never grants
    ///         weight. Returns 0 when the operator has no declaration on or
    ///         before that instant.
    function declaredMbpsAtEpoch(address operator, uint64 epoch) external view returns (uint256);

    /// @notice On-chain slash record consumed by `SlashAppeal.openSlashAppeal`
    ///         to validate appeals against a specific slash without trusting
    ///         the appellant's `operator` parameter (ADR 028 § Contract
    ///         surface — closes the unverified-operator hole).
    /// @return operator The operator whose bond was slashed.
    /// @return slashedAt `block.timestamp` of the slash transaction (cast to
    ///                  `uint64`).
    /// @return slashAmount Active + unbonding bond amount slashed at this
    ///                    `slashId`.
    function slashRecords(uint256 slashId)
        external
        view
        returns (address operator, uint64 slashedAt, uint256 slashAmount);

    /// @notice Number of slashes ever recorded against `operator`.
    function operatorSlashCount(address operator) external view returns (uint256);

    /// @notice The `index`-th slash against `operator`, in slash order.
    function operatorSlashIdAt(address operator, uint256 index) external view returns (uint256);

    /// @notice The full slash record, including its escrow status. Read by
    ///         `DecdnGovernor` to find a standing slash inside a vote window
    ///         (ADR 036 § Slashing zero-out).
    function getSlashRecord(uint256 slashId) external view returns (SlashRecord memory);
}
