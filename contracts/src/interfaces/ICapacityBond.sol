// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ICapacityBond
/// @notice Read + appeal-reversal surface of `CapacityBond` consumed by
///         `DecdnGovernor` (for vote weight per ADR 036) and `SafetyReserve`
///         (for slash-appeal reversal per ADR 028).
/// @dev    The full `CapacityBond` surface (register / unbond / slash /
///         Genesis Bond Credit flow / region attestation) lives on the
///         concrete contract. This interface declares only the cross-contract
///         entrypoints other contracts in the system need to know about.
interface ICapacityBond {
    /// @notice Timestamp at which the operator's `activeStake` first became
    ///         non-zero — set by the first `stake()` call that lifts the
    ///         balance above zero, or by `claimVestedCredit()` if a Genesis
    ///         Bond Credit claim is the first thing to activate the operator.
    ///         Never overwritten by subsequent re-bonds. Source of the
    ///         `age_ramp` numerator on `DecdnGovernor` per ADR 036 § Formula.
    ///         Returns 0 if the operator has never bonded.
    function firstBondedAt(address operator) external view returns (uint64);

    /// @notice Encoded slash-epoch stamp: returns `0` when the operator is
    ///         not currently in the slash-zero-out window; otherwise returns
    ///         `actualEpoch + 1`. The +1 offset exists so a slash in epoch 0
    ///         (the first `EPOCH_LENGTH` after deploy) is not collapsed with
    ///         the unslashed sentinel. Consumers MUST decode (`value - 1`)
    ///         before doing epoch arithmetic. Set by `slash()`; cleared to 0
    ///         by `clearSlashedAtEpoch` (called only via the `reverseAppeal`
    ///         path of `SafetyReserve` per ADR 028 § Contract surface).
    ///         Consumed by `DecdnGovernor._getVotes` per ADR 036 § Slashing
    ///         zero-out.
    function slashedAtEpoch(address operator) external view returns (uint64);

    /// @notice Clear `slashedAtEpoch[operator]` back to 0. Restricted to
    ///         `APPEAL_REVERSAL_ROLE`, granted to `SafetyReserve` post-deploy
    ///         (ADR 016 § Post-Deployment Initialization, step 6).
    function clearSlashedAtEpoch(address operator) external;

    /// @notice On-chain slash record consumed by `SafetyReserve.openSlashAppeal`
    ///         to validate appeals against a specific slash without trusting
    ///         the appellant's `operator` parameter (ADR 028 § Contract
    ///         surface — closes the unverified-operator hole).
    /// @return operator The operator whose stake (and pending credit, if any)
    ///                  was slashed.
    /// @return slashedAt `block.timestamp` of the slash transaction (cast to
    ///                  `uint64`).
    /// @return slashAmount Combined active-stake + unvested-credit amount
    ///                    slashed at this `slashId`.
    function slashRecords(uint256 slashId)
        external
        view
        returns (address operator, uint64 slashedAt, uint256 slashAmount);
}
