// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

/// @title ICapacityBondSlashEscrow
/// @notice Escrow-mutating appeal hooks on `CapacityBond`, consumed by the
///         `SlashAppeal` state machine (ADR 028 escrow-on-slash). All three
///         are gated by `CapacityBond.SLASH_APPEAL_ROLE`, granted to the
///         `SlashAppeal` contract post-deploy (ADR 016 § Post-Deployment
///         Initialization).
/// @dev    Read access to the underlying slash record (`slashRecords`) lives
///         on `ICapacityBond`. The slashed TOKEN is held in `CapacityBond`
///         escrow until one of these hooks (or the permissionless
///         `finalizeUnappealedSlash`) resolves it.
interface ICapacityBondSlashEscrow {
    /// @notice Lock a slash's escrow while an appeal is heard. Flips the
    ///         record from `Escrowed` to `AppealOpen` so the permissionless
    ///         `finalizeUnappealedSlash` path cannot distribute it. Reverts if
    ///         the slash is not `Escrowed` or the filing window has closed —
    ///         this is the single on-chain enforcer of the appeal filing
    ///         deadline (ADR 028 § Hard caps and frequency limits).
    function markAppealOpen(uint256 slashId) external;

    /// @notice Resolve an open appeal as UPHELD (the slash stands): distribute
    ///         the escrow 50% to the recorded challenger / 50% burned. Reverts
    ///         if the slash is not `AppealOpen`.
    function settleAppealUpheld(uint256 slashId) external;

    /// @notice Resolve an open appeal as GRANTED (the operator was wrongly
    ///         slashed): refund the full escrowed amount to the operator and
    ///         clear their `slashedAtEpoch` zero-out (ADR 036). Reverts if the
    ///         slash is not `AppealOpen`.
    function settleAppealGranted(uint256 slashId) external;

    /// @notice Combined slash-path paused-seconds accumulator. `CapacityBond`
    ///         is the single source of truth: both its own pauses and (via
    ///         `creditPauseTime`) `SlashAppeal`'s pauses fold in here, so every
    ///         window (filing, review, ratification) extends by the same total
    ///         and a pause on either contract never silently consumes a window
    ///         (ADR 028 §5).
    function pausedTotal() external view returns (uint64);

    /// @notice Add `SlashAppeal`'s just-ended pause interval to the combined
    ///         `pausedTotal`. Called only by `SlashAppeal._unpause` so the
    ///         filing window (enforced on `CapacityBond`) extends by the time
    ///         `SlashAppeal` was paused — the window during which operators
    ///         could not file. Gated by `SLASH_APPEAL_ROLE`.
    function creditPauseTime(uint64 delta) external;
}
