// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { SafeERC20 } from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

/// @notice Escrow lifecycle status of a slash record (ADR 028 escrow-on-slash).
///         File-scoped so both `CapacityBond` and `SlashEscrowLib` share the
///         type without a contract↔library import cycle.
enum SlashStatus {
    None,
    Escrowed,
    AppealOpen,
    Upheld,
    Reversed
}

/// @notice On-chain slash event record. The slashed TOKEN is held in escrow by
///         `CapacityBond` (`escrowedTotal`) until the slash resolves.
///         `SlashAppeal.openSlashAppeal` reads this record to validate appeals
///         against a specific slash without trusting the appellant's `operator`
///         parameter (ADR 028 — closes the unverified-operator hole).
struct SlashRecord {
    address operator; // slot 0: 20 bytes
    uint64 slashedAt; // slot 0: +8 = 28 bytes
    SlashStatus status; // slot 0: +1 = 29 bytes
    address challenger; // slot 1: 20 bytes — paid the 50% leg at finality
    uint64 appealWindowClose; // slot 1: +8 = 28 bytes
    uint256 slashAmount; // slot 2: escrowed TOKEN amount (bond + credit)
    uint256 creditPortion; // slot 3: the Genesis-credit share of slashAmount
}

/// @title SlashEscrowLib
/// @notice Escrow-finality state machine for `CapacityBond` slashes (ADR 028
///         escrow-on-slash, ADR 036 § Slashing zero-out): the unappealed
///         distribution, the appeal-open transition, and the upheld / granted
///         settle paths, plus the multi-slash watermark recompute.
/// @dev    Extracted from `CapacityBond` to keep that contract under the
///         EIP-170 runtime-size ceiling (issue #770 — the capacity-bond curve
///         enforcement consumed the remaining headroom). The entrypoints are
///         `public`, so they compile into this library's own deployed bytecode
///         and are reached from `CapacityBond` via a linked DELEGATECALL —
///         executing in `CapacityBond`'s storage context, so the `storage`
///         mapping arguments resolve to `CapacityBond`'s slots and the emitted
///         events carry `CapacityBond`'s address. `CapacityBond` retains the
///         thin external entrypoints (role gating, reentrancy guard, pause) and
///         applies the returned `escrowedTotal` / Genesis-credit effects, so
///         the value-typed `escrowedTotal` state variable (not passable by
///         storage reference) stays owned by the contract.
library SlashEscrowLib {
    using SafeERC20 for IERC20;

    /// @dev Mirrors `CapacityBond` (ADR 026 § Slashing and burn — 50%
    ///      challenger / 50% burn at finality) and the canonical epoch length.
    uint256 internal constant CHALLENGER_BPS = 5000;
    uint256 internal constant BPS_DENOMINATOR = 10_000;
    uint64 internal constant EPOCH_LENGTH = 7 days;

    event SlashRecorded(uint256 indexed slashId, address indexed operator, uint64 slashedAt, uint256 slashAmount);
    event SlashEscrowed(uint256 indexed slashId, address indexed operator, uint256 amount, uint64 appealWindowClose);
    event SlashAppealOpened(uint256 indexed slashId, address indexed operator);
    event SlashUpheld(uint256 indexed slashId, bool viaAppeal, uint256 challengerShare, uint256 burnShare);
    event SlashReversed(uint256 indexed slashId, address indexed operator, uint256 refund);
    event SlashedAtEpochStamped(address indexed operator, uint64 epochStampPlusOne);

    error UnknownSlash(uint256 slashId);
    error SlashNotEscrowed(uint256 slashId);
    error SlashAppealNotOpen(uint256 slashId);
    error FilingWindowStillOpen(uint256 closeAt);
    error FilingWindowClosed(uint256 closeAt);

    /// @notice Persist a new slash record (status `Escrowed`) and append it to
    ///         the operator's slash list. The caller (`CapacityBond.slash`)
    ///         allocates `slashId` from its `slashCounter` and books
    ///         `totalSlashAmount` into the value-typed `escrowedTotal`; this
    ///         function owns only the mapping/array writes and the events. The
    ///         record lets `SlashAppeal.openSlashAppeal` validate appeals
    ///         without trusting the appellant's `operator` claim (I2 fix), and
    ///         pins the challenger + filing-window deadline for finality.
    function mint(
        mapping(uint256 => SlashRecord) storage records,
        mapping(address => uint256[]) storage operatorSlashIds,
        uint256 slashId,
        address operator,
        address challenger,
        uint256 totalSlashAmount,
        uint256 creditPortion,
        uint64 appealFilingWindow
    ) public {
        uint64 nowTs = uint64(block.timestamp);
        uint64 windowClose = nowTs + appealFilingWindow;
        records[slashId] = SlashRecord({
            operator: operator,
            slashedAt: nowTs,
            status: SlashStatus.Escrowed,
            challenger: challenger,
            appealWindowClose: windowClose,
            slashAmount: totalSlashAmount,
            creditPortion: creditPortion
        });
        operatorSlashIds[operator].push(slashId);
        emit SlashRecorded(slashId, operator, nowTs, totalSlashAmount);
        emit SlashEscrowed(slashId, operator, totalSlashAmount, windowClose);
    }

    /// @notice Distribute a slash whose filing window lapsed with no appeal:
    ///         50% to the recorded challenger, 50% burned. Reverts unless the
    ///         record is `Escrowed` and the (pause-extended) window has closed.
    /// @return amount The escrowed TOKEN distributed — caller subtracts it from
    ///                `escrowedTotal`.
    function finalizeUnappealed(
        mapping(uint256 => SlashRecord) storage records,
        ERC20Burnable token,
        uint256 slashId,
        uint256 slashCounter,
        uint64 pausedTotal
    ) public returns (uint256 amount) {
        if (slashId >= slashCounter) revert UnknownSlash(slashId);
        SlashRecord storage r = records[slashId];
        if (r.status != SlashStatus.Escrowed) revert SlashNotEscrowed(slashId);
        // Filing window extended by the cumulative paused duration so a pause
        // never silently consumes the operator's window (ADR 028 §5).
        uint64 closeAt = r.appealWindowClose + pausedTotal;
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp <= closeAt) revert FilingWindowStillOpen(closeAt);
        return _distribute(r, token, slashId, false);
    }

    /// @notice Flip an `Escrowed` record to `AppealOpen` within the filing
    ///         window (ADR 028 §5). The single on-chain enforcer of the filing
    ///         deadline; once open, `finalizeUnappealed` can no longer race it.
    function markAppealOpen(
        mapping(uint256 => SlashRecord) storage records,
        uint256 slashId,
        uint256 slashCounter,
        uint64 pausedTotal
    ) public {
        if (slashId >= slashCounter) revert UnknownSlash(slashId);
        SlashRecord storage r = records[slashId];
        if (r.status != SlashStatus.Escrowed) revert SlashNotEscrowed(slashId);
        uint64 closeAt = r.appealWindowClose + pausedTotal;
        // forge-lint: disable-next-line(block-timestamp)
        if (block.timestamp > closeAt) revert FilingWindowClosed(closeAt);
        r.status = SlashStatus.AppealOpen;
        emit SlashAppealOpened(slashId, r.operator);
    }

    /// @notice Settle an upheld appeal — distribute the escrow exactly as the
    ///         unappealed path does. Requires an `AppealOpen` record.
    /// @return amount The escrowed TOKEN distributed — caller subtracts it from
    ///                `escrowedTotal`.
    function settleUpheld(
        mapping(uint256 => SlashRecord) storage records,
        ERC20Burnable token,
        uint256 slashId,
        uint256 slashCounter
    ) public returns (uint256 amount) {
        if (slashId >= slashCounter) revert UnknownSlash(slashId);
        SlashRecord storage r = records[slashId];
        if (r.status != SlashStatus.AppealOpen) revert SlashAppealNotOpen(slashId);
        return _distribute(r, token, slashId, true);
    }

    /// @notice Settle a granted appeal: mark the record `Reversed`, re-derive
    ///         the operator's `slashedAtEpoch` watermark over the remaining
    ///         still-standing slashes (ADR 036 § Slashing zero-out — multi-
    ///         slash), and return the amounts for the caller to refund. The
    ///         Genesis-credit restore and the liquid-bond transfer stay in
    ///         `CapacityBond` (they touch `_pendingCredit` / `token`).
    /// @return operator      The operator whose slash was reversed.
    /// @return refund        Total escrowed amount to release (caller subtracts
    ///                       from `escrowedTotal`).
    /// @return creditPortion The Genesis-credit share of `refund` (caller
    ///                       restores it to the vesting position; the remainder
    ///                       is refunded as liquid TOKEN).
    function settleGranted(
        mapping(uint256 => SlashRecord) storage records,
        mapping(address => uint256[]) storage operatorSlashIds,
        mapping(address => uint64) storage slashedAtEpoch,
        uint256 slashId,
        uint256 slashCounter
    ) public returns (address operator, uint256 refund, uint256 creditPortion) {
        if (slashId >= slashCounter) revert UnknownSlash(slashId);
        SlashRecord storage r = records[slashId];
        if (r.status != SlashStatus.AppealOpen) revert SlashAppealNotOpen(slashId);
        refund = r.slashAmount;
        creditPortion = r.creditPortion;
        operator = r.operator;
        r.status = SlashStatus.Reversed;
        _recomputeSlashedAtEpoch(operatorSlashIds, records, slashedAtEpoch, operator);
        emit SlashReversed(slashId, operator, refund);
    }

    /// @dev Distribute an upheld slash's escrow: 50% challenger / 50% burn.
    function _distribute(SlashRecord storage r, ERC20Burnable token, uint256 slashId, bool viaAppeal)
        private
        returns (uint256 amount)
    {
        amount = r.slashAmount;
        address challenger = r.challenger;
        r.status = SlashStatus.Upheld;

        uint256 challengerShare = (amount * CHALLENGER_BPS) / BPS_DENOMINATOR;
        uint256 burnShare = amount - challengerShare;
        if (challengerShare != 0) IERC20(address(token)).safeTransfer(challenger, challengerShare);
        if (burnShare != 0) token.burn(burnShare);
        emit SlashUpheld(slashId, viaAppeal, challengerShare, burnShare);
    }

    /// @dev Re-derive `slashedAtEpoch[operator]` (ADR 036 § Slashing zero-out —
    ///      multi-slash) as the `actualEpoch + 1` of the operator's most-recent
    ///      still-standing (non-`Reversed`) slash, or 0 when none remain. The
    ///      reversed record's status is already `Reversed`, so it is excluded
    ///      from the scan. `_operatorSlashIds` is appended in `slash()` order,
    ///      non-decreasing in epoch, so the last non-`Reversed` entry is the
    ///      max — scan from the tail and stop at the first hit.
    function _recomputeSlashedAtEpoch(
        mapping(address => uint256[]) storage operatorSlashIds,
        mapping(uint256 => SlashRecord) storage records,
        mapping(address => uint64) storage slashedAtEpoch,
        address operator
    ) private {
        uint256[] storage ids = operatorSlashIds[operator];
        uint64 newStamp = 0; // +1-encoded; 0 = no standing slash remains
        for (uint256 i = ids.length; i > 0;) {
            unchecked {
                --i;
            }
            SlashRecord storage rec = records[ids[i]];
            if (rec.status != SlashStatus.Reversed) {
                newStamp = uint64(rec.slashedAt / EPOCH_LENGTH) + 1;
                break;
            }
        }
        slashedAtEpoch[operator] = newStamp;
        emit SlashedAtEpochStamped(operator, newStamp);
    }
}
