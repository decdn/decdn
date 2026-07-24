// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { SlashAppeal } from "../src/SlashAppeal.sol";
import { SunsettingPausable } from "../src/SunsettingPausable.sol";
import { CapacityBond } from "../src/CapacityBond.sol";
import { SlashStatus, SlashRecord } from "../src/SlashEscrowLib.sol";
import { Token } from "../src/Token.sol";
import { ICapacityBond } from "../src/interfaces/ICapacityBond.sol";
import { ISlashAppeal } from "../src/interfaces/ISlashAppeal.sol";

import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";

/// @title SlashAppeal flow tests (ADR 028 escrow-on-slash)
/// @notice Drives the full appeal state machine against a real `CapacityBond`,
///         asserting the escrow settle hooks move the slashed TOKEN correctly:
///         grant → operator refunded + zero-out cleared; uphold/reject →
///         escrow distributed 50/50 and the appeal bond burned.
contract SlashAppealTest is Test {
    Token internal token;
    MockEd25519Verifier internal ed25519;
    CapacityBond internal bond;
    SlashAppeal internal appeal;

    address internal admin = address(0xA11CE); // GOVERNANCE_ROLE on both
    address internal operator = address(0xB0B);
    address internal challenger = address(0xC4A11);
    address internal appellant = address(0xA99EA1);
    address internal multisig = address(0xC0DE);

    uint256 internal constant MIN_BOND = 50_000e18;
    uint256 internal constant APPEAL_BOND = 1000e18;
    uint256 internal constant SLASH_AMT = 2500e18; // 5% of MIN_BOND

    function setUp() public {
        token = new Token(admin);
        ed25519 = new MockEd25519Verifier();

        bond = new CapacityBond({
            token_: token,
            ed25519Verifier_: ed25519,
            admin: admin,
            minBond_: MIN_BOND,
            unbondingPeriod_: 7 days,
            multiaddrUpdateCooldown_: 0,
            maxMultiaddrSize_: 1024,
            regionStabilityWindow_: 7 days,
            currentTermsHash_: keccak256("decdn operator terms v1")
        });

        appeal = new SlashAppeal({
            token_: token,
            capacityBond_: ICapacityBond(address(bond)),
            admin: admin,
            emergencyMultisig: multisig,
            appealBond_: APPEAL_BOND
        });

        vm.startPrank(admin);
        bond.grantRole(bond.SLASH_ROLE(), admin);
        bond.grantRole(bond.SLASH_APPEAL_ROLE(), address(appeal));
        token.transfer(operator, 100_000e18);
        token.transfer(appellant, 10_000e18);
        vm.stopPrank();

        vm.startPrank(operator);
        token.approve(address(bond), type(uint256).max);
        // Filing is operator-only (ADR 028 § Contract surface), so the operator posts the bond.
        token.approve(address(appeal), type(uint256).max);
        vm.stopPrank();
        vm.prank(appellant);
        token.approve(address(appeal), type(uint256).max);

        // Warp well past epoch 0 so slash timestamps are non-trivial.
        vm.warp(100 days);
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    function _bondAndSlash() internal returns (uint256 slashId) {
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(admin);
        (slashId,) = bond.slash(operator, challenger, 1);
    }

    /// @dev Filing is operator-only; the operator is the appellant.
    function _open(uint256 slashId) internal {
        vm.prank(operator);
        appeal.openSlashAppeal(slashId, keccak256("evidence"));
    }

    // -----------------------------------------------------------------
    // open
    // -----------------------------------------------------------------

    function test_open_locksEscrowAndPullsBond() public {
        uint256 slashId = _bondAndSlash();
        uint256 bondBefore = token.balanceOf(operator);
        _open(slashId);

        // Bond pulled from the operator into the appeal contract.
        assertEq(bondBefore - token.balanceOf(operator), APPEAL_BOND);
        assertEq(token.balanceOf(address(appeal)), APPEAL_BOND);
        // CapacityBond escrow flipped to AppealOpen.
        assertEq(uint8(bond.getSlashRecord(slashId).status), uint8(SlashStatus.AppealOpen));
        assertEq(uint8(appeal.getAppeal(slashId).status), uint8(ISlashAppeal.AppealStatus.Open));
    }

    function test_open_revertsUnknownSlash() public {
        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.UnknownSlash.selector, uint256(0)));
        appeal.openSlashAppeal(0, keccak256("e"));
    }

    /// ADR 028 § Contract surface — only the slashed operator may file; a third party (here the
    /// challenger, who profits from an upheld slash) cannot burn the slot.
    function test_open_revertsForNonOperator() public {
        uint256 slashId = _bondAndSlash();
        vm.prank(appellant);
        vm.expectRevert(abi.encodeWithSelector(SlashAppeal.CallerNotOperator.selector, operator));
        appeal.openSlashAppeal(slashId, keccak256("e"));
        // Slot is still open for the real operator.
        _open(slashId);
        assertEq(uint8(appeal.getAppeal(slashId).status), uint8(ISlashAppeal.AppealStatus.Open));
    }

    function test_open_revertsAfterFilingWindow() public {
        uint256 slashId = _bondAndSlash();
        uint64 closeTs = bond.getSlashRecord(slashId).appealWindowClose;
        vm.warp(block.timestamp + 30 days + 1);
        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.FilingWindowClosed.selector, closeTs));
        appeal.openSlashAppeal(slashId, keccak256("e"));
    }

    function test_open_revertsOnDoubleAppeal() public {
        uint256 slashId = _bondAndSlash();
        _open(slashId);
        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(SlashAppeal.AppealAlreadyExists.selector, slashId));
        appeal.openSlashAppeal(slashId, keccak256("e2"));
    }

    // -----------------------------------------------------------------
    // grant (operator vindicated)
    // -----------------------------------------------------------------

    function test_grant_refundsOperatorAndBondAndClearsZeroOut() public {
        uint256 slashId = _bondAndSlash();
        assertGt(bond.slashedAtEpoch(operator), 0);
        _open(slashId);

        // Operator is the appellant, so they receive both the escrow refund
        // (bond-only here → full SLASH_AMT liquid) and the appeal-bond refund.
        uint256 opBefore = token.balanceOf(operator);

        vm.prank(multisig);
        appeal.fastTrackAppeal(slashId);
        vm.prank(admin);
        appeal.grantAppeal(slashId);

        assertEq(token.balanceOf(operator) - opBefore, SLASH_AMT + APPEAL_BOND);
        assertEq(bond.slashedAtEpoch(operator), 0);
        assertEq(bond.escrowedTotal(), 0);
        assertEq(bond.getSlashRecord(slashId).status == SlashStatus.Reversed, true);
        // Frequency cap stamped.
        assertEq(appeal.lastAcceptedAppealAt(operator), uint64(block.timestamp));
    }

    function test_grant_revertsIfNotFastTracked() public {
        uint256 slashId = _bondAndSlash();
        _open(slashId);
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(SlashAppeal.AppealNotFastTracked.selector, slashId));
        appeal.grantAppeal(slashId);
    }

    function test_frequencyCap_blocksSecondAppealWithinWindow() public {
        uint256 slashId = _bondAndSlash();
        _open(slashId);
        vm.prank(multisig);
        appeal.fastTrackAppeal(slashId);
        vm.prank(admin);
        appeal.grantAppeal(slashId);

        // Second slash of the same operator, fresh escrow.
        vm.prank(admin);
        (uint256 slashId2,) = bond.slash(operator, challenger, 1);
        vm.prank(operator);
        vm.expectRevert(
            abi.encodeWithSelector(SlashAppeal.FrequencyCapHit.selector, uint64(block.timestamp + 365 days))
        );
        appeal.openSlashAppeal(slashId2, keccak256("e"));
    }

    // -----------------------------------------------------------------
    // uphold / reject (slash stands)
    // -----------------------------------------------------------------

    function test_uphold_distributesEscrowAndBurnsFullBond() public {
        uint256 slashId = _bondAndSlash();
        _open(slashId);
        vm.prank(multisig);
        appeal.fastTrackAppeal(slashId);

        uint256 challengerBefore = token.balanceOf(challenger);
        uint256 supplyBefore = token.totalSupply();

        vm.expectEmit(true, false, false, true, address(appeal));
        emit SlashAppeal.AppealUpheld(slashId, APPEAL_BOND);
        vm.prank(admin);
        appeal.upholdAppeal(slashId);

        // Escrow 50/50: challenger 1250, burn 1250.
        assertEq(token.balanceOf(challenger) - challengerBefore, SLASH_AMT / 2);
        // The separate appeal bond is burned in full — no share is diverted
        // anywhere, so the supply delta accounts for every wei of it.
        assertEq(supplyBefore - token.totalSupply(), SLASH_AMT / 2 + APPEAL_BOND);
        assertEq(bond.escrowedTotal(), 0);
    }

    function test_reject_burnsBondAndDistributesEscrow() public {
        uint256 slashId = _bondAndSlash();
        _open(slashId);

        uint256 challengerBefore = token.balanceOf(challenger);
        uint256 supplyBefore = token.totalSupply();

        vm.prank(multisig);
        appeal.rejectAppeal(slashId);

        assertEq(token.balanceOf(challenger) - challengerBefore, SLASH_AMT / 2);
        // Full appeal bond burned + 50% of escrow burned.
        assertEq(supplyBefore - token.totalSupply(), APPEAL_BOND + SLASH_AMT / 2);
        assertEq(bond.escrowedTotal(), 0);
    }

    // -----------------------------------------------------------------
    // cleanupExpiredAppeal (lapse)
    // -----------------------------------------------------------------

    function test_cleanup_openLapse_upholds() public {
        uint256 slashId = _bondAndSlash();
        _open(slashId);

        vm.warp(block.timestamp + 14 days + 1);
        uint256 supplyBefore = token.totalSupply();
        appeal.cleanupExpiredAppeal(slashId); // permissionless

        // Review-window lapse → upheld: bond burned + escrow 50/50.
        assertEq(supplyBefore - token.totalSupply(), APPEAL_BOND + SLASH_AMT / 2);
        assertEq(bond.escrowedTotal(), 0);
    }

    function test_cleanup_fastTrackedLapse_grantsOperatorFavorable() public {
        uint256 slashId = _bondAndSlash();
        _open(slashId);
        vm.prank(multisig);
        appeal.fastTrackAppeal(slashId);

        uint256 opBefore = token.balanceOf(operator);

        vm.warp(block.timestamp + 14 days + 1);
        appeal.cleanupExpiredAppeal(slashId); // permissionless

        // Ratification-window lapse → operator-favorable grant: escrow refunded
        // to operator + bond refunded to operator (the appellant), zero-out cleared.
        assertEq(token.balanceOf(operator) - opBefore, SLASH_AMT + APPEAL_BOND);
        assertEq(bond.slashedAtEpoch(operator), 0);
    }

    // -----------------------------------------------------------------
    // access control
    // -----------------------------------------------------------------

    function test_fastTrack_requiresMultisig() public {
        uint256 slashId = _bondAndSlash();
        _open(slashId);
        bytes32 role = appeal.EMERGENCY_MULTISIG_ROLE();
        vm.prank(operator);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, operator, role)
        );
        appeal.fastTrackAppeal(slashId);
    }

    function test_grant_requiresGovernance() public {
        uint256 slashId = _bondAndSlash();
        _open(slashId);
        vm.prank(multisig);
        appeal.fastTrackAppeal(slashId);
        bytes32 role = appeal.GOVERNANCE_ROLE();
        vm.prank(operator);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, operator, role)
        );
        appeal.grantAppeal(slashId);
    }

    // -----------------------------------------------------------------
    // ADR 036 § Slashing zero-out — multi-slash watermark recompute
    // (settleAppealGranted re-derives the watermark over standing slashes)
    // -----------------------------------------------------------------

    /// Granting an appeal for an OLDER slash must NOT clear the per-operator
    /// `slashedAtEpoch` stamp that belongs to a NEWER, still-standing slash.
    function test_grant_olderAppeal_preservesNewerSlashZeroOut() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);

        // Slash #1, open + fast-track its appeal.
        vm.prank(admin);
        (uint256 s1,) = bond.slash(operator, challenger, 1);
        uint64 stamp1 = bond.slashedAtEpoch(operator);
        _open(s1);
        vm.prank(multisig);
        appeal.fastTrackAppeal(s1);

        // Slash #2 in a later epoch — overwrites the per-operator stamp.
        vm.warp(block.timestamp + 8 days);
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        uint64 stamp2 = bond.slashedAtEpoch(operator);
        assertTrue(stamp2 != stamp1);

        // Grant the OLDER appeal — the newer slash's stamp must survive.
        vm.prank(admin);
        appeal.grantAppeal(s1);
        assertEq(bond.slashedAtEpoch(operator), stamp2);
    }

    /// Granting an appeal for the NEWER slash must NOT clear the watermark to
    /// zero while an OLDER slash still stands — it must fall back to the older
    /// slash's epoch (issue #709 multi-outstanding-slash recompute).
    function test_grant_newerAppeal_fallsBackToOlderStandingSlash() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);

        // Slash #1 (older) — stays Escrowed/standing for the whole test.
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        uint64 stamp1 = bond.slashedAtEpoch(operator);

        // Slash #2 (newer) in a later epoch — overwrites the per-operator stamp.
        vm.warp(block.timestamp + 8 days);
        vm.prank(admin);
        (uint256 s2,) = bond.slash(operator, challenger, 1);
        assertTrue(bond.slashedAtEpoch(operator) != stamp1);

        // Grant the NEWER appeal — the watermark must fall back to the older
        // still-standing slash's stamp, not clear to zero.
        _open(s2);
        vm.prank(multisig);
        appeal.fastTrackAppeal(s2);
        vm.prank(admin);
        appeal.grantAppeal(s2);
        assertEq(bond.slashedAtEpoch(operator), stamp1);
    }

    // -----------------------------------------------------------------
    // ADR 028 § Hard caps and frequency limits — pause extends the filing window by the paused duration
    // -----------------------------------------------------------------

    // ADR 009 § Emergency Multisig — the protocol-wide pause sunsets hard at
    // each contract's own construction time + 365 days; afterwards `pause()` reverts for everyone.
    function test_pause_revertsAfterSunset() public {
        bytes32 pauserRole = appeal.PAUSER_ROLE();
        vm.prank(admin);
        appeal.grantRole(pauserRole, admin);

        vm.warp(block.timestamp + 366 days);
        vm.prank(admin);
        vm.expectRevert(SunsettingPausable.PauseExpired.selector);
        appeal.pause();
    }

    function test_pause_extendsFilingWindow() public {
        bytes32 pauserRole = bond.PAUSER_ROLE();
        vm.prank(admin);
        bond.grantRole(pauserRole, admin);
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(admin);
        (uint256 slashId,) = bond.slash(operator, challenger, 1);
        uint256 slashTime = block.timestamp;

        // Pause for 10 days inside the filing window.
        vm.prank(admin);
        bond.pause();
        vm.warp(slashTime + 10 days);
        vm.prank(admin);
        bond.unpause();
        assertEq(bond.pausedTotal(), 10 days);

        // Just past the NOMINAL 30-day deadline — still open (extended by 10d).
        vm.warp(slashTime + 30 days + 1);
        uint64 closeAt = bond.getSlashRecord(slashId).appealWindowClose + bond.pausedTotal();
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.FilingWindowStillOpen.selector, closeAt));
        bond.finalizeUnappealedSlash(slashId);

        // Past the EXTENDED deadline — finalize succeeds.
        vm.warp(slashTime + 40 days + 1);
        bond.finalizeUnappealedSlash(slashId);
        assertEq(uint8(bond.getSlashRecord(slashId).status), uint8(SlashStatus.Upheld));
    }

    // -----------------------------------------------------------------
    // H-1 — pausing SlashAppeal extends the CapacityBond filing window
    // -----------------------------------------------------------------

    function test_slashAppealPause_extendsCapacityBondFilingWindow() public {
        // Pausing SlashAppeal blocks `openSlashAppeal`, so the filing deadline
        // (enforced on CapacityBond) must move by the paused duration even
        // though CapacityBond itself never paused — the combined-counter fix.
        uint256 slashId = _bondAndSlash();
        uint64 slashTime = uint64(block.timestamp);

        vm.startPrank(admin);
        appeal.grantRole(appeal.PAUSER_ROLE(), admin);
        appeal.pause();
        vm.stopPrank();
        vm.warp(slashTime + 10 days);
        vm.prank(admin);
        appeal.unpause();

        // SlashAppeal's pause time was credited to CapacityBond's counter.
        assertEq(bond.pausedTotal(), 10 days);

        // Just past the nominal 30-day deadline — still open (extended +10d).
        vm.warp(slashTime + 30 days + 1);
        uint64 closeAt = bond.getSlashRecord(slashId).appealWindowClose + bond.pausedTotal();
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.FilingWindowStillOpen.selector, closeAt));
        bond.finalizeUnappealedSlash(slashId);

        // The operator can still file inside the extended window.
        _open(slashId);
        assertEq(uint8(bond.getSlashRecord(slashId).status), uint8(SlashStatus.AppealOpen));
    }

    // -----------------------------------------------------------------
    // M-3 — governance escape hatch for a role-revocation-wedged escrow
    // -----------------------------------------------------------------

    function test_forceResolveStuckAppeal_resolvesAfterRoleRevoked() public {
        uint256 slashId = _bondAndSlash();
        _open(slashId); // AppealOpen — escrow locked
        assertEq(uint8(bond.getSlashRecord(slashId).status), uint8(SlashStatus.AppealOpen));

        // Migration mistake: SLASH_APPEAL_ROLE revoked mid-appeal, so SlashAppeal
        // can no longer drive the settle hooks and the escrow is wedged.
        bytes32 appealRole = bond.SLASH_APPEAL_ROLE();
        vm.prank(admin);
        bond.revokeRole(appealRole, address(appeal));

        uint256 challengerBefore = token.balanceOf(challenger);
        uint256 escrowBefore = bond.escrowedTotal();

        vm.prank(admin);
        bond.forceResolveStuckAppeal(slashId);

        // Resolved on the upheld path: 50% to challenger, 50% burned.
        assertEq(uint8(bond.getSlashRecord(slashId).status), uint8(SlashStatus.Upheld));
        assertEq(token.balanceOf(challenger) - challengerBefore, SLASH_AMT / 2);
        assertEq(escrowBefore - bond.escrowedTotal(), SLASH_AMT);
    }

    function test_forceResolveStuckAppeal_revertsWhenNotAppealOpen() public {
        uint256 slashId = _bondAndSlash(); // Escrowed, no appeal opened
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.SlashAppealNotOpen.selector, slashId));
        bond.forceResolveStuckAppeal(slashId);
    }

    function test_forceResolveStuckAppeal_revertsWithoutGovernanceRole() public {
        uint256 slashId = _bondAndSlash();
        _open(slashId);
        bytes32 govRole = bond.GOVERNANCE_ROLE();
        vm.prank(challenger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, challenger, govRole)
        );
        bond.forceResolveStuckAppeal(slashId);
    }
}
