// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { SlashAppeal } from "../src/SlashAppeal.sol";
import { CapacityBond } from "../src/CapacityBond.sol";
import { Token } from "../src/Token.sol";
import { ICapacityBond } from "../src/interfaces/ICapacityBond.sol";
import { ISlashAppeal } from "../src/interfaces/ISlashAppeal.sol";

import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";

/// @title SlashAppeal flow tests (ADR 028 escrow-on-slash)
/// @notice Drives the full appeal state machine against a real `CapacityBond`,
///         asserting the escrow settle hooks move the slashed TOKEN correctly:
///         grant → operator refunded + zero-out cleared; uphold/reject →
///         escrow distributed 50/50 and the appeal bond burned (or split).
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
    address internal pool = address(0xCCEE);

    uint256 internal constant MIN_STAKE = 50_000e18;
    uint256 internal constant APPEAL_BOND = 1000e18;
    uint256 internal constant SLASH_AMT = 2500e18; // 5% of MIN_STAKE

    function setUp() public {
        token = new Token(admin);
        ed25519 = new MockEd25519Verifier();

        bond = new CapacityBond({
            token_: token,
            ed25519Verifier_: ed25519,
            admin: admin,
            minStake_: MIN_STAKE,
            unbondingPeriod_: 7 days,
            multiaddrUpdateCooldown_: 0,
            maxMultiaddrSize_: 1024,
            regionStabilityWindow_: 7 days,
            genesisCreditWindow_: 30 days
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
        appeal.setChallengerIncentivePool(pool);
        token.transfer(operator, 100_000e18);
        token.transfer(appellant, 10_000e18);
        vm.stopPrank();

        vm.prank(operator);
        token.approve(address(bond), type(uint256).max);
        vm.prank(appellant);
        token.approve(address(appeal), type(uint256).max);

        // Warp well past epoch 0 so slash timestamps are non-trivial.
        vm.warp(100 days);
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    function _stakeAndSlash() internal returns (uint256 slashId) {
        vm.prank(operator);
        bond.stake(MIN_STAKE);
        vm.prank(admin);
        (slashId,) = bond.slash(operator, challenger, 1);
    }

    function _open(uint256 slashId) internal {
        vm.prank(appellant);
        appeal.openSlashAppeal(slashId, keccak256("evidence"));
    }

    // -----------------------------------------------------------------
    // open
    // -----------------------------------------------------------------

    function test_open_locksEscrowAndPullsBond() public {
        uint256 slashId = _stakeAndSlash();
        uint256 bondBefore = token.balanceOf(appellant);
        _open(slashId);

        // Bond pulled into the appeal contract.
        assertEq(bondBefore - token.balanceOf(appellant), APPEAL_BOND);
        assertEq(token.balanceOf(address(appeal)), APPEAL_BOND);
        // CapacityBond escrow flipped to AppealOpen.
        assertEq(uint8(bond.getSlashRecord(slashId).status), uint8(CapacityBond.SlashStatus.AppealOpen));
        assertEq(uint8(appeal.getAppeal(slashId).status), uint8(ISlashAppeal.AppealStatus.Open));
    }

    function test_open_revertsUnknownSlash() public {
        vm.prank(appellant);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.UnknownSlash.selector, uint256(0)));
        appeal.openSlashAppeal(0, keccak256("e"));
    }

    function test_open_revertsAfterFilingWindow() public {
        uint256 slashId = _stakeAndSlash();
        uint64 closeTs = bond.getSlashRecord(slashId).appealWindowClose;
        vm.warp(block.timestamp + 30 days + 1);
        vm.prank(appellant);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.FilingWindowClosed.selector, closeTs));
        appeal.openSlashAppeal(slashId, keccak256("e"));
    }

    function test_open_revertsOnDoubleAppeal() public {
        uint256 slashId = _stakeAndSlash();
        _open(slashId);
        vm.prank(appellant);
        vm.expectRevert(abi.encodeWithSelector(SlashAppeal.AppealAlreadyExists.selector, slashId));
        appeal.openSlashAppeal(slashId, keccak256("e2"));
    }

    // -----------------------------------------------------------------
    // grant (operator vindicated)
    // -----------------------------------------------------------------

    function test_grant_refundsOperatorAndBondAndClearsZeroOut() public {
        uint256 slashId = _stakeAndSlash();
        assertGt(bond.slashedAtEpoch(operator), 0);
        _open(slashId);

        uint256 opBefore = token.balanceOf(operator);
        uint256 appellantBefore = token.balanceOf(appellant);

        vm.prank(multisig);
        appeal.fastTrackAppeal(slashId);
        vm.prank(admin);
        appeal.grantAppeal(slashId);

        // Operator gets their escrowed TOKEN back; zero-out cleared.
        assertEq(token.balanceOf(operator) - opBefore, SLASH_AMT);
        assertEq(bond.slashedAtEpoch(operator), 0);
        assertEq(bond.escrowedTotal(), 0);
        // Appeal bond refunded to the appellant.
        assertEq(token.balanceOf(appellant) - appellantBefore, APPEAL_BOND);
        assertEq(bond.getSlashRecord(slashId).status == CapacityBond.SlashStatus.Reversed, true);
        // Frequency cap stamped.
        assertEq(appeal.lastAcceptedAppealAt(operator), uint64(block.timestamp));
    }

    function test_grant_revertsIfNotFastTracked() public {
        uint256 slashId = _stakeAndSlash();
        _open(slashId);
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(SlashAppeal.AppealNotFastTracked.selector, slashId));
        appeal.grantAppeal(slashId);
    }

    function test_frequencyCap_blocksSecondAppealWithinWindow() public {
        uint256 slashId = _stakeAndSlash();
        _open(slashId);
        vm.prank(multisig);
        appeal.fastTrackAppeal(slashId);
        vm.prank(admin);
        appeal.grantAppeal(slashId);

        // Second slash of the same operator, fresh escrow.
        vm.prank(admin);
        (uint256 slashId2,) = bond.slash(operator, challenger, 1);
        vm.prank(appellant);
        vm.expectRevert(
            abi.encodeWithSelector(SlashAppeal.FrequencyCapHit.selector, uint64(block.timestamp + 365 days))
        );
        appeal.openSlashAppeal(slashId2, keccak256("e"));
    }

    // -----------------------------------------------------------------
    // uphold / reject (slash stands)
    // -----------------------------------------------------------------

    function test_uphold_distributesEscrowAndSplitsBond() public {
        uint256 slashId = _stakeAndSlash();
        _open(slashId);
        vm.prank(multisig);
        appeal.fastTrackAppeal(slashId);

        uint256 challengerBefore = token.balanceOf(challenger);
        uint256 supplyBefore = token.totalSupply();
        uint256 poolBefore = token.balanceOf(pool);

        vm.prank(admin);
        appeal.upholdAppeal(slashId);

        // Escrow 50/50: challenger 1250, burn 1250.
        assertEq(token.balanceOf(challenger) - challengerBefore, SLASH_AMT / 2);
        // Bond 50/50: 500 burned, 500 to pool. Total burned = 1250 + 500.
        assertEq(token.balanceOf(pool) - poolBefore, APPEAL_BOND / 2);
        assertEq(supplyBefore - token.totalSupply(), SLASH_AMT / 2 + APPEAL_BOND / 2);
        assertEq(bond.escrowedTotal(), 0);
    }

    function test_reject_burnsBondAndDistributesEscrow() public {
        uint256 slashId = _stakeAndSlash();
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
        uint256 slashId = _stakeAndSlash();
        _open(slashId);

        vm.warp(block.timestamp + 14 days + 1);
        uint256 supplyBefore = token.totalSupply();
        appeal.cleanupExpiredAppeal(slashId); // permissionless

        // Review-window lapse → upheld: bond burned + escrow 50/50.
        assertEq(supplyBefore - token.totalSupply(), APPEAL_BOND + SLASH_AMT / 2);
        assertEq(bond.escrowedTotal(), 0);
    }

    function test_cleanup_fastTrackedLapse_grantsOperatorFavorable() public {
        uint256 slashId = _stakeAndSlash();
        _open(slashId);
        vm.prank(multisig);
        appeal.fastTrackAppeal(slashId);

        uint256 opBefore = token.balanceOf(operator);
        uint256 appellantBefore = token.balanceOf(appellant);

        vm.warp(block.timestamp + 14 days + 1);
        appeal.cleanupExpiredAppeal(slashId); // permissionless

        // Ratification-window lapse → operator-favorable grant: escrow refunded
        // to operator, bond refunded to appellant, zero-out cleared.
        assertEq(token.balanceOf(operator) - opBefore, SLASH_AMT);
        assertEq(token.balanceOf(appellant) - appellantBefore, APPEAL_BOND);
        assertEq(bond.slashedAtEpoch(operator), 0);
    }

    // -----------------------------------------------------------------
    // access control
    // -----------------------------------------------------------------

    function test_fastTrack_requiresMultisig() public {
        uint256 slashId = _stakeAndSlash();
        _open(slashId);
        bytes32 role = appeal.EMERGENCY_MULTISIG_ROLE();
        vm.prank(operator);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, operator, role)
        );
        appeal.fastTrackAppeal(slashId);
    }

    function test_grant_requiresGovernance() public {
        uint256 slashId = _stakeAndSlash();
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
}
