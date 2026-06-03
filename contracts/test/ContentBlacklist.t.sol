// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { ICapacityBondEjector } from "../src/interfaces/ICapacityBondEjector.sol";
import { Token } from "../src/Token.sol";
import { MockRegionBond } from "./mocks/MockRegionBond.sol";

/// @title ContentBlacklist smoke tests
/// @notice Critical-path coverage for hash/operator blacklist, cross-contract
///         `ejectNode` wire, ADR 031 appeal state machine happy + reject
///         + reverse paths, and the SF-M1 `MissingRegion` revert.
contract ContentBlacklistTest is Test {
    Token internal token;
    MockRegionBond internal bondMock;
    ContentBlacklist internal blacklist;

    address internal admin = address(0xA11CE);
    address internal multisig = address(0xC0DE);
    address internal regionalBody = address(0xDE);
    address internal filer = address(0xF1);
    address internal operator = address(0xB0B);

    bytes32 internal constant REGION_US = bytes32("US");
    bytes32 internal constant SAMPLE_HASH = bytes32(uint256(0xABCDEF));
    uint256 internal constant APPEAL_BOND = 100e18;

    function setUp() public {
        token = new Token(admin);
        bondMock = new MockRegionBond();
        blacklist = new ContentBlacklist(ICapacityBondEjector(address(bondMock)), token, admin, APPEAL_BOND);

        // Grant the filer ripe path-2 (Operator) standing in REGION_US so the
        // appeal-lifecycle tests below exercise the {open → fast-track →
        // ratify/reverse/lapse} state machine, not the standing gate. These
        // tests don't probe ripening: window = 0 makes standing ripe at any
        // timestamp. The ADR 030 ripening boundary itself is covered in
        // ContentBlacklistRegionScope.t.sol and CapacityBondRegionE2E.t.sol.
        bondMock.setWindow(0);
        bondMock.setNode(filer, "US", "", 0, 0);

        // Fund filer for appeal bonds.
        vm.prank(admin);
        token.transfer(filer, APPEAL_BOND * 10);
        vm.prank(filer);
        token.approve(address(blacklist), type(uint256).max);

        // Grant multisig + regional body roles.
        bytes32 multisigRole = blacklist.EMERGENCY_MULTISIG_ROLE();
        bytes32 bodyRole = blacklist.REGIONAL_BODY_ROLE();
        vm.startPrank(admin);
        blacklist.grantRole(multisigRole, multisig);
        blacklist.grantRole(bodyRole, regionalBody);
        vm.stopPrank();
    }

    function test_addHashGlobal_andQuery() public {
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH);
        assertTrue(blacklist.isHashBlacklisted(SAMPLE_HASH));
    }

    function test_addHashRegional_globalTakesPrecedence() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
        assertTrue(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));
        assertFalse(blacklist.isHashBlacklisted(SAMPLE_HASH));
    }

    function test_addOperator_callsEjectOnCapacityBond() public {
        vm.prank(admin);
        blacklist.addOperator(operator);
        assertTrue(blacklist.isOperatorBlacklisted(operator));
        assertEq(bondMock.ejectedCount(), 1);
        assertEq(bondMock.ejected(0), operator);
    }

    function test_openBlacklistAppeal_revertsOnZeroRegion() public {
        // SF-M1 fix: must pass region explicitly.
        vm.prank(filer);
        vm.expectRevert();
        blacklist.openBlacklistAppeal(SAMPLE_HASH, bytes32(0), bytes32("e"), ContentBlacklist.StandingPath.Operator);
    }

    function test_openBlacklistAppeal_revertsOnEntryNotBlacklisted() public {
        vm.prank(filer);
        vm.expectRevert();
        blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator);
    }

    function test_fullAppealFlow_ratifyRemovesHash() public {
        // Seed regional blacklist.
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);

        // Filer opens appeal.
        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("evidence"), ContentBlacklist.StandingPath.Operator
        );

        // Multisig fast-tracks (entry.suspended = true).
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);
        assertFalse(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));

        // Governance ratifies, hash removed.
        uint256 filerTokenBefore = token.balanceOf(filer);
        vm.prank(admin);
        blacklist.ratifyBlacklistAppealRemoval(appealId);
        // Bond refunded.
        assertEq(token.balanceOf(filer) - filerTokenBefore, APPEAL_BOND);
        // Entry deleted.
        assertEq(blacklist.getHashEntry(REGION_US, SAMPLE_HASH).addedAt, 0);
    }

    function test_rejectAppeal_burnsBond() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
        vm.prank(filer);
        uint256 appealId =
            blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator);

        uint256 supplyBefore = token.totalSupply();
        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(appealId);
        assertEq(supplyBefore - token.totalSupply(), APPEAL_BOND);
    }

    function test_reverseAppeal_clearsSuspendedAndBurnsBond() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
        vm.prank(filer);
        uint256 appealId =
            blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator);
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);
        // Suspended → not enforced.
        assertFalse(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));

        uint256 supplyBefore = token.totalSupply();
        vm.prank(admin);
        blacklist.reverseBlacklistAppeal(appealId);
        // Suspended cleared → blacklist live again.
        assertTrue(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));
        assertEq(supplyBefore - token.totalSupply(), APPEAL_BOND);
    }

    /// @notice M-1 — the per-region concurrent cap counts only FAST-TRACKED
    ///         appeals (the ones holding interim relief), not Open ones. Open
    ///         filings no longer crowd out others, but simultaneous suspensions
    ///         per region stay bounded.
    function test_regionalCap_countsFastTrackedNotOpen() public {
        // Seed 4 hashes in REGION_US.
        for (uint256 i = 0; i < 4; i++) {
            vm.prank(regionalBody);
            blacklist.addHashRegional(REGION_US, bytes32(i + 1));
        }
        // Open 4 appeals — ALL succeed: Open appeals do not charge the cap.
        uint256[4] memory ids;
        for (uint256 i = 0; i < 4; i++) {
            vm.prank(filer);
            ids[i] = blacklist.openBlacklistAppeal(
                bytes32(i + 1), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator
            );
        }
        // Fast-track 3 — each charges the per-region relief cap.
        for (uint256 i = 0; i < 3; i++) {
            vm.prank(multisig);
            blacklist.fastTrackBlacklistAppeal(ids[i]);
        }
        // The 4th fast-track hits the cap.
        vm.prank(multisig);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.RegionalCapHit.selector, REGION_US, uint256(3)));
        blacklist.fastTrackBlacklistAppeal(ids[3]);
    }

    /// @notice H-2 — re-adding a hash while an appeal is live must revert
    ///         rather than silently un-suspend and orphan the in-flight appeal
    ///         (and reset the slash-eligibility boundary).
    function test_addHash_revertsWhileAppealActive() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
        vm.prank(filer);
        uint256 appealId =
            blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator);
        assertTrue(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));

        // Re-add blocked while the appeal is live.
        vm.prank(regionalBody);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.HashHasActiveAppeal.selector, REGION_US, SAMPLE_HASH));
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);

        // Once the appeal terminates, the re-add is allowed again.
        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(appealId);
        assertFalse(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
    }

    /// @notice T-3 — concurrent appeals on the same (region, hash) revert.
    function test_openBlacklistAppeal_revertsDuplicatePerHash() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);

        vm.prank(filer);
        blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e1"), ContentBlacklist.StandingPath.Operator);

        // Fund a second filer; even with a different filer, same (region, hash)
        // can't have a second active appeal.
        address filer2 = address(0xF2);
        vm.prank(admin);
        token.transfer(filer2, APPEAL_BOND);
        vm.prank(filer2);
        token.approve(address(blacklist), type(uint256).max);

        vm.prank(filer2);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.AppealAlreadyActive.selector, REGION_US, SAMPLE_HASH));
        blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e2"), ContentBlacklist.StandingPath.Operator);

        assertTrue(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
    }

    /// @notice T-3 — `hasActiveAppeal` clears on reject, allowing a new
    ///         appeal to be filed afterwards.
    function test_hasActiveAppeal_clearedOnReject() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
        vm.prank(filer);
        uint256 appealId =
            blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator);
        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(appealId);

        assertFalse(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));

        // A follow-up open should succeed (freq cap doesn't apply since the
        // prior appeal was rejected, not ratified).
        vm.prank(filer);
        blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e2"), ContentBlacklist.StandingPath.Operator);
        assertTrue(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
    }

    /// @notice T-3 — `hasActiveAppeal` clears on reverse.
    function test_hasActiveAppeal_clearedOnReverse() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
        vm.prank(filer);
        uint256 appealId =
            blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator);
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);
        vm.prank(admin);
        blacklist.reverseBlacklistAppeal(appealId);
        assertFalse(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
    }

    // -----------------------------------------------------------------
    // Access-control guards on governance / regional-body setters
    // -----------------------------------------------------------------

    function test_addHashGlobal_revertsWithoutGovernanceRole() public {
        _expectMissingRole(filer, blacklist.GOVERNANCE_ROLE());
        vm.prank(filer);
        blacklist.addHashGlobal(SAMPLE_HASH);
    }

    function test_addHashRegional_revertsWithoutRegionalBodyRole() public {
        _expectMissingRole(filer, blacklist.REGIONAL_BODY_ROLE());
        vm.prank(filer);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
    }

    function test_registerRegionalBody_revertsWithoutGovernanceRole() public {
        _expectMissingRole(filer, blacklist.GOVERNANCE_ROLE());
        vm.prank(filer);
        blacklist.registerRegionalBody(address(0xBEEF));
    }

    function test_setAppealBond_revertsWithoutGovernanceRole() public {
        _expectMissingRole(filer, blacklist.GOVERNANCE_ROLE());
        vm.prank(filer);
        blacklist.setAppealBond(APPEAL_BOND * 2);
    }

    /// @dev See `CapacityBond.t.sol:_expectMissingRole` for rationale.
    function _expectMissingRole(address caller, bytes32 role) internal {
        vm.expectRevert(abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, caller, role));
    }

    /// @notice `REGIONAL_BODY_ROLE` must NOT be able to remove `GLOBAL_REGION`
    ///         entries — that would let any regional body bypass governance.
    ///         Mirrors the contract comment on `removeHashRegional`.
    function test_removeHashRegional_blocksGlobalRegionEntries() public {
        // Seed a global entry.
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH);

        // Regional body tries to delete via removeHashRegional with the
        // GLOBAL_REGION sentinel (must revert with MissingRegion).
        bytes32 globalRegion = bytes32("GLOBAL");
        vm.prank(regionalBody);
        vm.expectRevert(ContentBlacklist.MissingRegion.selector);
        blacklist.removeHashRegional(globalRegion, SAMPLE_HASH);

        // Entry remains live.
        assertTrue(blacklist.isHashBlacklisted(SAMPLE_HASH));
    }

    /// @notice T-3 — `hasActiveAppeal` clears on lapse from Open and
    ///         FastTracked branches.
    function test_hasActiveAppeal_clearedOnLapseOpen() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
        vm.prank(filer);
        uint256 appealId =
            blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator);
        vm.warp(block.timestamp + 15 days);
        blacklist.cleanupExpiredBlacklistAppeal(appealId);
        assertFalse(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
    }
}
