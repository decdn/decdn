// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { IStakingRegistryEject } from "../src/interfaces/IStakingRegistryEject.sol";
import { MockStakingRegistryEject } from "./mocks/MockStakingRegistryEject.sol";

contract ContentBlacklistTest is Test {
    MockStakingRegistryEject internal staking;
    ContentBlacklist internal cb;

    address internal admin = makeAddr("admin");
    address internal governance = makeAddr("governance");
    address internal emergency = makeAddr("emergency");
    address internal euBody = makeAddr("euBody");
    address internal operator = makeAddr("operator");

    bytes32 internal constant H = keccak256("blob1");
    bytes32 internal constant H2 = keccak256("blob2");

    bytes2 internal constant EU = hex"4555"; // canonical bytes2 form of "EU"

    // Category enum mirror.
    uint8 internal constant GENERAL = 0;
    uint8 internal constant CSAM = 1;
    uint8 internal constant TERRORIST = 2;

    function setUp() public {
        vm.warp(1_700_000_000); // realistic timestamp before deploy (sets blacklistDeadline base)
        staking = new MockStakingRegistryEject();
        cb = new ContentBlacklist(IStakingRegistryEject(address(staking)), admin, governance, emergency);
    }

    // -----------------------------------------------------------------
    // Constructor / initial state
    // -----------------------------------------------------------------

    function test_constructor_revertsOnZeroArgs() public {
        vm.expectRevert(ContentBlacklist.ZeroAddress.selector);
        new ContentBlacklist(IStakingRegistryEject(address(0)), admin, governance, emergency);
        vm.expectRevert(ContentBlacklist.ZeroAddress.selector);
        new ContentBlacklist(IStakingRegistryEject(address(staking)), address(0), governance, emergency);
        vm.expectRevert(ContentBlacklist.ZeroAddress.selector);
        new ContentBlacklist(IStakingRegistryEject(address(staking)), admin, address(0), emergency);
        vm.expectRevert(ContentBlacklist.ZeroAddress.selector);
        new ContentBlacklist(IStakingRegistryEject(address(staking)), admin, governance, address(0));
    }

    function test_initialState() public view {
        assertEq(cb.complianceWindow(), 24 hours);
        assertEq(cb.version(), 0);
        assertEq(cb.blacklistDeadline(), uint256(block.timestamp) + 365 days);
        assertTrue(cb.hasRole(cb.GOVERNANCE_ROLE(), governance));
        assertTrue(cb.hasRole(cb.EMERGENCY_ROLE(), emergency));
        assertTrue(cb.hasRole(cb.DEFAULT_ADMIN_ROLE(), admin));
    }

    // -----------------------------------------------------------------
    // Global hash path
    // -----------------------------------------------------------------

    function test_addHash_blacklistsAndSetsEffectiveAt() public {
        vm.prank(governance);
        cb.addHash(H, "DMCA-001");

        assertTrue(cb.isBlacklisted(H));
        assertEq(cb.version(), 1);

        ContentBlacklist.BlacklistEntry memory e = cb.getEntry(H);
        assertEq(e.addedAt, uint256(block.timestamp));
        assertEq(e.effectiveAt, uint256(block.timestamp) + 24 hours);
        assertEq(e.expiresAt, 0, "governance entry never expires");
        assertEq(e.region, bytes2(0));
        assertFalse(e.emergency);
        assertEq(e.reason, "DMCA-001");
    }

    function test_addHash_onlyGovernance() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector, address(this), cb.GOVERNANCE_ROLE()
            )
        );
        cb.addHash(H, "x");
    }

    function test_addHash_revertsOnZeroHash() public {
        vm.prank(governance);
        vm.expectRevert(ContentBlacklist.ZeroHash.selector);
        cb.addHash(bytes32(0), "x");
    }

    function test_removeHash_clearsAndBumpsVersion() public {
        vm.prank(governance);
        cb.addHash(H, "x");
        vm.prank(governance);
        cb.removeHash(H);
        assertFalse(cb.isBlacklisted(H));
        assertEq(cb.version(), 2);
        assertEq(cb.getEntry(H).addedAt, 0);
    }

    function test_removeHash_revertsIfNotFound() public {
        vm.prank(governance);
        vm.expectRevert(ContentBlacklist.EntryNotFound.selector);
        cb.removeHash(H);
    }

    function test_addHash_overwriteRefreshesEffectiveAt() public {
        vm.prank(governance);
        cb.addHash(H, "first");
        uint256 firstEffective = cb.getEntry(H).effectiveAt;

        vm.warp(block.timestamp + 1 hours);
        vm.prank(governance);
        cb.addHash(H, "second");
        ContentBlacklist.BlacklistEntry memory e = cb.getEntry(H);
        assertEq(e.reason, "second");
        assertEq(e.effectiveAt, firstEffective + 1 hours, "effectiveAt refreshed");
    }

    // -----------------------------------------------------------------
    // Regional path + bodies
    // -----------------------------------------------------------------

    function _registerEu() internal {
        vm.prank(governance);
        cb.registerRegionalBody("EU", euBody);
    }

    function test_regionalBody_canBlacklistInRegion() public {
        _registerEu();
        vm.prank(euBody);
        cb.addHashRegional(H, "EU", "DSA-DE-1");

        assertFalse(cb.isBlacklisted(H), "not global");
        assertTrue(cb.isBlacklistedInRegion(H, "EU"));
        assertFalse(cb.isBlacklistedInRegion(H, "US"), "other region unaffected");
    }

    function test_globalEntry_blacklistsAllRegions() public {
        vm.prank(governance);
        cb.addHash(H, "global");
        assertTrue(cb.isBlacklistedInRegion(H, "EU"));
        assertTrue(cb.isBlacklistedInRegion(H, "US"));
    }

    function test_addHashRegional_revertsForNonBody() public {
        _registerEu();
        vm.expectRevert(ContentBlacklist.NotRegionalBody.selector);
        cb.addHashRegional(H, "EU", "x"); // caller is test, not euBody
    }

    function test_addHashRegional_revertsWhenSuspended() public {
        _registerEu();
        vm.prank(emergency);
        cb.suspendRegionalBody("EU");
        vm.prank(euBody);
        vm.expectRevert(ContentBlacklist.RegionalBodyIsSuspended.selector);
        cb.addHashRegional(H, "EU", "x");
    }

    function test_regionCanonicalization_lowercaseEqualsUpper() public {
        _registerEu();
        vm.prank(euBody);
        cb.addHashRegional(H, "eu", "lowercase"); // lowercase canonicalizes to EU
        assertTrue(cb.isBlacklistedInRegion(H, "EU"));
        assertTrue(cb.isBlacklistedInRegion(H, "eU"));
    }

    function test_invalidRegion_reverts() public {
        _registerEu();
        vm.startPrank(euBody);
        vm.expectRevert(ContentBlacklist.InvalidRegion.selector);
        cb.addHashRegional(H, "USA", "too long");
        vm.expectRevert(ContentBlacklist.InvalidRegion.selector);
        cb.addHashRegional(H, "U1", "non-alpha");
        vm.stopPrank();
    }

    function test_addHashRegional_emptyRegionReverts() public {
        _registerEu();
        vm.prank(euBody);
        vm.expectRevert(ContentBlacklist.EmptyRegion.selector);
        cb.addHashRegional(H, "", "x");
    }

    function test_removeHashRegional_onlyGovernance() public {
        _registerEu();
        vm.prank(euBody);
        cb.addHashRegional(H, "EU", "x");

        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector, euBody, cb.GOVERNANCE_ROLE()
            )
        );
        vm.prank(euBody);
        cb.removeHashRegional(H, "EU");

        vm.prank(governance);
        cb.removeHashRegional(H, "EU");
        assertFalse(cb.isBlacklistedInRegion(H, "EU"));
    }

    function test_registerRegionalBody_revertsIfRegionTaken() public {
        _registerEu();
        vm.prank(governance);
        vm.expectRevert(ContentBlacklist.RegionAlreadyHasBody.selector);
        cb.registerRegionalBody("EU", makeAddr("otherBody"));
    }

    function test_deregisterRegionalBody_leavesEntriesActive() public {
        _registerEu();
        vm.prank(euBody);
        cb.addHashRegional(H, "EU", "x");
        vm.prank(governance);
        cb.deregisterRegionalBody("EU");
        assertEq(cb.regionalBodyOf(EU), address(0));
        assertTrue(cb.isBlacklistedInRegion(H, "EU"), "existing entry remains active");
    }

    function test_unsuspendRegionalBody_restores() public {
        _registerEu();
        vm.prank(emergency);
        cb.suspendRegionalBody("EU");
        assertTrue(cb.isRegionalBodySuspended(EU));
        vm.prank(governance);
        cb.unsuspendRegionalBody("EU");
        assertFalse(cb.isRegionalBodySuspended(EU));
        vm.prank(euBody);
        cb.addHashRegional(H, "EU", "x"); // works again
        assertTrue(cb.isBlacklistedInRegion(H, "EU"));
    }

    function test_suspendRegionalBody_onlyEmergency() public {
        _registerEu();
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector, governance, cb.EMERGENCY_ROLE()
            )
        );
        vm.prank(governance);
        cb.suspendRegionalBody("EU");
    }

    // -----------------------------------------------------------------
    // Emergency path
    // -----------------------------------------------------------------

    function test_emergencyAdd_effectiveAtIs2hAndExpires() public {
        vm.prank(emergency);
        cb.emergencyAdd(H, GENERAL, "CSAM-ish");
        ContentBlacklist.BlacklistEntry memory e = cb.getEntry(H);
        assertTrue(e.emergency);
        assertEq(e.effectiveAt, uint256(block.timestamp) + 2 hours);
        assertEq(e.expiresAt, uint256(block.timestamp) + 14 days);
        assertTrue(cb.isBlacklisted(H));
    }

    function test_emergencyAdd_generalExpiresAfter14d() public {
        vm.prank(emergency);
        cb.emergencyAdd(H, GENERAL, "x");
        vm.warp(block.timestamp + 14 days);
        assertFalse(cb.isBlacklisted(H), "expired at 14d boundary");
    }

    function test_emergencyAdd_severeExpiresAfter90d() public {
        vm.prank(emergency);
        cb.emergencyAdd(H, CSAM, "x");
        vm.warp(block.timestamp + 14 days + 1);
        assertTrue(cb.isBlacklisted(H), "CSAM still active at 14d");
        vm.warp(block.timestamp + 90 days);
        assertFalse(cb.isBlacklisted(H), "CSAM expired at 90d");
    }

    function test_emergencyAdd_onlyEmergencyRole() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector, governance, cb.EMERGENCY_ROLE()
            )
        );
        vm.prank(governance);
        cb.emergencyAdd(H, GENERAL, "x");
    }

    function test_emergencyAdd_invalidCategoryReverts() public {
        vm.prank(emergency);
        vm.expectRevert(ContentBlacklist.InvalidCategory.selector);
        cb.emergencyAdd(H, 3, "x");
    }

    function test_emergencyAdd_revertsAfterSunset() public {
        vm.warp(cb.blacklistDeadline());
        vm.prank(emergency);
        vm.expectRevert(ContentBlacklist.EmergencySunsetReached.selector);
        cb.emergencyAdd(H, GENERAL, "x");
    }

    function test_governanceAddHash_overridesExpiredEmergency() public {
        vm.prank(emergency);
        cb.emergencyAdd(H, GENERAL, "emergency");
        vm.warp(block.timestamp + 14 days); // expired
        assertFalse(cb.isBlacklisted(H));

        vm.prank(governance);
        cb.addHash(H, "ratified"); // overwrites with permanent entry
        ContentBlacklist.BlacklistEntry memory e = cb.getEntry(H);
        assertFalse(e.emergency);
        assertEq(e.expiresAt, 0);
        assertTrue(cb.isBlacklisted(H));
    }

    // -----------------------------------------------------------------
    // Origin path
    // -----------------------------------------------------------------

    function test_addOrigin_ejectsAndBlacklists() public {
        vm.expectCall(address(staking), abi.encodeCall(IStakingRegistryEject.ejectNode, (operator)));
        vm.prank(governance);
        cb.addOrigin(operator, "repeat-sourcing");

        assertTrue(cb.isOriginBlacklisted(operator));
        assertEq(staking.ejectedCount(), 1);
        assertEq(staking.ejected(0), operator);
    }

    function test_addOrigin_onlyGovernance() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector, address(this), cb.GOVERNANCE_ROLE()
            )
        );
        cb.addOrigin(operator, "x");
    }

    function test_removeOrigin_clearsWithoutUnejecting() public {
        vm.prank(governance);
        cb.addOrigin(operator, "x");
        vm.prank(governance);
        cb.removeOrigin(operator);
        assertFalse(cb.isOriginBlacklisted(operator));
        // removeOrigin does not call back into StakingRegistry — still 1 eject.
        assertEq(staking.ejectedCount(), 1);
    }

    function test_removeOrigin_revertsIfNotFound() public {
        vm.prank(governance);
        vm.expectRevert(ContentBlacklist.OriginNotFound.selector);
        cb.removeOrigin(operator);
    }

    function test_emergencyAddOrigin_ejectsAndExpires() public {
        vm.expectCall(address(staking), abi.encodeCall(IStakingRegistryEject.ejectNode, (operator)));
        vm.prank(emergency);
        cb.emergencyAddOrigin(operator, CSAM, "x");
        assertTrue(cb.isOriginBlacklisted(operator));
        vm.warp(block.timestamp + 90 days);
        assertFalse(cb.isOriginBlacklisted(operator), "emergency origin expired at 90d");
    }

    // -----------------------------------------------------------------
    // Compliance window
    // -----------------------------------------------------------------

    function test_setComplianceWindow_bounds() public {
        vm.startPrank(governance);
        vm.expectRevert(
            abi.encodeWithSelector(
                ContentBlacklist.ParamOutOfBounds.selector, uint256(1 hours - 1), uint256(1 hours), uint256(7 days)
            )
        );
        cb.setComplianceWindow(1 hours - 1);
        vm.expectRevert(
            abi.encodeWithSelector(
                ContentBlacklist.ParamOutOfBounds.selector, uint256(7 days + 1), uint256(1 hours), uint256(7 days)
            )
        );
        cb.setComplianceWindow(7 days + 1);
        cb.setComplianceWindow(3 days);
        vm.stopPrank();
        assertEq(cb.complianceWindow(), 3 days);
    }

    function test_complianceWindow_affectsEffectiveAt() public {
        vm.prank(governance);
        cb.setComplianceWindow(2 days);
        vm.prank(governance);
        cb.addHash(H, "x");
        assertEq(cb.getEntry(H).effectiveAt, uint256(block.timestamp) + 2 days);
    }

    // -----------------------------------------------------------------
    // Version counter
    // -----------------------------------------------------------------

    function test_version_bumpsOnEveryMutation() public {
        assertEq(cb.version(), 0);
        vm.prank(governance);
        cb.addHash(H, "x");
        assertEq(cb.version(), 1);
        vm.prank(emergency);
        cb.emergencyAdd(H2, GENERAL, "x");
        assertEq(cb.version(), 2);
        vm.prank(governance);
        cb.addOrigin(operator, "x");
        assertEq(cb.version(), 3);
        vm.prank(governance);
        cb.removeHash(H);
        assertEq(cb.version(), 4);
    }

    // -----------------------------------------------------------------
    // getEntryInRegion + canonical region emission
    // -----------------------------------------------------------------

    function test_getEntryInRegion_regionalAndEmptyGlobal() public {
        _registerEu();
        vm.prank(euBody);
        cb.addHashRegional(H, "EU", "x");
        vm.prank(governance);
        cb.addHash(H, "global");

        assertEq(cb.getEntryInRegion(H, "EU").region, EU);
        // Empty region resolves to the global entry (consistent with isBlacklistedInRegion).
        assertEq(cb.getEntryInRegion(H, "").region, bytes2(0));
        assertEq(cb.getEntryInRegion(H, "").addedAt, cb.getEntry(H).addedAt);
    }

    function test_removeHashRegional_emitsCanonicalRegion() public {
        _registerEu();
        vm.prank(euBody);
        cb.addHashRegional(H, "EU", "x");
        // Called with lowercase "eu"; HashRemoved must carry the canonical "EU".
        vm.expectEmit(true, false, false, true, address(cb));
        emit ContentBlacklist.HashRemoved(H, 0, "EU");
        vm.prank(governance);
        cb.removeHashRegional(H, "eu");
    }
}
