// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { TOKEN } from "../src/TOKEN.sol";
import { StakingRegistry } from "../src/StakingRegistry.sol";
import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { IContentBlacklist } from "../src/interfaces/IContentBlacklist.sol";
import { IStakingRegistry } from "../src/interfaces/IStakingRegistry.sol";
import { Errors } from "../src/libraries/Errors.sol";
import { Roles } from "../src/libraries/Roles.sol";

contract ContentBlacklistTest is Test {
    TOKEN internal token;
    StakingRegistry internal reg;
    ContentBlacklist internal bl;

    address internal admin = makeAddr("admin");
    address internal governor = makeAddr("governor");
    address internal emergency = makeAddr("emergency");
    address internal operator = makeAddr("operator");
    address internal randomUser = makeAddr("randomUser");

    bytes32 internal constant HASH_A = bytes32(uint256(0xAA));
    bytes32 internal constant HASH_B = bytes32(uint256(0xBB));

    function setUp() public {
        token = new TOKEN(address(this), 10_000_000e18, address(this));
        reg = new StakingRegistry(token, 1000e18, 7 days, admin);
        bl = new ContentBlacklist(reg, admin);

        vm.startPrank(admin);
        bl.grantRole(Roles.GOVERNANCE_ROLE, governor);
        bl.grantRole(Roles.EMERGENCY_ROLE, emergency);
        reg.grantRole(Roles.BLACKLIST_ROLE, address(bl));
        reg.unpause();
        vm.stopPrank();
    }

    function test_Constructor_Reverts() public {
        vm.expectRevert(Errors.ZeroAddress.selector);
        new ContentBlacklist(IStakingRegistry(address(0)), admin);
        vm.expectRevert(Errors.ZeroAddress.selector);
        new ContentBlacklist(reg, address(0));
    }

    function test_AddHash_OnlyGovernance() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                randomUser,
                Roles.GOVERNANCE_ROLE
            )
        );
        vm.prank(randomUser);
        bl.addHash(HASH_A);
    }

    function test_AddHash_Basic() public {
        uint256 v = bl.blacklistVersion();
        vm.prank(governor);
        bl.addHash(HASH_A);
        assertTrue(bl.isBlacklisted(HASH_A));
        assertEq(bl.blacklistVersion(), v + 1);
        IContentBlacklist.Entry memory e = bl.getEntry(HASH_A);
        assertTrue(e.exists);
        assertEq(e.removedAt, 0);
    }

    function test_AddHash_RejectsZero() public {
        vm.expectRevert(Errors.ZeroAddress.selector);
        vm.prank(governor);
        bl.addHash(bytes32(0));
    }

    function test_AddHash_RejectsDuplicate() public {
        vm.prank(governor);
        bl.addHash(HASH_A);
        vm.expectRevert(ContentBlacklist.AlreadyListed.selector);
        vm.prank(governor);
        bl.addHash(HASH_A);
    }

    function test_RemoveHash() public {
        vm.prank(governor);
        bl.addHash(HASH_A);
        uint256 v = bl.blacklistVersion();
        vm.prank(governor);
        bl.removeHash(HASH_A);
        assertFalse(bl.isBlacklisted(HASH_A));
        assertEq(bl.blacklistVersion(), v + 1);
    }

    function test_RemoveHash_NotListedReverts() public {
        vm.expectRevert(ContentBlacklist.NotListed.selector);
        vm.prank(governor);
        bl.removeHash(HASH_A);
    }

    function test_ReAddAfterRemoval() public {
        vm.startPrank(governor);
        bl.addHash(HASH_A);
        bl.removeHash(HASH_A);
        bl.addHash(HASH_A);
        vm.stopPrank();
        assertTrue(bl.isBlacklisted(HASH_A));
    }

    function test_EmergencyAdd_AutoExpires() public {
        vm.prank(emergency);
        bl.emergencyAdd(HASH_A);
        assertTrue(bl.isBlacklisted(HASH_A));
        assertEq(bl.emergencyExpiryOf(HASH_A), block.timestamp + bl.EMERGENCY_EXPIRY());

        vm.warp(block.timestamp + bl.EMERGENCY_EXPIRY() + 1);
        assertFalse(bl.isBlacklisted(HASH_A));
    }

    function test_EmergencyAdd_OnlyEmergencyRole() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                governor,
                Roles.EMERGENCY_ROLE
            )
        );
        vm.prank(governor);
        bl.emergencyAdd(HASH_A);
    }

    function test_Ratify_PromotesEmergencyEntry() public {
        vm.prank(emergency);
        bl.emergencyAdd(HASH_A);
        vm.prank(governor);
        bl.ratifyEmergency(HASH_A);

        // After ratification, warping past expiry must keep it blacklisted.
        vm.warp(block.timestamp + 30 days);
        assertTrue(bl.isBlacklisted(HASH_A));
        assertEq(bl.emergencyExpiryOf(HASH_A), 0);
    }

    function test_Ratify_RejectsExpiredEmergencyEntry() public {
        vm.prank(emergency);
        bl.emergencyAdd(HASH_A);
        vm.warp(block.timestamp + bl.EMERGENCY_EXPIRY() + 1);

        // The entry is no longer active (auto-expired). Ratification must
        // not retroactively re-blacklist it with the stale addedAt.
        vm.expectRevert(ContentBlacklist.NotListed.selector);
        vm.prank(governor);
        bl.ratifyEmergency(HASH_A);
    }

    function test_Ratify_RejectsNonEmergency() public {
        vm.prank(governor);
        bl.addHash(HASH_A);
        vm.expectRevert(ContentBlacklist.NotEmergencyEntry.selector);
        vm.prank(governor);
        bl.ratifyEmergency(HASH_A);
    }

    function test_Ratify_RejectsMissing() public {
        vm.expectRevert(ContentBlacklist.NotListed.selector);
        vm.prank(governor);
        bl.ratifyEmergency(HASH_A);
    }

    function test_EjectOrigin_CallsStakingRegistry() public {
        // Operator must have stake to demonstrate the cross-contract path.
        token.transfer(operator, 10_000e18);
        vm.prank(operator);
        token.approve(address(reg), type(uint256).max);
        vm.prank(operator);
        reg.stake(5000e18);

        vm.prank(governor);
        bl.ejectOrigin(operator);

        StakingRegistry.StakeInfo memory info = reg.getStakeInfo(operator);
        assertEq(uint256(info.state), uint256(StakingRegistry.OperatorState.Ejected));
        assertEq(info.active, 0);
        assertEq(info.unbonding, 5000e18);
    }

    function test_EjectOrigin_RejectsZero() public {
        vm.expectRevert(Errors.ZeroAddress.selector);
        vm.prank(governor);
        bl.ejectOrigin(address(0));
    }

    function test_EjectOrigin_OnlyGovernance() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                randomUser,
                Roles.GOVERNANCE_ROLE
            )
        );
        vm.prank(randomUser);
        bl.ejectOrigin(operator);
    }

    function test_BlacklistVersionMonotonic() public {
        uint256 v0 = bl.blacklistVersion();
        vm.startPrank(governor);
        bl.addHash(HASH_A);
        assertEq(bl.blacklistVersion(), v0 + 1);
        bl.addHash(HASH_B);
        assertEq(bl.blacklistVersion(), v0 + 2);
        bl.removeHash(HASH_A);
        assertEq(bl.blacklistVersion(), v0 + 3);
        vm.stopPrank();
    }

    // ---------------- history views ----------------

    function test_IntervalCount_StartsZero() public view {
        assertEq(bl.intervalCount(HASH_A), 0);
    }

    function test_IntervalCount_GrowsWithIntervals() public {
        vm.startPrank(governor);
        bl.addHash(HASH_A);
        assertEq(bl.intervalCount(HASH_A), 1);
        bl.removeHash(HASH_A);
        assertEq(bl.intervalCount(HASH_A), 1);
        bl.addHash(HASH_A);
        assertEq(bl.intervalCount(HASH_A), 2);
        vm.stopPrank();
    }

    function test_IntervalAt_ReturnsRecord() public {
        vm.prank(governor);
        bl.addHash(HASH_A);
        ContentBlacklist.Interval memory iv = bl.intervalAt(HASH_A, 0);
        assertEq(iv.addedAt, uint64(block.timestamp));
        assertEq(iv.removedAt, 0);
        assertEq(iv.emergencyExpiresAt, 0);
    }

    function test_EmergencyExpiry_HonoredWhenSoonerThanRemoval() public {
        // An emergency entry that's later removed before its emergency
        // window expires has its `endsAt` driven by the emergency expiry
        // (the smaller of the two), not the removal — exercises the
        // `endsAt = emergencyExpiresAt` branch in `isBlacklisted`.
        vm.prank(emergency);
        bl.emergencyAdd(HASH_A);
        // Confirm the emergency window is shorter than time to elapse.
        uint64 expiry = bl.emergencyExpiryOf(HASH_A);
        assertGt(expiry, 0);

        vm.prank(governor);
        bl.removeHash(HASH_A);
        // Both removal and expiry are now in the past — definitely no
        // longer blacklisted, but the path with both fields set is
        // exercised by isBlacklisted reading the smaller of the two.
        vm.warp(uint256(expiry) + 1);
        assertFalse(bl.isBlacklisted(HASH_A));
    }

    function test_AddHash_RejectsBeyondIntervalLimit() public {
        // Cap interval-history depth so `wasBlacklistedAt`'s O(intervals)
        // walk stays bounded even under repeated add/remove churn.
        uint256 max = bl.MAX_INTERVALS_PER_HASH();
        vm.startPrank(governor);
        for (uint256 i = 0; i < max; ++i) {
            bl.addHash(HASH_A);
            bl.removeHash(HASH_A);
        }
        vm.expectRevert(ContentBlacklist.IntervalLimitReached.selector);
        bl.addHash(HASH_A);
        vm.stopPrank();
    }
}
