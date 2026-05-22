// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Token } from "../src/Token.sol";
import { VotingEscrow } from "../src/VotingEscrow.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";

contract VotingEscrowTest is Test {
    // Test code legitimately week-aligns (`(t / WEEK) * WEEK`) and compares
    // against `block.timestamp` throughout — these mirror the contract's
    // intentional patterns, so the lints are disabled for the test body.
    // forge-lint: disable-start(divide-before-multiply)
    // forge-lint: disable-start(block-timestamp)

    Token internal token;
    VotingEscrow internal ve;

    address internal admin = makeAddr("admin");
    address internal pauser = makeAddr("pauser");
    address internal alice = makeAddr("alice");
    address internal bob = makeAddr("bob");
    address internal carol = makeAddr("carol");

    uint256 internal constant WEEK = 7 days;
    uint256 internal constant MIN_LOCK = 1 weeks;
    uint256 internal constant MAX_LOCK = 4 * 365 days;

    function setUp() public {
        token = new Token(admin);
        ve = new VotingEscrow(IERC20(address(token)), admin, MIN_LOCK, MAX_LOCK);

        bytes32 pauserRole = ve.PAUSER_ROLE();
        vm.startPrank(admin);
        ve.grantRole(pauserRole, pauser);
        // Fund lockers.
        assertTrue(token.transfer(alice, 1_000_000e18));
        assertTrue(token.transfer(bob, 1_000_000e18));
        assertTrue(token.transfer(carol, 1_000_000e18));
        vm.stopPrank();

        // Warp to a clean week boundary so week-alignment is predictable.
        vm.warp((block.timestamp / WEEK + 100) * WEEK);
    }

    function _lock(address who, uint256 amount, uint256 duration) internal returns (uint256 end) {
        end = ((block.timestamp + duration) / WEEK) * WEEK;
        vm.startPrank(who);
        token.approve(address(ve), amount);
        ve.createLock(amount, block.timestamp + duration);
        vm.stopPrank();
    }

    /// @dev Mirror of the contract's ve-weight formula at a timestamp.
    function _expectedWeight(uint256 amount, uint256 end, uint256 ts) internal pure returns (uint256) {
        if (ts >= end) return 0;
        uint256 slope = amount / MAX_LOCK;
        return slope * (end - ts);
    }

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    function test_constructor_setsState() public view {
        assertEq(address(ve.token()), address(token));
        assertEq(ve.minLockDuration(), MIN_LOCK);
        assertEq(ve.maxLockDuration(), MAX_LOCK);
        assertTrue(ve.hasRole(ve.DEFAULT_ADMIN_ROLE(), admin));
    }

    function test_constructor_revertsZeroToken() public {
        vm.expectRevert(VotingEscrow.ZeroAddress.selector);
        new VotingEscrow(IERC20(address(0)), admin, MIN_LOCK, MAX_LOCK);
    }

    function test_constructor_revertsBadDurations() public {
        vm.expectRevert();
        new VotingEscrow(IERC20(address(token)), admin, 0, MAX_LOCK);
        vm.expectRevert();
        new VotingEscrow(IERC20(address(token)), admin, MAX_LOCK + 1, MAX_LOCK);
    }

    function test_constructor_revertsMaxLockExceedsWalkCap() public {
        // The checkpoint walk is capped at 255 week-steps; a maxLockDuration
        // beyond 255 weeks could outrun it and corrupt totalSupplyAt.
        uint256 tooLong = 256 weeks;
        vm.expectRevert();
        new VotingEscrow(IERC20(address(token)), admin, MIN_LOCK, tooLong);
        // 255 weeks is the boundary and is accepted.
        new VotingEscrow(IERC20(address(token)), admin, MIN_LOCK, 255 weeks);
    }

    // -----------------------------------------------------------------
    // createLock
    // -----------------------------------------------------------------

    function test_createLock_locksAndSetsWeight() public {
        uint256 amount = 100_000e18;
        uint256 end = _lock(alice, amount, MAX_LOCK);

        (uint256 lockedAmount, uint256 lockedEnd) = ve.locked(alice);
        assertEq(lockedAmount, amount);
        assertEq(lockedEnd, end);
        assertEq(token.balanceOf(address(ve)), amount);

        // Weight at creation ~= amount * remaining / maxLock (week-aligned).
        assertEq(ve.balanceOf(alice), _expectedWeight(amount, end, block.timestamp));
    }

    function test_createLock_weekAligned() public {
        uint256 end = _lock(alice, 1000e18, MAX_LOCK / 2 + 3 days);
        assertEq(end % WEEK, 0, "end is week-aligned");
    }

    function test_createLock_revertsZeroAmount() public {
        vm.prank(alice);
        vm.expectRevert(VotingEscrow.ZeroAmount.selector);
        ve.createLock(0, block.timestamp + MAX_LOCK);
    }

    function test_createLock_revertsExistingLock() public {
        _lock(alice, 1000e18, MAX_LOCK);
        vm.startPrank(alice);
        token.approve(address(ve), 1000e18);
        vm.expectRevert(VotingEscrow.ExistingLock.selector);
        ve.createLock(1000e18, block.timestamp + MAX_LOCK);
        vm.stopPrank();
    }

    function test_createLock_revertsBelowMinDuration() public {
        vm.startPrank(alice);
        token.approve(address(ve), 1000e18);
        vm.expectRevert();
        ve.createLock(1000e18, block.timestamp + 1 days); // < 1 week after week-align -> 0
        vm.stopPrank();
    }

    function test_createLock_revertsAboveMaxDuration() public {
        vm.startPrank(alice);
        token.approve(address(ve), 1000e18);
        vm.expectRevert();
        ve.createLock(1000e18, block.timestamp + MAX_LOCK + 2 weeks);
        vm.stopPrank();
    }

    // -----------------------------------------------------------------
    // Decay
    // -----------------------------------------------------------------

    function test_balanceOf_decaysLinearlyToZero() public {
        uint256 amount = 200_000e18;
        uint256 end = _lock(alice, amount, MAX_LOCK);
        uint256 start = block.timestamp;

        uint256 wStart = ve.balanceOf(alice);
        assertEq(wStart, _expectedWeight(amount, end, start));

        // Midway through the lock.
        uint256 mid = start + (end - start) / 2;
        vm.warp(mid);
        assertEq(ve.balanceOf(alice), _expectedWeight(amount, end, mid));
        assertApproxEqRel(ve.balanceOf(alice), wStart / 2, 0.01e18);

        // At expiry -> zero.
        vm.warp(end);
        assertEq(ve.balanceOf(alice), 0);

        // After expiry -> still zero.
        vm.warp(end + 10 weeks);
        assertEq(ve.balanceOf(alice), 0);
    }

    function test_balanceOfAt_historical() public {
        uint256 amount = 100_000e18;
        uint256 end = _lock(alice, amount, MAX_LOCK);
        uint256 t0 = block.timestamp;

        vm.warp(t0 + 50 weeks);
        // Past lookup returns the historical decayed weight.
        assertEq(ve.balanceOfAt(alice, t0), _expectedWeight(amount, end, t0));
        assertEq(ve.balanceOfAt(alice, t0 + 25 weeks), _expectedWeight(amount, end, t0 + 25 weeks));
    }

    function test_balanceOfAt_revertsFutureLookup() public {
        _lock(alice, 1000e18, MAX_LOCK);
        vm.expectRevert(abi.encodeWithSelector(VotingEscrow.FutureLookup.selector, block.timestamp + 1));
        ve.balanceOfAt(alice, block.timestamp + 1);
    }

    function test_balanceOf_zeroForNeverLocked() public view {
        assertEq(ve.balanceOf(bob), 0);
    }

    // -----------------------------------------------------------------
    // increaseAmount / increaseUnlockTime
    // -----------------------------------------------------------------

    function test_increaseAmount_raisesWeight() public {
        uint256 end = _lock(alice, 100_000e18, MAX_LOCK);
        uint256 before = ve.balanceOf(alice);

        vm.startPrank(alice);
        token.approve(address(ve), 100_000e18);
        ve.increaseAmount(100_000e18);
        vm.stopPrank();

        (uint256 amount,) = ve.locked(alice);
        assertEq(amount, 200_000e18);
        assertEq(ve.balanceOf(alice), _expectedWeight(200_000e18, end, block.timestamp));
        assertGt(ve.balanceOf(alice), before);
    }

    function test_increaseAmount_revertsNoLock() public {
        vm.prank(alice);
        vm.expectRevert(VotingEscrow.NoLock.selector);
        ve.increaseAmount(1000e18);
    }

    function test_increaseUnlockTime_extendsAndRaisesWeight() public {
        _lock(alice, 100_000e18, MAX_LOCK / 4);
        uint256 before = ve.balanceOf(alice);

        uint256 newEnd = ((block.timestamp + MAX_LOCK / 2) / WEEK) * WEEK;
        vm.prank(alice);
        ve.increaseUnlockTime(block.timestamp + MAX_LOCK / 2);

        (, uint256 end) = ve.locked(alice);
        assertEq(end, newEnd);
        assertGt(ve.balanceOf(alice), before, "longer lock means more weight");
    }

    function test_increaseUnlockTime_revertsNotInFuture() public {
        uint256 end = _lock(alice, 1000e18, MAX_LOCK / 2);
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(VotingEscrow.UnlockTimeNotInFuture.selector, end, end));
        ve.increaseUnlockTime(end);
    }

    function test_increaseUnlockTime_revertsAboveMax() public {
        _lock(alice, 1000e18, MAX_LOCK / 2);
        vm.prank(alice);
        vm.expectRevert();
        ve.increaseUnlockTime(block.timestamp + MAX_LOCK + 2 weeks);
    }

    // -----------------------------------------------------------------
    // withdraw
    // -----------------------------------------------------------------

    function test_withdraw_afterExpiry() public {
        uint256 amount = 100_000e18;
        uint256 end = _lock(alice, amount, MAX_LOCK / 4);
        uint256 balBefore = token.balanceOf(alice);

        vm.warp(end + 1);
        vm.prank(alice);
        ve.withdraw();

        assertEq(token.balanceOf(alice), balBefore + amount);
        (uint256 lockedAmount,) = ve.locked(alice);
        assertEq(lockedAmount, 0);
        assertEq(ve.balanceOf(alice), 0);
    }

    function test_withdraw_revertsBeforeExpiry() public {
        uint256 end = _lock(alice, 1000e18, MAX_LOCK / 4);
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(VotingEscrow.LockNotExpired.selector, end));
        ve.withdraw();
    }

    function test_withdraw_revertsNoLock() public {
        vm.prank(alice);
        vm.expectRevert(VotingEscrow.NoLock.selector);
        ve.withdraw();
    }

    // -----------------------------------------------------------------
    // totalSupply == sum(balanceOf) - the core checkpoint invariant
    // -----------------------------------------------------------------

    function test_totalSupply_equalsSumOfBalances_multipleLocks() public {
        uint256 end1 = _lock(alice, 300_000e18, MAX_LOCK);
        uint256 end2 = _lock(bob, 150_000e18, MAX_LOCK / 2);
        uint256 end3 = _lock(carol, 500_000e18, MAX_LOCK / 4);

        // Check the invariant at a sequence of timestamps, including past each
        // lock's expiry (exercises slopeChanges).
        uint256[] memory checkpoints = new uint256[](6);
        checkpoints[0] = block.timestamp;
        checkpoints[1] = end3 - 1 weeks;
        checkpoints[2] = end3 + 1; // carol expired
        checkpoints[3] = end2 + 1; // bob expired
        checkpoints[4] = end1 - 1 weeks;
        checkpoints[5] = end1 + 1; // all expired

        uint256 finalT = checkpoints[5];
        vm.warp(finalT);

        for (uint256 i = 0; i < checkpoints.length; i++) {
            uint256 ts = checkpoints[i];
            uint256 sum = ve.balanceOfAt(alice, ts) + ve.balanceOfAt(bob, ts) + ve.balanceOfAt(carol, ts);
            assertEq(ve.totalSupplyAt(ts), sum, "totalSupplyAt == sum of balanceOfAt");
        }

        assertEq(ve.totalSupplyAt(end1 + 1), 0, "all expired means zero total");
    }

    function test_totalSupply_currentMatchesSum() public {
        _lock(alice, 100_000e18, MAX_LOCK);
        _lock(bob, 200_000e18, MAX_LOCK / 2);
        vm.warp(block.timestamp + 30 weeks);
        assertEq(ve.totalSupply(), ve.balanceOf(alice) + ve.balanceOf(bob));
    }

    function test_totalSupplyAt_revertsFutureLookup() public {
        vm.expectRevert(abi.encodeWithSelector(VotingEscrow.FutureLookup.selector, block.timestamp + 1));
        ve.totalSupplyAt(block.timestamp + 1);
    }

    function test_totalSupplyAt_zeroBeforeGenesis() public {
        _lock(alice, 100_000e18, MAX_LOCK);
        (,, uint256 genesisTs) = ve.pointHistory(0);
        // A timestamp before the genesis checkpoint returns 0, not an
        // arithmetic-underflow panic (the regression gemini flagged).
        assertEq(ve.totalSupplyAt(genesisTs - 1), 0);
    }

    // -----------------------------------------------------------------
    // Pause
    // -----------------------------------------------------------------

    function test_pause_blocksCreateLock() public {
        vm.prank(pauser);
        ve.pause();
        vm.startPrank(alice);
        token.approve(address(ve), 1000e18);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        ve.createLock(1000e18, block.timestamp + MAX_LOCK);
        vm.stopPrank();
    }

    // -----------------------------------------------------------------
    // Fuzz
    // -----------------------------------------------------------------

    function testFuzz_balanceOf_neverExceedsAmount_decaysMonotonically(uint256 amount, uint256 duration, uint256 dt)
        public
    {
        amount = bound(amount, 1e18, 1_000_000e18);
        duration = bound(duration, MIN_LOCK, MAX_LOCK);
        uint256 end = _lock(alice, amount, duration);
        if (end <= block.timestamp) return; // week-align rounded below min; skip

        uint256 w0 = ve.balanceOf(alice);
        assertLe(w0, amount, "weight never exceeds locked amount");

        dt = bound(dt, 0, (end - block.timestamp));
        vm.warp(block.timestamp + dt);
        uint256 w1 = ve.balanceOf(alice);
        assertLe(w1, w0, "weight decays monotonically");
    }

    function testFuzz_totalSupplyMatchesSum(uint256 a1, uint256 a2, uint256 d1, uint256 d2, uint256 warpBy) public {
        a1 = bound(a1, 1e18, 500_000e18);
        a2 = bound(a2, 1e18, 500_000e18);
        d1 = bound(d1, MIN_LOCK, MAX_LOCK);
        d2 = bound(d2, MIN_LOCK, MAX_LOCK);

        _lock(alice, a1, d1);
        _lock(bob, a2, d2);

        warpBy = bound(warpBy, 0, MAX_LOCK + 4 weeks);
        vm.warp(block.timestamp + warpBy);

        assertEq(ve.totalSupply(), ve.balanceOf(alice) + ve.balanceOf(bob));
    }

    // -----------------------------------------------------------------
    // Non-transferability - no ERC20 surface
    // -----------------------------------------------------------------

    function test_noTransferSurface() public {
        _lock(alice, 1000e18, MAX_LOCK);
        // VotingEscrow exposes no transfer / approve / transferFrom.
        (bool ok1,) = address(ve).call(abi.encodeWithSignature("transfer(address,uint256)", bob, 1));
        (bool ok2,) = address(ve).call(abi.encodeWithSignature("approve(address,uint256)", bob, 1));
        (bool ok3,) = address(ve).call(abi.encodeWithSignature("transferFrom(address,address,uint256)", alice, bob, 1));
        assertFalse(ok1);
        assertFalse(ok2);
        assertFalse(ok3);
    }

    // forge-lint: disable-end(divide-before-multiply)
    // forge-lint: disable-end(block-timestamp)
}
