// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";

import { ReentrancyGuard } from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";

import { TOKEN } from "../src/TOKEN.sol";
import { StakingRegistry } from "../src/StakingRegistry.sol";
import { IStakingRegistry } from "../src/interfaces/IStakingRegistry.sol";
import { Errors } from "../src/libraries/Errors.sol";
import { Roles } from "../src/libraries/Roles.sol";

import { ReentrantERC20 } from "./mocks/ReentrantERC20.sol";

contract StakingRegistryTest is Test {
    TOKEN internal token;
    StakingRegistry internal reg;

    address internal admin = makeAddr("admin");
    address internal slasher = makeAddr("slasher");
    address internal blacklister = makeAddr("blacklister");

    uint256 internal operatorPk = 0xB0B;
    address internal operator;
    uint256 internal otherPk = 0xCA11;
    address internal other;

    uint256 internal constant MIN_STAKE = 1000e18;
    uint64 internal constant UNBONDING = 7 days;
    uint64 internal constant RESET_PERIOD = 90 days;

    function setUp() public {
        operator = vm.addr(operatorPk);
        other = vm.addr(otherPk);

        token = new TOKEN(address(this), 10_000_000e18, address(this));
        reg = new StakingRegistry(token, MIN_STAKE, UNBONDING, RESET_PERIOD, admin);

        vm.startPrank(admin);
        reg.grantRole(Roles.SLASH_ROLE, slasher);
        reg.grantRole(Roles.BLACKLIST_ROLE, blacklister);
        reg.unpause();
        vm.stopPrank();

        token.transfer(operator, 1_000_000e18);
        token.transfer(other, 1_000_000e18);
        vm.prank(operator);
        token.approve(address(reg), type(uint256).max);
        vm.prank(other);
        token.approve(address(reg), type(uint256).max);
    }

    // ---------- constructor ----------

    function test_Constructor_StoresConfig() public view {
        assertEq(address(reg.TOKEN_CONTRACT()), address(token));
        assertEq(reg.minStake(), MIN_STAKE);
        assertEq(reg.unbondingPeriod(), UNBONDING);
        assertTrue(reg.hasRole(reg.DEFAULT_ADMIN_ROLE(), admin));
    }

    function test_Constructor_RevertsOnZeroAddresses() public {
        vm.expectRevert(Errors.ZeroAddress.selector);
        new StakingRegistry(TOKEN(address(0)), MIN_STAKE, UNBONDING, RESET_PERIOD, admin);

        vm.expectRevert(Errors.ZeroAddress.selector);
        new StakingRegistry(token, MIN_STAKE, UNBONDING, RESET_PERIOD, address(0));
    }

    function test_Constructor_EnforcesBounds() public {
        vm.expectRevert(Errors.OutOfBounds.selector);
        new StakingRegistry(token, 99e18, UNBONDING, RESET_PERIOD, admin);

        vm.expectRevert(Errors.OutOfBounds.selector);
        new StakingRegistry(token, 100_001e18, UNBONDING, RESET_PERIOD, admin);

        vm.expectRevert(Errors.OutOfBounds.selector);
        new StakingRegistry(token, MIN_STAKE, 2 days, RESET_PERIOD, admin);

        vm.expectRevert(Errors.OutOfBounds.selector);
        new StakingRegistry(token, MIN_STAKE, 31 days, RESET_PERIOD, admin);
    }

    function test_Constructor_StartsPaused() public {
        StakingRegistry reg2 = new StakingRegistry(token, MIN_STAKE, UNBONDING, RESET_PERIOD, admin);
        assertTrue(reg2.paused());
    }

    // ---------- stake / unstake / withdraw ----------

    function test_Stake_TopsUpSlot() public {
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        assertEq(reg.getStakeInfo(operator).active, MIN_STAKE);

        vm.prank(operator);
        reg.stake(MIN_STAKE);
        assertEq(reg.getStakeInfo(operator).active, 2 * MIN_STAKE);
    }

    function test_Stake_RevertsOnZero() public {
        vm.expectRevert(Errors.ZeroAmount.selector);
        vm.prank(operator);
        reg.stake(0);
    }

    function test_Stake_RevertsAfterEjection() public {
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        vm.prank(blacklister);
        reg.ejectNode(operator);

        vm.expectRevert(StakingRegistry.Ejected.selector);
        vm.prank(operator);
        reg.stake(MIN_STAKE);
    }

    function test_Unstake_RejectsWhenQueueFull() public {
        vm.prank(operator);
        reg.stake(100 * MIN_STAKE);
        for (uint256 i = 0; i < reg.MAX_UNBONDING_ENTRIES(); ++i) {
            vm.prank(operator);
            reg.unstake(1);
        }
        vm.expectRevert(StakingRegistry.UnbondingQueueFull.selector);
        vm.prank(operator);
        reg.unstake(1);
    }

    function test_Unstake_InsufficientActiveReverts() public {
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        vm.expectRevert(StakingRegistry.InsufficientActiveStake.selector);
        vm.prank(operator);
        reg.unstake(MIN_STAKE + 1);
    }

    function test_Unstake_Withdraw_HappyPath() public {
        vm.prank(operator);
        reg.stake(10 * MIN_STAKE);
        vm.prank(operator);
        reg.unstake(3 * MIN_STAKE);

        // Not yet matured.
        vm.expectRevert(StakingRegistry.NothingToWithdraw.selector);
        vm.prank(operator);
        reg.withdrawUnbonded();

        vm.warp(block.timestamp + UNBONDING + 1);
        uint256 before = token.balanceOf(operator);
        vm.prank(operator);
        reg.withdrawUnbonded();
        assertEq(token.balanceOf(operator) - before, 3 * MIN_STAKE);
        assertEq(reg.getStakeInfo(operator).unbonding, 0);
        assertEq(reg.getStakeInfo(operator).active, 7 * MIN_STAKE);
    }

    function test_Unstake_MultipleEntries_PartialMaturity() public {
        vm.prank(operator);
        reg.stake(10 * MIN_STAKE);
        vm.prank(operator);
        reg.unstake(2 * MIN_STAKE);
        vm.warp(block.timestamp + 3 days);
        vm.prank(operator);
        reg.unstake(3 * MIN_STAKE);

        // advance so only first matures
        vm.warp(block.timestamp + (UNBONDING - 3 days) + 1);
        uint256 before = token.balanceOf(operator);
        vm.prank(operator);
        reg.withdrawUnbonded();
        assertEq(token.balanceOf(operator) - before, 2 * MIN_STAKE);
        assertEq(reg.getStakeInfo(operator).unbonding, 3 * MIN_STAKE);
        assertEq(reg.unbondingQueueLength(operator), 1);

        vm.warp(block.timestamp + 3 days + 1);
        vm.prank(operator);
        reg.withdrawUnbonded();
        assertEq(reg.getStakeInfo(operator).unbonding, 0);
        assertEq(reg.unbondingQueueLength(operator), 0);
    }

    // ---------- client stake ----------

    function test_ClientStake_InstantWithdraw() public {
        vm.prank(other);
        reg.clientStake(500e18);
        assertEq(reg.clientStakeOf(other), 500e18);
        vm.prank(other);
        reg.clientUnstake(200e18);
        assertEq(reg.clientStakeOf(other), 300e18);
    }

    function test_ClientUnstake_Insufficient() public {
        vm.expectRevert(StakingRegistry.InsufficientClientStake.selector);
        vm.prank(other);
        reg.clientUnstake(1);
    }

    // ---------- getStakeMultiple ----------

    function test_GetStakeMultiple() public {
        vm.prank(operator);
        reg.stake(MIN_STAKE * 15);
        assertEq(reg.getStakeMultiple(operator), 15);

        vm.prank(operator);
        reg.unstake(MIN_STAKE * 5);
        assertEq(reg.getStakeMultiple(operator), 10); // active drops to 10x
    }

    // ---------- slash ----------

    function test_Slash_OnlyRole() public {
        vm.prank(operator);
        reg.stake(MIN_STAKE);

        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector, other, Roles.SLASH_ROLE
            )
        );
        vm.prank(other);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
    }

    function test_Slash_Tier0_SplitsRewardAndBurn() public {
        vm.prank(operator);
        reg.stake(10 * MIN_STAKE); // 10,000 TOKEN

        // First offense → tier 0, 5% slash.
        uint256 expectedSlash = (10 * MIN_STAKE * 500) / 10_000;
        uint256 expectedReward = expectedSlash / 2;
        uint256 expectedBurn = expectedSlash - expectedReward;

        uint256 slasherBefore = token.balanceOf(slasher);
        uint256 supplyBefore = token.totalSupply();
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.RateManipulation);

        assertEq(reg.getStakeInfo(operator).active, 10 * MIN_STAKE - expectedSlash);
        assertEq(token.balanceOf(slasher) - slasherBefore, expectedReward);
        // Burn is a real ERC20Burnable.burn() — totalSupply shrinks.
        assertEq(supplyBefore - token.totalSupply(), expectedBurn);
    }

    function test_Slash_SpillsIntoUnbonding() public {
        vm.prank(operator);
        reg.stake(10 * MIN_STAKE);
        vm.prank(operator);
        // Leave a tiny sliver active so the first slash has to spill into
        // unbonding immediately. With tier 0 = 5% on 10x slashable, the
        // slash amount is 0.5x — larger than 0.05x active, so the
        // remainder pulls from the unbonding queue.
        reg.unstake(9950e18); // 9.95x unbonding, 0.05x active
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.Corruption);

        // 5% of 10x = 0.5x = 500e18. Active had 0.05x = 50e18 → fully consumed.
        // Remaining 0.45x = 450e18 spilled into unbonding (9950 - 450 = 9500).
        assertEq(reg.getStakeInfo(operator).active, 0);
        assertEq(reg.getStakeInfo(operator).unbonding, 9500e18);
    }

    function test_Slash_RevertsWhenNotStaked() public {
        vm.expectRevert(StakingRegistry.NodeNotStaked.selector);
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
    }

    // ---------- escalating slash schedule (ADR 004) ----------

    function test_Slash_TierEscalates_FromZeroToOneToTwo() public {
        vm.prank(operator);
        reg.stake(100 * MIN_STAKE);

        // First offense → 5%. effectiveTier was 0 → bumps to 1.
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        assertEq(reg.currentTier(operator), 1);
        assertEq(reg.getStakeInfo(operator).lifetimeOffenseCount, 1);

        // Second offense (still within reset period) → 15%. Bumps to 2.
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        assertEq(reg.currentTier(operator), 2);
        assertEq(reg.getStakeInfo(operator).lifetimeOffenseCount, 2);

        // Third offense → 50%, tier already 2 stays at 2 (capped).
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        assertEq(reg.currentTier(operator), 2);
        assertEq(reg.getStakeInfo(operator).lifetimeOffenseCount, 3);
    }

    function test_Slash_TierDecaysAfterResetPeriod() public {
        vm.prank(operator);
        reg.stake(100 * MIN_STAKE);

        // Bring storedTier up to 2.
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        assertEq(reg.currentTier(operator), 2);

        // Lifetime count = 2 → effective period is 2× the base.
        uint64 effective = reg.effectiveResetPeriod(operator);
        assertEq(effective, 2 * RESET_PERIOD);

        // After exactly one effective period, tier drops to 1.
        vm.warp(block.timestamp + effective);
        assertEq(reg.currentTier(operator), 1);

        // After another period, tier drops to 0.
        vm.warp(block.timestamp + effective);
        assertEq(reg.currentTier(operator), 0);
    }

    function test_Slash_LifetimeCountUsesIncreasingResetPeriod() public {
        vm.prank(operator);
        reg.stake(100 * MIN_STAKE);

        // 0 lifetime offenses → 1× base period.
        assertEq(reg.effectiveResetPeriod(operator), RESET_PERIOD);

        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        // 1 → 1× (max(1,1)-1 = 0, mult = 1).
        assertEq(reg.effectiveResetPeriod(operator), RESET_PERIOD);

        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        // 2 → 2× base.
        assertEq(reg.effectiveResetPeriod(operator), 2 * RESET_PERIOD);

        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        // 3 → 4× cap.
        assertEq(reg.effectiveResetPeriod(operator), 4 * RESET_PERIOD);

        // Further offenses stay at the 4× cap.
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        assertEq(reg.effectiveResetPeriod(operator), 4 * RESET_PERIOD);
    }

    function test_SetBaseResetPeriod_Bounds() public {
        vm.prank(admin);
        reg.setBaseResetPeriod(180 days);
        assertEq(reg.baseResetPeriod(), 180 days);

        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        reg.setBaseResetPeriod(29 days);

        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        reg.setBaseResetPeriod(366 days);
    }

    // ---------- ejectNode ----------

    function test_Eject_MovesActiveToUnbonding() public {
        vm.prank(operator);
        reg.stake(5 * MIN_STAKE);
        vm.prank(blacklister);
        reg.ejectNode(operator);

        StakingRegistry.StakeInfo memory s = reg.getStakeInfo(operator);
        assertEq(uint256(s.state), uint256(StakingRegistry.OperatorState.Ejected));
        assertEq(s.active, 0);
        assertEq(s.unbonding, 5 * MIN_STAKE);
    }

    function test_Eject_FoldsIntoLastEntryWhenQueueFull() public {
        vm.prank(operator);
        reg.stake(100 * MIN_STAKE);
        uint256 cap = reg.MAX_UNBONDING_ENTRIES();
        for (uint256 i = 0; i < cap; ++i) {
            vm.prank(operator);
            reg.unstake(1);
        }
        uint256 activeBefore = reg.getStakeInfo(operator).active;
        assertEq(reg.unbondingQueueLength(operator), cap);

        StakingRegistry.UnbondRequest memory tailBefore = reg.unbondingEntry(operator, cap - 1);

        vm.warp(block.timestamp + 1); // so fold would bump unlock forward
        vm.prank(blacklister);
        reg.ejectNode(operator);

        // Queue length must NOT grow past the cap.
        assertEq(reg.unbondingQueueLength(operator), cap);
        StakingRegistry.StakeInfo memory info = reg.getStakeInfo(operator);
        assertEq(uint256(info.state), uint256(StakingRegistry.OperatorState.Ejected));
        assertEq(info.active, 0);
        assertEq(info.unbonding, activeBefore + cap);

        // Last entry absorbed the folded amount and took the later unlock.
        StakingRegistry.UnbondRequest memory tailAfter = reg.unbondingEntry(operator, cap - 1);
        assertEq(uint256(tailAfter.amount), uint256(tailBefore.amount) + activeBefore);
        assertGt(tailAfter.unlockTime, tailBefore.unlockTime);
    }

    function test_Slash_AutoEjectsBelowHalfMinStake() public {
        // ADR 004 §Auto-Ejection: a slash that drops the slashable pool
        // below 50% of `minStake` MUST auto-eject the operator.
        // Tier 2 (50% slash) trips the threshold reliably for any node
        // staked at the minimum: starting at 1.0x, slash leaves 0.5x —
        // not strictly less than 0.5x, so we need 1× minus a tiny bit
        // less than half, i.e. drop the pool first. Easiest path:
        // build to tier 2 and slash, then verify ejection.
        bytes32 nodeId = keccak256("auto-eject");
        bytes memory sigOp = _signBind(operatorPk, nodeId, 0);
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        vm.prank(operator);
        reg.registerNode(nodeId, sigOp);

        // First slash → tier 0 (5%): 950 TOKEN remaining. Above threshold.
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        assertTrue(reg.getStakeInfo(operator).state == StakingRegistry.OperatorState.Registered);

        // Second slash → tier 1 (15%): 950 * 0.85 = 807.5 TOKEN. Above threshold.
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        assertTrue(reg.getStakeInfo(operator).state == StakingRegistry.OperatorState.Registered);

        // Third slash → tier 2 (50%): 807.5 * 0.5 ≈ 403.75 TOKEN < 500 = 0.5x minStake.
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);

        // Auto-ejected: removed from active set, state == Ejected.
        assertEq(reg.getActiveNodeCount(), 0);
        assertTrue(reg.getStakeInfo(operator).state == StakingRegistry.OperatorState.Ejected);
    }

    function test_Slash_DoesNotAutoEjectAboveThreshold() public {
        // First-offense 5% slash on a healthy 1x stake leaves 0.95x — well
        // above 0.5x, so no ejection.
        bytes32 nodeId = keccak256("healthy");
        bytes memory sigOp = _signBind(operatorPk, nodeId, 0);
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        vm.prank(operator);
        reg.registerNode(nodeId, sigOp);

        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);

        assertEq(reg.getActiveNodeCount(), 1);
        assertTrue(reg.getStakeInfo(operator).state == StakingRegistry.OperatorState.Registered);
    }

    function test_Slash_RevertsOnDustStake() public {
        // `stake()` has no lower-bound check (only `registerNode` enforces
        // `minStake`), so we can deposit dust directly. 9 wei of slashable
        // yields slashAmount = 9 * 1000 / 10_000 = 0 under the PoC schedule,
        // which must now revert rather than silently zero the reward.
        vm.prank(operator);
        reg.stake(9);
        vm.expectRevert(StakingRegistry.StakeTooSmallToSlash.selector);
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
    }

    function test_Eject_NodeIdStaysBoundPermanently() public {
        // After ejection, the ejected operator's nodeId remains registered
        // to them so a DIFFERENT EVM address cannot re-use the same iroh
        // identity. Makes ejection a network-identity ban.
        bytes32 nodeId = keccak256("ironode/banned");
        bytes memory sigOp = _signBind(operatorPk, nodeId, 0);
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        vm.prank(operator);
        reg.registerNode(nodeId, sigOp);
        vm.prank(blacklister);
        reg.ejectNode(operator);

        // The ejected address itself cannot re-register.
        assertEq(reg.nodeIdToOperator(nodeId), operator);

        // A fresh operator trying to grab the same nodeId is rejected.
        vm.prank(other);
        reg.stake(MIN_STAKE);
        bytes memory sigOther = _signBind(otherPk, nodeId, 0);
        vm.expectRevert(StakingRegistry.NodeIdTaken.selector);
        vm.prank(other);
        reg.registerNode(nodeId, sigOther);
    }

    function test_Eject_IsIdempotent() public {
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        vm.prank(blacklister);
        reg.ejectNode(operator);
        vm.prank(blacklister);
        reg.ejectNode(operator); // no-op, must not revert
    }

    function test_Eject_OnlyRole() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                other,
                Roles.BLACKLIST_ROLE
            )
        );
        vm.prank(other);
        reg.ejectNode(operator);
    }

    // ---------- registerNode ----------

    function test_RegisterNode_HappyPath() public {
        bytes32 nodeId = keccak256("ironode/1");
        bytes memory sig = _signBind(operatorPk, nodeId, 0);
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        vm.prank(operator);
        reg.registerNode(nodeId, sig);

        StakingRegistry.StakeInfo memory s = reg.getStakeInfo(operator);
        assertEq(uint256(s.state), uint256(StakingRegistry.OperatorState.Registered));
        assertEq(s.nodeId, nodeId);
        assertEq(s.bindingNonce, 1);
        assertEq(reg.nodeIdToOperator(nodeId), operator);
    }

    function test_RegisterNode_RejectsBelowMinStake() public {
        bytes32 nodeId = keccak256("x");
        bytes memory sig = _signBind(operatorPk, nodeId, 0);
        vm.prank(operator);
        reg.stake(MIN_STAKE - 1);
        vm.expectRevert(StakingRegistry.StakeBelowMinimum.selector);
        vm.prank(operator);
        reg.registerNode(nodeId, sig);
    }

    function test_RegisterNode_RejectsDoubleRegister() public {
        bytes32 nodeId = keccak256("x");
        bytes32 nodeId2 = keccak256("y");
        bytes memory sig0 = _signBind(operatorPk, nodeId, 0);
        bytes memory sig1 = _signBind(operatorPk, nodeId2, 1);

        vm.prank(operator);
        reg.stake(MIN_STAKE);
        vm.prank(operator);
        reg.registerNode(nodeId, sig0);

        vm.expectRevert(StakingRegistry.AlreadyRegistered.selector);
        vm.prank(operator);
        reg.registerNode(nodeId2, sig1);
    }

    function test_RegisterNode_RejectsTakenNodeId() public {
        bytes32 nodeId = keccak256("x");
        bytes memory sigOp = _signBind(operatorPk, nodeId, 0);
        bytes memory sigOther = _signBind(otherPk, nodeId, 0);

        vm.prank(operator);
        reg.stake(MIN_STAKE);
        vm.prank(operator);
        reg.registerNode(nodeId, sigOp);

        vm.prank(other);
        reg.stake(MIN_STAKE);
        vm.expectRevert(StakingRegistry.NodeIdTaken.selector);
        vm.prank(other);
        reg.registerNode(nodeId, sigOther);
    }

    function test_RegisterNode_RejectsWrongSigner() public {
        bytes32 nodeId = keccak256("x");
        bytes memory sig = _signBind(otherPk, nodeId, 0); // wrong key
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        vm.expectRevert(Errors.InvalidSignature.selector);
        vm.prank(operator);
        reg.registerNode(nodeId, sig);
    }

    function test_RegisterNode_RejectsEjected() public {
        bytes32 nodeId = keccak256("x");
        bytes memory sig = _signBind(operatorPk, nodeId, 0);
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        vm.prank(blacklister);
        reg.ejectNode(operator);
        vm.expectRevert(StakingRegistry.Ejected.selector);
        vm.prank(operator);
        reg.registerNode(nodeId, sig);
    }

    // ---------- pause ----------

    function test_Pause_BlocksMutations() public {
        vm.prank(admin);
        reg.pause();

        vm.expectRevert(Pausable.EnforcedPause.selector);
        vm.prank(operator);
        reg.stake(MIN_STAKE);
    }

    // ---------- governance params ----------

    function test_SetMinStake_RespectsBounds() public {
        vm.prank(admin);
        reg.setMinStake(50_000e18);
        assertEq(reg.minStake(), 50_000e18);

        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        reg.setMinStake(99e18);
    }

    function test_SetUnbondingPeriod_RespectsBounds() public {
        vm.prank(admin);
        reg.setUnbondingPeriod(14 days);
        assertEq(reg.unbondingPeriod(), 14 days);

        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        reg.setUnbondingPeriod(1 days);
    }

    // ---------- multi-entry slash spill ----------

    function test_Slash_MultiEntry_SpillAcrossQueue() public {
        // Stake 20× MIN_STAKE, queue four 5× entries (full unstake), so
        // active is zero and any slash must consume from the queue.
        vm.prank(operator);
        reg.stake(20 * MIN_STAKE);
        for (uint256 i = 0; i < 4; ++i) {
            vm.prank(operator);
            reg.unstake(5 * MIN_STAKE);
        }
        assertEq(reg.unbondingQueueLength(operator), 4);
        assertEq(reg.getStakeInfo(operator).active, 0);

        // First offense, tier 0 = 5%. slashable = 20×, slashAmount = 1×.
        // Entry 0 has 5×; partial-consume by 1× leaves 4×, queue depth
        // unchanged.
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        assertEq(reg.unbondingQueueLength(operator), 4);
        StakingRegistry.UnbondRequest memory e0 = reg.unbondingEntry(operator, 0);
        assertEq(uint256(e0.amount), 4 * MIN_STAKE);
        assertEq(reg.getStakeInfo(operator).unbonding, 19 * MIN_STAKE);

        // Second offense within reset period → tier 1 = 15%.
        // slashable = 19×, slashAmount = 19 * 1500 / 10_000 = 2.85×.
        // entry 0 (4×) absorbs the full 2.85×, leaving 1.15× in entry 0
        // and the remaining three entries untouched.
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        e0 = reg.unbondingEntry(operator, 0);
        // Be careful with the arithmetic: 4*MIN_STAKE - 19*MIN_STAKE*1500/10_000.
        uint256 secondSlash = (19 * MIN_STAKE * 1500) / 10_000;
        assertEq(uint256(e0.amount), 4 * MIN_STAKE - secondSlash);
    }

    // ---------- reentrancy ----------

    function test_Reentrancy_BlocksWithdrawUnbonded() public {
        ReentrantERC20 bad = new ReentrantERC20();
        StakingRegistry reg2 = new StakingRegistry(bad, MIN_STAKE, UNBONDING, RESET_PERIOD, admin);

        vm.prank(admin);
        reg2.unpause();

        bad.transfer(operator, 100_000e18);
        vm.prank(operator);
        bad.approve(address(reg2), type(uint256).max);
        vm.prank(operator);
        reg2.stake(10 * MIN_STAKE);
        vm.prank(operator);
        reg2.unstake(MIN_STAKE);
        vm.warp(block.timestamp + UNBONDING + 1);

        // Arm the token so the next transfer back to `operator` re-enters
        // `withdrawUnbonded`. The nonReentrant guard must trip.
        bytes memory payload = abi.encodeWithSelector(reg2.withdrawUnbonded.selector);
        bad.armAttack(address(reg2), payload);

        vm.expectRevert(ReentrancyGuard.ReentrancyGuardReentrantCall.selector);
        vm.prank(operator);
        reg2.withdrawUnbonded();
    }

    // ---------- fuzz ----------

    function testFuzz_StakeUnstakeWithdrawRoundtrip(
        uint256 a,
        uint256 b
    ) public {
        a = bound(a, 1, 100_000e18);
        b = bound(b, 1, a);
        vm.prank(operator);
        reg.stake(a);
        vm.prank(operator);
        reg.unstake(b);
        vm.warp(block.timestamp + UNBONDING + 1);
        uint256 before = token.balanceOf(operator);
        vm.prank(operator);
        reg.withdrawUnbonded();
        assertEq(token.balanceOf(operator) - before, b);
        assertEq(reg.getStakeInfo(operator).active, a - b);
    }

    // ---------- helpers ----------

    function _signBind(
        uint256 pk,
        bytes32 nodeId,
        uint64 nonce
    ) internal view returns (bytes memory) {
        bytes32 structHash = keccak256(abi.encode(reg.BIND_NODE_TYPEHASH(), nodeId, nonce));
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", reg.domainSeparator(), structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(pk, digest);
        return abi.encodePacked(r, s, v);
    }

    // ---------- off-chain read API (ADR 016 §3) ----------

    function test_GetActiveNodeCount_StartsZero() public view {
        assertEq(reg.getActiveNodeCount(), 0);
    }

    function test_GetActiveNodes_EmptyWhenNoneRegistered() public view {
        IStakingRegistry.NodeInfo[] memory nodes = reg.getActiveNodes(0, 100);
        assertEq(nodes.length, 0);
    }

    function test_RegisterNode_AddsToActiveSet() public {
        bytes32 nodeId = keccak256("op-node");
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        bytes memory sig = _signBind(operatorPk, nodeId, 0);
        vm.prank(operator);
        reg.registerNode(nodeId, sig);

        assertEq(reg.getActiveNodeCount(), 1);
        IStakingRegistry.NodeInfo[] memory nodes = reg.getActiveNodes(0, 100);
        assertEq(nodes.length, 1);
        assertEq(nodes[0].operator, operator);
        assertEq(nodes[0].nodeId, nodeId);
        assertEq(nodes[0].lastSettlementAt, 0);
    }

    function test_EjectNode_RemovesFromActiveSet() public {
        bytes32 nodeId = keccak256("op-node");
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        bytes memory sig = _signBind(operatorPk, nodeId, 0);
        vm.prank(operator);
        reg.registerNode(nodeId, sig);

        vm.prank(blacklister);
        reg.ejectNode(operator);

        assertEq(reg.getActiveNodeCount(), 0);
        IStakingRegistry.NodeInfo[] memory nodes = reg.getActiveNodes(0, 100);
        assertEq(nodes.length, 0);
    }

    function test_GetActiveNodes_PaginationCoversFullSet() public {
        // Register three operators and verify pagination covers all of them.
        address[] memory ops = new address[](3);
        uint256[] memory pks = new uint256[](3);
        ops[0] = operator;
        pks[0] = operatorPk;
        ops[1] = other;
        pks[1] = otherPk;
        // Third operator funded inline.
        uint256 thirdPk = 0xDEADBEEF;
        address third = vm.addr(thirdPk);
        token.transfer(third, 1_000_000e18);
        vm.prank(third);
        token.approve(address(reg), type(uint256).max);
        ops[2] = third;
        pks[2] = thirdPk;

        for (uint256 i = 0; i < 3; ++i) {
            vm.prank(ops[i]);
            reg.stake(MIN_STAKE);
            bytes32 nid = keccak256(abi.encode("nid", i));
            bytes memory sig = _signBind(pks[i], nid, 0);
            vm.prank(ops[i]);
            reg.registerNode(nid, sig);
        }

        assertEq(reg.getActiveNodeCount(), 3);

        // First page of size 2.
        IStakingRegistry.NodeInfo[] memory page0 = reg.getActiveNodes(0, 2);
        assertEq(page0.length, 2);

        // Second page picks up the remainder; partial page is OK.
        IStakingRegistry.NodeInfo[] memory page1 = reg.getActiveNodes(2, 2);
        assertEq(page1.length, 1);

        // Offset past the end yields empty.
        IStakingRegistry.NodeInfo[] memory pageEnd = reg.getActiveNodes(3, 100);
        assertEq(pageEnd.length, 0);
    }

    function test_GetFirstRegisteredAt_ZeroBeforeRegister() public view {
        assertEq(reg.getFirstRegisteredAt(operator), 0);
    }

    function test_GetFirstRegisteredAt_StableAcrossReRegister() public {
        bytes32 nodeId = keccak256("op-node");
        vm.warp(1_700_000_000);
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        bytes memory sig0 = _signBind(operatorPk, nodeId, 0);
        vm.prank(operator);
        reg.registerNode(nodeId, sig0);

        uint256 firstTs = reg.getFirstRegisteredAt(operator);
        assertEq(firstTs, 1_700_000_000);

        // Eject and re-register from a *different* address (the original
        // operator is permanently banned). Re-registration of the same
        // operator address is impossible by design (`Ejected`), so we only
        // verify that the first-registered timestamp does not move when
        // *another* operator joins.
        vm.prank(blacklister);
        reg.ejectNode(operator);

        vm.warp(1_700_001_000);
        vm.prank(other);
        reg.stake(MIN_STAKE);
        bytes32 otherNode = keccak256("other-node");
        bytes memory sigOther = _signBind(otherPk, otherNode, 0);
        vm.prank(other);
        reg.registerNode(otherNode, sigOther);

        // Original operator's timestamp is preserved.
        assertEq(reg.getFirstRegisteredAt(operator), firstTs);
        // New operator gets the current timestamp.
        assertEq(reg.getFirstRegisteredAt(other), 1_700_001_000);
    }

    // ---------- recordSettlement / settlement-weighted ranking ----------

    function test_RecordSettlement_RequiresRole() public {
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector,
                address(this),
                Roles.SETTLEMENT_REPORTER_ROLE
            )
        );
        reg.recordSettlement(operator);
    }

    function test_RecordSettlement_StampsTimestamp() public {
        address reporter = makeAddr("reporter");
        vm.prank(admin);
        reg.grantRole(Roles.SETTLEMENT_REPORTER_ROLE, reporter);

        vm.warp(1_700_000_500);
        vm.prank(reporter);
        reg.recordSettlement(operator);

        assertEq(reg.lastSettlementAt(operator), 1_700_000_500);
    }

    function test_RecordSettlement_FlowsIntoNodeInfo() public {
        // Register an operator, record a settlement, verify it appears in
        // the NodeInfo tuple returned by getActiveNodes.
        bytes32 nodeId = keccak256("op-node");
        vm.prank(operator);
        reg.stake(MIN_STAKE);
        bytes memory sig = _signBind(operatorPk, nodeId, 0);
        vm.prank(operator);
        reg.registerNode(nodeId, sig);

        address reporter = makeAddr("reporter");
        vm.prank(admin);
        reg.grantRole(Roles.SETTLEMENT_REPORTER_ROLE, reporter);

        vm.warp(1_700_000_900);
        vm.prank(reporter);
        reg.recordSettlement(operator);

        IStakingRegistry.NodeInfo[] memory nodes = reg.getActiveNodes(0, 100);
        assertEq(nodes.length, 1);
        assertEq(nodes[0].lastSettlementAt, 1_700_000_900);
    }
}
