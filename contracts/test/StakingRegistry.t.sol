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

    function setUp() public {
        operator = vm.addr(operatorPk);
        other = vm.addr(otherPk);

        token = new TOKEN(address(this), 10_000_000e18, address(this));
        reg = new StakingRegistry(token, MIN_STAKE, UNBONDING, admin);

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
        new StakingRegistry(TOKEN(address(0)), MIN_STAKE, UNBONDING, admin);

        vm.expectRevert(Errors.ZeroAddress.selector);
        new StakingRegistry(token, MIN_STAKE, UNBONDING, address(0));
    }

    function test_Constructor_EnforcesBounds() public {
        vm.expectRevert(Errors.OutOfBounds.selector);
        new StakingRegistry(token, 99e18, UNBONDING, admin);

        vm.expectRevert(Errors.OutOfBounds.selector);
        new StakingRegistry(token, 100_001e18, UNBONDING, admin);

        vm.expectRevert(Errors.OutOfBounds.selector);
        new StakingRegistry(token, MIN_STAKE, 2 days, admin);

        vm.expectRevert(Errors.OutOfBounds.selector);
        new StakingRegistry(token, MIN_STAKE, 31 days, admin);
    }

    function test_Constructor_StartsPaused() public {
        StakingRegistry reg2 = new StakingRegistry(token, MIN_STAKE, UNBONDING, admin);
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

    function test_Slash_FlatTenPercent_SplitsRewardAndBurn() public {
        vm.prank(operator);
        reg.stake(10 * MIN_STAKE); // 10,000 TOKEN

        uint256 expectedSlash = (10 * MIN_STAKE) / 10; // 1,000
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
        reg.unstake(9 * MIN_STAKE); // leaves 1x active, 9x unbonding
        // slashable = 10x, 10% = 1x. All absorbed by active.
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.Corruption);

        assertEq(reg.getStakeInfo(operator).active, 0);
        assertEq(reg.getStakeInfo(operator).unbonding, 9 * MIN_STAKE);

        // A second slash must spill into unbonding (active is zero).
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.Corruption);

        // slashable was 9x, 10% = 0.9x → pulled from first unbond entry.
        assertEq(reg.getStakeInfo(operator).active, 0);
        assertEq(reg.getStakeInfo(operator).unbonding, 9 * MIN_STAKE - (9 * MIN_STAKE) / 10);
    }

    function test_Slash_RevertsWhenNotStaked() public {
        vm.expectRevert(StakingRegistry.NodeNotStaked.selector);
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
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
        // Stake 20× MIN_STAKE, unstake three times of 5× each.
        vm.prank(operator);
        reg.stake(20 * MIN_STAKE);
        for (uint256 i = 0; i < 3; ++i) {
            vm.prank(operator);
            reg.unstake(5 * MIN_STAKE);
        }
        assertEq(reg.unbondingQueueLength(operator), 3);
        assertEq(reg.getStakeInfo(operator).active, 5 * MIN_STAKE);
        assertEq(reg.getStakeInfo(operator).unbonding, 15 * MIN_STAKE);

        // slashable = 20×, 10% = 2×. Pulls 5× from active (only 5× there),
        // then spills 2× - 5× = no spill needed. Need bigger slash; stake
        // 90× more and set up so slash spills.
        // Better: unstake ALL and slash to force pure-queue spill across
        // entries.
        vm.prank(operator);
        reg.unstake(5 * MIN_STAKE); // active -> 0, 4 entries
        assertEq(reg.getStakeInfo(operator).active, 0);
        assertEq(reg.unbondingQueueLength(operator), 4);

        // slashable = 20×, slashAmount = 2×. Must consume the first entry
        // (5× full? no, 2× partial).
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        // First entry was 5× → partial consume by 2× → leaves 3×. Queue
        // retains all 4 entries (no full consumption).
        assertEq(reg.unbondingQueueLength(operator), 4);
        StakingRegistry.UnbondRequest memory e0 = reg.unbondingEntry(operator, 0);
        assertEq(uint256(e0.amount), 3 * MIN_STAKE);
        assertEq(reg.getStakeInfo(operator).unbonding, 18 * MIN_STAKE);

        // Second slash: slashable = 18×, slashAmount = 1.8×. Consumes
        // entry0 fully (3×) and partial on entry1 ... actually 1.8× < 3×,
        // so only partial entry0.
        vm.prank(slasher);
        reg.slash(operator, IStakingRegistry.OffenseType.PhantomBlob);
        e0 = reg.unbondingEntry(operator, 0);
        assertEq(uint256(e0.amount), 3 * MIN_STAKE - (18 * MIN_STAKE) / 10);
    }

    // ---------- reentrancy ----------

    function test_Reentrancy_BlocksWithdrawUnbonded() public {
        ReentrantERC20 bad = new ReentrantERC20();
        StakingRegistry reg2 = new StakingRegistry(bad, MIN_STAKE, UNBONDING, admin);

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
}
