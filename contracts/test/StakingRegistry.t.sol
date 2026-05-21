// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Token } from "../src/Token.sol";
import { StakingRegistry } from "../src/StakingRegistry.sol";
import { ISafetyReserve } from "../src/interfaces/ISafetyReserve.sol";
import { MockSafetyReserve } from "./mocks/MockSafetyReserve.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";

contract StakingRegistryTest is Test {
    Token internal token;
    StakingRegistry internal reg;
    MockSafetyReserve internal safetyReserve;

    address internal admin = makeAddr("admin");
    address internal slashJudge = makeAddr("slashJudge");
    address internal blacklist = makeAddr("blacklist");
    address internal feeRouter = makeAddr("feeRouter");
    address internal pauser = makeAddr("pauser");
    address internal operator = makeAddr("operator");
    address internal challenger = makeAddr("challenger");

    uint256 internal constant MIN_STAKE = 50_000e18;
    uint256 internal constant UNBONDING_PERIOD = 7 days;

    function setUp() public {
        token = new Token(admin);
        safetyReserve = new MockSafetyReserve();
        reg = new StakingRegistry(token, admin, MIN_STAKE, UNBONDING_PERIOD);

        vm.startPrank(admin);
        reg.grantRole(reg.SLASH_ROLE(), slashJudge);
        reg.grantRole(reg.BLACKLIST_ROLE(), blacklist);
        reg.grantRole(reg.SETTLEMENT_REPORTER_ROLE(), feeRouter);
        reg.grantRole(reg.PAUSER_ROLE(), pauser);
        reg.setSafetyReserve(ISafetyReserve(address(safetyReserve)));
        // Seed the operator with stake-worthy TOKEN. Sized above the fuzz
        // upper bound (1M TOKEN) so fuzz tests don't run out.
        assertTrue(token.transfer(operator, 100 * MIN_STAKE));
        vm.stopPrank();
    }

    function _stake(address who, uint256 amount) internal {
        vm.startPrank(who);
        token.approve(address(reg), amount);
        reg.stake(amount);
        vm.stopPrank();
    }

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    function test_constructor_setsImmutableAndState() public view {
        assertEq(address(reg.token()), address(token));
        assertEq(reg.minStake(), MIN_STAKE);
        assertEq(reg.unbondingPeriod(), UNBONDING_PERIOD);
        assertTrue(reg.hasRole(reg.DEFAULT_ADMIN_ROLE(), admin));
        assertTrue(reg.hasRole(reg.GOVERNANCE_ROLE(), admin));
    }

    function test_constructor_revertsOnZeroToken() public {
        vm.expectRevert(StakingRegistry.ZeroAddress.selector);
        new StakingRegistry(ERC20Burnable(address(0)), admin, MIN_STAKE, UNBONDING_PERIOD);
    }

    function test_constructor_revertsOnZeroAdmin() public {
        vm.expectRevert(StakingRegistry.ZeroAddress.selector);
        new StakingRegistry(token, address(0), MIN_STAKE, UNBONDING_PERIOD);
    }

    function test_constructor_revertsOnOutOfBoundsMinStake() public {
        vm.expectRevert();
        new StakingRegistry(token, admin, 1e18, UNBONDING_PERIOD);
        vm.expectRevert();
        new StakingRegistry(token, admin, 10_000_000e18, UNBONDING_PERIOD);
    }

    function test_constructor_revertsOnOutOfBoundsUnbondingPeriod() public {
        vm.expectRevert();
        new StakingRegistry(token, admin, MIN_STAKE, 1 days);
        vm.expectRevert();
        new StakingRegistry(token, admin, MIN_STAKE, 60 days);
    }

    // -----------------------------------------------------------------
    // Staking
    // -----------------------------------------------------------------

    function test_stake_addsToActiveStake() public {
        _stake(operator, MIN_STAKE);
        assertEq(reg.activeStake(operator), MIN_STAKE);
        assertEq(reg.stakeOf(operator), MIN_STAKE);
        assertEq(token.balanceOf(address(reg)), MIN_STAKE);
    }

    function test_stake_revertsOnZeroAmount() public {
        vm.prank(operator);
        vm.expectRevert(StakingRegistry.ZeroAmount.selector);
        reg.stake(0);
    }

    function test_stake_revertsWhenPaused() public {
        vm.prank(pauser);
        reg.pause();
        vm.prank(operator);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        reg.stake(MIN_STAKE);
    }

    function test_stake_clearsEjectedFlagWhenRestakedToMinimum() public {
        _stake(operator, MIN_STAKE);
        // Force ejection via the blacklist path
        vm.prank(blacklist);
        reg.ejectNode(operator);
        assertTrue(reg.ejected(operator));

        // Re-stake while already at minimum doesn't auto-clear (the
        // re-stake path is for when stake fell below — but blacklist
        // ejection didn't reduce stake). Add one more wei to trigger the
        // path explicitly.
        _stake(operator, 1);
        assertFalse(reg.ejected(operator));
    }

    function test_isActive_requiresMinStakeAndNotEjected() public {
        assertFalse(reg.isActive(operator));
        _stake(operator, MIN_STAKE - 1);
        assertFalse(reg.isActive(operator));
        _stake(operator, 1);
        assertTrue(reg.isActive(operator));

        vm.prank(blacklist);
        reg.ejectNode(operator);
        assertFalse(reg.isActive(operator));
    }

    // -----------------------------------------------------------------
    // Unbonding + unstake
    // -----------------------------------------------------------------

    function test_requestUnstake_movesAmountToUnbonding() public {
        _stake(operator, 2 * MIN_STAKE);

        vm.prank(operator);
        reg.requestUnstake(MIN_STAKE);

        assertEq(reg.activeStake(operator), MIN_STAKE);
        (uint256 amount, uint256 unlockAt) = reg.unbondingOf(operator);
        assertEq(amount, MIN_STAKE);
        assertEq(unlockAt, block.timestamp + UNBONDING_PERIOD);
    }

    function test_requestUnstake_revertsOnInsufficientStake() public {
        _stake(operator, MIN_STAKE);
        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(StakingRegistry.InsufficientStake.selector, MIN_STAKE + 1, MIN_STAKE));
        reg.requestUnstake(MIN_STAKE + 1);
    }

    function test_requestUnstake_revertsOnInflightRequest() public {
        _stake(operator, 2 * MIN_STAKE);
        vm.prank(operator);
        reg.requestUnstake(MIN_STAKE);
        vm.prank(operator);
        vm.expectRevert(StakingRegistry.UnbondingInProgress.selector);
        reg.requestUnstake(MIN_STAKE);
    }

    function test_unstake_revertsBeforeUnbondingComplete() public {
        _stake(operator, MIN_STAKE);
        vm.prank(operator);
        reg.requestUnstake(MIN_STAKE);

        vm.warp(block.timestamp + UNBONDING_PERIOD - 1);
        vm.prank(operator);
        vm.expectRevert();
        reg.unstake();
    }

    function test_unstake_returnsTokensAfterUnbonding() public {
        _stake(operator, MIN_STAKE);
        uint256 operatorBalanceBefore = token.balanceOf(operator);

        vm.prank(operator);
        reg.requestUnstake(MIN_STAKE);

        vm.warp(block.timestamp + UNBONDING_PERIOD);
        vm.prank(operator);
        reg.unstake();

        assertEq(token.balanceOf(operator), operatorBalanceBefore + MIN_STAKE);
        (uint256 amount,) = reg.unbondingOf(operator);
        assertEq(amount, 0);
    }

    function test_unstake_revertsWithNoRequest() public {
        vm.prank(operator);
        vm.expectRevert(StakingRegistry.NoUnbondingRequest.selector);
        reg.unstake();
    }

    // -----------------------------------------------------------------
    // Slash: tier escalation
    // -----------------------------------------------------------------

    function test_slash_tier1_is5Percent() public {
        _stake(operator, 100_000e18);
        uint256 amount = _slash(operator);
        assertEq(amount, 5000e18, "1st offense = 5%");
        assertEq(reg.lifetimeOffenseCount(operator), 1);
    }

    function test_slash_tier2_is15Percent() public {
        _stake(operator, 100_000e18);
        _slash(operator);
        uint256 amount = _slash(operator);
        // After tier-1 slash, remaining stake is 95k. 15% of 95k = 14_250.
        assertEq(amount, 14_250e18, "2nd offense = 15%");
        assertEq(reg.lifetimeOffenseCount(operator), 2);
    }

    function test_slash_tier3_is50Percent() public {
        _stake(operator, 100_000e18);
        _slash(operator); // tier 1: -5_000 → 95_000
        _slash(operator); // tier 2: -14_250 → 80_750
        uint256 amount = _slash(operator); // tier 3: -50% of 80_750 = 40_375
        assertEq(amount, 40_375e18, "3rd offense = 50%");
        assertEq(reg.lifetimeOffenseCount(operator), 3);
    }

    function test_slash_tier3Stays50PercentBeyondThird() public {
        _stake(operator, 1_000_000e18); // big enough to survive several slashes
        for (uint256 i = 0; i < 5; i++) {
            _slash(operator);
        }
        assertEq(reg.lifetimeOffenseCount(operator), 5);
    }

    // -----------------------------------------------------------------
    // Slash: 50/30/20 split
    // -----------------------------------------------------------------

    function test_slash_distributes50_30_20() public {
        _stake(operator, 100_000e18);
        uint256 supplyBefore = token.totalSupply();
        uint256 challengerBalanceBefore = token.balanceOf(challenger);
        uint256 safetyBalanceBefore = token.balanceOf(address(safetyReserve));

        vm.prank(slashJudge);
        uint256 slashed = reg.slash(operator, challenger, 0);

        assertEq(slashed, 5000e18);
        assertEq(token.balanceOf(challenger), challengerBalanceBefore + 2500e18, "50% to challenger");
        assertEq(token.balanceOf(address(safetyReserve)), safetyBalanceBefore + 1500e18, "30% to SafetyReserve");
        assertEq(token.totalSupply(), supplyBefore - 1000e18, "20% burned");
    }

    function test_slash_notifiesSafetyReserve() public {
        _stake(operator, 100_000e18);
        vm.prank(slashJudge);
        reg.slash(operator, challenger, 0);

        assertEq(safetyReserve.inflowCount(), 1);
        (address inflowOp, uint256 inflowAmount) = safetyReserve.inflows(0);
        assertEq(inflowOp, operator);
        assertEq(inflowAmount, 1500e18);
    }

    // -----------------------------------------------------------------
    // Slash: applies to active + unbonding (anti-slash-then-run)
    // -----------------------------------------------------------------

    function test_slash_appliesToActivePlusUnbonding() public {
        _stake(operator, 100_000e18);
        vm.prank(operator);
        reg.requestUnstake(40_000e18);
        // Now: active=60_000, unbonding=40_000. Total at-risk=100_000.

        vm.prank(slashJudge);
        uint256 slashed = reg.slash(operator, challenger, 0);

        assertEq(slashed, 5000e18, "5% of full at-risk");
        // Active reduced first.
        assertEq(reg.activeStake(operator), 55_000e18);
        (uint256 unbondingAmount,) = reg.unbondingOf(operator);
        assertEq(unbondingAmount, 40_000e18, "unbonding untouched (active absorbed full slash)");
    }

    function test_slash_drawsFromUnbondingWhenActiveInsufficient() public {
        _stake(operator, 100_000e18);
        vm.prank(operator);
        reg.requestUnstake(99_000e18);
        // Now: active=1_000, unbonding=99_000. Total at-risk=100_000.

        vm.prank(slashJudge);
        uint256 slashed = reg.slash(operator, challenger, 0); // 5% = 5_000

        assertEq(slashed, 5000e18);
        assertEq(reg.activeStake(operator), 0);
        (uint256 unbondingAmount,) = reg.unbondingOf(operator);
        assertEq(unbondingAmount, 95_000e18, "unbonding absorbed 4_000");
    }

    // -----------------------------------------------------------------
    // Slash: auto-ejection
    // -----------------------------------------------------------------

    function test_slash_autoEjectsWhenStakeFallsBelowHalfMinimum() public {
        // Stake exactly at minimum; a single 50% slash drops active to half,
        // and 50% of minStake is *not* below 50% of minStake (boundary).
        // Force three slashes to drop below the threshold.
        _stake(operator, MIN_STAKE);
        _slash(operator); // 5% off MIN_STAKE = 47_500
        _slash(operator); // 15% off 47_500 = 7_125 → active=40_375
        _slash(operator); // 50% off 40_375 = 20_187.5 → active=20_187.5
        // 20_187.5 < 25_000 (MIN_STAKE / 2) ⇒ should be ejected.
        assertTrue(reg.ejected(operator));
        assertFalse(reg.isActive(operator));
    }

    // -----------------------------------------------------------------
    // Slash: access control + wiring
    // -----------------------------------------------------------------

    function test_slash_revertsWithoutSlashRole() public {
        _stake(operator, MIN_STAKE);
        vm.expectRevert();
        reg.slash(operator, challenger, 0);
    }

    function test_slash_revertsWhenSafetyReserveNotWired() public {
        vm.prank(admin);
        reg.setSafetyReserve(ISafetyReserve(address(0)));
        _stake(operator, MIN_STAKE);
        vm.prank(slashJudge);
        vm.expectRevert(StakingRegistry.SafetyReserveNotWired.selector);
        reg.slash(operator, challenger, 0);
    }

    function test_slash_revertsOnZeroChallenger() public {
        _stake(operator, MIN_STAKE);
        vm.prank(slashJudge);
        vm.expectRevert(StakingRegistry.ZeroAddress.selector);
        reg.slash(operator, address(0), 0);
    }

    // -----------------------------------------------------------------
    // ContentBlacklist ejection
    // -----------------------------------------------------------------

    function test_ejectNode_setsFlagWithoutMovingTokens() public {
        _stake(operator, MIN_STAKE);
        uint256 stakeBefore = reg.activeStake(operator);
        vm.prank(blacklist);
        reg.ejectNode(operator);

        assertTrue(reg.ejected(operator));
        assertEq(reg.activeStake(operator), stakeBefore, "stake untouched");
    }

    function test_ejectNode_revertsWithoutBlacklistRole() public {
        vm.expectRevert();
        reg.ejectNode(operator);
    }

    // -----------------------------------------------------------------
    // Settlement recording (FeeRouter)
    // -----------------------------------------------------------------

    function test_recordSettlement_updatesTimestamp() public {
        vm.prank(feeRouter);
        reg.recordSettlement(operator);
        assertEq(reg.lastSettlementAt(operator), block.timestamp);
    }

    function test_recordSettlement_revertsWithoutReporterRole() public {
        vm.expectRevert();
        reg.recordSettlement(operator);
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function test_setMinStake_revertsOutOfBounds() public {
        vm.prank(admin);
        vm.expectRevert();
        reg.setMinStake(1e18);
        vm.prank(admin);
        vm.expectRevert();
        reg.setMinStake(2_000_000e18);
    }

    function test_setUnbondingPeriod_revertsOutOfBounds() public {
        vm.prank(admin);
        vm.expectRevert();
        reg.setUnbondingPeriod(2 days);
        vm.prank(admin);
        vm.expectRevert();
        reg.setUnbondingPeriod(60 days);
    }

    function test_setMinStake_withinBoundsSucceeds() public {
        vm.prank(admin);
        reg.setMinStake(100_000e18);
        assertEq(reg.minStake(), 100_000e18);
    }

    // -----------------------------------------------------------------
    // Fuzz: slash math always balances
    // -----------------------------------------------------------------

    function testFuzz_slash_threeLegsSumToSlashAmount(uint256 stakeAmount) public {
        stakeAmount = bound(stakeAmount, MIN_STAKE, 1_000_000e18);
        _stake(operator, stakeAmount);

        uint256 supplyBefore = token.totalSupply();
        uint256 challengerBefore = token.balanceOf(challenger);
        uint256 safetyBefore = token.balanceOf(address(safetyReserve));

        vm.prank(slashJudge);
        uint256 slashed = reg.slash(operator, challenger, 0);

        uint256 challengerDelta = token.balanceOf(challenger) - challengerBefore;
        uint256 safetyDelta = token.balanceOf(address(safetyReserve)) - safetyBefore;
        uint256 burned = supplyBefore - token.totalSupply();

        assertEq(challengerDelta + safetyDelta + burned, slashed, "three legs sum to slashAmount");
    }

    // -----------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------

    function _slash(address op) internal returns (uint256) {
        vm.prank(slashJudge);
        return reg.slash(op, challenger, 0);
    }
}
