// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { CapacityBond } from "../src/CapacityBond.sol";
import { Token } from "../src/Token.sol";
import { ISafetyReserve } from "../src/interfaces/ISafetyReserve.sol";

import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";
import { MockSafetyReserve } from "./mocks/MockSafetyReserve.sol";

/// @title CapacityBond smoke tests
/// @notice Minimal coverage of the new ADR 036/028/030/026-v2.2 surface:
///         `firstBondedAt`, `slashedAtEpoch` + `clearSlashedAtEpoch`,
///         Genesis Bond Credit grant/vest/claim, and pending-credit slash
///         burn. NodeId / region-attestation paths require a signed
///         registration helper and land with the broader integration suite.
contract CapacityBondTest is Test {
    Token internal token;
    MockEd25519Verifier internal ed25519;
    MockSafetyReserve internal safety;
    CapacityBond internal bond;

    address internal admin = address(0xA11CE);
    address internal operator = address(0xB0B);
    address internal challenger = address(0xC4A11);

    uint256 internal constant MIN_STAKE = 50_000e18;
    uint256 internal constant UNBONDING = 7 days;

    function setUp() public {
        token = new Token(admin);
        ed25519 = new MockEd25519Verifier();
        safety = new MockSafetyReserve();

        bond = new CapacityBond({
            token_: token,
            ed25519Verifier_: ed25519,
            admin: admin,
            minStake_: MIN_STAKE,
            unbondingPeriod_: UNBONDING,
            multiaddrUpdateCooldown_: 0,
            maxMultiaddrSize_: 1024,
            regionStabilityWindow_: 7 days,
            genesisCreditWindow_: 30 days
        });

        vm.startPrank(admin);
        bond.setSafetyReserve(ISafetyReserve(address(safety)));
        bond.grantRole(bond.SLASH_ROLE(), admin);
        token.transfer(operator, 200_000e18);
        vm.stopPrank();

        vm.prank(operator);
        token.approve(address(bond), type(uint256).max);
    }

    function test_firstBondedAt_setsOnFirstStake() public {
        assertEq(bond.firstBondedAt(operator), 0);
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.stake(MIN_STAKE);
        assertEq(bond.firstBondedAt(operator), 1_000_000);
    }

    function test_firstBondedAt_immutableAcrossReBonds() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.stake(MIN_STAKE);
        uint64 first = bond.firstBondedAt(operator);

        vm.warp(2_000_000);
        vm.prank(operator);
        bond.stake(10e18);
        assertEq(bond.firstBondedAt(operator), first);
    }

    function test_slashedAtEpoch_zeroBeforeSlash() public view {
        assertEq(bond.slashedAtEpoch(operator), 0);
    }

    function test_slash_stampsSlashedAtEpoch() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.stake(MIN_STAKE);

        vm.warp(2_000_000);
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        uint64 expected = uint64(uint256(2_000_000) / bond.EPOCH_LENGTH());
        assertEq(bond.slashedAtEpoch(operator), expected);
    }

    function test_clearSlashedAtEpoch_requiresRole() public {
        vm.prank(operator);
        vm.expectRevert();
        bond.clearSlashedAtEpoch(operator);
    }

    function test_clearSlashedAtEpoch_clears() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.stake(MIN_STAKE);
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        assertGt(bond.slashedAtEpoch(operator), 0);

        bytes32 reversalRole = bond.APPEAL_REVERSAL_ROLE();
        vm.startPrank(admin);
        bond.grantRole(reversalRole, admin);
        bond.clearSlashedAtEpoch(operator);
        vm.stopPrank();
        assertEq(bond.slashedAtEpoch(operator), 0);
    }

    function test_declaredMbps_storesValue() public {
        vm.prank(operator);
        bond.declareMbps(1000);
        assertEq(bond.declaredMbps(operator), 1000);
    }

    function test_genesisCredit_grantWithinWindow() public {
        _setupGenesisGrant(50_000e18);
        CapacityBond.PendingCredit memory pc = bond.pendingCredit(operator);
        assertEq(pc.originalGrant, 50_000e18);
        assertEq(pc.claimed, 0);
    }

    function test_genesisCredit_revertsAfterWindow() public {
        bytes32 grantorRole = bond.GENESIS_GRANTOR_ROLE();
        vm.startPrank(admin);
        token.approve(address(bond), 100_000e18);
        bond.grantRole(grantorRole, admin);
        vm.warp(block.timestamp + 31 days);
        vm.expectRevert();
        bond.grantGenesisCredit(operator, 50_000e18);
        vm.stopPrank();
    }

    function test_genesisCredit_vestsLinearly() public {
        _setupGenesisGrant(100_000e18);
        vm.warp(block.timestamp + 365 days);
        // 365 / 730 = 0.5 → curve at half = 50k.
        assertEq(bond.curveVested(operator), 50_000e18);
        assertEq(bond.claimableCredit(operator), 50_000e18);
    }

    function test_genesisCredit_fullVestAfterDuration() public {
        _setupGenesisGrant(100_000e18);
        vm.warp(block.timestamp + 730 days);
        assertEq(bond.curveVested(operator), 100_000e18);
        assertEq(bond.claimableCredit(operator), 100_000e18);
    }

    function test_claimVestedCredit_movesIntoActiveStake() public {
        _setupGenesisGrant(100_000e18);
        // I5 gate: needs activeStake > 0.
        vm.prank(operator);
        bond.stake(MIN_STAKE);

        vm.warp(block.timestamp + 365 days);
        vm.prank(operator);
        bond.claimVestedCredit();

        // Stake increased by claimable (50k vested at half-curve).
        assertEq(bond.activeStake(operator), MIN_STAKE + 50_000e18);
        assertEq(bond.pendingCredit(operator).claimed, 50_000e18);
        assertEq(bond.pendingCredit(operator).originalGrant, 100_000e18);
    }

    function test_claimVestedCredit_revertsWhenNoActiveStake() public {
        _setupGenesisGrant(100_000e18);
        vm.warp(block.timestamp + 365 days);
        // No stake → I5 gate trips.
        vm.prank(operator);
        vm.expectRevert();
        bond.claimVestedCredit();
    }

    function test_claimVestedCredit_revertsWhenSlashedInWindow() public {
        // Warp past an epoch boundary so the slash stamp is non-zero.
        vm.warp(2 * 7 days);
        _setupGenesisGrant(100_000e18);
        vm.prank(operator);
        bond.stake(MIN_STAKE);
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        assertGt(bond.slashedAtEpoch(operator), 0);

        // Stay inside the 13-epoch slash gate window (advance only a few
        // weeks past the slash). Claim must revert.
        vm.warp(block.timestamp + 4 weeks);
        vm.prank(operator);
        vm.expectRevert();
        bond.claimVestedCredit();
    }

    function test_claimVestedCredit_succeedsAfterSlashGateExpires() public {
        // Stamp slashedAtEpoch on the operator, then warp past the gate.
        vm.warp(2 * 7 days);
        _setupGenesisGrant(100_000e18);
        vm.prank(operator);
        bond.stake(MIN_STAKE);
        vm.prank(admin);
        bond.slash(operator, challenger, 1);

        // Warp 14 epochs (= 98 days) past the slash so currentEpoch
        // exceeds slashEpoch + CLAIM_SLASH_GATE_EPOCHS (13).
        vm.warp(block.timestamp + 14 weeks);
        vm.prank(operator);
        bond.claimVestedCredit();
        assertGt(bond.pendingCredit(operator).claimed, 0);
    }

    function test_claimVestedCredit_multiClaimFollowsLinearCurve() public {
        _setupGenesisGrant(100_000e18);
        vm.prank(operator);
        bond.stake(MIN_STAKE);

        // Total 730 days vest. Claim midway and at full vest; assert the
        // cumulative claim equals the curve target at the second timestamp.
        vm.warp(block.timestamp + 365 days);
        vm.prank(operator);
        bond.claimVestedCredit();

        vm.warp(block.timestamp + 365 days);
        vm.prank(operator);
        bond.claimVestedCredit();

        // C4 fix: cumulative claim at full vest equals the original grant
        // (100k), regardless of the intermediate claim. Without the
        // originalGrant decoupling, the second claim would skew.
        assertEq(bond.pendingCredit(operator).claimed, 100_000e18);
    }

    function test_pendingCreditSlash_reducesOriginalGrant() public {
        _setupGenesisGrant(100_000e18);
        vm.prank(operator);
        bond.stake(MIN_STAKE);

        uint128 grantBefore = bond.pendingCredit(operator).originalGrant;
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        uint128 grantAfter = bond.pendingCredit(operator).originalGrant;
        // tier 1 = 5% of unvested 100k = 5k.
        assertEq(grantBefore - grantAfter, 5000e18);
    }

    function test_creditSlash_routesThroughChallengerAndSafety() public {
        _setupGenesisGrant(100_000e18);
        vm.prank(operator);
        bond.stake(MIN_STAKE);

        uint256 challengerBalanceBefore = token.balanceOf(challenger);
        uint256 safetyBalanceBefore = token.balanceOf(address(safety));

        vm.prank(admin);
        bond.slash(operator, challenger, 1);

        // C1: credit slash (5k) joins stake slash (5% × 50k = 2.5k) for a
        // combined 7.5k routed through 50/30/20. Challenger gets 50% = 3.75k.
        assertEq(token.balanceOf(challenger) - challengerBalanceBefore, 3750e18);
        assertEq(token.balanceOf(address(safety)) - safetyBalanceBefore, 2250e18);
    }

    function test_forfeitUnvestedCredit_onUnbond() public {
        _setupGenesisGrant(100_000e18);
        vm.prank(operator);
        bond.stake(MIN_STAKE);

        address treasury_ = address(0xDEAD);
        vm.prank(admin);
        bond.setTreasury(treasury_);

        vm.warp(block.timestamp + 365 days);
        // Vested = 50k, unvested = 50k → forfeit on requestUnstake.
        vm.prank(operator);
        bond.requestUnstake(MIN_STAKE);

        assertEq(token.balanceOf(treasury_), 50_000e18);
        assertEq(bond.pendingCredit(operator).originalGrant, 50_000e18);
    }

    function test_forfeitUnvestedCredit_burnsWhenNoTreasury() public {
        _setupGenesisGrant(100_000e18);
        vm.prank(operator);
        bond.stake(MIN_STAKE);

        // treasury unset → forfeit burns.
        uint256 supplyBefore = token.totalSupply();
        vm.warp(block.timestamp + 365 days);
        vm.prank(operator);
        bond.requestUnstake(MIN_STAKE);
        assertEq(supplyBefore - token.totalSupply(), 50_000e18);
    }

    function test_slashId_persistsRecord() public {
        vm.prank(operator);
        bond.stake(MIN_STAKE);
        vm.prank(admin);
        (uint256 slashId,) = bond.slash(operator, challenger, 1);
        assertEq(slashId, 0);
        (address op, uint64 ts, uint256 amount) = bond.slashRecords(0);
        assertEq(op, operator);
        assertGt(ts, 0);
        assertGt(amount, 0);
        assertEq(bond.slashCounter(), 1);
    }

    function _setupGenesisGrant(uint256 amount) internal {
        bytes32 grantorRole = bond.GENESIS_GRANTOR_ROLE();
        vm.startPrank(admin);
        token.approve(address(bond), amount);
        bond.grantRole(grantorRole, admin);
        bond.grantGenesisCredit(operator, amount);
        vm.stopPrank();
    }
}
