// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { CapacityBond } from "../src/CapacityBond.sol";
import { Token } from "../src/Token.sol";
import { ISafetyReserve } from "../src/interfaces/ISafetyReserve.sol";
import { IEd25519Verifier } from "../src/interfaces/IEd25519Verifier.sol";

import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";
import { MockSafetyReserve } from "./mocks/MockSafetyReserve.sol";

/// @notice Test-only `CapacityBond` subclass exposing the internal
///         stake-reduction logic so the C2 defensive remainder-clip branch in
///         `_reduceStakeAtTier` can be exercised directly. That branch is
///         unreachable through the public `slash()`
///         API: it fires only when `slashAmount > totalAtRisk`, i.e.
///         `tierBps > 10_000` (>100%), and the immutable slash ladder maxes at
///         5_000 (50%). It exists as a forward-guard for a hypothetical future
///         tier schedule, so the only honest way to cover it is to drive the
///         internal call with an out-of-ladder `tierBps`.
contract TestableCapacityBond is CapacityBond {
    constructor(
        ERC20Burnable token_,
        IEd25519Verifier ed25519Verifier_,
        address admin,
        uint256 minStake_,
        uint256 unbondingPeriod_,
        uint256 multiaddrUpdateCooldown_,
        uint256 maxMultiaddrSize_,
        uint256 regionStabilityWindow_,
        uint256 genesisCreditWindow_
    )
        CapacityBond(
            token_,
            ed25519Verifier_,
            admin,
            minStake_,
            unbondingPeriod_,
            multiaddrUpdateCooldown_,
            maxMultiaddrSize_,
            regionStabilityWindow_,
            genesisCreditWindow_
        )
    { }

    /// @dev Write active + unbonding stake directly so the clip test needn't
    ///      plumb tokens and the unbonding period through the public API.
    function setStakeState(address operator, uint256 active, uint256 unbonding) external {
        activeStake[operator] = active;
        unbondingOf[operator].amount = unbonding;
    }

    function exposed_reduceStakeAtTier(address operator, uint256 tierBps) external returns (uint256) {
        return _reduceStakeAtTier(operator, tierBps);
    }
}

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
        // `slashedAtEpoch` stores `actualEpoch + 1` so an epoch-0 slash isn't
        // confused with the unslashed sentinel; expect the +1-offset stamp.
        uint64 expected = uint64(uint256(2_000_000) / bond.EPOCH_LENGTH()) + 1;
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

    // ── Slashing-lifecycle coverage (issue #703) ────────────────────────────

    /// Slash the same operator 3× and assert the tier ladder escalates
    /// 5% → 15% → 50% (`SLASH_BPS_TIER_1/2/3`) driven by `lifetimeOffenseCount`.
    /// No genesis credit, so `creditSlash == 0` and the reductions are pure
    /// active-stake math. Start at 160k so even after the 50% tier the active
    /// balance (64.6k) stays above the `minStake/2` (25k) auto-eject floor.
    function test_slash_tierEscalation_15then50pct() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.stake(160_000e18);

        // Tier 1: 5% of 160k = 8k.
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        assertEq(bond.lifetimeOffenseCount(operator), 1);
        assertEq(bond.activeStake(operator), 152_000e18);

        // Tier 2: 15% of 152k = 22.8k.
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        assertEq(bond.lifetimeOffenseCount(operator), 2);
        assertEq(bond.activeStake(operator), 129_200e18);

        // Tier 3 (3rd+ offense): 50% of 129.2k = 64.6k.
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        assertEq(bond.lifetimeOffenseCount(operator), 3);
        assertEq(bond.activeStake(operator), 64_600e18);
    }

    /// Tier-3 slash against an operator holding BOTH active and unbonding stake
    /// where `unbonding > active`, so the slash exhausts active first and taps
    /// unbonding for the remainder (the reachable `else` branch of
    /// `_reduceStakeAtTier`). The literal remainder-clip inside that branch is
    /// unreachable here (it needs `tierBps > 100%`); see
    /// `test_reduceStakeAtTier_remainderClip` for that path.
    function test_slash_tier3_mixedStake_exhaustsActiveTapsUnbonding() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.stake(100_000e18);

        // Bump lifetimeOffenseCount to 2 so the next slash lands at tier 3.
        vm.prank(admin);
        bond.slash(operator, challenger, 1); // 5% → 95k active
        vm.prank(admin);
        bond.slash(operator, challenger, 1); // 15% of 95k → 80.75k active
        assertEq(bond.activeStake(operator), 80_750e18);

        // Move most stake into unbonding so unbonding (60k) > active (20.75k).
        vm.prank(operator);
        bond.requestUnstake(60_000e18);
        assertEq(bond.activeStake(operator), 20_750e18);

        // Tier 3: totalAtRisk = 80.75k, slashAmount = 40.375k > active 20.75k.
        // Active is zeroed; remainder (19.625k) comes out of unbonding, leaving
        // 60k − 19.625k = 40.375k. No underflow, no clip.
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        assertEq(bond.lifetimeOffenseCount(operator), 3);
        assertEq(bond.activeStake(operator), 0);
        (uint256 unbondingAmt,) = bond.unbondingOf(operator);
        assertEq(unbondingAmt, 40_375e18);
    }

    /// Directly exercise the C2 defensive remainder-clip in
    /// `_reduceStakeAtTier`. It is unreachable through `slash()` — the
    /// clip fires only when `slashAmount > totalAtRisk`, i.e. `tierBps > 10_000`
    /// (>100%), and the immutable ladder maxes at 5_000 (50%). Drive it via the
    /// harness with `tierBps = 12_000` to prove it caps `slashAmount` to the
    /// at-risk total (no over-transfer) and never underflows the unbonding pool.
    function test_reduceStakeAtTier_remainderClip() public {
        TestableCapacityBond harness = new TestableCapacityBond({
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

        harness.setStakeState(operator, 100e18, 100e18);
        // totalAtRisk = 200e18; 120% → slashAmount would be 240e18 > totalAtRisk
        // → clip clamps remainder to req.amount and re-derives slashAmount.
        uint256 slashed = harness.exposed_reduceStakeAtTier(operator, 12_000);
        assertEq(slashed, 200e18); // capped to at-risk, not 240e18
        assertEq(harness.activeStake(operator), 0);
        (uint256 unbondingAmt,) = harness.unbondingOf(operator);
        assertEq(unbondingAmt, 0);
    }

    /// A successful appeal reversal (`SafetyReserve.reverseAppeal` →
    /// `clearSlashedAtEpoch`) must re-enable `claimVestedCredit` IMMEDIATELY,
    /// inside the original slash gate window — without waiting for the
    /// 13-epoch gate to expire naturally. This is the cross-contract recovery
    /// semantics ADR 028/036 promise.
    function test_appealReversal_unlocksClaimBeforeGateExpiry() public {
        vm.warp(2 * 7 days);
        _setupGenesisGrant(100_000e18);
        vm.prank(operator);
        bond.stake(MIN_STAKE);
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        assertGt(bond.slashedAtEpoch(operator), 0);

        // Inside the gate window the claim is blocked by the slash gate
        // specifically (not NothingVested / NotActiveForClaim).
        vm.warp(block.timestamp + 4 weeks);
        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.SlashedInWindowForClaim.selector, operator));
        bond.claimVestedCredit();

        // Appeal reversal clears the stamp (simulating SafetyReserve).
        bytes32 reversalRole = bond.APPEAL_REVERSAL_ROLE();
        vm.startPrank(admin);
        bond.grantRole(reversalRole, admin);
        bond.clearSlashedAtEpoch(operator);
        vm.stopPrank();
        assertEq(bond.slashedAtEpoch(operator), 0);

        // Without advancing past the gate, the claim now succeeds.
        vm.prank(operator);
        bond.claimVestedCredit();
        assertGt(bond.pendingCredit(operator).claimed, 0);
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
        // Tier 1 = 5% of the at-risk pool (originalGrant − claimed = 100k − 0
        // = 100k) = 5k. Pre-fix this was 5% of just the unvested 100k → also
        // 5k for this no-claim case, but the regression test below covers the
        // case where it actually differs.
        assertEq(grantBefore - grantAfter, 5000e18);
    }

    /// @notice Closes the vested-but-unclaimed loophole — slashing must hit
    ///         the full `originalGrant - claimed` pool, not just the unvested
    ///         portion. An operator who claims partway and then is slashed
    ///         MUST see their unclaimed-vested portion slashed too. Pre-fix,
    ///         only the unvested portion was slashed; the vested-but-
    ///         unclaimed sat in the contract escaping the slash entirely.
    function test_slashHitsVestedButUnclaimed() public {
        _setupGenesisGrant(100_000e18);
        vm.prank(operator);
        bond.stake(MIN_STAKE * 2);

        // Half-vest: vested = 50k, unvested = 50k.
        vm.warp(block.timestamp + 365 days);
        // Claim the full vested (50k); claimed = 50k, activeStake += 50k.
        vm.prank(operator);
        bond.claimVestedCredit();
        assertEq(bond.pendingCredit(operator).claimed, 50_000e18);

        // At-risk pool = originalGrant − claimed = 100k − 50k = 50k (= the
        // unvested portion, since the vested-claimed amount is now in
        // activeStake and slashed there).
        uint128 grantBefore = bond.pendingCredit(operator).originalGrant;
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        uint128 grantAfter = bond.pendingCredit(operator).originalGrant;
        // Tier 1 = 5% of 50k = 2.5k slashed from originalGrant.
        assertEq(grantBefore - grantAfter, 2500e18);
    }

    /// @notice Inverse of the prior test: operator never claims, half-vests,
    ///         then is slashed. The at-risk pool is the FULL 100k (unvested
    ///         50k + vested-unclaimed 50k). Pre-fix only the unvested 50k
    ///         was slashed; post-fix the slash basis is the entire 100k.
    function test_slashHitsVestedAndUnvestedTogether() public {
        address op2 = address(0xB0B2);
        // Provision + grant BEFORE any warp so we stay inside the
        // GENESIS_CREDIT_WINDOW (default 30 days from construction).
        vm.prank(admin);
        token.transfer(op2, 200_000e18);
        vm.prank(op2);
        token.approve(address(bond), type(uint256).max);
        vm.prank(op2);
        bond.stake(MIN_STAKE);

        vm.startPrank(admin);
        token.approve(address(bond), 100_000e18);
        bond.grantRole(bond.GENESIS_GRANTOR_ROLE(), admin);
        bond.grantGenesisCredit(op2, 100_000e18);
        vm.stopPrank();

        // Half-vest. Operator claims nothing.
        vm.warp(block.timestamp + 365 days);
        // originalGrant=100k, claimed=0 → at-risk = 100k.
        uint128 op2Before = bond.pendingCredit(op2).originalGrant;
        vm.prank(admin);
        bond.slash(op2, challenger, 1);
        uint128 op2After = bond.pendingCredit(op2).originalGrant;
        // Tier 1 = 5% × 100k = 5k. Pre-fix would have been 5% × 50k unvested = 2.5k.
        assertEq(op2Before - op2After, 5000e18);
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

    /// @notice Regression for the retroactive clawback bug. After forfeit,
    ///         `curveVested` MUST return the truncated principal directly
    ///         and MUST NOT shrink below `claimed`. The pre-fix
    ///         `_forfeitUnvestedCredit` set `originalGrant = vested` but
    ///         left `grantedAt` untouched, so the curve re-stretched the
    ///         smaller principal over the original timeline and returned
    ///         `vested × elapsed/duration` < vested at the forfeit instant —
    ///         clawing back claims an operator had already legitimately
    ///         vested. The fix stamps `FULLY_VESTED_SENTINEL` so the curve
    ///         immediately returns the truncated principal.
    function test_forfeitUnvestedCredit_doesNotClawBackVestedUnclaimed() public {
        _setupGenesisGrant(100_000e18);
        // Stake well above MIN_STAKE so a partial unbond leaves activeStake
        // non-zero (I5 gate requires activeStake > 0 to claim).
        vm.prank(operator);
        bond.stake(MIN_STAKE * 2);

        // Wait halfway through the vest (vested = 50k, unvested = 50k).
        vm.warp(block.timestamp + 365 days);
        // Claim the full currently-claimable amount (50k at half-vest).
        vm.prank(operator);
        bond.claimVestedCredit();
        assertEq(bond.pendingCredit(operator).claimed, 50_000e18);

        // Partial unstake triggers forfeit while leaving activeStake > 0.
        // Pre-fix: post-forfeit `curveVested` would return 50k × 365/730 = 25k
        // and `claimableCredit` would saturate at 0 even though the math
        // implies a 25k retroactive shrink below `claimed`. With the
        // sentinel fix, `curveVested` returns the truncated 50k principal
        // directly — `claimed` is preserved at 50k, claimable settles to 0
        // by exhaustion (50k − 50k), not by silent clawback.
        vm.prank(operator);
        bond.requestUnstake(MIN_STAKE);

        CapacityBond.PendingCredit memory pc = bond.pendingCredit(operator);
        assertEq(pc.originalGrant, 50_000e18);
        assertEq(pc.claimed, 50_000e18);
        assertEq(bond.curveVested(operator), 50_000e18);
        assertEq(bond.claimableCredit(operator), 0);
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

    /// @notice I3 regression — `CapacityBond.slash()` MUST succeed even when
    ///         the wired `SafetyReserve.recordSlashInflow` reverts. Without
    ///         the try/catch in `_routeSlashShares`, a faulty / paused
    ///         SafetyReserve would brick every slash. Asserts the slash
    ///         completes (counter advances, record minted) AND that no
    ///         inflow row was recorded — proving the catch arm was reached
    ///         (the happy path would have appended one row).
    function test_slash_succeedsEvenWhenInflowCallbackReverts() public {
        vm.prank(operator);
        bond.stake(MIN_STAKE);

        // Flip the mock to revert inside `recordSlashInflow`.
        safety.setRevertOnRecordSlashInflow(true);

        uint256 inflowsBefore = safety.inflowCount();

        vm.prank(admin);
        (uint256 slashId, uint256 totalSlash) = bond.slash(operator, challenger, 1);

        // Slash record persisted, counter advanced — proves the slash
        // wasn't reverted by the failing callback.
        assertEq(slashId, 0);
        assertEq(bond.slashCounter(), 1);
        // Tier-1 slash on 50k active stake = 5% × 50k = 2_500e18; no
        // genesis credit in this test, so totalSlash == stakeSlash = 2_500e18.
        assertEq(totalSlash, 2500e18);
        // No new inflow was recorded — confirms the catch arm executed
        // rather than the happy path.
        assertEq(safety.inflowCount(), inflowsBefore);
    }

    function _setupGenesisGrant(uint256 amount) internal {
        bytes32 grantorRole = bond.GENESIS_GRANTOR_ROLE();
        vm.startPrank(admin);
        token.approve(address(bond), amount);
        bond.grantRole(grantorRole, admin);
        bond.grantGenesisCredit(operator, amount);
        vm.stopPrank();
    }

    // ----------------------------------------------------------------------
    // ADR 030 — region self-attestation
    // ----------------------------------------------------------------------

    /// @notice ADR 030 § Region-stability window: the first `updateRegion`
    ///         call has no cooldown (operators may correct their initial
    ///         `registerNode` region); every subsequent call is gated by
    ///         `regionStabilityWindow` and snapshots the prior value into
    ///         `regionPrev`. Also asserts the `RegionUpdated` event payload
    ///         on both successful calls so a future emit-arg rename or
    ///         omission lands as a test failure. Uses a key-derived operator
    ///         so the EIP-712 binding signature for `registerNode` is
    ///         forge-signable.
    function test_updateRegion_cooldownAndPrevSnapshot() public {
        uint256 opPk = 0xC0FFEE;
        address opAddr = vm.addr(opPk);

        // Fund + approve from the admin's TOKEN balance.
        vm.prank(admin);
        token.transfer(opAddr, MIN_STAKE);
        vm.prank(opAddr);
        token.approve(address(bond), type(uint256).max);

        // Warp to a base far past EPOCH boundaries so any nested epoch math
        // in this test is comfortably non-zero.
        vm.warp(1_000_000);

        // Stake to satisfy the registerNode precondition.
        vm.prank(opAddr);
        bond.stake(MIN_STAKE);

        // Register node — binding signature signed with opPk; ed25519 is
        // mocked to accept any signature unconditionally.
        bytes32 nodeId = bytes32(uint256(0xC0FFEEC0FFEEC0FFEE));
        bytes memory bindingSig = _signBindNode(opPk, opAddr, nodeId);
        bytes memory edSig = hex"01";
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", bindingSig, edSig);

        // First updateRegion has no cooldown (lastChanged == 0 branch).
        vm.expectEmit(true, false, false, true, address(bond));
        emit CapacityBond.RegionUpdated(nodeId, "us-east", "eu-west");
        vm.prank(opAddr);
        bond.updateRegion("eu-west");
        assertEq(bond.regionPrev(opAddr), "us-east");
        assertEq(bond.regionLastChanged(opAddr), uint64(block.timestamp));

        // Second call inside `regionStabilityWindow` (7 days) reverts.
        vm.warp(block.timestamp + 1 days);
        vm.prank(opAddr);
        vm.expectRevert();
        bond.updateRegion("ap-south");

        // After the window elapses, the call succeeds and `regionPrev`
        // captures the now-prior region.
        vm.warp(block.timestamp + 7 days + 1);
        vm.expectEmit(true, false, false, true, address(bond));
        emit CapacityBond.RegionUpdated(nodeId, "eu-west", "ap-south");
        vm.prank(opAddr);
        bond.updateRegion("ap-south");
        assertEq(bond.regionPrev(opAddr), "eu-west");
    }

    // ----------------------------------------------------------------------
    // Access-control guards on governance setters
    //
    // None of the setters below were previously covered for role enforcement.
    // A regression that drops `onlyRole(GOVERNANCE_ROLE)` would otherwise
    // pass CI silently, so each setter gets a "reverts when called by a
    // non-governance address" test. The PAUSER_ROLE-gated pause/unpause
    // pair gets the same treatment.
    // ----------------------------------------------------------------------

    function test_setMinStake_revertsWithoutRole() public {
        _expectMissingRole(operator, bond.GOVERNANCE_ROLE());
        vm.prank(operator);
        bond.setMinStake(MIN_STAKE);
    }

    function test_setUnbondingPeriod_revertsWithoutRole() public {
        _expectMissingRole(operator, bond.GOVERNANCE_ROLE());
        vm.prank(operator);
        bond.setUnbondingPeriod(UNBONDING);
    }

    function test_setSafetyReserve_revertsWithoutRole() public {
        _expectMissingRole(operator, bond.GOVERNANCE_ROLE());
        vm.prank(operator);
        bond.setSafetyReserve(ISafetyReserve(address(safety)));
    }

    function test_setTreasury_revertsWithoutRole() public {
        _expectMissingRole(operator, bond.GOVERNANCE_ROLE());
        vm.prank(operator);
        bond.setTreasury(address(0xDEAD));
    }

    function test_setMultiaddrUpdateCooldown_revertsWithoutRole() public {
        _expectMissingRole(operator, bond.GOVERNANCE_ROLE());
        vm.prank(operator);
        bond.setMultiaddrUpdateCooldown(0);
    }

    function test_setMaxMultiaddrSize_revertsWithoutRole() public {
        _expectMissingRole(operator, bond.GOVERNANCE_ROLE());
        vm.prank(operator);
        bond.setMaxMultiaddrSize(512);
    }

    function test_setRegionStabilityWindow_revertsWithoutRole() public {
        _expectMissingRole(operator, bond.GOVERNANCE_ROLE());
        vm.prank(operator);
        bond.setRegionStabilityWindow(7 days);
    }

    function test_setClaimSlashGateEpochs_revertsWithoutRole() public {
        _expectMissingRole(operator, bond.GOVERNANCE_ROLE());
        vm.prank(operator);
        bond.setClaimSlashGateEpochs(13);
    }

    function test_pause_revertsWithoutRole() public {
        _expectMissingRole(operator, bond.PAUSER_ROLE());
        vm.prank(operator);
        bond.pause();
    }

    function test_unpause_revertsWithoutRole() public {
        bytes32 pauserRole = bond.PAUSER_ROLE();
        vm.prank(admin);
        bond.grantRole(pauserRole, admin);
        vm.prank(admin);
        bond.pause();

        _expectMissingRole(operator, pauserRole);
        vm.prank(operator);
        bond.unpause();
    }

    /// @dev Helper for AccessControl revert assertion. Reading the role
    ///      bytes32 BEFORE calling this helper is required so the
    ///      cheat-resolved STATICCALL doesn't consume the subsequent
    ///      `vm.prank` (the bug fixed in `test_setMinStake_revertsWithoutRole`
    ///      pre-merge). The helper itself only invokes a cheat code, which
    ///      does NOT consume the prank.
    function _expectMissingRole(address caller, bytes32 role) internal {
        vm.expectRevert(abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, caller, role));
    }

    function test_setSafetyReserve_revertsOnZeroAddress() public {
        vm.prank(admin);
        vm.expectRevert(CapacityBond.ZeroAddress.selector);
        bond.setSafetyReserve(ISafetyReserve(address(0)));
    }

    // ----------------------------------------------------------------------
    // slash() preconditions
    // ----------------------------------------------------------------------

    /// @notice `slash()` must revert when `safetyReserve` has not been wired
    ///         (ADR 016 § Post-Deployment Initialization, step 6). Without
    ///         this guard the 30% safety leg would `safeTransfer` to
    ///         `address(0)` and revert mid-flow, leaving inconsistent state.
    function test_slash_revertsWhenSafetyReserveUnset() public {
        // Fresh CapacityBond without `setSafetyReserve` wired.
        CapacityBond fresh = new CapacityBond({
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
        bytes32 slashRole = fresh.SLASH_ROLE();
        vm.startPrank(admin);
        fresh.grantRole(slashRole, admin);
        vm.expectRevert(CapacityBond.SafetyReserveNotWired.selector);
        fresh.slash(operator, challenger, 1);
        vm.stopPrank();
    }

    // ----------------------------------------------------------------------
    // registerNode preconditions
    // ----------------------------------------------------------------------

    function test_registerNode_revertsOnInvalidEd25519Sig() public {
        uint256 opPk = 0xDEADBEEF;
        address opAddr = vm.addr(opPk);
        vm.prank(admin);
        token.transfer(opAddr, MIN_STAKE);
        vm.prank(opAddr);
        token.approve(address(bond), type(uint256).max);
        vm.prank(opAddr);
        bond.stake(MIN_STAKE);

        // Flip the verifier to reject mode.
        ed25519.setAccept(false);

        bytes32 nodeId = bytes32(uint256(0xDEADBEEFDEADBEEF));
        bytes memory bindingSig = _signBindNode(opPk, opAddr, nodeId);
        vm.prank(opAddr);
        vm.expectRevert(CapacityBond.InvalidEd25519Signature.selector);
        bond.registerNode(nodeId, hex"", "us-east", bindingSig, hex"01");
    }

    function test_registerNode_revertsWhenAlreadyActive() public {
        uint256 opPk = 0xC0FFEE2;
        address opAddr = vm.addr(opPk);
        vm.prank(admin);
        token.transfer(opAddr, MIN_STAKE);
        vm.prank(opAddr);
        token.approve(address(bond), type(uint256).max);
        vm.prank(opAddr);
        bond.stake(MIN_STAKE);

        bytes32 nodeId = bytes32(uint256(0xC0FFEE2C0FFEE2));
        bytes memory bindingSig = _signBindNode(opPk, opAddr, nodeId);
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", bindingSig, hex"01");

        // Second registration without deregistering — bindingNonce has moved
        // on, so we re-sign with the new nonce to isolate the failure to the
        // `NodeAlreadyRegistered` guard rather than `InvalidBindingSignature`.
        bytes memory bindingSig2 = _signBindNode(opPk, opAddr, nodeId);
        vm.prank(opAddr);
        vm.expectRevert(CapacityBond.NodeAlreadyRegistered.selector);
        bond.registerNode(nodeId, hex"", "us-east", bindingSig2, hex"01");
    }

    function test_updateMultiaddrs_revertsWhenNodeNotActive() public {
        // Operator never registered a node — `_nodes[op].active` is false.
        vm.prank(operator);
        vm.expectRevert(CapacityBond.NodeNotActive.selector);
        bond.updateMultiaddrs(hex"deadbeef");
    }

    function test_updateRegion_revertsWhenNodeNotActive() public {
        vm.prank(operator);
        vm.expectRevert(CapacityBond.NodeNotActive.selector);
        bond.updateRegion("eu-west");
    }

    /// @dev Construct the EIP-712 `BindNode(bytes32 nodeId, uint64 nonce)`
    ///      digest used by `_verifyBindingSignature` and ECDSA-sign it with
    ///      `opPk`. Reads the current nonce off the contract so the helper
    ///      works for both the initial bind and any subsequent rebind.
    function _signBindNode(uint256 opPk, address opAddr, bytes32 nodeId) internal view returns (bytes memory) {
        uint64 nonce = bond.bindingNonce(opAddr);
        bytes32 structHash = keccak256(abi.encode(bond.BIND_NODE_TYPEHASH(), nodeId, nonce));
        bytes32 domainSeparator = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes("CapacityBond")),
                keccak256(bytes("1")),
                block.chainid,
                address(bond)
            )
        );
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", domainSeparator, structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(opPk, digest);
        return abi.encodePacked(r, s, v);
    }
}
