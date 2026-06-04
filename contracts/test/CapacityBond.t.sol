// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { CapacityBond } from "../src/CapacityBond.sol";
import { SlashStatus, SlashRecord } from "../src/SlashEscrowLib.sol";
import { BondMath } from "../src/BondMath.sol";
import { Token } from "../src/Token.sol";

import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";
import { ISlashJudgeEvidenceView } from "../src/interfaces/ISlashJudgeEvidenceView.sol";
import { MockSlashJudgeEvidence } from "./mocks/MockSlashJudgeEvidence.sol";

/// @title CapacityBond smoke tests
/// @notice Minimal coverage of the new ADR 036/028/030/026-v2.2 surface:
///         `firstBondedAt`, `slashedAtEpoch`, escrow-on-slash (slash escrows
///         TOKEN; `finalizeUnappealedSlash` / the `SLASH_APPEAL_ROLE` settle
///         hooks resolve it). NodeId / region-attestation paths require a
///         signed registration helper and land with the broader integration
///         suite.
/// @dev    `admin` is granted `SLASH_APPEAL_ROLE` in `setUp` so escrow-hook
///         tests can drive `markAppealOpen` / `settleAppealUpheld` /
///         `settleAppealGranted` directly, standing in for the `SlashAppeal`
///         contract (whose full flow is covered in `SlashAppeal.t.sol`).
contract CapacityBondTest is Test {
    Token internal token;
    MockEd25519Verifier internal ed25519;
    CapacityBond internal bond;

    address internal admin = address(0xA11CE);
    address internal operator = address(0xB0B);
    address internal challenger = address(0xC4A11);

    uint256 internal constant MIN_BOND = 50_000e18;
    uint256 internal constant UNBONDING = 7 days;
    // 10 days in microseconds — a valid evidence-age ceiling (within [1d,30d])
    // that is also >= the [3d,30d] unbonding floor, so the mirror check and the
    // individual bound can be exercised independently.
    uint256 internal constant MOCK_EVIDENCE_AGE_US = 10 days * 1_000_000;

    function setUp() public {
        token = new Token(admin);
        ed25519 = new MockEd25519Verifier();

        bond = new CapacityBond({
            token_: token,
            ed25519Verifier_: ed25519,
            admin: admin,
            minBond_: MIN_BOND,
            unbondingPeriod_: UNBONDING,
            multiaddrUpdateCooldown_: 0,
            maxMultiaddrSize_: 1024,
            regionStabilityWindow_: 7 days
        });

        vm.startPrank(admin);
        bond.grantRole(bond.SLASH_ROLE(), admin);
        bond.grantRole(bond.SLASH_APPEAL_ROLE(), admin);
        token.transfer(operator, 200_000e18);
        vm.stopPrank();

        vm.prank(operator);
        token.approve(address(bond), type(uint256).max);
    }

    /// @dev Bring the operator's active bond up to exactly the ADR 026
    ///      § Capacity-bond curve requirement `bondRequired(mbps)` so
    ///      `declareMbps(mbps)` passes the on-chain enforcement (issue #770).
    ///      Bonds only the deficit (and tops the operator up from `admin` only
    ///      for that deficit), so the helper is idempotent and never overshoots
    ///      — callers asserting "exactly at curve" boundaries stay precise even
    ///      if the operator already holds bond.
    function _bondForMbps(uint256 mbps) internal {
        uint256 need = bond.bondRequired(mbps);
        uint256 active = bond.activeBond(operator);
        if (active >= need) return;
        uint256 deficit = need - active;
        uint256 bal = token.balanceOf(operator);
        if (bal < deficit) {
            vm.prank(admin);
            token.transfer(operator, deficit - bal);
        }
        vm.prank(operator);
        bond.bond(deficit);
    }

    function test_firstBondedAt_setsOnFirstBond() public {
        assertEq(bond.firstBondedAt(operator), 0);
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.bond(MIN_BOND);
        assertEq(bond.firstBondedAt(operator), 1_000_000);
    }

    function test_firstBondedAt_immutableAcrossReBonds() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.bond(MIN_BOND);
        uint64 first = bond.firstBondedAt(operator);

        vm.warp(2_000_000);
        vm.prank(operator);
        bond.bond(10e18);
        assertEq(bond.firstBondedAt(operator), first);
    }

    function test_slashedAtEpoch_zeroBeforeSlash() public view {
        assertEq(bond.slashedAtEpoch(operator), 0);
    }

    function test_slash_stampsSlashedAtEpoch() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.bond(MIN_BOND);

        vm.warp(2_000_000);
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        // `slashedAtEpoch` stores `actualEpoch + 1` so an epoch-0 slash isn't
        // confused with the unslashed sentinel; expect the +1-offset stamp.
        uint64 expected = uint64(uint256(2_000_000) / bond.EPOCH_LENGTH()) + 1;
        assertEq(bond.slashedAtEpoch(operator), expected);
    }

    function test_escrowHooks_requireSlashAppealRole() public {
        _expectMissingRole(operator, bond.SLASH_APPEAL_ROLE());
        vm.prank(operator);
        bond.markAppealOpen(0);
    }

    /// A successful appeal (markAppealOpen → settleAppealGranted) clears the
    /// slash zero-out and refunds the operator's escrowed TOKEN.
    function test_settleAppealGranted_clearsAndRefunds() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(admin);
        (uint256 slashId, uint256 totalSlash) = bond.slash(operator, challenger, 1);
        assertGt(bond.slashedAtEpoch(operator), 0);
        assertEq(bond.escrowedTotal(), totalSlash);

        uint256 opBefore = token.balanceOf(operator);
        vm.startPrank(admin);
        bond.markAppealOpen(slashId);
        bond.settleAppealGranted(slashId);
        vm.stopPrank();

        assertEq(bond.slashedAtEpoch(operator), 0);
        assertEq(token.balanceOf(operator) - opBefore, totalSlash);
        assertEq(bond.escrowedTotal(), 0);
    }

    // ── Slashing-lifecycle coverage (issue #703) ────────────────────────────

    /// Slash the same operator 3× and assert the tier ladder escalates
    /// 5% → 15% → 50% (`SLASH_BPS_TIER_1/2/3`) driven by `lifetimeOffenseCount`.
    /// The reductions are pure active-bond math. Start at 160k so even after the
    /// 50% tier the active balance (64.6k) stays above the `minBond/2` (25k)
    /// auto-eject floor.
    function test_slash_tierEscalation_15then50pct() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.bond(160_000e18);

        // Tier 1: 5% of 160k = 8k.
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        assertEq(bond.lifetimeOffenseCount(operator), 1);
        assertEq(bond.activeBond(operator), 152_000e18);

        // Tier 2: 15% of 152k = 22.8k.
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        assertEq(bond.lifetimeOffenseCount(operator), 2);
        assertEq(bond.activeBond(operator), 129_200e18);

        // Tier 3 (3rd+ offense): 50% of 129.2k = 64.6k. Under escrow-on-slash
        // nothing is distributed here — the `Slashed` event reports the
        // escrowed total only; distribution happens at finality.
        uint256 challengerBefore = token.balanceOf(challenger);
        uint256 escrowBefore = bond.escrowedTotal();
        vm.expectEmit(true, true, false, true);
        emit CapacityBond.Slashed(operator, challenger, 1, 3, 64_600e18);
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        assertEq(bond.lifetimeOffenseCount(operator), 3);
        assertEq(bond.activeBond(operator), 64_600e18);
        // Escrow grew by the slashed total; challenger paid nothing yet.
        assertEq(bond.escrowedTotal() - escrowBefore, 64_600e18);
        assertEq(token.balanceOf(challenger), challengerBefore);
    }

    /// Tier-3 slash against an operator holding BOTH active and unbonding bond
    /// where `unbonding > active`, so the slash exhausts active first and taps
    /// unbonding for the remainder (the reachable `else` branch of
    /// `_reduceBondAtTier`). The literal remainder-clip inside that branch is
    /// unreachable here (it needs `tierBps > 100%`); see
    /// `test_reduceBondAtTier_remainderClip` for that path.
    function test_slash_tier3_mixedBond_exhaustsActiveTapsUnbonding() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.bond(100_000e18);

        // Bump lifetimeOffenseCount to 2 so the next slash lands at tier 3.
        vm.prank(admin);
        bond.slash(operator, challenger, 1); // 5% → 95k active
        vm.prank(admin);
        bond.slash(operator, challenger, 1); // 15% of 95k → 80.75k active
        assertEq(bond.activeBond(operator), 80_750e18);

        // Move most bond into unbonding so unbonding (60k) > active (20.75k).
        vm.prank(operator);
        bond.requestUnbond(60_000e18);
        assertEq(bond.activeBond(operator), 20_750e18);

        // Tier 3: totalAtRisk = 80.75k, slashAmount = 40.375k > active 20.75k.
        // Active is zeroed; remainder (19.625k) comes out of unbonding, leaving
        // 60k - 19.625k = 40.375k. No underflow, no clip.
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        assertEq(bond.lifetimeOffenseCount(operator), 3);
        assertEq(bond.activeBond(operator), 0);
        (uint256 unbondingAmt,) = bond.unbondingOf(operator);
        assertEq(unbondingAmt, 40_375e18);
        // Active fell to 0 (< minBond/2 = 25k), so the slash auto-ejected.
        assertTrue(bond.ejected(operator));
    }

    /// Directly exercise the C2 defensive remainder-clip in
    /// `BondMath.reduceAtTier` (the math behind `_reduceBondAtTier`). It is
    /// unreachable through `slash()` — the clip fires only when
    /// `slashAmount > totalAtRisk`, i.e. `tierBps > 10_000` (>100%), and the
    /// immutable ladder maxes at 5_000 (50%). Call the pure library with
    /// `tierBps = 12_000` to prove it caps `slashAmount` to the at-risk total
    /// (no over-transfer) and never underflows the unbonding pool.
    function test_reduceBondAtTier_remainderClip() public pure {
        // Asymmetric split so `slashAmount` isn't a coincidental round multiple:
        // totalAtRisk = 110e18; 120% → slashAmount would be 132e18 > totalAtRisk
        // → clip clamps remainder to unbonding (100e18) and re-derives
        // slashAmount to active + unbonding = 110e18 (the full at-risk pool).
        (uint256 slashed, uint256 newActive, uint256 newUnbonding) = BondMath.reduceAtTier(10e18, 100e18, 12_000);
        assertEq(slashed, 110e18); // capped to at-risk (10 + 100), not 132e18
        assertEq(newActive, 0);
        assertEq(newUnbonding, 0);
    }

    /// `BondMath.reduceAtTier` against an operator with NO active bond — the
    /// whole slash comes out of the unbonding bucket. Reachable in production
    /// when an operator fully unbonds and is then slashed, so an in-ladder tier
    /// (50%) is used. Active stays 0; unbonding is halved.
    function test_reduceBondAtTier_unbondingOnly() public pure {
        // totalAtRisk = 100e18; 50% = 50e18. active is already 0, so the entire
        // 50e18 comes from unbonding via the else branch (no clip).
        (uint256 slashed, uint256 newActive, uint256 newUnbonding) = BondMath.reduceAtTier(0, 100e18, 5000);
        assertEq(slashed, 50e18);
        assertEq(newActive, 0);
        assertEq(newUnbonding, 50e18);
    }

    /// The common production path: the slash fits entirely within active bond,
    /// so unbonding is untouched (the `slashAmount <= active` branch).
    function test_reduceBondAtTier_activeOnly() public pure {
        // totalAtRisk = 150e18; 50% = 75e18 ≤ active (100e18) → all from active.
        (uint256 slashed, uint256 newActive, uint256 newUnbonding) = BondMath.reduceAtTier(100e18, 50e18, 5000);
        assertEq(slashed, 75e18);
        assertEq(newActive, 25e18);
        assertEq(newUnbonding, 50e18); // untouched
    }

    /// Boundary: `slashAmount == active` exactly takes the `<=` branch, zeroing
    /// active and leaving unbonding whole (guards a future `<=` → `<` slip).
    function test_reduceBondAtTier_slashEqualsActive() public pure {
        // totalAtRisk = 200e18; 50% = 100e18 == active → active branch, exact.
        (uint256 slashed, uint256 newActive, uint256 newUnbonding) = BondMath.reduceAtTier(100e18, 100e18, 5000);
        assertEq(slashed, 100e18);
        assertEq(newActive, 0);
        assertEq(newUnbonding, 100e18); // untouched
    }

    /// Partial spill: slash exceeds active and takes the remainder from unbonding
    /// without clipping (the `else` no-clip branch with nonzero residual both).
    function test_reduceBondAtTier_spillsIntoUnbonding() public pure {
        // totalAtRisk = 300e18; 50% = 150e18 > active (100e18) → 50e18 spills
        // into unbonding (200e18), leaving 150e18 unbonding, no clip.
        (uint256 slashed, uint256 newActive, uint256 newUnbonding) = BondMath.reduceAtTier(100e18, 200e18, 5000);
        assertEq(slashed, 150e18);
        assertEq(newActive, 0);
        assertEq(newUnbonding, 150e18);
    }

    /// Degenerate inputs: zero balances and a zero tier are well-defined no-ops
    /// (slashAmount 0, balances unchanged) — pins the contract against a future
    /// rounding/divide change.
    function test_reduceBondAtTier_zeroInputs() public pure {
        (uint256 s0, uint256 a0, uint256 u0) = BondMath.reduceAtTier(0, 0, 5000);
        assertEq(s0, 0);
        assertEq(a0, 0);
        assertEq(u0, 0);

        (uint256 s1, uint256 a1, uint256 u1) = BondMath.reduceAtTier(100e18, 100e18, 0);
        assertEq(s1, 0);
        assertEq(a1, 100e18);
        assertEq(u1, 100e18);
    }

    /// Two outstanding slashes: granting the NEWER appeal must recompute the
    /// `slashedAtEpoch` watermark down to the OLDER still-standing slash, and
    /// only granting the older one too clears it to zero (issue #709).
    function test_settleAppealGranted_multiSlash_recomputesThenClears() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.bond(MIN_BOND);

        // Slash #0 (older), then Slash #1 (newer) one epoch later.
        vm.prank(admin);
        (uint256 s0,) = bond.slash(operator, challenger, 1);
        uint64 stamp0 = bond.slashedAtEpoch(operator);
        vm.warp(block.timestamp + 8 days);
        vm.prank(admin);
        (uint256 s1,) = bond.slash(operator, challenger, 1);
        assertTrue(bond.slashedAtEpoch(operator) != stamp0);

        vm.startPrank(admin);
        // Grant the newer → watermark falls back to the older standing slash.
        bond.markAppealOpen(s1);
        bond.settleAppealGranted(s1);
        assertEq(bond.slashedAtEpoch(operator), stamp0);

        // Grant the older too → no slash stands → watermark clears to zero,
        // surfaced on-chain as the `SlashedAtEpochStamped(op, 0)` clear sentinel
        // (the dedicated `SlashedAtEpochCleared` event was removed) — assert the
        // event shape so off-chain vote-weight indexers stay locked to it.
        bond.markAppealOpen(s0);
        vm.expectEmit(true, false, false, true, address(bond));
        emit CapacityBond.SlashedAtEpochStamped(operator, 0);
        bond.settleAppealGranted(s0);
        vm.stopPrank();
        assertEq(bond.slashedAtEpoch(operator), 0);
    }

    /// Reversing a MIDDLE slash of three leaves the watermark at the newest
    /// standing slash: the tail-scan must skip the interior `Reversed` entry and
    /// keep the max, not fall back (issue #709 multi-slash recompute).
    function test_settleAppealGranted_reverseMiddleSlash_keepsNewest() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.bond(MIN_BOND);

        // Three slashes across distinct epochs: #0 (old), #1 (mid), #2 (new).
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        vm.warp(block.timestamp + 8 days);
        vm.prank(admin);
        (uint256 sMid,) = bond.slash(operator, challenger, 1);
        vm.warp(block.timestamp + 8 days);
        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        uint64 stampNewest = bond.slashedAtEpoch(operator);

        // Reverse the MIDDLE slash — the newest still stands, so the watermark
        // is unchanged (the scan skips the reversed interior entry).
        vm.startPrank(admin);
        bond.markAppealOpen(sMid);
        bond.settleAppealGranted(sMid);
        vm.stopPrank();
        assertEq(bond.slashedAtEpoch(operator), stampNewest);
    }

    function test_declaredMbps_storesValue() public {
        _bondForMbps(1000);
        vm.prank(operator);
        bond.declareMbps(1000);
        assertEq(bond.declaredMbps(operator), 1000);
    }

    // ADR 026 § Capacity-bond curve — declared-capacity band on `declareMbps`.

    function test_capacityBand_defaults() public view {
        assertEq(bond.minCapacityMbps(), 10);
        assertEq(bond.maxCapacityMbps(), 200_000);
    }

    function test_declareMbps_acceptsFloorAndCeiling() public {
        _bondForMbps(200_000);
        vm.startPrank(operator);
        bond.declareMbps(10);
        assertEq(bond.declaredMbps(operator), 10);
        bond.declareMbps(200_000);
        assertEq(bond.declaredMbps(operator), 200_000);
        vm.stopPrank();
    }

    function test_declareMbps_revertsBelowFloor() public {
        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.DeclaredCapacityOutOfBand.selector, 9, 10, 200_000));
        bond.declareMbps(9);
    }

    function test_declareMbps_revertsZero() public {
        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.DeclaredCapacityOutOfBand.selector, 0, 10, 200_000));
        bond.declareMbps(0);
    }

    function test_declareMbps_revertsAboveCeiling() public {
        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.DeclaredCapacityOutOfBand.selector, 200_001, 10, 200_000));
        bond.declareMbps(200_001);
    }

    function test_setMinCapacity_updatesBandAndEmits() public {
        vm.expectEmit(false, false, false, true, address(bond));
        emit CapacityBond.MinCapacityMbpsUpdated(10, 500);
        vm.prank(admin);
        bond.setMinCapacityMbps(500);
        assertEq(bond.minCapacityMbps(), 500);

        // 100 Mbps is now below the raised floor and reverts.
        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.DeclaredCapacityOutOfBand.selector, 100, 500, 200_000));
        bond.declareMbps(100);

        _bondForMbps(500);
        vm.prank(operator);
        bond.declareMbps(500);
        assertEq(bond.declaredMbps(operator), 500);
    }

    function test_setMaxCapacity_updatesBandAndEmits() public {
        vm.expectEmit(false, false, false, true, address(bond));
        emit CapacityBond.MaxCapacityMbpsUpdated(200_000, 50_000);
        vm.prank(admin);
        bond.setMaxCapacityMbps(50_000);
        assertEq(bond.maxCapacityMbps(), 50_000);

        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.DeclaredCapacityOutOfBand.selector, 60_000, 10, 50_000));
        bond.declareMbps(60_000);
    }

    function test_setMinCapacity_revertsOutOfBounds() public {
        vm.startPrank(admin);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, 9, 10, 1000));
        bond.setMinCapacityMbps(9);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, 1001, 10, 1000));
        bond.setMinCapacityMbps(1001);
        vm.stopPrank();
    }

    function test_setMaxCapacity_revertsOutOfBounds() public {
        vm.startPrank(admin);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, 49_999, 50_000, 1_000_000));
        bond.setMaxCapacityMbps(49_999);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, 1_000_001, 50_000, 1_000_000));
        bond.setMaxCapacityMbps(1_000_001);
        vm.stopPrank();
    }

    function test_setCapacity_onlyGovernance() public {
        bytes32 role = bond.GOVERNANCE_ROLE();
        vm.prank(operator);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, operator, role)
        );
        bond.setMinCapacityMbps(500);

        vm.prank(operator);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, operator, role)
        );
        bond.setMaxCapacityMbps(50_000);
    }

    // -----------------------------------------------------------------
    // ADR 026 § Capacity-bond curve — on-chain enforcement + governance
    // (issue #770). `bond_required(Mbps) = k × Mbps^α`, default k = 12.6
    // TOKEN, α = 1.2; the coupling `activeBond ≥ bondRequired(declaredMbps)`
    // is enforced at `declareMbps` / `requestUnbond` / `registerNode`.
    // -----------------------------------------------------------------

    function test_bondRequired_matchesAdrWorkedExamples() public view {
        // ADR 026 § Capacity-bond curve worked table (α = 1.2, k = 12.6),
        // checked within ±2% of the rounded ADR figures.
        assertApproxEqRel(bond.bondRequired(10), 200e18, 0.02e18, "10 Mbps");
        assertApproxEqRel(bond.bondRequired(1000), 50_000e18, 0.02e18, "1 Gbps");
        assertApproxEqRel(bond.bondRequired(10_000), 795_000e18, 0.02e18, "10 Gbps");
        assertApproxEqRel(bond.bondRequired(100_000), 12_600_000e18, 0.02e18, "100 Gbps");
    }

    function test_bondRequired_zeroForZeroMbps() public view {
        assertEq(bond.bondRequired(0), 0);
    }

    function test_bondRequired_strictlyMonotonic() public view {
        assertLt(bond.bondRequired(10), bond.bondRequired(100));
        assertLt(bond.bondRequired(100), bond.bondRequired(1000));
        assertLt(bond.bondRequired(1000), bond.bondRequired(10_000));
        assertLt(bond.bondRequired(10_000), bond.bondRequired(100_000));
    }

    /// @dev Monotonicity across the full declarable band (α=1.2 default is
    ///      strictly increasing), guarding powWad rounding plateaus / edge
    ///      cases the fixed points above miss.
    function testFuzz_bondRequired_monotonic(uint256 a, uint256 b) public view {
        a = bound(a, 1, 1_000_000);
        b = bound(b, 1, 1_000_000);
        if (a > b) (a, b) = (b, a);
        assertLe(bond.bondRequired(a), bond.bondRequired(b));
    }

    function test_declareMbps_revertsWhenBondBelowCurve() public {
        uint256 required = bond.bondRequired(1000);
        // Bond one wei short of the curve.
        vm.prank(admin);
        token.transfer(operator, required);
        vm.prank(operator);
        bond.bond(required - 1);

        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.BondBelowCurve.selector, required - 1, required));
        bond.declareMbps(1000);
    }

    function test_declareMbps_succeedsExactlyAtCurve() public {
        _bondForMbps(1000);
        vm.prank(operator);
        bond.declareMbps(1000);
        assertEq(bond.declaredMbps(operator), 1000);
    }

    function test_requestUnbond_revertsWhenItDropsBelowCurve() public {
        _bondForMbps(1000);
        vm.prank(operator);
        bond.declareMbps(1000);

        uint256 required = bond.bondRequired(1000);
        // Operator bonded exactly `required`; unbonding any amount drops below.
        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.BondBelowCurve.selector, required - 1, required));
        bond.requestUnbond(1);
    }

    function test_requestUnbond_allowedWhenNoCapacityDeclared() public {
        // declaredMbps defaults to 0 ⇒ bondRequired(0) == 0, so the curve does
        // not constrain unbonding.
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(operator);
        bond.requestUnbond(MIN_BOND / 2);
        assertEq(bond.activeBond(operator), MIN_BOND / 2);
    }

    function test_requestUnbond_succeedsDownToExactlyCurve() public {
        // Over-bond above the 1 Gbps curve, declare 1000, then unbond the slack
        // down to exactly bondRequired(1000) — the inclusive boundary succeeds.
        uint256 required = bond.bondRequired(1000);
        uint256 slack = 5000e18;
        vm.prank(admin);
        token.transfer(operator, required + slack);
        vm.startPrank(operator);
        bond.bond(required + slack);
        bond.declareMbps(1000);
        bond.requestUnbond(slack);
        vm.stopPrank();
        assertEq(bond.activeBond(operator), required);
    }

    function test_slash_doesNotEnforceCurve() public {
        // A slash may drop active bond below the curve; it must NOT revert
        // (slash paths are intentionally exempt — ADR 026 § Slashing and burn).
        _bondForMbps(1000);
        vm.prank(operator);
        bond.declareMbps(1000);

        vm.prank(admin);
        bond.slash(operator, challenger, 1);
        // Post-slash active bond is below bondRequired(1000); no revert occurred.
        assertLt(bond.activeBond(operator), bond.bondRequired(1000));
    }

    function test_registerNode_revertsWhenBondBelowCurve() public {
        uint256 opPk = 0xBADC0DE;
        address opAddr = vm.addr(opPk);

        // Bond exactly bondRequired(1000) and declare the 1 Gbps tier (passes).
        uint256 bonded = bond.bondRequired(1000);
        vm.prank(admin);
        token.transfer(opAddr, bonded);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(bonded);
        bond.declareMbps(1000);
        vm.stopPrank();

        // Governance raises k so the 1 Gbps requirement now exceeds the
        // (unchanged) bond, while the bond still clears minBond.
        vm.prank(admin);
        bond.setK(20e18);
        uint256 newRequired = bond.bondRequired(1000);
        assertGt(bonded, MIN_BOND); // still satisfies the minBond gate
        assertLt(bonded, newRequired); // but is now below the curve

        vm.warp(1_000_000);
        bytes32 nodeId = bytes32(uint256(0xDEAD));
        bytes memory bindingSig = _signBindNode(opPk, opAddr, nodeId);
        vm.prank(opAddr);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.BondBelowCurve.selector, bonded, newRequired));
        bond.registerNode(nodeId, hex"", "us-east", bindingSig, hex"01");
    }

    function test_registerNode_succeedsAtCurve() public {
        // The success side of the registerNode curve gate: declaredMbps > 0 and
        // bonded ≥ bondRequired(declaredMbps) registers cleanly.
        uint256 opPk = 0xF00D;
        address opAddr = vm.addr(opPk);

        uint256 bonded = bond.bondRequired(1000);
        vm.prank(admin);
        token.transfer(opAddr, bonded);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(bonded);
        bond.declareMbps(1000);
        vm.stopPrank();

        vm.warp(1_000_000);
        bytes32 nodeId = bytes32(uint256(0xF00DF00D));
        bytes memory bindingSig = _signBindNode(opPk, opAddr, nodeId);
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", bindingSig, hex"01");

        assertEq(bond.addressToNodeId(opAddr), nodeId);
        assertTrue(bond.isActive(opAddr));
    }

    function test_setK_updatesCurveAndEmits() public {
        uint256 oldK = bond.kConstant();
        vm.expectEmit(false, false, false, true, address(bond));
        emit CapacityBond.KUpdated(oldK, 20e18);
        vm.prank(admin);
        bond.setK(20e18);
        assertEq(bond.kConstant(), 20e18);
        // bondRequired now scales with the new k.
        assertEq(bond.bondRequired(1000), BondMath.bondRequired(1000, 20e18, 1.2e18));
    }

    function test_setAlpha_updatesCurveAndEmits() public {
        uint256 oldAlpha = bond.alphaWad();
        vm.expectEmit(false, false, false, true, address(bond));
        emit CapacityBond.AlphaUpdated(oldAlpha, 1e18);
        vm.prank(admin);
        bond.setAlpha(1e18);
        assertEq(bond.alphaWad(), 1e18);
        // α = 1.0 ⇒ linear: bondRequired(1000) = k × 1000 = 12_600 TOKEN.
        assertApproxEqRel(bond.bondRequired(1000), 12_600e18, 0.001e18);
    }

    function test_setAlpha_revertsOutOfBounds() public {
        vm.startPrank(admin);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, 0.9e18, 1e18, 1.8e18));
        bond.setAlpha(0.9e18);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, 1.9e18, 1e18, 1.8e18));
        bond.setAlpha(1.9e18);
        vm.stopPrank();
    }

    function test_setAlpha_revertsWhenGbpsTierOutOfRange() public {
        // α = 1.8 with default k = 12.6 pushes the 1 Gbps tier far above the
        // 200K-TOKEN ceiling, so the coupled bound rejects it.
        uint256 oldAlpha = bond.alphaWad();
        uint256 tierBond = BondMath.bondRequired(1000, 12.6e18, 1.8e18);
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, tierBond, 10_000e18, 200_000e18));
        bond.setAlpha(1.8e18);
        // Store-then-validate: the reverted setter's tentative write is rolled back.
        assertEq(bond.alphaWad(), oldAlpha);
    }

    function test_setK_revertsWhenGbpsTierOutOfRange() public {
        uint256 oldK = bond.kConstant();
        // k too low ⇒ 1 Gbps tier below the 10K floor.
        uint256 lowTier = BondMath.bondRequired(1000, 1e18, 1.2e18);
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, lowTier, 10_000e18, 200_000e18));
        bond.setK(1e18);

        // k too high ⇒ 1 Gbps tier above the 200K ceiling.
        uint256 highTier = BondMath.bondRequired(1000, 100e18, 1.2e18);
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, highTier, 10_000e18, 200_000e18));
        bond.setK(100e18);

        // Store-then-validate: neither reverted setter left a tentative write.
        assertEq(bond.kConstant(), oldK);
    }

    function test_setCurve_onlyGovernance() public {
        bytes32 role = bond.GOVERNANCE_ROLE();
        vm.startPrank(operator);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, operator, role)
        );
        bond.setK(20e18);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, operator, role)
        );
        bond.setAlpha(1e18);
        vm.stopPrank();
    }

    function test_curveDefaults() public view {
        assertEq(bond.kConstant(), 12.6e18);
        assertEq(bond.alphaWad(), 1.2e18);
    }

    /// @dev Locks the documented `setK`/`setAlpha` ordering hazard: each setter
    ///      validates the 1 Gbps tier against the *live* other coefficient, so a
    ///      valid target pair can be unreachable in the wrong order. Target
    ///      (k=180, α=1.0) ⇒ 1 Gbps = 180_000 TOKEN (in range), but k-first
    ///      passes through (k=180, α=1.2) ⇒ ~716K TOKEN (over the 200K ceiling).
    function test_setKAlpha_orderingMatters() public {
        vm.startPrank(admin);

        // k-first reverts at the out-of-range intermediate (k=180, α=1.2).
        uint256 badIntermediate = BondMath.bondRequired(1000, 180e18, 1.2e18);
        vm.expectRevert(
            abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, badIntermediate, 10_000e18, 200_000e18)
        );
        bond.setK(180e18);

        // α-first reaches the same target pair: each step stays in range.
        bond.setAlpha(1e18); // (k=12.6, α=1.0) ⇒ 12_600 TOKEN
        bond.setK(180e18); // (k=180, α=1.0) ⇒ 180_000 TOKEN
        vm.stopPrank();

        assertEq(bond.kConstant(), 180e18);
        assertEq(bond.alphaWad(), 1e18);
        assertApproxEqRel(bond.bondRequired(1000), 180_000e18, 0.001e18);
    }

    function test_declareMbps_emitsOldAndNewValue() public {
        _bondForMbps(500);
        vm.startPrank(operator);

        // First declaration: `old` is the zero default.
        vm.expectEmit(true, false, false, true, address(bond));
        emit CapacityBond.MbpsDeclared(operator, 0, 100);
        bond.declareMbps(100);

        // Re-declaration: `old` is the prior value, not zero.
        vm.expectEmit(true, false, false, true, address(bond));
        emit CapacityBond.MbpsDeclared(operator, 100, 500);
        bond.declareMbps(500);

        vm.stopPrank();
        assertEq(bond.declaredMbps(operator), 500);
    }

    function test_setMinCapacity_acceptsInclusiveBounds() public {
        vm.startPrank(admin);
        bond.setMinCapacityMbps(10); // floor of the [10, 1000] setter range
        assertEq(bond.minCapacityMbps(), 10);
        bond.setMinCapacityMbps(1000); // ceiling of the setter range
        assertEq(bond.minCapacityMbps(), 1000);
        vm.stopPrank();
    }

    function test_setMaxCapacity_acceptsInclusiveBounds() public {
        vm.startPrank(admin);
        bond.setMaxCapacityMbps(50_000); // floor of the [50_000, 1_000_000] setter range
        assertEq(bond.maxCapacityMbps(), 50_000);
        bond.setMaxCapacityMbps(1_000_000); // ceiling of the setter range
        assertEq(bond.maxCapacityMbps(), 1_000_000);
        vm.stopPrank();
    }

    /// @dev Guards the disjoint-range invariant that lets `declareMbps` skip a
    ///      cross-parameter `min < max` check: pushing the floor to its highest
    ///      governance-reachable value (MIN_CAPACITY_CEILING_MBPS) and the
    ///      ceiling to its lowest (MAX_CAPACITY_FLOOR_MBPS) must still leave
    ///      `min < max`. If a future edit relaxes those constants into overlap,
    ///      this trips.
    function test_capacityBand_floorAlwaysBelowCeiling() public {
        vm.startPrank(admin);
        bond.setMinCapacityMbps(1000); // MIN_CAPACITY_CEILING_MBPS
        bond.setMaxCapacityMbps(50_000); // MAX_CAPACITY_FLOOR_MBPS
        vm.stopPrank();
        assertLt(bond.minCapacityMbps(), bond.maxCapacityMbps());
    }

    function test_slashId_persistsRecord() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(admin);
        (uint256 slashId,) = bond.slash(operator, challenger, 1);
        assertEq(slashId, 0);
        (address op, uint64 ts, uint256 amount) = bond.slashRecords(0);
        assertEq(op, operator);
        assertGt(ts, 0);
        assertGt(amount, 0);
        assertEq(bond.slashCounter(), 1);
    }

    // ── Escrow-on-slash lifecycle (ADR 028) ─────────────────────────────────

    /// Slash escrows the TOKEN without distributing it: no challenger transfer,
    /// no burn, escrow accounting grows, status is `Escrowed`.
    function test_slash_escrowsWithoutDistribution() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);

        uint256 supplyBefore = token.totalSupply();
        uint256 challengerBefore = token.balanceOf(challenger);

        vm.prank(admin);
        (uint256 slashId, uint256 totalSlash) = bond.slash(operator, challenger, 1);

        assertEq(totalSlash, 2500e18); // 5% of 50k
        assertEq(bond.escrowedTotal(), totalSlash);
        assertEq(token.balanceOf(challenger), challengerBefore); // nothing paid yet
        assertEq(token.totalSupply(), supplyBefore); // nothing burned yet
        SlashRecord memory r = bond.getSlashRecord(slashId);
        assertEq(uint8(r.status), uint8(SlashStatus.Escrowed));
        assertEq(r.challenger, challenger);
    }

    /// Slashing an operator with zero active AND zero unbonding bond mints a
    /// clean zero-amount escrow record. The (now-removed) Genesis-credit slash
    /// leg used to make a fully-zero slash unreachable; with it gone this is a
    /// new reachable state, so pin that it stays safe — the offense counter,
    /// slash-epoch watermark, and auto-eject still fire, escrow stays flat, and
    /// the unappealed-finalize terminal path no-ops without moving any TOKEN.
    function test_slash_zeroBondOperator_mintsZeroAmountRecordNoOpsAtFinality() public {
        // Warp to epoch 2 (14d / 7d EPOCH_LENGTH) so we can assert the *exact*
        // +1-encoded watermark below. (The +1 encoding means even an epoch-0
        // slash stamps 1, so `assertGt(.., 0)` would be near-trivially true —
        // assert the precise value to actually pin the stamp arithmetic.)
        vm.warp(2 * 7 days);

        address noBond = address(0xDEAD11);
        assertEq(bond.activeBond(noBond), 0);

        uint256 supplyBefore = token.totalSupply();
        uint256 escrowBefore = bond.escrowedTotal();

        vm.prank(admin);
        (uint256 slashId, uint256 totalSlash) = bond.slash(noBond, challenger, 1);

        // Zero economic value, but the offense / watermark / eject side effects
        // still fire and the escrow record is well-formed.
        assertEq(totalSlash, 0);
        assertEq(bond.escrowedTotal(), escrowBefore);
        assertEq(bond.lifetimeOffenseCount(noBond), 1);
        assertEq(bond.slashedAtEpoch(noBond), 3); // epoch 2, +1-encoded
        assertTrue(bond.ejected(noBond));
        SlashRecord memory r = bond.getSlashRecord(slashId);
        assertEq(r.slashAmount, 0);
        assertEq(uint8(r.status), uint8(SlashStatus.Escrowed));

        // Unappealed finalize after the filing window no-ops: no transfer, no burn.
        vm.warp(block.timestamp + 30 days + 1);
        uint256 challengerBefore = token.balanceOf(challenger);
        bond.finalizeUnappealedSlash(slashId);
        assertEq(token.balanceOf(challenger), challengerBefore);
        assertEq(token.totalSupply(), supplyBefore);
        assertEq(bond.escrowedTotal(), escrowBefore);
        assertEq(uint8(bond.getSlashRecord(slashId).status), uint8(SlashStatus.Upheld));
    }

    /// A granted appeal on a zero-amount slash refunds nothing (no `safeTransfer`
    /// of a zero amount) and still clears the slash-epoch watermark.
    function test_settleAppealGranted_zeroBondSlash_refundsNothingClearsWatermark() public {
        vm.warp(2 * 7 days);
        address noBond = address(0xDEAD12);

        vm.prank(admin);
        (uint256 slashId, uint256 totalSlash) = bond.slash(noBond, challenger, 1);
        assertEq(totalSlash, 0);
        assertEq(bond.slashedAtEpoch(noBond), 3); // epoch 2, +1-encoded

        uint256 opBefore = token.balanceOf(noBond);
        vm.startPrank(admin);
        bond.markAppealOpen(slashId);
        // The reversal still emits over the zero-amount record (refund == 0), so
        // indexers see the state change even though no TOKEN moves.
        vm.expectEmit(true, true, false, true, address(bond));
        emit CapacityBond.SlashReversed(slashId, noBond, 0);
        bond.settleAppealGranted(slashId);
        vm.stopPrank();

        assertEq(token.balanceOf(noBond), opBefore); // refund == 0, nothing transferred
        assertEq(bond.escrowedTotal(), 0);
        assertEq(bond.slashedAtEpoch(noBond), 0); // watermark recomputed to "no standing slash"
        assertEq(uint8(bond.getSlashRecord(slashId).status), uint8(SlashStatus.Reversed));
    }

    /// No appeal filed → after the filing window anyone can finalize, paying
    /// 50% to the challenger and burning 50%.
    function test_finalizeUnappealedSlash_distributes5050() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(admin);
        (uint256 slashId, uint256 totalSlash) = bond.slash(operator, challenger, 1);

        // Too early: filing window still open.
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.FilingWindowStillOpen.selector, _windowClose(slashId)));
        bond.finalizeUnappealedSlash(slashId);

        vm.warp(block.timestamp + 30 days + 1);
        uint256 supplyBefore = token.totalSupply();
        uint256 challengerBefore = token.balanceOf(challenger);

        // Permissionless: a random address triggers finality.
        vm.prank(address(0xDEAD));
        bond.finalizeUnappealedSlash(slashId);

        assertEq(token.balanceOf(challenger) - challengerBefore, totalSlash / 2);
        assertEq(supplyBefore - token.totalSupply(), totalSlash - totalSlash / 2);
        assertEq(bond.escrowedTotal(), 0);
        assertEq(uint8(bond.getSlashRecord(slashId).status), uint8(SlashStatus.Upheld));
    }

    /// An opened appeal flips the escrow to `AppealOpen`, so the permissionless
    /// finalize path can no longer race it.
    function test_markAppealOpen_blocksFinalize() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(admin);
        (uint256 slashId,) = bond.slash(operator, challenger, 1);

        vm.prank(admin);
        bond.markAppealOpen(slashId);

        vm.warp(block.timestamp + 30 days + 1);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.SlashNotEscrowed.selector, slashId));
        bond.finalizeUnappealedSlash(slashId);
    }

    /// Upheld appeal distributes the escrow 50/50 (same as the no-appeal path).
    function test_settleAppealUpheld_distributes5050() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(admin);
        (uint256 slashId, uint256 totalSlash) = bond.slash(operator, challenger, 1);

        uint256 supplyBefore = token.totalSupply();
        uint256 challengerBefore = token.balanceOf(challenger);
        vm.startPrank(admin);
        bond.markAppealOpen(slashId);
        bond.settleAppealUpheld(slashId);
        vm.stopPrank();

        assertEq(token.balanceOf(challenger) - challengerBefore, totalSlash / 2);
        assertEq(supplyBefore - token.totalSupply(), totalSlash - totalSlash / 2);
        assertEq(bond.escrowedTotal(), 0);
    }

    /// @dev Read the recorded filing-window deadline for revert-arg assertions.
    function _windowClose(uint256 slashId) internal view returns (uint64) {
        return bond.getSlashRecord(slashId).appealWindowClose;
    }

    // ----------------------------------------------------------------------
    // ADR 030 — region self-attestation
    //
    // The full keygen → signed `registerNode` (real `Ed25519Verifier`) →
    // `updateRegion` cooldown/replay flow, plus the cross-contract eject path,
    // lives in `CapacityBondRegionE2E.t.sol` (it needs the production verifier
    // and the generated signing vector). `CapacityBondTest` deploys the mock
    // verifier, so only the no-signature unit branch (`NodeNotActive`) is
    // exercised here, plus the ADR 030 ripening-predicate read surface below.
    // ----------------------------------------------------------------------

    function test_regionGateActivatedAt_setAtConstruction() public {
        // Non-upgradeable fresh deploy == gate activation: the stamp is the
        // construction block.timestamp (ADR 030 § Region-stability window).
        vm.warp(4_242_424);
        CapacityBond fresh = new CapacityBond({
            token_: token,
            ed25519Verifier_: ed25519,
            admin: admin,
            minBond_: MIN_BOND,
            unbondingPeriod_: UNBONDING,
            multiaddrUpdateCooldown_: 0,
            maxMultiaddrSize_: 1024,
            regionStabilityWindow_: 7 days
        });
        assertEq(fresh.regionGateActivatedAt(), 4_242_424);
    }

    function test_regionScopeData_returnsRegisteredNodeInputs() public {
        uint256 opPk = 0xF00D;
        address opAddr = vm.addr(opPk);

        uint256 bonded = bond.bondRequired(1000);
        vm.prank(admin);
        token.transfer(opAddr, bonded);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(bonded);
        bond.declareMbps(1000);
        vm.stopPrank();

        bytes32 nodeId = bytes32(uint256(0xF00DF00D));
        bytes memory bindingSig = _signBindNode(opPk, opAddr, nodeId);
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", bindingSig, hex"01");

        (
            string memory regionHint,
            string memory regionPrev,
            uint64 regionLastChanged,
            uint64 firstBondedAt,
            uint64 gateActivatedAt,
            uint256 window
        ) = bond.regionScopeData(opAddr);

        assertEq(regionHint, "us-east");
        assertEq(regionPrev, ""); // never changed
        assertEq(regionLastChanged, 0); // never changed
        assertEq(firstBondedAt, bond.firstBondedAt(opAddr));
        assertEq(gateActivatedAt, bond.regionGateActivatedAt());
        assertEq(window, bond.regionStabilityWindow());
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

    function test_setMinBond_revertsWithoutRole() public {
        _expectMissingRole(operator, bond.GOVERNANCE_ROLE());
        vm.prank(operator);
        bond.setMinBond(MIN_BOND);
    }

    function test_setUnbondingPeriod_revertsWithoutRole() public {
        _expectMissingRole(operator, bond.GOVERNANCE_ROLE());
        vm.prank(operator);
        bond.setUnbondingPeriod(UNBONDING);
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
    ///      `vm.prank` (the bug fixed in `test_setMinBond_revertsWithoutRole`
    ///      pre-merge). The helper itself only invokes a cheat code, which
    ///      does NOT consume the prank.
    function _expectMissingRole(address caller, bytes32 role) internal {
        vm.expectRevert(abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, caller, role));
    }

    // ----------------------------------------------------------------------
    // registerNode preconditions
    // ----------------------------------------------------------------------

    function test_registerNode_revertsOnInvalidEd25519Sig() public {
        uint256 opPk = 0xDEADBEEF;
        address opAddr = vm.addr(opPk);
        vm.prank(admin);
        token.transfer(opAddr, MIN_BOND);
        vm.prank(opAddr);
        token.approve(address(bond), type(uint256).max);
        vm.prank(opAddr);
        bond.bond(MIN_BOND);

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
        token.transfer(opAddr, MIN_BOND);
        vm.prank(opAddr);
        token.approve(address(bond), type(uint256).max);
        vm.prank(opAddr);
        bond.bond(MIN_BOND);

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

    // -----------------------------------------------------------------
    // setSlashJudge (ADR 014 § Interaction with unbonding period)
    // -----------------------------------------------------------------

    function test_setSlashJudge_revertsWithoutRole() public {
        MockSlashJudgeEvidence judge = new MockSlashJudgeEvidence(MOCK_EVIDENCE_AGE_US);
        _expectMissingRole(operator, bond.GOVERNANCE_ROLE());
        vm.prank(operator); // not GOVERNANCE_ROLE
        bond.setSlashJudge(ISlashJudgeEvidenceView(address(judge)));
    }

    function test_setSlashJudge_revertsOnZeroAddress() public {
        vm.prank(admin);
        vm.expectRevert(CapacityBond.ZeroAddress.selector);
        bond.setSlashJudge(ISlashJudgeEvidenceView(address(0)));
    }

    function test_setSlashJudge_setsAndEmits() public {
        // Judge's maxEvidenceAgeUs (5 days*1e6) is strictly below the bond's
        // unbondingPeriod*1e6 (7 days*1e6), so the wire-time invariant guard passes.
        MockSlashJudgeEvidence judge = new MockSlashJudgeEvidence(uint256(5 days) * 1_000_000);
        vm.prank(admin);
        vm.expectEmit(true, true, false, false, address(bond));
        emit CapacityBond.SlashJudgeUpdated(address(0), address(judge));
        bond.setSlashJudge(ISlashJudgeEvidenceView(address(judge)));
        assertEq(address(bond.slashJudge()), address(judge));
    }

    function test_setSlashJudge_rejectsJudgeViolatingInvariant() public {
        // bond's unbondingPeriod is 7 days. A judge claiming maxEvidenceAgeUs == 7d*1e6
        // would violate the strict invariant (7d*1e6 <= 7d*1e6) -> wire must revert.
        MockSlashJudgeEvidence badJudge = new MockSlashJudgeEvidence(uint256(7 days) * 1_000_000);
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                CapacityBond.UnbondingBelowEvidenceAge.selector,
                uint256(7 days) * 1_000_000,
                uint256(7 days) * 1_000_000
            )
        );
        bond.setSlashJudge(ISlashJudgeEvidenceView(address(badJudge)));
    }

    function test_setSlashJudge_acceptsJudgeSatisfyingInvariant() public {
        // maxEvidenceAgeUs = 5 days*1e6 < 7 days*1e6 -> satisfies invariant, wire succeeds.
        MockSlashJudgeEvidence okJudge = new MockSlashJudgeEvidence(uint256(5 days) * 1_000_000);
        vm.prank(admin);
        bond.setSlashJudge(ISlashJudgeEvidenceView(address(okJudge)));
        assertEq(address(bond.slashJudge()), address(okJudge));
    }

    function test_setSlashJudge_rewireEmitsPreviousJudgeAsOld() public {
        // Re-wiring a second judge must emit the FIRST judge as `old` (not address(0)).
        // Both maxEvidenceAgeUs values (4d, 5d *1e6) are strictly below the bond's
        // unbondingPeriod*1e6 (7 days*1e6), so both wires satisfy the invariant.
        MockSlashJudgeEvidence first = new MockSlashJudgeEvidence(uint256(4 days) * 1_000_000);
        MockSlashJudgeEvidence second = new MockSlashJudgeEvidence(uint256(5 days) * 1_000_000);
        vm.startPrank(admin);
        bond.setSlashJudge(ISlashJudgeEvidenceView(address(first)));
        vm.expectEmit(true, true, false, false, address(bond));
        emit CapacityBond.SlashJudgeUpdated(address(first), address(second));
        bond.setSlashJudge(ISlashJudgeEvidenceView(address(second)));
        vm.stopPrank();
        assertEq(address(bond.slashJudge()), address(second));
    }

    // -----------------------------------------------------------------
    // setUnbondingPeriod x maxEvidenceAgeUs mirror invariant (ADR 014)
    // -----------------------------------------------------------------

    function test_setUnbondingPeriod_revertsWhenEqualToEvidenceAge() public {
        MockSlashJudgeEvidence judge = new MockSlashJudgeEvidence(MOCK_EVIDENCE_AGE_US); // 10 days us
        vm.startPrank(admin);
        // Raise unbonding to 20 days first so the 10-day judge satisfies the
        // wire-time invariant (20d*1e6 > 10d*1e6) and can be wired.
        bond.setUnbondingPeriod(20 days);
        bond.setSlashJudge(ISlashJudgeEvidenceView(address(judge)));
        // 10 days * 1e6 == MOCK_EVIDENCE_AGE_US -> must revert (strict `<` invariant).
        vm.expectRevert(
            abi.encodeWithSelector(
                CapacityBond.UnbondingBelowEvidenceAge.selector, uint256(10 days) * 1_000_000, MOCK_EVIDENCE_AGE_US
            )
        );
        bond.setUnbondingPeriod(10 days);
        vm.stopPrank();
    }

    function test_setUnbondingPeriod_revertsWhenBelowEvidenceAge() public {
        MockSlashJudgeEvidence judge = new MockSlashJudgeEvidence(MOCK_EVIDENCE_AGE_US); // 10 days us
        vm.startPrank(admin);
        // Raise unbonding to 20 days first so the 10-day judge can be wired.
        bond.setUnbondingPeriod(20 days);
        bond.setSlashJudge(ISlashJudgeEvidenceView(address(judge)));
        // 9 days is within [3d,30d] but 9d*1e6 < 10d*1e6 -> mirror check reverts.
        vm.expectRevert(
            abi.encodeWithSelector(
                CapacityBond.UnbondingBelowEvidenceAge.selector, uint256(9 days) * 1_000_000, MOCK_EVIDENCE_AGE_US
            )
        );
        bond.setUnbondingPeriod(9 days);
        vm.stopPrank();
    }

    function test_setUnbondingPeriod_succeedsAboveEvidenceAge() public {
        MockSlashJudgeEvidence judge = new MockSlashJudgeEvidence(MOCK_EVIDENCE_AGE_US); // 10 days us
        vm.startPrank(admin);
        // Raise unbonding to 20 days first so the 10-day judge can be wired.
        bond.setUnbondingPeriod(20 days);
        bond.setSlashJudge(ISlashJudgeEvidenceView(address(judge)));
        // 11 days: within [3d,30d] AND 11d*1e6 > 10d*1e6 -> passes both checks.
        bond.setUnbondingPeriod(11 days);
        vm.stopPrank();
        assertEq(bond.unbondingPeriod(), 11 days);
    }

    function test_setUnbondingPeriod_boundStillEnforcedWithJudge() public {
        MockSlashJudgeEvidence judge = new MockSlashJudgeEvidence(MOCK_EVIDENCE_AGE_US);
        vm.startPrank(admin);
        // Raise unbonding to 20 days first so the 10-day judge can be wired.
        bond.setUnbondingPeriod(20 days);
        bond.setSlashJudge(ISlashJudgeEvidenceView(address(judge)));
        // 61 days exceeds UNBONDING_PERIOD_CEILING (30 days) -> the bound check reverts FIRST.
        vm.expectRevert(
            abi.encodeWithSelector(
                CapacityBond.ParamOutOfBounds.selector, uint256(61 days), uint256(3 days), uint256(30 days)
            )
        );
        bond.setUnbondingPeriod(61 days);
        vm.stopPrank();
    }

    function test_setUnbondingPeriod_unwiredAppliesOnlyBound() public {
        // No setSlashJudge call: slashJudge == address(0).
        assertEq(address(bond.slashJudge()), address(0));
        vm.prank(admin);
        bond.setUnbondingPeriod(8 days); // within [3d,30d], no judge -> succeeds
        assertEq(bond.unbondingPeriod(), 8 days);
    }

    function test_setUnbondingPeriod_floorWithLowEvidenceAgeSucceeds() public {
        // Tightest valid corner: unbonding at the floor (UNBONDING_PERIOD_FLOOR == 3 days)
        // with a 1-day evidence-age judge. The fixture bond starts at UNBONDING (7 days),
        // so wiring a 1-day judge passes (7d*1e6 > 1d*1e6); lowering to 3 days then passes
        // both the [3d,30d] bound (3 days is the floor) and the mirror (3d*1e6 > 1d*1e6).
        MockSlashJudgeEvidence judge = new MockSlashJudgeEvidence(uint256(1 days) * 1_000_000);
        vm.startPrank(admin);
        bond.setSlashJudge(ISlashJudgeEvidenceView(address(judge)));
        bond.setUnbondingPeriod(3 days); // floor; 3d*1e6 > 1d*1e6 and within [3d,30d]
        vm.stopPrank();
        assertEq(bond.unbondingPeriod(), 3 days);
    }
}
