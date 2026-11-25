// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Vm } from "forge-std/Vm.sol";

import { CapacityBond } from "../src/CapacityBond.sol";
import { SunsettingPausable } from "../src/SunsettingPausable.sol";
import { BondMath } from "../src/BondMath.sol";
import { Token } from "../src/Token.sol";

import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";

import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";

/// @title CapacityBond unit tests
/// @notice Coverage of the bond / unbond / capacity-curve surface, the node
///         registry, the blacklist-ejection latch, and the ADR 036 / 030 reads
///         (`firstBondedAt`, `declaredMbpsAtEpoch`, `regionScopeData`).
/// @dev    `admin` is granted `BLACKLIST_ROLE` in `setUp` so ejection tests can
///         drive `ejectNode` / `unEjectNode` directly, standing in for the
///         `ContentBlacklist` contract.
contract CapacityBondTest is Test {
    Token internal token;
    MockEd25519Verifier internal ed25519;
    CapacityBond internal bond;

    address internal admin = address(0xA11CE);
    address internal operator = address(0xB0B);

    uint256 internal constant MIN_BOND = 50_000e18;
    // ADR 019 § Terms Acceptance — genesis operator-terms hash the fixture
    // registers against (stand-in for `keccak256(TERMS.md)`).
    bytes32 internal constant TERMS_HASH = keccak256("decdn operator terms v1");

    function setUp() public {
        token = new Token(admin);
        ed25519 = new MockEd25519Verifier();

        bond = new CapacityBond({
            token_: token,
            ed25519Verifier_: ed25519,
            admin: admin,
            minBond_: MIN_BOND,
            multiaddrUpdateCooldown_: 0,
            maxMultiaddrSize_: 1024,
            regionStabilityWindow_: 7 days,
            currentTermsHash_: TERMS_HASH
        });

        vm.startPrank(admin);
        bond.grantRole(bond.BLACKLIST_ROLE(), admin);
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

    /// @notice A dust bond below `minBond` does not start the tenure ramp; the
    ///         bond that reaches `minBond` does.
    function test_firstBondedAt_setsAtMinBondCrossingNotDust() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.bond(1);
        assertEq(bond.firstBondedAt(operator), 0, "a dust bond stamps no tenure");

        vm.warp(2_000_000);
        vm.prank(operator);
        bond.bond(MIN_BOND - 1);
        assertEq(bond.firstBondedAt(operator), 2_000_000, "the minBond crossing stamps tenure");
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

    // ── Tenure reset on exit ────────────────────────────────────────────────
    // `firstBondedAt` measures continuous bonding at or above the floor. An
    // `unbond` below `bondFloorAtActivation` resets it and the floor to 0, and
    // the next entry stamps both afresh, so an address cannot be pre-aged by
    // bonding `minBond` and withdrawing it at once.

    /// @notice A full unbond ends the tenure; the next entry stamps a fresh one.
    function test_firstBondedAt_fullUnbondResetsAndReentryRestamps() public {
        vm.warp(1_000_000);
        vm.startPrank(operator);
        bond.bond(MIN_BOND);
        assertEq(bond.firstBondedAt(operator), 1_000_000);

        bond.unbond(MIN_BOND);
        assertEq(bond.firstBondedAt(operator), 0, "a full unbond ends the tenure");
        assertEq(bond.bondFloorAtActivation(operator), 0, "and clears the floor");

        vm.warp(5_000_000);
        bond.bond(MIN_BOND);
        vm.stopPrank();
        assertEq(bond.firstBondedAt(operator), 5_000_000, "re-entry stamps a fresh tenure");
        assertEq(bond.bondFloorAtActivation(operator), MIN_BOND, "re-entry records the live minBond");
    }

    /// @notice A partial unbond that leaves the bond exactly at the floor keeps
    ///         the tenure and the floor; one wei more ends both.
    function test_firstBondedAt_partialUnbondHoldingFloorKeepsTenure() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.bond(2 * MIN_BOND);

        vm.warp(2_000_000);
        vm.prank(operator);
        bond.unbond(MIN_BOND);
        assertEq(bond.activeBond(operator), MIN_BOND, "the bond sits exactly at the floor");
        assertEq(bond.firstBondedAt(operator), 1_000_000, "an unbond that holds the floor keeps the tenure");
        assertEq(bond.bondFloorAtActivation(operator), MIN_BOND, "and the floor");

        vm.prank(operator);
        bond.unbond(1);
        assertEq(bond.firstBondedAt(operator), 0, "one wei below the floor ends the tenure");
        assertEq(bond.bondFloorAtActivation(operator), 0, "and clears the floor");
    }

    /// @notice While a tenure runs, no bond moves its stamp or its floor:
    ///         neither a top-up above the floor nor a top-up through a raised
    ///         `minBond`.
    function test_firstBondedAt_reBondDuringTenureNeverMovesIt() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.bond(MIN_BOND);

        vm.warp(2_000_000);
        vm.prank(operator);
        bond.bond(10e18);
        assertEq(bond.firstBondedAt(operator), 1_000_000, "a top-up above the floor keeps the stamp");

        vm.prank(admin);
        bond.setMinBond(2 * MIN_BOND);
        vm.warp(3_000_000);
        vm.prank(operator);
        bond.bond(MIN_BOND); // 50k + 10 + 50k tops up through the raised 100k minBond
        assertEq(bond.firstBondedAt(operator), 1_000_000, "a top-up through a raised minBond keeps the stamp");
        assertEq(bond.bondFloorAtActivation(operator), MIN_BOND, "and the grandfathered floor");
    }

    /// @notice An operator that exits fully and re-enters after a `minBond`
    ///         raise re-grandfathers at the raised value with a fresh tenure.
    function test_firstBondedAt_reentryAfterMinBondRaiseRegrandfathers() public {
        vm.warp(1_000_000);
        vm.startPrank(operator);
        bond.bond(MIN_BOND);
        bond.unbond(MIN_BOND);
        vm.stopPrank();

        vm.prank(admin);
        bond.setMinBond(2 * MIN_BOND);

        vm.warp(2_000_000);
        vm.prank(operator);
        bond.bond(2 * MIN_BOND);
        assertEq(bond.firstBondedAt(operator), 2_000_000, "re-entry stamps the new tenure");
        assertEq(bond.bondFloorAtActivation(operator), 2 * MIN_BOND, "re-entry grandfathers the raised minBond");
    }

    /// @notice An operator whose dip ended its tenure but whose balance still
    ///         clears a since-lowered `minBond` re-enters with its next bond of
    ///         any size, at the lowered floor.
    function test_firstBondedAt_reentryAboveLoweredMinBondStampsOnNextBond() public {
        vm.prank(admin);
        bond.setMinBond(2 * MIN_BOND);
        vm.warp(1_000_000);
        vm.startPrank(operator);
        bond.bond(2 * MIN_BOND);
        bond.unbond(MIN_BOND / 2); // 75k < 100k floor
        vm.stopPrank();
        assertEq(bond.firstBondedAt(operator), 0, "setup: the dip ends the tenure");

        vm.prank(admin);
        bond.setMinBond(MIN_BOND); // 75k now clears the live minBond

        vm.warp(2_000_000);
        vm.prank(operator);
        bond.bond(1);
        assertEq(bond.firstBondedAt(operator), 2_000_000, "the next bond re-enters");
        assertEq(bond.bondFloorAtActivation(operator), MIN_BOND, "at the lowered minBond");
    }

    /// @notice The pre-aging attack: bond `minBond` and withdraw it at once on
    ///         many addresses in one block. No address keeps a tenure.
    function test_firstBondedAt_bondUnbondLoopCannotPreAgeAddresses() public {
        vm.warp(1_000_000);
        for (uint256 i = 1; i <= 5; i++) {
            address sybil = address(uint160(0x5B1100 + i));
            vm.prank(admin);
            token.transfer(sybil, MIN_BOND);
            vm.startPrank(sybil);
            token.approve(address(bond), type(uint256).max);
            bond.bond(MIN_BOND);
            bond.unbond(MIN_BOND);
            vm.stopPrank();
        }
        for (uint256 i = 1; i <= 5; i++) {
            address sybil = address(uint160(0x5B1100 + i));
            assertEq(bond.firstBondedAt(sybil), 0, "a bond/unbond round trip leaves no tenure");
            assertEq(bond.bondFloorAtActivation(sybil), 0, "nor a floor");
            assertEq(token.balanceOf(sybil), MIN_BOND, "the round trip returns the bond");
        }
    }

    // ── Blacklist-ejection latch (issue #850) ───────────────────────────────
    // Governance blacklisting (ADR 011) sets `ejected` through `ejectNode`, and
    // `blacklistEjected` latches it so `bond()` can't clear it. `unEjectNode`
    // releases only the latch; the next `bond()` that holds `minBond` then
    // clears `ejected`.

    /// @notice Core bug lock-in: a governance-blacklisted operator cannot
    ///         self-reinstate by re-bonding above `minBond`.
    function test_blacklistEjected_cannotSelfReinstateViaBond() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);

        vm.prank(admin);
        bond.ejectNode(operator);
        assertTrue(bond.ejected(operator));
        assertTrue(bond.blacklistEjected(operator));
        assertFalse(bond.isActive(operator));

        // Re-bond well above `minBond` — the reinstatement path must NOT fire
        // while the governance latch is set.
        vm.prank(operator);
        bond.bond(MIN_BOND);
        assertTrue(bond.ejected(operator)); // still ejected — no self-reinstate
        assertTrue(bond.blacklistEjected(operator));
        assertFalse(bond.isActive(operator));
    }

    /// @notice Governance lifting the blacklist clears only the latch; the
    ///         operator re-enters through the normal re-bond reinstatement.
    function test_unEjectNode_clearsBlacklistLatch_thenRebondReinstates() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(admin);
        bond.ejectNode(operator);

        vm.expectEmit(true, false, false, false, address(bond));
        emit CapacityBond.BlacklistEjectionCleared(operator);
        vm.prank(admin);
        bond.unEjectNode(operator);
        assertFalse(bond.blacklistEjected(operator));
        assertTrue(bond.ejected(operator)); // latch cleared, master gate not

        // A re-bond (balance already ≥ minBond) now reinstates via `bond()`.
        vm.expectEmit(true, false, false, false, address(bond));
        emit CapacityBond.Reinstated(operator);
        vm.prank(operator);
        bond.bond(1e18);
        assertFalse(bond.ejected(operator));
    }

    /// @notice An ejected operator that unbonds below its floor loses its
    ///         tenure. After `unEjectNode`, the bond that holds `minBond` both
    ///         reinstates it and starts a fresh tenure.
    function test_unEjectNode_afterFloorDip_rebondReinstatesWithFreshTenure() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(admin);
        bond.ejectNode(operator);

        vm.prank(operator);
        bond.unbond(MIN_BOND);
        assertEq(bond.firstBondedAt(operator), 0, "the dip ends the tenure");
        assertEq(bond.bondFloorAtActivation(operator), 0, "and clears the floor");

        vm.prank(admin);
        bond.unEjectNode(operator);
        assertTrue(bond.ejected(operator), "setup: the latch is cleared, the gate is not");

        vm.warp(2_000_000);
        vm.expectEmit(true, false, false, false, address(bond));
        emit CapacityBond.Reinstated(operator);
        vm.prank(operator);
        bond.bond(MIN_BOND);
        assertFalse(bond.ejected(operator), "the bond that holds minBond reinstates");
        assertEq(bond.firstBondedAt(operator), 2_000_000, "and stamps a fresh tenure");
        assertEq(bond.bondFloorAtActivation(operator), MIN_BOND, "at the live minBond");
    }

    function test_unEjectNode_onlyBlacklistRole() public {
        _expectMissingRole(operator, bond.BLACKLIST_ROLE());
        vm.prank(operator);
        bond.unEjectNode(operator);
    }

    /// @notice Un-ejecting an operator that was never blacklist-ejected is a
    ///         clean no-op (guarded; no revert, no state flip).
    function test_unEjectNode_idempotentNoState() public {
        vm.prank(admin);
        bond.unEjectNode(operator);
        assertFalse(bond.blacklistEjected(operator));
        assertFalse(bond.ejected(operator));
    }

    /// @notice The latch is set even when the operator is ALREADY ejected
    ///         (an earlier blacklist, lifted by `unEjectNode` but not yet
    ///         reinstated by `bond`) — proves `blacklistEjected` is written
    ///         outside the `if (!ejected)` one-time-effects guard in `ejectNode`.
    function test_ejectNode_setsBlacklistLatch_evenWhenAlreadyEjected() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);

        vm.startPrank(admin);
        bond.ejectNode(operator);
        bond.unEjectNode(operator);
        vm.stopPrank();
        assertTrue(bond.ejected(operator));
        assertFalse(bond.blacklistEjected(operator));

        vm.prank(admin);
        bond.ejectNode(operator);
        assertTrue(bond.blacklistEjected(operator));

        // Re-bond above `minBond` still cannot reinstate.
        vm.prank(operator);
        bond.bond(MIN_BOND);
        assertTrue(bond.ejected(operator));
    }

    /// @notice End-to-end security property via the REAL gates: a registered,
    ///         active operator who is blacklisted flips `isActive` to false and
    ///         cannot re-register (`registerNode` reverts `OperatorEjected`)
    ///         even after re-bonding above the curve. The `isActive` assertion
    ///         here is load-bearing (the node WAS active), unlike the latch
    ///         unit tests where the operator never registered.
    function test_blacklistEjected_registerNodeBarred_isActiveFlips() public {
        uint256 opPk = 0xBEEF1234;
        address opAddr = vm.addr(opPk);

        uint256 bonded = bond.bondRequired(1000);
        vm.prank(admin);
        token.transfer(opAddr, bonded * 2);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(bonded);
        bond.declareMbps(1000);
        vm.stopPrank();

        vm.warp(1_000_000);
        bytes32 nodeId = bytes32(uint256(0xB1AC5));
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
        assertTrue(bond.isActive(opAddr)); // active before blacklist

        vm.prank(admin);
        bond.ejectNode(opAddr);
        assertFalse(bond.isActive(opAddr)); // gate flips — not a tautology

        // Re-bond well above the curve; the latch must keep both `isActive`
        // false and `registerNode` barred.
        vm.prank(opAddr);
        bond.bond(bonded);
        assertFalse(bond.isActive(opAddr));
        vm.prank(opAddr);
        vm.expectRevert(CapacityBond.OperatorEjected.selector);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
    }

    /// @notice The re-bond of a blacklisted operator must NOT emit `Reinstated`
    ///         (the off-chain reactivation signal). Guards against a regression
    ///         that emits the event while leaving `ejected` set.
    function test_blacklistEjected_reBond_doesNotEmitReinstated() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(admin);
        bond.ejectNode(operator);

        vm.recordLogs();
        vm.prank(operator);
        bond.bond(MIN_BOND);

        Vm.Log[] memory logs = vm.getRecordedLogs();
        bytes32 reinstatedSig = keccak256("Reinstated(address)");
        for (uint256 i = 0; i < logs.length; i++) {
            assertTrue(logs[i].topics[0] != reinstatedSig, "Reinstated must not fire while blacklisted");
        }
    }

    function test_unEjectNode_revertsZeroAddress() public {
        vm.prank(admin);
        vm.expectRevert(CapacityBond.ZeroAddress.selector);
        bond.unEjectNode(address(0));
    }

    // ADR 026 § Capacity-bond curve — declared-capacity band on `declareMbps`.

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

    /// @notice Zero is the one sub-floor value an INACTIVE operator may
    ///         declare — the #1361 tier-release path — so it is not a band
    ///         violation the way `9` above is. The active-operator case still
    ///         reverts: `test_activeOperator_cannotDeclareZero`.
    /// @dev    `operator` is bonded but never registered, which is exactly the
    ///         state the release exists for. The curve gate is skipped too, so
    ///         this passes whatever the operator's bond is.
    function test_declareMbps_zeroReleasesTierWhenInactive() public {
        _bondForMbps(10);
        vm.startPrank(operator);
        bond.declareMbps(10);
        assertEq(bond.declaredMbps(operator), 10);
        bond.declareMbps(0);
        vm.stopPrank();

        assertFalse(bond.isActive(operator), "the release path is gated on being inactive");
        assertEq(bond.declaredMbps(operator), 0, "and it clears the tier rather than reverting");
    }

    /// @notice The tier release is an exit step, so a pause does not block it;
    ///         a normal declaration still reverts while paused.
    function test_declareMbps_releaseWorksWhilePaused() public {
        _bondForMbps(10);
        vm.prank(operator);
        bond.declareMbps(10);

        vm.startPrank(admin);
        bond.grantRole(bond.PAUSER_ROLE(), admin);
        bond.pause();
        vm.stopPrank();

        vm.prank(operator);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        bond.declareMbps(20);

        vm.prank(operator);
        bond.declareMbps(0);
        assertEq(bond.declaredMbps(operator), 0, "the release lands during a pause");
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
    // is enforced at `declareMbps` / `unbond` / `registerNode`.
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

    function test_unbond_revertsWhenItDropsBelowCurve() public {
        _bondForMbps(1000);
        vm.prank(operator);
        bond.declareMbps(1000);

        uint256 required = bond.bondRequired(1000);
        // Operator bonded exactly `required`; unbonding any amount drops below.
        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.BondBelowCurve.selector, required - 1, required));
        bond.unbond(1);
    }

    function test_unbond_allowedWhenNoCapacityDeclared() public {
        // declaredMbps defaults to 0 ⇒ bondRequired(0) == 0, so the curve does
        // not constrain unbonding.
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(operator);
        bond.unbond(MIN_BOND / 2);
        assertEq(bond.activeBond(operator), MIN_BOND / 2);
    }

    function test_unbond_succeedsDownToExactlyCurve() public {
        // Over-bond above the 1 Gbps curve, declare 1000, then unbond the slack
        // down to exactly bondRequired(1000) — the inclusive boundary succeeds.
        uint256 required = bond.bondRequired(1000);
        uint256 slack = 5000e18;
        vm.prank(admin);
        token.transfer(operator, required + slack);
        uint256 balBefore = token.balanceOf(operator);
        vm.startPrank(operator);
        bond.bond(required + slack);
        bond.declareMbps(1000);
        bond.unbond(slack);
        vm.stopPrank();
        assertEq(bond.activeBond(operator), required);
        assertEq(token.balanceOf(operator), balBefore - required, "the slack is back with the operator");
    }

    function test_unbond_revertsOnZeroAmount() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(operator);
        vm.expectRevert(CapacityBond.ZeroAmount.selector);
        bond.unbond(0);
    }

    function test_unbond_revertsWhenAmountExceedsActiveBond() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);
        vm.prank(operator);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.InsufficientBond.selector, MIN_BOND + 1, MIN_BOND));
        bond.unbond(MIN_BOND + 1);
    }

    /// @notice `unbond` pays the TOKEN out in the same call — no window, no
    ///         second transaction — and logs the amount and the new active bond.
    function test_unbond_transfersImmediatelyAndEmits() public {
        vm.prank(operator);
        bond.bond(MIN_BOND);
        uint256 opBefore = token.balanceOf(operator);
        uint256 bondBefore = token.balanceOf(address(bond));

        vm.expectEmit(true, false, false, true, address(bond));
        emit CapacityBond.Unbonded(operator, 20_000e18, MIN_BOND - 20_000e18);
        vm.prank(operator);
        bond.unbond(20_000e18);

        assertEq(bond.activeBond(operator), MIN_BOND - 20_000e18);
        assertEq(token.balanceOf(operator), opBefore + 20_000e18, "the operator holds the TOKEN at once");
        assertEq(token.balanceOf(address(bond)), bondBefore - 20_000e18, "the contract no longer holds it");
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
        vm.prank(opAddr);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.BondBelowCurve.selector, bonded, newRequired));
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
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
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");

        assertEq(bond.addressToNodeId(opAddr), nodeId);
        assertTrue(bond.isActive(opAddr));
    }

    /// @dev Register a fresh operator with exactly `bondAmount` and no declared
    ///      Mbps, so `isActive` is gated only by `minBond` and the four-way
    ///      predicate. Returns the operator address.
    function _registerFreshOperator(uint256 opPk, bytes32 nodeId, uint256 bondAmount)
        internal
        returns (address opAddr)
    {
        opAddr = vm.addr(opPk);
        vm.prank(admin);
        token.transfer(opAddr, bondAmount);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(bondAmount);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
        vm.stopPrank();
    }

    /// @notice A partial unbond that retains the operator's floor keeps it
    ///         active.
    function test_isActive_partialUnbondAboveFloorStaysActive() public {
        vm.warp(1_000_000);
        address b = _registerFreshOperator(0xB2, bytes32(uint256(0xB2)), 2 * MIN_BOND);

        vm.prank(b);
        bond.unbond(MIN_BOND);
        assertEq(bond.activeBond(b), MIN_BOND, "B retains exactly its floor");
        assertTrue(bond.isActive(b), "a partial unbond above the floor keeps B active");
    }

    /// @notice `getRegisteredNodes` returns the full registered page and a
    ///         parallel `active[]` computed on-chain via `isActive`. An operator
    ///         whose bond sits below its floor is `isActive == false` but MUST
    ///         stay in the page — it is inactive yet still payable, so the
    ///         off-chain bindings projection depends on it not vanishing
    ///         (issue #1565).
    function test_getRegisteredNodes_marksSubFloorOperatorInactiveButPresent() public {
        vm.warp(1_000_000);
        address a = _registerFreshOperator(0xA1, bytes32(uint256(0xA1)), MIN_BOND);
        address b = _registerFreshOperator(0xB2, bytes32(uint256(0xB2)), MIN_BOND);
        assertTrue(bond.isActive(a));
        assertTrue(bond.isActive(b));

        vm.prank(b);
        bond.unbond(1);
        assertLt(bond.activeBond(b), MIN_BOND, "B drops below its floor");
        assertFalse(bond.isActive(b), "a sub-floor bond flips isActive false");

        (CapacityBond.NodeInfo[] memory page, bool[] memory active) = bond.getRegisteredNodes(0, 100);

        assertEq(page.length, 2, "both operators still in the registered page");
        assertEq(active.length, page.length, "active[] is index-aligned with page");

        bool sawA;
        bool sawB;
        for (uint256 i = 0; i < page.length; i++) {
            if (page[i].ethAddress == a) {
                sawA = true;
                assertTrue(active[i], "active operator flagged active");
            } else if (page[i].ethAddress == b) {
                sawB = true;
                assertFalse(active[i], "sub-floor operator present but flagged inactive");
            } else {
                revert("unexpected operator in page");
            }
        }
        assertTrue(sawA && sawB, "both operators enumerated");
    }

    // ADR 003 § Node Registry describes a full exit as deregistration followed
    // by unbonding. That only holds because deregistration also clears the
    // declared tier: `unbond`'s floor is `bondRequired(declaredMbps)`,
    // so an operator that had ever declared would otherwise retain that much
    // bond forever (#1351).
    //
    // `deregisterNode` requires an active node, so the paths that deactivate
    // without it leave the tier standing; those operators exit via
    // `declareMbps(0)` instead (#1361). The four tests below —
    // `test_belowCurveOperator_canReleaseTheTierAndExit`,
    // `test_neverRegistered_canReleaseTheTierAndExit`,
    // `test_blacklistEjectedOperator_canReleaseTheTierAndExit`, and the negative
    // `test_activeOperator_cannotDeclareZero` — pin that split.

    /// @dev Bond up to the tier requirement, declare it, and register a node
    ///      for a fresh operator derived from `opPk`. The deregistration tests
    ///      all start from a registered node at a declared tier; this is the
    ///      sequence `test_registerNode_succeedsAtCurve` spells out inline.
    function _onboardAtTier(uint256 opPk, uint256 mbps, bytes32 nodeId) internal returns (address opAddr) {
        opAddr = vm.addr(opPk);
        // Both floors read off the contract, like `_bondForMbps` — a fixture
        // that hardcoded `MIN_BOND` would silently under-bond after a
        // `setMinBond` in some future shared setup.
        uint256 bonded = bond.bondRequired(mbps);
        uint256 floor = bond.minBond();
        if (bonded < floor) bonded = floor;

        vm.prank(admin);
        token.transfer(opAddr, bonded);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(bonded);
        bond.declareMbps(mbps);
        vm.stopPrank();
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
    }

    function test_deregisterNode_clearsDeclaredMbpsAndEmits() public {
        address opAddr = _onboardAtTier(0xDE9151, 1000, bytes32(uint256(0xDE9151)));
        assertEq(bond.declaredMbps(opAddr), 1000);

        // The clear is announced: `MbpsDeclared` is the only tier signal an
        // indexer has, so a silent second writer would desync it.
        vm.expectEmit(true, false, false, true, address(bond));
        emit CapacityBond.MbpsDeclared(opAddr, 1000, 0);
        vm.prank(opAddr);
        bond.deregisterNode();

        assertEq(bond.declaredMbps(opAddr), 0, "deregistration releases the curve floor");
    }

    function test_deregisterNode_emitsNoMbpsEventWhenNeverDeclared() public {
        // `registerNode` does not require a prior `declareMbps` — `minBond`
        // alone suffices when the tier is 0 — so this operator reaches
        // deregistration having never declared.
        uint256 opPk = 0x0DEC1;
        address opAddr = vm.addr(opPk);
        vm.prank(admin);
        token.transfer(opAddr, MIN_BOND);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(MIN_BOND);
        vm.stopPrank();

        bytes32 nodeId = bytes32(uint256(0x0DEC1));
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");

        vm.recordLogs();
        vm.prank(opAddr);
        bond.deregisterNode();

        Vm.Log[] memory logs = vm.getRecordedLogs();
        for (uint256 i = 0; i < logs.length; i++) {
            assertTrue(
                logs[i].topics[0] != CapacityBond.MbpsDeclared.selector,
                "a never-declared operator must not log a 0 -> 0 tier change"
            );
        }
    }

    function test_fullExit_deregisterThenUnbondEverything() public {
        address opAddr = _onboardAtTier(0xE811, 1000, bytes32(uint256(0xE811)));
        uint256 bonded = bond.activeBond(opAddr);
        uint256 balanceBefore = token.balanceOf(opAddr);

        // While the tier stands, releasing the whole bond is barred by the
        // curve. `bondRequired` is read before the prank: an external call
        // inside the `expectRevert` argument would consume it and send `unbond`
        // from the test contract instead.
        uint256 curveFloor = bond.bondRequired(1000);
        vm.prank(opAddr);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.BondBelowCurve.selector, 0, curveFloor));
        bond.unbond(bonded);

        vm.prank(opAddr);
        bond.deregisterNode();

        // One call, same block: no window between deregistration and payout.
        vm.expectEmit(true, false, false, true, address(bond));
        emit CapacityBond.Unbonded(opAddr, bonded, 0);
        vm.prank(opAddr);
        bond.unbond(bonded);

        assertEq(bond.activeBond(opAddr), 0, "the last TOKEN is withdrawable once the tier is cleared");
        assertEq(token.balanceOf(opAddr), balanceBefore + bonded, "the full bond comes back");
    }

    // ── Exits are never pausable (issue #2030) ───────────────────────────────

    /// @notice A pause must not trap an operator's bond. With the contract
    ///         paused, an operator can still `unbond` the principal back. Pause
    ///         halts intake and serving only; a captured PAUSER cannot freeze
    ///         the exit.
    function test_unbond_worksWhilePaused() public {
        vm.warp(1_000_000);
        vm.prank(operator);
        bond.bond(MIN_BOND);
        uint256 balBefore = token.balanceOf(operator);

        vm.startPrank(admin);
        bond.grantRole(bond.PAUSER_ROLE(), admin);
        bond.pause();
        vm.stopPrank();

        // No capacity is declared, so the curve floor is 0 and the whole bond is
        // releasable — the exit runs entirely while the contract is paused.
        vm.prank(operator);
        bond.unbond(MIN_BOND);

        assertEq(bond.activeBond(operator), 0, "bond fully released despite the pause");
        assertEq(token.balanceOf(operator), balBefore + MIN_BOND, "principal returned while paused");
    }

    /// @notice A re-registration into a new region snapshots the old region in
    ///         `regionPrev`, so the old region's blacklist entries keep applying
    ///         through the stability window (ADR 030). #2837: two
    ///         deregister/re-register hops inside one stability
    ///         window cannot flip the region twice. A → B waits out the window;
    ///         B → C inside the next window reverts `RegionCooldownActive`, so
    ///         `regionPrev` still holds A and A's regional entries still reach
    ///         the operator.
    function test_reRegister_secondRegionFlipInsideWindow_reverts() public {
        uint256 opPk = 0x8EEA;
        bytes32 nodeId = bytes32(uint256(0x8EEA));
        address opAddr = _onboardAtTier(opPk, 1000, nodeId);

        // Inside the registration-time window, even the first flip reverts.
        uint64 readyAt = uint64(block.timestamp + bond.regionStabilityWindow());
        vm.prank(opAddr);
        bond.deregisterNode();
        vm.prank(opAddr);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.RegionCooldownActive.selector, readyAt));
        bond.registerNode(nodeId, hex"", "eu-west", TERMS_HASH, hex"01");

        // A → B once the window has elapsed.
        vm.warp(readyAt);
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "eu-west", TERMS_HASH, hex"01");

        // B → C in the same block: rejected.
        vm.prank(opAddr);
        bond.deregisterNode();
        readyAt = uint64(block.timestamp + bond.regionStabilityWindow());
        vm.prank(opAddr);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.RegionCooldownActive.selector, readyAt));
        bond.registerNode(nodeId, hex"", "ap-south", TERMS_HASH, hex"01");

        // Re-registering into the current region is not a change and is allowed.
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "eu-west", TERMS_HASH, hex"01");
        (string memory region, string memory prev,,) = bond.regionScopeData(opAddr);
        assertEq(region, "eu-west");
        assertEq(prev, "us-east", "the serve-time region stays in scope");
    }

    function test_reRegisterAfterDeregister_keepsBondAndNeedsFreshDeclare() public {
        uint256 opPk = 0x8EE8;
        bytes32 nodeId = bytes32(uint256(0x8EE8));
        address opAddr = _onboardAtTier(opPk, 1000, nodeId);
        uint256 bonded = bond.activeBond(opAddr);

        vm.prank(opAddr);
        bond.deregisterNode();
        assertEq(bond.activeBond(opAddr), bonded, "deregistration never touches the bond");

        // ADR 003: "an operator who changes their mind can re-register without
        // re-funding" — the retained bond clears `minBond` and, with the tier
        // now 0, the curve gate as well.
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
        assertTrue(bond.isActive(opAddr));
        assertEq(bond.declaredMbps(opAddr), 0, "the tier does not come back with the registration");

        vm.prank(opAddr);
        bond.declareMbps(1000);
        assertEq(bond.declaredMbps(opAddr), 1000, "re-declaring needs no further bond");
    }

    /// @notice The #1361 tier-release path on an operator whose bond sits
    ///         BELOW its standing tier: governance raised `k` after the
    ///         declaration, so `unbond` rejects EVERY non-zero amount while the
    ///         tier stands.
    /// @dev    `declareMbps` enforces no monotonicity, so this operator could
    ///         also declare DOWN to `minCapacityMbps` and unbond above
    ///         `bondRequired(10)`, stranding that floor-tier cost. What
    ///         `declareMbps(0)` adds is a one-step release that strands nothing.
    ///         The assertion below is `activeBond == 0`, which the tier-down
    ///         route cannot reach — that is the difference this test pins.
    function test_belowCurveOperator_canReleaseTheTierAndExit() public {
        address opAddr = vm.addr(0xE7EC7);
        uint256 bonded = bond.bondRequired(1000);
        vm.prank(admin);
        token.transfer(opAddr, bonded);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(bonded);
        bond.declareMbps(1000);
        vm.stopPrank();

        vm.prank(admin);
        bond.setK(20e18);
        uint256 floor = bond.bondRequired(1000);
        assertLt(bonded, floor, "the fixture must leave the bond below the standing curve floor");

        // With the bond already BELOW the floor, every non-zero amount reverts,
        // not merely amounts that would cross it.
        vm.prank(opAddr);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.BondBelowCurve.selector, bonded - 1, floor));
        bond.unbond(1);

        vm.expectEmit(true, false, false, true, address(bond));
        emit CapacityBond.MbpsDeclared(opAddr, 1000, 0);
        vm.prank(opAddr);
        bond.declareMbps(0);
        assertEq(bond.declaredMbps(opAddr), 0, "the tier is released");

        // With the floor gone the whole bond is withdrawable.
        uint256 balanceBefore = token.balanceOf(opAddr);
        vm.prank(opAddr);
        bond.unbond(bonded);

        assertEq(bond.activeBond(opAddr), 0, "the bond is no longer trapped");
        assertEq(token.balanceOf(opAddr), balanceBefore + bonded, "and it lands back with the operator");
    }

    /// @notice The second stuck state #1361 closes, and the one directly
    ///         reachable from the shipped CLI: `decdn node bond --mbps N` runs
    ///         approve -> `bond` -> `declareMbps` and never registers, so an
    ///         operator who stops there holds a standing tier with no
    ///         `deregisterNode` available.
    /// @dev    Distinct from the ejected case: here the bond still clears the
    ///         curve, so the operator is pinned at `bondRequired(tier)` rather
    ///         than at everything. `unbond(bonded)` is the assertion —
    ///         the surplus was always releasable, the floor never was.
    function test_neverRegistered_canReleaseTheTierAndExit() public {
        address opAddr = vm.addr(0x4E4E4);
        uint256 bonded = bond.bondRequired(1000);

        vm.prank(admin);
        token.transfer(opAddr, bonded);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(bonded);
        bond.declareMbps(1000);

        // Pinned at the curve floor, with no registration to deregister.
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.BondBelowCurve.selector, 0, bonded));
        bond.unbond(bonded);
        vm.stopPrank();

        vm.prank(opAddr);
        vm.expectRevert(CapacityBond.NodeNotActive.selector);
        bond.deregisterNode();

        vm.startPrank(opAddr);
        bond.declareMbps(0);
        bond.unbond(bonded);
        vm.stopPrank();

        assertEq(bond.activeBond(opAddr), 0, "a never-registered operator can get their bond back");
    }

    /// @notice A blacklist-ejected operator can release and exit without
    ///         governance intervention.
    /// @dev    This is the state with no other escape at all: `registerNode`
    ///         reverts `OperatorEjected` until governance calls `unEjectNode`,
    ///         so no "re-register, then deregister" route exists here.
    function test_blacklistEjectedOperator_canReleaseTheTierAndExit() public {
        address opAddr = _onboardAtTier(0xB1AC4, 1000, bytes32(uint256(0xB1AC4)));
        uint256 bonded = bond.activeBond(opAddr);

        vm.prank(admin);
        bond.ejectNode(opAddr);
        assertTrue(bond.blacklistEjected(opAddr), "the fixture must actually blacklist-eject");

        vm.startPrank(opAddr);
        bond.declareMbps(0);
        bond.unbond(bonded);
        vm.stopPrank();

        assertEq(bond.activeBond(opAddr), 0, "no unEjectNode needed to recover the bond");
    }

    /// @notice The release is not a back door around the capacity band: an
    ///         ACTIVE operator's `declareMbps(0)` still reverts.
    /// @dev    Without this, `declareMbps(0)` would be a second, unlogged way
    ///         to leave the active set at tier 0 — bypassing `deregisterNode`'s
    ///         registered-set removal and `registrationNonce` bump.
    function test_activeOperator_cannotDeclareZero() public {
        address opAddr = _onboardAtTier(0xAC71E, 1000, bytes32(uint256(0xAC71E)));

        uint256 floor = bond.minCapacityMbps();
        uint256 ceiling = bond.maxCapacityMbps();
        vm.prank(opAddr);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.DeclaredCapacityOutOfBand.selector, 0, floor, ceiling));
        bond.declareMbps(0);

        assertEq(bond.declaredMbps(opAddr), 1000, "the tier survives the rejected release");
        assertTrue(bond.isActive(opAddr), "and the node stays active");
    }

    /// @notice `deregisterNode` clears the declared tier and NOTHING else.
    /// @dev    A future "finish the cleanup" refactor is the threat this
    ///         guards. `activeBond`: deregistration and bond exit are separate
    ///         operations (ADR 003 § Node Registry). `firstBondedAt`: ADR 003
    ///         § Node Registry states it is never cleared by `deregisterNode`,
    ///         and the age-ramp depends on it.
    function test_deregisterNode_preservesBondAndFirstBonded() public {
        address opAddr = _onboardAtTier(0x9A4D, 1000, bytes32(uint256(0x9A4D)));
        uint256 bonded = bond.activeBond(opAddr);
        uint64 firstBonded = bond.firstBondedAt(opAddr);
        assertGt(firstBonded, 0, "the fixture must stamp tenure");

        vm.prank(opAddr);
        bond.deregisterNode();

        assertEq(bond.declaredMbps(opAddr), 0, "the tier is the one thing it clears");
        assertEq(bond.activeBond(opAddr), bonded, "the bond stays put");
        assertEq(bond.firstBondedAt(opAddr), firstBonded, "deregistering keeps the tenure");
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
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");

        (string memory regionHint, string memory regionPrev, uint64 regionLastChanged, uint256 window) =
            bond.regionScopeData(opAddr);

        assertEq(regionHint, "us-east");
        assertEq(regionPrev, ""); // never changed
        // Stamped at registration (ADR 030): never 0 for an active node — the
        // ripening window and `updateRegion` cooldown both run from this stamp.
        assertEq(regionLastChanged, uint64(block.timestamp));
        assertEq(window, bond.regionStabilityWindow());
    }

    // ----------------------------------------------------------------------
    // Access-control guards on governance setters
    //
    // Role enforcement on the setters below. A regression that drops
    // `onlyRole(GOVERNANCE_ROLE)` would otherwise
    // pass CI silently, so each setter gets a "reverts when called by a
    // non-governance address" test. The PAUSER_ROLE-gated pause/unpause
    // pair gets the same treatment.
    // ----------------------------------------------------------------------

    function test_setMinBond_revertsWithoutRole() public {
        _expectMissingRole(operator, bond.GOVERNANCE_ROLE());
        vm.prank(operator);
        bond.setMinBond(MIN_BOND);
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

    // ADR 009 § Emergency Multisig — the protocol-wide pause sunsets hard at
    // each contract's own construction time + 365 days; afterwards `pause()` reverts for every
    // caller (progressive immutability).
    function test_pause_revertsAfterSunset() public {
        bytes32 pauserRole = bond.PAUSER_ROLE();
        vm.prank(admin);
        bond.grantRole(pauserRole, admin);

        vm.warp(block.timestamp + 366 days);
        vm.prank(admin);
        vm.expectRevert(SunsettingPausable.PauseExpired.selector);
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
        vm.prank(opAddr);
        vm.expectRevert(CapacityBond.InvalidEd25519Signature.selector);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
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
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");

        // Second registration without deregistering.
        vm.prank(opAddr);
        vm.expectRevert(CapacityBond.NodeAlreadyRegistered.selector);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
    }

    // -----------------------------------------------------------------
    // minBond is non-retroactive (issue #2033)
    // -----------------------------------------------------------------

    /// @dev Bond exactly `amount` from a fresh operator and register a node with
    ///      no Mbps declared, so only `minBond` gates entry. Returns the address.
    function _bondAndRegister(uint256 opPk, uint256 amount) internal returns (address opAddr) {
        opAddr = vm.addr(opPk);
        vm.prank(admin);
        token.transfer(opAddr, amount);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(amount);
        vm.stopPrank();
        bytes32 nodeId = bytes32(uint256(opPk));
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
    }

    /// @notice Core: an operator active at the launch `minBond` (50k) stays
    ///         active — retaining vote-weight eligibility — after governance
    ///         raises `minBond` to 100k. The raise is non-retroactive: it never
    ///         de-activates the grandfathered operator.
    function test_minBond_raise_grandfathersActiveOperator() public {
        address opAddr = _bondAndRegister(0xF10021, MIN_BOND);
        assertTrue(bond.isActive(opAddr));
        assertEq(bond.bondFloorAtActivation(opAddr), MIN_BOND);

        vm.prank(admin);
        bond.setMinBond(100_000e18);

        // Still active though active bond (50k) sits below the new minBond.
        assertTrue(bond.isActive(opAddr));
        assertFalse(bond.ejected(opAddr), "a minBond raise alone never ejects");
        assertEq(bond.activeBond(opAddr), MIN_BOND);
        assertEq(bond.bondFloorAtActivation(opAddr), MIN_BOND); // floor unchanged
    }

    /// @notice `registerNode` is an activation path distinct from `bond()`: an
    ///         operator can satisfy `activeBond >= minBond` without a `bond()`
    ///         up-crossing when governance lowered `minBond` after they bonded.
    ///         registerNode must still capture a non-zero grandfathered floor —
    ///         otherwise a later `minBond` raise would wrongly keep an
    ///         under-floor operator active (`activeBond >= 0`). PR #2038 review.
    function test_minBond_registerAfterDecrease_capturesFloor() public {
        // Raise minBond above the operator's future bond so `bond()` records no floor.
        vm.prank(admin);
        bond.setMinBond(100_000e18);

        uint256 opPk = 0xF10099;
        address opAddr = vm.addr(opPk);
        bytes32 nodeId = bytes32(uint256(0xF10099));
        vm.prank(admin);
        token.transfer(opAddr, 60_000e18);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(60_000e18); // 60k < 100k minBond → no up-crossing, floor stays 0
        vm.stopPrank();
        assertEq(bond.bondFloorAtActivation(opAddr), 0, "bond() below minBond captures no floor");
        assertEq(bond.firstBondedAt(opAddr), 0, "bond() below minBond starts no tenure");
        assertFalse(bond.isActive(opAddr), "not active: unregistered");

        // Governance lowers minBond below the operator's existing bond.
        vm.prank(admin);
        bond.setMinBond(50_000e18);

        // Activate via registerNode (no further bond()).
        vm.startPrank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
        vm.stopPrank();

        assertEq(bond.bondFloorAtActivation(opAddr), 50_000e18, "registerNode captures live minBond as floor");
        assertEq(bond.firstBondedAt(opAddr), block.timestamp, "registerNode starts the tenure");
        assertTrue(bond.isActive(opAddr), "active after registration");

        // A later raise must not de-activate the grandfathered operator.
        vm.prank(admin);
        bond.setMinBond(100_000e18);
        assertTrue(bond.isActive(opAddr), "grandfathered at 50k floor, stays active");
        assertEq(bond.bondFloorAtActivation(opAddr), 50_000e18, "floor unchanged by later raise");
    }

    /// @notice A floor left by an earlier `bond()` entry above the live
    ///         `minBond` is replaced at registration, and the running tenure is
    ///         kept.
    function test_minBond_registerAfterDecrease_replacesStaleHigherFloor() public {
        vm.prank(admin);
        bond.setMinBond(100_000e18);

        uint256 opPk = 0xF1009A;
        address opAddr = vm.addr(opPk);
        bytes32 nodeId = bytes32(uint256(0xF1009A));
        vm.prank(admin);
        token.transfer(opAddr, 100_000e18);
        vm.warp(1_000_000);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(100_000e18); // crosses the 100k minBond → floor 100k
        vm.stopPrank();
        assertEq(bond.bondFloorAtActivation(opAddr), 100_000e18);

        vm.prank(admin);
        bond.setMinBond(50_000e18);

        vm.warp(2_000_000);
        vm.startPrank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
        vm.stopPrank();

        assertEq(bond.bondFloorAtActivation(opAddr), 50_000e18, "registration records the live minBond");
        assertEq(bond.firstBondedAt(opAddr), 1_000_000, "registration keeps the running tenure");
        assertTrue(bond.isActive(opAddr), "100k clears the 50k entry floor");
    }

    /// @notice An unbond below the floor ends the tenure before registration.
    ///         A balance that clears a since-lowered `minBond` then registers
    ///         without another `bond()`, and registration starts a fresh tenure.
    function test_minBond_registerAfterFloorDip_startsFreshTenure() public {
        vm.prank(admin);
        bond.setMinBond(100_000e18);

        uint256 opPk = 0xF1009B;
        address opAddr = vm.addr(opPk);
        bytes32 nodeId = bytes32(uint256(0xF1009B));
        vm.prank(admin);
        token.transfer(opAddr, 100_000e18);
        vm.warp(1_000_000);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(100_000e18); // floor 100k, tenure from 1_000_000
        bond.unbond(40_000e18); // 60k < 100k floor
        vm.stopPrank();
        assertEq(bond.bondFloorAtActivation(opAddr), 0, "the dip clears the floor");
        assertEq(bond.firstBondedAt(opAddr), 0, "the dip ends the tenure");

        vm.prank(admin);
        bond.setMinBond(50_000e18);

        vm.warp(2_000_000);
        vm.startPrank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
        vm.stopPrank();

        assertEq(bond.bondFloorAtActivation(opAddr), 50_000e18, "registration records the live minBond");
        assertEq(bond.firstBondedAt(opAddr), 2_000_000, "registration starts a fresh tenure");
        assertTrue(bond.isActive(opAddr), "60k clears the 50k entry floor");
    }

    /// @notice A NEW operator bonding below the raised `minBond` cannot activate:
    ///         entry is gated on the live `minBond`, and no floor is recorded.
    function test_minBond_raise_barsNewEntrantBelowFloor() public {
        vm.prank(admin);
        bond.setMinBond(100_000e18);

        uint256 opPk = 0xF10022;
        address opAddr = vm.addr(opPk);
        vm.prank(admin);
        token.transfer(opAddr, MIN_BOND); // 50k, under the new 100k floor
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(MIN_BOND);
        vm.stopPrank();
        // Never crossed the live minBond, so no grandfathered floor was set.
        assertEq(bond.bondFloorAtActivation(opAddr), 0);

        bytes32 nodeId = bytes32(uint256(opPk));
        vm.prank(opAddr);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.BondBelowMinimum.selector, MIN_BOND, 100_000e18));
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
    }

    /// @notice A grandfathered operator's own partial unbond below its
    ///         activation floor de-activates it (mirrors the below-floor
    ///         behavior, keyed to the grandfathered floor).
    function test_minBond_partialUnbondBelowFloor_deactivates() public {
        address opAddr = _bondAndRegister(0xF10024, 60_000e18); // crosses 50k → floor 50k
        assertTrue(bond.isActive(opAddr));
        assertEq(bond.bondFloorAtActivation(opAddr), MIN_BOND);

        // Unbond 15k → active 45k, under the 50k grandfathered floor.
        vm.prank(opAddr);
        bond.unbond(15_000e18);
        assertEq(bond.activeBond(opAddr), 45_000e18);
        assertFalse(bond.isActive(opAddr)); // 45k < 50k floor: own action de-activates
        assertEq(bond.firstBondedAt(opAddr), 0); // and ends the tenure
        assertEq(bond.bondFloorAtActivation(opAddr), 0); // and the grandfathered floor
    }

    /// @notice A tenured operator that tops up through a raised `minBond`
    ///         records nothing: it keeps its grandfathered floor and its stamp,
    ///         so a later unbond is judged against the old floor.
    function test_minBond_tenuredTopUpThroughRaiseKeepsFloor() public {
        vm.warp(1_000_000);
        address opAddr = _bondAndRegister(0xF10026, 80_000e18); // floor 50k
        assertEq(bond.bondFloorAtActivation(opAddr), MIN_BOND);

        vm.prank(admin);
        bond.setMinBond(100_000e18);
        vm.prank(admin);
        token.transfer(opAddr, 40_000e18);

        vm.warp(2_000_000);
        vm.prank(opAddr);
        bond.bond(40_000e18); // 80k → 120k, through the raised 100k minBond
        assertEq(bond.bondFloorAtActivation(opAddr), MIN_BOND, "a top-up keeps the grandfathered floor");
        assertEq(bond.firstBondedAt(opAddr), 1_000_000, "and the stamp");

        vm.prank(opAddr);
        bond.unbond(60_000e18); // 60k: below the live 100k, above the 50k floor
        assertEq(bond.firstBondedAt(opAddr), 1_000_000, "an unbond that holds the floor keeps the tenure");
        assertEq(bond.bondFloorAtActivation(opAddr), MIN_BOND, "and the floor");
        assertTrue(bond.isActive(opAddr), "and the operator stays active");
    }

    /// @notice The tenure-ending `unbond` compares against the recorded floor,
    ///         not the live `minBond`: a remainder below a raised `minBond` but
    ///         above the floor keeps the tenure.
    function test_minBond_unbondComparesFloorNotRaisedMinBond() public {
        vm.warp(1_000_000);
        address opAddr = _bondAndRegister(0xF10027, 80_000e18); // floor 50k

        vm.prank(admin);
        bond.setMinBond(100_000e18);

        vm.prank(opAddr);
        bond.unbond(20_000e18); // 60k < 100k minBond, ≥ 50k floor
        assertEq(bond.firstBondedAt(opAddr), 1_000_000, "the tenure holds");
        assertEq(bond.bondFloorAtActivation(opAddr), MIN_BOND, "at the 50k floor");
        assertTrue(bond.isActive(opAddr), "and the operator stays active");
    }

    /// @notice The tenure-ending `unbond` compares against the recorded floor,
    ///         not the live `minBond`: a remainder above a lowered `minBond` but
    ///         below the floor ends the tenure.
    function test_minBond_unbondComparesFloorNotLoweredMinBond() public {
        vm.prank(admin);
        bond.setMinBond(100_000e18);
        vm.warp(1_000_000);
        address opAddr = _bondAndRegister(0xF10028, 100_000e18); // floor 100k

        vm.prank(admin);
        bond.setMinBond(MIN_BOND);

        vm.prank(opAddr);
        bond.unbond(25_000e18); // 75k ≥ 50k minBond, < 100k floor
        assertEq(bond.firstBondedAt(opAddr), 0, "the dip below the floor ends the tenure");
        assertEq(bond.bondFloorAtActivation(opAddr), 0, "and clears the floor");
        assertFalse(bond.isActive(opAddr), "and de-activates the operator");
    }

    /// @notice A registered operator whose dip ended its tenure forfeits its
    ///         grandfathered floor: after a `minBond` raise, bonding back to
    ///         the old floor does not re-activate it, and reaching the raised
    ///         `minBond` re-enters it with a fresh tenure.
    function test_minBond_dipForfeitsGrandfatheredFloor() public {
        vm.warp(1_000_000);
        address opAddr = _bondAndRegister(0xF10025, MIN_BOND); // floor 50k
        vm.prank(admin);
        token.transfer(opAddr, 2 * MIN_BOND);

        vm.prank(opAddr);
        bond.unbond(10_000e18); // 40k < 50k floor
        assertFalse(bond.isActive(opAddr), "the dip de-activates");

        vm.prank(admin);
        bond.setMinBond(2 * MIN_BOND);

        vm.prank(opAddr);
        bond.bond(10_000e18); // back to the old 50k floor, below the 100k minBond
        assertFalse(bond.isActive(opAddr), "the old floor no longer applies");
        assertEq(bond.firstBondedAt(opAddr), 0, "no entry below the live minBond");

        vm.warp(2_000_000);
        vm.prank(opAddr);
        bond.bond(MIN_BOND); // 100k reaches the raised minBond
        assertTrue(bond.isActive(opAddr), "re-entry at the live minBond re-activates");
        assertEq(bond.firstBondedAt(opAddr), 2_000_000, "with a fresh tenure");
        assertEq(bond.bondFloorAtActivation(opAddr), 2 * MIN_BOND, "at the raised floor");
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

    // -----------------------------------------------------------------
    // ADR 019 § Terms Acceptance — termsHash at registration + governor swap
    // -----------------------------------------------------------------

    /// @dev Fund `opAddr`, bond above the 1-Gbps curve, and declare capacity so
    ///      the operator is registration-ready. Returns the operator address.
    function _readyOperator(uint256 opPk) internal returns (address opAddr) {
        opAddr = vm.addr(opPk);
        uint256 bonded = bond.bondRequired(1000);
        vm.prank(admin);
        token.transfer(opAddr, bonded);
        vm.startPrank(opAddr);
        token.approve(address(bond), type(uint256).max);
        bond.bond(bonded);
        bond.declareMbps(1000);
        vm.stopPrank();
    }

    function test_registerNode_recordsTermsAcceptance() public {
        uint256 opPk = 0x7E12A5;
        address opAddr = _readyOperator(opPk);
        bytes32 nodeId = bytes32(uint256(0x7E125));

        vm.warp(1_700_000_000);

        vm.expectEmit(true, true, false, true, address(bond));
        emit CapacityBond.TermsAccepted(nodeId, TERMS_HASH, 1_700_000_000);

        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");
        assertTrue(bond.isActive(opAddr));
    }

    function test_registerNode_revertsOnTermsHashMismatch() public {
        uint256 opPk = 0x7E12A6;
        address opAddr = _readyOperator(opPk);
        bytes32 nodeId = bytes32(uint256(0x7E126));
        bytes32 stale = keccak256("decdn operator terms v0");

        vm.prank(opAddr);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.TermsHashMismatch.selector, stale, TERMS_HASH));
        bond.registerNode(nodeId, hex"", "us-east", stale, hex"01");
    }

    function test_setCurrentTermsHash_onlyGovernance() public {
        _expectMissingRole(operator, bond.GOVERNANCE_ROLE());
        vm.prank(operator);
        bond.setCurrentTermsHash(keccak256("v2"));
    }

    function test_setCurrentTermsHash_revertsOnZero() public {
        vm.prank(admin);
        vm.expectRevert(CapacityBond.ZeroTermsHash.selector);
        bond.setCurrentTermsHash(bytes32(0));
    }

    function test_constructor_revertsOnZeroTermsHash() public {
        vm.expectRevert(CapacityBond.ZeroTermsHash.selector);
        new CapacityBond({
            token_: token,
            ed25519Verifier_: ed25519,
            admin: admin,
            minBond_: MIN_BOND,
            multiaddrUpdateCooldown_: 0,
            maxMultiaddrSize_: 1024,
            regionStabilityWindow_: 7 days,
            currentTermsHash_: bytes32(0)
        });
    }

    function test_setCurrentTermsHash_emitsAndSwaps() public {
        bytes32 next = keccak256("decdn operator terms v2");
        vm.expectEmit(false, false, false, true, address(bond));
        emit CapacityBond.CurrentTermsHashUpdated(TERMS_HASH, next);
        vm.prank(admin);
        bond.setCurrentTermsHash(next);
        assertEq(bond.currentTermsHash(), next);
    }

    function test_setCurrentTermsHash_bindsNewRegistrantsOnly() public {
        bytes32 next = keccak256("decdn operator terms v2");
        vm.prank(admin);
        bond.setCurrentTermsHash(next);

        uint256 opPk = 0x7E12A8;
        address opAddr = _readyOperator(opPk);
        bytes32 nodeId = bytes32(uint256(0x7E128));

        // The now-stale genesis hash is rejected...
        vm.prank(opAddr);
        vm.expectRevert(abi.encodeWithSelector(CapacityBond.TermsHashMismatch.selector, TERMS_HASH, next));
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");

        // ...and the freshly-adopted hash succeeds.
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", next, hex"01");
        assertTrue(bond.isActive(opAddr));
    }

    function test_bindNodeId_rebindRequiresNoTermsAcceptance() public {
        // Register under the genesis terms, then rotate the NodeId via
        // `bindNodeId`. Rebinding does not re-accept terms (ADR 019 §
        // enforcement at registration only).
        uint256 opPk = 0x7E12A9;
        address opAddr = _readyOperator(opPk);
        bytes32 nodeId = bytes32(uint256(0x7E129));
        vm.prank(opAddr);
        bond.registerNode(nodeId, hex"", "us-east", TERMS_HASH, hex"01");

        // Rotate to a fresh NodeId even after governance bumps the terms hash;
        // the rebind still succeeds without naming any terms.
        vm.prank(admin);
        bond.setCurrentTermsHash(keccak256("decdn operator terms v2"));

        bytes32 newNodeId = bytes32(uint256(0x7E129B));
        vm.prank(opAddr);
        bond.bindNodeId(newNodeId, hex"02");
        assertEq(bond.addressToNodeId(opAddr), newNodeId);
    }

    function test_declaredMbpsAtEpoch_keepsPerEpochHistory() public {
        uint64 el = bond.EPOCH_LENGTH();

        // Declare 1000 Mbps during epoch 100.
        vm.warp(uint256(el) * 100 + 5);
        _bondForMbps(1000);
        vm.prank(operator);
        bond.declareMbps(1000);

        // Sampled at end of epoch 100 → 1000; the epoch before the declaration → 0.
        assertEq(bond.declaredMbpsAtEpoch(operator, 100), 1000, "epoch100");
        assertEq(bond.declaredMbpsAtEpoch(operator, 99), 0, "epoch99 not retroactive");

        // Lower to 500 during epoch 105 (bond already covers the smaller tier).
        vm.warp(uint256(el) * 105 + 5);
        vm.prank(operator);
        bond.declareMbps(500);

        assertEq(bond.declaredMbpsAtEpoch(operator, 105), 500, "epoch105 new value");
        assertEq(bond.declaredMbpsAtEpoch(operator, 104), 1000, "epoch104 keeps old value");
        assertEq(bond.declaredMbpsAtEpoch(operator, 100), 1000, "epoch100 unchanged");

        // Release the tier during epoch 110 (operator is not a registered node).
        vm.warp(uint256(el) * 110 + 5);
        vm.prank(operator);
        bond.declareMbps(0);

        assertEq(bond.declaredMbpsAtEpoch(operator, 110), 0, "epoch110 released to 0");
        assertEq(bond.declaredMbpsAtEpoch(operator, 109), 500, "epoch109 keeps 500");
    }
}
