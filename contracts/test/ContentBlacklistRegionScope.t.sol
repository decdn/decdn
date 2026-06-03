// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { ICapacityBondEjector } from "../src/interfaces/ICapacityBondEjector.sol";
import { Token } from "../src/Token.sol";
import { MockRegionBond } from "./mocks/MockRegionBond.sol";

/// @title ContentBlacklist ADR 030 region-ripening tests
/// @notice Unit coverage for `isOperatorInBlacklistScope` (the blacklist-scope
///         ripening predicate, ADR 030 § Eligibility) and the path-2 (Operator)
///         standing gate in `openBlacklistAppeal` (ADR 011 § Standing). Region
///         inputs are driven through `MockRegionBond` so the stability-window
///         boundary is exercised directly. The end-to-end variant against a
///         real `CapacityBond` lives in `CapacityBondRegionE2E.t.sol`.
contract ContentBlacklistRegionScopeTest is Test {
    Token internal token;
    MockRegionBond internal bondMock;
    ContentBlacklist internal blacklist;

    address internal admin = address(0xA11CE);
    address internal multisig = address(0xC0DE);
    address internal regionalBody = address(0xDE);
    address internal filer = address(0xF1);
    address internal operator = address(0xB0B);

    bytes32 internal constant REGION_US = bytes32("US");
    bytes32 internal constant REGION_EU = bytes32("EU");
    bytes32 internal constant GLOBAL_REGION = bytes32("GLOBAL");
    bytes32 internal constant HASH = bytes32(uint256(0xABCDEF));
    uint256 internal constant APPEAL_BOND = 100e18;
    uint256 internal constant WINDOW = 7 days;

    function setUp() public {
        // Warp into the future so tests can set `regionLastChanged` timestamps
        // in the past (e.g. NOW - 8 days) without underflow.
        vm.warp(100 days);

        token = new Token(admin);
        bondMock = new MockRegionBond();
        blacklist = new ContentBlacklist(ICapacityBondEjector(address(bondMock)), token, admin, APPEAL_BOND);
        bondMock.setWindow(WINDOW);

        vm.prank(admin);
        token.transfer(filer, APPEAL_BOND * 10);
        vm.prank(filer);
        token.approve(address(blacklist), type(uint256).max);

        vm.startPrank(admin);
        blacklist.grantRole(blacklist.EMERGENCY_MULTISIG_ROLE(), multisig);
        blacklist.grantRole(blacklist.REGIONAL_BODY_ROLE(), regionalBody);
        vm.stopPrank();
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    function _addGlobal(bytes32 hash) internal {
        vm.prank(admin);
        blacklist.addHashGlobal(hash);
    }

    function _addRegional(bytes32 region, bytes32 hash) internal {
        vm.prank(regionalBody);
        blacklist.addHashRegional(region, hash);
    }

    // -----------------------------------------------------------------
    // Scope predicate — isOperatorInBlacklistScope
    // -----------------------------------------------------------------

    /// @notice A global entry is in scope for any operator, regardless of region.
    function test_scope_globalEntry_inScopeForAnyOperator() public {
        _addGlobal(HASH);
        bondMock.setNode(operator, "EU", "", 0, 0);
        assertTrue(blacklist.isOperatorInBlacklistScope(operator, HASH, GLOBAL_REGION));
    }

    /// @notice An entry whose region matches the operator's current region is in scope.
    function test_scope_currentRegionMatch_inScope() public {
        _addRegional(REGION_EU, HASH);
        bondMock.setNode(operator, "EU", "", 0, 0);
        assertTrue(blacklist.isOperatorInBlacklistScope(operator, HASH, REGION_EU));
    }

    /// @notice An entry in a region the operator does NOT serve and never
    ///         served is out of scope.
    function test_scope_unrelatedRegion_outOfScope() public {
        _addRegional(REGION_US, HASH);
        bondMock.setNode(operator, "EU", "", 0, 0);
        assertFalse(blacklist.isOperatorInBlacklistScope(operator, HASH, REGION_US));
    }

    /// @notice ADR 030 ripening leg: a region change that has NOT yet ripened
    ///         keeps the previous region's entries in scope (slash exposure
    ///         persists). Operator flipped US→EU "just now"; a US entry still
    ///         applies because `block.timestamp - effective < window`.
    function test_scope_unripenedRegionChange_prevRegionStillInScope() public {
        _addRegional(REGION_US, HASH);
        // Flipped to EU at NOW; previous region US; effective == NOW.
        bondMock.setNode(operator, "EU", "US", uint64(block.timestamp), 0);
        assertTrue(blacklist.isOperatorInBlacklistScope(operator, HASH, REGION_US));
    }

    /// @notice ADR 030 ripening leg: once the change has ripened
    ///         (`block.timestamp - effective >= window`), the previous region's
    ///         entries drop out of scope.
    function test_scope_ripenedRegionChange_prevRegionOutOfScope() public {
        _addRegional(REGION_US, HASH);
        // Flipped to EU 8 days ago (> 7-day window); US exposure has lapsed.
        bondMock.setNode(operator, "EU", "US", uint64(block.timestamp - 8 days), 0);
        assertFalse(blacklist.isOperatorInBlacklistScope(operator, HASH, REGION_US));
    }

    /// @notice Boundary: exactly at the window edge the change has ripened
    ///         (predicate uses strict `<`), so the previous region is out.
    function test_scope_exactlyAtWindowEdge_prevRegionOutOfScope() public {
        _addRegional(REGION_US, HASH);
        bondMock.setNode(operator, "EU", "US", uint64(block.timestamp - WINDOW), 0);
        assertFalse(blacklist.isOperatorInBlacklistScope(operator, HASH, REGION_US));
    }

    /// @notice A non-existent (never-added) entry is out of scope.
    function test_scope_entryNotLive_outOfScope() public {
        bondMock.setNode(operator, "US", "", 0, 0);
        assertFalse(blacklist.isOperatorInBlacklistScope(operator, HASH, REGION_US));
    }

    /// @notice A fast-track-suspended entry is out of scope (matches the
    ///         `_isLive` liveness used by the rest of the contract).
    function test_scope_suspendedEntry_outOfScope() public {
        _addRegional(REGION_US, HASH);
        // Give the filer ripe US standing so the appeal can be opened, then
        // fast-track it to suspend the entry.
        bondMock.setNode(filer, "US", "", 0, uint64(1));
        vm.prank(filer);
        uint256 appealId =
            blacklist.openBlacklistAppeal(HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator);
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);

        // Operator is unripened-in-scope for US, but the entry is suspended.
        bondMock.setNode(operator, "EU", "US", uint64(block.timestamp), 0);
        assertFalse(blacklist.isOperatorInBlacklistScope(operator, HASH, REGION_US));
    }

    // -----------------------------------------------------------------
    // Path-2 (Operator) standing — openBlacklistAppeal
    // -----------------------------------------------------------------

    /// @notice Operator standing inside the stability window reverts:
    ///         the filer flipped into US within `window`, so the region
    ///         hasn't ripened and permissionless standing is denied.
    function test_path2_insideWindow_revertsRegionNotRipened() public {
        _addRegional(REGION_US, HASH);
        // Flipped to US at NOW; effective == NOW; readyAt == NOW + window.
        bondMock.setNode(filer, "US", "EU", uint64(block.timestamp), 0);
        uint64 readyAt = uint64(block.timestamp) + uint64(WINDOW);
        vm.prank(filer);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.RegionNotRipened.selector, readyAt));
        blacklist.openBlacklistAppeal(HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator);
    }

    /// @notice Operator standing once the region change has ripened succeeds.
    function test_path2_outsideWindow_succeeds() public {
        _addRegional(REGION_US, HASH);
        // Flipped to US 8 days ago (> window); standing has ripened.
        bondMock.setNode(filer, "US", "EU", uint64(block.timestamp - 8 days), 0);
        vm.prank(filer);
        blacklist.openBlacklistAppeal(HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator);
        assertTrue(blacklist.hasActiveAppeal(REGION_US, HASH));
    }

    /// @notice Operator standing requires the disputed region to match the
    ///         filer's current region.
    function test_path2_regionMismatch_revertsUnauthorized() public {
        _addRegional(REGION_US, HASH);
        bondMock.setNode(filer, "EU", "", 0, 0);
        vm.prank(filer);
        vm.expectRevert(
            abi.encodeWithSelector(
                ContentBlacklist.UnauthorizedStanding.selector, ContentBlacklist.StandingPath.Operator
            )
        );
        blacklist.openBlacklistAppeal(HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator);
    }

    /// @notice There is no operator standing for a global entry (any operator
    ///         would qualify) — global disputes use paths 1/3.
    function test_path2_globalEntry_revertsUnauthorized() public {
        _addGlobal(HASH);
        bondMock.setNode(filer, "US", "", 0, 0);
        vm.prank(filer);
        vm.expectRevert(
            abi.encodeWithSelector(
                ContentBlacklist.UnauthorizedStanding.selector, ContentBlacklist.StandingPath.Operator
            )
        );
        blacklist.openBlacklistAppeal(HASH, GLOBAL_REGION, bytes32("e"), ContentBlacklist.StandingPath.Operator);
    }

    /// @notice The `effective` fallback uses `firstBondedAt` when the operator
    ///         has never called `updateRegion` (`regionLastChanged == 0`):
    ///         a long-bonded operator that never changed region has standing.
    function test_path2_neverChangedRegion_usesFirstBondedFallback() public {
        _addRegional(REGION_US, HASH);
        // regionLastChanged == 0 → effective = max(firstBondedAt, gate).
        // firstBondedAt 8 days ago; gate 0 ⇒ effective 8 days ago ⇒ ripened.
        bondMock.setNode(filer, "US", "", 0, uint64(block.timestamp - 8 days));
        vm.prank(filer);
        blacklist.openBlacklistAppeal(HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator);
        assertTrue(blacklist.hasActiveAppeal(REGION_US, HASH));
    }

    /// @notice `regionGateActivatedAt` wins over an ancient `firstBondedAt` in
    ///         the fallback: a node bonded long ago but never region-changed is
    ///         NOT auto-ripe the instant the gate deploys — standing is denied
    ///         until `gate + window`. This is the core anti-evasion guarantee
    ///         of the gate timestamp, and the branch the firstBonded-fallback
    ///         test above does not exercise (it sets gate = 0).
    function test_path2_gateWinsOverAncientFirstBonded_revertsNotRipened() public {
        _addRegional(REGION_US, HASH);
        bondMock.setGate(uint64(block.timestamp - 1 days)); // gate just activated
        // firstBondedAt is well past the window (would be ripe on its own), but
        // regionLastChanged == 0 ⇒ effective = max(firstBondedAt, gate) = gate
        // (1 day ago) ⇒ not ripe. Without the gate this filer would have standing.
        bondMock.setNode(filer, "US", "", 0, uint64(block.timestamp - 30 days));
        uint64 readyAt = uint64(block.timestamp - 1 days) + uint64(WINDOW);
        vm.prank(filer);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.RegionNotRipened.selector, readyAt));
        blacklist.openBlacklistAppeal(HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator);
    }

    // -----------------------------------------------------------------
    // Edge cases / regression
    // -----------------------------------------------------------------

    /// @notice An operator with no previous region (`regionPrev == ""`) is out
    ///         of scope for a real regional entry even while unripened: the
    ///         ripening leg's `region == _toRegionKey("")` is `region ==
    ///         bytes32(0)`, which never equals a real entry key. Guards the
    ///         "empty prev ⇒ no second region" invariant the predicate relies on.
    function test_scope_emptyPrevRegion_unripened_outOfScope() public {
        _addRegional(REGION_US, HASH);
        // Current region EU, no prior region, clock says unripened.
        bondMock.setNode(operator, "EU", "", uint64(block.timestamp), 0);
        assertFalse(blacklist.isOperatorInBlacklistScope(operator, HASH, REGION_US));
    }

    /// @notice The predicate must not revert when `effective >= block.timestamp`
    ///         (the leg is written `now < effective + window`, not a
    ///         subtraction). A future-dated `effective` is treated as unripened,
    ///         so a prev-region entry stays in scope without an underflow panic.
    function test_scope_effectiveNotInPast_noUnderflowRevert() public {
        _addRegional(REGION_US, HASH);
        // effective == now + 1 day (future): no revert, still unripened ⇒ in scope.
        bondMock.setNode(operator, "EU", "US", uint64(block.timestamp + 1 days), 0);
        assertTrue(blacklist.isOperatorInBlacklistScope(operator, HASH, REGION_US));
    }

    /// @notice A full 16-byte (`MAX_REGION_HINT_BYTES`) region round-trips
    ///         through `_toRegionKey` and matches its `bytes32(...)` entry key,
    ///         exercising the masking path for a non-trivial length.
    function test_scope_maxLengthRegionKey_matches() public {
        bytes32 region16 = bytes32("abcdefghijklmnop"); // exactly 16 bytes
        vm.prank(regionalBody);
        blacklist.addHashRegional(region16, HASH);
        bondMock.setNode(operator, "abcdefghijklmnop", "", 0, 0);
        assertTrue(blacklist.isOperatorInBlacklistScope(operator, HASH, region16));
    }

    /// @notice Region keys that share a prefix but differ in length do not
    ///         collide: an operator in "US" is not in scope for a "USX" entry.
    function test_scope_prefixSharedDifferentLength_noCollision() public {
        bytes32 regionUSX = bytes32("USX");
        vm.prank(regionalBody);
        blacklist.addHashRegional(regionUSX, HASH);
        bondMock.setNode(operator, "US", "", 0, 0);
        assertFalse(blacklist.isOperatorInBlacklistScope(operator, HASH, regionUSX));
    }
}
