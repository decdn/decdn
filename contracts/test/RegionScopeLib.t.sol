// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { RegionScopeLib } from "../src/RegionScopeLib.sol";

/// @dev Thin harness so the `internal` library functions are reachable from the
///      test (and so `pack` can be fed a deliberately dirty-padded memory string).
contract RegionScopeHarness {
    function pack(string memory s) external pure returns (bytes32) {
        return RegionScopeLib.pack(s);
    }

    function effectiveSince(uint64 rlc, uint64 fb, uint64 gate) external pure returns (uint64) {
        return RegionScopeLib.effectiveSince(rlc, fb, gate);
    }

    function scopedRegions(
        bytes32 globalRegion,
        string memory regionHint,
        string memory regionPrev,
        uint64 nowTs,
        uint64 effective,
        uint256 window
    ) external pure returns (bytes32 currentKey, bytes32 prevKey, bool prevApplies) {
        return RegionScopeLib.scopedRegions(globalRegion, regionHint, regionPrev, nowTs, effective, window);
    }

    /// @dev Builds a length-`len` string whose backing memory word has all 32
    ///      bytes set to 0xFF, so bytes beyond `len` are dirty. Exercises the
    ///      mask in `pack` (Solidity does not guarantee padding bytes are zero).
    function packDirty(uint256 len) external pure returns (bytes32) {
        string memory s;
        assembly ("memory-safe") {
            s := mload(0x40)
            mstore(s, len)
            mstore(add(s, 32), not(0)) // all-0xFF data word (dirty tail)
            mstore(0x40, add(s, 64))
        }
        return RegionScopeLib.pack(s);
    }
}

contract RegionScopeLibTest is Test {
    bytes32 internal constant GLOBAL_REGION = bytes32("GLOBAL");

    RegionScopeHarness internal h;

    function setUp() public {
        h = new RegionScopeHarness();
    }

    // --- pack ---------------------------------------------------------------

    function test_pack_matchesLiteralPacking() public view {
        assertEq(h.pack("US"), bytes32("US"));
        assertEq(h.pack("us-east"), bytes32("us-east"));
        assertEq(h.pack("GLOBAL"), GLOBAL_REGION);
    }

    function test_pack_emptyIsZero() public view {
        assertEq(h.pack(""), bytes32(0));
    }

    function test_pack_fullSixteenBytesRoundTrips() public view {
        // 16-byte region (MAX_REGION_HINT_BYTES) packs without truncation.
        assertEq(h.pack("abcdefghijklmnop"), bytes32("abcdefghijklmnop"));
    }

    function test_pack_masksDirtyTrailingBytes() public view {
        // A 2-byte string over an all-0xFF word must still mask to the top 2 bytes.
        assertEq(h.packDirty(2), bytes32(hex"ffff"));
        // A 0-length dirty string is still bytes32(0).
        assertEq(h.packDirty(0), bytes32(0));
    }

    // --- effectiveSince -----------------------------------------------------

    function test_effectiveSince_lastChangedWins() public view {
        // regionLastChanged != 0 takes precedence over both fallbacks.
        assertEq(h.effectiveSince(500, 100, 300), 500);
    }

    function test_effectiveSince_fallbackPicksMax() public view {
        // Never changed: max(firstBondedAt, gate). Both orderings.
        assertEq(h.effectiveSince(0, 400, 300), 400); // firstBonded > gate
        assertEq(h.effectiveSince(0, 100, 300), 300); // gate > firstBonded
        assertEq(h.effectiveSince(0, 300, 300), 300); // equal
    }

    // --- scopedRegions ------------------------------------------------------

    uint64 internal constant NOW = 1_000_000;
    uint256 internal constant WINDOW = 7 days;

    function test_scopedRegions_currentOnly_afterWindow() public view {
        // Ripened (effective exactly WINDOW ago): only current applies, prev does not.
        uint64 effective = NOW - uint64(WINDOW);
        (bytes32 cur, bytes32 prev, bool prevApplies) =
            h.scopedRegions(GLOBAL_REGION, "us-east", "eu-west", NOW, effective, WINDOW);
        assertEq(cur, bytes32("us-east"));
        assertFalse(prevApplies);
        assertEq(prev, bytes32(0));
    }

    function test_scopedRegions_prevAppliesInWindow() public view {
        // Changed WINDOW/2 ago: both current and prev apply.
        uint64 effective = NOW - uint64(WINDOW) / 2;
        (bytes32 cur, bytes32 prev, bool prevApplies) =
            h.scopedRegions(GLOBAL_REGION, "eu-west", "us-east", NOW, effective, WINDOW);
        assertEq(cur, bytes32("eu-west"));
        assertTrue(prevApplies);
        assertEq(prev, bytes32("us-east"));
    }

    function test_scopedRegions_windowBoundaryIsExclusive() public view {
        // Exactly == window: NOT in window (boundary exclusive). Mirrors `< window`.
        (,, bool atBoundary) = h.scopedRegions(GLOBAL_REGION, "eu-west", "us-east", NOW, NOW - uint64(WINDOW), WINDOW);
        assertFalse(atBoundary);
        // One second inside the window: prev applies.
        (,, bool justInside) =
            h.scopedRegions(GLOBAL_REGION, "eu-west", "us-east", NOW, NOW - uint64(WINDOW) + 1, WINDOW);
        assertTrue(justInside);
    }

    function test_scopedRegions_currentGlobalZeroedOut() public view {
        // An operator whose current region literally packs to GLOBAL is not a
        // regional-leg match (GLOBAL is the caller's separate concern).
        (bytes32 cur,,) = h.scopedRegions(GLOBAL_REGION, "GLOBAL", "", NOW, NOW, WINDOW);
        assertEq(cur, bytes32(0));
    }

    function test_scopedRegions_emptyPrevNeverApplies() public view {
        // Never-changed node (prev ""): the ripening leg cannot match a real key.
        uint64 effective = NOW - 1; // well inside the window
        (bytes32 cur, bytes32 prev, bool prevApplies) =
            h.scopedRegions(GLOBAL_REGION, "eu-west", "", NOW, effective, WINDOW);
        assertEq(cur, bytes32("eu-west"));
        assertFalse(prevApplies);
        assertEq(prev, bytes32(0));
    }

    function test_scopedRegions_prevEqualsCurrentDeduped() public view {
        // Re-flip US -> EU -> US: prev == current, so prev does not double-count.
        uint64 effective = NOW - 1;
        (bytes32 cur,, bool prevApplies) = h.scopedRegions(GLOBAL_REGION, "us-east", "us-east", NOW, effective, WINDOW);
        assertEq(cur, bytes32("us-east"));
        assertFalse(prevApplies);
    }

    function test_scopedRegions_prevGlobalNeverApplies() public view {
        // An operator whose prev region packs to GLOBAL is not a ripening match.
        uint64 effective = NOW - 1;
        (,, bool prevApplies) = h.scopedRegions(GLOBAL_REGION, "eu-west", "GLOBAL", NOW, effective, WINDOW);
        assertFalse(prevApplies);
    }
}
