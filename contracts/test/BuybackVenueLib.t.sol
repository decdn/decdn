// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { BuybackVenueLib } from "../script/lib/BuybackVenueLib.sol";

/// @title BuybackVenueLibTest — the venue string→enum boundary.
/// @notice `parseVenue` decides which pool the protocol's revenue is swapped
///         against, and it is now the single dispatch both the deploy-time genesis
///         path and the post-deploy runbook go through (issue #1090). Before the
///         consolidation there were two copies and neither had a test; consolidating
///         also turned two `internal view` env-readers into one `internal pure`
///         function, which is what makes this suite possible at all.
/// @dev    A library's `internal` functions inline into the caller, so the harness
///         below exists to give `vm.expectRevert` an external call boundary.
contract BuybackVenueLibTest is Test {
    VenueParserHarness internal harness;

    function setUp() public {
        harness = new VenueParserHarness();
    }

    function test_parsesBothSupportedVenues() public view {
        assertEq(uint256(harness.parse("uniswap")), uint256(BuybackVenueLib.Venue.UNISWAP), "uniswap");
        // Pinned explicitly: a mutation returning UNISWAP here would otherwise be
        // caught only by the RPC-gated Balancer fork suite, which does not run in CI.
        assertEq(uint256(harness.parse("balancer")), uint256(BuybackVenueLib.Venue.BALANCER), "balancer");
    }

    /// @notice The enum ordering is load-bearing beyond dispatch: `_deriveTokenSeed`
    ///         branches on `== BALANCER` to pick 80/20 vs 1:1 seed weights, so a
    ///         reorder silently changes the genesis pool's anchor price.
    function test_venueOrdinalsFrozen() public pure {
        assertEq(uint8(BuybackVenueLib.Venue.UNISWAP), 0, "UNISWAP ordinal");
        assertEq(uint8(BuybackVenueLib.Venue.BALANCER), 1, "BALANCER ordinal");
    }

    function test_revertsOnUnknownVenue() public {
        vm.expectRevert(abi.encodeWithSelector(BuybackVenueLib.UnknownBuybackVenue.selector, "curve"));
        harness.parse("curve");
    }

    /// @notice The docstring promises "case-sensitive and exact — a typo reverts
    ///         rather than silently defaulting". Asserted rather than trusted,
    ///         because a lenient parse would route revenue at the wrong venue.
    function test_isCaseSensitiveAndExact() public {
        string[4] memory nearMisses = ["Uniswap", "UNISWAP", "uniswap ", ""];
        for (uint256 i = 0; i < nearMisses.length; i++) {
            vm.expectRevert(abi.encodeWithSelector(BuybackVenueLib.UnknownBuybackVenue.selector, nearMisses[i]));
            harness.parse(nearMisses[i]);
        }
    }

    function test_steadySharesMatchAdr026Split() public pure {
        uint256[3] memory shares = BuybackVenueLib.steadyShares();
        assertEq(shares[0], 6000, "operator");
        assertEq(shares[1], 3000, "buyback");
        assertEq(shares[2], 1000, "treasury");
        assertEq(shares[0] + shares[1] + shares[2], 10_000, "sums to 10_000 bps");
    }
}

contract VenueParserHarness {
    function parse(string memory venue) external pure returns (BuybackVenueLib.Venue) {
        return BuybackVenueLib.parseVenue(venue);
    }
}
