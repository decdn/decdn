// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";

import { BuybackVenueLib } from "../script/lib/BuybackVenueLib.sol";
import { GuardedBuybackBurner } from "../src/GuardedBuybackBurner.sol";

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

    /// @notice What the ordinals actually protect is the ZERO value, not the
    ///         dispatch: `_deriveTokenSeed` compares symbolically (`== Venue.BALANCER`)
    ///         so a reorder leaves its branch behaving identically. But
    ///         `Venue.UNISWAP == 0` means a default-constructed `BuybackActivation` or
    ///         `PoolSeed` carries the Uniswap tag rather than an obviously-unset one,
    ///         which is why `_assertVenueSeedMatches` re-derives instead of trusting
    ///         the tag. The raw `uint8(venue)` cast in `UnknownVenueVariant` also
    ///         surfaces these numbers to operators.
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

    // -----------------------------------------------------------------
    // Construction guards — the chokepoint BOTH deploy paths share
    // -----------------------------------------------------------------

    /// @notice `wiring.vault` is the one field with no downstream backstop. A zero
    ///         router or permit2 hits `ZeroAddress` in `BuybackBurnerBalancerV3`'s
    ///         constructor; a zero vault is that contract's documented
    ///         deferred-wiring path, so it skips validation and the burner reverts
    ///         `PoolNotWired` on every swap forever while holding 30% of revenue.
    ///         Tested here rather than through `_runFullDeploy` because the guard
    ///         fires before `new BuybackBurnerBalancerV3(...)`, so it needs no live
    ///         factory, vault or pool — and because the library is the seam that gives
    ///         `ActivateBuyback` the same protection the genesis path gets.
    function test_balancerBurner_rejectsIncompleteWiring() public {
        string[5] memory fields =
            ["balancer.swapRouter", "balancer.pool", "balancer.vault", "balancer.permit2", "balancer.subSwapCount"];
        for (uint256 i = 0; i < fields.length; i++) {
            vm.expectRevert(abi.encodeWithSelector(BuybackVenueLib.WiringIncomplete.selector, fields[i]));
            harness.deployBalancer(_wiringWithFieldCleared(i), _liveGuard());
        }
    }

    function test_uniswapBurner_rejectsIncompleteWiring() public {
        vm.expectRevert(abi.encodeWithSelector(BuybackVenueLib.WiringIncomplete.selector, "uniswap.swapRouter"));
        harness.deployUniswap(address(0), address(0xB001), _liveGuard());

        vm.expectRevert(abi.encodeWithSelector(BuybackVenueLib.WiringIncomplete.selector, "uniswap.pool"));
        harness.deployUniswap(address(0x8081), address(0), _liveGuard());
    }

    /// @notice `min == max == 0` passes every bound `GuardedBuybackBurner`'s
    ///         constructor checks — it rejects an inverted band, not a zero one — and
    ///         then reverts `AboveMaxBuyback` on every call forever. Reachable from
    ///         production env as `MIN_BUYBACK_AMOUNT=0 MAX_BUYBACK_AMOUNT=0`, the
    ///         "0 means unlimited" misreading.
    function test_rejectsDeadGuardBand() public {
        GuardedBuybackBurner.GuardParams memory dead = _liveGuard();
        dead.minBuybackAmount_ = 0;
        dead.maxBuybackAmount_ = 0;

        vm.expectRevert(BuybackVenueLib.GuardBandDead.selector);
        harness.deployUniswap(address(0x8081), address(0xB001), dead);

        vm.expectRevert(BuybackVenueLib.GuardBandDead.selector);
        harness.deployBalancer(_wiring(), dead);
    }

    function _liveGuard() internal pure returns (GuardedBuybackBurner.GuardParams memory) {
        return GuardedBuybackBurner.GuardParams({
            twapMinWindow_: 1800,
            maxBuybackAmount_: 10_000e6,
            minBuybackAmount_: 100e6,
            slippageBps_: 200,
            epochLiquidityCapFraction_: 1000
        });
    }

    function _wiring() internal pure returns (BuybackVenueLib.BalancerWiring memory) {
        return BuybackVenueLib.BalancerWiring({
            swapRouter: address(0x8081),
            pool: address(0xB001),
            vault: address(0xA017),
            permit2: BuybackVenueLib.CANONICAL_PERMIT2,
            subSwapCount: 4,
            subSwapMinBlockGap: 10
        });
    }

    /// @dev `_wiring()` with field `i` zeroed, in the order the guard checks them.
    function _wiringWithFieldCleared(uint256 i) internal pure returns (BuybackVenueLib.BalancerWiring memory w) {
        w = _wiring();
        if (i == 0) w.swapRouter = address(0);
        else if (i == 1) w.pool = address(0);
        else if (i == 2) w.vault = address(0);
        else if (i == 3) w.permit2 = address(0);
        else w.subSwapCount = 0;
    }

    /// @notice The Permit2 default every Balancer seed approves through when
    ///         `PERMIT2_ADDRESS` is unset. A typo sends the seed's approvals at a dead
    ///         address, and the wiring guard only checks it non-zero.
    function test_canonicalPermit2Pinned() public pure {
        assertEq(BuybackVenueLib.CANONICAL_PERMIT2, 0x000000000022D473030F116dDEE9F6B43aC78BA3, "canonical Permit2");
    }

    function test_steadySharesMatchAdr026Split() public pure {
        uint256[3] memory shares = BuybackVenueLib.steadyShares();
        assertEq(shares[0], 6000, "operator");
        assertEq(shares[1], 3000, "buyback");
        assertEq(shares[2], 1000, "treasury");
        assertEq(shares[0] + shares[1] + shares[2], 10_000, "sums to 10_000 bps");
    }
}

/// @dev External boundary for `vm.expectRevert` — library `internal` functions inline
///      into the caller, so a direct call would revert inside the test frame.
contract VenueParserHarness {
    function parse(string memory venue) external pure returns (BuybackVenueLib.Venue) {
        return BuybackVenueLib.parseVenue(venue);
    }

    /// @dev Token args are pass-through and never touched before the guards revert, so
    ///      these need no live contracts. A call that gets past the guards will fail on
    ///      the real deployment — which is fine, every test here asserts a revert.
    function deployBalancer(BuybackVenueLib.BalancerWiring memory wiring, GuardedBuybackBurner.GuardParams memory guard)
        external
        returns (GuardedBuybackBurner)
    {
        return BuybackVenueLib.deployBalancerBurner(
            IERC20(address(0xDEC)), ERC20Burnable(address(0xDEC)), address(0xA), wiring, guard
        );
    }

    function deployUniswap(address swapRouter, address pool, GuardedBuybackBurner.GuardParams memory guard)
        external
        returns (GuardedBuybackBurner)
    {
        return BuybackVenueLib.deployUniswapBurner(
            IERC20(address(0xDEC)), ERC20Burnable(address(0xDEC)), address(0xA), swapRouter, pool, guard
        );
    }
}
