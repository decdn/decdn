// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { BuybackVenueLib } from "../script/lib/BuybackVenueLib.sol";
import { GuardedBuybackBurner } from "../src/GuardedBuybackBurner.sol";

/// @title BuybackVenueLibTest — the burner-construction seam both deploy paths share.
/// @notice `BuybackVenueLib` is the single chokepoint the deploy-time genesis path
///         and the post-deploy runbook go through (issue #1090), so the two entry
///         points cannot wire different burners from the same inputs. This suite
///         pins the wiring guards and the steady-state split it owns.
/// @dev    A library's `internal` functions inline into the caller, so the harness
///         below exists to give `vm.expectRevert` an external call boundary.
contract BuybackVenueLibTest is Test {
    VenueGuardHarness internal harness;

    function setUp() public {
        harness = new VenueGuardHarness();
    }

    // -----------------------------------------------------------------
    // Construction guards — the chokepoint BOTH deploy paths share
    // -----------------------------------------------------------------

    function test_uniswapBurner_rejectsIncompleteWiring() public {
        vm.expectRevert(abi.encodeWithSelector(BuybackVenueLib.WiringIncomplete.selector, "uniswap.swapRouter"));
        harness.requireUniswap(address(0), address(0xB001), _liveGuard());

        vm.expectRevert(abi.encodeWithSelector(BuybackVenueLib.WiringIncomplete.selector, "uniswap.pool"));
        harness.requireUniswap(address(0x8081), address(0), _liveGuard());
    }

    /// @notice `min == max == 0` reverts `AboveMaxBuyback` on every non-zero
    ///         buyback until governance raises the ceiling (48h timelock), while
    ///         the FeeRouter keeps accruing into the burner. Reachable from
    ///         production env as `MIN_BUYBACK_AMOUNT=0 MAX_BUYBACK_AMOUNT=0`, the
    ///         "0 means unlimited" misreading. `GuardedBuybackBurner` rejects it
    ///         too since #1532 (`BuybackBandDead`); this pins the library's
    ///         pre-broadcast fail-fast, so a mis-set env var surfaces before the
    ///         deploy transaction rather than as a reverted deployment mid-run.
    function test_rejectsDeadGuardBand() public {
        GuardedBuybackBurner.GuardParams memory dead = _liveGuard();
        dead.minBuybackAmount_ = 0;
        dead.maxBuybackAmount_ = 0;

        vm.expectRevert(BuybackVenueLib.GuardBandDead.selector);
        harness.requireUniswap(address(0x8081), address(0xB001), dead);
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

    /// With the reference `operatorBps` of 6000, the steady split gives the
    /// operator 60%, the buyback 30% and the treasury 10% of a redemption.
    function test_steadySplitMatchesAdr026Shares() public pure {
        (uint256 rebateBps, uint256 buybackBps) = BuybackVenueLib.steadySplit();
        uint256 protocolShare = 10_000 - 6000;
        uint256 operator = 6000 + (protocolShare * rebateBps) / 10_000;
        uint256 buyback = (protocolShare * buybackBps) / 10_000;
        assertEq(operator, 6000, "operator");
        assertEq(buyback, 3000, "buyback");
        assertEq(10_000 - operator - buyback, 1000, "treasury");
    }
}

/// @dev External boundary for `vm.expectRevert` — library `internal` functions inline
///      into the caller, so a direct call would revert inside the test frame.
contract VenueGuardHarness {
    /// @dev Calls the VALIDATOR, not the builder. A harness that called
    ///      `deployUniswapBurner` would inline the burner's creation bytecode and
    ///      exceed EIP-170, failing the `--sizes` CI gate — which is why the guard
    ///      lives in its own function.
    function requireUniswap(address swapRouter, address pool, GuardedBuybackBurner.GuardParams memory guard)
        external
        pure
    {
        BuybackVenueLib.requireUniswapWiring(swapRouter, pool, guard);
    }
}
