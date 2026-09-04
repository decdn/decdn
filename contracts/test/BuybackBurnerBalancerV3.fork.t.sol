// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { BuybackBurnerBalancerV3 } from "../src/BuybackBurnerBalancerV3.sol";
import { IBalancerV3Router } from "../src/interfaces/IBalancerV3Router.sol";

import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

/// @title BuybackBurnerBalancerV3 — Arbitrum One fork tests
/// @notice Exercises the Balancer V3 read + validation surface against the LIVE
///         deployed Vault, Router, and a real weighted pool — the one thing the
///         hand-written mocks in `BuybackBurnerBalancerV3.t.sol` cannot verify:
///         that our `IBalancerV3*` interface ABIs match the deployed bytecode,
///         and that `_poolState`/`_spotPrice`/the TWAP accumulator read real
///         pool data correctly. The swap+burn path is intentionally NOT covered
///         here — it needs the burnable TOKEN to exist inside a real pool, which
///         no mainnet has pre-launch (tracked separately; see the PR).
/// @dev    GATED: the suite self-skips unless `ARBITRUM_RPC_URL` is set, so the
///         default offline `forge test` stays green. Pinning a historical block
///         requires an ARCHIVE endpoint — public Arbitrum RPCs prune state and
///         answer a pinned block with "metadata is not found". CI runs this via
///         the `solidity fork test` job (FOUNDRY_PROFILE=fork) with the secret.
contract BuybackBurnerBalancerV3ForkTest is Test {
    // Real Balancer V3 on Arbitrum One (balancer-deployments/addresses/arbitrum.json).
    address constant VAULT = 0xbA1333333333a1BA1108E8412f11850A5C319bA9;
    address constant ROUTER = 0xEAedc32a51c510d35ebC11088fD5fF2b47aACF2E; // 20250307-v3-router-v2

    // Live V3 weighted pool "50USDC 50WETH" (createTime 2025-07-31). WETH stands
    // in for TOKEN — the contract only requires the configured {usdc, token}
    // addresses to be present pool legs; the swap/burn path is not exercised.
    address constant POOL = 0x9F52eF16f2Cd76B727460c86eB81A235165161c1;
    address constant USDC = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831; // 6 decimals
    address constant WETH = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
    // Canonical Uniswap Permit2 (same address on every chain). Unused by these
    // read-only tests (no swap), but the constructor requires it non-zero.
    address constant PERMIT2 = 0x000000000022D473030F116dDEE9F6B43aC78BA3;

    // Pinned for determinism + forge RPC-cache reuse. Safely post-dates the pool
    // (2025-07-31) and below the chain tip. Refresh to any block an archive node
    // serves that contains the pool. Requires an ARCHIVE endpoint to resolve.
    uint256 constant FORK_BLOCK = 470_000_000;

    BuybackBurnerBalancerV3 internal bb;
    bool internal forkActive;

    function setUp() public {
        // Self-skip when no RPC is configured (offline / fork PRs without the
        // secret): leaves the default `forge test` suite green and network-free.
        if (bytes(vm.envOr("ARBITRUM_RPC_URL", string(""))).length == 0) return;
        vm.createSelectFork(vm.rpcUrl("arbitrum"), FORK_BLOCK);
        forkActive = true;

        BuybackBurnerBalancerV3.Config memory cfg = BuybackBurnerBalancerV3.Config({
            swapRouter_: IBalancerV3Router(ROUTER),
            pool_: POOL,
            vault_: VAULT,
            permit2_: PERMIT2,
            twapMinWindow_: 30 minutes,
            maxBuybackAmount_: 1_000_000e6,
            minBuybackAmount_: 1e6,
            slippageBps_: 200,
            epochLiquidityCapFraction_: 1000
        });

        // The constructor runs `_validatePoolWiring()` against the LIVE Vault and
        // pool: if any `IBalancerV3*` ABI disagrees with deployed bytecode, or
        // the pool is mis-read, this reverts. That is the core fork coverage.
        bb = new BuybackBurnerBalancerV3(IERC20(USDC), ERC20Burnable(WETH), address(this), cfg);
    }

    modifier requiresFork() {
        if (!forkActive) {
            vm.skip(true);
            return;
        }
        _;
    }

    /// @notice Constructor-level validation passed against the live pool, and the
    ///         decimals-derived scaling factor was read from real USDC.
    function test_constructor_validates_against_live_pool() public requiresFork {
        assertEq(address(bb.swapRouter()), ROUTER);
        assertEq(bb.usdcTo18(), 1e12); // 10 ** (18 - 6)
    }

    /// @notice The TWAP accumulator samples the real pool spot and produces a
    ///         non-zero time-weighted price after the maturity window elapses.
    function test_twap_accumulator_reads_real_pool() public requiresFork {
        bb.poke(); // initialize accumulator from the live marginal spot
        skip(31 minutes);
        bb.poke(); // advance past twapMinWindow; slide the window
        uint256 p = bb.twapPrice();
        assertGt(p, 0, "twap should be non-zero from real pool state");
        emit log_named_uint("twap TOKEN-per-USDC (1e18)", p);
    }
}
