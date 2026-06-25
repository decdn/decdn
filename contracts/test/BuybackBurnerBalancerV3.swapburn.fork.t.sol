// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { BuybackBurnerBalancerV3 } from "../src/BuybackBurnerBalancerV3.sol";
import { IBalancerV3Router } from "../src/interfaces/IBalancerV3Router.sol";
import { IBalancerV3Vault } from "../src/interfaces/IBalancerV3Vault.sol";
import { IPermit2 } from "../src/interfaces/IPermit2.sol";
import { IBalancerV3RouterInit, IBalancerV3WeightedPoolFactory } from "./interfaces/IBalancerV3PoolCreation.sol";

/// @dev 6-decimal stand-in for USDC (the bond/settlement currency).
contract MintableUSDC is ERC20 {
    constructor() ERC20("Mock USD Coin", "USDC") { }

    function decimals() public pure override returns (uint8) {
        return 6;
    }

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

/// @dev 18-decimal burnable stand-in for the protocol TOKEN. The swap+burn path
///      requires the TOKEN leg to be `ERC20Burnable` so `BuybackBurner` can
///      `.burn()` the swapped proceeds.
contract MintableBurnableToken is ERC20Burnable {
    constructor() ERC20("Mock deCDN Token", "TOKEN") { }

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

/// @title BuybackBurnerBalancerV3 — Ethereum Sepolia swap+burn fork tests
/// @notice Closes the one coverage gap the mocks and the Arbitrum One fork tests
///         (`BuybackBurnerBalancerV3.fork.t.sol`) cannot reach (issue #995): a
///         REAL `executeBuyback` that swaps USDC->TOKEN through a live Balancer
///         V3 Router against real 80/20 weighted-pool math, then burns the
///         proceeds. The unit-test Router mock returns a preset `amountOut` and
///         never exercises pool math, price impact, fee realization, or the
///         actual token-pull/settlement; the Arbitrum One fork uses non-burnable
///         WETH as a TOKEN stand-in and skips the swap entirely.
///
///         Our burnable TOKEN exists in no live pool pre-launch (the 80/20 POL
///         pool is seeded only at launch — ADR 018), so this test forks Ethereum
///         Sepolia — where Balancer V3 *is* deployed — and builds the pool it
///         needs IN-TEST: deploy a burnable TOKEN + a 6-dec USDC, create a real
///         80/20 weighted pool via the live `WeightedPoolFactory`, seed it via
///         the live Router (Permit2), then drive `executeBuyback`.
/// @dev    GATED: self-skips unless `SEPOLIA_RPC_URL` is set, so the default
///         offline `forge test` stays green and network-free. Unlike the
///         Arbitrum One fork tests this forks at LATEST (no pinned block) and so
///         needs NO archive endpoint: every price-relevant token/pool is created
///         inside the test, so only the stable Balancer V3 + Permit2 bytecode is
///         read from the fork — none of the assertions depend on chain height.
///         CI runs this via the `solidity fork test` job (FOUNDRY_PROFILE=fork).
contract BuybackBurnerBalancerV3SwapBurnForkTest is Test {
    // Balancer V3 on Ethereum Sepolia (balancer/balancer-deployments,
    // addresses/sepolia.json). The Vault is the canonical CREATE2 address shared
    // across every V3 chain; Router + WeightedPoolFactory are Sepolia-specific.
    address constant VAULT = 0xbA1333333333a1BA1108E8412f11850A5C319bA9;
    address constant ROUTER = 0x5e315f96389C1aaF9324D97d3512ae1e0Bf3C21a; // 20250307-v3-router-v2
    address constant FACTORY = 0xc383B240B40660cca6c5b6Fbf4fbAb85E9F4de24; // v3-weighted-pool
    // Canonical Uniswap Permit2 (same address on every chain).
    address constant PERMIT2 = 0x000000000022D473030F116dDEE9F6B43aC78BA3;

    uint256 constant WAD = 1e18;
    uint256 constant BPS = 10_000;

    // 80/20 TOKEN/USDC seed (ADR 018 proportions, scaled to test units): ~250k
    // USDC against ~100M TOKEN -> the pool prices TOKEN at ~$0.01.
    uint256 constant USDC_SEED = 250_000e6;
    uint256 constant TOKEN_SEED = 100_000_000e18;
    uint256 constant SWAP_FEE = 1e16; // 1% (ADR 018 pool fee), within V3 weighted bounds.

    // Buyback parameters mirroring the Arbitrum One fork test cfg.
    uint256 constant SLIPPAGE_BPS = 200; // 2%
    uint256 constant EPOCH_CAP_FRACTION = 1000; // 10%
    uint256 constant AMOUNT_IN = 100e6; // small vs. depth -> price impact << slippage band.

    // Deterministic from the seed + 80/20 weights (the pool is created in-test):
    //   spot = (wUsdc * balToken18) / (balUsdc18 * wToken)
    //        = (0.2e18 * 1e26) / (2.5e23 * 0.8e18) scaled = 100e18 TOKEN per USDC.
    uint256 constant EXPECTED_SPOT = 100e18; // TOKEN per USDC, 1e18 fixed point
    // Marginal (linear, fee-free) out for AMOUNT_IN at EXPECTED_SPOT:
    //   AMOUNT_IN(100e6) * usdcTo18(1e12) * 100e18 / 1e18 = 10_000e18. Realized
    //   exact-in output sits just below this (1% fee + small price impact).
    uint256 constant MARGINAL_OUT = 10_000e18;

    MintableUSDC internal usdc;
    MintableBurnableToken internal token;
    BuybackBurnerBalancerV3 internal bb;
    address internal pool;
    bool internal forkActive;

    function setUp() public {
        // Self-skip when no RPC is configured (offline / fork PRs without the
        // secret): leaves the default `forge test` suite green and network-free.
        if (bytes(vm.envOr("SEPOLIA_RPC_URL", string(""))).length == 0) return;
        vm.createSelectFork(vm.rpcUrl("sepolia"));
        forkActive = true;

        usdc = new MintableUSDC();
        token = new MintableBurnableToken();
        pool = _createAndSeedPool();

        BuybackBurnerBalancerV3.Config memory cfg = BuybackBurnerBalancerV3.Config({
            swapRouter_: IBalancerV3Router(ROUTER),
            pool_: pool,
            vault_: VAULT,
            permit2_: PERMIT2,
            subSwapCount_: 4,
            subSwapMinBlockGap_: 1,
            twapMinWindow_: 30 minutes,
            maxBuybackAmount_: 1_000_000e6,
            minBuybackAmount_: 1e6,
            slippageBps_: SLIPPAGE_BPS,
            epochLiquidityCapFraction_: EPOCH_CAP_FRACTION
        });
        bb = new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), address(this), cfg);
        bb.setKeeper(address(this)); // grants KEEPER_ROLE to the test (GOVERNANCE_ROLE held via admin).
    }

    modifier requiresFork() {
        if (!forkActive) {
            vm.skip(true);
            return;
        }
        _;
    }

    // -----------------------------------------------------------------
    // Pool creation + seeding (live factory + router, Permit2)
    // -----------------------------------------------------------------

    /// @dev Creates a real 80/20 TOKEN/USDC weighted pool via the live factory
    ///      and seeds it through the live Router. Tokens are sorted ascending by
    ///      address (Vault `registerPool` invariant); weights track the sorted
    ///      order so TOKEN keeps 80% and USDC 20%.
    function _createAndSeedPool() internal returns (address newPool) {
        bool usdcFirst = address(usdc) < address(token);

        IBalancerV3WeightedPoolFactory.TokenConfig[] memory tokens = new IBalancerV3WeightedPoolFactory.TokenConfig[](2);
        uint256[] memory weights = new uint256[](2);
        {
            IBalancerV3WeightedPoolFactory.TokenConfig memory usdcCfg = IBalancerV3WeightedPoolFactory.TokenConfig({
                token: IERC20(address(usdc)),
                tokenType: IBalancerV3WeightedPoolFactory.TokenType.STANDARD,
                rateProvider: address(0),
                paysYieldFees: false
            });
            IBalancerV3WeightedPoolFactory.TokenConfig memory tokenCfg = IBalancerV3WeightedPoolFactory.TokenConfig({
                token: IERC20(address(token)),
                tokenType: IBalancerV3WeightedPoolFactory.TokenType.STANDARD,
                rateProvider: address(0),
                paysYieldFees: false
            });
            tokens[0] = usdcFirst ? usdcCfg : tokenCfg;
            tokens[1] = usdcFirst ? tokenCfg : usdcCfg;
            weights[0] = usdcFirst ? 0.2e18 : 0.8e18;
            weights[1] = usdcFirst ? 0.8e18 : 0.2e18;
        }

        IBalancerV3WeightedPoolFactory.PoolRoleAccounts memory roles = IBalancerV3WeightedPoolFactory.PoolRoleAccounts({
            pauseManager: address(0), swapFeeManager: address(0), poolCreator: address(0)
        });

        newPool = IBalancerV3WeightedPoolFactory(FACTORY)
            .create(
                "deCDN 80TOKEN-20USDC",
                "dcdn-8020",
                tokens,
                weights,
                roles,
                SWAP_FEE,
                address(0), // no hooks
                false, // enableDonation
                false, // disableUnbalancedLiquidity
                bytes32(uint256(0x9951)) // salt
            );

        // Seed initial liquidity through the Router (it pulls via Permit2).
        IERC20[] memory initTokens = new IERC20[](2);
        uint256[] memory initAmounts = new uint256[](2);
        initTokens[0] = IERC20(address(tokens[0].token));
        initTokens[1] = IERC20(address(tokens[1].token));
        initAmounts[0] = usdcFirst ? USDC_SEED : TOKEN_SEED;
        initAmounts[1] = usdcFirst ? TOKEN_SEED : USDC_SEED;

        usdc.mint(address(this), USDC_SEED);
        token.mint(address(this), TOKEN_SEED);
        _permit2Approve(address(usdc), USDC_SEED);
        _permit2Approve(address(token), TOKEN_SEED);

        IBalancerV3RouterInit(ROUTER).initialize(newPool, initTokens, initAmounts, 0, false, "");
    }

    /// @dev The two-step Permit2 grant the V3 Router requires to pull `amount` of
    ///      `erc20` from this contract: ERC20-approve Permit2, then set the
    ///      Permit2 allowance for the Router.
    function _permit2Approve(address erc20, uint256 amount) internal {
        IERC20(erc20).approve(PERMIT2, type(uint256).max);
        IPermit2(PERMIT2).approve(erc20, ROUTER, uint160(amount), uint48(block.timestamp + 1 days));
    }

    /// @dev Advance the TWAP accumulator past `twapMinWindow` so `executeBuyback`
    ///      clears the fail-closed `TwapNotReady` guard.
    function _matureTwap() internal {
        bb.poke();
        skip(31 minutes);
        bb.poke();
    }

    // -----------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------

    /// @notice Sanity: the in-test pool stood up correctly — the burner's
    ///         constructor `_validatePoolWiring` passed against the live Vault and
    ///         the freshly-created pool, and the TWAP reads a real non-zero spot.
    function test_pool_created_and_twap_reads_real_pool() public requiresFork {
        assertTrue(IBalancerV3Vault(VAULT).isPoolRegistered(pool), "pool registered with live vault");
        _matureTwap();
        // Spot is constant across pokes, so the TWAP equals the seeded marginal
        // spot exactly. Asserting the known value (not just > 0) validates
        // `_spotPrice`'s mulDiv + the usdcTo18 scaling against the LIVE Vault's
        // scaled-18 balance conventions — the one thing the hand-fed mock cannot.
        assertApproxEqRel(bb.twapPrice(), EXPECTED_SPOT, 0.001e18, "twap == seeded marginal spot");
    }

    /// @notice The headline gap: a real USDC->TOKEN swap through the live Router
    ///         against real 80/20 weighted-pool math, followed by a real burn.
    ///         Asserts USDC left the burner, TOKEN totalSupply dropped by exactly
    ///         the swapped amount (the burn), and the burner retains no TOKEN
    ///         (CEI: swap then burn).
    function test_executeBuyback_swaps_and_burns_real_pool() public requiresFork {
        _matureTwap();
        usdc.mint(address(bb), AMOUNT_IN);

        uint256 minOut = _twapFloor(AMOUNT_IN);
        uint256 supplyBefore = token.totalSupply();
        uint256 burnerUsdcBefore = usdc.balanceOf(address(bb));

        uint256 tokenOut = bb.executeBuyback(AMOUNT_IN, minOut);

        // Realized output is bounded ABOVE by the fee-free marginal and lands in a
        // tight band just under it — proving real weighted-pool math + fee/impact
        // realization, not just "some TOKEN came back". A scaling bug (e.g. a
        // usdcTo18 decimals error) would blow past this band.
        assertGe(tokenOut, minOut, "realized out cleared the floor");
        assertLt(tokenOut, MARGINAL_OUT, "realized below fee-free marginal");
        assertApproxEqRel(tokenOut, 9897e18, 0.01e18, "realized ~= marginal less 1% fee + impact");
        assertEq(usdc.balanceOf(address(bb)), burnerUsdcBefore - AMOUNT_IN, "USDC spent");
        assertEq(token.totalSupply(), supplyBefore - tokenOut, "TOKEN burned == swapped out");
        assertEq(token.balanceOf(address(bb)), 0, "no TOKEN stranded in burner");
        // The per-epoch cap snapshotted the LIVE Vault's USDC depth (scaled-18 ->
        // raw via usdcTo18), validating that decimals round-trip on real data.
        assertEq(bb.epochStartUsdcDepth(), USDC_SEED, "epoch depth snapshot == seeded USDC depth");
        // The scoped Permit2 ERC20 approval was reset to 0 after the swap.
        assertEq(usdc.allowance(address(bb), PERMIT2), 0, "scoped permit2 approval reset");
    }

    /// @notice `minOut` forwarding + real price-impact realization: a swap large
    ///         enough that real 80/20 price impact pushes realized output below
    ///         the contract's floor reverts at the LIVE Router's own min-out
    ///         check. The contract's pre-swap floor gate passes (minOut == floor),
    ///         so the revert proves our `minOut` argument actually reaches the
    ///         Router and that real impact — not a mock's preset output — is
    ///         realized. Sized under the per-epoch cap (10% of ~250k depth).
    function test_executeBuyback_reverts_when_live_impact_exceeds_floor() public requiresFork {
        _matureTwap();
        uint256 bigIn = 20_000e6; // ~8% of seeded USDC depth -> impact > fee+slippage band
        usdc.mint(address(bb), bigIn);
        uint256 floor = _twapFloor(bigIn); // marginal-derived; real exact-in output falls below it
        // The revert must come from the LIVE Router's min-out check after real
        // price impact, NOT from the contract's own floor gate (minOut == floor
        // passes it). A bare `vm.expectRevert()` accepts any revert and would let
        // a floor-gate regression masquerade as the intended Router rejection, so
        // capture the payload and assert it is not `MinOutBelowTwapFloor`.
        (bool ok, bytes memory err) = address(bb).call(abi.encodeCall(bb.executeBuyback, (bigIn, floor)));
        assertFalse(ok, "expected a revert from the live Router min-out check");
        assertGe(err.length, 4, "expected a typed/standard revert payload");
        assertTrue(
            bytes4(err) != BuybackBurnerBalancerV3.MinOutBelowTwapFloor.selector,
            "must revert at the Router, not the contract floor gate"
        );
    }

    /// @notice Floor gating against real curves: a keeper `minOut` just below the
    ///         contract's live-derived TWAP floor is rejected (MinOutBelowTwapFloor)
    ///         before any Router call — validating the floor is computed from real
    ///         pool reads, not a mock's preset output.
    function test_executeBuyback_reverts_below_twap_floor() public requiresFork {
        _matureTwap();
        usdc.mint(address(bb), AMOUNT_IN);
        uint256 floor = _twapFloor(AMOUNT_IN);
        vm.expectRevert(abi.encodeWithSelector(BuybackBurnerBalancerV3.MinOutBelowTwapFloor.selector, floor - 1, floor));
        bb.executeBuyback(AMOUNT_IN, floor - 1);
    }

    /// @dev Replicates `BuybackBurnerBalancerV3._twapFloor` using the same live
    ///      reads, so the keeper `minOut` is exactly the contract's accepted
    ///      floor: marginal expected-out, netted for the live swap fee, then
    ///      discounted by `slippageBps`.
    function _twapFloor(uint256 amountIn) internal view returns (uint256) {
        uint256 price = bb.twapPrice(); // TOKEN per USDC, 1e18
        uint256 amountIn18 = amountIn * bb.usdcTo18();
        uint256 expectedOut = (amountIn18 * price) / WAD;
        uint256 feeE18 = IBalancerV3Vault(VAULT).getStaticSwapFeePercentage(pool);
        uint256 afterFee = (expectedOut * (WAD - feeE18)) / WAD;
        return (afterFee * (BPS - SLIPPAGE_BPS)) / BPS;
    }
}
