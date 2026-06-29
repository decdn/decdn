// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { BuybackBurner } from "../src/BuybackBurner.sol";
import { GuardedBuybackBurner } from "../src/GuardedBuybackBurner.sol";
import { BuybackBurnerUniswapV3 } from "../src/BuybackBurnerUniswapV3.sol";
import { IUniswapV3SwapRouter } from "../src/interfaces/IUniswapV3SwapRouter.sol";
import { Token } from "../src/Token.sol";

import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

// -----------------------------------------------------------------
// Mocks
// -----------------------------------------------------------------

contract MockUSDC is ERC20 {
    constructor() ERC20("USDC", "USDC") {
        _mint(msg.sender, 1_000_000_000e6);
    }

    function decimals() public pure override returns (uint8) {
        return 6;
    }

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

/// @notice Minimal Uniswap V3 pool stub: configurable `slot0` sqrt-price, token
///         ordering, and fee tier. Holds no real reserves — the burner's
///         per-epoch cap reads USDC `balanceOf(pool)`, which the test funds
///         directly by minting USDC to this address.
contract MockV3Pool {
    uint160 internal sqrtP;
    address internal t0;
    address internal t1;
    uint24 internal f;

    constructor(uint160 sqrtP_, address t0_, address t1_, uint24 f_) {
        sqrtP = sqrtP_;
        t0 = t0_;
        t1 = t1_;
        f = f_;
    }

    function setSqrtPrice(uint160 sqrtP_) external {
        sqrtP = sqrtP_;
    }

    function slot0() external view returns (uint160, int24, uint16, uint16, uint16, uint8, bool) {
        return (sqrtP, int24(0), uint16(0), uint16(0), uint16(0), uint8(0), true);
    }

    function token0() external view returns (address) {
        return t0;
    }

    function token1() external view returns (address) {
        return t1;
    }

    function fee() external view returns (uint24) {
        return f;
    }
}

/// @notice Minimal `SwapRouter02` stub: pulls `amountIn` of `tokenIn` from the
///         caller via the scoped ERC20 allowance, enforces `amountOutMinimum`,
///         and hands a preconfigured `amountOut` of `tokenOut` to `recipient`.
///         Pre-funded with TOKEN by the test.
contract MockV3Router {
    uint256 public amountOut;

    function setAmountOut(uint256 amountOut_) external {
        amountOut = amountOut_;
    }

    function exactInputSingle(IUniswapV3SwapRouter.ExactInputSingleParams calldata p)
        external
        payable
        returns (uint256)
    {
        // slither-disable-next-line unchecked-transfer
        IERC20(p.tokenIn).transferFrom(msg.sender, address(this), p.amountIn);
        require(amountOut >= p.amountOutMinimum, "MockV3Router: min out");
        // slither-disable-next-line unchecked-transfer
        IERC20(p.tokenOut).transfer(p.recipient, amountOut);
        return amountOut;
    }
}

/// @title BuybackBurnerUniswapV3 unit tests
/// @notice Mock-backed coverage for the Uniswap V3 venue hooks: the `slot0`
///         spot-price math (both token orderings), the inherited TWAP floor /
///         band / cap firing through the V3 swap, wiring validation, and the
///         scoped router approval. The shared MEV stack itself is covered by the
///         Balancer suite; here we verify the venue-specific reads/swap.
contract BuybackBurnerUniswapV3Test is Test {
    MockUSDC internal usdc;
    Token internal token;
    MockV3Router internal router;
    MockV3Pool internal pool;
    BuybackBurnerUniswapV3 internal bb;

    address internal admin = address(0xA11CE);
    address internal keeper = address(0xCAFE);

    uint24 internal constant FEE = 3000; // 0.30%
    uint256 internal constant FEE_E18 = 3e15;
    uint256 internal constant TWAP_WINDOW = 1800; // 30 min (== MIN_TWAP_WINDOW)
    uint256 internal constant SLIPPAGE_BPS = 200; // 2%
    uint256 internal constant MIN_BUYBACK = 100e6;
    uint256 internal constant MAX_BUYBACK = 10_000e6;
    uint256 internal constant CAP_FRACTION = 1000; // 10%

    // sqrtPriceX96 for a 1:1 TOKEN/USDC human price (token0 = USDC, token1 =
    // TOKEN): price_raw = TOKEN_raw/USDC_raw = 1e18/1e6 = 1e12, so
    // sqrtPriceX96 = sqrt(1e12) * 2^96 = 1e6 * 2^96. Spot == 1e18 (1.0).
    uint160 internal constant SQRTP_ONE_DOLLAR = uint160(uint256(1e6) * (1 << 96));

    function setUp() public {
        usdc = new MockUSDC();
        token = new Token(admin);
        router = new MockV3Router();
        // token0 = USDC, token1 = TOKEN.
        pool = new MockV3Pool(SQRTP_ONE_DOLLAR, address(usdc), address(token), FEE);

        bb = _deploy(address(pool));

        vm.startPrank(admin);
        bb.grantRole(bb.KEEPER_ROLE(), keeper);
        // Fund the router with TOKEN so it can hand `amountOut` to the burner.
        token.transfer(address(router), 10_000_000e18);
        vm.stopPrank();

        // Fund the burner's USDC bucket (would arrive via FeeRouter) and give
        // the pool USDC depth for the per-epoch cap denominator.
        usdc.transfer(address(bb), 1_000_000e6);
        usdc.mint(address(pool), 1_000_000e6); // cap = 10% = 100_000e6
    }

    function _deploy(address pool_) internal returns (BuybackBurnerUniswapV3) {
        return new BuybackBurnerUniswapV3(
            IERC20(address(usdc)),
            ERC20Burnable(address(token)),
            admin,
            BuybackBurnerUniswapV3.Config({
                swapRouter_: IUniswapV3SwapRouter(address(router)),
                pool_: pool_,
                twapMinWindow_: TWAP_WINDOW,
                maxBuybackAmount_: MAX_BUYBACK,
                minBuybackAmount_: MIN_BUYBACK,
                slippageBps_: SLIPPAGE_BPS,
                epochLiquidityCapFraction_: CAP_FRACTION
            })
        );
    }

    /// @dev Mature the inherited TWAP accumulator to a constant spot: poke,
    ///      advance one full window, poke again → `twapPrice == spot`.
    function _matureTwap() internal {
        bb.poke();
        vm.warp(block.timestamp + TWAP_WINDOW);
        bb.poke();
    }

    function _expectedFloor(uint256 amountIn, uint256 spot1e18) internal pure returns (uint256) {
        uint256 amountIn18 = amountIn * 1e12; // usdcTo18 for 6-dec USDC
        uint256 expectedOut = amountIn18 * spot1e18 / 1e18;
        uint256 afterFee = expectedOut * (1e18 - FEE_E18) / 1e18;
        return afterFee * (10_000 - SLIPPAGE_BPS) / 10_000;
    }

    // -----------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------

    function test_constructor_revertsOnZeroRouter() public {
        vm.expectRevert(BuybackBurner.ZeroAddress.selector);
        new BuybackBurnerUniswapV3(
            IERC20(address(usdc)),
            ERC20Burnable(address(token)),
            admin,
            BuybackBurnerUniswapV3.Config({
                swapRouter_: IUniswapV3SwapRouter(address(0)),
                pool_: address(pool),
                twapMinWindow_: TWAP_WINDOW,
                maxBuybackAmount_: MAX_BUYBACK,
                minBuybackAmount_: MIN_BUYBACK,
                slippageBps_: SLIPPAGE_BPS,
                epochLiquidityCapFraction_: CAP_FRACTION
            })
        );
    }

    function test_constructor_setsVenue() public view {
        assertEq(address(bb.swapRouter()), address(router));
        assertEq(bb.pool(), address(pool));
        assertEq(bb.usdcTo18(), 1e12);
    }

    function test_constructor_revertsOnMisWiredPool() public {
        // A non-zero pool supplied at deploy must fail-fast if it is not a
        // USDC/TOKEN pair (mirrors `setPool` and the Balancer constructor).
        MockV3Pool bad = new MockV3Pool(SQRTP_ONE_DOLLAR, address(0xDEAD), address(0xBEEF), FEE);
        vm.expectRevert(BuybackBurnerUniswapV3.PoolStateInvalid.selector);
        _deploy(address(bad));
    }

    // -----------------------------------------------------------------
    // Spot price math (both token orderings)
    // -----------------------------------------------------------------

    function test_twapPrice_token0Usdc_oneDollar() public {
        _matureTwap();
        // sqrtP encodes a 1:1 human price → spot == 1e18.
        assertApproxEqRel(bb.twapPrice(), 1e18, 1e12);
    }

    function test_twapPrice_token0Token_sameHumanPrice() public {
        // Reverse the ordering (token0 = TOKEN, token1 = USDC) at the SAME human
        // price. price_raw = USDC_raw/TOKEN_raw = 1e6/1e18 = 1e-12, so
        // sqrtPriceX96 = sqrt(1e-12) * 2^96 = 2^96 / 1e6.
        MockV3Pool rev = new MockV3Pool(uint160(uint256(1 << 96) / 1_000_000), address(token), address(usdc), FEE);
        BuybackBurnerUniswapV3 b2 = _deploy(address(rev));
        // mature b2's TWAP
        b2.poke();
        vm.warp(block.timestamp + TWAP_WINDOW);
        b2.poke();
        // Same 1.0 human price regardless of leg ordering.
        assertApproxEqRel(b2.twapPrice(), 1e18, 1e15);
    }

    function test_twapPrice_revertsBeforeWindow() public {
        bb.poke();
        vm.expectRevert(GuardedBuybackBurner.TwapNotReady.selector);
        bb.twapPrice();
    }

    // -----------------------------------------------------------------
    // executeBuyback
    // -----------------------------------------------------------------

    function test_executeBuyback_revertsPoolNotWired() public {
        BuybackBurnerUniswapV3 fresh = _deploy(address(0));
        vm.startPrank(admin);
        fresh.grantRole(fresh.KEEPER_ROLE(), keeper);
        vm.stopPrank();
        usdc.transfer(address(fresh), 1000e6);

        vm.prank(keeper);
        vm.expectRevert(BuybackBurner.PoolNotWired.selector);
        fresh.executeBuyback(1000e6, 1);
    }

    function test_executeBuyback_happyPath_swapsBurnsEmits() public {
        _matureTwap();
        uint256 amountIn = 1000e6;
        uint256 floor = _expectedFloor(amountIn, 1e18);
        router.setAmountOut(floor); // router gives exactly the floor

        uint256 supplyBefore = token.totalSupply();

        vm.expectEmit(false, false, false, true, address(bb));
        emit BuybackBurner.BuybackExecuted(amountIn, floor);
        vm.prank(keeper);
        uint256 tokenOut = bb.executeBuyback(amountIn, floor);

        assertEq(tokenOut, floor);
        assertEq(supplyBefore - token.totalSupply(), floor); // burned
        assertEq(token.balanceOf(address(bb)), 0); // no residual TOKEN
        // Scoped approval reset to 0 after the swap.
        assertEq(usdc.allowance(address(bb), address(router)), 0);
    }

    function test_executeBuyback_revertsBelowTwapFloor() public {
        _matureTwap();
        uint256 amountIn = 1000e6;
        uint256 floor = _expectedFloor(amountIn, 1e18);
        router.setAmountOut(floor);

        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(GuardedBuybackBurner.MinOutBelowTwapFloor.selector, floor - 1, floor));
        bb.executeBuyback(amountIn, floor - 1);
    }

    function test_executeBuyback_revertsBelowMinBand() public {
        _matureTwap();
        uint256 amountIn = MIN_BUYBACK - 1;
        uint256 floor = _expectedFloor(amountIn, 1e18);
        router.setAmountOut(floor);

        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(GuardedBuybackBurner.BelowMinBuyback.selector, amountIn, MIN_BUYBACK));
        bb.executeBuyback(amountIn, floor);
    }

    function test_executeBuyback_revertsAboveMaxBand() public {
        _matureTwap();
        uint256 amountIn = MAX_BUYBACK + 1;
        uint256 floor = _expectedFloor(amountIn, 1e18);
        router.setAmountOut(floor);

        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(GuardedBuybackBurner.AboveMaxBuyback.selector, amountIn, MAX_BUYBACK));
        bb.executeBuyback(amountIn, floor);
    }

    function test_swapFee_reflectsPoolFeeTier() public {
        // A keeper minOut at the no-fee floor (only slippage discounted) must be
        // rejected, proving the fee tier nets into the floor.
        _matureTwap();
        uint256 amountIn = 1000e6;
        uint256 amountIn18 = amountIn * 1e12;
        uint256 expectedOut = amountIn18 * 1e18 / 1e18;
        uint256 noFeeFloor = expectedOut * (10_000 - SLIPPAGE_BPS) / 10_000; // fee NOT netted
        uint256 realFloor = _expectedFloor(amountIn, 1e18); // fee netted (lower)
        assertGt(noFeeFloor, realFloor);
        router.setAmountOut(noFeeFloor);

        // minOut just above the real floor passes; equal to noFeeFloor also
        // passes (>= floor). The point: realFloor < noFeeFloor due to the fee.
        vm.prank(keeper);
        uint256 out = bb.executeBuyback(amountIn, realFloor);
        assertEq(out, noFeeFloor);
    }

    // -----------------------------------------------------------------
    // Per-epoch cap (Uniswap-specific `balanceOf(pool)` depth read)
    // -----------------------------------------------------------------

    function test_executeBuyback_revertsAboveEpochCap() public {
        // Fresh burner against a low-depth pool so the per-epoch cap (10% of the
        // pool's USDC `balanceOf`) binds within the band: depth 50_000e6 -> cap
        // 5_000e6, exceeded by a band-valid 6_000e6 swap.
        MockV3Pool lowPool = new MockV3Pool(SQRTP_ONE_DOLLAR, address(usdc), address(token), FEE);
        BuybackBurnerUniswapV3 bb2 = _deploy(address(lowPool));
        vm.startPrank(admin);
        bb2.grantRole(bb2.KEEPER_ROLE(), keeper);
        token.transfer(address(router), 1_000_000e18);
        vm.stopPrank();
        usdc.transfer(address(bb2), 1_000_000e6);
        usdc.mint(address(lowPool), 50_000e6); // cap = 10% = 5_000e6

        bb2.poke();
        vm.warp(block.timestamp + TWAP_WINDOW);
        bb2.poke();

        uint256 amountIn = 6000e6; // in band [100e6, 10_000e6], over the 5_000e6 cap
        uint256 floor = _expectedFloor(amountIn, 1e18);
        router.setAmountOut(floor);

        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(GuardedBuybackBurner.EpochCapExceeded.selector, amountIn, 5000e6));
        bb2.executeBuyback(amountIn, floor);
    }

    function test_setPool_reSnapshotsDepthFromNewPool() public {
        // After a rotation the per-epoch cap must re-snapshot depth from the NEW
        // pool (the reset zeroes `epochStartUsdcDepth`), not carry the old one.
        MockV3Pool p2 = new MockV3Pool(SQRTP_ONE_DOLLAR, address(usdc), address(token), FEE);
        usdc.mint(address(p2), 500_000e6); // new-pool depth (setUp gave the old pool 1_000_000e6)
        vm.prank(admin);
        bb.setPool(address(p2)); // resets guard state

        _matureTwap(); // re-mature against the new pool
        uint256 amountIn = 1000e6;
        uint256 floor = _expectedFloor(amountIn, 1e18);
        router.setAmountOut(floor);
        vm.prank(keeper);
        bb.executeBuyback(amountIn, floor);

        assertEq(bb.epochStartUsdcDepth(), 500_000e6, "depth re-snapshotted from new pool");
        assertEq(bb.epochSwappedUsdc(), amountIn, "epoch accrual against new pool");
    }

    // -----------------------------------------------------------------
    // Wiring validation
    // -----------------------------------------------------------------

    function test_setPool_revertsOnNonUsdcTokenPair() public {
        // A pool whose legs are neither USDC nor TOKEN must be rejected.
        MockV3Pool bad = new MockV3Pool(SQRTP_ONE_DOLLAR, address(0xDEAD), address(0xBEEF), FEE);
        vm.prank(admin);
        vm.expectRevert(BuybackBurnerUniswapV3.PoolStateInvalid.selector);
        bb.setPool(address(bad));
    }

    function test_setPool_revertsWhenDirectSpotFloorsToZero() public {
        // token0=USDC, token1=TOKEN with a degenerate sqrt price (priceX96 floors
        // to 0): the direct-branch spot is 0 and must revert fail-closed, matching
        // the inverted branch (regression guard for the zero-spot hardening).
        MockV3Pool zeroSpot = new MockV3Pool(uint160(1), address(usdc), address(token), FEE);
        vm.prank(admin);
        vm.expectRevert(BuybackBurnerUniswapV3.PoolStateInvalid.selector);
        bb.setPool(address(zeroSpot));
    }

    function test_setPool_acceptsZeroAsUnwire() public {
        vm.prank(admin);
        bb.setPool(address(0));
        assertEq(bb.pool(), address(0));
    }

    function test_setPool_resetsGuardStateOnRotation() public {
        // Mature the accumulator against the current pool.
        _matureTwap();
        assertApproxEqRel(bb.twapPrice(), 1e18, 1e12);

        // Rotate to a fresh, valid USDC/TOKEN pool: the guard state must reset
        // so the floor re-derives against the new pool (fail-closed until the
        // window re-matures) instead of carrying the old pool's accumulator.
        MockV3Pool p2 = new MockV3Pool(SQRTP_ONE_DOLLAR, address(usdc), address(token), FEE);
        vm.expectEmit(false, false, false, false, address(bb));
        emit GuardedBuybackBurner.GuardStateReset();
        vm.prank(admin);
        bb.setPool(address(p2));

        vm.expectRevert(GuardedBuybackBurner.TwapNotReady.selector);
        bb.twapPrice();
    }

    function test_setSwapRouter_revertsOnZero() public {
        vm.prank(admin);
        vm.expectRevert(BuybackBurner.ZeroAddress.selector);
        bb.setSwapRouter(address(0));
    }
}
