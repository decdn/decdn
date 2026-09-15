// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { BuybackBurner } from "../src/BuybackBurner.sol";
import { BuybackBurnerBalancerV3 } from "../src/BuybackBurnerBalancerV3.sol";
import { GuardedBuybackBurner } from "../src/GuardedBuybackBurner.sol";
import { Token } from "../src/Token.sol";
import { IBalancerV3Router } from "../src/interfaces/IBalancerV3Router.sol";

import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { ERC20Burnable } from "@openzeppelin/contracts/token/ERC20/extensions/ERC20Burnable.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";

contract MockUSDC is ERC20 {
    constructor() ERC20("USDC", "USDC") {
        _mint(msg.sender, 1_000_000_000e6);
    }

    function decimals() public pure override returns (uint8) {
        return 6;
    }
}

contract MockHighDecimalsToken is ERC20 {
    constructor() ERC20("X", "X") { }

    function decimals() public pure override returns (uint8) {
        return 19;
    }
}

/// @notice Minimal Balancer V3 Vault mock. Holds the pool's token set + their
///         scaled-18 live balances and answers the pool registration / state
///         reads (`isPoolRegistered`, `getPoolTokens`, `getCurrentLiveBalances`,
///         `getStaticSwapFeePercentage`). It is NOT an approval target — under V3
///         the Router pulls `tokenIn` via Permit2 (see `MockPermit2`).
contract MockBalancerV3Vault {
    IERC20[] internal tokens;
    uint256[] internal balances18;
    uint256 public swapFeePercentage = 1e16; // 1%, 1e18-scaled
    bool public registered = true; // global registration toggle for isPoolRegistered
    // When non-zero, only this pool address is reported registered — lets a test
    // assert the contract passes the correct pool argument to isPoolRegistered.
    // address(0) (default) keeps the address-agnostic behavior gated by `registered`.
    address public registeredPool;

    function setPool(IERC20[] calldata tokens_, uint256[] calldata balances18_) external {
        tokens = tokens_;
        balances18 = balances18_;
    }

    /// @dev Toggle the registration state returned by `isPoolRegistered`.
    function setRegistered(bool registered_) external {
        registered = registered_;
    }

    /// @dev Pin the single pool address `isPoolRegistered` reports as registered.
    function setRegisteredPool(address pool_) external {
        registeredPool = pool_;
    }

    function isPoolRegistered(address pool) external view returns (bool) {
        if (!registered) return false;
        return registeredPool == address(0) || pool == registeredPool;
    }

    /// @dev Mutate one leg's scaled-18 balance to simulate a spot move.
    function setBalance(uint256 index, uint256 balance18) external {
        balances18[index] = balance18;
    }

    function setSwapFee(uint256 feeE18) external {
        swapFeePercentage = feeE18;
    }

    function getPoolTokens(address) external view returns (IERC20[] memory) {
        return tokens;
    }

    function getCurrentLiveBalances(address) external view returns (uint256[] memory) {
        return balances18;
    }

    function getStaticSwapFeePercentage(address) external view returns (uint256) {
        return swapFeePercentage;
    }
}

contract MockBalancerV3WeightedPool {
    uint256[] internal weights;

    constructor(uint256[] memory weights_) {
        weights = weights_;
    }

    function getNormalizedWeights() external view returns (uint256[] memory) {
        return weights;
    }
}

/// @notice Minimal Uniswap Permit2 (`AllowanceTransfer`) mock — the leg a real
///         Balancer V3 Router drives to pull `tokenIn`. Stores per-(owner,token,
///         spender) allowances set via `approve`, and `transferFrom` (called by
///         the Router) checks the spender's allowance + expiration, decrements
///         it, and moves tokens via the standard ERC20 allowance the owner
///         granted Permit2. Mirrors why the burner must ERC20-approve Permit2 AND
///         grant the Router a scoped Permit2 allowance — a direct Vault allowance
///         (the V2 model) is never consulted.
contract MockPermit2 {
    struct Allow {
        uint160 amount;
        uint48 expiration;
    }

    // owner => token => spender => allowance
    mapping(address => mapping(address => mapping(address => Allow))) internal allow;

    function approve(address token, address spender, uint160 amount, uint48 expiration) external {
        // Permit2 reads expiration 0 as "valid this block" (block.timestamp).
        allow[msg.sender][token][spender] =
            Allow({ amount: amount, expiration: expiration == 0 ? uint48(block.timestamp) : expiration });
    }

    /// @dev The scoped Permit2 allowance `owner` granted `spender` for `token`.
    function allowanceAmount(address owner, address token, address spender) external view returns (uint160) {
        return allow[owner][token][spender].amount;
    }

    function transferFrom(address from, address to, uint160 amount, address token) external {
        Allow storage a = allow[from][token][msg.sender];
        // forge-lint: disable-next-line(block-timestamp)
        require(block.timestamp <= a.expiration, "permit2: expired");
        require(a.amount >= amount, "permit2: insufficient");
        a.amount -= amount;
        // slither-disable-next-line arbitrary-send-erc20
        IERC20(token).transferFrom(from, to, amount);
    }
}

/// @notice Minimal Balancer V3 Router mock. Records the scoped Permit2 allowance
///         the caller granted this Router in `observedAllowance` (the test
///         asserts it equals `exactAmountIn`), pulls `tokenIn` via Permit2 — the
///         real V3 token-pull path — into the Vault, enforces `minAmountOut`, and
///         pays `amountOut` TOKEN out of its own (pre-funded) balance.
contract MockBalancerV3Router {
    MockBalancerV3Vault internal vault;
    MockPermit2 internal permit2;

    uint256 public amountOut;
    uint256 public observedAllowance;
    address public lastPool;
    uint256 public lastDeadline;
    bool public lastWethIsEth;

    error RouterMinOut(uint256 amountOut, uint256 minAmountOut);

    constructor(MockBalancerV3Vault vault_, MockPermit2 permit2_) {
        vault = vault_;
        permit2 = permit2_;
    }

    function setAmountOut(uint256 amountOut_) external {
        amountOut = amountOut_;
    }

    function swapSingleTokenExactIn(
        address pool,
        IERC20 tokenIn,
        IERC20 tokenOut_,
        uint256 exactAmountIn,
        uint256 minAmountOut,
        uint256 deadline,
        bool wethIsEth,
        bytes calldata
    ) external returns (uint256) {
        observedAllowance = permit2.allowanceAmount(msg.sender, address(tokenIn), address(this));
        lastPool = pool;
        lastDeadline = deadline;
        lastWethIsEth = wethIsEth;

        permit2.transferFrom(msg.sender, address(vault), uint160(exactAmountIn), address(tokenIn));
        if (amountOut < minAmountOut) revert RouterMinOut(amountOut, minAmountOut);
        // slither-disable-next-line unchecked-transfer
        tokenOut_.transfer(msg.sender, amountOut);
        return amountOut;
    }
}

/// @title BuybackBurnerBalancerV3 tests
/// @notice Covers the live single-swap path, the TWAP `minOut` floor, the
///         per-epoch USDC liquidity cap, the min/max buyback band, the scoped
///         Permit2 approval lifecycle, and governance-setter access control.
contract BuybackBurnerBalancerV3Test is Test {
    MockUSDC internal usdc;
    Token internal token;
    MockBalancerV3Vault internal vault;
    MockPermit2 internal permit2;
    MockBalancerV3WeightedPool internal pool;
    MockBalancerV3Router internal router;
    BuybackBurnerBalancerV3 internal bb;

    address internal admin = address(0xA11CE);
    address internal keeper = address(0xCAFE);
    address internal pauser = address(0xBAD);
    address internal treasury = address(0x7EA);
    address internal gov = address(0x60F);

    // 80/20 TOKEN/USDC weighted pool seeded with 1,000,000 USDC + 100,000,000
    // TOKEN. Marginal TOKEN-per-USDC spot = (wUsdc*balToken)/(balUsdc*wToken)
    // = (0.2*1e26)/(1e24*0.8) scaled = 25e18 (25 TOKEN per USDC).
    uint256 internal constant W_TOKEN = 0.8e18;
    uint256 internal constant W_USDC = 0.2e18;
    uint256 internal constant POOL_USDC_18 = 1e24; // 1,000,000 USDC scaled-18
    uint256 internal constant POOL_TOKEN_18 = 1e26; // 100,000,000 TOKEN
    uint256 internal constant SPOT = 25e18; // TOKEN per USDC, 1e18 fixed point

    uint256 internal constant TWAP_WINDOW = 3600;
    uint256 internal constant SLIPPAGE_BPS = 200;
    /// Mirrors `GuardedBuybackBurner.SLIPPAGE_CEILING`, which is `internal`.
    uint256 internal constant SLIPPAGE_CEILING = 1000; // 10%
    uint256 internal constant MIN_BUYBACK = 100e6;
    uint256 internal constant MAX_BUYBACK = 500_000e6;
    uint256 internal constant CAP_FRACTION = 1000; // 10%

    uint256 internal constant BUYBACK_USDC = 1000e6;
    // expectedOut = 1000e6 * 1e12 * 25e18 / 1e18 = 25_000e18.
    // floor = expectedOut * (1 - 1% fee) * (1 - 2% slippage)
    //       = 25_000e18 * 0.99 * 0.98 = 24_255e18.
    uint256 internal constant EXPECTED_OUT = 25_000e18;
    uint256 internal constant FLOOR_OUT = 24_255e18;

    function setUp() public {
        usdc = new MockUSDC();
        token = new Token(admin);

        vault = new MockBalancerV3Vault();
        uint256[] memory weights = new uint256[](2);
        weights[0] = W_TOKEN;
        weights[1] = W_USDC;
        pool = new MockBalancerV3WeightedPool(weights);

        IERC20[] memory tokens = new IERC20[](2);
        tokens[0] = IERC20(address(token));
        tokens[1] = IERC20(address(usdc));
        uint256[] memory bals = new uint256[](2);
        bals[0] = POOL_TOKEN_18;
        bals[1] = POOL_USDC_18;
        vault.setPool(tokens, bals);

        permit2 = new MockPermit2();
        router = new MockBalancerV3Router(vault, permit2);

        bb = _deploy(CAP_FRACTION, MAX_BUYBACK);
        router.setAmountOut(EXPECTED_OUT);

        // Fund the Router with TOKEN to pay out, and the BuybackBurner with
        // USDC (would arrive via FeeRouter.routeSettlement).
        vm.prank(admin);
        token.transfer(address(router), 500_000_000e18);
        usdc.transfer(address(bb), 10_000_000e6);

        _warmTwap(bb);
    }

    function _deploy(uint256 capFraction, uint256 maxBuyback) internal returns (BuybackBurnerBalancerV3 newBb) {
        BuybackBurnerBalancerV3.Config memory cfg = BuybackBurnerBalancerV3.Config({
            swapRouter_: IBalancerV3Router(address(router)),
            pool_: address(pool),
            vault_: address(vault),
            permit2_: address(permit2),
            twapMinWindow_: TWAP_WINDOW,
            maxBuybackAmount_: maxBuyback,
            minBuybackAmount_: MIN_BUYBACK,
            slippageBps_: SLIPPAGE_BPS,
            epochLiquidityCapFraction_: capFraction
        });
        newBb = new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);

        vm.startPrank(admin);
        newBb.grantRole(newBb.KEEPER_ROLE(), keeper);
        newBb.grantRole(newBb.PAUSER_ROLE(), pauser);
        newBb.grantRole(newBb.GOVERNANCE_ROLE(), gov);
        vm.stopPrank();
    }

    /// @dev Establish a ready TWAP: an init poke, then a poke two windows later
    ///      so the sliding window matures and `_twapPrice` is trusted.
    function _warmTwap(BuybackBurnerBalancerV3 target) internal {
        target.poke();
        vm.warp(block.timestamp + 2 * TWAP_WINDOW);
        target.poke();
    }

    // -----------------------------------------------------------------
    // Happy path
    // -----------------------------------------------------------------

    function test_executeBuyback_happyPath_swapsBurnsEmits() public {
        uint256 supplyBefore = token.totalSupply();

        vm.expectEmit(false, false, false, true, address(bb));
        emit BuybackBurner.BuybackExecuted(BUYBACK_USDC, EXPECTED_OUT);
        vm.prank(keeper);
        uint256 out = bb.executeBuyback(BUYBACK_USDC, FLOOR_OUT);

        assertEq(out, EXPECTED_OUT, "returned tokenOut");
        assertEq(supplyBefore - token.totalSupply(), EXPECTED_OUT, "burned amount");
        assertEq(router.lastPool(), address(pool), "router pool arg");
        assertEq(router.lastWethIsEth(), false, "wethIsEth arg");
        assertEq(bb.epochSwappedUsdc(), BUYBACK_USDC, "epoch accrual");
    }

    function test_twapPrice_reflectsStableSpot() public view {
        // Clean power-of-ten pool state: the time-weighted average over a flat
        // spot is exactly SPOT, no rounding.
        assertEq(bb.twapPrice(), SPOT, "twap == spot exactly");
    }

    function test_twapFloor_accountsForSwapFee() public {
        // Bump the pool fee to 5%; the floor must drop accordingly:
        // 25_000e18 * 0.95 * 0.98 = 23_275e18. A keeper minOut between the old
        // 1%-fee floor (24_255e18) and the new floor now passes.
        vault.setSwapFee(5e16);
        uint256 newFloor = 23_275e18;
        vm.prank(keeper);
        bb.executeBuyback(BUYBACK_USDC, newFloor);
        // And one wei below the new floor reverts with the recomputed floor.
        vm.prank(keeper);
        vm.expectRevert(
            abi.encodeWithSelector(GuardedBuybackBurner.MinOutBelowTwapFloor.selector, newFloor - 1, newFloor)
        );
        bb.executeBuyback(BUYBACK_USDC, newFloor - 1);
    }

    // -----------------------------------------------------------------
    // TWAP floor
    // -----------------------------------------------------------------

    function test_executeBuyback_minOutAtFloor_passes() public {
        vm.prank(keeper);
        bb.executeBuyback(BUYBACK_USDC, FLOOR_OUT);
        assertEq(bb.epochSwappedUsdc(), BUYBACK_USDC);
    }

    function test_executeBuyback_revertsWhenMinOutBelowTwapFloor() public {
        vm.prank(keeper);
        vm.expectRevert(
            abi.encodeWithSelector(GuardedBuybackBurner.MinOutBelowTwapFloor.selector, FLOOR_OUT - 1, FLOOR_OUT)
        );
        bb.executeBuyback(BUYBACK_USDC, FLOOR_OUT - 1);
    }

    /// @notice The Router's own min-out check is the live MEV defense: a keeper
    ///         `minOut` that clears the contract's floor gate (`== FLOOR_OUT`) but
    ///         exceeds what the swap realizes reverts at the Router (`RouterMinOut`,
    ///         NOT the pre-swap floor), and the per-epoch accrual rolls back (CEI)
    ///         rather than counting a swap that never landed. Offline counterpart
    ///         to the gated `*.swapburn.fork` live-impact test.
    function test_executeBuyback_routerMinOutRevert_rollsBackEpochAccrual() public {
        // Accrue a real buyback first so the post-revert assertion proves a
        // rollback, not an epoch counter that was simply never touched.
        vm.prank(keeper);
        bb.executeBuyback(BUYBACK_USDC, FLOOR_OUT);
        assertEq(bb.epochSwappedUsdc(), BUYBACK_USDC, "first buyback accrued");

        // Router now realizes one wei below the forwarded keeper `minOut`.
        router.setAmountOut(FLOOR_OUT - 1);
        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(MockBalancerV3Router.RouterMinOut.selector, FLOOR_OUT - 1, FLOOR_OUT));
        bb.executeBuyback(BUYBACK_USDC, FLOOR_OUT);

        assertEq(bb.epochSwappedUsdc(), BUYBACK_USDC, "accrual unchanged after router revert (CEI rollback)");
    }

    function test_executeBuyback_revertsTwapNotReady() public {
        BuybackBurnerBalancerV3 fresh = _deploy(CAP_FRACTION, MAX_BUYBACK);
        usdc.transfer(address(fresh), 10_000_000e6);
        fresh.poke(); // initialize, but do not span the window
        vm.prank(keeper);
        vm.expectRevert(GuardedBuybackBurner.TwapNotReady.selector);
        fresh.executeBuyback(BUYBACK_USDC, FLOOR_OUT);
    }

    function test_executeBuyback_revertsOnDegenerateSwapFee() public {
        // A 100% pool fee would collapse the floor to 0 (fail-open); reject it.
        vault.setSwapFee(1e18);
        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(GuardedBuybackBurner.SwapFeeInvalid.selector, uint256(1e18)));
        bb.executeBuyback(BUYBACK_USDC, FLOOR_OUT);
    }

    function test_twapFloor_resistsSingleBlockSpotSpike() public {
        uint256 before = bb.twapPrice();
        // Halve the TOKEN leg in one block -> spot doubles instantaneously.
        vault.setBalance(0, POOL_TOKEN_18 / 2);
        bb.poke(); // samples the spiked spot but with zero elapsed weight
        uint256 afterSpike = bb.twapPrice();
        assertApproxEqAbs(afterSpike, before, 1e15, "twap barely moves on a one-block spike");
    }

    // -----------------------------------------------------------------
    // Min / max buyback band
    // -----------------------------------------------------------------

    function test_executeBuyback_revertsBelowMinBuyback() public {
        // minOut large enough to clear the TWAP floor so the band check (which
        // runs after the floor) is what reverts.
        vm.prank(keeper);
        vm.expectRevert(
            abi.encodeWithSelector(GuardedBuybackBurner.BelowMinBuyback.selector, MIN_BUYBACK - 1, MIN_BUYBACK)
        );
        bb.executeBuyback(MIN_BUYBACK - 1, 1e25);
    }

    function test_executeBuyback_revertsAboveMaxBuyback() public {
        uint256 over = MAX_BUYBACK + 1;
        // Floor for `over` USDC; keeper supplies a generous minOut so the band
        // check (not the floor) is what reverts.
        router.setAmountOut(type(uint256).max / 2);
        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(GuardedBuybackBurner.AboveMaxBuyback.selector, over, MAX_BUYBACK));
        bb.executeBuyback(over, type(uint256).max / 2);
    }

    // -----------------------------------------------------------------
    // Per-epoch liquidity cap
    // -----------------------------------------------------------------

    // 60,000 USDC buyback: floor = 60000e6 * 1e12 * 25e18 / 1e18 * 9800/10000.
    uint256 internal constant BIG_BUYBACK = 60_000e6;
    uint256 internal constant BIG_FLOOR = 1_470_000e18;
    uint256 internal constant BIG_OUT = 1_500_000e18;

    function test_epochCap_accruesAndBindsWithinEpoch() public {
        // cap = 10% of 1,000,000 USDC = 100,000 USDC.
        router.setAmountOut(BIG_OUT);
        vm.startPrank(keeper);
        bb.executeBuyback(BIG_BUYBACK, BIG_FLOOR); // 60k ok
        assertEq(bb.epochSwappedUsdc(), BIG_BUYBACK);
        uint256 cap = 100_000e6;
        vm.expectRevert(
            abi.encodeWithSelector(GuardedBuybackBurner.EpochCapExceeded.selector, BIG_BUYBACK + BIG_BUYBACK, cap)
        );
        bb.executeBuyback(BIG_BUYBACK, BIG_FLOOR); // 120k > 100k cap
        vm.stopPrank();
    }

    function test_epochCap_resetsAcrossEpochs() public {
        router.setAmountOut(BIG_OUT);
        vm.prank(keeper);
        bb.executeBuyback(BIG_BUYBACK, BIG_FLOOR);
        assertEq(bb.epochSwappedUsdc(), BIG_BUYBACK);

        vm.warp(block.timestamp + 7 days);
        vm.expectEmit(true, false, false, false, address(bb));
        emit GuardedBuybackBurner.EpochRolled(uint64(block.timestamp / 7 days), 0);
        vm.prank(keeper);
        bb.executeBuyback(BIG_BUYBACK, BIG_FLOOR);
        assertEq(bb.epochSwappedUsdc(), BIG_BUYBACK, "epoch accrual reset then re-accrued");
    }

    // -----------------------------------------------------------------
    // Scoped Permit2 approval
    // -----------------------------------------------------------------

    function test_scopedApproval_setToAmountInThenResetToZero() public {
        vm.prank(keeper);
        bb.executeBuyback(BUYBACK_USDC, FLOOR_OUT);
        // The Router observed a Permit2 allowance scoped to exactly amountIn...
        assertEq(router.observedAllowance(), BUYBACK_USDC, "permit2->router allowance scoped to amountIn at swap time");
        // ...and both legs of the grant are reset to 0 afterward (no standing allowance).
        assertEq(
            permit2.allowanceAmount(address(bb), address(usdc), address(router)),
            0,
            "permit2->router allowance reset after swap"
        );
        assertEq(usdc.allowance(address(bb), address(permit2)), 0, "erc20->permit2 allowance reset after swap");
        // The V2-style direct Vault allowance is never set.
        assertEq(usdc.allowance(address(bb), address(vault)), 0, "no direct vault allowance");
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function test_setSwapRouter_updatesAndGuards() public {
        vm.prank(gov);
        bb.setSwapRouter(address(0xABCD));
        assertEq(address(bb.swapRouter()), address(0xABCD));

        vm.prank(gov);
        vm.expectRevert(BuybackBurner.ZeroAddress.selector);
        bb.setSwapRouter(address(0));

        vm.expectRevert();
        bb.setSwapRouter(address(0x1234)); // no GOVERNANCE_ROLE
    }

    function test_setKeeper_rotatesRole() public {
        address newKeeper = address(0x5EE);
        vm.prank(gov);
        bb.setKeeper(newKeeper);
        assertTrue(bb.hasRole(bb.KEEPER_ROLE(), newKeeper));
        assertEq(bb.keeper(), newKeeper);

        vm.prank(gov);
        bb.setKeeper(keeper);
        assertFalse(bb.hasRole(bb.KEEPER_ROLE(), newKeeper), "old keeper role revoked");
    }

    function test_setSlippageTolerance_boundsAndGuards() public {
        vm.prank(gov);
        bb.setSlippageTolerance(500);
        assertEq(bb.slippageBps(), 500);

        // The ceiling is inclusive: exactly 10% is accepted, 1 bp above reverts.
        vm.prank(gov);
        bb.setSlippageTolerance(SLIPPAGE_CEILING);
        assertEq(bb.slippageBps(), SLIPPAGE_CEILING);

        vm.prank(gov);
        vm.expectRevert(
            abi.encodeWithSelector(
                GuardedBuybackBurner.SlippageOutOfBounds.selector, SLIPPAGE_CEILING + 1, SLIPPAGE_CEILING
            )
        );
        bb.setSlippageTolerance(SLIPPAGE_CEILING + 1);

        // A slippage tolerance short of 100% would scale the TWAP floor toward
        // 0.01% of fair value — a disabled guard — so the ceiling rejects it.
        vm.prank(gov);
        vm.expectRevert(
            abi.encodeWithSelector(GuardedBuybackBurner.SlippageOutOfBounds.selector, 9999, SLIPPAGE_CEILING)
        );
        bb.setSlippageTolerance(9999);
    }

    function test_setMaxBuybackAmount_revertsOnZero() public {
        // `0` is not an *inverted* band once the floor is also 0, so the
        // inverted-band check alone let governance walk the band to `0/0` in two
        // calls, stalling every non-zero buyback until a further governance call
        // raised the ceiling (#1532).
        vm.prank(gov);
        bb.setMinBuybackAmount(0);

        vm.prank(gov);
        vm.expectRevert(GuardedBuybackBurner.BuybackBandDead.selector);
        bb.setMaxBuybackAmount(0);
    }

    function test_setEpochLiquidityCapFraction_boundsAndGuards() public {
        vm.prank(gov);
        bb.setEpochLiquidityCapFraction(3000);
        assertEq(bb.epochLiquidityCapFraction(), 3000);

        vm.prank(gov);
        vm.expectRevert(abi.encodeWithSelector(GuardedBuybackBurner.CapFractionOutOfBounds.selector, 3001, 100, 3000));
        bb.setEpochLiquidityCapFraction(3001);
    }

    function test_setters_requireGovernanceRole() public {
        vm.expectRevert();
        bb.setMinBuybackAmount(1);
        vm.expectRevert();
        bb.setMaxBuybackAmount(1);
        vm.expectRevert();
        bb.setEpochLiquidityCapFraction(1000);
        vm.expectRevert();
        bb.setPool(address(pool));
        vm.expectRevert();
        bb.setVault(address(vault));
    }

    // -----------------------------------------------------------------
    // Pause + constructor + accumulated fees
    // -----------------------------------------------------------------

    function test_pause_blocksExecuteBuyback() public {
        vm.prank(pauser);
        bb.pause();
        vm.prank(keeper);
        vm.expectRevert(Pausable.EnforcedPause.selector);
        bb.executeBuyback(BUYBACK_USDC, FLOOR_OUT);
    }

    function test_constructor_revertsOnZeroRouter() public {
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.swapRouter_ = IBalancerV3Router(address(0));
        vm.expectRevert(BuybackBurner.ZeroAddress.selector);
        new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
    }

    function test_constructor_revertsOnZeroPermit2() public {
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.permit2_ = address(0);
        vm.expectRevert(BuybackBurner.ZeroAddress.selector);
        new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
    }

    function test_constructor_revertsOnCapFractionOutOfBounds() public {
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.epochLiquidityCapFraction_ = 3001;
        vm.expectRevert(abi.encodeWithSelector(GuardedBuybackBurner.CapFractionOutOfBounds.selector, 3001, 100, 3000));
        new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
    }

    function test_constructor_revertsOnTwapWindowTooShort() public {
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.twapMinWindow_ = 30 minutes - 1;
        vm.expectRevert(
            abi.encodeWithSelector(GuardedBuybackBurner.TwapWindowTooShort.selector, 30 minutes - 1, 30 minutes)
        );
        new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
    }

    function test_constructor_revertsOnSlippageAboveCeiling() public {
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.slippageBps_ = SLIPPAGE_CEILING + 1;
        vm.expectRevert(
            abi.encodeWithSelector(
                GuardedBuybackBurner.SlippageOutOfBounds.selector, SLIPPAGE_CEILING + 1, SLIPPAGE_CEILING
            )
        );
        new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
    }

    function test_constructor_acceptsSlippageAtCeiling() public {
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.slippageBps_ = SLIPPAGE_CEILING;
        BuybackBurnerBalancerV3 atCeiling =
            new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
        assertEq(atCeiling.slippageBps(), SLIPPAGE_CEILING, "ceiling is inclusive");
    }

    function test_constructor_revertsOnZeroMaxBuyback() public {
        // `min == max == 0` would let the burner construct cleanly and then revert
        // `AboveMaxBuyback` on every non-zero call forever, so the constructor rejects it.
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.minBuybackAmount_ = 0;
        cfg.maxBuybackAmount_ = 0;
        vm.expectRevert(GuardedBuybackBurner.BuybackBandDead.selector);
        new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
    }

    function test_constructor_revertsOnInvertedBand() public {
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.minBuybackAmount_ = MAX_BUYBACK + 1;
        vm.expectRevert(
            abi.encodeWithSelector(GuardedBuybackBurner.BuybackBandInverted.selector, MAX_BUYBACK + 1, MAX_BUYBACK)
        );
        new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
    }

    function test_setMaxBuybackAmount_revertsBelowMin() public {
        vm.prank(gov);
        vm.expectRevert(
            abi.encodeWithSelector(GuardedBuybackBurner.BuybackBandInverted.selector, MIN_BUYBACK, MIN_BUYBACK - 1)
        );
        bb.setMaxBuybackAmount(MIN_BUYBACK - 1);
    }

    function test_constructor_revertsOnUsdcDecimalsAbove18() public {
        MockHighDecimalsToken bad = new MockHighDecimalsToken();
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        vm.expectRevert(abi.encodeWithSelector(GuardedBuybackBurner.UnsupportedTokenDecimals.selector, uint8(19)));
        new BuybackBurnerBalancerV3(IERC20(address(bad)), ERC20Burnable(address(token)), admin, treasury, cfg);
    }

    function test_poke_revertsWhenPoolUnwired() public {
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.pool_ = address(0);
        cfg.vault_ = address(0);
        BuybackBurnerBalancerV3 bb2 =
            new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
        vm.expectRevert(BuybackBurner.PoolNotWired.selector);
        bb2.poke();
    }

    function test_getAccumulatedFees_reportsUsdcBalance() public view {
        assertEq(bb.getAccumulatedFees(), usdc.balanceOf(address(bb)));
    }

    // -----------------------------------------------------------------
    // Pool-state read robustness (address-matching, not index-based)
    // -----------------------------------------------------------------

    function test_poolState_reversedTokenOrdering() public {
        // USDC at index 0, TOKEN at index 1 (opposite of setUp). The spot must
        // still resolve to SPOT because legs are matched by address, not index.
        BuybackBurnerBalancerV3 bb2 =
            _deployAgainstPool(_orderedTokens(true), _orderedWeights(true), _orderedBals(true));
        _warmTwap(bb2);
        assertEq(bb2.twapPrice(), SPOT, "spot correct under reversed token ordering");
    }

    function test_poolState_ignoresDecoyLegInThreeTokenPool() public {
        // 3-token pool (TOKEN, decoy, USDC) — the decoy leg must be ignored.
        IERC20[] memory t = new IERC20[](3);
        t[0] = IERC20(address(token));
        t[1] = IERC20(address(0xBEEF));
        t[2] = IERC20(address(usdc));
        uint256[] memory w = new uint256[](3);
        w[0] = W_TOKEN;
        w[1] = 0.1e18;
        w[2] = W_USDC;
        uint256[] memory b = new uint256[](3);
        b[0] = POOL_TOKEN_18;
        b[1] = 12_345e18;
        b[2] = POOL_USDC_18;
        BuybackBurnerBalancerV3 bb2 = _deployAgainstPool(t, w, b);
        _warmTwap(bb2);
        assertEq(bb2.twapPrice(), SPOT, "decoy leg ignored in 3-token pool");
    }

    function test_poolState_revertsWhenTokenLegRemovedAfterWiring() public {
        // The pool wired in setUp is valid; mutate the Vault's token set to drop
        // the TOKEN leg (e.g. a pool re-composition after wiring). The swap-path
        // read remains the runtime backstop and reverts PoolStateInvalid.
        IERC20[] memory t = new IERC20[](2);
        t[0] = IERC20(address(usdc));
        t[1] = IERC20(address(0xDEAD));
        vault.setPool(t, _orderedBals(true));
        vm.expectRevert(BuybackBurnerBalancerV3.PoolStateInvalid.selector);
        bb.poke(); // poke -> _updateTwapAccumulator -> _spotPrice -> _poolState
    }

    // -----------------------------------------------------------------
    // Fail-fast pool-wiring validation (issue #968)
    // -----------------------------------------------------------------

    function test_constructor_revertsOnUnregisteredPool() public {
        MockBalancerV3Vault v2 = new MockBalancerV3Vault();
        v2.setPool(_orderedTokens(false), _orderedBals(false));
        v2.setRegistered(false);
        MockBalancerV3WeightedPool p2 = new MockBalancerV3WeightedPool(_orderedWeights(false));
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.vault_ = address(v2);
        cfg.pool_ = address(p2);
        vm.expectRevert(
            abi.encodeWithSelector(BuybackBurnerBalancerV3.PoolNotRegistered.selector, address(p2), address(v2))
        );
        new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
    }

    function test_constructor_revertsOnMissingTokenLeg() public {
        // Pool registered but holds USDC + a decoy, no TOKEN leg -> ctor rejects.
        IERC20[] memory t = new IERC20[](2);
        t[0] = IERC20(address(usdc));
        t[1] = IERC20(address(0xDEAD));
        MockBalancerV3Vault v2 = new MockBalancerV3Vault();
        v2.setPool(t, _orderedBals(true));
        MockBalancerV3WeightedPool p2 = new MockBalancerV3WeightedPool(_orderedWeights(true));
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.vault_ = address(v2);
        cfg.pool_ = address(p2);
        vm.expectRevert(BuybackBurnerBalancerV3.PoolStateInvalid.selector);
        new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
    }

    function test_constructor_skipsValidationWhenWiringDeferred() public {
        // pool_ and vault_ == 0: deploy succeeds, validation deferred to wiring.
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.pool_ = address(0);
        cfg.vault_ = address(0);
        BuybackBurnerBalancerV3 bb2 =
            new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
        assertEq(bb2.balancerPool(), address(0), "pool unwired");
        assertEq(bb2.balancerVault(), address(0), "vault unwired");
    }

    function test_setPool_revertsOnUnregisteredPool() public {
        // The configured Vault now reports the pool as unregistered.
        vault.setRegistered(false);
        vm.prank(gov);
        vm.expectRevert(
            abi.encodeWithSelector(BuybackBurnerBalancerV3.PoolNotRegistered.selector, address(pool), address(vault))
        );
        bb.setPool(address(pool));
    }

    function test_setPool_revertsWhenVaultRegistersOnlyADifferentPool() public {
        // Vault recognizes only the originally-wired pool; wiring a different
        // (otherwise valid) pool reverts — proves the contract passes the pool
        // address through to isPoolRegistered rather than ignoring it.
        vault.setRegisteredPool(address(pool));
        MockBalancerV3WeightedPool other = new MockBalancerV3WeightedPool(_orderedWeights(false));
        vm.prank(gov);
        vm.expectRevert(
            abi.encodeWithSelector(BuybackBurnerBalancerV3.PoolNotRegistered.selector, address(other), address(vault))
        );
        bb.setPool(address(other));
        // A rejected wiring must not partially take effect: the prior good pool
        // is preserved (the `super.setPool` write is rolled back by the revert).
        assertEq(bb.balancerPool(), address(pool), "rejected wiring rolled back");
    }

    function test_setPool_validatesAndWiresHappyPath() public {
        MockBalancerV3WeightedPool p2 = new MockBalancerV3WeightedPool(_orderedWeights(false));
        vm.prank(gov);
        vm.expectEmit(true, true, false, false, address(bb));
        emit BuybackBurnerBalancerV3.PoolUpdated(address(pool), address(p2));
        bb.setPool(address(p2));
        assertEq(bb.balancerPool(), address(p2), "pool re-wired after passing validation");
    }

    function test_setVault_revertsWhenPoolNotRegisteredWithNewVault() public {
        MockBalancerV3Vault v2 = new MockBalancerV3Vault();
        v2.setPool(_orderedTokens(false), _orderedBals(false));
        v2.setRegistered(false);
        vm.prank(gov);
        vm.expectRevert(
            abi.encodeWithSelector(BuybackBurnerBalancerV3.PoolNotRegistered.selector, address(pool), address(v2))
        );
        bb.setVault(address(v2));
    }

    function test_setVault_revertsWhenNewVaultReportsInvalidPoolState() public {
        // The documented danger case: a new Vault registers the pool (so the
        // isPoolRegistered gate passes) but reports a token set missing the TOKEN
        // leg, so `_poolState` reverts PoolStateInvalid on the rotation.
        IERC20[] memory t = new IERC20[](2);
        t[0] = IERC20(address(usdc));
        t[1] = IERC20(address(0xDEAD));
        MockBalancerV3Vault v2 = new MockBalancerV3Vault();
        v2.setPool(t, _orderedBals(true)); // registered == true by default
        vm.prank(gov);
        vm.expectRevert(BuybackBurnerBalancerV3.PoolStateInvalid.selector);
        bb.setVault(address(v2));
    }

    function test_setVault_validatesAndRotatesHappyPath() public {
        // Rotate to a second Vault that registers the current pool and reports
        // valid {USDC, TOKEN} legs: validation passes, vault updates, event fires.
        MockBalancerV3Vault v2 = new MockBalancerV3Vault();
        v2.setPool(_orderedTokens(false), _orderedBals(false));
        vm.prank(gov);
        vm.expectEmit(true, true, false, false, address(bb));
        emit BuybackBurnerBalancerV3.VaultUpdated(address(vault), address(v2));
        bb.setVault(address(v2));
        assertEq(bb.balancerVault(), address(v2), "vault rotated after passing validation");
    }

    function test_setPool_resetsGuardStateOnRotation() public {
        // TWAP is matured in setUp; rotating the pool must reset the inherited
        // guard state so the floor re-derives against the new pool instead of
        // carrying the old pool's accumulator.
        assertEq(bb.twapPrice(), SPOT, "twap matured pre-rotation");
        MockBalancerV3WeightedPool p2 = new MockBalancerV3WeightedPool(_orderedWeights(false));
        vm.expectEmit(false, false, false, false, address(bb));
        emit GuardedBuybackBurner.GuardStateReset();
        vm.prank(gov);
        bb.setPool(address(p2));
        vm.expectRevert(GuardedBuybackBurner.TwapNotReady.selector);
        bb.twapPrice();
    }

    function test_setVault_resetsGuardStateOnRotation() public {
        // The Vault feeds the spot/depth reads; rotating it must reset the
        // inherited guard state (fail-closed `TwapNotReady` until re-matured).
        assertEq(bb.twapPrice(), SPOT, "twap matured pre-rotation");
        MockBalancerV3Vault v2 = new MockBalancerV3Vault();
        v2.setPool(_orderedTokens(false), _orderedBals(false));
        vm.expectEmit(false, false, false, false, address(bb));
        emit GuardedBuybackBurner.GuardStateReset();
        vm.prank(gov);
        bb.setVault(address(v2));
        vm.expectRevert(GuardedBuybackBurner.TwapNotReady.selector);
        bb.twapPrice();
    }

    function test_setPool_unwireToZeroSkipsValidationAndSucceeds() public {
        // Zeroing a wired leg is the documented "not wired" path: it must NOT be
        // rejected by the new override even when the Vault would report the pool
        // unregistered. Disables the buyback (PoolNotWired) without reverting here.
        vault.setRegistered(false);
        vm.prank(gov);
        bb.setPool(address(0));
        assertEq(bb.balancerPool(), address(0), "pool unwired");
    }

    function test_wiring_validatesOnCompletingSetter_eitherOrder() public {
        // Deploy deferred, then wire in vault-then-pool order; the pool setter
        // that completes the pair triggers validation and passes.
        BuybackBurnerBalancerV3 a = _deployDeferred();
        vm.startPrank(gov);
        a.setVault(address(vault)); // pool == 0 -> validation skipped
        a.setPool(address(pool)); // completes the pair -> validates, passes
        vm.stopPrank();
        assertEq(a.balancerPool(), address(pool));
        assertEq(a.balancerVault(), address(vault));
    }

    function test_wiring_completingSetterValidates_setVaultLast() public {
        // Reverse order: setPool first (vault == 0, skipped), then setVault
        // completes the pair against a Vault that reports the pool unregistered.
        BuybackBurnerBalancerV3 a = _deployDeferred();
        vault.setRegistered(false);
        vm.startPrank(gov);
        a.setPool(address(pool)); // vault == 0 -> validation skipped
        vm.expectRevert(
            abi.encodeWithSelector(BuybackBurnerBalancerV3.PoolNotRegistered.selector, address(pool), address(vault))
        );
        a.setVault(address(vault)); // completes the pair -> validates -> reverts
        vm.stopPrank();
    }

    function _deployDeferred() internal returns (BuybackBurnerBalancerV3 a) {
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.pool_ = address(0);
        cfg.vault_ = address(0);
        a = new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
        bytes32 govRole = a.GOVERNANCE_ROLE();
        vm.prank(admin);
        a.grantRole(govRole, gov);
    }

    function test_twap_tracksRecentSpotAcrossMultipleWindows() public {
        // Roll several windows at SPOT, then double the TOKEN leg (spot -> 50e18)
        // and roll several more. The sliding anchor must discard the stale 25e18
        // region; twapPrice converges to the recent spot, not a lifetime average.
        for (uint256 i = 0; i < 4; ++i) {
            vm.warp(block.timestamp + 2 * TWAP_WINDOW);
            bb.poke();
        }
        vault.setBalance(0, POOL_TOKEN_18 * 2); // index 0 == TOKEN -> spot doubles
        for (uint256 i = 0; i < 4; ++i) {
            vm.warp(block.timestamp + 2 * TWAP_WINDOW);
            bb.poke();
        }
        assertApproxEqAbs(bb.twapPrice(), 50e18, 1e18, "twap tracks recent spot across rolls");
    }

    function _deployAgainstPool(IERC20[] memory t, uint256[] memory w, uint256[] memory b)
        internal
        returns (BuybackBurnerBalancerV3 bb2)
    {
        MockBalancerV3Vault v2 = new MockBalancerV3Vault();
        v2.setPool(t, b);
        MockBalancerV3WeightedPool p2 = new MockBalancerV3WeightedPool(w);
        BuybackBurnerBalancerV3.Config memory cfg = _defaultCfg();
        cfg.vault_ = address(v2);
        cfg.pool_ = address(p2);
        bb2 = new BuybackBurnerBalancerV3(IERC20(address(usdc)), ERC20Burnable(address(token)), admin, treasury, cfg);
    }

    function _orderedTokens(bool usdcFirst) internal view returns (IERC20[] memory t) {
        t = new IERC20[](2);
        t[0] = IERC20(address(usdcFirst ? address(usdc) : address(token)));
        t[1] = IERC20(address(usdcFirst ? address(token) : address(usdc)));
    }

    function _orderedWeights(bool usdcFirst) internal pure returns (uint256[] memory w) {
        w = new uint256[](2);
        w[0] = usdcFirst ? W_USDC : W_TOKEN;
        w[1] = usdcFirst ? W_TOKEN : W_USDC;
    }

    function _orderedBals(bool usdcFirst) internal pure returns (uint256[] memory b) {
        b = new uint256[](2);
        b[0] = usdcFirst ? POOL_USDC_18 : POOL_TOKEN_18;
        b[1] = usdcFirst ? POOL_TOKEN_18 : POOL_USDC_18;
    }

    function _defaultCfg() internal view returns (BuybackBurnerBalancerV3.Config memory cfg) {
        cfg = BuybackBurnerBalancerV3.Config({
            swapRouter_: IBalancerV3Router(address(router)),
            pool_: address(pool),
            vault_: address(vault),
            permit2_: address(permit2),
            twapMinWindow_: TWAP_WINDOW,
            maxBuybackAmount_: MAX_BUYBACK,
            minBuybackAmount_: MIN_BUYBACK,
            slippageBps_: SLIPPAGE_BPS,
            epochLiquidityCapFraction_: CAP_FRACTION
        });
    }
}
