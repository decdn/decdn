// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { BaseProtocolDeploy } from "../script/BaseProtocolDeploy.s.sol";
import { INonfungiblePositionManager } from "../script/interfaces/IUniswapV3PoolCreation.sol";
import { BuybackBurnerUniswapV3 } from "../src/BuybackBurnerUniswapV3.sol";
import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";

// -----------------------------------------------------------------
// Mocks: a Uniswap V3 pool + NonfungiblePositionManager stand-in so the genesis
// activation bundle runs in-process (no fork) under the default `forge test`.
// The live-Uniswap acceptance path is covered by the gated Arbitrum Sepolia fork
// test `GenesisBuybackActivation.fork.t.sol`.
// -----------------------------------------------------------------

contract MockUSDC is ERC20 {
    constructor() ERC20("USDC", "USDC") { }

    function decimals() public pure override returns (uint8) {
        return 6;
    }

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

/// @notice Minimal Uniswap V3 pool: fixed `slot0` sqrt-price, token ordering, fee.
///         Holds real ERC20 balances (the mock manager transfers the seed here),
///         so `usdc.balanceOf(pool)` reads the seeded depth like a live pool.
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

/// @notice `NonfungiblePositionManager` stand-in: `createAndInitialize…` deploys a
///         `MockV3Pool` at the given price; `mint` pulls both legs from the caller
///         into the pool (simulating a seeded position). `seed=false` skips the
///         pull so the `PoolNotSeeded` guard can be exercised.
contract MockPositionManager {
    address public lastPool;
    bool internal immutable seed;

    constructor(bool seed_) {
        seed = seed_;
    }

    function createAndInitializePoolIfNecessary(address token0, address token1, uint24 fee, uint160 sqrtPriceX96)
        external
        payable
        returns (address pool)
    {
        pool = address(new MockV3Pool(sqrtPriceX96, token0, token1, fee));
        lastPool = pool;
    }

    function mint(INonfungiblePositionManager.MintParams calldata p)
        external
        payable
        returns (uint256, uint128, uint256, uint256)
    {
        if (seed) {
            // slither-disable-next-line unchecked-transfer
            IERC20(p.token0).transferFrom(msg.sender, lastPool, p.amount0Desired);
            // slither-disable-next-line unchecked-transfer
            IERC20(p.token1).transferFrom(msg.sender, lastPool, p.amount1Desired);
        }
        return (1, uint128(1), p.amount0Desired, p.amount1Desired);
    }

    function factory() external pure returns (address) {
        return address(0);
    }
}

/// @title GenesisBuybackActivationTest — in-process coverage of the OFF-by-default
///        deploy-time genesis buyback activation (`BuybackActivation`).
/// @notice Runs the `_runFullDeploy(cfg, act)` pipeline under the test contract's
///         own context (like `DeployProtocolTest`) so state is assertable without a
///         fork. Proves: (1) the OFF path reproduces the dormant launch exactly;
///         (2) the ON Uniswap path lands the steady-state `[6000, 3000, 1000]`
///         split with the burner wired, keeper set, pool seeded, and every role
///         handed to the Timelock; (3) the activation guards revert.
contract GenesisBuybackActivationTest is Test, BaseProtocolDeploy {
    MockUSDC internal usdc;
    MockEd25519Verifier internal ed;

    address internal emergencyMultisig = address(0xC0DE);
    address internal challengerPool = address(0xCCEE);
    address internal keeper = address(0xCAFE);
    // A nonzero SwapRouter02 stand-in — the burner constructor only checks it is
    // nonzero; no swap is executed in a deploy test.
    address internal swapRouter = address(0x5AFE);

    uint256 internal constant USDC_SEED = 10_000e6;
    uint256 internal constant TOKEN_SEED = 1_000_000e18;

    function setUp() public {
        usdc = new MockUSDC();
        ed = new MockEd25519Verifier();
        // The deployer (this contract) is the initial TOKEN holder and is funded
        // with USDC so the in-script seed pull has both legs.
        usdc.mint(address(this), 100_000_000e6);
    }

    function _config() internal view returns (DeployConfig memory) {
        return DeployConfig({
            usdc: usdc,
            ed25519Verifier: ed,
            deployer: address(this),
            emergencyMultisig: emergencyMultisig,
            initialTokenHolder: address(this),
            challengerIncentivePool: challengerPool,
            timelockDelay: 48 hours,
            minBond: 50_000e18,
            unbondingPeriod: 14 days,
            multiaddrUpdateCooldown: 0,
            maxMultiaddrSize: 1024,
            regionStabilityWindow: 7 days,
            currentTermsHash: keccak256("decdn operator terms v1"),
            feeRouterEpochLength: 7 days,
            feeRouterWindowEpochs: 13,
            feeRouterShares: [uint256(9000), uint256(0), uint256(1000)],
            buybackBurner: address(0),
            slashAppealBond: 1000e18,
            blacklistAppealBond: 100e18
        });
    }

    function _uniswapActivation(address positionManager) internal view returns (BuybackActivation memory act) {
        act.activate = true;
        act.venue = BuybackVenue.UNISWAP;
        act.keeper = keeper;
        act.twapMinWindow = 1800;
        act.maxBuybackAmount = 10_000e6;
        act.minBuybackAmount = 100e6;
        act.slippageBps = 200;
        act.epochLiquidityCapFraction = 1000;
        act.uniSwapRouter = swapRouter;
        act.uniPositionManager = positionManager;
        act.uniPoolFee = 10_000;
        act.usdcSeed = USDC_SEED;
        act.tokenSeed = TOKEN_SEED;
    }

    // External wrapper so `vm.expectRevert` catches reverts at the call boundary.
    function externalRunFullDeploy(DeployConfig calldata cfg, BuybackActivation calldata act)
        external
        returns (Deployment memory)
    {
        return _runFullDeploy(cfg, act);
    }

    // -----------------------------------------------------------------
    // OFF path — byte-for-byte the dormant launch
    // -----------------------------------------------------------------

    function test_off_reproducesDormantLaunch() public {
        Deployment memory d = _runFullDeploy(_config(), _noBuybackActivation());

        uint256[3] memory shares = d.router.getShares();
        assertEq(shares[0], 9000, "operator 9000");
        assertEq(shares[1], 0, "buyback dormant");
        assertEq(shares[2], 1000, "treasury 1000");
        assertEq(d.router.buybackBurner(), address(0), "burner unwired");
        assertEq(address(d.buybackBurner), address(0), "no burner deployed");
    }

    function test_off_singleArgMatchesExplicitOff() public {
        // The single-arg entry point must be exactly the all-off activation.
        Deployment memory a = _runFullDeploy(_config());
        assertEq(a.router.getShares()[1], 0, "single-arg dormant");
        assertEq(address(a.buybackBurner), address(0), "single-arg no burner");
    }

    // -----------------------------------------------------------------
    // ON path (Uniswap) — full activation bundle
    // -----------------------------------------------------------------

    function test_uniswap_activatesFullBundle() public {
        MockPositionManager npm = new MockPositionManager(true);
        // The manager pulls the seed via allowance from the deployer (this).
        usdc.approve(address(npm), type(uint256).max);

        Deployment memory d = _runFullDeploy(_config(), _uniswapActivation(address(npm)));

        // FeeRouter flipped to steady-state split with the burner + timelock wired.
        uint256[3] memory shares = d.router.getShares();
        assertEq(shares[0], 6000, "operator 6000");
        assertEq(shares[1], 3000, "buyback 3000");
        assertEq(shares[2], 1000, "treasury 1000");
        assertEq(d.router.buybackBurner(), address(d.buybackBurner), "burner wired into router");
        assertEq(d.router.treasury(), address(d.timelock), "treasury is timelock");

        // Keeper + pauser wired on the burner.
        assertTrue(d.buybackBurner.hasRole(d.buybackBurner.KEEPER_ROLE(), keeper), "keeper role");
        assertTrue(d.buybackBurner.hasRole(d.buybackBurner.PAUSER_ROLE(), emergencyMultisig), "pauser role to multisig");

        // Pool created + seeded (USDC depth for the per-epoch cap denominator).
        address pool = BuybackBurnerUniswapV3(address(d.buybackBurner)).pool();
        assertGt(usdc.balanceOf(pool), 0, "pool seeded with USDC");
        assertEq(usdc.balanceOf(pool), USDC_SEED, "pool holds the USDC seed");
        assertEq(d.token.balanceOf(pool), TOKEN_SEED, "pool holds the TOKEN seed");
    }

    function test_uniswap_burnerHandedToTimelock_noDeployerBackDoor() public {
        MockPositionManager npm = new MockPositionManager(true);
        usdc.approve(address(npm), type(uint256).max);

        Deployment memory d = _runFullDeploy(_config(), _uniswapActivation(address(npm)));
        address tl = address(d.timelock);

        assertTrue(d.buybackBurner.hasRole(GOVERNANCE_ROLE, tl), "timelock gov");
        assertTrue(d.buybackBurner.hasRole(DEFAULT_ADMIN_ROLE, tl), "timelock admin");
        assertFalse(d.buybackBurner.hasRole(GOVERNANCE_ROLE, address(this)), "no deployer gov back door");
        assertFalse(d.buybackBurner.hasRole(DEFAULT_ADMIN_ROLE, address(this)), "no deployer admin back door");
    }

    // -----------------------------------------------------------------
    // Guards
    // -----------------------------------------------------------------

    function test_revertsWhenKeeperMissing() public {
        BuybackActivation memory act = _uniswapActivation(address(0xdead));
        act.keeper = address(0);
        vm.expectRevert(BaseProtocolDeploy.MissingBuybackKeeper.selector);
        this.externalRunFullDeploy(_config(), act);
    }

    function test_revertsWhenPoolNotSeeded() public {
        // A manager that creates the pool but seeds no liquidity must fail the
        // "seed before wire" guard.
        MockPositionManager npm = new MockPositionManager(false);
        usdc.approve(address(npm), type(uint256).max);
        BuybackActivation memory act = _uniswapActivation(address(npm));
        // Selector-only: PoolNotSeeded(pool)'s arg is the runtime-computed pool.
        vm.expectPartialRevert(BaseProtocolDeploy.PoolNotSeeded.selector);
        this.externalRunFullDeploy(_config(), act);
    }

    function test_revertsWhenDeployerLacksSeedBalance() public {
        MockPositionManager npm = new MockPositionManager(true);
        usdc.approve(address(npm), type(uint256).max);
        DeployConfig memory cfg = _config();
        // Route TOKEN to a different holder so the deployer cannot fund the seed.
        cfg.initialTokenHolder = address(0xBEEF);
        // Selector-only: InsufficientSeedBalance(token, have, need) carries runtime args.
        vm.expectPartialRevert(BaseProtocolDeploy.InsufficientSeedBalance.selector);
        this.externalRunFullDeploy(cfg, _uniswapActivation(address(npm)));
    }

    // The Balancer venue creates + seeds its pool through the live Balancer V3
    // factory/router (Permit2), so its full-bundle coverage is the gated Ethereum
    // Sepolia fork suite `GenesisBuybackActivationBalancer.fork.t.sol` — the
    // factory/vault/router stack is impractical to mock in-process. The keeper
    // guard here (`test_revertsWhenKeeperMissing`) is venue-agnostic and fires
    // before any venue-specific work, so it covers the Balancer path too.
}
