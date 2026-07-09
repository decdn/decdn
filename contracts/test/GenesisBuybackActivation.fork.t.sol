// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { BaseProtocolDeploy } from "../script/BaseProtocolDeploy.s.sol";
import { IUniswapV3Factory } from "../script/interfaces/IUniswapV3PoolCreation.sol";
import { BuybackBurnerUniswapV3 } from "../src/BuybackBurnerUniswapV3.sol";
import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";

/// @title GenesisBuybackActivationForkTest — Arbitrum Sepolia acceptance for the
///        OFF-by-default deploy-time genesis buyback activation.
/// @notice Runs the real `_runFullDeploy(cfg, act)` pipeline against the LIVE
///         Uniswap V3 deployment on Arbitrum Sepolia (the initial network, where
///         Balancer V3 is not deployed): the script creates + seeds a real
///         TOKEN/USDC V3 pool via the live `NonfungiblePositionManager`, deploys
///         `BuybackBurnerUniswapV3` bound to it and the live `SwapRouter02`, flips
///         the FeeRouter to the steady-state `[6000, 3000, 1000]` split, and hands
///         every role — burner included — to the Timelock. Asserts the full bundle
///         landed and that the burner reads a real TWAP spot off the seeded pool.
/// @dev    GATED: self-skips unless `ARBITRUM_SEPOLIA_RPC_URL` is set, so the
///         default offline `forge test` stays green and network-free. Forks at
///         LATEST (no pinned block) and creates its own pool, so a plain public
///         (non-archive) endpoint suffices. CI runs this via the fork job
///         (FOUNDRY_PROFILE=fork). The mock Ed25519 verifier is injected because
///         this suite covers buyback activation, not signature verification.
contract GenesisBuybackActivationForkTest is Test, BaseProtocolDeploy {
    // Arbitrum Sepolia references (Uniswap V3 periphery + Circle testnet USDC).
    address internal constant USDC = 0x75faf114eafb1BDbe2F0316DF893fd58CE46AA4d;
    address internal constant SWAP_ROUTER = 0x101F443B4d1b059569D643917553c771E1b9663E; // SwapRouter02
    address internal constant FACTORY = 0x248AB79Bbb9bC29bB72f7Cd42F17e054Fc40188e;
    address internal constant POSITION_MANAGER = 0x6b2937Bde17889EDCf8fbD8dE31C3C2a70Bc4d65;
    uint24 internal constant FEE = 10_000; // 1% tier

    uint256 internal constant USDC_SEED = 10_000e6;
    uint256 internal constant TOKEN_SEED = 1_000_000e18; // pairs the USDC seed at the $0.01 anchor

    address internal emergencyMultisig = address(0xC0DE);
    address internal challengerPool = address(0xCCEE);
    address internal keeper = address(0xCAFE);

    MockEd25519Verifier internal ed;
    bool internal forkActive;

    function setUp() public {
        if (bytes(vm.envOr("ARBITRUM_SEPOLIA_RPC_URL", string(""))).length == 0) return;
        vm.createSelectFork(vm.rpcUrl("arbitrum_sepolia"));
        forkActive = true;
        ed = new MockEd25519Verifier();
        // Fund the deployer (this) with USDC for the pool seed; it already holds
        // the full TOKEN supply as `initialTokenHolder`.
        deal(USDC, address(this), 1_000_000e6);
    }

    modifier requiresFork() {
        if (!forkActive) {
            vm.skip(true);
            return;
        }
        _;
    }

    function _config() internal view returns (DeployConfig memory) {
        return DeployConfig({
            usdc: IERC20(USDC),
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

    function _activation() internal view returns (BuybackActivation memory act) {
        act.activate = true;
        act.venue = BuybackVenue.UNISWAP;
        act.keeper = keeper;
        act.twapMinWindow = 1800;
        act.maxBuybackAmount = 10_000e6;
        act.minBuybackAmount = 100e6;
        act.slippageBps = 200;
        act.epochLiquidityCapFraction = 1000;
        act.uniSwapRouter = SWAP_ROUTER;
        act.uniPositionManager = POSITION_MANAGER;
        act.uniPoolFee = FEE;
        act.usdcSeed = USDC_SEED;
        act.tokenSeed = TOKEN_SEED;
    }

    /// @notice The headline acceptance: flag ON + Uniswap venue lands the
    ///         steady-state split with the burner wired, keeper set, and a seeded
    ///         live pool created through the real factory.
    function test_uniswap_genesisActivation_lands_full_bundle() public requiresFork {
        Deployment memory d = _runFullDeploy(_config(), _activation());

        uint256[3] memory shares = d.router.getShares();
        assertEq(shares[0], 6000, "operator 6000");
        assertEq(shares[1], 3000, "buyback 3000");
        assertEq(shares[2], 1000, "treasury 1000");
        assertEq(d.router.buybackBurner(), address(d.buybackBurner), "burner wired");
        assertEq(d.router.treasury(), address(d.timelock), "treasury is timelock");

        assertTrue(d.buybackBurner.hasRole(d.buybackBurner.KEEPER_ROLE(), keeper), "keeper set");
        assertTrue(
            d.buybackBurner.hasRole(d.buybackBurner.PAUSER_ROLE(), emergencyMultisig), "pauser is emergency multisig"
        );

        // The pool was created through the LIVE factory and seeded with USDC depth.
        address pool = BuybackBurnerUniswapV3(address(d.buybackBurner)).pool();
        assertEq(IUniswapV3Factory(FACTORY).getPool(USDC, address(d.token), FEE), pool, "pool registered in factory");
        assertGt(IERC20(USDC).balanceOf(pool), 0, "pool seeded with USDC");
    }

    /// @notice The burner is a governed target too: its roles land on the Timelock
    ///         with no deployer back door (the in-script `setKeeper` window closed).
    function test_uniswap_burner_handed_to_timelock() public requiresFork {
        Deployment memory d = _runFullDeploy(_config(), _activation());
        address tl = address(d.timelock);
        assertTrue(d.buybackBurner.hasRole(GOVERNANCE_ROLE, tl), "burner gov to timelock");
        assertTrue(d.buybackBurner.hasRole(DEFAULT_ADMIN_ROLE, tl), "burner admin to timelock");
        assertFalse(d.buybackBurner.hasRole(GOVERNANCE_ROLE, address(this)), "no deployer gov back door");
        assertFalse(d.buybackBurner.hasRole(DEFAULT_ADMIN_ROLE, address(this)), "no deployer admin back door");
    }

    /// @notice The burner reads a real, non-zero TWAP spot off the freshly-seeded
    ///         live pool once the accumulator matures — proving the pool is priced
    ///         and correctly wired (not just present). Mirrors the ADR 018 TWAP
    ///         warmup note: buybacks revert `TwapNotReady` until the window matures.
    function test_uniswap_seeded_pool_prices_twap() public requiresFork {
        Deployment memory d = _runFullDeploy(_config(), _activation());
        BuybackBurnerUniswapV3 burner = BuybackBurnerUniswapV3(address(d.buybackBurner));

        burner.poke();
        vm.warp(block.timestamp + 1800);
        burner.poke();

        // ~100 TOKEN per USDC at the $0.01 anchor (1e18 fixed point). Loose bound:
        // real pool math + tick rounding shift the marginal spot slightly.
        assertApproxEqRel(burner.twapPrice(), 100e18, 0.05e18, "twap ~= seeded anchor spot");
    }
}
