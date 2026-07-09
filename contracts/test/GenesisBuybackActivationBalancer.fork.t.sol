// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { BaseProtocolDeploy } from "../script/BaseProtocolDeploy.s.sol";
import { BuybackBurnerBalancerV3 } from "../src/BuybackBurnerBalancerV3.sol";
import { IBalancerV3Vault } from "../src/interfaces/IBalancerV3Vault.sol";
import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";

/// @dev 6-decimal stand-in for USDC. The genesis deploy wires the whole protocol
///      to this token, and the Balancer seed pairs it against the script-deployed
///      burnable TOKEN in a pool the test creates on the live fork — the same
///      pattern the swap+burn fork suite uses (no live TOKEN pool exists pre-launch).
contract MintableUSDC is ERC20 {
    constructor() ERC20("Mock USD Coin", "USDC") { }

    function decimals() public pure override returns (uint8) {
        return 6;
    }

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }
}

/// @title GenesisBuybackActivationBalancerForkTest — Ethereum Sepolia acceptance
///        for the Balancer venue of the deploy-time genesis buyback activation.
/// @notice Runs the real `_runFullDeploy(cfg, act)` pipeline with `venue=BALANCER`
///         against the LIVE Balancer V3 deployment on Ethereum Sepolia (Balancer V3
///         is not on Arbitrum Sepolia, so the Balancer path is exercised here): the
///         script creates + seeds a real 80/20 TOKEN/USDC weighted pool via the live
///         `WeightedPoolFactory` + Router (Permit2), deploys `BuybackBurnerBalancerV3`
///         bound to it, flips the FeeRouter to `[6000, 3000, 1000]`, and hands every
///         role — burner included — to the Timelock. Asserts the full bundle landed
///         and that the burner reads a real TWAP spot off the seeded pool.
/// @dev    GATED: self-skips unless `SEPOLIA_RPC_URL` is set, so the default offline
///         `forge test` stays green and network-free. Forks at LATEST and creates
///         its own pool, so a plain public (non-archive) endpoint suffices. CI runs
///         this via the fork job (FOUNDRY_PROFILE=fork). Mirrors the addresses and
///         seed proportions of `BuybackBurnerBalancerV3.swapburn.fork.t.sol`.
contract GenesisBuybackActivationBalancerForkTest is Test, BaseProtocolDeploy {
    // Balancer V3 on Ethereum Sepolia (balancer/balancer-deployments). The Vault is
    // the canonical CREATE2 address shared across every V3 chain; Router + Factory
    // are Sepolia-specific. Permit2 is canonical on every chain.
    address internal constant VAULT = 0xbA1333333333a1BA1108E8412f11850A5C319bA9;
    address internal constant ROUTER = 0x5e315f96389C1aaF9324D97d3512ae1e0Bf3C21a; // v3-router-v2
    address internal constant FACTORY = 0xc383B240B40660cca6c5b6Fbf4fbAb85E9F4de24; // v3-weighted-pool
    address internal constant PERMIT2 = 0x000000000022D473030F116dDEE9F6B43aC78BA3;

    // 80/20 seed (ADR 018 proportions): 250k USDC pairs 100M TOKEN at the $0.01
    // anchor, giving a marginal spot of ~100 TOKEN per USDC.
    uint256 internal constant USDC_SEED = 250_000e6;
    uint256 internal constant TARGET_PRICE = 10_000; // $0.01/TOKEN in 6-dec USDC units
    uint256 internal constant EXPECTED_SPOT = 100e18; // TOKEN per USDC, 1e18 fixed point

    address internal emergencyMultisig = address(0xC0DE);
    address internal challengerPool = address(0xCCEE);
    address internal keeper = address(0xCAFE);

    MintableUSDC internal usdc;
    MockEd25519Verifier internal ed;
    bool internal forkActive;

    function setUp() public {
        if (bytes(vm.envOr("SEPOLIA_RPC_URL", string(""))).length == 0) return;
        vm.createSelectFork(vm.rpcUrl("sepolia"));
        forkActive = true;
        ed = new MockEd25519Verifier();
        usdc = new MintableUSDC();
        // The deployer (this) holds the full TOKEN supply as `initialTokenHolder`
        // and is funded with USDC for the pool seed.
        usdc.mint(address(this), 1_000_000e6);
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
            usdc: IERC20(address(usdc)),
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
        act.venue = BuybackVenue.BALANCER;
        act.keeper = keeper;
        act.twapMinWindow = 1800;
        act.maxBuybackAmount = 10_000e6;
        act.minBuybackAmount = 100e6;
        act.slippageBps = 200;
        act.epochLiquidityCapFraction = 1000;
        act.balFactory = FACTORY;
        act.balRouter = ROUTER;
        act.balVault = VAULT;
        act.permit2 = PERMIT2;
        act.balSwapFee = 1e16; // 1%
        act.balSubSwapCount = 4;
        act.balSubSwapMinBlockGap = 10;
        act.usdcSeed = USDC_SEED;
        act.tokenSeed = _deriveTokenSeed(BuybackVenue.BALANCER, USDC_SEED, TARGET_PRICE);
    }

    /// @notice The headline acceptance: flag ON + Balancer venue creates + seeds a
    ///         live 80/20 pool and lands the steady-state split with the burner
    ///         wired and keeper set.
    function test_balancer_genesisActivation_lands_full_bundle() public requiresFork {
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

        // The pool was created through the live factory and registered with the Vault.
        address pool = BuybackBurnerBalancerV3(address(d.buybackBurner)).balancerPool();
        assertTrue(IBalancerV3Vault(VAULT).isPoolRegistered(pool), "pool registered with live vault");
    }

    /// @notice The burner is a governed target too: its roles land on the Timelock
    ///         with no deployer back door.
    function test_balancer_burner_handed_to_timelock() public requiresFork {
        Deployment memory d = _runFullDeploy(_config(), _activation());
        address tl = address(d.timelock);
        assertTrue(d.buybackBurner.hasRole(GOVERNANCE_ROLE, tl), "burner gov to timelock");
        assertTrue(d.buybackBurner.hasRole(DEFAULT_ADMIN_ROLE, tl), "burner admin to timelock");
        assertFalse(d.buybackBurner.hasRole(GOVERNANCE_ROLE, address(this)), "no deployer gov back door");
        assertFalse(d.buybackBurner.hasRole(DEFAULT_ADMIN_ROLE, address(this)), "no deployer admin back door");
    }

    /// @notice The burner reads a real, non-zero TWAP spot off the freshly-seeded
    ///         live pool once the accumulator matures — proving the pool is priced
    ///         and correctly wired against the live Vault's scaled-18 balances.
    function test_balancer_seeded_pool_prices_twap() public requiresFork {
        Deployment memory d = _runFullDeploy(_config(), _activation());
        BuybackBurnerBalancerV3 burner = BuybackBurnerBalancerV3(address(d.buybackBurner));

        burner.poke();
        vm.warp(block.timestamp + 1800);
        burner.poke();

        assertApproxEqRel(burner.twapPrice(), EXPECTED_SPOT, 0.01e18, "twap ~= seeded 80/20 marginal spot");
    }
}
