// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { BaseProtocolDeploy } from "../script/BaseProtocolDeploy.s.sol";
import { IEd25519Verifier } from "../src/interfaces/IEd25519Verifier.sol";
import { Token } from "../src/Token.sol";
import { CapacityBond } from "../src/CapacityBond.sol";
import { FeeRouter } from "../src/FeeRouter.sol";
import { SafetyReserve } from "../src/SafetyReserve.sol";
import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { PublisherRegistry } from "../src/PublisherRegistry.sol";

import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";

contract DeployUSDC is ERC20 {
    constructor() ERC20("USDC", "USDC") { }

    function decimals() public pure override returns (uint8) {
        return 6;
    }
}

/// @title DeployProtocolTest — companion in-process test for the production
///        deploy script (issue #694).
/// @notice Asserts the post-deploy role topology matches ADR 016 § Role Matrix
///         and that `_assertNoBackDoors` is a real guard, not a no-op.
///         The script itself runs the same `_runFullDeploy` pipeline under
///         `vm.startBroadcast` — this test runs it under the test contract's
///         own context so we can assert state without relying on a fork.
contract DeployProtocolTest is Test, BaseProtocolDeploy {
    address internal emergencyMultisig = address(0xC0DE);
    address internal initialTokenHolder = address(0xBEEF);
    address internal challengerPool = address(0xCCEE);

    Deployment internal d;
    DeployConfig internal cfg;

    function setUp() public {
        cfg = _testConfig();
        d = _runFullDeploy(cfg);
    }

    function _testConfig() internal returns (DeployConfig memory) {
        DeployUSDC usdc = new DeployUSDC();
        MockEd25519Verifier ed = new MockEd25519Verifier();
        return DeployConfig({
            usdc: usdc,
            ed25519Verifier: ed,
            deployer: address(this),
            emergencyMultisig: emergencyMultisig,
            initialTokenHolder: initialTokenHolder,
            challengerIncentivePool: challengerPool,
            timelockDelay: 48 hours,
            minStake: 50_000e18,
            unbondingPeriod: 14 days,
            multiaddrUpdateCooldown: 0,
            maxMultiaddrSize: 1024,
            regionStabilityWindow: 7 days,
            genesisCreditWindow: 30 days,
            feeRouterEpochLength: 7 days,
            feeRouterWindowEpochs: 13,
            feeRouterShares: [uint256(8500), uint256(0), uint256(1000), uint256(500)],
            buybackBurner: address(0),
            safetyAppealBond: 1000e18,
            maxAppealRestitution: 100_000e6,
            blacklistAppealBond: 100e18
        });
    }

    // Role-matrix assertions (ADR 016 § Access Control Matrix). Mirror of
    // GovernanceLifecycleTest.test_canary_governanceRoleHandoff so the deploy
    // script and the governance lifecycle suite stay in lockstep — if the
    // base contract drifts, both tests fail in the same way.

    function test_roleMatrix_timelockHoldsGovernanceOnEveryTarget() public view {
        address tl = address(d.timelock);
        assertTrue(d.router.hasRole(GOVERNANCE_ROLE, tl), "router gov");
        assertTrue(d.bond.hasRole(GOVERNANCE_ROLE, tl), "bond gov");
        assertTrue(d.blacklist.hasRole(GOVERNANCE_ROLE, tl), "blacklist gov");
        assertTrue(d.reserve.hasRole(GOVERNANCE_ROLE, tl), "reserve gov");
        assertTrue(d.registry.hasRole(GOVERNANCE_ROLE, tl), "registry gov");
    }

    function test_roleMatrix_timelockHoldsDefaultAdminOnEveryTarget() public view {
        address tl = address(d.timelock);
        assertTrue(d.router.hasRole(DEFAULT_ADMIN_ROLE, tl), "router admin");
        assertTrue(d.bond.hasRole(DEFAULT_ADMIN_ROLE, tl), "bond admin");
        assertTrue(d.blacklist.hasRole(DEFAULT_ADMIN_ROLE, tl), "blacklist admin");
        assertTrue(d.reserve.hasRole(DEFAULT_ADMIN_ROLE, tl), "reserve admin");
        assertTrue(d.registry.hasRole(DEFAULT_ADMIN_ROLE, tl), "registry admin");
    }

    function test_roleMatrix_deployerHoldsNoRoleAnywhere() public view {
        address dep = cfg.deployer;
        assertFalse(d.router.hasRole(GOVERNANCE_ROLE, dep), "router gov back door");
        assertFalse(d.bond.hasRole(GOVERNANCE_ROLE, dep), "bond gov back door");
        assertFalse(d.blacklist.hasRole(GOVERNANCE_ROLE, dep), "blacklist gov back door");
        assertFalse(d.reserve.hasRole(GOVERNANCE_ROLE, dep), "reserve gov back door");
        assertFalse(d.registry.hasRole(GOVERNANCE_ROLE, dep), "registry gov back door");

        assertFalse(d.router.hasRole(DEFAULT_ADMIN_ROLE, dep), "router admin back door");
        assertFalse(d.bond.hasRole(DEFAULT_ADMIN_ROLE, dep), "bond admin back door");
        assertFalse(d.blacklist.hasRole(DEFAULT_ADMIN_ROLE, dep), "blacklist admin back door");
        assertFalse(d.reserve.hasRole(DEFAULT_ADMIN_ROLE, dep), "reserve admin back door");
        assertFalse(d.registry.hasRole(DEFAULT_ADMIN_ROLE, dep), "registry admin back door");

        assertFalse(d.timelock.hasRole(DEFAULT_ADMIN_ROLE, dep), "timelock admin back door");
    }

    function test_roleMatrix_emergencyMultisigGrants() public view {
        assertTrue(d.reserve.hasRole(d.reserve.EMERGENCY_MULTISIG_ROLE(), emergencyMultisig), "reserve emergency");
        assertTrue(d.blacklist.hasRole(d.blacklist.EMERGENCY_MULTISIG_ROLE(), emergencyMultisig), "blacklist emergency");
    }

    // -----------------------------------------------------------------
    // Cross-contract wiring (ADR 016 § Post-Deployment Initialization)
    // -----------------------------------------------------------------

    function test_crossContractWiring_capacityBondPeerRoles() public view {
        assertTrue(d.bond.hasRole(d.bond.SETTLEMENT_REPORTER_ROLE(), address(d.router)), "router to bond settlement");
        assertTrue(d.bond.hasRole(d.bond.APPEAL_REVERSAL_ROLE(), address(d.reserve)), "reserve to bond reversal");
        assertTrue(d.bond.hasRole(d.bond.BLACKLIST_ROLE(), address(d.blacklist)), "blacklist to bond eject");
        assertTrue(d.reserve.hasRole(d.reserve.SLASH_INFLOW_REPORTER_ROLE(), address(d.bond)), "bond to reserve inflow");
    }

    function test_crossContractWiring_capacityBondPointsAtReserve() public view {
        assertEq(address(d.bond.safetyReserve()), address(d.reserve), "bond.safetyReserve");
    }

    function test_crossContractWiring_reserveChallengerPool() public view {
        assertEq(d.reserve.challengerIncentivePool(), challengerPool, "reserve.challengerIncentivePool");
    }

    // -----------------------------------------------------------------
    // Governor / Timelock binding
    // -----------------------------------------------------------------

    function test_governor_timelockBinding() public view {
        assertEq(d.governor.timelock(), address(d.timelock), "governor.timelock");
        assertTrue(d.timelock.hasRole(d.timelock.PROPOSER_ROLE(), address(d.governor)), "proposer");
        assertTrue(d.timelock.hasRole(d.timelock.CANCELLER_ROLE(), address(d.governor)), "canceller");
        assertEq(d.timelock.getMinDelay(), cfg.timelockDelay, "min delay");
    }

    function test_governor_openExecutor() public view {
        // Open executor: address(0) holds EXECUTOR_ROLE so anyone may execute
        // after the delay. Standard OZ pattern — the privileged step is the
        // 48h schedule (ADR 009), not the execute call.
        assertTrue(d.timelock.hasRole(d.timelock.EXECUTOR_ROLE(), address(0)), "open executor");
    }

    // FeeRouter launch state — buyback bucket dormant pending a concrete
    // BuybackBurner subclass (see BaseProtocolDeploy header).

    function test_feeRouter_launchSharesDormantBuyback() public view {
        uint256[4] memory shares = d.router.getShares();
        assertEq(shares[0] + shares[1] + shares[2] + shares[3], 10_000, "shares sum to 10_000");
        assertEq(shares[1], 0, "buyback share is 0 at launch (dormant)");
        assertEq(d.router.buybackBurner(), address(0), "buybackBurner unwired at launch");
    }

    function test_feeRouter_treasuryIsTimelock() public view {
        // ADR 016 § Deployment Order step 7: the treasury bucket is
        // Timelock-custodied. The script deploys the Timelock first precisely so
        // its address can seed FeeRouter's treasury, not an external EOA.
        assertEq(d.router.treasury(), address(d.timelock), "router.treasury == timelock");
    }

    function test_feeRouter_safetyBucketWiredAtLaunch() public view {
        // SafetyReserve is concrete and deployable, so the safety bucket
        // ships active from day one (unlike the buyback bucket).
        assertEq(d.router.safetyReserve(), address(d.reserve), "router.safetyReserve");
    }

    function test_token_initialHolderHoldsFullSupply() public view {
        assertEq(d.token.balanceOf(initialTokenHolder), d.token.TOTAL_SUPPLY(), "initial holder");
        assertEq(d.token.totalSupply(), 1_000_000_000e18, "total supply");
    }

    // Negative test: `_assertNoBackDoors` must REVERT if the handoff is
    // incomplete. Without this guarantee, the script silently succeeds even
    // when the deployer keeps god-mode on a target — the failure mode #694
    // closes.

    function test_assertNoBackDoors_revertsWhenHandoffIncomplete() public {
        DeployConfig memory cfg2 = _testConfig();
        // Run deploy + governance + wiring, but DELIBERATELY skip the handoff.
        Deployment memory d2 = _deployTargets(cfg2, _deployTimelock(cfg2));
        _deployGovernor(cfg2, d2);
        _wireCrossContractRoles(cfg2, d2);
        // Sanity: deployer still holds GOVERNANCE_ROLE on router (handoff skipped).
        assertTrue(d2.router.hasRole(GOVERNANCE_ROLE, cfg2.deployer), "precondition gov");
        assertTrue(d2.router.hasRole(DEFAULT_ADMIN_ROLE, cfg2.deployer), "precondition admin");

        // `vm.expectRevert` only catches reverts at external-call boundaries,
        // so we route through `this.<external wrapper>` instead of calling the
        // internal `_assertNoBackDoors` directly.
        vm.expectRevert(
            abi.encodeWithSelector(
                BaseProtocolDeploy.DeployerStillHoldsRole.selector, address(d2.router), GOVERNANCE_ROLE
            )
        );
        this.externalAssertNoBackDoors(cfg2, d2);
    }

    function externalAssertNoBackDoors(DeployConfig calldata cfg2, Deployment calldata d2) external view {
        _assertNoBackDoors(cfg2, d2);
    }

    // ZeroAddress fail-fast checks in `_deployTargets`. One test per validated
    // field — these guard against silent misconfiguration (e.g. SafetyReserve
    // silently skipping the EMERGENCY_MULTISIG_ROLE grant when the multisig
    // is zero, or ContentBlacklist granting that role to `address(0)`).

    function externalDeployTargets(DeployConfig calldata cfgIn) external returns (Deployment memory) {
        return _deployTargets(cfgIn, _deployTimelock(cfgIn));
    }

    function _expectZeroAddressRevert(DeployConfig memory bad, string memory field) internal {
        vm.expectRevert(abi.encodeWithSelector(BaseProtocolDeploy.ZeroAddress.selector, field));
        this.externalDeployTargets(bad);
    }

    function test_deployTargets_revertsOnZeroUsdc() public {
        DeployConfig memory bad = _testConfig();
        bad.usdc = IERC20(address(0));
        _expectZeroAddressRevert(bad, "usdc");
    }

    function test_deployTargets_revertsOnZeroEd25519Verifier() public {
        DeployConfig memory bad = _testConfig();
        bad.ed25519Verifier = IEd25519Verifier(address(0));
        _expectZeroAddressRevert(bad, "ed25519Verifier");
    }

    function test_deployTargets_revertsOnZeroDeployer() public {
        DeployConfig memory bad = _testConfig();
        bad.deployer = address(0);
        _expectZeroAddressRevert(bad, "deployer");
    }

    function test_deployTargets_revertsOnZeroEmergencyMultisig() public {
        DeployConfig memory bad = _testConfig();
        bad.emergencyMultisig = address(0);
        _expectZeroAddressRevert(bad, "emergencyMultisig");
    }

    function test_deployTargets_revertsOnZeroInitialTokenHolder() public {
        DeployConfig memory bad = _testConfig();
        bad.initialTokenHolder = address(0);
        _expectZeroAddressRevert(bad, "initialTokenHolder");
    }

    function test_deployTargets_revertsOnZeroChallengerIncentivePool() public {
        DeployConfig memory bad = _testConfig();
        bad.challengerIncentivePool = address(0);
        _expectZeroAddressRevert(bad, "challengerIncentivePool");
    }
}
