// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";
import { IGovernor } from "@openzeppelin/contracts/governance/IGovernor.sol";

import { BaseProtocolDeploy } from "../script/BaseProtocolDeploy.s.sol";
import { DecdnGovernor } from "../src/DecdnGovernor.sol";
import { FeeRouter } from "../src/FeeRouter.sol";
import { CapacityBond } from "../src/CapacityBond.sol";
import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { SlashAppeal } from "../src/SlashAppeal.sol";
import { PublisherRegistry } from "../src/PublisherRegistry.sol";
import { Token } from "../src/Token.sol";

import { MockEd25519Verifier } from "./mocks/MockEd25519Verifier.sol";

contract LifecycleUSDC is ERC20 {
    constructor() ERC20("USDC", "USDC") {
        _mint(msg.sender, 1_000_000_000e6);
    }

    function decimals() public pure override returns (uint8) {
        return 6;
    }
}

/// @title GovernanceLifecycleTest — full propose → vote → queue → execute for
///        every `GOVERNANCE_ROLE`-gated setter on the production contracts
///        (issue #693).
/// @notice `DecdnGovernor.t.sol` deliberately skips the production governance
///         path and only exercises vote-weight math against mocks. This file
///         deploys the real `FeeRouter`, `CapacityBond`, `ContentBlacklist`,
///         `SlashAppeal`, `PublisherRegistry`, `DecdnGovernor`, and
///         `TimelockController`, hands off `GOVERNANCE_ROLE` on every target
///         to the Timelock to mirror the production deploy ordering (issue
///         #694 supplies the deploy script), and asserts that each setter's
///         happy and out-of-bounds path closes through the Governor → Timelock
///         → target wiring.
///
/// @dev    Seeding strategy for proposal-threshold (0.1%) + quorum (4%):
///         three test operators (`proposer`, `voter1`, `voter2`) bond at T0,
///         then we walk forward 60 epochs (≈ 420 days, past the default 6-month
///         age ramp so `age_ramp == 1`). Across the trailing 13 epochs we
///         call `FeeRouter.routeSettlement` to populate `bytesPerEpoch` for
///         each operator. With `voteCapBps` at its 500 bps default, each
///         operator's vote weight saturates at 5% of total — three operators
///         voting `For` clear the 4% quorum, and `proposer` alone clears the
///         0.1% proposal threshold by a wide margin.
contract GovernanceLifecycleTest is Test, BaseProtocolDeploy {
    // -----------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------

    uint64 internal constant EPOCH = 7 days;
    uint64 internal constant WINDOW_EPOCHS = 13;
    uint64 internal constant FINAL_EPOCH = 60;
    // Matches the ADR 009 production minimum. Lifecycle helpers read the live
    // `timelock.getMinDelay()` so a future change in DecdnGovernor doesn't
    // silently desync this test.
    uint256 internal constant TIMELOCK_DELAY = 48 hours;

    uint256 internal constant MIN_BOND = 50_000e18;
    uint256 internal constant UNBONDING_PERIOD = 7 days;
    uint256 internal constant SLASH_APPEAL_BOND = 1000e18;

    // -----------------------------------------------------------------
    // Deployments
    // -----------------------------------------------------------------

    LifecycleUSDC internal usdc;
    Token internal token;
    MockEd25519Verifier internal ed25519;
    CapacityBond internal bond;
    FeeRouter internal router;
    SlashAppeal internal slashAppeal;
    ContentBlacklist internal blacklist;
    PublisherRegistry internal registry;
    TimelockController internal timelock;
    DecdnGovernor internal gov;

    // -----------------------------------------------------------------
    // Test accounts
    // -----------------------------------------------------------------

    address internal proposer = address(0xAAAA);
    address internal voter1 = address(0xBBBB);
    address internal voter2 = address(0xCCCC);
    address internal multisig = address(0xC0DE);
    bytes32 internal constant REGION_DE = bytes32("DE");

    // Counter used to make each proposal description unique so identical
    // setter calls don't collide on `proposalId`.
    uint256 internal _propCounter;

    // -----------------------------------------------------------------
    // setUp — production-shaped role hand-off
    // -----------------------------------------------------------------

    function setUp() public {
        usdc = new LifecycleUSDC();
        ed25519 = new MockEd25519Verifier();

        DeployConfig memory cfg = DeployConfig({
            usdc: usdc,
            ed25519Verifier: ed25519,
            deployer: address(this),
            emergencyMultisig: multisig,
            // Token is minted entirely to the test harness so it can fund
            // operators + FeeRouter bucket destinations downstream.
            initialTokenHolder: address(this),
            timelockDelay: TIMELOCK_DELAY,
            // Direct-to-Timelock handoff; the ADR 009 bootstrap phase is opt-in.
            bootstrapMultisig: address(0),
            minBond: MIN_BOND,
            unbondingPeriod: UNBONDING_PERIOD,
            multiaddrUpdateCooldown: 0,
            maxMultiaddrSize: 1024,
            regionStabilityWindow: 7 days,
            currentTermsHash: keccak256("decdn operator terms v1"),
            feeRouterEpochLength: EPOCH,
            feeRouterWindowEpochs: WINDOW_EPOCHS,
            // Steady-state shares (60/30/10 from ADR 026 § FeeRouter split) so
            // the setter / topology exercise covers the production-activated
            // state, not the launch dormancy that DeployProtocol.s.sol ships.
            feeRouterShares: [uint256(6000), uint256(3000), uint256(1000)],
            buybackBurner: address(0xBB),
            slashAppealBond: SLASH_APPEAL_BOND
        });

        Deployment memory d = _runFullDeploy(cfg);
        token = d.token;
        bond = d.bond;
        slashAppeal = d.slashAppeal;
        router = d.router;
        blacklist = d.blacklist;
        registry = d.registry;
        timelock = d.timelock;
        gov = d.governor;

        _bondOperatorsAndSeedVoteWeight();
    }

    /// @dev Override the base hook to grant the test harness
    ///      `ROUTER_CALLER_ROLE` on FeeRouter so it can call `routeSettlement`
    ///      to seed served-bytes for vote-weight tests. In production the role
    ///      belongs to `PaymentChannel` alone — the deploy script grants it
    ///      there and asserts it — so this grant is an extra holder for the
    ///      harness, not a substitute. The hook runs in phase 4 — before the
    ///      GOVERNANCE_ROLE handoff, which would otherwise put `grantRole`
    ///      behind the 48h Timelock.
    function _postWiringHook(DeployConfig memory, Deployment memory dDeploy) internal override {
        dDeploy.router.grantRole(dDeploy.router.ROUTER_CALLER_ROLE(), address(this));
    }

    function _bondOperatorsAndSeedVoteWeight() internal {
        address[3] memory ops = [proposer, voter1, voter2];

        // Fund each operator with TOKEN, approve CapacityBond, and bond.
        for (uint256 i = 0; i < ops.length; i++) {
            token.transfer(ops[i], MIN_BOND * 10);
            vm.prank(ops[i]);
            token.approve(address(bond), type(uint256).max);
            vm.prank(ops[i]);
            bond.bond(MIN_BOND);
        }

        // Fund the test harness with USDC and approve the router. Each epoch
        // we seed three settlements; USDC headroom must cover the full
        // trailing window with margin.
        usdc.approve(address(router), type(uint256).max);

        // Walk forward `FINAL_EPOCH` epochs in chronological order. Settlement
        // is recorded for the trailing `WINDOW_EPOCHS` epochs so every
        // operator's `bytesInWindow` is non-zero at the eventual proposal
        // snapshot.
        for (uint64 e = 1; e <= FINAL_EPOCH; e++) {
            vm.warp(uint256(e) * EPOCH + 1);
            if (e + WINDOW_EPOCHS > FINAL_EPOCH) {
                for (uint256 k = 0; k < ops.length; k++) {
                    // 1_000_000 bytes / 1e6 USDC unit per settlement.
                    router.routeSettlement(ops[k], 1_000_000, 1e6);
                }
            }
        }

        // Sanity: each operator individually has non-zero vote weight at the
        // current snapshot AND `proposer` clears the proposal threshold. If
        // the seeding loop silently skipped any operator, every downstream
        // test would fail at `propose()` (threshold) or `queue()` (quorum)
        // with no pointer back to seeding — these asserts surface the cause
        // here.
        uint48 snap = gov.clock() - 1;
        assertGt(gov.getVotes(proposer, snap), gov.proposalThreshold(), "proposer below threshold");
        assertGt(gov.getVotes(voter1, snap), 0, "voter1 unseeded");
        assertGt(gov.getVotes(voter2, snap), 0, "voter2 unseeded");
    }

    // -----------------------------------------------------------------
    // Canary tests — guard the assumptions every lifecycle test relies on.
    // -----------------------------------------------------------------

    function test_canary_governanceRoleHandoff() public view {
        address tl = address(timelock);
        bytes32 defaultAdmin = 0x00;

        // Timelock holds GOVERNANCE_ROLE on every target.
        assertTrue(router.hasRole(GOVERNANCE_ROLE, tl), "router gov");
        assertTrue(bond.hasRole(GOVERNANCE_ROLE, tl), "bond gov");
        assertTrue(blacklist.hasRole(GOVERNANCE_ROLE, tl), "blacklist gov");
        assertTrue(slashAppeal.hasRole(GOVERNANCE_ROLE, tl), "slashAppeal gov");
        assertTrue(registry.hasRole(GOVERNANCE_ROLE, tl), "registry gov");

        // Timelock holds DEFAULT_ADMIN_ROLE on every target (the meta-admin
        // that can re-grant any role). Without this, the deployer would still
        // be able to bypass the Timelock by re-granting itself GOVERNANCE_ROLE.
        assertTrue(router.hasRole(defaultAdmin, tl), "router admin");
        assertTrue(bond.hasRole(defaultAdmin, tl), "bond admin");
        assertTrue(blacklist.hasRole(defaultAdmin, tl), "blacklist admin");
        assertTrue(slashAppeal.hasRole(defaultAdmin, tl), "slashAppeal admin");
        assertTrue(registry.hasRole(defaultAdmin, tl), "registry admin");

        // Test contract must NOT retain either role on any target — closes the
        // "deployer keeps a back door" failure mode in #694's deploy script.
        assertFalse(router.hasRole(GOVERNANCE_ROLE, address(this)), "router gov back door");
        assertFalse(bond.hasRole(GOVERNANCE_ROLE, address(this)), "bond gov back door");
        assertFalse(blacklist.hasRole(GOVERNANCE_ROLE, address(this)), "blacklist gov back door");
        assertFalse(slashAppeal.hasRole(GOVERNANCE_ROLE, address(this)), "slashAppeal gov back door");
        assertFalse(registry.hasRole(GOVERNANCE_ROLE, address(this)), "registry gov back door");

        assertFalse(router.hasRole(defaultAdmin, address(this)), "router admin back door");
        assertFalse(bond.hasRole(defaultAdmin, address(this)), "bond admin back door");
        assertFalse(blacklist.hasRole(defaultAdmin, address(this)), "blacklist admin back door");
        assertFalse(slashAppeal.hasRole(defaultAdmin, address(this)), "slashAppeal admin back door");
        assertFalse(registry.hasRole(defaultAdmin, address(this)), "registry admin back door");

        assertFalse(timelock.hasRole(defaultAdmin, address(this)), "timelock admin back door");
    }

    function test_canary_executorIsTimelock() public view {
        // GovernorTimelockControl exposes `timelock()` — `_executor()` is
        // internal, but the public `timelock()` returns the same address.
        assertEq(gov.timelock(), address(timelock));
    }

    function test_canary_governorVoteWeightWired() public view {
        // Proposer's snapshot weight must be strictly greater than the live
        // `proposalThreshold` for `propose()` to succeed — this guards the
        // `_getVotes` → FeeRouter wiring + age-ramp bonding pipeline.
        uint256 weight = gov.getVotes(proposer, gov.clock() - 1);
        assertGt(weight, gov.proposalThreshold(), "weight below threshold");
        assertGt(gov.quorum(gov.clock() - 1), 0, "quorum read returned zero");
    }

    // -----------------------------------------------------------------
    // Lifecycle helpers
    // -----------------------------------------------------------------

    /// @dev Run propose → vote → queue → execute against `target` with
    ///      `data`. Returns the proposal id for any post-execute asserts.
    ///      Explicitly gates on `state() == Succeeded` after the voting period
    ///      so a vote-tally regression fails here with a readable message
    ///      instead of cascading into `GovernorUnexpectedProposalState` at
    ///      `queue()` (silent-failure hardening).
    function _runLifecycle(address target, bytes memory data) internal returns (uint256 proposalId) {
        (address[] memory targets, uint256[] memory values, bytes[] memory calldatas, string memory desc) =
            _proposalArgs(target, data);

        vm.prank(proposer);
        proposalId = gov.propose(targets, values, calldatas, desc);

        vm.warp(block.timestamp + gov.votingDelay() + 1);

        vm.prank(proposer);
        gov.castVote(proposalId, 1);
        vm.prank(voter1);
        gov.castVote(proposalId, 1);
        vm.prank(voter2);
        gov.castVote(proposalId, 1);

        vm.warp(block.timestamp + gov.votingPeriod() + 1);
        assertEq(uint256(gov.state(proposalId)), uint256(IGovernor.ProposalState.Succeeded), "vote did not pass");

        bytes32 descHash = keccak256(bytes(desc));
        gov.queue(targets, values, calldatas, descHash);

        vm.warp(block.timestamp + timelock.getMinDelay() + 1);

        gov.execute(targets, values, calldatas, descHash);
    }

    /// @dev Same shape as `_runLifecycle` but asserts `execute()` reverts with
    ///      `expectedRevert`. OZ's `TimelockController._execute` calls
    ///      `Address.verifyCallResult`, which bubbles the inner revert payload
    ///      verbatim when returndata is non-empty (custom errors always carry
    ///      returndata), so the asserted payload should be the target's bare
    ///      selector + args. The `state() == Succeeded` gate matches
    ///      `_runLifecycle` — a vote-tally regression fails here, not at the
    ///      `execute()` mismatch.
    function _runLifecycleExpectExecuteRevert(address target, bytes memory data, bytes memory expectedRevert) internal {
        (address[] memory targets, uint256[] memory values, bytes[] memory calldatas, string memory desc) =
            _proposalArgs(target, data);

        vm.prank(proposer);
        uint256 proposalId = gov.propose(targets, values, calldatas, desc);

        vm.warp(block.timestamp + gov.votingDelay() + 1);

        vm.prank(proposer);
        gov.castVote(proposalId, 1);
        vm.prank(voter1);
        gov.castVote(proposalId, 1);
        vm.prank(voter2);
        gov.castVote(proposalId, 1);

        vm.warp(block.timestamp + gov.votingPeriod() + 1);
        assertEq(uint256(gov.state(proposalId)), uint256(IGovernor.ProposalState.Succeeded), "vote did not pass");

        bytes32 descHash = keccak256(bytes(desc));
        gov.queue(targets, values, calldatas, descHash);

        vm.warp(block.timestamp + timelock.getMinDelay() + 1);

        vm.expectRevert(expectedRevert);
        gov.execute(targets, values, calldatas, descHash);
    }

    function _proposalArgs(address target, bytes memory data)
        internal
        returns (address[] memory targets, uint256[] memory values, bytes[] memory calldatas, string memory desc)
    {
        targets = new address[](1);
        targets[0] = target;
        values = new uint256[](1);
        values[0] = 0;
        calldatas = new bytes[](1);
        calldatas[0] = data;
        _propCounter++;
        desc = string(abi.encodePacked("lifecycle proposal #", vm.toString(_propCounter)));
    }

    // =================================================================
    // FeeRouter (6 setters)
    // =================================================================

    function test_lifecycle_FeeRouter_setShares_happy() public {
        uint256[3] memory newShares = [uint256(5000), uint256(3500), uint256(1500)];
        _runLifecycle(address(router), abi.encodeCall(FeeRouter.setShares, (newShares)));
        uint256[3] memory got = router.getShares();
        assertEq(got[0], 5000);
        assertEq(got[1], 3500);
        assertEq(got[2], 1500);
    }

    function test_lifecycle_FeeRouter_setShares_outOfBounds() public {
        // Sum != 10_000 → SharesDoNotSum
        uint256[3] memory bad = [uint256(5000), uint256(3000), uint256(1500)];
        _runLifecycleExpectExecuteRevert(
            address(router),
            abi.encodeCall(FeeRouter.setShares, (bad)),
            abi.encodeWithSelector(FeeRouter.SharesDoNotSum.selector, uint256(9500))
        );
    }

    function test_lifecycle_FeeRouter_setSharesAndDestinations_happy() public {
        uint256[3] memory newShares = [uint256(4500), uint256(4000), uint256(1500)];
        FeeRouter.ShareDestinations memory dests =
            FeeRouter.ShareDestinations({ buybackBurner: address(0xBEEF), treasury: address(0xFEED) });
        _runLifecycle(address(router), abi.encodeCall(FeeRouter.setSharesAndDestinations, (newShares, dests)));
        // Assert both destinations AND every bucket share landed — otherwise a
        // regression that silently skipped one of the inner setters could pass
        // with only a subset of fields checked.
        assertEq(router.buybackBurner(), address(0xBEEF));
        assertEq(router.treasury(), address(0xFEED));
        uint256[3] memory got = router.getShares();
        assertEq(got[0], 4500);
        assertEq(got[1], 4000);
        assertEq(got[2], 1500);
    }

    function test_lifecycle_FeeRouter_setSharesAndDestinations_outOfBounds() public {
        // Treasury == address(0) → ZeroAddress at the top of the setter.
        uint256[3] memory newShares = [uint256(4500), uint256(4000), uint256(1500)];
        FeeRouter.ShareDestinations memory dests =
            FeeRouter.ShareDestinations({ buybackBurner: address(0xBEEF), treasury: address(0) });
        _runLifecycleExpectExecuteRevert(
            address(router),
            abi.encodeCall(FeeRouter.setSharesAndDestinations, (newShares, dests)),
            abi.encodeWithSelector(FeeRouter.ZeroAddress.selector)
        );
    }

    function test_lifecycle_FeeRouter_setBuybackBurner_happy() public {
        address newAddr = address(0xBB02);
        _runLifecycle(address(router), abi.encodeCall(FeeRouter.setBuybackBurner, (newAddr)));
        assertEq(router.buybackBurner(), newAddr);
    }

    function test_lifecycle_FeeRouter_setBuybackBurner_outOfBounds() public {
        // Buyback share is 3000 bps (non-zero, from setUp), so clearing the
        // burner to address(0) must revert.
        _runLifecycleExpectExecuteRevert(
            address(router),
            abi.encodeCall(FeeRouter.setBuybackBurner, (address(0))),
            abi.encodeWithSelector(FeeRouter.NonZeroShareNeedsDestination.selector, uint256(1))
        );
    }

    function test_lifecycle_FeeRouter_setTreasury_happy() public {
        address newAddr = address(0xD8);
        _runLifecycle(address(router), abi.encodeCall(FeeRouter.setTreasury, (newAddr)));
        assertEq(router.treasury(), newAddr);
    }

    function test_lifecycle_FeeRouter_setTreasury_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(router),
            abi.encodeCall(FeeRouter.setTreasury, (address(0))),
            abi.encodeWithSelector(FeeRouter.ZeroAddress.selector)
        );
    }

    function test_lifecycle_FeeRouter_setWindowEpochs_happy() public {
        _runLifecycle(address(router), abi.encodeCall(FeeRouter.setWindowEpochs, (uint64(20))));
        assertEq(router.windowEpochs(), 20);
    }

    function test_lifecycle_FeeRouter_setWindowEpochs_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(router),
            abi.encodeCall(FeeRouter.setWindowEpochs, (uint64(27))),
            abi.encodeWithSelector(FeeRouter.WindowOutOfBounds.selector, uint64(27), uint64(4), uint64(26))
        );
        assertEq(router.windowEpochs(), WINDOW_EPOCHS);
    }

    // =================================================================
    // CapacityBond (8 setters)
    // =================================================================

    function test_lifecycle_CapacityBond_setMinBond_happy() public {
        _runLifecycle(address(bond), abi.encodeCall(CapacityBond.setMinBond, (75_000e18)));
        assertEq(bond.minBond(), 75_000e18);
    }

    function test_lifecycle_CapacityBond_setMinBond_outOfBounds() public {
        // Ceiling = 1_000_000e18; one over.
        uint256 bad = 1_000_001e18;
        _runLifecycleExpectExecuteRevert(
            address(bond),
            abi.encodeCall(CapacityBond.setMinBond, (bad)),
            abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, bad, 10_000e18, 1_000_000e18)
        );
    }

    function test_lifecycle_CapacityBond_setUnbondingPeriod_happy() public {
        _runLifecycle(address(bond), abi.encodeCall(CapacityBond.setUnbondingPeriod, (14 days)));
        assertEq(bond.unbondingPeriod(), 14 days);
    }

    function test_lifecycle_CapacityBond_setUnbondingPeriod_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(bond),
            abi.encodeCall(CapacityBond.setUnbondingPeriod, (61 days)),
            abi.encodeWithSelector(
                CapacityBond.ParamOutOfBounds.selector, uint256(61 days), uint256(7 days), uint256(60 days)
            )
        );
    }

    function test_lifecycle_CapacityBond_setMultiaddrUpdateCooldown_happy() public {
        _runLifecycle(address(bond), abi.encodeCall(CapacityBond.setMultiaddrUpdateCooldown, (1 hours)));
        assertEq(bond.multiaddrUpdateCooldown(), 1 hours);
    }

    function test_lifecycle_CapacityBond_setMultiaddrUpdateCooldown_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(bond),
            abi.encodeCall(CapacityBond.setMultiaddrUpdateCooldown, (uint256(2 days))),
            abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, uint256(2 days), uint256(0), uint256(1 days))
        );
    }

    function test_lifecycle_CapacityBond_setMaxMultiaddrSize_happy() public {
        _runLifecycle(address(bond), abi.encodeCall(CapacityBond.setMaxMultiaddrSize, (512)));
        assertEq(bond.maxMultiaddrSize(), 512);
    }

    function test_lifecycle_CapacityBond_setMaxMultiaddrSize_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(bond),
            abi.encodeCall(CapacityBond.setMaxMultiaddrSize, (uint256(32))),
            abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, uint256(32), uint256(64), uint256(1024))
        );
    }

    function test_lifecycle_CapacityBond_setRegionStabilityWindow_happy() public {
        _runLifecycle(address(bond), abi.encodeCall(CapacityBond.setRegionStabilityWindow, (14 days)));
        assertEq(bond.regionStabilityWindow(), 14 days);
    }

    function test_lifecycle_CapacityBond_setRegionStabilityWindow_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(bond),
            abi.encodeCall(CapacityBond.setRegionStabilityWindow, (uint256(31 days))),
            abi.encodeWithSelector(
                CapacityBond.ParamOutOfBounds.selector, uint256(31 days), uint256(3 days), uint256(30 days)
            )
        );
    }

    function test_lifecycle_CapacityBond_setMinCapacityMbps_happy() public {
        _runLifecycle(address(bond), abi.encodeCall(CapacityBond.setMinCapacityMbps, (uint256(100))));
        assertEq(bond.minCapacityMbps(), 100);
    }

    function test_lifecycle_CapacityBond_setMinCapacityMbps_outOfBounds() public {
        // Floor = 10, ceiling = 1000; one over.
        uint256 bad = 1001;
        _runLifecycleExpectExecuteRevert(
            address(bond),
            abi.encodeCall(CapacityBond.setMinCapacityMbps, (bad)),
            abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, bad, uint256(10), uint256(1000))
        );
    }

    function test_lifecycle_CapacityBond_setMaxCapacityMbps_happy() public {
        _runLifecycle(address(bond), abi.encodeCall(CapacityBond.setMaxCapacityMbps, (uint256(500_000))));
        assertEq(bond.maxCapacityMbps(), 500_000);
    }

    function test_lifecycle_CapacityBond_setMaxCapacityMbps_outOfBounds() public {
        // Floor = 50_000, ceiling = 1_000_000; one over.
        uint256 bad = 1_000_001;
        _runLifecycleExpectExecuteRevert(
            address(bond),
            abi.encodeCall(CapacityBond.setMaxCapacityMbps, (bad)),
            abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, bad, uint256(50_000), uint256(1_000_000))
        );
    }

    // =================================================================
    // DecdnGovernor self-params (2)
    // =================================================================

    function test_lifecycle_DecdnGovernor_setVoteCapBps_happy() public {
        _runLifecycle(address(gov), abi.encodeCall(DecdnGovernor.setVoteCapBps, (uint256(1500))));
        assertEq(gov.voteCapBps(), 1500);
    }

    function test_lifecycle_DecdnGovernor_setVoteCapBps_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(gov),
            abi.encodeCall(DecdnGovernor.setVoteCapBps, (uint256(2501))),
            abi.encodeWithSelector(DecdnGovernor.ParamOutOfBounds.selector, uint256(2501), uint256(100), uint256(2500))
        );
    }

    function test_lifecycle_DecdnGovernor_setAgeRampMonths_happy() public {
        _runLifecycle(address(gov), abi.encodeCall(DecdnGovernor.setAgeRampMonths, (uint256(12))));
        assertEq(gov.ageRampMonths(), 12);
    }

    function test_lifecycle_DecdnGovernor_setAgeRampMonths_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(gov),
            abi.encodeCall(DecdnGovernor.setAgeRampMonths, (uint256(25))),
            abi.encodeWithSelector(DecdnGovernor.ParamOutOfBounds.selector, uint256(25), uint256(1), uint256(24))
        );
    }

    // =================================================================
    // ContentBlacklist (7 setters)
    // =================================================================

    function test_lifecycle_ContentBlacklist_addHashGlobal_happy() public {
        bytes32 h = keccak256("bad-content");
        _runLifecycle(address(blacklist), abi.encodeCall(ContentBlacklist.addHashGlobal, (h, "DMCA-2026-001")));
        assertTrue(blacklist.isHashBlacklisted(h));
    }

    function test_lifecycle_ContentBlacklist_addHashGlobal_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(blacklist),
            abi.encodeCall(ContentBlacklist.addHashGlobal, (bytes32(0), "DMCA-2026-001")),
            abi.encodeWithSelector(ContentBlacklist.ZeroHash.selector)
        );
    }

    function test_lifecycle_ContentBlacklist_removeHashGlobal_happy() public {
        bytes32 h = keccak256("removable");
        _runLifecycle(address(blacklist), abi.encodeCall(ContentBlacklist.addHashGlobal, (h, "DMCA-2026-001")));
        _runLifecycle(address(blacklist), abi.encodeCall(ContentBlacklist.removeHashGlobal, (h)));
        assertFalse(blacklist.isHashBlacklisted(h));
    }

    function test_lifecycle_ContentBlacklist_removeHashGlobal_outOfBounds() public {
        // Removing a hash that was never added.
        bytes32 h = keccak256("never-added");
        _runLifecycleExpectExecuteRevert(
            address(blacklist),
            abi.encodeCall(ContentBlacklist.removeHashGlobal, (h)),
            abi.encodeWithSelector(ContentBlacklist.EntryNotBlacklisted.selector, bytes32("GLOBAL"), h)
        );
    }

    function test_lifecycle_ContentBlacklist_addOperator_happy() public {
        // ContentBlacklist.addOperator calls bond.ejectNode, which requires
        // BLACKLIST_ROLE on CapacityBond — granted in _wireCrossContractRoles.
        address op = address(0xDEAD01);
        _runLifecycle(address(blacklist), abi.encodeCall(ContentBlacklist.addOperator, (op)));
        assertTrue(blacklist.isOperatorBlacklisted(op));
    }

    function test_lifecycle_ContentBlacklist_addOperator_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(blacklist),
            abi.encodeCall(ContentBlacklist.addOperator, (address(0))),
            abi.encodeWithSelector(ContentBlacklist.ZeroAddress.selector)
        );
    }

    function test_lifecycle_ContentBlacklist_removeOperator_happy() public {
        address op = address(0xDEAD02);
        _runLifecycle(address(blacklist), abi.encodeCall(ContentBlacklist.addOperator, (op)));
        _runLifecycle(address(blacklist), abi.encodeCall(ContentBlacklist.removeOperator, (op)));
        assertFalse(blacklist.isOperatorBlacklisted(op));
    }

    // `removeOperator` has no revert path on its own (idempotent no-op when
    // the operator isn't currently blacklisted) — happy-path only.

    function test_lifecycle_ContentBlacklist_setOriginBlacklist_happy() public {
        address origin = address(0xFEED01);
        _runLifecycle(address(blacklist), abi.encodeCall(ContentBlacklist.setOriginBlacklist, (origin, true)));
        assertTrue(blacklist.isOriginBlacklisted(origin));
    }

    function test_lifecycle_ContentBlacklist_setOriginBlacklist_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(blacklist),
            abi.encodeCall(ContentBlacklist.setOriginBlacklist, (address(0), true)),
            abi.encodeWithSelector(ContentBlacklist.ZeroAddress.selector)
        );
    }

    function test_lifecycle_ContentBlacklist_registerRegionalBody_happy() public {
        address regionalBody = address(0xEEEE01);
        _runLifecycle(
            address(blacklist),
            abi.encodeCall(ContentBlacklist.registerRegionalBody, (REGION_DE, regionalBody, multisig))
        );
        assertTrue(blacklist.hasRole(blacklist.REGIONAL_BODY_ROLE(), regionalBody));
        assertEq(blacklist.getRegionalBody(REGION_DE).body, regionalBody);
        assertEq(blacklist.regionOfBody(regionalBody), REGION_DE);
    }

    function test_lifecycle_ContentBlacklist_registerRegionalBody_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(blacklist),
            abi.encodeCall(ContentBlacklist.registerRegionalBody, (REGION_DE, address(0), multisig)),
            abi.encodeWithSelector(ContentBlacklist.ZeroAddress.selector)
        );
    }

    // =================================================================
    // SlashAppeal
    // =================================================================

    function test_lifecycle_SlashAppeal_setAppealBond_happy() public {
        _runLifecycle(address(slashAppeal), abi.encodeCall(SlashAppeal.setAppealBond, (uint256(5000e18))));
        assertEq(slashAppeal.appealBond(), 5000e18);
    }

    function test_lifecycle_SlashAppeal_setAppealBond_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(slashAppeal),
            abi.encodeCall(SlashAppeal.setAppealBond, (uint256(50e18))),
            abi.encodeWithSelector(
                SlashAppeal.ParamOutOfBounds.selector, uint256(50e18), uint256(100e18), uint256(10_000e18)
            )
        );
    }

    // =================================================================
    // PublisherRegistry (2 setters)
    // =================================================================

    function test_lifecycle_PublisherRegistry_setMaxNamespacesPerPublisher_happy() public {
        _runLifecycle(address(registry), abi.encodeCall(PublisherRegistry.setMaxNamespacesPerPublisher, (uint256(500))));
        assertEq(registry.maxNamespacesPerPublisher(), 500);
    }

    function test_lifecycle_PublisherRegistry_setMaxNamespacesPerPublisher_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(registry),
            abi.encodeCall(PublisherRegistry.setMaxNamespacesPerPublisher, (uint256(1001))),
            abi.encodeWithSelector(
                PublisherRegistry.ParamOutOfBounds.selector, uint256(1001), uint256(1), uint256(1000)
            )
        );
    }

    function test_lifecycle_PublisherRegistry_setNamespaceTransferTimelock_happy() public {
        _runLifecycle(
            address(registry), abi.encodeCall(PublisherRegistry.setNamespaceTransferTimelock, (uint64(14 days)))
        );
        assertEq(registry.namespaceTransferTimelock(), 14 days);
    }

    function test_lifecycle_PublisherRegistry_setNamespaceTransferTimelock_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(registry),
            abi.encodeCall(PublisherRegistry.setNamespaceTransferTimelock, (uint64(31 days))),
            abi.encodeWithSelector(
                PublisherRegistry.ParamOutOfBounds.selector, uint256(31 days), uint256(1 days), uint256(30 days)
            )
        );
    }
}
