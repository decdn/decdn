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
import { SafetyReserve } from "../src/SafetyReserve.sol";
import { PublisherRegistry } from "../src/PublisherRegistry.sol";
import { Token } from "../src/Token.sol";
import { ICapacityBond } from "../src/interfaces/ICapacityBond.sol";
import { ISafetyReserve } from "../src/interfaces/ISafetyReserve.sol";

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
///         `SafetyReserve`, `PublisherRegistry`, `DecdnGovernor`, and
///         `TimelockController`, hands off `GOVERNANCE_ROLE` on every target
///         to the Timelock to mirror the production deploy ordering (issue
///         #694 supplies the deploy script), and asserts that each setter's
///         happy and out-of-bounds path closes through the Governor → Timelock
///         → target wiring.
///
/// @dev    Seeding strategy for proposal-threshold (0.1%) + quorum (4%):
///         three test operators (`proposer`, `voter1`, `voter2`) stake at T0,
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

    uint256 internal constant MIN_STAKE = 50_000e18;
    uint256 internal constant UNBONDING_PERIOD = 7 days;
    uint256 internal constant SAFETY_APPEAL_BOND = 1000e18;
    uint256 internal constant MAX_RESTITUTION = 100_000e6;
    uint256 internal constant BLACKLIST_APPEAL_BOND = 100e18;

    // -----------------------------------------------------------------
    // Deployments
    // -----------------------------------------------------------------

    LifecycleUSDC internal usdc;
    Token internal token;
    MockEd25519Verifier internal ed25519;
    CapacityBond internal bond;
    FeeRouter internal router;
    SafetyReserve internal reserve;
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
    address internal treasuryEoa = address(0xD7);
    address internal multisig = address(0xC0DE);
    address internal challengerPool = address(0xCCEE);

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
            treasury: treasuryEoa,
            emergencyMultisig: multisig,
            // Token is minted entirely to the test harness so it can fund
            // operators + FeeRouter bucket destinations downstream.
            initialTokenHolder: address(this),
            challengerIncentivePool: challengerPool,
            timelockDelay: TIMELOCK_DELAY,
            minStake: MIN_STAKE,
            unbondingPeriod: UNBONDING_PERIOD,
            multiaddrUpdateCooldown: 0,
            maxMultiaddrSize: 1024,
            regionStabilityWindow: 7 days,
            genesisCreditWindow: 30 days,
            feeRouterEpochLength: EPOCH,
            feeRouterWindowEpochs: WINDOW_EPOCHS,
            // Steady-state shares (60/25/10/5 from ADR 026 § FeeRouter split)
            // so the setter / topology exercise covers the production-activated
            // state, not the launch dormancy that DeployProtocol.s.sol ships.
            feeRouterShares: [uint256(6000), uint256(2500), uint256(1000), uint256(500)],
            buybackBurner: address(0xBB),
            safetyAppealBond: SAFETY_APPEAL_BOND,
            maxAppealRestitution: MAX_RESTITUTION,
            blacklistAppealBond: BLACKLIST_APPEAL_BOND
        });

        Deployment memory d = _runFullDeploy(cfg);
        token = d.token;
        bond = d.bond;
        reserve = d.reserve;
        router = d.router;
        blacklist = d.blacklist;
        registry = d.registry;
        timelock = d.timelock;
        gov = d.governor;

        _stakeOperatorsAndSeedVoteWeight();
    }

    /// @dev Override the base hook to grant the test harness
    ///      `ROUTER_CALLER_ROLE` on FeeRouter so it can call `routeSettlement`
    ///      to seed served-bytes for vote-weight tests. In production this
    ///      role lives on `PaymentChannel` (not yet deployed). The hook runs
    ///      in phase 3 — before the GOVERNANCE_ROLE handoff, which would
    ///      otherwise put `grantRole` behind the 48h Timelock.
    function _postWiringHook(DeployConfig memory, Deployment memory dDeploy) internal override {
        dDeploy.router.grantRole(dDeploy.router.ROUTER_CALLER_ROLE(), address(this));
    }

    function _stakeOperatorsAndSeedVoteWeight() internal {
        address[3] memory ops = [proposer, voter1, voter2];

        // Fund each operator with TOKEN, approve CapacityBond, and stake.
        for (uint256 i = 0; i < ops.length; i++) {
            token.transfer(ops[i], MIN_STAKE * 10);
            vm.prank(ops[i]);
            token.approve(address(bond), type(uint256).max);
            vm.prank(ops[i]);
            bond.stake(MIN_STAKE);
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
        assertTrue(reserve.hasRole(GOVERNANCE_ROLE, tl), "reserve gov");
        assertTrue(registry.hasRole(GOVERNANCE_ROLE, tl), "registry gov");

        // Timelock holds DEFAULT_ADMIN_ROLE on every target (the meta-admin
        // that can re-grant any role). Without this, the deployer would still
        // be able to bypass the Timelock by re-granting itself GOVERNANCE_ROLE.
        assertTrue(router.hasRole(defaultAdmin, tl), "router admin");
        assertTrue(bond.hasRole(defaultAdmin, tl), "bond admin");
        assertTrue(blacklist.hasRole(defaultAdmin, tl), "blacklist admin");
        assertTrue(reserve.hasRole(defaultAdmin, tl), "reserve admin");
        assertTrue(registry.hasRole(defaultAdmin, tl), "registry admin");

        // Test contract must NOT retain either role on any target — closes the
        // "deployer keeps a back door" failure mode in #694's deploy script.
        assertFalse(router.hasRole(GOVERNANCE_ROLE, address(this)), "router gov back door");
        assertFalse(bond.hasRole(GOVERNANCE_ROLE, address(this)), "bond gov back door");
        assertFalse(blacklist.hasRole(GOVERNANCE_ROLE, address(this)), "blacklist gov back door");
        assertFalse(reserve.hasRole(GOVERNANCE_ROLE, address(this)), "reserve gov back door");
        assertFalse(registry.hasRole(GOVERNANCE_ROLE, address(this)), "registry gov back door");

        assertFalse(router.hasRole(defaultAdmin, address(this)), "router admin back door");
        assertFalse(bond.hasRole(defaultAdmin, address(this)), "bond admin back door");
        assertFalse(blacklist.hasRole(defaultAdmin, address(this)), "blacklist admin back door");
        assertFalse(reserve.hasRole(defaultAdmin, address(this)), "reserve admin back door");
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
        // `_getVotes` → FeeRouter wiring + age-ramp staking pipeline.
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
        uint256[4] memory newShares = [uint256(5000), uint256(2500), uint256(1500), uint256(1000)];
        _runLifecycle(address(router), abi.encodeCall(FeeRouter.setShares, (newShares)));
        uint256[4] memory got = router.getShares();
        assertEq(got[0], 5000);
        assertEq(got[1], 2500);
        assertEq(got[2], 1500);
        assertEq(got[3], 1000);
    }

    function test_lifecycle_FeeRouter_setShares_outOfBounds() public {
        // Sum != 10_000 → SharesDoNotSum
        uint256[4] memory bad = [uint256(5000), uint256(2500), uint256(1500), uint256(500)];
        _runLifecycleExpectExecuteRevert(
            address(router),
            abi.encodeCall(FeeRouter.setShares, (bad)),
            abi.encodeWithSelector(FeeRouter.SharesDoNotSum.selector, uint256(9500))
        );
    }

    function test_lifecycle_FeeRouter_setSharesAndDestinations_happy() public {
        uint256[4] memory newShares = [uint256(4500), uint256(3000), uint256(1500), uint256(1000)];
        FeeRouter.ShareDestinations memory dests = FeeRouter.ShareDestinations({
            safetyReserve: address(0xBA5A), buybackBurner: address(0xBEEF), treasury: address(0xFEED)
        });
        _runLifecycle(address(router), abi.encodeCall(FeeRouter.setSharesAndDestinations, (newShares, dests)));
        // Assert all three destinations AND every bucket share landed —
        // otherwise a regression that silently skipped one of the four inner
        // setters could pass with only a subset of fields checked.
        assertEq(router.safetyReserve(), address(0xBA5A));
        assertEq(router.buybackBurner(), address(0xBEEF));
        assertEq(router.treasury(), address(0xFEED));
        uint256[4] memory got = router.getShares();
        assertEq(got[0], 4500);
        assertEq(got[1], 3000);
        assertEq(got[2], 1500);
        assertEq(got[3], 1000);
    }

    function test_lifecycle_FeeRouter_setSharesAndDestinations_outOfBounds() public {
        // Treasury == address(0) → ZeroAddress at the top of the setter.
        uint256[4] memory newShares = [uint256(4500), uint256(3000), uint256(1500), uint256(1000)];
        FeeRouter.ShareDestinations memory dests = FeeRouter.ShareDestinations({
            safetyReserve: address(reserve), buybackBurner: address(0xBEEF), treasury: address(0)
        });
        _runLifecycleExpectExecuteRevert(
            address(router),
            abi.encodeCall(FeeRouter.setSharesAndDestinations, (newShares, dests)),
            abi.encodeWithSelector(FeeRouter.ZeroAddress.selector)
        );
    }

    function test_lifecycle_FeeRouter_setSafetyReserve_happy() public {
        address newAddr = address(0x5A5A);
        _runLifecycle(address(router), abi.encodeCall(FeeRouter.setSafetyReserve, (newAddr)));
        assertEq(router.safetyReserve(), newAddr);
    }

    function test_lifecycle_FeeRouter_setSafetyReserve_outOfBounds() public {
        // Default safety share is 500 (non-zero), so address(0) must revert.
        _runLifecycleExpectExecuteRevert(
            address(router),
            abi.encodeCall(FeeRouter.setSafetyReserve, (address(0))),
            abi.encodeWithSelector(FeeRouter.NonZeroShareNeedsDestination.selector, uint256(3))
        );
    }

    function test_lifecycle_FeeRouter_setBuybackBurner_happy() public {
        address newAddr = address(0xBB02);
        _runLifecycle(address(router), abi.encodeCall(FeeRouter.setBuybackBurner, (newAddr)));
        assertEq(router.buybackBurner(), newAddr);
    }

    function test_lifecycle_FeeRouter_setBuybackBurner_outOfBounds() public {
        // Default buyback share is 2500 (non-zero), so address(0) must revert.
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

    function test_lifecycle_CapacityBond_setMinStake_happy() public {
        _runLifecycle(address(bond), abi.encodeCall(CapacityBond.setMinStake, (75_000e18)));
        assertEq(bond.minStake(), 75_000e18);
    }

    function test_lifecycle_CapacityBond_setMinStake_outOfBounds() public {
        // Ceiling = 1_000_000e18; one over.
        uint256 bad = 1_000_001e18;
        _runLifecycleExpectExecuteRevert(
            address(bond),
            abi.encodeCall(CapacityBond.setMinStake, (bad)),
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
            abi.encodeCall(CapacityBond.setUnbondingPeriod, (31 days)),
            abi.encodeWithSelector(
                CapacityBond.ParamOutOfBounds.selector, uint256(31 days), uint256(3 days), uint256(30 days)
            )
        );
    }

    function test_lifecycle_CapacityBond_setSafetyReserve_happy() public {
        // Deploy a stand-in SafetyReserve so we can rewire (the contract
        // requires the new address be non-zero).
        SafetyReserve newReserve = new SafetyReserve({
            usdc_: usdc,
            token_: token,
            capacityBond_: ICapacityBond(address(bond)),
            admin: address(this),
            emergencyMultisig: multisig,
            appealBond_: SAFETY_APPEAL_BOND,
            maxAppealRestitution_: MAX_RESTITUTION
        });
        _runLifecycle(
            address(bond), abi.encodeCall(CapacityBond.setSafetyReserve, (ISafetyReserve(address(newReserve))))
        );
        assertEq(address(bond.safetyReserve()), address(newReserve));
    }

    function test_lifecycle_CapacityBond_setSafetyReserve_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(bond),
            abi.encodeCall(CapacityBond.setSafetyReserve, (ISafetyReserve(address(0)))),
            abi.encodeWithSelector(CapacityBond.ZeroAddress.selector)
        );
    }

    function test_lifecycle_CapacityBond_setTreasury_happy() public {
        // CapacityBond.setTreasury allows address(0); pick a non-zero value.
        address newTreasury = address(0xD9);
        _runLifecycle(address(bond), abi.encodeCall(CapacityBond.setTreasury, (newTreasury)));
        assertEq(bond.treasury(), newTreasury);
    }

    // No `_outOfBounds` test for `setTreasury` — the contract intentionally
    // accepts `address(0)` as a "burn unvested credit on forfeit" sentinel.

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

    function test_lifecycle_CapacityBond_setClaimSlashGateEpochs_happy() public {
        _runLifecycle(address(bond), abi.encodeCall(CapacityBond.setClaimSlashGateEpochs, (uint64(20))));
        assertEq(bond.claimSlashGateEpochs(), 20);
    }

    function test_lifecycle_CapacityBond_setClaimSlashGateEpochs_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(bond),
            abi.encodeCall(CapacityBond.setClaimSlashGateEpochs, (uint64(27))),
            abi.encodeWithSelector(CapacityBond.ParamOutOfBounds.selector, uint256(27), uint256(4), uint256(26))
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
        _runLifecycle(address(blacklist), abi.encodeCall(ContentBlacklist.addHashGlobal, (h)));
        assertTrue(blacklist.isHashBlacklisted(h));
    }

    function test_lifecycle_ContentBlacklist_addHashGlobal_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(blacklist),
            abi.encodeCall(ContentBlacklist.addHashGlobal, (bytes32(0))),
            abi.encodeWithSelector(ContentBlacklist.ZeroHash.selector)
        );
    }

    function test_lifecycle_ContentBlacklist_removeHashGlobal_happy() public {
        bytes32 h = keccak256("removable");
        _runLifecycle(address(blacklist), abi.encodeCall(ContentBlacklist.addHashGlobal, (h)));
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

    function test_lifecycle_ContentBlacklist_setAppealBond_happy() public {
        _runLifecycle(address(blacklist), abi.encodeCall(ContentBlacklist.setAppealBond, (uint256(2000e18))));
        assertEq(blacklist.appealBond(), 2000e18);
    }

    function test_lifecycle_ContentBlacklist_setAppealBond_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(blacklist),
            abi.encodeCall(ContentBlacklist.setAppealBond, (uint256(10_000e18))),
            abi.encodeWithSelector(
                ContentBlacklist.ParamOutOfBounds.selector, uint256(10_000e18), uint256(50e18), uint256(5000e18)
            )
        );
    }

    function test_lifecycle_ContentBlacklist_registerRegionalBody_happy() public {
        address regionalBody = address(0xEEEE01);
        _runLifecycle(address(blacklist), abi.encodeCall(ContentBlacklist.registerRegionalBody, (regionalBody)));
        assertTrue(blacklist.hasRole(blacklist.REGIONAL_BODY_ROLE(), regionalBody));
    }

    // `registerRegionalBody` does not validate its argument (OZ AccessControl
    // permits granting a role to `address(0)` — a no-op for permissioning),
    // so there's no revert path to exercise.

    // =================================================================
    // SafetyReserve (4 setters)
    // =================================================================

    function test_lifecycle_SafetyReserve_setBalancerPool_happy() public {
        address pool = address(0xBA1A);
        _runLifecycle(address(reserve), abi.encodeCall(SafetyReserve.setBalancerPool, (pool)));
        assertEq(reserve.balancerPool(), pool);
    }

    function test_lifecycle_SafetyReserve_setBalancerPool_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(reserve),
            abi.encodeCall(SafetyReserve.setBalancerPool, (address(0))),
            abi.encodeWithSelector(SafetyReserve.ZeroAddress.selector)
        );
    }

    function test_lifecycle_SafetyReserve_setChallengerIncentivePool_happy() public {
        address pool = address(0xCC02);
        _runLifecycle(address(reserve), abi.encodeCall(SafetyReserve.setChallengerIncentivePool, (pool)));
        assertEq(reserve.challengerIncentivePool(), pool);
    }

    function test_lifecycle_SafetyReserve_setChallengerIncentivePool_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(reserve),
            abi.encodeCall(SafetyReserve.setChallengerIncentivePool, (address(0))),
            abi.encodeWithSelector(SafetyReserve.ZeroAddress.selector)
        );
    }

    function test_lifecycle_SafetyReserve_setAppealBond_happy() public {
        _runLifecycle(address(reserve), abi.encodeCall(SafetyReserve.setAppealBond, (uint256(5000e18))));
        assertEq(reserve.appealBond(), 5000e18);
    }

    function test_lifecycle_SafetyReserve_setAppealBond_outOfBounds() public {
        _runLifecycleExpectExecuteRevert(
            address(reserve),
            abi.encodeCall(SafetyReserve.setAppealBond, (uint256(50e18))),
            abi.encodeWithSelector(
                SafetyReserve.ParamOutOfBounds.selector, uint256(50e18), uint256(100e18), uint256(10_000e18)
            )
        );
    }

    function test_lifecycle_SafetyReserve_setMaxAppealRestitution_happy() public {
        _runLifecycle(address(reserve), abi.encodeCall(SafetyReserve.setMaxAppealRestitution, (uint256(200_000e6))));
        assertEq(reserve.maxAppealRestitution(), 200_000e6);
    }

    // `setMaxAppealRestitution` is intentionally unbounded — governance sets
    // the cap freely. No revert path.

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
