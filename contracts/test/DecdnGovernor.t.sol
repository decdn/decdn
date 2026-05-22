// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IGovernor } from "@openzeppelin/contracts/governance/IGovernor.sol";
import { TimelockController } from "@openzeppelin/contracts/governance/TimelockController.sol";

import { DecdnGovernor } from "../src/DecdnGovernor.sol";
import { MockVotingEscrow } from "./mocks/MockVotingEscrow.sol";

/// @notice Timelock-gated target governed by proposals. `setValue` is callable
///         only by the timelock, so only an executed governance proposal can
///         change `value`.
contract GovTarget {
    address public immutable timelock;
    uint256 public value;

    error NotTimelock();

    constructor(address timelock_) {
        timelock = timelock_;
    }

    function setValue(uint256 v) external {
        if (msg.sender != timelock) revert NotTimelock();
        value = v;
    }
}

contract DecdnGovernorTest is Test {
    MockVotingEscrow internal ve;
    TimelockController internal timelock;
    DecdnGovernor internal gov;
    GovTarget internal target;

    address internal proposer = makeAddr("proposer");
    address internal voterFor = makeAddr("voterFor");
    address internal voterAgainst = makeAddr("voterAgainst");

    uint256 internal constant SUPPLY = 1_000_000e18;
    uint256 internal constant TIMELOCK_DELAY = 48 hours;

    // GovernorCountingSimple vote types.
    uint8 internal constant AGAINST = 0;
    uint8 internal constant FOR = 1;
    uint8 internal constant ABSTAIN = 2;

    function setUp() public {
        ve = new MockVotingEscrow();

        address[] memory proposers = new address[](0);
        address[] memory executors = new address[](1); // executors[0] == address(0) => open execution
        timelock = new TimelockController(TIMELOCK_DELAY, proposers, executors, address(this));

        gov = new DecdnGovernor(ve, timelock);
        // The governor schedules onto the timelock, so it needs PROPOSER_ROLE.
        timelock.grantRole(timelock.PROPOSER_ROLE(), address(gov));
        timelock.grantRole(timelock.CANCELLER_ROLE(), address(gov));

        target = new GovTarget(address(timelock));
        ve.setSupply(SUPPLY);
    }

    // -----------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------

    function test_constructor_revertsOnZeroVotingEscrow() public {
        vm.expectRevert(DecdnGovernor.ZeroVotingEscrow.selector);
        new DecdnGovernor(MockVotingEscrow(address(0)), timelock);
    }

    function test_clockIsTimestampMode() public view {
        assertEq(gov.clock(), uint48(block.timestamp));
        assertEq(gov.CLOCK_MODE(), "mode=timestamp");
    }

    function test_votingScheduleConstants() public view {
        assertEq(gov.votingDelay(), 1 days);
        assertEq(gov.votingPeriod(), 7 days);
    }

    function test_quorumIs4PercentOfVeSupply() public view {
        assertEq(gov.quorum(block.timestamp), (SUPPLY * 4) / 100);
    }

    function test_proposalThresholdIsTenthPercentOfVeSupply() public view {
        assertEq(gov.proposalThreshold(), SUPPLY / 1000);
    }

    function test_proposalThresholdTracksSupply() public {
        ve.setSupply(2_000_000e18);
        assertEq(gov.proposalThreshold(), 2_000_000e18 / 1000);
        assertEq(gov.quorum(block.timestamp), (2_000_000e18 * 4) / 100);
    }

    function test_proposalThresholdUsesSnapshotSupply() public {
        // The threshold reads ve-supply at the proposer-vote snapshot
        // (clock() - 1), not current supply, so it stays consistent with the
        // proposer's measured weight as ve-supply decays.
        vm.warp(1_000_000);
        uint256 snapshot = block.timestamp - 1;
        ve.setSupplyAt(snapshot, 2_000_000e18); // supply at the snapshot
        ve.setSupply(1_000_000e18); // different "current" supply -> must be ignored
        assertEq(gov.proposalThreshold(), 2_000_000e18 / 1000);
    }

    function test_getVotesReadsVeBalance() public {
        ve.setBalance(voterFor, 1234e18);
        // timepoint must not be in the future for the real VE; mock ignores it.
        assertEq(gov.getVotes(voterFor, block.timestamp - 1), 1234e18);
    }

    function test_targetIsTimelockGated() public {
        vm.expectRevert(GovTarget.NotTimelock.selector);
        target.setValue(1);
    }

    // -----------------------------------------------------------------
    // Proposal lifecycle
    // -----------------------------------------------------------------

    function _proposeSetValue(uint256 v, string memory description)
        internal
        returns (
            uint256 proposalId,
            address[] memory targets,
            uint256[] memory values,
            bytes[] memory calldatas,
            bytes32 descriptionHash
        )
    {
        targets = new address[](1);
        targets[0] = address(target);
        values = new uint256[](1);
        calldatas = new bytes[](1);
        calldatas[0] = abi.encodeCall(GovTarget.setValue, (v));
        descriptionHash = keccak256(bytes(description));

        vm.prank(proposer);
        proposalId = gov.propose(targets, values, calldatas, description);
    }

    function test_fullLifecycle_passesQueuesExecutes() public {
        ve.setBalance(proposer, SUPPLY / 1000); // exactly the 0.1% threshold
        ve.setBalance(voterFor, (SUPPLY * 5) / 100); // 5% For > 4% quorum

        (
            uint256 id,
            address[] memory targets,
            uint256[] memory values,
            bytes[] memory calldatas,
            bytes32 descriptionHash
        ) = _proposeSetValue(42, "set value to 42");

        assertEq(uint256(gov.state(id)), uint256(IGovernor.ProposalState.Pending));

        vm.warp(block.timestamp + gov.votingDelay() + 1);
        assertEq(uint256(gov.state(id)), uint256(IGovernor.ProposalState.Active));

        vm.prank(voterFor);
        gov.castVote(id, FOR);

        vm.warp(block.timestamp + gov.votingPeriod() + 1);
        assertEq(uint256(gov.state(id)), uint256(IGovernor.ProposalState.Succeeded));

        gov.queue(targets, values, calldatas, descriptionHash);
        assertEq(uint256(gov.state(id)), uint256(IGovernor.ProposalState.Queued));

        // Cannot execute before the timelock delay elapses.
        vm.expectRevert();
        gov.execute(targets, values, calldatas, descriptionHash);

        vm.warp(block.timestamp + TIMELOCK_DELAY + 1);
        gov.execute(targets, values, calldatas, descriptionHash);

        assertEq(uint256(gov.state(id)), uint256(IGovernor.ProposalState.Executed));
        assertEq(target.value(), 42);
    }

    function test_propose_revertsBelowThreshold() public {
        ve.setBalance(proposer, SUPPLY / 1000 - 1); // one wei below 0.1%

        address[] memory targets = new address[](1);
        targets[0] = address(target);
        uint256[] memory values = new uint256[](1);
        bytes[] memory calldatas = new bytes[](1);
        calldatas[0] = abi.encodeCall(GovTarget.setValue, (1));

        vm.prank(proposer);
        vm.expectRevert(
            abi.encodeWithSelector(
                IGovernor.GovernorInsufficientProposerVotes.selector, proposer, SUPPLY / 1000 - 1, SUPPLY / 1000
            )
        );
        gov.propose(targets, values, calldatas, "below threshold");
    }

    function test_defeated_whenQuorumNotReached() public {
        ve.setBalance(proposer, SUPPLY / 1000);
        ve.setBalance(voterFor, (SUPPLY * 3) / 100); // 3% For < 4% quorum

        (uint256 id,,,,) = _proposeSetValue(7, "under quorum");
        vm.warp(block.timestamp + gov.votingDelay() + 1);

        vm.prank(voterFor);
        gov.castVote(id, FOR);

        vm.warp(block.timestamp + gov.votingPeriod() + 1);
        assertEq(uint256(gov.state(id)), uint256(IGovernor.ProposalState.Defeated));
    }

    function test_defeated_whenAgainstOutweighsFor() public {
        ve.setBalance(proposer, SUPPLY / 1000);
        ve.setBalance(voterFor, (SUPPLY * 5) / 100); // quorum reached (For counts toward quorum)
        ve.setBalance(voterAgainst, (SUPPLY * 6) / 100); // but Against > For

        (uint256 id,,,,) = _proposeSetValue(9, "against wins");
        vm.warp(block.timestamp + gov.votingDelay() + 1);

        vm.prank(voterFor);
        gov.castVote(id, FOR);
        vm.prank(voterAgainst);
        gov.castVote(id, AGAINST);

        vm.warp(block.timestamp + gov.votingPeriod() + 1);
        assertEq(uint256(gov.state(id)), uint256(IGovernor.ProposalState.Defeated));
    }

    function test_abstainCountsTowardQuorumButNotApproval() public {
        address abstainer = makeAddr("abstainer");
        ve.setBalance(proposer, SUPPLY / 1000);
        ve.setBalance(abstainer, (SUPPLY * 5) / 100); // 5% Abstain -> meets quorum
        ve.setBalance(voterFor, (SUPPLY * 1) / 100); // 1% For, 0 Against

        (uint256 id,,,,) = _proposeSetValue(11, "abstain quorum");
        vm.warp(block.timestamp + gov.votingDelay() + 1);

        vm.prank(abstainer);
        gov.castVote(id, ABSTAIN);
        vm.prank(voterFor);
        gov.castVote(id, FOR);

        vm.warp(block.timestamp + gov.votingPeriod() + 1);
        // Quorum (For + Abstain = 6% >= 4%) reached and For (1%) > Against (0) -> Succeeded.
        assertEq(uint256(gov.state(id)), uint256(IGovernor.ProposalState.Succeeded));
    }

    function test_cancelByProposer_beforeVoteStart() public {
        ve.setBalance(proposer, SUPPLY / 1000);
        (
            uint256 id,
            address[] memory targets,
            uint256[] memory values,
            bytes[] memory calldatas,
            bytes32 descriptionHash
        ) = _proposeSetValue(5, "to cancel");

        vm.prank(proposer);
        gov.cancel(targets, values, calldatas, descriptionHash);
        assertEq(uint256(gov.state(id)), uint256(IGovernor.ProposalState.Canceled));
    }
}
