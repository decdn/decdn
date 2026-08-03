// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Vm } from "forge-std/Vm.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { TimelockedGovernanceVettingPolicy } from "../src/TimelockedGovernanceVettingPolicy.sol";
import { IPublisherRegistryOwnership } from "../src/interfaces/IPublisherRegistryOwnership.sol";

contract MockPublisherRegistry is IPublisherRegistryOwnership {
    mapping(uint256 => address) internal _owner;
    mapping(address => uint256) internal _count;

    function setNamespaceCount(address publisher, uint256 n) external {
        _count[publisher] = n;
    }

    function ownerOf(uint256 ns) external view override returns (address) {
        return _owner[ns];
    }

    function namespaceCount(address publisher) external view override returns (uint256) {
        return _count[publisher];
    }
}

contract TimelockedGovernanceVettingPolicyTest is Test {
    MockPublisherRegistry internal registry;
    TimelockedGovernanceVettingPolicy internal policy;

    address internal admin = address(0xA11CE);
    address internal publisher = address(0xBEEF);
    address internal stranger = address(0x5747A);

    uint256 internal constant TIMELOCK = 3 days;
    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");

    function setUp() public {
        registry = new MockPublisherRegistry();
        policy = new TimelockedGovernanceVettingPolicy(registry, admin);
        registry.setNamespaceCount(publisher, 1);
        vm.warp(1_000_000);
    }

    function test_constructor_rejectsZeroRegistry() public {
        vm.expectRevert(TimelockedGovernanceVettingPolicy.ZeroAddress.selector);
        new TimelockedGovernanceVettingPolicy(IPublisherRegistryOwnership(address(0)), admin);
    }

    function test_constructor_rejectsZeroAdmin() public {
        vm.expectRevert(TimelockedGovernanceVettingPolicy.ZeroAddress.selector);
        new TimelockedGovernanceVettingPolicy(registry, address(0));
    }

    // ----- request / cancel -----

    function test_requestVetting_storesReadyAt() public {
        vm.prank(publisher);
        policy.requestVetting();
        assertEq(policy.getPendingVetting(publisher), block.timestamp + TIMELOCK);
        assertFalse(policy.isVetted(publisher), "a request alone does not vet");
    }

    function test_requestVetting_revertsWithoutANamespace() public {
        vm.prank(stranger);
        vm.expectRevert(abi.encodeWithSelector(TimelockedGovernanceVettingPolicy.NoNamespaceOwned.selector, stranger));
        policy.requestVetting();
    }

    function test_requestVetting_revertsWhileOneIsPending() public {
        vm.startPrank(publisher);
        policy.requestVetting();
        vm.expectRevert(
            abi.encodeWithSelector(TimelockedGovernanceVettingPolicy.VettingRequestPending.selector, publisher)
        );
        policy.requestVetting();
        vm.stopPrank();
    }

    function test_requestVetting_revertsWhenAlreadyVetted() public {
        _grantInstant(publisher);
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(TimelockedGovernanceVettingPolicy.AlreadyVetted.selector, publisher));
        policy.requestVetting();
    }

    function test_cancelVettingRequest_clearsPending() public {
        vm.startPrank(publisher);
        policy.requestVetting();
        policy.cancelVettingRequest();
        vm.stopPrank();
        assertEq(policy.getPendingVetting(publisher), 0);
    }

    function test_cancelVettingRequest_revertsWithoutOne() public {
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(TimelockedGovernanceVettingPolicy.NoVettingRequest.selector, publisher));
        policy.cancelVettingRequest();
    }

    // ----- grant -----

    function test_grantVetting_vetsAfterTimelockAndClearsPending() public {
        vm.prank(publisher);
        policy.requestVetting();
        vm.warp(block.timestamp + TIMELOCK);

        vm.prank(admin);
        policy.grantVetting(publisher);

        assertTrue(policy.isVetted(publisher));
        assertEq(policy.getPendingVetting(publisher), 0);
    }

    function test_grantVetting_revertsBeforeTimelock() public {
        vm.prank(publisher);
        policy.requestVetting();
        uint256 readyAt = block.timestamp + TIMELOCK;

        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(TimelockedGovernanceVettingPolicy.VettingTimelockNotElapsed.selector, readyAt)
        );
        policy.grantVetting(publisher);
    }

    function test_grantVetting_revertsWithoutRequest() public {
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(TimelockedGovernanceVettingPolicy.NoVettingRequest.selector, publisher));
        policy.grantVetting(publisher);
    }

    function test_grantVetting_onlyGovernance() public {
        vm.prank(publisher);
        policy.requestVetting();
        vm.warp(block.timestamp + TIMELOCK);

        vm.prank(stranger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        policy.grantVetting(publisher);
    }

    // ----- instant override -----

    function test_setPublisherVetted_grantsInstantlyAndConsumesPending() public {
        vm.prank(publisher);
        policy.requestVetting();

        _grantInstant(publisher);

        assertTrue(policy.isVetted(publisher));
        assertEq(policy.getPendingVetting(publisher), 0);
    }

    /// Un-vetting must also clear a pending request — otherwise a rogue
    /// publisher's already-ripened request would walk it straight back in.
    function test_setPublisherVetted_unvetClearsRipenedPendingRequest() public {
        vm.prank(publisher);
        policy.requestVetting();
        vm.warp(block.timestamp + TIMELOCK + 1);

        vm.prank(admin);
        policy.setPublisherVetted(publisher, false);

        assertFalse(policy.isVetted(publisher));
        assertEq(policy.getPendingVetting(publisher), 0);

        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(TimelockedGovernanceVettingPolicy.NoVettingRequest.selector, publisher));
        policy.grantVetting(publisher);
    }

    function test_setPublisherVetted_rejectsZeroAddress() public {
        vm.prank(admin);
        vm.expectRevert(TimelockedGovernanceVettingPolicy.ZeroAddress.selector);
        policy.setPublisherVetted(address(0), true);
    }

    function test_setPublisherVetted_onlyGovernance() public {
        vm.prank(stranger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        policy.setPublisherVetted(publisher, true);
    }

    function test_setPublisherVetted_emitsDirectionAndCaller() public {
        vm.expectEmit(true, true, true, true);
        emit TimelockedGovernanceVettingPolicy.PublisherVetted(publisher, true, admin);
        vm.prank(admin);
        policy.setPublisherVetted(publisher, true);

        vm.expectEmit(true, true, true, true);
        emit TimelockedGovernanceVettingPolicy.PublisherVetted(publisher, false, admin);
        vm.prank(admin);
        policy.setPublisherVetted(publisher, false);
    }

    /// Governance clearing a pending request must not announce it as a
    /// *cancellation* — that word is reserved for the publisher withdrawing its own.
    function test_setPublisherVetted_emitsNoCancellation() public {
        vm.recordLogs();
        _grantInstant(publisher);
        Vm.Log[] memory logs = vm.getRecordedLogs();
        for (uint256 i = 0; i < logs.length; ++i) {
            assertTrue(
                logs[i].topics[0] != TimelockedGovernanceVettingPolicy.VettingRequestCancelled.selector,
                "governance must not emit VettingRequestCancelled"
            );
        }
    }

    // ----- timelock param -----

    function test_setVettingTimelock_enforcesBounds() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                TimelockedGovernanceVettingPolicy.ParamOutOfBounds.selector,
                uint256(1 hours),
                uint256(24 hours),
                uint256(14 days)
            )
        );
        policy.setVettingTimelock(1 hours);
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                TimelockedGovernanceVettingPolicy.ParamOutOfBounds.selector,
                uint256(15 days),
                uint256(24 hours),
                uint256(14 days)
            )
        );
        policy.setVettingTimelock(15 days);
        vm.prank(admin);
        policy.setVettingTimelock(7 days);
        assertEq(policy.vettingTimelock(), 7 days);
    }

    function test_setVettingTimelock_emitsOldThenNew() public {
        vm.expectEmit(true, true, true, true);
        emit TimelockedGovernanceVettingPolicy.VettingTimelockUpdated(TIMELOCK, 7 days);
        vm.prank(admin);
        policy.setVettingTimelock(7 days);
    }

    function _grantInstant(address p) internal {
        vm.prank(admin);
        policy.setPublisherVetted(p, true);
    }
}
