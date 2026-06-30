// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { SunsettingPausable } from "../src/SunsettingPausable.sol";

/// @dev Minimal concrete `SunsettingPausable` used to exercise the base in
///      isolation: a role-gated `pause()` that goes through the sunset guard.
contract SunsettingPausableHarness is AccessControl, SunsettingPausable {
    bytes32 public constant PAUSER_ROLE = keccak256("PAUSER_ROLE");

    constructor(address admin) {
        _grantRole(DEFAULT_ADMIN_ROLE, admin);
        _grantRole(PAUSER_ROLE, admin);
    }

    function pause() external onlyRole(PAUSER_ROLE) {
        _requirePauseWindowOpen();
        _pause();
    }

    function unpause() external onlyRole(PAUSER_ROLE) {
        _unpause();
    }
}

contract SunsettingPausableTest is Test {
    SunsettingPausableHarness internal harness;

    address internal admin = address(0xA11CE);
    // A second pauser standing in for a governance/timelock holder, to prove
    // the sunset is role-agnostic (ADR 009 Option A — even the DAO cannot
    // pause after the deadline).
    address internal governance = address(0x9ED);

    uint256 internal constant SUNSET_PERIOD = 365 days;

    uint256 internal deployTimestamp;

    function setUp() public {
        // Start from a realistic, non-zero timestamp so deadline arithmetic
        // is not masked by the default `block.timestamp == 1`.
        vm.warp(1_700_000_000);
        deployTimestamp = block.timestamp;
        harness = new SunsettingPausableHarness(admin);

        bytes32 pauserRole = harness.PAUSER_ROLE();
        vm.prank(admin);
        harness.grantRole(pauserRole, governance);
    }

    function test_pauseDeadline_isDeployTimePlusSunsetPeriod() public view {
        assertEq(harness.pauseDeadline(), deployTimestamp + SUNSET_PERIOD);
    }

    function test_pause_succeedsBeforeDeadline() public {
        vm.warp(harness.pauseDeadline() - 1);
        vm.prank(admin);
        harness.pause();
        assertTrue(harness.paused());
    }

    function test_pause_succeedsExactlyAtDeadline() public {
        vm.warp(harness.pauseDeadline());
        vm.prank(admin);
        harness.pause();
        assertTrue(harness.paused());
    }

    function test_pause_revertsAfterDeadline() public {
        vm.warp(harness.pauseDeadline() + 1);
        vm.prank(admin);
        vm.expectRevert(SunsettingPausable.PauseExpired.selector);
        harness.pause();
    }

    /// @dev Option A: the sunset removes the capability, not just one role's
    ///      access. A governance-class pauser is equally blocked after the
    ///      deadline.
    function test_pause_revertsAfterDeadline_forGovernanceCaller() public {
        vm.warp(harness.pauseDeadline() + 1);
        vm.prank(governance);
        vm.expectRevert(SunsettingPausable.PauseExpired.selector);
        harness.pause();
    }

    /// @dev Role enforcement is unchanged: a non-pauser still trips access
    ///      control before the sunset guard is reached.
    function test_pause_revertsWithoutRoleBeforeDeadline() public {
        address stranger = address(0xBAD);
        vm.expectRevert(
            abi.encodeWithSelector(
                IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, harness.PAUSER_ROLE()
            )
        );
        vm.prank(stranger);
        harness.pause();
    }
}
