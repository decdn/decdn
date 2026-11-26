// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { AccessControl } from "@openzeppelin/contracts/access/AccessControl.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";
import { Pausable } from "@openzeppelin/contracts/utils/Pausable.sol";

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

    /// @dev A `whenNotPaused` entrypoint standing in for the gated protocol
    ///      calls (`deposit`, `openPool`, `topUp`, …).
    function gated() external view whenNotPaused returns (bool) {
        return true;
    }

    /// @dev A `whenPaused` entrypoint, to pin the inverse gate.
    function gatedWhilePaused() external view whenPaused returns (bool) {
        return true;
    }
}

contract SunsettingPausableTest is Test {
    SunsettingPausableHarness internal harness;

    address internal admin = address(0xA11CE);
    // A second pauser standing in for a governance/timelock holder, to prove
    // the sunset is role-agnostic (ADR 009 § Emergency Multisig — even the DAO
    // cannot pause after the deadline).
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

    /// @dev The sunset removes the capability, not just one role's access:
    ///      a governance-class pauser is equally blocked after the deadline
    ///      (the no-role-carve-out decision).
    function test_pause_revertsAfterDeadline_forGovernanceCaller() public {
        vm.warp(harness.pauseDeadline() + 1);
        vm.prank(governance);
        vm.expectRevert(SunsettingPausable.PauseExpired.selector);
        harness.pause();
    }

    /// @dev A pause in force at the deadline still holds at the deadline.
    function test_pauseInForce_holdsAtDeadline() public {
        vm.warp(harness.pauseDeadline() - 1);
        vm.prank(admin);
        harness.pause();

        vm.warp(harness.pauseDeadline());
        assertTrue(harness.paused());
        vm.expectRevert(Pausable.EnforcedPause.selector);
        harness.gated();
        assertTrue(harness.gatedWhilePaused());
    }

    /// @dev A pause in force ends at the sunset: `paused()` reads false and
    ///      every `whenNotPaused` gate opens, with no `unpause()` call.
    function test_pauseInForce_endsAfterDeadline() public {
        vm.warp(harness.pauseDeadline() - 1);
        vm.prank(admin);
        harness.pause();
        assertTrue(harness.paused());
        vm.expectRevert(Pausable.EnforcedPause.selector);
        harness.gated();

        vm.warp(harness.pauseDeadline() + 1);
        assertFalse(harness.paused());
        assertTrue(harness.gated());
        vm.expectRevert(Pausable.ExpectedPause.selector);
        harness.gatedWhilePaused();
    }

    /// @dev Past the sunset the contract already reads unpaused, so
    ///      `unpause()` reverts `ExpectedPause` and leaves it unpaused.
    function test_unpause_afterDeadline_revertsAndStaysUnpaused() public {
        vm.warp(harness.pauseDeadline() - 1);
        vm.prank(admin);
        harness.pause();

        vm.warp(harness.pauseDeadline() + 1);
        vm.prank(admin);
        vm.expectRevert(Pausable.ExpectedPause.selector);
        harness.unpause();
        assertFalse(harness.paused());
        assertTrue(harness.gated());
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
