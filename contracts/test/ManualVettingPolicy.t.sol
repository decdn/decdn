// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { ManualVettingPolicy } from "../src/ManualVettingPolicy.sol";
import { IVettingRequestable } from "../src/interfaces/IVettingRequestable.sol";

contract ManualVettingPolicyTest is Test {
    ManualVettingPolicy internal policy;

    address internal admin = address(0xA11CE);
    address internal vetter = address(0xF00D);
    address internal publisher = address(0xBEEF);
    address internal stranger = address(0x5747A);

    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 internal constant VETTER_ROLE = keccak256("VETTER_ROLE");

    function setUp() public {
        policy = new ManualVettingPolicy(admin, vetter);
    }

    function test_isVetted_falseByDefault() public view {
        assertFalse(policy.isVetted(publisher));
    }

    function test_setVetted_grantsAndEmits() public {
        vm.expectEmit(true, true, true, true);
        emit ManualVettingPolicy.PublisherVetted(publisher, true, vetter);
        vm.prank(vetter);
        policy.setVetted(publisher, true);
        assertTrue(policy.isVetted(publisher));
    }

    function test_setVetted_revokes() public {
        vm.prank(vetter);
        policy.setVetted(publisher, true);
        vm.prank(vetter);
        policy.setVetted(publisher, false);
        assertFalse(policy.isVetted(publisher));
    }

    function test_setVetted_onlyVetter() public {
        vm.prank(stranger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, VETTER_ROLE)
        );
        policy.setVetted(publisher, true);
    }

    /// GOVERNANCE_ROLE decides WHO may vet: it grants VETTER_ROLE to a new
    /// address, that address can then vet, and a revoke removes the power. This
    /// is the "move who does the vetting without swapping the contract" lever.
    function test_governanceGrantsAndRevokesVetter() public {
        address newVetter = address(0xC0FFEE);

        vm.prank(admin);
        policy.grantRole(VETTER_ROLE, newVetter);
        vm.prank(newVetter);
        policy.setVetted(publisher, true);
        assertTrue(policy.isVetted(publisher));

        vm.prank(admin);
        policy.revokeRole(VETTER_ROLE, newVetter);
        vm.prank(newVetter);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, newVetter, VETTER_ROLE)
        );
        policy.setVetted(stranger, true);
    }

    /// VETTER_ROLE's admin is GOVERNANCE_ROLE, not the default admin — so
    /// governance (the Timelock post-handoff) manages the vetter set.
    function test_vetterRoleAdminIsGovernance() public view {
        assertEq(policy.getRoleAdmin(VETTER_ROLE), GOVERNANCE_ROLE);
    }

    function test_constructor_seedsInitialVetter() public view {
        assertTrue(policy.hasRole(VETTER_ROLE, vetter));
        assertTrue(policy.hasRole(GOVERNANCE_ROLE, admin));
        assertTrue(policy.hasRole(policy.DEFAULT_ADMIN_ROLE(), admin));
    }

    function test_constructor_zeroInitialVetter_leavesRoleUnfilled() public {
        ManualVettingPolicy p = new ManualVettingPolicy(admin, address(0));
        assertFalse(p.hasRole(VETTER_ROLE, admin));
        // Governance can still seat a vetter afterwards.
        vm.prank(admin);
        p.grantRole(VETTER_ROLE, vetter);
        assertTrue(p.hasRole(VETTER_ROLE, vetter));
    }

    function test_constructor_rejectsZeroAdmin() public {
        vm.expectRevert(ManualVettingPolicy.ZeroAddress.selector);
        new ManualVettingPolicy(address(0), vetter);
    }

    function test_requestVetting_revertsUnsupported() public {
        vm.prank(publisher);
        vm.expectRevert(
            abi.encodeWithSelector(
                IVettingRequestable.VettingRequestUnsupported.selector,
                "vetting is granted by a VETTER_ROLE holder, not self-service"
            )
        );
        policy.requestVetting();
    }
}
