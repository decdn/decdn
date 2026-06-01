// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { OriginAssignment } from "../src/OriginAssignment.sol";
import { ICapacityBondActivity } from "../src/interfaces/ICapacityBondActivity.sol";
import { IPublisherRegistryOwnership } from "../src/interfaces/IPublisherRegistryOwnership.sol";
import { IContentBlacklistOriginView } from "../src/interfaces/IContentBlacklistOriginView.sol";

contract MockBondActivity is ICapacityBondActivity {
    mapping(address => bool) internal _active;

    function setActive(address op, bool a) external {
        _active[op] = a;
    }

    function isActive(address op) external view override returns (bool) {
        return _active[op];
    }
}

contract MockPublisherRegistry is IPublisherRegistryOwnership {
    mapping(uint256 => address) internal _owner;

    function setOwner(uint256 ns, address o) external {
        _owner[ns] = o;
    }

    function ownerOf(uint256 ns) external view override returns (address) {
        return _owner[ns];
    }
}

contract MockBlacklistOrigin is IContentBlacklistOriginView {
    mapping(address => bool) internal _bl;
    mapping(address => bool) internal _opBl;

    function setBlacklisted(address op, bool b) external {
        _bl[op] = b;
    }

    function setOperatorBlacklisted(address op, bool b) external {
        _opBl[op] = b;
    }

    function isOriginBlacklisted(address op) external view override returns (bool) {
        return _bl[op];
    }

    function isOperatorBlacklisted(address op) external view override returns (bool) {
        return _opBl[op];
    }
}

contract OriginAssignmentTest is Test {
    MockBondActivity internal bond;
    MockPublisherRegistry internal registry;
    MockBlacklistOrigin internal blacklist;
    OriginAssignment internal oa;

    address internal admin = address(0xA11CE);
    address internal publisher = address(0xBEEF);
    address internal stranger = address(0x5747A);
    address internal opA = address(0xA);
    address internal opB = address(0xB);
    address internal opC = address(0xC);

    uint256 internal constant NS = 1;
    uint256 internal constant TIMELOCK = 3 days;
    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");

    function setUp() public {
        bond = new MockBondActivity();
        registry = new MockPublisherRegistry();
        blacklist = new MockBlacklistOrigin();

        oa = new OriginAssignment(bond, registry, address(blacklist), admin);

        registry.setOwner(NS, publisher);
        bond.setActive(opA, true);
        bond.setActive(opB, true);
        bond.setActive(opC, true);

        vm.warp(1_000_000);
    }

    function _ops2() internal view returns (address[] memory a) {
        a = new address[](2);
        a[0] = opA;
        a[1] = opB;
    }

    function _propose(address[] memory ops) internal {
        vm.prank(publisher);
        oa.proposeAssignment(NS, ops);
    }

    // -----------------------------------------------------------------
    // proposeAssignment
    // -----------------------------------------------------------------

    function test_propose_storesPending() public {
        _propose(_ops2());
        (address[] memory ops, uint256 readyAt) = oa.getPendingAssignment(NS);
        assertEq(ops.length, 2);
        assertEq(ops[0], opA);
        assertEq(readyAt, block.timestamp + TIMELOCK);
    }

    function test_propose_revertsNonOwner() public {
        vm.prank(stranger);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.NotNamespaceOwner.selector, NS, stranger));
        oa.proposeAssignment(NS, _ops2());
    }

    function test_propose_revertsEmpty() public {
        address[] memory empty = new address[](0);
        vm.prank(publisher);
        vm.expectRevert(OriginAssignment.EmptyOperatorSet.selector);
        oa.proposeAssignment(NS, empty);
    }

    function test_propose_revertsDuplicate() public {
        address[] memory dup = new address[](2);
        dup[0] = opA;
        dup[1] = opA;
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.DuplicateOperator.selector, opA));
        oa.proposeAssignment(NS, dup);
    }

    function test_propose_revertsInactiveOperator() public {
        bond.setActive(opB, false);
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.OperatorNotActive.selector, opB));
        oa.proposeAssignment(NS, _ops2());
    }

    function test_propose_revertsTooMany() public {
        vm.prank(admin);
        oa.setMaxOriginsPerNamespace(1);
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.TooManyOrigins.selector, uint256(2), uint256(1)));
        oa.proposeAssignment(NS, _ops2());
    }

    function test_propose_overwriteReplacesPending() public {
        _propose(_ops2());
        address[] memory one = new address[](1);
        one[0] = opC;
        vm.prank(publisher);
        oa.proposeAssignment(NS, one);
        (address[] memory ops,) = oa.getPendingAssignment(NS);
        assertEq(ops.length, 1);
        assertEq(ops[0], opC);
    }

    // -----------------------------------------------------------------
    // activateAssignment
    // -----------------------------------------------------------------

    function test_activate_setsAuthorizedSet() public {
        _propose(_ops2());
        vm.warp(block.timestamp + TIMELOCK);
        vm.prank(admin);
        oa.activateAssignment(NS);

        assertTrue(oa.isAuthorizedOrigin(NS, opA));
        assertTrue(oa.isAuthorizedOrigin(NS, opB));
        assertEq(oa.getOrigins(NS).length, 2);
        (, uint256 readyAt) = oa.getPendingAssignment(NS);
        assertEq(readyAt, 0); // pending cleared
    }

    function test_activate_revertsBeforeTimelock() public {
        _propose(_ops2());
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(OriginAssignment.TimelockNotElapsed.selector, block.timestamp + TIMELOCK)
        );
        oa.activateAssignment(NS);
    }

    function test_activate_revertsNoPending() public {
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.NoPendingProposal.selector, NS));
        oa.activateAssignment(NS);
    }

    function test_activate_revertsOperatorWentInactive() public {
        _propose(_ops2());
        vm.warp(block.timestamp + TIMELOCK);
        bond.setActive(opB, false);
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.OperatorNotActive.selector, opB));
        oa.activateAssignment(NS);
    }

    function test_activate_revertsOperatorBlacklisted() public {
        _propose(_ops2());
        vm.warp(block.timestamp + TIMELOCK);
        blacklist.setBlacklisted(opA, true);
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.OperatorBlacklisted.selector, opA));
        oa.activateAssignment(NS);
    }

    /// @notice M-2 — activation must also reject an operator blacklisted via the
    ///         operator mapping (`addOperator`), not only the origin mapping.
    function test_activate_revertsOperatorBlacklistedViaOperatorMapping() public {
        _propose(_ops2());
        vm.warp(block.timestamp + TIMELOCK);
        blacklist.setOperatorBlacklisted(opB, true);
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.OperatorBlacklisted.selector, opB));
        oa.activateAssignment(NS);
    }

    function test_activate_onlyGovernance() public {
        _propose(_ops2());
        vm.warp(block.timestamp + TIMELOCK);
        vm.prank(stranger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        oa.activateAssignment(NS);
    }

    function test_activate_replacesPreviousSet() public {
        _propose(_ops2());
        vm.warp(block.timestamp + TIMELOCK);
        vm.prank(admin);
        oa.activateAssignment(NS);

        // Re-propose a different single-operator set and activate.
        address[] memory one = new address[](1);
        one[0] = opC;
        vm.prank(publisher);
        oa.proposeAssignment(NS, one);
        vm.warp(block.timestamp + TIMELOCK);
        vm.prank(admin);
        oa.activateAssignment(NS);

        assertFalse(oa.isAuthorizedOrigin(NS, opA));
        assertFalse(oa.isAuthorizedOrigin(NS, opB));
        assertTrue(oa.isAuthorizedOrigin(NS, opC));
        assertEq(oa.getOrigins(NS).length, 1);
    }

    function test_activate_blacklistSkippedWhenUnset() public {
        OriginAssignment oaNoBl = new OriginAssignment(bond, registry, address(0), admin);
        vm.prank(publisher);
        oaNoBl.proposeAssignment(NS, _ops2());
        vm.warp(block.timestamp + TIMELOCK);
        // opA "blacklisted" in the standalone mock, but oaNoBl has no binding → check skipped.
        blacklist.setBlacklisted(opA, true);
        vm.prank(admin);
        oaNoBl.activateAssignment(NS);
        assertTrue(oaNoBl.isAuthorizedOrigin(NS, opA));
    }

    // -----------------------------------------------------------------
    // cancel / revoke / prune
    // -----------------------------------------------------------------

    function test_cancel_clearsPending() public {
        _propose(_ops2());
        vm.prank(publisher);
        oa.cancelAssignmentProposal(NS);
        (, uint256 readyAt) = oa.getPendingAssignment(NS);
        assertEq(readyAt, 0);
    }

    function test_revoke_byPublisherAndGovernance() public {
        _propose(_ops2());
        vm.warp(block.timestamp + TIMELOCK);
        vm.prank(admin);
        oa.activateAssignment(NS);

        vm.prank(publisher);
        oa.revokeAssignment(NS, opA);
        assertFalse(oa.isAuthorizedOrigin(NS, opA));

        vm.prank(admin);
        oa.revokeAssignment(NS, opB);
        assertFalse(oa.isAuthorizedOrigin(NS, opB));
    }

    function test_revoke_revertsNonMember() public {
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.NotAuthorizedOrigin.selector, NS, opC));
        oa.revokeAssignment(NS, opC);
    }

    function test_prune_removesBlacklistedOperator() public {
        _propose(_ops2());
        vm.warp(block.timestamp + TIMELOCK);
        vm.prank(admin);
        oa.activateAssignment(NS);

        blacklist.setBlacklisted(opA, true);
        // Permissionless — any caller.
        vm.prank(stranger);
        oa.pruneBlacklistedAssignment(NS, opA);
        assertFalse(oa.isAuthorizedOrigin(NS, opA));
    }

    /// @notice M-2 — prune must also work for an operator blacklisted via the
    ///         operator mapping (`addOperator`), not only the origin mapping.
    function test_prune_removesOperatorBlacklistedViaOperatorMapping() public {
        _propose(_ops2());
        vm.warp(block.timestamp + TIMELOCK);
        vm.prank(admin);
        oa.activateAssignment(NS);

        blacklist.setOperatorBlacklisted(opA, true);
        vm.prank(stranger);
        oa.pruneBlacklistedAssignment(NS, opA);
        assertFalse(oa.isAuthorizedOrigin(NS, opA));
    }

    function test_prune_revertsWhenNotBlacklisted() public {
        _propose(_ops2());
        vm.warp(block.timestamp + TIMELOCK);
        vm.prank(admin);
        oa.activateAssignment(NS);
        vm.prank(stranger);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.OperatorNotBlacklisted.selector, opA));
        oa.pruneBlacklistedAssignment(NS, opA);
    }

    function test_prune_revertsWhenBlacklistUnset() public {
        OriginAssignment oaNoBl = new OriginAssignment(bond, registry, address(0), admin);
        vm.prank(stranger);
        vm.expectRevert(OriginAssignment.ContentBlacklistNotSet.selector);
        oaNoBl.pruneBlacklistedAssignment(NS, opA);
    }

    // -----------------------------------------------------------------
    // Default-open allow-list (namespaceId == 0)
    // -----------------------------------------------------------------

    function test_defaultOpen_setAllowlist() public {
        vm.prank(admin);
        oa.setDefaultOpenAllowlist(_ops2());
        assertTrue(oa.isAuthorizedOrigin(0, opA));
        assertTrue(oa.isAuthorizedOrigin(0, opB));
    }

    function test_defaultOpen_addAndRemove() public {
        vm.prank(admin);
        oa.addDefaultOpenOperator(opA);
        assertTrue(oa.isAuthorizedOrigin(0, opA));
        vm.prank(admin);
        oa.removeDefaultOpenOperator(opA);
        assertFalse(oa.isAuthorizedOrigin(0, opA));
    }

    function test_defaultOpen_revertsInactive() public {
        bond.setActive(opA, false);
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.OperatorNotActive.selector, opA));
        oa.addDefaultOpenOperator(opA);
    }

    function test_defaultOpen_onlyGovernance() public {
        vm.prank(stranger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        oa.addDefaultOpenOperator(opA);
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function test_setContentBlacklist_rejectsZero() public {
        vm.prank(admin);
        vm.expectRevert(OriginAssignment.ZeroAddress.selector);
        oa.setContentBlacklist(address(0));
    }

    function test_setAssignmentTimelock_enforcesBounds() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                OriginAssignment.ParamOutOfBounds.selector, uint256(1 hours), uint256(24 hours), uint256(14 days)
            )
        );
        oa.setAssignmentTimelock(1 hours);
        vm.prank(admin);
        oa.setAssignmentTimelock(7 days);
        assertEq(oa.assignmentTimelock(), 7 days);
    }

    function test_setMaxOrigins_enforcesBounds() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(OriginAssignment.ParamOutOfBounds.selector, uint256(0), uint256(1), uint256(50))
        );
        oa.setMaxOriginsPerNamespace(0);
    }
}
