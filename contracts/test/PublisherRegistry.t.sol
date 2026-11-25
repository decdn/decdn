// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { PublisherRegistry } from "../src/PublisherRegistry.sol";

contract PublisherRegistryTest is Test {
    PublisherRegistry internal reg;

    address internal admin = makeAddr("admin");
    address internal alice = makeAddr("alice");
    address internal bob = makeAddr("bob");

    function setUp() public {
        reg = new PublisherRegistry(admin);
    }

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    function test_constructor_setsDefaultsAndRoles() public view {
        assertEq(reg.maxNamespacesPerPublisher(), 100);
        assertTrue(reg.hasRole(reg.DEFAULT_ADMIN_ROLE(), admin));
        assertTrue(reg.hasRole(reg.GOVERNANCE_ROLE(), admin));
    }

    function test_constructor_revertsZeroAdmin() public {
        vm.expectRevert(PublisherRegistry.ZeroAddress.selector);
        new PublisherRegistry(address(0));
    }

    // -----------------------------------------------------------------
    // createNamespace
    // -----------------------------------------------------------------

    function test_createNamespace_assignsIdsFromOne() public {
        vm.prank(alice);
        uint256 id1 = reg.createNamespace();
        vm.prank(alice);
        uint256 id2 = reg.createNamespace();

        assertEq(id1, 1, "first id is 1 (0 reserved for default-open)");
        assertEq(id2, 2);
        assertEq(reg.ownerOf(id1), alice);
        assertEq(reg.ownerOf(id2), alice);
        assertEq(reg.namespaceCount(alice), 2);
    }

    function test_createNamespace_zeroIsNeverAssigned() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        assertGt(id, 0);
        // namespace 0 (no namespace) has no owner
        assertEq(reg.ownerOf(0), address(0));
    }

    function test_createNamespace_revertsAtCap() public {
        vm.prank(admin);
        reg.setMaxNamespacesPerPublisher(2);

        vm.prank(alice);
        reg.createNamespace();
        vm.prank(alice);
        reg.createNamespace();
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NamespaceCapReached.selector, 2));
        reg.createNamespace();
    }

    // -----------------------------------------------------------------
    // Namespace transfer (offer, then accept)
    // -----------------------------------------------------------------

    function test_transfer_fullFlow() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();

        vm.expectEmit(address(reg));
        emit PublisherRegistry.NamespaceTransferStarted(id, alice, bob);
        vm.prank(alice);
        reg.transferNamespace(id, bob);

        assertEq(reg.pendingOwnerOf(id), bob);
        // An offer alone moves nothing.
        assertEq(reg.ownerOf(id), alice);

        // The recipient accepts in the same block: no delay applies.
        vm.expectEmit(address(reg));
        emit PublisherRegistry.NamespaceTransferred(id, alice, bob);
        vm.prank(bob);
        reg.acceptNamespace(id);

        assertEq(reg.ownerOf(id), bob);
        assertEq(reg.namespaceCount(alice), 0);
        assertEq(reg.namespaceCount(bob), 1);
        assertEq(reg.pendingOwnerOf(id), address(0));
    }

    function test_transfer_revertsIfNotOwner() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        vm.prank(bob);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NotNamespaceOwner.selector, id, bob));
        reg.transferNamespace(id, bob);
    }

    function test_transfer_acceptRevertsIfNotPendingOwner() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        vm.prank(alice);
        reg.transferNamespace(id, bob);

        // alice (current owner) cannot accept; only the pending recipient.
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NotPendingOwner.selector, id, alice));
        reg.acceptNamespace(id);
    }

    function test_transfer_acceptRevertsWithoutOffer() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        vm.prank(bob);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NotPendingOwner.selector, id, bob));
        reg.acceptNamespace(id);
    }

    function test_transfer_zeroAddressWithdrawsOffer() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        vm.prank(alice);
        reg.transferNamespace(id, bob);

        vm.expectEmit(address(reg));
        emit PublisherRegistry.NamespaceTransferStarted(id, alice, address(0));
        vm.prank(alice);
        reg.transferNamespace(id, address(0));
        assertEq(reg.pendingOwnerOf(id), address(0));

        vm.prank(bob);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NotPendingOwner.selector, id, bob));
        reg.acceptNamespace(id);
    }

    function test_transfer_newOfferReplacesPending() public {
        address carol = makeAddr("carol");
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        vm.prank(alice);
        reg.transferNamespace(id, bob);
        vm.prank(alice);
        reg.transferNamespace(id, carol);

        vm.prank(bob);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NotPendingOwner.selector, id, bob));
        reg.acceptNamespace(id);

        vm.prank(carol);
        reg.acceptNamespace(id);
        assertEq(reg.ownerOf(id), carol);
    }

    function test_transfer_firstAcceptLocksOutTheOldKey() public {
        // The owner of a leaked key moves the namespace to a fresh key before
        // the key's other holder acts. Once accepted, the old key controls
        // nothing: it can neither offer the namespace nor accept it.
        address freshKey = makeAddr("freshKey");
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        vm.prank(alice);
        reg.transferNamespace(id, freshKey);
        vm.prank(freshKey);
        reg.acceptNamespace(id);

        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NotNamespaceOwner.selector, id, alice));
        reg.transferNamespace(id, alice);
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NotPendingOwner.selector, id, alice));
        reg.acceptNamespace(id);
    }

    function test_transfer_revertsWhenRecipientAtCap() public {
        // The anti-squatting cap must hold on the receiving side too —
        // otherwise it's bypassable by minting namespaces under throwaway
        // addresses and transferring them to one publisher.
        vm.prank(admin);
        reg.setMaxNamespacesPerPublisher(1);

        vm.prank(bob);
        reg.createNamespace(); // bob now at the cap

        vm.prank(alice);
        uint256 id = reg.createNamespace();
        vm.prank(alice);
        reg.transferNamespace(id, bob);

        vm.prank(bob);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NamespaceCapReached.selector, 1));
        reg.acceptNamespace(id);
    }

    function test_transfer_selfTransferExemptFromCap() public {
        vm.prank(admin);
        reg.setMaxNamespacesPerPublisher(1);

        vm.prank(alice);
        uint256 id = reg.createNamespace(); // alice at the cap

        // Transferring to oneself leaves the count unchanged, so the cap
        // check must not block it.
        vm.prank(alice);
        reg.transferNamespace(id, alice);
        vm.prank(alice);
        reg.acceptNamespace(id);

        assertEq(reg.ownerOf(id), alice);
        assertEq(reg.namespaceCount(alice), 1);
    }

    // -----------------------------------------------------------------
    // Governable setters
    // -----------------------------------------------------------------

    function test_setMaxNamespaces_withinBounds() public {
        vm.prank(admin);
        reg.setMaxNamespacesPerPublisher(500);
        assertEq(reg.maxNamespacesPerPublisher(), 500);
    }

    function test_setMaxNamespaces_revertsOutOfBounds() public {
        vm.prank(admin);
        vm.expectRevert();
        reg.setMaxNamespacesPerPublisher(0);
        vm.prank(admin);
        vm.expectRevert();
        reg.setMaxNamespacesPerPublisher(1001);
    }

    function test_setMaxNamespaces_revertsWithoutRole() public {
        bytes32 govRole = reg.GOVERNANCE_ROLE();
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, alice, govRole)
        );
        vm.prank(alice);
        reg.setMaxNamespacesPerPublisher(50);
    }

    // -----------------------------------------------------------------
    // Fuzz
    // -----------------------------------------------------------------

    function testFuzz_createNamespace_idsAreSequentialAndOwned(uint8 n) public {
        uint256 count = bound(n, 1, 100);
        for (uint256 i = 0; i < count; i++) {
            vm.prank(alice);
            uint256 id = reg.createNamespace();
            assertEq(id, i + 1);
            assertEq(reg.ownerOf(id), alice);
        }
        assertEq(reg.namespaceCount(alice), count);
    }
}
