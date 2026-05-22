// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { PublisherRegistry } from "../src/PublisherRegistry.sol";

contract PublisherRegistryTest is Test {
    PublisherRegistry internal reg;

    address internal admin = makeAddr("admin");
    address internal alice = makeAddr("alice");
    address internal bob = makeAddr("bob");

    uint64 internal constant DEFAULT_TIMELOCK = 7 days;

    function setUp() public {
        reg = new PublisherRegistry(admin);
    }

    // -----------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------

    function test_constructor_setsDefaultsAndRoles() public view {
        assertEq(reg.maxNamespacesPerPublisher(), 100);
        assertEq(reg.namespaceTransferTimelock(), DEFAULT_TIMELOCK);
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
        // default-open namespace 0 has no owner
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
    // Content claims
    // -----------------------------------------------------------------

    function test_claimContent_recordsClaim() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();

        bytes32 hash = keccak256("blob");
        vm.prank(alice);
        reg.claimContent(id, hash);

        uint256[] memory claims = reg.namespaceOf(hash);
        assertEq(claims.length, 1);
        assertEq(claims[0], id);
    }

    function test_claimContent_multiClaimAcrossNamespaces() public {
        vm.prank(alice);
        uint256 aliceNs = reg.createNamespace();
        vm.prank(bob);
        uint256 bobNs = reg.createNamespace();

        bytes32 hash = keccak256("shared-blob");
        vm.prank(alice);
        reg.claimContent(aliceNs, hash);
        vm.prank(bob);
        reg.claimContent(bobNs, hash);

        uint256[] memory claims = reg.namespaceOf(hash);
        assertEq(claims.length, 2, "independent multi-claim is allowed");
        assertEq(claims[0], aliceNs);
        assertEq(claims[1], bobNs);
    }

    function test_claimContent_revertsOnDuplicateSameNamespace() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        bytes32 hash = keccak256("blob");

        vm.prank(alice);
        reg.claimContent(id, hash);
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.AlreadyClaimed.selector, id, hash));
        reg.claimContent(id, hash);
    }

    function test_claimContent_revertsIfNotOwner() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        bytes32 hash = keccak256("blob");

        vm.prank(bob);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NotNamespaceOwner.selector, id, bob));
        reg.claimContent(id, hash);
    }

    function test_claimContent_revertsForDefaultOpenNamespace() public {
        // No one owns namespace 0, so claiming under it reverts.
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NotNamespaceOwner.selector, uint256(0), alice));
        reg.claimContent(0, keccak256("blob"));
    }

    function test_namespaceOf_emptyForUnclaimed() public view {
        assertEq(reg.namespaceOf(keccak256("never-claimed")).length, 0);
    }

    // -----------------------------------------------------------------
    // Namespace transfer (2-step + timelock)
    // -----------------------------------------------------------------

    function test_transfer_fullFlow() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();

        vm.prank(alice);
        reg.initiateNamespaceTransfer(id, bob);

        (address pendingOwner, uint256 readyAt) = reg.pendingTransfer(id);
        assertEq(pendingOwner, bob);
        assertEq(readyAt, block.timestamp + DEFAULT_TIMELOCK);

        // Cannot finalize before timelock.
        vm.warp(block.timestamp + DEFAULT_TIMELOCK - 1);
        vm.prank(bob);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.TransferNotReady.selector, readyAt));
        reg.finalizeNamespaceTransfer(id);

        // After timelock, the new owner finalizes.
        vm.warp(readyAt);
        vm.prank(bob);
        reg.finalizeNamespaceTransfer(id);

        assertEq(reg.ownerOf(id), bob);
        assertEq(reg.namespaceCount(alice), 0);
        assertEq(reg.namespaceCount(bob), 1);
        (address clearedOwner,) = reg.pendingTransfer(id);
        assertEq(clearedOwner, address(0));
    }

    function test_transfer_initiateRevertsIfNotOwner() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        vm.prank(bob);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NotNamespaceOwner.selector, id, bob));
        reg.initiateNamespaceTransfer(id, bob);
    }

    function test_transfer_initiateRevertsZeroNewOwner() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        vm.prank(alice);
        vm.expectRevert(PublisherRegistry.TransferToZeroAddress.selector);
        reg.initiateNamespaceTransfer(id, address(0));
    }

    function test_transfer_finalizeRevertsIfNotPendingOwner() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        vm.prank(alice);
        reg.initiateNamespaceTransfer(id, bob);

        vm.warp(block.timestamp + DEFAULT_TIMELOCK);
        // alice (current owner) cannot finalize; only the pending recipient.
        vm.prank(alice);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NotPendingOwner.selector, id, alice));
        reg.finalizeNamespaceTransfer(id);
    }

    function test_transfer_cancelByOwner() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        vm.prank(alice);
        reg.initiateNamespaceTransfer(id, bob);

        vm.prank(alice);
        reg.cancelNamespaceTransfer(id);

        (address pendingOwner,) = reg.pendingTransfer(id);
        assertEq(pendingOwner, address(0));

        // bob can no longer finalize.
        vm.warp(block.timestamp + DEFAULT_TIMELOCK);
        vm.prank(bob);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NoPendingTransfer.selector, id));
        reg.finalizeNamespaceTransfer(id);
    }

    function test_transfer_cancelRevertsIfNotOwner() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        vm.prank(alice);
        reg.initiateNamespaceTransfer(id, bob);

        vm.prank(bob);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NotNamespaceOwner.selector, id, bob));
        reg.cancelNamespaceTransfer(id);
    }

    function test_transfer_finalizeRevertsNoPending() public {
        vm.prank(alice);
        uint256 id = reg.createNamespace();
        vm.prank(bob);
        vm.expectRevert(abi.encodeWithSelector(PublisherRegistry.NoPendingTransfer.selector, id));
        reg.finalizeNamespaceTransfer(id);
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
        vm.prank(alice);
        vm.expectRevert();
        reg.setMaxNamespacesPerPublisher(50);
    }

    function test_setTransferTimelock_withinBounds() public {
        vm.prank(admin);
        reg.setNamespaceTransferTimelock(14 days);
        assertEq(reg.namespaceTransferTimelock(), 14 days);
    }

    function test_setTransferTimelock_revertsOutOfBounds() public {
        vm.prank(admin);
        vm.expectRevert();
        reg.setNamespaceTransferTimelock(1 hours); // < 1 day
        vm.prank(admin);
        vm.expectRevert();
        reg.setNamespaceTransferTimelock(60 days); // > 30 days
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
