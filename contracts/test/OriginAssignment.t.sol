// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Vm } from "forge-std/Vm.sol";
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
    mapping(address => uint256) internal _count;

    function setOwner(uint256 ns, address o) external {
        _owner[ns] = o;
    }

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
        registry.setNamespaceCount(publisher, 1);
        bond.setActive(opA, true);
        bond.setActive(opB, true);
        bond.setActive(opC, true);

        vm.warp(1_000_000);
    }

    /// Governance's instant grant — the fast setup for the origin-plane tests,
    /// which are about seating, not about how the wallet got vetted.
    function _vet(address p) internal {
        vm.prank(admin);
        oa.setPublisherVetted(p, true);
    }

    function _seat(address operator) internal {
        vm.prank(publisher);
        oa.addOrigin(NS, operator);
    }

    /// A vetted publisher with two origins seated — the starting state for the
    /// removal, prune, and enumeration tests.
    function _vetAndSeatBoth() internal {
        _vet(publisher);
        _seat(opA);
        _seat(opB);
    }

    // -----------------------------------------------------------------
    // Vetting — request / cancel
    // -----------------------------------------------------------------

    function test_requestVetting_storesReadyAt() public {
        vm.prank(publisher);
        oa.requestVetting();
        assertEq(oa.getPendingVetting(publisher), block.timestamp + TIMELOCK);
        assertFalse(oa.isVettedPublisher(publisher), "a request alone does not vet");
    }

    /// Vetting is for publishers: an address that never created a namespace has
    /// nothing to seat origins for, so it cannot enter the queue.
    function test_requestVetting_revertsWithoutANamespace() public {
        vm.prank(stranger);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.NoNamespaceOwned.selector, stranger));
        oa.requestVetting();
    }

    function test_requestVetting_revertsWhileOneIsPending() public {
        vm.startPrank(publisher);
        oa.requestVetting();
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.VettingRequestPending.selector, publisher));
        oa.requestVetting();
        vm.stopPrank();
    }

    function test_requestVetting_revertsWhenAlreadyVetted() public {
        _vet(publisher);
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.AlreadyVetted.selector, publisher));
        oa.requestVetting();
    }

    function test_cancelVettingRequest_clearsPending() public {
        vm.startPrank(publisher);
        oa.requestVetting();
        oa.cancelVettingRequest();
        vm.stopPrank();
        assertEq(oa.getPendingVetting(publisher), 0);
    }

    function test_cancelVettingRequest_revertsWithoutOne() public {
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.NoVettingRequest.selector, publisher));
        oa.cancelVettingRequest();
    }

    // -----------------------------------------------------------------
    // Vetting — governance grant
    // -----------------------------------------------------------------

    function test_grantVetting_vetsAfterTimelockAndClearsPending() public {
        vm.prank(publisher);
        oa.requestVetting();
        vm.warp(block.timestamp + TIMELOCK);

        vm.prank(admin);
        oa.grantVetting(publisher);

        assertTrue(oa.isVettedPublisher(publisher));
        assertEq(oa.getPendingVetting(publisher), 0);
    }

    function test_grantVetting_revertsBeforeTimelock() public {
        vm.prank(publisher);
        oa.requestVetting();
        uint256 readyAt = block.timestamp + TIMELOCK;

        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.VettingTimelockNotElapsed.selector, readyAt));
        oa.grantVetting(publisher);
    }

    function test_grantVetting_revertsWithoutRequest() public {
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.NoVettingRequest.selector, publisher));
        oa.grantVetting(publisher);
    }

    function test_grantVetting_onlyGovernance() public {
        vm.prank(publisher);
        oa.requestVetting();
        vm.warp(block.timestamp + TIMELOCK);

        vm.prank(stranger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        oa.grantVetting(publisher);
    }

    // -----------------------------------------------------------------
    // Vetting — governance override
    // -----------------------------------------------------------------

    /// The instant grant also consumes any queued request, so the publisher is
    /// not left holding a ripened one.
    function test_setPublisherVetted_grantsInstantlyAndConsumesPending() public {
        vm.prank(publisher);
        oa.requestVetting();

        _vet(publisher);

        assertTrue(oa.isVettedPublisher(publisher));
        assertEq(oa.getPendingVetting(publisher), 0);
    }

    /// Un-vetting must also clear a pending request — otherwise a rogue
    /// publisher's already-ripened request would walk it straight back in. The
    /// request is warped past its timelock FIRST: an implementation that only
    /// cleared un-ripened requests would pass the un-warped version of this test
    /// while leaving the exact hole open.
    function test_setPublisherVetted_unvetClearsRipenedPendingRequest() public {
        vm.prank(publisher);
        oa.requestVetting();
        vm.warp(block.timestamp + TIMELOCK + 1);

        vm.prank(admin);
        oa.setPublisherVetted(publisher, false);

        assertFalse(oa.isVettedPublisher(publisher));
        assertEq(oa.getPendingVetting(publisher), 0);

        // The ripened request is gone, not merely hidden: governance cannot
        // grant it, so the publisher must queue a fresh one and wait again.
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.NoVettingRequest.selector, publisher));
        oa.grantVetting(publisher);
    }

    function test_setPublisherVetted_rejectsZeroAddress() public {
        vm.prank(admin);
        vm.expectRevert(OriginAssignment.ZeroAddress.selector);
        oa.setPublisherVetted(address(0), true);
    }

    function test_setPublisherVetted_onlyGovernance() public {
        vm.prank(stranger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        oa.setPublisherVetted(publisher, true);
    }

    // -----------------------------------------------------------------
    // addOrigin
    // -----------------------------------------------------------------

    function test_addOrigin_seatsInstantly() public {
        _vet(publisher);

        vm.expectEmit(true, true, true, true);
        emit OriginAssignment.OriginAdded(NS, opA, publisher);
        vm.prank(publisher);
        oa.addOrigin(NS, opA);

        assertTrue(oa.isAuthorizedOrigin(NS, opA));
        assertEq(oa.getOrigins(NS).length, 1);
    }

    function test_addOrigin_revertsNonOwner() public {
        _vet(stranger);
        vm.prank(stranger);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.NotNamespaceOwner.selector, NS, stranger));
        oa.addOrigin(NS, opA);
    }

    function test_addOrigin_revertsUnvettedPublisher() public {
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.PublisherNotVetted.selector, publisher));
        oa.addOrigin(NS, opA);
    }

    function test_addOrigin_revertsInactiveOperator() public {
        _vet(publisher);
        bond.setActive(opA, false);
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.OperatorNotActive.selector, opA));
        oa.addOrigin(NS, opA);
    }

    function test_addOrigin_revertsBlacklistedOperator() public {
        _vet(publisher);
        blacklist.setBlacklisted(opA, true);
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.OperatorBlacklisted.selector, opA));
        oa.addOrigin(NS, opA);
    }

    /// @notice M-2 — seating must also reject an operator blacklisted via the
    ///         operator mapping (`addOperator`), not only the origin mapping.
    function test_addOrigin_revertsOperatorBlacklistedViaOperatorMapping() public {
        _vet(publisher);
        blacklist.setOperatorBlacklisted(opA, true);
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.OperatorBlacklisted.selector, opA));
        oa.addOrigin(NS, opA);
    }

    function test_addOrigin_revertsAlreadySeated() public {
        _vet(publisher);
        _seat(opA);
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.DuplicateOperator.selector, opA));
        oa.addOrigin(NS, opA);
    }

    /// At cap, a re-add of an ALREADY-SEATED operator must still report the
    /// duplicate — ADR 011 § Edge cases states that unconditionally, and
    /// `TooManyOrigins` would name a count the set never reaches.
    function test_addOrigin_duplicateOutranksCapWhenFull() public {
        vm.prank(admin);
        oa.setMaxOriginsPerNamespace(1);
        _vet(publisher);
        _seat(opA);

        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.DuplicateOperator.selector, opA));
        oa.addOrigin(NS, opA);
    }

    function test_addOrigin_revertsAtCap() public {
        vm.prank(admin);
        oa.setMaxOriginsPerNamespace(1);
        _vet(publisher);
        _seat(opA);

        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.TooManyOrigins.selector, uint256(2), uint256(1)));
        oa.addOrigin(NS, opB);
    }

    /// Deployment window: with no `ContentBlacklist` bound, seating validates
    /// against `CapacityBond.isActive` alone.
    function test_addOrigin_blacklistSkippedWhenUnset() public {
        OriginAssignment oaNoBl = new OriginAssignment(bond, registry, address(0), admin);
        vm.prank(admin);
        oaNoBl.setPublisherVetted(publisher, true);
        // opA "blacklisted" in the standalone mock, but oaNoBl has no binding → check skipped.
        blacklist.setBlacklisted(opA, true);

        vm.prank(publisher);
        oaNoBl.addOrigin(NS, opA);
        assertTrue(oaNoBl.isAuthorizedOrigin(NS, opA));
    }

    /// ADR 016 post-deploy step 2: binding the blacklist turns the guard on.
    /// Nothing else proves the setter writes the slot the guards read — every
    /// other blacklist test gets its binding from the constructor, so a setter
    /// that wrote the wrong slot would ship a permanently unenforced blacklist
    /// with a green suite.
    function test_setContentBlacklist_turnsTheGuardOn() public {
        OriginAssignment oaNoBl = new OriginAssignment(bond, registry, address(0), admin);
        vm.prank(admin);
        oaNoBl.setPublisherVetted(publisher, true);
        vm.prank(publisher);
        oaNoBl.addOrigin(NS, opA);

        vm.prank(admin);
        oaNoBl.setContentBlacklist(address(blacklist));

        blacklist.setBlacklisted(opB, true);
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.OperatorBlacklisted.selector, opB));
        oaNoBl.addOrigin(NS, opB);

        // And the prune path, which reverted `ContentBlacklistNotSet` before.
        blacklist.setBlacklisted(opA, true);
        oaNoBl.pruneBlacklistedOrigin(NS, opA);
        assertFalse(oaNoBl.isAuthorizedOrigin(NS, opA));
    }

    /// The #1107 regression, stated as an invariant: seating B validates B and
    /// nothing else. A live origin A that has gone inactive AND been blacklisted
    /// cannot block the publisher from adding a redundant origin.
    function test_addOrigin_deltaSeatIsIndependentOfLiveOriginState() public {
        _vet(publisher);
        _seat(opA);

        bond.setActive(opA, false);
        blacklist.setBlacklisted(opA, true);

        _seat(opB);

        assertTrue(oa.isAuthorizedOrigin(NS, opB), "B seats despite A's state");
        assertTrue(oa.isAuthorizedOrigin(NS, opA), "A is untouched, not re-validated");
    }

    /// Un-vetting is a proactive backstop on FUTURE seating. Already-seated
    /// origins stay until an explicit removal or a blacklist prune — evicting
    /// them here would be unbounded in the publisher's namespace count.
    function test_unvetBlocksNewOriginsButKeepsSeatedOnes() public {
        _vet(publisher);
        _seat(opA);

        vm.prank(admin);
        oa.setPublisherVetted(publisher, false);

        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.PublisherNotVetted.selector, publisher));
        oa.addOrigin(NS, opB);

        assertTrue(oa.isAuthorizedOrigin(NS, opA), "the seated origin keeps serving");

        // The publisher can still unseat, and governance can force it.
        vm.prank(admin);
        oa.removeOrigin(NS, opA);
        assertFalse(oa.isAuthorizedOrigin(NS, opA));
    }

    // -----------------------------------------------------------------
    // namespace 0 (NO_NAMESPACE) is unassignable and authorizes nothing
    // -----------------------------------------------------------------

    /// Namespace 0 is the `NO_NAMESPACE` sentinel: it has no publisher
    /// (`ownerOf(0) == address(0)`), so it can never be seated an origin, and both
    /// origin views resolve to empty for it. This is the on-chain half of the
    /// invariant the node's pull-through gate and origin directory rely on (ADR 002
    /// §Namespace 0).
    function test_namespaceZero_isUnassignableAndHasNoOrigins() public {
        // Even a vetted publisher cannot seat namespace 0 — the registry owns it
        // to nobody, so the ownership check rejects every caller.
        _vet(publisher);
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.NotNamespaceOwner.selector, uint256(0), publisher));
        oa.addOrigin(0, opA);

        // And both origin views are empty/false for namespace 0.
        assertEq(oa.getOrigins(0).length, 0);
        assertFalse(oa.isAuthorizedOrigin(0, opA));
    }

    // -----------------------------------------------------------------
    // removeOrigin / pruneBlacklistedOrigin
    // -----------------------------------------------------------------

    function test_removeOrigin_byPublisherAndGovernance() public {
        _vetAndSeatBoth();

        vm.expectEmit(true, true, true, true);
        emit OriginAssignment.OriginRemoved(NS, opA, publisher);
        vm.prank(publisher);
        oa.removeOrigin(NS, opA);
        assertFalse(oa.isAuthorizedOrigin(NS, opA));

        vm.expectEmit(true, true, true, true);
        emit OriginAssignment.OriginRemoved(NS, opB, admin);
        vm.prank(admin);
        oa.removeOrigin(NS, opB);
        assertFalse(oa.isAuthorizedOrigin(NS, opB));
    }

    function test_removeOrigin_revertsForThirdParty() public {
        _vetAndSeatBoth();
        vm.prank(stranger);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.NotNamespaceOwner.selector, NS, stranger));
        oa.removeOrigin(NS, opA);
    }

    function test_removeOrigin_revertsNonMember() public {
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.NotAuthorizedOrigin.selector, NS, opC));
        oa.removeOrigin(NS, opC);
    }

    function test_prune_removesBlacklistedOperator() public {
        _vetAndSeatBoth();

        blacklist.setBlacklisted(opA, true);
        // Permissionless — any caller.
        vm.expectEmit(true, true, true, true);
        emit OriginAssignment.BlacklistedOriginPruned(NS, opA, stranger);
        vm.prank(stranger);
        oa.pruneBlacklistedOrigin(NS, opA);
        assertFalse(oa.isAuthorizedOrigin(NS, opA));
    }

    /// @notice M-2 — prune must also work for an operator blacklisted via the
    ///         operator mapping (`addOperator`), not only the origin mapping.
    function test_prune_removesOperatorBlacklistedViaOperatorMapping() public {
        _vetAndSeatBoth();

        blacklist.setOperatorBlacklisted(opA, true);
        vm.prank(stranger);
        oa.pruneBlacklistedOrigin(NS, opA);
        assertFalse(oa.isAuthorizedOrigin(NS, opA));
    }

    function test_prune_revertsWhenNotBlacklisted() public {
        _vetAndSeatBoth();
        vm.prank(stranger);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.OperatorNotBlacklisted.selector, opA));
        oa.pruneBlacklistedOrigin(NS, opA);
    }

    function test_prune_revertsWhenBlacklistUnset() public {
        OriginAssignment oaNoBl = new OriginAssignment(bond, registry, address(0), admin);
        vm.prank(stranger);
        vm.expectRevert(OriginAssignment.ContentBlacklistNotSet.selector);
        oaNoBl.pruneBlacklistedOrigin(NS, opA);
    }

    // -----------------------------------------------------------------
    // Governance setters
    // -----------------------------------------------------------------

    function test_setContentBlacklist_rejectsZero() public {
        vm.prank(admin);
        vm.expectRevert(OriginAssignment.ZeroAddress.selector);
        oa.setContentBlacklist(address(0));
    }

    function test_setVettingTimelock_enforcesBounds() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                OriginAssignment.ParamOutOfBounds.selector, uint256(1 hours), uint256(24 hours), uint256(14 days)
            )
        );
        oa.setVettingTimelock(1 hours);
        // The ceiling half: a copy-paste slip comparing against the FLOOR twice
        // would pass the check above and let governance set a 90-day delay.
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                OriginAssignment.ParamOutOfBounds.selector, uint256(15 days), uint256(24 hours), uint256(14 days)
            )
        );
        oa.setVettingTimelock(15 days);
        vm.prank(admin);
        oa.setVettingTimelock(7 days);
        assertEq(oa.vettingTimelock(), 7 days);
    }

    /// ADR 011 § Contract promises each governance setter emits its own update
    /// event, and the CHANGELOG tells indexers to map those topics. Nothing
    /// asserted the payloads, so a swapped old/new argument — the classic slip
    /// in a two-value update event — would ship silently.
    function test_governanceSetters_emitOldThenNew() public {
        vm.expectEmit(true, true, true, true);
        emit OriginAssignment.VettingTimelockUpdated(TIMELOCK, 7 days);
        vm.prank(admin);
        oa.setVettingTimelock(7 days);

        vm.expectEmit(true, true, true, true);
        emit OriginAssignment.MaxOriginsPerNamespaceUpdated(10, 5);
        vm.prank(admin);
        oa.setMaxOriginsPerNamespace(5);

        OriginAssignment oaNoBl = new OriginAssignment(bond, registry, address(0), admin);
        vm.expectEmit(true, true, true, true);
        emit OriginAssignment.ContentBlacklistUpdated(address(0), address(blacklist));
        vm.prank(admin);
        oaNoBl.setContentBlacklist(address(blacklist));
    }

    /// `PublisherVetted` carries the direction and the caller; both matter to a
    /// consumer deciding whether a publisher may still seat origins.
    function test_setPublisherVetted_emitsDirectionAndCaller() public {
        vm.expectEmit(true, true, true, true);
        emit OriginAssignment.PublisherVetted(publisher, true, admin);
        vm.prank(admin);
        oa.setPublisherVetted(publisher, true);

        vm.expectEmit(true, true, true, true);
        emit OriginAssignment.PublisherVetted(publisher, false, admin);
        vm.prank(admin);
        oa.setPublisherVetted(publisher, false);
    }

    function test_setMaxOrigins_enforcesBounds() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(OriginAssignment.ParamOutOfBounds.selector, uint256(0), uint256(1), uint256(50))
        );
        oa.setMaxOriginsPerNamespace(0);
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(OriginAssignment.ParamOutOfBounds.selector, uint256(51), uint256(1), uint256(50))
        );
        oa.setMaxOriginsPerNamespace(51);
    }

    /// The three remaining governance setters share one role gate; a missing
    /// `onlyRole` on `setContentBlacklist` would hand anyone the power to
    /// re-point the blacklist read and disable the operator-blacklist guard.
    function test_governanceSetters_rejectNonGovernance() public {
        bytes memory denied =
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE);
        vm.startPrank(stranger);
        vm.expectRevert(denied);
        oa.setContentBlacklist(address(blacklist));
        vm.expectRevert(denied);
        oa.setVettingTimelock(7 days);
        vm.expectRevert(denied);
        oa.setMaxOriginsPerNamespace(5);
        vm.stopPrank();
    }

    /// `isVettedPublisher` is a public mapping, so `InterfaceFreeze` cannot
    /// freeze its selector the way it freezes real functions — a rename there
    /// compiles fine. The Rust `sol!` binding and the e2e fixtures both bind this
    /// getter by hand, and a silent rename would mis-decode against a live
    /// deployment, which only the anvil job would catch. Probe the deployed ABI
    /// directly so the cheap gate catches it.
    function test_publicGetters_matchTheFrozenAbi() public {
        _vet(publisher);
        (bool ok, bytes memory ret) =
            address(oa).staticcall(abi.encodeWithSelector(bytes4(keccak256("isVettedPublisher(address)")), publisher));
        assertTrue(ok, "isVettedPublisher(address) must exist at the frozen selector");
        assertTrue(abi.decode(ret, (bool)), "and must answer for a vetted publisher");

        // Same situation, same reason: a public variable bound by hand in
        // `crates/e2e/src/bindings.rs`.
        (ok, ret) = address(oa).staticcall(abi.encodeWithSelector(bytes4(keccak256("vettingTimelock()"))));
        assertTrue(ok, "vettingTimelock() must exist at the frozen selector");
        assertEq(abi.decode(ret, (uint256)), TIMELOCK, "and must answer the configured delay");
    }

    /// Event topic0s, frozen. `InterfaceFreeze` pins function selectors, so
    /// nothing there covers events — and this redesign changed `OriginAdded`
    /// from one indexed field plus an ABI-encoded array to three indexed fields
    /// and an empty data section. That shape is what the node's `sol!` binding
    /// decodes; drift makes every log undecodable, which the watcher swallows by
    /// design (a `warn!` plus a counter). Without this the only gate is the anvil
    /// e2e.
    function test_eventTopics_frozen() public pure {
        assertEq(
            OriginAssignment.OriginAdded.selector, keccak256("OriginAdded(uint256,address,address)"), "OriginAdded"
        );
        assertEq(
            OriginAssignment.OriginRemoved.selector,
            keccak256("OriginRemoved(uint256,address,address)"),
            "OriginRemoved"
        );
        assertEq(
            OriginAssignment.BlacklistedOriginPruned.selector,
            keccak256("BlacklistedOriginPruned(uint256,address,address)"),
            "BlacklistedOriginPruned"
        );
        assertEq(
            OriginAssignment.VettingRequested.selector,
            keccak256("VettingRequested(address,uint256)"),
            "VettingRequested"
        );
    }

    /// Governance clearing a pending request must not announce it as a
    /// *cancellation* — that word is reserved for the publisher withdrawing its
    /// own. A spurious emit here would corrupt the pending-set view of exactly
    /// the log consumer the event exists for, and Foundry does not fail on
    /// unexpected events unless an `expectEmit` is armed, so assert it directly.
    function test_setPublisherVetted_emitsNoCancellation() public {
        vm.recordLogs();
        _vet(publisher);
        Vm.Log[] memory logs = vm.getRecordedLogs();
        for (uint256 i = 0; i < logs.length; ++i) {
            assertTrue(
                logs[i].topics[0] != OriginAssignment.VettingRequestCancelled.selector,
                "governance must not emit VettingRequestCancelled"
            );
        }
    }

    // -----------------------------------------------------------------
    // Namespace enumeration
    // -----------------------------------------------------------------
    //
    // Membership within a namespace was always readable via `getOrigins`; the
    // key set was not, which is why a consumer had to replay the seating events
    // from the deploy block just to learn which ids exist.

    /// Nothing seated yet enumerates as empty rather than reverting.
    function test_assignedNamespaces_emptyBeforeAnySeat() public view {
        assertEq(oa.assignedNamespaceCount(), 0);
        assertEq(oa.assignedNamespaces(0, 10).length, 0);
    }

    /// The namespace enters the index on its 0→1 transition — the first seat.
    function test_assignedNamespaces_seatedOnFirstOrigin() public {
        _vet(publisher);
        assertEq(oa.assignedNamespaceCount(), 0, "vetting alone seats nothing");

        _seat(opA);

        assertEq(oa.assignedNamespaceCount(), 1);
        assertEq(oa.assignedNamespaces(0, 10)[0], NS);
    }

    /// A second seat in the same namespace must not duplicate the key.
    function test_assignedNamespaces_secondOriginDoesNotDuplicate() public {
        _vetAndSeatBoth();
        assertEq(oa.assignedNamespaceCount(), 1);
    }

    /// The key set tracks "has origins", not "was ever assigned": it survives
    /// removal of one operator and is withdrawn only when the last one goes.
    function test_assignedNamespaces_withdrawnOnlyWhenSetEmpties() public {
        _vetAndSeatBoth();

        vm.prank(publisher);
        oa.removeOrigin(NS, opA);
        assertEq(oa.assignedNamespaceCount(), 1, "one operator remains");

        vm.prank(publisher);
        oa.removeOrigin(NS, opB);
        assertEq(oa.assignedNamespaceCount(), 0, "set is now empty");
        assertEq(oa.getOrigins(NS).length, 0);
    }

    /// The permissionless prune path must maintain the index too, or a fully
    /// pruned namespace would linger in the enumeration forever.
    function test_assignedNamespaces_prunePathWithdrawsWhenEmptied() public {
        _vetAndSeatBoth();
        blacklist.setBlacklisted(opA, true);
        blacklist.setBlacklisted(opB, true);

        oa.pruneBlacklistedOrigin(NS, opA);
        assertEq(oa.assignedNamespaceCount(), 1);
        oa.pruneBlacklistedOrigin(NS, opB);
        assertEq(oa.assignedNamespaceCount(), 0);
    }

    /// The index invariant, stated directly: a namespace is enumerated iff it
    /// has origins.
    function test_assignedNamespaces_matchesGetOriginsNonEmpty() public {
        _vetAndSeatBoth();
        assertEq(oa.assignedNamespaceCount(), 1);
        assertGt(oa.getOrigins(NS).length, 0);

        vm.startPrank(publisher);
        oa.removeOrigin(NS, opA);
        oa.removeOrigin(NS, opB);
        vm.stopPrank();

        assertEq(oa.assignedNamespaceCount(), 0);
        assertEq(oa.getOrigins(NS).length, 0);
    }

    /// Same pagination contract as the other enumeration views, including the
    /// `type(uint256).max` clamp.
    function test_assignedNamespaces_pagination() public {
        _vet(publisher);
        registry.setNamespaceCount(publisher, 3);
        for (uint256 ns = 1; ns <= 3; ++ns) {
            registry.setOwner(ns, publisher);
            vm.prank(publisher);
            oa.addOrigin(ns, opA);
        }

        assertEq(oa.assignedNamespaceCount(), 3);
        assertEq(oa.assignedNamespaces(0, 2).length, 2);
        assertEq(oa.assignedNamespaces(2, 2).length, 1);
        assertEq(oa.assignedNamespaces(3, 1).length, 0);
        assertEq(oa.assignedNamespaces(0, 0).length, 0);
        assertEq(oa.assignedNamespaces(0, type(uint256).max).length, 3);
    }

    /// Removal is swap-and-pop, so paging after one is where an off-by-one drops
    /// a namespace — and a dropped id is a namespace whose origins the node
    /// never learns at bootstrap. Asserted as a SET: order is explicitly not
    /// stable across mutations.
    function test_assignedNamespaces_pagesCorrectlyAfterARemoval() public {
        _vet(publisher);
        registry.setNamespaceCount(publisher, 3);
        for (uint256 ns = 1; ns <= 3; ++ns) {
            registry.setOwner(ns, publisher);
            vm.prank(publisher);
            oa.addOrigin(ns, opA);
        }

        vm.prank(publisher);
        oa.removeOrigin(2, opA);

        assertEq(oa.assignedNamespaceCount(), 2);
        uint256 first = oa.assignedNamespaces(0, 1)[0];
        uint256 second = oa.assignedNamespaces(1, 1)[0];
        assertTrue(first != second, "pages must not repeat an id");
        assertTrue(first == 1 || first == 3, "only 1 and 3 remain");
        assertTrue(second == 1 || second == 3, "only 1 and 3 remain");
        assertEq(oa.assignedNamespaces(2, 1).length, 0);
    }
}
