// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { OriginAssignment } from "../src/OriginAssignment.sol";
import { ManualVettingPolicy } from "../src/ManualVettingPolicy.sol";
import { ICapacityBondActivity } from "../src/interfaces/ICapacityBondActivity.sol";
import { IPublisherRegistryOwnership } from "../src/interfaces/IPublisherRegistryOwnership.sol";
import { IContentBlacklistOriginView } from "../src/interfaces/IContentBlacklistOriginView.sol";
import { IVettingPolicy } from "../src/interfaces/IVettingPolicy.sol";

/// A policy that vets nobody — used to prove `setVettingPolicy` changes the
/// decision `addOrigin` reads.
contract RejectAllVettingPolicy is IVettingPolicy {
    function isVetted(address) external pure returns (bool) {
        return false;
    }
}

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
    ManualVettingPolicy internal policy;
    OriginAssignment internal oa;

    address internal admin = address(0xA11CE);
    address internal publisher = address(0xBEEF);
    address internal stranger = address(0x5747A);
    address internal opA = address(0xA);
    address internal opB = address(0xB);
    address internal opC = address(0xC);

    uint256 internal constant NS = 1;
    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");

    function setUp() public {
        bond = new MockBondActivity();
        registry = new MockPublisherRegistry();
        blacklist = new MockBlacklistOrigin();
        // admin holds GOVERNANCE_ROLE and, as `initialVetter`, VETTER_ROLE — so it
        // can vet directly in these origin-plane tests.
        policy = new ManualVettingPolicy(admin, admin);

        oa = new OriginAssignment(bond, registry, address(blacklist), IVettingPolicy(address(policy)), admin);

        registry.setOwner(NS, publisher);
        registry.setNamespaceCount(publisher, 1);
        bond.setActive(opA, true);
        bond.setActive(opB, true);
        bond.setActive(opC, true);

        vm.warp(1_000_000);
    }

    /// Vet via the installed policy — the fast setup for the origin-plane tests,
    /// which are about seating, not about how the wallet got vetted.
    function _vet(address p) internal {
        vm.prank(admin);
        policy.setVetted(p, true);
    }

    /// Un-vet via the installed policy.
    function _unvet(address p) internal {
        vm.prank(admin);
        policy.setVetted(p, false);
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
    // Vetting policy — the swappable seam
    // -----------------------------------------------------------------

    /// `addOrigin` reads the installed policy's decision, not a stored bool.
    function test_addOrigin_usesPolicyDecision() public {
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.PublisherNotVetted.selector, publisher));
        oa.addOrigin(NS, opA);

        _vet(publisher);
        _seat(opA);
        assertTrue(oa.isAuthorizedOrigin(NS, opA));
    }

    /// The passthrough getter mirrors the installed policy.
    function test_isVettedPublisher_reflectsPolicy() public {
        assertFalse(oa.isVettedPublisher(publisher));
        _vet(publisher);
        assertTrue(oa.isVettedPublisher(publisher));
    }

    /// Swapping the policy changes the decision for FUTURE seating: a publisher
    /// vetted under the old policy can no longer seat once a reject-all policy is
    /// installed. Already-seated origins are untouched (bounded-work invariant).
    function test_setVettingPolicy_swapsTheDecision() public {
        _vet(publisher);
        _seat(opA);

        RejectAllVettingPolicy rejectAll = new RejectAllVettingPolicy();
        vm.expectEmit(true, true, true, true);
        emit OriginAssignment.VettingPolicyUpdated(address(policy), address(rejectAll));
        vm.prank(admin);
        oa.setVettingPolicy(IVettingPolicy(address(rejectAll)));

        assertFalse(oa.isVettedPublisher(publisher), "the new policy vets nobody");
        vm.prank(publisher);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.PublisherNotVetted.selector, publisher));
        oa.addOrigin(NS, opB);

        assertTrue(oa.isAuthorizedOrigin(NS, opA), "the seated origin is untouched by the swap");
    }

    function test_setVettingPolicy_rejectsZero() public {
        vm.prank(admin);
        vm.expectRevert(OriginAssignment.ZeroAddress.selector);
        oa.setVettingPolicy(IVettingPolicy(address(0)));
    }

    /// An EOA (or not-yet-deployed address) is rejected: pointing the policy at
    /// one would make addOrigin revert on the isVetted ABI decode instead.
    function test_setVettingPolicy_rejectsNonContract() public {
        address eoa = address(0xE0A);
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.VettingPolicyNotAContract.selector, eoa));
        oa.setVettingPolicy(IVettingPolicy(eoa));
    }

    /// The constructor applies the same contract guard to the genesis policy.
    function test_constructor_rejectsNonContractPolicy() public {
        address eoa = address(0xE0A);
        vm.expectRevert(abi.encodeWithSelector(OriginAssignment.VettingPolicyNotAContract.selector, eoa));
        new OriginAssignment(bond, registry, address(blacklist), IVettingPolicy(eoa), admin);
    }

    function test_setVettingPolicy_onlyGovernance() public {
        RejectAllVettingPolicy rejectAll = new RejectAllVettingPolicy();
        vm.prank(stranger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        oa.setVettingPolicy(IVettingPolicy(address(rejectAll)));
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
        OriginAssignment oaNoBl =
            new OriginAssignment(bond, registry, address(0), IVettingPolicy(address(policy)), admin);
        _vet(publisher);
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
        OriginAssignment oaNoBl =
            new OriginAssignment(bond, registry, address(0), IVettingPolicy(address(policy)), admin);
        _vet(publisher);
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

        _unvet(publisher);

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
        OriginAssignment oaNoBl =
            new OriginAssignment(bond, registry, address(0), IVettingPolicy(address(policy)), admin);
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

    /// ADR 011 § Contract promises each governance setter emits its own update
    /// event, and the CHANGELOG tells indexers to map those topics. Nothing
    /// asserted the payloads, so a swapped old/new argument — the classic slip
    /// in a two-value update event — would ship silently.
    function test_governanceSetters_emitOldThenNew() public {
        RejectAllVettingPolicy rejectAll = new RejectAllVettingPolicy();
        vm.expectEmit(true, true, true, true);
        emit OriginAssignment.VettingPolicyUpdated(address(policy), address(rejectAll));
        vm.prank(admin);
        oa.setVettingPolicy(IVettingPolicy(address(rejectAll)));

        vm.expectEmit(true, true, true, true);
        emit OriginAssignment.MaxOriginsPerNamespaceUpdated(10, 5);
        vm.prank(admin);
        oa.setMaxOriginsPerNamespace(5);

        OriginAssignment oaNoBl =
            new OriginAssignment(bond, registry, address(0), IVettingPolicy(address(policy)), admin);
        vm.expectEmit(true, true, true, true);
        emit OriginAssignment.ContentBlacklistUpdated(address(0), address(blacklist));
        vm.prank(admin);
        oaNoBl.setContentBlacklist(address(blacklist));
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
        RejectAllVettingPolicy rejectAll = new RejectAllVettingPolicy();
        vm.startPrank(stranger);
        vm.expectRevert(denied);
        oa.setContentBlacklist(address(blacklist));
        vm.expectRevert(denied);
        oa.setVettingPolicy(IVettingPolicy(address(rejectAll)));
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
        (ok, ret) = address(oa).staticcall(abi.encodeWithSelector(bytes4(keccak256("vettingPolicy()"))));
        assertTrue(ok, "vettingPolicy() must exist at the frozen selector");
        assertEq(abi.decode(ret, (address)), address(policy), "and must answer the installed policy");
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
            OriginAssignment.VettingPolicyUpdated.selector,
            keccak256("VettingPolicyUpdated(address,address)"),
            "VettingPolicyUpdated"
        );
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
