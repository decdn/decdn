// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { ICapacityBondEjector } from "../src/interfaces/ICapacityBondEjector.sol";
import { ICapacityBondRegionView } from "../src/interfaces/ICapacityBondRegionView.sol";
import { IPublisherRegistryStanding } from "../src/interfaces/IPublisherRegistryStanding.sol";
import { PublisherRegistry } from "../src/PublisherRegistry.sol";
import { Token } from "../src/Token.sol";

/// @dev Safe-shaped signer enumeration, for the ADR 011 § Signer non-overlap
///      probe in `registerRegionalBody`. Deliberately implements the interface
///      structurally rather than importing it — a real Safe does not import
///      deCDN's interface either, and the probe must work on that basis.
contract MockSigners {
    address[] internal _owners;

    constructor(address[] memory owners_) {
        _owners = owners_;
    }

    function getOwners() external view returns (address[] memory) {
        return _owners;
    }
}

/// @dev A contract at the body address that does NOT expose `getOwners()`.
///      Exercises the ADR 011 fallback: not enumerable is not a failure, it
///      routes disjointness to the off-chain governance obligation.
contract MockOpaqueBody {
    // solhint-disable-next-line no-empty-blocks
    function unrelated() external pure { }
}

/// @dev Doubles as the `ejectNode` sink and the ADR 030 region-scope read source
///      (`ICapacityBondRegionView`) — production `CapacityBond` is both at one
///      address, so the consumer reads region data off the same reference.
contract MockEjector is ICapacityBondEjector, ICapacityBondRegionView {
    address[] public ejected;
    address[] public unEjected;

    mapping(address => string) internal _regionHint;
    mapping(address => string) internal _regionPrev;
    mapping(address => uint64) internal _regionLastChanged;
    mapping(address => uint64) internal _firstBondedAt;
    uint64 public gateActivatedAt;
    uint256 public window = 7 days;

    function ejectNode(address operator) external override {
        ejected.push(operator);
    }

    function unEjectNode(address operator) external override {
        unEjected.push(operator);
    }

    function ejectedCount() external view returns (uint256) {
        return ejected.length;
    }

    function unEjectedCount() external view returns (uint256) {
        return unEjected.length;
    }

    function setRegion(address op, string memory current, string memory prev, uint64 lastChanged) external {
        _regionHint[op] = current;
        _regionPrev[op] = prev;
        _regionLastChanged[op] = lastChanged;
    }

    function setFirstBondedAt(address op, uint64 ts) external {
        _firstBondedAt[op] = ts;
    }

    function setGate(uint64 ts, uint256 window_) external {
        gateActivatedAt = ts;
        window = window_;
    }

    function regionScopeData(address operator)
        external
        view
        override
        returns (string memory, string memory, uint64, uint64, uint64, uint256)
    {
        return (
            _regionHint[operator],
            _regionPrev[operator],
            _regionLastChanged[operator],
            _firstBondedAt[operator],
            gateActivatedAt,
            window
        );
    }
}

/// @title ContentBlacklist smoke tests
/// @notice Critical-path coverage for hash/operator blacklist, cross-contract
///         `ejectNode` wire, ADR 031 appeal state machine happy + reject
///         + reverse paths, and the SF-M1 `MissingRegion` revert.
contract ContentBlacklistTest is Test {
    Token internal token;
    MockEjector internal bondMock;
    PublisherRegistry internal registry;
    ContentBlacklist internal blacklist;

    address internal admin = address(0xA11CE);
    address internal multisig = address(0xC0DE);
    address internal regionalBody = address(0xDE);
    /// @dev A second body, for `REGION_EU`. Distinct from `regionalBody`
    ///      because a body's authority is scoped to exactly one region
    ///      (ADR 011 § Regional Governance Bodies) — the US body cannot write
    ///      EU entries, which is the point of the scoping.
    address internal euBody = address(0xDF);
    address internal filer = address(0xF1);
    address internal operator = address(0xB0B);

    bytes32 internal constant REGION_US = bytes32("US");
    bytes32 internal constant SAMPLE_HASH = bytes32(uint256(0xABCDEF));
    uint256 internal constant APPEAL_BOND = 100e18;

    function setUp() public {
        token = new Token(admin);
        bondMock = new MockEjector();
        registry = new PublisherRegistry(admin);
        blacklist = new ContentBlacklist(
            ICapacityBondEjector(address(bondMock)),
            token,
            IPublisherRegistryStanding(address(registry)),
            admin,
            APPEAL_BOND
        );

        // Fund filer for appeal bonds.
        vm.prank(admin);
        token.transfer(filer, APPEAL_BOND * 10);
        vm.prank(filer);
        token.approve(address(blacklist), type(uint256).max);

        // The appeal-machinery tests below file under `StandingPath.Operator` on
        // `REGION_US` entries, so the ADR 011 path-2 region match requires the
        // filer's current attested region to be "US". (Region-match revert / soft
        // norm behavior is covered explicitly in the path-2 tests.)
        bondMock.setRegion(filer, "US", "", 0);

        // Grant the multisig role, then register the regional body FOR ITS
        // REGION. A bare `grantRole(REGIONAL_BODY_ROLE, …)` is no longer
        // sufficient authority to write entries — ADR 011 § Regional Governance
        // Bodies scopes a body to its own jurisdiction, so `addHashRegional`
        // also requires the caller to be the body bound to the named region.
        // `multisig` is an EOA here, so the signer-disjointness probe finds no
        // enumerable owner set and falls back to the off-chain regime.
        bytes32 multisigRole = blacklist.EMERGENCY_MULTISIG_ROLE();
        vm.startPrank(admin);
        blacklist.grantRole(multisigRole, multisig);
        blacklist.registerRegionalBody(REGION_US, regionalBody, multisig);
        blacklist.registerRegionalBody(REGION_EU, euBody, multisig);
        vm.stopPrank();
    }

    function test_addHashGlobal_andQuery() public {
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-TEST");
        assertTrue(blacklist.isHashBlacklisted(SAMPLE_HASH));
    }

    function test_addHashRegional_globalTakesPrecedence() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        assertTrue(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));
        assertFalse(blacklist.isHashBlacklisted(SAMPLE_HASH));
    }

    /// @notice ADR 011 § Blacklist version — `getBlacklistVersion()` starts at
    ///         zero and advances by exactly one on every hash add and every hash
    ///         removal, on both the global and the regional path. Nodes poll this
    ///         instead of replaying the full event history. The appeal paths
    ///         (ratification-removal, suspend, resume) are covered separately by
    ///         the version tests below — this one drives no appeal at all.
    function test_getBlacklistVersion_bumpsOnEveryAddAndRemove() public {
        assertEq(blacklist.getBlacklistVersion(), 0);

        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-TEST");
        assertEq(blacklist.getBlacklistVersion(), 1);

        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        assertEq(blacklist.getBlacklistVersion(), 2);

        vm.prank(admin);
        blacklist.removeHashGlobal(SAMPLE_HASH);
        assertEq(blacklist.getBlacklistVersion(), 3);

        vm.prank(regionalBody);
        blacklist.removeHashRegional(REGION_US, SAMPLE_HASH);
        assertEq(blacklist.getBlacklistVersion(), 4);

        // Re-adding an already-removed hash is a fresh add, so it bumps again —
        // the counter tracks operations, not the live entry count.
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-TEST-2");
        assertEq(blacklist.getBlacklistVersion(), 5);
    }

    /// @notice The counter must not move for anything that leaves the enforced
    ///         hash set alone: operator + origin blacklisting, governance
    ///         setters, and reads. A spurious bump costs every node a delta
    ///         fetch. (The `suspended` toggle DOES move it — it changes what
    ///         `isHashBlacklisted*` reports; see the suspension tests below.)
    function test_getBlacklistVersion_unaffectedByNonEntryOperations() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        uint256 versionAfterAdd = blacklist.getBlacklistVersion();
        assertEq(versionAfterAdd, 1);

        vm.startPrank(admin);
        blacklist.addOperator(operator);
        blacklist.removeOperator(operator);
        blacklist.setOriginBlacklist(address(0xBEEF), true);
        blacklist.setAppealBond(200e18);
        blacklist.setRejectionCooldownWindow(30 days);
        vm.stopPrank();
        assertEq(blacklist.getBlacklistVersion(), versionAfterAdd);

        // Reads never bump.
        blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US);
        blacklist.getHashEntry(REGION_US, SAMPLE_HASH);
        assertEq(blacklist.getBlacklistVersion(), versionAfterAdd);

        // Merely filing an appeal changes nothing enforceable — no bump.
        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("evidence"), ContentBlacklist.StandingPath.Operator, 0
        );
        assertEq(blacklist.getBlacklistVersion(), versionAfterAdd);

        // Fast-tracking suspends the entry, and that is itself a bump — which is
        // why the ratification assertion below must be relative to a version
        // captured *here* rather than to `versionAfterAdd`.
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);
        uint256 versionAfterSuspend = blacklist.getBlacklistVersion();
        assertEq(versionAfterSuspend, versionAfterAdd + 1);

        // Ratification removes the entry, so that one bumps too.
        vm.prank(admin);
        blacklist.ratifyBlacklistAppealRemoval(appealId);
        assertEq(blacklist.getHashEntry(REGION_US, SAMPLE_HASH).addedAt, 0);
        assertEq(blacklist.getBlacklistVersion(), versionAfterSuspend + 1);
    }

    /// @notice Suspension flips `isHashBlacklisted*` to false, so it must bump the
    ///         counter — a node that missed it would keep enforcing a hash the
    ///         contract no longer considers blacklisted, blocking content and
    ///         exposing operators to slashing for serving it.
    function test_getBlacklistVersion_bumpsOnSuspend() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        uint256 versionAfterAdd = blacklist.getBlacklistVersion();

        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("evidence"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);

        assertTrue(blacklist.getHashEntry(REGION_US, SAMPLE_HASH).suspended);
        assertFalse(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));
        assertEq(blacklist.getBlacklistVersion(), versionAfterAdd + 1);
    }

    /// @notice Resumption re-arms enforcement, and ADR 011 § Authority and flow /
    ///         § Compliance Window have operators detect it off exactly this
    ///         counter. Without the bump a
    ///         node would silently under-enforce a live entry — the compliance
    ///         failure the poll cycle exists to prevent.
    function test_getBlacklistVersion_bumpsOnResume() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        uint256 versionAfterAdd = blacklist.getBlacklistVersion();

        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("evidence"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);

        // Reversal clears `suspended`: the entry is enforced again.
        vm.prank(admin);
        blacklist.reverseBlacklistAppeal(appealId);

        assertFalse(blacklist.getHashEntry(REGION_US, SAMPLE_HASH).suspended);
        assertTrue(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));
        assertEq(blacklist.getBlacklistVersion(), versionAfterAdd + 2);
    }

    /// @notice ADR 011 § Polling — `HashRemoved` carries the post-change version
    ///         so a delta consumer can key the removal to the counter it polled.
    function test_HashRemoved_carriesVersion() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");

        uint256 expectedVersion = blacklist.getBlacklistVersion() + 1;
        vm.expectEmit(true, true, false, true, address(blacklist));
        emit ContentBlacklist.HashRemoved(REGION_US, SAMPLE_HASH, expectedVersion);
        vm.prank(regionalBody);
        blacklist.removeHashRegional(REGION_US, SAMPLE_HASH);
    }

    /// @notice ADR 011 § Polling — every `_blacklistVersion` bump has a matching
    ///         log. Suspend and resume each emit `HashSuspensionUpdated` carrying
    ///         the post-change version and the new `suspended` state, so a
    ///         delta-fetcher never sees the counter move with no event (the
    ///         silent-under-enforcement failure the poll cycle exists to prevent).
    function test_HashSuspensionUpdated_onSuspendAndResume() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");

        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("evidence"), ContentBlacklist.StandingPath.Operator, 0
        );

        // Fast-track suspends the entry: version bumps, state -> true.
        uint256 suspendVersion = blacklist.getBlacklistVersion() + 1;
        vm.expectEmit(true, true, false, true, address(blacklist));
        emit ContentBlacklist.HashSuspensionUpdated(REGION_US, SAMPLE_HASH, suspendVersion, true);
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);

        // Reversal resumes it: version bumps again, state -> false.
        uint256 resumeVersion = blacklist.getBlacklistVersion() + 1;
        vm.expectEmit(true, true, false, true, address(blacklist));
        emit ContentBlacklist.HashSuspensionUpdated(REGION_US, SAMPLE_HASH, resumeVersion, false);
        vm.prank(admin);
        blacklist.reverseBlacklistAppeal(appealId);
    }

    /// @notice Rejecting a fast-tracked appeal also un-suspends the entry, so it
    ///         resumes enforcement and must bump the counter — the same
    ///         resume-side compliance case as `bumpsOnResume`, reached via
    ///         `rejectBlacklistAppeal` rather than `reverseBlacklistAppeal`. All
    ///         four `_setEntrySuspended(false)` clear paths (reverse, reject,
    ///         perjury-reject, cleanup-lapse) route through the same choke point;
    ///         this guards the reject path against a future re-inline dropping it.
    function test_getBlacklistVersion_bumpsOnRejectAfterFastTrack() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        uint256 versionAfterAdd = blacklist.getBlacklistVersion();

        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("evidence"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);

        // Reject clears `suspended`: the entry is enforced again.
        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(appealId);

        assertFalse(blacklist.getHashEntry(REGION_US, SAMPLE_HASH).suspended);
        assertTrue(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));
        assertEq(blacklist.getBlacklistVersion(), versionAfterAdd + 2);
    }

    function test_addOperator_callsEjectOnCapacityBond() public {
        vm.prank(admin);
        blacklist.addOperator(operator);
        assertTrue(blacklist.isOperatorBlacklisted(operator));
        assertEq(bondMock.ejectedCount(), 1);
        assertEq(bondMock.ejected(0), operator);
    }

    function test_removeOperator_callsUnEjectOnCapacityBond() public {
        vm.prank(admin);
        blacklist.addOperator(operator);
        assertEq(bondMock.ejectedCount(), 1);
        // `addOperator` ejects but must NOT un-eject.
        assertEq(bondMock.unEjectedCount(), 0);

        vm.prank(admin);
        blacklist.removeOperator(operator);
        assertFalse(blacklist.isOperatorBlacklisted(operator));
        assertEq(bondMock.unEjectedCount(), 1);
        assertEq(bondMock.unEjected(0), operator);
    }

    /// @notice `removeOperator` calls `unEjectNode` UNCONDITIONALLY (idempotent)
    ///         so it can re-sync a `CapacityBond` latch that was set without a
    ///         matching local entry — even when the local flag is already clear.
    function test_removeOperator_notBlacklisted_stillResyncsLatch() public {
        vm.prank(admin);
        blacklist.removeOperator(operator);
        assertFalse(blacklist.isOperatorBlacklisted(operator));
        assertEq(bondMock.unEjectedCount(), 1);
        assertEq(bondMock.unEjected(0), operator);
    }

    function test_removeOperator_revertsZeroAddress() public {
        vm.prank(admin);
        vm.expectRevert(ContentBlacklist.ZeroAddress.selector);
        blacklist.removeOperator(address(0));
    }

    function test_openBlacklistAppeal_revertsOnZeroRegion() public {
        // SF-M1 fix: must pass region explicitly.
        vm.prank(filer);
        vm.expectRevert();
        blacklist.openBlacklistAppeal(SAMPLE_HASH, bytes32(0), bytes32("e"), ContentBlacklist.StandingPath.Operator, 0);
    }

    function test_openBlacklistAppeal_revertsOnEntryNotBlacklisted() public {
        vm.prank(filer);
        vm.expectRevert();
        blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0);
    }

    function test_fullAppealFlow_ratifyRemovesHash() public {
        // Seed regional blacklist.
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");

        // Filer opens appeal.
        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("evidence"), ContentBlacklist.StandingPath.Operator, 0
        );

        // Multisig fast-tracks (entry.suspended = true).
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);
        assertFalse(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));

        // Governance ratifies, hash removed.
        uint256 filerTokenBefore = token.balanceOf(filer);
        vm.prank(admin);
        blacklist.ratifyBlacklistAppealRemoval(appealId);
        // Bond refunded.
        assertEq(token.balanceOf(filer) - filerTokenBefore, APPEAL_BOND);
        // Entry deleted.
        assertEq(blacklist.getHashEntry(REGION_US, SAMPLE_HASH).addedAt, 0);
    }

    function test_rejectAppeal_burnsBond() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );

        uint256 supplyBefore = token.totalSupply();
        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(appealId);
        assertEq(supplyBefore - token.totalSupply(), APPEAL_BOND);
    }

    function test_reverseAppeal_clearsSuspendedAndBurnsBond() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);
        // Suspended → not enforced.
        assertFalse(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));

        uint256 supplyBefore = token.totalSupply();
        vm.prank(admin);
        blacklist.reverseBlacklistAppeal(appealId);
        // Suspended cleared → blacklist live again.
        assertTrue(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));
        assertEq(supplyBefore - token.totalSupply(), APPEAL_BOND);
    }

    /// @notice M-1 — the per-region concurrent cap counts only FAST-TRACKED
    ///         appeals (the ones holding interim relief), not Open ones. Open
    ///         filings no longer crowd out others, but simultaneous suspensions
    ///         per region stay bounded.
    /// @notice Fund a second filer and attest it to REGION_US so it has
    ///         StandingPath.Operator standing on US entries.
    function _fundedFiler(address who) internal returns (address) {
        vm.prank(admin);
        token.transfer(who, APPEAL_BOND * 10);
        vm.prank(who);
        token.approve(address(blacklist), type(uint256).max);
        bondMock.setRegion(who, "US", "", 0);
        return who;
    }

    function test_regionalCap_countsFastTrackedNotOpen() public {
        // The per-filer sub-cap (2) is below the region ceiling (3), so filling a
        // region now requires two filers — one filer alone tops out at the sub-cap.
        address filerB = _fundedFiler(address(0xF2));

        // Seed 4 hashes in REGION_US.
        for (uint256 i = 0; i < 4; i++) {
            vm.prank(regionalBody);
            blacklist.addHashRegional(REGION_US, bytes32(i + 1), "DMCA-TEST");
        }
        // Open 4 appeals — ALL succeed (Open appeals do not charge the cap):
        // filer takes 1,2; filerB takes 3,4.
        uint256[4] memory ids;
        for (uint256 i = 0; i < 2; i++) {
            vm.prank(filer);
            ids[i] = blacklist.openBlacklistAppeal(
                bytes32(i + 1), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
            );
        }
        for (uint256 i = 2; i < 4; i++) {
            vm.prank(filerB);
            ids[i] = blacklist.openBlacklistAppeal(
                bytes32(i + 1), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
            );
        }
        // Fast-track 3 (filer: 2 — its sub-cap; filerB: 1) — each charges the
        // per-region relief cap, filling the region (3).
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(ids[0]);
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(ids[1]);
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(ids[2]);
        // The 4th fast-track (filerB's 2nd) hits the REGION ceiling — filerB is
        // still under the per-filer sub-cap, so it is the region cap that blocks.
        vm.prank(multisig);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.RegionalCapHit.selector, REGION_US, uint256(3)));
        blacklist.fastTrackBlacklistAppeal(ids[3]);
    }

    /// @notice H-2 — re-adding a hash while an appeal is live must revert
    ///         rather than silently un-suspend and orphan the in-flight appeal
    ///         (and reset the slash-eligibility boundary).
    function test_addHash_revertsWhileAppealActive() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        assertTrue(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));

        // Re-add blocked while the appeal is live.
        vm.prank(regionalBody);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.HashHasActiveAppeal.selector, REGION_US, SAMPLE_HASH));
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");

        // Once the appeal terminates, the re-add is allowed again.
        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(appealId);
        assertFalse(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
    }

    /// @notice T-3 — concurrent appeals on the same (region, hash) revert.
    function test_openBlacklistAppeal_revertsDuplicatePerHash() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");

        vm.prank(filer);
        blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e1"), ContentBlacklist.StandingPath.Operator, 0);

        // Fund a second filer; even with a different filer, same (region, hash)
        // can't have a second active appeal.
        address filer2 = address(0xF2);
        vm.prank(admin);
        token.transfer(filer2, APPEAL_BOND);
        vm.prank(filer2);
        token.approve(address(blacklist), type(uint256).max);

        vm.prank(filer2);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.AppealAlreadyActive.selector, REGION_US, SAMPLE_HASH));
        blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e2"), ContentBlacklist.StandingPath.Operator, 0);

        assertTrue(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
    }

    /// @notice T-3 — `hasActiveAppeal` clears on reject, allowing a new
    ///         appeal to be filed afterwards.
    function test_hasActiveAppeal_clearedOnReject() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(appealId);

        assertFalse(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));

        // A follow-up open should succeed (freq cap doesn't apply since the
        // prior appeal was rejected, not ratified).
        vm.prank(filer);
        blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32("e2"), ContentBlacklist.StandingPath.Operator, 0);
        assertTrue(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
    }

    /// @notice T-3 — `hasActiveAppeal` clears on reverse.
    function test_hasActiveAppeal_clearedOnReverse() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);
        vm.prank(admin);
        blacklist.reverseBlacklistAppeal(appealId);
        assertFalse(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
    }

    // -----------------------------------------------------------------
    // Access-control guards on governance / regional-body setters
    // -----------------------------------------------------------------

    function test_addHashGlobal_revertsWithoutGovernanceRole() public {
        _expectMissingRole(filer, blacklist.GOVERNANCE_ROLE());
        vm.prank(filer);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-TEST");
    }

    function test_addHashRegional_revertsWithoutRegionalBodyRole() public {
        _expectMissingRole(filer, blacklist.REGIONAL_BODY_ROLE());
        vm.prank(filer);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
    }

    function test_registerRegionalBody_revertsWithoutGovernanceRole() public {
        _expectMissingRole(filer, blacklist.GOVERNANCE_ROLE());
        vm.prank(filer);
        blacklist.registerRegionalBody(bytes32("FR"), address(0xBEEF), multisig);
    }

    function test_setAppealBond_revertsWithoutGovernanceRole() public {
        _expectMissingRole(filer, blacklist.GOVERNANCE_ROLE());
        vm.prank(filer);
        blacklist.setAppealBond(APPEAL_BOND * 2);
    }

    /// @dev See `CapacityBond.t.sol:_expectMissingRole` for rationale.
    function _expectMissingRole(address caller, bytes32 role) internal {
        vm.expectRevert(abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, caller, role));
    }

    /// @notice `REGIONAL_BODY_ROLE` must NOT be able to remove `GLOBAL_REGION`
    ///         entries — that would let any regional body bypass governance.
    ///         Mirrors the contract comment on `removeHashRegional`.
    function test_removeHashRegional_blocksGlobalRegionEntries() public {
        // Seed a global entry.
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-TEST");

        // Regional body tries to delete via removeHashRegional with the
        // GLOBAL_REGION sentinel (must revert with MissingRegion).
        bytes32 globalRegion = bytes32("GLOBAL");
        vm.prank(regionalBody);
        vm.expectRevert(ContentBlacklist.MissingRegion.selector);
        blacklist.removeHashRegional(globalRegion, SAMPLE_HASH);

        // Entry remains live.
        assertTrue(blacklist.isHashBlacklisted(SAMPLE_HASH));
    }

    /// @notice T-3 — `hasActiveAppeal` clears on lapse from Open and
    ///         FastTracked branches.
    function test_hasActiveAppeal_clearedOnLapseOpen() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.warp(block.timestamp + 15 days);
        blacklist.cleanupExpiredBlacklistAppeal(appealId);
        assertFalse(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
    }

    // -----------------------------------------------------------------
    // ADR 011 § Standing path-2 (Operator) region match (ADR 030)
    // -----------------------------------------------------------------

    bytes32 internal constant REGION_EU = bytes32("EU");

    function test_openBlacklistAppeal_operatorRegionMismatch_reverts() public {
        // filer's attested region is "US" (set in setUp); appealing an "EU"
        // regional entry under Operator standing must hard-revert.
        vm.prank(euBody);
        blacklist.addHashRegional(REGION_EU, SAMPLE_HASH, "DMCA-TEST");
        vm.prank(filer);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.OperatorRegionMismatch.selector, REGION_EU, REGION_US));
        blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_EU, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0);
    }

    function test_openBlacklistAppeal_inWindowFiling_notAutoRejected() public {
        vm.warp(30 days); // headroom so the `- 1 days` stamp below doesn't underflow
        // filer flipped US -> EU one day ago (region change has NOT ripened).
        // A path-2 filing on the NEW current region (EU) must NOT be auto-rejected
        // — the ripening window is a soft norm left to multisig (ADR 011).
        bondMock.setRegion(filer, "EU", "US", uint64(block.timestamp - 1 days));
        vm.prank(euBody);
        blacklist.addHashRegional(REGION_EU, SAMPLE_HASH, "DMCA-TEST");
        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_EU, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        assertTrue(blacklist.hasActiveAppeal(REGION_EU, SAMPLE_HASH));
        assertEq(appealId, 0);
    }

    function test_openBlacklistAppeal_globalEntry_skipsRegionCheck() public {
        // Operator standing on a GLOBAL entry: region match is not applied
        // (an in-scope operator can appeal a global entry regardless of region).
        bondMock.setRegion(filer, "ZZ", "", 0); // deliberately non-matching
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-TEST");
        bytes32 globalRegion = bytes32("GLOBAL");
        vm.prank(filer);
        blacklist.openBlacklistAppeal(
            SAMPLE_HASH, globalRegion, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        assertTrue(blacklist.hasActiveAppeal(globalRegion, SAMPLE_HASH));
    }

    function test_openBlacklistAppeal_nonOperatorStanding_skipsRegionCheck() public {
        // Publisher standing is not region-gated even on a mismatched region.
        vm.prank(euBody);
        blacklist.addHashRegional(REGION_EU, SAMPLE_HASH, "DMCA-TEST"); // filer region is "US"
        // Establish Publisher standing: filer owns a namespace that claimed the hash.
        vm.prank(filer);
        uint256 nsId = registry.createNamespace();
        vm.prank(filer);
        registry.claimContent(nsId, SAMPLE_HASH);
        vm.prank(filer);
        blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_EU, bytes32("e"), ContentBlacklist.StandingPath.Publisher, nsId
        );
        assertTrue(blacklist.hasActiveAppeal(REGION_EU, SAMPLE_HASH));
    }

    // -----------------------------------------------------------------
    // isHashBlacklistedForOperator read view (ADR 030 ripening predicate)
    // -----------------------------------------------------------------

    function test_isHashBlacklistedForOperator_global() public {
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-TEST");
        // Global applies to any operator regardless of region.
        assertTrue(blacklist.isHashBlacklistedForOperator(SAMPLE_HASH, operator));
    }

    function test_isHashBlacklistedForOperator_currentRegion() public {
        bondMock.setRegion(operator, "US", "", 0);
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        assertTrue(blacklist.isHashBlacklistedForOperator(SAMPLE_HASH, operator));
        // An operator in a different region is out of scope.
        bondMock.setRegion(operator, "EU", "", 0);
        assertFalse(blacklist.isHashBlacklistedForOperator(SAMPLE_HASH, operator));
    }

    function test_isHashBlacklistedForOperator_suspendedRegional_false() public {
        bondMock.setRegion(operator, "US", "", 0);
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        assertTrue(blacklist.isHashBlacklistedForOperator(SAMPLE_HASH, operator));
        // Fast-tracking an appeal suspends the entry → lifts it from scope.
        vm.prank(filer); // filer region "US" (setUp) satisfies path-2 standing
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);
        assertFalse(blacklist.isHashBlacklistedForOperator(SAMPLE_HASH, operator));
    }

    function test_isHashBlacklistedForOperator_prevRegionWindow() public {
        vm.warp(30 days); // headroom for the `- N days` stamps below
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        // Flipped US -> EU one day ago: prev (US) entry still in scope.
        bondMock.setRegion(operator, "EU", "US", uint64(block.timestamp - 1 days));
        assertTrue(blacklist.isHashBlacklistedForOperator(SAMPLE_HASH, operator));
        // Flip 8 days ago: ripened, prev no longer in scope.
        bondMock.setRegion(operator, "EU", "US", uint64(block.timestamp - 8 days));
        assertFalse(blacklist.isHashBlacklistedForOperator(SAMPLE_HASH, operator));
    }

    function test_isHashBlacklistedForOperator_neverChangedFallback_ripensFromMaxBondGate() public {
        vm.warp(30 days); // headroom for the `- N days` stamps below
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        // `regionLastChanged == 0` (never changed on-chain) routes the window
        // through the `max(firstBondedAt, gate)` fallback — the path the
        // lastChanged tests above never reach via the real `regionScopeData` read.
        // The gate (1d ago) is the more-recent stamp; a stale `firstBondedAt` (10d
        // ago) would have closed the 7d window. Prev (US) entry still applies →
        // true, proving the fallback selected the gate (the `max`).
        bondMock.setRegion(operator, "EU", "US", 0);
        bondMock.setFirstBondedAt(operator, uint64(block.timestamp - 10 days));
        bondMock.setGate(uint64(block.timestamp - 1 days), 7 days);
        assertTrue(blacklist.isHashBlacklistedForOperator(SAMPLE_HASH, operator));
        // Now `firstBondedAt` (8d ago) is the `max` and the gate is older still
        // (9d); 8d ≥ the 7d window → prev US ripened out, current EU has no entry
        // → false. Proves the window closes the fallback and selects `firstBondedAt`.
        bondMock.setFirstBondedAt(operator, uint64(block.timestamp - 8 days));
        bondMock.setGate(uint64(block.timestamp - 9 days), 7 days);
        assertFalse(blacklist.isHashBlacklistedForOperator(SAMPLE_HASH, operator));
    }

    // -----------------------------------------------------------------
    // ADR 031 § APPEAL_FILER_REJECTION_COOLDOWN (audit finding H-4)
    // -----------------------------------------------------------------

    /// @dev Seeds a fresh REGION_US entry for `h`, has `filer` open an appeal
    ///      on it, and has the multisig reject that appeal.
    function _openAndReject(bytes32 h) internal {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, h, "DMCA-TEST");
        vm.prank(filer);
        uint256 id =
            blacklist.openBlacklistAppeal(h, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0);
        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(id);
    }

    /// @notice Three in-window rejections lock the filer out of
    ///         `openBlacklistAppeal` for a full window — the bond is no longer
    ///         the only brake on rejected-appeal spam across distinct entries.
    function test_rejectionCooldown_locksOutAfterThreeRejections() public {
        _openAndReject(bytes32(uint256(1)));
        _openAndReject(bytes32(uint256(2)));
        // Two rejections: not yet locked out.
        assertEq(blacklist.getFilerRejectionWindow(filer).cooldownUntilAt, 0);

        _openAndReject(bytes32(uint256(3)));
        uint64 cooldownUntil = blacklist.getFilerRejectionWindow(filer).cooldownUntilAt;
        assertEq(cooldownUntil, uint64(block.timestamp) + 90 days);

        // A fresh appeal on a distinct live entry is rejected at intake.
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(4)), "DMCA-TEST");
        vm.prank(filer);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.FilerInRejectionCooldown.selector, cooldownUntil));
        blacklist.openBlacklistAppeal(
            bytes32(uint256(4)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
    }

    /// @notice Two rejections never trip the cooldown — an occasional rejected
    ///         appeal does not penalize an otherwise legitimate filer.
    function test_rejectionCooldown_twoRejectionsDoNotLockOut() public {
        _openAndReject(bytes32(uint256(1)));
        _openAndReject(bytes32(uint256(2)));
        assertEq(blacklist.getFilerRejectionWindow(filer).cooldownUntilAt, 0);

        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(3)), "DMCA-TEST");
        vm.prank(filer);
        blacklist.openBlacklistAppeal(
            bytes32(uint256(3)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        assertTrue(blacklist.hasActiveAppeal(REGION_US, bytes32(uint256(3))));
    }

    /// @notice The cooldown lifts once the window elapses; the filer can file
    ///         again afterwards.
    function test_rejectionCooldown_liftsAfterWindow() public {
        _openAndReject(bytes32(uint256(1)));
        _openAndReject(bytes32(uint256(2)));
        _openAndReject(bytes32(uint256(3)));
        uint64 cooldownUntil = blacklist.getFilerRejectionWindow(filer).cooldownUntilAt;
        assertGt(cooldownUntil, 0);

        vm.warp(uint256(cooldownUntil) + 1);
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(4)), "DMCA-TEST");
        vm.prank(filer);
        blacklist.openBlacklistAppeal(
            bytes32(uint256(4)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        assertTrue(blacklist.hasActiveAppeal(REGION_US, bytes32(uint256(4))));
    }

    /// @notice The rolling window resets: rejections that age out of the window
    ///         never accumulate to three, so a later rejection does not lock out.
    function test_rejectionCooldown_rollingWindowResets() public {
        _openAndReject(bytes32(uint256(1)));
        _openAndReject(bytes32(uint256(2)));
        // Let the 90-day window roll past the first two rejections.
        vm.warp(block.timestamp + 91 days);
        _openAndReject(bytes32(uint256(3)));
        // The two stale rejections aged out → the third does NOT trip a cooldown.
        assertEq(blacklist.getFilerRejectionWindow(filer).cooldownUntilAt, 0);

        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(4)), "DMCA-TEST");
        vm.prank(filer);
        blacklist.openBlacklistAppeal(
            bytes32(uint256(4)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        assertTrue(blacklist.hasActiveAppeal(REGION_US, bytes32(uint256(4))));
    }

    /// @notice One filer's cooldown does not affect a different, well-behaved
    ///         filer — the throttle is strictly per-filer.
    function test_rejectionCooldown_doesNotAffectOtherFilers() public {
        _openAndReject(bytes32(uint256(1)));
        _openAndReject(bytes32(uint256(2)));
        _openAndReject(bytes32(uint256(3)));
        assertGt(blacklist.getFilerRejectionWindow(filer).cooldownUntilAt, 0);

        address filer2 = address(0xF2);
        vm.prank(admin);
        token.transfer(filer2, APPEAL_BOND);
        vm.prank(filer2);
        token.approve(address(blacklist), type(uint256).max);
        bondMock.setRegion(filer2, "US", "", 0);

        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(4)), "DMCA-TEST");
        vm.prank(filer2);
        blacklist.openBlacklistAppeal(
            bytes32(uint256(4)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        assertTrue(blacklist.hasActiveAppeal(REGION_US, bytes32(uint256(4))));
        assertEq(blacklist.getFilerRejectionWindow(filer2).cooldownUntilAt, 0);
    }

    /// @notice The lockout consults the governance-tunable window: tightening it
    ///         lets a wider rejection spacing avoid a lockout the default would
    ///         have triggered.
    function test_rejectionCooldown_usesGovernedWindow() public {
        vm.prank(admin);
        blacklist.setRejectionCooldownWindow(1 days);

        _openAndReject(bytes32(uint256(1)));
        _openAndReject(bytes32(uint256(2)));
        vm.warp(block.timestamp + 2 days);
        _openAndReject(bytes32(uint256(3)));
        // Beyond the 1-day governed window the third rejection ages the oldest
        // out, so no lockout — the default 90-day window would have locked out.
        assertEq(blacklist.getFilerRejectionWindow(filer).cooldownUntilAt, 0);
    }

    function test_setRejectionCooldownWindow_updatesValue() public {
        vm.prank(admin);
        blacklist.setRejectionCooldownWindow(30 days);
        assertEq(blacklist.rejectionCooldownWindow(), 30 days);
    }

    function test_setRejectionCooldownWindow_revertsOutOfBounds() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                ContentBlacklist.ParamOutOfBounds.selector, uint256(366 days), uint256(1 days), uint256(365 days)
            )
        );
        blacklist.setRejectionCooldownWindow(366 days);
    }

    function test_setRejectionCooldownWindow_revertsWithoutGovernanceRole() public {
        _expectMissingRole(filer, blacklist.GOVERNANCE_ROLE());
        vm.prank(filer);
        blacklist.setRejectionCooldownWindow(30 days);
    }

    // -----------------------------------------------------------------
    // ADR 031 § Evidence — perjury denylist (rejectAppealAsPerjury)
    // -----------------------------------------------------------------

    function _open(bytes32 h) internal returns (uint256 appealId) {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, h, "DMCA-TEST");
        vm.prank(filer);
        appealId = blacklist.openBlacklistAppeal(h, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0);
    }

    /// @notice Perjury-reject sets the 365-day denylist, burns the bond, and
    ///         finalizes the appeal as Rejected.
    function test_rejectAppealAsPerjury_denylistsBurnsAndFinalizes() public {
        uint256 appealId = _open(bytes32(uint256(1)));
        uint64 expectedUntil = uint64(block.timestamp) + 365 days;

        uint256 supplyBefore = token.totalSupply();
        // Both the perjury-specific event and the standard rejection event fire,
        // in that order.
        vm.expectEmit(true, true, false, true, address(blacklist));
        emit ContentBlacklist.BlacklistAppealRejectedAsPerjury(appealId, filer, expectedUntil);
        vm.expectEmit(true, false, false, true, address(blacklist));
        emit ContentBlacklist.BlacklistAppealRejected(appealId, APPEAL_BOND);
        vm.prank(multisig);
        blacklist.rejectAppealAsPerjury(appealId);

        // Bond burned.
        assertEq(supplyBefore - token.totalSupply(), APPEAL_BOND);
        // Appeal finalized as Rejected, bond zeroed.
        ContentBlacklist.BlacklistAppeal memory a = blacklist.getAppeal(appealId);
        assertEq(uint8(a.status), uint8(ContentBlacklist.AppealStatus.Rejected));
        assertEq(a.bond, 0);
        // Denylist set 365 days out.
        assertEq(blacklist.perjuryDenylistUntilAt(filer), expectedUntil);
        // Active-appeal flag cleared so the entry can be re-acted upon.
        assertFalse(blacklist.hasActiveAppeal(REGION_US, bytes32(uint256(1))));
    }

    /// @notice A denylisted filer is blocked from `openBlacklistAppeal` until the
    ///         365-day window lapses, then can file again.
    function test_rejectAppealAsPerjury_blocksFilerUntilWindowLapses() public {
        uint256 appealId = _open(bytes32(uint256(1)));
        vm.prank(multisig);
        blacklist.rejectAppealAsPerjury(appealId);
        uint64 until = blacklist.perjuryDenylistUntilAt(filer);

        // A fresh appeal on a distinct live entry reverts while denylisted.
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(2)), "DMCA-TEST");
        vm.prank(filer);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.FilerPerjuryDenylisted.selector, until));
        blacklist.openBlacklistAppeal(
            bytes32(uint256(2)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );

        // One second before expiry: still locked out. Re-add to refresh the
        // 14-day filing window (the entry above ages out across the warp).
        vm.warp(uint256(until) - 1);
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(2)), "DMCA-TEST");
        vm.prank(filer);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.FilerPerjuryDenylisted.selector, until));
        blacklist.openBlacklistAppeal(
            bytes32(uint256(2)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );

        // Exactly at `until` the lockout lifts — pins the exclusive
        // `perjuryUntil > block.timestamp` boundary (a `>=` impl would still
        // revert here). The entry from `until - 1` is still inside its filing window.
        vm.warp(until);
        vm.prank(filer);
        blacklist.openBlacklistAppeal(
            bytes32(uint256(2)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        assertTrue(blacklist.hasActiveAppeal(REGION_US, bytes32(uint256(2))));
    }

    /// @notice Only `EMERGENCY_MULTISIG_ROLE` may perjury-reject.
    function test_rejectAppealAsPerjury_revertsWithoutMultisigRole() public {
        uint256 appealId = _open(bytes32(uint256(1)));
        _expectMissingRole(filer, blacklist.EMERGENCY_MULTISIG_ROLE());
        vm.prank(filer);
        blacklist.rejectAppealAsPerjury(appealId);
    }

    /// @notice Perjury-rejecting a fast-tracked appeal also un-suspends the entry
    ///         and releases the per-region relief slot (mirrors rejectBlacklistAppeal).
    function test_rejectAppealAsPerjury_fastTracked_unsuspendsAndReleasesCap() public {
        uint256 appealId = _open(bytes32(uint256(1)));
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);
        // Suspended → entry not live; relief slot charged.
        assertFalse(blacklist.isHashBlacklistedInRegion(bytes32(uint256(1)), REGION_US));
        assertEq(blacklist.regionActiveReliefCount(REGION_US), 1);
        uint256 versionAfterSuspend = blacklist.getBlacklistVersion();

        vm.prank(multisig);
        blacklist.rejectAppealAsPerjury(appealId);

        // Un-suspended → entry live again; relief slot released. Resumption is a
        // change to the enforced set, so it bumps the version (ADR 011 § Polling).
        assertTrue(blacklist.isHashBlacklistedInRegion(bytes32(uint256(1)), REGION_US));
        assertEq(blacklist.getBlacklistVersion(), versionAfterSuspend + 1);
        assertEq(blacklist.regionActiveReliefCount(REGION_US), 0);
        assertEq(blacklist.perjuryDenylistUntilAt(filer), uint64(block.timestamp) + 365 days);
    }

    /// @notice One filer's perjury denylist does not affect another filer.
    function test_rejectAppealAsPerjury_isolatedPerFiler() public {
        uint256 appealId = _open(bytes32(uint256(1)));
        vm.prank(multisig);
        blacklist.rejectAppealAsPerjury(appealId);

        address filer2 = address(0xF2);
        vm.prank(admin);
        token.transfer(filer2, APPEAL_BOND);
        vm.prank(filer2);
        token.approve(address(blacklist), type(uint256).max);
        bondMock.setRegion(filer2, "US", "", 0);

        assertEq(blacklist.perjuryDenylistUntilAt(filer2), 0);
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(2)), "DMCA-TEST");
        vm.prank(filer2);
        blacklist.openBlacklistAppeal(
            bytes32(uint256(2)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        assertTrue(blacklist.hasActiveAppeal(REGION_US, bytes32(uint256(2))));
    }

    /// @notice Perjury-reject requires an Open or FastTracked appeal — a
    ///         terminal appeal reverts (reuses the rejectBlacklistAppeal guard).
    function test_rejectAppealAsPerjury_revertsOnTerminalAppeal() public {
        uint256 appealId = _open(bytes32(uint256(1)));
        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(appealId);
        // Already Rejected → revert.
        vm.prank(multisig);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.AppealNotOpen.selector, appealId));
        blacklist.rejectAppealAsPerjury(appealId);
    }

    // -----------------------------------------------------------------
    // ADR 031 — per-(filer, region) interim-relief sub-cap
    // -----------------------------------------------------------------

    /// @notice Fast-track `filer` to its per-(filer, region) sub-cap (2) in
    ///         REGION_US, returning the two fast-tracked appeal ids.
    function _filerAtReliefCap() internal returns (uint256 id1, uint256 id2) {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(0xA1)), "DMCA-TEST");
        vm.prank(filer);
        id1 = blacklist.openBlacklistAppeal(
            bytes32(uint256(0xA1)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(id1);

        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(0xA2)), "DMCA-TEST");
        vm.prank(filer);
        id2 = blacklist.openBlacklistAppeal(
            bytes32(uint256(0xA2)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(id2);

        assertEq(blacklist.filerRegionActiveRelief(REGION_US, filer), 2);
    }

    /// @notice A single filer is blocked at its per-filer sub-cap even when the
    ///         region ceiling still has a free slot.
    function test_filerReliefCap_blocksWhileRegionHasHeadroom() public {
        _filerAtReliefCap();
        // Region has headroom: 2 of 3 slots used.
        assertEq(blacklist.regionActiveReliefCount(REGION_US), 2);

        // A 3rd fast-track for the SAME filer reverts on the per-filer sub-cap,
        // not the region ceiling.
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(0xA3)), "DMCA-TEST");
        vm.prank(filer);
        uint256 id3 = blacklist.openBlacklistAppeal(
            bytes32(uint256(0xA3)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.prank(multisig);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.FilerReliefCapHit.selector, filer, uint256(2)));
        blacklist.fastTrackBlacklistAppeal(id3);
        // Region count unchanged — the slot was never charged.
        assertEq(blacklist.regionActiveReliefCount(REGION_US), 2);
    }

    function test_filerReliefCap_decrementsOnReject() public {
        (uint256 id1,) = _filerAtReliefCap();
        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(id1);
        assertEq(blacklist.filerRegionActiveRelief(REGION_US, filer), 1);
    }

    function test_filerReliefCap_decrementsOnPerjury() public {
        (uint256 id1,) = _filerAtReliefCap();
        vm.prank(multisig);
        blacklist.rejectAppealAsPerjury(id1);
        assertEq(blacklist.filerRegionActiveRelief(REGION_US, filer), 1);
    }

    function test_filerReliefCap_decrementsOnRatify() public {
        (uint256 id1,) = _filerAtReliefCap();
        vm.prank(admin);
        blacklist.ratifyBlacklistAppealRemoval(id1);
        assertEq(blacklist.filerRegionActiveRelief(REGION_US, filer), 1);
    }

    function test_filerReliefCap_decrementsOnReverse() public {
        (uint256 id1,) = _filerAtReliefCap();
        vm.prank(admin);
        blacklist.reverseBlacklistAppeal(id1);
        assertEq(blacklist.filerRegionActiveRelief(REGION_US, filer), 1);
    }

    /// @notice The cleanup exit path also releases the per-filer slot, and the
    ///         freed filer can fast-track again (full exit-path symmetry).
    function test_filerReliefCap_decrementsOnCleanup() public {
        (uint256 id1,) = _filerAtReliefCap();
        // Lapse the fast-tracked appeal past its ratification window.
        vm.warp(block.timestamp + 14 days + 1);
        blacklist.cleanupExpiredBlacklistAppeal(id1);
        assertEq(blacklist.filerRegionActiveRelief(REGION_US, filer), 1);

        // Freed slot lets the filer fast-track a fresh appeal again.
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(0xA3)), "DMCA-TEST");
        vm.prank(filer);
        uint256 id3 = blacklist.openBlacklistAppeal(
            bytes32(uint256(0xA3)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(id3);
        assertEq(blacklist.filerRegionActiveRelief(REGION_US, filer), 2);
    }

    // -----------------------------------------------------------------
    // ADR 031 § Function signatures and revert table — standing enforcement at
    // filing (audit I-3)
    // -----------------------------------------------------------------

    /// @dev Give `who` a namespace that has claimed `hash`, returning the id.
    function _publisherStanding(address who, bytes32 hash) internal returns (uint256 nsId) {
        vm.prank(who);
        nsId = registry.createNamespace();
        vm.prank(who);
        registry.claimContent(nsId, hash);
    }

    // --- Publisher path ---

    function test_publisherStanding_ownerOfClaimingNamespace_succeeds() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        uint256 nsId = _publisherStanding(filer, SAMPLE_HASH);
        vm.prank(filer);
        blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Publisher, nsId
        );
        assertTrue(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
    }

    function test_publisherStanding_nonOwner_reverts() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        // Namespace owned by someone else (admin), even though it claimed the hash.
        uint256 nsId = _publisherStanding(admin, SAMPLE_HASH);
        vm.prank(filer);
        vm.expectRevert(
            abi.encodeWithSelector(
                ContentBlacklist.UnauthorizedStanding.selector, ContentBlacklist.StandingPath.Publisher
            )
        );
        blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Publisher, nsId
        );
    }

    function test_publisherStanding_ownerButHashNotClaimed_reverts() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        // Filer owns a namespace, but it never claimed SAMPLE_HASH.
        vm.prank(filer);
        uint256 nsId = registry.createNamespace();
        vm.prank(filer);
        vm.expectRevert(
            abi.encodeWithSelector(
                ContentBlacklist.UnauthorizedStanding.selector, ContentBlacklist.StandingPath.Publisher
            )
        );
        blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Publisher, nsId
        );
    }

    function test_publisherStanding_wrongNamespaceId_reverts() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        _publisherStanding(filer, SAMPLE_HASH);
        // A namespaceId the filer does not own (unassigned id 999 → ownerOf == 0).
        vm.prank(filer);
        vm.expectRevert(
            abi.encodeWithSelector(
                ContentBlacklist.UnauthorizedStanding.selector, ContentBlacklist.StandingPath.Publisher
            )
        );
        blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Publisher, 999
        );
    }

    // --- TokenHolder path ---

    /// @notice TokenHolder standing IS the escrowed bond: a filer holding only the
    ///         bond — no threshold balance, no namespace — gets standing, and the
    ///         bond is actually pulled into escrow. Proves there is no extra
    ///         credential gate (so nothing for a flash loan to fake).
    function test_tokenHolderStanding_bondIsStanding_succeedsAndEscrows() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        // Fund a fresh holder with exactly the bond and nothing more — far below
        // any prior threshold — so success can only come from the bond itself.
        address smallHolder = address(0x5A11);
        vm.prank(admin);
        token.transfer(smallHolder, APPEAL_BOND);
        vm.prank(smallHolder);
        token.approve(address(blacklist), APPEAL_BOND);

        uint256 escrowedBefore = token.balanceOf(address(blacklist));
        vm.prank(smallHolder);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.TokenHolder, 0
        );

        assertTrue(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
        // Bond escrowed: contract balance rose by exactly the bond; holder drained.
        assertEq(token.balanceOf(address(blacklist)), escrowedBefore + APPEAL_BOND);
        assertEq(token.balanceOf(smallHolder), 0);
        ContentBlacklist.BlacklistAppeal memory a = blacklist.getAppeal(appealId);
        assertEq(a.bond, APPEAL_BOND);
        assertEq(uint8(a.standingPath), uint8(ContentBlacklist.StandingPath.TokenHolder));
    }

    // -----------------------------------------------------------------
    // #1180 — on-chain `reason` audit trail (ADR 011 § Reason field)
    // -----------------------------------------------------------------

    bytes32 internal constant GLOBAL_REGION = bytes32("GLOBAL");

    /// @notice `addHashGlobal` persists the reason and emits it on the event.
    function test_addHashGlobal_persistsReason() public {
        uint256 expectedVersion = blacklist.getBlacklistVersion() + 1;
        vm.expectEmit(true, true, false, true, address(blacklist));
        emit ContentBlacklist.HashBlacklisted(GLOBAL_REGION, SAMPLE_HASH, expectedVersion, "DMCA-2026-001");
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-2026-001");
        assertEq(blacklist.hashReason(GLOBAL_REGION, SAMPLE_HASH), "DMCA-2026-001");
    }

    /// @notice `addHashRegional` persists the reason under the entry's region.
    function test_addHashRegional_persistsReason() public {
        uint256 expectedVersion = blacklist.getBlacklistVersion() + 1;
        vm.expectEmit(true, true, false, true, address(blacklist));
        emit ContentBlacklist.HashBlacklisted(REGION_US, SAMPLE_HASH, expectedVersion, "DSA-DE-001");
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DSA-DE-001");
        assertEq(blacklist.hashReason(REGION_US, SAMPLE_HASH), "DSA-DE-001");
    }

    /// @notice Removing an entry clears its stored reason (no stale audit trail).
    function test_removeHash_clearsReason() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DSA-DE-001");
        assertEq(blacklist.hashReason(REGION_US, SAMPLE_HASH), "DSA-DE-001");
        vm.prank(regionalBody);
        blacklist.removeHashRegional(REGION_US, SAMPLE_HASH);
        assertEq(blacklist.hashReason(REGION_US, SAMPLE_HASH), "");
    }

    /// @notice A re-add overwrites the stored reason with the new notice's.
    function test_addHash_reAdd_overwritesReason() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-OLD");
        vm.prank(regionalBody);
        blacklist.removeHashRegional(REGION_US, SAMPLE_HASH);
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-NEW");
        assertEq(blacklist.hashReason(REGION_US, SAMPLE_HASH), "DMCA-NEW");
    }

    // -----------------------------------------------------------------
    // #1194 — reject an all-zero evidenceBundleHash
    // -----------------------------------------------------------------

    function test_openBlacklistAppeal_revertsOnEmptyEvidence() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        vm.prank(filer);
        vm.expectRevert(ContentBlacklist.EmptyEvidenceBundleHash.selector);
        blacklist.openBlacklistAppeal(SAMPLE_HASH, REGION_US, bytes32(0), ContentBlacklist.StandingPath.Operator, 0);
    }

    // -----------------------------------------------------------------
    // #1182 — cleanupExpiredBlacklistAppeal condition (c) global override
    // -----------------------------------------------------------------

    /// @notice Condition (a) — an `Open` appeal past its review window lapses and
    ///         BURNS the bond (reason 1), unchanged by the global-override work.
    function test_cleanup_openTimeout_burnsBond() public {
        uint256 appealId = _open(bytes32(uint256(1)));
        vm.warp(block.timestamp + 14 days + 1);

        uint256 supplyBefore = token.totalSupply();
        vm.expectEmit(true, false, false, true, address(blacklist));
        emit ContentBlacklist.BlacklistAppealLapsed(appealId, 1);
        blacklist.cleanupExpiredBlacklistAppeal(appealId);
        // Bond burned (supply drops), not refunded.
        assertEq(supplyBefore - token.totalSupply(), APPEAL_BOND);
    }

    /// @notice Condition (b) — a `FastTracked` appeal past its ratification window
    ///         lapses and BURNS the bond (reason 2).
    function test_cleanup_fastTrackedTimeout_burnsBond() public {
        uint256 appealId = _open(bytes32(uint256(1)));
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);
        uint256 versionAfterSuspend = blacklist.getBlacklistVersion();
        vm.warp(block.timestamp + 14 days + 1);

        uint256 supplyBefore = token.totalSupply();
        vm.expectEmit(true, false, false, true, address(blacklist));
        emit ContentBlacklist.BlacklistAppealLapsed(appealId, 2);
        blacklist.cleanupExpiredBlacklistAppeal(appealId);
        assertEq(supplyBefore - token.totalSupply(), APPEAL_BOND);

        // Lapse un-suspends the surviving entry → enforced-set change → version
        // bumps and the hash is enforced again (ADR 011 § Polling).
        assertEq(blacklist.getBlacklistVersion(), versionAfterSuspend + 1);
        assertTrue(blacklist.isHashBlacklistedInRegion(bytes32(uint256(1)), REGION_US));
    }

    /// @notice Condition (c) — an `Open` appeal whose entry was removed by a
    ///         slow-path global override is cleanable IMMEDIATELY (window not
    ///         elapsed) and REFUNDS the bond (reason 3, ADR 011 § Global Override).
    function test_cleanup_globalOverride_open_refunds() public {
        uint256 appealId = _open(bytes32(uint256(1)));
        assertTrue(blacklist.hasActiveAppeal(REGION_US, bytes32(uint256(1))));

        // Global override removes the entry while the appeal is still live.
        vm.prank(regionalBody);
        blacklist.removeHashRegional(REGION_US, bytes32(uint256(1)));

        uint256 filerBalBefore = token.balanceOf(filer);
        uint256 supplyBefore = token.totalSupply();

        // No warp — the window has NOT elapsed, yet condition (c) admits cleanup.
        vm.expectEmit(true, false, false, true, address(blacklist));
        emit ContentBlacklist.BlacklistAppealLapsed(appealId, 3);
        blacklist.cleanupExpiredBlacklistAppeal(appealId);

        // Bond refunded, not burned: filer balance up by the bond, supply flat.
        assertEq(token.balanceOf(filer) - filerBalBefore, APPEAL_BOND);
        assertEq(token.totalSupply(), supplyBefore);
        assertFalse(blacklist.hasActiveAppeal(REGION_US, bytes32(uint256(1))));
        ContentBlacklist.BlacklistAppeal memory a = blacklist.getAppeal(appealId);
        assertEq(uint8(a.status), uint8(ContentBlacklist.AppealStatus.Lapsed));
        assertEq(a.bond, 0);
    }

    /// @notice Condition (c) on a `FastTracked` appeal: refunds the bond AND
    ///         releases the interim-relief slot, immediately (no warp).
    function test_cleanup_globalOverride_fastTracked_refundsAndReleasesSlot() public {
        uint256 appealId = _open(bytes32(uint256(1)));
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);
        assertEq(blacklist.regionActiveReliefCount(REGION_US), 1);
        assertEq(blacklist.filerRegionActiveRelief(REGION_US, filer), 1);

        // Global override deletes the suspended entry mid-appeal.
        vm.prank(regionalBody);
        blacklist.removeHashRegional(REGION_US, bytes32(uint256(1)));
        uint256 versionAfterRemove = blacklist.getBlacklistVersion();

        uint256 filerBalBefore = token.balanceOf(filer);
        uint256 supplyBefore = token.totalSupply();

        vm.expectEmit(true, false, false, true, address(blacklist));
        emit ContentBlacklist.BlacklistAppealLapsed(appealId, 3);
        blacklist.cleanupExpiredBlacklistAppeal(appealId);

        assertEq(token.balanceOf(filer) - filerBalBefore, APPEAL_BOND);
        assertEq(token.totalSupply(), supplyBefore);
        // Both relief tiers released even though the entry no longer exists.
        assertEq(blacklist.regionActiveReliefCount(REGION_US), 0);
        assertEq(blacklist.filerRegionActiveRelief(REGION_US, filer), 0);
        assertFalse(blacklist.hasActiveAppeal(REGION_US, bytes32(uint256(1))));

        // The `entryGone` branch must NOT bump: the removal already bumped, and
        // there is no `suspended` flag left to clear, so nothing about the
        // enforced set changed here. A second bump would cost every polling node
        // a wasted fleet-wide delta fetch. This is the negative half of the only
        // CONDITIONAL bump site in the contract — the positive half is asserted
        // by `test_cleanup_fastTrackedTimeout_burnsBond`.
        assertEq(blacklist.getBlacklistVersion(), versionAfterRemove);
    }

    /// @notice A terminal appeal is still inadmissible for cleanup even after a
    ///         global override (guard unchanged).
    function test_cleanup_globalOverride_terminalAppeal_reverts() public {
        uint256 appealId = _open(bytes32(uint256(1)));
        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(appealId);
        // Entry re-activates after reject; remove it via override.
        vm.prank(regionalBody);
        blacklist.removeHashRegional(REGION_US, bytes32(uint256(1)));
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.AppealNotOpen.selector, appealId));
        blacklist.cleanupExpiredBlacklistAppeal(appealId);
    }

    // -----------------------------------------------------------------
    // #1299 — no phantom version bump when the entry is removed mid-appeal
    //
    // `removeHashGlobal`/`removeHashRegional` carry no `hasActiveAppeal` guard
    // (only `_addHash` does), so governance can delete an entry while its appeal
    // is still Open or FastTracked — a supported path with its own lapse reason
    // code (3, GlobalOverride). Three terminal paths that clear the flag
    // (reject, perjury-reject, reverse) then call `_setEntrySuspended(.., false)`
    // on a zeroed struct; cleanup guards its own call (`if (!entryGone)`) and so
    // skips it, and ratify removes the entry via `_removeHashRegional` instead
    // and so reverts. On those three the internal no-op is what must hold:
    // writing `suspended = false` (already false) and bumping the counter would
    // be a PHANTOM REVISION — the version advances with zero change to what any
    // `isHashBlacklisted*` view reports, so every polling node re-fetches deltas
    // for nothing and the audit trail claims a change that never happened.
    //
    // The suspend (`true`) side is not on this list: fast-tracking an appeal
    // whose entry is already gone now reverts `EntryNotBlacklisted` upfront
    // (see `test_fastTrack_afterEntryRemoved_reverts`), so `_setEntrySuspended`
    // is never reached with `suspended = true` on a zeroed struct.
    //
    // Suite invariant, worth stating once here: every test that asserts
    // `suspended` changed should also assert the version moved, and every test
    // that asserts it did NOT change should assert the version held. Asserting
    // the flag alone is exactly what let the original missing-bump bug through.
    // -----------------------------------------------------------------

    /// @notice Helper: add + open + fast-track, then global-override the entry
    ///         away. Returns the appeal id and the version after the removal —
    ///         the value every terminal path below must leave untouched.
    function _fastTrackedThenEntryRemoved(bytes32 h) internal returns (uint256, uint256) {
        uint256 appealId = _open(h);
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);
        vm.prank(regionalBody);
        blacklist.removeHashRegional(REGION_US, h);
        assertEq(blacklist.getHashEntry(REGION_US, h).addedAt, 0);
        return (appealId, blacklist.getBlacklistVersion());
    }

    /// @notice `rejectBlacklistAppeal` on a fast-tracked appeal whose entry was
    ///         already removed must not bump the version.
    function test_rejectAppeal_entryRemovedMidAppeal_doesNotBumpVersion() public {
        bytes32 h = bytes32(uint256(1));
        (uint256 appealId, uint256 versionAfterRemove) = _fastTrackedThenEntryRemoved(h);

        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(appealId);

        // The appeal still reaches its terminal state — this pins "no bump",
        // not "the call reverted".
        ContentBlacklist.BlacklistAppeal memory a = blacklist.getAppeal(appealId);
        assertEq(uint8(a.status), uint8(ContentBlacklist.AppealStatus.Rejected));
        assertEq(blacklist.getBlacklistVersion(), versionAfterRemove);
        assertFalse(blacklist.isHashBlacklistedInRegion(h, REGION_US));
    }

    /// @notice Same for `rejectAppealAsPerjury`.
    function test_rejectAppealAsPerjury_entryRemovedMidAppeal_doesNotBumpVersion() public {
        bytes32 h = bytes32(uint256(1));
        (uint256 appealId, uint256 versionAfterRemove) = _fastTrackedThenEntryRemoved(h);

        vm.prank(multisig);
        blacklist.rejectAppealAsPerjury(appealId);

        ContentBlacklist.BlacklistAppeal memory a = blacklist.getAppeal(appealId);
        assertEq(uint8(a.status), uint8(ContentBlacklist.AppealStatus.Rejected));
        assertEq(blacklist.getBlacklistVersion(), versionAfterRemove);
        assertFalse(blacklist.isHashBlacklistedInRegion(h, REGION_US));
    }

    /// @notice Same for `reverseBlacklistAppeal`, the one unconditional
    ///         `_setEntrySuspended(false)` call site.
    function test_reverseAppeal_entryRemovedMidAppeal_doesNotBumpVersion() public {
        bytes32 h = bytes32(uint256(1));
        (uint256 appealId, uint256 versionAfterRemove) = _fastTrackedThenEntryRemoved(h);

        vm.prank(admin);
        blacklist.reverseBlacklistAppeal(appealId);

        ContentBlacklist.BlacklistAppeal memory a = blacklist.getAppeal(appealId);
        assertEq(uint8(a.status), uint8(ContentBlacklist.AppealStatus.Reversed));
        assertEq(blacklist.getBlacklistVersion(), versionAfterRemove);
        assertFalse(blacklist.isHashBlacklistedInRegion(h, REGION_US));
    }

    /// @notice The suspend side of the guard: the entry is removed BEFORE the
    ///         fast-track. `fastTrackBlacklistAppeal` now reverts
    ///         `EntryNotBlacklisted` upfront rather than advancing a moot appeal
    ///         to `FastTracked` and burning two scarce relief slots on a
    ///         suspension that would no-op. Failing loudly also means
    ///         `_setEntrySuspended` is never reached with `suspended = true` on a
    ///         zeroed struct, so the belt-and-braces `_addHash` reset can never
    ///         be exercised by this path — the entry stays enforceable after a
    ///         later re-add regardless.
    /// @dev    The full tx rolls back: appeal still `Open`, no slots charged, no
    ///         version movement. The working exit is `cleanupExpiredBlacklist-
    ///         Appeal` (condition c, reason 3), which refunds the bond — asserted
    ///         so the recovery path is pinned alongside the revert.
    function test_fastTrack_afterEntryRemoved_reverts() public {
        bytes32 h = bytes32(uint256(1));
        uint256 appealId = _open(h);

        // Global override removes the entry while the appeal is still Open.
        vm.prank(regionalBody);
        blacklist.removeHashRegional(REGION_US, h);
        uint256 versionAfterRemove = blacklist.getBlacklistVersion();

        // Fast-tracking a moot appeal fails loudly.
        vm.prank(multisig);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.EntryNotBlacklisted.selector, REGION_US, h));
        blacklist.fastTrackBlacklistAppeal(appealId);

        // Fully rolled back: appeal still Open, no relief slots charged, no bump.
        assertEq(uint8(blacklist.getAppeal(appealId).status), uint8(ContentBlacklist.AppealStatus.Open));
        assertEq(blacklist.regionActiveReliefCount(REGION_US), 0);
        assertEq(blacklist.filerRegionActiveRelief(REGION_US, filer), 0);
        assertEq(blacklist.getBlacklistVersion(), versionAfterRemove);

        // Cleanup (condition (c), reason 3) is the working exit and refunds.
        uint256 filerBalBefore = token.balanceOf(filer);
        blacklist.cleanupExpiredBlacklistAppeal(appealId);
        assertEq(token.balanceOf(filer) - filerBalBefore, APPEAL_BOND);
        assertEq(uint8(blacklist.getAppeal(appealId).status), uint8(ContentBlacklist.AppealStatus.Lapsed));

        // Re-add: the hash is enforceable again.
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, h, "DMCA-TEST-2");
        assertFalse(blacklist.getHashEntry(REGION_US, h).suspended);
        assertTrue(blacklist.isHashBlacklistedInRegion(h, REGION_US));
    }

    /// @notice `ratifyBlacklistAppealRemoval` is the one FastTracked terminal
    ///         path that does NOT route through `_setEntrySuspended` — it removes
    ///         the entry via `_removeHashRegional`, which reverts
    ///         `EntryNotBlacklisted` when the entry is already gone. The whole tx
    ///         rolls back, the appeal survives in `FastTracked`, and cleanup
    ///         condition (c) is the working exit.
    /// @dev    Pinned because this PR teaches a "no-op on a removed entry"
    ///         instinct: applying it to `_removeHashRegional` would silently flip
    ///         ratify from revert to succeed-and-refund with nothing failing.
    ///         Note the consequence the revert preserves — ratify sets
    ///         `lastRatifiedSuccessAt` (the 90-day frequency cap) and the cleanup
    ///         fallback does not, so a filer whose ratify is blocked by a mid-
    ///         appeal removal both keeps the bond and skips the cooldown.
    function test_ratify_entryRemovedMidAppeal_reverts() public {
        bytes32 h = bytes32(uint256(1));
        (uint256 appealId, uint256 versionAfterRemove) = _fastTrackedThenEntryRemoved(h);

        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.EntryNotBlacklisted.selector, REGION_US, h));
        blacklist.ratifyBlacklistAppealRemoval(appealId);

        // Fully rolled back: appeal still live, no version movement, no
        // ratification recorded against the filer's frequency cap.
        ContentBlacklist.BlacklistAppeal memory a = blacklist.getAppeal(appealId);
        assertEq(uint8(a.status), uint8(ContentBlacklist.AppealStatus.FastTracked));
        assertEq(blacklist.getBlacklistVersion(), versionAfterRemove);
        assertEq(blacklist.lastRatifiedSuccessAt(filer), 0);

        // Cleanup remains available and refunds (reason 3).
        uint256 filerBalBefore = token.balanceOf(filer);
        blacklist.cleanupExpiredBlacklistAppeal(appealId);
        assertEq(token.balanceOf(filer) - filerBalBefore, APPEAL_BOND);
        assertEq(uint8(blacklist.getAppeal(appealId).status), uint8(ContentBlacklist.AppealStatus.Lapsed));
    }

    /// @notice The regional tests above all remove via `removeHashRegional`.
    ///         This drives the same phantom-bump guard through the OTHER removal
    ///         entry point, `removeHashGlobal` (`GOVERNANCE_ROLE`) on a
    ///         `GLOBAL_REGION` entry, to pin the guard's region-agnosticism
    ///         explicitly rather than by inference: both entry points funnel
    ///         through `_removeHashRegional`, which `delete`s the struct, and
    ///         `_setEntrySuspended` keys the `addedAt == 0` no-op off that struct
    ///         without inspecting the region.
    function test_rejectAppeal_entryRemovedViaGlobalOverride_doesNotBumpVersion() public {
        bytes32 h = bytes32(uint256(1));
        vm.prank(admin);
        blacklist.addHashGlobal(h, "DMCA-TEST");
        // Operator standing on a global entry skips the region match.
        vm.prank(filer);
        uint256 appealId =
            blacklist.openBlacklistAppeal(h, GLOBAL_REGION, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0);
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(appealId);

        // Global-override removal via the GOVERNANCE_ROLE entry point.
        vm.prank(admin);
        blacklist.removeHashGlobal(h);
        assertEq(blacklist.getHashEntry(GLOBAL_REGION, h).addedAt, 0);
        uint256 versionAfterRemove = blacklist.getBlacklistVersion();

        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(appealId);

        // Terminal state reached, no phantom bump — same guarantee as the
        // regional path.
        assertEq(uint8(blacklist.getAppeal(appealId).status), uint8(ContentBlacklist.AppealStatus.Rejected));
        assertEq(blacklist.getBlacklistVersion(), versionAfterRemove);
        assertFalse(blacklist.isHashBlacklistedInRegion(h, REGION_US));
    }

    // =================================================================
    // ADR 011 § Compliance Window (#1169)
    // =================================================================

    function test_complianceWindow_defaults() public view {
        assertEq(blacklist.complianceWindow(), 24 hours);
        assertEq(blacklist.emergencyComplianceWindow(), 2 hours);
    }

    function test_addHashGlobal_stampsEffectiveAt() public {
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-TEST");
        ContentBlacklist.HashEntry memory e = blacklist.getHashEntry(GLOBAL_REGION, SAMPLE_HASH);
        assertEq(e.addedAt, uint64(block.timestamp));
        assertEq(e.effectiveAt, uint64(block.timestamp) + 24 hours);
        assertFalse(e.emergency);
    }

    /// @notice The window is STAMPED, not derived. Lowering `complianceWindow`
    ///         after an add must not retroactively pull the slash boundary
    ///         forward under deliveries already served.
    function test_setComplianceWindow_doesNotRestampExistingEntries() public {
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-TEST");
        uint64 stamped = blacklist.getHashEntry(GLOBAL_REGION, SAMPLE_HASH).effectiveAt;

        vm.prank(admin);
        blacklist.setComplianceWindow(1 hours);

        assertEq(blacklist.getHashEntry(GLOBAL_REGION, SAMPLE_HASH).effectiveAt, stamped);
        // ...but the next add picks the new window up.
        bytes32 h2 = bytes32(uint256(0xB2));
        vm.prank(admin);
        blacklist.addHashGlobal(h2, "DMCA-TEST");
        assertEq(blacklist.getHashEntry(GLOBAL_REGION, h2).effectiveAt, uint64(block.timestamp) + 1 hours);
    }

    function test_setComplianceWindow_belowFloorReverts() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                ContentBlacklist.ParamOutOfBounds.selector, uint256(59 minutes), uint256(1 hours), uint256(7 days)
            )
        );
        blacklist.setComplianceWindow(59 minutes);
    }

    function test_setEmergencyComplianceWindow_aboveCeilingReverts() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                ContentBlacklist.ParamOutOfBounds.selector, uint256(8 days), uint256(1 hours), uint256(7 days)
            )
        );
        blacklist.setEmergencyComplianceWindow(8 days);
    }

    function test_setComplianceWindow_revertsWithoutGovernanceRole() public {
        _expectMissingRole(filer, blacklist.GOVERNANCE_ROLE());
        vm.prank(filer);
        blacklist.setComplianceWindow(2 hours);
    }

    // =================================================================
    // ADR 011 emergency multisig path (#1167)
    // =================================================================

    function test_emergencyAdd_isImmediatelyBlacklistedWithTightWindow() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(SAMPLE_HASH, uint8(ContentBlacklist.Category.CSAM), "CSAM");
        assertTrue(blacklist.isHashBlacklisted(SAMPLE_HASH));
        ContentBlacklist.HashEntry memory e = blacklist.getHashEntry(GLOBAL_REGION, SAMPLE_HASH);
        assertTrue(e.emergency);
        assertEq(e.category, uint8(ContentBlacklist.Category.CSAM));
        assertEq(e.effectiveAt, uint64(block.timestamp) + 2 hours);
    }

    function test_emergencyAdd_revertsWithoutMultisigRole() public {
        _expectMissingRole(filer, blacklist.EMERGENCY_MULTISIG_ROLE());
        vm.prank(filer);
        blacklist.emergencyAdd(SAMPLE_HASH, 0, "CSAM");
    }

    function test_emergencyAdd_invalidCategoryReverts() public {
        vm.prank(multisig);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.InvalidCategory.selector, uint8(3)));
        blacklist.emergencyAdd(SAMPLE_HASH, 3, "CSAM");
    }

    function test_emergencyAdd_generalExpiresAfter14Days() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(SAMPLE_HASH, uint8(ContentBlacklist.Category.GENERAL), "DMCA");
        vm.warp(block.timestamp + 14 days);
        assertTrue(blacklist.isHashBlacklisted(SAMPLE_HASH), "still live at the deadline");
        vm.warp(block.timestamp + 1);
        assertFalse(blacklist.isHashBlacklisted(SAMPLE_HASH), "lapsed one second past it");
    }

    function test_emergencyAdd_severeExpiresAfter90Days() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(SAMPLE_HASH, uint8(ContentBlacklist.Category.TERRORIST), "TCO");
        vm.warp(block.timestamp + 14 days + 1);
        assertTrue(blacklist.isHashBlacklisted(SAMPLE_HASH), "severe outlives the GENERAL term");
        vm.warp(block.timestamp + 90 days);
        assertFalse(blacklist.isHashBlacklisted(SAMPLE_HASH));
    }

    /// @notice Governance ratification: re-adding under `addHashGlobal` clears
    ///         `emergency`, so the entry stops self-expiring.
    function test_emergencyAdd_ratifiedByGovernanceSurvivesExpiry() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(SAMPLE_HASH, uint8(ContentBlacklist.Category.GENERAL), "DMCA");
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-RATIFIED");

        assertFalse(blacklist.getHashEntry(GLOBAL_REGION, SAMPLE_HASH).emergency);
        vm.warp(block.timestamp + 365 days);
        assertTrue(blacklist.isHashBlacklisted(SAMPLE_HASH));
    }

    /// @notice Expiry alone changes the enforced set with the version counter
    ///         frozen, which a delta-polling node cannot observe (ADR 011
    ///         § Polling). `expireEmergencyEntry` materializes it as a real
    ///         `HashRemoved` + version bump.
    function test_expireEmergencyEntry_bumpsVersionAndEmitsHashRemoved() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(SAMPLE_HASH, uint8(ContentBlacklist.Category.GENERAL), "DMCA");
        uint256 versionBefore = blacklist.getBlacklistVersion();
        vm.warp(block.timestamp + 14 days + 1);

        vm.expectEmit(true, true, false, true, address(blacklist));
        emit ContentBlacklist.HashRemoved(GLOBAL_REGION, SAMPLE_HASH, versionBefore + 1);
        // Permissionless — no prank, and `filer` holds no role.
        vm.prank(filer);
        blacklist.expireEmergencyEntry(GLOBAL_REGION, SAMPLE_HASH);

        assertEq(blacklist.getBlacklistVersion(), versionBefore + 1);
        assertEq(blacklist.getHashEntry(GLOBAL_REGION, SAMPLE_HASH).addedAt, 0);
    }

    function test_expireEmergencyEntry_revertsBeforeDeadline() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(SAMPLE_HASH, uint8(ContentBlacklist.Category.GENERAL), "DMCA");
        vm.expectRevert(ContentBlacklist.NotExpired.selector);
        blacklist.expireEmergencyEntry(GLOBAL_REGION, SAMPLE_HASH);
    }

    function test_expireEmergencyEntry_revertsOnGovernanceEntry() public {
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-TEST");
        vm.warp(block.timestamp + 365 days);
        vm.expectRevert(ContentBlacklist.NotExpired.selector);
        blacklist.expireEmergencyEntry(GLOBAL_REGION, SAMPLE_HASH);
    }

    function test_expireEmergencyEntry_revertsOnAbsentEntry() public {
        vm.expectRevert(
            abi.encodeWithSelector(ContentBlacklist.EntryNotBlacklisted.selector, GLOBAL_REGION, SAMPLE_HASH)
        );
        blacklist.expireEmergencyEntry(GLOBAL_REGION, SAMPLE_HASH);
    }

    // --- emergency origins ---

    function test_emergencyAddOrigin_blacklistsAndExpires() public {
        address origin = address(0x0121A);
        vm.expectEmit(true, false, false, true, address(blacklist));
        emit ContentBlacklist.OriginBlacklistUpdated(origin, true);
        vm.prank(multisig);
        blacklist.emergencyAddOrigin(origin, uint8(ContentBlacklist.Category.GENERAL), "DMCA");

        assertTrue(blacklist.isOriginBlacklisted(origin));
        vm.warp(block.timestamp + 14 days + 1);
        assertFalse(blacklist.isOriginBlacklisted(origin));
    }

    /// @notice Origin blacklisting sits outside the version poll entirely, so
    ///         the `OriginBlacklistUpdated(origin, false)` here is the ONLY
    ///         signal a node ever gets that the entry lapsed.
    function test_expireEmergencyOrigin_emitsClearingEvent() public {
        address origin = address(0x0121B);
        vm.prank(multisig);
        blacklist.emergencyAddOrigin(origin, uint8(ContentBlacklist.Category.GENERAL), "DMCA");
        vm.warp(block.timestamp + 14 days + 1);

        vm.expectEmit(true, false, false, true, address(blacklist));
        emit ContentBlacklist.OriginBlacklistUpdated(origin, false);
        vm.prank(filer);
        blacklist.expireEmergencyOrigin(origin);

        assertFalse(blacklist.isOriginBlacklisted(origin));
        assertEq(blacklist.getEmergencyOrigin(origin).addedAt, 0);
    }

    function test_expireEmergencyOrigin_revertsOnGovernanceOrigin() public {
        address origin = address(0x0121C);
        vm.prank(admin);
        blacklist.setOriginBlacklist(origin, true);
        vm.warp(block.timestamp + 365 days);
        vm.expectRevert(ContentBlacklist.NotExpired.selector);
        blacklist.expireEmergencyOrigin(origin);
    }

    /// @notice `setOriginBlacklist` is the ratification path — it must clear the
    ///         expiry record, or the governance entry would inherit the
    ///         emergency deadline and silently lapse.
    function test_setOriginBlacklist_ratifiesEmergencyOrigin() public {
        address origin = address(0x0121D);
        vm.prank(multisig);
        blacklist.emergencyAddOrigin(origin, uint8(ContentBlacklist.Category.GENERAL), "DMCA");
        vm.prank(admin);
        blacklist.setOriginBlacklist(origin, true);

        vm.warp(block.timestamp + 365 days);
        assertTrue(blacklist.isOriginBlacklisted(origin));
    }

    // =================================================================
    // ADR 011 § Regional Governance Bodies (#1178)
    // =================================================================

    function test_registerRegionalBody_bindsRegionAndGrantsRole() public view {
        assertEq(blacklist.getRegionalBody(REGION_US).body, regionalBody);
        assertEq(blacklist.regionOfBody(regionalBody), REGION_US);
        assertTrue(blacklist.hasRole(blacklist.REGIONAL_BODY_ROLE(), regionalBody));
    }

    /// @notice The core of #1178: `REGIONAL_BODY_ROLE` alone used to let ANY
    ///         registered body write entries for ANY region. A body's authority
    ///         is its own jurisdiction.
    function test_addHashRegional_bodyCannotWriteAnotherRegion() public {
        vm.prank(regionalBody); // registered for US, not EU
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.NotRegionalBodyFor.selector, REGION_EU, regionalBody));
        blacklist.addHashRegional(REGION_EU, SAMPLE_HASH, "DSA-DE-001");
    }

    function test_removeHashRegional_bodyCannotReachAnotherRegion() public {
        vm.prank(euBody);
        blacklist.addHashRegional(REGION_EU, SAMPLE_HASH, "DSA-DE-001");
        vm.prank(regionalBody);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.NotRegionalBodyFor.selector, REGION_EU, regionalBody));
        blacklist.removeHashRegional(REGION_EU, SAMPLE_HASH);
    }

    function test_registerRegionalBody_regionAlreadyTakenReverts() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(ContentBlacklist.RegionalBodyAlreadyRegistered.selector, REGION_US, regionalBody)
        );
        blacklist.registerRegionalBody(REGION_US, address(0xAB1), multisig);
    }

    function test_registerRegionalBody_bodyAlreadyServingRegionReverts() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(ContentBlacklist.BodyAlreadyServingRegion.selector, regionalBody, REGION_US)
        );
        blacklist.registerRegionalBody(bytes32("FR"), regionalBody, multisig);
    }

    function test_registerRegionalBody_rejectsGlobalAndUnsetRegion() public {
        vm.startPrank(admin);
        vm.expectRevert(ContentBlacklist.MissingRegion.selector);
        blacklist.registerRegionalBody(GLOBAL_REGION, address(0xAB2), multisig);
        vm.expectRevert(ContentBlacklist.MissingRegion.selector);
        blacklist.registerRegionalBody(bytes32(0), address(0xAB2), multisig);
        vm.stopPrank();
    }

    /// @notice The comparison side must actually hold the role, or a probe
    ///         against it proves nothing about the real multisig.
    function test_registerRegionalBody_rejectsNonMultisigComparisonSide() public {
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.NotEmergencyMultisig.selector, filer));
        blacklist.registerRegionalBody(bytes32("FR"), address(0xAB3), filer);
    }

    // --- signer disjointness ---

    function test_registerRegionalBody_revertsOnSharedSigner() public {
        address shared = address(0x5111);
        address[] memory msigOwners = new address[](2);
        msigOwners[0] = shared;
        msigOwners[1] = address(0x5112);
        MockSigners msig = new MockSigners(msigOwners);

        address[] memory bodyOwners = new address[](2);
        bodyOwners[0] = address(0x5113);
        bodyOwners[1] = shared;
        MockSigners body = new MockSigners(bodyOwners);

        vm.startPrank(admin);
        blacklist.grantRole(blacklist.EMERGENCY_MULTISIG_ROLE(), address(msig));
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.SignerOverlap.selector, shared));
        blacklist.registerRegionalBody(bytes32("FR"), address(body), address(msig));
        vm.stopPrank();
    }

    function test_registerRegionalBody_disjointSignersVerifiedOnChain() public {
        address[] memory msigOwners = new address[](1);
        msigOwners[0] = address(0x5121);
        MockSigners msig = new MockSigners(msigOwners);

        address[] memory bodyOwners = new address[](1);
        bodyOwners[0] = address(0x5122);
        MockSigners body = new MockSigners(bodyOwners);

        vm.startPrank(admin);
        blacklist.grantRole(blacklist.EMERGENCY_MULTISIG_ROLE(), address(msig));
        vm.expectEmit(true, true, false, true, address(blacklist));
        emit ContentBlacklist.RegionalBodyRegistered(bytes32("FR"), address(body), true);
        blacklist.registerRegionalBody(bytes32("FR"), address(body), address(msig));
        vm.stopPrank();
    }

    /// @notice A body holding the role DIRECTLY is an overlap even if the
    ///         multisig side turns out not to be enumerable.
    function test_registerRegionalBody_revertsWhenSignerHoldsRoleDirectly() public {
        address[] memory bodyOwners = new address[](1);
        bodyOwners[0] = multisig; // `multisig` holds EMERGENCY_MULTISIG_ROLE
        MockSigners body = new MockSigners(bodyOwners);

        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.SignerOverlap.selector, multisig));
        blacklist.registerRegionalBody(bytes32("FR"), address(body), multisig);
    }

    /// @notice Not enumerable is not a failure — ADR 011 routes disjointness to
    ///         the off-chain governance obligation and the event records it.
    function test_registerRegionalBody_opaqueBodyRegistersUnverified() public {
        MockOpaqueBody body = new MockOpaqueBody();
        vm.prank(admin);
        vm.expectEmit(true, true, false, true, address(blacklist));
        emit ContentBlacklist.RegionalBodyRegistered(bytes32("FR"), address(body), false);
        blacklist.registerRegionalBody(bytes32("FR"), address(body), multisig);
    }

    // --- suspension lifecycle ---

    function test_suspendRegionalBody_blocksNewEntriesButLeavesExistingLive() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");

        vm.prank(multisig);
        blacklist.suspendRegionalBody(REGION_US);

        assertTrue(blacklist.isRegionalBodySuspended(REGION_US));
        // Existing entry survives — suspension bounds future authority, it is
        // not a mass retraction of the jurisdiction's takedowns.
        assertTrue(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));

        vm.prank(regionalBody);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.NotRegionalBodyFor.selector, REGION_US, regionalBody));
        blacklist.addHashRegional(REGION_US, bytes32(uint256(0xB9)), "DMCA-TEST");
    }

    function test_suspendRegionalBody_revertsWithoutMultisigRole() public {
        _expectMissingRole(filer, blacklist.EMERGENCY_MULTISIG_ROLE());
        vm.prank(filer);
        blacklist.suspendRegionalBody(REGION_US);
    }

    /// @notice ADR 011 requires the suspension to be ratified or reversed within
    ///         14 days. Governance silence is not ratification, so an unratified
    ///         suspension lapses and the body writes again.
    function test_suspendRegionalBody_unratifiedLapsesAfterWindow() public {
        vm.prank(multisig);
        blacklist.suspendRegionalBody(REGION_US);

        vm.warp(block.timestamp + 14 days);
        assertTrue(blacklist.isRegionalBodySuspended(REGION_US), "still suspended at the deadline");
        vm.warp(block.timestamp + 1);
        assertFalse(blacklist.isRegionalBodySuspended(REGION_US), "lapsed one second past it");

        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        assertTrue(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));
    }

    function test_ratifyRegionalBodySuspension_standsPastWindow() public {
        vm.prank(multisig);
        blacklist.suspendRegionalBody(REGION_US);
        vm.prank(admin);
        blacklist.ratifyRegionalBodySuspension(REGION_US);

        vm.warp(block.timestamp + 365 days);
        assertTrue(blacklist.isRegionalBodySuspended(REGION_US));
    }

    function test_ratifyRegionalBodySuspension_revertsAfterWindowClosed() public {
        vm.prank(multisig);
        blacklist.suspendRegionalBody(REGION_US);
        uint64 closesAt = uint64(block.timestamp) + 14 days;
        vm.warp(block.timestamp + 14 days + 1);

        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.SuspensionWindowClosed.selector, closesAt));
        blacklist.ratifyRegionalBodySuspension(REGION_US);
    }

    function test_unsuspendRegionalBody_restoresAuthority() public {
        vm.prank(multisig);
        blacklist.suspendRegionalBody(REGION_US);
        vm.prank(admin);
        blacklist.ratifyRegionalBodySuspension(REGION_US);
        vm.prank(admin);
        blacklist.unsuspendRegionalBody(REGION_US);

        assertFalse(blacklist.isRegionalBodySuspended(REGION_US));
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
        assertTrue(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));
    }

    function test_suspendRegionalBody_revertsWhenAlreadySuspended() public {
        vm.startPrank(multisig);
        blacklist.suspendRegionalBody(REGION_US);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.RegionalBodyAlreadySuspended.selector, REGION_US));
        blacklist.suspendRegionalBody(REGION_US);
        vm.stopPrank();
    }

    function test_unsuspendRegionalBody_revertsWhenNotSuspended() public {
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.RegionalBodySuspensionNotActive.selector, REGION_US));
        blacklist.unsuspendRegionalBody(REGION_US);
    }

    function test_deregisterRegionalBody_revokesRoleAndClearsBinding() public {
        vm.prank(admin);
        blacklist.deregisterRegionalBody(REGION_US);

        assertEq(blacklist.getRegionalBody(REGION_US).body, address(0));
        assertEq(blacklist.regionOfBody(regionalBody), bytes32(0));
        assertFalse(blacklist.hasRole(blacklist.REGIONAL_BODY_ROLE(), regionalBody));
    }

    function test_deregisterRegionalBody_revertsWhenNotRegistered() public {
        vm.prank(admin);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.RegionalBodyNotRegistered.selector, bytes32("FR")));
        blacklist.deregisterRegionalBody(bytes32("FR"));
    }
}
