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

        // Grant multisig + regional body roles.
        bytes32 multisigRole = blacklist.EMERGENCY_MULTISIG_ROLE();
        bytes32 bodyRole = blacklist.REGIONAL_BODY_ROLE();
        vm.startPrank(admin);
        blacklist.grantRole(multisigRole, multisig);
        blacklist.grantRole(bodyRole, regionalBody);
        vm.stopPrank();
    }

    function test_addHashGlobal_andQuery() public {
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH);
        assertTrue(blacklist.isHashBlacklisted(SAMPLE_HASH));
    }

    function test_addHashRegional_globalTakesPrecedence() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
        assertTrue(blacklist.isHashBlacklistedInRegion(SAMPLE_HASH, REGION_US));
        assertFalse(blacklist.isHashBlacklisted(SAMPLE_HASH));
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
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);

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
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
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
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
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
            blacklist.addHashRegional(REGION_US, bytes32(i + 1));
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
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
        vm.prank(filer);
        uint256 appealId = blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        assertTrue(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));

        // Re-add blocked while the appeal is live.
        vm.prank(regionalBody);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.HashHasActiveAppeal.selector, REGION_US, SAMPLE_HASH));
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);

        // Once the appeal terminates, the re-add is allowed again.
        vm.prank(multisig);
        blacklist.rejectBlacklistAppeal(appealId);
        assertFalse(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
    }

    /// @notice T-3 — concurrent appeals on the same (region, hash) revert.
    function test_openBlacklistAppeal_revertsDuplicatePerHash() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);

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
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
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
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
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
        blacklist.addHashGlobal(SAMPLE_HASH);
    }

    function test_addHashRegional_revertsWithoutRegionalBodyRole() public {
        _expectMissingRole(filer, blacklist.REGIONAL_BODY_ROLE());
        vm.prank(filer);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
    }

    function test_registerRegionalBody_revertsWithoutGovernanceRole() public {
        _expectMissingRole(filer, blacklist.GOVERNANCE_ROLE());
        vm.prank(filer);
        blacklist.registerRegionalBody(address(0xBEEF));
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
        blacklist.addHashGlobal(SAMPLE_HASH);

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
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
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
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_EU, SAMPLE_HASH);
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
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_EU, SAMPLE_HASH);
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
        blacklist.addHashGlobal(SAMPLE_HASH);
        bytes32 globalRegion = bytes32("GLOBAL");
        vm.prank(filer);
        blacklist.openBlacklistAppeal(
            SAMPLE_HASH, globalRegion, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        assertTrue(blacklist.hasActiveAppeal(globalRegion, SAMPLE_HASH));
    }

    function test_openBlacklistAppeal_nonOperatorStanding_skipsRegionCheck() public {
        // Publisher standing is not region-gated even on a mismatched region.
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_EU, SAMPLE_HASH); // filer region is "US"
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
        blacklist.addHashGlobal(SAMPLE_HASH);
        // Global applies to any operator regardless of region.
        assertTrue(blacklist.isHashBlacklistedForOperator(SAMPLE_HASH, operator));
    }

    function test_isHashBlacklistedForOperator_currentRegion() public {
        bondMock.setRegion(operator, "US", "", 0);
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
        assertTrue(blacklist.isHashBlacklistedForOperator(SAMPLE_HASH, operator));
        // An operator in a different region is out of scope.
        bondMock.setRegion(operator, "EU", "", 0);
        assertFalse(blacklist.isHashBlacklistedForOperator(SAMPLE_HASH, operator));
    }

    function test_isHashBlacklistedForOperator_suspendedRegional_false() public {
        bondMock.setRegion(operator, "US", "", 0);
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
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
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
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
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
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
        blacklist.addHashRegional(REGION_US, h);
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
        blacklist.addHashRegional(REGION_US, bytes32(uint256(4)));
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
        blacklist.addHashRegional(REGION_US, bytes32(uint256(3)));
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
        blacklist.addHashRegional(REGION_US, bytes32(uint256(4)));
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
        blacklist.addHashRegional(REGION_US, bytes32(uint256(4)));
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
        blacklist.addHashRegional(REGION_US, bytes32(uint256(4)));
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
        blacklist.addHashRegional(REGION_US, h);
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
        blacklist.addHashRegional(REGION_US, bytes32(uint256(2)));
        vm.prank(filer);
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.FilerPerjuryDenylisted.selector, until));
        blacklist.openBlacklistAppeal(
            bytes32(uint256(2)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );

        // One second before expiry: still locked out. Re-add to refresh the
        // 14-day filing window (the entry above ages out across the warp).
        vm.warp(uint256(until) - 1);
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(2)));
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

        vm.prank(multisig);
        blacklist.rejectAppealAsPerjury(appealId);

        // Un-suspended → entry live again; relief slot released.
        assertTrue(blacklist.isHashBlacklistedInRegion(bytes32(uint256(1)), REGION_US));
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
        blacklist.addHashRegional(REGION_US, bytes32(uint256(2)));
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
        blacklist.addHashRegional(REGION_US, bytes32(uint256(0xA1)));
        vm.prank(filer);
        id1 = blacklist.openBlacklistAppeal(
            bytes32(uint256(0xA1)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(id1);

        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, bytes32(uint256(0xA2)));
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
        blacklist.addHashRegional(REGION_US, bytes32(uint256(0xA3)));
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
        blacklist.addHashRegional(REGION_US, bytes32(uint256(0xA3)));
        vm.prank(filer);
        uint256 id3 = blacklist.openBlacklistAppeal(
            bytes32(uint256(0xA3)), REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Operator, 0
        );
        vm.prank(multisig);
        blacklist.fastTrackBlacklistAppeal(id3);
        assertEq(blacklist.filerRegionActiveRelief(REGION_US, filer), 2);
    }

    // -----------------------------------------------------------------
    // ADR 031 § 216 — standing enforcement at filing (audit I-3)
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
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
        uint256 nsId = _publisherStanding(filer, SAMPLE_HASH);
        vm.prank(filer);
        blacklist.openBlacklistAppeal(
            SAMPLE_HASH, REGION_US, bytes32("e"), ContentBlacklist.StandingPath.Publisher, nsId
        );
        assertTrue(blacklist.hasActiveAppeal(REGION_US, SAMPLE_HASH));
    }

    function test_publisherStanding_nonOwner_reverts() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
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
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
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
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
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
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH);
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
}
