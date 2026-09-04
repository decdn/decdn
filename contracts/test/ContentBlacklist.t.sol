// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { ICapacityBondEjector } from "../src/interfaces/ICapacityBondEjector.sol";
import { ICapacityBondRegionView } from "../src/interfaces/ICapacityBondRegionView.sol";
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

    function setWindow(uint256 window_) external {
        window = window_;
    }

    function regionScopeData(address operator)
        external
        view
        override
        returns (string memory, string memory, uint64, uint256)
    {
        return (_regionHint[operator], _regionPrev[operator], _regionLastChanged[operator], window);
    }
}

/// @title ContentBlacklist smoke tests
/// @notice Critical-path coverage for hash/operator blacklist, cross-contract
///         `ejectNode` wire, regional-body registration and suspension,
///         emergency auto-expiry, and the SF-M1 `MissingRegion` revert.
contract ContentBlacklistTest is Test {
    Token internal token;
    MockEjector internal bondMock;
    ContentBlacklist internal blacklist;

    address internal admin = address(0xA11CE);
    address internal multisig = address(0xC0DE);
    address internal regionalBody = address(0xDE);
    /// @dev A second body, for `REGION_EU`. Distinct from `regionalBody`
    ///      because a body's authority is scoped to exactly one region
    ///      (ADR 011 § Regional Governance Bodies) — the US body cannot write
    ///      EU entries, which is the point of the scoping.
    address internal euBody = address(0xDF);
    address internal stranger = address(0xF1);
    address internal operator = address(0xB0B);

    bytes32 internal constant REGION_US = bytes32("US");
    bytes32 internal constant SAMPLE_HASH = bytes32(uint256(0xABCDEF));

    function setUp() public {
        token = new Token(admin);
        bondMock = new MockEjector();
        blacklist = new ContentBlacklist(ICapacityBondEjector(address(bondMock)), admin);

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

    /// @notice A removal emits `HashRemoved(region, hash)` — the low-latency
    ///         re-read signal for a node tracking the deny-set by enumeration.
    function test_HashRemoved_emitted() public {
        vm.prank(regionalBody);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");

        vm.expectEmit(true, true, false, true, address(blacklist));
        emit ContentBlacklist.HashRemoved(REGION_US, SAMPLE_HASH);
        vm.prank(regionalBody);
        blacklist.removeHashRegional(REGION_US, SAMPLE_HASH);
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

    // -----------------------------------------------------------------
    // Access-control guards on governance / regional-body setters
    // -----------------------------------------------------------------

    function test_addHashGlobal_revertsWithoutGovernanceRole() public {
        _expectMissingRole(stranger, blacklist.GOVERNANCE_ROLE());
        vm.prank(stranger);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-TEST");
    }

    function test_addHashRegional_revertsWithoutRegionalBodyRole() public {
        _expectMissingRole(stranger, blacklist.REGIONAL_BODY_ROLE());
        vm.prank(stranger);
        blacklist.addHashRegional(REGION_US, SAMPLE_HASH, "DMCA-TEST");
    }

    function test_registerRegionalBody_revertsWithoutGovernanceRole() public {
        _expectMissingRole(stranger, blacklist.GOVERNANCE_ROLE());
        vm.prank(stranger);
        blacklist.registerRegionalBody(bytes32("FR"), address(0xBEEF), multisig);
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

    // -----------------------------------------------------------------
    // ADR 011 § Standing path-2 (Operator) region match (ADR 030)
    // -----------------------------------------------------------------

    bytes32 internal constant REGION_EU = bytes32("EU");

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

    // --- Publisher path ---

    // --- TokenHolder path ---

    // -----------------------------------------------------------------
    // #1180 — on-chain `reason` audit trail (ADR 011 § Reason field)
    // -----------------------------------------------------------------

    bytes32 internal constant GLOBAL_REGION = bytes32("GLOBAL");

    /// @notice `addHashGlobal` persists the reason and emits it on the event.
    function test_addHashGlobal_persistsReason() public {
        vm.expectEmit(true, true, false, true, address(blacklist));
        emit ContentBlacklist.HashBlacklisted(GLOBAL_REGION, SAMPLE_HASH, "DMCA-2026-001");
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-2026-001");
        assertEq(blacklist.hashReason(GLOBAL_REGION, SAMPLE_HASH), "DMCA-2026-001");
    }

    /// @notice `addHashRegional` persists the reason under the entry's region.
    function test_addHashRegional_persistsReason() public {
        vm.expectEmit(true, true, false, true, address(blacklist));
        emit ContentBlacklist.HashBlacklisted(REGION_US, SAMPLE_HASH, "DSA-DE-001");
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
        _expectMissingRole(stranger, blacklist.GOVERNANCE_ROLE());
        vm.prank(stranger);
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
        _expectMissingRole(stranger, blacklist.EMERGENCY_MULTISIG_ROLE());
        vm.prank(stranger);
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

    /// @notice Expiry alone changes the enforced set with no write and no log,
    ///         which a node tracking membership by enumeration cannot observe
    ///         until it re-enumerates. `expireEmergencyEntry` materializes it as
    ///         a real `HashRemoved` and drops the entry from the enumerable
    ///         membership.
    function test_expireEmergencyEntry_emitsHashRemoved() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(SAMPLE_HASH, uint8(ContentBlacklist.Category.GENERAL), "DMCA");
        vm.warp(block.timestamp + 14 days + 1);

        vm.expectEmit(true, true, false, true, address(blacklist));
        emit ContentBlacklist.HashRemoved(GLOBAL_REGION, SAMPLE_HASH);
        // Permissionless — no prank, and `stranger` holds no role.
        vm.prank(stranger);
        blacklist.expireEmergencyEntry(GLOBAL_REGION, SAMPLE_HASH);

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

    /// @notice A node tracks the origin deny-set by enumeration and event tail,
    ///         so the `OriginBlacklistUpdated(origin, false)` here is the
    ///         low-latency signal that the entry lapsed.
    function test_expireEmergencyOrigin_emitsClearingEvent() public {
        address origin = address(0x0121B);
        vm.prank(multisig);
        blacklist.emergencyAddOrigin(origin, uint8(ContentBlacklist.Category.GENERAL), "DMCA");
        vm.warp(block.timestamp + 14 days + 1);

        vm.expectEmit(true, false, false, true, address(blacklist));
        emit ContentBlacklist.OriginBlacklistUpdated(origin, false);
        vm.prank(stranger);
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

    /// @notice REGRESSION: the emergency multisig must not be able to weaken a
    ///         standing governance decision. Re-adding a governance-blacklisted
    ///         hash through the emergency path would arm auto-expiry on it,
    ///         handing the multisig a delayed removal it otherwise has no
    ///         authority to perform (`removeHashGlobal` is GOVERNANCE_ROLE).
    function test_emergencyAdd_cannotDowngradeGovernanceEntry() public {
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-TEST");

        vm.prank(multisig);
        vm.expectRevert(
            abi.encodeWithSelector(ContentBlacklist.EmergencyCannotOverrideGovernance.selector, SAMPLE_HASH)
        );
        blacklist.emergencyAdd(SAMPLE_HASH, uint8(ContentBlacklist.Category.GENERAL), "CSAM");

        // The governance entry is untouched and still permanent.
        assertFalse(blacklist.getHashEntry(GLOBAL_REGION, SAMPLE_HASH).emergency);
        vm.warp(block.timestamp + 365 days);
        assertTrue(blacklist.isHashBlacklisted(SAMPLE_HASH));
    }

    /// @notice The multisig may still re-add over its OWN emergency entry — to
    ///         escalate the category, or to re-arm one that lapsed. That
    ///         sustains a takedown rather than weakening one, which is the
    ///         multisig's mandate.
    function test_emergencyAdd_mayReAddOverItsOwnEntry() public {
        vm.startPrank(multisig);
        blacklist.emergencyAdd(SAMPLE_HASH, uint8(ContentBlacklist.Category.GENERAL), "DMCA");
        blacklist.emergencyAdd(SAMPLE_HASH, uint8(ContentBlacklist.Category.CSAM), "CSAM");
        vm.stopPrank();
        assertEq(blacklist.getHashEntry(GLOBAL_REGION, SAMPLE_HASH).category, uint8(ContentBlacklist.Category.CSAM));
    }

    /// @notice Ratification still works in the other direction: governance
    ///         re-adding over an emergency entry makes it permanent. The guard
    ///         constrains only the emergency entry point.
    function test_addHashGlobal_stillRatifiesAnEmergencyEntry() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(SAMPLE_HASH, uint8(ContentBlacklist.Category.GENERAL), "DMCA");
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-RATIFIED");
        assertFalse(blacklist.getHashEntry(GLOBAL_REGION, SAMPLE_HASH).emergency);
    }

    /// @notice REGRESSION, origin side: same escalation via
    ///         `emergencyAddOrigin` over a governance-set origin entry.
    function test_emergencyAddOrigin_cannotDowngradeGovernanceEntry() public {
        address origin = address(0x0121E);
        vm.prank(admin);
        blacklist.setOriginBlacklist(origin, true);

        vm.prank(multisig);
        vm.expectRevert(
            abi.encodeWithSelector(ContentBlacklist.EmergencyCannotOverrideGovernanceOrigin.selector, origin)
        );
        blacklist.emergencyAddOrigin(origin, uint8(ContentBlacklist.Category.GENERAL), "DMCA");

        vm.warp(block.timestamp + 365 days);
        assertTrue(blacklist.isOriginBlacklisted(origin), "governance entry stays permanent");
    }

    /// @notice ...but re-adding over its own (or a lapsed) emergency origin
    ///         entry stays available.
    function test_emergencyAddOrigin_mayReAddOverItsOwnEntry() public {
        address origin = address(0x0121F);
        vm.startPrank(multisig);
        blacklist.emergencyAddOrigin(origin, uint8(ContentBlacklist.Category.GENERAL), "DMCA");
        vm.warp(block.timestamp + 14 days + 1);
        assertFalse(blacklist.isOriginBlacklisted(origin), "lapsed");
        blacklist.emergencyAddOrigin(origin, uint8(ContentBlacklist.Category.CSAM), "CSAM");
        vm.stopPrank();
        assertTrue(blacklist.isOriginBlacklisted(origin), "re-armed");
    }

    // =================================================================
    // ADR 011 § Regional Governance Bodies (#1178)
    // =================================================================

    function test_registerRegionalBody_bindsRegionAndGrantsRole() public view {
        assertEq(blacklist.getRegionalBody(REGION_US).body, regionalBody);
        assertEq(blacklist.regionOfBody(regionalBody), REGION_US);
        assertTrue(blacklist.hasRole(blacklist.REGIONAL_BODY_ROLE(), regionalBody));
    }

    /// @notice `REGIONAL_BODY_ROLE` alone must not let a registered body write
    ///         entries for another region. A body's authority is its own
    ///         jurisdiction.
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
        vm.expectRevert(abi.encodeWithSelector(ContentBlacklist.NotEmergencyMultisig.selector, stranger));
        blacklist.registerRegionalBody(bytes32("FR"), address(0xAB3), stranger);
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
        _expectMissingRole(stranger, blacklist.EMERGENCY_MULTISIG_ROLE());
        vm.prank(stranger);
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

    // ── Enumeration views ───────────────────────────────────────────────────
    //
    // These are what let a consumer READ the deny-set instead of rebuilding it
    // from the event log. Before them, `ContentBlacklist` exposed no enumeration
    // at all, so a node had to replay `HashBlacklisted` from the deploy block and
    // keep a durable projection of everything it had ever seen.

    /// Empty region enumerates as empty rather than reverting — the cold-start
    /// case a consumer hits before governance has blacklisted anything.
    function test_blacklistedHashes_emptyRegion() public view {
        assertEq(blacklist.blacklistedHashCount(REGION_US), 0);
        assertEq(blacklist.blacklistedHashes(REGION_US, 0, 10).length, 0);
    }

    /// An add indexes the hash under its own region only, and a remove withdraws
    /// it — so the index tracks the entry mapping rather than accumulating.
    function test_blacklistedHashes_tracksAddAndRemove() public {
        vm.prank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA");

        assertEq(blacklist.blacklistedHashCount(GLOBAL_REGION), 1);
        assertEq(blacklist.blacklistedHashes(GLOBAL_REGION, 0, 10)[0], SAMPLE_HASH);
        // Indexed under GLOBAL, not under an unrelated region.
        assertEq(blacklist.blacklistedHashCount(REGION_US), 0);

        vm.prank(admin);
        blacklist.removeHashGlobal(SAMPLE_HASH);
        assertEq(blacklist.blacklistedHashCount(GLOBAL_REGION), 0);
    }

    /// Re-adding an existing entry restamps it without duplicating the index
    /// entry — otherwise the count would drift from the real set size.
    function test_blacklistedHashes_reAddDoesNotDuplicate() public {
        vm.startPrank(admin);
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA");
        blacklist.addHashGlobal(SAMPLE_HASH, "DMCA-again");
        vm.stopPrank();
        assertEq(blacklist.blacklistedHashCount(GLOBAL_REGION), 1);
    }

    /// Pagination: pages reassemble to the whole set, a `limit` of zero and an
    /// offset past the end return empty, and `type(uint256).max` clamps rather
    /// than reverting on `offset + limit` overflow.
    function test_blacklistedHashes_pagination() public {
        vm.startPrank(admin);
        for (uint256 i = 1; i <= 5; ++i) {
            blacklist.addHashGlobal(bytes32(i), "DMCA");
        }
        vm.stopPrank();

        assertEq(blacklist.blacklistedHashCount(GLOBAL_REGION), 5);
        assertEq(blacklist.blacklistedHashes(GLOBAL_REGION, 0, 2).length, 2);
        assertEq(blacklist.blacklistedHashes(GLOBAL_REGION, 4, 2).length, 1); // short final page
        assertEq(blacklist.blacklistedHashes(GLOBAL_REGION, 5, 2).length, 0); // offset == len
        assertEq(blacklist.blacklistedHashes(GLOBAL_REGION, 0, 0).length, 0); // limit == 0
        // Must clamp, not overflow `offset + limit`.
        assertEq(blacklist.blacklistedHashes(GLOBAL_REGION, 1, type(uint256).max).length, 4);

        // Multi-page reassembly covers exactly the full set.
        bytes32[] memory a = blacklist.blacklistedHashes(GLOBAL_REGION, 0, 3);
        bytes32[] memory b = blacklist.blacklistedHashes(GLOBAL_REGION, 3, 3);
        uint256 seen;
        for (uint256 i = 1; i <= 5; ++i) {
            for (uint256 j = 0; j < a.length; ++j) {
                if (a[j] == bytes32(i)) ++seen;
            }
            for (uint256 j = 0; j < b.length; ++j) {
                if (b[j] == bytes32(i)) ++seen;
            }
        }
        assertEq(seen, 5);
    }

    /// An expired emergency entry stays in the RAW enumeration while
    /// `isHashBlacklistedInRegion` already reports it dead. Deliberate: filtering
    /// would hole a paginated view, and dropping it is the fail-open direction.
    function test_blacklistedHashes_retainsExpiredEmergencyEntry() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(SAMPLE_HASH, 0, "urgent");
        assertEq(blacklist.blacklistedHashCount(GLOBAL_REGION), 1);

        vm.warp(block.timestamp + 15 days); // past the 14-day GENERAL term
        assertFalse(blacklist.isHashBlacklisted(SAMPLE_HASH), "live predicate must expire it");
        assertEq(blacklist.blacklistedHashCount(GLOBAL_REGION), 1, "raw enumeration retains it");

        // The permissionless poke materializes the expiry into the index.
        blacklist.expireEmergencyEntry(GLOBAL_REGION, SAMPLE_HASH);
        assertEq(blacklist.blacklistedHashCount(GLOBAL_REGION), 0);
    }

    /// The address index is the UNION of the origin and operator lists, and
    /// clearing one while the other still holds must NOT drop the address —
    /// the two are written independently, so a mirror-the-last-write index
    /// would fail open here.
    function test_blacklistedAddresses_unionSemantics() public {
        assertEq(blacklist.blacklistedAddressCount(), 0);

        vm.startPrank(admin);
        blacklist.setOriginBlacklist(operator, true); // origin only
        assertEq(blacklist.blacklistedAddressCount(), 1);
        assertEq(blacklist.blacklistedAddresses(0, 10)[0], operator);

        blacklist.addOperator(operator); // now on both lists
        assertEq(blacklist.blacklistedAddressCount(), 1, "union must not double-count");

        blacklist.removeOperator(operator); // operator cleared, origin still set
        assertEq(blacklist.blacklistedAddressCount(), 1, "origin leg still holds it");

        blacklist.setOriginBlacklist(operator, false); // neither list holds it
        assertEq(blacklist.blacklistedAddressCount(), 0);
        vm.stopPrank();
    }

    /// The operator-only path (`addOperator`, which is the primary governance
    /// route and never touches the origin mapping) must also index.
    function test_blacklistedAddresses_operatorOnlyPath() public {
        vm.prank(admin);
        blacklist.addOperator(operator);
        assertEq(blacklist.blacklistedAddressCount(), 1);
        assertEq(blacklist.blacklistedAddresses(0, 10)[0], operator);
    }

    /// Same pagination contract as the hash view.
    function test_blacklistedAddresses_pagination() public {
        vm.startPrank(admin);
        for (uint160 i = 1; i <= 4; ++i) {
            blacklist.setOriginBlacklist(address(i), true);
        }
        vm.stopPrank();

        assertEq(blacklist.blacklistedAddressCount(), 4);
        assertEq(blacklist.blacklistedAddresses(0, 3).length, 3);
        assertEq(blacklist.blacklistedAddresses(3, 3).length, 1);
        assertEq(blacklist.blacklistedAddresses(4, 1).length, 0);
        assertEq(blacklist.blacklistedAddresses(0, type(uint256).max).length, 4);
    }

    /// `getScopeRegions` always yields GLOBAL first, adds the declared region,
    /// and adds the previous one only while the ripening window is open.
    function test_getScopeRegions_globalOnlyWhenNoRegionDeclared() public view {
        bytes32[] memory regions = blacklist.getScopeRegions(operator);
        assertEq(regions.length, 1);
        assertEq(regions[0], GLOBAL_REGION);
    }

    function test_getScopeRegions_includesCurrentAndRipeningPrev() public {
        vm.warp(100 days);
        bondMock.setRegion(operator, "US", "EU", uint64(block.timestamp));

        // Inside the ripening window: GLOBAL + US + EU.
        bytes32[] memory regions = blacklist.getScopeRegions(operator);
        assertEq(regions.length, 3);
        assertEq(regions[0], GLOBAL_REGION);
        assertEq(regions[1], REGION_US);
        assertEq(regions[2], REGION_EU);

        // Past it, the previous region drops out.
        vm.warp(block.timestamp + 8 days); // window is 7 days
        regions = blacklist.getScopeRegions(operator);
        assertEq(regions.length, 2);
        assertEq(regions[0], GLOBAL_REGION);
        assertEq(regions[1], REGION_US);
    }

    /// The property the whole enumeration path rests on: paging every region
    /// `getScopeRegions` returns and taking the union must give the SAME answer
    /// as the point query, for any hash and any region configuration. If the
    /// extracted `_scopeRegions` ever drifts from `isHashBlacklistedForOperator`,
    /// a node silently under-enforces; this is what catches that.
    ///
    /// Holds modulo expired emergency entries, which the raw enumeration retains
    /// and the live predicate rejects — asserted explicitly rather than skirted.
    function testFuzz_scopeEnumeration_matchesPointQuery(
        uint8 whichRegion,
        uint64 elapsed,
        bytes32 hash,
        bool declareRegion
    ) public {
        vm.assume(hash != bytes32(0));
        vm.warp(100 days);

        if (declareRegion) {
            bondMock.setRegion(operator, "US", "EU", uint64(block.timestamp));
        }

        // Seat the hash in exactly one of GLOBAL / US / EU.
        uint8 pick = whichRegion % 3;
        if (pick == 0) {
            vm.prank(admin);
            blacklist.addHashGlobal(hash, "fuzz");
        } else if (pick == 1) {
            vm.prank(regionalBody);
            blacklist.addHashRegional(REGION_US, hash, "fuzz");
        } else {
            vm.prank(euBody);
            blacklist.addHashRegional(REGION_EU, hash, "fuzz");
        }

        vm.warp(block.timestamp + (uint256(elapsed) % 30 days));

        bool viaEnumeration;
        bytes32[] memory regions = blacklist.getScopeRegions(operator);
        for (uint256 i = 0; i < regions.length; ++i) {
            bytes32[] memory page = blacklist.blacklistedHashes(regions[i], 0, type(uint256).max);
            for (uint256 j = 0; j < page.length; ++j) {
                // Raw membership is enumerated; apply the liveness predicate the
                // point query uses, so the two sides are compared like for like.
                if (page[j] == hash && blacklist.isHashBlacklistedInRegion(hash, regions[i])) {
                    viaEnumeration = true;
                }
            }
        }

        assertEq(
            viaEnumeration,
            blacklist.isHashBlacklistedForOperator(hash, operator),
            "enumeration union must equal the point query"
        );
    }
}
