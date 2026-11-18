// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { ICapacityBondEjector } from "../src/interfaces/ICapacityBondEjector.sol";
import { MockEjector } from "./ContentBlacklist.t.sol";

/// @title ContentBlacklist mutation ledger
/// @notice Every state change an authorised caller makes appends exactly one
///         record; a call that leaves the state as it was appends nothing;
///         replaying the ledger into a model reproduces the contract's answers
///         (ADR 011 § Following the mutation ledger).
contract ContentBlacklistMutationsTest is Test {
    MockEjector internal bondMock;
    ContentBlacklist internal blacklist;

    address internal admin = address(0xA11CE);
    address internal multisig = address(0xC0DE);
    address internal usBody = address(0xDE);
    address internal euBody = address(0xDF);
    address internal node = address(0xB0B);

    bytes32 internal constant GLOBAL = bytes32("GLOBAL");
    bytes32 internal constant REGION_US = bytes32("US");
    bytes32 internal constant REGION_EU = bytes32("EU");
    bytes32 internal constant H1 = bytes32(uint256(0xA1));
    bytes32 internal constant H2 = bytes32(uint256(0xA2));
    bytes32 internal constant H3 = bytes32(uint256(0xA3));
    address internal constant ADDR1 = address(0x1111);
    address internal constant ADDR2 = address(0x2222);

    uint8 internal constant GENERAL = 0;
    uint8 internal constant CSAM = 1;

    /// @dev `setUp` registers two bodies, so the ledger starts with two records.
    uint256 internal constant BASE = 2;

    function setUp() public {
        bondMock = new MockEjector();
        blacklist = new ContentBlacklist(ICapacityBondEjector(address(bondMock)), admin);
        bytes32 multisigRole = blacklist.EMERGENCY_MULTISIG_ROLE();
        vm.startPrank(admin);
        blacklist.grantRole(multisigRole, multisig);
        blacklist.registerRegionalBody(REGION_US, usBody, multisig);
        blacklist.registerRegionalBody(REGION_EU, euBody, multisig);
        vm.stopPrank();
        // The node sits in US and moved from EU at deploy time, so EU stays in
        // scope until the ripening window closes.
        bondMock.setRegion(node, "US", "EU", uint64(block.timestamp));
    }

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------

    /// @dev Records appended since `setUp`.
    function _n() internal view returns (uint256) {
        return blacklist.mutationCount() - BASE;
    }

    function _last() internal view returns (ContentBlacklist.Mutation memory) {
        uint256 n = blacklist.mutationCount();
        ContentBlacklist.Mutation[] memory page = blacklist.getMutations(n - 1, 1);
        return page[0];
    }

    function _assertRecord(
        ContentBlacklist.Mutation memory m,
        ContentBlacklist.MutationKind kind,
        bytes32 region,
        bytes32 key,
        uint64 expiresAt
    ) internal pure {
        assertEq(uint8(m.kind), uint8(kind), "kind");
        assertEq(m.expiresAt, expiresAt, "expiresAt");
        assertEq(m.region, region, "region");
        assertEq(m.key, key, "key");
    }

    function _assertLast(ContentBlacklist.MutationKind kind, bytes32 region, bytes32 key, uint64 expiresAt)
        internal
        view
    {
        ContentBlacklist.Mutation memory m = _last();
        assertEq(m.at, uint64(block.timestamp), "at");
        _assertRecord(m, kind, region, key, expiresAt);
    }

    function _key(address a) internal pure returns (bytes32) {
        return bytes32(uint256(uint160(a)));
    }

    // -----------------------------------------------------------------
    // Hash records
    // -----------------------------------------------------------------

    function test_setUp_recordsBodyRegistrations() public view {
        assertEq(blacklist.mutationCount(), BASE);
        ContentBlacklist.Mutation[] memory log = blacklist.getMutations(0, BASE);
        _assertRecord(log[0], ContentBlacklist.MutationKind.BODY_REGISTER, REGION_US, _key(usBody), 0);
        _assertRecord(log[1], ContentBlacklist.MutationKind.BODY_REGISTER, REGION_EU, _key(euBody), 0);
    }

    function test_addHashGlobal_appendsHashAdd() public {
        vm.prank(admin);
        blacklist.addHashGlobal(H1, "r");
        assertEq(_n(), 1);
        _assertLast(ContentBlacklist.MutationKind.HASH_ADD, bytes32(0), H1, 0);
    }

    /// @notice A live re-add carries a new notice (its reason), so it appends.
    function test_addHashGlobal_liveReAdd_appendsNewNotice() public {
        vm.startPrank(admin);
        blacklist.addHashGlobal(H1, "r");
        vm.warp(block.timestamp + 1 days);
        blacklist.addHashGlobal(H1, "r2");
        vm.stopPrank();
        assertEq(_n(), 2);
        _assertLast(ContentBlacklist.MutationKind.HASH_ADD, bytes32(0), H1, 0);
    }

    function test_addHashRegional_appendsHashAddWithRegion() public {
        vm.prank(usBody);
        blacklist.addHashRegional(REGION_US, H1, "r");
        assertEq(_n(), 1);
        _assertLast(ContentBlacklist.MutationKind.HASH_ADD, REGION_US, H1, 0);

        vm.prank(usBody);
        blacklist.addHashRegional(REGION_US, H1, "r2");
        assertEq(_n(), 2, "live regional re-add");
        _assertLast(ContentBlacklist.MutationKind.HASH_ADD, REGION_US, H1, 0);
    }

    function test_emergencyAdd_appendsExpiry() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(H1, GENERAL, "r");
        assertEq(_n(), 1);
        _assertLast(ContentBlacklist.MutationKind.HASH_EMERGENCY_ADD, bytes32(0), H1, uint64(block.timestamp) + 14 days);
    }

    /// @notice A lower category never shortens the term; the record carries the
    ///         entry's resulting deadline.
    function test_emergencyAdd_lowerCategoryReAdd_keepsDeadline() public {
        uint64 addedAt = uint64(block.timestamp);
        vm.prank(multisig);
        blacklist.emergencyAdd(H1, CSAM, "r");
        vm.warp(block.timestamp + 1 days);
        vm.prank(multisig);
        blacklist.emergencyAdd(H1, GENERAL, "r2");
        assertEq(_n(), 2);
        _assertLast(ContentBlacklist.MutationKind.HASH_EMERGENCY_ADD, bytes32(0), H1, addedAt + 90 days);
    }

    function test_emergencyAdd_escalation_appendsLongerExpiry() public {
        uint64 addedAt = uint64(block.timestamp);
        vm.prank(multisig);
        blacklist.emergencyAdd(H1, GENERAL, "r");
        vm.warp(block.timestamp + 1 days);
        vm.prank(multisig);
        blacklist.emergencyAdd(H1, CSAM, "r2");
        assertEq(_n(), 2);
        _assertLast(ContentBlacklist.MutationKind.HASH_EMERGENCY_ADD, bytes32(0), H1, addedAt + 90 days);
    }

    function test_emergencyAdd_overLapsed_appendsFreshExpiry() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(H1, GENERAL, "r");
        vm.warp(block.timestamp + 15 days);
        vm.prank(multisig);
        blacklist.emergencyAdd(H1, GENERAL, "r2");
        assertEq(_n(), 2);
        _assertLast(ContentBlacklist.MutationKind.HASH_EMERGENCY_ADD, bytes32(0), H1, uint64(block.timestamp) + 14 days);
    }

    function test_ratify_appendsRatifyWithoutExpiry() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(H1, GENERAL, "r");
        vm.prank(admin);
        blacklist.addHashGlobal(H1, "ratified");
        assertEq(_n(), 2);
        _assertLast(ContentBlacklist.MutationKind.HASH_RATIFY, bytes32(0), H1, 0);
    }

    function test_addHashGlobal_overLapsedEmergency_appendsHashAdd() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(H1, GENERAL, "r");
        vm.warp(block.timestamp + 15 days);
        vm.prank(admin);
        blacklist.addHashGlobal(H1, "g");
        assertEq(_n(), 2);
        _assertLast(ContentBlacklist.MutationKind.HASH_ADD, bytes32(0), H1, 0);
    }

    function test_removeHashGlobal_appendsHashRemove() public {
        vm.startPrank(admin);
        blacklist.addHashGlobal(H1, "r");
        blacklist.removeHashGlobal(H1);
        vm.stopPrank();
        assertEq(_n(), 2);
        _assertLast(ContentBlacklist.MutationKind.HASH_REMOVE, bytes32(0), H1, 0);
    }

    function test_removeHashRegional_appendsHashRemoveWithRegion() public {
        vm.startPrank(usBody);
        blacklist.addHashRegional(REGION_US, H1, "r");
        blacklist.removeHashRegional(REGION_US, H1);
        vm.stopPrank();
        assertEq(_n(), 2);
        _assertLast(ContentBlacklist.MutationKind.HASH_REMOVE, REGION_US, H1, 0);
    }

    function test_expireEmergencyEntry_appendsHashExpire() public {
        vm.prank(multisig);
        blacklist.emergencyAdd(H1, GENERAL, "r");
        vm.warp(block.timestamp + 15 days);
        blacklist.expireEmergencyEntry(GLOBAL, H1);
        assertEq(_n(), 2);
        _assertLast(ContentBlacklist.MutationKind.HASH_EXPIRE, bytes32(0), H1, 0);
    }

    // -----------------------------------------------------------------
    // Address records
    // -----------------------------------------------------------------

    function test_setOriginBlacklist_appendsSetAndClear_skipsNoOps() public {
        vm.startPrank(admin);
        blacklist.setOriginBlacklist(ADDR1, true);
        _assertLast(ContentBlacklist.MutationKind.ORIGIN_SET, bytes32(0), _key(ADDR1), 0);
        blacklist.setOriginBlacklist(ADDR1, true);
        assertEq(_n(), 1, "repeat set");
        blacklist.setOriginBlacklist(ADDR1, false);
        _assertLast(ContentBlacklist.MutationKind.ORIGIN_CLEAR, bytes32(0), _key(ADDR1), 0);
        blacklist.setOriginBlacklist(ADDR1, false);
        assertEq(_n(), 2, "repeat clear");
        vm.stopPrank();
    }

    function test_emergencyAddOrigin_appendsExpiry() public {
        vm.prank(multisig);
        blacklist.emergencyAddOrigin(ADDR1, CSAM, "r");
        assertEq(_n(), 1);
        _assertLast(
            ContentBlacklist.MutationKind.ORIGIN_EMERGENCY_ADD,
            bytes32(0),
            _key(ADDR1),
            uint64(block.timestamp) + 90 days
        );
    }

    function test_setOriginBlacklist_ratifiesEmergencyOrigin() public {
        vm.prank(multisig);
        blacklist.emergencyAddOrigin(ADDR1, GENERAL, "r");
        vm.prank(admin);
        blacklist.setOriginBlacklist(ADDR1, true);
        assertEq(_n(), 2);
        _assertLast(ContentBlacklist.MutationKind.ORIGIN_SET, bytes32(0), _key(ADDR1), 0);
    }

    function test_expireEmergencyOrigin_appendsOriginExpire() public {
        vm.prank(multisig);
        blacklist.emergencyAddOrigin(ADDR1, GENERAL, "r");
        vm.warp(block.timestamp + 15 days);
        blacklist.expireEmergencyOrigin(ADDR1);
        assertEq(_n(), 2);
        _assertLast(ContentBlacklist.MutationKind.ORIGIN_EXPIRE, bytes32(0), _key(ADDR1), 0);
    }

    function test_operator_appendsAddAndRemove_skipsNoOps() public {
        vm.startPrank(admin);
        blacklist.addOperator(ADDR1);
        _assertLast(ContentBlacklist.MutationKind.OPERATOR_ADD, bytes32(0), _key(ADDR1), 0);
        blacklist.addOperator(ADDR1);
        assertEq(_n(), 1, "repeat add");
        blacklist.removeOperator(ADDR1);
        _assertLast(ContentBlacklist.MutationKind.OPERATOR_REMOVE, bytes32(0), _key(ADDR1), 0);
        blacklist.removeOperator(ADDR1);
        assertEq(_n(), 2, "repeat remove");
        vm.stopPrank();
    }

    // -----------------------------------------------------------------
    // Body and window records
    // -----------------------------------------------------------------

    function test_bodyLifecycle_appendsEachChange() public {
        vm.prank(multisig);
        blacklist.suspendRegionalBody(REGION_US);
        _assertLast(
            ContentBlacklist.MutationKind.BODY_SUSPEND, REGION_US, _key(usBody), uint64(block.timestamp) + 14 days
        );

        vm.startPrank(admin);
        blacklist.ratifyRegionalBodySuspension(REGION_US);
        _assertLast(ContentBlacklist.MutationKind.BODY_SUSPENSION_RATIFY, REGION_US, _key(usBody), 0);
        blacklist.ratifyRegionalBodySuspension(REGION_US);
        assertEq(_n(), 2, "repeat ratify");

        blacklist.unsuspendRegionalBody(REGION_US);
        _assertLast(ContentBlacklist.MutationKind.BODY_UNSUSPEND, REGION_US, _key(usBody), 0);

        blacklist.deregisterRegionalBody(REGION_US);
        _assertLast(ContentBlacklist.MutationKind.BODY_DEREGISTER, REGION_US, _key(usBody), 0);

        blacklist.registerRegionalBody(REGION_US, usBody, multisig);
        _assertLast(ContentBlacklist.MutationKind.BODY_REGISTER, REGION_US, _key(usBody), 0);
        vm.stopPrank();
        assertEq(_n(), 5);
    }

    /// @notice Body changes move no deny answer: a suspended or deregistered
    ///         body's entries stay live, so a node applies body records without
    ///         touching its hash index.
    function test_bodyChanges_leaveEntriesLive() public {
        vm.prank(usBody);
        blacklist.addHashRegional(REGION_US, H1, "r");
        vm.prank(multisig);
        blacklist.suspendRegionalBody(REGION_US);
        assertTrue(blacklist.isHashBlacklistedInRegion(H1, REGION_US));
        vm.prank(admin);
        blacklist.deregisterRegionalBody(REGION_US);
        assertTrue(blacklist.isHashBlacklistedInRegion(H1, REGION_US));
    }

    function test_complianceWindows_appendNewValue_skipNoOps() public {
        vm.startPrank(admin);
        blacklist.setComplianceWindow(2 hours);
        _assertLast(ContentBlacklist.MutationKind.COMPLIANCE_WINDOW_SET, bytes32(0), bytes32(uint256(2 hours)), 0);
        blacklist.setEmergencyComplianceWindow(3 hours);
        _assertLast(
            ContentBlacklist.MutationKind.EMERGENCY_COMPLIANCE_WINDOW_SET, bytes32(0), bytes32(uint256(3 hours)), 0
        );
        blacklist.setComplianceWindow(2 hours);
        blacklist.setEmergencyComplianceWindow(3 hours);
        vm.stopPrank();
        assertEq(_n(), 2);
    }

    // -----------------------------------------------------------------
    // Paging
    // -----------------------------------------------------------------

    function test_getMutations_pagesAndClamps() public {
        vm.startPrank(admin);
        blacklist.addHashGlobal(H1, "r");
        blacklist.addHashGlobal(H2, "r");
        blacklist.addHashGlobal(H3, "r");
        vm.stopPrank();
        uint256 total = BASE + 3;
        assertEq(blacklist.mutationCount(), total);

        ContentBlacklist.Mutation[] memory all = blacklist.getMutations(BASE, 10);
        assertEq(all.length, 3);
        assertEq(all[0].key, H1);
        assertEq(all[1].key, H2);
        assertEq(all[2].key, H3);

        ContentBlacklist.Mutation[] memory mid = blacklist.getMutations(BASE + 1, 1);
        assertEq(mid.length, 1);
        assertEq(mid[0].key, H2);

        assertEq(blacklist.getMutations(1, type(uint256).max).length, total - 1, "no from+limit overflow");
        assertEq(blacklist.getMutations(type(uint256).max, type(uint256).max).length, 0, "from at max");
        assertEq(blacklist.getMutations(total, 5).length, 0, "from == count");
        assertEq(blacklist.getMutations(0, 0).length, 0, "zero limit");
    }

    // -----------------------------------------------------------------
    // Replay model
    // -----------------------------------------------------------------

    struct ModelEntry {
        bool present;
        uint64 expiresAt;
    }

    struct ModelBody {
        bytes32 body;
        bool suspended;
        uint64 expiresAt;
    }

    mapping(bytes32 region => mapping(bytes32 key => ModelEntry)) internal _mHash;
    mapping(bytes32 key => ModelEntry) internal _mOrigin;
    mapping(bytes32 key => bool) internal _mOperator;
    mapping(bytes32 region => ModelBody) internal _mBody;
    uint64 internal _mWindow = 24 hours;
    uint64 internal _mEmergencyWindow = 2 hours;

    /// @dev Each record overwrites the whole state of its key, so the model is
    ///      a plain last-writer-wins map.
    function _replay() internal {
        ContentBlacklist.Mutation[] memory log = blacklist.getMutations(0, blacklist.mutationCount());
        for (uint256 i = 0; i < log.length; ++i) {
            _apply(log[i]);
        }
    }

    function _apply(ContentBlacklist.Mutation memory m) internal {
        bytes32 region = m.region == bytes32(0) ? GLOBAL : m.region;
        ContentBlacklist.MutationKind k = m.kind;
        if (
            k == ContentBlacklist.MutationKind.HASH_ADD || k == ContentBlacklist.MutationKind.HASH_EMERGENCY_ADD
                || k == ContentBlacklist.MutationKind.HASH_RATIFY
        ) {
            _mHash[region][m.key] = ModelEntry(true, m.expiresAt);
        } else if (k == ContentBlacklist.MutationKind.HASH_REMOVE || k == ContentBlacklist.MutationKind.HASH_EXPIRE) {
            delete _mHash[region][m.key];
        } else if (
            k == ContentBlacklist.MutationKind.ORIGIN_SET || k == ContentBlacklist.MutationKind.ORIGIN_EMERGENCY_ADD
        ) {
            _mOrigin[m.key] = ModelEntry(true, m.expiresAt);
        } else if (k == ContentBlacklist.MutationKind.ORIGIN_CLEAR || k == ContentBlacklist.MutationKind.ORIGIN_EXPIRE)
        {
            delete _mOrigin[m.key];
        } else if (k == ContentBlacklist.MutationKind.OPERATOR_ADD) {
            _mOperator[m.key] = true;
        } else if (k == ContentBlacklist.MutationKind.OPERATOR_REMOVE) {
            _mOperator[m.key] = false;
        } else if (k == ContentBlacklist.MutationKind.BODY_REGISTER) {
            _mBody[m.region] = ModelBody(m.key, false, 0);
        } else if (k == ContentBlacklist.MutationKind.BODY_DEREGISTER) {
            delete _mBody[m.region];
        } else if (
            k == ContentBlacklist.MutationKind.BODY_SUSPEND || k == ContentBlacklist.MutationKind.BODY_SUSPENSION_RATIFY
        ) {
            _mBody[m.region] = ModelBody(m.key, true, m.expiresAt);
        } else if (k == ContentBlacklist.MutationKind.BODY_UNSUSPEND) {
            _mBody[m.region] = ModelBody(m.key, false, 0);
        } else if (k == ContentBlacklist.MutationKind.COMPLIANCE_WINDOW_SET) {
            _mWindow = uint64(uint256(m.key));
        } else {
            _mEmergencyWindow = uint64(uint256(m.key));
        }
    }

    function _live(bool present, uint64 expiresAt) internal view returns (bool) {
        // forge-lint: disable-next-line(block-timestamp)
        return present && (expiresAt == 0 || block.timestamp <= expiresAt);
    }

    function _liveEntry(ModelEntry storage e) internal view returns (bool) {
        return _live(e.present, e.expiresAt);
    }

    function _modelForOperator(bytes32 hash) internal view returns (bool) {
        bytes32[] memory scope = blacklist.getScopeRegions(node);
        for (uint256 i = 0; i < scope.length; ++i) {
            if (_liveEntry(_mHash[scope[i]][hash])) return true;
        }
        return false;
    }

    function _assertModelMatches() internal view {
        bytes32[3] memory hashes = [H1, H2, H3];
        bytes32[3] memory regions = [GLOBAL, REGION_US, REGION_EU];
        for (uint256 h = 0; h < 3; ++h) {
            bytes32 hash = hashes[h];
            assertEq(blacklist.isHashBlacklisted(hash), _liveEntry(_mHash[GLOBAL][hash]), "global");
            assertEq(blacklist.isHashBlacklistedForOperator(hash, node), _modelForOperator(hash), "for operator");
            for (uint256 r = 1; r < 3; ++r) {
                bool want = _liveEntry(_mHash[GLOBAL][hash]) || _liveEntry(_mHash[regions[r]][hash]);
                assertEq(blacklist.isHashBlacklistedInRegion(hash, regions[r]), want, "in region");
            }
        }
        for (uint256 r = 0; r < 3; ++r) {
            uint256 raw;
            for (uint256 h = 0; h < 3; ++h) {
                if (_mHash[regions[r]][hashes[h]].present) ++raw;
            }
            assertEq(blacklist.blacklistedHashCount(regions[r]), raw, "raw membership");
        }
        for (uint256 r = 1; r < 3; ++r) {
            ModelBody storage b = _mBody[regions[r]];
            assertEq(_key(blacklist.getRegionalBody(regions[r]).body), b.body, "body");
            assertEq(blacklist.isRegionalBodySuspended(regions[r]), _live(b.suspended, b.expiresAt), "suspended");
        }
        assertEq(blacklist.complianceWindow(), _mWindow, "window");
        assertEq(blacklist.emergencyComplianceWindow(), _mEmergencyWindow, "emergency window");
        address[2] memory addrs = [ADDR1, ADDR2];
        for (uint256 a = 0; a < 2; ++a) {
            assertEq(blacklist.isOriginBlacklisted(addrs[a]), _liveEntry(_mOrigin[_key(addrs[a])]), "origin");
            assertEq(blacklist.isOperatorBlacklisted(addrs[a]), _mOperator[_key(addrs[a])], "operator");
        }
    }

    /// @notice An add then a remove of the same key leaves the count of
    ///         entries unchanged but moves the ledger twice: a cursor sees both.
    function test_replay_addRemovePairAndExpiryByTime() public {
        vm.prank(admin);
        blacklist.addHashGlobal(H1, "r");
        vm.prank(admin);
        blacklist.removeHashGlobal(H1);
        vm.prank(admin);
        blacklist.addHashGlobal(H2, "r");
        vm.prank(multisig);
        blacklist.emergencyAdd(H3, GENERAL, "r");
        vm.prank(multisig);
        blacklist.emergencyAddOrigin(ADDR1, GENERAL, "r");
        vm.prank(multisig);
        blacklist.suspendRegionalBody(REGION_EU);
        assertEq(blacklist.blacklistedHashCount(GLOBAL), 2);
        assertEq(_n(), 6);

        _replay();
        _assertModelMatches();
        assertTrue(blacklist.isHashBlacklisted(H3));
        assertTrue(blacklist.isRegionalBodySuspended(REGION_EU));

        // Expiry by time alone: no write, no record, both sides drop it.
        vm.warp(block.timestamp + 14 days + 1);
        assertEq(_n(), 6);
        _assertModelMatches();
        assertFalse(blacklist.isHashBlacklisted(H3));
        assertFalse(blacklist.isOriginBlacklisted(ADDR1));
        assertFalse(blacklist.isRegionalBodySuspended(REGION_EU));
    }

    /// @notice A random sequence of writes and time jumps, replayed from the
    ///         ledger, reproduces every deny answer, the raw hash membership,
    ///         the body state and the compliance windows.
    function testFuzz_replayReproducesAnswers(uint256 seed) public {
        bytes32[3] memory hashes = [H1, H2, H3];
        address[2] memory addrs = [ADDR1, ADDR2];
        for (uint256 step = 0; step < 32; ++step) {
            uint256 r = uint256(keccak256(abi.encode(seed, step)));
            bytes32 hash = hashes[(r >> 8) % 3];
            address a = addrs[(r >> 16) % 2];
            uint8 cat = uint8((r >> 24) % 3);
            bool eu = ((r >> 32) & 1) == 1;
            bytes32 region = eu ? REGION_EU : REGION_US;
            address body = eu ? euBody : usBody;
            _step(r % 20, r, hash, a, cat, region, body);
        }

        _replay();
        _assertModelMatches();
        // Time alone moves expiry, suspension lapse and ripening; the model
        // follows from `expiresAt` and the scope read with no new record.
        vm.warp(block.timestamp + 91 days);
        _assertModelMatches();
    }

    function _step(uint256 op, uint256 r, bytes32 hash, address a, uint8 cat, bytes32 region, address body) internal {
        if (op == 0) {
            vm.prank(admin);
            try blacklist.addHashGlobal(hash, "g") { } catch { }
        } else if (op == 1) {
            vm.prank(admin);
            try blacklist.removeHashGlobal(hash) { } catch { }
        } else if (op == 2) {
            vm.prank(body);
            try blacklist.addHashRegional(region, hash, "r") { } catch { }
        } else if (op == 3) {
            vm.prank(body);
            try blacklist.removeHashRegional(region, hash) { } catch { }
        } else if (op == 4) {
            vm.prank(multisig);
            try blacklist.emergencyAdd(hash, cat, "e") { } catch { }
        } else if (op == 5) {
            try blacklist.expireEmergencyEntry(GLOBAL, hash) { } catch { }
        } else if (op == 6) {
            vm.prank(admin);
            try blacklist.setOriginBlacklist(a, true) { } catch { }
        } else if (op == 7) {
            vm.prank(admin);
            try blacklist.setOriginBlacklist(a, false) { } catch { }
        } else if (op == 8) {
            vm.prank(multisig);
            try blacklist.emergencyAddOrigin(a, cat, "e") { } catch { }
        } else if (op == 9) {
            try blacklist.expireEmergencyOrigin(a) { } catch { }
        } else if (op == 10) {
            vm.prank(admin);
            try blacklist.addOperator(a) { } catch { }
        } else if (op == 11) {
            vm.prank(admin);
            try blacklist.removeOperator(a) { } catch { }
        } else if (op == 12) {
            vm.prank(multisig);
            try blacklist.suspendRegionalBody(region) { } catch { }
        } else if (op == 13) {
            vm.prank(admin);
            try blacklist.ratifyRegionalBodySuspension(region) { } catch { }
        } else if (op == 14) {
            vm.prank(admin);
            try blacklist.unsuspendRegionalBody(region) { } catch { }
        } else if (op == 15) {
            vm.prank(admin);
            try blacklist.deregisterRegionalBody(region) { } catch { }
        } else if (op == 16) {
            vm.prank(admin);
            try blacklist.registerRegionalBody(region, body, multisig) { } catch { }
        } else if (op == 17) {
            vm.prank(admin);
            try blacklist.setComplianceWindow(uint64(1 hours + (r >> 48) % 3 hours)) { } catch { }
        } else if (op == 18) {
            vm.prank(admin);
            try blacklist.setEmergencyComplianceWindow(uint64(1 hours + (r >> 48) % 3 hours)) { } catch { }
        } else {
            vm.warp(block.timestamp + (r >> 40) % 20 days);
        }
    }
}
