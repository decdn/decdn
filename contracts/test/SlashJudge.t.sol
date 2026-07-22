// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { SlashJudge } from "../src/SlashJudge.sol";
import { SunsettingPausable } from "../src/SunsettingPausable.sol";
import { ISlashJudge } from "../src/interfaces/ISlashJudge.sol";
import { ICapacityBondSlasher } from "../src/interfaces/ICapacityBondSlasher.sol";
import { ICapacityBondRegionView } from "../src/interfaces/ICapacityBondRegionView.sol";
import { IContentBlacklistHashView } from "../src/interfaces/IContentBlacklistHashView.sol";

contract MockToken is ERC20 {
    constructor() ERC20("TOKEN", "TKN") {
        _mint(msg.sender, 1_000_000_000e18);
    }
}

contract MockSlasher is ICapacityBondSlasher, ICapacityBondRegionView {
    uint256 public unbondingPeriodValue;
    uint256 public nextId = 1;
    uint256 public returnAmount;
    mapping(address => bytes32) public boundNodeId;

    address public lastOperator;
    address public lastChallenger;
    uint8 public lastOffense;
    uint256 public slashCount;

    // ADR 030 region-scope read source (production `CapacityBond` is both the
    // slasher and the region view at one address). Defaults to empty region =>
    // global-only behavior, so existing global blacklist tests are unaffected.
    mapping(address => string) internal _regionHint;
    mapping(address => string) internal _regionPrev;
    mapping(address => uint64) internal _regionLastChanged;
    mapping(address => uint64) internal _firstBondedAt;
    uint64 public gateActivatedAt;
    uint256 public window = 7 days;

    constructor(uint256 unbonding_) {
        unbondingPeriodValue = unbonding_;
    }

    function setBound(address operator, bytes32 nodeId) external {
        boundNodeId[operator] = nodeId;
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

    function setReturnAmount(uint256 amount) external {
        returnAmount = amount;
    }

    function unbondingPeriod() external view override returns (uint256) {
        return unbondingPeriodValue;
    }

    function nodeIdOf(address operator) external view override returns (bytes32, bool) {
        bytes32 id = boundNodeId[operator];
        return (id, id != bytes32(0));
    }

    function slash(address operator, address challenger, uint8 offenseType)
        external
        override
        returns (uint256, uint256)
    {
        lastOperator = operator;
        lastChallenger = challenger;
        lastOffense = offenseType;
        slashCount++;
        return (nextId++, returnAmount);
    }
}

contract MockBlacklistView is IContentBlacklistHashView {
    struct Entry {
        uint64 addedAt;
        bool suspended;
        uint64 effectiveAt;
        bool emergency;
        uint8 category;
    }

    mapping(bytes32 => mapping(bytes32 => Entry)) internal _entries;

    /// @dev Seeds an entry that is already past its compliance window
    ///      (`effectiveAt == addedAt`), which is what the pre-#1169 tests
    ///      implicitly assumed. Use `setEntryEffectiveAt` to exercise the grace.
    function setEntry(bytes32 region, bytes32 hash, uint64 addedAt) external {
        _entries[region][hash] = Entry(addedAt, false, addedAt, false, 0);
    }

    function setEntrySuspended(bytes32 region, bytes32 hash, uint64 addedAt, bool suspended) external {
        _entries[region][hash] = Entry(addedAt, suspended, addedAt, false, 0);
    }

    /// @dev ADR 011 § Compliance Window: `effectiveAt = addedAt + window`.
    function setEntryEffectiveAt(bytes32 region, bytes32 hash, uint64 addedAt, uint64 effectiveAt) external {
        _entries[region][hash] = Entry(addedAt, false, effectiveAt, false, 0);
    }

    function setEmergencyEntry(bytes32 region, bytes32 hash, uint64 addedAt, uint64 effectiveAt, uint8 category)
        external
    {
        _entries[region][hash] = Entry(addedAt, false, effectiveAt, true, category);
    }

    function getHashEntry(bytes32 region, bytes32 hash)
        external
        view
        override
        returns (uint64, bool, uint64, bool, uint8)
    {
        Entry memory e = _entries[region][hash];
        return (e.addedAt, e.suspended, e.effectiveAt, e.emergency, e.category);
    }
}

contract SlashJudgeTest is Test {
    MockToken internal token;
    MockSlasher internal slasher;
    MockBlacklistView internal blacklist;
    SlashJudge internal judge;

    uint256 internal constant NODE_PK = 0x4E0DE;
    address internal node;
    address internal challenger = address(0xC4A11E);
    address internal challenger2 = address(0xC0FFEE);
    address internal admin = address(0xA11CE);
    address internal pauser = address(0xDEAD);
    address internal stranger = address(0x5747A);

    bytes32 internal constant NODE_ID = bytes32(uint256(0xBEEF));
    bytes32 internal constant BLOB = bytes32(uint256(0xB10B));

    uint256 internal constant CHALLENGE_BOND = 100e18;
    uint256 internal constant MAX_EVIDENCE_AGE_US = 5 days * 1_000_000;
    uint256 internal constant UNBONDING = 14 days;

    // Mirror of the contract's fixed commit–reveal bounds (#854). Kept in the
    // test as literals because the contract exposes them only as internal
    // constants; a divergence would surface as a RevealTooEarly/Expired failure.
    uint256 internal constant REVEAL_DELAY = 1 minutes;
    uint256 internal constant REVEAL_WINDOW = 1 days;
    bytes32 internal constant SALT = bytes32(uint256(0x5A17));

    bytes32 internal constant GLOBAL_REGION = bytes32("GLOBAL");
    bytes32 internal constant GOVERNANCE_ROLE = keccak256("GOVERNANCE_ROLE");
    bytes32 internal constant PAUSER_ROLE = keccak256("PAUSER_ROLE");
    bytes32 internal constant PROBE_TYPEHASH =
        keccak256("ProbeResponse(bytes32 hash,bool hasBlob,uint64 ratePerMb,uint64 timestampUs)");
    bytes32 internal constant STREAM_TYPEHASH = keccak256(
        "StreamResponse(bytes32 hash,bool ok,uint64 ratePerMb,uint64 totalBytes,"
        "bytes32 channelId,uint64 timestampUs,bytes32 redirect)"
    );

    uint64 internal probeTs;
    uint64 internal streamTs;

    function setUp() public {
        node = vm.addr(NODE_PK);

        token = new MockToken();
        slasher = new MockSlasher(UNBONDING);
        blacklist = new MockBlacklistView();

        judge = new SlashJudge(slasher, IERC20(address(token)), blacklist, CHALLENGE_BOND, MAX_EVIDENCE_AGE_US, admin);

        vm.prank(admin);
        judge.grantRole(PAUSER_ROLE, pauser);

        slasher.setBound(node, NODE_ID);
        slasher.setReturnAmount(2500e18);

        token.transfer(challenger, 10_000e18);
        vm.prank(challenger);
        token.approve(address(judge), type(uint256).max);

        // Anchor a realistic clock so evidence timestamps land inside the window.
        vm.warp(1_000_000);
        uint256 nowUs = block.timestamp * 1_000_000;
        probeTs = uint64(nowUs - 10_000_000); // 10s ago
        streamTs = uint64(nowUs - 5_000_000); // 5s ago (5s after probe)
    }

    // -----------------------------------------------------------------
    // Evidence builders + signing
    // -----------------------------------------------------------------

    function _probe(bool hasBlob, uint64 ratePerMb, uint64 ts) internal pure returns (SlashJudge.ProbeMsg memory) {
        return SlashJudge.ProbeMsg({ hash: BLOB, hasBlob: hasBlob, ratePerMb: ratePerMb, timestampUs: ts });
    }

    function _stream(bool ok, uint64 ratePerMb, uint64 ts) internal pure returns (SlashJudge.StreamMsg memory) {
        return SlashJudge.StreamMsg({
            hash: BLOB,
            ok: ok,
            ratePerMb: ratePerMb,
            totalBytes: 1_048_576,
            channelId: bytes32(uint256(1)),
            timestampUs: ts,
            redirect: bytes32(0)
        });
    }

    function _probeStructHash(SlashJudge.ProbeMsg memory p) internal pure returns (bytes32) {
        return keccak256(abi.encode(PROBE_TYPEHASH, p.hash, p.hasBlob, p.ratePerMb, p.timestampUs));
    }

    function _streamStructHash(SlashJudge.StreamMsg memory s) internal pure returns (bytes32) {
        return keccak256(
            abi.encode(STREAM_TYPEHASH, s.hash, s.ok, s.ratePerMb, s.totalBytes, s.channelId, s.timestampUs, s.redirect)
        );
    }

    function _digest(bytes32 structHash) internal view returns (bytes32) {
        bytes32 ds = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes("deCDN SlashJudge")),
                keccak256(bytes("1")),
                block.chainid,
                address(judge)
            )
        );
        return keccak256(abi.encodePacked("\x19\x01", ds, structHash));
    }

    function _signProbe(SlashJudge.ProbeMsg memory p) internal view returns (bytes memory) {
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(NODE_PK, _digest(_probeStructHash(p)));
        return abi.encodePacked(r, s, v);
    }

    function _signStream(SlashJudge.StreamMsg memory st) internal view returns (bytes memory) {
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(NODE_PK, _digest(_streamStructHash(st)));
        return abi.encodePacked(r, s, v);
    }

    // -----------------------------------------------------------------
    // Commit–reveal helpers (#854)
    // -----------------------------------------------------------------

    function _pairHash(SlashJudge.ProbeMsg memory p, SlashJudge.StreamMsg memory s, ISlashJudge.OffenseType offense)
        internal
        pure
        returns (bytes32)
    {
        return keccak256(abi.encode(uint8(offense), _probeStructHash(p), _streamStructHash(s)));
    }

    function _blacklistHash(bytes32 structHash, bool isStream) internal pure returns (bytes32) {
        return keccak256(abi.encode(uint8(ISlashJudge.OffenseType.Blacklist), structHash, isStream));
    }

    /// @dev Commit `evidenceHash` as `who` (salt `SALT`) without advancing time.
    function _commitAs(address who, bytes32 evidenceHash) internal {
        vm.prank(who);
        judge.commitChallenge(keccak256(abi.encode(evidenceHash, SALT, who)));
    }

    /// @dev Commit as `challenger` and warp past `MIN_REVEAL_DELAY` so the next
    ///      `submit*Challenge` reveal is valid.
    function _commitAndMature(bytes32 evidenceHash) internal {
        _commitAs(challenger, evidenceHash);
        vm.warp(block.timestamp + REVEAL_DELAY + 1);
    }

    // -----------------------------------------------------------------
    // Phantom
    // -----------------------------------------------------------------

    function test_phantom_slashesAndRoundTripsBond() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);

        uint256 balBefore = token.balanceOf(challenger);
        _commitAndMature(_pairHash(p, s, ISlashJudge.OffenseType.Phantom));
        vm.prank(challenger);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);

        assertEq(slasher.slashCount(), 1);
        assertEq(slasher.lastOperator(), node);
        assertEq(slasher.lastChallenger(), challenger);
        assertEq(slasher.lastOffense(), uint8(ISlashJudge.OffenseType.Phantom));
        assertEq(token.balanceOf(challenger), balBefore); // bond returned
    }

    function test_phantom_revertsWhenStreamOk() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs); // ok=true → not phantom
        vm.prank(challenger);
        vm.expectRevert(SlashJudge.NotPhantom.selector);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
    }

    function test_phantom_revertsOnUnregisteredNode() public {
        slasher.setBound(node, bytes32(0));
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.NodeNotRegistered.selector, node));
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
    }

    function test_phantom_revertsOnNodeIdMismatch() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        bytes32 wrongId = bytes32(uint256(0xBAD));
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.NodeIdMismatch.selector, wrongId, NODE_ID));
        judge.submitPhantomChallenge(node, wrongId, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
    }

    function test_phantom_revertsOnBadProbeSignature() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        bytes memory probeSig = _signProbe(_probe(true, 99, probeTs)); // signed different rate
        vm.prank(challenger);
        vm.expectRevert(SlashJudge.InvalidProbeSignature.selector);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), probeSig, abi.encode(s), _signStream(s), SALT);
    }

    function test_phantom_revertsOutsideTimestampWindow() public {
        uint64 farStream = probeTs + 30_000_000; // exactly 30s → not < 30s
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, farStream);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.TimestampWindowViolated.selector, probeTs, farStream));
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
    }

    function test_phantom_revertsOnStaleEvidence() public {
        uint256 nowUs = block.timestamp * 1_000_000;
        uint64 oldProbe = uint64(nowUs - (6 days * 1_000_000)); // older than 5d ceiling
        uint64 oldStream = oldProbe + 5_000_000;
        SlashJudge.ProbeMsg memory p = _probe(true, 10, oldProbe);
        SlashJudge.StreamMsg memory s = _stream(false, 10, oldStream);
        vm.prank(challenger);
        vm.expectRevert(
            abi.encodeWithSelector(SlashJudge.EvidenceTooOld.selector, uint256(6 days * 1_000_000), MAX_EVIDENCE_AGE_US)
        );
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
    }

    function test_phantom_revertsOnFutureEvidence() public {
        uint256 nowUs = block.timestamp * 1_000_000;
        uint64 futureProbe = uint64(nowUs + 120_000_000); // 120s > 60s skew
        uint64 futureStream = futureProbe + 5_000_000;
        SlashJudge.ProbeMsg memory p = _probe(true, 10, futureProbe);
        SlashJudge.StreamMsg memory s = _stream(false, 10, futureStream);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.EvidenceInFuture.selector, futureProbe));
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
    }

    function test_phantom_revertsOnEvidenceReplay() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        bytes32 evidenceHash = _pairHash(p, s, ISlashJudge.OffenseType.Phantom);

        _commitAndMature(evidenceHash);
        vm.prank(challenger);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);

        // Re-commit the same proof: the commitment check now passes but the
        // evidence replay guard must still reject it (no offense-count ratchet).
        _commitAndMature(evidenceHash);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.EvidenceAlreadyUsed.selector, evidenceHash));
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);

        assertEq(slasher.slashCount(), 1);
        assertTrue(judge.usedEvidenceHash(evidenceHash));
    }

    // -----------------------------------------------------------------
    // Rate manipulation
    // -----------------------------------------------------------------

    function test_rate_slashesWhenStreamRateHigher() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(true, 25, streamTs); // 25 > 10
        _commitAndMature(_pairHash(p, s, ISlashJudge.OffenseType.RateManipulation));
        vm.prank(challenger);
        judge.submitRateChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
        assertEq(slasher.lastOffense(), uint8(ISlashJudge.OffenseType.RateManipulation));
    }

    function test_rate_revertsWhenNotHigher() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs); // equal → not manipulation
        vm.prank(challenger);
        vm.expectRevert(SlashJudge.NotRateManipulation.selector);
        judge.submitRateChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
    }

    // -----------------------------------------------------------------
    // Blacklist
    // -----------------------------------------------------------------

    function test_blacklist_slashesOnStreamResponse() public {
        blacklist.setEntry(GLOBAL_REGION, BLOB, uint64(block.timestamp - 1000));
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        _commitAndMature(_blacklistHash(_streamStructHash(s), true));
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
        assertEq(slasher.lastOffense(), uint8(ISlashJudge.OffenseType.Blacklist));
    }

    function test_blacklist_slashesOnProbeResponse() public {
        blacklist.setEntry(GLOBAL_REGION, BLOB, uint64(block.timestamp - 1000));
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        _commitAndMature(_blacklistHash(_probeStructHash(p), false));
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(p), _signProbe(p), false, SALT);
        assertEq(slasher.slashCount(), 1);
    }

    function test_blacklist_revertsWhenNotBlacklisted() public {
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.HashNotBlacklisted.selector, BLOB));
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
    }

    function test_blacklist_revertsWhenBlacklistedAfterResponse() public {
        blacklist.setEntry(GLOBAL_REGION, BLOB, uint64(block.timestamp + 1000)); // effective after response
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(
            abi.encodeWithSelector(
                SlashJudge.BlacklistAfterResponse.selector, uint256(block.timestamp + 1000) * 1_000_000, streamTs
            )
        );
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
    }

    // --- ADR 011 § Compliance Window (#1169) --------------------------------

    /// @notice The entry was ADDED before the response but its compliance window
    ///         had not elapsed, so the node had no cycle in which it could have
    ///         learned of it. Not slashable — this is the regression #1169
    ///         describes, where anchoring on `addedAt` punished a node for a
    ///         delivery it could not have known was prohibited.
    function test_blacklist_revertsWhenResponseInsideComplianceWindow() public {
        uint64 addedAt = uint64(block.timestamp - 1000);
        uint64 effectiveAt = addedAt + 24 hours; // still in the future
        blacklist.setEntryEffectiveAt(GLOBAL_REGION, BLOB, addedAt, effectiveAt);
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(
            abi.encodeWithSelector(
                SlashJudge.BlacklistAfterResponse.selector, uint256(effectiveAt) * 1_000_000, streamTs
            )
        );
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
    }

    /// @notice Same entry, same node — once `effectiveAt` precedes the response,
    ///         the slash lands. Pins that the window is a delay, not an immunity.
    function test_blacklist_slashesOnceComplianceWindowElapsed() public {
        uint64 addedAt = uint64(block.timestamp - 25 hours);
        blacklist.setEntryEffectiveAt(GLOBAL_REGION, BLOB, addedAt, addedAt + 24 hours);
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        _commitAndMature(_blacklistHash(_streamStructHash(s), true));
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
        assertEq(slasher.lastOffense(), uint8(ISlashJudge.OffenseType.Blacklist));
    }

    // --- ADR 011 emergency auto-expiry (#1167) ------------------------------

    /// @notice A GENERAL emergency entry past its 14-day deadline is no longer
    ///         enforceable, so it cannot ground a slash even though the response
    ///         post-dates `effectiveAt`.
    function test_blacklist_revertsWhenEmergencyEntryExpired() public {
        vm.warp(block.timestamp + 15 days);
        uint64 addedAt = uint64(block.timestamp - 15 days);
        blacklist.setEmergencyEntry(GLOBAL_REGION, BLOB, addedAt, addedAt + 2 hours, 0);
        SlashJudge.StreamMsg memory s = _stream(true, 10, uint64(block.timestamp * 1_000_000 - 1_000_000));
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.HashNotBlacklisted.selector, BLOB));
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
    }

    /// @notice A CSAM emergency entry at the same age is still inside its 90-day
    ///         deadline and remains slashable — the category, not the elapsed
    ///         time alone, decides.
    function test_blacklist_slashesOnUnexpiredSevereEmergencyEntry() public {
        vm.warp(block.timestamp + 15 days);
        uint64 addedAt = uint64(block.timestamp - 15 days);
        blacklist.setEmergencyEntry(GLOBAL_REGION, BLOB, addedAt, addedAt + 2 hours, 1); // Category.CSAM
        SlashJudge.StreamMsg memory s = _stream(true, 10, uint64(block.timestamp * 1_000_000 - 1_000_000));
        _commitAndMature(_blacklistHash(_streamStructHash(s), true));
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
        assertEq(slasher.lastOffense(), uint8(ISlashJudge.OffenseType.Blacklist));
    }

    function test_blacklist_revertsWhenSuspended() public {
        // Live (addedAt set) but fast-track-suspended → restriction lifted, not slashable.
        blacklist.setEntrySuspended(GLOBAL_REGION, BLOB, uint64(block.timestamp - 1000), true);
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.HashNotBlacklisted.selector, BLOB));
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
    }

    function test_blacklist_revertsOnHashMismatch() public {
        blacklist.setEntry(GLOBAL_REGION, BLOB, uint64(block.timestamp - 1000));
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs); // s.hash == BLOB
        bytes32 otherHash = bytes32(uint256(0xC0FFEE));
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.HashMismatch.selector, otherHash, BLOB));
        judge.submitBlacklistChallenge(node, NODE_ID, otherHash, abi.encode(s), _signStream(s), true, SALT);
    }

    // --- ADR 030 regional + ripening slash-eligibility ----------------------

    function test_blacklist_slashesOnCurrentRegionEntry() public {
        // Node's current region is "us-east"; a us-east entry is in scope.
        slasher.setRegion(node, "us-east", "", 0);
        blacklist.setEntry(bytes32("us-east"), BLOB, uint64(block.timestamp - 1000));
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        _commitAndMature(_blacklistHash(_streamStructHash(s), true));
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
        assertEq(slasher.lastOffense(), uint8(ISlashJudge.OffenseType.Blacklist));
    }

    function test_blacklist_slashesOnPrevRegionInsideWindow() public {
        // Node flipped us-east -> eu-west 1 day ago (inside the 7d window); the
        // us-east entry it was exposed to must keep applying (no flip evasion).
        slasher.setRegion(node, "eu-west", "us-east", uint64(block.timestamp - 1 days));
        blacklist.setEntry(bytes32("us-east"), BLOB, uint64(block.timestamp - 1000));
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        _commitAndMature(_blacklistHash(_streamStructHash(s), true));
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
        assertEq(slasher.slashCount(), 1);
    }

    function test_blacklist_revertsPrevRegionAfterWindow() public {
        // Same flip but 8 days ago — past the 7d window, so the prev (us-east)
        // entry no longer applies and the new region (eu-west) has no entry.
        slasher.setRegion(node, "eu-west", "us-east", uint64(block.timestamp - 8 days));
        blacklist.setEntry(bytes32("us-east"), BLOB, uint64(block.timestamp - 1000));
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.HashNotBlacklisted.selector, BLOB));
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
    }

    function test_blacklist_revertsRegionalEntryAfterResponse() public {
        // Current-region entry exists but post-dates the served response: the
        // regional leg folds the before-response check into a boolean, so this
        // surfaces the plain HashNotBlacklisted (BlacklistAfterResponse is only
        // preserved for the global leg).
        slasher.setRegion(node, "us-east", "", 0);
        blacklist.setEntry(bytes32("us-east"), BLOB, uint64(block.timestamp + 1000));
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.HashNotBlacklisted.selector, BLOB));
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
    }

    function test_blacklist_unrelatedRegionEntryDoesNotSlash() public {
        // An entry in a region the node was never in is out of scope.
        slasher.setRegion(node, "us-east", "", 0);
        blacklist.setEntry(bytes32("ap-south"), BLOB, uint64(block.timestamp - 1000));
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.HashNotBlacklisted.selector, BLOB));
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
    }

    function test_blacklist_globalAfterResponse_rescuedByRegionalLeg() public {
        // A GLOBAL entry post-dates the response, but a current-region entry
        // pre-dates it: the regional leg rescues the slash (the non-obvious
        // "don't early-revert BlacklistAfterResponse" branch).
        slasher.setRegion(node, "us-east", "", 0);
        blacklist.setEntry(GLOBAL_REGION, BLOB, uint64(block.timestamp + 1000)); // after response
        blacklist.setEntry(bytes32("us-east"), BLOB, uint64(block.timestamp - 1000)); // before response
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        _commitAndMature(_blacklistHash(_streamStructHash(s), true));
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
        assertEq(slasher.slashCount(), 1);
    }

    function test_blacklist_globalAndRegionalBothAfterResponse_revertsBlacklistAfter() public {
        // GLOBAL post-dates the response AND no regional leg rescues → the richer
        // BlacklistAfterResponse error is preserved on the global leg.
        slasher.setRegion(node, "us-east", "", 0);
        blacklist.setEntry(GLOBAL_REGION, BLOB, uint64(block.timestamp + 1000));
        blacklist.setEntry(bytes32("us-east"), BLOB, uint64(block.timestamp + 2000)); // also after
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(
            abi.encodeWithSelector(
                SlashJudge.BlacklistAfterResponse.selector, uint256(block.timestamp + 1000) * 1_000_000, streamTs
            )
        );
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
    }

    function test_blacklist_suspendedRegionalEntryDoesNotSlash() public {
        // A fast-track-suspended REGIONAL entry lifts slashability (mirrors the
        // global suspended case; the regional leg re-checks `suspended`).
        slasher.setRegion(node, "us-east", "", 0);
        blacklist.setEntrySuspended(bytes32("us-east"), BLOB, uint64(block.timestamp - 1000), true);
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.HashNotBlacklisted.selector, BLOB));
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
    }

    function test_blacklist_neverChangedFallback_ripensFromMaxBondGate() public {
        // `regionLastChanged == 0` (never-changed path) routes `effectiveSince`
        // through the `max(firstBondedAt, regionGateActivatedAt)` fallback, which
        // the other integration tests never exercise through the real
        // `regionScopeData` read. Here the more-recent stamp is the gate (1d ago);
        // the stale `firstBondedAt` (10d ago) would have closed the 7d window. The
        // prev-region (us-east) entry therefore still applies → slash, proving the
        // fallback selected the gate (the `max`), not `firstBondedAt`.
        slasher.setRegion(node, "eu-west", "us-east", 0);
        slasher.setFirstBondedAt(node, uint64(block.timestamp - 10 days));
        slasher.setGate(uint64(block.timestamp - 1 days), 7 days);
        blacklist.setEntry(bytes32("us-east"), BLOB, uint64(block.timestamp - 1000));
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        _commitAndMature(_blacklistHash(_streamStructHash(s), true));
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
        assertEq(slasher.slashCount(), 1);
    }

    function test_blacklist_neverChangedFallback_revertsAfterMaxBondGateWindow() public {
        // Same never-changed fallback, but now the `max` operand is `firstBondedAt`
        // (8d ago) and the gate is older still (9d ago). 8d ≥ the 7d window, so the
        // prev-region (us-east) entry has ripened out of scope and the current
        // region (eu-west) has no entry → revert. Proves the window closes the
        // fallback from `max(firstBondedAt, gate)` and that `firstBondedAt` is the
        // selected operand.
        slasher.setRegion(node, "eu-west", "us-east", 0);
        slasher.setFirstBondedAt(node, uint64(block.timestamp - 8 days));
        slasher.setGate(uint64(block.timestamp - 9 days), 7 days);
        blacklist.setEntry(bytes32("us-east"), BLOB, uint64(block.timestamp - 1000));
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.HashNotBlacklisted.selector, BLOB));
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
    }

    // -----------------------------------------------------------------
    // Serve-time (responseTs) ripening-window anchor (ADR 030 item 2, #801)
    // -----------------------------------------------------------------

    function test_blacklist_serveTimeWindow_stalledChallengeStillSlashes() public {
        // Edge 2 (#801): the node served us-east-blacklisted content WHILE in
        // us-east, then flipped us-east -> eu-west. Under `block.timestamp` scope the
        // prev (us-east) leg ripens out once `block.timestamp - effective >= window`,
        // so a stalled challenge would escape. Anchoring the window to the served
        // `responseTs` (which predates the flip) keeps us-east in scope -> still
        // slashable. Reachable only when `window < maxEvidenceAge`: governance can set
        // the window to its 3d floor while evidence age is 5d, so the gap is real.
        slasher.setGate(0, 3 days); // window = 3d (< 5d evidence age); gate unused (changed region)
        uint64 nowSec = uint64(block.timestamp);
        uint64 responseTsSec = nowSec - 4 days; // served 4d ago, within the 5d evidence age
        uint64 effective = nowSec - (3 days + 1); // flipped just over `window` ago, AFTER the serve
        slasher.setRegion(node, "eu-west", "us-east", effective);
        blacklist.setEntry(bytes32("us-east"), BLOB, responseTsSec - 1000); // us-east entry predates the serve
        // block.timestamp scope would drop the prev leg (block.timestamp - effective
        // = 3d + 1 >= 3d -> escape); responseTs scope keeps it (responseTs < effective
        // -> elapsed 0 -> prev applies).
        SlashJudge.StreamMsg memory s = _stream(true, 10, uint64(uint256(responseTsSec) * 1_000_000));
        _commitAndMature(_blacklistHash(_streamStructHash(s), true));
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
        assertEq(slasher.slashCount(), 1);
    }

    function test_blacklist_honestRelocator_currentLegSlashesAccepted() public {
        // Edge 1 (#801, accepted residual): the node served compliantly WHILE in
        // us-east (no us-east entry), then GENUINELY relocated to eu-west. The
        // current-region leg (entry.region == regionHint) applies regardless of when
        // the serve happened, so the old serve becomes slashable once eu-west holds an
        // entry that predates it. Exempting the pre-relocation serve would need
        // region-at-responseTs history, which the single `regionPrev` slot cannot
        // provide; the edge is accepted and pinned here so any future change to the
        // posture is a conscious one. NOTE: this slash is via the CURRENT-region leg,
        // which `scopedRegions` never gates on the window, so this test is
        // independent of the responseTs-vs-block.timestamp anchor — it passes under
        // both and guards the posture, not this PR's change specifically.
        uint64 nowSec = uint64(block.timestamp);
        uint64 responseTsSec = nowSec - 2 hours; // served 2h ago, while in us-east
        uint64 effective = nowSec - 1 hours; // relocated to eu-west AFTER the serve
        slasher.setRegion(node, "eu-west", "us-east", effective);
        blacklist.setEntry(bytes32("eu-west"), BLOB, nowSec - 3 hours); // eu-west entry predates the serve
        // us-east (serve-time region) has NO entry -> the serve was compliant when made.
        SlashJudge.StreamMsg memory s = _stream(true, 10, uint64(uint256(responseTsSec) * 1_000_000));
        _commitAndMature(_blacklistHash(_streamStructHash(s), true));
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
        assertEq(slasher.slashCount(), 1);
    }

    function test_blacklist_multiChangeResidual_serveTimeRegionUnrecoverable() public {
        // Accepted residual (#801): `regionPrev` is a SINGLE slot. After two region
        // changes (us-east -> eu-west -> ap-south) the slot holds eu-west, so the
        // region the node was in at serve time (us-east) is unrecoverable. A us-east
        // entry the node actually served is therefore out of scope -> NOT slashable.
        // Reconstructing the serve-time region would need multi-level history
        // (rejected on EIP-170 grounds); this pins the limitation so widening
        // `regionPrev` history is a conscious change. In production the two flips are
        // window-spaced, so this is reachable only when MAX_EVIDENCE_AGE_US exceeds
        // the window; the mock isolates the scope logic from that spacing.
        uint64 nowSec = uint64(block.timestamp);
        uint64 responseTsSec = nowSec - 1 hours; // served while in us-east, before both flips
        uint64 effective = nowSec - 30 minutes; // most recent flip (eu-west -> ap-south)
        slasher.setRegion(node, "ap-south", "eu-west", effective); // current ap-south, prev eu-west; us-east lost
        blacklist.setEntry(bytes32("us-east"), BLOB, nowSec - 2 hours); // us-east entry predates the serve
        SlashJudge.StreamMsg memory s = _stream(true, 10, uint64(uint256(responseTsSec) * 1_000_000));
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.HashNotBlacklisted.selector, BLOB));
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true, SALT);
        assertEq(slasher.slashCount(), 0);
    }

    // -----------------------------------------------------------------
    // Commit–reveal front-running mitigation (#854)
    // -----------------------------------------------------------------

    function test_commit_revertsOnDuplicate() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        bytes32 commitment = keccak256(abi.encode(_pairHash(p, s, ISlashJudge.OffenseType.Phantom), SALT, challenger));

        vm.prank(challenger);
        judge.commitChallenge(commitment);
        vm.prank(challenger);
        vm.expectRevert(SlashJudge.CommitmentExists.selector);
        judge.commitChallenge(commitment);
    }

    // ADR 009 § Emergency Multisig — the protocol-wide pause sunsets hard at
    // each contract's own construction time + 365 days; afterwards `pause()` reverts for everyone.
    function test_pause_revertsAfterSunset() public {
        vm.warp(block.timestamp + 366 days);
        vm.prank(pauser);
        vm.expectRevert(SunsettingPausable.PauseExpired.selector);
        judge.pause();
    }

    function test_commit_blockedWhilePaused() public {
        vm.prank(pauser);
        judge.pause();
        vm.prank(challenger);
        vm.expectRevert();
        judge.commitChallenge(bytes32(uint256(0x1234)));
    }

    function test_reveal_revertsWithoutCommit() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(SlashJudge.NoCommitment.selector);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
    }

    function test_reveal_revertsBeforeDelay() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        bytes32 evidenceHash = _pairHash(p, s, ISlashJudge.OffenseType.Phantom);

        _commitAs(challenger, evidenceHash);
        uint256 readyAt = block.timestamp + REVEAL_DELAY;
        // Still inside the maturation delay (warp less than MIN_REVEAL_DELAY).
        vm.warp(block.timestamp + REVEAL_DELAY - 1);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.RevealTooEarly.selector, readyAt));
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
    }

    function test_reveal_revertsAfterWindow() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        bytes32 evidenceHash = _pairHash(p, s, ISlashJudge.OffenseType.Phantom);

        _commitAs(challenger, evidenceHash);
        uint256 expiresAt = block.timestamp + REVEAL_WINDOW;
        // Past the reveal window. Evidence (≈5s old) is still inside the 5-day
        // staleness ceiling after a 1-day warp, so the expiry is the live revert.
        vm.warp(block.timestamp + REVEAL_WINDOW + 1);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.CommitmentExpired.selector, expiresAt));
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
    }

    function test_reveal_revertsForWrongChallenger() public {
        // A mempool copy of the reveal: the same evidence + salt submitted by a
        // different `msg.sender` reconstructs a different commitment, which was
        // never registered → NoCommitment. This is the front-running fix.
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        _commitAndMature(_pairHash(p, s, ISlashJudge.OffenseType.Phantom));

        vm.prank(stranger);
        vm.expectRevert(SlashJudge.NoCommitment.selector);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
    }

    function test_reveal_consumesCommitment() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        _commitAndMature(_pairHash(p, s, ISlashJudge.OffenseType.Phantom));

        vm.prank(challenger);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);

        // The commitment was deleted on the successful reveal, so a naive resubmit
        // (no fresh commit) fails the commitment check before the replay guard.
        vm.prank(challenger);
        vm.expectRevert(SlashJudge.NoCommitment.selector);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
    }

    function test_commit_emitsEvent() public {
        bytes32 commitment = keccak256(abi.encode(bytes32(uint256(0xABCD)), SALT, challenger));
        vm.expectEmit(true, false, false, false, address(judge));
        emit SlashJudge.ChallengeCommitted(commitment);
        vm.prank(challenger);
        judge.commitChallenge(commitment);
    }

    function test_reveal_succeedsAtExactReadyBoundary() public {
        // block.timestamp == committedAt + MIN_REVEAL_DELAY is valid (`<` not `<=`).
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        _commitAs(challenger, _pairHash(p, s, ISlashJudge.OffenseType.Phantom));
        vm.warp(block.timestamp + REVEAL_DELAY); // exactly at readyAt
        vm.prank(challenger);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
        assertEq(slasher.slashCount(), 1);
    }

    function test_reveal_succeedsAtExactExpiryBoundary() public {
        // block.timestamp == committedAt + REVEAL_WINDOW is still valid (`>` not `>=`).
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        _commitAs(challenger, _pairHash(p, s, ISlashJudge.OffenseType.Phantom));
        vm.warp(block.timestamp + REVEAL_WINDOW); // exactly at expiry
        vm.prank(challenger);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
        assertEq(slasher.slashCount(), 1);
    }

    function test_reveal_revertsForDifferentSalt() public {
        // The commitment binds the salt: revealing the same evidence under a
        // different salt reconstructs a commitment that was never stored.
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        _commitAndMature(_pairHash(p, s, ISlashJudge.OffenseType.Phantom)); // commits with SALT
        bytes32 otherSalt = bytes32(uint256(0xBEEF));
        vm.prank(challenger);
        vm.expectRevert(SlashJudge.NoCommitment.selector);
        judge.submitPhantomChallenge(
            node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), otherSalt
        );
    }

    function test_commit_overwritesExpiredCommitment() public {
        // A challenger who let a commitment expire (or lost a blind race) can
        // re-commit the SAME (evidence, salt) once it lapses past REVEAL_WINDOW,
        // then reveal — proving they are not permanently wedged on that salt.
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        bytes32 evidenceHash = _pairHash(p, s, ISlashJudge.OffenseType.Phantom);

        _commitAs(challenger, evidenceHash);
        vm.warp(block.timestamp + REVEAL_WINDOW + 1); // first commitment expires

        // Re-commit the identical triple: allowed because the prior one expired.
        _commitAs(challenger, evidenceHash);
        vm.warp(block.timestamp + REVEAL_DELAY + 1); // mature the fresh commitment
        vm.prank(challenger);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
        assertEq(slasher.slashCount(), 1);
    }

    function test_commit_revertsOnDuplicateWhileLive() public {
        // While a commitment is still inside REVEAL_WINDOW, a re-commit reverts
        // (complements test_commit_overwritesExpiredCommitment for the live case).
        bytes32 commitment = keccak256(abi.encode(bytes32(uint256(0xDEAD)), SALT, challenger));
        vm.prank(challenger);
        judge.commitChallenge(commitment);
        vm.warp(block.timestamp + REVEAL_WINDOW); // still live (boundary is inclusive)
        vm.prank(challenger);
        vm.expectRevert(SlashJudge.CommitmentExists.selector);
        judge.commitChallenge(commitment);
    }

    function test_reveal_blindRaceFirstRevealWins() public {
        // Two honest witnesses independently commit the same evidence (blind, so
        // neither can be front-run). The first to reveal is recorded as the
        // challenger and wins the reward; the loser's reveal — despite a valid
        // commitment — hits the evidence replay guard. The reward is decided by
        // reveal order, never by copying calldata.
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        bytes32 evidenceHash = _pairHash(p, s, ISlashJudge.OffenseType.Phantom);

        _commitAs(challenger, evidenceHash);
        _commitAs(challenger2, evidenceHash);
        vm.warp(block.timestamp + REVEAL_DELAY + 1);

        vm.prank(challenger);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
        assertEq(slasher.lastChallenger(), challenger);

        vm.prank(challenger2);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.EvidenceAlreadyUsed.selector, evidenceHash));
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);

        assertEq(slasher.slashCount(), 1);
    }

    // -----------------------------------------------------------------
    // Governance
    // -----------------------------------------------------------------

    function test_setMaxEvidenceAge_enforcesBoundsAndInvariant() public {
        // Above the 30-day ceiling.
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                SlashJudge.ParamOutOfBounds.selector,
                uint256(31 days * 1_000_000),
                uint256(1 days * 1_000_000),
                uint256(30 days * 1_000_000)
            )
        );
        judge.setMaxEvidenceAge(31 days * 1_000_000);

        // Within the bound but >= unbondingPeriod (14d) → invariant revert.
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(
                SlashJudge.EvidenceAgeExceedsUnbonding.selector,
                uint256(20 days * 1_000_000),
                uint256(UNBONDING * 1_000_000)
            )
        );
        judge.setMaxEvidenceAge(20 days * 1_000_000);

        vm.prank(admin);
        judge.setMaxEvidenceAge(7 days * 1_000_000);
        assertEq(judge.maxEvidenceAgeUs(), 7 days * 1_000_000);
    }

    function test_setChallengeBond_enforcesBounds() public {
        vm.prank(admin);
        vm.expectRevert(
            abi.encodeWithSelector(SlashJudge.ParamOutOfBounds.selector, uint256(0), uint256(1e18), uint256(1000e18))
        );
        judge.setChallengeBond(0);
        vm.prank(admin);
        judge.setChallengeBond(500e18);
        assertEq(judge.challengeBond(), 500e18);
    }

    function test_setters_onlyGovernance() public {
        vm.prank(stranger);
        vm.expectRevert(
            abi.encodeWithSelector(IAccessControl.AccessControlUnauthorizedAccount.selector, stranger, GOVERNANCE_ROLE)
        );
        judge.setChallengeBond(5e18);
    }

    function test_pause_blocksChallenges() public {
        vm.prank(pauser);
        judge.pause();
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert();
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s), SALT);
    }

    function test_constructor_revertsWhenEvidenceAgeExceedsUnbonding() public {
        MockSlasher shortUnbond = new MockSlasher(3 days);
        vm.expectRevert(
            abi.encodeWithSelector(
                SlashJudge.EvidenceAgeExceedsUnbonding.selector,
                uint256(5 days * 1_000_000),
                uint256(3 days * 1_000_000)
            )
        );
        new SlashJudge(shortUnbond, IERC20(address(token)), blacklist, CHALLENGE_BOND, MAX_EVIDENCE_AGE_US, admin);
    }
}
