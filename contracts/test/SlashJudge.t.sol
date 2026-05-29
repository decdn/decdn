// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { ERC20 } from "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { IAccessControl } from "@openzeppelin/contracts/access/IAccessControl.sol";

import { SlashJudge } from "../src/SlashJudge.sol";
import { ISlashJudge } from "../src/interfaces/ISlashJudge.sol";
import { ICapacityBondSlasher } from "../src/interfaces/ICapacityBondSlasher.sol";
import { IContentBlacklistHashView } from "../src/interfaces/IContentBlacklistHashView.sol";

contract MockToken is ERC20 {
    constructor() ERC20("TOKEN", "TKN") {
        _mint(msg.sender, 1_000_000_000e18);
    }
}

contract MockSlasher is ICapacityBondSlasher {
    uint256 public unbondingPeriodValue;
    uint256 public nextId = 1;
    uint256 public returnAmount;
    mapping(address => bytes32) public boundNodeId;

    address public lastOperator;
    address public lastChallenger;
    uint8 public lastOffense;
    uint256 public slashCount;

    constructor(uint256 unbonding_) {
        unbondingPeriodValue = unbonding_;
    }

    function setBound(address operator, bytes32 nodeId) external {
        boundNodeId[operator] = nodeId;
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
    mapping(bytes32 => mapping(bytes32 => uint64)) internal _addedAt;

    function setEntry(bytes32 region, bytes32 hash, uint64 addedAt) external {
        _addedAt[region][hash] = addedAt;
    }

    function getHashEntry(bytes32 region, bytes32 hash) external view override returns (uint64, bool) {
        return (_addedAt[region][hash], false);
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
    address internal admin = address(0xA11CE);
    address internal pauser = address(0xDEAD);
    address internal stranger = address(0x5747A);

    bytes32 internal constant NODE_ID = bytes32(uint256(0xBEEF));
    bytes32 internal constant BLOB = bytes32(uint256(0xB10B));

    uint256 internal constant CHALLENGE_BOND = 100e18;
    uint256 internal constant MAX_EVIDENCE_AGE_US = 5 days * 1_000_000;
    uint256 internal constant UNBONDING = 14 days;

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
        bytes32 sh = keccak256(abi.encode(PROBE_TYPEHASH, p.hash, p.hasBlob, p.ratePerMb, p.timestampUs));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(NODE_PK, _digest(sh));
        return abi.encodePacked(r, s, v);
    }

    function _signStream(SlashJudge.StreamMsg memory st) internal view returns (bytes memory) {
        bytes32 sh = keccak256(
            abi.encode(
                STREAM_TYPEHASH, st.hash, st.ok, st.ratePerMb, st.totalBytes, st.channelId, st.timestampUs, st.redirect
            )
        );
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(NODE_PK, _digest(sh));
        return abi.encodePacked(r, s, v);
    }

    // -----------------------------------------------------------------
    // Phantom
    // -----------------------------------------------------------------

    function test_phantom_slashesAndRoundTripsBond() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);

        uint256 balBefore = token.balanceOf(challenger);
        vm.prank(challenger);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s));

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
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s));
    }

    function test_phantom_revertsOnUnregisteredNode() public {
        slasher.setBound(node, bytes32(0));
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.NodeNotRegistered.selector, node));
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s));
    }

    function test_phantom_revertsOnNodeIdMismatch() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        bytes32 wrongId = bytes32(uint256(0xBAD));
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.NodeIdMismatch.selector, wrongId, NODE_ID));
        judge.submitPhantomChallenge(node, wrongId, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s));
    }

    function test_phantom_revertsOnBadProbeSignature() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, streamTs);
        bytes memory probeSig = _signProbe(_probe(true, 99, probeTs)); // signed different rate
        vm.prank(challenger);
        vm.expectRevert(SlashJudge.InvalidProbeSignature.selector);
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), probeSig, abi.encode(s), _signStream(s));
    }

    function test_phantom_revertsOutsideTimestampWindow() public {
        uint64 farStream = probeTs + 30_000_000; // exactly 30s → not < 30s
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(false, 10, farStream);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.TimestampWindowViolated.selector, probeTs, farStream));
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s));
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
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s));
    }

    function test_phantom_revertsOnFutureEvidence() public {
        uint256 nowUs = block.timestamp * 1_000_000;
        uint64 futureProbe = uint64(nowUs + 120_000_000); // 120s > 60s skew
        uint64 futureStream = futureProbe + 5_000_000;
        SlashJudge.ProbeMsg memory p = _probe(true, 10, futureProbe);
        SlashJudge.StreamMsg memory s = _stream(false, 10, futureStream);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.EvidenceInFuture.selector, futureProbe));
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s));
    }

    // -----------------------------------------------------------------
    // Rate manipulation
    // -----------------------------------------------------------------

    function test_rate_slashesWhenStreamRateHigher() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(true, 25, streamTs); // 25 > 10
        vm.prank(challenger);
        judge.submitRateChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s));
        assertEq(slasher.lastOffense(), uint8(ISlashJudge.OffenseType.RateManipulation));
    }

    function test_rate_revertsWhenNotHigher() public {
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs); // equal → not manipulation
        vm.prank(challenger);
        vm.expectRevert(SlashJudge.NotRateManipulation.selector);
        judge.submitRateChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s));
    }

    // -----------------------------------------------------------------
    // Blacklist
    // -----------------------------------------------------------------

    function test_blacklist_slashesOnStreamResponse() public {
        blacklist.setEntry(GLOBAL_REGION, BLOB, uint64(block.timestamp - 1000));
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true);
        assertEq(slasher.lastOffense(), uint8(ISlashJudge.OffenseType.Blacklist));
    }

    function test_blacklist_slashesOnProbeResponse() public {
        blacklist.setEntry(GLOBAL_REGION, BLOB, uint64(block.timestamp - 1000));
        SlashJudge.ProbeMsg memory p = _probe(true, 10, probeTs);
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(p), _signProbe(p), false);
        assertEq(slasher.slashCount(), 1);
    }

    function test_blacklist_revertsWhenNotBlacklisted() public {
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.HashNotBlacklisted.selector, BLOB));
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true);
    }

    function test_blacklist_revertsWhenBlacklistedAfterResponse() public {
        blacklist.setEntry(GLOBAL_REGION, BLOB, uint64(block.timestamp + 1000)); // effective after response
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs);
        vm.prank(challenger);
        vm.expectRevert(
            abi.encodeWithSelector(SlashJudge.BlacklistAfterResponse.selector, uint64(block.timestamp + 1000), streamTs)
        );
        judge.submitBlacklistChallenge(node, NODE_ID, BLOB, abi.encode(s), _signStream(s), true);
    }

    function test_blacklist_revertsOnHashMismatch() public {
        blacklist.setEntry(GLOBAL_REGION, BLOB, uint64(block.timestamp - 1000));
        SlashJudge.StreamMsg memory s = _stream(true, 10, streamTs); // s.hash == BLOB
        bytes32 otherHash = bytes32(uint256(0xC0FFEE));
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.HashMismatch.selector, otherHash, BLOB));
        judge.submitBlacklistChallenge(node, NODE_ID, otherHash, abi.encode(s), _signStream(s), true);
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
        judge.submitPhantomChallenge(node, NODE_ID, abi.encode(p), _signProbe(p), abi.encode(s), _signStream(s));
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
