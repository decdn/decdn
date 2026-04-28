// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

import { TOKEN } from "../src/TOKEN.sol";
import { StakingRegistry } from "../src/StakingRegistry.sol";
import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { SlashJudge } from "../src/SlashJudge.sol";
import { IStakingRegistry } from "../src/interfaces/IStakingRegistry.sol";
import { IStablePaymentChannel } from "../src/interfaces/IStablePaymentChannel.sol";
import { IContentBlacklist } from "../src/interfaces/IContentBlacklist.sol";
import { Errors } from "../src/libraries/Errors.sol";
import { Roles } from "../src/libraries/Roles.sol";

import { MockPaymentChannel } from "./mocks/MockPaymentChannel.sol";

contract SlashJudgeTest is Test {
    TOKEN internal token;
    StakingRegistry internal reg;
    ContentBlacklist internal bl;
    MockPaymentChannel internal channelMock;
    SlashJudge internal judge;

    address internal admin = makeAddr("admin");
    address internal governor = makeAddr("governor");

    uint256 internal challengerPk = 0xCAFE;
    address internal challenger;

    uint256 internal nodePk = 0xB0B;
    address internal node;

    uint256 internal constant MIN_STAKE = 1000e18;
    uint64 internal constant COUNTER = 24 hours;
    uint256 internal constant BOND = 100e18;
    bytes32 internal constant NODE_ID = keccak256("slashjudge-test-node");

    // Cached to avoid consuming vm.prank / vm.expectRevert via staticcalls.
    bytes32 internal PROBE_TH;
    bytes32 internal STREAM_TH;
    bytes32 internal RATE_TH;
    bytes32 internal RECEIPT_TH;
    bytes32 internal DOMAIN;

    function setUp() public {
        challenger = vm.addr(challengerPk);
        node = vm.addr(nodePk);

        token = new TOKEN(address(this), 100_000_000e18, address(this));
        reg = new StakingRegistry(token, MIN_STAKE, 7 days, 90 days, admin);
        bl = new ContentBlacklist(reg, admin);
        channelMock = new MockPaymentChannel();
        judge = new SlashJudge(reg, bl, channelMock, IERC20(address(token)), admin, BOND, COUNTER);

        vm.startPrank(admin);
        reg.grantRole(Roles.SLASH_ROLE, address(judge));
        reg.grantRole(Roles.BLACKLIST_ROLE, address(bl));
        bl.grantRole(Roles.GOVERNANCE_ROLE, governor);
        reg.unpause();
        vm.stopPrank();

        // Fund node and stake it so slash has something to pull from.
        token.transfer(node, 100_000e18);
        vm.prank(node);
        token.approve(address(reg), type(uint256).max);
        vm.prank(node);
        reg.stake(10_000e18);

        PROBE_TH = judge.PROBE_RESPONSE_TYPEHASH();
        STREAM_TH = judge.STREAM_RESPONSE_TYPEHASH();
        RATE_TH = judge.RATE_CHANGE_TYPEHASH();
        RECEIPT_TH = judge.DELIVERY_RECEIPT_TYPEHASH();
        DOMAIN = judge.domainSeparator();

        // Register the node with a stable nodeId — SlashJudge now
        // requires a registered operator and uses the bound nodeId for
        // all counter-sig verification.
        bytes memory bindSig = _signBind(nodePk, NODE_ID, 0);
        vm.prank(node);
        reg.registerNode(NODE_ID, bindSig);

        // Fund challenger with bond.
        token.transfer(challenger, 10_000e18);
        vm.prank(challenger);
        token.approve(address(judge), type(uint256).max);
    }

    function _signBind(
        uint256 pk,
        bytes32 nodeId,
        uint64 nonce
    ) internal view returns (bytes memory) {
        bytes32 structHash = keccak256(abi.encode(reg.BIND_NODE_TYPEHASH(), nodeId, nonce));
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", reg.domainSeparator(), structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(pk, digest);
        return abi.encodePacked(r, s, v);
    }

    // ---------------- phantom ----------------

    function test_Phantom_HappyPath_ResolveAfterWindow() public {
        SlashJudge.ProbeResponse memory probe = SlashJudge.ProbeResponse({
            hash: bytes32(uint256(0xDEAD)),
            hasBlob: true,
            ratePerMb: 10,
            timestamp: uint64(block.timestamp)
        });
        SlashJudge.StreamResponse memory sr = SlashJudge.StreamResponse({
            hash: probe.hash,
            ok: false,
            ratePerMb: 10,
            totalBytes: 0,
            channelId: bytes32(uint256(1)),
            timestamp: uint64(block.timestamp)
        });
        bytes memory pSig = _signProbe(nodePk, probe);
        bytes memory sSig = _signStream(nodePk, sr);

        vm.prank(challenger);
        uint256 id = judge.submitPhantomChallenge(node, probe, pSig, sr, sSig);
        assertEq(judge.getChallenge(id).bond, BOND);
        assertEq(judge.activeChallengeCount(node), 1);

        vm.warp(block.timestamp + COUNTER + 1);
        uint256 before = token.balanceOf(challenger);
        judge.resolveChallenge(id);

        // First offense → tier 0 = 5%. slashReward = 50% of (10,000e18 * 5%) = 250e18 ; bond
        // returned.
        uint256 expected = (10_000e18 * 500 / 10_000) / 2 + BOND;
        assertEq(token.balanceOf(challenger) - before, expected);
        assertEq(judge.activeChallengeCount(node), 0);
    }

    function test_Phantom_RejectsMismatchedHash() public {
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(1)), true, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(2)), false, 10);
        vm.expectRevert(SlashJudge.InvalidChallengePair.selector);
        vm.prank(challenger);
        judge.submitPhantomChallenge(
            node, probe, _signProbe(nodePk, probe), sr, _signStream(nodePk, sr)
        );
    }

    function test_Phantom_RejectsWhenHashMatchesButNoMismatch() public {
        // probe: hasBlob = false  → not phantom
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(1)), false, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(1)), false, 10);
        vm.expectRevert(SlashJudge.InvalidChallengePair.selector);
        vm.prank(challenger);
        judge.submitPhantomChallenge(
            node, probe, _signProbe(nodePk, probe), sr, _signStream(nodePk, sr)
        );
    }

    function test_Phantom_RejectsStaleEvidence() public {
        // rewind is not possible. Instead sign with old timestamp and warp forward.
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(1)), true, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(1)), false, 10);
        bytes memory pSig = _signProbe(nodePk, probe);
        bytes memory sSig = _signStream(nodePk, sr);
        vm.warp(block.timestamp + 6 days);

        vm.expectRevert(SlashJudge.EvidenceTooOld.selector);
        vm.prank(challenger);
        judge.submitPhantomChallenge(node, probe, pSig, sr, sSig);
    }

    function test_Phantom_RejectsFutureEvidence() public {
        SlashJudge.ProbeResponse memory probe = SlashJudge.ProbeResponse(
            bytes32(uint256(1)), true, 10, uint64(block.timestamp + 1 hours)
        );
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(1)), false, 10);
        bytes memory pSig = _signProbe(nodePk, probe);
        bytes memory sSig = _signStream(nodePk, sr);

        vm.expectRevert(SlashJudge.EvidenceInTheFuture.selector);
        vm.prank(challenger);
        judge.submitPhantomChallenge(node, probe, pSig, sr, sSig);
    }

    function test_Phantom_RejectsBadSignature() public {
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(1)), true, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(1)), false, 10);
        bytes memory pSig = _signProbe(0xDEADBEEF, probe);
        bytes memory sSig = _signStream(nodePk, sr);

        vm.expectRevert(Errors.InvalidSignature.selector);
        vm.prank(challenger);
        judge.submitPhantomChallenge(node, probe, pSig, sr, sSig);
    }

    // ---------------- rate ----------------

    function test_Rate_HappyPath_NotCountered() public {
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(7)), false, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(7)), true, 50);
        bytes memory pSig = _signProbe(nodePk, probe);
        bytes memory sSig = _signStream(nodePk, sr);

        vm.prank(challenger);
        uint256 id = judge.submitRateChallenge(node, probe, pSig, sr, sSig);

        vm.warp(block.timestamp + COUNTER + 1);
        uint256 before = token.balanceOf(challenger);
        judge.resolveChallenge(id);
        assertGt(token.balanceOf(challenger), before);
    }

    function test_Rate_CounterForfeitsBond() public {
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(7)), false, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(7)), true, 50);
        uint64 streamTs = sr.timestamp;
        vm.prank(challenger);
        uint256 id = judge.submitRateChallenge(
            node, probe, _signProbe(nodePk, probe), sr, _signStream(nodePk, sr)
        );

        // Counter with a RateChange signed by the node under the REGISTERED
        // nodeId. The (oldRate, newRate) pair must match what the probe/
        // stream captured, and `effectiveAt` must be <= streamTimestamp.
        (uint64 oldR, uint64 newR, uint64 effAt) = (10, 50, streamTs - 1);
        bytes memory sig = _signRateChange(nodePk, NODE_ID, oldR, newR, effAt);

        uint256 nodeBefore = token.balanceOf(node);
        uint256 supplyBefore = token.totalSupply();
        vm.prank(node);
        judge.counterRateChallenge(id, oldR, newR, streamTs, effAt, sig);

        assertEq(token.balanceOf(node) - nodeBefore, BOND / 2);
        // Bond-burn is a real ERC20Burnable.burn() on the judge's balance.
        assertEq(supplyBefore - token.totalSupply(), BOND - BOND / 2);

        // Resolve now must fail — state is Countered.
        vm.expectRevert(SlashJudge.NotActive.selector);
        judge.resolveChallenge(id);
    }

    function test_Rate_CounterRejectsMismatchedRates() public {
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(7)), false, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(7)), true, 50);
        uint64 streamTs = sr.timestamp;
        vm.prank(challenger);
        uint256 id = judge.submitRateChallenge(
            node, probe, _signProbe(nodePk, probe), sr, _signStream(nodePk, sr)
        );

        // Swap rates — counter should reject EvidenceMismatch.
        (uint64 oldR, uint64 newR, uint64 effAt) = (5, 99, streamTs - 1);
        bytes memory sig = _signRateChange(nodePk, NODE_ID, oldR, newR, effAt);
        vm.expectRevert(SlashJudge.EvidenceMismatch.selector);
        vm.prank(node);
        judge.counterRateChallenge(id, oldR, newR, streamTs, effAt, sig);
    }

    function test_Rate_CounterRejectsEffectiveAfterStream() public {
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(7)), false, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(7)), true, 50);
        uint64 streamTs = sr.timestamp;
        vm.prank(challenger);
        uint256 id = judge.submitRateChallenge(
            node, probe, _signProbe(nodePk, probe), sr, _signStream(nodePk, sr)
        );

        // effectiveAt > streamTimestamp: the rate change happened AFTER the
        // stream was billed, so it doesn't justify the higher charge.
        (uint64 oldR, uint64 newR, uint64 effAt) = (10, 50, streamTs + 1);
        bytes memory sig = _signRateChange(nodePk, NODE_ID, oldR, newR, effAt);
        vm.expectRevert(SlashJudge.EvidenceMismatch.selector);
        vm.prank(node);
        judge.counterRateChallenge(id, oldR, newR, streamTs, effAt, sig);
    }

    function test_Rate_CounterRejectsForeignNodeIdSig() public {
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(7)), false, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(7)), true, 50);
        uint64 streamTs = sr.timestamp;
        vm.prank(challenger);
        uint256 id = judge.submitRateChallenge(
            node, probe, _signProbe(nodePk, probe), sr, _signStream(nodePk, sr)
        );

        // Signature is over a different nodeId than the one bound at
        // registration — contract rebuilds the digest with the BOUND
        // nodeId, so recovery yields a different signer.
        bytes32 foreignNodeId = keccak256("other-node");
        (uint64 oldR, uint64 newR, uint64 effAt) = (10, 50, streamTs - 1);
        bytes memory sig = _signRateChange(nodePk, foreignNodeId, oldR, newR, effAt);
        vm.expectRevert(Errors.InvalidSignature.selector);
        vm.prank(node);
        judge.counterRateChallenge(id, oldR, newR, streamTs, effAt, sig);
    }

    function test_Rate_CounterByNonAccusedReverts() public {
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(7)), false, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(7)), true, 50);
        uint64 streamTs = sr.timestamp;
        vm.prank(challenger);
        uint256 id = judge.submitRateChallenge(
            node, probe, _signProbe(nodePk, probe), sr, _signStream(nodePk, sr)
        );

        bytes memory sig = _signRateChange(nodePk, NODE_ID, 10, 50, streamTs - 1);
        vm.expectRevert(SlashJudge.NotAccused.selector);
        vm.prank(challenger);
        judge.counterRateChallenge(id, 10, 50, streamTs, streamTs - 1, sig);
    }

    function test_Rate_CounterAfterWindowReverts() public {
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(7)), false, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(7)), true, 50);
        uint64 streamTs = sr.timestamp;
        vm.prank(challenger);
        uint256 id = judge.submitRateChallenge(
            node, probe, _signProbe(nodePk, probe), sr, _signStream(nodePk, sr)
        );
        vm.warp(block.timestamp + COUNTER + 1);

        bytes memory sig = _signRateChange(nodePk, NODE_ID, 10, 50, streamTs);
        vm.expectRevert(SlashJudge.CounterWindowClosed.selector);
        vm.prank(node);
        judge.counterRateChallenge(id, 10, 50, streamTs, streamTs, sig);
    }

    // ---------------- blacklist ----------------

    function test_Blacklist_RequiresListedBeforeEvidence() public {
        bytes32 h = bytes32(uint256(0x42));
        vm.prank(governor);
        bl.addHash(h);
        vm.warp(block.timestamp + 10);

        SlashJudge.ProbeResponse memory probe = _probe(h, true, 10);
        bytes memory sig = _signProbe(nodePk, probe);
        vm.prank(challenger);
        uint256 id = judge.submitBlacklistChallenge(node, probe, sig);
        assertGt(id, 0);
    }

    function test_Blacklist_RejectsUnlisted() public {
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(1)), true, 10);
        bytes memory sig = _signProbe(nodePk, probe);
        vm.expectRevert(SlashJudge.HashNotBlacklisted.selector);
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, probe, sig);
    }

    function test_Blacklist_RejectsWhenRemovedBeforeEvidence() public {
        bytes32 h = bytes32(uint256(0x77));
        vm.prank(governor);
        bl.addHash(h);
        vm.warp(block.timestamp + 1 hours);
        vm.prank(governor);
        bl.removeHash(h);
        vm.warp(block.timestamp + 1 hours);

        SlashJudge.ProbeResponse memory probe = _probe(h, true, 10);
        bytes memory sig = _signProbe(nodePk, probe);
        vm.expectRevert(SlashJudge.HashNotBlacklisted.selector);
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, probe, sig);
    }

    function test_Blacklist_RejectsWhenListedAfterEvidence() public {
        // Evidence timestamped NOW; blacklist happens later. `wasBlacklistedAt`
        // sees no interval covering the probe timestamp → `HashNotBlacklisted`.
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(1)), true, 10);
        bytes memory sig = _signProbe(nodePk, probe);
        vm.warp(block.timestamp + 1 hours);
        vm.prank(governor);
        bl.addHash(probe.hash);

        vm.expectRevert(SlashJudge.HashNotBlacklisted.selector);
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, probe, sig);
    }

    function test_Blacklist_AcceptsEvidenceFromPastInterval() public {
        // Hash added at T0, removed at T1, re-added at T2. Probe evidence
        // from time in [T0, T1) must still slash — historical record is
        // preserved via the interval list.
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(0x77)), true, 10);
        uint64 probeTs = uint64(block.timestamp);
        bytes memory sig = _signProbe(nodePk, probe);

        // T0: add
        vm.prank(governor);
        bl.addHash(probe.hash);
        // T1: remove (probe evidence is earlier than this removal)
        vm.warp(block.timestamp + 1 days);
        vm.prank(governor);
        bl.removeHash(probe.hash);
        // T2: re-add — historical interval [T0, T1] is preserved.
        vm.warp(block.timestamp + 1 days);
        vm.prank(governor);
        bl.addHash(probe.hash);

        // Need MAX_EVIDENCE_AGE freshness — warp forward less than that.
        assertLe(block.timestamp - probeTs, judge.MAX_EVIDENCE_AGE());
        vm.prank(challenger);
        uint256 id = judge.submitBlacklistChallenge(node, probe, sig);
        assertGt(id, 0);
    }

    // ---------------- corruption ----------------

    function test_Corruption_RejectsNonChannelClient() public {
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(42)), true, 10);
        bytes memory sSig = _signStream(nodePk, sr);
        // channel has no recorded client; any submitter is rejected.
        vm.expectRevert(SlashJudge.NotChannelClient.selector);
        vm.prank(challenger);
        judge.submitCorruptionChallenge(node, sr, sSig);

        // Record a DIFFERENT address as client; challenger still rejected.
        address otherClient = makeAddr("otherClient");
        channelMock.setChannelClient(sr.channelId, otherClient);
        vm.expectRevert(SlashJudge.NotChannelClient.selector);
        vm.prank(challenger);
        judge.submitCorruptionChallenge(node, sr, sSig);
    }

    function test_Corruption_CounterWithReceipt() public {
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(42)), true, 10);
        bytes memory sSig = _signStream(nodePk, sr);
        channelMock.setChannelClient(sr.channelId, challenger);
        vm.prank(challenger);
        uint256 id = judge.submitCorruptionChallenge(node, sr, sSig);

        // Receipt is bound to the registered nodeId and the (channelId,
        // blobHash) snapshot stored at submit. A receipt for a different
        // channel or blob would fail signature verification.
        uint64 deliveredAt = uint64(block.timestamp);
        bytes memory rsig =
            _signReceipt(challengerPk, challenger, NODE_ID, sr.channelId, sr.hash, deliveredAt);

        vm.prank(node);
        judge.counterCorruptionChallenge(id, deliveredAt, rsig);
        assertEq(uint256(judge.getChallenge(id).state), uint256(SlashJudge.State.Countered));
    }

    function test_Corruption_CounterMustBeSignedByChallenger() public {
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(42)), true, 10);
        channelMock.setChannelClient(sr.channelId, challenger);
        vm.prank(challenger);
        uint256 id = judge.submitCorruptionChallenge(node, sr, _signStream(nodePk, sr));

        // Signed by the node (wrong signer) must fail.
        uint64 deliveredAt = uint64(block.timestamp);
        bytes memory rsig =
            _signReceipt(nodePk, challenger, NODE_ID, sr.channelId, sr.hash, deliveredAt);
        vm.expectRevert(Errors.InvalidSignature.selector);
        vm.prank(node);
        judge.counterCorruptionChallenge(id, deliveredAt, rsig);
    }

    function test_Corruption_CounterRejectsReceiptForDifferentChannel() public {
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(42)), true, 10);
        channelMock.setChannelClient(sr.channelId, challenger);
        vm.prank(challenger);
        uint256 id = judge.submitCorruptionChallenge(node, sr, _signStream(nodePk, sr));

        // Receipt signed for a DIFFERENT (channelId, blobHash) pair — must
        // fail because the contract rebuilds the digest using the stored
        // evidence snapshot.
        bytes32 otherChannel = keccak256("other-channel");
        uint64 deliveredAt = uint64(block.timestamp);
        bytes memory rsig =
            _signReceipt(challengerPk, challenger, NODE_ID, otherChannel, sr.hash, deliveredAt);
        vm.expectRevert(Errors.InvalidSignature.selector);
        vm.prank(node);
        judge.counterCorruptionChallenge(id, deliveredAt, rsig);
    }

    // ---------------- resolve gating ----------------

    function test_Resolve_BeforeWindowReverts() public {
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(42)), true, 10);
        channelMock.setChannelClient(sr.channelId, challenger);
        vm.prank(challenger);
        uint256 id = judge.submitCorruptionChallenge(node, sr, _signStream(nodePk, sr));
        vm.expectRevert(SlashJudge.CounterWindowOpen.selector);
        judge.resolveChallenge(id);
    }

    function test_Resolve_Unknown() public {
        vm.expectRevert(SlashJudge.UnknownChallenge.selector);
        judge.resolveChallenge(9999);
    }

    // ---------------- per-challenger uniqueness (replaces global cap) ----------------

    function test_PerChallenger_DuplicateRejected() public {
        // One challenger can only hold ONE active challenge against the
        // same node at a time.
        bytes32 h1 = keccak256("h1");
        bytes32 h2 = keccak256("h2");
        vm.startPrank(governor);
        bl.addHash(h1);
        bl.addHash(h2);
        vm.stopPrank();

        SlashJudge.ProbeResponse memory p1 = _probe(h1, true, 10);
        SlashJudge.ProbeResponse memory p2 = _probe(h2, true, 10);

        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, p1, _signProbe(nodePk, p1));

        assertTrue(judge.hasActiveChallenge(node, challenger));
        assertEq(judge.activeChallengeCount(node), 1);

        vm.expectRevert(SlashJudge.DuplicateActiveChallenge.selector);
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, p2, _signProbe(nodePk, p2));
    }

    function test_PerChallenger_DifferentChallengersUnblocked() public {
        bytes32 h1 = keccak256("h1");
        bytes32 h2 = keccak256("h2");
        vm.startPrank(governor);
        bl.addHash(h1);
        bl.addHash(h2);
        vm.stopPrank();

        SlashJudge.ProbeResponse memory p1 = _probe(h1, true, 10);
        SlashJudge.ProbeResponse memory p2 = _probe(h2, true, 10);

        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, p1, _signProbe(nodePk, p1));

        // A second, independent challenger can also submit.
        uint256 otherPk = 0xBEE;
        address other = vm.addr(otherPk);
        token.transfer(other, 10_000e18);
        vm.prank(other);
        token.approve(address(judge), type(uint256).max);

        vm.prank(other);
        judge.submitBlacklistChallenge(node, p2, _signProbe(nodePk, p2));
        assertEq(judge.activeChallengeCount(node), 2);
    }

    function test_SelfChallenge_Rejected() public {
        // Node can't submit a challenge against itself.
        bytes32 h = keccak256("self");
        vm.prank(governor);
        bl.addHash(h);
        SlashJudge.ProbeResponse memory probe = _probe(h, true, 10);
        bytes memory sig = _signProbe(nodePk, probe);

        token.transfer(node, BOND);
        vm.prank(node);
        token.approve(address(judge), type(uint256).max);
        vm.expectRevert(SlashJudge.SelfChallenge.selector);
        vm.prank(node);
        judge.submitBlacklistChallenge(node, probe, sig);
    }

    // ---------------- inclusive boundaries ----------------

    function test_Constructor_InclusiveBoundsSucceed() public {
        // Floor values at the exact min/max must be accepted.
        new SlashJudge(
            reg,
            bl,
            channelMock,
            IERC20(address(token)),
            admin,
            judge.CHALLENGE_BOND_FLOOR(),
            judge.COUNTER_WINDOW_FLOOR()
        );
        new SlashJudge(
            reg,
            bl,
            channelMock,
            IERC20(address(token)),
            admin,
            judge.CHALLENGE_BOND_CEILING(),
            judge.COUNTER_WINDOW_CEILING()
        );
    }

    // ---------------- pausable sweep ----------------

    function test_Pause_BlocksAllMutators() public {
        vm.prank(admin);
        judge.pause();

        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(1)), true, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(1)), false, 10);
        bytes memory ps = _signProbe(nodePk, probe);
        bytes memory ss = _signStream(nodePk, sr);

        bytes4 paused = bytes4(keccak256("EnforcedPause()"));

        vm.expectRevert(paused);
        vm.prank(challenger);
        judge.submitPhantomChallenge(node, probe, ps, sr, ss);

        SlashJudge.ProbeResponse memory pr2 = _probe(bytes32(uint256(7)), false, 10);
        SlashJudge.StreamResponse memory sr2 = _stream(bytes32(uint256(7)), true, 50);
        vm.expectRevert(paused);
        vm.prank(challenger);
        judge.submitRateChallenge(node, pr2, _signProbe(nodePk, pr2), sr2, _signStream(nodePk, sr2));

        vm.expectRevert(paused);
        vm.prank(challenger);
        judge.submitBlacklistChallenge(node, probe, ps);

        SlashJudge.StreamResponse memory sr3 = _stream(bytes32(uint256(42)), true, 10);
        channelMock.setChannelClient(sr3.channelId, challenger);
        vm.expectRevert(paused);
        vm.prank(challenger);
        judge.submitCorruptionChallenge(node, sr3, _signStream(nodePk, sr3));

        vm.expectRevert(paused);
        judge.resolveChallenge(1);

        vm.expectRevert(paused);
        vm.prank(node);
        judge.counterRateChallenge(1, 0, 0, 0, 0, "");

        vm.expectRevert(paused);
        vm.prank(node);
        judge.counterCorruptionChallenge(1, 0, "");
    }

    // ---------------- governance ----------------

    function test_SetChallengeBond_Bounds() public {
        vm.prank(admin);
        judge.setChallengeBond(50e18);
        assertEq(judge.challengeBond(), 50e18);
        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        judge.setChallengeBond(1001e18);
    }

    function test_SetCounterWindow_Updates() public {
        vm.prank(admin);
        judge.setCounterWindow(36 hours);
        assertEq(judge.counterWindow(), 36 hours);
    }

    function test_SetCounterWindow_RejectsBelowFloor() public {
        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        judge.setCounterWindow(11 hours);
    }

    function test_SetCounterWindow_RejectsAboveCeiling() public {
        vm.expectRevert(Errors.OutOfBounds.selector);
        vm.prank(admin);
        judge.setCounterWindow(8 days);
    }

    function test_Unpause_RestoresMutators() public {
        vm.startPrank(admin);
        judge.pause();
        judge.unpause();
        vm.stopPrank();
        // submitPhantomChallenge is reachable again post-unpause: a happy
        // path call now succeeds, proving the unpause executed. (Failure
        // would surface as EnforcedPause.)
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(1)), true, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(1)), false, 10);
        bytes memory ps = _signProbe(nodePk, probe);
        bytes memory ss = _signStream(nodePk, sr);
        vm.startPrank(challenger);
        token.approve(address(judge), type(uint256).max);
        judge.submitPhantomChallenge(node, probe, ps, sr, ss);
        vm.stopPrank();
    }

    function test_Constructor_RejectsZeroAddresses() public {
        // Each zero-address slot in the constructor must trip the revert.
        vm.expectRevert(Errors.ZeroAddress.selector);
        new SlashJudge(
            IStakingRegistry(address(0)),
            bl,
            channelMock,
            IERC20(address(token)),
            admin,
            100e18,
            24 hours
        );

        vm.expectRevert(Errors.ZeroAddress.selector);
        new SlashJudge(
            reg,
            IContentBlacklist(address(0)),
            channelMock,
            IERC20(address(token)),
            admin,
            100e18,
            24 hours
        );

        vm.expectRevert(Errors.ZeroAddress.selector);
        new SlashJudge(
            reg,
            bl,
            IStablePaymentChannel(address(0)),
            IERC20(address(token)),
            admin,
            100e18,
            24 hours
        );

        vm.expectRevert(Errors.ZeroAddress.selector);
        new SlashJudge(reg, bl, channelMock, IERC20(address(0)), admin, 100e18, 24 hours);

        vm.expectRevert(Errors.ZeroAddress.selector);
        new SlashJudge(reg, bl, channelMock, IERC20(address(token)), address(0), 100e18, 24 hours);
    }

    function test_CounterRate_RejectsWrongOffenseType() public {
        // Submit a phantom challenge (not rate-manipulation), then try to
        // counter via the rate path — must revert OffenseNotCounterable.
        SlashJudge.ProbeResponse memory probe = _probe(bytes32(uint256(0xC0DE)), true, 10);
        SlashJudge.StreamResponse memory sr = _stream(bytes32(uint256(0xC0DE)), false, 10);
        bytes memory ps = _signProbe(nodePk, probe);
        bytes memory ss = _signStream(nodePk, sr);

        vm.startPrank(challenger);
        token.approve(address(judge), type(uint256).max);
        uint256 id = judge.submitPhantomChallenge(node, probe, ps, sr, ss);
        vm.stopPrank();

        vm.expectRevert(SlashJudge.OffenseNotCounterable.selector);
        vm.prank(node);
        judge.counterRateChallenge(id, 10, 20, uint64(block.timestamp), uint64(block.timestamp), "");
    }

    // ---------------- helpers ----------------

    function _probe(
        bytes32 h,
        bool hasBlob,
        uint64 rate
    ) internal view returns (SlashJudge.ProbeResponse memory) {
        return SlashJudge.ProbeResponse(h, hasBlob, rate, uint64(block.timestamp));
    }

    function _stream(
        bytes32 h,
        bool ok,
        uint64 rate
    ) internal view returns (SlashJudge.StreamResponse memory) {
        return
            SlashJudge.StreamResponse(h, ok, rate, 0, bytes32(uint256(1)), uint64(block.timestamp));
    }

    function _signProbe(
        uint256 pk,
        SlashJudge.ProbeResponse memory p
    ) internal view returns (bytes memory) {
        bytes32 structHash =
            keccak256(abi.encode(PROBE_TH, p.hash, p.hasBlob, p.ratePerMb, p.timestamp));
        return _sign(pk, structHash);
    }

    function _signStream(
        uint256 pk,
        SlashJudge.StreamResponse memory s
    ) internal view returns (bytes memory) {
        bytes32 structHash = keccak256(
            abi.encode(STREAM_TH, s.hash, s.ok, s.ratePerMb, s.totalBytes, s.channelId, s.timestamp)
        );
        return _sign(pk, structHash);
    }

    function _signRateChange(
        uint256 pk,
        bytes32 nodeId,
        uint64 oldR,
        uint64 newR,
        uint64 effAt
    ) internal view returns (bytes memory) {
        bytes32 structHash = keccak256(abi.encode(RATE_TH, nodeId, oldR, newR, effAt));
        return _sign(pk, structHash);
    }

    function _signReceipt(
        uint256 pk,
        address requester,
        bytes32 nodeId,
        bytes32 channelId,
        bytes32 blobHash,
        uint64 deliveredAt
    ) internal view returns (bytes memory) {
        bytes32 structHash =
            keccak256(abi.encode(RECEIPT_TH, requester, nodeId, channelId, blobHash, deliveredAt));
        return _sign(pk, structHash);
    }

    function _sign(
        uint256 pk,
        bytes32 structHash
    ) internal view returns (bytes memory) {
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", DOMAIN, structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(pk, digest);
        return abi.encodePacked(r, s, v);
    }
}
