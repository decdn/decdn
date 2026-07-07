// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { CapacityBond } from "../src/CapacityBond.sol";
import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { Ed25519Verifier } from "../src/Ed25519Verifier.sol";
import { SlashJudge } from "../src/SlashJudge.sol";
import { ISlashJudge } from "../src/interfaces/ISlashJudge.sol";
import { ICapacityBondSlasher } from "../src/interfaces/ICapacityBondSlasher.sol";
import { IContentBlacklistHashView } from "../src/interfaces/IContentBlacklistHashView.sol";
import { IERC20 } from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import { Token } from "../src/Token.sol";

/// @title CapacityBondRegionE2ETest
/// @notice End-to-end ADR 030 `updateRegion` coverage (#689): a node is
///         registered through the *production* {Ed25519Verifier} with a real
///         ed25519 ownership signature (not the mock used by
///         `CapacityBondTest`), then the region cooldown / prev-snapshot /
///         replay-protection and the cross-contract eject path are exercised.
/// @dev    The ed25519 signature is produced by `test/ed25519-vectors`
///         (ed25519-dalek 2.2.0) over the exact `registerNode` ownership digest
///         `keccak256(nodeId ‖ operator ‖ chainId ‖ registrationNonce)` and
///         pasted verbatim below — see that crate's README to regenerate. The
///         operator is Foundry's default account #0 so its key is forge-signable
///         for the EIP-712 binding signature; `setUp` asserts the key/address
///         and chain id match the generated vector so any drift fails loudly.
contract CapacityBondRegionE2ETest is Test {
    Token internal token;
    Ed25519Verifier internal verifier;
    CapacityBond internal bond;
    ContentBlacklist internal deployedBlacklist;

    address internal admin = address(0xA11CE);

    uint256 internal constant MIN_BOND = 50_000e18;
    uint256 internal constant UNBONDING = 7 days;
    uint256 internal constant REGION_WINDOW = 7 days;
    uint256 internal constant APPEAL_BOND = 100e18;

    // SlashJudge wiring for the cross-contract regional/ripening slash path.
    uint256 internal constant CHALLENGE_BOND = 100e18;
    uint256 internal constant MAX_EVIDENCE_AGE_US = 5 days * 1_000_000;
    address internal challenger = address(0xC4A11E);
    bytes32 internal constant BLOB = bytes32(uint256(0xB10B));
    bytes32 internal constant US_EAST = bytes32("us-east");

    // Commit–reveal (#854): mirror of SlashJudge's fixed MIN_REVEAL_DELAY and a
    // fixed salt for the challenger's blind commitment.
    uint256 internal constant REVEAL_DELAY = 1 minutes;
    bytes32 internal constant SALT = bytes32(uint256(0x5A17));

    // ===================================================================
    // AUTO-GENERATED — do not edit by hand.
    // Source: contracts/test/ed25519-vectors  (cargo run)
    // Reference: ed25519-dalek 2.2.0 / curve25519-dalek 4.1.3
    // ===================================================================

    // -- registerNode ownership vector (CapacityBondRegionE2E.t.sol) --
    // nodeId = ed25519 public key; chainId pinned to Foundry default 31337.
    bytes32 internal constant REG_NODE_ID = 0xd04ab232742bb4ab3a1368bd4615e4e6d0224ab71a016baf8520a332c9778737;
    address internal constant REG_OPERATOR = 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266;
    uint256 internal constant REG_OP_PK = 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80;
    uint256 internal constant REG_CHAIN_ID = 31_337;
    // Real ed25519 signature over the digest above; never hand-edit. Regenerate
    // the whole block with `cargo run` (see README) — a tweaked value fails
    // verify_strict, so registerNode would revert InvalidEd25519Signature.
    bytes internal constant REG_ED25519_SIG =
        hex"2d1b1805fe880782cc1b60bc49f7d1e27445870c01e1de195980970a65f4e0c7fa9ea30347bb876c4b1ce64174fd8d6a2b90520d7ed4dea12d805d401ea5e405";

    // ===================================================================
    // END AUTO-GENERATED
    // ===================================================================

    function setUp() public {
        // The ed25519 digest commits to block.chainid; pin it to the value the
        // vector was generated against so a Foundry default change can't silently
        // invalidate the signature.
        vm.chainId(REG_CHAIN_ID);
        assertEq(block.chainid, REG_CHAIN_ID, "chain id must match the generated vector");
        assertEq(vm.addr(REG_OP_PK), REG_OPERATOR, "operator key/address must match the generated vector");

        // A non-zero base timestamp keeps any epoch math comfortably away from 0.
        vm.warp(1_000_000);

        token = new Token(admin);
        verifier = new Ed25519Verifier();
        bond = new CapacityBond({
            token_: token,
            ed25519Verifier_: verifier,
            admin: admin,
            minBond_: MIN_BOND,
            unbondingPeriod_: UNBONDING,
            multiaddrUpdateCooldown_: 0,
            maxMultiaddrSize_: 1024,
            regionStabilityWindow_: REGION_WINDOW,
            currentTermsHash_: bytes32(0)
        });

        vm.prank(admin);
        token.transfer(REG_OPERATOR, 200_000e18);
        vm.prank(REG_OPERATOR);
        token.approve(address(bond), type(uint256).max);
    }

    // ----------------------------------------------------------------------
    // keygen → signed registerNode (real verifier)
    // ----------------------------------------------------------------------

    /// @notice The generated ed25519 vector verifies through the production
    ///         {Ed25519Verifier}: a real signed registration succeeds and the
    ///         node becomes active. Proves the off-chain signing harness and the
    ///         on-chain verifier agree on the `registerNode` ownership digest.
    function test_registerNode_realEd25519Signature_succeeds() public {
        _bondAndRegister();

        assertTrue(bond.isActive(REG_OPERATOR));
        assertTrue(bond.isActiveNode(REG_NODE_ID));
        assertEq(bond.getNodeByAddress(REG_OPERATOR).regionHint, "us-east");
    }

    /// @notice A corrupted ed25519 signature is rejected by the real verifier —
    ///         the negative control proving the verifier is genuinely in the
    ///         registration path and not rubber-stamping any 64-byte blob.
    function test_registerNode_corruptEd25519Signature_reverts() public {
        vm.prank(REG_OPERATOR);
        bond.bond(MIN_BOND);
        bytes memory bindingSig = _signRegisterNode(REG_OP_PK, REG_OPERATOR, REG_NODE_ID, bytes32(0));

        // Flip one byte of the otherwise-valid signature.
        bytes memory badSig = bytes.concat(REG_ED25519_SIG);
        badSig[0] = bytes1(uint8(badSig[0]) ^ 0xff);

        vm.prank(REG_OPERATOR);
        vm.expectRevert(CapacityBond.InvalidEd25519Signature.selector);
        bond.registerNode(REG_NODE_ID, hex"", "us-east", bytes32(0), bindingSig, badSig);
    }

    // ----------------------------------------------------------------------
    // updateRegion — cooldown, prev-snapshot, event
    // ----------------------------------------------------------------------

    /// @notice ADR 030 § Region-stability window over a real registration: the
    ///         first `updateRegion` is cooldown-exempt and snapshots the prior
    ///         region into `regionPrev`; a second call inside
    ///         `regionStabilityWindow` reverts `RegionCooldownActive`; once the
    ///         window elapses the call succeeds and re-snapshots. Asserts the
    ///         `RegionUpdated` payload on both successful calls.
    function test_updateRegion_cooldownPrevSnapshotAndEvent() public {
        _bondAndRegister();

        // First update — no cooldown (regionLastChanged == 0 branch).
        vm.expectEmit(true, false, false, true, address(bond));
        emit CapacityBond.RegionUpdated(REG_NODE_ID, "us-east", "eu-west");
        vm.prank(REG_OPERATOR);
        bond.updateRegion("eu-west");
        assertEq(bond.regionPrev(REG_OPERATOR), "us-east");
        assertEq(bond.regionLastChanged(REG_OPERATOR), uint64(block.timestamp));

        uint64 firstChanged = bond.regionLastChanged(REG_OPERATOR);

        // Second update inside the window reverts with the exact ready time.
        vm.warp(block.timestamp + 1 days);
        vm.prank(REG_OPERATOR);
        vm.expectRevert(
            abi.encodeWithSelector(CapacityBond.RegionCooldownActive.selector, firstChanged + uint64(REGION_WINDOW))
        );
        bond.updateRegion("ap-south");

        // After the window elapses the call succeeds and re-snapshots.
        vm.warp(uint256(firstChanged) + REGION_WINDOW + 1);
        vm.expectEmit(true, false, false, true, address(bond));
        emit CapacityBond.RegionUpdated(REG_NODE_ID, "eu-west", "ap-south");
        vm.prank(REG_OPERATOR);
        bond.updateRegion("ap-south");
        assertEq(bond.regionPrev(REG_OPERATOR), "eu-west");
    }

    // ----------------------------------------------------------------------
    // Replay protection
    // ----------------------------------------------------------------------

    /// @notice The ed25519 ownership signature is bound to
    ///         `registrationNonce[nodeId]`, so it cannot be replayed once the
    ///         nonce advances. `deregisterNode` increments the nonce without
    ///         ejecting (the eject/already-active preconditions revert before
    ///         the signature check, so deregister is the path that isolates the
    ///         ed25519 layer): replaying the original signature in a fresh
    ///         registration now reverts `InvalidEd25519Signature`.
    function test_registerNode_replayAfterNonceBump_reverts() public {
        _bondAndRegister();

        vm.prank(REG_OPERATOR);
        bond.deregisterNode();
        assertEq(bond.registrationNonce(REG_NODE_ID), 1);
        assertFalse(bond.isActive(REG_OPERATOR));

        // The binding nonce is at 1 (bumped by the first registerNode, not by
        // deregister), so re-sign with the live nonce to clear the binding
        // check; the ed25519 signature is replayed verbatim and is now stale
        // (it was signed over registration nonce 0).
        bytes memory bindingSig = _signRegisterNode(REG_OP_PK, REG_OPERATOR, REG_NODE_ID, bytes32(0));
        vm.prank(REG_OPERATOR);
        vm.expectRevert(CapacityBond.InvalidEd25519Signature.selector);
        bond.registerNode(REG_NODE_ID, hex"", "us-east", bytes32(0), bindingSig, REG_ED25519_SIG);
    }

    // ----------------------------------------------------------------------
    // Cross-contract eject
    // ----------------------------------------------------------------------

    /// @notice The cross-contract eject path: `ContentBlacklist.addOperator`
    ///         (GOVERNANCE_ROLE) calls `CapacityBond.ejectNode` (BLACKLIST_ROLE),
    ///         deactivating the node and bumping `registrationNonce`.
    /// @dev    The region-ripening slash-eligibility trigger #689 scope item #3
    ///         asked for now exists (ADR 030 enforcement) — see the
    ///         `test_crossContract_regionalSlash_*` tests below, which drive a
    ///         real `SlashJudge` against this contract's live `regionPrev` /
    ///         `regionLastChanged`. This test still covers the operator-level
    ///         blacklist-driven eject path, which is region-independent.
    function test_crossContractEject_viaContentBlacklist() public {
        _bondAndRegister();

        ContentBlacklist blacklist = new ContentBlacklist(bond, token, admin, APPEAL_BOND);
        // Cache the role getter before pranking: a nested external call inside
        // the pranked statement would otherwise consume the prank.
        bytes32 blacklistRole = bond.BLACKLIST_ROLE();
        vm.prank(admin);
        bond.grantRole(blacklistRole, address(blacklist));

        // Both eject side-effect events fire, in order: the blacklist marker
        // then the node auto-eject carrying the remaining bond (MIN_BOND — the
        // operator bonded the minimum and was never slashed).
        vm.expectEmit(true, false, false, false, address(bond));
        emit CapacityBond.EjectedByBlacklist(REG_OPERATOR);
        vm.expectEmit(true, false, false, true, address(bond));
        emit CapacityBond.NodeAutoEjected(REG_NODE_ID, MIN_BOND);
        vm.prank(admin);
        blacklist.addOperator(REG_OPERATOR);

        assertFalse(bond.isActive(REG_OPERATOR));
        assertFalse(bond.isActiveNode(REG_NODE_ID));
        // The `ejected` flag must be set, not just `active` cleared: it is what
        // the `registerNode` precondition checks, so a re-registration of the
        // operator stays blocked (`OperatorEjected`) rather than slipping
        // through on a cleared `active` alone.
        assertTrue(bond.ejected(REG_OPERATOR));
        // `_ejectNodeEffects` bumps the nonce so the old NodeId can't be reclaimed
        // with a stale ownership proof.
        assertEq(bond.registrationNonce(REG_NODE_ID), 1);
    }

    // ----------------------------------------------------------------------
    // Cross-contract regional + ripening slash (ADR 030 enforcement, #689 item 3)
    // ----------------------------------------------------------------------

    /// @notice A hash blacklisted in the node's CURRENT region makes it slashable
    ///         end-to-end: real `ContentBlacklist` regional entry → real
    ///         `SlashJudge` reads this contract's live region data → real
    ///         `CapacityBond.slash`. Proves the production read path, not a mock.
    function test_crossContract_regionalSlash_currentRegion() public {
        (, SlashJudge judge) = _deployBlacklistAndJudge();
        _bondAndRegister(); // node region = "us-east"

        _blacklistRegional(US_EAST, BLOB);
        // Advance so the served response can post-date the entry, then slash.
        vm.warp(block.timestamp + 10);
        _submitBlacklistSlash(judge, uint64(block.timestamp * 1_000_000 - 5_000_000));

        assertEq(bond.lifetimeOffenseCount(REG_OPERATOR), 1);
    }

    /// @notice ADR 030 ripening: after the node flips us-east → eu-west, an entry
    ///         in its PREVIOUS region (us-east) keeps it slashable for the full
    ///         stability window — reactive region-flip evasion is foreclosed.
    function test_crossContract_regionalSlash_prevRegionInWindow() public {
        (, SlashJudge judge) = _deployBlacklistAndJudge();
        _bondAndRegister();

        _blacklistRegional(US_EAST, BLOB);
        // Flip region (first update is cooldown-exempt): current becomes eu-west,
        // regionPrev snapshots us-east, regionLastChanged = now.
        vm.prank(REG_OPERATOR);
        bond.updateRegion("eu-west");

        // Still inside REGION_WINDOW: the us-east (prev) entry remains in scope.
        vm.warp(block.timestamp + 1 days);
        _submitBlacklistSlash(judge, uint64(block.timestamp * 1_000_000 - 5_000_000));

        assertEq(bond.lifetimeOffenseCount(REG_OPERATOR), 1);
    }

    /// @notice A serve made AFTER the region change ripens (the served `responseTs`
    ///         is more than REGION_WINDOW past the flip) is outside the previous
    ///         region's scope and not slashable for a prev-region entry — the flip
    ///         has fully taken effect. The slash window is anchored to `responseTs`
    ///         (ADR 030 item 2, #801), so the serve, not the challenge time, is what
    ///         must post-date ripening: here the response is taken ~1h before a
    ///         challenge that lands well past the window.
    function test_crossContract_regionalSlash_prevRegionAfterWindow() public {
        (, SlashJudge judge) = _deployBlacklistAndJudge();
        _bondAndRegister();

        _blacklistRegional(US_EAST, BLOB);
        vm.prank(REG_OPERATOR);
        bond.updateRegion("eu-west");

        // The serve post-dates ripening: `responseTs - effective` (flip) exceeds
        // REGION_WINDOW, so us-east (prev) is out of scope and eu-west has no entry.
        // We warp a full window + 1h past the flip, then take responseTs = now - 5s;
        // the 5s only keeps responseTs validly in the past for liveness/skew — it is
        // the flip being > a window before responseTs that drops the prev leg.
        vm.warp(block.timestamp + REGION_WINDOW + 1 hours);
        uint64 ts = uint64(block.timestamp * 1_000_000 - 5_000_000);
        SlashJudge.StreamMsg memory s = _stream(ts);
        bytes memory sig = _signStream(judge, s); // sign before prank (see _submitBlacklistSlash)
        // Reverts in `_checkBlacklistedBefore`, before the commit–reveal check, so
        // no commitment is needed; `salt` is still a required argument (#854).
        vm.prank(challenger);
        vm.expectRevert(abi.encodeWithSelector(SlashJudge.HashNotBlacklisted.selector, BLOB));
        judge.submitBlacklistChallenge(REG_OPERATOR, REG_NODE_ID, BLOB, abi.encode(s), sig, true, SALT);
        assertEq(bond.lifetimeOffenseCount(REG_OPERATOR), 0);
    }

    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------

    /// @dev Deploy a real `ContentBlacklist` + `SlashJudge` wired to `bond`, grant
    ///      the cross-contract roles, and fund the challenger's bond.
    function _deployBlacklistAndJudge() internal returns (ContentBlacklist blacklist, SlashJudge judge) {
        blacklist = new ContentBlacklist(bond, token, admin, APPEAL_BOND);
        deployedBlacklist = blacklist;
        judge = new SlashJudge(
            ICapacityBondSlasher(address(bond)),
            IERC20(address(token)),
            IContentBlacklistHashView(address(blacklist)),
            CHALLENGE_BOND,
            MAX_EVIDENCE_AGE_US,
            admin
        );
        bytes32 slashRole = bond.SLASH_ROLE();
        bytes32 bodyRole = blacklist.REGIONAL_BODY_ROLE();
        vm.startPrank(admin);
        bond.grantRole(slashRole, address(judge));
        blacklist.grantRole(bodyRole, admin);
        token.transfer(challenger, CHALLENGE_BOND * 10);
        vm.stopPrank();
        vm.prank(challenger);
        token.approve(address(judge), type(uint256).max);
    }

    function _blacklistRegional(bytes32 region, bytes32 hash) internal {
        vm.prank(admin);
        deployedBlacklist.addHashRegional(region, hash);
    }

    /// @dev Submit a blacklist challenge with a node-signed stream response at
    ///      `tsUs`; expects a successful slash. Commit–reveal (#854): commit the
    ///      evidence hash, mature past `MIN_REVEAL_DELAY`, then reveal.
    function _submitBlacklistSlash(SlashJudge judge, uint64 tsUs) internal {
        SlashJudge.StreamMsg memory s = _stream(tsUs);
        // Sign BEFORE pranking: `_signStream` makes an external view call that
        // would otherwise consume the prank, leaving `msg.sender` as the test.
        bytes memory sig = _signStream(judge, s);

        bytes32 evidenceHash =
            keccak256(abi.encode(uint8(ISlashJudge.OffenseType.Blacklist), _streamStructHash(judge, s), true));
        vm.prank(challenger);
        judge.commitChallenge(keccak256(abi.encode(evidenceHash, SALT, challenger)));
        // A 1-minute maturation is negligible against the 5-day evidence window.
        vm.warp(block.timestamp + REVEAL_DELAY + 1);

        vm.prank(challenger);
        judge.submitBlacklistChallenge(REG_OPERATOR, REG_NODE_ID, BLOB, abi.encode(s), sig, true, SALT);
    }

    function _stream(uint64 tsUs) internal pure returns (SlashJudge.StreamMsg memory) {
        return SlashJudge.StreamMsg({
            hash: BLOB,
            ok: true,
            ratePerMb: 10,
            totalBytes: 1_048_576,
            channelId: bytes32(uint256(1)),
            timestampUs: tsUs,
            redirect: bytes32(0)
        });
    }

    /// @dev The EIP-712 `StreamResponse` struct hash (the blacklist evidence
    ///      preimage component) over `SlashJudge`'s typehash.
    function _streamStructHash(SlashJudge judge, SlashJudge.StreamMsg memory s) internal view returns (bytes32) {
        return keccak256(
            abi.encode(
                judge.STREAM_RESPONSE_TYPEHASH(),
                s.hash,
                s.ok,
                s.ratePerMb,
                s.totalBytes,
                s.channelId,
                s.timestampUs,
                s.redirect
            )
        );
    }

    /// @dev EIP-712 sign a `StreamResponse` with the operator's secp256k1 key over
    ///      `SlashJudge`'s domain (`"deCDN SlashJudge"` / `"1"`).
    function _signStream(SlashJudge judge, SlashJudge.StreamMsg memory s) internal view returns (bytes memory) {
        bytes32 structHash = _streamStructHash(judge, s);
        bytes32 domainSeparator = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes("deCDN SlashJudge")),
                keccak256(bytes("1")),
                block.chainid,
                address(judge)
            )
        );
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", domainSeparator, structHash));
        (uint8 v, bytes32 r, bytes32 sig) = vm.sign(REG_OP_PK, digest);
        return abi.encodePacked(r, sig, v);
    }

    /// @dev Bond the minimum and register `REG_NODE_ID` with a fresh EIP-712
    ///      binding signature and the generated ed25519 ownership signature.
    function _bondAndRegister() internal {
        vm.prank(REG_OPERATOR);
        bond.bond(MIN_BOND);
        bytes memory bindingSig = _signRegisterNode(REG_OP_PK, REG_OPERATOR, REG_NODE_ID, bytes32(0));
        vm.prank(REG_OPERATOR);
        bond.registerNode(REG_NODE_ID, hex"", "us-east", bytes32(0), bindingSig, REG_ED25519_SIG);
    }

    /// @dev Construct the EIP-712 `RegisterNode(bytes32 nodeId, uint64 nonce,
    ///      bytes32 termsHash)` digest used by `_verifyRegistrationSignature`
    ///      (ADR 019 § Terms Acceptance) and ECDSA-sign it with `opPk`. Reads
    ///      the current nonce off the contract.
    function _signRegisterNode(uint256 opPk, address opAddr, bytes32 nodeId, bytes32 termsHash)
        internal
        view
        returns (bytes memory)
    {
        uint64 nonce = bond.bindingNonce(opAddr);
        bytes32 structHash = keccak256(abi.encode(bond.REGISTER_NODE_TYPEHASH(), nodeId, nonce, termsHash));
        bytes32 domainSeparator = keccak256(
            abi.encode(
                keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
                keccak256(bytes("CapacityBond")),
                keccak256(bytes("1")),
                block.chainid,
                address(bond)
            )
        );
        bytes32 digest = keccak256(abi.encodePacked("\x19\x01", domainSeparator, structHash));
        (uint8 v, bytes32 r, bytes32 s) = vm.sign(opPk, digest);
        return abi.encodePacked(r, s, v);
    }
}
