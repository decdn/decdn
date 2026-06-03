// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";

import { CapacityBond } from "../src/CapacityBond.sol";
import { ContentBlacklist } from "../src/ContentBlacklist.sol";
import { Ed25519Verifier } from "../src/Ed25519Verifier.sol";
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

    address internal admin = address(0xA11CE);

    uint256 internal constant MIN_BOND = 50_000e18;
    uint256 internal constant UNBONDING = 7 days;
    uint256 internal constant REGION_WINDOW = 7 days;
    uint256 internal constant APPEAL_BOND = 100e18;

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
            regionStabilityWindow_: REGION_WINDOW
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
        bytes memory bindingSig = _signBindNode(REG_OP_PK, REG_OPERATOR, REG_NODE_ID);

        // Flip one byte of the otherwise-valid signature.
        bytes memory badSig = bytes.concat(REG_ED25519_SIG);
        badSig[0] = bytes1(uint8(badSig[0]) ^ 0xff);

        vm.prank(REG_OPERATOR);
        vm.expectRevert(CapacityBond.InvalidEd25519Signature.selector);
        bond.registerNode(REG_NODE_ID, hex"", "us-east", bindingSig, badSig);
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
        bytes memory bindingSig = _signBindNode(REG_OP_PK, REG_OPERATOR, REG_NODE_ID);
        vm.prank(REG_OPERATOR);
        vm.expectRevert(CapacityBond.InvalidEd25519Signature.selector);
        bond.registerNode(REG_NODE_ID, hex"", "us-east", bindingSig, REG_ED25519_SIG);
    }

    // ----------------------------------------------------------------------
    // Cross-contract eject
    // ----------------------------------------------------------------------

    /// @notice The cross-contract eject path: `ContentBlacklist.addOperator`
    ///         (GOVERNANCE_ROLE) calls `CapacityBond.ejectNode` (BLACKLIST_ROLE),
    ///         deactivating the node and bumping `registrationNonce`.
    /// @dev    Issue #689 scope item #3 asks to cover the eject "triggered when
    ///         `regionLastChanged` falls outside the attestation window." That
    ///         region-ripening trigger has no production implementation in this
    ///         revision — `CapacityBond.sol` (region-attestation storage doc)
    ///         defers the predicate that reads `regionPrev` / `regionLastChanged`
    ///         to the ADR 030 enforcement PR, and no contract reads those slots
    ///         today. So this covers the only cross-contract eject path that
    ///         exists: the blacklist-driven one. See the note on issue #689.
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
    // Helpers
    // ----------------------------------------------------------------------

    /// @dev Bond the minimum and register `REG_NODE_ID` with a fresh EIP-712
    ///      binding signature and the generated ed25519 ownership signature.
    function _bondAndRegister() internal {
        vm.prank(REG_OPERATOR);
        bond.bond(MIN_BOND);
        bytes memory bindingSig = _signBindNode(REG_OP_PK, REG_OPERATOR, REG_NODE_ID);
        vm.prank(REG_OPERATOR);
        bond.registerNode(REG_NODE_ID, hex"", "us-east", bindingSig, REG_ED25519_SIG);
    }

    /// @dev Construct the EIP-712 `BindNodeId(bytes32 nodeId, uint64 nonce)`
    ///      digest used by `_verifyBindingSignature` and ECDSA-sign it with
    ///      `opPk`. Reads the current nonce off the contract so the helper works
    ///      for both the initial bind and any subsequent rebind.
    function _signBindNode(uint256 opPk, address opAddr, bytes32 nodeId) internal view returns (bytes memory) {
        uint64 nonce = bond.bindingNonce(opAddr);
        bytes32 structHash = keccak256(abi.encode(bond.BIND_NODE_TYPEHASH(), nodeId, nonce));
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
