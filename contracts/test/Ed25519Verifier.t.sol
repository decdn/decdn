// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { Test } from "forge-std/Test.sol";
import { Ed25519Verifier } from "../src/Ed25519Verifier.sol";
import { IEd25519Verifier } from "../src/interfaces/IEd25519Verifier.sol";
import { SCL_sha512 } from "crypto-lib/hash/SCL_sha512.sol";
import { n, p } from "crypto-lib/fields/SCL_wei25519.sol";

/// @title Ed25519VerifierTest
/// @notice Differential test suite for {Ed25519Verifier}. Vectors are generated
///         by `test/ed25519-vectors` (ed25519-dalek 2.2.0 / curve25519-dalek
///         4.1.3) and pasted verbatim below — see that crate's README to
///         regenerate. Agreement with dalek `verify_strict` is the bar: a more
///         permissive on-chain verifier is a security bug (#669).
contract Ed25519VerifierTest is Test {
    struct Vec {
        bytes32 pk;
        bytes32 message;
        bytes32 r;
        bytes32 s;
    }

    Ed25519Verifier internal verifier;

    // ===================================================================
    // AUTO-GENERATED — do not edit by hand.
    // Source: contracts/test/ed25519-vectors  (cargo run -- --write)
    // Reference: ed25519-dalek 2.2.0 / curve25519-dalek 4.1.3
    // ===================================================================

    // The 8 canonical small-order encodings (order divides cofactor 8).
    bytes32[8] internal smallOrder = [
        bytes32(uint256(0x0100000000000000000000000000000000000000000000000000000000000000)),
        bytes32(uint256(0xc7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac037a)),
        bytes32(uint256(0x0000000000000000000000000000000000000000000000000000000000000080)),
        bytes32(uint256(0x26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc05)),
        bytes32(uint256(0xecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f)),
        bytes32(uint256(0x26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc85)),
        bytes32(uint256(0x0000000000000000000000000000000000000000000000000000000000000000)),
        bytes32(uint256(0xc7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac03fa))
    ];

    // Off-curve: y = 2 (< p), x^2 non-residue; dalek decompress() == None.
    bytes32 internal constant OFF_CURVE_PK = 0x0200000000000000000000000000000000000000000000000000000000000000;

    // Valid vectors: dalek verify_strict() == Ok. sig = abi.encodePacked(R, s).
    function _validVectors() internal pure returns (Vec[] memory v) {
        v = new Vec[](6);
        // seed 01 / msg 00..
        v[0] = Vec(
            0x8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c,
            0x0000000000000000000000000000000000000000000000000000000000000000,
            0x3714689e5478c21106ed9da455589e89bb77bbf09f49503f85a24a5b3035068a,
            0x01291679f92ec6919b6603d2a887ad37fc482d7ea9b2bf079943705a96218c07
        );
        // seed 02 / msg ff..
        v[1] = Vec(
            0x8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394,
            0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff,
            0xee53eea7e31eaef724cf022b54b035657eb610a1c95375fe2bd054ea7b46c304,
            0xae5fa4beb005b65fbec3351ac0725df5dc1d575b95606c09b297a30c0cbc8e03
        );
        // seed 2a / msg 5a..
        v[2] = Vec(
            0x197f6b23e16c8532c6abc838facd5ea789be0c76b2920334039bfa8b3d368d61,
            0x5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a,
            0x2028849cf938c64e0810bc8a85d27933019a4e71427db637847e65bd08048496,
            0xf2311a9487a5ebafd2d75f06b99331193946707b7d090978f3c16a1e9bb32406
        );
        // seed 63 / msg incr
        v[3] = Vec(
            0xa7f6dfaf8f38b89ba8ce649b594f91e4d01fdc57f9c9493df43b5e50a9987367,
            0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f,
            0x06eb9845a09ac8e0f6092a693bc9f7ba92a68365b0a1fc5dfe47a6f4b426e072,
            0x4bdea0f91ec4a4cb684439be1b6262cc715a66099a2fc0af8e668f31c0e3d30e
        );
        // seed c8 / msg 01..
        v[4] = Vec(
            0x97ffc883c80bee7237ef95d9b9b703d4ad63e60a21e605867682b75b8b3f4303,
            0x0101010101010101010101010101010101010101010101010101010101010101,
            0x61d4a28efb582e3709e26656a4bb8c1626e6de84acab3ef6ab1d0ec15bd5e76b,
            0x7880bb446022193bc1653944a1a9cb5d347052ed76c4f9730226a7210fb3230f
        );
        // seed ff / msg hibit
        v[5] = Vec(
            0x76a1592044a6e4f511265bca73a604d90b0529d1df602be30a19a9257660d1f5,
            0x0000000000000000000000000000000000000000000000000000000000000080,
            0x741a874356c3ee801b3bb147da295b5f6e4c595e3fee1d90a1444fd49335f995,
            0x7cb05c5d43792407bb0167f88c51481961500632876c71d320379266dd57ae02
        );
    }

    // ===================================================================
    // END AUTO-GENERATED
    // ===================================================================

    function setUp() public {
        verifier = new Ed25519Verifier();
    }

    function _sig(bytes32 r, bytes32 s) internal pure returns (bytes memory) {
        return abi.encodePacked(r, s);
    }

    /// Every dalek-accepted signature must verify on-chain.
    function test_ValidVectorsAccepted() public view {
        Vec[] memory v = _validVectors();
        for (uint256 i; i < v.length; ++i) {
            assertTrue(verifier.verify(v[i].pk, v[i].message, _sig(v[i].r, v[i].s)), "valid signature must verify");
        }
    }

    /// The interface promises `view` so callers reach it via STATICCALL; prove
    /// the full path (incl. the SCL library delegatecall) runs in that context.
    function test_VerifyRunsUnderStaticcall() public view {
        Vec memory v = _validVectors()[0];
        (bool ok, bytes memory ret) =
            address(verifier).staticcall(abi.encodeCall(IEd25519Verifier.verify, (v.pk, v.message, _sig(v.r, v.s))));
        assertTrue(ok, "verify must not revert under STATICCALL");
        assertTrue(abi.decode(ret, (bool)), "valid signature must verify under STATICCALL");
    }

    // --- Negative cases: any single mismatched component must be rejected. ---

    function test_WrongMessageRejected() public view {
        Vec[] memory v = _validVectors();
        assertFalse(verifier.verify(v[0].pk, v[1].message, _sig(v[0].r, v[0].s)));
    }

    function test_WrongPubKeyRejected() public view {
        Vec[] memory v = _validVectors();
        assertFalse(verifier.verify(v[1].pk, v[0].message, _sig(v[0].r, v[0].s)));
    }

    function test_WrongRRejected() public view {
        Vec[] memory v = _validVectors();
        assertFalse(verifier.verify(v[0].pk, v[0].message, _sig(v[1].r, v[0].s)));
    }

    function test_WrongSRejected() public view {
        Vec[] memory v = _validVectors();
        assertFalse(verifier.verify(v[0].pk, v[0].message, _sig(v[0].r, v[1].s)));
    }

    /// Classic ed25519 malleability: s' = s + L. dalek rejects (s >= L); we match
    /// via SCL's `s >= n` range check.
    function test_MalleatedScalarRejected() public view {
        Vec memory v = _validVectors()[0];
        assertTrue(verifier.verify(v.pk, v.message, _sig(v.r, v.s)), "control must verify");
        uint256 sNat = SCL_sha512.Swap256(uint256(v.s)); // wire (LE) -> natural scalar < n
        bytes32 sMal = bytes32(SCL_sha512.Swap256(sNat + n)); // s + L, back to wire (LE)
        assertFalse(verifier.verify(v.pk, v.message, _sig(v.r, sMal)), "s + L must be rejected");
    }

    /// dalek verify_strict rejects a small-order public key A.
    function test_SmallOrderPubKeyRejected() public view {
        Vec memory v = _validVectors()[0];
        for (uint256 i; i < smallOrder.length; ++i) {
            assertFalse(verifier.verify(smallOrder[i], v.message, _sig(v.r, v.s)), "small-order A must be rejected");
        }
    }

    /// dalek verify_strict rejects a small-order signature point R.
    function test_SmallOrderRRejected() public view {
        Vec memory v = _validVectors()[0];
        for (uint256 i; i < smallOrder.length; ++i) {
            assertFalse(verifier.verify(v.pk, v.message, _sig(smallOrder[i], v.s)), "small-order R must be rejected");
        }
    }

    /// A public key with y == p (smallest non-canonical y) must be rejected.
    function test_NonCanonicalYRejected() public view {
        bytes32 pk = bytes32(SCL_sha512.Swap256(p)); // wire-encode natural y = p, sign 0
        Vec memory v = _validVectors()[0];
        assertFalse(verifier.verify(pk, v.message, _sig(v.r, v.s)));
    }

    /// A canonical (y < p) but off-curve public key must be rejected.
    function test_OffCurvePubKeyRejected() public view {
        Vec memory v = _validVectors()[0];
        assertFalse(verifier.verify(OFF_CURVE_PK, v.message, _sig(v.r, v.s)));
    }

    function test_WrongLengthSignatureRejected() public view {
        Vec memory v = _validVectors()[0];
        assertFalse(verifier.verify(v.pk, v.message, ""), "empty");
        assertFalse(verifier.verify(v.pk, v.message, hex"00"), "1 byte");
        assertFalse(verifier.verify(v.pk, v.message, abi.encodePacked(v.r)), "32 bytes");
        assertFalse(verifier.verify(v.pk, v.message, abi.encodePacked(v.r, v.s, uint8(0))), "65 bytes");
    }
}
