// SPDX-License-Identifier: MIT
pragma solidity 0.8.28;

import { IEd25519Verifier } from "./interfaces/IEd25519Verifier.sol";
import { SCL_EIP6565 } from "crypto-lib/lib/libSCL_EIP6565.sol";
import { SCL_sha512 } from "crypto-lib/hash/SCL_sha512.sol";
import { ModInv } from "crypto-lib/modular/SCL_modular.sol";
import { p, d, pMINUS_1, pp3div8, sqrtm1 } from "crypto-lib/fields/SCL_wei25519.sol";

/// @title Ed25519Verifier
/// @notice Production RFC 8032 PureEdDSA (ed25519) signature verifier behind the
///         frozen {IEd25519Verifier} surface.
/// @dev Verification of `[S]B == R + [k]A` is delegated to the audited Smoo.th
///      Crypto Lib EIP-6565 routine (`Verify_LE`), pinned as a git
///      submodule at `lib/crypto-lib`. SCL takes a
///      pre-expanded key, so this contract first performs the RFC 8032 §5.1.3
///      point decompression of the 32-byte NodeId together with the
///      strict-verification guards — reject small-order `A`/`R`, non-canonical
///      `y`, and off-curve `A` — that bring this verifier to parity with
///      `ed25519-dalek::verify_strict`, the check deCDN nodes run off-chain. A
///      more permissive on-chain verifier than dalek would let an attacker bind
///      a NodeId with a signature the network itself rejects, so parity is the
///      security bar. All work is `view` (modexp via the 0x05 precompile under
///      STATICCALL), so callers stay reentrancy-safe.
contract Ed25519Verifier is IEd25519Verifier {
    /// @dev Low 255 bits of the natural-form encoding select `y`; bit 255 is the
    ///      sign (low bit) of `x`.
    uint256 private constant SIGN_BIT = 1 << 255;
    uint256 private constant Y_MASK = SIGN_BIT - 1;

    /// @inheritdoc IEd25519Verifier
    function verify(bytes32 publicKey, bytes32 messageHash, bytes calldata signature) external view returns (bool) {
        if (signature.length != 64) {
            return false;
        }

        // Wire layout: signature[0:32] = R, signature[32:64] = s, both little
        // endian. Read as big-endian words; `Verify_LE` swaps `s` and hashes `R`
        // as raw wire bytes, matching this representation.
        uint256 r;
        uint256 s;
        assembly ("memory-safe") {
            r := calldataload(signature.offset)
            s := calldataload(add(signature.offset, 32))
        }

        // Strict guard (matches dalek verify_strict): reject small-order R and A.
        if (_isSmallOrder(bytes32(r)) || _isSmallOrder(publicKey)) {
            return false;
        }

        // Decompress A with canonical-y and on-curve guards.
        (uint256 ax, uint256 ay, bool ok) = _decompress(publicKey);
        if (!ok) {
            return false;
        }

        // SCL consumes the affine Weierstrass-25519 coordinates of A in
        // extKpub[0..1] and the raw compressed key in extKpub[4]; extKpub[2..3]
        // are unused on this path.
        uint256[5] memory extKpub;
        (extKpub[0], extKpub[1]) = SCL_EIP6565.Edwards2WeierStrass(ax, ay);
        extKpub[4] = uint256(publicKey);

        return SCL_EIP6565.Verify_LE(string(abi.encodePacked(messageHash)), r, s, extKpub);
    }

    /// @dev RFC 8032 §5.1.3 decompression of a 32-byte ed25519 public key into
    ///      Edwards affine coordinates. `ok` is false for a non-canonical
    ///      encoding (`y >= p`) or an off-curve key (`x^2` is a non-residue).
    function _decompress(bytes32 pk) private view returns (uint256 x, uint256 y, bool ok) {
        uint256 comp = SCL_sha512.Swap256(uint256(pk)); // wire (LE) -> natural integer
        uint256 sign = comp >> 255;
        y = comp & Y_MASK;
        if (y >= p) {
            return (0, 0, false); // non-canonical y
        }
        uint256 y2 = mulmod(y, y, p);
        uint256 u = addmod(y2, pMINUS_1, p); // y^2 - 1
        uint256 v = addmod(mulmod(d, y2, p), 1, p); // d*y^2 + 1
        if (v == 0) {
            return (0, 0, false);
        }
        uint256 x2 = mulmod(u, ModInv(v, p), p); // x^2 = (y^2 - 1) / (d*y^2 + 1)
        bool isQr;
        (x, isQr) = _sqrt(x2);
        if (!isQr) {
            return (0, 0, false); // x^2 is a non-residue -> A is off-curve
        }
        if (x == 0 && sign == 1) {
            return (0, 0, false); // RFC 8032: x == 0 with the sign bit set is invalid
        }
        if ((x & 1) != sign) {
            x = p - x;
        }
        ok = true;
    }

    /// @dev Modular square root for `p = 5 (mod 8)`: candidate `a^((p+3)/8)`,
    ///      corrected by `sqrt(-1)` when needed. `isQr` is false when `a` is a
    ///      quadratic non-residue (no root exists), letting the caller treat the
    ///      key as off-curve without reverting.
    function _sqrt(uint256 a) private view returns (uint256 root, bool isQr) {
        root = _modexp(a, pp3div8, p);
        // Happy path (~half of residues): the first candidate is the root, so
        // skip re-squaring the corrected value.
        if (mulmod(root, root, p) == a) {
            isQr = true;
        } else {
            root = mulmod(root, sqrtm1, p);
            isQr = mulmod(root, root, p) == a;
        }
    }

    /// @dev `base^e mod m` via the modexp precompile (0x05). STATICCALL keeps the
    ///      enclosing computation `view`.
    function _modexp(uint256 base, uint256 e, uint256 m) private view returns (uint256 result) {
        assembly ("memory-safe") {
            let ptr := mload(0x40)
            mstore(ptr, 0x20) // base length
            mstore(add(ptr, 0x20), 0x20) // exponent length
            mstore(add(ptr, 0x40), 0x20) // modulus length
            mstore(add(ptr, 0x60), base)
            mstore(add(ptr, 0x80), e)
            mstore(add(ptr, 0xa0), m)
            // Output lands in the scratch space at 0x00; inputs above the free
            // pointer are scratch (the pointer is intentionally not advanced).
            if iszero(staticcall(gas(), 0x05, ptr, 0xc0, 0x00, 0x20)) { revert(0, 0) }
            result := mload(0x00)
        }
    }

    /// @dev True when the 32-byte wire value is the canonical compressed encoding
    ///      of a point whose order divides the cofactor 8. These are the eight
    ///      torsion points (curve25519-dalek `EIGHT_TORSION`); dalek's
    ///      `verify_strict` rejects `A` or `R` in this set, so this verifier does
    ///      too. `test/Ed25519Verifier.t.sol` pins this list against the
    ///      reference implementation. Non-canonical encodings of these points are
    ///      caught separately by the `y >= p` guard (for `A`) and by SCL's
    ///      canonical-`R` recomputation (for `R`).
    function _isSmallOrder(bytes32 e) private pure returns (bool) {
        return e == 0x0100000000000000000000000000000000000000000000000000000000000000
            || e == 0x0000000000000000000000000000000000000000000000000000000000000000
            || e == 0x0000000000000000000000000000000000000000000000000000000000000080
            || e == 0xecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f
            || e == 0x26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc05
            || e == 0x26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc85
            || e == 0xc7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac037a
            || e == 0xc7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac03fa;
    }
}
