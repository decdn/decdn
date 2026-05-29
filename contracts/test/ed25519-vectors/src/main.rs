//! Differential test-vector generator for `Ed25519Verifier.sol`.
//!
//! Emits paste-ready Solidity for `test/Ed25519Verifier.t.sol`:
//!   1. The 8 canonical small-order point encodings (derived from dalek's
//!      `EIGHT_TORSION`, not hand-written), which `verify_strict` rejects as A
//!      or R — our on-chain verifier must reject the same set.
//!   2. Valid `(pk, msg, R, s)` vectors produced by ed25519-dalek and confirmed
//!      with `verify_strict`, so the on-chain verifier is checked against the
//!      reference implementation (agreement with dalek == RFC 8032 conformance
//!      for the 32-byte-message case our protocol uses).
//!
//! Run: `cargo run` (offline-capable; see README.md).

use curve25519_dalek::constants::EIGHT_TORSION;
use curve25519_dalek::edwards::CompressedEdwardsY;
use ed25519_dalek::{Signer, SigningKey};

fn h(b: &[u8]) -> String {
    hex::encode(b)
}

fn main() {
    println!("// ===================================================================");
    println!("// AUTO-GENERATED — do not edit by hand.");
    println!("// Source: contracts/test/ed25519-vectors  (cargo run)");
    println!("// Reference: ed25519-dalek 2.2.0 / curve25519-dalek 4.1.3");
    println!("// ===================================================================\n");

    // -- 1. Small-order (8-torsion) canonical compressed encodings (LE wire). --
    // `is_small_order()` is `[8]P == identity`, so EIGHT_TORSION is exactly the
    // set dalek's verify_strict refuses as A or R. There are exactly 8 such
    // points, each with one canonical encoding.
    let mut seen = std::collections::HashSet::new();
    println!("    // The 8 canonical small-order encodings (order divides cofactor 8).");
    println!("    bytes32[8] internal smallOrder = [");
    for (i, pt) in EIGHT_TORSION.iter().enumerate() {
        assert!(pt.is_small_order(), "EIGHT_TORSION[{i}] is not small order");
        let enc = pt.compress().to_bytes();
        assert!(seen.insert(enc), "duplicate small-order encoding at index {i}");
        let comma = if i == 7 { "" } else { "," };
        println!("        bytes32(uint256(0x{})){comma}", h(&enc));
    }
    println!("    ];\n");
    assert_eq!(seen.len(), 8, "expected exactly 8 distinct small-order points");

    // -- 2. Valid differential vectors. --
    // Deterministic seeds; messages span edge byte patterns (all-zero, all-one,
    // high-bit-set, incrementing) to exercise decompression sign/parity paths.
    let mut incr = [0u8; 32];
    for (i, b) in incr.iter_mut().enumerate() {
        *b = i as u8;
    }
    let mut hibit = [0u8; 32];
    hibit[31] = 0x80;

    let cases: [(&str, [u8; 32], [u8; 32]); 6] = [
        ("seed 01 / msg 00..", [0x01u8; 32], [0u8; 32]),
        ("seed 02 / msg ff..", [0x02u8; 32], [0xffu8; 32]),
        ("seed 2a / msg 5a..", [0x2au8; 32], [0x5au8; 32]),
        ("seed 63 / msg incr", [0x63u8; 32], incr),
        ("seed c8 / msg 01..", [0xc8u8; 32], [0x01u8; 32]),
        ("seed ff / msg hibit", [0xffu8; 32], hibit),
    ];

    println!("    // Valid vectors: dalek verify_strict() == Ok. sig = abi.encodePacked(R, s).");
    println!("    function _validVectors() internal pure returns (Vec[] memory v) {{");
    println!("        v = new Vec[]({});", cases.len());
    for (idx, (label, seed, msg)) in cases.iter().enumerate() {
        let sk = SigningKey::from_bytes(seed);
        let vk = sk.verifying_key();
        let sig = sk.sign(msg);
        vk.verify_strict(msg, &sig)
            .expect("dalek verify_strict must accept its own signature");
        let sb = sig.to_bytes();
        let (r, s) = sb.split_at(32);
        // Public key from a clamped scalar is torsion-free, never small order.
        assert!(
            !vk.to_edwards().is_small_order(),
            "unexpected small-order public key"
        );
        println!("        // {label}");
        println!(
            "        v[{idx}] = Vec(0x{}, 0x{}, 0x{}, 0x{});",
            h(vk.as_bytes()),
            h(msg),
            h(r),
            h(s)
        );
    }
    println!("    }}\n");

    // -- 3. An off-curve key: canonical y (< p) but x^2 is a non-residue, so
    // dalek's decompress() returns None. Isolates the on-curve guard from the
    // non-canonical-y guard. Small y keeps the encoding well inside [0, p).
    for yv in 2u64..100_000 {
        let mut enc = [0u8; 32];
        enc[..8].copy_from_slice(&yv.to_le_bytes());
        if CompressedEdwardsY(enc).decompress().is_none() {
            println!("    // Off-curve: y = {yv} (< p), x^2 non-residue; dalek decompress() == None.");
            println!("    bytes32 internal constant OFF_CURVE_PK = 0x{};", h(&enc));
            break;
        }
    }
}
