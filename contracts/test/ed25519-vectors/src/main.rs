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
//!   3. One off-curve public key (`OFF_CURVE_PK`) for `Ed25519Verifier.t.sol`.
//!   4. A `CapacityBond.registerNode` vector for `CapacityBondRegionE2E.t.sol`:
//!      a real ed25519 signature over the on-chain ownership digest
//!      `keccak256(nodeId ‖ operator ‖ chainId ‖ registrationNonce)`, so the
//!      e2e test can register a node through the production verifier.
//!
//! Run: `cargo run` (offline-capable; see README.md).

use curve25519_dalek::constants::EIGHT_TORSION;
use curve25519_dalek::edwards::CompressedEdwardsY;
use ed25519_dalek::{Signer, SigningKey};
use sha3::{Digest, Keccak256};

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

    emit_register_node_vector();
}

// -- 4. CapacityBond.registerNode ownership vector. --
//
// `CapacityBond._verifyEd25519OwnershipSignature` checks
// `ed25519Verifier.verify(nodeId, messageHash, sig)` where
//   messageHash = keccak256(abi.encodePacked(
//       nodeId (bytes32), msg.sender (address, 20B),
//       block.chainid (uint256, 32B BE), registrationNonce[nodeId] (uint64, 8B BE)))
// and the verifier treats the 32-byte `messageHash` as the ed25519 message. So
// the signature must be a `verify_strict`-valid ed25519 signature over those 32
// bytes, with the public key equal to `nodeId`.
//
// The operator address feeds the digest, so it must be fixed at generation time.
// We use Foundry's default account #0 (mnemonic "test test ... junk"), whose
// private key is forge-signable in the test (for the EIP-712 binding signature)
// and whose address is well-known and stable. `CapacityBondRegionE2E.t.sol`
// asserts `vm.addr(REG_OP_PK) == REG_OPERATOR` so any drift fails loudly.
fn emit_register_node_vector() {
    // Foundry / anvil default account #0.
    const REG_OP_PK: [u8; 32] =
        hex_lit("ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80");
    const REG_OPERATOR: [u8; 20] = hex_lit("f39fd6e51aad88f6f4ce6ab8827279cfffb92266");
    // Foundry's default test chain id.
    const CHAIN_ID: u64 = 31337;
    // registrationNonce[nodeId] is 0 for a never-registered nodeId.
    const NONCE: u64 = 0;

    // Deterministic ed25519 seed distinct from the section-2 differential seeds.
    let sk = SigningKey::from_bytes(&[0x11u8; 32]);
    let vk = sk.verifying_key();
    let node_id = vk.to_bytes();
    assert!(
        !vk.to_edwards().is_small_order(),
        "register-node public key unexpectedly small order"
    );

    // Build the abi.encodePacked preimage: 32 + 20 + 32 + 8 = 92 bytes.
    let mut preimage = [0u8; 92];
    preimage[0..32].copy_from_slice(&node_id);
    preimage[32..52].copy_from_slice(&REG_OPERATOR);
    preimage[52..84].copy_from_slice(&u256_be(CHAIN_ID));
    preimage[84..92].copy_from_slice(&NONCE.to_be_bytes());
    let digest: [u8; 32] = Keccak256::digest(preimage).into();

    // Sign the 32-byte digest as the ed25519 message; confirm strict acceptance.
    let sig = sk.sign(&digest);
    vk.verify_strict(&digest, &sig)
        .expect("dalek verify_strict must accept the registerNode signature");

    println!("\n    // -- registerNode ownership vector (CapacityBondRegionE2E.t.sol) --");
    println!("    // nodeId = ed25519 public key; chainId pinned to Foundry default 31337.");
    println!(
        "    bytes32 internal constant REG_NODE_ID = 0x{};",
        h(&node_id)
    );
    println!(
        "    address internal constant REG_OPERATOR = 0x{};",
        eip55(&REG_OPERATOR)
    );
    println!(
        "    uint256 internal constant REG_OP_PK = 0x{};",
        h(&REG_OP_PK)
    );
    println!("    uint256 internal constant REG_CHAIN_ID = {CHAIN_ID};");
    println!(
        "    bytes internal constant REG_ED25519_SIG = hex\"{}\";",
        h(&sig.to_bytes())
    );
}

/// uint256 big-endian encoding of a `u64` (matches `abi.encodePacked(uint256)`).
fn u256_be(v: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[24..].copy_from_slice(&v.to_be_bytes());
    out
}

/// EIP-55 mixed-case checksum of a 20-byte address (Solidity rejects
/// all-lowercase address literals as failing the checksum test).
fn eip55(addr: &[u8; 20]) -> String {
    let lower = hex::encode(addr);
    let hash = Keccak256::digest(lower.as_bytes());
    lower
        .chars()
        .enumerate()
        .map(|(i, c)| {
            if c.is_ascii_digit() {
                c
            } else {
                // High nibble of hash byte i/2 for even i, low nibble for odd i.
                let nibble = (hash[i / 2] >> (if i % 2 == 0 { 4 } else { 0 })) & 0x0f;
                if nibble >= 8 {
                    c.to_ascii_uppercase()
                } else {
                    c
                }
            }
        })
        .collect()
}

/// Compile-time hex decode for fixed-size byte arrays.
const fn hex_lit<const N: usize>(s: &str) -> [u8; N] {
    let b = s.as_bytes();
    assert!(b.len() == 2 * N, "hex literal length mismatch");
    let mut out = [0u8; N];
    let mut i = 0;
    while i < N {
        out[i] = (nibble(b[2 * i]) << 4) | nibble(b[2 * i + 1]);
        i += 1;
    }
    out
}

const fn nibble(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => panic!("invalid hex nibble"),
    }
}
