//! Differential test-vector generator for `Ed25519Verifier.sol`.
//!
//! Emits Solidity for `test/Ed25519Verifier.t.sol`:
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
//! Sections 1–3 form one `AUTO-GENERATED` block in `Ed25519Verifier.t.sol`;
//! section 4 forms another in `CapacityBondRegionE2E.t.sol`.
//!
//! Usage (offline-capable; see README.md):
//!   `cargo run`            — print both blocks to stdout.
//!   `cargo run -- --write` — splice both blocks into their `.t.sol` files in
//!                            place, between the existing marker frames. Run
//!                            `forge fmt` afterwards to normalise formatting.
//!
//! CI (`.github/workflows/ci.yml`, job `ed25519-vectors`) runs `--write` then
//! `forge fmt` and fails on any diff, so committed vectors can never silently
//! drift from a fresh dalek run.

use std::collections::HashSet;
use std::fmt::Write as _;

use curve25519_dalek::constants::EIGHT_TORSION;
use curve25519_dalek::edwards::CompressedEdwardsY;
use ed25519_dalek::{Signer, SigningKey};
use sha3::{Digest, Keccak256};

/// Reference dalek versions, echoed into the generated marker frame. Keep in
/// sync with `Cargo.toml`'s `=` pins; CI's pin-parity guard asserts these match
/// the version `decdn-incentive` resolves off-chain.
const ED25519_DALEK_VERSION: &str = "2.2.0";
const CURVE25519_DALEK_VERSION: &str = "4.1.3";

fn main() {
    let write_mode = match std::env::args().nth(1).as_deref() {
        None => false,
        Some("--write") => true,
        Some(other) => {
            eprintln!("error: unknown argument `{other}`\nusage: ed25519-vectors [--write]");
            std::process::exit(2);
        }
    };

    // Sections 1, 3, 2 in that order: constants (smallOrder, OFF_CURVE_PK)
    // before the `_validVectors()` function, matching Solidity layout convention
    // (and the committed file) so `--write` + `forge fmt` is a no-op when fresh.
    let verifier_block = build_verifier_block();
    let register_block = build_register_node_block();

    if write_mode {
        // CARGO_MANIFEST_DIR is `contracts/test/ed25519-vectors`; the target
        // files sit one level up in `contracts/test`.
        let manifest = env!("CARGO_MANIFEST_DIR");
        splice(
            &format!("{manifest}/../Ed25519Verifier.t.sol"),
            &verifier_block,
        );
        splice(
            &format!("{manifest}/../CapacityBondRegionE2E.t.sol"),
            &register_block,
        );
        eprintln!(
            "wrote test/Ed25519Verifier.t.sol and test/CapacityBondRegionE2E.t.sol; \
             run `forge fmt` next"
        );
    } else {
        print!("{verifier_block}");
        print!("\n{register_block}");
    }
}

/// Opening marker frame (4-space indented to sit inside the contract body).
fn open_frame() -> String {
    format!(
        "    // ===================================================================\n\
         \x20   // AUTO-GENERATED — do not edit by hand.\n\
         \x20   // Source: contracts/test/ed25519-vectors  (cargo run -- --write)\n\
         \x20   // Reference: ed25519-dalek {ED25519_DALEK_VERSION} / curve25519-dalek {CURVE25519_DALEK_VERSION}\n\
         \x20   // ===================================================================\n"
    )
}

/// Closing marker frame.
fn close_frame() -> &'static str {
    "    // ===================================================================\n\
     \x20   // END AUTO-GENERATED\n\
     \x20   // ===================================================================\n"
}

/// Sections 1 (small-order set), 3 (off-curve key), 2 (valid vectors) for
/// `Ed25519Verifier.t.sol`.
fn build_verifier_block() -> String {
    let mut o = open_frame();
    o.push('\n');

    // -- 1. Small-order (8-torsion) canonical compressed encodings (LE wire). --
    // `is_small_order()` is `[8]P == identity`, so EIGHT_TORSION is exactly the
    // set dalek's verify_strict refuses as A or R. There are exactly 8 such
    // points, each with one canonical encoding.
    let mut seen = HashSet::new();
    let _ = writeln!(
        o,
        "    // The 8 canonical small-order encodings (order divides cofactor 8)."
    );
    let _ = writeln!(o, "    bytes32[8] internal smallOrder = [");
    for (i, pt) in EIGHT_TORSION.iter().enumerate() {
        assert!(pt.is_small_order(), "EIGHT_TORSION[{i}] is not small order");
        let enc = pt.compress().to_bytes();
        assert!(
            seen.insert(enc),
            "duplicate small-order encoding at index {i}"
        );
        let comma = if i == 7 { "" } else { "," };
        let _ = writeln!(o, "        bytes32(uint256(0x{})){comma}", h(&enc));
    }
    let _ = writeln!(o, "    ];");
    assert_eq!(
        seen.len(),
        8,
        "expected exactly 8 distinct small-order points"
    );
    o.push('\n');

    // -- 3. An off-curve key: canonical y (< p) but x^2 is a non-residue, so
    // dalek's decompress() returns None. Isolates the on-curve guard from the
    // non-canonical-y guard. Small y keeps the encoding well inside [0, p).
    for yv in 2u64..100_000 {
        let mut enc = [0u8; 32];
        enc[..8].copy_from_slice(&yv.to_le_bytes());
        if CompressedEdwardsY(enc).decompress().is_none() {
            let _ = writeln!(
                o,
                "    // Off-curve: y = {yv} (< p), x^2 non-residue; dalek decompress() == None."
            );
            let _ = writeln!(
                o,
                "    bytes32 internal constant OFF_CURVE_PK = 0x{};",
                h(&enc)
            );
            break;
        }
    }
    o.push('\n');

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

    let _ = writeln!(
        o,
        "    // Valid vectors: dalek verify_strict() == Ok. sig = abi.encodePacked(R, s)."
    );
    let _ = writeln!(
        o,
        "    function _validVectors() internal pure returns (Vec[] memory v) {{"
    );
    let _ = writeln!(o, "        v = new Vec[]({});", cases.len());
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
        let _ = writeln!(o, "        // {label}");
        let _ = writeln!(
            o,
            "        v[{idx}] = Vec(0x{}, 0x{}, 0x{}, 0x{});",
            h(vk.as_bytes()),
            h(msg),
            h(r),
            h(s)
        );
    }
    let _ = writeln!(o, "    }}");
    o.push('\n');

    o.push_str(close_frame());
    o
}

/// Section 4: the `CapacityBond.registerNode` ownership vector for
/// `CapacityBondRegionE2E.t.sol`.
///
/// `CapacityBond._verifyEd25519OwnershipSignature` checks
/// `ed25519Verifier.verify(nodeId, messageHash, sig)` where
///   messageHash = keccak256(abi.encodePacked(
///       nodeId (bytes32), msg.sender (address, 20B),
///       block.chainid (uint256, 32B BE), registrationNonce[nodeId] (uint64, 8B BE)))
/// and the verifier treats the 32-byte `messageHash` as the ed25519 message. So
/// the signature must be a `verify_strict`-valid ed25519 signature over those 32
/// bytes, with the public key equal to `nodeId`.
///
/// The operator address feeds the digest, so it must be fixed at generation
/// time. We use Foundry's default account #0 (mnemonic "test test ... junk"),
/// whose private key is forge-signable in the test (for the EIP-712 binding
/// signature) and whose address is well-known and stable.
/// `CapacityBondRegionE2E.t.sol` asserts `vm.addr(REG_OP_PK) == REG_OPERATOR` so
/// any drift fails loudly.
fn build_register_node_block() -> String {
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

    let mut o = open_frame();
    o.push('\n');
    let _ = writeln!(
        o,
        "    // -- registerNode ownership vector (CapacityBondRegionE2E.t.sol) --"
    );
    let _ = writeln!(
        o,
        "    // nodeId = ed25519 public key; chainId pinned to Foundry default {CHAIN_ID}."
    );
    let _ = writeln!(
        o,
        "    bytes32 internal constant REG_NODE_ID = 0x{};",
        h(&node_id)
    );
    let _ = writeln!(
        o,
        "    address internal constant REG_OPERATOR = 0x{};",
        eip55(&REG_OPERATOR)
    );
    let _ = writeln!(
        o,
        "    uint256 internal constant REG_OP_PK = 0x{};",
        h(&REG_OP_PK)
    );
    let _ = writeln!(
        o,
        "    uint256 internal constant REG_CHAIN_ID = {CHAIN_ID};"
    );
    let _ = writeln!(
        o,
        "    // Real ed25519 signature over the digest above; never hand-edit."
    );
    let _ = writeln!(
        o,
        "    // Regenerate with `cargo run -- --write` (see README) — a tweaked value"
    );
    let _ = writeln!(
        o,
        "    // fails verify_strict, so registerNode would revert InvalidEd25519Signature."
    );
    let _ = writeln!(
        o,
        "    bytes internal constant REG_ED25519_SIG = hex\"{}\";",
        h(&sig.to_bytes())
    );
    o.push('\n');
    o.push_str(close_frame());
    o
}

/// Replace the `AUTO-GENERATED … END AUTO-GENERATED` span (marker frames
/// inclusive) of `path` with `block`. Panics if the markers are missing or
/// out of order — the generator is a dev-only tool, so a hard failure is the
/// right signal. Formatting is left to `forge fmt`.
fn splice(path: &str, block: &str) {
    let src = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let lines: Vec<&str> = src.lines().collect();

    let start_marker = "// AUTO-GENERATED — do not edit by hand.";
    let end_marker = "// END AUTO-GENERATED";
    let ai = lines
        .iter()
        .position(|l| l.trim() == start_marker)
        .unwrap_or_else(|| panic!("{path}: opening `{start_marker}` not found"));
    let bi = lines
        .iter()
        .position(|l| l.trim() == end_marker)
        .unwrap_or_else(|| panic!("{path}: closing `{end_marker}` not found"));
    // The `// ===` frame line sits immediately outside each marker.
    let start = ai
        .checked_sub(1)
        .unwrap_or_else(|| panic!("{path}: opening marker has no frame line above it"));
    let end = bi + 1;
    assert!(
        start < bi && end < lines.len(),
        "{path}: markers out of order"
    );

    let mut out = String::new();
    for l in &lines[..start] {
        out.push_str(l);
        out.push('\n');
    }
    out.push_str(block.trim_end_matches('\n'));
    out.push('\n');
    for l in &lines[end + 1..] {
        out.push_str(l);
        out.push('\n');
    }
    std::fs::write(path, out).unwrap_or_else(|e| panic!("write {path}: {e}"));
}

fn h(b: &[u8]) -> String {
    hex::encode(b)
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
