use bao_tree::io::outboard::PreOrderMemOutboard;
use bao_tree::io::sync::encode_ranges_validated;

use super::*;

/// Blob sizes around every boundary the encoder branches on: the chunk, the
/// chunk group, and powers of two of both.
const SIZES: [u64; 24] = [
    0,
    1,
    63,
    64,
    1023,
    1024,
    1025,
    2048,
    2049,
    16 * 1024 - 1,
    16 * 1024,
    16 * 1024 + 1,
    17 * 1024,
    32 * 1024,
    32 * 1024 + 1,
    48 * 1024,
    100_000,
    (1 << 20) - 1,
    1 << 20,
    (1 << 20) + 1,
    3 * (1 << 20) + 16 * 1024,
    5 * (1 << 20) + 123,
    8 * (1 << 20),
    37 * (1 << 20) + 1024 * 7 + 5,
];

fn blob(size: u64) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

/// The root, the group-level outboard and the tail of `data`: what an origin
/// holds for a blob.
fn origin_parts(data: &[u8]) -> ([u8; 32], Vec<u8>, &[u8]) {
    let ob = PreOrderMemOutboard::create(data, IROH_BLOCK_SIZE);
    let tail_start = usize::try_from(size_proof_tail_start(data.len() as u64)).unwrap();
    (*ob.root.as_bytes(), ob.data, &data[tail_start..])
}

/// The proof the stock `bao-tree` encoder gives from a full chunk-level
/// outboard: the reference for [`encode_size_proof`].
fn stock_proof(data: &[u8]) -> Vec<u8> {
    let ob = PreOrderMemOutboard::create(data, BlockSize::ZERO);
    let mut proof = Vec::new();
    encode_ranges_validated(data, &ob, &last_chunk(), &mut proof).unwrap();
    proof
}

fn wrong_sizes(size: u64) -> Vec<u64> {
    let mut wrong = vec![
        size + 1,
        size + 63,
        size + 64,
        size + 1023,
        size + 1024,
        size + 16 * 1024,
        size * 2 + 1,
        u64::from(u32::MAX),
        1 << 60,
        u64::MAX,
    ];
    for d in [1u64, 2, 63, 64, 65, 1023, 1024, 1025, 16 * 1024] {
        if size >= d {
            wrong.push(size - d);
        }
    }
    wrong.retain(|w| *w != size);
    wrong
}

/// The encoder builds, from the group-level outboard and the last chunk
/// group, the same bytes as the stock encoder builds from a chunk-level
/// outboard of the whole blob.
#[test]
fn the_proof_equals_the_stock_last_chunk_encoding() {
    for size in SIZES {
        let data = blob(size);
        let (root, outboard, tail) = origin_parts(&data);
        let proof = encode_size_proof(root, size, &outboard, tail).unwrap();
        assert_eq!(proof.as_ref(), stock_proof(&data).as_slice(), "size {size}");
        assert_eq!(
            root,
            *blake3::hash(&data).as_bytes(),
            "the root is plain BLAKE3"
        );
    }
}

/// A proof verifies under the true size and under no other size.
#[test]
fn a_proof_verifies_only_under_the_true_size() {
    for size in SIZES {
        let data = blob(size);
        let (root, outboard, tail) = origin_parts(&data);
        let proof = encode_size_proof(root, size, &outboard, tail).unwrap();
        assert!(verify_size_proof(root, size, &proof), "size {size}");
        for claimed in wrong_sizes(size) {
            assert!(
                !verify_size_proof(root, claimed, &proof),
                "a proof of {size} bytes verified as {claimed}"
            );
        }
    }
}

/// A proof is bound to its root: it does not verify under another blob's
/// hash, the empty proof included.
#[test]
fn a_proof_does_not_verify_under_another_root() {
    let other = *blake3::hash(b"another blob").as_bytes();
    for size in SIZES {
        let data = blob(size);
        let (root, outboard, tail) = origin_parts(&data);
        let proof = encode_size_proof(root, size, &outboard, tail).unwrap();
        assert!(!verify_size_proof(other, size, &proof), "size {size}");
    }
    assert!(
        !verify_size_proof(other, 0, &[]),
        "no hash but the empty blob's is 0 bytes"
    );
}

/// One flipped bit anywhere in a proof, a missing byte, or an added byte
/// makes it fail.
#[test]
fn a_changed_proof_does_not_verify() {
    for size in [1u64, 1024, 1025, 100_000, 5 * (1 << 20) + 123] {
        let data = blob(size);
        let (root, outboard, tail) = origin_parts(&data);
        let proof = encode_size_proof(root, size, &outboard, tail)
            .unwrap()
            .to_vec();
        for at in 0..proof.len() {
            let mut bad = proof.clone();
            bad[at] ^= 1;
            assert!(
                !verify_size_proof(root, size, &bad),
                "size {size}, bit flip at {at}"
            );
        }
        let mut longer = proof.clone();
        longer.push(0);
        assert!(
            !verify_size_proof(root, size, &longer),
            "size {size}, one byte more"
        );
        let shorter = &proof[..proof.len() - 1];
        assert!(
            !verify_size_proof(root, size, shorter),
            "size {size}, one byte less"
        );
    }
}

/// A proof is small: the last chunk and 64 bytes for each level on its path.
#[test]
fn a_proof_stays_under_the_stated_bound() {
    for size in SIZES {
        let data = blob(size);
        let (root, outboard, tail) = origin_parts(&data);
        let proof = encode_size_proof(root, size, &outboard, tail).unwrap();
        let chunks = size.div_ceil(CHUNK_BYTES).max(1);
        let levels = usize::try_from((chunks - 1).bit_width()).unwrap();
        assert!(
            proof.len() <= 1024 + PAIR_BYTES * levels,
            "size {size}: {} bytes",
            proof.len()
        );
        assert!(proof.len() <= MAX_SIZE_PROOF_BYTES);
    }
    assert!(!verify_size_proof(
        [0; 32],
        1 << 40,
        &vec![0; MAX_SIZE_PROOF_BYTES + 1]
    ));
}

/// The encoder trusts neither input: a wrong length is a typed error, and
/// bytes that do not belong to the blob give no proof.
#[test]
fn the_encoder_refuses_inputs_that_do_not_prove_the_size() {
    let size = 3 * (1 << 20) + 5000;
    let data = blob(size);
    let (root, outboard, tail) = origin_parts(&data);

    assert_eq!(
        encode_size_proof(root, size, &outboard[1..], tail),
        Err(SizeProofError::OutboardSize {
            expected: outboard.len() as u64,
            got: outboard.len() - 1,
            blob_size: size,
        })
    );
    assert_eq!(
        encode_size_proof(root, size, &outboard, &tail[1..]),
        Err(SizeProofError::TailSize {
            expected: tail.len() as u64,
            got: tail.len() - 1,
            blob_size: size,
        })
    );

    let mut bad_tail = tail.to_vec();
    bad_tail[0] ^= 1;
    assert_eq!(
        encode_size_proof(root, size, &outboard, &bad_tail),
        Err(SizeProofError::Verification { blob_size: size })
    );
    let mut bad_outboard = outboard.clone();
    bad_outboard[0] ^= 1;
    assert_eq!(
        encode_size_proof(root, size, &bad_outboard, tail),
        Err(SizeProofError::Verification { blob_size: size })
    );
    assert_eq!(
        encode_size_proof([7; 32], size, &outboard, tail),
        Err(SizeProofError::Verification { blob_size: size })
    );
}

/// The bao wire of the last chunk group, as a verified range export yields
/// it, builds the same proof as the outboard and the tail do.
#[test]
fn the_tail_wire_builds_the_same_proof() {
    for size in SIZES {
        let data = blob(size);
        let (root, outboard, tail) = origin_parts(&data);
        let tail_start = size_proof_tail_start(size);
        let aligned = crate::align_range_clamped(tail_start, size - tail_start, size).unwrap();
        // `encode_verified_range` leads with the 8-byte size header that an
        // import reads; the wire is what follows it.
        let wire = if size == 0 {
            Bytes::new()
        } else {
            crate::encode_verified_range(root, &aligned, tail, Bytes::from(outboard.clone()))
                .unwrap()
                .slice(8..)
        };
        let from_wire = size_proof_from_tail_wire(root, size, &wire).unwrap();
        let from_outboard = encode_size_proof(root, size, &outboard, tail).unwrap();
        assert_eq!(from_wire, from_outboard, "size {size}");
    }
}

#[test]
fn a_tampered_or_short_tail_wire_proves_nothing() {
    let size = 100_000;
    let data = blob(size);
    let (root, outboard, tail) = origin_parts(&data);
    let tail_start = size_proof_tail_start(size);
    let aligned = crate::align_range_clamped(tail_start, size - tail_start, size).unwrap();
    let wire = crate::encode_verified_range(root, &aligned, tail, Bytes::from(outboard))
        .unwrap()
        .slice(8..);
    let mut bad = wire.to_vec();
    bad[0] ^= 1;
    assert_eq!(
        size_proof_from_tail_wire(root, size, &bad),
        Err(SizeProofError::Verification { blob_size: size })
    );
    assert!(size_proof_from_tail_wire(root, size, &wire[..10]).is_err());
    assert!(size_proof_from_tail_wire(root, size + 1, &wire).is_err());
    let mut padded = vec![0u8; 64];
    padded.extend_from_slice(&wire);
    assert_eq!(
        size_proof_from_tail_wire(root, size, &padded),
        Err(SizeProofError::Verification { blob_size: size }),
        "an extra leading pair is refused"
    );
}

#[test]
fn the_tail_starts_at_the_last_chunk_group() {
    assert_eq!(size_proof_tail_start(0), 0);
    assert_eq!(size_proof_tail_start(1), 0);
    assert_eq!(size_proof_tail_start(16 * 1024), 0);
    assert_eq!(size_proof_tail_start(16 * 1024 + 1), 16 * 1024);
    assert_eq!(size_proof_tail_start(32 * 1024), 16 * 1024);
    assert_eq!(
        size_proof_tail_start(u64::MAX),
        (u64::MAX - 1) / 16384 * 16384
    );
}
