//! Size proofs: a blob's byte length, proven against its BLAKE3 root.
//!
//! BLAKE3 binds the length of its input into the tree, so only the true size
//! verifies a final chunk under the root. A size proof is the bao encoding of
//! that final chunk at chunk granularity ([`BlockSize::ZERO`]): one pair of
//! parent hashes (64 bytes) for each level on the path from the root to the
//! last chunk, then the chunk itself (at most 1 KiB).
//!
//! A proof depends on the hash alone. It names no node and carries no
//! signature, so any holder of one passes it on unchanged.
//!
//! [`verify_size_proof`] is the stock `bao-tree` decoder over the last chunk.
//! [`encode_size_proof`] builds the proof from what an origin holds: the
//! group-level `{hex}.obao4` outboard and the bytes of the last chunk group.
//! The outboard stores no hash below a chunk group, so the encoder computes
//! the pairs inside the last group from its bytes.

use bao_tree::blake3::hazmat::{HasherExt, left_subtree_len};
use bao_tree::io::outboard::EmptyOutboard;
use bao_tree::io::sync::decode_ranges;
use bao_tree::{BaoTree, BlockSize, ChunkNum, ChunkRanges, blake3};
use bytes::Bytes;
use positioned_io::WriteAt;

use crate::{CHUNK_GROUP_BYTES, IROH_BLOCK_SIZE};

/// Bytes in one BLAKE3 chunk, the leaf of a size proof.
const CHUNK_BYTES: u64 = 1024;

/// Bytes in one parent pair of a proof: two 32-byte hashes.
const PAIR_BYTES: usize = 64;

/// The longest size proof: a full last chunk and one parent pair for each
/// level of the deepest tree a `u64` length allows (`2^54` chunks).
pub const MAX_SIZE_PROOF_BYTES: usize = 1024 + PAIR_BYTES * 54;

/// Why [`encode_size_proof`] built no proof.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SizeProofError {
    /// The outboard is not the length that a pre-order outboard of a
    /// `blob_size`-byte blob has at [`IROH_BLOCK_SIZE`].
    #[error("outboard is {got} bytes, expected {expected} for a {blob_size}-byte blob")]
    OutboardSize {
        /// Length of a correct outboard.
        expected: u64,
        /// Length supplied.
        got: usize,
        /// Blob size the proof is for.
        blob_size: u64,
    },
    /// The tail is not the bytes from [`size_proof_tail_start`] to the end of
    /// the blob.
    #[error("tail is {got} bytes, expected {expected} for a {blob_size}-byte blob")]
    TailSize {
        /// Length of the last chunk group of the blob.
        expected: u64,
        /// Length supplied.
        got: usize,
        /// Blob size the proof is for.
        blob_size: u64,
    },
    /// The outboard and the tail do not prove `blob_size` under the root: one
    /// of the three is wrong.
    #[error("outboard and tail do not prove a {blob_size}-byte blob under the root")]
    Verification {
        /// Blob size the proof is for.
        blob_size: u64,
    },
}

/// The byte offset where the last chunk group of a `size`-byte blob starts.
/// [`encode_size_proof`] takes the bytes from this offset to the end of the
/// blob. It is `0` for an empty blob.
#[must_use]
pub const fn size_proof_tail_start(size: u64) -> u64 {
    size.saturating_sub(1) / CHUNK_GROUP_BYTES * CHUNK_GROUP_BYTES
}

/// The chunk range that selects the last chunk of a blob of any size.
fn last_chunk() -> ChunkRanges {
    ChunkRanges::from(ChunkNum(u64::MAX)..)
}

/// A [`WriteAt`] target that discards what the decoder writes. A size proof
/// is checked for what it proves, and the last chunk is not kept.
struct Discard;

impl WriteAt for Discard {
    fn write_at(&mut self, _pos: u64, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Whether `proof` proves that the blob with BLAKE3 hash `root` is `size`
/// bytes long.
///
/// `proof` is untrusted. A proof that is too long, that carries bytes past
/// its last chunk, or that does not verify under `root` for exactly `size`
/// bytes gives `false`. The check reads at most [`MAX_SIZE_PROOF_BYTES`] and
/// allocates nothing that grows with `size`.
#[must_use]
pub fn verify_size_proof(root: [u8; 32], size: u64, proof: &[u8]) -> bool {
    if proof.len() > MAX_SIZE_PROOF_BYTES {
        return false;
    }
    let outboard = EmptyOutboard {
        tree: BaoTree::new(size, BlockSize::ZERO),
        root: blake3::Hash::from(root),
    };
    let mut reader = proof;
    let decoded = decode_ranges(&mut reader, &last_chunk(), Discard, outboard).is_ok();
    // The decoder stops at the end of the last chunk. Bytes past it belong to
    // no proof of this size.
    decoded && reader.is_empty()
}

/// The chaining value of the subtree that covers `input`, which starts
/// `offset` bytes into the blob. `offset` is a multiple of [`CHUNK_BYTES`].
fn subtree_cv(offset: u64, input: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.set_input_offset(offset);
    hasher.update(input);
    hasher.finalize_non_root()
}

/// Build the size proof of a `size`-byte blob with BLAKE3 hash `root`.
///
/// `outboard` is the blob's pre-order outboard at [`IROH_BLOCK_SIZE`], the
/// bytes of its `{hex}.obao4` object. `tail` is the blob's bytes from
/// [`size_proof_tail_start`] to its end: the last chunk group, at most 16 KiB.
///
/// Neither input is trusted. The proof is verified against `root` before it
/// is returned, so a proof that this function returns always passes
/// [`verify_size_proof`].
///
/// # Errors
///
/// - [`SizeProofError::OutboardSize`] or [`SizeProofError::TailSize`] when an
///   input has the wrong length for `size`.
/// - [`SizeProofError::Verification`] when the inputs do not prove `size`
///   under `root`.
pub fn encode_size_proof(
    root: [u8; 32],
    size: u64,
    outboard: &[u8],
    tail: &[u8],
) -> Result<Bytes, SizeProofError> {
    let expected_outboard = BaoTree::new(size, IROH_BLOCK_SIZE).outboard_size();
    if u64::try_from(outboard.len()).ok() != Some(expected_outboard) {
        return Err(SizeProofError::OutboardSize {
            expected: expected_outboard,
            got: outboard.len(),
            blob_size: size,
        });
    }
    let tail_start = size_proof_tail_start(size);
    let expected_tail = size.saturating_sub(tail_start);
    if u64::try_from(tail.len()).ok() != Some(expected_tail) {
        return Err(SizeProofError::TailSize {
            expected: expected_tail,
            got: tail.len(),
            blob_size: size,
        });
    }
    let malformed = || SizeProofError::Verification { blob_size: size };
    // A slice of `tail` by blob offsets. Every offset here is inside the last
    // chunk group, so the differences fit a `usize`.
    let tail_at = |from: u64, to: u64| -> Result<&[u8], SizeProofError> {
        let from = usize::try_from(from.saturating_sub(tail_start)).map_err(|_| malformed())?;
        let to = usize::try_from(to.saturating_sub(tail_start)).map_err(|_| malformed())?;
        tail.get(from..to).ok_or_else(malformed)
    };

    let mut proof = Vec::with_capacity(MAX_SIZE_PROOF_BYTES.min(tail.len() + 16 * PAIR_BYTES));
    // Walk the right edge of the tree from the root to the last chunk. The
    // subtree in hand covers `[start, start + len)`.
    let mut start = 0u64;
    let mut len = size;
    // Offset in `outboard` of the pair of the subtree in hand.
    let mut pair_at = 0usize;
    while len > CHUNK_BYTES {
        let left = left_subtree_len(len);
        if len > CHUNK_GROUP_BYTES {
            // The subtree spans more than one chunk group, so the outboard
            // holds its pair. In pre-order the pairs of the left subtree
            // follow it: one for each of its chunk groups but the last.
            let pair = outboard
                .get(pair_at..pair_at.saturating_add(PAIR_BYTES))
                .ok_or_else(malformed)?;
            proof.extend_from_slice(pair);
            let left_pairs = (left / CHUNK_GROUP_BYTES).saturating_sub(1);
            let skip = usize::try_from(left_pairs)
                .ok()
                .and_then(|n| n.checked_mul(PAIR_BYTES))
                .and_then(|n| n.checked_add(PAIR_BYTES))
                .ok_or_else(malformed)?;
            pair_at = pair_at.checked_add(skip).ok_or_else(malformed)?;
        } else {
            // The subtree is inside the last chunk group. The outboard stores
            // no pair this low, so hash both children from the tail.
            let mid = start.saturating_add(left);
            let end = start.saturating_add(len);
            proof.extend_from_slice(&subtree_cv(start, tail_at(start, mid)?));
            proof.extend_from_slice(&subtree_cv(mid, tail_at(mid, end)?));
        }
        start = start.saturating_add(left);
        len = len.saturating_sub(left);
    }
    proof.extend_from_slice(tail_at(start, start.saturating_add(len))?);

    if !verify_size_proof(root, size, &proof) {
        return Err(malformed());
    }
    Ok(Bytes::from(proof))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests
mod tests;
