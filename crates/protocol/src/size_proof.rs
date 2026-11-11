//! The size proof a node attaches to every size it states (ADR 005 § A node
//! proves the blob size).
//!
//! A [`SizeProof`] is the bao encoding of the final BLAKE3 chunk of a blob at
//! chunk granularity: one 64-byte pair of parent hashes for each level on the
//! path from the root to the last chunk, then that chunk (at most 1 KiB).
//! BLAKE3 binds the length of its input into the tree, so a proof verifies
//! under the content hash for the true size alone.
//!
//! This crate holds no hashing, so it carries the proof as opaque bytes and
//! bounds its length at decode. `decdn_bao_range::verify_size_proof` checks
//! it. A proof names no node and carries no signature: any holder passes it on
//! unchanged.

use serde::{Deserialize, Serialize};

/// The longest size proof: a full last chunk (1 KiB) and one 64-byte parent
/// pair for each level of the deepest tree a `u64` length allows (`2^54`
/// chunks). A wire proof longer than this fails decode.
pub const MAX_SIZE_PROOF_BYTES: usize = 1024 + 64 * 54;

/// A blob size proof as it travels on the wire: at most
/// [`MAX_SIZE_PROOF_BYTES`] bytes, checked by nothing until the receiver
/// verifies it against the content hash and the stated size.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct SizeProof {
    #[serde(deserialize_with = "deserialize_bounded_proof")]
    bytes: Vec<u8>,
}

/// Why [`SizeProof::new`] refused its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("size proof is {len} bytes, more than MAX_SIZE_PROOF_BYTES ({MAX_SIZE_PROOF_BYTES})")]
pub struct SizeProofTooLong {
    /// Length offered.
    pub len: usize,
}

impl SizeProof {
    /// Wrap proof bytes for the wire.
    ///
    /// # Errors
    ///
    /// [`SizeProofTooLong`] when `bytes` is longer than
    /// [`MAX_SIZE_PROOF_BYTES`]. No valid proof is.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, SizeProofTooLong> {
        let bytes = bytes.into();
        if bytes.len() > MAX_SIZE_PROOF_BYTES {
            return Err(SizeProofTooLong { len: bytes.len() });
        }
        Ok(Self { bytes })
    }

    /// The proof bytes, for `decdn_bao_range::verify_size_proof`.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Bounds a wire proof to [`MAX_SIZE_PROOF_BYTES`] at decode. The enclosing
/// frame ceiling and serde's cautious capacity hint bound the allocation
/// before this check, so the wire length prefix never sizes a buffer.
fn deserialize_bounded_proof<'de, D>(d: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let bytes = Vec::<u8>::deserialize(d)?;
    if bytes.len() > MAX_SIZE_PROOF_BYTES {
        return Err(serde::de::Error::custom(SizeProofTooLong { len: bytes.len() }));
    }
    Ok(bytes)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
