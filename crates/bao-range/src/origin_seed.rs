//! Filesystem-origin layout contract and outboard seeding.
//!
//! A filesystem cache origin stores each blob as a sharded data object
//! `{base}/{hex[0..2]}/{hex}` with a sibling pre-order bao outboard
//! `{base}/{hex[0..2]}/{hex}.obao4`
//! ([ADR 037 §Origin-tier pull-through](https://github.com/decdn/decdn/blob/main/adr/037-regional-proxy-warming.md)).
//! The reader side lives in `decdn-cache` (`origin::fs` / `origin::s3`); the
//! writer side is `decdn origin import`. Both derive the sibling key and the
//! outboard bytes from this module, so a blob written here is one the daemon
//! reads back verbatim.
//!
//! The outboard is a `bao-tree` pre-order outboard at [`crate::IROH_BLOCK_SIZE`]
//! (16 KiB chunk groups). A stock `bao encode --outboard` uses a different chunk
//! group size and produces a file the daemon rejects, so the outboard **cannot**
//! be produced by generic shell tooling — it must come from this shared encoder.

use std::io::Read;

use bao_tree::io::outboard::PreOrderOutboard;
use bao_tree::io::sync::CreateOutboard;
use bytes::Bytes;

use crate::IROH_BLOCK_SIZE;
use crate::size_proof::{encode_size_proof, size_proof_tail_start};

/// Sibling-key suffix for the published pre-order bao outboard (`{H}.obao4`),
/// per [ADR 037 §Origin-tier pull-through](https://github.com/decdn/decdn/blob/main/adr/037-regional-proxy-warming.md).
/// Shared across the filesystem / S3 reader adapters and the `origin import`
/// writer so an operator `aws s3 sync`-ing between backends keeps the same
/// object names.
pub const OBAO4_SUFFIX: &str = ".obao4";

/// Sibling-key suffix for the published size proof (`{H}.sizeproof`): the
/// bytes of [`crate::encode_size_proof`] for the blob, which a node serves to
/// prove the blob's size (ADR 005 § A node proves the blob size). It sits
/// beside the `{H}.obao4` outboard under the same shard prefix.
pub const SIZE_PROOF_SUFFIX: &str = ".sizeproof";

/// The two-character shard prefix for a blob's lowercase-hex address:
/// `{base}/{hex[0..2]}/{hex}`. The first two hex chars shard the directory so a
/// content set with millions of entries doesn't land in a single dirent list.
///
/// Panic-free: a `hex` shorter than two chars (never produced by a real BLAKE3
/// address, but the workspace's anti-indexing lint forbids `&hex[..2]`) yields
/// the empty prefix rather than panicking.
#[must_use]
pub fn shard_prefix(hex: &str) -> &str {
    hex.get(..2).unwrap_or("")
}

/// A blob's content address plus its pre-order bao outboard, ready to write to a
/// filesystem origin as the sharded data object and its `{hex}.obao4` sibling.
#[derive(Debug, Clone)]
pub struct EncodedOutboard {
    /// The BLAKE3 root hash as 64 lowercase hex characters — the data object's
    /// file name and the shard prefix source.
    pub hash_hex: String,
    /// The pre-order bao outboard bytes, byte-identical to what the cache
    /// reader (`origin::fs::fetch_outboard`) expects at the `{hex}.obao4` key.
    pub outboard: Vec<u8>,
    /// The size proof of the blob, the bytes of its `{hex}.sizeproof`
    /// sibling ([`SIZE_PROOF_SUFFIX`]).
    pub size_proof: Bytes,
}

impl EncodedOutboard {
    /// The two-character shard prefix for this blob's address.
    #[must_use]
    pub fn shard(&self) -> &str {
        shard_prefix(&self.hash_hex)
    }

    /// The `{hex}.obao4` sibling file name for this blob.
    #[must_use]
    pub fn obao4_name(&self) -> String {
        format!("{}{OBAO4_SUFFIX}", self.hash_hex)
    }

    /// The `{hex}.sizeproof` sibling file name for this blob.
    #[must_use]
    pub fn size_proof_name(&self) -> String {
        format!("{}{SIZE_PROOF_SUFFIX}", self.hash_hex)
    }
}

/// A reader that passes bytes through and keeps the ones at or past `from`:
/// the last chunk group, which the size proof encoder needs.
struct TailCapture<R> {
    inner: R,
    pos: u64,
    from: u64,
    tail: Vec<u8>,
}

impl<R: Read> Read for TailCapture<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        let read = buf.get(..n).unwrap_or_default();
        let end = self.pos.saturating_add(read.len() as u64);
        if end > self.from {
            // `from - pos` is below `n` here, so it fits a `usize`.
            let skip = usize::try_from(self.from.saturating_sub(self.pos)).unwrap_or(usize::MAX);
            self.tail
                .extend_from_slice(read.get(skip..).unwrap_or_default());
        }
        self.pos = end;
        Ok(n)
    }
}

/// Stream `reader` (exactly `size` bytes) once, computing the BLAKE3 root hash,
/// the pre-order bao outboard at [`crate::IROH_BLOCK_SIZE`] and the size proof
/// in a single pass. `size` must equal the number of bytes `reader` yields — it
/// frames the bao tree, and a wrong value produces an outboard the reader
/// rejects.
///
/// The source bytes are **not** buffered in memory; only the outboard (roughly
/// `1/256` of the blob at 16 KiB groups) and the last chunk group are retained,
/// so a multi-gigabyte blob costs a few megabytes of RAM here rather than its
/// whole length.
///
/// # Errors
///
/// An I/O error from `reader`, or [`std::io::ErrorKind::InvalidData`] when
/// `reader` yields a number of bytes other than `size`.
pub fn encode_outboard(reader: impl Read, size: u64) -> std::io::Result<EncodedOutboard> {
    let mut capture = TailCapture {
        inner: reader,
        pos: 0,
        from: size_proof_tail_start(size),
        tail: Vec::new(),
    };
    let ob = PreOrderOutboard::<Vec<u8>>::create_sized(&mut capture, size, IROH_BLOCK_SIZE)?;
    let root: [u8; 32] = ob.root.into();
    let size_proof = encode_size_proof(root, size, &ob.data, &capture.tail)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(EncodedOutboard {
        hash_hex: ob.root.to_hex().to_string(),
        outboard: ob.data,
        size_proof,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
