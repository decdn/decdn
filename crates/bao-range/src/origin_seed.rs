//! Filesystem-origin layout contract and outboard seeding.
//!
//! A filesystem cache origin stores each blob as a sharded data object
//! `{base}/{hex[0..2]}/{hex}` with a sibling pre-order bao outboard
//! `{base}/{hex[0..2]}/{hex}.obao4`
//! ([ADR 037 §Origin-tier pull-through](../../../adr/037-regional-proxy-warming.md)).
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

use crate::IROH_BLOCK_SIZE;

/// Sibling-key suffix for the published pre-order bao outboard (`{H}.obao4`),
/// per [ADR 037 §Origin-tier pull-through](../../../adr/037-regional-proxy-warming.md).
/// Shared across the filesystem / S3 reader adapters and the `origin import`
/// writer so an operator `aws s3 sync`-ing between backends keeps the same
/// object names.
pub const OBAO4_SUFFIX: &str = ".obao4";

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
}

/// Stream `reader` (exactly `size` bytes) once, computing the BLAKE3 root hash
/// and the pre-order bao outboard at [`crate::IROH_BLOCK_SIZE`] in a single
/// pass. `size` must equal the number of bytes `reader` yields — it frames the
/// bao tree, and a wrong value produces an outboard the reader rejects.
///
/// The source bytes are **not** buffered in memory; only the outboard (roughly
/// `1/256` of the blob at 16 KiB groups) is retained, so a multi-gigabyte blob
/// costs a few megabytes of RAM here rather than its whole length.
pub fn encode_outboard(reader: impl Read, size: u64) -> std::io::Result<EncodedOutboard> {
    let ob = PreOrderOutboard::<Vec<u8>>::create_sized(reader, size, IROH_BLOCK_SIZE)?;
    Ok(EncodedOutboard {
        hash_hex: ob.root.to_hex().to_string(),
        outboard: ob.data,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;
    use bao_tree::io::outboard::PreOrderMemOutboard;

    /// The streaming encoder must produce byte-identical root and outboard to
    /// the in-memory `PreOrderMemOutboard::create` the cache tests seed with, so
    /// a blob written by `origin import` is one `origin::fs` reads back verbatim.
    #[test]
    fn encode_outboard_matches_in_memory_encoder() {
        let payload = (0..200u32)
            .flat_map(|i| std::iter::repeat_n((i & 0xff) as u8, 1024))
            .collect::<Vec<_>>();
        let size = u64::try_from(payload.len()).unwrap();

        let streamed = encode_outboard(payload.as_slice(), size).unwrap();

        let mem = PreOrderMemOutboard::create(&payload, IROH_BLOCK_SIZE);
        assert_eq!(streamed.hash_hex, mem.root.to_hex().to_string());
        assert_eq!(streamed.outboard, mem.data);
    }

    /// The root hash equals the plain BLAKE3 hash of the content — the bao tree
    /// root is the content address, so the data object's file name matches what
    /// `bundle create` records for the same file.
    #[test]
    fn encode_outboard_root_is_content_blake3() {
        let payload = vec![7u8; 40 * 1024];
        let size = u64::try_from(payload.len()).unwrap();
        let got = encode_outboard(payload.as_slice(), size).unwrap();
        assert_eq!(
            got.hash_hex,
            bao_tree::blake3::hash(&payload).to_hex().to_string()
        );
    }

    /// A zero-length blob has an empty outboard and still yields a stable hash.
    #[test]
    fn encode_outboard_empty_blob() {
        let got = encode_outboard([].as_slice(), 0).unwrap();
        assert_eq!(got.hash_hex.len(), 64);
        assert!(got.outboard.is_empty());
    }

    #[test]
    fn shard_prefix_takes_first_two_hex_chars() {
        assert_eq!(shard_prefix("abcdef"), "ab");
        assert_eq!(shard_prefix("a"), "");
        assert_eq!(shard_prefix(""), "");
    }

    #[test]
    fn obao4_name_appends_suffix() {
        let ob = EncodedOutboard {
            hash_hex: "ab".repeat(32),
            outboard: Vec::new(),
        };
        assert_eq!(ob.shard(), "ab");
        assert_eq!(ob.obao4_name(), format!("{}{OBAO4_SUFFIX}", ob.hash_hex));
    }
}
