//! Streaming, header-less whole-blob bao encoder (issue #1130's stream-while-
//! store seam): produces the same interleaved proof+data bytes as
//! [`crate::encode_verified_range`] over `ChunkRanges::all()`, but reads a
//! *sequential* [`std::io::Read`] source instead of requiring the whole blob
//! already buffered in memory, and omits the 8-byte LE size header that helper
//! prepends (the tee this feeds already carries the size out-of-band, in the
//! signed `StreamResponse`).
//!
//! Stays sync and leaf-pure — no `tokio` in `decdn-bao-range`. A later cache-
//! crate task drives this function on a `spawn_blocking` thread, streaming an
//! http/s3 origin miss to a paying client while teeing the same bytes into the
//! store, rather than buffering the whole blob before either side can start.

use std::cell::RefCell;
use std::io::{self, Read, Write};

use bao_tree::io::outboard::PreOrderMemOutboard;
use bao_tree::io::sync::encode_ranges_validated;
use bao_tree::{BaoTree, ChunkRanges};
use bytes::Bytes;
use positioned_io::ReadAt;

use crate::{IROH_BLOCK_SIZE, RangeVerifyError};

/// Adapts a sequential [`std::io::Read`] into the `&self`-based
/// `positioned_io::ReadAt` [`encode_ranges_validated`] requires. Bao's
/// full-range (`ChunkRanges::all()`) walk reads data leaves strictly
/// front-to-back exactly once, so a monotonic cursor suffices; the `RefCell`
/// gives that cursor interior mutability through `&self` (the whole encode
/// runs on one thread, so this stays single-threaded and un-shared).
///
/// A backward seek (`pos < cursor`) is rejected rather than silently
/// re-reading — it cannot happen for `ChunkRanges::all()` today, so tripping
/// it signals a future `bao-tree` walk-order change that needs a real fix
/// here, not a request that could have been serviced.
struct SeqReadAt<R> {
    state: RefCell<SeqState<R>>,
}

struct SeqState<R> {
    reader: R,
    cursor: u64,
}

impl<R: Read> SeqReadAt<R> {
    const fn new(reader: R) -> Self {
        Self {
            state: RefCell::new(SeqState { reader, cursor: 0 }),
        }
    }
}

impl<R: Read> ReadAt for SeqReadAt<R> {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> io::Result<usize> {
        let mut state = self.state.borrow_mut();
        if pos < state.cursor {
            return Err(io::Error::other(format!(
                "SeqReadAt: backward seek to {pos} from cursor {}; the sequential adapter \
                 only supports monotonic reads (ChunkRanges::all() must read front-to-back)",
                state.cursor
            )));
        }
        if pos > state.cursor {
            let mut skip = pos - state.cursor;
            let mut sink = [0u8; 4096];
            while skip > 0 {
                let want = usize::try_from(skip).unwrap_or(sink.len()).min(sink.len());
                let Some(chunk) = sink.get_mut(..want) else {
                    break;
                };
                let n = state.reader.read(chunk)?;
                if n == 0 {
                    // EOF while draining: nothing left to fill `buf` with either,
                    // so stop here and let the fill loop below report 0 bytes.
                    break;
                }
                skip = skip.saturating_sub(n as u64);
                state.cursor = state.cursor.saturating_add(n as u64);
            }
        }
        let mut filled = 0usize;
        while filled < buf.len() {
            let Some(dst) = buf.get_mut(filled..) else {
                break;
            };
            let n = state.reader.read(dst)?;
            if n == 0 {
                break; // Genuine EOF: a short read here is correct.
            }
            filled += n;
            state.cursor = state.cursor.saturating_add(n as u64);
        }
        Ok(filled)
    }
}

/// Writes the header-less bao interleaved encoding of the full `blob_size`-byte
/// blob (`ChunkRanges::all()`) to `out`, verifying `data` against `root` via the
/// untrusted pre-order `outboard` while streaming — no 8-byte size prefix
/// (unlike [`crate::encode_verified_range`]'s combined format), because the
/// caller already frames the size out-of-band.
///
/// `data` is read sequentially exactly once, front-to-back; it need not be
/// seekable or fully buffered up front, which is what lets a serving node
/// stream a cache-miss origin fetch to a paying client while teeing the same
/// bytes into the store (#1130), instead of buffering the whole blob first.
///
/// # Errors
///
/// - [`RangeVerifyError::OutboardSize`] if `outboard` is not the exact length a
///   `blob_size`-byte blob's pre-order outboard must be.
/// - [`RangeVerifyError::Verification`] if `data`/`outboard` do not verify
///   against `root` (tampered bytes, tampered/foreign outboard, or wrong root).
pub fn encode_whole_blob_headerless<R: Read>(
    root: [u8; 32],
    blob_size: u64,
    outboard: Bytes,
    data: R,
    out: &mut impl Write,
) -> Result<(), RangeVerifyError> {
    let tree = BaoTree::new(blob_size, IROH_BLOCK_SIZE);
    let expected = usize::try_from(tree.outboard_size()).unwrap_or(usize::MAX);
    if outboard.len() != expected {
        return Err(RangeVerifyError::OutboardSize {
            expected,
            got: outboard.len(),
            blob_size,
        });
    }
    let ob = PreOrderMemOutboard {
        root: bao_tree::blake3::Hash::from(root),
        tree,
        data: outboard,
    };
    let reader = SeqReadAt::new(data);
    encode_ranges_validated(&reader, &ob, ChunkRanges::all().as_ref(), out)
        .map_err(|source| RangeVerifyError::Verification { source })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;
    use crate::{CHUNK_GROUP_BYTES, align_range, encode_verified_range};

    // A blob spanning several chunk groups plus a partial final group, so the
    // tree has real interior nodes (matches the crate-root tests' rationale).
    const BLOB_SIZE: u64 = 5 * CHUNK_GROUP_BYTES + 123;

    fn blob() -> Vec<u8> {
        // Non-constant bytes so a byte-shuffling bug can't hide behind an
        // all-zero/all-same blob.
        (0..BLOB_SIZE).map(|i| (i % 251) as u8).collect()
    }

    #[test]
    fn streamed_headerless_matches_reference() {
        let data = blob();
        let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let root: [u8; 32] = *ob.root.as_bytes();
        let outboard = Bytes::from(ob.data.clone());

        let aligned = align_range(0, 0, BLOB_SIZE).expect("align whole blob");
        let reference = encode_verified_range(root, &aligned, &data, outboard.clone())
            .expect("reference encode");
        let reference_headerless = reference.get(8..).expect("reference has 8-byte header");

        let mut buf = Vec::new();
        encode_whole_blob_headerless(root, BLOB_SIZE, outboard, &data[..], &mut buf)
            .expect("streaming encode");

        assert_eq!(buf, reference_headerless);
    }

    #[test]
    fn corrupt_outboard_fails() {
        let data = blob();
        let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let root: [u8; 32] = *ob.root.as_bytes();
        let mut corrupt = ob.data.clone();
        if let Some(byte) = corrupt.get_mut(0) {
            *byte ^= 0xff;
        }
        let outboard = Bytes::from(corrupt);

        let mut buf = Vec::new();
        let err = encode_whole_blob_headerless(root, BLOB_SIZE, outboard, &data[..], &mut buf)
            .expect_err("corrupt outboard must fail verification");
        assert!(matches!(err, RangeVerifyError::Verification { .. }));
    }

    #[test]
    fn wrong_root_fails() {
        let data = blob();
        let ob = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let outboard = Bytes::from(ob.data.clone());
        let wrong_root = [0xABu8; 32];

        let mut buf = Vec::new();
        let err =
            encode_whole_blob_headerless(wrong_root, BLOB_SIZE, outboard, &data[..], &mut buf)
                .expect_err("wrong root must fail verification");
        assert!(matches!(err, RangeVerifyError::Verification { .. }));
    }
}
