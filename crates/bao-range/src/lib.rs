//! Iroh-blobs-free bao verified-range helpers (#823, #915), shared by
//! `decdn-cache` (origin range import + client serve) and `decdn-client-pull`
//! (client receive). Depends only on `bao-tree`, so linking it from the CLI's
//! pull path keeps the iroh-blobs-free invariant (#578).
//!
//! [ADR 037 §Origin-tier pull-through](../../../adr/037-regional-proxy-warming.md),
//! [ADR 038 §Wire format](../../../adr/038-bao-verified-range-streaming.md).
//!
//! An opaque origin backend (S3/R2/B2/HTTP/fs) serves raw bytes by hash with no
//! bao tree, so a node that wants only a byte range `[a, b)` of a large blob
//! historically had to pull the **whole** blob to verify any of it against the
//! content address. When the origin also publishes the blob's pre-order bao
//! outboard at the sibling key `{H}.obao4`, this module verifies the fetched
//! range against the root `H` using that (untrusted) outboard and produces the
//! bao interleaved encoding `iroh-blobs`' `import_bao_bytes` consumes — so the
//! node imports a verified **partial** blob without whole-blob origin egress.
//!
//! The outboard is **self-validating against `H`**: a tampered range, a tampered
//! outboard, or a wrong wanted-root all fail, because every leaf must hash to its
//! anchored parent and the spine must combine to the `H` the node already wanted
//! ([`encode_ranges_validated`] checks `ParentHashMismatch`/`LeafHashMismatch`).
//! There is no trusted-origin assumption — the origin stays a dumb byte store.

use bao_tree::io::outboard::PreOrderMemOutboard;
use bao_tree::io::sync::encode_ranges_validated;
use bao_tree::{BaoTree, BlockSize, ChunkNum, ChunkRanges};
use bytes::Bytes;
use positioned_io::ReadAt;

/// `iroh-blobs`' canonical on-disk block size (16 KiB chunk groups,
/// `from_chunk_log(4)`). Declared from the `bao-tree` primitive rather than
/// re-exported from `iroh-blobs`, because this leaf crate must stay
/// iroh-blobs-free (#578) so the CLI's pull path can link it. The value is
/// **identical by construction** to `iroh_blobs::store::IROH_BLOCK_SIZE` (which
/// is also `BlockSize::from_chunk_log(4)`); `decdn-cache` carries a lock-step
/// test asserting the two stay equal, so a divergence across an iroh-blobs bump
/// — which ADR 038 treats as a protocol-contract change — fails the build there.
pub const IROH_BLOCK_SIZE: BlockSize = BlockSize::from_chunk_log(4);

/// Bytes per chunk group, derived from [`IROH_BLOCK_SIZE`] (`2^chunk_log` 1 KiB
/// chunks) — the granularity an origin range must snap to, because a bao proof
/// anchors whole chunk groups to the root.
const CHUNK_GROUP_BYTES: u64 = 1u64 << (IROH_BLOCK_SIZE.chunk_log() + 10);

/// Exact byte length of the **header-less** bao interleaved encoding a serving
/// node emits for `chunk_ranges` of a `total_bytes`-byte blob — the sum of every
/// proof node (64 bytes each) and data leaf (≤ chunk-group bytes) in response
/// (pre-order) order ([ADR 038 §Wire format](../../../adr/038-bao-verified-range-streaming.md)).
///
/// This is the byte count that travels on `cdn/client/v1`: the wire carries the
/// response-format stream (no 8-byte size header — `total_bytes` is already the
/// signed `StreamResponse` field), so this excludes the header. It is computed
/// from the same [`BaoTree`] + [`ChunkRanges`] the serve-side encoder walks, so
/// it equals the emitted length byte-for-byte. The receiver uses it as the
/// voucher-cadence / overrun / short-delivery bound, because paid bytes are wire
/// bytes — content **plus** proof (ADR 038 §Payment metering) — not content
/// bytes. `chunk_ranges` should be the [`AlignedRange::chunk_ranges`] both sides
/// derive from [`align_range`], keeping encoder and decoder in lock-step.
#[must_use]
pub fn bao_encoded_size(total_bytes: u64, chunk_ranges: &ChunkRanges) -> u64 {
    // Walk exactly the node set the serve-side encoder emits. `encode_ranges` /
    // `encode_ranges_validated` iterate `ranges_pre_order_chunks_iter_ref(ranges,
    // 0)` over the real `IROH_BLOCK_SIZE` tree (sync.rs), so the same walk yields
    // the same Parent (64 B) + Leaf (data) sequence — and thus the same byte
    // count — that travels on the wire. (The `ResponseIterRef` convenience walks a
    // block-size-zero tree and descends to 1 KiB chunks, over-counting parents on
    // partial groups, so it must NOT be used here.) `align_range`'s ranges are
    // already clamped to the blob, so the encoder's `truncate_ranges` step is a
    // no-op and is not reachable here (its module is private upstream).
    let tree = BaoTree::new(total_bytes, IROH_BLOCK_SIZE);
    tree.ranges_pre_order_chunks_iter_ref(chunk_ranges.as_ref(), 0)
        .map(|chunk| u64::try_from(chunk.without_ranges().size()).unwrap_or(u64::MAX))
        .fold(0u64, u64::saturating_add)
}

/// Errors from preparing a verified origin range pull.
#[derive(Debug, thiserror::Error)]
pub enum RangeVerifyError {
    /// The requested `[byte_offset, byte_offset + byte_len)` runs past the blob,
    /// or `byte_offset + byte_len` overflows. ADR 005 requires the node to reject
    /// such a request rather than silently clamp it.
    #[error("range [{offset}, +{len}) is out of bounds for a {blob_size}-byte blob")]
    RangeOutOfBounds {
        /// Requested start offset.
        offset: u64,
        /// Requested length as supplied (`0` denotes "to end"), reported verbatim.
        len: u64,
        /// Known blob size from the signed `total_bytes` / manifest entry.
        blob_size: u64,
    },
    /// The supplied outboard is not the length a `blob_size`-byte blob's
    /// pre-order outboard must be — the origin served a malformed `{H}.obao4`.
    #[error("outboard is {got} bytes, expected {expected} for a {blob_size}-byte blob")]
    OutboardSize {
        /// Length a correct pre-order outboard would have.
        expected: usize,
        /// Length actually supplied.
        got: usize,
        /// Known blob size.
        blob_size: u64,
    },
    /// The fetched range or the outboard did not verify against the wanted root
    /// `H` — a tampered range, a tampered/foreign outboard, or a wrong root.
    /// Carries the bao codec's typed cause so a caller can distinguish an
    /// integrity failure (`ParentHashMismatch` / `LeafHashMismatch` — do not
    /// retry) from a truncated origin response (`Io` short read — retryable).
    #[error("range failed bao verification against the content root")]
    Verification {
        /// Underlying bao codec rejection.
        #[source]
        source: bao_tree::io::EncodeError,
    },
}

/// The chunk-group-aligned byte span a node must fetch from origin to serve a
/// requested `[byte_offset, byte_offset + byte_len)`, plus the chunk ranges to
/// import. A bao proof covers whole 16 KiB groups, so the fetch widens to the
/// enclosing group boundaries (the extra bytes are paid origin egress but never
/// re-served as client bytes — the request is still scoped, not whole-blob).
/// Fields are private and the only constructor is [`align_range`], so holding an
/// `AlignedRange` guarantees its invariants: `fetch_start <= fetch_end`, both
/// group-aligned (or `fetch_end == blob_size` for the final partial group), and
/// `chunk_ranges` exactly covers `[fetch_start, fetch_end)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlignedRange {
    fetch_start: u64,
    fetch_end: u64,
    chunk_ranges: ChunkRanges,
}

impl AlignedRange {
    /// First byte to fetch (the requested offset floored to a group boundary).
    #[must_use]
    pub const fn fetch_start(&self) -> u64 {
        self.fetch_start
    }

    /// One past the last byte to fetch (requested end ceiled to a group
    /// boundary, then clamped to the blob size for the final partial group).
    #[must_use]
    pub const fn fetch_end(&self) -> u64 {
        self.fetch_end
    }

    /// Chunk ranges (1 KiB `ChunkNum` units) the fetched bytes cover — passed to
    /// both [`encode_verified_range`] and `import_bao_bytes`.
    #[must_use]
    pub const fn chunk_ranges(&self) -> &ChunkRanges {
        &self.chunk_ranges
    }

    /// Number of bytes to fetch from origin for this range. Non-underflowing by
    /// construction — `align_range` guarantees `fetch_end >= fetch_start`.
    #[must_use]
    pub const fn fetch_len(&self) -> u64 {
        self.fetch_end - self.fetch_start
    }
}

/// Compute the chunk-group-aligned fetch span for a `[byte_offset, +byte_len)`
/// request against a `blob_size`-byte blob. `byte_len == 0` means "to end".
///
/// # Errors
///
/// [`RangeVerifyError::RangeOutOfBounds`] if the offset is past the blob end, or
/// the explicit end overflows / exceeds the blob size.
pub fn align_range(
    byte_offset: u64,
    byte_len: u64,
    blob_size: u64,
) -> Result<AlignedRange, RangeVerifyError> {
    let oob = || RangeVerifyError::RangeOutOfBounds {
        offset: byte_offset,
        len: byte_len,
        blob_size,
    };
    // An offset at or past the end has nothing to serve (the empty blob has no
    // byte 0 either).
    if byte_offset >= blob_size {
        return Err(oob());
    }
    let end = if byte_len == 0 {
        blob_size
    } else {
        let e = byte_offset.checked_add(byte_len).ok_or_else(oob)?;
        if e > blob_size {
            return Err(oob());
        }
        e
    };
    let fetch_start = (byte_offset / CHUNK_GROUP_BYTES) * CHUNK_GROUP_BYTES;
    let fetch_end = end
        .div_ceil(CHUNK_GROUP_BYTES)
        .saturating_mul(CHUNK_GROUP_BYTES)
        .min(blob_size);
    let chunk_ranges =
        ChunkRanges::from(ChunkNum::full_chunks(fetch_start)..ChunkNum::chunks(fetch_end));
    Ok(AlignedRange {
        fetch_start,
        fetch_end,
        chunk_ranges,
    })
}

/// Maps absolute blob offsets onto a buffer that holds only `[base, base+len)` —
/// the bytes a ranged origin fetch returned. Reads outside the held span report
/// EOF, which surfaces as a verification short-read rather than reading zeros.
struct OffsetReadAt<'a> {
    base: u64,
    data: &'a [u8],
}

impl ReadAt for OffsetReadAt<'_> {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        let Some(rel) = pos.checked_sub(self.base) else {
            return Ok(0);
        };
        let Ok(rel) = usize::try_from(rel) else {
            return Ok(0);
        };
        let Some(src) = self.data.get(rel..) else {
            return Ok(0);
        };
        let n = src.len().min(buf.len());
        let (Some(dst), Some(s)) = (buf.get_mut(..n), src.get(..n)) else {
            return Ok(0);
        };
        dst.copy_from_slice(s);
        Ok(n)
    }
}

/// Verify `range_data` (covering `aligned.fetch_start..aligned.fetch_end`) and
/// the supplied pre-order `outboard` against the wanted root `root`, returning
/// the bao interleaved encoding for `aligned.chunk_ranges` — ready for
/// `Blobs::import_bao_bytes(root, aligned.chunk_ranges, _)`.
///
/// The outboard is untrusted: verification anchors every chunk group to `root`,
/// so a tampered range, a foreign/tampered outboard, or a wrong `root` all fail
/// with [`RangeVerifyError::Verification`] — there is no trusted-origin path.
///
/// # Errors
///
/// - [`RangeVerifyError::OutboardSize`] if `outboard` is the wrong length for a
///   `blob_size`-byte blob.
/// - [`RangeVerifyError::Verification`] if the range/outboard do not verify
///   against `root`.
pub fn encode_verified_range(
    root: [u8; 32],
    blob_size: u64,
    aligned: &AlignedRange,
    range_data: &[u8],
    outboard: Bytes,
) -> Result<Bytes, RangeVerifyError> {
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
    let reader = OffsetReadAt {
        base: aligned.fetch_start,
        data: range_data,
    };
    // bao "combined" encoding: an 8-byte little-endian size header precedes the
    // interleaved proof+data stream. `iroh-blobs`' `import_bao_reader` reads the
    // header first to frame the tree, then feeds the rest to its decoder, so the
    // helper owns producing the full importable byte string. Pre-size the buffer
    // to the header + fetched range (plus the ~0.4% proof overhead grows once);
    // `fetch_len` always fits `usize` when `range_data` does, so a failed
    // conversion just degrades to the default growth.
    let cap = usize::try_from(aligned.fetch_len())
        .unwrap_or(0)
        .saturating_add(8);
    let mut encoded = Vec::with_capacity(cap);
    encoded.extend_from_slice(&blob_size.to_le_bytes());
    encode_ranges_validated(&reader, &ob, aligned.chunk_ranges.as_ref(), &mut encoded)
        .map_err(|source| RangeVerifyError::Verification { source })?;
    Ok(Bytes::from(encoded))
}
