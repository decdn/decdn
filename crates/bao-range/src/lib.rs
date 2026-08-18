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

use std::io::Write;

use bao_tree::io::outboard::PreOrderMemOutboard;
use bao_tree::io::sync::encode_ranges_validated;
use bao_tree::{BaoTree, BlockSize, ChunkNum, ChunkRanges};
use bytes::Bytes;
use positioned_io::ReadAt;

#[cfg(feature = "test-util")]
pub mod conformance;
pub mod ranged_store;
pub mod streaming;
pub use ranged_store::{RangedFuture, RangedStore, RangedStoreError};

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
///
/// Public so a caller that must snap an offset to a group boundary WITHOUT
/// knowing the blob's size can do so from the shared constant rather than
/// re-deriving it (the resumable client fetch, #1120: it picks its resume offset
/// before the signed response reveals `total_bytes`). Prefer [`align_range`] when
/// the size is known — it also bound-checks.
pub const CHUNK_GROUP_BYTES: u64 = 1u64 << (IROH_BLOCK_SIZE.chunk_log() + 10);

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
    // A 0-byte blob (#1054) has no proof and no data — zero wire bytes for ANY
    // requested range. An empty range set likewise encodes to nothing. Short-
    // circuit both before walking the tree: `bao-tree`'s pre-order chunk iterator
    // `debug_assert!`s `!ranges.is_empty()` (and would otherwise mis-walk an empty
    // set in release), and a 0-byte `BaoTree` walked over a non-empty range like
    // `ChunkRanges::all()` is a degenerate the callers never intend to bill for.
    if total_bytes == 0 || chunk_ranges.is_empty() {
        return 0;
    }
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
    /// `range_data` is not exactly the aligned fetch window's length — the origin
    /// served a 206 whose body is shorter or longer than `[fetch_start, fetch_end)`
    /// (e.g. a corrupt/extra tail). Rejected rather than silently ignoring the
    /// surplus (or short-reading the deficit) inside the offset-mapped reader.
    #[error(
        "range_data is {got} bytes, expected {expected} for the aligned fetch window \
         of a {blob_size}-byte blob"
    )]
    RangeDataSize {
        /// Aligned fetch-window length the range body must have (`fetch_len`).
        expected: u64,
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
    blob_size: u64,
}

impl AlignedRange {
    /// Whole-blob size this range was aligned against — the signed `total_bytes`
    /// [`align_range`] was given. Held so [`wire_len`](Self::wire_len) and
    /// [`encode_verified_range`] cannot be handed a `blob_size` that disagrees
    /// with the `chunk_ranges`.
    #[must_use]
    pub const fn blob_size(&self) -> u64 {
        self.blob_size
    }

    /// Exact header-less bao **wire** byte count for this range (content **plus**
    /// interleaved proof, ADR 038) — the paid quantity, and the receiver's
    /// voucher-cadence / overrun / short-delivery bound.
    ///
    /// This is [`bao_encoded_size`] applied to this range's own `blob_size` and
    /// `chunk_ranges`, so the two can never drift: a mismatched `(total, ranges)`
    /// pair would otherwise silently yield a plausible-but-wrong payment bound
    /// with no runtime backstop — the one misuse seam the crypto never sees.
    #[must_use]
    pub fn wire_len(&self) -> u64 {
        bao_encoded_size(self.blob_size, &self.chunk_ranges)
    }

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
/// The empty (0-byte) blob is addressable only as the whole blob — `(0, 0)` —
/// and aligns to an empty [`AlignedRange`] (`fetch_start == fetch_end == 0`,
/// empty `chunk_ranges`, zero [`wire_len`](AlignedRange::wire_len)). This
/// preserves the pre-bao empty-bytes delivery on the bao path (#1054); the empty
/// stream is still proven against the empty root `blake3::hash(&[])` by the
/// receiver, never accepted for an arbitrary root.
///
/// # Errors
///
/// [`RangeVerifyError::RangeOutOfBounds`] if the offset is past the blob end, or
/// the explicit end overflows / exceeds the blob size. For a 0-byte blob, any
/// positive offset or explicit positive length is out of bounds.
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
    // The empty blob is served only as its whole (empty) self: offset 0, "to
    // end". The construction below then yields an empty range. A non-empty blob
    // still rejects any offset at or past the end (reject, never clamp — ADR 005).
    if blob_size == 0 {
        if byte_offset != 0 || byte_len != 0 {
            return Err(oob());
        }
    } else if byte_offset >= blob_size {
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
        blob_size,
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
/// - [`RangeVerifyError::RangeDataSize`] if `range_data` is not exactly
///   `aligned.fetch_len()` bytes (a corrupt/extra or truncated origin 206 body).
/// - [`RangeVerifyError::OutboardSize`] if `outboard` is the wrong length for the
///   aligned range's blob size (`aligned.blob_size()`).
/// - [`RangeVerifyError::Verification`] if the range/outboard do not verify
///   against `root`.
pub fn encode_verified_range(
    root: [u8; 32],
    aligned: &AlignedRange,
    range_data: &[u8],
    outboard: Bytes,
) -> Result<Bytes, RangeVerifyError> {
    let blob_size = aligned.blob_size();
    // Guard the one seam the crypto never sees: the offset-mapped reader below only
    // exposes `[fetch_start, fetch_end)`, so a `range_data` longer than the aligned
    // fetch window would have its tail silently dropped (and a shorter one would
    // surface only as an opaque verification short-read). Reject either up front so
    // a malformed origin 206 is a typed error, not a silent truncation.
    if u64::try_from(range_data.len()).ok() != Some(aligned.fetch_len()) {
        return Err(RangeVerifyError::RangeDataSize {
            expected: aligned.fetch_len(),
            got: range_data.len(),
            blob_size,
        });
    }
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

/// Write the **header-less** bao interleaved encoding for `aligned.chunk_ranges`
/// to `out`, validating `data` against `root` via the untrusted pre-order
/// `outboard` as it streams. Unlike [`encode_verified_range`] there is no 8-byte
/// LE size prefix (the size travels out-of-band in the signed `StreamResponse`)
/// and no returned `Bytes` — bytes go straight to the sink in O(chunk-group) RAM.
///
/// `data` is a seekable [`ReadAt`] over the **whole blob** (e.g. `std::fs::File`):
/// the encoder reads at absolute blob offsets, so a ranged serve touches only the
/// aligned span's pages. The outboard is untrusted; a tampered range, foreign
/// outboard, or wrong `root` all fail with [`RangeVerifyError::Verification`].
///
/// # Errors
///
/// - [`RangeVerifyError::OutboardSize`] if `outboard` is the wrong length for
///   `aligned.blob_size()`.
/// - [`RangeVerifyError::Verification`] if the range/outboard do not verify.
pub fn encode_verified_range_headerless<R: ReadAt, W: Write>(
    root: [u8; 32],
    aligned: &AlignedRange,
    outboard: Bytes,
    data: R,
    out: &mut W,
) -> Result<(), RangeVerifyError> {
    let blob_size = aligned.blob_size();
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
    encode_ranges_validated(&data, &ob, aligned.chunk_ranges().as_ref(), out)
        .map_err(|source| RangeVerifyError::Verification { source })
}

/// Compute the pre-order outboard and root for a `blob_size`-byte blob by reading
/// `data` **sequentially once** in bounded memory (`bao_tree` buffers one chunk
/// group, ~16 KiB; the returned outboard is a fraction of a percent of the blob).
/// The returned bytes are exactly what belongs at the `{hex}.obao4` sibling.
///
/// # Errors
///
/// Propagates any read error from `data`.
pub fn compute_pre_order_outboard<R: std::io::Read>(
    data: R,
    blob_size: u64,
) -> std::io::Result<([u8; 32], Vec<u8>)> {
    let tree = BaoTree::new(blob_size, IROH_BLOCK_SIZE);
    let ob_len = usize::try_from(tree.outboard_size())
        .map_err(|_| std::io::Error::other("outboard size exceeds usize"))?;
    let mut ob = PreOrderMemOutboard {
        root: bao_tree::blake3::Hash::from([0u8; 32]),
        tree,
        data: vec![0u8; ob_len],
    };
    let root = bao_tree::io::sync::outboard(data, tree, &mut ob)?;
    Ok((*root.as_bytes(), ob.data))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests {
    use super::*;

    // A blob spanning several chunk groups so the aligned fetch window has a
    // non-trivial length we can over- and under-shoot.
    const BLOB_SIZE: u64 = 5 * CHUNK_GROUP_BYTES + 123;

    // `encode_verified_range` rejects a `range_data` longer than the aligned fetch
    // window before any verification, so the surplus tail can never be silently
    // dropped. The bogus outboard is never reached — the size guard fires first.
    #[test]
    fn rejects_overlong_range_data() {
        let aligned = align_range(0, CHUNK_GROUP_BYTES, BLOB_SIZE).expect("align");
        let fetch_len = aligned.fetch_len();
        let too_long = usize::try_from(fetch_len).expect("fits usize") + 1;
        let range_data = vec![0u8; too_long];
        let err = encode_verified_range([0u8; 32], &aligned, &range_data, Bytes::new())
            .expect_err("overlong range_data must be rejected");
        assert!(matches!(
            err,
            RangeVerifyError::RangeDataSize {
                expected,
                got,
                blob_size,
            } if expected == fetch_len && got == too_long && blob_size == BLOB_SIZE
        ));
    }

    // The mirror case: a truncated 206 body is a typed error, not an opaque
    // verification short-read.
    #[test]
    fn rejects_too_short_range_data() {
        let aligned = align_range(0, CHUNK_GROUP_BYTES, BLOB_SIZE).expect("align");
        let fetch_len = aligned.fetch_len();
        let too_short = usize::try_from(fetch_len).expect("fits usize") - 1;
        let range_data = vec![0u8; too_short];
        let err = encode_verified_range([0u8; 32], &aligned, &range_data, Bytes::new())
            .expect_err("short range_data must be rejected");
        assert!(matches!(
            err,
            RangeVerifyError::RangeDataSize { expected, got, .. }
                if expected == fetch_len && got == too_short
        ));
    }

    // A seekable in-memory ReadAt over the whole blob, so the encoder reads at
    // absolute offsets exactly like a std::fs::File would.
    struct SliceReadAt(Vec<u8>);
    impl ReadAt for SliceReadAt {
        fn read_at(&self, pos: u64, buf: &mut [u8]) -> std::io::Result<usize> {
            let Ok(pos) = usize::try_from(pos) else {
                return Ok(0);
            };
            let Some(src) = self.0.get(pos..) else {
                return Ok(0);
            };
            let n = src.len().min(buf.len());
            let (Some(d), Some(s)) = (buf.get_mut(..n), src.get(..n)) else {
                return Ok(0);
            };
            d.copy_from_slice(s);
            Ok(n)
        }
    }

    // The header-less range encoder yields exactly the reference encoder's output
    // minus its 8-byte LE size header, for both a whole-blob range and an interior
    // range. This is the property the serve path depends on: identical wire bytes,
    // no header, streamed from a seekable reader.
    #[test]
    fn headerless_range_matches_reference_minus_header() {
        let data: Vec<u8> = (0..BLOB_SIZE).map(|i| (i % 251) as u8).collect();
        let mem = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let root = *mem.root.as_bytes();
        let outboard = Bytes::from(mem.data.clone());

        for (off, len) in [(0u64, 0u64), (CHUNK_GROUP_BYTES, CHUNK_GROUP_BYTES)] {
            let aligned = align_range(off, len, BLOB_SIZE).expect("align");
            // Reference: buffered encoder over the aligned window, drop 8-byte header.
            let base = usize::try_from(aligned.fetch_start()).expect("fits");
            let flen = usize::try_from(aligned.fetch_len()).expect("fits");
            let window = data.get(base..base + flen).expect("slice").to_vec();
            let reference = encode_verified_range(root, &aligned, &window, outboard.clone())
                .expect("reference encode");
            let expected = reference.get(8..).expect("has header").to_vec();

            let mut got = Vec::new();
            encode_verified_range_headerless(
                root,
                &aligned,
                outboard.clone(),
                SliceReadAt(data.clone()),
                &mut got,
            )
            .expect("headerless encode");

            assert_eq!(got, expected, "range off={off} len={len}");
        }
    }

    // A tampered data byte fails verification rather than emitting corrupt bytes.
    #[test]
    fn headerless_rejects_tampered_data() {
        let mut data: Vec<u8> = (0..BLOB_SIZE).map(|i| (i % 251) as u8).collect();
        let mem = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);
        let root = *mem.root.as_bytes();
        let outboard = Bytes::from(mem.data.clone());
        *data.get_mut(0).expect("nonempty") ^= 0xFF; // flip a byte after hashing

        let aligned = align_range(0, 0, BLOB_SIZE).expect("align");
        let mut sink = Vec::new();
        let err = encode_verified_range_headerless(
            root,
            &aligned,
            outboard,
            SliceReadAt(data),
            &mut sink,
        )
        .expect_err("tampered data must fail verification");
        assert!(matches!(err, RangeVerifyError::Verification { .. }));
    }

    // The computed outboard + root round-trips through the verifying encoder: the
    // bytes this produces are a valid `.obao4` for the data. Also asserts the root
    // equals blake3-of-data via PreOrderMemOutboard::create (the in-memory oracle).
    #[test]
    fn computed_outboard_matches_mem_and_verifies() {
        let data: Vec<u8> = (0..BLOB_SIZE).map(|i| (i % 251) as u8).collect();
        let oracle = PreOrderMemOutboard::create(&data, IROH_BLOCK_SIZE);

        let (root, outboard) =
            compute_pre_order_outboard(std::io::Cursor::new(data.clone()), BLOB_SIZE)
                .expect("compute");

        assert_eq!(root, *oracle.root.as_bytes(), "root mismatch");
        assert_eq!(outboard, oracle.data, "outboard bytes mismatch");

        // And it verifies through the Task 1 encoder.
        let aligned = align_range(0, 0, BLOB_SIZE).expect("align");
        let mut sink = Vec::new();
        encode_verified_range_headerless(
            root,
            &aligned,
            Bytes::from(outboard),
            SliceReadAt(data),
            &mut sink,
        )
        .expect("verify roundtrip");
    }
}
