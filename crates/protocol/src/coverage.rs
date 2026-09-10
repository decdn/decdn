//! Range-keyed block coverage bitmap (ADR 039-adjacent partial-holder
//! discovery).
//!
//! A blob is divided into fixed-size [`DISCOVERY_BLOCK_BYTES`] "discovery
//! blocks" (64 MiB each); [`Coverage`] is a bitmap over those block indices
//! recording which blocks a holder can serve. This is the wire-level
//! granularity partial-holder discovery advertises and queries at — coarse
//! enough that a 64 MiB blob (this codebase's common case, #1164) still fits
//! in one bit, fine enough that a multi-GB blob's holders can be told apart
//! by which slice they actually hold.
//!
//! `Coverage` itself only carries the bitmap; deriving one from a node's
//! actual cache state (or from origin capability) is the caller's job — see
//! `decdn_cache::CacheEngine::coverage` for the cached-block derivation.

use serde::{Deserialize, Serialize};

/// Size of one discovery block: 64 MiB.
///
/// Blob byte ranges are bucketed into blocks of this size for coverage
/// bitmaps — see [`num_blocks`] and [`Coverage`].
pub const DISCOVERY_BLOCK_BYTES: u64 = 64 << 20;

/// Largest blob the discovery layer describes on the wire: 1 TiB.
///
/// This is a protocol ceiling that sizes the coverage-bitmap bound
/// ([`MAX_COVERAGE_BYTES`]), not a per-node store limit — `max_blob_size_mb`
/// is the separate, operator-tunable cache cap. A blob larger than this spans
/// more discovery blocks than a well-formed [`Coverage`] can name on the wire,
/// so it is not partial-holder-discoverable. The AI-model delivery wedge (#1164)
/// serves sub-TiB blobs, so this bound never rejects a legitimate advertisement.
pub const MAX_DISCOVERABLE_BLOB_BYTES: u64 = 1 << 40;

/// Upper bound, in bytes, on a [`Coverage`] bitmap decoded from untrusted wire.
///
/// A well-formed bitmap for a [`MAX_DISCOVERABLE_BLOB_BYTES`] blob is this many
/// bytes: bit-packed at 8 blocks per byte over the fixed 64 MiB
/// [`DISCOVERY_BLOCK_BYTES`] (never the `test-support` overridable size, so the
/// bound is a stable security constant). A frame whose `Coverage` is longer
/// names more discovery blocks than a 1 TiB blob spans — beyond what
/// partial-holder discovery represents — and is rejected at decode. This is a
/// wire representability bound, not a serving cap: a node may still be
/// configured to hold and serve a larger blob, it just cannot advertise partial
/// coverage for one over the DHT.
///
/// Without this cap a bonded publisher could pin arbitrary receiver memory:
/// coverage length is otherwise bounded only by the 16 MiB frame, and the
/// per-publisher record quota multiplies it (stored memory ≈ records × bitmap).
/// The cap makes an oversized bitmap unrepresentable rather than defended after
/// the fact. For 1 TiB it is 2048 bytes.
#[expect(
    clippy::cast_possible_truncation,
    reason = "1 TiB / 64 MiB / 8 = 2048, far within usize on every supported target"
)]
pub const MAX_COVERAGE_BYTES: usize = MAX_DISCOVERABLE_BLOB_BYTES
    .div_ceil(DISCOVERY_BLOCK_BYTES)
    .div_ceil(8) as usize;

/// The active discovery-block size in bytes.
///
/// Production reads the fixed [`DISCOVERY_BLOCK_BYTES`] constant — this
/// accessor inlines to it, so the block geometry is byte-identical to naming
/// the constant directly. Test builds that enable the `test-support` feature
/// get an overridable value instead (see
/// `override_discovery_block_bytes_for_test`), so a loopback test can drive
/// the block-spanning planner with tiny blocks and tiny blobs rather than the
/// 64 MiB a real two-block blob would need.
#[cfg(not(feature = "test-support"))]
#[inline]
#[must_use]
pub const fn discovery_block_bytes() -> u64 {
    DISCOVERY_BLOCK_BYTES
}

/// Number of [`discovery_block_bytes`] blocks a blob of `total_bytes` spans.
///
/// The empty blob spans zero blocks (`num_blocks(0) == 0`), not one — an
/// empty [`Coverage`] over it is trivially complete.
#[cfg(not(feature = "test-support"))]
#[must_use]
#[allow(clippy::cast_possible_truncation)] // a blob would need to be ~256 EiB to truncate here
pub const fn num_blocks(total_bytes: u64) -> u32 {
    total_bytes.div_ceil(DISCOVERY_BLOCK_BYTES) as u32
}

/// The active discovery-block size in bytes — the `test-support` twin whose
/// value a test can override. Defaults to [`DISCOVERY_BLOCK_BYTES`] until
/// [`override_discovery_block_bytes_for_test`] sets it.
#[cfg(feature = "test-support")]
#[must_use]
pub fn discovery_block_bytes() -> u64 {
    test_block_size::current()
}

/// Number of [`discovery_block_bytes`] blocks a blob of `total_bytes` spans —
/// the `test-support` twin, which reads the overridable block size rather than
/// the constant. Same arithmetic as the production `const fn`.
#[cfg(feature = "test-support")]
#[must_use]
#[allow(clippy::cast_possible_truncation)] // a blob would need to be ~256 EiB to truncate here
pub fn num_blocks(total_bytes: u64) -> u32 {
    total_bytes.div_ceil(discovery_block_bytes()) as u32
}

/// The overridable discovery-block size, compiled only under `test-support`.
#[cfg(feature = "test-support")]
mod test_block_size {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::DISCOVERY_BLOCK_BYTES;

    /// The active block size. Starts at [`DISCOVERY_BLOCK_BYTES`], so an
    /// un-overridden `test-support` build behaves exactly like production.
    static BLOCK_BYTES: AtomicU64 = AtomicU64::new(DISCOVERY_BLOCK_BYTES);

    pub(super) fn current() -> u64 {
        BLOCK_BYTES.load(Ordering::Relaxed)
    }

    pub(super) fn set(bytes: u64) {
        BLOCK_BYTES.store(bytes, Ordering::Relaxed);
    }

    pub(super) fn reset() {
        BLOCK_BYTES.store(DISCOVERY_BLOCK_BYTES, Ordering::Relaxed);
    }
}

/// A live override of the discovery-block size for the duration of the returned
/// guard (test-support only).
///
/// While the guard is held, [`discovery_block_bytes`] and [`num_blocks`] report
/// `bytes` instead of [`DISCOVERY_BLOCK_BYTES`], so the block-spanning planner
/// (`decdn_client_pull::plan_covered_runs`) and the node's coverage-union probe
/// gather split a tiny blob into several blocks. Dropping the guard restores the
/// production default.
///
/// The override is process-global, so it assumes the one-process-per-test
/// isolation `cargo nextest run` gives; two tests that override it concurrently
/// in one process (plain `cargo test`) would race.
#[cfg(feature = "test-support")]
#[must_use = "the override is reverted when the returned guard is dropped"]
pub fn override_discovery_block_bytes_for_test(bytes: u64) -> TestBlockSizeGuard {
    test_block_size::set(bytes);
    TestBlockSizeGuard { _private: () }
}

/// Restores the production discovery-block size when dropped (test-support only).
#[cfg(feature = "test-support")]
#[derive(Debug)]
pub struct TestBlockSizeGuard {
    _private: (),
}

#[cfg(feature = "test-support")]
impl Drop for TestBlockSizeGuard {
    fn drop(&mut self) {
        test_block_size::reset();
    }
}

/// A bitmap over discovery-block indices: bit `i` records whether block `i`
/// (bytes `[i * DISCOVERY_BLOCK_BYTES, (i + 1) * DISCOVERY_BLOCK_BYTES)`) is
/// covered.
///
/// Bit-packed as `Vec<u8>`: bit `i` lives at byte `i / 8`, bit position
/// `i % 8` (little-endian within the byte). Serializes over postcard as a
/// length-prefixed byte vector — cheap on the wire and trivial to union/merge
/// byte-wise if a future caller needs to.
///
/// Carries no `num_blocks` field. A `Coverage` built locally (via
/// [`Self::from_block_indices`] / [`Self::full`]) leaves the trailing partial
/// byte's unused high bits zero. A `Coverage` **deserialized from untrusted
/// wire** does not: the byte length only rounds the block count up to the next
/// multiple of 8, and nothing on the wire pins the exact count, so a peer may
/// set spurious high bits in that trailing byte. [`Self::covers`] and
/// [`Self::covered_blocks`] report the raw bits as-is, so such a bit makes
/// `covers(i)` true for an `i` past the blob's real block count. Consumers MUST
/// clamp interpretation to the blob's own [`num_blocks`]`(total_bytes)` — query
/// only `i < num_blocks`, and ignore any [`Self::covered_blocks`] index beyond
/// it. The signed size, not the bitmap, is the authority on how many blocks
/// exist.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct Coverage {
    #[serde(deserialize_with = "deserialize_bounded_blocks")]
    blocks: Vec<u8>,
}

/// Bounds a wire `Coverage` bitmap to [`MAX_COVERAGE_BYTES`] at decode.
///
/// A longer bitmap names more blocks than a [`MAX_DISCOVERABLE_BLOB_BYTES`] blob
/// spans — beyond what partial-holder discovery represents — so it is rejected
/// rather than stored, and an untrusted peer cannot amplify one record into
/// arbitrary receiver memory. This bounds what the wire represents, not what a
/// node may serve. Decoding the
/// bytes before the length check is safe: the enclosing 16 MiB frame cap and
/// serde's cautious capacity hint bound the allocation independently of the
/// wire length prefix, so the prefix is never trusted for sizing (#845).
fn deserialize_bounded_blocks<'de, D>(d: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let blocks = Vec::<u8>::deserialize(d)?;
    if blocks.len() > MAX_COVERAGE_BYTES {
        return Err(serde::de::Error::custom(format!(
            "Coverage bitmap length {} exceeds MAX_COVERAGE_BYTES ({MAX_COVERAGE_BYTES})",
            blocks.len(),
        )));
    }
    Ok(blocks)
}

impl Coverage {
    /// No blocks covered.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Every block in `0..num_blocks` covered.
    #[must_use]
    pub fn full(num_blocks: u32) -> Self {
        Self::from_block_indices(num_blocks, 0..num_blocks)
    }

    /// Builds a `Coverage` sized for `num_blocks` blocks with exactly the
    /// indices from `set` covered. Indices `>= num_blocks` in `set` are
    /// dropped rather than growing the bitmap past its declared size.
    #[must_use]
    pub fn from_block_indices(num_blocks: u32, set: impl Iterator<Item = u32>) -> Self {
        let byte_len = (num_blocks as usize).div_ceil(8);
        let mut blocks = vec![0u8; byte_len];
        for i in set {
            if i >= num_blocks {
                continue;
            }
            let byte_idx = (i / 8) as usize;
            let bit = i % 8;
            if let Some(b) = blocks.get_mut(byte_idx) {
                *b |= 1u8 << bit;
            }
        }
        Self { blocks }
    }

    /// Is `block` covered?
    #[must_use]
    pub fn covers(&self, block: u32) -> bool {
        let byte_idx = (block / 8) as usize;
        let bit = block % 8;
        self.blocks
            .get(byte_idx)
            .is_some_and(|b| (b >> bit) & 1 == 1)
    }

    /// No block is covered — every byte of the bitmap is zero.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.blocks.iter().all(|&b| b == 0)
    }

    /// Iterates the covered block indices in ascending order. Reports raw set
    /// bits: a `Coverage` from untrusted wire may yield indices past the blob's
    /// real block count (see the type doc), so a consumer bounds these against
    /// its own [`num_blocks`]`(total_bytes)`.
    pub fn covered_blocks(&self) -> impl Iterator<Item = u32> + '_ {
        self.blocks.iter().enumerate().flat_map(|(byte_idx, &b)| {
            // `byte_idx` is bounded by the bitmap's own byte length, which never
            // exceeds `num_blocks / 8` for any `Coverage` this type constructs —
            // well under `u32::MAX` in practice.
            #[allow(clippy::cast_possible_truncation)]
            let byte_idx = byte_idx as u32;
            (0..8u32)
                .filter(move |bit| (b >> bit) & 1 == 1)
                .map(move |bit| byte_idx * 8 + bit)
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn full_covers_every_block_in_range_and_nothing_past_it() {
        let cov = Coverage::full(3);
        for i in 0..=2 {
            assert!(cov.covers(i), "block {i} should be covered by full(3)");
        }
        assert!(!cov.covers(3), "full(3) must not cover block 3");
    }

    #[test]
    fn from_block_indices_covers_exactly_the_given_set() {
        let cov = Coverage::from_block_indices(4, [0, 2].into_iter());
        assert!(cov.covers(0));
        assert!(!cov.covers(1));
        assert!(cov.covers(2));
        assert!(!cov.covers(3));
    }

    #[test]
    fn empty_is_empty() {
        assert!(Coverage::empty().is_empty());
        assert!(Coverage::default().is_empty());
        assert!(!Coverage::full(1).is_empty());
    }

    #[test]
    fn num_blocks_zero_for_empty_blob() {
        assert_eq!(num_blocks(0), 0);
        assert_eq!(num_blocks(1), 1);
        assert_eq!(num_blocks(DISCOVERY_BLOCK_BYTES), 1);
        assert_eq!(num_blocks(DISCOVERY_BLOCK_BYTES + 1), 2);
    }

    #[test]
    fn covered_blocks_walks_set_bits_in_order() {
        let cov = Coverage::from_block_indices(20, [3, 5, 17].into_iter());
        assert_eq!(cov.covered_blocks().collect::<Vec<_>>(), vec![3, 5, 17]);
    }

    #[test]
    fn out_of_range_indices_in_the_input_set_are_dropped() {
        let cov = Coverage::from_block_indices(4, [0, 4, 100].into_iter());
        assert!(cov.covers(0));
        assert!(!cov.covers(4), "block 4 is out of range for num_blocks=4");
        assert_eq!(cov.covered_blocks().collect::<Vec<_>>(), vec![0]);
    }

    #[test]
    fn postcard_round_trips() {
        let cov = Coverage::from_block_indices(20, [3, 5, 17].into_iter());
        let bytes = postcard::to_allocvec(&cov).expect("postcard serialize");
        let back: Coverage = postcard::from_bytes(&bytes).expect("postcard deserialize");
        assert_eq!(cov, back);

        let empty = Coverage::empty();
        let bytes = postcard::to_allocvec(&empty).expect("postcard serialize");
        let back: Coverage = postcard::from_bytes(&bytes).expect("postcard deserialize");
        assert_eq!(empty, back);
    }

    #[test]
    fn max_coverage_bytes_matches_one_tib_blob() {
        // 1 TiB / 64 MiB = 16384 blocks, bit-packed at 8/byte = 2048 bytes.
        assert_eq!(MAX_COVERAGE_BYTES, 2048);
    }

    #[test]
    fn deserialize_accepts_bitmap_at_the_cap() {
        // A single-field struct postcard-encodes identically to its `Vec<u8>`
        // field, so an at-cap byte vector is a valid at-cap `Coverage`.
        let at_cap = vec![0xFFu8; MAX_COVERAGE_BYTES];
        let bytes = postcard::to_allocvec(&at_cap).expect("postcard serialize");
        let back: Coverage = postcard::from_bytes(&bytes).expect("at-cap coverage must decode");
        assert_eq!(back.blocks.len(), MAX_COVERAGE_BYTES);
    }

    #[test]
    fn deserialize_rejects_oversized_bitmap() {
        // One byte past the cap describes a blob larger than
        // MAX_DISCOVERABLE_BLOB_BYTES and must fail at decode rather than be
        // stored — this is the memory-amplification guard.
        let oversized = vec![0u8; MAX_COVERAGE_BYTES + 1];
        let bytes = postcard::to_allocvec(&oversized).expect("postcard serialize");
        let decoded: Result<Coverage, _> = postcard::from_bytes(&bytes);
        assert!(
            decoded.is_err(),
            "oversized coverage bitmap must be rejected"
        );
    }
}
