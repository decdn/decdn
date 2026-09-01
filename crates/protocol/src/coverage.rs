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

/// Number of [`DISCOVERY_BLOCK_BYTES`] blocks a blob of `total_bytes` spans.
///
/// The empty blob spans zero blocks (`num_blocks(0) == 0`), not one — an
/// empty [`Coverage`] over it is trivially complete.
#[must_use]
#[allow(clippy::cast_possible_truncation)] // a blob would need to be ~256 EiB to truncate here
pub const fn num_blocks(total_bytes: u64) -> u32 {
    total_bytes.div_ceil(DISCOVERY_BLOCK_BYTES) as u32
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
/// Carries no `num_blocks` field: the bitmap's own byte length only ever
/// rounds a block count *up* to the next multiple of 8, so a trailing partial
/// byte's unused high bits simply stay zero and never come back out of
/// [`Self::covered_blocks`] or [`Self::covers`] for an out-of-range index.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct Coverage {
    blocks: Vec<u8>,
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

    /// Iterates the covered block indices in ascending order.
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
}
