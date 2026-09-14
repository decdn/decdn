//! Content-defined chunking for `origin import --optimize` (fastcdc v2020).
//! Pure CDC + hashing: it produces the manifest's range-dedup chunk hints. No
//! chunk is ever written as its own blob — `origin.rs` stores each file as one
//! whole-file blob and calls this only to compute the hints over its bytes.

use anyhow::{Result, bail};
use decdn_protocol::client::MB_BYTES;
use fastcdc::v2020;

/// Validated fastcdc chunk-size triple (bytes). fastcdc v2020 takes `usize`;
/// this struct stores `u32` (bounded <= 16 MiB) and widens at the call site.
pub(crate) struct ChunkSizes {
    /// Minimum chunk size in bytes.
    pub(crate) min: u32,
    /// Average (target) chunk size in bytes; a power of two.
    pub(crate) avg: u32,
    /// Maximum chunk size in bytes.
    pub(crate) max: u32,
}

impl ChunkSizes {
    /// Resolve and validate the size triple. `min`/`max` default to
    /// `max(avg/4, MB_BYTES)` and `2*avg`. Rejects (a) non-power-of-two
    /// `avg`, (b) `min < 1 MiB` (`MB_BYTES`, the payment interval), (c)
    /// `min > avg` or `avg > max`, and (d) any value outside fastcdc
    /// v2020's own bounds — so `StreamCDC::new`'s internal asserts can
    /// never fire.
    pub(crate) fn resolve(avg: u64, min: Option<u64>, max: Option<u64>) -> Result<Self> {
        if !avg.is_power_of_two() {
            bail!("--chunk-avg {avg} must be a power of two");
        }
        // Derive rails; the default min is avg/4 but never below the 1 MiB
        // payment interval.
        let min = min.unwrap_or_else(|| (avg / 4).max(MB_BYTES));
        let max = max.unwrap_or_else(|| avg.saturating_mul(2));

        if min < MB_BYTES {
            bail!("--chunk-min {min} is below the 1 MiB payment interval (MB_BYTES)");
        }
        if min > avg || avg > max {
            bail!("--chunk sizes must satisfy min <= avg <= max (min={min}, avg={avg}, max={max})");
        }
        // fastcdc v2020 structural bounds — check against the crate's own
        // constants (declared `usize`) so `StreamCDC::new`'s internal
        // asserts can never fire. The widening `usize -> u64` conversion is
        // fallible only in principle (no supported target has `usize` wider
        // than `u64`); `unwrap_or(u64::MAX)` keeps the comparison total
        // without an `unwrap`/`expect` on the `Result`.
        let minimum_min = u64::try_from(v2020::MINIMUM_MIN).unwrap_or(u64::MAX);
        let minimum_max = u64::try_from(v2020::MINIMUM_MAX).unwrap_or(u64::MAX);
        let average_min = u64::try_from(v2020::AVERAGE_MIN).unwrap_or(u64::MAX);
        let average_max = u64::try_from(v2020::AVERAGE_MAX).unwrap_or(u64::MAX);
        let maximum_min = u64::try_from(v2020::MAXIMUM_MIN).unwrap_or(u64::MAX);
        let maximum_max = u64::try_from(v2020::MAXIMUM_MAX).unwrap_or(u64::MAX);

        if !(minimum_min..=minimum_max).contains(&min) {
            bail!("--chunk-min {min} out of fastcdc range [{minimum_min}, {minimum_max}]");
        }
        if !(average_min..=average_max).contains(&avg) {
            bail!("--chunk-avg {avg} out of fastcdc range [{average_min}, {average_max}]");
        }
        if !(maximum_min..=maximum_max).contains(&max) {
            bail!("--chunk-max {max} out of fastcdc range [{maximum_min}, {maximum_max}]");
        }
        Ok(Self {
            min: u32::try_from(min)
                .map_err(|_| anyhow::anyhow!("--chunk-min {min} exceeds u32"))?,
            avg: u32::try_from(avg)
                .map_err(|_| anyhow::anyhow!("--chunk-avg {avg} exceeds u32"))?,
            max: u32::try_from(max)
                .map_err(|_| anyhow::anyhow!("--chunk-max {max} exceeds u32"))?,
        })
    }
}

/// A file's whole-file identity plus its ordered range-dedup chunk hints.
pub(crate) struct ChunkedFile {
    /// BLAKE3 of the whole file — the manifest entry's end-to-end validator.
    pub(crate) whole_hash: blake3::Hash,
    /// Sum of the chunk sizes; equals the file length.
    pub(crate) total_size: u64,
    /// Chunk hints in content order (the byte ranges they cover span the
    /// whole file, in sequence).
    pub(crate) chunks: Vec<crate::commands::manifest::Chunk>,
}

/// Stream `source` through fastcdc v2020. For each chunk, in content order:
/// feed its bytes to a whole-file BLAKE3 hasher, BLAKE3 the chunk range (its
/// hint hash), invoke `sink(chunk_hash, bytes)`, and record the `Chunk` hint.
/// `sink` never writes a chunk blob — every caller passes a no-op, since a
/// chunk hash is never independently stored or served — it exists only as a
/// hook a caller could use over the streamed bytes (e.g. for diagnostics).
/// Returns the whole-file hash, the summed size, and the ordered hint list.
pub(crate) fn chunk_file<R, F>(source: R, sizes: &ChunkSizes, mut sink: F) -> Result<ChunkedFile>
where
    R: std::io::Read,
    F: FnMut(blake3::Hash, &[u8]) -> Result<()>,
{
    use crate::commands::manifest::{Chunk, b3_hex_str};

    let mut whole = blake3::Hasher::new();
    let mut total: u64 = 0;
    let mut chunks: Vec<Chunk> = Vec::new();

    // fastcdc v2020's StreamCDC::new takes `usize` sizes; ChunkSizes stores
    // `u32` (bounded <= 16 MiB), so widen here via try_from (infallible on all
    // real >=32-bit targets; never `as`).
    let min = usize::try_from(sizes.min).map_err(|_| anyhow::anyhow!("chunk min exceeds usize"))?;
    let avg = usize::try_from(sizes.avg).map_err(|_| anyhow::anyhow!("chunk avg exceeds usize"))?;
    let max = usize::try_from(sizes.max).map_err(|_| anyhow::anyhow!("chunk max exceeds usize"))?;
    let chunker = v2020::StreamCDC::new(source, min, avg, max);
    for result in chunker {
        let chunk = result.map_err(|e| anyhow::anyhow!("fastcdc streaming error: {e}"))?;
        let data = chunk.data.as_slice();
        whole.update(data);
        let size = u64::try_from(chunk.length)
            .map_err(|_| anyhow::anyhow!("chunk length {} exceeds u64", chunk.length))?;
        total = total
            .checked_add(size)
            .ok_or_else(|| anyhow::anyhow!("total size overflow"))?;
        let chash = blake3::hash(data);
        sink(chash, data)?;
        chunks.push(Chunk {
            hash: b3_hex_str(chash),
            size,
        });
    }
    Ok(ChunkedFile {
        whole_hash: whole.finalize(),
        total_size: total,
        chunks,
    })
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;
    use crate::commands::manifest::b3_hex_str;

    // Deterministic pseudo-random bytes so chunk boundaries actually form.
    fn pseudo(seed: u64, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut x = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        while out.len() < len {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            out.extend_from_slice(&x.to_le_bytes());
        }
        out.truncate(len);
        out
    }

    fn sizes() -> ChunkSizes {
        ChunkSizes::resolve(4 * 1024 * 1024, None, None).unwrap()
    }

    #[test]
    fn whole_hash_and_total_match_direct_blake3() {
        let data = pseudo(1, 20 * 1024 * 1024);
        let mut seen: Vec<Vec<u8>> = Vec::new();
        let cf = chunk_file(&data[..], &sizes(), |_h, b| {
            seen.push(b.to_vec());
            Ok(())
        })
        .unwrap();
        assert_eq!(cf.whole_hash, blake3::hash(&data));
        assert_eq!(cf.total_size, data.len() as u64);
        // sink saw the file in order, exactly once.
        assert_eq!(seen.concat(), data);
        // chunk sizes sum to total.
        assert_eq!(cf.chunks.iter().map(|c| c.size).sum::<u64>(), cf.total_size);
    }

    #[test]
    fn deterministic_boundaries() {
        let data = pseudo(2, 20 * 1024 * 1024);
        let a = chunk_file(&data[..], &sizes(), |_h, _b| Ok(())).unwrap();
        let b = chunk_file(&data[..], &sizes(), |_h, _b| Ok(())).unwrap();
        let ha: Vec<_> = a.chunks.iter().map(|c| c.hash.clone()).collect();
        let hb: Vec<_> = b.chunks.iter().map(|c| c.hash.clone()).collect();
        assert_eq!(ha, hb);
        assert!(ha.len() >= 2, "expected multiple chunks, got {}", ha.len());
    }

    #[test]
    fn shared_region_dedups_distinct_does_not() {
        // Two files sharing a big identical middle region share >=1 chunk hash;
        // a fully distinct file shares none.
        let shared = pseudo(3, 16 * 1024 * 1024);
        let mut f1 = pseudo(10, 4 * 1024 * 1024);
        f1.extend_from_slice(&shared);
        f1.extend_from_slice(&pseudo(11, 4 * 1024 * 1024));
        let mut f2 = pseudo(20, 4 * 1024 * 1024);
        f2.extend_from_slice(&shared);
        f2.extend_from_slice(&pseudo(21, 4 * 1024 * 1024));
        let distinct = pseudo(99, 24 * 1024 * 1024);
        let h = |d: &[u8]| -> std::collections::HashSet<String> {
            chunk_file(d, &sizes(), |_h, _b| Ok(()))
                .unwrap()
                .chunks
                .into_iter()
                .map(|c| c.hash)
                .collect()
        };
        let (s1, s2, sd) = (h(&f1), h(&f2), h(&distinct));
        assert!(
            s1.intersection(&s2).count() >= 1,
            "shared region should dedup"
        );
        assert_eq!(
            s1.intersection(&sd).count(),
            0,
            "distinct file should not dedup"
        );
        let _ = b3_hex_str; // keep the import used
    }

    #[test]
    fn cdc_resyncs_past_a_length_shift() {
        // The shared region starts at different byte offsets in each file
        // because the prefixes differ in LENGTH (1 MiB vs 1 MiB + 7 bytes) and
        // in content. A fixed-size chunker would misalign every downstream
        // chunk boundary and share zero chunks; CDC re-syncs its
        // content-defined cut points once it re-enters the shared bytes, so
        // the interior and trailing chunks of `shared` still come out
        // identical in both files (only the chunk straddling the
        // prefix->shared seam differs).
        let shared = pseudo(3, 16 * 1024 * 1024);
        let mut f1 = pseudo(10, 1024 * 1024);
        f1.extend_from_slice(&shared);
        let mut f2 = pseudo(20, 1024 * 1024 + 7);
        f2.extend_from_slice(&shared);

        let h = |d: &[u8]| -> std::collections::HashSet<String> {
            chunk_file(d, &sizes(), |_h, _b| Ok(()))
                .unwrap()
                .chunks
                .into_iter()
                .map(|c| c.hash)
                .collect()
        };
        let (s1, s2) = (h(&f1), h(&f2));
        let shared_count = s1.intersection(&s2).count();
        assert!(
            shared_count >= 2,
            "CDC should re-sync past the length shift and share several \
             chunks of `shared`; got {shared_count} shared out of {} / {} \
             chunks",
            s1.len(),
            s2.len()
        );
        // The differing prefixes mean the files are not chunk-for-chunk
        // identical.
        assert!(s1 != s2, "prefixes differ, so the chunk sets must too");
    }

    #[test]
    fn shared_head_divergent_tail_dedups_head_not_tail() {
        // A shared leading region followed by divergent tails: the head
        // chunks dedup (proving CDC finds the shared content), while each
        // file's tail produces chunks the other lacks (proving divergence
        // still yields distinct chunks, not spurious matches).
        let head = pseudo(5, 16 * 1024 * 1024);
        let mut f1 = head.clone();
        f1.extend_from_slice(&pseudo(30, 8 * 1024 * 1024));
        let mut f2 = head.clone();
        f2.extend_from_slice(&pseudo(40, 8 * 1024 * 1024));

        let h = |d: &[u8]| -> std::collections::HashSet<String> {
            chunk_file(d, &sizes(), |_h, _b| Ok(()))
                .unwrap()
                .chunks
                .into_iter()
                .map(|c| c.hash)
                .collect()
        };
        let (s1, s2) = (h(&f1), h(&f2));
        let shared_count = s1.intersection(&s2).count();
        assert!(
            shared_count >= 2,
            "shared head should dedup several chunks; got {shared_count}"
        );
        let f1_only: std::collections::HashSet<_> = s1.difference(&s2).cloned().collect();
        let f2_only: std::collections::HashSet<_> = s2.difference(&s1).cloned().collect();
        assert!(
            !f1_only.is_empty() && !f2_only.is_empty(),
            "divergent tails should each produce chunks the other file lacks \
             (f1_only={}, f2_only={})",
            f1_only.len(),
            f2_only.len()
        );
    }

    #[test]
    fn empty_input_yields_empty_chunks_and_empty_hash() {
        let cf = chunk_file(&[][..], &sizes(), |_h, _b| Ok(())).unwrap();
        assert_eq!(cf.total_size, 0);
        assert!(cf.chunks.is_empty());
        assert_eq!(cf.whole_hash, blake3::hash(&[]));
    }

    #[test]
    fn defaults_derive_rails_from_avg() {
        let s = ChunkSizes::resolve(4 * 1024 * 1024, None, None).unwrap();
        assert_eq!(s.min, 1024 * 1024);
        assert_eq!(s.avg, 4 * 1024 * 1024);
        assert_eq!(s.max, 8 * 1024 * 1024);
    }

    #[test]
    fn rejects_non_power_of_two_avg() {
        assert!(ChunkSizes::resolve(3 * 1024 * 1024, None, None).is_err());
    }

    #[test]
    fn rejects_min_below_one_mib() {
        assert!(ChunkSizes::resolve(4 * 1024 * 1024, Some(512 * 1024), None).is_err());
    }

    #[test]
    fn rejects_avg_above_fastcdc_ceiling() {
        // 8 MiB avg exceeds AVERAGE_MAX (4 MiB).
        assert!(ChunkSizes::resolve(8 * 1024 * 1024, None, None).is_err());
    }

    #[test]
    fn rejects_max_above_fastcdc_ceiling() {
        assert!(ChunkSizes::resolve(4 * 1024 * 1024, None, Some(32 * 1024 * 1024)).is_err());
    }

    #[test]
    fn rejects_min_gt_avg_and_avg_gt_max() {
        assert!(ChunkSizes::resolve(2 * 1024 * 1024, Some(4 * 1024 * 1024), None).is_err());
        assert!(ChunkSizes::resolve(4 * 1024 * 1024, None, Some(2 * 1024 * 1024)).is_err());
    }

    #[test]
    fn accepts_2mib_avg_floors_min_at_one_mib() {
        let s = ChunkSizes::resolve(2 * 1024 * 1024, None, None).unwrap();
        assert_eq!(
            (s.min, s.avg, s.max),
            (1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024)
        );
    }
}
