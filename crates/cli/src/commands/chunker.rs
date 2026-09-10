//! Content-defined chunking for `origin import --optimize` (fastcdc v2020).
//! Pure CDC + hashing; writing chunk blobs stays in `origin.rs`.

use anyhow::{Result, bail};
use decdn_protocol::client::MB_BYTES;
use fastcdc::v2020;

/// Validated fastcdc chunk-size triple (bytes). fastcdc takes `u32`.
// `chunk_file` (the streaming consumer that builds a `StreamCDC` from this
// triple) lands separately; until it does, the struct is only constructed
// by its own tests, so non-test builds see it as dead. `#[allow]`, not
// `#[expect]`: the lint only fires outside `cfg(test)`, and an `#[expect]`
// would go unfulfilled in the test build that exercises this type.
#[allow(
    dead_code,
    reason = "consumed by chunk_file, added in a follow-up change"
)]
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
    #[allow(
        dead_code,
        reason = "consumed by chunk_file, added in a follow-up change"
    )]
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;

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
