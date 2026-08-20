//! Pure segmentation and tail-steal helpers for the multi-source scheduler
//! (spec §5.3). No I/O, no async: split a gap-set into bao-aligned contiguous
//! segments, and pick-and-split the largest remaining range for a freed source
//! to steal. Every returned range is chunk-group aligned so it is
//! independently bao-verifiable.

// The multi-source scheduler (spec §5.3) is the consumer of this module; it
// wires `initial_segments`/`steal_split`/`MIN_SPLIT_SIZE` in. Until then
// these are unreached from any call site.
#![allow(dead_code)]

use decdn_bao_range::{AlignedRange, align_range};

/// Floor below which an idle source does not split/steal a remaining range —
/// no fresh stream for a tail smaller than this (spec §8).
pub(crate) const MIN_SPLIT_SIZE: u64 = 16 * 1024 * 1024;

/// Split the gap-set into up to `n` contiguous bao-aligned segments of roughly
/// equal total size. Never returns an empty segment; may return fewer than `n`
/// when the gap-set is small or a group-aligned split would otherwise degenerate.
///
/// Each returned segment lies wholly within a single input gap (a segment can
/// never span data the buyer already holds), so the `n` cap is enforced
/// per-gap: once the running count reaches `n`, the rest of the CURRENT gap
/// folds into its final segment. A gap-set fragmented into more than `n`
/// disjoint gaps therefore yields one segment per remaining gap beyond that —
/// exact coverage is never sacrificed to honor the cap exactly.
///
/// # Errors
///
/// Propagates [`decdn_bao_range::RangeVerifyError`] from `align_range` (an
/// out-of-bounds gap against `total_bytes`).
pub(crate) fn initial_segments(
    gaps: &[(u64, u64)],
    n: usize,
    total_bytes: u64,
) -> anyhow::Result<Vec<AlignedRange>> {
    let total_gap: u64 = gaps.iter().map(|&(_, len)| len).sum();
    if total_gap == 0 || n == 0 {
        return Ok(Vec::new());
    }
    let target = total_gap.div_ceil(n as u64).max(1);
    let mut out: Vec<AlignedRange> = Vec::new();
    for &(start, len) in gaps {
        let mut off = start;
        let end = start.saturating_add(len);
        while off < end {
            // Once the segment cap is reached, fold the rest of THIS gap (and any
            // later gaps) into the final segment so coverage stays exact — never
            // silently drop the remainder.
            let want = if out.len() + 1 >= n {
                end - off
            } else {
                target.min(end - off)
            };
            let seg = align_range(off, want, total_bytes)?;
            // Group-align the end, clamped to this gap's own end (a later gap
            // starts at its own aligned boundary, so this never re-covers bytes
            // a prior segment already claimed).
            let seg_end = seg.fetch_end().min(end);
            if seg_end <= off {
                // A degenerate zero-width step (would only occur at a gap of
                // width 0, which the outer `while off < end` already excludes).
                break;
            }
            let aligned = align_range(off, seg_end - off, total_bytes)?;
            out.push(aligned);
            off = seg_end;
        }
    }
    Ok(out)
}

/// Pick the largest remaining range and, if it is at least [`MIN_SPLIT_SIZE`],
/// return its aligned second half for a freed source to steal. Returns `None`
/// when nothing remaining is worth a fresh stream.
///
/// # Errors
///
/// Propagates [`decdn_bao_range::RangeVerifyError`] from `align_range`.
pub(crate) fn steal_split(
    remaining: &[(u64, u64)],
    total_bytes: u64,
) -> anyhow::Result<Option<AlignedRange>> {
    let Some(&(start, len)) = remaining.iter().max_by_key(|&&(_, len)| len) else {
        return Ok(None);
    };
    if len < MIN_SPLIT_SIZE {
        return Ok(None);
    }
    let mid_offset = start + len / 2;
    // Align the candidate second half; if group-alignment collapses it back
    // onto (or past) the whole range, there is nothing worth splitting off.
    let second_half = align_range(mid_offset, start + len - mid_offset, total_bytes)?;
    if second_half.fetch_start() <= start || second_half.fetch_start() >= start + len {
        return Ok(None);
    }
    Ok(Some(second_half))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests
mod tests {
    #[test]
    fn initial_segments_splits_contiguous_gap_into_n_aligned_parts() -> anyhow::Result<()> {
        let total = 64 * 1024 * 1024;
        let segs = super::initial_segments(&[(0, total)], 4, total)?;
        assert_eq!(segs.len(), 4);
        // Contiguous, non-overlapping, group-aligned, covering [0,total).
        assert_eq!(segs[0].fetch_start(), 0);
        let last = segs.last().expect("nonempty");
        assert_eq!(last.fetch_end(), total);
        for w in segs.windows(2) {
            assert_eq!(w[0].fetch_end(), w[1].fetch_start());
            assert_eq!(w[0].fetch_start() % (16 * 1024), 0);
        }
        Ok(())
    }

    #[test]
    fn initial_segments_caps_at_available_gap_count() -> anyhow::Result<()> {
        // A single 8 MiB gap with n=4 yields at most gaps sized >= one group each,
        // never more segments than make sense; every segment is non-empty.
        let segs = super::initial_segments(&[(0, 8 * 1024 * 1024)], 4, 8 * 1024 * 1024)?;
        assert!(!segs.is_empty() && segs.len() <= 4);
        assert!(segs.iter().all(|s| s.fetch_end() > s.fetch_start()));
        Ok(())
    }

    #[test]
    fn steal_split_takes_second_half_of_largest_above_floor() -> anyhow::Result<()> {
        let total = 100 * 1024 * 1024;
        // Largest remaining is 40 MiB at offset 10 MiB; steal its aligned second half.
        let stolen = super::steal_split(
            &[(0, 4 * 1024 * 1024), (10 * 1024 * 1024, 40 * 1024 * 1024)],
            total,
        )?
        .expect("above floor");
        assert!(stolen.fetch_start() > 10 * 1024 * 1024);
        assert_eq!(stolen.fetch_end(), 50 * 1024 * 1024);
        assert_eq!(stolen.fetch_start() % (16 * 1024), 0);
        Ok(())
    }

    #[test]
    fn steal_split_returns_none_below_floor() -> anyhow::Result<()> {
        let total = 100 * 1024 * 1024;
        // Largest remaining is 8 MiB < 16 MiB floor -> don't steal.
        assert!(super::steal_split(&[(0, 8 * 1024 * 1024)], total)?.is_none());
        Ok(())
    }
}
