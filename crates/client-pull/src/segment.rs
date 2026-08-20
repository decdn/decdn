//! Pure segmentation and tail-steal helpers for the multi-source scheduler
//! (spec §5.3). No I/O, no async: split a gap-set into bao-aligned contiguous
//! segments, and pick-and-split the largest remaining range for a freed source
//! to steal. Every returned range is chunk-group aligned so it is
//! independently bao-verifiable.

use decdn_bao_range::{AlignedRange, CHUNK_GROUP_BYTES, RangeVerifyError, align_range};

/// Floor below which an idle source does not split/steal a remaining range —
/// no fresh stream for a tail smaller than this (spec §8).
pub(crate) const MIN_SPLIT_SIZE: u64 = 16 * 1024 * 1024;

/// Round `[start, start + len)` out to its enclosing 16 KiB chunk-group
/// boundaries — start DOWN, end UP, end clamped to `total_bytes` — then merge
/// any of the resulting spans that now touch or overlap into one disjoint,
/// group-aligned set, in ascending order.
///
/// Bao verification is chunk-group-granular: a partial boundary group can only
/// be fetched (and independently verified) as a whole group, and a
/// [`decdn_bao_range::RangedStore`] only ever holds whole verified groups. So
/// on real inputs — gaps already derived from a group-aligned present/missing
/// split — every span here is already on a group boundary and this
/// canonicalization is a no-op. On any unaligned input (e.g. a raw byte range
/// a caller has not yet aligned) it guarantees the spans handed to the caller
/// are disjoint and group-aligned: every downstream `align_range` call then
/// operates on an already-aligned boundary, so its own ceiling-up can never
/// carry a segment past where the caller intended it to stop and overlap a
/// neighboring span. The one cost is a bounded over-fetch of at most one group
/// at each original edge, which is idempotent to re-fetch into the store.
///
/// Clamping the rounded-up END to `total_bytes` is the legitimate handling of
/// the blob's own partial last group (mirroring `align_range`'s own clamp) —
/// it is not license to silently accept a genuinely out-of-bounds span. A span
/// whose `start` is at or past `total_bytes`, or whose *pre-rounding* end
/// (`start + len`) exceeds `total_bytes`, references bytes that were never in
/// the blob at all, and is rejected before any rounding happens.
///
/// # Errors
///
/// [`RangeVerifyError::RangeOutOfBounds`] for a span whose `start` is at or
/// past `total_bytes`, or whose raw (pre-rounding) end exceeds `total_bytes` —
/// the same error `align_range` itself raises for an out-of-bounds request, so
/// callers of [`initial_segments`]/[`steal_split`] see one consistent error
/// type regardless of which stage rejected the input.
fn canonicalize_ranges(
    spans: &[(u64, u64)],
    total_bytes: u64,
) -> Result<Vec<(u64, u64)>, RangeVerifyError> {
    let mut canon: Vec<(u64, u64)> = Vec::with_capacity(spans.len());
    for &(start, len) in spans {
        if len == 0 {
            continue;
        }
        let oob = || RangeVerifyError::RangeOutOfBounds {
            offset: start,
            len,
            blob_size: total_bytes,
        };
        let raw_end = start.checked_add(len).ok_or_else(oob)?;
        if start >= total_bytes || raw_end > total_bytes {
            return Err(oob());
        }
        let canon_start = (start / CHUNK_GROUP_BYTES) * CHUNK_GROUP_BYTES;
        // Rounding the end UP past `raw_end` and clamping to `total_bytes` here
        // is the blob's own partial-last-group case, already proven in-bounds
        // by the check above — never an out-of-bounds acceptance.
        let canon_end = raw_end
            .div_ceil(CHUNK_GROUP_BYTES)
            .saturating_mul(CHUNK_GROUP_BYTES)
            .min(total_bytes);
        if canon_end > canon_start {
            canon.push((canon_start, canon_end));
        }
    }
    canon.sort_unstable_by_key(|&(s, _)| s);
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(canon.len());
    for (s, e) in canon.drain(..) {
        if let Some(last) = merged.last_mut()
            && s <= last.1
        {
            last.1 = last.1.max(e);
            continue;
        }
        merged.push((s, e));
    }
    Ok(merged)
}

/// Split the gap-set into up to `n` contiguous bao-aligned segments of roughly
/// equal total size. Never returns an empty segment; may return fewer than `n`
/// when the gap-set is small or a group-aligned split would otherwise degenerate.
///
/// The gap-set is first [canonicalized](canonicalize_ranges) to whole,
/// disjoint, group-aligned spans, and every returned segment lies wholly
/// within one canonical span — so segments are pairwise non-overlapping by
/// construction, never straddling a boundary another segment also claims. A
/// segment may include up to one chunk group of boundary-adjacent bytes the
/// buyer already holds (see [`canonicalize_ranges`]); fetching that group
/// again is redundant but idempotent, never double-counted by the store. The
/// `n` cap is enforced per-canonical-span: once the running count reaches `n`,
/// the rest of the CURRENT span folds into its final segment. A gap-set
/// fragmented into more than `n` disjoint spans therefore yields one segment
/// per remaining span beyond that — exact coverage is never sacrificed to
/// honor the cap exactly.
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
    let canon = canonicalize_ranges(gaps, total_bytes)?;
    let total_gap: u64 = canon.iter().map(|&(s, e)| e - s).sum();
    if total_gap == 0 || n == 0 {
        return Ok(Vec::new());
    }
    let target = total_gap.div_ceil(n as u64).max(1);
    let mut out: Vec<AlignedRange> = Vec::new();
    for &(start, end) in &canon {
        let mut off = start;
        while off < end {
            // Once the segment cap is reached, fold the rest of THIS span (and
            // any later spans) into the final segment so coverage stays exact —
            // never silently drop the remainder.
            let want = if out.len() + 1 >= n {
                end - off
            } else {
                target.min(end - off)
            };
            let seg = align_range(off, want, total_bytes)?;
            // `end` is itself a chunk-group boundary (canonicalize_ranges), so a
            // group-aligned ceiling from `off` can never land past it — this is
            // a defensive clamp, not a correctness-load-bearing one.
            let seg_end = seg.fetch_end().min(end);
            if seg_end <= off {
                // A degenerate zero-width step (would only occur at a span of
                // width 0, which the outer `while off < end` already excludes).
                break;
            }
            let aligned = if seg_end == seg.fetch_end() {
                seg
            } else {
                align_range(off, seg_end - off, total_bytes)?
            };
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
/// The chosen range is first [canonicalized](canonicalize_ranges) to its
/// enclosing group boundaries, so the returned half never rounds up past the
/// range's true end into bytes a neighboring segment already owns — the same
/// hazard [`initial_segments`] guards against, and the same up-to-one-group
/// boundary-adjacent over-fetch trade-off applies here.
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
    let Some((canon_start, canon_end)) = canonicalize_ranges(&[(start, len)], total_bytes)?
        .first()
        .copied()
    else {
        return Ok(None);
    };
    let mid_offset = canon_start + (canon_end - canon_start) / 2;
    // Align the candidate second half against the CANONICAL end, so the
    // ceiling-up align_range performs internally cannot carry it past the
    // range's own true end into a neighboring segment's territory.
    let second_half = align_range(mid_offset, canon_end - mid_offset, total_bytes)?;
    if second_half.fetch_start() <= canon_start || second_half.fetch_start() >= canon_end {
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

    #[test]
    fn initial_segments_two_gaps_separated_by_less_than_one_group_never_overlap()
    -> anyhow::Result<()> {
        // Two gaps separated by 4 KiB of held data — less than the 16 KiB chunk
        // group. Canonicalization must merge them into one span before
        // splitting, so no returned segment can straddle into the other gap's
        // territory and overlap a segment covering it.
        let total = 8 * 1024 * 1024;
        let gap_a = (0, 5 * 1024 * 1024 + 3 * 1024);
        let gap_b = (5 * 1024 * 1024 + 4 * 1024, 1024 * 1024);
        let segs = super::initial_segments(&[gap_a, gap_b], 4, total)?;
        assert!(!segs.is_empty());
        for s in &segs {
            assert_eq!(s.fetch_start() % (16 * 1024), 0);
            assert_eq!(s.fetch_end() % (16 * 1024), 0);
        }
        for w in segs.windows(2) {
            assert!(
                w[1].fetch_start() >= w[0].fetch_end(),
                "segments must not overlap: {:?} then {:?}",
                w[0],
                w[1]
            );
        }
        // Coverage reaches the (group-aligned) end of the canonicalized set.
        let last = segs.last().expect("nonempty");
        let canon_end = (gap_b.0 + gap_b.1).div_ceil(16 * 1024) * (16 * 1024);
        assert_eq!(last.fetch_end(), canon_end.min(total));
        Ok(())
    }

    #[test]
    fn initial_segments_single_unaligned_gap_last_end_is_group_aligned() -> anyhow::Result<()> {
        let total = 16 * 1024 * 1024;
        let gap = (0, 5 * 1024 * 1024 + 3 * 1024);
        let segs = super::initial_segments(&[gap], 4, total)?;
        let last = segs.last().expect("nonempty");
        assert_eq!(last.fetch_end() % (16 * 1024), 0);
        let enclosing_group_end = (gap.0 + gap.1).div_ceil(16 * 1024) * (16 * 1024);
        assert!(last.fetch_end() <= enclosing_group_end.min(total));
        Ok(())
    }

    #[test]
    fn steal_split_second_half_never_exceeds_group_aligned_end_of_source_range()
    -> anyhow::Result<()> {
        // A range with an unaligned end, embedded inside a larger blob so the
        // true end of the range (not the blob) is what must bound the steal.
        let range = (0, 20 * 1024 * 1024 + 3 * 1024);
        let total = 64 * 1024 * 1024;
        let stolen = super::steal_split(&[range], total)?.expect("above floor");
        let enclosing_group_end = (range.0 + range.1).div_ceil(16 * 1024) * (16 * 1024);
        assert!(stolen.fetch_end() <= enclosing_group_end.min(total));
        Ok(())
    }

    #[test]
    fn initial_segments_rejects_a_gap_starting_at_or_past_total_bytes() {
        let total = 4 * 1024 * 1024;
        // The gap's start is exactly at the blob's end — entirely out of bounds.
        let result = super::initial_segments(&[(total, 4096)], 4, total);
        assert!(
            result.is_err(),
            "gap starting at total_bytes must be rejected, not dropped"
        );
    }

    #[test]
    fn initial_segments_rejects_a_gap_extending_past_total_bytes() {
        let total = 4 * 1024 * 1024;
        // The gap starts in-bounds but its raw (pre-rounding) end overruns the blob.
        let result = super::initial_segments(&[(total - 1024, 4096)], 4, total);
        assert!(
            result.is_err(),
            "gap whose raw end exceeds total_bytes must be rejected, not truncated"
        );
    }

    #[test]
    fn initial_segments_accepts_a_legitimate_final_partial_group_gap() -> anyhow::Result<()> {
        // The gap's raw end exactly equals total_bytes, so rounding UP to the
        // next group boundary and then clamping to total_bytes is the blob's
        // own partial last group — legitimate, not out-of-bounds.
        let total = 5 * 1024 * 1024 + 3 * 1024;
        let segs = super::initial_segments(&[(0, total)], 4, total)?;
        let last = segs.last().expect("nonempty");
        assert_eq!(last.fetch_end(), total);
        Ok(())
    }

    #[test]
    fn steal_split_rejects_a_range_starting_at_or_past_total_bytes() {
        let total = 4 * 1024 * 1024;
        let result = super::steal_split(&[(total, 16 * 1024 * 1024)], total);
        assert!(
            result.is_err(),
            "range starting at total_bytes must be rejected, not dropped"
        );
    }

    #[test]
    fn steal_split_rejects_a_range_extending_past_total_bytes() {
        let total = 20 * 1024 * 1024;
        let result = super::steal_split(&[(total - 1024, 16 * 1024 * 1024)], total);
        assert!(
            result.is_err(),
            "range whose raw end exceeds total_bytes must be rejected, not truncated"
        );
    }
}
