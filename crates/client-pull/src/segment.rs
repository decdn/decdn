//! Pure byte-range helpers for the multi-source scheduler (spec §5.3, #1506).
//! No I/O, no async. WHICH source gets WHICH discovery block is the coverage
//! planner's job ([`crate::coverage_plan::spread_segments`]); this module only
//! turns one planner-assigned run into fetchable `AlignedRange`s
//! (`split_evenly`) and picks-and-splits the largest remaining range a freed
//! source covers, for it to steal (`steal_split`). Every returned range is
//! chunk-group aligned so it is independently bao-verifiable.

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
/// [`steal_split`] sees one consistent error type regardless of which stage
/// rejected the input.
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

/// Split `[start, start + len)` into up to `k` roughly-equal, chunk-group
/// aligned pieces. Never returns an empty piece; may return fewer than `k`
/// when the span is small or a group-aligned split would otherwise degenerate.
///
/// The coverage planner ([`crate::coverage_plan::spread_segments`]) assigns
/// whole 64 MiB discovery blocks, one run per source — coarser than this. When
/// `k` sources all cover the SAME run whole (the planner had to pick just one
/// of them, e.g. a blob no bigger than one discovery block, so several full
/// holders tie on it), splitting the run further here — with NO
/// [`MIN_SPLIT_SIZE`] floor, unlike [`steal_split`] — is what keeps every one
/// of them fetching from the start instead of sitting idle. The caller is
/// responsible for choosing `k` no larger than the number of sources that
/// actually cover the whole `[start, start + len)` span; splitting past that
/// would hand a piece to a source the scheduler's own coverage filter
/// (`Work::pick`) would then have to refuse.
///
/// # Errors
///
/// Propagates [`decdn_bao_range::RangeVerifyError`] from `align_range` (an
/// out-of-bounds span against `total_bytes`).
pub(crate) fn split_evenly(
    start: u64,
    len: u64,
    k: usize,
    total_bytes: u64,
) -> anyhow::Result<Vec<AlignedRange>> {
    if len == 0 || k == 0 {
        return Ok(Vec::new());
    }
    let Some(end) = start.checked_add(len) else {
        anyhow::bail!("split_evenly: start + len overflows for start={start} len={len}");
    };
    let target = len.div_ceil(k as u64).max(1);
    let mut out: Vec<AlignedRange> = Vec::new();
    let mut off = start;
    while off < end {
        // Once the piece cap is reached, fold the remainder into the final
        // piece so coverage stays exact — never silently drop the remainder.
        let want = if out.len() + 1 >= k {
            end - off
        } else {
            target.min(end - off)
        };
        let seg = align_range(off, want, total_bytes)?;
        let seg_end = seg.fetch_end().min(end);
        if seg_end <= off {
            // A degenerate zero-width step (only possible at a zero-width
            // span, which the outer `while off < end` already excludes).
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
    Ok(out)
}

/// Pick the largest remaining range this source COVERS and, if it is at least
/// [`MIN_SPLIT_SIZE`], return its index in `remaining` together with its
/// aligned second half for a freed source to steal. Returns `None` when
/// nothing remaining — among the ranges `covers` accepts — is worth a fresh
/// stream, including when `covers` accepts nothing at all (the freed source
/// parks rather than stealing a range it cannot serve, #1506).
///
/// `covers(start, len)` is the freed source's coverage predicate: a candidate
/// range is a steal target only when it returns `true` for it. Filtering
/// happens BEFORE the largest-range argmax, so a source with narrow coverage
/// never steals a huge range it cannot fully deliver just because it happens
/// to be the biggest one in flight.
///
/// The INDEX is returned, not just the half, so the caller trims the range this
/// function actually split. Re-deriving the argmax at the call site couples two
/// modules to one tie-breaking rule with no compiler support: a caller that
/// picked a different maximum would trim and cancel one source while a second
/// keeps streaming — and paying for — the tail this half just handed away.
///
/// The chosen range is first [canonicalized](canonicalize_ranges) to its
/// enclosing group boundaries, so the returned half never rounds up past the
/// range's true end into bytes a neighboring segment already owns.
///
/// # Errors
///
/// Propagates [`decdn_bao_range::RangeVerifyError`] for a range out of bounds
/// against `total_bytes` — raised by [`canonicalize_ranges`], which runs before
/// `align_range` ever sees the range.
pub(crate) fn steal_split(
    remaining: &[(u64, u64)],
    total_bytes: u64,
    covers: impl Fn(u64, u64) -> bool,
) -> anyhow::Result<Option<(usize, AlignedRange)>> {
    let Some((victim, &(start, len))) = remaining
        .iter()
        .enumerate()
        .filter(|&(_, &(s, l))| covers(s, l))
        .max_by_key(|&(_, &(_, len))| len)
    else {
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
    Ok(Some((victim, second_half)))
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
    fn split_evenly_splits_a_span_into_k_aligned_pieces_below_no_size_floor() -> anyhow::Result<()>
    {
        // 8 MiB is far below `MIN_SPLIT_SIZE` (16 MiB) — `split_evenly` has no
        // such floor, unlike `steal_split`, so it still splits.
        let total = 8 * 1024 * 1024;
        let segs = super::split_evenly(0, total, 2, total)?;
        assert_eq!(segs.len(), 2);
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
    fn split_evenly_caps_at_the_span_size() -> anyhow::Result<()> {
        // A single 8 MiB span with k=4 yields at most pieces sized >= one
        // group each, never more pieces than make sense; every piece is
        // non-empty.
        let segs = super::split_evenly(0, 8 * 1024 * 1024, 4, 8 * 1024 * 1024)?;
        assert!(!segs.is_empty() && segs.len() <= 4);
        assert!(segs.iter().all(|s| s.fetch_end() > s.fetch_start()));
        Ok(())
    }

    #[test]
    fn split_evenly_k_one_returns_the_whole_span_as_one_piece() -> anyhow::Result<()> {
        let total = 20 * 1024 * 1024;
        let segs = super::split_evenly(0, total, 1, total)?;
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].fetch_start(), 0);
        assert_eq!(segs[0].fetch_end(), total);
        Ok(())
    }

    #[test]
    fn split_evenly_zero_len_or_zero_k_returns_nothing() -> anyhow::Result<()> {
        let total = 20 * 1024 * 1024;
        assert!(super::split_evenly(0, 0, 4, total)?.is_empty());
        assert!(super::split_evenly(0, total, 0, total)?.is_empty());
        Ok(())
    }

    #[test]
    fn steal_split_takes_second_half_of_largest_above_floor() -> anyhow::Result<()> {
        let total = 100 * 1024 * 1024;
        // Largest remaining is 40 MiB at offset 10 MiB; steal its aligned second half.
        let stolen = super::steal_split(
            &[(0, 4 * 1024 * 1024), (10 * 1024 * 1024, 40 * 1024 * 1024)],
            total,
            |_, _| true,
        )?
        .expect("above floor");
        let (victim, stolen) = stolen;
        // The 40 MiB range at index 1 is the one split — the caller trims THAT
        // one, never a re-derived argmax of its own.
        assert_eq!(victim, 1);
        assert!(stolen.fetch_start() > 10 * 1024 * 1024);
        assert_eq!(stolen.fetch_end(), 50 * 1024 * 1024);
        assert_eq!(stolen.fetch_start() % (16 * 1024), 0);
        Ok(())
    }

    #[test]
    fn steal_split_returns_none_below_floor() -> anyhow::Result<()> {
        let total = 100 * 1024 * 1024;
        // Largest remaining is 8 MiB < 16 MiB floor -> don't steal.
        assert!(super::steal_split(&[(0, 8 * 1024 * 1024)], total, |_, _| true)?.is_none());
        Ok(())
    }

    #[test]
    fn steal_split_declines_a_range_the_predicate_rejects_even_when_it_is_the_largest()
    -> anyhow::Result<()> {
        let total = 100 * 1024 * 1024;
        // The 40 MiB range is by far the largest, but the predicate rejects it
        // (models a freed source whose coverage does not include it) — the 20
        // MiB range is the largest ACCEPTED one and must be the one split.
        let accepted_start = 60 * 1024 * 1024;
        let stolen = super::steal_split(
            &[
                (0, 20 * 1024 * 1024),
                (accepted_start, 40 * 1024 * 1024),
                (10 * 1024 * 1024, 4 * 1024 * 1024),
            ],
            total,
            |s, _| s != accepted_start,
        )?
        .expect("the 20 MiB range clears the floor and is accepted");
        let (victim, _) = stolen;
        assert_eq!(
            victim, 0,
            "the accepted 20 MiB range, not the rejected 40 MiB one"
        );
        Ok(())
    }

    #[test]
    fn steal_split_returns_none_when_predicate_accepts_nothing() -> anyhow::Result<()> {
        let total = 100 * 1024 * 1024;
        // A single large, otherwise-stealable range, but the predicate covers
        // nothing — the freed source parks rather than stealing what it cannot
        // serve.
        assert!(super::steal_split(&[(0, 40 * 1024 * 1024)], total, |_, _| false)?.is_none());
        Ok(())
    }

    #[test]
    fn steal_split_second_half_never_exceeds_group_aligned_end_of_source_range()
    -> anyhow::Result<()> {
        // A range with an unaligned end, embedded inside a larger blob so the
        // true end of the range (not the blob) is what must bound the steal.
        let range = (0, 20 * 1024 * 1024 + 3 * 1024);
        let total = 64 * 1024 * 1024;
        let (_, stolen) = super::steal_split(&[range], total, |_, _| true)?.expect("above floor");
        let enclosing_group_end = (range.0 + range.1).div_ceil(16 * 1024) * (16 * 1024);
        assert!(stolen.fetch_end() <= enclosing_group_end.min(total));
        Ok(())
    }

    #[test]
    fn steal_split_rejects_a_range_starting_at_or_past_total_bytes() {
        let total = 4 * 1024 * 1024;
        let result = super::steal_split(&[(total, 16 * 1024 * 1024)], total, |_, _| true);
        assert!(
            result.is_err(),
            "range starting at total_bytes must be rejected, not dropped"
        );
    }

    #[test]
    fn steal_split_rejects_a_range_extending_past_total_bytes() {
        let total = 20 * 1024 * 1024;
        let result = super::steal_split(&[(total - 1024, 16 * 1024 * 1024)], total, |_, _| true);
        assert!(
            result.is_err(),
            "range whose raw end exceeds total_bytes must be rejected, not truncated"
        );
    }
}
