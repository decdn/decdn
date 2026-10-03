//! Pure byte-range helpers for the multi-source scheduler (ADR 039 § Dynamic
//! segmentation and tail-stealing, #1506).
//! No I/O, no async. WHICH source gets WHICH discovery block is the coverage
//! planner's job ([`crate::coverage_plan::spread_segments`]); this module only
//! turns one planner-assigned run into fetchable `AlignedRange`s
//! (`split_evenly`) and picks-and-splits the missing remainder of the range in
//! flight whose covered tail misses the most, for a freed source to steal
//! (`steal_split`). Every returned range is
//! chunk-group aligned so it is independently bao-verifiable.

use decdn_bao_range::{AlignedRange, CHUNK_GROUP_BYTES, RangeVerifyError, align_range};

/// Floor below which an idle source does not split/steal a remaining range —
/// no fresh stream for a tail smaller than this (ADR 039 § Parameters).
pub(crate) const MIN_SPLIT_SIZE: u64 = 16 * 1024 * 1024;

/// The fewest missing bytes a steal leaves its victim, one ingest checkpoint
/// interval. The victim streams on while the steal runs, so a split right at
/// its received frontier could fall behind bytes it takes in before it reads
/// the lowered end.
pub(crate) const MIN_VICTIM_KEEP: u64 = crate::ClientRangedStore::INGEST_CHECKPOINT_BYTES;

/// The fewest missing bytes a steal takes, half of [`MIN_SPLIT_SIZE`]: the
/// smallest tail an even split above the floor hands out. A fresh paid stream
/// for fewer bytes costs its handshake and payment ramp for almost nothing.
pub(crate) const MIN_STOLEN: u64 = MIN_SPLIT_SIZE / 2;

// Both floors fit in the smallest remainder a steal splits.
const _: () = assert!(MIN_VICTIM_KEEP + MIN_STOLEN <= MIN_SPLIT_SIZE);

/// The bytes of `left` missing bytes a steal's victim keeps, when the stealer
/// runs at `stealer` and the victim at `victim` bytes per second:
/// `left * victim / (stealer + victim)`, so both finish at about the same
/// time. When either side has no non-zero rate, the victim keeps half. `None`
/// when the stealer's share by rate is below [`MIN_STOLEN`]: the victim
/// finishes that tail sooner than a fresh stream would.
fn victim_keeps(left: u64, stealer: Option<u64>, victim: Option<u64>) -> Option<u64> {
    match (stealer, victim) {
        (Some(stealer), Some(victim)) if stealer > 0 && victim > 0 => {
            let kept =
                u128::from(left) * u128::from(victim) / (u128::from(stealer) + u128::from(victim));
            let kept = u64::try_from(kept).unwrap_or(left);
            (left.saturating_sub(kept) >= MIN_STOLEN).then_some(kept)
        }
        _ => Some(left / 2),
    }
}

/// `runs` with every byte of `cut` taken out. Both hold `(start, len)` byte
/// runs; the result stays ascending and disjoint when `runs` is.
pub(crate) fn without_runs(runs: &[(u64, u64)], cut: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut out = Vec::with_capacity(runs.len());
    for &(start, len) in runs {
        let mut pieces = vec![(start, start.saturating_add(len))];
        for &(cut_start, cut_len) in cut {
            let cut_end = cut_start.saturating_add(cut_len);
            pieces = pieces
                .into_iter()
                .flat_map(|(s, e)| [(s, e.min(cut_start)), (s.max(cut_end), e)])
                .filter(|&(s, e)| s < e)
                .collect();
        }
        out.extend(pieces.into_iter().map(|(s, e)| (s, e - s)));
    }
    out
}

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

/// The `(start, end)` spans of `missing` that fall inside `[start, end)`,
/// clipped to it. `missing` is ascending and disjoint `(start, len)` runs.
fn missing_within(missing: &[(u64, u64)], start: u64, end: u64) -> Vec<(u64, u64)> {
    missing
        .iter()
        .map(|&(s, l)| (s.max(start), s.saturating_add(l).min(end)))
        .filter(|&(s, e)| s < e)
        .collect()
}

/// Among the remaining ranges that miss at least [`MIN_SPLIT_SIZE`] and whose
/// covered suffix misses at least [`MIN_STOLEN`], pick the one whose covered
/// suffix misses the most, and return its index in `remaining` together with
/// the aligned tail of its MISSING bytes for a freed source to steal. Returns
/// `None` when no remaining range qualifies, including when the source covers
/// the last block of no remaining range (the freed source parks rather than
/// stealing bytes it cannot serve, #1506).
///
/// `remaining` holds the ranges in flight as picked, and `missing` the byte
/// runs of the blob the store still misses, ascending and disjoint. A range in
/// flight is delivered front to back, so the argmax and the split both count
/// its missing bytes: the victim keeps the front of its remainder and still
/// has work after the steal, so it has no reason to steal back.
///
/// The split weighs the remainder by the two sources' observed rates, the
/// freed source's `stealer_rate` and `victim_rate(index)`: the victim keeps
/// `r_v / (r_s + r_v)` of the missing bytes of the whole range, so at steady
/// rates one steal leaves both sources finishing together. When either side
/// has no non-zero rate, the victim keeps half. A steal whose share by rate is
/// below [`MIN_STOLEN`] is declined. The victim keeps at least
/// [`MIN_VICTIM_KEEP`].
/// The split point is the offset with the
/// kept bytes before it, rounded down to a chunk group. The returned tail
/// runs from there to the range's end. A split that would leave the victim no
/// missing byte declines the steal: that steal takes the victim's whole
/// remaining work.
///
/// `covered_from(start, len)` gives the start of the longest suffix of a
/// remaining range that the freed source covers, or `None` when it does not
/// cover the range's last byte (#2303). A victim streams front to back, so a
/// steal only ever takes a suffix: a split before that start moves to it, and
/// the victim keeps every byte before it, even past its share by rate. The
/// argmax counts only the missing bytes in that suffix, so a source with
/// narrow coverage never picks a huge range for bytes it cannot deliver.
///
/// The INDEX is returned, not just the tail, so the caller trims the range this
/// function actually split. Re-deriving the argmax at the call site couples two
/// modules to one tie-breaking rule with no compiler support: a caller that
/// picked a different maximum would trim and stop one source while a second
/// keeps streaming, and paying for, the tail this function just handed away.
///
/// The chosen range and its missing runs are first
/// [canonicalized](canonicalize_ranges) to their enclosing group boundaries,
/// so the returned tail never rounds up past the range's true end into bytes a
/// neighboring segment already owns.
///
/// # Errors
///
/// Propagates [`decdn_bao_range::RangeVerifyError`] for a range out of bounds
/// against `total_bytes`, raised by [`canonicalize_ranges`], which runs before
/// `align_range` ever sees the range.
pub(crate) fn steal_split(
    remaining: &[(u64, u64)],
    missing: &[(u64, u64)],
    total_bytes: u64,
    covered_from: impl Fn(u64, u64) -> Option<u64>,
    stealer_rate: Option<u64>,
    victim_rate: impl Fn(usize) -> Option<u64>,
) -> anyhow::Result<Option<(usize, AlignedRange)>> {
    let missing_of = |start: u64, end: u64| {
        missing_within(missing, start, end)
            .iter()
            .map(|&(s, e)| e - s)
            .fold(0, u64::saturating_add)
    };
    let Some((victim, &(start, len), from)) = remaining
        .iter()
        .enumerate()
        .filter_map(|(i, r @ &(s, l))| {
            let from = covered_from(s, l)?;
            let end = s.saturating_add(l);
            let stealable = missing_of(from, end);
            (missing_of(s, end) >= MIN_SPLIT_SIZE && stealable >= MIN_STOLEN)
                .then_some((i, r, from, stealable))
        })
        .max_by_key(|&(_, _, _, stealable)| stealable)
        .map(|(i, r, from, _)| (i, r, from))
    else {
        return Ok(None);
    };
    let Some((canon_start, canon_end)) = canonicalize_ranges(&[(start, len)], total_bytes)?
        .first()
        .copied()
    else {
        return Ok(None);
    };
    let runs: Vec<(u64, u64)> = canonicalize_ranges(
        &missing_within(missing, canon_start, canon_end)
            .iter()
            .map(|&(s, e)| (s, e - s))
            .collect::<Vec<_>>(),
        total_bytes,
    )?;
    let Some(&(first_missing, _)) = runs.first() else {
        return Ok(None);
    };
    let run_bytes = runs
        .iter()
        .map(|&(s, e)| e - s)
        .fold(0, u64::saturating_add);
    let Some(keep) = victim_keeps(run_bytes, stealer_rate, victim_rate(victim)) else {
        return Ok(None);
    };
    let keep = keep
        .max(MIN_VICTIM_KEEP)
        .min(run_bytes.saturating_sub(MIN_STOLEN));
    let mut before = 0u64;
    let mut split = None;
    for &(s, e) in &runs {
        if before.saturating_add(e - s) > keep {
            split = Some(s + (keep - before));
            break;
        }
        before = before.saturating_add(e - s);
    }
    let Some(split) = split else {
        return Ok(None);
    };
    // The freed source covers only `[from, end)`: move the split up to
    // `from`, so the victim keeps every byte before it.
    let split = split.max(from);
    if split >= canon_end {
        return Ok(None);
    }
    // Align the candidate tail against the CANONICAL end, so the ceiling-up
    // align_range performs internally cannot carry it past the range's own
    // true end into a neighboring segment's territory.
    let tail = align_range(split, canon_end - split, total_bytes)?;
    // The victim keeps `[canon_start, fetch_start)`: decline unless that part
    // still misses a byte.
    if tail.fetch_start() <= first_missing || tail.fetch_start() >= canon_end {
        return Ok(None);
    }
    Ok(Some((victim, tail)))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests
mod tests {
    /// A store that misses every byte: each range in flight is all missing.
    const ALL: &[(u64, u64)] = &[(0, u64::MAX)];
    const MIB: u64 = 1024 * 1024;

    #[test]
    fn steal_split_splits_the_missing_remainder_not_the_picked_range() -> anyhow::Result<()> {
        // The victim picked [0, 64 MiB) and delivered up to 40 MiB: the thief
        // takes the second half of the missing [40, 64 MiB), and the victim
        // keeps [40, 52 MiB).
        let (victim, stolen) = super::steal_split(
            &[(0, 64 * MIB)],
            &[(40 * MIB, 24 * MIB)],
            128 * MIB,
            |s, _| Some(s),
            None,
            |_| None,
        )?
        .expect("24 MiB missing clears the floor");
        assert_eq!(victim, 0);
        assert_eq!(stolen.fetch_start(), 52 * MIB);
        assert_eq!(stolen.fetch_end(), 64 * MIB);
        Ok(())
    }

    #[test]
    fn steal_split_picks_the_range_that_misses_the_most() -> anyhow::Result<()> {
        // The 64 MiB range misses 4 MiB; the 32 MiB range misses all of it.
        let (victim, stolen) = super::steal_split(
            &[(0, 64 * MIB), (64 * MIB, 32 * MIB)],
            &[(60 * MIB, 36 * MIB)],
            96 * MIB,
            |s, _| Some(s),
            None,
            |_| None,
        )?
        .expect("the 32 MiB range clears the floor");
        assert_eq!(victim, 1);
        assert_eq!(stolen.fetch_start(), 80 * MIB);
        Ok(())
    }

    #[test]
    fn steal_split_declines_a_long_range_with_a_small_missing_remainder() -> anyhow::Result<()> {
        // 64 MiB picked, 4 MiB missing: below the floor however long the pick.
        assert!(
            super::steal_split(
                &[(0, 64 * MIB)],
                &[(60 * MIB, 4 * MIB)],
                64 * MIB,
                |s, _| Some(s),
                None,
                |_| None
            )?
            .is_none()
        );
        Ok(())
    }

    #[test]
    fn steal_split_halves_missing_bytes_across_holes() -> anyhow::Result<()> {
        // 24 MiB missing in two runs: the victim keeps 12 MiB, all of the
        // first run and 4 MiB of the second.
        let (_, stolen) = super::steal_split(
            &[(0, 64 * MIB)],
            &[(0, 8 * MIB), (32 * MIB, 16 * MIB)],
            64 * MIB,
            |s, _| Some(s),
            None,
            |_| None,
        )?
        .expect("24 MiB missing clears the floor");
        assert_eq!(stolen.fetch_start(), 36 * MIB);
        assert_eq!(stolen.fetch_end(), 64 * MIB);
        Ok(())
    }

    #[test]
    fn steal_split_weighs_the_remainder_by_the_two_rates() -> anyhow::Result<()> {
        // 40 MiB missing; the stealer runs 3x the victim's rate, so the victim
        // keeps a quarter.
        let (_, stolen) = super::steal_split(
            &[(0, 64 * MIB)],
            &[(24 * MIB, 40 * MIB)],
            64 * MIB,
            |s, _| Some(s),
            Some(3 * MIB),
            |_| Some(MIB),
        )?
        .expect("40 MiB missing clears the floor");
        assert_eq!(stolen.fetch_start(), 34 * MIB);
        assert_eq!(stolen.fetch_end(), 64 * MIB);
        Ok(())
    }

    #[test]
    fn steal_split_halves_without_a_rate_on_both_sides() -> anyhow::Result<()> {
        for (stealer, victim) in [(None, Some(MIB)), (Some(MIB), None), (Some(MIB), Some(0))] {
            let (_, stolen) = super::steal_split(
                &[(0, 64 * MIB)],
                &[(24 * MIB, 40 * MIB)],
                64 * MIB,
                |s, _| Some(s),
                stealer,
                |_| victim,
            )?
            .expect("40 MiB missing clears the floor");
            assert_eq!(stolen.fetch_start(), 44 * MIB, "{stealer:?} / {victim:?}");
        }
        Ok(())
    }

    #[test]
    fn steal_split_leaves_a_slow_victim_one_checkpoint_and_declines_a_tiny_share()
    -> anyhow::Result<()> {
        let steal = |stealer: u64, victim: u64| {
            super::steal_split(
                &[(0, 64 * MIB)],
                &[(0, 32 * MIB)],
                64 * MIB,
                |s, _| Some(s),
                Some(stealer),
                |_| Some(victim),
            )
        };
        // A victim at a thousandth of the stealer's rate still keeps one
        // ingest checkpoint.
        let (_, stolen) = steal(1000, 1)?.expect("a slow victim is stolen from");
        assert_eq!(stolen.fetch_start(), super::MIN_VICTIM_KEEP);
        // A victim far faster than the stealer would finish the stealer's
        // share sooner than a fresh stream: no steal.
        assert!(steal(1, 1_000_000)?.is_none());
        Ok(())
    }

    #[test]
    fn steal_split_takes_the_covered_tail_of_a_partly_covered_range() -> anyhow::Result<()> {
        // #2303: the victim holds [0, 44 MiB) and the stealer covers only
        // [30, 44 MiB). The even split (22 MiB) lies before the covered
        // suffix, so the stealer takes the whole suffix and the victim keeps
        // [0, 30 MiB).
        let (victim, stolen) = super::steal_split(
            &[(0, 44 * MIB)],
            ALL,
            64 * MIB,
            |_, _| Some(30 * MIB),
            None,
            |_| None,
        )?
        .expect("14 MiB of covered tail clears MIN_STOLEN");
        assert_eq!(victim, 0);
        assert_eq!(stolen.fetch_start(), 30 * MIB);
        assert_eq!(stolen.fetch_end(), 44 * MIB);
        Ok(())
    }

    #[test]
    fn steal_split_splits_by_rate_inside_the_covered_tail() -> anyhow::Result<()> {
        // The covered suffix starts at 18 MiB, before the even split at
        // 22 MiB: the split by rate stands.
        let (_, stolen) = super::steal_split(
            &[(0, 44 * MIB)],
            ALL,
            64 * MIB,
            |_, _| Some(18 * MIB),
            None,
            |_| None,
        )?
        .expect("44 MiB missing clears the floor");
        assert_eq!(stolen.fetch_start(), 22 * MIB);
        assert_eq!(stolen.fetch_end(), 44 * MIB);
        Ok(())
    }

    #[test]
    fn steal_split_declines_when_the_covered_tail_is_below_min_stolen() -> anyhow::Result<()> {
        // Only [40, 44 MiB) is covered: 4 MiB is too small for a fresh stream.
        assert!(
            super::steal_split(
                &[(0, 44 * MIB)],
                ALL,
                64 * MIB,
                |_, _| Some(40 * MIB),
                None,
                |_| None,
            )?
            .is_none()
        );
        // A larger range whose covered tail is too small does not qualify, so
        // a smaller range the stealer covers in full is the victim.
        let (victim, _) = super::steal_split(
            &[(0, 44 * MIB), (44 * MIB, 20 * MIB)],
            ALL,
            64 * MIB,
            |s, _| Some(if s == 0 { 40 * MIB } else { s }),
            None,
            |_| None,
        )?
        .expect("the 20 MiB range is covered in full");
        assert_eq!(victim, 1);
        Ok(())
    }

    #[test]
    fn without_runs_takes_out_every_cut_byte() {
        assert_eq!(
            super::without_runs(&[(0, 100), (200, 100)], &[(50, 20), (190, 30), (290, 50)]),
            vec![(0, 50), (70, 30), (220, 70)]
        );
        assert_eq!(super::without_runs(&[(0, 100)], &[]), vec![(0, 100)]);
        assert!(super::without_runs(&[(10, 10)], &[(0, 100)]).is_empty());
    }

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
    fn steal_split_ranks_by_the_covered_tail_not_the_whole_range() -> anyhow::Result<()> {
        // The 44 MiB range misses more in all (44 MiB), but only 14 MiB of
        // it lies in the stealer's covered suffix; the 20 MiB range is
        // covered in full, so it has more to steal.
        let (victim, stolen) = super::steal_split(
            &[(0, 44 * MIB), (44 * MIB, 20 * MIB)],
            ALL,
            64 * MIB,
            |s, _| Some(if s == 0 { 30 * MIB } else { s }),
            None,
            |_| None,
        )?
        .expect("both ranges qualify");
        assert_eq!(victim, 1);
        assert_eq!(stolen.fetch_start(), 54 * MIB);
        Ok(())
    }

    #[test]
    fn steal_split_takes_second_half_of_largest_above_floor() -> anyhow::Result<()> {
        let total = 100 * 1024 * 1024;
        // Largest remaining is 40 MiB at offset 10 MiB; steal its aligned second half.
        let stolen = super::steal_split(
            &[(0, 4 * 1024 * 1024), (10 * 1024 * 1024, 40 * 1024 * 1024)],
            ALL,
            total,
            |s, _| Some(s),
            None,
            |_| None,
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
        assert!(
            super::steal_split(
                &[(0, 8 * 1024 * 1024)],
                ALL,
                total,
                |s, _| Some(s),
                None,
                |_| None
            )?
            .is_none()
        );
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
            ALL,
            total,
            |s, _| (s != accepted_start).then_some(s),
            None,
            |_| None,
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
        assert!(
            super::steal_split(
                &[(0, 40 * 1024 * 1024)],
                ALL,
                total,
                |_, _| None,
                None,
                |_| None
            )?
            .is_none()
        );
        Ok(())
    }

    #[test]
    fn steal_split_second_half_never_exceeds_group_aligned_end_of_source_range()
    -> anyhow::Result<()> {
        // A range with an unaligned end, embedded inside a larger blob so the
        // true end of the range (not the blob) is what must bound the steal.
        let range = (0, 20 * 1024 * 1024 + 3 * 1024);
        let total = 64 * 1024 * 1024;
        let (_, stolen) = super::steal_split(&[range], ALL, total, |s, _| Some(s), None, |_| None)?
            .expect("above floor");
        let enclosing_group_end = (range.0 + range.1).div_ceil(16 * 1024) * (16 * 1024);
        assert!(stolen.fetch_end() <= enclosing_group_end.min(total));
        Ok(())
    }

    #[test]
    fn steal_split_rejects_a_range_starting_at_or_past_total_bytes() {
        let total = 4 * 1024 * 1024;
        let result = super::steal_split(
            &[(total, 16 * 1024 * 1024)],
            ALL,
            total,
            |s, _| Some(s),
            None,
            |_| None,
        );
        assert!(
            result.is_err(),
            "range starting at total_bytes must be rejected, not dropped"
        );
    }

    #[test]
    fn steal_split_rejects_a_range_extending_past_total_bytes() {
        let total = 20 * 1024 * 1024;
        let result = super::steal_split(
            &[(total - 1024, 16 * 1024 * 1024)],
            ALL,
            total,
            |s, _| Some(s),
            None,
            |_| None,
        );
        assert!(
            result.is_err(),
            "range whose raw end exceeds total_bytes must be rejected, not truncated"
        );
    }
}
