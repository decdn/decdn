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
fn steal_split_leaves_a_slow_victim_one_checkpoint_and_declines_a_tiny_share() -> anyhow::Result<()>
{
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
fn split_evenly_splits_a_span_into_k_aligned_pieces_below_no_size_floor() -> anyhow::Result<()> {
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
fn steal_split_second_half_never_exceeds_group_aligned_end_of_source_range() -> anyhow::Result<()> {
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
