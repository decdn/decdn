use super::*;
use decdn_protocol::DISCOVERY_BLOCK_BYTES;

fn cov(num_blocks: u32, blocks: &[u32]) -> Coverage {
    Coverage::from_block_indices(num_blocks, blocks.iter().copied())
}

fn whole_gap(total_bytes: u64) -> ChunkRanges {
    ChunkRanges::from(ChunkNum(0)..ChunkNum(total_bytes.div_ceil(BAO_CHUNK_BYTES)))
}

#[test]
fn covering_sources_picks_best_ranked() {
    let sources = vec![
        SourceCoverage {
            source_ix: 0,
            coverage: cov(2, &[0, 1]),
        },
        SourceCoverage {
            source_ix: 1,
            coverage: cov(2, &[0, 1]),
        },
    ];
    // Rank B (1) ahead of A (0): B wins even though A is index 0.
    assert_eq!(covering_sources(0, &sources, &[1, 0]), Some(1));
    assert_eq!(covering_sources(0, &sources, &[0, 1]), Some(0));
}

#[test]
fn covers_byte_range_true_only_when_every_intersected_block_is_covered() {
    let coverage = cov(3, &[0, 1]);
    // Wholly inside block 0: covered.
    assert!(covers_byte_range(
        &coverage,
        0,
        DISCOVERY_BLOCK_BYTES / 2,
        3 * DISCOVERY_BLOCK_BYTES
    ));
    // Spans blocks 0 and 1, both covered.
    assert!(covers_byte_range(
        &coverage,
        0,
        2 * DISCOVERY_BLOCK_BYTES,
        3 * DISCOVERY_BLOCK_BYTES
    ));
    // Spans blocks 1 and 2; block 2 is NOT covered, so the whole range is
    // rejected even though most of it is covered.
    assert!(!covers_byte_range(
        &coverage,
        DISCOVERY_BLOCK_BYTES,
        2 * DISCOVERY_BLOCK_BYTES,
        3 * DISCOVERY_BLOCK_BYTES
    ));
    // A zero-length range is trivially covered.
    assert!(covers_byte_range(
        &coverage,
        0,
        0,
        3 * DISCOVERY_BLOCK_BYTES
    ));
}

#[test]
fn covered_suffix_start_is_the_start_of_the_trailing_covered_run() {
    const B: u64 = DISCOVERY_BLOCK_BYTES;
    let total = 6 * B;
    // Blocks 0, 2, 3 and 4 covered; 1 and 5 not.
    let coverage = cov(6, &[0, 2, 3, 4]);
    // A range it holds in full gives its own start.
    assert_eq!(
        covered_suffix_start(&coverage, 2 * B, 3 * B, total),
        Some(2 * B)
    );
    // Block 1 is a hole: the suffix starts at block 2.
    assert_eq!(
        covered_suffix_start(&coverage, 0, 5 * B, total),
        Some(2 * B)
    );
    // The block of the last byte is not covered: nothing to take.
    assert_eq!(covered_suffix_start(&coverage, 0, 6 * B, total), None);
    // A start inside a covered block stays the start.
    assert_eq!(
        covered_suffix_start(&coverage, 2 * B + 4096, 2 * B, total),
        Some(2 * B + 4096)
    );
    // A zero-length range gives its start.
    assert_eq!(covered_suffix_start(&coverage, B, 0, total), Some(B));
    // A covered run that reaches block 0 gives offset 0.
    assert_eq!(
        covered_suffix_start(&cov(6, &[0, 1, 2, 3, 4, 5]), 0, total, total),
        Some(0)
    );
    // A partial last block: the suffix is that block.
    let short = 2 * B + 5 * 1024 * 1024;
    assert_eq!(
        covered_suffix_start(&cov(3, &[2]), 0, short, short),
        Some(2 * B)
    );
}

#[test]
fn covering_sources_none_when_nobody_covers() {
    let sources = vec![SourceCoverage {
        source_ix: 0,
        coverage: cov(2, &[0]),
    }];
    assert_eq!(covering_sources(1, &sources, &[0]), None);
}

#[test]
fn concentrate_forces_one_flip_at_the_coverage_boundary() {
    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let sources = vec![
        SourceCoverage {
            source_ix: 0,
            coverage: cov(2, &[0]),
        },
        SourceCoverage {
            source_ix: 1,
            coverage: cov(2, &[1]),
        },
    ];
    let (runs, uncovered) = plan_covered_runs(&whole_gap(total), total, &sources, &[0, 1]);
    assert_eq!(
        runs,
        vec![
            CoveredRun {
                offset: 0,
                len: DISCOVERY_BLOCK_BYTES,
                source_ix: 0,
            },
            CoveredRun {
                offset: DISCOVERY_BLOCK_BYTES,
                len: DISCOVERY_BLOCK_BYTES,
                source_ix: 1,
            },
        ]
    );
    assert!(uncovered.is_empty());
}

#[test]
fn concentrate_single_source_covering_three_blocks_is_one_run() {
    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let sources = vec![SourceCoverage {
        source_ix: 0,
        coverage: cov(3, &[0, 1, 2]),
    }];
    let (runs, uncovered) = plan_covered_runs(&whole_gap(total), total, &sources, &[0]);
    assert_eq!(
        runs,
        vec![CoveredRun {
            offset: 0,
            len: 3 * DISCOVERY_BLOCK_BYTES,
            source_ix: 0,
        }]
    );
    assert!(uncovered.is_empty());
}

#[test]
fn concentrate_stays_sticky_even_when_a_higher_ranked_source_also_covers() {
    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let sources = vec![
        // A covers both blocks; B covers only block 1 but is ranked higher.
        SourceCoverage {
            source_ix: 0,
            coverage: cov(2, &[0, 1]),
        },
        SourceCoverage {
            source_ix: 1,
            coverage: cov(2, &[1]),
        },
    ];
    let (runs, uncovered) = plan_covered_runs(&whole_gap(total), total, &sources, &[1, 0]);
    // A single run on A: no flip to B at block 1, even though B ranks first.
    assert_eq!(
        runs,
        vec![CoveredRun {
            offset: 0,
            len: 2 * DISCOVERY_BLOCK_BYTES,
            source_ix: 0,
        }]
    );
    assert!(uncovered.is_empty());
}

#[test]
fn concentrate_uncovered_block_lands_in_uncovered_set() {
    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let sources = vec![SourceCoverage {
        source_ix: 0,
        coverage: cov(2, &[0]),
    }];
    let (runs, uncovered) = plan_covered_runs(&whole_gap(total), total, &sources, &[0]);
    assert_eq!(
        runs,
        vec![CoveredRun {
            offset: 0,
            len: DISCOVERY_BLOCK_BYTES,
            source_ix: 0,
        }]
    );
    assert!(!uncovered.is_empty());
    let expected = block_chunks(1);
    assert_eq!(uncovered, expected);
}

/// Union of the given discovery blocks' full chunk ranges — a `gap` that
/// is a strict subset of the blob (some blocks already held, so their
/// chunks are absent from the gap).
fn gap_of_blocks(blocks: &[u32]) -> ChunkRanges {
    blocks
        .iter()
        .fold(ChunkRanges::empty(), |acc, &b| acc | block_chunks(b))
}

#[test]
fn concentrate_a_gap_skipped_block_breaks_run_contiguity() {
    // 3-block blob; gap covers blocks 0 and 2 only — block 1 is already
    // held, so its chunks are NOT in the gap and the walk skips it. A
    // single source covers all three blocks. Even though the same
    // source covers both block 0 and block 2, the skipped block 1 in
    // between must NOT let them coalesce into one run: block 0 and
    // block 2 are not byte-adjacent in the output, so merging them
    // would silently claim bytes (block 1) that were never in the gap
    // and don't need fetching.
    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let sources = vec![SourceCoverage {
        source_ix: 0,
        coverage: cov(3, &[0, 1, 2]),
    }];
    let gap = gap_of_blocks(&[0, 2]);
    let (runs, uncovered) = plan_covered_runs(&gap, total, &sources, &[0]);
    assert_eq!(
        runs,
        vec![
            CoveredRun {
                offset: 0,
                len: DISCOVERY_BLOCK_BYTES,
                source_ix: 0,
            },
            CoveredRun {
                offset: 2 * DISCOVERY_BLOCK_BYTES,
                len: DISCOVERY_BLOCK_BYTES,
                source_ix: 0,
            },
        ],
        "block 1 is skipped (not in the gap), so blocks 0 and 2 must NOT coalesce"
    );
    assert!(uncovered.is_empty());
}

#[test]
fn concentrate_gap_of_a_single_middle_block_yields_one_run() {
    // gap covers only block 1 (blocks 0 and 2 already held); a source
    // covering all three blocks yields exactly one run, for block 1.
    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let sources = vec![SourceCoverage {
        source_ix: 0,
        coverage: cov(3, &[0, 1, 2]),
    }];
    let gap = gap_of_blocks(&[1]);
    let (runs, uncovered) = plan_covered_runs(&gap, total, &sources, &[0]);
    assert_eq!(
        runs,
        vec![CoveredRun {
            offset: DISCOVERY_BLOCK_BYTES,
            len: DISCOVERY_BLOCK_BYTES,
            source_ix: 0,
        }]
    );
    assert!(uncovered.is_empty());
}

#[test]
fn spread_gives_each_overlapping_source_a_block_when_both_cover_everything() {
    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let sources = vec![
        SourceCoverage {
            source_ix: 0,
            coverage: cov(2, &[0, 1]),
        },
        SourceCoverage {
            source_ix: 1,
            coverage: cov(2, &[0, 1]),
        },
    ];
    let (runs, uncovered) = spread_segments(&whole_gap(total), total, &sources, &[0, 1]);
    assert!(uncovered.is_empty());
    assert_eq!(runs.len(), 2, "both lanes should be busy: {runs:?}");
    let by_source: std::collections::HashSet<usize> = runs.iter().map(|r| r.source_ix).collect();
    assert_eq!(by_source, std::collections::HashSet::from([0, 1]));
}

#[test]
fn spread_assigns_a_block_only_one_source_covers_to_that_source_regardless_of_rank() {
    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let sources = vec![
        SourceCoverage {
            source_ix: 0,
            coverage: cov(2, &[0, 1]),
        },
        // B only covers block 1, but B is ranked first.
        SourceCoverage {
            source_ix: 1,
            coverage: cov(2, &[1]),
        },
    ];
    let (runs, uncovered) = spread_segments(&whole_gap(total), total, &sources, &[1, 0]);
    assert!(uncovered.is_empty());
    let block1_run = runs
        .iter()
        .find(|r| r.offset == DISCOVERY_BLOCK_BYTES)
        .expect("block 1 covered");
    assert_eq!(block1_run.source_ix, 1);
}

/// A gap of exactly `[65 MiB, 66 MiB)` — a 1 MiB slice wholly inside block 1 —
/// yields a run of exactly `(65 MiB, 1 MiB)` from BOTH planners, never the
/// whole 64 MiB block (#1506). Without the clamp the node would pull and pay
/// for 63 MiB no one asked for, including bytes an attached sibling owns.
fn slice_gap(from: u64, to: u64) -> ChunkRanges {
    ChunkRanges::from(ChunkNum(from / BAO_CHUNK_BYTES)..ChunkNum(to / BAO_CHUNK_BYTES))
}

#[test]
fn concentrate_clamps_a_run_to_a_partial_in_block_gap() {
    let mib = 1024 * 1024;
    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let sources = vec![SourceCoverage {
        source_ix: 0,
        coverage: cov(2, &[0, 1]),
    }];
    let gap = slice_gap(65 * mib, 66 * mib);
    let (runs, uncovered) = plan_covered_runs(&gap, total, &sources, &[0]);
    assert_eq!(
        runs,
        vec![CoveredRun {
            offset: 65 * mib,
            len: mib,
            source_ix: 0,
        }],
        "run clamped to the gap slice, not the whole 64 MiB block"
    );
    assert!(uncovered.is_empty());
}

#[test]
fn spread_clamps_a_run_to_a_partial_in_block_gap() {
    let mib = 1024 * 1024;
    let total = 2 * DISCOVERY_BLOCK_BYTES;
    let sources = vec![SourceCoverage {
        source_ix: 0,
        coverage: cov(2, &[0, 1]),
    }];
    let gap = slice_gap(65 * mib, 66 * mib);
    let (runs, uncovered) = spread_segments(&gap, total, &sources, &[0]);
    assert_eq!(
        runs,
        vec![CoveredRun {
            offset: 65 * mib,
            len: mib,
            source_ix: 0,
        }],
        "run clamped to the gap slice, not the whole 64 MiB block"
    );
    assert!(uncovered.is_empty());
}

#[test]
fn spread_gives_each_full_holder_one_contiguous_run_not_one_per_block() {
    // 8-block blob, 4 sources each holding the WHOLE blob. The old block-by-block
    // round-robin fragmented this into 8 single-block runs (then `blocks × N`
    // scheduler segments); the contiguous spread hands each holder ONE
    // contiguous multi-block run — ~N runs, not ~B (#1506 fan-out).
    let total = 8 * DISCOVERY_BLOCK_BYTES;
    let all: Vec<u32> = (0..8).collect();
    let sources: Vec<SourceCoverage> = (0..4)
        .map(|ix| SourceCoverage {
            source_ix: ix,
            coverage: cov(8, &all),
        })
        .collect();
    let (runs, uncovered) = spread_segments(&whole_gap(total), total, &sources, &[0, 1, 2, 3]);
    assert!(uncovered.is_empty());
    assert_eq!(
        runs.len(),
        4,
        "one contiguous run per full holder: {runs:?}"
    );
    let sources_used: HashSet<usize> = runs.iter().map(|r| r.source_ix).collect();
    assert_eq!(sources_used.len(), 4, "every holder engaged: {runs:?}");
    for r in &runs {
        assert_eq!(
            r.len,
            2 * DISCOVERY_BLOCK_BYTES,
            "each run is the contiguous 2-block share: {runs:?}"
        );
    }
}

#[test]
fn spread_resume_tail_still_engages_multiple_lanes() {
    // Resume: only blocks 6 and 7 (a 2-block tail) of an 8-block blob remain,
    // held by 4 full holders. The tail must still spread across ≥2 lanes rather
    // than collapse onto one (#1506 fan-out defect 2).
    let total = 8 * DISCOVERY_BLOCK_BYTES;
    let all: Vec<u32> = (0..8).collect();
    let sources: Vec<SourceCoverage> = (0..4)
        .map(|ix| SourceCoverage {
            source_ix: ix,
            coverage: cov(8, &all),
        })
        .collect();
    let gap = gap_of_blocks(&[6, 7]);
    let (runs, uncovered) = spread_segments(&gap, total, &sources, &[0, 1, 2, 3]);
    assert!(uncovered.is_empty());
    let sources_used: HashSet<usize> = runs.iter().map(|r| r.source_ix).collect();
    assert!(
        sources_used.len() >= 2,
        "the tail spreads across ≥2 lanes: {runs:?}"
    );
}
