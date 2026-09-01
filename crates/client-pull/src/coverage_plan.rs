//! Shared coverage-map primitive plus two objective-specific planners over
//! it (#1506).
//!
//! Both consumers walk the same [`decdn_protocol::Coverage`] discovery
//! bitmaps against a byte-range gap, but want opposite outcomes: the node's
//! ranged-drive loop wants to CONCENTRATE on as few sources as possible (a
//! source flip costs a fresh lane and a mid-stream serve pause), while the
//! client's multi-source scheduler wants to SPREAD across every admitted
//! source so all lanes run concurrently. [`plan_covered_runs`] and
//! [`spread_segments`] are the two assigners; [`covering_sources`] is the
//! shared best-ranked lookup both build on.
//!
//! Value ranking (`rank`) is always the existing unified selection score
//! (ADR 001: rate + RTT + reputation) — passed in as source indices, best
//! first. Neither planner ranks cost or latency on its own; the node's
//! margin is enforced by the ADR 041 gate, not here.

use bao_tree::{ChunkNum, ChunkRanges};
use decdn_protocol::{Coverage, DISCOVERY_BLOCK_BYTES, num_blocks};
use std::collections::{HashMap, HashSet};

/// Bao chunk size in bytes — the [`ChunkNum`] unit, fixed by `bao-tree`.
const BAO_CHUNK_BYTES: u64 = 1024;

/// Number of bao chunks spanned by one [`DISCOVERY_BLOCK_BYTES`] discovery
/// block.
const CHUNKS_PER_BLOCK: u64 = DISCOVERY_BLOCK_BYTES / BAO_CHUNK_BYTES;

/// One source's advertised discovery-block coverage, paired with the index
/// the caller uses to identify it (a position in its own candidate list —
/// this module never dereferences `source_ix`, it only carries it through).
#[derive(Debug, Clone)]
pub struct SourceCoverage {
    pub source_ix: usize,
    pub coverage: Coverage,
}

/// One contiguous byte run assigned to one source. `offset`/`len` are
/// discovery-block-aligned (64 MiB multiples), except a run touching the
/// blob's tail, whose `len` is clamped to `total_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoveredRun {
    pub offset: u64,
    pub len: u64,
    pub source_ix: usize,
}

/// The byte-offset start of discovery block `block`.
fn block_offset(block: u32) -> u64 {
    u64::from(block) * DISCOVERY_BLOCK_BYTES
}

/// The chunk-range span of discovery block `block`: `block * 65536` through
/// `(block + 1) * 65536`, exclusive, in [`ChunkNum`] units.
fn block_chunks(block: u32) -> ChunkRanges {
    let start = u64::from(block) * CHUNKS_PER_BLOCK;
    let end = start + CHUNKS_PER_BLOCK;
    ChunkRanges::from(ChunkNum(start)..ChunkNum(end))
}

/// Build the [`CoveredRun`] spanning discovery blocks `[start_block,
/// last_block]` inclusive, clamping its end to `total_bytes` for a run that
/// reaches the blob's final (possibly partial) block.
fn run_from(source_ix: usize, start_block: u32, last_block: u32, total_bytes: u64) -> CoveredRun {
    let offset = block_offset(start_block);
    let end = block_offset(last_block)
        .saturating_add(DISCOVERY_BLOCK_BYTES)
        .min(total_bytes);
    CoveredRun {
        offset,
        len: end.saturating_sub(offset),
        source_ix,
    }
}

/// The best-`rank`ed source index whose coverage includes `block`, or `None`
/// if no source in `rank` covers it.
///
/// `rank` lists source indices best-first (the existing unified selection
/// score, ADR 001) — this never re-derives a ranking of its own.
#[must_use]
pub fn covering_sources(block: u32, sources: &[SourceCoverage], rank: &[usize]) -> Option<usize> {
    rank.iter().copied().find(|&ix| {
        sources
            .iter()
            .any(|s| s.source_ix == ix && s.coverage.covers(block))
    })
}

/// Node planner: concentrate on as few sources as possible, minimizing flips
/// (#1506, ADR 001).
///
/// Walks discovery blocks `0..num_blocks(total_bytes)` in offset order,
/// considering only blocks that intersect `gap`. For each such block, the
/// CURRENT run's source is kept if it still covers this block — sticky, no
/// flip — even when a higher-ranked source also covers it; only when the
/// current source's coverage ends does [`covering_sources`] pick a
/// replacement. Contiguous same-source blocks coalesce into one
/// [`CoveredRun`]; a block no source covers breaks the run (if any) and its
/// gap-intersecting chunks land in the returned `uncovered` set.
///
/// Every flip costs a fresh lane and a mid-stream serve pause on the node's
/// ranged-drive loop, so stickiness here is load-bearing, not cosmetic.
#[must_use]
pub fn plan_covered_runs(
    gap: &ChunkRanges,
    total_bytes: u64,
    sources: &[SourceCoverage],
    rank: &[usize],
) -> (Vec<CoveredRun>, ChunkRanges) {
    let n = num_blocks(total_bytes);
    let mut runs = Vec::new();
    let mut uncovered = ChunkRanges::empty();
    // Active run: (source_ix, first_block, last_block_so_far).
    let mut current: Option<(usize, u32, u32)> = None;

    for block in 0..n {
        let chunks = block_chunks(block);
        if !gap.intersects(&chunks) {
            continue;
        }

        let sticky = current.and_then(|(src, _, last)| {
            (last.checked_add(1) == Some(block)
                && sources
                    .iter()
                    .any(|s| s.source_ix == src && s.coverage.covers(block)))
            .then_some(src)
        });
        let chosen = sticky.or_else(|| covering_sources(block, sources, rank));

        if let Some(src) = chosen {
            current = match current {
                Some((cur_src, start, last))
                    if cur_src == src && last.checked_add(1) == Some(block) =>
                {
                    Some((cur_src, start, block))
                }
                Some((cur_src, start, last)) => {
                    runs.push(run_from(cur_src, start, last, total_bytes));
                    Some((src, block, block))
                }
                None => Some((src, block, block)),
            };
        } else {
            if let Some((cur_src, start, last)) = current.take() {
                runs.push(run_from(cur_src, start, last, total_bytes));
            }
            uncovered |= &chunks & gap;
        }
    }
    if let Some((cur_src, start, last)) = current.take() {
        runs.push(run_from(cur_src, start, last, total_bytes));
    }

    (runs, uncovered)
}

/// Client planner: spread across every covering source so all lanes run
/// concurrently (#1506).
///
/// For each `gap`-intersecting discovery block, collects the sources that
/// cover it (its candidates). Blocks are then assigned rarest-candidate
/// first — a block only one source can serve is locked in before any
/// ambiguous block competes for that source — and each assignment goes to
/// whichever candidate currently holds the fewest assigned blocks (a
/// size-balanced share), tied-broken by `rank`. A block no source covers
/// contributes its gap-intersecting chunks to `uncovered`. Contiguous
/// same-source blocks (in real block-index order) coalesce into one
/// [`CoveredRun`], exactly as [`plan_covered_runs`] does.
#[must_use]
pub fn spread_segments(
    gap: &ChunkRanges,
    total_bytes: u64,
    sources: &[SourceCoverage],
    rank: &[usize],
) -> (Vec<CoveredRun>, ChunkRanges) {
    let n = num_blocks(total_bytes);
    let mut uncovered = ChunkRanges::empty();
    let mut candidates_by_block: Vec<(u32, Vec<usize>)> = Vec::new();
    let mut is_uncovered: HashSet<u32> = HashSet::new();

    for block in 0..n {
        let chunks = block_chunks(block);
        if !gap.intersects(&chunks) {
            continue;
        }
        let candidates: Vec<usize> = sources
            .iter()
            .filter(|s| s.coverage.covers(block))
            .map(|s| s.source_ix)
            .collect();
        if candidates.is_empty() {
            uncovered |= &chunks & gap;
            is_uncovered.insert(block);
        } else {
            candidates_by_block.push((block, candidates));
        }
    }

    // Assign rarest-covered blocks first, tie-broken by original (offset)
    // order — Rust's `sort_by_key` is stable — so a block only one source
    // covers is locked in before any ambiguous block competes for that
    // source's share.
    let mut order: Vec<usize> = (0..candidates_by_block.len()).collect();
    order.sort_by_key(|&i| candidates_by_block.get(i).map_or(0, |(_, c)| c.len()));

    let mut assigned_count: HashMap<usize, u32> = HashMap::new();
    let mut assignment: HashMap<u32, usize> = HashMap::new();
    for i in order {
        let Some((block, candidates)) = candidates_by_block.get(i) else {
            continue;
        };
        let Some(&chosen) = candidates.iter().min_by_key(|&&src| {
            let load = assigned_count.get(&src).copied().unwrap_or(0);
            let rank_pos = rank.iter().position(|&r| r == src).unwrap_or(usize::MAX);
            (load, rank_pos)
        }) else {
            continue;
        };
        assignment.insert(*block, chosen);
        *assigned_count.entry(chosen).or_insert(0) += 1;
    }

    let mut runs = Vec::new();
    let mut current: Option<(usize, u32, u32)> = None;
    for block in 0..n {
        if let Some(&src) = assignment.get(&block) {
            current = match current {
                Some((cur_src, start, last))
                    if cur_src == src && last.checked_add(1) == Some(block) =>
                {
                    Some((cur_src, start, block))
                }
                Some((cur_src, start, last)) => {
                    runs.push(run_from(cur_src, start, last, total_bytes));
                    Some((src, block, block))
                }
                None => Some((src, block, block)),
            };
        } else if is_uncovered.contains(&block)
            && let Some((cur_src, start, last)) = current.take()
        {
            runs.push(run_from(cur_src, start, last, total_bytes));
        }
        // else: block does not intersect `gap` — leave `current` untouched.
    }
    if let Some((cur_src, start, last)) = current.take() {
        runs.push(run_from(cur_src, start, last, total_bytes));
    }

    (runs, uncovered)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

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
        let by_source: std::collections::HashSet<usize> =
            runs.iter().map(|r| r.source_ix).collect();
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
}
