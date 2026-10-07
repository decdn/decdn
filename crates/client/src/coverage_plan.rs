//! Shared coverage-map primitive plus two objective-specific planners over
//! it (#1506).
//!
//! Both consumers walk the same [`decdn_protocol::Coverage`] discovery
//! bitmaps against a byte-range gap, but want opposite outcomes: the node's
//! ranged-drive loop wants to CONCENTRATE on as few sources as possible (a
//! source flip costs a fresh lane and a mid-stream serve pause), while the
//! client's multi-source scheduler wants to SPREAD across every admitted
//! source so all lanes run concurrently. [`plan_covered_runs`] and
//! `spread_segments` are the two assigners: the node's sticky
//! [`plan_covered_runs`] builds on the shared best-ranked lookup
//! `covering_sources`, while `spread_segments` needs every covering
//! source per block (not just the best-ranked one) and so runs its own
//! per-block candidate scan.
//!
//! Value ranking (`rank`) is always the existing unified selection score
//! (ADR 001: rate + RTT + reputation) — passed in as source indices, best
//! first. Neither planner ranks cost or latency on its own; the node's
//! margin is enforced by the ADR 041 gate, not here.

use bao_tree::{ChunkNum, ChunkRanges};
use decdn_protocol::{Coverage, discovery_block_bytes, num_blocks};
use std::collections::{HashMap, HashSet};

/// Bao chunk size in bytes — the [`ChunkNum`] unit, fixed by `bao-tree`.
const BAO_CHUNK_BYTES: u64 = 1024;

/// Number of bao chunks spanned by one discovery block. Reads
/// [`discovery_block_bytes`], which is the fixed `DISCOVERY_BLOCK_BYTES` in
/// production and a test-overridable value under the `test-support` feature.
// Not a `const fn`: under the `test-support` feature `discovery_block_bytes()`
// reads a runtime override rather than the constant, so this cannot be const in
// every feature configuration.
#[allow(clippy::missing_const_for_fn)]
fn chunks_per_block() -> u64 {
    discovery_block_bytes() / BAO_CHUNK_BYTES
}

/// One source's advertised discovery-block coverage, paired with the index
/// the caller uses to identify it (a position in its own candidate list —
/// this module never dereferences `source_ix`, it only carries it through).
#[derive(Debug, Clone)]
pub struct SourceCoverage {
    /// The caller's opaque handle for this source — a position in its own
    /// candidate list. This module carries it through untouched.
    pub source_ix: usize,
    /// The discovery-block bitmap this source advertises.
    pub coverage: Coverage,
}

/// One contiguous byte run assigned to one source. `offset`/`len` are CLAMPED to
/// the gap being fetched intersected with the request — a run spans only bytes
/// that are actually missing and requested, never the whole discovery block(s) it
/// falls in (#1506). It therefore starts and ends on the gap's own chunk
/// boundaries, and a run reaching the blob's tail has its end clamped to
/// `total_bytes`. A run whose block span was covered by the gap in full is exactly
/// that block span; one covering a partial in-block slice is exactly that slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoveredRun {
    /// First byte of the run, clamped to the gap∩request start.
    pub offset: u64,
    /// Length of the run in bytes, clamped so `offset + len` never exceeds the
    /// gap∩request end (nor `total_bytes` at the blob tail).
    pub len: u64,
    /// The source assigned to serve this run — the `source_ix` of the
    /// [`SourceCoverage`] whose coverage spans it.
    pub source_ix: usize,
}

/// The byte-offset start of discovery block `block`.
fn block_offset(block: u32) -> u64 {
    u64::from(block) * discovery_block_bytes()
}

/// The tight byte extent of `gap` within `[lo, hi)`: the lowest missing byte at or
/// after `lo` through the highest missing byte before `hi`, the end clamped to
/// `hi`. `None` when `gap` misses nothing in `[lo, hi)`.
///
/// A planned run is clamped to this so it covers only the in-gap, in-request bytes
/// of its discovery block(s) — never the whole block. Without the clamp a `Mixed`
/// remainder of, say, `[65 MiB, 66 MiB)` would plan a whole-block `[64 MiB, 128 MiB)`
/// run and pull (and pay for) 63 MiB no one asked for, including bytes an attached
/// sibling claim owns and is concurrently pulling (#1506).
fn gap_extent(gap: &ChunkRanges, lo: u64, hi: u64) -> Option<(u64, u64)> {
    if hi <= lo {
        return None;
    }
    let lo_chunk = lo / BAO_CHUNK_BYTES;
    let hi_chunk = hi.div_ceil(BAO_CHUNK_BYTES);
    let window = ChunkRanges::from(ChunkNum(lo_chunk)..ChunkNum(hi_chunk));
    let intersect = gap & &window;
    let bounds = intersect.boundaries();
    let first = bounds.first()?;
    let last = bounds.last()?;
    let start = first.0.saturating_mul(BAO_CHUNK_BYTES).max(lo);
    let end = last.0.saturating_mul(BAO_CHUNK_BYTES).min(hi);
    (end > start).then_some((start, end))
}

/// The chunk-range span of discovery block `block`: `block * 65536` through
/// `(block + 1) * 65536`, exclusive, in [`ChunkNum`] units.
fn block_chunks(block: u32) -> ChunkRanges {
    let per = chunks_per_block();
    let start = u64::from(block) * per;
    let end = start + per;
    ChunkRanges::from(ChunkNum(start)..ChunkNum(end))
}

/// Build the [`CoveredRun`] spanning discovery blocks `[start_block,
/// last_block]` inclusive, CLAMPED to the byte extent `gap` actually misses
/// inside that block span (#1506). A block span the gap covers in full yields
/// the whole block span (its end clamped to `total_bytes`); one the gap touches
/// only partially yields exactly the in-gap slice. Every block in `[start_block,
/// last_block]` intersects the gap by construction, so the extent is non-empty;
/// the whole block span is a defensive fallback that never triggers.
fn run_from(
    source_ix: usize,
    start_block: u32,
    last_block: u32,
    total_bytes: u64,
    gap: &ChunkRanges,
) -> CoveredRun {
    let block_lo = block_offset(start_block);
    let block_hi = block_offset(last_block)
        .saturating_add(discovery_block_bytes())
        .min(total_bytes);
    let (offset, end) = gap_extent(gap, block_lo, block_hi).unwrap_or((block_lo, block_hi));
    CoveredRun {
        offset,
        len: end.saturating_sub(offset),
        source_ix,
    }
}

/// Does `coverage` include every discovery block the byte range
/// `[start, start + len)` intersects (clamped to `total_bytes`)?
///
/// The scheduler uses this wherever a range must lie wholly inside a source's
/// coverage: a worker's preference for a pending segment (#1506), and the
/// count of full holders a planned run splits across. A steal uses
/// [`covered_suffix_start`] instead.
#[must_use]
pub(crate) fn covers_byte_range(
    coverage: &Coverage,
    start: u64,
    len: u64,
    total_bytes: u64,
) -> bool {
    if len == 0 {
        return true;
    }
    let end = start.saturating_add(len).min(total_bytes);
    if end <= start {
        return true;
    }
    let block_bytes = discovery_block_bytes();
    let first_block = u32::try_from(start / block_bytes).unwrap_or(u32::MAX);
    let last_block = u32::try_from((end - 1) / block_bytes).unwrap_or(u32::MAX);
    (first_block..=last_block).all(|b| coverage.covers(b))
}

/// The start of the longest suffix of the byte range `[start, start + len)`
/// (clamped to `total_bytes`) whose every intersected discovery block
/// `coverage` includes, or `None` when `coverage` does not include the block
/// that holds the range's last byte. A range `coverage` holds in full gives
/// `start`; an empty range gives `start` too.
///
/// A steal (#2303) takes the tail of a range in flight, so a freed source may
/// steal from a range it covers only from some block onward: the result is
/// the lowest offset it may take. The block size is a multiple of the chunk
/// group, so a result past `start` is group-aligned.
#[must_use]
pub(crate) fn covered_suffix_start(
    coverage: &Coverage,
    start: u64,
    len: u64,
    total_bytes: u64,
) -> Option<u64> {
    let end = start.saturating_add(len).min(total_bytes);
    if end <= start {
        return Some(start);
    }
    let block_bytes = discovery_block_bytes();
    let first_block = u32::try_from(start / block_bytes).unwrap_or(u32::MAX);
    let mut block = u32::try_from((end - 1) / block_bytes).unwrap_or(u32::MAX);
    if !coverage.covers(block) {
        return None;
    }
    while block > first_block && coverage.covers(block - 1) {
        block -= 1;
    }
    Some(u64::from(block).saturating_mul(block_bytes).max(start))
}

/// The chunks of `chunks` that lie in discovery blocks `coverage` holds.
pub(crate) fn covered_part(
    coverage: &Coverage,
    chunks: &ChunkRanges,
    total_bytes: u64,
) -> ChunkRanges {
    let mut held = ChunkRanges::empty();
    for block in (0..num_blocks(total_bytes)).filter(|&b| coverage.covers(b)) {
        held |= block_chunks(block);
    }
    chunks & &held
}

/// The best-`rank`ed source index whose coverage includes `block`, or `None`
/// if no source in `rank` covers it.
///
/// `rank` lists source indices best-first (the existing unified selection
/// score, ADR 001) — this never re-derives a ranking of its own.
#[must_use]
pub(crate) fn covering_sources(
    block: u32,
    sources: &[SourceCoverage],
    rank: &[usize],
) -> Option<usize> {
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
/// current source's coverage ends does `covering_sources` pick a
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
                    runs.push(run_from(cur_src, start, last, total_bytes, gap));
                    Some((src, block, block))
                }
                None => Some((src, block, block)),
            };
        } else {
            if let Some((cur_src, start, last)) = current.take() {
                runs.push(run_from(cur_src, start, last, total_bytes, gap));
            }
            uncovered |= &chunks & gap;
        }
    }
    if let Some((cur_src, start, last)) = current.take() {
        runs.push(run_from(cur_src, start, last, total_bytes, gap));
    }

    (runs, uncovered)
}

/// Client planner: spread across every covering source so all lanes run
/// concurrently, in CONTIGUOUS runs so a large gap plans about one run per source
/// rather than one per block (#1506).
///
/// For each `gap`-intersecting discovery block, collects the sources that cover
/// it (its candidates). Blocks are then walked in offset order and assigned
/// STICKILY: the current run's source keeps the next block while it still covers
/// it and has not yet filled its fair share (`ceil(covered_blocks / sources)`),
/// so contiguous blocks land on one source and coalesce into one multi-block
/// [`CoveredRun`]. When the current source cannot take a block — it does not cover
/// it, or its share is full — the block goes to whichever candidate currently
/// holds the fewest assigned blocks (spreading the load), tied-broken by `rank`.
/// A block only one source covers always lands on that source, whatever its share.
/// A block no source covers contributes its gap-intersecting chunks to
/// `uncovered`. Runs are then clamped to the gap exactly as [`plan_covered_runs`]
/// does.
///
/// The share cap is what turns the old block-by-block round-robin — which
/// fragmented a whole-blob fan-out across N full holders into one single-block
/// run per block (then `blocks × N` scheduler segments) — into ~N contiguous runs,
/// one span per holder. A gap smaller than the source count still spreads across
/// as many lanes as it has blocks.
#[must_use]
pub(crate) fn spread_segments(
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

    // Sticky, share-capped assignment in offset order. `target_share` is the
    // largest number of blocks one source is asked to hold before the walk moves
    // on to a fresh source; keeping it as the sticky bound is what produces
    // contiguous multi-block runs (one span per source) instead of a round-robin
    // fragment per block.
    let n_sources = sources.len().max(1);
    let target_share = candidates_by_block.len().div_ceil(n_sources).max(1);
    // `rank` lists source indices best-first; invert it once so the tie-break in
    // the assignment loop below reads a source's rank position in O(1) instead of
    // scanning `rank` per candidate per block.
    let rank_pos_by_source: HashMap<usize, usize> = rank
        .iter()
        .enumerate()
        .map(|(pos, &src)| (src, pos))
        .collect();
    let mut assigned_count: HashMap<usize, usize> = HashMap::new();
    let mut assignment: HashMap<u32, usize> = HashMap::new();
    let mut current: Option<usize> = None;
    for (block, candidates) in &candidates_by_block {
        let stick = current.filter(|src| {
            candidates.contains(src) && assigned_count.get(src).copied().unwrap_or(0) < target_share
        });
        let chosen = stick.or_else(|| {
            candidates.iter().copied().min_by_key(|src| {
                let load = assigned_count.get(src).copied().unwrap_or(0);
                let rank_pos = rank_pos_by_source.get(src).copied().unwrap_or(usize::MAX);
                (load, rank_pos)
            })
        });
        if let Some(src) = chosen {
            assignment.insert(*block, src);
            *assigned_count.entry(src).or_insert(0) += 1;
            current = Some(src);
        }
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
                    runs.push(run_from(cur_src, start, last, total_bytes, gap));
                    Some((src, block, block))
                }
                None => Some((src, block, block)),
            };
        } else if is_uncovered.contains(&block)
            && let Some((cur_src, start, last)) = current.take()
        {
            runs.push(run_from(cur_src, start, last, total_bytes, gap));
        }
        // else: block does not intersect `gap` — leave `current` untouched.
    }
    if let Some((cur_src, start, last)) = current.take() {
        runs.push(run_from(cur_src, start, last, total_bytes, gap));
    }

    (runs, uncovered)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests;
