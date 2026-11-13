use std::sync::Arc;

use alloy::primitives::B256;
use decdn_bao_range::{CHUNK_GROUP_BYTES, align_range};
use decdn_client::{Cumulative, PULL_AHEAD_BYTES, PoolLedger};
use decdn_protocol::{Coverage, DISCOVERY_BLOCK_BYTES, num_blocks};

use super::{PrimeKey, PrimeLeg, handshake_order};

const MIB: u64 = 1024 * 1024;

/// The first draw is whole chunk groups: what one chunk of paid wire covers,
/// floored to a group, plus the pull's ahead slack.
#[test]
fn the_first_draw_is_one_chunks_content_plus_the_ahead_slack() {
    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let draw = PrimeLeg::first_draw(0, total);
    assert_eq!(draw % CHUNK_GROUP_BYTES, 0);
    assert!(
        draw > PULL_AHEAD_BYTES && draw <= MIB + PULL_AHEAD_BYTES,
        "one chunk of wire covers a little under one chunk of content: {draw}"
    );
}

/// The predicted leg is the request cut to the first draw, the blob, the
/// source's covered span, and the received-byte ceiling; it starts on the
/// request's chunk group.
#[test]
fn the_predicted_leg_cuts_the_request_like_the_first_drive_leg() {
    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let full = Coverage::full(num_blocks(total));
    let prime = PrimeLeg::new(0, 0);
    let draw = PrimeLeg::first_draw(0, total);

    // A whole-blob pull draws the first draw from offset 0.
    assert_eq!(
        prime.predicted_leg(total, &full, 0),
        align_range(0, draw, total).ok()
    );
    // A nonzero offset paired with `len == u64::MAX` genuinely overflows
    // `offset + len` (unlike offset 0, which does not). A grown size claim
    // can widen `len` this far, and `saturating_add` plus the
    // `.min(total_bytes)` clamp right after it primes a leg (the same one a
    // `byte_len == 0` "to end" request from the same offset would) rather
    // than losing the first leg to `None`.
    assert_eq!(
        PrimeLeg::new(CHUNK_GROUP_BYTES, u64::MAX).predicted_leg(total, &full, 0),
        align_range(
            CHUNK_GROUP_BYTES,
            PrimeLeg::first_draw(CHUNK_GROUP_BYTES, total),
            total
        )
        .ok()
    );
    // A mid-group start rounds down to its group; a short request is cut
    // to its own end.
    let short = PrimeLeg::new(CHUNK_GROUP_BYTES + 5, 2 * CHUNK_GROUP_BYTES);
    assert_eq!(
        short.predicted_leg(total, &full, 0),
        align_range(CHUNK_GROUP_BYTES, 3 * CHUNK_GROUP_BYTES, total).ok()
    );
    // The cap on bytes held cuts the draw to the cap's worth.
    assert_eq!(
        prime.predicted_leg(total, &full, CHUNK_GROUP_BYTES),
        align_range(0, CHUNK_GROUP_BYTES, total).ok()
    );
    // The draw is cut to the source's covered span.
    let first_block = Coverage::from_block_indices(num_blocks(total), [0, 2].into_iter());
    let covered = PrimeLeg::new(0, 0).predicted_leg(total, &first_block, 0);
    assert!(covered.is_some());
    // A source that does not cover the first block runs no first leg here.
    let later = Coverage::from_block_indices(num_blocks(total), [1].into_iter());
    assert_eq!(prime.predicted_leg(total, &later, 0), None);
    // A request past the hinted end has no leg.
    let past = PrimeLeg::new(total, 0);
    assert_eq!(past.predicted_leg(total, &full, 0), None);
}

/// `predicted_leg` is the leg the pull leg's first run opens: for each case,
/// plan the fresh gap over the candidates as `assemble` does, then compare the
/// first run's source's prediction with `first_leg` over that run.
#[tokio::test]
async fn the_predicted_leg_is_the_first_runs_first_leg() {
    use decdn_bao_range::RangedStore as _;
    use decdn_client::{ClientRangedStore, SourceCoverage, first_leg, plan_covered_runs};

    let total = 3 * DISCOVERY_BLOCK_BYTES - 5 * CHUNK_GROUP_BYTES - 7;
    let blocks = num_blocks(total);
    let full = Coverage::full(blocks);
    let tail = Coverage::from_block_indices(blocks, [1, 2].into_iter());
    let head = Coverage::from_block_indices(blocks, [0].into_iter());
    let group = CHUNK_GROUP_BYTES;
    // (offset, len, cap on bytes held, coverages)
    let cases: Vec<(u64, u64, u64, Vec<Coverage>)> = vec![
        (0, 0, 0, vec![full.clone()]),
        (group + 5, 3 * group, 0, vec![full.clone()]),
        (5 * MIB + 3, 0, 0, vec![full.clone()]),
        (0, 0, 2 * group, vec![full.clone()]),
        // The first block's only holder is the second candidate.
        (0, 0, 0, vec![tail.clone(), head.clone()]),
        // A request inside the ragged last block.
        (total - 3 * group, 0, 0, vec![full.clone()]),
        (DISCOVERY_BLOCK_BYTES + 7, 2 * MIB, 0, vec![head, tail]),
    ];
    for (offset, len, ceiling, coverages) in cases {
        let dir = tempfile::tempdir().unwrap();
        let store = ClientRangedStore::create(dir.path(), "b", [7; 32], total).unwrap();
        let prime = PrimeLeg::new(offset, len);
        let gap = store.missing_ranges(offset, len).await.unwrap();
        let sources: Vec<SourceCoverage> = coverages
            .iter()
            .enumerate()
            .map(|(source_ix, coverage)| SourceCoverage {
                source_ix,
                coverage: coverage.clone(),
            })
            .collect();
        let rank: Vec<usize> = (0..sources.len()).collect();
        let (runs, _) = plan_covered_runs(&gap, total, &sources, &rank);
        let run = runs.first().unwrap();
        let start = offset - offset % group;
        let want = first_leg(
            &store,
            &[(run.offset, run.len)],
            PrimeLeg::first_draw(start, total),
            ceiling,
        )
        .await
        .unwrap();
        let got = prime.predicted_leg(total, coverages.get(run.source_ix).unwrap(), ceiling);
        assert_eq!(
            got, want,
            "offset {offset}, len {len}, ceiling {ceiling}"
        );
    }
}

/// The handshake visits first the holder of the request's first block,
/// the one the first run uses, and keeps each candidate's ranked index.
#[test]
fn the_handshake_visits_the_first_runs_holder_first() {
    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let blocks = num_blocks(total);
    let later = Coverage::from_block_indices(blocks, [1, 2].into_iter());
    let head = Coverage::from_block_indices(blocks, [0].into_iter());
    let prime = PrimeLeg::new(0, 0);
    assert_eq!(
        handshake_order(&[later, head], Some(total), Some(prime)),
        vec![1, 0]
    );
}

/// Without a prime, or without a size to plan over, the handshake walks
/// the rank order.
#[test]
fn without_a_prime_the_handshake_walks_the_rank_order() {
    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let blocks = num_blocks(total);
    let later = Coverage::from_block_indices(blocks, [1, 2].into_iter());
    let head = Coverage::from_block_indices(blocks, [0].into_iter());
    let coverages = [later, head];
    assert_eq!(handshake_order(&coverages, Some(total), None), vec![0, 1]);
    let prime = PrimeLeg::new(0, 0);
    assert_eq!(handshake_order(&coverages, None, Some(prime)), vec![0, 1]);
}

/// When no candidate covers the prime's first block, the handshake walks
/// the rank order, even though a later block has a holder.
#[test]
fn with_no_holder_of_the_first_block_the_handshake_walks_the_rank_order() {
    let total = 3 * DISCOVERY_BLOCK_BYTES;
    let blocks = num_blocks(total);
    let last = Coverage::from_block_indices(blocks, [2].into_iter());
    let middle = Coverage::from_block_indices(blocks, [1].into_iter());
    let prime = PrimeLeg::new(0, 0);
    assert_eq!(
        handshake_order(&[last, middle], Some(total), Some(prime)),
        vec![0, 1]
    );
}

/// A run adopts the pull only on its own source, pool, and lane ledger,
/// opening exactly its range. The pull's age is `PrimedSource`'s to check.
#[test]
fn a_run_adopts_only_its_own_leg() {
    let total = 8 * CHUNK_GROUP_BYTES;
    let range = align_range(0, 2 * CHUNK_GROUP_BYTES, total).unwrap();
    let ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    let pool = B256::repeat_byte(7);
    let key = PrimeKey {
        candidate_ix: 1,
        pool_id: pool,
        ledger: Arc::clone(&ledger),
        range: range.clone(),
        opened_at: tokio::time::Instant::now(),
    };
    assert!(key.answers(1, pool, &ledger, Some(&range)));

    assert!(
        !key.answers(0, pool, &ledger, Some(&range)),
        "another source"
    );
    assert!(
        !key.answers(1, B256::repeat_byte(8), &ledger, Some(&range)),
        "another pool"
    );
    let other_ledger = Arc::new(PoolLedger::new(Cumulative::default()));
    assert!(
        !key.answers(1, pool, &other_ledger, Some(&range)),
        "another lane ledger"
    );
    let other = align_range(0, CHUNK_GROUP_BYTES, total).unwrap();
    assert!(!key.answers(1, pool, &ledger, Some(&other)), "another leg");
    assert!(!key.answers(1, pool, &ledger, None), "nothing to open");
}
