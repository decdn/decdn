use super::content_paid_frontier;
use decdn_bao_range::{CHUNK_GROUP_BYTES, align_range, bao_encoded_size};

/// Exact wire byte count to deliver content `[fetch_start, c)` of a
/// `total`-byte blob — the same `bao_encoded_size` walk `content_paid_frontier`
/// inverts.
fn wire_to(fetch_start: u64, c: u64, total: u64) -> u64 {
    if c <= fetch_start {
        return 0;
    }
    align_range(fetch_start, c - fetch_start, total)
        .map_or(u64::MAX, |r| bao_encoded_size(total, r.chunk_ranges()))
}

/// `content_paid_frontier` must map a paid WIRE watermark to the LARGEST
/// content group boundary whose wire cost is fully within it: never above
/// (that would under-pay), and tight (the next group would exceed the paid
/// wire). Swept across a blob spanning several groups and every plausible paid
/// wire watermark.
#[test]
fn content_paid_frontier_never_over_maps_and_is_tight() {
    let group = CHUNK_GROUP_BYTES;
    // A few groups plus a partial tail, and a mid-blob resume start so the
    // `fetch_start > 0` arithmetic is exercised too.
    for total in [3 * group + 123, 6 * group, group + 1] {
        for fetch_start in [0u64, group, 2 * group] {
            if fetch_start >= total {
                continue;
            }
            let full_wire = wire_to(fetch_start, total, total);
            // Sweep paid-wire watermarks across the whole stream, including
            // values that land mid-group and mid-proof.
            for step in 0..=40u64 {
                let paid_wire = full_wire.saturating_mul(step) / 40;
                let c = content_paid_frontier(fetch_start, total, paid_wire);

                assert!(c >= fetch_start, "frontier must not regress below start");
                assert!(c <= total, "frontier must not exceed the blob");
                assert_eq!(
                    (c - fetch_start) % group,
                    if c == total {
                        (total - fetch_start) % group
                    } else {
                        0
                    },
                    "frontier is a group boundary (or the blob end)"
                );
                // No under-pay: every content byte up to `c` was inside the
                // paid wire prefix.
                assert!(
                    wire_to(fetch_start, c, total) <= paid_wire,
                    "c={c} maps past the paid wire {paid_wire} (would under-pay)"
                );
                // Tight: the NEXT group would have spilled past the paid wire
                // (unless we are already at the blob end).
                if c < total {
                    let next = (c + group).min(total);
                    assert!(
                        wire_to(fetch_start, next, total) > paid_wire,
                        "next group {next} still fits in {paid_wire}: not tight"
                    );
                }
            }
        }
    }
}

/// The per-leg precondition, stated as a test (#1497 review).
///
/// `content_paid_frontier` inverts the wire cost of ONE contiguous delivery
/// starting at `fetch_start`. Its caller derives `paid_wire` from a
/// CHANNEL-cumulative watermark, so if the `(fetch_start, baseline)` pair is
/// not re-anchored when a new leg begins, the second call is handed the SUM of
/// two independent bao range encodings against the first leg's start offset.
///
/// That sum strictly exceeds the contiguous cost of the same span, so it can
/// only map the frontier FORWARD — content delivered but never billed, and a
/// `byte_offset` past the verified on-disk prefix.
///
/// Two magnitudes, both covered here:
///
/// * **Disjoint legs** — the excess is only the re-sent root->`fetch_start`
///   proof path (~64 B per tree level). Real, but usually smaller than the
///   16 KiB group the answer is snapped down to, so it often lands on the same
///   boundary. Correctness here rests on luck, not on the invariant, which is
///   why the assertion is the direction (never backwards) rather than a jump.
/// * **A re-paid leg** — a resume retry re-delivers a span already paid for
///   (the ledger reseeds and the next attempt reopens at the same offset), so
///   the cumulative watermark climbs by a WHOLE duplicated span. That is
///   megabytes, not bytes, and it moves the frontier by entire groups.
#[test]
fn summed_multi_leg_wire_over_maps_which_is_why_baselines_are_per_leg() {
    let group = CHUNK_GROUP_BYTES;
    let total = 12 * group;
    // Leg 1 delivers [0, 3 groups); leg 2 resumes there and delivers to 6.
    let (frontier1, frontier2) = (3 * group, 6 * group);
    let leg1_wire = wire_to(0, frontier1, total);
    let leg2_wire = wire_to(frontier1, frontier2, total);
    let contiguous_wire = wire_to(0, frontier2, total);

    // The sum double-counts leg 2's re-sent left-boundary proof path.
    assert!(
        leg1_wire + leg2_wire > contiguous_wire,
        "the two legs' wire ({leg1_wire} + {leg2_wire}) must exceed the contiguous \
         cost of the same span ({contiguous_wire}), else this hazard would not exist"
    );

    // Correct (per-leg) call: anchored at leg 2's own start and budget.
    let correct = content_paid_frontier(frontier1, total, leg2_wire);
    assert_eq!(
        correct, frontier2,
        "the per-leg call lands on the true frontier"
    );

    // Stale baseline, disjoint legs: never maps BEHIND the true frontier, so it
    // can only skip billing, never re-bill.
    let stale = content_paid_frontier(0, total, leg1_wire + leg2_wire);
    assert!(
        stale >= frontier2,
        "a summed budget cannot under-report the frontier: {stale} < {frontier2}"
    );

    // Stale baseline with a RE-PAID leg: the ledger's cumulative wire includes
    // [0, 3 groups) twice — once for the abandoned attempt, once for the retry.
    // Anchored at the fetch's original start, that budget runs far past the
    // frontier actually paid for, skipping whole groups of billing.
    let with_repaid = content_paid_frontier(0, total, 2 * leg1_wire + leg2_wire);
    assert!(
        with_repaid > frontier2 + group,
        "a re-paid leg must visibly overshoot the true frontier by more than a \
         group: got {with_repaid}, true frontier {frontier2}"
    );
    // And the per-leg anchoring is immune to it: leg 2's own budget is unchanged
    // by whatever earlier legs re-paid for.
    assert_eq!(
        content_paid_frontier(frontier1, total, leg2_wire),
        frontier2,
        "per-leg anchoring is unaffected by an earlier leg being re-paid"
    );
}

/// `paid_wire == 0` (nothing accepted on this leg) resumes exactly where the
/// leg began — no spurious advance.
#[test]
fn content_paid_frontier_zero_paid_stays_at_start() {
    let group = CHUNK_GROUP_BYTES;
    assert_eq!(content_paid_frontier(0, 5 * group, 0), 0);
    assert_eq!(content_paid_frontier(group, 5 * group, 0), group);
}

use bytes::Bytes;

use super::{BlobCache, MemoryBlobCache, NoCache};

/// The default [`NoCache`] never hits and discards every `put`, so a caller
/// that injects no cache always fetches the whole range.
#[tokio::test]
async fn no_cache_always_misses_and_discards() -> anyhow::Result<()> {
    let cache = NoCache;
    let hash = [7u8; 32];
    cache.put(hash, 0, Bytes::from_static(b"hello")).await?;
    anyhow::ensure!(
        cache.get(hash, 0, 5).await?.is_none(),
        "the no-op cache must never hit"
    );
    Ok(())
}

/// The in-memory test cache round-trips one `(hash, range)`, and any other
/// hash / offset / length misses (the caller then fetches the complement).
#[tokio::test]
async fn memory_cache_round_trips_a_hash_range() -> anyhow::Result<()> {
    let cache = MemoryBlobCache::new();
    let hash = [1u8; 32];
    let other = [2u8; 32];
    cache.put(hash, 16, Bytes::from_static(b"abcd")).await?;
    anyhow::ensure!(
        cache.get(hash, 16, 4).await?.as_deref() == Some(&b"abcd"[..]),
        "an exact (hash, range) hit returns the stored bytes"
    );
    anyhow::ensure!(
        cache.get(other, 16, 4).await?.is_none(),
        "wrong hash misses"
    );
    anyhow::ensure!(
        cache.get(hash, 0, 4).await?.is_none(),
        "wrong offset misses"
    );
    anyhow::ensure!(
        cache.get(hash, 16, 2).await?.is_none(),
        "wrong length misses"
    );
    Ok(())
}
