use super::*;

fn pool(n: u8) -> B256 {
    B256::repeat_byte(n)
}

fn addr(n: u8) -> Address {
    Address::repeat_byte(n)
}

fn lane(signer: Address, new_cumulative: u64) -> PaymentPool::LaneSettled {
    PaymentPool::LaneSettled {
        signer,
        newPaidCumulative: new_cumulative,
        bytesPaid: 0,
    }
}

#[tokio::test]
async fn unknown_pool_is_none() {
    let view = PoolProjection::new();
    assert!(view.status(pool(1)).await.is_none());
    // A top-up or redemption for a never-opened pool creates no entry.
    view.record_topup(pool(1), U256::from(100u64), None);
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 50)], None);
    assert!(view.status(pool(1)).await.is_none());
}

#[tokio::test]
async fn opened_pool_reports_full_deposit() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    let status = view.status(pool(1)).await.expect("opened pool is known");
    assert_eq!(status.owner, addr(7));
    assert_eq!(status.remaining, U256::from(1_000u64));
}

#[tokio::test]
async fn topup_raises_remaining() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    view.record_topup(pool(1), U256::from(2_500u64), None);
    let status = view.status(pool(1)).await.unwrap();
    assert_eq!(status.remaining, U256::from(2_500u64));
}

#[tokio::test]
async fn redeemed_subtracts_across_providers_and_lanes() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    // Two providers, two signers — totalRedeemed sums every lane.
    view.record_redeemed(
        pool(1),
        addr(100),
        &[lane(addr(2), 200), lane(addr(3), 100)],
        None,
    );
    view.record_redeemed(pool(1), addr(101), &[lane(addr(2), 50)], None);
    let status = view.status(pool(1)).await.unwrap();
    // remaining = 1000 − (200 + 100 + 50) = 650.
    assert_eq!(status.remaining, U256::from(650u64));
}

#[tokio::test]
async fn redeemed_is_idempotent_and_monotone() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 300)], None);
    // Replayed identical event (watcher retry / reorg rewind): no double count.
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 300)], None);
    assert_eq!(
        view.status(pool(1)).await.unwrap().remaining,
        U256::from(700u64)
    );
    // A stale (lower) cumulative never lowers the total.
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 100)], None);
    assert_eq!(
        view.status(pool(1)).await.unwrap().remaining,
        U256::from(700u64)
    );
    // A genuine advance folds only the delta.
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 450)], None);
    assert_eq!(
        view.status(pool(1)).await.unwrap().remaining,
        U256::from(550u64)
    );
}

#[tokio::test]
async fn fully_redeemed_pool_reports_zero_not_none() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(500u64));
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 500)], None);
    let status = view.status(pool(1)).await.unwrap();
    // Matches on-chain getPool: owner stays, remaining is zero (solvency gate
    // then refuses; the pool is not dropped until reclaim).
    assert_eq!(status.owner, addr(7));
    assert_eq!(status.remaining, U256::ZERO);
}

#[tokio::test]
async fn over_redeemed_saturates_to_zero() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(100u64));
    // Should never happen on-chain, but the projection must not underflow.
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 400)], None);
    assert_eq!(view.status(pool(1)).await.unwrap().remaining, U256::ZERO);
}

#[tokio::test]
async fn reclaim_drops_the_entry() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    view.forget(pool(1));
    assert!(view.status(pool(1)).await.is_none());
}

#[tokio::test]
async fn cached_status_matches_status() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    let s = view.status(pool(1)).await.unwrap();
    let c = view.cached_status(pool(1)).await.unwrap();
    assert_eq!(s.owner, c.owner);
    assert_eq!(s.remaining, c.remaining);
}

#[tokio::test]
async fn record_closing_sets_lifecycle_deadline() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    view.record_closing(pool(1), 1_900_000_000);
    let s = view.status(pool(1)).await.unwrap();
    assert_eq!(
        s.lifecycle,
        Lifecycle::Closing {
            deadline: 1_900_000_000
        }
    );
    assert_eq!(
        s.remaining,
        U256::from(1_000u64),
        "closing does not change remaining"
    );
}

#[tokio::test]
async fn opened_pool_defaults_to_open_lifecycle() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    assert_eq!(
        view.status(pool(1)).await.unwrap().lifecycle,
        Lifecycle::Open
    );
}

#[tokio::test]
async fn record_closing_unknown_pool_is_noop() {
    let view = PoolProjection::new();
    view.record_closing(pool(1), 1_900_000_000);
    assert!(view.status(pool(1)).await.is_none());
}

#[tokio::test]
async fn redeemed_still_folds_after_closing() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    view.record_closing(pool(1), 1_900_000_000);
    view.record_redeemed(pool(1), addr(9), &[lane(addr(3), 400)], None);
    let s = view.status(pool(1)).await.unwrap();
    assert_eq!(s.remaining, U256::from(600u64));
    assert_eq!(
        s.lifecycle,
        Lifecycle::Closing {
            deadline: 1_900_000_000
        }
    );
}

#[tokio::test]
async fn record_resolved_seeds_an_absent_pool() {
    let view = PoolProjection::new();
    // A pool opened before the watcher's cold-start head is absent until the
    // resolver folds a getPool snapshot.
    assert!(view.status(pool(1)).await.is_none());
    view.record_resolved(addr(7), read(pool(1), 1_000, 0));
    let s = view.status(pool(1)).await.expect("resolved pool is known");
    assert_eq!(s.owner, addr(7));
    assert_eq!(s.remaining, U256::from(1_000u64));
    assert_eq!(s.lifecycle, Lifecycle::Open);
}

#[tokio::test]
async fn record_resolved_does_not_clobber_a_live_entry() {
    let view = PoolProjection::new();
    // An event fold (the authoritative writer) has already recorded the pool
    // with a redemption drawn down; a late resolve for the same pool must not
    // overwrite the folded remaining back up to the full deposit.
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 400)], None);
    view.record_resolved(addr(8), read(pool(1), 5_000, 0));
    let s = view.status(pool(1)).await.unwrap();
    assert_eq!(
        s.owner,
        addr(7),
        "resolve must not overwrite the folded owner"
    );
    assert_eq!(
        s.remaining,
        U256::from(600u64),
        "resolve must not overwrite the folded remaining"
    );
}

async fn remaining(view: &PoolProjection, id: B256) -> u64 {
    let status = view.status(id).await.expect("pool is known");
    u64::try_from(status.remaining).expect("remaining fits u64")
}

fn read(id: B256, deposit: u64, total_redeemed: u64) -> PoolRead {
    PoolRead {
        pool_id: id,
        deposit,
        total_redeemed,
        lifecycle: Lifecycle::Open,
    }
}

#[tokio::test]
async fn record_resolved_counts_onchain_redemptions() {
    let view = PoolProjection::new();
    // A pool drained before the restart reads as drained from the first admission.
    view.record_resolved(addr(7), read(pool(1), 1_000, 1_000));
    assert_eq!(remaining(&view, pool(1)).await, 0);
    assert_eq!(view.pending_rereads(), vec![pool(1)]);
}

#[tokio::test]
async fn unanchored_seed_does_not_double_count_a_lane_with_history() {
    let view = PoolProjection::new();
    view.record_resolved(addr(7), read(pool(1), 1_000, 600));
    // The lane's 450 may sit wholly inside the snapshot's 600: count neither twice.
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 450)], Some(5));
    assert_eq!(remaining(&view, pool(1)).await, 400);
    assert_eq!(view.pending_rereads(), vec![pool(1)]);
}

#[tokio::test]
async fn anchored_snapshot_counts_a_known_lanes_advance_once() {
    let view = PoolProjection::new();
    view.record_resolved(addr(7), read(pool(1), 1_000, 600));
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 450)], Some(5));
    let report = view.reanchor(10, &[read(pool(1), 1_000, 650)]);
    assert_eq!(
        report,
        ReanchorReport {
            anchored: 1,
            skipped: vec![]
        }
    );
    assert!(view.pending_rereads().is_empty());
    assert_eq!(remaining(&view, pool(1)).await, 350);
    // Only the 50 the lane advanced past its folded 450 is new.
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 500)], Some(11));
    assert_eq!(remaining(&view, pool(1)).await, 300);
    assert!(view.pending_rereads().is_empty());
    // A later read holds those 50: the count above the old anchor restarts.
    view.reanchor(12, &[read(pool(1), 1_000, 700)]);
    assert_eq!(remaining(&view, pool(1)).await, 300);
}

#[tokio::test]
async fn a_lane_with_no_baseline_marks_the_snapshot_for_a_reread() {
    let view = PoolProjection::new();
    view.record_resolved(addr(7), read(pool(1), 1_000, 600));
    view.reanchor(10, &[read(pool(1), 1_000, 600)]);
    // 300 may be all new or mostly history the snapshot already holds: count
    // neither, and ask for a re-read.
    view.record_redeemed(pool(1), addr(9), &[lane(addr(3), 300)], Some(11));
    assert_eq!(remaining(&view, pool(1)).await, 400);
    assert_eq!(view.pending_rereads(), vec![pool(1)]);
    view.reanchor(12, &[read(pool(1), 1_000, 900)]);
    assert_eq!(remaining(&view, pool(1)).await, 100);
    assert!(view.pending_rereads().is_empty());
}

#[tokio::test]
async fn a_stale_snapshot_keeps_counting_advances_it_can_place() {
    let view = PoolProjection::new();
    // 1000 redeemed on lanes the watcher never saw.
    view.record_resolved(addr(7), read(pool(1), 2_000, 1_000));
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 100)], Some(5));
    view.reanchor(10, &[read(pool(1), 2_000, 1_000)]);
    // An unplaceable advance makes the snapshot stale...
    view.record_redeemed(pool(1), addr(9), &[lane(addr(3), 50)], Some(11));
    // ...but a known lane's advance above the anchor still counts.
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 600)], Some(12));
    // redeemed ≥ 1000 (read) + 500 (lane 2's advance); the lane fold is 650.
    assert_eq!(remaining(&view, pool(1)).await, 500);
    assert_eq!(view.pending_rereads(), vec![pool(1)]);
}

#[tokio::test]
async fn replayed_redemptions_at_or_below_the_anchor_are_not_counted_again() {
    let view = PoolProjection::new();
    view.record_resolved(addr(7), read(pool(1), 1_000, 400));
    view.reanchor(20, &[read(pool(1), 1_000, 400)]);
    // A watcher catching up from an old checkpoint folds history the read holds.
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 300)], Some(5));
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 400)], Some(20));
    assert_eq!(remaining(&view, pool(1)).await, 600);
    assert!(view.pending_rereads().is_empty());
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 450)], Some(21));
    assert_eq!(remaining(&view, pool(1)).await, 550);
}

#[tokio::test]
async fn reanchor_below_a_folded_event_is_ignored() {
    let view = PoolProjection::new();
    view.record_resolved(addr(7), read(pool(1), 1_000, 600));
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 100)], Some(30));
    // A lagging backend's read at block 25 predates the folded block-30 event.
    // Its values pass the value guard, so only the block order rejects it.
    let report = view.reanchor(25, &[read(pool(1), 1_000, 650)]);
    assert_eq!(
        report.skipped,
        vec![ReanchorSkip {
            read: read(pool(1), 1_000, 650),
            reason: ReanchorStale::BelowFolded { folded_through: 30 },
        }]
    );
    assert_eq!(report.anchored, 0);
    assert_eq!(view.pending_rereads(), vec![pool(1)]);
    assert_eq!(remaining(&view, pool(1)).await, 400);

    view.record_resolved(addr(7), read(pool(2), 1_000, 0));
    view.record_topup(pool(2), U256::from(2_000u64), Some(30));
    let report = view.reanchor(25, &[read(pool(2), 2_500, 0)]);
    assert_eq!(
        report.skipped.first().map(|skip| skip.reason),
        Some(ReanchorStale::BelowFolded { folded_through: 30 })
    );
    assert_eq!(remaining(&view, pool(2)).await, 2_000);
    assert!(view.pending_rereads().contains(&pool(2)));
}

#[tokio::test]
async fn reanchor_below_the_seed_is_ignored() {
    let view = PoolProjection::new();
    // A pool drained before the restart, with no lane folded yet.
    view.record_resolved(addr(7), read(pool(1), 1_000, 1_000));
    // A backend behind the admit read still sees nothing redeemed.
    let report = view.reanchor(10, &[read(pool(1), 1_000, 0)]);
    assert_eq!(
        report.skipped.first().map(|skip| skip.reason),
        Some(ReanchorStale::LowerRedeemed { held: 1_000 })
    );
    assert_eq!(remaining(&view, pool(1)).await, 0);
    assert_eq!(view.pending_rereads(), vec![pool(1)]);

    // A deposit below the seed's is just as stale.
    view.record_resolved(addr(7), read(pool(2), 3_000, 0));
    let report = view.reanchor(10, &[read(pool(2), 1_000, 0)]);
    assert_eq!(
        report.skipped.first().map(|skip| skip.reason),
        Some(ReanchorStale::LowerDeposit { held: 3_000 })
    );
    assert_eq!(remaining(&view, pool(2)).await, 3_000);
    assert!(view.pending_rereads().contains(&pool(2)));

    // A read that catches up anchors the pool.
    let report = view.reanchor(11, &[read(pool(1), 1_000, 1_000)]);
    assert_eq!(
        report,
        ReanchorReport {
            anchored: 1,
            skipped: vec![]
        }
    );
    assert_eq!(view.pending_rereads(), vec![pool(2)]);
}

#[tokio::test]
async fn a_redemption_without_a_block_marks_a_reread_only_when_it_advances() {
    let view = PoolProjection::new();
    view.record_resolved(addr(7), read(pool(1), 1_000, 600));
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 100)], Some(5));
    view.reanchor(10, &[read(pool(1), 1_000, 600)]);
    // A replay advances no lane: nothing to re-read.
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 100)], None);
    assert!(view.pending_rereads().is_empty());
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 150)], None);
    assert_eq!(view.pending_rereads(), vec![pool(1)]);
}

#[tokio::test]
async fn reanchor_leaves_an_event_opened_pool_alone() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 100)], Some(3));
    // Neither anchored nor stale: the pool has no snapshot to anchor.
    assert_eq!(
        view.reanchor(10, &[read(pool(1), 1_000, 999)]),
        ReanchorReport::default()
    );
    assert_eq!(remaining(&view, pool(1)).await, 900);
    assert!(view.pending_rereads().is_empty());
}

#[tokio::test]
async fn reanchor_keeps_a_folded_close() {
    let view = PoolProjection::new();
    view.record_resolved(addr(7), read(pool(1), 1_000, 0));
    view.record_closing(pool(1), 1_900_000_000);
    // A lagging read still sees the pool Open.
    view.reanchor(10, &[read(pool(1), 1_000, 0)]);
    assert_eq!(
        view.status(pool(1)).await.unwrap().lifecycle,
        Lifecycle::Closing {
            deadline: 1_900_000_000
        }
    );
}

#[tokio::test]
async fn snapshot_deposit_ignores_topups_the_read_holds() {
    let view = PoolProjection::new();
    view.record_resolved(addr(7), read(pool(1), 3_000, 0));
    // A replayed open never lowers the read's deposit.
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    assert_eq!(remaining(&view, pool(1)).await, 3_000);
    view.reanchor(10, &[read(pool(1), 3_000, 0)]);
    view.record_topup(pool(1), U256::from(2_000u64), Some(5));
    assert_eq!(remaining(&view, pool(1)).await, 3_000);
    view.record_topup(pool(1), U256::from(4_000u64), Some(11));
    assert_eq!(remaining(&view, pool(1)).await, 4_000);
}

#[tokio::test]
async fn snapshot_deposit_only_rises_on_an_unordered_topup() {
    let view = PoolProjection::new();
    view.record_resolved(addr(7), read(pool(1), 3_000, 0));
    // Unanchored: the read's block is unknown, so an older top-up cannot lower it.
    view.record_topup(pool(1), U256::from(2_000u64), Some(5));
    assert_eq!(remaining(&view, pool(1)).await, 3_000);
    view.reanchor(10, &[read(pool(1), 3_000, 0)]);
    // No block: the top-up cannot be ordered against the anchor either.
    view.record_topup(pool(1), U256::from(2_500u64), None);
    assert_eq!(remaining(&view, pool(1)).await, 3_000);
}

#[tokio::test]
async fn reanchor_takes_a_closing_lifecycle_from_the_read() {
    let view = PoolProjection::new();
    view.record_resolved(addr(7), read(pool(1), 1_000, 0));
    let closing = Lifecycle::Closing {
        deadline: 1_900_000_000,
    };
    view.reanchor(
        10,
        &[PoolRead {
            lifecycle: closing,
            ..read(pool(1), 1_000, 0)
        }],
    );
    assert_eq!(view.status(pool(1)).await.unwrap().lifecycle, closing);
}

#[tokio::test]
async fn event_opened_topup_is_absolute() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    // A reorg that swaps a top-up for a smaller one lowers the deposit again.
    view.record_topup(pool(1), U256::from(10_000u64), Some(5));
    view.record_topup(pool(1), U256::from(6_000u64), Some(5));
    assert_eq!(remaining(&view, pool(1)).await, 6_000);
}

#[tokio::test]
async fn record_resolved_carries_closing_lifecycle() {
    let view = PoolProjection::new();
    view.record_resolved(
        addr(7),
        PoolRead {
            lifecycle: Lifecycle::Closing {
                deadline: 1_900_000_000,
            },
            ..read(pool(1), 1_000, 0)
        },
    );
    assert_eq!(
        view.status(pool(1)).await.unwrap().lifecycle,
        Lifecycle::Closing {
            deadline: 1_900_000_000
        }
    );
}

#[tokio::test]
async fn signer_spent_sums_a_signers_lanes_across_providers() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(10_000u64));
    // Signer 2 redeems at two providers; signer 3 redeems at one. `signer_spent`
    // for signer 2 sums BOTH of its lanes (the shared-cap total), and ignores
    // signer 3's lane.
    view.record_redeemed(
        pool(1),
        addr(100),
        &[lane(addr(2), 200), lane(addr(3), 90)],
        None,
    );
    view.record_redeemed(pool(1), addr(101), &[lane(addr(2), 350)], None);
    assert_eq!(view.signer_spent(pool(1), addr(2)), 550);
    assert_eq!(view.signer_spent(pool(1), addr(3)), 90);
    // A signer with no lane in the pool has spent nothing.
    assert_eq!(view.signer_spent(pool(1), addr(9)), 0);
    // The cross-provider view reads the same through the trait surface.
    assert_eq!(view.signer_spent_cached(pool(1), addr(2)).await, Some(550));
}

#[tokio::test]
async fn signer_spent_unknown_pool_is_zero() {
    // A pool the projection has not folded (opened before the watcher's
    // cold-start head) reports zero spent, so the mid-stream re-check's
    // `held_cap − spent` headroom is its widest — the fail-toward-serving
    // direction.
    let view = PoolProjection::new();
    assert_eq!(view.signer_spent(pool(1), addr(2)), 0);
}

#[tokio::test]
async fn signer_spent_tracks_a_drain_since_admit() {
    // Model the mid-stream drain the re-check catches: at admit the signer has
    // spent little, then it drains its shared cap at OTHER providers while a
    // stream is live here — `signer_spent` rises as those redemptions fold.
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(10_000u64));
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 100)], None);
    assert_eq!(view.signer_spent(pool(1), addr(2)), 100);
    // Drain at two more providers mid-stream.
    view.record_redeemed(pool(1), addr(101), &[lane(addr(2), 4_000)], None);
    view.record_redeemed(pool(1), addr(102), &[lane(addr(2), 3_000)], None);
    assert_eq!(view.signer_spent(pool(1), addr(2)), 7_100);
}

#[tokio::test]
async fn signer_spent_is_idempotent_and_monotone_under_replay() {
    // The per-signer total is a folded aggregate, so it must fold only positive
    // lane advances — a replayed or stale event must not double-count it, exactly
    // like `total_redeemed`.
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(10_000u64));
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 300)], None);
    // Replayed identical event (watcher retry / reorg rewind): no double count.
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 300)], None);
    assert_eq!(view.signer_spent(pool(1), addr(2)), 300);
    // A stale (lower) cumulative never lowers the total.
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 100)], None);
    assert_eq!(view.signer_spent(pool(1), addr(2)), 300);
    // A genuine advance folds only the delta.
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 450)], None);
    assert_eq!(view.signer_spent(pool(1), addr(2)), 450);
}

#[tokio::test]
async fn forget_clears_closing_pool() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    view.record_closing(pool(1), 1_900_000_000);
    view.forget(pool(1));
    assert!(view.status(pool(1)).await.is_none());
}
