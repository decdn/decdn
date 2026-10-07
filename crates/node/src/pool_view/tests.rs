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
    view.record_topup(pool(1), U256::from(100u64));
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 50)]);
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
    view.record_topup(pool(1), U256::from(2_500u64));
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
    );
    view.record_redeemed(pool(1), addr(101), &[lane(addr(2), 50)]);
    let status = view.status(pool(1)).await.unwrap();
    // remaining = 1000 − (200 + 100 + 50) = 650.
    assert_eq!(status.remaining, U256::from(650u64));
}

#[tokio::test]
async fn redeemed_is_idempotent_and_monotone() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(1_000u64));
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 300)]);
    // Replayed identical event (watcher retry / reorg rewind): no double count.
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 300)]);
    assert_eq!(
        view.status(pool(1)).await.unwrap().remaining,
        U256::from(700u64)
    );
    // A stale (lower) cumulative never lowers the total.
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 100)]);
    assert_eq!(
        view.status(pool(1)).await.unwrap().remaining,
        U256::from(700u64)
    );
    // A genuine advance folds only the delta.
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 450)]);
    assert_eq!(
        view.status(pool(1)).await.unwrap().remaining,
        U256::from(550u64)
    );
}

#[tokio::test]
async fn fully_redeemed_pool_reports_zero_not_none() {
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(500u64));
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 500)]);
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
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 400)]);
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
    view.record_redeemed(pool(1), addr(9), &[lane(addr(3), 400)]);
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
    view.record_resolved(pool(1), addr(7), U256::from(1_000u64), Lifecycle::Open);
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
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 400)]);
    view.record_resolved(pool(1), addr(8), U256::from(5_000u64), Lifecycle::Open);
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

#[tokio::test]
async fn record_resolved_rebuilds_remaining_without_double_count() {
    let view = PoolProjection::new();
    // Seed the FULL deposit with a zero redeemed baseline (as record_opened
    // does), so a post-seed PoolRedeemed for a lane that already had on-chain
    // history folds only its current cumulative — not double.
    view.record_resolved(pool(1), addr(7), U256::from(1_000u64), Lifecycle::Open);
    view.record_redeemed(pool(1), addr(9), &[lane(addr(2), 150)]);
    assert_eq!(
        view.status(pool(1)).await.unwrap().remaining,
        U256::from(850u64)
    );
}

#[tokio::test]
async fn record_resolved_carries_closing_lifecycle() {
    let view = PoolProjection::new();
    view.record_resolved(
        pool(1),
        addr(7),
        U256::from(1_000u64),
        Lifecycle::Closing {
            deadline: 1_900_000_000,
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
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 200), lane(addr(3), 90)]);
    view.record_redeemed(pool(1), addr(101), &[lane(addr(2), 350)]);
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
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 100)]);
    assert_eq!(view.signer_spent(pool(1), addr(2)), 100);
    // Drain at two more providers mid-stream.
    view.record_redeemed(pool(1), addr(101), &[lane(addr(2), 4_000)]);
    view.record_redeemed(pool(1), addr(102), &[lane(addr(2), 3_000)]);
    assert_eq!(view.signer_spent(pool(1), addr(2)), 7_100);
}

#[tokio::test]
async fn signer_spent_is_idempotent_and_monotone_under_replay() {
    // The per-signer total is a folded aggregate, so it must fold only positive
    // lane advances — a replayed or stale event must not double-count it, exactly
    // like `total_redeemed`.
    let view = PoolProjection::new();
    view.record_opened(pool(1), addr(7), U256::from(10_000u64));
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 300)]);
    // Replayed identical event (watcher retry / reorg rewind): no double count.
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 300)]);
    assert_eq!(view.signer_spent(pool(1), addr(2)), 300);
    // A stale (lower) cumulative never lowers the total.
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 100)]);
    assert_eq!(view.signer_spent(pool(1), addr(2)), 300);
    // A genuine advance folds only the delta.
    view.record_redeemed(pool(1), addr(100), &[lane(addr(2), 450)]);
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
