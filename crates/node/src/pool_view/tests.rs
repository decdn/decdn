use super::*;

fn pool(n: u8) -> B256 {
    B256::repeat_byte(n)
}

fn status(remaining: u64) -> PoolStatus {
    PoolStatus {
        owner: Address::repeat_byte(0xAA),
        remaining: U256::from(remaining),
        lifecycle: Lifecycle::Open,
    }
}

#[tokio::test]
async fn a_read_answers_until_forgotten() {
    let projection = PoolProjection::new();
    assert!(projection.status(pool(1)).await.is_none());
    projection.record_read(pool(1), status(10));
    assert_eq!(
        projection.status(pool(1)).await.map(|s| s.remaining),
        Some(U256::from(10))
    );
    projection.record_read(pool(1), status(4));
    assert_eq!(
        projection.snapshot(pool(1)).map(|s| s.remaining),
        Some(U256::from(4)),
        "a newer read replaces the older"
    );
    projection.forget(pool(1));
    assert!(projection.snapshot(pool(1)).is_none());
}

/// A read stops answering admission once it is older than the refresh
/// interval, but still answers the non-blocking mid-stream read.
#[tokio::test(start_paused = true)]
async fn a_read_is_fresh_for_one_refresh_interval() {
    let projection = PoolProjection::new();
    projection.record_read(pool(1), status(10));
    assert!(projection.fresh(pool(1)).is_some());
    tokio::time::advance(POOL_REFRESH_INTERVAL).await;
    assert!(projection.fresh(pool(1)).is_none());
    assert!(projection.snapshot(pool(1)).is_some());
}

/// Pruning drops only old reads of pools with no live lane.
#[tokio::test(start_paused = true)]
async fn prune_idle_keeps_live_and_recent_pools() {
    let projection = PoolProjection::new();
    projection.record_read(pool(1), status(1));
    projection.record_read(pool(2), status(2));
    tokio::time::advance(POOL_REFRESH_INTERVAL).await;
    projection.record_read(pool(3), status(3));
    projection.prune_idle(&HashSet::from([pool(1)]));
    assert!(projection.snapshot(pool(1)).is_some(), "live");
    assert!(projection.snapshot(pool(2)).is_none(), "idle and old");
    assert!(projection.snapshot(pool(3)).is_some(), "recent");
}

#[test]
fn a_closed_pool_is_dropped_and_queued_once() {
    let projection = PoolProjection::new();
    projection.record_read(pool(1), status(1));
    projection.mark_closed(pool(1));
    projection.mark_closed(pool(1));
    assert!(projection.snapshot(pool(1)).is_none());
    assert_eq!(projection.take_closed(), vec![pool(1)]);
    assert!(projection.take_closed().is_empty());
}
