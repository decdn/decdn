use alloy::primitives::LogData;
use alloy::providers::ProviderBuilder;

use super::*;
use crate::metrics::Metrics;

/// `route()` never calls the chain (it only constructs a contract handle),
/// so a mocked client with no scripted responses is sufficient here.
fn mock_provider() -> impl Provider + Clone + 'static {
    ProviderBuilder::new().connect_mocked_client(alloy::providers::mock::Asserter::new())
}

/// Build a `FeeSharesSink` over a mocked, never-called provider. No live
/// contract: `on_tick_complete`'s `getShares()` call is never exercised by
/// tests that only drive `apply`, and the mocked client panics loudly if a
/// test path did reach it unscripted.
fn for_test(shares: OperatorShares) -> FeeSharesSink<impl Provider + Clone + 'static> {
    FeeSharesSink {
        contract: FeeRouter::new(Address::ZERO, mock_provider()),
        shares,
        poll_interval: Duration::from_secs(u64::MAX),
        last_poll: Some(Instant::now()),
        metrics: Arc::new(crate::metrics::Metrics::new()),
    }
}

/// Encode a synthetic `SharesUpdated { newShares }` log, the same way the
/// live poller would hand one to `apply`.
fn synthetic_shares_updated_log(new_shares: [U256; 3]) -> Log {
    let event = FeeRouter::SharesUpdated {
        newShares: new_shares,
    };
    Log {
        inner: alloy::primitives::Log {
            address: Address::ZERO,
            data: event.encode_log_data(),
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn shares_updated_log_updates_cell() {
    let shares = OperatorShares::new(6000);
    let mut sink = for_test(shares.clone());
    let log = synthetic_shares_updated_log([
        U256::from(4000u64),
        U256::from(5000u64),
        U256::from(1000u64),
    ]);
    sink.apply(log).await.expect("apply ok");
    assert_eq!(shares.bps(), 4000);
}

/// A log whose topic0 doesn't match `SharesUpdated` is skipped, not
/// errored, and never touches the cell.
#[tokio::test]
async fn foreign_topic_log_is_skipped() {
    let shares = OperatorShares::new(6000);
    let mut sink = for_test(shares.clone());
    let mut log = synthetic_shares_updated_log([
        U256::from(4000u64),
        U256::from(5000u64),
        U256::from(1000u64),
    ]);
    log.inner.data = LogData::empty();
    sink.apply(log)
        .await
        .expect("apply ok even for a foreign topic");
    assert_eq!(
        shares.bps(),
        6000,
        "cell must be untouched by a non-matching log"
    );
}

/// The fee-shares watcher's `Route` must carry exactly the
/// `SharesUpdated` topic0 and start at head (`HeadMinusWindow { 0 }`) —
/// this fails if the topic0 were ever dropped or swapped for another event.
#[test]
fn route_watches_shares_updated_from_head() {
    let metrics = Arc::new(Metrics::new());
    let route = route(
        mock_provider(),
        Address::repeat_byte(0x22),
        OperatorShares::new(0),
        Duration::from_hours(1),
        &metrics,
    );

    assert_eq!(route.addresses, vec![Address::repeat_byte(0x22)]);
    assert_eq!(
        route.topic0s,
        vec![FeeRouter::SharesUpdated::SIGNATURE_HASH],
        "must watch exactly SharesUpdated — no more, no fewer"
    );
    assert!(
        matches!(
            route.start,
            CursorStart::HeadMinusWindow { window_blocks: 0 }
        ),
        "must start at head with no lookback window (HeadMinusWindow{{ 0 }})"
    );
}
