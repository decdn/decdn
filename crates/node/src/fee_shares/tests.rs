use alloy::primitives::{Bytes, U256};
use alloy::providers::ProviderBuilder;
use alloy::providers::mock::Asserter;
use alloy::sol_types::SolValue;

use super::*;
use crate::chain_events::boot_retry::BOOT_CHAIN_RETRY_BUDGET;
use crate::metrics::Metrics;

const FLOOR: u16 = 4000;

/// `store` hands back the share it replaced, so the watcher can tell a real
/// change from a re-read of the same value.
#[test]
fn store_returns_the_replaced_share() {
    let shares = OperatorShares::new(6000);
    assert_eq!(shares.store(6000), 6000);
    assert_eq!(shares.store(4000), 6000);
    assert_eq!(shares.bps(), 4000);
}

fn retries(metrics: &Metrics) -> u64 {
    let text = metrics.encode().unwrap();
    text.lines()
        .find_map(|l| l.strip_prefix("decdn_chain_boot_read_retries_total "))
        .and_then(|v| v.parse().ok())
        .unwrap()
}

fn outage() -> alloy_json_rpc::ErrorPayload {
    serde_json::from_value(serde_json::json!({
        "code": 19,
        "message": "Temporary internal error. Please retry",
    }))
    .unwrap()
}

async fn seed(asserter: &Asserter, boot: &BootRetry) -> FeeShareSeed {
    seed_from_chain(
        ProviderBuilder::new().connect_mocked_client(asserter.clone()),
        Address::repeat_byte(0x11),
        FLOOR,
        boot,
    )
    .await
}

/// A transient `feeRouter()` error is retried, so the router — and with it
/// the fee-shares watcher — is not lost for the life of the process.
#[tokio::test(start_paused = true)]
async fn a_transient_fee_router_error_is_retried() {
    let router = Address::repeat_byte(0x22);
    let asserter = Asserter::new();
    asserter.push_failure(outage());
    asserter.push_success(&Bytes::from(router.abi_encode()));
    let shares = [
        U256::from(4500u64),
        U256::from(3500u64),
        U256::from(2000u64),
    ];
    asserter.push_success(&Bytes::from(shares.abi_encode()));
    let metrics = Arc::new(Metrics::new());

    let seed = seed(
        &asserter,
        &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
    )
    .await;

    assert_eq!(
        seed,
        FeeShareSeed {
            router: Some(router),
            operator_bps: 4500,
        }
    );
    assert_eq!(retries(&metrics), 1);
}

/// No contract at the pool address is deterministic: the seed falls back to
/// the floor at once, with no router to watch.
#[tokio::test(start_paused = true)]
async fn a_deterministic_fee_router_error_falls_back_at_once() {
    let asserter = Asserter::new();
    asserter.push_success(&Bytes::new());
    let metrics = Arc::new(Metrics::new());
    let start = tokio::time::Instant::now();

    let seed = seed(
        &asserter,
        &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
    )
    .await;

    assert_eq!(
        seed,
        FeeShareSeed {
            router: None,
            operator_bps: FLOOR,
        }
    );
    assert_eq!(retries(&metrics), 0);
    assert_eq!(start.elapsed(), std::time::Duration::ZERO);
}

/// A stalled provider cannot wedge boot: the fee reads are bounded by the
/// per-call timeout and fall back to the floor share.
#[tokio::test(start_paused = true)]
async fn a_stalled_provider_falls_back_to_the_floor() {
    use crate::chain_events::test_support::{bounded, hanging_provider};

    let metrics = Arc::new(Metrics::new());
    let seed = bounded(
        "fee-share seed",
        seed_from_chain(
            hanging_provider(),
            Address::repeat_byte(0x11),
            FLOOR,
            &BootRetry::single_attempt(Arc::clone(&metrics)),
        ),
    )
    .await;

    assert_eq!(
        seed,
        FeeShareSeed {
            router: None,
            operator_bps: FLOOR,
        }
    );
}

/// A `getShares()` read that never succeeds seeds the floor but keeps the
/// router, so the watcher's periodic re-read can recover the share.
#[tokio::test(start_paused = true)]
async fn a_failed_shares_read_keeps_the_router() {
    let router = Address::repeat_byte(0x22);
    let asserter = Asserter::new();
    asserter.push_success(&Bytes::from(router.abi_encode()));
    asserter.push_failure(outage());
    let metrics = Arc::new(Metrics::new());

    let seed = seed(&asserter, &BootRetry::single_attempt(Arc::clone(&metrics))).await;

    assert_eq!(
        seed,
        FeeShareSeed {
            router: Some(router),
            operator_bps: FLOOR,
        }
    );
}

#[test]
fn shares_narrow_to_operator_bps() {
    let shares = [
        U256::from(6000u64),
        U256::from(3000u64),
        U256::from(1000u64),
    ];
    assert_eq!(operator_bps_from_shares(shares, "0xrouter").unwrap(), 6000);
}

#[test]
fn shares_above_denominator_are_rejected() {
    let shares = [U256::from(10_001u64), U256::ZERO, U256::ZERO];
    assert!(operator_bps_from_shares(shares, "0xrouter").is_err());
}

#[test]
fn cell_round_trips_and_is_shared_across_clones() {
    let cell = OperatorShares::new(6000);
    let clone = cell.clone();
    cell.store(4000);
    assert_eq!(clone.bps(), 4000);
}
