use super::*;
use alloy::primitives::{Bytes, U64};
use alloy::providers::ProviderBuilder;
use alloy::providers::mock::Asserter;

fn mocked() -> (Asserter, impl Provider + Clone) {
    let asserter = Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    (asserter, provider)
}

/// A snapshot sits the lag margin below the reported head while the
/// contract has code there.
#[tokio::test]
async fn snapshot_block_sits_the_margin_below_head() {
    let (asserter, provider) = mocked();
    asserter.push_success(&U64::from(1_000));
    asserter.push_success(&Bytes::from_static(&[0x60, 0x80]));

    let block = snapshot_block(&provider, Address::ZERO).await.ok();

    assert_eq!(block, Some(1_000 - SNAPSHOT_LAG_MARGIN_BLOCKS));
    assert_eq!(asserter.read_q().len(), 0);
}

/// A contract deployed inside the margin has no code at the lagged block, so
/// the snapshot takes the head — including a head below the margin, whose
/// lagged block saturates at genesis.
#[tokio::test]
async fn snapshot_block_takes_the_head_when_the_contract_is_younger() {
    let (asserter, provider) = mocked();
    asserter.push_success(&U64::from(1_000));
    asserter.push_success(&Bytes::new());
    asserter.push_success(&U64::from(SNAPSHOT_LAG_MARGIN_BLOCKS - 1));
    asserter.push_success(&Bytes::new());

    assert_eq!(
        snapshot_block(&provider, Address::ZERO).await.ok(),
        Some(1_000)
    );
    assert_eq!(
        snapshot_block(&provider, Address::ZERO).await.ok(),
        Some(SNAPSHOT_LAG_MARGIN_BLOCKS - 1)
    );
    assert_eq!(asserter.read_q().len(), 0);
}

/// A failed head read fails the snapshot.
#[tokio::test]
async fn snapshot_block_fails_on_a_failed_head_read() {
    let (asserter, provider) = mocked();
    asserter.push_failure_msg("boom");

    assert!(snapshot_block(&provider, Address::ZERO).await.is_err());
}
