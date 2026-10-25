use alloy::primitives::{B256, Log as PrimitiveLog};
use alloy::providers::ProviderBuilder;
use alloy::providers::mock::Asserter;

use super::*;

const ROUTER: Address = Address::repeat_byte(0xF0);
const OPERATOR: Address = Address::repeat_byte(0x0E);

fn shares(operator: u64, buyback: u64, treasury: u64) -> [U256; 3] {
    [
        U256::from(operator),
        U256::from(buyback),
        U256::from(treasury),
    ]
}

fn log(data: alloy::primitives::LogData, block: u64, log_index: u64) -> Log {
    Log {
        inner: PrimitiveLog {
            address: ROUTER,
            data,
        },
        block_hash: None,
        block_number: Some(block),
        block_timestamp: None,
        transaction_hash: Some(B256::repeat_byte(u8::try_from(block % 256).unwrap_or(0))),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}

fn settled(block: u64, log_index: u64, amount: u64, bytes: u64) -> Log {
    let event = FeeRouter::Settled {
        operator: OPERATOR,
        bytesDelivered: U256::from(bytes),
        amount: U256::from(amount),
        epoch: block / 1_000,
    };
    log(event.encode_log_data(), block, log_index)
}

fn shares_updated(block: u64, log_index: u64, new: [U256; 3]) -> Log {
    let event = FeeRouter::SharesUpdated { newShares: new };
    log(event.encode_log_data(), block, log_index)
}

#[test]
fn net_floors_each_bucket_like_the_router() {
    // 1001 gross at 60/30/10: buyback floor(300.3) = 300, treasury
    // floor(100.1) = 100, the operator keeps 601 (the dust included).
    assert_eq!(
        net_of(U256::from(1_001u64), shares(6000, 3000, 1000)),
        U256::from(601u64)
    );
    assert_eq!(
        net_of(U256::from(1_000u64), shares(9000, 0, 1000)),
        U256::from(900u64)
    );
    assert_eq!(net_of(U256::ZERO, shares(6000, 3000, 1000)), U256::ZERO);
    // A malformed split over 100 % saturates to 0 rather than wrapping.
    assert_eq!(net_of(U256::from(10u64), shares(0, 9000, 9000)), U256::ZERO);
}

#[test]
fn shares_at_picks_the_latest_change_at_or_before_the_position() {
    let history = shares_history(&[
        shares_updated(100, 0, shares(6000, 3000, 1000)),
        shares_updated(500, 3, shares(9000, 0, 1000)),
    ]);
    let current = shares(5000, 4000, 1000);
    assert_eq!(
        shares_at(&history, current, (100, 0)),
        shares(6000, 3000, 1000)
    );
    assert_eq!(
        shares_at(&history, current, (499, 9)),
        shares(6000, 3000, 1000)
    );
    // Same block, earlier log: the change has not happened yet.
    assert_eq!(
        shares_at(&history, current, (500, 2)),
        shares(6000, 3000, 1000)
    );
    assert_eq!(
        shares_at(&history, current, (500, 3)),
        shares(9000, 0, 1000)
    );
    assert_eq!(
        shares_at(&history, current, (9_999, 0)),
        shares(9000, 0, 1000)
    );
    // Before the first known change: the first change stands in (the caller
    // warns). No history at all: the current split.
    assert_eq!(
        shares_at(&history, current, (50, 0)),
        shares(6000, 3000, 1000)
    );
    assert_eq!(shares_at(&[], current, (50, 0)), current);
}

#[test]
fn rows_decode_in_chain_order_with_the_split_in_force() {
    let history = shares_history(&[shares_updated(1, 0, shares(6000, 3000, 1000))]);
    let logs = vec![settled(300, 2, 1_000, 4_096), settled(200, 0, 2_000, 8_192)];
    let rows = rows_from_logs(&logs, &history, shares(9000, 0, 1000));
    assert_eq!(rows.len(), 2);
    let first = rows.first().expect("row");
    assert_eq!((first.block_number, first.log_index), (200, 0));
    assert_eq!(first.gross_micro_usdc, 2_000);
    assert_eq!(first.net_micro_usdc, 1_200);
    assert_eq!(first.bytes_delivered, 8_192);
    assert_eq!(first.tx_hash, B256::repeat_byte(200));
    assert_eq!(first.landed_at_unix, 0, "stamped by the caller, not here");
    assert_eq!(rows.last().map(|r| r.net_micro_usdc), Some(600));
}

/// A block at `timestamp`, as `eth_getBlockByNumber` returns it.
fn block(timestamp: u64) -> alloy::rpc::types::Block {
    let mut block: alloy::rpc::types::Block = alloy::rpc::types::Block::default();
    block.header.inner.timestamp = timestamp;
    block
}

#[tokio::test]
async fn block_at_or_after_binary_searches_the_headers() {
    // Blocks 0..=7 at 100, 110, …, 170; target 135 is block 4 (140). The
    // search reads `hi` first, then probes 3, 5, 4.
    let asserter = Asserter::new();
    for probe in [7u64, 3, 5, 4] {
        asserter.push_success(&block(100 + 10 * probe));
    }
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    assert_eq!(block_at_or_after(&provider, 135, 0, 7).await.unwrap(), 4);
}

#[tokio::test]
async fn block_at_or_after_past_head_is_an_empty_period() {
    let asserter = Asserter::new();
    asserter.push_success(&block(170));
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    assert_eq!(block_at_or_after(&provider, 500, 0, 7).await.unwrap(), 8);
    let asserter = Asserter::new();
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);
    assert_eq!(block_at_or_after(&provider, 500, 9, 7).await.unwrap(), 8);
}

fn args(
    rpc_url: Option<&str>,
    payment_pool: Option<&str>,
    from_block: Option<u64>,
) -> cli::EarningsArgs {
    cli::EarningsArgs {
        admin_url: None,
        config: None,
        json: false,
        timeout_ms: 5_000,
        since: None,
        address: Some(OPERATOR.to_string()),
        rpc_url: rpc_url.map(str::to_owned),
        payment_pool_address: payment_pool.map(str::to_owned),
        from_block,
    }
}

#[test]
fn coordinates_default_to_the_known_testnet() {
    let coords = resolve_coordinates(&args(None, None, None), &chain_ctx::FileConfig::default())
        .expect("known chain fills every field");
    let known = known_chains::by_chain_id(DEFAULT_CHAIN_ID).expect("default chain is known");
    assert_eq!(coords.rpc_url, known.public_rpc);
    assert_eq!(coords.from_block, known.l2_start_block);
    assert_eq!(
        coords.payment_pool.to_string(),
        known.addresses().expect("manifest").payment_pool
    );
    assert_eq!(coords.span.get(), DEFAULT_GET_LOGS_MAX_BLOCK_SPAN);
}

#[test]
fn flags_override_the_known_chain() {
    let pool = Address::repeat_byte(0x42);
    let coords = resolve_coordinates(
        &args(
            Some("http://rpc.example"),
            Some(&pool.to_string()),
            Some(77),
        ),
        &chain_ctx::FileConfig::default(),
    )
    .expect("flags resolve");
    assert_eq!(coords.rpc_url, "http://rpc.example");
    assert_eq!(coords.payment_pool, pool);
    assert_eq!(coords.from_block, 77);
}
