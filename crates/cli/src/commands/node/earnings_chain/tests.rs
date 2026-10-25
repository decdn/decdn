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
    ])
    .expect("decodes");
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
    // Before every entry, or no history at all: the current split. `scan_rows`
    // inserts the split in force before the first change, so the first case
    // is unreachable from it.
    assert_eq!(shares_at(&history, current, (50, 0)), current);
    assert_eq!(shares_at(&[], current, (50, 0)), current);
}

#[test]
fn rows_decode_in_chain_order_with_the_split_in_force() {
    let history =
        shares_history(&[shares_updated(1, 0, shares(6000, 3000, 1000))]).expect("decodes");
    let logs = vec![settled(300, 2, 1_000, 4_096), settled(200, 0, 2_000, 8_192)];
    let rows = rows_from_logs(&logs, &history, shares(9000, 0, 1000)).expect("decodes");
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

#[test]
fn an_undecodable_or_positionless_log_fails_the_scan() {
    let mut garbage = settled(300, 2, 1_000, 4_096);
    garbage.inner.data = alloy::primitives::LogData::new_unchecked(
        garbage.inner.data.topics().to_vec(),
        alloy::primitives::Bytes::from(vec![0u8; 3]),
    );
    let err = rows_from_logs(&[garbage], &[], shares(9000, 0, 1000)).expect_err("ABI drift");
    assert!(format!("{err:#}").contains("block 300 log 2"), "{err:#}");

    let mut pending = settled(300, 2, 1_000, 4_096);
    pending.block_number = None;
    let err = rows_from_logs(&[pending], &[], shares(9000, 0, 1000)).expect_err("no position");
    assert!(
        format!("{err:#}").contains("without a block number"),
        "{err:#}"
    );

    let mut garbage = shares_updated(100, 0, shares(6000, 3000, 1000));
    garbage.inner.data = alloy::primitives::LogData::new_unchecked(
        garbage.inner.data.topics().to_vec(),
        alloy::primitives::Bytes::from(vec![0u8; 3]),
    );
    assert!(shares_history(&[garbage]).is_err());
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
    assert_eq!(coords.chain_id, DEFAULT_CHAIN_ID);
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

#[test]
fn the_config_file_fills_what_flags_leave_unset_and_flags_still_win() {
    let file: chain_ctx::FileConfig = toml::from_str(
        r#"
[blockchain]
rpc_url = "http://file.example"
chain_id = 421614
payment_pool_address = "0x0000000000000000000000000000000000000042"
get_logs_max_block_span = 100
"#,
    )
    .expect("parses");
    let coords = resolve_coordinates(&args(None, None, None), &file).expect("resolves");
    assert_eq!(coords.rpc_url, "http://file.example");
    assert_eq!(coords.payment_pool, Address::with_last_byte(0x42));
    assert_eq!(coords.span.get(), 100);
    assert_eq!(
        coords.from_block,
        known_chains::by_chain_id(421_614)
            .expect("known")
            .l2_start_block,
        "the deployment block still comes from the known chain"
    );
    let coords = resolve_coordinates(&args(Some("http://flag.example"), None, Some(5)), &file)
        .expect("resolves");
    assert_eq!(coords.rpc_url, "http://flag.example");
    assert_eq!(coords.from_block, 5);
}

#[test]
fn an_unknown_chain_needs_every_coordinate_spelled_out() {
    let file: chain_ctx::FileConfig =
        toml::from_str("[blockchain]\nchain_id = 31337\n").expect("parses");
    let err = resolve_coordinates(&args(None, None, None), &file).expect_err("no rpc");
    assert!(err.to_string().contains("--rpc-url"), "{err}");
    let err = resolve_coordinates(&args(Some("http://x"), None, None), &file).expect_err("no pool");
    assert!(err.to_string().contains("--payment-pool-address"), "{err}");
    let pool = Address::repeat_byte(0x42).to_string();
    let err = resolve_coordinates(&args(Some("http://x"), Some(&pool), None), &file)
        .expect_err("no from block");
    assert!(err.to_string().contains("--from-block"), "{err}");
    let coords = resolve_coordinates(&args(Some("http://x"), Some(&pool), Some(1)), &file)
        .expect("all spelled out");
    assert_eq!(coords.chain_id, 31_337);
}

mod scan {
    use alloy::sol_types::SolValue;

    use super::*;

    /// ABI-encode a `getShares()` answer.
    fn shares_answer(split: [U256; 3]) -> alloy::primitives::Bytes {
        alloy::primitives::Bytes::from(split.abi_encode())
    }

    /// Drive `scan_rows` over one window: the `Settled` scan, the
    /// `SharesUpdated` scan, `getShares()`, and, when the first settlement
    /// precedes the first change, the block-tagged `getShares()`.
    async fn scan(
        settled_logs: Vec<Log>,
        shares_logs: Vec<Log>,
        current: [U256; 3],
        before: Option<[U256; 3]>,
    ) -> anyhow::Result<Vec<SettlementRecord>> {
        let asserter = Asserter::new();
        asserter.push_success(&settled_logs);
        asserter.push_success(&shares_logs);
        asserter.push_success(&shares_answer(current));
        if let Some(before) = before {
            asserter.push_success(&shares_answer(before));
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        scan_rows(
            &provider,
            ROUTER,
            OPERATOR,
            0,
            999,
            NonZeroU64::new(10_000).expect("non-zero"),
        )
        .await
    }

    #[tokio::test]
    async fn a_settlement_before_the_first_change_uses_the_split_read_at_the_block_before() {
        let rows = scan(
            vec![settled(50, 0, 1_000, 10), settled(200, 0, 1_000, 10)],
            vec![shares_updated(100, 0, shares(9000, 0, 1000))],
            shares(9000, 0, 1000),
            Some(shares(6000, 3000, 1000)),
        )
        .await
        .expect("scan");
        assert_eq!(
            rows.iter().map(|r| r.net_micro_usdc).collect::<Vec<_>>(),
            vec![600, 900],
            "the block-50 row is priced under the split in force before block 100"
        );
    }

    #[tokio::test]
    async fn a_settlement_earlier_in_the_change_block_is_priced_under_the_old_split() {
        let rows = scan(
            vec![settled(100, 2, 1_000, 10), settled(100, 7, 1_000, 10)],
            vec![shares_updated(100, 5, shares(9000, 0, 1000))],
            shares(9000, 0, 1000),
            Some(shares(6000, 3000, 1000)),
        )
        .await
        .expect("scan");
        assert_eq!(
            rows.iter().map(|r| r.net_micro_usdc).collect::<Vec<_>>(),
            vec![600, 900],
            "log order inside the change block decides"
        );
    }

    #[tokio::test]
    async fn a_range_with_no_change_prices_every_row_under_the_current_split() {
        // Nothing queued for a block-tagged read: a request for one fails.
        let rows = scan(
            vec![settled(50, 0, 1_000, 10)],
            vec![],
            shares(9000, 0, 1000),
            None,
        )
        .await
        .expect("scan");
        assert_eq!(rows.first().map(|r| r.net_micro_usdc), Some(900));
    }
}
