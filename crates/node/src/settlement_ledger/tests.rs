use alloy::primitives::{Address, B256, Bloom, Log as PrimitiveLog, U256};
use alloy::rpc::types::Log;

use super::*;

const ROUTER: Address = Address::repeat_byte(0xF0);
const USDC: Address = Address::repeat_byte(0xA0);
const POOL: Address = Address::repeat_byte(0xB0);
const OPERATOR: Address = Address::repeat_byte(0x0E);

/// A confirmed receipt at `block` carrying `logs`. `TransactionReceipt` has
/// no constructor, so every field the decoder does not read is filler.
fn receipt(block: Option<u64>, logs: Vec<Log>) -> TransactionReceipt {
    TransactionReceipt {
        inner: alloy::consensus::ReceiptEnvelope::Eip1559(alloy::consensus::ReceiptWithBloom {
            receipt: alloy::consensus::Receipt {
                status: alloy::consensus::Eip658Value::Eip658(true),
                cumulative_gas_used: 0,
                logs,
            },
            logs_bloom: Bloom::ZERO,
        }),
        transaction_hash: B256::repeat_byte(0x55),
        transaction_index: Some(0),
        block_hash: None,
        block_number: block,
        gas_used: 0,
        effective_gas_price: 0,
        blob_gas_used: None,
        blob_gas_price: None,
        from: OPERATOR,
        to: None,
        contract_address: None,
    }
}

fn log(address: Address, data: alloy::primitives::LogData, log_index: u64) -> Log {
    Log {
        inner: PrimitiveLog { address, data },
        block_hash: None,
        block_number: Some(777),
        block_timestamp: None,
        transaction_hash: Some(B256::repeat_byte(0x55)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}

fn settled(operator: Address, bytes: u64, amount: U256, log_index: u64) -> Log {
    let event = FeeRouter::Settled {
        operator,
        bytesDelivered: U256::from(bytes),
        amount,
        epoch: 42,
    };
    log(ROUTER, event.encode_log_data(), log_index)
}

fn transfer(from: Address, to: Address, value: U256, log_index: u64) -> Log {
    let event = Erc20::Transfer { from, to, value };
    log(USDC, event.encode_log_data(), log_index)
}

fn pool_redeemed(provider: Address, log_index: u64) -> Log {
    let event = PaymentPool::PoolRedeemed {
        poolId: B256::repeat_byte(0x11),
        provider,
        lanes: vec![],
    };
    log(POOL, event.encode_log_data(), log_index)
}

/// Replace a log's data bytes with garbage, keeping its topics.
fn corrupt(mut log: Log) -> Log {
    log.inner.data = alloy::primitives::LogData::new_unchecked(
        log.inner.data.topics().to_vec(),
        alloy::primitives::Bytes::from(vec![0u8; 3]),
    );
    log
}

fn decode(block: u64, logs: Vec<Log>) -> Result<Option<SettlementRecord>, DecodeError> {
    settlement_from_receipt(&receipt(Some(block), logs), OPERATOR, block, 1_700_000_000)
}

#[test]
fn decodes_gross_bytes_epoch_and_the_operator_transfer_as_net() {
    let logs = vec![
        pool_redeemed(OPERATOR, 0),
        transfer(ROUTER, Address::repeat_byte(0xBB), U256::from(300), 1),
        transfer(ROUTER, OPERATOR, U256::from(600), 2),
        transfer(ROUTER, Address::repeat_byte(0xCC), U256::from(100), 3),
        settled(OPERATOR, 4_096, U256::from(1_000), 4),
    ];
    let row = decode(900, logs)
        .expect("decodes")
        .expect("a Settled for the operator");
    assert_eq!(row.block_number, 900);
    assert_eq!(row.log_index, 4);
    assert_eq!(row.tx_hash, B256::repeat_byte(0x55));
    assert_eq!(row.landed_at_unix, 1_700_000_000);
    assert_eq!(row.epoch, 42);
    assert_eq!(row.gross_micro_usdc, 1_000);
    assert_eq!(row.net_micro_usdc, 600);
    assert_eq!(row.bytes_delivered, 4_096);
}

#[test]
fn a_receipt_that_routed_nothing_is_no_row_and_no_error() {
    // A re-sent batch whose vouchers were all at their watermark: the pool
    // emits neither PoolRedeemed nor, through the router, Settled.
    assert!(matches!(decode(1, vec![]), Ok(None)));
    // Unrelated transfers do not make it a routed receipt.
    let logs = vec![transfer(ROUTER, OPERATOR, U256::from(600), 0)];
    assert!(matches!(decode(1, logs), Ok(None)));
}

#[test]
fn a_routed_receipt_without_our_settled_is_an_error() {
    let other = Address::repeat_byte(0x99);
    let logs = vec![
        pool_redeemed(other, 0),
        transfer(ROUTER, other, U256::from(600), 1),
        settled(other, 10, U256::from(1_000), 2),
    ];
    assert!(matches!(
        decode(1, logs),
        Err(DecodeError::NoSettledForOperator { operator }) if operator == OPERATOR
    ));
    // A PoolRedeemed for us with no Settled at all is the same fault.
    let logs = vec![pool_redeemed(OPERATOR, 0)];
    assert!(matches!(
        decode(1, logs),
        Err(DecodeError::NoSettledForOperator { .. })
    ));
}

#[test]
fn an_undecodable_settled_or_transfer_is_an_error_not_a_zero() {
    let logs = vec![corrupt(settled(OPERATOR, 10, U256::from(1_000), 1))];
    assert!(matches!(
        decode(1, logs),
        Err(DecodeError::UndecodableSettled(_))
    ));
    let logs = vec![
        corrupt(transfer(ROUTER, OPERATOR, U256::from(600), 0)),
        settled(OPERATOR, 10, U256::from(1_000), 1),
    ];
    assert!(matches!(
        decode(1, logs),
        Err(DecodeError::UndecodableTransfer(_))
    ));
}

#[test]
fn a_settled_without_a_log_index_is_an_error() {
    let mut no_index = settled(OPERATOR, 10, U256::from(1_000), 1);
    no_index.log_index = None;
    assert!(matches!(
        decode(1, vec![no_index]),
        Err(DecodeError::MissingLogIndex)
    ));
}

#[test]
fn no_operator_transfer_is_net_zero() {
    // Operator share 0: the router paid only the other buckets.
    let logs = vec![
        transfer(ROUTER, Address::repeat_byte(0xBB), U256::from(1_000), 0),
        settled(OPERATOR, 10, U256::from(1_000), 1),
    ];
    let row = decode(1, logs).expect("decodes").expect("row");
    assert_eq!(row.net_micro_usdc, 0);
    assert_eq!(row.gross_micro_usdc, 1_000);
}

#[test]
fn a_transfer_to_the_operator_from_elsewhere_does_not_count() {
    let logs = vec![
        transfer(Address::repeat_byte(0x77), OPERATOR, U256::from(5_000), 0),
        transfer(ROUTER, OPERATOR, U256::from(600), 1),
        settled(OPERATOR, 10, U256::from(1_000), 2),
    ];
    let row = decode(1, logs).expect("decodes").expect("row");
    assert_eq!(row.net_micro_usdc, 600);
}

#[test]
fn amounts_above_u64_saturate() {
    let huge = U256::from(u64::MAX) + U256::from(1);
    let logs = vec![
        transfer(ROUTER, OPERATOR, huge, 0),
        settled(OPERATOR, 10, huge, 1),
    ];
    let row = decode(1, logs).expect("decodes").expect("row");
    assert_eq!(row.net_micro_usdc, u64::MAX);
    assert_eq!(row.gross_micro_usdc, u64::MAX);
}

mod recorder {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use decdn_incentive::{MemorySettlementLedger, SettlementLedger, StoreError};

    use super::*;

    /// The current value of counter `name` in the `OpenMetrics` text.
    fn counter(metrics: &Metrics, name: &str) -> u64 {
        metrics
            .encode()
            .unwrap_or_default()
            .lines()
            .find(|line| line.starts_with(&format!("{name}_total ")))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    /// A block at `timestamp`, as `eth_getBlockByNumber` returns it.
    fn block(timestamp: u64) -> alloy::rpc::types::Block {
        let mut block: alloy::rpc::types::Block = alloy::rpc::types::Block::default();
        block.header.inner.timestamp = timestamp;
        block
    }

    fn landed_receipt() -> TransactionReceipt {
        receipt(
            Some(900),
            vec![
                transfer(ROUTER, OPERATOR, U256::from(600), 2),
                settled(OPERATOR, 4_096, U256::from(1_000), 4),
            ],
        )
    }

    /// Set every log's `block_timestamp`, as an RPC that fills it in would.
    fn stamp_logs(receipt: &mut TransactionReceipt, timestamp: u64) {
        if let alloy::consensus::ReceiptEnvelope::Eip1559(inner) = &mut receipt.inner {
            for log in &mut inner.receipt.logs {
                log.block_timestamp = Some(timestamp);
            }
        }
    }

    fn recorder(ledger: Arc<dyn SettlementLedger>) -> SettlementRecorder {
        SettlementRecorder::new(ledger, OPERATOR, Arc::new(Metrics::new()))
    }

    #[tokio::test]
    async fn record_landed_stamps_the_row_with_the_block_time() {
        let asserter = Asserter::new();
        asserter.push_success(&block(1_700_000_123));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let ledger = Arc::new(MemorySettlementLedger::default());
        recorder(ledger.clone())
            .record_landed(&provider, &landed_receipt())
            .await;
        let rows = ledger.load_settlements().expect("ledger");
        assert_eq!(rows.len(), 1);
        let row = rows.first().expect("one row");
        assert_eq!(row.landed_at_unix, 1_700_000_123);
        assert_eq!(
            (row.block_number, row.log_index, row.net_micro_usdc),
            (900, 4, 600)
        );
    }

    #[tokio::test]
    async fn a_log_timestamp_saves_the_header_read() {
        // Nothing queued: any RPC request would fail loudly.
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let mut receipt = landed_receipt();
        stamp_logs(&mut receipt, 1_700_000_999);
        let ledger = Arc::new(MemorySettlementLedger::default());
        recorder(ledger.clone())
            .record_landed(&provider, &receipt)
            .await;
        let rows = ledger.load_settlements().expect("ledger");
        assert_eq!(rows.first().map(|r| r.landed_at_unix), Some(1_700_000_999));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_header_read_falls_back_to_the_wall_clock_on_the_redeemer_path() {
        let asserter = Asserter::new();
        for _ in 0..=HEADER_READ_RETRIES {
            asserter.push_failure_msg("internal error");
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let ledger = Arc::new(MemorySettlementLedger::default());
        let before = unix_now();
        recorder(ledger.clone())
            .record_landed(&provider, &landed_receipt())
            .await;
        let rows = ledger.load_settlements().expect("ledger");
        assert!(rows.first().is_some_and(|row| row.landed_at_unix >= before));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_header_read_skips_the_row_on_the_backfill_path() {
        let asserter = Asserter::new();
        asserter.push_success(&landed_receipt());
        for _ in 0..=HEADER_READ_RETRIES {
            asserter.push_failure_msg("internal error");
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let ledger = Arc::new(MemorySettlementLedger::default());
        let rec = recorder(ledger.clone());
        rec.backfill(&provider, 900, landed_receipt().transaction_hash)
            .await;
        assert!(
            ledger.load_settlements().expect("ledger").is_empty(),
            "an old settlement must not be stamped with today's clock"
        );
        assert_eq!(
            counter(&rec.metrics, "decdn_settlement_ledger_skipped_rows"),
            1
        );
    }

    #[tokio::test]
    async fn a_zero_pay_landing_writes_nothing_and_counts_nothing() {
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let ledger = Arc::new(MemorySettlementLedger::default());
        let rec = recorder(ledger.clone());
        // No logs at all, as a re-sent batch at its watermark produces, and
        // nothing queued on the provider: a header read would fail loudly, and
        // on a real RPC it could stall the redeemer for the call timeout.
        let quiet = receipt(Some(900), vec![]);
        rec.record_landed(&provider, &quiet).await;
        assert!(ledger.load_settlements().expect("ledger").is_empty());
        assert_eq!(
            counter(&rec.metrics, "decdn_settlement_ledger_skipped_rows"),
            0
        );
    }

    #[tokio::test]
    async fn backfill_fetches_the_receipt_once_and_skips_a_known_tx() {
        let asserter = Asserter::new();
        // One receipt read, one header read, then nothing: the second
        // backfill of the same tx must make no request.
        asserter.push_success(&landed_receipt());
        asserter.push_success(&block(1_700_000_500));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let ledger = Arc::new(MemorySettlementLedger::default());
        let rec = recorder(ledger.clone());
        let tx = landed_receipt().transaction_hash;
        rec.backfill(&provider, 900, tx).await;
        assert_eq!(ledger.load_settlements().expect("ledger").len(), 1);
        rec.backfill(&provider, 900, tx).await;
        assert_eq!(ledger.load_settlements().expect("ledger").len(), 1);
        assert_eq!(
            counter(&rec.metrics, "decdn_settlement_ledger_skipped_rows"),
            0,
            "a re-fetch against the empty queue would have been counted as a skip"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn backfill_of_a_receipt_the_rpc_lacks_writes_nothing() {
        let asserter = Asserter::new();
        for _ in 0..=HEADER_READ_RETRIES {
            asserter.push_success(&Option::<TransactionReceipt>::None);
        }
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let ledger = Arc::new(MemorySettlementLedger::default());
        let rec = recorder(ledger.clone());
        rec.backfill(&provider, 900, B256::repeat_byte(0x55)).await;
        assert!(ledger.load_settlements().expect("ledger").is_empty());
        assert_eq!(
            counter(&rec.metrics, "decdn_settlement_ledger_skipped_rows"),
            1
        );
    }

    struct FailingLedger;

    impl SettlementLedger for FailingLedger {
        fn record_settlement(&self, _: &SettlementRecord) -> Result<(), StoreError> {
            Err(StoreError::Backend("disk full".into()))
        }

        fn load_settlements(&self) -> Result<Vec<SettlementRecord>, StoreError> {
            Ok(Vec::new())
        }

        fn has_settlement_tx(&self, _: u64, _: B256) -> Result<bool, StoreError> {
            Err(StoreError::Backend("locked".into()))
        }
    }

    #[tokio::test]
    async fn a_failing_ledger_write_is_counted() {
        let asserter = Asserter::new();
        asserter.push_success(&block(1_700_000_123));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let rec = recorder(Arc::new(FailingLedger));
        rec.record_landed(&provider, &landed_receipt()).await;
        assert_eq!(
            counter(&rec.metrics, "decdn_settlement_ledger_write_failures"),
            1
        );
    }

    #[tokio::test]
    async fn a_failing_guard_read_makes_no_rpc_and_is_counted() {
        // Nothing queued: a receipt fetch would fail the test loudly.
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let rec = recorder(Arc::new(FailingLedger));
        rec.backfill(&provider, 900, B256::repeat_byte(0x55)).await;
        assert_eq!(
            counter(&rec.metrics, "decdn_settlement_ledger_skipped_rows"),
            1
        );
    }
}
