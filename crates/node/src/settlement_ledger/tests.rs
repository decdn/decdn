use alloy::primitives::{Address, B256, Bloom, Log as PrimitiveLog, U256};
use alloy::rpc::types::Log;

use super::*;

const ROUTER: Address = Address::repeat_byte(0xF0);
const USDC: Address = Address::repeat_byte(0xA0);
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

#[test]
fn decodes_gross_bytes_epoch_and_the_operator_transfer_as_net() {
    let logs = vec![
        transfer(ROUTER, Address::repeat_byte(0xBB), U256::from(300), 1),
        transfer(ROUTER, OPERATOR, U256::from(600), 2),
        transfer(ROUTER, Address::repeat_byte(0xCC), U256::from(100), 3),
        settled(OPERATOR, 4_096, U256::from(1_000), 4),
    ];
    let row = settlement_from_receipt(&receipt(Some(900), logs), OPERATOR, 1_700_000_000)
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
fn no_settled_log_is_no_row() {
    let logs = vec![transfer(ROUTER, OPERATOR, U256::from(600), 0)];
    assert!(settlement_from_receipt(&receipt(Some(1), logs), OPERATOR, 0).is_none());
}

#[test]
fn another_operators_settled_is_ignored() {
    let other = Address::repeat_byte(0x99);
    let logs = vec![
        transfer(ROUTER, other, U256::from(600), 0),
        settled(other, 10, U256::from(1_000), 1),
    ];
    assert!(settlement_from_receipt(&receipt(Some(1), logs), OPERATOR, 0).is_none());
}

#[test]
fn no_operator_transfer_is_net_zero() {
    // Operator share 0: the router paid only the other buckets.
    let logs = vec![
        transfer(ROUTER, Address::repeat_byte(0xBB), U256::from(1_000), 0),
        settled(OPERATOR, 10, U256::from(1_000), 1),
    ];
    let row = settlement_from_receipt(&receipt(Some(1), logs), OPERATOR, 0).expect("row");
    assert_eq!(row.net_micro_usdc, 0);
    assert_eq!(row.gross_micro_usdc, 1_000);
}

#[test]
fn a_transfer_to_the_operator_from_elsewhere_does_not_count() {
    // Only the router's transfer is the payout; a buyer refund in the same
    // transaction (hypothetically) is not earnings.
    let logs = vec![
        transfer(Address::repeat_byte(0x77), OPERATOR, U256::from(5_000), 0),
        transfer(ROUTER, OPERATOR, U256::from(600), 1),
        settled(OPERATOR, 10, U256::from(1_000), 2),
    ];
    let row = settlement_from_receipt(&receipt(Some(1), logs), OPERATOR, 0).expect("row");
    assert_eq!(row.net_micro_usdc, 600);
}

#[test]
fn amounts_above_u64_saturate() {
    let huge = U256::from(u64::MAX) + U256::from(1);
    let logs = vec![
        transfer(ROUTER, OPERATOR, huge, 0),
        settled(OPERATOR, 10, huge, 1),
    ];
    let row = settlement_from_receipt(&receipt(Some(1), logs), OPERATOR, 0).expect("row");
    assert_eq!(row.net_micro_usdc, u64::MAX);
    assert_eq!(row.gross_micro_usdc, u64::MAX);
}

#[test]
fn block_number_falls_back_to_the_log_when_the_receipt_has_none() {
    let logs = vec![settled(OPERATOR, 10, U256::from(1), 0)];
    let row = settlement_from_receipt(&receipt(None, logs), OPERATOR, 0).expect("row");
    assert_eq!(row.block_number, 777);
}

mod recorder {
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use decdn_incentive::{MemorySettlementLedger, SettlementLedger};

    use super::*;

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

    #[tokio::test]
    async fn record_landed_stamps_the_row_with_the_block_time() {
        let asserter = Asserter::new();
        asserter.push_success(&block(1_700_000_123));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let ledger = Arc::new(MemorySettlementLedger::default());
        let recorder = SettlementRecorder::new(ledger.clone(), OPERATOR);
        recorder.record_landed(&provider, &landed_receipt()).await;
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
    async fn a_failed_header_read_falls_back_to_the_wall_clock() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("internal error");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let ledger = Arc::new(MemorySettlementLedger::default());
        let recorder = SettlementRecorder::new(ledger.clone(), OPERATOR);
        let before = unix_now();
        recorder.record_landed(&provider, &landed_receipt()).await;
        let rows = ledger.load_settlements().expect("ledger");
        assert!(rows.first().is_some_and(|row| row.landed_at_unix >= before));
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
        let recorder = SettlementRecorder::new(ledger.clone(), OPERATOR);
        let tx = landed_receipt().transaction_hash;
        recorder.backfill(&provider, tx).await;
        assert_eq!(ledger.load_settlements().expect("ledger").len(), 1);
        recorder.backfill(&provider, tx).await;
        assert_eq!(ledger.load_settlements().expect("ledger").len(), 1);
    }

    #[tokio::test]
    async fn backfill_of_a_receipt_the_rpc_lacks_writes_nothing() {
        let asserter = Asserter::new();
        asserter.push_success(&Option::<TransactionReceipt>::None);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let ledger = Arc::new(MemorySettlementLedger::default());
        let recorder = SettlementRecorder::new(ledger.clone(), OPERATOR);
        recorder.backfill(&provider, B256::repeat_byte(0x55)).await;
        assert!(ledger.load_settlements().expect("ledger").is_empty());
    }
}
