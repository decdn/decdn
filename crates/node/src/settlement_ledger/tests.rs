use alloy::primitives::{Address, B256, Bloom, Log as PrimitiveLog, U256};
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use decdn_incentive::Deployment;
use decdn_incentive::erc20::Erc20;
use decdn_incentive::payment_pool::PaymentPool;

use super::*;

const OPERATOR: Address = Address::repeat_byte(0x0E);
const USDC: Address = Address::repeat_byte(0xA0);
const POOL: Address = Address::repeat_byte(0xB0);
const DEPLOYMENT: Deployment = Deployment {
    chain_id: 31_337,
    payment_pool: POOL,
};
/// A 60% operator share, the reference deployment's.
const SOURCE: LedgerSource = LedgerSource {
    deployment: DEPLOYMENT,
    usdc: USDC,
    operator_bps: 6_000,
};

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
        block_number: Some(900),
        block_timestamp: None,
        transaction_hash: Some(B256::repeat_byte(0x55)),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}

fn transfer(token: Address, from: Address, to: Address, value: u64, log_index: u64) -> Log {
    let event = Erc20::Transfer {
        from,
        to,
        value: U256::from(value),
    };
    log(token, event.encode_log_data(), log_index)
}

/// A `PoolRedeemed` from `pool` for `provider`, one lane per `bytes` entry.
fn pool_redeemed(pool: Address, provider: Address, bytes: &[u64], log_index: u64) -> Log {
    let event = PaymentPool::PoolRedeemed {
        poolId: B256::repeat_byte(0x11),
        provider,
        lanes: bytes
            .iter()
            .map(|&bytes_paid| PaymentPool::LaneSettled {
                signer: Address::repeat_byte(0x5A),
                newPaidCumulative: 1_000,
                bytesPaid: bytes_paid,
            })
            .collect(),
    };
    log(pool, event.encode_log_data(), log_index)
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

    fn recorder(ledger: Arc<dyn SettlementLedger>) -> SettlementRecorder {
        SettlementRecorder::new(ledger, OPERATOR, SOURCE, Arc::new(Metrics::new()))
    }

    /// A landed redemption writes its row, stamped with the block time the
    /// receipt's logs carry, so no header read is made.
    #[tokio::test]
    async fn a_landing_writes_its_row() {
        // Nothing queued: a header read would fail loudly.
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let ledger = Arc::new(MemorySettlementLedger::default());
        let rec = recorder(ledger.clone());
        let mut paid = transfer(USDC, POOL, OPERATOR, 600, 2);
        paid.block_timestamp = Some(1_700_000_123);
        let logs = vec![pool_redeemed(POOL, OPERATOR, &[4_096], 0), paid];
        rec.record_landed(&provider, &receipt(Some(900), logs))
            .await;
        let rows = ledger.load_settlements().expect("ledger");
        assert_eq!(rows.len(), 1);
        let row = rows.first().expect("one row");
        assert_eq!(row.key(), (DEPLOYMENT, 900, 2));
        assert_eq!(row.landed_at_unix, 1_700_000_123);
        assert_eq!(row.net_micro_usdc, 600);
        assert_eq!(row.gross_micro_usdc, 1_000);
        assert_eq!(row.bytes_delivered, 4_096);
    }

    #[tokio::test]
    async fn a_landing_that_paid_nothing_writes_nothing_and_counts_nothing() {
        // Nothing queued: a header read would fail loudly.
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let ledger = Arc::new(MemorySettlementLedger::default());
        let rec = recorder(ledger.clone());
        rec.record_landed(&provider, &receipt(Some(900), vec![]))
            .await;
        assert!(ledger.load_settlements().expect("ledger").is_empty());
        assert_eq!(
            counter(&rec.metrics, "decdn_settlement_ledger_skipped_rows"),
            0
        );
    }

    /// A sent hash whose receipt a crash or a timed-out wait missed: the
    /// recovery fetches the receipt, writes its row and drops the hash.
    #[tokio::test]
    async fn a_missed_receipt_is_recovered_by_its_sent_hash() {
        let tx = B256::repeat_byte(0x55);
        let mut paid = transfer(USDC, POOL, OPERATOR, 600, 2);
        paid.block_timestamp = Some(1_700_000_123);
        let mined = receipt(
            Some(900),
            vec![pool_redeemed(POOL, OPERATOR, &[4_096], 0), paid],
        );
        let asserter = Asserter::new();
        asserter.push_success(&Some(mined));
        let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
        let ledger = Arc::new(MemorySettlementLedger::default());
        let rec = recorder(ledger.clone());
        rec.record_sent(tx).await;
        rec.recover_sent(&provider).await;
        let rows = ledger.load_settlements().expect("ledger");
        assert_eq!(rows.len(), 1);
        assert!(ledger.sent_redeems(DEPLOYMENT).expect("sent").is_empty());
        assert!(asserter.read_q().is_empty());
    }

    /// A sent hash the chain does not know (a dropped or replaced
    /// transaction) is dropped with no row.
    #[tokio::test]
    async fn a_dropped_send_is_forgotten() {
        let asserter = Asserter::new();
        asserter.push_success(&Option::<TransactionReceipt>::None);
        asserter.push_success(&Option::<alloy::rpc::types::Transaction>::None);
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let ledger = Arc::new(MemorySettlementLedger::default());
        let rec = recorder(ledger.clone());
        rec.record_sent(B256::repeat_byte(0x66)).await;
        rec.recover_sent(&provider).await;
        assert!(ledger.load_settlements().expect("ledger").is_empty());
        assert!(ledger.sent_redeems(DEPLOYMENT).expect("sent").is_empty());
    }

    /// A failed receipt read keeps the hash for the next attempt.
    #[tokio::test]
    async fn a_failed_receipt_read_keeps_the_hash() {
        let asserter = Asserter::new();
        asserter.push_failure_msg("upstream down");
        let provider = ProviderBuilder::new().connect_mocked_client(asserter);
        let ledger = Arc::new(MemorySettlementLedger::default());
        let rec = recorder(ledger.clone());
        rec.record_sent(B256::repeat_byte(0x77)).await;
        rec.recover_sent(&provider).await;
        assert_eq!(
            ledger.sent_redeems(DEPLOYMENT).expect("sent"),
            vec![B256::repeat_byte(0x77)]
        );
    }

    /// With nothing recorded, the recovery makes no RPC.
    #[tokio::test]
    async fn nothing_sent_reads_nothing() {
        // Nothing queued: a receipt read would fail the test loudly.
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let rec = recorder(Arc::new(MemorySettlementLedger::default()));
        rec.recover_sent(&provider).await;
    }

    struct FailingLedger;

    impl SettlementLedger for FailingLedger {
        fn record_settlement(&self, _: &SettlementRecord) -> Result<(), StoreError> {
            Err(StoreError::Backend("disk full".into()))
        }

        fn load_settlements(&self) -> Result<Vec<SettlementRecord>, StoreError> {
            Ok(Vec::new())
        }

        fn has_settlement_tx(&self, _: Deployment, _: u64, _: B256) -> Result<bool, StoreError> {
            Err(StoreError::Backend("locked".into()))
        }

        fn record_sent_redeem(&self, _: Deployment, _: B256) -> Result<(), StoreError> {
            Err(StoreError::Backend("disk full".into()))
        }

        fn sent_redeems(&self, _: Deployment) -> Result<Vec<B256>, StoreError> {
            Err(StoreError::Backend("locked".into()))
        }

        fn forget_sent_redeem(&self, _: Deployment, _: B256) -> Result<(), StoreError> {
            Err(StoreError::Backend("locked".into()))
        }
    }

    /// A sent hash the store refuses is counted as a ledger write failure, and
    /// an unreadable sent set makes no RPC.
    #[tokio::test]
    async fn a_failing_store_is_counted_and_reads_nothing() {
        // Nothing queued: a receipt fetch would fail the test loudly.
        let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
        let rec = recorder(Arc::new(FailingLedger));
        rec.record_sent(B256::repeat_byte(0x55)).await;
        rec.recover_sent(&provider).await;
        assert_eq!(
            counter(&rec.metrics, "decdn_settlement_ledger_write_failures"),
            1
        );
    }
}
