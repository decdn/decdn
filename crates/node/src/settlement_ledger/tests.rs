use alloy::primitives::{Address, B256, Bloom};
use alloy::rpc::types::Log;

use super::*;

const OPERATOR: Address = Address::repeat_byte(0x0E);
const DEPLOYMENT: Deployment = Deployment {
    chain_id: 31_337,
    payment_pool: Address::repeat_byte(0xB0),
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

/// This build decodes no row from any receipt.
#[test]
fn a_receipt_decodes_to_no_row() {
    let decoded = settlement_from_receipt(
        &receipt(Some(900), Vec::new()),
        OPERATOR,
        DEPLOYMENT,
        900,
        1_700_000_000,
    );
    assert!(matches!(decoded, Ok(None)));
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
        SettlementRecorder::new(ledger, OPERATOR, DEPLOYMENT, Arc::new(Metrics::new()))
    }

    #[tokio::test]
    async fn a_landing_writes_nothing_and_counts_nothing() {
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

        fn has_settlement_tx(&self, _: Deployment, _: u64, _: B256) -> Result<bool, StoreError> {
            Err(StoreError::Backend("locked".into()))
        }
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

    mod backfills {
        use decdn_incentive::test_rpc::ScriptedRpc;

        use super::*;

        /// A receipt read that never answers does not hold the caller: the
        /// spawn returns at once, and the backfill runs out its three
        /// attempts at the call timeout on its own task, where the miss is
        /// counted.
        #[tokio::test(start_paused = true)]
        async fn a_backfill_runs_off_the_caller_and_its_miss_is_counted() {
            let ledger = Arc::new(MemorySettlementLedger::default());
            let rec = recorder(ledger.clone());
            let backfills = LedgerBackfills::new(rec.clone());
            // Every unscripted read on this transport parks forever.
            let provider = ScriptedRpc::parked_receipt().provider();
            let started = tokio::time::Instant::now();
            backfills.spawn(provider, 900, B256::repeat_byte(0x55));
            assert_eq!(backfills.in_flight(), 1);
            // Three attempts at the call timeout plus the pauses between them;
            // the paused clock skips ahead whenever every task is parked, and
            // the drain returns the moment the task gives up.
            backfills.drain(Duration::from_secs(60)).await;
            assert_eq!(
                started.elapsed(),
                3 * crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT + 2 * HEADER_READ_RETRY_DELAY,
                "three receipt attempts at the call timeout, two pauses between them"
            );
            assert_eq!(
                counter(&rec.metrics, "decdn_settlement_ledger_skipped_rows"),
                1,
                "the backfill ran to its miss on its own task"
            );
            assert!(ledger.load_settlements().expect("ledger").is_empty());
        }

        /// A backfill still parked at the deadline is aborted, not waited on
        /// past it, and the set is empty afterwards.
        #[tokio::test(start_paused = true)]
        async fn drain_aborts_what_outlives_the_deadline() {
            let ledger = Arc::new(MemorySettlementLedger::default());
            let backfills = LedgerBackfills::new(recorder(ledger.clone()));
            backfills.spawn(
                ScriptedRpc::parked_receipt().provider(),
                900,
                B256::repeat_byte(0x55),
            );
            let started = tokio::time::Instant::now();
            backfills.drain(Duration::from_secs(1)).await;
            assert_eq!(started.elapsed(), Duration::from_secs(1));
            assert_eq!(backfills.in_flight(), 0);
            // A drain after the abort has nothing to wait for.
            backfills.drain(Duration::from_secs(1)).await;
            assert_eq!(started.elapsed(), Duration::from_secs(1));
        }

        /// A spawn after the drain has begun starts no task: the poller can
        /// still be applying a window while the service shuts down, and a
        /// task started then would end with the process. The miss is counted.
        #[tokio::test]
        async fn a_spawn_after_the_drain_is_refused_and_counted() {
            // Nothing queued: a receipt read would fail loudly.
            let provider = ProviderBuilder::new().connect_mocked_client(Asserter::new());
            let ledger = Arc::new(MemorySettlementLedger::default());
            let rec = recorder(ledger.clone());
            let backfills = LedgerBackfills::new(rec.clone());
            backfills.drain(Duration::from_secs(1)).await;
            backfills.spawn(provider, 900, B256::repeat_byte(0x55));
            assert_eq!(backfills.in_flight(), 0, "no task started");
            tokio::task::yield_now().await;
            assert_eq!(
                counter(&rec.metrics, "decdn_settlement_ledger_skipped_rows"),
                1
            );
        }
    }
}
