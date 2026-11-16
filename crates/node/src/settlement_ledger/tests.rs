use alloy::primitives::{Address, B256, Bloom, Log as PrimitiveLog, U256};
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
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

/// Replace a log's data bytes with garbage, keeping its topics.
fn corrupt(mut log: Log) -> Log {
    log.inner.data = alloy::primitives::LogData::new_unchecked(
        log.inner.data.topics().to_vec(),
        alloy::primitives::Bytes::from(vec![0u8; 3]),
    );
    log
}

fn decode(logs: Vec<Log>) -> Result<Option<SettlementRecord>, DecodeError> {
    settlement_from_receipt(
        &receipt(Some(900), logs),
        OPERATOR,
        SOURCE,
        900,
        1_700_000_000,
    )
}

/// The operator's income is the pool's USDC `Transfer` to it; gross scales
/// it by `10000 / operatorBps`; bytes sum the pool's `PoolRedeemed` lanes for
/// the operator, across every pool group in the batch.
#[test]
fn a_redeem_receipt_decodes_net_gross_and_bytes() {
    let logs = vec![
        pool_redeemed(POOL, OPERATOR, &[1_024, 2_048], 0),
        pool_redeemed(POOL, OPERATOR, &[1_024], 1),
        transfer(USDC, POOL, OPERATOR, 600, 2),
    ];
    let row = decode(logs).expect("decodes").expect("a row");
    assert_eq!(row.deployment, DEPLOYMENT);
    assert_eq!(row.block_number, 900);
    assert_eq!(
        row.log_index, 2,
        "the row is keyed on the operator transfer"
    );
    assert_eq!(row.tx_hash, B256::repeat_byte(0x55));
    assert_eq!(row.landed_at_unix, 1_700_000_000);
    assert_eq!(row.net_micro_usdc, 600);
    assert_eq!(row.gross_micro_usdc, 1_000);
    assert_eq!(row.bytes_delivered, 4_096);
    assert_eq!(row.operator, OPERATOR);
}

/// The contract floors the operator share, so the gross is the smallest
/// payout that floors to the net: rounded up, never over the true payout.
#[test]
fn the_gross_rounds_up_to_the_smallest_payout_with_that_share() {
    // A payout of 1_001 at 60% pays floor(600.6) = 600 to the operator; the
    // smallest payout paying 600 is 1_000.
    let logs = vec![
        pool_redeemed(POOL, OPERATOR, &[10], 0),
        transfer(USDC, POOL, OPERATOR, 601, 1),
    ];
    let row = decode(logs).expect("decodes").expect("a row");
    // 601 × 10000 / 6000 = 1001.67, rounded up.
    assert_eq!(row.gross_micro_usdc, 1_002);
}

/// Transfers to someone else, from someone else, or of another token are not
/// the operator's income from this pool.
#[test]
fn foreign_transfers_and_other_tokens_are_ignored() {
    let other = Address::repeat_byte(0x99);
    let logs = vec![
        pool_redeemed(POOL, OPERATOR, &[512], 0),
        transfer(USDC, POOL, other, 300, 1),
        transfer(USDC, other, OPERATOR, 300, 2),
        transfer(Address::repeat_byte(0xC0), POOL, OPERATOR, 900, 3),
        transfer(USDC, POOL, OPERATOR, 600, 4),
    ];
    let row = decode(logs).expect("decodes").expect("a row");
    assert_eq!(row.net_micro_usdc, 600);
    assert_eq!(row.log_index, 4);
}

/// Another provider's lanes and another pool's events add no bytes.
#[test]
fn foreign_pool_redeemed_lanes_are_ignored() {
    let logs = vec![
        pool_redeemed(POOL, Address::repeat_byte(0x99), &[9_999], 0),
        pool_redeemed(Address::repeat_byte(0xB1), OPERATOR, &[9_999], 1),
        pool_redeemed(POOL, OPERATOR, &[100], 2),
        transfer(USDC, POOL, OPERATOR, 60, 3),
    ];
    let row = decode(logs).expect("decodes").expect("a row");
    assert_eq!(row.bytes_delivered, 100);
}

/// No operator transfer is no row: the batch paid nothing, or its operator
/// share floored to zero.
#[test]
fn a_receipt_without_an_operator_transfer_is_no_row() {
    assert!(matches!(decode(vec![]), Ok(None)));
    let logs = vec![
        pool_redeemed(POOL, OPERATOR, &[100], 0),
        transfer(Address::repeat_byte(0xC0), POOL, OPERATOR, 60, 1),
    ];
    assert!(matches!(decode(logs), Ok(None)));
}

/// A pool with no operator share pays the operator nothing in the
/// transaction, so it yields no row and no division by zero.
#[test]
fn a_zero_operator_share_is_no_row() {
    let source = LedgerSource {
        operator_bps: 0,
        ..SOURCE
    };
    let decoded = settlement_from_receipt(
        &receipt(
            Some(900),
            vec![
                pool_redeemed(POOL, OPERATOR, &[100], 0),
                transfer(USDC, POOL, OPERATOR, 60, 1),
            ],
        ),
        OPERATOR,
        source,
        900,
        0,
    );
    assert!(matches!(decoded, Ok(None)));
}

/// An undecodable event from the pool or the token is an error, never a zero.
#[test]
fn an_undecodable_event_is_an_error() {
    let logs = vec![
        corrupt(pool_redeemed(POOL, OPERATOR, &[100], 0)),
        transfer(USDC, POOL, OPERATOR, 60, 1),
    ];
    assert!(matches!(
        decode(logs),
        Err(DecodeError::UndecodablePoolRedeemed(_))
    ));
    let logs = vec![
        pool_redeemed(POOL, OPERATOR, &[100], 0),
        corrupt(transfer(USDC, POOL, OPERATOR, 60, 1)),
    ];
    assert!(matches!(
        decode(logs),
        Err(DecodeError::UndecodableTransfer(_))
    ));
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
