use std::sync::Mutex;

use super::*;
use alloy::providers::ProviderBuilder;
use alloy::providers::mock::Asserter;
use alloy::sol_types::SolValue;

use crate::chain_events::boot_retry::BOOT_CHAIN_RETRY_BUDGET;

/// A transient provider error during the boot enumeration is retried on the
/// boot budget, not fatal.
#[tokio::test(start_paused = true)]
async fn a_transient_boot_enumeration_error_is_retried() {
    let asserter = Asserter::new();
    // The first read of an attempt is the head; it fails.
    asserter.push_failure(
        serde_json::from_value(serde_json::json!({
            "code": 19,
            "message": "Temporary internal error. Please retry",
        }))
        .unwrap(),
    );
    // The retry: the head, the code-presence `eth_getCode`, no slashes, then
    // the pause offset.
    asserter.push_success(&alloy::primitives::U64::from(1_000));
    asserter.push_success(&alloy::primitives::Bytes::from_static(&[0x60]));
    for _ in 0..2 {
        asserter.push_success(&alloy::primitives::Bytes::from(U256::ZERO.abi_encode()));
    }
    let metrics = Arc::new(Metrics::new());

    let (store, _task) = bootstrap(
        ProviderBuilder::new().connect_mocked_client(asserter.clone()),
        Address::repeat_byte(0x11),
        Address::repeat_byte(0x22),
        Arc::clone(&metrics),
        &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert!(store.read().unwrap().is_empty());
    assert!(asserter.read_q().is_empty(), "both attempts ran");
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_chain_boot_read_retries_total 1"),
        "{text}"
    );
}

/// A deterministic head-read failure fails boot at once rather than retrying.
#[tokio::test(start_paused = true)]
async fn a_permanent_head_error_fails_the_boot_enumeration_at_once() {
    let asserter = Asserter::new();
    asserter.push_failure(
        serde_json::from_value(serde_json::json!({
            "code": -32601,
            "message": "method not found",
        }))
        .unwrap(),
    );
    let metrics = Arc::new(Metrics::new());
    let start = tokio::time::Instant::now();

    let err = bootstrap(
        ProviderBuilder::new().connect_mocked_client(asserter),
        Address::repeat_byte(0x11),
        Address::repeat_byte(0x22),
        Arc::clone(&metrics),
        &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
        CancellationToken::new(),
    )
    .await
    .err()
    .unwrap();

    assert!(format!("{err:#}").contains("not retried"), "{err:#}");
    assert_eq!(start.elapsed(), Duration::ZERO);
}

/// A stalled provider cannot wedge boot: every read is bounded by the
/// per-call timeout.
#[tokio::test(start_paused = true)]
async fn a_stalled_provider_fails_the_boot_enumeration() {
    use crate::chain_events::test_support::{bounded, hanging_provider};

    let metrics = Arc::new(Metrics::new());
    let err = bounded(
        "slash boot enumeration",
        bootstrap(
            hanging_provider(),
            Address::repeat_byte(0x11),
            Address::repeat_byte(0x22),
            Arc::clone(&metrics),
            &BootRetry::single_attempt(Arc::clone(&metrics)),
            CancellationToken::new(),
        ),
    )
    .await
    .err()
    .map(|e| format!("{e:#}"))
    .unwrap();

    assert!(err.contains("eth_blockNumber timed out after"), "{err}");
}

struct StubSlashReads {
    operator: Address,
    /// (slashId, record) in append order (index 0 = oldest).
    slashes: Mutex<Vec<(U256, SlashRecordView)>>,
    /// Global `CapacityBond.pausedTotal` the enumeration adds to each base close.
    paused_total: u64,
    /// slashIds whose record was point-read, for the early-stop assertion.
    reads: Mutex<Vec<U256>>,
    /// The block every read ran at, for the pinned-read assertion.
    blocks: Mutex<Vec<BlockId>>,
}

impl StubSlashReads {
    fn new(operator: Address, slashes: Vec<(U256, SlashRecordView)>) -> Self {
        Self {
            operator,
            slashes: Mutex::new(slashes),
            paused_total: 0,
            reads: Mutex::new(Vec::new()),
            blocks: Mutex::new(Vec::new()),
        }
    }

    fn blocks_read(&self) -> Vec<BlockId> {
        self.blocks.lock().unwrap().clone()
    }

    fn with_paused_total(mut self, paused_total: u64) -> Self {
        self.paused_total = paused_total;
        self
    }

    fn records_read(&self) -> Vec<U256> {
        self.reads.lock().unwrap().clone()
    }
}

impl SlashChainReads for StubSlashReads {
    async fn snapshot_block(&self) -> Result<u64> {
        Ok(0)
    }

    async fn operator_slash_count(&self, operator: Address, at: BlockId) -> Result<U256> {
        self.blocks.lock().unwrap().push(at);
        assert_eq!(operator, self.operator, "unexpected operator");
        Ok(U256::from(self.slashes.lock().unwrap().len()))
    }

    async fn operator_slash_id_at(
        &self,
        operator: Address,
        index: U256,
        at: BlockId,
    ) -> Result<U256> {
        self.blocks.lock().unwrap().push(at);
        assert_eq!(operator, self.operator, "unexpected operator");
        let i: usize = index.to();
        self.slashes
            .lock()
            .unwrap()
            .get(i)
            .map(|(id, _)| *id)
            .ok_or_else(|| anyhow::anyhow!("SlashIndexOutOfRange"))
    }

    async fn get_slash_record(&self, slash_id: U256, at: BlockId) -> Result<SlashRecordView> {
        self.blocks.lock().unwrap().push(at);
        self.reads.lock().unwrap().push(slash_id);
        self.slashes
            .lock()
            .unwrap()
            .iter()
            .find(|(id, _)| *id == slash_id)
            .map(|(_, rec)| rec.clone())
            .ok_or_else(|| anyhow::anyhow!("no such slash"))
    }

    async fn paused_total(&self, at: BlockId) -> Result<u64> {
        self.blocks.lock().unwrap().push(at);
        Ok(self.paused_total)
    }
}

fn record(
    offense_type: u8,
    amount: u64,
    evidence: u8,
    appeal_window_close: u64,
) -> SlashRecordView {
    SlashRecordView {
        offense_type,
        amount: U256::from(amount),
        evidence_hash: B256::repeat_byte(evidence),
        appeal_window_close,
    }
}

/// Every enumeration read runs at the block it is given: the count, the
/// pause offset, each index and each record.
#[tokio::test]
async fn every_enumeration_read_runs_at_the_given_block() {
    let op = Address::repeat_byte(0xAB);
    let now = 1_000;
    let reads = StubSlashReads::new(
        op,
        vec![
            (U256::from(20u64), record(1, 200, 0x22, now + 50)),
            (U256::from(30u64), record(0, 300, 0x33, now + 99)),
        ],
    );

    bootstrap_slashes(&reads, op, now, BlockId::number(100))
        .await
        .unwrap();

    let blocks = reads.blocks_read();
    // Count, pause offset, then an index and a record per slash.
    assert_eq!(blocks.len(), 6);
    assert!(
        blocks.iter().all(|b| *b == BlockId::number(100)),
        "{blocks:?}"
    );
}

/// Enumeration surfaces the still-appealable slashes newest-first, carrying
/// the authoritative record fields, and excludes those whose window closed.
#[tokio::test]
async fn bootstrap_slashes_surfaces_only_still_appealable_slashes_newest_first() {
    let op = Address::repeat_byte(0xAB);
    let now = 1_000;
    let reads = StubSlashReads::new(
        op,
        vec![
            (U256::from(10u64), record(1, 100, 0x11, now - 1)),
            (U256::from(20u64), record(1, 200, 0x22, now + 50)),
            (U256::from(30u64), record(0, 300, 0x33, now + 99)),
        ],
    );

    let (count, slashes) = bootstrap_slashes(&reads, op, now, BlockId::latest())
        .await
        .unwrap();

    assert_eq!(count, U256::from(3u64));
    assert_eq!(slashes.len(), 2, "closed slash 10 must be excluded");
    assert_eq!(slashes[0].slash_id, U256::from(30u64));
    assert_eq!(slashes[1].slash_id, U256::from(20u64));
    assert_eq!(slashes[0].appeal_window_close, Some(now + 99));
    assert_eq!(slashes[0].offense_type, 0);
    assert_eq!(slashes[1].evidence_hash, B256::repeat_byte(0x22));
}

/// The backward walk stops at the first closed record and never reads the
/// older ones (they are closed too — appended in `slashedAt` order).
#[tokio::test]
async fn bootstrap_slashes_stops_at_the_first_closed_record() {
    let op = Address::repeat_byte(0xCD);
    let now = 1_000;
    let reads = StubSlashReads::new(
        op,
        vec![
            (U256::from(1u64), record(0, 1, 0x01, now - 100)),
            (U256::from(2u64), record(0, 1, 0x02, now - 50)),
            (U256::from(3u64), record(0, 1, 0x03, now + 10)),
        ],
    );

    let (_, slashes) = bootstrap_slashes(&reads, op, now, BlockId::latest())
        .await
        .unwrap();

    assert_eq!(slashes.len(), 1);
    assert_eq!(slashes[0].slash_id, U256::from(3u64));
    assert_eq!(
        reads.records_read(),
        vec![U256::from(3u64), U256::from(2u64)]
    );
}

/// The enforced deadline is the base `appealWindowClose` plus the global
/// `pausedTotal`.
#[tokio::test]
async fn bootstrap_slashes_honors_the_pause_extended_deadline() {
    let op = Address::repeat_byte(0x5A);
    let now = 1_000;
    let reads = StubSlashReads::new(
        op,
        vec![
            (U256::from(10u64), record(0, 1, 0x10, now - 50)),
            (U256::from(20u64), record(0, 1, 0x20, now - 10)),
            (U256::from(30u64), record(0, 1, 0x30, now - 5)),
        ],
    )
    .with_paused_total(100);

    let (_, slashes) = bootstrap_slashes(&reads, op, now, BlockId::latest())
        .await
        .unwrap();

    assert_eq!(slashes.len(), 3);
    assert_eq!(slashes[0].appeal_window_close, Some(now + 95));
    assert_eq!(slashes[2].appeal_window_close, Some(now + 50));
}

fn sync_over(reads: StubSlashReads, count: u64) -> (SlashSync<StubSlashReads>, Arc<Metrics>) {
    let metrics = Arc::new(Metrics::new());
    (
        SlashSync {
            reads,
            self_address: Address::repeat_byte(0xAB),
            store: Arc::new(RwLock::new(Vec::new())),
            metrics: Arc::clone(&metrics),
            count: Mutex::new(U256::from(count)),
        },
        metrics,
    )
}

/// A clean operator whose count has not moved costs one
/// `operatorSlashCount` read per refresh.
#[tokio::test]
async fn an_unchanged_count_with_no_open_slash_costs_one_read() {
    let (sync, _metrics) = sync_over(StubSlashReads::new(Address::repeat_byte(0xAB), vec![]), 0);
    sync.refresh().await;
    assert_eq!(sync.reads.blocks_read(), vec![BlockId::latest()]);
}

/// A new slash moves the count, so the refresh re-enumerates and records it.
#[tokio::test]
async fn a_new_slash_is_detected_on_the_next_refresh() {
    let far = unix_now() + 30 * 86_400;
    let (sync, metrics) = sync_over(StubSlashReads::new(Address::repeat_byte(0xAB), vec![]), 0);
    sync.reads
        .slashes
        .lock()
        .unwrap()
        .push((U256::from(5u64), record(1, 9, 0x05, far)));
    sync.refresh().await;
    let store = sync.store.read().unwrap().clone();
    assert_eq!(store.len(), 1);
    assert_eq!(store[0].slash_id, U256::from(5u64));
    assert_eq!(sync.count(), U256::from(1u64));
    let text = metrics.encode().unwrap();
    assert!(
        text.lines().any(|l| l == "decdn_slashes_detected_total 1"),
        "{text}"
    );

    // A second refresh re-reads (a slash is open) but counts nothing new.
    sync.refresh().await;
    let text = metrics.encode().unwrap();
    assert!(
        text.lines().any(|l| l == "decdn_slashes_detected_total 1"),
        "{text}"
    );
}
