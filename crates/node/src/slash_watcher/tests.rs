use super::*;

/// A transient provider error during the boot enumeration is retried on the
/// boot budget, not fatal.
#[tokio::test(start_paused = true)]
async fn a_transient_boot_enumeration_error_is_retried() {
    use crate::chain_events::boot_retry::{BOOT_CHAIN_RETRY_BUDGET, BootRetry};
    use crate::chain_events::shared_head::SharedHead;
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolValue;

    let head_asserter = Asserter::new();
    head_asserter.push_success(&alloy::primitives::U64::from(100));
    let head: Arc<dyn HeadSource> = Arc::new(SharedHead::with_ttl(
        ProviderBuilder::new().connect_mocked_client(head_asserter),
        Duration::from_hours(1),
        None,
    ));
    let asserter = Asserter::new();
    // The first read of an attempt is the snapshot block's code-presence
    // `eth_getCode`, so the failure lands there; every boot read shares the
    // retry path.
    asserter.push_failure(
        serde_json::from_value(serde_json::json!({
            "code": 19,
            "message": "Temporary internal error. Please retry",
        }))
        .unwrap(),
    );
    // The retry: the code-presence `eth_getCode`, no slashes, then the pause
    // offset.
    asserter.push_success(&alloy::primitives::Bytes::from_static(&[0x60]));
    for _ in 0..2 {
        asserter.push_success(&alloy::primitives::Bytes::from(U256::ZERO.abi_encode()));
    }
    let metrics = Arc::new(Metrics::new());

    let (store, _route) = bootstrap(
        ProviderBuilder::new().connect_mocked_client(asserter.clone()),
        Address::repeat_byte(0x11),
        Address::repeat_byte(0x22),
        head,
        Arc::clone(&metrics),
        &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
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

/// The slash route watches exactly `SlashRecorded` — no more, no fewer —
/// and there is deliberately no `topic2` (operator) constraint on the route
/// (see [`slash_route_topic0s`]'s doc for why filtering happens in
/// [`decode_recorded`] instead).
#[test]
fn route_topic0s_is_exactly_slash_recorded() {
    assert_eq!(
        slash_route_topic0s(),
        vec![CapacityBond::SlashRecorded::SIGNATURE_HASH]
    );
}

/// The slash route seeds its cursor at the enumeration snapshot block, with
/// no durable persistence (the store is rebuilt from enumeration each boot).
#[test]
fn cursor_start_seeds_at_snapshot_with_no_persistence() {
    let start = slash_cursor_start(12_345);
    assert_eq!(start.seed(), Some(12_345));
    assert!(
        matches!(start, CursorStart::Seeded { .. }),
        "must not carry a durable checkpoint"
    );
}

/// A resync keeps a stored slash the tail saw above its snapshot block, and
/// drops one at or below the block that the authoritative enumeration lacks.
#[test]
fn a_resync_keeps_tail_slashes_above_its_block() {
    let tail_late = DetectedSlash {
        block_number: Some(900),
        ..slash(1)
    };
    let tail_early = DetectedSlash {
        block_number: Some(700),
        ..slash(2)
    };
    let enumerated = DetectedSlash {
        block_number: None,
        ..slash(3)
    };

    let folded = fold_resync(&[tail_late, tail_early], vec![enumerated], 800);

    let mut ids: Vec<U256> = folded.iter().map(|s| s.slash_id).collect();
    ids.sort();
    assert_eq!(ids, vec![U256::from(1u64), U256::from(3u64)]);
}

/// The sink's resync reads at the snapshot block and folds the tail on top:
/// a slash the tail recorded above that block survives a snapshot that lacks
/// it.
#[tokio::test]
async fn the_resync_folds_the_tail_over_the_snapshot() {
    let op = Address::repeat_byte(0xAB);
    let mut reads = StubSlashReads::new(op, Vec::new());
    reads.snapshot_block = 800;
    let store: SlashStore = Arc::new(RwLock::new(vec![DetectedSlash {
        block_number: Some(900),
        ..slash(1)
    }]));
    let mut sink = SlashSink {
        reads,
        self_address: op,
        store: Arc::clone(&store),
        metrics: Arc::new(Metrics::new()),
        resync_interval: SLASH_RESYNC_INTERVAL,
        last_resync: None,
    };

    sink.on_tick_complete().await.unwrap();

    assert_eq!(store.read().unwrap().len(), 1);
    assert!(
        sink.reads
            .blocks_read()
            .iter()
            .all(|b| *b == BlockId::number(800))
    );
}

/// Every contract read is bounded by the per-call timeout, not only the
/// snapshot block's `eth_getCode`: a stalled `operatorSlashCount` fails
/// rather than wedging boot or the resync.
#[tokio::test(start_paused = true)]
async fn a_stalled_contract_read_times_out() {
    use crate::chain_events::shared_head::SharedHead;
    use crate::chain_events::test_support::{bounded, hanging_provider};

    let provider = hanging_provider();
    let reads = ContractReads {
        bond: CapacityBond::new(Address::repeat_byte(0x11), provider.clone()),
        head: Arc::new(SharedHead::with_ttl(provider, Duration::ZERO, None)),
    };

    let err = bounded(
        "operatorSlashCount",
        reads.operator_slash_count(Address::repeat_byte(0x22), BlockId::number(1)),
    )
    .await
    .err()
    .map(|e| format!("{e:#}"))
    .unwrap();

    assert!(err.contains("operatorSlashCount timed out after"), "{err}");
}

/// A `DetectedSlash` distinguished only by `slash_id` (the dedup key).
fn slash(id: u64) -> DetectedSlash {
    DetectedSlash {
        slash_id: U256::from(id),
        offense_type: 0,
        amount: U256::from(42u64),
        evidence_hash: B256::repeat_byte(7),
        block_number: Some(100),
        appeal_window_close: None,
    }
}

/// A well-formed `SlashRecorded` RPC log for `operator`, with the `removed`
/// reorg flag under test control.
fn recorded_log(operator: Address, removed: bool) -> alloy::rpc::types::Log {
    let event = CapacityBond::SlashRecorded {
        slashId: U256::from(1u64),
        operator,
        slashedAt: 0,
        slashAmount: U256::from(42u64),
    };
    alloy::rpc::types::Log {
        inner: alloy::primitives::Log {
            address: Address::repeat_byte(0xAA),
            data: event.encode_log_data(),
        },
        removed,
        ..Default::default()
    }
}

/// Exact `<name> <value>` line match against the Prometheus text encoding,
/// so `..._total 1` can't accidentally match `..._total 10`.
fn has_metric_line(text: &str, name: &str, value: u64) -> bool {
    let needle = format!("{name} {value}");
    text.lines().any(|l| l.trim_end() == needle)
}

#[test]
fn record_slash_dedupes_by_slash_id() {
    let store: SlashStore = Arc::new(RwLock::new(Vec::new()));
    let metrics = Arc::new(Metrics::new());

    // Backfill/live overlap re-delivers the same slashId: no double insert,
    // no double count.
    record_slash(&store, &metrics, slash(1));
    record_slash(&store, &metrics, slash(1));
    record_slash(&store, &metrics, slash(2));

    let guard = store.read().unwrap();
    assert_eq!(guard.len(), 2, "duplicate slashId must not double-insert");
    assert!(guard.iter().any(|s| s.slash_id == U256::from(1u64)));
    assert!(guard.iter().any(|s| s.slash_id == U256::from(2u64)));
    drop(guard);
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_slashes_detected_total", 2),
        "duplicate slashId must not double-count:\n{text}"
    );
}

#[test]
fn decode_recorded_skips_removed_and_foreign_logs() {
    let operator = Address::repeat_byte(0x11);

    // The same log decodes when live but is skipped once reorged out
    // (`removed == true`), so a reorg can't record a phantom slash.
    assert_eq!(
        decode_recorded(operator, &recorded_log(operator, false)),
        Some(U256::from(1u64))
    );
    assert!(decode_recorded(operator, &recorded_log(operator, true)).is_none());
    // Defensive operator check: another operator's slash is never recorded
    // even if a provider ignores the `topic2` filter.
    assert!(decode_recorded(Address::repeat_byte(0x22), &recorded_log(operator, false)).is_none());
}

/// Scripted [`SlashChainReads`]: no provider, no chain. Slashes are supplied
/// oldest-first (append order), matching `operatorSlashIdAt`'s stable indices.
struct StubSlashReads {
    operator: Address,
    /// (slashId, record) in append order (index 0 = oldest).
    slashes: Vec<(U256, SlashRecordView)>,
    /// Global `CapacityBond.pausedTotal` the enumeration adds to each base close.
    paused_total: u64,
    /// slashIds whose record was point-read, for the early-stop assertion.
    reads: std::sync::Mutex<Vec<U256>>,
    /// The block every read ran at, for the pinned-read assertion.
    blocks: std::sync::Mutex<Vec<BlockId>>,
    /// What `snapshot_block` returns.
    snapshot_block: u64,
}

impl StubSlashReads {
    fn new(operator: Address, slashes: Vec<(U256, SlashRecordView)>) -> Self {
        Self {
            operator,
            slashes,
            paused_total: 0,
            reads: std::sync::Mutex::new(Vec::new()),
            blocks: std::sync::Mutex::new(Vec::new()),
            snapshot_block: 0,
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
        Ok(self.snapshot_block)
    }

    async fn operator_slash_count(&self, operator: Address, at: BlockId) -> Result<U256> {
        self.blocks.lock().unwrap().push(at);
        assert_eq!(operator, self.operator, "unexpected operator");
        Ok(U256::from(self.slashes.len()))
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
            .get(i)
            .map(|(id, _)| *id)
            .ok_or_else(|| anyhow::anyhow!("SlashIndexOutOfRange"))
    }

    async fn get_slash_record(&self, slash_id: U256, at: BlockId) -> Result<SlashRecordView> {
        self.blocks.lock().unwrap().push(at);
        self.reads.lock().unwrap().push(slash_id);
        self.slashes
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

/// A JSON-RPC responder that answers `eth_blockNumber` with block 1000 and
/// every other call (`eth_getCode`, `eth_call`) with a zero word, recording
/// each call's block tag.
struct BlockTagRpc {
    tags: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}

impl wiremock::Respond for BlockTagRpc {
    fn respond(&self, req: &wiremock::Request) -> wiremock::ResponseTemplate {
        let body: serde_json::Value =
            serde_json::from_slice(&req.body).unwrap_or(serde_json::Value::Null);
        let id = body.get("id").cloned().unwrap_or(serde_json::Value::Null);
        let result =
            if body.get("method").and_then(serde_json::Value::as_str) == Some("eth_blockNumber") {
                serde_json::json!("0x3e8")
            } else {
                let tag = body
                    .pointer("/params/1")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                self.tags.lock().unwrap().push(tag);
                serde_json::json!(format!("0x{}", "00".repeat(32)))
            };
        wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "jsonrpc": "2.0", "id": id, "result": result,
        }))
    }
}

/// The boot enumeration pins its reads to one snapshot block, so a lagging
/// load-balanced backend cannot answer the count and the records from
/// different blocks. The pin sits the lag margin below the reported head
/// (1000), so an upstream behind that head can still serve it.
#[tokio::test(flavor = "multi_thread")]
async fn the_boot_enumeration_reads_at_the_snapshot_block() {
    use crate::chain_events::boot_retry::BootRetry;
    use crate::chain_events::shared_head::{SNAPSHOT_LAG_MARGIN_BLOCKS, SharedHead};
    use alloy::providers::ProviderBuilder;

    let tags = Arc::new(std::sync::Mutex::new(Vec::new()));
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(BlockTagRpc {
            tags: Arc::clone(&tags),
        })
        .mount(&server)
        .await;
    let url: reqwest::Url = server.uri().parse().unwrap();
    let provider = ProviderBuilder::new().connect_http(url);
    let head: Arc<dyn HeadSource> = Arc::new(SharedHead::with_ttl(
        provider.clone(),
        Duration::from_hours(1),
        None,
    ));
    let metrics = Arc::new(Metrics::new());

    let (_store, route) = bootstrap(
        provider,
        Address::repeat_byte(0x11),
        Address::repeat_byte(0x22),
        head,
        Arc::clone(&metrics),
        &BootRetry::single_attempt(Arc::clone(&metrics)),
    )
    .await
    .unwrap();

    // No slashes: the code-presence `eth_getCode`, the count and the pause
    // offset — all at the snapshot block, which also seeds the tail cursor.
    let tags = tags.lock().unwrap().clone();
    let pinned = 1_000 - SNAPSHOT_LAG_MARGIN_BLOCKS;
    assert_eq!(tags, vec![serde_json::json!(format!("{pinned:#x}")); 3]);
    assert_eq!(route.start.seed(), Some(pinned));
}

/// A stalled provider cannot wedge boot: every read is bounded by the
/// per-call timeout, so the boot read fails into its budget. The first read
/// to stall is the snapshot block's `eth_getCode`;
/// `a_stalled_contract_read_times_out` covers the contract reads.
#[tokio::test(start_paused = true)]
async fn a_stalled_provider_fails_the_boot_enumeration() {
    use crate::chain_events::boot_retry::BootRetry;
    use crate::chain_events::shared_head::SharedHead;
    use crate::chain_events::test_support::{bounded, hanging_provider};
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;

    let head_asserter = Asserter::new();
    head_asserter.push_success(&alloy::primitives::U64::from(100));
    let head: Arc<dyn HeadSource> = Arc::new(SharedHead::with_ttl(
        ProviderBuilder::new().connect_mocked_client(head_asserter),
        Duration::from_hours(1),
        None,
    ));
    let metrics = Arc::new(Metrics::new());

    let err = bounded(
        "slash boot enumeration",
        bootstrap(
            hanging_provider(),
            Address::repeat_byte(0x11),
            Address::repeat_byte(0x22),
            head,
            Arc::clone(&metrics),
            &BootRetry::single_attempt(Arc::clone(&metrics)),
        ),
    )
    .await
    .err()
    .map(|e| format!("{e:#}"))
    .unwrap();

    assert!(err.contains("eth_getCode timed out after"), "{err}");
}

/// A deterministic head-read failure keeps its typed cause through the
/// `SharedHead` cache, so boot fails at once rather than retrying.
#[tokio::test(start_paused = true)]
async fn a_permanent_head_error_fails_the_boot_enumeration_at_once() {
    use crate::chain_events::boot_retry::{BOOT_CHAIN_RETRY_BUDGET, BootRetry};
    use crate::chain_events::shared_head::SharedHead;
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;

    let head_asserter = Asserter::new();
    head_asserter.push_failure(
        serde_json::from_value(serde_json::json!({
            "code": -32601,
            "message": "method not found",
        }))
        .unwrap(),
    );
    let head: Arc<dyn HeadSource> = Arc::new(SharedHead::with_ttl(
        ProviderBuilder::new().connect_mocked_client(head_asserter),
        Duration::from_hours(1),
        None,
    ));
    let metrics = Arc::new(Metrics::new());
    let start = tokio::time::Instant::now();

    let err = bootstrap(
        ProviderBuilder::new().connect_mocked_client(Asserter::new()),
        Address::repeat_byte(0x11),
        Address::repeat_byte(0x22),
        head,
        Arc::clone(&metrics),
        &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
    )
    .await
    .unwrap_err();

    assert!(format!("{err:#}").contains("not retried"), "{err:#}");
    assert_eq!(start.elapsed(), Duration::ZERO);
}

/// Enumeration surfaces the still-appealable slashes newest-first, carrying
/// the authoritative record fields, and excludes those whose window closed.
#[tokio::test]
async fn bootstrap_slashes_surfaces_only_still_appealable_slashes_newest_first() {
    let op = Address::repeat_byte(0xAB);
    let now = 1_000;
    // Append order (oldest→newest): id 10 already closed, 20 and 30 still open.
    let reads = StubSlashReads::new(
        op,
        vec![
            (U256::from(10u64), record(1, 100, 0x11, now - 1)),
            (U256::from(20u64), record(1, 200, 0x22, now + 50)),
            (U256::from(30u64), record(0, 300, 0x33, now + 99)),
        ],
    );

    let slashes = bootstrap_slashes(&reads, op, now, BlockId::latest())
        .await
        .unwrap();

    assert_eq!(slashes.len(), 2, "closed slash 10 must be excluded");
    // Newest-first backward walk: 30 then 20.
    assert_eq!(slashes[0].slash_id, U256::from(30u64));
    assert_eq!(slashes[1].slash_id, U256::from(20u64));
    // Authoritative appeal-window close carried from the record, not derived.
    assert_eq!(slashes[0].appeal_window_close, Some(now + 99));
    assert_eq!(slashes[0].offense_type, 0);
    assert_eq!(slashes[1].evidence_hash, B256::repeat_byte(0x22));
    // No log, so no block number.
    assert_eq!(slashes[0].block_number, None);
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

    let slashes = bootstrap_slashes(&reads, op, now, BlockId::latest())
        .await
        .unwrap();

    assert_eq!(slashes.len(), 1);
    assert_eq!(slashes[0].slash_id, U256::from(3u64));
    // Only the newest was point-read; the walk stopped at slash 2 without
    // reading slash 1.
    assert_eq!(
        reads.records_read(),
        vec![U256::from(3u64), U256::from(2u64)]
    );
}

/// The enforced deadline is the base `appealWindowClose` plus the global
/// `pausedTotal`. A slash whose base window has closed but whose pause-extended
/// window is still open MUST stay visible — and the backward walk must not stop
/// early on it and hide older still-appealable slashes.
#[tokio::test]
async fn bootstrap_slashes_honors_the_pause_extended_deadline() {
    let op = Address::repeat_byte(0x5A);
    let now = 1_000;
    // Every base window has already closed (all <= now)…
    let reads = StubSlashReads::new(
        op,
        vec![
            (U256::from(10u64), record(0, 1, 0x10, now - 50)),
            (U256::from(20u64), record(0, 1, 0x20, now - 10)),
            (U256::from(30u64), record(0, 1, 0x30, now - 5)),
        ],
    )
    // …but a 100s protocol pause moves every effective deadline past `now`.
    .with_paused_total(100);

    let slashes = bootstrap_slashes(&reads, op, now, BlockId::latest())
        .await
        .unwrap();

    assert_eq!(
        slashes.len(),
        3,
        "all three are still appealable once pausedTotal is added; ignoring it \
         would surface none (base windows closed) and stop the walk early"
    );
    // The surfaced deadline is the pause-extended one the contract enforces.
    assert_eq!(slashes[0].appeal_window_close, Some(now + 95)); // 30: (now-5)+100
    assert_eq!(slashes[2].appeal_window_close, Some(now + 50)); // 10: (now-50)+100
}

/// An operator with no slashes enumerates to an empty set without error.
#[tokio::test]
async fn bootstrap_slashes_of_a_clean_operator_is_empty() {
    let op = Address::repeat_byte(0xEF);
    let reads = StubSlashReads::new(op, vec![]);
    assert!(
        bootstrap_slashes(&reads, op, 1_000, BlockId::latest())
            .await
            .unwrap()
            .is_empty()
    );
}
