use super::*;
use alloy::primitives::{B256, Bytes, U64};
use alloy::providers::ProviderBuilder;
use alloy::providers::mock::Asserter;
use alloy::sol_types::SolCall;

use crate::chain_events::boot_retry::BOOT_CHAIN_RETRY_BUDGET;

fn nid(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}

fn addr(byte: u8) -> Address {
    Address::from([byte; 20])
}

/// A transient provider error on the registry snapshot is retried on the
/// boot budget, not fatal.
#[tokio::test(start_paused = true)]
async fn a_transient_boot_snapshot_error_is_retried() {
    let asserter = Asserter::new();
    // The first read of an attempt is the head; it fails.
    asserter.push_failure(
        serde_json::from_value(serde_json::json!({
            "code": 1,
            "message": "no available upstreams to process the request",
        }))
        .unwrap(),
    );
    // The retry: the head, the code-presence `eth_getCode`, then one empty page.
    asserter.push_success(&U64::from(1_000));
    asserter.push_success(&Bytes::from_static(&[0x60]));
    let empty: (Vec<CapacityBond::NodeInfo>, Vec<bool>) = (Vec::new(), Vec::new());
    asserter.push_success(&Bytes::from(
        CapacityBond::getRegisteredNodesCall::abi_encode_returns_tuple(&empty),
    ));
    let metrics = Arc::new(Metrics::new());

    let handles = bootstrap(
        ProviderBuilder::new().connect_mocked_client(asserter.clone()),
        Address::repeat_byte(0x11),
        false,
        Arc::clone(&metrics),
        &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
        CancellationToken::new(),
    )
    .await
    .unwrap();

    assert!(handles.operator_to_node.read().unwrap().is_empty());
    assert!(asserter.read_q().is_empty(), "both attempts ran");
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_chain_boot_read_retries_total 1"),
        "{text}"
    );
}

/// A stalled provider cannot wedge the snapshot: each page read is bounded
/// by the per-call timeout.
#[tokio::test(start_paused = true)]
async fn a_stalled_provider_fails_the_snapshot_read() {
    use crate::chain_events::test_support::{bounded, hanging_provider};

    let registry = CapacityBond::new(Address::repeat_byte(0x11), hanging_provider());
    let err = bounded(
        "registry snapshot",
        bootstrap_registry(&registry, BlockId::latest()),
    )
    .await
    .err()
    .map(|e| format!("{e:#}"))
    .unwrap();

    assert!(err.contains("getRegisteredNodes timed out after"), "{err}");
}

/// A page whose `active[]` length differs from its `page` length is a
/// decoder fault, and a boot read must not retry it.
#[tokio::test]
async fn a_page_length_mismatch_is_a_permanent_boot_fault() {
    let asserter = Asserter::new();
    let page = vec![CapacityBond::NodeInfo {
        nodeId: B256::repeat_byte(0x01),
        ethAddress: Address::repeat_byte(0x02),
        active: true,
        lastMultiaddrUpdate: 0,
        multiaddrs: Bytes::new(),
        regionHint: String::new(),
    }];
    let active: Vec<bool> = Vec::new();
    asserter.push_success(&Bytes::from(
        CapacityBond::getRegisteredNodesCall::abi_encode_returns_tuple(&(page, active)),
    ));
    let provider = ProviderBuilder::new().connect_mocked_client(asserter);

    let err = bootstrap_registry(
        &CapacityBond::new(Address::repeat_byte(0x11), provider),
        BlockId::latest(),
    )
    .await
    .unwrap_err();

    assert!(
        format!("{err:#}").contains("mismatched page/active"),
        "{err:#}"
    );
    assert!(
        err.chain().any(<dyn std::error::Error>::is::<BootFault>),
        "{err:#}"
    );
}

/// Scripted [`RegistryChainReads`]: no provider, no chain.
struct StubReads {
    snapshot: std::sync::Mutex<std::result::Result<RegistrySnapshot, &'static str>>,
    reads: std::sync::atomic::AtomicUsize,
}

impl StubReads {
    fn new(snapshot: std::result::Result<RegistrySnapshot, &'static str>) -> Self {
        Self {
            snapshot: std::sync::Mutex::new(snapshot),
            reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn set(&self, snapshot: std::result::Result<RegistrySnapshot, &'static str>) {
        *self.snapshot.lock().unwrap() = snapshot;
    }
}

impl RegistryChainReads for StubReads {
    async fn snapshot_block(&self) -> Result<u64> {
        Ok(7)
    }

    async fn full_snapshot(&self, _at: u64) -> Result<RegistrySnapshot> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match &*self.snapshot.lock().unwrap() {
            Ok(v) => Ok(v.clone()),
            Err(msg) => Err(anyhow::anyhow!(*msg)),
        }
    }
}

fn snapshot_of(ids: &[u8]) -> RegistrySnapshot {
    let mut snap = RegistrySnapshot::default();
    for &id in ids {
        snap.active.insert(nid(id));
        snap.bindings.insert(nid(id), addr(id));
        snap.operator_to_node.insert(addr(id), nid(id));
        snap.regions.insert(nid(id), "DE".to_owned());
    }
    snap
}

#[tokio::test]
async fn the_boot_snapshot_seeds_every_projection() {
    let metrics = Arc::new(Metrics::new());
    let (handles, _sync, _trigger) = assemble(
        StubReads::new(Err("unused")),
        true,
        Arc::clone(&metrics),
        snapshot_of(&[1, 2]),
    );
    assert!(handles.staker_set.is_active(&nid(1)));
    assert_eq!(
        handles.node_addresses.as_ref().unwrap().address_of(&nid(2)),
        Some(addr(2))
    );
    assert_eq!(
        handles.operator_to_node.read().unwrap().get(&addr(1)),
        Some(&nid(1))
    );
    assert_eq!(region_of(&handles.regions, nid(1)).as_deref(), Some("DE"));
    let text = metrics.encode().unwrap();
    assert!(
        text.lines().any(|l| l == "decdn_staker_set_active_count 2"),
        "{text}"
    );
}

#[tokio::test]
async fn bindings_are_not_built_with_pull_through_off() {
    let (handles, _sync, _trigger) = assemble(
        StubReads::new(Err("unused")),
        false,
        Arc::new(Metrics::new()),
        snapshot_of(&[1]),
    );
    assert!(handles.node_addresses.is_none());
    assert!(!handles.operator_to_node.read().unwrap().is_empty());
}

/// A re-read replaces every projection wholesale.
#[tokio::test]
async fn a_refresh_replaces_every_projection() {
    let (handles, sync, _trigger) = assemble(
        StubReads::new(Ok(snapshot_of(&[3]))),
        true,
        Arc::new(Metrics::new()),
        snapshot_of(&[1, 2]),
    );
    sync.refresh().await;
    assert!(!handles.staker_set.is_active(&nid(1)));
    assert!(handles.staker_set.is_active(&nid(3)));
    assert_eq!(handles.operator_to_node.read().unwrap().len(), 1);
    assert_eq!(
        handles.node_addresses.as_ref().unwrap().address_of(&nid(3)),
        Some(addr(3))
    );
}

/// A failed re-read keeps the previous projections and counts the failure.
#[tokio::test]
async fn a_failed_refresh_keeps_the_previous_projections() {
    let metrics = Arc::new(Metrics::new());
    let (handles, sync, _trigger) = assemble(
        StubReads::new(Err("upstream down")),
        true,
        Arc::clone(&metrics),
        snapshot_of(&[1]),
    );
    sync.refresh().await;
    assert!(handles.staker_set.is_active(&nid(1)));
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_capacity_bond_registry_resync_failures_total 1"),
        "{text}"
    );
}

/// A lookup for an unknown `NodeId` pulls the next re-read forward, rate
/// limited to one per [`UNKNOWN_NODE_REFRESH_GAP`]; a node registered since the
/// boot snapshot becomes active on that re-read.
#[tokio::test(start_paused = true)]
async fn an_unknown_node_pulls_a_rate_limited_refresh() {
    let (handles, sync, trigger) = assemble(
        StubReads::new(Ok(snapshot_of(&[1]))),
        false,
        Arc::new(Metrics::new()),
        snapshot_of(&[1]),
    );
    let sync = Arc::new(sync);
    let shutdown = CancellationToken::new();
    let looped = Arc::clone(&sync);
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            run_refresh_loop(
                REGISTRY_REFRESH_INTERVAL,
                UNKNOWN_NODE_REFRESH_GAP,
                Some(trigger),
                shutdown,
                move || {
                    let looped = Arc::clone(&looped);
                    async move { looped.refresh().await }
                },
            )
            .await;
        }
    });
    sync.reads.set(Ok(snapshot_of(&[1, 9])));

    // Many lookups for the new node inside one gap cost one re-read.
    for _ in 0..20 {
        assert!(!handles.staker_set.is_active(&nid(9)));
    }
    tokio::time::sleep(UNKNOWN_NODE_REFRESH_GAP.saturating_sub(Duration::from_secs(1))).await;
    assert_eq!(
        sync.reads.reads.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        sync.reads.reads.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert!(handles.staker_set.is_active(&nid(9)));

    // Without a miss, the next read waits out the full interval.
    tokio::time::sleep(REGISTRY_REFRESH_INTERVAL.saturating_sub(Duration::from_secs(5))).await;
    assert_eq!(
        sync.reads.reads.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(
        sync.reads.reads.load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    shutdown.cancel();
}
