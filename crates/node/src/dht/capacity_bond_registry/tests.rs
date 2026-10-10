use super::*;
use alloy::primitives::{Bytes, LogData};

/// The registry route watches exactly the six `CapacityBond`
/// staker-membership events plus the region and multiaddr updates — no
/// more, no fewer.
///
/// `EjectedByBlacklist` is load-bearing and was absent until #1030: it is
/// the only event that reports a blacklist ejection, `ejected` is a conjunct
/// of `isActive`, and without it an ejected operator stayed in the active
/// set until the next 15-minute re-enumeration.
#[test]
fn route_topic0s_covers_every_projection_event() {
    assert_eq!(
        registry_route_topic0s(),
        vec![
            CapacityBond::NodeRegistered::SIGNATURE_HASH,
            CapacityBond::NodeDeregistered::SIGNATURE_HASH,
            CapacityBond::NodeAutoEjected::SIGNATURE_HASH,
            CapacityBond::Reinstated::SIGNATURE_HASH,
            CapacityBond::UnbondingRequested::SIGNATURE_HASH,
            CapacityBond::EjectedByBlacklist::SIGNATURE_HASH,
            CapacityBond::RegionUpdated::SIGNATURE_HASH,
            CapacityBond::NodeMultiaddrUpdated::SIGNATURE_HASH,
        ]
    );
}

/// A transient provider error on the registry snapshot is retried on the
/// boot budget, not fatal.
#[tokio::test(start_paused = true)]
async fn a_transient_boot_snapshot_error_is_retried() {
    use crate::chain_events::boot_retry::{BOOT_CHAIN_RETRY_BUDGET, BootRetry};
    use crate::chain_events::shared_head::SharedHead;
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;
    use alloy::sol_types::SolCall;

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
            "code": 1,
            "message": "no available upstreams to process the request",
        }))
        .unwrap(),
    );
    // The retry: the code-presence `eth_getCode`, then one empty page.
    asserter.push_success(&Bytes::from_static(&[0x60]));
    let empty: (Vec<CapacityBond::NodeInfo>, Vec<bool>) = (Vec::new(), Vec::new());
    asserter.push_success(&Bytes::from(
        CapacityBond::getRegisteredNodesCall::abi_encode_returns_tuple(&empty),
    ));
    let metrics = Arc::new(Metrics::new());

    let handles = bootstrap(
        ProviderBuilder::new().connect_mocked_client(asserter.clone()),
        Address::repeat_byte(0x11),
        head,
        false,
        Arc::clone(&metrics),
        &BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, Arc::clone(&metrics)),
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
    use alloy::sol_types::SolCall;

    let asserter = alloy::providers::mock::Asserter::new();
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
    let provider = alloy::providers::ProviderBuilder::new().connect_mocked_client(asserter);

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

/// The registry route seeds its cursor at the enumeration snapshot block,
/// with no durable persistence (the staker set is rebuilt from enumeration
/// each boot).
#[test]
fn cursor_start_seeds_at_snapshot_with_no_persistence() {
    let start = registry_cursor_start(54_321);
    assert_eq!(start.seed(), Some(54_321));
    assert!(
        matches!(start, CursorStart::Seeded { .. }),
        "must not carry a durable checkpoint"
    );
}

fn nid(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}

fn addr(byte: u8) -> Address {
    Address::from([byte; 20])
}

/// Scripted [`RegistryChainReads`]: no provider, no chain.
struct StubReads {
    node_id: std::result::Result<Option<(NodeId, bool)>, &'static str>,
    /// What `full_snapshot` returns; `Err` models an unreadable chain.
    snapshot: std::result::Result<RegistrySnapshot, &'static str>,
    /// What `snapshot_block` returns.
    snapshot_block: u64,
}

impl StubReads {
    fn new(node_id: std::result::Result<Option<(NodeId, bool)>, &'static str>) -> Self {
        Self {
            node_id,
            snapshot: Ok(RegistrySnapshot::default()),
            snapshot_block: 0,
        }
    }

    fn with_snapshot(
        mut self,
        snapshot: std::result::Result<RegistrySnapshot, &'static str>,
    ) -> Self {
        self.snapshot = snapshot;
        self
    }
}

impl RegistryChainReads for StubReads {
    async fn snapshot_block(&self) -> Result<u64> {
        Ok(self.snapshot_block)
    }

    async fn node_id_of(&self, _operator: Address) -> Result<Option<(NodeId, bool)>> {
        match &self.node_id {
            Ok(v) => Ok(*v),
            Err(msg) => Err(anyhow::anyhow!(*msg)),
        }
    }

    /// An `Err` carries the same page context the real reader adds, so the
    /// cause sits one level below the outermost message.
    async fn full_snapshot(&self, _at: u64) -> Result<RegistrySnapshot> {
        match &self.snapshot {
            Ok(v) => Ok(v.clone()),
            Err(msg) => Err(anyhow::anyhow!(*msg)
                .context(format!("getRegisteredNodes(offset=0, limit={PAGE_SIZE})"))),
        }
    }
}

/// A sink plus handles on the projections it writes to.
type Fixture = (
    RegistrySink<StubReads>,
    Arc<RwLock<HashSet<NodeId>>>,
    Option<Arc<RwLock<HashMap<NodeId, Address>>>>,
    Arc<RwLock<HashMap<Address, NodeId>>>,
    Arc<RwLock<HashMap<NodeId, String>>>,
    Arc<Metrics>,
);

/// A sink over empty projections. `bindings_on` mirrors
/// `cache.node_to_node_pull_through_enabled`.
fn sink(reads: StubReads, bindings_on: bool) -> Fixture {
    let active = Arc::new(RwLock::new(HashSet::new()));
    let bindings = bindings_on.then(|| Arc::new(RwLock::new(HashMap::new())));
    let operator_to_node = Arc::new(RwLock::new(HashMap::new()));
    let regions = Arc::new(RwLock::new(HashMap::new()));
    let metrics = Arc::new(Metrics::new());
    let s = RegistrySink {
        reads,
        active: Arc::clone(&active),
        bindings: bindings.clone(),
        operator_to_node: Arc::clone(&operator_to_node),
        regions: Arc::clone(&regions),
        dial_addrs: DialAddrDirectory::default(),
        metrics: Arc::clone(&metrics),
        resync_interval: REGISTRY_RESYNC_INTERVAL,
        last_resync: Some(Instant::now()),
        tail_changes: HashMap::new(),
    };
    (s, active, bindings, operator_to_node, regions, metrics)
}

fn ok_reads() -> StubReads {
    StubReads::new(Ok(None))
}

fn is_active(active: &Arc<RwLock<HashSet<NodeId>>>, id: NodeId) -> bool {
    active.read().is_ok_and(|g| g.contains(&id))
}

fn binding_of(
    bindings: Option<&Arc<RwLock<HashMap<NodeId, Address>>>>,
    id: NodeId,
) -> Option<Address> {
    bindings.and_then(|b| b.read().ok().and_then(|g| g.get(&id).copied()))
}

fn reverse_of(
    operator_to_node: &Arc<RwLock<HashMap<Address, NodeId>>>,
    operator: Address,
) -> Option<NodeId> {
    operator_to_node
        .read()
        .ok()
        .and_then(|g| g.get(&operator).copied())
}

/// `NodeRegistered(nodeId indexed, ethAddress indexed, ...)`.
fn registered_log(id: NodeId, operator: Address) -> Log {
    let event = CapacityBond::NodeRegistered {
        nodeId: B256::from(*id.as_bytes()),
        ethAddress: operator,
        multiaddrs: Bytes::new(),
        regionHint: String::new(),
        bindingNonce: 1,
        registrationNonce: 1,
    };
    log_from(event.encode_log_data())
}

fn registered_log_region(id: NodeId, operator: Address, region: &str) -> Log {
    let event = CapacityBond::NodeRegistered {
        nodeId: B256::from(*id.as_bytes()),
        ethAddress: operator,
        multiaddrs: Bytes::new(),
        regionHint: region.to_string(),
        bindingNonce: 1,
        registrationNonce: 1,
    };
    log_from(event.encode_log_data())
}

fn region_updated_log(id: NodeId, old: &str, new: &str) -> Log {
    let event = CapacityBond::RegionUpdated {
        nodeId: B256::from(*id.as_bytes()),
        oldRegion: old.to_string(),
        newRegion: new.to_string(),
    };
    log_from(event.encode_log_data())
}

fn deregistered_log(id: NodeId) -> Log {
    let event = CapacityBond::NodeDeregistered {
        nodeId: B256::from(*id.as_bytes()),
    };
    log_from(event.encode_log_data())
}

fn auto_ejected_log(id: NodeId) -> Log {
    let event = CapacityBond::NodeAutoEjected {
        nodeId: B256::from(*id.as_bytes()),
        remainingBond: U256::ZERO,
    };
    log_from(event.encode_log_data())
}

fn reinstated_log(operator: Address) -> Log {
    let event = CapacityBond::Reinstated { operator };
    log_from(event.encode_log_data())
}

fn ejected_by_blacklist_log(operator: Address) -> Log {
    let event = CapacityBond::EjectedByBlacklist { operator };
    log_from(event.encode_log_data())
}

fn log_from(data: LogData) -> Log {
    Log {
        inner: alloy::primitives::Log {
            address: Address::ZERO,
            data,
        },
        ..Default::default()
    }
}

/// The core of the merge: one log, one decode, both projections updated.
#[tokio::test]
async fn node_registered_updates_both_projections_from_one_decode() {
    let (mut s, active, bindings, _op, _regions, _m) = sink(ok_reads(), true);
    let r = s.apply(registered_log(nid(1), addr(9))).await;

    assert!(r.is_ok());
    assert!(is_active(&active, nid(1)), "registered node is active");
    assert_eq!(binding_of(bindings.as_ref(), nid(1)), Some(addr(9)));
}

/// With pull-through off the bindings projection does not exist, so the
/// staker set still updates and nothing touches (or publishes) bindings.
#[tokio::test]
async fn node_registered_with_bindings_off_updates_only_the_staker_set() {
    let (mut s, active, bindings, _op, _regions, _m) = sink(ok_reads(), false);
    let r = s.apply(registered_log(nid(1), addr(9))).await;

    assert!(r.is_ok());
    assert!(is_active(&active, nid(1)));
    assert!(bindings.is_none(), "no bindings projection when gated off");
}

/// `deregisterNode` is the ONLY event that clears a binding.
#[tokio::test]
async fn node_deregistered_removes_from_both() {
    let (mut s, active, bindings, _op, _regions, _m) = sink(ok_reads(), true);
    let _ = s.apply(registered_log(nid(1), addr(9))).await;

    let r = s.apply(deregistered_log(nid(1))).await;

    assert!(r.is_ok());
    assert!(!is_active(&active, nid(1)));
    assert_eq!(binding_of(bindings.as_ref(), nid(1)), None);
}

/// The highest-value test in this module. Ejection flips `isActive` ONLY —
/// the binding must survive, because the operator may still be owed payment
/// on an open lane. A reflexive "union the arms" merge would drop it here.
#[tokio::test]
async fn node_auto_ejected_deactivates_but_keeps_the_binding() {
    let (mut s, active, bindings, _op, _regions, _m) = sink(ok_reads(), true);
    let _ = s.apply(registered_log(nid(1), addr(9))).await;

    let r = s.apply(auto_ejected_log(nid(1))).await;

    assert!(r.is_ok());
    assert!(!is_active(&active, nid(1)), "ejection deactivates");
    assert_eq!(
        binding_of(bindings.as_ref(), nid(1)),
        Some(addr(9)),
        "ejection must NOT clear the payout binding"
    );
}

/// `NodeRegistered` populates the reverse map alongside the forward binding.
#[tokio::test]
async fn node_registered_populates_reverse_map() {
    let (mut s, _active, _bindings, operator_to_node, _regions, _m) = sink(ok_reads(), true);
    let r = s.apply(registered_log(nid(1), addr(9))).await;

    assert!(r.is_ok());
    assert_eq!(reverse_of(&operator_to_node, addr(9)), Some(nid(1)));
}

/// `NodeDeregistered` removes the reverse entry, the same lifecycle as the
/// forward binding.
#[tokio::test]
async fn node_deregistered_removes_from_reverse_map() {
    let (mut s, _active, _bindings, operator_to_node, _regions, _m) = sink(ok_reads(), true);
    let _ = s.apply(registered_log(nid(1), addr(9))).await;

    let r = s.apply(deregistered_log(nid(1))).await;

    assert!(r.is_ok());
    assert_eq!(reverse_of(&operator_to_node, addr(9)), None);
}

/// Ejection must NOT drop the reverse entry: the map is unfiltered, and the
/// origin directory resolves an authorized operator's `NodeId` regardless of
/// current liveness — the `StakerSet` applies the liveness filter separately
/// at read time.
#[tokio::test]
async fn node_auto_ejected_keeps_reverse_binding() {
    let (mut s, _active, _bindings, operator_to_node, _regions, _m) = sink(ok_reads(), true);
    let _ = s.apply(registered_log(nid(1), addr(9))).await;

    let r = s.apply(auto_ejected_log(nid(1))).await;

    assert!(r.is_ok());
    assert_eq!(
        reverse_of(&operator_to_node, addr(9)),
        Some(nid(1)),
        "ejection must NOT clear the reverse binding"
    );
}

/// The reverse map is always built, unlike the pull-through-gated
/// `bindings` projection.
#[tokio::test]
async fn reverse_map_built_even_with_pull_through_off() {
    let (mut s, _active, bindings, operator_to_node, _regions, _m) = sink(ok_reads(), false);
    let r = s.apply(registered_log(nid(1), addr(9))).await;

    assert!(r.is_ok());
    assert!(bindings.is_none(), "bindings is gated off");
    assert_eq!(
        reverse_of(&operator_to_node, addr(9)),
        Some(nid(1)),
        "the reverse map is always-on"
    );
}

/// The region projection mirrors binding lifecycle: set on registration,
/// cleared on deregistration.
#[tokio::test]
async fn node_registered_captures_region_and_deregister_clears_it() {
    let (mut s, _active, _bindings, _op, regions, _m) = sink(ok_reads(), true);

    let _ = s.apply(registered_log_region(nid(1), addr(9), "DE")).await;
    assert_eq!(region_of(&regions, nid(1)), Some("DE".to_string()));

    let _ = s.apply(deregistered_log(nid(1))).await;
    assert_eq!(
        region_of(&regions, nid(1)),
        None,
        "deregister clears region"
    );
}

/// An empty `regionHint` means absence — no map entry, not an empty string
/// entry.
#[tokio::test]
async fn empty_region_hint_is_not_stored() {
    let (mut s, _active, _bindings, _op, regions, _m) = sink(ok_reads(), true);
    // The existing `registered_log` helper sets `regionHint: String::new()`.
    let _ = s.apply(registered_log(nid(1), addr(9))).await;
    assert_eq!(region_of(&regions, nid(1)), None, "empty region is absence");
}

/// Ejection deactivates but must not clear the region, mirroring the
/// binding: the region reflects the payout jurisdiction, not membership.
#[tokio::test]
async fn auto_eject_keeps_region() {
    let (mut s, _active, _bindings, _op, regions, _m) = sink(ok_reads(), true);
    let _ = s.apply(registered_log_region(nid(1), addr(9), "FR")).await;

    let _ = s.apply(auto_ejected_log(nid(1))).await;

    assert_eq!(
        region_of(&regions, nid(1)),
        Some("FR".to_string()),
        "ejection deactivates but keeps region, like the binding"
    );
}

/// The per-region gauge counts active nodes only, normalizes the region
/// through `Region::parse`, buckets unparsable codes as unknown, and drops
/// a region's series once its last node leaves.
#[tokio::test]
async fn tick_publishes_active_nodes_by_region() {
    let (mut s, _active, _bindings, _op, _regions, metrics) = sink(ok_reads(), true);
    let _ = s.apply(registered_log_region(nid(1), addr(1), "DE")).await;
    let _ = s
        .apply(registered_log_region(nid(2), addr(2), " de "))
        .await;
    let _ = s
        .apply(registered_log_region(nid(3), addr(3), "Germany"))
        .await;
    let _ = s.apply(registered_log_region(nid(4), addr(4), "FR")).await;
    let _ = s.apply(auto_ejected_log(nid(4))).await;
    // No hint at all: no region-map entry, still an active node.
    let _ = s.apply(registered_log(nid(5), addr(5))).await;
    s.on_tick_complete().await.unwrap();

    let text = metrics.encode().unwrap();
    let has = |line: &str| text.lines().any(|l| l == line);
    assert!(
        has(r#"decdn_staker_set_active_by_region{node_region="DE"} 2"#),
        "{text}"
    );
    assert!(has("decdn_staker_set_active_unknown_region 2"), "{text}");
    // The split accounts for every active node after a clean tick.
    assert!(has("decdn_staker_set_active_count 4"), "{text}");
    assert!(
        !text.contains(r#"node_region="FR""#),
        "an ejected node was counted"
    );
    assert!(
        !text.contains(r#"node_region="Germany""#),
        "an unparsable code became a label"
    );

    let _ = s.apply(deregistered_log(nid(1))).await;
    let _ = s.apply(deregistered_log(nid(2))).await;
    s.on_tick_complete().await.unwrap();
    let text = metrics.encode().unwrap();
    assert!(
        !text.contains("decdn_staker_set_active_by_region{"),
        "an emptied region kept its series: {text}"
    );
}

/// Every write path stores the canonical code: a raw `" de "` must compare
/// equal to the node's own normalized `DE` in the ADR-030 penalty, and an
/// invalid code is absence.
#[tokio::test]
async fn region_map_stores_canonical_codes_only() {
    let (mut s, _active, _bindings, _op, regions, _m) = sink(ok_reads(), true);
    let _ = s
        .apply(registered_log_region(nid(1), addr(1), " de "))
        .await;
    let _ = s
        .apply(registered_log_region(nid(2), addr(2), "Germany"))
        .await;
    assert_eq!(region_of(&regions, nid(1)), Some("DE".to_string()));
    assert_eq!(region_of(&regions, nid(2)), None, "invalid code is absence");

    let _ = s.apply(region_updated_log(nid(1), "DE", "us ")).await;
    assert_eq!(region_of(&regions, nid(1)), Some("US".to_string()));
    let _ = s.apply(region_updated_log(nid(1), "US", "Germany")).await;
    assert_eq!(
        region_of(&regions, nid(1)),
        None,
        "invalid update is absence"
    );
}

/// `RegionUpdated` moves a node between regions on the tick it lands, and
/// an empty new region counts the node as unknown.
#[tokio::test]
async fn region_updated_moves_the_node_on_the_next_tick() {
    let (mut s, active, _bindings, _op, regions, metrics) = sink(ok_reads(), true);
    let _ = s.apply(registered_log_region(nid(1), addr(1), "DE")).await;
    let _ = s.apply(region_updated_log(nid(1), "DE", "US")).await;
    s.on_tick_complete().await.unwrap();

    assert_eq!(region_of(&regions, nid(1)), Some("US".to_string()));
    assert!(
        is_active(&active, nid(1)),
        "a region change touched membership"
    );
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == r#"decdn_staker_set_active_by_region{node_region="US"} 1"#),
        "{text}"
    );
    assert!(
        !text.contains(r#"node_region="DE""#),
        "the old region kept its series"
    );

    let _ = s.apply(region_updated_log(nid(1), "US", "")).await;
    s.on_tick_complete().await.unwrap();
    assert_eq!(region_of(&regions, nid(1)), None, "empty region is absence");
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_staker_set_active_unknown_region 1"),
        "{text}"
    );
}

/// A resync republishes the region split from the new snapshot. The
/// publish at the top of the tick runs before the swap, so only the
/// post-swap publish can export the snapshot's region.
#[tokio::test]
async fn resync_republishes_the_region_split() {
    let snapshot = RegistrySnapshot {
        regions: HashMap::from([(nid(2), "JP".to_string())]),
        ..snapshot_of(&[2], &[2])
    };
    let reads = StubReads::new(Ok(None)).with_snapshot(Ok(snapshot));
    let (mut sink, _active, _bindings, _op, _regions, metrics) = sink(reads, true);
    sink.last_resync = None;

    sink.on_tick_complete().await.unwrap();

    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == r#"decdn_staker_set_active_by_region{node_region="JP"} 1"#),
        "{text}"
    );
}

/// Operator-indexed events resolve through `nodeIdOf` and never touch a
/// binding.
#[tokio::test]
async fn reinstated_resolves_via_node_id_of_and_leaves_bindings_untouched() {
    let (mut s, active, bindings, _op, _regions, _m) =
        sink(StubReads::new(Ok(Some((nid(1), true)))), true);
    let _ = s.apply(registered_log(nid(1), addr(9))).await;
    let _ = s.apply(auto_ejected_log(nid(1))).await;

    let r = s.apply(reinstated_log(addr(9))).await;

    assert!(r.is_ok());
    assert!(is_active(&active, nid(1)), "nodeIdOf.active wins");
    assert_eq!(binding_of(bindings.as_ref(), nid(1)), Some(addr(9)));
}

/// The `Reinstated` twin, and the arm #1030 added. `ejected` is a conjunct
/// of `isActive`, so a blacklist ejection must deactivate immediately —
/// before #1030 this event was not in the route's OR-set at all, so an
/// ejected operator stayed servable (and DHT-admissible) for up to
/// `REGISTRY_RESYNC_INTERVAL`.
///
/// `nodeIdOf` reports `active: false` here, matching what the event implies;
/// the binding survives, because an ejected operator may still be owed
/// payment on an open lane.
#[tokio::test]
async fn ejected_by_blacklist_deactivates_but_keeps_the_binding() {
    let (mut s, active, bindings, _op, _regions, _m) =
        sink(StubReads::new(Ok(Some((nid(1), false)))), true);
    let _ = s.apply(registered_log(nid(1), addr(9))).await;
    assert!(is_active(&active, nid(1)), "registered node starts active");

    let r = s.apply(ejected_by_blacklist_log(addr(9))).await;

    assert!(r.is_ok());
    assert!(
        !is_active(&active, nid(1)),
        "a blacklist ejection must deactivate the operator"
    );
    assert_eq!(
        binding_of(bindings.as_ref(), nid(1)),
        Some(addr(9)),
        "ejection must NOT clear the payout binding"
    );
}

/// The canonical `nodeIdOf.active` beats what the event implies when a later
/// transition raced it.
#[tokio::test]
async fn node_id_of_disagreeing_with_the_event_wins() {
    // `Reinstated` implies active, but nodeIdOf says otherwise.
    let (mut s, active, _b, _op, _regions, _m) =
        sink(StubReads::new(Ok(Some((nid(1), false)))), true);
    let _ = s.apply(registered_log(nid(1), addr(9))).await;

    let r = s.apply(reinstated_log(addr(9))).await;

    assert!(r.is_ok());
    assert!(!is_active(&active, nid(1)), "canonical nodeIdOf wins");
}

/// A `nodeIdOf` failure is counted and skipped — it must NOT return `Err`,
/// which would back the whole shared loop off (and now that the loop is
/// shared, would stall the bindings projection too).
#[tokio::test]
async fn node_id_of_failure_bumps_resolve_failure_and_returns_ok() {
    let (mut s, _a, _b, _op, _regions, metrics) = sink(StubReads::new(Err("rpc down")), true);
    let r = s.apply(reinstated_log(addr(9))).await;

    assert!(r.is_ok(), "a resolve failure must not fail the tick");
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_staker_set_watcher_resolve_failures_total 1"),
        "the drop must surface on its own counter:\n{text}"
    );
}

/// An unbound operator (`bytes32(0)`) is ignored: binding precedes activation.
#[tokio::test]
async fn unbound_operator_event_is_ignored() {
    let (mut s, active, _b, _op, _regions, _m) = sink(StubReads::new(Ok(None)), true);
    let r = s.apply(reinstated_log(addr(9))).await;

    assert!(r.is_ok());
    assert_eq!(active.read().map_or(1, |g| g.len()), 0);
}

/// An undecodable log is skipped, not surfaced as `Err` — a deterministic
/// re-scan of one would otherwise hot-loop the cursor forever.
#[tokio::test]
async fn undecodable_log_is_skipped_and_returns_ok() {
    let (mut s, active, _b, _op, _regions, _m) = sink(ok_reads(), true);
    let mut log = registered_log(nid(1), addr(9));
    log.inner.data.data = Bytes::from_static(b"garbage");

    let r = s.apply(log).await;

    assert!(r.is_ok(), "undecodable log must not fail the tick");
    assert!(!is_active(&active, nid(1)));
}

/// The staker-set family tracks the one shared loop. It is unconditional:
/// this loop feeds the bindings projection too, so its health is the same
/// health whether or not pull-through is on.
///
/// The third call is what gives the `established` leg coverage.
/// `backoff_started` is edge-triggered on `down_since` being unset, so the
/// re-arm only counts if `established` actually closed the window. Asserting
/// `down_seconds 0` after an `established` instead would prove nothing: an
/// immediate scrape reads `0` even with the window open, because the elapsed
/// time floors to zero seconds.
#[test]
fn watcher_hooks_track_the_shared_loop() {
    let metrics = Arc::new(Metrics::new());
    let established = metric_hook(&metrics, Metrics::staker_set_watcher_cycle_established);
    let backoff = metric_hook(&metrics, Metrics::staker_set_watcher_backoff_started);
    backoff();
    established();
    backoff();

    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_staker_set_watcher_restarts_total 2"),
        "established() must close the drift window so a later backoff re-arms:\n{text}"
    );
}

// ── Dial-address directory ──────────────────────────────────────────────

/// A registry id that is a valid ed25519 key, so the directory also
/// publishes it to the iroh lookup.
fn dialable(seed: u8) -> (NodeId, iroh::PublicKey) {
    let pk = iroh::SecretKey::from_bytes(&[seed; 32]).public();
    (NodeId::from_bytes(*pk.as_bytes()), pk)
}

fn packed(multiaddrs: &[&str]) -> Bytes {
    let owned: Vec<String> = multiaddrs.iter().map(ToString::to_string).collect();
    Bytes::from(decdn_incentive::node_register::pack_multiaddrs(&owned).unwrap())
}

fn sock(port: u16) -> SocketAddr {
    SocketAddr::from(([203, 0, 113, 10], port))
}

fn registered_log_multiaddrs(id: NodeId, operator: Address, multiaddrs: Bytes) -> Log {
    let event = CapacityBond::NodeRegistered {
        nodeId: B256::from(*id.as_bytes()),
        ethAddress: operator,
        multiaddrs,
        regionHint: String::new(),
        bindingNonce: 1,
        registrationNonce: 1,
    };
    log_from(event.encode_log_data())
}

fn multiaddr_updated_log(id: NodeId, multiaddrs: Bytes) -> Log {
    let event = CapacityBond::NodeMultiaddrUpdated {
        nodeId: B256::from(*id.as_bytes()),
        multiaddrs,
    };
    log_from(event.encode_log_data())
}

/// The directory follows a node's registry record through its lifecycle:
/// `NodeRegistered` sets it, `NodeMultiaddrUpdated` replaces it (an empty
/// record clears it), and `NodeDeregistered` removes it. Each change reaches
/// the iroh lookup the endpoint resolves bare-id dials through.
#[tokio::test]
async fn dial_addrs_follow_register_update_and_deregister() {
    let (mut s, _active, _bindings, _op, _regions, _m) = sink(ok_reads(), false);
    let (id, pk) = dialable(1);
    let in_lookup = |s: &RegistrySink<StubReads>| {
        s.dial_addrs.lookup().get_endpoint_info(pk).map(|info| {
            info.to_endpoint_addr()
                .ip_addrs()
                .copied()
                .collect::<Vec<_>>()
        })
    };

    s.apply(registered_log_multiaddrs(
        id,
        addr(9),
        packed(&["/ip4/203.0.113.10/udp/4433/quic-v1"]),
    ))
    .await
    .unwrap();
    assert_eq!(s.dial_addrs.get(&id), Some(vec![sock(4433)]));
    assert_eq!(in_lookup(&s), Some(vec![sock(4433)]));

    s.apply(multiaddr_updated_log(
        id,
        packed(&["/ip4/203.0.113.10/udp/5000/quic-v1"]),
    ))
    .await
    .unwrap();
    assert_eq!(in_lookup(&s), Some(vec![sock(5000)]), "update replaces");

    s.apply(multiaddr_updated_log(id, Bytes::new()))
        .await
        .unwrap();
    assert_eq!(s.dial_addrs.get(&id), None, "an empty record clears");
    assert_eq!(in_lookup(&s), None);

    s.apply(multiaddr_updated_log(
        id,
        packed(&["/ip4/203.0.113.10/udp/4433/quic-v1"]),
    ))
    .await
    .unwrap();
    s.apply(deregistered_log(id)).await.unwrap();
    assert_eq!(s.dial_addrs.get(&id), None, "deregistration removes");
    assert_eq!(in_lookup(&s), None);
}

/// A resync swaps the dial-address directory like every other projection:
/// a node the snapshot no longer lists leaves, one it lists arrives, and a
/// node the tail changed above the snapshot block keeps its current entry.
#[tokio::test]
async fn resync_replaces_dial_addrs_and_keeps_tail_changes() {
    let (stale, _) = dialable(1);
    let (listed, _) = dialable(2);
    let (tail, _) = dialable(3);
    let mut reads = StubReads::new(Ok(None)).with_snapshot(Ok(RegistrySnapshot {
        dial_addrs: HashMap::from([(listed, vec![sock(2)])]),
        ..RegistrySnapshot::default()
    }));
    reads.snapshot_block = 800;
    let (mut s, _active, _bindings, _op, _regions, _m) = sink(reads, false);
    s.dial_addrs.set(stale, vec![sock(1)]);
    let mut log =
        registered_log_multiaddrs(tail, addr(3), packed(&["/ip4/203.0.113.10/udp/3/quic-v1"]));
    log.block_number = Some(900);
    s.apply(log).await.unwrap();
    s.last_resync = None;

    s.on_tick_complete().await.unwrap();

    assert_eq!(s.dial_addrs.get(&stale), None);
    assert_eq!(s.dial_addrs.get(&listed), Some(vec![sock(2)]));
    assert_eq!(
        s.dial_addrs.get(&tail),
        Some(vec![sock(3)]),
        "a tail change above the snapshot block survives the resync"
    );
}

// ── Periodic re-enumeration ─────────────────────────────────────────────
//
// The self-healing leg. Every other input to these projections is an event,
// and a missed or orphaned event never surfaces as an error — so without
// this, a drifted set is only repaired by restarting the process.

fn snapshot_of(ids: &[u8], addrs: &[u8]) -> RegistrySnapshot {
    RegistrySnapshot {
        active: ids.iter().map(|b| nid(*b)).collect(),
        bindings: addrs.iter().map(|b| (nid(*b), addr(*b))).collect(),
        operator_to_node: addrs.iter().map(|b| (addr(*b), nid(*b))).collect(),
        ..RegistrySnapshot::default()
    }
}

/// A due resync replaces both projections wholesale, so an entry the event
/// tail dropped comes back and one it wrongly added goes away.
#[tokio::test]
async fn resync_replaces_both_projections() {
    let reads = StubReads::new(Ok(None)).with_snapshot(Ok(snapshot_of(&[2, 3], &[2, 3])));
    let (mut sink, active, bindings, operator_to_node, _regions, _m) = sink(reads, true);
    // Seed a stale view: 1 is gone from chain, 2 is missing locally.
    with_write(&active, "t", |set| {
        set.insert(nid(1));
    });
    sink.last_resync = None; // force the resync on this tick

    sink.on_tick_complete().await.unwrap();

    let got = active.read().unwrap().clone();
    assert_eq!(
        got,
        snapshot_of(&[2, 3], &[]).active,
        "stale entry dropped, missing one restored"
    );
    let b = bindings.unwrap();
    assert_eq!(b.read().unwrap().len(), 2, "bindings are replaced too");
    assert_eq!(
        operator_to_node.read().unwrap().len(),
        2,
        "the reverse map is replaced too"
    );
}

/// A resync pinned below a tail change keeps it in every projection: a node
/// the tail registered above the snapshot block stays, one it deregistered
/// above the block stays gone, and a change at or below the block yields to
/// the authoritative snapshot.
#[tokio::test]
async fn resync_keeps_tail_changes_above_its_block() {
    let mut reads = StubReads::new(Ok(None)).with_snapshot(Ok(snapshot_of(&[2], &[2])));
    reads.snapshot_block = 800;
    let (mut sink, active, bindings, operator_to_node, _regions, _m) = sink(reads, true);
    let at = |mut log: Log, block: u64| {
        log.block_number = Some(block);
        log
    };
    sink.apply(at(registered_log(nid(2), addr(2)), 600))
        .await
        .unwrap();
    sink.apply(at(registered_log(nid(6), addr(6)), 700))
        .await
        .unwrap();
    sink.apply(at(registered_log(nid(5), addr(5)), 900))
        .await
        .unwrap();
    sink.apply(at(deregistered_log(nid(2)), 950)).await.unwrap();
    sink.last_resync = None;

    sink.on_tick_complete().await.unwrap();

    assert_eq!(*active.read().unwrap(), HashSet::from([nid(5)]));
    let bindings = bindings.unwrap();
    assert_eq!(
        *bindings.read().unwrap(),
        HashMap::from([(nid(5), addr(5))])
    );
    assert_eq!(
        *operator_to_node.read().unwrap(),
        HashMap::from([(addr(5), nid(5))])
    );
    assert_eq!(
        sink.tail_changes.keys().copied().collect::<HashSet<_>>(),
        HashSet::from([nid(2), nid(5)]),
        "changes at or below the pin are dropped"
    );
}

/// The boot enumeration pins its code check and every `getRegisteredNodes`
/// page to the snapshot block, and seeds the tail cursor at that same block.
#[tokio::test(flavor = "multi_thread")]
async fn the_boot_enumeration_pins_its_pages_and_cursor_to_the_snapshot_block() {
    use crate::chain_events::boot_retry::BootRetry;
    use crate::chain_events::shared_head::{SNAPSHOT_LAG_MARGIN_BLOCKS, SharedHead};
    use alloy::providers::ProviderBuilder;

    let tags = Arc::new(std::sync::Mutex::new(Vec::new()));
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .respond_with(RegistryRpc {
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

    let handles = bootstrap(
        provider,
        Address::repeat_byte(0x11),
        head,
        false,
        Arc::clone(&metrics),
        &BootRetry::single_attempt(Arc::clone(&metrics)),
    )
    .await
    .unwrap();

    let pinned = 1_000 - SNAPSHOT_LAG_MARGIN_BLOCKS;
    // The code check, then one empty `getRegisteredNodes` page.
    let tags = tags.lock().unwrap().clone();
    assert_eq!(tags, vec![serde_json::json!(format!("{pinned:#x}")); 2]);
    assert_eq!(handles.route.start.seed(), Some(pinned));
}

/// A JSON-RPC responder for the registry boot: head 1000, contract code at
/// every block, and an empty `getRegisteredNodes` page, recording the block
/// tag of every call but `eth_blockNumber`.
struct RegistryRpc {
    tags: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
}

impl wiremock::Respond for RegistryRpc {
    fn respond(&self, req: &wiremock::Request) -> wiremock::ResponseTemplate {
        use alloy::sol_types::SolCall;

        let body: serde_json::Value =
            serde_json::from_slice(&req.body).unwrap_or(serde_json::Value::Null);
        let id = body.get("id").cloned().unwrap_or(serde_json::Value::Null);
        let method = body.get("method").and_then(serde_json::Value::as_str);
        let result = if method == Some("eth_blockNumber") {
            serde_json::json!("0x3e8")
        } else {
            let tag = body
                .pointer("/params/1")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            self.tags.lock().unwrap().push(tag);
            if method == Some("eth_getCode") {
                serde_json::json!("0x60")
            } else {
                let empty: (Vec<CapacityBond::NodeInfo>, Vec<bool>) = (Vec::new(), Vec::new());
                serde_json::json!(Bytes::from(
                    CapacityBond::getRegisteredNodesCall::abi_encode_returns_tuple(&empty)
                ))
            }
        };
        wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "jsonrpc": "2.0", "id": id, "result": result,
        }))
    }
}

/// `full_snapshot` derives the reverse map as the inverse of `bindings` for
/// every registered operator.
#[tokio::test]
async fn resync_derives_reverse_map_as_inverse_of_bindings() {
    let reads = StubReads::new(Ok(None)).with_snapshot(Ok(snapshot_of(&[2, 3], &[2, 3])));
    let (mut sink, _active, _bindings, operator_to_node, _regions, _m) = sink(reads, true);
    sink.last_resync = None;

    sink.on_tick_complete().await.unwrap();

    let rev = operator_to_node.read().unwrap();
    assert_eq!(rev.get(&addr(2)).copied(), Some(nid(2)));
    assert_eq!(rev.get(&addr(3)).copied(), Some(nid(3)));
    assert_eq!(rev.len(), 2, "one entry per registered operator");
}

/// A failed read must leave the previous projections intact. Emptying the
/// staker set would make the node treat every peer as unstaked.
#[tokio::test]
async fn resync_failure_keeps_the_previous_projections() {
    let reads = StubReads::new(Ok(None)).with_snapshot(Err("rpc down"));
    let (mut sink, active, _bindings, _op, _regions, metrics) = sink(reads, true);
    with_write(&active, "t", |set| {
        set.insert(nid(1));
    });
    sink.last_resync = None;

    sink.on_tick_complete().await.unwrap();

    assert!(
        active.read().unwrap().contains(&nid(1)),
        "a failed resync must not clear the set"
    );
    // The reconcile reports success upward on purpose, so nothing else
    // moves: without this counter a repair that never lands is invisible.
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_capacity_bond_registry_resync_failures_total 1"),
        "a failed resync must surface on its counter:\n{text}"
    );
    // And the liveness gauge must stay unstamped. Stamping it here would
    // disable the staleness alert, which is the only thing that catches the
    // resync being skipped rather than failing.
    assert!(
        text.lines()
            .any(|l| l == "decdn_capacity_bond_registry_last_resync_timestamp_seconds 0"),
        "a failed resync must not stamp the liveness gauge:\n{text}"
    );
}

/// Shared in-memory sink for the captured tracing output.
#[derive(Clone, Default)]
struct CapturedLog(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The resync warning carries the whole error chain. The outermost
/// context names only the page; the RPC cause sits below it, and an
/// operator reading the log needs both.
#[tokio::test]
async fn resync_failure_logs_the_full_error_chain() {
    let reads = StubReads::new(Ok(None)).with_snapshot(Err("timed out after 10s"));
    let (mut sink, _active, _bindings, _op, _regions, _m) = sink(reads, true);
    sink.last_resync = None;

    let log = CapturedLog::default();
    let writer = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    sink.on_tick_complete().await.unwrap();

    let text = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
    let line = text
        .lines()
        .find(|l| l.contains("capacity-bond registry resync failed"))
        .unwrap_or_else(|| panic!("no resync warning logged:\n{text}"));
    assert!(line.contains("WARN"), "{line}");
    assert!(
        line.contains(&format!("getRegisteredNodes(offset=0, limit={PAGE_SIZE})")),
        "the warning must keep the page context: {line}"
    );
    assert!(
        line.contains("timed out after 10s"),
        "the warning must keep the RPC cause: {line}"
    );
}

/// A successful resync stamps the liveness gauge. That gauge is the only
/// signal that catches a resync being *skipped* rather than failing — the
/// reconcile does not run at all while the route is errored, which emits
/// nothing, not even the failure counter.
#[tokio::test]
async fn a_successful_resync_stamps_the_liveness_gauge() {
    let reads = StubReads::new(Ok(None)).with_snapshot(Ok(snapshot_of(&[9], &[9])));
    let (mut sink, _active, _bindings, _op, _regions, metrics) = sink(reads, true);

    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_capacity_bond_registry_last_resync_timestamp_seconds 0"),
        "the gauge must export at zero before the first resync, so an alert \
         can guard on `> 0`:\n{text}"
    );

    sink.last_resync = None;
    sink.on_tick_complete().await.unwrap();

    let text = metrics.encode().unwrap();
    let stamped = text.lines().any(|l| {
        l.strip_prefix("decdn_capacity_bond_registry_last_resync_timestamp_seconds ")
            .and_then(|v| v.parse::<i64>().ok())
            .is_some_and(|v| v > 0)
    });
    assert!(stamped, "a successful resync must stamp the gauge:\n{text}");
}

/// The watcher recovering from an errored tick must force a re-enumeration,
/// not wait out the cadence.
///
/// An outage is when the set is most likely to have drifted and when the
/// cadence helps least: the reconcile is skipped entirely while the route is
/// errored, so the repair has not been running, and the first cadence tick
/// to land afterwards has no relationship to when the watcher came back.
#[tokio::test]
async fn recovery_forces_a_resync_the_cadence_would_defer() {
    let reads = StubReads::new(Ok(None)).with_snapshot(Ok(snapshot_of(&[9], &[9])));
    let (mut sink, active, _bindings, _op, _regions, _m) = sink(reads, true);

    // Last resync a few minutes ago: past the recovery floor, far short of
    // the cadence. This is the state a route is in after an outage — the
    // cadence alone would defer for the rest of the interval.
    sink.last_resync = Instant::now().checked_sub(Duration::from_mins(5));
    sink.on_tick_complete().await.unwrap();
    assert!(
        active.read().unwrap().is_empty(),
        "the cadence must still gate a tick that is not due"
    );

    sink.on_recovered();
    assert!(
        sink.last_resync.is_none(),
        "recovery must clear the cadence clock"
    );
    sink.on_tick_complete().await.unwrap();

    assert!(
        is_active(&active, nid(9)),
        "the reconcile on the recovery tick must re-enumerate"
    );
}

/// The forced resync is floored, so a flapping endpoint cannot buy one full
/// enumeration per flap.
///
/// A recovery edge fires every time a route comes back from an errored tick.
/// An endpoint that flaps rather than staying down produces one on roughly
/// the backoff interval — seconds — and each unfloored one would aim a
/// paginated `getRegisteredNodes` at an endpoint that is already failing.
#[tokio::test]
async fn a_flapping_route_cannot_force_a_resync_per_recovery() {
    let reads = StubReads::new(Ok(None)).with_snapshot(Ok(snapshot_of(&[9], &[9])));
    let (mut sink, active, _bindings, _op, _regions, _m) = sink(reads, true);

    // A resync that just landed — the state after the previous flap's
    // forced re-enumeration.
    let just_now = Instant::now();
    sink.last_resync = Some(just_now);

    sink.on_recovered();
    assert_eq!(
        sink.last_resync,
        Some(just_now),
        "a resync younger than the floor must survive the recovery edge"
    );
    sink.on_tick_complete().await.unwrap();
    assert!(
        active.read().unwrap().is_empty(),
        "and the reconcile must still be gated, so no enumeration is issued"
    );
}

/// Not-yet-due ticks must not re-read: the watcher ticks every few seconds,
/// so an ungated resync would hammer the RPC with a paginated enumeration.
#[tokio::test]
async fn resync_is_cadence_gated() {
    let reads = StubReads::new(Ok(None)).with_snapshot(Ok(snapshot_of(&[9], &[9])));
    let (mut sink, active, _bindings, _op, _regions, _m) = sink(reads, true);
    // `sink()` stamps `last_resync` to now, so nothing is due yet.
    sink.on_tick_complete().await.unwrap();
    assert!(
        active.read().unwrap().is_empty(),
        "a resync ran despite not being due"
    );
}

/// The resync is what closes the `nodeIdOf` drop window. An operator-indexed
/// event whose follow-up read fails is dropped silently (no `Err`, no
/// backoff), so the counter is the only live signal — and the cadence-gated
/// re-enumeration is the only systematic repair short of a restart. A later
/// event for the same operator would also correct it, but nothing guarantees
/// one arrives. This pins both legs together, since either alone reads as
/// complete.
#[tokio::test]
async fn resync_repairs_a_set_drifted_by_a_dropped_node_id_of() {
    let (mut sink, active, _bindings, _op, _regions, metrics) =
        sink(StubReads::new(Err("rpc down")), true);

    // Leg 1: the reinstatement is lost, counted, and does not fail the tick.
    sink.apply(reinstated_log(addr(9))).await.unwrap();
    assert!(
        !is_active(&active, nid(9)),
        "a dropped nodeIdOf must leave the operator out of the cached set"
    );
    let text = metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_staker_set_watcher_resolve_failures_total 1"),
        "the drop must surface on its own counter:\n{text}"
    );

    // Leg 2: the RPC recovers and the next due resync reconciles against
    // chain truth, which has 9 active.
    sink.reads = StubReads::new(Ok(None)).with_snapshot(Ok(snapshot_of(&[9], &[9])));
    sink.last_resync = None;
    sink.on_tick_complete().await.unwrap();

    assert!(
        is_active(&active, nid(9)),
        "the resync must restore the membership change the event tail dropped"
    );
}

/// A failing read still stamps the clock, so retries follow the resync
/// cadence rather than the (seconds-scale) watcher tick.
#[tokio::test]
async fn failed_resync_still_stamps_the_clock() {
    let reads = StubReads::new(Ok(None)).with_snapshot(Err("rpc down"));
    let (mut sink, _active, _bindings, _op, _regions, _m) = sink(reads, true);
    sink.last_resync = None;

    sink.on_tick_complete().await.unwrap();

    assert!(
        sink.last_resync.is_some(),
        "a failed resync must still stamp, or it retries every tick"
    );
}
