use super::*;
use std::collections::HashMap;

const US: B256 = B256::repeat_byte(0x01);
const FR: B256 = B256::repeat_byte(0x02);

/// The blacklist route watches exactly the five hash + origin blacklist
/// events — no more, no fewer.
#[test]
fn route_topic0s_covers_hash_and_origin_events() {
    assert_eq!(
        blacklist_route_topic0s(),
        vec![
            HashBlacklisted::SIGNATURE_HASH,
            HashRemoved::SIGNATURE_HASH,
            OriginBlacklistUpdated::SIGNATURE_HASH,
            OperatorBlacklisted::SIGNATURE_HASH,
            OperatorBlacklistCleared::SIGNATURE_HASH,
        ]
    );
}

/// The blacklist route seeds its cursor at the enumeration snapshot block,
/// with no durable persistence (the deny-set is rebuilt from enumeration
/// each boot).
#[test]
fn cursor_start_seeds_at_snapshot_with_no_persistence() {
    let start = blacklist_cursor_start(99_999);
    assert_eq!(start.seed(), Some(99_999));
    assert!(
        matches!(start, CursorStart::Seeded { .. }),
        "must not carry a durable checkpoint"
    );
}

/// A provider that answers nothing. The pure-helper tests never reach an RPC
/// through `WatcherState`; erasing to `DynProvider` keeps the fixture's type
/// nameable so it unifies with the sink's own contract instance.
fn mock_provider() -> alloy::providers::DynProvider {
    alloy::providers::ProviderBuilder::new()
        .connect_mocked_client(alloy::providers::mock::Asserter::new())
        .erased()
}

fn state() -> WatcherState {
    state_with_denylist(Arc::new(ContentDenylist::empty()))
}

fn state_with_denylist(denylist: Arc<ContentDenylist>) -> WatcherState {
    WatcherState {
        known: HashSet::new(),
        denylist,
        origin_changes: HashMap::new(),
        removed_entries: HashMap::new(),
    }
}

// ----- enumeration helpers (the #1497 core) -----

fn hash(byte: u8) -> Hash {
    Hash::from_bytes([byte; 32])
}

fn b256(byte: u8) -> B256 {
    B256::repeat_byte(byte)
}

fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

/// Scripted [`BlacklistChainReads`]: no provider, no chain. The address union
/// and per-region hash sets are supplied directly, with optional count
/// overrides so a test can simulate a swap-and-pop `seen != count` skew.
struct StubReads {
    operator: Address,
    block: u64,
    /// RAW `blacklistedAddresses` membership.
    addresses: Vec<Address>,
    /// Override for `blacklistedAddressCount` (defaults to `addresses.len()`).
    address_count: Option<U256>,
    /// `isOriginBlacklisted == true` for these.
    origin_live: HashSet<Address>,
    /// `isOperatorBlacklisted == true` for these.
    operator_live: HashSet<Address>,
    /// `getScopeRegions(operator)`.
    scope_regions: Vec<B256>,
    /// RAW `blacklistedHashes` membership per region.
    hashes_by_region: HashMap<B256, Vec<B256>>,
    /// Override for a region's `blacklistedHashCount`.
    hash_count: HashMap<B256, U256>,
}

impl StubReads {
    fn new(operator: Address) -> Self {
        Self {
            operator,
            block: 42,
            addresses: Vec::new(),
            address_count: None,
            origin_live: HashSet::new(),
            operator_live: HashSet::new(),
            scope_regions: Vec::new(),
            hashes_by_region: HashMap::new(),
            hash_count: HashMap::new(),
        }
    }
}

impl BlacklistChainReads for StubReads {
    async fn snapshot_block(&self) -> Result<u64> {
        Ok(self.block)
    }

    async fn scope_regions(&self, operator: Address, at: u64) -> Result<Vec<B256>> {
        assert_eq!(operator, self.operator, "unexpected operator");
        assert_eq!(at, self.block, "reads must be pinned to the snapshot block");
        Ok(self.scope_regions.clone())
    }

    async fn blacklisted_hash_count(&self, region: B256, at: u64) -> Result<U256> {
        assert_eq!(at, self.block);
        Ok(self
            .hash_count
            .get(&region)
            .copied()
            .unwrap_or_else(|| U256::from(self.hashes_by_region.get(&region).map_or(0, Vec::len))))
    }

    async fn blacklisted_hashes(
        &self,
        region: B256,
        offset: U256,
        limit: U256,
        at: u64,
    ) -> Result<Vec<B256>> {
        assert_eq!(at, self.block);
        Ok(page(
            self.hashes_by_region
                .get(&region)
                .map_or(&[], Vec::as_slice),
            offset,
            limit,
        ))
    }

    async fn blacklisted_address_count(&self, at: u64) -> Result<U256> {
        assert_eq!(at, self.block);
        Ok(self
            .address_count
            .unwrap_or_else(|| U256::from(self.addresses.len())))
    }

    async fn blacklisted_addresses(
        &self,
        offset: U256,
        limit: U256,
        at: u64,
    ) -> Result<Vec<Address>> {
        assert_eq!(at, self.block);
        Ok(page(&self.addresses, offset, limit))
    }

    async fn is_origin_blacklisted(&self, addr: Address, at: u64) -> Result<bool> {
        assert_eq!(at, self.block);
        Ok(self.origin_live.contains(&addr))
    }

    async fn is_operator_blacklisted(&self, addr: Address, at: u64) -> Result<bool> {
        assert_eq!(at, self.block);
        Ok(self.operator_live.contains(&addr))
    }
}

/// Slice out one `[offset, offset+limit)` page, saturating at the end.
fn page<T: Clone>(all: &[T], offset: U256, limit: U256) -> Vec<T> {
    let start: usize = offset.saturating_to();
    let len: usize = limit.saturating_to();
    all.iter().skip(start).take(len).cloned().collect()
}

/// (a) The address union is built from `blacklistedAddresses` and
/// liveness-filtered by the UNION predicate: an origin-only live address and an
/// operator-only live address both survive; a lapsed emergency origin (in
/// neither mapping) is dropped.
#[tokio::test]
async fn address_union_is_liveness_filtered_by_the_union_predicate() -> Result<()> {
    let op = addr(0xA0);
    let origin_only = addr(0xA1);
    let operator_only = addr(0xA2);
    let lapsed = addr(0xA3);
    let mut stub = StubReads::new(op);
    stub.addresses = vec![origin_only, operator_only, lapsed];
    stub.origin_live = [origin_only].into_iter().collect();
    stub.operator_live = [operator_only].into_iter().collect();

    let union = enumerate_address_union(&stub, op, stub.block).await?;

    assert!(union.contains(&origin_only), "a live origin survives");
    assert!(union.contains(&operator_only), "a live operator survives");
    assert!(
        !union.contains(&lapsed),
        "an address in neither mapping (lapsed emergency origin) is dropped"
    );
    assert_eq!(union.len(), 2);
    Ok(())
}

/// (b) The load-bearing #1499 regression guard, ported from
/// `operator_blacklist_log_reaches_the_same_deny_set` to the enumeration path:
/// an OPERATOR-only entry (`isOperatorBlacklisted == true`,
/// `isOriginBlacklisted == false`) STILL lands in the deny-set. Filtering by
/// `isOriginBlacklisted` alone would drop it — a restarted node serving a
/// governance-ejected operator.
#[tokio::test]
async fn operator_only_entry_survives_the_enumeration_liveness_filter() -> Result<()> {
    let op = addr(0xB0);
    let operator_only = addr(0xB1);
    let mut stub = StubReads::new(op);
    stub.addresses = vec![operator_only];
    // The whole point: NOT in the origin mapping.
    stub.origin_live = HashSet::new();
    stub.operator_live = [operator_only].into_iter().collect();

    // Precondition making the guard's teeth explicit: the origin predicate alone
    // returns false, so an `isOriginBlacklisted`-only filter would drop this.
    assert!(
        !stub
            .is_origin_blacklisted(operator_only, stub.block)
            .await?
    );
    assert!(
        stub.is_operator_blacklisted(operator_only, stub.block)
            .await?
    );

    let union = enumerate_address_union(&stub, op, stub.block).await?;

    assert!(
        union.contains(&operator_only),
        "the operator-only address MUST survive the union filter (#1499)"
    );
    Ok(())
}

/// (c) A `seen != count` mismatch ABORTS (`ensure!`) rather than seating a
/// partial set — the swap-and-pop / pinned-block guard.
#[tokio::test]
async fn address_count_mismatch_aborts_rather_than_seating_a_partial_set() {
    let op = addr(0xC0);
    let mut stub = StubReads::new(op);
    stub.addresses = vec![addr(0xC1)];
    stub.origin_live = [addr(0xC1)].into_iter().collect();
    // Count claims two, only one is enumerable → a concurrent swap-and-pop skew.
    stub.address_count = Some(U256::from(2u8));

    let err = enumerate_address_union(&stub, op, stub.block)
        .await
        .expect_err("a seen != count skew must abort");
    assert!(
        format!("{err:#}").contains("read 1 of 2"),
        "abort must name the shortfall: {err:#}"
    );
}

/// The hash half has the same pinned-block count guard.
#[tokio::test]
async fn hash_count_mismatch_aborts() {
    let op = addr(0xD0);
    let mut stub = StubReads::new(op);
    stub.hashes_by_region = [(US, vec![b256(0xEE)])].into_iter().collect();
    stub.hash_count = [(US, U256::from(3u8))].into_iter().collect();

    let err = enumerate_region_hashes(&stub, US, stub.block)
        .await
        .expect_err("a seen != count skew must abort");
    assert!(format!("{err:#}").contains("read 1 of 3"), "{err:#}");
}

/// `ContractReads::snapshot_block` must route through the shared, TTL-cached
/// [`SharedHead`] single-flight rather than issue its own `eth_blockNumber` —
/// two calls inside the TTL cost exactly one head RPC — and pin the lag
/// margin below that head. The unconsumed asserter
/// queue is the proof: a second direct read would have popped a response
/// that was never pushed.
#[tokio::test]
async fn contract_reads_snapshot_block_routes_through_shared_head() -> Result<()> {
    use crate::chain_events::shared_head::{SNAPSHOT_LAG_MARGIN_BLOCKS, SharedHead};
    use alloy::primitives::U64;
    use alloy::providers::ProviderBuilder;
    use alloy::providers::mock::Asserter;

    const TTL: Duration = Duration::from_secs(4);

    let asserter = Asserter::new();
    asserter.push_success(&U64::from(1_000));
    // One code-presence `eth_getCode` per call.
    asserter.push_success(&alloy::primitives::Bytes::from_static(&[0x60]));
    asserter.push_success(&alloy::primitives::Bytes::from_static(&[0x60]));
    let provider = ProviderBuilder::new().connect_mocked_client(asserter.clone());
    let head: Arc<dyn HeadSource> = Arc::new(SharedHead::with_ttl(provider.clone(), TTL, None));
    let pinned = 1_000 - SNAPSHOT_LAG_MARGIN_BLOCKS;

    let contract = ContentBlacklist::new(Address::ZERO, provider);
    let reads = ContractReads { contract, head };

    assert_eq!(reads.snapshot_block().await?, pinned);
    assert_eq!(
        reads.snapshot_block().await?,
        pinned,
        "second call is TTL-cached via SharedHead"
    );
    assert_eq!(
        asserter.read_q().len(),
        0,
        "exactly one eth_blockNumber RPC was issued"
    );
    Ok(())
}

/// Boot enumeration builds the full `(region, hash)` deny-set from every
/// in-scope region — the enumeration analogue of "a fresh process rebuilds the
/// full deny-set".
#[tokio::test]
async fn boot_enumeration_builds_the_known_set_across_in_scope_regions() -> Result<()> {
    let op = addr(0xF0);
    let global = b256(0x00);
    let mut stub = StubReads::new(op);
    stub.scope_regions = vec![global, US];
    stub.hashes_by_region = [
        (global, vec![b256(0x11)]),
        (US, vec![b256(0x22), b256(0x33)]),
    ]
    .into_iter()
    .collect();

    let snapshot = bootstrap_snapshot(&stub, op).await?;

    assert_eq!(snapshot.block, stub.block);
    assert_eq!(snapshot.known.len(), 3);
    assert!(snapshot.known.contains(&(global, hash(0x11))));
    assert!(snapshot.known.contains(&(US, hash(0x22))));
    assert!(snapshot.known.contains(&(US, hash(0x33))));
    Ok(())
}

/// A re-enumeration pinned below a tail change keeps it: an origin the tail
/// blacklisted above the snapshot block stays denied, and one it cleared
/// above the block stays clear, whatever the lagged snapshot says.
#[test]
fn a_re_enumeration_keeps_origin_changes_above_its_block() {
    let denylist = Arc::new(ContentDenylist::empty());
    let mut state = state_with_denylist(Arc::clone(&denylist));
    let (late_add, late_clear, early) = (addr(0xA1), addr(0xA2), addr(0xA3));
    state.set_origin(late_add, true, 900);
    state.set_origin(late_clear, false, 901);
    state.set_origin(early, true, 700);

    // The snapshot at 800 predates the two late changes, and its read of
    // `early` (changed at 700, below the pin) is authoritative.
    state.fold_reenumeration(BootstrapSnapshot {
        block: 800,
        origins: HashSet::from([late_clear]),
        known: HashSet::new(),
    });

    assert!(denylist.is_origin_denied(&late_add));
    assert!(!denylist.is_origin_denied(&late_clear));
    assert!(!denylist.is_origin_denied(&early));
    assert_eq!(
        state.origin_changes.len(),
        2,
        "changes at or below the pin are dropped"
    );
}

/// A re-enumeration pinned below a `HashRemoved` does not re-add the removed
/// entry, while an entry removed at or below the pin comes back from the
/// authoritative snapshot.
#[test]
fn a_re_enumeration_does_not_re_add_an_entry_removed_above_its_block() {
    let mut state = state();
    let (late, early) = (hash(0x51), hash(0x52));
    state.add_entry(US, late);
    state.add_entry(US, early);
    state.remove_entry(US, late, 900);
    state.remove_entry(US, early, 700);

    state.fold_reenumeration(BootstrapSnapshot {
        block: 800,
        origins: HashSet::new(),
        known: HashSet::from([(US, late), (US, early)]),
    });

    assert!(!state.known.contains(&(US, late)));
    assert!(state.known.contains(&(US, early)));
}

/// Paging reads more than one page and stops when it has `count` entries.
#[tokio::test]
async fn enumeration_pages_past_the_page_size() -> Result<()> {
    let op = addr(0x30);
    let mut stub = StubReads::new(op);
    // One-and-a-bit pages of DISTINCT addresses (a two-byte counter, so no
    // truncation and no repeats to dedup).
    let total = BLACKLIST_ENUM_PAGE_SIZE + 5;
    let addrs: Vec<Address> = (0..total)
        .map(|i| {
            let mut bytes = [0u8; 20];
            bytes[0..8].copy_from_slice(&i.to_be_bytes());
            Address::from(bytes)
        })
        .collect();
    stub.addresses = addrs.clone();
    stub.origin_live = addrs.iter().copied().collect();

    let union = enumerate_address_union(&stub, op, stub.block).await?;
    assert_eq!(union.len(), addrs.len(), "every distinct address survives");
    Ok(())
}

// ----- periodic re-enumeration fold (Ok arm of `on_tick_complete`) -----

/// The correctness-critical asymmetry of the re-enumeration fold: `known` is
/// UNIONED (an out-of-scope entry the tail learned and retained for a future
/// ripening transition must survive an in-scope-only re-enumeration), while the
/// chain origins are REPLACED WHOLESALE (the authoritative current set). A live
/// pre-existing origin X must NOT survive a snapshot that no longer lists it.
#[test]
fn reenumeration_unions_known_and_replaces_origins() {
    let x = addr(0x11);
    let y = addr(0x22);
    let denylist = Arc::new(ContentDenylist::empty());
    // Seed the prior chain-origin set with X, as a previous enumeration would.
    denylist.set_chain_origins([x].into_iter().collect());
    let mut state = state_with_denylist(Arc::clone(&denylist));
    // An out-of-scope `(region, hash)` the tail retained for a future ripening.
    let retained = (b256(0xEE), hash(0xEE));
    state.known.insert(retained);

    let snapshot = BootstrapSnapshot {
        block: 100,
        origins: [y].into_iter().collect(),
        known: [(US, hash(0x33))].into_iter().collect(),
    };
    state.fold_reenumeration(snapshot);

    // `known` is UNIONed: the retained out-of-scope entry survives alongside the
    // freshly enumerated in-scope one.
    assert!(
        state.known.contains(&retained),
        "the retained out-of-scope entry MUST survive the union"
    );
    assert!(
        state.known.contains(&(US, hash(0x33))),
        "the newly enumerated in-scope entry is added"
    );
    assert_eq!(state.known.len(), 2);

    // Chain origins are REPLACED wholesale: Y is now denied, X is gone.
    assert!(denylist.is_origin_denied(&y), "the new origin Y is denied");
    assert!(
        !denylist.is_origin_denied(&x),
        "the prior origin X was replaced wholesale, not unioned"
    );
}

// ----- in-memory worklist behaviour -----

/// Removing one region's entry must not drop a surviving same-hash entry in
/// another region — otherwise a later `updateRegion` into the surviving region
/// (which emits no blacklist event) would never lead to eviction.
#[test]
fn remove_entry_is_region_scoped() {
    let h = hash(0xAB);
    let mut state = state();
    state.add_entry(US, h);
    state.add_entry(FR, h);

    state.remove_entry(FR, h, 1);

    assert!(!state.known.contains(&(FR, h)));
    assert!(state.known.contains(&(US, h)), "US entry must survive");
    assert_eq!(state.distinct_hashes(), vec![h]);
}

#[test]
fn remove_entry_drops_last_entry_for_hash() {
    let h = hash(0xCD);
    let mut state = state();
    state.add_entry(US, h);

    state.remove_entry(US, h, 1);

    assert!(state.known.is_empty());
    assert!(state.distinct_hashes().is_empty());
}

/// Local eviction is sticky and region-independent, so `drop_hash` clears every
/// regional entry for the hash while leaving other hashes untouched.
#[test]
fn drop_hash_clears_all_regions_for_that_hash_only() {
    let evicted = hash(0xEE);
    let retained = hash(0x11);
    let mut state = state();
    state.add_entry(US, evicted);
    state.add_entry(FR, evicted);
    state.add_entry(FR, retained);

    state.drop_hash(evicted);

    assert!(!state.known.contains(&(US, evicted)));
    assert!(!state.known.contains(&(FR, evicted)));
    assert_eq!(state.distinct_hashes(), vec![retained]);
}

/// The scope view is per `(operator, hash)`, so re-scoping must issue one check
/// per distinct hash even when several regional entries share it.
#[test]
fn distinct_hashes_dedupes_across_regions() {
    let h = hash(0x42);
    let mut state = state();
    state.add_entry(US, h);
    state.add_entry(FR, h);

    assert_eq!(state.distinct_hashes(), vec![h]);
}

// ----- enforcement over a mocked provider -----

/// One ABI-encoded `bool` return word, as an `eth_call` result.
fn abi_bool(value: bool) -> alloy::primitives::Bytes {
    let mut word = [0u8; 32];
    if value && let Some(last) = word.last_mut() {
        *last = 1;
    }
    alloy::primitives::Bytes::from(word.to_vec())
}

/// What one boot-bootstrap run leaves behind, for the fail-closed assertions.
struct BootRun {
    result: Result<Route>,
    gate: InitialSyncResult,
    metrics: Arc<Metrics>,
    cache: CacheEngine,
    _tmp: tempfile::TempDir,
}

/// Run [`bootstrap`] against a mocked contract whose `eth_call`s answer from
/// `asserter` in order. The head read answers once; its long TTL serves every
/// retry from cache, so the queue holds only contract reads.
async fn run_boot(
    asserter: &alloy::providers::mock::Asserter,
    boot: impl FnOnce(Arc<Metrics>) -> crate::chain_events::boot_retry::BootRetry,
) -> Result<BootRun> {
    run_boot_with(asserter, boot, |_| Ok(())).await
}

/// [`run_boot`], with `prepare` run on the cache directory after the cache
/// opens — the seam a test uses to make the disk fail.
async fn run_boot_with(
    asserter: &alloy::providers::mock::Asserter,
    boot: impl FnOnce(Arc<Metrics>) -> crate::chain_events::boot_retry::BootRetry,
    prepare: impl FnOnce(&std::path::Path) -> std::io::Result<()>,
) -> Result<BootRun> {
    use crate::chain_events::shared_head::SharedHead;
    use alloy::providers::mock::Asserter;

    let head_asserter = Asserter::new();
    head_asserter.push_success(&alloy::primitives::U64::from(100));
    let head: Arc<dyn HeadSource> = Arc::new(SharedHead::with_ttl(
        alloy::providers::ProviderBuilder::new()
            .connect_mocked_client(head_asserter)
            .erased(),
        Duration::from_hours(1),
        None,
    ));
    let provider = alloy::providers::ProviderBuilder::new()
        .connect_mocked_client(asserter.clone())
        .erased();
    let tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(tmp.path(), Vec::new(), 1).await?;
    prepare(tmp.path())?;
    let metrics = Arc::new(Metrics::new());
    let (ready_tx, ready_rx) = oneshot::channel();
    let result = bootstrap(
        provider,
        Address::repeat_byte(0x11),
        Address::repeat_byte(0x22),
        cache.clone(),
        Arc::new(crate::warming_allowance::WarmingAllowance::new(1000, 0)),
        head,
        Duration::from_mins(10),
        ready_tx,
        &metrics,
        Arc::new(ContentDenylist::empty()),
        ChainFreshness::new(Duration::from_mins(30)),
        &boot(Arc::clone(&metrics)),
    )
    .await;
    Ok(BootRun {
        result,
        gate: ready_rx.await?,
        metrics,
        cache,
        _tmp: tmp,
    })
}

fn rpc_error(code: i64, message: &str) -> alloy_json_rpc::ErrorPayload {
    serde_json::from_value(serde_json::json!({ "code": code, "message": message })).unwrap()
}

/// Queue the reads of a clean enumeration: the snapshot block's
/// code-presence `eth_getCode`, no blacklisted addresses, and `hashes` under the one
/// in-scope region `US`.
fn push_enumeration(asserter: &alloy::providers::mock::Asserter, hashes: &[B256]) {
    use alloy::sol_types::SolValue;
    asserter.push_success(&alloy::primitives::Bytes::from_static(&[0x60]));
    asserter.push_success(&alloy::primitives::Bytes::from(U256::ZERO.abi_encode()));
    if hashes.is_empty() {
        asserter.push_success(&alloy::primitives::Bytes::from(
            Vec::<B256>::new().abi_encode(),
        ));
        return;
    }
    asserter.push_success(&alloy::primitives::Bytes::from(vec![US].abi_encode()));
    asserter.push_success(&alloy::primitives::Bytes::from(
        U256::from(hashes.len()).abi_encode(),
    ));
    asserter.push_success(&alloy::primitives::Bytes::from(
        hashes.to_vec().abi_encode(),
    ));
}

fn counter(metrics: &Metrics, name: &str) -> u64 {
    let text = metrics.encode().unwrap();
    text.lines()
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' '))
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("{name} not exported:\n{text}"))
}

/// A transient provider error during the boot enumeration is retried in
/// process, and the readiness gate opens on the clean retry.
#[tokio::test(start_paused = true)]
async fn a_transient_boot_enumeration_error_is_retried_not_fatal() -> Result<()> {
    use crate::chain_events::boot_retry::{BOOT_CHAIN_RETRY_BUDGET, BootRetry};

    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_failure(rpc_error(
        1,
        "no available upstreams to process the request",
    ));
    push_enumeration(&asserter, &[]);

    let run = run_boot(&asserter, |m| BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, m)).await?;

    run.result?;
    assert_eq!(run.gate, Ok(()), "the gate opens on the clean retry");
    assert!(asserter.read_q().is_empty(), "both attempts ran");
    assert_eq!(
        counter(&run.metrics, "decdn_chain_boot_read_retries_total"),
        1
    );
    Ok(())
}

/// A deterministic enumeration fault fails boot at once and keeps the gate
/// shut. The zero retry count is the proof: an exhausted mock queue answers
/// with a transient error, so a misclassified fault would retry.
#[tokio::test(start_paused = true)]
async fn a_permanent_boot_enumeration_error_keeps_the_gate_shut() -> Result<()> {
    use crate::chain_events::boot_retry::{BOOT_CHAIN_RETRY_BUDGET, BootRetry};

    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_failure(rpc_error(-32601, "method not found"));
    let start = tokio::time::Instant::now();

    let run = run_boot(&asserter, |m| BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, m)).await?;

    let err = run.result.expect_err("boot fails");
    assert!(format!("{err:#}").contains("not retried"), "{err:#}");
    let gate = run.gate.expect_err("the gate stays shut");
    assert!(
        gate.starts_with("initial ContentBlacklist sync failed"),
        "{gate}"
    );
    assert_eq!(
        counter(&run.metrics, "decdn_chain_boot_read_retries_total"),
        0
    );
    assert_eq!(start.elapsed(), Duration::ZERO);
    Ok(())
}

/// An enforcement pass that cannot re-check every hash is retried whole, and
/// the gate opens only once the retry denies and evicts the hash.
#[tokio::test(start_paused = true)]
async fn an_unclean_boot_enforcement_is_retried_before_the_gate_opens() -> Result<()> {
    use crate::chain_events::boot_retry::{BOOT_CHAIN_RETRY_BUDGET, BootRetry};

    let h = b256(0x42);
    let asserter = alloy::providers::mock::Asserter::new();
    push_enumeration(&asserter, &[h]);
    asserter.push_failure(rpc_error(19, "Temporary internal error. Please retry"));
    push_enumeration(&asserter, &[h]);
    asserter.push_success(&abi_bool(true));

    let run = run_boot(&asserter, |m| BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, m)).await?;

    run.result?;
    assert_eq!(run.gate, Ok(()));
    assert!(asserter.read_q().is_empty(), "both attempts ran");
    let hash = Hash::from_bytes(h.0);
    assert!(run.cache.is_chain_denied(hash));
    assert!(run.cache.is_evicted(hash));
    assert_eq!(
        counter(&run.metrics, "decdn_chain_boot_read_retries_total"),
        1
    );
    assert_eq!(
        counter(&run.metrics, "decdn_blacklist_enforcement_failures_total"),
        1
    );
    Ok(())
}

/// A local eviction error at boot is a disk fault a retry does not repair:
/// boot fails at once, the gate stays shut, and the deny still stops serving.
#[tokio::test(start_paused = true)]
async fn a_boot_eviction_error_fails_boot_at_once() -> Result<()> {
    use crate::chain_events::boot_retry::{BOOT_CHAIN_RETRY_BUDGET, BootFault, BootRetry};

    let h = b256(0x43);
    let asserter = alloy::providers::mock::Asserter::new();
    push_enumeration(&asserter, &[h]);
    asserter.push_success(&abi_bool(true));
    let start = tokio::time::Instant::now();

    // A directory where the eviction log goes makes every eviction fail.
    let run = run_boot_with(
        &asserter,
        |m| BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, m),
        |dir| std::fs::create_dir(dir.join("evicted.log")),
    )
    .await?;

    let err = run.result.expect_err("boot fails");
    assert!(
        err.chain().any(<dyn std::error::Error>::is::<BootFault>),
        "{err:#}"
    );
    assert!(run.gate.is_err(), "the gate stays shut");
    assert_eq!(
        counter(&run.metrics, "decdn_chain_boot_read_retries_total"),
        0
    );
    assert_eq!(start.elapsed(), Duration::ZERO);
    let hash = Hash::from_bytes(h.0);
    assert!(
        run.cache.is_chain_denied(hash),
        "deny lands before the eviction"
    );
    assert!(!run.cache.is_evicted(hash));
    Ok(())
}

/// A revert on the scope read is deterministic: boot fails at once.
#[tokio::test(start_paused = true)]
async fn a_reverting_boot_scope_read_fails_boot_at_once() -> Result<()> {
    use crate::chain_events::boot_retry::{BOOT_CHAIN_RETRY_BUDGET, BootRetry};

    let asserter = alloy::providers::mock::Asserter::new();
    push_enumeration(&asserter, &[b256(0x44)]);
    asserter.push_failure(rpc_error(3, "execution reverted"));

    let run = run_boot(&asserter, |m| BootRetry::new(BOOT_CHAIN_RETRY_BUDGET, m)).await?;

    run.result.expect_err("boot fails");
    assert!(run.gate.is_err(), "the gate stays shut");
    assert_eq!(
        counter(&run.metrics, "decdn_chain_boot_read_retries_total"),
        0
    );
    Ok(())
}

/// The boot pass stops at its first failed scope read: the attempt is lost,
/// so the other hashes' reads are not sent. Every scope read here fails (the
/// exhausted mock queue answers with an error), so a pass that went on would
/// count one failure per hash.
#[tokio::test(start_paused = true)]
async fn the_boot_pass_stops_at_the_first_failed_scope_read() -> Result<()> {
    use crate::chain_events::boot_retry::BootRetry;

    let asserter = alloy::providers::mock::Asserter::new();
    push_enumeration(&asserter, &[b256(0x45), b256(0x46), b256(0x47)]);

    let run = run_boot(&asserter, BootRetry::single_attempt).await?;

    run.result.expect_err("boot fails");
    assert_eq!(
        counter(&run.metrics, "decdn_blacklist_enforcement_failures_total"),
        1
    );
    Ok(())
}

/// An exhausted budget fails boot and keeps the gate shut.
#[tokio::test(start_paused = true)]
async fn an_exhausted_boot_budget_keeps_the_gate_shut() -> Result<()> {
    use crate::chain_events::boot_retry::BootRetry;

    let asserter = alloy::providers::mock::Asserter::new();
    asserter.push_failure(rpc_error(19, "Temporary internal error. Please retry"));

    let run = run_boot(&asserter, BootRetry::single_attempt).await?;

    let err = run.result.expect_err("boot fails");
    assert!(
        format!("{err:#}").contains("gave up after 1 attempts"),
        "{err:#}"
    );
    let gate = run.gate.expect_err("the gate stays shut");
    assert!(gate.contains("gave up after 1 attempts"), "{gate}");
    Ok(())
}

/// A sink whose `isHashBlacklistedForOperator` calls answer `scope_results` in
/// order, so a test can drive the enforcing path. Its `reads` provider is a
/// separate empty mock (the enforcement tests call `recheck`/`on_removed_log`
/// directly and never touch the enumeration path).
async fn enforcing_sink(
    scope_results: &[bool],
    metrics: &Arc<Metrics>,
) -> Result<BlacklistSink<alloy::providers::DynProvider>> {
    let asserter = alloy::providers::mock::Asserter::new();
    for in_scope in scope_results {
        asserter.push_success(&abi_bool(*in_scope));
    }
    let provider = alloy::providers::ProviderBuilder::new()
        .connect_mocked_client(asserter)
        .erased();
    let tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(tmp.path(), Vec::new(), 1).await?;
    Ok(BlacklistSink {
        contract: ContentBlacklist::new(Address::repeat_byte(0x11), provider),
        reads: ContractReads {
            contract: ContentBlacklist::new(Address::repeat_byte(0x11), mock_provider()),
            head: Arc::new(crate::chain_events::shared_head::SharedHead::with_ttl(
                mock_provider(),
                Duration::from_secs(1),
                None,
            )),
        },
        operator: Address::repeat_byte(0x22),
        cache,
        warming: Arc::new(crate::warming_allowance::WarmingAllowance::new(1000, 0)),
        state: state(),
        shutdown: CancellationToken::new(),
        rescan_interval: Duration::from_secs(1),
        last_rescan: None,
        metrics: Arc::clone(metrics),
    })
}

/// A sink with `entries` distinct known hashes over an empty asserter, so every
/// enumeration read AND every re-scope re-check fails (`Recheck::Failed`).
async fn failing_sink(
    entries: u8,
    metrics: &Arc<Metrics>,
) -> Result<BlacklistSink<alloy::providers::DynProvider>> {
    let provider = alloy::providers::ProviderBuilder::new()
        .connect_mocked_client(alloy::providers::mock::Asserter::new())
        .erased();
    let tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(tmp.path(), Vec::new(), 1).await?;
    let mut state = state();
    for n in 0..entries {
        state.add_entry(US, hash(n));
    }
    Ok(BlacklistSink {
        contract: ContentBlacklist::new(Address::repeat_byte(0x11), provider),
        reads: ContractReads {
            contract: ContentBlacklist::new(Address::repeat_byte(0x11), mock_provider()),
            head: Arc::new(crate::chain_events::shared_head::SharedHead::with_ttl(
                mock_provider(),
                Duration::from_secs(1),
                None,
            )),
        },
        operator: Address::repeat_byte(0x22),
        cache,
        warming: Arc::new(crate::warming_allowance::WarmingAllowance::new(1000, 0)),
        state,
        shutdown: CancellationToken::new(),
        rescan_interval: Duration::from_secs(1),
        last_rescan: None,
        metrics: Arc::clone(metrics),
    })
}

/// Enforcement is TWO writes: the deny (which selects the wire refusal code) and
/// the eviction (which reclaims the bytes). Both must land.
#[tokio::test]
async fn enforcement_denies_and_evicts() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let mut sink = enforcing_sink(&[true], &metrics).await?;
    let h = hash(0x51);
    sink.state.add_entry(US, h);

    let outcome = recheck(
        &sink.contract,
        sink.operator,
        &sink.cache,
        &sink.warming,
        &mut sink.state,
        h,
    )
    .await;

    assert!(matches!(outcome, Recheck::Evicted), "{outcome:?}");
    assert!(
        sink.cache.is_chain_denied(h),
        "the live deny-set the delivery path reads must carry the reason"
    );
    assert!(
        sink.cache.is_evicted(h),
        "and the bytes still get reclaimed"
    );
    Ok(())
}

/// A governance takedown must forget the evicted hash's ADR 041 warming tag
/// (issue #1751 review), exactly like the eviction driver's own sweep does —
/// otherwise a re-admitted hash could spuriously credit a stale source's
/// allowance. Proven observably: a serve credit after the takedown must be a
/// no-op (the source stays exactly as drained as the speculative buy left
/// it), since `credit_serve` is a no-op once the hash has no known source.
#[tokio::test]
async fn takedown_forgets_the_warming_tag() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let mut sink = enforcing_sink(&[true], &metrics).await?;
    let h = hash(0x57);
    let source = crate::warming_allowance::SourceId::from_bytes([9u8; 32]);
    sink.state.add_entry(US, h);

    // Tag the hash as speculatively bought from `source`, spending the
    // fixture's whole 1000-unit budget.
    sink.warming.debit_speculative(source, h, 1000);
    assert!(
        !sink.warming.available(source),
        "the speculative buy must drain the source"
    );

    let outcome = recheck(
        &sink.contract,
        sink.operator,
        &sink.cache,
        &sink.warming,
        &mut sink.state,
        h,
    )
    .await;
    assert!(matches!(outcome, Recheck::Evicted), "{outcome:?}");

    // If the tag survived the takedown, this credit would refill `source`.
    // With the tag forgotten, `credit_serve` is a documented no-op.
    sink.warming.credit_serve(h, 600);
    assert!(
        !sink.warming.available(source),
        "a credit against a forgotten tag must not resurrect the source's allowance"
    );
    Ok(())
}

/// The upgrade / restart path. `evicted.log` records that a hash was evicted,
/// never *why*, so a hash evicted by an older build (or a prior boot) would
/// answer `EvictedSinceProbe` forever. The enumeration re-check back-fills the
/// governance reason.
#[tokio::test]
async fn an_already_evicted_hash_is_back_filled_into_the_deny_set() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let mut sink = enforcing_sink(&[true], &metrics).await?;
    let h = hash(0x53);
    sink.cache.evict(h).await?;
    sink.state.add_entry(US, h);

    let outcome = recheck(
        &sink.contract,
        sink.operator,
        &sink.cache,
        &sink.warming,
        &mut sink.state,
        h,
    )
    .await;

    assert!(
        matches!(outcome, Recheck::NoAction),
        "already evicted, nothing to evict"
    );
    assert!(
        sink.cache.is_chain_denied(h),
        "but the reason must still be recorded, or the wire code stays wrong"
    );
    Ok(())
}

/// ...and it costs nothing once recorded: a second pass must not re-spend a
/// scope read on a hash already known to be governance-denied. (The sink is
/// built with ONE queued response, so a second `eth_call` would error.)
#[tokio::test]
async fn back_fill_does_not_repeat_once_recorded() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let mut sink = enforcing_sink(&[true], &metrics).await?;
    let h = hash(0x54);
    sink.cache.evict(h).await?;

    for _ in 0..2 {
        sink.state.add_entry(US, h);
        let outcome = recheck(
            &sink.contract,
            sink.operator,
            &sink.cache,
            &sink.warming,
            &mut sink.state,
            h,
        )
        .await;
        assert!(matches!(outcome, Recheck::NoAction), "{outcome:?}");
    }
    Ok(())
}

/// A `HashRemoved` lifts the governance deny — but only on a definitive
/// out-of-scope read, since the event is per-region and a same-hash entry under
/// another region can still cover this operator. The hash stays evicted either
/// way; only the refusal *code* moves.
#[tokio::test]
async fn hash_removal_lifts_the_deny_when_out_of_scope() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    // Two scope reads: one to enforce, one on the removal.
    let mut sink = enforcing_sink(&[true, false], &metrics).await?;
    let h = hash(0x55);
    sink.state.add_entry(US, h);
    let _ = recheck(
        &sink.contract,
        sink.operator,
        &sink.cache,
        &sink.warming,
        &mut sink.state,
        h,
    )
    .await;
    assert!(sink.cache.is_chain_denied(h), "denied before the removal");

    on_removed_log(
        &sink.contract,
        sink.operator,
        &sink.cache,
        &mut sink.state,
        &removed_log(US, *h.as_bytes()),
    )
    .await;

    assert!(
        !sink.cache.is_chain_denied(h),
        "de-listed hashes stop being blacklist-coded"
    );
    assert!(
        sink.cache.is_evicted(h),
        "...but the eviction is sticky and one-way"
    );
    Ok(())
}

/// ...whereas a hash still in scope under another region keeps its deny.
#[tokio::test]
async fn hash_removal_keeps_the_deny_when_still_in_scope() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let mut sink = enforcing_sink(&[true, true], &metrics).await?;
    let h = hash(0x56);
    sink.state.add_entry(US, h);
    let _ = recheck(
        &sink.contract,
        sink.operator,
        &sink.cache,
        &sink.warming,
        &mut sink.state,
        h,
    )
    .await;

    on_removed_log(
        &sink.contract,
        sink.operator,
        &sink.cache,
        &mut sink.state,
        &removed_log(FR, *h.as_bytes()),
    )
    .await;

    assert!(sink.cache.is_chain_denied(h));
    Ok(())
}

/// A scope read that fails on the removal keeps the deny: over-denying is the
/// safe direction, and the deny is what stops serving if the eviction failed.
#[tokio::test]
async fn hash_removal_keeps_the_deny_when_the_scope_read_fails() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    // One scope read to enforce; the removal's read finds the queue empty.
    let mut sink = enforcing_sink(&[true], &metrics).await?;
    let h = hash(0x57);
    sink.state.add_entry(US, h);
    let _ = recheck(
        &sink.contract,
        sink.operator,
        &sink.cache,
        &sink.warming,
        &mut sink.state,
        h,
    )
    .await;

    on_removed_log(
        &sink.contract,
        sink.operator,
        &sink.cache,
        &mut sink.state,
        &removed_log(FR, *h.as_bytes()),
    )
    .await;

    assert!(sink.cache.is_chain_denied(h));
    Ok(())
}

/// A live `HashBlacklisted` whose enforcement fails forces the batched
/// re-scope onto this tick instead of the operator's re-scope cadence.
#[tokio::test]
async fn a_failed_live_enforcement_forces_a_prompt_rescan() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let h = hash(0x58);

    // The scope read fails (empty queue): the next tick must re-scope.
    let mut sink = enforcing_sink(&[], &metrics).await?;
    sink.last_rescan = Some(Instant::now());
    sink.apply(blacklisted_log(US, *h.as_bytes())).await?;
    assert!(sink.last_rescan.is_none());

    // Control: a clean enforcement keeps the cadence.
    let mut sink = enforcing_sink(&[true], &metrics).await?;
    sink.last_rescan = Some(Instant::now());
    sink.apply(blacklisted_log(US, *h.as_bytes())).await?;
    assert!(sink.last_rescan.is_some());
    assert!(sink.cache.is_chain_denied(h));
    assert!(sink.cache.is_evicted(h));
    Ok(())
}

/// #1319: a re-scope that cannot enforce every entry must NOT bail (so the loop
/// reads healthy), but MUST bump `blacklist_enforcement_failures_total`.
#[tokio::test]
async fn enforcement_failure_counts_without_bailing() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let mut sink = failing_sink(1, &metrics).await?;

    let result = sink.on_tick_complete().await;
    assert!(
        result.is_ok(),
        "an enforcement failure must not bail the tick: {result:?}"
    );

    let text = metrics.encode()?;
    assert!(
        text.lines()
            .any(|l| l == "decdn_blacklist_enforcement_failures_total 1"),
        "one unenforced entry must bump the enforcement counter:\n{text}"
    );
    assert!(
        text.lines()
            .any(|l| l == "decdn_blacklist_watcher_down_seconds 0"),
        "an enforcement failure is not a chain-read outage — down_seconds stays 0:\n{text}"
    );
    Ok(())
}

/// #1319, aggregate semantics: the counter bumps by the *count* of unenforced
/// hashes in one pass (`inc_by`), not once.
#[tokio::test]
async fn enforcement_failure_counter_aggregates_per_pass() -> Result<()> {
    let metrics = Arc::new(Metrics::new());
    let mut sink = failing_sink(2, &metrics).await?;

    sink.on_tick_complete().await?;

    let text = metrics.encode()?;
    assert!(
        text.lines()
            .any(|l| l == "decdn_blacklist_enforcement_failures_total 2"),
        "two unenforced entries in one pass must bump the counter by 2:\n{text}"
    );
    Ok(())
}

fn blacklisted_log(region: B256, hash_bytes: [u8; 32]) -> Log {
    let event = HashBlacklisted {
        region,
        hash: B256::from(hash_bytes),
        reason: String::new(),
    };
    Log {
        inner: alloy::primitives::Log {
            address: Address::repeat_byte(0x11),
            data: event.encode_log_data(),
        },
        ..Default::default()
    }
}

fn removed_log(region: B256, hash_bytes: [u8; 32]) -> Log {
    let event = HashRemoved {
        region,
        hash: B256::from(hash_bytes),
    };
    Log {
        inner: alloy::primitives::Log {
            address: Address::repeat_byte(0x11),
            data: event.encode_log_data(),
        },
        ..Default::default()
    }
}

fn origin_log(origin: Address, blacklisted: bool) -> Log {
    let event = OriginBlacklistUpdated {
        origin,
        blacklisted,
    };
    Log {
        inner: alloy::primitives::Log {
            address: Address::repeat_byte(0x11),
            data: event.encode_log_data(),
        },
        ..Default::default()
    }
}

fn operator_log(operator: Address) -> Log {
    let event = OperatorBlacklisted { operator };
    Log {
        inner: alloy::primitives::Log {
            address: Address::repeat_byte(0x11),
            data: event.encode_log_data(),
        },
        ..Default::default()
    }
}

// ----- origin deny-set (ADR 011 § Hash Evasion) -----

/// An `OriginBlacklistUpdated` log reaches the deny-set the delivery path reads.
#[test]
fn origin_log_reaches_the_deny_set() -> Result<()> {
    let deny = Arc::new(ContentDenylist::empty());
    let mut state = state_with_denylist(Arc::clone(&deny));
    let origin = Address::repeat_byte(0x44);

    on_origin_log(&mut state, &origin_log(origin, true))?;

    assert!(deny.is_origin_denied(&origin), "reaches the live deny-set");
    Ok(())
}

/// De-listing clears the deny-set entry.
#[test]
fn origin_delisting_clears_the_deny_set() -> Result<()> {
    let deny = Arc::new(ContentDenylist::empty());
    let mut state = state_with_denylist(Arc::clone(&deny));
    let origin = Address::repeat_byte(0x45);

    on_origin_log(&mut state, &origin_log(origin, true))?;
    on_origin_log(&mut state, &origin_log(origin, false))?;

    assert!(!deny.is_origin_denied(&origin));
    Ok(())
}

/// `addOperator` is the primary governance path — it emits `OperatorBlacklisted`,
/// never `OriginBlacklistUpdated`, and writes a different on-chain mapping.
/// Watching only the latter left the voted, ejecting path unenforced at the
/// delivery gate — the tail twin of the #1499 enumeration guard.
#[test]
fn operator_blacklist_log_reaches_the_same_deny_set() -> Result<()> {
    let deny = Arc::new(ContentDenylist::empty());
    let mut state = state_with_denylist(Arc::clone(&deny));
    let operator = Address::repeat_byte(0x46);

    on_operator_log(&mut state, &operator_log(operator), true)?;

    assert!(deny.is_origin_denied(&operator));
    Ok(())
}

/// An undecodable origin log must NOT be skipped: the tick aborts rather than
/// advancing the cursor past a takedown the node could not read.
#[test]
fn undecodable_origin_log_aborts_the_tick() {
    let mut state = state();
    let mut log = origin_log(Address::repeat_byte(0x48), true);
    log.inner.data.data = vec![0x01].into();

    let Err(err) = on_origin_log(&mut state, &log) else {
        panic!("an unreadable takedown event must not be skipped");
    };
    assert!(
        format!("{err:#}").contains("refusing to advance"),
        "{err:#}"
    );
}
