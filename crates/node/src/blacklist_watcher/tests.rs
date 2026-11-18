use std::sync::Mutex;

use super::*;

const NOW: u64 = 1_000_000;

fn hash(n: u8) -> B256 {
    B256::repeat_byte(n)
}

fn addr(n: u8) -> Address {
    Address::repeat_byte(n)
}

fn region(code: &str) -> B256 {
    B256::right_padding_from(code.as_bytes())
}

fn record(kind: RecordKind, region: B256, key: B256, expires_at: u64) -> Record {
    Record {
        kind,
        expires_at,
        region,
        key,
    }
}

/// A page bound as an iterator count; one past `usize` reads as "all".
fn idx(v: u64) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

fn addr_key(a: Address) -> B256 {
    a.into_word()
}

fn index_hashes(index: &DenyIndex, region: B256) -> HashMap<Hash, u64> {
    index.hashes.get(&region).cloned().unwrap_or_default()
}

#[test]
fn global_region_key_is_the_packed_literal() {
    assert_eq!(GLOBAL_REGION, region("GLOBAL"));
}

#[test]
fn hash_add_kinds_set_the_hash_with_its_expiry_and_region_zero_is_global() {
    let mut index = DenyIndex::default();
    index.apply(&record(RecordKind::HashAdd, B256::ZERO, hash(1), 0));
    index.apply(&record(
        RecordKind::HashEmergencyAdd,
        B256::ZERO,
        hash(2),
        NOW + 10,
    ));
    index.apply(&record(RecordKind::HashAdd, region("DE"), hash(3), 0));
    let global = index_hashes(&index, GLOBAL_REGION);
    assert_eq!(global.get(&Hash::from_bytes(hash(1).0)), Some(&0));
    assert_eq!(global.get(&Hash::from_bytes(hash(2).0)), Some(&(NOW + 10)));
    assert!(index_hashes(&index, region("DE")).contains_key(&Hash::from_bytes(hash(3).0)));

    // A ratify overwrites the emergency expiry with none.
    index.apply(&record(RecordKind::HashRatify, B256::ZERO, hash(2), 0));
    assert_eq!(
        index_hashes(&index, GLOBAL_REGION).get(&Hash::from_bytes(hash(2).0)),
        Some(&0)
    );
}

#[test]
fn hash_remove_and_expire_delete_only_their_region() {
    let mut index = DenyIndex::default();
    index.apply(&record(RecordKind::HashAdd, B256::ZERO, hash(1), 0));
    index.apply(&record(RecordKind::HashAdd, region("DE"), hash(1), 0));
    index.apply(&record(RecordKind::HashRemove, region("DE"), hash(1), 0));
    assert!(index_hashes(&index, region("DE")).is_empty());
    assert_eq!(index_hashes(&index, GLOBAL_REGION).len(), 1);
    index.apply(&record(RecordKind::HashExpire, B256::ZERO, hash(1), 0));
    assert!(index_hashes(&index, GLOBAL_REGION).is_empty());
}

#[test]
fn address_kinds_set_and_clear_each_leg() {
    let mut index = DenyIndex::default();
    index.apply(&record(
        RecordKind::OriginSet,
        B256::ZERO,
        addr_key(addr(1)),
        0,
    ));
    index.apply(&record(
        RecordKind::OriginEmergencyAdd,
        B256::ZERO,
        addr_key(addr(2)),
        NOW + 5,
    ));
    index.apply(&record(
        RecordKind::OperatorAdd,
        B256::ZERO,
        addr_key(addr(3)),
        0,
    ));
    assert_eq!(
        index.denied_addresses(NOW),
        HashSet::from([addr(1), addr(2), addr(3)])
    );
    index.apply(&record(
        RecordKind::OriginClear,
        B256::ZERO,
        addr_key(addr(1)),
        0,
    ));
    index.apply(&record(
        RecordKind::OriginExpire,
        B256::ZERO,
        addr_key(addr(2)),
        0,
    ));
    index.apply(&record(
        RecordKind::OperatorRemove,
        B256::ZERO,
        addr_key(addr(3)),
        0,
    ));
    assert!(index.denied_addresses(NOW).is_empty());
}

/// An address stays denied while either leg is live: clearing the origin leg
/// keeps an operator-blacklisted address refused.
#[test]
fn an_address_is_denied_while_either_leg_holds() {
    let mut index = DenyIndex::default();
    index.apply(&record(
        RecordKind::OriginSet,
        B256::ZERO,
        addr_key(addr(1)),
        0,
    ));
    index.apply(&record(
        RecordKind::OperatorAdd,
        B256::ZERO,
        addr_key(addr(1)),
        0,
    ));
    index.apply(&record(
        RecordKind::OriginClear,
        B256::ZERO,
        addr_key(addr(1)),
        0,
    ));
    assert_eq!(index.denied_addresses(NOW), HashSet::from([addr(1)]));
}

#[test]
fn body_and_window_records_never_touch_the_hash_index_or_the_legs() {
    let mut index = DenyIndex::default();
    index.apply(&record(RecordKind::HashAdd, region("DE"), hash(1), 0));
    index.apply(&record(
        RecordKind::OriginSet,
        B256::ZERO,
        addr_key(addr(9)),
        0,
    ));
    let before = (
        index.hashes.clone(),
        index.origin_leg.clone(),
        index.operator_leg.clone(),
    );

    let body = addr_key(addr(7));
    index.apply(&record(RecordKind::BodyRegister, region("DE"), body, 0));
    assert_eq!(
        index.bodies.get(&region("DE")),
        Some(&BodyState {
            body: addr(7),
            suspension: None
        })
    );
    index.apply(&record(
        RecordKind::BodySuspend,
        region("DE"),
        body,
        NOW + 99,
    ));
    assert_eq!(
        index.bodies[&region("DE")].suspension,
        Some(Suspension::Until(NOW + 99))
    );
    index.apply(&record(
        RecordKind::BodySuspensionRatify,
        region("DE"),
        body,
        0,
    ));
    assert_eq!(
        index.bodies[&region("DE")].suspension,
        Some(Suspension::Ratified)
    );
    index.apply(&record(RecordKind::BodyUnsuspend, region("DE"), body, 0));
    assert_eq!(index.bodies[&region("DE")].suspension, None);
    index.apply(&record(RecordKind::BodyDeregister, region("DE"), body, 0));
    assert!(index.bodies.is_empty());
    index.apply(&record(
        RecordKind::ComplianceWindowSet,
        B256::ZERO,
        B256::from(U256::from(7200u64)),
        0,
    ));
    index.apply(&record(
        RecordKind::EmergencyComplianceWindowSet,
        B256::ZERO,
        B256::from(U256::from(3600u64)),
        0,
    ));
    assert_eq!(index.compliance_window, 7200);
    assert_eq!(index.emergency_compliance_window, 3600);

    assert_eq!(
        (
            index.hashes.clone(),
            index.origin_leg.clone(),
            index.operator_leg.clone()
        ),
        before
    );
}

/// Records apply in ledger order: an add then a remove of one key between
/// two polls leaves it absent, a remove then a re-add leaves it present.
#[test]
fn records_apply_in_ledger_order() {
    let mut index = DenyIndex::default();
    index.apply(&record(RecordKind::HashAdd, B256::ZERO, hash(1), 0));
    index.apply(&record(RecordKind::HashRemove, B256::ZERO, hash(1), 0));
    assert!(index.denied_hashes(&[GLOBAL_REGION], NOW).is_empty());
    index.apply(&record(RecordKind::HashAdd, B256::ZERO, hash(1), 0));
    assert_eq!(index.denied_hashes(&[GLOBAL_REGION], NOW).len(), 1);
}

/// The node expires an entry locally once its clock passes `expiresAt`, with
/// no record marking the moment; the deadline itself is still live.
#[test]
fn local_expiry_follows_expires_at() {
    let mut index = DenyIndex::default();
    index.apply(&record(
        RecordKind::HashEmergencyAdd,
        B256::ZERO,
        hash(1),
        NOW,
    ));
    index.apply(&record(
        RecordKind::OriginEmergencyAdd,
        B256::ZERO,
        addr_key(addr(1)),
        NOW,
    ));
    assert_eq!(index.denied_hashes(&[GLOBAL_REGION], NOW).len(), 1);
    assert_eq!(index.denied_addresses(NOW).len(), 1);
    assert!(index.denied_hashes(&[GLOBAL_REGION], NOW + 1).is_empty());
    assert!(index.denied_addresses(NOW + 1).is_empty());
}

#[test]
fn only_regions_in_scope_deny() {
    let mut index = DenyIndex::default();
    index.apply(&record(RecordKind::HashAdd, region("DE"), hash(1), 0));
    index.apply(&record(RecordKind::HashAdd, region("US"), hash(2), 0));
    let denied = index.denied_hashes(&[GLOBAL_REGION, region("DE")], NOW);
    assert_eq!(denied, HashSet::from([Hash::from_bytes(hash(1).0)]));
}

#[test]
fn emergency_terms_mirror_the_contract() {
    assert_eq!(emergency_expires_at(10, 0), 10 + 14 * 86_400);
    assert_eq!(emergency_expires_at(10, 1), 10 + 90 * 86_400);
    assert_eq!(emergency_expires_at(10, 2), 10 + 90 * 86_400);
}

/// A scripted `ContentBlacklist` at one current state, read at any block.
#[derive(Default)]
struct FakeChain {
    inner: Mutex<FakeState>,
}

#[derive(Default, Clone)]
struct FakeState {
    safe: u64,
    ledger: Vec<Record>,
    scope: Vec<B256>,
    regions: HashMap<B256, Vec<(B256, Option<u64>)>>,
    addresses: Vec<(Address, Option<u64>, bool)>,
    /// When set, `hash_count` reports this many more entries than it pages.
    short_page: bool,
    /// When set, `getMutations` fails.
    fail_mutations: bool,
    /// Every read call, by name.
    calls: Vec<&'static str>,
}

impl FakeChain {
    fn with(state: FakeState) -> Self {
        Self {
            inner: Mutex::new(state),
        }
    }

    fn update(&self, f: impl FnOnce(&mut FakeState)) {
        f(&mut self.inner.lock().unwrap());
    }

    fn calls(&self, name: &str) -> usize {
        self.inner
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|c| **c == name)
            .count()
    }

    fn read<T>(&self, name: &'static str, f: impl FnOnce(&FakeState) -> T) -> T {
        let mut guard = self.inner.lock().unwrap();
        guard.calls.push(name);
        f(&guard)
    }
}

impl BlacklistReads for FakeChain {
    async fn safe_block(&self) -> Result<u64> {
        Ok(self.read("safe_block", |s| s.safe))
    }

    async fn mutation_count(&self, _at: u64) -> Result<u64> {
        Ok(self.read("mutation_count", |s| s.ledger.len() as u64))
    }

    async fn mutations(&self, from: u64, limit: u64, _at: u64) -> Result<Vec<Record>> {
        self.read("mutations", |s| {
            anyhow::ensure!(!s.fail_mutations, "getMutations failed");
            Ok(s.ledger
                .iter()
                .skip(idx(from))
                .take(idx(limit))
                .copied()
                .collect())
        })
    }

    async fn scope_regions(&self, _operator: Address, _at: u64) -> Result<Vec<B256>> {
        Ok(self.read("scope_regions", |s| s.scope.clone()))
    }

    async fn hash_count(&self, region: B256, _at: u64) -> Result<u64> {
        Ok(self.read("hash_count", |s| {
            let n = s.regions.get(&region).map_or(0, Vec::len) as u64;
            if s.short_page { n + 1 } else { n }
        }))
    }

    async fn hash_page(
        &self,
        region: B256,
        offset: u64,
        limit: u64,
        _at: u64,
    ) -> Result<Vec<B256>> {
        Ok(self.read("hash_page", |s| {
            s.regions
                .get(&region)
                .map(|v| {
                    v.iter()
                        .skip(idx(offset))
                        .take(idx(limit))
                        .map(|(h, _)| *h)
                        .collect()
                })
                .unwrap_or_default()
        }))
    }

    async fn hash_expiry(&self, region: B256, hash: B256, _at: u64) -> Result<Option<u64>> {
        Ok(self.read("hash_expiry", |s| {
            s.regions
                .get(&region)
                .and_then(|v| v.iter().find(|(h, _)| *h == hash))
                .and_then(|(_, e)| *e)
        }))
    }

    async fn address_count(&self, _at: u64) -> Result<u64> {
        Ok(self.read("address_count", |s| s.addresses.len() as u64))
    }

    async fn address_page(&self, offset: u64, limit: u64, _at: u64) -> Result<Vec<Address>> {
        Ok(self.read("address_page", |s| {
            s.addresses
                .iter()
                .skip(idx(offset))
                .take(idx(limit))
                .map(|(a, _, _)| *a)
                .collect()
        }))
    }

    async fn address_legs(&self, addr: Address, _at: u64) -> Result<(Option<u64>, bool)> {
        Ok(self.read("address_legs", |s| {
            s.addresses
                .iter()
                .find(|(a, _, _)| *a == addr)
                .map_or((None, false), |(_, o, op)| (*o, *op))
        }))
    }

    async fn regional_body(&self, _region: B256, _at: u64) -> Result<Option<BodyState>> {
        Ok(self.read("regional_body", |_| None))
    }

    async fn compliance_windows(&self, _at: u64) -> Result<(u64, u64)> {
        Ok(self.read("compliance_windows", |_| (86_400, 7_200)))
    }
}

fn base_state() -> FakeState {
    FakeState {
        safe: 100,
        scope: vec![GLOBAL_REGION],
        regions: HashMap::from([(
            GLOBAL_REGION,
            vec![
                (hash(1), Some(0)),
                // A lapsed emergency entry nobody has expired: raw membership
                // lists it, its liveness drops it.
                (hash(2), Some(NOW - 1)),
            ],
        )]),
        addresses: vec![(addr(1), Some(0), false), (addr(2), None, true)],
        ledger: vec![record(RecordKind::HashAdd, B256::ZERO, hash(1), 0)],
        ..FakeState::default()
    }
}

#[tokio::test]
async fn snapshot_keeps_live_entries_and_takes_the_cursor() {
    let chain = FakeChain::with(base_state());
    let snap = read_snapshot(&chain, addr(0xAA), NOW).await.unwrap();
    assert_eq!(snap.cursor, 1);
    assert_eq!(snap.block, 100);
    assert_eq!(
        snap.index.denied_hashes(&snap.scope, NOW),
        HashSet::from([Hash::from_bytes(hash(1).0)])
    );
    assert_eq!(
        snap.index.denied_addresses(NOW),
        HashSet::from([addr(1), addr(2)])
    );
    assert_eq!(snap.index.compliance_window, 86_400);
}

#[tokio::test]
async fn snapshot_aborts_on_a_count_mismatch() {
    let mut state = base_state();
    state.short_page = true;
    let chain = FakeChain::with(state);
    let err = read_snapshot(&chain, addr(0xAA), NOW).await.unwrap_err();
    assert!(format!("{err:#}").contains("partial set"), "{err:#}");
}

#[tokio::test]
async fn poll_applies_new_records_in_order_and_moves_the_cursor() {
    let chain = FakeChain::with(base_state());
    let mut state =
        LedgerState::from_snapshot(read_snapshot(&chain, addr(0xAA), NOW).await.unwrap());
    chain.update(|s| {
        s.ledger
            .push(record(RecordKind::HashAdd, B256::ZERO, hash(3), 0));
        s.ledger
            .push(record(RecordKind::HashRemove, B256::ZERO, hash(1), 0));
        s.ledger.push(record(
            RecordKind::OperatorAdd,
            B256::ZERO,
            addr_key(addr(5)),
            0,
        ));
    });
    let outcome = state.poll(&chain, addr(0xAA), NOW).await.unwrap();
    assert_eq!(outcome.applied, 3);
    assert!(!outcome.resnapshot);
    assert_eq!(state.cursor, 4);
    assert_eq!(
        state.index.denied_hashes(&state.scope, NOW),
        HashSet::from([Hash::from_bytes(hash(3).0)])
    );
    assert!(state.index.denied_addresses(NOW).contains(&addr(5)));

    // A quiet ledger reads no record page.
    let before = chain.calls("mutations");
    let outcome = state.poll(&chain, addr(0xAA), NOW).await.unwrap();
    assert_eq!(outcome, PollOutcome::default());
    assert_eq!(chain.calls("mutations"), before);
}

#[tokio::test]
async fn a_failed_record_read_leaves_the_state_unchanged() {
    let chain = FakeChain::with(base_state());
    let mut state =
        LedgerState::from_snapshot(read_snapshot(&chain, addr(0xAA), NOW).await.unwrap());
    chain.update(|s| {
        s.ledger
            .push(record(RecordKind::HashRemove, B256::ZERO, hash(1), 0));
        s.fail_mutations = true;
    });
    let before = state.index.clone();
    assert!(state.poll(&chain, addr(0xAA), NOW).await.is_err());
    assert_eq!(state.cursor, 1);
    assert_eq!(state.index, before);
}

/// A count below the cursor means a reorg past the `safe` block: the poll
/// takes a new snapshot instead of applying records.
#[tokio::test]
async fn a_count_below_the_cursor_takes_a_new_snapshot() {
    let chain = FakeChain::with(base_state());
    let mut state =
        LedgerState::from_snapshot(read_snapshot(&chain, addr(0xAA), NOW).await.unwrap());
    chain.update(|s| {
        s.ledger.clear();
        s.regions.insert(GLOBAL_REGION, vec![(hash(7), Some(0))]);
    });
    let outcome = state.poll(&chain, addr(0xAA), NOW).await.unwrap();
    assert!(outcome.resnapshot);
    assert_eq!(state.cursor, 0);
    assert_eq!(
        state.index.denied_hashes(&state.scope, NOW),
        HashSet::from([Hash::from_bytes(hash(7).0)])
    );
}

/// A region that enters the scope (an ADR 030 ripening transition, which writes
/// nothing to `ContentBlacklist`) is paged once and replaces what the index
/// held for it; a region already paged is not paged again.
#[tokio::test]
async fn a_region_new_to_the_scope_is_paged_once() {
    let chain = FakeChain::with(base_state());
    let mut state =
        LedgerState::from_snapshot(read_snapshot(&chain, addr(0xAA), NOW).await.unwrap());
    // The index learned a stale DE entry from the ledger before DE entered
    // the scope; the page replaces it.
    state
        .index
        .apply(&record(RecordKind::HashAdd, region("DE"), hash(8), 0));
    chain.update(|s| {
        s.scope.push(region("DE"));
        s.regions.insert(region("DE"), vec![(hash(9), Some(0))]);
    });
    let outcome = state.poll(&chain, addr(0xAA), NOW).await.unwrap();
    assert_eq!(outcome.paged_regions, 1);
    assert_eq!(
        index_hashes(&state.index, region("DE")),
        HashMap::from([(Hash::from_bytes(hash(9).0), 0)])
    );
    assert!(
        state
            .index
            .denied_hashes(&state.scope, NOW)
            .contains(&Hash::from_bytes(hash(9).0))
    );

    let pages = chain.calls("hash_count");
    let outcome = state.poll(&chain, addr(0xAA), NOW).await.unwrap();
    assert_eq!(outcome.paged_regions, 0);
    assert_eq!(chain.calls("hash_count"), pages);
}

async fn enforcer() -> anyhow::Result<(
    tempfile::TempDir,
    CacheEngine,
    Enforcer,
    Arc<ContentDenylist>,
)> {
    let tmp = tempfile::tempdir()?;
    let cache = CacheEngine::open(tmp.path(), Vec::new(), 1).await?;
    let denylist = Arc::new(ContentDenylist::empty());
    let enforcer = Enforcer::new(
        cache.clone(),
        Arc::new(WarmingAllowance::new(1000, 0)),
        Arc::clone(&denylist),
    );
    Ok((tmp, cache, enforcer, denylist))
}

/// Enforcement denies and evicts every live in-scope hash, lifts the deny on a
/// hash that left the set (the eviction stays), and publishes the address
/// union.
#[tokio::test]
async fn enforcement_follows_the_index() -> anyhow::Result<()> {
    let (_tmp, cache, mut enforcer, denylist) = enforcer().await?;
    let mut state = LedgerState {
        index: DenyIndex::default(),
        cursor: 0,
        scope: vec![GLOBAL_REGION],
        paged: HashSet::new(),
    };
    state
        .index
        .apply(&record(RecordKind::HashAdd, B256::ZERO, hash(1), 0));
    state.index.apply(&record(
        RecordKind::OperatorAdd,
        B256::ZERO,
        addr_key(addr(4)),
        0,
    ));
    let h1 = Hash::from_bytes(hash(1).0);

    assert!(enforcer.enforce(&state, NOW).await.is_empty());
    assert!(cache.is_chain_denied(h1));
    assert!(cache.is_evicted(h1));
    assert!(denylist.is_origin_denied(&addr(4)));

    state
        .index
        .apply(&record(RecordKind::HashRemove, B256::ZERO, hash(1), 0));
    state.index.apply(&record(
        RecordKind::OperatorRemove,
        B256::ZERO,
        addr_key(addr(4)),
        0,
    ));
    assert!(enforcer.enforce(&state, NOW).await.is_empty());
    assert!(!cache.is_chain_denied(h1));
    assert!(cache.is_evicted(h1), "eviction is sticky");
    assert!(!denylist.is_origin_denied(&addr(4)));
    Ok(())
}

/// An emergency entry stops being denied once the local clock passes its
/// `expiresAt`, with no record and no chain read.
#[tokio::test]
async fn enforcement_lifts_a_locally_expired_entry() -> anyhow::Result<()> {
    let (_tmp, cache, mut enforcer, _denylist) = enforcer().await?;
    let mut state = LedgerState {
        index: DenyIndex::default(),
        cursor: 0,
        scope: vec![GLOBAL_REGION],
        paged: HashSet::new(),
    };
    state.index.apply(&record(
        RecordKind::HashEmergencyAdd,
        B256::ZERO,
        hash(1),
        NOW,
    ));
    let h1 = Hash::from_bytes(hash(1).0);
    enforcer.enforce(&state, NOW).await;
    assert!(cache.is_chain_denied(h1));
    enforcer.enforce(&state, NOW + 1).await;
    assert!(!cache.is_chain_denied(h1));
    Ok(())
}

/// The boot pass seats the snapshot, enforces it and stamps freshness; a
/// later tick stamps freshness again even when the count did not move.
#[tokio::test]
async fn boot_enforces_and_every_successful_poll_stamps_freshness() -> anyhow::Result<()> {
    let (_tmp, cache, enforcer, _denylist) = enforcer().await?;
    let metrics = Arc::new(Metrics::new());
    let freshness = ChainFreshness::new(Duration::from_secs(60));
    let chain = FakeChain::with(base_state());
    let boot = BootRetry::single_attempt(Arc::clone(&metrics));
    let mut sync = boot_sync(
        chain,
        addr(0xAA),
        enforcer,
        freshness.clone(),
        metrics,
        &boot,
    )
    .await?;
    assert!(cache.is_chain_denied(Hash::from_bytes(hash(1).0)));
    assert!(!freshness.is_stale());
    sync.tick().await;
    assert!(!freshness.is_stale());
    Ok(())
}
