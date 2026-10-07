use super::*;
use std::sync::RwLock as StdRwLock;
use std::sync::atomic::{AtomicUsize, Ordering};

fn nid(b: u8) -> NodeId {
    NodeId::from_bytes([b; 32])
}
fn addr(b: u8) -> Address {
    Address::from([b; 20])
}
fn ns(n: u64) -> U256 {
    U256::from(n)
}

/// Stub staker set with a mutable active set, for liveness tests.
#[derive(Debug)]
struct StubStakers(StdRwLock<std::collections::HashSet<NodeId>>);

impl StubStakers {
    fn new(active: &[NodeId]) -> Self {
        Self(StdRwLock::new(active.iter().copied().collect()))
    }
    fn set_active(&self, node_id: NodeId, active: bool) {
        let mut guard = self.0.write().unwrap();
        if active {
            guard.insert(node_id);
        } else {
            guard.remove(&node_id);
        }
    }
}

impl StakerSet for StubStakers {
    fn is_active(&self, node_id: &NodeId) -> bool {
        self.0.read().unwrap().contains(node_id)
    }
    fn active_nodes(&self) -> Vec<NodeId> {
        self.0.read().unwrap().iter().copied().collect()
    }
    fn len(&self) -> usize {
        self.0.read().unwrap().len()
    }
}

/// Scripted [`OriginChainReads`]: per-namespace authoritative operator sets
/// (mutable, so a test can flip a namespace's result between calls) and an
/// injectable failure set, plus a call counter so tests can assert on cache
/// hits vs RPC misses.
struct StubReads {
    origins: StdRwLock<HashMap<U256, Vec<Address>>>,
    fail_origins: StdRwLock<std::collections::HashSet<U256>>,
    calls: AtomicUsize,
}

impl StubReads {
    fn new() -> Self {
        Self {
            origins: StdRwLock::new(HashMap::new()),
            fail_origins: StdRwLock::new(std::collections::HashSet::new()),
            calls: AtomicUsize::new(0),
        }
    }
    fn origins(self, ns_ops: &[(u64, &[Address])]) -> Self {
        *self.origins.write().unwrap() = ns_ops
            .iter()
            .map(|(n, ops)| (ns(*n), ops.to_vec()))
            .collect();
        self
    }
    fn set_origins(&self, n: u64, ops: &[Address]) {
        self.origins.write().unwrap().insert(ns(n), ops.to_vec());
    }
    fn fail_origins(self, nss: &[u64]) -> Self {
        *self.fail_origins.write().unwrap() = nss.iter().map(|n| ns(*n)).collect();
        self
    }
    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl OriginChainReads for StubReads {
    async fn get_origins(&self, namespace: U256) -> Result<Vec<Address>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_origins.read().unwrap().contains(&namespace) {
            return Err(anyhow::anyhow!(
                "injected getOrigins failure for {namespace}"
            ));
        }
        Ok(self
            .origins
            .read()
            .unwrap()
            .get(&namespace)
            .cloned()
            .unwrap_or_default())
    }
}

/// Build a `ChainOriginDirectory` directly over a `StubReads`/`StubStakers`
/// pair, bypassing `new`'s provider-backed construction (tests have no
/// chain endpoint).
fn directory(
    reads: Arc<StubReads>,
    operator_to_node: HashMap<Address, NodeId>,
    staker_set: Arc<StubStakers>,
    positive_ttl: Duration,
    negative_ttl: Duration,
) -> ChainOriginDirectory {
    directory_with_deny(
        reads,
        operator_to_node,
        staker_set,
        Arc::new(ContentDenylist::empty()),
        positive_ttl,
        negative_ttl,
    )
}

fn directory_with_deny(
    reads: Arc<StubReads>,
    operator_to_node: HashMap<Address, NodeId>,
    staker_set: Arc<StubStakers>,
    content_deny: Arc<ContentDenylist>,
    positive_ttl: Duration,
    negative_ttl: Duration,
) -> ChainOriginDirectory {
    ChainOriginDirectory {
        reads,
        cache: LazyOriginCache::new(8, positive_ttl, negative_ttl),
        operator_to_node: Arc::new(RwLock::new(operator_to_node)),
        staker_set,
        content_deny,
        metrics: Arc::new(Metrics::new()),
    }
}

const LONG: Duration = Duration::from_secs(30);

#[tokio::test]
async fn cold_miss_fetches_and_resolves() {
    let reads = Arc::new(StubReads::new().origins(&[(7, &[addr(0xA), addr(0xB)])]));
    let stakers = Arc::new(StubStakers::new(&[nid(0xA), nid(0xB)]));
    let dir = directory(
        Arc::clone(&reads),
        HashMap::from([(addr(0xA), nid(0xA)), (addr(0xB), nid(0xB))]),
        stakers,
        LONG,
        LONG,
    );
    let got = dir.lookup_origins(ns(7)).await;
    assert_eq!(got, vec![nid(0xA), nid(0xB)]);
    assert_eq!(reads.call_count(), 1);
}

#[tokio::test]
async fn second_lookup_is_a_cache_hit() {
    let reads = Arc::new(StubReads::new().origins(&[(7, &[addr(0xA)])]));
    let stakers = Arc::new(StubStakers::new(&[nid(0xA)]));
    let dir = directory(
        Arc::clone(&reads),
        HashMap::from([(addr(0xA), nid(0xA))]),
        stakers,
        LONG,
        LONG,
    );
    let first = dir.lookup_origins(ns(7)).await;
    let second = dir.lookup_origins(ns(7)).await;
    assert_eq!(first, vec![nid(0xA)]);
    assert_eq!(second, vec![nid(0xA)]);
    assert_eq!(
        reads.call_count(),
        1,
        "second lookup must be a cache hit, not a new RPC"
    );
}

#[tokio::test]
async fn empty_namespace_is_negatively_cached() {
    let reads = Arc::new(StubReads::new());
    let stakers = Arc::new(StubStakers::new(&[]));
    let dir = directory(Arc::clone(&reads), HashMap::new(), stakers, LONG, LONG);
    let first = dir.lookup_origins(ns(9)).await;
    let second = dir.lookup_origins(ns(9)).await;
    assert!(first.is_empty());
    assert!(second.is_empty());
    assert_eq!(
        reads.call_count(),
        1,
        "the negative result must be cached too"
    );
}

#[tokio::test]
async fn negative_entry_expires_faster_than_positive() {
    let reads = Arc::new(StubReads::new());
    let stakers = Arc::new(StubStakers::new(&[nid(0xA)]));
    let dir = directory(
        Arc::clone(&reads),
        HashMap::from([(addr(0xA), nid(0xA))]),
        stakers,
        LONG,
        Duration::from_millis(20),
    );
    // First lookup: namespace 7 currently has no origins → negative entry.
    assert!(dir.lookup_origins(ns(7)).await.is_empty());
    assert_eq!(reads.call_count(), 1);
    // Chain state changes underneath the cache.
    reads.set_origins(7, &[addr(0xA)]);
    tokio::time::sleep(Duration::from_millis(60)).await;
    // The negative TTL has elapsed, so this must re-fetch and observe the
    // new chain state rather than continuing to serve the stale negative.
    let got = dir.lookup_origins(ns(7)).await;
    assert_eq!(got, vec![nid(0xA)]);
    assert_eq!(
        reads.call_count(),
        2,
        "the negative entry must have expired and re-fetched"
    );
}

#[tokio::test]
async fn namespace_zero_never_fetches() {
    let reads = Arc::new(StubReads::new());
    let stakers = Arc::new(StubStakers::new(&[]));
    let dir = directory(Arc::clone(&reads), HashMap::new(), stakers, LONG, LONG);
    assert!(dir.lookup_origins(ns(0)).await.is_empty());
    assert_eq!(
        reads.call_count(),
        0,
        "namespace 0 must never issue getOrigins"
    );
}

#[tokio::test]
async fn rpc_failure_resolves_empty_and_is_not_cached() {
    let reads = Arc::new(
        StubReads::new()
            .origins(&[(7, &[addr(0xA)])])
            .fail_origins(&[7]),
    );
    let stakers = Arc::new(StubStakers::new(&[nid(0xA)]));
    let dir = directory(
        Arc::clone(&reads),
        HashMap::from([(addr(0xA), nid(0xA))]),
        stakers,
        LONG,
        LONG,
    );
    let failed = dir.lookup_origins(ns(7)).await;
    assert!(failed.is_empty());
    let text = dir.metrics.encode().unwrap();
    assert!(
        text.lines()
            .any(|l| l == "decdn_origin_directory_get_origins_failures_total 1"),
        "the RPC failure must bump the failure counter:\n{text}"
    );
    // Clear the injected failure: a subsequent lookup must retry (proving
    // the failed result was not cached) and resolve successfully.
    reads.fail_origins.write().unwrap().clear();
    let healed = dir.lookup_origins(ns(7)).await;
    assert_eq!(healed, vec![nid(0xA)]);
    assert_eq!(reads.call_count(), 2, "a failed lookup must not be cached");
}

#[tokio::test]
async fn liveness_is_applied_live_from_staker_set() {
    let reads = Arc::new(StubReads::new().origins(&[(7, &[addr(0xA), addr(0xB)])]));
    let stakers = Arc::new(StubStakers::new(&[nid(0xA), nid(0xB)]));
    let dir = directory(
        Arc::clone(&reads),
        HashMap::from([(addr(0xA), nid(0xA)), (addr(0xB), nid(0xB))]),
        Arc::clone(&stakers),
        LONG,
        LONG,
    );
    let first = dir.lookup_origins(ns(7)).await;
    assert_eq!(first, vec![nid(0xA), nid(0xB)]);
    // Flip B inactive; the next hit reads the cached operator set but must
    // re-apply liveness live, with no RPC.
    stakers.set_active(nid(0xB), false);
    let second = dir.lookup_origins(ns(7)).await;
    assert_eq!(second, vec![nid(0xA)]);
    assert_eq!(
        reads.call_count(),
        1,
        "liveness change must not trigger a re-fetch"
    );
}

#[tokio::test]
async fn inactive_or_unbound_operators_are_filtered() {
    // B is bonded-but-inactive; C has no reverse binding at all.
    let reads = Arc::new(StubReads::new().origins(&[(7, &[addr(0xA), addr(0xB), addr(0xC)])]));
    let stakers = Arc::new(StubStakers::new(&[nid(0xA)]));
    let dir = directory(
        Arc::clone(&reads),
        HashMap::from([(addr(0xA), nid(0xA)), (addr(0xB), nid(0xB))]),
        stakers,
        LONG,
        LONG,
    );
    let got = dir.lookup_origins(ns(7)).await;
    assert_eq!(got, vec![nid(0xA)]);
}

#[tokio::test]
async fn blacklisted_operator_is_dropped_from_the_resolved_set() {
    // Both A and B are bonded, active, and authorized in getOrigins, but B is
    // on the deny-set (a governance origin/operator blacklist the watcher
    // synced). The node must not route an origin-pull to B even though the
    // seat set still lists it — the contract does not filter getOrigins.
    let reads = Arc::new(StubReads::new().origins(&[(7, &[addr(0xA), addr(0xB)])]));
    let stakers = Arc::new(StubStakers::new(&[nid(0xA), nid(0xB)]));
    let deny = Arc::new(ContentDenylist::empty());
    deny.apply_chain_origin(addr(0xB), true);
    let dir = directory_with_deny(
        Arc::clone(&reads),
        HashMap::from([(addr(0xA), nid(0xA)), (addr(0xB), nid(0xB))]),
        stakers,
        Arc::clone(&deny),
        LONG,
        LONG,
    );

    assert_eq!(dir.lookup_origins(ns(7)).await, vec![nid(0xA)]);

    // The filter is applied live: lifting the blacklist restores B with no
    // re-fetch, proving the deny-set is not baked into the cached set.
    deny.apply_chain_origin(addr(0xB), false);
    assert_eq!(dir.lookup_origins(ns(7)).await, vec![nid(0xA), nid(0xB)]);
    assert_eq!(
        reads.call_count(),
        1,
        "a blacklist change must not trigger a re-fetch"
    );
}

/// `getOrigins` is bounded, so a stalled provider fails this lookup rather
/// than wedging it.
///
/// This drives `Contracts<P>` — the *production* [`OriginChainReads`]
/// impl — deliberately. Every other test in this module uses `StubReads`,
/// which carries no `timed` wrap and would therefore pass whether or not
/// the production impl bounds anything: a stub-level test here would look
/// like a wiring test while asserting nothing about the wiring.
#[tokio::test(start_paused = true)]
async fn hanging_get_origins_fails_the_lookup_rather_than_wedging() {
    use crate::chain_events::test_support::{bounded, hanging_provider};
    let provider = hanging_provider();
    let contracts = Contracts {
        origin: OriginAssignment::OriginAssignmentInstance::new(Address::ZERO, provider),
    };
    let err = bounded("get_origins", contracts.get_origins(U256::from(1)))
        .await
        .err()
        .map(|e| format!("{e:#}"));
    assert!(
        err.as_ref()
            .is_some_and(|e| e.contains("getOrigins timed out after")),
        "a stalled getOrigins must fail, not hang: {err:?}"
    );
}

/// The operator-facing render must name *why* the read failed, not just
/// what was attempted.
///
/// `get_origins` wraps `timed` in `.with_context(…)`, and an `anyhow`
/// Display renders only the outermost context — so logging it with
/// `sanitize_rpc_display` prints a bare `getOrigins(namespace=1)` and
/// drops "timed out after 10s", which is the entire product of bounding
/// the read. This pins the *render*, deliberately.
#[tokio::test(start_paused = true)]
async fn stalled_get_origins_renders_the_timeout_to_the_operator() {
    use crate::chain_events::test_support::{bounded, hanging_provider};
    let provider = hanging_provider();
    let contracts = Contracts {
        origin: OriginAssignment::OriginAssignmentInstance::new(Address::ZERO, provider),
    };
    let err = bounded("get_origins", contracts.get_origins(U256::from(1)))
        .await
        .unwrap_err();

    let rendered = sanitize_err_chain(&err);
    assert!(
        rendered.contains("getOrigins timed out after"),
        "the operator must see why it failed, not only what was attempted: {rendered}"
    );
    assert!(
        rendered.contains("namespace=1"),
        "the context must survive alongside the cause: {rendered}"
    );
}
