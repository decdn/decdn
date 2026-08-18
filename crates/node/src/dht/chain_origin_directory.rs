//! Chain-backed [`OriginDirectory`] implementation.
//!
//! Resolves a request's **namespace** to the set of currently-active operator
//! `NodeId`s authorised as origins for it. The resolution chain (ADR 022 §
//! FIND\_VALUE Flow "Origin discovery", ADR 011 § Origin Assignment
//! Authority) is:
//!
//!   `OriginAssignment.getOrigins(namespaceId)` → `[operator...]`
//!     → capacity-bond reverse projection (`operator → NodeId`, binding only)
//!     → keep operators the shared [`StakerSet`] reports `active`
//!
//! The bare hash carries no origin information (ADR 002 § Retrieval by
//! namespace): the request supplies the namespace its content is published
//! under. `namespaceId == 0` has no authorized origins, so it resolves to no
//! origins here and is never fetched or cached.
//!
//! Namespace creation is permissionless and free, so this directory does not
//! mirror the whole chain-side namespace set up front: it resolves each
//! namespace lazily, on first request, and caches the result (positive or
//! negative) behind a split TTL ([`crate::dht::lazy_origin_cache`]). A live
//! cache hit — including a cached "no origins" — resolves with no RPC; a
//! cold miss issues one `getOrigins(namespace)` call. Chain reads therefore
//! scale with the namespaces this node is actually asked to serve, not the
//! permissionless global namespace count.
//!
//! The cache stores only the raw operator address set; it never stores
//! resolved `NodeId`s or liveness. Both the `operator → NodeId` binding (the
//! shared capacity-bond reverse projection) and staker liveness (the shared
//! [`StakerSet`]) are read live at resolution time, so a TTL-anchored cache
//! entry never goes stale on the parts of the answer that can change without
//! a new `getOrigins` call.
//!
//! A `getOrigins` RPC failure fails closed: the lookup resolves no origins
//! for that request, and — because a transient RPC failure must not be frozen
//! for a TTL — the result is not cached, so the next request retries.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use anyhow::{Context, Result};
use tracing::warn;

use crate::chain_events::timed;
use crate::dht::lazy_origin_cache::{LazyOriginCache, resolve_active};
use crate::dht::origin::OriginDirectory;
use crate::dht::routing::NodeId;
use crate::dht::staker_set::StakerSet;
use crate::metrics::Metrics;
use decdn_common::redact::sanitize_err_chain;
use decdn_incentive::origin_assignment::OriginAssignment;

/// Contract handle(s) the directory needs. Bundled so production construction
/// has one call site; tests inject [`OriginChainReads`] directly via
/// `StubReads` instead of this type.
struct Contracts<P: Provider + Clone> {
    origin: OriginAssignment::OriginAssignmentInstance<P>,
}

/// The chain read this directory performs, behind a trait so lookup is
/// unit-testable without a provider and so [`ChainOriginDirectory`] can hold
/// it as `Arc<dyn OriginChainReads + Send + Sync>` (object-safe via
/// `#[async_trait]`). The production implementation is [`Contracts`]; tests
/// supply a scripted stub.
#[async_trait::async_trait]
trait OriginChainReads {
    /// Authoritative current operator set for `namespace` (`getOrigins`).
    async fn get_origins(&self, namespace: U256) -> Result<Vec<Address>>;
}

#[async_trait::async_trait]
impl<P> OriginChainReads for Contracts<P>
where
    P: Provider + Clone + Send + Sync,
{
    async fn get_origins(&self, namespace: U256) -> Result<Vec<Address>> {
        // Bounded here, in the production impl: a stalled provider must fail
        // this single lookup rather than hang the caller's DHT-miss fallback
        // path indefinitely. `StubReads` needs no timeout.
        timed(None, "getOrigins", self.origin.getOrigins(namespace).call())
            .await
            .with_context(|| format!("getOrigins(namespace={namespace})"))
    }
}

/// Chain-backed origin directory: a lazy TTL cache over `getOrigins`, resolving
/// operators to active `NodeId`s against the shared capacity-bond reverse
/// projection. No background watcher and no whole-directory mirror — a single
/// `getOrigins(ns)` RPC fires only on a cold-namespace miss, and chain reads
/// therefore scale with the namespaces this node is actually asked to serve,
/// not the permissionless global namespace count.
pub struct ChainOriginDirectory {
    reads: Arc<dyn OriginChainReads + Send + Sync>,
    cache: LazyOriginCache,
    operator_to_node: Arc<RwLock<HashMap<Address, NodeId>>>,
    staker_set: Arc<dyn StakerSet>,
    metrics: Arc<Metrics>,
}

impl std::fmt::Debug for ChainOriginDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainOriginDirectory")
            .finish_non_exhaustive()
    }
}

impl ChainOriginDirectory {
    /// Build a directory over the given `OriginAssignment` contract. No RPC:
    /// the cache is lazy and populates on the first lookup miss for each
    /// namespace.
    #[allow(clippy::too_many_arguments)]
    pub fn new<P>(
        provider: P,
        origin_assignment_addr: Address,
        operator_to_node: Arc<RwLock<HashMap<Address, NodeId>>>,
        staker_set: Arc<dyn StakerSet>,
        cache_capacity: usize,
        positive_ttl: Duration,
        negative_ttl: Duration,
        metrics: Arc<Metrics>,
    ) -> Self
    where
        P: Provider + Clone + Send + Sync + 'static,
    {
        let reads = Arc::new(Contracts {
            origin: OriginAssignment::new(origin_assignment_addr, provider),
        });
        Self {
            reads,
            cache: LazyOriginCache::new(cache_capacity, positive_ttl, negative_ttl),
            operator_to_node,
            staker_set,
            metrics,
        }
    }
}

#[async_trait::async_trait]
impl OriginDirectory for ChainOriginDirectory {
    async fn lookup_origins(&self, namespace_id: U256) -> Vec<NodeId> {
        // Namespace 0 (NO_NAMESPACE) authorizes nothing by construction — never
        // fetch or cache it (ADR 002 §Namespace 0).
        if namespace_id == U256::ZERO {
            return Vec::new();
        }
        // Live TTL hit (positive OR negative) → resolve from the cached operator
        // set with no RPC.
        if let Some(operators) = self.cache.get(&namespace_id) {
            return resolve_active(&operators, &self.operator_to_node, self.staker_set.as_ref());
        }
        // Cold miss: one on-demand getOrigins. On RPC error, fail closed
        // (resolve nothing) and DO NOT cache — a transient failure must not be
        // frozen for a TTL, and the next request retries.
        let operators = match self.reads.get_origins(namespace_id).await {
            Ok(ops) => ops,
            Err(err) => {
                self.metrics.origin_directory_get_origins_failure();
                warn!(
                    err = %sanitize_err_chain(&err),
                    %namespace_id,
                    "getOrigins lookup failed; resolving no origins for this request"
                );
                return Vec::new();
            }
        };
        // Cache the authoritative set (empty → negative entry, short TTL).
        self.cache.insert(namespace_id, operators.clone());
        self.metrics.origin_directory_cache_size(self.cache.len());
        resolve_active(&operators, &self.operator_to_node, self.staker_set.as_ref())
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
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
        ChainOriginDirectory {
            reads,
            cache: LazyOriginCache::new(8, positive_ttl, negative_ttl),
            operator_to_node: Arc::new(RwLock::new(operator_to_node)),
            staker_set,
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
}
