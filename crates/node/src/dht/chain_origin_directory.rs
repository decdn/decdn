//! Chain-backed [`OriginDirectory`] implementation.
//!
//! Resolves a request's **namespace** to the set of currently-active operator
//! `NodeId`s authorised as origins for it. The resolution chain (ADR 022 §
//! FIND\_VALUE Flow "Origin discovery", ADR 011 § Origin Assignment
//! Authority) is:
//!
//!   `OriginAssignment.getOrigins(namespaceId)` → `[operator...]`
//!     → drop operators the local [`ContentDenylist`] denies (the origin ∪
//!       operator blacklist union the watcher syncs from `ContentBlacklist`,
//!       plus the operator's own `[content] denied_origins`)
//!     → capacity-bond reverse projection (`operator → NodeId`, binding only)
//!     → keep operators the shared [`StakerSet`] reports `active`
//!
//! The blacklist drop is this node protecting itself: `getOrigins` is a routing
//! hint the contract does not filter (ADR 011 § Interaction with
//! `ContentBlacklist`), so the consumer applies the same deny-set its delivery
//! gate already holds rather than trusting the seat set to be blacklist-clean.
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
use crate::content_deny::ContentDenylist;
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
    /// The node's live origin deny-set — the same union the delivery gate
    /// consults. A blacklisted operator is dropped from every resolved origin
    /// set, so this node never routes an origin-pull to one. Read live (never
    /// cached), so a governance blacklist takes effect without waiting out the
    /// `getOrigins` TTL.
    content_deny: Arc<ContentDenylist>,
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
        content_deny: Arc<ContentDenylist>,
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
            content_deny,
            metrics,
        }
    }

    /// Resolve a cached/fetched operator address set to active origin
    /// `NodeId`s, first dropping any operator the local deny-set blacklists.
    /// Both the blacklist filter and liveness are applied live at read time, so
    /// a TTL-anchored cache entry never keeps routing to an operator that has
    /// since been blacklisted or gone inactive.
    fn resolve(&self, operators: &[Address]) -> Vec<NodeId> {
        let allowed: Vec<Address> = operators
            .iter()
            .copied()
            .filter(|op| !self.content_deny.is_origin_denied(op))
            .collect();
        resolve_active(&allowed, &self.operator_to_node, self.staker_set.as_ref())
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
            return self.resolve(&operators);
        }
        // Cold miss: one on-demand getOrigins. On RPC error, fail closed
        // (resolve nothing) and DO NOT cache — a transient failure must not be
        // frozen for a TTL, and the next request retries.
        let operators = match self.reads.get_origins(namespace_id).await {
            Ok(ops) => ops,
            Err(err) => {
                self.metrics.origin_directory_get_origins_failure();
                warn!(
                    error = %sanitize_err_chain(&err),
                    %namespace_id,
                    "getOrigins lookup failed; resolving no origins for this request"
                );
                return Vec::new();
            }
        };
        // Cache the authoritative set (empty → negative entry, short TTL).
        self.cache.insert(namespace_id, operators.clone());
        self.metrics.origin_directory_cache_size(self.cache.len());
        self.resolve(&operators)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests;
