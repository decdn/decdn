//! Requester-side iterative `FindValue` lookup (ADR 022
//! §`FIND_VALUE` Flow + §Lookup integrity).
//!
//! Wraps the per-RPC primitive [`super::client::find_value`] in a
//! standard Kademlia iteration with α=3 parallelism, three response
//! filters applied in order, and provider-set randomisation before
//! return. The lookup returns the *raw* surviving provider set;
//! ranking / probing belongs to the downstream caller.
//!
//! ## Termination
//!
//! Stops as soon as **either**:
//! - `providers.len() >= k` (saturation — caller has enough to probe), OR
//! - a full round completes without surfacing any candidate strictly
//!   closer to `target` than the current best queried node
//!   (convergence — no further progress possible).
//!
//! Mirrors the textbook Kademlia termination. The empty-providers
//! convergence case lets the caller fall back to the on-chain
//! origin-directory ([`super::OriginDirectory`]) per ADR 022 §192.
//!
//! ## Three filters (ADR 022 §180–186)
//!
//! Applied in order to **every** `FindValueResponse`:
//! 1. **XOR-distance** — drop `closer_nodes` entries whose distance
//!    to the target is not strictly less than the responder's own
//!    distance. Honest Kademlia responders never produce such entries.
//! 2. **Active-staker** — drop non-staked `NodeId`s from both
//!    `closer_nodes` and `providers`. The DHT routing pool is
//!    restricted to staked nodes by spec.
//! 3. **Negative probe cache** — drop `(provider, target)` pairs the
//!    requester has already observed returning `has_blob: false`
//!    within the cache TTL. Only applies to `providers`, never to
//!    `closer_nodes`.
//!
//! After filtering, the surviving provider set is **randomised**
//! before return. ADR 022 §186: "Randomization on the requester is
//! the load-bearing defense against ordering manipulation."

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::{Endpoint, EndpointAddr, PublicKey};
use rand::seq::SliceRandom;
use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::dht::client;
use crate::dht::negative_cache::{Hash, NegativeProbeCache};
use crate::dht::routing::{K_BUCKET_SIZE, NodeId, RoutingTable, xor_distance};
use crate::dht::staker_set::StakerSet;

/// Default α — parallel in-flight RPCs per round. ADR 022 §99.
pub const DEFAULT_ALPHA: usize = 3;
/// Default K — providers-accumulated saturation cap. ADR 022 §99.
pub const DEFAULT_K: usize = K_BUCKET_SIZE;
/// Default round timeout. Belt over the per-RPC 8s timeout in
/// [`super::client`]; bounds the wall-clock budget for a single
/// iteration even if a few peers hang.
pub const DEFAULT_ROUND_TIMEOUT: Duration = Duration::from_secs(8);

/// Lookup tuning knobs.
#[derive(Debug, Clone, Copy)]
pub struct LookupConfig {
    /// Parallel in-flight RPCs per round.
    pub alpha: usize,
    /// Stop after this many providers accumulated.
    pub k: usize,
    /// Per-round timeout — applied around the `JoinSet` drain so a
    /// slow / hanging peer doesn't wedge the iteration.
    pub round_timeout: Duration,
}

impl Default for LookupConfig {
    fn default() -> Self {
        Self {
            alpha: DEFAULT_ALPHA,
            k: DEFAULT_K,
            round_timeout: DEFAULT_ROUND_TIMEOUT,
        }
    }
}

/// Run the iterative `FindValue` lookup for `target`.
///
/// Returns the randomised, deduplicated, filter-survived provider
/// list. An empty return means convergence with no usable providers
/// — the caller should fall back to the on-chain origin directory
/// (ADR 022 §192) if applicable.
///
/// The function never panics or returns `Err`: a transport-level
/// failure on any single peer is treated like an empty response
/// (logged at `debug!`). The lookup is best-effort by spec; total
/// failure manifests as an empty return.
pub async fn find_providers(
    endpoint: &Endpoint,
    routing_table: &Arc<Mutex<RoutingTable>>,
    staker_set: &Arc<dyn StakerSet>,
    negative_cache: &NegativeProbeCache,
    requester_id: NodeId,
    target: Hash,
    cfg: LookupConfig,
) -> Vec<NodeId> {
    let ctx = LookupCtx {
        endpoint,
        staker_set: staker_set.as_ref(),
        negative_cache,
        requester_id,
        target,
        cfg,
    };
    let mut state = LookupState::new(routing_table, &target, requester_id, cfg);

    loop {
        if state.have_enough_providers() {
            break;
        }
        let batch = state.pick_round_batch();
        if batch.is_empty() {
            break;
        }
        let observed_closer = run_round(&ctx, &batch, &mut state).await;
        if !observed_closer {
            break;
        }
    }

    state.into_randomised_providers()
}

/// Shared inputs to a lookup round — bundled here so `run_round` /
/// `process_response` don't have to thread eight arguments each.
struct LookupCtx<'a> {
    endpoint: &'a Endpoint,
    staker_set: &'a dyn StakerSet,
    negative_cache: &'a NegativeProbeCache,
    requester_id: NodeId,
    target: Hash,
    cfg: LookupConfig,
}

/// Run one round of α parallel `find_value` RPCs against `batch`,
/// applying the three filters to each response and folding results
/// into `state`. Returns whether the round surfaced any candidate
/// strictly closer to the target than the previous best queried
/// distance (the convergence signal).
async fn run_round(ctx: &LookupCtx<'_>, batch: &[NodeId], state: &mut LookupState) -> bool {
    let mut tasks: JoinSet<(
        NodeId,
        anyhow::Result<decdn_protocol::dht::FindValueResponse>,
    )> = JoinSet::new();
    for &peer in batch {
        let endpoint = ctx.endpoint.clone();
        let requester_id = ctx.requester_id;
        let target = ctx.target;
        tasks.spawn(async move {
            let Ok(pk) = PublicKey::from_bytes(&peer) else {
                return (
                    peer,
                    Err(anyhow::anyhow!(
                        "peer NodeId is not a valid Ed25519 public key"
                    )),
                );
            };
            let addr = EndpointAddr::new(pk);
            let result = client::find_value(&endpoint, addr, target, requester_id).await;
            (peer, result)
        });
    }

    let mut observed_closer = false;
    let drain = async {
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok((responder, Ok(resp))) => {
                    if process_response(ctx, resp, responder, state) {
                        observed_closer = true;
                    }
                }
                Ok((responder, Err(err))) => {
                    debug!(?responder, %err, "dht lookup: find_value RPC failed");
                }
                Err(err) => {
                    warn!(%err, "dht lookup: round task panicked");
                }
            }
        }
    };

    if tokio::time::timeout(ctx.cfg.round_timeout, drain)
        .await
        .is_err()
    {
        warn!(
            timeout_ms = u64::try_from(ctx.cfg.round_timeout.as_millis()).unwrap_or(u64::MAX),
            "dht lookup: round timeout fired; aborting in-flight RPCs"
        );
        tasks.abort_all();
    }

    observed_closer
}

/// Apply the three lookup-integrity filters to one response and
/// fold survivors into `state`. Returns whether any surviving
/// `closer_node` is strictly closer to `target` than the previous
/// best queried distance.
fn process_response(
    ctx: &LookupCtx<'_>,
    resp: decdn_protocol::dht::FindValueResponse,
    responder: NodeId,
    state: &mut LookupState,
) -> bool {
    let kept_closer = filter_xor_closer(resp.closer_nodes, &ctx.target, &responder);
    let kept_closer = filter_active_stakers(kept_closer, ctx.staker_set);
    let kept_providers = filter_active_stakers(resp.providers, ctx.staker_set);
    let kept_providers = filter_negative_cache(kept_providers, &ctx.target, ctx.negative_cache);

    for p in kept_providers {
        state.record_provider(p);
    }
    let mut observed_closer = false;
    for c in kept_closer {
        if c == ctx.requester_id {
            continue;
        }
        if state.add_candidate(c) {
            observed_closer = true;
        }
    }
    observed_closer
}

// ============================================================
// Filter helpers (ADR 022 §180–186) — pure free functions.
// ============================================================

/// Filter 1: drop `closer_nodes` entries whose XOR distance to
/// `target` is not strictly less than `responder`'s own distance.
/// Honest Kademlia responders never produce such entries; an
/// offending entry is either responder misbehaviour or a relay-induced
/// stale value.
#[must_use]
pub fn filter_xor_closer(
    closer_nodes: Vec<NodeId>,
    target: &Hash,
    responder: &NodeId,
) -> Vec<NodeId> {
    let responder_distance = xor_distance(responder, target);
    closer_nodes
        .into_iter()
        .filter(|n| xor_distance(n, target) < responder_distance)
        .collect()
}

/// Filter 2: drop non-staked `NodeId`s. Applies symmetrically to
/// `closer_nodes` and `providers` (the caller invokes once per
/// field). The DHT routing pool is restricted to staked nodes per
/// ADR 022 §183.
#[must_use]
pub fn filter_active_stakers(nodes: Vec<NodeId>, staker_set: &dyn StakerSet) -> Vec<NodeId> {
    nodes
        .into_iter()
        .filter(|n| staker_set.is_active(n))
        .collect()
}

/// Filter 3: drop `(provider, target)` pairs already present in the
/// negative probe cache. Only applies to `providers` — a peer that
/// previously denied holding `target` may still legitimately route
/// `closer_nodes` for it.
#[must_use]
pub fn filter_negative_cache(
    providers: Vec<NodeId>,
    target: &Hash,
    cache: &NegativeProbeCache,
) -> Vec<NodeId> {
    providers
        .into_iter()
        .filter(|p| !cache.contains_active(p, target))
        .collect()
}

// ============================================================
// Lookup state machine
// ============================================================

/// Candidate set + queried tracking + accumulated providers for a
/// single iterative lookup. Distance to `target` is computed once
/// on insertion and used as the key for closest-first iteration.
struct LookupState {
    target: Hash,
    /// Distance → `NodeId`, sorted ascending. `BTreeMap` gives O(log n)
    /// insert and `.iter().next()` for "closest unqueried" in O(log n).
    candidates: BTreeMap<[u8; NODE_ID_LEN], NodeId>,
    queried: HashSet<NodeId>,
    /// Survivor set, deduplicated. Insertion order preserved here;
    /// randomisation happens at return time.
    providers: Vec<NodeId>,
    providers_seen: HashSet<NodeId>,
    /// Smallest XOR distance among queried nodes so far. New
    /// candidates only count as "closer" if they beat this.
    best_queried_distance: [u8; NODE_ID_LEN],
    k: usize,
    alpha: usize,
}

impl LookupState {
    fn new(
        routing_table: &Arc<Mutex<RoutingTable>>,
        target: &Hash,
        requester_id: NodeId,
        cfg: LookupConfig,
    ) -> Self {
        let mut state = Self {
            target: *target,
            candidates: BTreeMap::new(),
            queried: HashSet::new(),
            providers: Vec::new(),
            providers_seen: HashSet::new(),
            best_queried_distance: [0xFFu8; NODE_ID_LEN],
            k: cfg.k,
            alpha: cfg.alpha,
        };
        // Seed from the local routing table. Take more than α so the
        // first round has alternates if the closest few don't answer.
        let seed = match routing_table.lock() {
            Ok(t) => t.closest(target, cfg.alpha.saturating_mul(4)),
            Err(poisoned) => {
                warn!("dht lookup: routing table mutex poisoned; recovering inner state");
                poisoned
                    .into_inner()
                    .closest(target, cfg.alpha.saturating_mul(4))
            }
        };
        for peer in seed {
            if peer != requester_id {
                state.add_candidate(peer);
            }
        }
        state
    }

    /// Insert `peer` into the candidate set if not already present
    /// and not already queried. Returns whether the insertion brought
    /// in a candidate strictly closer than the current best queried
    /// distance (the convergence-tracking signal).
    fn add_candidate(&mut self, peer: NodeId) -> bool {
        if self.queried.contains(&peer) || self.candidates.values().any(|v| v == &peer) {
            return false;
        }
        let dist = xor_distance(&peer, &self.target);
        let strictly_closer = dist < self.best_queried_distance;
        self.candidates.insert(dist, peer);
        strictly_closer
    }

    /// Pick up to α unqueried candidates ordered by closeness, mark
    /// them queried, return the batch. The closest is at the front
    /// of the returned vec.
    fn pick_round_batch(&mut self) -> Vec<NodeId> {
        let mut picked = Vec::with_capacity(self.alpha);
        while picked.len() < self.alpha {
            let Some((&dist, &peer)) = self.candidates.iter().next() else {
                break;
            };
            self.candidates.remove(&dist);
            self.queried.insert(peer);
            if dist < self.best_queried_distance {
                self.best_queried_distance = dist;
            }
            picked.push(peer);
        }
        picked
    }

    fn record_provider(&mut self, peer: NodeId) {
        if self.providers_seen.insert(peer) {
            self.providers.push(peer);
        }
    }

    const fn have_enough_providers(&self) -> bool {
        self.providers.len() >= self.k
    }

    /// Consume state, return the randomised provider list capped at K.
    /// ADR 022 §186: randomisation is mandatory before probing.
    fn into_randomised_providers(mut self) -> Vec<NodeId> {
        self.providers.shuffle(&mut rand::rng());
        self.providers.truncate(self.k);
        self.providers
    }
}

use crate::dht::routing::NODE_ID_LEN;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use crate::dht::staker_set::ConfigStakerSet;
    use std::collections::HashSet as StdHashSet;

    fn nid(byte: u8) -> NodeId {
        [byte; 32]
    }
    fn h(byte: u8) -> Hash {
        [byte; 32]
    }

    /// `filter_xor_closer` keeps entries strictly closer than the
    /// responder; drops entries at-or-beyond the responder's distance.
    #[test]
    fn filter_xor_closer_drops_at_or_beyond_responder_distance() {
        // target = 0x00…00. distance from peer X is just X itself.
        let target = h(0);
        // responder at distance 0x10 (closer = lower XOR).
        let responder = nid(0x10);
        // closer_nodes: 0x05 (closer), 0x10 (equal — drop), 0x20 (further — drop), 0x01 (closer).
        let input = vec![nid(0x05), nid(0x10), nid(0x20), nid(0x01)];
        let kept = filter_xor_closer(input, &target, &responder);
        // Only strictly-closer survives.
        let kept_set: StdHashSet<NodeId> = kept.into_iter().collect();
        assert_eq!(kept_set, StdHashSet::from([nid(0x05), nid(0x01)]));
    }

    /// `filter_active_stakers` drops non-staked `NodeId`s; keeps staked.
    #[test]
    fn filter_active_stakers_drops_non_staked() {
        let mut staked = StdHashSet::new();
        staked.insert(nid(1));
        staked.insert(nid(3));
        let set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(staked));
        let input = vec![nid(1), nid(2), nid(3), nid(4)];
        let kept = filter_active_stakers(input, set.as_ref());
        assert_eq!(kept, vec![nid(1), nid(3)]);
    }

    /// `filter_negative_cache` drops only entries the cache flags
    /// active for the given target — leaves others untouched.
    #[test]
    fn filter_negative_cache_drops_only_active_pairs_for_target() {
        let cache = NegativeProbeCache::new();
        let target = h(0xAA);
        cache.record_failure(nid(1), target);
        cache.record_failure(nid(2), h(0xBB)); // different target → no effect

        let input = vec![nid(1), nid(2), nid(3)];
        let kept = filter_negative_cache(input, &target, &cache);
        assert_eq!(kept, vec![nid(2), nid(3)]);
    }

    /// `LookupState::pick_round_batch` returns up to α candidates
    /// ordered closest-first, and marks them queried so a second
    /// pick doesn't return the same peers.
    #[test]
    fn lookup_state_pick_returns_closest_alpha_marks_queried() {
        // Empty routing table; we'll manually seed candidates.
        let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
        let target = h(0);
        let cfg = LookupConfig {
            alpha: 2,
            k: 20,
            round_timeout: Duration::from_secs(1),
        };
        let mut state = LookupState::new(&routing, &target, nid(0xFF), cfg);
        // Inject candidates manually.
        state.add_candidate(nid(0x10));
        state.add_candidate(nid(0x02));
        state.add_candidate(nid(0x40));

        let first = state.pick_round_batch();
        assert_eq!(first, vec![nid(0x02), nid(0x10)]);
        let second = state.pick_round_batch();
        assert_eq!(second, vec![nid(0x40)]);
        let third = state.pick_round_batch();
        assert!(third.is_empty());
    }

    /// `LookupState::add_candidate` rejects already-queried peers
    /// and reports whether a newcomer is strictly closer.
    #[test]
    fn lookup_state_add_candidate_tracks_strictly_closer() {
        let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
        let target = h(0);
        let cfg = LookupConfig::default();
        let mut state = LookupState::new(&routing, &target, nid(0xFF), cfg);

        // First add: best is "infinity" (0xFF…FF), so any peer is closer.
        assert!(state.add_candidate(nid(0x80)));
        // Query it, then add a closer peer — still strictly closer.
        let _ = state.pick_round_batch();
        assert!(state.add_candidate(nid(0x40)));
        // A peer not closer than best_queried (0x80 → distance 0x80…80)
        // — re-adding 0xC0 is further than 0x80, so not closer.
        assert!(!state.add_candidate(nid(0xC0)));
    }

    /// `LookupState::record_provider` dedupes.
    #[test]
    fn lookup_state_record_provider_dedupes() {
        let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
        let cfg = LookupConfig::default();
        let mut state = LookupState::new(&routing, &h(0), nid(0xFF), cfg);
        state.record_provider(nid(1));
        state.record_provider(nid(2));
        state.record_provider(nid(1));
        assert_eq!(state.providers, vec![nid(1), nid(2)]);
    }

    /// `into_randomised_providers` returns all providers (≤ k) and
    /// truncates beyond k. We verify ordering can differ across calls
    /// to catch a deterministic regression — the assertion is "not
    /// always equal", not "always different".
    #[test]
    fn into_randomised_providers_truncates_to_k_and_shuffles() {
        let mut deterministic_count = 0;
        let canonical = (1u8..=20).map(nid).collect::<Vec<_>>();
        for _ in 0..16 {
            let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
            let cfg = LookupConfig {
                alpha: 3,
                k: 10,
                round_timeout: Duration::from_secs(1),
            };
            let mut state = LookupState::new(&routing, &h(0), nid(0xFF), cfg);
            for &p in &canonical {
                state.record_provider(p);
            }
            let out = state.into_randomised_providers();
            assert_eq!(out.len(), 10);
            if out == canonical[..10] {
                deterministic_count += 1;
            }
        }
        // With 20 providers truncated to 10 and shuffled, the
        // probability of seeing the canonical order across all 16
        // runs is vanishingly small (factorially small). One
        // deterministic match is plausible; 16/16 is not.
        assert!(
            deterministic_count < 16,
            "providers were never shuffled across 16 calls — randomisation broken"
        );
    }
}
