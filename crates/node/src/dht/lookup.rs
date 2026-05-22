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

use indexmap::IndexSet;
use iroh::{Endpoint, EndpointAddr, PublicKey};
use rand::seq::SliceRandom;
use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::dht::client;
use crate::dht::negative_cache::{Hash, NegativeProbeCache};
use crate::dht::routing::{K_BUCKET_SIZE, NODE_ID_LEN, NodeId, RoutingTable, xor_distance};
use crate::dht::staker_set::StakerSet;

/// Default α — parallel in-flight RPCs per round. ADR 022 § Routing Table.
pub const DEFAULT_ALPHA: usize = 3;
/// Default K — providers-accumulated saturation cap. ADR 022 § Routing Table.
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

/// Clamp zero values that would otherwise silently turn the lookup
/// into a no-op (`alpha == 0` → empty batches; `k == 0` → instant
/// "saturation" before any round runs). Emits a `warn!` so the
/// misconfiguration is surfaced at runtime rather than hidden behind
/// an empty result.
fn sanitize_config(cfg: LookupConfig) -> LookupConfig {
    let alpha = cfg.alpha.max(1);
    let k = cfg.k.max(1);
    if alpha != cfg.alpha || k != cfg.k {
        warn!(
            cfg_alpha = cfg.alpha,
            cfg_k = cfg.k,
            "dht lookup: LookupConfig had zero alpha/k; clamped to 1"
        );
    }
    LookupConfig {
        alpha,
        k,
        round_timeout: cfg.round_timeout,
    }
}

/// Run the iterative `FindValue` lookup for `target`.
///
/// Returns the randomised, deduplicated, filter-survived provider
/// list. An empty return means convergence with no usable providers
/// — the caller should fall back to the on-chain origin directory
/// (ADR 022 §192) if applicable.
///
/// The function never panics or returns `Err`. Per-peer RPC
/// failures are classified: transport-level failures are absorbed
/// as empty responses and logged at `debug!`; an invalid Ed25519
/// `NodeId` (which implies state corruption upstream) is logged at
/// `warn!`; round-timeout aborts log the aborted-peer count at
/// `warn!`. The lookup is best-effort by spec; total failure
/// manifests as an empty return.
pub async fn find_providers(
    endpoint: &Endpoint,
    routing_table: &Arc<Mutex<RoutingTable>>,
    staker_set: &Arc<dyn StakerSet>,
    negative_cache: &NegativeProbeCache,
    requester_id: NodeId,
    target: Hash,
    cfg: LookupConfig,
) -> Vec<NodeId> {
    let cfg = sanitize_config(cfg);
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

/// Per-peer error category surfaced by a single RPC attempt within a
/// round. Distinguishes "this `NodeId` failed to decode as an
/// Ed25519 public key" — which implies state-integrity damage
/// upstream (routing table or filter pipeline let an invalid id
/// through) and deserves a `warn!` — from a generic transport-level
/// RPC failure, which is the expected best-effort outcome per the
/// function docstring and gets logged at `debug!`.
enum RoundRpcError {
    InvalidPubKey,
    Transport(anyhow::Error),
}

/// Shared inputs to a lookup round.
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
    type RoundResult = (
        NodeId,
        Result<decdn_protocol::dht::FindValueResponse, RoundRpcError>,
    );
    let mut tasks: JoinSet<RoundResult> = JoinSet::new();
    for &peer in batch {
        let endpoint = ctx.endpoint.clone();
        let requester_id = ctx.requester_id;
        let target = ctx.target;
        tasks.spawn(async move {
            let Ok(pk) = PublicKey::from_bytes(&peer) else {
                return (peer, Err(RoundRpcError::InvalidPubKey));
            };
            let addr = EndpointAddr::new(pk);
            let result = client::find_value(&endpoint, addr, target, requester_id)
                .await
                .map_err(RoundRpcError::Transport);
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
                Ok((responder, Err(RoundRpcError::InvalidPubKey))) => {
                    // Bytes already survived the routing-table seed
                    // and/or three response filters; failing Ed25519
                    // decode here points at state corruption upstream.
                    warn!(
                        ?responder,
                        "dht lookup: peer NodeId is not a valid Ed25519 public key"
                    );
                }
                Ok((responder, Err(RoundRpcError::Transport(err)))) => {
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
        // After the drain future drops, `tasks` still holds the
        // handles for peers that hadn't been joined yet — these are
        // the ones being aborted. Logging the count lets a future
        // dashboard distinguish "round drained cleanly" from "round
        // timed out with N peers in flight."
        let aborted = tasks.len();
        warn!(
            aborted_peers = aborted,
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
    fold_response(
        &ctx.target,
        ctx.staker_set,
        ctx.negative_cache,
        resp,
        responder,
        state,
    )
}

/// Filter + fold step shared with the unit tests, which can't easily
/// stand up an `iroh::Endpoint` to populate a full `LookupCtx`. Pure
/// over its inputs.
fn fold_response(
    target: &Hash,
    staker_set: &dyn StakerSet,
    negative_cache: &NegativeProbeCache,
    resp: decdn_protocol::dht::FindValueResponse,
    responder: NodeId,
    state: &mut LookupState,
) -> bool {
    let kept_closer = filter_xor_closer(resp.closer_nodes, target, &responder);
    let kept_closer = filter_active_stakers(kept_closer, staker_set);
    let kept_providers = filter_active_stakers(resp.providers, staker_set);
    let kept_providers = filter_negative_cache(kept_providers, target, negative_cache);

    for p in kept_providers {
        state.record_provider(p);
    }
    let mut observed_closer = false;
    for c in kept_closer {
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
fn filter_xor_closer(closer_nodes: Vec<NodeId>, target: &Hash, responder: &NodeId) -> Vec<NodeId> {
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
fn filter_active_stakers(nodes: Vec<NodeId>, staker_set: &dyn StakerSet) -> Vec<NodeId> {
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
fn filter_negative_cache(
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
    /// Identity of the local node. Used to reject self from the
    /// candidate / provider sets — self-filtering lives here, on the
    /// state object, so a future caller can't accidentally pollute
    /// the state via a code path that forgets the guard.
    requester_id: NodeId,
    /// Distance → `NodeId`, sorted ascending by XOR distance so
    /// `iter().next()` yields the closest unqueried candidate.
    candidates: BTreeMap<[u8; NODE_ID_LEN], NodeId>,
    queried: HashSet<NodeId>,
    /// Survivor set, deduplicated. Insertion order preserved;
    /// randomisation happens at return time.
    providers: IndexSet<NodeId>,
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
            requester_id,
            candidates: BTreeMap::new(),
            queried: HashSet::new(),
            providers: IndexSet::new(),
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
            state.add_candidate(peer);
        }
        state
    }

    /// Insert `peer` into the candidate set if not already present
    /// and not already queried. Returns whether the insertion brought
    /// in a candidate strictly closer than the current best queried
    /// distance (the convergence-tracking signal). Rejects
    /// `requester_id`.
    fn add_candidate(&mut self, peer: NodeId) -> bool {
        if peer == self.requester_id {
            return false;
        }
        let dist = xor_distance(&peer, &self.target);
        // XOR distance is bijective for a fixed target, so
        // `contains_key(&dist)` is equivalent to scanning values for
        // `peer` — but O(log N) instead of O(N).
        if self.queried.contains(&peer) || self.candidates.contains_key(&dist) {
            return false;
        }
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

    /// Insert `peer` into the provider survivor set if not already
    /// present. Rejects `requester_id`. `IndexSet::insert` handles
    /// dedup and insertion-order preservation in one step.
    fn record_provider(&mut self, peer: NodeId) {
        if peer == self.requester_id {
            return;
        }
        self.providers.insert(peer);
    }

    fn have_enough_providers(&self) -> bool {
        self.providers.len() >= self.k
    }

    /// Consume state, return the surviving provider set with order
    /// randomised, then truncated to K. ADR 022 §186 requires
    /// randomisation of the *surviving* set — shuffle precedes
    /// truncation so the truncated K is a random sample, not a
    /// deterministic prefix.
    fn into_randomised_providers(self) -> Vec<NodeId> {
        let mut providers: Vec<NodeId> = self.providers.into_iter().collect();
        providers.shuffle(&mut rand::rng());
        providers.truncate(self.k);
        providers
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
    use crate::dht::staker_set::ConfigStakerSet;
    use std::collections::HashSet as StdHashSet;

    fn nid(byte: u8) -> NodeId {
        [byte; 32]
    }
    fn h(byte: u8) -> Hash {
        [byte; 32]
    }

    #[test]
    fn filter_xor_closer_drops_at_or_beyond_responder_distance() {
        // target = 0x00…00. Distance from peer X is just X itself.
        let target = h(0);
        let responder = nid(0x10);
        // 0x05/0x01 closer; 0x10 equal → drop; 0x20 further → drop.
        let input = vec![nid(0x05), nid(0x10), nid(0x20), nid(0x01)];
        let kept = filter_xor_closer(input, &target, &responder);
        let kept_set: StdHashSet<NodeId> = kept.into_iter().collect();
        assert_eq!(kept_set, StdHashSet::from([nid(0x05), nid(0x01)]));
    }

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

    #[test]
    fn lookup_state_pick_returns_closest_alpha_marks_queried() {
        let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
        let target = h(0);
        let cfg = LookupConfig {
            alpha: 2,
            k: 20,
            round_timeout: Duration::from_secs(1),
        };
        let mut state = LookupState::new(&routing, &target, nid(0xFF), cfg);
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

    #[test]
    fn lookup_state_add_candidate_tracks_strictly_closer() {
        let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
        let target = h(0);
        let cfg = LookupConfig::default();
        let mut state = LookupState::new(&routing, &target, nid(0xFF), cfg);

        // best_queried_distance starts at 0xFF…FF, so any peer wins.
        assert!(state.add_candidate(nid(0x80)));
        let _ = state.pick_round_batch();
        assert!(state.add_candidate(nid(0x40)));
        // 0xC0 is further than 0x80, so not strictly closer.
        assert!(!state.add_candidate(nid(0xC0)));
    }

    #[test]
    fn lookup_state_record_provider_dedupes() {
        let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
        let cfg = LookupConfig::default();
        let mut state = LookupState::new(&routing, &h(0), nid(0xFF), cfg);
        state.record_provider(nid(1));
        state.record_provider(nid(2));
        state.record_provider(nid(1));
        let providers_in_order: Vec<NodeId> = state.providers.iter().copied().collect();
        assert_eq!(providers_in_order, vec![nid(1), nid(2)]);
    }

    #[test]
    fn into_randomised_providers_truncates_to_k_and_shuffles() {
        // Stronger than "not always canonical": require at least two
        // distinct orderings across 16 runs, so a swap-only-the-
        // last-two shuffle bug doesn't ship green.
        let canonical = (1u8..=20).map(nid).collect::<Vec<_>>();
        let mut seen_orderings: StdHashSet<Vec<NodeId>> = StdHashSet::new();
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
            assert!(
                out.iter().all(|p| canonical.contains(p)),
                "shuffle invented elements not in the canonical set"
            );
            seen_orderings.insert(out);
        }
        assert!(
            seen_orderings.len() >= 2,
            "providers came back in the same order across 16 runs — shuffle is degenerate or broken"
        );
    }

    /// ADR 022 §184 mandates that the negative-cache filter applies
    /// only to `providers`, never `closer_nodes`. A peer that
    /// previously denied holding `target` may still legitimately
    /// route towards it, so dropping it from `closer_nodes` would
    /// silently strand lookups whose only path passes through that
    /// peer. Pin the asymmetry against a future "tidy-up" that
    /// extends Filter-3 to both fields.
    #[test]
    fn fold_response_negative_cache_does_not_drop_closer_nodes() {
        let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
        let target = h(0);
        let cfg = LookupConfig::default();
        let mut state = LookupState::new(&routing, &target, nid(0xFF), cfg);

        let staked: StdHashSet<NodeId> = [nid(0x05), nid(0x10), nid(0x20)].into_iter().collect();
        let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(staked));

        let cache = NegativeProbeCache::new();
        // Both 0x05 and 0x10 are flagged negative for `target`.
        cache.record_failure(nid(0x05), target);
        cache.record_failure(nid(0x10), target);

        // Responder 0x20 returns 0x05 + 0x10 in BOTH fields. Per
        // ADR 022 §184 only `providers` is filtered.
        let resp = decdn_protocol::dht::FindValueResponse {
            hash: target,
            providers: vec![nid(0x05), nid(0x10)],
            closer_nodes: vec![nid(0x05), nid(0x10)],
        };
        fold_response(
            &target,
            staker_set.as_ref(),
            &cache,
            resp,
            nid(0x20),
            &mut state,
        );

        // Providers were dropped by Filter 3.
        assert_eq!(state.providers.len(), 0);
        // Closer_nodes survived — they appear as candidates.
        assert!(state.candidates.values().any(|v| v == &nid(0x05)));
        assert!(state.candidates.values().any(|v| v == &nid(0x10)));
    }

    /// A round can surface providers while no candidate is strictly
    /// closer than the current best. `fold_response` must still fold
    /// the providers into state, and the convergence signal it
    /// returns is independent of provider presence.
    #[test]
    fn fold_response_records_providers_even_when_no_closer_node() {
        let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
        let target = h(0);
        let cfg = LookupConfig::default();
        let mut state = LookupState::new(&routing, &target, nid(0xFF), cfg);

        let staked: StdHashSet<NodeId> = [nid(0x05), nid(0x07)].into_iter().collect();
        let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(staked));
        let cache = NegativeProbeCache::new();

        // Responder at 0x05 returns provider 0x07 and a closer_node
        // 0x07 that is NOT strictly closer than the responder
        // (0x07 > 0x05 in XOR to target=0x00), so Filter 1 drops it.
        let resp = decdn_protocol::dht::FindValueResponse {
            hash: target,
            providers: vec![nid(0x07)],
            closer_nodes: vec![nid(0x07)],
        };
        let observed_closer = fold_response(
            &target,
            staker_set.as_ref(),
            &cache,
            resp,
            nid(0x05),
            &mut state,
        );

        assert!(!observed_closer, "no candidate was strictly closer");
        assert_eq!(state.providers.len(), 1, "provider must still be folded");
        assert!(state.providers.contains(&nid(0x07)));
    }

    /// `have_enough_providers` is `>= k`, not `> k`. Pin the
    /// boundary so a regression at the comparison ships red.
    #[test]
    fn have_enough_providers_is_inclusive_at_k() {
        let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
        let cfg = LookupConfig {
            alpha: 3,
            k: 2,
            round_timeout: Duration::from_secs(1),
        };
        let mut state = LookupState::new(&routing, &h(0), nid(0xFF), cfg);
        assert!(!state.have_enough_providers());
        state.record_provider(nid(1));
        assert!(!state.have_enough_providers());
        state.record_provider(nid(2));
        assert!(state.have_enough_providers());
        state.record_provider(nid(3));
        assert!(state.have_enough_providers());
    }

    /// `add_candidate` and `record_provider` reject `requester_id`
    /// even if the wire layer somehow let it through. The self-
    /// filter lives on `LookupState` itself (not in
    /// `process_response`) precisely so this can't regress.
    #[test]
    fn lookup_state_rejects_self_in_candidates_and_providers() {
        let routing = Arc::new(Mutex::new(RoutingTable::new(nid(0))));
        let cfg = LookupConfig::default();
        let me = nid(0xAA);
        let mut state = LookupState::new(&routing, &h(0), me, cfg);

        // Both methods silently no-op for self.
        assert!(!state.add_candidate(me));
        assert!(state.candidates.is_empty());
        state.record_provider(me);
        assert!(state.providers.is_empty());

        // Non-self still flows through.
        assert!(state.add_candidate(nid(0x42)));
        state.record_provider(nid(0x43));
        assert_eq!(state.candidates.len(), 1);
        assert_eq!(state.providers.len(), 1);
    }

    /// `sanitize_config` clamps zero alpha / k to 1; otherwise a
    /// caller passing `LookupConfig::default()` with `..Default::default()`
    /// patterns could silently wedge the lookup.
    #[test]
    fn sanitize_config_clamps_zero_alpha_and_k_to_one() {
        let cfg = LookupConfig {
            alpha: 0,
            k: 0,
            round_timeout: Duration::from_secs(1),
        };
        let clamped = sanitize_config(cfg);
        assert_eq!(clamped.alpha, 1);
        assert_eq!(clamped.k, 1);
        assert_eq!(clamped.round_timeout, Duration::from_secs(1));
    }
}
