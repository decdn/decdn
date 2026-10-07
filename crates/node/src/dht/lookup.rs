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
//! origin-directory ([`super::OriginDirectory`]) per ADR 022 § `FIND_VALUE` Flow.
//!
//! ## Three filters (ADR 022 § Lookup integrity)
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
//! before return. ADR 022 § Lookup integrity: "Randomization on the requester is
//! the load-bearing defense against ordering manipulation."

use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use decdn_protocol::Coverage;
use indexmap::IndexMap;
use iroh::{Endpoint, EndpointAddr, PublicKey};
use rand::seq::SliceRandom;
use tokio::task::JoinSet;
use tracing::{Instrument as _, debug, warn};

use crate::dht::client;
use crate::dht::negative_cache::{Hash, NegativeProbeCache};
use crate::dht::routing::{K_BUCKET_SIZE, NODE_ID_LEN, NodeId, RoutingTable, xor_distance_bytes};
use crate::dht::staker_set::StakerSet;

/// Default α — parallel in-flight RPCs per round. ADR 022 § Routing Table.
pub const DEFAULT_ALPHA: NonZeroUsize = match NonZeroUsize::new(3) {
    Some(n) => n,
    None => NonZeroUsize::MIN,
};
/// Default K — providers-accumulated saturation cap. ADR 022 § Routing Table.
pub const DEFAULT_K: NonZeroUsize = match NonZeroUsize::new(K_BUCKET_SIZE) {
    Some(n) => n,
    None => NonZeroUsize::MIN,
};
/// Default round timeout. Belt over the per-RPC 8s timeout in
/// [`super::client`]; bounds the wall-clock budget for a single
/// iteration even if a few peers hang.
pub const DEFAULT_ROUND_TIMEOUT: Duration = Duration::from_secs(8);

/// Hard ceiling on the number of rounds one lookup may run (#1145 review).
///
/// Each round is individually bounded by `round_timeout`, but the LOOP was not: it exits on
/// convergence (no closer node observed, no candidates left, or K providers found), never on
/// a count. So the lookup's worst case was `round_timeout × (unbounded)`, and it runs INSIDE
/// the caller's outer pull deadline — which is derived from a slack term that has to know
/// what discovery can cost. An unbounded term cannot be budgeted for, and
/// [`crate::selection::PULL_THROUGH_OUTER_SLACK`] budgeted 10 s for something that could
/// exceed that in a single slow round.
///
/// Kademlia converges in `O(log n)` rounds. For the initial deployment target — tens of
/// nodes on a testnet — that is ~2-3 rounds, so this ceiling clears a healthy lookup while
/// bounding the pathological one, which is all a deadline needs. It is NOT generous at scale:
/// at thousands of nodes `O(log n)` with α-way fan-out is already ~10 rounds, ABOVE this cap
/// (`dht_lookup_round_ceiling` is the signal that the network outgrew the constant and it —
/// and `PULL_THROUGH_OUTER_SLACK`, derived from it — must be raised).
pub const MAX_LOOKUP_ROUNDS: u32 = 4;

/// Lookup tuning knobs. `alpha` and `k` are `NonZeroUsize` so the
/// type system rules out the silent-no-op states a zero would
/// produce — empty batches from `pick_round_batch`, instant
/// "saturation" from `have_enough_providers`. Caller-side clamping
/// belongs at the config-parse boundary, not at every lookup entry.
#[derive(Debug, Clone, Copy)]
pub struct LookupConfig {
    /// Parallel in-flight RPCs per round.
    pub alpha: NonZeroUsize,
    /// Stop after this many providers accumulated.
    pub k: NonZeroUsize,
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
/// (ADR 022 § `FIND_VALUE` Flow) if applicable.
///
/// The function never panics or returns `Err`. Per-peer RPC
/// failures are classified: transport-level failures are absorbed
/// as empty responses and logged at `debug!`; an invalid Ed25519
/// `NodeId` (which implies state corruption upstream) is logged at
/// `warn!`; round-timeout aborts log the aborted-peer count at
/// `warn!`. The lookup is best-effort by spec; total failure
/// manifests as an empty return.
/// `metrics` is optional so the DHT test suite can drive a lookup without standing one up;
/// the production caller always passes `Some`, and it is the only way the round ceiling
/// below becomes observable.
///
/// Runs inside a `dht_lookup` span that records the `rounds` run and the
/// `providers` found; each round's per-peer RPCs run inside it too.
// One argument over the threshold, and every one of them is a distinct collaborator the
// lookup genuinely needs. Bundling them into a struct would move the same list one level out
// without making any call site clearer.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    name = "dht_lookup",
    skip_all,
    fields(
        hash = %target,
        rounds = tracing::field::Empty,
        providers = tracing::field::Empty,
    )
)]
pub async fn find_providers(
    endpoint: &Endpoint,
    routing_table: &Arc<Mutex<RoutingTable>>,
    staker_set: &Arc<dyn StakerSet>,
    negative_cache: &NegativeProbeCache,
    requester_id: NodeId,
    target: Hash,
    cfg: LookupConfig,
    metrics: Option<&crate::metrics::Metrics>,
) -> Vec<(NodeId, Coverage)> {
    if let Some(metrics) = metrics {
        metrics.dht_findvalue_query();
    }
    let ctx = LookupCtx {
        endpoint,
        staker_set: staker_set.as_ref(),
        negative_cache,
        requester_id,
        target,
        cfg,
        metrics,
    };
    let mut state = LookupState::new(routing_table, &target, requester_id, cfg);

    let mut rounds_run: u32 = 0;
    for round in 0..MAX_LOOKUP_ROUNDS {
        if state.have_enough_providers() {
            break;
        }
        let batch = state.pick_round_batch();
        if batch.is_empty() {
            break;
        }
        rounds_run = rounds_run.saturating_add(1);
        let observed_closer = run_round(&ctx, &batch, &mut state).await;
        if !observed_closer {
            break;
        }
        // Cut off a lookup that keeps finding closer nodes without saturating. The loop used
        // to run until it converged, with no count bound, so its worst case was unbounded —
        // and it runs inside the caller's outer pull deadline, whose slack term has to know
        // what discovery can cost (#1145 review). Not an error: whatever providers we have
        // are still usable, and the pull proceeds with them.
        //
        // But it is not nothing either, and a `debug!` alone made it invisible at the
        // project's own default `RUST_LOG=info` (#1145 review). Truncating here shrinks the
        // candidate set that feeds `MAX_PROVIDER_ATTEMPTS`, so a node whose network is large
        // enough to hit the ceiling on EVERY lookup is systematically pulling from a worse
        // provider set than it should — and had no way to know. The counter makes "my ceiling
        // is too low for my network size" an observable fact rather than a guess.
        if round + 1 == MAX_LOOKUP_ROUNDS {
            if let Some(metrics) = metrics {
                metrics.dht_lookup_round_ceiling();
            }
            tracing::debug!(
                rounds = MAX_LOOKUP_ROUNDS,
                providers = state.provider_count(),
                "dht lookup: hit the round ceiling; proceeding with the providers found so far"
            );
        }
    }

    let providers = state.into_randomised_providers();
    let span = tracing::Span::current();
    span.record("rounds", rounds_run);
    span.record("providers", providers.len());
    providers
}

/// Per-peer error category surfaced by a single RPC attempt within a
/// round. Distinguishes:
///
/// - **`InvalidPubKey`** — the candidate `NodeId` failed Ed25519
///   decode. The bytes already survived the routing-table seed
///   (see [`LookupState::new`]) and/or three response filters, so
///   reaching this arm implies one of two upstream paths leaked an
///   invalid id: a corrupt routing table, or a staker-set seeded
///   with malformed `NodeId` bytes. Either is actionable and gets a
///   `warn!`.
/// - **`Transport(_)`** — a generic transport-level RPC failure
///   (connect timeout, stream drop, malformed response decode).
///   Expected best-effort outcome per the function docstring;
///   logged at `debug!`.
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
    metrics: Option<&'a crate::metrics::Metrics>,
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
        tasks.spawn(
            async move {
                let Ok(pk) = PublicKey::from_bytes(peer.as_bytes()) else {
                    return (peer, Err(RoundRpcError::InvalidPubKey));
                };
                let addr = EndpointAddr::new(pk);
                let result = client::find_value(&endpoint, addr, target, requester_id)
                    .await
                    .map_err(RoundRpcError::Transport);
                (peer, result)
            }
            .in_current_span(),
        );
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
                        peer = %responder,
                        "dht lookup: peer NodeId is not a valid Ed25519 public key"
                    );
                }
                Ok((responder, Err(RoundRpcError::Transport(err)))) => {
                    debug!(peer = %responder, error = %err, "dht lookup: find_value RPC failed");
                }
                Err(err) => {
                    warn!(error = %err, "dht lookup: round task panicked");
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
        if let Some(metrics) = ctx.metrics {
            metrics.dht_lookup_round_timeout();
        }
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
    let kept_closer = filter_xor_closer(resp.closer_nodes.into_inner(), target, &responder);
    let kept_closer = filter_active_stakers(kept_closer, staker_set);

    // Filters 2 and 3 apply to `providers` exactly as they do to
    // `closer_nodes` above, but `providers` carries `Coverage` alongside
    // each `NodeId` and the shared filters operate on bare `NodeId`s — run
    // them over the extracted ids, then keep only the `Provider`s whose id
    // survived, so `coverage` rides along to `record_provider` rather than
    // being dropped by a `Vec<NodeId>` round-trip.
    let provider_ids: Vec<NodeId> = resp.providers.iter().map(|p| p.node).collect();
    let kept_ids = filter_active_stakers(provider_ids, staker_set);
    let kept_ids = filter_negative_cache(kept_ids, target, negative_cache);
    let kept_ids: HashSet<NodeId> = kept_ids.into_iter().collect();

    for p in resp
        .providers
        .into_iter()
        .filter(|p| kept_ids.contains(&p.node))
    {
        state.record_provider(p.node, p.coverage);
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
// Filter helpers (ADR 022 § Lookup integrity) — pure free functions.
// ============================================================

/// Filter 1: drop `closer_nodes` entries whose XOR distance to
/// `target` is not strictly less than `responder`'s own distance.
/// Honest Kademlia responders never produce such entries; an
/// offending entry is either responder misbehaviour or a relay-induced
/// stale value.
#[must_use]
fn filter_xor_closer(closer_nodes: Vec<NodeId>, target: &Hash, responder: &NodeId) -> Vec<NodeId> {
    // Cross-domain XOR: `target` is a content hash, `responder`/`n` are node
    // ids — they share the 256-bit keyspace (ADR 022 §Routing Table).
    let responder_distance = xor_distance_bytes(responder.as_bytes(), target.as_bytes());
    closer_nodes
        .into_iter()
        .filter(|n| xor_distance_bytes(n.as_bytes(), target.as_bytes()) < responder_distance)
        .collect()
}

/// Filter 2: drop non-staked `NodeId`s. Applies symmetrically to
/// `closer_nodes` and `providers` (the caller invokes once per
/// field). The DHT routing pool is restricted to staked nodes per
/// ADR 022 § Lookup integrity.
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
    /// Survivor set, deduplicated by `NodeId`, keyed to its most recently
    /// folded `Coverage`. Insertion order preserved; randomisation happens
    /// at return time.
    providers: IndexMap<NodeId, Coverage>,
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
            providers: IndexMap::new(),
            best_queried_distance: [0xFFu8; NODE_ID_LEN],
            k: cfg.k.get(),
            alpha: cfg.alpha.get(),
        };
        // Seed from the local routing table. Take more than α so the
        // first round has alternates if the closest few don't answer.
        let seed_count = cfg.alpha.get().saturating_mul(4);
        let seed = match routing_table.lock() {
            Ok(t) => t.closest(target.as_bytes(), seed_count),
            Err(poisoned) => {
                warn!("dht lookup: routing table mutex poisoned; recovering inner state");
                poisoned.into_inner().closest(target.as_bytes(), seed_count)
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
        let dist = xor_distance_bytes(peer.as_bytes(), self.target.as_bytes());
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

    /// Insert `peer` into the provider survivor set (or refresh its
    /// coverage if already present). Rejects `requester_id`.
    /// `IndexMap::insert` handles dedup and insertion-order preservation —
    /// re-inserting an existing key updates its value in place without
    /// moving it — in one step.
    fn record_provider(&mut self, peer: NodeId, coverage: Coverage) {
        if peer == self.requester_id {
            return;
        }
        self.providers.insert(peer, coverage);
    }

    fn have_enough_providers(&self) -> bool {
        self.providers.len() >= self.k
    }

    /// Providers accumulated so far — for the round-ceiling log line, which is only useful
    /// if it says what the truncated lookup came away with.
    fn provider_count(&self) -> usize {
        self.providers.len()
    }

    /// Consume state, return the surviving provider set with order
    /// randomised, then truncated to K. ADR 022 § Lookup integrity requires
    /// randomisation of the *surviving* set — shuffle precedes
    /// truncation so the truncated K is a random sample, not a
    /// deterministic prefix.
    fn into_randomised_providers(self) -> Vec<(NodeId, Coverage)> {
        let mut providers: Vec<(NodeId, Coverage)> = self.providers.into_iter().collect();
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
mod tests;
