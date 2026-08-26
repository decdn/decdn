//! DHT bootstrap (ADR 022 §Bootstrap).
//!
//! On node startup the routing table is empty. ADR 022 specifies:
//!
//!  1. Build initial routing table from the on-chain registry peer list
//!     (same source as the peer table bootstrap in ADR 019).
//!  2. Issue `FindNode(self.node_id)` to those initial peers —
//!     standard Kademlia self-lookup populates k-buckets with peers
//!     close to the local id.
//!
//! Step 1 here reads from a [`StakerSet`]. The daemon supplies the
//! chain-backed [`ChainStakerSet`](crate::dht::chain_staker_set::ChainStakerSet),
//! projected from `CapacityBond`; `ConfigStakerSet` is the in-memory
//! implementation of the same trait that tests seed directly.
//!
//! Step 2 runs in parallel against up to a small fan-out of the seed
//! set, with a generous timeout. Failures are non-fatal — a node that
//! can't reach any seed at boot still has a populated routing table
//! from step 1; subsequent inbound traffic refreshes it via the
//! handler's per-request `note_peer_seen` path, and the bucket-refresh
//! task ([`super::bucket_refresh`]) eventually fills the K-closest
//! buckets via `FindNode` probes of random per-bucket targets.

use std::sync::{Arc, Mutex};

use iroh::{Endpoint, EndpointAddr, PublicKey};
use rand::seq::SliceRandom;

use crate::dht::chain_projection::with_lock;
use crate::dht::client;
use crate::dht::routing::{NodeId, RoutingTable};
use crate::dht::staker_set::StakerSet;

/// Cap on the number of seed peers we probe in parallel during the
/// initial self-lookup. The full staker set may be hundreds of nodes;
/// fanning out to all of them at boot would generate a synchronised
/// connection storm against the responding subset. The first few
/// successful responses are enough to seed the k-buckets — iterative
/// lookup (and the bucket-refresh task) will reach the rest at the
/// rate ADR 022 §Routing Table prescribes (one refresh per bucket
/// per hour).
pub const BOOTSTRAP_FANOUT: usize = 8;

/// Summary of a bootstrap run, returned for logs + tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootstrapOutcome {
    /// Number of distinct seed `NodeId`s the staker set provided.
    pub seeds_seen: usize,
    /// Number of seeds inserted into the routing table.
    pub seeds_inserted: usize,
    /// Number of seeds we successfully `FindNode`'d (any response).
    pub find_node_ok: usize,
    /// Number of seeds whose `FindNode` failed (timeout, transport, etc).
    pub find_node_err: usize,
    /// Number of additional peers learned from `closer_nodes` responses
    /// and inserted into the routing table.
    pub closer_peers_inserted: usize,
}

/// Seed the routing table and run the initial self-lookup. Returns a
/// summary so the runtime can log a single high-signal startup line.
///
/// Bootstrap is *best-effort* — a node that fails every seed connection
/// still has a populated routing table from the staker-set seed step.
/// The runtime SHOULD NOT abort startup on bootstrap failure.
//
// Linear "seed → fanout → join" sequence; splitting would scatter the
// step-1/step-2 ADR-022 mapping across helpers and lose the visible
// ordering that the seed table is populated BEFORE we go to network.
#[allow(clippy::cognitive_complexity)]
pub async fn bootstrap(
    endpoint: &Endpoint,
    self_id: PublicKey,
    routing: &Arc<Mutex<RoutingTable>>,
    staker_set: &Arc<dyn StakerSet>,
) -> BootstrapOutcome {
    let self_node_id = NodeId::from_bytes(*self_id.as_bytes());
    let seeds = staker_set.active_nodes();
    let mut outcome = BootstrapOutcome {
        seeds_seen: seeds.len(),
        seeds_inserted: 0,
        find_node_ok: 0,
        find_node_err: 0,
        closer_peers_inserted: 0,
    };

    // Step 1: seed the routing table directly. This is the part that
    // works even when every seed is unreachable: the table now knows
    // *who* the network's active stakers are, even if it can't talk to
    // them yet. Per ADR 022 §Bootstrap "no separate bootstrap window
    // during which content discovery is unavailable" — inbound DHT
    // traffic (probes, FindNode queries from other nodes) immediately
    // routes through this seed set.
    outcome.seeds_inserted = with_lock(routing, "dht routing table", |table| {
        let mut inserted = 0usize;
        for &peer in &seeds {
            if peer == self_node_id {
                // Self-id is rejected by `RoutingTable::insert` anyway,
                // but skip it here so the seeds_inserted count is honest.
                continue;
            }
            if table.insert(peer) {
                inserted += 1;
            }
        }
        inserted
    });

    // Step 2: parallel self-lookup against a fan-out of the seed set.
    // The `StakerSet` trait contract (line 30 of staker_set.rs) says:
    // "Order is unspecified; the caller MUST randomize before any
    // selection step that an attacker could influence (e.g. bootstrap
    // target picking)." A future chain-backed `StakerSet` may return
    // entries in on-chain insertion order, which an attacker who
    // submits transactions can influence — so we shuffle here to keep
    // the picks unpredictable from outside the node.
    let mut seeds_shuffled: Vec<NodeId> =
        seeds.into_iter().filter(|n| *n != self_node_id).collect();
    seeds_shuffled.shuffle(&mut rand::rng());
    let fanout: Vec<NodeId> = seeds_shuffled.into_iter().take(BOOTSTRAP_FANOUT).collect();
    let mut handles = Vec::with_capacity(fanout.len());
    for peer in fanout {
        let endpoint_cloned = endpoint.clone();
        let target_id = peer;
        handles.push(tokio::spawn(async move {
            let target_pk = match PublicKey::from_bytes(target_id.as_bytes()) {
                Ok(k) => k,
                Err(e) => {
                    // A staker-set entry that doesn't decode as an
                    // Ed25519 public key is operator config rot; log
                    // and skip rather than aborting bootstrap.
                    tracing::warn!(
                        peer = ?target_id,
                        error = %e,
                        "dht bootstrap: staker_set entry is not a valid Ed25519 public key"
                    );
                    return (target_id, Err(anyhow::anyhow!("invalid pubkey: {e}")));
                }
            };
            let addr = EndpointAddr::new(target_pk);
            let result =
                client::find_node(&endpoint_cloned, addr, self_node_id, self_node_id).await;
            (target_id, result)
        }));
    }

    for handle in handles {
        match handle.await {
            Ok((_, Ok(resp))) => {
                outcome.find_node_ok += 1;
                // Insert every peer the responder named — these are
                // the seed's K-closest to self.node_id, exactly what
                // we want in our k-buckets. Response-learned ids inserted
                // as probe candidates by design (not authenticated peers);
                // the `AuthenticatedNodeId` boundary covers the handler's
                // recency refresh, not this discovery path.
                outcome.closer_peers_inserted += with_lock(routing, "dht routing table", |table| {
                    let mut inserted = 0usize;
                    for peer in resp.closer_nodes.into_inner() {
                        if peer == self_node_id {
                            continue;
                        }
                        if table.insert(peer) {
                            inserted += 1;
                        }
                    }
                    inserted
                });
            }
            Ok((peer, Err(e))) => {
                outcome.find_node_err += 1;
                tracing::debug!(
                    seed = ?peer,
                    error = %e,
                    "dht bootstrap: FindNode failed against seed"
                );
            }
            Err(e) => {
                outcome.find_node_err += 1;
                tracing::warn!(error = %e, "dht bootstrap: spawned task panicked");
            }
        }
    }

    outcome
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
    use std::collections::HashSet;

    fn nid(b: u8) -> NodeId {
        NodeId::from_bytes([b; 32])
    }

    /// Without spinning up real iroh endpoints we can't exercise the
    /// `FindNode` round-trip from a unit test — that lives in the
    /// loopback integration test. But step 1 (seed the routing table
    /// from `StakerSet::active_nodes`) is pure local mutation; verify
    /// it works against a self-id and against an empty set.
    #[test]
    fn empty_staker_set_yields_empty_routing_table() {
        let self_id = nid(0);
        let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::empty());
        let routing = Arc::new(Mutex::new(RoutingTable::new(self_id)));
        // Step 1 is pure; we can run it without an endpoint.
        let seeds = staker_set.active_nodes();
        assert!(seeds.is_empty());
        for peer in seeds {
            routing.lock().unwrap().insert(peer);
        }
        assert!(routing.lock().unwrap().is_empty());
    }

    #[test]
    fn seed_step_skips_self_id_and_inserts_others() {
        let self_id = nid(0);
        let mut set = HashSet::new();
        set.insert(self_id); // should be filtered
        set.insert(nid(1));
        set.insert(nid(2));
        set.insert(nid(3));
        let staker_set: Arc<dyn StakerSet> = Arc::new(ConfigStakerSet::new(set));
        let routing = Arc::new(Mutex::new(RoutingTable::new(self_id)));
        // Simulate the seed step in isolation.
        let mut inserted = 0usize;
        for &peer in &staker_set.active_nodes() {
            if peer == self_id {
                continue;
            }
            if routing.lock().unwrap().insert(peer) {
                inserted += 1;
            }
        }
        assert_eq!(inserted, 3, "all non-self seeds must land in the table");
        assert!(!routing.lock().unwrap().contains(&self_id));
        assert!(routing.lock().unwrap().contains(&nid(1)));
    }
}
