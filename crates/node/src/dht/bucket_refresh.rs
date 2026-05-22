//! Routing-table bucket refresh (ADR 022 §Routing Table — "Bucket
//! refresh interval | 1 hour").
//!
//! Every routing-table bucket holds peers at a specific XOR-distance
//! prefix from `self.node_id`. As nodes leave the network those
//! peers' entries silently die — without refresh, a long-lived node's
//! buckets fill with stale entries and lookup convergence degrades.
//!
//! The fix per ADR 022 §Routing Table is to periodically issue a
//! `FindNode(random_id_in_bucket)` to one peer in each bucket, every
//! hour. Live peers in that distance shell respond with their own
//! K-closest, populating the bucket with fresh entries.
//!
//! Implementation: a single tokio task wakes on a 1-hour ticker and
//! refreshes *every* non-empty bucket per tick (see
//! [`BUCKET_REFRESH_TICK`] for the rate-budget argument). For each
//! non-empty bucket it synthesises a random `NodeId` whose XOR
//! distance to `self` falls in that bucket's range and runs
//! `FindNode` against the bucket's most-recently-seen peer; the
//! per-bucket RPCs fan out in parallel.
//!
//! This is a "best-effort hygiene" task: a refresh that fails (peer
//! unreachable, timeout) is silently ignored — the next tick will try
//! the next bucket. Failure has no operator-actionable signal.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::{Endpoint, EndpointAddr, PublicKey};
use rand::RngExt;
use tokio::sync::oneshot;

use crate::dht::client;
use crate::dht::routing::{KEYSPACE_BITS, NodeId, RoutingTable, bucket_index};

/// Bucket-refresh tick interval. ADR 022 §Routing Table specifies "1
/// hour" as the refresh interval **per bucket** — we run *every*
/// non-empty bucket per tick so each populated bucket gets the
/// ADR-mandated hourly refresh. At realistic scale (≤30 populated
/// buckets even in a 500-node network) this is ≤30 `FindNode` RPCs
/// per hour, comfortably within the routing-table peers' own per-IP
/// rate limit (100 req/s by default).
pub const BUCKET_REFRESH_TICK: Duration = Duration::from_hours(1);

/// Long-running task: every [`BUCKET_REFRESH_TICK`] runs a
/// `FindNode(random_id_in_bucket)` against each non-empty bucket's
/// most-recently-seen peer in parallel. Exits on `stop_rx`.
pub async fn run_bucket_refresh(
    endpoint: Endpoint,
    self_id: PublicKey,
    routing: Arc<Mutex<RoutingTable>>,
    mut stop_rx: oneshot::Receiver<()>,
    interval: Duration,
) {
    let self_id_bytes = *self_id.as_bytes();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // burn first tick — bucket refresh isn't needed at t=0

    loop {
        tokio::select! {
            biased;
            _ = &mut stop_rx => {
                tracing::debug!("dht bucket-refresh: shutdown signal received");
                return;
            }
            _ = ticker.tick() => {
                refresh_all_non_empty_buckets(&endpoint, self_id_bytes, &routing).await;
            }
        }
    }
}

/// Refresh every non-empty bucket once. Snapshots the (`bucket_index`,
/// peer, target) tuples under a single short lock acquire, then fans
/// the network requests out in parallel.
async fn refresh_all_non_empty_buckets(
    endpoint: &Endpoint,
    self_id_bytes: [u8; 32],
    routing: &Arc<Mutex<RoutingTable>>,
) {
    let picks: Vec<BucketPick> = {
        let Ok(table) = routing.lock() else {
            tracing::error!("dht bucket-refresh: routing-table mutex poisoned");
            return;
        };
        all_non_empty_picks(&table)
    };
    if picks.is_empty() {
        return;
    }
    let mut handles = Vec::with_capacity(picks.len());
    for pick in picks {
        let endpoint_cloned = endpoint.clone();
        let routing_cloned = Arc::clone(routing);
        handles.push(tokio::spawn(async move {
            refresh_one_bucket(&endpoint_cloned, self_id_bytes, &routing_cloned, pick).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
}

/// Snapshot all non-empty buckets as `BucketPick`s. Returns one pick
/// per non-empty bucket; the caller fans the corresponding `FindNode`
/// RPCs out in parallel.
fn all_non_empty_picks(table: &RoutingTable) -> Vec<BucketPick> {
    (0..KEYSPACE_BITS)
        .filter_map(|idx| {
            let peer = bucket_iter(table, idx).last().copied()?;
            let target = random_target_in_bucket(table.self_id(), idx);
            Some(BucketPick {
                bucket_index: idx,
                peer,
                target,
            })
        })
        .collect()
}

/// Run a single bucket's refresh: `FindNode(target)` against the
/// bucket's most-recently-seen peer, then insert any closer-nodes the
/// responder returns. The pick is computed upstream in
/// [`all_non_empty_picks`] so the routing-table lock is held only
/// briefly under the snapshot.
async fn refresh_one_bucket(
    endpoint: &Endpoint,
    self_id_bytes: [u8; 32],
    routing: &Arc<Mutex<RoutingTable>>,
    pick: BucketPick,
) {
    let BucketPick {
        bucket_index,
        peer,
        target,
    } = pick;
    let target_pk = match PublicKey::from_bytes(&peer) {
        Ok(k) => k,
        Err(e) => {
            tracing::warn!(
                peer = ?peer,
                error = %e,
                "dht bucket-refresh: routing-table peer not a valid public key"
            );
            return;
        }
    };
    let addr = EndpointAddr::new(target_pk);
    match client::find_node(endpoint, addr, target, self_id_bytes).await {
        Ok(resp) => {
            // Insert any new peers from the response into the routing
            // table. The bucket's freshness is updated by virtue of
            // these inserts moving entries to MRU.
            if let Ok(mut table) = routing.lock() {
                for nid in resp.closer_nodes {
                    if nid != self_id_bytes {
                        table.insert(nid);
                    }
                }
            }
        }
        Err(e) => {
            tracing::debug!(
                bucket = bucket_index,
                error = %e,
                "dht bucket-refresh: FindNode failed; will retry on next tick"
            );
        }
    }
}

struct BucketPick {
    bucket_index: usize,
    /// The peer we'll send the `FindNode` to — the bucket's
    /// most-recently-seen entry (highest chance of responding).
    peer: NodeId,
    /// A random `NodeId` whose XOR distance to `self_id` falls in
    /// `bucket_index`'s prefix range — used as the `target` of the
    /// `FindNode` so the responder returns peers from that distance
    /// shell.
    target: NodeId,
}

/// Build a single non-empty bucket's pick. Used by tests; the
/// production loop uses [`all_non_empty_picks`] which collects every
/// non-empty bucket per tick.
#[cfg(test)]
fn pick_bucket(table: &RoutingTable, bucket: usize) -> Option<BucketPick> {
    let peer = bucket_iter(table, bucket).last().copied()?;
    let target = random_target_in_bucket(table.self_id(), bucket);
    Some(BucketPick {
        bucket_index: bucket,
        peer,
        target,
    })
}

/// Iterate over the peers in `bucket_index` by filtering `iter_peers`
/// on the distance computation. Marginal cost compared to exposing a
/// dedicated `bucket(idx)` accessor on `RoutingTable`, but keeps the
/// `RoutingTable` API minimal.
fn bucket_iter(table: &RoutingTable, target_bucket: usize) -> impl Iterator<Item = &NodeId> {
    let self_id = *table.self_id();
    table.iter_peers().filter(move |peer| {
        let d = crate::dht::routing::xor_distance(&self_id, peer);
        bucket_index(&d) == Some(target_bucket)
    })
}

/// Produce a random `NodeId` whose XOR-distance to `self_id` falls
/// inside `bucket_index`'s prefix range. Used as the `target` of the
/// refresh `FindNode` so the responder returns peers from that
/// distance shell.
fn random_target_in_bucket(self_id: &NodeId, bucket_index: usize) -> NodeId {
    // bucket_index = KEYSPACE_BITS - 1 - bit_from_msb  →  bit_from_msb = 255 - bucket_index
    let bit_from_msb = KEYSPACE_BITS.saturating_sub(1).saturating_sub(bucket_index);
    let byte_idx = bit_from_msb / 8;
    let bit_within = bit_from_msb % 8;
    let mut target = *self_id;
    // Flip the bucket's prefix bit (guarantees the XOR distance has
    // its highest set bit at this position).
    let flip_mask = 1u8 << (7 - bit_within);
    if let Some(b) = target.get_mut(byte_idx) {
        *b ^= flip_mask;
    }
    // Randomise the lower-order bits so the target isn't predictable.
    let mut rng = rand::rng();
    for slot in target.iter_mut().skip(byte_idx + 1) {
        *slot = rng.random();
    }
    target
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
    use crate::dht::routing::xor_distance;

    fn nid(b: u8) -> NodeId {
        [b; 32]
    }

    /// `random_target_in_bucket` MUST produce a `NodeId` that XOR-
    /// distances to `self_id` into the requested bucket. The exact
    /// bucket assignment is what makes the refreshed peer respond with
    /// entries in that distance shell.
    #[test]
    fn random_target_lands_in_requested_bucket() {
        let self_id = nid(0x00);
        for bucket in [0_usize, 1, 50, 100, 200, 254, 255] {
            for _ in 0..32 {
                let target = random_target_in_bucket(&self_id, bucket);
                let d = xor_distance(&self_id, &target);
                let got = bucket_index(&d);
                assert_eq!(
                    got,
                    Some(bucket),
                    "target {target:?} landed in bucket {got:?}, wanted {bucket}"
                );
            }
        }
    }

    #[test]
    fn all_non_empty_picks_is_empty_for_empty_table() {
        let table = RoutingTable::new(nid(0));
        assert!(all_non_empty_picks(&table).is_empty());
    }

    #[test]
    fn all_non_empty_picks_returns_one_entry_per_non_empty_bucket() {
        let self_id = nid(0);
        let mut table = RoutingTable::new(self_id);
        // Insert two peers in two different buckets.
        let p1 = {
            // Force bucket 255 (high-bit set).
            let mut id = [0u8; 32];
            id[0] = 0x80;
            id
        };
        let p2 = {
            // Force bucket 0 (only low-bit differs).
            let mut id = [0u8; 32];
            id[31] = 0x01;
            id
        };
        table.insert(p1);
        table.insert(p2);

        let picks = all_non_empty_picks(&table);
        // One pick per non-empty bucket — exactly two here. ADR 022
        // requires *every* populated bucket be refreshed each tick.
        assert_eq!(picks.len(), 2);
        let mut indexes: Vec<usize> = picks.iter().map(|p| p.bucket_index).collect();
        indexes.sort_unstable();
        assert_eq!(indexes, vec![0, 255]);
        // The pick's peer is the bucket's most-recently-seen entry
        // (the only peer in each single-peer bucket here).
        for pick in &picks {
            assert!(pick.peer == p1 || pick.peer == p2);
        }
    }

    /// Single-bucket helper from the test-only API still works for the
    /// `bucket_iter` / `random_target_in_bucket` integration.
    #[test]
    fn pick_bucket_for_single_bucket_returns_a_target_in_range() {
        let self_id = nid(0);
        let mut table = RoutingTable::new(self_id);
        let p = {
            let mut id = [0u8; 32];
            id[0] = 0x80;
            id
        };
        table.insert(p);
        let pick = pick_bucket(&table, 255).expect("non-empty bucket 255");
        assert_eq!(pick.bucket_index, 255);
        let d = xor_distance(&self_id, &pick.target);
        assert_eq!(bucket_index(&d), Some(255));
    }
}
