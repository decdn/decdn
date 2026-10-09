//! Live in-flight serve counters: one node-wide count and one per client,
//! generalizing the per-lane atomic-count admission technique to a node-wide
//! resource gate. A [`ShedSlot`] holds one admitted stream's place and releases
//! it on every exit — success, error, `?`, disconnect, panic — via `Drop`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use alloy::primitives::B256;
use dashmap::DashMap;

/// Node-wide and per-client in-flight serve counts. Held in an `Arc` so a
/// [`ShedSlot`] can outlive the borrow that created it.
#[derive(Debug, Default)]
pub struct ShedState {
    node_active: AtomicU32,
    per_client: DashMap<B256, Arc<AtomicU32>>,
}

impl ShedState {
    /// A counter with nothing in flight, already wrapped so [`ShedSlot`]s can
    /// hold it.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// `(node_in_flight, client_in_flight)`. A client with no entry counts 0.
    #[must_use]
    pub fn counts(&self, client: B256) -> (u32, u32) {
        let node = self.node_active.load(Ordering::Relaxed);
        let per = self
            .per_client
            .get(&client)
            .map_or(0, |c| c.load(Ordering::Relaxed));
        (node, per)
    }

    /// Node-wide serves in flight across all clients — the same count
    /// [`Self::counts`] returns as its first element, without needing a client
    /// key. Sampled onto the `decdn_load_shed_streams_in_flight` gauge so an
    /// operator can read live concurrency against the shed high-water mark.
    #[must_use]
    pub fn node_in_flight(&self) -> u32 {
        self.node_active.load(Ordering::Relaxed)
    }

    /// Increment both counters and hand back the RAII slot.
    #[must_use]
    pub fn acquire(self: &Arc<Self>, client: B256) -> ShedSlot {
        self.node_active.fetch_add(1, Ordering::Relaxed);
        let counter = self
            .per_client
            .entry(client)
            .or_insert_with(|| Arc::new(AtomicU32::new(0)))
            .clone();
        counter.fetch_add(1, Ordering::Relaxed);
        ShedSlot {
            state: Arc::clone(self),
            client,
            counter,
        }
    }
}

/// RAII place for one admitted serve. `Drop` releases the node-wide and
/// per-client counts on every exit path, so a finished stream always frees its
/// slot; the count is owned here, never decremented by hand.
#[derive(Debug)]
pub struct ShedSlot {
    state: Arc<ShedState>,
    client: B256,
    counter: Arc<AtomicU32>,
}

impl Drop for ShedSlot {
    fn drop(&mut self) {
        self.state
            .node_active
            .update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                n.saturating_sub(1)
            });
        self.counter
            .update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                n.saturating_sub(1)
            });
        // Best-effort prune: only removes if whatever is currently mapped for
        // this key is zero; a concurrently-recreated live entry is left intact
        // (remove_if re-checks under the shard lock).
        self.state
            .per_client
            .remove_if(&self.client, |_, c| c.load(Ordering::Relaxed) == 0);
    }
}

#[cfg(test)]
mod tests;
