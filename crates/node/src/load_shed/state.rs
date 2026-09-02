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
        let _ = self
            .state
            .node_active
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
        let _ = self
            .counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
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
mod tests {
    use super::*;
    use alloy::primitives::B256;

    fn client(n: u8) -> B256 {
        B256::from([n; 32])
    }

    #[test]
    fn acquire_increments_and_drop_decrements_both_counters() {
        let state = ShedState::new();
        assert_eq!(state.counts(client(1)), (0, 0));
        let a = state.acquire(client(1));
        let b = state.acquire(client(1));
        let c = state.acquire(client(2));
        assert_eq!(state.counts(client(1)), (3, 2)); // node=3, client(1)=2
        assert_eq!(state.counts(client(2)), (3, 1));
        drop(b);
        assert_eq!(state.counts(client(1)), (2, 1));
        drop(a);
        drop(c);
        assert_eq!(state.counts(client(1)), (0, 0));
        assert_eq!(state.counts(client(2)), (0, 0));
    }

    #[test]
    fn zeroed_client_entry_is_pruned() {
        let state = ShedState::new();
        let a = state.acquire(client(9));
        assert_eq!(state.per_client.len(), 1);
        drop(a);
        assert_eq!(
            state.per_client.len(),
            0,
            "per-client map must not leak zeroed entries"
        );
    }

    #[test]
    fn reacquire_after_prune_counts_the_new_slot_only() {
        let state = ShedState::new();
        let a = state.acquire(client(7));
        assert_eq!(state.counts(client(7)), (1, 1));
        assert_eq!(state.per_client.len(), 1);
        drop(a);
        assert_eq!(state.counts(client(7)), (0, 0));
        assert_eq!(state.per_client.len(), 0, "entry must be pruned after drop");

        // Reacquire for the same client: gets a fresh entry, counts only the new slot.
        let b = state.acquire(client(7));
        assert_eq!(state.counts(client(7)), (1, 1));
        assert_eq!(state.per_client.len(), 1);
        drop(b);
        assert_eq!(state.counts(client(7)), (0, 0));
        assert_eq!(state.per_client.len(), 0);
    }
}
