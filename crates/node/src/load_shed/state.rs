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
        }
    }

    /// Number of distinct clients currently tracked (test/observability).
    #[must_use]
    pub fn tracked_clients(&self) -> usize {
        self.per_client.len()
    }

    /// Release one slot: decrement both counters and prune a zeroed per-client
    /// entry so the map stays bounded. Called only by [`ShedSlot::drop`].
    fn release(&self, client: B256) {
        let _ = self
            .node_active
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
        if let Some(counter) = self.per_client.get(&client).map(|c| Arc::clone(c.value())) {
            let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
        }
        // Prune the entry if it dropped to zero. `remove_if` re-checks under the
        // shard lock, so a concurrent `acquire` that just re-incremented keeps
        // its entry; the worst case is a stale zero entry pruned next release.
        self.per_client
            .remove_if(&client, |_, c| c.load(Ordering::Relaxed) == 0);
    }
}

/// RAII place for one admitted serve. `Drop` releases the node-wide and
/// per-client counts on every exit path, so a finished stream always frees its
/// slot; the count is owned here, never decremented by hand.
#[derive(Debug)]
pub struct ShedSlot {
    state: Arc<ShedState>,
    client: B256,
}

impl Drop for ShedSlot {
    fn drop(&mut self) {
        self.state.release(self.client);
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
        assert_eq!(state.tracked_clients(), 1);
        drop(a);
        assert_eq!(
            state.tracked_clients(),
            0,
            "per-client map must not leak zeroed entries"
        );
    }
}
