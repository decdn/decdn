//! The in-memory cache of the size proofs the engine keeps (ADR 005 § A node
//! proves the blob size).
//!
//! The store holds every kept proof under a named tag, so this memo is a
//! cache in front of it. A proof is a few KiB, and the memo holds at most
//! [`SIZE_PROOF_ENTRIES`] of them, so it stays under about 300 MiB at the
//! longest proof and far less at a typical one. When it is full, the oldest
//! entry goes, and the next lookup reads the proof from the store again.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::PoisonError;

use bytes::Bytes;
use iroh_blobs::Hash;

/// Most proofs the memo keeps.
pub(crate) const SIZE_PROOF_ENTRIES: usize = 65_536;

/// A verified size and its proof.
#[derive(Debug, Clone)]
pub(crate) struct KeptProof {
    /// The size the proof proves.
    pub(crate) size: u64,
    /// The proof bytes.
    pub(crate) proof: Bytes,
}

#[derive(Debug, Default)]
struct State {
    entries: HashMap<Hash, KeptProof>,
    /// Insertion order, oldest first. A hash may appear more than once after
    /// a replace or a remove; a stale position is skipped at eviction.
    order: VecDeque<Hash>,
}

/// Bounded map from a hash to its verified size proof. Every entry was
/// verified against its hash before it was kept.
#[derive(Debug)]
pub(crate) struct SizeProofMemo {
    capacity: usize,
    state: Mutex<State>,
}

impl SizeProofMemo {
    /// An empty memo that holds at most `capacity` proofs.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(State::default()),
        }
    }

    /// The kept proof of `hash`, whatever size it proves.
    pub(crate) fn get(&self, hash: &Hash) -> Option<KeptProof> {
        self.lock().entries.get(hash).cloned()
    }

    /// Keep `proof` as the proof of `hash`. The caller verified it.
    pub(crate) fn insert(&self, hash: Hash, size: u64, proof: Bytes) {
        let mut state = self.lock();
        if state
            .entries
            .insert(hash, KeptProof { size, proof })
            .is_none()
        {
            state.order.push_back(hash);
        }
        while state.entries.len() > self.capacity {
            let Some(oldest) = state.order.pop_front() else {
                break;
            };
            state.entries.remove(&oldest);
        }
        // Bound the order queue too: stale positions of removed hashes would
        // otherwise grow it without limit.
        if state.order.len() > self.capacity.saturating_mul(2) {
            let State { entries, order } = &mut *state;
            order.retain(|h| entries.contains_key(h));
        }
    }

    /// Drop the proof of `hash`.
    pub(crate) fn remove(&self, hash: &Hash) {
        self.lock().entries.remove(hash);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
