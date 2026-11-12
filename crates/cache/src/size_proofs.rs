//! The in-memory cache of the size proofs the engine keeps (ADR 005 § A node
//! proves the blob size).
//!
//! The store keeps the proof of each blob it tracks under a named tag, so
//! this memo is a cache in front of it, and the first home of a proof a pull
//! carries before its bytes land. A proof is at most 4,480 bytes, and the
//! memo holds at most [`SIZE_PROOF_ENTRIES`] of them: about 70 MiB at the
//! longest proof and far less at a typical one. When it is full, the oldest
//! entry goes, and the next lookup reads the proof from the store again.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::PoisonError;

use bytes::Bytes;
use iroh_blobs::Hash;

/// Most proofs the memo keeps.
pub(crate) const SIZE_PROOF_ENTRIES: usize = 16_384;

/// A verified size and its proof.
#[derive(Debug, Clone)]
pub(crate) struct KeptProof {
    /// The size the proof proves.
    pub(crate) size: u64,
    /// The proof bytes.
    pub(crate) proof: Bytes,
    /// Whether the store holds the proof's tag too.
    pub(crate) stored: bool,
    /// When the proof was kept, in insertion order: its place in the queue.
    seq: u64,
}

#[derive(Debug, Default)]
struct State {
    entries: HashMap<Hash, KeptProof>,
    /// Insertion order, oldest first, as `(hash, seq)`. A position whose seq
    /// is no longer its hash's entry's is stale: a remove or a later insert
    /// left it behind, and eviction skips it.
    order: VecDeque<(Hash, u64)>,
    /// The seq of the next insert.
    next_seq: u64,
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
    /// `stored` says whether the store holds its tag.
    pub(crate) fn insert(&self, hash: Hash, size: u64, proof: Bytes, stored: bool) {
        let mut state = self.lock();
        let seq = state.next_seq;
        state.next_seq = seq.wrapping_add(1);
        state.entries.insert(
            hash,
            KeptProof {
                size,
                proof,
                stored,
                seq,
            },
        );
        state.order.push_back((hash, seq));
        while state.entries.len() > self.capacity {
            let Some((oldest, seq)) = state.order.pop_front() else {
                break;
            };
            if state.entries.get(&oldest).is_some_and(|e| e.seq == seq) {
                state.entries.remove(&oldest);
            }
        }
        // Bound the queue too: stale positions would otherwise grow it.
        if state.order.len() > self.capacity.saturating_mul(2) {
            let State { entries, order, .. } = &mut *state;
            order.retain(|(h, seq)| entries.get(h).is_some_and(|e| e.seq == *seq));
        }
    }

    /// Record that the store now holds the proof of `hash`.
    pub(crate) fn mark_stored(&self, hash: &Hash) {
        if let Some(entry) = self.lock().entries.get_mut(hash) {
            entry.stored = true;
        }
    }

    /// Drop the proof of `hash`.
    pub(crate) fn remove(&self, hash: &Hash) {
        self.lock().entries.remove(hash);
    }

    #[cfg(test)]
    fn queue_len(&self) -> usize {
        self.lock().order.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)] // tests
mod tests;
