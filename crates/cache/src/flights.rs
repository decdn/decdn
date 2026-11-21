//! A map of per-key values that live while someone holds them.
//!
//! [`Flights`] hands out one shared `Arc<V>` per key: the first
//! [`Flights::get`] of a key makes a fresh `V`, and every later `get` while a
//! holder keeps that `Arc` returns the same value. The map holds only a `Weak`,
//! so an entry dies with its last holder, and the next `get` of that key makes
//! a fresh value. The usual `V` is a lock that serializes the fills of one key:
//! when each caller re-checks its cache under the lock, concurrent misses of
//! the key wait for one fill instead of each running their own.

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, Mutex, PoisonError, Weak};

/// The map size at which [`Flights`] first prunes its dead entries.
pub const PRUNE_FLOOR: usize = 16;

/// One shared value per key, held while someone holds it.
///
/// A dead entry stays in the map until a prune, or until a `get` of its key
/// replaces it. A `get` that inserts prunes first when the map has reached
/// its prune size, then sets the next prune size to twice the live entries
/// (at least [`PRUNE_FLOOR`]). Pruning so costs amortized O(1) per insert,
/// and the map holds at most about twice the most live entries it has held,
/// or [`PRUNE_FLOOR`] when that is more.
pub struct Flights<K, V> {
    inner: Mutex<FlightMap<K, V>>,
}

/// The entries of a [`Flights`] and its next prune size.
struct FlightMap<K, V> {
    /// Each value by its key, held as `Weak`.
    map: HashMap<K, Weak<V>>,
    /// The map size at which the next insert first prunes the dead entries.
    prune_at: usize,
}

impl<K, V> Default for Flights<K, V> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(FlightMap {
                map: HashMap::new(),
                prune_at: PRUNE_FLOOR,
            }),
        }
    }
}

impl<K, V> fmt::Debug for Flights<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Flights")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl<K, V> Flights<K, V> {
    /// How many entries the map holds, dead ones not yet pruned included.
    pub fn len(&self) -> usize {
        self.lock().map.len()
    }

    /// Whether the map holds no entries, dead or live.
    pub fn is_empty(&self) -> bool {
        self.lock().map.is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FlightMap<K, V>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<K: Eq + Hash, V> Flights<K, V> {
    /// The live value of `key`, or a fresh `V::default()` that the map holds
    /// until its last holder drops it.
    pub fn get(&self, key: K) -> Arc<V>
    where
        V: Default,
    {
        let mut flights = self.lock();
        if let Some(live) = flights.map.get(&key).and_then(Weak::upgrade) {
            return live;
        }
        if flights.map.len() >= flights.prune_at {
            flights.map.retain(|_, w| w.strong_count() > 0);
            flights.prune_at = flights.map.len().saturating_mul(2).max(PRUNE_FLOOR);
        }
        let flight = Arc::new(V::default());
        flights.map.insert(key, Arc::downgrade(&flight));
        flight
    }

    /// How many `Arc`s of the value of `key` exist now: `0` when the key has
    /// no entry or its entry is dead.
    pub fn holders(&self, key: &K) -> usize {
        self.lock().map.get(key).map_or(0, Weak::strong_count)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)] // tests
mod tests;
