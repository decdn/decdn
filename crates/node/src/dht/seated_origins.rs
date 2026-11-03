//! The seated-origin set (ADR 022 § Origin Attestation).
//!
//! An operator holds an origin seat when `OriginAssignment.getOrigins(n)`
//! contains it for at least one namespace `n`. Every DHT record's origin
//! attestation must recover to an operator in this set, so the set is what
//! makes a record valid. Seats are tracked per `(namespace, operator)` pair: an
//! operator unseated from one namespace stays in the set while it holds a seat
//! in another.
//!
//! [`SeatedOriginSet`] is the in-memory projection. The chain watcher in
//! [`crate::origin_seat_watcher`] fills it from `OriginAssignment` and keeps it
//! current. Before the first successful read the set is empty, so no record is
//! valid: the set fails closed.

use std::collections::{HashMap, HashSet};
use std::sync::{PoisonError, RwLock};

use alloy::primitives::{Address, U256};

/// Read-only view of the seated-origin set.
pub trait SeatedOrigins: Send + Sync + std::fmt::Debug {
    /// Whether `operator` holds an origin seat in at least one namespace.
    fn is_seated(&self, operator: &Address) -> bool;

    /// How many operators hold at least one seat.
    fn len(&self) -> usize;

    /// Whether no operator holds a seat.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// In-memory seated-origin projection, keyed by operator, holding the
/// namespaces each operator is seated in.
#[derive(Debug, Default)]
pub struct SeatedOriginSet {
    seats: RwLock<HashMap<Address, HashSet<U256>>>,
}

impl SeatedOriginSet {
    /// An empty set: no attestation is valid until seats are added.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a set from `(namespace, operator)` seats.
    #[must_use]
    pub fn from_seats(seats: impl IntoIterator<Item = (U256, Address)>) -> Self {
        let set = Self::new();
        set.replace(seats);
        set
    }

    /// Record that `operator` holds a seat in `namespace`.
    pub fn seat(&self, namespace: U256, operator: Address) {
        let mut seats = self.seats.write().unwrap_or_else(PoisonError::into_inner);
        seats.entry(operator).or_default().insert(namespace);
    }

    /// Record that `operator` no longer holds a seat in `namespace`. The
    /// operator leaves the set with its last seat.
    pub fn unseat(&self, namespace: U256, operator: Address) {
        let mut seats = self.seats.write().unwrap_or_else(PoisonError::into_inner);
        if let Some(namespaces) = seats.get_mut(&operator) {
            namespaces.remove(&namespace);
            if namespaces.is_empty() {
                seats.remove(&operator);
            }
        }
    }

    /// Replace every seat with `seats`, an authoritative enumeration.
    pub fn replace(&self, seats: impl IntoIterator<Item = (U256, Address)>) {
        let mut fresh: HashMap<Address, HashSet<U256>> = HashMap::new();
        for (namespace, operator) in seats {
            fresh.entry(operator).or_default().insert(namespace);
        }
        *self.seats.write().unwrap_or_else(PoisonError::into_inner) = fresh;
    }
}

impl SeatedOrigins for SeatedOriginSet {
    fn is_seated(&self, operator: &Address) -> bool {
        self.seats
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(operator)
    }

    fn len(&self) -> usize {
        self.seats
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
