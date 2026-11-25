//! The seated-origin set (ADR 022 § Origin Attestation).
//!
//! An operator holds an origin seat in namespace `n` when
//! `OriginAssignment.getOrigins(n)` contains it. An origin attestation for
//! namespace `n` is valid only while its signer holds a seat in `n`, so the set
//! is what makes an attestation valid. Seats are tracked per `(namespace,
//! operator)` pair: unseating an operator from one namespace voids its
//! attestations in that namespace only.
//!
//! [`SeatedOriginSet`] is the in-memory projection. The snapshot in
//! [`crate::origin_seat_watcher`] fills it from `OriginAssignment` and keeps it
//! current. Before the first successful read the set is empty, so no record is
//! valid: the set fails closed. A lookup for an operator the set does not hold
//! asks that snapshot for an early re-read, so a newly seated origin's records
//! become valid without waiting out the refresh interval.

use std::collections::{HashMap, HashSet};
use std::sync::{PoisonError, RwLock};

use alloy::primitives::{Address, U256};

use crate::chain_events::refresh::RefreshTrigger;

/// Read-only view of the seated-origin set.
pub trait SeatedOrigins: Send + Sync + std::fmt::Debug {
    /// Whether `operator` holds an origin seat in `namespace`.
    fn is_seated_in(&self, namespace: U256, operator: &Address) -> bool;

    /// The lowest namespace `operator` holds a seat in, or `None` when it
    /// holds none. A seated node signs its own attestations in it.
    fn first_namespace(&self, operator: &Address) -> Option<U256>;

    /// Ask for an early re-read of the seat set: an operator may hold a seat
    /// the set has not seen yet.
    fn request_refresh(&self);

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
    /// Asks the seat snapshot for an early re-read when a lookup misses.
    on_miss: RefreshTrigger,
}

impl SeatedOriginSet {
    /// The trigger a lookup miss pulls; the seat snapshot's refresh loop
    /// listens to it.
    #[must_use]
    pub fn refresh_trigger(&self) -> RefreshTrigger {
        self.on_miss.clone()
    }

    /// An empty set: no attestation is valid until seats are added.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a set from `(namespace, operator)` seats.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn from_seats(seats: impl IntoIterator<Item = (U256, Address)>) -> Self {
        let set = Self::new();
        set.replace(seats);
        set
    }

    /// Record that `operator` holds a seat in `namespace`.
    #[cfg(test)]
    pub fn seat(&self, namespace: U256, operator: Address) {
        let mut seats = self.seats.write().unwrap_or_else(PoisonError::into_inner);
        seats.entry(operator).or_default().insert(namespace);
    }

    /// Record that `operator` no longer holds a seat in `namespace`. The
    /// operator leaves the set with its last seat.
    #[cfg(any(test, feature = "test-support"))]
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
    fn is_seated_in(&self, namespace: U256, operator: &Address) -> bool {
        let seated = self
            .seats
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(operator)
            .is_some_and(|namespaces| namespaces.contains(&namespace));
        if !seated {
            self.on_miss.request();
        }
        seated
    }

    fn first_namespace(&self, operator: &Address) -> Option<U256> {
        self.seats
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(operator)
            .and_then(|namespaces| namespaces.iter().min().copied())
    }

    fn request_refresh(&self) {
        self.on_miss.request();
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
