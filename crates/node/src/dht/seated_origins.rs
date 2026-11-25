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
//! valid: the set fails closed. A lookup that finds no seat asks that snapshot
//! for an early re-read, so a newly seated origin's records become valid, and
//! its namespace routable, without waiting out the refresh interval.
//!
//! The set holds every seat in every namespace, so it also answers "which
//! operators are origins for namespace `n`" ([`SeatedOrigins::origins_in`]):
//! the origin directory resolves a namespace from it with no chain read.

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

    /// Every operator seated in `namespace`, in no particular order. Empty
    /// when the namespace has no seat, which asks for an early re-read.
    fn origins_in(&self, namespace: U256) -> Vec<Address>;

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

/// Every seat, indexed both ways so each lookup is one map read.
#[derive(Debug, Default)]
struct Seats {
    /// Operator → the namespaces it is seated in.
    by_operator: HashMap<Address, HashSet<U256>>,
    /// Namespace → the operators seated in it.
    by_namespace: HashMap<U256, HashSet<Address>>,
}

impl Seats {
    fn insert(&mut self, namespace: U256, operator: Address) {
        self.by_operator
            .entry(operator)
            .or_default()
            .insert(namespace);
        self.by_namespace
            .entry(namespace)
            .or_default()
            .insert(operator);
    }

    fn remove(&mut self, namespace: U256, operator: Address) {
        if let Some(namespaces) = self.by_operator.get_mut(&operator) {
            namespaces.remove(&namespace);
            if namespaces.is_empty() {
                self.by_operator.remove(&operator);
            }
        }
        if let Some(operators) = self.by_namespace.get_mut(&namespace) {
            operators.remove(&operator);
            if operators.is_empty() {
                self.by_namespace.remove(&namespace);
            }
        }
    }
}

/// In-memory seated-origin projection, indexed by operator and by namespace.
#[derive(Debug, Default)]
pub struct SeatedOriginSet {
    seats: RwLock<Seats>,
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
    pub fn seat(&self, namespace: U256, operator: Address) {
        self.seats
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(namespace, operator);
    }

    /// Record that `operator` no longer holds a seat in `namespace`. The
    /// operator leaves the set with its last seat, and the namespace with its
    /// last operator.
    pub fn unseat(&self, namespace: U256, operator: Address) {
        self.seats
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(namespace, operator);
    }

    /// Replace every seat with `seats`, an authoritative enumeration.
    pub fn replace(&self, seats: impl IntoIterator<Item = (U256, Address)>) {
        let mut fresh = Seats::default();
        for (namespace, operator) in seats {
            fresh.insert(namespace, operator);
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
            .by_operator
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
            .by_operator
            .get(operator)
            .and_then(|namespaces| namespaces.iter().min().copied())
    }

    fn origins_in(&self, namespace: U256) -> Vec<Address> {
        let operators: Vec<Address> = self
            .seats
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .by_namespace
            .get(&namespace)
            .map(|operators| operators.iter().copied().collect())
            .unwrap_or_default();
        if operators.is_empty() {
            self.on_miss.request();
        }
        operators
    }

    fn request_refresh(&self) {
        self.on_miss.request();
    }

    fn len(&self) -> usize {
        self.seats
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .by_operator
            .len()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
