use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::{Flights, PRUNE_FLOOR};

type LockFlights = Flights<u64, tokio::sync::Mutex<()>>;

/// The prunes `get(key)` ran and the entries they scanned, told from the map
/// size before and after it. A prune that drops no entry leaves no trace, so
/// both counts are lower bounds.
#[derive(Default)]
struct PruneLog {
    prunes: usize,
    scanned: usize,
}

impl PruneLog {
    /// `get` of a key the map holds no entry for, logging a prune when the
    /// map did not grow by that one entry.
    fn get<V: Default>(&mut self, flights: &Flights<u64, V>, key: u64) -> Arc<V> {
        let before = flights.len();
        let flight = flights.get(key);
        if flights.len() <= before {
            self.prunes += 1;
            self.scanned += before;
        }
        flight
    }
}

#[test]
fn get_returns_the_live_entry() {
    let flights = LockFlights::default();
    let first = flights.get(7);
    let second = flights.get(7);
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(flights.holders(&7), 2);
    assert_eq!(flights.len(), 1);
}

#[test]
fn a_dropped_entry_is_replaced() {
    let flights = LockFlights::default();
    let first = flights.get(7);
    let weak = Arc::downgrade(&first);
    drop(first);
    assert_eq!(flights.holders(&7), 0);
    let second = flights.get(7);
    assert!(weak.upgrade().is_none());
    assert_eq!(flights.holders(&7), 1);
    assert_eq!(flights.len(), 1);
    drop(second);
}

#[test]
fn an_absent_key_has_no_holders() {
    let flights = LockFlights::default();
    assert!(flights.is_empty());
    assert_eq!(flights.holders(&7), 0);
}

#[test]
fn pruning_is_amortized() {
    const N: u64 = 10_000;
    let n = usize::try_from(N).unwrap();

    // Dead keys only: the map stays within the floor, and each insert pays
    // for at most one scanned entry on average.
    let flights = LockFlights::default();
    let mut log = PruneLog::default();
    for key in 0..N {
        drop(log.get(&flights, key));
        assert!(flights.len() <= PRUNE_FLOOR + 1);
    }
    assert!(log.scanned <= n, "{} entries scanned", log.scanned);

    // Every other key held: the live entries grow, so the prune size grows
    // with them and the prunes stay O(log N).
    let flights = LockFlights::default();
    let mut log = PruneLog::default();
    let mut held = Vec::new();
    for key in 0..N {
        let flight = log.get(&flights, key);
        if key % 2 == 0 {
            held.push(flight);
        }
        assert!(flights.len() <= (2 * held.len()).max(PRUNE_FLOOR) + 1);
    }
    let bound = 2 * usize::try_from(N.ilog2()).unwrap() + 2;
    assert!(log.prunes <= bound, "{} prunes, bound {bound}", log.prunes);
    assert!(log.scanned <= 4 * n, "{} entries scanned", log.scanned);
}

#[test]
fn live_entries_survive_a_prune() {
    let flights = LockFlights::default();
    let keep = flights.get(0);
    let mut log = PruneLog::default();
    let mut key = 1;
    while log.prunes == 0 {
        drop(log.get(&flights, key));
        key += 1;
    }
    let again = flights.get(0);
    assert!(Arc::ptr_eq(&keep, &again));
    assert_eq!(flights.holders(&0), 2);
}

/// A value that is no lock: [`Flights`] takes any `V: Default`.
#[derive(Default)]
struct Counter(AtomicU64);

#[test]
fn a_value_of_any_default_type_is_shared() {
    let flights = Flights::<&str, Counter>::default();
    let first = flights.get("a");
    first.0.fetch_add(1, Ordering::Relaxed);
    let second = flights.get("a");
    assert_eq!(second.0.load(Ordering::Relaxed), 1);
    assert_eq!(flights.get("b").0.load(Ordering::Relaxed), 0);
    assert_eq!(flights.holders(&"a"), 2);
}
