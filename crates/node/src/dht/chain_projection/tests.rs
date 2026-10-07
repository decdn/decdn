use std::collections::HashSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

use super::*;

/// The poison-recovery arms in [`with_read`] / [`with_write`] /
/// [`mutate_gauged`] had no coverage before the consolidation (#1255). Poison
/// the lock, then confirm every accessor still returns the inner state rather
/// than propagating the panic into the read/apply hot path.
#[test]
fn accessors_recover_a_poisoned_lock() {
    let state = RwLock::new(HashSet::<u8>::from([1, 2]));

    let poisoned = catch_unwind(AssertUnwindSafe(|| {
        let _guard = state.write().unwrap();
        panic!("poison the lock while holding the write guard");
    }));
    assert!(poisoned.is_err());
    assert!(state.is_poisoned());

    // Read recovers.
    assert_eq!(with_read(&state, "test", HashSet::len), 2);

    // Mutate + gauge recovers, and only fires on a real change.
    let mut gauged = None;
    let mutated = mutate_gauged(
        &state,
        "test",
        |s| s.insert(3).then_some(s.len()),
        |n| gauged = Some(n),
    );
    assert!(mutated);
    assert_eq!(gauged, Some(3));

    // A no-op re-insert leaves the gauge unpublished.
    let mut gauged_noop = None;
    let mutated_noop = mutate_gauged(
        &state,
        "test",
        |s| s.insert(3).then_some(s.len()),
        |n| gauged_noop = Some(n),
    );
    assert!(!mutated_noop);
    assert_eq!(gauged_noop, None);
}

/// [`with_lock`] recovers a poisoned `Mutex` the same way, including through
/// a nested pair — the shape the republish scheduler holds its heap and its
/// live-hash map in.
#[test]
fn with_lock_recovers_a_poisoned_mutex() {
    let outer = Mutex::new(vec![1u8, 2]);
    let inner = Mutex::new(HashSet::<u8>::from([1, 2]));

    let poisoned = catch_unwind(AssertUnwindSafe(|| {
        let _outer = outer.lock().unwrap();
        let _inner = inner.lock().unwrap();
        panic!("poison both locks while holding the guards");
    }));
    assert!(poisoned.is_err());
    assert!(outer.is_poisoned());
    assert!(inner.is_poisoned());

    let total = with_lock(&outer, "test outer", |o| {
        o.push(3);
        with_lock(&inner, "test inner", |i| {
            i.insert(3);
            o.len() + i.len()
        })
    });
    assert_eq!(total, 6);
}
