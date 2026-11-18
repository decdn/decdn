use std::collections::HashSet;
use std::panic::{AssertUnwindSafe, catch_unwind};

use super::*;

/// Poison the lock, then confirm [`with_read`] and [`with_write`] still
/// return the inner state rather than propagating the panic into the
/// read/apply hot path.
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

    // Write recovers.
    let len = with_write(&state, "test", |s| {
        s.insert(3);
        s.len()
    });
    assert_eq!(len, 3);
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
