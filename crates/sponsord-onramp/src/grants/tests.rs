use sponsord_api::MicroUsdc;

use super::*;

fn cap(token: &str, expiry: u64) -> IssuedCapability {
    IssuedCapability {
        token: token.into(),
        spending_cap: MicroUsdc(1),
        expiry,
    }
}

const A: Address = Address::repeat_byte(0xa);
const B: Address = Address::repeat_byte(0xb);
const C: Address = Address::repeat_byte(0xc);

#[test]
fn holds_until_the_handoff_window_or_expiry_closes() {
    let grants = GrantCache::new(100, 10);
    grants.put(A, cap("a", 10_000), 1_000);
    assert_eq!(grants.get(A, 1_099).map(|c| c.token), Some("a".into()));
    assert_eq!(grants.get(A, 1_100), None, "hand-off window closed");

    grants.put(B, cap("b", 1_050), 1_000);
    assert_eq!(grants.get(B, 1_049).map(|c| c.token), Some("b".into()));
    assert_eq!(grants.get(B, 1_050), None, "capability expired first");
}

#[test]
fn put_replaces_and_sweep_drops_stale_entries() {
    let grants = GrantCache::new(100, 10);
    grants.put(A, cap("old", 10_000), 1_000);
    grants.put(A, cap("new", 10_000), 1_010);
    assert_eq!(grants.get(A, 1_020).map(|c| c.token), Some("new".into()));
    grants.sweep(2_000);
    assert!(grants.is_empty());
}

#[test]
fn at_the_bound_the_oldest_entry_goes() {
    let grants = GrantCache::new(100, 2);
    grants.put(A, cap("a", 10_000), 1_000);
    grants.put(B, cap("b", 10_000), 1_001);
    grants.put(C, cap("c", 10_000), 1_002);
    assert_eq!(grants.len(), 2);
    assert_eq!(grants.get(A, 1_003), None);
    assert!(grants.get(B, 1_003).is_some());
    assert!(grants.get(C, 1_003).is_some());
}
