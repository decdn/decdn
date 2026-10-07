use super::*;
use std::thread;

fn nid(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}
fn h(byte: u8) -> Hash {
    Hash::from_bytes([byte; 32])
}

#[test]
fn absent_key_returns_false() {
    let c = NegativeProbeCache::new();
    assert!(!c.contains_active(&nid(1), &h(1)));
    assert!(c.is_empty());
}

#[test]
fn record_then_contains_returns_true() {
    let c = NegativeProbeCache::new();
    c.record_failure(nid(1), h(1));
    assert!(c.contains_active(&nid(1), &h(1)));
    assert!(!c.contains_active(&nid(2), &h(1)));
    assert!(!c.contains_active(&nid(1), &h(2)));
    assert_eq!(c.len(), 1);
}

/// Entries carry their own expiry, so a caller with weaker evidence can suppress a
/// (peer, hash) for less time than the cache's default (#1145 review). This is what
/// lets an unattributable delivery refusal — a wire `NotFound`, onto which seven
/// reject reasons deliberately collapse, three of them ours or transient — cost a peer
/// seconds of suppression rather than the five minutes an authoritative probe answer
/// earns.
#[test]
fn a_short_ttl_entry_expires_while_a_default_one_is_still_active() {
    let c = NegativeProbeCache::new(); // 5-minute default
    c.record_failure(nid(1), h(1)); // authoritative: the full TTL
    c.record_failure_with_ttl(nid(2), h(1), Duration::from_millis(30)); // weak evidence

    assert!(c.contains_active(&nid(1), &h(1)));
    assert!(c.contains_active(&nid(2), &h(1)));

    thread::sleep(Duration::from_millis(60));

    assert!(
        c.contains_active(&nid(1), &h(1)),
        "the default-TTL entry must outlive the short one"
    );
    assert!(
        !c.contains_active(&nid(2), &h(1)),
        "a short-TTL entry must expire on its OWN clock — if it inherited the cache \
         default, a healthy peer stays blackholed long after the transient cause passed"
    );
}

#[test]
fn expired_entry_returns_false_and_is_evicted() {
    // Margins kept generous (TTL 500ms, sleep 750ms) so loaded
    // CI runners with cargo-nextest parallelism don't flake on
    // wall-clock checks.
    let c = NegativeProbeCache::with_capacity_and_ttl(8, Duration::from_millis(500));
    c.record_failure(nid(1), h(1));
    assert!(c.contains_active(&nid(1), &h(1)));
    thread::sleep(Duration::from_millis(750));
    assert!(!c.contains_active(&nid(1), &h(1)));
    assert!(
        c.is_empty(),
        "expired entry should have been evicted on read"
    );
}

#[test]
fn lru_eviction_at_cap_drops_oldest() {
    let c = NegativeProbeCache::with_capacity(2);
    c.record_failure(nid(1), h(1));
    c.record_failure(nid(2), h(2));
    c.record_failure(nid(3), h(3));
    // nid(1) was least-recently-used → evicted.
    assert!(!c.contains_active(&nid(1), &h(1)));
    assert!(c.contains_active(&nid(2), &h(2)));
    assert!(c.contains_active(&nid(3), &h(3)));
    assert_eq!(c.len(), 2);
}

#[test]
fn read_hit_bumps_lru_so_oldest_eviction_changes() {
    let c = NegativeProbeCache::with_capacity(2);
    c.record_failure(nid(1), h(1));
    c.record_failure(nid(2), h(2));
    // Bump nid(1) by reading it → nid(2) becomes LRU.
    assert!(c.contains_active(&nid(1), &h(1)));
    c.record_failure(nid(3), h(3));
    assert!(c.contains_active(&nid(1), &h(1)));
    assert!(!c.contains_active(&nid(2), &h(2)));
    assert!(c.contains_active(&nid(3), &h(3)));
}

/// Pins indexmap's `shift_insert(0, existing_key, value)`
/// semantic: an existing key MOVES to index 0 (MRU position).
/// The LRU bumping in `contains_active` and the refresh path in
/// `record_failure` both rely on this. If indexmap ever changes
/// to "keep at original index" (a major-version concern), this
/// test fails and surfaces the regression before LRU silently
/// degrades.
#[test]
fn shift_insert_on_existing_key_moves_to_front_of_lru() {
    let c = NegativeProbeCache::with_capacity(4);
    c.record_failure(nid(1), h(1));
    c.record_failure(nid(2), h(2));
    c.record_failure(nid(3), h(3));
    // Re-record nid(1) — should become MRU (index 0).
    c.record_failure(nid(1), h(1));
    let guard = c.lock();
    assert_eq!(
        guard.entries.get_index_of(&(nid(1), h(1))),
        Some(0),
        "shift_insert should have moved nid(1) to index 0"
    );
}

/// Pins the ADR 001 § Probe cache invariant that TTL is anchored
/// at insertion, NOT refreshed on read. A regression in
/// [`NegativeProbeCache::contains_active`] that re-stamped
/// expiry during the LRU bump would extend the suppression
/// window beyond spec and ship green without this test.
#[test]
fn read_hit_does_not_refresh_ttl() {
    let c = NegativeProbeCache::with_capacity_and_ttl(8, Duration::from_millis(500));
    c.record_failure(nid(1), h(1));
    // Half-TTL — entry still live; read bumps LRU.
    thread::sleep(Duration::from_millis(250));
    assert!(c.contains_active(&nid(1), &h(1)));
    // Past the original TTL window. If the read had refreshed
    // the expiry, the entry would still be live here.
    thread::sleep(Duration::from_millis(500));
    assert!(
        !c.contains_active(&nid(1), &h(1)),
        "read-hit illegally extended the TTL window"
    );
}

#[test]
fn re_recording_same_key_refreshes_ttl_does_not_grow_len() {
    // TTL=1000ms; insert, wait 500ms, re-record, wait 750ms.
    // Without the refresh the first insert (t=0, TTL 1000ms)
    // would have expired by t=1250ms; the refresh at t=500ms
    // reset the window, so the entry should still be live at
    // t=1250ms (750ms post-refresh, inside the 1000ms TTL).
    let c = NegativeProbeCache::with_capacity_and_ttl(8, Duration::from_secs(1));
    c.record_failure(nid(1), h(1));
    thread::sleep(Duration::from_millis(500));
    c.record_failure(nid(1), h(1));
    thread::sleep(Duration::from_millis(750));
    assert!(c.contains_active(&nid(1), &h(1)));
    assert_eq!(c.len(), 1);
}
