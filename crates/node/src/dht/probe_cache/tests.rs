use super::*;
use std::thread;

fn nid(byte: u8) -> NodeId {
    NodeId::from_bytes([byte; 32])
}
fn h(byte: u8) -> Hash {
    Hash::from_bytes([byte; 32])
}
fn p(byte: u8) -> ProbedProvider {
    ProbedProvider {
        node_id: nid(byte),
        rate_per_mb: u64::from(byte),
        rtt_ms: u32::from(byte),
        // A distinct one-block coverage per provider, so a round-trip that
        // dropped or aliased the field fails the `PartialEq` assertions.
        coverage: Coverage::from_block_indices(8, std::iter::once(u32::from(byte % 8))),
        total_bytes_hint: Some(u64::from(byte)),
    }
}

/// ADR 005 §Derived constants: `probe_cache_ttl = PROBE_SLASH_WINDOW / 2`.
/// Pins the DERIVATION, not the number: a literal `15s` passes an
/// `== Duration::from_secs(15)` assertion and then silently fails to move
/// when governance changes the slashing window — the one thing ADR 005
/// explicitly asks implementations to get right.
#[test]
fn ttl_is_derived_from_the_probe_slash_window() {
    assert_eq!(DEFAULT_TTL * 2, PROBE_SLASH_WINDOW);
    assert_eq!(DEFAULT_TTL, Duration::from_secs(15));
}

#[test]
fn absent_hash_returns_none() {
    let c = PositiveProbeCache::new();
    assert!(c.get(&h(1)).is_none());
    assert!(c.is_empty());
}

#[test]
fn insert_then_get_returns_providers_in_order() {
    let c = PositiveProbeCache::new();
    c.insert(h(1), vec![p(1), p(2)]);
    assert_eq!(c.get(&h(1)), Some(vec![p(1), p(2)]));
    assert!(c.get(&h(2)).is_none());
    assert_eq!(c.len(), 1);
}

/// ADR 001 §Probe cache: "Each hash entry retains at most 10 responses (top
/// 10 by selection score)." This truncation is what bounds the cache at the
/// ADR's stated ~1 MB; without it a hash with a large probe fanout is
/// unbounded.
#[test]
fn insert_keeps_only_the_top_ten_providers() {
    let c = PositiveProbeCache::new();
    let many: Vec<_> = (1..=25u8).map(p).collect();
    c.insert(h(1), many);
    let got = c.get(&h(1)).unwrap();
    assert_eq!(got.len(), MAX_PROVIDERS_PER_HASH);
    // The FIRST ten — the caller ranked best-first, so truncation must drop
    // the tail. Taking the last ten would keep the ten WORST providers.
    assert_eq!(got, (1..=10u8).map(p).collect::<Vec<_>>());
}

/// An empty entry would occupy an LRU slot, hit on every read, and yield
/// nothing — strictly worse than no entry.
#[test]
fn inserting_no_providers_is_a_no_op() {
    let c = PositiveProbeCache::new();
    c.insert(h(1), vec![]);
    assert!(c.is_empty());
    assert!(c.get(&h(1)).is_none());
}

#[test]
fn expired_entry_returns_none_and_is_evicted() {
    // Margins kept generous (TTL 500ms, sleep 750ms) so loaded CI runners
    // with cargo-nextest parallelism don't flake on wall-clock checks.
    let c = PositiveProbeCache::with_capacity_and_ttl(8, Duration::from_millis(500));
    c.insert(h(1), vec![p(1)]);
    assert!(c.get(&h(1)).is_some());
    thread::sleep(Duration::from_millis(750));
    assert!(c.get(&h(1)).is_none());
    assert!(c.is_empty(), "expired entry should be evicted on read");
}

/// ADR 001 §Probe cache anchors the TTL at insertion. A read that re-stamped
/// expiry during the LRU bump would keep a hot hash's probe results alive
/// indefinitely — exactly the staleness the 15s window exists to bound, and
/// it would ship green without this test.
#[test]
fn read_hit_does_not_refresh_ttl() {
    let c = PositiveProbeCache::with_capacity_and_ttl(8, Duration::from_millis(500));
    c.insert(h(1), vec![p(1)]);
    thread::sleep(Duration::from_millis(250));
    assert!(c.get(&h(1)).is_some());
    thread::sleep(Duration::from_millis(500));
    assert!(
        c.get(&h(1)).is_none(),
        "read-hit illegally extended the TTL"
    );
}

#[test]
fn lru_eviction_at_cap_drops_oldest() {
    let c = PositiveProbeCache::with_capacity(2);
    c.insert(h(1), vec![p(1)]);
    c.insert(h(2), vec![p(2)]);
    c.insert(h(3), vec![p(3)]);
    assert!(c.get(&h(1)).is_none());
    assert!(c.get(&h(2)).is_some());
    assert!(c.get(&h(3)).is_some());
    assert_eq!(c.len(), 2);
}

/// Pins that `get`'s remove-then-`shift_insert(0, ..)` really is an MRU bump
/// and not an accidental no-op. The non-`Copy` value forces a different
/// dance than `negative_cache`'s single `shift_insert`, so its equivalent
/// test does not cover this one.
#[test]
fn read_hit_bumps_lru_so_oldest_eviction_changes() {
    let c = PositiveProbeCache::with_capacity(2);
    c.insert(h(1), vec![p(1)]);
    c.insert(h(2), vec![p(2)]);
    assert!(c.get(&h(1)).is_some()); // bump h(1) → h(2) becomes LRU
    c.insert(h(3), vec![p(3)]);
    assert!(c.get(&h(1)).is_some());
    assert!(c.get(&h(2)).is_none());
    assert!(c.get(&h(3)).is_some());
}

#[test]
fn reinsert_replaces_providers_and_does_not_grow_len() {
    let c = PositiveProbeCache::with_capacity(4);
    c.insert(h(1), vec![p(1), p(2)]);
    c.insert(h(1), vec![p(3)]);
    assert_eq!(c.get(&h(1)), Some(vec![p(3)]));
    assert_eq!(c.len(), 1);
}

#[test]
fn invalidate_removes_the_entry() {
    let c = PositiveProbeCache::new();
    c.insert(h(1), vec![p(1)]);
    c.invalidate(&h(1));
    assert!(c.get(&h(1)).is_none());
    assert!(c.is_empty());
}

/// The documented "disabled" configuration, so no `disabled()` constructor
/// needs to exist for production code to eventually call.
#[test]
fn a_zero_ttl_cache_never_hits() {
    let c = PositiveProbeCache::with_capacity_and_ttl(8, Duration::ZERO);
    c.insert(h(1), vec![p(1)]);
    assert!(c.get(&h(1)).is_none());
}
