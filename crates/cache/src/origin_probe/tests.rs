use super::{OriginProbeMemo, OriginProbePolicy, Presence};
use crate::Hash;
use std::time::{Duration, Instant};

fn hash(seed: u8) -> Hash {
    Hash::from([seed; 32])
}

#[test]
fn positive_answer_is_memoised_within_ttl() {
    let mut memo = OriginProbeMemo::new(OriginProbePolicy {
        positive_ttl: Duration::from_secs(10),
        negative_ttl: Duration::from_secs(10),
        fault_ttl: Duration::from_secs(5),
        timeout: Duration::from_secs(2),
        capacity: 16,
    });
    let t0 = Instant::now();
    assert_eq!(memo.get(hash(1), t0), None, "cold lookup misses");
    memo.insert(hash(1), Presence::Present(4096), t0);
    assert_eq!(
        memo.get(hash(1), t0 + Duration::from_secs(9)),
        Some(Presence::Present(4096)),
        "answer is live within the TTL",
    );
}

#[test]
fn negative_answer_is_memoised_too() {
    let mut memo = OriginProbeMemo::new(OriginProbePolicy {
        positive_ttl: Duration::from_secs(10),
        negative_ttl: Duration::from_secs(2),
        fault_ttl: Duration::from_secs(5),
        timeout: Duration::from_secs(2),
        capacity: 16,
    });
    let t0 = Instant::now();
    memo.insert(hash(2), Presence::Absent, t0);
    assert_eq!(
        memo.get(hash(2), t0 + Duration::from_secs(1)),
        Some(Presence::Absent),
        "a 404 is cached so a random-hash flood does not re-HEAD every probe",
    );
    assert_eq!(
        memo.get(hash(2), t0 + Duration::from_secs(3)),
        None,
        "the negative TTL, not the positive one, bounds how long Absent is honored",
    );
}

#[test]
fn entry_expires_after_ttl_and_is_swept_on_read() {
    let mut memo = OriginProbeMemo::new(OriginProbePolicy {
        positive_ttl: Duration::from_secs(10),
        negative_ttl: Duration::from_secs(10),
        fault_ttl: Duration::from_secs(5),
        timeout: Duration::from_secs(2),
        capacity: 16,
    });
    let t0 = Instant::now();
    memo.insert(hash(3), Presence::Present(1), t0);
    assert_eq!(
        memo.get(hash(3), t0 + Duration::from_secs(11)),
        None,
        "expired"
    );
    assert!(memo.is_empty(), "expired entry is swept off the read path");
}

#[test]
fn capacity_is_never_exceeded_under_live_pressure() {
    let cap: usize = 4;
    // TTL far longer than the (instantaneous) test so nothing expires: this
    // exercises the live-eviction arm, not the sweep.
    let mut memo = OriginProbeMemo::new(OriginProbePolicy {
        positive_ttl: Duration::from_secs(100),
        negative_ttl: Duration::from_secs(100),
        fault_ttl: Duration::from_secs(5),
        timeout: Duration::from_secs(2),
        capacity: cap,
    });
    let t0 = Instant::now();
    // Insert more distinct live hashes than capacity.
    for i in 0..(cap + 6) {
        let seed = u8::try_from(i).unwrap_or(u8::MAX);
        memo.insert(hash(seed), Presence::Absent, t0);
        assert!(memo.len() <= cap, "memo stays within its capacity bound");
    }
    assert_eq!(memo.len(), cap, "memo saturates exactly at capacity");
}

#[test]
fn expired_entries_are_reclaimed_before_evicting_live_ones() {
    let cap = 2;
    let mut memo = OriginProbeMemo::new(OriginProbePolicy {
        positive_ttl: Duration::from_secs(10),
        negative_ttl: Duration::from_secs(10),
        fault_ttl: Duration::from_secs(5),
        timeout: Duration::from_secs(2),
        capacity: cap,
    });
    let t0 = Instant::now();
    memo.insert(hash(10), Presence::Absent, t0);
    memo.insert(hash(11), Presence::Absent, t0);
    // A later insert past the first two's TTL should reclaim expired space
    // rather than the map growing.
    let t1 = t0 + Duration::from_secs(11);
    memo.insert(hash(12), Presence::Present(9), t1);
    assert!(memo.len() <= cap);
    assert_eq!(
        memo.get(hash(12), t1),
        Some(Presence::Present(9)),
        "the fresh entry is retained",
    );
}

#[test]
fn refreshing_existing_key_does_not_grow_map() {
    let mut memo = OriginProbeMemo::new(OriginProbePolicy {
        positive_ttl: Duration::from_secs(10),
        negative_ttl: Duration::from_secs(10),
        fault_ttl: Duration::from_secs(5),
        timeout: Duration::from_secs(2),
        capacity: 2,
    });
    let t0 = Instant::now();
    memo.insert(hash(20), Presence::Absent, t0);
    memo.insert(hash(20), Presence::Present(5), t0);
    assert_eq!(memo.len(), 1);
    assert_eq!(
        memo.get(hash(20), t0),
        Some(Presence::Present(5)),
        "refreshed in place"
    );
}

#[test]
fn negative_entries_expire_on_the_short_ttl_while_positive_ones_persist() {
    // positive 10s, negative 2s.
    let mut memo = OriginProbeMemo::new(OriginProbePolicy {
        positive_ttl: Duration::from_secs(10),
        negative_ttl: Duration::from_secs(2),
        fault_ttl: Duration::from_secs(5),
        timeout: Duration::from_secs(2),
        capacity: 16,
    });
    let t0 = Instant::now();
    memo.insert(hash(1), Presence::Present(4096), t0);
    memo.insert(hash(2), Presence::Absent, t0);

    // At t0 + 3s the negative entry is gone (fresh content can re-probe),
    // but the positive entry is still live.
    let t = t0 + Duration::from_secs(3);
    assert_eq!(memo.get(hash(2), t), None, "negative expires on the 2s TTL");
    assert_eq!(
        memo.get(hash(1), t),
        Some(Presence::Present(4096)),
        "positive still live on the 10s TTL",
    );
}

#[test]
fn presence_size_maps_to_probe_answer() {
    assert_eq!(Presence::Present(42).size(), Some(42));
    assert_eq!(Presence::Absent.size(), None);
    assert_eq!(
        Presence::Fault.size(),
        None,
        "a fault also leaves has_blob false"
    );
}

#[test]
fn fault_answer_is_memoised_under_the_fault_ttl() {
    // fault 5s, negative 2s, positive 100s.
    let mut memo = OriginProbeMemo::new(OriginProbePolicy {
        positive_ttl: Duration::from_secs(100),
        negative_ttl: Duration::from_secs(2),
        fault_ttl: Duration::from_secs(5),
        timeout: Duration::from_secs(2),
        capacity: 16,
    });
    let t0 = Instant::now();
    memo.insert(hash(40), Presence::Fault, t0);

    // Live within the fault TTL but past the shorter negative TTL — a fault
    // is a backend outage, so it must last longer than a per-hash 404.
    assert_eq!(
        memo.get(hash(40), t0 + Duration::from_secs(3)),
        Some(Presence::Fault),
        "fault stays memoised past the negative TTL",
    );
    assert_eq!(
        memo.get(hash(40), t0 + Duration::from_secs(6)),
        None,
        "fault expires on the fault TTL so a recovered origin is re-probed",
    );
}
