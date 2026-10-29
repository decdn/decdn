use super::*;

#[test]
fn limits_per_ip_per_minute() {
    let limiter = RateLimiter::new(2);
    let a = Some("203.0.113.1".parse().unwrap());
    let b = Some("203.0.113.2".parse().unwrap());
    assert!(limiter.allow(a, 60));
    assert!(limiter.allow(a, 61));
    assert!(!limiter.allow(a, 62));
    assert!(limiter.allow(b, 62), "another address has its own budget");
    assert!(limiter.allow(a, 120), "a new minute resets the count");
}

#[test]
fn a_full_map_is_swept_once_per_minute() {
    let limiter = RateLimiter::new(5);
    let ip = |n: u32| Some(IpAddr::from(n.to_be_bytes()));
    let sweep_at = u32::try_from(SWEEP_AT).unwrap();
    for n in 0..sweep_at {
        assert!(limiter.allow(ip(n), 60));
    }
    // A new minute: the first request sweeps the stale windows...
    assert!(limiter.allow(ip(u32::MAX), 120));
    assert_eq!(limiter.windows.lock().unwrap().counts.len(), 1);
    // ...and refilling the map within that minute sweeps nothing more.
    for n in 0..sweep_at {
        assert!(limiter.allow(ip(n), 121));
    }
    assert_eq!(limiter.windows.lock().unwrap().swept, 2);
    assert_eq!(limiter.windows.lock().unwrap().counts.len(), SWEEP_AT + 1);
}

#[test]
fn zero_or_unknown_address_is_never_limited() {
    let off = RateLimiter::new(0);
    let a = Some("203.0.113.1".parse().unwrap());
    assert!((0..100).all(|_| off.allow(a, 60)));
    let on = RateLimiter::new(1);
    assert!((0..100).all(|_| on.allow(None, 60)));
}

#[test]
fn an_ipv6_slash_64_is_one_client() {
    let limiter = RateLimiter::new(2);
    let ip = |s: &str| Some(s.parse().unwrap());
    assert!(limiter.allow(ip("2001:db8:1:2::1"), 60));
    assert!(limiter.allow(ip("2001:db8:1:2:ffff:ffff:ffff:ffff"), 60));
    assert!(
        !limiter.allow(ip("2001:db8:1:2:abcd::9"), 60),
        "every address in the /64 shares its budget"
    );
    assert!(
        limiter.allow(ip("2001:db8:1:3::1"), 60),
        "the next /64 has its own budget"
    );
}

#[test]
fn an_ipv4_mapped_address_counts_as_its_ipv4_address() {
    let limiter = RateLimiter::new(1);
    assert!(limiter.allow(Some("::ffff:203.0.113.1".parse().unwrap()), 60));
    assert!(!limiter.allow(Some("203.0.113.1".parse().unwrap()), 60));
    assert!(
        limiter.allow(Some("::ffff:203.0.113.2".parse().unwrap()), 60),
        "mapped addresses do not share one IPv6 /64"
    );
}

#[test]
fn clients_past_the_cap_share_one_overflow_budget() {
    let limiter = RateLimiter::new(2);
    let ip = |n: u32| Some(IpAddr::from(n.to_be_bytes()));
    let max = u32::try_from(MAX_TRACKED).unwrap();
    for n in 0..max {
        assert!(limiter.allow(ip(n), 60));
    }
    assert!(limiter.allow(ip(max), 60));
    assert!(limiter.allow(ip(max + 1), 60));
    assert!(
        !limiter.allow(ip(max + 2), 60),
        "untracked clients share the overflow budget"
    );
    assert!(limiter.allow(ip(0), 60), "a tracked client keeps its own");
    assert!(!limiter.allow(ip(0), 60));
    assert_eq!(limiter.windows.lock().unwrap().counts.len(), MAX_TRACKED);

    // A new minute sweeps the stale windows, and a new client is tracked.
    assert!(limiter.allow(ip(max + 2), 120));
    assert_eq!(limiter.windows.lock().unwrap().counts.len(), 1);
}
