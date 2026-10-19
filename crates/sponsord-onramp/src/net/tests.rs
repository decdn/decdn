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
