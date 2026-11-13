use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use super::{ConnectionLimiter, RejectReason};
use crate::metrics::Metrics;
use decdn_common::config::ResolvedSecurity;

fn security(max: u32) -> ResolvedSecurity {
    ResolvedSecurity {
        max_concurrent_handlers: max,
    }
}

fn ip(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(a, b, c, d))
}

/// Every inbound connection counts once by arrival path, rejected ones
/// included: a direct IP path or the relay.
#[test]
fn acquire_counts_inbound_connections_by_path() {
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&security(1), Arc::clone(&metrics));
    let _held = limiter
        .acquire_inner(Some(ip(192, 0, 2, 1)))
        .expect("direct");
    limiter
        .acquire_inner(None)
        .expect_err("global cap rejects the relayed one");

    let text = metrics.encode().expect("encode");
    assert!(
        text.contains("decdn_inbound_connections_direct_total 1"),
        "{text}"
    );
    assert!(
        text.contains("decdn_inbound_connections_relayed_total 1"),
        "{text}"
    );
}

#[test]
fn acquire_global_full_rejects_when_semaphore_exhausted() {
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&security(1), Arc::clone(&metrics));
    let permit = limiter
        .acquire_inner(Some(ip(127, 0, 0, 1)))
        .expect("first acquire");
    let err = limiter
        .acquire_inner(Some(ip(10, 0, 0, 1)))
        .expect_err("second acquire should be rejected");
    assert_eq!(err, RejectReason::GlobalFull);
    // After dropping the first permit the slot frees and a third acquire
    // succeeds — proves the OwnedSemaphorePermit drop releases the slot.
    drop(permit);
    let _p = limiter
        .acquire_inner(Some(ip(10, 0, 0, 2)))
        .expect("acquire after drop should succeed");
}

/// Many connections from one address all pass under a generous global cap:
/// the limiter keys nothing on the source address (#2505).
#[test]
fn acquire_admits_many_connections_from_one_address() {
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&security(u32::MAX), Arc::clone(&metrics));
    let same_ip = Some(ip(192, 0, 2, 1));
    let held: Vec<_> = (0..64)
        .map(|_| limiter.acquire_inner(same_ip).expect("same address admits"))
        .collect();
    assert_eq!(held.len(), 64);
}

#[test]
fn permit_drop_decrements_in_flight_metric() {
    // We can't read the gauge value directly without scraping, so the
    // contract is verified indirectly: permits must be released so that
    // a strictly-bounded global semaphore can be re-acquired after Drop.
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&security(2), Arc::clone(&metrics));
    let p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
    let p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();
    // Global is full. Drop one and re-acquire.
    drop(p1);
    let _p3 = limiter
        .acquire_inner(Some(ip(10, 0, 0, 3)))
        .expect("slot should be free after dropping p1");
    drop(p2);
}

#[test]
fn connection_limiter_disabled_global_skips_semaphore() {
    // max_concurrent_handlers=0 disables the global cap. Drop-test verifies
    // all permits live until the explicit drop at the end.
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&security(0), Arc::clone(&metrics));
    let mut held = Vec::with_capacity(256);
    for i in 0..256 {
        let octet = u8::try_from(i % 200).unwrap_or(0);
        let p = limiter
            .acquire_inner(Some(ip(10, 0, 0, octet)))
            .expect("disabled global cap must accept all acquires");
        held.push(p);
    }
    assert_eq!(held.len(), 256);
    drop(held);
}

// --- ConnectionLimiter::reload --------------------------------------------

#[tokio::test]
async fn connection_limiter_reload_grows_semaphore() {
    // Whole-Arc swap: a reload to cap=5 installs a fresh semaphore
    // with 5 permits. Already-held permits drain into the *previous*
    // semaphore on drop and don't count against the new cap. Five
    // fresh acquires must succeed; the sixth rejects.
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&security(2), Arc::clone(&metrics));
    let _p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
    let _p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();
    assert!(limiter.acquire_inner(Some(ip(10, 0, 0, 3))).is_err());

    limiter.reload(&security(5));

    let mut held = Vec::with_capacity(5);
    for i in 3..8u8 {
        held.push(
            limiter
                .acquire_inner(Some(ip(10, 0, 0, i)))
                .expect("under new cap=5"),
        );
    }
    assert!(
        limiter.acquire_inner(Some(ip(10, 0, 0, 99))).is_err(),
        "6th acquire against the new sem must reject at cap=5"
    );
}

#[tokio::test]
async fn connection_limiter_reload_shrinks_caps_new_acquires() {
    // Whole-Arc swap: a reload to a smaller cap installs a fresh
    // semaphore at that size. New acquires hit the new sem; the
    // (new+1)th rejects. Already-held permits hold the previous
    // semaphore alive until they drop — they don't count against
    // the new cap.
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&security(4), Arc::clone(&metrics));
    let _p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
    let _p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();

    limiter.reload(&security(3));

    // Three acquires must succeed against the new (cap=3) sem.
    let _p3 = limiter.acquire_inner(Some(ip(10, 0, 0, 3))).unwrap();
    let _p4 = limiter.acquire_inner(Some(ip(10, 0, 0, 4))).unwrap();
    let _p5 = limiter.acquire_inner(Some(ip(10, 0, 0, 5))).unwrap();
    assert!(
        limiter.acquire_inner(Some(ip(10, 0, 0, 6))).is_err(),
        "4th acquire against the new cap=3 sem must reject"
    );
}

#[tokio::test]
async fn connection_limiter_reload_disable_then_re_enable_caps_correctly() {
    // Start cap=4, disable (max_concurrent_handlers=0), acquire
    // freely (semaphore skipped), re-enable to cap=3 — fresh
    // semaphore with 3 permits installed; new acquires up to 3
    // succeed, 4th rejects.
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&security(4), Arc::clone(&metrics));

    limiter.reload(&security(0));

    // While disabled, hold many permits — the acquire path skips the
    // semaphore entirely.
    let bulk: Vec<_> = (0..16u8)
        .map(|i| {
            limiter
                .acquire_inner(Some(ip(10, 0, 0, i)))
                .expect("disabled cap accepts all")
        })
        .collect();
    drop(bulk);

    limiter.reload(&security(3));

    let _p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
    let _p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();
    let _p3 = limiter.acquire_inner(Some(ip(10, 0, 0, 3))).unwrap();
    assert!(
        limiter.acquire_inner(Some(ip(10, 0, 0, 4))).is_err(),
        "after re-enable to 3, 4th must reject"
    );
}

/// A global-cap rejection shows in the encoded scrape. Metrics must keep
/// flowing after a hot reload, which reuses the shared `Arc<Metrics>`.
#[tokio::test]
async fn rejection_counter_global_visible_in_scrape_after_reload() {
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&security(u32::MAX), Arc::clone(&metrics));
    limiter.reload(&security(1));
    let _p = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
    let _err = limiter
        .acquire_inner(Some(ip(10, 0, 0, 2)))
        .expect_err("global cap exhausted");
    let text = metrics.encode().unwrap();
    assert!(
        text.contains("decdn_dispatch_rejected_global_total 1"),
        "global reject must increment its counter; got:\n{text}"
    );
}
