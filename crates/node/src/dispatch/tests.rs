use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use super::{ConnectionLimiter, RejectReason};
use crate::metrics::Metrics;
use decdn_common::config::ResolvedSecurity;

fn strict_security(max: u32) -> ResolvedSecurity {
    ResolvedSecurity {
        max_concurrent_handlers: max,
        per_source_rate_per_sec: 1.0,
        per_source_burst: 1,
        max_tracked_sources: 16,
    }
}

fn permissive_security() -> ResolvedSecurity {
    ResolvedSecurity {
        max_concurrent_handlers: u32::MAX,
        per_source_rate_per_sec: 1e9,
        per_source_burst: u32::MAX,
        max_tracked_sources: 4096,
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
    let limiter = ConnectionLimiter::new(&strict_security(1), Arc::clone(&metrics));
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
    let limiter = ConnectionLimiter::new(&strict_security(1), Arc::clone(&metrics));
    let permit = limiter
        .acquire_inner(Some(ip(127, 0, 0, 1)))
        .expect("first acquire");
    // Second acquire from a *different* IP must still fail the
    // global cap before the per-source layer gets a chance.
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

#[test]
fn acquire_per_source_rejects_after_burst_exhausted() {
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
    let same_ip = Some(ip(192, 0, 2, 1));
    // First acquire from the IP succeeds.
    let _p1 = limiter.acquire_inner(same_ip).expect("first acquire");
    // Second from the same IP hits the per-source burst-1 limit.
    let err = limiter
        .acquire_inner(same_ip)
        .expect_err("second per-source acquire should reject");
    assert_eq!(err, RejectReason::PerSource);
}

#[test]
fn acquire_per_source_rejection_does_not_charge_global_permit() {
    // Per-source layer is checked *after* the global semaphore — a
    // per-source rejection releases the held semaphore permit on
    // drop. Verify that a fresh IP can immediately acquire after a
    // per-source rejection from another IP, even when the global
    // cap is tight.
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&strict_security(2), Arc::clone(&metrics));
    let same_ip = Some(ip(192, 0, 2, 99));
    let _p1 = limiter
        .acquire_inner(same_ip)
        .expect("p1 drains the per-source bucket for that IP");
    let err = limiter
        .acquire_inner(same_ip)
        .expect_err("p2: per-source bucket empty for that IP");
    assert_eq!(err, RejectReason::PerSource);
    // Global cap = 2; held = 1 (p1). Fresh IP must still acquire
    // — the rejected p2 must have released its semaphore slot.
    let _p3 = limiter
        .acquire_inner(Some(ip(10, 0, 0, 1)))
        .expect("fresh IP must acquire under global=2");
}

#[test]
fn acquire_relay_connection_skips_per_source() {
    // peer_ip = None simulates a relay-only connection. Per-source
    // layer is skipped; only the global cap applies.
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
    let _p1 = limiter.acquire_inner(None).expect("relay acquire 1");
    // Second relay acquire — must succeed (per-source doesn't apply).
    let _p2 = limiter.acquire_inner(None).expect("relay acquire 2");
}

#[test]
fn permit_drop_decrements_in_flight_metric() {
    // We can't read the gauge value directly without scraping, so the
    // contract is verified indirectly: permits must be released so that
    // a strictly-bounded global semaphore can be re-acquired after Drop.
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&strict_security(2), Arc::clone(&metrics));
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
fn concurrent_acquires_from_same_source_serialize_correctly() {
    // 32 threads racing on a burst=1 limiter must all see exactly one
    // success and 31 PerSource rejections. governor's keyed limiter
    // is internally synchronised; this proves we don't accidentally
    // leak two permits through the per-source check.
    use std::sync::Barrier;
    let metrics = Arc::new(Metrics::new());
    let limiter = Arc::new(ConnectionLimiter::new(&strict_security(u32::MAX), metrics));
    let n = 32;
    let barrier = Arc::new(Barrier::new(n));
    let single_ip = Some(ip(192, 0, 2, 250));
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let lim = Arc::clone(&limiter);
        let bar = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            bar.wait();
            lim.acquire_inner(single_ip).is_ok()
        }));
    }
    let successes: usize = handles
        .into_iter()
        .map(|h| usize::from(h.join().unwrap()))
        .sum();
    assert_eq!(successes, 1, "exactly one acquire should succeed");
}

// --- Disabled-layer (0 = unlimited) ---------------------------------------

fn disabled_global_security() -> ResolvedSecurity {
    ResolvedSecurity {
        max_concurrent_handlers: 0,
        per_source_rate_per_sec: 1e9,
        per_source_burst: u32::MAX,
        max_tracked_sources: 4096,
    }
}

#[test]
fn connection_limiter_disabled_global_skips_semaphore() {
    // max_concurrent_handlers=0 disables the global cap. We can hold
    // far more permits than the per-source layer would normally
    // allow at startup. Drop-test verifies all permits live until the
    // explicit drop at the end.
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&disabled_global_security(), Arc::clone(&metrics));
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

#[test]
fn connection_limiter_disabled_per_source_passes_all() {
    // per_source_rate_per_sec=0 disables the per-source layer. With
    // global cap also generous, hundreds of acquires from a single
    // IP must all succeed.
    let metrics = Arc::new(Metrics::new());
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: u32::MAX,
        per_source_rate_per_sec: 0.0,
        per_source_burst: 0,
        max_tracked_sources: 16,
    };
    let limiter = ConnectionLimiter::new(&cfg, Arc::clone(&metrics));
    let same_ip = Some(ip(10, 0, 0, 1));
    for _ in 0..512 {
        let _p = limiter
            .acquire_inner(same_ip)
            .expect("per-source disabled must accept");
    }
}

// --- ConnectionLimiter::reload --------------------------------------------

#[tokio::test]
async fn connection_limiter_reload_grows_semaphore() {
    // Whole-Arc swap: a reload to cap=5 installs a fresh semaphore
    // with 5 permits. Already-held permits drain into the *previous*
    // semaphore on drop and don't count against the new cap. Five
    // fresh acquires from new IPs must succeed; the sixth rejects.
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&strict_security(2), Arc::clone(&metrics));
    let _p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
    let _p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();
    assert!(limiter.acquire_inner(Some(ip(10, 0, 0, 3))).is_err());

    let mut new_cfg = permissive_security();
    new_cfg.max_concurrent_handlers = 5;
    limiter.reload(&new_cfg);

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
    let limiter = ConnectionLimiter::new(&strict_security(4), Arc::clone(&metrics));
    let _p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
    let _p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();

    let mut new_cfg = permissive_security();
    new_cfg.max_concurrent_handlers = 3;
    limiter.reload(&new_cfg);

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
async fn connection_limiter_reload_swaps_per_source_quota() {
    // Start with strict per-source burst=1; exhaust it from a
    // single IP; reload with raised burst=10 and confirm the
    // limiter accepts new acquires immediately. (The new keyed
    // limiter has a fresh state, so the next acquire from the
    // same IP starts with a full burst budget.)
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
    let attacker_ip = Some(ip(10, 0, 0, 1));
    let _p1 = limiter.acquire_inner(attacker_ip).unwrap();
    // Per-source burst=1 exhausted.
    assert!(limiter.acquire_inner(attacker_ip).is_err());
    // Reload: raise per-source burst & rate so the rebuild yields a
    // fresh limiter state.
    let mut new_cfg = permissive_security();
    new_cfg.max_concurrent_handlers = u32::MAX;
    limiter.reload(&new_cfg);
    let _p2 = limiter
        .acquire_inner(attacker_ip)
        .expect("rebuild must reset per-source state");
}

#[tokio::test]
async fn connection_limiter_reload_disable_then_re_enable_caps_correctly() {
    // Start cap=4, disable (max_concurrent_handlers=0), acquire
    // freely (semaphore skipped), re-enable to cap=3 — fresh
    // semaphore with 3 permits installed; new acquires up to 3
    // succeed, 4th rejects.
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&strict_security(4), Arc::clone(&metrics));

    // Disable the global cap.
    let mut c = permissive_security();
    c.max_concurrent_handlers = 0;
    limiter.reload(&c);

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

    // Re-enable to cap=3.
    c.max_concurrent_handlers = 3;
    limiter.reload(&c);

    let _p1 = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
    let _p2 = limiter.acquire_inner(Some(ip(10, 0, 0, 2))).unwrap();
    let _p3 = limiter.acquire_inner(Some(ip(10, 0, 0, 3))).unwrap();
    assert!(
        limiter.acquire_inner(Some(ip(10, 0, 0, 4))).is_err(),
        "after re-enable to 3, 4th must reject"
    );
}

#[test]
fn connection_limiter_reload_disables_per_source_independently() {
    // Per-source enabled at startup: burst=1 rejects the second
    // acquire from a single IP. After reload disabling per-source,
    // many consecutive acquires from that IP succeed.
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
    let same_ip = Some(ip(192, 0, 2, 99));
    let _p1 = limiter
        .acquire_inner(same_ip)
        .expect("first per-source acquire");
    let err = limiter
        .acquire_inner(same_ip)
        .expect_err("second from same IP must reject");
    assert_eq!(err, RejectReason::PerSource);

    // Disable per-source via reload.
    let mut c = strict_security(u32::MAX);
    c.per_source_rate_per_sec = 0.0;
    c.per_source_burst = 0;
    limiter.reload(&c);

    for _ in 0..32 {
        let _p = limiter
            .acquire_inner(same_ip)
            .expect("per-source disabled must accept");
    }
}

/// Metrics counters must continue to flow after a hot reload — the
/// `Arc<Metrics>` handle is shared in by the `ConnectionLimiter`
/// constructor and reused via in-place mutation. A regression that
/// rebuilt the limiter on reload (and lost the `Arc<Metrics>`) would
/// leave reject counters frozen at zero post-reload.
#[tokio::test]
async fn reload_preserves_metrics_handle_for_post_reload_rejects() {
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&permissive_security(), Arc::clone(&metrics));

    // Tighten per-source via reload to burst=1, rate ~ 0 so refill
    // doesn't lift the cap within the test window.
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: u32::MAX,
        per_source_rate_per_sec: 0.001,
        per_source_burst: 1,
        max_tracked_sources: 16,
    };
    limiter.reload(&cfg);

    let same_ip = Some(ip(10, 0, 0, 1));
    let _ok = limiter
        .acquire_inner(same_ip)
        .expect("first acquire under tightened limit");
    // Second from same IP — per-source burst exhausted post-reload.
    let _err = limiter
        .acquire_inner(same_ip)
        .expect_err("post-reload reject");

    let text = metrics.encode().unwrap();
    // OpenMetrics auto-appends `_total` to counter field names, so
    // the field `dispatch_rejected_per_source` (under the `decdn`
    // group) is exposed as `decdn_dispatch_rejected_per_source_total`.
    assert!(
        text.contains("decdn_dispatch_rejected_per_source_total 1"),
        "post-reload reject must increment counter; got:\n{text}"
    );
}

/// Sibling of `reload_preserves_metrics_handle_for_post_reload_rejects`
/// covering the *global-cap* reject path. A regression that swapped the
/// `dispatch_rejected_global` and `dispatch_rejected_per_source`
/// counter calls would still pass `RejectReason`-equality assertions
/// in other tests; only an encoded-scrape assertion catches the typo.
#[tokio::test]
async fn rejection_counters_global_visible_in_scrape() {
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&strict_security(1), Arc::clone(&metrics));
    let _p = limiter.acquire_inner(Some(ip(10, 0, 0, 1))).unwrap();
    let _err = limiter
        .acquire_inner(Some(ip(10, 0, 0, 2)))
        .expect_err("global cap exhausted");
    let text = metrics.encode().unwrap();
    assert!(
        text.contains("decdn_dispatch_rejected_global_total 1"),
        "global reject must increment its own counter; got:\n{text}"
    );
    assert!(
        !text.contains("decdn_dispatch_rejected_per_source_total 1"),
        "per-source counter must not move on a global rejection"
    );
}

/// Sibling covering the *per-source* reject path against the encoded
/// scrape.
#[tokio::test]
async fn rejection_counters_per_source_visible_in_scrape() {
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
    let same_ip = Some(ip(192, 0, 2, 7));
    let _p = limiter
        .acquire_inner(same_ip)
        .expect("first per-source acquire");
    let _err = limiter
        .acquire_inner(same_ip)
        .expect_err("second per-source acquire rejects");
    let text = metrics.encode().unwrap();
    assert!(
        text.contains("decdn_dispatch_rejected_per_source_total 1"),
        "per-source reject must increment its own counter; got:\n{text}"
    );
    assert!(
        !text.contains("decdn_dispatch_rejected_global_total 1"),
        "global counter must not move on a per-source rejection"
    );
}

/// Per-source layer enabled + relay-only connection (no peer IP):
/// `dispatch_per_source_skipped_no_addr_total` increments. When the
/// layer is disabled, no skip is recorded (no enforcement intent =>
/// nothing to skip).
#[tokio::test]
async fn per_source_skipped_when_relay_only_and_layer_enabled() {
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
    let _p = limiter
        .acquire_inner(None)
        .expect("relay acquire under per-source enabled");
    let text = metrics.encode().unwrap();
    assert!(
        text.contains("decdn_dispatch_per_source_skipped_no_addr_total 1"),
        "relay-only acquire under enabled per-source must increment skip counter; got:\n{text}"
    );

    // Disable per-source via reload; a subsequent relay acquire must
    // *not* increment the counter (no enforcement intent).
    let mut c = strict_security(u32::MAX);
    c.per_source_rate_per_sec = 0.0;
    c.per_source_burst = 0;
    limiter.reload(&c);
    let _p2 = limiter
        .acquire_inner(None)
        .expect("relay acquire under per-source disabled");
    let text = metrics.encode().unwrap();
    assert!(
        text.contains("decdn_dispatch_per_source_skipped_no_addr_total 1"),
        "disabled per-source must not bump the skip counter past 1; got:\n{text}"
    );
}

/// `gc_per_source` (#440) drops fully-refilled buckets between
/// acquires. Without it, a long-lived node whose connection rate
/// stays below the over-cap threshold accumulates stale entries
/// indefinitely — the acquire-path prune only fires under flood.
#[tokio::test]
async fn gc_per_source_drops_refilled_buckets() {
    // Fast refill: rate=1000/s, burst=1. Each bucket refills to
    // baseline well within a 100ms wait, so `retain_recent()` will
    // drop every key it sees.
    let metrics = Arc::new(Metrics::new());
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: u32::MAX,
        per_source_rate_per_sec: 1000.0,
        per_source_burst: 1,
        // cap=0 disables the acquire-path opportunistic prune so
        // this test isolates the explicit GC method.
        max_tracked_sources: 0,
    };
    let limiter = ConnectionLimiter::new(&cfg, Arc::clone(&metrics));

    // Fill 8 distinct per-source buckets and drop each permit
    // immediately so the bucket state matches "fresh baseline"
    // after refill.
    for i in 0..8u8 {
        let _p = limiter
            .acquire_inner(Some(ip(10, 0, 0, i)))
            .expect("acquire should succeed under generous rate");
    }
    assert_eq!(limiter.per_source_tracked(), 8);

    // Wait long enough for every bucket to refill (rate=1000/s
    // means the single token returns in ~1ms).
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    limiter.gc_per_source();
    assert_eq!(
        limiter.per_source_tracked(),
        0,
        "refilled buckets should be dropped by gc_per_source"
    );
}

/// An accept that trips `cap + cap/10` prunes the refilled keyspace off the
/// bucket check (#1788 item 2). In production the sweep is offloaded to a
/// `spawn_blocking` task; here there is no tokio runtime, so `dispatch_prune`
/// runs the SAME single-flighted `retain_recent` sweep inline — which lets
/// this test assert the outcome deterministically. Fast refill (rate=1000/s,
/// burst=1) so an idle bucket returns to its fresh baseline within the wait,
/// and `retain_recent` drops every bucket except the one the triggering
/// accept just used.
#[test]
fn over_cap_accept_prunes_refilled_keyspace() {
    let metrics = Arc::new(Metrics::new());
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: u32::MAX,
        per_source_rate_per_sec: 1000.0,
        per_source_burst: 1,
        // Small cap so a handful of distinct sources trips cap + cap/10 (== 2).
        max_tracked_sources: 2,
    };
    let limiter = ConnectionLimiter::new(&cfg, Arc::clone(&metrics));
    // Populate 8 distinct per-source buckets, well past cap + cap/10. In this
    // tight synchronous loop no bucket has time to refill, so the over-cap
    // sweeps that fire during it drop nothing and the keyspace grows to 8.
    for i in 0..8u8 {
        let _ = limiter.acquire_inner(Some(ip(10, 0, 0, i)));
    }
    assert_eq!(
        limiter.per_source_tracked(),
        8,
        "every distinct source should be tracked before the refill window"
    );
    // Let every populated bucket refill to its fresh baseline (~1ms each).
    std::thread::sleep(std::time::Duration::from_millis(50));
    // One more over-cap accept from a fresh source. Its bucket is now
    // non-baseline (a token was just drawn), so the sweep keeps it and drops
    // the 8 refilled buckets.
    let _ = limiter.acquire_inner(Some(ip(10, 0, 0, 200)));
    assert_eq!(
        limiter.per_source_tracked(),
        1,
        "the over-cap accept should prune the refilled keyspace back to only \
         the just-used bucket"
    );
}

/// `gc_per_source` is a no-op when the per-source layer is
/// disabled — exercises the early-return arm.
#[test]
fn gc_per_source_is_noop_when_layer_disabled() {
    let metrics = Arc::new(Metrics::new());
    let cfg = ResolvedSecurity {
        max_concurrent_handlers: u32::MAX,
        per_source_rate_per_sec: 0.0,
        per_source_burst: 0,
        max_tracked_sources: 0,
    };
    let limiter = ConnectionLimiter::new(&cfg, Arc::clone(&metrics));
    // Must not panic and must return None (layer disabled).
    assert!(limiter.gc_per_source().is_none());
    assert_eq!(limiter.per_source_tracked(), 0);
}

/// `PruneGuard` releases `pruning_in_progress` even when the
/// protected operation panics. Without this, a single panic inside
/// `retain_recent` (third-party code from `governor`, or a future
/// allocation failure during the walk) would leave the flag stuck
/// `true` and permanently disable both prune codepaths for the
/// lifetime of the process — the unbounded-keyspace failure mode
/// #440 is meant to prevent. We can't make `retain_recent` itself
/// panic on demand, so test the guard's Drop semantics directly:
/// a `catch_unwind` around a guard whose protected scope panics
/// must observe the flag reset to `false`.
#[test]
fn prune_guard_resets_flag_on_panic() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::{AtomicBool, Ordering};

    let flag = AtomicBool::new(false);
    flag.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .expect("uncontended CAS must succeed");

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _guard = super::PruneGuard(&flag);
        panic!("simulated retain_recent panic");
    }));
    assert!(
        result.is_err(),
        "panic should propagate out of catch_unwind"
    );
    assert!(
        !flag.load(Ordering::Acquire),
        "PruneGuard::drop must reset the flag during panic unwind"
    );
}

/// IPv6 addresses in the same /64 share a per-source bucket. Without
/// `/64` grouping an attacker with a customer-grade IPv6 allocation
/// can trivially defeat the per-source layer.
#[test]
fn per_source_buckets_ipv6_by_64_prefix() {
    use std::net::{IpAddr, Ipv6Addr};
    let metrics = Arc::new(Metrics::new());
    let limiter = ConnectionLimiter::new(&strict_security(u32::MAX), Arc::clone(&metrics));
    // Two distinct IPv6 addresses inside the same /64 prefix
    // (`2001:db8::1` and `2001:db8::ffff:ffff:ffff:ffff`). Without
    // /64 grouping these would use independent buckets.
    let v6_a = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1));
    let v6_b = IpAddr::V6(Ipv6Addr::new(
        0x2001, 0xdb8, 0, 0, 0xffff, 0xffff, 0xffff, 0xffff,
    ));
    let _p1 = limiter
        .acquire_inner(Some(v6_a))
        .expect("first acquire from /64");
    // burst=1 per-source — second acquire from any address in the
    // same /64 must reject.
    let err = limiter
        .acquire_inner(Some(v6_b))
        .expect_err("second IPv6 from same /64 must reject");
    assert_eq!(err, RejectReason::PerSource);

    // Address in a *different* /64 must succeed — proves the mask
    // isn't accidentally collapsing every IPv6 into one bucket.
    let v6_c = IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 1));
    let _p2 = limiter
        .acquire_inner(Some(v6_c))
        .expect("acquire from a different /64 must succeed");
}
