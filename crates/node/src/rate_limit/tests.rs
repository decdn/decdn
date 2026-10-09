use super::*;

// Eight sentinels, one per mapped field, so no two fields can be confused
// for each other by a transposed assignment. Shared by both `From` tests:
// the sentinel identifies the *slot*, and the two `Resolved*` types are
// what select the impl under test.
//
// Two properties make them work, and both are pinned rather than left to
// chance: pairwise distinctness (the `const _` asserts just below, so a swap
// always changes an asserted value) and no collision with either `Default`
// impl (`sentinels_never_collide_with_defaults`, so a slot that silently
// fell back to a default cannot pass for a correct map).
const S_PER_PEER_RATE: f64 = 11.0;
const S_PER_PEER_BURST: u32 = 22;
const S_PER_IP_RATE: f64 = 33.0;
const S_PER_IP_BURST: u32 = 44;
const S_GLOBAL_RATE: f64 = 55.0;
const S_GLOBAL_BURST: u32 = 66;
const S_MAX_TRACKED_PER_IP: usize = 77;
const S_MAX_TRACKED_PER_PEER: usize = 88;

// Cross-type swaps cannot compile (`f64`/`u32`/`usize` do not coerce in a
// struct literal), so distinctness only has to hold *within* each type
// group. A future edit that made two same-type sentinels equal would leave
// both `*_mapping_is_not_transposed` tests passing while silently
// disarming the guard for that pair — the compiler catches it here first.
const _: () = assert!(
    S_PER_PEER_RATE != S_PER_IP_RATE
        && S_PER_IP_RATE != S_GLOBAL_RATE
        && S_PER_PEER_RATE != S_GLOBAL_RATE,
    "the three f64 sentinels must stay pairwise distinct"
);
const _: () = assert!(
    S_PER_PEER_BURST != S_PER_IP_BURST
        && S_PER_IP_BURST != S_GLOBAL_BURST
        && S_PER_PEER_BURST != S_GLOBAL_BURST,
    "the three u32 sentinels must stay pairwise distinct"
);
const _: () = assert!(
    S_MAX_TRACKED_PER_IP != S_MAX_TRACKED_PER_PEER,
    "the two usize sentinels must stay pairwise distinct"
);

/// Assert the sentinels landed in their matching `RateLimitConfig` slots.
///
/// The destructure is exhaustive (no `..`) so this helper joins the arity
/// guard rather than opting out of it. Without it the impl literals would
/// force a ninth field to be *mapped* while leaving it unpinned — a slot
/// that is mapped but unasserted, which is where #1457 started.
///
/// The enforcement is two links, not one: a ninth field hard-errors here
/// (`E0027`, pattern does not mention field) until the pattern names it,
/// and CI's `CARGO_BUILD_WARNINGS=deny cargo clippy --workspace --all-targets` then
/// rejects the named-but-unread binding as `unused_variables`. Note that
/// two of the fixes rustc suggests for `E0027` — `field: _` and `..` —
/// silence both links with no diagnostic at all; adding an `assert_eq!` is
/// the only response that keeps the guard.
fn assert_sentinels_in_place(cfg: &RateLimitConfig) {
    let RateLimitConfig {
        per_peer_rate_per_sec,
        per_peer_burst,
        per_ip_rate_per_sec,
        per_ip_burst,
        global_rate_per_sec,
        global_burst,
        max_tracked_per_ip,
        max_tracked_per_peer,
    } = cfg;
    assert_eq!(
        *per_peer_rate_per_sec, S_PER_PEER_RATE,
        "per_peer_rate_per_sec"
    );
    assert_eq!(*per_peer_burst, S_PER_PEER_BURST, "per_peer_burst");
    assert_eq!(*per_ip_rate_per_sec, S_PER_IP_RATE, "per_ip_rate_per_sec");
    assert_eq!(*per_ip_burst, S_PER_IP_BURST, "per_ip_burst");
    assert_eq!(*global_rate_per_sec, S_GLOBAL_RATE, "global_rate_per_sec");
    assert_eq!(*global_burst, S_GLOBAL_BURST, "global_burst");
    assert_eq!(
        *max_tracked_per_ip, S_MAX_TRACKED_PER_IP,
        "max_tracked_per_ip"
    );
    assert_eq!(
        *max_tracked_per_peer, S_MAX_TRACKED_PER_PEER,
        "max_tracked_per_peer"
    );
}

/// `From<&ResolvedDht>` must not transpose same-typed siblings. The
/// compiler catches arity but not order: every one of the eight fields is
/// `f64`/`u32`/`usize`, so any swap within a type group still compiles
/// (#1457). The exhaustive `ResolvedDht` literal below is also what carries
/// the source-side arity guard — see the impl comment above.
#[test]
fn dht_mapping_is_not_transposed() {
    let resolved = decdn_common::config::ResolvedDht {
        per_peer_rate_per_sec: S_PER_PEER_RATE,
        per_peer_burst: S_PER_PEER_BURST,
        per_ip_rate_per_sec: S_PER_IP_RATE,
        per_ip_burst: S_PER_IP_BURST,
        global_rate_per_sec: S_GLOBAL_RATE,
        global_burst: S_GLOBAL_BURST,
        max_tracked_per_ip: S_MAX_TRACKED_PER_IP,
        max_tracked_per_peer: S_MAX_TRACKED_PER_PEER,
    };
    assert_sentinels_in_place(&RateLimitConfig::from(&resolved));
}

/// Sibling of `dht_mapping_is_not_transposed` for `From<&ResolvedProbe>`.
/// Load-bearing independently of the DHT test: the two `Self { .. }`
/// literals are byte-identical, so a transposition in one is invisible to
/// review, and the probe defaults are deliberately tighter per-peer
/// (ADR 005 §Probe rate limiting: 5 probes/sec against the per-IP 50). A
/// `per_peer`/`per_ip` swap here raises the per-peer cap to that 50 and
/// the per-peer burst from 5 to 200 — and the probe path is where that
/// hurts most, since probers are unstaked and can rotate `NodeId` for free
/// (ADR 005 §Why three layers), so the per-peer bucket is the only layer
/// charging them at all.
#[test]
fn probe_mapping_is_not_transposed() {
    let resolved = decdn_common::config::ResolvedProbe {
        per_peer_rate_per_sec: S_PER_PEER_RATE,
        per_peer_burst: S_PER_PEER_BURST,
        per_ip_rate_per_sec: S_PER_IP_RATE,
        per_ip_burst: S_PER_IP_BURST,
        global_rate_per_sec: S_GLOBAL_RATE,
        global_burst: S_GLOBAL_BURST,
        max_tracked_per_ip: S_MAX_TRACKED_PER_IP,
        max_tracked_per_peer: S_MAX_TRACKED_PER_PEER,
    };
    assert_sentinels_in_place(&RateLimitConfig::from(&resolved));
}

/// No sentinel may equal a value either `Default` impl can supply.
///
/// Pairwise distinctness (the `const _` asserts above) makes a *swap*
/// detectable; this makes a *fallback* detectable. If a mapping row were
/// ever rewritten to reach for a default instead of the resolved field, the
/// slot would hold a default value — which the sentinel assertions only
/// catch because no sentinel can be mistaken for one.
#[test]
fn sentinels_never_collide_with_defaults() {
    let sentinel_rates = [S_PER_PEER_RATE, S_PER_IP_RATE, S_GLOBAL_RATE];
    let sentinel_bursts = [S_PER_PEER_BURST, S_PER_IP_BURST, S_GLOBAL_BURST];
    let sentinel_tracked = [S_MAX_TRACKED_PER_IP, S_MAX_TRACKED_PER_PEER];

    // Both `Resolved*` defaults plus this engine's own, since a fallback
    // could plausibly reach for any of the three.
    let dht = RateLimitConfig::from(&decdn_common::config::ResolvedDht::default());
    let probe = RateLimitConfig::from(&decdn_common::config::ResolvedProbe::default());
    for cfg in [&RateLimitConfig::default(), &dht, &probe] {
        for rate in [
            cfg.per_peer_rate_per_sec,
            cfg.per_ip_rate_per_sec,
            cfg.global_rate_per_sec,
        ] {
            assert!(
                !sentinel_rates.contains(&rate),
                "default rate {rate} collides with an f64 sentinel"
            );
        }
        for burst in [cfg.per_peer_burst, cfg.per_ip_burst, cfg.global_burst] {
            assert!(
                !sentinel_bursts.contains(&burst),
                "default burst {burst} collides with a u32 sentinel"
            );
        }
        for tracked in [cfg.max_tracked_per_ip, cfg.max_tracked_per_peer] {
            assert!(
                !sentinel_tracked.contains(&tracked),
                "default cap {tracked} collides with a usize sentinel"
            );
        }
    }
}

/// `RateLimitConfig::default()` must stay equal to the DHT resolved
/// defaults, as this module's docs claim (the engine `Default` is
/// documented as "the ADR 022 DHT-layer values").
///
/// The two live in different crates with no shared constant, so nothing but
/// this test ties them together. Drift would mean every fixture that builds
/// a limiter from `RateLimitConfig::default()` silently exercises a
/// configuration the resolver never produces.
#[test]
fn dht_default_matches_engine_default() {
    let from_resolved = RateLimitConfig::from(&decdn_common::config::ResolvedDht::default());
    let engine = RateLimitConfig::default();
    assert_eq!(
        engine.per_peer_rate_per_sec,
        from_resolved.per_peer_rate_per_sec
    );
    assert_eq!(engine.per_peer_burst, from_resolved.per_peer_burst);
    assert_eq!(
        engine.per_ip_rate_per_sec,
        from_resolved.per_ip_rate_per_sec
    );
    assert_eq!(engine.per_ip_burst, from_resolved.per_ip_burst);
    assert_eq!(
        engine.global_rate_per_sec,
        from_resolved.global_rate_per_sec
    );
    assert_eq!(engine.global_burst, from_resolved.global_burst);
    assert_eq!(engine.max_tracked_per_ip, from_resolved.max_tracked_per_ip);
    assert_eq!(
        engine.max_tracked_per_peer,
        from_resolved.max_tracked_per_peer
    );
}

/// Mirrors `dispatch.rs::prune_guard_resets_flag_on_panic`: a panic inside
/// the guarded section must still release the single-flight flag via
/// `Drop`. Without this, a single panic inside `retain_recent` would
/// permanently disable the prune codepath for the lifetime of the process
/// — the unbounded-keyspace failure #645 exists to prevent.
#[test]
fn prune_guard_resets_flag_on_panic() {
    let flag = AtomicBool::new(false);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert!(
            flag.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        );
        let _guard = PruneGuard(&flag);
        panic!("simulated panic inside retain_recent");
    }));
    assert!(result.is_err(), "panic was caught");
    assert!(
        !flag.load(Ordering::Acquire),
        "PruneGuard::drop must release the flag during unwind"
    );
}

/// Layer label strings are pinned because operators alert on these values.
#[test]
fn layer_label_strings_are_stable() {
    assert_eq!(RejectLayer::PerPeer.as_str(), "per_peer");
    assert_eq!(RejectLayer::PerIp.as_str(), "per_ip");
    assert_eq!(RejectLayer::Global.as_str(), "global");
}
