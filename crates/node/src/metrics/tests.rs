use std::fs;
use std::path::Path;
use std::time::Duration;

use super::*;

/// Match an exact `<name> <value>` metric line, anchored against
/// surrounding lines so `decdn_cache_hits_total 1` doesn't
/// accidentally substring-match into a future
/// `decdn_cache_hits_total_foo` series or the `OpenMetrics`
/// `_created` companion line.
fn has_metric_line(text: &str, name: &str, value: u64) -> bool {
    let needle = format!("{name} {value}");
    text.lines().any(|l| l == needle)
}

/// Parse the `u64` value of an exact-named series from encoded
/// `OpenMetrics` text. Applies the same whole-line discipline as
/// [`has_metric_line`] — the name must be followed by a single space — so
/// `decdn_x 5` never matches a `decdn_x_total`/`decdn_x_foo` sibling.
/// Returns `None` if the series is absent or its value doesn't parse.
fn metric_value(text: &str, name: &str) -> Option<u64> {
    text.lines()
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.parse::<u64>().ok())
}

/// Every latency histogram exports its full bucket set at zero from startup,
/// and one observation lands in the right bucket, the sum and the count.
/// Also proves that `exported_series` folds the histogram samples back to
/// the base name that a registry row names.
#[test]
fn latency_histograms_export_buckets_sum_and_count() {
    const HISTOGRAMS: [&str; 10] = [
        "decdn_probe_collection_latency_seconds",
        "decdn_serve_first_byte_hit_seconds",
        "decdn_serve_first_byte_miss_seconds",
        "decdn_node_pull_first_byte_seconds",
        "decdn_node_pull_through_wait_seconds",
        "decdn_serve_admission_seconds",
        "decdn_serve_response_hit_seconds",
        "decdn_serve_response_miss_seconds",
        "decdn_serve_first_frame_hit_seconds",
        "decdn_serve_first_frame_miss_seconds",
    ];
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();
    let exported = exported_series(&text);
    for name in HISTOGRAMS {
        let type_line = format!("# TYPE {name} histogram");
        assert!(
            text.lines().any(|l| l == type_line),
            "missing `{type_line}`"
        );
        assert!(
            has_metric_line(&text, &format!("{name}_bucket{{le=\"+Inf\"}}"), 0),
            "{name} has no +Inf bucket at zero"
        );
        assert!(has_metric_line(&text, &format!("{name}_count"), 0));
        assert!(
            exported.contains(name),
            "exported_series did not fold {name}"
        );
    }

    // 40 ms: above the 0.025 bucket, inside the 0.05 bucket of both sets.
    let elapsed = Duration::from_millis(40);
    metrics.probe_collection_latency(elapsed);
    metrics.node_pull_first_byte(elapsed);
    let now = Instant::now();
    FirstByteClock::new(now, now, RequestClass::CacheHit).record(&metrics);
    let text = metrics.encode().unwrap();
    for name in [HISTOGRAMS[0], HISTOGRAMS[3]] {
        assert!(has_metric_line(
            &text,
            &format!("{name}_bucket{{le=\"0.025\"}}"),
            0
        ));
        assert!(has_metric_line(
            &text,
            &format!("{name}_bucket{{le=\"0.05\"}}"),
            1
        ));
        assert!(has_metric_line(
            &text,
            &format!("{name}_bucket{{le=\"+Inf\"}}"),
            1
        ));
        assert!(has_metric_line(&text, &format!("{name}_count"), 1));
        let sum_line = format!("{name}_sum 0.04");
        assert!(text.lines().any(|l| l == sum_line), "missing `{sum_line}`");
    }
    // The clock records into the sibling of its class only.
    assert!(has_metric_line(
        &text,
        "decdn_serve_first_byte_hit_seconds_count",
        1
    ));
    assert!(has_metric_line(
        &text,
        "decdn_serve_first_byte_miss_seconds_count",
        0
    ));
}

/// The clock records each serve phase once, into the class sibling where the
/// phase has one. A clock that never saw its response written records the
/// admission phase and the total, and leaves the two later phases empty.
#[test]
fn first_byte_clock_records_each_phase_once() {
    let metrics = Metrics::new();
    let now = Instant::now();
    let mut miss = FirstByteClock::new(now, now, RequestClass::CacheMiss);
    miss.mark_responded();
    miss.record(&metrics);
    FirstByteClock::new(now, now, RequestClass::CacheHit).record(&metrics);

    let text = metrics.encode().unwrap();
    for (name, count) in [
        ("decdn_serve_admission_seconds_count", 2),
        ("decdn_serve_response_miss_seconds_count", 1),
        ("decdn_serve_response_hit_seconds_count", 0),
        ("decdn_serve_first_frame_miss_seconds_count", 1),
        ("decdn_serve_first_frame_hit_seconds_count", 0),
        ("decdn_serve_first_byte_miss_seconds_count", 1),
        ("decdn_serve_first_byte_hit_seconds_count", 1),
    ] {
        assert!(has_metric_line(&text, name, count), "{name} != {count}");
    }
}

/// A client waits on a cache miss for at most the outer pull deadline, so a
/// first byte at the default timeouts must land in a finite bucket.
#[test]
fn first_byte_buckets_reach_past_the_default_outer_pull_deadline() {
    use decdn_common::config::{DEFAULT_NODE_PULL_STALL_WINDOW_SEC, DEFAULT_NODE_PULL_TIMEOUT_SEC};
    let deadline = crate::selection::outer_pull_deadline(
        Duration::from_secs(DEFAULT_NODE_PULL_TIMEOUT_SEC),
        Duration::from_secs(DEFAULT_NODE_PULL_STALL_WINDOW_SEC),
    );
    let top = FIRST_BYTE_BUCKETS.last().copied().unwrap_or_default();
    assert!(
        top > deadline.as_secs_f64(),
        "top first-byte bucket {top}s does not reach the default outer pull deadline \
         {deadline:?}"
    );
}

#[test]
fn key_rotation_metrics_export_canonical_names_at_zero() {
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();

    for name in [
        "decdn_streams_active{direction=\"inbound\"}",
        "decdn_streams_active{direction=\"outbound\"}",
        "decdn_lanes_open",
        "decdn_pool_deposit_usdc",
        "decdn_unredeemed_usdc",
        "decdn_node_uptime_seconds",
    ] {
        assert!(
            has_metric_line(&text, name, 0),
            "canonical gauge {name} should be exposed at zero:\n{text}"
        );
    }
    assert!(
        !text
            .lines()
            .any(|line| line.starts_with("decdn_uptime_seconds ")),
        "retired uptime name must not be exported:\n{text}"
    );
}

#[test]
fn set_unredeemed_usdc_publishes_the_total() {
    // The redeemer sweep hands this setter the summed `owed − paid` across
    // the node's lanes; the gauge must reflect exactly that raw value so
    // `decdn node top` and dashboards report the pending redemption.
    let metrics = Metrics::new();
    metrics.set_unredeemed_usdc(U256::from(12_345u64));
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_unredeemed_usdc", 12_345),
        "gauge must publish the set total:\n{text}"
    );
    // A later, smaller sweep result replaces (not accumulates) the value —
    // a gauge, so a redemption that draws the total down is visible.
    metrics.set_unredeemed_usdc(U256::from(42u64));
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_unredeemed_usdc", 42),
        "gauge must overwrite on the next sweep:\n{text}"
    );
}

#[test]
fn probe_hold_unavailable_exports_every_reason_at_zero() {
    // Pins the pre-materialization in `Metrics::new` (#1443). A `Family`
    // creates each child series lazily on `get_or_create`, so without that
    // step a `reason` would be missing from `/metrics` until it first
    // fired — a dashboard gap where the three `reason` series of
    // `decdn_probe_hold_unavailable_total` read absent instead of zero, and
    // a silent hole in the `DecdnProbeHoldViolations` alert's
    // input. Also pins the rendered series text (label name, snake_case
    // value encoding, and the `_total` suffix the encoder appends).
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();

    for reason in ["exhausted", "disabled", "stake_lane_reserved"] {
        let name = format!("decdn_probe_hold_unavailable_total{{reason=\"{reason}\"}}");
        assert!(
            has_metric_line(&text, &name, 0),
            "{name} should be exposed at zero from a fresh registry:\n{text}"
        );
    }

    // None of these three names is a valid export; probe-hold reasons ship
    // only as `reason` labels on the collapsed counter
    // `decdn_probe_hold_unavailable_total`.
    for retired in [
        "decdn_probe_hold_violations_total",
        "decdn_probe_holds_disabled_total",
        "decdn_probe_stake_lane_reserved_total",
    ] {
        assert!(
            !text.lines().any(|line| line.starts_with(retired)),
            "retired metric name {retired} must not be exported:\n{text}"
        );
    }
}

/// Every `(method, outcome)` child of `decdn_rpc_requests_total` and every
/// per-method duration histogram exports at zero from a fresh registry, so
/// an error-ratio panel has a series before the first failure. A recorded
/// request lands on its own child only, and a dropped one records no
/// duration sample.
#[test]
fn rpc_request_series_export_at_zero_and_record_per_label() {
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();
    for method in RpcMethod::ALL {
        let method = method.as_str();
        for outcome in [
            "ok",
            "reverted",
            "rpc_error",
            "rate_limited",
            "transport_error",
            "timeout",
        ] {
            let name =
                format!("decdn_rpc_requests_total{{method=\"{method}\",outcome=\"{outcome}\"}}");
            assert!(has_metric_line(&text, &name, 0), "{name} missing:\n{text}");
        }
        let count = format!("decdn_rpc_request_duration_seconds_count{{method=\"{method}\"}}");
        assert!(has_metric_line(&text, &count, 0), "{count} missing");
    }
    assert_eq!(RpcOutcome::ALL.len(), 6, "extend the outcome list above");

    metrics.rpc_request(
        RpcMethod::EthGetLogs,
        RpcOutcome::RateLimited,
        Some(Duration::from_millis(40)),
    );
    metrics.rpc_request(RpcMethod::EthCall, RpcOutcome::Timeout, None);
    let text = metrics.encode().unwrap();
    let requests = |method: &str, outcome: &str| {
        metric_value(
            &text,
            &format!("decdn_rpc_requests_total{{method=\"{method}\",outcome=\"{outcome}\"}}"),
        )
    };
    assert_eq!(requests("eth_getLogs", "rate_limited"), Some(1));
    assert_eq!(requests("eth_getLogs", "ok"), Some(0));
    assert_eq!(requests("eth_call", "timeout"), Some(1));
    assert!(has_metric_line(
        &text,
        "decdn_rpc_request_duration_seconds_bucket{method=\"eth_getLogs\",le=\"0.05\"}",
        1
    ));
    assert!(has_metric_line(
        &text,
        "decdn_rpc_request_duration_seconds_count{method=\"eth_call\"}",
        0
    ));
}

/// A request slower than every finite bucket is one the per-call deadline
/// drops, so the top bucket reaches the deadline.
#[test]
fn rpc_request_buckets_reach_the_per_call_deadline() {
    let top = RPC_REQUEST_BUCKETS.last().copied().unwrap_or_default();
    assert!(top >= crate::chain_events::DEFAULT_RPC_CALL_TIMEOUT.as_secs_f64());
}

/// Every series name the encoder actually emits, label suffixes stripped.
///
/// Built from **sample** lines, not `# TYPE` lines. In `OpenMetrics` a
/// counter's TYPE line carries the *unsuffixed* stem (`# TYPE
/// decdn_cache_hits counter`) while the sample is `decdn_cache_hits_total
/// 0` — so parsing TYPE would blind the gate to the `_total` suffix, which
/// is the single most common way a documented name goes wrong here. (The
/// convention is that a counter field omits `_total` and lets the encoder
/// append it; the encoder only appends when it is absent, so a field that
/// spells it out explicitly still exports correctly. Follow the
/// convention in new code, but it is not a hard rule.)
///
/// Truncating at the first `{` folds labelled families down to their base
/// name, so `decdn_streams_active{direction="inbound"} 0` registers as
/// `decdn_streams_active` — a name [`has_metric_line`] cannot match
/// because it requires an exact value-bearing line.
///
/// Histograms get their `_bucket`/`_sum`/`_count` samples folded back to
/// the base name too. A histogram has no bare `name` sample, so without
/// this every `live` histogram row would fail the registry gate. The raw
/// sample names stay in the set as well.
fn exported_series(text: &str) -> std::collections::HashSet<String> {
    text.lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .filter_map(|l| {
            let name = l.split(['{', ' ']).next()?;
            (!name.is_empty()).then(|| name.to_string())
        })
        .flat_map(|name| {
            let base = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|sfx| name.strip_suffix(sfx))
                .map(str::to_string);
            std::iter::once(name).chain(base)
        })
        .collect()
}

/// Every `live` row in the ADR metric registry must resolve to an exported
/// series. `adr/appendix-observability.md` calls itself the canonical
/// registry, so a row naming a series the node never emits sends operators
/// off to build a dashboard that renders `(no data)` — which is what a
/// stale registry row did until #1513.
///
/// Rows whose Status column says `planned` are skipped: the registry is
/// allowed to record design intent, it is just not allowed to do so
/// silently. That column is the allowlist, and it is the reason this gate
/// can be absolute rather than carrying a hand-maintained skip list that
/// would rot the same way the names did.
#[test]
fn adr_registry_names_are_exported() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let text = fs::read_to_string(root.join("adr/appendix-observability.md")).unwrap();
    let exported = exported_series(&Metrics::new().encode().unwrap());

    let mut checked = 0usize;
    let mut stale: Vec<String> = Vec::new();
    for line in text.lines() {
        // A registry row is `| <name> | <Type> | <Tier> | <Status> | … |`.
        // Keying on the *Type* cell is what separates registry rows from
        // the other `decdn_*`-bearing tables in this file — the alert
        // thresholds (`| Metric | Warning | Critical | Action |`), the
        // health-endpoint JSON mapping, and the informal-name
        // cross-reference — without hard-coding section boundaries.
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        let (Some(metric), Some(kind), Some(status)) = (cells.get(1), cells.get(2), cells.get(4))
        else {
            continue;
        };
        if !matches!(*kind, "Counter" | "Gauge" | "Histogram") {
            continue;
        }
        let Some(name) = metric.strip_prefix('`').and_then(|m| m.split('`').next()) else {
            continue;
        };
        // Documentation forms, not single series: label suffixes and
        // brace-expanded shorthand (`..._{per_peer,per_ip}_total`), and the
        // per-watcher templates (`decdn_<watcher>_task_panicked_total`),
        // which stand in for one row per watcher rather than naming one.
        if name.contains('{') || name.contains('<') {
            continue;
        }
        if *status == "planned" {
            continue;
        }
        assert_eq!(
            *status, "live",
            "row for {name} has Status {status:?}; the only values are `live` and `planned`"
        );
        checked += 1;
        if !exported.contains(name) {
            stale.push(name.to_string());
        }
    }

    // Floor sized just under the real count (47 live rows today). `> 20`
    // would tolerate a table-shape change that silently dropped more than
    // half the registry — the exact rot this gate exists to catch.
    assert!(
        checked >= 40,
        "only {checked} registry rows parsed — the table shape changed and this \
         gate silently stopped covering the registry"
    );
    assert!(
        stale.is_empty(),
        "adr/appendix-observability.md documents series the exporter does not emit: \
         {stale:?}\nEither correct the name or mark the row `planned` in its Status \
         column."
    );
}

/// A republish overwrites counts and removes a region that left, rather
/// than leaving it exported at its last value.
#[test]
fn staker_set_active_by_region_replaces_the_previous_map() {
    let metrics = Metrics::new();
    let de = Region::parse("DE").unwrap();
    let us = Region::parse("US").unwrap();
    metrics.staker_set_active_by_region(&BTreeMap::from([(de, 2), (us, 1)]), 1);
    metrics.staker_set_active_by_region(&BTreeMap::from([(de, 1)]), 0);

    let text = metrics.encode().unwrap();
    let has = |line: &str| text.lines().any(|l| l == line);
    assert!(
        has(r#"decdn_staker_set_active_by_region{node_region="DE"} 1"#),
        "{text}"
    );
    assert!(
        !text.contains(r#"node_region="US""#),
        "a departed region kept its series"
    );
    assert!(has("decdn_staker_set_active_unknown_region 0"), "{text}");
}

#[test]
fn probe_hold_unavailable_increments_only_the_named_reason() {
    // The whole point of the label is that each value keeps its own
    // remedy: `exhausted` drives "raise max_probe_holds", `disabled` is an
    // intentional operator choice, and `stake_lane_reserved` is a priority
    // decision taken before the cache is even consulted. Bumping one must
    // never move another, or the alert filter re-fires on the deliberate
    // cases the #739/#757 split existed to keep out.
    let metrics = Metrics::new();
    metrics.probe_hold_unavailable(ProbeHoldUnavailableReason::Exhausted);
    let text = metrics.encode().unwrap();

    assert!(
        has_metric_line(
            &text,
            "decdn_probe_hold_unavailable_total{reason=\"exhausted\"}",
            1
        ),
        "the named reason must increment:\n{text}"
    );
    for untouched in ["disabled", "stake_lane_reserved"] {
        let name = format!("decdn_probe_hold_unavailable_total{{reason=\"{untouched}\"}}");
        assert!(
            has_metric_line(&text, &name, 0),
            "{name} must stay at zero:\n{text}"
        );
    }
}

#[test]
fn uptime_counts_from_construction_without_any_bring_up_hook() {
    // Regression guard (#1264): `decdn_node_uptime_seconds` derives solely
    // from `started_at`, which `Metrics::new()` stamps at construction, and
    // `encode()` recomputes it every scrape. There is no recorder that
    // marks "started" — a re-introduced `set(0)` (or any pre-scrape write)
    // would be unobservable and is exactly the dead surface this issue
    // removed. Backdate `started_at` and scrape WITHOUT calling any hook:
    // uptime must reflect the backdate rather than reset to 0.
    //
    // `Instant` is monotonic (boot-relative), so the backdate stays small
    // to keep `checked_sub` from underflowing to `None` on a freshly-booted
    // CI host, and the check is a range rather than an exact second count so
    // scrape-time `as_secs()` truncation can't race it.
    const BACKDATE_SECS: u64 = 60;
    let mut metrics = Metrics::new();
    metrics.started_at = Instant::now()
        .checked_sub(Duration::from_secs(BACKDATE_SECS))
        .unwrap_or_else(Instant::now);

    let text = metrics.encode().unwrap();
    let uptime = metric_value(&text, "decdn_node_uptime_seconds").unwrap();
    assert!(
        (BACKDATE_SECS..3600).contains(&uptime),
        "uptime must count from construction (started_at ~{BACKDATE_SECS}s ago), got {uptime}:\n{text}"
    );
}

#[test]
fn stream_guards_track_direction_and_drop_on_error() {
    fn inbound_error(metrics: &Metrics) -> Result<(), ()> {
        let _guard = metrics.inbound_stream_guard();
        let text = metrics.encode().unwrap();
        assert!(has_metric_line(
            &text,
            "decdn_streams_active{direction=\"inbound\"}",
            1
        ));
        Err(())
    }

    let metrics = Metrics::new();
    assert!(inbound_error(&metrics).is_err());
    assert!(has_metric_line(
        &metrics.encode().unwrap(),
        "decdn_streams_active{direction=\"inbound\"}",
        0
    ));

    {
        let _first = metrics.outbound_stream_guard();
        let _second = metrics.outbound_stream_guard();
        assert!(has_metric_line(
            &metrics.encode().unwrap(),
            "decdn_streams_active{direction=\"outbound\"}",
            2
        ));
    }
    assert!(has_metric_line(
        &metrics.encode().unwrap(),
        "decdn_streams_active{direction=\"outbound\"}",
        0
    ));
}

#[test]
fn cache_metrics_counters_start_at_zero() {
    // Pinning down the OpenMetrics shape — a fresh registry must
    // expose the cache counters at zero so dashboards built before
    // any fetch has fired don't render `(no data)`.
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();
    for name in [
        "decdn_cache_origin_fetches_total",
        "decdn_cache_origin_retry_exhausted_total",
        "decdn_cache_origin_fallback_total",
        "decdn_cache_hits_total",
        "decdn_cache_misses_total",
        "decdn_cache_bytes_returned_total",
        "decdn_cache_pull_through_bytes_total",
        // GC counters (#518). The Rust struct fields are `gc_runs`
        // / `gc_bytes_reclaimed`; the OpenMetrics encoder appends
        // `_total`. Asserting the suffixed forms locks in the
        // exported names — a regression that re-renamed the
        // struct fields to include `_total` would emit
        // `..._total_total`, breaking dashboards/alerts that
        // reference the names below.
        "decdn_cache_gc_runs_total",
        "decdn_cache_gc_bytes_reclaimed_total",
        // Circuit-breaker counters (#963). Auto-exposed via the
        // `MetricsGroup` derive; pin the exported names so dashboards
        // tracking origin-outage load-shed don't silently lose them.
        "decdn_cache_circuit_breaker_trips_total",
        "decdn_cache_circuit_breaker_recoveries_total",
        "decdn_cache_circuit_breaker_short_circuits_total",
        // Eviction-driver counters (#1173). Same `_total`-suffix trap as
        // the GC pair above: the struct fields are `evictions`,
        // `evictions_bytes`, `evictions_starved`, `size_measure_failures`,
        // `recency_seed_failures`, `evicted_operator`.
        "decdn_cache_evictions_total",
        "decdn_cache_evictions_bytes_total",
        "decdn_cache_evictions_starved_total",
        "decdn_cache_size_measure_failures_total",
        "decdn_cache_recency_seed_failures_total",
        "decdn_cache_evicted_operator_total",
        // Per-partial `observe()` failures inside the size walk. The struct
        // field is `partial_size_observe_failures`.
        "decdn_cache_partial_size_observe_failures_total",
        // Serve-detected stored corruption. The struct field is
        // `held_corruption_quarantined`.
        "decdn_cache_held_corruption_quarantined_total",
        // Best-effort tag-deletion failures. The array is hand-maintained, so
        // a new `CacheMetrics` field is covered only if someone adds it.
        "decdn_cache_tag_drop_failures_total",
        // In-flight coalescing mutex poison (#1517). The struct field is
        // `inflight_mutex_poisoned`. Any nonzero value is a bug report, so
        // the series must exist from a fresh registry — an operator has to
        // be able to alert on `> 0` before it has ever fired.
        "decdn_cache_inflight_mutex_poisoned_total",
        // Origin-rescan probe faults. The struct field is
        // `origin_probe_failures`. The announce set shrinks silently
        // without it, so an operator has to be able to alert on `> 0`
        // before it has ever fired.
        "decdn_cache_origin_probe_failures_total",
        // The other leg of the same rescan. The struct field is
        // `origin_enumerate_failures`.
        "decdn_cache_origin_enumerate_failures_total",
    ] {
        assert!(
            has_metric_line(&text, name, 0),
            "counter {name} should be exposed at zero on a fresh registry:\n{text}"
        );
    }

    // Cache-health GAUGES (#1173) take the opposite naming rule: the
    // encoder appends `_total` only to counters, so these must appear
    // WITHOUT a suffix. Asserting both families together pins the
    // distinction that `crates/cache/src/metrics.rs`'s module doc describes.
    for name in [
        "decdn_cache_bytes",
        "decdn_cache_size_limit_bytes",
        "decdn_cache_pinned_count",
    ] {
        assert!(
            has_metric_line(&text, name, 0),
            "gauge {name} should be exposed at zero on a fresh registry:\n{text}"
        );
    }
}

#[test]
fn quic_0rtt_and_session_ticket_series_are_not_exposed() {
    // The probe path is a plain QUIC handshake: no early-data
    // classification and no session-ticket working-set proxy, so no
    // metric may name either. A partial revert that reintroduces one
    // counter but not its recorder shows up here.
    //
    // The needles are matched against the whole exposition, which also
    // carries iroh's transport metrics — so an upstream series named
    // `*0rtt*` would trip this too. That breadth is deliberate: this
    // node claims to expose no early-data accounting at all, whoever
    // registered it.
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();
    for needle in ["0rtt", "session_ticket"] {
        assert!(
            !text.contains(needle),
            "exposition must not carry a {needle} series:\n{text}"
        );
    }
}

#[test]
fn voucher_nonce_gap_metric_starts_at_zero_and_increments() {
    // #747. The struct field is `voucher_nonce_gaps`; the OpenMetrics
    // encoder appends `_total`, so the exported name is
    // `decdn_voucher_nonce_gaps_total` — the operator-visible name the
    // `apply_voucher` docs and any alert reference. Asserting the suffixed
    // form locks it in: re-naming the field to include `_total` would emit
    // `..._total_total` (the same footgun the cache GC counters guard
    // against above). The counter is event-scoped — `voucher_nonce_gap()`
    // bumps it once per gapped voucher regardless of gap size — so two
    // calls must read exactly 2.
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_voucher_nonce_gaps_total", 0),
        "voucher nonce-gap counter should be exposed at zero on a fresh registry:\n{text}"
    );

    metrics.voucher_nonce_gap();
    metrics.voucher_nonce_gap();

    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_voucher_nonce_gaps_total", 2),
        "expected 2 gap events (one bump each, not gap-size weighted):\n{text}"
    );
}

#[test]
fn buyer_pool_skipped_undecodable_metric_starts_at_zero_and_increments() {
    // #1271. The struct field is `buyer_pool_store_skipped_undecodable_records`;
    // the OpenMetrics encoder appends `_total`, so the exported name is
    // `decdn_buyer_pool_store_skipped_undecodable_records_total` — the
    // operator-visible name the observability appendix and any escrowed-but-
    // untracked alert reference. Pin the suffixed form: a rename that re-added
    // `_total` would emit `..._total_total` (the same footgun the cache GC and
    // voucher-nonce counters guard against), silently dropping the alert. The
    // counter takes a per-load skipped count, so `inc_by(2)` must read 2.
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(
            &text,
            "decdn_buyer_pool_store_skipped_undecodable_records_total",
            0
        ),
        "skipped-undecodable counter should be exposed at zero on a fresh registry:\n{text}"
    );

    metrics.buyer_pool_store_skipped_undecodable_records(2);

    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(
            &text,
            "decdn_buyer_pool_store_skipped_undecodable_records_total",
            2
        ),
        "expected the per-load skipped count (2) to increment the counter:\n{text}"
    );
}

#[test]
fn serve_stream_rejected_counters_start_at_zero_and_increment_per_reason() {
    // #876. Each `serve_stream` reject branch maps to a distinct counter
    // because a plain counter field carries no label dimension and the wire
    // `StreamError` deliberately conflates the three `NotFound` reasons.
    // The exported names carry the encoder-appended `_total` suffix. All
    // must be exposed at zero on a fresh registry (so dashboards don't read
    // `(no data)`) and each method must bump exactly its own counter.
    let metrics = Metrics::new();
    let reasons = [
        "decdn_serve_stream_rejected_evicted_since_probe_total",
        "decdn_serve_stream_rejected_cache_miss_total",
        "decdn_serve_stream_rejected_internal_error_total",
        "decdn_serve_stream_rejected_unknown_lane_total",
        "decdn_serve_stream_rejected_owner_mismatch_total",
        "decdn_serve_stream_rejected_insufficient_deposit_total",
        "decdn_serve_stream_rejected_pool_unconfirmed_total",
        "decdn_serve_stream_rejected_pool_closing_total",
        "decdn_serve_stream_rejected_signer_cap_exhausted_total",
        "decdn_serve_stream_midstream_signer_cap_exhausted_total",
        "decdn_serve_stream_rejected_signer_floor_at_cap_total",
        "decdn_serve_stream_rejected_pull_loop_guard_total",
        // Completed in #1520. These four always exported (the fields have
        // existed as long as their siblings) — what was missing was any
        // assertion pinning it, so a rename could have silently broken a
        // dashboard without failing this test.
        "decdn_serve_stream_rejected_range_not_satisfiable_total",
        "decdn_serve_stream_rejected_blob_too_large_total",
        "decdn_serve_stream_rejected_hash_denied_total",
        "decdn_serve_stream_rejected_chain_hash_denied_total",
        "decdn_serve_stream_rejected_origin_denied_total",
        "decdn_serve_stream_rejected_foreign_declined_total",
        "decdn_serve_stream_rejected_chain_stale_total",
    ];
    let text = metrics.encode().unwrap();
    for name in reasons {
        assert!(
            has_metric_line(&text, name, 0),
            "reject counter {name} should be exposed at zero on a fresh registry:\n{text}"
        );
    }

    metrics.serve_stream_rejected_evicted_since_probe();
    metrics.serve_stream_rejected_cache_miss();
    metrics.serve_stream_rejected_internal_error();
    metrics.serve_stream_rejected_unknown_lane();
    metrics.serve_stream_rejected_owner_mismatch();
    metrics.serve_stream_rejected_insufficient_deposit();
    metrics.serve_stream_rejected_pool_unconfirmed();
    metrics.serve_stream_rejected_pool_closing();
    metrics.serve_stream_rejected_signer_cap_exhausted();
    metrics.serve_stream_midstream_signer_cap_exhausted();
    metrics.serve_stream_rejected_signer_floor_at_cap();
    metrics.serve_stream_rejected_pull_loop_guard();
    metrics.serve_stream_rejected_range_not_satisfiable();
    metrics.serve_stream_rejected_blob_too_large();
    metrics.serve_stream_rejected_hash_denied();
    metrics.serve_stream_rejected_chain_hash_denied();
    metrics.serve_stream_rejected_origin_denied();
    metrics.serve_stream_rejected_foreign_declined();
    metrics.serve_stream_rejected_chain_stale();

    let text = metrics.encode().unwrap();
    for name in reasons {
        assert!(
            has_metric_line(&text, name, 1),
            "reject counter {name} should read exactly 1 after one bump:\n{text}"
        );
    }
}

#[test]
fn load_shed_metrics_appear_in_scrape() {
    let metrics = Metrics::new();
    metrics.serve_stream_rejected_load_shed_hit();
    metrics.serve_stream_rejected_load_shed_miss();
    metrics.load_shed_egress_bps(1_234);
    metrics.load_shed_pressure_active(true);
    metrics.load_shed_streams_in_flight(7);
    let text = metrics.encode().unwrap();
    assert!(
        text.contains("decdn_serve_stream_rejected_load_shed_hit_total 1"),
        "{text}"
    );
    assert!(
        text.contains("decdn_serve_stream_rejected_load_shed_miss_total 1"),
        "{text}"
    );
    assert!(text.contains("decdn_load_shed_egress_bps 1234"), "{text}");
    assert!(text.contains("decdn_load_shed_pressure_active 1"), "{text}");
    assert!(
        has_metric_line(&text, "decdn_load_shed_streams_in_flight", 7),
        "{text}"
    );
}

#[test]
fn load_shed_refused_counters_start_at_zero_and_increment_per_reason() {
    use crate::load_shed::ShedReason;
    // Reason splits are siblings, not labels (ADR § Reason splits): each
    // shed reason has an unrelated remedy, so bumping one must not move
    // another, and all three must export at zero from a fresh registry.
    let metrics = Metrics::new();
    let names = [
        "decdn_load_shed_refused_node_at_capacity_total",
        "decdn_load_shed_refused_egress_saturated_total",
        "decdn_load_shed_refused_client_at_capacity_total",
    ];
    let text = metrics.encode().unwrap();
    for name in names {
        assert!(
            has_metric_line(&text, name, 0),
            "{name} not at zero:\n{text}"
        );
    }

    metrics.load_shed_refused(ShedReason::NodeAtCapacity);
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_load_shed_refused_node_at_capacity_total", 1),
        "the named reason must increment:\n{text}"
    );
    for untouched in [
        "decdn_load_shed_refused_egress_saturated_total",
        "decdn_load_shed_refused_client_at_capacity_total",
    ] {
        assert!(
            has_metric_line(&text, untouched, 0),
            "{untouched} must stay at zero:\n{text}"
        );
    }
}

#[test]
fn serve_economics_refused_splits_by_regime_and_bumps_the_aggregate() {
    // Mirrors the `pool_open_failures_*` shape: an aggregate plus regime
    // siblings, all exported at zero from a fresh registry. Each refusal
    // bumps the aggregate and exactly one regime sibling.
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();
    for name in [
        "decdn_serve_economics_refused_total",
        "decdn_serve_economics_refused_warming_total",
        "decdn_serve_economics_refused_amortized_total",
    ] {
        assert!(
            has_metric_line(&text, name, 0),
            "{name} not at zero:\n{text}"
        );
    }

    metrics.serve_economics_refused(ServeEconomicsRegime::Warming);
    metrics.serve_economics_refused(ServeEconomicsRegime::Amortized);
    metrics.serve_economics_refused(ServeEconomicsRegime::Amortized);
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_serve_economics_refused_total", 3),
        "aggregate must sum both regimes:\n{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_serve_economics_refused_warming_total", 1),
        "{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_serve_economics_refused_amortized_total", 2),
        "{text}"
    );
}

#[test]
fn warming_block_metrics_start_at_zero_and_increment() {
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_warming_speculative_blocked_total", 0),
        "{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_warming_sources_blocked", 0),
        "{text}"
    );
    metrics.warming_speculative_blocked();
    metrics.warming_sources_blocked(4);
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_warming_speculative_blocked_total", 1),
        "{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_warming_sources_blocked", 4),
        "{text}"
    );
}

/// Every counter and gauge this node exposes for stream outcomes, byte
/// volume, on-chain transactions, DHT health and operator-path failures
/// exports at zero from a fresh registry, so an alert or a rate over it
/// never starts absent.
#[test]
fn observability_counters_export_at_zero() {
    let text = Metrics::new().encode().unwrap();
    for name in [
        "decdn_bytes_served_total",
        "decdn_bytes_received_total",
        "decdn_pool_redemptions_total",
        "decdn_vouchers_received_total",
        "decdn_preimage_reveals_received_total",
        "decdn_pool_grace_closes_total",
        "decdn_onchain_tx_landed_total",
        "decdn_onchain_tx_reverted_total",
        "decdn_onchain_tx_send_failed_total",
        "decdn_onchain_tx_receipt_failed_total",
        "decdn_onchain_tx_timeout_total",
        "decdn_onchain_tx_receipt_recovered_total",
        "decdn_chain_boot_read_retries_total",
        "decdn_chain_get_logs_retries_total",
        "decdn_chain_get_logs_deferred_total",
        "decdn_dht_findvalue_queries_total",
        "decdn_dht_lookup_round_timeouts_total",
        "decdn_dht_store_published_total",
        "decdn_dht_routing_table_size",
        "decdn_dht_bucket_refresh_failures_total",
        "decdn_dht_bootstrap_find_node_failures_total",
        "decdn_fee_shares_watcher_poll_failures_total",
        "decdn_fee_shares_watcher_restarts_total",
        "decdn_fee_shares_watcher_down_seconds",
        "decdn_fee_shares_watcher_unregistered",
        "decdn_config_reload_failures_total",
        "decdn_receipt_write_failures_total",
        "decdn_serve_stream_midstream_pool_exhausted_total",
        "decdn_serve_stream_proof_budget_exhausted_total",
        "decdn_redeem_hints_parked_total",
    ] {
        assert!(
            has_metric_line(&text, name, 0),
            "{name} should export at zero on a fresh registry:\n{text}"
        );
    }
}

/// Every inbound failure reason exports at zero, so the residual the
/// dashboards plot is never computed against an absent series.
#[test]
fn inbound_failure_reasons_export_at_zero() {
    let text = Metrics::new().encode().unwrap();
    for name in INBOUND_FAILURE_REASONS {
        assert!(
            has_metric_line(&text, name, 0),
            "{name} should export at zero on a fresh registry:\n{text}"
        );
    }
}

/// Every exported `decdn_serve_stream_*_total` counter is an inbound failure
/// reason. A new serve-stream counter lands in [`INBOUND_FAILURE_REASONS`], or
/// this test names it.
#[test]
fn every_serve_stream_counter_is_an_inbound_failure_reason() {
    let exported = exported_series(&Metrics::new().encode().unwrap());
    let missing: Vec<&String> = exported
        .iter()
        .filter(|n| n.starts_with("decdn_serve_stream_") && n.ends_with("_total"))
        .filter(|n| !INBOUND_FAILURE_REASONS.contains(&n.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "serve-stream counters missing from INBOUND_FAILURE_REASONS: {missing:?}"
    );
}

/// Both directions of both stream-outcome families export at zero, and
/// each end lands in exactly one family.
#[test]
fn stream_outcomes_split_by_direction() {
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();
    for family in [
        "decdn_streams_completed_total",
        "decdn_streams_failed_total",
    ] {
        for direction in ["inbound", "outbound"] {
            let line = format!("{family}{{direction=\"{direction}\"}} 0");
            assert!(text.lines().any(|l| l == line), "missing {line}:\n{text}");
        }
    }
    metrics.inbound_stream_ended(true);
    metrics.inbound_stream_ended(false);
    metrics.inbound_stream_ended(false);
    metrics.outbound_stream_ended(true);
    let text = metrics.encode().unwrap();
    for line in [
        "decdn_streams_completed_total{direction=\"inbound\"} 1",
        "decdn_streams_failed_total{direction=\"inbound\"} 2",
        "decdn_streams_completed_total{direction=\"outbound\"} 1",
        "decdn_streams_failed_total{direction=\"outbound\"} 0",
    ] {
        assert!(text.lines().any(|l| l == line), "missing {line}:\n{text}");
    }
}

#[test]
fn receipt_writes_dropped_metric_starts_at_zero_and_increments() {
    // #803. The struct field is `receipt_writes_dropped`; the OpenMetrics
    // encoder appends `_total`, so the exported name is
    // `decdn_receipt_writes_dropped_total` — the operator-visible name an
    // alert on lost audit records references. Must be exposed at zero on a
    // fresh registry and bump once per dropped receipt.
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_receipt_writes_dropped_total", 0),
        "receipt-write-dropped counter should be exposed at zero on a fresh registry:\n{text}"
    );

    metrics.receipt_write_dropped();

    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_receipt_writes_dropped_total", 1),
        "expected one dropped-receipt event:\n{text}"
    );
}

#[test]
fn warming_credits_dropped_metric_starts_at_zero_and_increments() {
    // ADR 041. Exposed at zero on a fresh registry so an alert can be
    // written against it before the first drop ever happens, and bumped once
    // per credit that never reached the ledger. A rate that tracks the serve
    // rate means the aggregator is gone and warming will stop node-wide.
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_warming_credits_dropped_total", 0),
        "warming-credit-dropped counter should be exposed at zero on a fresh registry:\n{text}"
    );

    metrics.warming_credit_dropped();

    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_warming_credits_dropped_total", 1),
        "expected one dropped-warming-credit event:\n{text}"
    );
}

#[test]
fn warming_credits_applied_metric_starts_at_zero_and_increments() {
    // ADR 041. The success sibling of the dropped counter: an operator
    // reading zero drops needs this to tell a healthy node from one whose
    // serve path never reached the ledger. A handler left with
    // `NoopWarmingCreditSink` enqueues nothing, so it drops nothing — both
    // counters sit at zero and only this one makes that legible.
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_warming_credits_applied_total", 0),
        "warming-credit-applied counter should be exposed at zero on a fresh registry:\n{text}"
    );

    metrics.warming_credit_applied();

    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_warming_credits_applied_total", 1),
        "expected one applied warming credit:\n{text}"
    );
}

#[test]
fn staker_set_watcher_metrics_start_at_zero_and_increment() {
    // #783. The struct field is `staker_set_watcher_restarts`; the
    // OpenMetrics encoder appends `_total`, so the exported name is
    // `decdn_staker_set_watcher_restarts_total` — the operator-visible
    // name any alert references. Asserting the suffixed form locks it in:
    // re-naming the field to include `_total` would emit `..._total_total`
    // (the same footgun the cache GC counters guard against).
    let metrics = Metrics::new();
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_staker_set_watcher_restarts_total", 0),
        "watcher restart counter should be exposed at zero on a fresh registry:\n{text}"
    );
    // Both gauges are exposed at zero so dashboards don't render `(no
    // data)` before the watcher's first event. `down_seconds` reads 0
    // before any cycle is established (a quiet "not yet up").
    assert!(
        has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 0),
        "watcher down-seconds gauge should start at zero:\n{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_staker_set_active_count", 0),
        "active-count gauge should start at zero:\n{text}"
    );

    // The resolve-failure counter (#788) is also exposed at zero.
    assert!(
        has_metric_line(&text, "decdn_staker_set_watcher_resolve_failures_total", 0),
        "resolve-failure counter should start at zero:\n{text}"
    );

    // Two distinct drift windows (each `backoff_started` is an edge into
    // the error state; `cycle_established` between them closes the first).
    metrics.staker_set_watcher_backoff_started();
    metrics.staker_set_watcher_cycle_established();
    metrics.staker_set_watcher_backoff_started();
    metrics.staker_set_watcher_resolve_failure();
    metrics.staker_set_active_count(7);

    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_staker_set_watcher_restarts_total", 2),
        "expected 2 restart events:\n{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_staker_set_watcher_resolve_failures_total", 1),
        "expected 1 resolve failure:\n{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_staker_set_active_count", 7),
        "expected active-count gauge to report 7:\n{text}"
    );
}

#[test]
fn staker_set_watcher_down_seconds_reads_zero_across_a_long_healthy_cycle() {
    // CRITICAL-fix regression guard (#788): `down_seconds` measures true
    // downtime, NOT cycle age. A healthy poll loop persists
    // indefinitely, so the gauge must read 0 for the entire life of an
    // established cycle — even when the node has been up (and the cycle
    // live) for a while. We simulate "a while" by backdating the metrics'
    // `started_at` well into the past: under the OLD age-based model the
    // gauge climbed with cycle age and would read ~3600 here; under the
    // downtime model `down_since` is `None`, so it stays 0.
    let mut metrics = Metrics::new();
    metrics.started_at = Instant::now()
        .checked_sub(Duration::from_hours(1))
        .unwrap_or_else(Instant::now);
    metrics.staker_set_watcher_cycle_established();

    let text = metrics.encode().unwrap();
    // Sanity: the node really is "old" (uptime reflects the backdate), so a
    // gauge that tracked age would be non-zero.
    assert!(
        has_metric_line(&text, "decdn_node_uptime_seconds", 3600),
        "uptime should reflect the backdated start:\n{text}"
    );
    assert!(
        has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 0),
        "down-seconds must read 0 across a long healthy cycle (true downtime, not age):\n{text}"
    );

    // An error opens the drift window: down-seconds is now driven by
    // `down_since` (not cycle age). An immediate scrape reads ~0s of
    // *downtime*; backdating `down_since` proves it then climbs.
    metrics.staker_set_watcher_backoff_started();
    if let Ok(mut down_since) = metrics.staker_set_watcher_down_since.lock() {
        *down_since = Instant::now().checked_sub(Duration::from_secs(150));
    }
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 150),
        "down-seconds should climb to the downtime depth once in backoff:\n{text}"
    );

    // Re-establishing the cycle clears the window back to 0.
    metrics.staker_set_watcher_cycle_established();
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 0),
        "down-seconds should reset to 0 once filters re-establish:\n{text}"
    );
}

#[test]
fn slash_watcher_down_seconds_tracks_true_downtime() {
    // Mirrors the staker-set guard for the slash watcher (#1032): the gauge
    // measures downtime, not cycle age, so a long healthy cycle reads 0, a
    // backoff window climbs, and re-establishing clears it.
    let mut metrics = Metrics::new();
    metrics.started_at = Instant::now()
        .checked_sub(Duration::from_hours(1))
        .unwrap_or_else(Instant::now);
    metrics.slash_watcher_cycle_established();
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_slash_watcher_down_seconds", 0),
        "down-seconds must read 0 across a long healthy cycle:\n{text}"
    );

    metrics.slash_watcher_backoff_started();
    if let Ok(mut down_since) = metrics.slash_watcher_down_since.lock() {
        *down_since = Instant::now().checked_sub(Duration::from_secs(150));
    }
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_slash_watcher_down_seconds", 150),
        "down-seconds should climb to the downtime depth once in backoff:\n{text}"
    );
    // The restart counter bumps exactly once per drift window (edge-triggered).
    metrics.slash_watcher_backoff_started();
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_slash_watcher_restarts_total", 1),
        "restarts must bump once per drift window, not per call:\n{text}"
    );

    metrics.slash_watcher_cycle_established();
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_slash_watcher_down_seconds", 0),
        "down-seconds should reset to 0 once the cycle re-establishes:\n{text}"
    );
}

#[test]
#[allow(clippy::panic)] // deliberately poison the lock, mirroring `dispatch::tests`.
fn poisoned_down_since_reports_i64_max_not_zero() {
    // The load-bearing invariant of the down-seconds gauges: a poisoned
    // `down_since` must report `i64::MAX`, never `0`, because reporting `0`
    // would MASK an in-progress outage (#783/#1032). The watchers'
    // scrape recompute goes through one shared `refresh_watcher_down_seconds`
    // template, so poisoning any single lock exercises that conservative-alerting
    // fallback for all of them.
    let metrics = Arc::new(Metrics::new());
    let for_thread = Arc::clone(&metrics);
    // Poison `slash_watcher_down_since` from a panicking thread holding the
    // lock (the house pattern — see `dispatch::tests`).
    let join = std::thread::spawn(move || {
        let _g = for_thread.slash_watcher_down_since.lock().unwrap();
        panic!("intentional");
    });
    let _ = join.join();
    assert!(metrics.slash_watcher_down_since.is_poisoned());

    let i64_max = u64::try_from(i64::MAX).unwrap();
    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_slash_watcher_down_seconds", i64_max),
        "a poisoned down_since must report i64::MAX, not 0:\n{text}"
    );
    // The two healthy watchers still read 0 in the same scrape — the poison
    // is isolated to its own row of the shared recompute.
    assert!(
        has_metric_line(&text, "decdn_staker_set_watcher_down_seconds", 0),
        "a poisoned slash lock must not perturb the staker-set gauge:\n{text}"
    );
    // The watchers brought to down-family parity (#1283/#1316) share
    // the same recompute row, so they too read a clean 0 under the poison.
    for name in [
        "decdn_blacklist_watcher_down_seconds",
        "decdn_settlement_watcher_down_seconds",
    ] {
        assert!(
            has_metric_line(&text, name, 0),
            "a poisoned slash lock must not perturb {name}:\n{text}"
        );
    }
}

#[test]
fn watcher_liveness_gauges_stamp_wall_clock_on_tick() {
    // The `*_watcher_tick` recorders (the `on_tick_success` hooks) stamp a
    // wall-clock timestamp, so a live watcher's gauge is non-zero and a dead
    // one's stays 0 — the positive liveness signal the error-triggered
    // down-seconds gauge cannot provide (#1316/#1320).
    let metrics = Metrics::new();
    let before = unix_now_secs();

    metrics.slash_watcher_tick();
    metrics.staker_set_watcher_tick();
    metrics.blacklist_watcher_tick();
    metrics.settlement_watcher_tick();

    let text = metrics.encode().unwrap();
    for name in [
        "decdn_slash_watcher_last_tick_timestamp_seconds",
        "decdn_staker_set_watcher_last_tick_timestamp_seconds",
        "decdn_blacklist_watcher_last_tick_timestamp_seconds",
        "decdn_settlement_watcher_last_tick_timestamp_seconds",
    ] {
        let floor = u64::try_from(before).unwrap();
        assert!(
            metric_value(&text, name).is_some_and(|stamped| stamped >= floor),
            "liveness gauge {name} must be exported and stamped with the current time:\n{text}"
        );
    }
}

#[test]
#[allow(clippy::type_complexity)] // a compact table of (recorder, recorder, gauge, counter).
fn new_watchers_down_seconds_track_true_downtime() {
    // The watchers brought to down-family parity (#1283/#1316) share
    // the `watcher_downtime_recorders!` template, so one compact pass per
    // watcher confirms the wiring: healthy reads 0, backoff climbs, one
    // restart per drift window, re-establish clears.
    let cases: [(fn(&Metrics), fn(&Metrics), &str, &str); 3] = [
        (
            Metrics::blacklist_watcher_cycle_established,
            Metrics::blacklist_watcher_backoff_started,
            "decdn_blacklist_watcher_down_seconds",
            "decdn_blacklist_watcher_restarts_total",
        ),
        (
            Metrics::settlement_watcher_cycle_established,
            Metrics::settlement_watcher_backoff_started,
            "decdn_settlement_watcher_down_seconds",
            "decdn_settlement_watcher_restarts_total",
        ),
        (
            Metrics::fee_shares_watcher_cycle_established,
            Metrics::fee_shares_watcher_backoff_started,
            "decdn_fee_shares_watcher_down_seconds",
            "decdn_fee_shares_watcher_restarts_total",
        ),
    ];
    for (established, backoff, down_seconds, restarts) in cases {
        let metrics = Metrics::new();
        established(&metrics);
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, down_seconds, 0),
            "{down_seconds} must read 0 on a healthy cycle:\n{text}"
        );
        backoff(&metrics);
        backoff(&metrics); // second call: same drift window, no extra restart.
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, restarts, 1),
            "{restarts} must bump once per drift window:\n{text}"
        );
        established(&metrics);
        let text = metrics.encode().unwrap();
        assert!(
            has_metric_line(&text, down_seconds, 0),
            "{down_seconds} must reset to 0 once re-established:\n{text}"
        );
    }
}

/// Minimal in-memory origin: returns the prearranged payload for its
/// hash, `NotFound` otherwise. Mirrors the `StubOrigin` used in
/// crates/cache tests but is local to these tests so the cache crate's
/// test fixtures stay private.
#[derive(Debug)]
struct StubOrigin {
    data: bytes::Bytes,
    hash: iroh_blobs::Hash,
}

impl decdn_cache::Origin for StubOrigin {
    fn kind(&self) -> decdn_cache::OriginKind {
        decdn_cache::OriginKind::Http
    }
    fn fetch(
        &self,
        hash: iroh_blobs::Hash,
        _max_bytes: u64,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<decdn_cache::OriginFetch, decdn_cache::OriginPullError>,
                > + Send
                + '_,
        >,
    > {
        let result = if hash == self.hash {
            Ok(decdn_cache::OriginFetch::found_one_shot(self.data.clone()))
        } else {
            Ok(decdn_cache::OriginFetch::NotFound)
        };
        Box::pin(async move { result })
    }
}

/// `decdn_probe_hold_slots_used` tracks hold expiry with no probe
/// traffic: a hold taken once reads `1` at the next scrape and `0` at a
/// scrape after `PROBE_HOLD_DURATION` lapses, with nothing but the clock
/// in between.
#[tokio::test(start_paused = true)]
async fn probe_hold_slots_used_drops_to_zero_after_hold_expires() {
    use std::sync::Arc;

    use decdn_cache::{CacheEngine, PROBE_HOLD_DURATION, ProbeHoldOutcome};

    let payload = b"probe hold gauge".to_vec();
    let hash = iroh_blobs::Hash::new(&payload);
    let stub = StubOrigin {
        data: bytes::Bytes::from(payload),
        hash,
    };
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open(
        tmp.path(),
        vec![Arc::new(stub) as Arc<dyn decdn_cache::Origin>],
        10,
    )
    .await
    .unwrap();
    let _ = engine.get(hash).await.unwrap();

    let metrics = Metrics::new();
    metrics.attach_probe_holds(engine.clone());
    assert_eq!(
        engine.try_probe_hold(hash).await.unwrap(),
        ProbeHoldOutcome::Held
    );

    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_probe_hold_slots_used", 1),
        "a live hold must read 1:\n{text}"
    );

    tokio::time::advance(PROBE_HOLD_DURATION + Duration::from_secs(1)).await;

    let text = metrics.encode().unwrap();
    assert!(
        has_metric_line(&text, "decdn_probe_hold_slots_used", 0),
        "an expired hold must read 0 with no further probe:\n{text}"
    );
}

#[tokio::test]
async fn engine_bumps_surface_in_openmetrics_output() {
    use std::sync::Arc;

    use bytes::Bytes;
    use decdn_cache::{CacheEngine, PinnedHashes, RetryPolicy};
    use iroh_blobs::Hash;

    let payload = b"hello /metrics integration".to_vec();
    let hash = Hash::new(&payload);
    let stub = StubOrigin {
        data: Bytes::from(payload.clone()),
        hash,
    };

    let metrics = Arc::new(Metrics::new());
    let cache_handle = metrics.cache_metrics();
    let tmp = tempfile::tempdir().unwrap();
    let engine = CacheEngine::open_full(
        tmp.path(),
        vec![Arc::new(stub) as Arc<dyn decdn_cache::Origin>],
        10,
        PinnedHashes::empty(),
        RetryPolicy::default(),
        decdn_cache::CircuitBreakerPolicy::default(),
        Some(Arc::clone(&cache_handle)),
        std::time::Duration::ZERO,
    )
    .await
    .unwrap();

    // 1 miss (pull-through) + 1 hit.
    let _ = engine.get(hash).await.unwrap();
    let _ = engine.get(hash).await.unwrap();

    let text = metrics.encode().unwrap();
    let payload_len = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    for (name, expected) in [
        ("decdn_cache_hits_total", 1u64),
        ("decdn_cache_misses_total", 1),
        ("decdn_cache_pull_through_bytes_total", payload_len),
        ("decdn_cache_bytes_returned_total", payload_len * 2),
    ] {
        assert!(
            has_metric_line(&text, name, expected),
            "counter {name} should report {expected} after 1 miss + 1 hit:\n{text}"
        );
    }
}

/// The serve-path hit family exports at zero and each recorder bumps its
/// own sibling.
///
/// The zero floor is the load-bearing half. These three exist because the
/// `get`-scoped `decdn_cache_hits_total` sits at a permanent zero on a node
/// that only serves paying clients, and a dashboard cannot tell that apart
/// from "no requests yet". A family that only appeared after its first bump
/// would reintroduce exactly that ambiguity for its own first hour.
#[test]
fn serve_cache_hit_family_exports_and_records() {
    let metrics = Metrics::new();
    let names = [
        "decdn_serve_cache_hit_total",
        "decdn_serve_cache_partial_hit_total",
        "decdn_serve_cache_miss_total",
    ];
    let text = metrics.encode().unwrap();
    for name in names {
        assert!(
            has_metric_line(&text, name, 0),
            "{name} must export at zero before the first serve:\n{text}"
        );
    }

    // One bump each, so a recorder wired to the wrong sibling shows up as a
    // count on a name it should not have touched.
    metrics.serve_cache_hit();
    metrics.serve_cache_partial_hit();
    metrics.serve_cache_miss();
    let text = metrics.encode().unwrap();
    for name in names {
        assert!(
            has_metric_line(&text, name, 1),
            "{name} should report 1 after its recorder fired once:\n{text}"
        );
    }
}

/// `bind` accepts both loopback and non-loopback addresses (the
/// non-loopback path emits a `WARN` per #579 but does not reject).
/// We can't easily intercept the tracing emission without a
/// dedicated capture subscriber, so this is a smoke test of both
/// branches plus IPv6 loopback — a future refactor that narrowed
/// the predicate to e.g. `addr.ip() == Ipv4Addr::LOCALHOST` would
/// regress on `::1` and break here visibly.
#[tokio::test]
async fn bind_accepts_loopback_and_warns_on_non_loopback() {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    // IPv4 loopback: warn-free.
    let v4_loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let listener = bind(v4_loopback).unwrap();
    let bound = listener.local_addr().unwrap();
    assert!(
        bound.ip().is_loopback(),
        "IPv4 loopback bind should resolve to a loopback addr: got {bound}"
    );
    drop(listener);

    // IPv6 loopback `::1`: also warn-free. Some hosts disable
    // IPv6; skip rather than fail if the bind itself errors.
    let v6_loopback = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0);
    if let Ok(listener) = bind(v6_loopback) {
        let bound = listener.local_addr().unwrap();
        assert!(
            bound.ip().is_loopback(),
            "IPv6 loopback bind should resolve to a loopback addr: got {bound}"
        );
    }

    // Unspecified (`0.0.0.0`): allowed, but the bind path WARNs.
    // Bind succeeds (a regression that rejected unspecified
    // would surface as a `bind` error here). `local_addr()`
    // echoes the requested IP so `is_unspecified()` is the
    // direct post-bind assertion.
    let unspecified = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    let listener = bind(unspecified).unwrap();
    let bound = listener.local_addr().unwrap();
    assert!(
        bound.ip().is_unspecified(),
        "0.0.0.0 bind should resolve to the unspecified addr: got {bound}"
    );
    drop(listener);
}

#[test]
fn cache_metrics_handle_shares_atomic_with_registered_group() {
    // Sanity: the Arc<CacheMetrics> handed to the engine must be
    // the same one the registry reads from at scrape time. A bug
    // that built two Arcs would surface as cache bumps never
    // appearing in the scrape output.
    let metrics = Metrics::new();
    let handle = metrics.cache_metrics();
    handle.origin_fetches.inc();
    handle.origin_retry_exhausted.inc();
    handle.origin_fallback.inc();
    handle.hits.inc();
    handle.misses.inc();
    handle.bytes_returned.inc_by(1024);
    handle.pull_through_bytes.inc_by(2048);
    // GC counters (#518). The struct fields are `gc_runs` /
    // `gc_bytes_reclaimed`; bumping them here and asserting the
    // `..._total`-suffixed exported names round-trip locks in the
    // encoder behavior that motivated the field-name shape.
    handle.gc_runs.inc();
    handle.gc_bytes_reclaimed.inc_by(4096);
    let text = metrics.encode().unwrap();
    for (name, expected) in [
        ("decdn_cache_origin_fetches_total", 1u64),
        ("decdn_cache_origin_retry_exhausted_total", 1),
        ("decdn_cache_hits_total", 1),
        ("decdn_cache_misses_total", 1),
        ("decdn_cache_bytes_returned_total", 1024),
        ("decdn_cache_pull_through_bytes_total", 2048),
        ("decdn_cache_gc_runs_total", 1),
        ("decdn_cache_gc_bytes_reclaimed_total", 4096),
    ] {
        assert!(
            has_metric_line(&text, name, expected),
            "counter {name} should report {expected}:\n{text}"
        );
    }
}

#[test]
fn pool_open_failures_by_reason_label_distinct_counters() {
    // The three buyer `openPool` failure classes (#966) must each land
    // in their own `decdn_pool_open_failures_{reason}_total` sibling
    // counter — that label split is the whole point of the issue, so a
    // bump on one reason must NOT leak into another.
    let metrics = Metrics::new();

    // Fresh registry: every reason exposed at zero so dashboards don't
    // render `(no data)` before the first failure.
    let text = metrics.encode().unwrap();
    for name in [
        "decdn_pool_open_failures_insufficient_deposit_total",
        "decdn_pool_open_failures_contract_revert_total",
        "decdn_pool_open_failures_rpc_error_total",
    ] {
        assert!(
            has_metric_line(&text, name, 0),
            "reason counter {name} should start at zero:\n{text}"
        );
    }

    // Bump each reason a distinct number of times so a cross-wired counter
    // is caught by the mismatched count, not just a nonzero value.
    metrics.pool_open_failure_by_reason(PoolOpenFailureReason::InsufficientDeposit);
    metrics.pool_open_failure_by_reason(PoolOpenFailureReason::InsufficientDeposit);
    metrics.pool_open_failure_by_reason(PoolOpenFailureReason::ContractRevert);
    metrics.pool_open_failure_by_reason(PoolOpenFailureReason::RpcError);
    metrics.pool_open_failure_by_reason(PoolOpenFailureReason::RpcError);
    metrics.pool_open_failure_by_reason(PoolOpenFailureReason::RpcError);

    let text = metrics.encode().unwrap();
    for (name, expected) in [
        ("decdn_pool_open_failures_insufficient_deposit_total", 2u64),
        ("decdn_pool_open_failures_contract_revert_total", 1),
        ("decdn_pool_open_failures_rpc_error_total", 3),
    ] {
        assert!(
            has_metric_line(&text, name, expected),
            "reason counter {name} should report {expected}:\n{text}"
        );
    }

    // The by-reason family is independent of the unlabeled total — bumping
    // a reason does NOT touch `node_pull_pool_open_failures` (that total
    // is bumped separately, and also covers non-tx causes).
    assert!(
        has_metric_line(&text, "decdn_node_pull_pool_open_failures_total", 0),
        "unlabeled total must not move when only the by-reason helper is called:\n{text}"
    );
}

/// iroh's transport metrics export under the `decdn_iroh_` prefix once an
/// endpoint registers them, and not before. The runbook points operators at
/// `decdn_iroh_socket_*`, so a changed prefix or a renamed iroh group fails
/// here rather than on a blank panel.
#[test]
fn iroh_metrics_export_under_the_decdn_iroh_prefix() {
    let metrics = Metrics::new();
    let before = exported_series(&metrics.encode().unwrap());
    assert!(
        !before.iter().any(|n| n.starts_with("decdn_iroh_")),
        "decdn_iroh_* exported before any endpoint registered"
    );

    metrics
        .register_iroh_metrics(&EndpointMetrics::default())
        .unwrap();
    let after = exported_series(&metrics.encode().unwrap());
    let socket: Vec<&String> = after
        .iter()
        .filter(|n| n.starts_with("decdn_iroh_socket_"))
        .collect();
    assert!(
        !socket.is_empty(),
        "no decdn_iroh_socket_* series after registering EndpointMetrics; \
         decdn_iroh_* exported: {:?}",
        after
            .iter()
            .filter(|n| n.starts_with("decdn_iroh_"))
            .collect::<Vec<_>>()
    );
    assert!(
        !after
            .iter()
            .any(|n| n.starts_with("decdn_iroh_decdn_iroh_")),
        "iroh metrics carry a doubled prefix"
    );
}

/// Each failed inbound stream counts on exactly one entry of
/// [`INBOUND_FAILURE_REASONS`], so a duplicate entry would double-count it.
#[test]
fn inbound_failure_reasons_hold_no_duplicate() {
    let unique: std::collections::BTreeSet<&str> =
        INBOUND_FAILURE_REASONS.iter().copied().collect();
    assert_eq!(unique.len(), INBOUND_FAILURE_REASONS.len());
}
