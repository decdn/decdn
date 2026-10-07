use super::*;
use std::time::Duration;

fn snap(
    uptime: u64,
    hits: u64,
    misses: u64,
    bytes: u64,
    in_flight: u64,
    conns: u64,
    healthy: bool,
) -> Snapshot {
    Snapshot {
        uptime_seconds: uptime,
        active_connections: conns,
        dispatch_in_flight: in_flight,
        cache_hits: hits,
        cache_misses: misses,
        cache_bytes_returned: bytes,
        unredeemed_usdc: 0,
        rpc_healthy: healthy,
    }
}

#[test]
fn format_bytes_picks_unit() {
    assert_eq!(format_bytes(0), "0 B");
    assert_eq!(format_bytes(512), "512 B");
    assert_eq!(format_bytes(2048), "2.0 KiB");
    assert_eq!(format_bytes(15 * 1024 * 1024), "15.0 MiB");
    assert_eq!(format_bytes(3 * 1024_u64.pow(3)), "3.0 GiB");
}

#[test]
fn format_usdc_renders_six_decimals() {
    assert_eq!(format_usdc(0), "$0.000000");
    assert_eq!(format_usdc(1), "$0.000001");
    assert_eq!(format_usdc(12_345_678), "$12.345678");
    // Whole dollars keep the trailing zeros so 1e6 × display == raw.
    assert_eq!(format_usdc(5_000_000), "$5.000000");
}

#[test]
fn format_rate_uses_per_second_suffix() {
    assert_eq!(format_rate_bytes(0.0), "0 B/s");
    assert_eq!(format_rate_bytes(1024.0), "1.0 KiB/s");
    assert_eq!(format_rate_bytes(2_500_000.0), "2.4 MiB/s");
}

#[test]
fn format_uptime_crosses_unit_boundaries() {
    // The three cross-unit boundaries: 60s (s→m), 3600s (m→h),
    // 86400s (h→d), plus in-band cases. `write_top_table`
    // tests exercise the m+s shape via substring; cross-units
    // need direct asserts so a regression at the boundary
    // arithmetic doesn't sneak past unnoticed.
    assert_eq!(format_uptime(0), "0s");
    assert_eq!(format_uptime(59), "59s");
    assert_eq!(format_uptime(60), "1m 0s");
    assert_eq!(format_uptime(3_599), "59m 59s");
    assert_eq!(format_uptime(3_600), "1h 0m");
    assert_eq!(format_uptime(86_399), "23h 59m");
    assert_eq!(format_uptime(86_400), "1d 0h");
    assert_eq!(format_uptime(2 * 86_400 + 3 * 3_600), "2d 3h");
}

#[test]
fn write_top_table_first_tick_no_deltas() {
    // First tick has no previous snapshot, so the /sec column
    // shows a stable placeholder ("-") rather than a nonsense 0.
    let s = snap(312, 812, 94, 14_900_000, 2, 4, true);
    let mut buf = Vec::<u8>::new();
    write_top_table(
        &mut buf,
        "http://127.0.0.1:9090",
        &s,
        None,
        Duration::from_secs(1),
    )
    .unwrap();
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("decdn node top"), "header missing: {out}");
    assert!(
        out.contains("uptime=5m 12s") || out.contains("uptime=312s"),
        "uptime missing: {out}"
    );
    assert!(out.contains("rpc=ok"), "rpc status missing: {out}");
    assert!(
        out.contains("dispatch_in_flight"),
        "in_flight row missing: {out}"
    );
    assert!(
        out.contains("cache_hit_rate"),
        "hit_rate row missing: {out}"
    );
    for needle in [
        "cache_hits_total",
        "cache_misses_total",
        "cache_bytes_returned_total",
    ] {
        assert!(out.contains(needle), "row {needle} missing: {out}");
    }
}

#[test]
fn write_top_table_renders_per_second_when_prev_present() {
    let prev = snap(310, 800, 90, 14_000_000, 2, 4, true);
    let now = snap(312, 812, 94, 14_420_000, 2, 4, true);
    let mut buf = Vec::<u8>::new();
    write_top_table(
        &mut buf,
        "http://127.0.0.1:9090",
        &now,
        Some((&prev, Duration::from_secs(2))),
        Duration::from_secs(1),
    )
    .unwrap();
    let out = String::from_utf8(buf).unwrap();
    // /sec column for cache_hits_total should be "6.0" since
    // (812-800)/2 = 6.0. Asserted as substring rather than line
    // shape so column-width tweaks don't break the test.
    assert!(out.contains("6.0"), "hits/s missing: {out}");
    // bytes/s = (14_420_000 - 14_000_000) / 2 = 210_000 B/s, which
    // format_rate_bytes renders as "205.1 KiB/s" (210000/1024).
    assert!(
        out.contains("205.1 KiB/s"),
        "bytes/s missing (expected 205.1 KiB/s for 210000 B/s): {out}"
    );
}

#[test]
fn write_top_table_renders_unredeemed_usdc_row() {
    let s = Snapshot {
        uptime_seconds: 312,
        active_connections: 4,
        dispatch_in_flight: 2,
        cache_hits: 812,
        cache_misses: 94,
        cache_bytes_returned: 14_900_000,
        unredeemed_usdc: 12_345_678,
        rpc_healthy: true,
    };
    let mut buf = Vec::<u8>::new();
    write_top_table(
        &mut buf,
        "http://127.0.0.1:9090",
        &s,
        None,
        Duration::from_secs(1),
    )
    .unwrap();
    let out = String::from_utf8(buf).unwrap();
    assert!(
        out.contains("unredeemed_usdc"),
        "unredeemed row missing: {out}"
    );
    assert!(
        out.contains("$12.345678"),
        "unredeemed dollar value missing: {out}"
    );
}

#[test]
fn write_top_table_hit_rate_na_on_empty_cache() {
    let s = snap(0, 0, 0, 0, 0, 0, true);
    let mut buf = Vec::<u8>::new();
    write_top_table(
        &mut buf,
        "http://127.0.0.1:9090",
        &s,
        None,
        Duration::from_secs(1),
    )
    .unwrap();
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("n/a"), "hit_rate must show n/a on 0/0: {out}");
}

#[test]
fn write_top_table_marks_rpc_unhealthy() {
    let s = snap(312, 812, 94, 14_900_000, 2, 4, false);
    let mut buf = Vec::<u8>::new();
    write_top_table(
        &mut buf,
        "http://127.0.0.1:9090",
        &s,
        None,
        Duration::from_secs(1),
    )
    .unwrap();
    let out = String::from_utf8(buf).unwrap();
    assert!(
        out.contains("rpc=unhealthy"),
        "expected rpc=unhealthy: {out}"
    );
}
