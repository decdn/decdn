use super::*;
use decdn_common::admin::SlashRecordDto;

fn health_response(binding: BindingStatus, bound: Option<&str>) -> HealthResponse {
    health_response_with_registry(binding, bound, true)
}

fn health_response_with_registry(
    binding: BindingStatus,
    bound: Option<&str>,
    registry_active: bool,
) -> HealthResponse {
    HealthResponse {
        node_id: "aa".repeat(32),
        uptime_s: 42,
        in_flight_streams: 0,
        binding,
        bound_node_id: bound.map(str::to_string),
        registry_active,
    }
}

fn health_lines(resp: &HealthResponse) -> String {
    let mut buf = Vec::new();
    write_health(&mut buf, resp).expect("write to a Vec cannot fail");
    String::from_utf8(buf).expect("output is ASCII")
}

/// The two-line contract holds — operator scripts grep
/// these — and the binding line is additive.
#[test]
fn health_keeps_node_id_and_uptime_lines() {
    let s = health_lines(&health_response(
        BindingStatus::Bound,
        Some(&"aa".repeat(32)),
    ));
    assert!(s.contains(&format!("node_id={}", "aa".repeat(32))), "{s}");
    assert!(s.contains("uptime_s=42"), "{s}");
}

/// A mismatch is the unslashable state, and the bound id is what the
/// operator needs to act on it — printing the status without the id would
/// report a problem and withhold the fix.
#[test]
fn health_names_the_bound_id_on_a_mismatch() {
    let bound = "bb".repeat(32);
    let s = health_lines(&health_response(BindingStatus::Mismatch, Some(&bound)));
    assert!(s.contains("binding=mismatch"), "{s}");
    assert!(s.contains(&format!("bound_node_id={bound}")), "{s}");
}

/// `registry_active` must print in BOTH states, for the same reason
/// `binding` does: a script grepping for the failure has to be able to tell
/// "checked, and this node cannot sell" from "the line is missing" (#1030).
/// The two fields are independent — a node can be correctly `bound` and
/// still be out of the active set (deregistered, ejected, unbonding, or
/// bond below `minBond`), which is exactly the case an operator misreads
/// without this line.
#[test]
fn health_prints_registry_active_in_both_states() {
    let serving = health_lines(&health_response_with_registry(
        BindingStatus::Bound,
        Some(&"aa".repeat(32)),
        true,
    ));
    assert!(serving.contains("registry_active=true"), "{serving}");

    let idle = health_lines(&health_response_with_registry(
        BindingStatus::Bound,
        Some(&"aa".repeat(32)),
        false,
    ));
    assert!(
        idle.contains("registry_active=false"),
        "a bound node that is out of the active set must still say so: {idle}"
    );
}

/// `unknown` must print. Omitting the line for the not-checked case would
/// let a script that greps for `binding=mismatch` read "we never checked"
/// as "all clear" — the exact conflation the status enum exists to stop.
#[test]
fn health_prints_unknown_rather_than_omitting_the_line() {
    let s = health_lines(&health_response(BindingStatus::Unknown, None));
    assert!(s.contains("binding=unknown"), "{s}");
    assert!(
        !s.contains("bound_node_id="),
        "there is no bound id to name: {s}"
    );
}

/// The plain and `--json` renderings must spell the status the same way,
/// or an operator switching between them sees two vocabularies.
#[test]
fn binding_label_matches_the_serde_spelling() {
    for status in [
        BindingStatus::Bound,
        BindingStatus::Mismatch,
        BindingStatus::Unbound,
        BindingStatus::Unknown,
    ] {
        let json = serde_json::to_string(&status).expect("status serializes");
        assert_eq!(
            json.trim_matches('"'),
            binding_label(status),
            "plain and JSON renderings disagree for {status:?}"
        );
    }
}

/// `emit_drain_complete` JSON shape, polled-to-zero branch:
/// `admin_closed=false`. Asserts the exact JSON object so
/// machine consumers can rely on a fixed schema across both
/// terminal-success paths (#662 review).
#[test]
fn emit_drain_complete_json_admin_closed_false() {
    let mut buf = Vec::new();
    emit_drain_complete(&mut buf, true, false).expect("write succeeds");
    let s = String::from_utf8(buf).expect("utf8");
    let parsed: serde_json::Value = serde_json::from_str(s.trim()).expect("valid json");
    assert_eq!(
        parsed,
        serde_json::json!({
            "drain_complete": true,
            "in_flight_streams": 0,
            "admin_closed": false,
        })
    );
}

/// JSON shape, ECONNREFUSED branch: `admin_closed=true`.
#[test]
fn emit_drain_complete_json_admin_closed_true() {
    let mut buf = Vec::new();
    emit_drain_complete(&mut buf, true, true).expect("write succeeds");
    let s = String::from_utf8(buf).expect("utf8");
    let parsed: serde_json::Value = serde_json::from_str(s.trim()).expect("valid json");
    assert_eq!(
        parsed,
        serde_json::json!({
            "drain_complete": true,
            "in_flight_streams": 0,
            "admin_closed": true,
        })
    );
}

/// Plain-text shape, both branches. Operator scripts parse the
/// `key=value` form via grep, so the exact byte layout is a
/// public contract — guard it with a literal assertion.
#[test]
fn emit_drain_complete_plain_shapes() {
    let mut buf = Vec::new();
    emit_drain_complete(&mut buf, false, false).expect("write succeeds");
    assert_eq!(
        String::from_utf8(buf).expect("utf8"),
        "drain_complete=true in_flight_streams=0 admin_closed=false\n",
    );

    let mut buf = Vec::new();
    emit_drain_complete(&mut buf, false, true).expect("write succeeds");
    assert_eq!(
        String::from_utf8(buf).expect("utf8"),
        "drain_complete=true in_flight_streams=0 admin_closed=true\n",
    );
}

/// `is_admin_closed_transport_error` walks the `source()` chain
/// and matches the four io kinds that indicate the admin
/// listener / accepted socket went away. A regression that
/// dropped one of the kinds would falsely turn a race-window
/// close into a non-zero exit from the `--wait` polling loop.
#[test]
fn is_admin_closed_transport_error_matches_close_like_kinds() {
    use std::io::{Error, ErrorKind};
    for kind in [
        ErrorKind::ConnectionRefused,
        ErrorKind::ConnectionReset,
        ErrorKind::ConnectionAborted,
        ErrorKind::UnexpectedEof,
    ] {
        let io_err = Error::new(kind, "boom");
        // Wrap to exercise the source-chain walk, not a direct
        // downcast — production errors are typically several
        // layers deep (hyper / jsonrpsee / tower).
        let wrapped: Box<dyn std::error::Error + 'static> = Box::new(io_err);
        assert!(
            is_admin_closed_transport_error(wrapped.as_ref()),
            "kind {kind:?} must be classified as admin-closed",
        );
    }
    // Unrelated kinds must not match.
    for kind in [
        ErrorKind::Other,
        ErrorKind::PermissionDenied,
        ErrorKind::TimedOut,
    ] {
        let io_err = Error::new(kind, "boom");
        let wrapped: Box<dyn std::error::Error + 'static> = Box::new(io_err);
        assert!(
            !is_admin_closed_transport_error(wrapped.as_ref()),
            "kind {kind:?} must not be classified as admin-closed",
        );
    }
}

/// `is_connection_refused` keeps the original narrow semantics
/// — only `ConnectionRefused` matches. Used by the non-wait
/// `classify_client_error` path; the wider helper is only for
/// the `--wait` poll loop.
#[test]
fn is_connection_refused_only_matches_refused() {
    use std::io::{Error, ErrorKind};
    let refused: Box<dyn std::error::Error + 'static> =
        Box::new(Error::new(ErrorKind::ConnectionRefused, "x"));
    assert!(is_connection_refused(refused.as_ref()));
    let reset: Box<dyn std::error::Error + 'static> =
        Box::new(Error::new(ErrorKind::ConnectionReset, "x"));
    assert!(!is_connection_refused(reset.as_ref()));
}

fn mk_status(last_refresh_us: Option<u64>) -> StatusResponse {
    use decdn_common::admin::{BucketStat, RecordStoreHealth, RepublishHealth, RoutingHealth};
    StatusResponse {
        node_id: "ab".repeat(32),
        operator_address: Some("0x52908400098527886E0F7030069857D2E4169EE7".to_string()),
        routing: RoutingHealth {
            total_peers: 21,
            non_empty_buckets: 2,
            buckets: vec![
                BucketStat { index: 0, fill: 1 },
                BucketStat {
                    index: 255,
                    fill: 20,
                },
            ],
            bucket_capacity: 20,
            refresh_interval_s: 3_600,
            last_refresh_us,
        },
        known_stakers: 7,
        record_store: RecordStoreHealth {
            records: 50_000,
            capacity: 100_000,
        },
        republish: RepublishHealth {
            scheduled_records: 5,
        },
        chain_denied_origins: 3,
        pull_through: Some(decdn_common::admin::PullThroughStatus {
            enabled: true,
            buyer_wallet_micro_usdc: Some(12_500_000),
        }),
    }
}

#[test]
fn write_status_renders_summary_and_bucket_table() -> anyhow::Result<()> {
    // last_seen 1s past epoch, now 1s later → "1s ago".
    let status = mk_status(Some(1_000_000));
    let mut buf = Vec::<u8>::new();
    write_status(
        &mut buf,
        &status,
        &EarningsLine::Unavailable(String::new()),
        2_000_000,
    )?;
    let s = String::from_utf8(buf)?;
    assert!(s.contains(&format!("node_id={}", "ab".repeat(32))), "{s}");
    assert!(
        s.contains("operator_address=0x52908400098527886E0F7030069857D2E4169EE7"),
        "{s}"
    );
    assert!(s.contains("known_stakers=7"), "{s}");
    assert!(s.contains("total_peers=21"), "{s}");
    assert!(s.contains("non_empty_buckets=2"), "{s}");
    assert!(s.contains("refresh_interval=1h"), "{s}");
    assert!(s.contains("last_refresh=1s ago"), "{s}");
    // Record-store utilization: 50000/100000 → 50%.
    assert!(s.contains("record_store records=50000/100000 (50%)"), "{s}");
    assert!(s.contains("republish scheduled_records=5"), "{s}");
    assert!(s.contains("chain_denied_origins=3"), "{s}");
    assert!(
        s.contains("pull_through=on buyer_wallet_usdc=12.500000\n"),
        "{s}"
    );
    // Bucket table: header + a full bucket at 100%.
    assert!(s.contains("BUCKET"), "{s}");
    assert!(s.contains("FILL%"), "{s}");
    assert!(s.contains("20/20"), "{s}");
    assert!(s.contains("100%"), "{s}");
    Ok(())
}

/// The `pull_through=` line names each state an operator acts on
/// differently, and flags an empty buyer wallet (#2603).
#[test]
fn pull_through_line_names_each_state() {
    use decdn_common::admin::PullThroughStatus;
    let on = |usdc| {
        pull_through_line(Some(&PullThroughStatus {
            enabled: true,
            buyer_wallet_micro_usdc: usdc,
        }))
    };
    assert_eq!(pull_through_line(None), "pull_through=(unknown)");
    assert_eq!(
        pull_through_line(Some(&PullThroughStatus {
            enabled: false,
            buyer_wallet_micro_usdc: Some(0),
        })),
        "pull_through=off"
    );
    assert_eq!(on(None), "pull_through=on buyer_wallet_usdc=(not read yet)");
    assert!(on(Some(0)).contains("buyer_wallet_usdc=0.000000 (unfunded"));
    assert_eq!(
        on(Some(2_000_001)),
        "pull_through=on buyer_wallet_usdc=2.000001"
    );
}

/// A node that reports no resolvable operator address (no chain wiring)
/// renders an explicit sentinel rather than dropping the line, so scripts
/// can always find the field.
#[test]
fn write_status_absent_operator_renders_unknown_sentinel() -> anyhow::Result<()> {
    let mut status = mk_status(Some(1_000_000));
    status.operator_address = None;
    let mut buf = Vec::<u8>::new();
    write_status(
        &mut buf,
        &status,
        &EarningsLine::Unavailable(String::new()),
        2_000_000,
    )?;
    let s = String::from_utf8(buf)?;
    assert!(s.contains("operator_address=(unknown)"), "{s}");
    Ok(())
}

#[test]
fn write_status_never_refreshed_renders_never() -> anyhow::Result<()> {
    let status = mk_status(None);
    let mut buf = Vec::<u8>::new();
    write_status(
        &mut buf,
        &status,
        &EarningsLine::Unavailable(String::new()),
        2_000_000,
    )?;
    let s = String::from_utf8(buf)?;
    assert!(s.contains("last_refresh=never"), "{s}");
    Ok(())
}

/// Cold-start: a node that has joined no buckets yet renders the
/// empty-table sentinel and omits the bucket table header entirely —
/// the precise scenario `decdn node status` exists to diagnose.
#[test]
fn write_status_empty_routing_table_renders_sentinel() -> anyhow::Result<()> {
    let mut status = mk_status(None);
    status.routing.buckets.clear();
    status.routing.non_empty_buckets = 0;
    status.routing.total_peers = 0;
    let mut buf = Vec::<u8>::new();
    write_status(
        &mut buf,
        &status,
        &EarningsLine::Unavailable(String::new()),
        2_000_000,
    )?;
    let s = String::from_utf8(buf)?;
    assert!(s.contains("(routing table empty"), "{s}");
    assert!(
        !s.contains("BUCKET"),
        "empty table must omit the header: {s}"
    );
    assert!(!s.contains("FILL%"), "{s}");
    Ok(())
}

/// Clock skew between the node's stamp and the CLI's wall clock must
/// render "in future" via `relative_age`, not a huge wrapped age.
#[test]
fn write_status_future_last_refresh_renders_in_future() -> anyhow::Result<()> {
    let status = mk_status(Some(5_000_000));
    let mut buf = Vec::<u8>::new();
    // now_us earlier than the stamp.
    write_status(
        &mut buf,
        &status,
        &EarningsLine::Unavailable(String::new()),
        1_000_000,
    )?;
    let s = String::from_utf8(buf)?;
    assert!(s.contains("last_refresh=in future"), "{s}");
    Ok(())
}

#[test]
fn percent_handles_zero_denominator() {
    assert_eq!(percent(0, 0), "n/a");
    assert_eq!(percent(5, 0), "n/a");
    assert_eq!(percent(1, 20), "5%");
    assert_eq!(percent(20, 20), "100%");
}

#[test]
fn format_interval_uses_coarsest_exact_unit() {
    assert_eq!(format_interval(0), "0s");
    assert_eq!(format_interval(45), "45s");
    assert_eq!(format_interval(30 * 60), "30m");
    assert_eq!(format_interval(3_600), "1h");
    assert_eq!(format_interval(2 * 86_400), "2d");
    // 90 minutes isn't a whole number of hours → falls back to minutes.
    assert_eq!(format_interval(90 * 60), "90m");
}

#[test]
fn resolve_admin_url_prefers_flag() {
    let got = resolve_admin_url(Some("http://custom:1234"), None).expect("flag path ok");
    assert_eq!(got, "http://custom:1234");
}

#[test]
fn admin_url_from_env_unset_is_none() {
    assert_eq!(admin_url_from_env(None).expect("unset ok"), None);
}

#[test]
fn admin_url_from_env_valid_port_builds_loopback_url() {
    assert_eq!(
        admin_url_from_env(Some("9999".to_string())).expect("valid ok"),
        Some("http://127.0.0.1:9999".to_string())
    );
    // Surrounding whitespace is tolerated (env values can carry it).
    assert_eq!(
        admin_url_from_env(Some(" 9999 ".to_string())).expect("trimmed ok"),
        Some("http://127.0.0.1:9999".to_string())
    );
}

#[test]
fn admin_url_from_env_zero_and_malformed_error() {
    assert!(admin_url_from_env(Some("0".to_string())).is_err());
    assert!(admin_url_from_env(Some("notaport".to_string())).is_err());
    // Out of u16 range.
    assert!(admin_url_from_env(Some("70000".to_string())).is_err());
}

#[test]
fn port_from_config_file_missing_default_returns_none() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("absent.toml");
    assert_eq!(
        port_from_config_file(Some(&path), ConfigPathSource::Default)?,
        None
    );
    Ok(())
}

#[test]
fn port_from_config_file_missing_explicit_errors() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("absent.toml");
    let err = port_from_config_file(Some(&path), ConfigPathSource::Explicit)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected explicit-missing error"))?
        .to_string();
    assert!(
        err.contains("failed to read config file"),
        "missing context: {err}"
    );
    Ok(())
}

#[test]
fn port_from_config_file_reads_admin_port_default_path() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("node.toml");
    std::fs::write(&path, b"[observability]\nadmin_port = 12345\n")?;
    assert_eq!(
        port_from_config_file(Some(&path), ConfigPathSource::Default)?,
        Some(12345)
    );
    Ok(())
}

#[test]
fn port_from_config_file_reads_admin_port_explicit_path() -> anyhow::Result<()> {
    // Explicit-source + valid file must succeed the same way as the
    // default-source case. Without this test, a regression that
    // broadened the explicit-source error arm to swallow successes
    // would still pass CI.
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("node.toml");
    std::fs::write(&path, b"[observability]\nadmin_port = 7777\n")?;
    assert_eq!(
        port_from_config_file(Some(&path), ConfigPathSource::Explicit)?,
        Some(7777)
    );
    Ok(())
}

#[test]
fn port_from_config_file_errors_on_explicit_zero() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("node.toml");
    std::fs::write(&path, b"[observability]\nadmin_port = 0\n")?;
    let err = port_from_config_file(Some(&path), ConfigPathSource::Default)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected error for explicit 0"))?
        .to_string();
    assert!(
        err.contains("disables the admin server"),
        "missing context: {err}"
    );
    Ok(())
}

#[test]
fn port_from_config_file_errors_on_invalid_toml() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("broken.toml");
    std::fs::write(&path, b"not = valid = toml")?;
    let err = port_from_config_file(Some(&path), ConfigPathSource::Default)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected parse error"))?
        .to_string();
    assert!(err.contains("parse"), "missing context: {err}");
    Ok(())
}

// Locks in the partial-deserializer choice: a wrong type in some
// unrelated section (here, a malformed `[network]` field that the
// full FileConfig would reject) must not stop these commands from
// resolving the admin port. If a future refactor reverts to
// parsing FileConfig, this test fails.
#[test]
fn port_from_config_file_ignores_unrelated_field_errors() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("node.toml");
    // `network.bind_addr` would be a string in the real schema;
    // making it an integer is a guaranteed type-mismatch for the
    // full FileConfig, but the narrow AdminPortConfig deserializer
    // never sees `network` so it must round-trip fine.
    std::fs::write(
        &path,
        b"[observability]\nadmin_port = 4242\n[network]\nbind_addr = 7\n",
    )?;
    assert_eq!(
        port_from_config_file(Some(&path), ConfigPathSource::Default)?,
        Some(4242)
    );
    Ok(())
}

fn mk_lane(
    pool_id: &str,
    counterparty: &str,
    last_nonce: u64,
    outstanding: u64,
    deposit: u64,
    secs_since: Option<u64>,
    eligible: bool,
) -> LaneSnapshot {
    LaneSnapshot {
        pool_id: pool_id.to_string(),
        counterparty: Some(counterparty.to_string()),
        // Self-signing default; the delegated and unknown-owner renderings
        // get their own dedicated tests rather than an eighth parameter.
        voucher_signer: counterparty.to_string(),
        last_nonce,
        outstanding_micro_usdc: outstanding,
        // Half the claim is still unredeemed in every fixture; the lanes
        // table renders both columns.
        unredeemed_micro_usdc: outstanding / 2,
        deposit_micro_usdc: deposit,
        seconds_since_last_voucher: secs_since,
        settlement_eligible: eligible,
    }
}

#[test]
fn write_lanes_table_empty_emits_sentinel() -> anyhow::Result<()> {
    let resp = LanesResponse {
        lanes: Vec::new(),
        redeem_threshold_micro_usdc: 1_000_000,
    };
    let mut buf = Vec::<u8>::new();
    write_lanes_table(&mut buf, &resp)?;
    let s = String::from_utf8(buf)?;
    // Summary line still prints the threshold, then the sentinel.
    assert!(s.contains("redeem_threshold=1.000000"), "{s}");
    assert!(s.contains("lanes=0"), "{s}");
    assert!(s.contains("(no open lanes)"), "{s}");
    // No table header when there are no rows.
    assert!(!s.contains("LANE"), "header must be omitted: {s}");
    Ok(())
}

#[test]
fn write_lanes_table_renders_header_and_rows() -> anyhow::Result<()> {
    // The DTO documents `counterparty` as an EIP-55 mixed-case
    // checksummed address (`alloy`'s `Address` Display), so the
    // fixture must be a real checksummed string — a lowercase
    // placeholder wouldn't exercise the mixed-case rendering the
    // table inherits verbatim. Derive it via `alloy` so the literal
    // is provably the canonical checksum, not a hand-typed guess.
    let counterparty =
        alloy::primitives::address!("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed").to_string();
    // Sanity-guard the fixture itself: the checksum is genuinely
    // mixed-case (some hex letters upper, some lower), so a regression
    // that lowercased it before rendering would be caught below.
    assert_ne!(
        counterparty,
        counterparty.to_lowercase(),
        "fixture must be a mixed-case EIP-55 address: {counterparty}"
    );
    let resp = LanesResponse {
        redeem_threshold_micro_usdc: 1_000_000,
        lanes: vec![
            mk_lane(
                &format!("0x{}", "a".repeat(64)),
                &counterparty,
                7,
                2_500_000,
                10_000_000,
                Some(90),
                true,
            ),
            mk_lane(
                &format!("0x{}", "c".repeat(64)),
                &format!("0x{}", "d".repeat(40)),
                0,
                0,
                5_000_000,
                None,
                false,
            ),
        ],
    };
    let mut buf = Vec::<u8>::new();
    write_lanes_table(&mut buf, &resp)?;
    let s = String::from_utf8(buf)?;
    assert!(s.contains("lanes=2"), "{s}");
    assert!(s.contains("LANE"), "header missing: {s}");
    assert!(s.contains("OUTSTANDING"), "header missing: {s}");
    assert!(s.contains("ELIGIBLE"), "header missing: {s}");
    // First row: USDC-formatted amounts, 90s → "1m ago", eligible "yes".
    assert!(s.contains("2.500000"), "outstanding USDC missing: {s}");
    assert!(s.contains("10.000000"), "deposit USDC missing: {s}");
    assert!(s.contains("1m ago"), "last-voucher age missing: {s}");
    // Lane id preview is the short form.
    assert!(s.contains("0xaaaaaaaaaa"), "lane preview missing: {s}");
    // Counterparty preview is the short form AND preserves the EIP-55
    // mixed case verbatim — `short_node_id` truncates to 12 chars, so
    // assert the row carries that checksummed prefix unchanged (a
    // regression that lowercased the address would miss this).
    let cp_preview = short_node_id(&counterparty);
    assert!(
        cp_preview.chars().any(|ch| ch.is_ascii_uppercase()),
        "expected mixed-case counterparty preview: {cp_preview}"
    );
    assert!(
        s.contains(&cp_preview),
        "checksummed counterparty preview missing: {s}"
    );
    // Second row: no activity → "never", not eligible → "no".
    assert!(s.contains("never"), "never sentinel missing: {s}");
    Ok(())
}

#[test]
fn write_slashes_table_empty_emits_sentinel() -> anyhow::Result<()> {
    let resp = SlashesResponse {
        slashes: Vec::new(),
    };
    let mut buf = Vec::<u8>::new();
    write_slashes_table(&mut buf, &resp)?;
    let s = String::from_utf8(buf)?;
    assert!(s.contains("slashes=0"), "{s}");
    assert!(s.contains("(no slashes detected)"), "{s}");
    // No table header when there are no rows.
    assert!(!s.contains("SLASH_ID"), "header must be omitted: {s}");
    Ok(())
}

#[test]
fn write_slashes_table_renders_header_and_rows() -> anyhow::Result<()> {
    let resp = SlashesResponse {
        slashes: vec![
            SlashRecordDto {
                slash_id: "42".to_string(),
                offense_type: 0,
                amount: "1_000_000".to_string(),
                evidence_hash: format!("0x{}", "a".repeat(64)),
                block_number: Some(1_234_567),
                appeal_window_close: Some(1_700_000_000),
            },
            SlashRecordDto {
                slash_id: "43".to_string(),
                offense_type: 1,
                amount: "2_500_000".to_string(),
                evidence_hash: format!("0x{}", "c".repeat(64)),
                block_number: None,
                appeal_window_close: None,
            },
        ],
    };
    let mut buf = Vec::<u8>::new();
    write_slashes_table(&mut buf, &resp)?;
    let s = String::from_utf8(buf)?;
    assert!(s.contains("slashes=2"), "{s}");
    assert!(s.contains("SLASH_ID"), "header missing: {s}");
    assert!(s.contains("APPEAL_CLOSE"), "header missing: {s}");
    // First row: slash id, offense type, amount, block, evidence preview.
    assert!(s.contains("42"), "slash id missing: {s}");
    assert!(s.contains("1_000_000"), "amount missing: {s}");
    assert!(s.contains("1234567"), "block number missing: {s}");
    assert!(s.contains("0xaaaaaaaaaa"), "evidence preview missing: {s}");
    // Second row: missing block / appeal close render as "?".
    assert!(s.contains('?'), "missing-value sentinel missing: {s}");
    Ok(())
}

/// The SIGNER column: a delegated lane renders the delegate, not the
/// funder.
#[test]
fn write_lanes_table_renders_the_voucher_signer_column() -> anyhow::Result<()> {
    let funder = format!("0x{}", "1".repeat(40));
    let delegate = format!("0x{}", "2".repeat(40));
    let mut delegated = mk_lane(
        &format!("0x{}", "a".repeat(64)),
        &funder,
        1,
        1,
        2,
        Some(1),
        false,
    );
    delegated.voucher_signer = delegate.clone();

    let mut buf = Vec::<u8>::new();
    write_lanes_table(
        &mut buf,
        &LanesResponse {
            redeem_threshold_micro_usdc: 1_000_000,
            lanes: vec![delegated],
        },
    )?;
    let s = String::from_utf8(buf)?;
    assert!(s.contains("SIGNER"), "SIGNER header missing: {s}");
    let row = s
        .lines()
        .find(|l| l.starts_with("0xaaaaaaaaaa"))
        .unwrap_or_default();
    assert!(
        row.contains(&short_node_id(&delegate)),
        "delegate signer preview missing: {row}"
    );
    assert_eq!(
        row.matches(&short_node_id(&funder)).count(),
        1,
        "the funder must appear once (COUNTERPARTY only), never in SIGNER: {row}"
    );
    Ok(())
}

/// A lane whose pool the node has not seen has no known owner: the
/// COUNTERPARTY column says `unknown`, and the SIGNER column still names the
/// signer. The signer never stands in for the owner.
#[test]
fn write_lanes_table_renders_an_unknown_counterparty() -> anyhow::Result<()> {
    let signer = format!("0x{}", "2".repeat(40));
    let mut lane = mk_lane(
        &format!("0x{}", "a".repeat(64)),
        &signer,
        0,
        1,
        0,
        None,
        false,
    );
    lane.counterparty = None;

    let mut buf = Vec::<u8>::new();
    write_lanes_table(
        &mut buf,
        &LanesResponse {
            redeem_threshold_micro_usdc: 1_000_000,
            lanes: vec![lane],
        },
    )?;
    let s = String::from_utf8(buf)?;
    let row = s
        .lines()
        .find(|l| l.starts_with("0xaaaaaaaaaa"))
        .unwrap_or_default();
    assert!(
        row.contains("unknown"),
        "unknown-owner sentinel missing: {row}"
    );
    assert_eq!(
        row.matches(&short_node_id(&signer)).count(),
        1,
        "the signer must appear once (SIGNER only), never as the owner: {row}"
    );
    Ok(())
}

/// `--json` renders an unknown owner as `null`, not an empty string or the
/// signer, so a script can tell "not seen" from an address.
#[test]
fn lanes_json_renders_an_unknown_counterparty_as_null() -> anyhow::Result<()> {
    let mut lane = mk_lane(
        &format!("0x{}", "a".repeat(64)),
        &format!("0x{}", "2".repeat(40)),
        0,
        1,
        0,
        None,
        false,
    );
    lane.counterparty = None;
    let pretty = serde_json::to_string_pretty(&LanesResponse {
        redeem_threshold_micro_usdc: 1_000_000,
        lanes: vec![lane],
    })?;
    let value: serde_json::Value = serde_json::from_str(&pretty)?;
    let c0 = value["lanes"]
        .as_array()
        .and_then(|lanes| lanes.first())
        .ok_or_else(|| anyhow::anyhow!("one lane: {pretty}"))?;
    // Present as `null`, not absent: `Value` indexing reads a missing key as
    // `Null` too, so check the key itself.
    assert!(
        c0.as_object()
            .is_some_and(|lane| lane.contains_key("counterparty")),
        "counterparty key must be present: {pretty}"
    );
    assert!(
        c0["counterparty"].is_null(),
        "counterparty must be null: {pretty}"
    );
    assert!(
        c0["voucher_signer"].is_string(),
        "signer still reported: {pretty}"
    );
    Ok(())
}

#[test]
fn format_usdc_renders_six_decimals() {
    assert_eq!(format_usdc(0), "0.000000");
    assert_eq!(format_usdc(1_000_000), "1.000000");
    assert_eq!(format_usdc(2_500_000), "2.500000");
    assert_eq!(format_usdc(1), "0.000001");
    assert_eq!(format_usdc(12_345_678), "12.345678");
}

/// `decdn node lanes --json` serializes the `LanesResponse`
/// DTO with `serde_json::to_string_pretty` (the seam the `--json`
/// branch in [`lanes`] uses). Assert the pretty encoding (a)
/// round-trips back to the same value and (b) carries every
/// load-bearing field with its wire key, so a rename or a
/// skipped-field regression on the DTO breaks here rather than only
/// at the shell. The counterparty is a real EIP-55 checksummed
/// address so the JSON reflects production output.
#[test]
fn lanes_json_pretty_roundtrips_and_carries_fields() -> anyhow::Result<()> {
    let counterparty =
        alloy::primitives::address!("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed").to_string();
    let resp = LanesResponse {
        redeem_threshold_micro_usdc: 1_000_000,
        lanes: vec![mk_lane(
            &format!("0x{}", "a".repeat(64)),
            &counterparty,
            7,
            2_500_000,
            10_000_000,
            Some(42),
            true,
        )],
    };

    // Exact seam the `--json` branch uses.
    let pretty = serde_json::to_string_pretty(&resp)?;
    // Pretty form is multi-line (indented) — guards against an
    // accidental switch to the compact encoder.
    assert!(
        pretty.contains('\n'),
        "pretty JSON must be multi-line: {pretty}"
    );

    // Round-trips back through the DTO with no lossy field — the
    // generated client deserializes this exact shape. (The DTO doesn't
    // derive `PartialEq`, so assert the reconstructed fields directly
    // rather than comparing whole structs.)
    let back: LanesResponse = serde_json::from_str(&pretty)?;
    assert_eq!(back.redeem_threshold_micro_usdc, 1_000_000);
    assert_eq!(back.lanes.len(), 1);
    let bc = back.lanes.first().expect("one lane");
    assert_eq!(bc.counterparty.as_deref(), Some(counterparty.as_str()));
    assert_eq!(bc.last_nonce, 7);
    assert_eq!(bc.outstanding_micro_usdc, 2_500_000);
    assert_eq!(bc.deposit_micro_usdc, 10_000_000);
    assert_eq!(bc.seconds_since_last_voucher, Some(42));
    assert!(bc.settlement_eligible);

    // Each wire key is present with the expected value, including the
    // checksummed counterparty verbatim (mixed-case preserved).
    let value: serde_json::Value = serde_json::from_str(&pretty)?;
    assert_eq!(value["redeem_threshold_micro_usdc"], 1_000_000);
    let chans = value["lanes"].as_array().expect("lanes array");
    assert_eq!(chans.len(), 1);
    let c0 = &chans[0];
    assert_eq!(c0["counterparty"].as_str(), Some(counterparty.as_str()));
    assert_eq!(c0["last_nonce"], 7);
    assert_eq!(c0["outstanding_micro_usdc"], 2_500_000);
    assert_eq!(c0["deposit_micro_usdc"], 10_000_000);
    assert_eq!(c0["seconds_since_last_voucher"], 42);
    assert_eq!(c0["settlement_eligible"], true);
    Ok(())
}

#[test]
fn format_age_units() {
    assert_eq!(format_age(500_000), "<1s ago");
    assert_eq!(format_age(2_000_000), "2s ago");
    assert_eq!(format_age(90 * 1_000_000), "1m ago");
    assert_eq!(format_age(2 * 3600 * 1_000_000), "2h ago");
    assert_eq!(format_age(36 * 3600 * 1_000_000), "1d ago");
}

#[test]
fn relative_age_handles_future_and_zero() {
    assert_eq!(relative_age(100, 200), "in future");
    assert_eq!(relative_age(100, 0), "unknown");
}

#[test]
fn short_node_id_trims_long_hex() {
    let full = "a".repeat(64);
    let s = short_node_id(&full);
    assert_eq!(s.chars().count(), 13); // 12 hex + ellipsis
    assert!(s.ends_with('…'));
}

#[test]
fn short_node_id_passthrough_when_already_short() {
    let s = short_node_id("abcd");
    assert_eq!(s, "abcd");
}

/// `--dry-run` plain output is multi-line, key=value, grep-friendly.
/// Asserts every load-bearing field appears on its own line so a
/// regression that collapsed the table back to one line (or dropped
/// e.g. `pinned=`) breaks here rather than silently eating the
/// information the operator needs to decide whether to run the real
/// evict.
#[test]
fn write_dry_run_human_emits_all_fields() -> anyhow::Result<()> {
    use decdn_common::admin::EvictPreview;
    let resp = EvictResponse {
        was_present: true,
        dry_run: true,
        preview: EvictPreview {
            size_bytes: Some(1024),
            last_accessed_us_ago: Some(2_000_000), // 2s ago via format_age
            pinned: true,
            already_evicted: false,
            origin_kinds: vec![decdn_cache::OriginKind::Http],
        },
    };
    let mut buf = Vec::<u8>::new();
    write_dry_run_human(&mut buf, "abcd", &resp)?;
    let s = String::from_utf8(buf)?;
    assert!(s.contains("hash=abcd"), "missing hash: {s}");
    assert!(s.contains("dry_run=true"), "missing dry_run tag: {s}");
    assert!(s.contains("was_present=true"), "missing was_present: {s}");
    assert!(s.contains("pinned=true"), "missing pinned: {s}");
    assert!(
        s.contains("already_evicted=false"),
        "missing already_evicted: {s}"
    );
    assert!(s.contains("size_bytes=1024"), "missing size_bytes: {s}");
    assert!(
        s.contains("last_accessed=2s ago"),
        "expected formatted last_accessed, got: {s}"
    );
    assert!(
        s.contains("origin_kinds=http"),
        "missing origin_kinds (#439, #284): {s}"
    );
    Ok(())
}

/// Sentinels for the "no information available" cases:
/// `size_bytes=not_stored` and `last_accessed=none_since_start`. Distinct from
/// "0" / "<1s ago" so an operator can tell "the engine has no
/// record" from "the record is at the floor".
#[test]
fn write_dry_run_human_uses_sentinels_for_absent_fields() -> anyhow::Result<()> {
    use decdn_common::admin::EvictPreview;
    let resp = EvictResponse {
        was_present: false,
        dry_run: true,
        preview: EvictPreview {
            size_bytes: None,
            last_accessed_us_ago: None,
            pinned: false,
            already_evicted: false,
            // Cache-only mode: no origin configured, so the
            // dry-run reports `none` rather than omitting the
            // line entirely (#439).
            origin_kinds: Vec::new(),
        },
    };
    let mut buf = Vec::<u8>::new();
    write_dry_run_human(&mut buf, "deadbeef", &resp)?;
    let s = String::from_utf8(buf)?;
    assert!(
        s.contains("size_bytes=not_stored"),
        "expected not_stored sentinel, got: {s}"
    );
    assert!(
        s.contains("origin_kinds=none"),
        "expected origin_kinds=none sentinel for cache-only mode, got: {s}"
    );
    assert!(
        s.contains("last_accessed=none_since_start"),
        "expected none_since_start sentinel, got: {s}"
    );
    Ok(())
}

/// Multi-origin chain (#284): the dry-run preview surfaces every
/// configured backend kind, comma-separated in declared order, so
/// operators evaluating worst-case egress cost across a fallback
/// chain see the full chain length and composition rather than
/// only the primary's kind.
#[test]
fn write_dry_run_human_renders_multi_origin_kinds_in_declared_order() -> anyhow::Result<()> {
    use decdn_common::admin::EvictPreview;
    let resp = EvictResponse {
        was_present: true,
        dry_run: true,
        preview: EvictPreview {
            size_bytes: Some(512),
            last_accessed_us_ago: Some(1_500_000),
            pinned: false,
            already_evicted: false,
            origin_kinds: vec![
                decdn_cache::OriginKind::Http,
                decdn_cache::OriginKind::S3,
                decdn_cache::OriginKind::Filesystem,
            ],
        },
    };
    let mut buf = Vec::<u8>::new();
    write_dry_run_human(&mut buf, "cafebabe", &resp)?;
    let s = String::from_utf8(buf)?;
    // Order is operator-controlled and load-bearing — assert the
    // exact comma-separated sequence rather than just substring
    // matches, so a regression that sorts or dedupes the list is
    // caught here.
    assert!(
        s.contains("origin_kinds=http,s3,filesystem"),
        "expected ordered comma-separated chain, got: {s}"
    );
    Ok(())
}

/// `filter_candidates` (#1481): network-free filtering behind `decdn node
/// lookup`, unit-tested without a chain or network per the task brief.
fn lookup_candidate(seed: u8, region: &str) -> decdn_client::discovery::NodeCandidate {
    decdn_client::discovery::NodeCandidate {
        node_id: iroh::SecretKey::from_bytes(&[seed; 32]).public(),
        eth_address: alloy::primitives::Address::repeat_byte(seed),
        region_hint: decdn_protocol::Region::parse(region),
        multiaddrs: alloy::primitives::Bytes::new(),
    }
}

#[test]
fn filter_candidates_no_filter_returns_all() {
    let cands = vec![lookup_candidate(1, "US"), lookup_candidate(2, "EU")];
    let out = filter_candidates(cands.clone(), None, None);
    assert_eq!(out, cands);
}

#[test]
fn filter_candidates_exact_node_id_match() {
    let a = lookup_candidate(1, "US");
    let b = lookup_candidate(2, "EU");
    let out = filter_candidates(vec![a.clone(), b], Some(a.node_id), None);
    assert_eq!(out, vec![a]);
}

#[test]
fn filter_candidates_region_match() {
    let a = lookup_candidate(1, "US");
    let b = lookup_candidate(2, "EU");
    let c = lookup_candidate(3, "US");
    let out = filter_candidates(
        vec![a.clone(), b, c.clone()],
        None,
        decdn_protocol::Region::parse("US"),
    );
    assert_eq!(out, vec![a, c]);
}

#[test]
fn filter_candidates_node_id_and_region_combine() {
    let a = lookup_candidate(1, "US");
    let b = lookup_candidate(2, "US");
    let out = filter_candidates(
        vec![a.clone(), b],
        Some(a.node_id),
        decdn_protocol::Region::parse("US"),
    );
    assert_eq!(out, vec![a]);
}

mod earnings {
    use decdn_common::admin::{EarningsPeriod, LastSettlement, PendingRedeem};

    use super::*;

    const NOW: u64 = 1_792_926_150; // 2026-10-25 11:02:30 UTC
    const LEDGER_SINCE: u64 = 1_780_000_000; // 2026-05-28

    fn period(from: u64, net: u64, bytes: u64, settlements: u64) -> EarningsPeriod {
        EarningsPeriod {
            from_unix: from,
            net_micro_usdc: net,
            gross_micro_usdc: net * 10 / 6,
            bytes_settled: bytes,
            settlements,
        }
    }

    fn response() -> EarningsResponse {
        EarningsResponse {
            operator_address: Some("0x52908400098527886E0F7030069857D2E4169EE7".to_owned()),
            ledger_since_unix: Some(LEDGER_SINCE),
            last_24h: period(NOW - 86_400, 4_120_000, 412_300_000_000, 9),
            last_7d: period(NOW - 604_800, 29_070_000, 2_900_000_000_000, 61),
            since_ledger_start: period(LEDGER_SINCE, 121_410_000, 12_400_000_000_000, 1_160),
            since: None,
            last_settlement: Some(LastSettlement {
                at_unix: NOW - 3_600,
                tx_hash: "0xabc".to_owned(),
                block_number: 123_456_789,
                net_micro_usdc: 500_000,
            }),
            pending: Some(PendingRedeem {
                micro_usdc: 380_000,
                lanes: 3,
                stranded_micro_usdc: 0,
                stranded_lanes: 0,
                last_sweep_at_unix: Some(NOW - 50),
                next_sweep_at_unix: Some(NOW + 250),
            }),
        }
    }

    fn render(resp: &EarningsResponse) -> String {
        let mut buf = Vec::<u8>::new();
        write_earnings(&mut buf, resp, NOW).expect("write");
        String::from_utf8(buf).expect("utf8")
    }

    #[test]
    fn write_earnings_renders_header_table_last_and_pending() {
        let s = render(&response());
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(
            lines.first().copied(),
            Some("operator=0x52908400098527886E0F7030069857D2E4169EE7 ledger_since=2026-05-28")
        );
        assert!(
            lines.get(1).is_some_and(|l| l.starts_with("PERIOD")
                && l.contains("SETTLED_USDC")
                && l.contains("BYTES_SETTLED")
                && l.contains("SETTLEMENTS")),
            "{s}"
        );
        let day = lines.get(2).copied().unwrap_or_default();
        assert!(day.starts_with("last 24h "), "{day}");
        assert!(
            day.contains("4.120000") && day.contains("412.3 GB") && day.trim_end().ends_with('9'),
            "{day}"
        );
        let week = lines.get(3).copied().unwrap_or_default();
        assert!(
            week.starts_with("last 7d ")
                && week.contains("29.070000")
                && week.contains("2.9 TB")
                && week.contains("61"),
            "{week}"
        );
        let all = lines.get(4).copied().unwrap_or_default();
        assert!(
            all.starts_with("since 2026-05-28 ")
                && all.contains("121.410000")
                && all.contains("12.4 TB")
                && all.contains("1160"),
            "{all}"
        );
        assert_eq!(
            lines.get(5).copied(),
            Some("last settlement: 2026-10-25 10:02 UTC, tx 0xabc, block 123456789")
        );
        assert_eq!(
            lines.get(6).copied(),
            Some(
                "pending redeem: 0.380000 USDC over 3 lanes (next sweep in ~4 min, last sweep \
                 50s ago)"
            )
        );
        assert_eq!(lines.len(), 7, "no stranded line when nothing is stranded");
    }

    #[test]
    fn write_earnings_names_an_empty_ledger_and_a_missing_schedule() {
        let mut resp = response();
        resp.ledger_since_unix = None;
        resp.last_settlement = None;
        resp.since_ledger_start = period(0, 0, 0, 0);
        resp.pending = Some(PendingRedeem {
            micro_usdc: 0,
            lanes: 0,
            stranded_micro_usdc: 0,
            stranded_lanes: 0,
            last_sweep_at_unix: None,
            next_sweep_at_unix: None,
        });
        let s = render(&resp);
        assert!(s.contains("ledger_since=(no settlements yet)"), "{s}");
        assert!(s.contains("since (no settlements yet)"), "{s}");
        assert!(s.contains("0 B"), "{s}");
        assert!(s.contains("last settlement: none"), "{s}");
        assert!(
            s.contains(
                "pending redeem: 0.000000 USDC over 0 lanes (next sweep not scheduled yet, no \
                 sweep yet)"
            ),
            "{s}"
        );
    }

    #[test]
    fn a_period_older_than_the_ledger_says_so() {
        let mut resp = response();
        // The ledger started two days ago: the 7-day row and a `--since` from
        // a month ago cover less than their names say.
        resp.ledger_since_unix = Some(NOW - 2 * 86_400);
        resp.since_ledger_start.from_unix = NOW - 2 * 86_400;
        resp.since = Some(period(NOW - 30 * 86_400, 7_000_000, 1_000, 2));
        let s = render(&resp);
        assert!(
            s.lines()
                .any(|l| l.starts_with("last 24h ") && !l.contains("ledger from")),
            "{s}"
        );
        assert!(
            s.lines()
                .any(|l| l.starts_with("last 7d (ledger from 2026-10-23)")),
            "{s}"
        );
        assert!(
            s.lines().any(
                |l| l.starts_with("since 2026-09-25 (ledger from 2026-10-23)")
                    && l.contains("7.000000")
            ),
            "{s}"
        );
    }

    #[test]
    fn write_earnings_in_chain_mode_omits_pending() {
        let mut resp = response();
        resp.pending = None;
        let s = render(&resp);
        assert!(!s.contains("pending redeem"), "{s}");
        assert!(!s.contains("stranded"), "{s}");
    }

    #[test]
    fn stranded_value_gets_its_own_line() {
        let mut resp = response();
        if let Some(p) = resp.pending.as_mut() {
            p.stranded_micro_usdc = 1_500_000;
            p.stranded_lanes = 2;
        }
        let s = render(&resp);
        assert!(s.contains("stranded: 1.500000 USDC over 2 lanes"), "{s}");
    }

    #[test]
    fn a_due_sweep_says_so() {
        let mut resp = response();
        if let Some(p) = resp.pending.as_mut() {
            p.next_sweep_at_unix = Some(NOW - 1);
        }
        assert!(render(&resp).contains("(next sweep due now,"));
    }

    #[test]
    fn approx_duration_rounds_to_the_unit_that_reads() {
        assert_eq!(approx_duration(5), "5s");
        assert_eq!(approx_duration(250), "4 min");
        assert_eq!(approx_duration(3_600 + 120), "1h 2m");
    }

    #[test]
    fn status_line_and_setup_line_summarise_the_response() {
        let resp = response();
        assert_eq!(
            earnings_summary_line(&EarningsLine::Available(&resp)),
            "earnings=ok 24h=4.120000 7d=29.070000 since_ledger_start=121.410000 \
             pending=0.380000 last_settlement=2026-10-25T10:02Z"
        );
        assert_eq!(
            earnings_summary_line(&EarningsLine::Unavailable(String::new())),
            "earnings=unavailable"
        );
        assert_eq!(
            earnings_summary_line(&EarningsLine::Unavailable(
                "settlement ledger load failed: corrupt".to_owned()
            )),
            "earnings=unavailable reason=\"settlement ledger load failed: corrupt\""
        );
        assert_eq!(
            earned_so_far_line(&Ok(Some(resp.clone()))).as_deref(),
            Some("earned_so_far=121.410000 USDC settlements=1160")
        );
        assert_eq!(earned_so_far_line(&Ok(None)), None);
        assert_eq!(
            earned_so_far_line(&Err("no ledger".to_owned())).as_deref(),
            Some("earned_so_far=unavailable reason=\"no ledger\"")
        );
        assert_eq!(first_line("line one\nline two"), "line one");
    }

    /// A daemon's JSON-RPC error reaches the status and setup lines as its
    /// code and message, not as the error object's debug form, so the
    /// runbook's `settlement ledger load failed: …` text is what the
    /// operator greps for.
    #[test]
    fn a_daemon_error_renders_as_its_message_on_the_status_line() {
        let err = JsonRpcClientError::Call(jsonrpsee::types::ErrorObjectOwned::owned(
            -32_010,
            "settlement ledger load failed: corrupt record at block 7 log 2",
            None::<()>,
        ));
        let reason = earnings_unavailable_reason("http://127.0.0.1:9191", 2_000, err);
        assert_eq!(
            reason,
            "admin at http://127.0.0.1:9191 returned JSON-RPC error -32010: \
             settlement ledger load failed: corrupt record at block 7 log 2"
        );
        assert!(!reason.contains("ErrorObject"), "{reason}");
        assert_eq!(
            earnings_summary_line(&EarningsLine::Unavailable(reason.clone())),
            format!("earnings=unavailable reason={reason:?}")
        );
        let timeout = earnings_unavailable_reason(
            "http://127.0.0.1:9191",
            2_000,
            JsonRpcClientError::RequestTimeout,
        );
        assert!(
            timeout.starts_with("admin at http://127.0.0.1:9191 did not respond within 2000ms"),
            "{timeout}"
        );
    }

    /// A daemon that accepts the connection and never answers is a reason on
    /// the `earned_so_far` line, not silence; only a refused connection is
    /// silent. The listener completes the handshake into its backlog and
    /// nothing ever reads the request.
    #[tokio::test]
    async fn earned_so_far_reports_a_timeout_and_stays_silent_only_when_refused()
    -> anyhow::Result<()> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("config.toml");
        std::fs::write(&path, format!("[observability]\nadmin_port = {port}\n"))?;
        let reason = earned_so_far(Some(&path), Duration::from_millis(200))
            .await
            .expect_err("a daemon that does not answer is reported");
        assert!(
            reason.starts_with(&format!(
                "admin at http://127.0.0.1:{port} did not respond within 200ms"
            )),
            "{reason}"
        );
        // Nothing listens on the port once the listener is gone.
        drop(listener);
        let silent = earned_so_far(Some(&path), Duration::from_millis(200)).await;
        assert!(matches!(silent, Ok(None)), "{silent:?}");
        Ok(())
    }

    #[test]
    fn write_status_carries_the_earnings_line() -> anyhow::Result<()> {
        let status = mk_status(Some(1_000_000));
        let mut buf = Vec::<u8>::new();
        write_status(
            &mut buf,
            &status,
            &EarningsLine::Available(&response()),
            2_000_000,
        )?;
        let s = String::from_utf8(buf)?;
        assert!(s.contains("\nearnings=ok 24h=4.120000 7d=29.070000"), "{s}");
        Ok(())
    }
}
