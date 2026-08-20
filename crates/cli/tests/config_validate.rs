//! Integration tests for `decdn config validate`.
//!
//! Env-var resolution is covered by the expansion tests inside `config::tests`
//! — mutating the process environment is `unsafe` under edition 2024 and the
//! workspace forbids `unsafe_code`, so those paths are exercised through
//! `HOME`-driven fixtures there rather than from this harness.

use std::fs;
use std::path::{Path, PathBuf};

use clap::Parser;
use decdn_cli::commands::config as commands;
use decdn_common::cli::{ConfigValidateArgs, RunArgs};
use decdn_common::config::{
    ResolvedBlockchain, ResolvedCache, ResolvedConfig, ResolvedIdentity, ResolvedNetwork,
    ResolvedObservability, ResolvedPayment, ResolvedSecurity,
};
use tempfile::TempDir;

// Local `Parser` wrapper lets tests build a `RunArgs` from `&[&str]` without
// dragging in the full `Cli` subcommand matcher.
#[derive(Parser, Debug)]
struct RunArgsWrap {
    #[command(flatten)]
    run: RunArgs,
}

const VALID_CONFIG: &str = r#"
[identity]
region = "us"

[blockchain]
rpc_url = "https://sepolia-rollup.arbitrum.io/rpc"
payment_pool_address = "0x0000000000000000000000000000000000000001"
capacity_bond_address = "0x0000000000000000000000000000000000000002"
slash_judge_address = "0x0000000000000000000000000000000000000003"
content_blacklist_address = "0x0000000000000000000000000000000000000004"
"#;

const MISSING_RPC: &str = r#"
[blockchain]
payment_pool_address = "0x0000000000000000000000000000000000000001"
capacity_bond_address = "0x0000000000000000000000000000000000000002"
"#;

fn unknown_var_config(var_name: &str) -> String {
    format!(
        r#"
[blockchain]
rpc_url = "${{{var_name}}}"
payment_pool_address = "0x0000000000000000000000000000000000000001"
capacity_bond_address = "0x0000000000000000000000000000000000000002"
"#
    )
}

// Points `--data-dir` at a tempdir so the test does not depend on `$HOME`.
fn args(data_dir: &Path) -> anyhow::Result<ConfigValidateArgs> {
    let data_dir = data_dir
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("non-utf8 tempdir path"))?;
    let wrap = RunArgsWrap::try_parse_from(["decdn", "--data-dir", data_dir])?;
    Ok(ConfigValidateArgs { run: wrap.run })
}

fn write_config(dir: &TempDir, body: &str) -> anyhow::Result<PathBuf> {
    let path = dir.path().join("node.toml");
    fs::write(&path, body)?;
    Ok(path)
}

// -- end-to-end tests that drive `config_validate` -------------------------

#[test]
fn validate_passes_for_complete_config() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = write_config(&dir, VALID_CONFIG)?;
    fs::write(dir.path().join("keystore.json"), "")?;
    commands::config_validate(Some(&path), &args(dir.path())?)
}

#[test]
fn validate_fails_for_malformed_relay_url() -> anyhow::Result<()> {
    // #818: a malformed `network.relay_urls` entry must fail `config validate`
    // up front and name the offending entry, rather than only surfacing later
    // at node bring-up. Otherwise this is the complete, valid config.
    let body = format!(
        "{VALID_CONFIG}\n[network]\nrelay_urls = [\"https://ok.example\", \"not a url\"]\n"
    );
    let dir = TempDir::new()?;
    let path = write_config(&dir, &body)?;
    fs::write(dir.path().join("keystore.json"), "")?;
    let err = commands::config_validate(Some(&path), &args(dir.path())?)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected validation to fail"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("network.relay_urls[1]") && msg.contains("not a url"),
        "error should name the malformed relay entry: {msg}"
    );
    Ok(())
}

#[test]
fn validate_fails_for_malformed_discovery_peer_addr() -> anyhow::Result<()> {
    // #818 scope 1: a malformed `[network.discovery]` peer socket address must
    // fail `config validate` up front and name the offending indexed field.
    let id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let body = format!(
        "{VALID_CONFIG}\n[network.discovery.peers.{id}]\naddrs = [\"not-a-socket-addr\"]\n"
    );
    let dir = TempDir::new()?;
    let path = write_config(&dir, &body)?;
    fs::write(dir.path().join("keystore.json"), "")?;
    let err = commands::config_validate(Some(&path), &args(dir.path())?)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected validation to fail"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains(&format!("network.discovery.peers[{id}].addrs[0]"))
            && msg.contains("not-a-socket-addr"),
        "error should name the malformed peer address: {msg}"
    );
    Ok(())
}

#[test]
fn validate_fails_when_required_field_missing() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = write_config(&dir, MISSING_RPC)?;
    let err = commands::config_validate(Some(&path), &args(dir.path())?)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected validation to fail"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("rpc_url"),
        "error should mention rpc_url: {msg}"
    );
    Ok(())
}

#[test]
fn validate_fails_for_unknown_field_in_section() -> anyhow::Result<()> {
    // #842: a typo'd key inside a known section must fail at load rather than
    // silently keeping the default for the intended (security-relevant) knob.
    let body = format!("{VALID_CONFIG}\n[security]\nmax_concurrent_handlerss = 10\n");
    let dir = TempDir::new()?;
    let path = write_config(&dir, &body)?;
    fs::write(dir.path().join("keystore.json"), "")?;
    let err = commands::config_validate(Some(&path), &args(dir.path())?)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected validation to fail"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("max_concurrent_handlerss"),
        "error should name the unknown key: {msg}"
    );
    Ok(())
}

#[test]
fn validate_fails_for_typoed_whole_section() -> anyhow::Result<()> {
    // #842: a typo'd whole-section header (`[netork]` for `[network]`) is the
    // highest-value catch — `deny_unknown_fields` on `FileConfig` rejects it
    // instead of dropping the entire section.
    let body = format!("{VALID_CONFIG}\n[netork]\nrelay_urls = [\"https://ok.example\"]\n");
    let dir = TempDir::new()?;
    let path = write_config(&dir, &body)?;
    fs::write(dir.path().join("keystore.json"), "")?;
    let err = commands::config_validate(Some(&path), &args(dir.path())?)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected validation to fail"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("netork"),
        "error should name the unknown section: {msg}"
    );
    Ok(())
}

#[test]
fn validate_fails_when_env_var_unset() -> anyhow::Result<()> {
    // PID-suffixed name plus an up-front `var_os` check guarantees neither a
    // concurrent test nor ambient CI environment can silently satisfy the
    // variable and flip this assertion to a false pass.
    let var_name = format!("DECDN_TEST_UNSET_{}", std::process::id());
    anyhow::ensure!(
        std::env::var_os(&var_name).is_none(),
        "test precondition: {var_name} must not be set"
    );

    let dir = TempDir::new()?;
    let path = write_config(&dir, &unknown_var_config(&var_name))?;
    let err = commands::config_validate(Some(&path), &args(dir.path())?)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected validation to fail"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains(&var_name),
        "error should name the missing env var: {msg}"
    );
    Ok(())
}

#[test]
fn validate_fails_when_config_flag_points_at_missing_file() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let missing = dir.path().join("does-not-exist.toml");
    let err = commands::config_validate(Some(&missing), &args(dir.path())?)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected validation to fail"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("does-not-exist.toml"),
        "error should name the missing file: {msg}"
    );
    // `load_file_config` stays fail-fast: a file that never parsed has
    // nothing to accumulate. Post-parse validation aggregates into the
    // `configuration has N problem(s):` header, but a file-load failure
    // must surface as-is so the operator gets the parser error directly.
    anyhow::ensure!(
        !msg.contains("problem(s):"),
        "file-load failure must not be wrapped in the aggregate header: {msg}"
    );
    Ok(())
}

// Three independent problems across three sections: missing rpc_url,
// max_blob_size_mb >= cache_size_mb, and rate_per_mb = 0.
const MULTI_ERROR: &str = r#"
[blockchain]
payment_pool_address = "0x0000000000000000000000000000000000000001"
capacity_bond_address = "0x0000000000000000000000000000000000000002"
slash_judge_address = "0x0000000000000000000000000000000000000003"

[cache]
cache_size_mb = 100
max_blob_size_mb = 500

[payment]
rate_per_mb = 0
"#;

#[test]
fn validate_emits_all_problems_at_once() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = write_config(&dir, MULTI_ERROR)?;
    fs::write(dir.path().join("keystore.json"), "")?;
    let err = commands::config_validate(Some(&path), &args(dir.path())?)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected validation to fail"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("configuration has 4 problem(s):"),
        "expected aggregated header: {msg}"
    );
    for needle in [
        "rpc_url",
        "content_blacklist_address",
        "max_blob_size_mb",
        "rate_per_mb",
    ] {
        anyhow::ensure!(
            msg.contains(needle),
            "aggregated error should name {needle}: {msg}"
        );
    }
    Ok(())
}

// -- tests for the `effective_source` helper ------------------------------

#[test]
fn effective_source_prefers_explicit_path() -> anyhow::Result<()> {
    let explicit = PathBuf::from("/tmp/decdn-test-explicit.toml");
    let resolved = commands::effective_source(Some(&explicit), || {
        Some(PathBuf::from("/tmp/decdn-test-default.toml"))
    })?;
    anyhow::ensure!(resolved == Some(explicit), "explicit path must win");
    Ok(())
}

#[test]
fn effective_source_falls_back_to_default_when_present() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let default = dir.path().join("node.toml");
    fs::write(&default, "")?;
    let resolved = commands::effective_source(None, || Some(default.clone()))?;
    anyhow::ensure!(
        resolved.as_deref() == Some(default.as_path()),
        "default path should be reported when it exists",
    );
    Ok(())
}

#[test]
fn effective_source_returns_none_when_default_missing() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let default = dir.path().join("absent.toml");
    let resolved = commands::effective_source(None, || Some(default))?;
    anyhow::ensure!(
        resolved.is_none(),
        "absent default path should yield None, not a lie"
    );
    Ok(())
}

#[test]
fn effective_source_returns_none_when_no_default_available() -> anyhow::Result<()> {
    let resolved = commands::effective_source(None, || None)?;
    anyhow::ensure!(resolved.is_none());
    Ok(())
}

// -- tests for the printed summary (redaction + optional-field gating) ----

#[allow(clippy::too_many_lines)] // exhaustive struct literal, not real complexity
fn sample_resolved(overrides: impl FnOnce(&mut ResolvedConfig)) -> ResolvedConfig {
    let mut cfg = ResolvedConfig {
        identity: ResolvedIdentity {
            data_dir: PathBuf::from("/var/lib/decdn"),
            region: None,
        },
        network: ResolvedNetwork {
            bind_port: 4433,
            relay_urls: Vec::new(),
            discovery: decdn_common::config::ResolvedDiscovery::default(),
        },
        blockchain: ResolvedBlockchain {
            origin_assignment_address: None,
            origin_directory_positive_ttl_sec:
                decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_POSITIVE_TTL_SEC,
            origin_directory_negative_ttl_sec:
                decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_NEGATIVE_TTL_SEC,
            origin_directory_cache_capacity:
                decdn_common::config::DEFAULT_ORIGIN_DIRECTORY_CACHE_CAPACITY,
            publisher_registry_address: None,
            rpc_url: "https://rpc.example/SECRET_TOKEN_abc123".to_string(),
            eth_keystore: PathBuf::from("/var/lib/decdn/keystore.json"),
            keystore_password_file: None,
            payment_pool_address: "0x0000000000000000000000000000000000000001".to_string(),
            capacity_bond_address: "0x0000000000000000000000000000000000000002".to_string(),
            rpc_watchdog_interval_sec: 30,
            event_poll_interval_ms: 7000,
            rate_bounds_poll_interval_sec: 3600,
            redeem_threshold_micro_usdc: 1_000_000,
            redeem_max_vouchers_per_tx: 300,
            redeem_interval_secs: 300,
            buyer_working_deposit_micro_usdc: 10_000_000,
            buyer_max_approve: true,
            pool_min_remaining_deposit_micro_usdc: 100_000,
            slash_judge_address: "0x0000000000000000000000000000000000000003".to_string(),
            content_blacklist_address: None,
            content_blacklist_poll_interval_sec: 600,
            chain_id: decdn_common::config::DEFAULT_CHAIN_ID,
        },
        cache: ResolvedCache {
            cache_dir: PathBuf::from("/var/lib/decdn/cache"),
            cache_size_mb: 10_240,
            max_blob_size_mb: 1_024,
            max_rate_per_mb: 0,
            origins: Vec::new(),
            pinned_hashes: decdn_cache::PinnedHashes::empty(),
            origin_retry: decdn_cache::RetryPolicy::default(),
            circuit_breaker: decdn_cache::CircuitBreakerPolicy::default(),
            user_agent: decdn_cache::DEFAULT_USER_AGENT.to_string(),
            gc_interval_sec: 300,
            fs_rescan_interval_sec: 60,
            origin_probe_ttl_sec: decdn_common::config::DEFAULT_ORIGIN_PROBE_TTL_SEC,
            origin_probe_timeout_ms: decdn_common::config::DEFAULT_ORIGIN_PROBE_TIMEOUT_MS,
            origin_probe_memo_capacity: decdn_common::config::DEFAULT_ORIGIN_PROBE_MEMO_CAPACITY,
            eviction_high_water_pct: 90,
            eviction_target_pct: 80,
            eviction_per_sweep_budget: 16,
            eviction_tick_secs: 1,
            max_probe_holds: decdn_common::config::DEFAULT_MAX_PROBE_HOLDS,
            stake_lane_reserved_holds: decdn_common::config::DEFAULT_STAKE_LANE_RESERVED_HOLDS,
            node_to_node_pull_through_enabled: false,
            node_pull_probe_fanout: decdn_common::config::DEFAULT_NODE_PULL_PROBE_FANOUT,
            node_pull_timeout_sec: decdn_common::config::DEFAULT_NODE_PULL_TIMEOUT_SEC,
            node_pull_stall_timeout_sec: decdn_common::config::DEFAULT_NODE_PULL_STALL_TIMEOUT_SEC,
        },
        payment: ResolvedPayment {
            rate_per_mb: 10,
            delivery_floor: 0,
            credit_max: decdn_common::config::DEFAULT_CREDIT_MAX,
            credit_ramp_divisor: decdn_common::config::DEFAULT_CREDIT_RAMP_DIVISOR,
            voucher_commit_interval_ms: decdn_common::config::DEFAULT_VOUCHER_COMMIT_INTERVAL_MS,
        },
        observability: ResolvedObservability {
            log_level: decdn_common::cli::common::LogLevel::Info,
            log_format: decdn_common::cli::LogFormat::Pretty,
            metrics_port: 9090,
            metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            admin_port: Some(9191),
            otlp_endpoint: None,
        },
        security: ResolvedSecurity {
            max_concurrent_handlers: 256,
            per_source_rate_per_sec: 100.0,
            per_source_burst: 200,
            max_tracked_sources: 4096,
        },
        load_shed: decdn_common::config::ResolvedLoadShed::default(),
        dht: decdn_common::config::ResolvedDht::default(),
        probe: decdn_common::config::ResolvedProbe::default(),
        receipts: decdn_common::config::ResolvedReceipts::default(),
        content: decdn_common::config::ResolvedContent::default(),
    };
    overrides(&mut cfg);
    cfg
}

fn render(source: Option<&Path>, cfg: &ResolvedConfig) -> anyhow::Result<String> {
    let mut buf = Vec::new();
    commands::write_validate_summary(&mut buf, source, cfg)?;
    Ok(String::from_utf8(buf)?)
}

/// The node→node pull-through summary is gated on the feature being ENABLED, so with the
/// fixture's default `false` none of it renders and every knob on that line is unexercised
/// — including `stall_timeout_sec`, added by #1134. A knob an operator cannot see is a knob
/// they cannot check, and `decdn config validate` is where they look.
#[test]
fn summary_reports_the_pull_through_deadlines_when_enabled() -> anyhow::Result<()> {
    let cfg = sample_resolved(|c| {
        c.cache.node_to_node_pull_through_enabled = true;
        c.cache.node_pull_timeout_sec = 25;
        c.cache.node_pull_stall_timeout_sec = 15;
    });
    let out = render(None, &cfg)?;
    anyhow::ensure!(
        out.contains("pull_timeout_sec=25"),
        "the summary must report the resolved stream-open budget: {out}"
    );
    anyhow::ensure!(
        out.contains("stall_timeout_sec=15"),
        "the summary must report the resolved inactivity budget — it is the primary health \
         signal, and it is a term of the derived outer deadline: {out}"
    );
    Ok(())
}

/// `fs_rescan_interval_sec` (#1508) controls how much origin work a node does on
/// its own initiative, and it was invisible in `decdn config validate` — the one
/// place an operator checks what a config actually resolved to. A knob that costs
/// I/O must not be silently on or off.
#[test]
fn summary_reports_the_origin_rescan_knob() -> anyhow::Result<()> {
    let out = render(
        None,
        &sample_resolved(|c| c.cache.fs_rescan_interval_sec = 45),
    )?;
    anyhow::ensure!(
        out.contains("fs_rescan_interval_sec:   45"),
        "the summary must report the resolved rescan cadence: {out}"
    );

    let out = render(
        None,
        &sample_resolved(|c| c.cache.fs_rescan_interval_sec = 0),
    )?;
    anyhow::ensure!(
        out.contains("fs_rescan_interval_sec:   disabled"),
        "a zero cadence must render as disabled, not as the bare number 0: {out}"
    );
    Ok(())
}

#[test]
fn summary_reports_discovery_without_leaking_secrets() -> anyhow::Result<()> {
    // #818: the summary names dns_origin and the peer count, redacts the
    // pkarr_url userinfo (its only credential vector) while keeping the host
    // visible for diagnostics, and never echoes peer addresses.
    let cfg = sample_resolved(|c| {
        c.network.discovery = decdn_common::config::ResolvedDiscovery {
            pkarr_url: Some("https://user:PKARR_SECRET_xyz@pkarr.example/".to_string()),
            dns_origin: Some("discovery.example.".to_string()),
            peers: vec![decdn_common::config::ResolvedDiscoveryPeer {
                node_id: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
                relay_url: None,
                addrs: vec!["203.0.113.4:4433".to_string()],
            }],
        };
    });
    let out = render(None, &cfg)?;
    anyhow::ensure!(
        out.contains("discovery.pkarr_url:"),
        "summary should emit the pkarr_url label: {out}"
    );
    anyhow::ensure!(
        out.contains("discovery.dns_origin:     discovery.example."),
        "summary should name dns_origin: {out}"
    );
    anyhow::ensure!(
        out.contains("discovery.peers:          1"),
        "summary should report the peer count: {out}"
    );
    anyhow::ensure!(
        !out.contains("PKARR_SECRET_xyz"),
        "pkarr_url credentials must never appear in summary: {out}"
    );
    anyhow::ensure!(
        out.contains("***@pkarr.example/"),
        "pkarr_url userinfo should be redacted with the host preserved: {out}"
    );
    anyhow::ensure!(
        !out.contains("203.0.113.4:4433"),
        "peer addresses must not appear in summary: {out}"
    );
    Ok(())
}

#[test]
fn summary_omits_discovery_lines_when_unset() -> anyhow::Result<()> {
    // Default discovery is empty (the node uses the n0 pkarr/DNS default), so
    // the summary must print no `discovery.*` lines at all.
    let cfg = sample_resolved(|_| {});
    let out = render(None, &cfg)?;
    anyhow::ensure!(
        !out.contains("discovery."),
        "no discovery lines should appear when discovery is unset: {out}"
    );
    Ok(())
}

#[test]
fn summary_redacts_rpc_url_value() -> anyhow::Result<()> {
    let cfg = sample_resolved(|_| {});
    let out = render(None, &cfg)?;
    anyhow::ensure!(
        !out.contains("SECRET_TOKEN_abc123"),
        "rpc_url value must never appear in summary: {out}"
    );
    anyhow::ensure!(
        out.contains("rpc_url:                  <redacted>"),
        "rpc_url should be labeled <redacted>: {out}"
    );
    Ok(())
}

#[test]
fn summary_redacts_otlp_endpoint_value() -> anyhow::Result<()> {
    let cfg = sample_resolved(|c| {
        c.observability.otlp_endpoint = Some("https://otel.example/?token=OTEL_SECRET_xyz".into());
    });
    let out = render(None, &cfg)?;
    anyhow::ensure!(
        !out.contains("OTEL_SECRET_xyz"),
        "otlp_endpoint value must never appear in summary: {out}"
    );
    anyhow::ensure!(
        out.contains("otlp_endpoint:            <redacted>"),
        "otlp_endpoint should be labeled <redacted>: {out}"
    );
    Ok(())
}

#[test]
fn summary_omits_optional_fields_when_unset() -> anyhow::Result<()> {
    let cfg = sample_resolved(|_| {}); // region/relay_url/otlp_endpoint = None
    let out = render(None, &cfg)?;
    for label in ["region:", "relay_url:", "otlp_endpoint:"] {
        anyhow::ensure!(
            !out.contains(label),
            "unset optional field {label} should not appear: {out}"
        );
    }
    Ok(())
}

#[test]
fn summary_prints_optional_fields_when_set() -> anyhow::Result<()> {
    let cfg = sample_resolved(|c| {
        c.identity.region = Some("US".into());
        c.network.relay_urls = vec!["https://relay.iroh.network".into()];
        c.observability.otlp_endpoint = Some("https://otel.example".into());
    });
    let out = render(None, &cfg)?;
    anyhow::ensure!(out.contains("region:                   US"), "{out}");
    anyhow::ensure!(
        out.contains("relay_url:                https://relay.iroh.network"),
        "{out}"
    );
    anyhow::ensure!(out.contains("otlp_endpoint:"), "{out}");
    Ok(())
}

#[test]
fn summary_prints_one_line_per_relay() -> anyhow::Result<()> {
    let cfg = sample_resolved(|c| {
        c.network.relay_urls = vec![
            "https://relay-a.example".into(),
            "https://relay-b.example".into(),
        ];
    });
    let out = render(None, &cfg)?;
    anyhow::ensure!(
        out.contains("relay_url:                https://relay-a.example"),
        "{out}"
    );
    anyhow::ensure!(
        out.contains("relay_url:                https://relay-b.example"),
        "{out}"
    );
    Ok(())
}

#[test]
fn summary_redacts_relay_url_credentials() -> anyhow::Result<()> {
    // #862: a relay entry carrying `user:pass@` userinfo must be redacted in
    // the summary (host kept for diagnostics), the same treatment pkarr_url
    // gets — the summary is the share-into-an-issue surface that's why rpc_url
    // is hidden in the first place.
    let cfg = sample_resolved(|c| {
        c.network.relay_urls = vec!["https://user:RELAY_SECRET_xyz@relay.example:7842".into()];
    });
    let out = render(None, &cfg)?;
    anyhow::ensure!(
        !out.contains("RELAY_SECRET_xyz"),
        "relay credentials must never appear in summary: {out}"
    );
    anyhow::ensure!(
        out.contains("***@relay.example:7842"),
        "relay userinfo should be redacted with the host preserved: {out}"
    );
    Ok(())
}

#[test]
fn summary_reports_receipt_log_path_and_rotation_cap() -> anyhow::Result<()> {
    // #964: the download-receipt audit log (#802) must be confirmable from the
    // summary alone. Surface the derived log path (data_dir + canonical
    // filename) alongside the rotation cap and retained-backup count, so an
    // operator can verify receipt logging without reading the raw TOML.
    let cfg = sample_resolved(|c| {
        c.receipts.max_file_bytes = 64 * 1024 * 1024;
        c.receipts.retained_files = 7;
    });
    let out = render(None, &cfg)?;
    // The log lives at the canonical filename under the resolved data_dir.
    // Derive the expected path from the fixture's own data_dir so the
    // assertion can't drift from the sample, tracking the same constant the
    // daemon uses.
    let expected_path = cfg
        .identity
        .data_dir
        .join(decdn_common::config::RECEIPT_LOG_FILE)
        .display()
        .to_string();
    anyhow::ensure!(
        out.contains(&format!("receipts.log_path:        {expected_path}")),
        "summary should surface the derived receipt log path: {out}"
    );
    anyhow::ensure!(
        out.contains(&format!("receipts.max_file_bytes:  {}", 64 * 1024 * 1024)),
        "summary should reflect the resolved rotation cap: {out}"
    );
    anyhow::ensure!(
        out.contains("receipts.retained_files:  7"),
        "summary should reflect the resolved retained-backup count: {out}"
    );
    Ok(())
}

#[test]
fn summary_reports_receipt_defaults() -> anyhow::Result<()> {
    // The defaults are always present (the [receipts] section is config-
    // additive, #807), so the summary reports the built-in cap and retention
    // even when nothing is configured — never a "disabled" line.
    let cfg = sample_resolved(|_| {});
    let out = render(None, &cfg)?;
    anyhow::ensure!(
        out.contains(&format!(
            "receipts.max_file_bytes:  {}",
            decdn_common::config::DEFAULT_RECEIPT_MAX_FILE_BYTES
        )),
        "summary should report the default rotation cap: {out}"
    );
    anyhow::ensure!(
        out.contains(&format!(
            "receipts.retained_files:  {}",
            decdn_common::config::DEFAULT_RECEIPT_RETAINED_FILES
        )),
        "summary should report the default retained-backup count: {out}"
    );
    Ok(())
}

#[test]
fn summary_reports_explicit_source_path() -> anyhow::Result<()> {
    let cfg = sample_resolved(|_| {});
    let src = PathBuf::from("/etc/decdn/node.toml");
    let out = render(Some(&src), &cfg)?;
    anyhow::ensure!(
        out.contains("source:                   /etc/decdn/node.toml"),
        "{out}"
    );
    Ok(())
}

#[test]
fn summary_reports_defaults_only_when_no_source() -> anyhow::Result<()> {
    let cfg = sample_resolved(|_| {});
    let out = render(None, &cfg)?;
    anyhow::ensure!(
        out.contains("source:                   (defaults + env only"),
        "{out}"
    );
    Ok(())
}
