//! Integration tests for `decdn config validate`.
//!
//! Env-var resolution is covered by the expansion tests inside `config::tests`
//! — mutating the process environment is `unsafe` under edition 2024 and the
//! workspace forbids `unsafe_code`, so those paths are exercised through
//! `HOME`-driven fixtures there rather than from this harness.

use std::fs;
use std::path::{Path, PathBuf};

use clap::Parser;
use decdn_node::cli::{ConfigValidateArgs, RunArgs};
use decdn_node::commands;
use decdn_node::config::{
    ResolvedBlockchain, ResolvedCache, ResolvedConfig, ResolvedGossip, ResolvedIdentity,
    ResolvedNetwork, ResolvedObservability, ResolvedPayment,
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
payment_channel_address = "0x0000000000000000000000000000000000000001"
staking_registry_address = "0x0000000000000000000000000000000000000002"
"#;

const MISSING_RPC: &str = r#"
[blockchain]
payment_channel_address = "0x0000000000000000000000000000000000000001"
staking_registry_address = "0x0000000000000000000000000000000000000002"
"#;

fn unknown_var_config(var_name: &str) -> String {
    format!(
        r#"
[blockchain]
rpc_url = "${{{var_name}}}"
payment_channel_address = "0x0000000000000000000000000000000000000001"
staking_registry_address = "0x0000000000000000000000000000000000000002"
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

fn sample_resolved(overrides: impl FnOnce(&mut ResolvedConfig)) -> ResolvedConfig {
    let mut cfg = ResolvedConfig {
        identity: ResolvedIdentity {
            data_dir: PathBuf::from("/var/lib/decdn"),
            region: None,
        },
        network: ResolvedNetwork {
            bind_port: 4433,
            relay_url: None,
        },
        blockchain: ResolvedBlockchain {
            rpc_url: "https://rpc.example/SECRET_TOKEN_abc123".to_string(),
            eth_keystore: PathBuf::from("/var/lib/decdn/keystore.json"),
            payment_channel_address: "0x0000000000000000000000000000000000000001".to_string(),
            staking_registry_address: "0x0000000000000000000000000000000000000002".to_string(),
            rpc_watchdog_interval_sec: 30,
        },
        cache: ResolvedCache {
            cache_dir: PathBuf::from("/var/lib/decdn/cache"),
            cache_size_mb: 10_240,
            max_blob_size_mb: 1_024,
            origin_url: None::<decdn_cache::OriginUrl>,
            origin_path: None,
        },
        payment: ResolvedPayment { rate_per_mb: 10 },
        observability: ResolvedObservability {
            log_level: decdn_node::cli::common::LogLevel::Info,
            log_format: decdn_node::cli::LogFormat::Pretty,
            metrics_port: 9090,
            metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            admin_port: Some(9191),
            otlp_endpoint: None,
        },
        gossip: ResolvedGossip {
            announce_interval_sec: 60,
            peer_ttl_sec: 600,
            subscribe_global: true,
            allowlist: Vec::new(),
        },
    };
    overrides(&mut cfg);
    cfg
}

fn render(source: Option<&Path>, cfg: &ResolvedConfig) -> anyhow::Result<String> {
    let mut buf = Vec::new();
    commands::write_validate_summary(&mut buf, source, cfg)?;
    Ok(String::from_utf8(buf)?)
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
        c.network.relay_url = Some("https://relay.iroh.network".into());
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
