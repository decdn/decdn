//! Integration tests for `decdn config validate`.
//!
//! Exercises [`commands::config_validate`] directly against TOML fixtures on
//! disk. Env-var handling is covered by the expansion tests inside
//! `config::tests` — mutating the process environment is `unsafe` under
//! edition 2024 and the workspace forbids `unsafe_code`, so those paths are
//! validated through `HOME`-driven fixtures there and a manual smoke test in
//! the plan file rather than from this harness.

use std::fs;
use std::path::PathBuf;

use clap::Parser;
use decdn_node::cli::{ConfigValidateArgs, RunArgs};
use decdn_node::commands;
use tempfile::TempDir;

/// Tiny `Parser` wrapper around `RunArgs` so the tests can build one from a
/// `&[&str]` without going through the full `Cli` + subcommand matcher.
#[derive(Parser, Debug)]
struct RunArgsWrap {
    #[command(flatten)]
    run: RunArgs,
}

const VALID_CONFIG: &str = r#"
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

const UNKNOWN_VAR: &str = r#"
[blockchain]
rpc_url = "${DECDN_TEST_CONFIG_VALIDATE_UNSET_XYZ}"
payment_channel_address = "0x0000000000000000000000000000000000000001"
staking_registry_address = "0x0000000000000000000000000000000000000002"
"#;

/// Build a `ConfigValidateArgs` with no CLI overrides, pointing the data
/// directory at `data_dir` so the test does not depend on `$HOME`.
fn args(data_dir: &std::path::Path) -> anyhow::Result<ConfigValidateArgs> {
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

#[test]
fn validate_passes_for_complete_config() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = write_config(&dir, VALID_CONFIG)?;
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
    let dir = TempDir::new()?;
    let path = write_config(&dir, UNKNOWN_VAR)?;
    let err = commands::config_validate(Some(&path), &args(dir.path())?)
        .err()
        .ok_or_else(|| anyhow::anyhow!("expected validation to fail"))?;
    let msg = format!("{err:#}");
    anyhow::ensure!(
        msg.contains("DECDN_TEST_CONFIG_VALIDATE_UNSET_XYZ"),
        "error should name the missing env var: {msg}"
    );
    Ok(())
}
