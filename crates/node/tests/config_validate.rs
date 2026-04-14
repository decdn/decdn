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

/// Build a TOML fixture that references `${var_name}` in `rpc_url`. We
/// accept the name as a parameter (rather than hard-coding it) so each test
/// can pick a unique-per-process name and guarantee the variable is unset —
/// a fixed name could, in principle, be set in the ambient test environment
/// and make the assertion below pass when it should fail.
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
    // Embed the PID so concurrent test runners and any ambient environment
    // in CI cannot have this variable set. Assert it is unset before we
    // rely on the failure — if it is, fail loudly rather than silently pass.
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
