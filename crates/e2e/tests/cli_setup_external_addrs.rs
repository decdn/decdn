//! `decdn setup` checks `network.external_addrs` on a node that already has a
//! key only when a registration is due (ADR 019 § Phase 2).
//!
//! # Why this exists
//!
//! On a clean slate setup checks the entries before it generates keys, and the
//! `decdn` binary tests cover that. A node with a key checks them after it
//! reads its registration state. That check needs a chain: without it a bad
//! entry fails at the register step, after `approve`, `bond` and `declareMbps`
//! are mined. The other direction matters too: a registered node registers no
//! address, so a bad entry must not stop it.
//!
//! `decdn setup` and `decdn node register` also refuse a multiaddr set whose
//! packed form exceeds `CapacityBond.maxMultiaddrSize` before any prompt or
//! transaction. `registerNode` reverts `MultiaddrsTooLarge` above the ceiling,
//! and the ceiling is a chain read, so only a deployed contract shows where
//! the check runs.

#![cfg(feature = "anvil-e2e")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

use anyhow::Context;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::decdn_command;
use decdn_e2e::node::{KEYSTORE_PASSWORD, NodeFixture};
use tokio::process::Command;

/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule).
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// Tier to bond up to, inside the deployed `[minCapacityMbps,
/// maxCapacityMbps]` band and above what onboarding posts, so a live run has a
/// real bond step.
const TARGET_MBPS: u64 = 1000;

/// A private address: [`decdn_common::net::parse_external_addr`] refuses it.
const BAD_ENTRY: &str = "10.0.0.5:4433";

#[tokio::test(flavor = "multi_thread")]
async fn setup_refuses_a_bad_entry_before_any_transaction_when_registration_is_due()
-> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, refuses_when_due())
        .await
        .context("cli_setup_external_addrs (due) timed out")?
}

#[tokio::test(flavor = "multi_thread")]
async fn setup_ignores_a_bad_entry_on_a_registered_node() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, ignores_when_registered())
        .await
        .context("cli_setup_external_addrs (registered) timed out")?
}

#[tokio::test(flavor = "multi_thread")]
async fn an_oversized_multiaddr_set_fails_before_any_prompt_or_transaction() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, refuses_an_oversized_set())
        .await
        .context("cli_setup_external_addrs (oversized) timed out")?
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".parse().expect("static filter parses")),
        )
        .with_writer(std::io::stderr)
        .try_init();
}

async fn refuses_when_due() -> anyhow::Result<()> {
    init_tracing();
    let chain = ChainFixture::launch().await?;
    // Keys and a keystore, nothing on chain.
    let (node, _hash) = NodeFixture::launch_bare(&chain, "US", b"external-addrs due").await?;
    let operator = node.operator_addr();

    // Fund the whole target, so the pre-flight passes and the run reaches the
    // registration-state read.
    let target = chain
        .min_bond()
        .await?
        .max(chain.bond_required(TARGET_MBPS).await?);
    chain.transfer_token(operator, target).await?;
    let token_before = chain.token_balance(operator).await?;
    let bond_before = chain.active_bond(operator).await?;

    let config = config_with_bad_entry(&node)?;
    let out = run_setup(&node, &config, &["--yes", "--accept-terms", "--json"]).await?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "a bad entry must stop a due registration.\nstderr: {stderr}"
    );
    assert!(stderr.contains("network.external_addrs[0]"), "{stderr}");
    assert!(stderr.contains("10.0.0.5"), "{stderr}");

    assert_eq!(
        chain.token_balance(operator).await?,
        token_before,
        "no TOKEN moved"
    );
    assert_eq!(
        chain.active_bond(operator).await?,
        bond_before,
        "no bond was posted"
    );
    assert!(!chain.is_active(operator).await?, "the node is not active");
    Ok(())
}

async fn ignores_when_registered() -> anyhow::Result<()> {
    init_tracing();
    let chain = ChainFixture::launch().await?;
    // Bonded at the minimum and registered.
    let (node, _hash) = NodeFixture::launch(&chain, "US", b"external-addrs registered").await?;
    let operator = node.operator_addr();
    let target = chain
        .min_bond()
        .await?
        .max(chain.bond_required(TARGET_MBPS).await?);
    chain.transfer_token(operator, target).await?;

    let config = config_with_bad_entry(&node)?;

    let dry = run_setup(&node, &config, &["--dry-run", "--json"]).await?;
    assert!(
        dry.status.success(),
        "a registered node's dry run ignores the entry.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&dry.stdout),
        String::from_utf8_lossy(&dry.stderr)
    );
    let summary = last_json_line(&dry)?;
    assert_eq!(
        summary["readiness"]["multiaddrs"].as_u64(),
        Some(0),
        "a registered node registers no address: {summary}"
    );

    let live = run_setup(&node, &config, &["--yes", "--accept-terms", "--json"]).await?;
    assert!(
        live.status.success(),
        "a registered node's live run ignores the entry.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&live.stdout),
        String::from_utf8_lossy(&live.stderr)
    );
    assert_eq!(chain.declared_mbps(operator).await?, TARGET_MBPS);
    assert!(chain.is_active(operator).await?, "the node stays active");
    Ok(())
}

async fn refuses_an_oversized_set() -> anyhow::Result<()> {
    init_tracing();
    let chain = ChainFixture::launch().await?;
    // Keys and a keystore, nothing on chain.
    let (node, _hash) = NodeFixture::launch_bare(&chain, "US", b"multiaddr size").await?;
    let operator = node.operator_addr();

    // Fund the whole target, so the pre-flight passes and setup reaches the
    // registration-state read.
    let target = chain
        .min_bond()
        .await?
        .max(chain.bond_required(TARGET_MBPS).await?);
    chain.transfer_token(operator, target).await?;
    let token_before = chain.token_balance(operator).await?;
    let bond_before = chain.active_bond(operator).await?;

    // One address whose packed form exceeds any ceiling the contract accepts
    // (`maxMultiaddrSize` is at most 1,024 bytes).
    let too_large = format!("/dns4/{}.example/udp/4433/quic-v1", "a".repeat(1_100));
    let mbps = TARGET_MBPS.to_string();
    let setup = ["setup", "--mbps", &mbps, "--region", "US"];

    // `node register` without `--accept-terms` and without a terminal aborts at
    // the terms prompt. A size error instead shows the check runs first.
    let register = run_with_multiaddr(
        &node,
        &["node", "register", "--region", "US"],
        &too_large,
        &[],
    )
    .await?;
    assert_size_refused(&register, "node register");

    let dry = run_with_multiaddr(&node, &setup, &too_large, &["--dry-run", "--json"]).await?;
    assert_size_refused(&dry, "setup --dry-run");

    let live = run_with_multiaddr(
        &node,
        &setup,
        &too_large,
        &["--yes", "--accept-terms", "--json"],
    )
    .await?;
    assert_size_refused(&live, "setup");

    assert_eq!(
        chain.token_balance(operator).await?,
        token_before,
        "no TOKEN moved"
    );
    assert_eq!(
        chain.active_bond(operator).await?,
        bond_before,
        "no bond was posted"
    );
    assert!(!chain.is_active(operator).await?, "the node is not active");
    Ok(())
}

/// The command must fail with the size pre-check's error, which names the
/// call it guards and the revert it prevents.
fn assert_size_refused(out: &Output, what: &str) {
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !out.status.success(),
        "{what} must refuse an oversized set.\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("`registerNode` would revert MultiaddrsTooLarge"),
        "{what} fails at the size pre-check, not later.\nstderr: {stderr}"
    );
}

/// `decdn <args> --multiaddr <multiaddr> <extra>` against the node's own
/// config, asserting nothing.
async fn run_with_multiaddr(
    node: &NodeFixture,
    args: &[&str],
    multiaddr: &str,
    extra: &[&str],
) -> anyhow::Result<Output> {
    Command::from(decdn_command(node.data_dir(), KEYSTORE_PASSWORD)?)
        .args(args)
        .args(["--multiaddr", multiaddr])
        .args(extra)
        .arg("--config")
        .arg(node.config_path())
        .kill_on_drop(true)
        .output()
        .await
        .with_context(|| format!("spawn decdn {}", args.join(" ")))
}

/// A copy of the node's config whose existing `[network]` table gains a bad
/// `external_addrs` entry. The daemon keeps its own file.
fn config_with_bad_entry(node: &NodeFixture) -> anyhow::Result<PathBuf> {
    let mut doc: toml::Table = std::fs::read_to_string(node.config_path())?
        .parse()
        .context("parse node config")?;
    doc.get_mut("network")
        .and_then(toml::Value::as_table_mut)
        .context("node config has no [network] table")?
        .insert(
            "external_addrs".to_owned(),
            toml::Value::Array(vec![toml::Value::String(BAD_ENTRY.to_owned())]),
        );
    let path = node.data_dir().join("external-addrs.toml");
    std::fs::write(&path, toml::to_string(&doc)?)?;
    Ok(path)
}

/// `decdn setup` against `config`, asserting nothing.
async fn run_setup(node: &NodeFixture, config: &Path, extra: &[&str]) -> anyhow::Result<Output> {
    Command::from(decdn_command(node.data_dir(), KEYSTORE_PASSWORD)?)
        .arg("setup")
        .args(["--mbps", &TARGET_MBPS.to_string(), "--region", "US"])
        .args(extra)
        .arg("--config")
        .arg(config)
        .kill_on_drop(true)
        .output()
        .await
        .context("spawn decdn setup")
}

fn last_json_line(out: &Output) -> anyhow::Result<serde_json::Value> {
    let text = String::from_utf8_lossy(&out.stdout);
    let line = text
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .context("command produced no stdout")?;
    serde_json::from_str(line).with_context(|| format!("parse JSON summary: {line}"))
}
