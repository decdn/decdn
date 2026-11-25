//! `decdn setup` and `decdn node register` refuse a multiaddr set whose packed
//! form exceeds `CapacityBond.maxMultiaddrSize` before any prompt or
//! transaction.
//!
//! # Why this exists
//!
//! `registerNode` reverts `MultiaddrsTooLarge` above the ceiling. Setup
//! registers after `approve`, `bond` and `declareMbps`, so without a pre-check
//! an oversized set fails only once those are mined. The ceiling is a chain
//! read, so only a test against a deployed contract shows where the check runs.

#![cfg(feature = "anvil-e2e")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

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

#[tokio::test(flavor = "multi_thread")]
async fn an_oversized_multiaddr_set_fails_before_any_prompt_or_transaction() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, oversized_set())
        .await
        .context("cli_multiaddr_size timed out")?
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

/// One address whose packed form exceeds any ceiling the contract accepts
/// (`maxMultiaddrSize` is at most 1,024 bytes).
fn oversized_multiaddr() -> String {
    format!("/dns4/{}.example/udp/4433/quic-v1", "a".repeat(1_100))
}

async fn oversized_set() -> anyhow::Result<()> {
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

    let too_large = oversized_multiaddr();
    let mbps = TARGET_MBPS.to_string();
    let setup = ["setup", "--mbps", &mbps, "--region", "US"];

    // `node register` without `--accept-terms` and without a terminal aborts at
    // the terms prompt. A size error instead shows the check runs first.
    let register = run(
        &node,
        &["node", "register", "--region", "US"],
        &too_large,
        &[],
    )
    .await?;
    assert_refused(&register, "node register");

    let dry = run(&node, &setup, &too_large, &["--dry-run", "--json"]).await?;
    assert_refused(&dry, "setup --dry-run");

    let live = run(
        &node,
        &setup,
        &too_large,
        &["--yes", "--accept-terms", "--json"],
    )
    .await?;
    assert_refused(&live, "setup");

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

/// The command must fail with the pre-check's error, which names the call it
/// guards and the revert it prevents.
fn assert_refused(out: &Output, what: &str) {
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

/// `decdn <args> --multiaddr <multiaddr> <extra>` against the node's config,
/// asserting nothing.
async fn run(
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
