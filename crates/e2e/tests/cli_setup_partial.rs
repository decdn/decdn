//! `decdn setup` must report what already landed when a live phase fails
//! part-way (#1355).
//!
//! # Why this exists
//!
//! `setup` submits an irreversible sequence — USDC swap, then `approve`, then
//! `bond`, then `declareMbps`, then `registerNode`. Under `--json` its aggregated
//! summary is the *only* machine-readable carrier of those transaction hashes:
//! the human-mode progress lines are suppressed. Before #1355 a mid-sequence
//! failure propagated straight out, so an operator whose TOKEN had already moved
//! into `CapacityBond` saw an error and an empty stdout.
//!
//! The unit tests around that fix cover `build_summary`'s *shape*, which is not
//! the same thing as covering the restructure: deleting the partial-report
//! emission entirely leaves them green (verified). This journey covers the
//! behaviour — that a real `setup`, failing at a real on-chain write, still
//! prints the summary.
//!
//! # How the failure is induced
//!
//! Pausing `CapacityBond` is the one deterministic way to fail a *write* while
//! every *read* still succeeds. `bond()` is `whenNotPaused`; the balance,
//! allowance and bond-curve views the pre-flight uses are not. So the command
//! clears its pre-flight gate, enters the live phase, and reverts inside it —
//! which is exactly the window the reporting fix protects. Starving the operator
//! of TOKEN would not do: that trips the pre-flight and the live phase is never
//! entered.

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

/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule);
/// anvil launch + forge deploy dominate its runtime.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// Tier to bond up to. Must exceed what `onboard_operator` posted (`minBond`,
/// undeclared) so the run has a real shortfall to `approve` + `bond`, and must
/// sit inside the deployed `[minCapacityMbps, maxCapacityMbps]` band.
const TARGET_MBPS: u64 = 1000;

#[tokio::test(flavor = "multi_thread")]
async fn setup_reports_the_partial_outcome_when_a_live_phase_fails() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, run())
        .await
        .context("cli_setup_partial timed out")?
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".parse().expect("static filter parses")),
        )
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let (node, _hash) = NodeFixture::launch(&chain, "US", b"cli-setup-partial fixture").await?;
    let operator = node.operator_addr();

    // Fund the full curve target so the pre-flight's balance check passes — the
    // point is to fail at the *write*, not at a funding gate.
    let target = chain
        .min_bond()
        .await?
        .max(chain.bond_required(TARGET_MBPS).await?);
    chain.transfer_token(operator, target).await?;
    let bond_before = chain.active_bond(operator).await?;
    assert!(
        target > bond_before,
        "the run needs a real shortfall to bond; onboarding already posted {bond_before}"
    );

    // Reads stay live, writes revert.
    chain.pause_capacity_bond().await?;

    let out = run_setup_json(&node).await?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(
        !out.status.success(),
        "a reverted bond must exit non-zero.\nstdout: {stdout}\nstderr: {stderr}"
    );

    // The regression this journey exists for: stdout must not be empty. Deleting
    // the partial-report emission fails here, which no unit test does.
    let summary: serde_json::Value = {
        let line = stdout
            .lines()
            .rev()
            .find(|l| !l.trim().is_empty())
            .with_context(|| {
                format!(
                    "setup --json printed NOTHING on a mid-sequence failure — the #1355 \
                         regression.\nstderr: {stderr}"
                )
            })?;
        serde_json::from_str(line).with_context(|| format!("parse partial summary: {line}"))?
    };

    // Exactly one object: the partial report replaces the success summary, it is
    // not printed alongside it.
    assert_eq!(
        stdout.lines().filter(|l| !l.trim().is_empty()).count(),
        1,
        "expected exactly one JSON object on stdout, got:\n{stdout}"
    );

    assert_eq!(
        summary.get("partial").and_then(serde_json::Value::as_bool),
        Some(true),
        "a failed run must be flagged partial: {summary}"
    );
    assert!(
        summary
            .get("readiness")
            .is_some_and(serde_json::Value::is_null),
        "the run never reached the read-back, so readiness must be null: {summary}"
    );
    // Same shape as a successful run — a consumer parses one schema and branches
    // on `partial`, rather than having to recognise an error object.
    for key in [
        "dry_run",
        "preflight",
        "bond",
        "register",
        "swap",
        "readiness",
    ] {
        assert!(
            summary.get(key).is_some(),
            "partial summary must keep the success shape, missing `{key}`: {summary}"
        );
    }
    // The bond itself definitively did not take effect, and the report says so
    // rather than omitting the field.
    assert!(
        summary["bond"]
            .get("bond_tx")
            .is_some_and(serde_json::Value::is_null),
        "a reverted bond must report a null bond_tx: {summary}"
    );

    // The re-run guidance is the operator-facing half of the fix: it must never
    // tell them the chain is untouched, because a receipt-fetch timeout leaves a
    // tx in flight.
    assert!(
        !stderr.contains("on-chain state is unchanged"),
        "resume guidance must not assert the chain is untouched: {stderr}"
    );

    // On-chain truth: the bond really did not move.
    assert_eq!(
        chain.active_bond(operator).await?,
        bond_before,
        "the paused bond must not have applied"
    );

    Ok(())
}

/// `decdn setup --json`, without asserting success — the whole point is the
/// non-zero exit. `setup` is a top-level command, not a `node` subcommand.
async fn run_setup_json(node: &NodeFixture) -> anyhow::Result<Output> {
    Command::from(decdn_command(node.data_dir(), KEYSTORE_PASSWORD)?)
        .arg("setup")
        .arg("--mbps")
        .arg(TARGET_MBPS.to_string())
        .arg("--region")
        .arg("US")
        .arg("--yes")
        .arg("--accept-terms")
        .arg("--json")
        .arg("--config")
        .arg(node.config_path())
        // Keep the outer timeout authoritative if the CLI wedges on a receipt.
        .kill_on_drop(true)
        .output()
        .await
        .context("spawn decdn setup")
}
