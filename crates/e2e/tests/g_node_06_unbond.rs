//! G-NODE-06: `decdn node unbond` — capacity reduction and the unbonding
//! window, end to end against a real `CapacityBond` (#1033).
//!
//! Drives the production CLI binary through all three phases of the window —
//! request, maturing, withdraw — and asserts the on-chain consequences the
//! command promises the operator, in particular that a request in flight makes
//! the node INACTIVE for the whole window (`isActive` has "no unbonding
//! request" as a conjunct, independent of the remaining bond).
//!
//! The CLI wiring mirrors `cli_publish.rs`: a `NodeFixture` supplies the
//! rendered `node.toml` (`[blockchain]` coordinates + keystore path) and the
//! isolated `HOME`, so no invocation reaches a developer's real `~/.decdn`.

#![cfg(feature = "anvil-e2e")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units,
    clippy::too_many_lines,
    clippy::cognitive_complexity
)]

use std::process::Output;
use std::time::Duration;

use alloy::primitives::U256;
use anyhow::Context;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::decdn_command;
use decdn_e2e::node::{KEYSTORE_PASSWORD, NodeFixture};
use tokio::process::Command;

/// Matches the CLI-journey budgets (anvil launch + forge deploy dominate).
const OVERALL_TIMEOUT: Duration = Duration::from_secs(780);

/// Tier the operator bonds up to before reducing. Both tiers must sit on the
/// part of the curve where `bondRequired` dominates `minBond`, or the two
/// targets collapse to the same `max(minBond, …)` and there is nothing to
/// release: with the deployed `k = 12.6`, `α = 1.2` and `minBond = 50_000`
/// TOKEN, the curve only overtakes the floor around 1000 Mbps. Both are inside
/// the default `[minCapacityMbps, maxCapacityMbps]` band of `[10, 200_000]`.
/// The assertion below re-derives this from chain state rather than trusting
/// the arithmetic, so a governance retune of the curve fails loudly here.
const START_MBPS: u64 = 5_000;
/// Tier the operator reduces to. Must be below `START_MBPS`.
const REDUCED_MBPS: u64 = 2_000;

#[tokio::test(flavor = "multi_thread")]
async fn unbond_reduces_capacity_and_returns_token_after_the_window() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("G-NODE-06 exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let (node, _hash) = NodeFixture::launch(&chain, "US", b"g-node-06 fixture blob").await?;
    let operator = node.operator_addr();

    // ---- 0. Declare up to START_MBPS so there is a tier to reduce from.
    // `onboard_operator` bonds only `minBond` and never declares, so fund the
    // curve shortfall first — `decdn node bond` bonds the shortfall, it does
    // not mint.
    let start_target = chain
        .min_bond()
        .await?
        .max(chain.bond_required(START_MBPS).await?);
    chain.transfer_token(operator, start_target).await?;
    run_node_cli(&node, &["bond", "--mbps", &START_MBPS.to_string()]).await?;
    assert_eq!(chain.declared_mbps(operator).await?, START_MBPS);
    assert_eq!(chain.active_bond(operator).await?, start_target);
    assert!(chain.is_active(operator).await?, "bonded + registered");

    // ---- 1. `--to-mbps` declares down, then starts the window.
    let reduced_target = chain
        .min_bond()
        .await?
        .max(chain.bond_required(REDUCED_MBPS).await?);
    let expected_release = start_target - reduced_target;
    assert!(
        expected_release > U256::ZERO,
        "the fixture curve must make {START_MBPS} Mbps cost more than {REDUCED_MBPS} Mbps; \
         otherwise this journey asserts nothing"
    );

    let out = run_node_cli(
        &node,
        &[
            "unbond",
            "--to-mbps",
            &REDUCED_MBPS.to_string(),
            "--yes",
            "--json",
        ],
    )
    .await?;
    let receipt = last_json_line(&out)?;
    assert_eq!(json_str(&receipt, "phase"), Some("request"));
    assert_eq!(
        json_str(&receipt, "release_base"),
        Some(expected_release.to_string().as_str())
    );

    assert_eq!(
        chain.declared_mbps(operator).await?,
        REDUCED_MBPS,
        "declareMbps must land before requestUnbond, or the curve check rejects it"
    );
    assert_eq!(chain.active_bond(operator).await?, reduced_target);

    let (pending, unlock_at) = chain.unbonding_of(operator).await?;
    assert_eq!(pending, expected_release);
    let period = chain.unbonding_period().await?;
    let head = chain.head_timestamp().await?;
    assert!(
        unlock_at > head && unlock_at <= head + period,
        "unlock_at ({unlock_at}) must be one unbonding period ({period}s) out from {head}"
    );

    // The operator-facing surprise the command warns about: the node leaves the
    // active set for the whole window even though the retained bond still
    // covers minBond and the reduced tier.
    assert!(
        reduced_target >= chain.min_bond().await?,
        "retained bond still covers minBond"
    );
    assert!(
        !chain.is_active(operator).await?,
        "a request in flight must make the node inactive for the whole window"
    );

    // ---- 2. Re-running while it matures submits nothing and exits non-zero.
    let waiting = run_node_cli_raw(&node, &["unbond", "--yes", "--json"]).await?;
    assert!(
        !waiting.status.success(),
        "a maturing request must exit non-zero so a retry loop can tell it apart from a withdrawal"
    );
    let receipt = last_json_line(&waiting)?;
    assert_eq!(json_str(&receipt, "phase"), Some("waiting"));
    assert_eq!(
        chain.unbonding_of(operator).await?.0,
        expected_release,
        "the waiting phase must not touch chain state"
    );

    // ---- 3. An amount flag while a request is in flight is an error, not a
    // silent no-op — `requestUnbond` would revert with UnbondingInProgress.
    let conflict = run_node_cli_raw(&node, &["unbond", "--all", "--yes"]).await?;
    assert!(!conflict.status.success());
    assert!(
        String::from_utf8_lossy(&conflict.stderr).contains("already in flight"),
        "stderr must name the pending request: {}",
        String::from_utf8_lossy(&conflict.stderr)
    );

    // ---- 4. After the window, the same command withdraws.
    let before = chain.token_balance(operator).await?;
    decdn_e2e::time::advance_to(chain.admin(), unlock_at).await?;
    let out = run_node_cli(&node, &["unbond", "--yes", "--json"]).await?;
    let receipt = last_json_line(&out)?;
    assert_eq!(json_str(&receipt, "phase"), Some("withdraw"));
    assert_eq!(
        json_str(&receipt, "withdrawn_base"),
        Some(expected_release.to_string().as_str())
    );

    assert_eq!(
        chain.token_balance(operator).await? - before,
        expected_release,
        "the matured request must return exactly the released amount"
    );
    assert_eq!(
        chain.unbonding_of(operator).await?.0,
        U256::ZERO,
        "unbond() clears the request"
    );
    assert!(
        chain.is_active(operator).await?,
        "with the request cleared and the bond above minBond, the node is active again"
    );

    // ---- 5. Negative: an --amount below the curve is rejected before any
    // transaction, with the remediation the operator needs.
    let over = chain.active_bond(operator).await? - chain.bond_required(REDUCED_MBPS).await?
        + U256::from(1u64);
    let rejected =
        run_node_cli_raw(&node, &["unbond", "--amount", &over.to_string(), "--yes"]).await?;
    assert!(!rejected.status.success());
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        stderr.contains("BondBelowCurve") && stderr.contains("--to-mbps"),
        "stderr must name the revert it avoided and how to fix it: {stderr}"
    );
    assert_eq!(
        chain.unbonding_of(operator).await?.0,
        U256::ZERO,
        "a rejected plan must submit nothing"
    );

    Ok(())
}

/// Run `decdn node <args…>` against the fixture's config + keystore, asserting
/// a clean exit. Mirrors `cli_publish.rs::run_publish`.
async fn run_node_cli(node: &NodeFixture, args: &[&str]) -> anyhow::Result<Output> {
    let out = run_node_cli_raw(node, args).await?;
    anyhow::ensure!(
        out.status.success(),
        "`decdn node {}` exited non-zero: {}\nstdout: {}\nstderr: {}",
        args.join(" "),
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    Ok(out)
}

/// [`run_node_cli`] without the success assertion, for the cases whose whole
/// point is a non-zero exit.
async fn run_node_cli_raw(node: &NodeFixture, args: &[&str]) -> anyhow::Result<Output> {
    Command::from(decdn_command(node.data_dir(), KEYSTORE_PASSWORD)?)
        .arg("node")
        .args(args)
        .arg("--config")
        .arg(node.config_path())
        // Keep the outer timeout authoritative if the CLI wedges on a receipt.
        .kill_on_drop(true)
        .output()
        .await
        .context("spawn decdn node")
}

/// Parse the last non-empty stdout line as JSON — the `--json` receipt shape
/// the other CLI journeys assert on.
fn last_json_line(out: &Output) -> anyhow::Result<serde_json::Value> {
    let text = String::from_utf8_lossy(&out.stdout);
    let line = text
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .context("command produced no stdout")?;
    serde_json::from_str(line).with_context(|| format!("parse JSON receipt: {line}"))
}

fn json_str<'a>(v: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    v.get(key).and_then(serde_json::Value::as_str)
}
