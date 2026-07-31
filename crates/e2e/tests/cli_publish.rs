//! Live anvil-backed e2e for the `decdn publish` control-plane binary paths
//! (issue #1073, follow-up to #1029). Drives the on-chain `decdn publish`
//! subcommands end-to-end against the deployed `PublisherRegistry` /
//! `OriginAssignment` and asserts the writes landed:
//!
//! 1. `publish namespace create` mints a namespace owned by the signer; the id
//!    is parsed from the `--json` receipt and confirmed via `ownerOf`.
//! 2. `publish assign <id> <operator>` **fails closed while the publisher is
//!    unvetted** — origin seating is gated on the wallet, not on a per-set
//!    governance vote.
//! 3. `publish request-vetting` queues the one governance-gated step, leaving a
//!    non-zero `getPendingVetting` deadline; the fixture then warps past the
//!    timelock and grants it as the governance Timelock.
//! 4. `publish assign <id> <operator>` now seats the origin **instantly** —
//!    `getOrigins` reflects it with no further governance action — and
//!    `publish revoke` unseats it just as immediately.
//!
//! There is no per-hash on-chain claim (ADR 002 § Hash-to-namespace
//! association) — content is bound to a namespace off-chain at fetch time.
//!
//! This covers the on-chain submit path the `crates/cli` `publish.rs` unit tests
//! intentionally skip — they cover output formatting only, matching the
//! `decdn node register` precedent.
//!
//! The CLI-binary lookup + config/keystore wiring mirror `slash_appeal.rs`'s
//! `decdn appeal slash` driver: the `decdn publish` binary reads its
//! `[blockchain]` coordinates (rpc url, chain id, `PublisherRegistry` /
//! `OriginAssignment` addresses, keystore path) from the node fixture's rendered
//! `node.toml`, with the keystore password in `DECDN_KEYSTORE_PASSWORD`.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and built `decdn-node` + `decdn` binaries:
//!
//! ```bash
//! cargo build -p decdn-node -p decdn-cli
//! cargo nextest run -p decdn-e2e --features anvil-e2e cli_publish
//! ```

#![cfg(feature = "anvil-e2e")]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units
)]

use std::time::Duration;

use alloy::primitives::U256;
use anyhow::Context;
use decdn_e2e::bindings::{OriginAssignment, PublisherRegistry};
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::decdn_command;
use decdn_e2e::node::{KEYSTORE_PASSWORD, NodeFixture};
use tokio::process::Command;

/// Defense-in-depth overall ceiling so an unbounded await fails fast with a
/// clear message rather than squatting the runner. Cleanup (anvil kill, daemon
/// kill) runs on drop even on timeout. Matches the smoke / cli-fetch budgets.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(780);

#[tokio::test(flavor = "multi_thread")]
async fn publish_namespace_and_assign_land_on_chain() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cli publish e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    // Surface daemon/background-task logs on failure (nextest captures stderr).
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    // A single onboarded operator plays both roles: the publisher whose keystore
    // signs the `decdn publish` writes (and therefore owns the namespace it
    // creates), and the active bonded node the `assign` seats as an authorized
    // origin — `addOrigin` gates every operator on `CapacityBond.isActive`,
    // which onboarding satisfies. The node fixture also renders the `node.toml`
    // the CLI reads its `[blockchain]` coordinates + keystore from.
    let serve_blob = b"decdn publish e2e fixture blob (#1073)".to_vec();
    let (node, _hash) = NodeFixture::launch(&chain, "US", &serve_blob).await?;
    let operator = node.operator_addr();

    let registry = PublisherRegistry::new(chain.addrs().publisher_registry, chain.admin());
    let assignment = OriginAssignment::new(chain.addrs().origin_assignment, chain.admin());

    // ---- 1. `publish namespace create` mints a namespace owned by the signer.
    let create = run_publish(&node, &["namespace", "create", "--json"]).await?;
    let namespace_id = parse_namespace_id(&create.stdout)?;
    assert!(
        namespace_id >= 1,
        "namespace id must be >= 1 (0 is reserved)"
    );
    assert_eq!(
        registry.ownerOf(U256::from(namespace_id)).call().await?,
        operator,
        "the CLI signer must own the namespace it created on-chain",
    );

    // ---- 2. Seating is gated on the publisher WALLET, so `assign` fails closed
    // until governance vets it. The revert is the contract's `PublisherNotVetted`.
    let denied = run_publish_expect_failure(
        &node,
        &[
            "assign",
            &namespace_id.to_string(),
            &format!("{operator:#x}"),
        ],
    )
    .await?;
    assert!(
        assignment
            .getOrigins(U256::from(namespace_id))
            .call()
            .await?
            .is_empty(),
        "an unvetted publisher must seat no origins, got {denied}",
    );
    assert!(
        !assignment.isVettedPublisher(operator).call().await?,
        "the publisher must start unvetted",
    );

    // ---- 3. `publish request-vetting` queues the single governance-gated step.
    let requested = run_publish(&node, &["request-vetting", "--json"]).await?;
    let ready_at = parse_ready_at(&requested.stdout)?;
    assert!(
        ready_at > 0,
        "request-vetting must report a timelock deadline"
    );
    assert_eq!(
        assignment.getPendingVetting(operator).call().await?,
        U256::from(ready_at),
        "getPendingVetting must match the readyAt the CLI reported",
    );
    // Warp past the timelock and grant as the governance Timelock — the cold
    // path in full, rather than the `setPublisherVetted` override.
    chain.grant_vetting_after_timelock(operator).await?;
    assert!(
        assignment.isVettedPublisher(operator).call().await?,
        "grantVetting must vet the publisher",
    );

    // ---- 4/5. Seat then unseat, both instant.
    seat_then_unseat(&node, &assignment, namespace_id, operator).await
}

/// The self-serve half of the journey: a vetted publisher seats an origin and
/// unseats it again, each in one transaction with no governance step in
/// between. Split out of [`run`] to keep either function readable.
async fn seat_then_unseat<P: alloy::providers::Provider>(
    node: &NodeFixture,
    assignment: &OriginAssignment::OriginAssignmentInstance<P>,
    namespace_id: u64,
    operator: alloy::primitives::Address,
) -> anyhow::Result<()> {
    // `assign` now seats the origin instantly: no pending state, no second
    // governance action — `getOrigins` reflects it immediately.
    run_publish(
        node,
        &[
            "assign",
            &namespace_id.to_string(),
            &format!("{operator:#x}"),
        ],
    )
    .await?;
    assert_eq!(
        assignment
            .getOrigins(U256::from(namespace_id))
            .call()
            .await?,
        vec![operator],
        "a vetted publisher's addOrigin must seat the operator in the same tx",
    );
    assert!(
        assignment
            .isAuthorizedOrigin(U256::from(namespace_id), operator)
            .call()
            .await?,
        "the seated operator must be an authorized origin",
    );

    // `publish revoke` unseats it, just as immediately.
    run_publish(
        node,
        &[
            "revoke",
            &namespace_id.to_string(),
            &format!("{operator:#x}"),
        ],
    )
    .await?;
    assert!(
        assignment
            .getOrigins(U256::from(namespace_id))
            .call()
            .await?
            .is_empty(),
        "removeOrigin must unseat the operator in the same tx",
    );

    Ok(())
}

/// Run `decdn publish <args…> --config <config>` against the node fixture's
/// rendered config + keystore, asserting a clean exit, and return the captured
/// output. The `[blockchain]` coordinates all come from the config; the keystore
/// password and the isolated `HOME` come from `decdn_command` (mirrors
/// `slash_appeal.rs`).
async fn run_publish(node: &NodeFixture, args: &[&str]) -> anyhow::Result<std::process::Output> {
    let out = Command::from(decdn_command(node.data_dir(), KEYSTORE_PASSWORD)?)
        .arg("publish")
        .args(args)
        .arg("--config")
        .arg(node.config_path())
        // Make the outer `tokio::time::timeout` authoritative even if the CLI
        // wedges waiting for a receipt: dropping this future terminates the
        // child instead of leaving it orphaned in the test runner.
        .kill_on_drop(true)
        .output()
        .await
        .context("spawn decdn publish")?;
    anyhow::ensure!(
        out.status.success(),
        "`decdn publish {}` exited non-zero: {}\nstdout: {}\nstderr: {}",
        args.join(" "),
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    Ok(out)
}

/// Run `decdn publish <args…>` expecting a NON-zero exit — the guard-rail half
/// of [`run_publish`]. Returns the combined output so the caller can quote it.
/// A clean exit is the failure here: it would mean the write landed.
async fn run_publish_expect_failure(node: &NodeFixture, args: &[&str]) -> anyhow::Result<String> {
    let out = Command::from(decdn_command(node.data_dir(), KEYSTORE_PASSWORD)?)
        .arg("publish")
        .args(args)
        .arg("--config")
        .arg(node.config_path())
        .kill_on_drop(true)
        .output()
        .await
        .context("spawn decdn publish")?;
    let rendered = format!(
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    anyhow::ensure!(
        !out.status.success(),
        "`decdn publish {}` unexpectedly succeeded\n{rendered}",
        args.join(" "),
    );
    Ok(rendered)
}

/// Parse `ready_at` from a `decdn publish request-vetting --json` receipt, the
/// same last-non-empty-line contract as [`parse_namespace_id`].
fn parse_ready_at(stdout: &[u8]) -> anyhow::Result<u64> {
    let text = String::from_utf8_lossy(stdout);
    let line = text
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .context("request-vetting produced no stdout")?;
    let v: serde_json::Value =
        serde_json::from_str(line).context("parse request-vetting JSON receipt")?;
    anyhow::ensure!(
        v["status"] == serde_json::json!("vetting_requested"),
        "request-vetting was not submitted (dry run?): {v}",
    );
    v["ready_at"]
        .as_u64()
        .context("request-vetting receipt missing a numeric ready_at")
}

/// Parse the `namespace_id` from a `decdn publish namespace create --json`
/// receipt. The receipt is a single JSON object; take the last non-empty stdout
/// line defensively (the `bundle create --json` CLI tests do the same).
fn parse_namespace_id(stdout: &[u8]) -> anyhow::Result<u64> {
    let text = String::from_utf8_lossy(stdout);
    let line = text
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .context("namespace-create produced no stdout")?;
    let v: serde_json::Value =
        serde_json::from_str(line).context("parse namespace-create JSON receipt")?;
    anyhow::ensure!(
        v["submitted"] == serde_json::json!(true),
        "namespace create was not submitted (dry run?): {v}",
    );
    v["namespace_id"]
        .as_u64()
        .context("namespace-create receipt missing a numeric namespace_id")
}
