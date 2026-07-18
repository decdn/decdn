//! Live anvil-backed e2e for the `decdn publish` control-plane binary paths
//! (issue #1073, follow-up to #1029). Drives the three on-chain `decdn publish`
//! subcommands end-to-end against the deployed `PublisherRegistry` /
//! `OriginAssignment` and asserts the writes landed:
//!
//! 1. `publish namespace create` mints a namespace owned by the signer; the id
//!    is parsed from the `--json` receipt and confirmed via `ownerOf`.
//! 2. `publish claim <hash> --namespace <id>` records the claim, so
//!    `namespaceOf(hash)` now lists exactly that namespace.
//! 3. `publish assign <id> <operator>` is **propose-only** (it calls
//!    `proposeAssignment`, not `activateAssignment`), so `getPendingAssignment`
//!    reflects the proposed set with a non-zero timelock `readyAt` while
//!    `getOrigins` stays empty until governance ratifies.
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

use std::path::PathBuf;
use std::time::Duration;

use alloy::primitives::{B256, U256};
use anyhow::Context;
use decdn_e2e::bindings::{OriginAssignment, PublisherRegistry};
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::node::{KEYSTORE_PASSWORD, NodeFixture};
use tokio::process::Command;

/// Defense-in-depth overall ceiling so an unbounded await fails fast with a
/// clear message rather than squatting the runner. Cleanup (anvil kill, daemon
/// kill) runs on drop even on timeout. Matches the smoke / cli-fetch budgets.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(780);

#[tokio::test(flavor = "multi_thread")]
async fn publish_namespace_claim_assign_land_on_chain() -> anyhow::Result<()> {
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
    // signs the three `decdn publish` writes (and therefore owns the namespace it
    // creates), and the active bonded node the `assign` proposes as an authorized
    // origin — `proposeAssignment` gates every operator on `CapacityBond.isActive`,
    // which onboarding satisfies. The node fixture also renders the `node.toml`
    // the CLI reads its `[blockchain]` coordinates + keystore from.
    let serve_blob = b"decdn publish e2e fixture blob (#1073)".to_vec();
    let (node, _hash) = NodeFixture::launch(&chain, "US", &serve_blob).await?;
    let config = node.config_path();
    let operator = node.operator_addr();

    let registry = PublisherRegistry::new(chain.addrs().publisher_registry, chain.admin());
    let assignment = OriginAssignment::new(chain.addrs().origin_assignment, chain.admin());

    // ---- 1. `publish namespace create` mints a namespace owned by the signer.
    let create = run_publish(config, &["namespace", "create", "--json"]).await?;
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

    // ---- 2. `publish claim <hash> --namespace <id>` records the claim, so
    // `namespaceOf(hash)` now lists exactly this namespace. Any 32 bytes is a
    // well-formed claim key; the `0x…` 64-hex form maps verbatim to the on-chain
    // `bytes32` (`parse_hash` → `blake3::Hash::from_hex`).
    let claim_hash = B256::repeat_byte(0x42);
    run_publish(
        config,
        &[
            "claim",
            &format!("{claim_hash:#x}"),
            "--namespace",
            &namespace_id.to_string(),
        ],
    )
    .await?;
    assert_eq!(
        registry.namespaceOf(claim_hash).call().await?,
        vec![U256::from(namespace_id)],
        "namespaceOf(hash) must list exactly the claiming namespace",
    );

    // ---- 3. `publish assign <id> <operator>` is propose-only: it calls
    // `proposeAssignment`, not `activateAssignment`. So the *pending* proposal
    // carries the proposed set + a non-zero timelock deadline, while the active
    // (`getOrigins`) set stays empty until governance ratifies.
    run_publish(
        config,
        &[
            "assign",
            &namespace_id.to_string(),
            &format!("{operator:#x}"),
        ],
    )
    .await?;
    let pending = assignment
        .getPendingAssignment(U256::from(namespace_id))
        .call()
        .await?;
    assert_eq!(
        pending.operators,
        vec![operator],
        "getPendingAssignment must reflect the proposed operator set",
    );
    assert!(
        pending.readyAt != U256::ZERO,
        "a pending proposal must carry a non-zero timelock readyAt, got 0",
    );
    assert!(
        assignment
            .getOrigins(U256::from(namespace_id))
            .call()
            .await?
            .is_empty(),
        "getOrigins must stay empty until governance activates the proposal",
    );

    Ok(())
}

/// Run `decdn publish <args…> --config <config>` against the node fixture's
/// rendered config + keystore, asserting a clean exit, and return the captured
/// output. The `[blockchain]` coordinates all come from the config; the keystore
/// password comes from `DECDN_KEYSTORE_PASSWORD` (mirrors `slash_appeal.rs`).
async fn run_publish(
    config: &std::path::Path,
    args: &[&str],
) -> anyhow::Result<std::process::Output> {
    let out = Command::new(decdn_cli_bin()?)
        .arg("publish")
        .args(args)
        .arg("--config")
        .arg(config)
        .env("DECDN_KEYSTORE_PASSWORD", KEYSTORE_PASSWORD)
        .env(
            "RUST_LOG",
            std::env::var("DECDN_NODE_LOG").unwrap_or_else(|_| "warn".into()),
        )
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

/// Locate the built `decdn` CLI binary relative to the test executable
/// (`target/<profile>/decdn`), falling back to `DECDN_CLI_BIN`. Mirrors the
/// daemon-binary lookup in the node fixture / `slash_appeal.rs`.
fn decdn_cli_bin() -> anyhow::Result<PathBuf> {
    if let Some(p) = std::env::var_os("DECDN_CLI_BIN") {
        return Ok(PathBuf::from(p));
    }
    let exe = std::env::current_exe().context("current_exe")?;
    let profile_dir = exe
        .parent()
        .and_then(|deps| deps.parent())
        .context("resolve target profile dir")?;
    let bin = profile_dir.join(if cfg!(windows) { "decdn.exe" } else { "decdn" });
    anyhow::ensure!(
        bin.exists(),
        "decdn binary not found at {}; run `cargo build -p decdn-cli` first (or set DECDN_CLI_BIN)",
        bin.display()
    );
    Ok(bin)
}
