//! The node's boot preflight identifies the configured `PaymentPool` by
//! calling `getRateBounds()` (`check_deployment_preflight` in
//! `crates/node/src/runtime/mod.rs`). The probe only works while no other
//! contract answers that selector: a node configured with such a contract's
//! address passes the preflight, binds its lane store to the wrong address,
//! and drops its seller lane state.
//!
//! `deployment_preflight_guard` boots a node against every deployed contract,
//! but a contract the e2e deploy leaves undeployed (the dormant
//! `BuybackBurner`) is not on chain there, though a live deployment may deploy
//! it. This test needs no chain: it reads the method identifiers `forge build`
//! records for every contract compiled from `contracts/src`, and fails if any
//! contract other than `PaymentPool` exposes the probe's selector. It compares
//! 4-byte selectors, not names, so a differently named function that collides
//! with the selector fails it too. The selector comes from the node's own
//! `PaymentPool` binding, so the test checks the call the node actually makes.
//!
//! Gated behind the `anvil-e2e` feature (off by default), which marks the tests
//! that need Foundry. Requires `forge` on `PATH`:
//!
//! ```bash
//! cargo nextest run -p decdn-e2e --features anvil-e2e --test payment_pool_probe_selector
//! ```

#![cfg(feature = "anvil-e2e")]
// Test scaffolding legitimately uses unwrap/expect/panic; the workspace
// anti-panic policy targets runtime code.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::path::Path;

use alloy::sol_types::SolCall;
use anyhow::Context;
use decdn_incentive::payment_pool::PaymentPool;

/// The source file and contract name `forge build` records for the
/// `PaymentPool` artifact.
const PAYMENT_POOL: (&str, &str) = ("src/PaymentPool.sol", "PaymentPool");

/// Contracts the scan must find, so a change in the artifact layout cannot make
/// it pass on an empty or partial list. `FeeRouter` and `DecdnGovernor` share a
/// `PaymentPool` view, and `BuybackBurner` is the contract the e2e deploy leaves
/// undeployed.
const MUST_SCAN: [&str; 3] = ["FeeRouter", "DecdnGovernor", "BuybackBurner"];

#[tokio::test]
async fn only_payment_pool_exposes_the_preflight_probe_selector() -> anyhow::Result<()> {
    let probe = alloy::hex::encode(PaymentPool::getRateBoundsCall::SELECTOR);
    let contracts = decdn_e2e::chain::build_contracts().await?;
    let selectors = src_method_selectors(&contracts.join("out"))?;

    for name in MUST_SCAN {
        anyhow::ensure!(
            selectors.keys().any(|(_, scanned)| scanned == name),
            "the artifact scan found no {name}; scanned: {:?}",
            selectors.keys().collect::<Vec<_>>()
        );
    }
    let pool_key = (PAYMENT_POOL.0.to_string(), PAYMENT_POOL.1.to_string());
    anyhow::ensure!(
        selectors
            .get(&pool_key)
            .is_some_and(|ids| ids.contains(&probe)),
        "PaymentPool does not expose the probe selector 0x{probe} the node calls"
    );

    let colliding: Vec<_> = selectors
        .iter()
        .filter(|(key, ids)| **key != pool_key && ids.contains(&probe))
        .map(|(key, _)| key)
        .collect();
    anyhow::ensure!(
        colliding.is_empty(),
        "these contracts expose 0x{probe}, the selector of PaymentPool.getRateBounds() that \
         the node's deployment preflight uses as the PaymentPool identity probe; a node \
         configured with one of their addresses would pass the preflight and drop its \
         seller lane state: {colliding:?}"
    );
    Ok(())
}

/// Every contract `forge build` compiled from `contracts/src`, keyed by
/// `(source path, contract name)`, mapped to its 4-byte method selectors as
/// lowercase hex. Reads each artifact's `methodIdentifiers`, the same map
/// `forge inspect <Contract> methodIdentifiers` prints.
fn src_method_selectors(out: &Path) -> anyhow::Result<BTreeMap<(String, String), Vec<String>>> {
    let mut selectors = BTreeMap::new();
    for source_dir in std::fs::read_dir(out).with_context(|| format!("read {}", out.display()))? {
        let source_dir = source_dir?.path();
        // `out/` holds one directory per source file, plus `build-info/`.
        if source_dir.extension().is_none_or(|ext| ext != "sol") {
            continue;
        }
        for artifact in std::fs::read_dir(&source_dir)? {
            let path = artifact?.path();
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }
            let json: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)
                .with_context(|| format!("parse artifact {}", path.display()))?;
            // A contract's artifact names its source file and contract in the
            // compilation target. An artifact without one holds no contract (a
            // file of only free functions or constants), so it has no methods
            // to expose.
            let Some((source, name)) = json["metadata"]["settings"]["compilationTarget"]
                .as_object()
                .and_then(|target| target.iter().next())
                .and_then(|(source, name)| Some((source.clone(), name.as_str()?.to_string())))
            else {
                continue;
            };
            // Only `contracts/src` is in scope: `lib/` dependencies, tests and
            // scripts are not part of the deployment.
            if !source.starts_with("src/") {
                continue;
            }
            let ids = json["methodIdentifiers"]
                .as_object()
                .with_context(|| format!("artifact {} has no methodIdentifiers", path.display()))?
                .values()
                .filter_map(|id| id.as_str().map(str::to_ascii_lowercase))
                .collect();
            selectors.insert((source, name), ids);
        }
    }
    Ok(selectors)
}
