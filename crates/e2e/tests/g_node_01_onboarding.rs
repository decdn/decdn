//! G-NODE-01: the full node-onboarding sequence, bare → accepting delivery
//! (#1030).
//!
//! Starts where an operator actually starts — a provisioned data dir, a running
//! daemon, and nothing on chain — and drives the production `decdn setup` CLI
//! through `approve` → `bond` → `declareMbps` → `registerNode` until the same
//! process is selling bytes. Every other journey in this directory begins
//! already onboarded (`NodeFixture::launch` onboards before the daemon spawns),
//! so this is the only one that observes the bare state at all.
//!
//! # What this covers that nothing else does
//!
//! `cli_setup_partial` already drives `decdn setup`, but only into a revert;
//! `smoke` already proves an *already-bonded* node delivers. Neither runs the
//! command to a clean finish, and neither starts from nothing. This does both,
//! on **one process**: a daemon that is bare when it boots is selling by the
//! end of the test, with no restart in between.
//!
//! The no-restart property is the load-bearing one. It is what proves the
//! daemon's registry projection is live — that a `NodeRegistered` event reaches
//! a running node — rather than a value sampled once at bring-up. `decdn node
//! health`'s `registry_active` is the observable.
//!
//! # This journey asserts no serve-side enforcement, deliberately
//!
//! #1030's third negative asked for "daemon refuses paid delivery before
//! registration is confirmed on-chain". There is no such refusal, and there
//! should not be: a node deciding whether to honour *its own* registration
//! status is not a control an adversary is subject to — anyone who wants to
//! sell unregistered deletes the check and rebuilds.
//!
//! The real constraints are external and already exist. An unregistered
//! operator never called `declareMbps`, so `DecdnGovernor._getVotes` caps every
//! epoch's credited bytes at `declaredMbpsAtEpoch × …` = 0 and it accrues no
//! vote weight; and a peer choosing an upstream should not pull from a node it
//! cannot slash. See ADR 019 §Phase 4.
//!
//! # A correction to the issue text
//!
//! #1030 lists "under-bonded `bond()` reverts" as a negative. `bond()` has no
//! floor at all — `CapacityBond.bond` guards only `ZeroAmount`, so bonding one
//! wei below `minBond` **succeeds**. The floor is enforced downstream, at
//! `registerNode`. [`register_below_min_bond_reverts`] tests the guard that
//! actually exists rather than the one the issue names; asserting on `bond()`
//! would have been asserting on nothing.
//!
//! Gated behind the `anvil-e2e` feature. Requires `anvil` + `forge` on `PATH`
//! and BOTH binaries built:
//!
//! ```bash
//! cargo build -p decdn-node -p decdn-cli
//! cargo nextest run -p decdn-e2e --features anvil-e2e -E 'binary(g_node_01_onboarding)'
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

use std::process::Output;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_common::admin::AdminRpcClient;
use decdn_e2e::assert as e2e_assert;
use decdn_e2e::bindings::CapacityBond;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::{decdn_command, ensure_decdn_cli_built};
use decdn_e2e::client::{ClientFixture, DEPOSIT_MICRO_USDC};
use decdn_e2e::node::{KEYSTORE_PASSWORD, NodeFixture};
use decdn_e2e::poll;
use tokio::process::Command;

const MIB: usize = 1024 * 1024;

/// Standard journey tier (see [`decdn_e2e::timeout`] for the rule that picks
/// it). The longest wait here is the post-`setup` gate-open poll plus the
/// settlement poll, well inside the ~150s ceiling for the standard tier; anvil
/// launch + forge deploy dominate the runtime.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// Capacity tier `setup` declares. Inside the deployed
/// `[minCapacityMbps, maxCapacityMbps]` band of `[10, 200_000]`. Low enough
/// that `max(minBond, bondRequired(mbps))` resolves to `minBond` — this journey
/// is about the sequence, not the bond curve, and `g_node_06_unbond` already
/// covers the curve's crossover.
const TARGET_MBPS: u64 = 100;

#[tokio::test(flavor = "multi_thread")]
async fn onboarding_from_bare_reaches_paid_delivery() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_journey()))
        .await
        .context("G-NODE-01 onboarding journey exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    reason = "sequential on-chain journey: each step depends on the previous step's chain state, \
              so decomposing it would thread a state bundle through helpers without reducing the \
              journey's length or making it easier to follow"
)]
async fn run_journey() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();
    // Fail in milliseconds on a missing binary rather than after a minute of
    // anvil + forge.
    ensure_decdn_cli_built()?;

    let chain = ChainFixture::launch().await?;

    // 2 MiB at the fixture's `rate_per_mb = 10` is a 20 µUSDC claim, above the
    // rendered `redeem_threshold_micro_usdc = 10`. That is what makes the seller
    // actually redeem, so the on-chain served-bytes assertion at the end has
    // something to observe; a smaller blob silently never settles.
    let payload = vec![0x5Au8; 2 * MIB];
    let (node, hash) = NodeFixture::launch_bare(&chain, "US", &payload).await?;
    let operator = node.operator_addr();

    // ---- 1. Bare. The daemon is UP and healthy, and holds the blob, but
    // nothing about it exists on chain yet.
    //
    // Startup is deliberately not gated on registration: a daemon that refused
    // to boot unregistered could not be used to run the `decdn setup` that fixes
    // it. `registry_active` is the diagnostic that says so — it is what an
    // operator reads when their node is up and earning nothing.
    let admin = node.admin_client()?;
    let health = admin.health().await.context("admin health while bare")?;
    assert!(
        !health.registry_active,
        "a node that has never registered must report registry_active=false, got {health:?}"
    );
    assert!(
        !chain.is_registered(operator).await?,
        "nothing should be on chain yet"
    );
    assert!(
        !chain.is_active(operator).await?,
        "a bare operator cannot be active"
    );

    // No paid fetch is attempted here. A bare node WILL serve — see the module
    // doc — and pinning that either way would be asserting on a decision the
    // protocol deliberately does not delegate to the seller.
    let client = ClientFixture::new(&chain).await?;

    // ---- 2. Run the real `decdn setup` against the LIVE daemon.
    //
    // `launch_bare` funds gas but not TOKEN, so the funding pre-flight has a
    // genuine step to clear. Fund exactly the curve target and no more, so an
    // over-bond cannot mask a wrong target computation.
    let target_bond = chain
        .min_bond()
        .await?
        .max(chain.bond_required(TARGET_MBPS).await?);
    chain.transfer_token(operator, target_bond).await?;

    let out = run_setup_json(&node).await?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "`decdn setup` must succeed from bare.\nstdout: {stdout}\nstderr: {stderr}"
    );
    let summary = last_json_line(&out)?;

    // ---- 3. The summary is the operator's machine-readable record of an
    // irreversible sequence. Assert every leg landed, not merely that the exit
    // code was zero: `partial` is the flag a consumer branches on (#1355).
    assert_eq!(
        summary.get("partial").and_then(serde_json::Value::as_bool),
        Some(false),
        "a clean run must not be flagged partial: {summary}"
    );
    for gate in ["chain_id_ok", "clock_ok", "native_ok", "token_ok"] {
        assert_eq!(
            summary["preflight"]
                .get(gate)
                .and_then(serde_json::Value::as_bool),
            Some(true),
            "pre-flight gate `{gate}` must pass: {summary}"
        );
    }
    for tx in ["approve_tx", "bond_tx", "declare_tx"] {
        assert!(
            summary["bond"].get(tx).is_some_and(|v| !v.is_null()),
            "the bond phase must report a real `{tx}`: {summary}"
        );
    }
    assert_eq!(
        summary["register"]
            .get("submitted")
            .and_then(serde_json::Value::as_bool),
        Some(true),
        "registerNode must have been submitted: {summary}"
    );
    // The registration must bind the key this daemon is ACTUALLY serving under.
    // A `setup` that registered some other id would leave the node unslashable
    // and — since #1030 — permanently unable to sell, so this is the one field
    // worth cross-checking against the live process rather than against config.
    let live_node_id = node.current_node_id().await?;
    let live_hex = format!(
        "0x{}",
        alloy::primitives::hex::encode(live_node_id.as_bytes())
    );
    assert_eq!(
        summary["register"]
            .get("node_id")
            .and_then(serde_json::Value::as_str),
        Some(live_hex.as_str()),
        "setup must register the key the daemon is serving under: {summary}"
    );
    assert_eq!(
        summary["readiness"]
            .get("registry_active")
            .and_then(serde_json::Value::as_bool),
        Some(true),
        "setup's own read-back must see the operator active: {summary}"
    );

    // ---- 4. Cross-check on chain, independently of what the CLI reported.
    assert!(chain.is_active(operator).await?, "bonded + registered");
    assert_eq!(chain.declared_mbps(operator).await?, TARGET_MBPS);
    assert_eq!(chain.active_bond(operator).await?, target_bond);
    assert_eq!(
        chain.node_id_of(operator).await?,
        B256::from_slice(live_node_id.as_bytes()),
        "the on-chain binding must name the daemon's live key"
    );

    // ---- 5. The load-bearing assertion. No restart anywhere in this test: the
    // daemon has been the same process since step 1. `registry_active` flipping
    // proves the registration reached the running node's registry projection off
    // the `NodeRegistered` event, rather than being a value sampled once at
    // bring-up. That projection is what feeds DHT admission, so a stale one is a
    // real defect — this is the cheapest end-to-end proof that it is live.
    poll(Duration::from_secs(60), || async {
        let h = admin.health().await.context("admin health after setup")?;
        Ok(h.registry_active.then_some(()))
    })
    .await?
    .context(
        "the daemon never saw its own registration: `registry_active` stayed false after a \
         successful `decdn setup`, so its registry projection is stale until a restart",
    )?;

    // ---- 6. ADR 019 Phase 3 state sync, now that the node is servable.
    //
    // Rate floor (Step 3.1): the signed probe quote must be at or above the
    // on-chain `getRateBounds()` floor. `g_gov_02` owns the retune case; here it
    // is enough that the startup read produced a live clamp at all.
    let probe = client.probe(&node, hash).await?;
    assert!(probe.body.has_blob, "the node holds the blob");
    assert!(
        probe.body.rate_per_mb > 0,
        "a signed quote must carry a real rate: {:?}",
        probe.body
    );
    // Registry view (Step 3.3): the node counts itself among the active stakers
    // it will admit DHT records from. Blacklist sync (Step 3.2) needs no
    // separate assertion — the router is gated on the initial enumeration, so a
    // daemon answering QUIC at all has completed it.
    let status = admin.status().await.context("admin status")?;
    assert!(
        status.known_stakers >= 1,
        "the node must see at least itself in the active-staker set: {status:?}"
    );
    // The operator wallet the node runs under is legible from `status` — the
    // first-party answer, without grepping the daemon log for it.
    let expected_operator = node.operator_addr().to_string();
    assert_eq!(
        status.operator_address.as_deref(),
        Some(expected_operator.as_str()),
        "status must report the operator address the node bonded and registered under: {status:?}"
    );

    // ---- 7. Phase 4: a paid fetch delivers.
    let outcome = client.fetch(&chain, &node, hash, U256::ZERO).await?;
    assert_eq!(
        outcome.bytes, payload,
        "delivered bytes must match the blob"
    );

    // ---- 8. And it is real money: the daemon reports a lane on the pool we
    // paid through, and the pool's on-chain deposit matches what was opened.
    let expected_pid = outcome.pool_id;
    let snapshot = poll(Duration::from_secs(30), || async {
        let c = admin.lanes().await.context("admin lanes")?;
        Ok(c.lanes
            .into_iter()
            .find(|s| s.pool_id.parse::<B256>().is_ok_and(|id| id == expected_pid)))
    })
    .await?
    .with_context(|| format!("daemon never reported a lane on the paid pool {expected_pid}"))?;
    assert_eq!(
        snapshot.counterparty,
        client.address().to_string(),
        "reported lane signer must be the buyer"
    );
    let pool =
        e2e_assert::read_pool(chain.admin(), chain.addrs().payment_pool, expected_pid).await?;
    assert_eq!(pool.deposit, DEPOSIT_MICRO_USDC);

    // ---- 9. Settlement lands on-chain: the seller redeemed, so `FeeRouter`
    // accumulates paid bytes. Paid bytes are WIRE bytes (content + interleaved
    // bao proof nodes, ADR 038 §Payment metering), and whether the trailing
    // sub-threshold proof delta has settled by read time is a race — so bound it
    // on both sides rather than asserting either endpoint (the #1381 flake).
    let content_bytes = U256::from(payload.len());
    let wire_bytes = U256::from(
        decdn_bao_range::align_range(0, 0, u64::try_from(payload.len())?)
            .context("align whole-blob payload for its wire length")?
            .wire_len(),
    );
    let served = poll(Duration::from_secs(90), || async {
        let b = chain
            .served_bytes(operator)
            .await
            .context("read served bytes")?;
        Ok((b >= content_bytes).then_some(b))
    })
    .await?
    .context(
        "on-chain served-bytes never reached the content total (seller redeem did not land)",
    )?;
    assert!(
        served >= content_bytes && served <= wire_bytes,
        "on-chain served-bytes {served} must land in [{content_bytes}, {wire_bytes}]"
    );

    Ok(())
}

/// #1030's first negative, stated correctly.
///
/// The issue says "under-bonded `bond()` reverts". It does not:
/// `CapacityBond.bond` guards only `ZeroAmount`, so a bond one wei below
/// `minBond` is accepted and the TOKEN really moves. The floor lives in
/// `_checkRegistrationPreconditions`, which is why an operator can strand
/// capital in the contract and still not be registered — the failure mode this
/// pins.
///
/// `BondBelowMinimum` has no coverage anywhere else in the repo: the Solidity
/// suite covers only its sibling `BondBelowCurve`.
#[tokio::test(flavor = "multi_thread")]
async fn register_below_min_bond_reverts() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_under_bonded()))
        .await
        .context("G-NODE-01 under-bond negative exceeded the overall timeout")??;
    Ok(())
}

async fn run_under_bonded() -> anyhow::Result<()> {
    let chain = ChainFixture::launch().await?;
    let operator = PrivateKeySigner::random();
    let node_secret = iroh::SecretKey::generate();

    let min_bond = chain.min_bond().await?;
    let short = min_bond - U256::from(1u64);

    // `bond()` accepts it. Asserting this is half the point: the operator's
    // TOKEN is now in the contract, and they are still not registered.
    chain.fund_and_bond(&operator, short).await?;
    assert_eq!(
        chain.active_bond(operator.address()).await?,
        short,
        "bond() has no floor — the under-bonded stake really is held"
    );

    let err = chain
        .register_node_raw(
            &operator,
            &node_secret,
            "US",
            "/ip4/127.0.0.1/udp/1/quic-v1",
        )
        .await
        .err()
        .context("registerNode must reject a bond below minBond")?;
    e2e_assert::expect_revert_anyhow::<CapacityBond::BondBelowMinimum>(
        &err,
        "registerNode below minBond",
    )?;

    assert!(
        !chain.is_registered(operator.address()).await?,
        "the failed registration must leave no node record"
    );
    Ok(())
}

/// #1030's second negative: one node id, one owner, forever.
///
/// An impostor that could re-register someone else's `NodeId` under its own
/// address would redirect that identity's slashability — `SlashJudge` resolves
/// an accused node through the binding — so the victim's bond would stop
/// covering their traffic.
///
/// The impostor is bonded to `minBond` FIRST, deliberately.
/// `_checkRegistrationPreconditions` runs before `_checkBindingOneToOne`, so an
/// unfunded impostor reverts on the bond floor and never reaches the guard this
/// test is about — it would pass for entirely the wrong reason. For the same
/// reason both signatures are genuine (`register_node_raw` signs the EIP-712
/// binding with the impostor's own eth key and the ed25519 proof with the
/// victim's node key, which a test can do and a real attacker cannot): this must
/// prove the binding rule fired, not that a forged signature was caught.
///
/// `NodeIdAlreadyBound` has no coverage anywhere else in the repo.
#[tokio::test(flavor = "multi_thread")]
async fn node_id_reregistration_by_another_address_reverts() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_reregistration()))
        .await
        .context("G-NODE-01 re-registration negative exceeded the overall timeout")??;
    Ok(())
}

async fn run_reregistration() -> anyhow::Result<()> {
    let chain = ChainFixture::launch().await?;

    // The victim: a normally-onboarded operator holding a node id.
    let victim = PrivateKeySigner::random();
    let victim_secret = iroh::SecretKey::generate();
    chain
        .onboard_operator(
            &victim,
            &victim_secret,
            "US",
            "/ip4/127.0.0.1/udp/1/quic-v1",
        )
        .await?;
    let victim_node_id = B256::from_slice(victim_secret.public().as_bytes());
    assert_eq!(
        chain.operator_of_node_id(victim_node_id).await?,
        victim.address(),
        "the victim owns the id before the attempt"
    );

    // The impostor: separately bonded to minBond so it clears every
    // precondition and reaches the binding guard.
    let impostor = PrivateKeySigner::random();
    chain
        .fund_and_bond(&impostor, chain.min_bond().await?)
        .await?;

    let err = chain
        .register_node_raw(
            &impostor,
            &victim_secret,
            "US",
            "/ip4/127.0.0.1/udp/2/quic-v1",
        )
        .await
        .err()
        .context("registerNode must reject a node id another address already owns")?;
    e2e_assert::expect_revert_anyhow::<CapacityBond::NodeIdAlreadyBound>(
        &err,
        "registerNode with a bound node id",
    )?;

    // The binding is untouched — the attempt must not have partially applied.
    assert_eq!(
        chain.operator_of_node_id(victim_node_id).await?,
        victim.address(),
        "the victim must still own the id"
    );
    assert_eq!(
        chain.node_id_of(impostor.address()).await?,
        Address::ZERO.into_word(),
        "the impostor must be bound to nothing"
    );
    assert!(chain.is_active(victim.address()).await?, "victim unharmed");
    Ok(())
}

/// `decdn setup --json`, asserting nothing — the caller decides what success
/// means. `setup` is a top-level command, not a `node` subcommand.
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

fn last_json_line(out: &Output) -> anyhow::Result<serde_json::Value> {
    let text = String::from_utf8_lossy(&out.stdout);
    let line = text
        .lines()
        .rev()
        .find(|l| !l.trim().is_empty())
        .context("command produced no stdout")?;
    serde_json::from_str(line).with_context(|| format!("parse JSON summary: {line}"))
}
