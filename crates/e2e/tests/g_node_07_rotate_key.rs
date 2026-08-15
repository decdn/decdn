//! G-NODE-07: `decdn node rotate-key --key iroh` — operator key rotation end to
//! end against a real `CapacityBond` and a live daemon (#1034).
//!
//! Drives the production CLI binary through the runbook's § iroh node-key
//! rotation only, and asserts the three properties that make rotation safe to
//! perform on a node that is earning:
//!
//! 1. **Slashability moves atomically.** `bindNodeId` deletes the old
//!    `nodeId → address` mapping and writes the new one in one transaction, so
//!    the new id is slashable the instant the old one stops being. Both
//!    directions are asserted on `addressToNodeId` / `nodeIdToAddress` — the
//!    raw mappings `SlashJudge._checkRegistered` resolves through — rather than
//!    on `isActiveNode`, which folds in a liveness flag the slashing path
//!    deliberately ignores.
//! 2. **The retired key stops serving and the new one starts.** iroh
//!    authenticates the peer key in the QUIC handshake, so this is checked by
//!    dialing both ids at the same socket.
//! 3. **Money is untouched.** The bond, tier, `firstBondedAt`, and the open
//!    payment pool all key on the Ethereum address, which does not change — and a
//!    delivery paid for before the rotation still settles on-chain after it.
//!
//! **What the settlement leg does and does not pin.** The node redeems on its
//! own schedule once a claim passes `redeem_threshold_micro_usdc`, so whether
//! the pre-rotation voucher settles before or after the rebinding is a race this
//! journey does not try to win. Asserting the *outcome* after the rotation and
//! restart is what is meaningful and what is stable: the operator can still
//! redeem vouchers signed against the old identity. A second paid fetch after
//! the restart covers the other half — the new identity can be paid at all.
//!
//! Two more tests sit on the same fixtures. The second covers the negative the
//! issue names: a key swapped without a matching `bindNodeId` leaves the node
//! **unslashable**, which must be flagged rather than silently tolerated, and
//! must be repairable. The third walks `--key eth`, the other half of the
//! runbook — the Ethereum-address migration, which has no rebinding API and so
//! spends the whole unbonding window moving the identity, resetting
//! `firstBondedAt` and the node id on the way.

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
use anyhow::Context;
use decdn_common::admin::BindingStatus;
use decdn_common::identity;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::decdn_command;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::{KEYSTORE_PASSWORD, NodeFixture};
use decdn_e2e::poll;
use tokio::process::Command;

const MIB: usize = 1024 * 1024;

/// Standard journey tier (see [`decdn_e2e::timeout`] for the tier rule) for the
/// iroh-rotation and unbound-key legs; anvil launch + forge deploy dominate.
const OVERALL_TIMEOUT: Duration = decdn_e2e::timeout::STANDARD;

/// Payload size for the paid leg. 2 MiB at the fixture's 10 micro-USDC/MiB is
/// a 20 micro-USDC claim, above the fixture's 10 micro-USDC redeem threshold, so
/// the seller really does redeem and on-chain served-bytes really do advance —
/// the same sizing `smoke.rs` relies on.
const PAYLOAD_LEN: usize = 2 * MIB;

/// The Ethereum leg walks the full unbonding window (five sequential CLI
/// invocations plus the chain-clock advance), so it takes the heavy journey tier
/// rather than the standard one (see [`decdn_e2e::timeout`] for the tier rule).
const ETH_TIMEOUT: Duration = decdn_e2e::timeout::HEAVY;

/// Tier declared on the migrated-to address. The contract's `minCapacityMbps`
/// default, and far below the bond curve's crossover with `minBond` — so the
/// re-bond target resolves to `minBond` and the funding step stays small. The
/// test derives the actual figure from chain state rather than trusting this.
const REONBOARD_MBPS: u64 = 10;

#[tokio::test(flavor = "multi_thread")]
async fn rotate_key_rebinds_slashability_and_preserves_in_flight_vouchers() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_rotation()))
        .await
        .context("G-NODE-07 rotation leg exceeded the overall timeout")??;
    Ok(())
}

#[allow(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    reason = "sequential on-chain journey: each step depends on the previous step's chain state, \
              so decomposing it would thread a state bundle through helpers without reducing the \
              journey's length or making it easier to follow"
)]
async fn run_rotation() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let payload = vec![0x5Au8; PAYLOAD_LEN];
    let (node, hash) = NodeFixture::launch(&chain, "US", &payload).await?;
    let operator = node.operator_addr();
    let old_node_id = node.node_id();
    let old_id_b256 = B256::from_slice(old_node_id.as_bytes());

    // ---- 0. Baseline: the daemon agrees with the chain about who it is.
    assert_eq!(
        node.binding_status().await?,
        BindingStatus::Bound,
        "a freshly onboarded node must report its key as the bound one"
    );
    assert_eq!(chain.node_id_of(operator).await?, old_id_b256);

    // Economic state captured BEFORE the rotation, so the carry-over assertions
    // below compare against what was actually there rather than a re-read.
    let bond_before = chain.active_bond(operator).await?;
    let mbps_before = chain.declared_mbps(operator).await?;
    let first_bonded_before = chain.first_bonded_at(operator).await?;
    assert!(
        bond_before > U256::ZERO && first_bonded_before > 0,
        "fixture must have a real bond to prove rotation preserves it"
    );

    // ---- 1. Pay for a delivery under the OLD identity, so there is a voucher
    // in flight across the rotation.
    let client = ClientFixture::new(&chain).await?;
    let paid = client.fetch(&chain, &node, hash, U256::ZERO).await?;
    assert_eq!(paid.bytes, payload, "delivered bytes must match the blob");
    let pre_rotation_pool = paid.pool_id;

    // ---- 2a. `--dry-run` previews the rebinding and writes nothing. The branch
    // sits above the confirmation gate and the send, so a regression that moved
    // it below either would rebind for real on what the operator asked to
    // preview — and would replace `node.secret` while the daemon holds the old
    // key. Note the absent `--yes`: a dry run must not gate on confirmation.
    let key_path = identity::key_path(node.data_dir());
    let key_before = std::fs::read(&key_path).context("read node.secret before the dry run")?;
    let preview = run_node_cli(
        &node,
        &["rotate-key", "--key", "iroh", "--dry-run", "--json"],
    )
    .await?;
    let receipt = last_json_line(&preview)?;
    assert_eq!(json_str(&receipt, "key"), Some("iroh"));
    assert_eq!(json_bool(&receipt, "submitted"), Some(false));
    assert_eq!(json_bool(&receipt, "dry_run"), Some(true));
    assert_eq!(
        json_bool(&receipt, "preview_key"),
        Some(true),
        "a previewed generated key is discarded, and the receipt must say so"
    );
    assert!(
        json_str(&receipt, "ed25519_sig").is_some_and(|s| s.len() > 2),
        "a preview must produce a REAL ownership proof, not a placeholder: {receipt}"
    );
    assert_eq!(
        std::fs::read(&key_path).context("read node.secret after the dry run")?,
        key_before,
        "a preview must not touch the node key"
    );
    assert_eq!(
        chain.node_id_of(operator).await?,
        old_id_b256,
        "a preview must not move the on-chain binding"
    );

    // ---- 2b. The real rotation.
    let rotated = run_node_cli(&node, &["rotate-key", "--key", "iroh", "--yes", "--json"]).await?;
    let receipt = last_json_line(&rotated)?;
    assert_eq!(json_bool(&receipt, "submitted"), Some(true));
    assert_eq!(json_bool(&receipt, "dry_run"), Some(false));
    assert_eq!(
        json_str(&receipt, "old_node_id"),
        Some(format!("{old_id_b256:#x}").as_str()),
        "the receipt must name the id being retired"
    );
    let new_id_b256: B256 = json_str(&receipt, "new_node_id")
        .context("receipt carried no new_node_id")?
        .parse()
        .context("parse new_node_id")?;
    assert_ne!(new_id_b256, old_id_b256);
    assert!(
        json_str(&receipt, "bind_tx").is_some(),
        "a submitted rotation must name its transaction: {receipt}"
    );
    assert!(
        json_str(&receipt, "archived_key").is_some(),
        "the replaced key must be archived, not destroyed: {receipt}"
    );

    // ---- 3. Slashability moved, and moved completely. These are the two raw
    // mappings `SlashJudge` resolves through: the new id now has an operator to
    // charge, and the old id has none, so a challenge citing it reverts
    // `NodeNotRegistered`.
    assert_eq!(
        chain.node_id_of(operator).await?,
        new_id_b256,
        "the operator must now be bound to the new id — this is what makes it slashable"
    );
    assert_eq!(
        chain.operator_of_node_id(new_id_b256).await?,
        operator,
        "the new id must resolve back to the operator"
    );
    assert_eq!(
        chain.operator_of_node_id(old_id_b256).await?,
        Address::ZERO,
        "the retired id must resolve to nobody — otherwise it stays slashable and the operator \
         is exposed under a key they no longer control"
    );

    // ---- 4. Nothing economic moved. This is the runbook's carry-over table,
    // asserted rather than trusted.
    assert_eq!(
        chain.active_bond(operator).await?,
        bond_before,
        "the bond keys on the Ethereum address and must survive rotation"
    );
    assert_eq!(chain.declared_mbps(operator).await?, mbps_before);
    assert_eq!(
        chain.first_bonded_at(operator).await?,
        first_bonded_before,
        "firstBondedAt anchors the governance age ramp; an iroh rotation must not reset it"
    );

    // ---- 5. The daemon picks up the new identity on restart (the node key is
    // not hot-reloadable, so this is the only way it can), and agrees with the
    // chain about it again.
    node.restart().await?;
    let live_id = node.current_node_id().await?;
    assert_eq!(
        B256::from_slice(live_id.as_bytes()),
        new_id_b256,
        "the restarted daemon must serve under the newly bound key"
    );
    assert_eq!(
        node.binding_status().await?,
        BindingStatus::Bound,
        "after a completed rotation the daemon must report itself bound again"
    );

    // ---- 6. The retired key cannot serve; the new one can. iroh authenticates
    // the peer key during the handshake, so dialing the old id at the same
    // socket does not reach whoever is listening — it fails to connect.
    let probe = client
        .probe_node_id(live_id, node.bind_port(), hash)
        .await?;
    assert!(
        probe.body.has_blob,
        "the rotated node must still serve the blob it held under its old identity"
    );
    assert!(
        client
            .probe_node_id(old_node_id, node.bind_port(), hash)
            .await
            .is_err(),
        "the retired node id must no longer be reachable at this socket"
    );

    // ---- 7. Money still works across the rotation, in both directions.
    //
    // Backwards: the delivery paid for under the OLD identity settles. Paid
    // bytes are WIRE bytes (content plus interleaved bao proof nodes, ADR 038
    // § Payment metering), and the closing partial-group voucher may or may not
    // have landed by read time — so bound it on both sides the way `smoke.rs`
    // does rather than asserting an exact figure (#1381).
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
        "vouchers signed against the pre-rotation identity never settled on-chain — rotation \
         must not strand a channel the operator already earned on",
    )?;
    assert!(
        served >= content_bytes && served <= wire_bytes,
        "settled bytes {served} must land in [{content_bytes}, {wire_bytes}]"
    );
    let lane = decdn_e2e::assert::read_watermark(
        chain.admin(),
        chain.addrs().payment_pool,
        pre_rotation_pool,
        client.address(),
        operator,
    )
    .await?;
    assert!(
        lane.bytesDelivered > 0,
        "the pre-rotation pool's lane to this operator still carries the settled watermark — the \
         lane keys on the operator's Ethereum address, which rotation does not change"
    );

    // Forwards: a fresh pool against the NEW identity is payable, so the
    // rotated node is earning again and not merely reachable.
    let after = client.fetch(&chain, &node, hash, U256::ZERO).await?;
    assert_eq!(
        after.bytes, payload,
        "the rotated node must deliver over a freshly paid pool"
    );
    assert_ne!(
        after.pool_id, pre_rotation_pool,
        "the post-rotation fetch must be a genuinely new pool, not a replay of the old one"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unbound_local_key_is_flagged_and_repairable() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run_unbound()))
        .await
        .context("G-NODE-07 unbound leg exceeded the overall timeout")??;
    Ok(())
}

/// The negative case: a node key replaced WITHOUT the matching `bindNodeId`.
///
/// This is not merely an outage. `SlashJudge._checkRegistered` resolves an
/// accused node through the binding, so a node serving under an unbound key
/// cannot be slashed at all — it keeps earning while its bond is unreachable.
/// The state is unreachable *through* `decdn node rotate-key` (the key file is
/// committed only after the bind confirms), but reachable *around* it, by an
/// operator copying a key file or restoring the wrong backup. So the daemon has
/// to say so, and the command has to be able to repair it.
#[allow(
    clippy::cognitive_complexity,
    reason = "same sequential-journey shape as `run_rotation`: the steps are ordered by chain \
              state, not by branching, so splitting them would only move assertions away from \
              the state they describe"
)]
async fn run_unbound() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let (node, hash) = NodeFixture::launch(&chain, "US", b"g-node-07 unbound fixture").await?;
    let operator = node.operator_addr();
    let bound_id = chain.node_id_of(operator).await?;
    assert_eq!(bound_id, B256::from_slice(node.node_id().as_bytes()));

    // ---- 1. Swap the key on disk with no on-chain rebinding — exactly what an
    // operator who ran `decdn key-gen --force` in a registered data dir does.
    // Staged-then-committed so the replaced key is archived, matching what the
    // real path leaves behind.
    let mut staged =
        identity::stage_node_key(node.data_dir()).context("stage a hand-swapped node key")?;
    let orphan_id = B256::from_slice(staged.public().as_bytes());
    staged.commit().context("commit the hand-swapped key")?;
    assert_ne!(orphan_id, bound_id);

    // ---- 2. Restart, and the daemon must call it out. The chain has not moved:
    // the operator is still bound to the old id, so the key now being served is
    // bound to nobody and cannot be slashed.
    node.restart().await?;
    assert_eq!(
        B256::from_slice(node.current_node_id().await?.as_bytes()),
        orphan_id,
        "the restarted daemon picked up the hand-swapped key"
    );
    assert_eq!(
        node.binding_status().await?,
        BindingStatus::Mismatch,
        "a node serving an unbound key is UNSLASHABLE and must be flagged, not tolerated"
    );
    assert_eq!(
        chain.operator_of_node_id(orphan_id).await?,
        Address::ZERO,
        "the key being served resolves to no operator — nothing to slash"
    );
    assert_eq!(
        chain.node_id_of(operator).await?,
        bound_id,
        "the chain still points at the key the operator no longer has loaded"
    );

    // ---- 3. Repair. `--bind-existing` binds whatever is on disk rather than
    // generating a third key — the runbook's rollback lever, and the only one
    // that converges here. Nothing is archived, because nothing is replaced.
    let repaired = run_node_cli(
        &node,
        &[
            "rotate-key",
            "--key",
            "iroh",
            "--bind-existing",
            "--yes",
            "--json",
        ],
    )
    .await?;
    let receipt = last_json_line(&repaired)?;
    assert_eq!(json_bool(&receipt, "submitted"), Some(true));
    assert_eq!(
        json_bool(&receipt, "generated_key"),
        Some(false),
        "--bind-existing must bind the key on disk, not mint another one"
    );
    assert_eq!(
        json_str(&receipt, "new_node_id"),
        Some(format!("{orphan_id:#x}").as_str())
    );

    assert_eq!(
        chain.node_id_of(operator).await?,
        orphan_id,
        "the binding now names the key the daemon is actually serving"
    );
    node.restart().await?;
    assert_eq!(
        node.binding_status().await?,
        BindingStatus::Bound,
        "the repair must clear the flag"
    );

    // ---- 4. A second `--bind-existing` is refused rather than burning a
    // binding nonce on a no-op. The state is already correct, and a command that
    // reported success here would teach an operator that re-running is a way to
    // "make sure" — which on a nonce-consuming call it is not.
    let noop = run_node_cli_raw(
        &node,
        &[
            "rotate-key",
            "--key",
            "iroh",
            "--bind-existing",
            "--yes",
            "--json",
        ],
    )
    .await?;
    assert!(
        !noop.status.success(),
        "binding the already-bound key must fail, not silently succeed"
    );
    assert!(
        String::from_utf8_lossy(&noop.stderr).contains("already the one bound"),
        "the refusal must say why: {}",
        String::from_utf8_lossy(&noop.stderr)
    );

    // ---- 5. The repaired node serves under the key it is now bound to.
    let probe = client_probe(&chain, &node, hash).await?;
    assert!(
        probe.body.has_blob,
        "the repaired node must serve normally under its now-bound identity"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn eth_rotation_walks_the_unbonding_window() -> anyhow::Result<()> {
    tokio::time::timeout(ETH_TIMEOUT, Box::pin(run_eth_rotation()))
        .await
        .context("G-NODE-07 eth leg exceeded the overall timeout")??;
    Ok(())
}

/// The Ethereum-key path: no rebinding API exists for the on-chain address, so
/// the migration moves the whole identity across the unbonding window.
///
/// Each invocation reads chain state and performs the next step, so the journey
/// is written the way an operator actually runs it — the same command, four
/// times, with the chain clock advanced in between. What is asserted at each
/// step is the state transition the operator is paying for, plus the two costs
/// the runbook names and this path cannot avoid: `firstBondedAt` resets, and the
/// node id changes.
#[allow(
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    reason = "same sequential-journey shape as the other legs: the steps are ordered by chain \
              state, so splitting them would only move assertions away from the state they \
              describe"
)]
async fn run_eth_rotation() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let (node, _hash) = NodeFixture::launch(&chain, "DE", b"g-node-07 eth fixture").await?;
    let old_operator = node.operator_addr();
    let old_node_id = chain.node_id_of(old_operator).await?;
    let bond_amount = chain.active_bond(old_operator).await?;
    let first_bonded_before = chain.first_bonded_at(old_operator).await?;
    assert!(bond_amount > U256::ZERO && first_bonded_before > 0);

    // The new address, funded but not yet onboarded. Its keystore lives outside
    // the node's data dir so the two are never confusable — the migration reads
    // one and writes the other.
    let new_home = tempfile::tempdir().context("new keystore dir")?;
    // The keystore writer enforces the same `0o700` data-dir policy as
    // `node.secret`, and a umask of 022 leaves a fresh tempdir at 0o755 —
    // the same chmod `NodeFixture::launch_configured` does for its own dir.
    #[cfg(unix)]
    std::fs::set_permissions(
        new_home.path(),
        std::os::unix::fs::PermissionsExt::from_mode(0o700),
    )
    .context("chmod new keystore dir 0o700")?;
    let new_operator = decdn_incentive::eth_identity::generate_and_persist(
        new_home.path(),
        KEYSTORE_PASSWORD,
        false,
    )
    .context("generate the new eth keystore")?;
    let new_keystore = decdn_incentive::eth_identity::keystore_path(new_home.path());
    chain.fund_eth(new_operator, 100).await?;

    // ---- 0. A same-address `--new-keystore` is refused on the FIRST
    // invocation, before anything is submitted.
    //
    // This is the whole point of validating the flag ahead of the phase
    // branches: with the guard past the early returns, passing the
    // node's own keystore is accepted silently here and only refused at the
    // re-onboarding call — two weeks, a 14-day window and four transactions
    // later, with the tier already cleared and nothing gained. Asserting the
    // operator is still registered afterwards is what pins "refused *before*
    // `deregisterNode`" rather than merely "refused".
    let own_keystore = decdn_incentive::eth_identity::keystore_path(node.data_dir());
    let same_address = run_eth_cli_raw(
        &node,
        &["--new-keystore", &own_keystore.display().to_string()],
    )
    .await?;
    assert!(
        !same_address.status.success(),
        "migrating to the address being migrated away from must be refused"
    );
    assert!(
        String::from_utf8_lossy(&same_address.stderr).contains("SAME address"),
        "the refusal must name the mistake: {}",
        String::from_utf8_lossy(&same_address.stderr)
    );
    assert!(
        chain.is_registered(old_operator).await?,
        "the refusal must come BEFORE deregisterNode — nothing may have been submitted"
    );

    // ---- 1. Deregister. The fixture never declared a tier, so the phase
    // reports 0 — which is exactly why the re-onboarding step below has to be
    // told one explicitly.
    let out = run_eth_cli(&node, &[]).await?;
    let receipt = last_json_line(&out)?;
    assert_eq!(json_str(&receipt, "key"), Some("eth"));
    assert_eq!(json_str(&receipt, "phase"), Some("deregister"));
    assert!(json_str(&receipt, "deregister_tx").is_some(), "{receipt}");
    assert!(
        !chain.is_registered(old_operator).await?,
        "the old address must have left the active set"
    );
    assert_eq!(
        chain.active_bond(old_operator).await?,
        bond_amount,
        "deregistration must not move the bond — that is a separate operation"
    );

    // ---- 2. Start the window. The same command, no new flags: the phase came
    // from the chain, not from the operator remembering where they were.
    let out = run_eth_cli(&node, &[]).await?;
    let receipt = last_json_line(&out)?;
    assert_eq!(json_str(&receipt, "phase"), Some("request"));
    assert!(json_str(&receipt, "request_tx").is_some(), "{receipt}");
    let (pending, _unlock) = chain.unbonding_of(old_operator).await?;
    assert_eq!(pending, bond_amount, "the whole bond is now unbonding");

    // ---- 3. Maturing: nothing to submit, and the command must say so with a
    // NON-ZERO exit. A wrapper looping `until decdn node rotate-key; do sleep;
    // done` depends on that: a zero exit here would read as "migration done".
    let waiting = run_eth_cli_raw(&node, &[]).await?;
    assert!(
        !waiting.status.success(),
        "a maturing window must not report success"
    );
    let receipt = last_json_line(&waiting)?;
    assert_eq!(json_str(&receipt, "phase"), Some("waiting"));
    assert_eq!(json_bool(&receipt, "submitted"), Some(false));
    assert!(
        receipt
            .get("remaining_secs")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|s| s > 0),
        "the operator needs to know how long is left: {receipt}"
    );

    // ---- 4. Advance past the window and withdraw. `+2` rather than `+1`
    // because the comparison runs against the head block's timestamp, which the
    // withdrawing transaction advances again.
    let window = chain.unbonding_period().await?;
    chain.advance_time(window + 2).await?;
    let before = chain.token_balance(old_operator).await?;
    let out = run_eth_cli(&node, &[]).await?;
    let receipt = last_json_line(&out)?;
    assert_eq!(json_str(&receipt, "phase"), Some("withdraw"));
    assert!(json_str(&receipt, "withdraw_tx").is_some(), "{receipt}");
    assert_eq!(
        chain.token_balance(old_operator).await? - before,
        bond_amount,
        "every base unit came back to the OLD address"
    );
    assert_eq!(chain.active_bond(old_operator).await?, U256::ZERO);

    // ---- 5. Re-onboard on the new address. It needs its own TOKEN — the
    // command checks the balance but never moves funds between the operator's
    // addresses, which is the operator's decision to make.
    let target = chain
        .min_bond()
        .await?
        .max(chain.bond_required(REONBOARD_MBPS).await?);
    chain.transfer_token(new_operator, target).await?;

    let out = run_eth_cli(
        &node,
        &[
            "--new-keystore",
            &new_keystore.display().to_string(),
            "--mbps",
            &REONBOARD_MBPS.to_string(),
            "--accept-terms",
        ],
    )
    .await?;
    let receipt = last_json_line(&out)?;
    assert_eq!(json_str(&receipt, "phase"), Some("reonboard"));
    assert_eq!(
        json_str(&receipt, "new_operator"),
        Some(format!("{new_operator:#x}").as_str())
    );
    assert!(json_str(&receipt, "register_tx").is_some(), "{receipt}");

    assert!(
        chain.is_active(new_operator).await?,
        "the new address must be bonded and registered"
    );
    assert_eq!(chain.declared_mbps(new_operator).await?, REONBOARD_MBPS);

    // ---- 6. The two costs this path cannot avoid, asserted rather than
    // assumed.
    //
    // A fresh node id, because `deregisterNode` leaves `nodeIdToAddress`
    // pointing at the OLD address — so re-registering the original from a new
    // address would revert `NodeIdAlreadyBound`.
    let new_node_id = chain.node_id_of(new_operator).await?;
    assert_ne!(
        new_node_id, old_node_id,
        "the migration must register a fresh node id"
    );
    assert_eq!(
        json_str(&receipt, "new_node_id"),
        Some(format!("{new_node_id:#x}").as_str()),
        "the receipt must name the id it minted"
    );
    assert_eq!(
        chain.operator_of_node_id(old_node_id).await?,
        old_operator,
        "the ORIGINAL id is still held by the old address — that is why it cannot be reused, and \
         why carrying it across needs a `--key iroh` rebind on the old address first"
    );

    // And a reset age-ramp anchor: `firstBondedAt` is per-address and write-once,
    // so a new address necessarily starts over.
    let first_bonded_after = chain.first_bonded_at(new_operator).await?;
    assert!(
        first_bonded_after > first_bonded_before,
        "firstBondedAt must restart on the new address ({first_bonded_after} vs \
         {first_bonded_before}) — this is the governance age-ramp cost the runbook names"
    );

    // ---- 7. Re-running is a no-op rather than a second migration.
    let out = run_eth_cli(
        &node,
        &["--new-keystore", &new_keystore.display().to_string()],
    )
    .await?;
    let receipt = last_json_line(&out)?;
    assert_eq!(json_str(&receipt, "phase"), Some("complete"));
    assert_eq!(json_bool(&receipt, "submitted"), Some(false));

    Ok(())
}

/// Run `decdn node rotate-key --key eth --yes --json` with `extra` flags.
async fn run_eth_cli(node: &NodeFixture, extra: &[&str]) -> anyhow::Result<Output> {
    let out = run_eth_cli_raw(node, extra).await?;
    anyhow::ensure!(
        out.status.success(),
        "`decdn node rotate-key --key eth {}` exited non-zero: {}\nstdout: {}\nstderr: {}",
        extra.join(" "),
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    Ok(out)
}

/// [`run_eth_cli`] without the success assertion — the maturing phase exits
/// non-zero by design.
async fn run_eth_cli_raw(node: &NodeFixture, extra: &[&str]) -> anyhow::Result<Output> {
    let mut args = vec!["rotate-key", "--key", "eth", "--yes", "--json"];
    args.extend_from_slice(extra);
    run_node_cli_raw(node, &args).await
}

/// Probe `node` at its live identity through a throwaway client.
async fn client_probe(
    chain: &ChainFixture,
    node: &NodeFixture,
    hash: decdn_cache::Hash,
) -> anyhow::Result<decdn_protocol::ProbeResponse> {
    let client = ClientFixture::new(chain).await?;
    client
        .probe_node_id(node.current_node_id().await?, node.bind_port(), hash)
        .await
}

/// Run `decdn node <args…>` against the fixture's config + keystore, asserting
/// a clean exit. Mirrors `g_node_06_unbond.rs::run_node_cli`.
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

/// The `--json` receipt's flags are real booleans, not strings, so they need
/// their own accessor — `json_str` returns `None` for them and an
/// `assert_eq!(…, Some("true"))` against it would be vacuously wrong.
fn json_bool(v: &serde_json::Value, key: &str) -> Option<bool> {
    v.get(key).and_then(serde_json::Value::as_bool)
}
