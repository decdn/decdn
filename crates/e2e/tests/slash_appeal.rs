//! G-NODE-05 end-to-end: operator slash detection + appeal (#1032).
//!
//! Drives the whole journey against a real anvil deployment, all layers live:
//!
//! 1. Onboard operator **A** (appellant, daemon running) and operator **B**
//!    (voter). Age the chain ~185 days and have B serve real bytes so B has
//!    nonzero Governor vote weight (ADR-036 served bytes × age ramp) at the
//!    proposal snapshot — A's own weight is zeroed by the slash, so a second
//!    operator must carry the grant vote.
//! 2. Slash A through a **real** `SlashJudge` rate-manipulation commit-reveal
//!    challenge.
//! 3. Assert A's daemon surfaces the slash over `admin_v1_slashes`.
//! 4. File the appeal through the **`decdn appeal slash` CLI** (posts the bond),
//!    the emergency multisig fast-tracks, and a **real Governor proposal**
//!    (propose → vote → queue → timelock → execute) grants it.
//! 5. Assert the escrowed bond refunded and `slashedAtEpoch` recomputed to 0
//!    (vote weight restored).
//! 6. Negatives: a non-operator cannot appeal; a second appeal within 365 days
//!    is rejected; an appeal after the 30-day window is rejected.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH` and built `decdn-node` + `decdn` binaries:
//!
//! ```bash
//! cargo build -p decdn-node -p decdn
//! cargo nextest run -p decdn-e2e --features anvil-e2e
//! ```

#![cfg(feature = "anvil-e2e")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::duration_suboptimal_units
)]

use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use alloy::providers::DynProvider;
use alloy::signers::local::PrivateKeySigner;
use anyhow::Context;
use decdn_common::admin::AdminRpcClient;
use decdn_e2e::bindings::SlashAppeal;
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::cli::decdn_command;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::{KEYSTORE_PASSWORD, NodeFixture};
use decdn_e2e::poll;
use decdn_e2e::time;

const MIB: usize = 1024 * 1024;
const DAY: u64 = 24 * 60 * 60;
/// Appeal status codes (mirrors `ISlashAppeal.AppealStatus`).
const STATUS_OPEN: u8 = 1;
const STATUS_RESOLVED: u8 = 3;

/// Generous overall ceiling: the journey runs a real challenge + a full Governor
/// lifecycle with several time warps, plus two node subprocesses.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(1200);

#[tokio::test(flavor = "multi_thread")]
async fn slash_detection_appeal_and_grant() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("G-NODE-05 exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;

    // ---- Two operators: A (appellant, its daemon must surface the slash) and
    // B (voter, serves bytes to earn Governor vote weight). Both bond at ~T0.
    let payload = vec![0xABu8; 2 * MIB];
    let (node_a, _hash_a) = NodeFixture::launch(&chain, "US", &payload).await?;
    let (node_b, hash_b) = NodeFixture::launch(&chain, "US", &payload).await?;

    // ---- Age the chain past the ~180-day vote-weight ramp so B's weight is at
    // full strength once it has served bytes.
    time::increase_time(chain.admin(), 185 * DAY).await?;

    // ---- B serves real bytes → its FeeRouter served-bytes advance, giving it
    // Governor vote weight in the trailing window.
    let client = ClientFixture::new(&chain).await?;
    let outcome = client
        .fetch(&chain, &node_b, hash_b, alloy::primitives::U256::ZERO)
        .await?;
    assert_eq!(outcome.bytes, payload, "B must deliver the blob");
    let served = poll(Duration::from_secs(120), || async {
        let b = chain.served_bytes(node_b.operator_addr()).await?;
        Ok((b > U256::ZERO).then_some(b))
    })
    .await?;
    assert!(served.is_some(), "B's on-chain served-bytes never advanced");

    // ---- Cross an epoch boundary so those bytes sit in a fully-elapsed epoch
    // inside the trailing window at the proposal snapshot.
    time::increase_time(chain.admin(), 8 * DAY).await?;

    // ---- Slash A via a real SlashJudge rate-manipulation challenge. A fresh EOA
    // is the challenger; A's own eth key signs the self-incriminating evidence.
    let challenger = PrivateKeySigner::random();
    let node_a_id = B256::from_slice(node_a.node_id().as_bytes());
    let blob_hash = B256::repeat_byte(0x42);
    let slash_id = chain
        .slash_operator_via_judge(&challenger, node_a.operator(), node_a_id, blob_hash)
        .await?;

    // ---- Layer 1 (daemon): A surfaces the slash over admin RPC.
    let admin_a = node_a.admin_client()?;
    let surfaced = poll(Duration::from_secs(30), || async {
        let resp = admin_a.slashes().await.context("admin slashes")?;
        Ok(resp
            .slashes
            .iter()
            .any(|s| s.slash_id == slash_id.to_string())
            .then_some(()))
    })
    .await?;
    assert!(surfaced.is_some(), "A's daemon never surfaced the slash");

    // ---- Restart regression (#1032): a fresh process must re-surface the slash
    // by rebuilding its in-memory store from the SlashJudge floor scan, not
    // silently drop it. From the new process's view, the slash was mined while
    // it was down — the exact gap this guards against.
    node_a.restart().await?;
    let after_restart = poll(Duration::from_secs(30), || async {
        let resp = admin_a
            .slashes()
            .await
            .context("admin slashes after restart")?;
        Ok(resp
            .slashes
            .iter()
            .any(|s| s.slash_id == slash_id.to_string())
            .then_some(()))
    })
    .await?;
    assert!(
        after_restart.is_some(),
        "restarted daemon must re-surface the slash via the floor rescan"
    );

    // ---- Negative: a non-operator (B) cannot file A's appeal. Fund + approve
    // B's bond first so a zero-allowance `transferFrom` can't be the revert
    // reason — with the bond payable, only the `CallerNotOperator` guard can
    // reject it (if the guard were removed the call would succeed).
    let evidence = B256::repeat_byte(0xEE);
    let b_provider = chain.provider_for(node_b.operator());
    let bond = chain.appeal_bond().await?;
    fund_and_approve_bond(&chain, &b_provider, node_b.operator_addr(), bond).await?;
    let appeal_as_b = SlashAppeal::new(chain.addrs().slash_appeal, &b_provider)
        .openSlashAppeal(slash_id, evidence)
        .send()
        .await;
    assert!(
        appeal_as_b.is_err(),
        "a non-operator must not be able to open the appeal (CallerNotOperator)"
    );

    // ---- File the appeal through the `decdn appeal slash` CLI (posts the bond).
    // The operator's stake is locked in `CapacityBond`, so fund its wallet with
    // the appeal bond first — the CLI approves + posts it.
    chain.transfer_token(node_a.operator_addr(), bond).await?;
    let balance_before = chain.token_balance(node_a.operator_addr()).await?;
    run_appeal_cli(&node_a, slash_id, evidence)?;
    assert_eq!(
        chain.appeal_status(slash_id).await?,
        STATUS_OPEN,
        "appeal must be Open after the CLI filed it"
    );
    let balance_after_open = chain.token_balance(node_a.operator_addr()).await?;
    assert_eq!(
        balance_before - balance_after_open,
        bond,
        "the appeal bond must have been pulled from the operator"
    );

    // ---- Emergency multisig fast-tracks, then a real Governor proposal grants.
    let epoch_before = chain.slashed_at_epoch(node_a.operator_addr()).await?;
    assert!(
        epoch_before != 0,
        "A must carry a slash watermark pre-grant"
    );
    chain.fast_track_appeal(slash_id).await?;
    chain
        .governor_grant_appeal(slash_id, node_b.operator())
        .await?;

    // ---- Grant effects: appeal Resolved, bond refunded, watermark cleared.
    assert_eq!(
        chain.appeal_status(slash_id).await?,
        STATUS_RESOLVED,
        "appeal must be Resolved after the grant"
    );
    let balance_after_grant = chain.token_balance(node_a.operator_addr()).await?;
    assert!(
        balance_after_grant >= balance_before,
        "the appeal bond (and escrowed slash) must be refunded on a granted appeal: \
         before={balance_before}, after={balance_after_grant}"
    );
    assert_eq!(
        chain.slashed_at_epoch(node_a.operator_addr()).await?,
        0,
        "slashedAtEpoch must be recomputed to 0 (vote weight restored)"
    );

    // ---- Negative: a second appeal within 365 days is frequency-capped. A is
    // slashed again; fund + approve so the guard (`FrequencyCapHit`), not a
    // zero-allowance `transferFrom`, is what rejects A's own openSlashAppeal.
    let slash_id2 = chain
        .slash_operator_via_judge(&challenger, node_a.operator(), node_a_id, blob_hash)
        .await?;
    let a_provider = chain.provider_for(node_a.operator());
    fund_and_approve_bond(&chain, &a_provider, node_a.operator_addr(), bond).await?;
    let second_appeal = SlashAppeal::new(chain.addrs().slash_appeal, &a_provider)
        .openSlashAppeal(slash_id2, evidence)
        .send()
        .await;
    assert!(
        second_appeal.is_err(),
        "a second appeal within 365 days must be rejected (FrequencyCapHit)"
    );

    // ---- Negative: an appeal after the 30-day window is rejected. Slash B
    // (never granted, so not frequency-capped), fund + approve its bond so the
    // only possible revert is the closed filing window, warp past 30 days, then
    // openSlashAppeal must revert (FilingWindowClosed).
    let node_b_id = B256::from_slice(node_b.node_id().as_bytes());
    let slash_id3 = chain
        .slash_operator_via_judge(&challenger, node_b.operator(), node_b_id, blob_hash)
        .await?;
    fund_and_approve_bond(&chain, &b_provider, node_b.operator_addr(), bond).await?;
    time::increase_time(chain.admin(), 31 * DAY).await?;
    let late_appeal = SlashAppeal::new(chain.addrs().slash_appeal, &b_provider)
        .openSlashAppeal(slash_id3, evidence)
        .send()
        .await;
    assert!(
        late_appeal.is_err(),
        "an appeal after the 30-day filing window must be rejected (FilingWindowClosed)"
    );

    Ok(())
}

/// Fund `who`'s wallet with `bond` TOKEN and approve it to `SlashAppeal`, so a
/// negative test's `openSlashAppeal` revert can only be the on-chain guard
/// (`CallerNotOperator` / `FrequencyCapHit` / `FilingWindowClosed`) and never a
/// zero-allowance `transferFrom` — otherwise the test would pass even if the
/// guard were deleted.
async fn fund_and_approve_bond(
    chain: &ChainFixture,
    provider: &DynProvider,
    who: Address,
    bond: U256,
) -> anyhow::Result<()> {
    chain.transfer_token(who, bond).await?;
    let receipt = decdn_e2e::bindings::Erc20::new(chain.addrs().token, provider)
        .approve(chain.addrs().slash_appeal, bond)
        .send()
        .await?
        .get_receipt()
        .await?;
    anyhow::ensure!(receipt.status(), "bond approve must mine");
    Ok(())
}

/// Run the built `decdn appeal slash` command against the rendered config and
/// keystore of `node`, asserting a clean exit. `decdn_command` supplies the
/// keystore password and pins `HOME` to the fixture data dir.
fn run_appeal_cli(node: &NodeFixture, slash_id: U256, evidence: B256) -> anyhow::Result<()> {
    let status = decdn_command(node.data_dir(), KEYSTORE_PASSWORD)?
        .arg("appeal")
        .arg("slash")
        .arg(slash_id.to_string())
        .arg(format!("{evidence:#x}"))
        .arg("--config")
        .arg(node.config_path())
        .status()
        .context("spawn decdn appeal slash")?;
    anyhow::ensure!(
        status.success(),
        "`decdn appeal slash` exited non-zero: {status}"
    );
    Ok(())
}
