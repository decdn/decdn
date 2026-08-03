//! Live anvil-backed e2e for the cooperative-close **watermark reconciliation**
//! path (#1495), against a real node over a real paid `cdn/client/v1` channel.
//!
//! The scenario: a client signs vouchers, the node accepts and stores them, and
//! then the client's record of what it signed ends up *behind* the node's —
//! vouchers issued but not durably persisted before an unclean exit. The
//! over-claim guard in `prepare_close` is correct (without it a provider could
//! ask the client to sign away up to the full deposit) but had no reconciliation
//! branch, so the cooperative close dead-ended. Nothing was lost — the
//! `closeChannel` → dispute window → `settleChannel` fallback remains — but the
//! one-transaction settle was unreachable, and the fallback submits a voucher
//! below what the client actually signed.
//!
//! One journey, because one is what needs a live node:
//!
//! **A lagging watermark reconciles.** The authorized watermark handed to
//! `cooperative_close` is deliberately rolled back below what the fetch actually
//! paid — the desync, expressed exactly as an unclean exit would leave it. The
//! close must settle anyway, at the *node's* state, and report the healed
//! watermark, which must then match the on-chain `claimedAmount`. That the node
//! echoed our own voucher signature is what makes this safe, and only a live
//! node exercises it: the echo comes from the node's own channel store,
//! populated by the real paid fetch above.
//!
//! Everything else about the branch is cheaper to test elsewhere and is tested
//! there: the adversarial echoes (foreign key, and a genuine signature over a
//! *different* tuple) and the no-echo refusal are unit tests in
//! `client-pull::cooperative_close`, since forging them needs a hostile node
//! this harness does not model; the node-side attach/omit behaviour is in
//! `decdn-node`'s `client_loopback`; and the unknown-channel decline is already
//! covered there too.
//!
//! Gated behind the `anvil-e2e` feature (off by default). Requires `anvil` +
//! `forge` on `PATH`:
//!
//! ```bash
//! cargo nextest run -p decdn-e2e --features anvil-e2e coop_close_watermark_reconcile
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

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use alloy::primitives::U256;
use anyhow::Context;
use decdn_client_pull::cooperative_close::{
    AuthorizedWatermark, CooperativeCloseOutcome, cooperative_close,
};
use decdn_e2e::chain::ChainFixture;
use decdn_e2e::client::ClientFixture;
use decdn_e2e::node::NodeFixture;
use decdn_incentive::payment_channel::PaymentChannel;
use decdn_incentive::voucher_domain;
use iroh::EndpointAddr;

const OVERALL_TIMEOUT: Duration = Duration::from_secs(780);
const COOP_CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread")]
async fn coop_close_settles_when_the_client_watermark_lags_the_node() -> anyhow::Result<()> {
    tokio::time::timeout(OVERALL_TIMEOUT, Box::pin(run()))
        .await
        .context("cooperative-close reconcile e2e exceeded the overall timeout")??;
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .try_init();

    let chain = ChainFixture::launch().await?;
    let blob = vec![0x7Cu8; 512 * 1024];
    let (node, hashes) = NodeFixture::launch_with_blobs(&chain, "US", &[&blob]).await?;
    let hash = hashes[0];
    let client = ClientFixture::new(&chain).await?;

    // A real paid fetch: the client signs vouchers, the node accepts them and
    // stores the last one's signature. That stored signature is what the node
    // echoes on the close, so it has to come from a genuine delivery.
    let (session, bytes) = client.open_session(&chain, &node, hash).await?;
    anyhow::ensure!(bytes == blob, "warm-up fetch must deliver the blob");
    let channel_id = session.channel_id();

    let rpc = chain.provider_for(client.signer());
    let pc = PaymentChannel::new(chain.addrs().payment_channel, rpc);
    let domain = voucher_domain(chain.chain_id(), chain.addrs().payment_channel);
    let target = EndpointAddr::new(node.node_id()).with_ip_addr(SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::LOCALHOST,
        node.bind_port(),
    )));

    // Precondition: the channel is funded, so a settle has something to settle.
    let before = pc
        .getChannel(channel_id)
        .call()
        .await
        .context("read channel before close")?;
    anyhow::ensure!(
        before.deposit > U256::ZERO,
        "the channel must be funded before the close"
    );

    // The desync: our record of what we signed is empty, though we in fact
    // signed at least one voucher during the fetch above. This is exactly the
    // shape an unclean exit leaves — the node holds vouchers we cannot account
    // for, so every field of its declared tuple over-claims against our record.
    let lagging = AuthorizedWatermark {
        amount: U256::ZERO,
        nonce: U256::ZERO,
        bytes_delivered: U256::ZERO,
    };

    let mut reconciled = None;
    let outcome = cooperative_close(
        client.endpoint(),
        target,
        &pc,
        channel_id,
        node.operator_addr(),
        chain.usdc(),
        lagging,
        client.signer(),
        &domain,
        COOP_CLOSE_TIMEOUT,
        &mut reconciled,
    )
    .await
    .context("cooperative close with a lagging watermark")?;

    // Before #1495 this path could not reach an outcome at all: `prepare_close`
    // returned an `Err("provider over-claimed: ...")`, forcing the slow
    // `closeChannel` fallback with a voucher that underpays the node.
    anyhow::ensure!(
        outcome == CooperativeCloseOutcome::Settled,
        "expected a settled close, got {outcome:?}"
    );
    let healed = reconciled.ok_or_else(|| {
        anyhow::anyhow!("a close above a zeroed local watermark must report a reconciliation")
    })?;
    anyhow::ensure!(
        healed.nonce > U256::ZERO && healed.amount > U256::ZERO,
        "the healed watermark must carry the vouchers the fetch actually paid, got {healed:?}"
    );

    // The settlement really landed: the channel is terminal and the node was
    // paid the healed amount, not the zeroed one we walked in with.
    let after = pc
        .getChannel(channel_id)
        .call()
        .await
        .context("read channel after close")?;
    anyhow::ensure!(
        after.claimedAmount == healed.amount,
        "on-chain claimedAmount ({}) must equal the reconciled amount ({})",
        after.claimedAmount,
        healed.amount
    );

    Ok(())
}
