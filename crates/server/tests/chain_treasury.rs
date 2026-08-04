//! Anvil integration test for `sponsord::treasury::DecdnTreasury` (Task 8).
//!
//! Spins up `decdn_e2e::chain::ChainFixture` (anvil + the production
//! `DeployProtocol` script), onboards a bonded provider operator (a
//! `PaymentChannel.openChannel` requires the provider to be on-chain
//! `isActive` in `CapacityBond` — see `ChainFixture::onboard_operator`),
//! funds a buyer wallet with gas + mock USDC, builds a
//! `sponsord::treasury::TreasuryConfig` from the fixture's live endpoint and
//! deployed `PaymentChannel` address, and drives the `Treasury` trait exactly
//! as the sponsor server would: `open` a channel, read its escrow, then
//! `top_up_to` a higher target and confirm the on-chain deposit grew.
//!
//! Requires `anvil` + `forge` on `PATH` (`cargo test -p sponsord --features
//! anvil-e2e`). Mirrors the fixture usage in
//! `decdn/crates/e2e/tests/cli_fetch_topup.rs`.
#![cfg(feature = "anvil-e2e")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use alloy::primitives::{B256, U256};
use alloy::signers::local::PrivateKeySigner;
use decdn_e2e::chain::ChainFixture;
use sponsord::money::MicroUsdc;
use sponsord::treasury::{TreasuryConfig, connect};

#[tokio::test]
async fn open_then_topup_grows_escrow() {
    let chain = ChainFixture::launch()
        .await
        .expect("anvil + DeployProtocol launch");

    // Provider operator: `openChannel` requires it on-chain `isActive`
    // (bonded + registered), so onboard one exactly as decdn's own e2e
    // journeys do (`cli_fetch_topup.rs`).
    let provider_signer =
        PrivateKeySigner::from_bytes(&B256::repeat_byte(0x11)).expect("build provider signer");
    let provider_addr = provider_signer.address();
    let node_secret = iroh::SecretKey::from_bytes(&[0x33u8; 32]);
    chain
        .onboard_operator(
            &provider_signer,
            &node_secret,
            "US",
            "/ip4/127.0.0.1/udp/1/quic-v1",
        )
        .await
        .expect("onboard provider operator");

    // The treasury's own hot wallet: gas + enough mock USDC to cover the
    // initial deposit plus the top-up delta.
    let treasury_signer =
        PrivateKeySigner::from_bytes(&B256::repeat_byte(0x44)).expect("build treasury signer");
    let treasury_addr = treasury_signer.address();
    chain
        .fund_eth(treasury_addr, 100)
        .await
        .expect("fund treasury wallet gas");
    chain
        .mint_usdc(treasury_addr, U256::from(10_000_000u64))
        .await
        .expect("mint mock USDC to treasury wallet");

    let cfg = TreasuryConfig {
        rpc_url: chain.rpc_url(),
        payment_channel: chain.addrs().payment_channel,
        chain_id: chain.chain_id(),
        signer: treasury_signer,
    };
    let treasury = connect(&cfg).await.expect("connect DecdnTreasury");

    // A delegated voucher signer distinct from the treasury's own key
    // (the publisher-pays posture — ADR: pinned `voucherSigner`).
    let voucher_signer = PrivateKeySigner::random().address();

    let channel_id = treasury
        .open(provider_addr, voucher_signer, MicroUsdc(2_000_000))
        .await
        .expect("open channel");
    let d0 = treasury
        .deposit_of(channel_id)
        .await
        .expect("read initial deposit");
    assert_eq!(
        d0.0, 2_000_000,
        "initial on-chain deposit must equal the opened amount"
    );

    let d1 = treasury
        .top_up_to(channel_id, provider_addr, MicroUsdc(4_000_000))
        .await
        .expect("top up channel");
    assert_eq!(
        d1.0, 4_000_000,
        "top_up_to must raise the on-chain deposit to the target"
    );

    // Re-read directly to confirm `top_up_to`'s return value matches chain truth.
    let d2 = treasury
        .deposit_of(channel_id)
        .await
        .expect("re-read deposit after top-up");
    assert_eq!(d2.0, 4_000_000);
}
