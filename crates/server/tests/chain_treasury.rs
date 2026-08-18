//! Anvil integration test for `sponsord::treasury::DecdnTreasury`.
//!
//! Launches the deploy fixture, funds a hot wallet with gas + mock USDC,
//! opens a PaymentPool as that wallet, then drives the pool `Treasury`
//! exactly as the sponsor would: confirm `pool_owner` is the wallet, read
//! `remaining`, `top_up`, and confirm `remaining` grew by the credited amount.
//!
//! Requires `anvil` + `forge` on PATH: `cargo test -p sponsord --features anvil-e2e`.
#![cfg(feature = "anvil-e2e")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::indexing_slicing)]

use std::sync::Arc;

use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;
use decdn_client_pull::buyer_pool::{ensure_allowance, open_pool};
use decdn_e2e::chain::ChainFixture;
use decdn_incentive::payment_pool::PaymentPool;
use decdn_incentive::voucher_domain;
use sponsord::money::MicroUsdc;
use sponsord::treasury::{TreasuryConfig, connect};

/// Opening deposit, in USDC base units (6 decimals) — well above the top-up
/// amount so `remaining` never risks going negative.
const OPEN_DEPOSIT_MICRO_USDC: u64 = 50_000_000;
/// Amount credited by the test's `top_up` call.
const TOPUP_MICRO_USDC: u64 = 5_000_000;

#[tokio::test]
async fn remaining_grows_after_topup() {
    let chain = ChainFixture::launch().await.expect("launch");

    // 1. Fund a fresh hot wallet with gas + mock USDC, and grant the
    //    PaymentPool an allowance covering both the pool-open deposit and the
    //    later sponsor top_up.
    let signer = PrivateKeySigner::random();
    let owner = signer.address();
    chain.fund_eth(owner, 100).await.expect("fund gas");
    chain
        .mint_usdc(
            owner,
            U256::from(OPEN_DEPOSIT_MICRO_USDC + TOPUP_MICRO_USDC),
        )
        .await
        .expect("mint usdc");

    let open_provider = chain.provider_for(&signer);
    ensure_allowance(
        &open_provider,
        chain.usdc(),
        owner,
        chain.addrs().payment_pool,
        Some(U256::from(OPEN_DEPOSIT_MICRO_USDC + TOPUP_MICRO_USDC)),
    )
    .await
    .expect("approve PaymentPool allowance");

    // 2. Open a pool as that wallet via the deployed PaymentPool.openPool,
    //    capturing pool_id.
    let voucher_dom = voucher_domain(chain.chain_id(), chain.addrs().payment_pool);
    let contract = PaymentPool::new(chain.addrs().payment_pool, open_provider);
    let opened = open_pool(
        &contract,
        Arc::new(signer.clone()),
        &voucher_dom,
        chain.usdc(),
        owner,
        U256::from(OPEN_DEPOSIT_MICRO_USDC),
    )
    .await
    .expect("open pool");
    let pool_id = opened.state.pool_id;

    // 3. Build the sponsor treasury against the fixture endpoint + pool addr,
    //    driving it exactly as sponsord does.
    let cfg = TreasuryConfig {
        rpc_url: chain.rpc_url(),
        payment_pool: chain.addrs().payment_pool,
        chain_id: chain.chain_id(),
        signer: signer.clone(),
    };
    let treasury = connect(&cfg).await.expect("connect");

    assert_eq!(
        treasury.pool_owner(pool_id).await.unwrap(),
        treasury.owner_address()
    );
    let before = treasury.remaining(pool_id).await.unwrap();
    let credited = treasury
        .top_up(pool_id, MicroUsdc(TOPUP_MICRO_USDC))
        .await
        .unwrap();
    let after = treasury.remaining(pool_id).await.unwrap();
    assert_eq!(after.0, before.0 + credited.0);
}
