//! Anvil integration test for `sponsord_core::pool::ChainPool`.
//!
//! Launches the deploy fixture, funds a hot wallet with gas + mock USDC,
//! opens a PaymentPool as that wallet, then drives the pool `PoolChain`
//! exactly as the sponsor would: confirm `pool_owner` is the wallet, read
//! `remaining`, `top_up`, and confirm `remaining` grew by the credited amount.
//!
//! Requires `anvil` + `forge` on PATH: `cargo test -p sponsord-core --features anvil-e2e`.
#![cfg(feature = "anvil-e2e")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;

use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;
use decdn_client::buyer_pool::{ensure_allowance, open_pool};
use decdn_e2e::chain::ChainFixture;
use decdn_incentive::Deployment;
use decdn_incentive::payment_pool::PaymentPool;
use sponsord_core::pool::{ChainPool, PoolChain};
use sponsord_core::{ChainConfig, Limits, MicroUsdc, Sponsor, TermsRequest};

/// Opening deposit, in USDC base units (6 decimals), well above the top-up
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
    let deployment = Deployment {
        chain_id: chain.chain_id(),
        payment_pool: chain.addrs().payment_pool,
    };
    let contract = PaymentPool::new(chain.addrs().payment_pool, open_provider);
    let opened = open_pool(
        &contract,
        Arc::new(signer.clone()),
        deployment,
        chain.usdc(),
        owner,
        U256::from(OPEN_DEPOSIT_MICRO_USDC),
    )
    .await
    .expect("open pool");
    let pool_id = opened.state.pool_id;

    // 3. Build the sponsor's pool client against the fixture endpoint + pool addr,
    //    driving it exactly as sponsord does.
    let pool = ChainPool::connect(&chain.rpc_url(), chain.addrs().payment_pool, signer.clone())
        .await
        .expect("connect");

    assert_eq!(
        pool.pool_owner(pool_id).await.unwrap(),
        pool.owner_address()
    );
    let before = pool.remaining(pool_id).await.unwrap();
    let credited = pool
        .top_up(pool_id, MicroUsdc(TOPUP_MICRO_USDC))
        .await
        .unwrap();
    let after = pool.remaining(pool_id).await.unwrap();
    assert_eq!(after.0, before.0 + credited.0);

    // A signer that never redeemed is unregistered.
    assert_eq!(
        pool.authorization(pool_id, alloy::primitives::Address::repeat_byte(0x77))
            .await
            .unwrap(),
        None
    );

    // 4. The full sponsor on the same chain: the boot owner check passes and
    //    an unregistered signer gets a fresh capability.
    let sponsor = Sponsor::connect(
        signer,
        ChainConfig {
            rpc_url: chain.rpc_url(),
            chain_id: chain.chain_id(),
            payment_pool: chain.addrs().payment_pool,
            pool_id,
        },
        Limits {
            max_spending_cap: MicroUsdc(1_000_000),
            max_ttl_secs: 3_600,
        },
    )
    .await
    .expect("sponsor connect");
    let issued = sponsor
        .issue(
            alloy::primitives::Address::repeat_byte(0x77),
            &TermsRequest::default(),
            1_769_904_000,
        )
        .await
        .expect("issue");
    assert!(!issued.registered);
    assert_eq!(issued.capability.spending_cap, MicroUsdc(1_000_000));
}
