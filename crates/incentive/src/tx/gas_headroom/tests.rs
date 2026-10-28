#![allow(clippy::unwrap_used)]

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, TxKind};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;

use super::*;
use crate::test_rpc::GasStub;

/// The gas limit `declareMbps` was estimated at in the #2730 repro: the
/// overwrite path, which the push path outgrew in the next second.
const ESTIMATE: u64 = 57_076;

/// A request from `signer` with every field but the gas limit set, so the
/// only chain read before the send is the gas estimate.
fn request(signer: &PrivateKeySigner, gas: Option<u64>) -> TransactionRequest {
    TransactionRequest {
        from: Some(signer.address()),
        to: Some(TxKind::Call(Address::repeat_byte(0x01))),
        nonce: Some(0),
        gas,
        chain_id: Some(1),
        max_fee_per_gas: Some(1),
        max_priority_fee_per_gas: Some(1),
        ..Default::default()
    }
}

async fn send_through_filler(gas: Option<u64>) -> Vec<u64> {
    decdn_common::tls::install_crypto_provider();
    let stub = GasStub::spawn(ESTIMATE).await.unwrap();
    let signer = PrivateKeySigner::random();
    let provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .filler(GasHeadroomFiller::default())
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_http(stub.url().parse().unwrap());
    let _pending = provider
        .send_transaction(request(&signer, gas))
        .await
        .unwrap();
    stub.sent_gas_limits()
}

/// An estimated limit goes out with the headroom on top, so a path that
/// writes a fresh slot the estimate's path did not still fits (#2730).
#[tokio::test]
async fn an_estimated_limit_is_sent_with_the_headroom() {
    assert_eq!(
        send_through_filler(None).await,
        vec![ESTIMATE + GAS_HEADROOM]
    );
}

/// A limit the caller set is theirs: it goes out unchanged, with no
/// estimate taken.
#[tokio::test]
async fn a_caller_set_limit_is_sent_unchanged() {
    assert_eq!(send_through_filler(Some(21_000)).await, vec![21_000]);
}

/// The headroom covers two fresh slots, a `FeeRouter` settle in a new epoch:
/// a cold zero-to-nonzero `SSTORE` (22,100) each, plus the 1/64 a calling
/// frame withholds.
#[test]
fn the_headroom_covers_two_fresh_slots() {
    let fresh_slots = 2 * 22_100;
    assert!(GAS_HEADROOM >= fresh_slots + fresh_slots / 63);
}

#[test]
fn the_headroom_saturates() {
    assert_eq!(with_headroom(u64::MAX - 1), u64::MAX);
    assert_eq!(with_headroom(ESTIMATE), ESTIMATE + GAS_HEADROOM);
}
