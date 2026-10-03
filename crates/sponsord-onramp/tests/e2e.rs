//! The onramp against a real `sponsord` router on an ephemeral port, over
//! real HTTP: the contract between the two processes.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

#[path = "../src/test_support.rs"]
mod test_support;

use std::str::FromStr;
use std::sync::Arc;

use alloy::primitives::Address;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use decdn_incentive::{CapabilityGrant, voucher_domain};
use serde_json::{Value, json};
use sponsord::http::ApiState;
use sponsord_api::MicroUsdc;
use sponsord_api::client::DaemonClient;
use sponsord_core::pool::{Authorization, PoolChain};
use sponsord_core::test_support::{FakePool, TEST_CHAIN_ID, TEST_PAYMENT_POOL, fake_sponsor};
use sponsord_onramp::daemon::CapabilitySource;
use tower::ServiceExt;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

async fn stack() -> (Router, Arc<FakePool>) {
    let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
    let daemon = sponsord::http::router(ApiState::new(Arc::new(sponsor), Arc::from(TOKEN)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, daemon).await.unwrap() });

    let mut cfg = test_support::test_config(tempfile::tempdir().unwrap().keep());
    cfg.daemon_url = url;
    cfg.daemon_token = TOKEN.into();
    let source: Arc<dyn CapabilitySource> = Arc::new(DaemonClient::new(
        &cfg.daemon_url,
        cfg.daemon_token.clone(),
        reqwest::Client::new(),
    ));
    let st =
        sponsord_onramp::state::build(cfg, source, Arc::new(test_support::FakeCaptcha::new(true)))
            .await
            .unwrap();
    (sponsord_onramp::http::router(st), pool)
}

async fn fund(app: &Router, client: &str) -> (StatusCode, Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::post("/v1/fund")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"client": client, "proof": "ok"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn decode(token: &str) -> CapabilityGrant {
    CapabilityGrant::from_token(token).unwrap()
}

#[tokio::test]
async fn fund_then_capability_returns_an_owner_signed_token() {
    let (app, pool) = stack().await;
    let client = "0x00000000000000000000000000000000000000aa";
    let (s, v) = fund(&app, client).await;
    assert_eq!(s, StatusCode::OK);
    let token = v["token"].as_str().unwrap().to_owned();
    let grant = decode(&token);
    assert_eq!(grant.signer, Address::from_str(client).unwrap());
    assert_eq!(grant.spending_cap, 5_000_000);
    assert_eq!(
        grant
            .owner(&voucher_domain(TEST_CHAIN_ID, TEST_PAYMENT_POOL))
            .unwrap(),
        pool.owner_address()
    );

    let resp = app
        .oneshot(
            Request::get(format!("/v1/capability?client={client}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["token"].as_str(), Some(token.as_str()));
}

#[tokio::test]
async fn registered_signer_gets_its_existing_terms_through_the_stack() {
    let (app, pool) = stack().await;
    let client = "0x00000000000000000000000000000000000000bb";
    let expiry = 4_000_000_000;
    pool.register(
        Address::from_str(client).unwrap(),
        Authorization {
            spending_cap: MicroUsdc(3_000_000),
            expiry,
        },
    );
    let (s, v) = fund(&app, client).await;
    assert_eq!(s, StatusCode::OK);
    let grant = decode(v["token"].as_str().unwrap());
    assert_eq!(grant.spending_cap, 3_000_000);
    assert_eq!(grant.expiry, expiry);
}

#[tokio::test]
async fn expired_registration_is_409_through_the_stack() {
    let (app, pool) = stack().await;
    let client = "0x00000000000000000000000000000000000000cc";
    pool.register(
        Address::from_str(client).unwrap(),
        Authorization {
            spending_cap: MicroUsdc(5_000_000),
            expiry: 1_000,
        },
    );
    let (s, v) = fund(&app, client).await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert_eq!(v["error"], "signer_expired");
}

#[tokio::test]
async fn installers_render_the_daemon_chain_values() {
    let (app, _) = stack().await;
    let resp = app
        .oneshot(Request::get("/decdn.sh").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(body.contains(&TEST_PAYMENT_POOL.to_string()));
    assert!(body.contains(&TEST_CHAIN_ID.to_string()));
}
