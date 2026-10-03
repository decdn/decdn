#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::sync::Arc;

use alloy::primitives::Address;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use decdn_incentive::{CapabilityGrant, voucher_domain};
use serde_json::{Value, json};
use sponsord::http::{ApiState, router};
use sponsord_api::daemon::{Info, IssueResponse};
use sponsord_api::{ErrorBody, ErrorCode, MicroUsdc};
use sponsord_core::pool::{Authorization, PoolChain};
use sponsord_core::test_support::{FakePool, TEST_CHAIN_ID, TEST_PAYMENT_POOL, fake_sponsor};
use tower::ServiceExt;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";
const SIGNER: &str = "0x00000000000000000000000000000000000000aa";

async fn app() -> (Router, Arc<FakePool>) {
    let (sponsor, pool) = fake_sponsor(5_000_000, 172_800).await;
    let app = router(ApiState::new(Arc::new(sponsor), Arc::from(TOKEN)));
    (app, pool)
}

async fn send(app: &Router, req: Request<Body>) -> (StatusCode, Value) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, v)
}

fn issue_req(auth: Option<&str>, body: Value) -> Request<Body> {
    let mut b = Request::post("/v1/capabilities").header("content-type", "application/json");
    if let Some(a) = auth {
        b = b.header("authorization", a);
    }
    b.body(Body::from(body.to_string())).unwrap()
}

fn bearer() -> String {
    format!("Bearer {TOKEN}")
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::test]
async fn healthz_needs_no_token() {
    let (app, _) = app().await;
    let (s, v) = send(&app, Request::get("/healthz").body(Body::empty()).unwrap()).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v, json!({"ok": true}));
}

#[tokio::test]
async fn missing_or_wrong_token_is_401() {
    let (app, _) = app().await;
    let (s, v) = send(&app, Request::get("/v1/info").body(Body::empty()).unwrap()).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    assert_eq!(v["error"], "unauthorized");

    let wrong = format!("Bearer {}", "f".repeat(32));
    let (s, _) = send(&app, issue_req(Some(&wrong), json!({"signer": SIGNER}))).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);

    let (s, _) = send(&app, issue_req(Some(TOKEN), json!({"signer": SIGNER}))).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED, "token without a scheme");
}

#[tokio::test]
async fn bearer_scheme_is_case_insensitive() {
    let (app, _) = app().await;
    let lower = format!("bearer {TOKEN}");
    let (s, _) = send(&app, issue_req(Some(&lower), json!({"signer": SIGNER}))).await;
    assert_eq!(s, StatusCode::OK);
}

#[tokio::test]
async fn info_reports_chain_and_maximums() {
    let (app, _) = app().await;
    let (s, v) = send(
        &app,
        Request::get("/v1/info")
            .header("authorization", bearer())
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let typed: Info = serde_json::from_value(v.clone()).unwrap();
    assert_eq!(typed.payment_pool, TEST_PAYMENT_POOL);
    assert_eq!(
        v,
        json!({
            "chain_id": TEST_CHAIN_ID,
            "payment_pool": TEST_PAYMENT_POOL.to_string(),
            "max_spending_cap": 5_000_000,
            "max_ttl_secs": 172_800,
        })
    );
}

#[tokio::test]
async fn issue_defaults_to_the_maximum_and_signs_for_the_signer() {
    let (app, pool) = app().await;
    let before = now();
    let (s, v) = send(&app, issue_req(Some(&bearer()), json!({"signer": SIGNER}))).await;
    assert_eq!(s, StatusCode::OK);
    let typed: IssueResponse = serde_json::from_value(v.clone()).unwrap();
    assert!(!typed.registered);
    assert_eq!(v["registered"], false);
    assert_eq!(v["spending_cap"], 5_000_000);
    let expiry = v["expiry"].as_u64().unwrap();
    assert!(expiry >= before + 172_800 && expiry <= now() + 172_800);

    let grant = CapabilityGrant::from_token(v["token"].as_str().unwrap()).unwrap();
    assert_eq!(grant.signer.to_string().to_lowercase(), SIGNER);
    assert_eq!(grant.spending_cap, 5_000_000);
    assert_eq!(grant.expiry, expiry);
    assert_eq!(
        grant
            .owner(&voucher_domain(TEST_CHAIN_ID, TEST_PAYMENT_POOL))
            .unwrap(),
        pool.owner_address()
    );
}

#[tokio::test]
async fn issue_honours_lower_terms() {
    let (app, _) = app().await;
    let before = now();
    let (s, v) = send(
        &app,
        issue_req(
            Some(&bearer()),
            json!({"signer": SIGNER, "spending_cap": 1_000_000, "ttl_secs": 3_600}),
        ),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["spending_cap"], 1_000_000);
    let expiry = v["expiry"].as_u64().unwrap();
    assert!(expiry >= before + 3_600 && expiry <= now() + 3_600);
}

#[tokio::test]
async fn above_maximum_is_400_exceeds_max_with_the_maximums() {
    let (app, _) = app().await;
    let (s, v) = send(
        &app,
        issue_req(
            Some(&bearer()),
            json!({"signer": SIGNER, "spending_cap": 5_000_001}),
        ),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(
        v,
        json!({"error": "exceeds_max", "max_spending_cap": 5_000_000, "max_ttl_secs": 172_800})
    );
}

#[tokio::test]
async fn zero_terms_are_400_zero() {
    let (app, _) = app().await;
    let (s, v) = send(
        &app,
        issue_req(Some(&bearer()), json!({"signer": SIGNER, "ttl_secs": 0})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(v["error"], "zero");
}

#[tokio::test]
async fn malformed_requests_are_400_bad_request() {
    let (app, _) = app().await;
    let (s, v) = send(
        &app,
        issue_req(Some(&bearer()), json!({"signer": "0xnope"})),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(v["error"], "bad_request");

    let (s, v) = send(&app, issue_req(Some(&bearer()), json!({"spending_cap": 1}))).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(v["error"], "bad_request");

    let (s, v) = send(
        &app,
        Request::post("/v1/capabilities")
            .header("authorization", bearer())
            .body(Body::from("not json"))
            .unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(v["error"], "bad_request");
}

#[tokio::test]
async fn lowercase_and_checksummed_signers_are_both_accepted() {
    let (app, _) = app().await;
    let checksummed = Address::repeat_byte(0xab).to_string();
    let lower = checksummed.to_lowercase();
    for signer in [checksummed, lower] {
        let (s, _) = send(&app, issue_req(Some(&bearer()), json!({"signer": signer}))).await;
        assert_eq!(s, StatusCode::OK);
    }
}

#[tokio::test]
async fn registered_signer_gets_the_same_token_back() {
    let (app, pool) = app().await;
    let (_, first) = send(&app, issue_req(Some(&bearer()), json!({"signer": SIGNER}))).await;
    pool.register(
        SIGNER.parse().unwrap(),
        Authorization {
            spending_cap: MicroUsdc(first["spending_cap"].as_u64().unwrap()),
            expiry: first["expiry"].as_u64().unwrap(),
        },
    );
    let (s, again) = send(
        &app,
        issue_req(
            Some(&bearer()),
            json!({"signer": SIGNER, "spending_cap": 1}),
        ),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(again["registered"], true);
    assert_eq!(again["token"], first["token"]);
    assert_eq!(again["spending_cap"], first["spending_cap"]);
}

#[tokio::test]
async fn expired_registration_is_409_signer_expired() {
    let (app, pool) = app().await;
    pool.register(
        SIGNER.parse().unwrap(),
        Authorization {
            spending_cap: MicroUsdc(5_000_000),
            expiry: 1_000,
        },
    );
    let (s, v) = send(&app, issue_req(Some(&bearer()), json!({"signer": SIGNER}))).await;
    assert_eq!(s, StatusCode::CONFLICT);
    assert_eq!(v, json!({"error": "signer_expired", "expiry": 1_000}));
    let typed: ErrorBody = serde_json::from_value(v).unwrap();
    assert_eq!(typed.error, ErrorCode::SignerExpired);
}

#[tokio::test]
async fn failed_chain_read_is_503() {
    let (app, pool) = app().await;
    pool.fail_authorization_reads(true);
    let (s, v) = send(&app, issue_req(Some(&bearer()), json!({"signer": SIGNER}))).await;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(v["error"], "chain_unavailable");
}
