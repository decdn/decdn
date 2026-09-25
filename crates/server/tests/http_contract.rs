#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

#[path = "../src/test_support.rs"]
mod test_support;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use test_support::app_state_with_fakes;
use test_support::{FakeOptions, app_state_with_options};

use serde_json::Value;

#[tokio::test]
async fn healthz_ok() {
    let state = app_state_with_fakes();
    let app = sponsord::http::router(state);
    let resp = app
        .oneshot(Request::get("/healthz").body(Body::empty()).expect("req"))
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::OK);
}

const CLIENT: &str = "0x00000000000000000000000000000000000000aa";

async fn json_body(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&bytes).expect("json")
}

fn fund_body() -> String {
    serde_json::json!({ "client": CLIENT, "turnstile_token": "ok" }).to_string()
}

#[tokio::test]
async fn fund_issues_token_then_capability_returns_same_token() {
    let state = test_support::app_state_with_fakes();
    let app = sponsord::http::router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::post("/fund")
                .header("content-type", "application/json")
                .body(Body::from(fund_body()))
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;
    let token = v["token"].as_str().expect("token").to_string();
    assert!(token.starts_with("dcap1:"));

    // GET /capability returns the same token.
    let resp = app
        .clone()
        .oneshot(
            Request::get(format!("/capability?client={CLIENT}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        json_body(resp).await["token"].as_str(),
        Some(token.as_str())
    );

    // Second /fund is idempotent: identical token, no re-sign.
    let resp = app
        .oneshot(
            Request::post("/fund")
                .header("content-type", "application/json")
                .body(Body::from(fund_body()))
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        json_body(resp).await["token"].as_str(),
        Some(token.as_str())
    );
}

#[tokio::test]
async fn capability_204_before_issue() {
    let state = test_support::app_state_with_fakes();
    let app = sponsord::http::router(state);
    let resp = app
        .oneshot(
            Request::get(format!("/capability?client={CLIENT}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn fund_rejects_bad_captcha_403() {
    let state = app_state_with_options(FakeOptions {
        captcha_passes: false,
        ..FakeOptions::default()
    });
    let app = sponsord::http::router(state);
    let resp = app
        .oneshot(
            Request::post("/fund")
                .header("content-type", "application/json")
                .body(Body::from(fund_body()))
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        json_body(resp).await["error"].as_str(),
        Some("captcha_failed")
    );
}

#[tokio::test]
async fn fund_page_embeds_sitekey_and_client_and_rejects_non_hex() {
    let state = test_support::app_state_with_fakes();
    let app = sponsord::http::router(state);

    let resp = app
        .clone()
        .oneshot(
            Request::get(format!("/fund?client={CLIENT}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    let html = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(html.contains("TEST_SITEKEY"));
    assert!(html.contains(CLIENT));

    let resp = app
        .oneshot(
            Request::get("/fund?client=%3Cscript%3E")
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn expired_grant_is_treated_as_absent_and_reissued() {
    use alloy::primitives::Address;
    use sponsord::money::MicroUsdc;
    use sponsord::store::GrantRecord;
    use std::str::FromStr;

    let state = test_support::app_state_with_fakes();

    // Pre-seed the store with an already-expired grant (fixed past unix
    // timestamp), bypassing the issuer entirely.
    let signer = Address::from_str(CLIENT).expect("addr");
    let stale = GrantRecord {
        spending_cap: MicroUsdc(10_000_000).0,
        expiry: 1_000_000,
        issued_unix: 0,
        token: "dcap1:STALE".to_string(),
    };
    state.store.put_grant(signer, &stale).expect("seed grant");

    let app = sponsord::http::router(state);

    // GET /capability treats the expired grant as absent.
    let resp = app
        .clone()
        .oneshot(
            Request::get(format!("/capability?client={CLIENT}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // POST /fund re-issues a fresh token rather than returning the stale one.
    let resp = app
        .oneshot(
            Request::post("/fund")
                .header("content-type", "application/json")
                .body(Body::from(fund_body()))
                .expect("req"),
        )
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::OK);
    let token = json_body(resp).await["token"]
        .as_str()
        .expect("token")
        .to_string();
    assert_ne!(token, "dcap1:STALE");
    assert!(token.starts_with("dcap1:"));
}

#[tokio::test]
async fn decdn_sh_templated_with_payment_pool() {
    let state = test_support::app_state_with_fakes();
    let app = sponsord::http::router(state);
    let resp = app
        .oneshot(Request::get("/decdn.sh").body(Body::empty()).expect("req"))
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(!body.contains("{{"), "no placeholder should remain");
    assert!(
        body.contains("payment_pool ="),
        "installer writes payment_pool"
    );
    assert_pins_releases(&body);
}

/// Both installers download from the GitHub Releases pinned in config
/// (`test_support`: decdn v0.1.0, sponsord v0.2.0), by tag and by the digest
/// of each release's SHA256SUMS.
fn assert_pins_releases(body: &str) {
    assert!(body.contains("https://github.com/decdn"));
    assert!(body.contains("'v0.1.0'") || body.contains("\"v0.1.0\""));
    assert!(body.contains("'v0.2.0'") || body.contains("\"v0.2.0\""));
    assert!(body.contains(&"ab".repeat(32)), "decdn SHA256SUMS digest");
    assert!(
        body.contains(&"cd".repeat(32)),
        "sponsord SHA256SUMS digest"
    );
    assert!(!body.contains("/dl/"), "no gateway-hosted binaries");
}

#[tokio::test]
async fn decdn_ps1_templated_with_payment_pool() {
    let state = test_support::app_state_with_fakes();
    let app = sponsord::http::router(state);
    let resp = app
        .oneshot(Request::get("/decdn.ps1").body(Body::empty()).expect("req"))
        .await
        .expect("resp");
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(!body.contains("{{"), "no placeholder should remain");
    assert!(
        body.contains("payment_pool ="),
        "installer writes payment_pool"
    );
    assert!(
        body.contains("-pc-windows-msvc"),
        "installer downloads the Windows release archives"
    );
    assert_pins_releases(&body);
}
