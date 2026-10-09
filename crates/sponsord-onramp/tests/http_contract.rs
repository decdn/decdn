//! The onramp's HTTP contract, against a fake daemon and gate.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::net::SocketAddr;
use std::sync::Arc;

use alloy_primitives::Address;
use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use sponsord_api::onramp::Profile;
use sponsord_api::time::{Clock, FixedClock, SystemClock};
use sponsord_api::{ErrorBody, ErrorCode, IssuedCapability, MicroUsdc};
use sponsord_onramp::net::ClientIpSource;
use sponsord_onramp::test_support::{
    FakeOptions, SourceBehavior, app_state_with_fakes, app_state_with_options, test_config,
};
use tower::ServiceExt;

const CLIENT: &str = "0x00000000000000000000000000000000000000aa";

fn client() -> Address {
    CLIENT.parse().unwrap()
}

async fn send(app: &Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, bytes.to_vec())
}

async fn send_json(app: &Router, req: Request<Body>) -> (StatusCode, Value) {
    let (status, bytes) = send(app, req).await;
    let v = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, v)
}

fn get(uri: &str) -> Request<Body> {
    Request::get(uri).body(Body::empty()).unwrap()
}

fn fund_req(body: Value) -> Request<Body> {
    Request::post("/v1/fund")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn fund() -> Request<Body> {
    fund_req(json!({ "client": CLIENT, "proof": "ok" }))
}

fn poll() -> Request<Body> {
    get(&format!("/v1/capability?client={CLIENT}"))
}

fn error_code(v: &Value) -> ErrorCode {
    serde_json::from_value::<ErrorBody>(v.clone())
        .unwrap()
        .error
}

#[tokio::test]
async fn healthz_ok() {
    let app = sponsord_onramp::http::router(app_state_with_fakes().await);
    let (s, v) = send_json(&app, get("/healthz")).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v, json!({"ok": true}));
}

#[tokio::test]
async fn fund_issues_a_token_the_poll_then_returns() {
    let (state, fakes) = app_state_with_options(FakeOptions::default()).await;
    let app = sponsord_onramp::http::router(state);

    let (s, v) = send_json(&app, poll()).await;
    assert_eq!((s, v), (StatusCode::NO_CONTENT, Value::Null));

    let (s, v) = send_json(&app, fund()).await;
    assert_eq!(s, StatusCode::OK);
    let token = v["token"].as_str().unwrap().to_owned();
    assert!(token.starts_with("dcap1:"));

    let (s, v) = send_json(&app, poll()).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["token"].as_str(), Some(token.as_str()));

    // A second fund is idempotent: the same token, no second daemon call.
    let (s, v) = send_json(&app, fund()).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["token"].as_str(), Some(token.as_str()));
    assert_eq!(fakes.source.issued_count(), 1);
}

#[tokio::test]
async fn a_refused_gate_is_403_gate_failed() {
    let (state, fakes) = app_state_with_options(FakeOptions {
        gate_passes: false,
        ..FakeOptions::default()
    })
    .await;
    let app = sponsord_onramp::http::router(state);
    let (s, v) = send_json(&app, fund()).await;
    assert_eq!(s, StatusCode::FORBIDDEN);
    assert_eq!(error_code(&v), ErrorCode::GateFailed);
    assert_eq!(fakes.source.issued_count(), 0);
}

#[tokio::test]
async fn malformed_requests_are_400_bad_request() {
    let app = sponsord_onramp::http::router(app_state_with_fakes().await);
    for req in [
        fund_req(json!({ "client": "0xnope", "proof": "ok" })),
        fund_req(json!({ "client": CLIENT })),
        get("/v1/capability?client=%3Cscript%3E"),
        get("/v1/capability"),
        get("/fund?client=%3Cscript%3E"),
    ] {
        let uri = req.uri().clone();
        let (s, v) = send_json(&app, req).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(error_code(&v), ErrorCode::BadRequest, "{uri}");
    }
}

#[tokio::test]
async fn fund_page_is_the_gate_page_for_the_client() {
    let app = sponsord_onramp::http::router(app_state_with_fakes().await);
    let (s, body) = send(&app, get(&format!("/fund?client={CLIENT}"))).await;
    assert_eq!(s, StatusCode::OK);
    let html = String::from_utf8(body).unwrap();
    assert_eq!(html, format!("<p>fake gate for {}</p>", client()));
}

#[tokio::test]
async fn an_expired_capability_reads_as_absent_and_is_reissued() {
    let (mut state, fakes) = app_state_with_options(FakeOptions::default()).await;
    let now = SystemClock.now_unix();
    let clock = Arc::new(FixedClock::new(now));
    state.clock = clock.clone();
    state.grants.put(
        client(),
        IssuedCapability {
            token: "dcap1:STALE".into(),
            spending_cap: MicroUsdc(1),
            expiry: now + 10,
        },
        now,
    );
    let app = sponsord_onramp::http::router(state);
    let (s, v) = send_json(&app, poll()).await;
    assert_eq!(
        (s, v["token"].as_str()),
        (StatusCode::OK, Some("dcap1:STALE"))
    );

    clock.advance(10);
    let (s, _) = send_json(&app, poll()).await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (s, v) = send_json(&app, fund()).await;
    assert_eq!(s, StatusCode::OK);
    assert_ne!(v["token"].as_str(), Some("dcap1:STALE"));
    assert_eq!(fakes.source.issued_count(), 1);
}

#[tokio::test]
async fn fund_maps_daemon_outcomes() {
    for (behavior, status, code) in [
        (
            SourceBehavior::SignerExpired,
            StatusCode::CONFLICT,
            ErrorCode::SignerExpired,
        ),
        (
            SourceBehavior::Unavailable,
            StatusCode::BAD_GATEWAY,
            ErrorCode::Upstream,
        ),
        (
            SourceBehavior::Rejected,
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Internal,
        ),
    ] {
        let (state, _) = app_state_with_options(FakeOptions {
            source: behavior,
            ..FakeOptions::default()
        })
        .await;
        let app = sponsord_onramp::http::router(state);
        let (s, v) = send_json(&app, fund()).await;
        assert_eq!(s, status, "{behavior:?}");
        assert_eq!(error_code(&v), code, "{behavior:?}");
        let (s, _) = send_json(&app, poll()).await;
        assert_eq!(s, StatusCode::NO_CONTENT, "nothing held after {behavior:?}");
    }
}

#[tokio::test]
async fn fund_asks_the_daemon_for_the_configured_terms() {
    let mut config = test_config();
    config.spending_cap = Some(MicroUsdc(1_000_000));
    config.ttl_secs = Some(3_600);
    let (state, fakes) = app_state_with_options(FakeOptions {
        config,
        ..FakeOptions::default()
    })
    .await;
    let app = sponsord_onramp::http::router(state);
    assert_eq!(send(&app, fund()).await.0, StatusCode::OK);
    let req = fakes.source.last_request().unwrap();
    assert_eq!(req.signer, client());
    assert_eq!(req.spending_cap, Some(MicroUsdc(1_000_000)));
    assert_eq!(req.ttl_secs, Some(3_600));
}

#[tokio::test]
async fn profile_carries_the_daemon_chain_and_the_configured_contracts() {
    let mut config = test_config();
    config.slash_judge = Some(Address::repeat_byte(0x44));
    config.min_cli_version = Some(semver::Version::new(0, 2, 0));
    let (state, _) = app_state_with_options(FakeOptions {
        config,
        ..FakeOptions::default()
    })
    .await;
    let app = sponsord_onramp::http::router(state);
    let (s, v) = send_json(&app, get("/v1/profile")).await;
    assert_eq!(s, StatusCode::OK);
    let profile: Profile = serde_json::from_value(v).unwrap();
    assert_eq!(
        profile,
        Profile {
            chain_id: 421_614,
            rpc_url: "https://rpc.example".into(),
            payment_pool: Address::repeat_byte(0x22),
            capacity_bond: Address::repeat_byte(0x33),
            slash_judge: Some(Address::repeat_byte(0x44)),
            min_cli_version: Some(semver::Version::new(0, 2, 0)),
        }
    );
}

async fn installer(path: &str) -> String {
    let app = sponsord_onramp::http::router(app_state_with_fakes().await);
    let (s, body) = send(&app, get(path)).await;
    assert_eq!(s, StatusCode::OK);
    String::from_utf8(body).unwrap()
}

/// Both installers name this onramp, download from the pinned GitHub
/// Releases (`test_config`: decdn `v0.1.0`, `decdn-sponsored-v0.2.0`) by tag
/// and SHA256SUMS digest, name the archives by the version the onramp parsed
/// out of each tag, and write only the onramp URL and `decdn` path.
fn assert_installer(body: &str) {
    let quoted = |s: &str| body.contains(&format!("'{s}'")) || body.contains(&format!("\"{s}\""));
    assert!(!body.contains("{{"), "no placeholder should remain");
    assert!(body.contains("https://up.example.org"));
    assert!(body.contains("https://github.com/decdn"));
    assert!(quoted("v0.1.0"), "decdn release tag");
    assert!(quoted("0.1.0"), "decdn version");
    assert!(
        quoted("decdn-sponsored-v0.2.0"),
        "decdn-sponsored release tag"
    );
    assert!(quoted("0.2.0"), "decdn-sponsored version");
    assert!(body.contains(&"ab".repeat(32)), "decdn SHA256SUMS digest");
    assert!(
        body.contains(&"cd".repeat(32)),
        "sponsord SHA256SUMS digest"
    );
    assert!(body.contains("onramp_url = "));
    assert!(body.contains("decdn_bin = "));
    assert!(
        !body.contains("payment_pool"),
        "chain values come from /v1/profile"
    );
}

#[tokio::test]
async fn decdn_sh_is_rendered() {
    let body = installer("/decdn.sh").await;
    assert_installer(&body);
    assert!(body.starts_with("#!/bin/sh"));
}

#[tokio::test]
async fn decdn_ps1_is_rendered() {
    let body = installer("/decdn.ps1").await;
    assert_installer(&body);
    assert!(body.contains("-pc-windows-msvc"));
}

fn from_peer(mut req: Request<Body>, ip: &str) -> Request<Body> {
    let addr: SocketAddr = format!("{ip}:40000").parse().unwrap();
    req.extensions_mut().insert(ConnectInfo(addr));
    req
}

#[tokio::test]
async fn fund_and_poll_are_rate_limited_per_client_address() {
    let mut config = test_config();
    config.fund_rate_per_min = 1;
    config.poll_rate_per_min = 2;
    let (state, fakes) = app_state_with_options(FakeOptions {
        config,
        ..FakeOptions::default()
    })
    .await;
    let app = sponsord_onramp::http::router(state);

    assert_eq!(
        send(&app, from_peer(fund(), "203.0.113.1")).await.0,
        StatusCode::OK
    );
    let (s, v) = send_json(&app, from_peer(fund(), "203.0.113.1")).await;
    assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(error_code(&v), ErrorCode::RateLimited);
    assert_eq!(
        send(&app, from_peer(fund(), "203.0.113.2")).await.0,
        StatusCode::OK
    );
    assert_eq!(
        fakes.gate.last_ip(),
        Some("203.0.113.2".parse().unwrap()),
        "the gate sees the client's address"
    );

    for _ in 0..2 {
        assert_eq!(
            send(&app, from_peer(poll(), "203.0.113.1")).await.0,
            StatusCode::OK
        );
    }
    assert_eq!(
        send(&app, from_peer(poll(), "203.0.113.1")).await.0,
        StatusCode::TOO_MANY_REQUESTS
    );
}

#[tokio::test]
async fn the_client_address_can_come_from_a_proxy_header() {
    let mut config = test_config();
    config.client_ip = ClientIpSource::Header("x-forwarded-for".parse().unwrap());
    config.fund_rate_per_min = 1;
    let (state, fakes) = app_state_with_options(FakeOptions {
        config,
        ..FakeOptions::default()
    })
    .await;
    let app = sponsord_onramp::http::router(state);

    // The proxy appends the real peer; whatever the client put before it is
    // ignored, so a spoofed left-hand address neither names the client nor
    // buys it a fresh rate-limit budget.
    for spoofed in ["192.0.2.1", "192.0.2.2"] {
        let mut req = from_peer(fund(), "10.0.0.1");
        req.headers_mut().insert(
            "x-forwarded-for",
            format!("{spoofed}, 198.51.100.9").parse().unwrap(),
        );
        let status = send(&app, req).await.0;
        assert_eq!(fakes.gate.last_ip(), Some("198.51.100.9".parse().unwrap()));
        if spoofed == "192.0.2.1" {
            assert_eq!(status, StatusCode::OK);
        } else {
            assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        }
    }

    // Without the header, the TCP peer is the client: still limited.
    assert_eq!(
        send(&app, from_peer(fund(), "10.0.0.7")).await.0,
        StatusCode::OK
    );
    assert_eq!(
        send(&app, from_peer(fund(), "10.0.0.7")).await.0,
        StatusCode::TOO_MANY_REQUESTS
    );
}
