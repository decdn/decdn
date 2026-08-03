//! Contract tests for `/fund`, `/channel`, `/topup` over `sponsord`'s
//! injectable fakes (`sponsord::test_support`). Runs as a plain
//! `cargo test -p sponsord` integration test — no feature flag needed.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use sponsord::money::MicroUsdc;
use sponsord::test_support::{FakeOptions, app_state_with_fakes, app_state_with_options};

const CLIENT: &str = "0x00000000000000000000000000000000000000aa";
const HASH: &str = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";

async fn json_body(resp: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    serde_json::from_slice(&bytes).expect("parse json")
}

fn fund_body() -> String {
    serde_json::json!({
        "client": CLIENT,
        "hash": HASH,
        "turnstile_token": "ok",
    })
    .to_string()
}

#[tokio::test]
async fn fund_then_channel_then_topup_happy_path() {
    let state = app_state_with_fakes();
    let app = sponsord::http::router(state.clone());

    // POST /fund opens a fresh channel.
    let resp = app
        .clone()
        .oneshot(
            Request::post("/fund")
                .header("content-type", "application/json")
                .body(Body::from(fund_body()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;
    let channel_id = v["channel_id"].as_str().expect("channel_id").to_string();
    assert!(v["provider"].as_str().is_some());
    assert!(v["node_id"].as_str().is_some());

    // GET /channel finds the same channel.
    let resp = app
        .clone()
        .oneshot(
            Request::get(format!("/channel?client={CLIENT}&hash={HASH}"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_body(resp).await;
    assert_eq!(v["channel_id"].as_str().expect("channel_id"), channel_id);

    // A second /fund is idempotent: same channel_id, no new open.
    let resp = app
        .clone()
        .oneshot(
            Request::post("/fund")
                .header("content-type", "application/json")
                .body(Body::from(fund_body()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(resp.status(), StatusCode::OK);
    let v2 = json_body(resp).await;
    assert_eq!(v2["channel_id"].as_str().expect("channel_id"), channel_id);

    // POST /topup with a bad signature exercises the 401 auth path (the
    // fakes have no real signer for the stored channel's client, so the
    // success path for topup is covered at the unit level by
    // `topup_auth::verify`'s own tests; here we confirm the handler wiring
    // rejects an unauthenticated request rather than silently succeeding).
    let bad_sig = format!("0x{}", "11".repeat(65));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let topup_body = serde_json::json!({
        "channel_id": channel_id,
        "timestamp": now,
        "signature": bad_sig,
    })
    .to_string();
    let resp = app
        .clone()
        .oneshot(
            Request::post("/topup")
                .header("content-type", "application/json")
                .body(Body::from(topup_body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // An unknown channel id 404s.
    let unknown = format!("0x{}", "22".repeat(32));
    let topup_body = serde_json::json!({
        "channel_id": unknown,
        "timestamp": now,
        "signature": bad_sig,
    })
    .to_string();
    let resp = app
        .oneshot(
            Request::post("/topup")
                .header("content-type", "application/json")
                .body(Body::from(topup_body))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fund_rejects_bad_captcha_with_403() {
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
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let v = json_body(resp).await;
    assert_eq!(v["error"].as_str(), Some("captcha_failed"));
}

#[tokio::test]
async fn fund_rejects_over_cap_with_429() {
    let state = app_state_with_options(FakeOptions {
        captcha_passes: true,
        monthly_cap: MicroUsdc(1), // below initial_deposit (2_000_000)
    });
    let app = sponsord::http::router(state);

    let resp = app
        .oneshot(
            Request::post("/fund")
                .header("content-type", "application/json")
                .body(Body::from(fund_body()))
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    let v = json_body(resp).await;
    assert_eq!(v["error"].as_str(), Some("cap_exhausted"));
}
