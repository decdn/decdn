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
