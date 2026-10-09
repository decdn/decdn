use serde_json::json;
use wiremock::matchers::{body_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;
use crate::{IssuedCapability, MicroUsdc};

const SIGNER: Address = Address::repeat_byte(0xaa);

fn daemon(base: &str) -> DaemonClient {
    DaemonClient::new(base, "tok".into(), reqwest::Client::new())
}

async fn mock_issue(status: u16, body: serde_json::Value) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(daemon::routes::CAPABILITIES))
        .respond_with(ResponseTemplate::new(status).set_body_json(body))
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn issue_sends_bearer_and_terms_and_parses_the_response() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(daemon::routes::CAPABILITIES))
        .and(header("authorization", "Bearer tok"))
        .and(body_json(json!({
            "signer": SIGNER,
            "spending_cap": 1_000_000,
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "token": "dcap1:X", "spending_cap": 1_000_000, "expiry": 99, "registered": false
        })))
        .mount(&server)
        .await;
    let req = IssueRequest {
        spending_cap: Some(MicroUsdc(1_000_000)),
        ..IssueRequest::new(SIGNER)
    };
    let resp = daemon(&server.uri()).issue(&req).await.unwrap();
    assert_eq!(
        resp,
        IssueResponse {
            capability: IssuedCapability {
                token: "dcap1:X".into(),
                spending_cap: MicroUsdc(1_000_000),
                expiry: 99,
            },
            registered: false,
        }
    );
}

#[tokio::test]
async fn trailing_slash_in_base_url_is_tolerated() {
    let server = mock_issue(
        200,
        json!({"token": "dcap1:X", "spending_cap": 1, "expiry": 2, "registered": true}),
    )
    .await;
    let base = format!("{}/", server.uri());
    assert!(
        daemon(&base)
            .issue(&IssueRequest::new(SIGNER))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn signer_expired_maps_to_its_own_error() {
    let server = mock_issue(409, json!({"error": "signer_expired", "expiry": 1_000})).await;
    let err = daemon(&server.uri())
        .issue(&IssueRequest::new(SIGNER))
        .await
        .unwrap_err();
    assert!(matches!(err, DaemonError::SignerExpired { expiry: 1_000 }));
}

#[tokio::test]
async fn signer_expired_outside_a_409_is_not_a_verdict_on_the_signer() {
    let server = mock_issue(500, json!({"error": "signer_expired", "expiry": 1_000})).await;
    let err = daemon(&server.uri())
        .issue(&IssueRequest::new(SIGNER))
        .await
        .unwrap_err();
    assert!(matches!(err, DaemonError::Unavailable(_)), "{err}");
}

#[test]
fn debug_leaves_the_token_out() {
    let shown = format!("{:?}", daemon("http://d"));
    assert!(!shown.contains("tok\""), "{shown}");
    assert!(shown.contains("<redacted>"));
}

#[tokio::test]
async fn client_errors_are_rejections() {
    for (status, code) in [(400, ErrorCode::ExceedsMax), (401, ErrorCode::Unauthorized)] {
        let server = mock_issue(status, json!({"error": code})).await;
        let err = daemon(&server.uri())
            .issue(&IssueRequest::new(SIGNER))
            .await
            .unwrap_err();
        assert!(
            matches!(err, DaemonError::Rejected { status: s, code: c } if s == status && c == code),
            "{err}"
        );
    }
}

#[tokio::test]
async fn server_errors_and_refused_connections_are_unavailable() {
    let server = mock_issue(503, json!({"error": "chain_unavailable"})).await;
    let err = daemon(&server.uri())
        .issue(&IssueRequest::new(SIGNER))
        .await
        .unwrap_err();
    assert!(matches!(err, DaemonError::Unavailable(_)));

    let err = daemon("http://127.0.0.1:1")
        .issue(&IssueRequest::new(SIGNER))
        .await
        .unwrap_err();
    assert!(matches!(err, DaemonError::Unavailable(_)));
}

#[tokio::test]
async fn a_hung_daemon_times_out_as_unavailable() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
        .mount(&server)
        .await;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    let c = DaemonClient::new(&server.uri(), "tok".into(), http);
    let err = c.issue(&IssueRequest::new(SIGNER)).await.unwrap_err();
    assert!(matches!(err, DaemonError::Unavailable(_)));
}

#[tokio::test]
async fn info_parses() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(daemon::routes::INFO))
        .and(header("authorization", "Bearer tok"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "chain_id": 421_614,
            "payment_pool": Address::repeat_byte(0x22),
            "max_spending_cap": 5_000_000,
            "max_ttl_secs": 172_800,
        })))
        .mount(&server)
        .await;
    assert_eq!(
        daemon(&server.uri()).info().await.unwrap(),
        Info {
            chain_id: 421_614,
            payment_pool: Address::repeat_byte(0x22),
            max_spending_cap: MicroUsdc(5_000_000),
            max_ttl_secs: 172_800,
        }
    );
}

fn onramp(base: &str) -> OnrampClient {
    OnrampClient::new(base, reqwest::Client::new())
}

#[tokio::test]
async fn capability_maps_204_to_none_and_200_to_the_token() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(onramp::routes::CAPABILITY))
        .and(query_param("client", SIGNER.to_string()))
        .respond_with(ResponseTemplate::new(204))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(onramp::routes::CAPABILITY))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"token": "dcap1:A"})))
        .mount(&server)
        .await;
    let c = onramp(&server.uri());
    assert_eq!(c.capability(SIGNER).await.unwrap(), None);
    assert_eq!(
        c.capability(SIGNER).await.unwrap().as_deref(),
        Some("dcap1:A")
    );
}

#[tokio::test]
async fn poll_waits_for_the_token_and_times_out_without_one() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(onramp::routes::CAPABILITY))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    let err = onramp(&server.uri())
        .poll_capability(SIGNER, Duration::from_millis(10), Duration::from_millis(50))
        .await
        .unwrap_err();
    assert!(matches!(err, OnrampError::Timeout(_)));
}

#[tokio::test]
async fn poll_timeout_bounds_a_request_in_flight() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(onramp::routes::CAPABILITY))
        .respond_with(ResponseTemplate::new(204).set_delay(Duration::from_secs(30)))
        .mount(&server)
        .await;
    let started = std::time::Instant::now();
    let err = onramp(&server.uri())
        .poll_capability(
            SIGNER,
            Duration::from_millis(10),
            Duration::from_millis(200),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, OnrampError::Timeout(_)));
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn other_statuses_carry_the_error_code() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(onramp::routes::CAPABILITY))
        .respond_with(ResponseTemplate::new(429).set_body_json(json!({"error": "rate_limited"})))
        .mount(&server)
        .await;
    let err = onramp(&server.uri()).capability(SIGNER).await.unwrap_err();
    assert!(matches!(
        err,
        OnrampError::Status {
            status: 429,
            code: ErrorCode::RateLimited,
            ..
        }
    ));
}

#[tokio::test]
async fn profile_parses() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(onramp::routes::PROFILE))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "chain_id": 421_614,
            "rpc_url": "https://rpc.example",
            "payment_pool": Address::repeat_byte(0x01),
            "capacity_bond": Address::repeat_byte(0x02),
            "min_cli_version": "0.2.0",
        })))
        .mount(&server)
        .await;
    let p = onramp(&server.uri()).profile().await.unwrap();
    assert_eq!(p.payment_pool, Address::repeat_byte(0x01));
    assert_eq!(p.slash_judge, None);
    assert_eq!(p.min_cli_version, Some(semver::Version::new(0, 2, 0)));
}

#[test]
fn fund_url_names_the_client() {
    let c = onramp("https://s.example/");
    assert_eq!(
        c.fund_url(SIGNER),
        format!("https://s.example/fund?client={SIGNER}")
    );
}
