use std::convert::Infallible;
use std::path::Path;

use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use jsonrpsee::types::ErrorObjectOwned;

use super::super::super::activity::{Paid, PullKind, PullRecord};
use super::super::super::local_http::{Body, json, text};
use super::*;

fn record(started: u64, updated: u64, state: PullState) -> PullRecord {
    PullRecord {
        uri: "decdn:00".to_owned(),
        kind: PullKind::Blob,
        output: "out".to_owned(),
        pid: 1,
        started_unix_ms: started,
        updated_unix_ms: updated,
        finished_unix_ms: None,
        state,
        received_bytes: 0,
        expected_bytes: None,
        files_started: 0,
        files_landed: 0,
        paid: Paid::default(),
        nodes: Vec::new(),
        bundle: None,
        error: None,
    }
}

fn write(dir: &Path, record: &PullRecord) {
    let path = dir.join(format!("{}-{}.json", record.started_unix_ms, record.pid));
    std::fs::write(path, serde_json::to_vec(record).unwrap()).unwrap();
}

/// Running records are active, the rest recent; a running record whose
/// heartbeat stopped is marked stale.
#[test]
fn pulls_split_running_from_ended() {
    let dir = tempfile::tempdir().unwrap();
    let now = activity::now_ms();
    write(dir.path(), &record(now - 3, now, PullState::Running));
    write(
        dir.path(),
        &record(now - 2, now - 60_000, PullState::Running),
    );
    write(dir.path(), &record(now - 1, now, PullState::Done));
    let section = read_pulls(dir.path());
    assert_eq!(section.active.len(), 2);
    assert_eq!(section.recent.len(), 1);
    let stale: Vec<_> = section.active.iter().map(|v| v.stale).collect();
    assert_eq!(stale, [true, false], "newest first");
    assert_eq!(section.recent[0].record.state, PullState::Done);
}

/// Serve `answer` for every request on a loopback port; its address.
async fn serve(answer: fn(&Request<hyper::body::Incoming>) -> Response<Body>) -> Url {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let service =
                    service_fn(move |req| async move { Ok::<_, Infallible>(answer(&req)) });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    Url::parse(&format!("http://{addr}/_status")).unwrap()
}

fn client() -> reqwest::Client {
    decdn_common::tls::install_crypto_provider();
    reqwest::Client::new()
}

/// Nothing listening is no gateway.
#[tokio::test]
async fn a_closed_port_is_no_gateway() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let url = Url::parse(&format!("http://{addr}/_status")).unwrap();
    assert!(matches!(
        ask_gateway(&client(), url, None).await,
        GatewayState::Absent
    ));
}

/// The gateway's body passes through when the token is right; a refusal is
/// `unauthorized`, and a body that is no gateway's is an error.
#[tokio::test]
async fn the_gateway_answers_through_the_token() {
    let url = serve(|req| {
        if req.headers().get(TOKEN_HEADER).is_some_and(|t| t == "tok") {
            json(
                StatusCode::OK,
                &serde_json::json!({ "mode": "loopback", "counters": { "requests_ok": 1 } }),
            )
        } else {
            text(StatusCode::UNAUTHORIZED, "token required\n")
        }
    })
    .await;
    match ask_gateway(&client(), url.clone(), Some("tok")).await {
        GatewayState::Up { status } => assert_eq!(status["mode"], "loopback"),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        ask_gateway(&client(), url, None).await,
        GatewayState::Unauthorized
    ));

    let other = serve(|_| json(StatusCode::OK, &serde_json::json!({ "hello": 1 }))).await;
    assert!(matches!(
        ask_gateway(&client(), other, None).await,
        GatewayState::Error { .. }
    ));
}

/// An admin error code that means the node runs without the feature is
/// `unconfigured`; any other error is an error.
#[test]
fn a_missing_feature_is_unconfigured_not_an_error() {
    let call = |code| {
        Err::<(), _>(JsonRpcClientError::Call(ErrorObjectOwned::owned::<()>(
            code,
            "not wired",
            None,
        )))
    };
    for code in [
        SLASH_DETECTION_UNAVAILABLE_CODE,
        BUYER_POOL_UNAVAILABLE_CODE,
        EARNINGS_UNAVAILABLE_CODE,
        DHT_UNAVAILABLE_CODE,
    ] {
        let part = part("http://127.0.0.1:9191", FAST_TIMEOUT, call(code));
        assert!(
            matches!(part.read, Read::Unconfigured { ref message } if message == "not wired"),
            "{code}"
        );
    }
    let part = part("http://127.0.0.1:9191", FAST_TIMEOUT, call(-32_007));
    assert!(matches!(part.read, Read::Error { .. }));
    assert!(part.updated_unix_ms.is_some());
}

/// The gateway token file is read trimmed, and an empty or missing one sends
/// no token.
#[test]
fn the_gateway_token_is_read_trimmed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    assert_eq!(read_token(&path), None);
    std::fs::write(&path, "\n").unwrap();
    assert_eq!(read_token(&path), None);
    std::fs::write(&path, "abc\n").unwrap();
    assert_eq!(read_token(&path).as_deref(), Some("abc"));
}
