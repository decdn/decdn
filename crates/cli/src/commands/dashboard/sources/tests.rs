use std::convert::Infallible;

use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use jsonrpsee::types::ErrorObjectOwned;

use super::super::super::activity::{Paid, PullKind, PullRecord, PullState};
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
        state,
        received_bytes: 0,
        downloaded_bytes: 0,
        resumed_bytes: 0,
        reused_bytes: 0,
        expected_bytes: None,
        paid: Paid::default(),
        nodes: Vec::new(),
    }
}

fn write_record(dir: &Path, record: &PullRecord) {
    let path = dir.join(format!("{}-{}.json", record.started_unix_ms, record.pid));
    std::fs::write(path, serde_json::to_vec(record).unwrap()).unwrap();
}

/// Running records are active, the rest recent; a running record whose
/// heartbeat stopped is marked stale.
#[test]
fn pulls_split_running_from_ended() {
    let dir = tempfile::tempdir().unwrap();
    let now = activity::now_ms();
    write_record(dir.path(), &record(now - 3, now, PullState::Running));
    write_record(
        dir.path(),
        &record(now - 2, now - 60_000, PullState::Running),
    );
    let done = PullState::Done {
        finished_unix_ms: now,
    };
    write_record(dir.path(), &record(now - 1, now, done.clone()));
    let section = read_pulls(dir.path());
    assert_eq!(section.active.len(), 2);
    assert_eq!(section.recent.len(), 1);
    let stale: Vec<_> = section.active.iter().map(|v| v.stale).collect();
    assert_eq!(stale, [true, false], "newest first");
    assert_eq!(section.recent[0].record.state, done);
    assert_eq!(section.error, None);
}

/// A file that does not read as a record is named with why, beside the
/// records that do.
#[test]
fn an_unreadable_record_is_named_with_why() {
    let dir = tempfile::tempdir().unwrap();
    let now = activity::now_ms();
    write_record(dir.path(), &record(now, now, PullState::Running));
    let torn = dir.path().join("1-1.json");
    std::fs::write(&torn, br#"{"uri":"decdn:00","#).unwrap();
    let section = read_pulls(dir.path());
    assert_eq!(section.active.len(), 1);
    let [unreadable] = section.unreadable.as_slice() else {
        panic!("one unreadable file: {section:?}");
    };
    assert_eq!(unreadable.path, torn.display().to_string());
    assert!(unreadable.error.contains("EOF"), "{unreadable:?}");
}

/// A directory that cannot be listed says why, rather than reading as no
/// pulls.
#[test]
fn an_unlistable_pulls_directory_says_why() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("file");
    std::fs::write(&file, b"").unwrap();
    let section = read_pulls(&file);
    assert!(section.error.is_some(), "{section:?}");
    assert!(section.active.is_empty() && section.recent.is_empty());
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
    http_client().unwrap()
}

/// A gateway that answers with a gateway's `/_status` body.
fn gateway(_: &Request<hyper::body::Incoming>) -> Response<Body> {
    json(
        StatusCode::OK,
        &serde_json::json!({ "mode": "loopback", "counters": { "requests_ok": 1 } }),
    )
}

fn closed_port() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

/// Nothing listening is no gateway.
#[tokio::test]
async fn a_closed_port_is_no_gateway() {
    let url = Url::parse(&format!("http://{}/_status", closed_port())).unwrap();
    assert!(matches!(
        ask_gateway(&client(), url).await,
        GatewayState::Absent
    ));
}

/// A name that does not resolve is an error, not a gateway that is not
/// running, and the error keeps its cause.
#[tokio::test]
async fn a_name_that_does_not_resolve_is_an_error() {
    let url = Url::parse("http://decdn-dashboard-test.invalid/_status").unwrap();
    match ask_gateway(&client(), url).await {
        GatewayState::Error { message } => assert!(message.contains(':'), "{message}"),
        other => panic!("{other:?}"),
    }
}

/// The gateway's body passes through; any other answer is an error, a
/// refusal included, and so is a body that is no gateway's.
#[tokio::test]
async fn the_gateway_answers_through() {
    let url = serve(gateway).await;
    match ask_gateway(&client(), url).await {
        GatewayState::Up { status } => assert_eq!(status["mode"], "loopback"),
        other => panic!("{other:?}"),
    }

    let refusing = serve(|_| text(StatusCode::UNAUTHORIZED, "token required\n")).await;
    match ask_gateway(&client(), refusing).await {
        GatewayState::Error { message } => assert!(message.contains("401"), "{message}"),
        other => panic!("{other:?}"),
    }

    let other = serve(|_| json(StatusCode::OK, &serde_json::json!({ "hello": 1 }))).await;
    assert!(matches!(
        ask_gateway(&client(), other).await,
        GatewayState::Error { .. }
    ));
}

fn sources() -> Sources {
    Sources::new(&Targets {
        pulls_dir: None,
        gateway: Url::parse("http://127.0.0.1:8484").unwrap(),
        admin_url: "http://127.0.0.1:9191".to_owned(),
        metrics_url: "http://127.0.0.1:9090/metrics".to_owned(),
    })
}

/// One ask records what the gateway answered and when.
#[tokio::test]
async fn a_tick_records_the_answer() {
    let url = serve(gateway).await;
    let sources = sources();
    gateway_tick(&sources, &client(), url).await;
    let section = sources.gateway.read().unwrap();
    assert!(matches!(section.state, GatewayState::Up { .. }));
    assert!(section.updated_unix_ms.is_some());
}

fn call<T>(code: i32) -> Result<T, JsonRpcClientError> {
    Err(JsonRpcClientError::Call(ErrorObjectOwned::owned::<()>(
        code,
        "not wired",
        None,
    )))
}

/// An admin error code that means the node runs without the feature is
/// `unconfigured`; any other error is an error.
#[test]
fn a_missing_feature_is_unconfigured_not_an_error() {
    for code in [
        BUYER_POOL_UNAVAILABLE_CODE,
        EARNINGS_UNAVAILABLE_CODE,
        DHT_UNAVAILABLE_CODE,
    ] {
        let part = part::<()>("http://127.0.0.1:9191", FAST_TIMEOUT, call(code));
        assert!(
            matches!(part.read(), Read::Unconfigured { message } if message == "not wired"),
            "{code}"
        );
    }
    let part = part::<()>("http://127.0.0.1:9191", FAST_TIMEOUT, call(-32_007));
    assert!(matches!(part.read(), Read::Error { .. }));
}

/// A refused health call is no node; any other failure is a node in error,
/// with why; an answer is a node that is up.
#[tokio::test]
async fn the_node_state_follows_the_health_call() {
    let url = format!("http://{}", closed_port());
    let admin = admin_client(&url, FAST_TIMEOUT).unwrap();
    let health = admin.health().await;
    let refused = matches!(
        &health,
        Err(JsonRpcClientError::Transport(inner)) if is_connection_refused(inner.as_ref())
    );
    assert!(refused, "{health:?}");
    let health = part(&url, FAST_TIMEOUT, health);
    assert_eq!(node_state(&health, refused), NodeState::Absent);

    let failed = part::<()>(&url, FAST_TIMEOUT, call(-32_000));
    assert!(matches!(
        node_state(&failed, false),
        NodeState::Error { ref message } if message.contains("-32000")
    ));
    assert_eq!(
        node_state(&Part::now(Read::Ok { value: () }), false),
        NodeState::Up
    );
    assert_eq!(
        node_state(&Part::<()>::waiting(), false),
        NodeState::Waiting
    );
}
