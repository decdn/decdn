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

/// A gateway that answers with the gateway's body when the token is `tok`.
fn token_gateway(req: &Request<hyper::body::Incoming>) -> Response<Body> {
    if req.headers().get(TOKEN_HEADER).is_some_and(|t| t == "tok") {
        json(
            StatusCode::OK,
            &serde_json::json!({ "mode": "loopback", "counters": { "requests_ok": 1 } }),
        )
    } else {
        text(StatusCode::UNAUTHORIZED, "token required\n")
    }
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
        ask_gateway(&client(), url, None).await,
        GatewayState::Absent
    ));
}

/// A name that does not resolve is an error, not a gateway that is not
/// running, and the error keeps its cause.
#[tokio::test]
async fn a_name_that_does_not_resolve_is_an_error() {
    let url = Url::parse("http://decdn-dashboard-test.invalid/_status").unwrap();
    match ask_gateway(&client(), url, None).await {
        GatewayState::Error { message } => assert!(message.contains(':'), "{message}"),
        other => panic!("{other:?}"),
    }
}

/// The gateway's body passes through when the token is right; a refusal is
/// `unauthorized`, and a body that is no gateway's is an error.
#[tokio::test]
async fn the_gateway_answers_through_the_token() {
    let url = serve(token_gateway).await;
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

/// A redirect is not followed, so the token never reaches the host it names.
#[tokio::test]
async fn a_redirect_is_not_followed() {
    let url = serve(|_| {
        let mut response = text(StatusCode::FOUND, "");
        response.headers_mut().insert(
            hyper::header::LOCATION,
            hyper::header::HeaderValue::from_static("http://elsewhere.invalid/_status"),
        );
        response
    })
    .await;
    match ask_gateway(&client(), url, Some("tok")).await {
        GatewayState::Error { message } => assert!(message.contains("302"), "{message}"),
        other => panic!("{other:?}"),
    }
}

fn sources() -> Sources {
    Sources::new(&Targets {
        pulls_dir: None,
        gateway: Url::parse("http://127.0.0.1:8484").unwrap(),
        gateway_token_file: PathBuf::from("/nonexistent/token"),
        gateway_token_named: false,
        admin_url: "http://127.0.0.1:9191".to_owned(),
        metrics_url: "http://127.0.0.1:9090/metrics".to_owned(),
    })
}

/// Each ask reads the token file anew: a gateway that writes its token after
/// the dashboard started is read on the next ask.
#[tokio::test]
async fn each_ask_reads_the_token_anew() {
    let url = serve(token_gateway).await;
    let dir = tempfile::tempdir().unwrap();
    let token = GatewayToken {
        file: dir.path().join("token"),
        named: false,
    };
    let sources = sources();
    gateway_tick(&sources, &client(), url.clone(), &token).await;
    assert!(matches!(
        sources.gateway.read().unwrap().state,
        GatewayState::Unauthorized
    ));
    std::fs::write(&token.file, "tok\n").unwrap();
    gateway_tick(&sources, &client(), url, &token).await;
    assert!(matches!(
        sources.gateway.read().unwrap().state,
        GatewayState::Up { .. }
    ));
    assert!(sources.gateway.read().unwrap().updated_unix_ms.is_some());
}

/// A token file the operator named and that cannot be read is an error that
/// names it, not a gateway that wants a token.
#[tokio::test]
async fn a_named_token_file_that_cannot_be_read_says_so() {
    let url = serve(token_gateway).await;
    let dir = tempfile::tempdir().unwrap();
    let token = GatewayToken {
        file: dir.path().join("missing"),
        named: true,
    };
    let sources = sources();
    gateway_tick(&sources, &client(), url, &token).await;
    match &sources.gateway.read().unwrap().state {
        GatewayState::Error { message } => assert!(message.contains("missing"), "{message}"),
        other => panic!("{other:?}"),
    }
}

/// The token goes to this host only.
#[test]
fn the_token_goes_to_loopback_only() {
    for url in [
        "http://127.0.0.1:8484",
        "http://localhost:8484",
        "http://[::1]:8484",
    ] {
        assert!(is_loopback(&Url::parse(url).unwrap()), "{url}");
    }
    for url in [
        "http://gw.example",
        "http://10.0.0.1:8484",
        "http://127.example",
    ] {
        assert!(!is_loopback(&Url::parse(url).unwrap()), "{url}");
    }
}

#[test]
fn the_token_file_is_read_trimmed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    assert_eq!(read_token(&path, false), Ok(None));
    assert!(read_token(&path, true).is_err());
    std::fs::write(&path, "\n").unwrap();
    assert_eq!(read_token(&path, true), Ok(None));
    std::fs::write(&path, "abc\n").unwrap();
    assert_eq!(read_token(&path, false), Ok(Some("abc".to_owned())));
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
        SLASH_DETECTION_UNAVAILABLE_CODE,
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
