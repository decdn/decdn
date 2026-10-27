use hyper::header::{self, HeaderValue};
use url::Url;

use super::*;

const TOKEN: &str = "ab";

fn dashboard() -> Dashboard {
    let targets = Targets {
        pulls_dir: None,
        gateway: Url::parse("http://127.0.0.1:8484").unwrap(),
        gateway_token_file: std::path::PathBuf::from("/nonexistent/token"),
        gateway_token_named: false,
        admin_url: "http://127.0.0.1:9191".to_owned(),
        metrics_url: "http://127.0.0.1:9090/metrics".to_owned(),
    };
    Dashboard {
        token: TOKEN.to_owned(),
        hosts: allowed_hosts("127.0.0.1:8485".parse().unwrap()),
        sources: Arc::new(Sources::new(&targets)),
        started_unix: 7,
    }
}

fn headers(host: &str, token: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(header::HOST, HeaderValue::from_str(host).unwrap());
    if let Some(token) = token {
        headers.insert(
            super::super::local_http::TOKEN_HEADER,
            HeaderValue::from_str(token).unwrap(),
        );
    }
    headers
}

fn get(path: &str, token: Option<&str>) -> Response<Body> {
    dashboard().answer(&Method::GET, path, &headers("127.0.0.1:8485", token))
}

/// `/_status` takes the dashboard's token, and answers every section, each
/// waiting for its first read.
#[test]
fn status_needs_the_token() {
    assert_eq!(get("/_status", None).status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        get("/_status", Some("ac")).status(),
        StatusCode::UNAUTHORIZED
    );
    let answer = get("/_status", Some(TOKEN));
    assert_eq!(answer.status(), StatusCode::OK);
    let status = serde_json::to_value(dashboard().status()).unwrap();
    assert_eq!(status["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(status["started_unix"], 7);
    assert_eq!(status["gateway"]["state"], "waiting");
    assert_eq!(status["node"]["state"], "waiting");
    assert_eq!(status["node"]["lanes"]["state"], "waiting");
    assert!(status["pulls"]["active"].as_array().unwrap().is_empty());
}

/// The page and its assets answer with no token: the page asks for it.
#[test]
fn the_page_and_its_assets_answer_without_the_token() {
    for path in ["/", "/_dashboard/app.js", "/_dashboard/app.css"] {
        assert_eq!(get(path, None).status(), StatusCode::OK, "{path}");
    }
    let page = get("/", None);
    assert!(
        page.headers()
            .get(header::CONTENT_SECURITY_POLICY)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|csp| csp.contains("connect-src 'self'")),
        "{:?}",
        page.headers()
    );
}

/// A name rebound to the listener's address reads nothing, and nothing but
/// `GET` of a known path answers.
#[test]
fn only_its_own_host_and_routes_answer() {
    let d = dashboard();
    for path in ["/", "/_status"] {
        let rebound = d.answer(
            &Method::GET,
            path,
            &headers("evil.example:8485", Some(TOKEN)),
        );
        assert_eq!(rebound.status(), StatusCode::FORBIDDEN, "{path}");
    }
    let named = d.answer(&Method::GET, "/", &headers("localhost:8485", None));
    assert_eq!(named.status(), StatusCode::OK);
    assert_eq!(get("/nope", Some(TOKEN)).status(), StatusCode::NOT_FOUND);
    for method in [Method::OPTIONS, Method::POST, Method::PUT] {
        let answer = d.answer(&method, "/_status", &headers("127.0.0.1:8485", Some(TOKEN)));
        assert_eq!(answer.status(), StatusCode::NOT_FOUND, "{method}");
        assert!(
            answer
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none()
        );
    }
}
