//! Direct tests for `is_permanent_scrape_error`. The classifier
//! is the only thing standing between a misconfigured
//! `--metrics-url` and an indefinite retry loop, so a future
//! refactor that breaks the predicate (e.g. changes the
//! `is_builder()` arm to `is_request()`, or drops the sentinel
//! match) needs to fail here rather than going unnoticed.
use super::*;

/// Build an `anyhow::Error` from a real reqwest send-error so
/// the chain shape matches what `fetch_metrics` would produce at
/// runtime (rather than a hand-constructed mock that could diverge
/// from reqwest's internal error structure).
async fn reqwest_send_error(url: &str) -> anyhow::Error {
    let err = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .expect_err("test url must fail to send");
    anyhow::Error::from(err).context(format!("GET {url} failed"))
}

#[tokio::test]
async fn recognises_builder_error_as_permanent() {
    // "not a url" produces a reqwest::Error::is_builder() == true.
    let err = reqwest_send_error("not a url").await;
    assert!(
        is_permanent_scrape_error(&err),
        "URL parse error must be classed as permanent: {err:#}"
    );
}

#[tokio::test]
async fn does_not_class_connection_refused_as_permanent() {
    // Port 1 is reserved + nothing listens; the error is a
    // transport-level connect failure, not a builder error.
    // Must be left for the consecutive-failure budget to
    // handle (transient — daemon may come back).
    let err = reqwest_send_error("http://127.0.0.1:1/metrics").await;
    assert!(
        !is_permanent_scrape_error(&err),
        "connection refused must NOT be classed as permanent: {err:#}"
    );
}

#[test]
fn recognises_permanent_status_tag_in_chain() {
    // fetch_metrics tags persistent 4xx with this sentinel
    // string. A regression that drops the tag (or changes the
    // string) must fail here.
    let err = anyhow::anyhow!("GET http://h:9090/metrics returned HTTP 404 Not Found")
        .context(PERMANENT_STATUS_TAG);
    assert!(
        is_permanent_scrape_error(&err),
        "PERMANENT_STATUS_TAG must be recognised: {err:#}"
    );
}

#[test]
fn does_not_class_unrelated_anyhow_as_permanent() {
    // A bare anyhow error with no reqwest in the chain and no
    // sentinel tag must be transient. Guards against a too-broad
    // predicate that classes everything as permanent.
    let err = anyhow::anyhow!("something else went wrong");
    assert!(
        !is_permanent_scrape_error(&err),
        "unrelated error must NOT be classed as permanent: {err:#}"
    );
}
