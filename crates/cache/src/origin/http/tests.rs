use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;
use crate::origin::log_capture::LogBuffer;

/// A published sibling `{hex}.obao4` is fetched in full via
/// `fetch_outboard` alone (no data GET at all, #1130 stream-while-store
/// seam).
#[tokio::test]
async fn fetch_outboard_returns_sibling_obao4() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-outboard-marker");
    let hex = hash.to_hex();
    let outboard_bytes = vec![0xABu8; 4096];
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(outboard_bytes.clone()))
        .mount(&server)
        .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    match origin.fetch_outboard(hash, 1 << 20).await? {
        OutboardFetch::Found(bytes) => {
            anyhow::ensure!(
                bytes.as_ref() == outboard_bytes.as_slice(),
                "outboard bytes mismatch"
            );
        }
        other => anyhow::bail!("expected Found, got {other:?}"),
    }
    Ok(())
}

/// A `404` on the sibling outboard is `NotFound`, not an error.
#[tokio::test]
async fn fetch_outboard_404_is_not_found() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-outboard-missing-marker");
    let hex = hash.to_hex();
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    anyhow::ensure!(
        matches!(
            origin.fetch_outboard(hash, 1 << 20).await?,
            OutboardFetch::NotFound
        ),
        "404 must be NotFound",
    );
    Ok(())
}

/// A deterministic decline (403 here) degrades to `Unsupported` rather than
/// erroring — the optimization is best-effort. Among non-404 statuses, only
/// the retriable ones (5xx/408/429) fault; see
/// `fetch_outboard_5xx_is_a_transient_fault_not_a_decline`.
#[tokio::test]
async fn fetch_outboard_non_404_decline_is_unsupported() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-outboard-forbidden-marker");
    let hex = hash.to_hex();
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    anyhow::ensure!(
        matches!(
            origin.fetch_outboard(hash, 1 << 20).await?,
            OutboardFetch::Unsupported
        ),
        "a 403 decline must degrade to Unsupported",
    );
    Ok(())
}

/// An outboard GET 503 is a transient fault, never a decline (#2826); see
/// `get_outboard_bounded`.
#[tokio::test]
async fn fetch_outboard_5xx_is_a_transient_fault_not_a_decline() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-outboard-outage-marker");
    let hex = hash.to_hex();
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    match origin.fetch_outboard(hash, 1 << 20).await {
        Err(err) => anyhow::ensure!(
            err.is_transient(),
            "a 503 must classify as Transient; got a permanent fault: {err}"
        ),
        Ok(fetch) => anyhow::bail!("a 503 must fault, got Ok({fetch:?})"),
    }
    Ok(())
}

/// Each outboard-decline status class warns once per origin: a repeat of the
/// class logs at debug, a new class warns again, and clones share the state.
#[tokio::test]
async fn outboard_decline_warns_once_per_status_class() -> anyhow::Result<()> {
    let origin = HttpOrigin::parse("http://127.0.0.1:1")?;
    let clone = origin.clone();
    let steps = [
        (&origin, StatusCode::FORBIDDEN, true),
        (&origin, StatusCode::FORBIDDEN, false),
        (&clone, StatusCode::FORBIDDEN, false),
        (&clone, StatusCode::UNAUTHORIZED, true),
        (&origin, StatusCode::FOUND, true),
        (&origin, StatusCode::TEMPORARY_REDIRECT, false),
        (&origin, StatusCode::GONE, true),
        (&clone, StatusCode::METHOD_NOT_ALLOWED, false),
    ];
    for (o, status, want) in steps {
        anyhow::ensure!(
            o.first_outboard_decline(status) == want,
            "first_outboard_decline({status}) must be {want}",
        );
    }
    Ok(())
}

/// The first outboard decline of a status class logs at warn and a repeat logs
/// at debug. The bit logic alone cannot catch swapped `warn!`/`debug!` arms
/// (#2856), so this pins the levels in the formatted output.
#[test]
fn outboard_decline_logs_warn_once_then_debug() -> anyhow::Result<()> {
    let logs = LogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    tracing::subscriber::with_default(subscriber, || {
        let _guard = runtime.enter();
        let origin = HttpOrigin::parse("http://127.0.0.1:1")?;
        origin.log_outboard_decline("http://127.0.0.1:1/h.obao4", StatusCode::FORBIDDEN);
        origin.log_outboard_decline("http://127.0.0.1:1/h.obao4", StatusCode::FORBIDDEN);
        anyhow::Ok(())
    })?;

    let text = logs.contents();
    let lines: Vec<&str> = text
        .lines()
        .filter(|l| l.contains("declined the outboard GET"))
        .collect();
    anyhow::ensure!(
        matches!(
            lines.as_slice(),
            [first, second] if first.contains("WARN") && second.contains("DEBUG")
        ),
        "expected one WARN then one DEBUG decline line:\n{text}"
    );
    Ok(())
}

/// A ranged-data GET 503 is a transient fault, never a decline: a decline
/// reads as an origin that cannot serve ranges and hides the outage behind a
/// whole-blob pull. See `get_range_bytes`.
#[tokio::test]
async fn fetch_range_data_5xx_is_a_transient_fault_not_a_decline() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-range-outage-marker");
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let origin = HttpOrigin::parse(&server.uri())?;
    let req = OriginRangeRequest {
        fetch_start: 0,
        fetch_end: 1024,
    };

    match origin.fetch_range_data(hash, req).await {
        Err(err) => anyhow::ensure!(
            err.is_transient(),
            "a 503 must classify as Transient; got a permanent fault: {err}"
        ),
        Ok(fetch) => anyhow::bail!("a 503 must fault, got Ok({fetch:?})"),
    }
    Ok(())
}

/// A ranged-data GET 404 is a decline, not a fault: the range path degrades
/// to a whole-blob pull, which answers the absence authoritatively.
#[tokio::test]
async fn fetch_range_data_404_is_unsupported() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-range-missing-marker");
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let origin = HttpOrigin::parse(&server.uri())?;
    let req = OriginRangeRequest {
        fetch_start: 0,
        fetch_end: 1024,
    };

    anyhow::ensure!(
        matches!(
            origin.fetch_range_data(hash, req).await?,
            OriginRangeFetch::Unsupported
        ),
        "a 404 range GET must degrade to Unsupported",
    );
    Ok(())
}

/// 5xx, 408 and 429 are retriable; every other status is not. Shared by the
/// whole-blob GET, the size HEAD, the ranged-data GET and the outboard GET.
#[test]
fn is_transient_status_classification_matches_rfc_9110() -> anyhow::Result<()> {
    for code in [500, 503, 599, 408, 429] {
        let status = StatusCode::from_u16(code)?;
        anyhow::ensure!(is_transient_status(status), "{code} must be transient");
    }
    for code in [400, 401, 403, 404, 409, 200, 304] {
        let status = StatusCode::from_u16(code)?;
        anyhow::ensure!(!is_transient_status(status), "{code} must not be transient");
    }
    Ok(())
}

/// An outboard whose advertised `Content-Length` exceeds
/// `outboard_max_bytes` degrades to `Unsupported` rather than buffering —
/// the OOM guard on the one outboard read a range pull makes.
#[tokio::test]
async fn fetch_outboard_oversize_is_unsupported() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-outboard-oversize-marker");
    let hex = hash.to_hex();
    let huge = vec![0x5Au8; 8192];
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(huge))
        .mount(&server)
        .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    anyhow::ensure!(
        matches!(
            origin.fetch_outboard(hash, 1024).await?,
            OutboardFetch::Unsupported
        ),
        "oversize outboard must degrade to Unsupported",
    );
    Ok(())
}

/// A `HEAD` 404 is an authoritative absence: `size` answers
/// [`OriginSize::Absent`] so the probe chain reads it as `Absent`.
#[tokio::test]
async fn size_404_is_absent() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-size-missing-marker");
    Mock::given(method("HEAD"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    anyhow::ensure!(
        origin.size(hash).await? == OriginSize::Absent,
        "a 404 must answer Absent, not error",
    );
    Ok(())
}

/// A `HEAD` 503 is a transient fault, never an absence (#1815): folding it
/// into `Ok(None)` made `probe_origin_chain` answer `Absent` for an origin
/// mid-outage — the serve gate then signed an authoritative `NotFound`,
/// memoised under the negative TTL, and the DHT republisher unscheduled
/// the hash entirely.
#[tokio::test]
async fn size_5xx_is_a_transient_fault_not_an_absence() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-size-outage-marker");
    Mock::given(method("HEAD"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    match origin.size(hash).await {
        Err(err) => anyhow::ensure!(
            err.is_transient(),
            "a 503 is worth waiting out; got a permanent fault: {err}"
        ),
        Ok(size) => anyhow::bail!("a 503 must fault, got Ok({size:?})"),
    }
    Ok(())
}

/// A `HEAD` 403 is a permanent fault: it will read the same on every
/// probe, so fault-aware callers must not hold state open for it — but it
/// is still not an absence the serve gate may sign. Mirrors the S3
/// adapter's `classify_head_object_error`.
#[tokio::test]
async fn size_non_404_decline_is_a_permanent_fault() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-size-forbidden-marker");
    Mock::given(method("HEAD"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(403))
        .mount(&server)
        .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    match origin.size(hash).await {
        Err(err) => anyhow::ensure!(
            !err.is_transient(),
            "a 403 reads the same on every probe; got a transient fault: {err}"
        ),
        Ok(size) => anyhow::bail!("a 403 must fault, got Ok({size:?})"),
    }
    Ok(())
}

/// A successful `HEAD` reports the object's `Content-Length`.
#[tokio::test]
async fn size_reads_content_length_on_success() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-size-present-marker");
    Mock::given(method("HEAD"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(200).insert_header("content-length", "12345"))
        .mount(&server)
        .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    anyhow::ensure!(
        origin.size(hash).await? == OriginSize::Known(12345),
        "a 200 must surface the Content-Length",
    );
    Ok(())
}

/// `DEFAULT_USER_AGENT` (#435, now in the `decdn-config-types` leaf
/// crate per #578) embeds that crate's `CARGO_PKG_VERSION` so origin
/// operators can attribute pull-through traffic. The `decdn-node/`
/// prefix is the stable contract — operators grep it in access logs.
#[test]
fn default_user_agent_has_expected_prefix_and_version() -> anyhow::Result<()> {
    anyhow::ensure!(
        DEFAULT_USER_AGENT.starts_with("decdn-node/"),
        "got: {DEFAULT_USER_AGENT}"
    );
    anyhow::ensure!(
        DEFAULT_USER_AGENT.len() > "decdn-node/".len(),
        "version segment missing: {DEFAULT_USER_AGENT}"
    );
    Ok(())
}

/// `HttpOrigin::new_with_user_agent` accepts a non-default UA without
/// erroring on a typical operator-supplied value.
#[tokio::test]
async fn new_with_user_agent_accepts_custom_value() -> anyhow::Result<()> {
    let url = parse_origin_url("https://origin.example/")?;
    let _origin = HttpOrigin::new_with_user_agent(url, "MyCdn/1.0 (+ops@example.com)")?;
    Ok(())
}

/// Building an origin outside a tokio runtime is an error, never a panic:
/// the origin has no runtime to own its connections.
#[test]
fn new_outside_a_runtime_is_an_error() -> anyhow::Result<()> {
    let url = parse_origin_url("https://origin.example/")?;
    anyhow::ensure!(HttpOrigin::new(url).is_err(), "built outside a runtime");
    Ok(())
}
