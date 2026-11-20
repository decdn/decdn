use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::*;
use crate::origin::log_capture::LogBuffer;

/// Mount a `GET` of `/{name}` answering `response`.
async fn mount_get(server: &MockServer, name: String, response: ResponseTemplate) {
    Mock::given(method("GET"))
        .and(path(format!("/{name}")))
        .respond_with(response)
        .mount(server)
        .await;
}

/// The request covering the first KiB of a blob.
const FIRST_KIB: OriginRangeRequest = OriginRangeRequest {
    fetch_start: 0,
    fetch_end: 1024,
};

/// A published sibling `{hex}.obao4` is fetched in full via
/// `fetch_outboard` alone (no data GET at all).
#[tokio::test]
async fn fetch_outboard_returns_sibling_obao4() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-outboard-marker");
    let outboard_bytes = vec![0xABu8; 4096];
    mount_get(
        &server,
        format!("{}.obao4", hash.to_hex()),
        ResponseTemplate::new(200).set_body_bytes(outboard_bytes.clone()),
    )
    .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    match origin.fetch_outboard(hash, 1 << 20).await? {
        OutboardFetch::Found(bytes) => {
            anyhow::ensure!(
                bytes.as_ref() == outboard_bytes.as_slice(),
                "outboard bytes mismatch"
            );
        }
        OutboardFetch::NotFound => anyhow::bail!("expected Found, got NotFound"),
    }
    Ok(())
}

/// A `404` or a `403` on the sibling outboard is `NotFound`, not an error:
/// an origin in front of a bucket without list permission answers 403 for a
/// missing key.
#[tokio::test]
async fn fetch_outboard_404_and_403_are_not_found() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let origin = HttpOrigin::parse(&server.uri())?;
    for status in [404u16, 403] {
        let hash = Hash::new(format!("http-outboard-missing-{status}").as_bytes());
        mount_get(
            &server,
            format!("{}.obao4", hash.to_hex()),
            ResponseTemplate::new(status),
        )
        .await;
        anyhow::ensure!(
            matches!(
                origin.fetch_outboard(hash, 1 << 20).await?,
                OutboardFetch::NotFound
            ),
            "{status} must be NotFound",
        );
    }
    Ok(())
}

/// A `401` on the sibling outboard is a permanent fault: it reads the same on
/// every request and is not a missing key.
#[tokio::test]
async fn fetch_outboard_401_is_a_permanent_fault() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-outboard-unauthorized-marker");
    mount_get(
        &server,
        format!("{}.obao4", hash.to_hex()),
        ResponseTemplate::new(401),
    )
    .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    match origin.fetch_outboard(hash, 1 << 20).await {
        Err(err) => anyhow::ensure!(!err.is_transient(), "a 401 is permanent: {err}"),
        Ok(fetch) => anyhow::bail!("a 401 must fault, got Ok({fetch:?})"),
    }
    Ok(())
}

/// An outboard GET 503 is a transient fault (#2826); see `missing_or_fault`.
#[tokio::test]
async fn fetch_outboard_5xx_is_a_transient_fault() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-outboard-outage-marker");
    mount_get(
        &server,
        format!("{}.obao4", hash.to_hex()),
        ResponseTemplate::new(503),
    )
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

/// A `403` read as a missing object logs at warn the first time on an origin
/// and at debug after that; clones share the state. The warn is the
/// operator's only record that the origin's permissions may hide objects.
#[test]
fn forbidden_logs_warn_once_then_debug() -> anyhow::Result<()> {
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
        let clone = origin.clone();
        origin.log_forbidden("http://127.0.0.1:1/h.obao4");
        clone.log_forbidden("http://127.0.0.1:1/h.sizeproof");
        anyhow::Ok(())
    })?;

    let text = logs.contents();
    let lines: Vec<&str> = text
        .lines()
        .filter(|l| l.contains("answered 403"))
        .collect();
    anyhow::ensure!(
        matches!(
            lines.as_slice(),
            [first, second] if first.contains("WARN") && second.contains("DEBUG")
        ),
        "expected one WARN then one DEBUG 403 line:\n{text}"
    );
    Ok(())
}

/// A ranged-data GET 503 is a transient fault. See `get_range_bytes`.
#[tokio::test]
async fn fetch_range_data_5xx_is_a_transient_fault() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-range-outage-marker");
    mount_get(&server, hash.to_hex().clone(), ResponseTemplate::new(503)).await;
    let origin = HttpOrigin::parse(&server.uri())?;

    match origin.fetch_range_data(hash, FIRST_KIB).await {
        Err(err) => anyhow::ensure!(
            err.is_transient(),
            "a 503 must classify as Transient; got a permanent fault: {err}"
        ),
        Ok(fetch) => anyhow::bail!("a 503 must fault, got Ok({fetch:?})"),
    }
    Ok(())
}

/// A ranged-data GET 404 is a missing data object: `NotFound`.
#[tokio::test]
async fn fetch_range_data_404_is_not_found() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-range-missing-marker");
    mount_get(&server, hash.to_hex().clone(), ResponseTemplate::new(404)).await;
    let origin = HttpOrigin::parse(&server.uri())?;

    anyhow::ensure!(
        matches!(
            origin.fetch_range_data(hash, FIRST_KIB).await?,
            OriginRangeFetch::NotFound
        ),
        "a 404 range GET must be NotFound",
    );
    Ok(())
}

/// An origin that ignores `Range` and answers `200` with the whole object is
/// a permanent fault: the range path needs byte ranges.
#[tokio::test]
async fn fetch_range_data_200_is_a_permanent_fault() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-range-ignored-marker");
    mount_get(
        &server,
        hash.to_hex().clone(),
        ResponseTemplate::new(200).set_body_bytes(vec![0u8; 4096]),
    )
    .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    match origin.fetch_range_data(hash, FIRST_KIB).await {
        Err(err) => anyhow::ensure!(
            !err.is_transient() && format!("{err:#}").contains("not 206"),
            "a 200 range answer must be a permanent fault: {err:#}"
        ),
        Ok(fetch) => anyhow::bail!("a 200 range answer must fault, got Ok({fetch:?})"),
    }
    Ok(())
}

/// A `206` with a body shorter than the requested span is a permanent fault.
#[tokio::test]
async fn fetch_range_data_wrong_length_206_is_a_permanent_fault() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-range-short-marker");
    mount_get(
        &server,
        hash.to_hex().clone(),
        ResponseTemplate::new(206).set_body_bytes(vec![0u8; 100]),
    )
    .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    match origin.fetch_range_data(hash, FIRST_KIB).await {
        Err(err) => anyhow::ensure!(!err.is_transient(), "a short 206 is permanent: {err}"),
        Ok(fetch) => anyhow::bail!("a short 206 must fault, got Ok({fetch:?})"),
    }
    Ok(())
}

/// 5xx, 408 and 429 are retriable; every other status is not. Shared by the
/// ranged-data GET and the sibling GETs.
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

/// An outboard over `outboard_max_bytes` is a permanent fault rather than a
/// buffer — the OOM guard on the one outboard read a range pull makes.
#[tokio::test]
async fn fetch_outboard_oversize_is_a_permanent_fault() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-outboard-oversize-marker");
    mount_get(
        &server,
        format!("{}.obao4", hash.to_hex()),
        ResponseTemplate::new(200).set_body_bytes(vec![0x5Au8; 8192]),
    )
    .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    match origin.fetch_outboard(hash, 1024).await {
        Err(err) => anyhow::ensure!(
            !err.is_transient() && format!("{err:#}").contains("over its 1024-byte cap"),
            "an oversize outboard must be a permanent fault: {err:#}"
        ),
        Ok(fetch) => anyhow::bail!("an oversize outboard must fault, got Ok({fetch:?})"),
    }
    Ok(())
}

/// A published `{hex}.sizeproof` is read verbatim; a `404` or a `403` is no
/// proof (`None`), a `503` a transient fault.
#[tokio::test]
async fn fetch_size_proof_classifies_its_status() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let origin = HttpOrigin::parse(&server.uri())?;
    let object = decdn_bao_range::size_proof_object(5, b"proof");

    let present = Hash::new(b"http-size-proof-present");
    mount_get(
        &server,
        format!("{}.sizeproof", present.to_hex()),
        ResponseTemplate::new(200).set_body_bytes(object.clone()),
    )
    .await;
    anyhow::ensure!(
        origin.fetch_size_proof(present).await?.as_deref() == Some(object.as_slice()),
        "a 200 must return the object"
    );

    for status in [404u16, 403] {
        let hash = Hash::new(format!("http-size-proof-{status}").as_bytes());
        mount_get(
            &server,
            format!("{}.sizeproof", hash.to_hex()),
            ResponseTemplate::new(status),
        )
        .await;
        anyhow::ensure!(
            origin.fetch_size_proof(hash).await?.is_none(),
            "{status} must be no proof"
        );
    }

    let outage = Hash::new(b"http-size-proof-503");
    mount_get(
        &server,
        format!("{}.sizeproof", outage.to_hex()),
        ResponseTemplate::new(503),
    )
    .await;
    match origin.fetch_size_proof(outage).await {
        Err(err) => anyhow::ensure!(err.is_transient(), "a 503 is transient: {err}"),
        Ok(got) => anyhow::bail!("a 503 must fault, got Ok({got:?})"),
    }
    Ok(())
}

/// A `{hex}.sizeproof` over its cap is a broken object: a permanent fault,
/// never an absence.
#[tokio::test]
async fn fetch_size_proof_oversize_is_a_permanent_fault() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"http-size-proof-oversize");
    mount_get(
        &server,
        format!("{}.sizeproof", hash.to_hex()),
        ResponseTemplate::new(200).set_body_bytes(vec![
            0u8;
            decdn_bao_range::MAX_SIZE_PROOF_OBJECT_BYTES
                + 1
        ]),
    )
    .await;
    let origin = HttpOrigin::parse(&server.uri())?;

    match origin.fetch_size_proof(hash).await {
        Err(err) => anyhow::ensure!(!err.is_transient(), "oversize is permanent: {err}"),
        Ok(got) => anyhow::bail!("an oversize proof must fault, got Ok({got:?})"),
    }
    Ok(())
}

/// `DEFAULT_USER_AGENT` (#435, in the `decdn-config-types` leaf crate per
/// #578) embeds that crate's `CARGO_PKG_VERSION` so origin operators can
/// attribute pull-through traffic. The `decdn-node/` prefix is the stable
/// contract — operators grep it in access logs.
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
