//! Integration tests for [`decdn_cache::CacheEngine`] end-to-end with a
//! mocked HTTP origin.

use std::sync::Arc;

use decdn_cache::{CacheEngine, CacheError, Hash, HttpOrigin};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Spin up a wiremock server that serves a single blob at
/// `/{blake3_hex}`. Returns the `(server, hash)` pair.
async fn serve_blob(payload: &'static [u8]) -> (MockServer, Hash) {
    let server = MockServer::start().await;
    let hash = Hash::new(payload);
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
        .mount(&server)
        .await;
    (server, hash)
}

async fn build_engine(origin_url: &str) -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::new(origin_url)?);
    let engine = CacheEngine::open(tmp.path(), Some(origin), 16).await?;
    Ok((engine, tmp))
}

fn err_of<T: std::fmt::Debug, E>(r: Result<T, E>) -> anyhow::Result<E> {
    r.err()
        .ok_or_else(|| anyhow::anyhow!("expected error, got Ok"))
}

#[tokio::test]
async fn cache_miss_pulls_from_origin_and_caches() -> anyhow::Result<()> {
    let payload: &[u8] = b"hello, decdn";
    let (server, hash) = serve_blob(payload).await;
    let (engine, _tmp) = build_engine(&server.uri()).await?;

    anyhow::ensure!(!engine.has(hash).await?, "blob should be absent initially");

    let got = engine.get(hash).await?;
    anyhow::ensure!(&got[..] == payload, "first get should return origin bytes");
    anyhow::ensure!(engine.has(hash).await?, "blob should be cached after miss");

    // Second get is served locally — drop the mock server to prove it.
    drop(server);
    let got2 = engine.get(hash).await?;
    anyhow::ensure!(&got2[..] == payload, "second get should be served locally");

    Ok(())
}

#[tokio::test]
async fn hash_mismatch_is_rejected_and_not_cached() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let expected = Hash::new(b"expected");
    let mismatched_payload: &[u8] = b"something else entirely";
    Mock::given(method("GET"))
        .and(path(format!("/{}", expected.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(mismatched_payload))
        .mount(&server)
        .await;

    let (engine, _tmp) = build_engine(&server.uri()).await?;
    let err = err_of(engine.get(expected).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::HashMismatch { .. }),
        "expected HashMismatch, got: {err:?}"
    );
    anyhow::ensure!(!engine.has(expected).await?, "bad bytes must not be cached");
    Ok(())
}

#[tokio::test]
async fn origin_not_found_surfaces_not_found() -> anyhow::Result<()> {
    let server = MockServer::start().await;
    let hash = Hash::new(b"absent");
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let (engine, _tmp) = build_engine(&server.uri()).await?;
    let err = err_of(engine.get(hash).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::NotFound { .. }),
        "expected NotFound, got: {err:?}"
    );
    Ok(())
}

#[tokio::test]
async fn blob_too_large_is_rejected() -> anyhow::Result<()> {
    // Payload of 2 MB, cap at 1 MB.
    let payload = vec![0xABu8; 2 * 1024 * 1024];
    let payload_static: &'static [u8] = Box::leak(payload.into_boxed_slice());
    let hash = Hash::new(payload_static);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload_static))
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir()?;
    let origin = Arc::new(HttpOrigin::new(&server.uri())?);
    let engine = CacheEngine::open(tmp.path(), Some(origin), 1).await?;

    let err = err_of(engine.get(hash).await)?;
    // The HTTP origin trips the content-length check before bytes are read,
    // which surfaces as OriginError. Either OriginError or BlobTooLarge is
    // acceptable — both represent "refused to cache".
    anyhow::ensure!(
        matches!(
            err,
            CacheError::OriginError { .. } | CacheError::BlobTooLarge { .. }
        ),
        "expected size rejection, got: {err:?}"
    );
    Ok(())
}

#[tokio::test]
async fn miss_without_origin_returns_no_origin() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open(tmp.path(), None, 16).await?;
    let err = err_of(engine.get(Hash::new(b"whatever")).await)?;
    anyhow::ensure!(
        matches!(err, CacheError::NoOrigin { .. }),
        "expected NoOrigin, got: {err:?}"
    );
    Ok(())
}
