//! Shared fixtures for the `present_ranges` integration suite.
//!
//! Mirrors the proven helpers in `pull_through.rs` / `range_pull.rs`
//! (`make_blob`, `sub`, `CacheEngine::open_full`, whole-blob origin mocking) —
//! duplicated locally by design, matching how those suites already
//! duplicate each other rather than sharing a cross-suite helper crate.

use std::sync::Arc;
use std::time::Duration;

use decdn_cache::{
    CacheEngine, CacheMetrics, CircuitBreakerPolicy, HttpOrigin, Origin, PinnedHashes, RetryPolicy,
};
use iroh_blobs::Hash;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Deterministic pseudo-random blob, identical generator to
/// `pull_through.rs:4035` / `range_pull.rs`.
#[allow(dead_code)]
pub(crate) fn make_blob(len: usize) -> Vec<u8> {
    let mut v = vec![0u8; len];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut v {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    v
}

#[allow(dead_code)]
pub(crate) fn sub(data: &[u8], start: u64, end: u64) -> anyhow::Result<Vec<u8>> {
    let s = usize::try_from(start)?;
    let e = usize::try_from(end)?;
    data.get(s..e)
        .map(<[u8]>::to_vec)
        .ok_or_else(|| anyhow::anyhow!("range [{start}, {end}) out of bounds"))
}

async fn build(origins: Vec<Arc<dyn Origin>>) -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open_full(
        tmp.path(),
        origins,
        16,
        PinnedHashes::empty(),
        RetryPolicy::disabled(),
        CircuitBreakerPolicy::default(),
        Some(Arc::new(CacheMetrics::default())),
        Duration::ZERO,
    )
    .await?;
    Ok((engine, tmp))
}

#[allow(dead_code)]
pub(crate) async fn empty_engine() -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    build(vec![]).await
}

#[allow(dead_code)]
pub(crate) async fn engine_with_whole_blob(
    payload: &[u8],
) -> anyhow::Result<(CacheEngine, Hash, tempfile::TempDir, MockServer)> {
    let hash = Hash::new(payload);
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?) as Arc<dyn Origin>;
    let (engine, tmp) = build(vec![origin]).await?;
    Ok((engine, hash, tmp, server))
}
