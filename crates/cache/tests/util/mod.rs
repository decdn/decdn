//! Shared fixtures for the `present_ranges` integration suite.
//!
//! Mirrors the proven helpers in `pull_through.rs` / `range_pull.rs`
//! (`make_blob`, `sub`, `CacheEngine::open_full`, range-origin mocking) —
//! duplicated locally by design, matching how those suites already
//! duplicate each other rather than sharing a cross-suite helper crate.

use std::sync::Arc;
use std::time::Duration;

use bao_tree::io::outboard::PreOrderMemOutboard;
use decdn_cache::range_pull::IROH_BLOCK_SIZE;
use decdn_cache::{
    CacheEngine, CacheMetrics, CircuitBreakerPolicy, HttpOrigin, Origin, PinnedHashes, RetryPolicy,
};
use iroh_blobs::Hash;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// Deterministic pseudo-random blob, identical generator to
/// `pull_through.rs:4035` / `range_pull.rs`.
#[allow(dead_code)]
pub fn make_blob(len: usize) -> Vec<u8> {
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
pub fn sub(data: &[u8], start: u64, end: u64) -> anyhow::Result<Vec<u8>> {
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
pub async fn empty_engine() -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    build(vec![]).await
}

#[allow(dead_code)]
pub async fn engine_with_whole_blob(
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

#[allow(dead_code)]
pub async fn engine_with_range_origin(
    payload: &[u8],
) -> anyhow::Result<(CacheEngine, Hash, tempfile::TempDir, MockServer)> {
    let ob = PreOrderMemOutboard::create(payload, IROH_BLOCK_SIZE);
    let hash = Hash::from_bytes(*ob.root.as_bytes());
    let server = serve_range_origin_impl(payload, &ob, hash).await;
    let origin = Arc::new(HttpOrigin::parse(&server.uri())?) as Arc<dyn Origin>;
    let (engine, tmp) = build(vec![origin]).await?;
    Ok((engine, hash, tmp, server))
}

/// Answers any exact-byte `Range: bytes=a-b` request with the matching slice
/// of `payload` as a `206`. Unlike `pull_through.rs`'s `serve_range_origin`
/// (which pins a single precomputed range), this must serve whatever aligned
/// span the engine ends up requesting, since callers here compute that span
/// themselves after the server is already standing.
struct RangeResponder {
    payload: Vec<u8>,
}

impl Respond for RangeResponder {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let requested = request
            .headers
            .get("range")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_range_header);
        match requested {
            Some((start, end_inclusive)) => {
                let s = usize::try_from(start).unwrap_or(usize::MAX);
                let e = usize::try_from(end_inclusive)
                    .ok()
                    .and_then(|e| e.checked_add(1))
                    .unwrap_or(usize::MAX);
                match self.payload.get(s..e.min(self.payload.len())) {
                    Some(slice) => ResponseTemplate::new(206).set_body_bytes(slice.to_vec()),
                    None => ResponseTemplate::new(416),
                }
            }
            None => ResponseTemplate::new(200).set_body_bytes(self.payload.clone()),
        }
    }
}

fn parse_range_header(value: &str) -> Option<(u64, u64)> {
    let rest = value.strip_prefix("bytes=")?;
    let (a, b) = rest.split_once('-')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

/// Adapted from `serve_range_origin` (`pull_through.rs`): same outboard + 206-range
/// (`pull_through.rs:4063`): mounts `{hex}.obao4` (full `200`) and `{hex}`
/// (`206` on `Range`), adapted to serve arbitrary ranges via
/// [`RangeResponder`] instead of one pinned range.
async fn serve_range_origin_impl(
    payload: &[u8],
    ob: &PreOrderMemOutboard,
    hash: Hash,
) -> MockServer {
    let server = MockServer::start().await;
    let hex = hash.to_hex();

    // Outboard sibling — full 200.
    Mock::given(method("GET"))
        .and(path(format!("/{hex}.obao4")))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(ob.data.clone()))
        .mount(&server)
        .await;

    // Ranged data — 206 with exactly the requested span.
    Mock::given(method("GET"))
        .and(path(format!("/{hex}")))
        .respond_with(RangeResponder {
            payload: payload.to_vec(),
        })
        .mount(&server)
        .await;

    server
}
