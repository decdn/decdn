//! Integration tests for remote-origin prewarm (`CacheEngine::prewarm`, #1130).
//!
//! Prewarm is the half of #1130 that serves `http`/`s3` origins: pull the
//! operator's pin set into the store before any client asks, so the first
//! request does not pay full pull-through latency. The properties worth pinning
//! are the ones that cost money or correctness if they regress — that prewarm
//! pays origin egress only for what is actually missing, that it never bypasses
//! a takedown, and that it never fronts USDC to a peer.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use decdn_cache::{
    CacheEngine, CacheMetrics, Hash, HttpOrigin, Origin, OriginFetch, OriginKind, OriginPullError,
    PinnedHashes, RetryPolicy,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A mock HTTP origin serving each of `payloads` at `/{blake3_hex}`.
///
/// Returns a fixed-size array so callers destructure (`let [a, b] = …`) instead
/// of indexing — the workspace denies `clippy::indexing_slicing` in tests too.
async fn serve_blobs<const N: usize>(payloads: [&'static [u8]; N]) -> (MockServer, [Hash; N]) {
    let server = MockServer::start().await;
    let mut hashes = Vec::with_capacity(N);
    for payload in payloads {
        let hash = Hash::new(payload);
        Mock::given(method("GET"))
            .and(path(format!("/{}", hash.to_hex())))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
            .mount(&server)
            .await;
        hashes.push(hash);
    }
    let hashes: [Hash; N] = hashes
        .try_into()
        .unwrap_or_else(|_| unreachable!("pushed exactly N hashes"));
    (server, hashes)
}

async fn build_engine(
    origins: Vec<Arc<dyn Origin>>,
    pinned: PinnedHashes,
    metrics: Option<Arc<CacheMetrics>>,
) -> anyhow::Result<(CacheEngine, tempfile::TempDir)> {
    let tmp = tempfile::tempdir()?;
    let engine = CacheEngine::open_full(
        tmp.path(),
        origins,
        16,
        pinned,
        RetryPolicy::disabled(),
        decdn_cache::CircuitBreakerPolicy::default(),
        metrics,
        Duration::ZERO,
    )
    .await?;
    Ok((engine, tmp))
}

fn pin(hashes: &[Hash]) -> PinnedHashes {
    PinnedHashes::new(
        hashes
            .iter()
            .map(|h| decdn_config_types::Hash::from_bytes(*h.as_bytes()))
            .collect(),
    )
}

#[tokio::test]
async fn prewarm_fetches_missing_pins_and_skips_present_ones() -> anyhow::Result<()> {
    let present: &[u8] = b"already in the store";
    let missing: &[u8] = b"only at the origin";
    let (server, [present_hash, missing_hash]) = serve_blobs([present, missing]).await;

    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let metrics = Arc::new(CacheMetrics::default());
    let (engine, _tmp) = build_engine(
        vec![origin as Arc<dyn Origin>],
        pin(&[present_hash, missing_hash]),
        Some(Arc::clone(&metrics)),
    )
    .await?;

    // Land one of the two in the store the normal way, so prewarm meets a
    // realistic mixed set rather than an all-cold one.
    engine.get(present_hash).await?;

    let report = engine.prewarm_pinned().await;
    anyhow::ensure!(
        report.fetched == 1 && report.already_present == 1 && report.failed == 0,
        "prewarm should pay egress only for the absent pin, got {report:?}"
    );
    anyhow::ensure!(
        report.bytes == missing.len() as u64,
        "byte attribution should cover exactly the fetched blob, got {}",
        report.bytes
    );
    anyhow::ensure!(
        engine.has(missing_hash).await?,
        "the missing pin should be resident after prewarm"
    );

    // Re-running is a no-op: this is what makes the reload path safe to call
    // with the whole pin set instead of a delta.
    let again = engine.prewarm_pinned().await;
    anyhow::ensure!(
        again.fetched == 0 && again.already_present == 2 && again.bytes == 0,
        "a second prewarm must not re-pay egress, got {again:?}"
    );

    anyhow::ensure!(
        metrics.prewarm_blobs.get() == 1 && metrics.prewarm_bytes.get() == missing.len() as u64,
        "prewarm metrics should count the single real fetch across both passes"
    );
    Ok(())
}

#[tokio::test]
async fn prewarm_counts_a_pin_the_origin_does_not_have_as_a_failure() -> anyhow::Result<()> {
    // The common misconfiguration: pinned_hashes naming content that is not in
    // the configured origin. It must be loud (a failure count and a metric) and
    // non-fatal — the node still boots and serves everything else.
    let held: &[u8] = b"origin has this";
    let (server, [held_hash]) = serve_blobs([held]).await;
    let absent = Hash::new(b"origin has never heard of this");

    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let metrics = Arc::new(CacheMetrics::default());
    let (engine, _tmp) = build_engine(
        vec![origin as Arc<dyn Origin>],
        pin(&[held_hash, absent]),
        Some(Arc::clone(&metrics)),
    )
    .await?;

    let report = engine.prewarm_pinned().await;
    anyhow::ensure!(
        report.fetched == 1
            && report.failed == 1
            && report.already_present == 0
            && report.refused == 0
            && report.bytes == held.len() as u64,
        "one pin warms, the unknown one fails, and nothing lands in another bin: {report:?}"
    );
    anyhow::ensure!(
        metrics.prewarm_failures.get() == 1,
        "the failure must be visible as a metric, not only in the log"
    );
    Ok(())
}

#[tokio::test]
async fn prewarm_never_re_pulls_a_refused_hash() -> anyhow::Result<()> {
    // A takedown must survive prewarm. `evict` is durable (`evicted.log`), and
    // an operator who pins and later evicts the same hash has to win — prewarm
    // re-pulling it would silently undo a DMCA response.
    let payload: &[u8] = b"taken down";
    let (server, [hash]) = serve_blobs([payload]).await;

    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let (engine, _tmp) = build_engine(vec![origin as Arc<dyn Origin>], pin(&[hash]), None).await?;

    engine.get(hash).await?;
    engine.evict(hash).await?;

    // Count origin requests across the prewarm, not `has()` afterwards: `has`
    // short-circuits on `refuses`, so asserting `!has` would merely restate
    // `has`'s own contract and would still pass if prewarm HAD re-pulled the
    // bytes. What we care about is that no GET reached the origin.
    let before = server.received_requests().await.map_or(0, |r| r.len());
    let report = engine.prewarm_pinned().await;
    let after = server.received_requests().await.map_or(0, |r| r.len());
    anyhow::ensure!(
        report.refused == 1 && report.fetched == 0,
        "an evicted hash must be refused, not re-pulled, got {report:?}"
    );
    anyhow::ensure!(
        after == before,
        "prewarm must not have hit the origin for a refused hash ({before} -> {after})"
    );
    Ok(())
}

#[tokio::test]
async fn prewarm_dedups_input_and_the_counts_partition_it() -> anyhow::Result<()> {
    // `prewarm`'s two documented contracts, neither of which `prewarm_pinned`
    // can exercise (it sources from a `HashSet`, so duplicates never reach the
    // dedup guard): repeated hashes collapse, and the four outcome counts
    // partition the distinct input exactly — the property that makes the log
    // line diagnosable.
    let held: &[u8] = b"origin holds this one";
    let (server, [held_hash]) = serve_blobs([held]).await;
    let absent = Hash::new(b"nowhere to be found");

    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let (engine, _tmp) =
        build_engine(vec![origin as Arc<dyn Origin>], PinnedHashes::empty(), None).await?;

    let report = engine
        .prewarm(vec![held_hash, held_hash, held_hash, absent, absent])
        .await;
    anyhow::ensure!(
        report.fetched == 1 && report.failed == 1,
        "three copies of one hash are one fetch; two copies of another are one failure, \
         got {report:?}"
    );
    let total = report.fetched + report.already_present + report.refused + report.failed;
    anyhow::ensure!(
        total == 2,
        "the four counts must partition the 2 DISTINCT inputs, summed to {total}: {report:?}"
    );
    Ok(())
}

/// An origin that reports itself as `Peer` and records every fetch. Stands in
/// for the node→node pull origin, whose egress is billed in USDC.
#[derive(Debug, Default)]
struct PeerOriginSpy {
    fetches: AtomicUsize,
}

impl Origin for PeerOriginSpy {
    fn fetch(
        &self,
        _hash: Hash,
        _max_bytes: u64,
    ) -> Pin<Box<dyn Future<Output = Result<OriginFetch, OriginPullError>> + Send + '_>> {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(OriginFetch::NotFound) })
    }

    fn kind(&self) -> OriginKind {
        OriginKind::Peer
    }
}

#[tokio::test]
async fn prewarm_never_fronts_usdc_to_a_peer_origin() -> anyhow::Result<()> {
    // Prewarm fills local-only, so the `Peer` origin is skipped. The operator
    // opted into paying their *own* origin's egress for content nobody has
    // requested yet; paying upstream nodes for it is a different decision they
    // have not made.
    //
    // The chain here is `[Http, Peer]`, which is the shape the node actually
    // builds — the node→node origin is appended after the configured ones. A
    // `[Peer]`-only chain would prove less: every outcome would be a failure, so
    // it could not distinguish "skipped the peer" from "aborted entirely".
    // Pinning one hash the HTTP origin holds and one only the peer holds forces
    // prewarm to keep walking the chain while still refusing the paid fallback.
    let via_http: &[u8] = b"the configured origin has this";
    let (server, [http_hash]) = serve_blobs([via_http]).await;
    let peer_only = Hash::new(b"only a peer could serve this");

    let spy = Arc::new(PeerOriginSpy::default());
    let http = Arc::new(HttpOrigin::parse(&server.uri())?);
    let (engine, _tmp) = build_engine(
        vec![http as Arc<dyn Origin>, Arc::clone(&spy) as Arc<dyn Origin>],
        pin(&[http_hash, peer_only]),
        None,
    )
    .await?;

    let report = engine.prewarm_pinned().await;
    anyhow::ensure!(
        report.fetched == 1 && report.failed == 1,
        "the http pin warms; the peer-only pin must fail rather than be bought, got {report:?}"
    );
    anyhow::ensure!(
        spy.fetches.load(Ordering::SeqCst) == 0,
        "prewarm must not have touched the peer origin at all"
    );
    Ok(())
}

#[tokio::test]
async fn overlapping_prewarms_do_not_double_count_paid_egress() -> anyhow::Result<()> {
    // Two passes can overlap in production: the startup warm is detached and a
    // SIGHUP mid-warm starts a second. The engine coalesces concurrent fills for
    // the same hash, so only ONE of them pays the origin — and the metrics must
    // agree with the origin, not with the number of callers who saw `Ok`.
    //
    // One-directional assertion, so it cannot false-fail: if the interleaving
    // happens to serialize, both the counter and the request count are 1.
    let payload: &[u8] = b"exactly one of these fetches is real";
    let (server, [hash]) = serve_blobs([payload]).await;

    let origin = Arc::new(HttpOrigin::parse(&server.uri())?);
    let metrics = Arc::new(CacheMetrics::default());
    let (engine, _tmp) = build_engine(
        vec![origin as Arc<dyn Origin>],
        pin(&[hash]),
        Some(Arc::clone(&metrics)),
    )
    .await?;

    let (a, b) = tokio::join!(engine.prewarm_pinned(), engine.prewarm_pinned());
    let gets = server.received_requests().await.map_or(0, |r| r.len());
    let counted = a.fetched + b.fetched;
    anyhow::ensure!(
        counted == gets,
        "prewarm counted {counted} fetches but the origin served {gets} requests \
         (a coalesced waiter must not be counted as a fetch): {a:?} / {b:?}"
    );
    anyhow::ensure!(
        metrics.prewarm_bytes.get() == payload.len() as u64,
        "bytes must be attributed once per real fetch, got {}",
        metrics.prewarm_bytes.get()
    );
    Ok(())
}
