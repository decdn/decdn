//! A held blob whose stored bytes change after admission (#1984).
//!
//! Every serve export validates the held bytes against the content root. A
//! mismatch must quarantine the hash: the engine stops serving and announcing
//! it, releases the entry to GC, and lifts the quarantine once the sweep
//! reclaims it, so a later pull-through admits a verified copy.

use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use decdn_cache::{CacheEngine, CacheMetrics, Hash, HttpOrigin, Origin, PinnedHashes, RetryPolicy};
use futures_util::StreamExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Large enough that the store keeps the data in `data/{hex}.data` rather than
/// inline in its database (the inline threshold is 16 KiB).
const BLOB_LEN: usize = 200 * 1024 + 1234;

fn payload() -> Vec<u8> {
    let mut payload = vec![0u8; BLOB_LEN];
    let mut x: u32 = 0x9e37_79b9;
    for b in &mut payload {
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        *b = x.to_le_bytes().first().copied().unwrap_or(0);
    }
    payload
}

async fn serve(payload: &[u8]) -> (MockServer, Hash) {
    let server = MockServer::start().await;
    let hash = Hash::new(payload);
    Mock::given(method("GET"))
        .and(path(format!("/{}", hash.to_hex())))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload.to_vec()))
        .mount(&server)
        .await;
    (server, hash)
}

async fn open(
    dir: &Path,
    origin_url: &str,
    pinned: PinnedHashes,
    metrics: &Arc<CacheMetrics>,
    gc_interval: Duration,
) -> anyhow::Result<CacheEngine> {
    let origin = Arc::new(HttpOrigin::parse(origin_url)?);
    Ok(CacheEngine::open_full(
        dir,
        vec![origin as Arc<dyn Origin>],
        16,
        pinned,
        RetryPolicy::default(),
        decdn_cache::CircuitBreakerPolicy::default(),
        Some(Arc::clone(metrics)),
        gc_interval,
    )
    .await?)
}

/// Flip one byte inside the stored data file of `hash`, behind the live store.
fn tamper(dir: &Path, hash: Hash) -> anyhow::Result<()> {
    let file = dir.join("data").join(format!("{}.data", hash.to_hex()));
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file)?;
    let offset = 100 * 1024;
    f.seek(SeekFrom::Start(offset))?;
    let mut byte = [0u8; 1];
    f.read_exact(&mut byte)?;
    byte[0] ^= 0xff;
    f.seek(SeekFrom::Start(offset))?;
    f.write_all(&byte)?;
    f.sync_all()?;
    Ok(())
}

/// Drain a whole-blob serve export. Returns whether it ended in an `Err` item.
async fn serve_fails(engine: &CacheEngine, hash: Hash) -> anyhow::Result<bool> {
    let len = u64::try_from(BLOB_LEN)?;
    let mut stream = engine.export_bao_range_stream(hash, 0, 0, len).await?;
    while let Some(item) = stream.next().await {
        if item.is_err() {
            return Ok(true);
        }
    }
    Ok(false)
}

#[tokio::test]
async fn a_serve_that_trips_stored_corruption_quarantines_the_hash() -> anyhow::Result<()> {
    let payload = payload();
    let (server, hash) = serve(&payload).await;
    let tmp = tempfile::tempdir()?;
    let metrics = Arc::new(CacheMetrics::default());
    let engine = open(
        tmp.path(),
        &server.uri(),
        PinnedHashes::empty(),
        &metrics,
        Duration::ZERO,
    )
    .await?;
    engine.get(hash).await?;
    anyhow::ensure!(!serve_fails(&engine, hash).await?, "the intact blob serves");

    tamper(tmp.path(), hash)?;
    anyhow::ensure!(
        serve_fails(&engine, hash).await?,
        "the export must fail validation on the tampered group"
    );
    anyhow::ensure!(
        engine.is_quarantined(hash),
        "the mismatch quarantines the hash"
    );
    anyhow::ensure!(engine.refuses(hash), "a quarantined hash is refused");
    anyhow::ensure!(
        !engine.has(hash).await?,
        "a quarantined hash reports absent"
    );
    anyhow::ensure!(
        !engine.iter_hashes().await?.contains(&hash),
        "a quarantined hash leaves the announce enumeration"
    );
    anyhow::ensure!(
        engine.get(hash).await.is_err(),
        "a quarantined hash still on disk is not re-acquired or served"
    );

    // A second trip is the same quarantine, not a second count.
    let _ = engine
        .outboard_pairs(hash, &bao_tree::ChunkRanges::all())
        .await;
    anyhow::ensure!(
        metrics.held_corruption_quarantined.get() == 1,
        "one quarantine per hash; got {}",
        metrics.held_corruption_quarantined.get()
    );
    anyhow::ensure!(
        !engine.is_evicted(hash),
        "the quarantine is not a durable takedown"
    );
    Ok(())
}

#[tokio::test]
async fn outboard_pairs_over_stored_corruption_quarantines_the_hash() -> anyhow::Result<()> {
    let payload = payload();
    let (server, hash) = serve(&payload).await;
    let tmp = tempfile::tempdir()?;
    let metrics = Arc::new(CacheMetrics::default());
    let engine = open(
        tmp.path(),
        &server.uri(),
        PinnedHashes::empty(),
        &metrics,
        Duration::ZERO,
    )
    .await?;
    engine.get(hash).await?;

    tamper(tmp.path(), hash)?;
    anyhow::ensure!(
        engine
            .outboard_pairs(hash, &bao_tree::ChunkRanges::all())
            .await
            .is_err(),
        "outboard_pairs must fail validation on the tampered group"
    );
    anyhow::ensure!(
        engine.is_quarantined(hash),
        "the mismatch quarantines the hash"
    );
    anyhow::ensure!(metrics.held_corruption_quarantined.get() == 1);
    Ok(())
}

#[tokio::test]
async fn an_export_fault_that_is_not_a_mismatch_does_not_quarantine() -> anyhow::Result<()> {
    let payload = payload();
    let (server, hash) = serve(&payload).await;
    let tmp = tempfile::tempdir()?;
    let metrics = Arc::new(CacheMetrics::default());
    let engine = open(
        tmp.path(),
        &server.uri(),
        PinnedHashes::empty(),
        &metrics,
        Duration::ZERO,
    )
    .await?;

    // Never fetched: the export faults on an absent blob.
    let failed = serve_fails(&engine, hash).await.unwrap_or(true);
    anyhow::ensure!(failed, "exporting an absent blob must fail");
    anyhow::ensure!(
        !engine.is_quarantined(hash),
        "an absent blob is not stored corruption"
    );
    anyhow::ensure!(metrics.held_corruption_quarantined.get() == 0);
    Ok(())
}

/// GC reclaims the quarantined entry, the quarantine lifts, and a pull-through
/// admits a verified copy that serves. Pinned, so the test also proves the
/// quarantine releases a pinned hash to GC.
#[tokio::test]
async fn gc_reclaim_lifts_the_quarantine_and_a_pinned_hash_re_admits() -> anyhow::Result<()> {
    let payload = payload();
    let (server, hash) = serve(&payload).await;
    let tmp = tempfile::tempdir()?;
    let metrics = Arc::new(CacheMetrics::default());
    let gc_interval = Duration::from_millis(200);
    let engine = open(
        tmp.path(),
        &server.uri(),
        PinnedHashes::new(HashSet::from([decdn_config_types::Hash::from_bytes(
            *hash.as_bytes(),
        )])),
        &metrics,
        gc_interval,
    )
    .await?;
    engine.get(hash).await?;

    tamper(tmp.path(), hash)?;
    anyhow::ensure!(
        serve_fails(&engine, hash).await?,
        "the tampered serve fails"
    );
    anyhow::ensure!(engine.is_quarantined(hash));

    let deadline = std::time::Instant::now() + gc_interval * 32;
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(gc_interval).await;
        // `has` lifts a reclaimed quarantine as a side effect.
        let _ = engine.has(hash).await?;
        if !engine.is_quarantined(hash) {
            break;
        }
    }
    anyhow::ensure!(
        !engine.is_quarantined(hash),
        "GC must reclaim the released entry and the quarantine must lift"
    );

    let got = engine.get(hash).await?;
    anyhow::ensure!(
        got.as_ref() == payload.as_slice(),
        "re-admitted bytes match"
    );
    anyhow::ensure!(
        !serve_fails(&engine, hash).await?,
        "the re-admitted copy serves"
    );
    anyhow::ensure!(engine.has(hash).await?, "the re-admitted copy is held");
    Ok(())
}
